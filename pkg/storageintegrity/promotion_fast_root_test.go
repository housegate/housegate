package storageintegrity

import (
	"context"
	"strings"
	"testing"

	"housegate/housegate/pkg/replay"
)

// shadowReadbackCountingReader records which tables ReadActiveParts was called
// on, so a test can assert the fast path skipped the shadow readback while the
// promotion result (which reads the promoted safe table) still runs.
type shadowReadbackCountingReader struct {
	partsByTable map[string][]replay.PartManifestEntry
	readTables   []string
}

func (r *shadowReadbackCountingReader) ReadActiveParts(_ context.Context, table string, _ []string) ([]replay.PartManifestEntry, error) {
	r.readTables = append(r.readTables, table)
	return append([]replay.PartManifestEntry(nil), r.partsByTable[table]...), nil
}

// TestClickHousePromoterArithmeticPostRootMatchesReadback proves the fast-path
// arithmetic post root (base + Σ candidate part LtHashes) equals the readback
// root partitionRootFromActiveParts would produce, and that the fast path does
// NOT read back the shadow table for the CAS.
func TestClickHousePromoterArithmeticPostRootMatchesReadback(t *testing.T) {
	// Two candidate parts; their additive sum is the expected post root (genesis
	// -empty base). This is exactly what a byte-preserving ATTACH of both parts
	// into an empty shadow partition would read back.
	c1, c2 := rawAccumHex("cand-1"), rawAccumHex("cand-2")
	expected, err := sumPartRowLtHashes([]string{c1, c2})
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	shadow := "`hg_promote`.`promotion_p__202607`"
	conn := &fakeSQLConn{}
	// The shadow readback, if it were consulted, would return the same two parts
	// (so the test can't pass by luck — we assert it is NOT consulted).
	reader := &shadowReadbackCountingReader{partsByTable: map[string][]replay.PartManifestEntry{
		shadow: {
			{PartitionID: "202607", PartName: "s1", PartRowLtHash: c1, RowCount: 2},
			{PartitionID: "202607", PartName: "s2", PartRowLtHash: c2, RowCount: 3},
		},
		"`hg_safe`.`events`": {{PartitionID: "202607", PartName: "safe_p1", PartRowLtHash: expected, RowCount: 5}},
	}}
	promoter := ClickHousePromoter{
		Conn:            conn,
		ActiveParts:     reader,
		PromoteDatabase: "hg_promote",
		// StrictVerification: false (default) → arithmetic fast path.
	}
	result, err := promoter.Promote(context.Background(), PromotionTask{
		PromotionID:             "promotion-p",
		UnsafeTable:             "`hg_unsafe`.`events`",
		SafeTable:               "`hg_safe`.`events`",
		PartitionIDs:            []string{"202607"},
		CandidateParts:          []ByteSidePart{{PartitionID: "202607", PartName: "p_1_1_0", PartRowLtHash: c1}, {PartitionID: "202607", PartName: "p_1_2_0", PartRowLtHash: c2}},
		ExpectedPostRoot:        expected,
		RequirePostRootCAS:      true,
		SkipBasePartitionAttach: true,
	})
	if err != nil {
		t.Fatalf("Promote (fast path): %v", err)
	}
	// The shadow must NOT have been read back for the CAS; only the promoted safe
	// table is read (by promotionResult).
	for _, tbl := range reader.readTables {
		if tbl == shadow {
			t.Fatalf("fast path must not read back the shadow %q for the CAS; readTables=%v", shadow, reader.readTables)
		}
	}
	if len(result.ActiveParts) != 1 || result.ActiveParts[0].PartName != "safe_p1" {
		t.Fatalf("promotion result active parts = %+v", result.ActiveParts)
	}
	// REPLACE must have run (CAS passed).
	if !strings.Contains(strings.Join(conn.execs, "\n"), "REPLACE PARTITION ID '202607'") {
		t.Fatalf("expected REPLACE PARTITION after a passing fast-path CAS:\n%s", strings.Join(conn.execs, "\n"))
	}
}

// TestClickHousePromoterStrictModeReadsBackShadow proves strict mode consults
// the shadow readback (not the arithmetic path).
func TestClickHousePromoterStrictModeReadsBackShadow(t *testing.T) {
	root := rawAccumHex("only")
	shadow := "`hg_promote`.`promotion_p__202607`"
	conn := &fakeSQLConn{}
	reader := &shadowReadbackCountingReader{partsByTable: map[string][]replay.PartManifestEntry{
		shadow:               {{PartitionID: "202607", PartName: "s1", PartRowLtHash: root, RowCount: 4}},
		"`hg_safe`.`events`": {{PartitionID: "202607", PartName: "safe_p1", PartRowLtHash: root, RowCount: 4}},
	}}
	promoter := ClickHousePromoter{
		Conn:               conn,
		ActiveParts:        reader,
		PromoteDatabase:    "hg_promote",
		StrictVerification: true,
	}
	_, err := promoter.Promote(context.Background(), PromotionTask{
		PromotionID:             "promotion-p",
		UnsafeTable:             "`hg_unsafe`.`events`",
		SafeTable:               "`hg_safe`.`events`",
		PartitionIDs:            []string{"202607"},
		CandidateParts:          []ByteSidePart{{PartitionID: "202607", PartName: "p_1_1_0", PartRowLtHash: root}},
		ExpectedPostRoot:        root,
		RequirePostRootCAS:      true,
		SkipBasePartitionAttach: true,
	})
	if err != nil {
		t.Fatalf("Promote (strict): %v", err)
	}
	sawShadowRead := false
	for _, tbl := range reader.readTables {
		if tbl == shadow {
			sawShadowRead = true
		}
	}
	if !sawShadowRead {
		t.Fatalf("strict mode must read back the shadow %q; readTables=%v", shadow, reader.readTables)
	}
}
