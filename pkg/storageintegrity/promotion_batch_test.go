package storageintegrity

import (
	"context"
	"strings"
	"testing"

	"housegate/housegate/pkg/replay"
)

// TestClickHousePromoterBatchSinglePartitionOneReplace verifies a batched
// promotion (multiple statement ids) covering a single partition executes
// exactly one REPLACE PARTITION for that partition and CASes the merged root.
func TestClickHousePromoterBatchSinglePartitionOneReplace(t *testing.T) {
	c1, c2 := rawAccumHex("b1"), rawAccumHex("b2")
	expected, err := sumPartRowLtHashes([]string{c1, c2})
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	conn := &fakeSQLConn{}
	promoter := ClickHousePromoter{
		Conn: conn,
		ActiveParts: &fakeActivePartReader{parts: []replay.PartManifestEntry{
			{PartitionID: "202607", PartName: "safe_p1", PartRowLtHash: expected, RowCount: 4},
		}},
		PromoteDatabase: "hg_promote",
	}
	_, err = promoter.Promote(context.Background(), PromotionTask{
		PromotionID:  "promotion-batch",
		UnsafeTable:  "`hg_unsafe`.`events`",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607"},
		// Two statements batched into one promotion for the same partition.
		StatementIDs: []string{"stmt-1", "stmt-2"},
		CandidateParts: []ByteSidePart{
			{PartitionID: "202607", PartName: "p_1_1_0", PartRowLtHash: c1},
			{PartitionID: "202607", PartName: "p_2_2_0", PartRowLtHash: c2},
		},
		ExpectedPostRoot:        expected,
		RequirePostRootCAS:      true,
		SkipBasePartitionAttach: true,
	})
	if err != nil {
		t.Fatalf("batched single-partition Promote: %v", err)
	}
	replaces := 0
	for _, sql := range conn.execs {
		if strings.Contains(sql, "REPLACE PARTITION ID '202607'") {
			replaces++
		}
	}
	if replaces != 1 {
		t.Fatalf("expected exactly one REPLACE PARTITION for the batched partition, got %d:\n%s", replaces, strings.Join(conn.execs, "\n"))
	}
}

// TestClickHousePromoterBatchCrossPartitionFailsClosed verifies a batched
// promotion whose parts span more than one partition fails closed before any
// DDL runs.
func TestClickHousePromoterBatchCrossPartitionFailsClosed(t *testing.T) {
	conn := &fakeSQLConn{}
	promoter := ClickHousePromoter{Conn: conn, PromoteDatabase: "hg_promote"}
	_, err := promoter.Promote(context.Background(), PromotionTask{
		PromotionID:  "promotion-batch-bad",
		UnsafeTable:  "`hg_unsafe`.`events`",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202606", "202607"},
		StatementIDs: []string{"stmt-1", "stmt-2"},
		CandidateParts: []ByteSidePart{
			{PartitionID: "202606", PartName: "p_a"},
			{PartitionID: "202607", PartName: "p_b"},
		},
		RequirePostRootCAS: true,
	})
	if err == nil || !strings.Contains(err.Error(), "must cover a single partition") {
		t.Fatalf("err = %v, want cross-partition batch rejection", err)
	}
	if len(conn.execs) != 0 {
		t.Fatalf("cross-partition batch must fail closed before any DDL, got: %v", conn.execs)
	}
}

// TestClickHousePromoterBatchCandidateInWrongPartitionFailsClosed verifies a
// batched single-partition promotion whose candidate part is attributed to a
// different partition fails closed.
func TestClickHousePromoterBatchCandidateInWrongPartitionFailsClosed(t *testing.T) {
	conn := &fakeSQLConn{}
	promoter := ClickHousePromoter{Conn: conn, PromoteDatabase: "hg_promote"}
	_, err := promoter.Promote(context.Background(), PromotionTask{
		PromotionID:  "promotion-batch-wrong",
		UnsafeTable:  "`hg_unsafe`.`events`",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607"},
		StatementIDs: []string{"stmt-1", "stmt-2"},
		CandidateParts: []ByteSidePart{
			{PartitionID: "202607", PartName: "p_ok"},
			{PartitionID: "202606", PartName: "p_wrong"},
		},
		RequirePostRootCAS: true,
	})
	if err == nil || !strings.Contains(err.Error(), "must cover a single partition") {
		t.Fatalf("err = %v, want wrong-partition candidate rejection", err)
	}
	if len(conn.execs) != 0 {
		t.Fatalf("must fail closed before any DDL, got: %v", conn.execs)
	}
}

// TestClickHousePromoterSingleStatementUnaffectedByBatchGuard verifies a normal
// single-statement multi-partition promotion is NOT restricted by the batch
// guard (only batches of >1 statement are).
func TestClickHousePromoterSingleStatementUnaffectedByBatchGuard(t *testing.T) {
	if err := validateBatchPromotion(PromotionTask{
		StatementIDs: []string{"only-one"},
		PartitionIDs: []string{"202606", "202607"},
		CandidateParts: []ByteSidePart{
			{PartitionID: "202606", PartName: "p_a"},
			{PartitionID: "202607", PartName: "p_b"},
		},
	}); err != nil {
		t.Fatalf("single-statement multi-partition promotion must not be restricted: %v", err)
	}
}
