package storageintegrity

import (
	"context"
	"testing"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"
)

// rowAccHex returns the raw accumulator hex over the given row elements, the
// same form ReadActiveParts now emits for part_row_lthash (gap-14).
func rowAccHex(elems ...string) string {
	acc := lthash.New()
	for _, e := range elems {
		acc.Add([]byte(e))
	}
	return lthashAccumulatorHex(acc)
}

// TestPartitionRootFromActivePartsIsAdditive is the gap-14 ledger equation on
// the HouseGate side: the additive partition root over a set of parts equals
// the lane-wise sum of the parts' raw accumulators and is invariant under
// re-packing the same rows into differently named parts (controlled
// compaction, spec §8.1). If this held only via the old part-name-bearing
// digest, re-packing would change the root and compaction could never satisfy
// sum(input)==sum(output).
func TestPartitionRootFromActivePartsIsAdditive(t *testing.T) {
	// Same three rows, packed two different ways.
	twoParts := []replay.PartManifestEntry{
		{TableID: "t", PartitionID: "P", PartName: "p_1", PartRowLtHash: rowAccHex("row-a", "row-b")},
		{TableID: "t", PartitionID: "P", PartName: "p_2", PartRowLtHash: rowAccHex("row-c")},
	}
	threeParts := []replay.PartManifestEntry{
		{TableID: "t", PartitionID: "P", PartName: "q_1", PartRowLtHash: rowAccHex("row-a")},
		{TableID: "t", PartitionID: "P", PartName: "q_2", PartRowLtHash: rowAccHex("row-b")},
		{TableID: "t", PartitionID: "P", PartName: "q_3", PartRowLtHash: rowAccHex("row-c")},
	}

	rootTwo := partitionRootFromActiveParts(twoParts, "P")
	rootThree := partitionRootFromActiveParts(threeParts, "P")
	if rootTwo != rootThree {
		t.Fatalf("partition root not re-pack invariant: two-part=%s three-part=%s", rootTwo, rootThree)
	}

	// The additive root must also equal a single accumulator over all rows.
	wantAll := rowAccHex("row-a", "row-b", "row-c")
	if rootTwo != wantAll {
		t.Fatalf("partition root != Σ rows: got %s want %s", rootTwo, wantAll)
	}

	// A different row set produces a different root.
	other := partitionRootFromActiveParts([]replay.PartManifestEntry{
		{TableID: "t", PartitionID: "P", PartName: "x", PartRowLtHash: rowAccHex("row-a", "row-b")},
	}, "P")
	if other == rootTwo {
		t.Fatalf("distinct row sets produced the same partition root")
	}
}

// TestMutationDeltaRootIsAdditive is the gap-31 invariant: the partition delta
// root is the additive difference post − base, so base + delta == post
// reconstructs the post commitment exactly.
func TestMutationDeltaRootIsAdditive(t *testing.T) {
	base := rowAccHex("row-a@100", "row-b@50")
	post := rowAccHex("row-a@110", "row-b@50") // UPDATE alice 100 -> 110

	deltaRoot, ok := subLtHashAccumulators(post, base)
	if !ok {
		t.Fatalf("expected additive delta over raw accumulators")
	}

	// base + delta must equal post.
	reconstructed, err := sumPartRowLtHashes([]string{base, deltaRoot})
	if err != nil {
		t.Fatalf("sum base+delta: %v", err)
	}
	if reconstructed != post {
		t.Fatalf("base + delta != post: got %s want %s", reconstructed, post)
	}

	// An empty base (INSERT-like) yields delta == post.
	zero := lthashAccumulatorHex(lthash.New())
	deltaFromZero, ok := subLtHashAccumulators(post, zero)
	if !ok || deltaFromZero != post {
		t.Fatalf("delta from zero base should equal post: ok=%v got=%s want=%s", ok, deltaFromZero, post)
	}
}

// TestCompactionLedgerEquationDetectsRowChange confirms ClickHouseCompactor
// fails closed when the compacted output no longer sums to the input parts'
// accumulator (a compaction that silently changed the row set), and passes when
// the same rows are merely re-packed.
func TestCompactionLedgerEquationDetectsRowChange(t *testing.T) {
	inputParts := []replay.PartManifestEntry{
		{TableID: "tenant.events", PartitionID: "202607", PartName: "in_1", PartRowLtHash: rowAccHex("r1")},
		{TableID: "tenant.events", PartitionID: "202607", PartName: "in_2", PartRowLtHash: rowAccHex("r2")},
	}

	compactTable := "`hg_compact`.`events_compact`"
	newCompactor := func(outputHash string) ClickHouseCompactor {
		return ClickHouseCompactor{
			Conn: &fakeSQLConn{},
			ActiveParts: &fakeActivePartReader{partsByTable: map[string][]replay.PartManifestEntry{
				compactTable: {{
					TableID:       "tenant.events",
					PartitionID:   "202607",
					PartName:      "out_merged",
					PartRowLtHash: outputHash,
					RowCount:      2,
				}},
			}},
			CompactDatabase: "hg_compact",
		}
	}
	task := CompactionTask{
		CompactionID: "c1",
		PromotionSeq: 1,
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		CompactTable: compactTable,
		PartitionIDs: []string{"202607"},
		InputParts:   inputParts,
	}

	// Honest compaction: output merges the same two rows → equation holds.
	if _, err := newCompactor(rowAccHex("r1", "r2")).Compact(context.Background(), task); err != nil {
		t.Fatalf("honest compaction rejected: %v", err)
	}

	// Fraudulent compaction: output dropped a row → equation violated.
	if _, err := newCompactor(rowAccHex("r1")).Compact(context.Background(), task); err == nil {
		t.Fatalf("expected ledger-equation violation for a compaction that changed the row set")
	}
}
