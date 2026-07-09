package storageintegrity

import (
	"context"
	"testing"

	"housegate/housegate/pkg/replay"
)

// baseFastScan builds a CachingPartScanner over fakes that returns the given
// base parts for hg_safe.events, so the mutation executor's base read is served
// by the fast path instead of the full-table hasher.
func baseFastScan(parts []PartDescriptor, fold map[string]replay.PartManifestEntry) *CachingPartScanner {
	return &CachingPartScanner{
		Inspector: &fakePartInspector{parts: map[string][]PartDescriptor{"hg_safe.events": parts}},
		Cache:     NewInMemoryPartLtHashCache(0),
		Scanner:   &fakeNamedPartReader{byName: fold},
		Schema:    fakeSchemaColumnReader{hash: "schema-1"},
		Source:    "mutation_base",
	}
}

func TestClickHouseMutationExecutorBaseScanFastPath(t *testing.T) {
	conn := &fakeSQLConn{}
	// Hasher returns ONLY the post (scratch) state — the base must come from the
	// fast path, so the hasher is called exactly once (for the scratch read).
	hasher := &fakeTableHasher{root: "post-root", parts: []ByteSidePart{{
		PartitionID: "202607", PartName: "scratch_p1", RowCount: 3, PartRowLtHash: rawAccumHex("post"),
	}}}
	// Base fast path: two physical parts in one partition; per-partition sum is
	// what feeds the base root / RowsBefore.
	baseParts := []PartDescriptor{
		{Database: "hg_safe", Table: "events", TableID: "hg_safe.events", PartitionID: "202607", PartName: "safe_p1", PartPhysHash: "phys-1", Rows: 3},
		{Database: "hg_safe", Table: "events", TableID: "hg_safe.events", PartitionID: "202607", PartName: "safe_p2", PartPhysHash: "phys-2", Rows: 2},
	}
	fold := map[string]replay.PartManifestEntry{
		"safe_p1": {TableID: "hg_safe.events", PartitionID: "202607", PartName: "safe_p1", RowCount: 3, PartRowLtHash: rawAccumHex("s1")},
		"safe_p2": {TableID: "hg_safe.events", PartitionID: "202607", PartName: "safe_p2", RowCount: 2, PartRowLtHash: rawAccumHex("s2")},
	}
	executor := ClickHouseMutationExecutor{
		Conn:            conn,
		Hasher:          hasher,
		BaseScan:        baseFastScan(baseParts, fold),
		ClaimSigner:     &fakeMutationClaimSigner{},
		WorkerID:        "worker-a",
		ScratchDatabase: "hg_mutation",
	}
	claim, err := executor.ExecuteMutation(context.Background(), MutationTask{
		StatementID:  "stmt-mut",
		TableID:      "tenant.events",
		MutationType: MutationTypeDelete,
		MutationSQL:  "ALTER TABLE `hg_safe`.`events` DELETE WHERE day = '2026-07-03'",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607"},
	})
	if err != nil {
		t.Fatalf("ExecuteMutation: %v", err)
	}
	// RowsBefore is the summed base rows (3+2), RowsAfter the post rows (3).
	if claim.RowsBefore != 5 || claim.RowsAfter != 3 {
		t.Fatalf("rows before/after = %d/%d, want 5/3 (base from fast path)", claim.RowsBefore, claim.RowsAfter)
	}
	// The base root is the additive sum of the two base parts.
	wantBaseRoot, err := sumPartRowLtHashes([]string{rawAccumHex("s1"), rawAccumHex("s2")})
	if err != nil {
		t.Fatalf("sum base parts: %v", err)
	}
	if len(claim.BasePartitionRoots) != 1 || claim.BasePartitionRoots[0].Root != wantBaseRoot {
		t.Fatalf("base partition roots = %+v, want additive sum %s", claim.BasePartitionRoots, wantBaseRoot)
	}
	// The hasher was consulted only for the scratch (post) read, NOT the base.
	if len(hasher.tables) != 1 || hasher.tables[0] != claim.ScratchTable {
		t.Fatalf("hasher tables = %+v, want only the scratch read %q (base served by fast path)", hasher.tables, claim.ScratchTable)
	}
}

func TestClickHouseMutationExecutorBaseScanActiveSetMismatchFailsClosed(t *testing.T) {
	conn := &fakeSQLConn{}
	hasher := &fakeTableHasher{root: "post-root", parts: []ByteSidePart{{
		PartitionID: "202607", PartName: "scratch_p1", RowCount: 3, PartRowLtHash: rawAccumHex("post"),
	}}}
	baseParts := []PartDescriptor{
		{Database: "hg_safe", Table: "events", TableID: "hg_safe.events", PartitionID: "202607", PartName: "safe_p1", PartPhysHash: "phys-1", Rows: 3},
	}
	fold := map[string]replay.PartManifestEntry{
		"safe_p1": {TableID: "hg_safe.events", PartitionID: "202607", PartName: "safe_p1", RowCount: 3, PartRowLtHash: rawAccumHex("s1")},
	}
	executor := ClickHouseMutationExecutor{
		Conn:            conn,
		Hasher:          hasher,
		BaseScan:        baseFastScan(baseParts, fold),
		ClaimSigner:     &fakeMutationClaimSigner{},
		WorkerID:        "worker-a",
		ScratchDatabase: "hg_mutation",
	}
	// The arbiter pinned a base root that does NOT match the local fast-path base
	// sum → the mutation must fail closed before running (spec §7.3 step 6).
	_, err := executor.ExecuteMutation(context.Background(), MutationTask{
		StatementID:  "stmt-mut",
		MutationType: MutationTypeDelete,
		MutationSQL:  "ALTER TABLE `hg_safe`.`events` DELETE WHERE day = '2026-07-03'",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607"},
		BasePartitionRoots: []replay.PartitionCommitment{
			{TableID: "hg_safe.events", PartitionID: "202607", Root: "0xdifferent-pinned-root"},
		},
	})
	if err == nil {
		t.Fatalf("expected fail-closed on base partition root mismatch")
	}
	// Fail-closed happens before any scratch DDL: no statement executed.
	if len(conn.execs) != 0 {
		t.Fatalf("no scratch DDL should run on base mismatch, got: %v", conn.execs)
	}
}
