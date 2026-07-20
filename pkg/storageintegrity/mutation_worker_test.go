package storageintegrity

import (
	"context"
	"testing"
)

// workerTaskFixture is a task over one affected partition pinned to snapshot
// "safe-1" with base root "base-p1".
func workerTaskFixture() MutationTask {
	return MutationTask{
		ContractVersion:    MutationContractVersion,
		MutationID:         "m-1",
		StatementID:        "m-1",
		StatementKind:      KindUpdate,
		PrevSafeSnapshotID: "safe-1",
		SchemaSnapshotID:   "schema-1",
		ExecutorProfileID:  "profile-1",
		AffectedPartitions: []AffectedPartition{{TableID: "net1.events", PartitionID: "p1"}},
		BasePartitionRoots: []PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Commitment: "base-p1"}},
		MaterializedSQL:    "ALTER TABLE net1.events UPDATE v = 1 WHERE p = 'p1'",
	}
}

// cleanView is the current snapshot matching the pinned base with no pending
// inserts.
func cleanView() SnapshotView {
	return SnapshotView{
		SnapshotID:     "safe-1",
		PartitionRoots: []PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Commitment: "base-p1"}},
	}
}

// --- Green-today: pure pending/stale/rebind decision logic. ---

func TestStaleAgainst_SnapshotIdAdvance(t *testing.T) {
	v := cleanView()
	v.SnapshotID = "safe-2"
	if stale, _ := StaleAgainst(workerTaskFixture(), v); !stale {
		t.Fatal("a snapshot-id advance must be stale")
	}
}

func TestStaleAgainst_BaseRootChanged(t *testing.T) {
	v := cleanView()
	v.PartitionRoots = []PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Commitment: "changed"}}
	if stale, _ := StaleAgainst(workerTaskFixture(), v); !stale {
		t.Fatal("a changed base root must be stale")
	}
}

func TestStaleAgainst_MissingAffectedPartition(t *testing.T) {
	v := cleanView()
	v.PartitionRoots = nil
	if stale, _ := StaleAgainst(workerTaskFixture(), v); !stale {
		t.Fatal("a missing affected partition must be treated as stale (fail closed)")
	}
}

func TestStaleAgainst_IdenticalIsNotStale(t *testing.T) {
	if stale, reason := StaleAgainst(workerTaskFixture(), cleanView()); stale {
		t.Fatalf("identical base must not be stale: %s", reason)
	}
}

func TestPendingInsertBlocks_UnresolvedEarlierInsertBlocks(t *testing.T) {
	v := cleanView()
	v.PendingInserts = []PendingInsert{{StatementID: "i-1", TableID: "net1.events", PartitionID: "p1", Resolved: false}}
	if blocks, _ := PendingInsertBlocks(workerTaskFixture(), v); !blocks {
		t.Fatal("an unresolved same-partition INSERT must block")
	}
}

func TestPendingInsertBlocks_ResolvedInsertDoesNotBlock(t *testing.T) {
	v := cleanView()
	v.PendingInserts = []PendingInsert{{StatementID: "i-1", TableID: "net1.events", PartitionID: "p1", Resolved: true}}
	if blocks, _ := PendingInsertBlocks(workerTaskFixture(), v); blocks {
		t.Fatal("a resolved INSERT must not block")
	}
}

func TestPendingInsertBlocks_DifferentPartitionDoesNotBlock(t *testing.T) {
	v := cleanView()
	v.PendingInserts = []PendingInsert{{StatementID: "i-1", TableID: "net1.events", PartitionID: "p9", Resolved: false}}
	if blocks, _ := PendingInsertBlocks(workerTaskFixture(), v); blocks {
		t.Fatal("an unresolved INSERT on a non-affected partition must not block")
	}
}

func TestDecideRebind_PendingInsertBeforeExecuteBlocks(t *testing.T) {
	before := cleanView()
	before.PendingInserts = []PendingInsert{{StatementID: "i-1", TableID: "net1.events", PartitionID: "p1", Resolved: false}}
	d, _ := DecideRebind(workerTaskFixture(), before, cleanView(), false)
	if d != DecisionBlockPendingInsert {
		t.Fatalf("pending insert before execute must block, got %s", d)
	}
}

func TestDecideRebind_StaleBeforeExecuteRebinds(t *testing.T) {
	before := cleanView()
	before.SnapshotID = "safe-2"
	d, _ := DecideRebind(workerTaskFixture(), before, cleanView(), false)
	if d != DecisionRebind {
		t.Fatalf("stale before execute must rebind, got %s", d)
	}
}

func TestDecideRebind_StaleAfterExecuteSupersedes(t *testing.T) {
	after := cleanView()
	after.SnapshotID = "safe-2"
	d, _ := DecideRebind(workerTaskFixture(), cleanView(), after, false)
	if d != DecisionSupersede {
		t.Fatalf("stale after execute (not applied) must supersede, got %s", d)
	}
}

func TestDecideRebind_LocallyAppliedStaleAfterCannotRebind(t *testing.T) {
	after := cleanView()
	after.SnapshotID = "safe-2"
	d, _ := DecideRebind(workerTaskFixture(), cleanView(), after, true)
	if d != DecisionLocallyAppliedNoRebind {
		t.Fatalf("locally-applied worker with stale after must not rebind, got %s", d)
	}
}

func TestDecideRebind_CleanBeforeAndAfterProceeds(t *testing.T) {
	d, _ := DecideRebind(workerTaskFixture(), cleanView(), cleanView(), false)
	if d != DecisionProceed {
		t.Fatalf("clean before and after must proceed, got %s", d)
	}
}

// --- Gated: the worker's execute+submit path needs the absent C2 seam. ---

type fakeSnapshotQuerier struct {
	views []SnapshotView
	calls int
}

func (q *fakeSnapshotQuerier) QuerySnapshot(_ context.Context, _ []PartitionCommitment) (SnapshotView, error) {
	idx := q.calls
	if idx >= len(q.views) {
		idx = len(q.views) - 1
	}
	q.calls++
	return q.views[idx], nil
}

type fakeWorkerExecutor struct {
	res    MutationExecuteResult
	called bool
}

func (e *fakeWorkerExecutor) CloneAndExecute(_ context.Context, _ MutationTask) (MutationExecuteResult, error) {
	e.called = true
	return e.res, nil
}

func TestMutationWorker_Run_ProceedReturnsClaim(t *testing.T) {
	requireCompanionMutationConsensus(t)

	q := &fakeSnapshotQuerier{views: []SnapshotView{cleanView(), cleanView()}}
	exec := &fakeWorkerExecutor{res: MutationExecuteResult{Claim: SignedMutationClaim{ClaimHash: "h"}, LocallyApplied: true}}
	w := NewMutationWorker("w-1", q, exec)
	res, err := w.Run(context.Background(), workerTaskFixture())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Decision != DecisionProceed || res.Claim.ClaimHash == "" {
		t.Fatalf("clean run must proceed with a claim, got %s claim=%+v", res.Decision, res.Claim)
	}
	if q.calls != 2 {
		t.Fatalf("snapshot must be queried before AND after, got %d queries", q.calls)
	}
}

func TestMutationWorker_Run_PendingInsertBlocksBeforeExecute(t *testing.T) {
	requireCompanionMutationConsensus(t)

	before := cleanView()
	before.PendingInserts = []PendingInsert{{StatementID: "i-1", TableID: "net1.events", PartitionID: "p1", Resolved: false}}
	q := &fakeSnapshotQuerier{views: []SnapshotView{before, cleanView()}}
	exec := &fakeWorkerExecutor{}
	w := NewMutationWorker("w-1", q, exec)
	res, err := w.Run(context.Background(), workerTaskFixture())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Decision != DecisionBlockPendingInsert {
		t.Fatalf("expected block, got %s", res.Decision)
	}
	if exec.called {
		t.Fatal("blocked mutation must NOT call the executor")
	}
}

func TestMutationWorker_Run_SupersededClaimNotReturned(t *testing.T) {
	requireCompanionMutationConsensus(t)

	after := cleanView()
	after.SnapshotID = "safe-2"
	q := &fakeSnapshotQuerier{views: []SnapshotView{cleanView(), after}}
	exec := &fakeWorkerExecutor{res: MutationExecuteResult{Claim: SignedMutationClaim{ClaimHash: "h"}, LocallyApplied: false}}
	w := NewMutationWorker("w-1", q, exec)
	res, err := w.Run(context.Background(), workerTaskFixture())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Decision != DecisionSupersede {
		t.Fatalf("expected supersede, got %s", res.Decision)
	}
	if res.Claim.ClaimHash != "" {
		t.Fatal("a superseded result must carry no claim")
	}
}
