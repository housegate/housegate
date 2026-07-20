package storageintegrity

import (
	"context"
	"fmt"
)

// SnapshotView is the result of re-querying the current safe snapshot for a
// mutation's affected partitions: the current snapshot id, the current base
// commitments for those partitions, and any not-yet-terminal same-partition
// INSERTs. The worker queries it before AND after execution to detect a base
// advance or a blocking pending write (design section 4.5).
type SnapshotView struct {
	SnapshotID     string
	PartitionRoots []PartitionCommitment
	PendingInserts []PendingInsert
}

// PendingInsert is an earlier same-partition INSERT. Resolved is true once it
// reached Safe or Rejected; an unresolved one blocks a mutation on that
// partition (design section 4.5: the mutation stays in the pending write queue
// and must not execute on a snapshot that cannot see the INSERT).
type PendingInsert struct {
	StatementID string
	TableID     string
	PartitionID string
	Resolved    bool
}

// MutationDecision is the pure verdict of the pending/stale/rebind state machine
// (design section 4.5).
type MutationDecision int

const (
	// DecisionProceed: no pending insert and no base advance — execute.
	DecisionProceed MutationDecision = iota
	// DecisionBlockPendingInsert: an unresolved same-partition INSERT blocks the
	// mutation; it stays in the pending write queue and never executes.
	DecisionBlockPendingInsert
	// DecisionRebind: the base advanced before execution — rebind to the latest
	// manifest and re-clone; never runs the executor.
	DecisionRebind
	// DecisionSupersede: the base advanced after execution and the worker had NOT
	// locally applied — drop the scratch and claim, do not submit.
	DecisionSupersede
	// DecisionLocallyAppliedNoRebind: the base advanced after execution but the
	// worker already locally applied — it may not be treated as un-executed; it
	// stays unserviceable until a publication cut accepts its readback or a repair
	// (design section 4.5).
	DecisionLocallyAppliedNoRebind
)

func (d MutationDecision) String() string {
	switch d {
	case DecisionProceed:
		return "Proceed"
	case DecisionBlockPendingInsert:
		return "BlockPendingInsert"
	case DecisionRebind:
		return "Rebind"
	case DecisionSupersede:
		return "Supersede"
	case DecisionLocallyAppliedNoRebind:
		return "LocallyAppliedNoRebind"
	default:
		return "Unknown"
	}
}

// MutationWorkerResult is the worker's outcome for one task. Claim is populated
// only when Decision is DecisionProceed and the gated executor produced one; a
// blocked/rebind/superseded result carries no claim.
type MutationWorkerResult struct {
	MutationID string
	Decision   MutationDecision
	Reason     string
	Claim      SignedMutationClaim
}

// SnapshotQuerier re-reads the current safe snapshot for the affected
// partitions. It is a read-only local snapshot reader (a real adapter projects a
// replay.SafeSnapshotManifest into a SnapshotView), NOT the mutation-consensus
// seam — so the pure decision logic it feeds runs green today.
type SnapshotQuerier interface {
	QuerySnapshot(ctx context.Context, roots []PartitionCommitment) (SnapshotView, error)
}

// MutationExecuteResult is what the gated scratch execution reports: the signed
// claim and whether the worker locally applied the mutation (which changes how a
// later stale observation is handled).
type MutationExecuteResult struct {
	Claim          SignedMutationClaim
	LocallyApplied bool
}

// StaleAgainst reports whether a task's pinned base is stale relative to the
// current snapshot view: the snapshot id advanced, an affected partition's base
// root changed, or an affected partition is missing from the current view
// (which cannot be validated, so it is treated as stale — fail closed). This is
// the design-section-4.5 base-advance detector.
func StaleAgainst(base MutationTask, current SnapshotView) (bool, string) {
	if base.PrevSafeSnapshotID != current.SnapshotID {
		return true, fmt.Sprintf("snapshot advanced: pinned %s, current %s", base.PrevSafeSnapshotID, current.SnapshotID)
	}
	currentRoots := map[string]string{}
	for _, c := range current.PartitionRoots {
		currentRoots[c.TableID+"/"+c.PartitionID] = c.Commitment
	}
	for _, pinned := range base.BasePartitionRoots {
		key := pinned.TableID + "/" + pinned.PartitionID
		cur, ok := currentRoots[key]
		if !ok {
			return true, fmt.Sprintf("affected partition %s missing from current snapshot", key)
		}
		if cur != pinned.Commitment {
			return true, fmt.Sprintf("base root for %s changed: pinned %s, current %s", key, pinned.Commitment, cur)
		}
	}
	return false, ""
}

// PendingInsertBlocks reports whether an unresolved earlier same-partition
// INSERT blocks the task (design section 4.5).
func PendingInsertBlocks(task MutationTask, current SnapshotView) (bool, string) {
	affected := map[string]bool{}
	for _, ap := range task.AffectedPartitions {
		affected[ap.TableID+"/"+ap.PartitionID] = true
	}
	for _, pi := range current.PendingInserts {
		if pi.Resolved {
			continue
		}
		if affected[pi.TableID+"/"+pi.PartitionID] {
			return true, fmt.Sprintf("unresolved INSERT %s on affected partition %s/%s", pi.StatementID, pi.TableID, pi.PartitionID)
		}
	}
	return false, ""
}

// DecideRebind is the pure pending/stale/rebind state machine (design section
// 4.5). It is evaluated with the before-view, and, once execution has run, the
// after-view and whether the worker locally applied.
func DecideRebind(task MutationTask, before SnapshotView, after SnapshotView, locallyApplied bool) (MutationDecision, string) {
	if blocks, reason := PendingInsertBlocks(task, before); blocks {
		return DecisionBlockPendingInsert, reason
	}
	if stale, reason := StaleAgainst(task, before); stale {
		return DecisionRebind, "stale before execute: " + reason
	}
	if stale, reason := StaleAgainst(task, after); stale {
		if locallyApplied {
			return DecisionLocallyAppliedNoRebind, "stale after execute but locally applied: " + reason
		}
		return DecisionSupersede, "stale after execute: " + reason
	}
	return DecisionProceed, ""
}

// MutationWorker runs one worker's mutation task: it re-queries the snapshot
// before and after the gated scratch execution and applies the section-4.5
// decision, dropping any claim computed against a superseded base and never
// treating a locally-applied worker as un-executed. It holds no Arbiter FSM
// state and makes no quorum/publication decision.
type MutationWorker struct {
	workerID string
	querier  SnapshotQuerier
	exec     WorkerScratchExecutor
}

// WorkerScratchExecutor is the gated port the worker drives to clone the frozen
// base, execute the mutation, and produce a signed claim (plus whether it
// locally applied). No real implementation exists; see
// CompanionMutationConsensusAvailable.
type WorkerScratchExecutor interface {
	CloneAndExecute(ctx context.Context, task MutationTask) (MutationExecuteResult, error)
}

// NewMutationWorker constructs a worker over the snapshot querier and gated
// scratch executor.
func NewMutationWorker(workerID string, q SnapshotQuerier, e WorkerScratchExecutor) *MutationWorker {
	return &MutationWorker{workerID: workerID, querier: q, exec: e}
}

// Run drives one task through the section-4.5 flow: query before → block/rebind
// if the before-view blocks or is stale (never touching the executor) →
// otherwise clone+execute via the gated port → query after → decide
// proceed/supersede/locally-applied. Only a Proceed result carries the claim; a
// superseded/blocked/rebind result carries none.
func (w *MutationWorker) Run(ctx context.Context, task MutationTask) (MutationWorkerResult, error) {
	before, err := w.querier.QuerySnapshot(ctx, task.BasePartitionRoots)
	if err != nil {
		return MutationWorkerResult{MutationID: task.MutationID}, fmt.Errorf("mutation worker: query before: %w", err)
	}
	if blocks, reason := PendingInsertBlocks(task, before); blocks {
		return MutationWorkerResult{MutationID: task.MutationID, Decision: DecisionBlockPendingInsert, Reason: reason}, nil
	}
	if stale, reason := StaleAgainst(task, before); stale {
		return MutationWorkerResult{MutationID: task.MutationID, Decision: DecisionRebind, Reason: "stale before execute: " + reason}, nil
	}

	if w.exec == nil {
		return MutationWorkerResult{MutationID: task.MutationID}, fmt.Errorf("mutation worker: no scratch executor wired")
	}
	execRes, err := w.exec.CloneAndExecute(ctx, task)
	if err != nil {
		return MutationWorkerResult{MutationID: task.MutationID}, fmt.Errorf("mutation worker: execute: %w", err)
	}

	after, err := w.querier.QuerySnapshot(ctx, task.BasePartitionRoots)
	if err != nil {
		return MutationWorkerResult{MutationID: task.MutationID}, fmt.Errorf("mutation worker: query after: %w", err)
	}
	decision, reason := DecideRebind(task, before, after, execRes.LocallyApplied)
	res := MutationWorkerResult{MutationID: task.MutationID, Decision: decision, Reason: reason}
	if decision == DecisionProceed {
		res.Claim = execRes.Claim
	}
	// Supersede / LocallyAppliedNoRebind / (already-handled block/rebind) carry no
	// submitted claim.
	return res, nil
}
