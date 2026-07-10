package storageintegrity

import (
	"context"
	"fmt"
	"time"

	"housegate/housegate/pkg/replay"
)

const defaultWorkerPollInterval = 500 * time.Millisecond

func resolveArbiterWorkerClient(arbiter, legacy ArbiterWorkerClient) ArbiterWorkerClient {
	if arbiter != nil {
		return arbiter
	}
	return legacy
}

type VerifierWorker struct {
	WorkerID string
	Arbiter  ArbiterWorkerClient
	// Sequencer is the legacy control-plane field kept for compatibility.
	Sequencer       SequencerWorkerClient
	ReplayVerifier  ReplayVerifier
	ByteSideScanner ByteSideScanner
	PollInterval    time.Duration
}

func (w VerifierWorker) RunOnce(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	arbiter := resolveArbiterWorkerClient(w.Arbiter, w.Sequencer)
	if arbiter == nil {
		return false, fmt.Errorf("arbiter worker client is required")
	}
	if w.WorkerID == "" {
		return false, fmt.Errorf("worker_id is required")
	}
	didWork := false
	if w.ReplayVerifier != nil {
		job, ok, err := arbiter.ClaimReplayJob(ctx)
		if err != nil {
			return didWork, fmt.Errorf("claim replay job: %w", err)
		}
		if ok {
			att, err := w.ReplayVerifier.Verify(ctx, job)
			if err != nil {
				return didWork, fmt.Errorf("verify replay job %d: %w", job.BlockSeq, err)
			}
			if att.ReplicaID == "" {
				att.ReplicaID = w.WorkerID
			}
			if err := arbiter.SubmitReplayAttestation(ctx, att); err != nil {
				return didWork, fmt.Errorf("submit replay attestation: %w", err)
			}
			didWork = true
		}
	}
	if w.ByteSideScanner != nil {
		task, ok, err := arbiter.ClaimByteSideScan(ctx)
		if err != nil {
			return didWork, fmt.Errorf("claim byte-side scan: %w", err)
		}
		if ok {
			result, err := w.ByteSideScanner.ScanByteSide(ctx, task)
			if err != nil {
				return didWork, fmt.Errorf("byte-side scan %s: %w", task.ScanID, err)
			}
			if result.WorkerID == "" {
				result.WorkerID = w.WorkerID
			}
			if result.UnsafeBufferID == 0 {
				result.UnsafeBufferID = task.UnsafeBufferID
			}
			if result.UnsafeBufferEpoch == 0 {
				result.UnsafeBufferEpoch = task.UnsafeBufferEpoch
			}
			if result.UnsafeBufferDatabase == "" {
				result.UnsafeBufferDatabase = task.UnsafeBufferDatabase
			}
			if err := arbiter.SubmitByteSideScan(ctx, result); err != nil {
				return didWork, fmt.Errorf("submit byte-side scan: %w", err)
			}
			didWork = true
		}
	}
	return didWork, nil
}

func (w VerifierWorker) Run(ctx context.Context) error {
	return runWorkerLoop(ctx, w.PollInterval, w.RunOnce)
}

type PromotionWorker struct {
	WorkerID string
	Arbiter  ArbiterWorkerClient
	// Sequencer is the legacy control-plane field kept for compatibility.
	Sequencer    SequencerWorkerClient
	Promoter     Promoter
	PollInterval time.Duration
	// LeaderVerifier, when configured (Enabled), verifies the arbiter leader's
	// signature on every promotion task before executing (spec §9.1/§10,
	// HG-P0-03). nil / disabled skips the check unless RequireLeaderSignature.
	LeaderVerifier *LeaderSignatureVerifier
	// RequireLeaderSignature makes verification mandatory (protected mode): a
	// disabled verifier fails closed instead of skipping.
	RequireLeaderSignature bool
}

func (w PromotionWorker) RunOnce(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	arbiter := resolveArbiterWorkerClient(w.Arbiter, w.Sequencer)
	if arbiter == nil {
		return false, fmt.Errorf("arbiter worker client is required")
	}
	if w.Promoter == nil {
		return false, fmt.Errorf("promoter is required")
	}
	task, ok, err := arbiter.ClaimPromotion(ctx)
	if err != nil {
		return false, fmt.Errorf("claim promotion: %w", err)
	}
	if !ok {
		return false, nil
	}
	// Fail closed on an unauthorized publication before any physical mutation
	// (gap-25): if a leader key is configured, the task must carry a valid
	// leader signature over its canonical command.
	if err := ValidatePromotionLeaderSignature(w.LeaderVerifier, w.RequireLeaderSignature, task); err != nil {
		return false, err
	}
	if err := validatePromotionUnsafeBufferEpoch(ctx, arbiter, task); err != nil {
		return false, err
	}
	result, err := w.Promoter.Promote(ctx, task)
	if err != nil {
		return false, fmt.Errorf("promote %s: %w", task.PromotionID, err)
	}
	if result.PromotionID == "" {
		result.PromotionID = task.PromotionID
	}
	if result.PromotionSeq == 0 {
		result.PromotionSeq = task.PromotionSeq
	}
	if result.LeaseID == "" {
		result.LeaseID = task.LeaseID
	}
	if result.WorkerID == "" {
		result.WorkerID = w.WorkerID
	}
	if result.TableID == "" {
		result.TableID = task.TableID
	}
	if result.BaseSafeSnapshotID == "" {
		result.BaseSafeSnapshotID = task.BaseSafeSnapshotID
	}
	if result.BasePartitionRoot == "" {
		result.BasePartitionRoot = task.BasePartitionRoot
	}
	if result.SafeTable == "" {
		result.SafeTable = task.SafeTable
	}
	if result.SourceTable == "" {
		result.SourceTable = firstNonEmptyString(task.SourceTable, task.UnsafeTable)
	}
	if result.UnsafeBufferID == 0 {
		result.UnsafeBufferID = task.UnsafeBufferID
	}
	if result.UnsafeBufferEpoch == 0 {
		result.UnsafeBufferEpoch = task.UnsafeBufferEpoch
	}
	if result.UnsafeBufferDatabase == "" {
		result.UnsafeBufferDatabase = task.UnsafeBufferDatabase
	}
	if len(result.PartitionIDs) == 0 {
		result.PartitionIDs = append([]string(nil), task.PartitionIDs...)
	}
	if len(result.StatementIDs) == 0 {
		result.StatementIDs = append([]string(nil), task.StatementIDs...)
	}
	if len(result.CleanupUnsafeParts) == 0 {
		result.CleanupUnsafeParts = append([]ByteSidePart(nil), task.CleanupUnsafeParts...)
	}
	if _, err := arbiter.SubmitPromotionResult(ctx, result); err != nil {
		return false, fmt.Errorf("submit promotion result: %w", err)
	}
	return true, nil
}

func (w PromotionWorker) Run(ctx context.Context) error {
	return runWorkerLoop(ctx, w.PollInterval, w.RunOnce)
}

func validatePromotionUnsafeBufferEpoch(ctx context.Context, arbiter ArbiterWorkerClient, task PromotionTask) error {
	return validateUnsafeBufferEpoch(ctx, arbiter, "promotion", task.PromotionID, UnsafeBufferEpochCheckRequest{
		TableID:              task.TableID,
		UnsafeTable:          task.UnsafeTable,
		UnsafeBufferID:       task.UnsafeBufferID,
		UnsafeBufferEpoch:    task.UnsafeBufferEpoch,
		UnsafeBufferDatabase: task.UnsafeBufferDatabase,
	})
}

func validateRollbackUnsafeBufferEpoch(ctx context.Context, arbiter ArbiterWorkerClient, task RollbackTask) error {
	return validateUnsafeBufferEpoch(ctx, arbiter, "rollback", task.RollbackID, UnsafeBufferEpochCheckRequest{
		TableID:              task.TableID,
		UnsafeTable:          task.UnsafeTable,
		UnsafeBufferID:       task.UnsafeBufferID,
		UnsafeBufferEpoch:    task.UnsafeBufferEpoch,
		UnsafeBufferDatabase: task.UnsafeBufferDatabase,
	})
}

// validateUnsafeBufferEpoch confirms with the arbiter that the task still
// targets a live unsafe buffer epoch before mutating physical state. It is a
// no-op only when the task carries no unsafe-buffer semantics at all (legacy
// single-buffer configs): if either the epoch or the buffer database is set,
// the check is mandatory and fails closed when the arbiter cannot perform it.
func validateUnsafeBufferEpoch(ctx context.Context, arbiter ArbiterWorkerClient, kind, id string, req UnsafeBufferEpochCheckRequest) error {
	if req.UnsafeBufferEpoch == 0 && req.UnsafeBufferDatabase == "" {
		return nil
	}
	checker, ok := arbiter.(UnsafeBufferEpochChecker)
	if !ok {
		return fmt.Errorf("unsafe buffer epoch check is required for %s %s", kind, id)
	}
	decision, err := checker.CheckUnsafeBufferEpoch(ctx, req)
	if err != nil {
		return fmt.Errorf("unsafe buffer epoch check for %s %s: %w", kind, id, err)
	}
	if !decision.OK {
		reason := decision.Reason
		if reason == "" {
			reason = "arbiter rejected unsafe buffer epoch"
		}
		return fmt.Errorf("unsafe buffer epoch check for %s %s rejected: %s", kind, id, reason)
	}
	return nil
}

type MutationWorker struct {
	WorkerID string
	Arbiter  ArbiterWorkerClient
	// Sequencer is the legacy control-plane field kept for compatibility.
	Sequencer         SequencerWorkerClient
	Executor          MutationExecutor
	SnapshotReader    SnapshotReader
	MaxRebindAttempts int
	PollInterval      time.Duration
}

func (w MutationWorker) RunOnce(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	arbiter := resolveArbiterWorkerClient(w.Arbiter, w.Sequencer)
	if arbiter == nil {
		return false, fmt.Errorf("arbiter worker client is required")
	}
	if w.WorkerID == "" {
		return false, fmt.Errorf("worker_id is required")
	}
	if w.Executor == nil {
		return false, fmt.Errorf("mutation executor is required")
	}
	didWork := false
	task, ok, err := arbiter.ClaimMutationTask(ctx)
	if err != nil {
		return didWork, fmt.Errorf("claim mutation task: %w", err)
	}
	if ok {
		if task.PendingInsertBarrier {
			claim := w.pendingInsertBarrierClaim(task)
			if err := arbiter.SubmitMutationClaim(ctx, claim); err != nil {
				return didWork, fmt.Errorf("submit mutation barrier claim: %w", err)
			}
			didWork = true
		} else if claim, stale, err := w.staleRebindClaim(ctx, task); err != nil {
			return didWork, err
		} else if stale {
			if err := arbiter.SubmitMutationClaim(ctx, claim); err != nil {
				return didWork, fmt.Errorf("submit mutation stale-rebind claim: %w", err)
			}
			didWork = true
		} else {
			claim, err := w.Executor.ExecuteMutation(ctx, task)
			if err != nil {
				return didWork, fmt.Errorf("execute mutation %s: %w", task.StatementID, err)
			}
			if claim.WorkerID == "" {
				claim.WorkerID = w.WorkerID
			}
			fillMutationClaimDefaults(&claim, task)
			if err := arbiter.SubmitMutationClaim(ctx, claim); err != nil {
				return didWork, fmt.Errorf("submit mutation claim: %w", err)
			}
			didWork = true
		}
	}

	replayTask, ok, err := arbiter.ClaimMutationReplayTask(ctx)
	if err != nil {
		return didWork, fmt.Errorf("claim mutation replay task: %w", err)
	}
	if ok {
		if replayTask.PendingInsertBarrier {
			result := w.pendingInsertBarrierReplayResult(replayTask)
			if err := arbiter.SubmitMutationReplay(ctx, result); err != nil {
				return didWork, fmt.Errorf("submit mutation replay barrier result: %w", err)
			}
			didWork = true
		} else if result, stale, err := w.staleRebindReplayResult(ctx, replayTask); err != nil {
			return didWork, err
		} else if stale {
			if err := arbiter.SubmitMutationReplay(ctx, result); err != nil {
				return didWork, fmt.Errorf("submit mutation replay stale-rebind result: %w", err)
			}
			didWork = true
		} else {
			result, err := w.Executor.ReplayMutation(ctx, replayTask)
			if err != nil {
				return didWork, fmt.Errorf("replay mutation %s: %w", replayTask.StatementID, err)
			}
			if result.WorkerID == "" {
				result.WorkerID = w.WorkerID
			}
			fillMutationReplayDefaults(&result, replayTask)
			if err := arbiter.SubmitMutationReplay(ctx, result); err != nil {
				return didWork, fmt.Errorf("submit mutation replay: %w", err)
			}
			didWork = true
		}
	}
	return didWork, nil
}

func (w MutationWorker) Run(ctx context.Context) error {
	return runWorkerLoop(ctx, w.PollInterval, w.RunOnce)
}

func (w MutationWorker) pendingInsertBarrierClaim(task MutationTask) MutationClaim {
	claim := MutationClaim{
		StatementID:          task.StatementID,
		WorkerID:             w.WorkerID,
		Error:                fmt.Sprintf("pending INSERT barrier for mutation %s", task.StatementID),
		PendingInsertBarrier: true,
	}
	fillMutationClaimDefaults(&claim, task)
	return claim
}

func (w MutationWorker) pendingInsertBarrierReplayResult(task MutationTask) MutationReplayResult {
	result := MutationReplayResult{
		StatementID:          task.StatementID,
		WorkerID:             w.WorkerID,
		Error:                fmt.Sprintf("pending INSERT barrier for mutation replay %s", task.StatementID),
		PendingInsertBarrier: true,
	}
	fillMutationReplayDefaults(&result, task)
	return result
}

func (w MutationWorker) staleRebindClaim(ctx context.Context, task MutationTask) (MutationClaim, bool, error) {
	rebind, stale, err := w.staleRebind(ctx, task)
	if err != nil || !stale {
		return MutationClaim{}, false, err
	}
	claim := MutationClaim{
		StatementID:          task.StatementID,
		WorkerID:             w.WorkerID,
		Error:                rebind.reason,
		StaleRebind:          true,
		StaleReason:          rebind.reason,
		LatestSafeSnapshotID: rebind.latestSnapshotID,
		LatestStateRoot:      rebind.latestStateRoot,
		RebindCount:          task.RebindCount,
	}
	fillMutationClaimDefaults(&claim, task)
	return claim, true, nil
}

func (w MutationWorker) staleRebindReplayResult(ctx context.Context, task MutationTask) (MutationReplayResult, bool, error) {
	rebind, stale, err := w.staleRebind(ctx, task)
	if err != nil || !stale {
		return MutationReplayResult{}, false, err
	}
	result := MutationReplayResult{
		StatementID:          task.StatementID,
		WorkerID:             w.WorkerID,
		Error:                rebind.reason,
		StaleRebind:          true,
		StaleReason:          rebind.reason,
		LatestSafeSnapshotID: rebind.latestSnapshotID,
		LatestStateRoot:      rebind.latestStateRoot,
		RebindCount:          task.RebindCount,
	}
	fillMutationReplayDefaults(&result, task)
	return result, true, nil
}

type staleMutationRebind struct {
	reason           string
	latestSnapshotID string
	latestStateRoot  string
}

func (w MutationWorker) staleRebind(ctx context.Context, task MutationTask) (staleMutationRebind, bool, error) {
	if w.SnapshotReader == nil || task.BaseSafeSnapshotID == "" {
		return staleMutationRebind{}, false, nil
	}
	watermark, err := w.SnapshotReader.GetSafeWatermark(ctx)
	if err != nil {
		return staleMutationRebind{}, false, fmt.Errorf("get safe watermark for mutation rebind: %w", err)
	}
	if watermark.SnapshotID == "" || watermark.SnapshotID == task.BaseSafeSnapshotID {
		return staleMutationRebind{}, false, nil
	}
	reason := fmt.Sprintf("stale rebind required: task snapshot %s latest %s", task.BaseSafeSnapshotID, watermark.SnapshotID)
	if w.MaxRebindAttempts > 0 && task.RebindCount >= w.MaxRebindAttempts {
		reason = fmt.Sprintf("stale rebind attempts exceeded: task snapshot %s latest %s attempts %d max %d",
			task.BaseSafeSnapshotID, watermark.SnapshotID, task.RebindCount, w.MaxRebindAttempts)
	}
	return staleMutationRebind{
		reason:           reason,
		latestSnapshotID: watermark.SnapshotID,
		latestStateRoot:  watermark.StateRoot,
	}, true, nil
}

type SafeAuditWorker struct {
	WorkerID string
	Arbiter  ArbiterWorkerClient
	// Sequencer is the legacy control-plane field kept for compatibility.
	Sequencer    SequencerWorkerClient
	Auditor      SafeAuditor
	PollInterval time.Duration
}

func (w SafeAuditWorker) RunOnce(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	arbiter := resolveArbiterWorkerClient(w.Arbiter, w.Sequencer)
	if arbiter == nil {
		return false, fmt.Errorf("arbiter worker client is required")
	}
	if w.WorkerID == "" {
		return false, fmt.Errorf("worker_id is required")
	}
	if w.Auditor == nil {
		return false, fmt.Errorf("safe auditor is required")
	}
	task, ok, err := arbiter.ClaimSafeAudit(ctx)
	if err != nil {
		return false, fmt.Errorf("claim safe audit: %w", err)
	}
	if !ok {
		return false, nil
	}
	vote, err := w.Auditor.AuditSafe(ctx, task)
	if err != nil {
		return false, fmt.Errorf("safe audit %s: %w", task.AuditID, err)
	}
	if vote.WorkerID == "" {
		vote.WorkerID = w.WorkerID
	}
	if err := arbiter.SubmitSafeAudit(ctx, vote); err != nil {
		return false, fmt.Errorf("submit safe audit: %w", err)
	}
	return true, nil
}

func (w SafeAuditWorker) Run(ctx context.Context) error {
	return runWorkerLoop(ctx, w.PollInterval, w.RunOnce)
}

type RollbackWorker struct {
	WorkerID string
	Arbiter  ArbiterWorkerClient
	// Sequencer is the legacy control-plane field kept for compatibility.
	Sequencer    SequencerWorkerClient
	Executor     RollbackExecutor
	PollInterval time.Duration
}

func (w RollbackWorker) RunOnce(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	arbiter := resolveArbiterWorkerClient(w.Arbiter, w.Sequencer)
	if arbiter == nil {
		return false, fmt.Errorf("arbiter worker client is required")
	}
	if w.WorkerID == "" {
		return false, fmt.Errorf("worker_id is required")
	}
	if w.Executor == nil {
		return false, fmt.Errorf("rollback executor is required")
	}
	task, ok, err := arbiter.ClaimRollback(ctx)
	if err != nil {
		return false, fmt.Errorf("claim rollback: %w", err)
	}
	if !ok {
		return false, nil
	}
	if err := validateRollbackUnsafeBufferEpoch(ctx, arbiter, task); err != nil {
		return false, err
	}
	result, err := w.Executor.Rollback(ctx, task)
	if err != nil {
		return false, fmt.Errorf("rollback %s: %w", task.RollbackID, err)
	}
	fillRollbackResultDefaults(&result, task, w.WorkerID)
	if err := arbiter.SubmitRollback(ctx, result); err != nil {
		return false, fmt.Errorf("submit rollback result: %w", err)
	}
	return true, nil
}

func (w RollbackWorker) Run(ctx context.Context) error {
	return runWorkerLoop(ctx, w.PollInterval, w.RunOnce)
}

type RepairSyncWorker struct {
	WorkerID string
	Arbiter  ArbiterWorkerClient
	// Sequencer is the legacy control-plane field kept for compatibility.
	Sequencer    SequencerWorkerClient
	Executor     RepairSyncExecutor
	PollInterval time.Duration
}

func (w RepairSyncWorker) RunOnce(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	arbiter := resolveArbiterWorkerClient(w.Arbiter, w.Sequencer)
	if arbiter == nil {
		return false, fmt.Errorf("arbiter worker client is required")
	}
	if w.WorkerID == "" {
		return false, fmt.Errorf("worker_id is required")
	}
	if w.Executor == nil {
		return false, fmt.Errorf("repair/sync executor is required")
	}
	task, ok, err := arbiter.ClaimRepairSync(ctx)
	if err != nil {
		return false, fmt.Errorf("claim repair/sync: %w", err)
	}
	if !ok {
		return false, nil
	}
	result, err := w.Executor.RepairSync(ctx, task)
	if err != nil {
		return false, fmt.Errorf("repair/sync %s: %w", task.RepairID, err)
	}
	fillRepairSyncResultDefaults(&result, task, w.WorkerID)
	if err := arbiter.SubmitRepairSync(ctx, result); err != nil {
		return false, fmt.Errorf("submit repair/sync result: %w", err)
	}
	return true, nil
}

func (w RepairSyncWorker) Run(ctx context.Context) error {
	return runWorkerLoop(ctx, w.PollInterval, w.RunOnce)
}

type CompactionWorker struct {
	WorkerID string
	Arbiter  ArbiterWorkerClient
	// Sequencer is the legacy control-plane field kept for compatibility.
	Sequencer    SequencerWorkerClient
	Executor     CompactionExecutor
	PollInterval time.Duration
	// LeaderVerifier, when configured, verifies the arbiter leader's signature
	// on every compaction task before publishing (spec §8.1/§9.1, HG-P0-03).
	LeaderVerifier *LeaderSignatureVerifier
	// RequireLeaderSignature makes verification mandatory (protected mode).
	RequireLeaderSignature bool
}

func (w CompactionWorker) RunOnce(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	arbiter := resolveArbiterWorkerClient(w.Arbiter, w.Sequencer)
	if arbiter == nil {
		return false, fmt.Errorf("arbiter worker client is required")
	}
	if w.WorkerID == "" {
		return false, fmt.Errorf("worker_id is required")
	}
	if w.Executor == nil {
		return false, fmt.Errorf("compaction executor is required")
	}
	task, ok, err := arbiter.ClaimCompaction(ctx)
	if err != nil {
		return false, fmt.Errorf("claim compaction: %w", err)
	}
	if !ok {
		return false, nil
	}
	if err := ValidateCompactionLeaderSignature(w.LeaderVerifier, w.RequireLeaderSignature, task); err != nil {
		return false, err
	}
	result, err := w.Executor.Compact(ctx, task)
	if err != nil {
		return false, fmt.Errorf("compact %s: %w", task.CompactionID, err)
	}
	fillCompactionResultDefaults(&result, task, w.WorkerID)
	if err := arbiter.SubmitCompaction(ctx, result); err != nil {
		return false, fmt.Errorf("submit compaction result: %w", err)
	}
	return true, nil
}

func (w CompactionWorker) Run(ctx context.Context) error {
	return runWorkerLoop(ctx, w.PollInterval, w.RunOnce)
}

func fillMutationClaimDefaults(claim *MutationClaim, task MutationTask) {
	if claim.StatementID == "" {
		claim.StatementID = task.StatementID
	}
	if claim.BaseSafeSnapshotID == "" {
		claim.BaseSafeSnapshotID = task.BaseSafeSnapshotID
	}
	if claim.BasePartitionRoot == "" {
		claim.BasePartitionRoot = task.BasePartitionRoot
	}
	if len(claim.BasePartitionRoots) == 0 {
		claim.BasePartitionRoots = append([]replay.PartitionCommitment(nil), task.BasePartitionRoots...)
	}
	if claim.SchemaSnapshotID == "" {
		claim.SchemaSnapshotID = task.SchemaSnapshotID
	}
	if claim.PromotionSeq == 0 {
		claim.PromotionSeq = task.PromotionSeq
	}
}

func fillMutationReplayDefaults(result *MutationReplayResult, task MutationTask) {
	if result.StatementID == "" {
		result.StatementID = task.StatementID
	}
	if result.BaseSafeSnapshotID == "" {
		result.BaseSafeSnapshotID = task.BaseSafeSnapshotID
	}
	if result.BasePartitionRoot == "" {
		result.BasePartitionRoot = task.BasePartitionRoot
	}
	if len(result.BasePartitionRoots) == 0 {
		result.BasePartitionRoots = append([]replay.PartitionCommitment(nil), task.BasePartitionRoots...)
	}
	if result.SchemaSnapshotID == "" {
		result.SchemaSnapshotID = task.SchemaSnapshotID
	}
	if result.PromotionSeq == 0 {
		result.PromotionSeq = task.PromotionSeq
	}
}

func fillRollbackResultDefaults(result *RollbackResult, task RollbackTask, workerID string) {
	if result.RollbackID == "" {
		result.RollbackID = task.RollbackID
	}
	if result.StatementID == "" {
		result.StatementID = task.StatementID
	}
	if result.PromotionID == "" {
		result.PromotionID = task.PromotionID
	}
	if result.WorkerID == "" {
		result.WorkerID = workerID
	}
	if result.TableID == "" {
		result.TableID = task.TableID
	}
	if result.Reason == "" {
		result.Reason = task.Reason
	}
	if result.UnsafeTable == "" {
		result.UnsafeTable = task.UnsafeTable
	}
	if result.ScratchTable == "" {
		result.ScratchTable = task.ScratchTable
	}
	if result.PromoteTable == "" {
		result.PromoteTable = task.PromoteTable
	}
	if len(result.PartitionIDs) == 0 {
		result.PartitionIDs = append([]string(nil), task.PartitionIDs...)
	}
}

func fillRepairSyncResultDefaults(result *RepairSyncResult, task RepairSyncTask, workerID string) {
	if result.RepairID == "" {
		result.RepairID = task.RepairID
	}
	if result.WorkerID == "" {
		result.WorkerID = workerID
	}
	if result.SnapshotID == "" {
		result.SnapshotID = firstNonEmptyString(task.SnapshotID, task.Manifest.SnapshotID)
	}
	if result.TableID == "" {
		result.TableID = firstNonEmptyString(task.TableID, tableIDFromManifest(task.Manifest))
	}
	if result.SafeTable == "" {
		result.SafeTable = task.SafeTable
	}
	if result.SourceTable == "" {
		result.SourceTable = task.SourceTable
	}
	if len(result.PartitionIDs) == 0 {
		result.PartitionIDs = append([]string(nil), task.PartitionIDs...)
	}
	if result.ManifestRoot == "" {
		result.ManifestRoot = firstNonEmptyString(task.ExpectedManifestRoot, task.Manifest.ManifestRoot)
	}
}

func fillCompactionResultDefaults(result *CompactionResult, task CompactionTask, workerID string) {
	if result.CompactionID == "" {
		result.CompactionID = task.CompactionID
	}
	if result.PromotionSeq == 0 {
		result.PromotionSeq = task.PromotionSeq
	}
	if result.WorkerID == "" {
		result.WorkerID = workerID
	}
	if result.TableID == "" {
		result.TableID = task.TableID
	}
	if result.SafeTable == "" {
		result.SafeTable = task.SafeTable
	}
	if result.CompactTable == "" {
		result.CompactTable = task.CompactTable
	}
	if result.BaseSafeSnapshotID == "" {
		result.BaseSafeSnapshotID = task.BaseSafeSnapshotID
	}
	if result.BasePartitionRoot == "" {
		result.BasePartitionRoot = task.BasePartitionRoot
	}
	if result.ExpectedPostRoot == "" {
		result.ExpectedPostRoot = task.ExpectedPostRoot
	}
	if len(result.PartitionIDs) == 0 {
		result.PartitionIDs = append([]string(nil), task.PartitionIDs...)
	}
}

func runWorkerLoop(ctx context.Context, pollInterval time.Duration, runOnce func(context.Context) (bool, error)) error {
	if pollInterval <= 0 {
		pollInterval = defaultWorkerPollInterval
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		didWork, err := runOnce(ctx)
		if err != nil {
			return err
		}
		delay := time.Duration(0)
		if !didWork {
			delay = pollInterval
		}
		timer.Reset(delay)
	}
}
