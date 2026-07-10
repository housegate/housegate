package storageintegrity

import (
	"context"
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/replay"
)

func TestVerifierWorkerRunOnceClaimsReplayAndByteSideTasks(t *testing.T) {
	seq := &fakeWorkerSequencer{
		replayJob: replay.ReplayJob{
			BlockSeq:           7,
			PrevSafeSnapshotID: "snap",
			PrevStateRoot:      "prev",
			SchemaSnapshotID:   "schema",
			ExecutorProfileID:  "exec",
			SourceClaimRoot:    "root",
			Statements: []replay.Statement{{
				StatementID:   "stmt-1",
				StatementSeq:  1,
				SQL:           "INSERT INTO t",
				SQLHash:       replay.DigestString("INSERT INTO t"),
				SettingsHash:  "settings",
				TargetTableID: "tenant.t",
			}},
		},
		replayOK: true,
		byteTask: ByteSideScanTask{
			ScanID:      "scan-1",
			StatementID: "stmt-1",
			TableID:     "tenant.t",
			UnsafeTable: "`hg_unsafe`.`t`",
		},
		byteOK: true,
	}
	verifier := &fakeReplayVerifier{att: replay.ReplayAttestation{
		ReplicaID: "replica-a",
		Receipt: replay.ExecutionReceipt{
			BlockSeq:          7,
			SourceClaimRoot:   "root",
			ComputedStateRoot: "root",
			MatchSourceRoot:   true,
		},
		ReceiptHash:     "receipt",
		Signature:       "sig",
		MatchSourceRoot: true,
	}}
	scanner := &fakeByteScanner{result: ByteSideScanResult{
		ScanID:      "scan-1",
		StatementID: "stmt-1",
		TableID:     "tenant.t",
		UnsafeTable: "`hg_unsafe`.`t`",
		Parts: []ByteSidePart{{
			PartitionID:   "all",
			PartName:      "all_1_1_0",
			RowCount:      2,
			PartRowLtHash: "0xpart",
		}},
	}}
	worker := VerifierWorker{
		WorkerID:        "replica-a",
		Sequencer:       seq,
		ReplayVerifier:  verifier,
		ByteSideScanner: scanner,
	}

	didWork, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !didWork {
		t.Fatal("RunOnce didWork=false, want true")
	}
	if verifier.job.BlockSeq != 7 || seq.submittedReplay.ReceiptHash != "receipt" {
		t.Fatalf("replay path not executed: verifier job=%+v submitted=%+v", verifier.job, seq.submittedReplay)
	}
	if scanner.task.ScanID != "scan-1" || seq.submittedByte.WorkerID != "replica-a" {
		t.Fatalf("byte-side path not executed: scanner task=%+v submitted=%+v", scanner.task, seq.submittedByte)
	}
}

func TestPromotionWorkerRunOnceExecutesClaimedPromotion(t *testing.T) {
	seq := &fakeWorkerSequencer{
		promotionTask: PromotionTask{
			PromotionID:      "promotion-stmt-1",
			TableID:          "tenant.events",
			Kind:             "mutation",
			SafeTable:        "`hg_safe`.`events`",
			SourceTable:      "`hg_mutation`.`events_stmt_1`",
			ReplacePartition: true,
			PartitionIDs:     []string{"202607"},
			StatementIDs:     []string{"stmt-1"},
		},
		promotionOK: true,
	}
	promoter := &fakePromoter{}
	worker := PromotionWorker{WorkerID: "promoter-a", Sequencer: seq, Promoter: promoter}

	didWork, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !didWork {
		t.Fatal("RunOnce didWork=false, want true")
	}
	if promoter.task.PromotionID != "promotion-stmt-1" || !promoter.task.ReplacePartition {
		t.Fatalf("promotion task = %+v", promoter.task)
	}
	if seq.submittedPromotion.PromotionID != "promotion-stmt-1" ||
		seq.submittedPromotion.WorkerID != "promoter-a" ||
		len(seq.submittedPromotion.ActiveParts) != 1 ||
		seq.submittedPromotion.ActiveParts[0].PartName != "p_1_1_0" {
		t.Fatalf("promotion result = %+v", seq.submittedPromotion)
	}
}

func TestPromotionWorkerRejectsStaleUnsafeBufferEpoch(t *testing.T) {
	seq := &fakeWorkerSequencer{
		promotionTask: PromotionTask{
			PromotionID:          "promotion-stmt-1",
			TableID:              "tenant.events",
			UnsafeTable:          "`hg_unsafe_0`.`events`",
			SafeTable:            "`hg_safe`.`events`",
			UnsafeBufferID:       0,
			UnsafeBufferEpoch:    17,
			UnsafeBufferDatabase: "hg_unsafe_0",
			PartitionIDs:         []string{"202607"},
		},
		promotionOK: true,
		unsafeBufferDecision: UnsafeBufferEpochDecision{
			OK:     false,
			Reason: "epoch 17 is frozen/stale",
		},
	}
	promoter := &fakePromoter{}
	worker := PromotionWorker{WorkerID: "promoter-a", Sequencer: seq, Promoter: promoter}

	didWork, err := worker.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unsafe buffer epoch") {
		t.Fatalf("RunOnce error = %v, want unsafe buffer epoch rejection", err)
	}
	if didWork {
		t.Fatal("RunOnce didWork=true after stale epoch rejection")
	}
	if promoter.task.PromotionID != "" {
		t.Fatalf("promotion executed despite stale unsafe buffer epoch: %+v", promoter.task)
	}
	if seq.unsafeBufferCheck.TableID != "tenant.events" ||
		seq.unsafeBufferCheck.UnsafeBufferID != 0 ||
		seq.unsafeBufferCheck.UnsafeBufferEpoch != 17 ||
		seq.unsafeBufferCheck.UnsafeBufferDatabase != "hg_unsafe_0" {
		t.Fatalf("unsafe buffer epoch check = %+v", seq.unsafeBufferCheck)
	}
}

func TestMutationWorkerRunOnceSubmitsClaimAndReplayResult(t *testing.T) {
	seq := &fakeWorkerSequencer{
		mutationTask: MutationTask{
			StatementID:  "stmt-mut",
			TableID:      "tenant.events",
			MutationType: MutationTypeUpdate,
			MutationSQL:  "ALTER TABLE `hg_safe`.`events` UPDATE label = 'b' WHERE day = '2026-07-03'",
			SafeTable:    "`hg_safe`.`events`",
			PartitionIDs: []string{"202607"},
		},
		mutationOK: true,
		mutationReplayTask: MutationTask{
			StatementID:  "stmt-mut",
			TableID:      "tenant.events",
			MutationType: MutationTypeUpdate,
			MutationSQL:  "ALTER TABLE `hg_safe`.`events` UPDATE label = 'b' WHERE day = '2026-07-03'",
			SafeTable:    "`hg_safe`.`events`",
			PartitionIDs: []string{"202607"},
		},
		mutationReplayOK: true,
	}
	executor := &fakeMutationExecutor{
		claim: MutationClaim{
			StatementID:   "stmt-mut",
			ScratchTable:  "`hg_mutation`.`events_stmt_mut_worker_a`",
			PostStateRoot: "root-mut",
		},
		replay: MutationReplayResult{
			StatementID:   "stmt-mut",
			PostStateRoot: "root-mut",
		},
	}
	worker := MutationWorker{WorkerID: "worker-a", Sequencer: seq, Executor: executor}

	didWork, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !didWork {
		t.Fatal("RunOnce didWork=false, want true")
	}
	if seq.submittedMutationClaim.WorkerID != "worker-a" || seq.submittedMutationClaim.PostStateRoot != "root-mut" {
		t.Fatalf("mutation claim = %+v", seq.submittedMutationClaim)
	}
	if seq.submittedMutationReplay.WorkerID != "worker-a" || seq.submittedMutationReplay.PostStateRoot != "root-mut" {
		t.Fatalf("mutation replay = %+v", seq.submittedMutationReplay)
	}
}

func TestMutationWorkerRejectsPendingInsertBarrierTask(t *testing.T) {
	seq := &fakeWorkerSequencer{
		mutationTask: MutationTask{
			StatementID:          "stmt-mut-barrier",
			TableID:              "tenant.events",
			MutationType:         MutationTypeDelete,
			MutationSQL:          "ALTER TABLE `hg_safe`.`events` DELETE WHERE day = '2026-07-03'",
			SafeTable:            "`hg_safe`.`events`",
			PartitionIDs:         []string{"202607"},
			PendingInsertBarrier: true,
		},
		mutationOK: true,
	}
	executor := &fakeMutationExecutor{}
	worker := MutationWorker{WorkerID: "worker-a", Sequencer: seq, Executor: executor}

	didWork, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !didWork {
		t.Fatal("RunOnce didWork=false, want barrier claim submission")
	}
	if executor.task.StatementID != "" {
		t.Fatalf("mutation executed despite barrier: executor=%+v", executor.task)
	}
	if seq.submittedMutationClaim.StatementID != "stmt-mut-barrier" ||
		!seq.submittedMutationClaim.PendingInsertBarrier ||
		!strings.Contains(seq.submittedMutationClaim.Error, "pending INSERT barrier") {
		t.Fatalf("barrier claim = %+v", seq.submittedMutationClaim)
	}
}

func TestMutationWorkerReportsStaleRebindInsteadOfExecuting(t *testing.T) {
	seq := &fakeWorkerSequencer{
		mutationTask: MutationTask{
			StatementID:        "stmt-mut-stale",
			TableID:            "tenant.events",
			MutationType:       MutationTypeUpdate,
			MutationSQL:        "ALTER TABLE `hg_safe`.`events` UPDATE label = 'b' WHERE day = '2026-07-03'",
			SafeTable:          "`hg_safe`.`events`",
			BaseSafeSnapshotID: "snap-old",
			PartitionIDs:       []string{"202607"},
			RebindCount:        1,
		},
		mutationOK: true,
		watermark:  SafeWatermark{SnapshotID: "snap-new", SafeL3BlockSeq: 12, StateRoot: "root-new"},
	}
	executor := &fakeMutationExecutor{}
	worker := MutationWorker{
		WorkerID:          "worker-a",
		Sequencer:         seq,
		Executor:          executor,
		SnapshotReader:    seq,
		MaxRebindAttempts: 3,
	}

	didWork, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !didWork {
		t.Fatal("RunOnce didWork=false, want stale rebind claim submission")
	}
	if executor.task.StatementID != "" {
		t.Fatalf("mutation executed despite stale rebind: executor=%+v", executor.task)
	}
	if seq.submittedMutationClaim.StatementID != "stmt-mut-stale" ||
		!seq.submittedMutationClaim.StaleRebind ||
		seq.submittedMutationClaim.LatestSafeSnapshotID != "snap-new" ||
		seq.submittedMutationClaim.LatestStateRoot != "root-new" ||
		seq.submittedMutationClaim.RebindCount != 1 ||
		!strings.Contains(seq.submittedMutationClaim.Error, "stale rebind") {
		t.Fatalf("stale rebind claim = %+v", seq.submittedMutationClaim)
	}
}

func TestMutationWorkerReportsReplayBarrier(t *testing.T) {
	seq := &fakeWorkerSequencer{
		mutationReplayTask: MutationTask{
			StatementID:          "stmt-replay-barrier",
			TableID:              "tenant.events",
			MutationType:         MutationTypeDelete,
			MutationSQL:          "ALTER TABLE `hg_safe`.`events` DELETE WHERE day = '2026-07-03'",
			SafeTable:            "`hg_safe`.`events`",
			BaseSafeSnapshotID:   "snap-old",
			PartitionIDs:         []string{"202607"},
			PendingInsertBarrier: true,
		},
		mutationReplayOK: true,
	}
	executor := &fakeMutationExecutor{}
	worker := MutationWorker{WorkerID: "worker-a", Sequencer: seq, Executor: executor}

	didWork, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !didWork {
		t.Fatal("RunOnce didWork=false, want replay barrier result submission")
	}
	if executor.replayTask.StatementID != "" {
		t.Fatalf("mutation replay executed despite barrier: executor=%+v", executor.replayTask)
	}
	if seq.submittedMutationReplay.StatementID != "stmt-replay-barrier" ||
		!seq.submittedMutationReplay.PendingInsertBarrier ||
		!strings.Contains(seq.submittedMutationReplay.Error, "pending INSERT barrier") {
		t.Fatalf("barrier replay result = %+v", seq.submittedMutationReplay)
	}
}

func TestSafeAuditWorkerRunOnceSubmitsAuditVote(t *testing.T) {
	seq := &fakeWorkerSequencer{
		safeAuditTask: SafeAuditTask{
			AuditID:      "audit-1",
			SnapshotID:   "snap-2",
			SafeTable:    "`hg_safe`.`events`",
			StateRoot:    "root-safe",
			PartitionIDs: []string{"202607"},
		},
		safeAuditOK: true,
	}
	auditor := &fakeSafeAuditor{vote: SafeAuditVote{
		AuditID:    "audit-1",
		SnapshotID: "snap-2",
		StateRoot:  "root-safe",
		Match:      true,
	}}
	worker := SafeAuditWorker{WorkerID: "auditor-a", Sequencer: seq, Auditor: auditor}

	didWork, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !didWork {
		t.Fatal("RunOnce didWork=false, want true")
	}
	if seq.submittedSafeAudit.WorkerID != "auditor-a" || !seq.submittedSafeAudit.Match {
		t.Fatalf("safe audit vote = %+v", seq.submittedSafeAudit)
	}
}

func TestRollbackWorkerRunOnceSubmitsRollbackResult(t *testing.T) {
	seq := &fakeWorkerSequencer{
		rollbackTask: RollbackTask{
			RollbackID:   "rollback-stmt-1",
			StatementID:  "stmt-1",
			TableID:      "tenant.events",
			UnsafeTable:  "`hg_unsafe`.`events`",
			ScratchTable: "`hg_mutation`.`events_stmt_1_worker_a`",
			PartitionIDs: []string{"202607"},
			UnsafeParts:  []ByteSidePart{{PartitionID: "202607", PartName: "p_1_1_0"}},
		},
		rollbackOK: true,
	}
	executor := &fakeRollbackExecutor{result: RollbackResult{
		RollbackID:         "rollback-stmt-1",
		StatementID:        "stmt-1",
		TableID:            "tenant.events",
		CleanedUnsafeParts: []ByteSidePart{{PartitionID: "202607", PartName: "p_1_1_0"}},
		DroppedScratch:     true,
	}}
	worker := RollbackWorker{WorkerID: "rollback-a", Sequencer: seq, Executor: executor}

	didWork, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !didWork {
		t.Fatal("RunOnce didWork=false, want true")
	}
	if executor.task.RollbackID != "rollback-stmt-1" {
		t.Fatalf("rollback task = %+v", executor.task)
	}
	if seq.submittedRollback.WorkerID != "rollback-a" ||
		seq.submittedRollback.RollbackID != "rollback-stmt-1" ||
		!seq.submittedRollback.DroppedScratch {
		t.Fatalf("rollback result = %+v", seq.submittedRollback)
	}
}

func TestRollbackWorkerRejectsStaleUnsafeBufferEpoch(t *testing.T) {
	seq := &fakeWorkerSequencer{
		rollbackTask: RollbackTask{
			RollbackID:           "rollback-stale",
			StatementID:          "stmt-1",
			TableID:              "tenant.events",
			UnsafeTable:          "`hg_unsafe_0`.`events`",
			UnsafeBufferID:       0,
			UnsafeBufferEpoch:    17,
			UnsafeBufferDatabase: "hg_unsafe_0",
			PartitionIDs:         []string{"202607"},
		},
		rollbackOK:          true,
		unsafeBufferDecision: UnsafeBufferEpochDecision{OK: false, Reason: "buffer rotated"},
	}
	executor := &fakeRollbackExecutor{}
	worker := RollbackWorker{WorkerID: "rollback-a", Sequencer: seq, Executor: executor}

	_, err := worker.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unsafe buffer epoch") {
		t.Fatalf("RunOnce err = %v, want unsafe buffer epoch rejection", err)
	}
	if executor.task.RollbackID != "" {
		t.Fatalf("executor ran despite stale epoch: %+v", executor.task)
	}
	if seq.unsafeBufferCheck.UnsafeBufferEpoch != 17 || seq.unsafeBufferCheck.UnsafeBufferDatabase != "hg_unsafe_0" {
		t.Fatalf("epoch check request = %+v", seq.unsafeBufferCheck)
	}
}

func TestRollbackWorkerAllowsMatchingUnsafeBufferEpoch(t *testing.T) {
	seq := &fakeWorkerSequencer{
		rollbackTask: RollbackTask{
			RollbackID:           "rollback-live",
			TableID:              "tenant.events",
			UnsafeTable:          "`hg_unsafe_0`.`events`",
			UnsafeBufferEpoch:    18,
			UnsafeBufferDatabase: "hg_unsafe_0",
		},
		rollbackOK:          true,
		unsafeBufferDecision: UnsafeBufferEpochDecision{OK: true},
	}
	executor := &fakeRollbackExecutor{result: RollbackResult{RollbackID: "rollback-live"}}
	worker := RollbackWorker{WorkerID: "rollback-a", Sequencer: seq, Executor: executor}

	didWork, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !didWork || executor.task.RollbackID != "rollback-live" {
		t.Fatalf("expected rollback to run, executor task = %+v", executor.task)
	}
}

func TestRepairSyncWorkerRunOnceSubmitsRepairResult(t *testing.T) {
	manifest := sealedOpsTestManifest(t, []replay.PartManifestEntry{{
		TableID:       "tenant.events",
		PartitionID:   "202607",
		PartName:      "safe_p1",
		PartRowLtHash: "safe-root",
		RowCount:      3,
	}})
	seq := &fakeWorkerSequencer{
		repairSyncTask: RepairSyncTask{
			RepairID:     "repair-node-b",
			SnapshotID:   manifest.SnapshotID,
			TableID:      "tenant.events",
			SafeTable:    "`hg_safe`.`events`",
			SourceTable:  "`hg_repair`.`events_latest`",
			Manifest:     manifest,
			PartitionIDs: []string{"202607"},
		},
		repairSyncOK: true,
	}
	executor := &fakeRepairSyncExecutor{result: RepairSyncResult{
		RepairID:   "repair-node-b",
		SnapshotID: manifest.SnapshotID,
		TableID:    "tenant.events",
		StateRoot:  manifest.StateRoot,
		InSync:     true,
		Repaired:   true,
	}}
	worker := RepairSyncWorker{WorkerID: "repair-a", Sequencer: seq, Executor: executor}

	didWork, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !didWork {
		t.Fatal("RunOnce didWork=false, want true")
	}
	if executor.task.RepairID != "repair-node-b" {
		t.Fatalf("repair task = %+v", executor.task)
	}
	if seq.submittedRepairSync.WorkerID != "repair-a" ||
		seq.submittedRepairSync.RepairID != "repair-node-b" ||
		!seq.submittedRepairSync.InSync {
		t.Fatalf("repair result = %+v", seq.submittedRepairSync)
	}
}

func TestCompactionWorkerRunOnceSubmitsCompactionResult(t *testing.T) {
	seq := &fakeWorkerSequencer{
		compactionTask: CompactionTask{
			CompactionID:       "compact-202607",
			PromotionSeq:       101,
			TableID:            "tenant.events",
			SafeTable:          "`hg_safe`.`events`",
			CompactDatabase:    "hg_compact",
			BaseSafeSnapshotID: "snap-1",
			BasePartitionRoot:  "base-root",
			ExpectedPostRoot:   "base-root",
			PartitionIDs:       []string{"202607"},
		},
		compactionOK: true,
	}
	executor := &fakeCompactionExecutor{result: CompactionResult{
		CompactionID:       "compact-202607",
		PromotionSeq:       101,
		TableID:            "tenant.events",
		BaseSafeSnapshotID: "snap-1",
		BasePartitionRoot:  "base-root",
		CompactTable:       "`hg_compact`.`events_compact_202607_101`",
		PartitionIDs:       []string{"202607"},
	}}
	worker := CompactionWorker{WorkerID: "compact-a", Sequencer: seq, Executor: executor}

	didWork, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !didWork {
		t.Fatal("RunOnce didWork=false, want true")
	}
	if executor.task.CompactionID != "compact-202607" {
		t.Fatalf("compaction task = %+v", executor.task)
	}
	if seq.submittedCompaction.WorkerID != "compact-a" ||
		seq.submittedCompaction.CompactionID != "compact-202607" ||
		seq.submittedCompaction.PromotionSeq != 101 {
		t.Fatalf("compaction result = %+v", seq.submittedCompaction)
	}
}

type fakeReplayVerifier struct {
	job replay.ReplayJob
	att replay.ReplayAttestation
}

func (f *fakeReplayVerifier) Verify(_ context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error) {
	f.job = job
	return f.att, nil
}

type fakeByteScanner struct {
	task   ByteSideScanTask
	result ByteSideScanResult
}

func (f *fakeByteScanner) ScanByteSide(_ context.Context, task ByteSideScanTask) (ByteSideScanResult, error) {
	f.task = task
	return f.result, nil
}

type fakePromoter struct {
	task   PromotionTask
	result PromotionResult
}

func (f *fakePromoter) Promote(_ context.Context, task PromotionTask) (PromotionResult, error) {
	f.task = task
	if f.result.PromotionID == "" {
		f.result = PromotionResult{
			PromotionID:  task.PromotionID,
			SafeTable:    task.SafeTable,
			PartitionIDs: append([]string(nil), task.PartitionIDs...),
			ActiveParts: []replay.PartManifestEntry{{
				TableID:       task.TableID,
				PartitionID:   "202607",
				PartName:      "p_1_1_0",
				PartRowLtHash: "part-root",
				RowCount:      2,
			}},
		}
	}
	return f.result, nil
}

type fakeMutationExecutor struct {
	task       MutationTask
	replayTask MutationTask
	claim      MutationClaim
	replay     MutationReplayResult
}

func (f *fakeMutationExecutor) ExecuteMutation(_ context.Context, task MutationTask) (MutationClaim, error) {
	f.task = task
	return f.claim, nil
}

func (f *fakeMutationExecutor) ReplayMutation(_ context.Context, task MutationTask) (MutationReplayResult, error) {
	f.replayTask = task
	return f.replay, nil
}

type fakeSafeAuditor struct {
	task SafeAuditTask
	vote SafeAuditVote
}

func (f *fakeSafeAuditor) AuditSafe(_ context.Context, task SafeAuditTask) (SafeAuditVote, error) {
	f.task = task
	return f.vote, nil
}

type fakeRollbackExecutor struct {
	task   RollbackTask
	result RollbackResult
}

func (f *fakeRollbackExecutor) Rollback(_ context.Context, task RollbackTask) (RollbackResult, error) {
	f.task = task
	return f.result, nil
}

type fakeRepairSyncExecutor struct {
	task   RepairSyncTask
	result RepairSyncResult
}

func (f *fakeRepairSyncExecutor) RepairSync(_ context.Context, task RepairSyncTask) (RepairSyncResult, error) {
	f.task = task
	return f.result, nil
}

type fakeCompactionExecutor struct {
	task   CompactionTask
	result CompactionResult
}

func (f *fakeCompactionExecutor) Compact(_ context.Context, task CompactionTask) (CompactionResult, error) {
	f.task = task
	return f.result, nil
}

type fakeWorkerSequencer struct {
	replayJob replay.ReplayJob
	replayOK  bool

	byteTask ByteSideScanTask
	byteOK   bool

	promotionTask PromotionTask
	promotionOK   bool

	mutationTask MutationTask
	mutationOK   bool

	mutationReplayTask MutationTask
	mutationReplayOK   bool

	safeAuditTask SafeAuditTask
	safeAuditOK   bool

	rollbackTask RollbackTask
	rollbackOK   bool

	repairSyncTask RepairSyncTask
	repairSyncOK   bool

	compactionTask CompactionTask
	compactionOK   bool
	watermark      SafeWatermark
	manifests      map[string]replay.SafeSnapshotManifest

	unsafeBufferDecision UnsafeBufferEpochDecision
	unsafeBufferCheck    UnsafeBufferEpochCheckRequest

	submittedReplay         replay.ReplayAttestation
	submittedByte           ByteSideScanResult
	submittedMutationClaim  MutationClaim
	submittedMutationReplay MutationReplayResult
	submittedPromotion      PromotionResult
	submittedSafeAudit      SafeAuditVote
	submittedRollback       RollbackResult
	submittedRepairSync     RepairSyncResult
	submittedCompaction     CompactionResult
}

func (f *fakeWorkerSequencer) ClaimReplayJob(context.Context) (replay.ReplayJob, bool, error) {
	return f.replayJob, f.replayOK, nil
}

func (f *fakeWorkerSequencer) SubmitReplayAttestation(_ context.Context, att replay.ReplayAttestation) error {
	f.submittedReplay = att
	return nil
}

func (f *fakeWorkerSequencer) ClaimByteSideScan(context.Context) (ByteSideScanTask, bool, error) {
	return f.byteTask, f.byteOK, nil
}

func (f *fakeWorkerSequencer) SubmitByteSideScan(_ context.Context, result ByteSideScanResult) error {
	f.submittedByte = result
	return nil
}

func (f *fakeWorkerSequencer) ClaimPromotion(context.Context) (PromotionTask, bool, error) {
	return f.promotionTask, f.promotionOK, nil
}

func (f *fakeWorkerSequencer) SubmitPromotionResult(_ context.Context, result PromotionResult) (PromotionReceipt, error) {
	f.submittedPromotion = result
	return PromotionReceipt{OK: true}, nil
}

func (f *fakeWorkerSequencer) CheckUnsafeBufferEpoch(_ context.Context, req UnsafeBufferEpochCheckRequest) (UnsafeBufferEpochDecision, error) {
	f.unsafeBufferCheck = req
	return f.unsafeBufferDecision, nil
}

func (f *fakeWorkerSequencer) ClaimMutationTask(context.Context) (MutationTask, bool, error) {
	return f.mutationTask, f.mutationOK, nil
}

func (f *fakeWorkerSequencer) SubmitMutationClaim(_ context.Context, claim MutationClaim) error {
	f.submittedMutationClaim = claim
	return nil
}

func (f *fakeWorkerSequencer) ClaimMutationReplayTask(context.Context) (MutationTask, bool, error) {
	return f.mutationReplayTask, f.mutationReplayOK, nil
}

func (f *fakeWorkerSequencer) SubmitMutationReplay(_ context.Context, result MutationReplayResult) error {
	f.submittedMutationReplay = result
	return nil
}

func (f *fakeWorkerSequencer) ClaimSafeAudit(context.Context) (SafeAuditTask, bool, error) {
	return f.safeAuditTask, f.safeAuditOK, nil
}

func (f *fakeWorkerSequencer) SubmitSafeAudit(_ context.Context, vote SafeAuditVote) error {
	f.submittedSafeAudit = vote
	return nil
}

func (f *fakeWorkerSequencer) ClaimRollback(context.Context) (RollbackTask, bool, error) {
	return f.rollbackTask, f.rollbackOK, nil
}

func (f *fakeWorkerSequencer) SubmitRollback(_ context.Context, result RollbackResult) error {
	f.submittedRollback = result
	return nil
}

func (f *fakeWorkerSequencer) ClaimRepairSync(context.Context) (RepairSyncTask, bool, error) {
	return f.repairSyncTask, f.repairSyncOK, nil
}

func (f *fakeWorkerSequencer) SubmitRepairSync(_ context.Context, result RepairSyncResult) error {
	f.submittedRepairSync = result
	return nil
}

func (f *fakeWorkerSequencer) ClaimCompaction(context.Context) (CompactionTask, bool, error) {
	return f.compactionTask, f.compactionOK, nil
}

func (f *fakeWorkerSequencer) SubmitCompaction(_ context.Context, result CompactionResult) error {
	f.submittedCompaction = result
	return nil
}

func (f *fakeWorkerSequencer) GetSafeWatermark(context.Context) (SafeWatermark, error) {
	return f.watermark, nil
}

func (f *fakeWorkerSequencer) GetSafeSnapshot(_ context.Context, snapshotID string) (replay.SafeSnapshotManifest, bool, error) {
	manifest, ok := f.manifests[snapshotID]
	return manifest, ok, nil
}

// deadlineCapturingExecutor records whether the context passed to ExecuteMutation
// carried a deadline, so a test can assert the MutationWorker applied its
// wall-clock budget (HG-P1-02).
type deadlineCapturingExecutor struct {
	sawDeadline bool
}

func (e *deadlineCapturingExecutor) ExecuteMutation(ctx context.Context, task MutationTask) (MutationClaim, error) {
	_, e.sawDeadline = ctx.Deadline()
	return MutationClaim{StatementID: task.StatementID, PostStateRoot: "r"}, nil
}

func (e *deadlineCapturingExecutor) ReplayMutation(ctx context.Context, task MutationTask) (MutationReplayResult, error) {
	_, e.sawDeadline = ctx.Deadline()
	return MutationReplayResult{StatementID: task.StatementID, PostStateRoot: "r"}, nil
}

func TestMutationWorkerAppliesMutationTimeoutDeadline(t *testing.T) {
	task := MutationTask{StatementID: "stmt-mut", TableID: "tenant.events", MutationType: MutationTypeUpdate, SafeTable: "`hg_safe`.`events`", PartitionIDs: []string{"202607"}}

	// With a MutationTimeout set, the executor sees a context with a deadline.
	execWithTimeout := &deadlineCapturingExecutor{}
	w := MutationWorker{WorkerID: "w", Sequencer: &fakeWorkerSequencer{mutationTask: task, mutationOK: true}, Executor: execWithTimeout, MutationTimeout: time.Minute}
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !execWithTimeout.sawDeadline {
		t.Fatalf("expected the executor context to carry a deadline when MutationTimeout is set")
	}

	// Without a budget, the caller's (deadline-free) context is passed through.
	execNoTimeout := &deadlineCapturingExecutor{}
	w2 := MutationWorker{WorkerID: "w", Sequencer: &fakeWorkerSequencer{mutationTask: task, mutationOK: true}, Executor: execNoTimeout}
	if _, err := w2.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if execNoTimeout.sawDeadline {
		t.Fatalf("expected no deadline when neither MutationTimeout nor MaxRebindDuration is set")
	}
}
