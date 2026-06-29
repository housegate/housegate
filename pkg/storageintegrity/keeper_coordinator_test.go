package storageintegrity

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"

	"housegate/housegate/pkg/replay"
)

func TestWaitForZKSessionIgnoresTransientStates(t *testing.T) {
	events := make(chan zk.Event, 2)
	events <- zk.Event{State: zk.StateConnecting}
	events <- zk.Event{State: zk.StateHasSession}

	if err := waitForZKSession(context.Background(), events, time.Second); err != nil {
		t.Fatalf("waitForZKSession: %v", err)
	}
}

func TestWaitForZKSessionReturnsAuthFailure(t *testing.T) {
	events := make(chan zk.Event, 1)
	events <- zk.Event{State: zk.StateAuthFailed}

	err := waitForZKSession(context.Background(), events, time.Second)
	if err == nil || !strings.Contains(err.Error(), "auth failed") {
		t.Fatalf("waitForZKSession err = %v, want auth failed", err)
	}
}

func TestKeeperCoordinatorEnsuresDecisionRootForKeeperSideEffects(t *testing.T) {
	store := newMemoryKeeperStore()
	c := newTestKeeperCoordinator(t, store, "hg-1")

	if exists := testKeeperNodeExists(t, store, c.path("decisions")); !exists {
		t.Fatalf("decisions root was not created")
	}
}

func TestKeeperCoordinatorUsesConfiguredParticipantsForStatementAndLocalUnsafeValidation(t *testing.T) {
	ctx := context.Background()
	store := newMemoryKeeperStore()
	hg1 := newTestKeeperCoordinator(t, store, "r1")
	hg2 := newTestKeeperCoordinator(t, store, "r2")
	hg3 := newTestKeeperCoordinator(t, store, "r3")

	rec := testKeeperInsertRecord("stmt-configured-participants")
	if err := hg1.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	statementData := testKeeperNodeData(t, store, hg1.statementPath(rec.StatementID))
	if !strings.Contains(statementData, "participants=r1,r2,r3\n") {
		t.Fatalf("statement znode = %q, want configured participants", statementData)
	}
	if !strings.Contains(statementData, "partition_ids=202606\n") {
		t.Fatalf("statement znode = %q, want partition_ids from part registry", statementData)
	}

	testKeeperMaterializeStatementTasksForParticipants(t, store, hg1, rec, "r1", "r2", "r3")
	task, ok, err := hg1.ClaimUnsafeValidation(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimUnsafeValidation ok=%v err=%v", ok, err)
	}
	if len(task.Replicas) != 1 || task.Replicas[0].ReplicaID != "r1" {
		t.Fatalf("unsafe task replicas = %+v, want only local participant", task.Replicas)
	}
	if err := hg1.SubmitUnsafeValidation(ctx, testUnsafeResult(task, "0xrows", "r1")); err != nil {
		t.Fatalf("SubmitUnsafeValidation: %v", err)
	}
	if !testKeeperNodeExists(t, store, hg1.unsafeParticipantResultPath(rec.StatementID, "r1")) {
		t.Fatalf("missing participant unsafe result")
	}

	testKeeperMaterializePromotion(t, store, hg1, rec)
	for _, hg := range []*KeeperCoordinator{hg1, hg2, hg3} {
		promotion, ok, err := hg.ClaimPromotion(ctx)
		if err != nil || !ok {
			t.Fatalf("%s ClaimPromotion ok=%v err=%v", hg.cfg.WorkerID, ok, err)
		}
		if err := hg.FinishPromotion(ctx, PromotionResult{PromotionID: promotion.PromotionID, LeaseID: promotion.LeaseID}); err != nil {
			t.Fatalf("%s FinishPromotion: %v", hg.cfg.WorkerID, err)
		}
	}
	for _, participant := range []string{"r1", "r2", "r3"} {
		key := "audit-" + rec.StatementID + "/" + participant
		if !testKeeperNodeExists(t, store, hg1.safeAuditTaskPath(key)) {
			t.Fatalf("missing safe audit task for participant %s", participant)
		}
	}
	auditTask, ok, err := hg2.ClaimSafeAudit(ctx)
	if err != nil || !ok {
		t.Fatalf("hg2 ClaimSafeAudit ok=%v err=%v", ok, err)
	}
	if auditTask.ReplicaID != "r2" || auditTask.AuditID != "audit-"+rec.StatementID {
		t.Fatalf("hg2 safe audit task = %+v, want local configured participant", auditTask)
	}
	if auditTask, ok, err := hg3.ClaimSafeAudit(ctx); err != nil || !ok || auditTask.ReplicaID != "r3" {
		t.Fatalf("hg3 ClaimSafeAudit task=%+v ok=%v err=%v, want local configured participant", auditTask, ok, err)
	}
}

func TestKeeperCoordinatorPromotionReadbackAggregatesParticipantUnsafeResults(t *testing.T) {
	ctx := context.Background()
	store := newMemoryKeeperStore()
	hg1 := newTestKeeperCoordinator(t, store, "r1")
	hg2 := newTestKeeperCoordinator(t, store, "r2")
	hg3 := newTestKeeperCoordinator(t, store, "r3")

	rec := testKeeperInsertRecord("stmt-participant-readback")
	if err := hg1.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	testKeeperMaterializeStatementTasksForParticipants(t, store, hg1, rec, "r1", "r2", "r3")
	for _, hg := range []*KeeperCoordinator{hg1, hg2, hg3} {
		task, ok, err := hg.ClaimUnsafeValidation(ctx)
		if err != nil || !ok {
			t.Fatalf("%s ClaimUnsafeValidation ok=%v err=%v", hg.cfg.WorkerID, ok, err)
		}
		if err := hg.SubmitUnsafeValidation(ctx, testUnsafeResult(task, "0xrows", hg.cfg.WorkerID)); err != nil {
			t.Fatalf("%s SubmitUnsafeValidation: %v", hg.cfg.WorkerID, err)
		}
	}
	testKeeperMaterializePromotion(t, store, hg1, rec)

	task, ok, err := hg1.ClaimPromotion(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimPromotion ok=%v err=%v", ok, err)
	}
	if task.Readback.ExpectedRows != 1 || task.Readback.ExpectedHash != "0xrows" {
		t.Fatalf("promotion readback = %+v, want participant unsafe aggregate", task.Readback)
	}
	if got, want := strings.Join(task.PartitionIDs, ","), "202606"; got != want {
		t.Fatalf("promotion partition ids = %q, want %q", got, want)
	}
}

func TestKeeperCoordinatorPromotionRunsOncePerParticipantBeforeSafeAudit(t *testing.T) {
	ctx := context.Background()
	store := newMemoryKeeperStore()
	hg1 := newTestKeeperCoordinator(t, store, "r1")
	hg2 := newTestKeeperCoordinator(t, store, "r2")
	hg3 := newTestKeeperCoordinator(t, store, "r3")

	rec := testKeeperInsertRecord("stmt-participant-promotion")
	if err := hg1.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	testKeeperMaterializePromotion(t, store, hg1, rec)

	task1, ok, err := hg1.ClaimPromotion(ctx)
	if err != nil || !ok {
		t.Fatalf("r1 ClaimPromotion ok=%v err=%v", ok, err)
	}
	task2, ok, err := hg2.ClaimPromotion(ctx)
	if err != nil || !ok {
		t.Fatalf("r2 ClaimPromotion ok=%v err=%v", ok, err)
	}
	task3, ok, err := hg3.ClaimPromotion(ctx)
	if err != nil || !ok {
		t.Fatalf("r3 ClaimPromotion ok=%v err=%v", ok, err)
	}
	if task1.LeaseID == task2.LeaseID || task2.LeaseID == task3.LeaseID {
		t.Fatalf("promotion lease IDs should be participant scoped: %q %q %q", task1.LeaseID, task2.LeaseID, task3.LeaseID)
	}
	reclaimed, ok, err := hg1.ClaimPromotion(ctx)
	if err != nil || !ok || reclaimed.LeaseID != task1.LeaseID {
		t.Fatalf("r1 duplicate ClaimPromotion task=%+v ok=%v err=%v, want idempotent lease reclaim", reclaimed, ok, err)
	}

	if err := hg1.FinishPromotion(ctx, PromotionResult{PromotionID: task1.PromotionID, LeaseID: task1.LeaseID}); err != nil {
		t.Fatalf("r1 FinishPromotion: %v", err)
	}
	if testKeeperNodeExists(t, store, hg1.safeAuditTaskPath("audit-"+rec.StatementID+"/r1")) {
		t.Fatalf("safe audit queued before all participants finished promotion")
	}
	if err := hg2.FinishPromotion(ctx, PromotionResult{PromotionID: task2.PromotionID, LeaseID: task2.LeaseID}); err != nil {
		t.Fatalf("r2 FinishPromotion: %v", err)
	}
	if testKeeperNodeExists(t, store, hg1.safeAuditTaskPath("audit-"+rec.StatementID+"/r1")) {
		t.Fatalf("safe audit queued before final participant finished promotion")
	}
	if err := hg3.FinishPromotion(ctx, PromotionResult{PromotionID: task3.PromotionID, LeaseID: task3.LeaseID}); err != nil {
		t.Fatalf("r3 FinishPromotion: %v", err)
	}
	for _, participant := range []string{"r1", "r2", "r3"} {
		resultPath := hg1.promotionParticipantResultPath(rec.StatementID, participant)
		if !testKeeperNodeExists(t, store, resultPath) {
			t.Fatalf("missing participant promotion result %s", resultPath)
		}
		key := "audit-" + rec.StatementID + "/" + participant
		if !testKeeperNodeExists(t, store, hg1.safeAuditTaskPath(key)) {
			t.Fatalf("missing safe audit task for participant %s", participant)
		}
	}
}

func TestKeeperCoordinatorRequiresReplayQuorumAndUnsafeAllReplicasBeforePromotion(t *testing.T) {
	ctx := context.Background()
	store := newMemoryKeeperStore()
	hg1 := newTestKeeperCoordinator(t, store, "hg-1")
	hg2 := newTestKeeperCoordinator(t, store, "hg-2")
	hg3 := newTestKeeperCoordinator(t, store, "hg-3")

	rec := testKeeperInsertRecord("stmt-keeper-1")
	if err := hg1.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	if exists := testKeeperNodeExists(t, store, hg1.replayJobPath(rec.StatementID)); exists {
		t.Fatalf("SubmitInsert created replay job; Keeper C++ should own managed task creation")
	}
	if exists := testKeeperNodeExists(t, store, hg1.unsafeTaskPath(rec.StatementID)); exists {
		t.Fatalf("SubmitInsert created unsafe task; Keeper C++ should own managed task creation")
	}
	if statementData := testKeeperNodeData(t, store, hg1.statementPath(rec.StatementID)); strings.HasPrefix(strings.TrimSpace(statementData), "{") {
		t.Fatalf("statement znode is JSON, want Keeper C++ key/value protocol: %s", statementData)
	}
	testKeeperMaterializeStatementTasks(t, store, hg1, rec)

	if _, ok, err := hg1.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion before quorum ok=%v err=%v, want no task", ok, err)
	}

	job1, ok, err := hg1.ClaimReplayJob(ctx)
	if err != nil || !ok {
		t.Fatalf("hg1 ClaimReplayJob ok=%v err=%v", ok, err)
	}
	job2, ok, err := hg2.ClaimReplayJob(ctx)
	if err != nil || !ok {
		t.Fatalf("hg2 ClaimReplayJob ok=%v err=%v", ok, err)
	}
	if _, ok, err := hg3.ClaimReplayJob(ctx); err != nil || !ok {
		t.Fatalf("hg3 ClaimReplayJob before quorum ok=%v err=%v, want visible job", ok, err)
	}
	if job1.BlockSeq != job2.BlockSeq || job1.Statements[0].StatementID != rec.StatementID {
		t.Fatalf("replay jobs = %+v / %+v", job1, job2)
	}

	if err := hg1.SubmitReplayAttestation(ctx, testReplayAttestation(job1, "hg-1", "0xstate-a")); err != nil {
		t.Fatalf("hg1 SubmitReplayAttestation: %v", err)
	}
	if err := hg1.SubmitFinality(ctx, FinalityRecord{
		Kind:        "mock",
		BatchID:     rec.StatementID,
		StatementID: rec.StatementID,
		PayloadRef:  rec.Payload.Ref,
		PayloadHash: rec.Payload.Hash,
		Finalized:   true,
	}); err != nil {
		t.Fatalf("SubmitFinality: %v", err)
	}
	unsafeTask, ok, err := hg1.ClaimUnsafeValidation(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimUnsafeValidation ok=%v err=%v", ok, err)
	}
	if err := hg1.SubmitUnsafeValidation(ctx, testUnsafeResult(unsafeTask, "0xrows", "r1", "r2", "r3")); err != nil {
		t.Fatalf("SubmitUnsafeValidation: %v", err)
	}
	if unsafeData := testKeeperNodeData(t, store, hg1.unsafeResultPath(rec.StatementID)); !strings.Contains(unsafeData, "replica_digests=r1:1:0xrows,r2:1:0xrows,r3:1:0xrows\n") {
		t.Fatalf("unsafe result znode = %q, want Keeper C++ key/value replica_digests", unsafeData)
	}
	if _, ok, err := hg1.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion with one replay vote ok=%v err=%v, want no task", ok, err)
	}

	if err := hg2.SubmitReplayAttestation(ctx, testReplayAttestation(job2, "hg-2", "0xstate-a")); err != nil {
		t.Fatalf("hg2 SubmitReplayAttestation: %v", err)
	}
	testKeeperMaterializePromotion(t, store, hg1, rec)
	task, ok, err := hg1.ClaimPromotion(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimPromotion after 2/3 replay + 3/3 unsafe + finality ok=%v err=%v", ok, err)
	}
	if task.PromotionID != "promotion-"+rec.StatementID || task.LeaseID == "" {
		t.Fatalf("promotion task ids = %+v", task)
	}
	if task.SafeTable != "`hg_safe`.`dual_hg_auth.t`" || task.UnsafeTable != "`hg_unsafe`.`dual_hg_auth.t_a`" {
		t.Fatalf("promotion tables = safe %q unsafe %q", task.SafeTable, task.UnsafeTable)
	}
	if len(task.Statements) != 0 {
		t.Fatalf("promotion statements = %+v, want attach-partition worker expansion", task.Statements)
	}
	if task.Readback.ExpectedRows != 1 || task.Readback.ExpectedHash != "0xrows" {
		t.Fatalf("promotion readback expectation = %+v", task.Readback)
	}
}

func TestKeeperCoordinatorSkipsWorkWhenWorkerIsReplayQuarantined(t *testing.T) {
	ctx := context.Background()
	store := newMemoryKeeperStore()
	hg1 := newTestKeeperCoordinator(t, store, "hg-1")
	hg2 := newTestKeeperCoordinator(t, store, "hg-2")

	rec := testKeeperInsertRecord("stmt-keeper-quarantine")
	if err := hg1.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	testKeeperMaterializeStatementTasks(t, store, hg1, rec)
	testKeeperMaterializePromotion(t, store, hg1, rec)
	testKeeperMaterializeRollback(t, store, hg1, rec, "dispute")
	testKeeperCreateNode(t, store, hg1.path("replay_quarantine", escapeSegment("hg-2")),
		"worker_id=hg-2\n"+
			"reason=replay_minority_mismatch\n"+
			"statement_id="+rec.StatementID+"\n"+
			"status=active\n")

	if _, ok, err := hg1.ClaimReplayJob(ctx); err != nil || !ok {
		t.Fatalf("hg1 ClaimReplayJob ok=%v err=%v, want task", ok, err)
	}
	if _, ok, err := hg2.ClaimReplayJob(ctx); err != nil || ok {
		t.Fatalf("hg2 ClaimReplayJob ok=%v err=%v, want quarantined worker skipped", ok, err)
	}
	if _, ok, err := hg2.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("hg2 ClaimPromotion ok=%v err=%v, want quarantined worker skipped", ok, err)
	}
	if _, ok, err := hg2.ClaimRollback(ctx); err != nil || ok {
		t.Fatalf("hg2 ClaimRollback ok=%v err=%v, want quarantined worker skipped", ok, err)
	}

	testKeeperCreateNode(t, store, hg1.safeAuditTaskPath("audit-"+rec.StatementID+"/hg-2"),
		`{"audit_id":"audit-`+rec.StatementID+`","replica_id":"hg-2","network_id":"net","table_id":"dual_hg_auth.t","schema_hash":"schema","snapshot_id":"snap","range":"all"}`)
	if _, ok, err := hg2.ClaimSafeAudit(ctx); err != nil || ok {
		t.Fatalf("hg2 ClaimSafeAudit ok=%v err=%v, want quarantined worker skipped", ok, err)
	}
}

func TestKeeperCoordinatorRequiresUnsafeValidationFromEveryConfiguredReplica(t *testing.T) {
	ctx := context.Background()
	store := newMemoryKeeperStore()
	hg1 := newTestKeeperCoordinator(t, store, "hg-1")
	hg2 := newTestKeeperCoordinator(t, store, "hg-2")

	rec := testKeeperInsertRecord("stmt-keeper-unsafe")
	if err := hg1.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	testKeeperMaterializeStatementTasks(t, store, hg1, rec)
	job1, _, _ := hg1.ClaimReplayJob(ctx)
	job2, _, _ := hg2.ClaimReplayJob(ctx)
	if err := hg1.SubmitReplayAttestation(ctx, testReplayAttestation(job1, "hg-1", "0xstate-a")); err != nil {
		t.Fatalf("hg1 SubmitReplayAttestation: %v", err)
	}
	if err := hg2.SubmitReplayAttestation(ctx, testReplayAttestation(job2, "hg-2", "0xstate-a")); err != nil {
		t.Fatalf("hg2 SubmitReplayAttestation: %v", err)
	}
	if err := hg1.SubmitFinality(ctx, FinalityRecord{Kind: "mock", BatchID: rec.StatementID, StatementID: rec.StatementID, Finalized: true}); err != nil {
		t.Fatalf("SubmitFinality: %v", err)
	}
	unsafeTask, ok, err := hg1.ClaimUnsafeValidation(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimUnsafeValidation ok=%v err=%v", ok, err)
	}
	if err := hg1.SubmitUnsafeValidation(ctx, testUnsafeResult(unsafeTask, "0xrows", "r1", "r2")); err == nil {
		t.Fatalf("SubmitUnsafeValidation with missing replica succeeded, want error")
	}
	if _, ok, err := hg1.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion after incomplete unsafe ok=%v err=%v, want no task", ok, err)
	}
}

func TestKeeperCoordinatorTreatsDuplicateUnsafeValidationWritesAsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := newMemoryKeeperStore()
	hg1 := newTestKeeperCoordinator(t, store, "hg-1")

	rec := testKeeperInsertRecord("stmt-keeper-unsafe-idempotent")
	if err := hg1.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	testKeeperMaterializeStatementTasks(t, store, hg1, rec)
	task, ok, err := hg1.ClaimUnsafeValidation(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimUnsafeValidation ok=%v err=%v", ok, err)
	}

	result := testUnsafeResult(task, "0xrows", "r1", "r2", "r3")
	if err := hg1.SubmitUnsafeValidation(ctx, result); err != nil {
		t.Fatalf("SubmitUnsafeValidation #1: %v", err)
	}
	if err := hg1.SubmitUnsafeValidation(ctx, result); err != nil {
		t.Fatalf("SubmitUnsafeValidation #2: %v", err)
	}

	failure := UnsafeValidationFailure{
		ValidationID: task.ValidationID,
		StatementID:  "stmt-keeper-unsafe-failure-idempotent",
		Error:        "timeout",
	}
	if err := hg1.SubmitUnsafeValidationFailure(ctx, failure); err != nil {
		t.Fatalf("SubmitUnsafeValidationFailure #1: %v", err)
	}
	if err := hg1.SubmitUnsafeValidationFailure(ctx, failure); err != nil {
		t.Fatalf("SubmitUnsafeValidationFailure #2: %v", err)
	}
}

func TestKeeperCoordinatorRollbackBlocksPromotionAndQueuesRollbackTask(t *testing.T) {
	ctx := context.Background()
	store := newMemoryKeeperStore()
	hg1 := newTestKeeperCoordinator(t, store, "hg-1")
	hg2 := newTestKeeperCoordinator(t, store, "hg-2")

	rec := testKeeperInsertRecord("stmt-keeper-rollback")
	if err := hg1.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	testKeeperMaterializeStatementTasks(t, store, hg1, rec)
	job1, _, _ := hg1.ClaimReplayJob(ctx)
	job2, _, _ := hg2.ClaimReplayJob(ctx)
	unsafeTask, _, _ := hg1.ClaimUnsafeValidation(ctx)
	if err := hg1.SubmitReplayAttestation(ctx, testReplayAttestation(job1, "hg-1", "0xstate-a")); err != nil {
		t.Fatalf("hg1 SubmitReplayAttestation: %v", err)
	}
	if err := hg2.SubmitReplayAttestation(ctx, testReplayAttestation(job2, "hg-2", "0xstate-a")); err != nil {
		t.Fatalf("hg2 SubmitReplayAttestation: %v", err)
	}
	if err := hg1.SubmitUnsafeValidation(ctx, testUnsafeResult(unsafeTask, "0xrows", "r1", "r2", "r3")); err != nil {
		t.Fatalf("SubmitUnsafeValidation: %v", err)
	}
	if err := hg1.SubmitRollback(ctx, RollbackEvent{Kind: "mock", BatchID: rec.StatementID, StatementID: rec.StatementID, Reason: "dispute"}); err != nil {
		t.Fatalf("SubmitRollback: %v", err)
	}
	if exists := testKeeperNodeExists(t, store, hg1.rollbackTaskPath(rec.StatementID)); exists {
		t.Fatalf("SubmitRollback created rollback task; Keeper C++ should own managed task creation")
	}
	if err := hg1.SubmitFinality(ctx, FinalityRecord{Kind: "mock", BatchID: rec.StatementID, StatementID: rec.StatementID, Finalized: true}); err != nil {
		t.Fatalf("SubmitFinality: %v", err)
	}
	if _, ok, err := hg1.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion after rollback ok=%v err=%v, want no task", ok, err)
	}
	testKeeperMaterializeRollback(t, store, hg1, rec, "dispute")
	task, ok, err := hg1.ClaimRollback(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimRollback ok=%v err=%v, want task", ok, err)
	}
	if task.RollbackID != "rollback-"+rec.StatementID || task.LeaseID == "" || len(task.Statements) != 1 {
		t.Fatalf("rollback task = %+v", task)
	}
}

func newTestKeeperCoordinator(t *testing.T, store keeperStore, workerID string) *KeeperCoordinator {
	t.Helper()
	c, err := NewKeeperCoordinatorWithStore(KeeperCoordinatorConfig{
		Root:                    "/housekeeper/v1/storage_integrity_test",
		WorkerID:                workerID,
		ReplayQuorum:            2,
		RequireFinality:         true,
		RequireReplay:           true,
		RequireUnsafeValidation: true,
		UnsafeReplicas: []UnsafeReplica{
			{ReplicaID: "r1", Addr: "127.0.0.1:9001"},
			{ReplicaID: "r2", Addr: "127.0.0.1:9002"},
			{ReplicaID: "r3", Addr: "127.0.0.1:9003"},
		},
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
	}, store)
	if err != nil {
		t.Fatalf("NewKeeperCoordinatorWithStore: %v", err)
	}
	return c
}

func newTestKeeperCoordinatorWithoutReplicaConfig(t *testing.T, store keeperStore, participantID string) *KeeperCoordinator {
	t.Helper()
	c, err := NewKeeperCoordinatorWithStore(KeeperCoordinatorConfig{
		Root:                    "/housekeeper/v1/storage_integrity_test",
		WorkerID:                participantID,
		ReplayQuorum:            2,
		RequireFinality:         true,
		RequireReplay:           true,
		RequireUnsafeValidation: true,
		UnsafeDatabase:          "hg_unsafe",
		SafeDatabase:            "hg_safe",
		UnsafeTableSuffix:       "_a",
	}, store)
	if err != nil {
		t.Fatalf("NewKeeperCoordinatorWithStore: %v", err)
	}
	return c
}

func testKeeperInsertRecord(statementID string) InsertRecord {
	return InsertRecord{
		TableID:      "dual_hg_auth.t",
		StatementID:  statementID,
		OriginalSQL:  "INSERT INTO dual_hg_auth.t VALUES (1)",
		UnsafeSQL:    "INSERT INTO `hg_unsafe`.`dual_hg_auth.t_a` VALUES (1)",
		UnsafeTable:  "`hg_unsafe`.`dual_hg_auth.t_a`",
		SafeTable:    "`hg_safe`.`dual_hg_auth.t`",
		PartitionIDs: []string{"202606"},
		Payload: PayloadCommitment{
			Ref:    "mockda://dual_hg_auth.t/" + statementID + "/hash",
			Hash:   "0xpayload",
			Length: 11,
		},
	}
}

func testReplayAttestation(job replay.ReplayJob, replicaID, stateRoot string) replay.ReplayAttestation {
	receipt := replay.ExecutionReceipt{
		BlockSeq:          job.BlockSeq,
		SourceClaimRoot:   job.SourceClaimRoot,
		ComputedStateRoot: stateRoot,
		MatchSourceRoot:   true,
	}
	return replay.ReplayAttestation{
		ReplicaID:       replicaID,
		Receipt:         receipt,
		ReceiptHash:     "receipt-" + stateRoot,
		Signature:       "sig-" + replicaID,
		MatchSourceRoot: true,
	}
}

func testUnsafeResult(task UnsafeValidationTask, rowsHash string, replicas ...string) UnsafeValidationResult {
	result := UnsafeValidationResult{
		ValidationID: task.ValidationID,
		StatementID:  task.StatementID,
		TableID:      task.TableID,
		UnsafeTable:  task.UnsafeTable,
		RowCount:     1,
		RowsHash:     rowsHash,
	}
	for _, replicaID := range replicas {
		result.Replicas = append(result.Replicas, UnsafeReplicaDigest{ReplicaID: replicaID, RowCount: 1, RowsHash: rowsHash})
	}
	return result
}

func testKeeperMaterializeStatementTasks(t *testing.T, store keeperStore, c *KeeperCoordinator, rec InsertRecord) {
	t.Helper()
	testKeeperEnsurePath(t, store, c.attestationsPath(rec.StatementID))
	testKeeperCreateNode(t, store, c.replayJobPath(rec.StatementID),
		"statement_id="+rec.StatementID+"\n"+
			"table_id="+rec.TableID+"\n"+
			"payload_ref="+rec.Payload.Ref+"\n"+
			"payload_hash="+rec.Payload.Hash+"\n")
	testKeeperCreateNode(t, store, c.unsafeTaskPath(rec.StatementID),
		"statement_id="+rec.StatementID+"\n"+
			"table_id="+rec.TableID+"\n"+
			"unsafe_table="+rec.UnsafeTable+"\n"+
			"replicas=r1,r2,r3\n")
	testKeeperCreateNode(t, store, c.path("decisions", escapeSegment(rec.StatementID)),
		"statement_id="+rec.StatementID+"\n"+
			"replay_quorum_met=false\n"+
			"unsafe_validated=false\n"+
			"finalized=false\n"+
			"rollback_requested=false\n"+
			"promotion_ready=false\n"+
			"rollback_ready=false\n"+
			"replay_result_hash=\n"+
			"replay_tally=\n")
}

func testKeeperMaterializeStatementTasksForParticipants(t *testing.T, store keeperStore, c *KeeperCoordinator, rec InsertRecord, participants ...string) {
	t.Helper()
	testKeeperEnsurePath(t, store, c.attestationsPath(rec.StatementID))
	testKeeperEnsurePath(t, store, c.unsafeResultPath(rec.StatementID))
	testKeeperCreateNode(t, store, c.replayJobPath(rec.StatementID),
		"statement_id="+rec.StatementID+"\n"+
			"table_id="+rec.TableID+"\n"+
			"payload_ref="+rec.Payload.Ref+"\n"+
			"payload_hash="+rec.Payload.Hash+"\n")
	testKeeperCreateNode(t, store, c.unsafeTaskPath(rec.StatementID),
		"statement_id="+rec.StatementID+"\n"+
			"table_id="+rec.TableID+"\n"+
			"unsafe_table="+rec.UnsafeTable+"\n"+
			"participants="+strings.Join(participants, ",")+"\n")
	testKeeperCreateNode(t, store, c.path("decisions", escapeSegment(rec.StatementID)),
		"statement_id="+rec.StatementID+"\n"+
			"replay_quorum_met=false\n"+
			"unsafe_validated=false\n"+
			"finalized=false\n"+
			"rollback_requested=false\n"+
			"promotion_ready=false\n"+
			"rollback_ready=false\n"+
			"replay_result_hash=\n"+
			"replay_tally=\n")
}

func testKeeperMaterializePromotion(t *testing.T, store keeperStore, c *KeeperCoordinator, rec InsertRecord) {
	t.Helper()
	testKeeperCreateNode(t, store, c.promotionPath(rec.StatementID),
		"promotion_id=promotion-"+rec.StatementID+"\n"+
			"lease_id=lease-"+rec.StatementID+"\n"+
			"unsafe_table="+rec.UnsafeTable+"\n"+
			"safe_table="+rec.SafeTable+"\n"+
			"partition_ids="+strings.Join(rec.PartitionIDs, ",")+"\n")
}

func testKeeperMaterializeRollback(t *testing.T, store keeperStore, c *KeeperCoordinator, rec InsertRecord, reason string) {
	t.Helper()
	testKeeperCreateNode(t, store, c.rollbackTaskPath(rec.StatementID),
		"rollback_id=rollback-"+rec.StatementID+"\n"+
			"lease_id=rollback-lease-"+rec.StatementID+"\n"+
			"statement_id="+rec.StatementID+"\n"+
			"batch_id="+rec.StatementID+"\n"+
			"reason="+reason+"\n"+
			"unsafe_table="+rec.UnsafeTable+"\n")
}

func testKeeperEnsurePath(t *testing.T, store keeperStore, p string) {
	t.Helper()
	if err := store.EnsurePath(context.Background(), p); err != nil {
		t.Fatalf("EnsurePath(%s): %v", p, err)
	}
}

func testKeeperCreateNode(t *testing.T, store keeperStore, p string, data string) {
	t.Helper()
	testKeeperEnsurePath(t, store, parentPath(p))
	if err := store.Create(context.Background(), p, []byte(data)); err != nil {
		t.Fatalf("Create(%s): %v", p, err)
	}
}

func testKeeperNodeExists(t *testing.T, store keeperStore, p string) bool {
	t.Helper()
	_, ok, err := store.Get(context.Background(), p)
	if err != nil {
		t.Fatalf("Get(%s): %v", p, err)
	}
	return ok
}

func testKeeperNodeData(t *testing.T, store keeperStore, p string) string {
	t.Helper()
	data, ok, err := store.Get(context.Background(), p)
	if err != nil {
		t.Fatalf("Get(%s): %v", p, err)
	}
	if !ok {
		t.Fatalf("Get(%s): missing node", p)
	}
	return string(data)
}

type memoryKeeperStore struct {
	mu    sync.Mutex
	nodes map[string][]byte
}

func newMemoryKeeperStore() *memoryKeeperStore {
	return &memoryKeeperStore{nodes: map[string][]byte{"/": nil}}
}

func (s *memoryKeeperStore) EnsurePath(_ context.Context, p string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	parts := strings.Split(strings.Trim(p, "/"), "/")
	cur := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		cur += "/" + part
		if _, ok := s.nodes[cur]; !ok {
			s.nodes[cur] = nil
		}
	}
	return nil
}

func (s *memoryKeeperStore) Create(_ context.Context, p string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[p]; ok {
		return errKeeperNodeExists
	}
	parent := parentPath(p)
	if _, ok := s.nodes[parent]; !ok {
		return errKeeperNoNode
	}
	s.nodes[p] = append([]byte(nil), data...)
	return nil
}

func (s *memoryKeeperStore) Set(_ context.Context, p string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[p]; !ok {
		return errKeeperNoNode
	}
	s.nodes[p] = append([]byte(nil), data...)
	return nil
}

func (s *memoryKeeperStore) Get(_ context.Context, p string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.nodes[p]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), data...), true, nil
}

func (s *memoryKeeperStore) Children(_ context.Context, p string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[p]; !ok {
		return nil, errKeeperNoNode
	}
	seen := map[string]struct{}{}
	prefix := strings.TrimRight(p, "/") + "/"
	for node := range s.nodes {
		if !strings.HasPrefix(node, prefix) {
			continue
		}
		rest := strings.TrimPrefix(node, prefix)
		if rest == "" || strings.Contains(rest, "/") {
			continue
		}
		seen[rest] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for child := range seen {
		out = append(out, child)
	}
	sort.Strings(out)
	return out, nil
}

func (s *memoryKeeperStore) Close() {}

var _ keeperStore = (*memoryKeeperStore)(nil)

func TestMemoryKeeperStoreErrorsMatchSentinels(t *testing.T) {
	store := newMemoryKeeperStore()
	if err := store.Create(context.Background(), "/missing/child", nil); !errors.Is(err, errKeeperNoNode) {
		t.Fatalf("Create without parent err=%v, want errKeeperNoNode", err)
	}
}
