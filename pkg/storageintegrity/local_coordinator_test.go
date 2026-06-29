package storageintegrity

import (
	"context"
	"testing"

	"housegate/housegate/pkg/replay"
)

func TestLocalCoordinatorWaitsForExternalFinalityBeforePromotion(t *testing.T) {
	ctx := context.Background()
	c := NewLocalCoordinator(LocalCoordinatorConfig{
		RequireFinality:   true,
		RequireReplay:     true,
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
	})
	rec := InsertRecord{
		TableID:     "dual_hg_auth.t",
		StatementID: "stmt-1",
		OriginalSQL: "INSERT INTO dual_hg_auth.t VALUES (1)",
		UnsafeSQL:   "INSERT INTO `hg_unsafe`.`dual_hg_auth.t_a` VALUES (1)",
		UnsafeTable: "`hg_unsafe`.`dual_hg_auth.t_a`",
		SafeTable:   "`hg_safe`.`dual_hg_auth.t`",
		Payload: PayloadCommitment{
			Ref:    "mockda://dual_hg_auth.t/stmt-1/hash",
			Hash:   "0xpayload",
			Length: 12,
		},
	}
	if err := c.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	job, ok, err := c.ClaimReplayJob(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimReplayJob ok=%v err=%v", ok, err)
	}
	if len(job.Statements) != 1 || job.Statements[0].StatementID != "stmt-1" {
		t.Fatalf("replay job = %+v", job)
	}
	if _, ok, err := c.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion before attest/finality ok=%v err=%v, want no task", ok, err)
	}
	if err := c.SubmitFinality(ctx, FinalityRecord{
		Kind:        "mock",
		BatchID:     rec.StatementID,
		StatementID: rec.StatementID,
		PayloadRef:  rec.Payload.Ref,
		PayloadHash: rec.Payload.Hash,
		Finalized:   true,
	}); err != nil {
		t.Fatalf("SubmitFinality: %v", err)
	}
	if _, ok, err := c.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion before replay ok=%v err=%v, want no task", ok, err)
	}
	att := replay.ReplayAttestation{
		ReplicaID: "mock-replay",
		Receipt: replay.ExecutionReceipt{
			BlockSeq:        job.BlockSeq,
			SourceClaimRoot: job.SourceClaimRoot,
			MatchSourceRoot: true,
		},
		ReceiptHash:     "0xreceipt",
		MatchSourceRoot: true,
	}
	if err := c.SubmitReplayAttestation(ctx, att); err != nil {
		t.Fatalf("SubmitReplayAttestation: %v", err)
	}
	task, ok, err := c.ClaimPromotion(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimPromotion after attest/finality ok=%v err=%v", ok, err)
	}
	if task.PromotionID == "" || task.LeaseID == "" {
		t.Fatalf("promotion task missing ids: %+v", task)
	}
	if len(task.Statements) != 2 {
		t.Fatalf("promotion statements = %d, want 2: %+v", len(task.Statements), task.Statements)
	}
	if got, want := task.Statements[0], "INSERT INTO `hg_safe`.`dual_hg_auth.t` SELECT * FROM `hg_unsafe`.`dual_hg_auth.t_a`"; got != want {
		t.Fatalf("promotion INSERT = %q, want %q", got, want)
	}
	if got, want := task.Statements[1], "TRUNCATE TABLE `hg_unsafe`.`dual_hg_auth.t_a`"; got != want {
		t.Fatalf("promotion TRUNCATE = %q, want %q", got, want)
	}
}

func TestLocalCoordinatorBlocksPromotionUntilUnsafeValidation(t *testing.T) {
	ctx := context.Background()
	c := NewLocalCoordinator(LocalCoordinatorConfig{
		RequireFinality:         true,
		RequireReplay:           true,
		RequireUnsafeValidation: true,
		UnsafeReplicas: []UnsafeReplica{
			{ReplicaID: "r1", Addr: "127.0.0.1:9000"},
			{ReplicaID: "r2", Addr: "127.0.0.1:9001"},
		},
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
	})
	rec := InsertRecord{
		TableID:     "dual_hg_auth.t",
		StatementID: "stmt-unsafe-1",
		OriginalSQL: "INSERT INTO dual_hg_auth.t VALUES (1)",
		UnsafeSQL:   "INSERT INTO `hg_unsafe`.`dual_hg_auth.t_a` VALUES (1)",
		UnsafeTable: "`hg_unsafe`.`dual_hg_auth.t_a`",
		SafeTable:   "`hg_safe`.`dual_hg_auth.t`",
		Payload: PayloadCommitment{
			Ref:    "mockda://dual_hg_auth.t/stmt-unsafe-1/hash",
			Hash:   "0xpayload",
			Length: 12,
		},
	}
	if err := c.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	unsafeTask, ok, err := c.ClaimUnsafeValidation(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimUnsafeValidation ok=%v err=%v", ok, err)
	}
	if unsafeTask.StatementID != rec.StatementID || unsafeTask.UnsafeTable != rec.UnsafeTable {
		t.Fatalf("unsafe validation task = %+v", unsafeTask)
	}
	if len(unsafeTask.Replicas) != 2 {
		t.Fatalf("unsafe validation replicas = %+v, want 2", unsafeTask.Replicas)
	}
	job, ok, err := c.ClaimReplayJob(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimReplayJob ok=%v err=%v", ok, err)
	}
	if err := c.SubmitUnsafeValidation(ctx, UnsafeValidationResult{
		ValidationID: unsafeTask.ValidationID,
		StatementID:  unsafeTask.StatementID,
		TableID:      unsafeTask.TableID,
		UnsafeTable:  unsafeTask.UnsafeTable,
		RowCount:     1,
		RowsHash:     "0xrows",
		Replicas: []UnsafeReplicaDigest{
			{ReplicaID: "r1", RowCount: 1, RowsHash: "0xrows"},
			{ReplicaID: "r2", RowCount: 1, RowsHash: "0xrows"},
		},
	}); err != nil {
		t.Fatalf("SubmitUnsafeValidation: %v", err)
	}
	if err := c.SubmitReplayAttestation(ctx, replay.ReplayAttestation{
		ReplicaID: "mock-replay",
		Receipt: replay.ExecutionReceipt{
			BlockSeq:        job.BlockSeq,
			SourceClaimRoot: job.SourceClaimRoot,
			MatchSourceRoot: true,
		},
		ReceiptHash:     "0xreceipt",
		MatchSourceRoot: true,
	}); err != nil {
		t.Fatalf("SubmitReplayAttestation: %v", err)
	}
	if _, ok, err := c.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion before finality ok=%v err=%v, want no task", ok, err)
	}
	if err := c.SubmitFinality(ctx, FinalityRecord{
		Kind:        "mock",
		BatchID:     rec.StatementID,
		StatementID: rec.StatementID,
		PayloadRef:  rec.Payload.Ref,
		PayloadHash: rec.Payload.Hash,
		Finalized:   true,
	}); err != nil {
		t.Fatalf("SubmitFinality: %v", err)
	}
	if _, ok, err := c.ClaimPromotion(ctx); err != nil || !ok {
		t.Fatalf("ClaimPromotion after unsafe validation/replay/finality ok=%v err=%v, want task", ok, err)
	}
}

func TestLocalCoordinatorExternalRollbackBlocksPromotion(t *testing.T) {
	ctx := context.Background()
	c := NewLocalCoordinator(LocalCoordinatorConfig{
		RequireFinality:         true,
		RequireReplay:           true,
		RequireUnsafeValidation: true,
		UnsafeDatabase:          "hg_unsafe",
		SafeDatabase:            "hg_safe",
		UnsafeTableSuffix:       "_a",
	})
	rec := InsertRecord{
		TableID:     "dual_hg_auth.t",
		StatementID: "stmt-rollback-1",
		OriginalSQL: "INSERT INTO dual_hg_auth.t VALUES (1)",
		UnsafeSQL:   "INSERT INTO `hg_unsafe`.`dual_hg_auth.t_a` VALUES (1)",
		UnsafeTable: "`hg_unsafe`.`dual_hg_auth.t_a`",
		SafeTable:   "`hg_safe`.`dual_hg_auth.t`",
		Payload: PayloadCommitment{
			Ref:    "mockda://dual_hg_auth.t/stmt-rollback-1/hash",
			Hash:   "0xpayload",
			Length: 12,
		},
	}
	if err := c.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	job, ok, err := c.ClaimReplayJob(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimReplayJob ok=%v err=%v", ok, err)
	}
	unsafeTask, ok, err := c.ClaimUnsafeValidation(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimUnsafeValidation ok=%v err=%v", ok, err)
	}
	if err := c.SubmitReplayAttestation(ctx, replay.ReplayAttestation{
		ReplicaID: "mock-replay",
		Receipt: replay.ExecutionReceipt{
			BlockSeq:        job.BlockSeq,
			SourceClaimRoot: job.SourceClaimRoot,
			MatchSourceRoot: true,
		},
		ReceiptHash:     "0xreceipt",
		MatchSourceRoot: true,
	}); err != nil {
		t.Fatalf("SubmitReplayAttestation: %v", err)
	}
	if err := c.SubmitUnsafeValidation(ctx, UnsafeValidationResult{
		ValidationID: unsafeTask.ValidationID,
		StatementID:  unsafeTask.StatementID,
		TableID:      unsafeTask.TableID,
		UnsafeTable:  unsafeTask.UnsafeTable,
		RowCount:     1,
		RowsHash:     "0xrows",
	}); err != nil {
		t.Fatalf("SubmitUnsafeValidation: %v", err)
	}
	if err := c.SubmitRollback(ctx, RollbackEvent{
		Kind:        "mock",
		BatchID:     rec.StatementID,
		StatementID: rec.StatementID,
		Reason:      "dispute",
	}); err != nil {
		t.Fatalf("SubmitRollback: %v", err)
	}
	if err := c.SubmitFinality(ctx, FinalityRecord{
		Kind:        "mock",
		BatchID:     rec.StatementID,
		StatementID: rec.StatementID,
		PayloadRef:  rec.Payload.Ref,
		PayloadHash: rec.Payload.Hash,
		Finalized:   true,
	}); err != nil {
		t.Fatalf("SubmitFinality after rollback: %v", err)
	}
	if _, ok, err := c.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion after rollback ok=%v err=%v, want no task", ok, err)
	}
	rollbackTask, ok, err := c.ClaimRollback(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimRollback ok=%v err=%v, want task", ok, err)
	}
	if rollbackTask.StatementID != rec.StatementID || rollbackTask.RollbackID == "" || rollbackTask.LeaseID == "" {
		t.Fatalf("rollback task = %+v", rollbackTask)
	}
	if len(rollbackTask.Statements) != 1 || rollbackTask.Statements[0] != "TRUNCATE TABLE `hg_unsafe`.`dual_hg_auth.t_a`" {
		t.Fatalf("rollback statements = %+v", rollbackTask.Statements)
	}
}

func TestLocalCoordinatorQueuesSafeAuditAndDecidesMajority(t *testing.T) {
	ctx := context.Background()
	c := NewLocalCoordinator(LocalCoordinatorConfig{
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
		SafeAuditReplicas: []SafeAuditReplica{
			{ReplicaID: "safe-r1"},
			{ReplicaID: "safe-r2"},
			{ReplicaID: "safe-r3"},
		},
		SafeAuditNetworkID:  "net-1",
		SafeAuditSchemaHash: "0xschema",
	})
	rec := InsertRecord{
		TableID:     "dual_hg_auth.t",
		StatementID: "stmt-audit-1",
		OriginalSQL: "INSERT INTO dual_hg_auth.t VALUES (1)",
		UnsafeTable: "`hg_unsafe`.`dual_hg_auth.t_a`",
		SafeTable:   "`hg_safe`.`dual_hg_auth.t`",
		Payload: PayloadCommitment{
			Ref:    "mockda://dual_hg_auth.t/stmt-audit-1/hash",
			Hash:   "0xpayload",
			Length: 12,
		},
	}
	if err := c.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	promotion, ok, err := c.ClaimPromotion(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimPromotion ok=%v err=%v", ok, err)
	}
	if err := c.FinishPromotion(ctx, PromotionResult{PromotionID: promotion.PromotionID, LeaseID: promotion.LeaseID}); err != nil {
		t.Fatalf("FinishPromotion: %v", err)
	}

	tasks := claimSafeAuditTasks(t, c, 3)
	auditID := tasks[0].AuditID
	if auditID == "" {
		t.Fatalf("audit id is empty: %+v", tasks[0])
	}
	for _, task := range tasks {
		if task.AuditID != auditID || task.TableID != rec.TableID || task.NetworkID != "net-1" || task.SchemaHash != "0xschema" {
			t.Fatalf("safe audit task = %+v", task)
		}
	}

	submitSafeAuditVote(t, c, auditID, "safe-r1", "0xmajority", 2)
	submitSafeAuditVote(t, c, auditID, "safe-r2", "0xmajority", 2)
	submitSafeAuditVote(t, c, auditID, "safe-r3", "0xminority", 2)

	decision, ok := c.SafeAuditDecision(ctx, auditID)
	if !ok {
		t.Fatalf("missing safe audit decision for %s", auditID)
	}
	if decision.Status != SafeAuditStatusMajority || decision.MajorityHash != "0xmajority" || decision.MajorityCount != 2 {
		t.Fatalf("decision = %+v, want majority 0xmajority count=2", decision)
	}
	if len(decision.MinorityReplicas) != 1 || decision.MinorityReplicas[0] != "safe-r3" {
		t.Fatalf("minority replicas = %+v, want [safe-r3]", decision.MinorityReplicas)
	}
}

func TestLocalCoordinatorMarksSafeAuditDisputeWithoutMajority(t *testing.T) {
	ctx := context.Background()
	c := NewLocalCoordinator(LocalCoordinatorConfig{
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
		SafeAuditReplicas: []SafeAuditReplica{
			{ReplicaID: "safe-r1"},
			{ReplicaID: "safe-r2"},
			{ReplicaID: "safe-r3"},
		},
	})
	rec := InsertRecord{
		TableID:     "dual_hg_auth.t",
		StatementID: "stmt-audit-dispute",
		OriginalSQL: "INSERT INTO dual_hg_auth.t VALUES (1)",
		UnsafeTable: "`hg_unsafe`.`dual_hg_auth.t_a`",
		SafeTable:   "`hg_safe`.`dual_hg_auth.t`",
		Payload: PayloadCommitment{
			Ref:    "mockda://dual_hg_auth.t/stmt-audit-dispute/hash",
			Hash:   "0xpayload",
			Length: 12,
		},
	}
	if err := c.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	promotion, ok, err := c.ClaimPromotion(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimPromotion ok=%v err=%v", ok, err)
	}
	if err := c.FinishPromotion(ctx, PromotionResult{PromotionID: promotion.PromotionID, LeaseID: promotion.LeaseID}); err != nil {
		t.Fatalf("FinishPromotion: %v", err)
	}

	tasks := claimSafeAuditTasks(t, c, 3)
	auditID := tasks[0].AuditID
	submitSafeAuditVote(t, c, auditID, "safe-r1", "0xhash1", 1)
	submitSafeAuditVote(t, c, auditID, "safe-r2", "0xhash2", 1)
	submitSafeAuditVote(t, c, auditID, "safe-r3", "0xhash3", 1)

	decision, ok := c.SafeAuditDecision(ctx, auditID)
	if !ok {
		t.Fatalf("missing safe audit decision for %s", auditID)
	}
	if decision.Status != SafeAuditStatusDispute || decision.MajorityHash != "" || decision.MajorityCount != 0 {
		t.Fatalf("decision = %+v, want dispute without majority", decision)
	}
}

func TestMockReplayVerifierBuildsAttestation(t *testing.T) {
	verifier := MockReplayVerifier{ReplicaID: "housegate-1"}
	att, err := verifier.Verify(context.Background(), replay.ReplayJob{
		BlockSeq:        7,
		SourceClaimRoot: "0xsource",
		Statements: []replay.Statement{{
			StatementID:   "stmt-7",
			SQLHash:       "0xsql",
			PayloadHash:   "0xpayload",
			PayloadLength: 3,
			TargetTableID: "dual_hg_auth.t",
		}},
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if att.ReplicaID != "housegate-1" {
		t.Fatalf("ReplicaID = %q", att.ReplicaID)
	}
	if !att.MatchSourceRoot || !att.Receipt.MatchSourceRoot {
		t.Fatalf("attestation should match source root: %+v", att)
	}
	if att.ReceiptHash == "" {
		t.Fatalf("ReceiptHash is empty: %+v", att)
	}
}

func claimSafeAuditTasks(t *testing.T, c *LocalCoordinator, n int) []SafeAuditTask {
	t.Helper()
	ctx := context.Background()
	tasks := make([]SafeAuditTask, 0, n)
	for i := 0; i < n; i++ {
		task, ok, err := c.ClaimSafeAudit(ctx)
		if err != nil || !ok {
			t.Fatalf("ClaimSafeAudit[%d] ok=%v err=%v", i, ok, err)
		}
		tasks = append(tasks, task)
	}
	if task, ok, err := c.ClaimSafeAudit(ctx); err != nil || ok {
		t.Fatalf("ClaimSafeAudit after drain task=%+v ok=%v err=%v, want no task", task, ok, err)
	}
	return tasks
}

func submitSafeAuditVote(t *testing.T, c *LocalCoordinator, auditID, replicaID, batchHash string, rows uint64) {
	t.Helper()
	if err := c.SubmitSafeAuditVote(context.Background(), SafeAuditVote{
		AuditID:    auditID,
		WorkerID:   "worker-" + replicaID,
		ReplicaID:  replicaID,
		SnapshotID: "snapshot-" + auditID,
		Range:      "safe=`hg_safe`.`dual_hg_auth.t`",
		BatchHash:  batchHash,
		RowCount:   rows,
		VoteHash:   "vote-" + replicaID,
		Signature:  "sig-" + replicaID,
	}); err != nil {
		t.Fatalf("SubmitSafeAuditVote(%s): %v", replicaID, err)
	}
}
