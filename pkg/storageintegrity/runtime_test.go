package storageintegrity

import (
	"context"
	"testing"
	"time"

	"housegate/housegate/pkg/replay"
)

func TestRuntimePollsReplayJobsUntilContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &singleReplayJobSource{job: replay.ReplayJob{BlockSeq: 99}}
	sink := &cancelingReplaySink{cancel: cancel}
	runtime := Runtime{
		PollInterval: time.Millisecond,
		ReplayJobs:   source,
		Replay: &ReplayWorker{
			Verifier: verifierFunc(func(context.Context, replay.ReplayJob) (replay.ReplayAttestation, error) {
				return replay.ReplayAttestation{ReplicaID: "replica-a", ReceiptHash: "0xhash", Signature: "sig"}, nil
			}),
			Sink: sink,
		},
	}

	if err := runtime.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if source.claims != 1 {
		t.Fatalf("claims = %d, want 1", source.claims)
	}
	if len(sink.attestations) != 1 {
		t.Fatalf("attestations = %#v", sink.attestations)
	}
}

func TestRuntimeRunsLocalInsertThroughPromotion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	coordinator := NewLocalCoordinator(LocalCoordinatorConfig{
		RequireFinality:         true,
		RequireReplay:           true,
		RequireUnsafeValidation: true,
		UnsafeDatabase:          "hg_unsafe",
		SafeDatabase:            "hg_safe",
		UnsafeTableSuffix:       "_a",
	})
	if err := coordinator.SubmitInsert(ctx, InsertRecord{
		TableID:     "dual_hg_auth.t",
		StatementID: "stmt-runtime-1",
		OriginalSQL: "INSERT INTO dual_hg_auth.t VALUES (1)",
		UnsafeSQL:   "INSERT INTO `hg_unsafe`.`dual_hg_auth.t_a` VALUES (1)",
		UnsafeTable: "`hg_unsafe`.`dual_hg_auth.t_a`",
		SafeTable:   "`hg_safe`.`dual_hg_auth.t`",
		Payload: PayloadCommitment{
			Ref:    "mockda://dual_hg_auth.t/stmt-runtime-1/hash",
			Hash:   "0xpayload",
			Length: 11,
		},
	}); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	if _, err := (MockExternalFinalityService{Sink: coordinator}).Submit(ctx, FinalityEvent{
		Kind:        "mock",
		BatchID:     "stmt-runtime-1",
		StatementID: "stmt-runtime-1",
		PayloadRef:  "mockda://dual_hg_auth.t/stmt-runtime-1/hash",
		PayloadHash: "0xpayload",
	}); err != nil {
		t.Fatalf("Submit external finality: %v", err)
	}
	exec := &runtimePromotionExecutor{}
	runtime := Runtime{
		PollInterval: time.Millisecond,
		ReplayJobs:   coordinator,
		Replay: &ReplayWorker{
			Verifier: MockReplayVerifier{ReplicaID: "runtime-test"},
			Sink:     coordinator,
		},
		UnsafeTasks: coordinator,
		Unsafe: &UnsafeValidationWorker{
			Verifier: MockUnsafeValidationVerifier{ReplicaID: "runtime-test"},
			Sink:     coordinator,
		},
		Promotions: coordinator,
		Promotion: &PromotionWorker{
			Executor: exec,
			Sink:     cancelingPromotionSink{PromotionSink: coordinator, cancel: cancel},
		},
	}

	if err := runtime.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(exec.statements) != 2 {
		t.Fatalf("promotion statements = %d, want 2: %+v", len(exec.statements), exec.statements)
	}
	if got, want := exec.statements[0], "INSERT INTO `hg_safe`.`dual_hg_auth.t` SELECT * FROM `hg_unsafe`.`dual_hg_auth.t_a`"; got != want {
		t.Fatalf("promotion INSERT = %q, want %q", got, want)
	}
	if got, want := exec.statements[1], "TRUNCATE TABLE `hg_unsafe`.`dual_hg_auth.t_a`"; got != want {
		t.Fatalf("promotion TRUNCATE = %q, want %q", got, want)
	}
}

func TestRuntimeRunsPromotionThroughSafeAuditMajority(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	coordinator := NewLocalCoordinator(LocalCoordinatorConfig{
		RequireFinality:         true,
		RequireReplay:           true,
		RequireUnsafeValidation: true,
		UnsafeDatabase:          "hg_unsafe",
		SafeDatabase:            "hg_safe",
		UnsafeTableSuffix:       "_a",
		SafeAuditReplicas: []SafeAuditReplica{
			{ReplicaID: "safe-r1"},
			{ReplicaID: "safe-r2"},
			{ReplicaID: "safe-r3"},
		},
		SafeAuditNetworkID:  "net-1",
		SafeAuditSchemaHash: "0xschema",
	})
	if err := coordinator.SubmitInsert(ctx, InsertRecord{
		TableID:     "dual_hg_auth.t",
		StatementID: "stmt-runtime-audit",
		OriginalSQL: "INSERT INTO dual_hg_auth.t VALUES (1)",
		UnsafeSQL:   "INSERT INTO `hg_unsafe`.`dual_hg_auth.t_a` VALUES (1)",
		UnsafeTable: "`hg_unsafe`.`dual_hg_auth.t_a`",
		SafeTable:   "`hg_safe`.`dual_hg_auth.t`",
		Payload: PayloadCommitment{
			Ref:    "mockda://dual_hg_auth.t/stmt-runtime-audit/hash",
			Hash:   "0xpayload",
			Length: 11,
		},
	}); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	if _, err := (MockExternalFinalityService{Sink: coordinator}).Submit(ctx, FinalityEvent{
		Kind:        "mock",
		BatchID:     "stmt-runtime-audit",
		StatementID: "stmt-runtime-audit",
		PayloadRef:  "mockda://dual_hg_auth.t/stmt-runtime-audit/hash",
		PayloadHash: "0xpayload",
	}); err != nil {
		t.Fatalf("Submit external finality: %v", err)
	}
	exec := &runtimePromotionExecutor{}
	runtime := Runtime{
		PollInterval: time.Millisecond,
		ReplayJobs:   coordinator,
		Replay: &ReplayWorker{
			Verifier: MockReplayVerifier{ReplicaID: "runtime-test"},
			Sink:     coordinator,
		},
		UnsafeTasks: coordinator,
		Unsafe: &UnsafeValidationWorker{
			Verifier: MockUnsafeValidationVerifier{ReplicaID: "runtime-test"},
			Sink:     coordinator,
		},
		Promotions: coordinator,
		Promotion: &PromotionWorker{
			Executor: exec,
			Sink:     coordinator,
		},
		SafeAudits: coordinator,
		SafeAudit: &SafeAuditWorker{
			Reader: &recordingSafeAuditReader{rows: []SafeRow{{RowID: "row-1", Values: []any{"alice", uint64(10)}}}},
			Signer: &recordingSafeAuditSigner{workerID: "audit-worker", signature: "audit-sig"},
			Sink:   cancelingSafeAuditSink{coordinator: coordinator, cancel: cancel},
		},
	}

	if err := runtime.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	decision, ok := coordinator.SafeAuditDecision(context.Background(), "audit-stmt-runtime-audit")
	if !ok {
		t.Fatalf("missing safe audit decision")
	}
	if decision.Status != SafeAuditStatusMajority || decision.MajorityCount < 2 {
		t.Fatalf("safe audit decision = %+v, want majority", decision)
	}
}

type singleReplayJobSource struct {
	job    replay.ReplayJob
	claims int
}

func (s *singleReplayJobSource) ClaimReplayJob(context.Context) (replay.ReplayJob, bool, error) {
	if s.claims > 0 {
		return replay.ReplayJob{}, false, nil
	}
	s.claims++
	return s.job, true, nil
}

type cancelingReplaySink struct {
	cancel       context.CancelFunc
	attestations []replay.ReplayAttestation
	failures     []ReplayFailure
}

func (s *cancelingReplaySink) SubmitReplayAttestation(_ context.Context, att replay.ReplayAttestation) error {
	s.attestations = append(s.attestations, att)
	s.cancel()
	return nil
}

func (s *cancelingReplaySink) SubmitReplayFailure(_ context.Context, failure ReplayFailure) error {
	s.failures = append(s.failures, failure)
	s.cancel()
	return nil
}

type runtimePromotionExecutor struct {
	statements []string
}

func (e *runtimePromotionExecutor) ExecPromotionSQL(_ context.Context, sql string) error {
	e.statements = append(e.statements, sql)
	return nil
}

func (e *runtimePromotionExecutor) ReadPromotionRows(context.Context, PromotionReadbackSpec) (PromotionReadbackResult, error) {
	return PromotionReadbackResult{}, nil
}

type cancelingPromotionSink struct {
	PromotionSink
	cancel context.CancelFunc
}

func (s cancelingPromotionSink) FinishPromotion(ctx context.Context, result PromotionResult) error {
	err := s.PromotionSink.FinishPromotion(ctx, result)
	s.cancel()
	return err
}

type cancelingSafeAuditSink struct {
	coordinator *LocalCoordinator
	cancel      context.CancelFunc
}

func (s cancelingSafeAuditSink) SubmitSafeAuditVote(ctx context.Context, vote SafeAuditVote) error {
	if err := s.coordinator.SubmitSafeAuditVote(ctx, vote); err != nil {
		return err
	}
	if decision, ok := s.coordinator.SafeAuditDecision(ctx, vote.AuditID); ok && decision.Status == SafeAuditStatusMajority {
		s.cancel()
	}
	return nil
}
