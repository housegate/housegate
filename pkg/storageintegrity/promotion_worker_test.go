package storageintegrity

import (
	"context"
	"errors"
	"testing"
)

func TestPromotionWorkerExecutesStatementsAndFinishesOnMatchingReadback(t *testing.T) {
	ctx := context.Background()
	exec := &recordingPromotionExecutor{
		readback: PromotionReadbackResult{RowCount: 2, RowsHash: "0xrows"},
	}
	sink := &recordingPromotionSink{}
	worker := PromotionWorker{Executor: exec, Sink: sink}

	err := worker.Apply(ctx, PromotionTask{
		PromotionID: "promo-1",
		LeaseID:     "lease-1",
		Statements: []string{
			"ALTER TABLE hg_safe REPLACE PARTITION 202606 FROM hg_promote",
			"DROP TABLE hg_promote",
		},
		Readback: PromotionReadbackSpec{
			Table:         "hg_safe.Transfer",
			ExpectedRows:  2,
			ExpectedHash:  "0xrows",
			PromotionExpr: "_hg_promotion_id = 'promo-1'",
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(exec.statements) != 2 {
		t.Fatalf("executed statements = %#v", exec.statements)
	}
	if len(sink.finished) != 1 || sink.finished[0].PromotionID != "promo-1" {
		t.Fatalf("finished = %#v", sink.finished)
	}
}

func TestPromotionWorkerFailsLeaseWhenReadbackMismatches(t *testing.T) {
	ctx := context.Background()
	exec := &recordingPromotionExecutor{
		readback: PromotionReadbackResult{RowCount: 1, RowsHash: "0xwrong"},
	}
	sink := &recordingPromotionSink{}
	worker := PromotionWorker{Executor: exec, Sink: sink}

	err := worker.Apply(ctx, PromotionTask{
		PromotionID: "promo-1",
		LeaseID:     "lease-1",
		Statements:  []string{"ALTER TABLE hg_safe ATTACH PARTITION 202606 FROM hg_unsafe"},
		Readback: PromotionReadbackSpec{
			Table:        "hg_safe.Transfer",
			ExpectedRows: 2,
			ExpectedHash: "0xrows",
		},
	})
	if err == nil {
		t.Fatal("expected readback mismatch")
	}
	if len(sink.failed) != 1 || sink.failed[0].PromotionID != "promo-1" {
		t.Fatalf("failed = %#v", sink.failed)
	}
	if len(sink.finished) != 0 {
		t.Fatalf("unexpected finish: %#v", sink.finished)
	}
}

type recordingPromotionExecutor struct {
	statements []string
	readback   PromotionReadbackResult
	execErr    error
}

func (e *recordingPromotionExecutor) ExecPromotionSQL(_ context.Context, sql string) error {
	e.statements = append(e.statements, sql)
	return e.execErr
}

func (e *recordingPromotionExecutor) ReadPromotionRows(_ context.Context, spec PromotionReadbackSpec) (PromotionReadbackResult, error) {
	if spec.Table == "" {
		return PromotionReadbackResult{}, errors.New("table required")
	}
	return e.readback, nil
}

type recordingPromotionSink struct {
	finished []PromotionResult
	failed   []PromotionFailure
}

func (s *recordingPromotionSink) FinishPromotion(_ context.Context, result PromotionResult) error {
	s.finished = append(s.finished, result)
	return nil
}

func (s *recordingPromotionSink) FailPromotion(_ context.Context, failure PromotionFailure) error {
	s.failed = append(s.failed, failure)
	return nil
}
