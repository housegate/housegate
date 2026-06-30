package storageintegrity

import (
	"context"
	"errors"
	"strings"
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
		PromotionID:  "promo-1",
		LeaseID:      "lease-1",
		UnsafeTable:  "hg_unsafe.Transfer",
		SafeTable:    "hg_safe.Transfer",
		PartitionIDs: []string{"202606"},
		Readback: PromotionReadbackSpec{
			Table:        "hg_safe.Transfer",
			ExpectedRows: 2,
			ExpectedHash: "0xrows",
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(exec.statements) != 1 {
		t.Fatalf("executed statements = %#v", exec.statements)
	}
	if got, want := exec.statements[0], "ALTER TABLE hg_safe.Transfer ATTACH PARTITION ID '202606' FROM hg_unsafe.Transfer"; got != want {
		t.Fatalf("executed statement = %q, want %q", got, want)
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

func TestPromotionWorkerRequiresPartitionIDsForAttachPromotion(t *testing.T) {
	ctx := context.Background()
	exec := &recordingPromotionExecutor{
		readback: PromotionReadbackResult{RowCount: 2, RowsHash: "0xrows"},
	}
	sink := &recordingPromotionSink{}
	worker := PromotionWorker{Executor: exec, Sink: sink}

	err := worker.Apply(ctx, PromotionTask{
		PromotionID: "promo-1",
		LeaseID:     "lease-1",
		UnsafeTable: "hg_unsafe.Transfer",
		SafeTable:   "hg_safe.Transfer",
		Readback: PromotionReadbackSpec{
			Table:        "hg_safe.Transfer",
			ExpectedRows: 2,
			ExpectedHash: "0xrows",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "partition_ids") {
		t.Fatalf("Apply error = %v, want missing partition_ids", err)
	}
	if len(exec.statements) != 0 {
		t.Fatalf("executed statements = %#v, want none", exec.statements)
	}
	if len(sink.failed) != 1 || !strings.Contains(sink.failed[0].Error, "partition_ids") {
		t.Fatalf("failed = %#v, want partition_ids failure", sink.failed)
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
