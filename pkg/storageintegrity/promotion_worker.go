package storageintegrity

import (
	"context"
	"fmt"
)

// PromotionWorker executes HouseKeeper-issued promotion leases against the
// local ClickHouse and reports Applied/Failed back to HouseKeeper.
type PromotionWorker struct {
	Executor PromotionExecutor
	Sink     PromotionSink
}

func (w PromotionWorker) Apply(ctx context.Context, task PromotionTask) error {
	if w.Executor == nil {
		return fmt.Errorf("promotion executor is required")
	}
	if w.Sink == nil {
		return fmt.Errorf("promotion sink is required")
	}
	if task.PromotionID == "" {
		return fmt.Errorf("promotion_id is required")
	}
	if task.LeaseID == "" {
		return fmt.Errorf("lease_id is required")
	}
	if len(task.Statements) == 0 {
		return w.fail(ctx, task, fmt.Errorf("at least one promotion statement is required"))
	}
	for _, sql := range task.Statements {
		if sql == "" {
			return w.fail(ctx, task, fmt.Errorf("promotion statement is empty"))
		}
		if err := w.Executor.ExecPromotionSQL(ctx, sql); err != nil {
			return w.fail(ctx, task, fmt.Errorf("execute promotion statement: %w", err))
		}
	}
	readback, err := w.Executor.ReadPromotionRows(ctx, task.Readback)
	if err != nil {
		return w.fail(ctx, task, fmt.Errorf("read promotion rows: %w", err))
	}
	if task.Readback.ExpectedRows != 0 && readback.RowCount != task.Readback.ExpectedRows {
		return w.fail(ctx, task, fmt.Errorf("promotion row count mismatch: got %d want %d", readback.RowCount, task.Readback.ExpectedRows))
	}
	if task.Readback.ExpectedHash != "" && readback.RowsHash != task.Readback.ExpectedHash {
		return w.fail(ctx, task, fmt.Errorf("promotion rows hash mismatch: got %s want %s", readback.RowsHash, task.Readback.ExpectedHash))
	}
	result := PromotionResult{PromotionID: task.PromotionID, LeaseID: task.LeaseID, Readback: readback}
	if err := w.Sink.FinishPromotion(ctx, result); err != nil {
		return fmt.Errorf("finish promotion: %w", err)
	}
	return nil
}

func (w PromotionWorker) fail(ctx context.Context, task PromotionTask, err error) error {
	failure := PromotionFailure{PromotionID: task.PromotionID, LeaseID: task.LeaseID, Error: err.Error()}
	if sinkErr := w.Sink.FailPromotion(ctx, failure); sinkErr != nil {
		return fmt.Errorf("%v; fail promotion: %w", err, sinkErr)
	}
	return err
}
