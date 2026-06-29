package storageintegrity

import (
	"context"
	"fmt"
)

// RollbackWorker executes HouseKeeper-issued rollback leases against the local
// ClickHouse and reports completion back to HouseKeeper.
type RollbackWorker struct {
	Executor RollbackExecutor
	Sink     RollbackSink
}

func (w RollbackWorker) Apply(ctx context.Context, task RollbackTask) error {
	if w.Executor == nil {
		return fmt.Errorf("rollback executor is required")
	}
	if w.Sink == nil {
		return fmt.Errorf("rollback sink is required")
	}
	if task.RollbackID == "" {
		return fmt.Errorf("rollback_id is required")
	}
	if task.LeaseID == "" {
		return fmt.Errorf("lease_id is required")
	}
	if len(task.Statements) == 0 {
		return w.fail(ctx, task, fmt.Errorf("at least one rollback statement is required"))
	}
	for _, sql := range task.Statements {
		if sql == "" {
			return w.fail(ctx, task, fmt.Errorf("rollback statement is empty"))
		}
		if err := w.Executor.ExecRollbackSQL(ctx, sql); err != nil {
			return w.fail(ctx, task, fmt.Errorf("execute rollback statement: %w", err))
		}
	}
	if err := w.Sink.FinishRollback(ctx, RollbackResult{RollbackID: task.RollbackID, LeaseID: task.LeaseID}); err != nil {
		return fmt.Errorf("finish rollback: %w", err)
	}
	return nil
}

func (w RollbackWorker) fail(ctx context.Context, task RollbackTask, err error) error {
	failure := RollbackFailure{RollbackID: task.RollbackID, LeaseID: task.LeaseID, Error: err.Error()}
	if sinkErr := w.Sink.FailRollback(ctx, failure); sinkErr != nil {
		return fmt.Errorf("%v; fail rollback: %w", err, sinkErr)
	}
	return err
}
