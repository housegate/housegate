package storageintegrity

import (
	"context"
	"fmt"
	"sort"
	"strings"
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
	statements, err := w.promotionStatements(ctx, task)
	if err != nil {
		return w.fail(ctx, task, err)
	}
	for _, sql := range statements {
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

func (w PromotionWorker) promotionStatements(_ context.Context, task PromotionTask) ([]string, error) {
	if len(task.Statements) > 0 {
		return append([]string(nil), task.Statements...), nil
	}
	if strings.TrimSpace(task.SafeTable) == "" {
		return nil, fmt.Errorf("safe_table is required for attach partition promotion")
	}
	if strings.TrimSpace(task.UnsafeTable) == "" {
		return nil, fmt.Errorf("unsafe_table is required for attach partition promotion")
	}
	partitionIDs := append([]string(nil), task.PartitionIDs...)
	partitionIDs = compactSortedStrings(partitionIDs)
	if len(partitionIDs) == 0 {
		return nil, fmt.Errorf("promotion task is missing partition_ids")
	}
	statements := make([]string, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		statements = append(statements, attachPartitionSQL(task.SafeTable, task.UnsafeTable, partitionID))
	}
	return statements, nil
}

func (w PromotionWorker) fail(ctx context.Context, task PromotionTask, err error) error {
	failure := PromotionFailure{PromotionID: task.PromotionID, LeaseID: task.LeaseID, Error: err.Error()}
	if sinkErr := w.Sink.FailPromotion(ctx, failure); sinkErr != nil {
		return fmt.Errorf("%v; fail promotion: %w", err, sinkErr)
	}
	return err
}

func attachPartitionSQL(safeTable, unsafeTable, partitionID string) string {
	return "ALTER TABLE " + safeTable + " ATTACH PARTITION ID '" + escapeClickHouseStringLiteral(partitionID) + "' FROM " + unsafeTable
}

func escapeClickHouseStringLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "\\'")
}

func compactSortedStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
