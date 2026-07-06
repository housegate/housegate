package storageintegrity

import (
	"context"
	"fmt"
	"sort"
)

type ClickHousePromotionSeqStore struct {
	Exec             SQLConn
	Query            HashQueryConn
	MetadataDatabase string
}

func (s ClickHousePromotionSeqStore) LastPromotionSeq(ctx context.Context, table, partitionID string) (uint64, error) {
	if s.Query == nil {
		return 0, fmt.Errorf("clickhouse query connection is required")
	}
	if err := s.ensure(ctx); err != nil {
		return 0, err
	}
	rows, err := s.Query.Query(ctx, fmt.Sprintf(
		"SELECT max(seq) FROM %s WHERE safe_table = %s AND partition_id = %s",
		s.tableName(), sqlStringLiteral(table), sqlStringLiteral(partitionID),
	))
	if err != nil {
		return 0, fmt.Errorf("query promotion_seq: %w", err)
	}
	defer rows.Close()
	var seq uint64
	if rows.Next() {
		if err := rows.Scan(&seq); err != nil {
			return 0, fmt.Errorf("scan promotion_seq: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("promotion_seq rows: %w", err)
	}
	return seq, nil
}

func (s ClickHousePromotionSeqStore) RecordPromotionSeq(ctx context.Context, table, partitionID string, seq uint64) error {
	if s.Exec == nil {
		return fmt.Errorf("clickhouse exec connection is required")
	}
	if err := s.ensure(ctx); err != nil {
		return err
	}
	current, err := s.LastPromotionSeq(ctx, table, partitionID)
	if err != nil {
		return err
	}
	if seq <= current {
		return fmt.Errorf("stale promotion_seq %d for partition %s: last applied %d", seq, partitionID, current)
	}
	return s.Exec.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (safe_table, partition_id, seq, updated_at) VALUES (%s, %s, %d, now64(3))",
		s.tableName(), sqlStringLiteral(table), sqlStringLiteral(partitionID), seq,
	))
}

func (s ClickHousePromotionSeqStore) ensure(ctx context.Context) error {
	if s.Exec == nil {
		return fmt.Errorf("clickhouse exec connection is required")
	}
	db := s.metadataDatabase()
	if err := s.Exec.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(db)); err != nil {
		return fmt.Errorf("create storage integrity metadata database: %w", err)
	}
	if err := s.Exec.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (safe_table String, partition_id String, seq UInt64, updated_at DateTime64(3)) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (safe_table, partition_id)",
		s.tableName(),
	)); err != nil {
		return fmt.Errorf("create promotion_seq table: %w", err)
	}
	return nil
}

func (s ClickHousePromotionSeqStore) tableName() string {
	return qualifiedTable(s.metadataDatabase(), "hg_promotion_seq")
}

func (s ClickHousePromotionSeqStore) metadataDatabase() string {
	if s.MetadataDatabase != "" {
		return s.MetadataDatabase
	}
	return "hg_meta"
}

type ClickHouseTableController struct {
	Conn                   SQLConn
	Query                  HashQueryConn
	StopMerges             bool
	EnforceNoMergeSettings bool
}

func (c ClickHouseTableController) PrepareDatabase(ctx context.Context, database string) error {
	if database == "" {
		return nil
	}
	if c.Query == nil {
		return fmt.Errorf("clickhouse query connection is required")
	}
	rows, err := c.Query.Query(ctx, fmt.Sprintf(
		"SELECT name FROM system.tables WHERE database = %s ORDER BY name",
		sqlStringLiteral(database),
	))
	if err != nil {
		return fmt.Errorf("query tables for %s: %w", database, err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan table for %s: %w", database, err)
		}
		if name != "" {
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("tables rows for %s: %w", database, err)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := c.PrepareTable(ctx, qualifiedTable(database, name)); err != nil {
			return err
		}
	}
	return nil
}

func (c ClickHouseTableController) PrepareTable(ctx context.Context, table string) error {
	if table == "" {
		return nil
	}
	if c.Conn == nil {
		return fmt.Errorf("clickhouse connection is required")
	}
	if c.StopMerges {
		if err := c.Conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			return fmt.Errorf("stop merges for %s: %w", table, err)
		}
	}
	if c.EnforceNoMergeSettings {
		if err := c.Conn.Exec(ctx, "ALTER TABLE "+table+" MODIFY SETTING merge_with_ttl_timeout = 0, merge_with_recompression_ttl_timeout = 0"); err != nil {
			return fmt.Errorf("enforce no-merge settings for %s: %w", table, err)
		}
	}
	return nil
}

type GuardingPromoter struct {
	Guard        ClickHouseTableController
	Resolver     SnapshotResolver
	VerifyActive bool
	Promoter     Promoter
}

func (g GuardingPromoter) Promote(ctx context.Context, task PromotionTask) (PromotionResult, error) {
	if err := g.verifyPromotionSnapshot(ctx, task); err != nil {
		return PromotionResult{}, err
	}
	for _, table := range uniqueNonEmpty([]string{task.SafeTable, task.UnsafeTable, task.SourceTable}) {
		if err := g.Guard.PrepareTable(ctx, table); err != nil {
			return PromotionResult{}, err
		}
	}
	return g.Promoter.Promote(ctx, task)
}

func (g GuardingPromoter) verifyPromotionSnapshot(ctx context.Context, task PromotionTask) error {
	if !g.VerifyActive {
		return nil
	}
	if task.BaseSafeSnapshotID == "" {
		return fmt.Errorf("base_safe_snapshot_id is required for promotion active part verification")
	}
	partitions := task.PartitionIDs
	if len(partitions) == 0 {
		partitions = task.DropPartitionIDs
	}
	if err := g.Resolver.VerifyLocalTable(ctx, task.BaseSafeSnapshotID, task.TableID, task.SafeTable, partitions); err != nil {
		return fmt.Errorf("verify promotion active parts: %w", err)
	}
	return nil
}

type GuardingMutationExecutor struct {
	Guard        ClickHouseTableController
	Resolver     SnapshotResolver
	VerifyActive bool
	Executor     MutationExecutor
}

func (g GuardingMutationExecutor) ExecuteMutation(ctx context.Context, task MutationTask) (MutationClaim, error) {
	if err := g.verifyMutationSnapshot(ctx, task); err != nil {
		return MutationClaim{}, err
	}
	if err := g.Guard.PrepareTable(ctx, task.SafeTable); err != nil {
		return MutationClaim{}, err
	}
	return g.Executor.ExecuteMutation(ctx, task)
}

func (g GuardingMutationExecutor) ReplayMutation(ctx context.Context, task MutationTask) (MutationReplayResult, error) {
	if err := g.verifyMutationSnapshot(ctx, task); err != nil {
		return MutationReplayResult{}, err
	}
	if err := g.Guard.PrepareTable(ctx, task.SafeTable); err != nil {
		return MutationReplayResult{}, err
	}
	return g.Executor.ReplayMutation(ctx, task)
}

func (g GuardingMutationExecutor) verifyMutationSnapshot(ctx context.Context, task MutationTask) error {
	if !g.VerifyActive {
		return nil
	}
	if task.BaseSafeSnapshotID == "" {
		return fmt.Errorf("base_safe_snapshot_id is required for mutation active part verification")
	}
	if err := g.Resolver.VerifyLocalTable(ctx, task.BaseSafeSnapshotID, task.TableID, task.SafeTable, task.PartitionIDs); err != nil {
		return fmt.Errorf("verify mutation active parts: %w", err)
	}
	return nil
}

type GuardingSafeAuditor struct {
	Guard   ClickHouseTableController
	Auditor SafeAuditor
}

func (g GuardingSafeAuditor) AuditSafe(ctx context.Context, task SafeAuditTask) (SafeAuditVote, error) {
	if err := g.Guard.PrepareTable(ctx, task.SafeTable); err != nil {
		return SafeAuditVote{}, err
	}
	return g.Auditor.AuditSafe(ctx, task)
}

type GuardingRepairSyncExecutor struct {
	Guard    ClickHouseTableController
	Executor RepairSyncExecutor
}

func (g GuardingRepairSyncExecutor) RepairSync(ctx context.Context, task RepairSyncTask) (RepairSyncResult, error) {
	for _, table := range uniqueNonEmpty([]string{task.SafeTable, task.SourceTable}) {
		if err := g.Guard.PrepareTable(ctx, table); err != nil {
			return RepairSyncResult{}, err
		}
	}
	return g.Executor.RepairSync(ctx, task)
}

type GuardingCompactor struct {
	Guard     ClickHouseTableController
	Compactor CompactionExecutor
}

func (g GuardingCompactor) Compact(ctx context.Context, task CompactionTask) (CompactionResult, error) {
	if err := g.Guard.PrepareTable(ctx, task.SafeTable); err != nil {
		return CompactionResult{}, err
	}
	return g.Compactor.Compact(ctx, task)
}
