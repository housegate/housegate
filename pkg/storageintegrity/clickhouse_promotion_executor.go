package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHousePromotionExecutor struct {
	conn             clickhouse.Conn
	StatementTimeout time.Duration
	ReadbackTimeout  time.Duration
}

func NewClickHousePromotionExecutor(addr string) (*ClickHousePromotionExecutor, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("clickhouse upstream address is required")
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse promotion connection: %w", err)
	}
	return &ClickHousePromotionExecutor{conn: conn}, nil
}

func (e *ClickHousePromotionExecutor) ExecPromotionSQL(ctx context.Context, sql string) error {
	if e == nil || e.conn == nil {
		return fmt.Errorf("clickhouse promotion executor is nil")
	}
	execCtx, cancel := context.WithTimeout(ctx, durationOrDefault(e.StatementTimeout, defaultPromotionStatementTimeout))
	defer cancel()
	err := e.conn.Exec(execCtx, sql)
	if err != nil && promotionStatementMayHaveApplied(sql) && (errors.Is(err, context.DeadlineExceeded) || execCtx.Err() != nil) {
		return nil
	}
	return err
}

func (e *ClickHousePromotionExecutor) ExecRollbackSQL(ctx context.Context, sql string) error {
	if e == nil || e.conn == nil {
		return fmt.Errorf("clickhouse rollback executor is nil")
	}
	return e.conn.Exec(ctx, sql)
}

func (e *ClickHousePromotionExecutor) ReadPromotionRows(ctx context.Context, spec PromotionReadbackSpec) (PromotionReadbackResult, error) {
	if e == nil || e.conn == nil {
		return PromotionReadbackResult{}, fmt.Errorf("clickhouse promotion executor is nil")
	}
	if strings.TrimSpace(spec.Table) == "" {
		return PromotionReadbackResult{}, nil
	}
	if strings.TrimSpace(spec.PromotionExpr) != "" {
		return PromotionReadbackResult{}, fmt.Errorf("promotion readback expression is not supported by parts digest reader")
	}
	databaseName, tableName, err := splitClickHouseQualifiedTableName(spec.Table)
	if err != nil {
		return PromotionReadbackResult{}, err
	}
	readCtx, cancel := context.WithTimeout(ctx, durationOrDefault(e.ReadbackTimeout, defaultPromotionReadbackTimeout))
	defer cancel()
	var rows uint64
	var rowsHash string
	if err := queryActivePartsDigest(readCtx, e.conn, databaseName, tableName, &rows, &rowsHash); err != nil {
		return PromotionReadbackResult{}, err
	}
	return PromotionReadbackResult{RowCount: rows, RowsHash: "0x" + strings.TrimPrefix(rowsHash, "0x")}, nil
}

const (
	defaultPromotionStatementTimeout = 10 * time.Second
	defaultPromotionReadbackTimeout  = 10 * time.Second
)

func durationOrDefault(d, fallback time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return fallback
}

func promotionStatementMayHaveApplied(sql string) bool {
	sql = strings.ToLower(strings.TrimSpace(sql))
	return strings.HasPrefix(sql, "insert ") ||
		strings.HasPrefix(sql, "truncate ") ||
		(strings.HasPrefix(sql, "alter table ") && strings.Contains(sql, " attach partition "))
}
