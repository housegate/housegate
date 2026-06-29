package storageintegrity

import (
	"context"
	"fmt"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHousePromotionExecutor struct {
	conn clickhouse.Conn
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
	return e.conn.Exec(ctx, sql)
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
	sql := "SELECT count() FROM " + spec.Table
	if strings.TrimSpace(spec.PromotionExpr) != "" {
		sql += " WHERE " + spec.PromotionExpr
	}
	var rows uint64
	if err := e.conn.QueryRow(ctx, sql).Scan(&rows); err != nil {
		return PromotionReadbackResult{}, err
	}
	return PromotionReadbackResult{RowCount: rows}, nil
}
