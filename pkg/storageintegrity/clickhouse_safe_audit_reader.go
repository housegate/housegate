package storageintegrity

import (
	"context"
	"fmt"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseSafeAuditReader struct {
	conn    clickhouseReplayConn
	Timeout time.Duration
	layout  TableLayout
}

func NewClickHouseSafeAuditReader(addr string, layout TableLayout) (*ClickHouseSafeAuditReader, error) {
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
		return nil, fmt.Errorf("open clickhouse safe audit connection: %w", err)
	}
	return &ClickHouseSafeAuditReader{conn: conn, layout: layout}, nil
}

func (r *ClickHouseSafeAuditReader) ReadSafeRows(ctx context.Context, task SafeAuditTask) ([]SafeRow, error) {
	if r == nil || r.conn == nil {
		return nil, fmt.Errorf("clickhouse safe audit reader is nil")
	}
	table := safeAuditTable(task, r.layout)
	if table == "" {
		return nil, fmt.Errorf("safe audit task %q has no safe table range", task.AuditID)
	}
	databaseName, tableName, err := splitClickHouseQualifiedTableName(table)
	if err != nil {
		return nil, err
	}
	timeout := durationOrDefault(r.Timeout, 30*time.Second)
	readCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	rows, err := readTableRowsForHash(readCtx, r.conn, databaseName, tableName)
	if err != nil {
		return nil, err
	}
	out := make([]SafeRow, 0, len(rows))
	for _, row := range rows {
		values := make([]any, len(row.Values))
		for i, value := range row.Values {
			values[i] = value
		}
		out = append(out, SafeRow{RowID: row.RowID, Values: values})
	}
	return out, nil
}

func safeAuditTable(task SafeAuditTask, layout TableLayout) string {
	for _, field := range strings.FieldsFunc(task.Range, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	}) {
		if strings.HasPrefix(field, "safe=") {
			return strings.TrimPrefix(field, "safe=")
		}
	}
	if strings.HasPrefix(task.Range, "safe=") {
		return strings.TrimPrefix(task.Range, "safe=")
	}
	if task.TableID != "" {
		return layout.SafeTable(task.TableID)
	}
	return ""
}
