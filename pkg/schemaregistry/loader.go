// Package schemaregistry loads column-level storage-integrity table schemas
// from a source and hands them to consumers as payloadexec.TableSchema. The
// consensus anchor is not here: whatever a Loader returns still flows through
// payloadexec.SchemaRoot and must match the genesis schema_root, so a wrong
// source refuses startup instead of diverging replay roots.
package schemaregistry

import (
	"context"
	"fmt"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

const protocolRowIDColumn = "_hg_row_id"

// TableRef names one storage-integrity table to load: the logical id that feeds
// TableSchemaHash, and the physical ClickHouse coordinates to introspect.
// Callers own the id-to-physical mapping.
type TableRef struct {
	TableID         string
	Database        string
	Table           string
	LogicalDatabase string
	LogicalTable    string
}

// Loader is the schema-source seam. Phase A implements it over the local
// ClickHouse; Phase B re-implements it over network state with hash
// verification, without changing consumers.
type Loader interface {
	Load(ctx context.Context, refs []TableRef) ([]payloadexec.TableSchema, error)
}

// ClickHouseLoader derives schemas from ClickHouse system metadata.
type ClickHouseLoader struct {
	conn clickhouse.Conn
}

// NewClickHouseLoader returns a schema loader backed by conn.
func NewClickHouseLoader(conn clickhouse.Conn) *ClickHouseLoader {
	return &ClickHouseLoader{conn: conn}
}

// Load derives schemas for refs in their supplied order.
func (l *ClickHouseLoader) Load(ctx context.Context, refs []TableRef) ([]payloadexec.TableSchema, error) {
	if err := validateRefs(refs); err != nil {
		return nil, err
	}
	if l.conn == nil {
		return nil, fmt.Errorf("schemaregistry: clickhouse connection is required")
	}

	out := make([]payloadexec.TableSchema, 0, len(refs))
	for _, ref := range refs {
		schema, err := l.loadOne(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("schemaregistry: table %s (%s.%s): %w", ref.TableID, ref.Database, ref.Table, err)
		}
		out = append(out, schema)
	}
	return out, nil
}

func validateRefs(refs []TableRef) error {
	if len(refs) == 0 {
		return fmt.Errorf("schemaregistry: at least one table reference is required")
	}
	seen := make(map[string]bool, len(refs))
	for i, ref := range refs {
		if ref.TableID == "" {
			return fmt.Errorf("schemaregistry: table reference %d: table id is required", i)
		}
		if ref.Database == "" {
			return fmt.Errorf("schemaregistry: table reference %d (%s): database is required", i, ref.TableID)
		}
		if ref.Table == "" {
			return fmt.Errorf("schemaregistry: table reference %d (%s): table is required", i, ref.TableID)
		}
		if seen[ref.TableID] {
			return fmt.Errorf("schemaregistry: duplicate table id %q", ref.TableID)
		}
		seen[ref.TableID] = true
	}
	return nil
}

func (l *ClickHouseLoader) loadOne(ctx context.Context, ref TableRef) (payloadexec.TableSchema, error) {
	var partitionKey string
	row := l.conn.QueryRow(
		ctx,
		"SELECT partition_key FROM system.tables WHERE database = @db AND name = @table",
		clickhouse.Named("db", ref.Database),
		clickhouse.Named("table", ref.Table),
	)
	if err := row.Scan(&partitionKey); err != nil {
		return payloadexec.TableSchema{}, fmt.Errorf("table not found or unreadable: %w", err)
	}

	rows, err := l.conn.Query(
		ctx,
		"SELECT name, type FROM system.columns WHERE database = @db AND table = @table AND name != @rowid ORDER BY position",
		clickhouse.Named("db", ref.Database),
		clickhouse.Named("table", ref.Table),
		clickhouse.Named("rowid", protocolRowIDColumn),
	)
	if err != nil {
		return payloadexec.TableSchema{}, fmt.Errorf("list columns: %w", err)
	}
	defer rows.Close()

	var columns []lthash.Column
	for rows.Next() {
		var name string
		var typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return payloadexec.TableSchema{}, fmt.Errorf("scan column: %w", err)
		}
		columns = append(columns, lthash.Column{Name: name, Type: typ})
	}
	if err := rows.Err(); err != nil {
		return payloadexec.TableSchema{}, fmt.Errorf("iterate columns: %w", err)
	}
	if len(columns) == 0 {
		return payloadexec.TableSchema{}, fmt.Errorf("no user columns (does the table exist and carry more than %s?)", protocolRowIDColumn)
	}

	return payloadexec.TableSchema{
		TableID:     ref.TableID,
		Columns:     columns,
		PartitionBy: partitionKey,
	}, nil
}
