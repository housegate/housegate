package schemaregistry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

var (
	// ErrSchemaContentMissing means network state does not contain the latest
	// declared schema content for a requested table.
	ErrSchemaContentMissing = errors.New("schemaregistry: schema content missing")
	// ErrSchemaHashMismatch means canonical schema content did not reproduce
	// the mirrored per-table commitment under this loader's network id.
	ErrSchemaHashMismatch = errors.New("schemaregistry: schema hash mismatch")
	// ErrSchemaIdentityMismatch means a declaration resolved for one logical
	// table embeds coordinates or canonical schema content for another table.
	ErrSchemaIdentityMismatch = errors.New("schemaregistry: schema identity mismatch")
	// ErrClickHouseDrift means the declared schema differs from the schema
	// materialized by the local ClickHouse.
	ErrClickHouseDrift = errors.New("schemaregistry: clickhouse schema drift")
)

// NetworkStateLoader loads declared schema content from network state, checks
// every per-table commitment, and optionally audits the result against the
// local ClickHouse through the Phase-A Loader seam.
type NetworkStateLoader struct {
	schemas    registry.TableSchemas
	networkID  string
	crossCheck Loader
}

// NewNetworkStateLoader returns a network-state-backed schema loader.
func NewNetworkStateLoader(schemas registry.TableSchemas, networkID string) *NetworkStateLoader {
	return &NetworkStateLoader{schemas: schemas, networkID: networkID}
}

// WithClickHouseCrossCheck configures a local ClickHouse audit of every schema
// loaded from network state.
func (l *NetworkStateLoader) WithClickHouseCrossCheck(conn clickhouse.Conn) *NetworkStateLoader {
	return l.withCrossCheckLoader(NewClickHouseLoader(conn))
}

// withCrossCheckLoader is the package-local test seam for the cross-check
// decorator. Production callers use WithClickHouseCrossCheck.
func (l *NetworkStateLoader) withCrossCheckLoader(loader Loader) *NetworkStateLoader {
	l.crossCheck = loader
	return l
}

// Load resolves the latest declaration for each logical table id, verifies its
// canonical content hash, and returns schemas in the supplied ref order.
func (l *NetworkStateLoader) Load(ctx context.Context, refs []TableRef) ([]payloadexec.TableSchema, error) {
	if err := validateRefs(refs); err != nil {
		return nil, err
	}
	if l.schemas == nil {
		return nil, fmt.Errorf("schemaregistry: network-state TableSchemas source is required")
	}

	out := make([]payloadexec.TableSchema, 0, len(refs))
	for _, ref := range refs {
		databaseID, tableID := ref.LogicalDatabase, ref.LogicalTable
		if databaseID == "" || tableID == "" {
			var err error
			databaseID, tableID, err = logicalTableCoordinates(ref.TableID)
			if err != nil {
				return nil, err
			}
		}
		latest, ok := l.schemas.LatestTableSchema(databaseID, tableID)
		if !ok {
			return nil, fmt.Errorf("%w: table %q", ErrSchemaContentMissing, ref.TableID)
		}
		declared, ok := l.schemas.TableSchema(databaseID, tableID, latest.Version)
		if !ok {
			return nil, fmt.Errorf(
				"%w: table %q version %d",
				ErrSchemaContentMissing,
				ref.TableID,
				latest.Version,
			)
		}
		if declared.DatabaseId != databaseID || declared.TableId != tableID {
			return nil, fmt.Errorf(
				"%w: table %q version %d declaration coordinates %q.%q",
				ErrSchemaIdentityMismatch,
				ref.TableID,
				declared.Version,
				declared.DatabaseId,
				declared.TableId,
			)
		}

		var schema payloadexec.TableSchema
		if err := json.Unmarshal([]byte(declared.SchemaJson), &schema); err != nil {
			return nil, fmt.Errorf(
				"schemaregistry: decode schema content for table %q version %d: %w",
				ref.TableID,
				declared.Version,
				err,
			)
		}
		if schema.TableID != ref.TableID {
			return nil, fmt.Errorf(
				"%w: requested table %q version %d embeds table_id %q",
				ErrSchemaIdentityMismatch,
				ref.TableID,
				declared.Version,
				schema.TableID,
			)
		}
		computed := payloadexec.TableSchemaHash(l.networkID, schema)
		if computed != declared.SchemaHash {
			return nil, fmt.Errorf(
				"%w: table %q version %d declared %s computed %s",
				ErrSchemaHashMismatch,
				ref.TableID,
				declared.Version,
				declared.SchemaHash,
				computed,
			)
		}
		out = append(out, schema)
	}

	if l.crossCheck == nil {
		return out, nil
	}
	local, err := l.crossCheck.Load(ctx, refs)
	if err != nil {
		return nil, fmt.Errorf("schemaregistry: clickhouse cross-check: %w", err)
	}
	if len(local) != len(out) {
		return nil, fmt.Errorf(
			"%w: schema count network_state=%d clickhouse=%d",
			ErrClickHouseDrift,
			len(out),
			len(local),
		)
	}
	for i := range out {
		if reflect.DeepEqual(out[i], local[i]) {
			continue
		}
		return nil, fmt.Errorf(
			"%w: table %q field %s",
			ErrClickHouseDrift,
			refs[i].TableID,
			firstSchemaDifference(out[i], local[i]),
		)
	}
	return out, nil
}

func logicalTableCoordinates(tableID string) (string, string, error) {
	databaseID, table, ok := strings.Cut(tableID, ".")
	if !ok || databaseID == "" || table == "" {
		return "", "", fmt.Errorf(
			"schemaregistry: logical table id %q must have <database>.<table> form",
			tableID,
		)
	}
	return databaseID, table, nil
}

func firstSchemaDifference(declared, local payloadexec.TableSchema) string {
	if declared.TableID != local.TableID {
		return fmt.Sprintf("table_id (network_state=%q clickhouse=%q)", declared.TableID, local.TableID)
	}
	if declared.PartitionBy != local.PartitionBy {
		return fmt.Sprintf(
			"partition_by (network_state=%q clickhouse=%q)",
			declared.PartitionBy,
			local.PartitionBy,
		)
	}
	if len(declared.Columns) != len(local.Columns) {
		return fmt.Sprintf(
			"columns.length (network_state=%d clickhouse=%d)",
			len(declared.Columns),
			len(local.Columns),
		)
	}
	for i := range declared.Columns {
		if declared.Columns[i].Name != local.Columns[i].Name {
			return fmt.Sprintf(
				"columns[%d].name (network_state=%q clickhouse=%q)",
				i,
				declared.Columns[i].Name,
				local.Columns[i].Name,
			)
		}
		if declared.Columns[i].Type != local.Columns[i].Type {
			return fmt.Sprintf(
				"columns[%d].type (network_state=%q clickhouse=%q)",
				i,
				declared.Columns[i].Type,
				local.Columns[i].Type,
			)
		}
	}
	return "schema"
}
