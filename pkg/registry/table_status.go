package registry

import "context"

// Storage-integrity table status names of the JSON-RPC contract (spec
// 2026-09-24 §10.2). They equal sitable.Status.String().
const (
	TableStatusOrdinary = "ordinary"
	TableStatusPending  = "pending"
	TableStatusRefused  = "refused"
	TableStatusActive   = "active"
	TableStatusGone     = "gone"
)

// TableStatus is one answer of sentio_getStorageIntegrityTableStatus, with the
// semantics of sitable.Snapshot.Lookup on the serving node. SchemaJSON and
// SchemaHash are set for active (and gone) tables.
type TableStatus struct {
	Status          string `json:"status"`
	RefusedCode     string `json:"refused_code"`
	RefusedReason   string `json:"refused_reason"`
	SchemaJSON      string `json:"schema_json"`
	SchemaHash      string `json:"schema_hash"`
	RegistryVersion uint64 `json:"registry_version"`
}

// TableStatuses is the agent's per-INSERT status source. Like TableSchemas it
// stays out of Registry so routing-only implementations need not stub it.
type TableStatuses interface {
	StorageIntegrityTableStatus(ctx context.Context, database, table string) (TableStatus, error)
}

// TableStatusesFromSchemas adapts a declared-schema source (the YAML
// table_schemas fixture, or a host-injected TableSchemas) into a status
// source: a declared table is active with its latest declaration, and every
// other table is ordinary.
func TableStatusesFromSchemas(schemas TableSchemas) TableStatuses {
	return declaredSchemaStatuses{schemas: schemas}
}

type declaredSchemaStatuses struct{ schemas TableSchemas }

func (d declaredSchemaStatuses) StorageIntegrityTableStatus(_ context.Context, database, table string) (TableStatus, error) {
	latest, ok := d.schemas.LatestTableSchema(database, table)
	if !ok {
		return TableStatus{Status: TableStatusOrdinary}, nil
	}
	return TableStatus{Status: TableStatusActive, SchemaJSON: latest.SchemaJson, SchemaHash: latest.SchemaHash}, nil
}
