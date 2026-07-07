package payloadexec

import "housegate/housegate/pkg/lthash"

// RowElementHash exposes the executor's canonical per-row LtHash element. Data
// plane scanners call this instead of reimplementing row identity or row
// encoding.
func RowElementHash(sch TableSchema, rowID []byte, values []any) (*lthash.Hash, error) {
	return rowElementHash(sch, rowID, values)
}

// SchemaRoot exposes the deployment schema-root derivation used by replay
// manifests and Arbiter genesis checks.
func SchemaRoot(networkID string, schemas []TableSchema) string {
	return schemaRoot(networkID, schemas)
}

// TableSchemaHash exposes the per-table schema digest used in table manifests.
func TableSchemaHash(networkID string, t TableSchema) string {
	return tableSchemaHash(networkID, t)
}
