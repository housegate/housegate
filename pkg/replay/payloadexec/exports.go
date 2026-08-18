package payloadexec

import "github.com/housegate/housegate/pkg/lthash"

// RowIDProfileID names the _hg_row_id derivation profile (§5.2). Envelope v2
// signs it as row_id_profile_id; verifiers reject any other value.
const RowIDProfileID = rowIDDomain

// PayloadFormatCSVWithNames is the legacy/test wire encoding decoded by
// DecodeCSV. Production storage-integrity payloads are Native
// (pkg/replay/nativepayload.PayloadFormat).
const PayloadFormatCSVWithNames = "csv-with-names-v1"

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
