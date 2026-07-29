package registry

// TableSchema is one versioned, declared table-schema record mirrored from
// network state. SchemaHash is the "0x"-prefixed payloadexec.TableSchemaHash;
// SchemaJson is the canonical JSON whose digest it commits to.
type TableSchema struct {
	DatabaseId string
	TableId    string
	Version    uint32
	SchemaHash string
	SchemaJson string
}

// TableSchemas is the standalone schema-content view. It intentionally does
// not join Registry: routing-only implementations should not need to stub
// storage-integrity schema content they cannot provide.
type TableSchemas interface {
	TableSchema(databaseId, tableId string, version uint32) (TableSchema, bool)
	LatestTableSchema(databaseId, tableId string) (TableSchema, bool)
}
