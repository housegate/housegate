// Package network owns the view the proxy has of the sentio
// decentralized indexer network — which processors are allocated to
// which indexers, how to reach an indexer's ClickHouse proxy, and so
// on. The concrete producer is a Redis-backed statemirror; the proxy
// is a read-only consumer.
//
// Types defined here are standalone — JSON and YAML tags are kept
// wire-compatible with the upstream statemirror producer so YAML
// fixtures and RPC payloads exchange unchanged. Future schema
// additions must be mirrored explicitly; there is no transitive
// type-alias path.
//
// Nothing in pkg/network imports pkg/proxy, pkg/config, or plugins —
// the dependency flows the other way.
package network

import (
	"strconv"

	"github.com/housegate/housegate/pkg/registry"
)

// IndexId is the dense uint64 identifier for an indexer. Statemirror
// keys indexers by stringified id; the in-memory form is uint64.
type IndexId = uint64

// ProcessorId is the string identifier of a Sentio processor (e.g.
// "coinbase", "pancakeswap123").
type ProcessorId = string

// Database is the canonical id type for a logical user database.
// Distinct from `string` only at the type-system level — values are
// plain strings on the wire.
type Database string

// AccountAddress is an account identifier (Ethereum address as a
// string). Distinct from Database so the compiler catches
// argument-order mistakes in permission lookups.
type AccountAddress string

// DbAuth is the bitmap of permissions an account holds on one
// database. Aliased to pkg/registry's identical type so both packages
// agree byte-for-byte and conversions are zero-cost.
type DbAuth = registry.DbAuth

// Action is the permission bit a caller is checking for. Aliased to
// pkg/registry for the same reason as DbAuth.
type Action = registry.Action

// DatabasePermissions is the per-account permission bitmap keyed by
// Database. Combine bits with bitwise OR; check individual bits using
// registry.DbAuthRead / DbAuthWrite / DbAuthAdmin / DbAuthOwner.
type DatabasePermissions = map[Database]DbAuth

// WildcardAddress is the all-zeros account whose permissions are
// unioned into every caller's effective bitmap. A grant to this
// address means "everyone has this permission" and is the encoding
// the smart-contract layer uses for public reads.
const WildcardAddress AccountAddress = "0x0000000000000000000000000000000000000000"

// IndexerInfo carries the network-visible address of a single indexer
// node (URL + per-service ports). JSON/YAML tags match the upstream
// statemirror producer's IndexerInfo so wire-format compatibility is
// preserved.
type IndexerInfo struct {
	IndexerId           uint64 `json:"indexerId" yaml:"indexer_id"`
	IndexerUrl          string `json:"indexerUrl" yaml:"indexer_url"`
	ComputeNodeRpcPort  uint16 `json:"computeNodeRpcPort" yaml:"compute_node_rpc_port"`
	StorageNodeRpcPort  uint16 `json:"storageNodeRpcPort" yaml:"storage_node_rpc_port"`
	ClickhouseProxyPort uint16 `json:"clickhouseProxyPort" yaml:"clickhouse_proxy_port"`
	Signer              string `json:"signer" yaml:"signer"`
}

// ProcessorAllocation links a processor to the indexer(s) that host
// its data. Wire-compatible with the upstream statemirror producer.
type ProcessorAllocation struct {
	ProcessorId string `json:"processorId" yaml:"processor_id"`
	IndexerId   uint64 `json:"indexerId" yaml:"indexer_id"`
}

// ProcessorInfo is processor-level metadata used for table-name
// rewriting. Wire-compatible with the upstream statemirror producer.
type ProcessorInfo struct {
	ProcessorId         string `json:"processorId" yaml:"processor_id"`
	EntitySchema        string `json:"entitySchema" yaml:"entity_schema"`
	EntitySchemaVersion int32  `json:"entitySchemaVersion" yaml:"entity_schema_version"`
}

// DatabaseType mirrors the on-chain Types.DatabaseType enum:
// USER = 0 (user-owned database), PROCESSOR = 1 (processor replica).
type DatabaseType uint8

const (
	DatabaseTypeUser      DatabaseType = 0
	DatabaseTypeProcessor DatabaseType = 1
)

// TableInfo is a single table entry inside DatabaseInfo.Tables.
type TableInfo struct {
	TableId   string `json:"tableId" yaml:"table_id"`
	TableType string `json:"tableType" yaml:"table_type"`
}

// DatabaseInfo describes a logical user-facing database: its type,
// the indexer hosting its tables, owning processor (for PROCESSOR-type
// databases), and pending-delete state. Ownership and access are
// tracked exclusively via DatabasePermissions bitmaps (Owner / Admin /
// Write / Read).
type DatabaseInfo struct {
	DatabaseId    string       `json:"databaseId" yaml:"database_id"`
	DbType        DatabaseType `json:"dbType" yaml:"db_type"`
	IndexerId     uint64       `json:"indexerId" yaml:"indexer_id"`
	ProcessorId   string       `json:"processorId,omitempty" yaml:"processor_id,omitempty"`
	PendingDelete bool         `json:"pendingDelete" yaml:"pending_delete"`
	Tables        []TableInfo  `json:"tables,omitempty" yaml:"tables,omitempty"`
}

// ParseIndexerId parses a decimal-encoded indexer id. Handy when the
// id arrives as a Redis hash field key (statemirror keys indexers by
// stringified id).
func ParseIndexerId(s string) (IndexId, error) {
	return strconv.ParseUint(s, 10, 64)
}
