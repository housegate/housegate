// Package network owns the view the proxy has of the sentio
// decentralized indexer network — which processors are allocated to
// which indexers, how to reach an indexer's ClickHouse proxy, and so
// on. The concrete producer is a Redis-backed statemirror; the proxy
// is a read-only consumer.
//
// Types defined here are standalone — JSON and YAML tags are kept
// wire-compatible with sentio-core's network/state package so YAML
// fixtures and RPC payloads exchange unchanged. Future schema
// additions must be mirrored explicitly; there is no transitive
// type-alias path.
//
// Nothing in pkg/network imports pkg/proxy, pkg/config, or plugins —
// the dependency flows the other way.
package network

import (
	"strconv"

	"housegate/housegate/pkg/registry"
)

// State is the read-only view of the network the proxy consumes.
// Implementations are in-memory (InMemoryNetworkState) for tests and
// YAML fixtures, RPC-backed (RpcNetworkState) for sidecar mode, and
// embedder-injected (sentio-node's Redis adapter) for production.
//
// All methods must be safe for concurrent use — every query handler
// goroutine reads from a single shared State, and an embedder-supplied
// implementation may be receiving updates concurrently.
//
// Lookup methods return (zero, false) when the key is not known and
// reserve error returns for cases where the lookup itself failed
// (network partition, malformed mirror entry, missing prerequisite).
// "Not found" is not an error — callers commonly probe for existence.
//
// AccountHasPermissionForDatabase is the one exception: an unknown
// database is reported as an error because permission checks against
// non-existent databases almost always indicate a caller bug.
type State interface {
	// RetrieveProcessorAllocation returns which indexers host
	// processorId. The slice is owned by the caller — implementations
	// must not retain it for later mutation. Returns ok=false when no
	// allocation exists for processorId.
	RetrieveProcessorAllocation(processorId ProcessorId) ([]ProcessorAllocation, bool)

	// RetrieveIndexerInfo returns connection information (URL + port
	// numbers) for indexerId. Returns ok=false when the indexer is
	// unknown to the network state.
	RetrieveIndexerInfo(indexerId IndexId) (IndexerInfo, bool)

	// RetrieveProcessorInfo returns processor metadata (entity schema
	// + schema version). Used by SQL rewriting to know which physical
	// table layout a processor's queries should target.
	RetrieveProcessorInfo(processorId ProcessorId) (ProcessorInfo, bool)

	// RetrieveAllIndexerInfos returns a snapshot of every known
	// indexer keyed by indexer id. The returned map is owned by the
	// caller and safe to retain. Forwarding-only proxies use this to
	// enumerate routing targets.
	RetrieveAllIndexerInfos() map[uint64]IndexerInfo

	// RetrieveDatabaseInfo returns the metadata record (indexer
	// assignment, db type, owning processor, pending-delete state)
	// for a logical database. Returns ok=false when the database has
	// not been declared in the network registry.
	RetrieveDatabaseInfo(database Database) (DatabaseInfo, bool)

	// RetrieveAllDatabaseInfos returns a snapshot of every known
	// logical database keyed by Database id. The returned map is
	// owned by the caller. The rewriter uses this for anonymous
	// connections, where every database is considered accessible
	// (the auth filter is skipped because there is no account to
	// scope it by).
	RetrieveAllDatabaseInfos() map[Database]DatabaseInfo

	// RetrieveDatabasePermissions returns the per-database permission
	// bitmap an account holds. The map is owned by the caller. An
	// account that exists but has no permissions on record returns
	// (empty-map, true); ok=false signals the lookup itself failed
	// (e.g. mirror unavailable).
	RetrieveDatabasePermissions(account AccountAddress) (DatabasePermissions, bool)

	// AccountHasPermissionForDatabase reports whether `account` has
	// the permission bit `action` on `database`. The standard
	// hierarchy (Owner ⇒ Admin|Write|Read; Write ⇒ Read) is applied
	// by implementations.
	//
	// Returns an error when the database is not found, or when the
	// underlying mirror lookup fails.
	AccountHasPermissionForDatabase(account AccountAddress, database Database, action Action) (bool, error)

	IsOperator(owner, signer AccountAddress) bool

	Type() StateType
}

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
// node (URL + per-service ports). JSON/YAML tags match the producer
// (sentio-core/network/state.IndexerInfo) so wire-format compatibility
// is preserved.
type IndexerInfo struct {
	IndexerId           uint64 `json:"indexerId" yaml:"indexer_id"`
	IndexerUrl          string `json:"indexerUrl" yaml:"indexer_url"`
	ComputeNodeRpcPort  uint16 `json:"computeNodeRpcPort" yaml:"compute_node_rpc_port"`
	StorageNodeRpcPort  uint16 `json:"storageNodeRpcPort" yaml:"storage_node_rpc_port"`
	ClickhouseProxyPort uint16 `json:"clickhouseProxyPort" yaml:"clickhouse_proxy_port"`
	Signer              string `json:"signer" yaml:"signer"`
}

// ProcessorAllocation links a processor to the indexer(s) that host
// its data. Wire-compatible with sentio-core/network/state.
type ProcessorAllocation struct {
	ProcessorId string `json:"processorId" yaml:"processor_id"`
	IndexerId   uint64 `json:"indexerId" yaml:"indexer_id"`
}

// ProcessorInfo is processor-level metadata used for table-name
// rewriting. Wire-compatible with sentio-core/network/state.
type ProcessorInfo struct {
	ProcessorId         string `json:"processorId" yaml:"processor_id"`
	EntitySchema        string `json:"entitySchema" yaml:"entity_schema"`
	EntitySchemaVersion int32  `json:"entitySchemaVersion" yaml:"entity_schema_version"`
}

// DatabaseType mirrors sentio-core's on-chain Types.DatabaseType enum:
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

type StateType string

const (
	StateTypeRedis    StateType = "redis"
	StateTypeInMemory StateType = "in_memory"
	StateTypeRpc      StateType = "rpc"
)
