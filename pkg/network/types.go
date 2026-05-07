// Package network owns the view the proxy has of the sentio
// decentralized indexer network — which processors are allocated to
// which indexers, how to reach an indexer's ClickHouse proxy, and so
// on. The concrete producer is a Redis-backed statemirror; the proxy is
// a read-only consumer.
//
// Types defined here are intentionally minimal — State is the
// interface that downstream code (rewriter, credentials, legacy proxy)
// programs against; IndexerInfo / ProcessorAllocation / ProcessorInfo
// are aliases to the authoritative sentio-core types so data stays byte
// compatible with the network's shared schema.
//
// Nothing in pkg/network imports pkg/proxy, pkg/config, or plugins —
// the dependency flows the other way.
package network

import (
	"strconv"

	"sentioxyz/sentio-core/network/registry"
	"sentioxyz/sentio-core/network/state"
)

// State is the read-only view of the network the proxy consumes.
// Implementations are typically Redis-backed (RedisNetworkState) in
// production and in-memory (InMemoryNetworkState) in tests.
//
// All methods must be safe for concurrent use — every query handler
// goroutine reads from a single shared State, and the Redis-backed
// implementation may be receiving statemirror updates concurrently.
//
// Lookup methods return (zero, false) when the key is not known and
// reserve error returns for cases where the lookup itself failed
// (network partition, malformed mirror entry, missing prerequisite).
// "Not found" is not an error — callers commonly probe for existence.
//
// AccountHasPermissionForDatabase is the one exception: an unknown
// database is reported as an error to match the sentio-core
// userDbRegistry contract, because permission checks against
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

	// RetrieveDatabaseInfo returns the metadata record (owner,
	// indexer assignment, db type) for a logical database. Returns
	// ok=false when the database has not been declared in the
	// network registry.
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
	// the permission bit `action` on `database`. The owner of a
	// database implicitly has DbAuthAdmin; the effective auth is
	// (per-account-bitmap | owner-bit) AND'd against `action`.
	//
	// Returns an error when the database is not found (matches
	// sentio-core userDbRegistry semantics), or when the underlying
	// mirror lookup fails.
	AccountHasPermissionForDatabase(account AccountAddress, database Database, action registry.Action) (bool, error)

	Type() StateType
}

// Type aliases to sentio-core so callers operate on the canonical
// shared schema without their own import of the sentio packages.
//
// Aliasing (vs wrapping) is deliberate: serialised JSON / YAML must
// stay byte-for-byte identical to what the network producers emit,
// and any future field added on the sentio side flows through with
// zero proxy changes.
type (
	// IndexId is the dense uint64 identifier the network state uses
	// for indexers. Statemirror keys indexers by string but the
	// canonical in-memory form is uint64.
	IndexId = uint64

	// ProcessorId is the string identifier of a Sentio processor
	// (e.g. "coinbase", "pancakeswap123"). Used as both the
	// allocation key and the db-prefix when rewriting "sentio_<id>."
	// table references.
	ProcessorId = string

	// IndexerInfo carries the network-visible address of a single
	// indexer node (URL + per-service ports). The proxy reads
	// ClickhouseProxyPort to dial peer proxies in remote() / __route__
	// flows.
	IndexerInfo = state.IndexerInfo

	// ProcessorAllocation links a processor to the indexer(s) that
	// host its data. The first allocation in the slice is the
	// primary; replicas (if any) follow.
	ProcessorAllocation = state.ProcessorAllocation

	// ProcessorInfo is processor-level metadata used for table-name
	// rewriting (schema -> physical name mapping).
	ProcessorInfo = state.ProcessorInfo

	// DatabaseInfo describes a logical user-facing database: its
	// type, the indexer that hosts its tables, owning processor (for
	// PROCESSOR-type databases), and pending-delete state. Ownership
	// and access are tracked exclusively via DatabasePermissions
	// bitmaps (Owner / Admin / Write / Read).
	DatabaseInfo = state.DatabaseInfo

	// TableInfo is a single table entry inside DatabaseInfo.Tables.
	// Aliased here so callers that mutate the table list (e.g. the
	// in-memory commitgate observer) don't need a direct dep on
	// sentio-core/network/state.
	TableInfo = state.TableInfo

	// Database is the canonical id type for a logical user database.
	// Distinct from `string` only at the type-system level — values
	// are plain strings on the wire.
	Database = registry.Database

	// AccountAddress is an account identifier (Ethereum address as a
	// string). Distinct from Database so the compiler catches
	// argument-order mistakes in permission lookups.
	AccountAddress = registry.Address

	// DatabasePermissions is the per-account permission bitmap keyed
	// by Database. Each value is a registry.DbAuth bitmap; combine
	// with bitwise OR. Use registry.DbAuthRead / DbAuthWrite /
	// DbAuthAdmin to read individual bits.
	DatabasePermissions = map[Database]registry.DbAuth
)

// ParseIndexerId parses a decimal-encoded indexer id. Handy when the id
// arrives as a Redis field key (statemirror keys indexers by string id).
func ParseIndexerId(s string) (IndexId, error) {
	return strconv.ParseUint(s, 10, 64)
}

type StateType string

const (
	StateTypeRedis    StateType = "redis"
	StateTypeInMemory StateType = "in_memory"
	StateTypeRpc      StateType = "rpc"
)
