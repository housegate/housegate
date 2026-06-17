package registry

// Database is the slice of database metadata housegate consumes. It
// intentionally omits fields the producer tracks (owning-account, audit
// metadata, etc.) that no housegate code path reads today; widen only
// when a real consumer needs the field.
type Database struct {
	// IndexerId is the indexer hosting this database's physical tables.
	// Used by forward/agent/rewriter to decide local vs. remote and to
	// resolve the peer dialing target via Topology.
	IndexerId uint64

	// PendingDelete marks a database on its way out. Permission and
	// routing decisions filter these as if the database did not exist.
	PendingDelete bool

	// DbType mirrors the on-chain database classification:
	//   0 = USER       (user-owned database)
	//   1 = PROCESSOR  (processor replica DB managed by the indexer)
	// Part of the NetworkState schema; no housegate proxy-chain code
	// reads it today — it is consumed by the host (sentio-node) through
	// the registry interface, e.g. to attribute indexing usage to a
	// processor. Any future enum value should reuse the producer's
	// encoding.
	DbType uint8

	// ProcessorId is the id of the processor that owns this DB. Set
	// only when DbType == PROCESSOR; empty for user DBs. Part of the
	// NetworkState schema; not read by housegate itself — the host
	// (sentio-node) reads it through the registry interface to attribute
	// indexing-usage INSERTs to a processor on chain.
	ProcessorId string

	// Tables is the current table set. The commitgate observer reads
	// and (on the in-memory impl) mutates this slice as DDL events fire.
	Tables []Table
}

// Table is the minimal per-table record housegate tracks.
type Table struct {
	Id string
	// Type is the producer-side classification of this table. Empty
	// for user-created tables that arrived via raw CREATE TABLE; one
	// of "counter" / "gauge" / "event" / "entity" for driver-created
	// processor tables registered via the DatabaseRegistryService
	// gRPC. Not read by housegate itself; the host (sentio-node)
	// maps it to an on-chain SKU (counter/gauge → metric, event →
	// event, entity → entity) and treats everything else as
	// not-billable.
	Type string
}

// Databases looks up database metadata by logical id.
type Databases interface {
	// Get returns the database record for id. (zero, false) signals an
	// unknown database — not an error.
	Get(id string) (Database, bool)

	// All returns a snapshot of every known database keyed by id. The
	// returned map is owned by the caller; implementations must return
	// a fresh copy.
	All() map[string]Database
}
