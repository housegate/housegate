package registry

// Database is the slice of database metadata housegate consumes. It
// intentionally omits fields the producer tracks (DbType, ProcessorId,
// owning-account, etc.) that no housegate code path reads today; widen
// only when a real consumer needs the field.
type Database struct {
	// IndexerId is the indexer hosting this database's physical tables.
	// Used by forward/sidecar/rewriter to decide local vs. remote and to
	// resolve the peer dialing target via Topology.
	IndexerId uint64

	// PendingDelete marks a database on its way out. Permission and
	// routing decisions filter these as if the database did not exist.
	PendingDelete bool

	// Tables is the current table set. The commitgate observer reads
	// and (on the in-memory impl) mutates this slice as DDL events fire.
	Tables []Table
}

// Table is the minimal per-table record housegate tracks.
type Table struct {
	Id string
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
