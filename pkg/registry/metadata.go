package registry

// DatabaseMetadata is the secondary view for non-routing auxiliary
// fields a database carries (processor ownership, classification,
// audit info, ...). It is intentionally separate from Databases so
// consumers that only need routing decisions don't depend on
// metadata they don't read.
//
// The interface follows the (b)-style design: one typed getter per
// concrete field, added as real consumers emerge. The (id) → (value,
// ok) shape mirrors the rest of pkg/registry: ok=false means the
// database is unknown or has no value on file, not an error.
//
// No consumer reads this in the proxy chain today; ProcessorId is the
// seed method so the interface is type-system-meaningful (an empty
// Go interface is satisfied by every value and provides no real
// guarantee). When a future consumer needs another field, add a
// method here, implement it on the adapter, and keep this comment
// honest.
type DatabaseMetadata interface {
	// ProcessorId returns the id of the processor that owns this
	// database. Returns ("", false) when the database is unknown or
	// has no processor association (i.e. a user-owned database).
	ProcessorId(databaseId string) (string, bool)
}
