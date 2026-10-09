package registry

// DbAuth is the bitmap of permissions an account holds on one database.
// Bits compose via bitwise OR.
type DbAuth int64

const (
	DbAuthRead DbAuth = 1 << iota
	DbAuthWrite
	DbAuthAdmin
	DbAuthOwner
)

// Action is the permission bit a caller is checking for. Bits align
// with DbAuth so HasPermission can do a single AND.
type Action int64

const (
	Read  Action = Action(DbAuthRead)
	Write Action = Action(DbAuthWrite)
)

// Access answers permission and operator questions. Implementations are
// expected to be safe for concurrent use.
type Access interface {
	// PermissionsFor returns the per-database permission bitmap for
	// account. (nil, false) means the account has no record on file; an
	// account that exists but holds no grants returns (empty-map, true).
	// The returned map is owned by the caller.
	PermissionsFor(account string) (map[string]DbAuth, bool)

	// HasPermission tests whether account holds the action bit on
	// database. Implementations should apply the standard hierarchy
	// (Owner ⇒ Admin|Write|Read, Write ⇒ Read) when interpreting stored
	// bitmaps.
	//
	// Returns an error when the database is not known — permission
	// checks against non-existent databases almost always indicate a
	// caller bug, and surfacing them loudly matches housegate's prior
	// AccountHasPermissionForDatabase contract.
	HasPermission(account, database string, action Action) (bool, error)

	// IsOperator reports whether signer is authorized to act on behalf
	// of owner. owner == signer is always true; otherwise an explicit
	// grant must exist. Empty strings always return false.
	IsOperator(owner, signer string) bool
}

// WriterAccess answers the contract's Databases.isDatabaseWriter exactly:
// indexer signer of the database's indexer, or the account's own Owner (0x08)
// or Write (0x02) bit. Admin alone and address(0) grants do not count. An
// unknown or pending-delete database is an error, which callers refuse.
// Implemented by the host over raw chain state (sentio-node FromStatecore) and
// by the in-memory network state; RpcNetworkState does not implement it.
type WriterAccess interface {
	IsDatabaseWriter(database, account string) (bool, error)
}

// OperatorChecker is the operator half of Access, for consumers that need
// only IsOperator (the storage-integrity ingress authorizer).
type OperatorChecker interface {
	IsOperator(owner, signer string) bool
}
