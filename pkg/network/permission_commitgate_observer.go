package network

import (
	"context"
	"fmt"

	"sentioxyz/sentio-core/network/registry"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/plugins/commitgate"
	"housegate/housegate/pkg/sqlmeta"
)

// PermissionCommitGateObserver enforces per-StatementType database
// permission policy by consulting NetworkState (production: Redis-
// backed RedisNetworkState; tests: InMemoryNetworkState).
//
// Topology — the observer relies on commitgate.Plugin's framework
// filters and adds no extra topology of its own:
//
//   - Forward-to-peer: commitgate.Plugin implements ForwardAware with
//     RunOnForward()=false, so this observer never fires on the
//     originating proxy when forward.Plugin pivots upstream to a peer.
//     The host proxy that owns the database is the one running the
//     check.
//   - Routed (__route__) sessions: commitgate.Plugin is not
//     RouteAware, so the entire chain skips on routed traffic. The
//     destination proxy fires its own commitgate, so we never
//     double-charge.
//
// Policy:
//
//   - StatementTypeUnspecified → always rejected. The rewriter failed
//     to classify the statement; refusing to forward is fail-safe.
//   - Read-bit-required: SELECT, SHOW TABLES, SHOW CREATE TABLE,
//     EXISTS TABLE, USE.
//   - Write-bit-required: INSERT, UPDATE, DELETE, TRUNCATE TABLE,
//     CREATE TABLE, DROP TABLE.
//   - Admin-bit-required: ALTER TABLE, RENAME TABLE, GRANT, REVOKE.
//   - Owner-bit-required: DROP DATABASE.
//
// Multi-target: every entry in ev.AccessedTables is checked
// independently with AND semantics — fail-fast on the first denied
// access. SELECT * FROM d1.t UNION SELECT * FROM d2.t requires Read
// on both d1 AND d2. CH parser limits DDL / GRANT / REVOKE to a
// single target, so iteration there always stops at one.
//
// Empty-AccessedTables semantics:
//
//   - SELECT and SHOW DATABASES legitimately surface no tables
//     (`SELECT 1`, the rewriter's metadata-shaped SHOW DATABASES
//     rewrite). The observer allows these — there is nothing to
//     check against. If any concrete DB shows up as a target the
//     normal per-table loop handles it.
//   - Other types with no AccessedTables = rewriter contract drift,
//     fail-closed.
//
// Bit semantics: Owner ⇒ Admin | Write | Read; Write ⇒ Read. Admin
// alone does NOT imply Write or Read — it only confers the ability to
// administer permissions (GRANT / REVOKE). The Owner bit is stored
// explicitly in DatabasePermissions (sentio-node's database event
// handlers set it on DatabaseCreated / DatabasePermissionChanged with
// the Owner bit). See promotePermissionBits below for the canonical
// implementation.
//
// SHOW DATABASES and CREATE DATABASE are intentionally NOT subscribed:
//
//   - SHOW DATABASES has no target database to scope a check against;
//     visibility filtering already happens at the rewriter layer via
//     database_map.
//   - CREATE DATABASE establishes ownership — there is nothing to
//     permission-check against the to-be-created DB. Conflict
//     detection (e.g. "DB already owned by another account") belongs
//     to a registrar observer such as InMemoryCommitGateObserver,
//     not here.
//
// The observer is purely read-only against NetworkState — no rollback
// state. Retries are trivially idempotent and OnStatementException is
// a no-op.
type PermissionCommitGateObserver struct {
	ns State
}

// NewPermissionCommitGateObserver wires the observer against any
// State implementation. Production deployments should pass the same
// RedisNetworkState used by the rewriter so that permission and
// database_map decisions stay consistent (both consult the same
// statemirror snapshot).
//
// Operator-on-behalf-of-owner DDL/DCL (sidecar with `--sidecar-owner`)
// is satisfied via State.IsOperator, which production wires off the
// statemirror's MappingOperators hash; sentio-node's permissions event
// handler keeps that hash in sync with the on-chain Permissions
// contract.
func NewPermissionCommitGateObserver(ns State) *PermissionCommitGateObserver {
	return &PermissionCommitGateObserver{ns: ns}
}

// permissionPolicy maps a StatementType to the registry.DbAuth bit
// required AFTER the standard promotion (Owner ⇒ Admin | Write | Read,
// Write ⇒ Read; Admin does NOT promote). Types absent from this table
// are NOT subscribed and pass through this observer unchecked.
// StatementTypeUnspecified is handled separately in BeforeStatement
// (always reject) and is not in this table.
var permissionPolicy = map[sqlmeta.StatementType]registry.DbAuth{
	sqlmeta.StatementTypeSelect:          registry.DbAuthRead,
	sqlmeta.StatementTypeShowTables:      registry.DbAuthRead,
	sqlmeta.StatementTypeShowCreateTable: registry.DbAuthRead,
	sqlmeta.StatementTypeExistsTable:     registry.DbAuthRead,
	sqlmeta.StatementTypeUse:             registry.DbAuthRead,

	sqlmeta.StatementTypeInsert:        registry.DbAuthWrite,
	sqlmeta.StatementTypeUpdate:        registry.DbAuthWrite,
	sqlmeta.StatementTypeDelete:        registry.DbAuthWrite,
	sqlmeta.StatementTypeTruncateTable: registry.DbAuthWrite,
	sqlmeta.StatementTypeCreateTable:   registry.DbAuthWrite,
	sqlmeta.StatementTypeDropTable:     registry.DbAuthWrite,

	sqlmeta.StatementTypeAlterTable:  registry.DbAuthAdmin,
	sqlmeta.StatementTypeRenameTable: registry.DbAuthAdmin,
	sqlmeta.StatementTypeGrant:       registry.DbAuthAdmin,
	sqlmeta.StatementTypeRevoke:      registry.DbAuthAdmin,

	sqlmeta.StatementTypeDropDatabase: registry.DbAuthOwner,
}

// SubscribedTypes returns the union of permissionPolicy plus
// StatementTypeUnspecified. Listed explicitly (rather than derived
// from the map at runtime) to keep the slice ordering stable across
// invocations — useful for tests and for the startup log line that
// commitgate emits for active subscriptions.
func (o *PermissionCommitGateObserver) SubscribedTypes() []sqlmeta.StatementType {
	return []sqlmeta.StatementType{
		sqlmeta.StatementTypeUnspecified,
		sqlmeta.StatementTypeSelect,
		sqlmeta.StatementTypeShowTables,
		sqlmeta.StatementTypeShowCreateTable,
		sqlmeta.StatementTypeExistsTable,
		sqlmeta.StatementTypeUse,
		sqlmeta.StatementTypeInsert,
		sqlmeta.StatementTypeUpdate,
		sqlmeta.StatementTypeDelete,
		sqlmeta.StatementTypeTruncateTable,
		sqlmeta.StatementTypeCreateTable,
		sqlmeta.StatementTypeDropTable,
		sqlmeta.StatementTypeAlterTable,
		sqlmeta.StatementTypeRenameTable,
		sqlmeta.StatementTypeGrant,
		sqlmeta.StatementTypeRevoke,
		sqlmeta.StatementTypeDropDatabase,
	}
}

// BeforeStatement is the gate. nil = allow; non-nil error aborts the
// query (relay synthesises an Exception to the client; ClickHouse is
// not contacted).
func (o *PermissionCommitGateObserver) BeforeStatement(ctx context.Context, ev *commitgate.Event) error {
	if ev.Type == sqlmeta.StatementTypeUnspecified {
		return fmt.Errorf("permission: rewriter did not classify statement (Unspecified); refusing to forward")
	}

	required, ok := permissionPolicy[ev.Type]
	if !ok {
		// Subscribed but no policy entry — programmer error
		// (SubscribedTypes drifted from permissionPolicy). Fail
		// closed so the divergence surfaces in CI / staging instead
		// of silently letting traffic through.
		return fmt.Errorf("permission: no policy for statement type %s", ev.Type)
	}

	if ev.User == "" {
		return fmt.Errorf("permission: %s requires an authenticated account", ev.Type)
	}

	// Effective principal resolution. When the sidecar acted as an
	// operator on behalf of an owner (SQL_x_payer setting), the JWS
	// signer (ev.User) is NOT the principal whose perms gate this
	// statement — the owner (ev.Owner) is. Validate the operator-of
	// relation against State.IsOperator (mirrored from the on-chain
	// Permissions contract via sentio-node's event handler) before
	// swapping in the owner; otherwise a malicious sidecar could
	// claim any arbitrary owner.
	account := AccountAddress(ev.User)
	if ev.Owner != "" {
		if !o.ns.IsOperator(AccountAddress(ev.Owner), AccountAddress(ev.User)) {
			return fmt.Errorf("permission: %s is not an authorized operator of %s", ev.User, ev.Owner)
		}
		account = AccountAddress(ev.Owner)
	}

	if len(ev.AccessedTables) == 0 {
		// Empty AccessedTables is legitimate for FROM-less SELECT
		// (`SELECT 1`, `SELECT version()`) and SHOW DATABASES — no
		// per-DB scope to gate. For every other subscribed type the
		// rewriter is expected to surface at least one entry; missing
		// entries indicate a contract drift, fail-closed.
		if allowsEmptyAccess(ev.Type) {
			return nil
		}
		return fmt.Errorf("permission: %s requires a target database (rewriter surfaced no AccessedTables)", ev.Type)
	}

	for _, a := range ev.AccessedTables {
		if a.LogicalDatabase == "" {
			return fmt.Errorf("permission: %s has unresolved logical database for table %q", ev.Type, a.OriginalTable)
		}
		if err := o.checkAccess(account, Database(a.LogicalDatabase), required, ev.Type); err != nil {
			return err
		}
	}
	return nil
}

// allowsEmptyAccess reports whether a subscribed StatementType may
// legitimately reach the observer with zero AccessedTables.
//
//   - SELECT: FROM-less compute (`SELECT 1`, `SELECT now()`).
//   - SHOW DATABASES: not subscribed today, but defensive — the
//     rewriter rewrites it into a SELECT over a database catalog,
//     not against any concrete DB.
//
// Adding a type here makes "no target" silently succeed; only do so
// when the SQL genuinely cannot have a database to gate against.
func allowsEmptyAccess(t sqlmeta.StatementType) bool {
	switch t {
	case sqlmeta.StatementTypeSelect, sqlmeta.StatementTypeShowDatabases:
		return true
	}
	return false
}

// systemDatabase is ClickHouse's built-in metadata schema. It is
// never registered as a tenant database in NetworkState, but tools
// like clickhouse-client read it on connect for command-line
// suggestions (a UNION over system.functions / system.tables /
// system.columns / ...). Per-row visibility is enforced server-side
// via viewIfPermitted, so allowing read-only access here is safe.
// Writes (INSERT / DDL targeting the system database) still fail-
// closed because they require a registered DB and CH itself rejects
// most of them anyway.
const systemDatabase Database = "system"

// PublicAccountAddress is the all-zeros Ethereum address acting as a
// public-grant pseudo-account. Permissions held by this address are
// OR'd into every authenticated account's effective permissions
// during checkAccess — i.e. granting Read on db `foo` to the public
// address makes `foo` readable by every user, in addition to whatever
// per-account permissions the bitmap holds.
//
// The address is intentionally the canonical zero address so it
// matches the args-validator's NormalizeEthereumAddress output for
// `0x000…000` and downstream registrar / permission lookups all key
// on the same canonical form.
const PublicAccountAddress AccountAddress = "0x0000000000000000000000000000000000000000"

// checkAccess runs a single (account, db) auth check for the
// `required` bit. Promotion (Owner ⇒ Admin | Write | Read; Write ⇒
// Read; Admin alone does not promote) is handled here via
// promotePermissionBits; callers pass the raw `required` bit from
// permissionPolicy. The Owner bit is stored explicitly in
// DatabasePermissions by sentio-node's database event handlers.
func (o *PermissionCommitGateObserver) checkAccess(account AccountAddress, db Database, required registry.DbAuth, stmtType sqlmeta.StatementType) error {
	if db == systemDatabase && required == registry.DbAuthRead {
		return nil
	}
	info, infoOk := o.ns.RetrieveDatabaseInfo(db)
	if !infoOk {
		return fmt.Errorf("permission: database %q is not registered with this proxy", db)
	}
	// PendingDelete is set by sentio-node when an on-chain pending-delete
	// event fires; the database is on its way out and the cleanup flow
	// runs outside the normal user-traffic path. Reject every subscribed
	// statement (read or write) so in-flight clients see a clear error
	// instead of racing with the deletion.
	if info.PendingDelete {
		return fmt.Errorf("permission: database %q is pending deletion", db)
	}
	auth := registry.DbAuth(0)
	// Lookup miss is treated as "no perms on file" — the final
	// &-test fails and the query is rejected with a clear
	// per-account message.
	if perms, permsOk := o.ns.RetrieveDatabasePermissions(account); permsOk {
		auth |= perms[db]
	}
	// PublicAccountAddress acts as a wildcard grantee: anything it
	// holds on `db` is unioned into the caller's effective bitmap.
	// Skip the lookup when the caller IS the public address to avoid
	// a redundant fetch.
	if account != PublicAccountAddress {
		if pubPerms, pubOk := o.ns.RetrieveDatabasePermissions(PublicAccountAddress); pubOk {
			auth |= pubPerms[db]
		}
	}
	auth = promotePermissionBits(auth)
	if auth&required == 0 {
		return fmt.Errorf("permission: account %s lacks %s on database %q for %s",
			account, prettyAuthBit(required), db, stmtType)
	}
	return nil
}

// OnStatementException is a no-op — the observer never mutates state.
func (o *PermissionCommitGateObserver) OnStatementException(_ context.Context, _ *commitgate.Event, _ *chproto.Exception) {
}

// promotePermissionBits expands hierarchical permissions: Owner ⇒ Admin|Write|Read,
// Write ⇒ Read. Admin alone does NOT imply Write or Read — it only grants
// the ability to manage permissions (GRANT/REVOKE).
func promotePermissionBits(auth registry.DbAuth) registry.DbAuth {
	if auth&registry.DbAuthOwner != 0 {
		auth |= registry.DbAuthAdmin | registry.DbAuthWrite | registry.DbAuthRead
	}
	if auth&registry.DbAuthWrite != 0 {
		auth |= registry.DbAuthRead
	}
	return auth
}

// prettyAuthBit produces a human-readable noun for a single DbAuth
// bit. Falls back to a hex form for unknown / composite bits so the
// error message stays parseable.
func prettyAuthBit(b registry.DbAuth) string {
	switch b {
	case registry.DbAuthRead:
		return "read"
	case registry.DbAuthWrite:
		return "write"
	case registry.DbAuthAdmin:
		return "admin"
	case registry.DbAuthOwner:
		return "owner"
	default:
		return fmt.Sprintf("auth=%x", int64(b))
	}
}

// Compile-time check the observer satisfies the commitgate contract.
var _ commitgate.Observer = (*PermissionCommitGateObserver)(nil)
