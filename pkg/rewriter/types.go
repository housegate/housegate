// Package rewriter owns the SQL rewriting contract and the
// Sentio-Network mode implementation.
//
// Two-tier shape:
//
//   - Factory is the long-lived process-wide object. It holds the rewrite
//     backend (gRPC client to the sql-rewriter service, or the in-process
//     rewriter-go engine), a NetworkState reference,
//     an Options snapshot, and any optional cluster manager / credential
//     provider used to render remote() table references.
//
//   - Rewriter is the per-connection stateful view. The proxy creates
//     one Rewriter per client connection (lazily, on the first OnQuery
//     when Identity has been established by the auth plugin) and uses
//     it for the lifetime of that connection. Rewrite() reads the
//     account address and current logical/physical database from the
//     Session each call so changes (USE statements, etc.) are picked
//     up automatically.
//
// All SQL flows through Rewriter.Rewrite — there is no longer a
// per-statement-kind entrypoint (RewriteDatabaseName / RewriteShowTables
// have been merged into the unified path; the gRPC service classifies
// the SQL via StatementType and applies SelectStmtArgs or GeneralArgs
// accordingly).
//
// Nothing in pkg/rewriter imports pkg/proxy or plugin packages; the
// dependency flows the other way.
//
// The plain Go types describing the *shape* of a classified statement
// (StatementType, AccessedTable) live in pkg/sqlmeta — a leaf package
// with zero proxy-specific deps so any plugin can consume them
// without dragging in pkg/network or this package.
package rewriter

import (
	"context"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/sqlmeta"
)

const StorageIntegrityContractV1 = pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1

// RewriteResult bundles everything the rewriter learned about one SQL
// statement. All fields are best-effort: when Rewrite short-circuits
// before calling the gRPC service — no mappings configured, no session
// context — the classification and discovery fields are zero-valued
// and SQL equals the input.
//
// Fields mirror the corresponding RewriteSQLResponse fields in
// protos/rewriter.proto. See that file for the authoritative shape
// (which key formats, which statement kinds populate which map, etc.).
type RewriteResult struct {
	// SQL is the post-rewrite SQL. Equals the input SQL when nothing
	// was changed (no mappings hit, UnsupportedStatement, or the
	// short-circuit path).
	SQL string

	// StatementType is the rewriter's classification of the INPUT SQL
	// (SELECT / CREATE_TABLE / CREATE_DATABASE / ...). Stays at
	// sqlmeta.StatementTypeUnspecified when classification didn't run
	// (parse error, short-circuit, closed rewriter); callers must
	// treat Unspecified as "unknown", not "definitely not a DDL".
	StatementType sqlmeta.StatementType

	// AccessedTables lists the tables the INPUT SQL referenced —
	// proto field `original_accessed_tables` (tag 12). Each entry
	// (sqlmeta.AccessedTable) carries the original database/table
	// the SQL contained plus the rewriter's best-effort resolution
	// of the logical and physical database under the active
	// TableNameRewrite mode. For SELECT/DML this includes tables
	// reached via CTEs. Tables that were rewritten (TableNameRewrite
	// hits) appear here under their original names; cross-reference
	// TableRewrites for the post-rewrite form. Nil when no gRPC
	// call happened (short-circuit path).
	AccessedTables []sqlmeta.AccessedTable

	// TableRewrites maps original "db.table" (or bare "table" when
	// no db was present in the SQL) to the post-rewrite "db.table"
	// form — proto field `table_rewrites`. Only entries whose
	// original differs from the rewritten form appear; untouched
	// tables are listed in AccessedTables instead. Nil when no
	// gRPC call happened.
	TableRewrites map[string]string

	// DatabaseRewrites maps original logical DB name to post-rewrite
	// physical DB name — proto field `database_rewrites`. Populated
	// only by USE / SHOW TABLES / SHOW DATABASES. Only entries whose
	// original differs from the rewritten form appear. Nil when no
	// gRPC call happened.
	DatabaseRewrites map[string]string

	// PrivilegesDeltas carries the structured GRANT/REVOKE output —
	// proto field `privileges_deltas` (tag 13). Populated only when
	// StatementType is StatementTypeGrant or StatementTypeRevoke;
	// empty/nil for every other statement kind. The SQL the rewriter
	// emits for these statements is `SELECT '<original>' AS gstmt`
	// (or `... AS rstmt`) — the deltas are the load-bearing output
	// that downstream auth services consume.
	PrivilegesDeltas []sqlmeta.PrivilegeDelta

	// ExistenceClause records the statement's existence-check clause —
	// proto field `existence_clause` (tag 14): IfNotExists for a CREATE
	// that carried IF NOT EXISTS, IfExists for a DROP / TRUNCATE that
	// carried IF EXISTS, Unspecified otherwise. The rewriter sets it as
	// soon as the SQL parses, so it is accurate even on a non-Success
	// (Unsupported) response; only a SyntaxError or the short-circuit
	// path leaves it Unspecified.
	ExistenceClause sqlmeta.ExistenceClause

	// StorageIntegrityContractVersion is the exact positive acknowledgement
	// echoed by the backend. It remains unspecified when no SI-aware backend
	// response was observed.
	StorageIntegrityContractVersion pb.StorageIntegrityContractVersion
}

// Rewriter rewrites Sentio-Network mode SQL into real SQL. It is bound
// to one client connection — the implementation reads account /
// session state on each call.
type Rewriter interface {
	// Rewrite transforms the input SQL. Ordinary implementation errors may
	// be handled fail-open by callers when no storage-integrity surface is
	// configured. A *RejectedError is different: it is a fail-closed SQL
	// outcome and callers MUST propagate it as a client Exception rather
	// than forwarding the original SQL.
	//
	// effectiveAccount is the principal whose database permissions gate
	// the per-query database_map sent to the rewriter service. Pass the
	// owner address when the JWS signer is acting as an operator
	// on-behalf-of-owner (qctx.Owner populated by the auth plugin) and
	// the bare signer otherwise. An empty string falls back to "no
	// account" — auth-off / anonymous semantics from buildDatabaseMap.
	//
	// On error, the returned RewriteResult is the zero value and must
	// not be inspected. On success, RewriteResult.SQL is always set
	// (equal to the input when nothing changed); the other fields are
	// best-effort — see the RewriteResult docs for when they're empty.
	Rewrite(ctx context.Context, sql, effectiveAccount string) (RewriteResult, error)

	// RewriteErrorMessage reverse-maps any rewritten table or database
	// names appearing in `message` so that exception text references
	// the names the client used. Uses the SQL of the most recent
	// successful Rewrite call as context, plus the effectiveAccount
	// captured during that call (so error reverse-mapping sees the same
	// database_map the originating Rewrite did).
	RewriteErrorMessage(ctx context.Context, message string) (rewroteMessage string, err error)

	// Close releases any per-connection resources. Safe to call
	// multiple times.
	Close() error
}

// Factory builds per-connection Rewriters using the shared resources
// it owns (gRPC client, NetworkState, options).
//
// One Factory is constructed at proxy startup and shared across every
// connection; tearing it down with Close shuts the gRPC connection and
// invalidates every Rewriter it produced.
type Factory interface {
	// NewRewriter returns a Rewriter wired to the given session. The
	// returned Rewriter reads sess on each Rewrite call (account from
	// Identity, logical/physical database from session state) so
	// callers do not have to recreate it as the session evolves.
	NewRewriter(sess Session) Rewriter

	// Close tears down the underlying rewrite backend (gRPC connection or
	// native engine). Per-connection Rewriters share that backend, so
	// Close on the factory ends rewriting for all of them; callers must
	// order shutdown accordingly.
	Close() error
}

// StorageIntegrityCapableFactory is a Factory that can enforce the v1
// storage-integrity request/response contract on every rewrite call.
type StorageIntegrityCapableFactory interface {
	Factory
	StorageIntegrityContractVersion() pb.StorageIntegrityContractVersion
}

// Session is the minimal per-connection state Rewriter reads on each
// call. *chsession.SessionState satisfies this interface implicitly —
// pkg/rewriter intentionally does NOT import pkg/chsession to keep
// the dependency direction clean (chsession ← rewriter would risk a
// cycle the moment chsession needed to call any rewriter method).
//
// The "Name" suffix on the database getters exists so the interface
// can be satisfied by chsession.SessionState's methods without
// colliding with its `LogicalDatabase` / `Database` struct fields,
// which Go syntax would otherwise force into renaming.
type Session interface {
	// Account returns the authenticated account address (typically
	// Identity.UserID set by the auth plugin). Empty string when the
	// connection is anonymous or auth has not run yet.
	Account() string

	// LogicalDatabaseName returns the current user-facing database
	// name (what the user typed in `USE <name>` or in
	// hello.Database). Empty if the user has not selected a database.
	LogicalDatabaseName() string

	// PhysicalDatabaseName returns the database name actually sent
	// to upstream ClickHouse. May equal LogicalDatabaseName when no
	// mapping is configured. Empty if no database has been bound.
	PhysicalDatabaseName() string

	// SetLogicalDatabase mirrors a `USE <name>` observation back into
	// the session. Called by the Rewriter itself when it classifies
	// the just-rewritten SQL as STATEMENT_TYPE_USE — that way the
	// next query's `upstream_logical_database_in_context` is correct
	// without the proxy needing a separate regex-based observer.
	SetLogicalDatabase(name string)
}

// Options configures Factory at construction time.
//
// The name is intentionally generic (not "Config") so it does not
// collide with the section type in pkg/config that controls operator
// behaviour — this struct is what the constructor wants and includes
// proxy bits (Listen, Upstream) needed for callback-address
// resolution.
type Options struct {
	Enabled     bool   // whether to enable rewriting at all
	ServiceAddr string // sql-rewriter gRPC address

	// Engine selects the rewrite backend: EngineGRPC ("" included) calls
	// the external sql-rewriter service at ServiceAddr; EngineNative runs
	// the in-process rewriter-go engine and ignores ServiceAddr.
	Engine string

	// NativeLibraryPath locates libpolyglot_sql_ffi.{so,dylib} for the
	// native engine. Empty falls back to the POLYGLOT_SQL_FFI_PATH env
	// var, then polyglot's standard install locations. Unused by grpc.
	NativeLibraryPath string

	Upstream     string        // upstream ClickHouse address (drives local/remote detection)
	CallbackAddr string        // address to render in remote() calls (defaults to Listen when empty)
	Listen       string        // proxy listen address (used for remote() callback)
	Timeout      time.Duration // gRPC timeout per call

	// PhysicalDatabase is the single physical ClickHouse database that
	// hosts every logical database known to this deployment. The
	// rewriter injects this as the value of every entry in
	// RewriteTableDynamicArgs.database_map, and as
	// upstream_physical_database_in_context when the session is
	// already bound to a physical DB.
	//
	// Empty disables database_map injection; non-SELECT statements
	// then have to be fully qualified by the client.
	PhysicalDatabase string

	// Delim is the separator used when the rewriter constructs
	// physical table names: `<physical>.<logical><delim><table>`.
	// Forwarded verbatim to the rewriter; empty means "let the
	// rewriter use its default" (currently "_").
	Delim string

	// AuthEnabled signals whether the proxy is running with
	// authentication on. It changes how anonymous connections (no
	// account on the session) are handled by the rewriter:
	//
	//   - AuthEnabled=false (auth disabled deployment) and account
	//     empty → treat as "see everything"; the rewriter's
	//     database_map is populated with every known logical
	//     database. This is the local-dev / auth-off path.
	//   - AuthEnabled=true and account empty → restrictive: empty
	//     database_map. Auth is on but the connection has not
	//     established an identity, so no logical database is
	//     accessible.
	//
	// Wired from cfg.Auth.Enabled in cmd.
	AuthEnabled bool

	StorageIntegrity StorageIntegrityOptions
}

// PeerSigner produces a peer-relay JWS authenticating this proxy to
// another housegate identified by audience. Defined here to keep
// pkg/rewriter free of a pkg/auth import; the production
// implementation (auth.RelaySigner) satisfies this interface.
//
// audience is the receiving proxy's indexer-id stringified (decimal).
// ttl bounds the token's validity window; the rewriter passes a short
// value (a few minutes) since each remote() call mints a fresh token.
type PeerSigner interface {
	Address() string
	SignPeerLogin(audience string, ttl time.Duration) (string, error)
}
