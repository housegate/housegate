// Package plugin defines the contract between Server/Relay and pluggable
// behavior — credential injection, authentication, SQL rewriting, billing,
// auditing, and so on.
//
// The package owns only the interfaces and the chain that composes them.
// Concrete plugins live under pkg/plugins/<domain>/ and depend on this
// package for their hook surface.
package plugin

import (
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/sqlmeta"
)

// QueryContext carries per-query state through the OnQuery chain.
//
// Plugins may mutate Query in place. Relay forwards qctx.Query to upstream
// after the chain completes, so any modification (Body rewrite, additional
// Settings, etc.) takes effect on the wire.
//
// Values is a per-query scratch map used for plugin-to-plugin handoffs
// (e.g. an auth plugin stashing decoded claims for a downstream audit
// plugin). It is allocated by Relay before invoking the chain.
type QueryContext struct {
	Session      chsession.Session
	OriginalSQL  string
	RewrittenSQL string
	Query        *chproto.Query

	// Owner is the on-behalf-of principal when the JWS signer is acting
	// as an *operator* — i.e. the proxy must gate the query against the
	// owner's permissions instead of the signer's. Sourced by the auth
	// plugin from the per-query SQL_x_payer setting after validating the
	// operator-of-owner relation via State.IsOperator. Empty when the
	// signer is acting on its own behalf or when no SQL_x_payer setting
	// was supplied.
	//
	// Plugins downstream of auth (rewrite, commitgate) MUST treat Owner
	// (when non-empty) as the effective principal for permission and
	// database-map decisions; Session.Account() / Identity.UserID stays
	// the JWS-authenticated signer for audit and log correlation.
	//
	// Lifetime is bounded by the query — never retained across calls.
	Owner string

	// RewriteArgs is the rewriter argument set used to produce RewrittenSQL.
	// It is kept as `any` so this package does not need to know about the
	// rewriter implementation. Plugins that need to read it (e.g. error
	// reverse-mapping) cast to the concrete type.
	RewriteArgs any

	// StatementType is the rewriter's classification of OriginalSQL
	// (SELECT / CREATE_TABLE / CREATE_DATABASE / ...). Set by the
	// rewrite plugin after the gRPC call returns. Stays at
	// sqlmeta.StatementTypeUnspecified when the rewrite plugin is
	// disabled, when the call short-circuits before classifying (no
	// mappings, no session context), or for routed sessions that
	// bypass the rewriter entirely.
	//
	// Plugins downstream of rewrite (usage, audit, ...) can branch on
	// this — but must treat Unspecified as "I don't know", not as
	// "definitely not a DDL".
	StatementType sqlmeta.StatementType

	// AccessedTables are the tables OriginalSQL referenced — proto
	// field `original_accessed_tables` (tag 12). Each entry
	// (sqlmeta.AccessedTable) carries the original database/table the
	// SQL contained plus the rewriter's best-effort resolution of its
	// logical/physical database. Includes tables reached via CTEs for
	// SELECT/DML; tables that were rewritten appear here under their
	// original names (cross-reference TableRewrites for the post-
	// rewrite form). Nil when the rewriter short-circuited and never
	// made the gRPC call.
	AccessedTables []sqlmeta.AccessedTable

	// TableRewrites maps original "db.table" (or bare "table") to the
	// post-rewrite "db.table" form — proto field `table_rewrites`.
	// Only entries whose original differs from the rewritten form
	// appear; untouched tables show up in AccessedTables instead.
	// Nil when no gRPC call happened.
	TableRewrites map[string]string

	// DatabaseRewrites maps original logical DB name to post-rewrite
	// physical DB name — proto field `database_rewrites`. Populated
	// only by USE / SHOW TABLES / SHOW DATABASES. Nil when no gRPC
	// call happened.
	DatabaseRewrites map[string]string

	// PrivilegesDeltas carries the structured GRANT/REVOKE output from
	// the rewriter — proto field `privileges_deltas` (tag 13). Set by
	// the rewrite plugin only when StatementType is StatementTypeGrant
	// or StatementTypeRevoke; nil for every other statement kind.
	// Downstream observers (commitgate) consume this to surface the
	// permission change for external auth-state reconciliation.
	PrivilegesDeltas []sqlmeta.PrivilegeDelta

	// AbortWithSuccess, when set by a plugin (currently only
	// commitgate via ErrAbortWithSuccess), instructs the relay to
	// reply EndOfStream to the client and skip forwarding to upstream.
	// Plugins should set this sparingly — the only intended use is
	// host-managed DDL that has no ClickHouse counterpart.
	AbortWithSuccess bool

	Values map[string]any
}
