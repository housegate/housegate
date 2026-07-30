// Package commitgate gates DDL statements (CREATE / DROP TABLE,
// CREATE / DROP DATABASE) on a host-supplied external commit.
//
// Library hosts of housegate.New register Observers via
// Options.CommitGateObservers. Each observer subscribes to a
// subset of StatementTypes and supplies a BeforeStatement
// implementation that runs synchronously before the matching
// query reaches ClickHouse. A non-nil error from any observer
// aborts the query — Relay returns a synthetic Exception to the
// client and ClickHouse is never contacted.
//
// The framework provides ordering ("hook before CH") and
// short-circuit ("error stops the chain"). It does NOT provide
// at-least-once delivery, deduplication, durable retry, or
// post-execute compensation. Hosts that need cross-system
// consistency must rely on hook idempotency + IF (NOT) EXISTS on
// the ClickHouse side; see the design spec for the full failure
// model.
package commitgate

import (
	"context"
	"errors"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// ErrAbortWithSuccess is a sentinel BeforeStatement may return to
// signal "I have handled this statement; do NOT forward it to
// ClickHouse, but reply success to the client (synthetic
// EndOfStream)." This matches the semantics needed by hosts that
// terminate certain DDL at the proxy edge — e.g. CREATE / DROP
// DATABASE that has been committed on-chain and has no physical
// ClickHouse counterpart.
//
// Observers MUST return this sentinel directly or wrap it with
// fmt.Errorf using %w so commitgate.Plugin can detect it via
// errors.Is. Returning a wrapped error that is NOT %w-wrapped
// degrades to abort-with-failure (synthetic Exception).
//
// Idempotency rules from Observer godoc still apply: a retry must
// produce the same outcome (commit + soft-success).
var ErrAbortWithSuccess = errors.New("commitgate: abort with synthetic success")

// Observer is the contract a host implements to gate DDL.
type Observer interface {
	// SubscribedTypes returns the StatementTypes this observer
	// wants to be invoked for. Empty / nil means "never fire".
	SubscribedTypes() []sqlmeta.StatementType

	// BeforeStatement runs synchronously before the Query is
	// forwarded to upstream. The return value selects the outcome:
	//   - return nil                  → forward to ClickHouse
	//   - return ErrAbortWithSuccess  → do NOT forward; reply EndOfStream to client
	//   - return any other error      → do NOT forward; reply Exception to client
	//
	// Implementations MUST be idempotent: the same Event may be
	// delivered more than once. The framework provides no
	// de-duplication.
	//
	// Implementations MUST honour ctx (the connection's context)
	// and MUST NOT block indefinitely — the call holds the
	// per-connection clientToUpstream goroutine.
	BeforeStatement(ctx context.Context, ev *Event) error

	// OnStatementException runs after ClickHouse returns an Exception
	// for a statement whose BeforeStatement previously returned nil.
	//
	// Best-effort: not guaranteed to fire (Relay's exception detection
	// is heuristic; multi-chunk exceptions or proxy crash will cause
	// the event to be dropped). Implementations MUST tolerate missed
	// dispatches and pair their state changes with idempotent
	// BeforeStatement logic so retries converge.
	//
	// No error return — by the time this fires, ClickHouse has already
	// surfaced an error to the client, and the observer's job is
	// observation/cleanup, not gating.
	//
	// ev is the same Event delivered to BeforeStatement. exc is the
	// decoded Exception from upstream — Code, Name, Message are
	// populated; Stack may or may not be (depends on CH version +
	// revision).
	OnStatementException(ctx context.Context, ev *Event, exc *chproto.Exception)
}

// SuccessObserver is an optional Observer extension for work that must happen
// only after ClickHouse completes a gated statement successfully.
//
// Dispatch is asynchronous after EndOfStream and best-effort: it may be lost
// on process crash. Implementations own idempotency and retries and must not
// mutate Event's slice/map fields.
type SuccessObserver interface {
	AfterStatementSuccess(ctx context.Context, ev Event)
}

// Event is the read-only payload delivered to BeforeStatement.
//
// Fields are valid only for the duration of the call; observers
// must not retain pointers.
type Event struct {
	// Type is the rewriter classification. Always one of the
	// values an observer subscribed to.
	Type sqlmeta.StatementType

	// User is the JWS-authenticated wallet/user identity, sourced
	// from SessionState.Identity.UserID (the same value the usage
	// and concurrency plugins use for billing and quota). This is
	// the durable cross-system identity hosts should use for their
	// external commit (e.g. on-chain ownership registration). Empty
	// string when the session is unauthenticated.
	User string

	// Owner is the on-behalf-of account when the JWS signer (User)
	// is acting as an *operator* — i.e. the upstream should treat
	// `Owner` (not `User`) as the principal that gets billed and
	// holds the on-chain Owner bit. Sourced from the SQL_x_payer
	// query setting that the agent plugin already injects when
	// configured with `--agent-owner=<O>`.
	//
	// buildEvent populates Owner only when SQL_x_payer is present
	// AND the address differs from User (after lowercasing). It
	// does NOT validate the operator-of relation — that's the
	// observer's job (see PermissionCommitGateObserver, which
	// calls State.IsOperator).
	//
	// Empty when no SQL_x_payer setting was supplied or when it
	// equals User (the legacy single-key agent path). Hosts that
	// implement billing or grant the Owner bit MUST treat Owner
	// (when non-empty) as the effective principal; User stays the
	// JWS-authenticated signer for audit / log correlation.
	Owner string

	// AccessedTables lists every (logical db, table) the statement
	// touches, populated from the rewriter's
	// `original_accessed_tables` (proto tag 12). The dispatcher does
	// NOT validate length or content — emptiness is a legal shape
	// for some statement kinds, and observers decide policy.
	//
	// Per-StatementType expectations:
	//
	//   - Table-scoped DDL (CREATE/DROP TABLE, ALTER TABLE,
	//     RENAME TABLE, TRUNCATE TABLE) and INSERT: exactly one entry;
	//     LogicalDatabase + OriginalTable populated.
	//   - Database-scoped DDL (CREATE/DROP DATABASE) and SHOW DATABASES:
	//     exactly one entry; LogicalDatabase populated, OriginalTable
	//     empty. (SHOW DATABASES is typically not subscribed.)
	//   - USE / SHOW TABLES: exactly one entry synthesised by the
	//     dispatcher from qctx.DatabaseRewrites (key = logical) or, as
	//     a fallback, from session.LogicalDatabaseName(). The rewriter
	//     does not surface AccessedTables for these — see
	//     pkg/rewriter/sentio.go:maybeUpdateLogicalDatabase.
	//   - SELECT / UPDATE / DELETE: zero or more entries (FROM-less
	//     SELECT like `SELECT 1` legitimately surfaces zero); observers
	//     MUST iterate, not assume [0] is exhaustive.
	//   - GRANT / REVOKE: exactly one entry mirroring the first delta's
	//     target (CH parser limits a statement to one target). Per-delta
	//     scope lives in PrivilegesDeltas.
	//
	// Observers MUST NOT mutate the slice or its entries. Lifetime is
	// bounded by the Query — do not retain.
	AccessedTables []sqlmeta.AccessedTable

	// QueryID is the upstream-bound ClickHouse query id, useful
	// for log correlation.
	QueryID string

	// OriginalSQL is the SQL the client sent (pre-rewrite).
	OriginalSQL string

	// RewrittenSQL is what would be sent to upstream if the
	// hook returns nil. For logging only.
	RewrittenSQL string

	// Values is per-observer scratch space. BeforeStatement may
	// populate it (e.g. with a rollback closure or a snapshot of
	// pre-mutation state) for OnStatementException to consume.
	// Observers must namespace their keys (e.g. "<package>/<purpose>")
	// to avoid collisions with other observers in the same chain.
	//
	// The framework allocates this map on Event creation; observers
	// should not reassign it (mutate in place). Lifetime is bounded
	// by the Query: cleared when the chain dispatches OnQueryComplete.
	Values map[string]any

	// Settings is the read-only Settings map from the Query packet.
	// Useful for hosts that need to inspect or pass through ClickHouse
	// settings the user supplied (e.g. JWS auth tokens encoded as
	// SQL_x_auth_token, or trust-domain markers like
	// SQL_sentio_routed).
	//
	// Observers MUST NOT mutate the map. Lifetime is bounded by the
	// Query — do not retain.
	Settings map[string]string

	// PrivilegesDeltas is the structured GRANT/REVOKE output, populated
	// only when Type is StatementTypeGrant or StatementTypeRevoke. One
	// entry per AccessRightsElement (i.e. typically one per privilege —
	// `GRANT SELECT, INSERT ON db.t TO u` produces two deltas with the
	// same target and grantees). All deltas in a single statement share
	// the same target — the dispatcher mirrors that target onto
	// AccessedTables[0] for symmetry with the DDL types. Empty / nil
	// for every other statement kind.
	//
	// Observers MUST NOT mutate the slice or its entries. Lifetime is
	// bounded by the Query.
	PrivilegesDeltas []sqlmeta.PrivilegeDelta

	// PrivilegesCategory is the OR of every PrivilegesDeltas entry's
	// Category — a READ/WRITE/ADMIN bitmap aligned with registry.DbAuth.
	// Convenience for observers that only need the coarse "what kind of
	// permission change is this?" answer without iterating the deltas
	// (e.g. routing a GRANT SELECT to a read-policy reconciler vs. a
	// GRANT ALL to the admin path). Zero (PrivilegeCategoryNone) when
	// the statement type is not GRANT/REVOKE or when no delta privilege
	// matched a known category.
	PrivilegesCategory sqlmeta.PrivilegeCategory

	// ExistenceClause records the existence-check clause the statement
	// carried, populated from the rewriter's `existence_clause` (proto
	// tag 14): ExistenceClauseIfNotExists for a CREATE-family statement
	// with IF NOT EXISTS, ExistenceClauseIfExists for a DROP / TRUNCATE
	// with IF EXISTS, ExistenceClauseUnspecified otherwise.
	//
	// Observers use it to decide conflict policy: a CREATE DATABASE
	// IF NOT EXISTS that names an already-registered database should
	// converge to soft-success (idempotent re-run) rather than reject,
	// and a DROP ... IF EXISTS on an absent target likewise. It is a
	// pure syntactic property of the parsed SQL — reported regardless
	// of whether the statement is later accepted or rejected.
	ExistenceClause sqlmeta.ExistenceClause
}
