// Package plugin defines the contract between Server/Relay and pluggable
// behavior — credential injection, authentication, SQL rewriting, billing,
// auditing, and so on.
//
// The package owns only the interfaces and the chain that composes them.
// Concrete plugins live under pkg/plugins/<domain>/ and depend on this
// package for their hook surface.
package plugin

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/sqlmeta"
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

	// ExistenceClause is the rewriter's record of the existence-check
	// clause OriginalSQL carried — proto field `existence_clause`
	// (tag 14): IfNotExists for CREATE ... IF NOT EXISTS, IfExists for
	// DROP / TRUNCATE ... IF EXISTS, Unspecified otherwise. Set by the
	// rewrite plugin after the gRPC call; stays Unspecified when the
	// rewrite plugin is disabled or the call short-circuits before
	// classifying. Downstream observers (commitgate) read it to tell an
	// idempotent CREATE / DROP from one that should fail on a conflict.
	ExistenceClause sqlmeta.ExistenceClause

	// AbortWithSuccess, when set by a plugin (currently only
	// commitgate via ErrAbortWithSuccess), instructs the relay to
	// reply EndOfStream to the client and skip forwarding to upstream.
	// Plugins should set this sparingly — the only intended use is
	// host-managed DDL that has no ClickHouse counterpart.
	AbortWithSuccess bool

	// SuppressUpstreamExecution, when set by a plugin during OnQuery,
	// instructs the relay to keep Native INSERT sample-block negotiation with
	// ordinary upstream but withhold non-empty client Data payload packets from
	// that upstream. Relay still drains the query's client input through
	// StrictDataPlugin / QueryInputCompleteStrictPlugin; if that staged input
	// lifecycle completes successfully, the terminating empty Data block is
	// forwarded so the ordinary upstream finishes with zero rows. This is for
	// payload-bearing statements whose actual write is owned by an out-of-band
	// path after input capture, such as storage-integrity staged INSERT.
	SuppressUpstreamExecution bool

	// DeferredInsert, when set by a QueryPlugin during OnQuery, switches Relay
	// into deferred-INSERT mode for this query (spec 2026-08-18 signed
	// envelope v2 §5.2): Relay answers the INSERT sample block itself from
	// SampleColumns, buffers every client Data packet (bounded by
	// MaxPayloadBytes) while running the strict data hooks, runs the strict
	// input-complete hook, and only then forwards Query + buffered Data +
	// terminator upstream — so a plugin can sign the payload before the
	// upstream ever sees the Query. Mutually exclusive with
	// SuppressUpstreamExecution; Relay rejects a query that sets both.
	DeferredInsert *DeferredInsertPlan

	// SynthesizedInsert, when set by a QueryPlugin during OnQuery, switches
	// Relay into synthesized-INSERT mode (spec 2026-09-23 D8): the agent
	// evaluated the rows, so Relay encodes the plan's blocks into client Data
	// packets with the upstream codec, hashes them through the strict
	// input-complete hook, and only then writes Query + packets + terminator
	// upstream. Mutually exclusive with DeferredInsert,
	// SuppressUpstreamExecution, AgentPrepare, QueryOnly and AbortWithSuccess;
	// Relay rejects a query that sets more than one of them.
	SynthesizedInsert *SynthesizedInsertPlan

	// AgentPrepare is the query-only agent lane.  It deliberately has no
	// relationship to DeferredInsert: preparation happens off the client reader
	// and the resulting Query is forwarded only after Relay wins its generation
	// gate.  A plugin installs it to suspend the rest of the OnQuery chain.
	AgentPrepare *AgentPreparePlan

	// QueryOnly is installed only by an independently authenticated executing
	// host. Relay completes it locally and never writes its Query or client Data
	// to ordinary upstream. It is intentionally distinct from AgentPrepare:
	// the latter produces a signed Query for a host, while this plan owns the
	// host-side intake acknowledgement.
	QueryOnly *QueryOnlyPlan

	Values map[string]any

	continuation *queryContinuation
}

// SnapshotQueryAgentKey is a process-local handoff marker.  It is never read
// from a client setting; Relay sets it only after a live preparation result has
// been accepted.
const SnapshotQueryAgentKey = "snapshot_query_agent_owned"

// ValuesKeyMaterialized is the Values key under which the agent-mode
// materialize plugin records its outcome: "applied", "noop" or
// "error:<reason>". The ordinary path ignores it and keeps failing open; the
// signed inline VALUES lane is fail-closed (spec 2026-09-23 D2) and refuses a
// statement whose key is absent or starts with "error:".
const ValuesKeyMaterialized = "materialize.outcome"

// QueryOnlyPlan is the local host execution lane. Run must honor ctx and
// return only after the configured durable acknowledgement boundary. It must
// never write to a client codec; Relay remains the sole client reader and
// writer. CancelClient stops client delivery and performs the plan's durable
// cancellation/reconciliation rules without assuming source work rolled back.
//
// Only the package-private executing-host adapter constructs this type after
// it has selected a trusted source. Its fields are deliberately private, so an
// ordinary plugin, client setting, or decoded protocol value cannot fabricate
// host execution authority.
type QueryOnlyPlan struct {
	run             func(context.Context) error
	cancelClient    func()
	maxControlBytes uint64
	hostProvenance  *queryOnlyHostProvenance
}

type queryOnlyHostProvenance struct{}

func newHostQueryOnlyPlan(run func(context.Context) error, cancelClient func(), maxControlBytes uint64) *QueryOnlyPlan {
	return &QueryOnlyPlan{
		run:             run,
		cancelClient:    cancelClient,
		maxControlBytes: maxControlBytes,
		hostProvenance:  &queryOnlyHostProvenance{},
	}
}

// ValidHostPlan reports whether Relay may accept this plan. It intentionally
// checks private provenance in addition to exported callback fields.
func (p *QueryOnlyPlan) ValidHostPlan() bool {
	return p != nil && p.run != nil && p.cancelClient != nil && p.maxControlBytes != 0 && p.hostProvenance != nil
}

func (p *QueryOnlyPlan) Run(ctx context.Context) error { return p.run(ctx) }
func (p *QueryOnlyPlan) CancelClient()                 { p.cancelClient() }
func (p *QueryOnlyPlan) MaxControlBytes() uint64       { return p.maxControlBytes }

// PreparedAgentQuery is deliberately detached from the live connection.  In
// particular it contains no Session, QueryContext, or socket.  Relay is the
// only component allowed to copy Query into the live context and forward it.
type PreparedAgentQuery struct {
	Query   *chproto.Query
	Claimed bool
}

// AgentPreparePlan is installed by an agent QueryPlugin.  Prepare must not
// read the client socket or mutate qctx/session state.  ReconcileCancel is run
// by Relay after cancellation has won and owns durable cleanup independently
// of the client context.
type AgentPreparePlan struct {
	Prepare         func(context.Context) (PreparedAgentQuery, error)
	ReconcileCancel func(context.Context) error
	MaxControlBytes uint64
	// PersistForwardIntent records an intent which deliberately does not grant
	// permission to write. AuthorizeForward must durably record the exact
	// envelope/generation authorization before Relay writes any Query bytes.
	PersistForwardIntent func(context.Context, PreparedAgentQuery) error
	AuthorizeForward     func(context.Context, PreparedAgentQuery) error
	// PersistForwardUnknown records service-owned reconciliation when the
	// forward gate already won but client delivery was canceled before launch.
	PersistForwardUnknown func(context.Context, PreparedAgentQuery) error
}

// queryContinuation records the first plugin that has not run.  It is opaque
// outside this package and single-use so a side-effecting QueryPlugin cannot
// be invoked twice when preparation finishes late.
type queryContinuation struct {
	chain *PluginChain
	next  int
	once  sync.Once
	used  atomic.Bool
	err   error
}

func (q *QueryContext) installContinuation(c *PluginChain, next int) error {
	if q.continuation != nil {
		return fmt.Errorf("query continuation already installed")
	}
	q.continuation = &queryContinuation{chain: c, next: next}
	return nil
}

// DeferredInsertPlan tells Relay how to run the deferred-INSERT protocol.
type DeferredInsertPlan struct {
	// SampleColumns is the 0-row sample block Relay writes to the client in
	// place of the upstream's (name + exact ClickHouse type, table order).
	SampleColumns []chproto.SampleColumn
	// MaxPayloadBytes bounds the buffered on-wire payload; exceeding it
	// aborts the query with an Exception before any byte reaches upstream.
	MaxPayloadBytes uint64
}

// SynthesizedInsertPlan tells Relay how to run the synthesized-INSERT protocol
// for a statement whose rows the agent evaluated instead of the client
// streaming them. Blocks carry typed columns rather than bytes because the Data
// packet header depends on the upstream codec's negotiated revision; Packets
// and PayloadBytes are filled by the relay lane before
// OnQueryInputCompleteStrict, so the signed payload is exactly what goes on
// the wire. A SampleColumns mismatch against upstream is schema drift.
type SynthesizedInsertPlan struct {
	Blocks        [][]proto.InputColumn
	SampleColumns []chproto.SampleColumn
	Rows          uint64
	PayloadBytes  uint64
	Packets       [][]byte
}

// Payload returns the concatenation of Packets: the exact bytes the statement
// token hashes and the relay lane writes upstream.
func (p *SynthesizedInsertPlan) Payload() []byte {
	if p == nil || len(p.Packets) == 0 {
		return nil
	}
	out := make([]byte, 0, p.PayloadBytes)
	for _, packet := range p.Packets {
		out = append(out, packet...)
	}
	return out
}
