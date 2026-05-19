# `commitgate` — Design Spec

**Date:** 2026-04-27
**Status:** Draft / for review
**Authors:** poetry, Claude

## 1. Goal

Let library hosts of `housegate.New(...)` register **synchronous, vetoable
hooks** that fire on `CREATE TABLE` / `DROP TABLE` / `CREATE DATABASE` /
`DROP DATABASE` **before** the statement reaches the upstream ClickHouse, so
the host can perform an external commit (e.g. an on-chain transaction
registering the table and its owner) and abort the DDL if that commit fails.

The package is named `commitgate` because that is its single responsibility:
**gate the DDL on a host-supplied external commit**. The hook is not just an
observer — it can refuse the statement, in which case ClickHouse is never
touched.

**Concrete motivating use case.** The host integrates with a blockchain
network state. Every `CREATE TABLE` must, before being executed in
ClickHouse, register `(user, database, table)` ownership on-chain. If the
on-chain transaction fails, ClickHouse must never be touched. The same
applies (inversely) to `DROP`.

## 2. Non-goals

- **No saga / two-phase commit / rollback machinery.** The hook fires
  synchronously *before* upstream dispatch; it cannot observe the
  post-upstream outcome in v1. Cross-system consistency is achieved by
  **idempotency in the hook implementation** combined with `IF NOT EXISTS` /
  `IF EXISTS` semantics on both sides — not by framework-managed
  compensation. See §7.
- **A best-effort post-execute hook (`OnStatementException`) is wired** so
  observers can react to ClickHouse rejecting a statement they previously
  gated. Delivery is best-effort (Relay's exception detection is the
  first-byte heuristic; multi-chunk exceptions or a proxy crash drop the
  event). The hook is observation-only — it returns nothing and runs only
  on the failure path. Observers must still pair their state mutations with
  idempotent `BeforeStatement` logic for retries to converge; the hook is
  an opportunistic cleanup channel, not a saga primitive.
- **`RENAME TABLE` is out of scope.** Rename is hard to make idempotent on
  the external system; we explicitly do not gate it.
- **`ALTER TABLE` is out of scope.** Schema-level changes do not change
  ownership in the motivating model.
- **No `commitgate` support in agent / forwarding-only modes.** Both
  modes forward to a server-mode proxy that already runs the chain — gating
  in two places would risk double-fire. Server-mode is the single point of
  enforcement.
- **No new dynamic string-keyed registry** (e.g.
  `proxy.Register("create_table_hook", fn)`). The codebase's plugin pattern
  is typed interfaces + `Options`; commit-gate observers fit that pattern.

## 3. Background

The rewriter has classified every query into a `rewriter.StatementType`
since [#13](https://github.com/sentioxyz/housegate/pull/13), and the rewrite
plugin already deposits that classification onto
`plugin.QueryContext.StatementType` for downstream plugins to read. So the
information needed to dispatch is already present in the chain at the
natural position — right after `rewrite.Plugin` runs in
[build.go:363-369](../../../build.go).

The rewriter has also recently been updated to expose
`original_accessed_tables` as a structured `repeated AccessedTable` (proto
tag 12, replacing the old `[]string` at tag 4). Each entry already carries
the rewriter's resolution of the canonical logical database, so the
commitgate plugin needs no extra lookup logic — see §4.6.

Earlier discussion considered firing on `OnQueryComplete` (post-success
observation), then a saga (pre-execute + rollback). Both were rejected:

- **Post-success only** does not satisfy "must register on-chain before CH
  executes" — by the time CH says EOS, the wrong thing might already exist.
- **Saga / rollback** is not reliable across blockchain + CH and the
  rollback itself can fail; we'd need a durable retry queue and reconciler
  to make it honest.

The accepted approach is **synchronous pre-execute + idempotency**: the hook
commits on-chain first; if that fails, the client gets an error and CH is
not contacted. If the hook succeeds and CH then fails, the next client
retry replays the same idempotent on-chain commit (no-op) and re-runs the
CH DDL with `IF NOT EXISTS` (succeeds). The system converges without
explicit compensation.

## 4. Design

### 4.1 Lifecycle position

The hook fires from a **new `QueryPlugin`** inserted into the server-mode
chain immediately after `rewrite.Plugin` and before `routeplugin.Signer`
(so we abort *before* JWS signing if the gate vetoes).

Concrete chain order in `buildServer` (changes vs. today in **bold**):

```
QueryPlugins:
  authplugin            (JWS validate)
  usage                 (billing usage check)
  concurrency           (optional, per cfg)
  sessionstate          (USE / SET tracker)             [HelloPlugin only — already there, renamed from `state`]
  rewrite               (gRPC rewriter — sets StatementType + AccessedTables)
  **commitgate**        (NEW — dispatches BeforeStatement to observers)
  routeplugin.Signer    (JWS sign for upstream)
  metrics               (semantic events)
```

The plugin sees `qctx.StatementType`, dispatches if it matches a configured
subset, and either returns `nil` (chain proceeds → upstream dispatch) or
returns an error (chain short-circuits → Relay synthesises a ClickHouse
Exception toward the client; upstream is never touched).

### 4.2 Observer interface

A new package `pkg/plugins/commitgate/` owns the public surface.

```go
// Package commitgate gates DDL statements on a host-supplied external
// commit. An Observer's BeforeStatement runs synchronously before the
// statement reaches ClickHouse; returning an error aborts the statement.
package commitgate

import (
    "context"

    "housegate/housegate/pkg/rewriter"
)

// Observer is the contract a host implements to gate DDL.
//
// BeforeStatement runs synchronously for every Query whose
// classified StatementType matches one of the observer's
// SubscribedTypes(). Returning a non-nil error aborts the Query
// before it reaches upstream — Relay forwards the error to the
// client as a synthetic ClickHouse Exception.
//
// Implementations MUST be idempotent: the same Event may be
// delivered more than once (client retry after CH failure, proxy
// crash + restart with no in-flight state, double connection on
// the host side). The framework provides no de-duplication.
//
// Implementations MUST honour ctx — the connection's lifetime
// bounds the call. If the client disconnects mid-hook, ctx is
// cancelled.
//
// Implementations MUST NOT block indefinitely — the call holds
// the per-connection clientToUpstream goroutine. Use ctx
// deadlines / your own timeouts.
type Observer interface {
    // SubscribedTypes returns the StatementTypes this observer
    // wants to be invoked for. Returning nil / empty means
    // "never fire" (effectively disables the observer).
    SubscribedTypes() []rewriter.StatementType

    // BeforeStatement is invoked before the Query is forwarded
    // to upstream. A non-nil return aborts the Query.
    BeforeStatement(ctx context.Context, ev *Event) error

    // OnStatementException runs after ClickHouse returns an
    // Exception for a statement whose BeforeStatement previously
    // returned nil.
    //
    // Best-effort: not guaranteed to fire (Relay's exception
    // detection is heuristic; multi-chunk exceptions or a proxy
    // crash will cause the event to be dropped). Implementations
    // MUST tolerate missed dispatches and pair their state changes
    // with idempotent BeforeStatement logic so retries converge.
    //
    // No error return — by the time this fires, ClickHouse has
    // already surfaced an error to the client; the observer's job
    // is observation/cleanup, not gating.
    //
    // ev is the same Event delivered to BeforeStatement. exc is
    // the decoded Exception from upstream (Code, Name, Message
    // populated; Stack and Nested may or may not be).
    OnStatementException(ctx context.Context, ev *Event, exc *chproto.Exception)
}
```

### 4.3 The `Event` payload

```go
// Event is the read-only payload delivered to BeforeStatement.
//
// Fields are populated from plugin.QueryContext at dispatch time;
// observers must not mutate any field, and fields are valid only
// for the duration of the BeforeStatement call (do not retain
// pointers).
type Event struct {
    // Type is the rewriter classification. Always one of the
    // values an observer subscribed to.
    Type rewriter.StatementType

    // User is the authenticated client user (post route strip,
    // post credential substitution — i.e. the *client-facing*
    // user, not the upstream-facing one).
    User string

    // AccessedTables lists every (logical db, table) the statement
    // touches. The dispatcher passes qctx.AccessedTables through
    // verbatim and synthesises a single entry from
    // qctx.DatabaseRewrites for USE / SHOW TABLES (which the rewriter
    // surfaces in database_rewrites instead of accessed_tables) and
    // from the first PrivilegeDelta for GRANT / REVOKE.
    //
    // Per-StatementType expectations — observers MUST iterate, not
    // assume [0] is exhaustive:
    //   - SELECT / UPDATE / DELETE: zero or more entries (FROM-less
    //     SELECT like `SELECT 1` legitimately surfaces zero).
    //   - INSERT / table-DDL / database-DDL / USE / SHOW TABLES /
    //     GRANT / REVOKE: exactly one entry (CH parser limits these
    //     to a single target).
    //
    // The dispatcher does NOT validate length or content — emptiness
    // is a legal shape for some statement kinds. Observers decide
    // policy: PermissionCommitGateObserver allows empty-AccessedTables
    // for SELECT / SHOW DATABASES (no per-DB scope to gate) and
    // fails-closed for everything else; InMemoryCommitGateObserver
    // requires AccessedTables[0] for the DDL types it owns.
    AccessedTables []sqlmeta.AccessedTable

    // QueryID is the upstream-bound ClickHouse query id, useful
    // for log correlation.
    QueryID string

    // OriginalSQL is the SQL the client sent (pre-rewrite).
    OriginalSQL string

    // RewrittenSQL is what would be sent to upstream if the
    // hook returns nil. Provided for logging only — observers
    // should not branch on its content.
    RewrittenSQL string
}
```

### 4.4 Registration

Two layers, both reusing the existing `Options` pattern at the root
package.

**Static (compile-time, primary form):**

```go
// in housegate.Options (proxy.go)
type Options struct {
    // ... existing fields

    // CommitGateObservers gate DDL statements. They fire in slice
    // order; the first non-nil error short-circuits the chain.
    // Empty slice = no commitgate plugin is wired.
    CommitGateObservers []commitgate.Observer
}
```

**Dynamic (post-construction, optional convenience):**

Out of scope for v1. The static form covers the motivating use case. If we
later need late binding, we can expose:

```go
// On the Proxy interface, optional:
//   Register(types []rewriter.StatementType, fn HookFunc) (cancel func())
// returning a cancel closure. Defer until a real demand surfaces; adding
// it later is non-breaking.
```

This keeps v1 tight and avoids two coexisting registration mechanisms while
the design is bedding in.

### 4.5 The `commitgate.Plugin`

```go
package commitgate

import (
    "context"
    "fmt"

    "housegate/housegate/pkg/plugin"
    "housegate/housegate/pkg/rewriter"
)

// Plugin is the QueryPlugin that dispatches BeforeStatement to
// registered observers. It is NOT RouteAware: routed
// (proxy-to-proxy) sessions skip it because the destination proxy
// fires its own commitgate plugin and we must not double-fire.
type Plugin struct {
    // observers indexed by StatementType for O(1) dispatch
    byType map[rewriter.StatementType][]Observer
}

func NewPlugin(observers []Observer) *Plugin {
    p := &Plugin{byType: make(map[rewriter.StatementType][]Observer)}
    for _, o := range observers {
        for _, t := range o.SubscribedTypes() {
            p.byType[t] = append(p.byType[t], o)
        }
    }
    return p
}

// OnQuery dispatches if StatementType matches a subscribed type.
// Statements whose type has no subscribed observer are a no-op.
//
// The dispatcher does NOT validate Event content. Policy lives in
// the observers — see PermissionCommitGateObserver and
// InMemoryCommitGateObserver. This is deliberate: legitimate empty
// shapes exist (e.g. `SELECT 1` has no AccessedTables) and only the
// observer knows whether its own policy is satisfied.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
    obs, ok := p.byType[qctx.StatementType]
    if !ok || len(obs) == 0 {
        return nil
    }
    ev := buildEvent(qctx) // see §4.6 — never returns error
    for _, o := range obs {
        if err := o.BeforeStatement(ctx, ev); err != nil {
            return fmt.Errorf("commitgate (%s): %w", qctx.StatementType, err)
        }
    }
    return nil
}
```

The plugin is wired into `buildServer` immediately after `rewritePlug` and
before `routeplugin.Signer` (see §5).

### 4.6 Building the `Event` from `QueryContext`

The rewriter response carries `repeated AccessedTable original_accessed_tables`
(proto tag 12). Each entry is:

```protobuf
message AccessedTable {
    string original_database  = 1; // what the SQL contained (empty if unqualified)
    string original_table     = 2;
    string logical_database   = 3; // rewriter-resolved canonical logical DB
    string physical_database  = 4; // ClickHouse-side physical DB
    bool   is_remote          = 5;
}
```

The Go-side `rewriter.RewriteResult.AccessedTables` (currently `[]string`)
must be migrated to `[]AccessedTable` before this design lands — see §10.
Once migrated, `plugin.QueryContext.AccessedTables` is
`[]rewriter.AccessedTable`.

**Why this simplifies the v1 design.** `LogicalDatabase` is the
rewriter's resolution of "what logical namespace does this statement
target?" — drawn from either the SQL qualifier (`db.table`) or the
session's logical-database context (`upstream_logical_database_in_context`,
which the `sessionstate` plugin establishes from `ClientHello`). The
commitgate plugin therefore does **not** need its own session-state
fallback: the rewriter already did it.

**`buildEvent(qctx)` is a pure projection — never returns an error.**
Policy (empty / malformed / contract-drift handling) is the
observer's job; the dispatcher only structures the data.

Per-StatementType source mapping:

| Type(s) | Event.AccessedTables source |
|---|---|
| SELECT / DML / table-DDL / database-DDL | `qctx.AccessedTables` verbatim (zero or more entries). |
| USE / SHOW TABLES | Synthesised single entry from `qctx.DatabaseRewrites` (key = logical, value = physical). Falls back to `session.LogicalDatabaseName()` for the known-physical USE case where `database_rewrites` is empty (the rewrite plugin's `maybeUpdateLogicalDatabase` populates the session). The rewriter does not surface `original_accessed_tables` for these — see [pkg/rewriter/sentio.go](pkg/rewriter/sentio.go) `maybeUpdateLogicalDatabase`. |
| GRANT / REVOKE | `qctx.PrivilegesDeltas` flows through verbatim; the first delta's target (LogicalDatabase / OriginalTable / PhysicalDatabase) is mirrored onto `AccessedTables[0]` for symmetry with the DDL path so observers can iterate uniformly. CH parser limits a GRANT/REVOKE statement to one target. `Event.PrivilegesCategory` is the OR of every delta's `Category`. |

```go
ev := &Event{
    Type:             qctx.StatementType,
    User:             user,
    AccessedTables:   qctx.AccessedTables,
    PrivilegesDeltas: qctx.PrivilegesDeltas,
    QueryID:          queryID,
    OriginalSQL:      qctx.OriginalSQL,
    RewrittenSQL:     qctx.RewrittenSQL,
    Settings:         settings,
}
switch qctx.StatementType {
case sqlmeta.StatementTypeGrant, sqlmeta.StatementTypeRevoke:
    if len(qctx.PrivilegesDeltas) > 0 {
        d := qctx.PrivilegesDeltas[0]
        ev.AccessedTables = []sqlmeta.AccessedTable{{
            OriginalDatabase: d.OriginalDatabase,
            OriginalTable:    d.OriginalTable,
            LogicalDatabase:  d.LogicalDatabase,
            PhysicalDatabase: d.PhysicalDatabase,
        }}
    }
    for _, d := range qctx.PrivilegesDeltas {
        ev.PrivilegesCategory |= d.Category
    }
case sqlmeta.StatementTypeUse, sqlmeta.StatementTypeShowTables:
    if len(ev.AccessedTables) == 0 {
        if a := synthDatabaseScopedAccess(qctx); a != nil {
            ev.AccessedTables = []sqlmeta.AccessedTable{*a}
        }
    }
}
return ev
```

**Why no validation in the dispatcher.** Earlier iterations of this
spec required `buildEvent` to reject empty AccessedTables /
unresolved LogicalDatabase / etc. with sentinel errors. Two cases
broke that:

1. **`SELECT 1` and friends** legitimately surface zero
   AccessedTables — there is nothing to gate against. Pre-rejecting
   them produced a misleading "rewriter did not surface accessed
   tables" error and broke driver auto-probes.
2. **USE / SHOW TABLES** never had AccessedTables to begin with —
   their target lives in `database_rewrites`. The old contract
   silently broke USE in production until the synthesis path was
   added.

Pushing the validation to observers means the same dispatcher works
for any future StatementType subscription without cargo-culting
"required entry" rules into the framework. Each observer owns the
allow/deny decision for its own policy domain (per-DB permissions,
in-memory state mutation, audit logging, etc.).

### 4.7 Idempotency contract

This is a **contract on the observer**, surfaced in the `Observer` doc
comment. It is enforced by the host's implementation, not by the framework.
Specifically:

- `BeforeStatement` may be invoked **more than once** for the same logical
  intent (client retry, host-side connection pool double-fire, proxy
  restart).
- The implementation must use a stable on-chain operation that is a no-op
  on duplicate input — typically: keying the chain transaction by
  `hash(user, database, table, statement_type)` and rejecting (server-side
  or by checking on-chain state first) if already present, returning
  success.
- On the ClickHouse side, the host should pair this with `IF NOT EXISTS` /
  `IF EXISTS` so a CH retry after a chain success + CH transient failure
  also no-ops.

The framework's contribution is **only**: "don't deliver the event before
the chain commit; if you fail, don't proceed to CH". Everything beyond is
the host's responsibility, **explicitly so**.

### 4.8 Error semantics

- `BeforeStatement` returning a non-nil `error` short-circuits the
  `QueryPlugin` chain (existing behaviour — `PluginChain.OnQuery` returns
  the first error, Relay synthesises a ClickHouse `Exception` packet
  toward the client).
- The error message reaches the client wrapped as
  `"commitgate (<type>): <msg>"`. Hosts should write user-actionable
  messages (e.g. `"on-chain registration failed: insufficient gas"`); they
  must NOT include sensitive material (private keys, internal addresses).
- The `OnQueryComplete` chain still fires (as it does for any rejected
  query) — concurrency permits / counters are released cleanly.

### 4.9 Cancellation & timeout

- The `ctx` passed to `BeforeStatement` is the `clientToUpstream`
  goroutine's context, derived from the connection's context. It is
  cancelled when the client disconnects.
- The plugin **does not** apply a default timeout. Hosts must apply their
  own (e.g. `context.WithTimeout(ctx, 30*time.Second)` inside the hook),
  because:
  - Block-time latencies vary widely across networks (1s to 60s).
  - A wrong default is worse than no default — operators surprised by
    timeouts on legitimate slow chain confirmations.
- Connection-level read/write deadlines in `Server` already protect
  against a hook hanging forever — the connection eventually closes and
  ctx is cancelled.

### 4.10 Mode & route applicability

- **Server mode only.** `buildAgent` and `buildForwarding` do NOT wire
  the plugin. Both forward upstream to a server-mode proxy that fires its
  own gate — single point of enforcement.
- **Not `RouteAware`.** Routed (proxy-to-proxy via `__route__<target>`)
  sessions skip the plugin. The destination proxy fires it.

## 5. `buildServer` integration

In [build.go](../../../build.go), after the existing rewrite-plugin block:

```go
if rewritePlug != nil {
    queryPlugins = append(queryPlugins, rewritePlug)
}

// NEW
if len(opts.CommitGateObservers) > 0 {
    cgPlug := commitgate.NewPlugin(opts.CommitGateObservers)
    queryPlugins = append(queryPlugins, cgPlug)
    log.Infow("commitgate enabled",
        "observers", len(opts.CommitGateObservers),
        "subscribed_types", cgPlug.SubscribedTypes(),
    )
}

queryPlugins = append(queryPlugins,
    &routeplugin.Signer{Signer: relaySigner, Observer: obs},
    metrics,
)
```

(`cgPlug.SubscribedTypes()` is a small helper for log visibility — returns
the union of all observers' subscribed types.)

No other wiring changes. `buildAgent` / `buildForwarding` are not
touched.

## 6. Files

```
pkg/plugins/commitgate/
├── BUILD.bazel
├── observer.go      // Observer interface, Event struct
├── plugin.go        // Plugin, NewPlugin, OnQuery, buildEvent
└── plugin_test.go   // unit tests (see §8)

pkg/plugins/sessionstate/   // RENAME of pkg/plugins/state/
├── BUILD.bazel
├── config.go
├── tracker.go
└── tracker_test.go

build.go             // wire opts.CommitGateObservers; update sessionstate import
proxy.go             // add Options.CommitGateObservers field
```

The `state` → `sessionstate` rename is a separate, mechanical change. The
package's behaviour does not change. Importers: `build.go`, the package's
own files, possibly `BUILD.bazel`.

## 7. Failure modes & non-guarantees

Documented explicitly so hosts integrate eyes-open.

| Scenario | Outcome | Mitigation (host responsibility) |
|---|---|---|
| Hook fails (network / chain rejection) | Client gets Exception; CH untouched. | Surface a useful error message. |
| Hook succeeds; CH fails (e.g. disk full, syntax). | Client gets CH Exception. `OnStatementException` fires (best-effort) with the same Event + decoded Exception so observers can run their own compensation (e.g. revert in-memory state, enqueue an off-line cleanup task). On-chain state still shows registered owner with no CH table when the hook can't be delivered. | Idempotent hook + `CREATE TABLE IF NOT EXISTS` → next client retry replays the chain (no-op) and CH (succeeds). Observers that supply rollback in `OnStatementException` must still tolerate dropped events; the hook is opportunistic, not authoritative. |
| Hook succeeds; proxy crashes before CH dispatch. | Same as above. | Same. |
| Client disconnects mid-hook. | `ctx` cancels; hook returns; chain returns error; nothing forwarded. | Hook implementation must respect `ctx.Done()`. |
| Concurrent retries from the same client (two CREATE TABLE in-flight). | Both hit the hook in parallel. | Host's chain-side de-dup decides: one wins, the other returns "already registered" (which the host can map to success or surface as conflict — host's call). |

The framework explicitly does **not** provide:

- At-least-once or at-most-once delivery guarantees.
- A durable outbox.
- Compensation / rollback on CH failure.

If a host needs these, the right architectural place is an external
reconciler that reads chain state + CH state and converges — not the
proxy.

## 8. Testing strategy

Unit tests in `pkg/plugins/commitgate/plugin_test.go`:

1. **Dispatch by StatementType.** A fake observer subscribes to
   `CreateTable`. Drive `OnQuery` with `QueryContext` populated for each
   StatementType in turn; assert the observer fires exactly once for
   `CreateTable` and never for the others.
2. **Multiple observers, slice order.** Two observers subscribed to the
   same type — assert both fire in registration order; first error
   short-circuits.
3. **Veto.** Observer returns an error — assert `OnQuery` returns an
   error wrapped with the statement type and observer message.
4. **AccessedTables pass-through.** Cases for each of `CREATE TABLE`,
   `DROP TABLE`, `CREATE DATABASE`, `DROP DATABASE` — both qualified
   (`db.tbl`) and unqualified (relying on session current database,
   which the rewriter projects into `LogicalDatabase`). Assert
   `Event.AccessedTables[0]` is the rewriter's exact entry.
5. **Multi-target SELECT (UNION).** Two `AccessedTable` entries for
   different logical DBs; assert the dispatcher does NOT collapse to
   one and the observer iterates both.
6. **USE / SHOW TABLES synthesis.** With `qctx.AccessedTables` empty
   and `qctx.DatabaseRewrites = {logical → physical}`, assert
   `Event.AccessedTables[0]` mirrors the rewrite. Plus the
   known-physical-USE fallback that recovers from
   `session.LogicalDatabaseName()`.
7. **Empty AccessedTables / empty PrivilegesDeltas dispatches.** The
   dispatcher does NOT pre-reject the empty shape — the observer
   sees it and decides. Mirrors the FROM-less SELECT path that
   `PermissionCommitGateObserver` allows.
8. **`StatementTypeUnspecified` dispatches to a subscribed observer.**
   `PermissionCommitGateObserver`'s "always reject" path uses this;
   guard that the framework hands the empty Event off rather than
   pre-empting with a misleading error.
9. **GRANT / REVOKE first-delta mirroring.** Assert
   `Event.AccessedTables[0]` reflects the first PrivilegeDelta's
   target so observers can iterate AccessedTables uniformly.
10. **`ctx` cancellation propagates** to the observer call.

Integration test (in `pkg/proxy/relay_test.go`-style framework, against
the existing mock CH server): a `QueryContext` carrying `CreateTable` is
driven through the chain; with a vetoing observer wired in, assert no
packet reaches the upstream and the client receives an Exception.

## 9. Open questions

1. **~~Post-execute hook for failure compensation later?~~ Resolved.**
   `OnStatementException` is wired in v1 with explicit best-effort
   semantics: Relay decodes upstream Exception via a first-byte heuristic,
   the chain dispatches to subscribed observers, and observers must
   tolerate dropped events. `InMemoryCommitGateObserver` uses this hook
   to revert its in-memory mutations when ClickHouse rejects the DDL.
2. **Late-binding `Proxy.RegisterCommitGate(...)`?** Out of scope for v1;
   can be added without breaking `Options`-style callers.
3. **Agent-side gate?** If hosts want chain commits to happen at the
   agent instead of the server proxy, we can add a agent variant —
   but the chain commit must be a single source of truth, so the *server*
   plugin would need to be configurable to skip when the agent already
   handled it. Defer until demand.

## 10. Implementation prerequisites

This design assumes the Go side of the rewriter is migrated to consume
the new structured `original_accessed_tables` proto field. The migration
is a discrete, mechanical change that should land before the commitgate
work itself:

1. **`pkg/rewriter/types.go`** — add an `AccessedTable` struct mirroring
   the proto:
   ```go
   type AccessedTable struct {
       OriginalDatabase string
       OriginalTable    string
       LogicalDatabase  string
       PhysicalDatabase string
       IsRemote         bool
   }
   ```
   Change `RewriteResult.AccessedTables` from `[]string` to
   `[]AccessedTable`.

2. **`pkg/rewriter/sentio.go`** — replace
   `resp.GetOriginalAccessedTableNames()` with a converter from the new
   `repeated AccessedTable` field. The old proto tag (4) is now
   `reserved`, so the existing call will start failing to compile once
   the generated code is regenerated.

3. **`pkg/plugin/context.go`** — `QueryContext.AccessedTables` becomes
   `[]rewriter.AccessedTable`. Update the doc comment (currently it
   describes the old `[]string` shape).

4. **All readers of `AccessedTables`** — sweep for current consumers
   (`grep -r "AccessedTables" pkg/ cmd/`) and update them to use the
   structured form. Most consumers today only read for logging / audit
   and can keep using `OriginalDatabase + "." + OriginalTable` for the
   human-readable form.

5. **Tests** — update fixtures that built `RewriteResult` literals.
