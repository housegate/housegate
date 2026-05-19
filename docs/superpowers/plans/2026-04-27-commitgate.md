# `commitgate` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Branch policy:** This user prefers branches over worktrees ([memory:feedback_branch_over_worktree](../../../.claude/projects/-Users-uranuswch-Dev-housegate-housegate/memory/feedback_branch_over_worktree.md)). Create a single branch `feature/commitgate` and execute the whole plan in place. **Do NOT create a worktree.**

**Goal:** Add a new `commitgate` plugin that lets library hosts of `housegate.New(...)` register synchronous, vetoable hooks on `CREATE TABLE` / `DROP TABLE` / `CREATE DATABASE` / `DROP DATABASE`. Hooks fire **before** the statement reaches ClickHouse; an error from the hook aborts the query and returns a synthetic Exception to the client. The motivating use case is on-chain ownership registration: the host commits to chain inside the hook, returns nil on chain success (CH proceeds) or error on chain failure (CH never touched).

**Architecture (read the spec first):** [docs/superpowers/specs/2026-04-27-commitgate-design.md](../specs/2026-04-27-commitgate-design.md). The plan assumes familiarity with the Observer/Event/Plugin shapes, the chain insertion point, the idempotency contract, and the §10 rewriter-consumption prerequisite.

**Tech Stack:** Go 1.24, Bazel 8.5.1 (Bzlmod), gRPC, Prometheus client_golang, sentio-core/log.

**Phase ordering & PR strategy:** The plan is one coherent feature branch. Within the branch, commits are organized so each phase is independently reviewable:

1. **Phase 1** — rewriter consumption migration (Go side reads new structured `AccessedTable`).
2. **Phase 2** — mechanical rename `pkg/plugins/state` → `pkg/plugins/sessionstate`.
3. **Phase 3** — `commitgate` package: types, plugin, unit tests.
4. **Phase 4** — wire into `housegate.Options` and `buildServer`.
5. **Phase 5** — contract verification against real rewriter; integration test.

Phases 1 and 2 are mechanical and order-independent; 3 depends on 1; 4 depends on 3; 5 depends on 4. **One commit per task** (not per phase) — keeps the diff reviewable.

**Build & test commands:**

```bash
bazel build //...                                     # full build
bazel test //...                                      # full test
bazel test //pkg/plugins/commitgate:commitgate_test   # this feature only
bazel test //pkg/proxy:proxy_test                     # main proxy test bundle
```

`go test ./...` does NOT work in this repo (protobuf init panic). Always use Bazel.

---

## Phase 1: Rewriter consumption migration (prerequisite)

The proto already exposes `repeated AccessedTable original_accessed_tables = 12`, but the Go side still reads the deprecated `OriginalAccessedTableNames` (tag 4, now `reserved`). Migrate Go consumption to the structured form. This is purely a refactor — no behaviour change, no commitgate code yet.

**Why first:** the commitgate plugin's `buildEvent` reads `qctx.AccessedTables[i].LogicalDatabase`. That field only exists after this migration.

### Task 1.1: Add `AccessedTable` struct and change `RewriteResult.AccessedTables` type

**Files:**
- Modify: `pkg/rewriter/types.go`

- [ ] **Step 1: Add the struct**

In `pkg/rewriter/types.go`, add (alongside the existing `RewriteResult` definition):

```go
// AccessedTable is one table the SQL referenced before rewrite,
// with the rewriter's best-effort resolution of its physical
// database under the active TableNameRewrite mode. Mirrors proto
// message AccessedTable (rewriter.proto tag 12 child).
//
// OriginalDatabase / OriginalTable are exactly what the SQL
// contained (empty database when the table was unqualified).
// LogicalDatabase / PhysicalDatabase are populated when the
// rewriter has enough context to resolve them; both can
// legitimately be empty (see proto comment for the cases).
//
// IsRemote is true iff the rewriter routed (or would have routed)
// this access through `remote(addr, db, table, user, password)`.
type AccessedTable struct {
    OriginalDatabase string
    OriginalTable    string
    LogicalDatabase  string
    PhysicalDatabase string
    IsRemote         bool
}
```

- [ ] **Step 2: Change the `RewriteResult` field**

Find the `AccessedTables []string` field on `RewriteResult` and change to `[]AccessedTable`. Update the field's doc comment to refer to the new structured shape (drop "list of `db.table` strings"-style language).

- [ ] **Step 3: Build — expect downstream compile errors**

```bash
bazel build //pkg/rewriter:rewriter
```

This target itself should still build (no other code in the rewriter package consumes `AccessedTables` beyond construction). The errors will surface in callers — that is Task 1.3.

### Task 1.2: Convert proto `AccessedTable` → Go `AccessedTable` in `sentio.go`

**Files:**
- Modify: `pkg/rewriter/sentio.go`

- [ ] **Step 1: Add a helper**

Add to `pkg/rewriter/sentio.go` (near the other `*FromProto` helpers like `statementTypeFromProto`):

```go
func accessedTablesFromProto(in []*pb.AccessedTable) []AccessedTable {
    if len(in) == 0 {
        return nil
    }
    out := make([]AccessedTable, len(in))
    for i, t := range in {
        out[i] = AccessedTable{
            OriginalDatabase: t.GetOriginalDatabase(),
            OriginalTable:    t.GetOriginalTable(),
            LogicalDatabase:  t.GetLogicalDatabase(),
            PhysicalDatabase: t.GetPhysicalDatabase(),
            IsRemote:         t.GetIsRemote(),
        }
    }
    return out
}
```

- [ ] **Step 2: Replace the two call sites**

In `pkg/rewriter/sentio.go` lines ~211-228 (the two `RewriteResult{...}` literals — Success and UnsupportedStatement branches):

Replace:
```go
AccessedTables: resp.GetOriginalAccessedTableNames(),
```
with:
```go
AccessedTables: accessedTablesFromProto(resp.GetOriginalAccessedTables()),
```

The `GetOriginalAccessedTableNames` method may no longer exist after the regenerated `.pb.go` (the proto reserved that tag). If the regen has not yet happened in this branch, regenerate first via `bazel mod tidy && bazel run //:gazelle` or whatever the project's proto-build command is.

- [ ] **Step 3: Update `discoverAccessedTables` (lines ~325-339)**

This helper currently returns `[]string` from `OriginalAccessedTableNames`. It is used internally by phase-1 rewrite to derive table names for static mapping. Two options:

  a. Keep its return type `[]string` and convert inside: `OriginalDatabase + "." + OriginalTable` for each entry (or just `OriginalTable` if the rest of the static-mapping path expects bare names — verify by reading `parseTables`).
  b. Change return to `[]AccessedTable` and let callers project as needed.

Pick (a) — least invasive — unless reading `parseTables` reveals it would benefit from the structured form. **Verify `parseTables` expectations before choosing.**

- [ ] **Step 4: Build the rewriter package**

```bash
bazel build //pkg/rewriter:rewriter
```

Should succeed.

### Task 1.3: Update `plugin.QueryContext.AccessedTables` and sweep all readers

**Files:**
- Modify: `pkg/plugin/context.go` (field type + doc)
- Sweep + modify: any other reader of `RewriteResult.AccessedTables` or `QueryContext.AccessedTables`.

- [ ] **Step 1: Sweep**

```bash
grep -rn "AccessedTables" /Users/uranuswch/Dev/housegate/housegate/pkg \
                          /Users/uranuswch/Dev/housegate/housegate/cmd \
                          /Users/uranuswch/Dev/housegate/housegate/build.go \
                          /Users/uranuswch/Dev/housegate/housegate/proxy.go \
                          2>/dev/null
```

For each hit outside `pkg/rewriter/` and `pkg/plugin/context.go`, decide what to do:

- **Logging/audit consumers** — change format to `OriginalDatabase + "." + OriginalTable` (or whatever projection preserves the human-readable form they had).
- **Plugins consuming AccessedTables for routing decisions** — read the relevant field directly (e.g. `LogicalDatabase` for namespace decisions).
- **Test fixtures** — update `RewriteResult` literals to use the new `AccessedTable` struct.

- [ ] **Step 2: Update `pkg/plugin/context.go`**

Change `AccessedTables []string` → `AccessedTables []rewriter.AccessedTable`. Rewrite the doc comment to describe the structured shape; remove the `[]string` description.

- [ ] **Step 3: Build & test all**

```bash
bazel build //...
bazel test //...
```

All existing tests should pass — this is a no-op refactor.

### Task 1.4: Phase 1 verification commit

- [ ] **Step 1: Confirm clean build + test on the branch**

```bash
git status
bazel test //...
```

- [ ] **Step 2: Commit phase 1 with message:**

```
refactor(rewriter): consume structured AccessedTable from proto

Replaces the deprecated OriginalAccessedTableNames []string consumption
with the structured original_accessed_tables (proto tag 12). No
behaviour change; downstream readers updated to project the new struct
into their existing log/audit forms.

Prepares the ground for the commitgate plugin which needs
LogicalDatabase resolution from the rewriter.
```

---

## Phase 2: Rename `pkg/plugins/state` → `pkg/plugins/sessionstate`

Mechanical rename — no behaviour change. Done as its own commit so the commitgate diff (next phase) is uncluttered.

### Task 2.1: Move the directory and update package declaration

**Files:**
- Move: `pkg/plugins/state/` → `pkg/plugins/sessionstate/`
- Modify: every `package state` → `package sessionstate` in moved files.

- [ ] **Step 1: Move the directory**

```bash
git mv pkg/plugins/state pkg/plugins/sessionstate
```

- [ ] **Step 2: Update package declarations**

In each `.go` file under `pkg/plugins/sessionstate/`, change the `package state` declaration to `package sessionstate`. Also update any leading package doc comment that says `// Package state ...` to `// Package sessionstate ...`.

- [ ] **Step 3: Update `BUILD.bazel`**

In `pkg/plugins/sessionstate/BUILD.bazel`, update:
- `name = "state"` → `name = "sessionstate"`
- `importpath = ".../pkg/plugins/state"` → `importpath = ".../pkg/plugins/sessionstate"`
- Anything else referencing the old name.

### Task 2.2: Update importers

**Files:**
- `build.go` (the only known importer; verify with grep)

- [ ] **Step 1: Find all importers**

```bash
grep -rn "pkg/plugins/state" /Users/uranuswch/Dev/housegate/housegate
grep -rn "statePlugin\|statePlug\|state\.Plugin\|state\.Config" /Users/uranuswch/Dev/housegate/housegate
```

The first grep finds import paths; the second finds usages by alias / type.

- [ ] **Step 2: Update each importer**

Replace import paths `pkg/plugins/state` → `pkg/plugins/sessionstate`. If the import alias was `statePlugin "..."`, rename to `sessionstate "..."` and update the call sites accordingly (`statePlugin.Plugin{}` → `sessionstate.Plugin{}`, etc.).

In `build.go` line 348:
```go
statePlug := &statePlugin.Plugin{Config: cfg.State}
```
becomes:
```go
sessstatePlug := &sessionstate.Plugin{Config: cfg.State}
```
(Or pick whichever local name reads clean — `sessstatePlug` / `ssPlug`. Don't matter much; just be consistent within the file.)

- [ ] **Step 3: Build & test**

```bash
bazel build //...
bazel test //...
```

Pass.

### Task 2.3: Phase 2 commit

- [ ] **Step 1: Commit:**

```
refactor: rename pkg/plugins/state → pkg/plugins/sessionstate

Disambiguates the plugin from the new commitgate plugin (which
introduces a `statement` concept) and from generic "state" elsewhere
in the codebase. No behaviour change.
```

---

## Phase 3: `commitgate` package

The feature itself: types, plugin, unit tests. Independent of `Options` wiring, which is Phase 4. This phase ends with a complete, tested package that does nothing yet because nothing constructs it.

### Task 3.1: Create the package skeleton

**Files:**
- Create: `pkg/plugins/commitgate/observer.go`
- Create: `pkg/plugins/commitgate/plugin.go`
- Create: `pkg/plugins/commitgate/BUILD.bazel`

- [ ] **Step 1: Write `observer.go`**

```go
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

    "housegate/housegate/pkg/rewriter"
)

// Observer is the contract a host implements to gate DDL.
type Observer interface {
    // SubscribedTypes returns the StatementTypes this observer
    // wants to be invoked for. Empty / nil means "never fire".
    SubscribedTypes() []rewriter.StatementType

    // BeforeStatement runs synchronously before the Query is
    // forwarded to upstream. A non-nil return aborts the Query.
    //
    // Implementations MUST be idempotent: the same Event may be
    // delivered more than once. The framework provides no
    // de-duplication.
    //
    // Implementations MUST honour ctx (the connection's context)
    // and MUST NOT block indefinitely — the call holds the
    // per-connection clientToUpstream goroutine.
    BeforeStatement(ctx context.Context, ev *Event) error
}

// Event is the read-only payload delivered to BeforeStatement.
//
// Fields are valid only for the duration of the call; observers
// must not retain pointers.
type Event struct {
    // Type is the rewriter classification. Always one of the
    // values an observer subscribed to.
    Type rewriter.StatementType

    // User is the authenticated client user.
    User string

    // Database is the canonical logical database the statement
    // targets, sourced from AccessedTable.LogicalDatabase. Stable
    // across qualified vs. unqualified SQL forms. Never empty
    // when delivered (the dispatcher rejects the query before
    // delivery if LogicalDatabase is empty).
    Database string

    // Table is the table being created / dropped, taken from
    // AccessedTable.OriginalTable. Empty for database-level
    // statements.
    Table string

    // QueryID is the upstream-bound ClickHouse query id, useful
    // for log correlation.
    QueryID string

    // OriginalSQL is the SQL the client sent (pre-rewrite).
    OriginalSQL string

    // RewrittenSQL is what would be sent to upstream if the
    // hook returns nil. For logging only.
    RewrittenSQL string
}
```

- [ ] **Step 2: Write `plugin.go`**

```go
package commitgate

import (
    "context"
    "errors"
    "fmt"

    "sentioxyz/sentio-core/common/log"

    "housegate/housegate/pkg/plugin"
    "housegate/housegate/pkg/rewriter"
)

// Plugin is the QueryPlugin that dispatches BeforeStatement to
// registered observers based on classified StatementType.
//
// Plugin is NOT RouteAware: routed (proxy-to-proxy) sessions skip
// it because the destination proxy fires its own commitgate
// plugin and we must not double-fire.
type Plugin struct {
    byType map[rewriter.StatementType][]Observer
}

// NewPlugin builds a Plugin from the given Observers, indexed by
// StatementType for O(1) dispatch. Observers with no subscribed
// types are silently skipped (they cannot fire anyway).
func NewPlugin(observers []Observer) *Plugin {
    p := &Plugin{byType: make(map[rewriter.StatementType][]Observer)}
    for _, o := range observers {
        for _, t := range o.SubscribedTypes() {
            p.byType[t] = append(p.byType[t], o)
        }
    }
    return p
}

// SubscribedTypes returns the union of all observers' subscribed
// StatementTypes — a small helper for buildServer to log what's
// active.
func (p *Plugin) SubscribedTypes() []rewriter.StatementType {
    out := make([]rewriter.StatementType, 0, len(p.byType))
    for t := range p.byType {
        out = append(out, t)
    }
    return out
}

// OnQuery dispatches to subscribed observers if qctx.StatementType
// matches. StatementTypeUnspecified (the rewriter didn't classify)
// is treated as "no dispatch" — observers are not invoked on
// unknown classifications.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
    obs, ok := p.byType[qctx.StatementType]
    if !ok || len(obs) == 0 {
        return nil
    }
    ev, err := buildEvent(qctx)
    if err != nil {
        return fmt.Errorf("commitgate (%s): %w", qctx.StatementType, err)
    }
    for _, o := range obs {
        if err := o.BeforeStatement(ctx, ev); err != nil {
            return fmt.Errorf("commitgate (%s): %w", qctx.StatementType, err)
        }
    }
    return nil
}

// errNoAccessedTables / errEmptyLogicalDB / errEmptyTable are
// exported as sentinel values primarily so tests can match them
// with errors.Is. Hosts should not branch on them.
var (
    errNoAccessedTables = errors.New("rewriter did not surface accessed tables")
    errEmptyLogicalDB   = errors.New("rewriter did not resolve logical database")
    errEmptyTable       = errors.New("rewriter did not surface table name")
)

// buildEvent extracts the (database, table) pair from
// qctx.AccessedTables and constructs an Event. Returns an error
// if the rewriter contract is violated for the statement type;
// see the spec §4.6 for the validation rules.
func buildEvent(qctx *plugin.QueryContext) (*Event, error) {
    tables := qctx.AccessedTables
    if len(tables) == 0 {
        return nil, errNoAccessedTables
    }
    if len(tables) > 1 {
        // Defensive: rewriter contract says exactly one entry for
        // the supported DDL types. Log and pick the first rather
        // than crashing if the contract drifts.
        log.Warnw("commitgate: rewriter returned multiple accessed tables for DDL; using first",
            "type", qctx.StatementType,
            "count", len(tables),
        )
    }
    entry := tables[0]
    if entry.LogicalDatabase == "" {
        return nil, errEmptyLogicalDB
    }
    isTableScoped := qctx.StatementType == rewriter.StatementTypeCreateTable ||
        qctx.StatementType == rewriter.StatementTypeDropTable
    if isTableScoped && entry.OriginalTable == "" {
        return nil, errEmptyTable
    }

    var queryID string
    if qctx.Query != nil {
        queryID = qctx.Query.QueryID
    }

    return &Event{
        Type:         qctx.StatementType,
        User:         qctx.Session.User(),
        Database:     entry.LogicalDatabase,
        Table:        entry.OriginalTable,
        QueryID:      queryID,
        OriginalSQL:  qctx.OriginalSQL,
        RewrittenSQL: qctx.RewrittenSQL,
    }, nil
}
```

(Verify the exact `chproto.Query` field name for `QueryID` before committing — the literal name may be `QueryID` or `Id`. Adjust accordingly.)

- [ ] **Step 3: Write `BUILD.bazel`**

Mirror the layout of `pkg/plugins/route/BUILD.bazel` (single `go_library` + `go_test` target). Deps:
- `//pkg/plugin`
- `//pkg/rewriter`
- `@sentio_core//common/log` (or whatever the project alias is)

Run:
```bash
bazel run //:gazelle
bazel build //pkg/plugins/commitgate:commitgate
```

If gazelle handles BUILD generation in this repo, prefer `gazelle` over hand-writing. Verify against another plugin's BUILD.bazel.

### Task 3.2: Unit tests

**Files:**
- Create: `pkg/plugins/commitgate/plugin_test.go`

- [ ] **Step 1: Write the test cases listed in spec §8**

Cover all eight cases:

1. Dispatch by StatementType — fake observer subscribes to `CreateTable`; drive each StatementType; observer fires only on `CreateTable`.
2. Multiple observers, slice order — two observers, same subscription; both fire in order; first error short-circuits.
3. Veto — observer returns error; `OnQuery` returns wrapped error.
4. `Database`/`Table` extraction — each of the four DDL types, qualified and unqualified inputs.
5. Empty `LogicalDatabase` aborts — `errors.Is(err, errEmptyLogicalDB)`.
6. Empty `AccessedTables` aborts — `errors.Is(err, errNoAccessedTables)`.
7. `StatementTypeUnspecified` is a no-op.
8. ctx cancellation propagates — observer receives a cancelled context.

Use a small `fakeObserver` struct in the test file; no need for a mocking framework.

For `QueryContext` construction in tests, study how an existing plugin test (e.g. `pkg/plugins/usage/`) builds its `QueryContext` — match that pattern so we don't reinvent fixtures.

- [ ] **Step 2: Run**

```bash
bazel test //pkg/plugins/commitgate:commitgate_test --test_output=errors
```

All 8 cases pass.

### Task 3.3: Phase 3 commit

- [ ] **Step 1: Commit:**

```
feat(commitgate): add DDL gate plugin with Observer registration

Introduces pkg/plugins/commitgate, a QueryPlugin that dispatches
BeforeStatement to registered Observers when the rewriter
classifies a query as CREATE/DROP TABLE/DATABASE. A non-nil error
from any Observer aborts the query before it reaches ClickHouse.

Idempotency is the Observer's responsibility; the framework provides
ordering and short-circuit only. See the design spec for the
failure model and non-guarantees.

Plugin is not yet wired into buildServer — that lands in the next
commit.
```

---

## Phase 4: Wire `Options.CommitGateObservers` into `buildServer`

The plugin from Phase 3 is dormant until something constructs it. Add the `Options` field and the `buildServer` wiring.

### Task 4.1: Add `Options.CommitGateObservers`

**Files:**
- Modify: `proxy.go`

- [ ] **Step 1: Add the field**

In `proxy.go`'s `Options` struct, add (after the existing dependency overrides):

```go
// CommitGateObservers gate DDL statements (CREATE / DROP TABLE,
// CREATE / DROP DATABASE) on host-supplied external commits.
// Observers fire in slice order; the first non-nil error
// short-circuits the chain and aborts the query before
// ClickHouse is contacted. Empty / nil = no commitgate plugin
// is wired.
//
// Server mode only. Agent / forwarding-only modes ignore this
// field — they forward to a server-mode proxy that fires its own
// gate. Routed (proxy-to-proxy) sessions also skip the plugin.
//
// See pkg/plugins/commitgate for the Observer contract,
// including the mandatory idempotency requirement.
CommitGateObservers []commitgate.Observer
```

Add the import: `"housegate/housegate/pkg/plugins/commitgate"`.

### Task 4.2: Wire into `buildServer`

**Files:**
- Modify: `build.go`

- [ ] **Step 1: Insert the wiring block**

After the existing `if rewritePlug != nil { queryPlugins = append(queryPlugins, rewritePlug) }` (around line 363-365), insert:

```go
if len(opts.CommitGateObservers) > 0 {
    cgPlug := commitgate.NewPlugin(opts.CommitGateObservers)
    queryPlugins = append(queryPlugins, cgPlug)
    log.Infow("commitgate enabled",
        "observers", len(opts.CommitGateObservers),
        "subscribed_types", cgPlug.SubscribedTypes(),
    )
}
```

The position is **after** rewrite (so `qctx.StatementType` and `qctx.AccessedTables` are populated) and **before** `routeplugin.Signer` (so we abort before JWS signing).

- [ ] **Step 2: Confirm `buildAgent` and `buildForwarding` are NOT touched**

Quick grep: `grep -n "CommitGate\|commitgate" /Users/uranuswch/Dev/housegate/housegate/build.go` — should appear only in `buildServer`.

- [ ] **Step 3: Update BUILD.bazel**

Either re-run `bazel run //:gazelle` or hand-add `//pkg/plugins/commitgate` as a dep of the root `housegate` package and `//cmd:cmd_lib`.

### Task 4.3: End-to-end build & test

- [ ] **Step 1: Full build + test**

```bash
bazel build //...
bazel test //...
```

Existing tests (no observer registered) should pass — empty `CommitGateObservers` slice means the plugin is not wired, so behaviour is unchanged.

### Task 4.4: Phase 4 commit

- [ ] **Step 1: Commit:**

```
feat(housegate): wire CommitGateObservers into buildServer

Adds Options.CommitGateObservers (server mode only). When
non-empty, buildServer constructs commitgate.NewPlugin and inserts
it into the QueryPlugin chain after rewrite and before
routeplugin.Signer.

Existing operators see no change — empty slice = no plugin wired.
```

---

## Phase 5: Contract verification + integration test

The design's §4.6 makes assumptions about what the rewriter populates in `original_accessed_tables` for each of the four DDL types. Verify these assumptions against the real rewriter, and add an end-to-end integration test that drives the full chain.

### Task 5.1: Verify rewriter contract for each DDL type

The rewriter is an external gRPC service; the integration tests under `pkg/proxy/rewriter_e2e_test.go` already need it on `localhost:50051`. Add a focused test that issues each of the four DDLs and asserts the response shape.

**Files:**
- Modify: `pkg/rewriter/sentio_test.go` (or whichever file exercises the gRPC client end-to-end; pick the existing one closest in style)

- [ ] **Step 1: Write a parameterised test**

Pseudocode:

```go
cases := []struct {
    name       string
    sql        string
    sessionDB  string  // session-context logical DB
    wantType   rewriter.StatementType
    wantTable  string
    wantLogDB  string  // expected LogicalDatabase
}{
    {"create_table_qualified",   "CREATE TABLE foo.t (id UInt64) ENGINE=MergeTree ORDER BY id", "", rewriter.StatementTypeCreateTable, "t", "foo"},
    {"create_table_unqualified", "CREATE TABLE t (id UInt64) ENGINE=MergeTree ORDER BY id",     "foo", rewriter.StatementTypeCreateTable, "t", "foo"},
    {"drop_table_qualified",     "DROP TABLE foo.t",   "", rewriter.StatementTypeDropTable, "t", "foo"},
    {"drop_table_unqualified",   "DROP TABLE t",       "foo", rewriter.StatementTypeDropTable, "t", "foo"},
    {"create_database",          "CREATE DATABASE foo","", rewriter.StatementTypeCreateDatabase, "", "foo"},
    {"drop_database",            "DROP DATABASE foo",  "", rewriter.StatementTypeDropDatabase,   "", "foo"},
}
```

For each, call the rewriter with the appropriate session context and assert:
- `result.StatementType == wantType`
- `len(result.AccessedTables) >= 1`
- `result.AccessedTables[0].OriginalTable == wantTable`
- `result.AccessedTables[0].LogicalDatabase == wantLogDB`

- [ ] **Step 2: Run against local rewriter**

```bash
bazel test //pkg/rewriter:rewriter_test --test_filter='DDL.*Contract'
```

(Skip with `t.Skip("rewriter not reachable")` if the gRPC service isn't up locally — match the existing skip pattern from other e2e tests.)

- [ ] **Step 3: If any case fails:**

The contract is broken for that DDL type. Two options:

  a. **Fix on the rewriter side** — file a PR against the rewriter repo to populate `original_accessed_tables` for that statement type. Document the gap in §9 of the design (open question) and proceed without that statement type in v1's subscribed set.

  b. **Document and downscope** — if only `CREATE/DROP DATABASE` is broken (likely), have the v1 commitgate plugin reject subscriptions to those types with a clear error at `NewPlugin` construction time, and add a TODO in the design.

  Pick (a) if the rewriter team is responsive; (b) otherwise. **Do not** silently fall back to extracting the database name from raw SQL inside commitgate — that breaks the design's "rewriter is the SQL parser" invariant.

### Task 5.2: End-to-end integration test through the chain

**Files:**
- Add: `pkg/proxy/commitgate_e2e_test.go` (or extend `relay_test.go`)

- [ ] **Step 1: Write the test**

Drive a full session through `Relay`:
1. Set up a mock CH upstream (use the existing test helper).
2. Construct a `housegate.New` with a `CommitGateObservers` slice containing one observer that records calls and returns nil.
3. Connect a client, send `CREATE TABLE foo.t (...)`.
4. Assert the observer was called once with the expected Event (`Type=CreateTable`, `Database=foo`, `Table=t`).
5. Assert the upstream received the query.

Then a second test with the observer returning an error:
6. Same setup, observer returns `errors.New("on-chain failed")`.
7. Send the same query; assert the upstream did NOT receive it; assert the client received an Exception whose message contains `"commitgate (CREATE_TABLE)"` and `"on-chain failed"`.

- [ ] **Step 2: Run**

```bash
bazel test //pkg/proxy:proxy_test --test_filter='CommitGate'
```

Both tests pass.

### Task 5.3: Phase 5 commit

- [ ] **Step 1: Commit:**

```
test(commitgate): verify rewriter contract + add end-to-end test

Adds rewriter-side parameterised test covering AccessedTable shape
for each gated DDL (CREATE/DROP TABLE, CREATE/DROP DATABASE),
qualified and unqualified forms.

Adds proxy-side integration test driving a full session through
the chain with a vetoing Observer; asserts upstream is never
contacted when the gate returns an error.
```

---

## Phase 6: Final verification, PR

### Task 6.1: Full repo build & test

- [ ] **Step 1: Clean run**

```bash
bazel clean
bazel build //...
bazel test //...
```

All green. No regressions vs. main baseline.

- [ ] **Step 2: Lint / formatter pass (if the repo has one)**

```bash
make fmt 2>/dev/null || gofmt -l .
```

### Task 6.2: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add a short bullet under "Key Modules"**

```
- **[pkg/plugins/commitgate/](pkg/plugins/commitgate/)** — DDL gate
  plugin (server mode only, not RouteAware). Observers registered via
  housegate.Options.CommitGateObservers run synchronously before
  CREATE/DROP TABLE/DATABASE reaches upstream; a non-nil return
  aborts the query. Idempotency is the observer's responsibility.
```

- [ ] **Step 2: Update the rename in the existing module list**

Find `pkg/plugins/state` references in CLAUDE.md and update to `pkg/plugins/sessionstate`.

### Task 6.3: Push branch & open PR

- [ ] **Step 1: Push**

```bash
git push -u origin feature/commitgate
```

- [ ] **Step 2: Open PR**

```bash
gh pr create \
  --title "feat: add commitgate plugin for DDL pre-execute hooks" \
  --body "$(cat <<'EOF'
## Summary

- Adds `pkg/plugins/commitgate`, a server-mode QueryPlugin that
  dispatches synchronous, vetoable Observer hooks on CREATE/DROP
  TABLE/DATABASE before the statement reaches ClickHouse.
- Library hosts register Observers via
  `housegate.Options.CommitGateObservers`. A non-nil Observer error
  aborts the query and returns a synthetic Exception to the client;
  ClickHouse is never contacted.
- Idempotency is the Observer's responsibility — the framework
  provides only "fire before CH" ordering and short-circuit on
  error. See the design spec for the full failure model.

## Sub-changes (one commit each)

1. Migrate Go consumption of the rewriter response to the new
   structured `AccessedTable` proto field (was `[]string`).
2. Rename `pkg/plugins/state` → `pkg/plugins/sessionstate`.
3. Add the `commitgate` package with unit tests.
4. Wire `Options.CommitGateObservers` into `buildServer`.
5. Verify rewriter contract for each gated DDL type +
   end-to-end integration test.
6. CLAUDE.md updates.

## Test plan

- [ ] `bazel test //...` passes
- [ ] New unit tests in `pkg/plugins/commitgate/` pass
- [ ] Rewriter contract test passes (or any gap is filed as a
      rewriter-side issue and downscoped here)
- [ ] End-to-end test asserts: success path delivers query to
      upstream; veto path keeps query off the wire and surfaces
      the Observer's error message to the client

## Spec

[docs/superpowers/specs/2026-04-27-commitgate-design.md](docs/superpowers/specs/2026-04-27-commitgate-design.md)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Risks & rollback

- **Phase 1 risk:** missing a reader of `AccessedTables` outside the immediate sweep. Mitigation: full `bazel build //...` will surface compile errors. Rollback: revert the phase-1 commit; phases 2-5 are independent only if phase 1 lands first.
- **Phase 5 risk:** rewriter doesn't populate `LogicalDatabase` for `CREATE/DROP DATABASE`. Mitigation: documented in §4.6 — fall back to filing a rewriter-side fix and downscoping v1 to `CREATE/DROP TABLE` until that lands.
- **Operator surprise:** none. Observers are opt-in; existing operators see no change.

## What's NOT in this plan (deferred per design §9)

- Late-binding `Proxy.RegisterCommitGate(...)` API — Options-only registration in v1.
- Post-execute / compensation hook — host-side reconciler if needed.
- Agent-mode gate — single point of enforcement is server-mode.
- `RENAME TABLE` / `ALTER TABLE` gating.
