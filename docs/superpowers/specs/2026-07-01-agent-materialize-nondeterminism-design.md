# Agent-Mode Non-Determinism Materialization (Phase 1) — Design

**Date:** 2026-07-01 **Status:** Proposed (v1) **Base:** [2026-06-22 Storage Integrity design §7 / §9 "Phase 1 — non-determinism materialization (agent/SDK, trusted, before signing)"](2026-06-22-storage-integrity-design.md)

## 1. Summary and Positioning

This feature implements **Phase 1 of the storage-integrity rewrite split** on the housegate side: in agent mode, before the agent signs an outgoing query, non-deterministic functions in the SQL text (`now()`, `rand()`, `generateUUIDv4()`, …) are rewritten to literal constants so that the SQL the user signs is deterministic. Per the storage-integrity design §7, the envelope signs the Phase-1 output (`rewritten_sql`), and every executor that later replays the statement runs the same constants, so determinism holds by construction.

The rewriting itself is done by the `MaterializeSQL` capability already implemented in `rewriter-go` (>= v0.3.0). housegate is purely the caller: it constructs the materialization inputs, invokes the engine (native in-process or via the `sql-rewriter` gRPC service), and swaps the query body for the materialized SQL before signing.

The feature is **flag-gated and off by default**. When enabled, the operator explicitly chooses the backend engine (`native` or `grpc`), mirroring the existing `rewriter` backend selection.

Scope boundary: this is Phase 1 (non-determinism materialization) only. It does not implement Phase 2 (deterministic physical table rewrite), `_hg_row_id` injection, envelope building, payload spooling, or any Keeper/replay interaction — those are separate items in the storage-integrity roadmap. This change only makes the *signed SQL text* deterministic.

## 2. Goals and Non-Goals

Goals:

1. In agent mode, materialize non-deterministic functions in the outgoing SQL to constants **before** the agent signer runs, so the signed body and the forwarded body are identical and deterministic.
2. Reuse the existing `rewriter-go` grpc/native backend seam so the operator can pick either transport.
3. Default off; when enabled, a single flag plus an engine selection turns it on.
4. Never silently break query traffic: any materialization failure falls open to the original SQL (operator-visible via logs and metrics).
5. Keep the new plugin self-contained and unit-testable without a live gRPC service or FFI library.

Non-Goals:

1. Phase 2 physical/table-name rewrite (that already exists for server mode via `pkg/plugins/rewrite`; it is out of scope here and does not run in agent mode).
2. `_hg_row_id` injection, `StatementEnvelopeV2` construction, payload spooling, or any signing-envelope change beyond the SQL body the agent already signs.
3. Server-mode materialization. The block is read only in agent mode.
4. Reproducible/seeded randomness. Because replay reads the materialized constants (never re-materializes), the agent's RNG source does not need to be reproducible.
5. Implementing the `MaterializeSQL` server RPC inside the `sql-rewriter` service (that lives in a different repository; see §9).

## 3. The `MaterializeSQL` Contract (rewriter-go v0.3.0)

Both the native `*rewriter.Service` and the gRPC `RewriterServiceClient` expose the same method, over the same `pb` messages:

```go
MaterializeSQL(ctx, *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error)
```

Request:

```text
MaterializeSQLRequest {
  sql     string
  inputs  MaterializationInputs {
    now_unix_ns           *int64     // trusted agent/SDK clock; REQUIRED when materializing time functions
    random_uint64_values  []uint64   // consumed in traversal order by rand()/rand32()/rand64()/random()
    uuid_values           []string   // consumed in traversal order by generateUUIDv4()
    random_float64_values []float64   // consumed in traversal order by randCanonical(); finite, in [0,1)
  }
  policy  MaterializationPolicy {
    profile_id  string  // empty selects default "sentio-p1-nondet-v1"
    timezone    string  // v1 accepts empty or "UTC" only
  }
}
```

Response:

```text
MaterializeSQLResponse {
  code                        MaterializeCode  // Success / SyntaxError / UnsupportedStatement / InvalidRequest / Error
  sql                         string           // echo of input
  sql_after_materialization   string           // materialized SQL (== input when no replacements)
  message                     string           // diagnostic on non-Success
  replacements                []MaterializedReplacement { function_name, ordinal, literal_sql, value_type }
  materializer_profile_id     string
}
```

Behavior notes (from the v0.3.0 implementation):

- The engine is a pure function of `(sql, inputs, policy)` — it does **not** read a clock or RNG itself. The caller supplies `now`/random/uuid values.
- Inputs are consumed in traversal order; **over-supplying is safe** (extras ignored), under-supplying yields `InvalidRequest` (mapped from `ErrMaterializationInputMissing`).
- `Success` with zero `replacements` means the SQL had no materializable non-determinism — the body is left unchanged.
- Non-`UTC` timezone or an unknown `profile_id` yields `InvalidRequest`.

## 4. Architecture and Data Flow

A new `QueryPlugin` runs first in the agent-mode chain, ahead of the signer:

```text
buildAgent QueryPlugins (when materialize.enabled):
  materialize.Plugin  →  agent.Plugin (signer)  →  metrics
```

`materialize.Plugin.OnQuery` rewrites `qctx.Query.Body` in place. Because (a) the agent signer signs `qctx.Query.Body`, and (b) the relay re-encodes the outgoing query from the same `Query` struct via `Codec.WriteQuery`, mutating the body once makes **both the signed bytes and the forwarded bytes** the materialized SQL. This matches storage-integrity §7: the signed `rewritten_sql` is the materialized text, and the source executes that same text.

Per-query flow:

```text
1. materialize.Plugin.OnQuery(qctx):
     if disabled / nil body → return (no-op)
     inputs  = { now = time.Now().UnixNano(),
                 random_uint64  = N crypto/rand uint64,
                 random_float64 = N crypto/rand float64 in [0,1),
                 uuid           = N uuid.NewString() }        // N = random_pool_size
     out, err = Materializer.Materialize(ctx, qctx.Query.Body)
     if err != nil            → metric(call_error); log warn; return (fail-open, body unchanged)
     if out.Code != Success   → metric(non_success, code); log warn; return (fail-open, body unchanged)
     if out.Changed           → qctx.Query.Body = out.SQL; metric(applied); log debug
     else                     → metric(noop)
2. agent.Plugin.OnQuery(qctx): SignToken(qctx.Query.Body) → inject auth token setting
3. relay: up.WriteQuery(qctx.Query) → materialized SQL forwarded upstream
```

The RNG source is `crypto/rand` (+ `google/uuid` for UUIDv4). Reproducibility is not required: replay reads the constants baked into the signed SQL and never re-runs materialization.

## 5. Components

### 5.1 `pkg/rewriter` — Materializer seam (reuse existing backend)

The grpc-dial and native-FFI-load logic for the shared `rewriter-go` Service already lives behind the unexported `backend` interface in [pkg/rewriter/backend.go](../../../pkg/rewriter/backend.go). We extend that seam rather than duplicating transport code in a new leaf package.

1. Add one method to the `backend` interface:

```go
MaterializeSQL(ctx context.Context, req *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error)
```

- `grpcBackend` delegates to `b.client.MaterializeSQL(ctx, req)`.
- native `*rewritergo.Service` already implements it (v0.3.0) — no change.
- `fakeBackend` in `backend_test.go` gains a stub for unit tests.

2. Export a thin wrapper (analogous to `sentioRewriter`, which owns per-session logic above the transport):

```go
type MaterializeOutcome struct {
    SQL     string             // SQL to use downstream: materialized on Success, original otherwise
    Changed bool               // true when replacements were applied
    Code    pb.MaterializeCode
    Message string
}

type Materializer interface {
    Materialize(ctx context.Context, sql string) (MaterializeOutcome, error)
    Close() error
}

func NewMaterializer(opts Options, poolSize int, profileID string) (Materializer, error)
```

`NewMaterializer` builds a `backend` via the existing `newBackend(opts)` dispatch (`""`/`grpc` → dial `ServiceAddr`; `native` → load FFI). The returned implementation, per call:

- builds `MaterializationInputs{ NowUnixNs: &now, RandomUint64Values, RandomFloat64Values, UuidValues }` with `poolSize` freshly generated values each;
- sets `MaterializationPolicy{ ProfileId: profileID }` (empty → engine default, `Timezone` left empty = UTC);
- calls `backend.MaterializeSQL`;
- on transport error → returns the error (caller falls open);
- on response → `Success` ⇒ `SQL = resp.SqlAfterMaterialization`, `Changed = len(resp.Replacements) > 0`; any other code ⇒ `SQL = original`, `Changed = false`; always carries `Code`/`Message` for observability.

Only the fields `newBackend` actually reads are required (`Engine`, `ServiceAddr`, `NativeLibraryPath`, `Timeout`); the rest of `Options` is irrelevant to materialization.

### 5.2 `pkg/plugins/materialize` — the plugin

Package `materialize` (no leaf-package name collision). Four files per the add-a-plugin recipe: `materialize.go`, `config.go`, `materialize_test.go`, `BUILD.bazel`.

```go
type Materializer interface {                       // local interface → tests inject a fake
    Materialize(ctx context.Context, sql string) (rewriter.MaterializeOutcome, error)
}

type Observer interface {                            // narrow metrics surface; *proxy.MetricsObserver satisfies it
    MaterializeApplied()
    MaterializeNoop()
    MaterializeNonSuccess(code string)
    MaterializeCallError()
}

type Plugin struct {
    Materializer Materializer                        // nil → plugin is a no-op
    PoolSize     int
    Observer     Observer                            // optional
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error { /* §4 flow, always fail-open */ }

var _ plugin.QueryPlugin = (*Plugin)(nil)
```

The plugin implements no marker interfaces: agent-mode outgoing sessions are neither routed nor peer-trusted, so default firing is correct. `OnQuery` never returns a rejecting error — all failure paths fall open.

### 5.3 Config — top-level `materialize` block

New block, parallel to `rewriter`, read only in agent mode:

```go
// pkg/plugins/materialize/config.go
type Config struct {
    Enabled                     bool              `json:"enabled"                        yaml:"enabled"`
    Engine                      string            `json:"engine"                         yaml:"engine"`               // "native" | "grpc"
    ServiceAddr                 string            `json:"service_addr"                   yaml:"service_addr"`          // grpc
    NativeLibraryPath           string            `json:"native_library_path"            yaml:"native_library_path"`   // native
    NativeLibraryRelease        string            `json:"native_library_release"         yaml:"native_library_release"`
    NativeLibrarySHA256         string            `json:"native_library_sha256"          yaml:"native_library_sha256"`
    NativeLibraryReleaseBaseURL string            `json:"native_library_release_base_url" yaml:"native_library_release_base_url"`
    Timeout                     cfgtypes.Duration `json:"timeout"                        yaml:"timeout"`
    RandomPoolSize              int               `json:"random_pool_size"               yaml:"random_pool_size"`      // default 16
    ProfileID                   string            `json:"profile_id"                     yaml:"profile_id"`            // "" → engine default
}
```

`Config.Validate()` (returns nil when disabled): require an explicit `engine` when enabled (`grpc` or `native` — no silent default, matching "operator picks native-go library or grpc service"); `grpc` requires `service_addr`; `random_pool_size` must be >= 0 (0 or empty → apply the 16 default at build time). The root `Config.Validate` invokes it **only when `Config.Mode()` is agent**, so a stray block in a server-mode config is ignored, not rejected. Root `Config.Default()` seeds `RandomPoolSize: 16`.

### 5.4 `build.go` wiring

In `buildAgent`, when `cfg.Materialize.Enabled`:

```go
m, err := buildMaterializer(cfg)                     // resolve FFI path/release, then rewriter.NewMaterializer
if err != nil {
    return nil, fmt.Errorf("materialize: %w", err)   // startup fail-fast (see §6)
}
queryPlugins = []plugin.QueryPlugin{
    &materialize.Plugin{Materializer: m, PoolSize: cfg.Materialize.RandomPoolSize, Observer: obs},
    &agent.Plugin{...}, metrics,
}
teardown = append(teardown, m.Close)                 // close backend on shutdown
```

`buildMaterializer` reuses the native-library resolution already present in `buildRewriterFactory` (path → release-fetch via `pkg/ffifetch`). That block is extracted into a small shared helper `resolveNativeLibraryPath(engine, path, release, sha256, baseURL) (string, error)` called by both the rewriter factory and the materializer, so the FFI download/caching/pinning path is not duplicated.

## 6. Error Handling

Two distinct policies, deliberately different:

- **Runtime: fail-open (per the chosen policy).** Any transport error or non-`Success` code leaves `qctx.Query.Body` untouched, so the query is signed and forwarded verbatim and never dropped. Each occurrence emits a warn log and a metric labelled by outcome, so operators can quantify how much traffic is bypassing materialization. This intentionally means a query whose SQL the engine cannot parse is signed **un**-materialized — acceptable for v1 because Phase-1 materialization is a hardening step layered ahead of the (not-yet-active) replay layer, not a hard admission gate.
- **Startup: fail-fast.** If `materialize.enabled` is true but the backend cannot be constructed (FFI library missing/unloadable, or the gRPC service undialable within `timeout`), `buildAgent` returns an error and the process does not start. Rationale: enabling the feature is an explicit integrity choice; a misconfiguration should surface immediately rather than silently degrade into signing non-deterministic SQL for the process lifetime. (This differs from the server-mode rewriter, which warns and disables itself on startup failure. If we later prefer parity, this is a one-line change; it is called out as an open question in §10.)

## 7. Observability

Extend `pkg/proxy/observer.go`'s `MetricsObserver` with the four `Observer` methods, backed by one counter vector:

```text
clickhouse_proxy_agent_materialize_total{result="applied|noop|non_success|call_error"[, code="SyntaxError|…"]}
```

- `applied` — replacements made, body swapped.
- `noop` — `Success`, no replacements (SQL had no materializable non-determinism).
- `non_success` — engine returned a non-`Success` code (fail-open); `code` label carries which.
- `call_error` — transport/engine call returned an error (fail-open).

Registration follows the existing `init()`-registered global pattern in `observer.go` (see the known-rough-edge about duplicate registration in tests).

## 8. Testing

- **Plugin unit tests** (`pkg/plugins/materialize`, fake `Materializer`, no gRPC/FFI): body is swapped only when `Changed`; body untouched on `noop`, `non_success`, and `call_error`; the correct `Observer` method fires in each case; nil `Materializer` is a clean no-op.
- **Materializer unit tests** (`pkg/rewriter`, extended `fakeBackend`): request carries `NowUnixNs` set and pool slices sized to `poolSize`; `Success`+replacements ⇒ `Changed` + materialized SQL; each non-`Success` code ⇒ original SQL + `Changed=false`; transport error is propagated (so the plugin falls open).
- **Native smoke test** (opt-in, gated on `POLYGLOT_SQL_FFI_PATH` like `pkg/rewriter/native_smoke_test.go`): materialize a real `now()` and assert a constant literal replaced it.
- **Config tests** (`pkg/config`): enabled without engine → error; `grpc` without `service_addr` → error; disabled block validates clean; default `RandomPoolSize` is 16.
- Baseline discipline: diff the failing-test set against a clean `main` Bazel build; the pre-existing `pkg/rewriter` external-dependency failures and the flaky lifecycle test are not regressions.

## 9. Dependencies and Two-Repo Note

- Bump `github.com/housegate/rewriter-go` from `v0.2.1` to **`v0.3.0`** in `go.mod` and `MODULE.bazel`, then `bazel mod tidy && bazel run //:gazelle`. Promote `github.com/google/uuid` from indirect to a direct dependency (used for UUIDv4 generation).
- The **native** engine is fully functional in-repo once the FFI library (v0.3.0) is resolved.
- The **gRPC** engine path is wired here, but end-to-end operation additionally requires the `sql-rewriter` service to implement the `MaterializeSQL` server RPC. That is a change in the service repository and is out of scope for this repo; until it ships, operators who select `grpc` will get `Unimplemented` at call time, which the fail-open path absorbs (logged + counted as `call_error`). This is documented so `grpc` is not assumed live.

## 10. Open Questions / Future Work

1. **Startup policy parity.** §6 chooses fail-fast at startup for the materializer while the rewriter warns-and-disables. Confirm this asymmetry is desired, or align them.
2. **Pool sizing vs. exact counts.** A fixed random/uuid pool (default 16) over-supplies the common case and fails open on the rare query that needs more. A future refinement could size inputs from a cheap pre-count, but v1 keeps the fixed pool for simplicity.
3. **Phase 2 / envelope work.** `_hg_row_id` injection and `StatementEnvelopeV2` are explicitly deferred; this feature only makes the signed SQL text deterministic.
4. **Config placement.** Top-level `materialize` (parallel to `rewriter`) was chosen over nesting under `agent`; revisit if operators find the agent-only scope confusing.
