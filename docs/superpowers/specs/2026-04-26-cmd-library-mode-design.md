# Refactor cmd entry point: standalone + library mode

**Date**: 2026-04-26
**Branch**: `refactor/cmd-library-mode`
**Status**: design approved, pending implementation plan

## Goal

Today housegate is binary-only: `cmd/main.go` parses flags, `cmd/main.go` + `cmd/serve.go` assemble every dependency, and `proxy.Server.Serve` blocks the binary's `main()`. Anything that wants to embed the proxy (another Go service, an integration test) has to fork the cmd package or shell out to the binary.

We want one codebase to serve two consumers:

1. **Standalone binary** — unchanged operator UX (CLI flags, env vars, config file, secret-* subcommands, `/metrics` HTTP, signal handling).
2. **Library** — a Go API that other services and tests import and call. The headline entry point is `housegate.New(opts) (Proxy, error)` with `Proxy` a small interface.

## Non-goals

- No behavior changes to the on-the-wire protocol or plugin chain.
- No new modes or features. The three runtime modes (`server` / `agent` / `forwarding-only`) stay exactly as they are; the library exposes all three through `cfg.Mode()` dispatch.
- No swap to functional-options style for the public API. A flat `Options` struct is enough for the cardinality we have and keeps godoc readable.
- No public API for `*cluster.PooledConn` replacement. Library users injecting a `Cluster` use `cluster.PooledConn` as the return type — the pool's connection lifetime stays internal to the cluster package.

## Design

### Package layout

```
housegate/                       (new — module root, import path "housegate/housegate")
├── proxy.go                     Proxy interface + Options + New()
├── build.go                     dep assembly (was cmd/main.go runServer/runAgent/
│                                runForwarding) + plugin chain wiring (was cmd/serve.go
│                                serveServer/serveAgent/serveForwarding)
└── redis.go                     redisFactory (moved from cmd/main.go)

cmd/                             (slimmed)
├── main.go                      flag parsing, secret-* dispatch, signal ctx,
│                                metrics HTTP server, then housegate.New(opts).Run(ctx)
├── secret.go                    unchanged
└── BUILD.bazel                  updated deps: housegate root pkg
```

The new package's import path is `housegate/housegate` (module root), so library users write:

```go
import "housegate/housegate"

p, err := housegate.New(housegate.Options{Config: cfg})
if err != nil { ... }
err = p.Run(ctx)
```

### Public API

```go
// Package housegate is the embeddable form of the ClickHouse-proxy.
//
// Standalone operators run cmd/housegate; integrators import this
// package and call New(opts).Run(ctx).
package housegate

// Proxy is a started, ready-to-Serve proxy. New returns it after every
// dependency that can fail synchronously has been resolved. Call Run or
// RunWith to begin accepting connections; cancel ctx to drain.
type Proxy interface {
    // Run binds a listener on cfg.Listen, starts the plugin chain, and
    // blocks until ctx is cancelled or the listener errors. Resource
    // teardown (cluster manager, rewriter factory, redis pool, billing
    // client) happens inside Run before it returns; callers do not need
    // to invoke a separate Close.
    Run(ctx context.Context) error

    // RunWith is identical to Run except that the caller owns the
    // listener. Useful for ":0" port-binding in tests, TLS-wrap, unix
    // sockets, etc. Run is implemented as RunWith with a freshly-bound
    // tcp listener.
    RunWith(ctx context.Context, ln net.Listener) error

    // Addr returns the bound listener address; nil before Run/RunWith
    // has bound. After binding it's stable for the lifetime of the
    // call.
    Addr() net.Addr
}

// Options configures a Proxy. Only Config is required; nil-valued
// optional fields are constructed from Config the same way the
// standalone binary does.
type Options struct {
    Config *config.Config // required

    // Optional dependency overrides. When non-nil, New uses the value
    // verbatim and does NOT build one from Config. Caller owns the
    // lifetime of any value it passes in (Close it after Proxy.Run
    // returns); when nil, the lib builds the dep and tears it down
    // when Run returns.
    NetworkState network.State
    CkhManager   ckhmanager.Manager
    Validator    auth.Validator
    Rewriter     rewriter.Factory
    CredProvider credentials.CredentialProvider
    Signer       auth.Signer            // was *auth.RelaySigner
    UsageClient  billing.UsageClient    // was *billing.Client
    Cluster      cluster.Cluster        // was *cluster.Manager

    // RedisClients is a pool of pre-dialed redis clients keyed by
    // resolved addr (post cfg.ResolveRedisAddr). New consults this map
    // before dialing; misses are dialed and added (but only the lib-
    // built ones are closed at Run end — caller-supplied entries
    // survive).
    RedisClients map[string]*redis.Client
}

// New validates Config, resolves every dependency (using Options
// overrides where provided), and returns a Proxy ready to Run.
//
// Errors here are fail-fast configuration / connectivity errors:
//   - cfg.Validate failure
//   - NetworkState dial failure
//   - ckhmanager load failure
//   - rewriter factory startup failure (when ckhMgr != nil)
//   - billing client connect failure
//   - signer key parse failure
//
// Errors from Run are listener errors and ctx cancellation only.
func New(opts Options) (Proxy, error)
```

### Three new tiny interfaces (in their own packages, not in housegate)

These are extracted from concrete types as the discussion landed. They're consumed by existing plugins, so the interface lives next to the concrete impl, not in the housegate top-level package.

```go
// pkg/auth — used by pkg/plugins/route.Signer and pkg/plugins/agent.Plugin
type Signer interface {
    Address() string
    SignToken(sql string) (string, error)
}
// existing *RelaySigner satisfies it without changes.

// pkg/billing — used by pkg/plugins/usage.Plugin
type UsageClient interface {
    CheckBalance(ctx context.Context, payer, signer string) (bool, usageProtos.CheckQueryBalanceRejection, error)
    ReportUsage(ctx context.Context, payer, signer string, amount uint64)
}
// existing *Client satisfies it. Close() stays on *Client only — it's
// the owner's responsibility, not the consumer's.

// pkg/cluster — used by housegate dialer and pkg/rewriter.SentioNetworkFactory
type Cluster interface {
    GetConnection(ctx context.Context) (*PooledConn, error)
    HasReplica(addr string) bool
}
// existing *Manager satisfies it. Start/Close stay on *Manager only —
// caller-injected Clusters must already be Started; the caller Closes
// them after Run.
```

Plugin field types switch from `*concrete` to interface:

| Field | Old | New |
|---|---|---|
| `pkg/plugins/route.Signer.Signer` | `*auth.RelaySigner` | `auth.Signer` |
| `pkg/plugins/agent.Plugin.Signer` | `*auth.RelaySigner` | `auth.Signer` |
| `pkg/plugins/usage.Plugin.Client` | `*billing.Client` | `billing.UsageClient` |
| `pkg/rewriter.SentioNetworkFactory.clusterMgr` (private) | `*cluster.Manager` | `cluster.Cluster` |
| `pkg/rewriter.(*SentioNetworkFactory).SetClusterManager` arg | `*cluster.Manager` | `cluster.Cluster` |

### Lifecycle ownership rules

For each dep listed in Options:

- If caller passed it in (non-nil): housegate **uses it as-is**, does not Close it. Caller owns lifetime; caller almost always wants to Close after `Run` returns.
- If caller left it nil: housegate **builds it from Config and tears it down** before Run returns. Teardown order is the reverse of build order, mirroring `runServer`'s defer stack today.

This rule is stated once in the `Options` godoc and applies uniformly. Tests get `housegate.New(Options{Config: cfg, NetworkState: yamlState}).Run(ctx)` and never have to think about which deps to clean up; embedding services that share a Redis pool can pass it in and continue using it after the proxy stops.

### `New` vs `Run`: who needs a ctx

`New(opts)` is **ctx-free**. Builders that don't need a long-lived context (NetworkState construct, ckhmanager load, rewriter factory dial-with-block, billing.NewClient, signer key parse, redis dial) all happen here. Their build-time errors are returned synchronously.

`cluster.Manager.Start(ctx)` is the one builder step that does need a long-lived ctx — it spawns the health-check loop. To keep the lifecycle clean ("everything stops when the run ctx is cancelled"), the design splits cluster setup into two phases:

1. `New` calls `cluster.NewManager(...)` (or `NewSingleReplicaManager`) but does **not** call `Start`.
2. `Run`/`RunWith` calls `Start(ctx)` immediately before `srv.Serve(ctx, ln)`, where `ctx` is the run-time context.
3. On `Run` return, the teardown stack calls `Close()` (only when lib-built).

This is a small departure from today's `buildClusterManager`, which calls `Start` inline. The plugin chain doesn't observe the difference — `Start` only kicks off background goroutines.

When the caller injects `Options.Cluster`, housegate assumes it's already Started and never calls Start/Close on it.

### What stays binary-only

`cmd/main.go` after the refactor is a thin shell:

```go
func main() {
    if handled, exit := secretSubcommand(); handled { os.Exit(exit) }

    cfg := loadConfigWithOverrides()              // unchanged: flag/env/file precedence
    logStartupBanner(&cfg)                        // unchanged

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()
    startMetricsServer(cfg.MetricsListen)         // unchanged: /metrics HTTP

    p, err := housegate.New(housegate.Options{Config: &cfg})
    if err != nil { log.Fatale(err, "init proxy") }
    if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
        log.Fatale(err, "proxy stopped")
    }
}
```

Everything that's purely operator-facing (signal handling, `/metrics` HTTP, `secret-*`, CLI flags) stays in `cmd`. Everything else moves into the housegate package.

### Migration of existing functions

| Old location | New location | Notes |
|---|---|---|
| `cmd/main.go: runServer` | `housegate/build.go: buildServer` (private) | Used by `New` when `cfg.Mode() == ModeServer` |
| `cmd/main.go: runAgent` | `housegate/build.go: buildAgent` (private) | |
| `cmd/main.go: runForwarding` | `housegate/build.go: buildForwarding` (private) | |
| `cmd/serve.go: serveServer / serveAgent / serveForwarding` | merged into the build* functions; the listener bind moves into `Proxy.RunWith` | One concrete `proxyImpl` struct holds the assembled `*proxy.Server` + the lifecycle cleanup func; its `RunWith` calls `srv.Serve(ctx, ln)` |
| `cmd/main.go: redisFactory` | `housegate/redis.go: redisFactory` | Stays unexported in housegate; the public Options exposes `RedisClients map[string]*redis.Client` instead |
| `cmd/main.go: loadNetworkState / loadCkhManager / buildRewriterFactory / buildClusterManager` | `housegate/build.go` | Unexported — internal building blocks |
| `cmd/main.go: dialRaw / pickRandomBoundProxy / selfListenPort / isLocalAddress` | `housegate/build.go` | Unexported — used only by the forwarding-mode builder |

### Error handling boundary (decision a.)

`New` is allowed to return errors from any synchronously-resolvable failure:
- config validate
- secretsload resolve
- NetworkState construction (yaml load OR redis dial)
- ckhmanager load
- rewriter factory construction (gRPC dial-with-block; this is what makes `New` slow today)
- billing client construct
- signer key parse
- concurrency-limit redis dial

`Run`/`RunWith` only return:
- `net.Listen` errors (Run only; RunWith trusts the supplied listener)
- `*proxy.Server.Serve` errors (which today is "ctx cancelled" or "fatal accept-loop error")

This split keeps the test ergonomics of "if `New` succeeded, `Run` will start" — tests can `require.NoError(t, err)` on `New` and then `RunWith` in a goroutine.

### Resource teardown (decision b.)

`Proxy.Run`/`RunWith` is the **single owner** of resource lifecycle. Cleanup happens after `srv.Serve` returns and before the function returns. No separate `Close()` method on the interface. This matches Go's standard "ctx-driven shutdown" idiom and keeps the interface to two methods.

Internal teardown stack (built by `New`, executed by `RunWith`):

1. Close billing.Client (if lib-built)
2. Close cluster.Manager (if lib-built)
3. Close rewriter factory (if lib-built; calls `SentioNetworkFactory.Close`)
4. Close redis pool entries (only those dialed by the lib; caller-supplied survive)

Ordering matches the existing `defer` stack in `runServer`.

### Test strategy

Two new test categories:

1. **Unit tests for the new `housegate` package** — table-driven `TestNew_DispatchByMode` covering server / agent / forwarding-only with minimal config; assert that the returned `*proxyImpl` has the expected plugin-chain shape (or smoke that it serves a single bind-and-close cycle on `:0`).

2. **Roundtrip integration test** — `housegate.New(Options{Config: minimalServerCfg, NetworkState: yamlState}).RunWith(ctx, listenZero)` then dial `Addr()` with a tiny ClickHouse client mock; verifies the lib path is wire-equivalent to the binary path.

Existing tests under `pkg/proxy`, `pkg/plugins/*`, `pkg/cluster` are unchanged.

For the three interface extractions: each plugin already has tests using fake/mock signers/clients that are *struct-typed*. Switching the field to interface lets us replace those mocks with smaller test doubles, but the existing test suite must still pass byte-identical. Verify with `bazel test //... ` before merging.

### Backward compatibility

- `cmd/housegate` binary CLI surface (flags, env vars, config schema): **unchanged**.
- Plugin construction sites in `pkg/plugins/*`: field types change, but `*auth.RelaySigner` / `*billing.Client` / `*cluster.Manager` continue to satisfy the new interfaces, so any external caller that constructs these plugin structs by hand keeps compiling.
- `*cluster.Manager.GetConnection` signature: unchanged (still returns `*PooledConn`).
- `pkg/rewriter.(*SentioNetworkFactory).SetClusterManager` parameter type widens from `*cluster.Manager` to `cluster.Cluster`. This is a source-compatible widening (existing `*cluster.Manager` arg still satisfies it) but is technically an exported-API change. Acceptable since this is a single-org repo and the call site is housegate's own builder.

## Build sequence

1. Add the three small interfaces (`auth.Signer`, `billing.UsageClient`, `cluster.Cluster`) and switch the four consumer field/parameter sites. Run `bazel test //...` — must remain green; this is a no-op refactor.
2. Create the `housegate` root package with `Proxy` / `Options` / `New` skeleton; copy-but-don't-yet-call the build helpers.
3. Move `redisFactory` and the build helpers into the housegate package. Have `cmd/main.go` call `housegate.New(...).Run(ctx)`. Delete `cmd/serve.go`. Update `cmd/BUILD.bazel`.
4. Add unit tests for `housegate.New` mode dispatch and one roundtrip integration test.
5. `bazel test //...` final pass.

Each step is independently reviewable and ship-able.

## Open questions

None at design time. Two notes for implementation:

- `pkg/rewriter` exposes `(*SentioNetworkFactory).Close()`. Make sure the housegate teardown only calls it on lib-built factories (skip when `Options.Rewriter != nil`).
- `Options.RedisClients` map: documented as keyed by *resolved* address (post `cfg.ResolveRedisAddr`). Make sure the godoc says this so callers don't pass a section-name-keyed map.
