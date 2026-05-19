# cmd standalone + library mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Branch policy:** This user prefers branches over worktrees ([memory:feedback_branch_over_worktree](../../../.claude/projects/-Users-uranuswch-Dev-housegate-housegate/memory/feedback_branch_over_worktree.md)). The work is already on branch `refactor/cmd-library-mode`. **Do NOT create a worktree** — `git switch refactor/cmd-library-mode` and execute in place.

**Goal:** Refactor housegate so the same codebase serves both the standalone binary (current operator UX, untouched) and a library API (`housegate.New(opts).Run(ctx)`) that other Go services and tests can embed.

**Architecture:** Add a new top-level package at module root (`housegate/housegate` import path, `package housegate`). It exposes a small `Proxy` interface and a flat `Options` struct. All dependency assembly + plugin chain wiring (currently spread across `cmd/main.go runServer/runAgent/runForwarding` and `cmd/serve.go serveServer/serveAgent/serveForwarding`) moves into the new package. `cmd/main.go` becomes a thin shell: flag parsing, secret-* dispatch, signal context, `/metrics` HTTP, then `housegate.New(opts).Run(ctx)`. Three small interfaces are extracted along the way (`auth.Signer`, `billing.UsageClient`, `cluster.Cluster`) so library users can inject their own implementations.

**Tech Stack:** Go 1.24, Bazel 8.5.1 (Bzlmod), gRPC, Redis (go-redis/v9), Prometheus client_golang, sentio-core/log.

**Spec:** [docs/superpowers/specs/2026-04-26-cmd-library-mode-design.md](../specs/2026-04-26-cmd-library-mode-design.md). Read it before starting — the plan assumes you know the public-API shape, lifecycle ownership rules, and migration table from the spec.

---

## Phase 1: Interface extractions (no-op refactors)

These three tasks each extract one interface and switch its consumer to use it. Each is a no-op refactor: the existing concrete types (`*auth.RelaySigner`, `*billing.Client`, `*cluster.Manager`) automatically satisfy the new interfaces, so all existing tests must continue to pass byte-for-byte. Bazel test on each commit.

### Task 1: Extract `auth.Signer` interface

**Files:**
- Create: `pkg/auth/signer.go`
- Modify: `pkg/plugins/route/signer.go:35` (field type)
- Modify: `pkg/plugins/agent/signer.go:36` (field type)
- Modify: `pkg/plugins/route/BUILD.bazel` (no dep change expected; verify after edit)

- [ ] **Step 1: Create the interface file**

Write to `pkg/auth/signer.go`:

```go
package auth

// Signer produces JWS tokens binding a query's SQL.
//
// *RelaySigner is the production implementation; library users may
// inject their own (e.g. a KMS-backed signer, or a test stub).
type Signer interface {
    // Address returns the lowercase 0x-prefixed Ethereum address of
    // this signer; the verifying side's allowlist must contain it.
    Address() string

    // SignToken returns a JWS compact-serialized token whose payload
    // binds the SQL via Keccak256.
    SignToken(sql string) (string, error)
}

// Compile-time guard: *RelaySigner must satisfy Signer.
var _ Signer = (*RelaySigner)(nil)
```

- [ ] **Step 2: Run a quick Bazel build to ensure the new file compiles**

Run: `bazel build //pkg/auth:auth`
Expected: build succeeds. Compile-time `var _ = ...` assertion guarantees `*RelaySigner` satisfies the interface.

- [ ] **Step 3: Switch `routeplugin.Signer.Signer` field to interface**

Modify `pkg/plugins/route/signer.go` line 35:

```go
type Signer struct {
    Signer   auth.Signer  // was *auth.RelaySigner
    Observer Observer
}
```

(Only the field type changes; all method calls on `p.Signer` already match the interface — `SignToken`, no other access.)

- [ ] **Step 4: Switch `agent.Plugin.Signer` field to interface**

Modify `pkg/plugins/agent/signer.go` line 36:

```go
type Plugin struct {
    Signer   auth.Signer  // was *auth.RelaySigner
    Observer Observer
}
```

- [ ] **Step 5: Run the existing tests for the two plugins to confirm zero behavior change**

Run: `bazel test //pkg/plugins/route:route_test //pkg/plugins/agent:agent_test`
Expected: PASS (same set as on `main`).

- [ ] **Step 6: Run the full plugin/proxy test suite**

Run: `bazel test //pkg/plugins/... //pkg/proxy:proxy_test //pkg/auth:auth_test`
Expected: PASS (same set as on `main`).

- [ ] **Step 7: Commit**

```bash
git add pkg/auth/signer.go pkg/plugins/route/signer.go pkg/plugins/agent/signer.go
git commit -m "$(cat <<'EOF'
refactor(auth): extract Signer interface, switch plugins to consume it

Plugins routeplugin.Signer and agent.Plugin now hold auth.Signer
instead of *auth.RelaySigner. *RelaySigner continues to satisfy it
via a compile-time assertion in pkg/auth/signer.go. No behavior
change.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Extract `billing.UsageClient` interface

**Files:**
- Create: `pkg/billing/usage_client.go`
- Modify: `pkg/plugins/usage/usage.go:30` (field type)

- [ ] **Step 1: Create the interface file**

Write to `pkg/billing/usage_client.go`:

```go
package billing

import (
    "context"

    usageProtos "sentioxyz/sentio-core/service/usage/protos"
)

// UsageClient is the consumer-facing surface of a billing client.
//
// *Client is the production implementation; library users may inject
// their own (e.g. a record-and-replay test stub). Close lives on the
// concrete type only — it is the owner's responsibility, not the
// consumer's.
type UsageClient interface {
    CheckBalance(ctx context.Context, payer, signer string) (bool, usageProtos.CheckQueryBalanceRejection, error)
    ReportUsage(ctx context.Context, payer, signer string, amount uint64)
}

// Compile-time guard: *Client must satisfy UsageClient.
var _ UsageClient = (*Client)(nil)
```

- [ ] **Step 2: Build the billing package**

Run: `bazel build //pkg/billing:billing`
Expected: build succeeds.

- [ ] **Step 3: Switch `usage.Plugin.Client` field to interface**

Modify `pkg/plugins/usage/usage.go` line 30:

```go
type Plugin struct {
    Client billing.UsageClient  // was *billing.Client
}
```

(All method calls on `p.Client` are `CheckBalance` and `ReportUsage` — both on the interface. The free function `billing.RejectionException(...)` used at line 61 is unchanged; it's a package-level function, not a method.)

- [ ] **Step 4: Run usage plugin tests**

Run: `bazel test //pkg/plugins/usage:usage_test //pkg/billing:billing_test`
Expected: PASS (same set as on `main`).

- [ ] **Step 5: Commit**

```bash
git add pkg/billing/usage_client.go pkg/plugins/usage/usage.go
git commit -m "$(cat <<'EOF'
refactor(billing): extract UsageClient interface, usage plugin consumes it

usage.Plugin now holds billing.UsageClient instead of *billing.Client.
*Client continues to satisfy it via a compile-time assertion. Close
stays on *Client only — the owner Closes, not the consumer.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Extract `cluster.Cluster` interface, widen rewriter setter

**Files:**
- Create: `pkg/cluster/cluster_iface.go`
- Modify: `pkg/rewriter/sentio.go:42` (field type) and `:95` (setter parameter type)

- [ ] **Step 1: Create the interface file**

Write to `pkg/cluster/cluster_iface.go`:

```go
package cluster

import "context"

// Cluster is the consumer-facing surface of a cluster manager.
//
// *Manager is the production implementation; library users may inject
// their own (e.g. a fake topology in integration tests). Start and
// Close stay on *Manager only — when the housegate library builds the
// manager itself, it owns Start/Close; when the caller injects a
// Cluster, the caller must hand it over already-Started and Close it
// after housegate's Run returns.
//
// GetConnection returns a *PooledConn (a concrete pkg/cluster type)
// rather than an interface; the connection-lifetime API
// (ReturnConnection / DiscardConnection / RecordQuerySuccess /
// RecordQueryError) lives on *Manager and stays internal to the
// cluster package. Custom Cluster implementations should construct
// PooledConns via cluster-package helpers if/when they're added.
type Cluster interface {
    GetConnection(ctx context.Context) (*PooledConn, error)
    HasReplica(addr string) bool
}

// Compile-time guard: *Manager must satisfy Cluster.
var _ Cluster = (*Manager)(nil)
```

- [ ] **Step 2: Build the cluster package**

Run: `bazel build //pkg/cluster:cluster`
Expected: build succeeds.

- [ ] **Step 3: Widen `SentioNetworkFactory.clusterMgr` and its setter**

Modify `pkg/rewriter/sentio.go`. At line 42 (the struct field):

```go
type SentioNetworkFactory struct {
    options            Options
    networkState       network.State
    grpcConn           *grpc.ClientConn
    grpcClient         pb.RewriterServiceClient
    tableMapperFactory SentioNetworkTableMapperFactory
    callbackAddr       string                         // resolved address for remote() callback
    clusterMgr         cluster.Cluster                // optional, for multi-replica isLocal detection
    credProvider       credentials.CredentialProvider // optional, for remote() credential replacement
}
```

At line 95 (the setter):

```go
// SetClusterManager wires a cluster manager so isLocal detection can
// account for multi-replica shards. Optional. The argument may be a
// *cluster.Manager (the production case) or any other cluster.Cluster
// implementation.
func (f *SentioNetworkFactory) SetClusterManager(m cluster.Cluster) { f.clusterMgr = m }
```

The only call site outside cmd is in housegate itself (after Phase 3); existing `cmd/main.go:294` passes a `*cluster.Manager` which satisfies the widened interface unchanged.

- [ ] **Step 4: Run rewriter tests**

Run: `bazel test //pkg/rewriter:rewriter_test`
Expected: same result as on `main` — note that the e2e test (`rewriter_e2e_test.go`) needs a gRPC service on `localhost:50051` and is expected to fail without it; per CLAUDE.md, diff against `main` to confirm we only see pre-existing failures.

- [ ] **Step 5: Run the full cluster + rewriter test surface**

Run: `bazel test //pkg/cluster:cluster_test //pkg/rewriter/...`
Expected: PASS (same set as `main`).

- [ ] **Step 6: Build the cmd binary to confirm the widened setter still accepts the existing call site**

Run: `bazel build //cmd:housegate`
Expected: build succeeds. (`cmd/main.go:294`'s `rwf.SetClusterManager(clusterMgr)` continues to compile since `*cluster.Manager` satisfies `cluster.Cluster`.)

- [ ] **Step 7: Commit**

```bash
git add pkg/cluster/cluster_iface.go pkg/rewriter/sentio.go
git commit -m "$(cat <<'EOF'
refactor(cluster): extract Cluster interface, widen rewriter setter

SentioNetworkFactory's clusterMgr field and SetClusterManager
parameter now use cluster.Cluster instead of *cluster.Manager.
*Manager continues to satisfy the interface; Start/Close stay on
the concrete type as lifecycle methods.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Phase-1 integration check

- [ ] **Step 1: Run the full test suite**

Run: `bazel test //...`
Expected: same pass/fail set as on `main` (compare with `git stash && bazel test //... ; git stash pop` if uncertain). No new failures.

- [ ] **Step 2: No commit needed** — this is a checkpoint only.

---

## Phase 2: New `housegate` root package skeleton

Add the new package, its public surface (Proxy / Options / New), and a placeholder `New` that errors with "not implemented" so the package compiles independently. Phase 3 fills in `New`'s body by moving code in from `cmd/`.

### Task 5: Add the housegate package skeleton

**Files:**
- Create: `proxy.go` (at module root, `package housegate`)
- Create: `BUILD.bazel` (at module root)
- Create: `proxy_test.go`

- [ ] **Step 1: Write a failing test for the public API surface**

Write to `proxy_test.go` (module root):

```go
package housegate

import (
    "testing"

    "housegate/housegate/pkg/config"
)

func TestNew_ErrorsBeforeImplemented(t *testing.T) {
    // Skeleton check: New(opts) returns an error path that compiles and
    // surfaces a clear message until Phase 3 wires the body. Once Phase
    // 3 lands, this test will be replaced (Task 11) with real coverage.
    _, err := New(Options{Config: &config.Config{}})
    if err == nil {
        t.Fatal("expected New to return an error in skeleton phase")
    }
}
```

- [ ] **Step 2: Verify the test fails (no New yet)**

Run: `bazel build //:housegate` (BUILD doesn't exist yet — expected to fail).
Expected: FAIL — "no BUILD file" or "package not found". This is the "test-first" baseline before we author the package.

- [ ] **Step 3: Create the public API skeleton**

Write to `proxy.go` (module root):

```go
// Package housegate is the embeddable form of the ClickHouse-proxy.
//
// Standalone operators run cmd/housegate; integrators import this
// package and call New(opts).Run(ctx). See the design spec at
// docs/superpowers/specs/2026-04-26-cmd-library-mode-design.md
// for lifecycle and ownership rules.
package housegate

import (
    "context"
    "errors"
    "net"

    "github.com/redis/go-redis/v9"

    ckhmanager "sentioxyz/sentio-core/common/clickhousemanager"

    "housegate/housegate/pkg/auth"
    "housegate/housegate/pkg/billing"
    "housegate/housegate/pkg/cluster"
    "housegate/housegate/pkg/config"
    "housegate/housegate/pkg/credentials"
    "housegate/housegate/pkg/network"
    "housegate/housegate/pkg/rewriter"
)

// Proxy is a started, ready-to-Serve proxy. Run/RunWith blocks until
// ctx is cancelled or the listener errors; resource teardown happens
// inside before the call returns. There is no separate Close.
type Proxy interface {
    // Run binds a TCP listener on Options.Config.Listen and serves
    // until ctx is cancelled.
    Run(ctx context.Context) error

    // RunWith is identical to Run but the caller owns the listener.
    // Useful for ":0" port-binding in tests, TLS-wrap, unix sockets.
    RunWith(ctx context.Context, ln net.Listener) error

    // Addr returns the bound listener address; nil before Run/RunWith
    // has bound. After binding it is stable for the lifetime of the
    // call.
    Addr() net.Addr
}

// Options configures a Proxy. Only Config is required. Optional
// dependency overrides: when non-nil, New uses the value verbatim and
// does NOT build one from Config — the caller owns its lifetime
// (Close it after Proxy.Run returns). When nil, the lib builds the
// dep and tears it down when Run returns.
type Options struct {
    Config *config.Config // required

    NetworkState network.State
    CkhManager   ckhmanager.Manager
    Validator    auth.Validator
    Rewriter     rewriter.Factory
    CredProvider credentials.CredentialProvider
    Signer       auth.Signer
    UsageClient  billing.UsageClient
    Cluster      cluster.Cluster

    // RedisClients is a pool of pre-dialed redis clients keyed by the
    // *resolved* address (post cfg.ResolveRedisAddr). New consults
    // this map before dialing; misses are dialed and added, but only
    // lib-dialed entries are closed when Run returns.
    RedisClients map[string]*redis.Client
}

// New validates Config and resolves every synchronously-resolvable
// dependency. See the spec for the full error contract.
func New(opts Options) (Proxy, error) {
    return nil, errors.New("housegate.New: not yet implemented (skeleton)")
}
```

- [ ] **Step 4: Create the BUILD.bazel for the root package**

Write to `BUILD.bazel` (module root):

```python
load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "housegate",
    srcs = ["proxy.go"],
    importpath = "housegate/housegate",
    visibility = ["//visibility:public"],
    deps = [
        "//pkg/auth",
        "//pkg/billing",
        "//pkg/cluster",
        "//pkg/config",
        "//pkg/credentials",
        "//pkg/network",
        "//pkg/rewriter",
        "@com_github_redis_go_redis_v9//:go-redis",
        "@sentio-core//common/clickhousemanager",
    ],
)

go_test(
    name = "housegate_test",
    srcs = ["proxy_test.go"],
    embed = [":housegate"],
    deps = [
        "//pkg/config",
    ],
)
```

- [ ] **Step 5: Run the skeleton test**

Run: `bazel test //:housegate_test`
Expected: PASS — `TestNew_ErrorsBeforeImplemented` confirms the not-implemented error path.

- [ ] **Step 6: Run gazelle to verify no manual BUILD edits are needed for re-generation**

Run: `bazel run //:gazelle`
Expected: no diff. (If gazelle reformats the BUILD, accept the result and `git diff BUILD.bazel` to confirm only cosmetic changes.)

- [ ] **Step 7: Commit**

```bash
git add proxy.go proxy_test.go BUILD.bazel
git commit -m "$(cat <<'EOF'
feat(housegate): add root package skeleton (Proxy/Options/New)

New is a not-implemented stub at this point; Phase 3 fills the body
by moving cmd/main.go's runServer/runAgent/runForwarding logic
and cmd/serve.go's serve* helpers into the housegate package.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase 3: Move dep assembly + plugin chain wiring into housegate

Move the meaningful logic out of `cmd/`. Three tasks: redis factory + small build helpers (Task 6), per-mode builders (Task 7), and the `proxyImpl` wiring everything (Task 8).

### Task 6: Move `redisFactory` and small build helpers into housegate

**Files:**
- Create: `redis.go` (at module root)
- Create: `build.go` (at module root) — populated incrementally; this task adds the small helpers only
- Modify: `BUILD.bazel` (add new srcs + deps)
- Modify: `cmd/main.go` — keep the existing helpers in place for now; do not delete yet (Task 8 deletes them). This task only **copies** logic.

- [ ] **Step 1: Add `redis.go`**

Write to `redis.go` (module root). This is a verbatim move of the unexported `redisFactory` type from `cmd/main.go:475-512`, plus a hook for the public `Options.RedisClients` map:

```go
package housegate

import (
    "context"
    "errors"
    "fmt"

    "sentioxyz/sentio-core/common/log"

    "github.com/redis/go-redis/v9"

    "housegate/housegate/pkg/config"
)

// redisFactory dedupes redis client construction by resolved address.
// Several config sections (network_state, usage, concurrency_limit)
// may point at the same redis host; we open one pool per resolved
// addr.
//
// libBuilt is the set of resolved addresses for which the factory
// dialed the client itself (vs. accepting one via Options.RedisClients).
// Only lib-built clients are Close()d in closeAll.
type redisFactory struct {
    cfg          *config.Config
    clients      map[string]*redis.Client
    libBuilt     map[string]bool
}

func newRedisFactory(cfg *config.Config, preDialed map[string]*redis.Client) *redisFactory {
    f := &redisFactory{
        cfg:      cfg,
        clients:  map[string]*redis.Client{},
        libBuilt: map[string]bool{},
    }
    for addr, c := range preDialed {
        f.clients[addr] = c
        // libBuilt stays false for caller-supplied entries.
    }
    return f
}

// get returns a redis client for the resolved addr. sectionAddr falls
// back to RedisDefaultAddr when blank. An empty resolved addr is an
// error.
func (f *redisFactory) get(sectionAddr string) (*redis.Client, error) {
    addr := f.cfg.ResolveRedisAddr(sectionAddr)
    if addr == "" {
        return nil, errors.New("no redis address configured (set section's redis_addr or top-level redis_default_addr)")
    }
    if c, ok := f.clients[addr]; ok {
        return c, nil
    }
    c := redis.NewClient(&redis.Options{Addr: addr})
    if err := c.Ping(context.Background()).Err(); err != nil {
        _ = c.Close()
        return nil, fmt.Errorf("redis %s: %w", addr, err)
    }
    f.clients[addr] = c
    f.libBuilt[addr] = true
    return c, nil
}

// closeAll closes only lib-built clients; caller-supplied entries are
// preserved so the caller can keep using them after Run returns.
func (f *redisFactory) closeAll() {
    for addr, c := range f.clients {
        if !f.libBuilt[addr] {
            continue
        }
        if err := c.Close(); err != nil {
            log.Warnfe(err, "redis close addr=%v", addr)
        }
    }
}
```

- [ ] **Step 2: Add `build.go` with the small build helpers**

Write to `build.go` (module root). Verbatim moves of `loadNetworkState`, `loadCkhManager`, `buildRewriterFactory` from `cmd/main.go:353-434`, plus a `buildClusterManager` that explicitly does **not** call Start (Start moves to Run; see spec):

```go
package housegate

import (
    "context"
    "fmt"

    ckhmanager "sentioxyz/sentio-core/common/clickhousemanager"
    "sentioxyz/sentio-core/common/log"
    "sentioxyz/sentio-core/network/sqlrewriter"
    "sentioxyz/sentio-core/service/processor/models"

    "housegate/housegate/pkg/cluster"
    "housegate/housegate/pkg/config"
    "housegate/housegate/pkg/network"
    "housegate/housegate/pkg/rewriter"
    "housegate/housegate/pkg/secretsload"
)

func loadNetworkState(cfg *config.Config, rf *redisFactory) (network.State, error) {
    if cfg.NetworkState.IsYAMLSource() {
        yamlState, err := network.LoadNetworkStateFromYAML(cfg.NetworkState.Source)
        if err != nil {
            return nil, fmt.Errorf("failed to load network state from YAML: %w", err)
        }
        log.Infow("network state loaded from YAML", "path", cfg.NetworkState.Source)
        return yamlState, nil
    }
    nsRedisClient, err := rf.get(cfg.NetworkState.Source)
    if err != nil {
        return nil, fmt.Errorf("network state redis: %w", err)
    }
    redisState, err := network.NewRedisNetworkState(nsRedisClient)
    if err != nil {
        return nil, fmt.Errorf("failed to initialize Redis network state: %w", err)
    }
    log.Infow("network state loaded from Redis", "addr", cfg.ResolveRedisAddr(cfg.NetworkState.Source))
    return redisState, nil
}

func loadCkhManager(cfg *config.Config) (ckhmanager.Manager, error) {
    if cfg.ForwardingOnly || cfg.CkhManagerConfigPath == "" {
        return nil, nil
    }
    resolved, err := secretsload.Resolve(cfg.CkhManagerConfigPath)
    if err != nil {
        return nil, fmt.Errorf("resolve ckh_manager_config_path: %w", err)
    }
    mgr := ckhmanager.LoadManager(resolved.Path, ckhmanager.LoadAllowEmptySharding())
    resolved.Cleanup()
    if mgr == nil {
        return nil, fmt.Errorf("failed to load ClickHouse manager from %s", cfg.CkhManagerConfigPath)
    }
    log.Infow("loaded ClickHouse manager", "path", cfg.CkhManagerConfigPath)
    return mgr, nil
}

func buildRewriterFactory(cfg *config.Config, ns network.State, ckhMgr ckhmanager.Manager) rewriter.Factory {
    if cfg.ForwardingOnly {
        log.Info("forwarding-only mode: SQL rewriter disabled")
        return nil
    }
    privateKeyHex := cfg.RelayPrivateKeyHex
    tableMapperFactory := func(ctx context.Context, processorId string,
        indexerInfo network.IndexerInfo, processorInfo network.ProcessorInfo) (rewriter.SentioNetworkTableMapper, error) {
        return sqlrewriter.NewTableMapper(privateKeyHex, processorId, 0, models.TablePatternNetworkV1, ckhMgr, indexerInfo, processorInfo)
    }
    log.Infow("using sentio-core TableMapper", "ckh_manager_config", cfg.CkhManagerConfigPath)

    rwConfig := rewriter.Options{
        Enabled:             true,
        ServiceAddr:         cfg.Rewriter.ServiceAddr,
        Upstream:            cfg.Upstream,
        Listen:              cfg.Listen,
        Timeout:             cfg.Rewriter.Timeout.Duration,
        PhysicalDatabase:    cfg.Rewriter.PhysicalDatabase,
        AuthEnabled:         cfg.Auth.Enabled,
        Delim:               cfg.Rewriter.Delimiter,
        EnableStaticMapping: cfg.Rewriter.EnableStaticMapping,
    }
    rwf, err := rewriter.NewSentioNetworkFactory(rwConfig, ns, tableMapperFactory)
    if err != nil {
        log.Warne(err, "failed to create rewriter factory, rewriting disabled")
        return nil
    }
    log.Infow("SQL rewriter enabled",
        "service_addr", cfg.Rewriter.ServiceAddr,
        "upstream", cfg.Upstream,
        "physical_database", cfg.Rewriter.PhysicalDatabase,
    )
    return rwf
}

// buildClusterManager constructs the multi-replica cluster manager
// WITHOUT calling Start. Start runs at Run-time so the health-check
// loop's lifetime is bound to the run ctx. The caller is responsible
// for Close()ing the returned manager.
func buildClusterManager(cfg *config.Config) (*cluster.Manager, error) {
    if cfg.Shard != nil {
        mgr, err := cluster.NewManager(*cfg.Shard, cfg.HealthCheck, cfg.Pool, cfg.Routing)
        if err != nil {
            return nil, fmt.Errorf("failed to create cluster manager: %w", err)
        }
        log.Infow("cluster manager built", "shard", cfg.Shard.Name, "replicas", len(cfg.Shard.Replicas))
        return mgr, nil
    }
    if cfg.Upstream != "" {
        mgr, err := cluster.NewSingleReplicaManager(cfg.Upstream)
        if err != nil {
            log.Warnfe(err, "failed to create single-replica cluster manager, falling back to direct dial upstream=%v", cfg.Upstream)
            return nil, nil
        }
        log.Infow("cluster manager built in single-replica mode", "upstream", cfg.Upstream)
        return mgr, nil
    }
    return nil, nil
}
```

- [ ] **Step 3: Update `BUILD.bazel` for the housegate package**

Modify `BUILD.bazel` (module root) to include the new srcs and the new deps:

```python
load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "housegate",
    srcs = [
        "build.go",
        "proxy.go",
        "redis.go",
    ],
    importpath = "housegate/housegate",
    visibility = ["//visibility:public"],
    deps = [
        "//pkg/auth",
        "//pkg/billing",
        "//pkg/cluster",
        "//pkg/config",
        "//pkg/credentials",
        "//pkg/network",
        "//pkg/rewriter",
        "//pkg/secretsload",
        "@com_github_redis_go_redis_v9//:go-redis",
        "@sentio-core//common/clickhousemanager",
        "@sentio-core//common/log",
        "@sentio-core//network/sqlrewriter",
        "@sentio-core//service/processor/models",
    ],
)

go_test(
    name = "housegate_test",
    srcs = ["proxy_test.go"],
    embed = [":housegate"],
    deps = [
        "//pkg/config",
    ],
)
```

- [ ] **Step 4: Build to confirm everything compiles**

Run: `bazel build //:housegate //cmd:housegate`
Expected: both succeed. cmd/main.go still has its own copies of these helpers — duplication is intentional until Task 8 deletes them.

- [ ] **Step 5: Run the existing tests to confirm no regressions**

Run: `bazel test //:housegate_test //cmd:housegate_test 2>/dev/null; bazel test //pkg/proxy:proxy_test`
Expected: PASS for `:housegate_test`; cmd has no test target so it'll be a no-op there; `proxy_test` passes (same as `main`).

- [ ] **Step 6: Commit**

```bash
git add BUILD.bazel build.go redis.go
git commit -m "$(cat <<'EOF'
feat(housegate): copy redisFactory and small build helpers into the lib

redisFactory now also accepts caller-supplied pre-dialed clients via
newRedisFactory's preDialed argument; only lib-built entries get
Close()d. buildClusterManager no longer calls Start (deferred to Run).

cmd still has its own copies — Task 8 will delete them once the lib
is the canonical caller.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Move per-mode builders into housegate

This task moves the bodies of `runServer`/`runAgent`/`runForwarding` (from `cmd/main.go`) and `serveServer`/`serveAgent`/`serveForwarding` (from `cmd/serve.go`) into housegate, **merged** into three private functions: `buildServer`/`buildAgent`/`buildForwarding`. Each returns a constructed `*proxy.Server` plus a teardown closure; it does not call `srv.Serve` (that's the next task). This produces a buildable but-not-yet-wired version.

**Files:**
- Modify: `build.go` — append the three buildXxx functions plus shared helpers (`dialRaw`, `pickRandomBoundProxy`, `selfListenPort`, `isLocalAddress`)
- Modify: `BUILD.bazel` — add deps that come along: `//pkg/proxy`, `//pkg/plugin`, `//pkg/plugins/*`, `//pkg/chproto`, `//pkg/chsession`

- [ ] **Step 1: Define the internal handoff type**

Append to `build.go`:

```go
import (
    "math/rand"
    "net"
    "strconv"
    "time"

    "github.com/redis/go-redis/v9"

    "housegate/housegate/pkg/auth"
    "housegate/housegate/pkg/billing"
    "housegate/housegate/pkg/chproto"
    "housegate/housegate/pkg/chsession"
    "housegate/housegate/pkg/credentials"
    "housegate/housegate/pkg/plugin"
    authplugin "housegate/housegate/pkg/plugins/auth"
    "housegate/housegate/pkg/plugins/concurrency"
    "housegate/housegate/pkg/plugins/credential"
    metricsplugin "housegate/housegate/pkg/plugins/metrics"
    "housegate/housegate/pkg/plugins/rewrite"
    routeplugin "housegate/housegate/pkg/plugins/route"
    "housegate/housegate/pkg/plugins/agent"
    statePlugin "housegate/housegate/pkg/plugins/state"
    "housegate/housegate/pkg/plugins/usage"
    "housegate/housegate/pkg/proxy"
)

// builtServer is what each per-mode builder returns to the proxyImpl.
// preServe is run once just before srv.Serve (e.g. cluster.Start).
// teardown runs after srv.Serve returns; it is the reverse-order
// cleanup of every lib-built dep.
type builtServer struct {
    srv      *proxy.Server
    preServe func(ctx context.Context)
    teardown func()
}
```

- [ ] **Step 2: Move `serveServer` body into `buildServer`**

Append to `build.go`. This is the merged form of `cmd/main.go runServer` + `cmd/serve.go serveServer`. The dialer no longer reads `cfg.Upstream` directly when a Cluster is present — it routes through the `cluster.Cluster` interface (which both lib-built `*Manager` and caller-injected impls satisfy). Cluster `Start` moves into `preServe`.

```go
func buildServer(opts Options, rf *redisFactory) (*builtServer, error) {
    cfg := opts.Config

    // Resolve every dep — opts override > built-from-config.
    var teardownStack []func()
    pushTeardown := func(f func()) { teardownStack = append(teardownStack, f) }

    var validator auth.Validator
    if opts.Validator != nil {
        validator = opts.Validator
    } else if cfg.Auth.Enabled {
        validator = auth.NewEthValidator(cfg.Auth.AllowedAddresses, cfg.Auth.MaxTokenAge.Duration, true, cfg.Auth.AllowNoAuth)
        log.Infow("Ethereum signature auth enabled",
            "allowed_addresses", len(cfg.Auth.AllowedAddresses),
            "allow_no_auth", cfg.Auth.AllowNoAuth)
    }

    var ns network.State
    if opts.NetworkState != nil {
        ns = opts.NetworkState
    } else {
        var err error
        ns, err = loadNetworkState(cfg, rf)
        if err != nil {
            return nil, err
        }
    }

    var ckhMgr ckhmanager.Manager
    if opts.CkhManager != nil {
        ckhMgr = opts.CkhManager
    } else {
        var err error
        ckhMgr, err = loadCkhManager(cfg)
        if err != nil {
            return nil, err
        }
    }

    var rwFactory rewriter.Factory
    if opts.Rewriter != nil {
        rwFactory = opts.Rewriter
    } else {
        rwFactory = buildRewriterFactory(cfg, ns, ckhMgr)
        if rwf, ok := rwFactory.(*rewriter.SentioNetworkFactory); ok && rwf != nil {
            pushTeardown(func() { rwf.Close() })
        }
    }

    // Cluster: lib-built path constructs and registers Close+Start;
    // caller-injected path is used as-is (no Start, no Close).
    var clusterIface cluster.Cluster
    var libCluster *cluster.Manager
    if opts.Cluster != nil {
        clusterIface = opts.Cluster
    } else {
        m, err := buildClusterManager(cfg)
        if err != nil {
            return nil, err
        }
        if m != nil {
            libCluster = m
            clusterIface = m
            pushTeardown(func() { m.Close() })
            // Wire cluster manager to rewriter factory for multi-replica
            // isLocal detection. Order matters: do this before constructing
            // any plugin that consults the factory.
            if rwf, ok := rwFactory.(*rewriter.SentioNetworkFactory); ok && rwf != nil {
                rwf.SetClusterManager(m)
            }
        }
    }
    // Wire injected Cluster into rewriter factory the same way.
    if opts.Cluster != nil {
        if rwf, ok := rwFactory.(*rewriter.SentioNetworkFactory); ok && rwf != nil {
            rwf.SetClusterManager(opts.Cluster)
        }
    }

    var credProvider credentials.CredentialProvider
    if opts.CredProvider != nil {
        credProvider = opts.CredProvider
    } else if cfg.CredentialReplaceEnabled && ckhMgr != nil {
        cp := credentials.NewCkhManagerCredentialProvider(ckhMgr, cfg.RelayPrivateKeyHex)
        credProvider = cp
        if rwf, ok := rwFactory.(*rewriter.SentioNetworkFactory); ok && rwf != nil {
            rwf.SetCredentialProvider(cp)
        }
        log.Info("credential replacement enabled via ckh_manager_config")
    }

    var relaySigner auth.Signer
    if opts.Signer != nil {
        relaySigner = opts.Signer
    } else if cfg.RelayPrivateKeyHex != "" {
        s, err := auth.NewRelaySigner(cfg.RelayPrivateKeyHex)
        if err != nil {
            return nil, fmt.Errorf("failed to create relay signer: %w", err)
        }
        relaySigner = s
        log.Infow("relay JWS signer enabled", "address", s.Address())
    }

    var usageClient billing.UsageClient
    if opts.UsageClient != nil {
        usageClient = opts.UsageClient
    } else if cfg.Usage.Enabled && cfg.Usage.SentioNodeAddr != "" {
        usageRedis, err := rf.get(cfg.Usage.RedisAddr)
        if err != nil {
            return nil, fmt.Errorf("usage redis: %w", err)
        }
        uc, err := billing.NewClient(cfg.Usage.SentioNodeAddr, usageRedis)
        if err != nil {
            log.Warne(err, "failed to create usage client, query billing disabled")
        } else {
            usageClient = uc
            pushTeardown(func() { uc.Close() })
            log.Infow("query usage reporting enabled", "sentio_node", cfg.Usage.SentioNodeAddr)
        }
    }

    var concurrencyRedis *redis.Client
    if cfg.ConcurrencyLimit.Enabled {
        var err error
        concurrencyRedis, err = rf.get(cfg.ConcurrencyLimit.RedisAddr)
        if err != nil {
            return nil, fmt.Errorf("concurrency limiter redis: %w", err)
        }
    }

    // ---- plugin chain (verbatim from cmd/serve.go serveServer) ----
    obs := proxy.NewMetricsObserver()
    metrics := metricsplugin.New(obs)

    queryPlugins := []plugin.QueryPlugin{
        &authplugin.Plugin{Validator: validator},
        &usage.Plugin{Client: usageClient},
    }
    queryCompletePlugins := []plugin.QueryCompletePlugin{}
    closePlugins := []plugin.ClosePlugin{}

    if cfg.ConcurrencyLimit.Enabled && concurrencyRedis != nil {
        lim := concurrency.NewRedisLimiter(concurrencyRedis, cfg.ConcurrencyLimit.Timeout.Duration)
        concPlugin := &concurrency.Plugin{
            Limiter:  lim,
            FailOpen: cfg.ConcurrencyLimit.FailOpen,
            Resolvers: []concurrency.DimensionResolver{
                concurrency.UserDimension(cfg.ConcurrencyLimit.PerUser),
                concurrency.NoneStakeLevelResolver(),
            },
        }
        queryPlugins = append(queryPlugins, concPlugin)
        queryCompletePlugins = append(queryCompletePlugins, concPlugin)
        closePlugins = append(closePlugins, concPlugin)
        log.Infow("concurrency limiter enabled",
            "per_user_quota", cfg.ConcurrencyLimit.PerUser,
            "timeout", cfg.ConcurrencyLimit.Timeout,
            "fail_open", cfg.ConcurrencyLimit.FailOpen,
        )
    }

    statePlug := &statePlugin.Plugin{Config: cfg.State}

    var rewritePlug *rewrite.Plugin
    if rwFactory != nil {
        var physicalDB string
        if rwf, ok := rwFactory.(*rewriter.SentioNetworkFactory); ok && rwf != nil {
            physicalDB = rwf.PhysicalDatabase()
        }
        rewritePlug = &rewrite.Plugin{
            Factory:          rwFactory,
            PhysicalDatabase: physicalDB,
            Observer:         obs,
        }
    }

    if rewritePlug != nil {
        queryPlugins = append(queryPlugins, rewritePlug)
    }
    queryPlugins = append(queryPlugins,
        &routeplugin.Signer{Signer: relaySigner, Observer: obs},
        metrics,
    )

    connLifecycle := []plugin.ConnLifecyclePlugin{metrics}
    exceptionPlugins := []plugin.ExceptionPlugin{}
    if rewritePlug != nil {
        connLifecycle = append(connLifecycle, rewritePlug)
        closePlugins = append(closePlugins, rewritePlug)
        exceptionPlugins = append(exceptionPlugins, rewritePlug)
    }
    exceptionPlugins = append(exceptionPlugins, metrics)

    helloPlugins := []plugin.HelloPlugin{
        routeplugin.Stripper{},
        &credential.Plugin{Provider: credProvider},
        statePlug,
    }
    if rewritePlug != nil {
        helloPlugins = append(helloPlugins, rewritePlug)
    }

    chain := &plugin.PluginChain{
        ConnLifecyclePlugins:     connLifecycle,
        HandshakeCompletePlugins: []plugin.HandshakeCompletePlugin{metrics},
        HelloPlugins:             helloPlugins,
        QueryPlugins:             queryPlugins,
        QueryCompletePlugins:     queryCompletePlugins,
        ClosePlugins:             closePlugins,
        ExceptionPlugins:         exceptionPlugins,
    }

    dialer := func(ctx context.Context, sess chsession.Session) (*chproto.Codec, error) {
        if target := routeplugin.RouteTarget(sess.State()); target != "" {
            return dialRaw(ctx, target, cfg.DialTimeout.Duration)
        }
        if clusterIface != nil {
            pc, err := clusterIface.GetConnection(ctx)
            if err != nil {
                return nil, fmt.Errorf("cluster GetConnection: %w", err)
            }
            return chproto.NewCodec(pc, chproto.DirToUpstream), nil
        }
        if cfg.Upstream == "" {
            return nil, fmt.Errorf("no cluster manager and no upstream configured")
        }
        return dialRaw(ctx, cfg.Upstream, cfg.DialTimeout.Duration)
    }

    srv := proxy.NewServerWithObserver(chain, dialer, obs)
    srv.ShutdownTimeout = cfg.ShutdownTimeout.Duration

    return &builtServer{
        srv: srv,
        preServe: func(ctx context.Context) {
            if libCluster != nil {
                libCluster.Start(ctx)
                log.Infow("cluster manager started", "shard", libCluster.Shard().Name)
            }
        },
        teardown: func() {
            // Reverse order, mirroring runServer's defer stack.
            for i := len(teardownStack) - 1; i >= 0; i-- {
                teardownStack[i]()
            }
        },
    }, nil
}
```

- [ ] **Step 3: Move `serveAgent` body into `buildAgent`**

Append to `build.go`:

```go
func buildAgent(opts Options) (*builtServer, error) {
    cfg := opts.Config

    var signer auth.Signer
    if opts.Signer != nil {
        signer = opts.Signer
    } else {
        s, err := auth.NewRelaySigner(cfg.Agent.PrivateKeyHex)
        if err != nil {
            return nil, fmt.Errorf("failed to create agent signer: %w", err)
        }
        signer = s
        log.Infow("agent proxy mode: signing queries",
            "address", s.Address(), "upstream", cfg.Agent.Upstream)
    }

    obs := proxy.NewMetricsObserver()
    metrics := metricsplugin.New(obs)
    chain := &plugin.PluginChain{
        ConnLifecyclePlugins:     []plugin.ConnLifecyclePlugin{metrics},
        HandshakeCompletePlugins: []plugin.HandshakeCompletePlugin{metrics},
        QueryPlugins: []plugin.QueryPlugin{
            &agent.Plugin{Signer: signer, Observer: obs},
            metrics,
        },
        ExceptionPlugins: []plugin.ExceptionPlugin{metrics},
    }

    dialer := func(ctx context.Context, _ chsession.Session) (*chproto.Codec, error) {
        return dialRaw(ctx, cfg.Agent.Upstream, cfg.DialTimeout.Duration)
    }

    srv := proxy.NewServerWithObserver(chain, dialer, obs)
    srv.ShutdownTimeout = cfg.ShutdownTimeout.Duration

    return &builtServer{srv: srv, preServe: func(context.Context) {}, teardown: func() {}}, nil
}
```

- [ ] **Step 4: Move `serveForwarding` body into `buildForwarding`**

Append to `build.go`:

```go
func buildForwarding(opts Options, rf *redisFactory) (*builtServer, error) {
    cfg := opts.Config

    var ns network.State
    if opts.NetworkState != nil {
        ns = opts.NetworkState
    } else {
        var err error
        ns, err = loadNetworkState(cfg, rf)
        if err != nil {
            return nil, err
        }
    }
    if ns == nil {
        return nil, fmt.Errorf("forwarding-only requires a non-nil NetworkState")
    }

    obs := proxy.NewMetricsObserver()
    metrics := metricsplugin.New(obs)
    chain := &plugin.PluginChain{
        ConnLifecyclePlugins:     []plugin.ConnLifecyclePlugin{metrics},
        HandshakeCompletePlugins: []plugin.HandshakeCompletePlugin{metrics},
        QueryPlugins:             []plugin.QueryPlugin{metrics},
        ExceptionPlugins:         []plugin.ExceptionPlugin{metrics},
    }

    selfPort := selfListenPort(cfg.Listen)
    const maxDialAttempts = 3
    dialer := func(ctx context.Context, _ chsession.Session) (*chproto.Codec, error) {
        var lastErr error
        for i := 0; i < maxDialAttempts; i++ {
            target, err := pickRandomBoundProxy(ns, selfPort)
            if err != nil {
                return nil, err
            }
            codec, err := dialRaw(ctx, target, cfg.DialTimeout.Duration)
            if err == nil {
                log.Infow("forwarding-only: bound peer", "target", target, "attempt", i+1)
                return codec, nil
            }
            lastErr = err
            log.Warnfe(err, "forwarding-only: dial %s failed (attempt %d/%d)", target, i+1, maxDialAttempts)
            obs.Error("dial", err)
        }
        return nil, fmt.Errorf("forwarding-only: all %d dial attempts failed: %w", maxDialAttempts, lastErr)
    }

    srv := proxy.NewServerWithObserver(chain, dialer, obs)
    srv.ShutdownTimeout = cfg.ShutdownTimeout.Duration

    return &builtServer{srv: srv, preServe: func(context.Context) {}, teardown: func() {}}, nil
}
```

- [ ] **Step 5: Add the dialer / picker helpers (verbatim from cmd/serve.go)**

Append to `build.go`:

```go
func dialRaw(ctx context.Context, addr string, timeout time.Duration) (*chproto.Codec, error) {
    d := net.Dialer{Timeout: timeout}
    conn, err := d.DialContext(ctx, "tcp", addr)
    if err != nil {
        return nil, fmt.Errorf("dial %s: %w", addr, err)
    }
    return chproto.NewCodec(conn, chproto.DirToUpstream), nil
}

func pickRandomBoundProxy(ns network.State, selfPort int) (string, error) {
    all := ns.RetrieveAllIndexerInfos()
    if len(all) == 0 {
        return "", fmt.Errorf("forwarding-only: no indexer infos in network state")
    }
    addrs := make([]string, 0, len(all))
    for _, info := range all {
        if info.ClickhouseProxyPort == 0 {
            continue
        }
        if selfPort > 0 && int(info.ClickhouseProxyPort) == selfPort && isLocalAddress(info.IndexerUrl) {
            continue
        }
        addrs = append(addrs, fmt.Sprintf("%s:%d", info.IndexerUrl, info.ClickhouseProxyPort))
    }
    if len(addrs) == 0 {
        return "", fmt.Errorf("forwarding-only: no bound proxies (all self or unbound)")
    }
    return addrs[rand.Intn(len(addrs))], nil
}

func selfListenPort(listen string) int {
    if listen == "" {
        return 0
    }
    host := listen
    if listen[0] != ':' {
        _, p, err := net.SplitHostPort(listen)
        if err != nil {
            return 0
        }
        host = ":" + p
    }
    port, err := strconv.Atoi(host[1:])
    if err != nil {
        return 0
    }
    return port
}

func isLocalAddress(host string) bool {
    if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" {
        return true
    }
    ifaces, err := net.InterfaceAddrs()
    if err != nil {
        return false
    }
    for _, a := range ifaces {
        if ipNet, ok := a.(*net.IPNet); ok && ipNet.IP.String() == host {
            return true
        }
    }
    return false
}
```

- [ ] **Step 6: Update `BUILD.bazel` — add the new package deps**

Modify `BUILD.bazel` (module root) `go_library` deps list to include all the imports added above:

```python
deps = [
    "//pkg/auth",
    "//pkg/billing",
    "//pkg/chproto",
    "//pkg/chsession",
    "//pkg/cluster",
    "//pkg/config",
    "//pkg/credentials",
    "//pkg/network",
    "//pkg/plugin",
    "//pkg/plugins/auth",
    "//pkg/plugins/concurrency",
    "//pkg/plugins/credential",
    "//pkg/plugins/metrics",
    "//pkg/plugins/rewrite",
    "//pkg/plugins/route",
    "//pkg/plugins/agent",
    "//pkg/plugins/state",
    "//pkg/plugins/usage",
    "//pkg/proxy",
    "//pkg/rewriter",
    "//pkg/secretsload",
    "@com_github_redis_go_redis_v9//:go-redis",
    "@sentio-core//common/clickhousemanager",
    "@sentio-core//common/log",
    "@sentio-core//network/sqlrewriter",
    "@sentio-core//service/processor/models",
],
```

- [ ] **Step 7: Run gazelle to keep BUILD canonical**

Run: `bazel run //:gazelle`
Expected: gazelle may reorder deps alphabetically — accept the result.

- [ ] **Step 8: Build to confirm everything compiles**

Run: `bazel build //:housegate //cmd:housegate`
Expected: both succeed. cmd is still using its own copies; the new code in `housegate` is built but not yet called.

- [ ] **Step 9: Commit**

```bash
git add build.go BUILD.bazel
git commit -m "$(cat <<'EOF'
feat(housegate): port per-mode builders into the lib (buildServer/Agent/Forwarding)

Mirrors cmd/main.go run* and cmd/serve.go serve* but: (1) takes deps
from Options when present, (2) constructs cluster.Manager without
calling Start (Start moves to Run), (3) returns a builtServer carrying
the *proxy.Server plus preServe and teardown closures.

cmd is still wired to its own copies; Task 8 wires New() to dispatch
into these builders.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Implement `New` dispatch and `proxyImpl`; delete cmd duplicates

This task makes `housegate.New` actually do something, deletes the now-duplicated `cmd/serve.go` + most of `cmd/main.go`, and reduces the cmd binary to a thin shell.

**Files:**
- Modify: `proxy.go` — implement `New` body and `proxyImpl`
- Delete: `cmd/serve.go`
- Modify: `cmd/main.go` — delete the moved helpers, switch `main()` to call `housegate.New`
- Modify: `cmd/BUILD.bazel` — drop deps that were only used by deleted code, add `//:housegate`

- [ ] **Step 1: Implement `proxyImpl` and `New` in `proxy.go`**

Replace the placeholder `New` and append `proxyImpl`. Final state of the relevant portion of `proxy.go`:

```go
func New(opts Options) (Proxy, error) {
    if opts.Config == nil {
        return nil, errors.New("housegate.New: Options.Config is required")
    }
    if err := opts.Config.Validate(); err != nil {
        return nil, fmt.Errorf("config validate: %w", err)
    }

    rf := newRedisFactory(opts.Config, opts.RedisClients)

    var built *builtServer
    var err error
    switch opts.Config.Mode() {
    case config.ModeAgent:
        built, err = buildAgent(opts)
    case config.ModeForwardingOnly:
        opts.Config.ForwardingOnly = true
        built, err = buildForwarding(opts, rf)
    default: // ModeServer
        built, err = buildServer(opts, rf)
    }
    if err != nil {
        rf.closeAll()
        return nil, err
    }

    return &proxyImpl{
        cfg:      opts.Config,
        built:    built,
        redisFac: rf,
    }, nil
}

type proxyImpl struct {
    cfg      *config.Config
    built    *builtServer
    redisFac *redisFactory

    addrMu sync.Mutex
    addr   net.Addr
}

func (p *proxyImpl) Run(ctx context.Context) error {
    ln, err := net.Listen("tcp", p.cfg.Listen)
    if err != nil {
        p.teardown()
        return fmt.Errorf("listen %s: %w", p.cfg.Listen, err)
    }
    return p.RunWith(ctx, ln)
}

func (p *proxyImpl) RunWith(ctx context.Context, ln net.Listener) error {
    p.addrMu.Lock()
    p.addr = ln.Addr()
    p.addrMu.Unlock()

    p.built.preServe(ctx)
    defer p.teardown()

    log.Infow("housegate listening", "addr", ln.Addr().String(), "mode", p.cfg.Mode())
    return p.built.srv.Serve(ctx, ln)
}

func (p *proxyImpl) Addr() net.Addr {
    p.addrMu.Lock()
    defer p.addrMu.Unlock()
    return p.addr
}

func (p *proxyImpl) teardown() {
    p.built.teardown()
    p.redisFac.closeAll()
}
```

Add the new imports at the top of `proxy.go`:

```go
import (
    "context"
    "errors"
    "fmt"
    "net"
    "sync"

    "sentioxyz/sentio-core/common/log"
    ckhmanager "sentioxyz/sentio-core/common/clickhousemanager"

    "github.com/redis/go-redis/v9"

    "housegate/housegate/pkg/auth"
    "housegate/housegate/pkg/billing"
    "housegate/housegate/pkg/cluster"
    "housegate/housegate/pkg/config"
    "housegate/housegate/pkg/credentials"
    "housegate/housegate/pkg/network"
    "housegate/housegate/pkg/rewriter"
)
```

- [ ] **Step 2: Build the housegate package**

Run: `bazel build //:housegate`
Expected: succeeds. (gazelle may need a re-run to pick up the new `sync` and log imports — `bazel run //:gazelle`.)

- [ ] **Step 3: Run the housegate skeleton test**

Run: `bazel test //:housegate_test`
Expected: PASS — but the previous test was checking for "skeleton not implemented"; now `New` errors on missing config or proceeds. Update the test in this step:

Replace `proxy_test.go`:

```go
package housegate

import (
    "testing"
)

func TestNew_RequiresConfig(t *testing.T) {
    _, err := New(Options{})
    if err == nil {
        t.Fatal("expected New(Options{}) to error on missing Config")
    }
}
```

(Comprehensive mode-dispatch tests come in Task 9.)

Run: `bazel test //:housegate_test`
Expected: PASS.

- [ ] **Step 4: Slim `cmd/main.go` to a shell**

Replace `cmd/main.go` with the slim version below. Note: `loadConfigWithOverrides`, `logStartupBanner`, `startMetricsServer`, and the `secret-*` dispatch (in `secret.go`) stay; everything else goes:

```go
// Package main is the housegate ClickHouse-proxy standalone binary.
//
// Library callers should import "housegate/housegate" instead and
// call housegate.New(opts).Run(ctx) — see proxy.go.
package main

import (
    "context"
    "errors"
    "flag"
    "net/http"
    "os"
    "os/signal"
    "syscall"

    "sentioxyz/sentio-core/common/flags"
    "sentioxyz/sentio-core/common/log"

    "github.com/prometheus/client_golang/prometheus/promhttp"

    "housegate/housegate"
    "housegate/housegate/pkg/config"
    "housegate/housegate/pkg/secretsload"
)

func main() {
    if handled, exit := secretSubcommand(); handled {
        os.Exit(exit)
    }

    cfg := loadConfigWithOverrides()
    logStartupBanner(&cfg)

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    startMetricsServer(cfg.MetricsListen)

    p, err := housegate.New(housegate.Options{Config: &cfg})
    if err != nil {
        log.Fatale(err, "init housegate")
    }
    if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
        log.Fatale(err, "housegate stopped")
    }
}

// loadConfigWithOverrides parses CLI flags and applies them on top of
// the file/env config. Override precedence: CLI flag > env var >
// config file > built-in default.
func loadConfigWithOverrides() config.Config {
    configPath := flag.String("config", config.EnvOrDefault("CK_CONFIG", ""), "path to JSON config file (optional)")

    agentMode := flag.Bool("agent", false, "enable agent mode (token-signing pass-through proxy)")
    agentUpstream := flag.String("agent-upstream", "", "server-side proxy address, e.g. 10.0.0.8:9001 (required in agent mode)")
    agentKey := flag.String("agent-key", "", "agent Ethereum private key hex for JWS signing (prefer env var CK_AGENT_KEY)")

    listenAddr := flag.String("listen", "", "proxy listen address, e.g. :9001 (overrides config/env)")
    metricsAddr := flag.String("metrics-listen", "", "Prometheus metrics listen address, e.g. :9091 (overrides config/env)")
    dialTimeout := flag.String("dial-timeout", "", "upstream dial timeout, e.g. 5s (overrides config/env)")
    idleTimeout := flag.String("idle-timeout", "", "connection idle timeout, e.g. 5m (overrides config/env)")
    logQueries := flag.Bool("log-queries", true, "log SQL query content")

    flags.ParseAndInitLogFlag()

    explicitFlags := make(map[string]bool)
    flag.Visit(func(f *flag.Flag) { explicitFlags[f.Name] = true })

    cfgPath := *configPath
    cfgCleanup := func() {}
    if cfgPath != "" {
        resolved, err := secretsload.Resolve(cfgPath)
        if err != nil {
            log.Fatale(err, "resolve config file")
        }
        cfgPath = resolved.Path
        cfgCleanup = resolved.Cleanup
    }
    cfg := config.Load(cfgPath)
    cfgCleanup()

    if explicitFlags["agent"] {
        cfg.Agent.Mode = *agentMode
    }
    if explicitFlags["agent-upstream"] {
        cfg.Agent.Upstream = *agentUpstream
    }
    if explicitFlags["agent-key"] {
        cfg.Agent.PrivateKeyHex = *agentKey
    }
    if explicitFlags["listen"] {
        cfg.Listen = *listenAddr
    }
    if explicitFlags["metrics-listen"] {
        cfg.MetricsListen = *metricsAddr
    }
    if explicitFlags["dial-timeout"] {
        var d config.Duration
        if err := d.UnmarshalText([]byte(*dialTimeout)); err != nil {
            log.Fatale(err, "invalid -dial-timeout")
        }
        cfg.DialTimeout = d
    }
    if explicitFlags["idle-timeout"] {
        var d config.Duration
        if err := d.UnmarshalText([]byte(*idleTimeout)); err != nil {
            log.Fatale(err, "invalid -idle-timeout")
        }
        cfg.IdleTimeout = d
    }
    if explicitFlags["log-queries"] {
        cfg.Logging.Queries = *logQueries
    }

    if err := cfg.Validate(); err != nil {
        log.Fatale(err, "config validation failed")
    }
    return cfg
}

func logStartupBanner(cfg *config.Config) {
    log.Infow("housegate starting",
        "mode", cfg.Mode(), "listen", cfg.Listen, "upstream", cfg.Upstream,
        "dial_timeout", cfg.DialTimeout, "idle_timeout", cfg.IdleTimeout,
        "stats_interval", cfg.StatsInterval,
        "log_queries", cfg.Logging.Queries, "log_data", cfg.Logging.Data,
        "auth_enabled", cfg.Auth.Enabled,
    )
    if cfg.Mode() == config.ModeForwardingOnly {
        log.Info("forwarding-only mode: no upstream/shard configured, requests will be forwarded to bound proxies via NetworkState")
    }
    if cfg.Shard != nil && cfg.Upstream != "" {
        log.Warn("both 'shard' and 'upstream' configured; 'shard' takes priority, 'upstream' will be ignored for routing")
    }
}

func startMetricsServer(addr string) {
    go func() {
        defer func() {
            if r := recover(); r != nil {
                log.Errorw("metrics server panic recovered", "panic", r)
            }
        }()
        log.Infow("metrics listening", "addr", addr)
        if err := http.ListenAndServe(addr, promhttp.Handler()); err != nil {
            log.Infoe(err, "metrics server error")
        }
    }()
}
```

- [ ] **Step 5: Delete `cmd/serve.go`**

Run: `git rm cmd/serve.go`

- [ ] **Step 6: Update `cmd/BUILD.bazel`**

Replace `cmd/BUILD.bazel`:

```python
load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "housegate_lib",
    srcs = [
        "main.go",
        "secret.go",
    ],
    data = [
        "//configs:exported_configs_example",
    ],
    importpath = "housegate/housegate/cmd",
    visibility = ["//visibility:private"],
    deps = [
        "//:housegate",
        "//pkg/config",
        "//pkg/secretsload",
        "@com_github_prometheus_client_golang//prometheus/promhttp",
        "@sentio-core//common/flags",
        "@sentio-core//common/log",
    ],
)

go_binary(
    name = "housegate",
    embed = [":housegate_lib"],
    visibility = ["//visibility:public"],
)
```

- [ ] **Step 7: Run gazelle then build**

Run: `bazel run //:gazelle && bazel build //... `
Expected: full module build succeeds.

- [ ] **Step 8: Run the full test suite**

Run: `bazel test //...`
Expected: same pass/fail set as on `main`. Compare against `main` baseline if uncertain.

- [ ] **Step 9: Smoke test the binary still starts**

Run (from repo root):

```bash
bazel build //cmd:housegate && \
  ./bazel-bin/cmd/housegate_/housegate -config /dev/null -listen :0 -metrics-listen :0 2>&1 | head -5 || true
```

Expected: the binary runs through config loading and either starts or fails with a clean config-related error (the empty `/dev/null` config is invalid). The point is to verify no panic / no missing import path. Hit Ctrl-C if it starts listening.

- [ ] **Step 10: Commit**

```bash
git add proxy.go proxy_test.go cmd/main.go cmd/BUILD.bazel BUILD.bazel
git rm cmd/serve.go
git commit -m "$(cat <<'EOF'
feat(housegate): wire New() to dispatch by mode; slim cmd/ to a shell

cmd/main.go drops to flag parsing + secret-* + signal ctx + metrics
HTTP + housegate.New(opts).Run(ctx). cmd/serve.go is deleted; all
plugin-chain wiring lives in housegate/build.go now. cmd/BUILD.bazel
loses every plugin/proxy dep (they're transitive via //:housegate).

Behavior is unchanged: standalone binary CLI/env/file precedence is
preserved verbatim, and cluster.Manager.Start now happens at Run-time
inside the lib (was inline in buildClusterManager before).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase 4: Tests for the new public API

### Task 9: Unit tests for `New` mode dispatch

**Files:**
- Modify: `proxy_test.go` — add table-driven mode dispatch tests
- Modify: `BUILD.bazel` — `go_test` may need additional deps (gazelle handles it)

- [ ] **Step 1: Write failing mode-dispatch tests**

Replace `proxy_test.go`:

```go
package housegate

import (
    "context"
    "net"
    "strings"
    "testing"
    "time"

    "housegate/housegate/pkg/config"
    "housegate/housegate/pkg/network"
)

// minimalAgentConfig returns a config that satisfies cfg.Validate
// for agent mode. The signing key is a deterministic test key.
func minimalAgentConfig(t *testing.T) *config.Config {
    t.Helper()
    cfg := config.Default()
    cfg.Listen = "127.0.0.1:0"
    cfg.MetricsListen = "127.0.0.1:0"
    cfg.Agent.Mode = true
    cfg.Agent.Upstream = "127.0.0.1:1" // we won't dial it
    cfg.Agent.PrivateKeyHex = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    return &cfg
}

// minimalForwardingConfig returns a config that satisfies cfg.Validate
// for forwarding-only mode. NetworkState is supplied via opts so we
// don't need redis or a yaml file.
func minimalForwardingConfig(t *testing.T) *config.Config {
    t.Helper()
    cfg := config.Default()
    cfg.Listen = "127.0.0.1:0"
    cfg.MetricsListen = "127.0.0.1:0"
    // Empty upstream + no shard + not agent = forwarding-only
    return &cfg
}

func TestNew_RequiresConfig(t *testing.T) {
    if _, err := New(Options{}); err == nil {
        t.Fatal("expected New(Options{}) to error on missing Config")
    }
}

func TestNew_AgentMode(t *testing.T) {
    p, err := New(Options{Config: minimalAgentConfig(t)})
    if err != nil {
        t.Fatalf("New: %v", err)
    }
    if p == nil {
        t.Fatal("New returned nil Proxy")
    }
}

func TestNew_ForwardingMode_RequiresNetworkState(t *testing.T) {
    cfg := minimalForwardingConfig(t)
    cfg.NetworkState.Source = "" // force the missing-NetworkState error path
    _, err := New(Options{Config: cfg})
    if err == nil {
        t.Fatal("expected forwarding-only New to error without NetworkState")
    }
    if !strings.Contains(err.Error(), "redis") && !strings.Contains(err.Error(), "NetworkState") && !strings.Contains(err.Error(), "network") {
        t.Fatalf("unexpected error: %v", err)
    }
}

func TestNew_ForwardingMode_AcceptsInjectedNetworkState(t *testing.T) {
    cfg := minimalForwardingConfig(t)
    p, err := New(Options{
        Config:       cfg,
        NetworkState: network.NewInMemoryNetworkState(),
    })
    if err != nil {
        t.Fatalf("New: %v", err)
    }
    if p == nil {
        t.Fatal("New returned nil Proxy")
    }
}

// TestRunWith_BindAndCancel proves the round-trip: New → RunWith on a
// :0 listener, cancel ctx, RunWith returns. Agent mode is the
// simplest mode to exercise here because it has no external deps.
func TestRunWith_BindAndCancel(t *testing.T) {
    p, err := New(Options{Config: minimalAgentConfig(t)})
    if err != nil {
        t.Fatalf("New: %v", err)
    }

    ln, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        t.Fatalf("Listen: %v", err)
    }
    defer ln.Close()

    ctx, cancel := context.WithCancel(context.Background())
    done := make(chan error, 1)
    go func() { done <- p.RunWith(ctx, ln) }()

    // Give the server a moment to bind, then assert Addr is populated.
    time.Sleep(50 * time.Millisecond)
    if got := p.Addr(); got == nil {
        t.Fatal("Addr() == nil after RunWith started")
    }

    cancel()
    select {
    case err := <-done:
        if err != nil && !strings.Contains(err.Error(), "context canceled") {
            t.Fatalf("RunWith returned unexpected error: %v", err)
        }
    case <-time.After(5 * time.Second):
        t.Fatal("RunWith did not return after ctx cancel")
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `bazel run //:gazelle && bazel test //:housegate_test`
Expected: PASS. `config.Default()` is defined in [pkg/config/config.go:222](../../../pkg/config/config.go) and `network.NewInMemoryNetworkState()` is defined in [pkg/network/inmemory.go:28](../../../pkg/network/inmemory.go) — the test uses them directly. If a test fails because of an actual logic bug in the lib (not an unknown symbol), fix it inline before continuing.

- [ ] **Step 3: Run gazelle and the full test suite**

Run: `bazel run //:gazelle && bazel test //...`
Expected: PASS for the new test target, no regressions elsewhere.

- [ ] **Step 4: Commit**

```bash
git add proxy_test.go BUILD.bazel
git commit -m "$(cat <<'EOF'
test(housegate): mode dispatch + RunWith bind-and-cancel coverage

Covers the New() error contract (Config required, NetworkState
injection works, forwarding-only without NetworkState fails fast),
agent happy path, and a full round-trip with RunWith on a :0
listener.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase 5: Final verification

### Task 10: Full Bazel test pass and PR readiness

- [ ] **Step 1: Run gazelle one more time to canonicalize all BUILDs**

Run: `bazel run //:gazelle`
Expected: no diff (or only cosmetic dep reorderings — accept them).

- [ ] **Step 2: Run the full test suite**

Run: `bazel test //...`
Expected: same pass/fail set as on `main`. CLAUDE.md notes that `pkg/rewriter:rewriter_e2e_test.go` and possibly other targets fail without external services — diff against the `main` baseline if uncertain. No NEW failures.

- [ ] **Step 3: Inspect the diff**

Run: `git diff --stat main && git log --oneline main..HEAD`
Expected: a clean, reviewable series of ~7-9 commits; the diff should show:
- New files: `proxy.go`, `redis.go`, `build.go`, `proxy_test.go`, `BUILD.bazel`, `pkg/auth/signer.go`, `pkg/billing/usage_client.go`, `pkg/cluster/cluster_iface.go`
- Modified files: `pkg/plugins/route/signer.go`, `pkg/plugins/agent/signer.go`, `pkg/plugins/usage/usage.go`, `pkg/rewriter/sentio.go`, `cmd/main.go`, `cmd/BUILD.bazel`
- Deleted: `cmd/serve.go`
- Untouched: every other plugin, every codec, every test besides our new one.

- [ ] **Step 4: Verify the standalone binary still starts cleanly with the example config**

Run:

```bash
bazel build //cmd:housegate
./bazel-bin/cmd/housegate_/housegate -config configs/local.network_state.yaml -listen :0 -metrics-listen :0 &
PID=$!
sleep 1
kill -TERM $PID
wait $PID 2>/dev/null
```

Expected: the proxy starts, logs "housegate listening", and exits cleanly on SIGTERM. (If `configs/local.network_state.yaml` requires fields not provided, fall back to whatever minimal config the README references.)

- [ ] **Step 5: No commit needed** — this is the final checkpoint.

---

## Notes for the executor

- **Do not deviate from spec without flagging it.** If a step's code doesn't compile because a real symbol differs from what the plan shows (e.g. `config.Default()` vs `config.NewDefault()`), pick the real symbol and proceed; don't redesign the API.
- **Each task ends with a commit.** The plan is structured so a code reviewer can read the series commit-by-commit and follow the refactor's logic.
- **gazelle is run after most tasks.** If gazelle reorders BUILD deps alphabetically, accept the change — it's authoritative.
- **The `pkg/rewriter:rewriter_e2e_test.go` failure is pre-existing** (needs a gRPC service on `localhost:50051`); per CLAUDE.md, this is not a regression. Confirm any test failures match `main`'s baseline before declaring a problem.
