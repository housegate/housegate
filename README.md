# House Gate

A lightweight ClickHouse native TCP protocol proxy. Sits transparently between clients and ClickHouse, providing **query auditing**, **JWS authentication**, **SQL rewriting**, **dynamic upstream routing**, and **Prometheus monitoring**.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Build](#build)
- **Part 1 — Configuration**
  - [Config File](#config-file)
  - [Full Parameter Reference](#full-parameter-reference)
- **Part 2 — Running House Gate**
  - [1. Standalone Mode](#1-standalone-mode)
    - [1.1 Relay Mode](#11-relay-mode)
    - [1.2 Agent Mode](#12-agent-mode)
    - [1.3 Router-Only Server](#13-router-only-server)
  - [2. Library Mode](#2-library-mode)
- **Part 3 — How Multi-Proxy Routing Works**
  - [Topology overview](#topology-overview)
  - [Two listeners per server (external / internal)](#two-listeners-per-server-external--internal)
  - [Forward-decision plugin](#forward-decision-plugin)
  - [Agent auto-discovery](#agent-auto-discovery)
  - [Cross-shard `remote()` envelopes](#cross-shard-remote-envelopes)

---

## Prerequisites

| Dependency | Version | Notes |
|-----------|---------|-------|
| Bazel | 8.5.1 | Required — Bzlmod-based build |
| Docker | 20.10+ | Optional, for containerized deploy |

> **Bazel-only.** Don't use `go build` / `go test` / `go install`. The vendored protobuf depends on a runtime version pinned by Bazel; the stock toolchain resolves a different `google.golang.org/protobuf` and panics at `init()`.

## Quick Start

```bash
# Build
bazel build //cmd:housegate

# Run (env vars cover the simplest case — no config file)
HOUSEGATE_LISTEN=":9001" HOUSEGATE_UPSTREAM="localhost:9000" \
  bazel-bin/cmd/housegate_/housegate
```

For anything beyond a single-upstream smoke test (auth, SQL rewriting, shard routing, agent mode), use a config file — see [Part 1 — Configuration](#config-file).

## Build

```bash
bazel build //cmd:housegate                                    # binary
bazel test //...                                               # all tests
bazel test //pkg/proxy:proxy_test --test_filter='Validate'     # filter
make build / make test                                         # delegate to Bazel
bazel mod tidy && bazel run //:gazelle                         # after .proto / go.mod edits
```

Output binary: `bazel-bin/cmd/housegate_/housegate`. Or `bazel run //cmd:housegate -- -config config.json`.

> **Why `go build` doesn't work:** the vendored gRPC/protobuf code assumes a specific `google.golang.org/protobuf` runtime. Bazel pins the right one; `go build` picks a different version from `go.mod` and `protos/rewriter.pb.go`'s `init()` panics with a version-skew message.

---

# Part 1 — Configuration

## Config File

JSON or YAML, selected by extension (`.yaml` / `.yml` → YAML; everything else → JSON). Lookup order:

1. CLI flag `-config /path/to/config.{json,yaml}`
2. `HOUSEGATE_CONFIG` environment variable
3. `config.json` in the current working directory (auto-detected)
4. Built-in defaults + `HOUSEGATE_*` env overrides

Files may be **age-encrypted** — proxy decrypts in memory (Linux: `memfd`, never on disk). The binary ships `secret-keygen` / `secret-encrypt` / `secret-decrypt` / `secret-edit` subcommands. See [docs/secrets.md](docs/secrets.md).

```bash
# Generate identity, encrypt config, run with ciphertext
bazel-bin/cmd/housegate_/housegate secret-keygen > ~/.housegate.age
HOUSEGATE_AGE_IDENTITY_FILE=~/.housegate.age \
  bazel-bin/cmd/housegate_/housegate secret-encrypt config.json config.json.age
HOUSEGATE_AGE_IDENTITY_FILE=~/.housegate.age \
  bazel-bin/cmd/housegate_/housegate -config config.json.age
```

> **Hierarchical keys.** Each plugin owns a section: `auth`, `rewriter`, `agent`, `usage`, `concurrency_limit`, `state`, `logging`, `network_state`. Old flat keys (`auth_enabled`, `log_queries`, `agent_mode`, `network_state_redis`, the `dbrewriter` block, …) are no longer recognized.

> **Duration format:** all `duration` keys accept human-readable strings (`"5s"`, `"1m"`, `"24h"`) or raw nanosecond integers. Values below 1s emit a warning — operators often confuse seconds vs. nanoseconds.

## Full Parameter Reference

### Top-Level — Network & Lifecycle

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `listen` | string | Yes | `:9001` | TCP listen address for the native ClickHouse protocol (the **external port** — agents and local CH `remote()` loopbacks land here) |
| `internal_listen` | string | No | `` (empty) | Optional second TCP listener restricted to peer housegates. Sessions accepted here are pre-flagged `IsPeerTrusted=true` + `IsInternalPort=true`; auth and rewrite are skipped, `__route__` envelopes are rejected. Bind to a peer-only subnet via firewall/SG. See [Part 3](#part-3--how-multi-proxy-routing-works). |
| `upstream` | string | No | `` (empty) | Single-upstream address. Ignored when `shard` is set. Empty + no `shard` + not agent ⇒ router-only server (every session is forwarded to a peer via NetworkState — see [§1.3](#13-router-only-server)). |
| `metrics_listen` | string | No | `:9091` | Prometheus metrics HTTP address. Serves `/metrics` (the proxy's own counters merged with the [`observability`](#observability) collector series) and, when enabled, an authenticated `/debug/pprof`. |
| `dial_timeout` | duration | No | `5s` | Upstream dial timeout |
| `idle_timeout` | duration | No | `5m` | Idle client connection timeout |
| `max_connection_lifetime` | duration | No | `24h` | Hard cap on a client connection's lifetime |
| `shutdown_timeout` | duration | No | `30s` | Graceful-drain budget on SIGINT/SIGTERM |
| `stats_interval` | duration | No | `10s` | Periodic packet-stat log interval |
| `streaming_buf_size` | int | No | `131072` | Streaming protocol parser bufio size (bytes) |
| `validate_checksum` | bool | No | `false` | Validate CityHash128 checksums on compressed Data blocks |
| `log_level` | string | No | `info` | Default level for housegate's own log output. Accepts `debug`/`info`/`warn`/`error`/`fatal` (case-insensitive) and slog's offset syntax (`DEBUG+1`). CLI override: `-log-level`; env: `HOUSEGATE_LOG_LEVEL`. |
| `log_file` | string | No | `` (stderr) | Redirect housegate's own log output to this file (`O_APPEND \| O_CREATE`, ANSI off). Rotation is external (`logrotate`). CLI override: `-log-file`; env: `HOUSEGATE_LOG_FILE`. |

### Top-Level — Cross-Cutting Credentials

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `relay_private_key_hex` | string | No | `` | Ethereum private key used to sign proxy-to-proxy (`__route__`) JWS tokens. All relays in a cluster should share this key, and its address must be in `auth.allowed_addresses`. |
| `ckh_manager_config_path` | string | Yes (relay) | `` | Legacy ClickHouseManager config path; the SQL rewriter needs it to resolve table mappings |
| `credential_replace_enabled` | bool | No | `true` | Replace client-supplied ClickHouse credentials with operator-managed ones before forwarding upstream |
| `redis_default_addr` | string | No | `` | Fallback Redis address used whenever a feature section leaves its own `redis_addr` blank |

### `auth` — JWS / Ethereum Signature

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `auth.enabled` | bool | No | `false` | Enable JWS verification on every query |
| `auth.allowed_addresses` | []string | No | `[]` | Lowercase `0x…` addresses whose signatures are accepted. Empty = any authenticated signer allowed |
| `auth.max_token_age` | duration | No | `1m` | Maximum age of the JWS `iat` claim |
| `auth.allow_no_auth` | bool | No | `false` | Let unsigned queries through (for soft rollouts) |

### `rewriter` — External SQL Rewriter gRPC Service

The rewriter is the canonical owner of physical/logical database mapping. Every SQL statement on a connection passes through it. Highlights:

- **Two-phase Rewrite.** Phase 1 calls the gRPC service with empty options to get back the AST-parsed accessed table names. Phase 2 builds `RewriteTableForSelectStmtArgs` (sentio-network table-name resolution via `SentioNetworkTableMapper`) and `RewriteTableForDynamicArgs` (auth-filtered `database_map`, plus `remote_upstreams` for logicals bound to other indexers) and re-calls the rewriter.
- **Permission-aware `database_map`.** Only databases the connection's account has read/write/admin permission on appear; tables in inaccessible databases are not addressable.
- **Fail-open.** A gRPC error or `UnsupportedStatement` falls back to the original SQL with a debug log; rewriter flakiness never blocks queries.
- **Error reverse-mapping.** When upstream returns an `Exception` referring to rewritten table/database names, the same per-connection Rewriter re-maps the message back via `RewriteErrorMessage`.
- **wire-level `hello.Database` rewrite.** `OnHello` substitutes `hello.Database` with `rewriter.physical_database`; the user-typed value is preserved in `SessionState.LogicalDatabase`.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `rewriter.service_addr` | string | No | `localhost:50051` | `sql-rewriter` gRPC address |
| `rewriter.timeout` | duration | No | `5s` | Per-call gRPC timeout |
| `rewriter.physical_database` | string | No | `` | The single physical ClickHouse database that hosts every logical database in this deployment. Empty disables both `database_map` and the `hello.Database` substitution |
| `rewriter.delimiter` | string | No | `_` | Separator inserted between `<logical>` and `<original_table>` |

### `agent` — Agent-Mode Settings

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `agent.mode` | bool | No | `false` | Enable agent mode |
| `agent.upstream` | string | Conditional | `` | Pinned server-side proxy address (e.g. `10.0.0.8:9001`). **Either** this **or** a top-level `network_state.source` must be set. When unset, agent auto-discovers a server-mode peer per session via NetworkState (see [§Agent auto-discovery](#agent-auto-discovery)). |
| `agent.private_key_hex` | string | Yes (agent) | `` | Agent's Ethereum private key for JWS signing. Prefer `HOUSEGATE_AGENT_KEY` env or an age-encrypted config over a plaintext file. |

### `usage` — Billing / Usage Reporting

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `usage.enabled` | bool | No | `false` | Enable balance check + usage reporting against `sentio-node` |
| `usage.sentio_node_addr` | string | When enabled | `` | gRPC address of the sentio-node service |
| `usage.redis_addr` | string | When enabled | `` → `redis_default_addr` | Redis used for the query-check cache |

### `concurrency_limit` — Per-User Concurrency

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `concurrency_limit.enabled` | bool | No | `false` | Enable per-user concurrency enforcement |
| `concurrency_limit.per_user` | int | No | `0` | Max concurrent queries per identity. `0` = track but don't enforce |
| `concurrency_limit.timeout` | duration | No | `60s` | Stale-permit reap window |
| `concurrency_limit.fail_open` | bool | No | `true` | On Redis outage: `true` allows the query with a warn log, `false` rejects |
| `concurrency_limit.redis_addr` | string | When enabled | `` → `redis_default_addr` | Redis backing the limiter sorted-set |

### `state` — Session Tracker (OnHello-only)

After the rewriter migration the state plugin's only job is OnHello: copy `ClientHello.Database` into `SessionState.LogicalDatabase` for the rewriter (which runs in OnQuery) to read. There are no operator-tunable fields here today.

### `logging`

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `logging.queries` | bool | No | `true` | Log SQL query content |
| `logging.data` | bool | No | `false` | Log Data packet content (debug only) |
| `logging.max_query_bytes` | int | No | `300` | Truncation length for logged queries (bytes) |
| `logging.max_data_bytes` | int | No | `200` | Truncation length for logged Data packets (bytes) |

### `observability`

Server-mode only. Adds a background **metrics Collector** that periodically polls the downstream ClickHouse, the host, and the Go runtime, plus an optional authenticated **pprof** endpoint. Both are served on `metrics_listen`: `/metrics` merges the proxy's own counters with the collected series in a single scrape, and `/debug/pprof` is mounted only when enabled. Agent mode does not run the collector.

Collector ClickHouse credentials come from the `ckh_manager_config_path` provider — there are no separate collector credential fields. The collector is **fail-open**: a failed poll logs a warning and keeps the previous snapshot; it never blocks queries or crashes the proxy.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `observability.collector.enabled` | bool | No | `true` | Run the background metrics collector. Env: `HOUSEGATE_COLLECTOR_ENABLED` (set `false` to disable). |
| `observability.collector.interval` | duration | No | `15s` | Poll period for the ClickHouse / host / runtime sources. Must be `> 0` when the collector is enabled. |
| `observability.collector.poll_timeout` | duration | No | `5s` | Per-poll timeout for the ClickHouse query. Must be `> 0` when the collector is enabled. |
| `observability.collector.ch_addr` | string | No | `` (shard replicas / upstream) | Advanced override for the ClickHouse poll target; normally derived automatically — leave empty. Env: `HOUSEGATE_COLLECTOR_CH_ADDR`. |
| `observability.pprof.enabled` | bool | No | `false` | Mount `/debug/pprof` on `metrics_listen`. Env: `HOUSEGATE_PPROF_ENABLED` (set `true` to enable). |
| `observability.pprof.token` | string | When pprof enabled | `` | Bearer token guarding `/debug/pprof` (constant-time compare). **Required** whenever pprof is enabled — keep it in an age-encrypted config, never plaintext. Env: `HOUSEGATE_PPROF_TOKEN`. |

```yaml
observability:
  collector: { enabled: true,  interval: "15s", poll_timeout: "5s" }
  pprof:     { enabled: false, token: "" }   # token REQUIRED when enabled
```

**Exposed series** (all prefixed `clickhouse_proxy_`, scraped from `metrics_listen`):

- **ClickHouse** (label `replica`): `ch_up`, `ch_query_total`, `ch_insert_total`, `ch_memory_tracking_bytes`, `ch_parts_active`, `ch_replication_queue`, `ch_mutations_pending`, `ch_mutations_running`, `ch_tables`, `ch_os_cpu_seconds`.
- **Host**: `host_cpu_percent`, `host_mem_available_bytes`, `host_mem_total_bytes`, `host_disk_read_bytes_total{device}`, `host_disk_write_bytes_total{device}`, `host_net_rx_bytes_total`, `host_net_tx_bytes_total`.
- **Go runtime**: `runtime_goroutines`, `runtime_heap_alloc_bytes`, `runtime_gc_pause_seconds`.
- **Collector health**: `collector_up`, `collector_last_success_timestamp_seconds{source}`.

`/debug/pprof` (when enabled) requires an `Authorization: Bearer <token>` header:

```bash
curl -s -H "Authorization: Bearer $HOUSEGATE_PPROF_TOKEN" \
  http://<host>:9091/debug/pprof/heap -o heap.pprof
go tool pprof heap.pprof
```

A full example block lives in [configs/local.server.yaml](configs/local.server.yaml).

### `network_state`

`network_state.source` is polymorphic, auto-detected by suffix:

- ends with `.yaml` / `.yml` → loaded as a static fixture into `InMemoryNetworkState` at startup. Schema in [pkg/network/yaml.go](pkg/network/yaml.go); example in [configs/local.network_state.yaml](configs/local.network_state.yaml).
- anything else → Redis address for the statemirror consumer. Empty falls back to `redis_default_addr`.

```yaml
network_state: { source: "configs/local.network_state.yaml" }   # YAML mode
network_state: { source: "redis.internal:6379" }                # Redis mode
```

Env-var fallback: `HOUSEGATE_NETWORK_STATE_SOURCE` (modern) and `HOUSEGATE_NETWORK_STATE_REDIS` (legacy, Redis-only).

### `shard`, `health_check`, `pool`, `routing` — Shard & Replicas

Optional; takes priority over the flat `upstream` field. Each proxy instance manages one shard.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `shard.name` | string | No | `` | Human-readable shard name |
| `shard.replicas` | []Replica | Yes (when `shard` is present) | `[]` | Per-replica `{host, port, weight, is_backup}` |
| `shard.settings` | map[string]string | No | `{}` | Default ClickHouse settings merged into queries for this shard |
| `health_check.interval` | duration | No | `5s` | Active health probe interval |
| `health_check.timeout` | duration | No | `3s` | Per-probe timeout |
| `health_check.failure_threshold` | int | No | `3` | Consecutive failures before marking unhealthy |
| `health_check.recovery_threshold` | int | No | `2` | Consecutive successes before marking healthy again |
| `pool.max_idle` | int | No | `5` | Idle connections per replica |
| `pool.max_open` | int | No | `50` | Max connections per replica |
| `pool.max_lifetime` | duration | No | `1h` | Max age of a pooled connection |
| `pool.max_idle_time` | duration | No | `10m` | Max idle time before close |
| `pool.dial_timeout` | duration | No | `5s` | Per-replica dial timeout |
| `routing.strategy` | string | No | `round-robin` | One of `round-robin`, `random`, `least-conn`, `weighted` |

---

# Part 2 — Running House Gate

There are two ways to run House Gate:

- **Standalone** — the `housegate` binary with a config file (or env / CLI flags). Three runtime modes are inferred from the config.
- **Library** — import `housegate` from your own Go process and call `housegate.New(opts).Run(ctx)`. Same plugin chain; you control dependency injection and lifecycle.

## 1. Standalone Mode

```bash
bazel-bin/cmd/housegate_/housegate -config config.json
# or via Bazel (rebuilds if needed)
bazel run //cmd:housegate -- -config config.json
```

Mode is **inferred from the config**, not a flag:

| Trigger | Mode |
|---------|------|
| `agent.mode: true` | Agent |
| `upstream` or `shard` set (and not agent) | Server (with local CH) |
| neither set | Server (router-only — forwards to peers via NetworkState) |

Press `Ctrl+C` for a graceful shutdown; final stats are printed before exit. On startup the proxy logs its resolved mode and key settings:

```
housegate listening  addr=:9001  mode=server  indexer_id=42
metrics listening    addr=:9091
```

### 1.1 Relay Mode

Server-mode proxy. Terminates client TCP, validates JWS auth, rewrites SQL via the `sql-rewriter` gRPC service, and forwards to local ClickHouse replicas. **Requires** `network_state.source` and `ckh_manager_config_path`.

Minimal config (no auth, single upstream):

```json
{
  "listen": ":9001",
  "upstream": "127.0.0.1:9000",
  "metrics_listen": ":9091",

  "ckh_manager_config_path": "/etc/housegate/ckh-manager.yaml",
  "redis_default_addr": "localhost:6379",

  "rewriter":      { "service_addr": "localhost:50051", "timeout": "5s" },
  "network_state": { "source":       "localhost:6379" },
  "logging":       { "queries":      true, "data": false }
}
```

With JWS authentication:

```json
{
  "listen": ":9001",
  "upstream": "127.0.0.1:9000",

  "ckh_manager_config_path": "/etc/housegate/ckh-manager.yaml",
  "redis_default_addr": "localhost:6379",
  "relay_private_key_hex": "0xYOUR_RELAY_PRIVATE_KEY_HERE",

  "auth": {
    "enabled": true,
    "allowed_addresses": ["0x1234567890123456789012345678901234567890"],
    "max_token_age": "1m",
    "allow_no_auth": false
  },

  "rewriter":      { "service_addr": "localhost:50051" },
  "network_state": { "source":       "localhost:6379" }
}
```

Shard-aware (per-replica pool + routing):

```json
{
  "listen": ":9001",
  "ckh_manager_config_path": "/etc/housegate/ckh-manager.yaml",
  "redis_default_addr": "redis.internal:6379",

  "shard": {
    "name": "shard-01",
    "replicas": [
      { "host": "ch-shard01-r1.internal", "port": 9000, "weight": 1 },
      { "host": "ch-shard01-r2.internal", "port": 9000, "weight": 1 }
    ]
  },
  "health_check": { "interval": "5s", "timeout": "3s", "failure_threshold": 3, "recovery_threshold": 2 },
  "pool":         { "max_idle": 5, "max_open": 50, "max_lifetime": "1h", "max_idle_time": "10m", "dial_timeout": "5s" },
  "routing":      { "strategy": "round-robin" },

  "network_state": { "source": "redis.internal:6379" }
}
```

### 1.2 Agent Mode

No local ClickHouse — every query is signed with `agent.private_key_hex` and forwarded to a relay-mode proxy at `agent.upstream`. Server-side features (rewriting, shard routing) are disabled.

Config-file form:

```json
{
  "listen": ":9001",
  "metrics_listen": ":9091",

  "agent": {
    "mode": true,
    "upstream": "10.0.0.8:9001",
    "private_key_hex": "0xYOUR_AGENT_PRIVATE_KEY_HERE"
  }
}
```

Agent can also start without a config file, via CLI flags or env vars:

```bash
# CLI flags
bazel-bin/cmd/housegate_/housegate \
  -agent -agent-upstream 10.0.0.8:9001 \
  -agent-key 0xYOUR_PRIVATE_KEY_HERE -listen :9001

# Env vars (recommended for secrets)
HOUSEGATE_AGENT=true \
HOUSEGATE_AGENT_UPSTREAM=10.0.0.8:9001 \
HOUSEGATE_AGENT_KEY=0xYOUR_PRIVATE_KEY_HERE \
HOUSEGATE_LISTEN=:9001 \
bazel-bin/cmd/housegate_/housegate

# Mixed (env for secret, flags for routing)
HOUSEGATE_AGENT_KEY=0xYOUR_PRIVATE_KEY_HERE \
bazel-bin/cmd/housegate_/housegate -agent -agent-upstream 10.0.0.8:9001
```

> **Security:** CLI flags are visible in process listings (`ps`, `/proc`). Always prefer `HOUSEGATE_AGENT_KEY` or a config file for the private key.

Override priority (highest → lowest): CLI flags → env vars → config file → built-in defaults.

All CLI flags:

| Flag | Default | Description |
|------|---------|-------------|
| `-agent` | `false` | Enable agent mode (overrides `agent.mode`) |
| `-agent-upstream` | (empty) | Server-side proxy address |
| `-agent-key` | (empty) | Ethereum private key for JWS signing |
| `-listen` | `:9001` | Proxy listen address |
| `-metrics-listen` | `:9091` | Prometheus metrics address |
| `-dial-timeout` | `5s` | Upstream dial timeout |
| `-idle-timeout` | `5m` | Connection idle timeout |
| `-log-queries` | `true` | Log SQL query content |
| `-config` | (empty) | Path to JSON/YAML config (also `HOUSEGATE_CONFIG`); age-encrypted accepted |

### 1.3 Router-Only Server

A server-mode housegate with **no local ClickHouse** (no `shard`, no `upstream`). Every session is forwarded to a peer relay discovered via `NetworkState`. The role the legacy "forwarding-only" mode used to play, now expressed as a sub-shape of server mode. **Requires** `network_state.source`; rewriter / ckh-manager are not needed and the SQL rewriter is auto-disabled in this configuration.

```json
{
  "listen": ":9001",
  "metrics_listen": ":9091",
  "network_state": { "source": "redis.internal:6379" }
}
```

The dialer falls through to a random peer with a non-zero `ClickhouseProxyPort` from `RetrieveAllIndexerInfos()` and excludes itself (`selfListenPort` + `isLocalAddress` check). On dial failure it retries up to three peers before erroring back to the client. If a agent's USE picks a different host, the receiving router-only server pivots the session via the forward-decision plugin just like any other server-mode proxy — see [Part 3](#part-3--how-multi-proxy-routing-works).

## 2. Library Mode

Embed House Gate inside another Go process. Same plugin chain as standalone — the binary is just a thin shell over this API.

```go
import (
    "context"

    "housegate/housegate"
    "housegate/housegate/pkg/config"
)

cfg := /* your *config.Config — load from file or build programmatically */

p, err := housegate.New(housegate.Options{
    Config: cfg,

    // Optional: report which indexer this proxy represents. Resolved
    // lazily on every Proxy.IndexerId() call so a host that learns
    // the id post-startup can update without rebuilding.
    GetIndexerId: func() uint64 { return myIndexerRegistry.Id() },

    // Optional: dependency overrides. Any non-nil value is used
    // verbatim (the caller owns its lifetime). Nil → New builds it
    // from Config and tears it down when Run returns.
    //
    //   NetworkState        network.State
    //   CkhManager          ckhmanager.Manager
    //   Validator           auth.Validator
    //   Rewriter            rewriter.Factory
    //   CredProvider        credentials.CredentialProvider
    //   Signer              auth.Signer
    //   UsageClient         billing.UsageClient
    //   Cluster             cluster.Cluster
    //   CommitGateObservers []commitgate.Observer
    //   RedisClients        map[string]*redis.Client
    //   Logger              *slog.Logger  // any slog.Logger; nil → pkg/log default
})
if err != nil { return err }

// Blocks until ctx is cancelled or the listener errors.
if err := p.Run(ctx); err != nil { return err }

// Or supply your own listener (TLS-wrap, ":0" port-binding, unix sockets):
// err := p.RunWith(ctx, ln)
```

The `Proxy` interface:

| Method | Description |
|--------|-------------|
| `Run(ctx) error` | Bind `cfg.Listen`, serve until `ctx` cancels or the listener errors. Resource teardown happens before the call returns. |
| `RunWith(ctx, ln) error` | Like `Run` but the caller owns the listener. |
| `Addr() net.Addr` | Bound listener address; nil before Run/RunWith has bound, stable thereafter. |
| `IndexerId() uint64` | Resolves the `GetIndexerId` getter on every call (returns 0 when no getter was supplied). |

**Ownership rules.** Any dep you pass in (non-nil field on `Options`) is yours to close after `Run` returns. Anything `New` built from the config is closed by the proxy itself in reverse construction order. There is no separate `Close` method — `Run` / `RunWith` is the lifecycle scope.

Use cases that benefit from library mode:
- Embedding House Gate inside a larger service that already has a Redis pool, network-state mirror, or ckh-manager you'd like to reuse.
- Tests that need a `:0`-port proxy with bespoke fakes for `Validator` / `NetworkState` / `Rewriter`.
- Hosts with a non-static indexer id (registered on-chain at runtime) — wire `GetIndexerId` to the registry.

See the design spec at [docs/superpowers/specs/2026-04-26-cmd-library-mode-design.md](docs/superpowers/specs/2026-04-26-cmd-library-mode-design.md) for lifecycle and ownership details.

---

# Part 3 — How Multi-Proxy Routing Works

A production deployment runs many server-mode housegates side by side, one per indexer. A agent's `clickhouse-client` may issue `USE tenantB` where `tenantB` is hosted on a *different* server-mode proxy than the one the agent is currently talking to. The proxy network resolves this transparently. Full design: [docs/superpowers/specs/2026-04-28-two-port-server-mode.md](docs/superpowers/specs/2026-04-28-two-port-server-mode.md).

## Topology overview

```
                     network_state (yaml file or Redis statemirror)
                              │
                              │  reads: indexer_infos, database_infos,
                              │         database_permissions
                              ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │                                                                      │
   │  agent (client side)                                               │
   │   • signs every query JWS with agent.private_key_hex               │
   │   • picks a server-mode peer per session via Selector (auto-disco)   │
   │     OR uses pinned agent.upstream                                  │
   │                                                                      │
   └────────────┬─────────────────────────────────────────────────────────┘
                │  ClientHello + JWS in password
                ▼
   ┌────────────────────────────────────┐         ┌────────────────────────────┐
   │ server-mode proxy A                │         │ server-mode proxy B        │
   │                                    │         │                            │
   │  external port  :9001 ─┐           │         │  external port  :9001      │
   │   • Stripper           │           │         │  internal port  :9101      │
   │   • credential         │           │         │   (peer-only)              │
   │   • auth (JWS)         │           │         │                            │
   │   • forward-decision   │           │         │                            │
   │   • rewrite, …         │           │         │                            │
   │                        │           │         │                            │
   │  internal port  :9101  │           │         │                            │
   │   (peer-only,          │           │         │                            │
   │    pre-flagged peer-   │           │         │                            │
   │    trusted)            │           │         │                            │
   │                        │           │         │                            │
   │   ClickHouse A ◄───────┘           │         │   ClickHouse B             │
   └─────────────────┬──────────────────┘         └────────────────────────────┘
                     │  USE tenantB  →  forward.Plugin sees tenantB lives on B
                     │
                     ▼
                Session.RebindToPeer(B:internal)
                ── re-handshake with __peer__ envelope + relay JWS ──►
                                                              │
                                                              ▼
                                            B's internal port pre-flags
                                            IsPeerTrusted; auth + rewrite
                                            skip; rest of chain runs.
```

The architectural invariant: **a ClickHouse instance only ever opens TCP to its co-located housegate.** Cross-shard `remote()` clauses always loop through the local housegate first; the network-layer ACL (firewall / SG) hard-blocks CH from reaching any other housegate or any peer's internal port.

## Two listeners per server (external / internal)

When `internal_listen` is set, [`buildServer`](build.go) binds a second `*proxy.Server` on that address. The second listener installs a `PreflagSession` callback that stamps every accepted session with `IsPeerTrusted=true` + `IsInternalPort=true` *before* OnHello fires, so peer-trust-aware plugins (auth, rewrite, commitgate) skip themselves automatically. Both listeners share the same plugin set; only the pre-flag changes.

| Aspect | external port (`listen`) | internal port (`internal_listen`) |
|---|---|---|
| Accepted dialers | agents, local CH `remote()` loopbacks | peer housegates only (firewall-enforced) |
| Pre-flagged session state | none | `IsPeerTrusted = true`, `IsInternalPort = true` |
| `__route__` envelope | accepted (loopback path) | **rejected** by `routeplugin.Stripper` to prevent forwarding loops |
| `__peer__` envelope | accepted when present | accepted (validates the dialing peer's JWS) |
| `auth` plugin | runs (validates agent JWS) | skipped (`PeerTrustAware.RunOnPeerTrust=false`) |
| `forward-decision` | runs | does not apply (internal port never forwards onward) |
| `rewrite`, `commitgate` | run | skipped (peer-trust opt-out) |
| `metrics`, `usage`, `concurrency`, `sessionstate` | run | run |

The internal port still validates the `__peer__` JWS even though the network layer already isolates it — the JWS gives cryptographic proof of which peer is calling, used for audit and metrics tagging.

## Forward-decision plugin

[`pkg/plugins/forward/`](pkg/plugins/forward/) runs on the external-port chain only. Two firing points:

**OnHello.** Looks up `hello.Database` in NetworkState. If the database lives on a peer indexer, calls `Session.RebindToPeer(peer:internal)` — replays the hello against the new upstream with a freshly-signed `__peer__` handshake, caches the peer's ServerHello so the relay echoes it to the client. Sets `IsForwarding=true`. If `hello.Database == ""`, defers; the first `USE` re-decides. If the database is unknown, replies with synthetic `Code: 81 Database doesn't exist`.

**OnQuery (USE detection).** A narrow regex (`^\s*USE\s+(\S+)\s*;?\s*$`, case-insensitive) catches standalone `USE <name>`. If the new database resolves to a different peer than the current upstream, fires `RebindToPeer` again before forwarding the USE packet. The atomic upstream-codec swap (`atomic.Pointer[chproto.Codec]` in `Session`) keeps the relay's read/write goroutines race-free; both `clientToUpstream` and `upstreamToClient` re-fetch the upstream pointer on each cycle and tolerate a swap mid-stream.

Cross-DB SQL inside a single statement (`tenant1.x JOIN tenant2.y`) is **not** re-routed at the session level — that flows through the SQL rewriter's per-clause `remote()` path described below.

The `ForwardAware` marker mirrors `PeerTrustAware` (default-on, opt-out). Plugins that should fire on the *originating* proxy regardless of forwarding (auth, metrics, concurrency, usage, sessionstate, credential) keep firing; plugins that belong to the *host* proxy (rewrite, commitgate) implement `RunOnForward()=false`.

## Agent auto-discovery

When `agent.upstream` is empty, [`pkg/plugins/agent.Selector`](pkg/plugins/agent/upstream_select.go) picks a server-mode peer per session via a two-tier algorithm against `network_state.source`:

```
account = derive_address(agent.private_key_hex)
perms   = NetworkState.RetrieveDatabasePermissions(account)

permissioned = indexers hosting at least one DB in `perms` AND with a non-zero ClickhouseProxyPort
bound        = any indexer with a non-zero ClickhouseProxyPort

switch {
case len(permissioned) > 0:  pick = random(permissioned)            // normal path
case len(bound) > 0:         pick = random(bound); IsBootstrap=true // see below
default:                     return error("no bound indexers")
}
```

**Bootstrap fallback.** A brand-new account has no permissions on any database — by construction, because they haven't created one yet. Failing the connection at dial time would lock new users out of `CREATE DATABASE` (chicken-and-egg). When `permissioned` is empty but `bound` is non-empty, the Selector picks a random bound indexer anyway; their first `CREATE DATABASE` lands there, `commitgate` registers the new DB in NetworkState, and subsequent sessions resolve correctly.

The bootstrap path emits a warn log and increments the `clickhouse_proxy_agent_bootstrap_fallback_total` Prometheus counter so operators can spot accounts that should not be in the bootstrap path.

Random selection across the chosen tier balances load and gives free failover (the next session re-rolls). A pinned `agent.upstream` still works as an explicit override.

## Cross-shard `remote()` envelopes

When the SQL rewriter emits a `remote()` clause for a logical DB hosted by a peer, the connection from the local CH back through housegate carries two prefixed values nested in the user/password fields:

- **`user`**: `__route__|<peer-addr>|__peer__|<self-address>` — the route envelope is peeled by the local proxy's `routeplugin.Stripper` on the loopback hop, then forwards the connection to `<peer-addr>` (typically the peer's internal port). The peer envelope rides through to be validated downstream.
- **`password`**: a peer-relay JWS signed by the relay private key with audience = peer's indexer-id. Once the receiving proxy's `credential.Plugin` validates the JWS, `IsPeerTrusted=true` and the chain skips auth + rewrite via `PeerTrustAware`.

Both envelopes share the `|` delimiter convention ([pkg/route](pkg/route/), [pkg/peer](pkg/peer/)). The two markers compose: a routed *and* peer-trusted session applies the route filter first, then the peer-trust filter on the survivors. Implementation in [pkg/rewriter/sentio.go](pkg/rewriter/sentio.go) (`buildSentioTableMappings`, `buildRemoteUpstreams`).

This is the per-clause cross-shard mechanism; the per-session forward-decision plugin (above) is the per-session counterpart for sessions whose entire scope is one remote logical DB.
