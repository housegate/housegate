# Two-Port Server Mode: Logical-DB-Aware Session Routing — Design Spec

**Date:** 2026-04-28
**Status:** Proposed
**Authors:** poetry, Claude

## 1. Goal

A agent's `clickhouse-client` connects through a agent housegate to an
upstream server-mode housegate. Today the agent's upstream is a single
fixed proxy; if the user issues `USE tenant2` and `tenant2` happens to be
hosted on a *different* server-mode proxy, the session dies with
`Code: 81. DB::Exception: Database tenant2 doesn't exist`.

We want every server-mode proxy to be a valid entry point for any agent:
the proxy resolves the logical database to its actual host proxy at
**handshake time** and again on each `USE`, transparently rebinding the
session's upstream to the correct server-mode peer when needed. Agents
discover entry points by reading NetworkState rather than holding a
configured upstream.

To express this cleanly, server mode grows **two listeners** with
distinct trust postures:

- **External port** — agents and local CH `remote()` loopbacks land here.
  Full plugin chain runs (auth, forward-decision, rewrite, …).
- **Internal port** — only peer housegates dial here. Connections are
  pre-flagged `IsPeerTrusted = true`; auth + rewrite are skipped via the
  existing `PeerTrustAware` filter.

The historic `forwarding-only` mode is **deleted**. Its capability
("route requests to other proxies via NetworkState") becomes a behavior of
server mode; a server with no local shard configured is simply a
router-only server.

## 2. Non-goals

- **Not changing how cross-database SQL is routed.** Queries that
  reference logical DBs across multiple host proxies (e.g.
  `tenant1.x JOIN tenant2.y`) continue to flow through the rewriter's
  `remote()` path. The new connection-level forwarder only handles
  sessions whose entire scope is one remote logical DB.
- **Not changing the rewriter.** All `remote()` emission rules, including
  the `__route__` + `__peer__` double envelope and the loopback to local
  housegate, are unchanged.
- **Not collapsing agent mode.** Agent's deployment topology (next to
  the client, holding the signing key) is fundamentally different from
  server mode's; merging would pollute config schema. Server mode and
  agent mode remain distinct.
- **Not replacing CH-to-CH network controls.** The invariant "CH only
  talks to its own housegate" is enforced at the network layer (firewall
  / SG); this spec assumes that and relies on it.

## 3. The invariant

> **A ClickHouse instance only ever opens TCP to its co-located
> housegate. CH never dials another shard's CH or another housegate
> directly.**

This is operationally enforced (firewall) and architecturally relied upon.
Consequences:

1. The rewriter always emits `remote('local-housegate:external', ...)`.
   Cross-shard `remote()` loops back through the local housegate first;
   the local Stripper peels `__route__` and forwards to the peer's
   internal port. This is **already** how `pkg/rewriter/sentio.go` works
   (`buildSentioTableMappings`, `buildRemoteUpstreams`); no change.
2. The internal port is exclusively peer-housegate ↔ peer-housegate
   traffic. Network ACL must hard-block CH from reaching any
   internal-port. This is the bottom-line enforcement of the invariant.
3. Internal port handles a connection by **serving locally**. It does
   *not* do connection-level forwarding to a third peer. If the inner
   query references yet another shard, that surfaces as a
   `remote('local-housegate:external', ...)` clause (rewriter output) and
   exits via the local CH → local housegate loopback — i.e., a *new* TCP
   on the local external-port, not a forward off the internal-port. This
   prevents internal-port forwarding loops by construction.

## 4. Topology

```
      (signs JWS)                                        (validates JWS)
agent ───────────────────────► external-port ──┐
                                                 │
local CH `remote()` ──► loopback ────────────────┤   external-port chain
   to local housegate                            │   • Stripper (peels __route__)
                                                 │   • credential (peer-trust if __peer__)
                                                 │   • auth (validates agent JWS)
                                                 │   • forward-decision  ◄── NEW
                                                 │   • rewrite, sessionstate, usage, ...
                                                 │
                                                 ▼
                              ┌── serve locally on local CH
                              │   (existing path)
                              └── rebind to peer:internal
                                    (RebindUpstream w/ __peer__ envelope + peer-relay JWS)
                                                       │
                                                       ▼
peer housegate ──────────────────► internal-port ─► internal-port chain
                                                    • IsPeerTrusted=true (pre-flagged)
                                                    • rewrite skipped (PeerTrustAware)
                                                    • auth skipped     (PeerTrustAware)
                                                    • commitgate skipped (PeerTrustAware,
                                                       DDL is host-proxy's call)
                                                    • metrics, usage, concurrency,
                                                       sessionstate run
                                                       │
                                                       ▼
                                                    local CH
```

## 5. Listener + chain composition

`Server` grows two listeners with distinct chain factories. Both listeners
share the same plugin set; the chain factory chooses which session-state
flags to pre-flag and which marker filters apply.

| Aspect | external-port | internal-port |
|---|---|---|
| Accepted dialers | agents, local CH `remote()` loopbacks | peer housegates only |
| Pre-flagged session state | none | `IsPeerTrusted = true`, `PeerAddress = <validated>` |
| Stripper (`__route__`) | runs (loopback path) | **rejected**: receiving `__route__` on internal-port is a misconfig; close the connection |
| credential (`__peer__`) | runs when envelope present | runs (validates the JWS that was injected by the dialing peer) |
| auth | runs (validates agent JWS) | skipped via `PeerTrustAware.RunOnPeerTrust=false` |
| forward-decision | **new**, runs | does not apply (internal-port never forwards onward) |
| rewrite | runs | skipped via `PeerTrustAware` |
| commitgate | runs | skipped via `PeerTrustAware` |
| sessionstate, usage, concurrency, metrics | run | run |

**Why the internal-port still validates `__peer__`**: the listener's
network-level isolation is one layer of defense; the JWS gives
cryptographic proof of which peer is calling and is needed for audit /
metrics tagging. Belt + suspenders, no extra cost since the credential
plugin is already there.

**Why Stripper hard-rejects on internal-port**: a `__route__` envelope on
internal-port would mean a peer is asking us to forward further, breaking
the invariant in §3.3. Reject early, log loudly.

## 6. Forward decision

A new `forwardplugin.Plugin` runs on the external-port chain only. It
fires twice in a session:

### 6.1 OnHello

```
hostIndexer = NetworkState.RetrieveDatabaseInfo(hello.Database).IndexerId
peerAddr    = NetworkState.RetrieveIndexerInfo(hostIndexer)
              .{IndexerUrl, ClickhouseProxyPort}    // peer's internal-port

if hostIndexer == self            → no forward; chain proceeds locally.
if peerAddr resolves              → set SessionState.RouteTarget = peerAddr,
                                     mark IsForwarding=true,
                                     RebindUpstream(peerAddr, replayState=false)
                                     using peer-relay JWS handshake.
if hello.Database == ""           → defer (no decision yet); fall through to
                                     local upstream as default. First USE will
                                     re-decide.
if RetrieveDatabaseInfo missing   → reply with a synthetic
                                     `Code:81 Database doesn't exist` exception
                                     and close. (Don't forward to a random peer.)
```

### 6.2 OnQuery — USE detection

`forwardplugin` peeks the SQL with a lightweight regex
`^\s*USE\s+(\S+)\s*;?\s*$` (case-insensitive). When matched:

```
newDb     = $1 (strip backticks/quotes)
newPeer   = resolve(newDb)  // same lookup as above
curPeer   = SessionState.RouteTarget

if newPeer == curPeer          → forward USE to current upstream as-is;
                                  sessionstate updates LogicalDatabase normally.
if newPeer != curPeer          → Session.RebindUpstream(newPeer, replayState=true)
                                  before forwarding the USE packet.
                                  Replay re-issues hello + accumulated SETs on
                                  the new upstream; the USE then runs against
                                  the right peer.
```

The regex is intentionally narrow (only `USE <name>` standalone).
Anything more elaborate (`USE … SETTINGS …`, multi-statement) falls
through to the rewriter's `maybeUpdateLogicalDatabase` callback path,
which already updates `SessionState.LogicalDatabase` after rewrite. We do
not re-evaluate forward-target on every query body, only on USE.

### 6.3 Cross-DB SQL falls back to rewriter's `remote()`

Sessions whose SQL references multiple logical DBs (without USE) are not
captured by forward-decision; the rewriter still emits `remote()` clauses
for non-local references, and those flow through the existing loopback +
`__route__` + `__peer__` path. **forward-decision does not peek SQL
bodies for table references.** This keeps the plugin's logic trivially
inspectable and keeps the rewriter as the single source of truth for SQL
rewriting.

## 7. `ForwardAware` marker interface

Mirrors the existing `PeerTrustAware`:

```go
// ForwardAware lets a plugin opt out of running on a session that the
// forward-decision plugin has marked as "this whole session is a
// transparent forward to a peer's internal-port". Default (no
// implementation, or RunOnForward()==true) means the plugin runs.
type ForwardAware interface {
    RunOnForward() bool
}
```

Default = run (most plugins still fire on the local housegate's books).
Opt-outs:

| Plugin | RunOnForward | Reason |
|---|---|---|
| auth | **true** | agent JWS is verified once at the entry proxy regardless of forwarding |
| metrics | true | local proxy still wants per-session counters |
| concurrency | true | forwarding consumes a local connection slot too |
| usage | true | local proxy bills the originating client even when forwarded |
| sessionstate | true | Replay needs accurate state to ship to peer |
| credential | true | runs only at OnHello regardless of forwarding |
| rewrite | **false** | the rewrite belongs to the host proxy, not the entry |
| commitgate | **false** | DDL gate fires on the host proxy |
| dbrewriter | **false** | logical↔physical DB mapping is the host's concern |

The chain filter is one new conditional in `pkg/plugin/chain.go` next to
the existing `RouteAware` / `PeerTrustAware` checks.

## 8. Agent upstream selection

Agent stops carrying a fixed `agent_upstream`. At dial time it
selects from NetworkState in two tiers:

```
account     = agent.account                    // Eth addr from agent_private_key_hex
perms       = NetworkState.RetrieveDatabasePermissions(account)
bound       = []   // any bound indexer
permissioned = []  // bound indexers hosting at least one DB the account can access

for indexerId, info := range NetworkState.RetrieveAllIndexerInfos() {
    if info.ClickhouseProxyPort == 0 { continue }   // unbound, skip
    bound = append(bound, info)
    if any db hosted by indexerId is in perms {
        permissioned = append(permissioned, info)
    }
}

switch {
case len(permissioned) > 0:
    upstream = permissioned[rand]                  // normal path
case len(bound) > 0:
    upstream = bound[rand]                         // bootstrap fallback (§8.1)
    log.Warnw("agent bootstrap fallback: no permissioned DBs",
              "account", account, "chosen", upstream.IndexerId)
    metric: agent_bootstrap_fallback_total{account=…}.Inc()
default:
    return error("no bound indexers in NetworkState")
}
```

Random selection across the chosen tier balances load and gives free
failover (next session re-rolls). The agent reconnects on upstream
failure naturally; no additional retry policy beyond what the TCP/relay
layer already does.

### 8.1 Bootstrap fallback for new accounts

A new account has no permissions on any database — by construction,
because they haven't created one yet. Failing the connection at dial
time would lock new users out of `CREATE DATABASE` (chicken-and-egg). So
when `permissioned` is empty but `bound` is non-empty, pick a random
bound indexer anyway. The user's first statement is typically
`CREATE DATABASE <name>`, which lands on whichever indexer was picked;
that indexer's commitgate plugin registers the new DB in NetworkState so
subsequent sessions resolve correctly.

The fallback is logged + metered (not silent) so operators can
distinguish "genuine new user" from "mis-permissioned existing user
sliding into bootstrap mode by accident". The metric also gives a
forward signal if the fallback rate spikes — usually means a
permissions-write path broke.

Server-side note: when CREATE DATABASE arrives via the bootstrap
fallback, the user is authenticated (their JWS verifies) but has no
DB-level perms. CREATE DATABASE itself must therefore be allowed
without prior DB perms — that's an existing assumption of the
auth/permission model, not new to this spec; flagging it for cross-check
during implementation.

Config:

- `agent_upstream` becomes optional. When set, it overrides
  auto-discovery (used for tests and for ops to pin agents to a
  specific server).
- New: `agent_network_state` block (Redis/YAML, same shape as
  server-mode's `network_state_redis`) so the agent can read
  NetworkState. This is the only material new dep on the agent.

## 9. RebindUpstream-to-peer

`Session.RebindUpstream` already replays USE + SET on a new connection.
We extend it with a "rebind-to-peer" mode where the dialer:

1. Dials the target `peer:internal` as a CH-native TCP.
2. Constructs a hello with `User = __peer__|<self-housegate-addr>` and
   `Password = <peer-relay JWS>`. The JWS is signed via
   `RelaySigner.SignPeerLogin(audience=<peer indexer-id>, ttl=…)`.
   Audience is the peer's indexer-id resolved from NetworkState; ttl is
   short (e.g. 30s) since we sign once per rebind.
3. Sends the hello; the peer's internal-port credential plugin validates
   the JWS, sets `IsPeerTrusted=true`, and the peer's chain proceeds.
4. Replay then issues `USE <db>` and `SET …` on the new connection.

The peer-relay JWS construction code lives in the rewriter today
(`resolveRemoteCredentials` inside `pkg/rewriter/sentio.go`); we extract
it to a public helper in `pkg/peer` so both the rewriter and the new
forward-decision plugin can call it.

**Rebind happens only at idle points.** USE is by definition standalone,
so the session is idle when rebind triggers. We never rebind mid-INSERT
data stream.

## 10. Configuration

```yaml
# server mode
listen_addr: "0.0.0.0:9000"            # external port (rename in code: external_listen_addr)
internal_listen_addr: "0.0.0.0:9001"   # NEW; peer-only, ACL-restricted
network_state_redis: { ... }
ckh_manager_config_path: "..."

# shard is now optional; absent = router-only server
shard:
  ...

# agent mode
agent_mode: true
agent_private_key_hex: "..."
agent_upstream: ""                    # optional override; empty = auto-discover
agent_network_state:                  # NEW
  redis: { ... }                        # same schema as server's network_state_redis
```

Backward compat:

- Existing server configs without `internal_listen_addr` print a
  startup warning and disable the internal-port path. Cross-proxy
  `remote()` calls then land on the external-port (current behavior),
  which still works because the credential plugin runs there too. New
  deployments should set internal-port; old ones keep working.
- Existing forwarding-only configs (no shard, no upstream) become
  router-only servers automatically — `Config.Mode()` no longer returns
  `Forwarding`. They keep working without config changes; the upgrade is
  pure code-side.
- Existing agent configs with `agent_upstream` set keep that pin
  until ops empties it and sets `agent_network_state`.

## 11. Migration & phasing

Each phase is independently shippable and rollback-able.

| Phase | Change | Risk |
|---|---|---|
| 1 | Add `internal_listen_addr` listener with `IsPeerTrusted=true` pre-flag + Stripper-rejection. Existing peer traffic keeps using external-port; verify on internal-port in staging. | low |
| 2 | Add `forward-decision` plugin (OnHello only). Agent still uses fixed `agent_upstream`. Pointing a agent at any server proxy now works for any DB. | medium — `RebindUpstream` to peer is new code path |
| 3 | Extend `forward-decision` with USE rebind. Replay verified against peer's internal-port. | medium — replay correctness |
| 4 | Agent auto-discovery from NetworkState. | low |
| 5 | Delete `Config.Mode() == Forwarding` branch, `buildForwarding`, `pickRandomBoundProxy`. | trivial cleanup |

Each phase has a feature flag (`enable_forward_decision`,
`enable_internal_port`, `enable_agent_auto_discover`) so the rollback
is config-only.

## 12. Open questions

1. **Peer-internal connection pool?** Currently each rebind opens a
   fresh TCP to peer:internal. For a hot session that bounces
   `USE tenant1; USE tenant2; USE tenant1;` we'd churn three TCPs.
   Probably not worth optimizing in v1; revisit if metrics show pain.
2. **Concurrency limits across both ports.** External-port and
   internal-port traffic both consume the same local CH replica pool.
   Should `concurrency` plugin track them as one shared budget or
   separate budgets? I lean shared (the local CH doesn't care which
   port the request arrived on). Operators can revisit.
3. **Internal-port observability.** Metrics labels need a
   `listener=internal|external` dimension so we can see peer-traffic
   share. Trivial but easy to forget.
4. **`hello.Database == ""` default behavior.** Current proposal:
   defer the forward decision and route locally; first USE re-evaluates.
   Edge case: a session that never USEs and never references a
   non-local DB silently runs locally on whatever default DB the
   server's CH provides — which is correct only if the user actually
   wanted local. If the local CH has no `default` database, they get a
   real CH error. Acceptable; matches today's UX.
5. **Bootstrap fallback abuse vector.** A user with no permissions can
   open a agent session via §8.1 fallback. They land on a random
   indexer, but every action (other than CREATE DATABASE) fails at
   auth/permission. Worth a connection-rate limit on bootstrap-fallback
   sessions per account to avoid trivial connection-flood DoS? Probably
   yes; defer to ops if needed. Today's threat model says no since the
   agent pubkey is already gated by who has the private key.

## 13. File map

**New**:
- `pkg/plugins/forward/` — `Plugin`, `Config`, `forward-decision` logic.
  Implements `QueryPlugin` (OnHello + OnQuery) and registers as
  `ForwardAware`-aware (so the chain filter respects opt-outs).
- `pkg/peer/relay_signer_helper.go` — extracted from
  `pkg/rewriter/sentio.go`'s `resolveRemoteCredentials`. Exposes a
  `SignPeerHello(audience) → (user, password)` helper for the forward
  plugin and the rewriter to share.

**Modified**:
- [build.go](build.go): collapse `buildForwarding` into `buildServer`;
  add internal-port listener wiring; register `forwardplugin` in the
  external-port chain before `rewrite`. Delete `pickRandomBoundProxy`.
- [pkg/proxy/server.go](pkg/proxy/server.go): two-listener accept loop;
  per-listener pre-flag of session state.
- [pkg/proxy/relay.go](pkg/proxy/relay.go): no behavior change beyond
  honoring `SessionState.RouteTarget` for upstream rebind.
- [pkg/chsession/session.go](pkg/chsession/session.go) +
  [state.go](pkg/chsession/state.go): `RebindUpstream` accepts a
  `RebindMode` (local-replica | peer-internal) and runs the
  peer-internal handshake when requested.
- [pkg/plugin/chain.go](pkg/plugin/chain.go): add `ForwardAware` filter
  next to existing `RouteAware` / `PeerTrustAware` filters.
- [pkg/plugins/auth/](pkg/plugins/auth/),
  [rewrite/](pkg/plugins/rewrite/),
  [commitgate/](pkg/plugins/commitgate/),
  [dbrewriter/](pkg/plugins/dbrewriter/): implement `ForwardAware`.
- [pkg/proxy/config.go](pkg/proxy/config.go): add
  `internal_listen_addr`, drop `Forwarding` from `Mode()`.
- Agent: [pkg/plugins/agent/](pkg/plugins/agent/) +
  [build.go](build.go) `buildAgent`: read `agent_network_state`,
  pick upstream by permissioned random.

**Deleted**:
- `Config.Mode() == "Forwarding"` branch and the standalone
  `buildForwarding`. The router-only deployment is now expressed as
  "server with no shard configured".

## 14. References

- [docs/superpowers/specs/2026-04-28-peer-trust-design.md](docs/superpowers/specs/2026-04-28-peer-trust-design.md)
  — peer-trust handshake mechanism reused on internal-port.
- [pkg/rewriter/sentio.go:581](pkg/rewriter/sentio.go) — static-mapping
  `remote()` emission with double envelope.
- [pkg/rewriter/sentio.go:729](pkg/rewriter/sentio.go) — dynamic-mapping
  `remote()` emission (`buildRemoteUpstreams`).
- [pkg/chsession/state.go:337](pkg/chsession/state.go) — Replay on
  rebind.
- [pkg/network/types.go:41](pkg/network/types.go) — NetworkState
  lookup surface, including `RetrieveDatabaseInfo` and
  `RetrieveDatabasePermissions`.
