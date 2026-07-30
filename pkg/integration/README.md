# Integration Tests

End-to-end tests that exercise the housegate proxy against real
ClickHouse containers (via [testcontainers-go]) and, where relevant, the
official `clickhouse` CLI binary. Unlike package-level unit tests these
spin up the full plugin chain — auth, rewriter, commitgate, concurrency,
usage, metrics, route, forward — and assert on observable wire behaviour
rather than on internal state.

## Running

```bash
bazel test //pkg/integration:integration_test                          # all
bazel test //pkg/integration:integration_test --test_filter='TestAuth' # subset
```

Requirements:

- Docker reachable from the test runner (testcontainers needs the
  daemon and a writable `HOME` for the reaper sidecar).
- For the `cli_test.go` cases: a `clickhouse` binary at
  `tests/bin/clickhouse` from the repo root. Missing binary → tests are
  skipped (not failed).

The shared ClickHouse container is started once in `TestMain`
([smoke_test.go](smoke_test.go)) and reused across every test in the
package; per-test isolation is achieved through per-test databases /
signers, not per-test containers.

## Test Inventory

### Smoke ([smoke_test.go](smoke_test.go))

- `TestProxyForwardsSelectOne` — bare end-to-end check: client → proxy →
  ClickHouse → client. Fails first if handshake, packet flow, or
  shutdown is broken at the most basic level.

### Single-upstream protocol coverage ([single_upstream_test.go](single_upstream_test.go))

- `TestInsertSelectRoundtrip` — Data-block path in both directions.
  Failure usually points at `Codec.Splice`, `proto.Reader` framing, or
  upstream Data-block forwarding.
- `TestLargeStream` — million-row pull. Stresses the chunk-by-chunk
  upstream→client packet path across negotiated transport modes.
- `TestException` — invalid SQL surfaces as a client-visible exception;
  the OnException first-byte heuristic fires best-effort.
- `TestGracefulShutdown` — `proxy.Close` returns within a tight deadline
  after a successful query.
- `TestCancelMidQuery` — client-side ctx cancel mid-`SELECT sleep`;
  exercises `ClientCancel` splice + server `EndOfStream` detection.

### Official CLI ([cli_test.go](cli_test.go))

Drives the *real* `clickhouse-client` binary against the proxy. Catches
framing/settings/addendum quirks that ch-go-on-both-sides would not see.

- `TestCLI_SelectOne` — CLI smoke (`SELECT 1` round trip).
- `TestCLI_AggregateStatePassthrough` — multiple opaque Native aggregate
  states (`avgState`, `uniqState`, and floating-point `sumState`) each pass
  through an official-client legacy connection without a value-layout decoder.
  A separate framed multiquery covers temporary-table/session preservation;
  opaque legacy connections are intentionally non-reusable.
- `TestCLI_InsertSelectRoundtrip` — CREATE / INSERT / SELECT / DROP via
  the CLI. Validates Data-block path under the official client.
- `TestCLI_LargeStream` — 100k-row stream via `SELECT count()` to avoid
  the CLI text-formatting every row.
- `TestCLI_Exception` — invalid SQL exits non-zero with offending
  identifier preserved; OnException best-effort doesn't swallow text.

### Auth plugin ([auth_test.go](auth_test.go))

JWS signing + commitgate `PermissionObserver` interaction. The observer
is auto-wired whenever `Auth.Enabled=true` and is what makes most of
these failure modes fail-closed.

- `TestAuth_NoToken` — unsigned query rejected when `AllowNoAuth=false`.
- `TestAuth_WrongKey` — JWS signed by a key not in `AllowedAddresses`
  rejected.
- `TestAuth_RejectsWithoutRewriter` — even a properly signed token is
  rejected without a rewriter: the observer gates on `StatementType` and
  refuses Unspecified.
- `TestAuth_ValidToken` — happy path: signed token + rewriter mock →
  query succeeds.
- `TestAuth_QueryHashMismatch` — caller signs SQL A but sends SQL B →
  proxy rejects with "query hash mismatch". Pins the SQL-binding
  semantics of the JWS.
- `TestAuth_AllowNoAuthBlockedByCommitgate` — `AllowNoAuth=true` lets
  the unsigned query past the auth plugin, but the commitgate observer
  rejects it anyway. Pins the non-obvious soft-rollout contract.

### Commit gate ([commitgate_test.go](commitgate_test.go))

Custom observers attached via `Options.CommitGateObservers`.

- `TestCommitGate_ObservesSelect` — observer fires for SELECT through
  the FROM-less `allowsEmptyAccess` path.
- `TestCommitGate_ObservesCreateTable` — canonical DDL gating: rewriter
  mock supplies `AccessedTables`, observer sees `CREATE_TABLE` event.
- `TestCommitGate_AbortWithSuccess` — observer returns
  `ErrAbortWithSuccess` → synthetic `EndOfStream` to client, DDL never
  reaches CH. The on-chain `CREATE/DROP DATABASE` contract.
- `TestCommitGate_ObserverError` — non-sentinel observer error becomes
  a CH-style exception with the observer's message; DDL not executed.

### Rewriter ([rewriter_mock_test.go](rewriter_mock_test.go), [rewriter_args_test.go](rewriter_args_test.go))

- `TestRewriterMock_RoundTrips` — the in-process rewriter mock is
  reachable, the full plugin chain runs (auth-off), and the mock
  actually observes the SQL.
- `TestRewriter_FailOpen` — rewriter returns non-Success → proxy logs
  and forwards the original SQL. Only meaningful in the
  no-permission-gating deployment.
- `TestRewriter_SeenSQLMatchesClient` — wire contract: SQL the client
  sends is the *same byte string* shipped to the rewriter. Catches
  pre-classification mutation regressions.
- `TestRewriter_PassesDynamicArgs` — auth-filtered `database_map`,
  `physical_database`, and `upstream_logical_database_in_context` all
  surface in the rewriter's `RewriteTableDynamicArgs`.

### Session state ([sessionstate_test.go](sessionstate_test.go))

- `TestSessionState_USEUpdatesLogical` — when the rewriter classifies a
  SQL as `STATEMENT_TYPE_USE`, `SessionState.SetLogicalDatabase` fires
  and the next query's `dynamic_args` carries the new logical DB in
  `upstream_logical_database_in_context`.

### Agent mode ([agent_test.go](agent_test.go))

Agent-mode housegate next to a CH client signing each query, forwarded
to a server-mode peer with auth on. The shared invariant is that
auth-on at the server is what proves the agent's signer ran — if it
didn't, the server would reject with "no token".

- `TestAgent_PinnedUpstream_HappyPath` — agent with explicit
  `cfg.Agent.Upstream` (no NetworkState, no Selector). The simplest
  hermetic topology; pins the agent plugin's signing hook + the agent
  dialer using the pinned upstream.
- `TestAgent_SelectorPicksPermissioned` — agent with no pinned
  upstream, only a NetworkState listing one bound indexer plus a
  database permission. Selector's tier-1 (permissioned) walk picks the
  one candidate; asserts `clickhouse_proxy_agent_bootstrap_fallback_total`
  does NOT increment.
- `TestAgent_SelectorBootstrapFallback` — agent with NetworkState but
  no permissions (the brand-new-account case). Tier-1 is empty so the
  bootstrap tier fires; query still succeeds and the bootstrap counter
  increments by ≥ 1.

### Peer-trust / cross-housegate routing ([peer_envelope_test.go](peer_envelope_test.go), [forward_test.go](forward_test.go), [route_envelope_test.go](route_envelope_test.go))

The three envelope/forward paths that move a session between housegate
proxies. They share `__peer__|<addr>` and `__route__|<target>|<real>`
on-the-wire encodings but differ in which plugin initiates the pivot.

- `TestPeerEnvelope_BypassesAuthAndCommitgate` — inbound `__peer__|<addr>`
  envelope + peer-relay JWS in the password is parsed by
  `credential.Plugin`, sets `IsPeerTrusted`, and the chain's
  `PeerTrustAware` filter skips auth + commitgate. Hits the external
  port deliberately (the internal port pre-flags every session so a
  success there wouldn't prove the envelope branch fired).
- `TestForward_PivotToPeerAtHello` — two server-mode proxies. proxyA's
  `forward.Plugin.OnHello` sees `hello.Database` resolving to peer
  indexer 2, dials proxyB with a `__peer__|<self>|forwarded` envelope,
  `RebindToPeer` swaps the upstream codec mid-handshake, and the
  SELECT lands on proxyB → CH. Catches regressions in NetworkState
  lookup, peer signing, RebindToPeer wiring, and the forwarded vs
  legacy envelope distinction.
- `TestRouteEnvelope_StripsAndPivots` — `routeplugin.Stripper` peels a
  `__route__|<peer-addr>|<realUser>` envelope from `hello.User` and
  pivots the dialer to `<peer-addr>` even though the router has its
  own upstream configured. Mirrors what the rewriter emits on
  cross-indexer `remote()` clauses.

### Concurrency limiter ([concurrency_test.go](concurrency_test.go))

Redis-backed per-user quota; tests use miniredis.

- `TestConcurrency_QuotaEnforced` — `PerUser=1`: second query from the
  same user while the first runs returns a quota error.
- `TestConcurrency_SlotReleased` — slot released after first completes;
  next query succeeds. Validates the `OnQueryComplete → Release` path.
- `TestConcurrency_FailOpen` — Redis dies after proxy startup,
  `FailOpen=true`: query still reaches CH.
- `TestConcurrency_DifferentUsersIndependent` — two signers, two
  independent quotas. Catches accidental globalisation of the key.

### Billing / Usage ([usage_test.go](usage_test.go))

`Options.UsageClient` is injected with a recording mock.

- `TestUsage_Recorded` — exactly one `CheckBalance` + one `ReportUsage`,
  both with `payer == signer == lowercased(ethAddr)`.
- `TestUsage_InsufficientBalanceRejects` — `CheckBalance` returns
  `ok=false` → client sees `INSUFFICIENT_BALANCE`, `ReportUsage` not
  called.

### Metrics ([metrics_test.go](metrics_test.go))

- `TestMetrics_QueriesForwardedCounter` — a successful signed query
  increments `clickhouse_proxy_queries_forwarded_total`. Reads back via
  `prometheus.DefaultGatherer.Gather()`; asserts on the *delta* (the
  metric is a process-wide global shared across the test binary).

### Database routing ([database_routing_test.go](database_routing_test.go))

- `TestUseDatabaseSwitch` — `USE <db>` on a second DB registered in
  NetworkState passes through `forward.Plugin`'s OnQuery USE detection.

### Replica routing ([replica_routing_test.go](replica_routing_test.go))

Proxy fronts two ClickHouse replicas via the cluster manager.

- `TestReplicaRouting_BothReachable` — round-robin actually visits both
  replicas (each replica has a unique marker row).
- `TestReplicaRouting_FailoverOnDown` — health check tightened via
  `WithConfigMutator`; queries continue to succeed after one replica is
  stopped.
- `TestReplicaRouting_Random` — "random" strategy reaches both replicas
  over 30 attempts (`(1/2)^30` miss probability).
- `TestReplicaRouting_Weighted` — 1:9 weight skews distribution; B/A ≥ 3
  over 50 attempts. Catches silent equivalence with round-robin.

### Router-only mode ([router_only_test.go](router_only_test.go))

- `TestRouterOnly_ForwardsToPeer` — server-mode proxy with no
  `upstream` / `shard` splices client → peer-mode proxy → CH. The
  `pickRandomBoundProxy` path is what selects the peer from
  NetworkState.

### Transport teardown ([cancel_test.go](cancel_test.go), [upstream_dies_test.go](upstream_dies_test.go))

- `TestCancel_KillsUpstream` — client ctx cancel propagates to CH:
  `system.processes` no longer shows the `sleep()` query after teardown.
  Catches relay buffering of cancels or leaked half-open sockets.
- `TestUpstream_DiesMidQuery` — docker-stop the upstream container
  mid-`SELECT sleep`: in-flight `QueryRow` returns an error within a
  bounded deadline (relay does not hang on the half-open upstream
  socket). Uses a dedicated CH instance because stopping `chEnv` would
  break every subsequent test in the binary.

## Helpers ([testenv/](testenv/))

| Helper | Purpose |
|---|---|
| `StartClickHouse(t)` | Start a ClickHouse container scoped to one test (auto-stops on cleanup). |
| `StartClickHouseForMain()` | Shared-instance variant used by `TestMain`. |
| `StartClickHousePair(t)` | Two containers for replica-routing tests. |
| `StartServerProxy(t, chAddr, opts...)` | Server-mode proxy in front of a single CH. |
| `StartReplicatedProxy(t, chA, chB, opts...)` | Server-mode proxy fronting two replicas. |
| `StartRouterOnlyProxy(t, peer, opts...)` | Router-mode proxy with `withPeerIndexer` pointing at a peer. |
| `StartAgentProxy(t, privateKeyHex, upstream, opts...)` | Agent-mode proxy pinned to an explicit upstream (no Selector). |
| `StartAgentProxyWithSelector(t, privateKeyHex, opts...)` | Agent-mode proxy that discovers its upstream per session via the in-memory NetworkState + Selector. Pair with `WithPeerAt`. |
| `WithRewriterMock(t)` | Returns a `ProxyOption` + a `*RewriterMock`. Mock implements `Rewrite` / `RewriteErrorMessage` / `Optimize`; exposes `SeenSQL` / `SeenDynamicArgs` / `FailNext` / `SetAccessedTables`. |
| `WithMiniredis(t)` / `WithMiniredisHandle(t)` | In-process redis fake. The `Handle` variant returns the `*miniredis.Miniredis` so the test can `Close()` it mid-run (FailOpen testing). |
| `WithConfigMutator(m)` | Last-mile mutation of the generated `*config.Config` before the proxy boots (used for tightening health-check timings, etc.). |
| `WithExtraDatabases(...)` | Seed additional databases in NetworkState so `forward.Plugin` accepts `USE` of them. |
| `WithDatabasePermission(account, db, auth)` | Grant a signer permission to a database. |
| `WithPeerAt(indexerId, peer)` | Register `peer` as the IndexerInfo for `indexerId` on the in-memory NetworkState (agent Selector / forward.Plugin topology). |
| `WithLogicalDatabaseAt(database, indexerId)` | Mark a logical database as hosted on `indexerId` so `forward.Plugin.OnHello` can decide to pivot. |
| `WithIndexerID(id)` | Set the proxy's own `cfg.IndexerID`. `forward.Plugin` compares this against the resolved database's indexer to decide local-vs-peer. |
| `WithRelayKey(privateKeyHex)` | Set `RelayPrivateKeyHex` so the proxy builds a `RelaySigner` / `PeerSigner` for signing peer-relay JWS. |
| `WithCredentialReplace()` | Enable `CredentialReplaceEnabled`: after the credential plugin strips a `__peer__` envelope, the proxy substitutes real CH creds from the ckh_manager YAML before dialling upstream. |
| `ClickHouseCLI(t)` / `RunCLI(...)` / `RunCLIContext(...)` | Invoke the real `clickhouse` binary; tests skip when the binary is missing. |

[testcontainers-go]: https://golang.testcontainers.org/
