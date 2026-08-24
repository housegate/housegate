# PROXY GUIDE

## OVERVIEW
`pkg/proxy` owns the TCP accept loop and per-connection relay. It does not decide business policy; it drives codecs and hook chains at packet boundaries.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Connection lifecycle | `server.go` | Accept loop, session creation, `OnConnect`/`OnDisconnect`, shutdown drain. |
| Handshake flow | `relay.go` | ClientHello, OnHello chain, upstream hello, addendum, forward-pivot special case. |
| Client to upstream packets | `relay.go` | Query hook chain, typed query write, non-query splice, ClientData hook. |
| Upstream to client bytes | `relay.go` | Raw byte copy, first-byte EndOfStream/Exception heuristics. |
| Metrics observer | `observer.go` | Prometheus globals and wire-level packet/byte counters. |

## CONVENTIONS
- The dialer runs after `OnHello`, so route and credential plugins can mutate session state before upstream selection.
- Forwarded sessions use `PeerServerHelloRaw` from `RebindToPeer`; do not repeat the upstream hello exchange.
- Client side stays packet-by-packet with `ReadPacket`; upstream side switches to raw copying after handshake.
- Query plugin errors become synthetic ClickHouse Exceptions to the client.
- `OnClientData` is fail-open. Hook errors are logged and the raw packet still continues.
- `OnQueryComplete` is best-effort, driven by EndOfStream/Exception heuristics on upstream chunks.

## ANTI-PATTERNS
- Do not add policy decisions directly to relay when a plugin hook is the intended surface.
- Do not parse upstream packets fully in `upstreamToClient` without revisiting chunking and capture invariants.
- Do not assume every upstream Exception is observed by hooks.
- Do not make metrics plugin counters replace wire-level observer counters; they measure different surfaces.
- Do not change shutdown behavior without considering in-flight sessions and `ShutdownTimeout`.

## COMMANDS
```bash
bazel test //pkg/proxy:proxy_test
bazel test //pkg/proxy:proxy_test --test_arg=-test.v
bazel test //pkg/integration:integration_test --test_filter='Test.*Proxy|Test.*Routing|Test.*Exception' --test_output=errors
```

## NOTES
- `prometheus.MustRegister` globals in `observer.go` can panic on duplicate registration in unusual test hosts.
- The known plain `go test` flake is `TestServer_ConnLifecycleHooks_FireOnDialFailure`; Bazel is the authoritative check.
