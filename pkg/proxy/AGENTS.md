# PROXY GUIDE

## OVERVIEW
`pkg/proxy` owns the TCP accept loop and per-connection relay. It does not decide business policy; it drives codecs and hook chains at packet boundaries.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Connection lifecycle | `server.go` | Accept loop, session creation, `OnConnect`/`OnDisconnect`, shutdown drain. |
| Handshake flow | `relay.go` | ClientHello, OnHello chain, upstream hello, addendum, forward-pivot special case. |
| Client to upstream packets | `relay.go` | Query hook chain, typed query writes, non-query `WriteRawPacket` forwarding, and ClientData hooks. |
| Upstream to client packets | `relay.go` | Packet-framed forwarding, decoded terminal lifecycle, and the legacy opaque-result fallback. |
| Metrics observer | `observer.go` | Prometheus globals and wire-level packet/byte counters. |

## CONVENTIONS
- The dialer runs after `OnHello`, so route and credential plugins can mutate session state before upstream selection.
- Forwarded sessions use `PeerServerHelloRaw` from `RebindToPeer`; do not repeat the upstream hello exchange.
- Both directions stay packet-by-packet with `ReadPacket`; cross-leg raw packets go through the destination codec's `WriteRawPacket` so independently negotiated chunked framing is preserved.
- Query plugin errors become synthetic ClickHouse Exceptions to the client.
- Legacy `OnClientData` observers are fail-open: hook errors are logged and the raw packet continues. `OnClientDataStrict` and `OnQueryInputCompleteStrict` are separate admission hooks whose errors fail closed before the commit boundary.
- A decoded EndOfStream fires `OnQuerySuccess` for a non-cancelled active query only after any deferred-INSERT sample and terminator preconditions are satisfied, then completes its active/deferred lifecycle. A premature deferred EndOfStream fails closed without success. A decoded Exception fires `OnException`, never success, and calls `OnQueryComplete` only when an active or deferred query owns that terminal. Terminal hooks finish before the terminal packet reaches the client.
- An ordinary non-chunked result that returns `ErrUnsupportedResultType` may enter the one-way opaque fallback. Its exact-query-id process probe supplies cleanup only, never success, and the connection rejects any later Query because the original EndOfStream was not framed. Commit-gated and chunked paths never use this fallback.
- Relay allows only one upstream query in flight; a new Query before a framed EndOfStream or Exception is rejected rather than forwarded.

## ANTI-PATTERNS
- Do not add policy decisions directly to relay when a plugin hook is the intended surface.
- Do not bypass the packet-framed upstream loop or infer lifecycle from TCP fragmentation, coalescing, or payload bytes.
- Do not dispatch success from the opaque-result completion probe; it proves upstream execution cleanup, not that the terminal crossed the original connection.
- Do not let commit-gated queries or chunked downstreams enter raw opaque mode. Once an ordinary legacy fallback enters opaque mode, never resume packet parsing or reuse that connection; the completion probe is cleanup-only and any later Query closes fail-closed.
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
