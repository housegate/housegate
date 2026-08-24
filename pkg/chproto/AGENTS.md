# CHPROTO GUIDE

## OVERVIEW
`pkg/chproto` is the ClickHouse native TCP codec wrapper: packet decode/capture, zero-copy splice, addendum negotiation, compression helpers, and the current chunked-protocol workaround.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Packet read/decode contract | `codec.go` | `ReadPacket`, capture buffer, typed decode, raw packet preservation. |
| Zero-copy forwarding | `codec.go`, `packet.go` | Use `Splice` only for undecoded packets. |
| Client/server hello behavior | `hello.go`, `codec.go` | ServerHello raw bytes are preserved and chunked advertisements are rewritten. |
| Addendum and chunked primitives | `addendum.go`, `features.go` | Primitives exist, but relay still forces `notchunked`. |
| Query encode/decode | `query.go`, `query_codec.go` | Client query packets and compression state. |
| INSERT data inspection | `data.go`, `compression.go` | Used by LtHash plugin and integration tests. |

## CONVENTIONS
- One `Codec` owns one `proto.Reader` for one connection. Do not replace `Codec.r` or create a fresh `proto.Reader` over `c.br` mid-connection.
- `ReadPacket` resets capture at packet boundaries and relies on the 1-byte capture reader to avoid orphaning prefetched bytes.
- ServerHello must be echoed from `Packet.Raw`, not re-encoded from ch-go structs. Newer ClickHouse revisions append fields ch-go does not model.
- The upstream codec is read with `ReadPacket` only during handshake; after that relay uses `ReadRaw`.
- `NegotiateAddendum` is read-only. Sending the addendum upstream is the caller's job via `SendAddendum`.
- `Splice` rejects decoded packets by design; decoded packets need typed writers.

## ANTI-PATTERNS
- Do not let a modern client negotiate chunked framing until the relay wraps reads/writes with `ChunkedReader`/`ChunkedWriter` end to end.
- Do not remove `rewriteServerHelloChunkedToNotChunked` without changing relay framing.
- Do not consume buffered bytes behind ServerHello except through the capture path that preserves trailing fields.
- Do not make decode failures on non-critical packets fail-closed unless the packet loop contract changes.

## COMMANDS
```bash
bazel test //pkg/chproto:chproto_test
bazel test //pkg/proxy:proxy_test --test_filter='Test.*Addendum|Test.*Chunked|Test.*Codec'
```

## NOTES
- `ReadRaw` has few direct callers; changing it requires relay verification.
- Chunked helpers in `addendum.go` are not proof that the proxy currently supports chunked transport.
- Compression-aware data walking is scoped; many higher-level paths intentionally skip compressed blocks.
