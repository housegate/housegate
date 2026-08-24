# CHPROTO GUIDE

## OVERVIEW
`pkg/chproto` is the ClickHouse native TCP codec wrapper: packet decode/capture, cross-leg packet reframing, addendum negotiation, independently terminated chunked transport, and compression-aware Native-block walking.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Packet read/decode contract | `codec.go` | `ReadPacket`, capture buffer, typed decode, raw packet preservation. |
| Packet forwarding | `codec.go`, `packet.go` | `Packet.Raw` preserves an unframed packet; send it to another codec with `WriteRawPacket`. |
| Client/server hello behavior | `hello.go`, `codec.go` | ServerHello raw bytes are preserved and rewritten to Housegate's revision and chunked capabilities. |
| Addendum and chunked transport | `addendum.go`, `features.go` | Client and upstream legs negotiate independently; codecs enable chunked reads/writes only after the addendum. |
| Query encode/decode | `query.go`, `codec.go` | Client query packets and per-query compression state. |
| Native data boundaries | `compress.go`, `codec.go` | ClientData empty detection plus compressed/uncompressed Data and TableColumns walking. |

## CONVENTIONS
- One `Codec` owns one `proto.Reader` for one connection. Do not replace `Codec.r` or create a fresh `proto.Reader` over `c.br` mid-connection.
- `ReadPacket` resets capture at packet boundaries and relies on the 1-byte capture reader to avoid orphaning prefetched bytes.
- ServerHello must be echoed from `Packet.Raw` through the destination codec's `WriteRawPacket`, not re-encoded from ch-go structs. Newer ClickHouse revisions append fields ch-go does not model.
- `ReadPacket` is the normal read path for the whole connection in both directions. A negotiated upstream chunk uses ClickHouse's `finishChunk` boundary as the authoritative server-packet boundary, including when a Native value is opaque to Housegate.
- `NegotiateAddendum` is read-only. Sending the addendum upstream is the caller's job via `SendAddendum`.
- Cross-leg forwarding must use the destination codec's `WriteRawPacket`, which applies that leg's framing. `Splice` writes directly to an `io.Writer` and is safe only when that destination explicitly expects unframed packet bytes; it is unsafe even on the same connection after chunked sending is enabled because it bypasses the codec's `ChunkedWriter`. It is unused in production and remains only for tests.

## ANTI-PATTERNS
- Do not derive server-packet boundaries from raw TCP reads. `ReadRaw` is reserved for the one-way legacy fallback after `ErrUnsupportedResultType` on an ordinary non-chunked result; once entered, that codec never returns to packet parsing.
- Do not couple client and upstream chunked negotiation. Housegate terminates and reframes the two legs independently, and advertises downstream capabilities through `rewriteServerHelloForProxy`.
- Do not consume buffered bytes behind ServerHello except through the capture path that preserves trailing fields.
- Do not send a chunked opaque Native packet into the legacy raw fallback: `finishChunk` already bounds it, so it is forwarded normally without decoding the value. An unsupported commit-gated result on a legacy non-chunked leg has no such authoritative boundary and must fail closed.

## COMMANDS
```bash
bazel test //pkg/chproto:chproto_test
bazel test //pkg/proxy:proxy_test --test_filter='Test.*Addendum|Test.*Chunked|Test.*Codec'
```

## NOTES
- `ReadRaw` is a terminal fallback API, not a general relay primitive; changing it requires legacy opaque-result and connection-reuse verification.
- The proxy caps negotiated protocol revision at `54470`; raise it only after every newly enabled packet field and layout is modeled and tested.
- Compression-aware data walking is scoped; many higher-level paths intentionally skip compressed blocks.
