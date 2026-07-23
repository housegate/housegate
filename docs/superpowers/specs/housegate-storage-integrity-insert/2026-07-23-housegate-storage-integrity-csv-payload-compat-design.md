# HouseGate CSVWithNames Payload Compatibility Deferral

This note extends the INSERT-only storage-integrity design set:

- `2026-07-14-housegate-storage-integrity-state-root-contract-design.md`
- `2026-07-16-housegate-storage-integrity-signed-ingress-design.md`
- `2026-07-20-housegate-storage-integrity-staged-intake-design.md`
- `2026-07-20-housegate-storage-integrity-p1e-runtime-e2e-design.md`

## Decision

HouseGate server-side storage-integrity ingress remains **Native-only** for this
slice. It must reject `INSERT ... FORMAT CSVWithNames` until Relay has a
dedicated raw-CSV capture/extraction path.

The current Arbiter P1/MVP replay profile can decode `csv-with-names-v1`, but
HouseGate's relay hook does not currently capture bare CSV bytes. In the
ClickHouse native TCP path, `Relay.clientToUpstream` calls strict data hooks
with `pkt.Raw`: the full `ClientData` packet bytes, including the packet code
and native block framing. Feeding those bytes to `payloadexec.DecodeCSV` would
make the CSV decoder interpret protocol framing as the CSV header.

The opposite direction is not valid either: a client cannot send arbitrary bare
CSV through the current strict ClientData path, because the relay classifies
ClientData with `chproto.ClientDataPacketIsEmpty` / native Data-block parsing.
There is no raw text-payload extraction contract today.

## Current Admitted Forms

| SQL source form | Ingress result | Payload encoding |
|---|---|---|
| `INSERT ...` with streaming ClickHouse native TCP Data blocks | admitted | `clickhouse-native-data-v1` |
| `INSERT ... FORMAT Native` | admitted | `clickhouse-native-data-v1` |
| `INSERT ... FORMAT CSVWithNames` | rejected | none |
| `INSERT ... FORMAT CSV` | rejected | none |
| `INSERT ... VALUES` | rejected | none |
| `INSERT ... SELECT` / `WITH ... SELECT` | rejected | none |

Compressed query payloads remain rejected before admission.

## What Changed In This Slice

The code now keeps payload encoding explicit in the admission projection, but
the only encoding HouseGate ingress can currently produce is
`clickhouse-native-data-v1`. This avoids hard-coding the projection forever
while still failing closed for CSV.

The SQL classifier rejects `FORMAT CSVWithNames` with an explicit reason: raw
CSV payload capture is not available. Tests pin this behavior so a direct plugin
unit test with bare CSV text cannot accidentally claim end-to-end support.

## Future CSV Support Requirements

Real CSV support needs a separate relay design and implementation:

1. Identify a ClickHouse native TCP path where the incoming bytes are actually
   the raw `CSVWithNames` payload, not a native Data block.
2. Add a strict capture hook that extracts exactly those raw CSV bytes without
   packet-code/native-block framing.
3. Ensure `FORMAT CSVWithNames` cannot be admitted unless that capture path is
   active.
4. Thread `payload_encoding = csv-with-names-v1` to the staged SNode intake and
   require the prepared result to bind the same encoding.
5. Add a wire-level integration test through Relay, not only a direct plugin
   test.

Until those conditions are met, CSV remains a replay profile used by Arbiter
fixtures, not a HouseGate ingress payload format.
