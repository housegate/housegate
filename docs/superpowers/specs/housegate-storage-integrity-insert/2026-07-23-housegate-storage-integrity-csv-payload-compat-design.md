# HouseGate CSVWithNames Payload Compatibility Bridge

Last updated: 2026-07-29

This note extends the INSERT-only storage-integrity design set:

- `2026-07-14-housegate-storage-integrity-state-root-contract-design.md`
- `2026-07-16-housegate-storage-integrity-signed-ingress-design.md`
- `2026-07-20-housegate-storage-integrity-staged-intake-design.md`
- `2026-07-20-housegate-storage-integrity-p1e-runtime-e2e-design.md`

## Decision

HouseGate server-side storage-integrity ingress supports
`INSERT ... FORMAT CSVWithNames` only through an explicit materialization bridge:
Relay still captures the complete ClickHouse Native `ClientData` packets, then a
configured `StorageIntegrityPayloadMaterializer` decodes that Native capture
under the pinned table schema and emits deterministic `CSVWithNames` bytes.

This is intentionally not a raw text-CSV capture path. The ClickHouse native TCP
relay still classifies client input as Native `ClientData`; a client cannot send
arbitrary bare CSV through the strict data path. Feeding `pkt.Raw` directly to
`payloadexec.DecodeCSV` remains invalid because those bytes include the packet
code and Native block framing.

The general ingress safety rule is: `FORMAT CSVWithNames` is admitted only when a payload
materializer is configured. Without that bridge, ingress rejects the query
fail-closed instead of declaring CSV support and storing Native framing bytes as
CSV.

## Current Admitted Forms

| SQL source form | Ingress result | Payload encoding |
|---|---|---|
| `INSERT ...` with streaming ClickHouse native TCP Data blocks | admitted | `clickhouse-native-data-v1` |
| `INSERT ... FORMAT Native` | admitted | `clickhouse-native-data-v1` |
| `INSERT ... FORMAT CSVWithNames` with `StorageIntegrityPayloadMaterializer` | admitted | `csv-with-names-v1` |
| `INSERT ... FORMAT CSVWithNames` without materializer | rejected | none |
| `INSERT ... FORMAT CSV` | rejected | none |
| `INSERT ... VALUES` | rejected | none |
| `INSERT ... SELECT` / `WITH ... SELECT` | rejected | none |

Compressed query payloads remain rejected before admission.

The table above describes the reusable ingress plugin. The built-in Arbiter P1
production runtime is intentionally narrower: it requires
`StorageIntegrityPayloadMaterializer` at startup and accepts only the
`csv-with-names-v1` result. Implicit Native and `FORMAT Native` remain available
to custom admission consumers, but the built-in runtime rejects them before
payload spooling/upload because the current arbiter-core staged SNode accepts
CSVWithNames only.

The production CSV bridge also requires a non-zero captured ClickHouse client
revision. The stored CSV bytes no longer need revision-specific decoding, but
HouseGate needs that revision to decode the incoming Native `ClientData` before
emitting CSV.

## What Changed In This Slice

The code now keeps payload encoding explicit in the admission projection and can
produce two replay payload encodings:

- `clickhouse-native-data-v1` for implicit streaming Native INSERTs and
  `FORMAT Native`;
- `csv-with-names-v1` for `FORMAT CSVWithNames`, but only after
  `NativeCSVPayloadMaterializer` decodes the captured Native `ClientData` and
  re-emits schema-ordered CSVWithNames bytes.

The plugin receives the materializer through
`housegate.Options.StorageIntegrityPayloadMaterializer`. The materializer
receives `{statement_id, table_id, sql, payload_encoding, native_wire,
revision}` and returns final payload bytes plus the final encoding. Admission
hash and length are computed over the final materialized payload, not the
captured Native wire bytes.

Tests pin both sides of the bridge:

- `FORMAT CSVWithNames` without a materializer is rejected;
- a configured materializer receives the captured Native wire bytes and the
  published admission contains the materialized CSV payload;
- `NativeCSVPayloadMaterializer` output is accepted by `payloadexec.DecodeCSV`;
- `buildServer` wires `StorageIntegrityPayloadMaterializer` into the ingress
  plugin;
- built-in runtime startup rejects a missing CSV materializer;
- a Native admission is rejected before PayloadStore access when the runtime is
  pinned to `MaterializerCSV`.

## Remaining Non-Scope

The bridge does not add a bare raw-CSV Relay path. If HouseGate later needs to
support clients that truly send text CSV payload bytes rather than Native
`ClientData`, that remains a separate relay design:

1. Identify a ClickHouse native TCP path where the incoming bytes are actually
   the raw `CSVWithNames` payload, not a native Data block.
2. Add a strict capture hook that extracts exactly those raw CSV bytes without
   packet-code/native-block framing.
3. Ensure `FORMAT CSVWithNames` cannot be admitted unless that capture path is
   active.
4. Add a wire-level integration test through Relay for that raw text path.

The current bridge is sufficient for the Arbiter P1/MVP CSV replay profile
because the stored payload is already `csv-with-names-v1` before it reaches
PayloadStore / staged SNode intake.
