# HouseGate CSVWithNames Payload Compatibility Contract

This note extends the INSERT-only storage-integrity design set:

- `2026-07-14-housegate-storage-integrity-state-root-contract-design.md`
- `2026-07-16-housegate-storage-integrity-signed-ingress-design.md`
- `2026-07-20-housegate-storage-integrity-staged-intake-design.md`
- `2026-07-20-housegate-storage-integrity-p1e-runtime-e2e-design.md`

Those documents make Native payload capture the production target. This
compatibility slice does not change that direction. It adds an explicit
`CSVWithNames` payload mode so HouseGate can interoperate with the current
Arbiter P1/MVP data plane, whose SNode and replay paths still decode
`payloadexec.DecodeCSV`.

## Decision

HouseGate admits two payload-local INSERT encodings:

| SQL source form | Payload encoding | Revision requirement |
|---|---|---|
| `INSERT ...` with streaming ClickHouse client `Data` blocks, or `FORMAT Native` | `clickhouse-native-data-v1` | required, nonzero |
| `INSERT ... FORMAT CSVWithNames` | `csv-with-names-v1` | not required, must be `0` in HouseGate admission output |

All other INSERT forms remain rejected:

- `INSERT ... VALUES`
- `INSERT ... SELECT`
- `WITH ... INSERT`
- `FORMAT CSV`
- any other non-`Native` / non-`CSVWithNames` format
- compressed query payloads

The restriction to `CSVWithNames` is intentional. The replay executor's CSV
profile requires a header row and validates it against the pinned
`payloadexec.TableSchema`. Plain `FORMAT CSV` cannot provide that contract.

## Encoding Propagation

The ingress plugin classifies the signed SQL before admitting a statement and
stores the resulting payload encoding in the captured payload metadata. The
runtime projection carries it through:

```text
plugin.CapturedPayload.Encoding
  -> storageintegrity.AdmissionRecord.PayloadEncoding
  -> storageintegrity.StatementEnvelope.PayloadEncoding
  -> SourcePreparer.PreparedLocalResult.PayloadEncoding
```

The orchestrator's prepare/submit consistency check requires the prepared
source result to return the same payload encoding. This prevents a Native
payload from being silently decoded as CSV or a CSV payload from being silently
decoded as Native.

`StatementEnvelopeV2` in the current Arbiter proto has only
`payload_ref/payload_hash/payload_length`; it does not carry `payload_encoding`
or `revision`. HouseGate therefore keeps encoding as a HouseGate/SNode staged
intake contract for this compatibility slice. The Arbiter proto extension for
Native production E2E remains a separate cross-repo change.

## Revision Semantics

Native payloads must carry the ClickHouse client protocol revision. The Native
materializer uses that revision to decode captured `ClientData` bytes and fails
closed on revision `0`.

CSVWithNames payloads are text payloads and do not use the ClickHouse Native
protocol revision. HouseGate normalizes CSV admissions to revision `0`, and the
orchestrator does not require a nonzero revision for `csv-with-names-v1`.

## Non-Goals

This slice does not:

- make `FORMAT CSV` admissible;
- allow inline `VALUES` or `INSERT ... SELECT`;
- make CSV the long-term production payload profile;
- add `payload_encoding` / `revision` fields to `arbiter-proto`;
- implement SNode-side CSV preparation, which already exists in the current
  Arbiter P1/MVP path via `payloadexec.DecodeCSV`;
- implement Native SNode/Verifier materialization for Arbiter.

## Tests

The implementation pins the compatibility contract with:

- SQL classifier tests for Native vs `CSVWithNames`;
- ingress tests proving `FORMAT CSVWithNames` is admitted and captures exact
  payload bytes with `csv-with-names-v1`;
- envelope tests proving CSV does not require a Native revision;
- consistency tests proving encoding mismatch still fails closed;
- runtime projection tests proving CSV encoding is not overwritten as Native.
