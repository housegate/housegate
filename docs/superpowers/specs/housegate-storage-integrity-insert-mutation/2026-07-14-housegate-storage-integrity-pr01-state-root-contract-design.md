# HouseGate Native Payload Materializer Contract

## Conclusion

This phase does not add a second Native state-root implementation. It narrows ClickHouse Native `ClientData` support to a `payloadexec.Materializer`.

Native code decodes captured Native payloads into `payloadexec.Row` and invokes the shared `payloadexec.RowID` helper. The row-id formula, row canonicalization, part LtHash folding, partition commitments, schema roots, manifests, and state roots remain owned by shared replay code.

This resolves the P1c/P1e baseline alignment issue in the 2026-07-01 design: Native Data block support cannot be declared at ingress only, and HouseGate must not reimplement state-root semantics for Native payloads.

This phase also preserves the existing CSV replay profile. It does not silently replace CSV's wire-text partition ids with typed Native partition formatting. Runtime selection and enforcement of the Native payload encoding/executor profile remain owned by the C0 data-plane baseline and later ingress wiring.

## Scope Boundary

`NativeMaterializer` may:

- decode one or more concatenated ClickHouse Native `ClientData` blocks;
- validate column names, types, and target table against the pinned `payloadexec.TableSchema`;
- reorder wire columns into schema-declared order;
- derive partitions through shared `payloadexec.PartitionIDForRow`;
- inject `_hg_row_id` through shared `payloadexec.RowID` using the statement-local global row ordinal.

`NativeMaterializer` must not:

- derive schema roots, state roots, or source claim roots;
- construct genesis manifests or safe snapshot manifests;
- reimplement row canonicalization, part LtHash, or state-root assembly;
- simplify source claims to "previous safe + current INSERT".

The absolute source-claim frontier, unsafe write, RC lifecycle, promotion, and mutation replay remain later phases.

## Interfaces

Production entrypoint:

```go
type NativeMaterializer struct {
    NetworkID string
    Revision  int
}

func (m NativeMaterializer) Materialize(
    ctx context.Context,
    schema payloadexec.TableSchema,
    st replay.PreparedStatement,
) ([]payloadexec.Row, error)
```

Decode helpers:

```go
func DecodeNativePayload(
    schema payloadexec.TableSchema,
    revision int,
    payload []byte,
) ([]payloadexec.Row, error)

func ValidateNativePayloadDecodable(
    schema payloadexec.TableSchema,
    revision int,
    payload []byte,
) error
```

Shared typed-materializer helper:

```go
func PartitionIDForRow(
    schema payloadexec.TableSchema,
    values []any,
) (string, error)
```

`DecodeNativePayload` returns rows without `RowID`. `Materialize` injects row ids with `payloadexec.RowID` from `NetworkID`, `schema.TableID`, `statement_id`, and the statement-local global ordinal. The legacy CSV materializer intentionally does not call `PartitionIDForRow`; it retains its existing `"p_" + wireValue` partition contract until a new executor profile is selected explicitly.

## Correctness Constraints

Native payloads must fail closed against the pinned `payloadexec.TableSchema`. Rejections include:

- empty protocol revision or empty payload;
- payloads that are not decodable Native `ClientData`;
- column names, column types, or column sets that do not match the pinned schema;
- reserved `_hg_row_id` in either payload or schema;
- empty prepared statement `TargetTableID`, or one that does not exactly match the pinned schema `TableID`;
- statements without `PayloadRef`, including mutation, DDL, or no-payload inputs.

Native rows must populate deterministic logical Native value bytes before they
enter the shared executor. Fixed-width scalar values use their Native physical
width, `String` uses content length, and `FixedString(32)` uses `32`. This byte
accounting is signed part metadata only; it does not participate in
`ComputeDataRoot` or `ComputeStateRoot`, and it does not try to reconstruct
packet framing or compression bytes.

The admitted Native scalar and logical byte matrix is:

| Native type | Materialized Go value | Logical bytes per value |
|---|---|---:|
| `UInt8/16/32/64` | matching-width unsigned integer | `1/2/4/8` |
| `Int8/16/32/64` | matching-width signed integer | `1/2/4/8` |
| `Float32/64` | matching-width float | `4/8` |
| `String` | `string` | content length |
| `FixedString(32)` | 32-byte `[]byte` | `32` |
| `Bool` | `bool` | `1` |
| `Date` | `time.Time` | `2` |
| `DateTime` | UTC `time.Time` | `4` |
| `DateTime64` | UTC `time.Time` | `8` |

Other decoded Native scalar types, including `Int128`, fail closed even when the pinned schema declares the same unsupported type. `PartitionIDForRow` admits typed string/bytes, boolean, integer, and float values. Typed float partition formatting collapses `-0.0` to `0`, matching the shared row canonicalization profile, and canonicalizes all NaN payloads to `NaN`. Direct Date/DateTime partition keys are not defined by this MVP helper and fail closed; this phase does not invent ClickHouse partition-expression semantics.

Wire column order is not part of the state-root semantics. For canonical-compatible CSV partition values, the same logical rows, network id, table schema, statement id, and block/statement sequence produce the same state root through the shared `payloadexec.Executor`. Non-canonical legacy CSV wire values such as `001` keep their historical partition id (`p_001`) and state-root vector; they are not reinterpreted as Native typed values under the old profile.

## Mapping To The 2026-07-01 Design

This phase mainly maps to these P1c/P1e baseline constraints:

- **Section 3.1 Client-side HouseGate**: `_hg_row_id` is currently injected by SNode through shared `payloadexec.RowID`; HouseGate must not reimplement the row-id formula.
- **Section 3.2 Server-side ingress sequence**: Native Data block support requires a separate payload encoding/replay profile; ingress cannot only declare it supported.
- **Section 3.2 Server-side ingress sequence**: adding Native support must not mutate the behavior of snapshots pinned to the legacy CSV executor profile.
- **Section 3.2 Server-side ingress sequence**: HouseGate must not reimplement row canonicalization, row-id, part LtHash, schema root, or state-root assembly; it must call shared helpers.
- **Section 3.3 Source claim wiring constraints**: `SourceClaimRoot` is an absolute source view and must not be simplified to "previous safe + current INSERT". This phase therefore avoids a Native source-root helper.

## Acceptance Points

- Native and canonical-compatible CSV produce the same `ComputedStateRoot`, partition commitments, and `PartRowLtHash` for the same logical rows through the shared executor.
- Legacy CSV partition wire text and its complete state-root golden vector remain unchanged.
- Native decoding validates against the pinned schema and rejects schema, type, target-table, and payload-class mismatches.
- Empty `TargetTableID` is rejected by `NativeMaterializer` itself, not only by its current executor caller.
- Native affected parts carry deterministic nonzero `Bytes` metadata for nonempty rows.
- Every admitted Native scalar branch has a value, shared-row-hash, and byte-accounting golden test; a matching pinned `Int128` payload proves default-deny behavior.
- Multi-block Native payloads inject row ids with one statement-local global ordinal; block boundaries do not reset the ordinal.
- The scoped spec no longer describes an independent Native root stack.

## Out Of Scope

This phase does not implement:

- staged ingress;
- PayloadStore;
- unsafe write;
- RC lifecycle;
- absolute source frontier;
- promotion;
- bounded UPDATE/DELETE mutation replay.
- runtime payload-encoding/executor-profile selection and dispatch.
