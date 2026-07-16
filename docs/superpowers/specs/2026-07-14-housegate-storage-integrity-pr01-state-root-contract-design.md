# HouseGate Native Payload Materializer Contract

## Conclusion

This phase does not add a second Native state-root implementation. It narrows ClickHouse Native `ClientData` support to a `payloadexec.Materializer`.

Native code only decodes captured Native payloads into `payloadexec.Row`. Row-id injection, row canonicalization, part LtHash folding, partition commitments, schema roots, manifests, and state roots remain owned by the shared `pkg/replay/payloadexec.Executor`.

This resolves the P1c/P1e baseline alignment issue in the 2026-07-01 design: Native Data block support cannot be declared at ingress only, and HouseGate must not reimplement state-root semantics for Native payloads.

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

Shared executor helper:

```go
func PartitionIDForRow(
    schema payloadexec.TableSchema,
    values []any,
) (string, error)
```

`DecodeNativePayload` returns rows without `RowID`. `Materialize` injects row ids with `payloadexec.RowID` from `NetworkID`, `schema.TableID`, `statement_id`, and the statement-local global ordinal.

## Correctness Constraints

Native payloads must fail closed against the pinned `payloadexec.TableSchema`. Rejections include:

- empty protocol revision or empty payload;
- payloads that are not decodable Native `ClientData`;
- column names, column types, or column sets that do not match the pinned schema;
- reserved `_hg_row_id` in either payload or schema;
- prepared statement `TargetTableID` that does not match the pinned schema `TableID`;
- statements without `PayloadRef`, including mutation, DDL, or no-payload inputs.

Native rows must populate deterministic logical Native value bytes before they
enter the shared executor. Fixed-width scalar values use their Native physical
width, `String` uses content length, and `FixedString(32)` uses `32`. This byte
accounting is signed part metadata only; it does not participate in
`ComputeDataRoot` or `ComputeStateRoot`, and it does not try to reconstruct
packet framing or compression bytes.

Wire column order is not part of the state-root semantics. For the same logical rows, network id, table schema, statement id, and block/statement sequence, CSV and Native must produce the same state root through the shared `payloadexec.Executor`.

## Mapping To The 2026-07-01 Design

This phase mainly maps to these P1c/P1e baseline constraints:

- **Section 3.1 Client-side HouseGate**: `_hg_row_id` is currently injected by SNode through shared `payloadexec.RowID`; HouseGate must not reimplement the row-id formula.
- **Section 3.2 Server-side ingress sequence**: Native Data block support requires a separate payload encoding/replay profile; ingress cannot only declare it supported.
- **Section 3.2 Server-side ingress sequence**: HouseGate must not reimplement row canonicalization, row-id, part LtHash, schema root, or state-root assembly; it must call shared helpers.
- **Section 3.3 Source claim wiring constraints**: `SourceClaimRoot` is an absolute source view and must not be simplified to "previous safe + current INSERT". This phase therefore avoids a Native source-root helper.

## Acceptance Points

- Native and CSV produce the same `ComputedStateRoot`, partition commitments, and `PartRowLtHash` for the same logical rows through the shared executor.
- Native decoding validates against the pinned schema and rejects schema, type, target-table, and payload-class mismatches.
- Native affected parts carry deterministic nonzero `Bytes` metadata for nonempty rows.
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
