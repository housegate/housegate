# Storage Integrity: Widening the Column-Type Profile

**Date:** 2026-08-25 **Status:** Proposed **Roadmap:** [closure roadmap](2026-08-25-storage-integrity-closure-roadmap.md) Spec Q. **Decided by:** the operator, in response to roadmap decision 11 — SI v1 widens its type profile rather than constraining devnet2's tables to scalars. **Remediates:** [Spec L D1](2026-08-19-storage-integrity-table-backpressure-hardening-design.md)'s column-type validation, which turned out narrower than the executors it was meant to describe. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §5.2, §5.3, §5.4; [multi-replica trust design](2026-06-10-multi-replica-trust-design.md) Appendix C. **Code base:** housegate `6fd56b8` (v0.11.0), arbiter-core `32b59a8` (v0.5.1), ch-go `v0.73.0-sentioxyz-20260629`. **Source of truth:** English version.

## 1. Problem

Roadmap decision 11 recorded that SI v1's column-type whitelist is `String`, `FixedString(N)`, `Bool`, `Float32/64` and `[U]Int8…64`, and that enforcing it at `snode.New` (Spec L D1) stops any node whose SI tables declare a `DateTime` — which is most real indexer tables. The decision taken is to widen the profile: **add `DateTime`, `Decimal`, `UUID` and `Nullable`.**

Measuring the code before designing the widening changed what the work is.

**1a — the temporal types are already supported everywhere except the validator (this is a defect, not a gap).** Three of the four components that have to agree already handle `Date`, `DateTime`, `DateTime(TZ)` and `DateTime64(P)`:

| Component | Temporal support | Evidence |
|---|---|---|
| Native payload decoder | yes | `pkg/replay/nativepayload/native.go:269-274` handles `*proto.ColDate`, `*proto.ColDateTime`, `*proto.ColDateTime64`, returning `time.Time` in UTC |
| Canonical row encoder | yes | `pkg/lthash/canonical.go`'s `encodeTime` dispatches on the `DateTime64` / `DateTime` / `Date` type prefixes and emits an absolute instant under `kindTime` |
| ClickHouse-backed executor | yes | `pkg/replay/chexec/materializer.go`'s `isTemporalColumnType` admits `Date`, `DateTime`, `DateTime(`, `DateTime64(` — under a comment stating it is "kept in sync with the Native decoder's admitted scalar matrix" |
| **Declaration validator** | **no** | `pkg/replay/payloadexec/column_types.go:56-92`'s `classifyColumnType` has no temporal branch |
| **Partition-value renderer** | **no** | `payloadexec`'s `partitionValueString` has cases for string, `[]byte`, bool, every int/uint width and both floats — and none for `time.Time` |

There is a **fifth** site, which the first pass of this table missed and the plan found: `partitionValueString` cannot render a `time.Time`, so a temporal *partition* column fails. That changes the shape of Phase 1 — restoring the temporal types without fixing it would convert a loud startup refusal into a late replay failure, which is strictly worse than the state being fixed. It is the same lesson as Q-D1 arriving one layer down: the count of components that have to agree is itself something to measure, not to assume.

So Spec L D1 did not describe the executors — it narrowed them. The whitelist it enforces is a fourth, independent list, and it is the strictest of the four. Widening the temporal types is therefore a **bug fix with no protocol consequence**: no new hash bytes, no new encoding, no profile change. It restores the set the pinned executor could already replay.

**1b — `UUID`, `Decimal` and `Nullable` are genuinely absent, but the decode side is close.** `proto.ColAuto.Infer` — which is what `decodeNativeDataBlock` uses via `results.Auto()` — already constructs columns for `ColumnTypeUUID`, `ColumnTypeDecimal` (dispatching on precision to `ColDecimal32/64/128/256`), the explicit `ColumnTypeDecimal32/64/128/256` spellings, and `ColumnTypeNullable` (by calling the inner column's `Nullable()` method). What is missing is downstream of construction: `nativeColumnValue`'s type switch has no case for any of them, `lthash.encodeValue` has no kind tag for a UUID, a decimal or a null, and `chexec` admits none of them.

**1c — `Nullable` is structurally harder than the other two.** `proto.ColNullable[T]` is generic, so a plain Go type switch would need one case per admitted inner type and would silently miss any inner type added later — the same "negative rule" failure mode as Spec N's SHOW classification. It does expose what a generic seam needs: exported `Nulls ColUInt8` and `Values ColumnOf[T]` fields plus `IsElemNull(i int) bool`.

**1d — the canonical type string is hashed, so spelling is consensus-critical.** `EncodeRow` frames `cols[i].Type` into the row element (`pkg/lthash/canonical.go:61`). `Decimal(18,4)` and `Decimal(18, 4)` therefore produce different row hashes for identical data. `CanonicalColumnType` already exists for exactly this reason — it normalizes `FixedString` widths — and every type added here needs its canonical spelling defined with the same care.

**1e — `FixedString` is admitted at 16,777,215 widths and decodes at exactly one.** This section originally guessed that `proto.ColAuto` contains no `FixedString` inference at all. Measured, that is wrong: `ColAuto` infers it through `inferGenerated` (`col_auto_gen.go:47-59`) at exactly seven widths — 8, 16, 32, 64, 128, 256, 512 — and any other width fails inference outright (`FixedString(17)` → `automatic column inference not supported`). Of those seven, `nativeColumnValue` handles only `*proto.ColFixedStr32`; widths 8, 16, 64 and 128 each measured `unsupported column type *proto.ColFixedStr<N>`. So `classifyColumnType` admits every width up to `0xFFFFFF` while the Native lane decodes exactly one. Same class as 1a with the polarity reversed, and in scope because Q-D1 is what makes it unrepeatable.

## 2. Goals / non-goals

**Goals.** Make one authority define the admitted type set, consumed by the validator and by every executor, so the four lists cannot drift again. Restore the temporal types. Add `UUID`, `Decimal` and `Nullable` end to end — decode, canonical encoding, ClickHouse read-back, validation. Do it before devnet2, because the profile identity cannot change once a chain is running.

**Non-goals.** `Array`, `Map`, `Tuple`, `LowCardinality`, `Enum8/16`, `IPv4/IPv6`, and nested combinations beyond `Nullable(<scalar>)`. Each needs its own canonical encoding decision and none is required by the tables that motivated this. `Nullable(Nullable(T))` is not legal ClickHouse and is not modelled. Changing what `settings_hash` or `schema_hash` cover. Retrofitting a running chain — see D4.

## 3. Decisions

### Q-D1 — one authority, consumed by all four components

The admitted type set is defined once, in `pkg/replay/payloadexec`, as a table mapping a canonical type spelling to (a) its canonical form, (b) the Go value type the decoders must produce, and (c) the `lthash` kind tag its values encode under. `classifyColumnType` / `SupportedColumnType` / `CanonicalColumnType`, `nativepayload.nativeColumnValue`, `chexec.supportedColumnType` and `chexec.newScanDest` all derive from it rather than each carrying a list.

**The proof that they agree is a test, not a convention:** one table-driven test enumerates the authority and asserts, for every entry, that the Native decoder produces the declared Go type, that `chexec` admits it and scans it into the same Go type, and that `lthash.encodeValue` accepts that Go type. A type in the authority that any component cannot handle fails the build. This is what makes 1a and 1e unrepeatable; without it, widening the list just moves the drift.

**The test lives in an internal test file in `package chexec`**, because the natural home is a cycle: `payloadexec` cannot import `nativepayload` or `chexec`, and `chexec` is the only package that already sees all four.

### Q-D2 — Phase 1: temporal types, no protocol change

`Date`, `Date32`, `DateTime`, `DateTime(<tz>)` and `DateTime64(P)` (and `DateTime64(P, <tz>)`) enter the authority. Nothing else changes: the decoder, the row encoder and the ClickHouse executor already handle them, and no new bytes enter any digest.

**`Date32` is Phase 2, measured.** `ColAuto` infers it to `*proto.ColDate32` and `nativeColumnValue` then fails with `unsupported column type *proto.ColDate32`; `chexec`'s `isTemporalColumnType` and `derefScan` exclude it too. The worry this spec originally raised about `encodeTime`'s `Date` prefix turned out to be unfounded — measured correct at `1900-01-01` (`kindTime` followed by little-endian `int64(-25567)`), because `ColDate32` values are always UTC midnight and so divide exactly even when negative. Adding it is a capability addition, so it belongs with the Phase 2 bump.

**Timezone spelling.** `DateTime('UTC')` is accepted and canonicalized only for whitespace — the timezone is *not* stripped. Stripping it would make the stored canonical type disagree with what ClickHouse reports for the physical column, which the verify-only DDL mode reads as drift. The value encoding is already timezone-independent (`encodeTime` emits an absolute instant), so two deployments that declare the same instant semantics with different spellings get different schema hashes, which the existing `schema_hash` binding is exactly the mechanism for catching.

### Q-D3 — Phase 2: `UUID`, `Decimal`, `Nullable`, under new kind tags

New `lthash` kind tags are appended after the existing six (`kindInt`…`kindTime`): `kindUUID`, `kindDecimal`, `kindNull`. **This does not change any existing row's bytes** — every currently-admitted type keeps its exact encoding — so the `housegate-row-mvp-v0` domain constant does **not** bump and no existing safe snapshot is invalidated.

Canonical value encodings:

- **`UUID`** — the 16 bytes in ClickHouse's storage order, under `kindUUID`. A distinct tag rather than reusing `kindString` because the profile's stated rule is that kind tags keep differently-typed values from colliding byte-wise, and a 16-byte `String` would otherwise share an encoding.
- **`Decimal(P,S)`** and the explicit `Decimal32/64/128/256(S)` spellings — the **raw scaled integer**, two's-complement, little-endian, at the physical width the precision selects (4/8/16/32 bytes), under `kindDecimal`. Never a float and never a decimal string: both reintroduce a rounding decision into a consensus digest.

  **`Decimal` needs a block-header change before any of that is reachable, and the obvious shortcut is unsound.** `Results.decodeAuto` reads the wire's verbatim column type but stores the *inferred* column, and `ColDecimal64.Type()` returns the bare `"Decimal64"` — the scale is gone. `nativepayload/native.go:172` compares that against the schema's `Decimal(18, 4)` with a plain `!=`, so a Decimal payload is rejected one stage *before* `nativeColumnValue` ever runs. Relaxing that comparison would be worse than leaving it: `Decimal(18,4)` and `Decimal(18,2)` both infer to `ColDecimal64`, so the **payload's** scale would go entirely unchecked while `lthash` frames the **schema's** scale into every row hash — a payload at scale 2 accepted against a schema declaring scale 4, and hashed as though it were scale 4. This spec's original claim that the scale is already covered by the framed type string is true of the schema side and false of the payload side, and the gap between them is exactly what `schema_hash` exists to bind. HouseGate therefore takes ownership of the block header decode so the verbatim wire type survives to the comparison. Feasibility is measured, not assumed: a hand-rolled `proto.Result` preserved `DateTime64(3, 'UTC')` verbatim, and `clickhouse-go/lib/proto/block.go:135` writes the declared `Decimal(P, S)` on the wire.
- **`Nullable(T)`** — a null encodes as the single byte `kindNull`; a non-null encodes exactly as bare `T` would. So a nullable column carrying no nulls hashes identically to the same data in a non-nullable column of the same declared type, which is the property that makes the addition auditable.

`Decimal512` is **excluded** even though ch-go supports it: ClickHouse's own `Decimal(P,S)` grammar tops out at P=76 (Decimal256), and admitting a width the DDL grammar cannot express would be an unreachable branch in a consensus path.

### Q-D4 — the profile identity bumps, and that is why this lands before devnet2

The accepted type set is part of what `ExecutorProfileID` means. `replay.Verifier` enforces `snap.ExecutorProfileID == job.ExecutorProfileID` (`pkg/replay/verifier.go:87`), where `snap` is the **previous safe snapshot** — so changing the profile mid-chain makes every job unchainable from the snapshot before it. Bumping it is free now, because no chain is running; after devnet2 it is a hard fork.

Phase 1 does **not** bump: it admits types the pinned executor already replays, so a verifier on the old profile computes identical roots. Phase 2 **does** bump, because a verifier on the old binary meeting a `UUID` column would fail to decode. That failure is already the safe one — an unsupported type is a pre-receipt error, which per Appendix C.4 is a local refusal to attest and not a signed mismatch — but the profile bump makes the refusal explicit rather than incidental.

**There is no constant to bump yet, and the gate it leans on is untested.** Every `ExecutorProfileID` occurrence in housegate is a struct field or a test literal; the string itself is chosen by arbiter-core / sentio-node. Phase 2 has to *create* the constant before it can bump it. And `verifier.go:87`'s `snap.ExecutorProfileID != job.ExecutorProfileID` — the check this whole decision rests on — has **no test**; `verifier_test.go:326` covers the sibling check at `:331` and reads enough like it to be mistaken for coverage. Phase 2 writes that test first, because a gate nothing exercises is a comment.

Consequence for sequencing: **Phase 2 must land before the first devnet2 chain**, and the plan states that as a gate rather than a preference.

### Q-D5 — canonical spellings are defined explicitly, with round-trip proof

Every admitted type gets its canonical spelling written down and enforced by `CanonicalColumnType`: whitespace normalized, no leading `+` or leading zeroes in numeric parameters, parameters rendered in base 10. `Decimal(18,4)`, `DateTime64(3)`, `DateTime64(3, 'UTC')`, `FixedString(16)`, `Nullable(UInt64)`.

Each spelling is proved by a round trip: declare it, create the table in ClickHouse, read the type back from `system.columns`, and assert it equals the canonical form. A spelling that ClickHouse normalizes differently from us is a drift bug waiting to happen in verify-only DDL mode, and the round trip is the only way to find it.

The examples above are **deliberately not** a decision: this spec's first draft wrote `DateTime64(3, 'UTC')` with a comma-space and `Decimal(18,4)` without, in the same sentence — exactly the inconsistency that produces two different row hashes for identical data. `ColumnType.With` joins with a comma-space, so ch-go reconstructs the spaced form regardless of what crossed the wire. `Decimal`'s canonical form must follow whatever `system.columns` reports and stays **undecided until measured**.

### Q-D6 — `Nullable` is decoded through a generic seam, not a type switch

`nativeColumnValue` handles nullable columns by asserting a narrow interface (`IsElemNull(i int) bool`) and reaching the inner `Values` column, then recursing into the ordinary per-type dispatch for the non-null case.

Measured: `IsElemNull` **is** reachable by a plain interface assertion, but the inner column is **not** — `ColNullable[T].Values` is a generic field and `col_nullable.go` exposes no non-generic accessor. One reflection call (`reflect.ValueOf(col).Elem().FieldByName("Values").Interface().(proto.ColResult)`) is unavoidable short of forking ch-go. It is scoped to that single exported-field lookup and its result recurses into the existing non-generic dispatch, so the property this decision is actually about — no per-instantiation type switch, no silent `default` for a future inner type — survives intact. A per-instantiation type switch is rejected: it would need one case per inner type and would silently drop to `default` for any inner type added later — the negative-rule failure mode Spec N D2 exists to eliminate.

Q-D1's authority test is what proves the seam is complete: it enumerates `Nullable(T)` for every scalar `T` in the authority and asserts each decodes, encodes and reads back.

### Q-D7 — `FixedString` widths are aligned to whatever is actually decodable

1e measured it: seven widths infer, one decodes. The two repair directions are not interchangeable, and only one composes with Q-D4. Teaching the decoder widths 8/16/64/128/256/512 is a **capability addition**, which by Q-D4's own logic requires the profile bump — and Phase 1 must not bump. Narrowing the authority to `FixedString(32)` is a **capability reduction**, safe in both directions: a verifier on the old binary still handles everything the new one admits.

So: **Phase 1 narrows the authority to `FixedString(32)`; Phase 2 widens it to all seven.** Narrowing is a breaking change for any declaration at another width — acceptable only because no chain is running and SI is not in production, the same window Q-D4 depends on. It breaks one in-tree fixture, `pkg/integration/chreplay_test.go:299`'s `FixedString(10)`, which Phase 1 updates. Silently leaving the validator wider than the decoder is the one outcome this decision forbids.

## 4. Testing / acceptance

1. **The authority test (Q-D1)** — table-driven over every admitted type: Native decode produces the declared Go type; `chexec` admits and scans into the same type; `lthash.encodeValue` accepts it. Must fail if any entry is added to the authority without all four components handling it.
2. **Phase 1 is a pure widening** — a test asserting that row hashes for the existing scalar types are byte-identical before and after, and a `DateTime` column that fails validation before the change and passes after.
3. **Executor equivalence** — the existing in-process-vs-ClickHouse equivalence test (`pkg/integration/chreplay_test.go`) extended to cover every newly admitted type, so the two executors are proved equal on the new set rather than assumed. This is more `chexec` work than §4's first draft implied: clickhouse-go's scan destinations are `uuid.UUID`, `decimal.Decimal` and `**T`, none of which is the canonical value type, so the authority needs a second `ScanGoType` column plus an insert-side inverse, and `shopspring/decimal` becomes a direct module dependency.
4. **`Nullable` null/non-null identity** — a nullable column with no nulls hashes identically to the same data in a non-nullable column of the same declared type; a column with nulls does not.
5. **`Decimal` exactness** — values that a float round trip would corrupt (large `Decimal128`/`Decimal256`, and scales at the precision limit) hash correctly and read back exactly.
6. **Canonical spelling round trip (Q-D5)** — declare, CREATE, read back from `system.columns`, assert equality, for every admitted type.
7. **Profile gate (Q-D4)** — a test asserting that a job whose profile ID differs from the previous snapshot's is refused, so the chaining constraint is enforced rather than documented.
8. **arbiter-core** picks up the widened validator through the housegate bump and its own suite stays green.

## 5. Delivery

Two phases, and Phase 1 does not wait for Phase 2.

**Phase 1** (temporal types + Q-D1's authority and its test) ships with Spec O's housegate release, because it is what unblocks the sentio-node bump — without it, the rollout stops at any table declaring a `DateTime`. It is a bug fix with no protocol consequence, so it carries no migration cost.

**Phase 2** (`UUID`, `Decimal`, `Nullable`, profile-ID bump) ships as its own release before the first devnet2 chain. If devnet2 is about to start and Phase 2 is not ready, the correct call is to delay devnet2 rather than to ship Phase 1's profile and bump later — D4 explains why.

Q-D7 lands with whichever phase the measurement in 1e assigns it to.

## 6. Out of scope / recorded debt

- `Array`, `Map`, `Tuple`, `LowCardinality`, `Enum8/16`, `IPv4/IPv6`, and nesting beyond `Nullable(<scalar>)`. Each needs its own canonical encoding; `Array` in particular needs an ordering decision inside the element, which is a real design question rather than a table entry.
- `Decimal512`, per Q-D3.
- The **production** row profile (`housegate-row-v1`, keyed by stable column IDs rather than names) remains future work; this spec extends the MVP profile only.
- Whether `ExecutorProfileID` should carry a structured capability set rather than an opaque string, so a verifier can report *which* type it cannot handle instead of refusing wholesale.
