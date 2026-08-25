# Storage-Integrity Column-Type Profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the four independently-drifting column-type lists (`payloadexec.classifyColumnType`, `nativepayload.nativeColumnValue`, `chexec.supportedColumnType`/`newScanDest`, `lthash.encodeValue`) with one authority in `pkg/replay/payloadexec` that all four derive from, restore the temporal types the executors already replay (Phase 1, unblocks Spec O), and add `UUID` / `Decimal` / `Nullable` / `Date32` / the remaining `FixedString` widths end to end under new `lthash` kind tags with an `ExecutorProfileID` bump (Phase 2, gates devnet2).

**Architecture:** One table, four derivations, one test that proves they agree. The authority is a Go table in `pkg/replay/payloadexec/column_types.go` keyed by canonical type spelling, carrying: the canonical form, the Go value type every decoder must produce, the `lthash` kind tag it encodes under, and the type string a decoded ch-go column reports on the wire. `classifyColumnType` / `SupportedColumnType` / `CanonicalColumnType` / `parseValue` read it directly; `nativepayload` and `chexec` read it through exported accessors. Above them sits one table-driven cross-component test in `package chexec` (the only package that already imports all of `payloadexec`, `nativepayload` and `lthash` without a cycle) that enumerates every authority entry and asserts a real Native encode→decode round trip, `chexec` admission and scan-destination identity, and `lthash` acceptance. An entry no component handles fails the build. Phase 1 is a strict subtraction-and-restoration: no new kind tag, no new digest byte, no profile bump, proved by a byte-level golden of every currently-admitted type. Phase 2 appends three kind tags after the existing six, adds a housegate-owned Native block-header decoder so the payload's declared `Decimal(P, S)` scale is actually bound, and bumps the profile identity.

**Tech Stack:** Go 1.26 + Bazel 9.1.0 (Bzlmod), ch-go fork `v0.73.0-sentioxyz-20260629`, clickhouse-go fork `v2.47.0-sentioxyz-20260629`, ClickHouse 25.8 in docker for `//pkg/integration`.

**Spec:** `docs/superpowers/specs/2026-08-25-storage-integrity-column-type-profile-design.md` (Spec Q). Roadmap: `docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md` §3 (Q Phase 1 blocks Spec O; Q Phase 2 blocks devnet2) and §4 decision 11. Remediates: `docs/superpowers/specs/2026-08-19-storage-integrity-table-backpressure-hardening-design.md` Spec L D1.

**Working directory for every task:** `/Users/uranuswch/Dev/housegate/hg-specq` (branch `feature/si-column-type-profile`). Do not use `/Users/uranuswch/Dev/housegate/housegate` — sibling agents hold it.

---

## Measured facts

Spec Q §1e, §3 Q-D2 and §3 Q-D6 each said "measure, do not infer". The measurements were executed against the pinned forks before this plan was written, with throwaway `zz_measure*_test.go` files in `pkg/replay/nativepayload` (encode a one-column Native `ClientData` block via `proto.Block.EncodeBlock`, decode it through `nativepayload.Decode`, and separately inspect `proto.ColAuto.Infer`). The files were deleted; Task 1 re-establishes every result below as a committed regression pin, because all of them are properties of a *dependency* and a ch-go bump can silently change them.

| # | Question | Measured answer |
|---|---|---|
| **M1** | Does the Native lane decode `FixedString(N)` for N != 32? | **No — and the reason is not what Spec Q §1e guessed.** `proto.ColAuto.Infer` *does* infer `FixedString`, via `inferGenerated` (`col_auto_gen.go:47-59`), at exactly seven discrete widths: **8, 16, 32, 64, 128, 256, 512**. Any other width fails inference outright: `FixedString(17)` → `automatic column inference not supported for "FixedString(17)"`. Of the seven that infer, `nativeColumnValue` handles only `*proto.ColFixedStr32`; widths 8/16/64/128 measured `unsupported column type *proto.ColFixedStr<N>`. So the Native lane decodes **exactly `FixedString(32)`**, while `classifyColumnType` admits `0 < N <= 0xFFFFFF` — a 16.7-million-wide validator over a one-element decoder. |
| **M2** | Does the Native lane decode `Date32`? | **No.** `ColAuto.Infer("Date32")` builds `*proto.ColDate32` fine, but `nativeColumnValue` has no case → `unsupported column type *proto.ColDate32`. `chexec.isTemporalColumnType` also excludes it and `derefScan`'s `*time.Time` branch would fall to its `default` error. Per Q-D2's own instruction, **`Date32` moves to Phase 2.** Separately measured and confirmed *good* news: `lthash.encodeTime`'s `HasPrefix(col.Type, "Date")` branch handles `Date32` correctly including pre-1970 (`1900-01-01` → `kindTime ‖ int64(-25567)` LE), because `ColDate32` values are always UTC midnight and `t.Unix()/86400` divides exactly for negative multiples. `Date` and `Date32` on the same calendar day produce identical value bytes and are separated only by the framed type string — which is correct under the profile's rules. |
| **M3** | Is the `Nullable` seam reachable without reflection? | **Half.** `ColAuto.Infer` produces `*proto.ColNullable[T]` for every scalar tried (`UInt64`, `String`, `Bool`, `Float64`, `FixedString(16)`, `FixedString(32)`, `Date`, `Date32`, `DateTime('UTC')`, `DateTime64(3, 'UTC')`, `UUID`, `Decimal(38,4)`), and `IsElemNull(int) bool` **is reachable by a plain non-generic interface assertion** — no reflection. The inner column is **not**: `ColNullable[T].Values` is a `ColumnOf[T]` *field*, with no non-generic accessor method anywhere in `col_nullable.go`. Measured working alternative: `reflect.ValueOf(col).Elem().FieldByName("Values").Interface().(proto.ColResult)` succeeds (`CanInterface=true`, exported field) and yields `*proto.ColUInt64` / `*proto.ColStr` with the right `Type()`. **Reflection is unavoidable without forking ch-go.** It is scoped to exactly one exported-field lookup per nullable column per block — not per row — and its result is an ordinary `proto.ColResult` that recurses into the existing non-generic dispatch, so Q-D6's "narrow interface, no per-instantiation type switch" property survives intact. |

Two things the measurements found that Spec Q does not model at all. Both are load-bearing.

**M4 — `Decimal` cannot survive `nativeBlockColumnPositions` today, and its scale is not bound.** `nativeBlockColumnPositions` compares the block's reported type string against the schema's with plain `!=` (`native.go:172`). The block's reported type comes from `rc.Data.Type()`, and `Results.decodeAuto` stores the *inner* inferred column, so a `Decimal(18, 4)` column reports **`Decimal64`** and a `Decimal(38, 4)` reports **`Decimal128`** — measured. Both fail the equality check before `nativeColumnValue` is ever reached. Worse, the downcast is lossy: `Decimal(18,4)` and `Decimal(18,2)` both report `Decimal64`, so relaxing the check to a canonicalized equivalence (the obvious fix) would leave the payload's **declared scale unbound** while `lthash` frames the *schema's* scale into every row element — precisely the executor/payload divergence SI exists to prevent. Measured fix: housegate's own `proto.Result` implementation that reads the column name and raw type string itself before calling `ColAuto.Infer` preserves the exact declared spelling (verified: `DateTime64(3, 'UTC')` survives verbatim through a hand-rolled `DecodeResult`), and `clickhouse-go/lib/proto/block.go:135` writes `PutString(string(c.Type()))` where `Decimal.Type()` returns the declared `Decimal(P, S)` (`lib/column/decimal.go:70`, parsed at `:38-56`) — so the scale really is on the wire and can be bound. **Phase 2 must own the block header decode.** See Task 12.

**M5 — a temporal partition column is a Phase 1 blocker Spec Q never mentions.** `payloadexec.partitionValueString` (`executor.go:427-469`) has no `time.Time` case. Measured: `PartitionIDForRow` on a `PARTITION BY <Date column>` schema returns `partition column "d": unsupported partition value type time.Time`, for both `Date` and `DateTime('UTC')`. Restoring the temporal types to the validator without this makes such a table pass startup validation and then fail at replay — turning a loud startup refusal into a late, per-statement failure, which is strictly worse. See Task 7.

---

## Global constraints

Copied from Spec Q and the repo contract. Every task's requirements implicitly include this section.

- **Bazel is the test ground truth.** `bazel build //...`, `bazel test //...`. Plain `go test ./pkg/replay/... ./pkg/lthash/...` is fine for iteration but a task is not done until its Bazel target is green. Run `bazel run //:gazelle` after adding any file, and `bazel mod tidy && bazel run //:gazelle` if a dependency changes.
- **`//pkg/integration` targets are `manual`-tagged** and are not reached by `bazel test //...`. Any new docker-bound target must be added to the explicit list in `.github/workflows/ci.yml:122-127`, or it never runs in CI.
- **English only** for identifiers, comments, log messages and operator-facing error strings.
- **The canonical type string is hashed.** `lthash.EncodeRow` frames `cols[i].Type` into every row element (`canonical.go:61`) and `payloadexec.tableSchemaHash` frames it again (`executor.go:699-714`). Any change to a canonical spelling changes both the row hash and the schema hash. Canonical spellings are frozen by Task 8's golden and Task 18's docker round trip.
- **Kind tags are append-only.** `kindInt=1, kindUint=2, kindFloat=3, kindString=4, kindBool=5, kindTime=6` (`pkg/lthash/canonical.go:29-36`). Phase 2 appends `kindUUID=7, kindDecimal=8, kindNull=9`. Renumbering any of the six is forbidden and Task 13 asserts it.
- **`housegate-row-mvp-v0` does not bump.** Spec Q Q-D3: appending kind tags changes no existing row's bytes, so the `lthash` canonical domain constant stays.
- **Phase 1 must be independently shippable.** Spec O's sentio-node bump stops at the first table declaring a `DateTime`. Part A + Part B + Part C are the release Spec O cuts; nothing in them may depend on Part D or E.
- **Phase 2 must land before the first devnet2 chain** (Q-D4). `replay.Verifier` requires `snap.ExecutorProfileID == job.ExecutorProfileID` (`verifier.go:87`), so the bump is free now and a hard fork afterwards.
- **Every new guard ships with a step proving it fails against the unfixed code** (roadmap §4 decision 9). Each task below names the expected pre-fix failure text.
- **One commit per task**, conventional-commit prefixes (`feat(payloadexec):`, `fix(nativepayload):`, `test(chexec):`, `chore(ci):`). Explicit `git add <paths>`; never `git add -A`.
- **Markdown: no hard line-wrapping**, one paragraph per line.

## File map

| Area | Create | Modify |
|---|---|---|
| authority | `pkg/replay/payloadexec/column_profile.go` (+`column_profile_test.go`) | `pkg/replay/payloadexec/column_types.go`, `column_types_test.go`, `executor.go` (`parseValue`, `parseFixedString`, `partitionValueString`), `exports.go` |
| Native lane | `pkg/replay/nativepayload/block.go` (+`block_test.go`, Phase 2) | `pkg/replay/nativepayload/native.go`, `native_test.go`, `BUILD.bazel` |
| CH lane | — | `pkg/replay/chexec/materializer.go`, `materializer_format_test.go`, `BUILD.bazel` |
| cross-component test | `pkg/replay/chexec/column_profile_authority_test.go` | `pkg/replay/chexec/BUILD.bazel` |
| row encoding | — | `pkg/lthash/canonical.go`, `canonical_test.go` |
| profile identity | — | `pkg/replay/payloadexec/exports.go`, `pkg/replay/verifier_test.go` |
| docker | `pkg/integration/chcolumntype_test.go` | `pkg/integration/chreplay_test.go`, `chreplay_time_test.go`, `BUILD.bazel`, `.github/workflows/ci.yml` |
| docs | — | `CLAUDE.md`, `docs/superpowers/specs/2026-08-25-storage-integrity-column-type-profile-design.md` |

---

## Part A — Q-D1: one authority, and the test that keeps it honest

Everything else hangs off this part. It ships in Phase 1 and changes no admitted type: it is a pure restructuring plus one new test, so the existing suite is its own regression proof.

- [x] **Task 0 (pre-flight, do once):** prove the baseline is green and record it.

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
git branch --show-current            # must print feature/si-column-type-profile
bazel test //pkg/... 2>&1 | tail -20
go test ./pkg/replay/... ./pkg/lthash/... 2>&1 | tail -20
```

Expected: every non-`manual` target passes. Record the pass count; Part B and Part D must not shrink it. Do not run `bazel test //...` with `--config=ci` locally — it pins `--platforms=//:linux_amd64` and will not resolve a toolchain on darwin/arm64.

### Task 1: pin the three measurements as ch-go capability regressions

The M1/M2/M3 results are properties of `ch-go v0.73.0-sentioxyz-20260629`, not of housegate. A fork bump can change any of them silently, and every later decision in this plan is derived from them. They become tests before anything is built on top.

**Files:**
- Create: `pkg/replay/nativepayload/chgo_capability_test.go`
- Modify: `pkg/replay/nativepayload/BUILD.bazel` (gazelle)

**Interfaces:** none exported. Test-only.

- [x] **Step 1: Write the three pins**

Create `pkg/replay/nativepayload/chgo_capability_test.go`, `package nativepayload`. It reuses the existing `encodeNativePacket` / `nativePayloadTestRevision` helpers from `native_test.go`.

```go
// TestChGoInfersFixedStringOnlyAtGeneratedWidths pins Spec Q measurement M1.
// proto.ColAuto.Infer resolves FixedString only at the seven widths
// inferGenerated enumerates (col_auto_gen.go); every other width fails
// inference outright. Widening this set is a ch-go change, not a housegate
// change, and Task 11 depends on the exact membership.
func TestChGoInfersFixedStringOnlyAtGeneratedWidths(t *testing.T) {
	inferable := []int{8, 16, 32, 64, 128, 256, 512}
	for _, w := range inferable {
		var c proto.ColAuto
		if err := c.Infer(proto.ColumnType(fmt.Sprintf("FixedString(%d)", w))); err != nil {
			t.Fatalf("FixedString(%d): Infer = %v, want nil", w, err)
		}
	}
	for _, w := range []int{1, 4, 10, 17, 33, 255, 1000} {
		var c proto.ColAuto
		if err := c.Infer(proto.ColumnType(fmt.Sprintf("FixedString(%d)", w))); err == nil {
			t.Fatalf("FixedString(%d): Infer = nil, want an inference error", w)
		}
	}
}

// TestChGoDate32AndDecimalReportDowncastTypes pins measurements M2 and M4.
func TestChGoDate32AndDecimalReportDowncastTypes(t *testing.T) {
	for _, tc := range []struct{ declared, reported string }{
		{"Date", "Date"},
		{"Date32", "Date32"},
		{"DateTime('UTC')", "DateTime('UTC')"},
		{"DateTime64(3, 'UTC')", "DateTime64(3, 'UTC')"},
		{"UUID", "UUID"},
		// M4: the decoded column loses the declared precision and scale.
		{"Decimal(9, 2)", "Decimal32"},
		{"Decimal(18, 4)", "Decimal64"},
		{"Decimal(18, 2)", "Decimal64"},
		{"Decimal(38, 4)", "Decimal128"},
		{"Decimal(76, 10)", "Decimal256"},
		{"Nullable(UInt64)", "Nullable(UInt64)"},
		{"Nullable(Decimal(38, 4))", "Nullable(Decimal128)"},
	} {
		var c proto.ColAuto
		if err := c.Infer(proto.ColumnType(tc.declared)); err != nil {
			t.Fatalf("%s: Infer = %v", tc.declared, err)
		}
		if got := string(c.Data.Type()); got != tc.reported {
			t.Fatalf("%s: decoded column reports %q, want %q", tc.declared, got, tc.reported)
		}
	}
}

// TestChGoNullableSeamShape pins measurement M3: IsElemNull is reachable by a
// plain interface assertion, and the inner Values column is reachable only
// through one exported-field reflection lookup. Task 15's decoder depends on
// both halves.
func TestChGoNullableSeamShape(t *testing.T) {
	for _, declared := range []string{"Nullable(UInt64)", "Nullable(String)", "Nullable(DateTime64(3, 'UTC'))"} {
		var c proto.ColAuto
		if err := c.Infer(proto.ColumnType(declared)); err != nil {
			t.Fatalf("%s: Infer = %v", declared, err)
		}
		if _, ok := c.Data.(interface{ IsElemNull(int) bool }); !ok {
			t.Fatalf("%s: decoded column does not expose IsElemNull(int) bool", declared)
		}
		field := reflect.ValueOf(c.Data).Elem().FieldByName("Values")
		if !field.IsValid() || !field.CanInterface() {
			t.Fatalf("%s: ColNullable.Values is not reachable by reflection", declared)
		}
		if _, ok := field.Interface().(proto.ColResult); !ok {
			t.Fatalf("%s: ColNullable.Values is not a proto.ColResult", declared)
		}
	}
}

// TestNativeDecoderRejectsUndecodableInferableTypes records today's gap: these
// types infer but nativeColumnValue has no case, so Decode fails. Task 11 and
// Tasks 11 and 13-15 flip each of them; until then this test is the honest statement
// of the Native lane's reach.
func TestNativeDecoderRejectsUndecodableInferableTypes(t *testing.T) { /* see Step 2 */ }
```

- [x] **Step 2: Write the negative decode pin**

`TestNativeDecoderRejectsUndecodableInferableTypes` encodes a one-column block per case and asserts `Decode` fails with the named message. Cases and expected substrings, all measured:

| declared | encoder column | expected error substring |
|---|---|---|
| `FixedString(16)` | `*proto.ColFixedStr16` | `unsupported column type *proto.ColFixedStr16` |
| `FixedString(64)` | `*proto.ColFixedStr64` | `unsupported column type *proto.ColFixedStr64` |
| `Date32` | `*proto.ColDate32` | `unsupported column type *proto.ColDate32` |
| `UUID` | `*proto.ColUUID` | `unsupported column type *proto.ColUUID` |
| `Decimal(18, 4)` | `*proto.ColDecimal64` | `type "Decimal64" does not match schema type "Decimal(18, 4)"` |
| `Nullable(UInt64)` | `new(proto.ColUInt64).Nullable()` | `unsupported column type *proto.ColNullable[uint64]` |

Note the `Decimal` row fails one stage earlier than the others — in `nativeBlockColumnPositions`, not `nativeColumnValue`. That difference is M4 and it is why Task 12 exists.

- [x] **Step 3: Sync Bazel and run**

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
bazel run //:gazelle
bazel test //pkg/replay/nativepayload:nativepayload_test --test_output=errors
```

Expected: green immediately. These tests describe today's behaviour; they are pins, not red tests. Tasks 11-15 delete rows from the Step 2 table as each type becomes decodable — deleting a row is the visible evidence a capability landed.

**Verification:** `bazel test //pkg/replay/nativepayload:nativepayload_test --test_output=errors`

**Commit:** `test(nativepayload): pin the ch-go column-capability surface (Spec Q M1-M4)`

### Task 2: build the authority table

**Files:**
- Create: `pkg/replay/payloadexec/column_profile.go`
- Modify: `pkg/replay/payloadexec/column_types.go` (becomes a thin reader), `pkg/replay/payloadexec/executor.go` (`parseValue` dispatches on the shared classification)
- Test: `pkg/replay/payloadexec/column_profile_test.go`

**Interfaces produced** — consumed by Tasks 3, 4, 5, 9, 11, 14, 15, 16:

```go
// ColumnFamily names one admitted shape of declared type. It is the closed set
// the profile is defined over; a family with no test vector fails Task 4.
type ColumnFamily string

const (
	FamilyString      ColumnFamily = "String"
	FamilyFixedString ColumnFamily = "FixedString"
	FamilyBool        ColumnFamily = "Bool"
	FamilyFloat       ColumnFamily = "Float"
	FamilyUInt        ColumnFamily = "UInt"
	FamilyInt         ColumnFamily = "Int"
	// Phase 1 additions (Task 6):
	FamilyDate        ColumnFamily = "Date"
	FamilyDateTime    ColumnFamily = "DateTime"
	FamilyDateTime64  ColumnFamily = "DateTime64"
	// Phase 2 additions (Tasks 11, 13-15):
	FamilyDate32      ColumnFamily = "Date32"
	FamilyUUID        ColumnFamily = "UUID"
	FamilyDecimal     ColumnFamily = "Decimal"
	FamilyNullable    ColumnFamily = "Nullable"
)

// ColumnProfile is the single authority Spec Q Q-D1 requires. Every consumer
// derives from it: classifyColumnType/SupportedColumnType/CanonicalColumnType
// and parseValue in this package, nativepayload.nativeColumnValue's admitted
// output types, and chexec's DDL admission and read-back scan destinations.
type ColumnProfile struct {
	Family ColumnFamily
	// Canonical is the one spelling stored, hashed and compared. It is what
	// lthash.EncodeRow frames and what tableSchemaHash digests.
	Canonical string
	// GoType is the Go value type every materializer must produce for this
	// column, on the Native lane and on the ClickHouse read-back lane alike.
	GoType reflect.Type
	// KindTag is the lthash value kind tag this column's values encode under.
	KindTag byte
	// NativeWireType is the type string a decoded ch-go column reports for
	// this declaration. Equal to Canonical for every family except Decimal
	// and Nullable(Decimal(...)), where ColAuto downcasts (measurement M4).
	NativeWireType string
	// Elem is the inner profile for Nullable(T); nil otherwise.
	Elem *ColumnProfile
	// Parameters carried by the parameterized families, zero elsewhere:
	// FixedStringWidth for FixedString(N); Precision for DateTime64(P) and
	// Decimal(P, S); Scale for Decimal(P, S); Timezone for DateTime(<tz>) and
	// DateTime64(P, <tz>). Every consumer reads these instead of re-parsing
	// the declaration, which is how the four lists stopped drifting.
	FixedStringWidth int
	Precision        int
	Scale            int
	Timezone         string
}

// ResolveColumnProfile classifies one declared type. It is the only parser;
// SupportedColumnType, CanonicalColumnType, ValidateColumnType and parseValue
// are all one-line readers of its result. Rejections unwrap to
// ErrUnsupportedColumnType.
func ResolveColumnProfile(typeName string) (ColumnProfile, error)

// AdmittedColumnTypeVectors returns one concrete admitted declaration per
// distinguishable shape, in canonical spelling. Task 4's cross-component test
// enumerates exactly this list, so adding a family without adding a vector is
// a build failure rather than an untested widening.
func AdmittedColumnTypeVectors() []string
```

- [x] **Step 1: Move the classification into the table, unchanged**

Create `column_profile.go` with `ColumnFamily`, `ColumnProfile`, `ResolveColumnProfile` and `AdmittedColumnTypeVectors`. Populate it with **exactly today's admitted set and nothing more**: `String`, `FixedString(N)` for `0 < N <= 0xFFFFFF`, `Bool`, `Float32/64`, `[U]Int8/16/32/64`. Carry today's `GoType` values (`string`, `[]byte`, `bool`, `float32`, `float64`, `uint8`…`uint64`, `int8`…`int64`) and today's kind tags (`kindString`, `kindBool`, `kindFloat`, `kindUint`, `kindInt`). `NativeWireType` equals `Canonical` for every entry at this point. `Elem` is nil everywhere.

Keep the existing `maxFixedStringWidth` constant and the legacy FixedString spelling tolerance (surrounding whitespace, leading `+`, leading zeroes) exactly as `classifyColumnType` has it — Task 9 is the task that changes FixedString, not this one.

- [x] **Step 2: Reduce `column_types.go` to readers**

`classifyColumnType` becomes a private wrapper over `ResolveColumnProfile`; `SupportedColumnType`, `ValidateColumnType`, `CanonicalColumnType`, `ValidateTableSchemaColumns` and `CanonicalizeTableSchemaColumnTypes` keep their exact signatures and behaviour. `unsupportedColumnTypeError`'s whitelist text moves to a generated form so later tasks do not have to hand-edit prose in two places:

```go
func unsupportedColumnTypeError(typeName string) error {
	return fmt.Errorf("%w %q (admitted profile: %s)", ErrUnsupportedColumnType, typeName, strings.Join(admittedProfileSummary(), ", "))
}
```

`admittedProfileSummary()` renders one line per family. Task 6, 9, 14, 15 and 16 each extend the families and the message follows automatically.

- [x] **Step 3: Point `parseValue` at the profile**

`parseValue` (`executor.go:472`) switches on `profile.Family` plus the FixedString width instead of the old `columnTypeKind`. No branch changes behaviour in this task.

- [x] **Step 4: Add the authority's own unit test**

`column_profile_test.go` asserts, for each entry in `AdmittedColumnTypeVectors()`: `ResolveColumnProfile` succeeds; `CanonicalColumnType(v) == v` (every vector is already canonical); `GoType != nil`; `KindTag != 0`; `NativeWireType != ""`. Plus: every `ColumnFamily` constant declared in the package appears as the `Family` of at least one vector.

- [x] **Step 5: Prove nothing moved**

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
bazel run //:gazelle
bazel test //pkg/replay/payloadexec:payloadexec_test --test_output=errors
```

Expected: green with `column_types_test.go` **completely unmodified**. That file's `supportedTypeMatrix` / `rejectedTypeMatrix` are the frozen statement of the current set (its own comment says so); if it needs an edit in this task, the restructuring changed behaviour and must be reverted.

**Verification:** `bazel test //pkg/replay/payloadexec:payloadexec_test --test_output=errors`

**Commit:** `refactor(payloadexec): make the column profile one table (Spec Q Q-D1)`

### Task 3: derive `nativepayload` and `chexec` from the authority

**Files:**
- Modify: `pkg/replay/chexec/materializer.go` (`supportedColumnType`, `newScanDest`, `derefScan`, `isTemporalColumnType`), `pkg/replay/nativepayload/native.go` (`nativeBlockColumnPositions`)
- Test: `pkg/replay/chexec/materializer_format_test.go`

**Interfaces:** no new exports. `chexec.supportedColumnType` and `newScanDest` stay unexported; Task 4's test is an internal test file in `package chexec`.

- [x] **Step 1: Write the drift test first (red)**

Append to `pkg/replay/chexec/materializer_format_test.go`:

```go
// TestChexecAdmissionEqualsTheColumnProfile is the Spec Q Q-D1 guard for this
// package: chexec must admit exactly what the authority admits, no more and no
// less. Before the fix it fails on FixedString(17), which chexec accepts by
// prefix match while the authority (and, per measurement M1, the Native lane)
// does not.
func TestChexecAdmissionEqualsTheColumnProfile(t *testing.T) {
	for _, v := range payloadexec.AdmittedColumnTypeVectors() {
		if !supportedColumnType(v) {
			t.Errorf("supportedColumnType(%q) = false, authority admits it", v)
		}
	}
	for _, v := range []string{"FixedString(17)", "FixedString(0)", "Nullable(String)", "Array(UInt64)", "IPv4", "Int128"} {
		if supportedColumnType(v) != payloadexec.SupportedColumnType(v) {
			t.Errorf("chexec and authority disagree on %q: chexec=%v authority=%v",
				v, supportedColumnType(v), payloadexec.SupportedColumnType(v))
		}
	}
}

// TestChexecScanDestMatchesTheProfileGoType proves the read-back destination is
// the Go type the authority declares, so ClickHouse read-back and Native decode
// feed lthash identical value types.
func TestChexecScanDestMatchesTheProfileGoType(t *testing.T) {
	for _, v := range payloadexec.AdmittedColumnTypeVectors() {
		p, err := payloadexec.ResolveColumnProfile(v)
		if err != nil {
			t.Fatalf("ResolveColumnProfile(%q): %v", v, err)
		}
		dest, err := newScanDest(v)
		if err != nil {
			t.Fatalf("newScanDest(%q): %v", v, err)
		}
		if got := reflect.TypeOf(dest).Elem(); got != p.GoType {
			t.Errorf("newScanDest(%q) -> *%s, profile GoType %s", v, got, p.GoType)
		}
	}
}
```

Pre-fix expectation: `TestChexecAdmissionEqualsTheColumnProfile` FAILS with `chexec and authority disagree on "FixedString(17)": chexec=true authority=false`. `TestChexecScanDestMatchesTheProfileGoType` passes today by coincidence and becomes load-bearing from Task 6 onward.

- [x] **Step 2: Replace `chexec`'s private lists**

`supportedColumnType(typeName string) bool { return payloadexec.SupportedColumnType(typeName) }`.

`newScanDest` becomes `reflect.New(profile.GoType).Interface()` with the profile resolved from the declared type; the whole per-type `switch` disappears. Its doc comment keeps the FixedString/`[]byte` note but now states that the Go type comes from the authority.

`derefScan` keeps its post-processing switch — the calendar rebuild for `Date` and the `.UTC()` normalization for `DateTime` families are genuinely lane-specific and must stay — but dispatches on `profile.Family` instead of string prefixes, and its `default` becomes a fail-closed error naming the family. `isTemporalColumnType` is deleted; `profile.Family` replaces it at both call sites.

- [x] **Step 3: Make the Native block/schema comparison go through the authority**

In `nativepayload.nativeBlockColumnPositions`, replace the raw `got.Type != want.Type` comparison with a comparison against the authority's declared wire type:

```go
wantProfile, err := payloadexec.ResolveColumnProfile(want.Type)
if err != nil {
	return nil, fmt.Errorf("native block column %q: %w", want.Name, err)
}
if got.Type != wantProfile.NativeWireType {
	return nil, fmt.Errorf("native block column %q type %q does not match schema type %q (expected wire type %q)",
		want.Name, got.Type, want.Type, wantProfile.NativeWireType)
}
```

At this task `NativeWireType == Canonical` for every entry, so behaviour is unchanged except that a schema declaring a type outside the profile now fails with `ErrUnsupportedColumnType` instead of a confusing string mismatch. Add one test for that in `native_test.go`. Task 12 is what makes `NativeWireType` diverge from `Canonical`.

- [x] **Step 4: Run**

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
bazel run //:gazelle
bazel test //pkg/replay/chexec:chexec_test //pkg/replay/nativepayload:nativepayload_test //pkg/replay/payloadexec:payloadexec_test --test_output=errors
```

**Verification:** `bazel test //pkg/replay/... --test_output=errors`

**Commit:** `refactor(chexec,nativepayload): derive column admission from the profile (Spec Q Q-D1)`

### Task 4: the cross-component authority test

This is the test Spec Q Q-D1 calls "the proof that they agree", and the reason 1a and 1e cannot recur.

**Files:**
- Create: `pkg/replay/chexec/column_profile_authority_test.go` (`package chexec` — an internal test file, so it reaches `supportedColumnType` / `newScanDest` / `derefScan` while importing `payloadexec`, `nativepayload`, `lthash` and `ch-go/proto`; `chexec` already depends on all four, so there is no import cycle and no new library dependency)
- Modify: `pkg/replay/chexec/BUILD.bazel` (gazelle adds `@com_github_clickhouse_ch_go//proto` and `//pkg/replay/nativepayload` to the test deps)
- Modify: `pkg/lthash/canonical.go` — export the kind tags (`KindInt`, `KindUint`, `KindFloat`, `KindString`, `KindBool`, `KindTime`) so the authority's `KindTag` claim is checkable rather than decorative. Keep the unexported names as aliases so no existing code changes.

**Interfaces:** test-only, plus the six exported `lthash.Kind*` constants.

- [x] **Step 1: Build the sample-column registry**

The test needs one encodable ch-go column per admitted vector. It cannot live in `payloadexec` — that would make the authority depend on `ch-go/proto`, which it deliberately does not. It lives in the test as a map, with a completeness assertion that closes the loop:

```go
// sampleColumns supplies one non-empty ch-go column per admitted declaration.
// The completeness assertion below is what makes Q-D1 real: an authority entry
// with no sample fails the build, so a type cannot be admitted without being
// proved decodable, scannable and hashable.
var sampleColumns = map[string]func() (proto.ColInput, any){
	"String":  func() (proto.ColInput, any) { c := new(proto.ColStr); c.Append("abc"); return c, "abc" },
	"UInt64":  func() (proto.ColInput, any) { return &proto.ColUInt64{64}, uint64(64) },
	// ... one per vector ...
}

func TestColumnProfileHasASampleForEveryAdmittedType(t *testing.T) {
	for _, v := range payloadexec.AdmittedColumnTypeVectors() {
		if _, ok := sampleColumns[v]; !ok {
			t.Errorf("admitted type %q has no sample column: add one, or remove it from the profile", v)
		}
	}
	for name := range sampleColumns {
		if !payloadexec.SupportedColumnType(name) {
			t.Errorf("sample column %q is not in the admitted profile", name)
		}
	}
}
```

- [x] **Step 2: Write the four-way assertion**

```go
// TestColumnProfileAgreesAcrossAllFourComponents is Spec Q Q-D1's proof. For
// every admitted declaration it asserts, in one table-driven pass:
//   1. the Native lane decodes it and yields the authority's declared GoType;
//   2. chexec admits it for DDL and scans it into the same GoType;
//   3. chexec's derefScan returns that GoType too;
//   4. lthash accepts the value and tags it with the authority's KindTag.
func TestColumnProfileAgreesAcrossAllFourComponents(t *testing.T) {
	for _, declared := range payloadexec.AdmittedColumnTypeVectors() {
		t.Run(declared, func(t *testing.T) {
			p, err := payloadexec.ResolveColumnProfile(declared)
			// ... 1: encode a one-column Native block, nativepayload.Decode,
			//        assert reflect.TypeOf(rows[0].Values[0]) == p.GoType
			// ... 2: supportedColumnType(declared) == true;
			//        reflect.TypeOf(newScanDest(declared)).Elem() == p.GoType
			// ... 3: derefScan(declared, dest) returns p.GoType
			// ... 4: lthash.EncodeRow with one column of this type accepts the
			//        decoded value, and the encoded value's leading byte == p.KindTag
		})
	}
}
```

Assertion 4 needs the value bytes, not just acceptance. `EncodeRow`'s layout is `domain ‖ table ‖ count ‖ [name ‖ type ‖ value]` with every field length-framed, so with a single column the value field is the final framed field: read the last 4-byte little-endian length and take that many trailing bytes; byte 0 of that slice is the kind tag. Put that in a `encodedValueField(t, b []byte) []byte` helper; Task 8's golden reuses it.

The Native encode helper is ~12 lines and mirrors `nativepayload`'s unexported `encodeNativePacket`: `proto.Buffer`, `PutUVarInt(uint64(proto.ClientCodeData))`, `PutString("")`, `proto.Block{Rows: 1, Columns: 1}.EncodeBlock(&buf, revision, proto.Input{{Name: "c", Data: col}})`. Do **not** export the nativepayload helper for this — the duplication is 12 lines and the alternative widens a production package's surface for a test.

- [x] **Step 3: Prove it fails against a deliberately broken authority**

Temporarily add `{Family: FamilyUInt, Canonical: "UInt128", GoType: reflect.TypeOf(proto.UInt128{}), KindTag: lthash.KindUint, NativeWireType: "UInt128"}` to the profile and add `"UInt128"` to `AdmittedColumnTypeVectors()`, but give it no sample column.

Expected: `TestColumnProfileHasASampleForEveryAdmittedType` FAILS with `admitted type "UInt128" has no sample column`. Then add a sample column for it and re-run: `TestColumnProfileAgreesAcrossAllFourComponents/UInt128` FAILS at assertion 1 with `unsupported column type *proto.ColUInt128`. **Revert both edits.** This step produces no commit content; it is the demonstration that the guard bites.

- [x] **Step 4: Run**

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
bazel run //:gazelle
bazel test //pkg/replay/chexec:chexec_test --test_output=errors
```

**Verification:** `bazel test //pkg/replay/chexec:chexec_test //pkg/lthash:lthash_test --test_output=errors`

**Commit:** `test(chexec): prove the column profile agrees across all four components (Spec Q Q-D1)`

---

## Part B — Phase 1: the temporal types, and the proof it is a pure widening

Phase 1 restores the set the pinned executor already replays. It ships with Spec O's housegate release. Nothing in Part B may add a kind tag, change an existing encoding, or bump the profile.

### Task 5: capture the non-regression golden **before** touching anything

Spec Q §4 item 2 wants "a test asserting that row hashes for the existing scalar types are byte-identical before and after". A golden captured *after* the widening proves nothing, so this task lands first and its table is committed unchanged by every later task.

**Files:**
- Create: `pkg/lthash/canonical_golden_test.go`
- Modify: none

**Interfaces:** test-only. Uses the exported `lthash.Kind*` constants added in Task 4.

- [x] **Step 1: Write the golden**

The golden pins the **canonical value encoding** — the framed value field `encodeValue` produces — rather than admission, so it stays meaningful when Task 9 narrows `FixedString` and Task 10 appends kind tags. `EncodeRow`'s layout is `domain ‖ table ‖ uint32(count) ‖ [name ‖ type ‖ value]`, every field length-framed with a little-endian `uint32`, so for a single column the value field is the third framed field after the count. Reuse the `encodedValueField` helper shape from Task 4 Step 2.

These are the measured values against `6fd56b8` + Tasks 1-4. They must not change for the remainder of this plan.

| declared type | value | canonical value-field bytes (hex) |
|---|---|---|
| `String` | `"abc"` | `04616263` |
| `FixedString(32)` | `"fixed"` NUL-padded to 32 | `046669786564` + 27 × `00` |
| `Bool` | `true` | `0501` |
| `Bool` | `false` | `0500` |
| `Float32` | `1.5` | `030000c03f` |
| `Float64` | `2.5` | `030000000000000440` |
| `UInt8` | `8` | `0208` |
| `UInt16` | `16` | `021000` |
| `UInt32` | `32` | `0220000000` |
| `UInt64` | `64` | `024000000000000000` |
| `Int8` | `-8` | `01f8` |
| `Int16` | `-16` | `01f0ff` |
| `Int32` | `-32` | `01e0ffffff` |
| `Int64` | `-64` | `01c0ffffffffffffff` |
| `Date` | `2026-07-16` UTC | `06aa50000000000000` |
| `DateTime` | `2026-07-16T12:34:56Z` | `06f0cf586a00000000` |
| `DateTime('UTC')` | `2026-07-16T12:34:56Z` | `06f0cf586a00000000` |
| `DateTime64(3, 'UTC')` | `2026-07-16T12:34:56.123Z` | `06c034c88143c5c218` |
| `Date32` | `1900-01-01` UTC | `06219cffffffffffff` |

Three of these rows are the load-bearing ones and each carries a comment in the test:

- `DateTime` and `DateTime('UTC')` produce **identical value bytes** and differ only through the framed type string. That is measured evidence for Q-D2's timezone decision: the value encoding is already timezone-independent, so the canonical spelling — not the value — is what distinguishes the two declarations, and stripping the timezone would silently merge two different schema hashes.
- `Date32` at `1900-01-01` encodes as `int64(-25567)` little-endian under `kindTime`. `encodeTime`'s `strings.HasPrefix(col.Type, "Date")` branch computes `t.Unix()/86400`, which truncates toward zero — correct here only because `ColDate32` values are always UTC midnight and therefore exact multiples of 86400. The comment must say so, because a future non-midnight temporal type in that branch would be silently wrong.
- `Date` and `Date32` on the same calendar day produce identical value bytes. This is intended: the framed type string separates them.

- [x] **Step 2: Add the kind-tag freeze assertion in the same file**

```go
// TestCanonicalKindTagsAreFrozen guards Spec Q Q-D3's append-only rule. New
// tags go after these six; renumbering any of them silently rewrites every
// historical row hash.
func TestCanonicalKindTagsAreFrozen(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  byte
		want byte
	}{
		{"kindInt", KindInt, 1}, {"kindUint", KindUint, 2}, {"kindFloat", KindFloat, 3},
		{"kindString", KindString, 4}, {"kindBool", KindBool, 5}, {"kindTime", KindTime, 6},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d — kind tags are append-only", tc.name, tc.got, tc.want)
		}
	}
}

// TestCanonicalDomainIsUnchanged guards the other half of Q-D3: appending kind
// tags must not bump the MVP row profile domain.
func TestCanonicalDomainIsUnchanged(t *testing.T) {
	if canonicalDomain != "housegate-row-mvp-v0" {
		t.Fatalf("canonicalDomain = %q; Spec Q Q-D3 requires it stay housegate-row-mvp-v0", canonicalDomain)
	}
}
```

- [x] **Step 3: Run**

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
bazel run //:gazelle
bazel test //pkg/lthash:lthash_test --test_output=errors
```

Expected: green on first run. If any hex differs from the table above, **stop** — something in Tasks 1-4 changed an encoding, and Part A was supposed to be behaviour-neutral.

**Verification:** `bazel test //pkg/lthash:lthash_test --test_output=errors`

**Commit:** `test(lthash): golden the canonical value encodings before widening (Spec Q §4.2)`

### Task 6: temporal types into the authority (Q-D2, Q-D5)

**Files:**
- Modify: `pkg/replay/payloadexec/column_profile.go` (three families), `pkg/replay/payloadexec/executor.go` (`parseValue`), `pkg/replay/payloadexec/column_types_test.go` (matrix moves)
- Modify: `pkg/replay/chexec/column_profile_authority_test.go` (three sample columns)

**Interfaces:** `FamilyDate`, `FamilyDateTime`, `FamilyDateTime64` become live. `AdmittedColumnTypeVectors()` gains `Date`, `DateTime`, `DateTime('UTC')`, `DateTime64(3)`, `DateTime64(3, 'UTC')`.

**Not included:** `Date32`. Measurement M2 proved it does not decode today, and Q-D2 says explicitly that it moves to Phase 2 rather than being declared supported on a prefix match. Task 11 lands it.

- [x] **Step 1: Move the temporal entries in `column_types_test.go` (red)**

That file's `rejectedTypeMatrix` currently lists `"Date"`, `"DateTime"`, `"DateTime64(3)"`. Move them to `supportedTypeMatrix` and add `"DateTime('UTC')"` and `"DateTime64(3, 'UTC')"`. Add to `rejectedTypeMatrix`: `"Date32"` (Phase 2, and it must stay rejected until then), `"DateTime64()"`, `"DateTime64(10)"` (precision out of ch-go's valid range), `"DateTime('Not/AZone')"`, `"DateTime64(3,'UTC')"` (no space — see Step 3).

Expected pre-fix failure: `SupportedColumnType("DateTime") = false, want true` from `TestValidateColumnType_AcceptsExactlyTheMVPWhitelist`, plus `TestValidateColumnType_AgreesWithParseValue` failing on the same names.

- [x] **Step 2: Add the three families to the authority**

```go
// Temporal families. Spec Q §1a: the Native decoder, the canonical row encoder
// and the ClickHouse-backed executor already handle all of these; only this
// validator rejected them, which is what Spec L D1 narrowed by accident.
// Admitting them adds no digest byte and no kind tag.
```

`Date` → `GoType time.Time`, `KindTag KindTime`. `DateTime` and `DateTime(<tz>)` → same. `DateTime64(P)` and `DateTime64(P, <tz>)` → same. `NativeWireType == Canonical` for all five.

Parameter validation, all fail-closed and all derived from what ch-go will actually accept at decode time (`col_datetime.go:36-49`, `col_datetime64.go:59-85`):
- `DateTime(<tz>)`: the timezone must be single-quoted and must load via `time.LoadLocation`. An unloadable zone is a rejection, because `ColDateTime.Infer` would fail at decode and the validator must not be the looser of the two.
- `DateTime64(P)`: `P` must satisfy `proto.Precision(P).Valid()` (0-9). Reject `DateTime64()` and any `P` outside that range.
- `DateTime64(P, <tz>)`: both rules.

- [x] **Step 3: Define the canonical spellings (Q-D5)**

This is the decision that has to be written down rather than left to `strings.TrimSpace`, because it is hashed twice.

| declaration | canonical form | why |
|---|---|---|
| `Date` | `Date` | no parameters |
| `DateTime` | `DateTime` | no parameters |
| `DateTime( 'UTC' )` | `DateTime('UTC')` | whitespace stripped; **the timezone is kept** (Q-D2) |
| `DateTime64(3)` | `DateTime64(3)` | base-10, no leading `+` or zeroes |
| `DateTime64(3,'UTC')` | `DateTime64(3, 'UTC')` | **comma-space** separator |
| `DateTime64( 03 , 'UTC' )` | `DateTime64(3, 'UTC')` | both rules |

The comma-space is not a style choice. `ColumnType.With` joins parameters with `", "` (`column.go:165-174`), so `ColDateTime64.Type()` reconstructs `DateTime64(3, 'UTC')` regardless of what the wire said. Canonicalizing to the no-space form would make `NativeWireType` permanently unequal to `Canonical` for every `DateTime64` with a timezone — measured. Choose the spelling ch-go reproduces.

Extend `CanonicalColumnType` accordingly and add a `TestCanonicalColumnType_TemporalSpellings` table covering every row above plus the rejections from Step 1.

- [x] **Step 4: Add the three `parseValue` branches**

The CSV lane is legacy (`PayloadFormatCSVWithNames`; the SI intake runtime pins `MaterializerNative`), but Q-D1's authority is shared, so a family in the profile with no `parseValue` branch is exactly the drift this plan exists to stop. Parse RFC3339 first, then ClickHouse's own text forms:

```go
case FamilyDate, FamilyDate32:
	return time.ParseInLocation("2006-01-02", raw, time.UTC)
case FamilyDateTime:
	return parseCHDateTime(raw) // "2006-01-02 15:04:05" or RFC3339, always resolved to UTC
case FamilyDateTime64:
	return parseCHDateTime64(raw, profile.Precision)
```

Both helpers return `time.Time` in UTC so the value matches what the Native lane produces (`nativeColumnValue` returns `.UTC()` for `ColDateTime`/`ColDateTime64`, and `ColDate.Time()` is already UTC). Anything else and the two lanes hash differently for the same row.

- [x] **Step 5: Add the sample columns to Task 4's registry**

```go
"Date":                 ColDate at 2026-07-16
"DateTime":             &proto.ColDateTime{} (nil Location — reports "DateTime")
"DateTime('UTC')":      &proto.ColDateTime{Location: time.UTC}
"DateTime64(3)":        new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
"DateTime64(3, 'UTC')": ... .WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)
```

`TestColumnProfileAgreesAcrossAllFourComponents` now covers them for free. Note the `DateTime` (no timezone) sample: `ColDateTime.loc()` defaults to `time.Local` when `Location` is nil, so `nativeColumnValue`'s `.UTC()` is what makes the value deterministic — assert the decoded `time.Time` equals the instant in UTC, not that its `Location` is UTC.

- [x] **Step 6: Run**

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors
```

Expected: green, **and `pkg/lthash/canonical_golden_test.go` unmodified**. If the golden had to change, Phase 1 was not a pure widening.

**Verification:** `bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors`

**Commit:** `fix(payloadexec): restore the temporal column types to the profile (Spec Q Q-D2)`

### Task 7: a temporal column can be the partition column (measurement M5)

Spec Q does not mention this and Phase 1 is incomplete without it. `partitionValueString` (`executor.go:427-469`) has no `time.Time` case, so a table with `PARTITION BY <a Date or DateTime column>` now passes startup validation and fails at replay with `partition column "d": unsupported partition value type time.Time` — measured. Turning a loud startup refusal into a late per-statement failure is a regression, not a widening.

**Files:**
- Modify: `pkg/replay/payloadexec/executor.go` (`partitionValueString`)
- Test: `pkg/replay/payloadexec/executor_test.go`

**Interfaces:** none.

- [x] **Step 1: Write the failing test (red)**

```go
// TestPartitionIDForRow_AcceptsTemporalPartitionColumns proves a Date or
// DateTime partition column is usable. Before the fix both cases fail with
// `unsupported partition value type time.Time`.
func TestPartitionIDForRow_AcceptsTemporalPartitionColumns(t *testing.T) {
	for _, tc := range []struct {
		typ  string
		v    time.Time
		want string
	}{
		{"Date", time.Date(2026, time.July, 16, 0, 0, 0, 0, time.UTC), "p_2026-07-16"},
		{"DateTime('UTC')", time.Date(2026, time.July, 16, 12, 34, 56, 0, time.UTC), "p_2026-07-16 12:34:56"},
		{"DateTime64(3, 'UTC')", time.Date(2026, time.July, 16, 12, 34, 56, 123000000, time.UTC), "p_2026-07-16 12:34:56.123"},
	} { /* ... */ }
}
```

Expected pre-fix failure, all three cases: `partition column "p": unsupported partition value type time.Time`.

- [x] **Step 2: Decide and document the rendering**

The partition id is **not** hashed into a row element — it is an executor-internal grouping key that must merely be stable and injective across the values a single partition column can take. Two rules, both stated in the code comment:

1. Render in UTC. `PartitionIDForRow` runs on both lanes; a local-timezone rendering would put the same row in different partitions on two verifiers.
2. Render per family, not per Go type: `Date`/`Date32` → `2006-01-02`; `DateTime`/`DateTime(tz)` → `2006-01-02 15:04:05`; `DateTime64(P)` → seconds plus exactly `P` fractional digits. `DateTime64(0)` renders with no fractional part.

Because the family is needed, `partitionValueString(v any)` becomes `partitionValueString(profile ColumnProfile, v any)` and `PartitionIDForRow` passes the already-resolved profile for the partition column. Its `default` branch stays fail-closed.

- [x] **Step 3: Guard the injectivity claim**

Add `TestPartitionIDForRow_TemporalRenderingIsInjective`: two distinct instants one millisecond apart under `DateTime64(3, 'UTC')` must produce different ids; the same two under `DateTime('UTC')` must produce the *same* id, and that is correct because the column's own resolution is one second.

- [x] **Step 4: Run**

```bash
bazel test //pkg/replay/payloadexec:payloadexec_test --test_output=errors
```

**Verification:** `bazel test //pkg/replay/... --test_output=errors`

**Commit:** `fix(payloadexec): accept temporal partition columns (Spec Q M5)`

### Task 8: Q-D5 canonical spellings, proved against a real ClickHouse

**Files:**
- Create: `pkg/integration/chcolumntype_test.go`
- Modify: `pkg/integration/BUILD.bazel` (gazelle), `.github/workflows/ci.yml`

**Interfaces:** test-only. Reuses `pkg/integration/testenv`'s ClickHouse container helper — read `pkg/integration/chschema_test.go` for the established pattern rather than inventing a new one.

- [x] **Step 1: Write the round trip**

```go
// TestColumnProfileCanonicalSpellingsSurviveClickHouse is Spec Q Q-D5's proof.
// For every admitted declaration: CREATE a table with that column type, read
// the type back from system.columns, and assert ClickHouse's own spelling
// equals our canonical form. A type ClickHouse renormalizes differently from us
// is a drift bug in verify-only DDL mode, and this round trip is the only way
// to find it.
func TestColumnProfileCanonicalSpellingsSurviveClickHouse(t *testing.T) {
	for _, declared := range payloadexec.AdmittedColumnTypeVectors() {
		// CREATE TABLE t_<i> (c <declared>) ENGINE = MergeTree ORDER BY tuple()
		// SELECT type FROM system.columns WHERE database = currentDatabase()
		//   AND table = 't_<i>' AND name = 'c'
		// assert readBack == declared (every vector is already canonical)
	}
}
```

Also assert the non-canonical spellings **round-trip to the canonical one**: create with `DateTime64( 03 , 'UTC' )` and assert `system.columns` reports `DateTime64(3, 'UTC')`, i.e. that our canonicalization and ClickHouse's agree rather than merely both being self-consistent. Include `FixedString( 4 )`, `FixedString(+4)`, `FixedString(04)` — the three legacy spellings `classifyColumnType` tolerates — as inputs whose canonical form is `FixedString(4)`. (Task 9 removes width 4 from the admitted set; keep these as *canonicalization* cases driven by `CanonicalColumnType` directly, not through the admitted-vector loop, so Task 9 does not have to rewrite this test.)

- [x] **Step 2: Register the target in CI**

`.github/workflows/ci.yml:122-127` lists docker-bound targets explicitly because they are `manual`-tagged. If gazelle puts the new test in the existing `//pkg/integration:integration_test` target — which it will, since that target compiles the whole package — **no ci.yml change is needed**, and the step is to *verify* that:

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
bazel query 'tests(//pkg/integration:integration_test)' | grep -c . 
bazel query 'attr(srcs, chcolumntype_test.go, //pkg/integration:all)'
```

Expected: the second query prints `//pkg/integration:integration_test`. If gazelle instead created a new target, add it to the ci.yml list in the same commit and say so in the commit message.

- [x] **Step 3: Run**

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
bazel test //pkg/integration:integration_test --test_filter='TestColumnProfileCanonicalSpellings' --test_output=errors
```

Requires docker. If the pre-existing `//pkg/integration:integration_test` target is red on `main` for unrelated reasons, diff the failing-test set against a clean `main` build before claiming a regression (CLAUDE.md's main-baseline rule).

**Verification:** `bazel test //pkg/integration:integration_test --test_filter='TestColumnProfileCanonicalSpellings' --test_output=errors`

**Commit:** `test(integration): round-trip every canonical column spelling through ClickHouse (Spec Q Q-D5)`

---

## Part C — Q-D7: `FixedString` widths aligned to what actually decodes

Measurement M1 settled 1e, and it settled it differently from how Spec Q guessed. `proto.ColAuto` **does** infer `FixedString`, at seven generated widths; `nativeColumnValue` handles **one** of them. So the gap is not "the decoder has no FixedString" but "the validator admits 16,777,215 widths for a decoder that handles one".

**The decision this plan takes:** Phase 1 **narrows** the admitted set to `FixedString(32)`. Phase 2 (Task 11) widens it to the seven widths ch-go can infer.

The alternative — widen `nativeColumnValue` to all seven in Phase 1 — is rejected on Q-D4's own logic. Widths 8/16/64/128/256/512 are not currently replayable at all, so admitting them is a *capability addition*: an old verifier meeting a `FixedString(16)` column would fail to decode, exactly like it would meeting a `UUID`. Q-D4 says that is what a profile bump is for, and Phase 1 must not bump or it stops being independently shippable alongside Spec O. Narrowing to 32 adds no capability and removes only branches that could never produce a root, so it changes no computed value anywhere.

### Task 9: narrow `FixedString` to the decodable width

**Files:**
- Modify: `pkg/replay/payloadexec/column_profile.go`, `column_types.go` (drop `maxFixedStringWidth`), `column_types_test.go`, `executor_test.go`
- Modify: `pkg/integration/chreplay_test.go` (its `FixedString(10)` column)

**Interfaces:** `ColumnProfile.FixedStringWidth int` becomes meaningful (it is `32` for the only admitted entry, and Task 11 makes it a real parameter).

- [x] **Step 1: Write the failing assertions (red)**

In `column_types_test.go`:
- `supportedTypeMatrix`: drop `"FixedString(1)"`, `"FixedString(255)"`, `"FixedString( 4 )"`, `"FixedString(+4)"`, `"FixedString(04)"`. Keep `"FixedString(32)"`.
- `rejectedTypeMatrix`: add `"FixedString(1)"`, `"FixedString(16)"`, `"FixedString(31)"`, `"FixedString(33)"`, `"FixedString(64)"`, `"FixedString(255)"`, `"FixedString(16777215)"`.
- `TestValidateColumnType_FixedStringWidthMatchesClickHouse25_8` is renamed `TestValidateColumnType_FixedStringWidthMatchesTheNativeDecoder` and now asserts `ValidateColumnType("FixedString(16777215)")` **fails**. Its comment must explain the change: the bound is no longer ClickHouse's `MAX_FIXEDSTRING_SIZE` but ch-go's `inferGenerated` set intersected with `nativeColumnValue`'s cases (measurement M1). Cite `col_auto_gen.go:47-59` and `native.go:262-264`.
- `TestCanonicalColumnType_NormalizesEveryAcceptedFixedStringSpelling` re-points its spellings at width 32: `"FixedString( 32 )"`, `"FixedString(+32)"`, `"FixedString(032)"` → `"FixedString(32)"`. The legacy tolerance itself is unchanged; only the width moves.

In `executor_test.go`, the `parseValue("FixedString(4)", ...)` cases at `:636-651` become `FixedString(32)` cases with correspondingly padded expectations, plus one new case asserting `parseValue("FixedString(4)", "ab")` now returns `ErrUnsupportedColumnType`.

Expected pre-fix failures: `SupportedColumnType("FixedString(1)") = true, want false` and friends; `ValidateColumnType("FixedString(16777215)")` returning nil where an error is now wanted.

- [x] **Step 2: Narrow the authority**

Replace the `0 < N <= maxFixedStringWidth` range check with membership in an enumerated set that currently holds one width:

```go
// fixedStringAdmittedWidths is the intersection of what proto.ColAuto can infer
// (inferGenerated: 8, 16, 32, 64, 128, 256, 512) with what nativeColumnValue can
// decode (ColFixedStr32 only). Spec Q Q-D7: the validator may never be wider
// than the decoder. Task 11 widens both halves together under the Phase 2
// profile bump.
var fixedStringAdmittedWidths = map[int]struct{}{32: {}}
```

Keep the `strconv.ParseInt(widthText, 10, 64)` call so a width literal that overflows `int` is rejected before any arithmetic, and keep the legacy whitespace/leading-plus/leading-zero tolerance — that is spelling, not width, and Q-D5 wants it canonicalized rather than removed. Delete `maxFixedStringWidth` and update `unsupportedColumnTypeError`'s summary line via `admittedProfileSummary()`.

- [x] **Step 3: Fix the one production-adjacent fixture this breaks**

`pkg/integration/chreplay_test.go:299` declares `{Name: "tx_hash", Type: "FixedString(10)"}` on the CSV lane, with a comment at `:293` explaining that width 10 exercises the NUL-padded `[]byte` read-back path. Change it to `FixedString(32)` and keep both a short and an exactly-full value so the padding path is still exercised. Update the comment at `:282-293` to say the width is pinned by the Native decoder's reach, not chosen for the test.

This is the visible cost of the narrowing and it is the only one in-tree: nothing else in the repo declares a non-32 `FixedString`. It is safe to pay now because, per the closure roadmap §1, no SI deployment is connected in production and the CSV lane is not the production lane (`MaterializerNative` is pinned by the intake runtime).

- [x] **Step 4: Delete the corresponding rows from Task 1's negative pin**

`TestNativeDecoderRejectsUndecodableInferableTypes` keeps its `FixedString(16)` / `FixedString(64)` rows — they are still undecodable — but add an assertion that the authority now also rejects them at declaration, so the two halves are stated together:

```go
if payloadexec.SupportedColumnType(tc.declared) {
	t.Errorf("%s decodes nowhere but is admitted by the profile — Q-D7 forbids exactly this", tc.declared)
}
```

That assertion is the durable statement of Q-D7 and it is what Task 11 has to flip deliberately.

- [x] **Step 5: Run**

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors
bazel test //pkg/integration:integration_test --test_filter='TestReplayCHExecutor' --test_output=errors
```

Expected: green, and `pkg/lthash/canonical_golden_test.go` still unmodified.

**Verification:** `bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors && bazel test //pkg/integration:integration_test --test_filter='TestReplayCHExecutor' --test_output=errors`

**Commit:** `fix(payloadexec): admit only the FixedString width the Native lane decodes (Spec Q Q-D7)`

> **Phase 1 ends here.** Tasks 0-9 are the release Spec O cuts. Before handing off: `bazel build //...` and `bazel test //...` fully green, `bazel test //pkg/integration:integration_test` no worse than the `main` baseline, and `CLAUDE.md`'s `pkg/replay/payloadexec` bullet updated to describe the authority and the admitted set. Do **not** start Part D on the same branch if Phase 1 needs to ship first — cut it, then continue.

---

## Part D — Phase 2: `UUID`, `Decimal`, `Nullable`, `Date32`, the remaining `FixedString` widths

Phase 2 adds capability and therefore bumps the profile (Part E). It must land before the first devnet2 chain.

### Task 10: append the three kind tags

**Files:**
- Modify: `pkg/lthash/canonical.go`
- Modify: `pkg/lthash/canonical_golden_test.go` (add the new-tag assertions; **do not touch the existing golden rows**)

**Interfaces produced:** `lthash.KindUUID = 7`, `lthash.KindDecimal = 8`, `lthash.KindNull = 9`, consumed by Tasks 13, 14, 15 and the authority's `KindTag` field.

- [ ] **Step 1: Extend the freeze test first (red)**

Add to `TestCanonicalKindTagsAreFrozen`: `{"kindUUID", KindUUID, 7}, {"kindDecimal", KindDecimal, 8}, {"kindNull", KindNull, 9}`. Expected pre-fix failure: a compile error, `undefined: KindUUID`. A compile failure is the correct red here — the constants do not exist yet.

- [ ] **Step 2: Append the tags**

```go
const (
	kindInt byte = iota + 1
	kindUint
	kindFloat
	kindString
	kindBool
	kindTime
	// Spec Q Q-D3. Appended after the original six; renumbering any of them
	// would rewrite every historical row hash, so new kinds only ever go at
	// the end. Appending changes no existing row's bytes, which is why
	// canonicalDomain stays housegate-row-mvp-v0.
	kindUUID
	kindDecimal
	kindNull
)
```

- [ ] **Step 3: Prove the existing encodings are untouched**

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
bazel test //pkg/lthash:lthash_test --test_output=errors
git diff --stat pkg/lthash/canonical_golden_test.go
```

Expected: green, and the diff shows only added lines in the kind-tag freeze test — **zero changes to the golden table**. If any golden row changed, the append was not an append.

**Verification:** `bazel test //pkg/lthash:lthash_test --test_output=errors`

**Commit:** `feat(lthash): append kindUUID/kindDecimal/kindNull (Spec Q Q-D3)`

### Task 11: `Date32` and the remaining `FixedString` widths decode

These two are grouped because they are the same shape of change — a missing `nativeColumnValue` case for a column `ColAuto` already infers — and because both flip an assertion in Task 1's negative pin.

**Files:**
- Modify: `pkg/replay/nativepayload/native.go` (`nativeColumnValue`), `pkg/replay/payloadexec/column_profile.go` (`FamilyDate32`, `fixedStringAdmittedWidths`), `pkg/replay/payloadexec/executor.go` (`parseValue`, `parseFixedString`), `pkg/replay/chexec/materializer.go` (`derefScan` `Date32` branch)
- Modify: `pkg/replay/nativepayload/chgo_capability_test.go` (delete the flipped rows), `pkg/replay/payloadexec/column_types_test.go` (`Date32` moves to supported), `pkg/replay/chexec/column_profile_authority_test.go` (new samples)

**Interfaces:** `FamilyDate32` becomes live; `fixedStringAdmittedWidths` becomes `{8, 16, 32, 64, 128, 256, 512}`.

- [ ] **Step 1: Flip the pins (red)**

Delete the `Date32`, `FixedString(16)` and `FixedString(64)` rows from `TestNativeDecoderRejectsUndecodableInferableTypes`. Move `"Date32"` from `rejectedTypeMatrix` to `supportedTypeMatrix`; move `"FixedString(16)"`, `"FixedString(64)"` likewise, and add `"FixedString(17)"`, `"FixedString(31)"`, `"FixedString(1000)"` to `rejectedTypeMatrix` — the enumerated set is still closed, just larger. Add the new vectors to `AdmittedColumnTypeVectors()` and sample columns for `Date32`, `FixedString(8)`, `FixedString(16)`, `FixedString(64)`, `FixedString(128)`, `FixedString(256)`, `FixedString(512)`.

Expected pre-fix failures: `TestColumnProfileAgreesAcrossAllFourComponents/Date32` with `unsupported column type *proto.ColDate32`, and one per new FixedString width with `unsupported column type *proto.ColFixedStr<N>`.

- [ ] **Step 2: Add the decoder cases**

```go
case *proto.ColDate32:
	return c.Row(i).UTC(), 4, nil
case *proto.ColFixedStr8:
	row := c.Row(i)
	return append([]byte(nil), row[:]...), 8, nil
// ... 16, 64, 128, 256, 512, mirroring the existing ColFixedStr32 case ...
```

Two invariants the comment must state. First, each `ColFixedStrN.Row(i)` returns `[N]byte` (a Go **array**), and `lthash.encodeValue` has no array case — the `row[:]` copy into a `[]byte` is what makes it hashable, exactly as the existing width-32 case does. Second, the seven cases are a closed enumeration mirroring `inferGenerated`, not an open-ended pattern: a width outside the set fails at `ColAuto.Infer` before reaching this switch, and Task 1's capability pin is what keeps that true across ch-go bumps.

`ColDate32.Row(i)` already returns UTC (`date32.go:20-22`); the explicit `.UTC()` matches the `ColDateTime` cases and costs nothing.

- [ ] **Step 3: Extend `parseValue` and `parseFixedString`**

`FamilyDate32` reuses the `FamilyDate` CSV branch from Task 6 Step 4. `parseFixedString` is unchanged — it already takes a width — but its doc comment must note the width is now one of seven rather than arbitrary.

- [ ] **Step 4: Extend `chexec`**

`derefScan`'s `*time.Time` post-processing gains a `FamilyDate32` branch identical to `FamilyDate`'s calendar rebuild (clickhouse-go relocates `Date32`'s midnight into the session timezone the same way it does `Date`'s). `newScanDest` needs no change — it derives from `GoType`.

- [ ] **Step 5: Run**

```bash
bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors
```

**Verification:** `bazel test //pkg/replay/... --test_output=errors`

**Commit:** `feat(nativepayload): decode Date32 and every inferable FixedString width (Spec Q Q-D2/Q-D7)`

### Task 12: own the Native block header so `Decimal`'s scale is actually bound

Measurement M4. This task has no user-visible effect on any Phase 1 type and must land **before** Task 14, because without it a `Decimal` column cannot pass `nativeBlockColumnPositions` at all, and the obvious workaround (compare against the downcast name) would leave the payload's declared scale unbound while `lthash` frames the schema's scale into every row element.

**Files:**
- Create: `pkg/replay/nativepayload/block.go` (+`block_test.go`)
- Modify: `pkg/replay/nativepayload/native.go` (`decodeNativeDataBlock`, `nativeBlockColumnPositions`), `BUILD.bazel`

**Interfaces produced:** `nativeDataBlock.Columns[i].Type` becomes the **raw declared wire type string** rather than the decoded column's reconstructed `Type()`. Consumed by `nativeBlockColumnPositions` and by Task 14.

- [ ] **Step 1: Write the failing test (red)**

In `block_test.go`, encode a block whose header declares `Decimal(18, 4)` and assert the captured wire type is exactly that, and a second declaring `Decimal(18, 2)` over identical payload bytes, asserting the two are distinguishable.

Producing such a block needs a `proto.ColInput` whose `Type()` returns the parameterized spelling — `proto.ColDecimal64` returns `Decimal64` — so the test wraps it:

```go
// declaredAs forces a column onto the wire under an explicit declared type,
// which is what a real clickhouse-go INSERT does: lib/proto/block.go:135 writes
// PutString(string(c.Type())) and lib/column/decimal.go:70 returns the declared
// Decimal(P, S) parsed from DDL.
type declaredAs struct {
	proto.ColInput
	declared proto.ColumnType
}

func (d declaredAs) Type() proto.ColumnType { return d.declared }
```

Expected pre-fix failure: the captured type is `"Decimal64"` for both cases, so the two are indistinguishable — `want "Decimal(18, 4)", got "Decimal64"`.

- [ ] **Step 2: Implement the header-owning `proto.Result`**

`block.go` holds one type implementing `proto.Result`. It reproduces `proto.Results.decodeAuto` (`ch-go/proto/results.go:31-79`) using only exported API, with the single difference that it keeps the raw type string:

```go
// rawTypedResults decodes a Native block header while preserving each column's
// exact declared type string. proto.Results.Auto() discards it — it stores the
// inferred column, whose Type() is a reconstruction — and for Decimal that
// reconstruction is lossy: ColAuto downcasts Decimal(P, S) to Decimal32/64/128/256,
// so Decimal(18,4) and Decimal(18,2) become indistinguishable (Spec Q M4).
// Since lthash frames the *schema's* declared type into every row element, an
// unbound payload scale is exactly the executor/payload divergence storage
// integrity exists to detect.
type rawTypedResults struct {
	names []string
	types []string
	cols  []proto.ColResult
}

func (s *rawTypedResults) DecodeResult(r *proto.Reader, version int, b proto.Block) error
```

The body: per column, `r.Str()` for the name, `r.Str()` for the raw type, `proto.FeatureCustomSerialization.In(version)` → `r.Bool()` and reject a true custom-serialization flag, then `(&proto.ColAuto{}).Infer(rawType)`, `Reset()`, and — when `b.Rows != 0` — the optional `proto.Stateful` `DecodeState` followed by `DecodeColumn(r, b.Rows)`. Store `col.Data`, not the `ColAuto` wrapper, so `nativeColumnValue`'s switch is unchanged.

`decodeNativeDataBlock` swaps `results.Auto()` for `&rawTypedResults{}` and populates `out.Columns[i].Type` from `s.types[i]`.

- [ ] **Step 3: Compare through the authority, not by raw equality**

`nativeBlockColumnPositions` (Task 3 Step 3 already routed it through the profile) now canonicalizes the wire spelling instead of trusting it:

```go
gotCanonical, err := payloadexec.CanonicalColumnType(got.Type)
if err != nil {
	return nil, fmt.Errorf("native block column %q declares unsupported type %q: %w", want.Name, got.Type, err)
}
if gotCanonical != wantProfile.Canonical {
	return nil, fmt.Errorf("native block column %q type %q does not match schema type %q", want.Name, got.Type, want.Type)
}
```

`ColumnProfile.NativeWireType` becomes unused by this comparison and is **deleted** from the struct in this task — owning the header makes it unnecessary, and a field that describes ch-go's downcast is a trap once nothing reads it. Update Task 1's `TestChGoDate32AndDecimalReportDowncastTypes` comment to say the downcast is now confined to `ColAuto` and no longer reaches housegate's comparison.

This is strictly stronger than what Phase 1 shipped: a wire spelling that differs only in whitespace now matches (`DateTime64(3,'UTC')` against a schema's `DateTime64(3, 'UTC')`), while a wire type outside the profile is rejected by name instead of by a confusing mismatch.

- [ ] **Step 4: Prove Phase 1 behaviour is unchanged**

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
bazel run //:gazelle
bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors
```

Expected: green with **no edits** to `native_test.go`'s existing assertions, including `TestNativePayloadDecodesClickHouseGoClientData` — the captured real-clickhouse-go fixture is the strongest available evidence that the hand-written header decode matches ch-go's.

**Verification:** `bazel test //pkg/replay/nativepayload:nativepayload_test --test_output=errors`

**Commit:** `fix(nativepayload): preserve the declared wire type through block decode (Spec Q M4)`

### Task 13: `UUID` end to end

**Files:**
- Modify: `pkg/lthash/canonical.go`, `pkg/replay/payloadexec/column_profile.go`, `executor.go` (`parseValue`), `pkg/replay/nativepayload/native.go`, `pkg/replay/chexec/materializer.go`
- Modify: the three test files and the sample registry

**Interfaces produced:** `ColumnProfile.ScanGoType reflect.Type` — the element type clickhouse-go scans this column into, which is **not** always `GoType`. It defaults to `GoType`; `newScanDest` uses `ScanGoType` and `derefScan` converts to `GoType`. Introduced here because `UUID` is the first type where they differ, and consumed by Tasks 14 and 15.

- [ ] **Step 1: Write the failing vectors (red)**

Add `"UUID"` to `AdmittedColumnTypeVectors()` and a sample column (`proto.ColUUID` with `uuid.MustParse("11111111-2222-3333-4444-555555555555")`). Move `"UUID"` out of `rejectedTypeMatrix`.

Expected pre-fix failure: `TestColumnProfileAgreesAcrossAllFourComponents/UUID` fails at assertion 1 with `unsupported column type *proto.ColUUID`.

- [ ] **Step 2: Choose the canonical Go value type**

`GoType = [16]byte`, **not** `uuid.UUID`. `lthash` has no non-stdlib imports today and must not grow one for a display type; `uuid.UUID` is defined as `type UUID [16]byte`, so the conversion in `nativeColumnValue` is free. `ScanGoType = uuid.UUID`, because clickhouse-go's `UUID.ScanRow` accepts only `*string`, `**string`, `*uuid.UUID`, `**uuid.UUID` or a `sql.Scanner` (`lib/column/uuid.go:47-71`) — a `*[16]byte` destination is a `ColumnConverterError`.

`[16]byte` also cannot collide with `FixedString(16)`'s `[]byte`: they are different Go types, so `encodeValue` dispatches differently even before the kind tag separates them.

- [ ] **Step 3: Implement the four sides**

- `lthash.encodeValue`: `case [16]byte: return append([]byte{kindUUID}, x[:]...), nil`. Q-D3: the 16 bytes in ClickHouse's storage order, which is what `uuid.UUID`'s array already holds.
- `nativeColumnValue`: `case *proto.ColUUID: return [16]byte(c.Row(i)), 16, nil`.
- `parseValue` `FamilyUUID`: `uuid.Parse(raw)` then `[16]byte(u)`; a malformed literal is an error, never a zero UUID.
- `chexec.derefScan`: `case *uuid.UUID: return [16]byte(*v), nil`.

- [ ] **Step 4: Add the canonical spelling**

`UUID` has no parameters; canonical form is `UUID`. Add it to Task 8's ClickHouse round trip vectors (it is picked up automatically, since that test iterates `AdmittedColumnTypeVectors()`).

- [ ] **Step 5: Add the golden row**

Append one row to `pkg/lthash/canonical_golden_test.go` for `UUID` / `11111111-2222-3333-4444-555555555555` → `07` followed by the 16 bytes `11111111222233334444555555555555`. Appending a row is allowed; editing an existing one is not.

- [ ] **Step 6: Run**

```bash
bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors
```

**Verification:** `bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors`

**Commit:** `feat(payloadexec): admit UUID end to end (Spec Q Q-D3)`

### Task 14: `Decimal` end to end, as the raw scaled integer

**Files:**
- Modify: `pkg/lthash/canonical.go`, `pkg/replay/payloadexec/column_profile.go`, `executor.go`, `pkg/replay/nativepayload/native.go`, `pkg/replay/chexec/materializer.go`
- Test: all four packages plus a new docker case in Task 17

**Interfaces produced:** `type lthash.Decimal []byte` — the raw scaled integer, two's-complement, little-endian, at the column's physical width. A named slice type, not `[]byte`, because `encodeValue` dispatches on the Go type and bare `[]byte` already means `kindString`.

- [ ] **Step 1: Write the exactness test first (red)**

Spec Q §4 item 5 wants "values that a float round trip would corrupt". Put them in `pkg/lthash/canonical_golden_test.go` as new golden rows and in `pkg/replay/payloadexec/column_profile_test.go` as parse cases:

| declared | value | why it is in the table |
|---|---|---|
| `Decimal(18, 4)` | `99999999999999.9999` | scaled integer `999999999999999999` > 2^53; a float64 round trip returns `1000000000000000000` |
| `Decimal(38, 10)` | `-1.0000000000` | negative, exercises the two's-complement path at 16 bytes |
| `Decimal(38, 38)` | `0.00000000000000000000000000000000000001` | scale at the precision limit |
| `Decimal(76, 0)` | `2^255 - 1` as an integer | the widest admitted value, 32 bytes |
| `Decimal(9, 2)` | `-0.01` | smallest width, negative, sign-extension into 4 bytes |

Expected pre-fix failure: `ResolveColumnProfile("Decimal(18, 4)")` returns `ErrUnsupportedColumnType`.

- [ ] **Step 2: Define the encoding precisely**

`encodeValue`: `case Decimal: return append([]byte{kindDecimal}, x...), nil`. The value must already be exactly the physical width; `encodeValue` does not pad or check, so the producers do:

| precision P | physical width | ch-go column | byte order |
|---|---|---|---|
| 1 ≤ P ≤ 9 | 4 | `ColDecimal32` (`int32`) | `binary.LittleEndian.PutUint32(uint32(v))` |
| 10 ≤ P ≤ 18 | 8 | `ColDecimal64` (`int64`) | `PutUint64(uint64(v))` |
| 19 ≤ P ≤ 38 | 16 | `ColDecimal128` (`Int128{Low, High}`) | `PutUint64(Low)` ‖ `PutUint64(High)` |
| 39 ≤ P ≤ 76 | 32 | `ColDecimal256` (`Int256{Low, High UInt128}`) | `Low.Low` ‖ `Low.High` ‖ `High.Low` ‖ `High.High`, each `PutUint64` |

This is byte-for-byte what clickhouse-go itself does when it converts a decimal column to a big.Int (`lib/column/decimal.go:98-113`), so the wire bytes, the Native-lane value and the ClickHouse read-back all agree by construction rather than by coincidence.

**`Decimal512` is rejected**, per Q-D3: ClickHouse's `Decimal(P, S)` grammar tops out at P = 76, so `ColAuto`'s `prec >= 77 && prec < 155` branch is unreachable through legal DDL and admitting it would put an unreachable branch in a consensus path. The authority rejects `P >= 77` explicitly, with that reason in the error message.

- [ ] **Step 3: Parameter validation**

`Decimal(P, S)` requires `1 <= P <= 76` and `0 <= S <= P` — the same bounds clickhouse-go's own parser enforces (`lib/column/decimal.go:44-52`), so the validator is never looser than the client. The explicit spellings `Decimal32(S)` / `Decimal64(S)` / `Decimal128(S)` / `Decimal256(S)` are admitted and canonicalize to **themselves**, not to `Decimal(P, S)`: ClickHouse stores and reports whichever the operator wrote, and Task 17's `system.columns` round trip is what proves it. `Decimal512(S)` is rejected.

Canonical spelling: `Decimal(18, 4)` — **comma-space**, matching the temporal rule from Task 6 Step 3 and matching what ClickHouse reports. Confirm against `system.columns` in Task 17 before freezing; if ClickHouse reports `Decimal(18, 4)` without the space, the canonical form follows ClickHouse and Task 6's `DateTime64` rule stays as it is (the two are independent — one follows ch-go's reconstruction, the other follows ClickHouse's DDL rendering, and Task 12 removed the need for them to agree).

- [ ] **Step 4: Implement the four sides**

- `nativeColumnValue`: four cases, `*proto.ColDecimal32/64/128/256`, each serializing per the Step 2 table. The physical width comes from the ch-go column type, not from the schema, and the authority test asserts the two agree.
- `parseValue` `FamilyDecimal`: parse the CSV text with `shopspring/decimal`, `Rescale(-int32(S))`, take `Coefficient()`, and serialize the `*big.Int` two's-complement little-endian at the physical width. Reject a value whose magnitude does not fit the width **before** truncating — a silently wrapped decimal in a consensus digest is the worst possible failure.
- `chexec`: `ScanGoType = decimal.Decimal` (clickhouse-go's `Decimal.ScanRow` accepts only `*decimal.Decimal`, `**decimal.Decimal` or a `sql.Scanner`, `lib/column/decimal.go:135-153`). `derefScan` runs the same `Rescale`/`Coefficient`/serialize path as `parseValue`, so there is exactly one implementation — extract it as `payloadexec.EncodeDecimal(coefficient *big.Int, width int) (lthash.Decimal, error)` and call it from both.
- `chexec.insertRows` needs no change: it appends `r.Values...`, and Task 16 makes the insert path carry `decimal.Decimal` rather than `lthash.Decimal`.

- [ ] **Step 5: Run**

```bash
bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors
```

**Verification:** `bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors`

**Commit:** `feat(payloadexec): admit Decimal as the raw scaled integer (Spec Q Q-D3)`

### Task 15: `Nullable(T)` through the generic seam (Q-D6)

**Files:**
- Modify: `pkg/lthash/canonical.go`, `pkg/replay/payloadexec/column_profile.go`, `executor.go` (`parseValue`, `partitionValueString`), `pkg/replay/nativepayload/native.go`, `pkg/replay/chexec/materializer.go`

**Interfaces:** `ColumnProfile.Elem *ColumnProfile` becomes live. `AdmittedColumnTypeVectors()` gains `Nullable(T)` for **every** scalar `T` already in the list — that enumeration is what Q-D6 says proves the seam is complete.

- [ ] **Step 1: Write the identity tests first (red)**

Spec Q §4 item 4 wants both directions:

```go
// TestNullableWithNoNullsHashesLikeTheBareType is the property that makes the
// Nullable addition auditable (Q-D3): a nullable column carrying no nulls
// encodes each value exactly as the bare type would, so only the framed type
// string distinguishes the two rows.
func TestNullableWithNoNullsEncodesLikeTheBareType(t *testing.T) {
	bare, _ := EncodeRow("t", []Column{{Name: "c", Type: "UInt64"}}, []any{uint64(7)})
	null, _ := EncodeRow("t", []Column{{Name: "c", Type: "Nullable(UInt64)"}}, []any{uint64(7)})
	if !bytes.Equal(encodedValueField(t, bare), encodedValueField(t, null)) {
		t.Fatal("Nullable(T) with no nulls must encode its values exactly as bare T")
	}
	if bytes.Equal(bare, null) {
		t.Fatal("the framed type string must still distinguish the two declarations")
	}
}

// TestNullEncodesAsASingleKindTag: the other direction.
func TestNullEncodesAsASingleKindTag(t *testing.T) {
	b, err := EncodeRow("t", []Column{{Name: "c", Type: "Nullable(UInt64)"}}, []any{nil})
	// encodedValueField(t, b) must be exactly []byte{KindNull}
}
```

Expected pre-fix failure: the second test errors with `lthash: unsupported value type <nil> for column "c" (Nullable(UInt64))`; the first fails at `ResolveColumnProfile` with `ErrUnsupportedColumnType`.

- [ ] **Step 2: Decide the null representation, and say why**

A null is the untyped Go `nil`, matched by `case nil:` in `encodeValue`'s type switch. The alternative — a named sentinel type — was considered and rejected: `nil` is what clickhouse-go's own `Append` takes for a nullable column, so `chexec.insertRows` needs no change; `reflect.DeepEqual` over `[]any` works; and the Q-D3 identity property falls out with no special case on the non-null side.

The cost is real and must be closed rather than waved away: a bug that produces `nil` for a *non*-nullable column would now hash as a null instead of erroring. Task 4's authority test closes it — add an assertion that every non-`Nullable` vector decodes to a non-nil value, so a decoder returning `nil` where it should not fails the build.

`partitionValueString` gains `case nil: return "", fmt.Errorf(...)`: a nullable column is **not** admissible as the partition column, because ClickHouse's own partition-id derivation for a null is not something this profile models. Reject it at `ResolveColumnProfile` time too, in `ValidateTableSchemaColumns`, so it is a startup refusal rather than a replay-time failure — the same lesson as measurement M5.

- [ ] **Step 3: Implement the decoder seam (Q-D6)**

In `nativeColumnValue`, **before** the concrete type switch:

```go
// Spec Q Q-D6: Nullable(T) is handled through a narrow interface plus one
// exported-field lookup, never a per-instantiation type switch. proto.ColNullable
// is generic, so a type switch would need one case per inner type and would drop
// silently to default for any inner type added later — the negative-rule failure
// mode Spec N D2 exists to eliminate.
//
// IsElemNull(int) bool is reachable by a plain interface assertion (measured).
// The inner Values column is not: ColNullable[T].Values is a generic field with
// no non-generic accessor anywhere in ch-go, so reaching it needs exactly one
// reflect.FieldByName. That lookup runs once per nullable column per block, not
// per row, and its result is an ordinary proto.ColResult that recurses into the
// dispatch below.
if nullable, ok := col.(interface{ IsElemNull(int) bool }); ok {
	if nullable.IsElemNull(i) {
		return nil, 1, nil
	}
	inner, err := nullableInnerColumn(col)
	if err != nil {
		return nil, 0, err
	}
	v, n, err := nativeColumnValue(inner, i)
	return v, n + 1, err
}
```

`nullableInnerColumn` is the single reflection site, in its own function with its own test, and it fails closed: a missing, unexported or non-`ColResult` `Values` field is an error, never a fallthrough. The `+1` on the byte count is the null mask byte, matching ch-go's own encoding (`ColNullable.EncodeColumn` writes `Nulls` then `Values`).

**Do not hoist the reflection out of the per-row loop in this task.** It is one `FieldByName` per row today; if profiling later shows it matters, cache it in `nativeDataBlock` at decode time — but correctness first, and the authority test is what will keep a cache honest.

- [ ] **Step 4: Implement the other three sides**

- `ResolveColumnProfile`: `Nullable(<inner>)` resolves the inner declaration recursively, requires it to be a non-`Nullable` admitted family (`Nullable(Nullable(T))` is not legal ClickHouse and is rejected by that rule alone), sets `Elem`, `KindTag = KindNull`, `GoType = Elem.GoType`, and `ScanGoType = reflect.PointerTo(Elem.ScanGoType)` — clickhouse-go's nullable `ScanRow` writes through a `**T` destination (`lib/column/nullable.go:74-94`).
- Canonical spelling: `Nullable(` + the inner canonical form + `)`. `Nullable( UInt64 )` → `Nullable(UInt64)`; `Nullable(Decimal(18,4))` → `Nullable(Decimal(18, 4))`.
- `parseValue`: the CSV lane has no unambiguous null literal — `\N` is ClickHouse's TSV convention and CSV uses an unquoted empty field — so `FamilyNullable` maps the exact unquoted token `\N` to `nil` and delegates everything else to the inner family. Add a comment saying the CSV lane is legacy and the Native lane is authoritative.
- `chexec.derefScan`: dereference the outer pointer; a nil inner pointer is `nil`, otherwise recurse into the inner family's conversion.

- [ ] **Step 5: Enumerate `Nullable(T)` for every admitted scalar**

Add `Nullable(T)` vectors and sample columns for `String`, `Bool`, `Float32`, `Float64`, `UInt8`…`UInt64`, `Int8`…`Int64`, `Date`, `Date32`, `DateTime`, `DateTime('UTC')`, `DateTime64(3, 'UTC')`, `UUID`, `FixedString(32)`, `Decimal(18, 4)`. Each sample column carries **two rows**: one set, one null, so both branches of the seam run for every inner type.

`Nullable(FixedString(N))` and `Nullable(Decimal(P, S))` are the two whose ClickHouse read-back is least certain — `ColNullable[[32]uint8]` and `ColNullable[Decimal128]` were measured to infer correctly, but clickhouse-go's scan destination for them was not. If either fails Task 17's docker round trip, **remove it from the vectors and record it in the spec's out-of-scope section** rather than special-casing it; a type in the profile that ClickHouse cannot read back breaks executor equivalence.

- [ ] **Step 6: Run**

```bash
bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors
```

**Verification:** `bazel test //pkg/replay/... //pkg/lthash:lthash_test --test_output=errors`

**Commit:** `feat(nativepayload): decode Nullable through a generic seam (Spec Q Q-D6)`

### Task 16: `chexec` materializes the Phase 2 types, and the two executors are proved equal on them

**Files:**
- Modify: `pkg/replay/chexec/materializer.go` (`createScratch`, `insertRows`, `readBack`, `derefScan`), `BUILD.bazel`, `go.mod`/`go.sum` (`github.com/shopspring/decimal` and `github.com/google/uuid` become direct deps of this package)
- Modify: `pkg/integration/chreplay_test.go`

**Interfaces produced:** `chexec.columnInsertValue(profile payloadexec.ColumnProfile, v any) (any, error)` — the inverse of `derefScan`, converting a canonical value back into the form clickhouse-go's `Append` accepts. It lives in `chexec`, not `payloadexec`, because it is the only place that may depend on clickhouse-go.

- [ ] **Step 1: Write the equivalence extension first (red)**

`pkg/integration/chreplay_test.go` already holds `TestReplayCHExecutorNativePayloadMatchesInProcessRoot` (`:398`), which runs the same signed Native payload through both `payloadexec`'s in-process materializer and `chexec`'s and asserts the computed state roots are equal. Add `TestReplayCHExecutorPhase2TypesMatchInProcessRoot` alongside it, over a schema declaring one column per Phase 2 family: `Date32`, `FixedString(16)`, `UUID`, `Decimal(9, 2)`, `Decimal(18, 4)`, `Decimal(38, 10)`, `Decimal(76, 0)`, `Nullable(UInt64)`, `Nullable(String)`, `Nullable(DateTime64(3, 'UTC'))`, with at least two rows so every nullable column carries one null and one set value.

Expected pre-fix failure: `unsupported column type "UUID" for ClickHouse executor (column "u")` from `createScratch`.

- [ ] **Step 2: Insert path**

`insertRows` currently appends `r.Values...` directly. Route each value through `columnInsertValue`: `[16]byte` → `uuid.UUID`; `lthash.Decimal` → `decimal.NewFromBigInt(<two's-complement LE decode>, -int32(scale))`; `nil` → `nil` (clickhouse-go's nullable `Append` takes it); everything else identity. The decimal round trip must be exactly inverse to Task 14's `EncodeDecimal`, and a unit test in `chexec` asserts `columnInsertValue(derefScan(x)) == x` for each Task 14 exactness vector.

- [ ] **Step 3: DDL path**

`createScratch` interpolates `c.Type` verbatim. Change it to interpolate `payloadexec.CanonicalColumnType(c.Type)`'s result, so a legacy spelling that survived into a schema cannot reach ClickHouse in a form that reads back differently from what we hash. The existing `supportedColumnType` guard stays as defence in depth.

- [ ] **Step 4: Sync the module graph**

```bash
cd /Users/uranuswch/Dev/housegate/hg-specq
go mod tidy
bazel mod tidy && bazel run //:gazelle
git diff --stat go.mod go.sum
```

Expected: `github.com/shopspring/decimal` moves from the `// indirect` block to a direct require. If anything else moves, stop and look — this task should not change any version.

- [ ] **Step 5: Run**

```bash
bazel test //pkg/replay/... --test_output=errors
bazel test //pkg/integration:integration_test --test_filter='TestReplayCHExecutor' --test_output=errors
```

**Verification:** `bazel test //pkg/integration:integration_test --test_filter='TestReplayCHExecutor' --test_output=errors`

**Commit:** `feat(chexec): materialize UUID, Decimal, Nullable and Date32 (Spec Q §4.3)`

### Task 17: `Decimal` exactness and the Phase 2 canonical spellings, against a real ClickHouse

**Files:**
- Modify: `pkg/integration/chcolumntype_test.go`
- Modify: `.github/workflows/ci.yml` only if Task 8 Step 2 created a new target

**Interfaces:** test-only.

- [ ] **Step 1: The spellings extend for free, and that is the point to check**

Task 8's round trip iterates `AdmittedColumnTypeVectors()`, so `UUID`, `Date32`, every `FixedString` width, every `Decimal` form and every `Nullable(T)` are now covered automatically. Run it before writing anything new:

```bash
bazel test //pkg/integration:integration_test --test_filter='TestColumnProfileCanonicalSpellings' --test_output=errors
```

Every disagreement it reports is a real Q-D5 finding and must be resolved by **changing our canonical form to match ClickHouse**, not by relaxing the assertion. Expect at least one: Task 14 Step 3 deliberately left `Decimal`'s comma-space undecided pending this run. Record the resolved spelling in the spec document's Q-D5 list in the same commit.

- [ ] **Step 2: Add the exactness test (Spec Q §4.5)**

```go
// TestDecimalValuesSurviveClickHouseExactly proves Q-D3's "never a float, never
// a decimal string": each value is INSERTed, read back, and its canonical
// encoding compared byte-for-byte against the value that went in. A float round
// trip corrupts every row in this table.
func TestDecimalValuesSurviveClickHouseExactly(t *testing.T) { /* Task 14 Step 1's table */ }
```

It must assert on the **canonical encoding bytes**, not on a formatted string: a string comparison would pass for a value that lost precision and was re-rendered.

- [ ] **Step 3: Add the `Nullable` read-back verdict**

For each `Nullable(T)` vector, INSERT one null and one set row and assert the read-back values are `nil` and the bare-`T` value respectively. Task 15 Step 5 flagged `Nullable(FixedString(N))` and `Nullable(Decimal(P, S))` as the uncertain pair — this is where the verdict is taken. If clickhouse-go cannot scan either, remove it from `AdmittedColumnTypeVectors()` and add it to the spec's §6 out-of-scope list in the same commit, with the `ColumnConverterError` text as the reason.

- [ ] **Step 4: Confirm CI reaches the target**

```bash
bazel query 'attr(srcs, chcolumntype_test.go, //pkg/integration:all)'
grep -n 'pkg/integration' .github/workflows/ci.yml
```

Expected: the query prints `//pkg/integration:integration_test`, which ci.yml already lists at `:122-127`. If a separate target exists, add it to that list now — a docker-bound target not in the list never runs in CI, and this one is the only proof that the canonical spellings are real.

**Verification:** `bazel test //pkg/integration:integration_test --test_filter='TestColumnProfileCanonicalSpellings|TestDecimalValuesSurviveClickHouseExactly' --test_output=errors`

**Commit:** `test(integration): prove Decimal exactness and the Phase 2 spellings (Spec Q §4.5/§4.6)`

---

## Part E — Q-D4: the profile identity bump and the chaining gate

### Task 18: give housegate an `ExecutorProfileID` constant, and enforce the chaining constraint

**A finding that changes what this task is:** housegate has **no `ExecutorProfileID` constant to bump.** `replay.SafeSnapshotManifest.ExecutorProfileID`, `ReplayJob.ExecutorProfileID` and `Executor.GenesisSnapshot(_, _, executorProfileID)` all take the value from the caller, and the only string literals in-tree are test fixtures (`"housegate-replay-mvp-v0"` at `pkg/replay/types_test.go:10`, `"executor-1"` at `pkg/replay/verifier_test.go:361`). The profile identity is chosen today by arbiter-core / sentio-node. Spec Q Q-D4 says "the profile identity bumps" without saying where it lives; **it has to be created before it can be bumped**, and it belongs here, because this repo is what defines the profile.

**Second finding:** the Q-D4 gate is **untested**. `pkg/replay/verifier_test.go:326` covers `validateExecutionResult`'s `result.ExecutorProfileID != job.ExecutorProfileID` (`verifier.go:331`), but nothing covers `snap.ExecutorProfileID != job.ExecutorProfileID` (`verifier.go:87`) — the snapshot-chaining check that is the entire reason Phase 2 must land before devnet2. Spec Q §4 item 7 asks for exactly this test and it does not exist.

**Files:**
- Modify: `pkg/replay/payloadexec/exports.go`, `pkg/replay/verifier_test.go`, `CLAUDE.md`
- Modify: `docs/superpowers/specs/2026-08-25-storage-integrity-column-type-profile-design.md` (record the constant's name and both phase values)

**Interfaces produced:**

```go
// ExecutorProfileID names the pinned executor profile: the row-id derivation,
// the canonical row encoding, and the admitted column-type set. replay.Verifier
// requires a job's profile to equal the previous safe snapshot's
// (verifier.go:87), so this value may not change once a chain is running —
// changing it mid-chain makes every job unchainable. Hosts (arbiter-core,
// sentio-node) must use this constant rather than a literal of their own.
//
// Spec Q Q-D4:
//   housegate-payloadexec-mvp-v1 — the pre-Q profile and everything Phase 1
//     admits, since Phase 1 only restores types this executor already replayed.
//   housegate-payloadexec-mvp-v2 — Phase 2, which adds UUID, Decimal, Nullable,
//     Date32 and the non-32 FixedString widths. A verifier on v1 meeting any of
//     them fails to decode, which is a pre-receipt error and therefore a local
//     refusal to attest (Appendix C.4) — the bump makes that refusal explicit
//     rather than incidental.
const ExecutorProfileID = "housegate-payloadexec-mvp-v2"
```

- [ ] **Step 1: Write the chaining gate test (red-by-deletion)**

```go
// TestVerifyRejectsAProfileChangeMidChain is Spec Q Q-D4's gate: a job whose
// executor_profile_id differs from the previous safe snapshot's is refused
// before any executor runs. This is why Phase 2 lands before the first devnet2
// chain — after it, the same change is a hard fork.
func TestVerifyRejectsAProfileChangeMidChain(t *testing.T) {
	// snapshot with ExecutorProfileID "housegate-payloadexec-mvp-v1",
	// job with "housegate-payloadexec-mvp-v2"
	// want: error containing "executor_profile_id mismatch", and the executor
	// must not have been called (assert on a spy Executor's call count).
}
```

Prove it bites: comment out `verifier.go:87-89` and re-run. Expected without the guard: the test fails because `Verify` returns an attestation instead of an error, **and** the spy reports one executor call. Restore the guard.

The "executor must not have been called" half is what makes this a real gate rather than a message assertion — a profile mismatch must be a pre-receipt refusal, never a signed mismatch.

- [ ] **Step 2: Add the constant**

Put it in `exports.go` next to `RowIDProfileID`, with the doc comment above verbatim.

- [ ] **Step 3: Add the phase-consistency test**

```go
// TestExecutorProfileIDMatchesTheAdmittedSet fails if the profile string and
// the admitted column-type set drift apart. Adding a family to the profile
// without bumping the id is the mistake Q-D4 exists to prevent.
func TestExecutorProfileIDMatchesTheAdmittedSet(t *testing.T) {
	if ExecutorProfileID == "housegate-payloadexec-mvp-v1" {
		for _, phase2 := range []string{"UUID", "Decimal(18, 4)", "Nullable(UInt64)", "Date32", "FixedString(16)"} {
			if SupportedColumnType(phase2) {
				t.Errorf("%q is admitted but the profile id is still v1", phase2)
			}
		}
	}
}
```

Ship it in the Phase 1 release with `ExecutorProfileID = "housegate-payloadexec-mvp-v1"` — the test then guards Phase 1 too, and Task 18's bump to `v2` is a one-line change that this test forces to happen at the right moment.

- [ ] **Step 4: Document the host contract**

`CLAUDE.md`'s `pkg/replay/` bullet gains one sentence: the profile identity is `payloadexec.ExecutorProfileID`, hosts must use the constant rather than a literal, and it may not change once a chain is running.

- [ ] **Step 5: Run**

```bash
bazel test //pkg/replay:replay_test //pkg/replay/payloadexec:payloadexec_test --test_output=errors
```

**Verification:** `bazel test //pkg/replay/... --test_output=errors`

**Commit:** `feat(replay): pin ExecutorProfileID and gate mid-chain profile changes (Spec Q Q-D4)`

---

## Part F — arbiter-core picks up the widened validator

### Task 19: coordinate the arbiter-core bump with Spec O, do not duplicate it

Spec L D1's enforcement lives in **arbiter-core's `snode.New`**, not in housegate: `ValidateTableSchemaColumns` and `CanonicalizeTableSchemaColumnTypes` have **no non-test callers in this repo** (verified). So the entire operational effect of Phase 1 — a node whose SI tables declare a `DateTime` starting instead of refusing — arrives through arbiter-core's housegate bump, which is Spec O's `arbiter-core tag` step.

**This task writes no arbiter-core code and cuts no tag.** Spec O's plan owns the pin chain (engine tag → housegate pin + merge + tag → arbiter-core tag → sentio-node pins + tag) and duplicating it here would produce two plans racing the same version bumps.

- [ ] **Step 1: Hand off Phase 1 with an explicit statement of what changed for the consumer**

Write this into the Phase 1 release notes / PR description, in these terms:

- `payloadexec.SupportedColumnType` / `ValidateColumnType` / `ValidateTableSchemaColumns` now **accept** `Date`, `DateTime`, `DateTime(<tz>)`, `DateTime64(P)`, `DateTime64(P, <tz>)`. Nodes that refused to start now start.
- They now **reject** `FixedString(N)` for every `N != 32` (Q-D7, measurement M1). This is a narrowing. Any deployment declaring another width was already unable to replay a Native payload for that column, so the change converts a per-statement replay failure into a startup refusal — but it *is* a behaviour change and arbiter-core's own fixtures must be checked for a non-32 width before the bump.
- `partitionValueString` accepts temporal partition columns; a table partitioned on a `Date`/`DateTime` column is newly replayable.
- No digest changed. `ExecutorProfileID` stays at `housegate-payloadexec-mvp-v1`.

- [ ] **Step 2: Name the arbiter-core-side checks Spec O must run**

Add these to Spec O's arbiter-core step rather than executing them here:
1. `snode.New` startup validation still refuses an unsupported type, with the new message text (`admitted profile: ...`) — any arbiter-core test asserting the old whitelist string needs updating.
2. Grep arbiter-core fixtures and configs for `FixedString(` with a width other than 32.
3. arbiter-core's own suite green against the Phase 1 housegate tag.

- [ ] **Step 3: State the Phase 2 gate**

Phase 2's `ExecutorProfileID` bump to `housegate-payloadexec-mvp-v2` must reach arbiter-core and sentio-node **before the first devnet2 chain starts**, and the constant must be adopted in place of any host-side literal. After the first chain, the bump is a hard fork (Q-D4). Record this in the closure roadmap §3 diagram's `M (devnet2)` node as an explicit precondition.

**Verification:** none in this repo — this task's output is text in the Phase 1 handoff and three added checks in Spec O's plan. Do not commit code.

**Commit:** `docs(spec): record the arbiter-core handoff for the widened type profile (Spec Q §5)`

---

## Where Spec Q is wrong or under-specified against the source

Recorded here rather than silently worked around, so the spec can be corrected. Each item was measured, not reasoned.

1. **§1e's premise is factually wrong.** "`proto.ColAuto` appears to contain no `FixedString` inference at all" — it has seven, generated into `col_auto_gen.go:47-59` for widths 8/16/32/64/128/256/512. The *conclusion* (validator wider than decoder) is right and is in fact worse than stated: the validator admits 16,777,215 widths and the Native lane decodes one. §1e should be rewritten around the real numbers, because the "seven inferable widths" set is what Q-D7's Phase 2 widening targets.
2. **Q-D3 does not model `nativeBlockColumnPositions`, and `Decimal` cannot pass it.** `Results.decodeAuto` stores the *inferred inner* column, whose `Type()` reports `Decimal64` / `Decimal128`, while `native.go:172` compares that against the schema's `Decimal(18, 4)` with plain `!=`. Measured: `native block column "c" type "Decimal64" does not match schema type "Decimal(18,4)"` — a rejection one stage before `nativeColumnValue` is even consulted. The obvious fix (compare against the downcast name) is unsafe: `Decimal(18,4)` and `Decimal(18,2)` both downcast to `Decimal64`, so the payload's declared scale would go unbound while `lthash` frames the *schema's* scale into every row. Q-D3's "the scale is already covered because the canonical type string is framed into the row element" is true of the schema side and false of the payload side. Task 12 exists only because of this.
3. **A temporal partition column is not replayable, and Phase 1 makes that worse.** `partitionValueString` (`executor.go:427-469`) has no `time.Time` case. Measured: `partition column "d": unsupported partition value type time.Time`, for `Date` and `DateTime` alike. Restoring the temporal types to the validator without fixing this converts a loud startup refusal into a late per-statement replay failure. Spec Q's §1a evidence table says three of four components "already handle" the temporal types; a fourth component it does not enumerate does not.
4. **There is no `ExecutorProfileID` to bump.** Q-D4 speaks of "the profile identity bumps" as if the value lived here. It does not: every occurrence in this repo is a struct field or a test literal, and the string is chosen by arbiter-core / sentio-node. Q-D4 needs a preceding step that *creates* the constant. Related: the gate Q-D4 leans on — `snap.ExecutorProfileID != job.ExecutorProfileID` at `verifier.go:87` — has **no test**. `verifier_test.go:326` covers the sibling check at `:331` and is easy to mistake for it.
5. **Q-D4's "Phase 1 does not bump" and Q-D7's "widen the decoder" are in tension.** Q-D7 offers "either the decoder gains the other widths or the authority admits only 32", but the first option adds capability, which by Q-D4's own reasoning requires a bump — and Phase 1 must not bump, because Spec O's release depends on it. The two decisions only compose if Phase 1 narrows and Phase 2 widens. Q-D7 should say so.
6. **Q-D2 lists `Date32` in Phase 1's prose before its own caveat removes it.** Measured: `unsupported column type *proto.ColDate32` — no decoder case, and `chexec.isTemporalColumnType` and `derefScan` exclude it too. It is Phase 2. The caveat is correct; the sentence above it is not, and the caveat's stated worry (that `encodeTime`'s `Date` prefix would mistreat it) turns out to be unfounded — measured correct at `1900-01-01`, because `ColDate32` values are always UTC midnight and therefore exact multiples of 86400 even when negative.
7. **Q-D5's example spellings are internally inconsistent.** It lists `DateTime64(3, 'UTC')` with a comma-space and `Decimal(18,4)` without one, in the same sentence, and says only "whitespace normalized". Whitespace inside parameter lists is exactly the case that needs a rule. `ColumnType.With` joins with `", "` (`column.go:165-174`), so ch-go reconstructs `DateTime64(3, 'UTC')` regardless of the wire spelling; `Decimal`'s canonical form has to follow whatever `system.columns` reports and cannot be assumed.
8. **Q-D6 says the inner column is reachable through "a narrow interface"; half of it is not.** `IsElemNull(int) bool` is reachable by a plain assertion — measured. `ColNullable[T].Values` is a generic *field* with no non-generic accessor anywhere in `col_nullable.go`, so reaching it needs one `reflect.FieldByName`. Q-D6's design intent survives (no per-instantiation type switch, no silent `default`), but the spec should say reflection is used, and where.
9. **Q-D1 does not say where the cross-component test lives, and the obvious place is a cycle.** `payloadexec` cannot import `nativepayload` or `chexec` (both import it). The only package that already sees all four components is `chexec`, and only from an internal test file. Worth recording, because the natural first attempt — a test in `payloadexec` — does not compile.
10. **§4 item 3 under-states the `chexec` work.** Extending the equivalence test to the new types is not just adding columns: clickhouse-go's scan destinations for `UUID`, `Decimal` and `Nullable` are `uuid.UUID`, `decimal.Decimal` and `**T`, none of which is the canonical value type, so the profile needs a second `ScanGoType` field and an insert-side inverse. `github.com/shopspring/decimal` also becomes a direct module dependency.
11. **Minor:** §4 item 2's "a `DateTime` column that fails validation before the change and passes after" is already the shape of `column_types_test.go`'s frozen `rejectedTypeMatrix` / `supportedTypeMatrix`, which the spec does not mention. Moving an entry between those two lists *is* the red test, and it is cheaper and more durable than writing a new one.

## Corrections found during Phase 1 execution

Recorded against the plan itself, in the same spirit as the section above. Each was measured while executing Tasks 0-9.

12. **Task 5's `FixedString(32)` golden row is one NUL byte too long.** The prose says "`04` + `6669786564` + 27 × `00`", which is right — 1 kind tag + 32 payload bytes = 33 bytes — but the hex literal beside it carries 28 zero bytes. Every other one of the 19 measured rows reproduced exactly. The committed golden builds the padding with `strings.Repeat("00", 27)` so the count cannot drift from the prose again.

13. **Task 3 must run after Task 6, not before it.** Task 3 Step 2 deletes `chexec.isTemporalColumnType` and dispatches `derefScan` on `profile.Family`, but Task 2 populates the authority with "exactly today's admitted set and nothing more" — which has no temporal families. Executed in the plan's order, Task 3 makes `chexec.supportedColumnType` reject `Date`/`DateTime`, breaking `materializer_time_test.go` and the docker temporal replay tests, with Task 6 restoring them three tasks later. Phase 1 was executed as 0, 1, 2, 5, 6, 3, 4, 7, 8, 9 so every commit is green. Task 5 moving earlier is free — it pins `lthash` value encodings, which nothing in Tasks 2/3/4/6 touches — and capturing it before the widening is what the task is for.

14. **Task 3's predicted pre-fix failure names the wrong member.** It expects `chexec and authority disagree on "FixedString(17)"`, but the authority still admits every width up to `0xFFFFFF` until Task 9 narrows it, so chexec and the authority agree on `FixedString(17)` at Task 3. The seven live disagreements are `FixedString(0)`, `FixedString(x)`, `FixedString(-1)`, `DateTime64()`, `DateTime64(10)`, `DateTime()` and `DateTime('Not/AZone')` — all cases where chexec's prefix match accepted a parameter spelling the authority rejects. The guard bites; only the named member was wrong.

15. **Task 8's "Task 9 does not have to rewrite this test" is wrong.** The width-4 canonicalization cases are driven through `CanonicalColumnType`, which *validates* as well as canonicalizes, so Task 9's narrowing makes `CanonicalColumnType("FixedString( 4 )")` an error. Task 9 drops those three rows; the width-32 spellings exercise the identical tolerance.

16. **Task 2's `ColumnFamily` block cannot declare the Phase 2 families.** Its own Step 4 asserts that every declared family appears as the `Family` of at least one admitted vector, so declaring `FamilyUUID`/`FamilyDecimal`/`FamilyNullable`/`FamilyDate32` ahead of their vectors fails that assertion. Only live families are declared, and an `allColumnFamilies` slice makes the closed set checkable in both directions. `ColumnProfile.Elem` and `.Scale` are likewise deferred to Phase 2 rather than shipped as fields nothing sets.

17. **Task 2 needs the exported `lthash.Kind*` constants that Task 4 lists.** The authority's `KindTag` column cannot be written without them, so the export moved from Task 4 to Task 2. Task 4 keeps the assertion that uses them.

18. **Q-D5's comma-space is confirmed by ClickHouse itself, not only by ch-go.** Task 8's round trip measured `system.columns` reporting `DateTime64(3, 'UTC')` for a table created as `DateTime64(3,'UTC')`. Removing the space from the canonical form fails both timezone-bearing `DateTime64` cases against a real server, so the two normalizers agree rather than merely each being self-consistent.

19. **Two existing partition tests encoded measurement M5 as an expectation.** `TestPartitionIDForRowRejectsUndefinedTypedPartitionValue` asserted that a `Date` partition column *fails*, and `TestPartitionIDForRowTypedValueGoldenMatrix` declared every partition column as the synthetic type `"fixture"`, which the authority does not admit once `PartitionIDForRow` resolves the column. Task 7 repoints both: the first at a genuinely undefined Go type plus a temporal column carrying a non-time value, the second at the real declared type each value belongs to.

## Self-review

Run after the plan is written, before execution.

**1. Spec coverage** — see the map below; every decision and every §4 acceptance item has a task.

**2. Placeholder scan** — no "TBD", no "similar to Task N", no test described without its assertion. Two values are deliberately left to be resolved by execution and each names the step that resolves it: `Decimal`'s canonical comma-space (Task 14 Step 3, resolved by Task 17 Step 1) and the `Nullable(FixedString(N))` / `Nullable(Decimal(P,S))` admission verdict (Task 15 Step 5, resolved by Task 17 Step 3).

**3. Type consistency**
- `payloadexec.ColumnProfile` (Task 2) is read by `chexec.newScanDest`/`derefScan` (Task 3), `nativepayload.nativeBlockColumnPositions` (Tasks 3, 12) and the authority test (Task 4). `NativeWireType` is introduced in Task 2 and **deleted** in Task 12 Step 3 — no other task may add a reader.
- `ColumnProfile.ScanGoType` (Task 13) is used by `newScanDest` and `derefScan` only; `GoType` remains the canonical value type asserted by the authority test.
- `lthash.Kind*` exported constants (Task 4) ↔ `ColumnProfile.KindTag` (Task 2) ↔ the freeze test (Tasks 5, 10).
- `lthash.Decimal` (Task 14) is produced by `nativeColumnValue`, `parseValue` and `derefScan`, and consumed by `encodeValue` and `chexec.columnInsertValue` (Task 16). One serializer, `payloadexec.EncodeDecimal`, has all three producers.
- `payloadexec.ExecutorProfileID` (Task 18) is the only profile literal; `pkg/replay/types_test.go`'s `"housegate-replay-mvp-v0"` and `verifier_test.go`'s `"executor-1"` are fixtures for the generic manifest machinery and stay as they are.
- `AdmittedColumnTypeVectors()` is the single enumeration driving Task 4's authority test, Task 8's ClickHouse round trip and Task 17's exactness pass. Adding a family without adding a vector fails Task 4 Step 1's completeness assertion.

**4. Phase independence** — Tasks 0-9 touch no file Tasks 10-19 create, and none of them reads `lthash.KindUUID`/`KindDecimal`/`KindNull`. Phase 1 can be cut and shipped at Task 9 without carrying any Phase 2 partial state.

## Spec coverage map

| Spec Q section | Requirement | Tasks |
|---|---|---|
| §1a | temporal types rejected only by the validator | 6 |
| §1b | `UUID` / `Decimal` / `Nullable` absent downstream of `ColAuto` | 13, 14, 15 |
| §1c | `Nullable` needs a generic seam | 15 |
| §1d | canonical spelling is hashed | 6 Step 3, 8, 14 Step 3, 17 Step 1 |
| §1e | validator/decoder `FixedString` mismatch, measured not inferred | 1 (M1), 9 |
| §3 Q-D1 | one authority, four derivations | 2, 3 |
| §3 Q-D1 | the agreement is a test | 4 |
| §3 Q-D2 | temporal types, no protocol change | 6, 7 |
| §3 Q-D2 | `Date32` measured before admitting | 1 (M2), 11 |
| §3 Q-D2 | timezone kept, not stripped | 5 Step 1 (evidence), 6 Step 3 |
| §3 Q-D3 | kind tags appended after the six | 10 |
| §3 Q-D3 | `UUID` = 16 storage-order bytes | 13 |
| §3 Q-D3 | `Decimal` = raw scaled integer, two's-complement LE | 14 |
| §3 Q-D3 | `Decimal512` excluded | 14 Step 2 |
| §3 Q-D3 | `Nullable` null/non-null identity | 15 Step 1 |
| §3 Q-D3 | `housegate-row-mvp-v0` does not bump | 5 Step 2, 10 Step 3 |
| §3 Q-D4 | profile identity bumps | 18 |
| §3 Q-D4 | chaining constraint enforced, not documented | 18 Step 1 |
| §3 Q-D5 | canonical spellings written down | 6 Step 3, 9 Step 2, 13 Step 4, 14 Step 3, 15 Step 4 |
| §3 Q-D5 | proved by ClickHouse round trip | 8, 17 Step 1 |
| §3 Q-D6 | narrow interface, no per-instantiation switch | 15 Step 3 |
| §3 Q-D6 | every `Nullable(T)` enumerated | 15 Step 5, 17 Step 3 |
| §3 Q-D7 | widths aligned to what decodes | 1 (M1), 9, 11 |
| §4.1 | the authority test | 4 |
| §4.2 | Phase 1 is a pure widening, byte-identical | 5, and the "golden unmodified" check in 6/9/10 |
| §4.3 | executor equivalence over the new set | 16 |
| §4.4 | `Nullable` null/non-null both directions | 15 Step 1, 17 Step 3 |
| §4.5 | `Decimal` exactness | 14 Step 1, 17 Step 2 |
| §4.6 | canonical spelling round trip | 8, 17 Step 1 |
| §4.7 | profile gate | 18 Step 1 |
| §4.8 | arbiter-core stays green | 19 |
| §5 | Phase 1 ships with Spec O | Tasks 0-9, handoff in 19 Step 1 |
| §5 | Phase 2 before devnet2 | Tasks 10-18, gate in 19 Step 3 |
| §5 | Q-D7 lands with whichever phase 1e assigns | 9 (narrow, Phase 1) + 11 (widen, Phase 2) |
| §6 | recorded debt | 17 Step 3 (if a `Nullable(T)` is dropped), 19 Step 3 |
