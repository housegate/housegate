# Storage-Integrity Rewriter Contract V2 Implementation Plan (sub-project 3, rewriter half)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `STORAGE_INTEGRITY_CONTRACT_V2` and `StorageIntegrityArgs.reserved_databases` to the rewriter contract and implement them in both engines: V2 activates the storage-integrity (SI) surface by version even with an empty table map (so session `SET`, `SYSTEM` and unmodelled statements stay refused while no table is Active); under V2 the protected physical namespace is the union of the map-derived databases and `reserved_databases`, so `hg_safe` / `hg_unsafe` / `hg_promote` stay unaddressable with an empty map; and V2 accepts a plain `DROP TABLE [IF EXISTS] name[, name...] [SYNC]` of logical SI tables by dropping only their ordinary physical tables. V1 ignores `reserved_databases`, and V1 behaviour stays byte-for-byte unchanged. The shared behaviour corpus gains a per-case `contract_version` and the V2 cases, and the three repositories are released and pinned in order.

**Architecture:** Three repositories change, in order. `rewriter-proto` adds the enum value, the `reserved_databases = 5` field and their comments (Task 1). `rewriter-go` (native engine) gains one activation predicate, `nameresolve.StorageIntegritySurfaceActive`, that replaces every table-count check and makes `doRewrite` accept and acknowledge V1 or V2 (Task 2); a token-level grammar check `engine.PlainDropTable` plus one slot per DROP name lets `handlers.RewriteWrite` exempt authorized logical SI targets of a plain V2 DROP TABLE exactly the way INSERT targets are exempted today (Task 3); the three physical-namespace helpers every handler already funnels through (`LookupStorageIntegrityPhysical`, `IsStorageIntegrityPhysicalDatabase`, `StorageIntegrityPhysicalDatabases`) add the V2 reserved databases, so every existing protection (direct addressing, DDL/DCL/SYSTEM/USE/SHOW targets, table-function carriers including heredocs) covers them with no handler change (Task 4); the shared corpus and both Go harness gates learn `contract_version` and `reserved_databases` (Task 5). `rewriter-grpc` (C++ service, GitHub `housegate/rewriter`) mirrors each step with `storageIntegritySurfaceActive` / `storageIntegrityDropContract` / `storageIntegrityPlainDrop` and a reserved-aware `isStorageIntegrityPhysicalDatabase` / `storageIntegrityPhysicalDatabases` (Tasks 6–9), re-pins its mandatory paired snapshot-qualification manifest (Task 10), and the controller releases everything (Task 11). Housegate is not changed here; the final section hands plan B everything it needs.

**Tech Stack:** protobuf with buf v1.71.0, protoc-gen-go v1.36.11, protoc-gen-go-grpc v1.5.1; Go 1.25 (rewriter-go), polyglot FFI v0.12.1 (`third_party/polyglot-src` at `d5fd24eec5efaa4444eed6e5f009044214ccdc84`, built with `make ffi`); C++ with ClickHouse 26.3 parsers (`clickHouse` submodule `1f0fb3353cd10c3f8558f14feb0652a77e871f95`), GoogleTest, Poco JSON, built and tested only on the private build box through CI.

**Spec:** `housegate/housegate` `docs/superpowers/specs/2026-09-24-dynamic-si-table-set-housegate-design.md` §4 H6, §8 (rewriter contract V2), §11.1, §12 step 1 and §14 "Mixed rewriter versions"; umbrella `2026-09-23-dynamic-si-table-set-design.md` §14 "Rewriter contract". Controller ruling of 2026-09-24 (§8.2 is being amended to match): V2 adds `repeated string reserved_databases` to `StorageIntegrityArgs`; housegate always sends `["hg_safe", "hg_unsafe", "hg_promote"]`; under V2 the protected set is the union of the map-derived databases and `reserved_databases`, also with an empty map; V1 ignores the field; a housegate lexical guard is not an acceptable substitute. Measured bases (2026-09-24, `origin/main`): rewriter-proto `d3844a5`, rewriter-go `1b6f0d3`, rewriter-grpc `7567dc5`, housegate `51c1141`. Out of scope: every housegate change (plan B: probe V2 cases, pins, sending V2).

## Global Constraints

- **Enum.** `STORAGE_INTEGRITY_CONTRACT_V2 = 2` in `enum StorageIntegrityContractVersion` (`proto/rewriter.proto`); Go `pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2`, C++ `rewriter::STORAGE_INTEGRITY_CONTRACT_V2`.
- **Field.** `repeated string reserved_databases = 5;` in `message StorageIntegrityArgs` (fields 1–4 are taken); Go `StorageIntegrityArgs.ReservedDatabases []string` / `GetReservedDatabases()`, C++ `reserved_databases()` / `add_reserved_databases()`, JSON corpus key `storage_integrity.reserved_databases`.
- **Reserved-namespace rule (both engines).** Only when the request carries V2: a database is protected if it is the database of any `safe_table` / `unsafe_table` in `tables` or appears in `reserved_databases`; the protection, codes and messages are exactly V1's for a map-derived physical database. V1 never reads the field and never validates it. Under V2 every entry must be a simple identifier, else `InvalidRewriteRequest` `storage-integrity reserved_databases entry "<value>" must be a simple identifier` (Go `%q` quoting), unacknowledged.
- **Activation rule (both engines).** The SI surface is active when the effective (last-wins) `TableNameRewrite` selection is dynamic and its `storage_integrity.contract_version == V2`, or its `storage_integrity.tables` is non-empty. An active request whose version is neither V1 nor V2 is `InvalidRewriteRequest` with message `storage-integrity contract version V1 or V2 is required`, unacknowledged. An accepted request acknowledges exactly the version it carried, on every response path.
- **V1 with an empty table map stays legacy** (measured on `1b6f0d3`): `SYSTEM RELOAD CONFIG` and `SET max_threads = 1` return `Success`, echo the SQL, `storage_integrity_contract_version = UNSPECIFIED`.
- **Plain DROP grammar (the only V2 exemption).** Tokens, after removing one trailing `;`, are exactly `DROP TABLE [IF EXISTS] name (, name)* [SYNC | NO DELAY]`, each name `ident` or `ident.ident` (plain or quoted). Anything else (`ON CLUSTER`, `TEMPORARY`, `IF EMPTY`, `FORMAT`, `SETTINGS`, a query parameter, a three-part name) keeps the V1 rejection. `DROP VIEW`, `DROP DICTIONARY`, `TRUNCATE`, `DETACH` never qualify.
- **Statement rejections reuse existing messages** (the only new messages are the two request-validation ones above): `storage-integrity table <db.t> accepts writes only through the signed statement lane`, `storage-integrity physical table <db.t> is not directly addressable`, `storage-integrity logical database <db> is not authorized by database_map`, `multi-table DROP/TRUNCATE is not supported`, `storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded`.
- **Shared corpus after Task 5:** 256 cases (237 existing + 19 new), 254170 bytes, FNV-1a/64 `7051648083520101593`, SHA-256 `5fe367217619243729045820135340ae061891b4c2f85606cd61994482072615`; rule `R8`: `contract_version` is `"V1"` or `"V2"`; `storage_integrity.reserved_databases` is an optional string array. The rewriter-go and rewriter-grpc copies are byte-identical.
- **rewriter-grpc test counts after Task 9:** `SpecG/StorageIntegrityGolden.*` = 256; `rewriter_tests` total = 1251 (1221 + 19 corpus cases + 11 new `TEST`s: 3 in Task 6, 4 in Task 7, 4 in Task 8).
- **Pins.** `PROTO_SHA` is the rewriter-proto `main` commit that merges Task 1; every consumer pins it by its pseudo-version `PROTO_PSEUDO` (`v0.2.1-0.<UTC yyyymmddhhmmss>-<sha12>`), not by the tag. `RG_SHA` is the rewriter-go `main` commit that merges Tasks 2–5.
- **Releases:** rewriter-proto `v0.3.0` on `PROTO_SHA`; rewriter-go `v0.13.0` on `RG_SHA` (FFI assets `libpolyglot_sql_ffi-linux-x86_64.so`, `libpolyglot_sql_ffi-macos-arm64.dylib`, `SHA256SUMS`); rewriter-grpc `v0.15.0` (Docker tag `0.15.0`). The workflows compute versions from the release day; the tags actually cut are authoritative.
- **Tests.** rewriter-go: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib SNAPSHOT_QUERY_ORDINARY=1 go test ./...` (Linux: `.so`); without `POLYGLOT_SQL_FFI_PATH` every engine test skips. rewriter-grpc: CI only (Plan decision P12).
- Never push to `main`; each repository changes through one feature branch `feat/si-contract-v2` and one PR. Plans and docs are not hard-wrapped.

## Review Focus

1. **Polyglot silently drops DROP modifiers.** `drop_table` carries no `ON CLUSTER`, `TEMPORARY`, `IF EMPTY`, `NO DELAY`, `FORMAT` or `SETTINGS` field (measured: `DROP TABLE db1.t ON CLUSTER c` regenerates as `DROP TABLE phys."db1.t"`), so an AST-only exemption would turn `DROP TABLE db1.t ON CLUSTER c` into a local drop and `DROP TEMPORARY TABLE t` (session database `db1`) into a drop of the SI table's ordinary table. Tests: Task 3 Step 1 `TestPlainDropTable` (22 spellings), `TestStorageIntegrityContractV2_DropRejections` rows "v2 on cluster", "v2 multi on cluster", "v2 if empty", and `TestStorageIntegrityContractV2_DropResolvesUnqualifiedTargetThroughContext` (TEMPORARY); Task 7 Step 1 `StorageIntegrityContractV2.DropExemptionRequiresPlainGrammar`.
2. **V1 must not move.** The activation predicate and the reserved-namespace helpers are shared by every handler; a slip that activates V1 with an empty map, lets V1 read or validate `reserved_databases`, or grants the DROP exemption to V1 changes V1 bytes. Tests: Task 2 Step 2 `TestStorageIntegrityContractV1_EmptyMapStaysLegacy` and `TestStorageIntegritySurfaceActive` row "V1 empty map stays inactive"; Task 3 Step 1 rows "v1 drop", "v1 drop if exists sync", "v1 multi", "v1 ordinary multi"; Task 4 Step 1 `TestReservedDatabasesIgnoredUnderV1` and `TestStorageIntegrityContractV1_IgnoresReservedDatabases` (every reserved-namespace statement, with and without a table map, byte-equal with and without a malformed `reserved_databases`); Task 5 corpus cases `si_v1_drop_if_exists_sync_rejected`, `si_v1_drop_unqualified_in_context_rejected`, plus all 237 existing cases unchanged apart from the added `contract_version` line; Task 6 Step 1 `StorageIntegrityContractV2.V1EmptyMapStaysLegacy`; Task 8 Step 1 `StorageIntegrityReservedDatabases.V1IgnoresReservedDatabases`.
3. **The empty-map hole in the protected namespace.** With an empty table map neither engine knew `hg_safe` / `hg_unsafe` / `hg_promote` (measured on the unfixed V2 prototype: `SELECT * FROM hg_safe.x` and `SELECT * FROM merge($tag$hg_safe$tag$, 'x')` were `Success`; `INSERT INTO hg_unsafe.x ...` was refused only by the incidental `database_map` check), so a direct write into `hg_unsafe` could bypass the signed lane. Every surface must match V1's map-derived protection exactly, and the union with map-derived databases must hold. Tests: Task 4 Step 1 `TestStorageIntegrityContractV2_ReservedDatabasesMatchV1Protection` (11 statements, whole-response `proto.Equal` against the V1 map-derived answer: SELECT, INSERT VALUES, INSERT SELECT, TRUNCATE TABLE, DROP TABLE, TRUNCATE DATABASE, tagged heredoc `merge`, USE, SHOW TABLES FROM, SYSTEM), `TestStorageIntegrityContractV2_ReservedDatabasesUnionWithTableMap`, `TestReservedDatabasesProtectedUnderV2` (unqualified table in a reserved session database); Task 5 corpus cases `si_v2_reserved_*`; Task 8 Step 1 `StorageIntegrityReservedDatabases.V2EmptyMapMatchesV1Protection` and `.UnionWithTableMap`.
4. **Selection and target resolution.** A V2 dynamic option followed by a static option must neither acknowledge V2 nor grant the DROP exemption (the static option is the effective selection); `USE db1; DROP TABLE t` resolves through `upstream_logical_database_in_context`, must be rewritten, and must stay classified `is_storage_integrity` so commitgate still sees a DROP of an SI table; a logical database missing from `database_map` must be refused. Tests: Task 3 Step 1 `TestStorageIntegrityContractV2_StaticSelectionShadowsDropExemption`, `TestStorageIntegrityDropContract` row "static selection shadows V2", `TestStorageIntegrityContractV2_DropResolvesUnqualifiedTargetThroughContext`, `TestStorageIntegrityContractV2_DropRequiresAuthorizedLogicalDatabase`; Task 5 corpus case `si_v2_drop_unqualified_in_context`; Task 7 Step 1 `StorageIntegrityContractV2.StaticSelectionShadowsDropExemption`, `.DropRequiresAuthorizedLogicalDatabase`.
5. **Multi-target position and mixing.** Rewrite decisions are keyed by `WriteRole`; one role for every name would rename every target to the first decision, and an SI target in second position could escape the SI flag. Tests: Task 3 Step 1 `TestRewriteWriteTargets_multiDropRenamesEveryName` and `TestStorageIntegrityContractV2_DropTableSucceeds` rows `DROP TABLE db1.t, other.u` and `DROP TABLE other.u, db1.t`; Task 5 corpus case `si_v2_drop_multi_mixed`; Task 7 Step 1 `StorageIntegrityContractV2.DropTableSucceeds`.

## Plan decisions

- **P1 — Activation by version is full activation.** The spec says a V2 request "activates the catch-all". Both engines have one notion of "the SI surface is active" spread over several sites (catch-all, LIVE VIEW refusal, handler-error sealing, mutation-surface collector failure, namespace refusal, reject annotation, SHOW catch-all). Every site switches to the one predicate, so V2 with an empty map behaves exactly like V2 with a table map in which no statement names a table. With an empty map the table lookups are vacuous; what remains are the conservative "cannot prove" refusals, which is the contract's meaning. This also keeps an ordinary table's behaviour independent of whether some other table is Active.
- **P2 — V1 with an empty map stays legacy,** as measured (Global Constraints). V1 activation remains table-count based.
- **P3 — The unknown-version message changes** from `... V1 is required` to `... V1 or V2 is required` in both engines. A request carrying V1 never reaches it, so V1 behaviour is unchanged; housegate does not match on it.
- **P4 — The exemption is a token grammar.** Go cannot use the AST (Review Focus 1); C++ checks the equivalent `ASTDropQuery` fields (`kind == Drop`, `!is_view`, `!is_dictionary`, `!isTemporary()`, `!if_empty`, `!has_all`, `!has_tables`, `like.empty()`, `cluster.empty()`, `!permanently`, no `out_file` / `format_ast` / `settings_ast`). `NO DELAY` is accepted because ClickHouse parses it into the same `sync` flag as `SYNC`, so C++ cannot tell them apart; the Go output drops `NO DELAY` (polyglot) while C++ prints `SYNC`, so no corpus case pins it.
- **P5 — Multi-target DROP TABLE is accepted under V2 for ordinary targets too,** when the grammar is plain. The spec allows mixing SI and ordinary targets; refusing an all-ordinary list while accepting a mixed one would be arbitrary. V1, no-SI and non-plain multi DROP keep `multi-table DROP/TRUNCATE is not supported`.
- **P6 — Rejected V2 DROP shapes reuse V1 messages.** A non-plain SI DROP gets the signed-lane message it gets today; a physical target keeps the physical-table message.
- **P7 — Authorization applies to DROP as to INSERT:** an SI logical key whose database is not in `database_map` is `InvalidRewriteRequest` with the unauthorized message, before any exemption.
- **P8 — `contract_version` is a mandatory top-level corpus key** with string values `"V1"` / `"V2"` (like `read_mode`), enforced by rule `R8` in both validators. Existing cases get `"contract_version": "V1",` inserted as the first key of each case (a line insertion, not a re-encode: the hand-written tail of the file is not machine formatted, and re-encoding measured a 5.8 KB formatting-only diff). New cases are appended in the tail's hand-written style.
- **P9 — Proto pins use the pseudo-version.** rewriter-grpc's mandatory paired gate asserts `rg_rp_version == *-${rp_sha:0:12}` on rewriter-go's `go.mod` (`.github/scripts/snapshot-query-paired-ci.sh:56-57`), and every repo already pins proto by a `main` pseudo-version. The controller computes `PROTO_PSEUDO` before the `v0.3.0` tag exists (afterwards `go` resolves the commit to the tag), then cuts the tag on the same commit, which keeps the spec's release order (proto tag before engine releases).
- **P10 — rewriter-grpc's paired qualification must move polyglot.** Its manifest pins rewriter-go `4e4a14a` with polyglot `b30c3d28`. Every rewriter-go commit after `1b6f0d3` (rewriter-go v0.12.0) carries polyglot `d5fd24e`, so pinning `RG_SHA` requires a new immutable FFI prerequisite bundle on the build box. This debt predates V2; Task 8 pays it.
- **P11 — The reserved namespace is engine-side (controller ruling).** Both engines learned the protected databases only from table entries, so V2 with an empty map left `hg_*` addressable. `reserved_databases` closes it inside the engines: Go changes only the three `nameresolve` helpers every handler already calls (verified to be the only derivations of the protected set: `grep` for `GetSafeTable()` / `GetUnsafeTable()` outside them finds only the read-rewrite construction), and C++ changes only `isStorageIntegrityPhysicalDatabase` (its 37 call sites cover the same surfaces) and `storageIntegrityPhysicalDatabases`. No handler, and no housegate lexical guard, is involved.
- **P14 — `reserved_databases` is V2-only on both read and validation.** V1 must stay byte-for-byte, so a V1 request with any `reserved_databases` value (even a malformed one) behaves as if the field were absent. Under V2 each entry must be a simple identifier; validation runs before acknowledgement like every other SI field. An empty list is valid (the union is then just the map-derived set).
- **P15 — A reserved-database hit has no logical key.** Go `LookupStorageIntegrityPhysical` returns `("", true)` for a table in a reserved database that no map entry names; every caller discards the key (verified), and the physical-table message is built from the addressed `db.table`, so messages equal V1's.
- **P16 — Corpus coverage of V1-ignores-reserved stays engine-local.** A shared V1 case with `reserved_databases` would pin an unmapped-database SELECT pass-through whose C++ output is not established by any existing shared case; the Go (`TestStorageIntegrityContractV1_IgnoresReservedDatabases`) and C++ (`StorageIntegrityReservedDatabases.V1IgnoresReservedDatabases`) tests instead assert byte-equality with the same request without the field, which is the actual requirement.
- **P12 — C++ is verified by CI only.** The `clickHouse` submodule is empty locally and the build runs over SSH on the private build box (`.github/workflows/ci.yml`, job `remote-build`, 90-minute timeout). The C++ below was written against the ClickHouse 26.3 headers (`ASTDropQuery`, `ASTTableIdentifier::getTableId` / `resetTable`, `ASTQueryWithOnCluster::cluster`, `ASTQueryWithOutput::{out_file,format_ast,settings_ast}`, `ASTQueryWithTableAndOutput::isTemporary`). Each C++ run step lists the exact expected result.
- **P13 — The Go golden now asserts `UNSPECIFIED` acknowledgement for cases without an SI block,** which the C++ golden already did.
- **P12 addendum — C++ task numbering.** The C++ red/green steps of Tasks 6–10 all run in the single draft-PR CI run of Task 10 Step 3.

---

## File Structure

**rewriter-proto** (`github.com/housegate/rewriter-proto`)
- Modify `proto/rewriter.proto` (enum at 279-285, `StorageIntegrityArgs.contract_version` comment at 306-308 and the new field 5 after it, `RewriteTableDynamicArgs.storage_integrity` comment at 262-276, `RewriteSQLResponse.storage_integrity_contract_version` comment at 739-742); regenerate `gen/pb/rewriter.pb.go`.
- Create `gen/pb/storage_integrity_contract_test.go`.

**rewriter-go** (`github.com/housegate/rewriter-go`)
- Modify `go.mod`, `go.sum` (proto pin).
- Create `internal/nameresolve/contract.go`, `internal/nameresolve/contract_test.go`, `internal/nameresolve/reserved_test.go`; modify `internal/nameresolve/resolve.go`.
- Modify `native.go`, `native_test.go`; create `native_v2_test.go`, `native_v2_drop_test.go`, `native_v2_reserved_test.go`.
- Modify `internal/handlers/dblevel.go`, `select.go`, `storage_integrity_policy.go`, `storage_integrity_reject.go`, `writes.go`.
- Create `internal/engine/drop_grammar.go`, `internal/engine/drop_grammar_test.go`; modify `internal/engine/writes.go`, `internal/engine/writes_test.go`.
- Modify `internal/harness/sicorpus_test.go`, `sicorpus_contract_test.go`, `storage_integrity_golden_test.go`, `testdata/storage_integrity_cases.json`, `AGENTS.md`; modify `README.md`.

**rewriter-grpc** (`github.com/housegate/rewriter`)
- Modify submodule `third_party/rewriter-proto`.
- Modify `src/rewriter-server.cc`, `src/handlers/storage_integrity.h`, `storage_integrity.cc` (activation, reserved namespace, validation), `grant.cc`, `select.cc`, `show_tables.cc`, `writes.cc`.
- Modify `tests/rewriter_test.cc`, `tests/si_corpus.h`, `tests/testdata/storage_integrity_cases.json`.
- Modify `tests/testdata/snapshot_query_ci_pins.json`, `.github/scripts/snapshot-query-ci.py`, `.github/scripts/snapshot-query-paired-ci.sh`.
- Modify `CLAUDE.md`, `AGENTS.md`.

**housegate** — no change (plan B).

---

## Task 1: rewriter-proto `STORAGE_INTEGRITY_CONTRACT_V2` and `reserved_databases`

**Files:**
- Modify: `proto/rewriter.proto:262-285`, `:306-309`, `:739-742`
- Regenerate: `gen/pb/rewriter.pb.go`
- Test: create `gen/pb/storage_integrity_contract_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2` (Go, value 2), `rewriter::STORAGE_INTEGRITY_CONTRACT_V2` (C++ after regeneration in rewriter-grpc); field `StorageIntegrityArgs.reserved_databases = 5` (Go `ReservedDatabases []string`, `GetReservedDatabases() []string`; C++ `reserved_databases()`, `add_reserved_databases(const std::string&)`).

- [ ] **Step 1: Branch and write the failing test**

```bash
cd rewriter-proto && git fetch origin && git switch -c feat/si-contract-v2 origin/main && make tools
```

Create `gen/pb/storage_integrity_contract_test.go`:

```go
package pb_test

import (
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestStorageIntegrityContractVersionValues pins the SI contract enum. V2 is
// the dynamic-table-set contract (housegate sub-project 3): DROP TABLE of a
// logical SI table succeeds, a V2 request activates the SI surface even when
// StorageIntegrityArgs.tables is empty, and reserved_databases joins the
// protected physical namespace.
func TestStorageIntegrityContractVersionValues(t *testing.T) {
	enum := pb.StorageIntegrityContractVersion(0).Descriptor()
	want := []protoreflect.Name{
		"STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED",
		"STORAGE_INTEGRITY_CONTRACT_V1",
		"STORAGE_INTEGRITY_CONTRACT_V2",
	}
	if enum.Values().Len() != len(want) {
		t.Fatalf("%s has %d values, want %d", enum.FullName(), enum.Values().Len(), len(want))
	}
	for number, name := range want {
		value := enum.Values().ByNumber(protoreflect.EnumNumber(number))
		if value == nil || value.Name() != name {
			t.Fatalf("%s value %d = %v, want %s", enum.FullName(), number, value, name)
		}
	}
	if pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2 != 2 {
		t.Fatalf("STORAGE_INTEGRITY_CONTRACT_V2 = %d, want 2", pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2)
	}
}

// TestStorageIntegrityArgsReservedDatabases pins the V2 reserved-namespace
// field: repeated string reserved_databases = 5.
func TestStorageIntegrityArgsReservedDatabases(t *testing.T) {
	field := (&pb.StorageIntegrityArgs{}).ProtoReflect().Descriptor().Fields().ByName("reserved_databases")
	if field == nil || field.Number() != 5 || field.Kind() != protoreflect.StringKind || !field.IsList() {
		t.Fatalf("StorageIntegrityArgs.reserved_databases = %v, want repeated string field 5", field)
	}
	args := &pb.StorageIntegrityArgs{ReservedDatabases: []string{"hg_safe", "hg_unsafe", "hg_promote"}}
	if got := args.GetReservedDatabases(); len(got) != 3 || got[2] != "hg_promote" {
		t.Fatalf("GetReservedDatabases() = %v", got)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./gen/pb/ -run 'TestStorageIntegrityContractVersionValues|TestStorageIntegrityArgsReservedDatabases'`
Expected: build failure `undefined: pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2` and `args.GetReservedDatabases undefined (type *pb.StorageIntegrityArgs has no field or method GetReservedDatabases)`.

- [ ] **Step 3: Edit the proto and regenerate**

In `proto/rewriter.proto`, replace:

```proto
    // Storage-integrity read surface (housegate Spec G). When present and
    // `tables` is non-empty, every SELECT-family table reference whose
    // logical key (`<db>.<table>`, db resolved from the SQL qualifier or
    // upstream_logical_database_in_context) is in `tables` is rewritten to a
    // derived table over the safe/unsafe physical tables INSTEAD of the
    // database_map path; EXISTS TABLE maps to safe_table; DESCRIBE becomes a
    // system.columns SELECT that hides reserved_row_id_column; every other
    // statement touching such a table (ALTER/DROP/TRUNCATE/RENAME/EXCHANGE/
    // OPTIMIZE/CREATE/GRANT/REVOKE/SHOW CREATE) rejects with
    // UnsupportedStatement. INSERT is deliberately NOT rejected here (the
    // caller's signed ingress owns that decision); it is rewritten through
    // the ordinary path and reported with AccessedTable.is_storage_integrity.
    // Any user identifier equal to reserved_row_id_column in a statement
    // touching an SI table rejects with RewriteError.
```

with:

```proto
    // Storage-integrity read surface (housegate Spec G). The surface is
    // active when contract_version is V1 and `tables` is non-empty, or when
    // contract_version is V2 (even with an empty `tables` map). While it is
    // active, every SELECT-family table reference whose
    // logical key (`<db>.<table>`, db resolved from the SQL qualifier or
    // upstream_logical_database_in_context) is in `tables` is rewritten to a
    // derived table over the safe/unsafe physical tables INSTEAD of the
    // database_map path; EXISTS TABLE maps to safe_table; DESCRIBE becomes a
    // system.columns SELECT that hides reserved_row_id_column; every other
    // statement touching such a table (ALTER/DROP/TRUNCATE/RENAME/EXCHANGE/
    // OPTIMIZE/CREATE/GRANT/REVOKE/SHOW CREATE) rejects with
    // UnsupportedStatement, except that V2 accepts DROP TABLE (see
    // StorageIntegrityContractVersion). INSERT is deliberately NOT rejected
    // here (the caller's signed ingress owns that decision); it is rewritten
    // through the ordinary path and reported with
    // AccessedTable.is_storage_integrity. Any user identifier equal to
    // reserved_row_id_column in a statement touching an SI table rejects with
    // RewriteError. Statement classes no handler models are refused while the
    // surface is active.
```

Replace:

```proto
enum StorageIntegrityContractVersion {
    STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED = 0;
    STORAGE_INTEGRITY_CONTRACT_V1 = 1;
}
```

with:

```proto
enum StorageIntegrityContractVersion {
    STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED = 0;
    // Static table set (Spec G/I). Active only when
    // StorageIntegrityArgs.tables is non-empty. Every non-INSERT DDL, DML or
    // DCL statement that targets an SI table is rejected, including
    // DROP TABLE.
    STORAGE_INTEGRITY_CONTRACT_V1 = 1;
    // Dynamic table set (housegate sub-project 3). Identical to V1 except:
    //   * the SI surface is active whenever V2 is sent, even with an empty
    //     `tables` map, so session SET, SYSTEM and unmodelled statements stay
    //     refused while no table is active;
    //   * DROP TABLE [IF EXISTS] of logical SI tables (optionally SYNC, and
    //     with several targets that may mix SI and ordinary tables) succeeds.
    //     Each SI target is rewritten to the ordinary physical table that
    //     database_map gives it; the safe/unsafe tables are untouched, and the
    //     target stays in original_accessed_tables with is_storage_integrity.
    //     A physical safe/unsafe target, ON CLUSTER, TRUNCATE, DROP VIEW and
    //     DROP DICTIONARY of an SI table are still rejected;
    //   * StorageIntegrityArgs.reserved_databases joins the protected physical
    //     namespace, which V1 derives from `tables` alone.
    STORAGE_INTEGRITY_CONTRACT_V2 = 2;
}
```

Replace:

```proto
    // Required to be V1 when tables is non-empty. Backends acknowledge an
    // accepted version on every response path; see the top-level enum.
    StorageIntegrityContractVersion contract_version = 4;
}
```

with:

```proto
    // Required to be V1 or V2 when tables is non-empty; V2 also activates the
    // surface with an empty `tables` map. Backends acknowledge the accepted
    // version on every response path; see the top-level enum.
    StorageIntegrityContractVersion contract_version = 4;
    // Protocol-owned physical databases (for example "hg_safe", "hg_unsafe",
    // "hg_promote") that user SQL may never address directly. Read only under
    // contract V2, where the protected namespace is the union of these names
    // and the databases of every safe_table / unsafe_table in `tables`, so it
    // stays protected when `tables` is empty. Each entry must be a non-empty
    // simple identifier. V1 ignores the field and derives the protected set
    // from `tables` alone.
    repeated string reserved_databases = 5;
}
```

Replace:

```proto
    // Echoed as V1 on every response path only after the backend accepted an
    // SI request whose StorageIntegrityArgs.tables is non-empty and whose
    // contract_version is V1. Zero for non-SI or unsupported-version calls.
```

with:

```proto
    // Echoes the accepted StorageIntegrityArgs.contract_version on every
    // response path: V1 after the backend accepted a V1 request whose
    // `tables` is non-empty, V2 after it accepted any V2 request. Zero for
    // non-SI, inactive (V1 with empty `tables`) or unsupported-version calls.
```

Run: `make proto`

- [ ] **Step 4: Run the full gate to verify it passes**

Run: `make verify VERIFY_BREAKING_BASELINE=origin/main`
Expected: `buf lint` clean, `buf breaking` clean (an added enum value and an added field are FILE-compatible), `git diff --exit-code -- gen/` clean after regeneration, `ok github.com/housegate/rewriter-proto/gen/pb`. Verified on the prototype: `gen/pb/rewriter.pb.go` changes by 158 lines, `proto/rewriter.proto` by 57.

- [ ] **Step 5: Commit and open the PR**

```bash
git add proto/rewriter.proto gen/pb/rewriter.pb.go gen/pb/storage_integrity_contract_test.go
git commit -m "feat(proto): add STORAGE_INTEGRITY_CONTRACT_V2 and reserved_databases for the dynamic SI table set" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/si-contract-v2
gh pr create -R housegate/rewriter-proto --fill
```

The controller merges it and runs Task 11 gate R1 before Task 2 starts.

---

## Task 2: rewriter-go — pin the proto and activate the contract by version

**Precondition:** Task 11 gate R1 is done; `PROTO_PSEUDO` is known.

**Files:**
- Modify: `go.mod:7`, `go.sum`
- Create: `internal/nameresolve/contract.go`
- Modify: `native.go:64-77`, `:85-103`, `:122-128`, `:137-152`, `:178`, `:251`
- Modify: `internal/handlers/dblevel.go:333`, `:345`, `:402`; `internal/handlers/select.go:105`; `internal/handlers/storage_integrity_policy.go:16`; `internal/handlers/storage_integrity_reject.go:36`; `internal/handlers/writes.go:140`
- Modify: `native_test.go:893`
- Test: create `internal/nameresolve/contract_test.go`, `native_v2_test.go`

**Interfaces:**
- Consumes: `pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2` (Task 1).
- Produces: `func nameresolve.StorageIntegritySurfaceActive(a *pb.RewriteTableDynamicArgs) bool`, `func nameresolve.StorageIntegrityDropContract(sel nameresolve.Selection) bool`, `const rewriter.StorageIntegrityContractMessage = "storage-integrity contract version V1 or V2 is required"`.

- [ ] **Step 1: Branch, pin the proto and build the FFI library**

```bash
cd rewriter-go && git fetch origin && git switch -c feat/si-contract-v2 origin/main
git submodule update --init third_party/polyglot-src
make ffi            # about 10 minutes on first build
go get github.com/housegate/rewriter-proto@$PROTO_PSEUDO && go mod tidy
grep 'housegate/rewriter-proto' go.mod    # must print exactly: github.com/housegate/rewriter-proto $PROTO_PSEUDO
export POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib   # .so on Linux
SNAPSHOT_QUERY_ORDINARY=1 go test ./...   # baseline: every package ok
```

- [ ] **Step 2: Write the failing tests**

Create `internal/nameresolve/contract_test.go`:

```go
package nameresolve

import (
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)

func TestStorageIntegritySurfaceActive(t *testing.T) {
	table := map[string]*pb.StorageIntegrityArgs_Table{
		"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
	}
	cases := []struct {
		name string
		args *pb.RewriteTableDynamicArgs
		want bool
	}{
		{"nil args", nil, false},
		{"no SI block", &pb.RewriteTableDynamicArgs{}, false},
		{"V1 with tables", &pb.RewriteTableDynamicArgs{StorageIntegrity: &pb.StorageIntegrityArgs{
			Tables: table, ContractVersion: pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1}}, true},
		{"V1 empty map stays inactive", &pb.RewriteTableDynamicArgs{StorageIntegrity: &pb.StorageIntegrityArgs{
			ContractVersion: pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1}}, false},
		{"V2 with tables", &pb.RewriteTableDynamicArgs{StorageIntegrity: &pb.StorageIntegrityArgs{
			Tables: table, ContractVersion: pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2}}, true},
		{"V2 empty map activates", &pb.RewriteTableDynamicArgs{StorageIntegrity: &pb.StorageIntegrityArgs{
			ContractVersion: pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2}}, true},
		{"unspecified with tables is active (rejected later as invalid)", &pb.RewriteTableDynamicArgs{StorageIntegrity: &pb.StorageIntegrityArgs{
			Tables: table}}, true},
		{"unknown version with empty map", &pb.RewriteTableDynamicArgs{StorageIntegrity: &pb.StorageIntegrityArgs{
			ContractVersion: pb.StorageIntegrityContractVersion(99)}}, false},
	}
	for _, c := range cases {
		if got := StorageIntegritySurfaceActive(c.args); got != c.want {
			t.Errorf("%s: StorageIntegritySurfaceActive = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestStorageIntegrityDropContract(t *testing.T) {
	v2 := &pb.RewriteTableDynamicArgs{StorageIntegrity: &pb.StorageIntegrityArgs{
		ContractVersion: pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2}}
	v1 := &pb.RewriteTableDynamicArgs{StorageIntegrity: &pb.StorageIntegrityArgs{
		ContractVersion: pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1}}
	cases := []struct {
		name string
		sel  Selection
		want bool
	}{
		{"dynamic V2", Selection{Mode: ModeDynamic, Dynamic: v2}, true},
		{"dynamic V1", Selection{Mode: ModeDynamic, Dynamic: v1}, false},
		{"static selection shadows V2", Selection{Mode: ModeStatic, Static: &pb.RewriteTableStaticArgs{}}, false},
		{"no selection", Selection{Mode: ModeNone}, false},
	}
	for _, c := range cases {
		if got := StorageIntegrityDropContract(c.sel); got != c.want {
			t.Errorf("%s: StorageIntegrityDropContract = %v, want %v", c.name, got, c.want)
		}
	}
}
```

Create `native_v2_test.go`:

```go
package rewriter

import (
	"context"
	"errors"
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)

// v2Dynamic is the housegate probe shape: db1 and other map to phys, and
// the SI table map is either empty (H6) or carries db1.t.
func v2Dynamic(version pb.StorageIntegrityContractVersion, withTable bool) *pb.RewriteTableDynamicArgs {
	d := &pb.RewriteTableDynamicArgs{
		DatabaseMap:            map[string]string{"db1": "phys", "other": "phys"},
		KnownPhysicalDatabases: []string{"phys"},
		Delim:                  "_",
		StorageIntegrity: &pb.StorageIntegrityArgs{
			Tables:              map[string]*pb.StorageIntegrityArgs_Table{},
			ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
			ReservedRowIdColumn: "_hg_row_id",
			ContractVersion:     version,
		},
	}
	if withTable {
		d.StorageIntegrity.Tables["db1.t"] = &pb.StorageIntegrityArgs_Table{
			SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t",
		}
	}
	return d
}

const (
	siV1 = pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1
	siV2 = pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2
)

func TestStorageIntegrityContractV2_EmptyMapRefusesUnmodelledStatements(t *testing.T) {
	e := newEngine(t)
	opts := []*pb.RewriteOption{tableRewriteDynamic(v2Dynamic(siV2, false))}
	for _, sql := range []string{"SYSTEM RELOAD CONFIG", "SET max_threads = 1"} {
		t.Run(sql, func(t *testing.T) {
			resp, err := doRewrite(e, sql, opts)
			if err != nil {
				t.Fatalf("doRewrite: %v", err)
			}
			if resp.GetCode() != pb.RewriteCode_UnsupportedStatement ||
				resp.GetMessage() != StorageIntegrityUnmodelledMessage ||
				resp.GetSqlAfterRewrite() != sql ||
				resp.GetStatementType() != pb.StatementType_STATEMENT_TYPE_UNSPECIFIED ||
				resp.GetStorageIntegrityContractVersion() != siV2 {
				t.Fatalf("resp = %+v, want acknowledged-V2 UnsupportedStatement %q echoing the SQL",
					resp, StorageIntegrityUnmodelledMessage)
			}
		})
	}
}

func TestStorageIntegrityContractV1_EmptyMapStaysLegacy(t *testing.T) {
	e := newEngine(t)
	opts := []*pb.RewriteOption{tableRewriteDynamic(v2Dynamic(siV1, false))}
	for _, sql := range []string{"SYSTEM RELOAD CONFIG", "SET max_threads = 1"} {
		t.Run(sql, func(t *testing.T) {
			resp, err := doRewrite(e, sql, opts)
			if err != nil {
				t.Fatalf("doRewrite: %v", err)
			}
			if resp.GetCode() != pb.RewriteCode_Success || resp.GetSqlAfterRewrite() != sql ||
				resp.GetStorageIntegrityContractVersion() != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED {
				t.Fatalf("resp = %+v, want legacy Success pass-through without acknowledgement", resp)
			}
		})
	}
}

func TestStorageIntegrityContractV2_AcknowledgedOnEveryPath(t *testing.T) {
	e := newEngine(t)
	cases := []struct {
		name      string
		withTable bool
		sql       string
		wantCode  pb.RewriteCode
		wantSQL   string
	}{
		{"empty map ordinary select", false, "SELECT a FROM db1.t", pb.RewriteCode_Success, `SELECT a FROM phys."db1.t" "db1.t"`},
		{"si table select", true, "SELECT a FROM db1.t", pb.RewriteCode_Success,
			`SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS "db1.t"`},
		{"si table truncate", true, "TRUNCATE TABLE db1.t", pb.RewriteCode_UnsupportedStatement, "TRUNCATE TABLE db1.t"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, err := doRewrite(e, c.sql, []*pb.RewriteOption{tableRewriteDynamic(v2Dynamic(siV2, c.withTable))})
			if err != nil {
				t.Fatalf("doRewrite: %v", err)
			}
			if resp.GetCode() != c.wantCode || resp.GetSqlAfterRewrite() != c.wantSQL ||
				resp.GetStorageIntegrityContractVersion() != siV2 {
				t.Fatalf("resp = %+v, want code=%v sql=%q ack=V2", resp, c.wantCode, c.wantSQL)
			}
		})
	}

	parseFail := nativeWithExactOptions(t, &fakeEngine{parseErr: errors.New("parse boom")},
		[]*pb.RewriteOption{tableRewriteDynamic(v2Dynamic(siV2, false))})
	defer parseFail.Close()
	res, err := parseFail.Rewrite(context.Background(), "SELECT (", "acct")
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != pb.RewriteCode_SyntaxError || res.StorageIntegrityContractVersion != siV2 {
		t.Fatalf("syntax = %+v, want SyntaxError acknowledged V2", res)
	}
}
```

In `native_test.go` (`TestStorageIntegrityContract_RejectsMissingOrUnknownVersionBeforeParse`), replace:

```go
			if !strings.Contains(res.Message, "storage-integrity contract version V1 is required") {
```

with:

```go
			if res.Message != StorageIntegrityContractMessage {
```

- [ ] **Step 3: Run them to verify they fail**

Run: `go test ./internal/nameresolve/ && go test -count=1 -run 'TestStorageIntegrityContract' .`
Expected: `undefined: StorageIntegritySurfaceActive`, `undefined: StorageIntegrityDropContract` (nameresolve) and `undefined: StorageIntegrityContractMessage` (root package). After Step 4's `contract.go` alone, the root tests fail with `resp = sql_after_rewrite:"SYSTEM RELOAD CONFIG" ..., want acknowledged-V2 UnsupportedStatement` and `resp = code:InvalidRewriteRequest message:"storage-integrity contract version V1 is required" ...`; `TestStorageIntegrityContractV1_EmptyMapStaysLegacy` already passes (it records today's behaviour).

- [ ] **Step 4: Implement**

Create `internal/nameresolve/contract.go`:

```go
package nameresolve

import "github.com/housegate/rewriter-proto/gen/pb"

// StorageIntegritySurfaceActive reports whether a request's dynamic args
// activate the storage-integrity surface. V2 activates it by version, even
// with an empty table map (housegate sub-project 3, H6); V1 and every other
// version keep the original table-count rule, so a V1 request with an empty
// map stays legacy byte-for-byte. doRewrite rejects a non-empty map whose
// version is neither V1 nor V2 before any handler runs.
func StorageIntegritySurfaceActive(a *pb.RewriteTableDynamicArgs) bool {
	si := a.GetStorageIntegrity()
	if si.GetContractVersion() == pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2 {
		return true
	}
	return len(si.GetTables()) > 0
}

// StorageIntegrityDropContract reports whether the effective selection
// carries the V2 contract, whose only write-policy difference from V1 is that
// DROP TABLE of a logical storage-integrity table is accepted.
func StorageIntegrityDropContract(sel Selection) bool {
	return sel.Mode == ModeDynamic &&
		sel.Dynamic.GetStorageIntegrity().GetContractVersion() == pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2
}
```

In `native.go`, make these replacements.

In `finalize`, replace:

```go
	if siVersion == pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1 {
		handlers.AnnotateStorageIntegrityRejectAST(e, resp, ast, sql, sel)
	}
```

with:

```go
	if siVersion != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED {
		handlers.AnnotateStorageIntegrityRejectAST(e, resp, ast, sql, sel)
	}
```

In `sealStorageIntegrityHandlerError`, replace:

```go
	if siVersion != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1 {
		return nil, handlerErr
	}
```

with:

```go
	if siVersion == pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED {
		return nil, handlerErr
	}
```

Replace:

```go
// StorageIntegrityUnmodelledMessage is returned when a request carries a
// non-empty storage_integrity.tables map and execution reaches the
// unmodelled-statement pass-through. The rewriter cannot prove such a
// statement is harmless to the protocol-owned namespaces, so it refuses to
// forward it (Spec I D1). Enumerated classes replace this text with a more
// specific one; see handlers.AnnotateStorageIntegrityReject.
const StorageIntegrityUnmodelledMessage = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"
```

with:

```go
// StorageIntegrityUnmodelledMessage is returned when a request activates the
// storage-integrity surface (a V1 request with a non-empty tables map, or any
// V2 request) and execution reaches the unmodelled-statement pass-through.
// The rewriter cannot prove such a statement is harmless to the
// protocol-owned namespaces, so it refuses to forward it (Spec I D1).
// Enumerated classes replace this text with a more specific one; see
// handlers.AnnotateStorageIntegrityReject.
const StorageIntegrityUnmodelledMessage = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"

// StorageIntegrityContractMessage rejects an active storage-integrity request
// whose contract_version this engine does not implement.
const StorageIntegrityContractMessage = "storage-integrity contract version V1 or V2 is required"
```

In `doRewrite`, replace:

```go
	if selection.Mode == nameresolve.ModeDynamic && len(selection.Dynamic.GetStorageIntegrity().GetTables()) > 0 {
		if selection.Dynamic.GetStorageIntegrity().GetContractVersion() != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1 {
			resp.Code = pb.RewriteCode_InvalidRewriteRequest
			resp.Message = "storage-integrity contract version V1 is required"
			return resp, nil
		}
		if err := nameresolve.ValidateStorageIntegrity(selection.Dynamic); err != nil {
			resp.Code = pb.RewriteCode_InvalidRewriteRequest
			resp.Message = err.Error()
			return resp, nil
		}
		siVersion = pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1
		resp.StorageIntegrityContractVersion = siVersion
	}
```

with:

```go
	if selection.Mode == nameresolve.ModeDynamic && nameresolve.StorageIntegritySurfaceActive(selection.Dynamic) {
		version := selection.Dynamic.GetStorageIntegrity().GetContractVersion()
		if version != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1 &&
			version != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2 {
			resp.Code = pb.RewriteCode_InvalidRewriteRequest
			resp.Message = StorageIntegrityContractMessage
			return resp, nil
		}
		if err := nameresolve.ValidateStorageIntegrity(selection.Dynamic); err != nil {
			resp.Code = pb.RewriteCode_InvalidRewriteRequest
			resp.Message = err.Error()
			return resp, nil
		}
		siVersion = version
		resp.StorageIntegrityContractVersion = siVersion
	}
```

Then replace the two remaining activation checks in `doRewrite` (before `engine.ClassifyLiveView` and before the pass-through tail), each exactly:

```go
	if siVersion == pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1 {
```

with:

```go
	if siVersion != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED {
```

Afterwards `grep -n CONTRACT_V1 native.go` prints only the version check inside the `doRewrite` activation block.

Switch every handler activation site to the predicate:

```bash
sed -i.bak \
 -e 's/if len(dyn.GetStorageIntegrity().GetTables()) > 0 {/if nameresolve.StorageIntegritySurfaceActive(dyn) {/' \
 -e 's/if len(dyn.GetStorageIntegrity().GetTables()) == 0 {/if !nameresolve.StorageIntegritySurfaceActive(dyn) {/' \
 -e 's/storageIntegrityActive := sel.Mode == nameresolve.ModeDynamic \&\& len(sel.Dynamic.GetStorageIntegrity().GetTables()) > 0/storageIntegrityActive := sel.Mode == nameresolve.ModeDynamic \&\& nameresolve.StorageIntegritySurfaceActive(sel.Dynamic)/' \
 -e 's/if sel.Mode != nameresolve.ModeDynamic || len(sel.Dynamic.GetStorageIntegrity().GetTables()) == 0 {/if sel.Mode != nameresolve.ModeDynamic || !nameresolve.StorageIntegritySurfaceActive(sel.Dynamic) {/' \
 -e 's/	if len(sel.Dynamic.GetStorageIntegrity().GetTables()) > 0 {/	if nameresolve.StorageIntegritySurfaceActive(sel.Dynamic) {/' \
 internal/handlers/dblevel.go internal/handlers/select.go internal/handlers/storage_integrity_reject.go \
 internal/handlers/storage_integrity_policy.go internal/handlers/writes.go
rm internal/handlers/*.go.bak
grep -n 'StorageIntegritySurfaceActive' internal/handlers/*.go
```

Expected grep output (seven sites): `dblevel.go:333`, `dblevel.go:345`, `dblevel.go:402`, `select.go:105`, `storage_integrity_reject.go:36`, `storage_integrity_policy.go:16`, `writes.go:140`. `grep -rn 'GetStorageIntegrity().GetTables()) [>=]' internal/handlers` must print nothing.

- [ ] **Step 5: Run to verify it passes**

Run: `go vet ./... && SNAPSHOT_QUERY_ORDINARY=1 go test -count=1 ./...`
Expected: every package `ok` (root, `cmd/rewrite`, `internal/corpus`, `engine`, `handlers`, `harness`, `nameresolve`, `reverse`); all 237 existing shared-corpus cases still pass unchanged.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/nameresolve/contract.go internal/nameresolve/contract_test.go native.go native_test.go native_v2_test.go internal/handlers/dblevel.go internal/handlers/select.go internal/handlers/storage_integrity_policy.go internal/handlers/storage_integrity_reject.go internal/handlers/writes.go
git commit -m "feat(storage-integrity): accept contract V2 and activate the SI surface by version" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 3: rewriter-go — V2 DROP TABLE of SI tables

**Files:**
- Create: `internal/engine/drop_grammar.go`
- Modify: `internal/engine/writes.go:26-31` (add `dropRole`), `:86-95` (`writeSlots` NodeDropTable)
- Modify: `internal/handlers/writes.go:19-35` (`RewriteWrite`), `:66-125` (`preflightStorageIntegrityWrite`), `:232-245` (`decideWriteTarget`), `:348-375` (`dispatchDropLike`)
- Modify: `internal/engine/writes_test.go:139-155`
- Test: create `internal/engine/drop_grammar_test.go`, `native_v2_drop_test.go`

**Interfaces:**
- Consumes: `nameresolve.StorageIntegrityDropContract`, `v2Dynamic`, `siV1`, `siV2` (Task 2).
- Produces: `func engine.PlainDropTable(e engine.Engine, sql string) (bool, error)`; DROP TABLE slots with roles `drop`, `drop#1`, `drop#2`, …; `func handlers.storageIntegrityDropAllowed(e engine.Engine, sql string, info engine.WriteInfo, sel nameresolve.Selection) (bool, error)`.

- [ ] **Step 1: Write the failing tests**

Create `internal/engine/drop_grammar_test.go`:

```go
package engine

import "testing"

func TestPlainDropTable(t *testing.T) {
	e := newTestEngine(t)
	cases := []struct {
		sql  string
		want bool
	}{
		{"DROP TABLE db1.t", true},
		{"drop table if exists `db1`.\"t\", other.u sync;", true},
		{"DROP TABLE IF EXISTS db1.t SYNC", true},
		{"DROP TABLE db1.t NO DELAY", true},
		{"DROP /* c */ TABLE db1.t -- trailing", true},
		{"DROP TABLE t", true},
		{"DROP TABLE sync", true},
		{"DROP TABLE db1.t, other.u", true},
		{"DROP TABLE db1.t ON CLUSTER c", false},
		{"DROP TABLE db1.t ON CLUSTER 'c' SYNC", false},
		{"DROP TEMPORARY TABLE t", false},
		{"DROP TABLE IF EMPTY db1.t", false},
		{"DROP TABLE db1.t FORMAT JSON", false},
		{"DROP TABLE db1.t SETTINGS a = 1", false},
		{"DROP TABLE {tbl:Identifier}", false},
		{"DROP TABLE a.b.c", false},
		{"DROP TABLE db1.t,", false},
		{"DROP TABLE db1.t `sync`", false},
		{"DROP VIEW db1.t", false},
		{"DROP DICTIONARY db1.t", false},
		{"TRUNCATE TABLE db1.t", false},
		{"DROP TABLE db1.t; DROP TABLE other.u", false},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			got, err := PlainDropTable(e, c.sql)
			if err != nil {
				t.Fatalf("PlainDropTable: %v", err)
			}
			if got != c.want {
				t.Fatalf("PlainDropTable(%q) = %v, want %v", c.sql, got, c.want)
			}
		})
	}
}

func TestRewriteWriteTargets_multiDropRenamesEveryName(t *testing.T) {
	e := newTestEngine(t)
	ast, err := e.ParseOne("DROP TABLE IF EXISTS db1.t, other.u SYNC")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := InspectWrite(ast)
	if err != nil {
		t.Fatalf("InspectWrite: %v", err)
	}
	if len(info.Slots) != 2 || info.Slots[0].Role != RoleDrop || info.Slots[1].Role != "drop#1" {
		t.Fatalf("Slots = %+v, want roles drop, drop#1", info.Slots)
	}
	out, err := RewriteWriteTargets(ast, func(s WriteSlot) TableDecision {
		return TableDecision{Action: ActionRename, NewDB: "phys", NewTable: s.Target.DB + "." + s.Target.Table}
	})
	if err != nil {
		t.Fatalf("RewriteWriteTargets: %v", err)
	}
	got, err := e.Generate(out)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if want := `DROP TABLE IF EXISTS phys."db1.t", phys."other.u" SYNC`; got != want {
		t.Fatalf("generated %q, want %q", got, want)
	}
}
```

In `internal/engine/writes_test.go` (`TestInspectWrite_dropMultiTable`), replace:

```go
	// writeSlots visits only names[0] — the handler rejects multi-table DROP before
	// rewriting, but InspectWrite must still expose exactly one coherent slot.
	if len(info.Slots) != 1 {
		t.Errorf("len(Slots) = %d, want 1 (first name only)", len(info.Slots))
	}
```

with:

```go
	// writeSlots visits every name, one role per position. Only the V2
	// storage-integrity DROP path rewrites a multi-table DROP; every other
	// path still rejects it before rewriting.
	if len(info.Slots) != 2 || info.Slots[0].Role != RoleDrop || info.Slots[1].Role != "drop#1" {
		t.Errorf("Slots = %+v, want roles drop, drop#1", info.Slots)
	}
```

Create `native_v2_drop_test.go`:

```go
package rewriter

import (
	"reflect"
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)

type wantAccess struct {
	db, table, logical, physical string
	si                           bool
}

func checkAccess(t *testing.T, got []*pb.AccessedTable, want []wantAccess) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("accessed = %v, want %d entries", got, len(want))
	}
	for i, w := range want {
		g := got[i]
		if g.GetOriginalDatabase() != w.db || g.GetOriginalTable() != w.table ||
			g.GetLogicalDatabase() != w.logical || g.GetPhysicalDatabase() != w.physical ||
			g.GetIsStorageIntegrity() != w.si || g.GetIsRemote() {
			t.Fatalf("accessed[%d] = %v, want %+v", i, g, w)
		}
	}
}

func TestStorageIntegrityContractV2_DropTableSucceeds(t *testing.T) {
	e := newEngine(t)
	opts := []*pb.RewriteOption{tableRewriteDynamic(v2Dynamic(siV2, true))}
	siT := wantAccess{"db1", "t", "db1", "phys", true}
	otherU := wantAccess{"other", "u", "other", "phys", false}
	cases := []struct {
		sql      string
		wantSQL  string
		rewrites map[string]string
		accessed []wantAccess
		ec       pb.ExistenceClause
	}{
		{"DROP TABLE db1.t", `DROP TABLE phys."db1.t"`,
			map[string]string{"db1.t": "phys.db1.t"}, []wantAccess{siT}, pb.ExistenceClause_EXISTENCE_CLAUSE_UNSPECIFIED},
		{"DROP TABLE IF EXISTS db1.t SYNC", `DROP TABLE IF EXISTS phys."db1.t" SYNC`,
			map[string]string{"db1.t": "phys.db1.t"}, []wantAccess{siT}, pb.ExistenceClause_EXISTENCE_CLAUSE_IF_EXISTS},
		{"DROP TABLE db1.t, other.u", `DROP TABLE phys."db1.t", phys."other.u"`,
			map[string]string{"db1.t": "phys.db1.t", "other.u": "phys.other.u"}, []wantAccess{siT, otherU}, pb.ExistenceClause_EXISTENCE_CLAUSE_UNSPECIFIED},
		{"DROP TABLE other.u, db1.t", `DROP TABLE phys."other.u", phys."db1.t"`,
			map[string]string{"db1.t": "phys.db1.t", "other.u": "phys.other.u"}, []wantAccess{otherU, siT}, pb.ExistenceClause_EXISTENCE_CLAUSE_UNSPECIFIED},
		{"DROP TABLE other.u, other.v", `DROP TABLE phys."other.u", phys."other.v"`,
			map[string]string{"other.u": "phys.other.u", "other.v": "phys.other.v"},
			[]wantAccess{otherU, {"other", "v", "other", "phys", false}}, pb.ExistenceClause_EXISTENCE_CLAUSE_UNSPECIFIED},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			resp, err := doRewrite(e, c.sql, opts)
			if err != nil {
				t.Fatalf("doRewrite: %v", err)
			}
			if resp.GetCode() != pb.RewriteCode_Success || resp.GetSqlAfterRewrite() != c.wantSQL ||
				resp.GetStatementType() != pb.StatementType_STATEMENT_TYPE_DROP_TABLE ||
				resp.GetStorageIntegrityContractVersion() != siV2 || resp.GetExistenceClause() != c.ec {
				t.Fatalf("resp = %+v, want Success DROP_TABLE %q ack V2 existence %v", resp, c.wantSQL, c.ec)
			}
			if !reflect.DeepEqual(resp.GetTableRewrites(), c.rewrites) {
				t.Fatalf("table_rewrites = %v, want %v", resp.GetTableRewrites(), c.rewrites)
			}
			checkAccess(t, resp.GetOriginalAccessedTables(), c.accessed)
		})
	}
}

func TestStorageIntegrityContractV2_DropRejections(t *testing.T) {
	e := newEngine(t)
	signedLane := "storage-integrity table db1.t accepts writes only through the signed statement lane"
	cases := []struct {
		name     string
		version  pb.StorageIntegrityContractVersion
		sql      string
		wantCode pb.RewriteCode
		wantMsg  string
	}{
		{"v2 truncate", siV2, "TRUNCATE TABLE db1.t", pb.RewriteCode_UnsupportedStatement, signedLane},
		{"v2 physical target", siV2, "DROP TABLE hg_safe.db1__t", pb.RewriteCode_UnsupportedStatement,
			"storage-integrity physical table hg_safe.db1__t is not directly addressable"},
		{"v2 on cluster", siV2, "DROP TABLE db1.t ON CLUSTER c", pb.RewriteCode_UnsupportedStatement, signedLane},
		{"v2 multi on cluster", siV2, "DROP TABLE other.u, db1.t ON CLUSTER c", pb.RewriteCode_UnsupportedStatement, signedLane},
		{"v2 ordinary multi on cluster", siV2, "DROP TABLE other.u, other.v ON CLUSTER c", pb.RewriteCode_UnsupportedStatement,
			"multi-table DROP/TRUNCATE is not supported"},
		{"v2 if empty", siV2, "DROP TABLE IF EMPTY db1.t", pb.RewriteCode_UnsupportedStatement, signedLane},
		{"v2 drop view", siV2, "DROP VIEW db1.t", pb.RewriteCode_UnsupportedStatement, signedLane},
		{"v2 drop dictionary", siV2, "DROP DICTIONARY db1.t", pb.RewriteCode_UnsupportedStatement, signedLane},
		{"v1 drop", siV1, "DROP TABLE db1.t", pb.RewriteCode_UnsupportedStatement, signedLane},
		{"v1 drop if exists sync", siV1, "DROP TABLE IF EXISTS db1.t SYNC", pb.RewriteCode_UnsupportedStatement, signedLane},
		{"v1 multi", siV1, "DROP TABLE db1.t, other.u", pb.RewriteCode_UnsupportedStatement, signedLane},
		{"v1 ordinary multi", siV1, "DROP TABLE other.u, other.v", pb.RewriteCode_UnsupportedStatement,
			"multi-table DROP/TRUNCATE is not supported"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, err := doRewrite(e, c.sql, []*pb.RewriteOption{tableRewriteDynamic(v2Dynamic(c.version, true))})
			if err != nil {
				t.Fatalf("doRewrite: %v", err)
			}
			if resp.GetCode() != c.wantCode || resp.GetMessage() != c.wantMsg || resp.GetSqlAfterRewrite() != c.sql ||
				resp.GetStatementType() != pb.StatementType_STATEMENT_TYPE_UNSPECIFIED ||
				resp.GetStorageIntegrityContractVersion() != c.version {
				t.Fatalf("resp = %+v, want %v %q echoing the SQL, ack %v", resp, c.wantCode, c.wantMsg, c.version)
			}
		})
	}
}

func TestStorageIntegrityContractV2_DropRequiresAuthorizedLogicalDatabase(t *testing.T) {
	e := newEngine(t)
	dyn := v2Dynamic(siV2, true)
	delete(dyn.DatabaseMap, "db1")
	resp, err := doRewrite(e, "DROP TABLE db1.t", []*pb.RewriteOption{tableRewriteDynamic(dyn)})
	if err != nil {
		t.Fatalf("doRewrite: %v", err)
	}
	if resp.GetCode() != pb.RewriteCode_InvalidRewriteRequest ||
		resp.GetMessage() != "storage-integrity logical database db1 is not authorized by database_map" {
		t.Fatalf("resp = %+v, want the unauthorized-logical rejection", resp)
	}
}

func TestStorageIntegrityContractV2_StaticSelectionShadowsDropExemption(t *testing.T) {
	e := newEngine(t)
	opts := []*pb.RewriteOption{tableRewriteDynamic(v2Dynamic(siV2, true)), tableRewriteStatic()}
	resp, err := doRewrite(e, "DROP TABLE db1.t, other.u", opts)
	if err != nil {
		t.Fatalf("doRewrite: %v", err)
	}
	if resp.GetCode() != pb.RewriteCode_UnsupportedStatement ||
		resp.GetMessage() != "multi-table DROP/TRUNCATE is not supported" ||
		resp.GetStorageIntegrityContractVersion() != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED {
		t.Fatalf("resp = %+v, want the legacy multi-table rejection without acknowledgement", resp)
	}
}

func TestStorageIntegrityContractV2_DropResolvesUnqualifiedTargetThroughContext(t *testing.T) {
	e := newEngine(t)
	dyn := v2Dynamic(siV2, true)
	dyn.UpstreamLogicalDatabaseInContext = "db1"
	opts := []*pb.RewriteOption{tableRewriteDynamic(dyn)}

	resp, err := doRewrite(e, "DROP TABLE t", opts)
	if err != nil {
		t.Fatalf("doRewrite: %v", err)
	}
	if resp.GetCode() != pb.RewriteCode_Success || resp.GetSqlAfterRewrite() != `DROP TABLE phys."db1.t"` {
		t.Fatalf("resp = %+v, want the unqualified SI target dropped as phys.\"db1.t\"", resp)
	}
	checkAccess(t, resp.GetOriginalAccessedTables(), []wantAccess{{"", "t", "db1", "phys", true}})

	// Polyglot parses DROP TEMPORARY TABLE as an ordinary drop_table node; the
	// token grammar keeps it on the V1 rejection instead of dropping the
	// session's logical SI table.
	resp, err = doRewrite(e, "DROP TEMPORARY TABLE t", opts)
	if err != nil {
		t.Fatalf("doRewrite: %v", err)
	}
	if resp.GetCode() != pb.RewriteCode_UnsupportedStatement ||
		resp.GetMessage() != "storage-integrity table db1.t accepts writes only through the signed statement lane" {
		t.Fatalf("resp = %+v, want DROP TEMPORARY TABLE refused", resp)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -count=1 ./internal/engine/ && go test -count=1 -run 'TestStorageIntegrityContractV2_Drop|TestStorageIntegrityContractV2_Static' .`
Expected: engine build failure `undefined: PlainDropTable`. With `drop_grammar.go` in place but no handler change, the root tests fail: every `DropTableSucceeds` row answers `code:UnsupportedStatement message:"storage-integrity table db1.t accepts writes only through the signed statement lane"`, the `DROP TABLE other.u, other.v` row answers `multi-table DROP/TRUNCATE is not supported`, and `DropResolvesUnqualifiedTargetThroughContext` fails on `DROP TABLE t`; `DropRejections`, `DropRequiresAuthorizedLogicalDatabase` and `StaticSelectionShadowsDropExemption` already pass.

- [ ] **Step 3: Implement**

Create `internal/engine/drop_grammar.go`:

```go
package engine

// PlainDropTable reports whether sql is exactly
//
//	DROP TABLE [IF EXISTS] name [, name ...] [SYNC | NO DELAY] [;]
//
// where each name is `table` or `database.table` (plain or quoted). It is
// the grammar the V2 storage-integrity contract accepts for DROP TABLE of a
// logical SI table. Polyglot's drop_table node silently discards ON CLUSTER,
// TEMPORARY, IF EMPTY, FORMAT and SETTINGS, so the AST alone cannot prove a
// drop is plain; the token stream can. Comments are not tokens.
func PlainDropTable(e Engine, sql string) (bool, error) {
	toks, err := tokenizeRaw(e, sql)
	if err != nil {
		return false, err
	}
	if n := len(toks); n > 0 && toks[n-1].TokenType == "SEMICOLON" {
		toks = toks[:n-1]
	}
	if len(toks) < 3 || !tokenTextIs(toks[0], "DROP") || !tokenTextIs(toks[1], "TABLE") ||
		toks[1].TokenType != "TABLE" {
		return false, nil
	}
	i := 2
	if i+1 < len(toks) && toks[i].TokenType == "IF" && tokenTextIs(toks[i+1], "EXISTS") {
		i += 2
	}
	for {
		next, ok := consumeMutationQualifiedName(toks, i)
		if !ok {
			return false, nil
		}
		i = next
		if i < len(toks) && toks[i].TokenType == "COMMA" {
			i++
			continue
		}
		break
	}
	switch {
	case i == len(toks):
		return true, nil
	case i+1 == len(toks) && toks[i].TokenType == "VAR" && tokenTextIs(toks[i], "SYNC"):
		return true, nil
	case i+2 == len(toks) && tokenTextIs(toks[i], "NO") && toks[i+1].TokenType == "VAR" && tokenTextIs(toks[i+1], "DELAY"):
		return true, nil
	default:
		return false, nil
	}
}
```

In `internal/engine/writes.go`, replace:

```go
// WriteSlot is one rewriteable table reference inside a write statement.
```

with:

```go
// dropRole names the i-th target of a DROP TABLE. The first keeps RoleDrop so
// single-target callers are unchanged; later targets get "drop#<i>" because
// RewriteWriteTargets keys its decisions by role.
func dropRole(i int) WriteRole {
	if i == 0 {
		return RoleDrop
	}
	return WriteRole(fmt.Sprintf("%s#%d", RoleDrop, i))
}

// WriteSlot is one rewriteable table reference inside a write statement.
```

and in `writeSlots` replace:

```go
	case NodeDropTable:
		if names, ok := body["names"].([]any); ok && len(names) > 0 {
			if tbl, ok := tblOf(names[0]); ok {
				visit(RoleDrop, tbl)
			}
		}
```

with:

```go
	case NodeDropTable:
		// One slot per name, in document order. A multi-table DROP is rewritten
		// only on the V2 storage-integrity path; every other path rejects it
		// before rewriting.
		if names, ok := body["names"].([]any); ok {
			for i, name := range names {
				if tbl, ok := tblOf(name); ok {
					visit(dropRole(i), tbl)
				}
			}
		}
```

In `internal/handlers/writes.go`, replace in `RewriteWrite`:

```go
	sel := nameresolve.FindActive(opts)
	if resp, rejected, err := preflightStorageIntegrityWrite(e, ast, sql, info, sel); err != nil {
```

with:

```go
	sel := nameresolve.FindActive(opts)
	siDrop, err := storageIntegrityDropAllowed(e, sql, info, sel)
	if err != nil {
		return nil, false, err
	}
	if resp, rejected, err := preflightStorageIntegrityWrite(e, ast, sql, info, sel, siDrop); err != nil {
```

and:

```go
		return dispatchDropLike(e, ast, sql, info, sel)
```

with:

```go
		return dispatchDropLike(e, ast, sql, info, sel, siDrop)
```

Replace the head of `preflightStorageIntegrityWrite`:

```go
// preflightStorageIntegrityWrite runs before statement-specific generic guards
// (multi-DROP, cross-table ALTER, AS table-function, bare rejects). Otherwise
// those guards can return a non-Success response without the SI access marker
// Housegate needs to keep fail-closed semantics.
func preflightStorageIntegrityWrite(e engine.Engine, ast engine.AST, sql string, info engine.WriteInfo, sel nameresolve.Selection) (*pb.RewriteSQLResponse, bool, error) {
```

with:

```go
// storageIntegrityDropAllowed reports whether this statement takes the V2
// DROP TABLE exemption: the effective selection carries contract V2 and the
// statement is exactly `DROP TABLE [IF EXISTS] name[, name...] [SYNC]`
// (engine.PlainDropTable). ON CLUSTER, TEMPORARY, IF EMPTY, FORMAT and
// SETTINGS keep the V1 rejection; DROP VIEW, DROP DICTIONARY and TRUNCATE are
// different node kinds and never qualify.
func storageIntegrityDropAllowed(e engine.Engine, sql string, info engine.WriteInfo, sel nameresolve.Selection) (bool, error) {
	if info.Kind != engine.NodeDropTable || !nameresolve.StorageIntegrityDropContract(sel) {
		return false, nil
	}
	return engine.PlainDropTable(e, sql)
}

// preflightStorageIntegrityWrite runs before statement-specific generic guards
// (multi-DROP, cross-table ALTER, AS table-function, bare rejects). Otherwise
// those guards can return a non-Success response without the SI access marker
// Housegate needs to keep fail-closed semantics. siDrop admits authorized
// logical SI targets of a V2 plain DROP TABLE, exactly as INSERT targets are
// admitted for the signed ingress lane.
func preflightStorageIntegrityWrite(e engine.Engine, ast engine.AST, sql string, info engine.WriteInfo, sel nameresolve.Selection, siDrop bool) (*pb.RewriteSQLResponse, bool, error) {
```

In the same function replace:

```go
	inspectTarget := func(tt engine.TableTarget, allowInsertTarget bool) (*pb.RewriteSQLResponse, bool) {
```

with:

```go
	inspectTarget := func(tt engine.TableTarget, allowLogicalTarget bool) (*pb.RewriteSQLResponse, bool) {
```

replace:

```go
			if allowInsertTarget {
				return nil, false // authorized signed ingress owns logical SI INSERT acceptance
			}
```

with:

```go
			if allowLogicalTarget {
				// Authorized signed ingress owns logical SI INSERT acceptance; a V2
				// plain DROP TABLE drops only the ordinary physical table.
				return nil, false
			}
```

and replace:

```go
		if resp, rejected := inspectTarget(tt, info.Kind == engine.NodeInsert); rejected {
```

with:

```go
		if resp, rejected := inspectTarget(tt, info.Kind == engine.NodeInsert || siDrop); rejected {
```

In `decideWriteTarget`, replace:

```go
	// Spec G §4.4: every non-INSERT slot resolving to a storage-integrity
	// table is refused (INSERT stays on the ordinary path — the caller's
	// signed ingress owns that decision, see plan deviation D-1).
	if sel.Mode == nameresolve.ModeDynamic && kind != engine.NodeInsert {
```

with:

```go
	// Spec G §4.4: every non-INSERT slot resolving to a storage-integrity
	// table is refused (INSERT stays on the ordinary path — the caller's
	// signed ingress owns that decision, see plan deviation D-1). Under
	// contract V2 a DROP TABLE slot also takes the ordinary path: preflight
	// has already refused every SI target of a DROP TABLE that is not plain.
	if sel.Mode == nameresolve.ModeDynamic && kind != engine.NodeInsert &&
		!(kind == engine.NodeDropTable && nameresolve.StorageIntegrityDropContract(sel)) {
```

In `dispatchDropLike`, replace the signature:

```go
func dispatchDropLike(e engine.Engine, ast engine.AST, sql string, info engine.WriteInfo, sel nameresolve.Selection) (*pb.RewriteSQLResponse, bool, error) {
```

with:

```go
func dispatchDropLike(e engine.Engine, ast engine.AST, sql string, info engine.WriteInfo, sel nameresolve.Selection, siDrop bool) (*pb.RewriteSQLResponse, bool, error) {
```

and replace:

```go
	if info.Multi {
		rejectUnsupported(resp, "multi-table DROP/TRUNCATE is not supported")
```

with:

```go
	// Contract V2 accepts a plain multi-table DROP TABLE, whose targets may mix
	// storage-integrity and ordinary tables; everything else stays rejected.
	if info.Multi && !siDrop {
		rejectUnsupported(resp, "multi-table DROP/TRUNCATE is not supported")
```

- [ ] **Step 4: Run to verify it passes**

Run: `gofmt -l internal native*.go; go vet ./... && SNAPSHOT_QUERY_ORDINARY=1 go test -count=1 ./... && go run ./cmd/fidelity-spike`
Expected: `gofmt -l` prints only the pre-existing `internal/handlers/dblevel.go` (a comment normalisation that exists on `origin/main`); every package `ok`; fidelity spike `OK 12 / total 12`.

- [ ] **Step 5: Commit**

```bash
git add internal/engine/drop_grammar.go internal/engine/drop_grammar_test.go internal/engine/writes.go internal/engine/writes_test.go internal/handlers/writes.go native_v2_drop_test.go
git commit -m "feat(storage-integrity): accept plain DROP TABLE of SI tables under contract V2" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 4: rewriter-go — reserved databases join the protected namespace under V2

**Files:**
- Modify: `internal/nameresolve/contract.go` (append `storageIntegrityReservedDatabase`), `internal/nameresolve/resolve.go:295-322` (`LookupStorageIntegrityPhysical`), `:324-345` (`IsStorageIntegrityPhysicalDatabase`), `:367-389` (`StorageIntegrityPhysicalDatabases`), `:454-495` (`ValidateStorageIntegrity`)
- Test: create `internal/nameresolve/reserved_test.go`, `native_v2_reserved_test.go`

**Interfaces:**
- Consumes: `pb.StorageIntegrityArgs.GetReservedDatabases()` (Task 1); `v2Dynamic`, `siV1`, `siV2` (Task 2); `tableRewriteDynamic` (existing `native_test.go`).
- Produces: `func nameresolve.storageIntegrityReservedDatabase(db string, a *pb.RewriteTableDynamicArgs) bool` (package-private); `LookupStorageIntegrityPhysical`, `IsStorageIntegrityPhysicalDatabase` and `StorageIntegrityPhysicalDatabases` return the V2 union; `ValidateStorageIntegrity` rejects a malformed V2 entry with `storage-integrity reserved_databases entry %q must be a simple identifier`.

- [ ] **Step 1: Write the failing tests**

Create `internal/nameresolve/reserved_test.go`:

```go
package nameresolve

import (
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)

func reservedArgs(version pb.StorageIntegrityContractVersion, withTable bool) *pb.RewriteTableDynamicArgs {
	si := &pb.StorageIntegrityArgs{
		ContractVersion:   version,
		ReservedDatabases: []string{"hg_safe", "hg_unsafe", "hg_promote"},
	}
	if withTable {
		si.Tables = map[string]*pb.StorageIntegrityArgs_Table{
			"db1.t": {SafeTable: "safe_x.db1__t", UnsafeTable: "unsafe_x.db1__t"},
		}
	}
	return &pb.RewriteTableDynamicArgs{StorageIntegrity: si}
}

func TestReservedDatabasesProtectedUnderV2(t *testing.T) {
	v2 := reservedArgs(pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2, false)
	for _, db := range []string{"hg_safe", "hg_unsafe", "hg_promote"} {
		if !IsStorageIntegrityPhysicalDatabase(db, v2) {
			t.Errorf("IsStorageIntegrityPhysicalDatabase(%q) = false under V2 with an empty map", db)
		}
		if _, ok := LookupStorageIntegrityPhysical(db, "x", v2); !ok {
			t.Errorf("LookupStorageIntegrityPhysical(%q, x) = false under V2 with an empty map", db)
		}
	}
	if IsStorageIntegrityPhysicalDatabase("phys", v2) {
		t.Error("an unreserved database must stay ordinary")
	}
	withContext := reservedArgs(pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2, false)
	withContext.UpstreamLogicalDatabaseInContext = "hg_unsafe"
	if _, ok := LookupStorageIntegrityPhysical("", "x", withContext); !ok {
		t.Error("an unqualified table in a reserved session database must be protected")
	}
	if got, want := StorageIntegrityPhysicalDatabases(v2), []string{"hg_promote", "hg_safe", "hg_unsafe"}; !reflect.DeepEqual(got, want) {
		t.Errorf("StorageIntegrityPhysicalDatabases = %v, want %v", got, want)
	}
}

func TestReservedDatabasesUnionWithMapDerivedDatabases(t *testing.T) {
	v2 := reservedArgs(pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2, true)
	want := []string{"hg_promote", "hg_safe", "hg_unsafe", "safe_x", "unsafe_x"}
	if got := StorageIntegrityPhysicalDatabases(v2); !reflect.DeepEqual(got, want) {
		t.Errorf("StorageIntegrityPhysicalDatabases = %v, want %v", got, want)
	}
	for _, db := range want {
		if !IsStorageIntegrityPhysicalDatabase(db, v2) {
			t.Errorf("IsStorageIntegrityPhysicalDatabase(%q) = false", db)
		}
	}
}

func TestReservedDatabasesIgnoredUnderV1(t *testing.T) {
	v1 := reservedArgs(pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1, true)
	if IsStorageIntegrityPhysicalDatabase("hg_safe", v1) {
		t.Error("V1 must ignore reserved_databases")
	}
	if _, ok := LookupStorageIntegrityPhysical("hg_unsafe", "x", v1); ok {
		t.Error("V1 must ignore reserved_databases")
	}
	if got, want := StorageIntegrityPhysicalDatabases(v1), []string{"safe_x", "unsafe_x"}; !reflect.DeepEqual(got, want) {
		t.Errorf("StorageIntegrityPhysicalDatabases = %v, want %v", got, want)
	}
	v1.StorageIntegrity.ReservedDatabases = []string{"not a name"}
	if err := ValidateStorageIntegrity(v1); err != nil {
		t.Errorf("V1 must not validate reserved_databases: %v", err)
	}
}

func TestValidateStorageIntegrityReservedDatabasesUnderV2(t *testing.T) {
	for _, bad := range []string{"", "hg-safe", "hg_safe.x", "1hg"} {
		a := reservedArgs(pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2, false)
		a.StorageIntegrity.ReservedDatabases = []string{"hg_safe", bad}
		err := ValidateStorageIntegrity(a)
		if err == nil || !strings.Contains(err.Error(), "reserved_databases entry") {
			t.Errorf("reserved database %q: err = %v, want a reserved_databases entry error", bad, err)
		}
	}
	if err := ValidateStorageIntegrity(reservedArgs(pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2, true)); err != nil {
		t.Errorf("valid V2 args: %v", err)
	}
}
```

Create `native_v2_reserved_test.go`:

```go
package rewriter

import (
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
)

var siReservedDatabases = []string{"hg_safe", "hg_unsafe", "hg_promote"}

// v1ReservedEquivalent names every reserved database through a V1 table map,
// which is how V1 learns its protected namespace.
func v1ReservedEquivalent() *pb.RewriteTableDynamicArgs {
	d := v2Dynamic(siV1, false)
	d.StorageIntegrity.Tables = map[string]*pb.StorageIntegrityArgs_Table{
		"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
		"db1.p": {SafeTable: "hg_promote.db1__p", UnsafeTable: "hg_unsafe.db1__p"},
	}
	return d
}

func v2Reserved(withTable bool) *pb.RewriteTableDynamicArgs {
	d := v2Dynamic(siV2, withTable)
	d.StorageIntegrity.ReservedDatabases = siReservedDatabases
	return d
}

var reservedNamespaceSQL = []string{
	"SELECT * FROM hg_safe.x",
	"SELECT * FROM hg_safe.db1__t",
	"INSERT INTO hg_unsafe.x VALUES (1)",
	"INSERT INTO hg_unsafe.x SELECT 1",
	"TRUNCATE TABLE hg_promote.x",
	"DROP TABLE hg_safe.x",
	"TRUNCATE DATABASE hg_safe",
	"SELECT * FROM merge($tag$hg_safe$tag$, 'x')",
	"USE hg_unsafe",
	"SHOW TABLES FROM hg_promote",
	"SYSTEM START MERGES hg_unsafe.x",
}

// TestStorageIntegrityContractV2_ReservedDatabasesMatchV1Protection proves the
// V2 reserved namespace is protected with an empty table map exactly as V1
// protects the physical databases it derives from a non-empty map.
func TestStorageIntegrityContractV2_ReservedDatabasesMatchV1Protection(t *testing.T) {
	e := newEngine(t)
	for _, sql := range reservedNamespaceSQL {
		t.Run(sql, func(t *testing.T) {
			want, err := doRewrite(e, sql, []*pb.RewriteOption{tableRewriteDynamic(v1ReservedEquivalent())})
			if err != nil {
				t.Fatalf("V1 doRewrite: %v", err)
			}
			got, err := doRewrite(e, sql, []*pb.RewriteOption{tableRewriteDynamic(v2Reserved(false))})
			if err != nil {
				t.Fatalf("V2 doRewrite: %v", err)
			}
			if got.GetCode() == pb.RewriteCode_Success {
				t.Fatalf("V2 with reserved databases forwarded %q: %+v", sql, got)
			}
			if got.GetStorageIntegrityContractVersion() != siV2 {
				t.Fatalf("ack = %v, want V2", got.GetStorageIntegrityContractVersion())
			}
			want.StorageIntegrityContractVersion = siV2
			if !proto.Equal(got, want) {
				t.Fatalf("V2 reserved response differs from the V1 map-derived one:\n got %+v\nwant %+v", got, want)
			}
		})
	}
}

func TestStorageIntegrityContractV2_ReservedDatabasesUnionWithTableMap(t *testing.T) {
	e := newEngine(t)
	dyn := v2Reserved(false)
	dyn.StorageIntegrity.Tables = map[string]*pb.StorageIntegrityArgs_Table{
		"db1.t": {SafeTable: "safe_x.db1__t", UnsafeTable: "unsafe_x.db1__t"},
	}
	for sql, msg := range map[string]string{
		"SELECT * FROM safe_x.y":  "storage-integrity physical table safe_x.y is not directly addressable",
		"SELECT * FROM hg_safe.y": "storage-integrity physical table hg_safe.y is not directly addressable",
	} {
		resp, err := doRewrite(e, sql, []*pb.RewriteOption{tableRewriteDynamic(dyn)})
		if err != nil {
			t.Fatalf("doRewrite(%q): %v", sql, err)
		}
		if resp.GetCode() != pb.RewriteCode_RewriteError || resp.GetMessage() != msg {
			t.Fatalf("%q: resp = %+v, want RewriteError %q", sql, resp, msg)
		}
	}
}

func TestStorageIntegrityContractV1_IgnoresReservedDatabases(t *testing.T) {
	e := newEngine(t)
	for _, withTable := range []bool{false, true} {
		plain := v2Dynamic(siV1, withTable)
		reserved := proto.Clone(plain).(*pb.RewriteTableDynamicArgs)
		reserved.StorageIntegrity.ReservedDatabases = append([]string{"not a name"}, siReservedDatabases...)
		for _, sql := range reservedNamespaceSQL {
			want, err := doRewrite(e, sql, []*pb.RewriteOption{tableRewriteDynamic(plain)})
			if err != nil {
				t.Fatalf("doRewrite: %v", err)
			}
			got, err := doRewrite(e, sql, []*pb.RewriteOption{tableRewriteDynamic(reserved)})
			if err != nil {
				t.Fatalf("doRewrite: %v", err)
			}
			if !proto.Equal(got, want) {
				t.Fatalf("V1 (tables=%v) %q changed by reserved_databases:\n got %+v\nwant %+v", withTable, sql, got, want)
			}
		}
	}
}

func TestStorageIntegrityContractV2_RejectsMalformedReservedDatabase(t *testing.T) {
	e := newEngine(t)
	dyn := v2Reserved(false)
	dyn.StorageIntegrity.ReservedDatabases = []string{"hg_safe", "hg-unsafe"}
	resp, err := doRewrite(e, "SELECT 1", []*pb.RewriteOption{tableRewriteDynamic(dyn)})
	if err != nil {
		t.Fatalf("doRewrite: %v", err)
	}
	if resp.GetCode() != pb.RewriteCode_InvalidRewriteRequest ||
		resp.GetMessage() != `storage-integrity reserved_databases entry "hg-unsafe" must be a simple identifier` ||
		resp.GetStorageIntegrityContractVersion() != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED {
		t.Fatalf("resp = %+v, want an unacknowledged InvalidRewriteRequest", resp)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -count=1 ./internal/nameresolve/ && go test -count=1 -run 'Reserved' .`
Expected (measured): `TestReservedDatabasesProtectedUnderV2` fails with `IsStorageIntegrityPhysicalDatabase("hg_safe") = false under V2 with an empty map` (and the same for `hg_unsafe`, `hg_promote`, the lookup, the unqualified case and `StorageIntegrityPhysicalDatabases = [], want [hg_promote hg_safe hg_unsafe]`); `TestReservedDatabasesUnionWithMapDerivedDatabases` fails with `StorageIntegrityPhysicalDatabases = [safe_x unsafe_x], want [hg_promote hg_safe hg_unsafe safe_x unsafe_x]`; `TestValidateStorageIntegrityReservedDatabasesUnderV2` fails for all four malformed entries; in the root package `TestStorageIntegrityContractV2_ReservedDatabasesMatchV1Protection`, `TestStorageIntegrityContractV2_ReservedDatabasesUnionWithTableMap` and `TestStorageIntegrityContractV2_RejectsMalformedReservedDatabase` fail (for example `V2 with reserved databases forwarded "SELECT * FROM hg_safe.x"`). `TestReservedDatabasesIgnoredUnderV1` and `TestStorageIntegrityContractV1_IgnoresReservedDatabases` already pass.

- [ ] **Step 3: Implement**

Append to `internal/nameresolve/contract.go`:

```go

// storageIntegrityReservedDatabase reports whether db is one of the V2
// reserved_databases. V1 ignores the field, so its protected namespace stays
// the databases derived from the table map, byte-for-byte.
func storageIntegrityReservedDatabase(db string, a *pb.RewriteTableDynamicArgs) bool {
	si := a.GetStorageIntegrity()
	if db == "" || si.GetContractVersion() != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2 {
		return false
	}
	for _, reserved := range si.GetReservedDatabases() {
		if reserved == db {
			return true
		}
	}
	return false
}
```

In `internal/nameresolve/resolve.go` replace:

```go
// LookupStorageIntegrityPhysical reports whether the caller addressed one of
// the protocol-owned safe/unsafe physical table names directly. Those names are
// never part of the public SQL surface, even when the physical database is not
// listed in known_physical_databases.
```

with:

```go
// LookupStorageIntegrityPhysical reports whether the caller addressed one of
// the protocol-owned safe/unsafe physical table names directly, or (contract
// V2) any table in a reserved_databases namespace. Those names are never part
// of the public SQL surface, even when the physical database is not listed in
// known_physical_databases. The logical key is empty for a reserved-database
// hit that no table-map entry names.
```

and at the end of that function replace:

```go
			if ok && effectiveDB == physicalDB {
				return logicalKey, true
			}
		}
	}
	return "", false
}
```

with:

```go
			if ok && effectiveDB == physicalDB {
				return logicalKey, true
			}
		}
	}
	return "", storageIntegrityReservedDatabase(effectiveDB, a)
}
```

Replace:

```go
// IsStorageIntegrityPhysicalDatabase reports whether db is one of the
// configured safe/unsafe physical namespaces. The reservation is database-wide:
```

with:

```go
// IsStorageIntegrityPhysicalDatabase reports whether db is one of the
// configured safe/unsafe physical namespaces, or (contract V2) one of
// reserved_databases. The reservation is database-wide:
```

and at the end of that function replace:

```go
			physicalDB, _, ok := exactQualifiedTable(physical)
			if ok && db == physicalDB {
				return true
			}
		}
	}
	return false
}
```

with:

```go
			physicalDB, _, ok := exactQualifiedTable(physical)
			if ok && db == physicalDB {
				return true
			}
		}
	}
	return storageIntegrityReservedDatabase(db, a)
}
```

Replace:

```go
// StorageIntegrityPhysicalDatabases returns the configured protocol-owned
// safe/unsafe database namespaces in deterministic order. It is used to attach
// conservative SI classification metadata when a table-function database
// expression cannot be resolved statically.
```

with:

```go
// StorageIntegrityPhysicalDatabases returns the configured protocol-owned
// safe/unsafe database namespaces, plus (contract V2) reserved_databases, in
// deterministic order. It is used to attach conservative SI classification
// metadata when a table-function database expression cannot be resolved
// statically.
```

and inside it replace:

```go
			db, _, ok := exactQualifiedTable(physical)
			if ok {
				seen[db] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
```

with:

```go
			db, _, ok := exactQualifiedTable(physical)
			if ok {
				seen[db] = true
			}
		}
	}
	for _, db := range a.GetStorageIntegrity().GetReservedDatabases() {
		if storageIntegrityReservedDatabase(db, a) {
			seen[db] = true
		}
	}
	out := make([]string, 0, len(seen))
```

At the end of `ValidateStorageIntegrity` replace:

```go
		if _, _, ok := exactQualifiedTable(tbl.GetUnsafeTable()); !ok {
			return fmt.Errorf("storage-integrity table %s unsafe_table %s must have exact <database>.<table> shape", key, tbl.GetUnsafeTable())
		}
	}
	return nil
}
```

with:

```go
		if _, _, ok := exactQualifiedTable(tbl.GetUnsafeTable()); !ok {
			return fmt.Errorf("storage-integrity table %s unsafe_table %s must have exact <database>.<table> shape", key, tbl.GetUnsafeTable())
		}
	}
	if si.GetContractVersion() == pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2 {
		for _, db := range si.GetReservedDatabases() {
			if !simpleIdentifier(db) {
				return fmt.Errorf("storage-integrity reserved_databases entry %q must be a simple identifier", db)
			}
		}
	}
	return nil
}
```

No handler changes: every protected-namespace check in `internal/handlers` (direct table nodes, write targets, DESCRIBE/EXISTS, GRANT/REVOKE, USE/SHOW, database DDL, SYSTEM targets, table-function carriers and heredoc bodies, reject annotation) already calls one of these three helpers (P11).

- [ ] **Step 4: Run to verify it passes**

Run: `go vet ./... && SNAPSHOT_QUERY_ORDINARY=1 go test -count=1 ./...`
Expected: every package `ok`. Measured on the prototype: under V2 with `tables` empty and `reserved_databases` `["hg_safe", "hg_unsafe", "hg_promote"]`, the 11 statements in `reservedNamespaceSQL` answer exactly as V1 does with a map naming those databases (for example `SELECT * FROM hg_safe.x` → `RewriteError` `storage-integrity physical table hg_safe.x is not directly addressable`; `INSERT INTO hg_unsafe.x VALUES (1)` and `TRUNCATE TABLE hg_promote.x` → `UnsupportedStatement` with the physical-table message; `TRUNCATE DATABASE hg_safe` → `UnsupportedStatement` `storage-integrity physical database hg_safe is not directly addressable`); `SELECT * FROM other.u` stays `Success`.

- [ ] **Step 5: Commit**

```bash
git add internal/nameresolve/contract.go internal/nameresolve/resolve.go internal/nameresolve/reserved_test.go native_v2_reserved_test.go
git commit -m "feat(storage-integrity): protect V2 reserved_databases as physical namespaces" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 5: rewriter-go — shared corpus `contract_version`, `reserved_databases` and the V2 cases

**Files:**
- Modify: `internal/harness/testdata/storage_integrity_cases.json` (every case, plus 19 appended)
- Modify: `internal/harness/sicorpus_test.go:39-44` (`SIArgs`), `:60-89` (`SICase`), `:125` (contract map), `:150-153` (R8), `:230-234` (pins)
- Modify: `internal/harness/storage_integrity_golden_test.go:66-70` (`options`), `:86` (`wantContractAck`), `:135-143`
- Modify: `internal/harness/sicorpus_contract_test.go` (V1 marks, three new tests)
- Modify: `internal/harness/AGENTS.md:62-63`, `README.md:30-41`

**Interfaces:**
- Consumes: Tasks 2–4 behaviour.
- Produces: corpus keys `contract_version` and `storage_integrity.reserved_databases`; `SICase.ContractVersion string`; `SIArgs.ReservedDatabases []string`; `siContractByName map[string]pb.StorageIntegrityContractVersion`; `func (SICase) wantContractAck() pb.StorageIntegrityContractVersion`; rule `R8`; pins `SICorpusFingerprint = 7051648083520101593`, `SICorpusBytes = 254170`, `SICorpusCases = 256`.

- [ ] **Step 1: Mark the existing validator literals and write the failing tests**

Save as `/tmp/mark_v1.py` (any scratch path) and run it from the rewriter-go root; it adds `ContractVersion: "V1"` to the seven multi-line and one single-line `SICase` literals in `internal/harness/sicorpus_contract_test.go`:

```python
import re

p = "internal/harness/sicorpus_contract_test.go"
s = open(p).read()


def mark(m):
    head, indent = m.group(1), m.group(2)
    return f'{head}\n{indent}ContractVersion: "V1",\n{indent}'


s, n = re.subn(r"(ValidateSICorpus\(\[\]SICase\{\{|c := SICase\{)\n(\s+)", mark, s)
s = s.replace(
    'SICase{{Name: "unpinned", SQL: "SELECT 1", WantCode: "Success"}}',
    'SICase{{ContractVersion: "V1", Name: "unpinned", SQL: "SELECT 1", WantCode: "Success"}}',
)
assert n == 7, n
open(p, "w").write(s)
```

Then add the `pb` import to `internal/harness/sicorpus_contract_test.go` by replacing:

```go
	"strings"
	"testing"
)
```

with:

```go
	"strings"
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)
```

and append to the end of the file:

```go

func TestValidateSICorpus_RequiresKnownContractVersion(t *testing.T) {
	for _, version := range []string{"", "V0", "v2", "V3"} {
		t.Run("contract_version="+version, func(t *testing.T) {
			got := ValidateSICorpus([]SICase{{
				Name: "versioned", SQL: "SELECT 1", ContractVersion: version,
				WantCode: "Success", WantSQL: "SELECT 1",
			}})
			if len(got) != 1 || !strings.Contains(got[0], "R8") || !strings.Contains(got[0], "contract_version") {
				t.Fatalf("want one R8 violation, got %v", got)
			}
		})
	}
	for _, version := range []string{"V1", "V2"} {
		got := ValidateSICorpus([]SICase{{
			Name: "versioned", SQL: "SELECT 1", ContractVersion: version,
			WantCode: "Success", WantSQL: "SELECT 1",
		}})
		if len(got) != 0 {
			t.Fatalf("contract_version %s: want no violation, got %v", version, got)
		}
	}
}

func TestSICaseOptionsCarryTheCaseContractVersion(t *testing.T) {
	for name, want := range map[string]pb.StorageIntegrityContractVersion{
		"V1": pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1,
		"V2": pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2,
	} {
		c := SICase{ContractVersion: name, Dynamic: &SIDynamic{StorageIntegrity: &SIArgs{Tables: map[string]SITable{}}}}
		got := c.options()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity().GetContractVersion()
		if got != want {
			t.Fatalf("options() contract_version for %s = %v, want %v", name, got, want)
		}
	}
}

func TestSICaseOptionsCarryReservedDatabases(t *testing.T) {
	c := SICase{ContractVersion: "V2", Dynamic: &SIDynamic{StorageIntegrity: &SIArgs{
		Tables: map[string]SITable{}, ReservedDatabases: []string{"hg_safe", "hg_unsafe", "hg_promote"},
	}}}
	got := c.options()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity().GetReservedDatabases()
	if !reflect.DeepEqual(got, []string{"hg_safe", "hg_unsafe", "hg_promote"}) {
		t.Fatalf("options() reserved_databases = %v", got)
	}
}
```

Run `gofmt -w internal/harness/sicorpus_contract_test.go`.

- [ ] **Step 2: Run to verify it fails**

Run: `go vet ./internal/harness/`
Expected: `unknown field ContractVersion in struct literal of type SICase` and `unknown field ReservedDatabases in struct literal of type SIArgs`.

- [ ] **Step 3: Implement the harness**

In `internal/harness/sicorpus_test.go` replace:

```go
	ReservedRowIDColumn string             `json:"reserved_row_id_column,omitempty"`
}
```

(the end of `SIArgs`) with:

```go
	ReservedRowIDColumn string             `json:"reserved_row_id_column,omitempty"`
	ReservedDatabases   []string           `json:"reserved_databases,omitempty"` // contract V2 only
}
```

replace:

```go
//   - want_sql_contains is an additional assertion only, and may not contain a
//     substring that is already present in the input SQL.
type SICase struct {
	Name                string            `json:"name"`
	SQL                 string            `json:"sql"`
```

with:

```go
//   - want_sql_contains is an additional assertion only, and may not contain a
//     substring that is already present in the input SQL;
//   - every case names the storage-integrity contract it is sent under,
//     contract_version "V1" or "V2" (R8).
type SICase struct {
	Name                string            `json:"name"`
	SQL                 string            `json:"sql"`
	ContractVersion     string            `json:"contract_version,omitempty"`
```

replace:

```go
var siKnownCodes = map[string]pb.RewriteCode{
```

with:

```go
// siContractByName maps a case's contract_version to the value the runners
// send in StorageIntegrityArgs.contract_version.
var siContractByName = map[string]pb.StorageIntegrityContractVersion{
	"V1": pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1,
	"V2": pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2,
}

var siKnownCodes = map[string]pb.RewriteCode{
```

and replace:

```go
		if strings.TrimSpace(c.SQL) == "" {
			add("R2", "sql must be non-empty")
		}
```

with:

```go
		if strings.TrimSpace(c.SQL) == "" {
			add("R2", "sql must be non-empty")
		}
		if _, ok := siContractByName[c.ContractVersion]; !ok {
			add("R8", fmt.Sprintf("contract_version must be \"V1\" or \"V2\", got %q", c.ContractVersion))
		}
```

In `internal/harness/storage_integrity_golden_test.go` replace:

```go
			ContractVersion:     pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1,
		}
```

with:

```go
			ContractVersion:     siContractByName[c.ContractVersion],
			ReservedDatabases:   si.ReservedDatabases,
		}
```

replace:

```go
// TestStorageIntegrityGolden is the Spec G parity gate.
```

with:

```go
// wantContractAck is the acknowledgement a case expects: its own version once
// the surface is active (V1 with a non-empty table map, or any V2 request),
// unless the case pins a pre-acknowledgement rejection.
func (c SICase) wantContractAck() pb.StorageIntegrityContractVersion {
	if c.Dynamic == nil || c.Dynamic.StorageIntegrity == nil || c.WantNoContractAck {
		return pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED
	}
	version := siContractByName[c.ContractVersion]
	if version == pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2 ||
		len(c.Dynamic.StorageIntegrity.Tables) > 0 {
		return version
	}
	return pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED
}

// TestStorageIntegrityGolden is the Spec G parity gate.
```

and replace:

```go
			if c.Dynamic != nil && c.Dynamic.StorageIntegrity != nil && len(c.Dynamic.StorageIntegrity.Tables) > 0 {
				want := pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1
				if c.WantNoContractAck {
					want = pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED
				}
				if res.StorageIntegrityContractVersion != want {
					t.Errorf("storage_integrity_contract_version = %v, want %v", res.StorageIntegrityContractVersion, want)
				}
			}
```

with:

```go
			if want := c.wantContractAck(); res.StorageIntegrityContractVersion != want {
				t.Errorf("storage_integrity_contract_version = %v, want %v", res.StorageIntegrityContractVersion, want)
			}
```

- [ ] **Step 4: Run to verify the corpus now fails the contract**

Run: `go test -count=1 -run 'TestSICorpusContract|TestValidateSICorpus|TestSICaseOptions' ./internal/harness/`
Expected: the new unit tests pass; `TestSICorpusContract` fails with 237 lines `R8: contract_version must be "V1" or "V2", got ""`.

- [ ] **Step 5: Migrate the corpus**

Save as `/tmp/migrate_corpus.py` and run `python3 /tmp/migrate_corpus.py internal/harness/testdata/storage_integrity_cases.json`:

```python
"""Mark every existing shared SI case as contract V1 and append the V2 cases."""
import sys

path = sys.argv[1]
text = open(path, encoding="utf-8").read()
lines = text.split("\n")
out = []
for line in lines:
    out.append(line)
    if line == "  {":
        out.append('    "contract_version": "V1",')
text = "\n".join(out)

SI = '"tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}}'
EMPTY = '"tables": {}'


RESERVED = ',\n        "reserved_databases": ["hg_safe", "hg_unsafe", "hg_promote"]'


def case(name, version, sql, tables, body, context=False, reserved=False):
    ctx = '\n      "upstream_logical_database_in_context": "db1",' if context else ""
    res = RESERVED if reserved else ""
    return f'''  {{
    "name": "{name}",
    "contract_version": "{version}",
    "sql": "{sql}",
    "dynamic": {{
      "database_map": {{"db1": "phys", "other": "phys"}},
      "known_physical_databases": ["phys"],{ctx}
      "delim": "_",
      "storage_integrity": {{
        {tables},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"{res}
      }}
    }},
{body}
  }}'''


SIGNED = "storage-integrity table db1.t accepts writes only through the signed statement lane"
UNMODELLED = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"
SI_T = '{"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}'
OTHER_U = '{"original_database": "other", "original_table": "u", "logical_database": "other", "physical_database": "phys"}'


def success(sql, rewrites, accessed):
    return f'''    "want_code": "Success", "want_stmt": "DROP_TABLE",
    "want_sql": "{sql}",
    "want_table_rewrites": {rewrites},
    "want_accessed": [{accessed}]'''


def physical(db, table):
    return f'{{"original_database": "{db}", "original_table": "{table}", "physical_database": "{db}", "is_storage_integrity": true}}'


def reject(code, message, accessed=None):
    extra = f',\n    "want_accessed": [{accessed}]' if accessed else ""
    return f'''    "reject": true,
    "want_code": "{code}", "want_stmt": "",
    "want_message_contains": "{message}"{extra}'''


cases = [
    case("si_v2_drop_table", "V2", "DROP TABLE db1.t", SI,
         success('DROP TABLE phys.\\"db1.t\\"', '{"db1.t": "phys.db1.t"}', SI_T)),
    case("si_v2_drop_table_if_exists_sync", "V2", "DROP TABLE IF EXISTS db1.t SYNC", SI,
         success('DROP TABLE IF EXISTS phys.\\"db1.t\\" SYNC', '{"db1.t": "phys.db1.t"}', SI_T)),
    case("si_v2_drop_multi_mixed", "V2", "DROP TABLE db1.t, other.u", SI,
         success('DROP TABLE phys.\\"db1.t\\", phys.\\"other.u\\"',
                 '{"db1.t": "phys.db1.t", "other.u": "phys.other.u"}', SI_T + ", " + OTHER_U)),
    case("si_v2_drop_unqualified_in_context", "V2", "DROP TABLE t", SI,
         success('DROP TABLE phys.\\"db1.t\\"', '{"t": "phys.db1.t"}',
                 '{"original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}'),
         context=True),
    case("si_v2_truncate_rejected", "V2", "TRUNCATE TABLE db1.t", SI,
         reject("UnsupportedStatement", SIGNED)),
    case("si_v2_drop_physical_rejected", "V2", "DROP TABLE hg_safe.db1__t", SI,
         reject("UnsupportedStatement", "storage-integrity physical table hg_safe.db1__t is not directly addressable",
                '{"original_database": "hg_safe", "original_table": "db1__t", "physical_database": "hg_safe", "is_storage_integrity": true}')),
    case("si_v2_drop_on_cluster_rejected", "V2", "DROP TABLE db1.t ON CLUSTER c", SI,
         reject("UnsupportedStatement", SIGNED)),
    case("si_v2_drop_view_rejected", "V2", "DROP VIEW db1.t", SI,
         reject("UnsupportedStatement", SIGNED)),
    case("si_v2_drop_dictionary_rejected", "V2", "DROP DICTIONARY db1.t", SI,
         reject("UnsupportedStatement", SIGNED)),
    case("si_v2_empty_map_set_rejected", "V2", "SET max_threads = 1", EMPTY,
         reject("UnsupportedStatement", UNMODELLED)),
    case("si_v2_empty_map_system_rejected", "V2", "SYSTEM RELOAD CONFIG", EMPTY,
         reject("UnsupportedStatement", UNMODELLED)),
    case("si_v1_drop_if_exists_sync_rejected", "V1", "DROP TABLE IF EXISTS db1.t SYNC", SI,
         reject("UnsupportedStatement", SIGNED)),
    case("si_v1_drop_unqualified_in_context_rejected", "V1", "DROP TABLE t", SI,
         reject("UnsupportedStatement", SIGNED), context=True),
    case("si_v2_reserved_select_rejected", "V2", "SELECT * FROM hg_safe.x", EMPTY,
         reject("RewriteError", "storage-integrity physical table hg_safe.x is not directly addressable",
                physical("hg_safe", "x")), reserved=True),
    case("si_v2_reserved_insert_rejected", "V2", "INSERT INTO hg_unsafe.x VALUES (1)", EMPTY,
         reject("UnsupportedStatement", "storage-integrity physical table hg_unsafe.x is not directly addressable",
                physical("hg_unsafe", "x")), reserved=True),
    case("si_v2_reserved_truncate_rejected", "V2", "TRUNCATE TABLE hg_promote.x", EMPTY,
         reject("UnsupportedStatement", "storage-integrity physical table hg_promote.x is not directly addressable",
                physical("hg_promote", "x")), reserved=True),
    case("si_v2_reserved_truncate_database_rejected", "V2", "TRUNCATE DATABASE hg_safe", EMPTY,
         reject("UnsupportedStatement", "storage-integrity physical database hg_safe is not directly addressable"),
         reserved=True),
    case("si_v2_reserved_merge_heredoc_rejected", "V2", "SELECT * FROM merge($tag$hg_safe$tag$, 'x')", EMPTY,
         reject("RewriteError", "storage-integrity physical table hg_safe.x is not directly addressable",
                physical("hg_safe", "x")), reserved=True),
    case("si_v2_reserved_union_with_table_map_rejected", "V2", "SELECT * FROM hg_promote.x", SI,
         reject("RewriteError", "storage-integrity physical table hg_promote.x is not directly addressable",
                physical("hg_promote", "x")), reserved=True),
]

tail = "\n  }\n]\n"
assert text.endswith(tail), "corpus must end with the last case and the closing bracket"
text = text[: -len(tail)] + "\n  },\n" + ",\n".join(cases) + "\n]\n"
open(path, "w", encoding="utf-8").write(text)
```

Then update the pins in `internal/harness/sicorpus_test.go`, replacing:

```go
	SICorpusFingerprint uint64 = 4366038644618079701
	SICorpusBytes       int    = 232837
	SICorpusCases       int    = 237
```

with:

```go
	SICorpusFingerprint uint64 = 7051648083520101593
	SICorpusBytes       int    = 254170
	SICorpusCases       int    = 256
```

Verify: `shasum -a 256 internal/harness/testdata/storage_integrity_cases.json` prints `5fe367217619243729045820135340ae061891b4c2f85606cd61994482072615`.

- [ ] **Step 6: Update the docs**

In `internal/harness/AGENTS.md`, replace:

```markdown
- Unknown JSON keys and content after the single corpus JSON value fail the
  strict load. Do not silently accept old keys such as `sql_exact`.
```

with:

```markdown
- Unknown JSON keys and content after the single corpus JSON value fail the
  strict load. Do not silently accept old keys such as `sql_exact`.
- Every case carries `contract_version` (`"V1"` or `"V2"`, rule R8). Both
  runners send it as `StorageIntegrityArgs.contract_version` and expect that
  version acknowledged whenever the surface is active: V1 with a non-empty
  `tables` map, or any V2 case. An optional
  `storage_integrity.reserved_databases` string array is sent as
  `StorageIntegrityArgs.reserved_databases` (read by the engines under V2
  only).
```

In `README.md`, replace:

```markdown
and every accepted SI request positively acknowledges contract v1 in the response.
```

with:

```markdown
and every accepted SI request positively acknowledges its contract version (v1 or
v2) in the response. Contract v2 (dynamic table set) differs from v1 in three ways:
it activates the SI surface by version, even with an empty table map; it protects
every database in `reserved_databases` exactly like a database named by a
safe/unsafe table (v1 ignores that field); and it accepts a plain
`DROP TABLE [IF EXISTS] name[, name...] [SYNC]` of logical SI tables by dropping
only their ordinary physical tables.
```

- [ ] **Step 7: Run to verify it passes**

Run: `SNAPSHOT_QUERY_ORDINARY=1 go test -count=1 ./... && go test -count=1 -run 'TestStorageIntegrityGolden/si_v' -v ./internal/harness/ | grep -c -- '--- PASS: TestStorageIntegrityGolden/'`
Expected: every package `ok`; the grep prints `19` (`si_v2_drop_table`, `si_v2_drop_table_if_exists_sync`, `si_v2_drop_multi_mixed`, `si_v2_drop_unqualified_in_context`, `si_v2_truncate_rejected`, `si_v2_drop_physical_rejected`, `si_v2_drop_on_cluster_rejected`, `si_v2_drop_view_rejected`, `si_v2_drop_dictionary_rejected`, `si_v2_empty_map_set_rejected`, `si_v2_empty_map_system_rejected`, `si_v1_drop_if_exists_sync_rejected`, `si_v1_drop_unqualified_in_context_rejected`, `si_v2_reserved_select_rejected`, `si_v2_reserved_insert_rejected`, `si_v2_reserved_truncate_rejected`, `si_v2_reserved_truncate_database_rejected`, `si_v2_reserved_merge_heredoc_rejected`, `si_v2_reserved_union_with_table_map_rejected`). `git diff --exit-code --ignore-submodules=all` after the run shows only this task's edits (CI's "tests did not mutate tracked files" gate).

- [ ] **Step 8: Commit and open the PR**

```bash
git add internal/harness/testdata/storage_integrity_cases.json internal/harness/sicorpus_test.go internal/harness/sicorpus_contract_test.go internal/harness/storage_integrity_golden_test.go internal/harness/AGENTS.md README.md
git commit -m "test(storage-integrity): version every shared corpus case and add the contract V2 and reserved-database cases" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/si-contract-v2
gh pr create -R housegate/rewriter-go --fill
```

Record in the PR description: corpus SHA-256 `5fe367217619243729045820135340ae061891b4c2f85606cd61994482072615`, 256 cases, 254170 bytes. The controller merges it (gate R2) before Task 10 Step 1.

---

## Task 6: rewriter-grpc — proto submodule and version-based activation

**Precondition:** Task 11 gate R1 is done (`PROTO_SHA` is on rewriter-proto `main`).

**Files:**
- Modify: submodule `third_party/rewriter-proto` → `PROTO_SHA`
- Modify: `src/handlers/storage_integrity.h:16-17`, `:29-31`; `src/handlers/storage_integrity.cc:807` (new definitions before it), `:1475`, `:1564`, `:1625`
- Modify: `src/handlers/grant.cc:89-91`, `src/handlers/select.cc:985-988`, `src/handlers/show_tables.cc:38-41`, `src/handlers/writes.cc:217-221`
- Modify: `src/rewriter-server.cc:332-362`
- Test: modify `tests/rewriter_test.cc` (`StorageIntegrityContract.RejectsMissingAndUnknownBeforeAcknowledgement` at 4566-4579; three new `TEST`s before `TEST(StorageIntegrityCatchAll, StructuredSystemTargetsSkipClusterDecoys)` at 4659)

**Interfaces:**
- Consumes: `rewriter::STORAGE_INTEGRITY_CONTRACT_V2` (Task 1).
- Produces: `bool rewriter_handlers::storageIntegritySurfaceActive(const rewriter::RewriteTableDynamicArgs &args)`, `bool rewriter_handlers::storageIntegrityDropContract(const rewriter::RewriteTableDynamicArgs &args)`, `kStorageIntegrityContractMessage = "storage-integrity contract version V1 or V2 is required"`; test helper `AddV2Option(rewriter::RewriteSQLRequest &, rewriter::StorageIntegrityContractVersion, bool)`.

- [ ] **Step 1: Branch, bump the submodule, write the failing tests**

```bash
cd rewriter-grpc && git fetch origin && git switch -c feat/si-contract-v2 origin/main
git submodule update --init third_party/rewriter-proto
git -C third_party/rewriter-proto fetch origin && git -C third_party/rewriter-proto checkout --detach $PROTO_SHA
git -C third_party/rewriter-proto grep -n 'STORAGE_INTEGRITY_CONTRACT_V2 = 2' proto/rewriter.proto   # must match
```

In `tests/rewriter_test.cc`, in `TEST(StorageIntegrityContract, RejectsMissingAndUnknownBeforeAcknowledgement)` replace:

```cpp
    EXPECT_EQ(resp.sql_after_rewrite(), "SELECT 1");
  }
}
```

(the first occurrence after that `TEST` line) with:

```cpp
    EXPECT_EQ(resp.sql_after_rewrite(), "SELECT 1");
    EXPECT_EQ(resp.message(), rewriter_handlers::kStorageIntegrityContractMessage);
  }
}
```

Immediately before `TEST(StorageIntegrityCatchAll, StructuredSystemTargetsSkipClusterDecoys) {` insert:

```cpp
// ---- Storage-integrity contract V2 (dynamic table set, housegate sub-project 3)
namespace {

// Probe-shaped V2 request: db1 and other map to phys; the SI table map carries
// db1.t or is empty (H6).
rewriter::RewriteTableDynamicArgs *AddV2Option(
  rewriter::RewriteSQLRequest &req,
  rewriter::StorageIntegrityContractVersion version,
  bool with_table) {
  auto *si = AddSIContractOption(req, version);
  if (!with_table) si->mutable_tables()->clear();
  si->set_reserved_row_id_column("_hg_row_id");
  auto *dyn = req.mutable_options(req.options_size() - 1)
                ->mutable_table_name_args()->mutable_dynamic_args();
  (*dyn->mutable_database_map())["other"] = "phys";
  dyn->set_delim("_");
  return dyn;
}

}  // namespace

TEST(StorageIntegrityContractV2, EmptyMapActivatesCatchAll) {
  for (const char *sql : {"SYSTEM RELOAD CONFIG", "SET max_threads = 1"}) {
    SCOPED_TRACE(sql);
    rewriter::RewriteSQLRequest req;
    req.set_sql(sql);
    AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V2, false);
    const auto resp = RunDirectRewrite(std::move(req));
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement);
    EXPECT_EQ(resp.message(), rewriter_handlers::kStorageIntegrityUnmodelledMessage);
    EXPECT_EQ(resp.sql_after_rewrite(), sql);
    EXPECT_EQ(resp.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
    EXPECT_EQ(resp.storage_integrity_contract_version(),
              rewriter::STORAGE_INTEGRITY_CONTRACT_V2);
  }
}

TEST(StorageIntegrityContractV2, V1EmptyMapStaysLegacy) {
  rewriter::RewriteSQLRequest req;
  req.set_sql("SYSTEM RELOAD CONFIG");
  AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V1, false);
  const auto resp = RunDirectRewrite(std::move(req));
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
  EXPECT_EQ(resp.storage_integrity_contract_version(),
            rewriter::STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED);
}

TEST(StorageIntegrityContractV2, AcknowledgesV2OnEveryPath) {
  struct Case {
    const char *sql;
    bool with_table;
    rewriter::RewriteCode code;
  };
  const std::vector<Case> cases = {
    {"SELECT a FROM db1.t", true, rewriter::RewriteCode::Success},
    {"SELECT a FROM db1.t", false, rewriter::RewriteCode::Success},
    {"TRUNCATE TABLE db1.t", true, rewriter::RewriteCode::UnsupportedStatement},
    {"SELECT (", false, rewriter::RewriteCode::SyntaxError},
  };
  for (const auto &c : cases) {
    SCOPED_TRACE(c.sql);
    rewriter::RewriteSQLRequest req;
    req.set_sql(c.sql);
    AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V2, c.with_table);
    const auto resp = RunDirectRewrite(std::move(req));
    EXPECT_EQ(resp.code(), c.code) << resp.message();
    EXPECT_EQ(resp.storage_integrity_contract_version(),
              rewriter::STORAGE_INTEGRITY_CONTRACT_V2);
  }
}

```

- [ ] **Step 2: Expected failure (build box)**

Run: CI `remote-build` (`./scripts.sh rebuild && ninja -C build rewriter_tests`, then `ctest`). Expected before Step 3: compile succeeds (the regenerated proto defines V2), and `StorageIntegrityContractV2.EmptyMapActivatesCatchAll` fails (`code()` is `Success`, acknowledgement `UNSPECIFIED`), `StorageIntegrityContractV2.AcknowledgesV2OnEveryPath` fails on the `with_table` rows (`InvalidRewriteRequest`, message `storage-integrity contract version V1 is required`) and on the empty-map rows (acknowledgement `UNSPECIFIED`), and `RejectsMissingAndUnknownBeforeAcknowledgement` fails on the message. `V1EmptyMapStaysLegacy` passes. Per P12 this run is batched with Tasks 7–10.

- [ ] **Step 3: Implement**

In `src/handlers/storage_integrity.h` replace:

```cpp
constexpr std::string_view kStorageIntegrityContractMessage =
  "storage-integrity contract version V1 is required";
```

with:

```cpp
constexpr std::string_view kStorageIntegrityContractMessage =
  "storage-integrity contract version V1 or V2 is required";
```

and replace:

```cpp
// Validate every v1 proof field before acknowledgement. Empty SI table maps
// are legacy/no-op and are intentionally not activated by the caller.
bool validateStorageIntegrity(const rewriter::RewriteTableDynamicArgs &args, std::string *error);
```

with:

```cpp
// Validate every proof field before acknowledgement. A V1 request with an
// empty table map is legacy/no-op and is not activated by the caller; a V2
// request is activated by version (see storageIntegritySurfaceActive).
bool validateStorageIntegrity(const rewriter::RewriteTableDynamicArgs &args, std::string *error);

// True when the request activates the storage-integrity surface: any V2
// request, even with an empty table map (housegate sub-project 3, H6), or any
// other request whose table map is non-empty. Mirrors rewriter-go's
// nameresolve.StorageIntegritySurfaceActive.
bool storageIntegritySurfaceActive(const rewriter::RewriteTableDynamicArgs &args);

// True when the request carries contract V2, whose only write-policy
// difference from V1 is that a plain DROP TABLE of a logical SI table is
// accepted. Mirrors rewriter-go's nameresolve.StorageIntegrityDropContract.
bool storageIntegrityDropContract(const rewriter::RewriteTableDynamicArgs &args);
```

In `src/handlers/storage_integrity.cc` replace:

```cpp
bool validateStorageIntegrity(const rewriter::RewriteTableDynamicArgs &args, std::string *error) {
```

with:

```cpp
bool storageIntegritySurfaceActive(const rewriter::RewriteTableDynamicArgs &args) {
  if (!args.has_storage_integrity()) return false;
  const auto &si = args.storage_integrity();
  return si.contract_version() == rewriter::STORAGE_INTEGRITY_CONTRACT_V2
    || !si.tables().empty();
}

bool storageIntegrityDropContract(const rewriter::RewriteTableDynamicArgs &args) {
  return args.has_storage_integrity()
    && args.storage_integrity().contract_version()
         == rewriter::STORAGE_INTEGRITY_CONTRACT_V2;
}

bool validateStorageIntegrity(const rewriter::RewriteTableDynamicArgs &args, std::string *error) {
```

and switch the three activation checks:

```bash
sed -i.bak 's/if (!args.has_storage_integrity() || args.storage_integrity().tables().empty()) return/if (!storageIntegritySurfaceActive(args)) return/' src/handlers/storage_integrity.cc
rm src/handlers/storage_integrity.cc.bak
grep -c 'if (!storageIntegritySurfaceActive(args)) return' src/handlers/storage_integrity.cc   # 3
```

(`lookupStorageIntegrity`'s own `tables().empty()` early exit at line 863 stays: with an empty map the lookup misses either way.)

In `src/handlers/grant.cc` replace:

```cpp
  if (selection.mode == TableRewriteMode::Dynamic
      && selection.dynamic_args->has_storage_integrity()
      && !selection.dynamic_args->storage_integrity().tables().empty()) {
```

with:

```cpp
  if (selection.mode == TableRewriteMode::Dynamic
      && storageIntegritySurfaceActive(*selection.dynamic_args)) {
```

In `src/handlers/select.cc` replace:

```cpp
  if (!body || selection.mode != TableRewriteMode::Dynamic
      || !selection.dynamic_args->has_storage_integrity()
      || selection.dynamic_args->storage_integrity().tables().empty()) {
```

with:

```cpp
  if (!body || selection.mode != TableRewriteMode::Dynamic
      || !storageIntegritySurfaceActive(*selection.dynamic_args)) {
```

In `src/handlers/show_tables.cc` replace:

```cpp
bool storageIntegrityActive(const rewriter::RewriteTableDynamicArgs *dynamic) {
  return dynamic && dynamic->has_storage_integrity()
    && !dynamic->storage_integrity().tables().empty();
}
```

with:

```cpp
bool storageIntegrityActive(const rewriter::RewriteTableDynamicArgs *dynamic) {
  return dynamic && storageIntegritySurfaceActive(*dynamic);
}
```

In `src/handlers/writes.cc` replace:

```cpp
bool hasActiveStorageIntegrity(const TableRewriteSelection &sel) {
  return sel.mode == TableRewriteMode::Dynamic
    && sel.dynamic_args->has_storage_integrity()
    && !sel.dynamic_args->storage_integrity().tables().empty();
}
```

with:

```cpp
bool hasActiveStorageIntegrity(const TableRewriteSelection &sel) {
  return sel.mode == TableRewriteMode::Dynamic
    && storageIntegritySurfaceActive(*sel.dynamic_args);
}
```

In `src/rewriter-server.cc` replace:

```cpp
  // Positive SI v1 proof is selected by the same last-wins table-rewrite
  // option as the handlers. Validate the entire active contract before
  // publishing acknowledgement, then stamp it before preprocessing/parsing so
  // syntax errors and every handler rejection retain the proof.
  const auto si_selection =
    rewriter_handlers::findActiveTableRewrite(request->options());
  const bool si_active =
    si_selection.mode == rewriter_handlers::TableRewriteMode::Dynamic
    && si_selection.dynamic_args->has_storage_integrity()
    && !si_selection.dynamic_args->storage_integrity().tables().empty();
  if (si_active) {
    const auto &si = si_selection.dynamic_args->storage_integrity();
    std::string validation_error;
    if (si.contract_version()
          != rewriter::STORAGE_INTEGRITY_CONTRACT_V1) {
```

with:

```cpp
  // Positive SI proof (V1 or V2) is selected by the same last-wins
  // table-rewrite option as the handlers. V2 activates the surface by
  // version, V1 only with a non-empty table map. Validate the entire active
  // contract before publishing acknowledgement, then stamp it before
  // preprocessing/parsing so syntax errors and every handler rejection
  // retain the proof.
  const auto si_selection =
    rewriter_handlers::findActiveTableRewrite(request->options());
  const bool si_active =
    si_selection.mode == rewriter_handlers::TableRewriteMode::Dynamic
    && rewriter_handlers::storageIntegritySurfaceActive(*si_selection.dynamic_args);
  if (si_active) {
    const auto &si = si_selection.dynamic_args->storage_integrity();
    std::string validation_error;
    if (si.contract_version() != rewriter::STORAGE_INTEGRITY_CONTRACT_V1
        && si.contract_version() != rewriter::STORAGE_INTEGRITY_CONTRACT_V2) {
```

and replace:

```cpp
    response->set_storage_integrity_contract_version(
      rewriter::STORAGE_INTEGRITY_CONTRACT_V1);
  }
```

with:

```cpp
    response->set_storage_integrity_contract_version(si.contract_version());
  }
```

- [ ] **Step 4: Expected pass (build box)**

Expected after Step 3 in the batched CI run: the three new `StorageIntegrityContractV2` tests and `RejectsMissingAndUnknownBeforeAcknowledgement` pass; every pre-existing `StorageIntegrityContract.*`, `StorageIntegrityCatchAll.*` and `SpecG/StorageIntegrityGolden.*` test passes unchanged.

- [ ] **Step 5: Commit**

```bash
git add third_party/rewriter-proto src/handlers/storage_integrity.h src/handlers/storage_integrity.cc src/handlers/grant.cc src/handlers/select.cc src/handlers/show_tables.cc src/handlers/writes.cc src/rewriter-server.cc tests/rewriter_test.cc
git commit -m "feat(storage-integrity): accept contract V2 and activate the SI surface by version" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 7: rewriter-grpc — V2 DROP TABLE of SI tables

**Files:**
- Modify: `src/handlers/writes.cc:223-251` (plain-drop helper, parameter rename at 227 and 237), `:327-341` (preflight), `:794-797` (multi-table rewrite)
- Test: `tests/rewriter_test.cc` (four new `TEST`s after `StorageIntegrityContractV2.AcknowledgesV2OnEveryPath`)

**Interfaces:**
- Consumes: `storageIntegrityDropContract`, `hasActiveStorageIntegrity`, `AddV2Option` (Task 6); `rewriteOneTarget`, `recordAccessedTable`, `acceptWrite` (existing).
- Produces: `bool storageIntegrityPlainDrop(const DB::ASTDropQuery &drop, const TableRewriteSelection &sel)` (file-local in `writes.cc`).

- [ ] **Step 1: Write the failing tests**

In `tests/rewriter_test.cc`, directly after the closing `}` of `TEST(StorageIntegrityContractV2, AcknowledgesV2OnEveryPath)`, insert:

```cpp
TEST(StorageIntegrityContractV2, DropTableSucceeds) {
  rewriter::RewriteSQLRequest req;
  req.set_sql("DROP TABLE db1.t, other.u");
  AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V2, true);
  const auto resp = RunDirectRewrite(std::move(req));
  ASSERT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
  EXPECT_EQ(resp.statement_type(), rewriter::STATEMENT_TYPE_DROP_TABLE);
  EXPECT_EQ(resp.sql_after_rewrite(), "DROP TABLE phys.`db1.t`, phys.`other.u`");
  EXPECT_EQ(resp.storage_integrity_contract_version(),
            rewriter::STORAGE_INTEGRITY_CONTRACT_V2);
  const auto *si = FindAccessed(resp, "db1", "t");
  ASSERT_NE(si, nullptr);
  EXPECT_TRUE(si->is_storage_integrity());
  EXPECT_EQ(si->physical_database(), "phys");
  const auto *ordinary = FindAccessed(resp, "other", "u");
  ASSERT_NE(ordinary, nullptr);
  EXPECT_FALSE(ordinary->is_storage_integrity());
  EXPECT_EQ(resp.table_rewrites().at("db1.t"), "phys.db1.t");
  EXPECT_EQ(resp.table_rewrites().at("other.u"), "phys.other.u");
}

TEST(StorageIntegrityContractV2, DropExemptionRequiresPlainGrammar) {
  const std::string signed_lane =
    "storage-integrity table db1.t accepts writes only through the signed statement lane";
  struct Case {
    const char *sql;
    bool session_db1;
    std::string message;
  };
  const std::vector<Case> cases = {
    {"DROP TABLE db1.t ON CLUSTER c", false, signed_lane},
    {"DROP TABLE IF EMPTY db1.t", false, signed_lane},
    {"DROP TEMPORARY TABLE t", true, signed_lane},
    {"DROP VIEW db1.t", false, signed_lane},
    {"DROP TABLE other.u, other.v ON CLUSTER c", false,
      "multi-table DROP/TRUNCATE is not supported"},
  };
  for (const auto &c : cases) {
    SCOPED_TRACE(c.sql);
    rewriter::RewriteSQLRequest req;
    req.set_sql(c.sql);
    auto *dyn = AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V2, true);
    if (c.session_db1) dyn->set_upstream_logical_database_in_context("db1");
    const auto resp = RunDirectRewrite(std::move(req));
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement);
    EXPECT_EQ(resp.message(), c.message);
    EXPECT_EQ(resp.sql_after_rewrite(), c.sql);
    EXPECT_EQ(resp.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
    EXPECT_EQ(resp.storage_integrity_contract_version(),
              rewriter::STORAGE_INTEGRITY_CONTRACT_V2);
  }
}

TEST(StorageIntegrityContractV2, StaticSelectionShadowsDropExemption) {
  rewriter::RewriteSQLRequest req;
  req.set_sql("DROP TABLE db1.t, other.u");
  AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V2, true);
  AddEmptyStaticOption(req);
  const auto resp = RunDirectRewrite(std::move(req));
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement);
  EXPECT_EQ(resp.message(), "multi-table DROP/TRUNCATE is not supported");
  EXPECT_EQ(resp.storage_integrity_contract_version(),
            rewriter::STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED);
}

TEST(StorageIntegrityContractV2, DropRequiresAuthorizedLogicalDatabase) {
  rewriter::RewriteSQLRequest req;
  req.set_sql("DROP TABLE db1.t");
  auto *dyn = AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V2, true);
  dyn->mutable_database_map()->erase("db1");
  const auto resp = RunDirectRewrite(std::move(req));
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
  EXPECT_EQ(resp.message(),
            "storage-integrity logical database db1 is not authorized by database_map");
}

```

- [ ] **Step 2: Expected failure (build box)**

Expected before Step 3: `DropTableSucceeds` fails with `code() == UnsupportedStatement` and message `storage-integrity table db1.t accepts writes only through the signed statement lane`; the other three pass (they pin rejections that already hold).

- [ ] **Step 3: Implement**

In `src/handlers/writes.cc` replace:

```cpp
bool rejectStorageIntegrityWriteTarget(
  rewriter::RewriteSQLResponse *response,
  const TableRewriteSelection &sel,
  const std::string &database,
  const std::string &table,
  bool allow_authorized_insert) {
```

with:

```cpp
// Contract V2 accepts exactly `DROP TABLE [IF EXISTS] name[, name...] [SYNC]`
// for logical SI targets (rewriter-go engine.PlainDropTable). ClickHouse
// parses NO DELAY into the same `sync` flag, so it is accepted too; every
// other DROP modifier keeps the V1 rejection.
bool storageIntegrityPlainDrop(const DB::ASTDropQuery &drop,
                               const TableRewriteSelection &sel) {
  return sel.mode == TableRewriteMode::Dynamic
    && storageIntegrityDropContract(*sel.dynamic_args)
    && drop.kind == DB::ASTDropQuery::Kind::Drop
    && !drop.is_view && !drop.is_dictionary && !drop.isTemporary()
    && !drop.if_empty && !drop.has_all && !drop.has_tables && drop.like.empty()
    && drop.cluster.empty() && !drop.permanently
    && !drop.out_file && !drop.format_ast && !drop.settings_ast
    && (drop.table || drop.database_and_tables);
}

// allow_authorized_logical admits an authorized logical SI target: the
// signed-ingress INSERT target, or a V2 plain DROP TABLE target, which drops
// only the ordinary physical table.
bool rejectStorageIntegrityWriteTarget(
  rewriter::RewriteSQLResponse *response,
  const TableRewriteSelection &sel,
  const std::string &database,
  const std::string &table,
  bool allow_authorized_logical) {
```

and inside that function replace:

```cpp
    if (allow_authorized_insert) {
```

with:

```cpp
    if (allow_authorized_logical) {
```

In `preflightStorageIntegrityWrite` replace:

```cpp
  if (auto *drop = ast->as<DB::ASTDropQuery>()) {
    if (drop->database_and_tables) {
      for (const auto &single : drop->getRewrittenASTsOfSingleTable(ast)) {
        const auto *one = single->as<DB::ASTDropQuery>();
        if (one && rejectStorageIntegrityWriteTarget(
              response, sel, one->getDatabase(), one->getTable(), false)) {
          return true;
        }
      }
      return false;
    }
    if (!drop->table) {
      return rejectStorageIntegrityWriteDatabase(
        response, sel, drop->getDatabase());
    }
    return rejectStorageIntegrityWriteTarget(
      response, sel, drop->getDatabase(), drop->getTable(), false);
  }
```

with:

```cpp
  if (auto *drop = ast->as<DB::ASTDropQuery>()) {
    const bool plain_si_drop = storageIntegrityPlainDrop(*drop, sel);
    if (drop->database_and_tables) {
      for (const auto &single : drop->getRewrittenASTsOfSingleTable(ast)) {
        const auto *one = single->as<DB::ASTDropQuery>();
        if (one && rejectStorageIntegrityWriteTarget(
              response, sel, one->getDatabase(), one->getTable(), plain_si_drop)) {
          return true;
        }
      }
      return false;
    }
    if (!drop->table) {
      return rejectStorageIntegrityWriteDatabase(
        response, sel, drop->getDatabase());
    }
    return rejectStorageIntegrityWriteTarget(
      response, sel, drop->getDatabase(), drop->getTable(), plain_si_drop);
  }
```

In `handleWriteQuery` replace:

```cpp
    if (drop_query->database_and_tables) {
      rejectUnsupported(response, "multi-table DROP/TRUNCATE is not supported");
      return WriteDispatchResult::Rejected;
    }
```

with:

```cpp
    if (drop_query->database_and_tables) {
      // Contract V2 accepts a plain multi-table DROP TABLE whose targets may
      // mix storage-integrity and ordinary tables; everything else stays
      // rejected. Each target takes the ordinary single-target policy.
      if (!storageIntegrityPlainDrop(*drop_query, sel)) {
        rejectUnsupported(response, "multi-table DROP/TRUNCATE is not supported");
        return WriteDispatchResult::Rejected;
      }
      auto &list = drop_query->database_and_tables->as<DB::ASTExpressionList &>();
      for (auto &child : list.children) {
        auto *identifier = child->as<DB::ASTTableIdentifier>();
        if (!identifier) {
          rejectUnsupported(response, "multi-table DROP/TRUNCATE is not supported");
          return WriteDispatchResult::Rejected;
        }
        const auto table_id = identifier->getTableId();
        const std::string origin_db = table_id.database_name;
        const std::string origin_table = table_id.table_name;
        std::string new_db = origin_db;
        std::string new_table = origin_table;
        if (!rewriteOneTarget(response, sel, origin_db, origin_table, stmt_kind_str,
            [&](const std::string &s){ new_db = s; },
            [&](const std::string &s){ new_table = s; })) {
          return WriteDispatchResult::Rejected;
        }
        if (new_db != origin_db || new_table != origin_table)
          identifier->resetTable(new_db, new_table);
      }
      acceptWrite(ast, response, rewriter::STATEMENT_TYPE_DROP_TABLE);
      return WriteDispatchResult::Handled;
    }
```

(`ASTTableIdentifier` comes from the already-included `Parsers/ASTIdentifier.h`; `ASTExpressionList` from `Parsers/ASTExpressionList.h`. A single-target plain V2 DROP needs no dispatch change: preflight no longer rejects it, and the existing `rewriteOneTarget` path rewrites it.)

- [ ] **Step 4: Expected pass (build box)**

Expected: the four new tests pass; `WriteOp.DropTable_MultiTable_Rejected` (no SI option) still passes; every existing test is unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/handlers/writes.cc tests/rewriter_test.cc
git commit -m "feat(storage-integrity): accept plain DROP TABLE of SI tables under contract V2" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 8: rewriter-grpc — reserved databases join the protected namespace under V2

**Files:**
- Modify: `src/handlers/storage_integrity.cc` end of `validateStorageIntegrity` (`origin/main` 850-858), `isStorageIntegrityPhysicalDatabase` (875-886), `storageIntegrityPhysicalDatabases` (898-910), all shifted by Task 6's insertion above them; `src/handlers/storage_integrity.h` (the `validateStorageIntegrity` comment Task 6 wrote)
- Test: `tests/rewriter_test.cc` (four new `TEST`s after `TEST(StorageIntegrityContractV2, DropRequiresAuthorizedLogicalDatabase)`)

**Interfaces:**
- Consumes: `StorageIntegrityArgs::reserved_databases()` (Task 1); `AddV2Option`, `RunDirectRewrite` (Task 6 and existing).
- Produces: file-local `bool storageIntegrityReservedDatabase(const std::string &, const rewriter::RewriteTableDynamicArgs &)`; `isStorageIntegrityPhysicalDatabase` and `storageIntegrityPhysicalDatabases` return the V2 union; `validateStorageIntegrity` rejects a malformed V2 entry with `storage-integrity reserved_databases entry "<value>" must be a simple identifier`.

- [ ] **Step 1: Write the failing tests**

In `tests/rewriter_test.cc`, directly after the closing `}` of `TEST(StorageIntegrityContractV2, DropRequiresAuthorizedLogicalDatabase)`, insert:

```cpp
// ---- Storage-integrity contract V2: reserved_databases -----------------------
namespace {

// V1 learns its protected namespace from the table map: name every reserved
// database through a map entry.
rewriter::RewriteTableDynamicArgs *AddReservedV1Equivalent(rewriter::RewriteSQLRequest &req) {
  auto *dyn = AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V1, true);
  auto &promote = (*dyn->mutable_storage_integrity()->mutable_tables())["db1.p"];
  promote.set_safe_table("hg_promote.db1__p");
  promote.set_unsafe_table("hg_unsafe.db1__p");
  return dyn;
}

rewriter::RewriteTableDynamicArgs *AddReservedV2(rewriter::RewriteSQLRequest &req, bool with_table) {
  auto *dyn = AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V2, with_table);
  for (const char *database : {"hg_safe", "hg_unsafe", "hg_promote"})
    dyn->mutable_storage_integrity()->add_reserved_databases(database);
  return dyn;
}

const std::vector<std::string> kReservedNamespaceSQL = {
  "SELECT * FROM hg_safe.x",
  "SELECT * FROM hg_safe.db1__t",
  "INSERT INTO hg_unsafe.x VALUES (1)",
  "INSERT INTO hg_unsafe.x SELECT 1",
  "TRUNCATE TABLE hg_promote.x",
  "DROP TABLE hg_safe.x",
  "TRUNCATE DATABASE hg_safe",
  "SELECT * FROM merge($tag$hg_safe$tag$, 'x')",
  "USE hg_unsafe",
  "SHOW TABLES FROM hg_promote",
  "SYSTEM START MERGES hg_unsafe.x",
};

}  // namespace

TEST(StorageIntegrityReservedDatabases, V2EmptyMapMatchesV1Protection) {
  for (const auto &sql : kReservedNamespaceSQL) {
    SCOPED_TRACE(sql);
    rewriter::RewriteSQLRequest v1;
    v1.set_sql(sql);
    AddReservedV1Equivalent(v1);
    auto want = RunDirectRewrite(std::move(v1));
    rewriter::RewriteSQLRequest v2;
    v2.set_sql(sql);
    AddReservedV2(v2, false);
    const auto got = RunDirectRewrite(std::move(v2));
    EXPECT_NE(got.code(), rewriter::RewriteCode::Success) << got.sql_after_rewrite();
    EXPECT_EQ(got.storage_integrity_contract_version(), rewriter::STORAGE_INTEGRITY_CONTRACT_V2);
    want.set_storage_integrity_contract_version(rewriter::STORAGE_INTEGRITY_CONTRACT_V2);
    EXPECT_EQ(got.DebugString(), want.DebugString());
  }
}

TEST(StorageIntegrityReservedDatabases, UnionWithTableMap) {
  const std::vector<std::pair<std::string, std::string>> cases = {
    {"SELECT * FROM safe_x.y", "storage-integrity physical table safe_x.y is not directly addressable"},
    {"SELECT * FROM hg_safe.y", "storage-integrity physical table hg_safe.y is not directly addressable"},
  };
  for (const auto &[sql, message] : cases) {
    SCOPED_TRACE(sql);
    rewriter::RewriteSQLRequest req;
    req.set_sql(sql);
    auto *dyn = AddReservedV2(req, false);
    auto &table = (*dyn->mutable_storage_integrity()->mutable_tables())["db1.t"];
    table.set_safe_table("safe_x.db1__t");
    table.set_unsafe_table("unsafe_x.db1__t");
    const auto resp = RunDirectRewrite(std::move(req));
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::RewriteError);
    EXPECT_EQ(resp.message(), message);
  }
}

TEST(StorageIntegrityReservedDatabases, V1IgnoresReservedDatabases) {
  for (const bool with_table : {false, true}) {
    for (const auto &sql : kReservedNamespaceSQL) {
      SCOPED_TRACE(sql);
      rewriter::RewriteSQLRequest plain;
      plain.set_sql(sql);
      AddV2Option(plain, rewriter::STORAGE_INTEGRITY_CONTRACT_V1, with_table);
      rewriter::RewriteSQLRequest reserved;
      reserved.set_sql(sql);
      auto *dyn = AddV2Option(reserved, rewriter::STORAGE_INTEGRITY_CONTRACT_V1, with_table);
      for (const char *database : {"not a name", "hg_safe", "hg_unsafe", "hg_promote"})
        dyn->mutable_storage_integrity()->add_reserved_databases(database);
      EXPECT_EQ(RunDirectRewrite(std::move(reserved)).DebugString(),
                RunDirectRewrite(std::move(plain)).DebugString());
    }
  }
}

TEST(StorageIntegrityReservedDatabases, RejectsMalformedEntryUnderV2) {
  rewriter::RewriteSQLRequest req;
  req.set_sql("SELECT 1");
  auto *dyn = AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V2, false);
  dyn->mutable_storage_integrity()->add_reserved_databases("hg_safe");
  dyn->mutable_storage_integrity()->add_reserved_databases("hg-unsafe");
  const auto resp = RunDirectRewrite(std::move(req));
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
  EXPECT_EQ(resp.message(),
            "storage-integrity reserved_databases entry \"hg-unsafe\" must be a simple identifier");
  EXPECT_EQ(resp.storage_integrity_contract_version(),
            rewriter::STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED);
}

```

- [ ] **Step 2: Expected failure (build box)**

Expected before Step 3: `V2EmptyMapMatchesV1Protection` fails on every statement (for example `SELECT * FROM hg_safe.x` answers `Success`; `INSERT INTO hg_unsafe.x ...` answers `InvalidRewriteRequest` from the `database_map` check instead of the physical-table rejection), `UnionWithTableMap` fails on `SELECT * FROM hg_safe.y`, `RejectsMalformedEntryUnderV2` fails (`Success`, acknowledgement V2); `V1IgnoresReservedDatabases` passes.

- [ ] **Step 3: Implement**

In `src/handlers/storage_integrity.h` replace:

```cpp
// Validate every proof field before acknowledgement. A V1 request with an
// empty table map is legacy/no-op and is not activated by the caller; a V2
// request is activated by version (see storageIntegritySurfaceActive).
```

with:

```cpp
// Validate every proof field before acknowledgement. A V1 request with an
// empty table map is legacy/no-op and is not activated by the caller; a V2
// request is activated by version (see storageIntegritySurfaceActive). Under
// V2 every reserved_databases entry must be a simple identifier; V1 ignores
// that field entirely.
```

In `src/handlers/storage_integrity.cc`, at the end of `validateStorageIntegrity` replace:

```cpp
    if (!splitExactQualified(table.unsafe_table(), &ignored_db, &ignored_table)) {
      if (error)
        *error = "storage-integrity table " + key + " unsafe_table " + table.unsafe_table() +
                 " must have exact <database>.<table> shape";
      return false;
    }
  }
  return true;
}
```

with:

```cpp
    if (!splitExactQualified(table.unsafe_table(), &ignored_db, &ignored_table)) {
      if (error)
        *error = "storage-integrity table " + key + " unsafe_table " + table.unsafe_table() +
                 " must have exact <database>.<table> shape";
      return false;
    }
  }
  if (si.contract_version() == rewriter::STORAGE_INTEGRITY_CONTRACT_V2) {
    for (const auto &database : si.reserved_databases()) {
      if (!simpleIdentifier(database)) {
        if (error)
          *error = "storage-integrity reserved_databases entry \"" + database +
                   "\" must be a simple identifier";
        return false;
      }
    }
  }
  return true;
}
```

Replace:

```cpp
bool isStorageIntegrityPhysicalDatabase(
  const std::string &database, const rewriter::RewriteTableDynamicArgs &args) {
  if (database.empty() || !args.has_storage_integrity()) return false;
  for (const auto &[_, table] : args.storage_integrity().tables()) {
    for (const auto *value : {&table.safe_table(), &table.unsafe_table()}) {
      std::string physical_db, ignored;
      if (splitExactQualified(*value, &physical_db, &ignored) && physical_db == database)
        return true;
    }
  }
  return false;
}
```

with:

```cpp
// Contract V2 only: V1 ignores reserved_databases, so its protected namespace
// stays the databases derived from the table map, byte-for-byte. Mirrors
// rewriter-go's nameresolve.storageIntegrityReservedDatabase.
static bool storageIntegrityReservedDatabase(
  const std::string &database, const rewriter::RewriteTableDynamicArgs &args) {
  if (database.empty() || !args.has_storage_integrity()
      || args.storage_integrity().contract_version()
           != rewriter::STORAGE_INTEGRITY_CONTRACT_V2)
    return false;
  for (const auto &reserved : args.storage_integrity().reserved_databases())
    if (reserved == database) return true;
  return false;
}

bool isStorageIntegrityPhysicalDatabase(
  const std::string &database, const rewriter::RewriteTableDynamicArgs &args) {
  if (database.empty() || !args.has_storage_integrity()) return false;
  for (const auto &[_, table] : args.storage_integrity().tables()) {
    for (const auto *value : {&table.safe_table(), &table.unsafe_table()}) {
      std::string physical_db, ignored;
      if (splitExactQualified(*value, &physical_db, &ignored) && physical_db == database)
        return true;
    }
  }
  return storageIntegrityReservedDatabase(database, args);
}
```

and replace:

```cpp
  if (args.has_storage_integrity()) {
    for (const auto &[_, table] : args.storage_integrity().tables()) {
      for (const auto *value : {&table.safe_table(), &table.unsafe_table()}) {
        std::string database, ignored;
        if (splitExactQualified(*value, &database, &ignored)) databases.insert(database);
      }
    }
  }
  return {databases.begin(), databases.end()};
```

with:

```cpp
  if (args.has_storage_integrity()) {
    for (const auto &[_, table] : args.storage_integrity().tables()) {
      for (const auto *value : {&table.safe_table(), &table.unsafe_table()}) {
        std::string database, ignored;
        if (splitExactQualified(*value, &database, &ignored)) databases.insert(database);
      }
    }
    for (const auto &reserved : args.storage_integrity().reserved_databases())
      if (storageIntegrityReservedDatabase(reserved, args)) databases.insert(reserved);
  }
  return {databases.begin(), databases.end()};
```

No handler changes: `isStorageIntegrityPhysicalDatabase` is the single C++ authority for the protected namespace (37 call sites across the write preflight, SELECT walk, `resolveAccessedTable`, DESCRIBE/EXISTS, GRANT, USE/SHOW, database DDL, SYSTEM targets, table-function and heredoc carriers, `storageIntegrityExecutionDatabase`, and the reject annotator), and `storageIntegrityPhysicalDatabases` feeds the unresolved table-function classification (P11). Go's `%q` and this `"…"` quoting agree for every printable ASCII entry without quotes or backslashes; the shared message is only pinned for `hg-unsafe`.

- [ ] **Step 4: Expected pass (build box)**

Expected: the four `StorageIntegrityReservedDatabases` tests pass; every Task 6/7 test and every pre-existing test passes unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/handlers/storage_integrity.h src/handlers/storage_integrity.cc tests/rewriter_test.cc
git commit -m "feat(storage-integrity): protect V2 reserved_databases as physical namespaces" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 9: rewriter-grpc — shared corpus, loader, golden driver and gate counts

**Precondition:** Task 5 is merged in rewriter-go (gate R2).

**Files:**
- Modify: `tests/testdata/storage_integrity_cases.json` (copy of rewriter-go's)
- Modify: `tests/si_corpus.h:5` (rule range comment), `:48-50` (pins), `:61-85` (`Case`), `:89-97` (`CaseKeys`), `:108-113` (`StorageIntegrityKeys`), `:249-252` (`ValidateDynamicSchema`), `:299-300` (`LoadCases`), `:376` (R8)
- Modify: `tests/rewriter_test.cc:5788` (`kSIStmtByName`), `:5791`, `:5821`, `:5827` (`applyStorageIntegrityArgs`), `:5855`, `:5862-5869` (acknowledgement check); line numbers are `origin/main`'s, before Tasks 6–8 inserted tests above them
- Modify: `.github/scripts/snapshot-query-paired-ci.sh:138`, `.github/scripts/snapshot-query-ci.py:104`, `:107`, `:114`, `tests/testdata/snapshot_query_ci_pins.json:77` (`qualification.cpp_tests`)
- Modify: `CLAUDE.md:95`, `:185`, `AGENTS.md:95`, `:185`

**Interfaces:**
- Consumes: rewriter-go corpus at `RG_SHA`.
- Produces: `si_corpus::Case::contract_version`; corpus key `storage_integrity.reserved_databases` accepted and bound; `applyStorageIntegrityArgs(rewriter::RewriteTableDynamicArgs *, const Poco::JSON::Object::Ptr &, const std::string &contract_version)`; pins `kCorpusFingerprint = 7051648083520101593ULL`, `kCorpusBytes = 254170`, `kCorpusCases = 256`.

- [ ] **Step 1: Copy the corpus (the failing input)**

```bash
cp ../rewriter-go/internal/harness/testdata/storage_integrity_cases.json tests/testdata/storage_integrity_cases.json
cmp ../rewriter-go/internal/harness/testdata/storage_integrity_cases.json tests/testdata/storage_integrity_cases.json
shasum -a 256 tests/testdata/storage_integrity_cases.json   # 5fe367217619243729045820135340ae061891b4c2f85606cd61994482072615
```

(`../rewriter-go` is a checkout of `RG_SHA`.)

- [ ] **Step 2: Expected failure (build box)**

Expected with only the copy: `StorageIntegrityCorpus.SatisfiesTheFrozenContract` fails with 256 lines `R7: unknown key "contract_version"` and 6 lines `R7: unknown key "reserved_databases"`; `StorageIntegrityCorpus.IsBytePinnedToRewriterGo` fails (`254170` vs `232837`, fingerprint, `256` vs `237`); the paired gate's `SpecG/StorageIntegrityGolden.* 237` count check fails.

- [ ] **Step 3: Implement the loader, validator and driver**

In `tests/si_corpus.h` replace:

```cpp
constexpr uint64_t kCorpusFingerprint = 4366038644618079701ULL;
constexpr size_t kCorpusBytes = 232837;
constexpr size_t kCorpusCases = 237;
```

with:

```cpp
constexpr uint64_t kCorpusFingerprint = 7051648083520101593ULL;
constexpr size_t kCorpusBytes = 254170;
constexpr size_t kCorpusCases = 256;
```

replace:

```cpp
struct Case {
  std::string name;
  std::string sql;
```

with:

```cpp
struct Case {
  std::string name;
  std::string sql;
  // "V1" or "V2" (rule R8): the StorageIntegrityArgs.contract_version the
  // runners send for this case.
  std::string contract_version;
```

replace:

```cpp
    "name", "sql", "dynamic", "want_code", "want_stmt", "want_sql",
```

with:

```cpp
    "name", "sql", "contract_version", "dynamic", "want_code", "want_stmt", "want_sql",
```

replace:

```cpp
    c.sql = JsonString(object, "sql", case_path);
    RejectUnknownKeys(object, CaseKeys(), case_path, violations);
```

with:

```cpp
    c.sql = JsonString(object, "sql", case_path);
    c.contract_version = JsonString(object, "contract_version", case_path);
    RejectUnknownKeys(object, CaseKeys(), case_path, violations);
```

and replace:

```cpp
    if (IsBlank(c.sql)) add("R2", "sql must be non-empty");
```

with:

```cpp
    if (IsBlank(c.sql)) add("R2", "sql must be non-empty");
    if (c.contract_version != "V1" && c.contract_version != "V2") {
      add("R8", "contract_version must be \"V1\" or \"V2\", got \""
                + c.contract_version + "\"");
    }
```

In the header comment (line 5) replace `these rules and their ids (R1..R7)` with `these rules and their ids (R1..R8)`.

Replace:

```cpp
    "tables", "read_mode", "reserved_row_id_column",
```

(inside `StorageIntegrityKeys`) with:

```cpp
    "tables", "read_mode", "reserved_row_id_column", "reserved_databases",
```

and in `ValidateDynamicSchema` replace:

```cpp
  JsonString(storage_integrity, "reserved_row_id_column", si_path);
```

with:

```cpp
  JsonString(storage_integrity, "reserved_row_id_column", si_path);
  JsonStrings(storage_integrity, "reserved_databases", si_path);
```

In `tests/rewriter_test.cc` replace:

```cpp
  {"CREATE_TABLE", rewriter::STATEMENT_TYPE_CREATE_TABLE},
};
```

(inside `kSIStmtByName`) with:

```cpp
  {"CREATE_TABLE", rewriter::STATEMENT_TYPE_CREATE_TABLE},
  {"DROP_TABLE", rewriter::STATEMENT_TYPE_DROP_TABLE},
};
```

replace:

```cpp
void applyStorageIntegrityArgs(rewriter::RewriteTableDynamicArgs *dyn, const Poco::JSON::Object::Ptr &d) {
```

with:

```cpp
void applyStorageIntegrityArgs(rewriter::RewriteTableDynamicArgs *dyn, const Poco::JSON::Object::Ptr &d,
                               const std::string &contract_version) {
```

replace:

```cpp
  args->set_contract_version(rewriter::STORAGE_INTEGRITY_CONTRACT_V1);
  const std::string mode = si->has("read_mode") ? si->getValue<std::string>("read_mode") : "";
```

with:

```cpp
  args->set_contract_version(contract_version == "V2"
                               ? rewriter::STORAGE_INTEGRITY_CONTRACT_V2
                               : rewriter::STORAGE_INTEGRITY_CONTRACT_V1);
  const std::string mode = si->has("read_mode") ? si->getValue<std::string>("read_mode") : "";
```

replace:

```cpp
  if (si->has("reserved_row_id_column")) args->set_reserved_row_id_column(si->getValue<std::string>("reserved_row_id_column"));
```

with:

```cpp
  if (si->has("reserved_row_id_column")) args->set_reserved_row_id_column(si->getValue<std::string>("reserved_row_id_column"));
  for (const auto &database : si_corpus::JsonStrings(si, "reserved_databases"))
    args->add_reserved_databases(database);
```

replace:

```cpp
    applyStorageIntegrityArgs(opt->mutable_table_name_args()->mutable_dynamic_args(), c.dynamic);
```

with:

```cpp
    applyStorageIntegrityArgs(opt->mutable_table_name_args()->mutable_dynamic_args(), c.dynamic,
                              c.contract_version);
```

and replace:

```cpp
  if (c.has_dynamic && c.dynamic->has("storage_integrity") &&
      c.dynamic->getObject("storage_integrity")->getObject("tables")->size() != 0) {
    EXPECT_EQ(resp.storage_integrity_contract_version(),
              c.want_no_contract_ack ? rewriter::STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED
                                     : rewriter::STORAGE_INTEGRITY_CONTRACT_V1);
  } else {
    EXPECT_EQ(resp.storage_integrity_contract_version(), rewriter::STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED);
  }
```

with:

```cpp
  // The surface is active for any V2 case and for a V1 case with a non-empty
  // table map; an active case expects its own version acknowledged.
  const bool si_block = c.has_dynamic && c.dynamic->has("storage_integrity");
  const bool si_active = si_block && (c.contract_version == "V2"
    || c.dynamic->getObject("storage_integrity")->getObject("tables")->size() != 0);
  if (si_active && !c.want_no_contract_ack) {
    EXPECT_EQ(resp.storage_integrity_contract_version(),
              c.contract_version == "V2" ? rewriter::STORAGE_INTEGRITY_CONTRACT_V2
                                         : rewriter::STORAGE_INTEGRITY_CONTRACT_V1);
  } else {
    EXPECT_EQ(resp.storage_integrity_contract_version(), rewriter::STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED);
  }
```

Update the paired-gate counts (256 golden cases; 1251 = 1221 + 19 golden cases + 11 new `TEST`s from Tasks 6–8):

```bash
sed -i.bak 's/"SpecG\/StorageIntegrityGolden\.\* 237"/"SpecG\/StorageIntegrityGolden.* 256"/' .github/scripts/snapshot-query-paired-ci.sh
sed -i.bak -e 's/assert int(suite.attrib\["tests"\]) == 1221/assert int(suite.attrib["tests"]) == 1251/' \
           -e 's/assert len(cases) == 1221 and/assert len(cases) == 1251 and/' \
           -e 's/"cpp_tests":1221/"cpp_tests":1251/' .github/scripts/snapshot-query-ci.py
sed -i.bak 's/"cpp_tests": 1221,/"cpp_tests": 1251,/' tests/testdata/snapshot_query_ci_pins.json
rm .github/scripts/*.bak tests/testdata/snapshot_query_ci_pins.json.bak
grep -n '256\|1251' .github/scripts/snapshot-query-paired-ci.sh .github/scripts/snapshot-query-ci.py tests/testdata/snapshot_query_ci_pins.json
```

Expected grep: one hit in the shell script, three in the Python helper, one in the pins file.

Update the docs. In both `CLAUDE.md` and `AGENTS.md` (line 95) replace:

```markdown
An effective dynamic option with a non-empty `storage_integrity.tables` map must carry `STORAGE_INTEGRITY_CONTRACT_V1`; every table entry, logical key, read-mode enum, physical name, and reserved row-id identifier is validated before the response acknowledges v1. Once accepted, the acknowledgement is present on every parse and dispatch outcome. Shadowed SI options and empty table maps do not activate the contract.
```

with:

```markdown
An effective dynamic option activates the storage-integrity surface when it carries `STORAGE_INTEGRITY_CONTRACT_V2` (even with an empty `storage_integrity.tables` map) or has a non-empty `tables` map, and an active option must carry V1 or V2; every table entry, logical key, read-mode enum, physical name, and reserved row-id identifier is validated before the response acknowledges the received version. Once accepted, the acknowledgement is present on every parse and dispatch outcome. Shadowed SI options and V1 options with an empty table map do not activate the contract. V2 differs from V1 in that activation, in protecting every `storage_integrity.reserved_databases` entry exactly like a database named by a safe/unsafe table (V1 ignores the field), and in accepting a plain `DROP TABLE [IF EXISTS] name[, name...] [SYNC]` of logical SI tables, each of which drops only its ordinary physical table.
```

and replace (line 185) `(the positive v1 enforcement proof)` with `(the positive v1/v2 enforcement proof)`.

- [ ] **Step 4: Expected pass (build box)**

Expected: `StorageIntegrityCorpus.*` (2) pass; `SpecG/StorageIntegrityGolden.*` lists 256 cases and all pass, including the 19 new ones (the four DROP successes compared with the C++ pins after `NormalizeSIIdentifierQuotes`, where `DROP TABLE phys.\`db1.t\`` normalizes to `DROP TABLE phys."db1.t"`; the six `si_v2_reserved_*` rejections by code, message and accessed tables). If a DROP pin diverges only in formatting, set `allow_sql_divergence: true` with `want_sql_go` / `want_sql_cpp` for that case in rewriter-go (Task 5 file), recompute the three pins in both repos, and re-copy; do not weaken any structured assertion.

- [ ] **Step 5: Commit**

```bash
git add tests/testdata/storage_integrity_cases.json tests/si_corpus.h tests/rewriter_test.cc .github/scripts/snapshot-query-paired-ci.sh .github/scripts/snapshot-query-ci.py tests/testdata/snapshot_query_ci_pins.json CLAUDE.md AGENTS.md
git commit -m "test(storage-integrity): version the shared corpus and add the contract V2 cases" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 10: rewriter-grpc — re-pin the paired snapshot qualification (controller, build box)

**Precondition:** gate R2 is done (`RG_SHA` exists on rewriter-go `main`).

**Files:**
- Modify: `tests/testdata/snapshot_query_ci_pins.json` (`prerequisite_root`, `sources.rg`, `sources.rp`, `sources.polyglot`, `files.ffi.{bytes,sha256,cargo_lock_sha256,toolchain}`)
- Modify: `.github/scripts/snapshot-query-ci.py:39` (prerequisite root assertion)

**Interfaces:**
- Consumes: `PROTO_SHA`, `RG_SHA`, polyglot `d5fd24eec5efaa4444eed6e5f009044214ccdc84`.
- Produces: build-box bundle `/home/sentio/ci/snapshot-query-prerequisites/issue-153-v2`.

- [ ] **Step 1: Stage the prerequisite bundle on the build box**

On the build host (the controller or an operator with its SSH access), with the manifest's recorded toolchain (`rustc 1.97.1`, `cargo 1.97.1`, `x86_64-unknown-linux-gnu`):

```bash
old=/home/sentio/ci/snapshot-query-prerequisites/issue-153-v1
new=/home/sentio/ci/snapshot-query-prerequisites/issue-153-v2
mkdir -p "$new"
cp -p "$old/snapshot-query-profile" "$old/clickhouse" "$old/cctz-zoneinfo-complete.tar" "$new/"
work=$(mktemp -d)
git clone https://github.com/tobilg/polyglot "$work/polyglot" && git -C "$work/polyglot" checkout --detach d5fd24eec5efaa4444eed6e5f009044214ccdc84
(cd "$work/polyglot" && cargo build --locked -p polyglot-sql-ffi --profile ffi_release)
cp "$work/polyglot/target/ffi_release/libpolyglot_sql_ffi.so" "$new/libpolyglot_sql_ffi.so"
chmod -R a-w "$new"
stat -c %s "$new/libpolyglot_sql_ffi.so"; sha256sum "$new/libpolyglot_sql_ffi.so"
sha256sum "$work/polyglot/Cargo.lock"   # must print 1bbae26c48a0897f774a4edf5e3609d4467113c90c0312cda0314986e40f3f2b
rustc --version; cargo --version
```

- [ ] **Step 2: Re-pin the manifest**

In `tests/testdata/snapshot_query_ci_pins.json` set: `"prerequisite_root": "/home/sentio/ci/snapshot-query-prerequisites/issue-153-v2"`; `sources.rg` = `RG_SHA`; `sources.rp` = `PROTO_SHA`; `sources.polyglot` = `"d5fd24eec5efaa4444eed6e5f009044214ccdc84"`; `files.ffi.bytes` and `files.ffi.sha256` = the Step 1 `stat` and `sha256sum` outputs; `files.ffi.cargo_lock_sha256` = `"1bbae26c48a0897f774a4edf5e3609d4467113c90c0312cda0314986e40f3f2b"`; `files.ffi.toolchain` = the Step 1 `rustc`/`cargo` lines in the existing format. In `.github/scripts/snapshot-query-ci.py` replace `assert root == Path("/home/sentio/ci/snapshot-query-prerequisites/issue-153-v1")` with `assert root == Path("/home/sentio/ci/snapshot-query-prerequisites/issue-153-v2")`. The rewriter-go pin check `rg_rp_version == *-${rp_sha:0:12}` holds because `RG_SHA`'s `go.mod` names `PROTO_PSEUDO` (P9); `cmp` of the snapshot corpus and profile record holds because Tasks 2–5 do not touch them.

- [ ] **Step 3: Run the draft-PR CI (Tasks 6–10 red/green)**

```bash
git add tests/testdata/snapshot_query_ci_pins.json .github/scripts/snapshot-query-ci.py
git commit -m "ci: re-pin the paired snapshot qualification to rewriter-go with contract V2" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/si-contract-v2
gh pr create -R housegate/rewriter --draft --fill
gh pr checks --watch
```

Expected: `Remote build and test (build box)` green: ctest `100% tests passed, 0 tests failed out of 3`; `gtest.xml` 1251 tests, 0 failures; explicit counts `SpecG/StorageIntegrityGolden.* cases: 256`, `StorageIntegrityCorpus.* cases: 2`, `SINormalize.* cases: 11`. A red run is debugged against the expected failures listed in Tasks 6–9 Step 2; mark the PR ready only when green, and record the corpus SHA-256 and `cmp` evidence in the description.

---

## Task 11: Release and pin sequence (controller)

**Files:** none (git tags, GitHub releases, PR merges).

**Interfaces:**
- Produces: `PROTO_SHA`, `PROTO_PSEUDO`, tag rewriter-proto `v0.3.0`; `RG_SHA`, tag rewriter-go `v0.13.0` with FFI assets; tag rewriter-grpc `v0.15.0` and Docker image `0.15.0`.

- [ ] **Gate R1 — rewriter-proto (after Task 1)**

```bash
gh pr merge -R housegate/rewriter-proto --squash feat/si-contract-v2
cd rewriter-proto && git fetch origin && PROTO_SHA=$(git rev-parse origin/main) && echo $PROTO_SHA
git show $PROTO_SHA:proto/rewriter.proto | grep -n 'STORAGE_INTEGRITY_CONTRACT_V2 = 2'
cd ../rewriter-go && PROTO_PSEUDO=$(GOFLAGS=-mod=mod go list -m -f '{{.Version}}' github.com/housegate/rewriter-proto@$PROTO_SHA) && echo $PROTO_PSEUDO
```

`PROTO_PSEUDO` must look like `v0.2.1-0.20260924hhmmss-<first 12 of PROTO_SHA>`; compute it before the tag exists (P9). Then cut the tag:

```bash
gh workflow run release.yml -R housegate/rewriter-proto --ref main
gh run watch -R housegate/rewriter-proto $(gh run list -R housegate/rewriter-proto --workflow release.yml --limit 1 --json databaseId --jq '.[0].databaseId')
git -C ../rewriter-proto fetch --tags && git -C ../rewriter-proto rev-parse 'v0.3.0^{commit}'   # must equal PROTO_SHA
```

- [ ] **Gate R2 — rewriter-go (after Task 5)**

```bash
gh pr checks -R housegate/rewriter-go feat/si-contract-v2    # unit + ffi lanes green
gh pr merge -R housegate/rewriter-go --squash feat/si-contract-v2
git -C rewriter-go fetch origin && RG_SHA=$(git -C rewriter-go rev-parse origin/main) && echo $RG_SHA
git -C rewriter-go show $RG_SHA:go.mod | grep 'rewriter-proto'    # must print PROTO_PSEUDO
gh workflow run release.yml -R housegate/rewriter-go --ref main
gh run watch -R housegate/rewriter-go $(gh run list -R housegate/rewriter-go --workflow release.yml --limit 1 --json databaseId --jq '.[0].databaseId')
git -C rewriter-go fetch --tags && git -C rewriter-go rev-parse 'v0.13.0^{commit}'    # must equal RG_SHA
```

Verify the FFI assets (the release job builds and `make test-ordinary`-gates both platforms, then publishes `SHA256SUMS`):

```bash
gh release view v0.13.0 -R housegate/rewriter-go --json assets --jq '.assets[].name' | sort
# expect: SHA256SUMS, libpolyglot_sql_ffi-linux-x86_64.so, libpolyglot_sql_ffi-macos-arm64.dylib
dir=$(mktemp -d) && gh release download v0.13.0 -R housegate/rewriter-go -D "$dir"
(cd "$dir" && shasum -a 256 -c SHA256SUMS)      # both lines: OK
cd rewriter-go && git checkout --detach v0.13.0 && git submodule update --init third_party/polyglot-src
POLYGLOT_SQL_FFI_PATH="$dir/libpolyglot_sql_ffi-macos-arm64.dylib" SNAPSHOT_QUERY_ORDINARY=1 go test -count=1 ./...   # every package ok
```

- [ ] **Gate R3 — rewriter-grpc (after Task 10 Step 3 is green)**

```bash
gh pr ready -R housegate/rewriter feat/si-contract-v2
gh pr merge -R housegate/rewriter --squash feat/si-contract-v2
gh workflow run cut-release.yml -R housegate/rewriter -f bump=auto -f dry_run=true    # prints the next tag; expect v0.15.0
gh workflow run cut-release.yml -R housegate/rewriter -f bump=auto -f dry_run=false
gh run list -R housegate/rewriter --workflow release.yml --limit 1                     # the dispatched image build; wait for success
```

- [ ] **Gate R4 — hand over**

Record `PROTO_SHA`, `PROTO_PSEUDO`, `RG_SHA`, the three tags and the Docker image tag in plan B's tracking issue. Upgrading the production gRPC rewriter service to image `0.15.0` (spec §8.4 release step 3) is a single-service image bump in `sentioxyz/production` and precedes the housegate release; older housegates keep sending V1, which both new engines serve unchanged.

---

## Handoff to plan B

1. **Enum, field and import.** Go `pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2` (value 2) from `pb "github.com/housegate/rewriter-proto/gen/pb"`; housegate today aliases V1 at `pkg/rewriter/types.go:44` (`StorageIntegrityContractV1`), so plan B adds `StorageIntegrityContractV2` beside it. C++ `rewriter::STORAGE_INTEGRITY_CONTRACT_V2`. New field `StorageIntegrityArgs.reserved_databases`, proto `repeated string reserved_databases = 5;`, Go field `ReservedDatabases []string` (getter `GetReservedDatabases()`), C++ `add_reserved_databases()`. Housegate sets `ReservedDatabases: []string{"hg_safe", "hg_unsafe", "hg_promote"}` on every V2 request, including the probe's and one with an empty `Tables` map. The engines read it only under V2; each entry must be a simple identifier or the request is `InvalidRewriteRequest` `storage-integrity reserved_databases entry "<value>" must be a simple identifier`, unacknowledged.
2. **Pins.** rewriter-proto `PROTO_PSEUDO` (commit `PROTO_SHA`, also tagged `v0.3.0`; housegate pins proto by pseudo-version today, `go.mod:109`); rewriter-go `v0.13.0` (commit `RG_SHA`; FFI assets at `https://github.com/housegate/rewriter-go/releases/download/v0.13.0/` with `SHA256SUMS`, built from polyglot v0.12.1 `d5fd24e`); rewriter-grpc `v0.15.0`, Docker image `0.15.0`. The actual tags cut in Task 11 are authoritative.
3. **`DROP TABLE db1.t` under the probe arguments with V2** (`database_map {"db1": "phys"}`, `known_physical_databases ["phys"]`, `delim "_"`, table `db1.t` → `hg_safe.db1__t` / `hg_unsafe.db1__t`, `READ_MODE_SAFE`, `reserved_row_id_column "_hg_row_id"`, `reserved_databases ["hg_safe", "hg_unsafe", "hg_promote"]`, contract V2; measured with and without `reserved_databases`, identical): `code = Success`, `statement_type = STATEMENT_TYPE_DROP_TABLE`, `message = "success"`, `storage_integrity_contract_version = V2`, `table_rewrites = {"db1.t": "phys.db1.t"}`, `original_accessed_tables = [{original_database: "db1", original_table: "t", logical_database: "db1", physical_database: "phys", is_storage_integrity: true}]`. `sql_after_rewrite` differs by engine in identifier quoting only: native `DROP TABLE phys."db1.t"` (measured on the prototype), gRPC ``DROP TABLE phys.`db1.t` `` (the C++ formatter's backtick style, as in `TableRewrite.DropTable_Basic`; pinned by corpus case `si_v2_drop_table` after `NormalizeSIIdentifierQuotes` and by `StorageIntegrityContractV2.DropTableSucceeds`). Housegate's probe compares `sql_after_rewrite` exactly, so plan B must pin a per-engine expected string (or normalize identifier quotes) for this case.
4. **`SYSTEM RELOAD CONFIG` under an empty table map with V2** (the probe arguments with `tables` empty, `reserved_databases` set or not): `code = UnsupportedStatement`, `statement_type = STATEMENT_TYPE_UNSPECIFIED`, `message = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"`, `sql_after_rewrite = "SYSTEM RELOAD CONFIG"`, acknowledgement V2 (both engines; native measured). A V1-only build answers the same request `Success` with acknowledgement `UNSPECIFIED` (it ignores an empty map), and answers every V2 request that carries tables `InvalidRewriteRequest` / `storage-integrity contract version V1 is required` with acknowledgement `UNSPECIFIED`; the new builds answer an unknown version with `storage-integrity contract version V1 or V2 is required`.
5. **The five existing probe cases keep their exact outputs under V2 with `reserved_databases` set** (native measured): `DESCRIBE TABLE db1.t` → `StorageIntegrityProbeExpectedSQL`; `SYSTEM RELOAD CONFIG` (with the table) → the unmodelled message; `SYSTEM START MERGES hg_unsafe.db1__t` → `storage-integrity physical table hg_unsafe.db1__t is not directly addressable`; `TRUNCATE DATABASE hg_safe` → `storage-integrity physical database hg_safe is not directly addressable`; the tagged heredoc → `RewriteError` with the physical-table message. Only the acknowledgement changes to V2. The minimum builds for the V2 probe are rewriter-go `v0.13.0` and rewriter-grpc `v0.15.0`.
6. **New probe case: `SELECT * FROM hg_safe.db1__t` under an empty table map with V2 and `reserved_databases ["hg_safe", "hg_unsafe", "hg_promote"]`** (the probe arguments with `tables` empty): `code = RewriteError`, `statement_type = STATEMENT_TYPE_UNSPECIFIED`, `message = "storage-integrity physical table hg_safe.db1__t is not directly addressable"`, `sql_after_rewrite = "SELECT * FROM hg_safe.db1__t"`, acknowledgement V2, `original_accessed_tables = [{original_database: "hg_safe", original_table: "db1__t", physical_database: "hg_safe", is_storage_integrity: true}]` (native measured; the gRPC engine pins the same code, message and accessed table through the shared corpus, e.g. `si_v2_reserved_select_rejected`). A build without `reserved_databases` answers `Success` pass-through, so this case discriminates the new builds. The same request with V1 is a legacy pass-through (V1 ignores the field).
7. **Pre-existing, unchanged:** the native engine drops `ON CLUSTER` from an ordinary (non-SI) `DROP TABLE x ON CLUSTER c` (polyglot's `drop_table` has no cluster field); V2 refuses the SI form, but ordinary tables keep today's behaviour.
