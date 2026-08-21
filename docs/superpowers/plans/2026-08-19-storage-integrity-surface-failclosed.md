# Storage-Integrity Surface Fail-Closed Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the two Critical storage-integrity (SI) read-surface holes — `SYSTEM START MERGES hg_unsafe.<t>` passing through as `Success`, and `TRUNCATE DATABASE hg_safe` being forwarded verbatim after a target-less engine rejection — by making both rewriter engines fail closed on every statement class they do not model whenever SI is configured, making HouseGate fail closed on every non-`Success` response in that configuration, and fixing the five correctness defects (literal escaping, reserved-column scoping, CTE scoping, `PREWHERE`, C++ literal-mangling) plus the peer-trust bypass decision record.

**Architecture:** Three layers, each independently safe. (1) The engines gain a *catch-all*: when the request carries a non-empty `storage_integrity.tables`, any execution path that reaches the unmodelled-statement pass-through returns `UnsupportedStatement` instead of `Success` (D1), and one shared annotator upgrades any non-`Success` message to name the SI table or reserved namespace the statement touched (D2). (2) HouseGate treats **any** non-`Success` response as a `*RejectedError` when SI tables are configured (D3), so an old or regressed engine cannot re-open the hole, and proves the engine build at startup with one fixed DESCRIBE probe (D5). (3) The remaining defects are point fixes with corpus coverage (D4, D7a–e) and the peer-trust bypass becomes a recorded, warned-about, tested decision (D6). The two engines stay behaviourally identical through one byte-identical golden corpus.

**Tech Stack:** Go 1.25 + polyglot FFI via PureGo (rewriter-go), C++23 + ClickHouse parser + gtest built on the remote box (rewriter-grpc), Go + Bazel 9 (housegate), ClickHouse 25.8 in docker for the regression test.

**Spec:** `docs/superpowers/specs/2026-08-19-storage-integrity-surface-failclosed-design.md` (Spec I). Roadmap: `docs/superpowers/specs/2026-08-19-storage-integrity-remediation-roadmap.md`. Remediates: `docs/superpowers/specs/2026-08-18-storage-integrity-read-surface-rewrite-design.md` (Spec G) and its plan `docs/superpowers/plans/2026-08-18-storage-integrity-read-surface-rewrite.md` — this plan changes that plan's deviations **D-1** (an SI-marked INSERT is still not rejected by the rewriter; unchanged here, but its sibling rule "only SI-flagged rejects fail closed" is replaced) and **D-2** (fail-closed keyed on the SI accessed-table flag → fail-closed keyed on *any* non-`Success` while SI tables are configured).

## Global Constraints

Copied verbatim from Spec I and the frozen Spec G contract. Every task's requirements implicitly include this section.

- **D1 generic message (exact string):** `storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded`.
- **Existing SI message vocabulary — reuse, never invent** (`rewriter-go/internal/nameresolve/resolve.go:423-445`, `rewriter-grpc/src/handlers/storage_integrity.h:47-51`):
  - `storage-integrity table <logical-key> accepts writes only through the signed statement lane`
  - `storage-integrity physical table <db>.<table> is not directly addressable`
  - `storage-integrity physical database <db> is not directly addressable`
  - `storage-integrity logical database <db> is not authorized by database_map`
  - `reserved column <rid> is not addressable`
- **Reserved row-id column** is `_hg_row_id` (rewriter default when `reserved_row_id_column` is empty). Physical naming is frozen: `hg_safe.<CHTableName(id)>` / `hg_unsafe.<CHTableName(id)>`, `CHTableName(id) = ReplaceAll(id, ".", "__")`.
- **Contract acknowledgement is unchanged.** SI requests carry `StorageIntegrityArgs.contract_version = STORAGE_INTEGRITY_CONTRACT_V1`; every response path echoes `RewriteSQLResponse.storage_integrity_contract_version = V1` once accepted. Nothing in this plan bumps rewriter-proto — no proto change is needed.
- **One corpus, byte-identical in two repos:** `rewriter-go/internal/harness/testdata/storage_integrity_cases.json` and `rewriter-grpc/tests/testdata/storage_integrity_cases.json`. Pre-change state: 178 cases, sha256 `cb37d657ebedfb04f0308b136f257636833bd4edefb4675b2f2c83147537cf9f`. The Go copy is authoritative during authoring; the C++ copy is produced by `cp`, never hand-edited (Spec G plan D-10).
- **rewriter-go:** polyglot imports only inside `internal/engine`; `internal/nameresolve` imports neither engine nor polyglot; engine-backed tests skip unless `POLYGLOT_SQL_FFI_PATH` is set (`make ffi` builds it at `third_party/lib/libpolyglot_sql_ffi.<dylib|so>`; `make test` exports it automatically).
- **rewriter-grpc builds only on the remote box** (`ssh -p 30100 sentio@64.38.131.242`, workdir `/home/sentio/chen/rewriter-grpc/`). Dev loop = rsync → `./scripts.sh rebuild` → `ctest --test-dir build --output-on-failure`. Single test: `./build/rewriter_tests --gtest_filter='<Suite.Name>'`. **Never run a local cmake.**
- **housegate:** Bazel is the test ground truth (`bazel build //...`, `bazel test //...`); module path `github.com/housegate/housegate`; run `bazel mod tidy && bazel run //:gazelle` after dependency or new-file changes; `pkg/integration` targets are `manual`-tagged and must be listed in `.github/workflows/ci.yml`.
- **English only** for identifiers, comments, log messages, and operator-facing error strings, in all three repos.
- **Commit per task** with the repo's conventional prefixes; every task ends with its named command green.

## Dependency on Spec J (corpus contract)

Spec I §4 says the corpus additions and Spec J's runner change "must land together or J first". This plan is written so that **I is correct and verifiable on its own**, and gets strictly stronger once J lands:

- Every new case is authored against the **current** schema keys (`name`, `sql`, `dynamic`, `want_code`, `want_stmt`, `want_sql`, `want_sql_contains`, `want_sql_not_contains`, `want_message_contains`, `want_table_rewrites`, `want_accessed`, `reject`, `allow_sql_divergence`, `want_no_contract_ack`). No new key is introduced.
- **No new case sets `sql_exact`** — Spec J D3 deletes that concept. Go compares `want_sql` semantically today; C++ compares it once J lands.
- Every new `want_sql_contains` entry is chosen so it is **not already a substring of the case's input SQL** (Spec J D3's vacuity rule), so no new case can pass vacuously in either runner.
- Reject cases (the majority here: D1, D2, D7c) are **fully verified in C++ today** — the existing C++ runner compares `want_code`, `want_stmt`, `want_message_contains`, and the reject SQL echo (`tests/rewriter_test.cc:4671-4686`). They need nothing from J.
- Success cases (D4, D7a, D7b, D7d) are verified in C++ today only through `want_sql_contains` / `want_sql_not_contains`. Their `want_sql` becomes load-bearing in C++ **only after Spec J D3** lands. Each such task says so explicitly in its steps.
- Cases that set `allow_sql_divergence` will need Spec J D3's per-engine `want_sql_go` / `want_sql_cpp`. **That edit belongs to Spec J's plan, not this one** — do not invent those keys here.

## File map

| Repo | Create | Modify |
|---|---|---|
| rewriter-go | `internal/engine/scope.go` (+`scope_test.go`), `internal/engine/lexical.go` (+`lexical_test.go`), `internal/handlers/storage_integrity_reject.go` (+`storage_integrity_reject_test.go`) | `native.go` (+`native_test.go`), `internal/handlers/select.go`, `internal/handlers/dblevel.go`, `internal/handlers/storage_integrity.go`, `internal/engine/nodes.go`, `internal/harness/testdata/storage_integrity_cases.json` |
| rewriter-grpc | — | `src/rewriter-server.cc`, `src/handlers/storage_integrity.h`, `src/handlers/storage_integrity.cc`, `src/handlers/select.cc`, `src/handlers/common.h`, `tests/rewriter_test.cc`, `tests/testdata/storage_integrity_cases.json` |
| housegate | `pkg/rewriter/probe.go` (+`probe_test.go`) | `pkg/rewriter/sentio.go`, `pkg/rewriter/storage_integrity.go`, `pkg/rewriter/backend_test.go`, `pkg/rewriter/storage_integrity_test.go`, `pkg/plugins/rewrite/rewriter.go` (+`rewriter_test.go`), `build.go`, `build_test.go`, `go.mod`, `go.sum`, `configs/local.server.yaml`, `configs/local.server-mock-remote.yaml`, `CLAUDE.md`, `pkg/integration/storage_integrity_read_test.go` |
| docs (housegate) | — | `docs/superpowers/specs/2026-06-22-storage-integrity-design.md`, `docs/superpowers/specs/2026-08-19-storage-integrity-surface-failclosed-design.md`, `docs/superpowers/specs/2026-08-19-storage-integrity-remediation-roadmap.md` |

---

## Part A — rewriter-go (native engine + shared corpus authoring)

**Working directory for every Part A task:** `/Users/uranuswch/Dev/housegate/rewriter-go`

> **Oracle warning for all of Part A:** `TestStorageIntegrityGolden` diffs against the C++ oracle when `REWRITER_ORACLE_ADDR` is set (`internal/harness/storage_integrity_golden_test.go:150,211`). The C++ side is not fixed until Part B, so **unset `REWRITER_ORACLE_ADDR`** for every Part A test run. Task 14 re-enables it.

- [ ] **Task 0 (pre-flight, do once):** create the branch and prove the baseline is green.

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git checkout -b feat/si-surface-failclosed
env -u REWRITER_ORACLE_ADDR make test
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json
```

Expected: every package `ok`; sha256 = `cb37d657ebedfb04f0308b136f257636833bd4edefb4675b2f2c83147537cf9f`.

### Task 1: D1 — the catch-all fails closed when storage-integrity is active

This is the safety property of the whole spec. It lands first and alone.

**Files:**
- Modify: `native.go` (`doRewrite` pass-through tail at `:188-195`; `finalize` at `:58-68`)
- Test: `native_test.go`
- Modify: `internal/harness/testdata/storage_integrity_cases.json` (2 new cases at the end of the array)

**Interfaces:**
- Produces: `const StorageIntegrityUnmodelledMessage = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"` exported from package `rewriter` (root package, `native.go`). Task 2 and housegate Task 21 both quote this string.

- [ ] **Step 1: Add the two corpus cases (red)**

Append these two objects to the end of the JSON array in `internal/harness/testdata/storage_integrity_cases.json` (insert before the closing `]`, and add a comma after the previous last object):

```json
  {
    "name": "si_unmodelled_statement_rejected",
    "sql": "SYSTEM RELOAD CONFIG",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"
  },
  {
    "name": "si_unmodelled_statement_on_ordinary_table_rejected",
    "sql": "CHECK TABLE other.u",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"
  }
```

The second case is the point of D1: an unmodelled class is refused **even when it names no SI object**, because the engine cannot prove it is harmless.

- [ ] **Step 2: Add the engine-local pass-through test (red)**

The "SI absent → still passes through" half cannot live in the shared corpus: with no SI args the Go engine classifies `SYSTEM RELOAD CONFIG` as `STATEMENT_TYPE_UNSPECIFIED` while the C++ tail stamps `STATEMENT_TYPE_SELECT`, a pre-existing divergence this spec does not change. Assert it per engine instead. Append to `native_test.go`:

```go
func TestDoRewrite_UnmodelledStatementPassesThroughWithoutStorageIntegrity(t *testing.T) {
	e := newEngine(t)
	opts := []*pb.RewriteOption{{Op: pb.RewriteOp_TableNameRewrite,
		Value: &pb.RewriteOption_TableNameArgs{TableNameArgs: &pb.RewriteTableNameArgs{
			DynamicArgs: &pb.RewriteTableDynamicArgs{
				DatabaseMap:            map[string]string{"db1": "phys"},
				KnownPhysicalDatabases: []string{"phys"},
				Delim:                  "_",
			}}}}}
	resp, err := doRewrite(e, "SYSTEM RELOAD CONFIG", opts)
	if err != nil {
		t.Fatalf("doRewrite: %v", err)
	}
	if resp.GetCode() != pb.RewriteCode_Success {
		t.Fatalf("code = %v (%s), want Success — empty-SI requests keep the legacy pass-through",
			resp.GetCode(), resp.GetMessage())
	}
	if resp.GetStorageIntegrityContractVersion() != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED {
		t.Fatalf("contract ack = %v, want UNSPECIFIED", resp.GetStorageIntegrityContractVersion())
	}
}

func TestDoRewrite_UnmodelledStatementFailsClosedWithStorageIntegrity(t *testing.T) {
	e := newEngine(t)
	opts := []*pb.RewriteOption{{Op: pb.RewriteOp_TableNameRewrite,
		Value: &pb.RewriteOption_TableNameArgs{TableNameArgs: &pb.RewriteTableNameArgs{
			DynamicArgs: &pb.RewriteTableDynamicArgs{
				DatabaseMap:            map[string]string{"db1": "phys"},
				KnownPhysicalDatabases: []string{"phys"},
				Delim:                  "_",
				StorageIntegrity: &pb.StorageIntegrityArgs{
					Tables: map[string]*pb.StorageIntegrityArgs_Table{
						"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"}},
					ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
					ReservedRowIdColumn: "_hg_row_id",
					ContractVersion:     pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1,
				},
			}}}}}
	resp, err := doRewrite(e, "SYSTEM RELOAD CONFIG", opts)
	if err != nil {
		t.Fatalf("doRewrite: %v", err)
	}
	if resp.GetCode() != pb.RewriteCode_UnsupportedStatement {
		t.Fatalf("code = %v, want UnsupportedStatement", resp.GetCode())
	}
	if resp.GetMessage() != StorageIntegrityUnmodelledMessage {
		t.Fatalf("message = %q, want %q", resp.GetMessage(), StorageIntegrityUnmodelledMessage)
	}
	if resp.GetSqlAfterRewrite() != "SYSTEM RELOAD CONFIG" {
		t.Fatalf("reject must echo the original SQL, got %q", resp.GetSqlAfterRewrite())
	}
	if resp.GetStatementType() != pb.StatementType_STATEMENT_TYPE_UNSPECIFIED {
		t.Fatalf("statement_type = %v, want UNSPECIFIED on a reject", resp.GetStatementType())
	}
	if resp.GetStorageIntegrityContractVersion() != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1 {
		t.Fatalf("contract ack = %v, want V1 on every SI response path", resp.GetStorageIntegrityContractVersion())
	}
}
```

`newEngine(t)` is the existing helper at `native_test.go:32`: it builds a polyglot engine, skips when `POLYGLOT_SQL_FFI_PATH` is unset, and closes on cleanup. Do **not** add a second engine constructor. (Package-local names differ: `internal/handlers` also calls it `newEngine` (`select_test.go:11`), while `internal/engine` calls it `newTestEngine` (`polyglot_test.go:9`).)

- [ ] **Step 3: Run both tests to verify they fail**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make ffi >/dev/null && \
  POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./... -run 'TestDoRewrite_Unmodelled|TestStorageIntegrityGolden/si_unmodelled' -v
```

Expected: `TestDoRewrite_UnmodelledStatementPassesThroughWithoutStorageIntegrity` PASSES (today's behaviour), `TestDoRewrite_UnmodelledStatementFailsClosedWithStorageIntegrity` FAILS with `code = Success, want UnsupportedStatement`, and both `si_unmodelled_*` golden cases FAIL with `code = Success`. On Linux use `.so` instead of `.dylib`.

- [ ] **Step 4: Implement the catch-all**

In `native.go`, add the exported constant just above `doRewrite`:

```go
// StorageIntegrityUnmodelledMessage is returned when a request carries a
// non-empty storage_integrity.tables map and execution reaches the
// unmodelled-statement pass-through. The rewriter cannot prove such a
// statement is harmless to the protocol-owned namespaces, so it refuses to
// forward it (Spec I D1). Enumerated classes replace this text with a more
// specific one; see handlers.AnnotateStorageIntegrityReject.
const StorageIntegrityUnmodelledMessage = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"
```

Replace the pass-through tail (`native.go:188-195`) with:

```go
	// Pass-through: regenerate (proves the engine round-trips); fall back to
	// the input on any generate hiccup so SQL is always runnable. With an
	// active storage-integrity contract this branch is a refusal instead:
	// reaching it means no handler modelled the statement, so no handler
	// checked it against the protocol-owned namespaces (Spec I D1).
	if siVersion == pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1 {
		resp.Code = pb.RewriteCode_UnsupportedStatement
		resp.Message = StorageIntegrityUnmodelledMessage
		finalize(resp, sql, ec, siVersion)
		return resp, nil
	}
	if gen, gerr := e.Generate(ast); gerr == nil && gen != "" {
		resp.SqlAfterRewrite = gen
	}
	resp.Code = pb.RewriteCode_Success
	finalize(resp, sql, ec, siVersion)
	return resp, nil
```

`finalize` already clears `statement_type` and echoes the original SQL on a non-`Success` code (`native.go:61-68`), which is what the corpus reject assertions expect.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test
```

Expected: every package `ok`, including all 180 golden cases. If a *pre-existing* case flipped to `UnsupportedStatement`, that case was riding the catch-all — record its name in the commit message and re-check it against the spec before changing its expectation.

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git add native.go native_test.go internal/harness/testdata/storage_integrity_cases.json
git commit -m "fix(storage-integrity): fail closed on the unmodelled-statement catch-all (Spec I D1)"
```

### Task 2: D2 — name the SI object an unmodelled or rejected statement touched

**Files:**
- Create: `internal/engine/lexical.go`, `internal/engine/lexical_test.go`
- Create: `internal/handlers/storage_integrity_reject.go`, `internal/handlers/storage_integrity_reject_test.go`
- Modify: `native.go` (`finalize` signature + its 7 call sites in `doRewrite`)
- Modify: `internal/harness/testdata/storage_integrity_cases.json` (10 new cases)

**Interfaces:**
- Consumes: `StorageIntegrityUnmodelledMessage` (Task 1).
- Produces: `func engine.NameRefs(e Engine, sql string) ([]TableTarget, error)` — every `[db.]table` name-run in the raw token stream, in source order, deduped, string literals and comments excluded. `func handlers.AnnotateStorageIntegrityReject(e engine.Engine, resp *pb.RewriteSQLResponse, sql string, sel nameresolve.Selection)` — upgrades the message of an already-rejected response to name the first SI object the statement mentions; no-op on `Success`, on a response that already carries an SI-flagged accessed table, and when nothing SI is named.

- [ ] **Step 1: Add the ten corpus cases (red)**

Append to `internal/harness/testdata/storage_integrity_cases.json`. All ten share this `dynamic` block — write it out in full for each case (the file has no anchors):

```json
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
```

The ten cases (`<DYN>` below stands for exactly that block):

```json
  {
    "name": "si_system_start_merges_unsafe_rejected",
    "sql": "SYSTEM START MERGES hg_unsafe.db1__t",
    <DYN>
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity physical table hg_unsafe.db1__t is not directly addressable"
  },
  {
    "name": "si_system_stop_merges_safe_rejected",
    "sql": "SYSTEM STOP MERGES hg_safe.db1__t",
    <DYN>
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity physical table hg_safe.db1__t is not directly addressable"
  },
  {
    "name": "si_system_restart_replica_unsafe_rejected",
    "sql": "SYSTEM RESTART REPLICA hg_unsafe.db1__t",
    <DYN>
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity physical table hg_unsafe.db1__t is not directly addressable"
  },
  {
    "name": "si_system_sync_replica_logical_rejected",
    "sql": "SYSTEM SYNC REPLICA db1.t",
    <DYN>
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity table db1.t accepts writes only through the signed statement lane"
  },
  {
    "name": "si_check_table_logical_rejected",
    "sql": "CHECK TABLE db1.t",
    <DYN>
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity table db1.t accepts writes only through the signed statement lane"
  },
  {
    "name": "si_truncate_database_safe_rejected",
    "sql": "TRUNCATE DATABASE hg_safe",
    <DYN>
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity physical database hg_safe is not directly addressable"
  },
  {
    "name": "si_truncate_all_tables_from_unsafe_rejected",
    "sql": "TRUNCATE ALL TABLES FROM hg_unsafe",
    <DYN>
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity physical database hg_unsafe is not directly addressable"
  },
  {
    "name": "si_alter_database_safe_rejected",
    "sql": "ALTER DATABASE hg_safe MODIFY COMMENT 'x'",
    <DYN>
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity physical database hg_safe is not directly addressable"
  },
  {
    "name": "si_drop_dictionary_safe_rejected",
    "sql": "DROP DICTIONARY hg_safe.d",
    <DYN>
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity physical table hg_safe.d is not directly addressable"
  },
  {
    "name": "si_create_live_view_over_si_rejected",
    "sql": "CREATE LIVE VIEW other.v AS SELECT * FROM db1.t",
    <DYN>
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity table db1.t accepts writes only through the signed statement lane"
  }
```

No `want_accessed` on these cases. The annotator deliberately records **no** accessed-table entry: HouseGate's D3 gate (Task 16) keys on the response code, not the SI flag, and inventing a new shared accessed-shape for protocol-owned names would be an unverifiable Go/C++ divergence risk. This is a deliberate narrowing of Spec I D2, which only specifies the *messages*.

- [ ] **Step 2: Run the corpus to verify the ten cases fail**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test 2>&1 | grep -E 'si_(system|check_table_logical|truncate|alter_database|drop_dictionary|create_live_view)'
```

Expected: every one of the ten `--- FAIL`. `si_system_*` and `si_check_table_logical_rejected` fail on the *message* (Task 1 gave them the generic text); the `TRUNCATE`/`ALTER DATABASE`/`DROP DICTIONARY`/`CREATE LIVE VIEW` cases fail on the message too (their handlers already reject with a generic sentence).

- [ ] **Step 3: Implement `engine.NameRefs`**

Create `internal/engine/lexical.go`:

```go
package engine

// NameRefs returns every `[db.]table` name-run in the raw token stream, in
// source order, deduplicated. It exists for statements whose AST is an opaque
// `command` node (SYSTEM, CHECK TABLE, ...) where no structured target is
// available: policy still has to see which names the text addresses.
//
// String literals and comments cannot contribute, because only VAR and
// QUOTED_IDENTIFIER tokens are considered name-run elements (isNameTok).
func NameRefs(e Engine, sql string) ([]TableTarget, error) {
	toks, err := tokenizeRaw(e, sql)
	if err != nil {
		return nil, err
	}
	var out []TableTarget
	seen := map[TableTarget]bool{}
	for i := 0; i < len(toks); i++ {
		if !isNameTok(toks[i].TokenType) {
			continue
		}
		target, ok := rawTokenTableTarget(toks, i)
		if !ok {
			continue
		}
		if target.DB != "" {
			i += 2 // consume `db . table`
		}
		if seen[target] {
			continue
		}
		seen[target] = true
		out = append(out, target)
	}
	return out, nil
}
```

Create `internal/engine/lexical_test.go`:

```go
package engine

import "testing"

func TestNameRefs_QualifiedBareAndLiterals(t *testing.T) {
	e := newTestEngine(t)
	got, err := NameRefs(e, "SYSTEM START MERGES hg_unsafe.db1__t")
	if err != nil {
		t.Fatalf("NameRefs: %v", err)
	}
	if len(got) == 0 || got[len(got)-1] != (TableTarget{DB: "hg_unsafe", Table: "db1__t"}) {
		t.Fatalf("got %+v, want the trailing qualified name hg_unsafe.db1__t", got)
	}

	lit, err := NameRefs(e, "SELECT 'hg_unsafe.db1__t' FROM other.u")
	if err != nil {
		t.Fatalf("NameRefs: %v", err)
	}
	for _, target := range lit {
		if target.DB == "hg_unsafe" {
			t.Fatalf("a string literal must not produce a name ref: %+v", lit)
		}
	}
}
```

`newTestEngine(t)` is the existing engine-package test helper (used by the other `internal/engine` tests); reuse it rather than adding another.

- [ ] **Step 4: Implement the annotator**

Create `internal/handlers/storage_integrity_reject.go`:

```go
package handlers

import (
	"github.com/housegate/rewriter-go/internal/engine"
	"github.com/housegate/rewriter-go/internal/nameresolve"
	"github.com/housegate/rewriter-proto/gen/pb"
)

// AnnotateStorageIntegrityReject upgrades the message of an already-rejected
// response so it names the storage-integrity object the statement addressed
// (Spec I D2). It runs for every non-Success response while the v1 contract is
// active, which is what gives SYSTEM / CHECK / TRUNCATE DATABASE / ALTER
// DATABASE / DROP DICTIONARY / CREATE LIVE VIEW a useful message without
// teaching each handler about storage integrity.
//
// It is deliberately message-only:
//   - Success responses are never touched.
//   - A response that already carries an SI-flagged accessed table went through
//     real SI policy and owns its own (more precise) message.
//   - When the statement names nothing storage-integrity, the caller's message
//     stands (for the catch-all that is the generic D1 text).
func AnnotateStorageIntegrityReject(e engine.Engine, resp *pb.RewriteSQLResponse, sql string, sel nameresolve.Selection) {
	if resp.GetCode() == pb.RewriteCode_Success {
		return
	}
	if sel.Mode != nameresolve.ModeDynamic || len(sel.Dynamic.GetStorageIntegrity().GetTables()) == 0 {
		return
	}
	if touchesStorageIntegrity(resp.GetOriginalAccessedTables()) {
		return
	}
	refs, err := engine.NameRefs(e, sql)
	if err != nil {
		return // tokenization failure leaves the caller's message intact
	}
	for _, ref := range refs {
		if ref.DB != "" {
			if nameresolve.IsStorageIntegrityPhysicalDatabase(ref.DB, sel.Dynamic) {
				resp.Message = nameresolve.StorageIntegrityPhysicalRejectMessage(qualify(ref.DB, ref.Table))
				return
			}
			if _, key, ok := nameresolve.LookupStorageIntegrity(ref.DB, ref.Table, sel.Dynamic); ok {
				resp.Message = nameresolve.StorageIntegrityWriteRejectMessage(key)
				return
			}
			continue
		}
		if nameresolve.IsStorageIntegrityPhysicalDatabase(ref.Table, sel.Dynamic) {
			// A bare name in a database position: TRUNCATE DATABASE hg_safe,
			// TRUNCATE ALL TABLES FROM hg_unsafe, ALTER DATABASE hg_safe ...
			resp.Message = nameresolve.StorageIntegrityPhysicalDatabaseRejectMessage(ref.Table)
			return
		}
		if _, key, ok := nameresolve.LookupStorageIntegrity("", ref.Table, sel.Dynamic); ok {
			resp.Message = nameresolve.StorageIntegrityWriteRejectMessage(key)
			return
		}
	}
}
```

`touchesStorageIntegrity` and `qualify` already exist in `internal/handlers/select.go:274-289`.

- [ ] **Step 5: Wire it into `finalize`**

In `native.go`, extend `finalize` (keep the existing doc comment and add the new paragraph):

```go
func finalize(resp *pb.RewriteSQLResponse, sql string, ec pb.ExistenceClause, siVersion pb.StorageIntegrityContractVersion, e engine.Engine, sel nameresolve.Selection) {
	resp.ExistenceClause = ec
	resp.StorageIntegrityContractVersion = siVersion
	if resp.GetCode() == pb.RewriteCode_Success {
		return
	}
	resp.StatementType = pb.StatementType_STATEMENT_TYPE_UNSPECIFIED
	if resp.GetSqlAfterRewrite() == "" {
		resp.SqlAfterRewrite = sql
	}
	// Spec I D2: one place upgrades every reject's message to name the SI
	// object it touched, so no handler has to grow its own SI branch.
	if siVersion == pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1 {
		handlers.AnnotateStorageIntegrityReject(e, resp, sql, sel)
	}
}
```

Update all `finalize(...)` call sites in `doRewrite` (there are seven: writes, db-level, describe, exists/show-create, grant, select, and the Task 1 catch-all) to pass `e, selection`. `selection` is already computed at `native.go:96`.

- [ ] **Step 6: Add the annotator unit test**

Create `internal/handlers/storage_integrity_reject_test.go`:

```go
package handlers

import (
	"testing"

	"github.com/housegate/rewriter-go/internal/nameresolve"
	"github.com/housegate/rewriter-proto/gen/pb"
)

func siRejectSelection() nameresolve.Selection {
	return nameresolve.Selection{
		Mode: nameresolve.ModeDynamic,
		Dynamic: &pb.RewriteTableDynamicArgs{
			DatabaseMap:            map[string]string{"db1": "phys"},
			KnownPhysicalDatabases: []string{"phys"},
			Delim:                  "_",
			StorageIntegrity: &pb.StorageIntegrityArgs{
				Tables: map[string]*pb.StorageIntegrityArgs_Table{
					"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"}},
				ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
				ReservedRowIdColumn: "_hg_row_id",
				ContractVersion:     pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1,
			},
		},
	}
}

func TestAnnotateStorageIntegrityReject(t *testing.T) {
	e := newEngine(t)
	for _, tc := range []struct{ name, sql, want string }{
		{"physical table", "SYSTEM START MERGES hg_unsafe.db1__t",
			"storage-integrity physical table hg_unsafe.db1__t is not directly addressable"},
		{"logical table", "CHECK TABLE db1.t",
			"storage-integrity table db1.t accepts writes only through the signed statement lane"},
		{"physical database", "TRUNCATE DATABASE hg_safe",
			"storage-integrity physical database hg_safe is not directly addressable"},
		{"nothing storage-integrity", "SYSTEM RELOAD CONFIG", "original"},
		{"literal only", "SYSTEM RELOAD DICTIONARY 'hg_safe.db1__t'", "original"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &pb.RewriteSQLResponse{Code: pb.RewriteCode_UnsupportedStatement, Message: "original"}
			AnnotateStorageIntegrityReject(e, resp, tc.sql, siRejectSelection())
			if resp.GetMessage() != tc.want {
				t.Fatalf("message = %q, want %q", resp.GetMessage(), tc.want)
			}
		})
	}

	t.Run("success is never touched", func(t *testing.T) {
		resp := &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, Message: "success"}
		AnnotateStorageIntegrityReject(e, resp, "SYSTEM START MERGES hg_unsafe.db1__t", siRejectSelection())
		if resp.GetMessage() != "success" {
			t.Fatalf("message = %q, want the untouched success message", resp.GetMessage())
		}
	})

	t.Run("existing SI classification wins", func(t *testing.T) {
		resp := &pb.RewriteSQLResponse{Code: pb.RewriteCode_UnsupportedStatement, Message: "handler message",
			OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "db1", OriginalTable: "t", IsStorageIntegrity: true}}}
		AnnotateStorageIntegrityReject(e, resp, "DROP TABLE db1.t", siRejectSelection())
		if resp.GetMessage() != "handler message" {
			t.Fatalf("message = %q, want the handler's own SI message", resp.GetMessage())
		}
	})
}
```

`newEngine(t)` is the existing `internal/handlers` test helper (`select_test.go:11`), already used by `storage_integrity_test.go`; reuse it.

- [ ] **Step 7: Run the full suite**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test
```

Expected: every package `ok`, 190 golden cases pass. If any of the 178 pre-existing cases changed message, stop: that means the case was one of the D1/D2 bugs (record it in the commit) or the annotator is over-reaching (fix the annotator).

- [ ] **Step 8: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git add internal/engine/lexical.go internal/engine/lexical_test.go \
  internal/handlers/storage_integrity_reject.go internal/handlers/storage_integrity_reject_test.go \
  native.go internal/harness/testdata/storage_integrity_cases.json
git commit -m "fix(storage-integrity): name the SI object in every reject message (Spec I D2)"
```

### Task 3: D4 — escape backslashes in single-quoted literals

**Files:**
- Modify: `internal/handlers/dblevel.go:353-355` (`escapeSQLLiteral`)
- Test: `internal/handlers/storage_integrity_test.go` (append)
- Modify: `internal/harness/testdata/storage_integrity_cases.json` (2 new cases)

**Interfaces:**
- Produces: `escapeSQLLiteral` escapes `\` → `\\` before `'` → `''`. Seven call sites already exist (`dblevel.go:151,189,283,286,287,324`, `storage_integrity.go:56`); none changes.

- [ ] **Step 1: Add the two corpus cases (red)**

```json
  {
    "name": "si_excluded_parts_escaping",
    "sql": "SELECT a FROM db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t", "excluded_unsafe_parts": ["o'brien_1_1_0", "back\\slash_1_1_0", "mix\\'ed_1_1_0"]}},
        "read_mode": "UNSAFE_LATEST",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t UNION ALL SELECT * EXCEPT (_hg_row_id) FROM hg_unsafe.db1__t WHERE _part NOT IN ('o''brien_1_1_0', 'back\\\\slash_1_1_0', 'mix\\\\''ed_1_1_0')) AS \"db1.t\"",
    "want_sql_contains": ["'o''brien_1_1_0'", "'back\\\\slash_1_1_0'", "'mix\\\\''ed_1_1_0'"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t"},
    "allow_sql_divergence": true
  },
  {
    "name": "si_excluded_part_trailing_backslash_cannot_close_literal",
    "sql": "SELECT a FROM db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t", "excluded_unsafe_parts": ["evil_1_1_0\\", "all_2_2_0"]}},
        "read_mode": "UNSAFE_LATEST",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t UNION ALL SELECT * EXCEPT (_hg_row_id) FROM hg_unsafe.db1__t WHERE _part NOT IN ('evil_1_1_0\\\\', 'all_2_2_0')) AS \"db1.t\"",
    "want_sql_contains": ["'evil_1_1_0\\\\', 'all_2_2_0'"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t"},
    "allow_sql_divergence": true
  }
```

JSON escaping reminder: `"back\\slash_1_1_0"` is the 17-character part name `back\slash_1_1_0` (one backslash); the expected SQL fragment `'back\\\\slash_1_1_0'` is the 19-character SQL text `'back\\slash_1_1_0'` (two backslashes — the correctly escaped form). The second case is the security case: pre-fix, `evil_1_1_0\` renders as `'evil_1_1_0\'`, whose closing quote is escaped away, so the following `, 'all_2_2_0'` is swallowed into the literal and the statement's meaning changes.

Under Spec J D3 these two cases will additionally need per-engine `want_sql_go` / `want_sql_cpp` because they set `allow_sql_divergence` (the derived-table shape renders `* EXCEPT (x)` differently in the two formatters). That edit belongs to Spec J's plan.

- [ ] **Step 2: Add the unit test (red)**

Append to `internal/handlers/storage_integrity_test.go`:

```go
func TestEscapeSQLLiteral_EscapesBackslashBeforeQuote(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"plain_1_1_0", "plain_1_1_0"},
		{"o'brien", "o''brien"},
		{`back\slash`, `back\\slash`},
		{`evil\`, `evil\\`},
		{`mix\'ed`, `mix\\''ed`},
	} {
		if got := escapeSQLLiteral(tc.in); got != tc.want {
			t.Errorf("escapeSQLLiteral(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
```

- [ ] **Step 3: Run to verify failure**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test 2>&1 | grep -E 'TestEscapeSQLLiteral|si_excluded_part'
```

Expected: `TestEscapeSQLLiteral_EscapesBackslashBeforeQuote` FAILs on the three backslash rows; both new golden cases FAIL.

- [ ] **Step 4: Implement**

Replace `internal/handlers/dblevel.go:349-355` (keep the existing doc comment, extend it):

```go
// escapeSQLLiteral makes s safe to embed inside a single-quoted ClickHouse
// string literal. ClickHouse honours BOTH '' and \' as an escaped quote inside
// a single-quoted literal, so a value ending in a backslash would otherwise
// escape the closing quote and swallow whatever follows. Backslashes are
// therefore doubled BEFORE quotes are doubled (Spec I D4).
func escapeSQLLiteral(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", "''")
}
```

- [ ] **Step 5: Run to verify pass**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test
```

Expected: all packages `ok`, 192 golden cases pass.

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git add internal/handlers/dblevel.go internal/handlers/storage_integrity_test.go internal/harness/testdata/storage_integrity_cases.json
git commit -m "fix(storage-integrity): escape backslashes in SQL literals (Spec I D4)"
```

### Task 4: D7a — scope the reserved-column check to references that resolve to an SI table

Today `SELECT a FROM db1.t WHERE a IN (SELECT _hg_row_id FROM other.u)` is rejected even though `other.u` is an ordinary table that may legitimately own a column with that name.

**Files:**
- Create: `internal/engine/scope.go`, `internal/engine/scope_test.go`
- Modify: `internal/handlers/select.go:170-179` (the reserved-column guard)
- Modify: `internal/harness/testdata/storage_integrity_cases.json` (2 new cases)

**Interfaces:**
- Produces: `func engine.ReferencesIdentifierInScope(ast AST, name string, protected func(TableTarget) bool) (bool, error)`. `engine.ReferencesIdentifier` (`nodes.go:745`) stays for its existing tests; `select.go` becomes the only production caller of the new function.

- [ ] **Step 1: Add the two corpus cases (red)**

```json
  {
    "name": "si_reserved_column_on_ordinary_table_in_subquery_allowed",
    "sql": "SELECT a FROM db1.t WHERE a IN (SELECT _hg_row_id FROM other.u)",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS \"db1.t\" WHERE a IN (SELECT _hg_row_id FROM phys.\"other.u\" AS \"other.u\")",
    "want_sql_contains": ["hg_safe.db1__t", "phys.\"other.u\""],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t", "other.u": "phys.other.u"},
    "want_accessed": [
      {"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true},
      {"original_database": "other", "original_table": "u", "logical_database": "other", "physical_database": "phys", "is_storage_integrity": false}
    ],
    "allow_sql_divergence": true
  },
  {
    "name": "si_reserved_column_qualified_by_si_alias_rejected",
    "sql": "SELECT a FROM db1.t AS s WHERE a IN (SELECT k FROM other.u WHERE k = s._hg_row_id)",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "RewriteError", "want_stmt": "", "reject": true,
    "want_message_contains": "reserved column _hg_row_id is not addressable"
  }
```

The second case is the fail-closed half: an inner block that reads only ordinary tables still cannot reach an outer SI table's hidden column through its alias.

- [ ] **Step 2: Run to verify the first case fails**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test 2>&1 | grep -E 'si_reserved_column_(on_ordinary|qualified_by)'
```

Expected: `si_reserved_column_on_ordinary_table_in_subquery_allowed` FAILs (`code = RewriteError, want Success`); `si_reserved_column_qualified_by_si_alias_rejected` already PASSes (today's blunt check rejects it for the wrong reason — it must keep passing after the fix, for the right one).

- [ ] **Step 3: Implement the scope-aware walker**

Create `internal/engine/scope.go`:

```go
package engine

import (
	"encoding/json"
	"fmt"
)

// ReferencesIdentifierInScope reports whether the AST addresses `name` in a
// position that can resolve to a table `protected` accepts.
//
// A reference counts when either:
//   - the query block that owns it reads at least one protected table (its own
//     FROM/JOIN list, after in-scope CTE aliases are removed), or
//   - the reference is qualified and its qualifier is bound to a protected
//     table in that block or any enclosing one.
//
// An unqualified reference inside a block that reads only ordinary tables does
// NOT count: an identically named column on an ordinary table is legitimate
// (Spec I D7a). Hiding is still enforced structurally — the protected table is
// replaced by a derived table that projects the column away — so this scoping
// narrows the error message, never the protection.
func ReferencesIdentifierInScope(ast AST, name string, protected func(TableTarget) bool) (bool, error) {
	var root map[string]any
	if err := json.Unmarshal(ast, &root); err != nil {
		return false, fmt.Errorf("engine: decode: %w", err)
	}
	return scopeWalk(root, name, protected, map[string]bool{}, false, nil), nil
}

// scopeWalk descends `node`, which belongs to a query block whose own tables
// make `blockProtected` true and whose qualifier bindings are `bindings`
// (qualifier -> is-protected, innermost binding wins).
func scopeWalk(node any, name string, protected func(TableTarget) bool,
	bindings map[string]bool, blockProtected bool, cteScope map[string]bool) bool {
	switch n := node.(type) {
	case map[string]any:
		// A nested query block re-derives its own tables and bindings.
		if sel, ok := n["select"].(map[string]any); ok {
			scope := forkCTEScope(sel, cteScope)
			nextBindings, nextProtected := blockScope(sel, scope, protected, bindings)
			for _, v := range sel {
				if scopeWalk(v, name, protected, nextBindings, nextProtected, scope) {
					return true
				}
			}
			return false
		}
		// A qualified reference (`alias.name`, `db.table.name`) is decided by
		// its qualifier and never descends further: the child identifier node
		// is the same reference and must not be re-judged as unqualified.
		if dot, ok := n["dot"].(map[string]any); ok && identName(dot["field"]) == name {
			if q := identName(dot["this"]); q != "" {
				if p, bound := bindings[q]; bound {
					return p
				}
			}
			return blockProtected
		}
		if unqualifiedIdentifierHit(n, name) {
			if blockProtected {
				return true
			}
			return false
		}
		for _, v := range n {
			if scopeWalk(v, name, protected, bindings, blockProtected, cteScope) {
				return true
			}
		}
	case []any:
		for _, v := range n {
			if scopeWalk(v, name, protected, bindings, blockProtected, cteScope) {
				return true
			}
		}
	}
	return false
}

// blockScope returns the qualifier bindings visible inside one select block
// (enclosing bindings plus this block's own tables) and whether the block
// itself reads a protected table.
func blockScope(sel map[string]any, cteScope map[string]bool, protected func(TableTarget) bool,
	parent map[string]bool) (map[string]bool, bool) {
	out := make(map[string]bool, len(parent)+2)
	for k, v := range parent {
		out[k] = v
	}
	blockProtected := false
	for _, tt := range blockTables(sel, cteScope) {
		p := protected(tt)
		blockProtected = blockProtected || p
		switch {
		case tt.Alias != "":
			out[tt.Alias] = p
		case tt.Table != "":
			out[tt.Table] = p
		}
	}
	return out, blockProtected
}

// blockTables returns the tables one select block reads directly. It never
// descends into a nested block (`select` / `subquery`) — those own their own
// scope — and skips bare references that match an in-scope CTE alias.
func blockTables(sel map[string]any, cteScope map[string]bool) []TableTarget {
	var out []TableTarget
	var walk func(node any)
	walk = func(node any) {
		switch n := node.(type) {
		case map[string]any:
			if _, nested := n["select"]; nested {
				return
			}
			if _, nested := n["subquery"]; nested {
				return
			}
			if tbl, ok := n["table"].(map[string]any); ok {
				tt := decodeTableTarget(tbl)
				if tt.Table == "" {
					for _, v := range n {
						walk(v)
					}
					return
				}
				if tt.DB == "" && cteScope[tt.Table] {
					return
				}
				out = append(out, tt)
				return
			}
			for _, v := range n {
				walk(v)
			}
		case []any:
			for _, v := range n {
				walk(v)
			}
		}
	}
	walk(sel)
	return out
}

// unqualifiedIdentifierHit recognizes the same reference shapes refWalk does,
// minus the `dot` form (handled by the caller): a bare Identifier node, a
// `column` node, a JOIN USING entry, and star EXCEPT / REPLACE / RENAME
// entries. String literals never match — they are not Identifier-shaped.
func unqualifiedIdentifierHit(n map[string]any, name string) bool {
	if got, ok := n["name"].(string); ok && got == name {
		if _, identifierShape := n["quoted"]; identifierShape {
			return true
		}
	}
	if col, ok := n["column"].(map[string]any); ok && identName(col["name"]) == name {
		return true
	}
	if using, ok := n["using"].([]any); ok {
		for _, e := range using {
			if identName(e) == name {
				return true
			}
		}
	}
	star, ok := n["star"].(map[string]any)
	if !ok {
		return false
	}
	if list, ok := star["except"].([]any); ok {
		for _, e := range list {
			if identName(e) == name {
				return true
			}
		}
	}
	if list, ok := star["replace"].([]any); ok {
		for _, e := range list {
			if m, ok := e.(map[string]any); ok && identName(m["alias"]) == name {
				return true
			}
		}
	}
	if list, ok := star["rename"].([]any); ok {
		for _, e := range list {
			if pair, ok := e.([]any); ok {
				for _, side := range pair {
					if identName(side) == name {
						return true
					}
				}
			}
		}
	}
	return false
}
```

- [ ] **Step 4: Add the engine-level test**

Create `internal/engine/scope_test.go`:

```go
package engine

import "testing"

func TestReferencesIdentifierInScope(t *testing.T) {
	e := newTestEngine(t)
	si := func(tt TableTarget) bool { return tt.DB == "db1" && tt.Table == "t" }
	for _, tc := range []struct {
		name string
		sql  string
		want bool
	}{
		{"bare ref in the si block", "SELECT _hg_row_id, a FROM db1.t", true},
		{"where ref in the si block", "SELECT a FROM db1.t WHERE _hg_row_id = 'x'", true},
		{"output alias in the si block", "SELECT 1 AS _hg_row_id FROM db1.t", true},
		{"join using in the si block", "SELECT * FROM db1.t AS a JOIN other.u AS b USING (_hg_row_id)", true},
		{"star except in the si block", "SELECT * EXCEPT (_hg_row_id) FROM db1.t", true},
		{"ordinary table in a nested block", "SELECT a FROM db1.t WHERE a IN (SELECT _hg_row_id FROM other.u)", false},
		{"si alias reached from a nested block", "SELECT a FROM db1.t AS s WHERE a IN (SELECT k FROM other.u WHERE k = s._hg_row_id)", true},
		{"ordinary alias in the si block", "SELECT o._hg_row_id FROM db1.t AS s JOIN other.u AS o ON 1", false},
		{"no si table at all", "SELECT _hg_row_id FROM other.u", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := e.ParseOne(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got, err := ReferencesIdentifierInScope(ast, "_hg_row_id", si)
			if err != nil {
				t.Fatalf("ReferencesIdentifierInScope: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 5: Switch the handler over**

In `internal/handlers/select.go`, replace lines 170-179 (the `rid` guard) with:

```go
		rid := nameresolve.ReservedRowIDColumn(sel.Dynamic)
		hit, herr := engine.ReferencesIdentifierInScope(ast, rid, func(tt engine.TableTarget) bool {
			_, _, ok := nameresolve.LookupStorageIntegrity(tt.DB, tt.Table, sel.Dynamic)
			return ok
		})
		if herr != nil {
			return nil, nil, herr
		}
		if hit {
			resp.Code = pb.RewriteCode_RewriteError
			resp.Message = fmt.Sprintf(reservedColumnRejectFmt, rid)
			return ast, resp, nil
		}
```

- [ ] **Step 6: Run the full suite**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test
```

Expected: all packages `ok`. Every pre-existing `si_reserved_column_*`, `si_union_reserved_rejected`, `si_except_reserved_column_rejected` and `si_reserved_output_alias_rejected` case must still pass — each of them puts the reference in a block that reads `db1.t`.

- [ ] **Step 7: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git add internal/engine/scope.go internal/engine/scope_test.go internal/handlers/select.go internal/harness/testdata/storage_integrity_cases.json
git commit -m "fix(storage-integrity): scope the reserved-column check to SI tables (Spec I D7a)"
```

### Task 5: D7b — make the SI namespace gate CTE-aware

`WITH t AS (SELECT 1 AS id) SELECT a FROM other.u WHERE id IN t` under `USE db1` with SI key `db1.t` is rejected today: `CollectNamespaceRefs` yields an `IN`-table ref for the bare name `t`, which the gate resolves against the current database. `CollectSelectTables` / `RewriteSelectTables` already carry CTE scope; the namespace collector must too.

**Files:**
- Modify: `internal/engine/nodes.go:152-193` (`CollectNamespaceRefs`)
- Test: `internal/engine/scope_test.go` (append)
- Modify: `internal/harness/testdata/storage_integrity_cases.json` (3 new cases)

**Interfaces:**
- Consumes: `forkCTEScope` (`nodes.go:1035`), `decodeInNamespaceRef` (`nodes.go:316`).
- Produces: `engine.CollectNamespaceRefs` keeps its signature `(ast AST) ([]NamespaceRef, error)`; the CTE scope is internal.

- [ ] **Step 1: Add the three corpus cases (red)**

```json
  {
    "name": "si_cte_alias_shadowing_si_name_in_from_allowed",
    "sql": "WITH t AS (SELECT 1 AS id) SELECT id FROM t",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "upstream_logical_database_in_context": "db1",
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "WITH t AS (SELECT 1 AS id) SELECT id FROM t",
    "want_sql_not_contains": ["hg_safe", "hg_unsafe"],
    "want_table_rewrites": {},
    "want_accessed": []
  },
  {
    "name": "si_cte_alias_shadowing_si_name_in_in_clause_allowed",
    "sql": "WITH t AS (SELECT 1 AS id) SELECT a FROM other.u WHERE id IN t",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "upstream_logical_database_in_context": "db1",
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "WITH t AS (SELECT 1 AS id) SELECT a FROM phys.\"other.u\" AS \"other.u\" WHERE id IN t",
    "want_sql_contains": ["phys.\"other.u\""],
    "want_sql_not_contains": ["hg_safe", "hg_unsafe"],
    "want_table_rewrites": {"other.u": "phys.other.u"},
    "want_accessed": [
      {"original_database": "other", "original_table": "u", "logical_database": "other", "physical_database": "phys", "is_storage_integrity": false}
    ]
  },
  {
    "name": "si_table_inside_cte_body_rewritten",
    "sql": "WITH c AS (SELECT a FROM db1.t) SELECT a FROM c",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "WITH c AS (SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS \"db1.t\") SELECT a FROM c",
    "want_sql_contains": ["hg_safe.db1__t"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t"},
    "want_accessed": [
      {"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}
    ],
    "allow_sql_divergence": true
  }
```

The third case is the *working* behaviour, added to lock it: an SI table named inside a CTE body must still be rewritten. The corpus contains zero `WITH <name> AS (…)` cases today, which is why the gap survived Spec G.

- [ ] **Step 2: Run to verify which cases fail**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test 2>&1 | grep -E 'si_cte_alias|si_table_inside_cte'
```

Expected: `si_cte_alias_shadowing_si_name_in_in_clause_allowed` FAILs with `code = RewriteError`. The other two are expected to pass already (they lock existing behaviour); if either fails, record the actual output before changing anything — it is a second, unreported bug.

- [ ] **Step 3: Implement CTE scope in the namespace collector**

In `internal/engine/nodes.go`, replace the body of `CollectNamespaceRefs` (`:152-193`) with the scope-carrying form:

```go
func CollectNamespaceRefs(ast AST) ([]NamespaceRef, error) {
	var root any
	if err := json.Unmarshal(ast, &root); err != nil {
		return nil, fmt.Errorf("engine: decode namespace references: %w", err)
	}
	var out []NamespaceRef
	var walk func(node any, cteScope map[string]bool)
	walk = func(node any, cteScope map[string]bool) {
		switch n := node.(type) {
		case map[string]any:
			// Every select block contributes its WITH aliases to a forked
			// scope, mirroring visitTables / CollectSelectTables. A bare
			// `IN <alias>` inside that scope is a CTE reference, not a table
			// (Spec I D7b).
			if sel, ok := n["select"].(map[string]any); ok {
				scope := forkCTEScope(sel, cteScope)
				for _, v := range sel {
					walk(v, scope)
				}
				return
			}
			if fn, ok := n["function"].(map[string]any); ok {
				if ref, ok := decodeNamespaceFunctionRef(fn); ok {
					out = append(out, ref)
				}
			}
			if in, ok := n["in"].(map[string]any); ok {
				if ref, ok := decodeInNamespaceRef(in); ok {
					if !(ref.Target.DB == "" && cteScope[ref.Target.Table]) {
						out = append(out, ref)
					}
				}
			}
			if property, ok := n["engine_property"].(map[string]any); ok {
				if ref, ok := decodeTableEngineNamespaceRef(property); ok {
					out = append(out, ref)
				}
			}
			if property, ok := n["dict_property"].(map[string]any); ok {
				if ref, ok := decodeDictionarySourceNamespaceRef(property); ok {
					out = append(out, ref)
				}
			}
			for _, child := range n {
				walk(child, cteScope)
			}
		case []any:
			for _, child := range n {
				walk(child, cteScope)
			}
		}
	}
	walk(root, nil)
	return out, nil
}
```

Only the bare (`DB == ""`) `IN` form is skipped: a qualified `db.alias` is a real table reference, exactly as `visitTables` treats it (`nodes.go:634`).

- [ ] **Step 4: Add the engine test**

Append to `internal/engine/scope_test.go`:

```go
func TestCollectNamespaceRefs_SkipsInScopeCTEAliases(t *testing.T) {
	e := newTestEngine(t)
	ast, err := e.ParseOne("WITH t AS (SELECT 1 AS id) SELECT a FROM other.u WHERE id IN t")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	refs, err := CollectNamespaceRefs(ast)
	if err != nil {
		t.Fatalf("CollectNamespaceRefs: %v", err)
	}
	for _, ref := range refs {
		if ref.Source == NamespaceRefInTable && ref.Target.DB == "" && ref.Target.Table == "t" {
			t.Fatalf("in-scope CTE alias must not be collected as a namespace ref: %+v", refs)
		}
	}

	ast, err = e.ParseOne("SELECT a FROM other.u WHERE id IN t")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	refs, err = CollectNamespaceRefs(ast)
	if err != nil {
		t.Fatalf("CollectNamespaceRefs: %v", err)
	}
	found := false
	for _, ref := range refs {
		if ref.Source == NamespaceRefInTable && ref.Target.Table == "t" {
			found = true
		}
	}
	if !found {
		t.Fatal("a bare IN target with no CTE in scope must still be collected")
	}
}
```

- [ ] **Step 5: Run the full suite**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test
```

Expected: all packages `ok`, 197 golden cases pass.

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git add internal/engine/nodes.go internal/engine/scope_test.go internal/harness/testdata/storage_integrity_cases.json
git commit -m "fix(storage-integrity): make the namespace gate CTE-aware (Spec I D7b)"
```

### Task 6: D7c — reject `PREWHERE` on an SI table with the clean message

Without this, `PREWHERE` reaches ClickHouse against the derived table and fails with a raw error whose text quotes `hg_safe.db1__t` and `_hg_row_id` back to the client — the names Spec G D-9 declares protocol-owned and unaddressable.

**Files:**
- Modify: `internal/engine/lexical.go` (add `PrewhereTargets`)
- Modify: `internal/handlers/select.go:136-169` (wrapper pre-reject set) and the message constant
- Test: `internal/engine/lexical_test.go` (append)
- Modify: `internal/harness/testdata/storage_integrity_cases.json` (3 new cases + 7 existing message updates)

**Interfaces:**
- Produces: `func engine.PrewhereTargets(e Engine, sql string) ([]TableTarget, error)` — for each `PREWHERE` keyword, the first table in the FROM clause of the query block that owns it (PREWHERE is a query-level clause bound to the main table, exactly like the SAMPLE handling in `collectSelectLevelSampleTargets`).
- Produces: the wrapper message becomes `FINAL/SAMPLE/PREWHERE/WITH OFFSET/column aliases on storage-integrity tables are not supported` in both engines.

- [ ] **Step 1: Update the seven existing wrapper messages and add three cases (red)**

First rewrite the shared message in the corpus:

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
grep -c 'FINAL/SAMPLE/WITH OFFSET' internal/harness/testdata/storage_integrity_cases.json
python3 - <<'PY'
import pathlib
p = pathlib.Path("internal/harness/testdata/storage_integrity_cases.json")
s = p.read_text()
old = "FINAL/SAMPLE/WITH OFFSET/column aliases on storage-integrity tables are not supported"
new = "FINAL/SAMPLE/PREWHERE/WITH OFFSET/column aliases on storage-integrity tables are not supported"
assert s.count(old) == 7, s.count(old)
p.write_text(s.replace(old, new))
PY
grep -c 'FINAL/SAMPLE/PREWHERE/WITH OFFSET' internal/harness/testdata/storage_integrity_cases.json
```

Expected: first `grep -c` prints `7`, the script exits silently, the second prints `7`.

Then append the three new cases:

```json
  {
    "name": "si_prewhere_rejected",
    "sql": "SELECT a FROM db1.t PREWHERE a > 1",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "RewriteError", "want_stmt": "", "reject": true,
    "want_message_contains": "FINAL/SAMPLE/PREWHERE/WITH OFFSET/column aliases on storage-integrity tables are not supported"
  },
  {
    "name": "si_mixed_ordinary_prewhere_allowed",
    "sql": "SELECT * FROM other.u AS x JOIN db1.t AS s ON 1 PREWHERE x.a > 1",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT * FROM phys.\"other.u\" AS x JOIN (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS s ON 1 PREWHERE x.a > 1",
    "want_sql_contains": ["hg_safe.db1__t", "phys.\"other.u\""],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t", "other.u": "phys.other.u"},
    "allow_sql_divergence": true
  },
  {
    "name": "si_prewhere_literal_allowed",
    "sql": "SELECT 'PREWHERE' FROM db1.t",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT 'PREWHERE' FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS \"db1.t\"",
    "want_sql_contains": ["hg_safe.db1__t"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t"},
    "allow_sql_divergence": true
  }
```

`si_mixed_ordinary_prewhere_allowed` pins the binding rule (PREWHERE belongs to the main FROM table, so an SI table joined in does not make it a rejection) and `si_prewhere_literal_allowed` mirrors the existing `si_with_offset_literal_allowed` guard against literal false positives.

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test 2>&1 | grep -E 'si_prewhere|si_mixed_ordinary_prewhere|si_final_rejected|si_with_offset_rejected'
```

Expected: `si_prewhere_rejected` FAILs (`code = Success, want RewriteError`); the seven re-worded cases FAIL on the message; the two allowed cases pass.

- [ ] **Step 3: Implement `PrewhereTargets`**

Append to `internal/engine/lexical.go`, adding an import block at the top of the file (Task 2 created it with none):

```go
// at the top of the file, directly under `package engine`
import "strings"

// PrewhereTargets binds each real PREWHERE keyword to the FIRST table of the
// FROM clause of the query block that owns it. PREWHERE is a query-level
// clause evaluated against the main table, so a JOINed table does not own it —
// the same rule collectSelectLevelSampleTargets applies to SELECT-level SAMPLE.
//
// The keyword is matched by text rather than token type because the dialect
// tokenizer does not necessarily give PREWHERE its own type; STRING tokens are
// excluded explicitly so a literal cannot trigger policy.
func PrewhereTargets(e Engine, sql string) ([]TableTarget, error) {
	toks, err := tokenizeRaw(e, sql)
	if err != nil {
		return nil, err
	}
	var out []TableTarget
	for i, tok := range toks {
		if tok.TokenType == "STRING" || !strings.EqualFold(tok.Text, "PREWHERE") {
			continue
		}
		from := -1
		nesting := 0
		for j := i - 1; j >= 0; j-- {
			if toks[j].Text == ")" {
				nesting++
				continue
			}
			if toks[j].Text == "(" && nesting > 0 {
				nesting--
				continue
			}
			if nesting == 0 && toks[j].TokenType == "FROM" {
				from = j
				break
			}
		}
		if from < 0 {
			continue
		}
		if target, ok := rawTokenTableTarget(toks, from+1); ok {
			out = append(out, target)
		}
	}
	return out, nil
}
```

- [ ] **Step 4: Add the engine test**

Append to `internal/engine/lexical_test.go`:

```go
func TestPrewhereTargets(t *testing.T) {
	e := newTestEngine(t)
	for _, tc := range []struct {
		name string
		sql  string
		want []TableTarget
	}{
		{"main table", "SELECT a FROM db1.t PREWHERE a > 1", []TableTarget{{DB: "db1", Table: "t"}}},
		{"binds to the FROM table, not the JOIN", "SELECT * FROM other.u AS x JOIN db1.t AS s ON 1 PREWHERE x.a > 1", []TableTarget{{DB: "other", Table: "u"}}},
		{"literal is not a keyword", "SELECT 'PREWHERE' FROM db1.t", nil},
		{"absent", "SELECT a FROM db1.t WHERE a > 1", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PrewhereTargets(e, tc.sql)
			if err != nil {
				t.Fatalf("PrewhereTargets: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
			for i := range got {
				if got[i].DB != tc.want[i].DB || got[i].Table != tc.want[i].Table {
					t.Fatalf("got %+v, want %+v", got, tc.want)
				}
			}
		})
	}
}
```

- [ ] **Step 5: Wire it into the pre-reject set**

In `internal/handlers/select.go`, inside the `if !modified && len(sourceSQL) > 0 { ... }` block (`:153-164`), add the PREWHERE probe next to the WITH OFFSET probe:

```go
		if !modified && len(sourceSQL) > 0 {
			withOffsetTargets, oerr := engine.WithOffsetTargets(e, sourceSQL[0])
			if oerr != nil {
				return nil, nil, oerr
			}
			prewhereTargets, perr := engine.PrewhereTargets(e, sourceSQL[0])
			if perr != nil {
				return nil, nil, perr
			}
			for _, tt := range append(withOffsetTargets, prewhereTargets...) {
				if _, _, ok := nameresolve.LookupStorageIntegrity(tt.DB, tt.Table, sel.Dynamic); ok {
					modified = true
					break
				}
			}
		}
```

and update the message literal at `:167`:

```go
			resp.Message = "FINAL/SAMPLE/PREWHERE/WITH OFFSET/column aliases on storage-integrity tables are not supported"
```

- [ ] **Step 6: Run the full suite**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test
```

Expected: all packages `ok`, 200 golden cases pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git add internal/engine/lexical.go internal/engine/lexical_test.go internal/handlers/select.go internal/harness/testdata/storage_integrity_cases.json
git commit -m "fix(storage-integrity): reject PREWHERE on SI tables with the wrapper message (Spec I D7c)"
```

### Task 7: Finalize the shared corpus (D7d case) and freeze its checksum

**Files:**
- Modify: `internal/harness/testdata/storage_integrity_cases.json` (1 new case)

**Interfaces:**
- Produces: the final Go-side corpus and its sha256, which Task 8 copies byte-for-byte into rewriter-grpc.

- [ ] **Step 1: Add the D7d literal-preservation case**

This case passes in Go today and is the red test for the C++ `restoreStorageIntegrityProjectionSyntax` fix (Task 10) — a real divergence the shared corpus could not see before.

```json
  {
    "name": "si_string_literal_except_is_not_mutated",
    "sql": "SELECT 'EXCEPT _hg_row_id' AS s FROM db1.t",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT 'EXCEPT _hg_row_id' AS s FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS \"db1.t\"",
    "want_sql_contains": ["hg_safe.db1__t"],
    "want_sql_not_contains": ["'EXCEPT (_hg_row_id)'"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t"},
    "allow_sql_divergence": true
  }
```

- [ ] **Step 2: Validate the JSON and run the whole suite**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
python3 -c "import json;d=json.load(open('internal/harness/testdata/storage_integrity_cases.json'));print(len(d),'cases');names=[c['name'] for c in d];assert len(names)==len(set(names)),'duplicate case name'"
env -u REWRITER_ORACLE_ADDR make test
```

Expected: `201 cases`, no duplicate name, every package `ok`.

- [ ] **Step 3: Record the new checksum**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json | tee /tmp/si_corpus_sha.txt
```

Expected: a single sha256 that is **not** `cb37d657…`. Keep `/tmp/si_corpus_sha.txt` — Task 8 and Task 14 compare against it.

- [ ] **Step 4: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git add internal/harness/testdata/storage_integrity_cases.json
git commit -m "test(storage-integrity): add the C++ literal-mangling regression case (Spec I D7d)"
```

---

## Part B — rewriter-grpc (C++ engine)

**Working directory for every Part B task:** `/Users/uranuswch/Dev/housegate/rewriter-grpc`

**Build/test loop for every Part B task** (never run a local cmake — ClickHouse does not compile on dev machines):

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
rsync -az --delete --exclude='.git' --exclude='build/' --exclude='clickHouse/' \
  --exclude='contrib' --exclude='docs/' -e "ssh -p 30100" \
  ./ sentio@64.38.131.242:/home/sentio/chen/rewriter-grpc/
ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && ./scripts.sh rebuild"
```

Single-case run (the corpus suite is parameterised by case name):

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/<case-name>'"
```

- [ ] **Task 7b (pre-flight, do once):** branch and prove the baseline builds.

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git checkout -b feat/si-surface-failclosed
# rsync + rebuild as above, then:
ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && ctest --test-dir build --output-on-failure"
```

Expected: all tests pass on the unmodified tree.

### Task 8: Copy the finalized corpus and add the D1 catch-all to the C++ dispatch

**Files:**
- Modify: `tests/testdata/storage_integrity_cases.json` (replaced wholesale by `cp`)
- Modify: `src/handlers/storage_integrity.h`, `src/handlers/storage_integrity.cc`, `src/rewriter-server.cc`

**Interfaces:**
- Consumes: the Go corpus finalized in Task 7.
- Produces: `constexpr std::string_view rewriter_handlers::kStorageIntegrityUnmodelledMessage` and `bool rewriter_handlers::rejectUnmodelledStorageIntegrityStatement(const DB::ASTPtr &, const rewriter::RewriteTableDynamicArgs &, rewriter::RewriteSQLResponse *)`.

- [ ] **Step 1: Copy the corpus byte-for-byte and prove equality**

```bash
cp /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json \
   /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json
cmp /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json \
    /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json && echo IDENTICAL
shasum -a 256 /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json
```

Expected: `IDENTICAL`, and the sha256 matches `/tmp/si_corpus_sha.txt` from Task 7 Step 3. **Never hand-edit the C++ copy** — if a case needs changing, change the Go copy and re-run this step.

- [ ] **Step 2: Run the corpus suite to see the red set**

Rsync + rebuild, then:

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.*' 2>&1 | tail -40"
```

Expected red set (record it — it is the C++ work queue for Tasks 8-13): `si_unmodelled_statement_rejected`, `si_unmodelled_statement_on_ordinary_table_rejected`, all ten `si_system_*` / `si_check_table_logical_rejected` / `si_truncate_*` / `si_alter_database_safe_rejected` / `si_drop_dictionary_safe_rejected` / `si_create_live_view_over_si_rejected`, the two `si_excluded_part*`, `si_reserved_column_on_ordinary_table_in_subquery_allowed`, `si_cte_alias_shadowing_si_name_in_in_clause_allowed`, `si_prewhere_rejected`, `si_string_literal_except_is_not_mutated`, and the seven pre-existing wrapper-message cases (`si_final_rejected`, `si_sample_rejected`, three `si_with_offset_*`, `si_alias_column_list_rejected`, `si_comma_si_with_offset_rejected`) whose message text changed in Task 6. Everything else must stay green.

- [ ] **Step 3: Declare the catch-all helper**

In `src/handlers/storage_integrity.h`, next to `kStorageIntegrityWrapperMessage` (`:18-19`):

```cpp
constexpr std::string_view kStorageIntegrityUnmodelledMessage =
  "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded";
```

and next to the other reject helpers:

```cpp
// Refuse any statement whose root is not a SELECT-family node while the v1
// storage-integrity contract is active: reaching the SELECT dispatch tail with
// such a root means no handler modelled the statement, so nothing checked it
// against the protocol-owned namespaces (Spec I D1). Returns true when the
// response was turned into a rejection.
bool rejectUnmodelledStorageIntegrityStatement(const DB::ASTPtr &ast,
  const rewriter::RewriteTableDynamicArgs &args,
  rewriter::RewriteSQLResponse *response);
```

- [ ] **Step 4: Implement it**

In `src/handlers/storage_integrity.cc`, add the includes

```cpp
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
```

and, in the exported (non-anonymous) part of `namespace rewriter_handlers`, the definition:

```cpp
bool rejectUnmodelledStorageIntegrityStatement(const DB::ASTPtr &ast,
  const rewriter::RewriteTableDynamicArgs &args,
  rewriter::RewriteSQLResponse *response) {
  if (!ast || !response) return false;
  if (!args.has_storage_integrity() || args.storage_integrity().tables().empty()) return false;
  if (ast->as<DB::ASTSelectWithUnionQuery>() || ast->as<DB::ASTSelectQuery>()
      || ast->as<DB::ASTSelectIntersectExceptQuery>())
    return false;
  response->set_code(rewriter::RewriteCode::UnsupportedStatement);
  response->set_message(std::string(kStorageIntegrityUnmodelledMessage));
  return true;
}
```

- [ ] **Step 5: Call it from the dispatch tail**

In `src/rewriter-server.cc`, replace the final dispatch line (`:447`, `rewriter_handlers::handleSelectQuery(ast, request, response);`) with:

```cpp
    if (si_active && rewriter_handlers::rejectUnmodelledStorageIntegrityStatement(
          ast, *si_selection.dynamic_args, response))
      return;
    rewriter_handlers::handleSelectQuery(ast, request, response);
```

`si_active` and `si_selection` are already in scope from `:336-341`. `response->set_sql_after_rewrite(original_query)` at `:367` already satisfies the corpus's reject-echo assertion.

- [ ] **Step 6: Rebuild and verify the two D1 cases flip green**

```bash
# rsync + rebuild, then:
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_unmodelled_statement*'"
```

Expected: both `si_unmodelled_statement_rejected` and `si_unmodelled_statement_on_ordinary_table_rejected` PASS. The rest of the red set from Step 2 stays red until Tasks 9-13 — do not chase it here.

- [ ] **Step 7: Add the C++-local pass-through test**

The "SI absent → unmodelled statements still pass through" half is engine-local (the two engines classify such a statement differently: C++ stamps `STATEMENT_TYPE_SELECT`, Go leaves `UNSPECIFIED`). Append to `tests/rewriter_test.cc`, right before the `} // namespace` that closes the SI golden section:

```cpp
TEST(StorageIntegrityCatchAll, UnmodelledStatementPassesThroughWithoutStorageIntegrity) {
  RewriterServiceImpl service;
  rewriter::RewriteSQLRequest req;
  req.set_sql("SYSTEM RELOAD CONFIG");
  auto *opt = req.add_options();
  opt->set_op(rewriter::RewriteOp::TableNameRewrite);
  auto *dyn = opt->mutable_table_name_args()->mutable_dynamic_args();
  (*dyn->mutable_database_map())["db1"] = "phys";
  dyn->add_known_physical_databases("phys");
  dyn->set_delim("_");

  rewriter::RewriteSQLResponse resp;
  grpc::ServerContext ctx;
  ASSERT_TRUE(service.Rewrite(&ctx, &req, &resp).ok());
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
  EXPECT_EQ(resp.storage_integrity_contract_version(),
            rewriter::STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED);
}
```

- [ ] **Step 8: Rebuild and run the new test**

```bash
# rsync + rebuild, then:
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='StorageIntegrityCatchAll.*'"
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add tests/testdata/storage_integrity_cases.json tests/rewriter_test.cc \
  src/handlers/storage_integrity.h src/handlers/storage_integrity.cc src/rewriter-server.cc
git commit -m "fix(storage-integrity): fail closed on the unmodelled-statement dispatch tail (Spec I D1)"
```

### Task 9: D2 — the C++ reject annotator

**Files:**
- Modify: `src/handlers/storage_integrity.h`, `src/handlers/storage_integrity.cc`, `src/rewriter-server.cc`

**Interfaces:**
- Produces: `void rewriter_handlers::annotateStorageIntegrityReject(rewriter::RewriteSQLResponse *response, const std::string &sql, const rewriter::RewriteTableDynamicArgs &args)` — the exact mirror of Go's `handlers.AnnotateStorageIntegrityReject` (Task 2): message-only, skips `Success`, skips responses that already carry an SI-flagged accessed table, uses the same lexical name-run scan so string literals cannot trigger it.

- [ ] **Step 1: Confirm the ten D2 cases are red**

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_system_*:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_check_table_logical_rejected:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_truncate_*:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_alter_database_safe_rejected:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_drop_dictionary_safe_rejected:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_create_live_view_over_si_rejected'"
```

Expected: 10 tests, all FAIL on the message.

- [ ] **Step 2: Declare the annotator**

In `src/handlers/storage_integrity.h`:

```cpp
// Upgrade an already-rejected response's message so it names the
// storage-integrity object the statement addressed (Spec I D2). Message-only:
// Success is never touched, a response that already carries an SI-flagged
// accessed table keeps its own message, and a statement that names nothing
// storage-integrity keeps the caller's message.
void annotateStorageIntegrityReject(rewriter::RewriteSQLResponse *response,
  const std::string &sql,
  const rewriter::RewriteTableDynamicArgs &args);
```

- [ ] **Step 3: Implement it**

In `src/handlers/storage_integrity.cc`, in the exported part of the namespace (it must come after the anonymous-namespace helpers `tokenizeSQL`, `identifierToken`, `targetAt` it uses):

```cpp
void annotateStorageIntegrityReject(rewriter::RewriteSQLResponse *response,
  const std::string &sql,
  const rewriter::RewriteTableDynamicArgs &args) {
  if (!response || response->code() == rewriter::RewriteCode::Success) return;
  if (!args.has_storage_integrity() || args.storage_integrity().tables().empty()) return;
  for (const auto &entry : response->original_accessed_tables())
    if (entry.is_storage_integrity()) return;

  const auto tokens = tokenizeSQL(sql);
  for (size_t i = 0; i < tokens.size(); ++i) {
    if (!identifierToken(tokens[i])) continue;
    const auto target = targetAt(tokens, i);
    if (!target) continue;
    if (!target->database.empty()) {
      i = target->next - 1;  // consume `db . table`
      if (isStorageIntegrityPhysicalDatabase(target->database, args)) {
        response->set_message(
          storageIntegrityPhysicalRejectMessage(target->database + "." + target->table));
        return;
      }
      if (auto hit = lookupStorageIntegrity(target->database, target->table, args)) {
        response->set_message(storageIntegrityWriteRejectMessage(hit->logical_key));
        return;
      }
      continue;
    }
    if (isStorageIntegrityPhysicalDatabase(target->table, args)) {
      // A bare name in a database position: TRUNCATE DATABASE hg_safe,
      // TRUNCATE ALL TABLES FROM hg_unsafe, ALTER DATABASE hg_safe ...
      response->set_message(storageIntegrityPhysicalDatabaseRejectMessage(target->table));
      return;
    }
    if (auto hit = lookupStorageIntegrity("", target->table, args)) {
      response->set_message(storageIntegrityWriteRejectMessage(hit->logical_key));
      return;
    }
  }
}
```

- [ ] **Step 4: Run it on every exit path of `doRewrite`**

In `src/rewriter-server.cc`, immediately after the `StorageIntegrityRestoreGuard _si_restore{...}` declaration (`:379-385`), add a second scope guard. Declaration order matters: this one is declared later, so it destructs **before** the restore guard and before the `AccessLog`, which therefore logs the final code.

```cpp
  // Spec I D2: one place upgrades every reject's message to name the SI object
  // it touched, on every exit path, so no handler has to grow its own SI
  // branch. Mirrors rewriter-go's finalize() hook.
  struct StorageIntegrityRejectAnnotator {
    RewriteSQLResponse *response;
    const std::string *sql;
    const rewriter::RewriteTableDynamicArgs *args;  // null when SI is inactive
    ~StorageIntegrityRejectAnnotator() {
      if (!args) return;
      rewriter_handlers::annotateStorageIntegrityReject(response, *sql, *args);
    }
  } _si_annotate{response, &original_query,
                 si_active ? si_selection.dynamic_args : nullptr};
```

- [ ] **Step 5: Rebuild and verify the ten cases pass**

Rsync + rebuild, then re-run the Step 1 filter.

Expected: 10 tests, all PASS.

- [ ] **Step 6: Full suite check for collateral damage**

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.*' 2>&1 | tail -30"
```

Expected: only the Task 10-13 red set remains (escaping, reserved-column scoping, CTE, PREWHERE, literal mangling, and the seven wrapper-message cases). No previously-green case may have turned red — if one did, the annotator is over-reaching (tighten it; do **not** edit the corpus).

- [ ] **Step 7: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add src/handlers/storage_integrity.h src/handlers/storage_integrity.cc src/rewriter-server.cc
git commit -m "fix(storage-integrity): name the SI object in every reject message (Spec I D2)"
```

### Task 10: D4 + D7d — literal correctness in the C++ engine

Both halves are literal-handling bugs in the same code path: one builds a literal, the other rewrites text that may contain one.

**Files:**
- Modify: `src/handlers/common.h:42-50` (`escapeSqlLiteral`)
- Modify: `src/handlers/storage_integrity.cc` (`restoreStorageIntegrityProjectionSyntax`, ~`:768-811`)
- Test: `tests/rewriter_test.cc`

- [ ] **Step 1: Confirm the three cases are red**

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_excluded_part*:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_string_literal_except_is_not_mutated'"
```

Expected: 3 FAIL (`si_excluded_parts_escaping`, `si_excluded_part_trailing_backslash_cannot_close_literal`, `si_string_literal_except_is_not_mutated`).

- [ ] **Step 2: Fix `escapeSqlLiteral`**

Replace `src/handlers/common.h:38-50` (comment included — the old comment explicitly documents the bug):

```cpp
// Escape `s` so it can be embedded inside a single-quoted ClickHouse string
// literal. ClickHouse honours BOTH '' and \' as an escaped quote, so a value
// ending in a backslash would otherwise escape the closing quote and swallow
// whatever follows. Backslashes are doubled BEFORE quotes (Spec I D4).
inline std::string escapeSqlLiteral(const std::string &s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    if (c == '\\') out += "\\\\";
    else if (c == '\'') out += "''";
    else out += c;
  }
  return out;
}
```

- [ ] **Step 3: Make the projection fixup literal-aware**

Replace the scan loop at the end of `restoreStorageIntegrityProjectionSyntax` (everything from `const std::string needle = ...` to the closing `return sql;`) with a single left-to-right pass that copies quoted runs verbatim:

```cpp
  const std::string needle = "EXCEPT " + reserved_column;
  const std::string replacement = "EXCEPT (" + rendered + ")";
  std::string out;
  out.reserve(sql.size());
  size_t i = 0;
  while (i < sql.size()) {
    const char c = sql[i];
    // Copy a quoted run (string literal or quoted identifier) verbatim: the
    // textual fixup must never reach inside caller data (Spec I D7d).
    if (c == '\'' || c == '`' || c == '"') {
      const char quote = c;
      out += sql[i++];
      while (i < sql.size()) {
        if (sql[i] == '\\' && i + 1 < sql.size()) {
          out += sql[i];
          out += sql[i + 1];
          i += 2;
          continue;
        }
        if (sql[i] == quote) {
          out += sql[i++];
          if (i < sql.size() && sql[i] == quote) {  // '' / `` / "" escape
            out += sql[i++];
            continue;
          }
          break;
        }
        out += sql[i++];
      }
      continue;
    }
    if (sql.compare(i, needle.size(), needle) == 0) {
      const size_t after = i + needle.size();
      if (after == sql.size()
          || !(std::isalnum(static_cast<unsigned char>(sql[after])) || sql[after] == '_')) {
        out += replacement;
        i = after;
        continue;
      }
    }
    out += sql[i++];
  }
  return out;
```

The `keywords` set and the `rendered` computation above it are unchanged.

- [ ] **Step 4: Add the direct unit tests**

Append to `tests/rewriter_test.cc` (outside the SI golden anonymous namespace):

```cpp
TEST(StorageIntegrityLiterals, EscapeSqlLiteralEscapesBackslashBeforeQuote) {
  EXPECT_EQ(rewriter_handlers::escapeSqlLiteral("plain_1_1_0"), "plain_1_1_0");
  EXPECT_EQ(rewriter_handlers::escapeSqlLiteral("o'brien"), "o''brien");
  EXPECT_EQ(rewriter_handlers::escapeSqlLiteral("back\\slash"), "back\\\\slash");
  EXPECT_EQ(rewriter_handlers::escapeSqlLiteral("evil\\"), "evil\\\\");
  EXPECT_EQ(rewriter_handlers::escapeSqlLiteral("mix\\'ed"), "mix\\\\''ed");
}

TEST(StorageIntegrityLiterals, ProjectionRestoreSkipsStringLiterals) {
  EXPECT_EQ(rewriter_handlers::restoreStorageIntegrityProjectionSyntax(
              "SELECT * EXCEPT _hg_row_id FROM hg_safe.db1__t", "_hg_row_id"),
            "SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t");
  EXPECT_EQ(rewriter_handlers::restoreStorageIntegrityProjectionSyntax(
              "SELECT 'EXCEPT _hg_row_id' AS s FROM t", "_hg_row_id"),
            "SELECT 'EXCEPT _hg_row_id' AS s FROM t");
  EXPECT_EQ(rewriter_handlers::restoreStorageIntegrityProjectionSyntax(
              "SELECT 'a''EXCEPT _hg_row_id' AS s, * EXCEPT _hg_row_id FROM t", "_hg_row_id"),
            "SELECT 'a''EXCEPT _hg_row_id' AS s, * EXCEPT (_hg_row_id) FROM t");
}
```

- [ ] **Step 5: Rebuild and verify**

```bash
# rsync + rebuild, then:
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='StorageIntegrityLiterals.*:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_excluded_part*:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_string_literal_except_is_not_mutated'"
```

Expected: all PASS. Note that `si_excluded_parts_escaping`'s `want_sql` is not yet compared in C++ (Spec J D3) — its `want_sql_contains` entries are, and those are exactly the escaped literals.

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add src/handlers/common.h src/handlers/storage_integrity.cc tests/rewriter_test.cc
git commit -m "fix(storage-integrity): escape backslashes and stop mutating string literals (Spec I D4, D7d)"
```

### Task 11: D7a — scope the C++ reserved-column check

**Files:**
- Modify: `src/handlers/storage_integrity.h`, `src/handlers/storage_integrity.cc`
- Modify: `src/handlers/select.cc:971-977` (the reserved-column guard inside `handleSelectQuery`)

**Interfaces:**
- Produces: `bool rewriter_handlers::astReferencesIdentifierInScope(const DB::ASTPtr &ast, const std::string &name, const rewriter::RewriteTableDynamicArgs &args)` — same contract as Go's `engine.ReferencesIdentifierInScope` with `protected` bound to `lookupStorageIntegrity`. `astReferencesIdentifier` stays (it has other callers in `rewriteEmbeddedViewBody`-adjacent paths and its own tests).

- [ ] **Step 1: Confirm the case is red**

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_reserved_column_*'"
```

Expected: `si_reserved_column_on_ordinary_table_in_subquery_allowed` FAILs; `si_reserved_column_qualified_by_si_alias_rejected` and every pre-existing `si_reserved_column_*` case PASSes.

- [ ] **Step 2: Declare the scoped check**

In `src/handlers/storage_integrity.h`, beside `astReferencesIdentifier`:

```cpp
// Scope-aware variant of astReferencesIdentifier: a reference counts only when
// the query block owning it reads a storage-integrity table, or when its
// qualifier is bound to one in that block or an enclosing one. An identically
// named column on an ordinary table is legitimate (Spec I D7a).
bool astReferencesIdentifierInScope(const DB::ASTPtr &ast,
  const std::string &name,
  const rewriter::RewriteTableDynamicArgs &args);
```

- [ ] **Step 3: Implement it**

In `src/handlers/storage_integrity.cc`, add `#include <Parsers/ASTSelectQuery.h>` (Task 8 already added it) and, in the anonymous namespace, the block-scope helpers plus the exported entry point:

```cpp
using StorageIntegrityBindings = std::map<std::string, bool>;

// Split a table identifier's "db.table" (or bare "table") at the first dot,
// the same split collectAccessedTablePairsFromAST uses.
std::pair<std::string, std::string> splitTableIdentifier(const std::string &full) {
  if (const auto dot = full.find('.'); dot != std::string::npos)
    return {full.substr(0, dot), full.substr(dot + 1)};
  return {std::string(), full};
}

// Collect one select block's own table bindings (alias, else table name) and
// report whether the block reads a storage-integrity table. Nested blocks own
// their own scope and are not descended into.
bool storageIntegrityBlockScope(const DB::ASTSelectQuery &select,
  const rewriter::RewriteTableDynamicArgs &args,
  StorageIntegrityBindings &bindings) {
  bool block_protected = false;
  const auto tables = const_cast<DB::ASTSelectQuery &>(select).tables();
  const auto *list = tables ? tables->as<DB::ASTTablesInSelectQuery>() : nullptr;
  if (!list) return false;
  for (const auto &child : list->children) {
    const auto *element = child->as<DB::ASTTablesInSelectQueryElement>();
    if (!element || !element->table_expression) continue;
    const auto *expr = element->table_expression->as<DB::ASTTableExpression>();
    if (!expr || !expr->database_and_table_name) continue;
    const auto *id = expr->database_and_table_name->as<DB::ASTTableIdentifier>();
    if (!id) continue;
    const auto [database, table] = splitTableIdentifier(id->name());
    const bool si = lookupStorageIntegrity(database, table, args).has_value();
    block_protected = block_protected || si;
    std::string key = expr->database_and_table_name->tryGetAlias();
    if (key.empty()) key = table;
    if (!key.empty()) bindings[key] = si;
  }
  return block_protected;
}

bool referencesIdentifierInScope(const DB::ASTPtr &ast,
  const std::string &name,
  const rewriter::RewriteTableDynamicArgs &args,
  StorageIntegrityBindings bindings,
  bool block_protected) {
  if (!ast) return false;
  if (const auto *select = ast->as<DB::ASTSelectQuery>()) {
    StorageIntegrityBindings next = bindings;
    const bool next_protected = storageIntegrityBlockScope(*select, args, next);
    for (const auto &child : select->children)
      if (referencesIdentifierInScope(child, name, args, next, next_protected)) return true;
    return false;
  }
  if (const auto *identifier = ast->as<DB::ASTIdentifier>();
      identifier && !ast->as<DB::ASTTableIdentifier>() && identifier->shortName() == name) {
    // A compound reference (`alias.col`, `db.table.col`) is decided by its
    // qualifier when that qualifier is bound in scope; otherwise it falls back
    // to the owning block.
    const auto &parts = identifier->name_parts;
    if (parts.size() >= 2) {
      const auto it = bindings.find(parts[parts.size() - 2]);
      if (it != bindings.end()) return it->second;
    }
    return block_protected;
  }
  if (const auto *replacement = ast->as<DB::ASTColumnsReplaceTransformer::Replacement>();
      replacement && replacement->name == name)
    return block_protected;
  if (!ast->tryGetAlias().empty() && ast->tryGetAlias() == name) return block_protected;
  for (const auto &child : ast->children)
    if (referencesIdentifierInScope(child, name, args, bindings, block_protected)) return true;
  return false;
}
```

and, in the exported part of the namespace:

```cpp
bool astReferencesIdentifierInScope(const DB::ASTPtr &ast,
  const std::string &name,
  const rewriter::RewriteTableDynamicArgs &args) {
  return referencesIdentifierInScope(ast, name, args, StorageIntegrityBindings{}, false);
}
```

- [ ] **Step 4: Switch the caller over**

In `src/handlers/select.cc`, replace the guard at `:971-977`:

```cpp
    if (responseTouchesStorageIntegrity(*response)
        && astReferencesIdentifierInScope(ast, reservedRowIdColumn(args), args)) {
      const std::string rid = reservedRowIdColumn(args);
      response->set_code(rewriter::RewriteCode::RewriteError);
      response->set_message("reserved column " + rid + " is not addressable");
      return;
    }
```

- [ ] **Step 5: Rebuild and verify**

```bash
# rsync + rebuild, then:
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_reserved_column_*:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_union_reserved_rejected:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_except_reserved_column_rejected:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_reserved_output_alias_rejected'"
```

Expected: every listed case PASSes, including the newly-allowed subquery case.

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add src/handlers/storage_integrity.h src/handlers/storage_integrity.cc src/handlers/select.cc
git commit -m "fix(storage-integrity): scope the reserved-column check to SI tables (Spec I D7a)"
```

### Task 12: D7b — make the C++ namespace collector CTE-aware

**Files:**
- Modify: `src/handlers/storage_integrity.cc` (`collectNamespaceRefs` at `:517-542`, `collectStorageIntegrityNamespaceRefs` at `:1183-1187`)

**Interfaces:**
- `collectStorageIntegrityNamespaceRefs(const DB::ASTPtr &)` keeps its signature; the CTE scope is internal, mirroring Go Task 5.

- [ ] **Step 1: Confirm the case is red**

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_cte_*:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_table_inside_cte_body_rewritten'"
```

Expected: `si_cte_alias_shadowing_si_name_in_in_clause_allowed` FAILs with `code = RewriteError`; the other two PASS.

- [ ] **Step 2: Implement the scope**

In `src/handlers/storage_integrity.cc`, add `#include <Parsers/ASTWithElement.h>` and `#include <unordered_set>`, then, in the anonymous namespace above `collectNamespaceRefs`:

```cpp
// Local mirror of select.cc's collectInlineCTEAliases: every WITH element name
// declared by this select block. Kept local so the namespace collector does not
// depend on the SELECT handler's internals.
bool storageIntegrityCTEAliases(
  const DB::ASTSelectQuery *select, std::unordered_set<std::string> &out_aliases) {
  if (!select) return false;
  const auto with_expr = const_cast<DB::ASTSelectQuery *>(select)->with();
  if (!with_expr) return false;
  bool added = false;
  for (const auto &child : with_expr->children) {
    if (const auto *element = child->as<DB::ASTWithElement>()) {
      if (!element->name.empty()) {
        out_aliases.insert(element->name);
        added = true;
      }
    }
  }
  return added;
}
```

Change `collectNamespaceRefs` to carry the scope (the existing two-argument form becomes a thin wrapper so no other caller changes):

```cpp
void collectNamespaceRefs(const DB::ASTPtr &ast, std::vector<NamespaceRef> &out,
  const std::unordered_set<std::string> &cte_scope) {
  if (!ast) return;

  std::unordered_set<std::string> extended;
  const std::unordered_set<std::string> *scope = &cte_scope;
  if (const auto *select = ast->as<DB::ASTSelectQuery>()) {
    extended = cte_scope;
    if (storageIntegrityCTEAliases(select, extended)) scope = &extended;
  }

  if (const auto *create = ast->as<DB::ASTCreateQuery>()) {
    /* ...unchanged engine + dictionary-source blocks... */
  }
  if (const auto *fn = ast->as<DB::ASTFunction>()) {
    if (auto ref = decodeNamespaceFunction(*fn, NamespaceRefSource::TableFunction))
      out.push_back(*ref);
    if (auto ref = decodeInFunction(*fn)) {
      // A bare `IN <alias>` whose name is an in-scope CTE alias is a CTE
      // reference, not a table (Spec I D7b). Qualified `db.alias` stays a real
      // table, exactly as collectAccessedTablePairsFromAST treats it.
      if (!(ref->database.empty() && scope->count(ref->table) > 0)) out.push_back(*ref);
    }
  }
  for (const auto &child : ast->children) collectNamespaceRefs(child, out, *scope);
}

void collectNamespaceRefs(const DB::ASTPtr &ast, std::vector<NamespaceRef> &out) {
  static const std::unordered_set<std::string> kEmpty;
  collectNamespaceRefs(ast, out, kEmpty);
}
```

Keep the `ASTCreateQuery` engine/dictionary blocks exactly as they are today — only the `ASTSelectQuery` scope fork, the `decodeInFunction` guard, and the recursion's third argument are new.

- [ ] **Step 3: Rebuild and verify**

```bash
# rsync + rebuild, then:
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_cte_*:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_table_inside_cte_body_rewritten:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_in_*'"
```

Expected: all PASS — in particular every pre-existing `si_in_*` case (bare and qualified `IN` targets with **no** CTE in scope) must stay red-free.

- [ ] **Step 4: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add src/handlers/storage_integrity.cc
git commit -m "fix(storage-integrity): make the namespace collector CTE-aware (Spec I D7b)"
```

### Task 13: D7c — reject `PREWHERE` on an SI table in the C++ engine

**Files:**
- Modify: `src/handlers/storage_integrity.h:18-19` (`kStorageIntegrityWrapperMessage`)
- Modify: `src/handlers/storage_integrity.cc` (`prepareStorageIntegritySQL`, after the WITH OFFSET loop that ends at ~`:978`)

- [ ] **Step 1: Confirm the red set**

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_prewhere_*:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_mixed_ordinary_prewhere_allowed:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_final_rejected:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_sample_rejected:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_with_offset_*:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_alias_column_list_rejected:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_comma_si_with_offset_rejected'"
```

Expected: `si_prewhere_rejected` FAILs on the code; the seven wrapper cases FAIL on the message; `si_mixed_ordinary_prewhere_allowed` and `si_prewhere_literal_allowed` PASS.

- [ ] **Step 2: Update the shared wrapper message**

In `src/handlers/storage_integrity.h`:

```cpp
constexpr std::string_view kStorageIntegrityWrapperMessage =
  "FINAL/SAMPLE/PREWHERE/WITH OFFSET/column aliases on storage-integrity tables are not supported";
```

- [ ] **Step 3: Add the lexical PREWHERE pre-reject**

In `prepareStorageIntegritySQL`, immediately after the WITH OFFSET loop closes (right before the `// Likewise, strip an ordinary table's alias column list …` comment), insert:

```cpp
  // PREWHERE is a query-level clause evaluated against the FROM clause's main
  // table, so bind it to the first target after the nearest preceding FROM at
  // the same depth (Spec I D7c). Rejecting here keeps the physical names and
  // the reserved column out of the raw ClickHouse error the client would
  // otherwise receive. Token-based, so a string literal cannot trigger it.
  for (size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i].kind != SQLToken::Kind::Word || tokens[i].upper != "PREWHERE") continue;
    std::optional<LexicalTarget> bound;
    for (size_t j = i; j > 0; --j) {
      const auto &candidate = tokens[j - 1];
      if (candidate.depth != tokens[i].depth) continue;
      if (candidate.upper == "FROM") {
        bound = targetAt(tokens, j);
        break;
      }
    }
    if (!bound) continue;
    if (targetTouchesStorageIntegrity(*bound, args)) {
      return reject_select_surface(*bound, std::string(kStorageIntegrityWrapperMessage));
    }
  }
```

`tokens[i].kind != SQLToken::Kind::Word` is what keeps `SELECT 'PREWHERE' FROM db1.t` out: a single-quoted run lexes as `Kind::String` (`storage_integrity.cc:291-298`).

- [ ] **Step 4: Rebuild and verify**

Rsync + rebuild, then re-run the Step 1 filter.

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add src/handlers/storage_integrity.h src/handlers/storage_integrity.cc
git commit -m "fix(storage-integrity): reject PREWHERE on SI tables with the wrapper message (Spec I D7c)"
```

### Task 14: Cross-engine parity gate

Spec I §5 requires both engines green on the whole corpus **and** the Go oracle diff green across it.

**Files:** none (verification only).

- [ ] **Step 1: Full C++ suite**

```bash
# rsync + rebuild, then:
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ctest --test-dir build --output-on-failure"
```

Expected: 100% tests passed, 0 failed.

- [ ] **Step 2: Prove the two corpus copies are still byte-identical**

```bash
cmp /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json \
    /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json && echo IDENTICAL
shasum -a 256 /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json \
              /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json
diff <(shasum -a 256 /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json | cut -d' ' -f1) \
     <(cut -d' ' -f1 /tmp/si_corpus_sha.txt) && echo "MATCHES TASK 7 CHECKSUM"
```

Expected: `IDENTICAL`, two identical sha256 values, and `MATCHES TASK 7 CHECKSUM`. If the Go copy drifted after Task 7, re-copy to C++ and re-run Step 1.

- [ ] **Step 3: Run the C++ server as the Go oracle**

On the build box, start the freshly built server (default gRPC port 50051), then from the Go repo:

```bash
ssh -p 30100 -f -N -L 50051:127.0.0.1:50051 sentio@64.38.131.242
ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && nohup ./build/clickhousegate_rewriter 50051 50052 >/tmp/rewriter.log 2>&1 &"
cd /Users/uranuswch/Dev/housegate/rewriter-go
REWRITER_ORACLE_ADDR=127.0.0.1:50051 make test
```

Expected: every package `ok`. `TestStorageIntegrityGolden` now diffs every case's structured fields (and SQL, except where `allow_sql_divergence` is set) against the C++ engine. Any `oracle divergence:` line is a real Go/C++ behaviour difference and must be fixed in the engine that is wrong — never by loosening the case.

Tear down the tunnel and the server when finished.

- [ ] **Step 4: Record the parity result**

Note in the PR body: corpus case count (201), sha256, "C++ ctest 100% pass", "Go harness green with REWRITER_ORACLE_ADDR against rewriter-grpc @ `<commit>`".

### Task 15: Release both engines in dependency order

Spec G plan D-10's publication rule: the corpus must never be published from only one engine, and rewriter-go releases first.

**Files:** none (release workflows).

- [ ] **Step 1: Merge and release rewriter-go**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
gh pr create --fill --title "fix(storage-integrity): surface fail-closed hardening (Spec I)"
```

After merge to `main`, run the repo's `release` workflow from `main` and capture the tag it prints:

```bash
gh workflow run release.yml --ref main --repo housegate/rewriter-go
gh release list --repo housegate/rewriter-go --limit 3
```

Expected: a new tag ≥ `v0.7.2`. Record it as `<rewriter-go-tag>` — Task 20 pins both the Go module and the FFI asset to it.

- [ ] **Step 2: Verify the FFI asset exists for the new tag**

```bash
gh release view <rewriter-go-tag> --repo housegate/rewriter-go --json assets --jq '.assets[].name'
```

Expected: `libpolyglot_sql_ffi.so` / `libpolyglot_sql_ffi.dylib` and `SHA256SUMS` present. The native engine's FFI binary is a separate artifact from the Go module — housegate needs both.

- [ ] **Step 3: Merge and release rewriter-grpc**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
gh pr create --fill --title "fix(storage-integrity): surface fail-closed hardening (Spec I)"
```

After merge, run the `cut-release` workflow (Actions → cut-release → Run workflow) — do **not** bump versions by hand. It tags `main` and dispatches `release.yml`, which builds and pushes the docker image on the build host.

```bash
gh release list --repo housegate/rewriter-grpc --limit 3
```

Expected: a new tag ≥ `v0.12.2`. Record it as `<rewriter-grpc-tag>` for the Spec I closure note (Task 23).

---

## Part C — housegate

**Working directory for every Part C task:** `/Users/uranuswch/Dev/housegate/housegate`

- [ ] **Task 15b (pre-flight, do once):** branch and prove the baseline.

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git checkout -b feat/si-surface-failclosed
bazel test //... --test_output=errors
```

Expected: all non-`manual` targets pass. Record any failure now — it is the baseline, not your regression.

### Task 16: D3 — any non-`Success` is a rejection when SI tables are configured

Today's gate (`pkg/rewriter/sentio.go:327-335`) only fires when some `original_accessed_tables` entry carries `is_storage_integrity`. Engine paths that reject *before* recording a target (`TRUNCATE DATABASE hg_safe`, `ALTER DATABASE`, `DROP DICTIONARY`, `CREATE LIVE VIEW`) produce neither, so `sentio.go:349-366` returns the original SQL with a nil error and the statement reaches ClickHouse. Part A/B fix the engines; this task makes HouseGate safe even against an old or regressed engine.

**Files:**
- Modify: `pkg/rewriter/sentio.go` (between the SI-flag block at `:327-335` and the `switch resp.Code` at `:336`)
- Test: `pkg/rewriter/backend_test.go`

**Interfaces:**
- Consumes: `RejectedError`, `storageIntegrityAccess` (`pkg/rewriter/storage_integrity.go:82-156`), `newSIFactory` / `acknowledgedSIResponse` / `fakeBackend` (`pkg/rewriter/backend_test.go:15-71,254-264`).
- Produces: no new exported symbol; the behaviour change is that `sentioRewriter.Rewrite` returns `*RejectedError` for every non-`Success` code while `Options.StorageIntegrity.Tables` is non-empty.

- [ ] **Step 1: Write the failing test**

Append to `pkg/rewriter/backend_test.go`:

```go
func TestSentioRewriter_FailsClosedOnAnyNonSuccessWhenSIConfigured(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     pb.RewriteCode
		accessed []*pb.AccessedTable
	}{
		{"unsupported with no accessed table", pb.RewriteCode_UnsupportedStatement, nil},
		{"rewrite error with no accessed table", pb.RewriteCode_RewriteError, nil},
		{"syntax error with no accessed table", pb.RewriteCode_SyntaxError, nil},
		{"invalid request with no accessed table", pb.RewriteCode_InvalidRewriteRequest, nil},
		{"unsupported naming an ordinary table", pb.RewriteCode_UnsupportedStatement,
			[]*pb.AccessedTable{{OriginalDatabase: "other", OriginalTable: "u"}}},
		{"unsupported naming an SI table", pb.RewriteCode_UnsupportedStatement,
			[]*pb.AccessedTable{{OriginalDatabase: "db1", OriginalTable: "t", IsStorageIntegrity: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
				Code:                   tc.code,
				Message:                "storage-integrity physical database hg_safe is not directly addressable",
				SqlAfterRewrite:        "TRUNCATE DATABASE hg_safe",
				OriginalAccessedTables: tc.accessed,
			})}
			_, err := newSIFactory(be, nil, true).NewRewriter(&fakeSession{}).
				Rewrite(context.Background(), "TRUNCATE DATABASE hg_safe", "")
			var rej *RejectedError
			if !errors.As(err, &rej) {
				t.Fatalf("err = %v, want *RejectedError — a configured SI surface must never forward a non-Success answer", err)
			}
			if rej.Code != tc.code {
				t.Fatalf("rejected code = %v, want %v", rej.Code, tc.code)
			}
			if !strings.Contains(rej.Message, "hg_safe is not directly addressable") {
				t.Fatalf("rejected message = %q, want the engine message", rej.Message)
			}
		})
	}
}

func TestSentioRewriter_EmptySIKeepsUnsupportedPassthrough(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code: pb.RewriteCode_UnsupportedStatement, Message: "nope",
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "other", OriginalTable: "u"}}}}
	res, err := newFakeFactory(be).NewRewriter(&fakeSession{}).
		Rewrite(context.Background(), "OPTIMIZE TABLE other.u", "")
	if err != nil {
		t.Fatalf("empty-SI deployments keep the legacy pass-through: %v", err)
	}
	if res.SQL != "OPTIMIZE TABLE other.u" {
		t.Fatalf("SQL = %q, want the original statement", res.SQL)
	}
}
```

- [ ] **Step 2: Run to verify the first test fails**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //pkg/rewriter:rewriter_test --test_filter='TestSentioRewriter_(FailsClosedOnAnyNonSuccess|EmptySIKeeps)' --test_output=all --nocache_test_results
```

Expected: `TestSentioRewriter_FailsClosedOnAnyNonSuccessWhenSIConfigured` FAILs on the four "no accessed table" and the "ordinary table" subtests (`err = <nil>, want *RejectedError`); the SI-table subtest and `TestSentioRewriter_EmptySIKeepsUnsupportedPassthrough` pass.

- [ ] **Step 3: Implement**

In `pkg/rewriter/sentio.go`, insert immediately after the existing SI-flag block (`:327-335`) and before `switch resp.Code`:

```go
	// Spec I D3: with a configured SI surface, ANY non-Success answer is a
	// rejection — not just one whose accessed tables carry the SI flag. Several
	// engine paths reject before they record a target (TRUNCATE DATABASE,
	// TRUNCATE ALL TABLES FROM, ALTER DATABASE, DROP DICTIONARY, CREATE LIVE
	// VIEW), so a flag-keyed gate forwards those statements verbatim. The flag
	// path above stays because it produces the more specific message and owns
	// the INSERT-lane decision. Defence in depth: this holds even against an
	// engine build that regressed D1.
	if len(r.factory.options.StorageIntegrity.Tables) > 0 && resp.GetCode() != pb.RewriteCode_Success {
		return RewriteResult{}, &RejectedError{Code: resp.GetCode(), Message: resp.GetMessage()}
	}
```

- [ ] **Step 4: Run to verify pass**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //pkg/rewriter:rewriter_test --test_output=errors --nocache_test_results
```

Expected: PASS, including the pre-existing `TestSentioRewriter_StorageIntegrityRejectIsFailClosed` (its non-SI `OPTIMIZE TABLE other.u` sub-assertion runs with SI **configured**, so it now expects a rejection — if it fails, update that one sub-assertion to `errors.As(err, &rej)` and note the intentional change in the commit message: it is exactly the behaviour D3 changes).

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/rewriter/sentio.go pkg/rewriter/backend_test.go
git commit -m "fix(storage-integrity): fail closed on any non-Success rewrite response (Spec I D3)"
```

### Task 17: D7e — scrub SI physical names and the reserved column from Exception text

A rewritten SI read executes against `hg_safe.<phys>` / `hg_unsafe.<phys>`; when ClickHouse errors, its message quotes those names and `_hg_row_id` straight back to the client — the names Spec G D-9 declares protocol-owned and unaddressable.

**Files:**
- Modify: `pkg/rewriter/storage_integrity.go`
- Modify: `pkg/plugins/rewrite/rewriter.go` (`OnException` at `:238-256`, plus one new field)
- Modify: `build.go` (`:663-665`)
- Test: `pkg/rewriter/storage_integrity_test.go`, `pkg/plugins/rewrite/rewriter_test.go`

**Interfaces:**
- Produces: `type rewriter.StorageIntegrityScrubber struct{...}`, `func rewriter.NewStorageIntegrityScrubber(opts StorageIntegrityOptions) *StorageIntegrityScrubber` (nil when no SI tables), `func (*StorageIntegrityScrubber) Scrub(msg string) string` (nil-receiver safe), and the new plugin field `rewrite.Plugin.StorageIntegrityScrubber *rewriter.StorageIntegrityScrubber`.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/rewriter/storage_integrity_test.go`:

```go
func TestStorageIntegrityScrubber(t *testing.T) {
	s := NewStorageIntegrityScrubber(StorageIntegrityOptions{Tables: []StorageIntegrityTable{
		{TableID: "db1.t", SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
	}})
	for _, tc := range []struct{ in, want string }{
		{"Table hg_safe.db1__t does not exist", "Table db1.t does not exist"},
		{"Missing columns: '_hg_row_id' while processing hg_unsafe.db1__t",
			"Missing columns: '<storage-integrity>' while processing db1.t"},
		{"Database hg_safe does not exist", "Database <storage-integrity> does not exist"},
		{"Table other.u does not exist", "Table other.u does not exist"},
		{"", ""},
	} {
		if got := s.Scrub(tc.in); got != tc.want {
			t.Errorf("Scrub(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	var none *StorageIntegrityScrubber
	if got := none.Scrub("Table hg_safe.db1__t does not exist"); got != "Table hg_safe.db1__t does not exist" {
		t.Errorf("a nil scrubber must be a no-op, got %q", got)
	}
	if NewStorageIntegrityScrubber(StorageIntegrityOptions{}) != nil {
		t.Error("an empty SI surface must produce no scrubber")
	}
}
```

Append to `pkg/plugins/rewrite/rewriter_test.go`:

```go
func TestOnException_ScrubsStorageIntegrityNames(t *testing.T) {
	rw := &fakeRewriter{out: "REWRITTEN-SQL"}
	p := &Plugin{
		Factory: &fakeFactory{rw: rw},
		StorageIntegrityScrubber: rewriter.NewStorageIntegrityScrubber(rewriter.StorageIntegrityOptions{
			Tables: []rewriter.StorageIntegrityTable{
				{TableID: "db1.t", SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
			}}),
	}
	sess := newSessionForTest(t, 1)

	exc := &chproto.Exception{Message: "Unknown identifier _hg_row_id in table hg_safe.db1__t"}
	if err := p.OnException(context.Background(), sess, exc); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	if strings.Contains(exc.Message, "hg_safe") || strings.Contains(exc.Message, "_hg_row_id") {
		t.Fatalf("protocol-owned names leaked to the client: %q", exc.Message)
	}
	if !strings.Contains(exc.Message, "db1.t") {
		t.Fatalf("the logical name must survive scrubbing: %q", exc.Message)
	}
}
```

Add `"strings"` and `"github.com/housegate/housegate/pkg/rewriter"` to that file's imports if they are not already there.

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //pkg/rewriter:rewriter_test //pkg/plugins/rewrite:rewrite_test --test_filter='StorageIntegrityScrub|ScrubsStorageIntegrityNames' --test_output=all --nocache_test_results
```

Expected: both FAIL to compile (`undefined: NewStorageIntegrityScrubber`, `unknown field StorageIntegrityScrubber`). A compile failure is a valid red for a new symbol.

- [ ] **Step 3: Implement the scrubber**

Append to `pkg/rewriter/storage_integrity.go` (and add `"sort"` to its imports):

```go
// storageIntegrityRedaction replaces a protocol-owned name that has no logical
// equivalent (a reserved database, the reserved row-id column).
const storageIntegrityRedaction = "<storage-integrity>"

// StorageIntegrityScrubber removes protocol-owned storage-integrity names from
// text that is about to reach a client (Spec I D7e). A rewritten SI read
// executes against hg_safe/hg_unsafe, so a ClickHouse error quotes those names
// and the reserved row-id column back at the user — names Spec G D-9 declares
// unaddressable. Qualified physical names map back to their logical table id;
// the reserved databases and the row-id column are redacted.
//
// This is deliberately a narrow, SI-scoped mapping, not a revival of the
// general ErrorReverseMap stub (pkg/plugins/rewrite/error_map.go).
type StorageIntegrityScrubber struct {
	replacer *strings.Replacer
}

// NewStorageIntegrityScrubber returns nil when no SI table is configured, so
// non-SI deployments pay nothing and Scrub stays a no-op.
func NewStorageIntegrityScrubber(opts StorageIntegrityOptions) *StorageIntegrityScrubber {
	if len(opts.Tables) == 0 {
		return nil
	}
	// Order matters: strings.Replacer matches patterns in argument order at each
	// position, so the qualified physical names must precede the bare database
	// names they start with.
	pairs := make([]string, 0, len(opts.Tables)*4+6)
	databases := map[string]bool{}
	for _, t := range opts.Tables {
		pairs = append(pairs, t.SafeTable, t.TableID, t.UnsafeTable, t.TableID)
		if db, _, ok := strings.Cut(t.SafeTable, "."); ok {
			databases[db] = true
		}
		if db, _, ok := strings.Cut(t.UnsafeTable, "."); ok {
			databases[db] = true
		}
	}
	names := make([]string, 0, len(databases))
	for db := range databases {
		names = append(names, db)
	}
	sort.Strings(names) // deterministic replacer construction
	for _, db := range names {
		pairs = append(pairs, db, storageIntegrityRedaction)
	}
	pairs = append(pairs, DefaultReservedRowIDColumn, storageIntegrityRedaction)
	return &StorageIntegrityScrubber{replacer: strings.NewReplacer(pairs...)}
}

// Scrub is safe on a nil receiver and on an empty message.
func (s *StorageIntegrityScrubber) Scrub(msg string) string {
	if s == nil || s.replacer == nil || msg == "" {
		return msg
	}
	return s.replacer.Replace(msg)
}
```

- [ ] **Step 4: Wire it into the plugin**

In `pkg/plugins/rewrite/rewriter.go`, add the field to `Plugin` (after `RequiredStorageIntegrityContractVersion`):

```go
	// StorageIntegrityScrubber removes protocol-owned SI names from Exception
	// text before it reaches the client (Spec I D7e). Nil disables scrubbing.
	StorageIntegrityScrubber *rewriter.StorageIntegrityScrubber
```

and change the head of `OnException`:

```go
func (p *Plugin) OnException(ctx context.Context, sess chsession.Session, exc *chproto.Exception) error {
	// Spec I D7e: SI physical names and the reserved row-id column are
	// protocol-owned and must not reach a client, including inside a raw
	// ClickHouse error. This runs before the reverse-map gate below because
	// scrubbing must not depend on whether this session had an active rewrite.
	if scrubbed := p.StorageIntegrityScrubber.Scrub(exc.Message); scrubbed != exc.Message {
		exc.Message = scrubbed
	}
	if !sess.State().Snapshot().HasActiveRewrite {
		return nil
	}
	// ... unchanged remainder ...
```

In `build.go`, inside the existing `if len(siOptions.Tables) > 0 {` block at `:663-665`:

```go
		if len(siOptions.Tables) > 0 {
			rewritePlug.RequiredStorageIntegrityContractVersion = rewriter.StorageIntegrityContractV1
			rewritePlug.StorageIntegrityScrubber = rewriter.NewStorageIntegrityScrubber(siOptions)
		}
```

- [ ] **Step 5: Run to verify pass**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //pkg/rewriter:rewriter_test //pkg/plugins/rewrite:rewrite_test //:housegate_test --test_output=errors --nocache_test_results
```

Expected: PASS. (`//:housegate_test` is the root-package target that owns `build_test.go`; if gazelle names it differently in your tree, use `bazel query 'tests(//:all)'` to find it.)

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/rewriter/storage_integrity.go pkg/rewriter/storage_integrity_test.go \
  pkg/plugins/rewrite/rewriter.go pkg/plugins/rewrite/rewriter_test.go build.go
git commit -m "fix(storage-integrity): scrub protocol-owned names from Exception text (Spec I D7e)"
```

### Task 18: D5 — prove the engine build at startup, not just the contract enum

`STORAGE_INTEGRITY_CONTRACT_V1` cannot distinguish rewriter-go v0.7.0 (broken DESCRIBE metadata names) from v0.7.1, nor rewriter-grpc v0.12.0 from v0.12.1 — and after this spec it cannot distinguish a pre-D1 build from a post-D1 build either. One fixed probe rewrite does.

**Files:**
- Create: `pkg/rewriter/probe.go`, `pkg/rewriter/probe_test.go`
- Modify: `build.go` (after the capable-factory check at `:445-450`)
- Test: `build_test.go`

**Interfaces:**
- Produces: `const rewriter.StorageIntegrityProbeExpectedSQL string`; `type rewriter.StorageIntegrityProbeFactory interface { Factory; ProbeStorageIntegrityBuild(ctx context.Context) error }`; `func (*SentioNetworkFactory) ProbeStorageIntegrityBuild(ctx context.Context) error`.
- Consumes: `rewriteOption` (`pkg/rewriter/args.go:47`), `SentioNetworkFactory.backend` / `.options` (`pkg/rewriter/sentio.go`).

- [ ] **Step 1: Write the failing test**

Create `pkg/rewriter/probe_test.go`:

```go
package rewriter

import (
	"context"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"
)

func TestProbeStorageIntegrityBuild(t *testing.T) {
	t.Run("correct build passes", func(t *testing.T) {
		be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			StatementType:   pb.StatementType_STATEMENT_TYPE_DESCRIBE,
			SqlAfterRewrite: StorageIntegrityProbeExpectedSQL,
		})}
		f := newSIFactory(be, nil, true)
		if err := f.ProbeStorageIntegrityBuild(context.Background()); err != nil {
			t.Fatalf("probe: %v", err)
		}
		si := be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
		if si.GetContractVersion() != StorageIntegrityContractV1 || si.GetTables()["db1.t"].GetSafeTable() != "hg_safe.db1__t" {
			t.Fatalf("probe request did not carry the fixed SI args: %v", si)
		}
	})

	t.Run("old build is refused", func(t *testing.T) {
		// rewriter-go v0.7.0 / rewriter-grpc v0.12.0 emitted metadata column
		// names that do not exist in ClickHouse 25.8 (Spec G plan D-11).
		be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			StatementType:   pb.StatementType_STATEMENT_TYPE_DESCRIBE,
			SqlAfterRewrite: "SELECT name, type, default_type, default_expression, comment, codec_expression, ttl_expression FROM system.columns WHERE database = 'hg_safe' AND table = 'db1__t' AND name != '_hg_row_id' ORDER BY position",
		})}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "storage-integrity engine probe") {
			t.Fatalf("err = %v, want a build-probe refusal", err)
		}
	})

	t.Run("missing acknowledgement is refused", func(t *testing.T) {
		be := &fakeBackend{resp: &pb.RewriteSQLResponse{
			Code: pb.RewriteCode_Success, SqlAfterRewrite: StorageIntegrityProbeExpectedSQL}}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "acknowledgement") {
			t.Fatalf("err = %v, want an acknowledgement refusal", err)
		}
	})

	t.Run("rejected probe is refused", func(t *testing.T) {
		be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code: pb.RewriteCode_UnsupportedStatement, Message: "nope"})}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "UnsupportedStatement") {
			t.Fatalf("err = %v, want a rejected-probe refusal", err)
		}
	})
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //pkg/rewriter:rewriter_test --test_filter='TestProbeStorageIntegrityBuild' --test_output=all --nocache_test_results
```

Expected: compile failure (`undefined: StorageIntegrityProbeExpectedSQL`).

- [ ] **Step 3: Implement the probe**

Create `pkg/rewriter/probe.go`:

```go
package rewriter

import (
	"context"
	"fmt"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// storageIntegrityProbeSQL is the fixed statement the startup build probe
// rewrites. DESCRIBE is chosen because both engines build its answer as a
// string, byte-identically, from the same shared corpus case
// (si_describe_metadata_select) — so the expected output is an exact,
// version-sensitive fingerprint of the engine build.
const storageIntegrityProbeSQL = "DESCRIBE TABLE db1.t"

// StorageIntegrityProbeExpectedSQL is what a correct v1 engine build emits for
// storageIntegrityProbeSQL under the fixed probe args. It must stay identical
// to the shared corpus case si_describe_metadata_select in both engines.
const StorageIntegrityProbeExpectedSQL = "SELECT name, type, default_kind AS default_type, default_expression, comment, '' AS codec_expression, '' AS ttl_expression FROM system.columns WHERE database = 'hg_safe' AND table = 'db1__t' AND name != '_hg_row_id' ORDER BY position"

// storageIntegrityProbeMinBuilds names the minimum engine builds in the failure
// message so an operator knows what to deploy.
const storageIntegrityProbeMinBuilds = "rewriter-go >= v0.7.2 / rewriter-grpc >= v0.12.2 (Spec I)"

// StorageIntegrityProbeFactory is a Factory whose engine build can be verified
// at startup. STORAGE_INTEGRITY_CONTRACT_V1 proves the backend understood the
// contract, not that it implements the current behaviour: the enum cannot
// distinguish patch releases, and bumping it per patch would need a proto
// change every time (Spec I D5).
type StorageIntegrityProbeFactory interface {
	Factory
	ProbeStorageIntegrityBuild(ctx context.Context) error
}

// storageIntegrityProbeArgs is the fixed dynamic-args payload. It deliberately
// does NOT go through buildDatabaseMap: the probe must not depend on network
// state or on any account's permissions.
func storageIntegrityProbeArgs() *pb.RewriteTableDynamicArgs {
	return &pb.RewriteTableDynamicArgs{
		DatabaseMap:            map[string]string{"db1": "phys"},
		KnownPhysicalDatabases: []string{"phys"},
		Delim:                  "_",
		StorageIntegrity: &pb.StorageIntegrityArgs{
			Tables: map[string]*pb.StorageIntegrityArgs_Table{
				"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
			},
			ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
			ReservedRowIdColumn: DefaultReservedRowIDColumn,
			ContractVersion:     StorageIntegrityContractV1,
		},
	}
}

// ProbeStorageIntegrityBuild issues one fixed SI DESCRIBE through the shared
// backend and requires the exact expected SQL plus a v1 acknowledgement.
// Returns an error naming the engine and the expected build on any mismatch;
// buildServer refuses to start on it.
func (f *SentioNetworkFactory) ProbeStorageIntegrityBuild(ctx context.Context) error {
	engine := f.options.Engine
	if engine == "" {
		engine = EngineGRPC
	}
	if f.backend == nil {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): no rewrite backend", engine)
	}
	timeout := f.options.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := f.backend.Rewrite(probeCtx, &pb.RewriteSQLRequest{
		Sql:     storageIntegrityProbeSQL,
		Options: []*pb.RewriteOption{rewriteOption(storageIntegrityProbeArgs())},
	})
	if err != nil {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): %w", engine, err)
	}
	if resp == nil {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): nil response", engine)
	}
	if resp.GetStorageIntegrityContractVersion() != StorageIntegrityContractV1 {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): contract acknowledgement %s, want %s; deploy %s",
			engine, resp.GetStorageIntegrityContractVersion(), StorageIntegrityContractV1, storageIntegrityProbeMinBuilds)
	}
	if resp.GetCode() != pb.RewriteCode_Success {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): code=%s message=%q; deploy %s",
			engine, resp.GetCode(), resp.GetMessage(), storageIntegrityProbeMinBuilds)
	}
	if got := resp.GetSqlAfterRewrite(); got != StorageIntegrityProbeExpectedSQL {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): unexpected build\n got: %s\nwant: %s\ndeploy %s",
			engine, got, StorageIntegrityProbeExpectedSQL, storageIntegrityProbeMinBuilds)
	}
	return nil
}

var _ StorageIntegrityProbeFactory = (*SentioNetworkFactory)(nil)
```

- [ ] **Step 4: Run to verify pass**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel run //:gazelle
bazel test //pkg/rewriter:rewriter_test --test_filter='TestProbeStorageIntegrityBuild' --test_output=all --nocache_test_results
```

Expected: all four subtests PASS.

- [ ] **Step 5: Run the probe at startup**

In `build.go`, replace the capable-factory check at `:445-450` with:

```go
	if len(siOptions.Tables) > 0 {
		capable, ok := rwFactory.(rewriter.StorageIntegrityCapableFactory)
		if !ok || capable.StorageIntegrityContractVersion() != rewriter.StorageIntegrityContractV1 {
			return nil, fmt.Errorf("storage_integrity.tables requires a storage-integrity contract v1 capable SQL rewriter; refusing fail-open startup")
		}
		// Spec I D5: the contract enum cannot distinguish engine builds. One
		// fixed DESCRIBE probe does. A factory that cannot be probed (an
		// injected test/host factory) is warned about rather than refused —
		// the probe verifies an engine build, and such a factory has none.
		if prober, ok := rwFactory.(rewriter.StorageIntegrityProbeFactory); ok {
			if err := prober.ProbeStorageIntegrityBuild(context.Background()); err != nil {
				return nil, err
			}
			log.Infow("storage-integrity rewriter build verified", "tables", len(siOptions.Tables))
		} else {
			log.Warnw("storage_integrity.tables: injected rewriter factory cannot be build-probed; the SI behaviour of this engine is unverified",
				"tables", len(siOptions.Tables))
		}
	}
```

- [ ] **Step 6: Add the startup test**

Append to `build_test.go`:

```go
// siProbeStubRewriterFactory is an SI-capable injected factory whose build
// probe fails, proving buildServer refuses startup on a mismatched engine.
type siProbeStubRewriterFactory struct {
	siCapableStubRewriterFactory
	err error
}

func (f siProbeStubRewriterFactory) ProbeStorageIntegrityBuild(context.Context) error { return f.err }

func TestBuildServer_RefusesStartupOnStorageIntegrityProbeMismatch(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Tables = []string{"tenant.events"}

	_, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
		Rewriter:     siProbeStubRewriterFactory{err: errors.New("storage-integrity engine probe (engine=native): unexpected build")},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "storage-integrity engine probe") {
		t.Fatalf("err = %v, want the probe refusal", err)
	}

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
		Rewriter:     siProbeStubRewriterFactory{},
	}, nil)
	if err != nil {
		t.Fatalf("a passing probe must start: %v", err)
	}
	bs.teardown()
}
```

- [ ] **Step 7: Run the full unit suite**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //... --test_output=errors
```

Expected: same result set as the Task 15b baseline, plus the new tests green.

- [ ] **Step 8: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/rewriter/probe.go pkg/rewriter/probe_test.go pkg/rewriter/BUILD.bazel build.go build_test.go
git commit -m "feat(storage-integrity): verify the rewriter engine build at startup (Spec I D5)"
```

### Task 19: D6 — the peer branch (record, warn, characterize)

Rewrite is skipped for peer-trusted, forwarded, maintenance and platform-operator sessions (`pkg/plugins/rewrite/rewriter.go:130-133,298,303`), and with no accessed tables the SI ingress short-circuits too (`pkg/plugins/storageintegrity/plugin.go:162`). Those sessions can address `hg_safe`/`hg_unsafe` directly.

**Spec I D6 splits these into two branches, and they need different work.** This task implements the *peer* branch only; Task 19b implements the *operator* branch. The split exists because the double-rewrite rationale — re-rewriting SQL the originating proxy already rewrote is what the peer-trust design forbids — justifies the peer and forward bypasses and nothing else. Maintenance and platform-operator sessions carry raw, un-rewritten SQL and are bypassed purely because those roles are trusted, which is a separate threat model with its own controls.

For the peer branch the mitigation is a documented network-isolation requirement, an operator-visible warning, and a test that makes any future change to the bypass a visible test change.

**Files:**
- Modify: `build.go` (new helper + one call in `buildServer`)
- Test: `build_test.go`, `pkg/plugins/rewrite/rewriter_test.go`

**Interfaces:**
- Produces: `func storageIntegrityInternalListenWarning(cfg *config.Config) string` in package `housegate` — the warning text, or `""` when it does not apply. Returning the string (instead of logging inline) is what makes it unit-testable.

- [ ] **Step 1: Write the failing tests**

Append to `build_test.go`:

```go
func TestStorageIntegrityInternalListenWarning(t *testing.T) {
	cfg := minimalServerCfg(t)
	if got := storageIntegrityInternalListenWarning(cfg); got != "" {
		t.Fatalf("no SI tables, no internal port: got %q, want no warning", got)
	}

	cfg.StorageIntegrity.Tables = []string{"tenant.events"}
	if got := storageIntegrityInternalListenWarning(cfg); got != "" {
		t.Fatalf("SI tables without an internal port: got %q, want no warning", got)
	}

	cfg.InternalListen = "0.0.0.0:9001"
	got := storageIntegrityInternalListenWarning(cfg)
	if !strings.Contains(got, "peer-trusted") || !strings.Contains(got, "internal_listen") {
		t.Fatalf("warning = %q, want it to name the peer-trust bypass and the internal port", got)
	}
}
```

Append to `pkg/plugins/rewrite/rewriter_test.go`:

```go
// TestPeerTrustedSessionBypassesStorageIntegrityRewrite pins Spec I D6: a
// peer-trusted session's SQL is NOT rewritten, so SI read policy does not run
// on it. This is deliberate — the originating proxy already rewrote that SQL,
// and re-running the rewrite would double-prefix physical names — and the
// mitigation is network isolation of the internal port, not a rewrite pass.
// If this test ever has to change, the peer-trust posture changed with it.
func TestPeerTrustedSessionBypassesStorageIntegrityRewrite(t *testing.T) {
	rw := &fakeRewriter{out: "REWRITTEN-SQL"}
	p := &Plugin{
		Factory:                                 &fakeFactory{rw: rw},
		FailClosedOnError:                       true,
		RequiredStorageIntegrityContractVersion: pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1,
	}
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}

	sess := newSessionForTest(t, 1)
	sess.State().SetPeerTrust("10.0.0.7:9001")

	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "SELECT a FROM db1.t",
		Query:       &chproto.Query{Body: "SELECT a FROM db1.t"},
	}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if rw.rewriteCalls != 0 {
		t.Fatalf("rewriteCalls = %d, want 0 — peer-trusted sessions bypass the rewrite plugin", rw.rewriteCalls)
	}
	if qctx.Query.Body != "SELECT a FROM db1.t" {
		t.Fatalf("query body = %q, want the peer SQL forwarded verbatim", qctx.Query.Body)
	}
}
```

Add `"github.com/housegate/housegate/pkg/plugin"` to that file's imports if it is not already there.

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //:housegate_test //pkg/plugins/rewrite:rewrite_test --test_filter='InternalListenWarning|PeerTrustedSessionBypasses' --test_output=all --nocache_test_results
```

Expected: `TestStorageIntegrityInternalListenWarning` fails to compile (`undefined: storageIntegrityInternalListenWarning`); `TestPeerTrustedSessionBypassesStorageIntegrityRewrite` PASSES immediately — it is a characterisation test that locks today's deliberate behaviour, exactly what Spec I D6(c) asks for.

- [ ] **Step 3: Implement the warning**

Add to `build.go`, next to `storageIntegrityRewriterOptions` (`:112`):

```go
// storageIntegrityInternalListenWarning returns the operator warning for the
// Spec I D6 trust boundary, or "" when it does not apply.
//
// Peer-trusted, forwarded, maintenance and platform-operator sessions bypass
// the rewrite plugin (pkg/plugins/rewrite/rewriter.go RunOnPeerTrust /
// RunOnForward and the maintenance short-circuit), and with no accessed tables
// the SI ingress short-circuits too. Such a session can therefore address
// hg_safe / hg_unsafe directly. Running SI rewrite on peer SQL is not an
// option — it was already rewritten by the originating proxy — so the
// mitigation is network isolation: the internal port must be reachable only
// from peer subnets.
func storageIntegrityInternalListenWarning(cfg *config.Config) string {
	if len(cfg.StorageIntegrity.Tables) == 0 || cfg.InternalListen == "" {
		return ""
	}
	return "storage_integrity: peer-trusted sessions arriving on internal_listen bypass storage-integrity rewrite and can address the hg_safe / hg_unsafe namespaces directly; internal_listen MUST be reachable only from trusted peer subnets"
}
```

and log it in `buildServer`, right after the existing read-state warning at `:425-427`:

```go
	if warning := storageIntegrityInternalListenWarning(cfg); warning != "" {
		log.Warnw(warning, "internal_listen", cfg.InternalListen, "tables", len(cfg.StorageIntegrity.Tables))
	}
```

- [ ] **Step 4: Run to verify pass**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //:housegate_test //pkg/plugins/rewrite:rewrite_test --test_output=errors --nocache_test_results
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add build.go build_test.go pkg/plugins/rewrite/rewriter_test.go
git commit -m "feat(storage-integrity): warn about and pin the peer-trust bypass (Spec I D6)"
```

### Task 19b: D6 — the operator branch (reserved-namespace pre-check)

Maintenance and platform-operator sessions keep their rewrite bypass — they legitimately need un-rewritten SQL — but Spec I D6 requires that they still cannot address the reserved SI physical namespaces or the reserved column. The control is a narrow pre-check that runs independently of the rewrite plugin, so it survives the bypass.

**Where this guard must live — read before writing code.** The obvious placement is inside `rewrite.Plugin`, next to the bypass it protects. That is wrong, and review of an earlier draft of this task caught it: `rewrite.Plugin` declares `RunOnForward() == false` (`pkg/plugins/rewrite/rewriter.go:303`), and `forward.Plugin` runs earlier in the chain and can set `IsForwarding`. On a forwarded session the whole rewrite plugin is skipped, so a guard inside it never runs — precisely for raw operator SQL, which is what it exists to stop. The guard therefore lives in its own tiny plugin that opts **into** both filters and gates on the session flags instead:

- `RunOnForward() bool { return true }` and `RunOnPeerTrust() bool { return true }` — the chain must not be able to skip it.
- Its `OnQuery` returns immediately unless `Maintenance || PlatformOperator` is set, so a plain peer-trusted or forwarded session is untouched and Spec I D6's peer decision is preserved exactly.

**The scanner must be written for this job; nothing existing fits.** Three candidates were considered and all are wrong here, so do not reach for them:

- Task 2's token scan lives in `rewriter-go/internal/engine` — not importable from housegate.
- `pkg/sqlident` offers only path helpers (`NormalizePath`, `SplitLastPath`, `Quote`, `QuotePath`). Useful as a *component* (below), not as the scan.
- `stripSQLLiteralsAndComments` (`pkg/plugins/storageintegrity/plugin.go:950`) is **actively unsafe for this purpose**, which review caught: `blankQuoted` erases backtick- and double-quoted spans exactly like single-quoted ones (`:955-956`). In ClickHouse those delimit *identifiers*, not string literals, so ``SELECT * FROM `hg_safe`.`t` `` and ``SELECT `_hg_row_id` FROM t`` would have the protected names blanked away and sail straight through the guard. That function is correct for its own job — finding non-deterministic function *names*, which cannot appear as quoted identifiers — and wrong for this one. Leave it where it is.

Write a small purpose-built scan in `sireserved` instead. It has two rules, and the second one matters as much as the first:

1. **Remove string literals and comments; preserve quoted identifiers.** `'…'`, `--…`, `/*…*/` are erased; `` `…` `` and `"…"` are kept and unquoted into path segments, so quoting cannot hide a reserved name.
2. **A reserved database is a violation as a qualifier *or* as a bare database target.** Emit candidate dotted paths and resolve each with `sqlident.SplitLastPath`, then compare the *database* segment — that covers `hg_safe.t`. But qualifier position alone is **not** sufficient, and getting this wrong is a fail-open on the flagship attack: in `TRUNCATE DATABASE hg_safe`, `DROP DATABASE hg_safe` and `USE hg_safe` the reserved name is a bare path, so `SplitLastPath` returns an empty database and a qualifier-only rule waves it through. `TRUNCATE DATABASE hg_safe` is one of the two Critical attacks this entire spec exists to close, and operator sessions skip rewrite, so the engine's own rejection cannot save it here.

   The scan therefore needs a little lexical context. A **bare** reserved identifier is a database reference when either:
   - the preceding significant token is `USE`, or `DATABASE` (which covers `CREATE`/`DROP`/`ALTER`/`ATTACH`/`DETACH`/`TRUNCATE`/`RENAME`/`EXISTS`/`SHOW CREATE` + `DATABASE`), or is `FROM`/`IN` immediately preceded by `TABLES` or `DICTIONARIES` (covering `SHOW TABLES FROM <db>`, `SHOW TABLES IN <db>`, `TRUNCATE ALL TABLES FROM <db>`); **or**
   - the statement contains the standalone keyword `DATABASE` anywhere outside literals. This is the fail-closed backstop, and it is deliberate: it mirrors D1's philosophy that the enumeration exists for message quality while a catch-all provides the safety property. A statement that says `DATABASE` and also names a reserved identifier is refused even if nobody enumerated its exact syntax.

   Anywhere else a bare reserved identifier is an ordinary name and is allowed, which is what keeps `SELECT hg_safe FROM ordinary.t` working. The reserved *column* needs none of this: any identifier segment equal to it is a violation, qualified or not, because there is no legitimate reference to `_hg_row_id`.

Segment comparison is exact and case-insensitive on the unquoted value, so `hg_safe_backup` does not match `hg_safe`.

**Files:**
- Create: `pkg/plugins/sireserved/plugin.go` (+ `plugin_test.go`, `BUILD.bazel`)
- Modify: `build.go` (register the plugin when SI tables are configured; extend the warning helper)
- Test: `pkg/plugins/sireserved/plugin_test.go`, `build_test.go`

**Step 3 also registers the plugin** in `buildServer`'s query-plugin list, ahead of `forward.Plugin`, and `build_test.go` asserts it is present when `storage_integrity.tables` is set and absent when it is not.

**Interfaces:**
- Consumes: `sqlident.SplitLastPath(value string) (database, table string)`; the configured SI table set and reserved databases from `pkg/config`.
- Produces: `func ReservedNamespaceViolation(sql string, dbs []string, rid string) string` in `pkg/plugins/sireserved` — the offending name, or `""`. Returning the string keeps it unit-testable without a session.
- Produces: `sireserved.Plugin{ReservedDatabases []string, ReservedRowIDColumn string}`, populated in `build.go` from `storage_integrity` config when SI tables are configured and left unregistered otherwise, so non-SI deployments keep a byte-identical chain.

- [ ] **Step 1: Write the failing tests**

Create `pkg/plugins/sireserved/plugin_test.go`. Construct the session with `chsession.New` exactly as `pkg/plugins/rewrite/rewriter_test.go:80-88`'s `newSessionForTest` does (copy that helper; do not invent `newTestRewritePlugin` / `newMaintenanceQueryContext` — they do not exist in this repo).

```go
// A maintenance session keeps its rewrite bypass but must not be able to
// address the protocol-owned namespaces. Spec I D6, operator branch.
func TestOnQuery_MaintenanceRefusesReservedNamespace(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		wantErr string
	}{
		{"safe database", "SELECT * FROM hg_safe.db1__t", "hg_safe"},
		{"unsafe database", "INSERT INTO hg_unsafe.db1__t VALUES (1)", "hg_unsafe"},
		{"reserved column", "SELECT _hg_row_id FROM db1.t", "_hg_row_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Plugin{
				ReservedDatabases:   []string{"hg_safe", "hg_unsafe"},
				ReservedRowIDColumn: "_hg_row_id",
			}
			sess := newSessionForTest(t, 1)
			sess.State().SetMaintenance(true)
			qctx := &plugin.QueryContext{
				Session:     sess,
				OriginalSQL: tc.sql,
				Query:       &chproto.Query{Body: tc.sql},
			}
			err := p.OnQuery(context.Background(), qctx)
			if err == nil {
				t.Fatal("maintenance session must be refused on a reserved name")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error must name %q: %v", tc.wantErr, err)
			}
		})
	}
}

// The guard must survive the forward filter. rewrite.Plugin declares
// RunOnForward() == false, so a guard living inside it would be skipped on a
// forwarded session — which is exactly when raw operator SQL is in flight.
func TestOnQuery_RefusesOnForwardedSession(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe"}, ReservedRowIDColumn: "_hg_row_id"}
	if !p.RunOnForward() || !p.RunOnPeerTrust() {
		t.Fatal("the guard must opt into both chain filters or it can be skipped")
	}
	sess := newSessionForTest(t, 1)
	sess.State().SetMaintenance(true)
	sess.State().SetForwarding(true)
	const sql = "SELECT * FROM hg_safe.db1__t"
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
	if err := p.OnQuery(context.Background(), qctx); err == nil {
		t.Fatal("a forwarded maintenance session must still be refused")
	}
}

// Ordinary tables pass through untouched, and a session without the operator
// flags is not affected at all — the peer branch of D6 is unchanged.
func TestOnQuery_LeavesOrdinaryAndNonOperatorSessionsAlone(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe"}, ReservedRowIDColumn: "_hg_row_id"}

	sess := newSessionForTest(t, 1)
	sess.State().SetMaintenance(true)
	const ordinary = "SELECT * FROM other.u"
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: ordinary, Query: &chproto.Query{Body: ordinary}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("ordinary table must pass: %v", err)
	}

	peer := newSessionForTest(t, 2)
	peer.State().SetPeerTrust("10.0.0.7:9000") // SetPeerTrust takes the peer address; there is no SetPeerTrusted(bool)
	const reserved = "SELECT * FROM hg_safe.db1__t"
	pctx := &plugin.QueryContext{Session: peer, OriginalSQL: reserved, Query: &chproto.Query{Body: reserved}}
	if err := p.OnQuery(context.Background(), pctx); err != nil {
		t.Fatalf("a peer session without operator flags is out of scope for this guard: %v", err)
	}
}

// The scan must survive quoting and must not fire on ordinary names. The
// quoted cases are the ones that bypass a literal-blanking stripper, and the
// false-positive cases are what a bare-token match would break.
func TestReservedNamespaceViolation(t *testing.T) {
	const rid = "_hg_row_id"
	dbs := []string{"hg_safe", "hg_unsafe"}

	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		// Must be caught.
		{"plain qualifier", "SELECT * FROM hg_safe.db1__t", "hg_safe"},
		{"backtick-quoted qualifier", "SELECT * FROM `hg_safe`.`db1__t`", "hg_safe"},
		{"double-quoted qualifier", `SELECT * FROM "hg_safe"."db1__t"`, "hg_safe"},
		{"mixed quoting", "SELECT * FROM `hg_unsafe`.db1__t", "hg_unsafe"},
		{"uppercase", "SELECT * FROM HG_SAFE.db1__t", "hg_safe"},
		{"reserved column bare", "SELECT _hg_row_id FROM db1.t", rid},
		{"reserved column quoted", "SELECT `_hg_row_id` FROM db1.t", rid},
		{"reserved column qualified", "SELECT t._hg_row_id FROM db1.t AS t", rid},

		// Bare database targets. A qualifier-only rule fails open on every one
		// of these, and the first is one of the two Critical attacks this spec
		// exists to close.
		{"truncate database", "TRUNCATE DATABASE hg_safe", "hg_safe"},
		{"truncate database quoted", "TRUNCATE DATABASE `hg_safe`", "hg_safe"},
		{"drop database", "DROP DATABASE hg_unsafe", "hg_unsafe"},
		{"drop database if exists", "DROP DATABASE IF EXISTS hg_safe", "hg_safe"},
		{"use", "USE hg_safe", "hg_safe"},
		{"use quoted", "USE `hg_safe`", "hg_safe"},
		{"attach database", "ATTACH DATABASE hg_safe", "hg_safe"},
		{"alter database", "ALTER DATABASE hg_safe MODIFY COMMENT 'x'", "hg_safe"},
		{"show tables from", "SHOW TABLES FROM hg_safe", "hg_safe"},
		{"show tables in", "SHOW TABLES IN hg_unsafe", "hg_unsafe"},
		{"truncate all tables from", "TRUNCATE ALL TABLES FROM hg_safe", "hg_safe"},

		// Must NOT be caught.
		{"reserved name in a literal", "SELECT 'hg_safe' AS s FROM db1.t", ""},
		{"reserved name in a comment", "SELECT * FROM db1.t -- hg_safe", ""},
		{"ordinary column with the same name", "SELECT hg_safe FROM ordinary.t", ""},
		{"ordinary column, aliased", "SELECT hg_safe AS x FROM ordinary.t", ""},
		{"ordinary column in WHERE", "SELECT a FROM ordinary.t WHERE hg_safe > 1", ""},
		{"prefix is not a match", "SELECT * FROM hg_safe_backup.t", ""},
		{"suffix is not a match", "SELECT * FROM my_hg_safe.t", ""},
		{"column literal mentioning the rid", "SELECT '_hg_row_id' AS s FROM db1.t", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ReservedNamespaceViolation(tc.sql, dbs, rid); got != tc.want {
				t.Fatalf("ReservedNamespaceViolation(%q) = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

// Guards the specific regression review found in the qualifier-only design:
// a rule that only inspects SplitLastPath's database segment fails open on
// every statement whose reserved name is the bare target.
func TestReservedNamespaceViolation_BareDatabaseTargetIsNotABypass(t *testing.T) {
	for _, sql := range []string{
		"TRUNCATE DATABASE hg_safe",
		"DROP DATABASE hg_safe",
		"USE hg_safe",
		"SHOW TABLES FROM hg_safe",
		"TRUNCATE ALL TABLES FROM hg_safe",
	} {
		if got := ReservedNamespaceViolation(sql, []string{"hg_safe", "hg_unsafe"}, "_hg_row_id"); got == "" {
			t.Fatalf("a bare database target must be refused: %q", sql)
		}
	}
}

// Guards the specific regression the review found: a scanner that blanks
// quoted identifiers (as stripSQLLiteralsAndComments does) passes this input.
func TestReservedNamespaceViolation_QuotingIsNotABypass(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM `hg_safe`.`t`",
		`SELECT * FROM "hg_unsafe"."t"`,
		"SELECT `_hg_row_id` FROM db1.t",
	} {
		if got := ReservedNamespaceViolation(sql, []string{"hg_safe", "hg_unsafe"}, "_hg_row_id"); got == "" {
			t.Fatalf("quoting must not bypass the guard: %q", sql)
		}
	}
}
```

Duplicate the first test for the platform-operator flag, substituting `SetPlatformOperator(true)` for `SetMaintenance(true)` — confirm the exact setter names in `pkg/chsession/state.go` before writing them (this task's guard gates on both, so neither may be dropped from the condition without a failing test).

- [ ] **Step 2: Run to verify they fail**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/sireserved:sireserved_test --test_output=all`
Expected: FAIL — the package does not compile yet (`undefined: Plugin`).

- [ ] **Step 3: Implement the pre-check**

Create `pkg/plugins/sireserved/plugin.go`:

```go
// Package sireserved refuses statements that address storage-integrity
// protocol-owned names on sessions whose rewrite is bypassed.
//
// The bypass exists because maintenance and platform-operator roles need
// un-rewritten SQL, not because they may address protocol-owned storage.
// Running the full SI rewrite for them would defeat the bypass; this narrow
// name check does not. It opts into the forward and peer-trust filters so the
// chain cannot skip it, and then gates on the operator flags itself.
// Spec I D6, operator branch.
package sireserved

type Plugin struct {
	ReservedDatabases   []string
	ReservedRowIDColumn string
}

func (p *Plugin) RunOnForward() bool   { return true }
func (p *Plugin) RunOnPeerTrust() bool { return true }

func (p *Plugin) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	st := qctx.Session.State()
	if st == nil {
		return nil
	}
	// Maintenance / PlatformOperator are Snapshot() fields, not methods.
	snap := st.Snapshot()
	if !snap.Maintenance && !snap.PlatformOperator {
		return nil
	}
	if name := ReservedNamespaceViolation(qctx.Query.Body, p.ReservedDatabases, p.ReservedRowIDColumn); name != "" {
		return fmt.Errorf(
			"storage-integrity reserved name %q is not addressable through the proxy; use a direct ClickHouse connection for physical access",
			name)
	}
	return nil
}

// ReservedNamespaceViolation reports the first reserved database used as a
// qualifier, or the reserved column referenced anywhere. Returns "" when the
// statement is clean.
//
// scanPaths removes string literals and comments but PRESERVES quoted
// identifiers, unquoting them into path segments: `hg_safe`.`t` and hg_safe.t
// must resolve identically, or backticks become a bypass. Do not substitute
// stripSQLLiteralsAndComments here — it blanks quoted identifiers.
func ReservedNamespaceViolation(sql string, dbs []string, rid string) string {
	reserved := make(map[string]string, len(dbs))
	for _, db := range dbs {
		reserved[strings.ToLower(db)] = db
	}
	lowerRID := strings.ToLower(rid)

	for _, path := range scanPaths(sql) {
		database, table := sqlident.SplitLastPath(path)
		// The reserved column is a violation wherever it appears.
		if lowerRID != "" {
			if strings.EqualFold(unquoteSegment(table), rid) {
				return rid
			}
			if database != "" && strings.EqualFold(unquoteSegment(database), rid) {
				return rid
			}
		}
		// A reserved database is a violation as a qualifier...
		if database != "" {
			if original, ok := reserved[strings.ToLower(unquoteSegment(database))]; ok {
				return original
			}
			continue
		}
		// ...or as a bare database target. Skipping this is a fail-open on
		// TRUNCATE/DROP DATABASE and USE, where the reserved name is the whole
		// path. Only bare identifiers in database position count, so an
		// ordinary column named hg_safe is still allowed.
		if !path.DatabasePosition {
			continue
		}
		if original, ok := reserved[strings.ToLower(unquoteSegment(table))]; ok {
			return original
		}
	}
	return ""
}
```

`scanPaths` is the ~70-line lexer described above. It returns `[]scannedPath{Text string; DatabasePosition bool}`, where `DatabasePosition` is set by the rule-2 context test (preceding token `USE` / `DATABASE` / `TABLES|DICTIONARIES FROM|IN`, or the statement carrying a standalone `DATABASE` keyword). `unquoteSegment` strips one layer of backticks or double quotes, collapsing the doubled-quote escape. Both live in the same file with their own table tests.



- [ ] **Step 4: Run the tests**

Run: `bazel test //pkg/plugins/sireserved:sireserved_test --test_output=all`
Expected: PASS.

- [ ] **Step 5: Extend the startup warning**

`storageIntegrityInternalListenWarning` (Task 19) gains a second sentence when SI tables are configured and `auth.platform_operator_addresses` is non-empty: name the count of operator addresses and the SI tables reachable through the operator bypass. Assert the new text in `build_test.go`.

- [ ] **Step 6: Full suite + commit**

Run: `bazel test //pkg/plugins/sireserved:all //pkg/plugins/storageintegrity:all //pkg/storageintegrity:all //:housegate_test --test_output=errors`
Expected: PASS (the storageintegrity targets cover the literal-stripper move).

```bash
git add pkg/plugins/sireserved pkg/storageintegrity/sql.go pkg/plugins/storageintegrity/plugin.go build.go build_test.go
git commit -m "feat(sireserved): refuse reserved SI names on operator-bypassed sessions"
```

### Task 20: Bump housegate onto the fixed engines

Three pin mechanisms must move together (`.claude/skills/upgrade-dependency/SKILL.md`): the plain `require`, the out-of-band FFI binary release, and every version string in configs and docs.

**Files:**
- Modify: `go.mod`, `go.sum`, `configs/local.server.yaml`, `configs/local.server-mock-remote.yaml`, `CLAUDE.md`

- [ ] **Step 1: Read the release notes and bump the module**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
grep -nE "rewriter-go|rewriter-proto" go.mod
gh release view <rewriter-go-tag> --repo housegate/rewriter-go
go get github.com/housegate/rewriter-go@<rewriter-go-tag> && go mod tidy
```

Expected: `go.mod` now says `github.com/housegate/rewriter-go <rewriter-go-tag>`. `rewriter-proto` stays at `v0.2.0` — this spec adds no proto field. A wide transitive diff is MVS doing its job; do not hand-pin transitives back down.

- [ ] **Step 2: Re-sync Bazel**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel mod tidy && bazel run //:gazelle
```

Expected: no `MODULE.bazel` edit needed for a pure version bump (`go_deps.from_file` reads `go.mod`).

- [ ] **Step 3: Chase the version string**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
grep -rn "v0.7.1" --include=*.go --include=*.yaml --include=*.json --include=*.md . | grep -v "^./bazel-" | grep -v docs/superpowers
```

Update each hit outside `docs/superpowers/` (those are point-in-time records — leave them):
- `configs/local.server.yaml:48` — `native_library_release: <rewriter-go-tag>`
- `configs/local.server-mock-remote.yaml:48` — the same key, commented out
- `CLAUDE.md`, `pkg/ffifetch` bullet — the "requires an FFI library built from rewriter-go >= vX.Y.Z" sentence
- `pkg/integration/storage_integrity_read_test.go:44` — the skip message's suggested `fetch-rewriter-lib --tag` value

- [ ] **Step 4: Fetch the new FFI library and run the native smoke rung**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
go run ./cmd fetch-rewriter-lib --tag <rewriter-go-tag>
```

It prints the resolved cached path on stdout. Feed it to Bazel:

```bash
bazel test //pkg/rewriter:rewriter_test --test_env=POLYGLOT_SQL_FFI_PATH=<printed-path> \
  --test_output=all --test_arg=-test.v --nocache_test_results 2>&1 | grep -E '^--- (PASS|FAIL): TestNativeEngineSmoke'
```

Expected: `--- PASS: TestNativeEngineSmoke`. A green run without that line means the test **skipped** and proves nothing.

- [ ] **Step 5: Build and test**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
go build ./... && go vet ./... 2>&1 | grep -v "unkeyed fields"
bazel build //... && bazel test //... --test_output=errors
```

Expected: clean build/vet (the `config.Duration ... unkeyed fields` notes are pre-existing noise) and the Task 15b baseline test result.

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add go.mod go.sum configs/local.server.yaml configs/local.server-mock-remote.yaml CLAUDE.md pkg/integration/storage_integrity_read_test.go
git commit -m "build: bump rewriter-go to <rewriter-go-tag> (Spec I fail-closed engines)"
```

### Task 21: The Critical regression test (docker, native engine)

Spec I §5's headline requirement: `SYSTEM START MERGES hg_unsafe.<phys>` and `TRUNCATE DATABASE hg_safe` must both return an Exception **and leave the tables provably untouched**. This test must fail on the pre-fix build.

**Files:**
- Modify: `pkg/integration/storage_integrity_read_test.go` (new test function; the file's helpers `siReadStateStub`, `openConn`, `openConnNoDB`, `chEnv`, `testenv.*` already exist)

- [ ] **Step 1: Write the test**

Append to `pkg/integration/storage_integrity_read_test.go`:

```go
// TestStorageIntegrityRead_CriticalStatementsAreRefused is the regression test
// for the two Critical findings of the 2026-08-19 review (Spec I §1):
//
//   - SYSTEM START MERGES hg_unsafe.<phys> passed the engines' catch-all as
//     Success. Merges being stopped is what makes the candidate-part boundary
//     equal the statement boundary (base design §12.2); a merged part carries a
//     name that is not in excluded_unsafe_parts, so unsafe_latest would return
//     already-promoted rows a second time.
//   - TRUNCATE DATABASE hg_safe was rejected by the engine before it recorded
//     an accessed table, so HouseGate's SI-flag-keyed gate never fired and the
//     statement reached ClickHouse, emptying the authoritative committed state.
//
// Both must now be Exceptions, and both tables must be byte-for-byte untouched
// afterwards — asserted through system.parts and SELECT count().
func TestStorageIntegrityRead_CriticalStatementsAreRefused(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run `go run ./cmd fetch-rewriter-lib --tag <rewriter-go-tag>` and pass --test_env")
	}
	ctx := context.Background()
	const phys = "phys_si_guard"
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS " + phys,
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"CREATE DATABASE IF NOT EXISTS hg_unsafe",
		"DROP TABLE IF EXISTS hg_safe.db1__guard",
		"DROP TABLE IF EXISTS hg_unsafe.db1__guard",
		"CREATE TABLE hg_unsafe.db1__guard (_hg_row_id FixedString(32), a UInt32) ENGINE = MergeTree ORDER BY a",
		"CREATE TABLE hg_safe.db1__guard AS hg_unsafe.db1__guard ENGINE = MergeTree ORDER BY a",
		"SYSTEM STOP MERGES hg_unsafe.db1__guard",
		"SYSTEM STOP MERGES hg_safe.db1__guard",
		"INSERT INTO hg_unsafe.db1__guard VALUES (repeat('a', 32), 1)",
		"INSERT INTO hg_unsafe.db1__guard VALUES (repeat('b', 32), 2)",
		"INSERT INTO hg_safe.db1__guard VALUES (repeat('c', 32), 3)",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP TABLE IF EXISTS hg_safe.db1__guard")
		_ = seed.Exec(ctx, "DROP TABLE IF EXISTS hg_unsafe.db1__guard")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys)
	})

	// Two separate INSERTs with merges stopped give hg_unsafe exactly two
	// active parts; a merge would collapse them into one, which is precisely
	// what the part-count assertion detects.
	activeParts := func(database, table string) uint64 {
		t.Helper()
		var n uint64
		if err := seed.QueryRow(ctx,
			"SELECT count() FROM system.parts WHERE database = ? AND table = ? AND active",
			database, table).Scan(&n); err != nil {
			t.Fatalf("system.parts(%s.%s): %v", database, table, err)
		}
		return n
	}
	rows := func(table string) uint64 {
		t.Helper()
		var n uint64
		if err := seed.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&n); err != nil {
			t.Fatalf("count(%s): %v", table, err)
		}
		return n
	}

	unsafePartsBefore, safePartsBefore := activeParts("hg_unsafe", "db1__guard"), activeParts("hg_safe", "db1__guard")
	unsafeRowsBefore, safeRowsBefore := rows("hg_unsafe.db1__guard"), rows("hg_safe.db1__guard")
	if unsafePartsBefore != 2 || safePartsBefore != 1 || unsafeRowsBefore != 2 || safeRowsBefore != 1 {
		t.Fatalf("seed shape = unsafe(parts=%d rows=%d) safe(parts=%d rows=%d), want unsafe(2,2) safe(1,1)",
			unsafePartsBefore, unsafeRowsBefore, safePartsBefore, safeRowsBefore)
	}

	port := &siReadStateStub{parts: map[string][]string{}}
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("db1"),
		testenv.WithStorageIntegrityReadState(port),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.StorageIntegrity.Tables = []string{"db1.guard"}
			cfg.StorageIntegrity.Read.DefaultMode = string(rewriter.ReadModeSafe)
		}),
	)
	conn := openConn(t, proxy.Addr)

	for _, tc := range []struct{ name, sql, wantMessage string }{
		{"system start merges on the unsafe namespace", "SYSTEM START MERGES hg_unsafe.db1__guard",
			"storage-integrity physical table hg_unsafe.db1__guard is not directly addressable"},
		{"system stop merges on the safe namespace", "SYSTEM STOP MERGES hg_safe.db1__guard",
			"storage-integrity physical table hg_safe.db1__guard is not directly addressable"},
		{"truncate the safe database", "TRUNCATE DATABASE hg_safe",
			"storage-integrity physical database hg_safe is not directly addressable"},
		{"unmodelled statement naming nothing storage-integrity", "SYSTEM RELOAD CONFIG",
			"statement class is not modelled by the rewriter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := conn.Exec(ctx, tc.sql)
			if err == nil {
				t.Fatalf("%q must be refused with an Exception", tc.sql)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("%q error = %v, want it to contain %q", tc.sql, err, tc.wantMessage)
			}
		})
	}

	// Provably untouched: identical active-part inventory (no merge ran) and
	// identical row counts (nothing was truncated).
	if got := activeParts("hg_unsafe", "db1__guard"); got != unsafePartsBefore {
		t.Fatalf("hg_unsafe active parts = %d, want %d — merges must still be stopped", got, unsafePartsBefore)
	}
	if got := activeParts("hg_safe", "db1__guard"); got != safePartsBefore {
		t.Fatalf("hg_safe active parts = %d, want %d", got, safePartsBefore)
	}
	if got := rows("hg_unsafe.db1__guard"); got != unsafeRowsBefore {
		t.Fatalf("hg_unsafe rows = %d, want %d", got, unsafeRowsBefore)
	}
	if got := rows("hg_safe.db1__guard"); got != safeRowsBefore {
		t.Fatalf("hg_safe rows = %d, want %d — TRUNCATE DATABASE must never reach ClickHouse", got, safeRowsBefore)
	}

	// The logical read surface still works after the refusals.
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM db1.guard").Scan(&n); err != nil {
		t.Fatalf("SELECT after refusals: %v", err)
	}
	if n != safeRowsBefore {
		t.Fatalf("safe-mode count = %d, want %d", n, safeRowsBefore)
	}
}
```

- [ ] **Step 2: Run it**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
FFI=$(go run ./cmd fetch-rewriter-lib --tag <rewriter-go-tag>)
bazel test //pkg/integration:integration_test \
  --test_filter='TestStorageIntegrityRead_CriticalStatementsAreRefused' \
  --test_env=POLYGLOT_SQL_FFI_PATH=$FFI \
  --test_env=DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}') \
  --test_env=HOME --test_output=all --nocache_test_results
```

Expected: PASS, with all four subtests listed.

- [ ] **Step 3: Prove it fails on the pre-fix build**

Spec I §5 requires this test to fail on the old engines — otherwise it is not a regression test.

```bash
cd /Users/uranuswch/Dev/housegate/housegate
OLD=$(go run ./cmd fetch-rewriter-lib --tag v0.7.1)
bazel test //pkg/integration:integration_test \
  --test_filter='TestStorageIntegrityRead_CriticalStatementsAreRefused' \
  --test_env=POLYGLOT_SQL_FFI_PATH=$OLD \
  --test_env=DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}') \
  --test_env=HOME --test_output=all --nocache_test_results
```

Expected: **FAIL**. Specifically, `system start merges on the unsafe namespace` fails with "must be refused with an Exception" (v0.7.1 answers `Success`), and `truncate the safe database` fails on the untouched-rows assertion or the message. Record the exact output in the PR — it is the evidence that the hole existed and is now closed. Then re-run Step 2 to leave the tree green.

Note: the pre-fix run exercises the D5 startup probe too. If `buildServer` refuses to start against v0.7.1 with `storage-integrity engine probe … unexpected build`, that is also a valid pre-fix failure and equally good evidence — record whichever you get.

- [ ] **Step 4: Confirm CI already runs this target**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
grep -n "pkg/integration" .github/workflows/ci.yml
```

Expected: `//pkg/integration:integration_test` is already in the explicit list, so the new test runs in CI without a workflow edit. (`POLYGLOT_SQL_FFI_PATH` is not set in CI, so the test self-skips there — the docker suite still proves the non-SI paths. Note this in the PR; making CI fetch the FFI lib is Spec J's territory.)

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/integration/storage_integrity_read_test.go
git commit -m "test(storage-integrity): regression-test the two Critical refusals end to end (Spec I §5)"
```

### Task 22: Full verification and housegate release

**Files:**
- Modify: `CLAUDE.md` (the storage-integrity / rewriter bullets)

- [ ] **Step 1: Update the repo guide**

In `CLAUDE.md`, extend the rewriter section so the next reader learns the new invariants. Add to the storage-integrity paragraph of "**4. SQL rewriter is a pluggable backend**":

```markdown
With a configured SI table set the engines also fail **closed on their catch-all**: any statement class the rewriter does not model (`SYSTEM`, `CHECK TABLE`, and whatever ClickHouse adds next) is rejected with `UnsupportedStatement` rather than forwarded, and every reject's message names the SI table or reserved namespace the statement addressed. HouseGate mirrors that posture: with `storage_integrity.tables` non-empty, **every** non-`Success` response becomes a `RejectedError`, not only one whose accessed tables carry the SI flag. Startup additionally issues one fixed DESCRIBE probe (`rewriter.StorageIntegrityProbeExpectedSQL`) and refuses to start when the engine build does not emit the exact expected SQL — the contract enum alone cannot distinguish engine patch releases. Exception text is scrubbed of `hg_safe` / `hg_unsafe` names and `_hg_row_id` before it reaches the client. Peer-trusted / forwarded / maintenance / platform-operator sessions still bypass SI rewrite entirely (Spec I D6): that is a recorded decision, pinned by `TestPeerTrustedSessionBypassesStorageIntegrityRewrite`, and its mitigation is that `internal_listen` must be reachable only from trusted peer subnets — `buildServer` warns when SI tables and an internal port are configured together.
```

- [ ] **Step 2: Full Bazel gate**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel build //... && bazel test //... --test_output=errors
```

Expected: identical to the Task 15b baseline plus the new tests. Any newly failing target is a regression — fix it before proceeding.

- [ ] **Step 3: Docker suite**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
FFI=$(go run ./cmd fetch-rewriter-lib --tag <rewriter-go-tag>)
bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test \
  --test_env=POLYGLOT_SQL_FFI_PATH=$FFI \
  --test_env=DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}') \
  --test_env=HOME --test_output=errors
```

Expected: pass. If an integration test fails, apply the main-baseline rule before calling it a regression: `git stash push`, re-run that single test with `--runs_per_test=10` on clean `main`, `git stash pop`, and record the ratio.

- [ ] **Step 4: Commit and open the PR**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add CLAUDE.md
git commit -m "docs: record the storage-integrity fail-closed posture (Spec I)"
gh pr create --fill --title "fix(storage-integrity): surface fail-closed hardening (Spec I)"
```

PR body must carry: the two Critical findings and their now-failing-then-passing evidence from Task 21 Step 3, the corpus case count and sha256, the engine tags pinned, and the Bazel baseline comparison.

- [ ] **Step 5: Release**

After merge, cut the housegate release per the repo's release workflow. Record the tag as `<housegate-tag>` for Task 23.

---

## Part D — specs

### Task 23: Record the D6 decision and close Spec I

Spec I §6 item 4: "Spec B edit list gains the D6 decision record."

**Files:**
- Modify: `docs/superpowers/specs/2026-06-22-storage-integrity-design.md` (the base design, "Spec B" in the roadmap's edit list)
- Modify: `docs/superpowers/specs/2026-08-19-storage-integrity-surface-failclosed-design.md` (status)
- Modify: `docs/superpowers/specs/2026-08-19-storage-integrity-remediation-roadmap.md` (Spec I row)

- [ ] **Step 1: Add the D6 decision record to the base design**

In `docs/superpowers/specs/2026-06-22-storage-integrity-design.md`, in the section that covers the trust boundaries (§11/§12 area, next to the merge-guard discussion), add:

```markdown
**Peer-trust bypass (recorded 2026-08-19, Spec I D6).** Peer-trusted, forwarded, maintenance and platform-operator sessions bypass the SQL rewrite plugin, and with no accessed tables the signed ingress short-circuits too. Such a session can address `hg_safe` / `hg_unsafe` directly and is not subject to storage-integrity read policy. This is deliberate in v1: peer SQL was already rewritten by the originating proxy, and re-running the rewrite would double-prefix physical names, which the peer-trust design forbids. The mitigation is network isolation — `internal_listen` MUST be reachable only from trusted peer subnets — plus a startup warning when storage-integrity tables and an internal port are configured together. The bypass is pinned by `TestPeerTrustedSessionBypassesStorageIntegrityRewrite`, so changing it is a visible test change rather than a silent behaviour change.
```

Write it as one line (no hard wrapping) per the repo's markdown convention.

- [ ] **Step 2: Close Spec I and update the roadmap**

In `2026-08-19-storage-integrity-surface-failclosed-design.md`, change `**Status:** Proposed` to `**Status:** Implemented` and append a closure line to §6 naming the three tags:

```markdown
**Delivered:** rewriter-go `<rewriter-go-tag>`, rewriter-grpc `<rewriter-grpc-tag>`, housegate `<housegate-tag>`. Corpus: 201 cases, sha256 `<corpus-sha>`, byte-identical in both engine repos.
```

In `2026-08-19-storage-integrity-remediation-roadmap.md` §2, change Spec I's row urgency cell from `**Blocker** — ship before any SI deployment` to `**Shipped** — <rewriter-go-tag> / <rewriter-grpc-tag> / <housegate-tag>`.

- [ ] **Step 3: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add docs/superpowers/specs/2026-06-22-storage-integrity-design.md \
  docs/superpowers/specs/2026-08-19-storage-integrity-surface-failclosed-design.md \
  docs/superpowers/specs/2026-08-19-storage-integrity-remediation-roadmap.md
git commit -m "docs(storage-integrity): record the peer-trust bypass decision and close Spec I"
```

---

## Self-review

Run after the plan is written, before execution.

**1. Spec coverage** — see the map below; every decision and every §5 acceptance item has a task.

**2. Placeholder scan** — no "TBD", no "add error handling", no "similar to Task N", no test described without its code. Two intentional `<placeholders>` remain and are release artifacts that cannot be known before execution: `<rewriter-go-tag>`, `<rewriter-grpc-tag>`, `<housegate-tag>`, `<corpus-sha>`. Each is produced by a named earlier step (Task 15 Steps 1/3, Task 7 Step 3, Task 22 Step 5).

**3. Type consistency**
- `StorageIntegrityUnmodelledMessage` (Go, Task 1) ↔ `kStorageIntegrityUnmodelledMessage` (C++, Task 8) — same literal, checked by the corpus.
- `handlers.AnnotateStorageIntegrityReject` (Task 2) ↔ `rewriter_handlers::annotateStorageIntegrityReject` (Task 9) — same argument order and same skip rules.
- `engine.ReferencesIdentifierInScope` (Task 4) ↔ `astReferencesIdentifierInScope` (Task 11).
- `engine.PrewhereTargets` (Task 6) ↔ the lexical PREWHERE loop (Task 13); both bind to the first FROM target.
- The wrapper message is changed in exactly three places: the corpus (Task 6 Step 1, 7 occurrences), `select.go:167` (Task 6 Step 5), `storage_integrity.h` (Task 13 Step 2).
- `rewriter.NewStorageIntegrityScrubber` (Task 17) is consumed by `rewrite.Plugin.StorageIntegrityScrubber` (Task 17 Step 4) and wired in `build.go` (same step).
- `rewriter.StorageIntegrityProbeFactory` / `ProbeStorageIntegrityBuild` (Task 18) is consumed by `build.go` (Task 18 Step 5) and by `siProbeStubRewriterFactory` (Task 18 Step 6).
- `StorageIntegrityProbeExpectedSQL` (Task 18) is the same string as corpus case `si_describe_metadata_select`'s `want_sql` and as both engines' `describeMetadataSQL`.
- Corpus case names are unique — asserted mechanically in Task 7 Step 2.

## Spec coverage map

| Spec I section | Requirement | Tasks |
|---|---|---|
| §1.1a | `SYSTEM` / `CHECK TABLE` pass through as `Success` | 1, 2 (Go), 8, 9 (C++), 16 (HouseGate), 21 (end-to-end) |
| §1.1b | target-less rejections forwarded verbatim | 2, 9, 16, 21 |
| §1 bullet 1 | `escapeSQLLiteral` ignores `\` | 3 (Go), 10 (C++) |
| §1 bullet 2 | reserved-column check scoped per statement, not per table | 4 (Go), 11 (C++) |
| §1 bullet 3 | SI namespace gate not CTE-aware; zero `WITH` cases | 5 (Go), 12 (C++) |
| §1 bullet 4 | `PREWHERE` leaks physical names through a raw error | 6 (Go), 13 (C++) |
| §1 bullet 5 | C++ `restoreStorageIntegrityProjectionSyntax` mutates literals | 7 (case), 10 (fix) |
| §1 bullet 6 | undocumented peer-trust bypass | 19, 23 |
| §3 D1 | catch-all fails closed | 1, 8 |
| §3 D2 | enumerated classes get object-naming messages | 2, 9 |
| §3 D3 | HouseGate rejects any non-`Success` when SI configured | 16 |
| §3 D4 | backslash escaping in both engines | 3, 10 |
| §3 D5 | startup engine-build probe | 18 |
| §3 D6 | peer-trust decision record + warning + test | 19, 23 |
| §3 D7a–c | scoping, CTE, `PREWHERE` | 4, 5, 6 (Go); 11, 12, 13 (C++) |
| §3 D7d | literal-aware projection restore (C++) | 7, 10 |
| §3 D7e | SI physical-name scrub in `OnException` | 17 |
| §4 corpus, D1 catch-all group | `SYSTEM` × 4, `CHECK TABLE`, exotic unmodelled | 1 (generic), 2 (object-naming) |
| §4 corpus, D2 enumerated group | `TRUNCATE DATABASE` / `TRUNCATE ALL TABLES FROM` / `ALTER DATABASE` / `DROP DICTIONARY` / `CREATE LIVE VIEW` | 2 |
| §4 corpus, D4 escaping group | `'`, `\`, `\'`, literal-closing name | 3 |
| §4 corpus, D7a/b/c/d groups | one group per defect | 4, 5, 6, 7 |
| §4 corpus byte-identity | sha256 compare as its own step | 8 Step 1, 14 Step 2 |
| §4 Spec J dependency | stated, per-case where relevant | "Dependency on Spec J" section; Tasks 3, 5, 6, 7, 10 |
| §5 both engines green + oracle diff | | 14 |
| §5 `TestRewriter_FailOpen` still passes | `TestSentioRewriter_EmptySIKeepsUnsupportedPassthrough` + `TestOnQuery_OrdinaryErrorStaysFailOpenWhenNoSISurfaceIsConfigured` | 16 |
| §5 new fail-closed unit test | `TestSentioRewriter_FailsClosedOnAnyNonSuccessWhenSIConfigured` | 16 |
| §5 D5 probe refuses old build | | 18 |
| §5 D6 bypass asserted | | 19 |
| §5 docker regression, provably untouched | | 21 |
| §6.1 rewriter-go → tag | | 1-7, 15 |
| §6.2 rewriter-grpc → tag | | 8-14, 15 |
| §6.3 housegate → tag | | 16-22 |
| §6.4 Spec B D6 decision record | | 23 |
