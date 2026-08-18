# Storage-Integrity Read Surface Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make storage-integrity (SI) tables readable through HouseGate: reads of an SI table resolve to `hg_safe.<t>` (mode `safe`) or `hg_safe ∪ hg_unsafe` minus promoted-not-yet-cleaned unsafe parts (mode `unsafe_latest`), `_hg_row_id` is hidden and unaddressable, non-lane writes/DDL against SI tables are refused, and both rewriter engines implement one contract from one shared golden file.

**Architecture:** A minor rewriter-proto bump adds `StorageIntegrityArgs` (per-logical-table safe/unsafe physical names + excluded unsafe parts + read mode + reserved column) to `RewriteTableDynamicArgs`, and `AccessedTable.is_storage_integrity`. rewriter-go and rewriter-grpc consult it *before* ordinary dynamic name resolution: SELECT-family table references become derived tables `(SELECT * EXCEPT (_hg_row_id) FROM hg_safe.<t> [UNION ALL … hg_unsafe.<t> WHERE _part NOT IN (…)]) AS <alias>`, `EXISTS TABLE` maps to the safe table, `DESCRIBE` becomes a `system.columns` metadata SELECT, and every other SI-touching statement is refused. HouseGate fills the new args from `storage_integrity.tables[]` + `storage_integrity.read.default_mode` + the per-query `SQL_x_read_mode` setting, and gets the excluded-part list from a new host port (`StorageIntegrityReadState.PromotedUnsafeParts(tableID)`) that sentio-node satisfies with the embedded `snode.Role`, which now journals promoted-unsafe-part names until their cleanup ack. HouseGate fails **closed** on any non-Success rewriter response that involves an SI table (a rejection must reach the client as an Exception, never fall open to the un-rewritten SQL).

**Tech Stack:** protobuf/buf (rewriter-proto), Go 1.25 + polyglot FFI via PureGo (rewriter-go), C++23 + ClickHouse parser + gtest (rewriter-grpc, built on the remote build box), Go + Bazel 9 (housegate), Go (sentio-node, arbiter-core).

**Spec:** `docs/superpowers/specs/2026-08-18-storage-integrity-read-surface-rewrite-design.md` (Spec G). Companion: `docs/superpowers/specs/2026-08-18-storage-integrity-v1-closure-roadmap.md` §4 (decisions 9: read-mode policy), `docs/superpowers/specs/2026-08-18-rewriter-convergence-design.md` (Spec E — shares the proto bump; its D6 DESCRIBE classification is a prerequisite of G §4.3), `docs/superpowers/specs/2026-08-18-storage-integrity-physical-table-lifecycle-design.md` D2 (physical naming `hg_unsafe.<CHTableName(table_id)>`, `CHTableName(id) = ReplaceAll(id, ".", "__")`).

## Global Constraints

- Physical naming is frozen by Spec C D2: SI table id = logical `<database>.<table>`; physical tables `hg_unsafe.<CHTableName(id)>`, `hg_safe.<CHTableName(id)>`, `CHTableName(id) = strings.ReplaceAll(id, ".", "__")`. HouseGate must not import arbiter-core; it re-implements the one-line rule as `config.StorageIntegrityPhysicalTable`.
- Reserved column name is `_hg_row_id` (rewriter default when `reserved_row_id_column` is empty). Rejection message on any user reference: `reserved column _hg_row_id is not addressable` (`RewriteCode = RewriteError`).
- SI write rejection message: `storage-integrity table <t> accepts writes only through the signed statement lane` (`RewriteCode = UnsupportedStatement`), `<t>` = the logical `db.table` key.
- Read modes: config `storage_integrity.read.default_mode: safe|unsafe_latest` (unset = `safe`, base design §11); per-query override setting `SQL_x_read_mode = 'safe' | 'unsafe_latest'`. `unsafe_latest` with no promotion-state port is **refused with an Exception**, never degraded (Spec G D2).
- SI membership is explicit: `storage_integrity.tables[]` (renamed from `runtime.merge_guard.tables`), a list of logical `<database>.<table>` ids shared by merge guard, ingress, and the read rewrite (Spec G D4).
- Proto: `StorageIntegrityArgs storage_integrity = 12` inside `RewriteTableDynamicArgs`; `bool is_storage_integrity = 6` on `AccessedTable`. Do NOT touch Spec E's `RewriteSQLResponse.insert_class` field; `STATEMENT_TYPE_DESCRIBE = 22` is added by whichever of E/G lands first (identical name+number, see Task 1).
- Both engines are golden-tested from ONE file: `storage_integrity_cases.json` (authored in Task 3, byte-identical copy in rewriter-grpc `tests/testdata/`).
- Housegate: Bazel is the test ground truth; `go.mod` module path `github.com/housegate/housegate`; run `bazel mod tidy && bazel run //:gazelle` after dependency changes; English identifiers/comments/log messages; structured logging via `pkg/log` (`Infow/Warnw/...`).
- rewriter-go: polyglot imports only inside `internal/engine`; `internal/nameresolve` imports neither engine nor polyglot; engine-backed tests skip unless `POLYGLOT_SQL_FFI_PATH` is set (`make ffi` builds it at `third_party/lib/libpolyglot_sql_ffi.<dylib|so>`).
- rewriter-grpc: builds only on the remote box (`ssh -p 30100 sentio@64.38.131.242`, workdir `/home/sentio/chen/rewriter-grpc/`); dev loop = rsync → `./scripts.sh rebuild` → `ctest --test-dir build --output-on-failure` (single test: `./build/rewriter_tests --gtest_filter='<Suite.Name>'`). Formatting through `formatAst` (backticks, WhenNecessary).
- Commit per task with the repo's conventional prefixes; every task ends with the named test command green.

## Deviations from the spec found while reading the code (reviewer decision points)

**D-1 — INSERT into an SI table is NOT rejected by the rewriter (spec §4.4 lists INSERT).** In housegate the SI ingress plugin (`pkg/plugins/storageintegrity`) runs *after* the rewrite plugin ([build.go](../../build.go) appends `rewritePlug` before `storageIntegrityIngress`), classifies the statement from `qctx.StatementType`, resolves the target from `qctx.AccessedTables`, and — with `SuppressUpstreamExecution` — still forwards the (rewritten) INSERT to ClickHouse for Native sample-block negotiation. A rewriter `UnsupportedStatement` would (a) clear `statement_type` (both engines do on reject) so the ingress fails with "statement type mismatch", and (b) forward the un-rewritten logical `INSERT INTO db.t` upstream, which does not exist physically. So the plan keeps INSERT on the ordinary dynamic path (Success, target = multi-tenant physical table, exactly today's behaviour) but marks the accessed table `is_storage_integrity = true`. Refusing a non-lane INSERT is HouseGate's job: the SI ingress already refuses any INSERT without `SQL_x_auth_token`; for deployments with SI tables but *no* ingress (`storage_integrity.ingress.enabled: false`) the rewriter wrapper (`pkg/rewriter/sentio.go`) rejects INSERTs whose accessed tables carry the SI flag with the spec's message. Golden case `si_insert_rewrites_like_today` (Task 3) records this. Flip back to a rewriter-side reject once Spec A moves sample-block synthesis to the agent.

**D-2 — Fail-closed rule in HouseGate.** Today `pkg/plugins/rewrite` fails open on every rewriter error (forwards original SQL). For SI that would silently send `SELECT _hg_row_id FROM db.t` or `DROP TABLE db.t` upstream as logical SQL. New rule (Task 17/18): if the rewriter response is non-Success **and** any `original_accessed_tables` entry has `is_storage_integrity = true`, `sentioRewriter.Rewrite` returns `*rewriter.RejectedError` and the plugin returns it (Exception to client). Ordinary fail-open is unchanged (`TestRewriter_FailOpen` keeps passing).

**D-3 — `SQL_x_read_mode` is not stripped before forwarding.** No `SQL_x_*` key is stripped today (`SQL_x_auth_token`, `SQL_x_payer` reach ClickHouse as `custom_settings_prefixes = SQL_` custom settings). Adding a strip mechanism only for this key would be an inconsistent cross-cutting change; the plan reads the setting and leaves it in place like its siblings.

**D-4 — The port is narrower than spec §5.** Membership comes from config (D4), so the host port only supplies the exclusion list: `interface StorageIntegrityReadState { PromotedUnsafeParts(tableID string) ([]string, error) }` — the exact method `snode.Role` gains, so sentio-node passes `siRole` with no adapter.

**D-5 — SI check sits beside `ApplyDynamic`, not inside it.** `nameresolve.LookupStorageIntegrity` is called by the SELECT / write / EXISTS / GRANT / DESCRIBE handlers *before* `nameresolve.Resolve`; `ApplyDynamic` itself stays pure so the INSERT path (D-1) can still use it. Same "SI mapping wins" semantics; C++ mirrors it in `dynamicRewriteWalk` / `rewriteOneTarget`.

**D-6 — `CREATE TABLE x AS SELECT … FROM <si>`** is not specially detected: neither engine walks a CREATE-AS-SELECT body today (Spec E D4 territory). `CREATE TABLE <si>` (target) and `CREATE TABLE <si> AS <src>` (clone) ARE rejected via the shared write-target check.

**D-7 — DESCRIBE.** Spec E D6 has not landed. Task 8/13 add the *minimal* DESCRIBE handling G needs: classify `DESCRIBE|DESC [TABLE] [db.]t` as `STATEMENT_TYPE_DESCRIBE`, SI table → metadata SELECT, non-SI → pass through unchanged (today's behaviour). If E's `RewriteDescribe` / `handlers/describe.cc` already exists when the task runs, add the SI branch inside it instead of creating a new handler.

## File map

| Repo | Create | Modify |
|---|---|---|
| rewriter-proto | — | `proto/rewriter.proto`, `gen/pb/*` (regenerated) |
| rewriter-go | `internal/harness/testdata/storage_integrity_cases.json`, `internal/harness/storage_integrity_golden_test.go`, `internal/handlers/storage_integrity.go`, `internal/handlers/storage_integrity_test.go`, `internal/handlers/describe.go`, `internal/handlers/describe_test.go` | `go.mod`, `internal/nameresolve/resolve.go` (+test), `internal/engine/nodes.go` (+test), `internal/engine/objtarget.go`, `internal/handlers/select.go`, `writes.go`, `exists.go`, `grant.go`, `native.go`, `internal/harness/select_golden_test.go` (accessedJSON), `AGENTS.md`, `README.md` |
| rewriter-grpc | `src/handlers/storage_integrity.h/.cc`, `src/handlers/describe.h/.cc`, `tests/testdata/storage_integrity_cases.json` | `third_party/rewriter-proto` (submodule pin), `src/handlers/name_rewrite.h/.cc`, `select.cc`, `writes.cc`, `exists.cc`, `show_create.cc`, `grant.cc`, `src/rewriter-server.cc`, `CMakeLists.txt`, `tests/CMakeLists.txt`, `tests/rewriter_test.cc`, `CLAUDE.md`, `AGENTS.md` |
| housegate | `pkg/rewriter/storage_integrity.go` (+test), `pkg/integration/storage_integrity_read_test.go` | `go.mod`, `pkg/sqlmeta/accessed_table.go`, `pkg/sqlmeta/statement_type.go`, `pkg/config/storage_integrity_config.go` (+test), `storage_integrity_runtime.go`, `build.go`, `build_test.go`, `proxy.go`, `pkg/rewriter/args.go`, `sentio.go`, `types.go`, `backend_test.go`, `pkg/plugins/rewrite/rewriter.go` (+test), `pkg/plugins/storageintegrity/plugin.go` (DESCRIBE in read-only list), `pkg/integration/testenv/proxy.go`, `configs/local.server.yaml`, `CLAUDE.md`, `README.md` |
| arbiter-core | — | `snode/state.go` (+test), `snode/promote.go`, `snode/cleanup.go`, `snode/snode.go` (`PromotedUnsafeParts`), `snode/promote_test.go`, `snode/cleanup_test.go` |
| sentio-node | — | `go.mod`, `config/config.go` (+test), `standalone/standalone.go`, `README.md` |

---

## Part A — rewriter-proto (contract)

### Task 1: `StorageIntegrityArgs` + `AccessedTable.is_storage_integrity` in rewriter-proto, release tag

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-proto`

**Files:**
- Modify: `proto/rewriter.proto` (after `RewriteTableDynamicArgs.remote_upstreams` at ~:260; `AccessedTable` at ~:721-727; `StatementType` at ~:495-565)
- Regenerate: `gen/pb/rewriter.pb.go`, `gen/pb/rewriter_grpc.pb.go`

**Interfaces:**
- Produces (Go, package `pb`): `pb.StorageIntegrityArgs{Tables map[string]*pb.StorageIntegrityArgs_Table, ReadMode pb.StorageIntegrityArgs_ReadMode, ReservedRowIdColumn string}`, `pb.StorageIntegrityArgs_Table{SafeTable, UnsafeTable string, ExcludedUnsafeParts []string}`, enum values `pb.StorageIntegrityArgs_READ_MODE_UNSPECIFIED/READ_MODE_SAFE/READ_MODE_UNSAFE_LATEST`, `pb.RewriteTableDynamicArgs.StorageIntegrity *pb.StorageIntegrityArgs` (getter `GetStorageIntegrity()`), `pb.AccessedTable.IsStorageIntegrity bool`, `pb.StatementType_STATEMENT_TYPE_DESCRIBE = 22`.

- [ ] **Step 1: Add the messages/fields to `proto/rewriter.proto`**

Insert immediately before the closing `}` of `message RewriteTableDynamicArgs` (after the `remote_upstreams = 8;` field):

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
    StorageIntegrityArgs storage_integrity = 12;
```

Insert this new top-level message right after `message RewriteTableDynamicArgs { … }`:

```proto
// Per-request storage-integrity surface description. See
// RewriteTableDynamicArgs.storage_integrity for the semantics.
message StorageIntegrityArgs {
    enum ReadMode {
        READ_MODE_UNSPECIFIED = 0;    // treated as SAFE
        READ_MODE_SAFE = 1;           // (SELECT * EXCEPT (rid) FROM safe_table)
        READ_MODE_UNSAFE_LATEST = 2;  // safe UNION ALL unsafe WHERE _part NOT IN (excluded_unsafe_parts)
    }
    message Table {
        string safe_table = 1;                      // physical "hg_safe.db__t"
        string unsafe_table = 2;                    // physical "hg_unsafe.db__t"
        repeated string excluded_unsafe_parts = 3;  // promoted, not yet cleaned unsafe part names
    }
    // key = logical "db.table" exactly as the user may write it after the
    // caller resolved USE (i.e. after upstream_logical_database_in_context).
    map<string, Table> tables = 1;
    ReadMode read_mode = 2;
    // Reserved per-row identity column; "" means "_hg_row_id".
    string reserved_row_id_column = 3;
}
```

Add to `message AccessedTable` after `bool is_remote = 5;`:

```proto
    // True iff this access resolved to a storage-integrity table (a key of
    // RewriteTableDynamicArgs.storage_integrity.tables). Callers use it to
    // gate writes and to fail closed on rejected responses; the logical
    // names above are unchanged so auth/usage keep working on them.
    bool is_storage_integrity = 6;
```

Add to `enum StatementType` after `STATEMENT_TYPE_DROP_VIEW = 21;` **only if Spec E's PR has not already added it** (check `git log -p main -- proto/rewriter.proto | grep DESCRIBE`); the name and number MUST be exactly these so the two branches merge cleanly:

```proto
    // DESCRIBE [TABLE] [db.]t (Spec E D6 / Spec G §4.3). Storage-integrity
    // tables are rewritten to a system.columns SELECT; others resolve like
    // EXISTS TABLE (E) or pass through until E lands (G-minimal).
    STATEMENT_TYPE_DESCRIBE = 22;
```

- [ ] **Step 2: Lint, regenerate, breaking-check, test**

Run: `make lint && make proto && make breaking BREAKING_BASELINE=main && make test`
Expected: `buf lint` silent; `git status --short gen/` shows `M gen/pb/rewriter.pb.go` (grpc stub unchanged); `buf breaking` silent (additive only); `scripts/next-version_test.sh` + `go test ./...` print `ok`.

- [ ] **Step 3: Verify the generated Go surface**

Run: `grep -n "GetStorageIntegrity()\|GetIsStorageIntegrity()\|StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST\|STATEMENT_TYPE_DESCRIBE" gen/pb/rewriter.pb.go | head`
Expected: four (or three, if DESCRIBE was E's) matching lines.

- [ ] **Step 4: Commit and release**

```bash
git checkout -b feat/storage-integrity-args
git add proto/rewriter.proto gen/pb
git commit -m "feat(proto): add StorageIntegrityArgs and AccessedTable.is_storage_integrity (Spec G)"
```

Open the PR, merge to `main`, then run the `release` GitHub Actions workflow from `main` (`gh workflow run release.yml --ref main`). It computes the version (first release on a later Asia/Shanghai day than `v0.1.0` → **`v0.2.0`**; if Spec E already released `v0.2.0` today the workflow prints `v0.2.1` — use whatever it prints as `<proto-tag>` in every later task).
Run: `gh release view v0.2.0 --repo housegate/rewriter-proto --json tagName --jq .tagName`
Expected: `v0.2.0`.

---

## Part B — rewriter-go (native engine + shared goldens)

### Task 2: Bump rewriter-go onto the new proto

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-go`

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Bump**

Run: `git checkout -b feat/storage-integrity-read-surface && go get github.com/housegate/rewriter-proto@v0.2.0 && go mod tidy`
Expected: `go.mod` line `github.com/housegate/rewriter-proto v0.2.0`.

- [ ] **Step 2: Build/vet/test (pure-Go lane) and the FFI lane**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: `ok` for every package (engine-backed tests skip).
Run: `make test`
Expected: all packages `ok` (this also proves the FFI lib is built at `third_party/lib/libpolyglot_sql_ffi.<ext>`).

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "build: bump rewriter-proto to v0.2.0 (StorageIntegrityArgs)"
```

### Task 3: Shared golden file `storage_integrity_cases.json` + harness runner (red)

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-go`

**Files:**
- Create: `internal/harness/testdata/storage_integrity_cases.json`
- Create: `internal/harness/storage_integrity_golden_test.go`
- Modify: `internal/harness/select_golden_test.go:57-63` (`accessedJSON` gains `is_storage_integrity`; `checkAccessed` compares it)

**Interfaces:**
- Produces: the JSON schema every engine test consumes — per case: `name`, `sql`, `dynamic{database_map, known_physical_databases, upstream_logical_database_in_context, delim, storage_integrity{tables{<key>{safe_table,unsafe_table,excluded_unsafe_parts}}, read_mode: "SAFE"|"UNSAFE_LATEST", reserved_row_id_column}}`, `want_code`, `want_stmt`, `want_table_rewrites`, `want_accessed[]{original_database, original_table, logical_database, physical_database, is_remote, is_storage_integrity}`, `want_sql` (polyglot canonical, compared semantically in Go), `sql_exact` (byte compare — used only for string-built outputs both engines emit verbatim), `want_sql_contains[]` / `want_sql_not_contains[]` (engine-agnostic substrings; the C++ test uses these), `want_message_contains`, `reject`, `allow_sql_divergence` (oracle: exempt `sql_after_rewrite` because ClickHouse renders `* EXCEPT _hg_row_id` without parens for a single column while polyglot renders `* EXCEPT (_hg_row_id)`; structured fields stay gated).

- [ ] **Step 1: Write the golden file (this exact content — the C++ copy in Task 11 must be byte-identical)**

```json
[
  {
    "name": "si_safe_plain_select",
    "sql": "SELECT a FROM db1.t WHERE a > 1",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys", "system"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS \"db1.t\" WHERE a > 1",
    "want_sql_contains": ["hg_safe.db1__t", "EXCEPT", "_hg_row_id"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t"},
    "want_accessed": [{"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}],
    "allow_sql_divergence": true
  },
  {
    "name": "si_safe_alias_join_non_si",
    "sql": "SELECT count() FROM db1.t AS a JOIN other.u AS b ON a.id = b.id",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys", "system"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT count() FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS a JOIN phys.\"other.u\" AS b ON a.id = b.id",
    "want_sql_contains": ["hg_safe.db1__t", "AS a", "other.u", "AS b"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t", "other.u": "phys.other.u"},
    "want_accessed": [
      {"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true},
      {"original_database": "other", "original_table": "u", "logical_database": "other", "physical_database": "phys", "is_storage_integrity": false}
    ],
    "allow_sql_divergence": true
  },
  {
    "name": "si_unsafe_latest_no_excluded",
    "sql": "SELECT a FROM db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t", "excluded_unsafe_parts": []}},
        "read_mode": "UNSAFE_LATEST",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t UNION ALL SELECT * EXCEPT (_hg_row_id) FROM hg_unsafe.db1__t) AS \"db1.t\"",
    "want_sql_contains": ["hg_safe.db1__t", "UNION ALL", "hg_unsafe.db1__t"],
    "want_sql_not_contains": ["NOT IN"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t"},
    "want_accessed": [{"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}],
    "allow_sql_divergence": true
  },
  {
    "name": "si_unsafe_latest_two_excluded",
    "sql": "SELECT a FROM db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t", "excluded_unsafe_parts": ["all_1_1_0", "all_2_2_0"]}},
        "read_mode": "UNSAFE_LATEST",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t UNION ALL SELECT * EXCEPT (_hg_row_id) FROM hg_unsafe.db1__t WHERE _part NOT IN ('all_1_1_0', 'all_2_2_0')) AS \"db1.t\"",
    "want_sql_contains": ["UNION ALL", "hg_unsafe.db1__t", "_part NOT IN ('all_1_1_0', 'all_2_2_0')"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t"},
    "want_accessed": [{"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}],
    "allow_sql_divergence": true
  },
  {
    "name": "si_safe_subquery_in_where",
    "sql": "SELECT a FROM other.u WHERE a IN (SELECT a FROM db1.t)",
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
    "want_sql": "SELECT a FROM phys.\"other.u\" AS \"other.u\" WHERE a IN (SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS \"db1.t\")",
    "want_sql_contains": ["other.u", "hg_safe.db1__t"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t", "other.u": "phys.other.u"},
    "want_accessed": [
      {"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true},
      {"original_database": "other", "original_table": "u", "logical_database": "other", "physical_database": "phys", "is_storage_integrity": false}
    ],
    "allow_sql_divergence": true
  },
  {
    "name": "si_reserved_column_select_rejected",
    "sql": "SELECT _hg_row_id, a FROM db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "RewriteError", "want_stmt": "", "reject": true,
    "want_message_contains": "reserved column _hg_row_id is not addressable",
    "want_accessed": [{"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}]
  },
  {
    "name": "si_reserved_column_where_rejected",
    "sql": "SELECT a FROM db1.t WHERE _hg_row_id = 'x' ORDER BY a",
    "dynamic": {
      "database_map": {"db1": "phys"},
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
  },
  {
    "name": "si_star_hides_rid",
    "sql": "SELECT * FROM db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT * FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS \"db1.t\"",
    "want_sql_contains": ["EXCEPT", "_hg_row_id", "hg_safe.db1__t"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t"},
    "allow_sql_divergence": true
  },
  {
    "name": "si_use_default_database",
    "sql": "SELECT a FROM t",
    "dynamic": {
      "database_map": {"db1": "phys"},
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
    "want_sql": "SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS t",
    "want_sql_contains": ["hg_safe.db1__t", "AS t"],
    "want_table_rewrites": {"t": "hg_safe.db1__t"},
    "want_accessed": [{"original_database": "", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}],
    "allow_sql_divergence": true
  },
  {
    "name": "si_describe_metadata_select",
    "sql": "DESCRIBE TABLE db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "DESCRIBE",
    "want_sql": "SELECT name, type, default_type, default_expression, comment, codec_expression, ttl_expression FROM system.columns WHERE database = 'hg_safe' AND table = 'db1__t' AND name != '_hg_row_id' ORDER BY position",
    "sql_exact": true,
    "want_accessed": [{"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}]
  },
  {
    "name": "si_exists_table_safe",
    "sql": "EXISTS TABLE db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "EXISTS_TABLE",
    "want_sql": "EXISTS TABLE hg_safe.db1__t",
    "want_sql_contains": ["EXISTS TABLE hg_safe.db1__t"],
    "want_table_rewrites": {"db1.t": "hg_safe.db1__t"},
    "want_accessed": [{"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}]
  },
  {
    "name": "si_show_create_rejected",
    "sql": "SHOW CREATE TABLE db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "SHOW CREATE TABLE on storage-integrity table db1.t is not supported",
    "want_accessed": [{"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}]
  },
  {
    "name": "si_insert_rewrites_like_today",
    "sql": "INSERT INTO db1.t (a) VALUES (1)",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "INSERT",
    "want_sql": "INSERT INTO phys.\"db1.t\" (a) VALUES (1)",
    "want_sql_contains": ["db1.t", "VALUES (1)"],
    "want_table_rewrites": {"db1.t": "phys.db1.t"},
    "want_accessed": [{"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}]
  },
  {
    "name": "si_alter_rejected",
    "sql": "ALTER TABLE db1.t DELETE WHERE a = 1",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity table db1.t accepts writes only through the signed statement lane",
    "want_accessed": [{"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": true}]
  },
  {
    "name": "si_drop_rejected",
    "sql": "DROP TABLE db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity table db1.t accepts writes only through the signed statement lane"
  },
  {
    "name": "si_truncate_rejected",
    "sql": "TRUNCATE TABLE db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity table db1.t accepts writes only through the signed statement lane"
  },
  {
    "name": "si_rename_rejected",
    "sql": "RENAME TABLE db1.t TO db1.t2",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity table db1.t accepts writes only through the signed statement lane"
  },
  {
    "name": "si_create_table_target_rejected",
    "sql": "CREATE TABLE db1.t (a UInt32) ENGINE = MergeTree ORDER BY a",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity table db1.t accepts writes only through the signed statement lane"
  },
  {
    "name": "si_optimize_rejected",
    "sql": "OPTIMIZE TABLE db1.t FINAL",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true
  },
  {
    "name": "si_grant_rejected",
    "sql": "GRANT SELECT ON db1.t TO u1",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity table db1.t accepts writes only through the signed statement lane"
  },
  {
    "name": "si_show_tables_unchanged",
    "sql": "SHOW TABLES",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "upstream_logical_database_in_context": "db1",
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SHOW_TABLES",
    "want_sql_contains": ["system.tables", "database = 'phys'", "startsWith(name, 'db1.')"]
  },
  {
    "name": "non_si_table_unaffected",
    "sql": "SELECT a FROM other.u",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "UNSAFE_LATEST",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT a FROM phys.\"other.u\" AS \"other.u\"",
    "want_sql_contains": ["other.u"],
    "want_sql_not_contains": ["hg_safe", "hg_unsafe", "EXCEPT"],
    "want_table_rewrites": {"other.u": "phys.other.u"},
    "want_accessed": [{"original_database": "other", "original_table": "u", "logical_database": "other", "physical_database": "phys", "is_storage_integrity": false}]
  },
  {
    "name": "si_absent_args_ordinary_rewrite",
    "sql": "SELECT a FROM db1.t",
    "dynamic": {
      "database_map": {"db1": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_"
    },
    "want_code": "Success", "want_stmt": "SELECT",
    "want_sql": "SELECT a FROM phys.\"db1.t\" AS \"db1.t\"",
    "want_sql_not_contains": ["hg_safe", "EXCEPT"],
    "want_table_rewrites": {"db1.t": "phys.db1.t"},
    "want_accessed": [{"original_database": "db1", "original_table": "t", "logical_database": "db1", "physical_database": "phys", "is_storage_integrity": false}]
  }
]
```

- [ ] **Step 2: Extend `accessedJSON` + `checkAccessed` in `internal/harness/select_golden_test.go`**

Change the struct (line ~57) and the comparison (inside `checkAccessed`, the `if g.GetOriginalDatabase() != w.OriginalDatabase || …` chain):

```go
type accessedJSON struct {
	OriginalDatabase   string `json:"original_database"`
	OriginalTable      string `json:"original_table"`
	LogicalDatabase    string `json:"logical_database"`
	PhysicalDatabase   string `json:"physical_database"`
	IsRemote           bool   `json:"is_remote"`
	IsStorageIntegrity bool   `json:"is_storage_integrity"`
}
```

and add `|| g.GetIsStorageIntegrity() != w.IsStorageIntegrity` to the mismatch condition in `checkAccessed` (existing corpora omit the field → `false`, so they keep passing).

- [ ] **Step 3: Write the harness runner `internal/harness/storage_integrity_golden_test.go`**

```go
package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/housegate/rewriter-go/internal/engine"
	"github.com/housegate/rewriter-proto/gen/pb"
)

// siDynamicJSON is dblevelDynamicJSON plus the storage_integrity block.
type siDynamicJSON struct {
	DatabaseMap            map[string]string `json:"database_map"`
	KnownPhysicalDatabases []string          `json:"known_physical_databases"`
	UpstreamLogical        string            `json:"upstream_logical_database_in_context"`
	Delim                  string            `json:"delim"`
	StorageIntegrity       *siArgsJSON       `json:"storage_integrity"`
}

type siArgsJSON struct {
	Tables              map[string]siTableJSON `json:"tables"`
	ReadMode            string                 `json:"read_mode"` // "SAFE" | "UNSAFE_LATEST"
	ReservedRowIDColumn string                 `json:"reserved_row_id_column"`
}

type siTableJSON struct {
	SafeTable           string   `json:"safe_table"`
	UnsafeTable         string   `json:"unsafe_table"`
	ExcludedUnsafeParts []string `json:"excluded_unsafe_parts"`
}

type siCase struct {
	Name                string            `json:"name"`
	SQL                 string            `json:"sql"`
	Dynamic             *siDynamicJSON    `json:"dynamic"`
	WantCode            string            `json:"want_code"`
	WantStmt            string            `json:"want_stmt"`
	WantTableRewrites   map[string]string `json:"want_table_rewrites"`
	WantAccessed        []accessedJSON    `json:"want_accessed"`
	WantSQL             string            `json:"want_sql"`
	SQLExact            bool              `json:"sql_exact"`
	WantSQLContains     []string          `json:"want_sql_contains"`
	WantSQLNotContains  []string          `json:"want_sql_not_contains"`
	WantMessageContains string            `json:"want_message_contains"`
	Reject              bool              `json:"reject"`
	AllowSQLDivergence  bool              `json:"allow_sql_divergence"`
}

var siReadModeByName = map[string]pb.StorageIntegrityArgs_ReadMode{
	"":              pb.StorageIntegrityArgs_READ_MODE_UNSPECIFIED,
	"SAFE":          pb.StorageIntegrityArgs_READ_MODE_SAFE,
	"UNSAFE_LATEST": pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST,
}

// siStmtByName adds the statement types the shared maps do not know yet.
var siStmtByName = map[string]pb.StatementType{
	"DESCRIBE": pb.StatementType_STATEMENT_TYPE_DESCRIBE,
}

var siCodeByName = map[string]pb.RewriteCode{
	"Success":               pb.RewriteCode_Success,
	"SyntaxError":           pb.RewriteCode_SyntaxError,
	"RewriteError":          pb.RewriteCode_RewriteError,
	"UnsupportedStatement":  pb.RewriteCode_UnsupportedStatement,
	"InvalidRewriteRequest": pb.RewriteCode_InvalidRewriteRequest,
}

func siStmtType(name string) pb.StatementType {
	if s, ok := siStmtByName[name]; ok {
		return s
	}
	return phase4StmtType(name)
}

func (c siCase) options() []*pb.RewriteOption {
	if c.Dynamic == nil {
		return nil
	}
	da := &pb.RewriteTableDynamicArgs{
		DatabaseMap:                      c.Dynamic.DatabaseMap,
		KnownPhysicalDatabases:           c.Dynamic.KnownPhysicalDatabases,
		UpstreamLogicalDatabaseInContext: c.Dynamic.UpstreamLogical,
		Delim:                            c.Dynamic.Delim,
	}
	if si := c.Dynamic.StorageIntegrity; si != nil {
		args := &pb.StorageIntegrityArgs{
			Tables:              map[string]*pb.StorageIntegrityArgs_Table{},
			ReadMode:            siReadModeByName[si.ReadMode],
			ReservedRowIdColumn: si.ReservedRowIDColumn,
		}
		for k, t := range si.Tables {
			args.Tables[k] = &pb.StorageIntegrityArgs_Table{
				SafeTable: t.SafeTable, UnsafeTable: t.UnsafeTable, ExcludedUnsafeParts: t.ExcludedUnsafeParts,
			}
		}
		da.StorageIntegrity = args
	}
	return []*pb.RewriteOption{{Op: pb.RewriteOp_TableNameRewrite,
		Value: &pb.RewriteOption_TableNameArgs{TableNameArgs: &pb.RewriteTableNameArgs{DynamicArgs: da}}}}
}

func loadSICases(t *testing.T) []siCase {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "storage_integrity_cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []siCase
	if err := json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

// TestStorageIntegrityGolden is the Spec G parity gate. Cases are driven
// through the public NativeRewriter (full doRewrite dispatch) so every
// statement family is covered. Structured fields compare exactly, SQL
// semantically (exact when sql_exact), plus engine-agnostic
// want_sql_contains / want_sql_not_contains substrings that the C++ test
// applies to the identical JSON.
func TestStorageIntegrityGolden(t *testing.T) {
	if os.Getenv("POLYGLOT_SQL_FFI_PATH") == "" {
		t.Skip("needs engine")
	}
	e, err := engine.NewPolyglot("")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	oracle, _ := DialOracle()
	defer oracle.Close()
	semEq := semanticSQLEq(e)

	for _, c := range loadSICases(t) {
		t.Run(c.Name, func(t *testing.T) {
			r := newWriteRewriter(e, c.options())
			res, err := r.Rewrite(context.Background(), c.SQL, "acct")
			if err != nil {
				t.Fatalf("rewrite: %v", err)
			}
			if c.WantCode != "" && res.Code != siCodeByName[c.WantCode] {
				t.Errorf("code = %v, want %s (%s)", res.Code, c.WantCode, res.Message)
			}
			if res.StatementType != siStmtType(c.WantStmt) {
				t.Errorf("statement_type = %v, want %q", res.StatementType, c.WantStmt)
			}
			if c.WantMessageContains != "" && !strings.Contains(res.Message, c.WantMessageContains) {
				t.Errorf("message = %q, want contains %q", res.Message, c.WantMessageContains)
			}
			if c.WantTableRewrites != nil && !eqStrMap(res.TableRewrites, c.WantTableRewrites) {
				t.Errorf("table_rewrites = %v, want %v", res.TableRewrites, c.WantTableRewrites)
			}
			if c.WantAccessed != nil {
				checkAccessed(t, res.OriginalAccessedTables, c.WantAccessed)
			}
			if c.Reject {
				if res.SQL != c.SQL {
					t.Errorf("reject must echo original SQL: got %q", res.SQL)
				}
			} else {
				switch {
				case c.SQLExact:
					if res.SQL != c.WantSQL {
						t.Errorf("sql (exact):\n got %q\nwant %q", res.SQL, c.WantSQL)
					}
				case c.WantSQL != "":
					if eq, err := semEq(res.SQL, c.WantSQL); err != nil || !eq {
						t.Errorf("sql (semantic):\n got %q\nwant %q (err=%v)", res.SQL, c.WantSQL, err)
					}
				}
			}
			for _, sub := range c.WantSQLContains {
				if !strings.Contains(res.SQL, sub) {
					t.Errorf("sql %q must contain %q", res.SQL, sub)
				}
			}
			for _, sub := range c.WantSQLNotContains {
				if strings.Contains(res.SQL, sub) {
					t.Errorf("sql %q must NOT contain %q", res.SQL, sub)
				}
			}
			if oracle != nil {
				want, oerr := oracle.Rewrite(c.SQL, c.options())
				if oerr != nil {
					t.Fatalf("oracle: %v", oerr)
				}
				got := pbFromResult(res)
				cmpEq := semEq
				if c.Reject || c.AllowSQLDivergence {
					got.SqlAfterRewrite = want.GetSqlAfterRewrite()
					if got.SqlAfterRewrite == "" {
						cmpEq = nil
					}
				}
				if d := Compare(got, want, cmpEq); !d.Equal() {
					t.Errorf("oracle divergence: %v", d.Mismatches)
				}
			}
		})
	}
}
```

- [ ] **Step 4: Run the new golden test — it must be RED (nothing implemented yet)**

Run: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/harness -run TestStorageIntegrityGolden -count=1 2>&1 | tail -30`
Expected: `--- FAIL: TestStorageIntegrityGolden` with sub-failures for every `si_*` case (`sql (semantic)` / `code = Success, want UnsupportedStatement` / `accessed len` mismatches); `non_si_table_unaffected` and `si_absent_args_ordinary_rewrite` pass. Existing corpora still green: `go test ./internal/harness -run 'TestSelectGolden|TestPhase4Golden|TestWritesGolden|TestDBLevelGolden' -count=1` → `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/harness/testdata/storage_integrity_cases.json internal/harness/storage_integrity_golden_test.go internal/harness/select_golden_test.go
git commit -m "test(harness): add shared storage-integrity golden corpus (Spec G, red)"
```

### Task 4: `nameresolve` — SI lookup, reserved column, `Accessed.IsStorageIntegrity`

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-go`

**Files:**
- Modify: `internal/nameresolve/resolve.go` (`Accessed` struct ~:179, `ResolveAccessed` ~:224)
- Test: `internal/nameresolve/resolve_test.go`

**Interfaces:**
- Produces:
  - `func LookupStorageIntegrity(db, table string, a *pb.RewriteTableDynamicArgs) (tbl *pb.StorageIntegrityArgs_Table, logicalKey string, ok bool)` — `logical := db` or `a.UpstreamLogicalDatabaseInContext`; `logicalKey = logical + "." + table`; ok iff `a.GetStorageIntegrity().GetTables()[logicalKey]` exists.
  - `func ReservedRowIDColumn(a *pb.RewriteTableDynamicArgs) string` — `storage_integrity.reserved_row_id_column` or `"_hg_row_id"`.
  - `func StorageIntegrityWriteRejectMessage(logicalKey string) string` → `"storage-integrity table " + logicalKey + " accepts writes only through the signed statement lane"`.
  - `Accessed.IsStorageIntegrity bool` populated by `ResolveAccessed` in dynamic mode.

- [ ] **Step 1: Write the failing tests (append to `resolve_test.go`)**

```go
func siArgs(upstream string) *pb.RewriteTableDynamicArgs {
	return &pb.RewriteTableDynamicArgs{
		DatabaseMap:                      map[string]string{"db1": "phys"},
		KnownPhysicalDatabases:           []string{"phys"},
		UpstreamLogicalDatabaseInContext: upstream,
		StorageIntegrity: &pb.StorageIntegrityArgs{
			Tables: map[string]*pb.StorageIntegrityArgs_Table{
				"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
			},
			ReadMode: pb.StorageIntegrityArgs_READ_MODE_SAFE,
		},
	}
}

func TestLookupStorageIntegrity(t *testing.T) {
	tbl, key, ok := LookupStorageIntegrity("db1", "t", siArgs(""))
	if !ok || key != "db1.t" || tbl.GetSafeTable() != "hg_safe.db1__t" {
		t.Fatalf("qualified hit: ok=%v key=%q tbl=%v", ok, key, tbl)
	}
	if _, key, ok := LookupStorageIntegrity("", "t", siArgs("db1")); !ok || key != "db1.t" {
		t.Fatalf("USE-resolved hit: ok=%v key=%q", ok, key)
	}
	if _, _, ok := LookupStorageIntegrity("", "t", siArgs("")); ok {
		t.Fatal("unqualified with no context must miss")
	}
	if _, _, ok := LookupStorageIntegrity("db1", "u", siArgs("")); ok {
		t.Fatal("non-SI table must miss")
	}
	if _, _, ok := LookupStorageIntegrity("db1", "t", &pb.RewriteTableDynamicArgs{}); ok {
		t.Fatal("no storage_integrity block must miss")
	}
	if _, _, ok := LookupStorageIntegrity("db1", "t", nil); ok {
		t.Fatal("nil args must miss")
	}
}

func TestReservedRowIDColumn(t *testing.T) {
	if got := ReservedRowIDColumn(siArgs("")); got != "_hg_row_id" {
		t.Fatalf("default = %q", got)
	}
	a := siArgs("")
	a.StorageIntegrity.ReservedRowIdColumn = "_rid"
	if got := ReservedRowIDColumn(a); got != "_rid" {
		t.Fatalf("explicit = %q", got)
	}
	if got := ReservedRowIDColumn(nil); got != "_hg_row_id" {
		t.Fatalf("nil = %q", got)
	}
}

func TestResolveAccessed_flagsStorageIntegrity(t *testing.T) {
	sel := Selection{Mode: ModeDynamic, Dynamic: siArgs("db1")}
	if a := ResolveAccessed("db1", "t", sel); !a.IsStorageIntegrity || a.LogicalDB != "db1" || a.PhysicalDB != "phys" {
		t.Fatalf("qualified: %+v", a)
	}
	if a := ResolveAccessed("", "t", sel); !a.IsStorageIntegrity {
		t.Fatalf("via upstream: %+v", a)
	}
	if a := ResolveAccessed("db1", "u", sel); a.IsStorageIntegrity {
		t.Fatalf("non-SI must not be flagged: %+v", a)
	}
}

func TestStorageIntegrityWriteRejectMessage(t *testing.T) {
	want := "storage-integrity table db1.t accepts writes only through the signed statement lane"
	if got := StorageIntegrityWriteRejectMessage("db1.t"); got != want {
		t.Fatalf("got %q", got)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/nameresolve -run 'TestLookupStorageIntegrity|TestReservedRowIDColumn|TestResolveAccessed_flagsStorageIntegrity|TestStorageIntegrityWriteRejectMessage' -count=1`
Expected: compile error `undefined: LookupStorageIntegrity` (and the others).

- [ ] **Step 3: Implement in `resolve.go`**

Add the field to `Accessed`:

```go
type Accessed struct {
	LogicalDB          string
	PhysicalDB         string
	IsRemote           bool
	IsStorageIntegrity bool // logical key is in storage_integrity.tables (Spec G)
}
```

In `ResolveAccessed`, dynamic branch, before `return Accessed{...}`:

```go
		_, _, isSI := LookupStorageIntegrity(db, table, sel.Dynamic)
		return Accessed{LogicalDB: logical, PhysicalDB: phys, IsRemote: isRemote, IsStorageIntegrity: isSI}
```

Append the new functions:

```go
// DefaultReservedRowIDColumn is the protocol row-identity column hidden from
// the logical surface (Spec G D3).
const DefaultReservedRowIDColumn = "_hg_row_id"

// LookupStorageIntegrity reports whether (db, table) — db resolved through
// upstream_logical_database_in_context when empty — is a key of
// dynamic_args.storage_integrity.tables. It is consulted BEFORE the ordinary
// dynamic resolution by every table-targeting handler; the SI mapping wins.
// It never rejects: an unqualified target with no context simply misses.
func LookupStorageIntegrity(db, table string, a *pb.RewriteTableDynamicArgs) (*pb.StorageIntegrityArgs_Table, string, bool) {
	tables := a.GetStorageIntegrity().GetTables()
	if len(tables) == 0 || table == "" {
		return nil, "", false
	}
	logical := db
	if logical == "" {
		logical = a.GetUpstreamLogicalDatabaseInContext()
	}
	if logical == "" {
		return nil, "", false
	}
	key := logical + "." + table
	tbl, ok := tables[key]
	if !ok || tbl == nil {
		return nil, "", false
	}
	return tbl, key, true
}

// ReservedRowIDColumn returns storage_integrity.reserved_row_id_column, or the
// protocol default when unset.
func ReservedRowIDColumn(a *pb.RewriteTableDynamicArgs) string {
	if rid := a.GetStorageIntegrity().GetReservedRowIdColumn(); rid != "" {
		return rid
	}
	return DefaultReservedRowIDColumn
}

// StorageIntegrityWriteRejectMessage is the shared (Go + C++) message for any
// non-lane write/DDL touching an SI table (Spec G §4.4).
func StorageIntegrityWriteRejectMessage(logicalKey string) string {
	return "storage-integrity table " + logicalKey + " accepts writes only through the signed statement lane"
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/nameresolve -count=1`
Expected: `ok  	github.com/housegate/rewriter-go/internal/nameresolve`.

- [ ] **Step 5: Commit**

```bash
git add internal/nameresolve/resolve.go internal/nameresolve/resolve_test.go
git commit -m "feat(nameresolve): storage-integrity table lookup, reserved column, accessed flag"
```

### Task 5: `engine` — `ActionSubquery` derived-table substitution + `ReferencesIdentifier`

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-go`

**Files:**
- Modify: `internal/engine/nodes.go` (`TableAction` consts ~:20-24, `TableDecision` ~:30-35, `applyDecision` ~:175-218)
- Test: `internal/engine/nodes_test.go`

**Interfaces:**
- Produces: `engine.ActionSubquery TableAction` (after `ActionRemote`); `TableDecision.Subquery AST` (a parsed `{"select":…}` / `{"union":…}` node from `Engine.ParseOne`, used as the derived-table body; the alias is the user alias or the original qualified name, exactly like `ActionRemote`); `func ReferencesIdentifier(ast AST, name string) (bool, error)` (any `column` node whose final name part equals `name`, or any `star.except/replace/rename` entry named `name`; string literals do not count).

- [ ] **Step 1: Write the failing tests (append to `nodes_test.go`)**

```go
func TestRewriteSelectTables_subquerySubstitution(t *testing.T) {
	if os.Getenv("POLYGLOT_SQL_FFI_PATH") == "" {
		t.Skip("needs engine")
	}
	e, err := NewPolyglot("")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	body, err := e.ParseOne(`SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db__t`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := RewriteSelectTables(load(t, "select"), func(tt TableTarget) TableDecision {
		return TableDecision{Action: ActionSubquery, Subquery: body}
	})
	if err != nil {
		t.Fatal(err)
	}
	got := genOf(t, out)
	want := `SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db__t) AS "db.t" WHERE x IN (1, 2)`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestRewriteSelectTables_subqueryKeepsUserAliasInJoin(t *testing.T) {
	if os.Getenv("POLYGLOT_SQL_FFI_PATH") == "" {
		t.Skip("needs engine")
	}
	e, err := NewPolyglot("")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ast, err := e.ParseOne(`SELECT count() FROM db.t AS a JOIN db.u AS b ON a.id = b.id`)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := e.ParseOne(`SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db__t`)
	out, err := RewriteSelectTables(ast, func(tt TableTarget) TableDecision {
		if tt.Table == "t" {
			return TableDecision{Action: ActionSubquery, Subquery: body}
		}
		return TableDecision{Action: ActionSkip}
	})
	if err != nil {
		t.Fatal(err)
	}
	got := genOf(t, out)
	want := `SELECT count() FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db__t) AS a JOIN db.u AS b ON a.id = b.id`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	// The substituted body's own table must NOT be re-visited/collected.
	tabs, err := CollectSelectTables(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range tabs {
		if tt.DB == "hg_safe" {
			t.Fatalf("substituted body table leaked into collection: %+v", tabs)
		}
	}
}

func TestReferencesIdentifier(t *testing.T) {
	if os.Getenv("POLYGLOT_SQL_FFI_PATH") == "" {
		t.Skip("needs engine")
	}
	e, err := NewPolyglot("")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	cases := []struct {
		sql  string
		want bool
	}{
		{`SELECT _hg_row_id FROM t`, true},
		{`SELECT a FROM t WHERE _hg_row_id = 'x'`, true},
		{`SELECT a FROM t ORDER BY _hg_row_id`, true},
		{`SELECT lower(t._hg_row_id) FROM t`, true},
		{`SELECT * EXCEPT (_hg_row_id) FROM t`, true},
		{`SELECT a FROM t WHERE b IN (SELECT _hg_row_id FROM u)`, true},
		{`SELECT a FROM t`, false},
		{`SELECT '_hg_row_id' FROM t`, false},
		{`SELECT hg_row_id FROM t`, false},
	}
	for _, c := range cases {
		ast, err := e.ParseOne(c.sql)
		if err != nil {
			t.Fatalf("parse %q: %v", c.sql, err)
		}
		got, err := ReferencesIdentifier(ast, "_hg_row_id")
		if err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("%q: got %v want %v", c.sql, got, c.want)
		}
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/engine -run 'TestRewriteSelectTables_subquery|TestReferencesIdentifier' -count=1`
Expected: compile error `undefined: ActionSubquery` / `unknown field Subquery` / `undefined: ReferencesIdentifier`.

- [ ] **Step 3: Implement in `nodes.go`**

Constants and decision:

```go
const (
	ActionSkip     TableAction = iota // leave the node untouched
	ActionRename                      // set table name (+ optionally schema/db)
	ActionRemote                      // replace the table expr with remote(...)
	ActionSubquery                    // replace the table expr with a derived table (Spec G SI surface)
)

// TableDecision is what a caller returns for a TableTarget.
type TableDecision struct {
	Action   TableAction
	NewDB    string      // ActionRename: new schema; "" keeps the existing schema untouched
	NewTable string      // ActionRename: new table name
	Remote   *RemoteSpec // ActionRemote: the remote() args
	// Subquery is the derived-table body for ActionSubquery: a parsed single
	// statement ({"select":…} or {"union":…}) obtained from Engine.ParseOne.
	// The alias is the user's alias, else the original qualified name —
	// same rule as ActionRemote — so column qualifiers keep resolving.
	Subquery AST
}
```

New `applyDecision` case (add before `case ActionSkip:`):

```go
	case ActionSubquery:
		if len(d.Subquery) == 0 {
			return // misconfigured decision — leave the table untouched
		}
		var body any
		if err := json.Unmarshal(d.Subquery, &body); err != nil {
			return
		}
		aliasName := tt.Alias
		if aliasName == "" {
			aliasName = originName(tt)
		}
		delete(expr, "table")
		// Shape mirrors what polyglot emits for `FROM (SELECT …) AS x`
		// (see testdata/ast-shapes/select_subquery_from.json).
		expr["subquery"] = map[string]any{
			"this":              body,
			"alias":             ident(aliasName),
			"alias_explicit_as": true,
			"alias_keyword":     "AS",
			"column_aliases":    []any{},
			"lateral":           false,
			"limit":             nil,
			"modifiers_inside":  false,
			"offset":            nil,
			"order_by":          nil,
			"trailing_comments": []any{},
		}
```

New function (append to `nodes.go`):

```go
// ReferencesIdentifier reports whether any column reference in the AST has
// the final name part `name` (bare `_hg_row_id`, qualified `t._hg_row_id`,
// in select list / WHERE / ORDER BY / function args / subqueries), or any
// `* EXCEPT|REPLACE|RENAME (...)` entry names it. String literals never
// match. Used for the Spec G reserved-column guard.
func ReferencesIdentifier(ast AST, name string) (bool, error) {
	var root any
	if err := json.Unmarshal(ast, &root); err != nil {
		return false, fmt.Errorf("engine: decode: %w", err)
	}
	return refWalk(root, name), nil
}

func refWalk(node any, name string) bool {
	switch n := node.(type) {
	case map[string]any:
		if col, ok := n["column"].(map[string]any); ok && identName(col["name"]) == name {
			return true
		}
		if star, ok := n["star"].(map[string]any); ok {
			for _, k := range []string{"except", "replace", "rename"} {
				if list, ok := star[k].([]any); ok {
					for _, e := range list {
						if identName(e) == name {
							return true
						}
					}
				}
			}
		}
		for _, v := range n {
			if refWalk(v, name) {
				return true
			}
		}
	case []any:
		for _, v := range n {
			if refWalk(v, name) {
				return true
			}
		}
	}
	return false
}
```

- [ ] **Step 4: Run tests**

Run: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/engine -count=1`
Expected: `ok  	github.com/housegate/rewriter-go/internal/engine` (incl. `TestCharacterizeAST` regenerating fixtures — check `git status` shows no fixture drift).

- [ ] **Step 5: Commit**

```bash
git add internal/engine/nodes.go internal/engine/nodes_test.go
git commit -m "feat(engine): ActionSubquery derived-table substitution and ReferencesIdentifier"
```

### Task 6: `handlers/select.go` — SI derived tables, reserved-column reject, SI accessed flag

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-go`

**Files:**
- Create: `internal/handlers/storage_integrity.go`, `internal/handlers/storage_integrity_test.go`
- Modify: `internal/handlers/select.go` (`RewriteSelect` :18-29, `rewriteSelectCore` :97-108, `buildAccessed` :151-177), `internal/handlers/writes.go` (`dispatchView` — propagate body reject)

**Interfaces:**
- Consumes: Task 4 `nameresolve.LookupStorageIntegrity/ReservedRowIDColumn`, Task 5 `engine.ActionSubquery/ReferencesIdentifier`.
- Produces: `func storageIntegritySurfaceSQL(tbl *pb.StorageIntegrityArgs_Table, args *pb.StorageIntegrityArgs) string`; `func splitPhysicalName(name string) (db, table string)` (split at first `.`; `("", name)` when none); `const reservedColumnRejectFmt = "reserved column %s is not addressable"`.

- [ ] **Step 1: Write failing handler tests (`storage_integrity_test.go`)**

```go
package handlers

import (
	"strings"
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)

func siDyn(mode pb.StorageIntegrityArgs_ReadMode, excluded ...string) *pb.RewriteTableDynamicArgs {
	return &pb.RewriteTableDynamicArgs{
		DatabaseMap:            map[string]string{"db1": "phys", "other": "phys"},
		KnownPhysicalDatabases: []string{"phys"},
		Delim:                  "_",
		StorageIntegrity: &pb.StorageIntegrityArgs{
			Tables: map[string]*pb.StorageIntegrityArgs_Table{
				"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t", ExcludedUnsafeParts: excluded},
			},
			ReadMode: mode,
		},
	}
}

func TestStorageIntegritySurfaceSQL(t *testing.T) {
	tbl := &pb.StorageIntegrityArgs_Table{SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"}
	safe := storageIntegritySurfaceSQL(tbl, &pb.StorageIntegrityArgs{ReadMode: pb.StorageIntegrityArgs_READ_MODE_SAFE})
	if safe != "SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t" {
		t.Fatalf("safe = %q", safe)
	}
	unspecified := storageIntegritySurfaceSQL(tbl, &pb.StorageIntegrityArgs{})
	if unspecified != safe {
		t.Fatalf("UNSPECIFIED must behave as SAFE: %q", unspecified)
	}
	tbl.ExcludedUnsafeParts = []string{"all_1_1_0", "it's"}
	unsafe := storageIntegritySurfaceSQL(tbl, &pb.StorageIntegrityArgs{ReadMode: pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST, ReservedRowIdColumn: "_rid"})
	want := "SELECT * EXCEPT (_rid) FROM hg_safe.db1__t UNION ALL SELECT * EXCEPT (_rid) FROM hg_unsafe.db1__t WHERE _part NOT IN ('all_1_1_0', 'it''s')"
	if unsafe != want {
		t.Fatalf("unsafe_latest = %q\nwant %q", unsafe, want)
	}
	tbl.ExcludedUnsafeParts = nil
	if got := storageIntegritySurfaceSQL(tbl, &pb.StorageIntegrityArgs{ReadMode: pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST}); strings.Contains(got, "WHERE") {
		t.Fatalf("empty exclusion list must omit WHERE: %q", got)
	}
}

func TestRewriteSelect_storageIntegritySafe(t *testing.T) {
	e := newEngine(t)
	ast, _ := e.ParseOne("SELECT a FROM db1.t AS x JOIN other.u AS y ON x.id = y.id")
	resp, err := RewriteSelect(e, ast, dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Code != pb.RewriteCode_Success {
		t.Fatalf("code = %v (%s)", resp.Code, resp.Message)
	}
	want := `SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS x JOIN phys."other.u" AS y ON x.id = y.id`
	if resp.SqlAfterRewrite != want {
		t.Fatalf("sql = %q\nwant %q", resp.SqlAfterRewrite, want)
	}
	if !mapEq(resp.TableRewrites, map[string]string{"db1.t": "hg_safe.db1__t", "other.u": "phys.other.u"}) {
		t.Fatalf("table_rewrites = %v", resp.TableRewrites)
	}
	var si, plain bool
	for _, a := range resp.OriginalAccessedTables {
		switch a.OriginalTable {
		case "t":
			si = a.IsStorageIntegrity && a.LogicalDatabase == "db1" && a.PhysicalDatabase == "phys"
		case "u":
			plain = !a.IsStorageIntegrity
		}
	}
	if !si || !plain {
		t.Fatalf("accessed = %+v", resp.OriginalAccessedTables)
	}
}

func TestRewriteSelect_storageIntegrityUnsafeLatest(t *testing.T) {
	e := newEngine(t)
	ast, _ := e.ParseOne("SELECT a FROM db1.t")
	resp, err := RewriteSelect(e, ast, dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST, "all_1_1_0", "all_2_2_0")))
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT a FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t UNION ALL SELECT * EXCEPT (_hg_row_id) FROM hg_unsafe.db1__t WHERE _part NOT IN ('all_1_1_0', 'all_2_2_0')) AS "db1.t"`
	if resp.SqlAfterRewrite != want {
		t.Fatalf("sql = %q\nwant %q", resp.SqlAfterRewrite, want)
	}
}

func TestRewriteSelect_reservedColumnRejected(t *testing.T) {
	e := newEngine(t)
	for _, sql := range []string{
		"SELECT _hg_row_id FROM db1.t",
		"SELECT a FROM db1.t WHERE _hg_row_id = 'x'",
		"SELECT a FROM other.u WHERE a IN (SELECT _hg_row_id FROM db1.t)",
	} {
		ast, _ := e.ParseOne(sql)
		resp, err := RewriteSelect(e, ast, dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE)))
		if err != nil {
			t.Fatal(err)
		}
		if resp.Code != pb.RewriteCode_RewriteError || resp.Message != "reserved column _hg_row_id is not addressable" {
			t.Fatalf("%q: code=%v msg=%q", sql, resp.Code, resp.Message)
		}
		if resp.SqlAfterRewrite != "" {
			t.Fatalf("%q: reject must leave sql_after_rewrite empty for finalize to echo the input, got %q", sql, resp.SqlAfterRewrite)
		}
		if len(resp.OriginalAccessedTables) == 0 {
			t.Fatalf("%q: accessed must be recorded before the reject", sql)
		}
	}
}

func TestRewriteSelect_reservedColumnOnNonSITableAllowed(t *testing.T) {
	e := newEngine(t)
	ast, _ := e.ParseOne("SELECT _hg_row_id FROM other.u")
	resp, err := RewriteSelect(e, ast, dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Code != pb.RewriteCode_Success {
		t.Fatalf("statement touching no SI table must not be guarded: %v %s", resp.Code, resp.Message)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/handlers -run 'TestStorageIntegritySurfaceSQL|TestRewriteSelect_storageIntegrity|TestRewriteSelect_reservedColumn' -count=1`
Expected: compile error `undefined: storageIntegritySurfaceSQL`.

- [ ] **Step 3: Create `internal/handlers/storage_integrity.go`**

```go
package handlers

import (
	"fmt"
	"strings"

	"github.com/housegate/rewriter-go/internal/engine"
	"github.com/housegate/rewriter-go/internal/nameresolve"
	"github.com/housegate/rewriter-proto/gen/pb"
)

// reservedColumnRejectFmt is the shared (Go + C++) message for a user
// reference to the protocol row-id column (Spec G D3).
const reservedColumnRejectFmt = "reserved column %s is not addressable"

// splitPhysicalName splits "hg_safe.db1__t" at the FIRST dot into
// (db, table). Physical SI names never contain a dot inside the table part
// (CHTableName replaces "." with "__"). A name without a dot is a bare table.
func splitPhysicalName(name string) (string, string) {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "", name
}

// storageIntegritySurfaceSQL renders the derived-table body for one SI table:
//
//	SAFE (and UNSPECIFIED): SELECT * EXCEPT (<rid>) FROM <safe>
//	UNSAFE_LATEST:          … UNION ALL SELECT * EXCEPT (<rid>) FROM <unsafe>
//	                        [WHERE _part NOT IN ('p1', 'p2')]   (omitted when empty)
//
// The caller parses it with Engine.ParseOne and hands the AST to
// engine.ActionSubquery. Identifiers go through engine.QuoteQualified so the
// text re-parses under polyglot's ClickHouse dialect; part names are
// single-quoted literals with '' escaping.
func storageIntegritySurfaceSQL(tbl *pb.StorageIntegrityArgs_Table, args *pb.StorageIntegrityArgs) string {
	rid := args.GetReservedRowIdColumn()
	if rid == "" {
		rid = nameresolve.DefaultReservedRowIDColumn
	}
	project := "SELECT * EXCEPT (" + engine.QuoteQualified("", rid) + ") FROM "
	sdb, st := splitPhysicalName(tbl.GetSafeTable())
	out := project + engine.QuoteQualified(sdb, st)
	if args.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST {
		return out
	}
	udb, ut := splitPhysicalName(tbl.GetUnsafeTable())
	out += " UNION ALL " + project + engine.QuoteQualified(udb, ut)
	if parts := tbl.GetExcludedUnsafeParts(); len(parts) > 0 {
		quoted := make([]string, len(parts))
		for i, p := range parts {
			quoted[i] = "'" + escapeSQLLiteral(p) + "'"
		}
		out += " WHERE _part NOT IN (" + strings.Join(quoted, ", ") + ")"
	}
	return out
}

// storageIntegrityDecision builds the ActionSubquery decision for one SI
// table reference and records table_rewrites[origin] = safe_table.
func storageIntegrityDecision(e engine.Engine, tt engine.TableTarget, tbl *pb.StorageIntegrityArgs_Table, args *pb.StorageIntegrityArgs, rewrites map[string]string) (engine.TableDecision, error) {
	body, err := e.ParseOne(storageIntegritySurfaceSQL(tbl, args))
	if err != nil {
		return engine.TableDecision{}, fmt.Errorf("storage-integrity surface for %s: %w", qualify(tt.DB, tt.Table), err)
	}
	recordRewrite(rewrites, tt, "", tbl.GetSafeTable())
	return engine.TableDecision{Action: engine.ActionSubquery, Subquery: body}, nil
}
```

- [ ] **Step 4: Wire it into `select.go`**

Replace the `RewriteSelectTables` call in `rewriteSelectCore` (lines ~103-108) with the SI-aware version, and add the reserved-column guard right after `resp.OriginalAccessedTables = buildAccessed(originals, sel)`:

```go
	originals, err := engine.CollectSelectTables(ast)
	if err != nil {
		return nil, nil, err
	}
	resp.OriginalAccessedTables = buildAccessed(originals, sel)

	// Spec G D3: a statement that touches at least one SI table must not
	// address the reserved row-id column anywhere. Checked on the ORIGINAL
	// AST (the substituted bodies legitimately mention it in EXCEPT).
	if sel.Mode == nameresolve.ModeDynamic && touchesStorageIntegrity(resp.OriginalAccessedTables) {
		rid := nameresolve.ReservedRowIDColumn(sel.Dynamic)
		hit, herr := engine.ReferencesIdentifier(ast, rid)
		if herr != nil {
			return nil, nil, herr
		}
		if hit {
			resp.Code = pb.RewriteCode_RewriteError
			resp.Message = fmt.Sprintf(reservedColumnRejectFmt, rid)
			return ast, resp, nil
		}
	}

	var siErr error
	rewritten, err := engine.RewriteSelectTables(ast, func(tt engine.TableTarget) engine.TableDecision {
		if sel.Mode == nameresolve.ModeDynamic {
			if tbl, _, ok := nameresolve.LookupStorageIntegrity(tt.DB, tt.Table, sel.Dynamic); ok {
				d, derr := storageIntegrityDecision(e, tt, tbl, sel.Dynamic.GetStorageIntegrity(), resp.TableRewrites)
				if derr != nil {
					siErr = derr
					return engine.TableDecision{Action: engine.ActionSkip}
				}
				return d
			}
		}
		return decideTable(tt, sel, resp.TableRewrites)
	})
	if err != nil {
		return nil, nil, err
	}
	if siErr != nil {
		return nil, nil, siErr
	}
```

Add the helper at the bottom of `select.go`:

```go
// touchesStorageIntegrity reports whether any accessed table is SI-flagged.
func touchesStorageIntegrity(accessed []*pb.AccessedTable) bool {
	for _, a := range accessed {
		if a.GetIsStorageIntegrity() {
			return true
		}
	}
	return false
}
```

Make `RewriteSelect` skip generation on a reject (so `finalize` echoes the input):

```go
func RewriteSelect(e engine.Engine, ast engine.AST, opts []*pb.RewriteOption) (*pb.RewriteSQLResponse, error) {
	rewritten, resp, err := rewriteSelectCore(e, ast, opts)
	if err != nil {
		return nil, err
	}
	if resp.Code != pb.RewriteCode_Success {
		return resp, nil // reject: leave SqlAfterRewrite empty; native.finalize echoes the input
	}
	sql, err := e.Generate(rewritten)
	...
```

In `buildAccessed`, add `IsStorageIntegrity: a.IsStorageIntegrity` to the `&pb.AccessedTable{...}` literal. Add `"fmt"` to select.go imports.

In `writes.go` `dispatchView`, right after `newBody, bodyResp, err := rewriteSelectCore(e, body, opts)` error check, propagate a body reject:

```go
			if bodyResp.Code != pb.RewriteCode_Success {
				mergeViewBody(resp, bodyResp)
				resp.Code, resp.Message = bodyResp.Code, bodyResp.Message
				return resp, true, nil
			}
```

- [ ] **Step 5: Run tests**

Run: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/handlers -count=1`
Expected: `ok  	github.com/housegate/rewriter-go/internal/handlers`.
Run: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/harness -run TestStorageIntegrityGolden -count=1 2>&1 | grep -E '^\s+--- (PASS|FAIL)'`
Expected: PASS for `si_safe_plain_select`, `si_safe_alias_join_non_si`, `si_unsafe_latest_*`, `si_safe_subquery_in_where`, `si_reserved_column_*`, `si_star_hides_rid`, `si_use_default_database`, `non_si_table_unaffected`, `si_absent_args_ordinary_rewrite`; the write/EXISTS/DESCRIBE cases still FAIL.

- [ ] **Step 6: Commit**

```bash
git add internal/handlers/storage_integrity.go internal/handlers/storage_integrity_test.go internal/handlers/select.go internal/handlers/writes.go
git commit -m "feat(handlers): storage-integrity SELECT surface (safe/unsafe_latest), reserved-column reject"
```

### Task 7: `handlers/writes.go`, `exists.go`, `grant.go` — SI write rejection, EXISTS → safe table, SHOW CREATE reject

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-go`

**Files:**
- Modify: `internal/handlers/writes.go` (`decideWriteTarget` :83-102, `recordAccessedWrite` :107-117), `internal/handlers/exists.go` (:21-52), `internal/handlers/grant.go` (:90-104 — the per-privilege loop before `if !resolved {`)
- Test: `internal/handlers/storage_integrity_test.go` (append)

**Interfaces:**
- Consumes: Task 4 helpers, Task 6 `splitPhysicalName`.
- Produces: SI branch semantics — `decideWriteTarget` rejects every non-INSERT slot resolving to an SI table with `nameresolve.StorageIntegrityWriteRejectMessage(key)` (accessed recorded first, `IsStorageIntegrity=true`); `RewriteExistsShowCreate`: EXISTS → `EXISTS TABLE <safe>` (`table_rewrites[key] = safe_table`), SHOW CREATE → `UnsupportedStatement` `"SHOW CREATE TABLE on storage-integrity table <key> is not supported"`; `RewriteGrant`: table-scoped GRANT/REVOKE on an SI table → `UnsupportedStatement` with the write message.

- [ ] **Step 1: Append failing tests**

```go
func TestWrites_storageIntegrityRejectsNonLane(t *testing.T) {
	e := newEngine(t)
	opts := dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE))
	want := "storage-integrity table db1.t accepts writes only through the signed statement lane"
	for _, sql := range []string{
		"ALTER TABLE db1.t DELETE WHERE a = 1",
		"DROP TABLE db1.t",
		"TRUNCATE TABLE db1.t",
		"CREATE TABLE db1.t (a UInt32) ENGINE = MergeTree ORDER BY a",
		"CREATE TABLE db1.t2 AS db1.t",
		"RENAME TABLE db1.t TO db1.t2",
		"EXCHANGE TABLES db1.t AND db1.t2",
		"ALTER TABLE db1.t UPDATE a = 1 WHERE a = 2",
	} {
		ast, err := e.ParseOne(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		resp, handled, err := RewriteWrite(e, ast, sql, opts)
		if err != nil || !handled {
			t.Fatalf("%q: handled=%v err=%v", sql, handled, err)
		}
		if resp.Code != pb.RewriteCode_UnsupportedStatement || resp.Message != want {
			t.Fatalf("%q: code=%v msg=%q", sql, resp.Code, resp.Message)
		}
		if len(resp.OriginalAccessedTables) == 0 || !resp.OriginalAccessedTables[0].IsStorageIntegrity {
			t.Fatalf("%q: SI access must be recorded before the reject: %+v", sql, resp.OriginalAccessedTables)
		}
	}
}

func TestWrites_storageIntegrityInsertRewritesLikeToday(t *testing.T) {
	e := newEngine(t)
	sql := "INSERT INTO db1.t (a) VALUES (1)"
	ast, _ := e.ParseOne(sql)
	resp, handled, err := RewriteWrite(e, ast, sql, dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE)))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if resp.Code != pb.RewriteCode_Success {
		t.Fatalf("INSERT must stay on the ordinary path (D-1): %v %s", resp.Code, resp.Message)
	}
	if resp.SqlAfterRewrite != `INSERT INTO phys."db1.t" (a) VALUES (1)` {
		t.Fatalf("sql = %q", resp.SqlAfterRewrite)
	}
	if !resp.OriginalAccessedTables[0].IsStorageIntegrity {
		t.Fatalf("accessed must carry the SI flag: %+v", resp.OriginalAccessedTables)
	}
}

func TestExists_storageIntegrityMapsToSafe(t *testing.T) {
	e := newEngine(t)
	sql := "EXISTS TABLE db1.t"
	ast, _ := e.ParseOne(sql)
	resp, handled, err := RewriteExistsShowCreate(e, ast, sql, dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE)))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if resp.SqlAfterRewrite != "EXISTS TABLE hg_safe.db1__t" || resp.TableRewrites["db1.t"] != "hg_safe.db1__t" {
		t.Fatalf("sql=%q rewrites=%v", resp.SqlAfterRewrite, resp.TableRewrites)
	}
	if !resp.OriginalAccessedTables[0].IsStorageIntegrity {
		t.Fatalf("accessed = %+v", resp.OriginalAccessedTables)
	}
}

func TestShowCreate_storageIntegrityRejected(t *testing.T) {
	e := newEngine(t)
	sql := "SHOW CREATE TABLE db1.t"
	ast, _ := e.ParseOne(sql)
	resp, _, err := RewriteExistsShowCreate(e, ast, sql, dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Code != pb.RewriteCode_UnsupportedStatement || resp.Message != "SHOW CREATE TABLE on storage-integrity table db1.t is not supported" {
		t.Fatalf("code=%v msg=%q", resp.Code, resp.Message)
	}
}

func TestGrant_storageIntegrityRejected(t *testing.T) {
	e := newEngine(t)
	sql := "GRANT SELECT ON db1.t TO u1"
	ast, _ := e.ParseOne(sql)
	resp, handled, err := RewriteGrant(e, ast, sql, dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE)))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if resp.Code != pb.RewriteCode_UnsupportedStatement || !strings.Contains(resp.Message, "storage-integrity table db1.t accepts writes only") {
		t.Fatalf("code=%v msg=%q", resp.Code, resp.Message)
	}
	// Database-scoped grants are not table-targeting and stay allowed.
	sql = "GRANT SELECT ON db1.* TO u1"
	ast, _ = e.ParseOne(sql)
	resp, _, _ = RewriteGrant(e, ast, sql, dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE)))
	if resp.Code != pb.RewriteCode_Success {
		t.Fatalf("db-scoped grant: code=%v msg=%q", resp.Code, resp.Message)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/handlers -run 'TestWrites_storageIntegrity|TestExists_storageIntegrity|TestShowCreate_storageIntegrity|TestGrant_storageIntegrity' -count=1`
Expected: FAIL — `code=Success msg="success"` for the reject cases; EXISTS produces ``EXISTS TABLE phys.`db1.t` ``.

- [ ] **Step 3: Implement**

`writes.go` — `decideWriteTarget` becomes:

```go
func decideWriteTarget(tt engine.TableTarget, kind string, sel nameresolve.Selection, resp *pb.RewriteSQLResponse) (engine.TableDecision, bool) {
	recordAccessedWrite(resp, tt, sel) // record BEFORE any reject (C++ writes.cc:118)
	// Spec G §4.4: every non-INSERT slot resolving to a storage-integrity
	// table is refused (INSERT stays on the ordinary path — the caller's
	// signed ingress owns that decision, see plan deviation D-1).
	if sel.Mode == nameresolve.ModeDynamic && kind != engine.NodeInsert {
		if _, key, ok := nameresolve.LookupStorageIntegrity(tt.DB, tt.Table, sel.Dynamic); ok {
			rejectUnsupported(resp, nameresolve.StorageIntegrityWriteRejectMessage(key))
			return engine.TableDecision{}, false
		}
	}
	o := nameresolve.Resolve(tt.DB, tt.Table, sel)
	... (unchanged)
```

`recordAccessedWrite`: add `IsStorageIntegrity: a.IsStorageIntegrity` to the `AccessedTable` literal.

`exists.go` — after `sel := nameresolve.FindActive(opts)` and `tt := …`, before `decideWriteTarget`:

```go
	if sel.Mode == nameresolve.ModeDynamic {
		if tbl, key, ok := nameresolve.LookupStorageIntegrity(tt.DB, tt.Table, sel.Dynamic); ok {
			recordAccessedWrite(resp, tt, sel)
			if t.Verb == engine.VerbShowCreate {
				// The physical DDL would expose engine/zk path/_hg_row_id (Spec G §4.3).
				rejectUnsupported(resp, "SHOW CREATE TABLE on storage-integrity table "+key+" is not supported")
				return resp, true, nil
			}
			db, table := splitPhysicalName(tbl.GetSafeTable())
			recordRewrite(resp.TableRewrites, tt, db, table)
			resp.SqlAfterRewrite = buildObjectSQL(keyword, t.Temporary, db, table)
			return resp, true, nil
		}
	}
```

`grant.go` — inside the `for _, p := range gp.Privileges {` loop, right after the column-level reject and before `if !resolved {`:

```go
		if !scopeDatabase {
			if _, key, ok := nameresolve.LookupStorageIntegrity(origDB, origTable, dyn); ok {
				rejectUnsupported(resp, nameresolve.StorageIntegrityWriteRejectMessage(key))
				return resp, true, nil
			}
		}
```

(`origDB`, `origTable`, `scopeDatabase`, `dyn` are the existing locals in `RewriteGrant`.)

- [ ] **Step 4: Run tests**

Run: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/handlers ./internal/harness -count=1 2>&1 | tail -20`
Expected: handlers `ok`; `TestStorageIntegrityGolden` now fails ONLY on `si_describe_metadata_select` (statement_type UNSPECIFIED, sql passthrough) — all others PASS; existing corpora `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/handlers/writes.go internal/handlers/exists.go internal/handlers/grant.go internal/handlers/storage_integrity_test.go
git commit -m "feat(handlers): reject non-lane writes/DDL on storage-integrity tables; EXISTS maps to hg_safe"
```

### Task 8: DESCRIBE — minimal handler (D-7), SI metadata SELECT, `STATEMENT_TYPE_DESCRIBE`

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-go`

**Files:**
- Create: `internal/handlers/describe.go`, `internal/handlers/describe_test.go`
- Modify: `internal/engine/objtarget.go` (:9-15 verbs, :36-52 switch), `internal/handlers/exists.go` (:31-33 verb guard), `native.go` (`doRewrite` :132-141 dispatch, `classifyCommand` :239-261)

**Interfaces:**
- Consumes: `engine.ParseObjectTarget`, Task 4/6 helpers.
- Produces: `engine.VerbDescribe ObjectVerb`; `func RewriteDescribe(e engine.Engine, ast engine.AST, sql string, opts []*pb.RewriteOption) (*pb.RewriteSQLResponse, bool, error)`; `func describeMetadataSQL(safeTable, rid string) string`.

> If Spec E's `handlers.RewriteDescribe` already exists when you get here: skip creating `describe.go`, add only the SI branch (`describeMetadataSQL` + the `LookupStorageIntegrity` check before its EXISTS-style resolution) and the tests below.

- [ ] **Step 1: Failing tests (`describe_test.go`)**

```go
package handlers

import (
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)

func TestDescribeMetadataSQL(t *testing.T) {
	want := "SELECT name, type, default_type, default_expression, comment, codec_expression, ttl_expression FROM system.columns WHERE database = 'hg_safe' AND table = 'db1__t' AND name != '_hg_row_id' ORDER BY position"
	if got := describeMetadataSQL("hg_safe.db1__t", "_hg_row_id"); got != want {
		t.Fatalf("got %q", got)
	}
}

func TestRewriteDescribe_storageIntegrity(t *testing.T) {
	e := newEngine(t)
	for _, sql := range []string{"DESCRIBE TABLE db1.t", "DESCRIBE db1.t", "DESC db1.t"} {
		ast, err := e.ParseOne(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		resp, handled, err := RewriteDescribe(e, ast, sql, dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE)))
		if err != nil || !handled {
			t.Fatalf("%q: handled=%v err=%v", sql, handled, err)
		}
		if resp.StatementType != pb.StatementType_STATEMENT_TYPE_DESCRIBE || resp.Code != pb.RewriteCode_Success {
			t.Fatalf("%q: stmt=%v code=%v", sql, resp.StatementType, resp.Code)
		}
		if resp.SqlAfterRewrite != describeMetadataSQL("hg_safe.db1__t", "_hg_row_id") {
			t.Fatalf("%q: sql=%q", sql, resp.SqlAfterRewrite)
		}
		if len(resp.OriginalAccessedTables) != 1 || !resp.OriginalAccessedTables[0].IsStorageIntegrity {
			t.Fatalf("%q: accessed=%+v", sql, resp.OriginalAccessedTables)
		}
	}
}

func TestRewriteDescribe_useDefaultDatabase(t *testing.T) {
	e := newEngine(t)
	dyn := siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE)
	dyn.UpstreamLogicalDatabaseInContext = "db1"
	ast, _ := e.ParseOne("DESCRIBE t")
	resp, _, err := RewriteDescribe(e, ast, "DESCRIBE t", dynOpt(dyn))
	if err != nil {
		t.Fatal(err)
	}
	if resp.SqlAfterRewrite != describeMetadataSQL("hg_safe.db1__t", "_hg_row_id") {
		t.Fatalf("sql=%q", resp.SqlAfterRewrite)
	}
}

func TestRewriteDescribe_nonSIPassesThrough(t *testing.T) {
	e := newEngine(t)
	ast, _ := e.ParseOne("DESCRIBE TABLE other.u")
	resp, handled, err := RewriteDescribe(e, ast, "DESCRIBE TABLE other.u", dynOpt(siDyn(pb.StorageIntegrityArgs_READ_MODE_SAFE)))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if resp.StatementType != pb.StatementType_STATEMENT_TYPE_DESCRIBE || resp.SqlAfterRewrite != "DESCRIBE TABLE other.u" {
		t.Fatalf("stmt=%v sql=%q (G-minimal: non-SI DESCRIBE passes through until Spec E D6)", resp.StatementType, resp.SqlAfterRewrite)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/handlers -run 'TestDescribe|TestRewriteDescribe' -count=1`
Expected: compile error `undefined: describeMetadataSQL` / `RewriteDescribe`.

- [ ] **Step 3: Implement**

`internal/engine/objtarget.go` — add the verb and recognise it:

```go
const (
	VerbNone       ObjectVerb = iota // not an EXISTS / SHOW CREATE / DESCRIBE statement
	VerbExists                       // EXISTS …
	VerbShowCreate                   // SHOW CREATE …
	VerbDescribe                     // DESCRIBE | DESC [TABLE] …
)
```

and in `ParseObjectTarget`'s `switch strings.ToUpper(toks[0].Text)` add:

```go
	case "DESCRIBE", "DESC":
		out.Verb, i = VerbDescribe, 1
```

`internal/handlers/exists.go` — replace `if t.Verb == engine.VerbNone {` with:

```go
	if t.Verb != engine.VerbExists && t.Verb != engine.VerbShowCreate {
		return nil, false, nil // not EXISTS / SHOW CREATE (DESCRIBE has its own handler) → caller falls through
	}
```

`internal/handlers/describe.go`:

```go
package handlers

import (
	"github.com/housegate/rewriter-go/internal/engine"
	"github.com/housegate/rewriter-go/internal/nameresolve"
	"github.com/housegate/rewriter-proto/gen/pb"
)

// describeMetadataSQL renders the metadata-shaped SELECT that stands in for
// DESCRIBE on a storage-integrity table (Spec G §4.3): the safe table's
// columns minus the reserved row-id column, in declaration order. Built as
// a string (not via the generator) so both engines emit byte-identical SQL.
func describeMetadataSQL(safeTable, rid string) string {
	db, table := splitPhysicalName(safeTable)
	return "SELECT name, type, default_type, default_expression, comment, codec_expression, ttl_expression FROM system.columns WHERE database = '" +
		escapeSQLLiteral(db) + "' AND table = '" + escapeSQLLiteral(table) + "' AND name != '" + escapeSQLLiteral(rid) + "' ORDER BY position"
}

// RewriteDescribe handles `DESCRIBE|DESC [TABLE] [db.]t` (an opaque command
// node under polyglot). G-minimal scope (plan deviation D-7): classify as
// STATEMENT_TYPE_DESCRIBE; a storage-integrity target becomes the
// system.columns metadata SELECT; any other target passes through unchanged
// (Spec E D6 adds the ordinary EXISTS-style physical resolution). Returns
// (resp, handled, err) with the RewriteWrite contract; native.go calls it
// before RewriteExistsShowCreate.
func RewriteDescribe(e engine.Engine, ast engine.AST, sql string, opts []*pb.RewriteOption) (*pb.RewriteSQLResponse, bool, error) {
	kind, err := engine.NodeKind(ast)
	if err != nil {
		return nil, false, err
	}
	if kind != engine.NodeCommand {
		return nil, false, nil
	}
	t, err := engine.ParseObjectTarget(e, sql)
	if err != nil {
		return nil, false, err
	}
	if t.Verb != engine.VerbDescribe {
		return nil, false, nil
	}
	resp := newWriteResp(pb.StatementType_STATEMENT_TYPE_DESCRIBE)
	sel := nameresolve.FindActive(opts)
	tt := engine.TableTarget{DB: t.DB, Table: t.Table}
	if sel.Mode == nameresolve.ModeDynamic {
		if tbl, _, ok := nameresolve.LookupStorageIntegrity(tt.DB, tt.Table, sel.Dynamic); ok {
			recordAccessedWrite(resp, tt, sel)
			resp.SqlAfterRewrite = describeMetadataSQL(tbl.GetSafeTable(), nameresolve.ReservedRowIDColumn(sel.Dynamic))
			return resp, true, nil
		}
	}
	recordAccessedWrite(resp, tt, sel)
	resp.SqlAfterRewrite = sql // pass through (Spec E D6 will resolve non-SI targets)
	return resp, true, nil
}
```

`native.go` — in `doRewrite`, before the `RewriteExistsShowCreate` block:

```go
	// Phase 4a: DESCRIBE (Spec G §4.3 / Spec E D6). Must run before
	// RewriteExistsShowCreate because both read the same tokenized command
	// node; exists.go now ignores VerbDescribe explicitly.
	if dresp, handled, derr := handlers.RewriteDescribe(e, ast, sql, opts); derr != nil {
		return nil, derr
	} else if handled {
		finalize(dresp, sql, ec)
		return dresp, nil
	}
```

and in `classifyCommand` add before the `default:` (note `DESCRIBE` also starts with `DESC`):

```go
	case strings.HasPrefix(u, "DESC"):
		return pb.StatementType_STATEMENT_TYPE_DESCRIBE
```

- [ ] **Step 4: Run tests**

Run: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/handlers ./internal/harness ./ -count=1 2>&1 | tail -20`
Expected: all `ok`; `TestStorageIntegrityGolden` fully green (23/23 subtests).

- [ ] **Step 5: Commit**

```bash
git add internal/engine/objtarget.go internal/handlers/exists.go internal/handlers/describe.go internal/handlers/describe_test.go native.go
git commit -m "feat: minimal DESCRIBE handler with storage-integrity metadata SELECT (STATEMENT_TYPE_DESCRIBE)"
```

### Task 9: rewriter-go — full suite, docs, release `v0.7.0`

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-go`

**Files:**
- Modify: `AGENTS.md` (CODE MAP + CONVENTIONS), `README.md` (Status paragraph), `internal/handlers/AGENTS.md`, `internal/harness/AGENTS.md` (WHERE TO LOOK rows)

- [ ] **Step 1: Full test run (pure + FFI + fidelity spike)**

Run: `go vet ./... && make test && POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go run ./cmd/fidelity-spike | tail -3`
Expected: every package `ok`; fidelity spike prints its summary line unchanged from `main` (no new parse failures — the SI paths only add nodes polyglot already round-trips).

- [ ] **Step 2: Docs**

Add to `AGENTS.md` CODE MAP: `| handlers.RewriteDescribe | function | internal/handlers/describe.go | DESCRIBE classification; SI metadata SELECT |`, `| nameresolve.LookupStorageIntegrity | function | internal/nameresolve/resolve.go | SI table lookup consulted before dynamic resolution |`, `| engine.ActionSubquery | const | internal/engine/nodes.go | derived-table substitution used by the SI read surface |`. Add to CONVENTIONS: "Storage-integrity (Spec G) goldens live in `internal/harness/testdata/storage_integrity_cases.json`; the C++ repo carries a byte-identical copy — change both in lockstep." Add the same one-line WHERE TO LOOK rows to `internal/handlers/AGENTS.md` (`storage_integrity.go`, `describe.go`) and `internal/harness/AGENTS.md` (`storage_integrity_golden_test.go`).

- [ ] **Step 3: Commit, PR, release**

```bash
git add AGENTS.md README.md internal/handlers/AGENTS.md internal/harness/AGENTS.md
git commit -m "docs: storage-integrity read surface (Spec G) knowledge-base rows"
git push -u origin feat/storage-integrity-read-surface
```

Open the PR, merge to `main`, then `gh workflow run release.yml --ref main` (major `0`). The date logic yields **`v0.7.0`** (first release on a later day than `v0.6.0`); note the printed tag as `<rewriter-go-tag>`.
Run: `gh release view v0.7.0 --repo housegate/rewriter-go --json assets --jq '.assets[].name'`
Expected: `libpolyglot_sql_ffi-linux-x86_64.so`, `libpolyglot_sql_ffi-macos-arm64.dylib`, `SHA256SUMS`.

---

## Part C — rewriter-grpc (C++ mirror)

All build/test commands in this part run on the remote box after an rsync. Use this exact loop for every "Run" step below (`$RB` = `ssh -p 30100 sentio@64.38.131.242`, `$WD` = `/home/sentio/chen/rewriter-grpc`):

```bash
rsync -az --delete --exclude='.git' --exclude='build/' --exclude='clickHouse/' --exclude='contrib' --exclude='docs/' -e "ssh -p 30100" ./ sentio@64.38.131.242:/home/sentio/chen/rewriter-grpc/
ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && ./scripts.sh rebuild"
```

### Task 10: Bump the proto submodule + red JSON-driven gtest over the shared corpus

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-grpc`

**Files:**
- Modify: `third_party/rewriter-proto` (submodule pin → `v0.2.0`), `tests/CMakeLists.txt` (test data dir define), `tests/rewriter_test.cc` (append the parametrised suite)
- Create: `tests/testdata/storage_integrity_cases.json` (byte-identical copy of rewriter-go's file)

**Interfaces:**
- Produces: gtest suite `StorageIntegrityGolden/*` reading `REWRITER_TEST_DATA_DIR/storage_integrity_cases.json`; helper `applyStorageIntegrityArgs(rewriter::RewriteTableDynamicArgs*, const Poco::JSON::Object::Ptr&)`.

- [ ] **Step 1: Bump the submodule and copy the corpus**

```bash
git checkout -b feat/storage-integrity-read-surface
git -C third_party/rewriter-proto fetch --tags && git -C third_party/rewriter-proto checkout v0.2.0
git add third_party/rewriter-proto
mkdir -p tests/testdata
cp ../rewriter-go/internal/harness/testdata/storage_integrity_cases.json tests/testdata/storage_integrity_cases.json
cmp tests/testdata/storage_integrity_cases.json ../rewriter-go/internal/harness/testdata/storage_integrity_cases.json && echo IDENTICAL
```
Expected: `IDENTICAL`.

- [ ] **Step 2: Test data path define in `tests/CMakeLists.txt`**

After `target_include_directories(rewriter_tests PRIVATE …)` add:

```cmake
target_compile_definitions(rewriter_tests PRIVATE
    REWRITER_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/testdata")
```

- [ ] **Step 3: Append the parametrised suite to `tests/rewriter_test.cc`**

```cpp
// ============================================================================
// Storage-integrity read surface (housegate Spec G) — shared golden corpus.
// tests/testdata/storage_integrity_cases.json is a byte-identical copy of
// rewriter-go/internal/harness/testdata/storage_integrity_cases.json; the Go
// harness compares sql_after_rewrite semantically, this suite applies the
// engine-agnostic want_sql_contains / want_sql_not_contains / sql_exact
// checks plus every structured field.
// ============================================================================
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <fstream>

namespace {

struct SIAccessed {
  std::string original_database, original_table, logical_database, physical_database;
  bool is_remote = false, is_storage_integrity = false;
};

struct SIGoldenCase {
  std::string name, sql, want_code, want_stmt, want_sql, want_message_contains;
  bool has_dynamic = false, sql_exact = false, reject = false;
  Poco::JSON::Object::Ptr dynamic; // raw; applied by applyStorageIntegrityArgs
  std::map<std::string, std::string> want_table_rewrites;
  bool has_table_rewrites = false, has_accessed = false;
  std::vector<SIAccessed> want_accessed;
  std::vector<std::string> want_sql_contains, want_sql_not_contains;
};

const std::map<std::string, rewriter::RewriteCode> kSICodeByName = {
  {"Success", rewriter::RewriteCode::Success},
  {"SyntaxError", rewriter::RewriteCode::SyntaxError},
  {"RewriteError", rewriter::RewriteCode::RewriteError},
  {"UnsupportedStatement", rewriter::RewriteCode::UnsupportedStatement},
  {"InvalidRewriteRequest", rewriter::RewriteCode::InvalidRewriteRequest},
};
const std::map<std::string, rewriter::StatementType> kSIStmtByName = {
  {"", rewriter::STATEMENT_TYPE_UNSPECIFIED}, {"SELECT", rewriter::STATEMENT_TYPE_SELECT},
  {"INSERT", rewriter::STATEMENT_TYPE_INSERT}, {"EXISTS_TABLE", rewriter::STATEMENT_TYPE_EXISTS_TABLE},
  {"SHOW_TABLES", rewriter::STATEMENT_TYPE_SHOW_TABLES}, {"DESCRIBE", rewriter::STATEMENT_TYPE_DESCRIBE},
};

std::vector<std::string> jsonStrings(const Poco::JSON::Object::Ptr &o, const std::string &key) {
  std::vector<std::string> out;
  if (!o || !o->has(key) || o->isNull(key)) return out;
  auto arr = o->getArray(key);
  for (size_t i = 0; i < arr->size(); ++i) out.push_back(arr->getElement<std::string>(i));
  return out;
}

void applyStorageIntegrityArgs(rewriter::RewriteTableDynamicArgs *dyn, const Poco::JSON::Object::Ptr &d) {
  if (d->has("database_map")) {
    auto m = d->getObject("database_map");
    for (const auto &k : *m) (*dyn->mutable_database_map())[k.first] = k.second.toString();
  }
  for (const auto &p : jsonStrings(d, "known_physical_databases")) dyn->add_known_physical_databases(p);
  if (d->has("upstream_logical_database_in_context"))
    dyn->set_upstream_logical_database_in_context(d->getValue<std::string>("upstream_logical_database_in_context"));
  if (d->has("delim")) dyn->set_delim(d->getValue<std::string>("delim"));
  if (!d->has("storage_integrity")) return;
  auto si = d->getObject("storage_integrity");
  auto *args = dyn->mutable_storage_integrity();
  const std::string mode = si->has("read_mode") ? si->getValue<std::string>("read_mode") : "";
  args->set_read_mode(mode == "UNSAFE_LATEST" ? rewriter::StorageIntegrityArgs::READ_MODE_UNSAFE_LATEST
                    : mode == "SAFE" ? rewriter::StorageIntegrityArgs::READ_MODE_SAFE
                    : rewriter::StorageIntegrityArgs::READ_MODE_UNSPECIFIED);
  if (si->has("reserved_row_id_column")) args->set_reserved_row_id_column(si->getValue<std::string>("reserved_row_id_column"));
  auto tables = si->getObject("tables");
  for (const auto &kv : *tables) {
    auto t = tables->getObject(kv.first);
    auto &out = (*args->mutable_tables())[kv.first];
    out.set_safe_table(t->getValue<std::string>("safe_table"));
    out.set_unsafe_table(t->getValue<std::string>("unsafe_table"));
    for (const auto &p : jsonStrings(t, "excluded_unsafe_parts")) out.add_excluded_unsafe_parts(p);
  }
}

std::vector<SIGoldenCase> loadSIGoldenCases() {
  std::ifstream in(std::string(REWRITER_TEST_DATA_DIR) + "/storage_integrity_cases.json");
  std::stringstream buf; buf << in.rdbuf();
  Poco::JSON::Parser parser;
  auto arr = parser.parse(buf.str()).extract<Poco::JSON::Array::Ptr>();
  std::vector<SIGoldenCase> out;
  for (size_t i = 0; i < arr->size(); ++i) {
    auto o = arr->getObject(i);
    SIGoldenCase c;
    c.name = o->getValue<std::string>("name");
    c.sql = o->getValue<std::string>("sql");
    if (o->has("want_code")) c.want_code = o->getValue<std::string>("want_code");
    if (o->has("want_stmt")) c.want_stmt = o->getValue<std::string>("want_stmt");
    if (o->has("want_sql")) c.want_sql = o->getValue<std::string>("want_sql");
    if (o->has("want_message_contains")) c.want_message_contains = o->getValue<std::string>("want_message_contains");
    if (o->has("sql_exact")) c.sql_exact = o->getValue<bool>("sql_exact");
    if (o->has("reject")) c.reject = o->getValue<bool>("reject");
    if (o->has("dynamic")) { c.has_dynamic = true; c.dynamic = o->getObject("dynamic"); }
    if (o->has("want_table_rewrites")) {
      c.has_table_rewrites = true;
      auto m = o->getObject("want_table_rewrites");
      for (const auto &kv : *m) c.want_table_rewrites[kv.first] = kv.second.toString();
    }
    if (o->has("want_accessed")) {
      c.has_accessed = true;
      auto a = o->getArray("want_accessed");
      for (size_t j = 0; j < a->size(); ++j) {
        auto e = a->getObject(j);
        SIAccessed w;
        if (e->has("original_database")) w.original_database = e->getValue<std::string>("original_database");
        if (e->has("original_table")) w.original_table = e->getValue<std::string>("original_table");
        if (e->has("logical_database")) w.logical_database = e->getValue<std::string>("logical_database");
        if (e->has("physical_database")) w.physical_database = e->getValue<std::string>("physical_database");
        if (e->has("is_remote")) w.is_remote = e->getValue<bool>("is_remote");
        if (e->has("is_storage_integrity")) w.is_storage_integrity = e->getValue<bool>("is_storage_integrity");
        c.want_accessed.push_back(w);
      }
    }
    c.want_sql_contains = jsonStrings(o, "want_sql_contains");
    c.want_sql_not_contains = jsonStrings(o, "want_sql_not_contains");
    out.push_back(std::move(c));
  }
  return out;
}

class StorageIntegrityGolden : public ::testing::TestWithParam<SIGoldenCase> {};

TEST_P(StorageIntegrityGolden, MatchesSharedCorpus) {
  const auto &c = GetParam();
  RewriterServiceImpl service;
  rewriter::RewriteSQLRequest req;
  req.set_sql(c.sql);
  if (c.has_dynamic) {
    auto *opt = req.add_options();
    opt->set_op(rewriter::RewriteOp::TableNameRewrite);
    applyStorageIntegrityArgs(opt->mutable_table_name_args()->mutable_dynamic_args(), c.dynamic);
  }
  rewriter::RewriteSQLResponse resp;
  grpc::ServerContext ctx;
  ASSERT_TRUE(service.Rewrite(&ctx, &req, &resp).ok());
  SCOPED_TRACE("sql_after_rewrite: " + resp.sql_after_rewrite() + " | message: " + resp.message());

  if (!c.want_code.empty()) EXPECT_EQ(resp.code(), kSICodeByName.at(c.want_code));
  EXPECT_EQ(resp.statement_type(), kSIStmtByName.at(c.want_stmt));
  if (!c.want_message_contains.empty()) EXPECT_NE(resp.message().find(c.want_message_contains), std::string::npos);
  if (c.has_table_rewrites) {
    std::map<std::string, std::string> got(resp.table_rewrites().begin(), resp.table_rewrites().end());
    EXPECT_EQ(got, c.want_table_rewrites);
  }
  if (c.has_accessed) {
    ASSERT_EQ(static_cast<size_t>(resp.original_accessed_tables_size()), c.want_accessed.size());
    for (const auto &w : c.want_accessed) {
      const auto *g = FindAccessed(resp, w.original_database, w.original_table);
      ASSERT_NE(g, nullptr) << w.original_database << "." << w.original_table;
      EXPECT_EQ(g->logical_database(), w.logical_database);
      EXPECT_EQ(g->physical_database(), w.physical_database);
      EXPECT_EQ(g->is_remote(), w.is_remote);
      EXPECT_EQ(g->is_storage_integrity(), w.is_storage_integrity);
    }
  }
  if (!c.reject && c.sql_exact) EXPECT_EQ(resp.sql_after_rewrite(), c.want_sql);
  for (const auto &s : c.want_sql_contains) EXPECT_NE(resp.sql_after_rewrite().find(s), std::string::npos) << "missing " << s;
  for (const auto &s : c.want_sql_not_contains) EXPECT_EQ(resp.sql_after_rewrite().find(s), std::string::npos) << "unexpected " << s;
}

INSTANTIATE_TEST_SUITE_P(SpecG, StorageIntegrityGolden, ::testing::ValuesIn(loadSIGoldenCases()),
  [](const ::testing::TestParamInfo<SIGoldenCase> &info) { return info.param.name; });

} // namespace
```

`FindAccessed` is the existing helper defined earlier in the same file (anonymous namespace at ~:727) — keep this block *below* it. If the include of `<Poco/JSON/Parser.h>` fails to resolve, add `Poco::JSON` to `target_link_libraries(rewriter_tests PRIVATE …)` in `tests/CMakeLists.txt` (ClickHouse's contrib Poco ships the JSON module).

- [ ] **Step 4: Build and run — must be RED**

Run: rsync + rebuild, then `$RB "cd $WD && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.*' 2>&1 | tail -40"`
Expected: build succeeds (proto v0.2.0 generates `has_storage_integrity()`); `[  FAILED  ]` for every `si_*` case, `[       OK ]` for `non_si_table_unaffected` and `si_absent_args_ordinary_rewrite`.

- [ ] **Step 5: Commit**

```bash
git add third_party/rewriter-proto tests/testdata/storage_integrity_cases.json tests/CMakeLists.txt tests/rewriter_test.cc
git commit -m "test: shared storage-integrity golden corpus + proto v0.2.0 (Spec G, red)"
```

### Task 11: `handlers/storage_integrity.{h,cc}` + SELECT derived table + reserved column + `is_storage_integrity`

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-grpc`

**Files:**
- Create: `src/handlers/storage_integrity.h`, `src/handlers/storage_integrity.cc`
- Modify: `src/handlers/name_rewrite.h` (`AccessedTableResolution` :~250), `src/handlers/name_rewrite.cc` (`resolveAccessedTable` dynamic branch, `recordAccessedTable`), `src/handlers/select.cc` (`dynamicRewriteWalk` :148-251, `handleSelectQuery` :754-824), `CMakeLists.txt` (`rewriter_core` sources :175-190)

**Interfaces:**
- Produces (namespace `rewriter_handlers`):
  - `constexpr std::string_view kDefaultReservedRowIdColumn = "_hg_row_id";`
  - `struct StorageIntegrityHit { const rewriter::StorageIntegrityArgs::Table *table; std::string logical_key; };`
  - `std::optional<StorageIntegrityHit> lookupStorageIntegrity(const std::string &origin_db, const std::string &origin_table, const rewriter::RewriteTableDynamicArgs &args);` (logical = origin_db or upstream ctx; key = logical + "." + table; miss when unqualified w/o context or block absent)
  - `std::string reservedRowIdColumn(const rewriter::RewriteTableDynamicArgs &args);`
  - `std::string storageIntegrityWriteRejectMessage(const std::string &logical_key);` → `"storage-integrity table " + key + " accepts writes only through the signed statement lane"`
  - `std::string storageIntegritySurfaceSQL(const rewriter::StorageIntegrityArgs::Table &t, const rewriter::StorageIntegrityArgs &args);` (same text as Go: `SELECT * EXCEPT (<rid>) FROM <safe>` [+ ` UNION ALL SELECT * EXCEPT (<rid>) FROM <unsafe>` [+ ` WHERE _part NOT IN ('p1', 'p2')`]]; identifiers backticked only when they contain chars outside `[A-Za-z0-9_]`; parts via `escapeSqlLiteral`)
  - `DB::ASTPtr buildStorageIntegrityDerivedTable(const rewriter::StorageIntegrityArgs::Table &t, const rewriter::StorageIntegrityArgs &args);` — `DB::ParserSelectWithUnionQuery` over the surface SQL → `std::make_shared<DB::ASTSubquery>(select_ast)`
  - `bool astReferencesIdentifier(const DB::ASTPtr &ast, const std::string &name);` — pre-order walk; true when an `DB::ASTIdentifier` that is NOT an `DB::ASTTableIdentifier` has `shortName() == name` (covers select list, WHERE, ORDER BY, function args, subqueries, and `ASTColumnsExceptTransformer` children); `ASTLiteral` never matches
  - `std::string describeMetadataSQL(const std::string &safe_table, const std::string &rid);` (Task 13 uses it; define here so both handlers share one string builder)
  - `AccessedTableResolution::is_storage_integrity` (bool) filled by `resolveAccessedTable` in dynamic mode; `recordAccessedTable` copies it into the proto.

- [ ] **Step 1: Write the header + implementation**

`src/handlers/storage_integrity.h` declares the functions above (`#pragma once`, includes `<Parsers/IAST.h>`, `<optional>`, `<string>`, `"rewriter.grpc.pb.h"`). `src/handlers/storage_integrity.cc`:

```cpp
// src/handlers/storage_integrity.cc — housegate Spec G read surface helpers.
#include "handlers/storage_integrity.h"

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>

#include "handlers/common.h"

namespace rewriter_handlers {

namespace {

bool needsQuoting(const std::string &s) {
  if (s.empty()) return true;
  for (size_t i = 0; i < s.size(); ++i) {
    const char c = s[i];
    const bool alpha = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
    const bool digit = c >= '0' && c <= '9';
    if (alpha) continue;
    if (digit && i > 0) continue;
    return true;
  }
  return false;
}
std::string quoteIdent(const std::string &s) { return needsQuoting(s) ? "`" + s + "`" : s; }

// "hg_safe.db1__t" → ("hg_safe", "db1__t"); split at the FIRST dot.
std::pair<std::string, std::string> splitPhysical(const std::string &name) {
  const auto dot = name.find('.');
  if (dot == std::string::npos) return {"", name};
  return {name.substr(0, dot), name.substr(dot + 1)};
}
std::string quotePhysical(const std::string &name) {
  auto [db, tbl] = splitPhysical(name);
  return db.empty() ? quoteIdent(tbl) : quoteIdent(db) + "." + quoteIdent(tbl);
}

} // namespace

std::optional<StorageIntegrityHit> lookupStorageIntegrity(
  const std::string &origin_db, const std::string &origin_table,
  const rewriter::RewriteTableDynamicArgs &args) {
  if (!args.has_storage_integrity() || args.storage_integrity().tables().empty() || origin_table.empty()) return std::nullopt;
  const std::string logical = origin_db.empty() ? args.upstream_logical_database_in_context() : origin_db;
  if (logical.empty()) return std::nullopt;
  const std::string key = logical + "." + origin_table;
  auto it = args.storage_integrity().tables().find(key);
  if (it == args.storage_integrity().tables().end()) return std::nullopt;
  return StorageIntegrityHit{&it->second, key};
}

std::string reservedRowIdColumn(const rewriter::RewriteTableDynamicArgs &args) {
  if (args.has_storage_integrity() && !args.storage_integrity().reserved_row_id_column().empty())
    return args.storage_integrity().reserved_row_id_column();
  return std::string(kDefaultReservedRowIdColumn);
}

std::string storageIntegrityWriteRejectMessage(const std::string &logical_key) {
  return "storage-integrity table " + logical_key + " accepts writes only through the signed statement lane";
}

std::string storageIntegritySurfaceSQL(const rewriter::StorageIntegrityArgs::Table &t,
                                       const rewriter::StorageIntegrityArgs &args) {
  const std::string rid = args.reserved_row_id_column().empty() ? std::string(kDefaultReservedRowIdColumn) : args.reserved_row_id_column();
  const std::string project = "SELECT * EXCEPT (" + quoteIdent(rid) + ") FROM ";
  std::string out = project + quotePhysical(t.safe_table());
  if (args.read_mode() != rewriter::StorageIntegrityArgs::READ_MODE_UNSAFE_LATEST) return out;
  out += " UNION ALL " + project + quotePhysical(t.unsafe_table());
  if (t.excluded_unsafe_parts_size() > 0) {
    out += " WHERE _part NOT IN (";
    for (int i = 0; i < t.excluded_unsafe_parts_size(); ++i) {
      if (i) out += ", ";
      out += "'" + escapeSqlLiteral(t.excluded_unsafe_parts(i)) + "'";
    }
    out += ")";
  }
  return out;
}

DB::ASTPtr buildStorageIntegrityDerivedTable(const rewriter::StorageIntegrityArgs::Table &t,
                                             const rewriter::StorageIntegrityArgs &args) {
  const std::string sql = storageIntegritySurfaceSQL(t, args);
  DB::ParserSelectWithUnionQuery parser;
  DB::ASTPtr select = DB::parseQuery(parser, sql.data(), sql.data() + sql.size(), /*description=*/"", 0, 0, 0);
  return std::make_shared<DB::ASTSubquery>(select);
}

bool astReferencesIdentifier(const DB::ASTPtr &ast, const std::string &name) {
  if (!ast) return false;
  if (const auto *id = ast->as<DB::ASTIdentifier>(); id && !ast->as<DB::ASTTableIdentifier>() && id->shortName() == name) return true;
  for (const auto &ch : ast->children) if (astReferencesIdentifier(ch, name)) return true;
  return false;
}

std::string describeMetadataSQL(const std::string &safe_table, const std::string &rid) {
  auto [db, tbl] = splitPhysical(safe_table);
  return "SELECT name, type, default_type, default_expression, comment, codec_expression, ttl_expression FROM system.columns WHERE database = '"
    + escapeSqlLiteral(db) + "' AND table = '" + escapeSqlLiteral(tbl) + "' AND name != '" + escapeSqlLiteral(rid) + "' ORDER BY position";
}

} // namespace rewriter_handlers
```

Add `src/handlers/storage_integrity.cc` to the `rewriter_core` source list in `CMakeLists.txt`.

- [ ] **Step 2: `name_rewrite` — accessed flag**

`name_rewrite.h`: add `bool is_storage_integrity = false;` to `AccessedTableResolution`. `name_rewrite.cc`: `#include "handlers/storage_integrity.h"`; in `resolveAccessedTable`'s dynamic branch, before `return out;` add `if (lookupStorageIntegrity(origin_db, origin_table, args)) out.is_storage_integrity = true;`; in `recordAccessedTable` add `entry->set_is_storage_integrity(resolution.is_storage_integrity);`.

- [ ] **Step 3: `select.cc` — SI branch in `dynamicRewriteWalk`, reserved guard in `handleSelectQuery`**

`#include "handlers/storage_integrity.h"`. In `dynamicRewriteWalk`, after the CTE-alias early-return and the db/table split, BEFORE `applyDynamicRewrite`:

```cpp
        if (auto hit = lookupStorageIntegrity(origin_db, origin_table, args)) {
          // Spec G: SI mapping wins over database_map. Replace the table
          // identifier with a derived table over hg_safe (∪ hg_unsafe).
          auto subquery = buildStorageIntegrityDerivedTable(*hit->table, args.storage_integrity());
          table_expression->children.clear();
          table_expression->database_and_table_name = nullptr;
          table_expression->subquery = subquery;
          table_expression->children.push_back(subquery);
          subquery->setAlias(origin_alias.empty() ? origin_full_name : origin_alias);
          if (out_table_rewrites) (*out_table_rewrites)[origin_full_name] = hit->table->safe_table();
          return; // never descend into the synthesized body
        }
```

In `handleSelectQuery`, move the `original_accessed_tables` population loop (currently after `forceGlobalForRemoteAsymmetry`) to right after `const auto selection = findActiveTableRewrite(...)`, then insert the reserved-column guard before `applyRewriteOptions`:

```cpp
  bool touches_si = false;
  for (const auto &t : response->original_accessed_tables()) touches_si |= t.is_storage_integrity();
  if (touches_si && selection.mode == TableRewriteMode::Dynamic) {
    const std::string rid = reservedRowIdColumn(*selection.dynamic_args);
    if (astReferencesIdentifier(ast, rid)) {
      std::cerr << "[select-reject] reserved column " << rid << " referenced" << std::endl;
      response->set_message("reserved column " + rid + " is not addressable");
      response->set_code(rewriter::RewriteCode::RewriteError);
      return; // statement_type stays UNSPECIFIED, matching every other reject
    }
  }
```

(`entry->set_is_storage_integrity(resolution.is_storage_integrity);` goes into the moved loop.) `rewriteEmbeddedViewBody` gets the same guard, returning after setting the code (its caller in writes.cc already returns Rejected when `response->code() != Success` after the body rewrite — verify; if not, add `if (response->code() != rewriter::RewriteCode::Success) return WriteDispatchResult::Rejected;` after the `rewriteEmbeddedViewBody` call).

- [ ] **Step 4: Build + run the golden suite**

Run: rsync + rebuild, then `$RB "cd $WD && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.*:AccessedTable.*:TableOnly.*:CTE*' 2>&1 | tail -40"`
Expected: SELECT-family cases OK (`si_safe_plain_select`, `si_safe_alias_join_non_si`, `si_unsafe_latest_*`, `si_safe_subquery_in_where`, `si_reserved_column_*`, `si_star_hides_rid`, `si_use_default_database`, `non_si_*`, `si_absent_*`); write/EXISTS/DESCRIBE cases still FAILED; all pre-existing suites OK.

- [ ] **Step 5: Commit**

```bash
git add src/handlers/storage_integrity.h src/handlers/storage_integrity.cc src/handlers/name_rewrite.h src/handlers/name_rewrite.cc src/handlers/select.cc CMakeLists.txt
git commit -m "feat(select): storage-integrity derived-table surface, reserved-column reject, is_storage_integrity"
```

### Task 12: C++ writes / EXISTS / SHOW CREATE / GRANT — SI rejection and safe-table mapping

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-grpc`

**Files:**
- Modify: `src/handlers/writes.cc` (`rewriteOneTarget` :104-160), `src/handlers/exists.cc` (:44-80), `src/handlers/show_create.cc` (mirror of exists), `src/handlers/grant.cc` (:150-205 element loop)

**Interfaces:**
- Consumes: Task 11 helpers.
- Produces: identical semantics to rewriter-go Task 7.

- [ ] **Step 1: `writes.cc` `rewriteOneTarget`** — after `recordAccessedTable(...)` and before `switch (sel.mode)`:

```cpp
  if (sel.mode == TableRewriteMode::Dynamic && stmt_kind != "INSERT") {
    if (auto hit = lookupStorageIntegrity(origin_db, origin_table, *sel.dynamic_args)) {
      rejectUnsupported(response, storageIntegrityWriteRejectMessage(hit->logical_key));
      return false;
    }
  }
```
`stmt_kind` is the existing `std::string_view` parameter; every caller passes `"INSERT"` for the insert slot (writes.cc:515) and other literals elsewhere, so this exemption is exact. RENAME/EXCHANGE sides and CREATE TABLE AS source flow through `rewriteOneTarget` too, so they are covered.

- [ ] **Step 2: `exists.cc`** — inside `case TableRewriteMode::Dynamic:` before `applyDynamicRewrite`:

```cpp
    if (auto hit = lookupStorageIntegrity(origin_db, origin_table, *sel.dynamic_args)) {
      const auto dot = hit->table->safe_table().find('.');
      const std::string safe_db = hit->table->safe_table().substr(0, dot);
      const std::string safe_tbl = hit->table->safe_table().substr(dot + 1);
      q->setDatabase(safe_db);
      q->setTable(safe_tbl);
      recordTableRewrite(response, origin_db, origin_table, safe_db, safe_tbl);
      break; // falls to the common setSuccessResponse(formatAst(ast), EXISTS_TABLE)
    }
```

`show_create.cc` — same spot, but reject: `rejectShowCreate(response, rewriter::RewriteCode::UnsupportedStatement, "SHOW CREATE TABLE on storage-integrity table " + hit->logical_key + " is not supported"); return ShowCreateDispatchResult::Handled;` (use whatever the file's local reject helper is named — mirror `rejectExists`).

- [ ] **Step 3: `grant.cc`** — inside the element loop, right after the column-level reject and before the `original_database` computation:

```cpp
    if (!elem.anyTable()) {
      const std::string odb = elem.default_database ? std::string() : elem.database;
      if (auto hit = lookupStorageIntegrity(odb, elem.table, *dynamic)) {
        rejectUnsupported(response, storageIntegrityWriteRejectMessage(hit->logical_key));
        return GrantDispatchResult::Rejected;
      }
    }
```

- [ ] **Step 4: Build + run**

Run: rsync + rebuild, then `$RB "cd $WD && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.*' 2>&1 | grep -E 'OK|FAILED' | tail -30"`
Expected: only `si_describe_metadata_select` FAILED (statement_type UNSPECIFIED); everything else OK.

- [ ] **Step 5: Commit**

```bash
git add src/handlers/writes.cc src/handlers/exists.cc src/handlers/show_create.cc src/handlers/grant.cc
git commit -m "feat(writes): refuse non-lane writes on storage-integrity tables; EXISTS → hg_safe; SHOW CREATE/GRANT reject"
```

### Task 13: C++ DESCRIBE handler (D-7) + dispatch

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-grpc`

**Files:**
- Create: `src/handlers/describe.h`, `src/handlers/describe.cc`
- Modify: `src/rewriter-server.cc` (dispatch :362-387), `CMakeLists.txt` (`rewriter_core` list)

**Interfaces:**
- Produces: `enum class DescribeDispatchResult { NotDescribe, Handled };` `DescribeDispatchResult handleDescribeQuery(DB::ASTPtr ast, const rewriter::RewriteSQLRequest *request, rewriter::RewriteSQLResponse *response);`

> If Spec E already added `handlers/describe.cc`, only add the SI branch (`lookupStorageIntegrity` → `describeMetadataSQL`) before its EXISTS-style resolution.

- [ ] **Step 1: Implement**

```cpp
// src/handlers/describe.cc
#include "handlers/describe.h"

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

#include "handlers/common.h"
#include "handlers/name_rewrite.h"
#include "handlers/storage_integrity.h"

namespace rewriter_handlers {

DescribeDispatchResult handleDescribeQuery(DB::ASTPtr ast,
                                           const rewriter::RewriteSQLRequest *request,
                                           rewriter::RewriteSQLResponse *response) {
  auto *q = ast->as<DB::ASTDescribeQuery>();
  if (!q) return DescribeDispatchResult::NotDescribe;

  std::string origin_db, origin_table;
  if (q->table_expression) {
    if (auto *te = q->table_expression->as<DB::ASTTableExpression>(); te && te->database_and_table_name) {
      if (auto *id = te->database_and_table_name->as<DB::ASTTableIdentifier>()) {
        const std::string full = id->name();
        if (const auto dot = full.find('.'); dot != std::string::npos) {
          origin_db = full.substr(0, dot);
          origin_table = full.substr(dot + 1);
        } else {
          origin_table = full;
        }
      }
    }
  }
  const auto sel = findActiveTableRewrite(request->options());
  recordAccessedTable(response, origin_db, origin_table, sel);
  if (sel.mode == TableRewriteMode::Dynamic) {
    if (auto hit = lookupStorageIntegrity(origin_db, origin_table, *sel.dynamic_args)) {
      setSuccessResponse(response,
        describeMetadataSQL(hit->table->safe_table(), reservedRowIdColumn(*sel.dynamic_args)),
        rewriter::STATEMENT_TYPE_DESCRIBE);
      return DescribeDispatchResult::Handled;
    }
  }
  // G-minimal: non-SI DESCRIBE passes through unchanged (Spec E D6 adds
  // EXISTS-style physical resolution here).
  setSuccessResponse(response, request->sql(), rewriter::STATEMENT_TYPE_DESCRIBE);
  return DescribeDispatchResult::Handled;
}

} // namespace rewriter_handlers
```

Dispatch in `rewriter-server.cc` — after the `handleShowTablesQuery` line and before `handleExistsQuery`:

```cpp
    if (rewriter_handlers::handleDescribeQuery(ast, request, response)
        == rewriter_handlers::DescribeDispatchResult::Handled) {
      return;
    }
```

Add `#include "handlers/describe.h"` and `src/handlers/describe.cc` to `rewriter_core`.

- [ ] **Step 2: Build + run the whole test binary**

Run: rsync + rebuild, then `$RB "cd $WD && ctest --test-dir build --output-on-failure 2>&1 | tail -15"`
Expected: `100% tests passed, 0 tests failed` (the gtest binary is one ctest entry; the `SpecG/StorageIntegrityGolden.*` cases are all OK, incl. `si_describe_metadata_select` with byte-exact SQL).

- [ ] **Step 3: Commit**

```bash
git add src/handlers/describe.h src/handlers/describe.cc src/rewriter-server.cc CMakeLists.txt
git commit -m "feat: DESCRIBE handler (STATEMENT_TYPE_DESCRIBE) with storage-integrity metadata SELECT"
```

### Task 14: rewriter-grpc docs + oracle parity run + release

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-grpc` (and rewriter-go for the oracle run)

**Files:**
- Modify: `CLAUDE.md` (Request flow list — add the DESCRIBE bullet and the SI paragraph under "Dynamic construction"), `AGENTS.md` (mirror), `README.md` (config example mentioning `storage_integrity`)

- [ ] **Step 1: Docs** — add to CLAUDE.md "Request flow" step 3 a bullet: "`ASTDescribeQuery` → [handlers/describe.cc]: `STATEMENT_TYPE_DESCRIBE`; storage-integrity target → `system.columns` metadata SELECT hiding `_hg_row_id`; otherwise passthrough (until Spec E D6)." Under "Dynamic construction" add a paragraph: "**Storage-integrity surface (`dynamic_args.storage_integrity`, [handlers/storage_integrity.cc]):** consulted by every table-targeting handler BEFORE `applyDynamicRewrite`. SELECT-side hits become `(SELECT * EXCEPT (_hg_row_id) FROM hg_safe.<t> [UNION ALL … hg_unsafe.<t> WHERE _part NOT IN (…)]) AS <alias>`; EXISTS maps to the safe table; DESCRIBE → metadata SELECT; every other statement kind (incl. GRANT/REVOKE, SHOW CREATE) rejects `UnsupportedStatement`; INSERT deliberately stays on the ordinary path (the caller's signed ingress decides). Any `_hg_row_id` identifier in an SI-touching statement → `RewriteError`. Golden corpus: `tests/testdata/storage_integrity_cases.json`, byte-identical to rewriter-go's; change both."

- [ ] **Step 2: Oracle parity from rewriter-go against this build**

On the box start the server (`$RB "cd $WD && ./build/clickhousegate_rewriter --port 50051 &"`), then locally, with an SSH tunnel `ssh -p 30100 -L 50051:localhost:50051 sentio@64.38.131.242 -N &`, in `/Users/uranuswch/Dev/housegate/rewriter-go`:
Run: `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib REWRITER_ORACLE_ADDR=localhost:50051 go test ./internal/harness -run TestStorageIntegrityGolden -count=1`
Expected: `ok` — structured fields identical on both engines; `sql_after_rewrite` exempted only where the corpus says `allow_sql_divergence` (`EXCEPT` paren rendering). Record the run in the PR description.

- [ ] **Step 3: Commit, PR, release**

```bash
git add CLAUDE.md AGENTS.md README.md
git commit -m "docs: storage-integrity read surface (Spec G) in CLAUDE.md/AGENTS.md"
git push -u origin feat/storage-integrity-read-surface
```
Merge the PR, then `Actions → cut-release → Run workflow` (tags `main` `vX.Y.Z` in UTC and dispatches `release.yml`, which builds + pushes the docker image). Note the tag for the housegate CLAUDE.md "Code base" line.

---

## Part D — housegate (config, port, dynamic args, plugin, tests)

### Task 15: Bump rewriter-proto/rewriter-go in housegate; `sqlmeta` gains `IsStorageIntegrity` + `StatementTypeDescribe`

**Working directory:** `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `go.mod`, `go.sum`, `pkg/sqlmeta/accessed_table.go:16-22`, `pkg/sqlmeta/statement_type.go:20-42` (+`String()`), `pkg/rewriter/sentio.go:43-58` (`accessedTablesFromProto`), `pkg/plugins/storageintegrity/plugin.go:608-614` (read-only case list), `configs/local.server.yaml:48`, `CLAUDE.md:141` (ffifetch floor sentence)
- Test: `pkg/rewriter/backend_test.go`, `pkg/plugins/storageintegrity/plugin_test.go`

**Interfaces:**
- Produces: `sqlmeta.AccessedTable.IsStorageIntegrity bool`; `sqlmeta.StatementTypeDescribe StatementType = 22` (String `"DESCRIBE"`).

- [ ] **Step 1: Bump (upgrade-dependency recipe: plain `require`, no replace edits needed here)**

```bash
git checkout -b feat/storage-integrity-read-surface
go get github.com/housegate/rewriter-proto@v0.2.0 github.com/housegate/rewriter-go@v0.7.0 && go mod tidy
bazel mod tidy && bazel run //:gazelle
go run ./cmd fetch-rewriter-lib --tag v0.7.0
```
Expected: `go.mod` shows both new versions; gazelle may reorder unrelated `load()`s (keep); the last command prints the cached lib path (e.g. `~/Library/Caches/housegate/rewriter-ffi/v0.7.0/libpolyglot_sql_ffi.dylib`) — export it as `$FFI` for the rest of this part.
Chase the version string: `sed -i '' 's/native_library_release: v0.6.0/native_library_release: v0.7.0/' configs/local.server.yaml`; in `CLAUDE.md` change "requires an FFI library built from rewriter-go >= v0.6.0" to `>= v0.7.0`.

- [ ] **Step 2: Failing tests**

Append to `pkg/rewriter/backend_test.go`:

```go
func TestSentioRewriter_AccessedTablesCarryStorageIntegrityFlag(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT,
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "db1", OriginalTable: "t", IsStorageIntegrity: true}},
	}}
	res, err := newFakeFactory(be).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT a FROM db1.t", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.AccessedTables) != 1 || !res.AccessedTables[0].IsStorageIntegrity {
		t.Fatalf("AccessedTables = %+v, want IsStorageIntegrity=true", res.AccessedTables)
	}
}
```

Append to `pkg/plugins/storageintegrity/plugin_test.go`:

```go
func TestIngressIgnoresDescribe(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "DESCRIBE TABLE tenant.events"
	qctx := signedQueryContext(t, 61, signer, sql, sql, sqlmeta.StatementTypeDescribe)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("DESCRIBE must be a read-only pass-through for the ingress: %v", err)
	}
	if qctx.SuppressUpstreamExecution {
		t.Fatal("DESCRIBE must not be intercepted")
	}
}
```

Run: `go test ./pkg/rewriter ./pkg/plugins/storageintegrity ./pkg/sqlmeta -run 'TestSentioRewriter_AccessedTablesCarryStorageIntegrityFlag|TestIngressIgnoresDescribe' -count=1`
Expected: compile errors `unknown field IsStorageIntegrity` / `undefined: sqlmeta.StatementTypeDescribe`.

- [ ] **Step 3: Implement**

`pkg/sqlmeta/accessed_table.go`: add `IsStorageIntegrity bool` after `IsRemote` with the doc comment "true iff the rewriter resolved this access to a storage-integrity table (Spec G); auth/usage keep using the logical names above." `pkg/sqlmeta/statement_type.go`: add `StatementTypeDescribe StatementType = 22` after `StatementTypeDropView` and `case StatementTypeDescribe: return "DESCRIBE"` in `String()`. `pkg/rewriter/sentio.go` `accessedTablesFromProto`: `IsStorageIntegrity: t.GetIsStorageIntegrity(),`. `pkg/plugins/storageintegrity/plugin.go` `classifyStorageIntegrityKind`: add `sqlmeta.StatementTypeDescribe` to the read-only `case` list next to `StatementTypeShowDatabases`.

- [ ] **Step 4: Verification ladder**

Run: `go build ./... && go vet ./... 2>&1 | grep -v "unkeyed fields"; bazel build //... && bazel test //pkg/rewriter:rewriter_test //pkg/plugins/storageintegrity:storageintegrity_test //pkg/sqlmeta:sqlmeta_test`
Expected: builds clean; the three targets PASS.
Run: `bazel test //pkg/rewriter:rewriter_test --test_env=POLYGLOT_SQL_FFI_PATH=$FFI --test_output=all --test_arg=-test.v --nocache_test_results 2>&1 | grep -E '^--- (PASS|FAIL): TestNativeEngineSmoke'`
Expected: `--- PASS: TestNativeEngineSmoke` (the new lib actually ran).

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum MODULE.bazel pkg/sqlmeta pkg/rewriter/sentio.go pkg/rewriter/backend_test.go pkg/plugins/storageintegrity configs/local.server.yaml CLAUDE.md
git commit -m "chore(deps): rewriter-proto v0.2.0 + rewriter-go v0.7.0; sqlmeta IsStorageIntegrity + StatementTypeDescribe"
```

### Task 16: Config — `storage_integrity.tables[]` (renamed) + `storage_integrity.read.default_mode`; merge guard derives physical names

**Working directory:** `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/config/storage_integrity_config.go`, `storage_integrity_runtime.go:42-60,158-190`, `build.go:~604` (`buildStorageIntegrityRuntimeConsumer` call), `README.md` (SI config sample), `configs/local.server.yaml` (commented sample block)
- Test: `pkg/config/storage_integrity_config_test.go`, `build_test.go:1112-1127,688-731`

**Interfaces:**
- Produces: `config.StorageIntegrityConfig.Tables []string` (yaml `tables`), `config.StorageIntegrityReadConfig{DefaultMode string}` (yaml `read.default_mode`), constants `config.StorageIntegrityUnsafeDatabase = "hg_unsafe"`, `config.StorageIntegritySafeDatabase = "hg_safe"`, `config.StorageIntegrityReadModeSafe = "safe"`, `config.StorageIntegrityReadModeUnsafeLatest = "unsafe_latest"`; `func config.StorageIntegrityPhysicalTable(tableID string) string`; `func config.SplitStorageIntegrityTableID(id string) (db, table string, ok bool)`; `StorageIntegrityRuntimeMergeGuardConfig` loses `Tables` (keeps `ReassertInterval`, gains hidden `LegacyTables` for the rename error); `buildStorageIntegrityRuntimeConsumer(runtimeCfg config.StorageIntegrityRuntimeConfig, tables []string, opts StorageIntegrityRuntimeOptions)`; `storageIntegrityMergeTables(tableIDs []string) ([]sicore.MergeTable, error)` → for each id, `{hg_safe, phys}` then `{hg_unsafe, phys}`.

- [ ] **Step 1: Failing config tests** — replace the two merge-guard subtests in `pkg/config/storage_integrity_config_test.go` (lines ~117-160) and the fixture (~195-216) with:

```go
	t.Run("runtime enabled requires storage_integrity.tables", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Tables = nil
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.tables is required when storage_integrity.runtime.enabled") {
			t.Fatalf("Validate err = %v", err)
		}
	})

	t.Run("legacy merge_guard.tables key is a pointed error", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Runtime.MergeGuard.LegacyTables = []StorageIntegrityRuntimeMergeTableConfig{{Database: "hg_unsafe", Table: "events"}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.runtime.merge_guard.tables was renamed to storage_integrity.tables") {
			t.Fatalf("Validate err = %v", err)
		}
	})

	t.Run("tables entries must be logical db.table ids, unique, server-mode only", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Tables = []string{"tenant.events", "noDot", "a.b.c", "tenant.events"}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate succeeded with malformed table ids")
		}
		for _, want := range []string{
			`storage_integrity.tables[1] "noDot" must be a logical <database>.<table> id`,
			`storage_integrity.tables[2] "a.b.c" must be a logical <database>.<table> id`,
			`storage_integrity.tables[3] duplicates "tenant.events"`,
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Validate err = %v, missing %q", err, want)
			}
		}
	})

	t.Run("read.default_mode validation", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Tables = []string{"tenant.events"}
		for _, ok := range []string{"", "safe", "unsafe_latest"} {
			cfg.StorageIntegrity.Read.DefaultMode = ok
			if err := cfg.Validate(); err != nil {
				t.Fatalf("default_mode %q: %v", ok, err)
			}
		}
		cfg.StorageIntegrity.Read.DefaultMode = "latest"
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), `storage_integrity.read.default_mode "latest" must be safe or unsafe_latest`) {
			t.Fatalf("Validate err = %v", err)
		}
		cfg.StorageIntegrity.Read.DefaultMode = "safe"
		cfg.StorageIntegrity.Tables = nil
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "storage_integrity.read.default_mode requires storage_integrity.tables") {
			t.Fatalf("Validate err = %v", err)
		}
	})
```

and in `storageIntegrityRuntimeConfigFixture` replace the `MergeGuard.Tables = …` assignment with `cfg.StorageIntegrity.Tables = []string{"tenant.events"}`. Add a pure helper test:

```go
func TestStorageIntegrityPhysicalNaming(t *testing.T) {
	if got := StorageIntegrityPhysicalTable("tenant.events"); got != "tenant__events" {
		t.Fatalf("physical = %q", got)
	}
	db, table, ok := SplitStorageIntegrityTableID("tenant.events")
	if !ok || db != "tenant" || table != "events" {
		t.Fatalf("split = %q %q %v", db, table, ok)
	}
	for _, bad := range []string{"", "events", ".events", "tenant.", "a.b.c"} {
		if _, _, ok := SplitStorageIntegrityTableID(bad); ok {
			t.Fatalf("%q must be rejected", bad)
		}
	}
}
```

Update `build_test.go`: `enableStorageIntegrityRuntimeTestConfig` sets `cfg.StorageIntegrity.Tables = []string{"tenant.events"}` instead of `MergeGuard.Tables`; in `TestBuildStorageIntegrityRuntimeBuildsMergeGuardFromConnAndConfig` pass `cfg.StorageIntegrity.Tables` as the new second argument and change `wantExecs` to `"SYSTEM STOP MERGES `hg_safe`.`tenant__events`"`, `"SYSTEM STOP MERGES `hg_unsafe`.`tenant__events`"`; update every other `buildStorageIntegrityRuntimeConsumer(cfg.StorageIntegrity.Runtime, …)` call in `build_test.go` to `buildStorageIntegrityRuntimeConsumer(cfg.StorageIntegrity.Runtime, cfg.StorageIntegrity.Tables, …)`.

Run: `go test ./pkg/config -run 'TestConfigValidateStorageIntegrityIngress|TestStorageIntegrityPhysicalNaming' -count=1 && go vet . 2>&1 | grep -v "unkeyed fields" | head`
Expected: compile errors (`LegacyTables`, `Tables`, `Read`, `StorageIntegrityPhysicalTable` undefined).

- [ ] **Step 2: Implement `pkg/config/storage_integrity_config.go`**

```go
const (
	defaultStorageIntegrityMaxPayloadBytes uint64 = 64 << 20

	// Physical homes of storage-integrity tables (Spec C D2 naming freeze).
	StorageIntegrityUnsafeDatabase = "hg_unsafe"
	StorageIntegritySafeDatabase   = "hg_safe"

	StorageIntegrityReadModeSafe         = "safe"
	StorageIntegrityReadModeUnsafeLatest = "unsafe_latest"
)

// StorageIntegrityPhysicalTable maps a logical table id "<db>.<table>" to
// the physical table name used under hg_unsafe / hg_safe / hg_promote —
// the same rule as arbiter-core's snode.CHTableName (Spec C D2). Kept
// here (one line) so housegate does not import arbiter-core.
func StorageIntegrityPhysicalTable(tableID string) string {
	return strings.ReplaceAll(tableID, ".", "__")
}

// SplitStorageIntegrityTableID validates and splits a logical table id:
// exactly one dot, non-empty database and table.
func SplitStorageIntegrityTableID(id string) (string, string, bool) {
	parts := strings.Split(id, ".")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// StorageIntegrityConfig owns HouseGate-local storage-integrity toggles.
type StorageIntegrityConfig struct {
	// Tables is the explicit SI membership (Spec G D4): logical
	// "<database>.<table>" ids. Shared by the merge guard (which guards
	// hg_safe.<phys> and hg_unsafe.<phys>), the ingress, and the read
	// rewrite. Renamed from runtime.merge_guard.tables.
	Tables     []string                         `json:"tables"      yaml:"tables"`
	Read       StorageIntegrityReadConfig       `json:"read"        yaml:"read"`
	Ingress    StorageIntegrityIngressConfig    `json:"ingress"     yaml:"ingress"`
	Runtime    StorageIntegrityRuntimeConfig    `json:"runtime"     yaml:"runtime"`
	SafeMerges StorageIntegritySafeMergesConfig `json:"safe_merges" yaml:"safe_merges"`
}

// StorageIntegrityReadConfig is the read-surface policy (Spec G D1).
type StorageIntegrityReadConfig struct {
	// DefaultMode is "safe" (default when empty) or "unsafe_latest"; a
	// query may override it with the SQL_x_read_mode setting.
	DefaultMode string `json:"default_mode" yaml:"default_mode"`
}

// StorageIntegrityRuntimeMergeGuardConfig tunes the startup SYSTEM STOP
// MERGES guard; the guarded table set is StorageIntegrityConfig.Tables.
type StorageIntegrityRuntimeMergeGuardConfig struct {
	ReassertInterval Duration `json:"reassert_interval" yaml:"reassert_interval"`
	// LegacyTables only exists to catch the pre-Spec-G key and turn it into
	// a pointed rename error instead of a silently unguarded deployment.
	LegacyTables []StorageIntegrityRuntimeMergeTableConfig `json:"tables" yaml:"tables"`
}
```

Replace `validate`:

```go
func (c StorageIntegrityConfig) validate(mode Mode) error {
	var errs []error
	if len(c.Runtime.MergeGuard.LegacyTables) > 0 {
		errs = append(errs, errors.New("storage_integrity.runtime.merge_guard.tables was renamed to storage_integrity.tables (list of logical <database>.<table> ids; hg_unsafe/hg_safe physical names are derived)"))
	}
	if len(c.Tables) > 0 && mode != ModeServer {
		errs = append(errs, errors.New("storage_integrity.tables is server mode only"))
	}
	seen := make(map[string]bool, len(c.Tables))
	for i, id := range c.Tables {
		if _, _, ok := SplitStorageIntegrityTableID(id); !ok {
			errs = append(errs, fmt.Errorf("storage_integrity.tables[%d] %q must be a logical <database>.<table> id", i, id))
		}
		if seen[id] {
			errs = append(errs, fmt.Errorf("storage_integrity.tables[%d] duplicates %q", i, id))
		}
		seen[id] = true
	}
	switch c.Read.DefaultMode {
	case "", StorageIntegrityReadModeSafe, StorageIntegrityReadModeUnsafeLatest:
	default:
		errs = append(errs, fmt.Errorf("storage_integrity.read.default_mode %q must be safe or unsafe_latest", c.Read.DefaultMode))
	}
	if c.Read.DefaultMode != "" && len(c.Tables) == 0 {
		errs = append(errs, errors.New("storage_integrity.read.default_mode requires storage_integrity.tables"))
	}
	if !c.Ingress.Enabled {
		if c.Runtime.Enabled {
			errs = append(errs, errors.New("storage_integrity.runtime.enabled requires storage_integrity.ingress.enabled"))
		}
		return joinStorageIntegrityErrs(errs)
	}
	// … the existing ingress/runtime checks unchanged, EXCEPT replace the
	// `len(c.Runtime.MergeGuard.Tables) == 0` block and the per-entry
	// database/table loop with:
	if c.Runtime.Enabled && len(c.Tables) == 0 {
		errs = append(errs, errors.New("storage_integrity.tables is required when storage_integrity.runtime.enabled"))
	}
	return joinStorageIntegrityErrs(errs)
}

func joinStorageIntegrityErrs(errs []error) error {
	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("storage_integrity: %w", joined)
	}
	return nil
}
```

`storage_integrity_runtime.go`:

```go
func buildStorageIntegrityRuntimeConsumer(runtimeCfg config.StorageIntegrityRuntimeConfig, tables []string, opts StorageIntegrityRuntimeOptions) (*StorageIntegrityIngress, StorageIntegrityMergeGuard, error) {
	// … unchanged until the merge guard:
	rawMergeGuard, err := buildStorageIntegrityMergeGuard(tables, opts)
	// …
}

func buildStorageIntegrityMergeGuard(tables []string, opts StorageIntegrityRuntimeOptions) (StorageIntegrityMergeGuard, error) {
	if opts.MergeGuard != nil {
		return opts.MergeGuard, nil
	}
	if opts.MergeConn == nil {
		return nil, nil
	}
	mergeTables, err := storageIntegrityMergeTables(tables)
	if err != nil {
		return nil, err
	}
	return sicore.NewMergeGuard(opts.MergeConn, mergeTables), nil
}

// storageIntegrityMergeTables derives the guarded physical set from the
// logical ids: hg_safe.<phys> then hg_unsafe.<phys> per id.
func storageIntegrityMergeTables(tableIDs []string) ([]sicore.MergeTable, error) {
	if len(tableIDs) == 0 {
		return nil, errors.New("storage_integrity.tables is required when using merge_conn")
	}
	out := make([]sicore.MergeTable, 0, 2*len(tableIDs))
	for i, id := range tableIDs {
		if _, _, ok := config.SplitStorageIntegrityTableID(id); !ok {
			return nil, fmt.Errorf("storage_integrity.tables[%d] %q must be a logical <database>.<table> id", i, id)
		}
		phys := config.StorageIntegrityPhysicalTable(id)
		out = append(out,
			sicore.MergeTable{Database: config.StorageIntegritySafeDatabase, Table: phys},
			sicore.MergeTable{Database: config.StorageIntegrityUnsafeDatabase, Table: phys},
		)
	}
	return out, nil
}
```

`build.go` (~:604): `consumer, guard, err := buildStorageIntegrityRuntimeConsumer(cfg.StorageIntegrity.Runtime, cfg.StorageIntegrity.Tables, opts.StorageIntegrityRuntime)`.

- [ ] **Step 3: Run**

Run: `bazel test //pkg/config:config_test //:housegate_test --test_output=errors`
Expected: PASS (all subtests, incl. the updated merge-guard exec expectations).

- [ ] **Step 4: Docs sample** — in `README.md`'s storage-integrity example and `configs/local.server.yaml`, replace the `merge_guard: tables:` list with:

```yaml
storage_integrity:
  tables: ["tenant.events"]        # logical <db>.<table> ids; hg_unsafe/hg_safe.tenant__events are derived
  read:
    default_mode: safe             # safe | unsafe_latest; per query: SETTINGS SQL_x_read_mode = 'unsafe_latest'
  runtime:
    merge_guard:
      reassert_interval: 30s
```

- [ ] **Step 5: Commit**

```bash
git add pkg/config storage_integrity_runtime.go build.go build_test.go README.md configs/local.server.yaml
git commit -m "feat(config): storage_integrity.tables (renamed from runtime.merge_guard.tables) + read.default_mode"
```

### Task 17: `pkg/rewriter` — read mode, port, `RejectedError`, `StorageIntegrityArgs` in dynamic args, fail-closed SI handling

**Working directory:** `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Create: `pkg/rewriter/storage_integrity.go`, `pkg/rewriter/storage_integrity_test.go`
- Modify: `pkg/rewriter/types.go` (`Options`), `pkg/rewriter/args.go` (`buildDynamicArgs`), `pkg/rewriter/sentio.go` (`Rewrite` :266-330, `RewriteErrorMessage` :402-407), `pkg/rewriter/BUILD.bazel` (gazelle), `pkg/rewriter/AGENTS.md`
- Test: `pkg/rewriter/backend_test.go`

**Interfaces:**
- Produces:
  - `const rewriter.ReadModeSettingKey = "SQL_x_read_mode"`
  - `type rewriter.ReadMode string`; `rewriter.ReadModeSafe = "safe"`, `rewriter.ReadModeUnsafeLatest = "unsafe_latest"`; `func rewriter.ParseReadMode(raw string) (ReadMode, error)` (trims spaces and `'"`; strict — `""` is an error)
  - `type rewriter.StorageIntegrityReadState interface { PromotedUnsafeParts(tableID string) ([]string, error) }`
  - `type rewriter.StorageIntegrityTable struct { TableID, SafeTable, UnsafeTable string }` (`TableID` doubles as the logical key)
  - `type rewriter.StorageIntegrityOptions struct { Tables []StorageIntegrityTable; DefaultReadMode ReadMode; ReadState StorageIntegrityReadState; InsertLaneEnabled bool }` on `rewriter.Options.StorageIntegrity`
  - `func rewriter.WithReadMode(ctx context.Context, m ReadMode) context.Context`; `func rewriter.ReadModeFromContext(ctx context.Context) (ReadMode, bool)`
  - `type rewriter.RejectedError struct { Code pb.RewriteCode; Message string }` with `Error()`; returned by `Rewrite` for every SI-related rejection (D-2)
  - `func buildStorageIntegrityArgs(opts StorageIntegrityOptions, mode ReadMode) (*pb.StorageIntegrityArgs, error)`; `buildDynamicArgs(..., si *pb.StorageIntegrityArgs)`

- [ ] **Step 1: Failing tests** — `pkg/rewriter/storage_integrity_test.go`:

```go
package rewriter

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"
)

type fakeReadState struct {
	parts map[string][]string
	err   error
	calls []string
}

func (f *fakeReadState) PromotedUnsafeParts(tableID string) ([]string, error) {
	f.calls = append(f.calls, tableID)
	if f.err != nil {
		return nil, f.err
	}
	return f.parts[tableID], nil
}

func siOpts(rs StorageIntegrityReadState) StorageIntegrityOptions {
	return StorageIntegrityOptions{
		Tables:          []StorageIntegrityTable{{TableID: "db1.t", SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"}},
		DefaultReadMode: ReadModeSafe,
		ReadState:       rs,
	}
}

func TestParseReadMode(t *testing.T) {
	for raw, want := range map[string]ReadMode{"safe": ReadModeSafe, "'unsafe_latest'": ReadModeUnsafeLatest, `" safe "`: ReadModeSafe} {
		got, err := ParseReadMode(raw)
		if err != nil || got != want {
			t.Fatalf("ParseReadMode(%q) = %q, %v", raw, got, err)
		}
	}
	for _, bad := range []string{"", "latest", "SAFE "} {
		if _, err := ParseReadMode(bad); err == nil {
			t.Fatalf("ParseReadMode(%q) must fail", bad)
		}
	}
}

func TestBuildStorageIntegrityArgs(t *testing.T) {
	if got, err := buildStorageIntegrityArgs(StorageIntegrityOptions{}, ReadModeSafe); got != nil || err != nil {
		t.Fatalf("no tables → nil args, got %v %v", got, err)
	}
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0", "all_2_2_0"}}}
	got, err := buildStorageIntegrityArgs(siOpts(rs), ReadModeSafe)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE || got.GetReservedRowIdColumn() != "_hg_row_id" {
		t.Fatalf("args = %v", got)
	}
	tbl := got.GetTables()["db1.t"]
	if tbl.GetSafeTable() != "hg_safe.db1__t" || tbl.GetUnsafeTable() != "hg_unsafe.db1__t" || len(tbl.GetExcludedUnsafeParts()) != 0 {
		t.Fatalf("safe mode table = %v (must not consult the port)", tbl)
	}
	if len(rs.calls) != 0 {
		t.Fatalf("safe mode must not call the port: %v", rs.calls)
	}
	got, err = buildStorageIntegrityArgs(siOpts(rs), ReadModeUnsafeLatest)
	if err != nil {
		t.Fatal(err)
	}
	if parts := got.GetTables()["db1.t"].GetExcludedUnsafeParts(); len(parts) != 2 || parts[0] != "all_1_1_0" {
		t.Fatalf("excluded = %v", parts)
	}
	if got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST {
		t.Fatalf("mode = %v", got.GetReadMode())
	}
	if got, err := buildStorageIntegrityArgs(siOpts(rs), ""); err != nil || got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE {
		t.Fatalf("empty mode must mean safe: %v %v", got, err)
	}
}

func TestBuildStorageIntegrityArgs_unsafeLatestWithoutPortIsRejected(t *testing.T) {
	_, err := buildStorageIntegrityArgs(siOpts(nil), ReadModeUnsafeLatest)
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "unsafe_latest") {
		t.Fatalf("err = %v, want RejectedError about unsafe_latest", err)
	}
	_, err = buildStorageIntegrityArgs(siOpts(&fakeReadState{err: errors.New("journal locked")}), ReadModeUnsafeLatest)
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "journal locked") {
		t.Fatalf("port error must surface as RejectedError: %v", err)
	}
}

func TestReadModeContext(t *testing.T) {
	if _, ok := ReadModeFromContext(context.Background()); ok {
		t.Fatal("empty ctx must report no mode")
	}
	ctx := WithReadMode(context.Background(), ReadModeUnsafeLatest)
	if m, ok := ReadModeFromContext(ctx); !ok || m != ReadModeUnsafeLatest {
		t.Fatalf("got %q %v", m, ok)
	}
}
```

Append to `pkg/rewriter/backend_test.go`:

```go
func newSIFactory(be backend, rs StorageIntegrityReadState, insertLane bool) *SentioNetworkFactory {
	f := newFakeFactory(be)
	f.options.StorageIntegrity = siOpts(rs)
	f.options.StorageIntegrity.InsertLaneEnabled = insertLane
	return f
}

func TestSentioRewriter_ShipsStorageIntegrityArgs(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "x", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT}}
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}}}
	rw := newSIFactory(be, rs, true).NewRewriter(&fakeSession{})
	if _, err := rw.Rewrite(context.Background(), "SELECT a FROM db1.t", ""); err != nil {
		t.Fatal(err)
	}
	si := be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
	if si.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE || si.GetTables()["db1.t"].GetSafeTable() != "hg_safe.db1__t" {
		t.Fatalf("default-mode args = %v", si)
	}
	ctx := WithReadMode(context.Background(), ReadModeUnsafeLatest)
	if _, err := rw.Rewrite(ctx, "SELECT a FROM db1.t", ""); err != nil {
		t.Fatal(err)
	}
	si = be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
	if si.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST || len(si.GetTables()["db1.t"].GetExcludedUnsafeParts()) != 1 {
		t.Fatalf("per-query unsafe_latest args = %v", si)
	}
}

func TestSentioRewriter_UnsafeLatestWithoutPortIsRejectedBeforeBackend(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success}}
	rw := newSIFactory(be, nil, true).NewRewriter(&fakeSession{})
	_, err := rw.Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("err = %v, want RejectedError", err)
	}
	if be.lastReq != nil {
		t.Fatal("backend must not be called when the mode is unavailable")
	}
}

func TestSentioRewriter_StorageIntegrityRejectIsFailClosed(t *testing.T) {
	for _, code := range []pb.RewriteCode{pb.RewriteCode_UnsupportedStatement, pb.RewriteCode_RewriteError} {
		be := &fakeBackend{resp: &pb.RewriteSQLResponse{
			Code: code, Message: "storage-integrity table db1.t accepts writes only through the signed statement lane",
			OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "db1", OriginalTable: "t", IsStorageIntegrity: true}},
		}}
		rw := newSIFactory(be, nil, true).NewRewriter(&fakeSession{})
		_, err := rw.Rewrite(context.Background(), "DROP TABLE db1.t", "")
		var rej *RejectedError
		if !errors.As(err, &rej) || rej.Code != code || !strings.Contains(rej.Message, "signed statement lane") {
			t.Fatalf("code %v: err = %v, want RejectedError carrying the rewriter message", code, err)
		}
	}
	// Non-SI Unsupported keeps today's pass-through contract.
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_UnsupportedStatement, Message: "nope",
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "other", OriginalTable: "u"}}}}
	res, err := newSIFactory(be, nil, true).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "OPTIMIZE TABLE other.u", "")
	if err != nil || res.SQL != "OPTIMIZE TABLE other.u" {
		t.Fatalf("non-SI unsupported must pass through: %v %v", res, err)
	}
}

func TestSentioRewriter_InsertIntoSITableWithoutLaneIsRejected(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code: pb.RewriteCode_Success, SqlAfterRewrite: `INSERT INTO phys."db1.t" (a) VALUES (1)`, StatementType: pb.StatementType_STATEMENT_TYPE_INSERT,
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "db1", OriginalTable: "t", LogicalDatabase: "db1", IsStorageIntegrity: true}},
	}}
	_, err := newSIFactory(be, nil, false).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "INSERT INTO db1.t (a) VALUES (1)", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Message != "storage-integrity table db1.t accepts writes only through the signed statement lane" {
		t.Fatalf("err = %v", err)
	}
	res, err := newSIFactory(be, nil, true).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "INSERT INTO db1.t (a) VALUES (1)", "")
	if err != nil || res.StatementType != sqlmeta.StatementTypeInsert {
		t.Fatalf("with the lane enabled the INSERT proceeds to the ingress: %v %v", res, err)
	}
}
```

Run: `go test ./pkg/rewriter -run 'TestParseReadMode|TestBuildStorageIntegrityArgs|TestReadModeContext|TestSentioRewriter_ShipsStorageIntegrityArgs|TestSentioRewriter_UnsafeLatest|TestSentioRewriter_StorageIntegrityReject|TestSentioRewriter_InsertIntoSI' -count=1`
Expected: compile errors (`undefined: ReadMode`, `RejectedError`, …).

- [ ] **Step 2: Implement `pkg/rewriter/storage_integrity.go`**

```go
package rewriter

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// ReadModeSettingKey is the per-query ClickHouse custom setting that selects
// the storage-integrity read mode (Spec G D1). Like the other SQL_x_* keys
// it is read by housegate and forwarded unchanged (ClickHouse accepts it
// under custom_settings_prefixes = SQL_).
const ReadModeSettingKey = "SQL_x_read_mode"

// ReadMode selects which physical surface an SI table read resolves to.
type ReadMode string

const (
	ReadModeSafe         ReadMode = "safe"          // hg_safe only
	ReadModeUnsafeLatest ReadMode = "unsafe_latest" // hg_safe ∪ hg_unsafe minus promoted-not-yet-cleaned parts
)

// DefaultReservedRowIDColumn is the protocol row-identity column hidden
// from the logical surface (Spec G D3).
const DefaultReservedRowIDColumn = "_hg_row_id"

// ParseReadMode parses a setting/config value. Surrounding whitespace and
// the '…' / "…" quoting clickhouse-go's CustomSetting adds are stripped.
// Empty is an error — callers decide their own default.
func ParseReadMode(raw string) (ReadMode, error) {
	v := strings.Trim(strings.TrimSpace(raw), "\"'")
	v = strings.TrimSpace(v)
	switch ReadMode(v) {
	case ReadModeSafe, ReadModeUnsafeLatest:
		return ReadMode(v), nil
	}
	return "", fmt.Errorf("%s: invalid value %q (want 'safe' or 'unsafe_latest')", ReadModeSettingKey, raw)
}

// StorageIntegrityReadState is the host port supplying the unsafe parts a
// promotion already copied into hg_safe but whose cleanup is not yet
// acknowledged (Spec G D2). sentio-node satisfies it with *snode.Role. Nil
// means "no co-located SNode": unsafe_latest is refused, never degraded.
type StorageIntegrityReadState interface {
	PromotedUnsafeParts(tableID string) ([]string, error)
}

// StorageIntegrityTable is one SI table as housegate knows it: the logical
// id (== the rewriter's lookup key) and its two physical homes.
type StorageIntegrityTable struct {
	TableID     string // logical "<db>.<table>"
	SafeTable   string // "hg_safe.<phys>"
	UnsafeTable string // "hg_unsafe.<phys>"
}

// StorageIntegrityOptions is the read-surface slice of rewriter.Options.
type StorageIntegrityOptions struct {
	Tables          []StorageIntegrityTable
	DefaultReadMode ReadMode                  // "" → safe
	ReadState       StorageIntegrityReadState // nil → unsafe_latest refused
	// InsertLaneEnabled is true when the SI ingress plugin is wired (it then
	// owns INSERT admission). When false, an INSERT whose accessed tables
	// include an SI table is rejected here (plan deviation D-1).
	InsertLaneEnabled bool
}

type readModeCtxKey struct{}

// WithReadMode attaches the per-query read mode (from SQL_x_read_mode).
func WithReadMode(ctx context.Context, m ReadMode) context.Context {
	return context.WithValue(ctx, readModeCtxKey{}, m)
}

// ReadModeFromContext returns the per-query read mode, if any.
func ReadModeFromContext(ctx context.Context) (ReadMode, bool) {
	m, ok := ctx.Value(readModeCtxKey{}).(ReadMode)
	return m, ok && m != ""
}

// RejectedError is a rewrite outcome that MUST reach the client as an
// Exception (the plugin fails closed on it) instead of falling open to
// the original SQL. Used for every storage-integrity rejection: reserved
// column, non-lane write, unavailable read mode.
type RejectedError struct {
	Code    pb.RewriteCode
	Message string
}

func (e *RejectedError) Error() string {
	return "rewriter rejected SQL (code=" + e.Code.String() + "): " + e.Message
}

// buildStorageIntegrityArgs renders the proto block for one call. Safe mode
// never consults the port; unsafe_latest requires it and surfaces every
// port error as a RejectedError (fail closed, D2).
func buildStorageIntegrityArgs(opts StorageIntegrityOptions, mode ReadMode) (*pb.StorageIntegrityArgs, error) {
	if len(opts.Tables) == 0 {
		return nil, nil
	}
	if mode == "" {
		mode = ReadModeSafe
	}
	out := &pb.StorageIntegrityArgs{
		Tables:              make(map[string]*pb.StorageIntegrityArgs_Table, len(opts.Tables)),
		ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
		ReservedRowIdColumn: DefaultReservedRowIDColumn,
	}
	if mode == ReadModeUnsafeLatest {
		if opts.ReadState == nil {
			return nil, &RejectedError{Code: pb.RewriteCode_RewriteError,
				Message: "storage_integrity read mode unsafe_latest is unavailable on this housegate: no promotion-state port (co-located SNode) is wired"}
		}
		out.ReadMode = pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST
	}
	for _, t := range opts.Tables {
		entry := &pb.StorageIntegrityArgs_Table{SafeTable: t.SafeTable, UnsafeTable: t.UnsafeTable}
		if mode == ReadModeUnsafeLatest {
			parts, err := opts.ReadState.PromotedUnsafeParts(t.TableID)
			if err != nil {
				return nil, &RejectedError{Code: pb.RewriteCode_RewriteError,
					Message: fmt.Sprintf("storage_integrity read mode unsafe_latest: cannot resolve promoted unsafe parts for %s: %v", t.TableID, err)}
			}
			entry.ExcludedUnsafeParts = parts
		}
		out.Tables[t.TableID] = entry
	}
	return out, nil
}

// storageIntegrityAccess returns the first SI-flagged accessed table's
// logical "db.table" key and whether one exists.
func storageIntegrityAccess(tables []*pb.AccessedTable) (string, bool) {
	for _, t := range tables {
		if t.GetIsStorageIntegrity() {
			db := t.GetLogicalDatabase()
			if db == "" {
				db = t.GetOriginalDatabase()
			}
			if db == "" {
				return t.GetOriginalTable(), true
			}
			return db + "." + t.GetOriginalTable(), true
		}
	}
	return "", false
}
```

`types.go` `Options`: add `StorageIntegrity StorageIntegrityOptions` with a doc comment pointing at Spec G. `args.go`: add trailing param `si *pb.StorageIntegrityArgs` and `StorageIntegrity: si` in the literal (update both callers in `sentio.go`).

`sentio.go` `Rewrite` — after building `dbMap/knownPhys/logicalToRemote/remoteUpstreams`:

```go
	mode := r.factory.options.StorageIntegrity.DefaultReadMode
	if m, ok := ReadModeFromContext(ctx); ok {
		mode = m
	}
	siArgs, err := buildStorageIntegrityArgs(r.factory.options.StorageIntegrity, mode)
	if err != nil {
		return RewriteResult{}, err // *RejectedError — the plugin fails closed on it
	}
	dynArgs := buildDynamicArgs(dbMap, knownPhys, r.sess.LogicalDatabaseName(), r.sess.PhysicalDatabaseName(), r.factory.options.Delim, logicalToRemote, remoteUpstreams, siArgs)
```

and after `resp, err := r.callWithTimeout(ctx, req)` succeeded, before the `switch resp.Code`:

```go
	// Spec G fail-closed rule (plan D-2): a non-Success answer that involves
	// a storage-integrity table must reach the client as an Exception.
	if key, si := storageIntegrityAccess(resp.GetOriginalAccessedTables()); si {
		if resp.GetCode() != pb.RewriteCode_Success {
			return RewriteResult{}, &RejectedError{Code: resp.GetCode(), Message: resp.GetMessage()}
		}
		if resp.GetStatementType() == pb.StatementType_STATEMENT_TYPE_INSERT && !r.factory.options.StorageIntegrity.InsertLaneEnabled {
			return RewriteResult{}, &RejectedError{Code: pb.RewriteCode_UnsupportedStatement,
				Message: "storage-integrity table " + key + " accepts writes only through the signed statement lane"}
		}
	}
```

`RewriteErrorMessage`: build `siArgs, _ := buildStorageIntegrityArgs(r.factory.options.StorageIntegrity, r.factory.options.StorageIntegrity.DefaultReadMode)` (ignore the error — reverse-mapping is best-effort; a nil block just skips SI names) and pass it to `buildDynamicArgs`.

- [ ] **Step 3: Run**

Run: `bazel run //:gazelle && bazel test //pkg/rewriter:rewriter_test --test_output=errors`
Expected: PASS (new + existing tests).

- [ ] **Step 4: AGENTS.md** — add to `pkg/rewriter/AGENTS.md` WHERE TO LOOK: `| Storage-integrity read surface | storage_integrity.go | ReadMode/SQL_x_read_mode, StorageIntegrityReadState port, RejectedError (fail-closed), StorageIntegrityArgs builder |` and CONVENTIONS bullet: "Any non-Success rewriter answer whose accessed tables carry `is_storage_integrity` is a `*RejectedError`, never fail-open."

- [ ] **Step 5: Commit**

```bash
git add pkg/rewriter
git commit -m "feat(rewriter): storage-integrity read mode, promotion-state port, fail-closed RejectedError"
```

### Task 18: `pkg/plugins/rewrite` — parse `SQL_x_read_mode`, carry it, fail closed on `RejectedError`

**Working directory:** `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/plugins/rewrite/rewriter.go:101-177` (`OnQuery`)
- Test: `pkg/plugins/rewrite/rewriter_test.go`

**Interfaces:**
- Consumes: `rewriter.ReadModeSettingKey`, `rewriter.ParseReadMode`, `rewriter.WithReadMode`, `*rewriter.RejectedError`.
- Produces: `func readModeFromQuery(q *chproto.Query) (rewriter.ReadMode, bool, error)`.

- [ ] **Step 1: Failing tests** (append to `rewriter_test.go`; `fakeRewriter` gains `err error` and `lastCtx context.Context` fields returned/recorded by `Rewrite`)

```go
func TestOnQuery_ReadModeSettingIsCarriedInContext(t *testing.T) {
	rw := &fakeRewriter{out: "REWRITTEN"}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}
	sess := newSessionForTest(t, 40)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1",
		Query: &chproto.Query{Body: "SELECT 1", Settings: []chproto.Setting{{Key: rewriter.ReadModeSettingKey, Value: "'unsafe_latest'", Custom: true}}}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatal(err)
	}
	if m, ok := rewriter.ReadModeFromContext(rw.lastCtx); !ok || m != rewriter.ReadModeUnsafeLatest {
		t.Fatalf("ctx read mode = %q %v", m, ok)
	}
	// Setting is left in place (D-3), like every other SQL_x_* key.
	if len(qctx.Query.Settings) != 1 {
		t.Fatalf("settings must not be stripped: %v", qctx.Query.Settings)
	}
}

func TestOnQuery_InvalidReadModeIsAnException(t *testing.T) {
	rw := &fakeRewriter{out: "REWRITTEN"}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}
	sess := newSessionForTest(t, 41)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1",
		Query: &chproto.Query{Body: "SELECT 1", Settings: []chproto.Setting{{Key: rewriter.ReadModeSettingKey, Value: "latest"}}}}
	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "SQL_x_read_mode") {
		t.Fatalf("err = %v, want invalid-mode rejection", err)
	}
	if rw.rewriteCalls != 0 {
		t.Fatal("rewriter must not run on an invalid mode")
	}
}

func TestOnQuery_RejectedErrorFailsClosed(t *testing.T) {
	rw := &fakeRewriter{err: &rewriter.RejectedError{Code: pb.RewriteCode_RewriteError, Message: "reserved column _hg_row_id is not addressable"}}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}
	sess := newSessionForTest(t, 42)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT _hg_row_id FROM db1.t", Query: &chproto.Query{Body: "SELECT _hg_row_id FROM db1.t"}}
	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "reserved column _hg_row_id") {
		t.Fatalf("err = %v, want the rewriter's rejection surfaced", err)
	}
}

func TestOnQuery_OrdinaryErrorStaysFailOpen(t *testing.T) {
	rw := &fakeRewriter{err: errors.New("transport down")}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}
	sess := newSessionForTest(t, 43)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1", Query: &chproto.Query{Body: "SELECT 1"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("ordinary rewriter errors must stay fail-open: %v", err)
	}
	if qctx.Query.Body != "SELECT 1" {
		t.Fatalf("body = %q", qctx.Query.Body)
	}
}
```

(imports: `errors`, `strings`, `pb "github.com/housegate/rewriter-proto/gen/pb"`.) In `fakeRewriter.Rewrite`: record `f.lastCtx = ctx`; `if f.err != nil { return rewriter.RewriteResult{}, f.err }`.

Run: `go test ./pkg/plugins/rewrite -run 'TestOnQuery_ReadMode|TestOnQuery_InvalidReadMode|TestOnQuery_RejectedError|TestOnQuery_OrdinaryError' -count=1`
Expected: FAIL (`ctx read mode = "" false`, invalid mode not rejected, RejectedError swallowed).

- [ ] **Step 2: Implement in `OnQuery`** — after the maintenance/platform-operator bypass and before `rw := p.rewriterFor(...)`:

```go
	if qctx.Query != nil {
		mode, ok, err := readModeFromQuery(qctx.Query)
		if err != nil {
			return err // invalid SQL_x_read_mode → Exception to the client
		}
		if ok {
			ctx = rewriter.WithReadMode(ctx, mode)
		}
	}
```

and replace the error branch after `rw.Rewrite`:

```go
	if err != nil {
		var rej *rewriter.RejectedError
		if errors.As(err, &rej) {
			logger.Infow("rewriter: statement rejected (fail-closed)", "code", rej.Code.String(), "message", rej.Message)
			return rej
		}
		logger.Warne(err, "rewriter: rewrite failed, forwarding original SQL")
		return nil
	}
```

Add the helper:

```go
// readModeFromQuery reads SQL_x_read_mode from the per-query settings.
// ok=false when absent; err when present but invalid. The setting is NOT
// removed — like SQL_x_auth_token / SQL_x_payer it flows to ClickHouse as
// a custom setting (plan deviation D-3).
func readModeFromQuery(q *chproto.Query) (rewriter.ReadMode, bool, error) {
	for _, s := range q.Settings {
		if s.Key != rewriter.ReadModeSettingKey {
			continue
		}
		m, err := rewriter.ParseReadMode(s.Value)
		if err != nil {
			return "", false, err
		}
		return m, true, nil
	}
	return "", false, nil
}
```

(add `"errors"` to imports.)

- [ ] **Step 3: Run**

Run: `bazel test //pkg/plugins/rewrite:rewrite_test --test_output=errors`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/plugins/rewrite
git commit -m "feat(rewrite): SQL_x_read_mode setting → per-query read mode; fail closed on RejectedError"
```

### Task 19: Root `Options.StorageIntegrityReadState` + `build.go` wiring + startup guard + testenv option

**Working directory:** `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `proxy.go` (`Options`, after `StorageIntegrityRuntime` :110), `build.go` (`buildRewriterFactory` :111-160 signature + `rewriter.Options` literal :132-144; caller :377; `buildServer` startup guard near :375), `pkg/integration/testenv/proxy.go` (new option), `build_test.go`
- Test: `build_test.go`

**Interfaces:**
- Produces: `housegate.Options.StorageIntegrityReadState rewriter.StorageIntegrityReadState`; `func buildRewriterFactory(cfg *config.Config, reg registry.Registry, si rewriter.StorageIntegrityOptions) rewriter.Factory`; `func storageIntegrityRewriterOptions(cfg *config.Config, rs rewriter.StorageIntegrityReadState) rewriter.StorageIntegrityOptions`; `testenv.WithStorageIntegrityReadState(rs rewriter.StorageIntegrityReadState) ProxyOption`.

- [ ] **Step 1: Failing tests** (append to `build_test.go`)

```go
func TestStorageIntegrityRewriterOptions_DerivesPhysicalNames(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Tables = []string{"tenant.events", "db1.t"}
	cfg.StorageIntegrity.Read.DefaultMode = "unsafe_latest"
	cfg.StorageIntegrity.Ingress.Enabled = false
	rs := &buildFakeReadState{}
	got := storageIntegrityRewriterOptions(cfg, rs)
	if len(got.Tables) != 2 || got.Tables[0] != (rewriter.StorageIntegrityTable{TableID: "tenant.events", SafeTable: "hg_safe.tenant__events", UnsafeTable: "hg_unsafe.tenant__events"}) {
		t.Fatalf("tables = %+v", got.Tables)
	}
	if got.DefaultReadMode != rewriter.ReadModeUnsafeLatest || got.ReadState != rs || got.InsertLaneEnabled {
		t.Fatalf("opts = %+v", got)
	}
	cfg.StorageIntegrity.Ingress.Enabled = true
	if !storageIntegrityRewriterOptions(cfg, nil).InsertLaneEnabled {
		t.Fatal("ingress enabled must enable the insert lane")
	}
}

func TestBuildServer_UnsafeLatestDefaultRequiresReadState(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Tables = []string{"tenant.events"}
	cfg.StorageIntegrity.Read.DefaultMode = "unsafe_latest"
	_, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), Rewriter: stubRewriterFactory{}}, nil)
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.read.default_mode unsafe_latest requires Options.StorageIntegrityReadState") {
		t.Fatalf("err = %v", err)
	}
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), Rewriter: stubRewriterFactory{}, StorageIntegrityReadState: &buildFakeReadState{}}, nil)
	if err != nil {
		t.Fatalf("with a port wired: %v", err)
	}
	bs.teardown()
}

type buildFakeReadState struct{}

func (buildFakeReadState) PromotedUnsafeParts(string) ([]string, error) { return nil, nil }
```

Run: `go vet . 2>&1 | grep -v "unkeyed fields" | head -5`
Expected: `undefined: storageIntegrityRewriterOptions` / `unknown field StorageIntegrityReadState`.

- [ ] **Step 2: Implement**

`proxy.go` `Options` (after `StorageIntegrityRuntime`):

```go
	// StorageIntegrityReadState supplies the promoted-but-not-yet-cleaned
	// unsafe part names the read rewrite excludes in unsafe_latest mode
	// (Spec G D2). sentio-node passes its embedded *snode.Role. Nil means
	// unsafe_latest is refused per query and rejected at startup as the
	// configured default.
	StorageIntegrityReadState rewriter.StorageIntegrityReadState
```

`build.go`:

```go
// storageIntegrityRewriterOptions derives the rewriter's SI read-surface
// options from config: physical names per Spec C D2, the default read mode,
// the host port, and whether the signed INSERT lane (ingress) is on.
func storageIntegrityRewriterOptions(cfg *config.Config, rs rewriter.StorageIntegrityReadState) rewriter.StorageIntegrityOptions {
	out := rewriter.StorageIntegrityOptions{
		DefaultReadMode:   rewriter.ReadMode(cfg.StorageIntegrity.Read.DefaultMode),
		ReadState:         rs,
		InsertLaneEnabled: cfg.StorageIntegrity.Ingress.Enabled,
	}
	for _, id := range cfg.StorageIntegrity.Tables {
		phys := config.StorageIntegrityPhysicalTable(id)
		out.Tables = append(out.Tables, rewriter.StorageIntegrityTable{
			TableID:     id,
			SafeTable:   config.StorageIntegritySafeDatabase + "." + phys,
			UnsafeTable: config.StorageIntegrityUnsafeDatabase + "." + phys,
		})
	}
	return out
}
```

`buildRewriterFactory(cfg, reg, si rewriter.StorageIntegrityOptions)`: set `StorageIntegrity: si` in the `rewriter.Options` literal. In `buildServer`, before the `var rwFactory` block:

```go
	if cfg.StorageIntegrity.Read.DefaultMode == config.StorageIntegrityReadModeUnsafeLatest && opts.StorageIntegrityReadState == nil {
		return nil, fmt.Errorf("storage_integrity.read.default_mode unsafe_latest requires Options.StorageIntegrityReadState (co-located SNode promotion journal); reference binaries can only serve safe reads")
	}
	if len(cfg.StorageIntegrity.Tables) > 0 && opts.StorageIntegrityReadState == nil {
		log.Warnw("storage_integrity: no read-state port wired; unsafe_latest reads will be refused", "tables", len(cfg.StorageIntegrity.Tables))
	}
```

and call `buildRewriterFactory(cfg, reg, storageIntegrityRewriterOptions(cfg, opts.StorageIntegrityReadState))`. `pkg/integration/testenv/proxy.go`:

```go
// WithStorageIntegrityReadState wires the promotion-state port the SI read
// rewrite consults in unsafe_latest mode.
func WithStorageIntegrityReadState(rs rewriter.StorageIntegrityReadState) ProxyOption {
	return func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityReadState = rs }
}
```

- [ ] **Step 3: Run**

Run: `bazel run //:gazelle && bazel test //:housegate_test //pkg/integration/testenv:testenv_test --test_output=errors`
Expected: PASS (testenv_test is docker-bound; if no docker socket, run `bazel build //pkg/integration/testenv:testenv_test` instead and note it).

- [ ] **Step 4: Commit**

```bash
git add proxy.go build.go build_test.go pkg/integration/testenv/proxy.go pkg/integration/testenv/BUILD.bazel
git commit -m "feat: Options.StorageIntegrityReadState port + SI read-surface rewriter wiring"
```

### Task 20: Docker integration test — SI reads through the native engine

**Working directory:** `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Create: `pkg/integration/storage_integrity_read_test.go` (target `//pkg/integration:integration_test`, already `manual` + in CI's explicit list)
- Modify: `.github/workflows/ci.yml` (integration job: fetch the FFI lib and pass `POLYGLOT_SQL_FFI_PATH`)

**Interfaces:**
- Consumes: `testenv.StartServerProxy`, `testenv.WithConfigMutator`, `testenv.WithExtraDatabases`, `testenv.WithStorageIntegrityReadState`, `openConnNoDB`, `openConn`, `chEnv`.

- [ ] **Step 1: Write the test**

```go
package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
)

type siReadStateStub struct {
	mu    sync.Mutex
	parts map[string][]string
}

func (s *siReadStateStub) PromotedUnsafeParts(tableID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.parts[tableID]...), nil
}

func (s *siReadStateStub) set(tableID string, parts ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.parts[tableID] = parts
}

// TestStorageIntegrityRead_SafeAndUnsafeLatest drives Spec G end to end
// through the native rewriter engine: an SI table's rows land in
// hg_unsafe (simulated staged write), are visible immediately under
// unsafe_latest, invisible under safe until a (simulated) promotion copies
// them into hg_safe, and never double-counted while the promoted unsafe
// part awaits cleanup. Needs the FFI lib (skips otherwise).
func TestStorageIntegrityRead_SafeAndUnsafeLatest(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run `go run ./cmd fetch-rewriter-lib --tag v0.7.0` and pass --test_env")
	}
	ctx := context.Background()
	const phys = "phys_si"
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS " + phys,
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"CREATE DATABASE IF NOT EXISTS hg_unsafe",
		"DROP TABLE IF EXISTS hg_safe.db1__t",
		"DROP TABLE IF EXISTS hg_unsafe.db1__t",
		"CREATE TABLE hg_unsafe.db1__t (_hg_row_id FixedString(32), a UInt32) ENGINE = MergeTree ORDER BY a",
		"CREATE TABLE hg_safe.db1__t AS hg_unsafe.db1__t ENGINE = MergeTree ORDER BY a",
		"SYSTEM STOP MERGES hg_unsafe.db1__t",
		"SYSTEM STOP MERGES hg_safe.db1__t",
		"INSERT INTO hg_unsafe.db1__t VALUES (repeat('a', 32), 1), (repeat('b', 32), 2)",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_safe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_unsafe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys)
	})
	var unsafePart string
	if err := seed.QueryRow(ctx, "SELECT name FROM system.parts WHERE database = 'hg_unsafe' AND table = 'db1__t' AND active").Scan(&unsafePart); err != nil {
		t.Fatalf("unsafe part: %v", err)
	}

	port := &siReadStateStub{parts: map[string][]string{}}
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("db1"),
		testenv.WithStorageIntegrityReadState(port),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.StorageIntegrity.Tables = []string{"db1.t"}
			cfg.StorageIntegrity.Read.DefaultMode = config.StorageIntegrityReadModeSafe
		}),
	)
	conn := openConn(t, proxy.Addr)
	count := func(mode string) (uint64, error) {
		qctx := ctx
		if mode != "" {
			qctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"SQL_x_read_mode": clickhouse.CustomSetting{Value: mode}}))
		}
		var n uint64
		err := conn.QueryRow(qctx, "SELECT count() FROM db1.t").Scan(&n)
		return n, err
	}
	mustCount := func(mode string, want uint64) {
		t.Helper()
		n, err := count(mode)
		if err != nil {
			t.Fatalf("count(mode=%q): %v", mode, err)
		}
		if n != want {
			t.Fatalf("count(mode=%q) = %d, want %d", mode, n, want)
		}
	}

	// 1. Staged rows: invisible in safe (the default), visible in unsafe_latest.
	mustCount("", 0)
	mustCount("safe", 0)
	mustCount("unsafe_latest", 2)

	// 2. Simulated promotion: rows copied into hg_safe; the unsafe part is
	//    now "promoted, not yet cleaned" → excluded from unsafe_latest.
	if err := seed.Exec(ctx, "INSERT INTO hg_safe.db1__t SELECT * FROM hg_unsafe.db1__t"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	port.set("db1.t", unsafePart)
	mustCount("safe", 2)
	mustCount("unsafe_latest", 2) // NOT 4 — the promoted unsafe part is excluded

	// 3. Simulated cleanup: unsafe part dropped, exclusion list emptied.
	if err := seed.Exec(ctx, fmt.Sprintf("ALTER TABLE hg_unsafe.db1__t DROP PART '%s'", unsafePart)); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	port.set("db1.t")
	mustCount("unsafe_latest", 2)

	// 4. Reserved column is unaddressable; `SELECT *` hides it (2 columns → only `a`).
	starRows, err := conn.Query(ctx, "SELECT * FROM db1.t LIMIT 1")
	if err != nil {
		t.Fatalf("SELECT *: %v", err)
	}
	if got := starRows.Columns(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("SELECT * columns = %v, want [a] (_hg_row_id must be hidden)", got)
	}
	starRows.Close()
	if err := conn.QueryRow(ctx, "SELECT _hg_row_id FROM db1.t").Scan(new(string)); err == nil || !strings.Contains(err.Error(), "reserved column _hg_row_id is not addressable") {
		t.Fatalf("reserved column must be rejected with the rewriter message, got %v", err)
	}
	// 5. Invalid mode is an Exception, not a silent fallback.
	if _, err := count("latest"); err == nil || !strings.Contains(err.Error(), "SQL_x_read_mode") {
		t.Fatalf("invalid SQL_x_read_mode must be rejected, got %v", err)
	}
	// 6. Non-lane write is refused with the spec message (no ingress configured here).
	if err := conn.Exec(ctx, "ALTER TABLE db1.t DELETE WHERE a = 1"); err == nil || !strings.Contains(err.Error(), "accepts writes only through the signed statement lane") {
		t.Fatalf("SI write must be refused, got %v", err)
	}
	// 7. DESCRIBE hides the reserved column.
	rows, err := conn.Query(ctx, "DESCRIBE TABLE db1.t")
	if err != nil {
		t.Fatalf("DESCRIBE: %v", err)
	}
	var names []string
	for rows.Next() {
		var name, typ, dt, de, cm, ce, te string
		if err := rows.Scan(&name, &typ, &dt, &de, &cm, &ce, &te); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, name)
	}
	if strings.Join(names, ",") != "a" {
		t.Fatalf("DESCRIBE columns = %v, want [a]", names)
	}
}
```

Delete step 4's `cols` probe if the `SELECT length(columns)` shape errors on your CH image — it is a log-only probe; the `_hg_row_id` rejection is the assertion.

- [ ] **Step 2: Run against docker (native engine)**

Run: `bazel test //pkg/integration:integration_test --test_filter='TestStorageIntegrityRead_SafeAndUnsafeLatest' --test_output=all --nocache_test_results --test_env=DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}') --test_env=HOME --test_env=POLYGLOT_SQL_FFI_PATH=$FFI 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: `--- PASS: TestStorageIntegrityRead_SafeAndUnsafeLatest`. Then the whole suite once, and diff any unrelated failure against `main` per the main-baseline rule.

- [ ] **Step 3: CI** — in `.github/workflows/ci.yml` integration job, before the `bazel test //pkg/integration…` step add:

```yaml
      - name: Fetch rewriter FFI lib
        run: echo "POLYGLOT_SQL_FFI_PATH=$(bazel run //cmd:housegate -- fetch-rewriter-lib --tag v0.7.0 2>/dev/null | tail -1)" >> "$GITHUB_ENV"
```
and append `--test_env=POLYGLOT_SQL_FFI_PATH` to that `bazel test` command line.

- [ ] **Step 4: Commit**

```bash
git add pkg/integration/storage_integrity_read_test.go pkg/integration/BUILD.bazel .github/workflows/ci.yml
git commit -m "test(integration): storage-integrity read surface e2e (safe / unsafe_latest / reserved column / DESCRIBE)"
```

### Task 21: housegate docs (CLAUDE.md, README) + PR

**Working directory:** `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `CLAUDE.md` (§4 rewriter paragraph :119; the `pkg/plugins/` bullet listing `rewrite`; add a `storage_integrity.*` bullet under Key Modules), `README.md` (config reference: `storage_integrity.tables`, `storage_integrity.read.default_mode`, `SQL_x_read_mode`)

- [ ] **Step 1: Write** — CLAUDE.md §4: after "Each call ships a single `dynamic_args` payload …" add: "When `storage_integrity.tables` is configured the same payload carries `storage_integrity` (`StorageIntegrityArgs`, rewriter-proto ≥ v0.2.0): per-logical-table `hg_safe`/`hg_unsafe` physical names (`config.StorageIntegrityPhysicalTable`), the read mode (config `storage_integrity.read.default_mode`, per query `SQL_x_read_mode`), and — in `unsafe_latest` — the promoted-not-yet-cleaned unsafe part names from `Options.StorageIntegrityReadState.PromotedUnsafeParts(tableID)` (sentio-node's `snode.Role`). Both engines turn SI reads into `(SELECT * EXCEPT (_hg_row_id) FROM hg_safe.<t> [UNION ALL … hg_unsafe.<t> WHERE _part NOT IN (…)]) AS <alias>`, `EXISTS TABLE` → safe table, `DESCRIBE` → `system.columns` SELECT, and refuse every other SI-touching statement (`UnsupportedStatement`) or a `_hg_row_id` reference (`RewriteError`). HouseGate **fails closed** on any non-Success answer whose `original_accessed_tables` carry `is_storage_integrity` (`*rewriter.RejectedError` → Exception), on `unsafe_latest` without the port, and on INSERT into an SI table when the ingress lane is off; ordinary rewriter errors stay fail-open. `SQL_x_read_mode` is read but not stripped (no `SQL_x_*` key is). Golden contract: `rewriter-go/internal/harness/testdata/storage_integrity_cases.json` (byte-identical copy in rewriter-grpc)." Under Known Rough Edges add: "SI INSERT is deliberately not rejected by the rewriter (ingress runs after rewrite and needs the classification + a resolvable physical target for sample-block negotiation) — see plan `docs/superpowers/plans/2026-08-18-storage-integrity-read-surface-rewrite.md` D-1."

- [ ] **Step 2: Full ladder** — `go build ./... && go vet ./... 2>&1 | grep -v "unkeyed fields"; bazel build //... && bazel test //...` → all PASS; then push and open the PR:

```bash
git add CLAUDE.md README.md
git commit -m "docs: storage-integrity read surface (Spec G) in CLAUDE.md/README"
git push -u origin feat/storage-integrity-read-surface
```

---

## Part E — arbiter-core + sentio-node (promotion journal port + wiring)

### Task 22: arbiter-core — journal promoted unsafe parts until cleanup; `snode.Role.PromotedUnsafeParts`

**Working directory:** `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

**Files:**
- Modify: `snode/state.go` (`localState` :22-29, `ensureMaps` :62-81, `RecordAppliedPromotion` :120-141, new methods), `snode/promote.go` (:52-59), `snode/cleanup.go` (:15-36), `snode/snode.go` (new exported method)
- Test: `snode/state_test.go` (pure, no ClickHouse), `snode/promote_test.go` + `snode/cleanup_test.go` (docker-bound, `ARBITER_CH_INTEGRATION=1`)

**Interfaces:**
- Produces:
  - `localState.PromotedUnsafeParts map[string][]string \`json:"promoted_unsafe_parts,omitempty"\`` keyed by `key(table, partition)` → unsafe part names promoted by an applied promotion whose cleanup has not been journaled (order = candidate order, deduped).
  - `func (st *stateStore) RecordAppliedPromotion(k partitionKey, seq uint64, ack arbiter.PromotionAck, newBaseRoot, newBaseSnapshotID string, partLtHashHexes []string, unsafePartNames []string) error` (new trailing param).
  - `func (st *stateStore) RecordCleanup(k partitionKey, partNames []string) error` — removes the names (missing names are a no-op) and persists.
  - `func (st *stateStore) PromotedUnsafeParts(table string) []string` — union over every partition of `table`, sorted.
  - `func (r *Role) PromotedUnsafeParts(tableID string) ([]string, error)` — the housegate `rewriter.StorageIntegrityReadState` port; never errors today (signature reserved for a future journal-read failure).

- [ ] **Step 1: Failing pure state tests** (append to `snode/state_test.go`)

```go
func TestStateStore_PromotedUnsafePartsLifecycle(t *testing.T) {
	dir := t.TempDir()
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	k0 := partitionKey{Table: "db.t", Partition: "p0"}
	k1 := partitionKey{Table: "db.t", Partition: "p1"}
	other := partitionKey{Table: "db.u", Partition: "p0"}
	ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0", Applied: true}
	if err := st.RecordAppliedPromotion(k0, 1, ack, "0xpost", "snap", nil, []string{"all_2_2_0", "all_1_1_0"}); err != nil {
		t.Fatalf("record p0: %v", err)
	}
	if err := st.RecordAppliedPromotion(k1, 1, ack, "0xpost", "snap", nil, []string{"all_9_9_0"}); err != nil {
		t.Fatalf("record p1: %v", err)
	}
	if err := st.RecordAppliedPromotion(other, 1, ack, "0xpost", "snap", nil, []string{"all_5_5_0"}); err != nil {
		t.Fatalf("record other: %v", err)
	}
	if got := st.PromotedUnsafeParts("db.t"); !reflect.DeepEqual(got, []string{"all_1_1_0", "all_2_2_0", "all_9_9_0"}) {
		t.Fatalf("promoted (sorted, table-wide) = %v", got)
	}
	// Second promotion on the same partition appends; duplicates collapse.
	if err := st.RecordAppliedPromotion(k0, 2, ack, "0xpost2", "snap", nil, []string{"all_3_3_0", "all_1_1_0"}); err != nil {
		t.Fatalf("record p0 seq2: %v", err)
	}
	if got := st.PromotedUnsafeParts("db.t"); len(got) != 4 {
		t.Fatalf("after second promotion = %v", got)
	}
	// Cleanup drops exactly the named parts; unknown names are ignored.
	if err := st.RecordCleanup(k0, []string{"all_1_1_0", "all_2_2_0", "ghost"}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if got := st.PromotedUnsafeParts("db.t"); !reflect.DeepEqual(got, []string{"all_3_3_0", "all_9_9_0"}) {
		t.Fatalf("after cleanup = %v", got)
	}
	// Durable across reopen.
	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := reopened.PromotedUnsafeParts("db.t"); !reflect.DeepEqual(got, []string{"all_3_3_0", "all_9_9_0"}) {
		t.Fatalf("reopened = %v", got)
	}
	if got := reopened.PromotedUnsafeParts("db.u"); !reflect.DeepEqual(got, []string{"all_5_5_0"}) {
		t.Fatalf("other table = %v", got)
	}
	if got := reopened.PromotedUnsafeParts("db.none"); got != nil {
		t.Fatalf("unknown table = %v, want nil", got)
	}
}
```

(add `"reflect"` to the test imports.)

Run: `go test ./snode -run TestStateStore_PromotedUnsafePartsLifecycle -count=1`
Expected: compile error (`too many arguments in call to st.RecordAppliedPromotion`, `undefined: RecordCleanup`).

- [ ] **Step 2: Implement in `state.go`**

```go
type localState struct {
	Watermarks      map[string]uint64               `json:"watermarks"`
	LastAcks        map[string]arbiter.PromotionAck `json:"last_acks"`
	BaseRoots       map[string]string               `json:"base_roots"`
	BaseSnapshotIDs map[string]string               `json:"base_snapshot_ids"`
	UnpromotedSums  map[string]string               `json:"unpromoted_sums"`
	IntakeParts     map[string]string               `json:"intake_parts,omitempty"`
	// PromotedUnsafeParts records, per partition key, the hg_unsafe part
	// names an applied promotion copied into hg_safe and whose cleanup we
	// have not journaled yet. HouseGate's unsafe_latest read surface
	// excludes exactly these (`_part NOT IN (...)`) so promoted rows are
	// not counted twice between REPLACE PARTITION and cleanup (Spec G D2).
	PromotedUnsafeParts map[string][]string `json:"promoted_unsafe_parts,omitempty"`
}
```

`ensureMaps`: `if s.PromotedUnsafeParts == nil { s.PromotedUnsafeParts = map[string][]string{} }`.

`RecordAppliedPromotion` gains `unsafePartNames []string` and, before `return st.persistLocked()`:

```go
	if len(unsafePartNames) > 0 {
		existing := st.s.PromotedUnsafeParts[ks]
		seen := make(map[string]bool, len(existing)+len(unsafePartNames))
		for _, p := range existing {
			seen[p] = true
		}
		for _, p := range unsafePartNames {
			if p == "" || seen[p] {
				continue
			}
			seen[p] = true
			existing = append(existing, p)
		}
		st.s.PromotedUnsafeParts[ks] = existing
	}
```

New methods:

```go
// RecordCleanup forgets promoted unsafe parts once their cleanup ran. It is
// journaled BEFORE the cleanup ack is sent: the parts are already dropped
// from ClickHouse, so even if the ack send fails the exclusion list must
// not keep naming parts that no longer exist (harmless) — the important
// direction is never to drop a name before the part is gone.
func (st *stateStore) RecordCleanup(k partitionKey, partNames []string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ks := key(k.Table, k.Partition)
	existing := st.s.PromotedUnsafeParts[ks]
	if len(existing) == 0 {
		return nil
	}
	drop := make(map[string]bool, len(partNames))
	for _, p := range partNames {
		drop[p] = true
	}
	kept := existing[:0]
	for _, p := range existing {
		if !drop[p] {
			kept = append(kept, p)
		}
	}
	if len(kept) == 0 {
		delete(st.s.PromotedUnsafeParts, ks)
	} else {
		st.s.PromotedUnsafeParts[ks] = kept
	}
	return st.persistLocked()
}

// PromotedUnsafeParts returns the sorted union, over every partition of
// `table`, of promoted-not-yet-cleaned unsafe part names; nil when none.
func (st *stateStore) PromotedUnsafeParts(table string) []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	prefix := table + "\x00"
	var out []string
	for ks, parts := range st.s.PromotedUnsafeParts {
		if !strings.HasPrefix(ks, prefix) {
			continue
		}
		out = append(out, parts...)
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}
```

(add `"sort"` to imports; `"strings"` is already imported.)

`promote.go` — collect the candidate names and pass them:

```go
	unsafeParts := make([]string, 0, len(cmd.CandidateParts))
	for _, cp := range cmd.CandidateParts {
		unsafeParts = append(unsafeParts, cp.PartName)
	}
	if err := r.state.RecordAppliedPromotion(k, cmd.PromotionSeq, ack, post, cmd.BaseSafeSnapshotID, hashes, unsafeParts); err != nil {
		return err
	}
```

`cleanup.go` — after the DROP PART loop, before building the ack:

```go
	names := make([]string, 0, len(cmd.Parts))
	for _, p := range cmd.Parts {
		names = append(names, p.PartName)
	}
	if err := r.state.RecordCleanup(partitionKey{Table: cmd.TableID, Partition: cmd.PartitionID}, names); err != nil {
		return fmt.Errorf("journal cleanup: %w", err)
	}
```

`snode.go` — the port:

```go
// PromotedUnsafeParts returns the hg_unsafe part names of tableID that an
// applied promotion already copied into hg_safe and whose cleanup has not
// yet been journaled. HouseGate's storage-integrity read surface excludes
// them in unsafe_latest mode (rewriter.StorageIntegrityReadState); pass
// the *Role straight into housegate.Options.StorageIntegrityReadState.
func (r *Role) PromotedUnsafeParts(tableID string) ([]string, error) {
	return r.state.PromotedUnsafeParts(tableID), nil
}
```

- [ ] **Step 3: Docker-bound assertions** — in `snode/promote_test.go` `TestHandlePromote_HappyPath` after the watermark check add:

```go
	if got, err := role.PromotedUnsafeParts(schema.TableID); err != nil || !reflect.DeepEqual(got, []string{part.PartName}) {
		t.Fatalf("PromotedUnsafeParts = %v %v, want [%s]", got, err, part.PartName)
	}
```

and in `snode/cleanup_test.go` `TestHandleCleanup_HappyPath` after `assertUnsafePartGone`:

```go
	if got, err := role.PromotedUnsafeParts(schema.TableID); err != nil || len(got) != 0 {
		t.Fatalf("PromotedUnsafeParts after cleanup = %v %v, want empty", got, err)
	}
```

- [ ] **Step 4: Run**

Run: `go build ./... && go test ./snode -run 'TestStateStore' -count=1 && bazel test //snode:snode_test --test_output=errors`
Expected: `ok` (pure tests); Bazel target PASS (CH-bound tests self-skip without `ARBITER_CH_INTEGRATION=1`). With a local ClickHouse: `ARBITER_CH_INTEGRATION=1 go test ./snode -run 'TestHandlePromote_HappyPath|TestHandleCleanup_HappyPath' -count=1` → `ok`.

- [ ] **Step 5: Commit + tag**

```bash
git checkout -b feat/promoted-unsafe-parts
git add snode
git commit -m "feat(snode): journal promoted unsafe parts until cleanup; Role.PromotedUnsafeParts port"
git push -u origin feat/promoted-unsafe-parts
```
Merge, then tag from `main`: `git tag -a v0.1.3 -m "snode.Role.PromotedUnsafeParts" && git push origin v0.1.3` (arbiter-core tags are `v0.1.x`; the current sentio-node pin is a pseudo-version past `v0.1.2`, so `v0.1.3` is the next tag — if a `v0.1.3` already exists use `v0.1.4`).

### Task 23: sentio-node — bump pins, `storage_integrity.tables` cross-check, wire the port

**Working directory:** `/Users/uranuswch/Dev/sentio_xyz/sentio-node`

**Files:**
- Modify: `go.mod`, `go.sum`, `config/config.go:393-425` (`validateStorageIntegrity`), `standalone/standalone.go:332-343` (`housegate.Options` literal), `README.md:56-59,94` (config sample + onboarding step 2), `MODULE.bazel` (only if `bazel mod tidy` changes it)
- Test: `config/config_test.go:166-193`

**Interfaces:**
- Consumes: housegate `Options.StorageIntegrityReadState`, `config.StorageIntegrityConfig.Tables`, `config.StorageIntegrityReadConfig`; arbiter-core `(*snode.Role).PromotedUnsafeParts`.

- [ ] **Step 1: Bump pins**

```bash
git checkout -b feat/storage-integrity-read-surface
go get github.com/sentioxyz/arbiter-core@v0.1.3 github.com/housegate/housegate@<housegate-commit-or-tag-from-Task-21> && go mod tidy
bazel mod tidy
```
Expected: `go.mod` shows the new arbiter-core tag and the housegate pseudo-version/tag; transitive `rewriter-go v0.7.0` / `rewriter-proto v0.2.0`.

- [ ] **Step 2: Failing config test** — replace `TestConfigValidate_StorageIntegrityAssembly`'s base setup and the "table id not merge-guarded" subtest:

```go
func TestConfigValidate_StorageIntegrityAssembly(t *testing.T) {
	base := indexerConfig(t)
	base.StorageIntegrity = validStorageIntegrityConfig()
	base.Housegate.StorageIntegrity.Runtime.ExpectedSource = "snode-1"
	base.Housegate.StorageIntegrity.Tables = []string{"orders.t"}
	require.NoError(t, base.Validate())

	t.Run("SI without housegate listen", func(t *testing.T) { /* unchanged */ })

	t.Run("table id not in housegate storage_integrity.tables", func(t *testing.T) {
		cfg := base
		cfg.StorageIntegrity.SNode.TableIDs = append(cfg.StorageIntegrity.SNode.TableIDs, "ghost.t")
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), `storage_integrity.snode.table_ids[1] "ghost.t" is not listed in housegate.storage_integrity.tables`)
	})

	t.Run("legacy merge_guard.tables is rejected by housegate validation", func(t *testing.T) {
		cfg := base
		cfg.Housegate.StorageIntegrity.Runtime.MergeGuard.LegacyTables = []housegateConfig.StorageIntegrityRuntimeMergeTableConfig{{Database: "hg_unsafe", Table: "orders__t"}}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "was renamed to storage_integrity.tables")
	})
}
```

Run: `go test ./config -run TestConfigValidate_StorageIntegrityAssembly -count=1`
Expected: compile error (`Tables` unknown / old `MergeGuard.Tables` field gone).

- [ ] **Step 3: Implement**

`config/config.go` `validateStorageIntegrity` — replace the `guarded` map + loop with:

```go
	declared := make(map[string]bool, len(c.Housegate.StorageIntegrity.Tables))
	for _, id := range c.Housegate.StorageIntegrity.Tables {
		declared[id] = true
	}
	for i, tableID := range c.StorageIntegrity.SNode.TableIDs {
		if !declared[tableID] {
			return fmt.Errorf(
				"storage_integrity.snode.table_ids[%d] %q is not listed in housegate.storage_integrity.tables (the merge guard, ingress and read rewrite derive hg_unsafe/hg_safe.%s from it)",
				i, tableID, snode.CHTableName(tableID),
			)
		}
	}
```

`standalone/standalone.go` — the `housegate.Options{...}` literal gains:

```go
			StorageIntegrityReadState:           siReadState,
```
with, just above the literal:

```go
		var siReadState rewriter.StorageIntegrityReadState // nil-safe: an untyped nil *snode.Role must not become a non-nil interface
		if siRole != nil {
			siReadState = siRole
		}
```
(import `"github.com/housegate/housegate/pkg/rewriter"`.) `siRole` is declared at :206 and assigned inside `if cfg.StorageIntegrity.Enabled {…}`, so this compiles as-is.

`README.md` — config sample: replace the `merge_guard:` block with

```yaml
  storage_integrity:
    tables: ["orders.t"]           # logical ids; hg_unsafe/hg_safe.orders__t derived
    read:
      default_mode: unsafe_latest  # devnet2 dogfood (roadmap §4.9); shipped default is safe
    ingress: …                     # unchanged
    runtime:
      …
      merge_guard:
        reassert_interval: 5s
```
and onboarding step 2: "Add its logical `<database>.<table>` id to BOTH `storage_integrity.snode.table_ids` and `housegate.storage_integrity.tables` (one list drives the merge guard, the ingress and the read rewrite)."

- [ ] **Step 4: Run**

Run: `go build ./... && go test ./config ./standalone -count=1 && bazel build //... && bazel test //config:config_test //standalone:standalone_test --test_output=errors`
Expected: `ok` / PASS (`storage_integrity_smoke_test.go` self-skips without `SENTIO_SI_E2E=1`). If `SENTIO_SI_E2E=1` infra is available: run it once and additionally issue `SELECT count() FROM <table> SETTINGS SQL_x_read_mode = 'unsafe_latest'` through the node's housegate right after the smoke INSERT — expected to return the inserted row count (the smoke's INSERT lands in `hg_unsafe`).

- [ ] **Step 5: Commit + PR**

```bash
git add go.mod go.sum MODULE.bazel config standalone README.md
git commit -m "feat(storage-integrity): wire snode.Role as housegate read-state port; storage_integrity.tables cross-check"
git push -u origin feat/storage-integrity-read-surface
```

---

## Self-review

**1. Spec coverage** — see the map below; every spec section maps to at least one task. Gaps found and closed while reviewing: `SHOW TABLES` "nothing to do beyond a test" → golden case `si_show_tables_unchanged` (Task 3); GRANT/REVOKE on SI tables → Task 7/12; view bodies referencing SI tables (reject propagation) → Task 6 `dispatchView` and Task 11 `rewriteEmbeddedViewBody`; startup guard for `default_mode: unsafe_latest` without a port → Task 19.

**2. Placeholder scan** — no TBD/TODO. Version tags that do not exist yet are stated with the rule that produces them (`v0.2.0` proto, `v0.7.0` rewriter-go, `v0.1.3` arbiter-core) plus the "use the printed tag" instruction; the housegate commit hash sentio-node pins is written as `<housegate-commit-or-tag-from-Task-21>` because it is produced by Task 21's merge. `$FFI` / `$RB` / `$WD` are shell variables defined in the same task/part.

**3. Type consistency** — checked: `nameresolve.LookupStorageIntegrity(db, table, args) (*pb.StorageIntegrityArgs_Table, string, bool)` used identically in Tasks 4/6/7/8; `engine.ActionSubquery` + `TableDecision.Subquery` (Task 5) consumed by Task 6's `storageIntegrityDecision`; `splitPhysicalName` defined in Task 6 and used by Tasks 7/8; `rewriter.StorageIntegrityReadState.PromotedUnsafeParts(tableID string) ([]string, error)` (Task 17) == `(*snode.Role).PromotedUnsafeParts` (Task 22) == the fakes in Tasks 17/19/20; `buildDynamicArgs(..., si *pb.StorageIntegrityArgs)` (Task 17) matches both call sites; `buildStorageIntegrityRuntimeConsumer(runtimeCfg, tables []string, opts)` (Task 16) matches `build.go` and `build_test.go`; `config.StorageIntegrityPhysicalTable` / `SplitStorageIntegrityTableID` (Task 16) used in Task 19; the SI reject message string is identical in Go (`nameresolve.StorageIntegrityWriteRejectMessage`), C++ (`storageIntegrityWriteRejectMessage`), housegate (`sentio.go` insert-lane reject) and the golden JSON.

**Fixes applied inline during review:** Task 20 originally contained a log-only "column probe"; it is removed below (the `_hg_row_id` rejection and the DESCRIBE column list are the assertions).

## Spec coverage map

| Spec G section / requirement | Task(s) |
|---|---|
| §3 D1 default read mode config + `SQL_x_read_mode` per query | 16 (config), 17 (`ReadMode`, ctx), 18 (setting parse), 19 (startup guard), 20 (e2e) |
| §3 D2 part-name exclusion list from SNode journal; nil port ⇒ refuse | 17 (`buildStorageIntegrityArgs`, `RejectedError`), 19, 22 (journal + port), 23 (wiring) |
| §3 D3 projection subquery hides `_hg_row_id`; explicit reference ⇒ `RewriteError` | 5 (`ActionSubquery`, `ReferencesIdentifier`), 6, 11 |
| §3 D4 explicit membership `storage_integrity.tables[]` (renamed) | 16, 23 (cross-check) |
| §4 proto `StorageIntegrityArgs` (=12) + `AccessedTable.is_storage_integrity` (=6) | 1; bumps 2, 10, 15 |
| §4.1 SELECT family SAFE / UNSAFE_LATEST (+ empty list omits WHERE), SI wins over `database_map`, reserved reject, `is_storage_integrity` on accessed | 4, 5, 6 (Go); 11 (C++); goldens 3/10 |
| §4.2 EXISTS → safe table; SHOW TABLES unchanged | 7, 12; golden `si_exists_table_safe`, `si_show_tables_unchanged` |
| §4.3 DESCRIBE metadata SELECT; SHOW CREATE unsupported (E D6 prerequisite handled per D-7) | 1 (enum), 8, 13; 7/12 (SHOW CREATE) |
| §4.4 non-lane writes/DDL/GRANT ⇒ `UnsupportedStatement` (INSERT per D-1) | 7, 12; housegate insert-lane reject 17/19 |
| §5 `buildDynamicArgs` fills `storage_integrity`; port; setting passthrough (D-3); config + `Validate()` | 15, 16, 17, 18, 19 |
| §6 shared goldens (both engines) | 3, 9, 10–14 (incl. oracle run in 14) |
| §6 housegate unit tests (args, invalid mode, nil port) | 17, 18, 19 |
| §6 docker `pkg/integration` SI read e2e | 20 |
| §6 sentio-node `PromotedUnsafeParts` unit test | 22 |
| §7 delivery order proto → engines → housegate → sentio-node | Parts A→E |
| Roadmap §4.9 devnet2 `unsafe_latest` policy | 23 (README sample) |
| Housegate CLAUDE.md / repo docs | 9, 14, 21, 23 |
