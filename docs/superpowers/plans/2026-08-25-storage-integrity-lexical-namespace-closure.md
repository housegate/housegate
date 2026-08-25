# Storage-Integrity Lexical and SHOW-Namespace Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the two bypasses the 2026-08-25 verification review reproduced against shipped code — ClickHouse heredoc literals (`$$…$$` / `$tag$…$tag$`) blanking `sireserved`'s operator guard, and the `SHOW COLUMNS` / `INDEX` / `INDEXES` / `KEYS` family escaping the engine namespace gate — plus the two Medium gaps beside them (foreign-connector table functions outside the namespace decoder, and a cross-engine differential that has never been executed). One further Critical bypass, measured while writing this plan and **not** described in Spec N, is folded in: the Go engine decodes a *tagged* heredoc argument as `"<tag>\x00<body>"`, so `merge($t$hg_safe$t$, 'db1__t')` passes the namespace gate and is then re-emitted as `merge('hg_safe', 'db1__t')` for ClickHouse to execute.

**Architecture:** Four independent layers, each safe on its own. (1) `sireserved`'s lexical model gains the heredoc span, so every place a reserved name can hide is a span the scanner understands, and an unmodelled `$` is refused rather than copied through (D1). (2) The engines' SHOW dispatch stops classifying negatively: an explicit three-way table (rewritten / target-bearing / target-less) gates every variant that names a database or a table, and any kind in none of the three lists falls through to the Spec I D1 catch-all instead of being assumed harmless (D2). (3) The namespace decoder learns the foreign-connector family and the tagged-heredoc literal encoding, so the value policy inspects is the value the generator emits (D4 + the new finding). (4) The two engines are proved equal by *executing* the `REWRITER_ORACLE_ADDR` differential over the whole corpus, not by asserting that they share a JSON file (D3). Every new behaviour is pinned in the one byte-identical corpus (D5).

**Tech Stack:** Go 1.25 + polyglot FFI via PureGo (rewriter-go), C++23 + ClickHouse 26.3 parser + gtest built on the remote box (rewriter-grpc), Go + Bazel 9.1 (housegate), ClickHouse 25.8 in docker for the integration lane.

**Spec:** `docs/superpowers/specs/2026-08-25-storage-integrity-lexical-namespace-closure-design.md` (Spec N, as amended by commit `5ec9d5b` with measured engine behaviour). Roadmap: `docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md` — its §4 decisions 1, 2, 3, 4 and 9 bind this plan. Remediates: `docs/superpowers/specs/2026-08-19-storage-integrity-surface-failclosed-design.md` (Spec I) and its plan `docs/superpowers/plans/2026-08-19-storage-integrity-surface-failclosed.md`; this plan does not change Spec I's decisions, it completes D1's lexical obligation and replaces the SHOW half of D2's negative classification.

## Global Constraints

Every task's requirements implicitly include this section. Nothing here is invented for this plan — each item is quoted from frozen Spec G/I/J contracts or measured at the source named beside it.

- **Corpus, pre-change state (measured 2026-08-25):** `rewriter-go/internal/harness/testdata/storage_integrity_cases.json` — **210 cases, 211531 bytes, sha256 `309d738050fd05edd8e1f51f59a071c9c593e7aae10ea79dc7d4781c708ce281`, FNV-1a/64 fingerprint `15695596693549276030`**. `cmp` against `rewriter-grpc/tests/testdata/storage_integrity_cases.json` is byte-identical today. The Go copy is authoritative during authoring; the C++ copy is produced by `cp` and never hand-edited (Spec G plan D-10).
- **The corpus is pinned in FOUR places and all four move together.** `rewriter-go/internal/harness/sicorpus_test.go:231-233` (`SICorpusFingerprint`, `SICorpusBytes`, `SICorpusCases`) and `rewriter-grpc/tests/si_corpus.h:48-50` (`kCorpusFingerprint`, `kCorpusBytes`, `kCorpusCases`). `TestSICorpusIsBytePinned` and its C++ mirror fail loudly when they drift — that is the intended tripwire, not an obstacle to route around.
- **Corpus schema is frozen and strictly decoded** (`DisallowUnknownFields`): `name`, `sql`, `dynamic`, `want_code`, `want_stmt`, `want_sql`, `want_sql_go`, `want_sql_cpp`, `want_sql_contains`, `want_sql_not_contains`, `want_message_contains`, `want_table_rewrites`, `want_accessed`, `reject`, `allow_sql_divergence`, `want_no_contract_ack`. **No new key is introduced by this plan.** `ValidateSICorpus` (`sicorpus_test.go:136`) enforces: a reject case sets `reject: true`, a non-`Success` `want_code`, a `want_message_contains`, and pins **no** SQL; a success case pins `want_sql` exactly (or `allow_sql_divergence` plus both per-engine pins); `want_sql_contains` may not already be a substring of the input SQL.
- **D1 generic message (exact string, `rewriter-go/native.go:127`):** `storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded`.
- **Existing SI message vocabulary — reuse, never invent** (`rewriter-go/internal/nameresolve/resolve.go:423-447`, `rewriter-grpc/src/handlers/storage_integrity.h`):
  - `storage-integrity physical database <db> is not directly addressable`
  - `storage-integrity physical table <db>.<table> is not directly addressable`
  - `storage-integrity logical database <db> is not directly addressable`
  - `storage-integrity logical database <db> is not authorized by database_map`
  - `storage-integrity table <logical-key> accepts writes only through the signed statement lane`
  - `storage-integrity table function namespace is not statically resolvable`
  - `storage-integrity SHOW <KIND> database is not statically resolvable` (`rejectUnresolvedShowDatabase`, `internal/handlers/dblevel.go:394-398`)
- **Reserved names are frozen.** Physical databases `hg_safe` / `hg_unsafe`, reserved column `_hg_row_id`, `CHTableName(id) = ReplaceAll(id, ".", "__")`.
- **Contract acknowledgement is unchanged.** `STORAGE_INTEGRITY_CONTRACT_V1` in, echoed once out. **Nothing in this plan bumps rewriter-proto.**
- **rewriter-go:** polyglot imports only inside `internal/engine`; `internal/nameresolve` imports neither engine nor polyglot; engine-backed tests skip unless `POLYGLOT_SQL_FFI_PATH` is set. `make ffi` builds it at `third_party/lib/libpolyglot_sql_ffi.<dylib|so>` and `make test` exports it automatically. **A prebuilt lib already exists at `/Users/uranuswch/Dev/housegate/rewriter-go/third_party/lib/libpolyglot_sql_ffi.dylib`** (macOS); on Linux substitute `.so` everywhere below.
- **rewriter-grpc builds only on the remote box** (`ssh -p 30100 sentio@64.38.131.242`, workdir `/home/sentio/chen/rewriter-grpc/`). Dev loop = rsync → `./scripts.sh rebuild` → `ctest --test-dir build --output-on-failure`. Single test: `./build/rewriter_tests --gtest_filter='<Suite.Name>'`. **Never run a local cmake.** The `clickHouse/` submodule is not checked out locally, so every ClickHouse AST class name and field must be confirmed on the box before code that names it is written.
- **housegate:** Bazel is the test ground truth (`bazel build //...`, `bazel test //...`); module path `github.com/housegate/housegate`; run `bazel mod tidy && bazel run //:gazelle` after dependency or new-file changes; `pkg/integration` targets are `manual`-tagged and must be listed in `.github/workflows/ci.yml`.
- **housegate work lands on the PR #141 branch `feature/si-surface-failclosed-housegate`, never on `main`.** Spec N §5: #141 must not merge without D1, because D1 closes a hole #141 itself introduces.
- **English only** for identifiers, comments, log messages, and operator-facing error strings, in all three repos. Markdown: no hard line-wrapping, one paragraph per line.
- **One commit per task**, conventional-commit prefixes, and every task ends with its named verification command green.
- **Every new guard ships with a step proving it fails against the unfixed code** (roadmap §4.9). Each task below states the expected pre-fix failure *per test*. A guard that passes before the fix is a guard that is not testing the fix — stop and re-derive it rather than proceeding.
- **Oracle warning for Parts A and D:** `TestStorageIntegrityGolden` diffs against the C++ oracle whenever `REWRITER_ORACLE_ADDR` is set. The C++ side is not fixed until Part B, so **`env -u REWRITER_ORACLE_ADDR`** every Go test run in Part A. Part C turns it back on deliberately.

## Deviations from Spec N (read before starting)

These are places where Spec N's text does not survive contact with the source. Each was measured, not inferred; each names the measurement. Do not silently work around them — the deviation is the finding.

- **D-1 (new Critical, not in Spec N): the tagged heredoc is an *engine* bypass, not only a `sireserved` one.** Polyglot encodes a heredoc as `literal_type: "dollar_string"` whose `value` is `"<tag>\x00<body>"` for the tagged form and plain `"<body>"` for the bare `$$…$$` form (AST dumped from the live v0.9.0 engine). `tableFunctionArgValue` (`internal/engine/nodes.go:1905-1913`) reads `lit["value"]` without consulting `literal_type`, so the namespace decoder sees `tag\x00hg_safe`, matches nothing, and lets the statement through — while `Generate` re-emits it as `'hg_safe'`. Measured with SI configured: `SELECT * FROM merge($tag$hg_safe$tag$, 'db1__t')` → **`Success`, SQL `SELECT * FROM merge('hg_safe', 'db1__t')`**, versus `merge($$hg_safe$$, 'db1__t')` → `RewriteError` and `merge('hg_safe', 'db1__t')` → `RewriteError`. This is reachable by any ordinary authenticated user — no operator marker — and is strictly more severe than 1d. **Task 4 closes it.**
- **D-2: `redis()`'s second argument is a column name, not a table, and `sqlite()`'s is a file-local table.** Verified against the ClickHouse docs (fetched 2026-08-25): `redis(host:port, key, structure[, db_index, password, pool_size])` — `key` is "any column name in the column list"; `sqlite(db_path, table_name)` — a table inside a SQLite *file*. Neither names a ClickHouse database or table, so Spec N D4's "`sqlite` and `redis` take `(path|address, table)` … decode as a single-name reference like `merge`'s one-argument form" would make them **false-positive generators**: `merge`'s one-argument form means *current database + table pattern*, so `SELECT * FROM sqlite('/tmp/x.db', 'db1__t')` under `upstream_physical_database_in_context = hg_safe` would flip from today's `Success` (measured) to a rejection of a statement that cannot touch ClickHouse at all. **Task 3 excludes `redis` and `sqlite` and records the exclusion**, keeping `mysql`, `postgresql`, `mongodb`, `jdbc`, `odbc` — the five whose documented signature really does carry a foreign `(database|schema, table)` pair reachable by a ClickHouse loopback (native MySQL 9004 / PostgreSQL 9005 listeners; a ClickHouse JDBC/ODBC DSN).
- **D-3: several D4 functions are arity-dependent and all of them have a named-collection form.** `mongodb` also accepts `(uri, collection, structure[, oid_columns])` where index 1 is the *collection*; `jdbc`/`odbc` accept both `(datasource, database, table)` and `(datasource, table)`; every one of the five accepts `f(named_collection[, k=v…])`. Task 3 therefore decodes by arity and treats a non-literal or keyword argument as *unresolvable* → rejection, following the `si_remote_unresolved_namespace_rejected` precedent, instead of the flat "pair at argument index 1" Spec N D4 states.
- **D-4: the unknown-SHOW-kind case probably cannot live in the shared corpus.** Polyglot parses `SHOW SOMETHINGNEW FROM hg_safe` cleanly (measured: `ShowWhat=SOMETHINGNEW`, `DB=hg_safe`, `Success` today), but ClickHouse's `ParserShowTablesQuery` accepts a fixed keyword set, so the C++ engine most likely answers `SyntaxError`. Both are rejections and both fail closed, but the corpus schema has exactly one `want_code` per case and no per-engine variant of it. Task 8 Step 2 **measures** the C++ answer on the box and takes the documented branch: same code and message → promote to the shared corpus; different code → keep the assertion engine-local in `internal/handlers/dblevel_test.go` plus a C++-local gtest, and record the reason in the spec's delivery note.
- **D-5: Spec N §4.6's heredoc integration assertion needs a maintenance session the harness cannot currently build.** `sireserved.OnQuery` returns immediately unless `Snapshot().Maintenance || Snapshot().PlatformOperator` (`pkg/plugins/sireserved/plugin.go:29-31`), and that flag is only set by `authplugin` after a JWS verify whose signer equals the host-injected `Options.Signer.Address()` (`build.go:356-361`, `pkg/plugins/auth/jws.go:87-95`). `testenv.StartServerProxy` never sets `opts.Signer`. Task 14 therefore builds the session explicitly from parts that already exist — `auth.NewRelaySigner`, `authProxyConfig`, `openSignedConn` (`pkg/integration/auth_test.go:50-75`), and a one-line inline `ProxyOption` that assigns `opts.Signer` — rather than assuming the assertion is a two-line addition.
- **D-6: the release tag is workflow-derived, so "v0.9.1" is a prediction, not an instruction.** `rewriter-go/scripts/next-version.sh` bumps `Z` only when the previous tag's creator date is *today* in `Asia/Shanghai`; `v0.9.0` was tagged 2026-08-25. A release the same day yields `v0.9.1`; a release the next day yields `v0.10.0`. Task 16 records whatever the workflow prints as `<rewriter-go-tag>` and never hand-forces a version. Spec O consumes that recorded tag.

## File map

| Repo | Create | Modify |
|---|---|---|
| rewriter-go | — | `internal/engine/dblevel.go` (+`dblevel_test.go`), `internal/engine/nodes.go` (+`nodes_test.go`), `internal/handlers/dblevel.go` (+`dblevel_test.go`), `internal/nameresolve/resolve.go`, `native.go`, `internal/harness/sicorpus_test.go`, `internal/harness/testdata/storage_integrity_cases.json` |
| rewriter-grpc | `src/handlers/show_columns.cc` + `src/handlers/show_columns.h` (only if Task 8 Step 3 shows `show_tables.cc` cannot host the family) | `src/handlers/show_tables.cc`, `src/handlers/show_tables.h`, `src/handlers/storage_integrity.cc`, `src/handlers/storage_integrity.h`, `src/rewriter-server.cc`, `tests/rewriter_test.cc`, `tests/si_corpus.h`, `tests/testdata/storage_integrity_cases.json`, `CMakeLists.txt` (only alongside a new source file) |
| housegate (branch `feature/si-surface-failclosed-housegate`) | — | `pkg/plugins/sireserved/plugin.go`, `pkg/plugins/sireserved/plugin_test.go`, `pkg/integration/storage_integrity_read_test.go`, `pkg/plugins/AGENTS.md`, `CLAUDE.md`, `.github/workflows/ci.yml` |
| housegate docs (branch `docs/storage-integrity-lexical-rollout-specs` or the PR branch, per Task 17) | — | `docs/superpowers/specs/2026-08-25-storage-integrity-lexical-namespace-closure-design.md`, `docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md` |

---

## Part A — rewriter-go (native engine + shared corpus authoring)

**Working directory for every Part A task:** `/Users/uranuswch/Dev/housegate/rewriter-go`

**Standard Part A test invocation** (referenced below as *the Go engine run*; the oracle is deliberately unset until Part C):

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./<pkg>/ -run '<regex>' -v
```

- [ ] **Task 0 (pre-flight, do once):** branch and prove the baseline is green and the corpus matches its recorded state.

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git checkout -b feat/si-lexical-namespace-closure
ls third_party/lib/libpolyglot_sql_ffi.dylib || make ffi
env -u REWRITER_ORACLE_ADDR make test
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json
cmp internal/harness/testdata/storage_integrity_cases.json \
    /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json && echo IDENTICAL
```

Expected: every package `ok`; sha256 = `309d738050fd05edd8e1f51f59a071c9c593e7aae10ea79dc7d4781c708ce281`; `IDENTICAL`. Record any pre-existing failure now — it is the baseline, not your regression.

### Task 1: D2 parser — the `EXTENDED` prefix and the reversed two-`FROM` grammar

`ParseDBLevel` treats the first `FROM|IN` after the SHOW kind as the *database*. For `COLUMNS` / `INDEX` / `INDEXES` / `KEYS` that clause names the **table**, and the database is an optional *second* clause. `EXTENDED` is not in the prefix set, so it is consumed as the kind. Both were re-measured on the live v0.9.0 engine while writing this plan and match Spec N §1b's table exactly.

**Files:**
- Modify: `internal/engine/dblevel.go` (`DBLevelInfo` struct at `:18-30`; the `case "SHOW":` arm at `:63-119`)
- Test: `internal/engine/dblevel_test.go`

**Interfaces:**
- Produces on `engine.DBLevelInfo`: `ShowTable string`, `ShowTableResolved bool`, `HasTableClause bool`. Task 2 is the only consumer.
- Unchanged semantics for every non-`COLUMNS`/`INDEX`-family kind: `TABLES`, `DATABASES`, `DICTIONARIES`, `CREATE`, `CLUSTER`, `SETTINGS`, `MERGES` keep today's `DB` / `HasDBClause` / `DBResolved` meaning, byte for byte.

- [ ] **Step 1: Write the failing table test (red)**

Append to `internal/engine/dblevel_test.go`:

```go
// TestParseDBLevel_columnsFamilyBindsTableThenDatabase pins ClickHouse's
// SHOW [EXTENDED] [FULL] COLUMNS {FROM|IN} <table> [{FROM|IN} <database>]
// grammar (and the INDEX/INDEXES/KEYS spelling of the same shape), whose
// clause order is the reverse of the SHOW TABLES family's.
func TestParseDBLevel_columnsFamilyBindsTableThenDatabase(t *testing.T) {
	e := newTestEngine(t)
	for _, tc := range []struct {
		sql             string
		wantShow        string
		wantExtended    bool
		wantFull        bool
		wantTableClause bool
		wantTable       string
		wantDBClause    bool
		wantDBResolved  bool
		wantDB          string
	}{
		{sql: "SHOW COLUMNS FROM db1__t FROM hg_safe", wantShow: "COLUMNS",
			wantTableClause: true, wantTable: "db1__t", wantDBClause: true, wantDBResolved: true, wantDB: "hg_safe"},
		{sql: "SHOW COLUMNS FROM hg_safe.db1__t", wantShow: "COLUMNS",
			wantTableClause: true, wantTable: "db1__t", wantDBClause: true, wantDBResolved: true, wantDB: "hg_safe"},
		{sql: "SHOW COLUMNS FROM db1__t", wantShow: "COLUMNS",
			wantTableClause: true, wantTable: "db1__t"},
		{sql: "SHOW EXTENDED COLUMNS FROM db1__t FROM hg_safe", wantShow: "COLUMNS", wantExtended: true,
			wantTableClause: true, wantTable: "db1__t", wantDBClause: true, wantDBResolved: true, wantDB: "hg_safe"},
		{sql: "SHOW EXTENDED FULL COLUMNS FROM db1__t IN hg_unsafe", wantShow: "COLUMNS", wantExtended: true, wantFull: true,
			wantTableClause: true, wantTable: "db1__t", wantDBClause: true, wantDBResolved: true, wantDB: "hg_unsafe"},
		{sql: "SHOW FULL COLUMNS FROM db1__t IN hg_unsafe", wantShow: "COLUMNS", wantFull: true,
			wantTableClause: true, wantTable: "db1__t", wantDBClause: true, wantDBResolved: true, wantDB: "hg_unsafe"},
		{sql: "SHOW INDEX FROM hg_safe.db1__t", wantShow: "INDEX",
			wantTableClause: true, wantTable: "db1__t", wantDBClause: true, wantDBResolved: true, wantDB: "hg_safe"},
		{sql: "SHOW INDEXES FROM db1__t FROM hg_safe", wantShow: "INDEXES",
			wantTableClause: true, wantTable: "db1__t", wantDBClause: true, wantDBResolved: true, wantDB: "hg_safe"},
		{sql: "SHOW KEYS FROM db1__t FROM hg_safe", wantShow: "KEYS",
			wantTableClause: true, wantTable: "db1__t", wantDBClause: true, wantDBResolved: true, wantDB: "hg_safe"},
		{sql: "SHOW EXTENDED INDEX FROM db1__t FROM hg_safe", wantShow: "INDEX", wantExtended: true,
			wantTableClause: true, wantTable: "db1__t", wantDBClause: true, wantDBResolved: true, wantDB: "hg_safe"},
		// An explicit but unresolvable database target stays explicit-and-unresolved,
		// exactly as the TABLES/DICTIONARIES family already does.
		{sql: "SHOW COLUMNS FROM db1__t FROM {db:Identifier}", wantShow: "COLUMNS",
			wantTableClause: true, wantTable: "db1__t", wantDBClause: true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			got, err := ParseDBLevel(e, tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			if got.ShowWhat != tc.wantShow || got.ShowExtended != tc.wantExtended || got.ShowFull != tc.wantFull ||
				got.HasTableClause != tc.wantTableClause || got.ShowTable != tc.wantTable ||
				got.HasDBClause != tc.wantDBClause || got.DBResolved != tc.wantDBResolved || got.DB != tc.wantDB {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

// TestParseDBLevel_nonColumnsFamilyGrammarIsUnchanged is the regression half:
// the reversed grammar must not leak into the families whose single FROM/IN
// really does name a database.
func TestParseDBLevel_nonColumnsFamilyGrammarIsUnchanged(t *testing.T) {
	e := newTestEngine(t)
	for _, tc := range []struct{ sql, want string }{
		{"SHOW TABLES FROM hg_safe", "hg_safe"},
		{"SHOW DICTIONARIES FROM hg_safe", "hg_safe"},
		{"SHOW FULL DICTIONARIES IN db1", "db1"},
		{"SHOW SOMETHINGNEW FROM hg_safe", "hg_safe"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			got, err := ParseDBLevel(e, tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			if !got.HasDBClause || !got.DBResolved || got.DB != tc.want || got.HasTableClause {
				t.Fatalf("got %+v, want database-only clause %q", got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run both tests and record the exact pre-fix failure**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./internal/engine/ -run 'TestParseDBLevel_columnsFamilyBindsTableThenDatabase|TestParseDBLevel_nonColumnsFamilyGrammarIsUnchanged' -v
```

Expected **before** the fix, measured on the live engine:

- The whole file does not compile: `ShowExtended`, `HasTableClause` and `ShowTable` are undefined fields on `DBLevelInfo`. That compile error IS the first red state; do not "fix" it by deleting the assertions.
- After adding only the three struct fields (zero-valued, no parser change), every `columnsFamily` row fails with the measured values: `SHOW COLUMNS FROM db1__t FROM hg_safe` → `DB:db1__t HasDBClause:true DBResolved:true`, i.e. the **table** bound as the database; `SHOW COLUMNS FROM hg_safe.db1__t` → `DB:hg_safe` but `ShowTable:""`; `SHOW COLUMNS FROM db1__t` → `DB:db1__t HasDBClause:true` (a table reported as a resolved database clause — the sharpest single symptom); every `EXTENDED` row → `ShowWhat:EXTENDED DB:"" HasDBClause:false`.
- `TestParseDBLevel_nonColumnsFamilyGrammarIsUnchanged` **passes** before and after. If it ever fails, the fix has leaked into the TABLES/DICTIONARIES grammar.

- [ ] **Step 3: Add the three fields**

In `internal/engine/dblevel.go`, extend `DBLevelInfo`:

```go
	ShowExtended        bool   // SHOW carries the optional EXTENDED prefix (SHOW [EXTENDED] [FULL] COLUMNS ...)
	ShowTable           string // COLUMNS/INDEX family: the table named by the FIRST FROM/IN clause; "" otherwise
	ShowTableResolved   bool   // the COLUMNS/INDEX family table target reduced to a static identifier
	HasTableClause      bool   // the COLUMNS/INDEX family carries an explicit table clause, resolvable or not
```

Keep the existing field comments intact; `DB` / `HasDBClause` / `DBResolved` keep meaning *database* for every kind, which is precisely what is wrong today for this family.

- [ ] **Step 4: Teach the SHOW arm the prefix and the reversed grammar**

In the `case "SHOW":` arm, add `EXTENDED` ahead of `FULL` (ClickHouse's fixed order is `SHOW [EXTENDED] [FULL] …`; `TEMPORARY` keeps its existing position after `FULL`):

```go
		if i < len(toks) && isUnquotedDBKeyword(toks[i], "EXTENDED") {
			info.ShowExtended = true
			i++
		}
```

After `info.ShowWhat` is captured, branch on the kind before consuming clauses:

```go
		if isShowTableTargetKind(info.ShowWhat) {
			// ClickHouse: SHOW [EXTENDED] [FULL] COLUMNS {FROM|IN} <table> [{FROM|IN} <database>]
			// and the same shape for INDEX / INDEXES / KEYS. The FIRST clause is the
			// table; the database is either the optional SECOND clause or the
			// qualifier of a `<database>.<table>` first clause. Binding the table into
			// DB (today's behaviour) is what let this family address hg_safe.
			i = parseShowTableThenDatabase(e, sql, toks, i, &info)
		} else if i < len(toks) && (toks[i].TokenType == "FROM" || toks[i].TokenType == "IN") {
			... existing database-clause block, unchanged ...
		}
```

`isShowTableTargetKind` returns true for exactly `COLUMNS`, `INDEX`, `INDEXES`, `KEYS`. `parseShowTableThenDatabase` reuses `parsedIdentifierAt` for every name so identifier authority stays with the parser (`SHOW DICTIONARIES FROM 123db` is already pinned by `TestParseDBLevel_showDatabaseClauseUsesParserIdentifierAuthority`), and:

1. consumes the first `FROM|IN`, sets `HasTableClause = true`;
2. resolves the following name; if the next token is `.` and a second name follows, binds `DB` = first name, `DBResolved = true`, `HasDBClause = true`, `ShowTable` = second name, `ShowTableResolved = true`;
3. otherwise binds `ShowTable` / `ShowTableResolved` only;
4. if a second `FROM|IN` follows, sets `HasDBClause = true` and resolves `DB` / `DBResolved` from it — an already-set qualified `DB` is *overwritten* only if the second clause resolves, and the ambiguous `SHOW COLUMNS FROM a.b FROM c` form binds `DB = c` (ClickHouse's own precedence: the explicit database clause wins);
5. returns the index at which the shared `LIKE` / `WHERE` / `LIMIT` tail loop resumes, so `SHOW COLUMNS FROM t FROM db LIKE 'a%'` keeps working.

- [ ] **Step 5: Run the two tests plus the whole engine package**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./internal/engine/ -run 'TestParseDBLevel' -v
```

Expected: all `TestParseDBLevel*` green, including the four pre-existing ones — in particular `TestParseDBLevel_showPrefixesPrecedeKindAndDatabaseClause`'s `SHOW TEMPORARY FULL DICTIONARIES FROM hg_safe` → `ShowWhat: "FULL"` row, which proves prefix ordering is still strict and that `EXTENDED` did not become order-free.

- [ ] **Step 6: Commit**

```bash
git add internal/engine/dblevel.go internal/engine/dblevel_test.go
git commit -m "fix(show): parse the COLUMNS/INDEX family's table-then-database grammar (Spec N D2)"
```

**Verification command:** `env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/engine/ -run TestParseDBLevel -v`

### Task 2: D2 handler — positive classification, the target-bearing gate, and a fail-closed unknown kind

`dispatchShowTables` (`internal/handlers/dblevel.go:255-278`) branches on `info.ShowWhat != "TABLES"` and treats everything except `DICTIONARIES` as target-less. Measured today with SI configured: `SHOW COLUMNS FROM db1__t FROM hg_safe`, `SHOW COLUMNS FROM hg_safe.db1__t`, `SHOW INDEX FROM hg_safe.db1__t`, `SHOW KEYS FROM db1__t FROM hg_safe`, `SHOW EXTENDED COLUMNS FROM db1__t FROM hg_safe`, `SHOW COLUMNS FROM db1__t` (with `upstream_physical_database_in_context = hg_safe`) and `SHOW SOMETHINGNEW FROM hg_safe` are **all `Success` with the SQL unchanged**.

**Files:**
- Modify: `internal/handlers/dblevel.go`
- Test: `internal/handlers/dblevel_test.go`
- Modify: `internal/harness/testdata/storage_integrity_cases.json` (new cases appended at the end of the array)

**Interfaces:**
- Consumes: `engine.DBLevelInfo.ShowTable` / `HasTableClause` / `ShowExtended` (Task 1), `recordAccessedDatabase` (`dblevel.go:90`), `recordAccessedStorageIntegrityLogicalDatabaseUnique` (`storage_integrity_policy.go:203`), `nameresolve.IsStorageIntegrityPhysicalDatabase` / `IsStorageIntegrityLogicalDatabase` / `AuthorizeStorageIntegrityLogical` / `LookupStorageIntegrity`, `StorageIntegrityPhysicalDatabaseRejectMessage`, `StorageIntegrityPhysicalRejectMessage`, `StorageIntegrityLogicalDatabaseRejectMessage`, `StorageIntegrityUnauthorizedMessage`, `rejectUnresolvedShowDatabase` (`dblevel.go:394`).
- Produces: `showKindClass(kind string) showClass` with the three constants `showRewritten`, `showTargetBearing`, `showTargetLess`, plus `showUnknown`. No new message string is invented.

- [ ] **Step 1: Add the corpus cases (red)**

Append to the end of the JSON array in `internal/harness/testdata/storage_integrity_cases.json`. The `dynamic` block below is written once here and repeated verbatim per case (the corpus has no anchors); `<DYN>` in the table means exactly this object:

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
    }
```

| name | sql | dynamic | want_code | want_stmt | reject | want_message_contains | want_accessed |
|---|---|---|---|---|---|---|---|
| `si_show_columns_two_from_safe_rejected` | `SHOW COLUMNS FROM db1__t FROM hg_safe` | `<DYN>` | `UnsupportedStatement` | `""` | true | `storage-integrity physical table hg_safe.db1__t is not directly addressable` | `[{original_database: hg_safe, original_table: db1__t, physical_database: hg_safe, is_storage_integrity: true}]` |
| `si_show_columns_qualified_safe_rejected` | `SHOW COLUMNS FROM hg_safe.db1__t` | `<DYN>` | `UnsupportedStatement` | `""` | true | same | same |
| `si_show_extended_columns_unsafe_rejected` | `SHOW EXTENDED COLUMNS FROM db1__t FROM hg_unsafe` | `<DYN>` | `UnsupportedStatement` | `""` | true | `storage-integrity physical table hg_unsafe.db1__t is not directly addressable` | `hg_unsafe` variant |
| `si_show_full_columns_in_unsafe_rejected` | `SHOW FULL COLUMNS FROM db1__t IN hg_unsafe` | `<DYN>` | `UnsupportedStatement` | `""` | true | same as previous | same |
| `si_show_index_qualified_safe_rejected` | `SHOW INDEX FROM hg_safe.db1__t` | `<DYN>` | `UnsupportedStatement` | `""` | true | `storage-integrity physical table hg_safe.db1__t is not directly addressable` | `hg_safe` variant |
| `si_show_indexes_two_from_safe_rejected` | `SHOW INDEXES FROM db1__t FROM hg_safe` | `<DYN>` | `UnsupportedStatement` | `""` | true | same | same |
| `si_show_keys_two_from_safe_rejected` | `SHOW KEYS FROM db1__t FROM hg_safe` | `<DYN>` | `UnsupportedStatement` | `""` | true | same | same |
| `si_show_columns_context_physical_rejected` | `SHOW COLUMNS FROM db1__t` | `<DYN>` + `"upstream_physical_database_in_context": "hg_safe"` | `UnsupportedStatement` | `""` | true | `storage-integrity physical database hg_safe is not directly addressable` | `[{original_database: hg_safe, physical_database: hg_safe, is_storage_integrity: true}]` |
| `si_show_columns_si_logical_database_rejected` | `SHOW COLUMNS FROM t FROM db1` | `<DYN>` | `UnsupportedStatement` | `""` | true | `storage-integrity logical database db1 is not directly addressable` | `[{original_database: db1, logical_database: db1, physical_database: phys, is_storage_integrity: true}]` |
| `si_show_columns_unresolved_database_rejected` | `SHOW COLUMNS FROM db1__t FROM {db:Identifier}` | `<DYN>` | `UnsupportedStatement` | `""` | true | `storage-integrity SHOW COLUMNS database is not statically resolvable` | — |

Plus the two pass-through controls, which are **success** cases and therefore pin `want_sql` exactly:

| name | sql | want_code | want_stmt | want_sql |
|---|---|---|---|---|
| `si_show_columns_ordinary_database_passthrough` | `SHOW COLUMNS FROM u FROM other` | `Success` | `SHOW_TABLES` | `SHOW COLUMNS FROM u FROM other` |
| `si_show_merges_target_less_passthrough` | `SHOW MERGES` | `Success` | `SHOW_TABLES` | `SHOW MERGES` |

`si_show_merges_target_less_passthrough` is the allowlist's own regression test: Spec N §2 deliberately keeps `MERGES` pass-through, and this case makes any later "tighten everything" edit a visible corpus change. Neither control may carry `want_sql_contains` — every candidate substring is already in the input SQL and `ValidateSICorpus` rule R4 rejects that as vacuous.

- [ ] **Step 2: Run the new cases and record the pre-fix failure**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./internal/harness/ -run 'TestStorageIntegrityGolden/si_show_' -v 2>&1 | tail -40
```

Expected **before** the fix, measured: every one of the ten reject cases fails with `code = Success, want UnsupportedStatement` and `statement_type = STATEMENT_TYPE_SHOW_TABLES, want ""`. The two pass-through controls **pass** before and after — they are the proof that the gate is targeted rather than a blanket refusal. `TestSICorpusIsBytePinned` will also fail; that is expected and is re-pinned in Task 5, not now.

- [ ] **Step 3: Replace the negative branch with the positive classification**

In `internal/handlers/dblevel.go`, add above `dispatchShowTables`:

```go
type showClass int

const (
	showUnknown showClass = iota // not in any list → fall through to the D1 catch-all under SI
	showRewritten                // TABLES: the synthetic system.tables enumeration
	showTargetBearing            // names a database and/or a table: gate before passing through
	showTargetLess               // names no database or table (Spec N §2 allowlist)
)

// showKindClass classifies a SHOW kind positively. Spec N D2: the previous
// rule was negative ("anything that is not TABLES has no target"), so every
// SHOW variant ClickHouse adds landed in the target-less bucket by default and
// SHOW COLUMNS / INDEX / INDEXES / KEYS reached ClickHouse addressing the
// protocol-owned namespaces. The target-less list is an explicit allowlist:
// adding to it requires proving in review that the kind names no database and
// no table. An unrecognized kind is showUnknown, which fails closed under an
// active storage-integrity contract.
func showKindClass(kind string) showClass {
	switch kind {
	case "TABLES":
		return showRewritten
	case "DICTIONARIES", "COLUMNS", "INDEX", "INDEXES", "KEYS":
		return showTargetBearing
	case "CLUSTER", "CLUSTERS", "SETTINGS", "MERGES", "CACHES", "PROCESSLIST",
		"FUNCTIONS", "GRANTS", "USERS", "ROLES", "ROW", "QUOTA", "QUOTAS",
		"PROFILES", "POLICIES", "ACCESS", "ENGINES", "FILESYSTEM":
		return showTargetLess
	default:
		return showUnknown
	}
}
```

Then replace the `if info.ShowWhat != "TABLES" { ... }` block with:

```go
	switch showKindClass(info.ShowWhat) {
	case showTargetBearing:
		if rejectShowTargetStorageIntegrityNamespace(resp, sql, info, dyn) {
			return resp, true, nil
		}
		if info.ShowWhat == "DICTIONARIES" && (info.ShowFull || info.ShowTemporary) {
			return passthroughOriginalDB(sql, resp)
		}
		if info.HasTableClause {
			// ASTShowColumnsQuery / ASTShowIndexesQuery formatting is not
			// round-tripped by passthroughDB's Generate; echo the original text
			// once the namespace is proved ordinary, exactly as SHOW FULL
			// DICTIONARIES already does.
			return passthroughOriginalDB(sql, resp)
		}
		return passthroughDB(e, ast, sql, resp)
	case showTargetLess:
		return passthroughDB(e, ast, sql, resp)
	case showUnknown:
		if len(dyn.GetStorageIntegrity().GetTables()) > 0 {
			// Fall through unhandled: native.go's pass-through tail is the Spec I
			// D1 catch-all and answers with the generic unmodelled-statement
			// refusal. Re-stating that message here would duplicate the single
			// source of it.
			return nil, false, nil
		}
		return passthroughDB(e, ast, sql, resp)
	}
	// showRewritten falls through to the existing SHOW TABLES body.
```

`rejectShowTargetStorageIntegrityNamespace` is `rejectShowDictionariesStorageIntegrityNamespace` renamed and generalized — same body, same order (contextual physical database first, then the explicit target, then the SI logical check with its authorization branch), with three additions:

1. it uses `info.ShowWhat` in `rejectUnresolvedShowDatabase`, which it already does, so the message reads `storage-integrity SHOW COLUMNS database is not statically resolvable` without any new string;
2. when `info.HasTableClause && info.ShowTableResolved`, after the database is resolved it also checks the **pair**: a resolved physical database plus a table rejects with `nameresolve.StorageIntegrityPhysicalRejectMessage(db + "." + table)` and records the accessed table (not just the database) so HouseGate's SI-flag path sees the object;
3. when the database is absent and the table is unqualified, it keeps the existing `upstream_physical_database_in_context` check verbatim (`dblevel.go:338-347`) — that is the `si_show_columns_context_physical_rejected` path.

Keep the existing call site's name in the `DICTIONARIES` path so the diff stays reviewable: `DICTIONARIES` has no table clause, so branch 2 is inert for it and its four existing corpus cases must not move.

- [ ] **Step 4: Add the handler-local unknown-kind test**

Per deviation **D-4** the shared corpus may not be able to hold this case. Add it to `internal/handlers/dblevel_test.go` regardless, so the Go behaviour is pinned independently of what Part B measures:

```go
func TestDispatchShowTables_UnknownKindFallsThroughUnderStorageIntegrity(t *testing.T) {
	// A SHOW kind in none of the three lists must not be assumed target-less.
	// Under an active SI contract dispatchShowTables declines to handle it, so
	// doRewrite's pass-through tail answers with the Spec I D1 refusal.
	e := newEngine(t)
	resp, err := doRewriteForTest(e, "SHOW SOMETHINGNEW FROM hg_safe", siOptionsForTest())
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if resp.GetCode() != pb.RewriteCode_UnsupportedStatement {
		t.Fatalf("code = %v (%s), want UnsupportedStatement", resp.GetCode(), resp.GetMessage())
	}
	if !strings.Contains(resp.GetMessage(), "statement class is not modelled by the rewriter") {
		t.Fatalf("message = %q, want the D1 generic refusal", resp.GetMessage())
	}
}

func TestDispatchShowTables_UnknownKindStillPassesThroughWithoutStorageIntegrity(t *testing.T) {
	e := newEngine(t)
	resp, err := doRewriteForTest(e, "SHOW SOMETHINGNEW FROM other", ordinaryOptionsForTest())
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if resp.GetCode() != pb.RewriteCode_Success {
		t.Fatalf("code = %v, want Success — an empty-SI request keeps the legacy pass-through", resp.GetCode())
	}
}
```

Reuse the package's existing engine/option helpers rather than adding new ones — `internal/handlers` already has `newEngine` (`select_test.go:11`); if no `doRewriteForTest` equivalent exists in that package, drive `RewriteDBLevel` directly and assert `handled == false` for the SI case and `handled == true, Code == Success` for the non-SI case. That is the same property one call earlier and needs no new plumbing.

Expected **before** Step 3: the SI case fails with `code = Success` (measured: `SHOW SOMETHINGNEW FROM hg_safe` returns `Success` with `statement_type SHOW_TABLES` and the SQL unchanged). The non-SI case passes before and after.

- [ ] **Step 5: Run the handler and harness tests**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./internal/handlers/ ./internal/harness/ -run 'ShowTables|TestStorageIntegrityGolden' -v 2>&1 | tail -40
```

Expected: every `si_show_*` case green — the twelve new ones and the seven pre-existing ones (`si_show_create_rejected`, `si_show_tables_unchanged`, `si_show_tables_safe_database_rejected`, `si_show_tables_unsafe_context_rejected`, `si_show_dictionaries_safe_database_rejected`, `si_show_dictionaries_logical_database_rejected`, `si_show_dictionaries_ordinary_database_passthrough`). `TestSICorpusIsBytePinned` still fails until Task 5.

- [ ] **Step 6: Commit**

```bash
git add internal/handlers/dblevel.go internal/handlers/dblevel_test.go internal/harness/testdata/storage_integrity_cases.json
git commit -m "fix(storage-integrity): classify the SHOW family positively and gate every target (Spec N D2)"
```

**Verification command:** `env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/handlers/ ./internal/harness/ -run 'ShowTables|TestStorageIntegrityGolden' -v`

### Task 3: D4 — foreign-connector table functions enter the namespace decoder

`decodeNamespaceFunctionRefDetail` (`internal/engine/nodes.go:269-313`) covers the local and cluster families but not the connector family. Measured today with SI configured, all `Success` with the SQL unchanged: `mysql('127.0.0.1:9004', 'hg_safe', 'db1__t', 'u', 'p')`, `postgresql('127.0.0.1:9005', 'hg_safe', 'db1__t', 'u', 'p')`, `mongodb('127.0.0.1:27017', 'hg_safe', 'db1__t', 'u', 'p', 'a String')`, `jdbc('jdbc:mysql://h/db', 'hg_safe', 'db1__t')`, `odbc('DSN=x', 'hg_safe', 'db1__t')`.

Read deviation **D-2** and **D-3** before starting: `redis` and `sqlite` are deliberately **excluded**, and the decode is arity-dependent.

**Files:**
- Modify: `internal/engine/nodes.go` (`decodeNamespaceFunctionRefDetail`)
- Test: `internal/engine/nodes_test.go`
- Modify: `internal/harness/testdata/storage_integrity_cases.json`

**Interfaces:**
- Consumes: `decodeNamespacePairDetail` / `decodeNamespaceSingleDetail` (unchanged), `NamespaceRefTableFunction`.
- Produces: no new exported symbol. `objectCarrierCallable` in housegate's `sireserved` (`pkg/plugins/sireserved/plugin.go:240-252`) mirrors this list for operator sessions; Task 13 Step 6 adds the same five names there so the two lists do not drift.

- [ ] **Step 1: Read each signature from the ClickHouse documentation and write the arity table down**

Do not infer argument positions from the existing `remote` decode. Fetch each page and record the positional list in the commit message:

```
https://clickhouse.com/docs/sql-reference/table-functions/mysql
https://clickhouse.com/docs/sql-reference/table-functions/postgresql
https://clickhouse.com/docs/sql-reference/table-functions/mongodb
https://clickhouse.com/docs/sql-reference/table-functions/jdbc
https://clickhouse.com/docs/sql-reference/table-functions/odbc
https://clickhouse.com/docs/sql-reference/table-functions/sqlite
https://clickhouse.com/docs/sql-reference/table-functions/redis
```

Documented signatures as read on 2026-08-25 — re-read them rather than trusting this table, and if a signature has changed, change the decode and say so in the commit:

| function | documented positional signature | decode |
|---|---|---|
| `mysql` | `(host:port, database, table, user, password[, replace_query, on_duplicate_clause])`; also `(named_collection[, k=v…])` | pair at index 1 |
| `postgresql` | `(host:port, database, table, user, password[, schema[, on_conflict]])`; also named collection | pair at index 1 |
| `mongodb` | `(host:port, database, collection, user, password, structure[, options[, oid_columns]])` **or** `(uri, collection, structure[, oid_columns])` | ≥6 args → pair at index 1; 3–5 args → single at index 1 |
| `jdbc` | `(datasource, external_database, external_table)` **or** `(datasource, external_table)` | 3 args → pair at index 1; otherwise single at index 1 |
| `odbc` | `(datasource, external_database, external_table)` **or** `(datasource, external_table)` | 3 args → pair at index 1; otherwise single at index 1 |
| `sqlite` | `(db_path, table_name)` — a table inside a SQLite **file** | **not decoded** (deviation D-2) |
| `redis` | `(host:port, key, structure[, db_index, password, pool_size])` — `key` is a **column** name | **not decoded** (deviation D-2) |

The single-at-index-1 fallback is `merge`'s existing one-argument semantics: a qualified `a.b` literal binds the pair, an unqualified one binds a table against the current database, and a non-literal is unresolved. That is the safe direction for a short form whose foreign namespace cannot be proved ordinary.

- [ ] **Step 2: Add the corpus cases (red)**

Reuse the `<DYN>` block from Task 2 Step 1. Five reject cases, one per decoded name:

| name | sql | want_code | want_message_contains |
|---|---|---|---|
| `si_mysql_physical_function_rejected` | `SELECT * FROM mysql('127.0.0.1:9004', 'hg_safe', 'db1__t', 'u', 'p')` | `RewriteError` | `storage-integrity physical table hg_safe.db1__t is not directly addressable` |
| `si_postgresql_physical_function_rejected` | `SELECT * FROM postgresql('127.0.0.1:9005', 'hg_unsafe', 'db1__t', 'u', 'p')` | `RewriteError` | `storage-integrity physical table hg_unsafe.db1__t is not directly addressable` |
| `si_mongodb_physical_function_rejected` | `SELECT * FROM mongodb('127.0.0.1:27017', 'hg_safe', 'db1__t', 'u', 'p', 'a String')` | `RewriteError` | `storage-integrity physical table hg_safe.db1__t is not directly addressable` |
| `si_jdbc_physical_function_rejected` | `SELECT * FROM jdbc('jdbc:clickhouse://127.0.0.1:8123', 'hg_safe', 'db1__t')` | `RewriteError` | `storage-integrity physical table hg_safe.db1__t is not directly addressable` |
| `si_odbc_physical_function_rejected` | `SELECT * FROM odbc('DSN=ch', 'hg_unsafe', 'db1__t')` | `RewriteError` | `storage-integrity physical table hg_unsafe.db1__t is not directly addressable` |

Plus the unresolvable-argument case (the `si_remote_unresolved_namespace_rejected` precedent) and three controls:

| name | sql | want_code | want_stmt | assertion |
|---|---|---|---|---|
| `si_mysql_unresolved_namespace_rejected` | `SELECT * FROM mysql('h', concat('hg_', 'safe'), 'db1__t', 'u', 'p')` | `RewriteError` | `""` | `want_message_contains: storage-integrity table function namespace is not statically resolvable` |
| `si_mysql_ordinary_database_allowed` | `SELECT * FROM mysql('127.0.0.1:9004', 'other', 'u', 'x', 'y')` | `Success` | `SELECT` | `want_sql` pinned to the engine's exact output |
| `si_sqlite_foreign_file_allowed` | `SELECT * FROM sqlite('/tmp/x.db', 'db1__t')` | `Success` | `SELECT` | `want_sql` pinned; this is deviation D-2's regression test — it must stay `Success` |
| `si_redis_column_name_allowed` | `SELECT * FROM redis('127.0.0.1:6379', 'hg_safe', 'k String')` | `Success` | `SELECT` | `want_sql` pinned; `hg_safe` here is a **column** name and gating it would be a false positive |

The exact `want_sql` for the three `Success` cases is whatever the engine emits — take it from the failing test output in Step 3, do not hand-write it. `want_accessed` on the five reject cases mirrors `si_remote_physical_function_rid_rejected`'s shape; copy that case's array and change the database/table.

`si_redis_column_name_allowed` is the case that makes D-2 permanent: it is the one statement whose *plain reading* looks like a reserved-name mention and is nonetheless correct to allow, so it cannot be "tidied up" later without an explicit corpus change.

- [ ] **Step 3: Run the new cases and record the pre-fix failure**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./internal/harness/ -run 'TestStorageIntegrityGolden/si_(mysql|postgresql|mongodb|jdbc|odbc|sqlite|redis)' -v 2>&1 | tail -40
```

Expected **before** the fix, measured: the five physical cases and `si_mysql_unresolved_namespace_rejected` all fail with `code = Success, want RewriteError` and `statement_type = STATEMENT_TYPE_SELECT, want ""`. `si_mysql_ordinary_database_allowed`, `si_sqlite_foreign_file_allowed` and `si_redis_column_name_allowed` **pass** before and after (once their `want_sql` is filled in from this run) — three of the four controls in this task are pre-existing behaviour that must survive.

- [ ] **Step 4: Extend the decoder**

In `decodeNamespaceFunctionRefDetail`, after the `mergetree` prefix branch and before the final `return namespaceRefDetail{}, false`:

```go
	// Foreign-connector table functions whose signature carries a
	// (database|schema, table) pair reachable by a ClickHouse loopback:
	// ClickHouse ships its own MySQL (9004) and PostgreSQL (9005) wire
	// listeners, and a JDBC/ODBC datasource can point back at ClickHouse
	// itself (Spec N D4). sqlite() and redis() are deliberately absent:
	// sqlite's second argument is a table inside a SQLite file and redis's
	// is a COLUMN name, so neither names a ClickHouse namespace and gating
	// them would only manufacture false positives (plan deviation D-2).
	switch lower {
	case "mysql", "postgresql":
		return decodeNamespacePairDetail(NamespaceRefTableFunction, name, args, 1), true
	case "mongodb":
		// (host:port, database, collection, user, password, structure, ...) vs
		// the URI form (uri, collection, structure, ...), which names no
		// ClickHouse database.
		if len(args) >= 6 {
			return decodeNamespacePairDetail(NamespaceRefTableFunction, name, args, 1), true
		}
		return decodeNamespaceSingleDetail(NamespaceRefTableFunction, name, argAt(args, 1)), true
	case "jdbc", "odbc":
		if len(args) == 3 {
			return decodeNamespacePairDetail(NamespaceRefTableFunction, name, args, 1), true
		}
		return decodeNamespaceSingleDetail(NamespaceRefTableFunction, name, argAt(args, 1)), true
	}
```

`argAt(args, 1)` returns `nil` when the slice is shorter, which `decodeNamespaceSingleDetail` already handles as "not a current-database arg, not a resolvable value" — the unresolved path. If no such helper exists in the file, add a two-line local one rather than indexing unguarded.

- [ ] **Step 5: Pin the named-collection shape by measurement, not by assumption**

Every one of the five functions also accepts `f(named_collection[, k=v…])`, where no argument is a static namespace literal. Run each through the fixed engine and record what it produces:

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
cat > /tmp/nc_probe.json <<'JSON'
[{"name":"nc_mysql","sql":"SELECT * FROM mysql(creds)","dynamic":{"database_map":{"db1":"phys"},"known_physical_databases":["phys"],"delim":"_","storage_integrity":{"tables":{"db1.t":{"safe_table":"hg_safe.db1__t","unsafe_table":"hg_unsafe.db1__t"}},"read_mode":"SAFE","reserved_row_id_column":"_hg_row_id"}},"want_code":"Success","want_sql":"@@PROBE@@"},
 {"name":"nc_mysql_kv","sql":"SELECT * FROM mysql(creds, database='hg_safe', table='db1__t')","dynamic":{"database_map":{"db1":"phys"},"known_physical_databases":["phys"],"delim":"_","storage_integrity":{"tables":{"db1.t":{"safe_table":"hg_safe.db1__t","unsafe_table":"hg_unsafe.db1__t"}},"read_mode":"SAFE","reserved_row_id_column":"_hg_row_id"}},"want_code":"Success","want_sql":"@@PROBE@@"}]
JSON
env -u REWRITER_ORACLE_ADDR SI_CORPUS_PATH=/tmp/nc_probe.json \
  POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./internal/harness/ -run 'TestStorageIntegrityGolden' -v 2>&1 | tail -20
```

`SI_CORPUS_PATH` (`internal/harness/sicorpus_test.go:19`) is the supported way to run a candidate corpus without touching the real one. The deliberately wrong `want_sql` makes the runner print the engine's actual code, message and SQL.

Decision rule, applied in this step and not deferred: if the named-collection shapes reject (unresolved namespace), add `si_mysql_named_collection_unresolved_rejected` to the corpus pinning that. If any of them returns `Success` while naming `hg_safe`, that is a residual bypass of the same class as this task and it must be closed here — extend the decode so a connector call whose namespace arguments are not static literals is unresolved — before the task is considered done. Do not proceed with a known-`Success` reserved-name shape.

- [ ] **Step 6: Run the harness and the whole engine package**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test
```

Expected: everything green except `TestSICorpusIsBytePinned` (re-pinned in Task 5). Pay particular attention to `internal/harness`'s non-SI suites (`select_golden`, `writes_golden`, `dblevel_golden`): a recognized namespace function is also a read-source carrier (`containsReadBearingNode`, `nodes.go:1878`), so adding names changes the read-source walk for **every** request, not only SI ones. A failure there is this task's regression, not a flake.

- [ ] **Step 7: Commit**

```bash
git add internal/engine/nodes.go internal/engine/nodes_test.go internal/harness/testdata/storage_integrity_cases.json
git commit -m "fix(storage-integrity): gate foreign-connector table functions (Spec N D4)"
```

**Verification command:** `env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/harness/ -run 'TestStorageIntegrityGolden' -v`

### Task 4: the tagged-heredoc namespace bypass (plan deviation D-1, not in Spec N)

**This task closes a Critical hole Spec N does not describe.** Polyglot encodes a heredoc literal as `literal_type: "dollar_string"`, whose `value` is the body for the bare `$$…$$` form and **`"<tag>\x00<body>"`** for the tagged `$tag$…$tag$` form. `tableFunctionArgValue` (`nodes.go:1905-1913`) reads `lit["value"]` without looking at `literal_type`, so the namespace policy sees `tag\x00hg_safe` and matches nothing — while `Generate` re-emits the argument as a plain `'hg_safe'` literal for ClickHouse to execute. The value policy inspects is not the value the engine emits.

Measured on the live v0.9.0 engine with SI configured:

```
RewriteError  SELECT * FROM merge('hg_safe', 'db1__t')            → physical table hg_safe.db1__t is not directly addressable
RewriteError  SELECT * FROM merge($$hg_safe$$, 'db1__t')          → same (bare heredoc decodes correctly)
Success       SELECT * FROM merge($tag$hg_safe$tag$, 'db1__t')    → forwarded as merge('hg_safe', 'db1__t')
Success       SELECT * FROM remote('h', $tag$hg_safe$tag$, 'db1__t') → forwarded as remote('h', 'hg_safe', 'db1__t')
```

No operator marker is needed; any authenticated user reaches it.

**Files:**
- Modify: `internal/engine/nodes.go` (`tableFunctionArgValue`)
- Test: `internal/engine/nodes_test.go`
- Modify: `internal/harness/testdata/storage_integrity_cases.json`

**Interfaces:**
- Produces: `decodeStringLiteralValue(lit map[string]any) (string, bool)` in `internal/engine` — the single place that turns a polyglot literal node into the semantic string a namespace argument denotes. `tableFunctionArgValue` is its only caller in this task.

- [ ] **Step 1: Enumerate the literal types polyglot emits in a namespace-argument position**

The fix must be a whitelist, not a special case for one type. Add a temporary probe (delete it before committing) that parses each shape and logs `literal_type` and `value`:

```
'hg_safe'                 $$hg_safe$$              $tag$hg_safe$tag$        $$$$ (empty body)
0x68675f73616665          e'hg_safe'               concat('hg_','safe')     hg_safe (bare identifier)
```

Run with `env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/engine/ -run <probe> -v` and record the observed `literal_type` set in the commit message. Known from this plan's own measurement: `'…'` → `string`, `$$…$$` → `dollar_string` with `value = body`, `$tag$…$tag$` → `dollar_string` with `value = "tag\x00body"`.

- [ ] **Step 2: Write the failing tests (red)**

Two layers, because the bug is a mismatch between two layers.

Engine unit test in `internal/engine/nodes_test.go`:

```go
// TestTableFunctionArgValue_DecodesHeredocLiterals pins that the value the
// namespace policy sees equals the value the generator emits. Polyglot encodes
// a tagged heredoc as "<tag>\x00<body>"; reading it raw made
// merge($t$hg_safe$t$, 'db1__t') invisible to the storage-integrity gate while
// Generate re-emitted it as merge('hg_safe', 'db1__t').
func TestTableFunctionArgValue_DecodesHeredocLiterals(t *testing.T) {
	e := newTestEngine(t)
	for _, tc := range []struct{ sql, want string }{
		{"SELECT * FROM merge('hg_safe', 'db1__t')", "hg_safe"},
		{"SELECT * FROM merge($$hg_safe$$, 'db1__t')", "hg_safe"},
		{"SELECT * FROM merge($tag$hg_safe$tag$, 'db1__t')", "hg_safe"},
		{"SELECT * FROM merge($x$hg_safe$x$, 'db1__t')", "hg_safe"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			refs := namespaceRefsForTest(t, e, tc.sql) // package-local helper; reuse the existing one
			if len(refs) != 1 || refs[0].Target.DB != tc.want {
				t.Fatalf("refs = %+v, want a single ref naming %q", refs, tc.want)
			}
		})
	}
}
```

Corpus cases, reusing Task 2's `<DYN>` block:

| name | sql | want_code | want_message_contains |
|---|---|---|---|
| `si_merge_tagged_heredoc_physical_rejected` | `SELECT * FROM merge($tag$hg_safe$tag$, 'db1__t')` | `RewriteError` | `storage-integrity physical table hg_safe.db1__t is not directly addressable` |
| `si_remote_tagged_heredoc_physical_rejected` | `SELECT * FROM remote('h', $tag$hg_unsafe$tag$, 'db1__t')` | `RewriteError` | `storage-integrity physical table hg_unsafe.db1__t is not directly addressable` |
| `si_merge_bare_heredoc_physical_rejected` | `SELECT * FROM merge($$hg_safe$$, 'db1__t')` | `RewriteError` | `storage-integrity physical table hg_safe.db1__t is not directly addressable` |

The third case is deliberately one that **already passes** — it is the regression guard for the bare form while the tagged form is being fixed.

- [ ] **Step 3: Run and record the pre-fix failure**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./internal/engine/ ./internal/harness/ \
  -run 'TestTableFunctionArgValue_DecodesHeredocLiterals|TestStorageIntegrityGolden/si_(merge|remote)_(tagged|bare)_heredoc' -v
```

Expected **before** the fix, measured: the two `$tag$` rows of the unit test fail with the DB read as `tag` or `tag\x00hg_safe` (whatever `exactFunctionQualified` makes of the NUL-bearing value); `si_merge_tagged_heredoc_physical_rejected` and `si_remote_tagged_heredoc_physical_rejected` fail with `code = Success, want RewriteError`; `si_merge_bare_heredoc_physical_rejected` **passes**. Anything else — in particular the bare form failing — means the probe in Step 1 was misread.

- [ ] **Step 4: Implement the whitelist decode**

In `internal/engine/nodes.go`:

```go
// decodeStringLiteralValue returns the semantic string a polyglot literal node
// denotes, and whether the node is a literal kind whose value is safe to use as
// a namespace name. ClickHouse heredocs arrive as literal_type "dollar_string";
// the tagged form $tag$body$tag$ encodes as "<tag>\x00<body>", so reading
// lit["value"] raw made the policy see a different string than Generate emits
// (plan deviation D-1). Any other literal type is NOT decoded: an unrecognized
// encoding must reach the caller as unresolvable so storage-integrity policy
// fails closed rather than matching on bytes it did not decode.
func decodeStringLiteralValue(lit map[string]any) (string, bool) {
	value, ok := lit["value"].(string)
	if !ok {
		return "", false
	}
	switch lit["literal_type"] {
	case "string":
		return value, true
	case "dollar_string":
		if nul := strings.IndexByte(value, 0); nul >= 0 {
			return value[nul+1:], true // strip the "<tag>\x00" prefix
		}
		return value, true
	default:
		return "", false
	}
}
```

and in `tableFunctionArgValue` replace

```go
	if lit, ok := m["literal"].(map[string]any); ok {
		value, ok := lit["value"].(string)
		return value, namespaceValueLiteral, ok && value != ""
	}
```

with a call to `decodeStringLiteralValue`, keeping the existing `value != ""` emptiness rule.

**The `default:` arm is a behaviour change for numeric and any other literal type in a namespace-argument position.** Before committing, confirm with the whole-repo run in Step 5 that no existing corpus or golden case relied on a numeric literal decoding to its digits there. If one does, add that literal type to the whitelist explicitly with a comment naming the case — never widen the default.

- [ ] **Step 5: Confirm no other policy reader has the same defect**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
grep -rn '\["literal"\]' --include='*.go' internal/ | grep -v _test
```

Expected hits: `internal/engine/lexical.go:992` (presence check only), `internal/engine/lexical.go:2836` (already guards `literal_type != "string"` — the fail-closed direction, leave it), `internal/engine/nodes.go:1910` (fixed in Step 4), `internal/engine/nodes.go:2497` (LIMIT, numeric). Any *new* reader of `lit["value"]` in a namespace or identifier position must route through `decodeStringLiteralValue`. Record the audited list in the commit message.

- [ ] **Step 6: Full Go run**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test
```

Expected: green except `TestSICorpusIsBytePinned`.

- [ ] **Step 7: Commit**

```bash
git add internal/engine/nodes.go internal/engine/nodes_test.go internal/harness/testdata/storage_integrity_cases.json
git commit -m "fix(storage-integrity): decode heredoc literals before the namespace gate"
```

**Verification command:** `env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/engine/ ./internal/harness/ -run 'HeredocLiterals|TestStorageIntegrityGolden' -v`

### Task 5: freeze the shared corpus and re-pin all four constants

**Files:**
- Modify: `internal/harness/sicorpus_test.go` (`SICorpusFingerprint`, `SICorpusBytes`, `SICorpusCases` at `:231-233`)

- [ ] **Step 1: Prove the corpus satisfies the frozen contract**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR go test ./internal/harness/ -run 'TestSICorpusContract|TestLoadSICorpus' -v
python3 -c "
import json,collections
c=json.load(open('internal/harness/testdata/storage_integrity_cases.json'))
n=[x['name'] for x in c]
dup=[k for k,v in collections.Counter(n).items() if v>1]
print('cases',len(c),'duplicates',dup)
"
```

Expected: `TestSICorpusContract` green (it needs no engine and no oracle), zero duplicate names, and a case count of 210 + however many were added in Tasks 2–4 (25 if every case above landed as written).

- [ ] **Step 2: Compute and record the new pins**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json | tee /tmp/si_corpus_sha.txt
wc -c < internal/harness/testdata/storage_integrity_cases.json
env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./internal/harness/ -run TestSICorpusIsBytePinned -v 2>&1 | tail -10
```

The failing `TestSICorpusIsBytePinned` prints the actual fingerprint, byte count and case count. Copy those three numbers into `sicorpus_test.go:231-233`. Do not compute the FNV-1a/64 fingerprint by hand — the test is the authority.

- [ ] **Step 3: Re-run and commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test
git add internal/harness/sicorpus_test.go
git commit -m "test(storage-integrity): re-pin the shared corpus after the Spec N additions"
```

Expected: **every** package `ok`, including `TestSICorpusIsBytePinned`. This is the first point in Part A where the whole repo is green; if it is not, stop — Part B copies this exact file.

**Verification command:** `env -u REWRITER_ORACLE_ADDR make test`

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

- [ ] **Task 5b (pre-flight, do once):** branch and prove the baseline builds.

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git checkout -b feat/si-lexical-namespace-closure
# rsync + rebuild as above, then:
ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && ctest --test-dir build --output-on-failure"
```

Expected: all tests pass on the unmodified tree. Record any failure now — it is the baseline.

### Task 6: copy the finalized corpus, re-pin `si_corpus.h`, and record the red set

**Files:**
- Modify: `tests/testdata/storage_integrity_cases.json` (replaced wholesale by `cp`), `tests/si_corpus.h`

- [ ] **Step 1: Copy byte-for-byte and prove equality**

```bash
cp /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json \
   /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json
cmp /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json \
    /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json && echo IDENTICAL
shasum -a 256 /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json
```

Expected: `IDENTICAL`, and the sha256 equals `/tmp/si_corpus_sha.txt` from Part A Task 5 Step 2. **Never hand-edit the C++ copy** — if a case needs changing, change the Go copy, re-run Part A Task 5, and re-copy.

- [ ] **Step 2: Mirror the three pins**

Copy the same three values Part A Task 5 wrote into `sicorpus_test.go:231-233` into `tests/si_corpus.h:48-50` (`kCorpusFingerprint`, `kCorpusBytes`, `kCorpusCases`). The FNV-1a/64 fingerprint is identical by construction — the two implementations hash the same bytes with the same constants — so a mismatch here means the copy is not byte-identical, not that the algorithm differs.

- [ ] **Step 3: Rebuild and record the red set**

```bash
# rsync + rebuild, then:
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='StorageIntegrityCorpus.*:SpecG/StorageIntegrityGolden.*' 2>&1 | tail -60"
```

Expected: `StorageIntegrityCorpus.SatisfiesTheFrozenContract` and `StorageIntegrityCorpus.IsBytePinnedToRewriterGo` green (the rules are mirrored, so a corpus the Go validator accepts the C++ validator accepts). **Record the failing case list — it is Part B's work queue**, and its *shape* is itself evidence:

- `si_show_columns_*`, `si_show_index*`, `si_show_keys_*`, `si_show_extended_columns_*`, `si_show_full_columns_*` are expected to be **already green** or to fail only on the message text: C++ has no `ASTShowColumnsQuery` handler, so these statements fall through `handleShowTablesQuery` to `rejectUnmodelledStorageIntegrityStatement` (`src/rewriter-server.cc:466`) and get the D1 generic refusal. Same code (`UnsupportedStatement`), different message. Task 8 converts that accidental refusal into a deliberate one with the corpus's message.
- `si_merge_tagged_heredoc_physical_rejected` and `si_remote_tagged_heredoc_physical_rejected` are expected **green from the start** — ClickHouse's own parser decodes heredocs into ordinary `ASTLiteral` strings, so the C++ side never had the Go engine's `dollar_string` defect. Confirm this rather than assume it: it is the concrete demonstration that the two engines diverged, and it is what Part C's differential exists to catch systematically.
- `si_mysql_*`, `si_postgresql_*`, `si_mongodb_*`, `si_jdbc_*`, `si_odbc_*` are expected to **fail** — the C++ `decodeNamespaceFunction` (`src/handlers/storage_integrity.cc:168-198`) has the same connector gap.
- `si_show_columns_ordinary_database_passthrough` and `si_show_merges_target_less_passthrough` are expected to fail on the C++ side today, because the former currently hits the unmodelled catch-all.

Write the observed list into the task's commit message. A red set materially different from the above means an assumption in this plan is wrong — investigate before writing code.

- [ ] **Step 4: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add tests/testdata/storage_integrity_cases.json tests/si_corpus.h
git commit -m "test(storage-integrity): import the Spec N corpus additions from rewriter-go"
```

**Verification command:** `ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='StorageIntegrityCorpus.*'"`

### Task 7: D2 in C++ — the SHOW COLUMNS / INDEX family becomes a deliberate gate

**Files:**
- Modify: `src/handlers/show_tables.cc`, `src/handlers/show_tables.h`, `src/rewriter-server.cc`
- Create (only if Step 3 shows the family cannot live in `show_tables.cc`): `src/handlers/show_columns.cc` + `.h`, and the matching `CMakeLists.txt` entry
- Modify: `tests/rewriter_test.cc` (engine-local assertions only, per deviation D-4)

**Interfaces:**
- Produces: `bool rewriter_handlers::handleShowColumnsQuery(DB::ASTPtr, const rewriter::RewriteSQLRequest *, rewriter::RewriteSQLResponse *)`, dispatched from `rewriter-server.cc` immediately after `handleShowTablesQuery` and **before** `handleExistsQuery`, mirroring the Go order (db-level before exists/show-create).
- Consumes: the existing `storageIntegrityPhysicalDatabaseRejectMessage`, `storageIntegrityPhysicalRejectMessage`, `isStorageIntegrityPhysicalDatabase`, `isStorageIntegrityLogicalDatabase`, `authorizeStorageIntegrityLogical`, `recordPhysicalStorageIntegrityAccess`, `recordStorageIntegrityAccess` (`src/handlers/storage_integrity.h`).

- [ ] **Step 1: Confirm the ClickHouse AST classes and their fields on the box**

The `clickHouse/` submodule is not checked out locally, so nothing below may be written from memory:

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc/clickHouse/src/Parsers && ls ASTShow*.h && \
   sed -n '1,60p' ASTShowColumnsQuery.h && sed -n '1,60p' ASTShowIndexesQuery.h"
```

Record the exact class names and the exact member names for the table target, the database target, and the `full` / `extended` flags. If either header does not exist under those names, find the real ones (`grep -rn 'class ASTShow' clickHouse/src/Parsers/`) and use them. Everything in Steps 3–4 is written against what this step printed.

- [ ] **Step 2: Prove the current behaviour is accidental, and decide D-4**

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_show_columns_two_from_safe_rejected:SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_show_somethingnew*' 2>&1 | tail -30"
```

Two things to record:

1. `si_show_columns_two_from_safe_rejected` — expected to fail on **message** only (`storage-integrity is configured; statement class is not modelled…` instead of `storage-integrity physical table hg_safe.db1__t is not directly addressable`) with the code already `UnsupportedStatement`. That is the "accidentally right" state Spec N describes; the fix makes it deliberate so the corpus can pin it.
2. **The deviation D-4 decision.** Feed `SHOW SOMETHINGNEW FROM hg_safe` through the C++ engine directly (a one-off gtest or the running server) and record its `code` and `message`. If it is `UnsupportedStatement` with the D1 generic message, add `si_show_unknown_kind_rejected` to the **shared corpus** (via the Go copy, then re-run Part A Task 5 and Part B Task 6 — the Go copy stays authoritative). If it is `SyntaxError`, leave the assertion engine-local: keep the Go-side `TestDispatchShowTables_UnknownKindFallsThroughUnderStorageIntegrity` from Part A Task 2 Step 4, add a C++ gtest asserting `SyntaxError`, and write the divergence and its reason into the Part C record. Do not add a case that would have to carry `allow_sql_divergence` for a *code* difference — the schema cannot express that.

- [ ] **Step 3: Implement the handler**

`handleShowColumnsQuery` casts to the class Step 1 named, and when the cast succeeds:

1. resolves the database exactly as `handleShowTablesQuery`'s dictionaries branch does — explicit target first, then `upstream_physical_database_in_context`, then `upstream_logical_database_in_context` — reusing that function's helper shape rather than a second copy of the logic; if the query has a syntactically explicit database target that does not reduce to a name (an `ASTQueryParameter`), reject with `storage-integrity SHOW COLUMNS database is not statically resolvable`, matching the existing `"storage-integrity " + family + " database is not statically resolvable"` construction at `show_tables.cc:57-63`;
2. rejects a reserved physical database with `storageIntegrityPhysicalDatabaseRejectMessage`, recording the access;
3. when the table target is also statically known, rejects the pair with `storageIntegrityPhysicalRejectMessage(db + "." + table)`, recording the accessed table — this is the message the corpus pins for the qualified and two-`FROM` forms;
4. rejects an SI logical database with the authorization branch (`authorizeStorageIntegrityLogical` first, then `storage-integrity logical database <db> is not directly addressable`), same order as `show_tables.cc:93-105`;
5. otherwise sets a `Success` response echoing `request->sql()` verbatim with `STATEMENT_TYPE_SHOW_TABLES` — **not** `formatAst(ast)`, for the same reason `SHOW FULL DICTIONARIES` already echoes (`show_tables.cc:114-120`): the formatter does not round-trip this family's prefixes.

Register it in `rewriter-server.cc` right after `handleShowTablesQuery`. Keep the `si_active` catch-all at `:466` exactly as it is — an unknown SHOW kind must keep reaching it.

- [ ] **Step 4: Rebuild and run the SHOW block**

```bash
# rsync + rebuild, then:
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_show_*' 2>&1 | tail -40"
```

Expected: all nineteen `si_show_*` cases green — the twelve added in Part A Task 2 and the seven pre-existing ones. In particular `si_show_columns_ordinary_database_passthrough` proves the new handler passes an ordinary target through with the SQL byte-identical, and `si_show_merges_target_less_passthrough` proves the allowlist did not shrink.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add src/ tests/
git commit -m "fix(storage-integrity): gate the SHOW COLUMNS/INDEX family deliberately (Spec N D2)"
```

**Verification command:** `ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_show_*'"`

### Task 8: D4 in C++ — the connector family, and the heredoc parity check

**Files:**
- Modify: `src/handlers/storage_integrity.cc` (`decodeNamespaceFunction` at `:168-198`)

- [ ] **Step 1: Extend `decodeNamespaceFunction` with the same five names and the same arity rules**

Mirror Part A Task 3 Step 4 exactly — same five names (`mysql`, `postgresql`, `mongodb`, `jdbc`, `odbc`), same exclusions (`sqlite`, `redis`), same arity branches, same `decodePair` / `decodeSingle` helpers (`storage_integrity.cc:119-165`). Carry the deviation D-2 comment across so a future reader of either engine finds the same reason in the same place.

- [ ] **Step 2: Confirm the C++ heredoc decode needs no change**

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='*heredoc*' 2>&1 | tail -20"
```

Expected: `si_merge_tagged_heredoc_physical_rejected`, `si_remote_tagged_heredoc_physical_rejected` and `si_merge_bare_heredoc_physical_rejected` all green **without any C++ change** — ClickHouse's parser materializes a heredoc into a plain `ASTLiteral`. If any of them is red, the C++ engine has its own variant of deviation D-1 and it must be fixed here with the same whitelist discipline (decode the literal kind, do not read raw bytes) before the task closes.

- [ ] **Step 3: Rebuild and run the connector block**

```bash
# rsync + rebuild, then:
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_*mysql*:*postgresql*:*mongodb*:*jdbc*:*odbc*:*sqlite*:*redis*' 2>&1 | tail -30"
```

Expected: five reject cases green, `si_mysql_unresolved_namespace_rejected` green, and the three allow-controls (`si_mysql_ordinary_database_allowed`, `si_sqlite_foreign_file_allowed`, `si_redis_column_name_allowed`) green with the SQL matching the Go pins. A `want_sql` mismatch on an allow-control is a real formatting divergence between the engines and is resolved in Part C, not by loosening the case.

- [ ] **Step 4: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add src/handlers/storage_integrity.cc
git commit -m "fix(storage-integrity): gate foreign-connector table functions (Spec N D4)"
```

**Verification command:** `ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.*'"`

### Task 9: full C++ suite green

**Files:** none (verification only).

- [ ] **Step 1: Whole suite**

```bash
# rsync + rebuild, then:
ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && ctest --test-dir build --output-on-failure"
```

Expected: 100% tests passed, 0 failed — identical to the Task 5b baseline plus the new behaviour.

- [ ] **Step 2: Re-prove byte identity after all the C++ work**

```bash
cmp /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json \
    /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json && echo IDENTICAL
diff <(shasum -a 256 /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json | cut -d' ' -f1) \
     <(cut -d' ' -f1 /tmp/si_corpus_sha.txt) && echo "MATCHES PART A CHECKSUM"
```

Expected: `IDENTICAL` and `MATCHES PART A CHECKSUM`. If the Go copy moved during Part B (it should not have), re-run Part A Task 5 and Part B Task 6 before continuing.

**Verification command:** `ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && ctest --test-dir build --output-on-failure"`

---

## Part C — the cross-engine differential (D3)

Spec N §1c's finding is that "the two halves are equal" has never been supported by an execution, only by a shared JSON file. This part is the execution. It is a **one-shot gate for this spec**, not a new CI job (rewriter-grpc builds only on the remote box; automating it is recorded as debt in Spec N §6).

### Task 10: run `REWRITER_ORACLE_ADDR` over the whole corpus and record the result

**Files:** none (verification and record-keeping).

**Preconditions:** Part A Task 5 green (`make test` all `ok`), Part B Task 9 green (`ctest` 100%), corpus byte-identical in both repos.

- [ ] **Step 1: Start the freshly built C++ engine as the oracle and tunnel to it**

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && nohup ./build/clickhousegate_rewriter 50051 50052 >/tmp/rewriter.log 2>&1 &"
ssh -p 30100 -f -N -L 50051:127.0.0.1:50051 sentio@64.38.131.242
ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && git rev-parse --short HEAD"
```

Record the printed commit as `<rewriter-grpc-commit>`. The binary must be the one Part B Task 9 built — if the box has an older build, rebuild before starting it, or the differential proves nothing.

- [ ] **Step 2: Run the full Go suite against the oracle**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
REWRITER_ORACLE_ADDR=127.0.0.1:50051 make test 2>&1 | tee /tmp/si_oracle_run.txt
grep -c 'oracle divergence' /tmp/si_oracle_run.txt
```

Expected: every package `ok`, and `grep -c` prints `0`. With the oracle set, `TestStorageIntegrityGolden` diffs every case's structured fields — code, statement type, message, table rewrites, accessed tables — and the SQL, except where `allow_sql_divergence` is set (`internal/harness/storage_integrity_golden_test.go:185-200`). The other oracle-aware suites (`dblevel_golden`, `select_golden`, `writes_golden`, `errmsg_golden`) run too and are part of the gate.

- [ ] **Step 3: Resolve every divergence in the engine that is wrong**

Each `oracle divergence:` line names one case and the mismatching fields. For each:

- Decide which engine is wrong against the spec and fix **that** engine. Never loosen a case to make a difference disappear, and never reach for `allow_sql_divergence` before proving the difference is cosmetic formatting rather than semantics.
- A legitimate formatting-only difference gets `allow_sql_divergence: true` plus both `want_sql_go` and `want_sql_cpp` (`ValidateSICorpus` rule R3 enforces both), **and a one-line written reason recorded in Step 5**. A divergence recorded without a reason is indistinguishable from a bug that was papered over.
- A fix in either engine means re-running Part A Task 5 (re-pin) and Part B Task 6 (re-copy, re-pin) before repeating Step 2. The differential is only meaningful over the final corpus.

Expected divergences to watch for specifically, from this plan's own measurements:

- the unknown-SHOW-kind case, if deviation D-4's decision in Part B Task 7 Step 2 came out `SyntaxError` — it must be absent from the shared corpus entirely, not present with a divergence flag;
- pass-through SQL formatting on `SHOW COLUMNS FROM u FROM other` and `SHOW MERGES`, since the Go side echoes original text and the C++ side may format the AST;
- `want_sql` on the three connector allow-controls.

- [ ] **Step 4: Tear down**

```bash
pkill -f 'ssh -p 30100 -f -N -L 50051' || true
ssh -p 30100 sentio@64.38.131.242 "pkill -f clickhousegate_rewriter || true"
```

- [ ] **Step 5: Record the run in the spec's delivery note**

Spec N D3 requires the run's date, the rewriter-grpc commit and the case count to be recorded. Append to §5 of `docs/superpowers/specs/2026-08-25-storage-integrity-lexical-namespace-closure-design.md` (this edit lands with Task 17, but the numbers are captured now):

```markdown
**Cross-engine differential (D3), executed <YYYY-MM-DD>:** `TestStorageIntegrityGolden` with `REWRITER_ORACLE_ADDR` against rewriter-grpc `<rewriter-grpc-commit>`, over all <N> corpus cases. Divergences: <none | the list, each with its written reason>. Corpus sha256 `<corpus-sha>`, byte-identical in both repos.
```

**Verification command:** `cd /Users/uranuswch/Dev/housegate/rewriter-go && REWRITER_ORACLE_ADDR=127.0.0.1:50051 make test`

---

## Part D — housegate (D1, onto PR #141's branch)

**Working directory for every Part D task:** `/Users/uranuswch/Dev/housegate/housegate`

**Branch:** `feature/si-surface-failclosed-housegate` — PR #141's own branch, **not** `main` and not the docs branch this plan lives on. Spec N §5: #141 must not merge without D1, because D1 closes a hole #141 itself introduces.

**Ordering note:** Task 12's heredoc assertions are engine-independent and run against the FFI tag CI already pins (`v0.9.0`). The `SHOW COLUMNS` integration assertion needs the **fixed** engine and therefore lands in Part E Task 15, in the same commit as the CI FFI pin bump. Do not add it here — it would red the branch's CI for the whole review window.

- [x] **Task 10b (pre-flight, do once):** check out the PR branch, prove the Bazel baseline, and run the pre-fix bypass reproduction.

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git fetch origin feature/si-surface-failclosed-housegate
git checkout -b si-lexical-closure origin/feature/si-surface-failclosed-housegate
bazel test //... --test_output=errors
cd /private/tmp/claude-501/-Users-uranuswch-Dev-housegate-housegate/82fd460d-8ca7-4afa-80f3-1690dc473789/scratchpad/heredoc-repro && go run .
```

Expected: all non-`manual` Bazel targets pass (record any failure — it is the baseline). The reproduction program — the PR branch's `scanSQLSurfaces` and every `consume*` helper extracted byte-identically, see its `FINDING.md` — prints **`6/8 statements passed the guard`**, with `NO VIOLATION` on the four heredoc bypasses, on `SELECT $$unterminated`, and on `SELECT 1 $ 2`, and `REFUSED (hg_safe)` on the two controls. **That 6/8 is this part's pre-fix baseline and Task 11 must flip it to 0/8.**

### Task 11: D1 — `sireserved` models ClickHouse heredocs and refuses an unmodelled `$`

`scanSQLSurfaces` (`pkg/plugins/sireserved/plugin.go:115-167`) models `'…'`, `` `…` ``, `"…"`, `--`, `#`, `#!`, `//` and nested `/* */`. A `$` falls to the byte-copying `default:` branch, so `--`, `#` or `//` **inside** a heredoc is treated as a real comment and `consumeLineComment` blanks the rest of the statement from **both** scan surfaces. `sireserved` is the only control on maintenance and platform-operator sessions: rewrite is skipped by design (Spec I D6) and the SI ingress short-circuits on an empty accessed-table set.

**Files:**
- Modify: `pkg/plugins/sireserved/plugin.go`
- Test: `pkg/plugins/sireserved/plugin_test.go`

**Interfaces:**
- Produces: `consumeHeredoc(sql string, start int) (next int, body string, err error)` in package `sireserved`, shaped exactly like the existing `consumeStringLiteral` (`plugin.go:169-188`) — same `(next, value, error)` signature, same "unterminated is an error" contract.
- No exported-API change: `ReservedNamespaceViolation` and `Plugin.OnQuery` keep their signatures.

- [x] **Step 1: Verify the escape claim against the real grammar before relying on it**

Spec N D1 asserts ClickHouse performs no escape processing inside a heredoc, and requires the implementation to confirm that rather than assume it. Measure it through the polyglot grammar the production engine uses:

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
cat > /tmp/heredoc_escape_probe.json <<'JSON'
[{"name":"heredoc_backslash","sql":"SELECT * FROM merge($$hg\\x5Fsafe$$, '^db1__t$')",
  "dynamic":{"database_map":{"db1":"phys"},"known_physical_databases":["phys"],"delim":"_",
  "storage_integrity":{"tables":{"db1.t":{"safe_table":"hg_safe.db1__t","unsafe_table":"hg_unsafe.db1__t"}},
  "read_mode":"SAFE","reserved_row_id_column":"_hg_row_id"}},
  "want_code":"Success","want_sql":"@@PROBE@@"}]
JSON
env -u REWRITER_ORACLE_ADDR SI_CORPUS_PATH=/tmp/heredoc_escape_probe.json \
  POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./internal/harness/ -run TestStorageIntegrityGolden -v 2>&1 | tail -10
```

Measured while writing this plan, on the live v0.9.0 engine: the case comes back **`Success`** with the SQL re-emitted as `SELECT * FROM merge('hg\\x5Fsafe', '^db1__t$')` — the backslash survived as two literal characters and the value is **not** `hg_safe`. The grammar therefore agrees with Spec N and heredoc bodies do **not** inherit `consumeStringLiteral`'s backslash refusal.

**If your run disagrees** — the case rejects with `physical table hg_safe.…`, meaning the grammar decoded `\x5F` into `_` — then implement the opposite: `consumeHeredoc` returns the same `backslash-bearing …` error shape `consumeStringLiteral` uses, and Step 3's `heredoc backslash is not an escape` test flips from "must pass" to "must be refused". Record which branch you took in the commit message. Do not implement the permissive branch on the strength of this plan's measurement alone.

- [x] **Step 2: Verify which heredoc tags the grammar accepts**

The guard's tag charset must be a **subset** of the grammar's. Too narrow only costs a false refusal; too wide means the guard treats real SQL as a heredoc body and blanks it from `outsideLiterals` — a new bypass. Probe each shape through the same harness and record which parse:

```
$$x$$      $tag$x$tag$      $_t$x$_t$      $t1$x$t1$      $1t$x$1t$      $t-1$x$t-1$
```

Adopt `[A-Za-z_][0-9A-Za-z_]*` (Spec N D1) only if every tag the grammar accepts matches it. If the grammar accepts a leading digit or a hyphen, narrow the guard's *opener* recognition accordingly and let the unmatched form fall to the bare-`$` refusal, which is the safe direction.

- [x] **Step 3: Write the failing tests (red)**

Append to `pkg/plugins/sireserved/plugin_test.go`. Every row states its pre-fix behaviour, because a row that already passes is not testing the fix.

```go
// TestReservedNamespaceViolation_HeredocCannotHideAComment is the 1a
// regression. ClickHouse's heredoc ($$...$$ / $tag$...$tag$) is a string
// literal, so a --, # or // inside one is content, not a comment. The shipped
// scanner copied `$` through its default branch, so consumeLineComment blanked
// the rest of the statement from both surfaces and the guard saw nothing.
func TestReservedNamespaceViolation_HeredocCannotHideAComment(t *testing.T) {
	const rid = "_hg_row_id"
	dbs := []string{"hg_safe", "hg_unsafe"}
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		// PRE-FIX: all four return "" (no violation). Reproduced at
		// scratchpad/heredoc-repro, 6/8 statements passed the guard.
		{"bare heredoc hiding a line comment", "SELECT $$--$$ AS x, count() FROM hg_safe.db1__t", "hg_safe"},
		{"bare heredoc hiding a hash comment", "SELECT $$#$$ AS x, _hg_row_id FROM hg_unsafe.db1__t", "hg_unsafe"},
		{"heredoc in an INSERT ... SELECT export", "INSERT INTO ordinary.t SELECT $$--$$, a FROM hg_safe.db1__t", "hg_safe"},
		{"tagged heredoc hiding a slash comment", "SELECT $tag$//$tag$ AS x, count() FROM hg_safe.db1__t", "hg_safe"},
		// PRE-FIX: already refused. Controls that must not regress.
		{"control: no heredoc", "SELECT count() FROM hg_safe.db1__t", "hg_safe"},
		{"control: reserved column", "SELECT _hg_row_id FROM db1.t", rid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReservedNamespaceViolation(tc.sql, dbs, rid)
			if err != nil {
				t.Fatalf("unexpected scan error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("ReservedNamespaceViolation(%q) = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

// TestReservedNamespaceViolation_HeredocBodyReachesTheLiteralSurface is the
// half that is easy to break while fixing the half above. ClickHouse table
// functions read literal arguments as identifiers, so a heredoc body must be
// blanked from outsideLiterals and written VERBATIM to withLiterals. These two
// statements are refused TODAY (the body carries no comment marker, so hg_safe
// survives as ordinary bytes); a fix that blanks heredoc bodies from both
// surfaces would turn a currently-caught statement into a bypass.
func TestReservedNamespaceViolation_HeredocBodyReachesTheLiteralSurface(t *testing.T) {
	for _, tc := range []struct{ name, sql, want string }{
		// PRE-FIX: already refused. MUST still be refused after the fix.
		{"merge database heredoc", "SELECT * FROM merge($$hg_safe$$, '^db1__t$')", "hg_safe"},
		// PRE-FIX: refused. Post-fix the tagged body must resolve to hg_safe too.
		{"tagged merge database heredoc", "SELECT * FROM merge($tag$hg_safe$tag$, '^db1__t$')", "hg_safe"},
		{"reserved column in a heredoc", "SELECT $$_hg_row_id$$", "_hg_row_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReservedNamespaceViolation(tc.sql, []string{"hg_safe", "hg_unsafe"}, "_hg_row_id")
			if err != nil || got != tc.want {
				t.Fatalf("heredoc body on the literal surface = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

// TestReservedNamespaceViolation_HeredocIsNotABlanketRefusal keeps the fix from
// degenerating into "any $ is an error": an ordinary heredoc naming nothing
// reserved must still pass, or every maintenance session that uses one breaks.
func TestReservedNamespaceViolation_HeredocIsNotABlanketRefusal(t *testing.T) {
	for _, sql := range []string{
		"SELECT $$ordinary$$ AS x FROM other.u",
		"SELECT $tag$ordinary$tag$ AS x FROM other.u",
		"SELECT $$hg_safe_backup$$ AS x FROM other.u",
		// No escape processing inside a heredoc (Step 1): this is the literal
		// text hg\x5Fsafe, which is NOT hg_safe. Flip this row to "must be
		// refused" only if Step 1's measurement came out the other way.
		`SELECT $$hg\x5Fsafe$$ AS x FROM other.u`,
	} {
		t.Run(sql, func(t *testing.T) {
			got, err := ReservedNamespaceViolation(sql, []string{"hg_safe", "hg_unsafe"}, "_hg_row_id")
			if err != nil {
				t.Fatalf("unexpected scan error: %v", err)
			}
			if got != "" {
				t.Fatalf("must not fire on %q, got %q", sql, got)
			}
		})
	}
}

// TestReservedNamespaceViolation_UnterminatedOrStrayDollarIsRefused pins the
// two remaining rows of the reproduction. An unterminated heredoc is an error
// like an unterminated literal or block comment; a `$` that opens nothing is
// refused, because outside a heredoc opener and a quoted span `$` is not part
// of any identifier or operator this guard needs to admit, and copying it
// through is exactly what produced the bypass (roadmap §4.1).
func TestReservedNamespaceViolation_UnterminatedOrStrayDollarIsRefused(t *testing.T) {
	for _, sql := range []string{
		// PRE-FIX: both return "" (no violation).
		"SELECT $$unterminated",
		"SELECT $tag$unterminated$tog$",
		"SELECT 1 $ 2",
		"SELECT $ FROM other.u",
		"SELECT $1 FROM other.u",
	} {
		t.Run(sql, func(t *testing.T) {
			if _, err := ReservedNamespaceViolation(sql, []string{"hg_safe"}, "_hg_row_id"); err == nil {
				t.Fatalf("unterminated or stray $ must be an error: %q", sql)
			}
		})
	}
}

// TestOnQuery_OperatorSessionRefusesHeredocHiddenReservedName drives the same
// statements through the plugin so the refusal is proved on the production
// path, including the error text an operator actually receives.
func TestOnQuery_OperatorSessionRefusesHeredocHiddenReservedName(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}
	for _, tc := range []struct{ name, sql, want string }{
		{"maintenance heredoc comment", "SELECT $$--$$ AS x, count() FROM hg_safe.db1__t", "hg_safe"},
		{"maintenance tagged heredoc comment", "SELECT $tag$//$tag$ AS x, count() FROM hg_safe.db1__t", "hg_safe"},
		{"maintenance heredoc export", "INSERT INTO ordinary.t SELECT $$--$$, a FROM hg_safe.db1__t", "hg_safe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSessionForTest(t, 30)
			sess.State().SetMaintenance(true)
			qctx := &plugin.QueryContext{Session: sess, OriginalSQL: tc.sql, Query: &chproto.Query{Body: tc.sql}}
			chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
			err := chain.OnQuery(context.Background(), qctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("heredoc-hidden reserved name must be refused with %q, err=%v", tc.want, err)
			}
		})
	}
}
```

- [x] **Step 4: Run the tests and record the exact pre-fix failure**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //pkg/plugins/sireserved:sireserved_test --test_output=all --test_arg=-test.v 2>&1 | tail -60
```

Expected **before** the fix, matching the reproduction program row for row:

| test | pre-fix result |
|---|---|
| `HeredocCannotHideAComment/bare heredoc hiding a line comment` | FAIL — `= "", want "hg_safe"` |
| `.../bare heredoc hiding a hash comment` | FAIL — `= "", want "hg_unsafe"` |
| `.../heredoc in an INSERT ... SELECT export` | FAIL — `= "", want "hg_safe"` |
| `.../tagged heredoc hiding a slash comment` | FAIL — `= "", want "hg_safe"` |
| `.../control: no heredoc`, `.../control: reserved column` | PASS (before and after) |
| `HeredocBodyReachesTheLiteralSurface/merge database heredoc` | **PASS before and after** — the live regression risk |
| `.../tagged merge database heredoc`, `.../reserved column in a heredoc` | PASS before and after |
| `HeredocIsNotABlanketRefusal/*` | PASS before; must still PASS after |
| `UnterminatedOrStrayDollarIsRefused/*` | FAIL — no error returned |
| `OnQuery_OperatorSessionRefusesHeredocHiddenReservedName/*` | FAIL — `err=<nil>` |

If `HeredocBodyReachesTheLiteralSurface/merge database heredoc` fails **before** the fix, stop: the branch is not the one this plan measured.

- [x] **Step 5: Implement the heredoc case**

In `scanSQLSurfaces`, insert ahead of `default:`:

```go
		case sql[i] == '$':
			// ClickHouse heredoc: $$body$$ or $tag$body$tag$. The body is a
			// string literal, so it is blanked from outsideLiterals and written
			// verbatim to withLiterals — table functions read literal arguments
			// as identifiers, so merge($$hg_safe$$, ...) must still be caught.
			// A `$` that opens no well-formed heredoc is refused: outside a
			// heredoc opener and a quoted span it is not part of any identifier
			// or operator this guard needs to admit, and copying it through is
			// what let a comment marker inside a heredoc blank the statement.
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, body, err := consumeHeredoc(sql, i)
			if err != nil {
				return sqlSurfaces{}, err
			}
			withLiterals.WriteString(body)
			withLiterals.WriteByte(' ')
			i = next
```

`consumeHeredoc` reads the opener `$<tag>$` with `tag` matching the charset Step 2 confirmed (empty tag allowed), scans for the next identical `$<tag>$`, and returns the body between them. Failure modes, all errors:

- no closing `$` for the opener tag, or a tag character outside the charset → `fmt.Errorf("stray $ is not a heredoc opener and is not accepted by the storage-integrity guard")`;
- opener well-formed but the closing `$<tag>$` never appears → `fmt.Errorf("unterminated heredoc string literal")`.

Do **not** add a backslash refusal unless Step 1 measured escape processing. Do **not** trim, unescape or case-fold the body — `reservedNamespaceViolationOnSurface` already tokenizes `withLiterals` on identifier boundaries and compares case-insensitively.

- [x] **Step 6: Mirror the connector names into `objectCarrierCallable`**

`isObjectCarrierName` (`plugin.go:240-252`) mirrors rewriter-go's namespace-reference authority. Part A Task 3 added five names there, so add the same five here — `mysql`, `postgresql`, `mongodb`, `jdbc`, `odbc` — and **not** `sqlite` or `redis` (deviation D-2). Add a test row per name to the existing `TestOnQuery_OperatorBypassRefusesObjectCarrierCallables` table, e.g. `{"mysql computed target", "SELECT * FROM mysql('h', concat('hg_', 'safe'), 'db1__t', 'u', 'p')", "mysql"}`. Expected pre-fix: each new row FAILS with `err=<nil>`. Add one row to `TestOnQuery_ObjectCarrierScanAvoidsNonCallableFalsePositives` (`"SELECT mysql, jdbc FROM ordinary.t"`) that passes before and after.

- [x] **Step 7: Re-run the reproduction program and the unit test**

```bash
cd /private/tmp/claude-501/-Users-uranuswch-Dev-housegate-housegate/82fd460d-8ca7-4afa-80f3-1690dc473789/scratchpad/heredoc-repro
# replace scanner.go's scanSQLSurfaces + consume* helpers with the fixed ones,
# byte-for-byte from pkg/plugins/sireserved/plugin.go, then:
go run .
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //pkg/plugins/sireserved:sireserved_test --test_output=errors
```

Expected: the program prints **`0/8 statements passed the guard`** — all eight lines `REFUSED` or `error:` — and the Bazel target is green. The 6/8 → 0/8 flip against an independently extracted copy of the scanner is this task's acceptance evidence; put both numbers in the commit message.

- [x] **Step 8: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/plugins/sireserved/plugin.go pkg/plugins/sireserved/plugin_test.go
git commit -m "fix(storage-integrity): model ClickHouse heredocs in the operator guard (Spec N D1)"
```

**Verification command:** `bazel test //pkg/plugins/sireserved:sireserved_test --test_output=errors`

### Task 12: the heredoc bypass, end to end against real ClickHouse

Spec N §4.6 asks for the heredoc statements in `pkg/integration/storage_integrity_read_test.go` "proving the rejection reaches the client as an exception rather than as a result set". Read deviation **D-5** first: `sireserved.OnQuery` returns immediately unless the session carries the maintenance or platform-operator flag, and that flag is set only by `authplugin` after a JWS verify whose signer equals the host-injected `Options.Signer.Address()`. `testenv.StartServerProxy` never sets `opts.Signer`, so the session has to be built explicitly. Everything needed already exists.

**Files:**
- Modify: `pkg/integration/storage_integrity_read_test.go`

**Interfaces:**
- Consumes: `auth.NewRelaySigner` (`pkg/auth/relay_signer.go:32`), `authProxyConfig` and `openSignedConn` (`pkg/integration/auth_test.go:50-75`), `authTestKey1`, `testenv.StartServerProxy`, `testenv.WithConfigMutator`, and a bare `testenv.ProxyOption` literal for `opts.Signer` — the same inline-literal pattern `storage_integrity_agent_test.go:97-99` already uses for `opts.StorageIntegrityAdmissionConsumer`.

- [x] **Step 1: Add the maintenance-session test**

```go
// TestStorageIntegrityRead_HeredocCannotHideAReservedName is the Spec N D1
// regression against a real ClickHouse. A maintenance session skips SQL
// rewrite by design (Spec I D6), so pkg/plugins/sireserved is the only control
// on this path; before Spec N a comment marker inside a heredoc blanked the
// rest of the statement from both scan surfaces and the guard saw nothing.
func TestStorageIntegrityRead_HeredocCannotHideAReservedName(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run `go run ./cmd fetch-rewriter-lib --tag <tag>` and pass --test_env")
	}
	ctx := context.Background()
	const phys = "phys_si_heredoc"
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	seed := openConnNoDB(t, chEnv.Addr)
	for _, query := range []string{
		"CREATE DATABASE IF NOT EXISTS " + phys,
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"DROP TABLE IF EXISTS hg_safe.db1__hd",
		"CREATE TABLE hg_safe.db1__hd (_hg_row_id FixedString(32), a UInt32) ENGINE = MergeTree ORDER BY a",
		"SYSTEM STOP MERGES hg_safe.db1__hd",
		"INSERT INTO hg_safe.db1__hd VALUES (repeat('a', 32), 1)",
	} {
		if err := seed.Exec(ctx, query); err != nil {
			t.Fatalf("seed %q: %v", query, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP TABLE IF EXISTS hg_safe.db1__hd")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys)
	})

	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("db1"),
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), "db1", registry.DbAuthOwner),
		// Options.Signer is the host attestation build.go requires before the
		// validator will honour SQL_sentio_maintenance (build.go:352-361).
		func(_ *config.Config, opts *housegate.Options) { opts.Signer = signer },
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.StorageIntegrity.Tables = []string{"db1.hd"}
			cfg.StorageIntegrity.Read.DefaultMode = string(rewriter.ReadModeSafe)
		}),
	)
	conn := openSignedConn(t, proxy.Addr, signer)
	maintenance := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"SQL_sentio_maintenance": clickhouse.CustomSetting{Value: "1"},
	}))

	for _, tc := range []struct{ name, sql string }{
		{"heredoc hiding a line comment", "SELECT $$--$$ AS x, count() FROM hg_safe.db1__hd"},
		{"tagged heredoc hiding a slash comment", "SELECT $tag$//$tag$ AS x, count() FROM hg_safe.db1__hd"},
		{"heredoc export to an ordinary table", "SELECT $$--$$ AS x, a FROM hg_safe.db1__hd"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := conn.Exec(maintenance, tc.sql)
			if err == nil {
				t.Fatalf("%q must reach the client as an Exception, not a result set", tc.sql)
			}
			if !strings.Contains(err.Error(), "hg_safe") {
				t.Fatalf("%q error = %v, want it to name the reserved database", tc.sql, err)
			}
		})
	}
	// The guard is targeted, not a blanket refusal of maintenance traffic.
	if err := conn.Exec(maintenance, "SELECT $$ordinary$$ AS x"); err != nil {
		t.Fatalf("an ordinary heredoc on a maintenance session must pass: %v", err)
	}
}
```

- [x] **Step 2: Prove the maintenance session is really established before trusting the assertion**

A test that passes because the flag was never set would prove nothing. Before running the negative assertions, assert the *positive*: the same session must be refused on a plain `SELECT count() FROM hg_safe.db1__hd` (which `sireserved` already catches today). Add that as the first sub-case. If it does **not** error, the maintenance wiring is wrong — `opts.Signer` unset, the signer address not in `auth.allowed_addresses`, or `SQL_sentio_maintenance` not reaching `meta.Settings` — and every other assertion in this test is vacuous.

- [x] **Step 3: Run it against docker, before and after Task 11**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
FFI=$(bazel run //cmd:housegate -- fetch-rewriter-lib --tag v0.9.0 | tail -n 1)
bazel test //pkg/integration:integration_test \
  --test_filter='TestStorageIntegrityRead_HeredocCannotHideAReservedName' \
  --test_env=POLYGLOT_SQL_FFI_PATH=$FFI \
  --test_env=DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}') \
  --test_env=HOME --test_output=errors
```

Expected **with Task 11 reverted** (`git stash` the plugin change, keep the test): the three heredoc sub-cases fail with `must reach the client as an Exception, not a result set`, while the plain-`SELECT` guard sub-case and the ordinary-heredoc sub-case pass. Expected **with Task 11 applied**: all green. Record both runs in the commit message — this is the end-to-end half of the 6/8 → 0/8 evidence, and the pre-fix run is the part that proves the assertion is load-bearing.

The FFI tag stays `v0.9.0` here: this test exercises housegate's own guard, not the engine, so it must not be coupled to the release Part E cuts.

- [x] **Step 4: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/integration/storage_integrity_read_test.go
git commit -m "test(storage-integrity): prove the heredoc guard end to end (Spec N D1)"
```

**Verification command:** `bazel test //pkg/integration:integration_test --test_filter='TestStorageIntegrityRead_HeredocCannotHideAReservedName' --test_env=POLYGLOT_SQL_FFI_PATH=$FFI --test_env=DOCKER_HOST=... --test_env=HOME --test_output=errors`

### Task 13: repo guide, full Bazel gate, and push to the PR branch

**Files:**
- Modify: `CLAUDE.md`, `pkg/plugins/AGENTS.md`

- [x] **Step 1: Update the repo guide**

In `CLAUDE.md`, in the `pkg/plugins/` bullet's `lthash` / `sireserved` neighbourhood, extend the `sireserved` description so the next reader learns the completed obligation (one line, no hard wrapping):

```markdown
`sireserved`'s scan is a complete lexical model of every span in which a name can hide: `'…'`, `` `…` ``, `"…"`, `--`, `#`, `#!`, `//`, nested `/* */` **and ClickHouse heredocs** (`$$…$$` / `$tag$…$tag$`). A heredoc body is blanked from the executable surface and kept verbatim on the literal surface, because table functions read literal arguments as identifiers; ClickHouse applies no escape processing inside a heredoc, so heredoc bodies do not inherit the backslash refusal `'…'` carries. A `$` that opens no well-formed heredoc is refused rather than copied through — copying it through is what let a comment marker inside a heredoc blank the rest of a statement from both surfaces (Spec N D1).
```

In `pkg/plugins/AGENTS.md`, extend the "SI operator-bypass guard" row so it names the heredoc span too.

- [x] **Step 2: Full Bazel gate**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel build //... && bazel test //... --test_output=errors
```

Expected: identical to the Task 10b baseline plus the new tests. Any newly failing target is a regression — fix it before pushing. No new files were added, so `bazel mod tidy` / `gazelle` are not needed; run them anyway if `BUILD.bazel` drift is reported.

- [ ] **Step 3: Push onto the PR branch**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git push origin si-lexical-closure:feature/si-surface-failclosed-housegate
```

Add a comment to PR #141 carrying: the 6/8 → 0/8 reproduction flip, the pre-fix integration failure from Task 12 Step 3, and the statement that Spec N D1 is now part of this PR so #141 no longer ships the hole it introduced.

**Verification command:** `bazel build //... && bazel test //... --test_output=errors`

---

## Part E — release, the engine-dependent integration assertion, and the spec record

### Task 14: release both engines in dependency order

Spec G plan D-10's publication rule: the corpus must never be published from only one engine, and rewriter-go releases first. Read deviation **D-6** before starting — the version is workflow-derived, not chosen.

**Files:** none (release workflows).

**Preconditions:** Part C Task 10 green with zero unexplained divergences.

- [ ] **Step 1: Merge and release rewriter-go**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
gh pr create --fill --title "fix(storage-integrity): lexical and SHOW-namespace closure (Spec N)"
```

The PR body must carry: the corpus case count and sha256, the Part C differential record (date, `<rewriter-grpc-commit>`, divergences), and the four measured pre-fix `Success` classes this closes (`SHOW COLUMNS`/`INDEX`/`INDEXES`/`KEYS`, the five connector functions, the tagged-heredoc namespace bypass, and the unknown SHOW kind).

After merge to `main`:

```bash
gh workflow run release.yml --ref main --repo housegate/rewriter-go
gh release list --repo housegate/rewriter-go --limit 3
```

Expected: a new tag **greater than `v0.9.0`**. `scripts/next-version.sh` yields `v0.9.1` only when `v0.9.0`'s annotated-tag date is *today* in `Asia/Shanghai` (it was tagged 2026-08-25); on any later day the workflow yields `v0.10.0`. **Record whatever it prints as `<rewriter-go-tag>` and do not hand-force a version** — Spec N §5's "v0.9.1" is a prediction. Spec O consumes the recorded tag.

- [ ] **Step 2: Verify the FFI assets exist for the new tag**

```bash
gh release view <rewriter-go-tag> --repo housegate/rewriter-go --json assets --jq '.assets[].name'
```

Expected: `libpolyglot_sql_ffi.so`, `libpolyglot_sql_ffi.dylib` and `SHA256SUMS`. The native engine's FFI binary is a separate artifact from the Go module — housegate's test lane needs the binary, Spec O's `go.mod` bump needs the module.

- [ ] **Step 3: Merge and release rewriter-grpc**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
gh pr create --fill --title "fix(storage-integrity): lexical and SHOW-namespace closure (Spec N)"
```

After merge, run the `cut-release` workflow (Actions → cut-release → Run workflow) — do **not** bump versions by hand. It tags `main` and dispatches `release.yml`.

```bash
gh release list --repo housegate/rewriter-grpc --limit 3
```

Record the tag as `<rewriter-grpc-tag>`.

**Verification command:** `gh release view <rewriter-go-tag> --repo housegate/rewriter-go --json assets --jq '.assets[].name'`

### Task 15: the `SHOW COLUMNS` integration assertion and the CI FFI pin

This is the half of Spec N §4.6 that needs the fixed engine. It lands on PR #141's branch in one commit together with the CI FFI pin bump, so the branch's CI never sees an assertion its pinned engine cannot satisfy.

**Files:**
- Modify: `pkg/integration/storage_integrity_read_test.go`, `.github/workflows/ci.yml`

**Preconditions:** Task 14 Step 1–2 done; `<rewriter-go-tag>` known and its FFI assets published.

- [ ] **Step 1: Bump the test-lane FFI pin**

In `.github/workflows/ci.yml`, change the integration job's `bazel run //cmd:housegate -- fetch-rewriter-lib --tag v0.9.0` (line ~111 on the PR branch) to `--tag <rewriter-go-tag>`. Update the same tag inside the `t.Skip` messages of `pkg/integration/storage_integrity_read_test.go` so a local skip tells the reader the right command.

This is the **test-lane** pin only. The `go.mod` / `go.sum` module pin is Spec O's, deliberately: this commit must not make #141 depend on a module bump it does not otherwise need.

- [ ] **Step 2: Extend the existing Critical-statement table**

`TestStorageIntegrityRead_CriticalStatementsAreRefused` (`pkg/integration/storage_integrity_read_test.go:175`) already drives a table of `{name, sql, wantMessage}` through an ordinary (unsigned) session with `db1.guard` configured. Append four rows:

```go
		{"show columns on the safe namespace, two-FROM form", "SHOW COLUMNS FROM db1__guard FROM hg_safe",
			"storage-integrity physical table hg_safe.db1__guard is not directly addressable"},
		{"show columns on the safe namespace, qualified form", "SHOW COLUMNS FROM hg_safe.db1__guard",
			"storage-integrity physical table hg_safe.db1__guard is not directly addressable"},
		{"show index on the unsafe namespace", "SHOW INDEX FROM hg_unsafe.db1__guard",
			"storage-integrity physical table hg_unsafe.db1__guard is not directly addressable"},
		{"show extended columns on the safe namespace", "SHOW EXTENDED COLUMNS FROM db1__guard FROM hg_safe",
			"storage-integrity physical table hg_safe.db1__guard is not directly addressable"},
```

Then, after the table loop and beside the existing part/row invariance assertions, add the pass-through control so the gate is proved targeted rather than blanket:

```go
	// An ordinary database still enumerates: the gate is on the reserved
	// namespaces, not on the SHOW COLUMNS family as such.
	if err := conn.Exec(ctx, "SHOW COLUMNS FROM guard FROM db1"); err != nil &&
		!strings.Contains(err.Error(), "storage-integrity") {
		t.Logf("SHOW COLUMNS on the logical database returned %v (a ClickHouse-level error is acceptable here; a storage-integrity refusal is not)", err)
	} else if err != nil {
		t.Fatalf("SHOW COLUMNS on an ordinary target must not be refused by storage-integrity: %v", err)
	}
```

`db1.guard` is itself an SI logical table, so a `storage-integrity logical database db1 …` refusal there would be *correct* — pick an ordinary logical database registered by `WithExtraDatabases` for the control if one is available, and if not, drop the control from the integration test and rely on the corpus's `si_show_columns_ordinary_database_passthrough`, noting that in the commit message. Do not leave an assertion whose expected outcome you cannot state.

- [ ] **Step 3: Run before and after the engine bump**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
OLD=$(bazel run //cmd:housegate -- fetch-rewriter-lib --tag v0.9.0 | tail -n 1)
NEW=$(bazel run //cmd:housegate -- fetch-rewriter-lib --tag <rewriter-go-tag> | tail -n 1)
for LIB in "$OLD" "$NEW"; do
  bazel test //pkg/integration:integration_test \
    --test_filter='TestStorageIntegrityRead_CriticalStatementsAreRefused' \
    --test_env=POLYGLOT_SQL_FFI_PATH=$LIB \
    --test_env=DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}') \
    --test_env=HOME --test_output=errors
done
```

Expected: with the **old** (`v0.9.0`) lib the four new rows fail — measured on that engine, `SHOW COLUMNS FROM db1__guard FROM hg_safe` returns `Success` with the SQL unchanged, so ClickHouse answers with a **result set** listing the SI physical schema including `_hg_row_id`, and `conn.Exec` returns `nil`. With the **new** lib every row passes. That pair of runs is the acceptance evidence for Spec N §1b; record both in the commit message.

- [ ] **Step 4: Full gate and push**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel build //... && bazel test //... --test_output=errors
git add pkg/integration/storage_integrity_read_test.go .github/workflows/ci.yml
git commit -m "test(storage-integrity): prove the SHOW COLUMNS gate end to end and pin the fixed engine"
git push origin si-lexical-closure:feature/si-surface-failclosed-housegate
```

**Verification command:** `bazel test //pkg/integration:integration_test --test_filter='TestStorageIntegrityRead_CriticalStatementsAreRefused' --test_env=POLYGLOT_SQL_FFI_PATH=$NEW --test_env=DOCKER_HOST=... --test_env=HOME --test_output=errors`

### Task 16: close Spec N and update the roadmap

**Files:**
- Modify: `docs/superpowers/specs/2026-08-25-storage-integrity-lexical-namespace-closure-design.md`
- Modify: `docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md`

Land these on whichever branch owns the spec files at that point — today that is `docs/storage-integrity-lexical-rollout-specs`; if it has merged, a small docs PR onto `main`. Do **not** put them on the #141 branch unless the specs already live there.

- [ ] **Step 1: Record the D3 differential and the deviations in Spec N**

Change `**Status:** Proposed` to `**Status:** Implemented` and append to §5:

```markdown
**Delivered:** rewriter-go `<rewriter-go-tag>`, rewriter-grpc `<rewriter-grpc-tag>`, housegate D1 on PR #141 (merged by Spec O). Corpus: `<N>` cases, sha256 `<corpus-sha>`, byte-identical in both engine repos.

**Cross-engine differential (D3), executed `<YYYY-MM-DD>`:** `TestStorageIntegrityGolden` with `REWRITER_ORACLE_ADDR` against rewriter-grpc `<rewriter-grpc-commit>` over all `<N>` cases. Divergences: `<none | list with reasons>`.

**Amendments made during implementation.** (a) A further Critical bypass of the same class was found and closed: the Go engine decoded a tagged heredoc argument as `"<tag>\x00<body>"`, so `merge($t$hg_safe$t$, 'db1__t')` passed the namespace gate and was re-emitted as `merge('hg_safe', 'db1__t')` — the value policy inspected was not the value the generator emitted. Reachable by any authenticated user; the C++ engine was unaffected because ClickHouse's parser materializes heredocs into ordinary literals. (b) D4's connector list is `mysql`, `postgresql`, `mongodb`, `jdbc`, `odbc`; `sqlite` and `redis` are excluded because their documented signatures name a SQLite file table and a Redis *column*, so gating them would only manufacture false positives. (c) The connector decode is arity-dependent — `mongodb`'s URI form and `jdbc`/`odbc`'s two-argument form carry no database — rather than a flat pair at index 1. (d) `<the D-4 unknown-SHOW-kind outcome: shared corpus, or engine-local with the C++ SyntaxError reason>`.
```

Add to §6: **the `redis` / `sqlite` exclusion** and **`jdbc`/`odbc`'s two-argument qualified form** as recorded debt, each with the reason above, so a later reader does not re-open them by accident.

- [ ] **Step 2: Update the roadmap**

In `2026-08-25-storage-integrity-closure-roadmap.md` §2, change Spec N's urgency cell from `**Blocker** — must land before PR #141 merges` to `**Shipped** — <rewriter-go-tag> / <rewriter-grpc-tag> / housegate D1 on #141`. In §1, extend the "Two new bypasses" paragraph to three, naming the tagged-heredoc namespace bypass, since it is the same finding class and the roadmap is the index a reader starts from.

- [ ] **Step 3: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add docs/superpowers/specs/2026-08-25-storage-integrity-lexical-namespace-closure-design.md \
        docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md
git commit -m "docs(storage-integrity): close Spec N and record the differential run"
```

**Verification command:** `git diff --stat HEAD~1` shows exactly the two spec files, and `grep -n 'Status:' docs/superpowers/specs/2026-08-25-storage-integrity-lexical-namespace-closure-design.md` shows `Implemented`.

---

## Self-review

Run after the plan is written, before execution.

**1. Spec coverage** — see the map below; every Spec N decision and every §4 acceptance item has a task.

**2. Placeholder scan** — no "TBD", no "add error handling", no "similar to Task N", no test described without its code or its exact expected pre-fix failure. The remaining `<placeholders>` are release and measurement artifacts that cannot be known before execution, and each is produced by a named earlier step: `<corpus-sha>` (Part A Task 5 Step 2), `<rewriter-grpc-commit>` (Part C Task 10 Step 1), `<N>` (Part A Task 5 Step 1), `<rewriter-go-tag>` (Part E Task 14 Step 1), `<rewriter-grpc-tag>` (Part E Task 14 Step 3).

**3. Pre-fix failure discipline (roadmap §4.9)** — every guard names the state it must flip:

| Guard | Pre-fix state (measured) | Proving step |
|---|---|---|
| SHOW `COLUMNS`/`INDEX` grammar | table bound into `DB`; `EXTENDED` consumed as the kind | A/1 Step 2 |
| SHOW target-bearing gate | ten statements `Success`, SQL unchanged | A/2 Step 2 |
| SHOW unknown kind | `SHOW SOMETHINGNEW FROM hg_safe` → `Success` | A/2 Step 4 |
| Connector table functions | five statements `Success`, SQL unchanged | A/3 Step 3 |
| Tagged heredoc namespace | `merge($tag$hg_safe$tag$, 'db1__t')` → `Success`, emitted as `merge('hg_safe', …)` | A/4 Step 3 |
| C++ SHOW family | refuses with the *generic* D1 message (accidental) | B/7 Step 2 |
| C++ connectors | five statements `Success` | B/6 Step 3 |
| `sireserved` heredoc | 6/8 statements pass the guard | D/10b, D/11 Step 4, D/11 Step 7 |
| `sireserved` `$` refusal | `SELECT 1 $ 2`, `SELECT $$unterminated` → no violation | D/11 Step 4 |
| Heredoc end to end | maintenance session gets a result set, not an Exception | D/12 Step 3 |
| SHOW COLUMNS end to end | ordinary session gets the SI physical schema as a result set | E/15 Step 3 |

**4. Guards that must NOT flip** — the controls, each stated as "passes before and after": `si_show_columns_ordinary_database_passthrough`, `si_show_merges_target_less_passthrough`, `si_mysql_ordinary_database_allowed`, `si_sqlite_foreign_file_allowed`, `si_redis_column_name_allowed`, `si_merge_bare_heredoc_physical_rejected`, `TestParseDBLevel_nonColumnsFamilyGrammarIsUnchanged`, `HeredocBodyReachesTheLiteralSurface/merge database heredoc`, `HeredocIsNotABlanketRefusal/*`, `TestOnQuery_ObjectCarrierScanAvoidsNonCallableFalsePositives`, and the four pre-existing `si_show_dictionaries_*` / `si_show_tables_*` cases. A "fix" that reds any of these is a wider blast radius than the hole it closes.

**5. Type and name consistency**
- `showKindClass` (Go, A/2) ↔ the C++ classification (B/7): the same three lists and the same fail-closed default, checked by the shared corpus.
- `engine.DBLevelInfo.ShowTable` / `HasTableClause` / `ShowExtended` (A/1) are consumed only by `dispatchShowTables` (A/2).
- `decodeStringLiteralValue` (A/4) is the only literal decoder used by `tableFunctionArgValue`; A/4 Step 5 audits that no other policy reader takes `lit["value"]` raw.
- The connector name list appears in exactly three places and they are added in the same order: `decodeNamespaceFunctionRefDetail` (A/3), C++ `decodeNamespaceFunction` (B/8), and `sireserved.isObjectCarrierName` (D/11 Step 6). `sqlite` and `redis` appear in none of them.
- `consumeHeredoc` (D/11) mirrors `consumeStringLiteral`'s `(next, value, error)` signature exactly.
- Corpus pins move in lockstep: `sicorpus_test.go:231-233` (A/5) and `si_corpus.h:48-50` (B/6), with `cmp` + sha256 proofs at A/0, B/6 Step 1 and B/9 Step 2.
- Every message string used in a new corpus case is quoted from Global Constraints; no task invents one.

**6. Ordering constraints that are real dependencies, not preferences**
- Part A Task 5 must be green before Part B Task 6 copies the corpus.
- Part B Task 9 must be green before Part C Task 10, or the differential compares against a stale binary.
- Part C Task 10 must be green before Part E Task 14, per Spec N D3 ("before Spec N merges").
- Part E Task 14 Steps 1–2 must precede Part E Task 15, which needs the published FFI assets.
- Part D is independent of Parts A–C and may run in parallel with them; only Part E Task 15 couples housegate to the engine release.

## Spec coverage map

| Spec N section | Requirement | Tasks |
|---|---|---|
| §1.1a | heredoc literals blank the operator guard | D/11, D/12 |
| §1.1b | `SHOW COLUMNS`/`INDEX`/`INDEXES`/`KEYS` escape the namespace gate; engines diverge | A/1, A/2, B/7, C/10, E/15 |
| §1.1c | the corpus cannot see engine divergence | C/10 |
| §1.1d | connector table functions outside the gate | A/3, B/8 |
| §3 D1 | heredoc span modelled; stray `$` refused; body on `withLiterals`; escape claim verified | D/11 Steps 1, 3, 5 |
| §3 D2 parser | `EXTENDED` prefix + reversed two-`FROM` grammar | A/1 |
| §3 D2 handler | positive three-way classification, target-bearing gate, fail-closed unknown kind | A/2, B/7 |
| §3 D3 | differential executed over the full corpus and recorded | C/10, E/16 Step 1 |
| §3 D4 | connector family in the namespace decoder, signatures read from the docs | A/3 Steps 1 and 4, B/8 |
| §3 D5 | every new behaviour pinned in the one byte-identical corpus | A/2, A/3, A/4, A/5, B/6, B/9 Step 2 |
| §4.1 | heredoc regression table, incl. `withLiterals`, unterminated, bare `$`, each failing pre-fix | D/11 Steps 3–4, D/10b |
| §4.2 | SHOW matrix green in both engines | A/2, B/7 |
| §4.3 | unknown-kind catch-all | A/2 Step 4, B/7 Step 2 (deviation D-4) |
| §4.4 | one reject per connector name plus an ordinary control | A/3 Step 2, B/8 Step 3 |
| §4.5 | cross-engine diff, zero unexplained divergences, recorded | C/10 |
| §4.6 | HouseGate integration: `SHOW COLUMNS` and heredoc against real ClickHouse | D/12, E/15 |
| §5 delivery | three PRs; engines together then the tag; housegate D1 onto #141 | D/13 Step 3, E/14, E/15 Step 4 |
| §6 debt | `redis`/`sqlite` exclusion and the `jdbc`/`odbc` short form recorded | E/16 Step 1 |
| roadmap §4.1 | a `$` outside a quoted span is refused, not copied through | D/11 Step 5 |
| roadmap §4.2 | positive SHOW classification with a fail-closed default | A/2 Step 3 |
| roadmap §4.3 | metadata confidentiality is not an SI v1 property (`SHOW MERGES` stays open) | A/2 Step 1 (`si_show_merges_target_less_passthrough`) |
| roadmap §4.4 | the two engines proved equal by execution, once, before N merges | C/10 |
| roadmap §4.9 | every new guard proven against the unfixed code | Self-review item 3 |
| — (plan deviation D-1) | tagged-heredoc namespace bypass | A/4, B/8 Step 2, E/16 Step 1 |
