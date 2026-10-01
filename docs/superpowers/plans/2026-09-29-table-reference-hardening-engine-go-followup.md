# Table-Reference Hardening — Plan A follow-up: an engine-neutral shared corpus (rewriter-go)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the merged 987-case storage-integrity corpus something a correct C++ engine can pass. Every case whose pin records a Go implementation artifact or defect (class G of the divergence inventory) is replaced. The spec-required behaviour stays pinned exactly. The pins for C++ formatting (class F) are deferred to the paired C++ re-pin. rewriter-go v0.14.0 stays unreleased until that re-pin shows zero divergences.

**Architecture:** Five native changes, each test-first and each flipping only its own corpus pins:

1. `doRewrite` normalises `CREATE TABLE … EMPTY AS SELECT`, whose body Polyglot drops, and puts `EMPTY` back after the rewrite.
2. A parenthesised IN operand stops being "SI-handler-blind".
3. SI-active refusals of `DESCRIBE (SELECT …)` and of column-expression reads name the SI object or use the SI catch-all.
4. The admitted session `SET` answers `success`.
5. The differential compares a rejection's accessed tables only when the case pins them.

Two corpus-only changes follow: inputs ClickHouse cannot parse are replaced, and shapes only one engine models leave the shared corpus for native unit tests. Every corpus edit goes through one deterministic script (Task 0), so the final bytes are known in advance: sha256 `76d8db60921387db62452fd27661211da62e06fbbba894c90af5f7d7ccd230e0`, 872362 bytes, 966 cases, FNV-1a/64 `10342073271660396945`.

**Tech Stack:** Go 1.25, Polyglot FFI v0.13.0 asset (`POLYGLOT_SQL_FFI_PATH`), the rewriter-go harness (`internal/harness`), Python 3 (corpus edit script, not committed), `clickhouse local` 26.3 (parse check), the rewriter-grpc v0.15.0 build on the build box (differential), `gh`.

**Spec:** `/Users/uranuswch/src/tableref-docs/2026-09-26-table-reference-hardening-design.md` (the housegate `docs/superpowers/specs/2026-09-26-table-reference-hardening-design.md`). Read §5 (policy table, precedence, "Mutation expressions…", "What the engines report"), §10.1 and §10.5.

**Inputs:** `/Users/uranuswch/src/tableref-docs/corpus-divergence-inventory.md` and `corpus-divergence.tsv` (the measured 726 divergences: M 610, F 51, G 46, P 19). This plan resolves exactly the 46 G cases and the 6 P-contract cases. Every change below was prototyped in a throwaway worktree of `54c4a8a`: every test, snippet and expected value in this plan was run there, and both CI lanes were green.

**Repository and working copy:**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go.feat-table-reference-hardening
git fetch origin
git worktree add -b fix/table-reference-engine-neutral-corpus \
  /Users/uranuswch/Dev/housegate/rewriter-go.fix-table-reference-engine-neutral-corpus origin/main
cd /Users/uranuswch/Dev/housegate/rewriter-go.fix-table-reference-engine-neutral-corpus
git submodule update --init third_party/polyglot-src
export POLYGLOT_SQL_FFI_PATH=$HOME/Library/Caches/housegate/rewriter-ffi/v0.13.0/libpolyglot_sql_ffi.dylib
```

`origin/main` is `54c4a8a` (PR #41 squash). Every path below is relative to this worktree.

## Global Constraints

- CI runs `go vet ./...` and `env SNAPSHOT_QUERY_ORDINARY=1 go test ./...`, with and without `POLYGLOT_SQL_FFI_PATH`. Both must pass at every commit. (Without `SNAPSHOT_QUERY_ORDINARY=1` the snapshot-query tests fail on `SNAPSHOT_MEASURED_FFI`; that is environment, not a regression.)
- The corpus is edited only through `tmp/corpus_edit.py` (Task 0), one group per task, in the order `empty` → `blind` → `simsg` → `chsql` → `local`. `tmp/` is git-ignored. The script re-emits only the elements it touches and keeps every other byte. After each group, copy the three constants it prints into `internal/harness/sicorpus_test.go` (`SICorpusFingerprint`, `SICorpusBytes`, `SICorpusCases`). The expected values after each task are listed in the task. A different value means the base or the script differs: stop.
- Do not run `UPDATE_GOLDEN` in this PR, and do not add `want_sql_cpp` / `allow_sql_divergence`. C++ SQL for any new or changed success case comes from the Plan B v2 build (Task 10).
- The five message families are unchanged cross-engine contract texts (rewriter-go `AGENTS.md`, "Message families"). This plan changes which message some V2 cases expect; it adds no text.
- Corpus schema: no new keys. Reject cases keep an exact `want_code` and a `want_message_contains`. Rationale: `harness.Compare` diffs code and message exactly (spec §10.5 extended it to `message` so that zero divergences covers the design's fields), so a looser "any rejection" assertion would still show up as a divergence. The engines must agree, so each G pin gets an input or behaviour both engines can agree on, or leaves the shared corpus. The one relaxation is Task 1's: spec §5 says a rejection's `AccessedTables` "may still be partial", so the differential compares it only where the corpus pins it, which is exactly what the C++ runner already does.
- Do not reformat unrelated files. `gofmt -l .` already flags `internal/handlers/dblevel.go` on `origin/main` (a Go 1.25 doc-comment quote rewrite); leave it alone.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Markdown has no hard line-wrapping.
- Spec §11: ask before merging the PR and before any release.

## Review Focus

These are inputs the change could get wrong that no existing test covers. Each line names the test that pins it.

1. **A table named `empty`.** `CREATE TABLE db1.empty ENGINE = Memory EMPTY AS SELECT …` must keep its name. `CREATE TABLE db1.empty AS SELECT 1` and `CREATE TABLE db1.n AS db1.empty` must not be touched. Task 2: `TestStripCreateTableEmpty` rows and the native row "ctas empty table named empty".
2. **`EMPTY` with a property the generator moves.** Polyglot prints `COMMENT` after the body, and ClickHouse 26.3 accepts `… EMPTY AS (SELECT …) COMMENT 'c'`. Task 2: "ctas empty comment before empty" and `TestInsertCreateTableEmpty` "trailing comment".
3. **A refused `EMPTY` statement echoes the caller's SQL, not the stripped SQL.** Task 2: "ctas empty protected source refused".
4. **Removing the parenthesised IN from the blind set opens nothing under V2.** Every position that can hold `x IN ((hg_*.t))`, including SELECT, ALTER DELETE/UPDATE, DELETE, UPDATE, INSERT … SELECT, CTAS, view, MV, a column DEFAULT, MODIFY COLUMN and MODIFY TTL, is still refused. Task 3: `TestTableRef_ParenthesizedInOperandUnderActiveSurface`.
5. **The new annotation fallback must not invent an SI object.** An alias-qualified column `hg_safe.x` is not a table. Task 4: two rows in `TestAnnotateStorageIntegrityReject_UnprovenNamesKeepCallerMessage`.
6. **With the surface inactive, `DESCRIBE (SELECT … phys …)` keeps `statement is not supported`.** It is an unmodelled class refused before T3, not a T3 hit. Task 4: `TestTableRef_DescribeAndShowTargets` (V1 rows unchanged).
7. **The harness relaxation is narrow.** A pinned rejection list and every success list are still diffed. Task 1: `TestAlignUnpinnedOracleFields`.

## Decisions this plan records

| G group (inventory §4) | Cases | Decision | Spec basis |
|---|---|---|---|
| G1 `EMPTY` body dropped | 1 | Fix Go (Task 2); pin the rewritten body; add a V2 derived-read case | §5 row "INSERT … SELECT body, CREATE TABLE … AS SELECT body (incl. `EMPTY`…)": rewrite, report |
| G2 Go walk/generator limits, top-level `(SELECT …)` | 12 | Leave the corpus; pin in `native_tableref_test.go` (Task 7) | §5: "refused with `statement is not supported` where [the engine] does not [model the position]"; "every engine-internal error … is a coded `UnsupportedStatement`" — the outcome is engine-specific by design |
| G3 input not ClickHouse SQL | 27 | 17 replaced by ClickHouse-parseable inputs that exercise the same rule (Task 6); 10 leave the corpus, already pinned by native tests (Task 7) | §1 invariant: a statement ClickHouse cannot parse reads nothing, whatever the engine says. The C++ engine uses ClickHouse's parser and can only say `SyntaxError` |
| G4 parenthesised IN "SI-handler-blind" | 1 | Fix Go (Task 3); pin the SI message | T3: "When the SI surface is active, the existing SI messages for `hg_*` and SI-physical names keep precedence" |
| G5 inactive text under an active surface | 5 | Fix Go (Task 4); pin the SI catch-all / SI object | §5 precedence: T3 "with the SI messages first" precedes the ordinary stage; "While the SI surface is active, every unmodelled class is answered with the SI catch-all message" |
| P-contract: unpinned rejection AccessedTables | 6 | Harness compares a rejection list only when pinned (Task 1) | §5: "`AccessedTables` on a rejection may still be partial" |

One more Go change is not G: the admitted `SET` pass-through returns `message: ""` where every other success returns `"success"` (Task 5). `si_tr_set_inactive_passthrough` also needs C++ to classify `SET` as `UNSPECIFIED`, which is M.

## Pin sequencing across the two repositories

| Lands in | What |
|---|---|
| **This PR** (rewriter-go `fix/table-reference-engine-neutral-corpus`) | Tasks 1–8: the native fixes, their pin changes, the new case, the 17 replaced inputs, the 22 removed cases, the harness rule, and the fingerprint re-pin to `76d8db60…` / 872362 / 966 / `10342073271660396945`. There are no C++ SQL pins. v0.14.0 is not released. |
| **Plan B v2 PR** (rewriter-grpc) | Copies the corpus from this PR once it is merged, byte for byte, and implements M and P. Its C++ runner cannot be fully green on success cases until the re-pin below, because F formatting differs. Plan B v2 decides whether to gate those rows on the re-pinned file or to stage the copy after it. |
| **Paired re-pin** (Task 10, run with Plan B v2) | With the Plan B build as the oracle, `UPDATE_GOLDEN=1` writes `want_sql_cpp` for the 51 F cases and for every M success case whose C++ formatting differs. The differential is re-run until it shows zero divergences and recorded in spec §10.5. A rewriter-go PR and the rewriter-grpc PR then pin the same SHA-256. rewriter-go v0.14.0 is released after that, then rewriter-grpc v0.16.0. |

---

### Task 0: branch, baseline, and the corpus edit script

**Files:**
- Create (git-ignored): `tmp/corpus_edit.py`

- [ ] **Step 1: Confirm the base**

```bash
git log -1 --format=%h                                          # 54c4a8a
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json
# 2c03e82861c723e3e320d2f3afcf6ebcc90219e4c0f3ff786b78eb23465a7fa4
go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
```

Expected: both lanes `ok` for every package.

- [ ] **Step 2: Write `tmp/corpus_edit.py`**

```python
#!/usr/bin/env python3
"""Engine-neutral corpus edits for the table-reference follow-up.

Usage: python3 tmp/corpus_edit.py <corpus.json> <group>

Edits internal/harness/testdata/storage_integrity_cases.json in place. Only
the elements named below are rewritten (re-emitted with json.dumps(indent=2));
every other element keeps its exact bytes. Prints the three constants for
internal/harness/sicorpus_test.go.
"""
import json
import sys

PATH, GROUP = sys.argv[1], sys.argv[2]
raw = open(PATH, encoding="utf-8").read()
dec = json.JSONDecoder()


def elements(text):
    i = text.index("[") + 1
    while True:
        while text[i] in " \n\t,":
            i += 1
        if text[i] == "]":
            return
        obj, end = dec.raw_decode(text, i)
        yield i, end, obj
        i = end


def emit(obj):
    lines = json.dumps(obj, indent=2, ensure_ascii=False).split("\n")
    return "\n".join(lines[:1] + ["  " + line for line in lines[1:]])


def acc(db, table, logical, physical, si=False):
    entry = {"original_database": db}
    if table:
        entry["original_table"] = table
    if logical:
        entry["logical_database"] = logical
    entry["physical_database"] = physical
    if si:
        entry["is_storage_integrity"] = True
    return entry


SI_CATCH_ALL = ("storage-integrity is configured; statement class is not "
                "modelled by the rewriter and cannot be forwarded")
PROJ = "ALTER TABLE db1.o ADD PROJECTION p (SELECT a ORDER BY "

# (G2, G3) Cases that leave the shared corpus. Each is pinned by a native
# unit test instead (native_tableref_test.go): TestTableRef_GoErrorsFailClosed,
# TestTableRef_EngineLocalShapes, TestTableRef_DescribeAndShowTargets,
# TestTableRef_ParametersInTablePositionsAreRefused,
# TestTableRef_ResidualShowColumnsBody, TestTableRef_Residual2RefreshPositions.
DELETE = {
    "si_tr_goerr_lambda_subquery_rejected", "si_tr_goerr_lambda_subquery_v2_rejected",
    "si_tr_goerr_any_subquery_rejected", "si_tr_goerr_any_subquery_v2_rejected",
    "si_tr_goerr_values_subquery_rejected", "si_tr_goerr_values_subquery_v2_rejected",
    "si_tr_goerr_nested_in_60_rejected", "si_tr_goerr_nested_in_60_v2_rejected",
    "si_tr_goerr_union_500_rejected", "si_tr_goerr_union_500_v2_rejected",
    "si_tr_top_level_paren_select_rejected", "si_tr_top_level_paren_select_v2_rejected",
    "si_tr_exists_empty_rejected", "si_tr_exists_empty_v2_rejected",
    "si_tr_show_create_empty_rejected", "si_tr_show_create_empty_v2_rejected",
    "si_tr_param_show_columns_from_rejected", "si_tr_param_show_index_from_rejected",
    "si_tr_show_columns_like_subquery_rejected", "si_tr_show_columns_like_subquery_v2_rejected",
    "si_tr_r2_mv_refresh_to_rejected", "si_tr_r2_mv_refresh_to_v2_rejected",
}

# (G3) ClickHouse 26.3 cannot parse the old input; the new one exercises the
# same rule in a position ClickHouse accepts. name -> (new name or None,
# new sql, extra field updates).
REPLACE = {
    "si_tr_r4_alter_add_projection_date_in_rejected": (None, PROJ + "date IN (`db2.x`))", {}),
    "si_tr_r4_alter_add_projection_date_in_v2_rejected": (None, PROJ + "date IN (`db2.x`))", {}),
    "si_tr_r4_alter_add_projection_not_callable_in_rejected": (None, PROJ + "not in(1, `db2.x`))", {}),
    "si_tr_r4_alter_add_projection_not_callable_in_v2_rejected": (None, PROJ + "not in(1, `db2.x`))", {}),
    "si_tr_r5_alter_add_projection_plus_in_rejected": (None, PROJ + "a IN +`db2.x`)", {}),
    "si_tr_r5_alter_add_projection_plus_in_v2_rejected": (None, PROJ + "a IN +`db2.x`)", {}),
    "si_tr_r5_alter_add_projection_in_array_literal_allowed": (None, PROJ + "a IN [1, 2])",
        {"want_sql": 'ALTER TABLE phys."db1.o" ADD PROJECTION p(SELECT a ORDER BY a IN[1, 2])'}),
    "si_tr_r5_alter_add_projection_in_array_literal_v2_allowed": (None, PROJ + "a IN [1, 2])",
        {"want_sql": 'ALTER TABLE phys."db1.o" ADD PROJECTION p(SELECT a ORDER BY a IN[1, 2])'}),
    "si_tr_r5_alter_add_projection_in_array3_literal_allowed": (None, PROJ + "a IN [1, 2, 3])",
        {"want_sql": 'ALTER TABLE phys."db1.o" ADD PROJECTION p(SELECT a ORDER BY a IN[1, 2, 3])'}),
    "si_tr_r5_alter_add_projection_in_array3_literal_v2_allowed": (None, PROJ + "a IN [1, 2, 3])",
        {"want_sql": 'ALTER TABLE phys."db1.o" ADD PROJECTION p(SELECT a ORDER BY a IN[1, 2, 3])'}),
    "si_tr_settings_plain_values_allowed": (None,
        "SELECT * FROM db1.o SETTINGS max_threads = 1, join_algorithm = 'hash', use_query_cache = true",
        {"want_sql": "SELECT * FROM phys.\"db1.o\" \"db1.o\" SETTINGS max_threads = 1, join_algorithm = 'hash', use_query_cache = TRUE"}),
    "si_tr_settings_plain_values_v2_allowed": (None,
        "SELECT * FROM db1.o SETTINGS max_threads = 1, join_algorithm = 'hash', use_query_cache = true",
        {"want_sql": "SELECT * FROM phys.\"db1.o\" \"db1.o\" SETTINGS max_threads = 1, join_algorithm = 'hash', use_query_cache = TRUE"}),
    "si_tr_set_plain_values_passthrough": (None,
        "SET max_threads = 1, max_block_size = 'a', use_query_cache = true",
        {"want_sql": "SET max_threads = 1, max_block_size = 'a', use_query_cache = true"}),
    "si_tr_settings_subquery_value_rejected": ("si_tr_settings_map_value_rejected",
        "SELECT * FROM db1.o SETTINGS max_threads = {'a': 'b'}", {}),
    "si_tr_settings_subquery_value_v2_rejected": ("si_tr_settings_map_value_v2_rejected",
        "SELECT * FROM db1.o SETTINGS max_threads = {'a': 'b'}", {}),
    "si_tr_set_subquery_value_rejected": ("si_tr_set_map_value_rejected",
        "SET max_threads = {'a': 'b'}", {}),
    "si_tr_set_subquery_value_v2_rejected": ("si_tr_set_map_value_v2_rejected",
        "SET max_threads = {'a': 'b'}", {}),
}

# (G1, G4, G5) Pins that change with the native fixes.
REPIN = {
    "si_tr_ctas_empty_own_source_rewritten": {
        "want_sql": 'CREATE TABLE phys."db1.n" ENGINE=Memory EMPTY AS (SELECT * FROM phys."db1.p" "db1.p")',
        "want_table_rewrites": {"db1.n": "phys.db1.n", "db1.p": "phys.db1.p"},
        "want_accessed": [acc("db1", "n", "db1", "phys"), acc("db1", "p", "db1", "phys")],
    },
    "si_tr_in_nested_paren_hg_safe_v2_rejected": {
        "want_code": "RewriteError",
        "want_message_contains": "storage-integrity physical table hg_safe.db1__t is not directly addressable",
        "want_accessed": [acc("db1", "o", "db1", "phys"), acc("hg_safe", "db1__t", "", "hg_safe", si=True)],
    },
    "si_tr_create_default_hg_safe_subquery_v2_rejected": {
        "want_message_contains": "storage-integrity physical table hg_safe.db1__t is not directly addressable"},
    "si_tr_create_default_hg_promote_subquery_v2_rejected": {
        "want_message_contains": "storage-integrity physical table hg_promote.x is not directly addressable"},
    "si_tr_describe_subquery_phys_v2_rejected": {"want_message_contains": SI_CATCH_ALL},
    "si_tr_describe_subquery_hg_safe_v2_rejected": {
        "want_message_contains": "storage-integrity physical table hg_safe.db1__t is not directly addressable"},
    "si_tr_describe_table_subquery_v2_rejected": {"want_message_contains": SI_CATCH_ALL},
}

# (G1) New case, inserted right after its V1 sibling; dynamic args and the
# accessed / table-rewrite pins copied from si_tr_ctas_active_source_derived_read.
INSERT_AFTER = {
    "si_tr_ctas_empty_own_source_rewritten": ("si_tr_ctas_active_source_derived_read", {
        "name": "si_tr_ctas_empty_active_source_derived_read",
        "sql": "CREATE TABLE db1.n ENGINE = Memory EMPTY AS SELECT * FROM db1.t",
        "want_sql": 'CREATE TABLE phys."db1.n" ENGINE=Memory EMPTY AS (SELECT * FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS "db1.t")',
    }),
}

GROUPS = {
    # Task 2 (G1): CREATE TABLE … EMPTY AS SELECT keeps and rewrites its body.
    "empty": {"repin": {"si_tr_ctas_empty_own_source_rewritten"},
              "insert": {"si_tr_ctas_empty_own_source_rewritten"}},
    # Task 3 (G4): a parenthesized IN operand is an SI-handler position.
    "blind": {"repin": {"si_tr_in_nested_paren_hg_safe_v2_rejected"}},
    # Task 4 (G5): SI-active refusals name the SI object or use the catch-all.
    "simsg": {"repin": {
        "si_tr_create_default_hg_safe_subquery_v2_rejected",
        "si_tr_create_default_hg_promote_subquery_v2_rejected",
        "si_tr_describe_subquery_phys_v2_rejected",
        "si_tr_describe_subquery_hg_safe_v2_rejected",
        "si_tr_describe_table_subquery_v2_rejected"}},
    # Task 6 (G3): inputs ClickHouse cannot parse, replaced.
    "chsql": {"replace": set(REPLACE)},
    # Task 7 (G2, G3): engine-local shapes leave the shared corpus.
    "local": {"delete": set(DELETE)},
}
if GROUP not in GROUPS:
    sys.exit("group must be one of %s" % sorted(GROUPS))
sel = GROUPS[GROUP]
DELETE = sel.get("delete", set())
REPLACE = {k: v for k, v in REPLACE.items() if k in sel.get("replace", set())}
REPIN = {k: v for k, v in REPIN.items() if k in sel.get("repin", set())}
INSERT_AFTER = {k: v for k, v in INSERT_AFTER.items() if k in sel.get("insert", set())}

elems = list(elements(raw))
by_name = {obj["name"]: obj for _, _, obj in elems}
missing = (DELETE | set(REPLACE) | set(REPIN) | set(INSERT_AFTER)) - set(by_name)
if missing:
    sys.exit("unknown case names: %s" % sorted(missing))

out, cursor = [], 0
for start, end, obj in elems:
    name = obj["name"]
    if name in DELETE:
        # Drop the element together with the separator that precedes it
        # (the first element of the array is never deleted).
        prev = raw.rindex("}", 0, start) + 1
        out.append(raw[cursor:prev])
        cursor = end
        continue
    if name in REPLACE or name in REPIN:
        new = dict(obj)
        if name in REPLACE:
            new_name, sql, extra = REPLACE[name]
            if new_name:
                new["name"] = new_name
            new["sql"] = sql
            new.update(extra)
        new.update(REPIN.get(name, {}))
        out.append(raw[cursor:start])
        out.append(emit(new))
        cursor = end
    if name in INSERT_AFTER:
        template_name, fields = INSERT_AFTER[name]
        template = dict(by_name[template_name])
        template.update(fields)
        out.append(raw[cursor:end])
        out.append(",\n  " + emit(template))
        cursor = end
out.append(raw[cursor:])
text = "".join(out)
json.loads(text)  # still one valid JSON value
open(PATH, "w", encoding="utf-8").write(text)
data = text.encode("utf-8")
fnv = 14695981039346656037
for b in data:
    fnv = ((fnv ^ b) * 1099511628211) & 0xFFFFFFFFFFFFFFFF
print("SICorpusFingerprint uint64 = %d" % fnv)
print("SICorpusBytes       int    = %d" % len(data))
print("SICorpusCases       int    = %d" % len(json.loads(text)))
```

- [ ] **Step 3: Prove the script reaches the target bytes (dry run on a copy)**

```bash
cp internal/harness/testdata/storage_integrity_cases.json tmp/dry.json
for g in empty blind simsg chsql local; do python3 tmp/corpus_edit.py tmp/dry.json $g; done
shasum -a 256 tmp/dry.json && rm tmp/dry.json
```

Expected: the last group prints `SICorpusFingerprint uint64 = 10342073271660396945`, `SICorpusBytes       int    = 872362`, `SICorpusCases       int    = 966`, and the sha256 is `76d8db60921387db62452fd27661211da62e06fbbba894c90af5f7d7ccd230e0`. Nothing to commit.

---

### Task 1: the differential compares a rejection's AccessedTables only when pinned

**Files:**
- Modify: `internal/harness/storage_integrity_golden_test.go` (the oracle branch of `TestStorageIntegrityGolden`; a new helper; a new test)

**Interfaces:**
- Produces: `alignUnpinnedOracleFields(c SICase, got, want *pb.RewriteSQLResponse) (semantic bool)` (package `harness`, test file).

- [ ] **Step 1: Write the failing test** (append to `internal/harness/storage_integrity_golden_test.go`)

```go
func TestAlignUnpinnedOracleFields(t *testing.T) {
	native := func() *pb.RewriteSQLResponse {
		return &pb.RewriteSQLResponse{
			Code: pb.RewriteCode_UnsupportedStatement, SqlAfterRewrite: "TRUNCATE DATABASE hg_safe",
			OriginalAccessedTables: []*pb.AccessedTable{},
		}
	}
	oracle := &pb.RewriteSQLResponse{
		Code: pb.RewriteCode_UnsupportedStatement, SqlAfterRewrite: "TRUNCATE DATABASE hg_safe",
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "hg_safe", PhysicalDatabase: "hg_safe", IsStorageIntegrity: true}},
	}

	got := native()
	alignUnpinnedOracleFields(SICase{Reject: true}, got, oracle)
	if d := Compare(got, oracle, nil); !d.Equal() {
		t.Fatalf("unpinned rejection accessed list is not a contract: %v", d.Mismatches)
	}

	got = native()
	alignUnpinnedOracleFields(SICase{Reject: true, WantAccessed: []accessedJSON{}}, got, oracle)
	if d := Compare(got, oracle, nil); d.Equal() {
		t.Fatal("a pinned rejection accessed list must still be diffed")
	}

	got = native()
	got.Code = pb.RewriteCode_Success
	success := &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: got.SqlAfterRewrite,
		OriginalAccessedTables: oracle.OriginalAccessedTables}
	alignUnpinnedOracleFields(SICase{}, got, success)
	if d := Compare(got, success, nil); d.Equal() {
		t.Fatal("a success accessed list must always be diffed")
	}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/harness/ -run TestAlignUnpinnedOracleFields -count=1`
Expected: FAIL to compile, `undefined: alignUnpinnedOracleFields`.

- [ ] **Step 3: Implement.** In `TestStorageIntegrityGolden`, replace

```go
				got := pbFromResult(res)
				cmpEq := semEq
				if c.Reject || c.AllowSQLDivergence {
					got.SqlAfterRewrite = want.GetSqlAfterRewrite()
					if got.SqlAfterRewrite == "" {
						cmpEq = nil
					}
				}
				if d := Compare(got, want, cmpEq); !d.Equal() {
```

with

```go
				got := pbFromResult(res)
				cmpEq := semEq
				if !alignUnpinnedOracleFields(*c, got, want) {
					cmpEq = nil
				}
				if d := Compare(got, want, cmpEq); !d.Equal() {
```

and insert this helper immediately above `// writeSICorpus rewrites the corpus file deterministically.`:

```go
// alignUnpinnedOracleFields copies into got the oracle's value of every field
// the shared corpus deliberately leaves engine-specific, so the differential
// diffs exactly what the corpus pins:
//   - the SQL of a rejection (both runners check that it echoes the input)
//     and of a declared allow_sql_divergence case (pinned per engine);
//   - the AccessedTables of a rejection whose case pins no want_accessed:
//     spec 2026-09-26 §5 lets a rejection report a partial list, so only a
//     pinned list is a cross-engine contract.
//
// It reports whether the SQL is still compared semantically; false means the
// SQL was aligned to an empty oracle string and must be compared exactly.
func alignUnpinnedOracleFields(c SICase, got, want *pb.RewriteSQLResponse) (semantic bool) {
	semantic = true
	if c.Reject || c.AllowSQLDivergence {
		got.SqlAfterRewrite = want.GetSqlAfterRewrite()
		if got.SqlAfterRewrite == "" {
			semantic = false
		}
	}
	if c.Reject && c.WantAccessed == nil {
		got.OriginalAccessedTables = want.GetOriginalAccessedTables()
	}
	return semantic
}

```

- [ ] **Step 4: Run the harness package**

Run: `go test ./internal/harness/ -run 'TestAlignUnpinnedOracleFields|TestStorageIntegrityGolden|TestSICorpus|TestWriteSICorpus' -count=1`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/harness/storage_integrity_golden_test.go
git commit -m "test(harness): diff a rejection's accessed tables only where the corpus pins them

Spec 2026-09-26 §5 lets a rejection report a partial AccessedTables list;
the C++ runner already checks it only when want_accessed is present. The
differential now applies the same rule, so an unpinned rejection list is
not a cross-engine divergence. Success lists stay diffed, in order.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `CREATE TABLE … EMPTY AS SELECT` keeps, rewrites and reports its body (G1)

**Files:**
- Create: `internal/engine/create_empty.go`, `internal/engine/create_empty_test.go`
- Modify: `native.go` (`doRewrite` becomes a wrapper; the old body becomes `rewriteStatement`; new `sealCreateTableEmpty`)
- Modify: `internal/engine/writes.go` (`ExtractCreateSelectBody` comment), `internal/handlers/writes.go` (`dispatchCreateTable` and `rewriteEmbeddedBody` comments)
- Modify: `native_tableref_test.go` (`TestTableRef_EmbeddedSourcesAreRewrittenAndReported`)
- Modify: corpus (group `empty`), `internal/harness/sicorpus_test.go`

**Interfaces:**
- Produces: `engine.StripCreateTableEmpty(e Engine, sql string) (stripped string, ok bool, err error)`, `engine.InsertCreateTableEmpty(e Engine, generated string) (string, error)`.

Background, measured on Polyglot v0.12.1. `CREATE TABLE db1.n ENGINE = Memory EMPTY AS SELECT * FROM db1.p` parses with `create_table.as_select = null`: `EMPTY` lexes as `VAR` and everything after it is lost. The generator then prints `CREATE TABLE db1.n ENGINE=Memory`, and ClickHouse 26.3 refuses that output with `INCORRECT_QUERY`. Without `EMPTY`, the same statement parses with its body. ClickHouse 26.3 accepts and executes `CREATE TABLE phys."db1.n" ENGINE=Memory EMPTY AS (SELECT * FROM phys."db1.p" "db1.p")` (0 rows, columns from the source), and `… EMPTY AS ((SELECT …)) COMMENT 'c'` too.

- [ ] **Step 1: Write the failing engine test** — create `internal/engine/create_empty_test.go`:

```go
package engine

import "testing"

func TestStripCreateTableEmpty(t *testing.T) {
	e := newTestEngine(t)
	for _, tc := range []struct {
		name, sql, want string
		ok              bool
	}{
		{"empty modifier", "CREATE TABLE db1.n ENGINE = Memory EMPTY AS SELECT * FROM db1.p",
			"CREATE TABLE db1.n ENGINE = Memory  AS SELECT * FROM db1.p", true},
		{"lower-case modifier", "create table db1.n engine = Memory empty as select 1",
			"create table db1.n engine = Memory  as select 1", true},
		{"table named empty keeps its name", "CREATE TABLE db1.empty ENGINE = Memory EMPTY AS SELECT 1",
			"CREATE TABLE db1.empty ENGINE = Memory  AS SELECT 1", true},
		{"plain ctas is untouched", "CREATE TABLE db1.n ENGINE = Memory AS SELECT 1", "", false},
		{"table named empty without modifier", "CREATE TABLE db1.empty AS SELECT 1", "", false},
		{"clone of a table named empty", "CREATE TABLE db1.n AS db1.empty", "", false},
		{"quoted identifier is not the keyword", "CREATE TABLE db1.n (`EMPTY` UInt8) ENGINE = Memory", "", false},
		{"not a create table", "SELECT 1 AS empty", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := StripCreateTableEmpty(e, tc.sql)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !ok {
				if got != tc.sql {
					t.Fatalf("unmatched statement changed: %q", got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("stripped = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestInsertCreateTableEmpty(t *testing.T) {
	e := newTestEngine(t)
	for _, tc := range []struct{ name, generated, want string }{
		{"parenthesized body", `CREATE TABLE phys."db1.n" ENGINE=Memory AS (SELECT * FROM phys."db1.p" "db1.p")`,
			`CREATE TABLE phys."db1.n" ENGINE=Memory EMPTY AS (SELECT * FROM phys."db1.p" "db1.p")`},
		{"trailing comment", `CREATE TABLE phys."db1.n" ENGINE=Memory AS ((SELECT 1)) COMMENT 'c'`,
			`CREATE TABLE phys."db1.n" ENGINE=Memory EMPTY AS ((SELECT 1)) COMMENT 'c'`},
		{"column list and a body alias", `CREATE TABLE phys."db1.n" (a UInt64 DEFAULT 1) ENGINE=Memory AS SELECT 1 AS a`,
			`CREATE TABLE phys."db1.n" (a UInt64 DEFAULT 1) ENGINE=Memory EMPTY AS SELECT 1 AS a`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := InsertCreateTableEmpty(e, tc.generated)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
	for _, generated := range []string{
		`CREATE TABLE phys."db1.n" ENGINE=Memory`,
		`SELECT 1`,
	} {
		if got, err := InsertCreateTableEmpty(e, generated); err == nil {
			t.Fatalf("%q: got %q, want an error", generated, got)
		}
	}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go vet ./internal/engine/`
Expected: `undefined: StripCreateTableEmpty`.

- [ ] **Step 3: Implement** — create `internal/engine/create_empty.go`:

```go
package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// StripCreateTableEmpty recognises `CREATE TABLE … EMPTY AS SELECT …`, whose
// body the pinned polyglot drops on parse (create_table.as_select is null
// and the generator emits the statement without it). It returns sql with the
// EMPTY keyword removed, so the ordinary pipeline sees an ordinary CREATE
// TABLE … AS SELECT and rewrites and reports its body (spec 2026-09-26 §5,
// the INSERT … SELECT / CREATE TABLE … AS SELECT row: "incl. EMPTY"), and
// ok=true. InsertCreateTableEmpty puts the keyword back into the rewritten
// SQL. ok=false, with sql unchanged, for every other statement. err is a
// tokenizer failure on a body-less create_table, which the caller refuses.
func StripCreateTableEmpty(e Engine, sql string) (stripped string, ok bool, err error) {
	if !strings.Contains(strings.ToUpper(sql), "EMPTY") {
		return sql, false, nil // the keyword cannot be there; skip the extra parse
	}
	ast, perr := e.ParseOne(sql)
	if perr != nil {
		return sql, false, nil // the caller reports the SyntaxError itself
	}
	kind, body, _, berr := bodyOf(ast)
	if berr != nil || kind != NodeCreateTable || body == nil {
		return sql, false, nil
	}
	if q, _ := body["as_select"].(map[string]any); q != nil {
		return sql, false, nil
	}
	toks, terr := tokenizeRaw(e, sql)
	if terr != nil {
		return sql, false, terr
	}
	name, _ := json.Marshal(body["name"])
	depth := 0
	for i := 0; i+1 < len(toks); i++ {
		switch toks[i].TokenType {
		case "L_PAREN":
			depth++
			continue
		case "R_PAREN":
			depth--
			continue
		}
		if depth != 0 || toks[i].TokenType != "VAR" || !strings.EqualFold(toks[i].Text, "EMPTY") ||
			toks[i+1].TokenType != "AS" {
			continue
		}
		candidate := sql[:toks[i].Span.Start] + sql[toks[i].Span.End:]
		cast, cerr := e.ParseOne(candidate)
		if cerr != nil {
			continue
		}
		ckind, cbody, _, cberr := bodyOf(cast)
		if cberr != nil || ckind != NodeCreateTable || cbody == nil {
			continue
		}
		if q, _ := cbody["as_select"].(map[string]any); q == nil {
			continue
		}
		// EMPTY must be the modifier, not the table's own name: the name
		// (spans included, since it precedes the keyword) must not move.
		if cname, _ := json.Marshal(cbody["name"]); !bytes.Equal(name, cname) {
			continue
		}
		return candidate, true, nil
	}
	return sql, false, nil
}

// createTableEmptySentinel marks the body position when InsertCreateTableEmpty
// regenerates a statement's head.
const createTableEmptySentinel = "'hg_create_table_empty_sentinel'"

// InsertCreateTableEmpty re-inserts EMPTY in front of the AS that opens the
// body of a generated CREATE TABLE … AS SELECT. The generated statement is
// reparsed and regenerated with a sentinel body; the text before the " AS "
// that opens the sentinel must be an exact prefix of the generated SQL and be
// followed by " AS ", or the output is refused rather than guessed at. The
// generator may place trailing properties such as COMMENT after the body;
// ClickHouse 26.3 accepts `… EMPTY AS (SELECT …) COMMENT 'c'`.
func InsertCreateTableEmpty(e Engine, generated string) (string, error) {
	ast, err := e.ParseOne(generated)
	if err != nil {
		return "", fmt.Errorf("engine: reparse CREATE TABLE … EMPTY output: %w", err)
	}
	kind, body, root, err := bodyOf(ast)
	if err != nil || kind != NodeCreateTable || body == nil {
		return "", fmt.Errorf("engine: CREATE TABLE … EMPTY output is not a create_table")
	}
	if q, _ := body["as_select"].(map[string]any); q == nil {
		return "", fmt.Errorf("engine: CREATE TABLE … EMPTY output lost its body")
	}
	sentinel, err := e.ParseOne("SELECT " + createTableEmptySentinel)
	if err != nil {
		return "", fmt.Errorf("engine: parse CREATE TABLE … EMPTY sentinel: %w", err)
	}
	var node map[string]any
	if err := json.Unmarshal(sentinel, &node); err != nil {
		return "", fmt.Errorf("engine: decode CREATE TABLE … EMPTY sentinel: %w", err)
	}
	body["as_select"] = node
	probe, err := json.Marshal(root)
	if err != nil {
		return "", fmt.Errorf("engine: encode CREATE TABLE … EMPTY probe: %w", err)
	}
	shape, err := e.Generate(AST(probe))
	if err != nil {
		return "", fmt.Errorf("engine: generate CREATE TABLE … EMPTY probe: %w", err)
	}
	mark := strings.Index(shape, createTableEmptySentinel)
	at := -1
	if mark >= 0 {
		at = strings.LastIndex(shape[:mark], " AS ")
	}
	if at < 0 || !strings.HasPrefix(generated, shape[:at]) || !strings.HasPrefix(generated[at:], " AS ") {
		return "", fmt.Errorf("engine: cannot locate the body of %q", generated)
	}
	return generated[:at] + " EMPTY" + generated[at:], nil
}
```

- [ ] **Step 4: Run the engine tests**

Run: `go test ./internal/engine/ -run 'CreateTableEmpty' -count=1`
Expected: `ok`.

- [ ] **Step 5: Write the failing native rows.** In `native_tableref_test.go`, `TestTableRef_EmbeddedSourcesAreRewrittenAndReported`, replace

```go
		{name: "ctas empty drops the body", sql: "CREATE TABLE db1.n ENGINE = Memory EMPTY AS SELECT * FROM db1.p", wantCode: pb.RewriteCode_Success,
			wantSQL: `CREATE TABLE phys."db1.n" ENGINE=Memory`, wantAcc: []string{"db1.n"}},
```

with

```go
		{name: "ctas empty keeps and rewrites the body", sql: "CREATE TABLE db1.n ENGINE = Memory EMPTY AS SELECT * FROM db1.p", wantCode: pb.RewriteCode_Success,
			wantSQL: `CREATE TABLE phys."db1.n" ENGINE=Memory EMPTY AS (SELECT * FROM phys."db1.p" "db1.p")`, wantAcc: []string{"db1.n", "db1.p"}},
		{name: "ctas empty table named empty", sql: "CREATE TABLE db1.empty ENGINE = Memory EMPTY AS SELECT * FROM db1.p", wantCode: pb.RewriteCode_Success,
			wantSQL: `CREATE TABLE phys."db1.empty" ENGINE=Memory EMPTY AS (SELECT * FROM phys."db1.p" "db1.p")`, wantAcc: []string{"db1.empty", "db1.p"}},
		{name: "ctas empty comment before empty", sql: "CREATE TABLE db1.n ENGINE = Memory COMMENT 'c' EMPTY AS SELECT * FROM db1.p", wantCode: pb.RewriteCode_Success,
			wantSQL: `CREATE TABLE phys."db1.n" ENGINE=Memory EMPTY AS (SELECT * FROM phys."db1.p" "db1.p") COMMENT 'c'`, wantAcc: []string{"db1.n", "db1.p"}},
		{name: "ctas empty protected source refused", sql: "CREATE TABLE db1.n ENGINE = Memory EMPTY AS SELECT * FROM phys.`db2.x`",
			wantCode: pb.RewriteCode_InvalidRewriteRequest, wantMsg: "protected database phys is not addressable",
			wantSQL: "CREATE TABLE db1.n ENGINE = Memory EMPTY AS SELECT * FROM phys.`db2.x`"},
		{name: "ctas empty active source", sql: "CREATE TABLE db1.n ENGINE = Memory EMPTY AS SELECT * FROM db1.t", si: true, wantCode: pb.RewriteCode_Success,
			wantSQL: `CREATE TABLE phys."db1.n" ENGINE=Memory EMPTY AS (SELECT * FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS "db1.t")`, wantAcc: []string{"db1.n", "db1.t"}},
```

- [ ] **Step 6: Run and watch them fail**

Run: `go test . -run TestTableRef_EmbeddedSourcesAreRewrittenAndReported -count=1`
Expected: FAIL on the five `ctas empty …` rows (the body is dropped).

- [ ] **Step 7: Implement the `doRewrite` wrapper in `native.go`.** The existing function body becomes `rewriteStatement`, and a new `doRewrite` wraps it. Replace this block (the doc comment, the signature and the first statement of today's `doRewrite`):

```go
// doRewrite is the engine-level rewrite pipeline shared by NativeRewriter
// (per-connection, options via callback) and Service (stateless, options
// from the request). A non-nil error means an unexpected/internal failure on
// a static or no-rewrite request; a dynamic-mode request never returns one
// (sealStorageIntegrityHandlerError turns it into an UnsupportedStatement
// response). Rewrite rejections travel inside the response Code.
func doRewrite(e engine.Engine, sql string, opts []*pb.RewriteOption) (*pb.RewriteSQLResponse, error) {
	resp := &pb.RewriteSQLResponse{SqlAfterRewrite: sql} // SQL always set; echoes input
```

with the following. The final four lines open the renamed function; everything after them in the old body stays as it is.

```go
// doRewrite is the engine-level rewrite pipeline shared by NativeRewriter
// (per-connection, options via callback) and Service (stateless, options
// from the request). A non-nil error means an unexpected/internal failure on
// a static or no-rewrite request; a dynamic-mode request never returns one
// (sealStorageIntegrityHandlerError turns it into an UnsupportedStatement
// response). Rewrite rejections travel inside the response Code.
//
// CREATE TABLE … EMPTY AS SELECT is rewritten without its EMPTY keyword,
// which polyglot cannot parse, and the keyword is put back into the result;
// a rejection echoes the caller's SQL (spec 2026-09-26 §5).
func doRewrite(e engine.Engine, sql string, opts []*pb.RewriteOption) (*pb.RewriteSQLResponse, error) {
	stripped, empty, stripErr := engine.StripCreateTableEmpty(e, sql)
	if stripErr == nil && !empty {
		return rewriteStatement(e, sql, opts)
	}
	text := sql
	if empty {
		text = stripped
	}
	resp, err := rewriteStatement(e, text, opts)
	if err != nil {
		return nil, err
	}
	if resp.GetCode() != pb.RewriteCode_Success {
		resp.SqlAfterRewrite = sql // a rejection echoes the caller's SQL
		return resp, nil
	}
	if stripErr == nil {
		out, insertErr := engine.InsertCreateTableEmpty(e, resp.GetSqlAfterRewrite())
		if insertErr == nil {
			resp.SqlAfterRewrite = out
			return resp, nil
		}
		stripErr = insertErr
	}
	return sealCreateTableEmpty(resp, sql, opts, stripErr)
}

// sealCreateTableEmpty refuses a CREATE TABLE … EMPTY AS SELECT whose EMPTY
// keyword could not be located or put back, instead of forwarding a statement
// without its body (spec 2026-09-26 §5: an engine-internal limit is a coded
// UnsupportedStatement in dynamic mode). Static and no-rewrite requests keep
// the legacy Go-error channel.
func sealCreateTableEmpty(resp *pb.RewriteSQLResponse, sql string, opts []*pb.RewriteOption, cause error) (*pb.RewriteSQLResponse, error) {
	if nameresolve.FindActive(opts).Mode != nameresolve.ModeDynamic {
		return nil, cause
	}
	msg := engine.UnsupportedStatementMessage
	if resp.GetStorageIntegrityContractVersion() != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED {
		msg = StorageIntegrityUnmodelledMessage
	}
	return &pb.RewriteSQLResponse{
		SqlAfterRewrite:                 sql,
		Code:                            pb.RewriteCode_UnsupportedStatement,
		Message:                         msg,
		ExistenceClause:                 resp.GetExistenceClause(),
		StorageIntegrityContractVersion: resp.GetStorageIntegrityContractVersion(),
	}, nil
}

// rewriteStatement is the single-statement pipeline doRewrite runs once the
// CREATE TABLE … EMPTY form has been normalised.
func rewriteStatement(e engine.Engine, sql string, opts []*pb.RewriteOption) (*pb.RewriteSQLResponse, error) {
	resp := &pb.RewriteSQLResponse{SqlAfterRewrite: sql} // SQL always set; echoes input
```

- [ ] **Step 8: Correct the three stale comments.**

In `internal/engine/writes.go`, `ExtractCreateSelectBody`, replace

```go
// parenthesization wrapper), or ok=false when absent. Empirically verified
// (Step 1): `EMPTY AS SELECT` carries no as_select at all — polyglot drops
// the body — so there is nothing to rewrite or report for that form. A
```

with

```go
// parenthesization wrapper), or ok=false when absent. polyglot drops the body
// of `EMPTY AS SELECT` on parse; doRewrite removes the EMPTY keyword first
// (StripCreateTableEmpty), so the body is present here for that form too. A
```

In `internal/handlers/writes.go`, `dispatchCreateTable`, replace

```go
// second half). `EMPTY AS SELECT` carries no as_select at all (Step 1), so the
// body step is a no-op for that form.
```

with

```go
// second half). `EMPTY AS SELECT` reaches this handler without its EMPTY
// keyword (doRewrite strips it and puts it back), so its body is rewritten too.
```

In `internal/handlers/writes.go`, `rewriteEmbeddedBody`, replace `returning has=false (VALUES / FORMAT / EMPTY AS SELECT / plain CREATE TABLE)` with `returning has=false (VALUES / FORMAT / plain CREATE TABLE)`.

- [ ] **Step 9: Run the native and engine tests**

Run: `go test . ./internal/engine/ ./internal/handlers/ -count=1`
Expected: `ok`. `go test ./internal/harness/ -run TestStorageIntegrityGolden -count=1` now FAILS only on `si_tr_ctas_empty_own_source_rewritten`: the corpus still pins the dropped body.

- [ ] **Step 10: Re-pin the corpus (group `empty`)**

```bash
python3 tmp/corpus_edit.py internal/harness/testdata/storage_integrity_cases.json empty
```

Expected output, which you then copy into the `const (…)` block of `internal/harness/sicorpus_test.go`:

```
SICorpusFingerprint uint64 = 410581697812268604
SICorpusBytes       int    = 918266
SICorpusCases       int    = 988
```

- [ ] **Step 11: Full suites**

Run: `go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1 && env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1`
Expected: all `ok`.

- [ ] **Step 12: Commit**

```bash
git add internal/engine/create_empty.go internal/engine/create_empty_test.go internal/engine/writes.go \
  internal/handlers/writes.go native.go native_tableref_test.go \
  internal/harness/testdata/storage_integrity_cases.json internal/harness/sicorpus_test.go
git commit -m "fix: CREATE TABLE … EMPTY AS SELECT keeps, rewrites and reports its body

Polyglot drops the body of EMPTY AS SELECT, so the engine emitted
CREATE TABLE phys.\"db1.n\" ENGINE=Memory, which ClickHouse 26.3 refuses,
and reported no source (spec 2026-09-26 §5 requires rewrite and report,
incl. EMPTY). doRewrite now rewrites the statement without EMPTY and puts
the keyword back; if it cannot, dynamic mode refuses the statement.
si_tr_ctas_empty_own_source_rewritten is re-pinned and
si_tr_ctas_empty_active_source_derived_read is added.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: a parenthesised IN operand is an ordinary SI-handler position (G4)

**Files:**
- Modify: `internal/engine/references.go` (`CollectDatabaseReferenceSets`; remove `collectParenthesizedInDatabases`)
- Modify: `internal/handlers/preflight.go` (T3 comment)
- Modify: `internal/engine/references_test.go`, `native_tableref_test.go`
- Modify: corpus (group `blind`), `internal/harness/sicorpus_test.go`

The "SI-handler-blind" treatment of `x IN (db.t)` predates the shared `decodeInOperand`, which now unwraps parentheses for every IN consumer, the SI handlers included. With the preflight no longer pre-empting it, every position is still refused, with the SI handler's own message. That is spec T3's rule and what C++ v0.15.0 already answers. The prototype confirmed it for every position in Step 1's new test.

- [ ] **Step 1: Write the failing tests**

In `internal/engine/references_test.go`, `TestCollectDatabaseReferenceSets_Blind`, replace

```go
		"SELECT * FROM db1.o WHERE a IN (hg_safe.x)":      {"hg_safe"},
		"SELECT * FROM db1.o WHERE a IN ((hg_safe.x))":    {"hg_safe"},
```

with

```go
		"SELECT * FROM db1.o WHERE a IN (hg_safe.x)":      nil,
		"SELECT * FROM db1.o WHERE a IN ((hg_safe.x))":    nil,
```

In `native_tableref_test.go`, `TestTableRef_ProtectedDatabasesAreRefusedEverywhere`, replace

```go
			isSIHandlerBlind := strings.Contains(shape, "joinGet") || strings.Contains(shape, "dictGet") ||
				strings.Contains(shape, "hasColumnInTable") || strings.Contains(shape, "IN (%s.")
```

with

```go
			isSIHandlerBlind := strings.Contains(shape, "joinGet") || strings.Contains(shape, "dictGet") ||
				strings.Contains(shape, "hasColumnInTable")
```

and in the comment block above it replace

```go
			//  1. siHandlerBlindShapes: the joinGet shape's database
			//     qualifier is a string-lookup argument, and the parenthesized
			//     IN shape is not is_field-tagged -- no existing SI handler in
			//     this repo classifies either position as a table reference,
			//     so nothing downstream would otherwise reject them.
```

with

```go
			//  1. siHandlerBlindShapes: the joinGet shape's database
			//     qualifier is a string-lookup argument -- no existing SI
			//     handler classifies that position as a table reference (a
			//     parenthesized IN operand is decoded for the SI handlers by
			//     decodeInOperand and gets their message, spec T3), so
			//     nothing downstream would otherwise reject it.
```

In `native_tableref_test.go`, `TestTableRef_InOperandsDecodeOnce`, replace

```go
			cases = append(cases, tablerefCase{name: c.sql, sql: c.sql, si: si,
				wantCode: pb.RewriteCode_InvalidRewriteRequest,
				wantMsg:  "protected database " + c.db + " is not addressable", wantSQL: c.sql})
```

with

```go
			code, msg := pb.RewriteCode_InvalidRewriteRequest, "protected database "+c.db+" is not addressable"
			if si && c.db == "hg_safe" {
				// A parenthesized IN operand is an ordinary SI-handler position:
				// the SI message keeps precedence (spec 2026-09-26 T3).
				code, msg = pb.RewriteCode_RewriteError, "storage-integrity physical table hg_safe.db1__t is not directly addressable"
			}
			cases = append(cases, tablerefCase{name: c.sql, sql: c.sql, si: si,
				wantCode: code, wantMsg: msg, wantSQL: c.sql})
```

Append to `native_tableref_test.go`:

```go
// TestTableRef_ParenthesizedInOperandUnderActiveSurface pins that dropping
// the parenthesized IN operand from the SI-handler-blind set opens nothing:
// with the SI surface active, every position that can hold `x IN ((hg_*.t))`
// is still refused, now with the SI handlers' own message (spec 2026-09-26 T3).
func TestTableRef_ParenthesizedInOperandUnderActiveSurface(t *testing.T) {
	const safe = "storage-integrity physical table hg_safe.db1__t is not directly addressable"
	var cases []tablerefCase
	for _, c := range []struct {
		sql  string
		code pb.RewriteCode
		msg  string
	}{
		{"SELECT * FROM db1.o WHERE a IN (hg_safe.db1__t)", pb.RewriteCode_RewriteError, safe},
		{"SELECT * FROM db1.o WHERE in(a, (hg_safe.db1__t))", pb.RewriteCode_RewriteError, safe},
		{"SELECT * FROM db1.o WHERE a GLOBAL IN ((hg_unsafe.db1__t))", pb.RewriteCode_RewriteError,
			"storage-integrity physical table hg_unsafe.db1__t is not directly addressable"},
		{"SELECT * FROM db1.o WHERE a NOT IN ((hg_promote.x))", pb.RewriteCode_RewriteError,
			"storage-integrity physical table hg_promote.x is not directly addressable"},
		{"ALTER TABLE db1.o DELETE WHERE a IN ((hg_safe.db1__t))", pb.RewriteCode_UnsupportedStatement, safe},
		{"ALTER TABLE db1.o UPDATE b = 1 WHERE a IN ((hg_safe.db1__t))", pb.RewriteCode_UnsupportedStatement, safe},
		{"DELETE FROM db1.o WHERE a IN ((hg_safe.db1__t))", pb.RewriteCode_UnsupportedStatement, safe},
		{"UPDATE db1.o SET b = 1 WHERE a IN ((hg_safe.db1__t))", pb.RewriteCode_UnsupportedStatement, safe},
		{"INSERT INTO db1.o SELECT * FROM db1.p WHERE a IN ((hg_safe.db1__t))", pb.RewriteCode_UnsupportedStatement, safe},
		{"CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM db1.p WHERE a IN ((hg_safe.db1__t))", pb.RewriteCode_UnsupportedStatement, safe},
		{"CREATE VIEW db1.v AS SELECT * FROM db1.p WHERE a IN ((hg_safe.db1__t))", pb.RewriteCode_UnsupportedStatement, safe},
		{"CREATE MATERIALIZED VIEW db1.mv TO db1.o AS SELECT * FROM db1.p WHERE a IN ((hg_safe.db1__t))", pb.RewriteCode_UnsupportedStatement, safe},
		{"CREATE TABLE db1.n (a UInt64, b UInt8 DEFAULT a IN ((hg_safe.db1__t))) ENGINE = Memory", pb.RewriteCode_UnsupportedStatement, safe},
		{"ALTER TABLE db1.o MODIFY COLUMN b UInt8 DEFAULT a IN ((hg_safe.db1__t))", pb.RewriteCode_UnsupportedStatement, safe},
		{"ALTER TABLE db1.o MODIFY TTL d + INTERVAL 1 DAY DELETE WHERE a IN ((hg_safe.db1__t))", pb.RewriteCode_UnsupportedStatement, safe},
	} {
		cases = append(cases, tablerefCase{name: c.sql, sql: c.sql, si: true, wantCode: c.code, wantMsg: c.msg, wantSQL: c.sql})
	}
	runTablerefCases(t, cases)
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test . ./internal/engine/ -run 'Blind|ProtectedDatabasesAreRefusedEverywhere|InOperandsDecodeOnce|ParenthesizedInOperandUnderActiveSurface' -count=1`
Expected: FAIL. `TestCollectDatabaseReferenceSets_Blind` fails on the two paren rows, and the three native tests fail with `code = InvalidRewriteRequest (protected database hg_… is not addressable)`.

- [ ] **Step 3: Implement.** In `internal/engine/references.go`, replace the doc paragraph of `CollectDatabaseReferenceSets`

```go
// the SI-handler-blind subset (blind): the databases named only through a
// position no storage-integrity handler classifies as a table reference — a
// joinGet/dictGet/hasColumnInTable-family argument (an ordinary scalar
// function call whose argument embeds a namespace) and a parenthesized
// single-operand IN (`x IN (db.table)`, at any paren depth; the SI handlers
// classify only the bare form). PreflightTableReferences refuses a protected
// blind hit even while the storage-integrity surface is active. Unqualified
// names contribute nothing: the logical context is checked by the caller.
```

with

```go
// the SI-handler-blind subset (blind): the databases named only through a
// position no storage-integrity handler classifies as a table reference — a
// joinGet/dictGet/hasColumnInTable-family argument (an ordinary scalar
// function call whose argument embeds a namespace). A parenthesized IN
// operand is not blind: the shared IN-operand decoder unwraps it for the SI
// handlers too, so they answer it with their own message (spec 2026-09-26
// T3). PreflightTableReferences refuses a protected blind hit even while the
// storage-integrity surface is active. Unqualified names contribute nothing:
// the logical context is checked by the caller.
```

Delete the line `	collectParenthesizedInDatabases(root, addBlind)` from `CollectDatabaseReferenceSets`. Delete the whole `collectParenthesizedInDatabases` function together with its doc comment, from `// collectParenthesizedInDatabases reports the database of every` through the function's closing `}`.

In `internal/handlers/preflight.go`, replace

```go
	// T3. A database named only through an SI-handler-blind position (a
	// string-lookup argument, a parenthesized IN operand) is refused even
	// while the storage-integrity surface is active, unlike an ordinary table
	// position, which defers to the SI handlers below.
```

with

```go
	// T3. A database named only through an SI-handler-blind position (a
	// string-lookup argument) is refused even while the storage-integrity
	// surface is active, unlike an ordinary table position (a parenthesized
	// IN operand included), which defers to the SI handlers below.
```

- [ ] **Step 4: Run the tests**

Run: `go test . ./internal/engine/ ./internal/handlers/ -count=1`
Expected: `ok`. The golden test now FAILS only on `si_tr_in_nested_paren_hg_safe_v2_rejected`.

- [ ] **Step 5: Re-pin the corpus (group `blind`)**

```bash
python3 tmp/corpus_edit.py internal/harness/testdata/storage_integrity_cases.json blind
```

Expected: `SICorpusFingerprint uint64 = 5718590109388997943`, `SICorpusBytes       int    = 918734`, `SICorpusCases       int    = 988`. Copy them into `sicorpus_test.go`.

- [ ] **Step 6: Full suites, then commit**

Run the three commands of Task 2 Step 11. Expected: all `ok`.

```bash
git add internal/engine/references.go internal/engine/references_test.go internal/handlers/preflight.go \
  native_tableref_test.go internal/harness/testdata/storage_integrity_cases.json internal/harness/sicorpus_test.go
git commit -m "fix: a parenthesized IN operand keeps the storage-integrity message

The preflight treated x IN (hg_*.t) as SI-handler-blind and answered it
with the protected-database message. decodeInOperand already unwraps the
parentheses for the SI handlers, which refuse every such position with
their own message, as spec 2026-09-26 T3 requires while the SI surface is
active (and as rewriter-grpc answers). Only string-lookup arguments stay
blind. si_tr_in_nested_paren_hg_safe_v2_rejected is re-pinned.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: SI-active refusals name the SI object or use the SI catch-all (G5)

**Files:**
- Modify: `internal/handlers/storage_integrity_reject.go` (`AnnotateStorageIntegrityRejectAST`: structural fallback)
- Modify: `internal/engine/objtarget.go` (`ObjectTarget.SubqueryStart`)
- Modify: `internal/handlers/describe.go` (SI-active message; `describeSubqueryStorageIntegrityTable`)
- Modify: `internal/handlers/storage_integrity_reject_test.go`, `native_tableref_test.go`
- Modify: corpus (group `simsg`), `internal/harness/sicorpus_test.go`

- [ ] **Step 1: Write the failing tests**

In `internal/handlers/storage_integrity_reject_test.go`, `TestAnnotateStorageIntegrityReject_ProvenD2Targets`, append after the `"reserved row id has no Task2 precedence"` row:

```go
		{"column default subquery physical table", "CREATE TABLE other.n (a UInt64 DEFAULT (SELECT max(a) FROM hg_safe.db1__t)) ENGINE = Memory",
			"storage-integrity physical table hg_safe.db1__t is not directly addressable"},
		{"materialized column IN physical table", "CREATE TABLE other.n (a UInt64 MATERIALIZED a IN hg_unsafe.db1__t) ENGINE = Memory",
			"storage-integrity physical table hg_unsafe.db1__t is not directly addressable"},
```

In `TestAnnotateStorageIntegrityReject_UnprovenNamesKeepCallerMessage`, append after the `"alias qualified column"` row. These two rows pass before and after; they guard Review Focus 5:

```go
		{"alias qualified column in a plain statement", "SELECT hg_safe.x FROM other.u AS hg_safe"},
		{"alias qualified column in a column default", "CREATE TABLE other.n (a UInt64 DEFAULT hg_safe.x) ENGINE = Memory"},
```

In `native_tableref_test.go`, `TestTableRef_MutationAndColumnExpressionReads`, replace

```go
		{sql: "CREATE TABLE db1.n (a UInt64 DEFAULT (SELECT max(a) FROM hg_promote.x)) ENGINE = Memory", code: pb.RewriteCode_InvalidRewriteRequest, msgOff: protected("hg_promote"),
			setCode: true, codeOn: pb.RewriteCode_UnsupportedStatement, msgOn: unsupported},
		{sql: "CREATE TABLE db1.n (a UInt64 DEFAULT (SELECT max(a) FROM hg_safe.db1__t)) ENGINE = Memory", code: pb.RewriteCode_InvalidRewriteRequest, msgOff: protected("hg_safe"),
			setCode: true, codeOn: pb.RewriteCode_UnsupportedStatement, msgOn: unsupported},
```

with

```go
		{sql: "CREATE TABLE db1.n (a UInt64 DEFAULT (SELECT max(a) FROM hg_promote.x)) ENGINE = Memory", code: pb.RewriteCode_InvalidRewriteRequest, msgOff: protected("hg_promote"),
			setCode: true, codeOn: pb.RewriteCode_UnsupportedStatement, msgOn: "storage-integrity physical table hg_promote.x is not directly addressable"},
		{sql: "CREATE TABLE db1.n (a UInt64 DEFAULT (SELECT max(a) FROM hg_safe.db1__t)) ENGINE = Memory", code: pb.RewriteCode_InvalidRewriteRequest, msgOff: protected("hg_safe"),
			setCode: true, codeOn: pb.RewriteCode_UnsupportedStatement, msgOn: "storage-integrity physical table hg_safe.db1__t is not directly addressable"},
```

In `native_tableref_test.go`, `TestTableRef_DescribeAndShowTargets`, replace

```go
		for _, sql := range []string{
			"DESCRIBE (SELECT * FROM phys.`db2.x`)",
			"DESCRIBE (SELECT * FROM hg_safe.db1__t)",
			"DESCRIBE TABLE (SELECT * FROM phys.`db2.x`)",
			"DESC (SELECT a FROM db1.o)",
			"EXISTS",
			"SHOW CREATE",
		} {
			cases = append(cases, tablerefCase{name: sql, sql: sql, si: si,
				wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: unsupported, wantSQL: sql})
		}
```

with

```go
		// DESCRIBE (SELECT …) is an unmodelled shape refused before T3; with
		// the SI surface active it answers with the SI catch-all, upgraded to
		// the SI object its body names (spec 2026-09-26 §5 precedence).
		for _, c := range []struct{ sql, siMsg string }{
			{"DESCRIBE (SELECT * FROM phys.`db2.x`)", StorageIntegrityUnmodelledMessage},
			{"DESCRIBE (SELECT * FROM hg_safe.db1__t)", "storage-integrity physical table hg_safe.db1__t is not directly addressable"},
			{"DESCRIBE TABLE (SELECT * FROM phys.`db2.x`)", StorageIntegrityUnmodelledMessage},
			{"DESC (SELECT a FROM db1.o)", StorageIntegrityUnmodelledMessage},
			{"EXISTS", unsupported},
			{"SHOW CREATE", unsupported},
		} {
			msg := unsupported
			if si {
				msg = c.siMsg
			}
			cases = append(cases, tablerefCase{name: c.sql, sql: c.sql, si: si,
				wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: msg, wantSQL: c.sql})
		}
```

- [ ] **Step 2: Run and watch them fail**

Run: `go test . ./internal/handlers/ -run 'ProvenD2Targets|UnprovenNames|MutationAndColumnExpressionReads|DescribeAndShowTargets' -count=1`
Expected: FAIL on the two new ProvenD2 rows, the two V2 `CREATE TABLE … DEFAULT` rows, and the four V2 `DESCRIBE`/`DESC` rows (`want substring`, got `statement is not supported`).

- [ ] **Step 3: Implement the structural annotation fallback.** In `internal/handlers/storage_integrity_reject.go`, `AnnotateStorageIntegrityRejectAST`, append after the `for _, ref := range refs { … }` loop (still inside the function):

```go
	// A table the statement reads in a position no SI handler classifies
	// (a column DEFAULT / MATERIALIZED subquery, spec 2026-09-26 R2) is still
	// a proven table reference: spec 2026-09-26 §5 puts the SI message (T3)
	// ahead of the ordinary-stage refusal.
	if kind, kerr := engine.NodeKind(ast); kerr != nil || kind == engine.NodeCommand {
		return
	}
	tables, terr := engine.CollectSelectTables(ast)
	if terr != nil {
		return
	}
	for _, tt := range tables {
		if tt.DB == "" {
			continue
		}
		if nameresolve.IsStorageIntegrityPhysicalDatabase(tt.DB, sel.Dynamic) {
			resp.Message = nameresolve.StorageIntegrityPhysicalRejectMessage(qualify(tt.DB, tt.Table))
			return
		}
	}
```

- [ ] **Step 4: Implement the DESCRIBE change.** In `internal/engine/objtarget.go`, add the field after `Shape ObjectTargetShape`:

```go
	// SubqueryStart is the byte offset of the opening parenthesis when
	// Shape == ObjectTargetSubquery, so sql[SubqueryStart:] is the
	// parenthesized body (spec 2026-09-26 R7).
	SubqueryStart int
```

and in `ParseObjectTarget` replace

```go
	} else if i < len(toks) && toks[i].TokenType == "L_PAREN" {
		out.Shape = ObjectTargetSubquery
	}
```

with

```go
	} else if i < len(toks) && toks[i].TokenType == "L_PAREN" {
		out.Shape = ObjectTargetSubquery
		out.SubqueryStart = toks[i].Span.Start
	}
```

In `internal/handlers/describe.go`, `RewriteDescribe`, replace the line `		rejectUnsupported(resp, engine.UnsupportedStatementMessage)` inside the `t.Shape == engine.ObjectTargetSubquery || t.Shape == engine.ObjectTargetNone` branch with:

```go
		msg := engine.UnsupportedStatementMessage
		if nameresolve.StorageIntegritySurfaceActive(sel.Dynamic) {
			msg = nameresolve.StorageIntegrityUnmodelledMessage
			if t.Shape == engine.ObjectTargetSubquery {
				if tt, ok := describeSubqueryStorageIntegrityTable(e, sql[t.SubqueryStart:], sel); ok {
					msg = nameresolve.StorageIntegrityPhysicalRejectMessage(qualify(tt.DB, tt.Table))
				}
			}
		}
		rejectUnsupported(resp, msg)
```

and append to the file:

```go

// describeSubqueryStorageIntegrityTable reports the first table of a
// DESCRIBE (SELECT …) body that lives in a storage-integrity physical
// database, so the SI-surface refusal names the object (spec 2026-09-26 §5).
func describeSubqueryStorageIntegrityTable(e engine.Engine, body string, sel nameresolve.Selection) (engine.TableTarget, bool) {
	ast, err := e.ParseOne(body)
	if err != nil {
		return engine.TableTarget{}, false
	}
	tables, err := engine.CollectSelectTables(ast)
	if err != nil {
		return engine.TableTarget{}, false
	}
	for _, tt := range tables {
		if tt.DB != "" && nameresolve.IsStorageIntegrityPhysicalDatabase(tt.DB, sel.Dynamic) {
			return tt, true
		}
	}
	return engine.TableTarget{}, false
}
```

- [ ] **Step 5: Run the tests**

Run: `go test . ./internal/engine/ ./internal/handlers/ -count=1`
Expected: `ok`. The golden test now FAILS only on the five G5 cases.

- [ ] **Step 6: Re-pin the corpus (group `simsg`)**

```bash
python3 tmp/corpus_edit.py internal/harness/testdata/storage_integrity_cases.json simsg
```

Expected: `SICorpusFingerprint uint64 = 8206811517622278554`, `SICorpusBytes       int    = 920095`, `SICorpusCases       int    = 988`. Copy them into `sicorpus_test.go`.

- [ ] **Step 7: Full suites, then commit**

```bash
git add internal/handlers/storage_integrity_reject.go internal/handlers/storage_integrity_reject_test.go \
  internal/engine/objtarget.go internal/handlers/describe.go native_tableref_test.go \
  internal/harness/testdata/storage_integrity_cases.json internal/harness/sicorpus_test.go
git commit -m "fix: SI-active refusals name the SI object or use the SI catch-all

With the storage-integrity surface active, DESCRIBE (SELECT …) answered
with the inactive-surface text, and a column DEFAULT subquery over hg_*
answered with the ordinary-stage T7 text. Spec 2026-09-26 §5 puts the SI
message ahead of both (T3 with the SI messages first; every unmodelled
class answers with the SI catch-all). The final-response annotation now
also reads proven table positions of structured statements, and the
DESCRIBE subquery refusal names the SI table its body reads. Five _v2
cases are re-pinned to the answers rewriter-grpc already gives.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: the admitted session `SET` answers `success`

**Files:**
- Modify: `native.go` (`rewriteStatement` final pass-through), `native_tableref_test.go`

- [ ] **Step 1: Write the failing test.** In `TestTableRef_UnmodelledClassesAreRefusedWithoutSI`, replace

```go
		{name: "set passes when inactive", sql: "SET max_threads = 1", wantCode: pb.RewriteCode_Success, wantSQL: "SET max_threads = 1"},
```

with

```go
		{name: "set passes when inactive", sql: "SET max_threads = 1", wantCode: pb.RewriteCode_Success, wantMsg: "success", wantSQL: "SET max_threads = 1"},
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test . -run TestTableRef_UnmodelledClassesAreRefusedWithoutSI -count=1`
Expected: FAIL, `message = "", want substring "success"`.

- [ ] **Step 3: Implement.** In `native.go`, at the end of `rewriteStatement`, replace

```go
	resp.Code = pb.RewriteCode_Success
	finalize(resp, ast, sql, ec, siVersion, e, selection)
	return resp, nil
}
```

with

```go
	resp.Code = pb.RewriteCode_Success
	resp.Message = "success"
	finalize(resp, ast, sql, ec, siVersion, e, selection)
	return resp, nil
}
```

- [ ] **Step 4: Full suites, then commit**

Expected: all `ok`. No corpus change: the corpus does not pin success messages, and `si_tr_set_inactive_passthrough` now matches rewriter-grpc's `success`.

```bash
git add native.go native_tableref_test.go
git commit -m "fix: the admitted SET pass-through answers message \"success\"

Every other Success response carries \"success\"; the T7 SET carve-out
returned an empty message, a differential divergence against rewriter-grpc.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: corpus inputs that ClickHouse cannot parse are replaced (G3, 17 cases)

**Files:**
- Modify: corpus (group `chsql`), `internal/harness/sicorpus_test.go`

ClickHouse 26.3 has no projection `WHERE`, no bare-identifier setting value, and no `(SELECT …)` setting value; the only non-plain value it parses is a map literal. The replacements keep each case's rule (`statement is not supported` for a read in an opaque ALTER projection body; the plain-value carve-out; the non-plain-value refusal) in a form ClickHouse accepts. Go's answers on the new inputs are the pins the script writes. The prototype produced them and confirmed that ClickHouse parses every new input.

- [ ] **Step 1: Apply group `chsql`**

```bash
python3 tmp/corpus_edit.py internal/harness/testdata/storage_integrity_cases.json chsql
```

Expected: `SICorpusFingerprint uint64 = 1331193737883061380`, `SICorpusBytes       int    = 923635`, `SICorpusCases       int    = 988`. Copy them into `sicorpus_test.go`.

- [ ] **Step 2: Confirm ClickHouse parses every new input** (needs any `clickhouse` 26.3.x binary; on the build box use the image below)

```bash
python3 - <<'EOF'
import json
cs = json.load(open('internal/harness/testdata/storage_integrity_cases.json'))
names = ('add_projection', 'plain_values', 'map_value')
out = ["SET implicit_select = 0;"]
for c in cs:
    if c['name'].startswith('si_tr_') and any(n in c['name'] for n in names):
        out.append(f"SELECT '{c['name']}', formatQuerySingleLineOrNull($hgq${c['sql']}$hgq$) FORMAT TSV;")
open('tmp/chsql.sql', 'w').write('\n'.join(out) + '\n')
EOF
scp -P 30100 tmp/chsql.sql sentio@64.38.131.242:/tmp/chsql-$USER.sql
ssh -p 30100 sentio@64.38.131.242 "docker run --rm -v /tmp/chsql-$USER.sql:/w/q.sql --entrypoint clickhouse us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:26.3-lts-decimal512-release-2026-09-16 local --queries-file /w/q.sql; rm -f /tmp/chsql-$USER.sql"
```

Expected: 19 rows (the 17 replaced cases plus the unchanged `si_tr_alter_add_projection{,_v2}_allowed`), and no row ending in `\N`.

- [ ] **Step 3: Run the harness, then full suites**

Run: `go test ./internal/harness/ -count=1` and then Task 2 Step 11. Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add internal/harness/testdata/storage_integrity_cases.json internal/harness/sicorpus_test.go
git commit -m "test(corpus): replace inputs ClickHouse 26.3 cannot parse

A case whose input no ClickHouse server parses proves nothing about the
table-reference invariant, and rewriter-grpc (ClickHouse's parser) can only
answer SyntaxError. Projection bodies use ORDER BY instead of WHERE, the
plain-value carve-out uses use_query_cache = true instead of a bare
identifier, and the non-plain setting value is a map literal (renamed
si_tr_settings_map_value_* / si_tr_set_map_value_*).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: engine-local shapes leave the shared corpus (G2, G3; 22 cases)

**Files:**
- Modify: `native_tableref_test.go` (new `TestTableRef_EngineLocalShapes`)
- Modify: corpus (group `local`), `internal/harness/sicorpus_test.go`

Every removed case stays pinned by a native test, in both SI states:

| Removed cases | Native test that keeps the pin |
|---|---|
| `si_tr_goerr_*` (10) | `TestTableRef_GoErrorsFailClosed` |
| `si_tr_top_level_paren_select{,_v2}_rejected` | `TestTableRef_EngineLocalShapes` (new) |
| `si_tr_exists_empty*`, `si_tr_show_create_empty*` | `TestTableRef_DescribeAndShowTargets` |
| `si_tr_param_show_{columns,index}_from_rejected` | `TestTableRef_ParametersInTablePositionsAreRefused` |
| `si_tr_show_columns_like_subquery*` | `TestTableRef_ResidualShowColumnsBody` |
| `si_tr_r2_mv_refresh_to*` | `TestTableRef_Residual2RefreshPositions` |

- [ ] **Step 1: Add the relocation pin** (append to `native_tableref_test.go`)

```go
// TestTableRef_EngineLocalShapes pins native-engine behaviour the shared
// storage-integrity corpus deliberately does not: shapes the native engine
// does not model and refuses (spec 2026-09-26 §5: "where the engine models
// the position"), which rewriter-grpc parses into an ordinary AST.
func TestTableRef_EngineLocalShapes(t *testing.T) {
	var cases []tablerefCase
	for _, si := range []bool{false, true} {
		msg := "statement is not supported"
		if si {
			msg = StorageIntegrityUnmodelledMessage
		}
		for _, sql := range []string{
			"(SELECT * FROM db1.o)",
			"((SELECT * FROM db1.o))",
		} {
			cases = append(cases, tablerefCase{name: sql, sql: sql, si: si,
				wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: msg, wantSQL: sql})
		}
	}
	runTablerefCases(t, cases)
}
```

- [ ] **Step 2: Run it**

Run: `go test . -run TestTableRef_EngineLocalShapes -count=1`
Expected: `ok`. It pins existing behaviour, so it passes at once. This task moves pins rather than changing behaviour.

- [ ] **Step 3: Confirm every other removed shape is still pinned**

```bash
for s in 'SHOW COLUMNS FROM {p:Identifier}' 'SHOW INDEX FROM db1.{p:Identifier}' \
  'SHOW COLUMNS FROM o LIKE (SELECT max(name) FROM `db2.x`)' \
  'CREATE MATERIALIZED VIEW db1.mv REFRESH TO db1.t2 AS SELECT * FROM db1.o' \
  '"EXISTS",' '"SHOW CREATE",' 'arrayMap(x -> x IN (SELECT a FROM phys.`db2.x`), [1])' \
  'a = ANY (SELECT a FROM phys.`db2.x`)' "values('a UInt64', (SELECT max(a) FROM phys.\`db2.x\`))" \
  '"nested_in_60"' '"union_500"'; do grep -c -F -- "$s" native_tableref_test.go; done
```

Expected: every count ≥ 1.

- [ ] **Step 4: Apply group `local`**

```bash
python3 tmp/corpus_edit.py internal/harness/testdata/storage_integrity_cases.json local
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json
```

Expected: `SICorpusFingerprint uint64 = 10342073271660396945`, `SICorpusBytes       int    = 872362`, `SICorpusCases       int    = 966`; sha256 `76d8db60921387db62452fd27661211da62e06fbbba894c90af5f7d7ccd230e0`. Copy the constants into `sicorpus_test.go`.

- [ ] **Step 5: Full suites, then commit**

```bash
git add native_tableref_test.go internal/harness/testdata/storage_integrity_cases.json internal/harness/sicorpus_test.go
git commit -m "test(corpus): engine-local shapes move out of the shared corpus

Go walk and generator limits, a top-level parenthesized SELECT, and inputs
ClickHouse cannot parse (an empty EXISTS / SHOW CREATE, a parameter in
SHOW COLUMNS/INDEX FROM, SHOW COLUMNS LIKE (SELECT …), a bare REFRESH TO)
have no engine-neutral answer: spec 2026-09-26 §5 ties them to where an
engine models the position, and rewriter-grpc can only answer SyntaxError
for the unparseable ones. They stay pinned by native tests; 22 cases leave
the corpus (sha256 76d8db60921387db62452fd27661211da62e06fbbba894c90af5f7d7ccd230e0).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: documentation

**Files:**
- Modify: `AGENTS.md` ("Table-reference policy" section)
- Modify: `internal/harness/AGENTS.md` ("STORAGE-INTEGRITY CORPUS CONTRACT")

- [ ] **Step 1: `AGENTS.md`**

Replace ``- `internal/engine/references.go` — T3 database collection (`CollectDatabaseReferences`, `CollectDatabaseReferenceSets` — every database plus the SI-handler-blind subset) and the T6 string-lookup family.`` with:

```markdown
- `internal/engine/references.go` — T3 database collection (`CollectDatabaseReferences`, `CollectDatabaseReferenceSets` — every database plus the SI-handler-blind subset, which is the string-lookup arguments) and the T6 string-lookup family.
- `internal/engine/create_empty.go` — `StripCreateTableEmpty` / `InsertCreateTableEmpty`: `doRewrite` rewrites `CREATE TABLE … EMPTY AS SELECT` without its `EMPTY` keyword (Polyglot drops the body otherwise) and puts it back; if it cannot, dynamic mode refuses the statement.
```

In precedence step 3, replace `except in an SI-handler-blind position: a string-lookup argument or a parenthesized single-operand IN.` with `except in an SI-handler-blind position: a string-lookup argument. A parenthesized IN operand is decoded for the SI handlers by the shared IN-operand decoder, so it gets their message.`

In precedence step 8, replace ``; `DESCRIBE (SELECT …)` and an empty EXISTS / SHOW CREATE are T7;`` with ``; `DESCRIBE (SELECT …)` and an empty EXISTS / SHOW CREATE are T7 (with the surface active `DESCRIBE (SELECT …)` answers with the SI catch-all, or with the SI physical-table message when its body reads one);``

After the paragraph that begins `Any handler, walk or generate error`, add:

```markdown
With the surface active, the final-response annotation (`handlers.AnnotateStorageIntegrityRejectAST`) also names an SI physical table that a structured statement reads in a position no SI handler classifies (a column `DEFAULT` / `MATERIALIZED` subquery), so the T3 SI message precedes the R2 refusal there, as spec §5 orders them. It never names an unproven token: an alias-qualified column such as `hg_safe.x` keeps the caller's message.
```

- [ ] **Step 2: `internal/harness/AGENTS.md`.** After the R9 bullet (``- Every case whose name starts with `si_tr_` …``), add:

````markdown
- Every input is ClickHouse SQL: it parses under ClickHouse 26.3 with `implicit_select = 0`. An input ClickHouse refuses proves nothing about the table-reference invariant, and rewriter-grpc, which uses ClickHouse's parser, can only answer `SyntaxError` for it. Check new inputs with `clickhouse local` (any 26.3.x; `clickhouse local` enables `implicit_select` by default, hence the `SET`):

  ```bash
  python3 - <<'EOF'
  import json
  out = ["SET implicit_select = 0;"]
  for c in json.load(open('internal/harness/testdata/storage_integrity_cases.json')):
      tag = 'hgq'
      while f'${tag}$' in c['sql']: tag += 'x'
      out.append(f"SELECT '{c['name']}', formatQuerySingleLineOrNull(${tag}${c['sql']}${tag}$) FORMAT TSV;")
  open('tmp/chparse.sql', 'w').write('\n'.join(out) + '\n')
  EOF
  clickhouse local --queries-file tmp/chparse.sql | grep '\\N$'
  ```

  The only rows allowed to end in `\N` are the twelve pre-Spec-2026-09-26 cases whose inputs predate this rule and agree across engines: `si_reserved_column_star_rename{,_source}_rejected`, `si_with_offset{,_newline,_tab}_rejected`, `si_attach_grant_{physical,logical}_table_rejected`, `si_attach_safe_database_rejected`, `si_mixed_ordinary_with_offset_allowed`, `si_comma_si_with_offset_rejected`, `si_comma_ordinary_with_offset_allowed`, `si_create_live_view_over_si_rejected`.
- A shape only one engine models (a Polyglot walk or generator limit, a top-level parenthesised `SELECT`) is engine-local. Pin it in that engine's own tests (`native_tableref_test.go` here), never in this corpus: spec 2026-09-26 §5 makes its outcome depend on "where the engine models the position".
- The differential (`TestStorageIntegrityGolden` with `REWRITER_ORACLE_ADDR`) compares a rejection's `original_accessed_tables` only when the case pins `want_accessed` (spec §5: a rejection may report a partial list), the same rule the C++ runner applies. A success's list is always compared, in order.
````

- [ ] **Step 3: Commit**

```bash
git add AGENTS.md internal/harness/AGENTS.md
git commit -m "docs: engine-neutral corpus rules and the follow-up policy changes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: verification, differential against v0.15.0, PR

- [ ] **Step 1: CI lanes and formatting**

```bash
go vet ./...
env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
gofmt -l .        # expected: internal/handlers/dblevel.go only (pre-existing on origin/main)
git status --short # expected: clean (tmp/ is ignored)
```

- [ ] **Step 2: ClickHouse parse check of the whole corpus.** Run the command block from `internal/harness/AGENTS.md` (Task 8 Step 2), for example on the build box through the ClickHouse image as in Task 6 Step 2. Expected: exactly the twelve pre-existing names listed there.

- [ ] **Step 3: Differential against rewriter-grpc v0.15.0.** This records what Plan B v2 still has to close. On 2026-09-29 the build box workdir held a `56a17b7` build (inventory §2). If the workdir has since been re-synced, first rebuild `56a17b7` as the inventory describes, under the lock. v0.15.0 segfaults on two corpus cases, so it runs under a restart loop on free ports:

```bash
ssh -p 30100 sentio@64.38.131.242 'test -x /home/sentio/chen/rewriter-grpc/build/clickhousegate_rewriter && ss -ltn | grep -c ":5615[12] " ; true'   # expect the binary and 0 listeners
ssh -p 30100 sentio@64.38.131.242 'mkdir -p /home/sentio/chen/tableref-inv && cat > /home/sentio/chen/tableref-inv/loop.sh <<"EOF"
#!/bin/bash
cd /home/sentio/chen/rewriter-grpc
while [ ! -f /home/sentio/chen/tableref-inv/STOP ]; do
  build/clickhousegate_rewriter 56151 56152 >> /home/sentio/chen/tableref-inv/server.log 2>&1
  sleep 0.3
done
EOF
chmod +x /home/sentio/chen/tableref-inv/loop.sh; rm -f /home/sentio/chen/tableref-inv/STOP
nohup /home/sentio/chen/tableref-inv/loop.sh > /dev/null 2>&1 < /dev/null & echo started'
ssh -f -N -o ExitOnForwardFailure=yes -p 30100 -L 56151:127.0.0.1:56151 sentio@64.38.131.242
env REWRITER_ORACLE_ADDR=localhost:56151 go test ./internal/harness/ -run TestStorageIntegrityGolden \
  -skip 'TestStorageIntegrityGolden/(si_tr_param_exists_rejected|si_tr_param_show_create_rejected)$' -count=1 -v \
  > tmp/differential-v0.15.0.log 2>&1
echo FAIL $(grep -c '^    --- FAIL' tmp/differential-v0.15.0.log) PASS $(grep -c '^    --- PASS' tmp/differential-v0.15.0.log)
ssh -p 30100 sentio@64.38.131.242 'touch /home/sentio/chen/tableref-inv/STOP; pkill -f "clickhousegate_rewrite[r] 56151"; true'
pkill -f 'L 56151:127.0.0.1:56151'
```

Expected: `FAIL 638 PASS 326`. These divergences are the M, F and P work of Plan B v2. The two skipped cases crash v0.15.0 (segfault; M-crash). None of the six G1/G4/G5 cases may appear with a Go-side cause. `si_tr_in_nested_paren_hg_safe_v2_rejected`, both `si_tr_create_default_*_v2` cases and `si_tr_describe_subquery_{phys,hg_safe}_v2_rejected` must PASS.

- [ ] **Step 4: Push and open the PR** (ask before merging, spec §11)

```bash
git push -u origin fix/table-reference-engine-neutral-corpus
gh pr create --base main --title "fix: engine-neutral table-reference corpus (follow-up to #41)" --body "$(cat <<'EOF'
Follow-up to #41. Makes the shared storage-integrity corpus passable by a correct rewriter-grpc engine; v0.14.0 stays unreleased until the paired C++ re-pin shows zero divergences (spec 2026-09-26 §10.5).

Measured inventory of the merged corpus against rewriter-grpc v0.15.0 (987 cases, 726 divergent): M 610, F 51, G 46, P 19. This PR resolves the 46 G cases and the 6 unpinned-rejection-accessed P cases:

- `CREATE TABLE … EMPTY AS SELECT` keeps, rewrites and reports its body (the old pin recorded a Polyglot drop that ClickHouse refuses).
- A parenthesized IN operand keeps the storage-integrity message under an active SI surface (spec T3).
- SI-active refusals of `DESCRIBE (SELECT …)` and of column-DEFAULT reads name the SI object or use the SI catch-all (spec §5 precedence).
- The admitted `SET` answers `success`.
- 17 inputs ClickHouse 26.3 cannot parse are replaced; 22 engine-local cases (Go walk/generator limits, top-level `(SELECT …)`, unparseable shapes) move to native tests.
- The differential compares a rejection's accessed tables only where the corpus pins them (spec §5).

No C++ SQL pins yet: they are generated with `UPDATE_GOLDEN` against the Plan B v2 build, in the paired re-pin.

Corpus: sha256 `76d8db60921387db62452fd27661211da62e06fbbba894c90af5f7d7ccd230e0`, 872362 bytes, 966 cases, FNV-1a/64 `10342073271660396945`.
Differential against rewriter-grpc v0.15.0 (`56a17b7`, two crash cases skipped): 964 run, 638 divergent (Plan B v2 scope), 326 agree.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

### Task 10 (deferred — executed with Plan B v2): C++ SQL pins, zero divergences, release

Run this after the Plan B v2 branch builds on the build box and its C++ runner passes every reject case of the corpus from this PR. The only success-case failures left should be `SQL pin mismatch`.

- [ ] **Step 1: Start the Plan B v2 server** (under `flock /home/sentio/ci/rewriter-build-box.lock` for the build; the restart loop from inventory §2 is no longer needed once M-crash is fixed) and tunnel its gRPC port to `localhost:56151`.

- [ ] **Step 2: Regenerate the SQL pins** in a rewriter-go branch off `main` (after this PR merges):

```bash
env UPDATE_GOLDEN=1 REWRITER_ORACLE_ADDR=localhost:56151 \
  go test ./internal/harness -run '^TestStorageIntegrityGolden$' -count=1
```

`writeSICorpus` re-serialises the whole file into its canonical form (sorted keys, two-space indent), so the byte diff is large. Review it semantically:

```bash
diff <(git show HEAD:internal/harness/testdata/storage_integrity_cases.json | jq -S .) \
     <(jq -S . internal/harness/testdata/storage_integrity_cases.json)
```

Only `want_sql`, `want_sql_go`, `want_sql_cpp` and `allow_sql_divergence` may change. Every `want_sql_go` must equal the old `want_sql`. Every new `want_sql_cpp` must be ClickHouse-parseable: run the Task 9 Step 2 check over the `want_sql_cpp` values.

- [ ] **Step 3: Zero divergences.** Re-run `TestStorageIntegrityGolden` with `REWRITER_ORACLE_ADDR` and no `-skip`. Expected: every case PASSES. Record date, both commits, the oracle binary sha256, the suite counts, zero divergences and the corpus fingerprint in spec §10.5 (housegate docs), as Spec N D3 did.

- [ ] **Step 4: Paired re-pin.** Update `SICorpusFingerprint` / `SICorpusBytes` / `SICorpusCases` here and `kCorpusFingerprint` / `kCorpusBytes` / `kCorpusCases` in rewriter-grpc `tests/si_corpus.h`, prove `cmp -s` identity, and record the same SHA-256 in both PR descriptions (`internal/harness/AGENTS.md`).

- [ ] **Step 5: Release.** Ask first. Then release rewriter-go v0.14.0 with the FFI asset (Plan A Task 9 Step 5), then rewriter-grpc v0.16.0 (Plan B v2). housegate (Plan C) pins both.
