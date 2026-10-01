# Table-Reference Hardening — rewriter-grpc (C++ engine) Implementation Plan v2

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task by task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the C++ SQL rewriter (`housegate/rewriter`, a.k.a. rewriter-grpc) enforce the table-reference hardening policy of spec 2026-09-26 exactly as the merged Go engine does, so that the engine-neutral shared corpus (966 cases, rewriter-go follow-up PR #43) passes byte for byte in both engines, then pair the `want_sql_cpp` re-pin with rewriter-go and release rewriter-grpc v0.16.0.

**Architecture:** One new dynamic-mode preflight, `rewriter_handlers::preflightTableReferences` (`src/handlers/table_reference.{h,cc}`), runs right after parsing and before any handler, in the spec's precedence order: T2 identifier parameters → T7 unmodelled class → T3 protected databases → T5 allowlists (surface inactive) → T6 string lookups → R5 settings and R2 ungoverned reads (surface inactive). While the storage-integrity (SI) surface is active, T5, R5 and R2 run inside the SI handler paths *after* the SI namespace policy, so an SI-owned message always wins. The SELECT pipeline gains IN operands as read sources (T4) and rewrites INSERT … SELECT / CTAS / `view()` bodies; SHOW COLUMNS / DESCRIBE resolve an unqualified target like FROM (R7); the SI reject annotator becomes structural. Everything reads ClickHouse's own AST; no second parser.

**Tech Stack:** C++23 on the ClickHouse fork (`clickHouse` submodule, parser `ParserQuery`, `DB::Lexer`, `formatAst`), GoogleTest v1.14 (`tests/rewriter_test.cc`, `tests/si_corpus.h`), gRPC + rewriter-proto v0.4.0 (`third_party/rewriter-proto` at `115173a960386329ad5d0b0d939195ca8eb506fe`, `repeated string protected_databases = 9`), Ninja on the private build box (`ssh -p 30100 sentio@64.38.131.242`, `/home/sentio/chen/rewriter-grpc`), the rewriter-go harness as differential oracle client.

**Spec:** `/Users/uranuswch/src/tableref-docs/2026-09-26-table-reference-hardening-design.md` (binding). Inputs: `corpus-divergence-inventory.md` + `corpus-divergence.tsv` (987 cases vs v0.15.0 `56a17b7`: M 610, F 51, G 46, P 19), the rewriter-go follow-up plan `2026-09-29-table-reference-hardening-engine-go-followup.md` (its Task 10 runs together with this plan's Task 22), the merged Go reference (`housegate/rewriter-go` origin/main `54c4a8a`, follow-up branch `fix/table-reference-engine-neutral-corpus` at `248ecf2`).

**Supersedes:** Plan B v1 (`2026-09-26-table-reference-hardening-engine-cpp.md`) and resolves every finding of its preflight scan (`.superpowers/sdd/2026-09-26-table-reference-hardening-engine-cpp/preflight.md`): B1 (missing behaviour) — Tasks 2–19 cover every M and P case, see the coverage table; B2 (precedence) — Tasks 3, 4, 5, 11 build T2 → T7 → T3 → T5 in one preflight and Task 11's precedence test pins it; B3 (C++ SQL pins) — Task 1's narrow interim rule plus Task 22's paired re-pin; B4 (SI fail-open) — `decodeInFunction` keeps classifying every identifier IN operand, only the SELECT root drops the namespace refs it now rewrites, and Task 5 makes the multi-part decode *stricter*; S1 corpus copy path — Task 1 Step 1; S2 accessed order — Tasks 1 and 8; S3 statement types — Task 1; S4 CMake — Task 3; S5/S6 compile hazards and duplication — Task 3's single header; S7 (a)–(e) — Tasks 13, 12, 5, 14 and 7 (the SI-handler-blind set is the string-lookup argument only, as rewriter-go follow-up Task 3 made it); S8 lookup names — Task 7; S10 unqualified operands — Task 8 compares `(database, table)` pairs; S11 annotator — Tasks 3 and 15; S12 CI lock — Task 0's loop; S13/S15 re-pin process — Task 22; S14 MV `TO db.{p}` — Task 3 uses `ASTViewTargets::hasTableASTWithQueryParams`.

## Global Constraints

- **Worktree and branch.** Work in `/Users/uranuswch/src/rewriter-grpc-tableref` on `feat/table-reference-hardening` (branched from `56a17b7`). Commit per task on this branch only. Do not push until Task 21. Never commit to `main`.
- **Build box.** Every `scripts.sh`, `ninja` or `cmake` invocation on the box runs under `flock /home/sentio/ci/rewriter-build-box.lock` (CI resets the shared ClickHouse tree under the same lock — preflight S12). Use `ninja -C build <target>` only; never `./scripts.sh rebuild` or `./scripts.sh test` in the persistent checkout. The disk is ~97% full: never create a second build directory, never delete anything that is not yours, and build `clickhousegate_rewriter` (2.8 GB) only in Task 22.
- **Message texts** (cross-engine contract, rewriter-go `AGENTS.md` "Message families"; exact bytes):
  - T2: `query parameters are not supported in a database or table position` — `InvalidRewriteRequest`.
  - T3: `protected database <db> is not addressable` — `InvalidRewriteRequest`.
  - T5: `table function <name> is not accepted` / `table function <name> is not recognised` / `table engine <name> is not accepted` / `table engine <name> is not recognised` / `table setting <name> is not accepted` — `UnsupportedStatement`; `<name>` exactly as written.
  - T6: `<fn> target "<arg>" does not resolve through the caller's databases` — `InvalidRewriteRequest`; `<fn>` as written, `<arg>` the literal target or `""` when not a literal.
  - T7 / R2 / R7 with the surface inactive: `statement is not supported` — `UnsupportedStatement`, `statement_type` `UNSPECIFIED`, `sql_after_rewrite` echoes the input.
  - T7 with the surface active: `storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded` (the existing `kStorageIntegrityUnmodelledMessage`).
- **Precedence** (spec §5, rewriter-go `AGENTS.md` precedence steps): T2 → T7 → T3 → T5 → T6 → R5 → R2 → handlers. While the SI surface is active: an unmodelled class runs the SI write preflight first (an SI target names the outcome); T3 defers every SI physical or reserved database (`hg_safe`, `hg_unsafe`, `hg_promote`, V2 `reserved_databases`) to the SI handlers except inside a string-lookup argument, which no SI handler classifies; T5, R5 and R2 run inside the SI SELECT and write paths after the SI namespace policy; the SI reject annotator never overrides a T2 message.
- **R2 rules** (rewriter-go `rejectUngovernedReads`): ALTER actions and the query / rows of an INSERT whose column list is followed by `SETTINGS` are opaque text — a subquery, a table, a cross-table partition action, `MODIFY QUERY`, or an IN-family occurrence whose operand region is not literal-only (literals, signs, `tuple`/`array` of literals) is refused. The positions rewriter-go's parser structures — structured UPDATE / DELETE, column / constraint / storage expressions, plain `INSERT … VALUES` rows — refuse a table (CTE-aware), a table function, an IN operand naming a table or an Identifier parameter; a table-free subquery and a tuple IN operand are allowed. REFRESH views and `FROM INFILE` are always refused.
- **Reported tables** (spec §5 "What the engines report", proto v0.4.0): `original_accessed_tables` lists a write target first, then the reads key-sorted by the name as written (the `std::map` key the SELECT collector already uses). A rejection that fired on a table source still reports the target first, with its `table_rewrites` entry.
- **Corpus rules.** `tests/testdata/storage_integrity_cases.json` is a byte-for-byte copy of rewriter-go's `internal/harness/testdata/storage_integrity_cases.json` from the follow-up PR: SHA-256 `76d8db60921387db62452fd27661211da62e06fbbba894c90af5f7d7ccd230e0`, 872362 bytes, 966 cases, FNV-1a/64 `10342073271660396945`. It is never edited in this repository; a corpus change is a paired rewriter-go PR (preflight S13). Every reject compares code and message substring exactly; every success compares SQL exactly after `NormalizeSIIdentifierQuotes`, except the interim list below.
- **Interim rule (narrow, temporary).** Until the paired `want_sql_cpp` re-pin (Task 22), exactly the 109 success cases listed in `tests/testdata/si_cpp_sql_pending.txt` compare their SQL through ClickHouse's own parser and formatter (`CanonicalClickHouseSQL`). A listed case must *not* match its pin exactly (the stale check), and `StorageIntegrityCorpus.InterimCanonicalListNamesUnpinnedSuccessCases` requires every entry to be an unpinned success case. No other case, and no reject, is relaxed. Task 22 deletes the file, the helper and both checks in the same commit that copies the re-pinned corpus.
- **Tests.** Every behaviour change starts with a failing GoogleTest. The box has no snapshot-query profiles, so the 641 `*Snapshot*` tests fail from the environment on the baseline too; local loops use `--gtest_filter='-*Snapshot*'`. CI runs them all.
- **Commits** end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Markdown has no hard line-wrapping. Code and comments are English.
- **Ask first** (spec §11): merging any PR (this one, the paired rewriter-go re-pin PR) and cutting any release.

## Review Focus

Inputs this change could get wrong that no pre-existing test covers; each names the test that pins it.

1. **The IN-operand change opens no SI hole (preflight B4).** Every position outside the SELECT root — ALTER DELETE/UPDATE, DELETE, UPDATE, INSERT … SELECT and CTAS bodies, views, MVs, column DEFAULT, MODIFY COLUMN, MODIFY TTL — still refuses an SI operand, parenthesised or not, and a write body refuses an IN operand naming an SI object even where the SELECT root rewrites it: `TableRefSources.InOperandChangeKeepsStorageIntegrityRefusals` and the SI-active rows of `TableRefSources.EmbeddedBodiesAreRewrittenAndReported`.
2. **Precedence T2 → T7 → T3 → T5.** One statement carrying a parameter, an unmodelled class, a protected name and a refused function answers with the earliest rule, and removing the earlier offender each time exposes the next: `TableRefPrecedence.ParameterThenClassThenProtectedThenAllowlist`.
3. **The v0.15.0 segfault.** `EXISTS` / `SHOW CREATE` of an Identifier-parameter target must never reach the formatter with an erased table, in dynamic, static and no-rewrite mode: `TableRefParam.ShowCreateAndExistsNeverReachTheFormatterWithAnEmptyName` (a crash aborts the whole test binary, so the RED run of this task is a crash).
4. **SI messages precede the allowlists while the surface is active.** `merge('hg_safe', 'db1__t')` must keep the SI physical-table message, not `table function merge is not accepted`: `TableRefAllowlist.StorageIntegrityMessagesPrecedeAllowlistsWhenActive` and corpus case `si_merge_physical_function_rejected`.
5. **The interim SQL rule stays narrow.** Only listed unpinned success cases are compared canonically, a listed case that starts matching exactly fails, and a reject can never be listed: the stale check in `StorageIntegrityGolden.MatchesSharedCorpus` and `StorageIntegrityCorpus.InterimCanonicalListNamesUnpinnedSuccessCases`.

## Task sizing rule

One task is one reviewable behaviour group: one spec rule (or one rule's position family) with its tests, its implementation and the corpus cases it moves, small enough that a reviewer can hold the diff and the case list together (the largest implementation step is Task 3's new file, ~250 lines). A task never mixes two precedence steps. Test-infrastructure (Task 1), parity-only refactors with no corpus case (Task 15), docs (Task 20), CI (Task 21), the re-pin (Task 22) and the release (Task 23) are their own tasks. Every task ends green: the whole non-snapshot suite passes, and the corpus failure count only goes down (measured in the Evidence section).

## Coverage

The inventory's 629 M + P cases (987-case corpus vs v0.15.0) all survive into the 966-case follow-up corpus under the same names. 623 of them are assigned to exactly one task below; the other 6 are out of scope (listed after the table). The follow-up also makes 13 more cases fail on v0.15.0 — 9 G cases whose input or pin it replaced and 4 new cases — and they are assigned too, so the tasks cover all 636 corpus cases v0.15.0 fails (plus a crash). Appendix A lists every case by task.

Columns are plan task numbers.

| Pattern | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 16 | 17 | 18 | 19 | Total |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| P (V2 parse) | 8 |  |  | 2 |  |  | 1 |  |  |  |  |  |  |  |  |  | 2 | 13 |
| M-T2 |  | 39 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 39 |
| M-crash |  | 2 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2 |
| M-T7class |  |  | 14 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 14 |
| G-replaced (G3) |  |  | 1 |  |  |  |  |  |  |  |  |  |  | 6 |  |  |  | 7 |
| M-R2 |  |  | 3 |  |  |  |  |  |  |  |  |  |  | 116 | 12 | 30 | 25 | 186 |
| G-replaced (G5) |  |  | 1 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1 |
| M-T3 |  |  |  | 100 | 62 | 23 |  |  |  |  |  |  |  |  |  |  | 2 | 187 |
| M-order |  |  |  |  |  |  | 20 |  |  |  |  |  |  |  |  |  |  | 20 |
| M-T4 |  |  |  |  |  |  | 33 | 15 |  |  |  |  |  |  |  |  |  | 48 |
| M-reject-acc |  |  |  |  |  |  |  | 2 |  |  |  |  |  |  |  |  |  | 2 |
| G-replaced (G1) |  |  |  |  |  |  |  | 1 |  |  |  |  |  |  |  |  |  | 1 |
| new case |  |  |  |  |  |  |  | 1 |  |  |  |  | 3 |  |  |  |  | 4 |
| M-T12 |  |  |  |  |  |  |  |  | 2 |  |  |  |  |  |  |  |  | 2 |
| M-T5fn |  |  |  |  |  |  |  |  |  | 23 |  |  |  |  |  |  |  | 23 |
| M-T5eng |  |  |  |  |  |  |  |  |  |  | 25 |  |  |  |  |  |  | 25 |
| M-R5set |  |  |  |  |  |  |  |  |  |  | 7 |  | 37 |  |  |  |  | 44 |
| M-T6 |  |  |  |  |  |  |  |  |  |  |  | 13 |  |  |  |  |  | 13 |
| M-R7 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 5 | 5 |
| **Total** | **8** | **41** | **19** | **102** | **62** | **23** | **54** | **19** | **2** | **23** | **32** | **13** | **40** | **122** | **12** | **30** | **34** | **636** |

Pattern keys are the inventory's note prefixes (`corpus-divergence.tsv`). M + P total 629 = 623 assigned + 6 out of scope. Totals: M 610, P 13, G-replaced 9, new 4 = 636 assigned cases.

**Out of scope (6 P-contract cases, reason).** `si_create_live_view_over_si_rejected`, `si_drop_dictionary_safe_rejected`, `si_truncate_all_tables_from_unsafe_rejected`, `si_truncate_database_safe_rejected`, `si_v2_drop_dictionary_rejected`, `si_v2_reserved_truncate_database_rejected` differ only in a rejection's `original_accessed_tables`, which the corpus does not pin and spec §5 allows to be partial. The C++ runner already compares a rejection's list only when pinned (`c.has_accessed`), so they pass in C++ at v0.15.0; they were divergences only in the Go differential, and rewriter-go follow-up Task 1 (`alignUnpinnedOracleFields`) removes them there. Task 22's zero-divergence run is their check.

**Not corpus-pinned.** Task 15 (structural annotator) moves no corpus case; its GoogleTest is the pin. Tasks 2–19 also carry GoogleTests for rows the corpus does not cover (Global Constraints' R2 rules, the SI-active write-body IN refusal, the multi-part IN decode).

## The dev loop (used by every task)

Task 0 creates `.superpowers/tableref-loop.sh` (the `.superpowers/` directory is git-ignored). It syncs the worktree to the box, builds `rewriter_tests` under the lock and runs one GoogleTest filter:

```bash
#!/usr/bin/env bash
# Plan B v2 dev loop. Usage: .superpowers/tableref-loop.sh '<gtest filter>' [ninja targets...]
set -euo pipefail
BOX=sentio@64.38.131.242
WORK=/home/sentio/chen/rewriter-grpc
FILTER=${1:--*Snapshot*}
shift || true
TARGETS=${*:-rewriter_tests}
cd /Users/uranuswch/src/rewriter-grpc-tableref
rsync -rlpgoDz --checksum --no-times --delete \
  --exclude='.git' --exclude='build/' --exclude='clickHouse/' --exclude='contrib' \
  --exclude='docs/' --exclude='.superpowers/' -e 'ssh -p 30100' ./ "$BOX:$WORK/"
ssh -p 30100 "$BOX" "flock -w 1800 /home/sentio/ci/rewriter-build-box.lock ninja -C $WORK/build $TARGETS"
ssh -p 30100 "$BOX" "cd $WORK && build/tests/rewriter_tests --gtest_filter='$FILTER' --gtest_brief=1"
```

Two filters recur. `CORPUS='SpecG/*:StorageIntegrityCorpus.*'` runs the shared corpus; `ALL='-*Snapshot*'` runs everything except the environment-bound snapshot suites. To count corpus failures (GoogleTest prints each failure twice, once inline and once in the summary): `.superpowers/tableref-loop.sh "$CORPUS" | grep 'FAILED  \] SpecG' | sort -u | wc -l`.

## Tasks

### Task 0: Branch state, proto v0.4.0, dev loop, baseline

**Files:**
- Modify: `third_party/rewriter-proto` (gitlink `192e43a96bf5bddafcc40606528888c55d3d7f0b` → `115173a960386329ad5d0b0d939195ca8eb506fe`, tag v0.4.0)
- Create (untracked): `.superpowers/tableref-loop.sh`

- [ ] **Step 1: Confirm the base.**

```bash
cd /Users/uranuswch/src/rewriter-grpc-tableref
git status --short          # expect: nothing
git log --oneline -1        # expect: 56a17b7 feat(storage-integrity): contract V2 …
git rev-parse --abbrev-ref HEAD   # expect: feat/table-reference-hardening
```

- [ ] **Step 2: Bump the proto submodule to v0.4.0** (adds `RewriteTableDynamicArgs.protected_databases = 9` and documents the target-first, key-sorted accessed order).

```bash
git -C third_party/rewriter-proto fetch --tags origin
git -C third_party/rewriter-proto checkout 115173a960386329ad5d0b0d939195ca8eb506fe
grep -n 'repeated string protected_databases = 9' third_party/rewriter-proto/proto/rewriter.proto   # expect one hit
```

- [ ] **Step 3: Create the dev loop** exactly as in "The dev loop" above, `chmod +x .superpowers/tableref-loop.sh`.

- [ ] **Step 4: Build and record the baseline** (the proto change regenerates the gRPC sources; the first build takes a few minutes).

```bash
.superpowers/tableref-loop.sh '-*Snapshot*'
```

Expected (measured on `56a17b7`): build succeeds; `[  PASSED  ] 596 tests`, no failure, on the v0.15.0 corpus (256 cases).

- [ ] **Step 5: Commit.**

```bash
git add third_party/rewriter-proto
git commit -m "build: require rewriter-proto v0.4.0 (protected_databases)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 1: Corpus runner loads the follow-up corpus byte for byte, with the narrow interim SQL rule

Copy the engine-neutral corpus from rewriter-go byte for byte and teach the C++ runner its new fields: the `protected_databases` dynamic key (and the rule R9 that every `si_tr_` case sets it), the four statement types the corpus now uses (preflight S3), the ordered accessed-table comparison (S2), and the narrow interim canonical-SQL rule for the 109 unpinned success cases. This task changes no engine code, so it makes the corpus *fail*: that failure list is the work of Tasks 2–19.

**Files:**
- Create: `tests/testdata/si_cpp_sql_pending.txt`
- Modify: `tests/si_corpus.h`
- Modify: `tests/rewriter_test.cc`

**Corpus cases:** 0 fixed (test infrastructure). It exposes the corpus failures that Tasks 2–19 fix.

- [ ] **Step 1: Copy the corpus and verify it.** After rewriter-go PR #43 merges, copy from `main`; until then from the PR head `248ecf2` (same bytes):

```bash
cd /Users/uranuswch/src/rewriter-grpc-tableref
git -C /Users/uranuswch/Dev/housegate/rewriter-go.fix-table-reference-engine-neutral-corpus \
  show 248ecf2:internal/harness/testdata/storage_integrity_cases.json \
  > tests/testdata/storage_integrity_cases.json
shasum -a 256 tests/testdata/storage_integrity_cases.json
# expect 76d8db60921387db62452fd27661211da62e06fbbba894c90af5f7d7ccd230e0
wc -c < tests/testdata/storage_integrity_cases.json   # expect 872362
```

If the hash differs, stop: the base or the follow-up changed.

- [ ] **Step 2: Write the failing tests.**

In `tests/si_corpus.h`, replace:

```cpp
constexpr uint64_t kCorpusFingerprint = 7051648083520101593ULL;
constexpr size_t kCorpusBytes = 254170;
constexpr size_t kCorpusCases = 256;
```

with:

```cpp
constexpr uint64_t kCorpusFingerprint = 10342073271660396945ULL;
constexpr size_t kCorpusBytes = 872362;
constexpr size_t kCorpusCases = 966;
```

In `tests/si_corpus.h`, replace:

```cpp
    "delim", "logical_database_to_remote_upstream_index", "remote_upstreams",
    "storage_integrity",
  };
```

with:

```cpp
    "delim", "logical_database_to_remote_upstream_index", "remote_upstreams",
    "storage_integrity", "protected_databases",
  };
```

In `tests/si_corpus.h`, replace:

```cpp
  JsonString(dynamic, "delim", path);
```

with:

```cpp
  JsonString(dynamic, "delim", path);
  JsonStrings(dynamic, "protected_databases", path);
```

In `tests/si_corpus.h`, replace:

```cpp
    if (c.contract_version != "V1" && c.contract_version != "V2") {
      add("R8", "contract_version must be \"V1\" or \"V2\", got \""
                + c.contract_version + "\"");
    }
```

with:

```cpp
    if (c.contract_version != "V1" && c.contract_version != "V2") {
      add("R8", "contract_version must be \"V1\" or \"V2\", got \""
                + c.contract_version + "\"");
    }
    if (c.name.rfind("si_tr_", 0) == 0
        && (!c.dynamic || JsonStrings(c.dynamic, "protected_databases").empty())) {
      add("R9", "a si_tr_ case must set a non-empty dynamic.protected_databases");
    }
```

In `tests/rewriter_test.cc`, replace:

```cpp
#include <Poco/JSON/Object.h>
```

with:

```cpp
#include <Poco/JSON/Object.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
```

In `tests/rewriter_test.cc`, replace:

```cpp
  {"DROP_TABLE", rewriter::STATEMENT_TYPE_DROP_TABLE},
};
```

with:

```cpp
  {"DROP_TABLE", rewriter::STATEMENT_TYPE_DROP_TABLE},
  {"ALTER_TABLE", rewriter::STATEMENT_TYPE_ALTER_TABLE},
  {"CREATE_VIEW", rewriter::STATEMENT_TYPE_CREATE_VIEW},
  {"CREATE_MATERIALIZED_VIEW", rewriter::STATEMENT_TYPE_CREATE_MATERIALIZED_VIEW},
  {"SHOW_CREATE_TABLE", rewriter::STATEMENT_TYPE_SHOW_CREATE_TABLE},
};
```

In `tests/rewriter_test.cc`, replace:

```cpp
  for (const auto &p : si_corpus::JsonStrings(d, "known_physical_databases"))
    dyn->add_known_physical_databases(p);
  if (d->has("upstream_logical_database_in_context"))
```

with:

```cpp
  for (const auto &p : si_corpus::JsonStrings(d, "known_physical_databases"))
    dyn->add_known_physical_databases(p);
  for (const auto &p : si_corpus::JsonStrings(d, "protected_databases"))
    dyn->add_protected_databases(p);
  if (d->has("upstream_logical_database_in_context"))
```

In `tests/rewriter_test.cc`, insert immediately after:

```cpp
class StorageIntegrityGolden : public ::testing::TestWithParam<SIGoldenCase> {};
```

the following:

```cpp

// INTERIM — Plan B v2 Task 22 deletes this block, si_cpp_sql_pending.txt and
// the interim branch of MatchesSharedCorpus at the paired want_sql_cpp re-pin
// (rewriter-go follow-up Task 10). Until then a listed success case passes its
// SQL check when ClickHouse's own parser and formatter print the C++ output
// and the shared want_sql identically: the formatting-only differences (alias
// AS, ENGINE = spacing, IN-operand parentheses, parameter spacing) fold, while
// a difference in any name, operand or clause does not. An unparseable text
// never compares equal. Every other case compares exactly.
const std::set<std::string> &InterimCanonicalCases() {
  static const std::set<std::string> names = [] {
    std::set<std::string> out;
    std::ifstream in(std::string(REWRITER_TEST_DATA_DIR) + "/si_cpp_sql_pending.txt");
    for (std::string line; std::getline(in, line);)
      if (!line.empty() && line.front() != '#') out.insert(line);
    return out;
  }();
  return names;
}

std::string CanonicalClickHouseSQL(const std::string &sql) {
  try {
    const char *begin = sql.data();
    const char *end = begin + sql.size();
    DB::ParserQuery parser(end, false);
    const auto ast = DB::parseQuery(parser, begin, end, /*description=*/"", 0, 0, 0);
    std::string out = rewriter_handlers::formatAst(ast);
    if (const auto *insert = ast->as<DB::ASTInsertQuery>(); insert && insert->data)
      out += " " + std::string(insert->data, insert->end);
    return out;
  } catch (const DB::Exception &e) {
    return "<unparseable: " + e.displayText() + "> " + sql;
  }
}
```

In `tests/rewriter_test.cc`, replace:

```cpp
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
```

with:

```cpp
  if (c.has_accessed) {
    // Spec 2026-09-26 §5 / rewriter-proto v0.4.0: the order is part of the
    // contract (a write target first, then key-sorted by the name as written).
    ASSERT_EQ(static_cast<size_t>(resp.original_accessed_tables_size()), c.want_accessed.size());
    for (size_t i = 0; i < c.want_accessed.size(); ++i) {
      const auto &w = c.want_accessed[i];
      const auto &g = resp.original_accessed_tables(static_cast<int>(i));
      SCOPED_TRACE("accessed[" + std::to_string(i) + "] want " + w.original_database + "." + w.original_table);
      EXPECT_EQ(g.original_database(), w.original_database);
      EXPECT_EQ(g.original_table(), w.original_table);
      EXPECT_EQ(g.logical_database(), w.logical_database);
      EXPECT_EQ(g.physical_database(), w.physical_database);
      EXPECT_EQ(g.is_remote(), w.is_remote);
      EXPECT_EQ(g.is_storage_integrity(), w.is_storage_integrity);
    }
  }
```

In `tests/rewriter_test.cc`, replace:

```cpp
    EXPECT_EQ(normalized_sql, si_corpus::NormalizeSIIdentifierQuotes(c.ExpectedSQL()))
      << "SQL pin mismatch for " << c.name;
```

with:

```cpp
    const std::string expected = si_corpus::NormalizeSIIdentifierQuotes(c.ExpectedSQL());
    if (InterimCanonicalCases().count(c.name) == 0) {
      EXPECT_EQ(normalized_sql, expected) << "SQL pin mismatch for " << c.name;
    } else {
      // INTERIM (si_cpp_sql_pending.txt): the entry must still be needed…
      EXPECT_NE(normalized_sql, expected)
        << c.name << " now matches its pin exactly: remove it from si_cpp_sql_pending.txt";
      // …and the C++ SQL must be the pinned statement up to ClickHouse's own
      // formatting.
      EXPECT_EQ(CanonicalClickHouseSQL(resp.sql_after_rewrite()), CanonicalClickHouseSQL(c.want_sql))
        << "SQL mismatch for " << c.name << " (interim ClickHouse-canonical comparison)";
    }
```

In `tests/rewriter_test.cc`, insert immediately after:

```cpp
INSTANTIATE_TEST_SUITE_P(SpecG, StorageIntegrityGolden, ::testing::ValuesIn(loadSIGoldenCases()),
  [](const ::testing::TestParamInfo<SIGoldenCase> &info) { return info.param.name; });
```

the following:

```cpp

// INTERIM: every si_cpp_sql_pending.txt entry names an unpinned success case
// of the corpus (a reject echoes its input; a divergence case is pinned).
TEST(StorageIntegrityCorpus, InterimCanonicalListNamesUnpinnedSuccessCases) {
  std::vector<std::string> ignored;
  const auto cases = si_corpus::LoadCases(
    std::string(REWRITER_TEST_DATA_DIR) + "/storage_integrity_cases.json", &ignored);
  std::map<std::string, const si_corpus::Case *> by_name;
  for (const auto &c : cases) by_name[c.name] = &c;
  for (const auto &name : InterimCanonicalCases()) {
    SCOPED_TRACE(name);
    const auto it = by_name.find(name);
    ASSERT_NE(it, by_name.end()) << "not a corpus case";
    EXPECT_EQ(it->second->want_code, "Success");
    EXPECT_FALSE(it->second->allow_sql_divergence);
  }
}
```

Create `tests/testdata/si_cpp_sql_pending.txt`:

```text
# INTERIM (Plan B v2 Task 1): success cases whose C++ SQL the paired want_sql_cpp
# re-pin (rewriter-go follow-up Task 10) has not pinned yet. MatchesSharedCorpus
# compares each one through ClickHouse's own parser and formatter
# (CanonicalClickHouseSQL) and fails once it matches its pin exactly. Plan B v2
# Task 22 deletes this file and the interim branch together.
si_in_logical_table_rewritten
si_ordinary_in_table_allowed
si_tr_alter_add_projection_allowed
si_tr_alter_add_projection_v2_allowed
si_tr_alter_update_literal_in_allowed
si_tr_alter_update_literal_in_v2_allowed
si_tr_ctas_active_source_derived_read
si_tr_ctas_active_source_paren_derived_read
si_tr_ctas_empty_active_source_derived_read
si_tr_ctas_empty_own_source_rewritten
si_tr_ctas_other_tenant_source_lenient
si_tr_ctas_own_source_paren_rewritten
si_tr_ctas_own_source_rewritten
si_tr_engine_memory_allowed
si_tr_engine_mergetree_allowed
si_tr_engine_replicated_bare_allowed
si_tr_global_in_dotted_rewritten
si_tr_global_in_dotted_v2_rewritten
si_tr_global_in_own_rewritten
si_tr_hascolumnintable_accessed_once
si_tr_in_active_derived_read
si_tr_in_active_unsafe_latest_derived_read
si_tr_in_bare_identifier_rewritten
si_tr_in_bare_identifier_v2_rewritten
si_tr_in_call_active_derived_read
si_tr_in_call_dotted_rewritten
si_tr_in_call_dotted_v2_rewritten
si_tr_in_call_own_rewritten
si_tr_in_cte_alias_untouched
si_tr_in_dotted_rewritten
si_tr_in_dotted_v2_rewritten
si_tr_in_literal_value_list_active_allowed
si_tr_in_literal_value_list_allowed
si_tr_in_nested_paren_dotted_rewritten
si_tr_in_nested_paren_dotted_v2_rewritten
si_tr_in_own_rewritten
si_tr_in_own_table_active_surface_rewritten
si_tr_in_paren_dotted_rewritten
si_tr_in_paren_dotted_v2_rewritten
si_tr_in_paren_own_rewritten
si_tr_in_system_reported
si_tr_in_tuple_own_rewritten
si_tr_in_unqualified_active_derived_read
si_tr_in_unqualified_call_active_derived_read
si_tr_in_unqualified_paren_active_derived_read
si_tr_in_unqualified_rewritten
si_tr_insert_select_active_source_paren_derived_read
si_tr_insert_select_active_target_unchanged
si_tr_insert_select_nested_in_rewritten
si_tr_insert_select_own_source_paren_rewritten
si_tr_insert_select_own_source_rewritten
si_tr_insert_select_unqualified_source_rewritten
si_tr_mv_named_refresh_rewritten
si_tr_mv_named_refresh_v2_rewritten
si_tr_mv_to_ordinary_rewritten
si_tr_mv_to_refresh_rewritten
si_tr_mv_to_refresh_v2_rewritten
si_tr_not_in_dotted_rewritten
si_tr_not_in_dotted_v2_rewritten
si_tr_nullin_dotted_rewritten
si_tr_nullin_dotted_v2_rewritten
si_tr_param_column_allowed
si_tr_param_value_allowed
si_tr_r2_show_columns_limit_plain_rewritten
si_tr_r2_view_refresh_column_rewritten
si_tr_r2_view_refresh_column_v2_rewritten
si_tr_r3_alter_delete_keyword_in_literal_list_allowed
si_tr_r3_alter_delete_keyword_in_literal_list_v2_allowed
si_tr_r4_alter_delete_callable_in_array_literal_allowed
si_tr_r4_alter_delete_callable_in_array_literal_v2_allowed
si_tr_r4_alter_delete_in_literal_list_allowed
si_tr_r4_alter_delete_in_literal_list_v2_allowed
si_tr_r4_alter_delete_not_in_string_list_allowed
si_tr_r4_alter_delete_not_in_string_list_v2_allowed
si_tr_r5_alter_add_column_default_in_array3_literal_allowed
si_tr_r5_alter_add_column_default_in_array3_literal_v2_allowed
si_tr_r5_alter_add_column_default_in_array_literal_allowed
si_tr_r5_alter_add_column_default_in_array_literal_v2_allowed
si_tr_r5_alter_add_projection_in_array3_literal_allowed
si_tr_r5_alter_add_projection_in_array3_literal_v2_allowed
si_tr_r5_alter_add_projection_in_array_literal_allowed
si_tr_r5_alter_add_projection_in_array_literal_v2_allowed
si_tr_r5_alter_delete_in_array3_literal_allowed
si_tr_r5_alter_delete_in_array3_literal_v2_allowed
si_tr_r5_alter_delete_in_array_literal_allowed
si_tr_r5_alter_delete_in_array_literal_v2_allowed
si_tr_r5_alter_delete_in_minus_literal_allowed
si_tr_r5_alter_delete_in_minus_literal_v2_allowed
si_tr_r5_alter_delete_in_plus_literal_allowed
si_tr_r5_alter_delete_in_signed_literal_list_allowed
si_tr_r5_alter_modify_column_default_in_array3_literal_allowed
si_tr_r5_alter_modify_column_default_in_array3_literal_v2_allowed
si_tr_r5_alter_modify_column_default_in_array_literal_allowed
si_tr_r5_alter_modify_column_default_in_array_literal_v2_allowed
si_tr_r5_alter_modify_column_materialized_in_array3_literal_allowed
si_tr_r5_alter_modify_column_materialized_in_array3_literal_v2_allowed
si_tr_r5_alter_modify_column_materialized_in_array_literal_allowed
si_tr_r5_alter_modify_column_materialized_in_array_literal_v2_allowed
si_tr_r5_alter_modify_ttl_in_array3_literal_allowed
si_tr_r5_alter_modify_ttl_in_array3_literal_v2_allowed
si_tr_r5_alter_modify_ttl_in_array_literal_allowed
si_tr_r5_alter_modify_ttl_in_array_literal_v2_allowed
si_tr_settings_plain_values_allowed
si_tr_settings_plain_values_v2_allowed
si_tr_show_columns_dotted_rewritten
si_tr_show_columns_where_plain_rewritten
si_tr_tf_view_body_rewritten
si_tr_tuple_in_dotted_rewritten
si_tr_tuple_in_dotted_v2_rewritten
```

- [ ] **Step 3: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*'
```

Expected: `.superpowers/tableref-loop.sh 'StorageIntegrityCorpus.*'` passes 3 tests (pins, frozen contract, interim meta-test). The corpus run fails and then aborts: the v0.15.0 segfault (EXISTS / SHOW CREATE of an Identifier parameter) kills the binary partway through the 966 cases. Those failures are the input of Tasks 2–19; this task has no implementation step, and the crash is expected until Task 3.

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Nothing new is expected to pass here; this step records the starting point. Expected (measured with the crash cases filtered out): corpus failures 636 → 636.

- [ ] **Step 5: Commit.**

```bash
git add tests/testdata/storage_integrity_cases.json tests/testdata/si_cpp_sql_pending.txt tests/si_corpus.h tests/rewriter_test.cc
git commit -m "test(corpus): copy the engine-neutral corpus (966 cases) and compare accessed order

Interim: 109 unpinned success cases compare through ClickHouse's own
formatter until the paired want_sql_cpp re-pin (Plan B v2 Task 22).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: V2 compatibility shim: a clause keyword is not an implicit alias (P, 8 cases)

`prepareStorageIntegritySQL` strips an ordinary table's alias column list so the pinned parser can build the AST. It treated `WHERE`, `LIMIT`, `USING`, … followed by `(` as an implicit alias with a column list, which broke `FROM o WHERE (a, b) IN x`, `FROM o LIMIT (SELECT …)` and `JOIN b USING (k)` under contract V2 only (inventory §6, the V2-only parse failures).

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/storage_integrity.cc`

**Corpus cases:** 8 P (`si_tr_show_columns_where_*_v2`, `si_tr_r2_show_columns_limit_*_v2`, … — Appendix A).

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// ============================================================================
// Table-reference hardening (housegate spec 2026-09-26; Plan B v2).
// ============================================================================

namespace {

// rewriter-go's tablerefDynamic: db1 → phys, phys known and protected, the
// three hg_* databases protected, logical context db1; `si` adds contract V2
// with db1.t Active.
rewriter::RewriteTableDynamicArgs *AddTableRefOption(rewriter::RewriteSQLRequest &req, bool si) {
  auto *opt = req.add_options();
  opt->set_op(rewriter::RewriteOp::TableNameRewrite);
  auto *dyn = opt->mutable_table_name_args()->mutable_dynamic_args();
  (*dyn->mutable_database_map())["db1"] = "phys";
  dyn->add_known_physical_databases("phys");
  dyn->set_upstream_logical_database_in_context("db1");
  dyn->set_delim("_");
  for (const char *p : {"phys", "hg_safe", "hg_unsafe", "hg_promote"}) dyn->add_protected_databases(p);
  if (si) {
    auto *args = dyn->mutable_storage_integrity();
    args->set_contract_version(rewriter::STORAGE_INTEGRITY_CONTRACT_V2);
    args->set_read_mode(rewriter::StorageIntegrityArgs::READ_MODE_SAFE);
    args->set_reserved_row_id_column("_hg_row_id");
    for (const char *r : {"hg_safe", "hg_unsafe", "hg_promote"}) args->add_reserved_databases(r);
    auto &t = (*args->mutable_tables())["db1.t"];
    t.set_safe_table("hg_safe.db1__t");
    t.set_unsafe_table("hg_unsafe.db1__t");
  }
  return dyn;
}

rewriter::RewriteSQLResponse RunTableRef(const std::string &sql, bool si = false) {
  rewriter::RewriteSQLRequest req;
  req.set_sql(sql);
  AddTableRefOption(req, si);
  return RunDirectRewrite(std::move(req));
}

}  // namespace

// Inventory §6 (P): the SI compatibility shim took a clause keyword followed
// by "(" for an implicit alias with a column list and cut the list out of the
// parse SQL, so ClickHouse's parser refused the rest.
TEST(StorageIntegrityCompatibility, ClauseKeywordIsNotAnImplicitAlias) {
  for (const char *sql : {
         "SHOW COLUMNS FROM o LIMIT (SELECT count() FROM `db2.x`)",
         "SHOW COLUMNS FROM o WHERE (SELECT count() FROM `db2.x`) = 2",
         "SHOW INDEX FROM o WHERE (SELECT count() FROM `db2.x`) = 2",
       }) {
    SCOPED_TRACE(sql);
    const auto resp = RunTableRef(sql, /*si=*/true);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.message(), "storage-integrity logical database db1 is not directly addressable");
  }
  for (const char *sql : {"SELECT * FROM db1.o WHERE (a, b) IN (1, 2)",
                          "SELECT * FROM db1.o JOIN db1.p USING (a)"}) {
    SCOPED_TRACE(sql);
    const auto resp = RunTableRef(sql, /*si=*/true);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
  }
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'StorageIntegrityCompatibility.ClauseKeywordIsNotAnImplicitAlias'
```

Expected: `StorageIntegrityCompatibility.ClauseKeywordIsNotAnImplicitAlias` fails (SyntaxError on the V2 rows). Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `StorageIntegrityCompatibility.ClauseKeywordIsNotAnImplicitAlias`.

- [ ] **Step 3: Implement.**

In `src/handlers/storage_integrity.cc`, replace:

```cpp
    } else if (cursor < tokens.size() && identifierToken(tokens[cursor]) &&
               cursor + 1 < tokens.size() && tokens[cursor + 1].text == "(") {
```

with:

```cpp
    } else if (cursor < tokens.size() && identifierToken(tokens[cursor]) &&
               !implicitAliasKeywords.count(tokens[cursor].upper) &&
               cursor + 1 < tokens.size() && tokens[cursor + 1].text == "(") {
```

In `src/handlers/storage_integrity.cc`, replace:

```cpp
  // substituting the table with a derived query would change their meaning.
  std::map<int, bool> in_from;
```

with:

```cpp
  // substituting the table with a derived query would change their meaning.
  // A clause keyword followed by "(" is not an implicit alias with a column
  // list: `FROM o WHERE (a, b) IN x`, `FROM o LIMIT (SELECT …)`,
  // `JOIN b USING (k)` (spec 2026-09-26 inventory §6, the V2-only parse failures).
  static const std::set<std::string> implicitAliasKeywords = {"WHERE", "PREWHERE", "GROUP",
    "HAVING", "ORDER", "LIMIT", "OFFSET", "SETTINGS", "UNION", "INTERSECT", "EXCEPT", "USING",
    "ON", "FORMAT", "WINDOW", "QUALIFY", "INTO", "FETCH", "ARRAY", "LEFT", "RIGHT", "INNER",
    "FULL", "CROSS", "GLOBAL", "ANY", "ALL", "ASOF", "SEMI", "ANTI", "PASTE", "FINAL", "SAMPLE",
    "LIKE", "ILIKE", "NOT", "FROM", "IN"};
  std::map<int, bool> in_from;
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'StorageIntegrityCompatibility.ClauseKeywordIsNotAnImplicitAlias'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured with the crash cases filtered out): corpus failures 636 → 628.

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/storage_integrity.cc
git commit -m "fix(storage-integrity): a clause keyword before '(' is not an implicit alias

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: T2 Identifier parameters in every table position, and the EXISTS / SHOW CREATE crash

Create the table-reference preflight with its first rule. An `{p:Identifier}` in any database or table position — FROM/JOIN, IN operand, statement targets, MV `TO db.{p}` (`ASTViewTargets::hasTableASTWithQueryParams`, preflight S14), `SHOW … FROM {d}`, table-function/engine arguments — is refused with the T2 text in dynamic mode, before any handler. An unmodelled class is opaque, so a parameter anywhere in it is T2 too. The crash fix is independent of mode: `exists.cc` and `show_create.cc` refuse an empty or parameter target before `setTable("")` erases the child the formatter dereferences. `writes.cc`'s old `query parameters are not supported as write targets` text becomes the T2 text. The SI reject annotator returns early on the T2 text (rewriter-go follow-up Task 4, `71e9d2c`), so an SI-active refusal of `INSERT INTO db1.{p} …` keeps the parameter message.

**Files:**
- Create: `src/handlers/table_reference.h`
- Create: `src/handlers/table_reference.cc`
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/storage_integrity.cc`
- Modify: `CMakeLists.txt`
- Modify: `src/rewriter-server.cc`
- Modify: `src/handlers/writes.cc`
- Modify: `src/handlers/exists.cc`
- Modify: `src/handlers/show_create.cc`

**Corpus cases:** 41 M: 39 M-T2 + 2 M-crash (`si_tr_param_exists_rejected`, `si_tr_param_show_create_rejected`).

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, replace:

```cpp
      if (!annotated) {
        EXPECT_EQ(resp.message(), "query parameters are not supported as write targets");
      }
```

with:

```cpp
      if (!annotated) {
        EXPECT_EQ(resp.message(), rewriter_handlers::kIdentifierParameterMessage);
      }
```

In `tests/rewriter_test.cc`, replace:

```cpp
    {"DROP TABLE other.u, {p:Identifier} UUID '00000000-0000-0000-0000-000000000001'",
      false, "query parameters are not supported as write targets",
      rewriter::RewriteCode::InvalidRewriteRequest},
```

with:

```cpp
    {"DROP TABLE other.u, {p:Identifier} UUID '00000000-0000-0000-0000-000000000001'",
      false, std::string(rewriter_handlers::kIdentifierParameterMessage),
      rewriter::RewriteCode::InvalidRewriteRequest},
```

In `tests/rewriter_test.cc`, replace:

```cpp
  const std::vector<Case> cases = {
    {"SHOW DICTIONARIES FROM {db:Identifier}",
      "storage-integrity SHOW DICTIONARIES database is not statically resolvable"},
    {"SHOW TABLES FROM {db:Identifier}",
      "storage-integrity SHOW TABLES database is not statically resolvable"},
  };

  for (const auto &c : cases) {
    SCOPED_TRACE(c.sql);
    rewriter::RewriteSQLRequest req;
    req.set_sql(c.sql);
    AddSIContractOption(req);
    req.mutable_options(0)->mutable_table_name_args()->mutable_dynamic_args()
      ->set_upstream_logical_database_in_context("db1");

    const auto resp = RunDirectRewrite(std::move(req));
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement);
```

with:

```cpp
  // Spec 2026-09-26 T2 names the outcome before the SI SHOW guard: an
  // Identifier parameter is refused in every database position.
  const std::vector<Case> cases = {
    {"SHOW DICTIONARIES FROM {db:Identifier}", rewriter_handlers::kIdentifierParameterMessage.data()},
    {"SHOW TABLES FROM {db:Identifier}", rewriter_handlers::kIdentifierParameterMessage.data()},
  };

  for (const auto &c : cases) {
    SCOPED_TRACE(c.sql);
    rewriter::RewriteSQLRequest req;
    req.set_sql(c.sql);
    AddSIContractOption(req);
    req.mutable_options(0)->mutable_table_name_args()->mutable_dynamic_args()
      ->set_upstream_logical_database_in_context("db1");

    const auto resp = RunDirectRewrite(std::move(req));
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
```

In `tests/rewriter_test.cc`, replace:

```cpp
  const std::vector<std::string> cases = {
    "SHOW DICTIONARIES FROM {db:Identifier}",
    "SHOW DICTIONARIES FROM hg_safe",
```

with:

```cpp
  const std::vector<std::string> cases = {
    "SHOW DICTIONARIES FROM hg_safe",
```

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 T2: an Identifier parameter in any database or table
// position is refused before any rewrite, whatever the SI state.
TEST(TableRefParam, RefusedInEveryTablePosition) {
  for (const bool si : {false, true}) {
    for (const char *sql : {
           "SELECT * FROM {p:Identifier}", "SELECT * FROM db1.{p:Identifier}",
           "SELECT * FROM {d:Identifier}.t", "SELECT * FROM db1.o AS a JOIN {p:Identifier} AS b USING (a)",
           "SELECT * FROM (SELECT * FROM {p:Identifier})",
           "SELECT * FROM db1.o WHERE a IN {p:Identifier}", "SELECT * FROM db1.o WHERE a IN (({p:Identifier}))",
           "SELECT * FROM db1.o WHERE a IN db1.{p:Identifier}", "SELECT * FROM db1.o WHERE in(a, {p:Identifier})",
           "SELECT * FROM db1.o WHERE a IN +{p:Identifier}",
           "INSERT INTO db1.o SELECT * FROM {p:Identifier}",
           "CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM {p:Identifier}",
           "CREATE MATERIALIZED VIEW db1.mv TO db1.{p:Identifier} AS SELECT * FROM db1.o",
           "CREATE MATERIALIZED VIEW {p:Identifier} ENGINE = Memory AS SELECT * FROM db1.o",
           "DROP TABLE {p:Identifier}", "DROP TABLE db1.{p:Identifier}",
           "INSERT INTO db1.{p:Identifier} VALUES (1)", "CREATE TABLE {p:Identifier} (a UInt64) ENGINE = Memory",
           "EXISTS TABLE db1.{p:Identifier}", "SHOW CREATE TABLE db1.{p:Identifier}",
           "DESCRIBE TABLE db1.{p:Identifier}", "RENAME TABLE db1.{p:Identifier} TO db1.z",
           "SHOW TABLES FROM {d:Identifier}", "USE {d:Identifier}",
           "CREATE DATABASE {d:Identifier}", "DROP DATABASE {d:Identifier}",
           "ALTER TABLE db1.{p:Identifier} UPDATE a = 1 WHERE 1",
           "ALTER TABLE db1.o UPDATE a = {c:Identifier} WHERE 1",
           "ALTER TABLE db1.o UPDATE b = (SELECT max(a) FROM {p:Identifier}) WHERE 1",
           "ALTER TABLE db1.o DELETE WHERE a IN {p:Identifier}",
           "ALTER TABLE db1.mv MODIFY QUERY SELECT * FROM {p:Identifier}",
           "EXPLAIN SELECT * FROM {p:Identifier}",
         }) {
      SCOPED_TRACE(std::string(sql) + (si ? " [V2]" : " [inactive]"));
      const auto resp = RunTableRef(sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest) << resp.message();
      EXPECT_EQ(resp.message(), rewriter_handlers::kIdentifierParameterMessage);
      EXPECT_EQ(resp.sql_after_rewrite(), sql);
      EXPECT_EQ(resp.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
    }
  }
}

TEST(TableRefParam, ValueAndColumnParametersStayAllowed) {
  for (const char *sql : {"SELECT * FROM db1.o WHERE a = {v:UInt64}", "SELECT {c:Identifier} FROM db1.o"}) {
    SCOPED_TRACE(sql);
    EXPECT_EQ(RunTableRef(sql).code(), rewriter::RewriteCode::Success);
  }
}

// v0.15.0 segfaulted on these: setTable("") erased the parameter target and
// the formatter dereferenced the missing table. Every mode refuses them now.
TEST(TableRefParam, ShowCreateAndExistsNeverReachTheFormatterWithAnEmptyName) {
  for (const char *sql : {"EXISTS TABLE db1.{p:Identifier}", "SHOW CREATE TABLE db1.{p:Identifier}",
                          "EXISTS TABLE {p:Identifier}", "SHOW CREATE TABLE {p:Identifier}"}) {
    SCOPED_TRACE(sql);
    const auto none = RunWriteOp(sql);
    EXPECT_EQ(none.code, rewriter::RewriteCode::InvalidRewriteRequest);
    EXPECT_EQ(none.message, rewriter_handlers::kIdentifierParameterMessage);
    rewriter::RewriteSQLRequest st;
    st.set_sql(sql);
    AddEmptyStaticOption(st);
    const auto static_resp = RunDirectRewrite(std::move(st));
    EXPECT_EQ(static_resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
    EXPECT_EQ(static_resp.message(), rewriter_handlers::kIdentifierParameterMessage);
    for (const bool si : {false, true}) {
      const auto dynamic = RunTableRef(sql, si);
      EXPECT_EQ(dynamic.code(), rewriter::RewriteCode::InvalidRewriteRequest);
      EXPECT_EQ(dynamic.message(), rewriter_handlers::kIdentifierParameterMessage);
      EXPECT_EQ(dynamic.sql_after_rewrite(), sql);
    }
  }
}

```

In `tests/rewriter_test.cc`, replace:

```cpp
#include "handlers/storage_integrity.h"
```

with:

```cpp
#include "handlers/storage_integrity.h"
#include "handlers/table_reference.h"
```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefParam.*:StorageIntegrity*'
```

Expected: Build fails: `tests/rewriter_test.cc` includes `handlers/table_reference.h`, which does not exist yet. (Without the include, `TableRefParam.ShowCreateAndExistsNeverReachTheFormatterWithAnEmptyName` segfaults the binary — the M-crash reproduction.) Measured: `tests/rewriter_test.cc:5:10: fatal error: 'handlers/table_reference.h' file not found`.

- [ ] **Step 3: Implement.**

Create `src/handlers/table_reference.h`:

```cpp
// src/handlers/table_reference.h — the table-reference policy (housegate spec
// 2026-09-26 §5), mirrored from rewriter-go internal/handlers/preflight.go.
#pragma once

#include <Parsers/IAST.h>

#include <string>
#include <string_view>
#include <utility>

#include "handlers/name_rewrite.h"
#include "rewriter.grpc.pb.h"

namespace rewriter_handlers {

// The cross-engine message families (rewriter-go AGENTS.md "Message
// families"). The texts are a contract with rewriter-go, byte for byte.
constexpr std::string_view kIdentifierParameterMessage =
  "query parameters are not supported in a database or table position";
constexpr std::string_view kUnsupportedStatementMessage = "statement is not supported";

// The IN family (in, notIn, nullIn, notNullIn, their global forms and every
// IgnoreSet alias), matched case-insensitively.
bool isInFamilyFunction(const std::string &name);

// True when `node` is an identifier bound to a query parameter
// (`{p:Identifier}`, `db.{p:Identifier}`, `{d:Identifier}.t`).
bool isIdentifierParameter(const DB::IAST *node);

// T7 (spec §5, rewriter-go precedence step 2): the statement class has no
// handler. A session SET is modelled only while the SI surface is inactive.
bool unmodelledStatementClass(const DB::ASTPtr &ast, const TableRewriteSelection &sel);

// Dynamic-mode table-reference preflight, run right after parsing, in the
// spec's precedence order. Returns true when it populated a rejection; the
// caller returns. Static and no-rewrite requests pass untouched.
bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response);

} // namespace rewriter_handlers
```

Create `src/handlers/table_reference.cc`:

```cpp
// src/handlers/table_reference.cc — see table_reference.h.
#include "handlers/table_reference.h"

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/Lexer.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

#include <algorithm>
#include <cctype>
#include <functional>
#include <set>
#include <string>
#include <vector>

#include "handlers/storage_integrity.h"
#include "handlers/writes.h"

namespace rewriter_handlers {

namespace {

std::string lowerName(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return value;
}

bool surfaceActive(const TableRewriteSelection &sel) {
  return sel.mode == TableRewriteMode::Dynamic && storageIntegritySurfaceActive(*sel.dynamic_args);
}

// Visits every child of `node`, and the AST-valued fields ClickHouse keeps
// outside `children` (the SHOW trailing clauses).
void forEachChild(const DB::IAST &node, const std::function<void(const DB::ASTPtr &)> &visit) {
  for (const auto &child : node.children) visit(child);
  if (const auto *show = node.as<DB::ASTShowTablesQuery>()) {
    if (show->where_expression) visit(show->where_expression);
    if (show->limit_length) visit(show->limit_length);
  } else if (const auto *columns = node.as<DB::ASTShowColumnsQuery>()) {
    if (columns->where_expression) visit(columns->where_expression);
    if (columns->limit_length) visit(columns->limit_length);
  } else if (const auto *indexes = node.as<DB::ASTShowIndexesQuery>()) {
    if (indexes->where_expression) visit(indexes->where_expression);
  }
}

bool anyIdentifierParameter(const DB::IAST &node) {
  if (isIdentifierParameter(&node)) return true;
  bool found = false;
  forEachChild(node, [&](const DB::ASTPtr &child) {
    if (!found && child) found = anyIdentifierParameter(*child);
  });
  return found;
}

// An IN-family call with its two arguments. The parser unwraps an operand's
// parentheses and drops a unary plus, so arguments[1] is the operand at
// every depth (`a IN ((x))`, `a IN +x`).
const DB::ASTFunction *inFamilyCall(const DB::IAST &node) {
  const auto *fn = node.as<DB::ASTFunction>();
  if (!fn || !isInFamilyFunction(fn->name) || !fn->arguments
      || fn->arguments->children.size() != 2)
    return nullptr;
  return fn;
}

// T2 over every database or table position (rewriter-go
// TablePositionParameter). A column-position parameter is not refused.
bool tablePositionParameter(const DB::IAST &node) {
  if (const auto *te = node.as<DB::ASTTableExpression>()) {
    if (isIdentifierParameter(te->database_and_table_name.get())) return true;
  }
  if (const auto *fn = inFamilyCall(node)) {
    if (isIdentifierParameter(fn->arguments->children[1].get())) return true;
  }
  if (const auto *q = dynamic_cast<const DB::ASTQueryWithTableAndOutput *>(&node)) {
    if (isIdentifierParameter(q->database.get()) || isIdentifierParameter(q->table.get())) return true;
  }
  if (const auto *insert = node.as<DB::ASTInsertQuery>()) {
    if (isIdentifierParameter(insert->database.get()) || isIdentifierParameter(insert->table.get()))
      return true;
  }
  if (const auto *rename = node.as<DB::ASTRenameQuery>()) {
    for (const auto &e : rename->getElements()) {
      if (isIdentifierParameter(e.from.database.get()) || isIdentifierParameter(e.from.table.get())
          || isIdentifierParameter(e.to.database.get()) || isIdentifierParameter(e.to.table.get()))
        return true;
    }
  }
  if (const auto *use = node.as<DB::ASTUseQuery>()) {
    if (isIdentifierParameter(use->database)) return true;
  }
  if (const auto *show = node.as<DB::ASTShowTablesQuery>()) {
    if (isIdentifierParameter(show->from)) return true;
  }
  if (const auto *create = node.as<DB::ASTCreateQuery>()) {
    // `MV … TO db.{p:Identifier}` keeps the target as a parameterised AST.
    if (create->targets) {
      for (const auto kind : create->targets->getKinds())
        if (create->targets->hasTableASTWithQueryParams(kind)) return true;
    }
  }
  if (const auto *alter = node.as<DB::ASTAlterQuery>()) {
    // Every ALTER action is opaque text to the reference engine: a parameter
    // anywhere in one is refused (rewriter-go AGENTS precedence step 1).
    if (alter->command_list && anyIdentifierParameter(*alter->command_list)) return true;
  }
  bool found = false;
  forEachChild(node, [&](const DB::ASTPtr &child) {
    if (!found && child) found = tablePositionParameter(*child);
  });
  return found;
}

} // namespace

bool isInFamilyFunction(const std::string &name) {
  std::string lower = lowerName(name);
  constexpr std::string_view ignore_set = "ignoreset";
  if (lower.ends_with(ignore_set)) lower.resize(lower.size() - ignore_set.size());
  static const std::set<std::string> family = {"in", "notin", "nullin", "notnullin",
    "globalin", "globalnotin", "globalnullin", "globalnotnullin"};
  return family.count(lower) > 0;
}

bool isIdentifierParameter(const DB::IAST *node) {
  const auto *id = node ? dynamic_cast<const DB::ASTIdentifier *>(node) : nullptr;
  return id && id->isParam();
}

bool unmodelledStatementClass(const DB::ASTPtr &ast, const TableRewriteSelection &sel) {
  using Kind = DB::IAST::QueryKind;
  switch (ast->getQueryKind()) {
  case Kind::Select:
  case Kind::Insert:
  case Kind::Delete:
  case Kind::Update:
  case Kind::Rename:
  case Kind::Grant:
  case Kind::Revoke:
  case Kind::Use:
  case Kind::Show:
  case Kind::Exists:
    return false;
  case Kind::Describe: {
    // DESCRIBE (SELECT …) is an unmodelled shape, refused before its names
    // are examined (rewriter-go AGENTS precedence step 8).
    const auto *describe = ast->as<DB::ASTDescribeQuery>();
    const auto *te = describe && describe->table_expression
      ? describe->table_expression->as<DB::ASTTableExpression>() : nullptr;
    return te && te->subquery;
  }
  case Kind::Alter: {
    const auto *alter = ast->as<DB::ASTAlterQuery>();
    return !alter || alter->alter_object != DB::ASTAlterQuery::AlterObjectType::TABLE;
  }
  case Kind::Create: {
    // ATTACH, CREATE DICTIONARY / WINDOW VIEW and every access-entity or
    // function CREATE (their own AST classes) are unmodelled.
    const auto *create = ast->as<DB::ASTCreateQuery>();
    return !create || create->attach || create->is_dictionary || create->is_window_view;
  }
  case Kind::Drop: {
    const auto *drop = ast->as<DB::ASTDropQuery>();
    return !drop || drop->kind == DB::ASTDropQuery::Kind::Detach;
  }
  case Kind::Set:
    return !ast->as<DB::ASTSetQuery>() || surfaceActive(sel);
  default:
    return true;
  }
}

bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response) {
  const auto sel = findActiveTableRewrite(request->options());
  if (sel.mode != TableRewriteMode::Dynamic || !ast) return false;
  const bool unmodelled = unmodelledStatementClass(ast, sel);

  // T2. An unmodelled class is opaque to the reference engine: a parameter
  // anywhere in it names the outcome (spec §5 "Command-text statements").
  if (unmodelled ? anyIdentifierParameter(*ast) : tablePositionParameter(*ast)) {
    response->set_code(rewriter::RewriteCode::InvalidRewriteRequest);
    response->set_message(std::string(kIdentifierParameterMessage));
    return true;
  }
  return false;
}

} // namespace rewriter_handlers
```

In `src/handlers/storage_integrity.cc`, replace:

```cpp
#include "handlers/name_rewrite.h"
```

with:

```cpp
#include "handlers/name_rewrite.h"
#include "handlers/table_reference.h"
```

In `src/handlers/storage_integrity.cc`, replace:

```cpp
  if (!response || response->code() == rewriter::RewriteCode::Success) return;
  if (!storageIntegritySurfaceActive(args)) return;
  for (const auto &entry : response->original_accessed_tables())
    if (entry.is_storage_integrity()) return;
```

with:

```cpp
  if (!response || response->code() == rewriter::RewriteCode::Success) return;
  // T2 (an Identifier parameter in a table position) precedes T3 (spec
  // 2026-09-26 §5): nothing below may upgrade its message.
  if (response->message() == kIdentifierParameterMessage) return;
  if (!storageIntegritySurfaceActive(args)) return;
  for (const auto &entry : response->original_accessed_tables())
    if (entry.is_storage_integrity()) return;
```

In `CMakeLists.txt`, replace:

```cmake
    src/handlers/storage_integrity.cc
```

with:

```cmake
    src/handlers/storage_integrity.cc
    src/handlers/table_reference.cc
```

In `src/rewriter-server.cc`, replace:

```cpp
#include "handlers/storage_integrity.h"
#include "handlers/describe.h"
```

with:

```cpp
#include "handlers/storage_integrity.h"
#include "handlers/table_reference.h"
#include "handlers/describe.h"
```

In `src/rewriter-server.cc`, replace:

```cpp
    if (rewriter_handlers::handleUseQuery(ast, request, response)) return;
```

with:

```cpp
    // Spec 2026-09-26 §5: the table-reference policy runs before any handler
    // can rewrite a name it refuses.
    if (rewriter_handlers::preflightTableReferences(ast, request, response)) return;

    if (rewriter_handlers::handleUseQuery(ast, request, response)) return;
```

In `src/handlers/writes.cc`, replace:

```cpp
#include "handlers/storage_integrity.h"
```

with:

```cpp
#include "handlers/storage_integrity.h"
#include "handlers/table_reference.h"
```

In `src/handlers/writes.cc`, replace:

```cpp
// setTable("") erases the target: `DROP TABLE db1.{p:Identifier}` would be
// formatted as `DROP DATABASE phys` (TRUNCATE as `TRUNCATE DATABASE`, CREATE
// TABLE as `CREATE DATABASE`). Every write target must be a literal name.
constexpr std::string_view kQueryParameterWriteTargetMessage =
  "query parameters are not supported as write targets";
```

with:

```cpp
// setTable("") erases the target: `DROP TABLE db1.{p:Identifier}` would be
// formatted as `DROP DATABASE phys` (TRUNCATE as `TRUNCATE DATABASE`, CREATE
// TABLE as `CREATE DATABASE`). Every write target must be a literal name; the
// refusal carries the spec 2026-09-26 T2 text (kIdentifierParameterMessage).
```

In `src/handlers/writes.cc`, replace:

```cpp
  // Before storage-integrity preflight, which skips an empty table name, and
  // before any rewrite (see kQueryParameterWriteTargetMessage).
  if (hasParameterWriteTarget(ast)) {
    rejectInvalidRequest(response, std::string(kQueryParameterWriteTargetMessage));
    return WriteDispatchResult::Rejected;
  }
```

with:

```cpp
  // Before storage-integrity preflight, which skips an empty table name, and
  // before any rewrite. In dynamic mode the table-reference preflight has
  // refused these already; this keeps Mode None and static requests safe.
  if (hasParameterWriteTarget(ast)) {
    rejectInvalidRequest(response, std::string(kIdentifierParameterMessage));
    return WriteDispatchResult::Rejected;
  }
```

In `src/handlers/exists.cc`, replace:

```cpp
#include "handlers/storage_integrity.h"
```

with:

```cpp
#include "handlers/storage_integrity.h"
#include "handlers/table_reference.h"
```

In `src/handlers/exists.cc`, replace:

```cpp
  const std::string origin_db = q->getDatabase();
  const std::string origin_table = q->getTable();
```

with:

```cpp
  const std::string origin_db = q->getDatabase();
  const std::string origin_table = q->getTable();
  // Spec 2026-09-26 T2 / T12: an Identifier parameter target has an empty
  // name. setTable("") would erase it and the formatter would dereference
  // the missing table (the v0.15.0 segfault). Refuse it in every mode.
  if (origin_table.empty() || isIdentifierParameter(q->table.get())
      || isIdentifierParameter(q->database.get())) {
    rejectExists(response, rewriter::RewriteCode::InvalidRewriteRequest,
      std::string(kIdentifierParameterMessage));
    return ExistsDispatchResult::Handled;
  }
```

In `src/handlers/show_create.cc`, replace:

```cpp
#include "handlers/storage_integrity.h"
```

with:

```cpp
#include "handlers/storage_integrity.h"
#include "handlers/table_reference.h"
```

In `src/handlers/show_create.cc`, replace:

```cpp
  const std::string origin_db = q->getDatabase();
  const std::string origin_table = q->getTable();
```

with:

```cpp
  const std::string origin_db = q->getDatabase();
  const std::string origin_table = q->getTable();
  // Spec 2026-09-26 T2 / T12: an Identifier parameter target has an empty
  // name. setTable("") would erase it and the formatter would dereference
  // the missing table (the v0.15.0 segfault). Refuse it in every mode.
  if (origin_table.empty() || isIdentifierParameter(q->table.get())
      || isIdentifierParameter(q->database.get())) {
    rejectShowCreate(response, rewriter::RewriteCode::InvalidRewriteRequest,
      std::string(kIdentifierParameterMessage));
    return ShowCreateDispatchResult::Handled;
  }
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefParam.*:StorageIntegrity*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 724 tests`); corpus failures 628 → 587 (41 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add src/handlers/table_reference.h src/handlers/table_reference.cc tests/rewriter_test.cc src/handlers/storage_integrity.cc CMakeLists.txt src/rewriter-server.cc src/handlers/writes.cc src/handlers/exists.cc src/handlers/show_create.cc
git commit -m "feat(table-ref): refuse Identifier parameters in table positions (T2) and fix the EXISTS/SHOW CREATE crash

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: T7 unmodelled statement classes, before T3

`unmodelledStatementClass` (landed in Task 3, where T2 needs it) classifies by `IAST::QueryKind`: SELECT, INSERT, DELETE, UPDATE, RENAME, GRANT, REVOKE, USE, SHOW and EXISTS are modelled; DESCRIBE except `DESCRIBE (subquery)`; ALTER only for `ALTER TABLE`; CREATE except ATTACH, dictionaries and window views; DROP / TRUNCATE except DETACH; a session SET only while the surface is inactive; every other kind (SYSTEM, KILL, CHECK TABLE, OPTIMIZE, UNDROP, BACKUP / RESTORE, EXPLAIN, access entities, …) is unmodelled. This task refuses an unmodelled class before its names are examined. With the surface active the SI write preflight runs first so an SI target still names the outcome, and the SI catch-all text answers otherwise. The target of a DETACH/ATTACH/OPTIMIZE/UNDROP is reported when ordinary (rewriter-go `RawTableRefs`). The admitted SET pass-through answers `success` with `STATEMENT_TYPE_UNSPECIFIED` instead of being classified as a SELECT.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/writes.h`
- Modify: `src/handlers/writes.cc`
- Modify: `src/handlers/table_reference.cc`
- Modify: `src/handlers/select.cc`

**Corpus cases:** 19: 17 M (14 M-T7class, one of them the SET classification; 3 M-R2 refused as T7) + 2 G-replaced inputs (`si_tr_set_plain_values_passthrough`, `si_tr_describe_table_subquery_v2_rejected`).

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, replace:

```cpp
TEST(StorageIntegrityCatchAll, UnmodelledStatementPassesThroughWithoutStorageIntegrity) {
```

with:

```cpp
TEST(StorageIntegrityCatchAll, UnmodelledStatementIsRefusedWithoutStorageIntegrity) {
```

In `tests/rewriter_test.cc`, replace:

```cpp
  ASSERT_TRUE(service.Rewrite(&ctx, &req, &resp).ok());
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
  EXPECT_EQ(resp.storage_integrity_contract_version(),
            rewriter::STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED);
}

// ---- Storage-integrity contract V2
```

with:

```cpp
  ASSERT_TRUE(service.Rewrite(&ctx, &req, &resp).ok());
  // Spec 2026-09-26 T7: an unmodelled class is refused in dynamic mode
  // whatever the SI state.
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
  EXPECT_EQ(resp.message(), rewriter_handlers::kUnsupportedStatementMessage);
  EXPECT_EQ(resp.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
  EXPECT_EQ(resp.sql_after_rewrite(), "SYSTEM RELOAD CONFIG");
  EXPECT_EQ(resp.storage_integrity_contract_version(),
            rewriter::STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED);
}

// ---- Storage-integrity contract V2
```

In `tests/rewriter_test.cc`, replace:

```cpp
TEST(StorageIntegrityContractV2, V1EmptyMapStaysLegacy) {
  rewriter::RewriteSQLRequest req;
  req.set_sql("SYSTEM RELOAD CONFIG");
  AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V1, false);
  const auto resp = RunDirectRewrite(std::move(req));
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
  EXPECT_EQ(resp.storage_integrity_contract_version(),
            rewriter::STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED);
}
```

with:

```cpp
TEST(StorageIntegrityContractV2, V1EmptyMapStaysLegacy) {
  // The inactive surface: no acknowledgement, the inactive-surface T7 text,
  // and SET as the one modelled pass-through (spec 2026-09-26 T7).
  rewriter::RewriteSQLRequest req;
  req.set_sql("SYSTEM RELOAD CONFIG");
  AddV2Option(req, rewriter::STORAGE_INTEGRITY_CONTRACT_V1, false);
  const auto resp = RunDirectRewrite(std::move(req));
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
  EXPECT_EQ(resp.message(), rewriter_handlers::kUnsupportedStatementMessage);
  EXPECT_EQ(resp.storage_integrity_contract_version(),
            rewriter::STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED);

  rewriter::RewriteSQLRequest set;
  set.set_sql("SET max_threads = 1");
  AddV2Option(set, rewriter::STORAGE_INTEGRITY_CONTRACT_V1, false);
  const auto set_resp = RunDirectRewrite(std::move(set));
  EXPECT_EQ(set_resp.code(), rewriter::RewriteCode::Success) << set_resp.message();
  EXPECT_EQ(set_resp.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
  EXPECT_EQ(set_resp.sql_after_rewrite(), "SET max_threads = 1");
}
```

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 T7: every unmodelled statement class is refused, after
// T2 and before T3, with the SI catch-all while the surface is active; a
// session SET is the one pass-through, and only while it is inactive.
TEST(TableRefClasses, UnmodelledClassesAreRefused) {
  for (const char *sql : {"SYSTEM RELOAD CONFIG", "EXPLAIN SELECT * FROM db1.o", "CHECK TABLE db1.o",
                          "CREATE USER u1", "KILL QUERY WHERE query_id = 'x'", "DETACH TABLE phys.x",
                          "OPTIMIZE TABLE phys.x", "ATTACH TABLE db1.x",
                          "DESCRIBE (SELECT * FROM phys.`db2.x`)"}) {
    SCOPED_TRACE(sql);
    const auto resp = RunTableRef(sql);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.message(), rewriter_handlers::kUnsupportedStatementMessage);
    EXPECT_EQ(resp.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
    EXPECT_EQ(resp.sql_after_rewrite(), sql);
  }
  for (const char *sql : {"SYSTEM RELOAD CONFIG", "EXPLAIN SELECT * FROM db1.o", "KILL QUERY WHERE query_id = 'x'",
                          "DETACH TABLE db1.o", "ATTACH TABLE db1.x", "OPTIMIZE TABLE phys.x",
                          "SET max_threads = 1"}) {
    SCOPED_TRACE(std::string(sql) + " [V2]");
    const auto resp = RunTableRef(sql, /*si=*/true);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.message(), rewriter_handlers::kStorageIntegrityUnmodelledMessage);
  }
  // The DETACH / ATTACH / OPTIMIZE / UNDROP target is reported (rewriter-go
  // RawTableRefs).
  const auto detach = RunTableRef("DETACH TABLE db1.o");
  ASSERT_EQ(detach.original_accessed_tables_size(), 1);
  EXPECT_EQ(detach.original_accessed_tables(0).original_table(), "o");
  EXPECT_EQ(detach.original_accessed_tables(0).physical_database(), "phys");

  const auto set = RunTableRef("SET max_threads = 1, max_block_size = 'a', use_query_cache = true");
  EXPECT_EQ(set.code(), rewriter::RewriteCode::Success) << set.message();
  EXPECT_EQ(set.message(), "success");
  EXPECT_EQ(set.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
  EXPECT_EQ(set.sql_after_rewrite(), "SET max_threads = 1, max_block_size = 'a', use_query_cache = true");
  // Mode None keeps the legacy pass-through for callers without rewrite options.
  EXPECT_EQ(RunWriteOp("SYSTEM RELOAD CONFIG").code, rewriter::RewriteCode::Success);
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefClasses.*:StorageIntegrityCatchAll.*:StorageIntegrityContractV2.*'
```

Expected: `TableRefClasses.UnmodelledClassesAreRefused`, `StorageIntegrityCatchAll.UnmodelledStatementIsRefusedWithoutStorageIntegrity` and `StorageIntegrityContractV2.V1EmptyMapStaysLegacy` fail (the classes pass through with `Success`). Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `StorageIntegrityCatchAll.UnmodelledStatementIsRefusedWithoutStorageIntegrity`, `StorageIntegrityContractV2.V1EmptyMapStaysLegacy`, `TableRefClasses.UnmodelledClassesAreRefused`.

- [ ] **Step 3: Implement.**

In `src/handlers/writes.h`, replace:

```cpp
#include "rewriter.grpc.pb.h"

namespace rewriter_handlers {
```

with:

```cpp
#include "handlers/name_rewrite.h"
#include "rewriter.grpc.pb.h"

namespace rewriter_handlers {

// The storage-integrity write preflight (SI targets, embedded SI reads, SI
// namespaces) for any statement. Returns true when it populated a rejection.
// The table-reference preflight runs it for an unmodelled statement class
// while the SI surface is active, so an SI target still names the outcome.
bool storageIntegrityWritePreflight(const DB::ASTPtr &ast,
                                    const TableRewriteSelection &sel,
                                    rewriter::RewriteSQLResponse *response);
```

In `src/handlers/writes.cc`, replace:

```cpp
} // namespace

bool handleRemovedLiveViewCompatibility(
```

with:

```cpp
} // namespace

bool storageIntegrityWritePreflight(const DB::ASTPtr &ast,
                                    const TableRewriteSelection &sel,
                                    rewriter::RewriteSQLResponse *response) {
  return preflightStorageIntegrityWrite(ast, sel, response);
}

bool handleRemovedLiveViewCompatibility(
```

In `src/handlers/table_reference.cc`, replace:

```cpp
  const auto sel = findActiveTableRewrite(request->options());
  if (sel.mode != TableRewriteMode::Dynamic || !ast) return false;
  const bool unmodelled = unmodelledStatementClass(ast, sel);
```

with:

```cpp
  const auto sel = findActiveTableRewrite(request->options());
  if (sel.mode != TableRewriteMode::Dynamic || !ast) return false;
  const auto &args = *sel.dynamic_args;
  const bool active = storageIntegritySurfaceActive(args);
  const bool unmodelled = unmodelledStatementClass(ast, sel);
```

In `src/handlers/table_reference.cc`, replace:

```cpp
    response->set_message(std::string(kIdentifierParameterMessage));
    return true;
  }
  return false;
}
```

with:

```cpp
    response->set_message(std::string(kIdentifierParameterMessage));
    return true;
  }

  // T7 before T3: an unmodelled class is refused before its names are
  // examined. With the SI surface active the SI write preflight names an SI
  // target first, and the reject annotator may name the SI object.
  if (unmodelled) {
    if (active && storageIntegrityWritePreflight(ast, sel, response)) return true;
    // Report the target of a DETACH / ATTACH / OPTIMIZE / UNDROP, the classes
    // rewriter-go's RawTableRefs reads, when it is ordinary. An SI target is
    // left to the reject annotator: an SI-flagged entry would stop it.
    const auto kind = ast->getQueryKind();
    const bool records_target = ast->as<DB::ASTDropQuery>() || ast->as<DB::ASTCreateQuery>()
      || kind == DB::IAST::QueryKind::Optimize || kind == DB::IAST::QueryKind::Undrop;
    if (const auto *q = dynamic_cast<const DB::ASTQueryWithTableAndOutput *>(ast.get()); q && records_target) {
      const std::string effective = q->getDatabase().empty()
        ? storageIntegrityExecutionDatabase(args) : q->getDatabase();
      if (!active || (!lookupStorageIntegrity(q->getDatabase(), q->getTable(), args)
                      && !isStorageIntegrityPhysicalDatabase(effective, args)))
        recordAccessedTable(response, q->getDatabase(), q->getTable(), sel);
    }
    response->set_code(rewriter::RewriteCode::UnsupportedStatement);
    response->set_message(std::string(active ? kStorageIntegrityUnmodelledMessage
                                             : kUnsupportedStatementMessage));
    return true;
  }
  return false;
}
```

In `src/handlers/select.cc`, replace:

```cpp
#include "handlers/storage_integrity.h"
```

with:

```cpp
#include "handlers/storage_integrity.h"
#include "handlers/table_reference.h"
```

In `src/handlers/select.cc`, replace:

```cpp
  if (shouldDumpAst()) {
    std::cout << "before transformation " << ast->dumpTree() << std::endl;
  }

  std::map<std::string, AccessedTablePair> originals;
  collectAccessedTablePairsFromAST(ast, originals);
```

with:

```cpp
  if (shouldDumpAst()) {
    std::cout << "before transformation " << ast->dumpTree() << std::endl;
  }
  // Spec 2026-09-26 T7: a session SET is the one modelled pass-through while
  // the SI surface is inactive (the preflight refused it otherwise). It names
  // no table and is not a SELECT.
  if (ast->as<DB::ASTSetQuery>()
      && findActiveTableRewrite(request->options()).mode == TableRewriteMode::Dynamic) {
    setSuccessResponse(response, formatAst(ast), rewriter::STATEMENT_TYPE_UNSPECIFIED);
    return;
  }

  std::map<std::string, AccessedTablePair> originals;
  collectAccessedTablePairsFromAST(ast, originals);
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefClasses.*:StorageIntegrityCatchAll.*:StorageIntegrityContractV2.*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 740 tests`); corpus failures 587 → 572 (15 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/writes.h src/handlers/writes.cc src/handlers/table_reference.cc src/handlers/select.cc
git commit -m "feat(table-ref): refuse unmodelled statement classes before names are examined (T7)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: T3 protected databases in read positions, protected-list validation, protected logical context

A protected database — any `database_map` value, any `protected_databases` entry, and while the surface is active any SI physical or reserved database — is refused in every read position (FROM/JOIN, IN operands, subqueries, CTE bodies, DESCRIBE/SHOW targets, the logical context itself). While the surface is active an SI physical name defers to the SI handlers. The server validates that every `protected_databases` entry is a simple identifier; `resolvePhysicalDatabase` stops treating a protected known-physical database as a pass-through (S7c). To make the deferral safe, `decodeInFunction` now splits a multi-part IN operand at its first part (`hg_safe.`db2.x`` → `hg_safe` / `db2.x`), as rewriter-go's `decodeInOperand` does; before, the joined spelling never split and `ALTER … DELETE WHERE a IN hg_safe.`db2.x`` was classified as an ordinary unqualified table and forwarded. This makes the decode stricter, never looser (B4). A blind SI physical name (a string-lookup argument) is reported as the SI object it is (`physical_database`, `is_storage_integrity`).

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/storage_integrity.cc`
- Modify: `src/handlers/table_reference.h`
- Modify: `src/handlers/table_reference.cc`
- Modify: `src/rewriter-server.cc`
- Modify: `src/handlers/name_rewrite.cc`

**Corpus cases:** 102: 100 M-T3 + 2 P (V2 parse).

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 T3: a protected database named in a read position is
// refused. While the SI surface is active an SI physical or reserved name is
// left to the SI handlers, whose messages keep precedence.
TEST(TableRefProtected, RefusedInReadPositions) {
  for (const char *db : {"phys", "hg_safe", "hg_unsafe", "hg_promote"}) {
    for (const char *shape : {
           "SELECT * FROM %s.`db2.x`", "SELECT * FROM db1.o AS a JOIN %s.`db2.x` AS b USING (a)",
           "SELECT * FROM (SELECT * FROM %s.`db2.x`)", "WITH c AS (SELECT * FROM %s.`db2.x`) SELECT * FROM c",
           "SELECT * FROM db1.o WHERE a IN %s.`db2.x`", "SELECT * FROM db1.o WHERE a IN ((%s.`db2.x`))",
           "SELECT * FROM db1.o WHERE (a, b) IN %s.`db2.x`", "SELECT * FROM db1.o WHERE in(a, %s.`db2.x`)",
           "SELECT * FROM db1.o WHERE a GLOBAL IN %s.`db2.x`", "INSERT INTO db1.o SELECT * FROM %s.`db2.x`",
           "CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM %s.`db2.x`",
           "CREATE VIEW db1.v AS SELECT * FROM %s.`db2.x`", "DESCRIBE TABLE %s.`db2.x`",
           "ALTER TABLE db1.o DELETE WHERE a IN %s.`db2.x`",
           "ALTER TABLE db1.o UPDATE b = (SELECT count() FROM %s.`db2.x`) WHERE 1",
           "CREATE TABLE db1.n (a UInt64 DEFAULT (SELECT max(a) FROM %s.`db2.x`)) ENGINE = Memory",
           "SHOW COLUMNS FROM o WHERE (SELECT count() FROM %s.`db2.x`) = 2",
         }) {
      std::string sql = shape;
      sql.replace(sql.find("%s"), 2, db);
      for (const bool si : {false, true}) {
        SCOPED_TRACE(sql + (si ? " [V2]" : " [inactive]"));
        const auto resp = RunTableRef(sql, si);
        EXPECT_NE(resp.code(), rewriter::RewriteCode::Success) << resp.message();
        EXPECT_EQ(resp.sql_after_rewrite(), sql);
        if (!si || std::string(db) == "phys") {
          EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
          EXPECT_EQ(resp.message(), std::string("protected database ") + db + " is not addressable");
        } else {
          EXPECT_NE(resp.message().find("storage-integrity"), std::string::npos) << resp.message();
        }
      }
    }
  }
}

TEST(TableRefProtected, ColumnAliasAndLiteralUsesAreAllowed) {
  for (const char *sql : {"SELECT phys FROM db1.o", "SELECT a AS hg_safe FROM db1.o",
                          "SELECT * FROM db1.o WHERE s = 'phys'", "SELECT o.a FROM db1.o AS o"}) {
    SCOPED_TRACE(sql);
    EXPECT_EQ(RunTableRef(sql).code(), rewriter::RewriteCode::Success);
  }
}

TEST(TableRefProtected, ProtectedLogicalContextIsRefused) {
  rewriter::RewriteSQLRequest req;
  req.set_sql("SELECT * FROM o");
  AddTableRefOption(req, false)->set_upstream_logical_database_in_context("phys");
  const auto resp = RunDirectRewrite(std::move(req));
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
  EXPECT_EQ(resp.message(), "protected database phys is not addressable");
}

TEST(TableRefProtected, EntriesMustBeSimpleIdentifiers) {
  rewriter::RewriteSQLRequest req;
  req.set_sql("SELECT 1");
  AddTableRefOption(req, false)->add_protected_databases("hg-promote");
  const auto resp = RunDirectRewrite(std::move(req));
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
  EXPECT_EQ(resp.message(), "protected_databases entry \"hg-promote\" must be a simple identifier");
  EXPECT_EQ(resp.sql_after_rewrite(), "SELECT 1");
}

// A protected known_physical_databases entry is no longer a pass-through
// logical name (rewriter-proto v0.4.0 protected_databases comment).
TEST(TableRefProtected, ProtectedKnownPhysicalDatabaseIsNotAPassThrough) {
  rewriter::RewriteSQLRequest req;
  AddTableRefOption(req, false);
  const auto &args = req.options(0).table_name_args().dynamic_args();
  EXPECT_FALSE(rewriter_handlers::resolvePhysicalDatabase("phys", args).has_value());
  EXPECT_EQ(rewriter_handlers::resolvePhysicalDatabase("db1", args), std::optional<std::string>("phys"));
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefProtected.*'
```

Expected: All five `TableRefProtected.*` tests fail; `EntriesMustBeSimpleIdentifiers` returns `Success` for `hg-promote`. Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `TableRefProtected.EntriesMustBeSimpleIdentifiers`, `TableRefProtected.ProtectedKnownPhysicalDatabaseIsNotAPassThrough`, `TableRefProtected.ProtectedLogicalContextIsRefused`, `TableRefProtected.RefusedInReadPositions`.

- [ ] **Step 3: Implement.**

In `src/handlers/storage_integrity.cc`, replace:

```cpp
  if (args.size() != 2 || !args[1]->as<DB::ASTIdentifier>()) return std::nullopt;
  NamespaceRef ref = decodeSingle(NamespaceRefSource::InTable, display, args[1]);
```

with:

```cpp
  if (args.size() != 2 || !args[1]->as<DB::ASTIdentifier>()) return std::nullopt;
  if (const auto &parts = args[1]->as<DB::ASTIdentifier>()->name_parts; parts.size() >= 2) {
    NamespaceRef ref{NamespaceRefSource::InTable, display};
    ref.database = parts[0];
    ref.table = parts[1];
    for (size_t i = 2; i < parts.size(); ++i) ref.table += "." + parts[i];
    ref.resolved = true;
    return ref;
  }
  NamespaceRef ref = decodeSingle(NamespaceRefSource::InTable, display, args[1]);
```

In `src/handlers/table_reference.h`, replace:

```cpp
// The IN family (in, notIn, nullIn, notNullIn, their global forms and every
```

with:

```cpp
// T3: a protected database is any database_map value, any
// protected_databases entry and, while the SI surface is active, any SI
// physical or reserved database (spec §5 "Names").
std::string protectedDatabaseRejectMessage(const std::string &database);
bool protectedDatabase(const std::string &name, const rewriter::RewriteTableDynamicArgs &args);
// Every protected_databases entry must be a simple identifier (spec §6).
bool validateProtectedDatabases(const rewriter::RewriteTableDynamicArgs &args, std::string *error);

// The IN family (in, notIn, nullIn, notNullIn, their global forms and every
```

In `src/handlers/table_reference.cc`, replace:

```cpp
bool surfaceActive(const TableRewriteSelection &sel) {
```

with:

```cpp
bool simpleIdentifierName(const std::string &value) {
  if (value.empty()) return false;
  for (size_t i = 0; i < value.size(); ++i) {
    const auto c = static_cast<unsigned char>(value[i]);
    if (!(std::isalpha(c) || c == '_' || (i > 0 && std::isdigit(c)))) return false;
  }
  return true;
}

bool surfaceActive(const TableRewriteSelection &sel) {
```

In `src/handlers/table_reference.cc`, replace:

```cpp
} // namespace

bool isInFamilyFunction(const std::string &name) {
```

with:

```cpp
bool isShowBody(const DB::IAST &node, const DB::ASTPtr &child) {
  if (const auto *show = node.as<DB::ASTShowTablesQuery>())
    return child == show->where_expression || child == show->limit_length;
  if (const auto *columns = node.as<DB::ASTShowColumnsQuery>())
    return child == columns->where_expression || child == columns->limit_length;
  if (const auto *indexes = node.as<DB::ASTShowIndexesQuery>())
    return child == indexes->where_expression;
  return false;
}

struct DatabaseRef {
  std::string database;
  bool blind = false;  // a string-lookup argument: no SI handler classifies it
};

// T3 collection (rewriter-go CollectDatabaseReferenceSets). `opaque` marks an
// ALTER action or a SHOW trailing clause: rewriter-go reads those as text, so
// only qualified names count there, never table-function or lookup literals.
void collectDatabases(const DB::IAST &node, bool opaque, std::vector<DatabaseRef> &out) {
  auto add = [&](const std::string &db, bool blind = false) {
    if (!db.empty()) out.push_back({db, blind});
  };
  // IAST::as<T>() matches the exact type; ASTTableIdentifier derives from
  // ASTIdentifier, so the identifier check needs a dynamic_cast.
  if (const auto *id = dynamic_cast<const DB::ASTIdentifier *>(&node)) {
    const bool table_identifier = node.as<DB::ASTTableIdentifier>() != nullptr;
    if ((table_identifier || opaque) && id->name_parts.size() >= 2) add(id->name_parts.front());
  }
  if (const auto *fn = inFamilyCall(node)) {
    const auto *operand = fn->arguments->children[1]->as<DB::ASTIdentifier>();
    if (operand && operand->name_parts.size() >= 2) add(operand->name_parts.front());
  }
  const auto *alter = node.as<DB::ASTAlterQuery>();
  forEachChild(node, [&](const DB::ASTPtr &child) {
    if (!child) return;
    const bool in_commands = alter && child.get() == alter->command_list;
    collectDatabases(*child, opaque || in_commands || isShowBody(node, child), out);
  });
}

} // namespace

std::string protectedDatabaseRejectMessage(const std::string &database) {
  return "protected database " + database + " is not addressable";
}

bool protectedDatabase(const std::string &name, const rewriter::RewriteTableDynamicArgs &args) {
  if (name.empty()) return false;
  for (const auto &[_, physical] : args.database_map())
    if (physical == name) return true;
  for (const auto &entry : args.protected_databases())
    if (entry == name) return true;
  return storageIntegritySurfaceActive(args) && isStorageIntegrityPhysicalDatabase(name, args);
}

bool validateProtectedDatabases(const rewriter::RewriteTableDynamicArgs &args, std::string *error) {
  for (const auto &entry : args.protected_databases()) {
    if (!simpleIdentifierName(entry)) {
      if (error) *error = "protected_databases entry \"" + entry + "\" must be a simple identifier";
      return false;
    }
  }
  return true;
}

bool isInFamilyFunction(const std::string &name) {
```

In `src/handlers/table_reference.cc`, replace:

```cpp
    response->set_message(std::string(active ? kStorageIntegrityUnmodelledMessage
                                             : kUnsupportedStatementMessage));
    return true;
  }
  return false;
}
```

with:

```cpp
    response->set_message(std::string(active ? kStorageIntegrityUnmodelledMessage
                                             : kUnsupportedStatementMessage));
    return true;
  }

  // T3. With the SI surface active an SI physical or reserved database
  // defers to the SI handlers (their messages are pinned), except in a
  // string-lookup argument, which no SI handler classifies.
  std::vector<DatabaseRef> databases;
  collectDatabases(*ast, false, databases);
  if (!args.upstream_logical_database_in_context().empty())
    databases.push_back({args.upstream_logical_database_in_context(), false});
  for (const auto &ref : databases) {
    if (!protectedDatabase(ref.database, args)) continue;
    if (active && !ref.blind && isStorageIntegrityPhysicalDatabase(ref.database, args)) continue;
    // A blind SI physical name is reported as the SI object it is, as the SI
    // handlers report one (rewriter-go parity).
    if (active && isStorageIntegrityPhysicalDatabase(ref.database, args))
      recordPhysicalStorageIntegrityAccess(response, ref.database, "", ref.database);
    else
      recordAccessedDatabase(response, ref.database, &args);
    response->set_code(rewriter::RewriteCode::InvalidRewriteRequest);
    response->set_message(protectedDatabaseRejectMessage(ref.database));
    return true;
  }
  return false;
}
```

In `src/rewriter-server.cc`, replace:

```cpp
    response->set_storage_integrity_contract_version(si.contract_version());
  }
```

with:

```cpp
    response->set_storage_integrity_contract_version(si.contract_version());
  }
  // Spec 2026-09-26 §6: every protected_databases entry is a simple
  // identifier, validated for every dynamic request whatever the SI state.
  if (si_selection.mode == rewriter_handlers::TableRewriteMode::Dynamic) {
    std::string protected_error;
    if (!rewriter_handlers::validateProtectedDatabases(
          *si_selection.dynamic_args, &protected_error)) {
      response->set_sql_after_rewrite(original_query);
      response->set_message(protected_error);
      response->set_code(rewriter::RewriteCode::InvalidRewriteRequest);
      return;
    }
  }
```

In `src/handlers/name_rewrite.cc`, replace:

```cpp
#include "handlers/storage_integrity.h"
```

with:

```cpp
#include "handlers/storage_integrity.h"
#include "handlers/table_reference.h"
```

In `src/handlers/name_rewrite.cc`, replace:

```cpp
  // 2) Already a physical DB the user knows about -> passthrough.
  for (const auto &p : args.known_physical_databases()) {
    if (p == logical_db) return logical_db;
  }
  return std::nullopt;
```

with:

```cpp
  // 2) Already a physical DB the user knows about -> passthrough, unless it
  //    is protected (spec 2026-09-26 §6: a name in both lists is protected).
  for (const auto &p : args.known_physical_databases()) {
    if (p == logical_db) {
      if (protectedDatabase(logical_db, args)) return std::nullopt;
      return logical_db;
    }
  }
  return std::nullopt;
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefProtected.*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 851 tests`); corpus failures 572 → 466 (106 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/storage_integrity.cc src/handlers/table_reference.h src/handlers/table_reference.cc src/rewriter-server.cc src/handlers/name_rewrite.cc
git commit -m "feat(table-ref): refuse protected databases in read positions (T3)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: T3 protected databases in statement targets

Extend the T3 walk to every statement target: write / DDL targets, EXISTS / SHOW CREATE, CREATE / DROP DATABASE, both RENAME sides, USE, `SHOW … FROM`, CREATE … AS, MV `TO`, and an ALTER partition action's FROM / TO table.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/table_reference.cc`

**Corpus cases:** 62 M-T3.

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 T3: a protected database named as a statement target is
// refused instead of being double-prefixed (``phys.`phys.x` ``) or passed
// through (`USE phys`).
TEST(TableRefProtected, RefusedInStatementTargets) {
  for (const char *db : {"phys", "hg_safe", "hg_unsafe", "hg_promote"}) {
    for (const char *shape : {
           "INSERT INTO %s.`db2.x` VALUES (1)", "DROP TABLE %s.`db2.x`",
           "CREATE TABLE %s.`db2.x` (a UInt64) ENGINE = Memory", "CREATE TABLE db1.n AS %s.`db2.x`",
           "RENAME TABLE %s.`db2.x` TO db1.z", "EXISTS TABLE %s.`db2.x`", "SHOW CREATE TABLE %s.`db2.x`",
           "SHOW TABLES FROM %s", "USE %s", "CREATE DATABASE %s", "DROP DATABASE %s",
           "CREATE MATERIALIZED VIEW db1.mv TO %s.`db2.x` AS SELECT * FROM db1.o",
           "ALTER TABLE db1.o REPLACE PARTITION tuple() FROM %s.`db2.x`",
         }) {
      std::string sql = shape;
      sql.replace(sql.find("%s"), 2, db);
      for (const bool si : {false, true}) {
        // An SI physical MV TO target reaches the SI write preflight in Task 10
        // (TableRefParity.MaterializedViewToActiveTableIsRefused covers it).
        if (si && std::string(db) != "phys" && sql.rfind("CREATE MATERIALIZED VIEW", 0) == 0) continue;
        SCOPED_TRACE(sql + (si ? " [V2]" : " [inactive]"));
        const auto resp = RunTableRef(sql, si);
        EXPECT_NE(resp.code(), rewriter::RewriteCode::Success) << resp.message();
        EXPECT_EQ(resp.sql_after_rewrite(), sql);
        if (!si || std::string(db) == "phys") {
          EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
          EXPECT_EQ(resp.message(), std::string("protected database ") + db + " is not addressable");
          ASSERT_EQ(resp.original_accessed_tables_size(), 1);
          EXPECT_EQ(resp.original_accessed_tables(0).original_database(), db);
          EXPECT_EQ(resp.original_accessed_tables(0).physical_database(), "");
        } else {
          EXPECT_NE(resp.message().find("storage-integrity"), std::string::npos) << resp.message();
        }
      }
    }
  }
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefProtected.RefusedInStatementTargets'
```

Expected: `TableRefProtected.RefusedInStatementTargets` fails (the targets are rewritten or reach the SI handlers). Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `TableRefProtected.RefusedInStatementTargets`.

- [ ] **Step 3: Implement.**

In `src/handlers/table_reference.cc`, replace:

```cpp
  if (const auto *fn = inFamilyCall(node)) {
    const auto *operand = fn->arguments->children[1]->as<DB::ASTIdentifier>();
    if (operand && operand->name_parts.size() >= 2) add(operand->name_parts.front());
  }
```

with:

```cpp
  if (const auto *fn = inFamilyCall(node)) {
    const auto *operand = fn->arguments->children[1]->as<DB::ASTIdentifier>();
    if (operand && operand->name_parts.size() >= 2) add(operand->name_parts.front());
  }
  // Statement targets: write / DDL targets, EXISTS / SHOW CREATE, CREATE /
  // DROP DATABASE, RENAME sides, USE, SHOW … FROM, CREATE … AS, MV TO, and an
  // ALTER partition action's FROM / TO table.
  if (const auto *q = dynamic_cast<const DB::ASTQueryWithTableAndOutput *>(&node)) add(q->getDatabase());
  if (const auto *command = node.as<DB::ASTAlterCommand>()) {
    add(command->from_database);
    add(command->to_database);
  }
  if (const auto *insert = node.as<DB::ASTInsertQuery>()) add(insert->getDatabase());
  if (const auto *rename = node.as<DB::ASTRenameQuery>()) {
    for (const auto &e : rename->getElements()) {
      add(e.from.getDatabase());
      add(e.to.getDatabase());
    }
  }
  if (const auto *use = node.as<DB::ASTUseQuery>()) add(use->getDatabase());
  if (const auto *show = node.as<DB::ASTShowTablesQuery>()) add(show->getFrom());
  if (const auto *columns = node.as<DB::ASTShowColumnsQuery>()) add(columns->database);
  if (const auto *indexes = node.as<DB::ASTShowIndexesQuery>()) add(indexes->database);
  if (const auto *create = node.as<DB::ASTCreateQuery>()) {
    add(create->as_database);
    if (create->targets && create->hasTargetTableID(DB::ViewTarget::To))
      add(create->getTargetTableID(DB::ViewTarget::To).database_name);
  }
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefProtected.RefusedInStatementTargets'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 914 tests`); corpus failures 466 → 404 (62 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/table_reference.cc
git commit -m "feat(table-ref): refuse protected databases in statement targets (T3)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 7: T3 protected databases in carrier arguments; string-lookup arguments are SI-handler-blind

The literal database of a table function (`remote`, `merge`, `cluster`, …), a table engine (`Merge`, `Buffer`, `Distributed`, …) and a string lookup (`joinGet`, `dictGet*`, `hasColumnInTable`, … — the full rewriter-go `IsStringLookup` set, S8) is a T3 position. `decodeEngine` becomes one exported decoder shared with the SI namespace policy. A string-lookup argument is the only SI-handler-blind position: it is refused as protected even while the surface is active (S7e as narrowed by rewriter-go follow-up Task 3).

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/storage_integrity.h`
- Modify: `src/handlers/storage_integrity.cc`
- Modify: `src/handlers/table_reference.cc`

**Corpus cases:** 23 M-T3.

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 T3 precedes T5 and T6: a protected database named in a
// table-function, table-engine or string-lookup argument is refused as
// protected. A lookup argument is SI-handler-blind, so it is refused as
// protected even while the SI surface is active.
TEST(TableRefProtected, RefusedInCarrierArguments) {
  struct Shape { const char *sql; bool blind; };
  for (const char *db : {"phys", "hg_safe", "hg_unsafe", "hg_promote"}) {
    for (const Shape shape : {
           Shape{"SELECT * FROM merge('%s', 'db2')", false},
           Shape{"SELECT * FROM remote('127.0.0.1:9000', '%s', 'db2.x')", false},
           Shape{"CREATE TABLE db1.n (a UInt64) ENGINE = Merge('%s', '^db2')", false},
           Shape{"SELECT joinGet('%s.`db2.x`', 'v', 1)", true},
           Shape{"SELECT dictGet('%s.`db2.x`', 'v', 1)", true},
           Shape{"SELECT hasColumnInTable('%s', 'db2', 'v')", true},
         }) {
      std::string sql = shape.sql;
      sql.replace(sql.find("%s"), 2, db);
      for (const bool si : {false, true}) {
        SCOPED_TRACE(sql + (si ? " [V2]" : " [inactive]"));
        const auto resp = RunTableRef(sql, si);
        EXPECT_NE(resp.code(), rewriter::RewriteCode::Success) << resp.message();
        EXPECT_EQ(resp.sql_after_rewrite(), sql);
        if (!si || shape.blind || std::string(db) == "phys") {
          EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
          EXPECT_EQ(resp.message(), std::string("protected database ") + db + " is not addressable");
          if (si && std::string(db) != "phys") {
            // A blind SI physical name is still reported as the SI object.
            ASSERT_EQ(resp.original_accessed_tables_size(), 1);
            EXPECT_TRUE(resp.original_accessed_tables(0).is_storage_integrity());
            EXPECT_EQ(resp.original_accessed_tables(0).physical_database(), db);
          }
        } else {
          EXPECT_NE(resp.message().find("storage-integrity"), std::string::npos) << resp.message();
        }
      }
    }
  }
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefProtected.RefusedInCarrierArguments'
```

Expected: `TableRefProtected.RefusedInCarrierArguments` fails for every lookup row and the inactive function/engine rows. Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `TableRefProtected.RefusedInCarrierArguments`.

- [ ] **Step 3: Implement.**

In `src/handlers/storage_integrity.h`, replace:

```cpp
#include <Parsers/IAST.h>
```

with:

```cpp
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>
```

In `src/handlers/storage_integrity.h`, replace:

```cpp
std::vector<NamespaceRef> collectStorageIntegrityNamespaceRefs(const DB::ASTPtr &ast);
```

with:

```cpp
std::vector<NamespaceRef> collectStorageIntegrityNamespaceRefs(const DB::ASTPtr &ast);

// The (database, table) a namespace-carrying table function or table engine
// names in its literal arguments, or nullopt for any other function. Shared
// with the table-reference preflight (T3), which reads only `database`.
std::optional<NamespaceRef> decodeTableFunctionNamespace(const DB::ASTFunction &fn);
std::optional<NamespaceRef> decodeTableEngineNamespace(const DB::ASTFunction &engine);
```

In `src/handlers/storage_integrity.cc`, replace:

```cpp
// Split a table identifier's "db.table" (or bare "table") at the first dot,
```

with:

```cpp
std::optional<NamespaceRef> decodeEngine(const DB::ASTFunction &engine) {
  const std::string lname = lower(engine.name);
  const auto &args = functionArgs(engine);
  if (lname == "remote" || lname == "distributed")
    return decodePair(NamespaceRefSource::TableEngine, engine.name, args, 1);
  if (lname == "merge" || lname == "buffer")
    return decodePair(NamespaceRefSource::TableEngine, engine.name, args, 0);
  return std::nullopt;
}

// Split a table identifier's "db.table" (or bare "table") at the first dot,
```

In `src/handlers/storage_integrity.cc`, replace:

```cpp
  if (const auto *create = ast->as<DB::ASTCreateQuery>()) {
    if (create->storage && create->storage->engine) {
      const std::string lname = lower(create->storage->engine->name);
      const auto &args = functionArgs(*create->storage->engine);
      std::optional<NamespaceRef> ref;
      if (lname == "remote" || lname == "distributed")
        ref = decodePair(
          NamespaceRefSource::TableEngine, create->storage->engine->name, args, 1);
      else if (lname == "merge" || lname == "buffer")
        ref = decodePair(
          NamespaceRefSource::TableEngine, create->storage->engine->name, args, 0);
      if (ref) {
```

with:

```cpp
  if (const auto *create = ast->as<DB::ASTCreateQuery>()) {
    if (create->storage && create->storage->engine) {
      if (auto ref = decodeEngine(*create->storage->engine)) {
```

In `src/handlers/storage_integrity.cc`, replace:

```cpp
std::vector<StorageIntegrityReadRef> collectStorageIntegrityReadRefs(const DB::ASTPtr &ast) {
```

with:

```cpp
std::optional<NamespaceRef> decodeTableFunctionNamespace(const DB::ASTFunction &fn) {
  return decodeNamespaceFunction(fn, NamespaceRefSource::TableFunction);
}

std::optional<NamespaceRef> decodeTableEngineNamespace(const DB::ASTFunction &engine) {
  return decodeEngine(engine);
}

std::vector<StorageIntegrityReadRef> collectStorageIntegrityReadRefs(const DB::ASTPtr &ast) {
```

In `src/handlers/table_reference.cc`, replace:

```cpp
bool isShowBody(const DB::IAST &node, const DB::ASTPtr &child) {
```

with:

```cpp
std::string stripIdentifierQuotes(std::string value) {
  if (value.size() >= 2 && (value.front() == '`' || value.front() == '"') && value.back() == value.front())
    return value.substr(1, value.size() - 2);
  return value;
}

// The database a string-lookup literal names: "db.table" / "db.`t.x`" → db.
std::string lookupLiteralDatabase(const std::string &literal) {
  const auto dot = literal.find('.');
  if (dot == std::string::npos) return {};
  return stripIdentifierQuotes(literal.substr(0, dot));
}

std::optional<std::string> stringLiteral(const DB::ASTPtr &node) {
  const auto *literal = node ? node->as<DB::ASTLiteral>() : nullptr;
  if (!literal || literal->value.getType() != DB::Field::Types::String) return std::nullopt;
  return literal->value.safeGet<String>();
}

// joinGet / joinGetOrNull and the dictGet / dictHas / … / hasColumnInTable
// families (rewriter-go IsStringLookup), matched case-insensitively.
bool isStringLookupName(const std::string &name) {
  const std::string lower = lowerName(name);
  if (lower == "joinget" || lower == "joingetornull") return true;
  for (const char *prefix : {"dictget", "dicthas", "dictgethierarchy", "dictisin",
         "dictgetchildren", "dictgetdescendants", "hascolumnintable"})
    if (lower.rfind(prefix, 0) == 0) return true;
  return false;
}

bool isShowBody(const DB::IAST &node, const DB::ASTPtr &child) {
```

In `src/handlers/table_reference.cc`, replace:

```cpp
  const auto *alter = node.as<DB::ASTAlterQuery>();
  forEachChild(node, [&](const DB::ASTPtr &child) {
    if (!child) return;
    const bool in_commands = alter && child.get() == alter->command_list;
```

with:

```cpp
  // Carrier arguments: a table function's or table engine's literal
  // database, and a string-lookup literal (SI-handler-blind).
  if (!opaque) {
    if (const auto *te = node.as<DB::ASTTableExpression>()) {
      if (const auto *fn = te->table_function ? te->table_function->as<DB::ASTFunction>() : nullptr) {
        if (auto ref = decodeTableFunctionNamespace(*fn)) add(ref->database);
      }
    }
    if (const auto *create = node.as<DB::ASTCreateQuery>(); create && create->storage && create->storage->engine) {
      if (auto ref = decodeTableEngineNamespace(*create->storage->engine)) add(ref->database);
    }
    if (const auto *fn = node.as<DB::ASTFunction>(); fn && isStringLookupName(fn->name) && fn->arguments) {
      const auto &args = fn->arguments->children;
      if (lowerName(fn->name).rfind("hascolumnintable", 0) == 0) {
        if (args.size() >= 3) {
          if (auto db = stringLiteral(args[args.size() - 3])) add(*db, true);
        }
      } else if (!args.empty()) {
        if (auto literal = stringLiteral(args[0])) add(lookupLiteralDatabase(*literal), true);
      }
    }
  }
  const auto *alter = node.as<DB::ASTAlterQuery>();
  forEachChild(node, [&](const DB::ASTPtr &child) {
    if (!child) return;
    const bool in_commands = alter && child.get() == alter->command_list;
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefProtected.RefusedInCarrierArguments'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 938 tests`); corpus failures 404 → 381 (23 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/storage_integrity.h src/handlers/storage_integrity.cc src/handlers/table_reference.cc
git commit -m "feat(table-ref): refuse protected databases in table-function, engine and lookup arguments (T3)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: T4 IN operands are read sources; reported order is key-sorted

In a SELECT root, an IN operand that names a table (`a IN db1.p`, `a IN ((db1.p))`, `in(a, db1.p)`, `GLOBAL IN`, a bare quoted `db2.x`) is rewritten and reported like FROM, and an Active SI operand becomes the derived safe read. The SELECT root drops the IN namespace refs it now rewrites; every other position keeps `decodeInFunction`'s refusal (preflight B4). An unqualified operand read through an SI physical execution context stays with the SI namespace policy. The lexical SI target collector sorts by the name as written, so SI-side accessed lists follow the same key order as the SELECT collector (M-order).

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/table_reference.h`
- Modify: `src/handlers/table_reference.cc`
- Modify: `src/handlers/storage_integrity.h`
- Modify: `src/handlers/storage_integrity.cc`
- Modify: `src/handlers/select.cc`

**Corpus cases:** 54: 53 M (33 M-T4, 20 M-order) + 1 P.

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, replace:

```cpp
  const std::vector<std::string> semantic_identifiers = {
    "SELECT * FROM merge(`\\x64b1`, t)",
    "SELECT * FROM remote('host', `\\x64b1`, t)",
    "SELECT * FROM other.u WHERE id IN `\\x64b1`.t",
  };
```

with:

```cpp
  const std::vector<std::string> semantic_identifiers = {
    "SELECT * FROM merge(`\\x64b1`, t)",
    "SELECT * FROM remote('host', `\\x64b1`, t)",
  };
```

In `tests/rewriter_test.cc`, replace:

```cpp
  const std::vector<std::string> literal_backslash_x = {
```

with:

```cpp
  // Spec 2026-09-26 T4: the escaped IN operand decodes to the Active db1.t
  // and is read through the derived safe read.
  {
    rewriter::RewriteSQLRequest req;
    req.set_sql("SELECT * FROM other.u WHERE id IN `\\x64b1`.t");
    AddSIContractOption(req);
    auto *dyn = req.mutable_options(0)->mutable_table_name_args()->mutable_dynamic_args();
    (*dyn->mutable_database_map())["other"] = "phys";
    const auto resp = RunDirectRewrite(std::move(req));
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
    EXPECT_NE(resp.sql_after_rewrite().find("IN (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t)"),
              std::string::npos) << resp.sql_after_rewrite();
    const auto *access = FindAccessed(resp, "db1", "t");
    ASSERT_NE(access, nullptr);
    EXPECT_TRUE(access->is_storage_integrity());
  }

  const std::vector<std::string> literal_backslash_x = {
```

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 T4: an IN operand is a read source like FROM — rewritten,
// reported, and read through the derived safe read when Active. The order is
// key-sorted by the name as written (§5 "What the engines report").
TEST(TableRefSources, InOperandsAreRewrittenAndReported) {
  using Pairs = std::vector<std::pair<std::string, std::string>>;
  struct Case { const char *sql; const char *want_sql; Pairs accessed; bool si = false; };
  const std::vector<Case> cases = {
    {"SELECT * FROM db1.o WHERE a IN db1.p",
     "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN (phys.`db1.p`)", {{"db1", "o"}, {"db1", "p"}}},
    {"SELECT * FROM db1.o WHERE a IN ((db1.p))",
     "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN (phys.`db1.p`)", {{"db1", "o"}, {"db1", "p"}}},
    {"SELECT * FROM db1.o WHERE (a, b) IN db1.p",
     "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE (a, b) IN (phys.`db1.p`)", {{"db1", "o"}, {"db1", "p"}}},
    {"SELECT * FROM db1.o WHERE in(a, db1.p)",
     "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE in(a, phys.`db1.p`)", {{"db1", "o"}, {"db1", "p"}}},
    {"SELECT * FROM db1.o WHERE a GLOBAL IN db1.p",
     "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a GLOBAL IN (phys.`db1.p`)", {{"db1", "o"}, {"db1", "p"}}},
    {"SELECT * FROM db1.o WHERE a IN p",
     "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN (phys.`db1.p`)", {{"db1", "o"}, {"", "p"}}},
    {"SELECT * FROM db1.o WHERE a IN `db2.x`",
     "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN (phys.`db1.db2.x`)", {{"db1", "o"}, {"", "db2.x"}}},
    {"SELECT * FROM db1.o WHERE a IN (b)",
     "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN (phys.`db1.b`)", {{"", "b"}, {"db1", "o"}}},
    {"WITH c AS (SELECT 1 AS a) SELECT * FROM db1.o WHERE a IN c",
     "WITH c AS (SELECT 1 AS a) SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN (c)", {{"db1", "o"}}},
    {"SELECT * FROM db1.o WHERE a IN system.tables",
     "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN (system.tables)", {{"db1", "o"}, {"system", "tables"}}},
    {"SELECT * FROM db1.o WHERE a IN db1.t",
     "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t)",
     {{"db1", "o"}, {"db1", "t"}}, true},
  };
  for (const auto &c : cases) {
    SCOPED_TRACE(c.sql);
    const auto resp = RunTableRef(c.sql, c.si);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
    EXPECT_EQ(resp.sql_after_rewrite(), c.want_sql);
    Pairs got;
    for (const auto &a : resp.original_accessed_tables()) got.emplace_back(a.original_database(), a.original_table());
    EXPECT_EQ(got, c.accessed);
  }
}

// Preflight finding B4: decodeInFunction and the embedded SI read stream are
// unchanged, so while the SI surface is active every position outside the
// SELECT root (write bodies, views, mutations, column and ALTER expressions)
// still refuses an SI operand, and the SELECT root still refuses an SI
// physical one.
TEST(TableRefSources, InOperandChangeKeepsStorageIntegrityRefusals) {
  const std::string safe = "storage-integrity physical table hg_safe.db1__t is not directly addressable";
  struct Case { const char *sql; rewriter::RewriteCode code; std::string message; };
  const std::vector<Case> cases = {
    {"SELECT * FROM db1.o WHERE a IN hg_safe.db1__t", rewriter::RewriteCode::RewriteError, safe},
    {"SELECT * FROM db1.o WHERE a IN ((hg_safe.db1__t))", rewriter::RewriteCode::RewriteError, safe},
    {"SELECT * FROM db1.o WHERE in(a, (hg_safe.db1__t))", rewriter::RewriteCode::RewriteError, safe},
    {"SELECT * FROM db1.o WHERE a GLOBAL IN ((hg_unsafe.db1__t))", rewriter::RewriteCode::RewriteError,
     "storage-integrity physical table hg_unsafe.db1__t is not directly addressable"},
    {"SELECT * FROM db1.o WHERE a NOT IN ((hg_promote.x))", rewriter::RewriteCode::RewriteError,
     "storage-integrity physical table hg_promote.x is not directly addressable"},
    {"ALTER TABLE db1.o DELETE WHERE a IN hg_safe.db1__t", rewriter::RewriteCode::UnsupportedStatement, safe},
    {"ALTER TABLE db1.o UPDATE b = 1 WHERE a IN ((hg_safe.db1__t))", rewriter::RewriteCode::UnsupportedStatement, safe},
    {"DELETE FROM db1.o WHERE a IN ((hg_safe.db1__t))", rewriter::RewriteCode::UnsupportedStatement, safe},
    {"UPDATE db1.o SET b = 1 WHERE a IN ((hg_safe.db1__t))", rewriter::RewriteCode::UnsupportedStatement, safe},
    {"INSERT INTO db1.o SELECT * FROM db1.p WHERE a IN ((hg_safe.db1__t))", rewriter::RewriteCode::UnsupportedStatement, safe},
    {"CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM db1.p WHERE a IN ((hg_safe.db1__t))",
     rewriter::RewriteCode::UnsupportedStatement, safe},
    {"CREATE VIEW db1.v AS SELECT * FROM db1.p WHERE a IN ((hg_safe.db1__t))", rewriter::RewriteCode::UnsupportedStatement, safe},
    {"CREATE MATERIALIZED VIEW db1.mv TO db1.o AS SELECT * FROM db1.p WHERE a IN ((hg_safe.db1__t))",
     rewriter::RewriteCode::UnsupportedStatement, safe},
    {"CREATE TABLE db1.n (a UInt64, b UInt8 DEFAULT a IN ((hg_safe.db1__t))) ENGINE = Memory",
     rewriter::RewriteCode::UnsupportedStatement, safe},
    {"ALTER TABLE db1.o MODIFY COLUMN b UInt8 DEFAULT a IN ((hg_safe.db1__t))", rewriter::RewriteCode::UnsupportedStatement, safe},
    {"ALTER TABLE db1.o MODIFY TTL d + INTERVAL 1 DAY DELETE WHERE a IN ((hg_safe.db1__t))",
     rewriter::RewriteCode::UnsupportedStatement, safe},
    // An Active logical operand outside a rewritten SELECT body keeps its refusal.
    {"ALTER TABLE db1.o DELETE WHERE a IN db1.t", rewriter::RewriteCode::UnsupportedStatement,
     "storage-integrity table db1.t is not directly addressable through IN table target"},
    {"CREATE VIEW db1.v AS SELECT * FROM db1.p WHERE a IN db1.t", rewriter::RewriteCode::UnsupportedStatement,
     "storage-integrity table db1.t is not directly addressable through IN table target"},
  };
  for (const auto &c : cases) {
    SCOPED_TRACE(c.sql);
    const auto resp = RunTableRef(c.sql, /*si=*/true);
    EXPECT_EQ(resp.code(), c.code) << resp.message();
    EXPECT_EQ(resp.message(), c.message);
    EXPECT_EQ(resp.sql_after_rewrite(), c.sql);
  }
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefSources.*:StorageIntegrityIdentifierEscapes*'
```

Expected: `TableRefSources.InOperandsAreRewrittenAndReported` fails (operands are left verbatim and unreported); `InOperandChangeKeepsStorageIntegrityRefusals` already passes — it is the regression guard for this task's change. Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `StorageIntegrityNamespaces.IdentifierEscapesAreSemanticButLiteralsAreNotDecodedAgain`, `TableRefSources.InOperandsAreRewrittenAndReported`.

- [ ] **Step 3: Implement.**

In `src/handlers/table_reference.h`, replace:

```cpp
// T7 (spec §5, rewriter-go precedence step 2): the statement class has no
```

with:

```cpp
// The (database, table) an IN-operand identifier names, from its parts: one
// part is an unqualified table (a bare quoted `db2.x` included), two or more
// split at the first part (spec 2026-09-26 T4, rewriter-go decodeInOperand).
std::pair<std::string, std::string> inOperandTarget(const DB::IAST &identifier);

// T7 (spec §5, rewriter-go precedence step 2): the statement class has no
```

In `src/handlers/table_reference.cc`, replace:

```cpp
bool unmodelledStatementClass(const DB::ASTPtr &ast, const TableRewriteSelection &sel) {
```

with:

```cpp
std::pair<std::string, std::string> inOperandTarget(const DB::IAST &identifier) {
  const auto &parts = dynamic_cast<const DB::ASTIdentifier &>(identifier).name_parts;
  if (parts.size() == 1) return {"", parts.front()};
  std::string table = parts[1];
  for (size_t i = 2; i < parts.size(); ++i) table += "." + parts[i];
  return {parts.front(), table};
}

bool unmodelledStatementClass(const DB::ASTPtr &ast, const TableRewriteSelection &sel) {
```

In `src/handlers/storage_integrity.h`, replace:

```cpp
std::vector<NamespaceRef> collectStorageIntegrityNamespaceRefs(const DB::ASTPtr &ast);
```

with:

```cpp
std::vector<NamespaceRef> collectStorageIntegrityNamespaceRefs(const DB::ASTPtr &ast);

// The refs minus IN-operand targets: callers whose own table walk already
// classifies IN operands as read sources (spec 2026-09-26 T4).
std::vector<NamespaceRef> withoutInOperandRefs(std::vector<NamespaceRef> refs);
```

In `src/handlers/storage_integrity.cc`, replace:

```cpp
std::optional<NamespaceRef> decodeTableFunctionNamespace(const DB::ASTFunction &fn) {
```

with:

```cpp
std::vector<NamespaceRef> withoutInOperandRefs(std::vector<NamespaceRef> refs) {
  refs.erase(std::remove_if(refs.begin(), refs.end(), [](const NamespaceRef &ref) {
    return ref.source == NamespaceRefSource::InTable;
  }), refs.end());
  return refs;
}

std::optional<NamespaceRef> decodeTableFunctionNamespace(const DB::ASTFunction &fn) {
```

In `src/handlers/storage_integrity.cc`, replace:

```cpp
  const auto selection = dynamicSelection(args);
  for (const auto &target : targets) {
    bool exists = false;
```

with:

```cpp
  const auto selection = dynamicSelection(args);
  // Spec 2026-09-26 §5 "What the engines report": key-sorted by the name as
  // written, the order the SELECT collector produces.
  std::vector<LexicalTarget> sorted = targets;
  std::stable_sort(sorted.begin(), sorted.end(), [](const LexicalTarget &l, const LexicalTarget &r) {
    return lexicalKey(l) < lexicalKey(r);
  });
  for (const auto &target : sorted) {
    bool exists = false;
```

In `src/handlers/select.cc`, replace:

```cpp
struct AccessedTablePair {
  std::string original_db;     // empty when the SQL didn't qualify the table.
  std::string original_table;
};
```

with:

```cpp
struct AccessedTablePair {
  std::string original_db;     // empty when the SQL didn't qualify the table.
  std::string original_table;
  bool in_operand = false;     // collected from an IN operand (spec T4)
};
```

In `src/handlers/select.cc`, replace:

```cpp
  if (!node) return;

  if (auto *te = node->as<DB::ASTTableExpression>()) {
    if (te->database_and_table_name) {
      if (auto *id = te->database_and_table_name->as<DB::ASTTableIdentifier>()) {
        const auto full = id->name();  // "db.table" or bare "table"
```

with:

```cpp
  if (!node) return;

  // Spec 2026-09-26 T4: an IN operand naming a table is a read source like
  // FROM. The parser unwraps its parentheses and drops a unary plus, so the
  // second argument of any IN-family call is the operand at every depth.
  if (const auto *fn = node->as<DB::ASTFunction>();
      fn && isInFamilyFunction(fn->name) && fn->arguments && fn->arguments->children.size() == 2) {
    const auto &operand = fn->arguments->children[1];
    if (const auto *id = operand->as<DB::ASTIdentifier>(); id && !id->isParam()) {
      const auto [db, table] = inOperandTarget(*id);
      const bool is_cte_ref = db.empty() && cte_scope.count(table) > 0;
      if (!table.empty() && !is_cte_ref)
        out.emplace(id->name(), AccessedTablePair{db, table, true});
    }
  }

  if (auto *te = node->as<DB::ASTTableExpression>()) {
    if (te->database_and_table_name) {
      if (auto *id = te->database_and_table_name->as<DB::ASTTableIdentifier>()) {
        const auto full = id->name();  // "db.table" or bare "table"
```

In `src/handlers/select.cc`, replace:

```cpp
  const std::unordered_set<std::string> &cte_scope) {
  if (!ast) return;

  if (auto *table_expression = ast->as<DB::ASTTableExpression>()) {
```

with:

```cpp
  const std::unordered_set<std::string> &cte_scope) {
  if (!ast) return;

  // Spec 2026-09-26 T4: rewrite an IN operand exactly like a FROM reference.
  // An Active SI table becomes the derived safe/unsafe read; a mapped table
  // its physical name; an unmapped logical name stays (lenient, like FROM).
  if (auto *fn = ast->as<DB::ASTFunction>();
      fn && isInFamilyFunction(fn->name) && fn->arguments && fn->arguments->children.size() == 2) {
    auto &operand = fn->arguments->children[1];
    const auto *id = operand->as<DB::ASTIdentifier>();
    if (id && !id->isParam()) {
      const std::string origin_full_name = id->name();
      const auto [origin_db, origin_table] = inOperandTarget(*id);
      const bool is_cte_ref = origin_db.empty() && cte_scope.count(origin_table) > 0;
      if (!origin_table.empty() && !is_cte_ref) {
        if (auto hit = lookupStorageIntegrity(origin_db, origin_table, args)) {
          operand = buildStorageIntegrityDerivedTable(*hit->table, args.storage_integrity());
          if (out_table_rewrites)
            (*out_table_rewrites)[origin_full_name] = hit->table->safe_table();
        } else {
          const auto outcome = applyDynamicRewrite(origin_db, origin_table, args, "IN");
          if (outcome.status == DynamicRewriteStatus::OK) {
            operand = DB::makeRewriterAST<DB::ASTIdentifier>(
              std::vector<std::string>{outcome.physical_db, outcome.new_table});
            if (out_table_rewrites)
              (*out_table_rewrites)[origin_full_name] = outcome.physical_db + "." + outcome.new_table;
          } else if (outcome.status == DynamicRewriteStatus::OkRemote) {
            operand = DB::makeASTFunction("remote",
              DB::makeRewriterAST<DB::ASTLiteral>(outcome.remote_addr),
              DB::makeRewriterAST<DB::ASTLiteral>(outcome.physical_db),
              DB::makeRewriterAST<DB::ASTLiteral>(outcome.new_table),
              DB::makeRewriterAST<DB::ASTLiteral>(outcome.remote_user),
              DB::makeRewriterAST<DB::ASTLiteral>(outcome.remote_password));
            if (out_table_rewrites)
              (*out_table_rewrites)[origin_full_name] = outcome.physical_db + "." + outcome.new_table;
          }
        }
      }
      // The left operand may hold subqueries of its own.
      dynamicRewriteWalk(fn->arguments->children[0], args, out_table_rewrites, cte_scope);
      return;
    }
  }

  if (auto *table_expression = ast->as<DB::ASTTableExpression>()) {
```

In `src/handlers/select.cc`, replace:

```cpp
  populateAccessedTables(originals, selection, response);
  if (!validateStorageIntegritySelectTargets(originals, selection, response))
    return;
  if (selection.mode == TableRewriteMode::Dynamic) {
    const auto &args = *selection.dynamic_args;
    if (rejectStorageIntegrityNamespaces(
          response, collectStorageIntegrityNamespaceRefs(ast), args,
          rewriter::RewriteCode::RewriteError)) return;
```

with:

```cpp
  // An unqualified IN operand read through an SI physical execution context
  // stays with the SI namespace policy, which names the physical table.
  const bool si_physical_context = selection.mode == TableRewriteMode::Dynamic
    && isStorageIntegrityPhysicalDatabase(
      storageIntegrityExecutionDatabase(*selection.dynamic_args), *selection.dynamic_args);
  if (si_physical_context) {
    std::erase_if(originals, [](const auto &entry) {
      return entry.second.in_operand && entry.second.original_db.empty();
    });
  }
  populateAccessedTables(originals, selection, response);
  if (!validateStorageIntegritySelectTargets(originals, selection, response))
    return;
  if (selection.mode == TableRewriteMode::Dynamic) {
    const auto &args = *selection.dynamic_args;
    // IN operands are read sources in `originals` now (T4): the SI target
    // check above already classified them. Only indirect namespaces remain.
    auto refs = collectStorageIntegrityNamespaceRefs(ast);
    if (!si_physical_context) refs = withoutInOperandRefs(std::move(refs));
    if (rejectStorageIntegrityNamespaces(
          response, refs, args, rewriter::RewriteCode::RewriteError)) return;
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefSources.*:StorageIntegrityIdentifierEscapes*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 994 tests`); corpus failures 381 → 327 (54 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/table_reference.h src/handlers/table_reference.cc src/handlers/storage_integrity.h src/handlers/storage_integrity.cc src/handlers/select.cc
git commit -m "feat(table-ref): rewrite and report IN operands like FROM (T4)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: T4 INSERT … SELECT, CTAS (incl. EMPTY) and view() bodies are rewritten and reported

The body of an INSERT … SELECT and of a CREATE TABLE … AS SELECT (EMPTY included) goes through the SELECT pipeline: names are rewritten, sources reported after the target, and an authorized Active FROM / JOIN source becomes the derived read unless the target itself is Active. `view(SELECT …)` bodies are rewritten. A refusal that fired on a table source still reports and rewrites the target first. While the surface is active, a write body keeps refusing an IN operand that names an SI object — rewriter-go's write-path namespace policy (`CollectNamespaceAndInTableRefs`) refuses it before the body pipeline — so `INSERT … WHERE a IN db1.t` is refused where `SELECT … WHERE a IN db1.t` is rewritten. A live view keeps its strict body check.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/select.h`
- Modify: `src/handlers/select.cc`
- Modify: `src/handlers/writes.cc`

**Corpus cases:** 19: 17 M (15 M-T4, 2 M-reject-acc) + 1 G1 (`si_tr_ctas_empty_own_source_rewritten`) + 1 new (`si_tr_ctas_empty_active_source_derived_read`).

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, replace:

```cpp
TEST(WriteOp, CreateTable_AsSelect_ReturnsTargetOnly) {
  auto r = RunWriteOp("CREATE TABLE t ENGINE=Memory AS SELECT * FROM src");
  EXPECT_EQ(r.code, rewriter::RewriteCode::Success);
  EXPECT_EQ(r.statement_type, rewriter::STATEMENT_TYPE_CREATE_TABLE);
  // Only the CREATE TABLE target — writes.cc does not walk the AS SELECT
  // subquery into rewriteOneTarget, so `src` is not recorded here.
  EXPECT_EQ(r.original_accessed_count, 1);
}
```

with:

```cpp
TEST(WriteOp, CreateTable_AsSelect_ReturnsTargetAndSource) {
  auto r = RunWriteOp("CREATE TABLE t ENGINE=Memory AS SELECT * FROM src");
  EXPECT_EQ(r.code, rewriter::RewriteCode::Success);
  EXPECT_EQ(r.statement_type, rewriter::STATEMENT_TYPE_CREATE_TABLE);
  // Spec 2026-09-26 T4: the AS SELECT body is a read source, reported after
  // the target in every mode.
  EXPECT_EQ(r.original_accessed_count, 2);
}
```

In `tests/rewriter_test.cc`, replace:

```cpp
TEST(WriteOp, Insert_Select_ReturnsTargetOnly) {
  auto r = RunWriteOp("INSERT INTO db.t SELECT * FROM src");
  EXPECT_EQ(r.code, rewriter::RewriteCode::Success);
  EXPECT_EQ(r.statement_type, rewriter::STATEMENT_TYPE_INSERT);
  // Only the INSERT target (db.t). writes.cc doesn't walk the SELECT subquery
  // into rewriteOneTarget, so `src` doesn't get its own AccessedTable entry.
  EXPECT_EQ(r.original_accessed_count, 1);
}
```

with:

```cpp
TEST(WriteOp, Insert_Select_ReturnsTargetAndSource) {
  auto r = RunWriteOp("INSERT INTO db.t SELECT * FROM src");
  EXPECT_EQ(r.code, rewriter::RewriteCode::Success);
  EXPECT_EQ(r.statement_type, rewriter::STATEMENT_TYPE_INSERT);
  // Spec 2026-09-26 T4: the SELECT body is a read source, reported after the
  // target (db.t) in every mode.
  EXPECT_EQ(r.original_accessed_count, 2);
}
```

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 T4: an INSERT … SELECT, CREATE TABLE … AS SELECT (EMPTY
// included) and view() body is rewritten and reported like FROM, its sources
// after the target; an Active source becomes the derived read unless the
// target itself is Active.
TEST(TableRefSources, EmbeddedBodiesAreRewrittenAndReported) {
  using Pairs = std::vector<std::pair<std::string, std::string>>;
  struct Case { const char *sql; const char *want_sql; Pairs accessed; bool si = false; };
  const std::vector<Case> cases = {
    {"INSERT INTO db1.o SELECT * FROM db1.p",
     "INSERT INTO phys.`db1.o` SELECT * FROM phys.`db1.p` AS `db1.p`", {{"db1", "o"}, {"db1", "p"}}},
    {"INSERT INTO db1.o SELECT * FROM p",
     "INSERT INTO phys.`db1.o` SELECT * FROM phys.`db1.p` AS p", {{"db1", "o"}, {"", "p"}}},
    {"INSERT INTO db1.o SELECT * FROM (SELECT * FROM db1.p WHERE a IN db1.q)",
     "INSERT INTO phys.`db1.o` SELECT * FROM (SELECT * FROM phys.`db1.p` AS `db1.p` WHERE a IN (phys.`db1.q`))",
     {{"db1", "o"}, {"db1", "p"}, {"db1", "q"}}},
    {"CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM db1.p",
     "CREATE TABLE phys.`db1.n` ENGINE = Memory AS SELECT * FROM phys.`db1.p` AS `db1.p`", {{"db1", "n"}, {"db1", "p"}}},
    {"CREATE TABLE db1.n ENGINE = Memory EMPTY AS SELECT * FROM db1.p",
     "CREATE TABLE phys.`db1.n` ENGINE = Memory EMPTY AS SELECT * FROM phys.`db1.p` AS `db1.p`",
     {{"db1", "n"}, {"db1", "p"}}},
    {"SELECT * FROM view(SELECT * FROM db1.o)",
     "SELECT * FROM view(SELECT * FROM phys.`db1.o` AS `db1.o`)", {{"db1", "o"}}},
    {"INSERT INTO db1.o SELECT * FROM db1.t",
     "INSERT INTO phys.`db1.o` SELECT * FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS `db1.t`",
     {{"db1", "o"}, {"db1", "t"}}, true},
    {"CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM db1.t",
     "CREATE TABLE phys.`db1.n` ENGINE = Memory AS SELECT * FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS `db1.t`",
     {{"db1", "n"}, {"db1", "t"}}, true},
    {"INSERT INTO db1.t SELECT * FROM db1.o",
     "INSERT INTO phys.`db1.t` SELECT * FROM phys.`db1.o` AS `db1.o`", {{"db1", "t"}, {"db1", "o"}}, true},
  };
  for (const auto &c : cases) {
    SCOPED_TRACE(c.sql);
    const auto resp = RunTableRef(c.sql, c.si);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
    EXPECT_EQ(resp.sql_after_rewrite(), c.want_sql);
    Pairs got;
    for (const auto &a : resp.original_accessed_tables()) got.emplace_back(a.original_database(), a.original_table());
    EXPECT_EQ(got, c.accessed);
  }
  // While the SI surface is active an IN operand naming an SI object stays
  // refused in every write body (rewriter-go's write-path namespace policy),
  // even where the SELECT root would rewrite it.
  for (const auto &[sql, message] : std::vector<std::pair<std::string, std::string>>{
         {"INSERT INTO db1.o SELECT * FROM db1.p WHERE a IN db1.t",
          "storage-integrity table db1.t is not directly addressable through IN table target"},
         {"INSERT INTO db1.o SELECT * FROM (SELECT * FROM db1.p WHERE a IN db1.q)",
          "storage-integrity logical database db1 is not directly addressable through IN table target"},
         {"CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM db1.p WHERE a IN db1.t",
          "storage-integrity table db1.t is not directly addressable through IN table target"}}) {
    SCOPED_TRACE(sql);
    const auto resp = RunTableRef(sql, /*si=*/true);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.message(), message);
    EXPECT_EQ(resp.sql_after_rewrite(), sql);
  }
  // A CTAS into an Active target stays refused.
  EXPECT_EQ(RunTableRef("CREATE TABLE db1.t ENGINE = Memory AS SELECT * FROM db1.o", true).code(),
            rewriter::RewriteCode::UnsupportedStatement);
  // A refused physical source still reports and rewrites the target first.
  const auto physical = RunTableRef("CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM hg_safe.db1__t", true);
  EXPECT_EQ(physical.code(), rewriter::RewriteCode::UnsupportedStatement);
  ASSERT_EQ(physical.original_accessed_tables_size(), 2);
  EXPECT_EQ(physical.original_accessed_tables(0).original_table(), "n");
  EXPECT_EQ(physical.original_accessed_tables(1).original_database(), "hg_safe");
  EXPECT_EQ(physical.table_rewrites().at("db1.n"), "phys.db1.n");
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefSources.*:WriteOp.*'
```

Expected: `TableRefSources.EmbeddedBodiesAreRewrittenAndReported` and the two renamed `WriteOp.*_ReturnsTargetAndSource` tests fail (bodies are forwarded verbatim; one accessed entry). Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `TableRefSources.EmbeddedBodiesAreRewrittenAndReported`, `WriteOp.CreateTable_AsSelect_ReturnsTargetAndSource`, `WriteOp.Insert_Select_ReturnsTargetAndSource`.

- [ ] **Step 3: Implement.**

In `src/handlers/select.h`, replace:

```cpp
void rewriteEmbeddedViewBody(
  const google::protobuf::RepeatedPtrField<rewriter::RewriteOption> &options,
  DB::ASTPtr body,
  rewriter::RewriteSQLResponse *response);
```

with:

```cpp
void rewriteEmbeddedViewBody(
  const google::protobuf::RepeatedPtrField<rewriter::RewriteOption> &options,
  DB::ASTPtr body,
  rewriter::RewriteSQLResponse *response);

// Which statement embeds the body. A view body refuses an SI read; an
// INSERT … SELECT or CREATE TABLE … AS SELECT body reads an Active table
// through the derived read and reports every source (spec 2026-09-26 T4).
enum class EmbeddedBodyKind { View, InsertSelect, CreateSelect };
void rewriteEmbeddedBody(
  const google::protobuf::RepeatedPtrField<rewriter::RewriteOption> &options,
  DB::ASTPtr body,
  rewriter::RewriteSQLResponse *response,
  EmbeddedBodyKind kind);
```

In `src/handlers/select.h`, replace:

```cpp
bool rejectEmbeddedStorageIntegrityRead(
  DB::ASTPtr body,
  const TableRewriteSelection &selection,
  rewriter::RewriteSQLResponse *response,
  rewriter::RewriteCode code = rewriter::RewriteCode::UnsupportedStatement);
```

with:

```cpp
// allow_logical_reads admits an authorized Active SI table in a FROM / JOIN
// position (the body reads it through the derived read): set only for the
// body of an INSERT … SELECT / CTAS whose target is ordinary (spec 2026-09-26
// T4). Every other caller refuses it. An IN operand naming an SI object is
// refused in every write body, as rewriter-go's write-path namespace policy
// refuses it.
// *rejected_table_source reports whether the refusal named a table source
// (rather than a namespace), for the target-first accessed order.
bool rejectEmbeddedStorageIntegrityRead(
  DB::ASTPtr body,
  const TableRewriteSelection &selection,
  rewriter::RewriteSQLResponse *response,
  rewriter::RewriteCode code = rewriter::RewriteCode::UnsupportedStatement,
  bool allow_logical_reads = false,
  bool *rejected_table_source = nullptr);
```

In `src/handlers/select.cc`, replace:

```cpp
    } else if (table_expression->subquery) {
      visitChildrenWithCTEVisibility(
```

with:

```cpp
    } else if (const auto *fn = table_expression->table_function
                 ? table_expression->table_function->as<DB::ASTFunction>() : nullptr;
               fn && fn->name == "view") {
      // view(SELECT …) is a data-only table function whose body is an
      // ordinary read (spec 2026-09-26 §5 table functions): rewrite it.
      visitChildrenWithCTEVisibility(
        *table_expression->table_function, cte_scope,
        [&](const DB::ASTPtr &child, const CTEAliasScope &child_scope) {
          dynamicRewriteWalk(child, args, out_table_rewrites, child_scope);
        });
    } else if (table_expression->subquery) {
      visitChildrenWithCTEVisibility(
```

In `src/handlers/select.cc`, replace:

```cpp
  rewriter::RewriteSQLResponse *response,
  rewriter::RewriteCode code) {
  if (!body || selection.mode != TableRewriteMode::Dynamic
```

with:

```cpp
  rewriter::RewriteSQLResponse *response,
  rewriter::RewriteCode code,
  bool allow_logical_reads,
  bool *rejected_table_source) {
  if (rejected_table_source) *rejected_table_source = false;
  if (!body || selection.mode != TableRewriteMode::Dynamic
```

In `src/handlers/select.cc`, replace:

```cpp
  for (const auto &event : collectStorageIntegrityReadRefs(body)) {
    if (event.kind == StorageIntegrityReadRef::Kind::Table) {
      const AccessedTablePair pair{event.database, event.table};
      if (auto hit = lookupStorageIntegrity(
            pair.original_db, pair.original_table, args)) {
        recordStorageIntegrityPairUnique(response, pair, selection);
        std::string logical;
        if (!authorizeStorageIntegrityLogical(pair.original_db, args, &logical)) {
```

with:

```cpp
  for (const auto &event : collectStorageIntegrityReadRefs(body)) {
    if (event.kind == StorageIntegrityReadRef::Kind::Table) {
      const AccessedTablePair pair{event.database, event.table};
      if (auto hit = lookupStorageIntegrity(
            pair.original_db, pair.original_table, args)) {
        std::string logical;
        const bool authorized = authorizeStorageIntegrityLogical(pair.original_db, args, &logical);
        if (allow_logical_reads && authorized) continue;  // the derived read
        if (rejected_table_source) *rejected_table_source = true;
        recordStorageIntegrityPairUnique(response, pair, selection);
        if (!authorized) {
```

In `src/handlers/select.cc`, replace:

```cpp
      if (isStorageIntegrityPhysicalDatabase(physical, args)) {
        recordStorageIntegrityPairUnique(response, pair, selection);
        response->set_code(code);
        response->set_message(storageIntegrityPhysicalRejectMessage(
          physical + "." + pair.original_table));
        return true;
      }
      continue;
```

with:

```cpp
      if (isStorageIntegrityPhysicalDatabase(physical, args)) {
        if (rejected_table_source) *rejected_table_source = true;
        recordStorageIntegrityPairUnique(response, pair, selection);
        response->set_code(code);
        response->set_message(storageIntegrityPhysicalRejectMessage(
          physical + "." + pair.original_table));
        return true;
      }
      continue;
```

In `src/handlers/select.cc`, replace:

```cpp
void rewriteEmbeddedViewBody(
  const google::protobuf::RepeatedPtrField<rewriter::RewriteOption> &options,
  DB::ASTPtr body,
  rewriter::RewriteSQLResponse *response) {
  if (!body) return;
```

with:

```cpp
void rewriteEmbeddedViewBody(
  const google::protobuf::RepeatedPtrField<rewriter::RewriteOption> &options,
  DB::ASTPtr body,
  rewriter::RewriteSQLResponse *response) {
  rewriteEmbeddedBody(options, body, response, EmbeddedBodyKind::View);
}

void rewriteEmbeddedBody(
  const google::protobuf::RepeatedPtrField<rewriter::RewriteOption> &options,
  DB::ASTPtr body,
  rewriter::RewriteSQLResponse *response,
  EmbeddedBodyKind kind) {
  if (!body) return;
```

In `src/handlers/select.cc`, replace:

```cpp
    if (rejectStorageIntegrityNamespaces(
          response, collectStorageIntegrityNamespaceRefs(body), args,
          rewriter::RewriteCode::UnsupportedStatement)) return;
    if (const std::string key = recordLogicalStorageIntegrityRewrites(
          originals, args, response);
        !key.empty()) {
```

with:

```cpp
    // Every body keeps the IN-operand namespace refusal; a view body may not
    // capture an SI read either, while an INSERT … SELECT / CTAS body reads
    // an Active FROM / JOIN table through the derived read (spec 2026-09-26
    // T4).
    const bool view = kind == EmbeddedBodyKind::View;
    if (rejectStorageIntegrityNamespaces(
          response, collectStorageIntegrityNamespaceRefs(body), args,
          rewriter::RewriteCode::UnsupportedStatement)) return;
    if (!view) {
      recordLogicalStorageIntegrityRewrites(originals, args, response);
    } else if (const std::string key = recordLogicalStorageIntegrityRewrites(
          originals, args, response);
        !key.empty()) {
```

In `src/handlers/writes.cc`, replace:

```cpp
void acceptWrite(const DB::ASTPtr &ast,
                 rewriter::RewriteSQLResponse *response,
                 rewriter::StatementType stmt_type) {
  setSuccessResponse(response, formatAst(ast), stmt_type);
}
```

with:

```cpp
// The formatted write. An INSERT … SELECT / CTAS body that reads an Active
// table carries the derived read, whose projection spelling is restored
// exactly as the SELECT path restores it.
std::string formatWrite(const DB::ASTPtr &ast, const rewriter::RewriteSQLResponse &response,
                        const TableRewriteSelection &sel) {
  std::string sql = formatAst(ast);
  if (sel.mode != TableRewriteMode::Dynamic) return sql;
  for (const auto &entry : response.original_accessed_tables()) {
    if (entry.is_storage_integrity())
      return restoreStorageIntegrityProjectionSyntax(std::move(sql), reservedRowIdColumn(*sel.dynamic_args));
  }
  return sql;
}

void acceptWrite(const DB::ASTPtr &ast,
                 rewriter::RewriteSQLResponse *response,
                 rewriter::StatementType stmt_type) {
  setSuccessResponse(response, formatAst(ast), stmt_type);
}
```

In `src/handlers/writes.cc`, replace:

```cpp
DB::ASTPtr ownedChild(const DB::IAST &parent, const DB::IAST *child) {
```

with:

```cpp
// Record a write target ahead of every entry already present, with its
// table rewrite: a rejection that fired on a table source still reports the
// target first, as rewriter-go does (spec 2026-09-26 §5 "What the engines
// report").
void prependAccessedTable(rewriter::RewriteSQLResponse *response,
                          const TableRewriteSelection &sel,
                          const std::string &database,
                          const std::string &table) {
  recordAccessedTable(response, database, table, sel);
  auto *list = response->mutable_original_accessed_tables();
  for (int i = list->size() - 1; i > 0; --i) list->SwapElements(i, i - 1);
  if (sel.mode == TableRewriteMode::Dynamic) {
    const auto outcome = applyDynamicRewrite(database, table, *sel.dynamic_args, "write target");
    if (outcome.status == DynamicRewriteStatus::OK)
      recordTableRewrite(response, database, table, outcome.physical_db, outcome.new_table);
  }
}

DB::ASTPtr ownedChild(const DB::IAST &parent, const DB::IAST *child) {
```

In `src/handlers/writes.cc`, replace:

```cpp
  if (auto *create = ast->as<DB::ASTCreateQuery>()) {
    if (reject_namespaces()) return true;
```

with:

```cpp
  if (auto *create = ast->as<DB::ASTCreateQuery>()) {
    if (reject_namespaces()) return true;
    const bool ctas = !create->is_ordinary_view && !create->is_materialized_view && create->select;
```

In `src/handlers/writes.cc`, replace:

```cpp
    if (!create->is_ordinary_view && !create->is_materialized_view
        && create->select) {
      if (auto body = ownedChild(*create, create->select);
          body && rejectEmbeddedStorageIntegrityRead(body, sel, response)) {
        return true;
      }
    }
```

with:

```cpp
    if (ctas) {
      bool table_source = false;
      if (auto body = ownedChild(*create, create->select);
          body && rejectEmbeddedStorageIntegrityRead(
            body, sel, response, rewriter::RewriteCode::UnsupportedStatement,
            /*allow_logical_reads=*/true, &table_source)) {
        if (table_source) prependAccessedTable(response, sel, create->getDatabase(), create->getTable());
        return true;
      }
    }
```

In `src/handlers/writes.cc`, replace:

```cpp
  if (auto *insert = ast->as<DB::ASTInsertQuery>()) {
    if (reject_namespaces()) return true;
    if (insert->table && rejectStorageIntegrityWriteTarget(
          response, sel, insert->getDatabase(), insert->getTable(), true)) {
      return true;
    }
    if (insert->select) {
      if (rejectEmbeddedStorageIntegrityRead(
            insert->select, sel, response)) {
        return true;
      }
    }
    return false;
  }
```

with:

```cpp
  if (auto *insert = ast->as<DB::ASTInsertQuery>()) {
    if (reject_namespaces()) return true;
    if (insert->table && rejectStorageIntegrityWriteTarget(
          response, sel, insert->getDatabase(), insert->getTable(), true)) {
      return true;
    }
    if (insert->select) {
      // An ordinary target reads an Active source through the derived read;
      // an SI target keeps the refusal (spec 2026-09-26 §7 "Sources").
      const bool si_target = insert->table
        && lookupStorageIntegrity(insert->getDatabase(), insert->getTable(), args).has_value();
      bool table_source = false;
      if (rejectEmbeddedStorageIntegrityRead(
            insert->select, sel, response, rewriter::RewriteCode::UnsupportedStatement,
            /*allow_logical_reads=*/!si_target, &table_source)) {
        if (table_source) prependAccessedTable(response, sel, insert->getDatabase(), insert->getTable());
        return true;
      }
    }
    return false;
  }
```

In `src/handlers/writes.cc`, replace:

```cpp
  const auto sel = findActiveTableRewrite(request->options());
  if (preflightStorageIntegrityWrite(ast, sel, response)) return true;

  rejectUnsupported(
```

with:

```cpp
  const auto sel = findActiveTableRewrite(request->options());
  if (preflightStorageIntegrityWrite(ast, sel, response)) return true;
  // A live view is a view: its body may not read an Active table, which the
  // CREATE preflight (now a CTAS-shaped create) no longer refuses (T4).
  if (auto body = ownedChild(*create, create->select);
      body && rejectEmbeddedStorageIntegrityRead(body, sel, response)) return true;

  rejectUnsupported(
```

In `src/handlers/writes.cc`, replace:

```cpp
    // CREATE TABLE x AS y — rewrite the AS source the same way. as_database /
    // as_table are plain Strings; the lambdas assign through to them.
```

with:

```cpp
    // Spec 2026-09-26 T4: a CREATE TABLE … AS SELECT body (EMPTY and OR
    // REPLACE included) is rewritten and reported like a view body; its
    // sources follow the target in original_accessed_tables.
    if (create_query->select) {
      if (auto body = ownedChild(*create_query, create_query->select)) {
        rewriteEmbeddedBody(request->options(), body, response, EmbeddedBodyKind::CreateSelect);
        if (response->code() != rewriter::RewriteCode::Success)
          return WriteDispatchResult::Rejected;
      }
    }

    // CREATE TABLE x AS y — rewrite the AS source the same way. as_database /
    // as_table are plain Strings; the lambdas assign through to them.
```

In `src/handlers/writes.cc`, replace:

```cpp
    acceptWrite(ast, response, rewriter::STATEMENT_TYPE_CREATE_TABLE);
    return WriteDispatchResult::Handled;
```

with:

```cpp
    setSuccessResponse(response, formatWrite(ast, *response, sel), rewriter::STATEMENT_TYPE_CREATE_TABLE);
    return WriteDispatchResult::Handled;
```

In `src/handlers/writes.cc`, replace:

```cpp
    // ASTInsertQuery::formatImpl only emits the prelude
```

with:

```cpp
    if (insert_query->select) {
      if (auto body = ownedChild(*insert_query, insert_query->select.get())) {
        rewriteEmbeddedBody(request->options(), body, response, EmbeddedBodyKind::InsertSelect);
        if (response->code() != rewriter::RewriteCode::Success)
          return WriteDispatchResult::Rejected;
      }
    }
    // ASTInsertQuery::formatImpl only emits the prelude
```

In `src/handlers/writes.cc`, replace:

```cpp
    std::string sql = formatAst(ast);
    if (insert_query->data && insert_query->end > insert_query->data) {
```

with:

```cpp
    std::string sql = formatWrite(ast, *response, sel);
    if (insert_query->data && insert_query->end > insert_query->data) {
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefSources.*:WriteOp.*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 1014 tests`); corpus failures 327 → 308 (19 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/select.h src/handlers/select.cc src/handlers/writes.cc
git commit -m "feat(table-ref): rewrite and report INSERT … SELECT, CTAS and view() bodies (T4)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 10: T12 a materialized view's TO / APPEND TO target is a write target

`CREATE MATERIALIZED VIEW … TO db1.t` (and `REFRESH … APPEND TO`) writes into its target, so an Active target is refused by the SI write preflight like any other write.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/writes.cc`

**Corpus cases:** 2 M-T12.

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 T12: a materialized view writes into its TO / APPEND TO
// target, so an Active target is refused with rewriter-go's message.
TEST(TableRefParity, MaterializedViewToActiveTableIsRefused) {
  for (const char *sql : {"CREATE MATERIALIZED VIEW db1.mv TO db1.t AS SELECT * FROM db1.o",
                          "CREATE MATERIALIZED VIEW db1.mv REFRESH EVERY 1 HOUR APPEND TO db1.t AS SELECT * FROM db1.o"}) {
    SCOPED_TRACE(sql);
    const auto resp = RunTableRef(sql, /*si=*/true);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.message(), rewriter_handlers::storageIntegrityWriteRejectMessage("db1.t"));
    EXPECT_EQ(resp.sql_after_rewrite(), sql);
  }
  const auto ok = RunTableRef("CREATE MATERIALIZED VIEW db1.mv TO db1.o AS SELECT * FROM db1.p", /*si=*/true);
  EXPECT_EQ(ok.code(), rewriter::RewriteCode::Success) << ok.message();
  // An SI physical target is refused as the physical table it names.
  for (const char *db : {"hg_safe", "hg_unsafe", "hg_promote"}) {
    const std::string sql = std::string("CREATE MATERIALIZED VIEW db1.mv TO ") + db + ".`db2.x` AS SELECT * FROM db1.o";
    SCOPED_TRACE(sql);
    const auto resp = RunTableRef(sql, /*si=*/true);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.message(), std::string("storage-integrity physical table ") + db + ".db2.x is not directly addressable");
    ASSERT_EQ(resp.original_accessed_tables_size(), 1);
    EXPECT_TRUE(resp.original_accessed_tables(0).is_storage_integrity());
  }
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefParity.*'
```

Expected: `TableRefParity.MaterializedViewToActiveTableIsRefused` fails (`Success`). Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `TableRefParity.MaterializedViewToActiveTableIsRefused`.

- [ ] **Step 3: Implement.**

In `src/handlers/writes.cc`, replace:

```cpp
    if (!create->as_table.empty()
        && rejectStorageIntegrityWriteTarget(
          response, sel, create->as_database, create->as_table, false)) {
      return true;
    }
```

with:

```cpp
    if (!create->as_table.empty()
        && rejectStorageIntegrityWriteTarget(
          response, sel, create->as_database, create->as_table, false)) {
      return true;
    }
    // Spec 2026-09-26 T12: a materialized view writes into its TO / APPEND TO
    // target, so an Active target is refused like any other write.
    if (create->targets && create->hasTargetTableID(DB::ViewTarget::To)) {
      const auto to = create->getTargetTableID(DB::ViewTarget::To);
      if (rejectStorageIntegrityWriteTarget(response, sel, to.database_name, to.table_name, false))
        return true;
    }
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefParity.*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 1017 tests`); corpus failures 308 → 306 (2 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/writes.cc
git commit -m "feat(storage-integrity): refuse a materialized view writing into an Active table (T12)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: T5 table-function allowlist; SI messages precede it while the surface is active

Classify every source-role table function (rewriter-go `internal/engine/allowlists.go`): refused, data-only, external, or unknown. While the surface is inactive the preflight runs it; while active, the SI SELECT and write paths run it after the SI namespace policy, so `merge('hg_safe', …)` keeps its SI message. ALTER commands are not source positions. This task also lands the precedence test that pins T2 → T7 → T3 → T5.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/table_reference.h`
- Modify: `src/handlers/table_reference.cc`
- Modify: `src/handlers/select.cc`
- Modify: `src/handlers/writes.cc`

**Corpus cases:** 23 M-T5fn.

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, replace:

```cpp
  const std::vector<std::string> literal_backslash_x = {
    "SELECT * FROM merge('db\\\\x31', 't')",
    "SELECT * FROM remote('host', 'db\\\\x31', 't')",
  };
  for (const auto &sql : literal_backslash_x) {
    SCOPED_TRACE(sql);
    rewriter::RewriteSQLRequest req;
    req.set_sql(sql);
    AddSIContractOption(req);

    const auto resp = RunDirectRewrite(std::move(req));
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
    for (const auto &access : resp.original_accessed_tables())
      EXPECT_FALSE(access.is_storage_integrity());
  }
}
```

with:

```cpp
  // A literal is not decoded again, so it names no SI namespace; the table
  // function itself is refused by the T5 allowlist after the SI policy.
  const std::vector<std::pair<std::string, std::string>> literal_backslash_x = {
    {"SELECT * FROM merge('db\\\\x31', 't')", "table function merge is not accepted"},
    {"SELECT * FROM remote('host', 'db\\\\x31', 't')", "table function remote is not accepted"},
  };
  for (const auto &[sql, message] : literal_backslash_x) {
    SCOPED_TRACE(sql);
    rewriter::RewriteSQLRequest req;
    req.set_sql(sql);
    AddSIContractOption(req);

    const auto resp = RunDirectRewrite(std::move(req));
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.message(), message);
    for (const auto &access : resp.original_accessed_tables())
      EXPECT_FALSE(access.is_storage_integrity());
  }
}
```

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 T5: the table-function allowlist, matched
// case-insensitively; the message names the function as written.
TEST(TableRefAllowlist, TableFunctions) {
  for (const char *fn : {"merge('db1', 'o')", "remote('h', 'db1', 'o')", "remoteSecure('h', 'db1', 'o')",
                         "cluster('c', db1.o)", "clusterAllReplicas('c', db1.o)", "loop('db1', 'o')",
                         "dictionary(db1.d)", "mergeTreeIndex('db1', 'o')", "mergeTreeProjection('db1', 'o', 'p')",
                         "timeSeriesData('db1', 'o')", "prometheusQuery('db1', 'o', 'up')", "clickhouse('db1.o')",
                         "mysql('h', 'db1', 'o', 'u', 'p')", "postgresql('h', 'db1', 'o', 'u', 'p')",
                         "jdbc('ds', 'db1', 'o')", "odbc('ds', 'db1', 'o')", "executable('x.sh', 'TSV', 'a UInt8')",
                         "fuzzQuery('SELECT 1')", "MERGE('db1', 'o')"}) {
    const std::string sql = std::string("SELECT * FROM ") + fn;
    const std::string name(fn, std::string_view(fn).find('('));
    SCOPED_TRACE(sql);
    const auto resp = RunTableRef(sql);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.message(), "table function " + name + " is not accepted");
  }
  const auto unknown = RunTableRef("SELECT * FROM frobnicate('x')");
  EXPECT_EQ(unknown.code(), rewriter::RewriteCode::UnsupportedStatement);
  EXPECT_EQ(unknown.message(), "table function frobnicate is not recognised");
  const auto insert_function = RunTableRef("INSERT INTO FUNCTION remote('h', 'db1', 'o') VALUES (1)");
  EXPECT_EQ(insert_function.message(), "table function remote is not accepted");
  for (const char *sql : {"SELECT * FROM numbers(10)", "SELECT * FROM url('http://127.0.0.1/x', CSV)",
                          "INSERT INTO db1.o SELECT * FROM input('a UInt8')"}) {
    SCOPED_TRACE(sql);
    EXPECT_EQ(RunTableRef(sql).code(), rewriter::RewriteCode::Success);
  }
}

// While the SI surface is active T5 runs after the SI namespace policy, so an
// SI-owned message wins (rewriter-go rejectDisallowedCarriers call sites).
TEST(TableRefAllowlist, StorageIntegrityMessagesPrecedeAllowlistsWhenActive) {
  const auto si = RunTableRef("SELECT * FROM merge('hg_safe', 'db1__t')", /*si=*/true);
  EXPECT_EQ(si.code(), rewriter::RewriteCode::RewriteError) << si.message();
  EXPECT_EQ(si.message(), "storage-integrity physical table hg_safe.db1__t is not directly addressable");
  const auto ordinary = RunTableRef("SELECT * FROM merge('other', 'x')", /*si=*/true);
  EXPECT_EQ(ordinary.code(), rewriter::RewriteCode::UnsupportedStatement) << ordinary.message();
  EXPECT_EQ(ordinary.message(), "table function merge is not accepted");
}

// Spec 2026-09-26 §5 precedence: T2, then T7, then T3, then T5.
TEST(TableRefPrecedence, ParameterThenClassThenProtectedThenAllowlist) {
  struct Case { const char *sql; rewriter::RewriteCode code; std::string message; };
  const std::vector<Case> cases = {
    {"EXPLAIN SELECT * FROM {p:Identifier}", rewriter::RewriteCode::InvalidRewriteRequest,
     std::string(rewriter_handlers::kIdentifierParameterMessage)},
    {"DETACH TABLE phys.x", rewriter::RewriteCode::UnsupportedStatement,
     std::string(rewriter_handlers::kUnsupportedStatementMessage)},
    {"SELECT * FROM merge('phys', 'db2')", rewriter::RewriteCode::InvalidRewriteRequest,
     "protected database phys is not addressable"},
    {"SELECT * FROM merge('db1', 'o')", rewriter::RewriteCode::UnsupportedStatement,
     "table function merge is not accepted"},
  };
  for (const auto &c : cases) {
    SCOPED_TRACE(c.sql);
    const auto resp = RunTableRef(c.sql);
    EXPECT_EQ(resp.code(), c.code) << resp.message();
    EXPECT_EQ(resp.message(), c.message);
    EXPECT_EQ(resp.sql_after_rewrite(), c.sql);
    EXPECT_EQ(resp.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
  }
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefAllowlist.*:TableRefPrecedence.*:StorageIntegrityIdentifierEscapes*'
```

Expected: `TableRefAllowlist.TableFunctions` and `TableRefPrecedence.ParameterThenClassThenProtectedThenAllowlist` fail (refused functions pass). Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `StorageIntegrityNamespaces.IdentifierEscapesAreSemanticButLiteralsAreNotDecodedAgain`, `TableRefAllowlist.StorageIntegrityMessagesPrecedeAllowlistsWhenActive`, `TableRefAllowlist.TableFunctions`, `TableRefPrecedence.ParameterThenClassThenProtectedThenAllowlist`.

- [ ] **Step 3: Implement.**

In `src/handlers/table_reference.h`, replace:

```cpp
// Dynamic-mode table-reference preflight, run right after parsing, in the
```

with:

```cpp
// T5 (spec §5 "Table functions" / "Table engines", rewriter-go
// internal/engine/allowlists.go).
std::string tableFunctionRefusedMessage(const std::string &name);
std::string tableFunctionUnknownMessage(const std::string &name);
enum class TableFunctionClass { Unknown, Refused, DataOnly, External };
TableFunctionClass classifyTableFunction(const std::string &name);

// T5: refuse a disallowed source table function, CREATE / materialized-view
// engine, or disk / storage_policy table setting. Returns true when it
// populated a rejection. The preflight runs it while the SI surface is
// inactive; the SI handler paths run it after their own checks while it is
// active, so an SI-owned message wins (rewriter-go rejectDisallowedCarriers).
bool rejectDisallowedCarriers(const DB::ASTPtr &ast, rewriter::RewriteSQLResponse *response);

// Dynamic-mode table-reference preflight, run right after parsing, in the
```

In `src/handlers/table_reference.cc`, replace:

```cpp
bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response) {
```

with:

```cpp
std::string tableFunctionRefusedMessage(const std::string &name) {
  return "table function " + name + " is not accepted";
}

std::string tableFunctionUnknownMessage(const std::string &name) {
  return "table function " + name + " is not recognised";
}

TableFunctionClass classifyTableFunction(const std::string &name) {
  static const std::set<std::string> refused = {"merge", "remote", "remotesecure", "cluster",
    "clusterallreplicas", "loop", "dictionary", "mergetreeindex", "mergetreeprojection",
    "timeseriesdata", "timeseriestags", "timeseriesmetrics", "timeseriesselector",
    "prometheusquery", "prometheusqueryrange", "clickhouse", "mysql", "postgresql", "mongodb",
    "jdbc", "odbc", "executable", "fuzzquery", "fuzzjson"};
  static const std::set<std::string> data_only = {"numbers", "numbers_mt", "generate_series",
    "generateseries", "generaterandom", "zeros", "zeros_mt", "null", "values", "format", "input",
    "view"};
  static const std::set<std::string> external = {"url", "s3", "gcs", "oss", "cosn", "file",
    "hdfs", "azureblobstorage", "iceberg", "deltalake", "hudi", "urlcluster", "s3cluster",
    "filecluster", "hdfscluster", "azureblobstoragecluster", "icebergcluster",
    "deltalakecluster", "hudicluster", "sqlite", "redis"};
  const std::string lower = lowerName(name);
  if (refused.count(lower) || lower.rfind("mergetree", 0) == 0) return TableFunctionClass::Refused;
  if (data_only.count(lower)) return TableFunctionClass::DataOnly;
  if (external.count(lower)) return TableFunctionClass::External;
  return TableFunctionClass::Unknown;
}

namespace {

// Source-role table functions (rewriter-go CollectSourceFunctionNames): every
// FROM / JOIN table function at any depth, INSERT INTO FUNCTION and CREATE
// TABLE … AS function. An ALTER action is opaque text there, so its subquery
// is left to R2.
void collectSourceFunctions(const DB::IAST &node, bool in_alter, std::vector<std::string> &out) {
  if (!in_alter) {
    if (const auto *te = node.as<DB::ASTTableExpression>()) {
      if (const auto *fn = te->table_function ? te->table_function->as<DB::ASTFunction>() : nullptr)
        out.push_back(fn->name);
    }
    if (const auto *insert = node.as<DB::ASTInsertQuery>()) {
      if (const auto *fn = insert->table_function ? insert->table_function->as<DB::ASTFunction>() : nullptr)
        out.push_back(fn->name);
    }
    if (const auto *create = node.as<DB::ASTCreateQuery>()) {
      if (const auto *fn = create->as_table_function ? create->as_table_function->as<DB::ASTFunction>() : nullptr)
        out.push_back(fn->name);
    }
  }
  const auto *alter = node.as<DB::ASTAlterQuery>();
  for (const auto &child : node.children) {
    if (!child) continue;
    collectSourceFunctions(*child, in_alter || (alter && child.get() == alter->command_list), out);
  }
}

} // namespace

bool rejectDisallowedCarriers(const DB::ASTPtr &ast, rewriter::RewriteSQLResponse *response) {
  std::vector<std::string> functions;
  collectSourceFunctions(*ast, false, functions);
  for (const auto &name : functions) {
    switch (classifyTableFunction(name)) {
    case TableFunctionClass::Refused:
      response->set_code(rewriter::RewriteCode::UnsupportedStatement);
      response->set_message(tableFunctionRefusedMessage(name));
      return true;
    case TableFunctionClass::Unknown:
      response->set_code(rewriter::RewriteCode::UnsupportedStatement);
      response->set_message(tableFunctionUnknownMessage(name));
      return true;
    default:
      break;
    }
  }
  return false;
}

bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response) {
```

In `src/handlers/table_reference.cc`, replace:

```cpp
    // A blind SI physical name is reported as the SI object it is, as the SI
    // handlers report one (rewriter-go parity).
    if (active && isStorageIntegrityPhysicalDatabase(ref.database, args))
      recordPhysicalStorageIntegrityAccess(response, ref.database, "", ref.database);
    else
      recordAccessedDatabase(response, ref.database, &args);
    response->set_code(rewriter::RewriteCode::InvalidRewriteRequest);
    response->set_message(protectedDatabaseRejectMessage(ref.database));
    return true;
  }
  return false;
}
```

with:

```cpp
    // A blind SI physical name is reported as the SI object it is, as the SI
    // handlers report one (rewriter-go parity).
    if (active && isStorageIntegrityPhysicalDatabase(ref.database, args))
      recordPhysicalStorageIntegrityAccess(response, ref.database, "", ref.database);
    else
      recordAccessedDatabase(response, ref.database, &args);
    response->set_code(rewriter::RewriteCode::InvalidRewriteRequest);
    response->set_message(protectedDatabaseRejectMessage(ref.database));
    return true;
  }

  // T5 while the surface is inactive; the SI handlers run it after their own
  // namespace policy while it is active.
  if (!active && rejectDisallowedCarriers(ast, response)) return true;
  return false;
}
```

In `src/handlers/select.cc`, replace:

```cpp
    if (rejectStorageIntegrityNamespaces(
          response, refs, args, rewriter::RewriteCode::RewriteError)) return;
    if (hasUnsupportedStorageIntegrityWrapper(ast, args)) {
```

with:

```cpp
    if (rejectStorageIntegrityNamespaces(
          response, refs, args, rewriter::RewriteCode::RewriteError)) return;
    // T5 while the SI surface is active runs after the SI namespace policy.
    if (storageIntegritySurfaceActive(args) && rejectDisallowedCarriers(ast, response)) return;
    if (hasUnsupportedStorageIntegrityWrapper(ast, args)) {
```

In `src/handlers/writes.cc`, replace:

```cpp
  if (preflightStorageIntegrityWrite(ast, sel, response))
    return WriteDispatchResult::Rejected;
```

with:

```cpp
  if (preflightStorageIntegrityWrite(ast, sel, response))
    return WriteDispatchResult::Rejected;
  // The table-reference checks that the preflight runs while the SI surface
  // is inactive run here, after the SI write preflight, while it is active,
  // so an SI-owned message wins (spec 2026-09-26 §5 precedence).
  if (hasActiveStorageIntegrity(sel)
      && (ast->as<DB::ASTCreateQuery>() || ast->as<DB::ASTInsertQuery>() || ast->as<DB::ASTAlterQuery>()
          || ast->as<DB::ASTUpdateQuery>() || ast->as<DB::ASTDeleteQuery>())
      && rejectDisallowedCarriers(ast, response))
    return WriteDispatchResult::Rejected;
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefAllowlist.*:TableRefPrecedence.*:StorageIntegrityIdentifierEscapes*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 1043 tests`); corpus failures 306 → 283 (23 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/table_reference.h src/handlers/table_reference.cc src/handlers/select.cc src/handlers/writes.cc
git commit -m "feat(table-ref): table-function allowlist after the SI namespace policy (T5)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 12: T5 table-engine allowlist (case-sensitive) and disk / storage_policy table settings

Engine names are matched exactly (`ENGINE = memory` is `not recognised`; `Replicated*` only without arguments, S7b), for CREATE TABLE, a materialized view's own engine (stored as the TO target's inner engine — `ASTViewTargets::getInnerEngines`) and ALTER … MODIFY SETTING. The SI read-ref collector also reads an MV's inner engine, so `ENGINE = Merge(REGEXP('hg_.*'), …)` keeps its SI message while active.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/table_reference.h`
- Modify: `src/handlers/table_reference.cc`
- Modify: `src/handlers/storage_integrity.cc`

**Corpus cases:** 32: 25 M-T5eng + 7 M-R5set (disk / storage_policy).

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 T5: the table-engine allowlist (case-sensitive: a list
// name in another case is "not recognised"; Replicated* only without
// arguments) and the disk / storage_policy table settings, for CREATE TABLE,
// a materialized view's own storage and ALTER … MODIFY SETTING.
TEST(TableRefAllowlist, EnginesAndTableSettings) {
  // `both`: also refused as T5 while the SI surface is active. A row whose
  // engine argument names the SI logical database db1 is left out there: the
  // SI namespace policy answers it first (TableRefAllowlist.StorageIntegrity…).
  struct Case { const char *sql; std::string message; bool both; };
  const std::vector<Case> refused = {
    {"CREATE TABLE db1.n (a UInt64) ENGINE = Merge('db1', '^o')", "table engine Merge is not accepted", false},
    {"CREATE TABLE db1.n (a UInt64) ENGINE = Buffer(db1.o, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
     "table engine Buffer is not accepted", false},
    {"CREATE TABLE db1.n (a UInt64) ENGINE = Distributed(default, db1.o)", "table engine Distributed is not accepted", false},
    {"CREATE TABLE db1.n (a UInt64) ENGINE = URL('http://127.0.0.1/x', CSV)", "table engine URL is not accepted", true},
    {"CREATE TABLE db1.n (a UInt64) ENGINE = KeeperMap('/x')", "table engine KeeperMap is not accepted", true},
    {"CREATE TABLE db1.n (a UInt64) ENGINE = Frob", "table engine Frob is not accepted", true},
    {"CREATE TABLE db1.n (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/x', 'r1') ORDER BY a",
     "table engine ReplicatedMergeTree is not accepted", true},
    {"CREATE TABLE db1.n (a UInt64) ENGINE = memory", "table engine memory is not recognised", true},
    {"CREATE TABLE db1.n (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 's3'",
     "table setting storage_policy is not accepted", true},
    {"CREATE TABLE db1.n (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS disk = 'd'",
     "table setting disk is not accepted", true},
    {"ALTER TABLE db1.o MODIFY SETTING disk = 'd'", "table setting disk is not accepted", true},
    {"CREATE MATERIALIZED VIEW db1.mv ENGINE = Merge(currentDatabase(), '^db2') AS SELECT * FROM db1.o",
     "table engine Merge is not accepted", true},
    {"CREATE MATERIALIZED VIEW db1.mv ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'p' AS SELECT * FROM db1.o",
     "table setting storage_policy is not accepted", true},
  };
  for (const bool si : {false, true}) {
    for (const auto &c : refused) {
      if (si && !c.both) continue;
      SCOPED_TRACE(std::string(c.sql) + (si ? " [V2]" : " [inactive]"));
      const auto resp = RunTableRef(c.sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
      EXPECT_EQ(resp.message(), c.message);
    }
  }
  for (const char *sql : {"CREATE TABLE db1.n (a UInt64) ENGINE = MergeTree ORDER BY a",
                          "CREATE TABLE db1.n (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a",
                          "CREATE TABLE db1.n (a UInt64) ENGINE = Memory"}) {
    SCOPED_TRACE(sql);
    EXPECT_EQ(RunTableRef(sql).code(), rewriter::RewriteCode::Success);
  }
  // While the SI surface is active the SI namespace policy reads a
  // materialized view's own engine first.
  const auto regexp = RunTableRef(
    "CREATE MATERIALIZED VIEW db1.mv ENGINE = Merge(REGEXP('hg_.*'),'.*') AS SELECT * FROM db1.o", /*si=*/true);
  EXPECT_EQ(regexp.code(), rewriter::RewriteCode::UnsupportedStatement);
  EXPECT_EQ(regexp.message(), "storage-integrity Merge table engine namespace is not statically resolvable");
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefAllowlist.EnginesAndTableSettings'
```

Expected: `TableRefAllowlist.EnginesAndTableSettings` fails on every refused row. Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `TableRefAllowlist.EnginesAndTableSettings`.

- [ ] **Step 3: Implement.**

In `src/handlers/table_reference.h`, replace:

```cpp
enum class TableFunctionClass { Unknown, Refused, DataOnly, External };
TableFunctionClass classifyTableFunction(const std::string &name);
```

with:

```cpp
enum class TableFunctionClass { Unknown, Refused, DataOnly, External };
TableFunctionClass classifyTableFunction(const std::string &name);
std::string tableEngineRefusedMessage(const std::string &name);
std::string tableEngineUnknownMessage(const std::string &name);
std::string tableSettingRefusedMessage(const std::string &name);
enum class TableEngineClass { Refused, Allowed, Unknown };
TableEngineClass classifyTableEngine(const std::string &name, size_t argument_count);
bool refusedTableSetting(const std::string &name);
```

In `src/handlers/table_reference.cc`, replace:

```cpp
namespace {

// Source-role table functions (rewriter-go CollectSourceFunctionNames): every
```

with:

```cpp
std::string tableEngineRefusedMessage(const std::string &name) {
  return "table engine " + name + " is not accepted";
}

std::string tableEngineUnknownMessage(const std::string &name) {
  return "table engine " + name + " is not recognised";
}

std::string tableSettingRefusedMessage(const std::string &name) {
  return "table setting " + name + " is not accepted";
}

// Engine names are case-sensitive in ClickHouse (`ENGINE = memory` is an
// unknown engine), so a name is matched exactly; a list name in another case
// is Unknown (rewriter-go R12). Replicated* is allowed only without arguments.
TableEngineClass classifyTableEngine(const std::string &name, size_t argument_count) {
  static const std::set<std::string> allowed = {"MergeTree", "ReplacingMergeTree",
    "SummingMergeTree", "AggregatingMergeTree", "CollapsingMergeTree",
    "VersionedCollapsingMergeTree", "GraphiteMergeTree", "Memory", "Log", "TinyLog", "StripeLog",
    "Null", "Set", "Join", "View", "MaterializedView", "LiveView"};
  constexpr std::string_view replicated = "Replicated";
  if (name.rfind(replicated, 0) == 0 && allowed.count(name.substr(replicated.size())))
    return argument_count == 0 ? TableEngineClass::Allowed : TableEngineClass::Refused;
  if (allowed.count(name)) return TableEngineClass::Allowed;
  const std::string lower = lowerName(name);
  for (const auto &candidate : allowed) {
    if (lowerName(candidate) == lower || "replicated" + lowerName(candidate) == lower)
      return TableEngineClass::Unknown;
  }
  return TableEngineClass::Refused;
}

bool refusedTableSetting(const std::string &name) {
  const std::string lower = lowerName(name);
  return lower == "disk" || lower == "storage_policy";
}

namespace {

bool rejectEngine(const DB::ASTStorage *storage, rewriter::RewriteSQLResponse *response) {
  if (!storage) return false;
  if (storage->engine) {
    const auto &engine = *storage->engine;
    const size_t argc = engine.arguments ? engine.arguments->children.size() : 0;
    switch (classifyTableEngine(engine.name, argc)) {
    case TableEngineClass::Refused:
      response->set_code(rewriter::RewriteCode::UnsupportedStatement);
      response->set_message(tableEngineRefusedMessage(engine.name));
      return true;
    case TableEngineClass::Unknown:
      response->set_code(rewriter::RewriteCode::UnsupportedStatement);
      response->set_message(tableEngineUnknownMessage(engine.name));
      return true;
    case TableEngineClass::Allowed:
      break;
    }
  }
  if (const auto *settings = storage->settings ? storage->settings->as<DB::ASTSetQuery>() : nullptr) {
    for (const auto &change : settings->changes) {
      if (refusedTableSetting(change.name)) {
        response->set_code(rewriter::RewriteCode::UnsupportedStatement);
        response->set_message(tableSettingRefusedMessage(change.name));
        return true;
      }
    }
  }
  return false;
}

// Source-role table functions (rewriter-go CollectSourceFunctionNames): every
```

In `src/handlers/table_reference.cc`, replace:

```cpp
    default:
      break;
    }
  }
  return false;
}

bool preflightTableReferences(
```

with:

```cpp
    default:
      break;
    }
  }
  if (const auto *create = ast->as<DB::ASTCreateQuery>()) {
    if (rejectEngine(create->storage, response)) return true;
    if (create->targets) {
      // A materialized view's own ENGINE / SETTINGS (`ENGINE = X` is stored
      // as the TO target's inner engine) follow the same allowlist.
      for (const auto *inner : create->targets->getInnerEngines())
        if (rejectEngine(inner, response)) return true;
    }
  }
  if (const auto *alter = ast->as<DB::ASTAlterQuery>(); alter && alter->command_list) {
    for (const auto &raw : alter->command_list->children) {
      const auto *command = raw->as<DB::ASTAlterCommand>();
      if (!command || command->type != DB::ASTAlterCommand::MODIFY_SETTING) continue;
      const auto *settings = command->settings_changes ? command->settings_changes->as<DB::ASTSetQuery>() : nullptr;
      if (!settings) continue;
      for (const auto &change : settings->changes) {
        if (refusedTableSetting(change.name)) {
          response->set_code(rewriter::RewriteCode::UnsupportedStatement);
          response->set_message(tableSettingRefusedMessage(change.name));
          return true;
        }
      }
    }
  }
  return false;
}

bool preflightTableReferences(
```

In `src/handlers/storage_integrity.cc`, replace:

```cpp
  if (const auto *create = ast->as<DB::ASTCreateQuery>()) {
    if (create->storage && create->storage->engine) {
      if (auto ref = decodeEngine(*create->storage->engine)) {
        StorageIntegrityReadRef event;
        event.kind = StorageIntegrityReadRef::Kind::Namespace;
        event.namespace_ref = std::move(*ref);
        out.push_back(std::move(event));
      }
    }
```

with:

```cpp
  if (const auto *create = ast->as<DB::ASTCreateQuery>()) {
    // A materialized view's own engine is its TO target's inner engine.
    std::vector<const DB::ASTStorage *> storages{create->storage};
    if (create->targets)
      for (const auto *inner : create->targets->getInnerEngines()) storages.push_back(inner);
    for (const auto *storage : storages) {
      if (!storage || !storage->engine) continue;
      if (auto ref = decodeEngine(*storage->engine)) {
        StorageIntegrityReadRef event;
        event.kind = StorageIntegrityReadRef::Kind::Namespace;
        event.namespace_ref = std::move(*ref);
        out.push_back(std::move(event));
      }
    }
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefAllowlist.EnginesAndTableSettings'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 1076 tests`); corpus failures 283 → 251 (32 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/table_reference.h src/handlers/table_reference.cc src/handlers/storage_integrity.cc
git commit -m "feat(table-ref): table-engine allowlist and refused table settings (T5)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 13: T6 string lookups: joinGet / dictGet refused statement-wide, hasColumnInTable rewritten

Every joinGet / joinGetOrNull / dictGet-family call is refused statement-wide (no ClickHouse spelling addresses a dotted-name tenant object, S7a). hasColumnInTable is rewritten to the physical pair in a rewritten SELECT body and refused elsewhere. `INSERT … VALUES` rows are raw text to ClickHouse's parser; they are parsed as an expression list (`ParserExpressionList`) so a lookup call in a VALUES row is refused too, as rewriter-go's parser sees it. An opaque VALUES text (header SETTINGS) is left to R2.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/table_reference.cc`
- Modify: `src/handlers/table_reference.h`
- Modify: `src/handlers/select.cc`

**Corpus cases:** 13 M-T6.

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 T6 as merged in rewriter-go: joinGet / joinGetOrNull and
// the dictGet family are refused statement-wide (no ClickHouse spelling
// addresses a dotted-name tenant object); hasColumnInTable is rewritten in a
// rewritten SELECT body and refused elsewhere.
TEST(TableRefLookup, ResolvesOrRefuses) {
  struct Case { const char *sql; bool si; std::string message; };
  const std::vector<Case> refused = {
    {"SELECT joinGet('db1.j', 'v', 1)", false, "joinGet target \"db1.j\""},
    {"SELECT joinGet(db1.j, 'v', 1)", false, "joinGet target \"db1.j\""},
    {"SELECT joinGet(concat('db1', '.j'), 'v', 1)", false, "joinGet target \"\""},
    {"SELECT dictGet('db1.d', 'v', 1)", false, "dictGet target \"db1.d\""},
    {"SELECT joinGet('db1.t', 'v', 1)", true, "joinGet target \"db1.t\""},
    {"SELECT dictGet(currentDatabase() || '.d', 'v', 1)", true, "dictGet target \"\""},
    {"UPDATE db1.o SET a = hasColumnInTable('db1', 'j', 'v') WHERE 1", false, "hasColumnInTable target \"db1.j\""},
    {"ALTER TABLE db1.o UPDATE a = `joinGet`('default.j', 'v', 1) WHERE 1", false, "joinGet target \"default.j\""},
    {"INSERT INTO db1.o VALUES (joinGet('db1.j', 'v', 1))", false, "joinGet target \"db1.j\""},
    {"INSERT INTO db1.o VALUES (1), (dictGet('db1.d', 'v', 1))", true, "dictGet target \"db1.d\""},
  };
  for (const auto &c : refused) {
    SCOPED_TRACE(c.sql);
    const auto resp = RunTableRef(c.sql, c.si);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest) << resp.message();
    EXPECT_EQ(resp.message(), c.message + " does not resolve through the caller's databases");
    EXPECT_EQ(resp.sql_after_rewrite(), c.sql);
  }
  const auto rewritten = RunTableRef("SELECT hasColumnInTable('db1', 'j', 'v')");
  EXPECT_EQ(rewritten.code(), rewriter::RewriteCode::Success) << rewritten.message();
  EXPECT_EQ(rewritten.sql_after_rewrite(), "SELECT hasColumnInTable('phys', 'db1.j', 'v')");
  ASSERT_EQ(rewritten.original_accessed_tables_size(), 1);
  EXPECT_EQ(rewritten.original_accessed_tables(0).original_table(), "j");
  EXPECT_EQ(rewritten.table_rewrites().at("db1.j"), "phys.db1.j");
  const auto once = RunTableRef(
    "SELECT hasColumnInTable('db1', 'o', 'a'), hasColumnInTable('db1', 'o', 'b') FROM db1.o");
  EXPECT_EQ(once.code(), rewriter::RewriteCode::Success) << once.message();
  EXPECT_EQ(once.original_accessed_tables_size(), 1);
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefLookup.*'
```

Expected: `TableRefLookup.ResolvesOrRefuses` fails (lookups pass; hasColumnInTable is not rewritten). Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `TableRefLookup.ResolvesOrRefuses`.

- [ ] **Step 3: Implement.**

In `src/handlers/table_reference.cc`, replace:

```cpp
#include <Parsers/Lexer.h>
```

with:

```cpp
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Lexer.h>
#include <Parsers/parseQuery.h>
```

In `src/handlers/table_reference.h`, replace:

```cpp
// Dynamic-mode table-reference preflight, run right after parsing, in the
```

with:

```cpp
// T6 (rewriter-go handlers/lookups.go): every joinGet / joinGetOrNull /
// dictGet-family call, and every hasColumnInTable call outside a rewritten
// SELECT body, is refused with `<fn> target "<arg>" does not resolve through
// the caller's databases`; <fn> as written, <arg> "" when not a literal.
std::string stringLookupUnresolvedMessage(const std::string &function, const std::string &arg);
bool rejectStringLookups(const DB::ASTPtr &ast, rewriter::RewriteSQLResponse *response);

// Dynamic-mode table-reference preflight, run right after parsing, in the
```

In `src/handlers/table_reference.cc`, replace:

```cpp
bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response) {
```

with:

```cpp
std::string stringLookupUnresolvedMessage(const std::string &function, const std::string &arg) {
  return function + " target \"" + arg + "\" does not resolve through the caller's databases";
}

namespace {

bool isSelectFamily(const DB::IAST &node) {
  return node.as<DB::ASTSelectWithUnionQuery>() || node.as<DB::ASTSelectQuery>()
    || node.as<DB::ASTSelectIntersectExceptQuery>();
}

bool isHasColumnInTable(const std::string &name) {
  return lowerName(name).rfind("hascolumnintable", 0) == 0;
}

// The table a lookup call names, as rewriter-go's StringLookup reports it:
// the first argument's literal or identifier text; hasColumnInTable's
// "<db>.<table>" pair; "" when the argument is not a literal.
std::string lookupTarget(const DB::ASTFunction &fn) {
  const auto &args = fn.arguments ? fn.arguments->children : DB::ASTs{};
  if (isHasColumnInTable(fn.name)) {
    if (args.size() < 3) return {};
    const auto db = stringLiteral(args[args.size() - 3]);
    const auto table = stringLiteral(args[args.size() - 2]);
    return db && table ? *db + "." + *table : std::string();
  }
  if (args.empty()) return {};
  if (auto literal = stringLiteral(args[0])) return *literal;
  if (const auto *id = args[0]->as<DB::ASTIdentifier>()) return id->name();
  return {};
}

// Every lookup call with whether it sits in a SELECT body the rewrite
// pipeline processes (a SELECT root, an INSERT … SELECT / CTAS / view body).
void collectLookupCalls(const DB::IAST &node, bool in_select_body,
                        std::vector<std::pair<const DB::ASTFunction *, bool>> &out) {
  if (const auto *fn = node.as<DB::ASTFunction>(); fn && isStringLookupName(fn->name))
    out.emplace_back(fn, in_select_body);
  const auto *insert = node.as<DB::ASTInsertQuery>();
  const auto *create = node.as<DB::ASTCreateQuery>();
  const auto *alter = node.as<DB::ASTAlterQuery>();
  const bool mutation = node.as<DB::ASTUpdateQuery>() || node.as<DB::ASTDeleteQuery>();
  forEachChild(node, [&](const DB::ASTPtr &child) {
    if (!child) return;
    bool body = in_select_body && !mutation;
    if (insert && child == insert->select) body = true;
    if (create && child.get() == create->select) body = true;
    if (alter && child.get() == alter->command_list) body = false;
    collectLookupCalls(*child, body, out);
  });
}

// rewriter-go measured: an INSERT whose column list is followed by a SETTINGS
// clause keeps the rest of the statement as opaque text (spec 2026-09-26 R2 /
// R5); only the lexical R2 rule reads its VALUES rows.
bool valuesAreOpaque(const DB::ASTInsertQuery &insert) {
  return insert.columns && insert.settings_ast;
}

// INSERT … VALUES rows, which ClickHouse's parser keeps as raw text, read as
// the expression list rewriter-go's parser builds for them. nullptr when the
// statement has no Values rows or they do not parse as expressions (the R2
// lexical rule then reads them, failing closed).
DB::ASTPtr parseValuesRows(const DB::ASTInsertQuery &insert) {
  if (!insert.data || insert.end <= insert.data || insert.format != "Values") return nullptr;
  try {
    DB::ParserExpressionList parser(/*allow_alias_without_as_keyword=*/false);
    return DB::parseQuery(parser, insert.data, insert.end, "", 0, 0, 0);
  } catch (...) {
    return nullptr;
  }
}

} // namespace

bool rejectStringLookups(const DB::ASTPtr &ast, rewriter::RewriteSQLResponse *response) {
  std::vector<std::pair<const DB::ASTFunction *, bool>> calls;
  collectLookupCalls(*ast, isSelectFamily(*ast), calls);
  // A lookup call in a VALUES row is a T6 refusal too (rewriter-go parses the
  // rows); an opaque VALUES text is left to the R2 lexical rule.
  DB::ASTPtr rows;
  if (const auto *insert = ast->as<DB::ASTInsertQuery>(); insert && !valuesAreOpaque(*insert)) {
    rows = parseValuesRows(*insert);
    if (rows) collectLookupCalls(*rows, /*in_select_body=*/false, calls);
  }
  for (const auto &[fn, in_select_body] : calls) {
    if (isHasColumnInTable(fn->name) && in_select_body) continue;  // the SELECT path rewrites it
    response->set_code(rewriter::RewriteCode::InvalidRewriteRequest);
    response->set_message(stringLookupUnresolvedMessage(fn->name, lookupTarget(*fn)));
    return true;
  }
  return false;
}

bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response) {
```

In `src/handlers/table_reference.cc`, replace:

```cpp
  if (!active && rejectDisallowedCarriers(ast, response)) return true;
  return false;
}
```

with:

```cpp
  if (!active && rejectDisallowedCarriers(ast, response)) return true;
  // T6 in both SI states: no SI handler examines a lookup call.
  if (rejectStringLookups(ast, response)) return true;
  return false;
}
```

In `src/handlers/select.cc`, replace:

```cpp
void doTableNameRewrite(
```

with:

```cpp
// Spec 2026-09-26 T6: a hasColumnInTable call in a rewritten SELECT body
// whose (database, table) literals resolve to an ordinary mapped table is
// rewritten to the physical pair and reported once; anything else is refused
// (rewriter-go rewriteStringLookups). Returns false after the refusal.
bool rewriteHasColumnInTable(const DB::ASTPtr &node,
                             const TableRewriteSelection &selection,
                             std::map<std::string, AccessedTablePair> &originals,
                             rewriter::RewriteSQLResponse *response) {
  if (!node || selection.mode != TableRewriteMode::Dynamic) return true;
  const auto &args = *selection.dynamic_args;
  auto *fn = node->as<DB::ASTFunction>();
  std::string lower = fn ? fn->name : std::string();
  std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  if (fn && lower.rfind("hascolumnintable", 0) == 0 && fn->arguments
      && fn->arguments->children.size() >= 3) {
    auto &children = fn->arguments->children;
    auto &db_arg = children[children.size() - 3];
    auto &table_arg = children[children.size() - 2];
    const auto *db_literal = db_arg->as<DB::ASTLiteral>();
    const auto *table_literal = table_arg->as<DB::ASTLiteral>();
    const bool literal = db_literal && table_literal
      && db_literal->value.getType() == DB::Field::Types::String
      && table_literal->value.getType() == DB::Field::Types::String;
    const std::string db = literal ? db_literal->value.safeGet<String>() : "";
    const std::string table = literal ? table_literal->value.safeGet<String>() : "";
    const auto outcome = literal && !table.empty() && !lookupStorageIntegrity(db, table, args)
      ? applyDynamicRewrite(db, table, args, "hasColumnInTable") : DynamicRewriteOutcome{};
    if (outcome.status != DynamicRewriteStatus::OK) {
      response->set_code(rewriter::RewriteCode::InvalidRewriteRequest);
      response->set_message(stringLookupUnresolvedMessage(fn->name, literal ? db + "." + table : ""));
      return false;
    }
    db_arg = DB::makeRewriterAST<DB::ASTLiteral>(outcome.physical_db);
    table_arg = DB::makeRewriterAST<DB::ASTLiteral>(outcome.new_table);
    const std::string key = db.empty() ? table : db + "." + table;
    originals.emplace(key, AccessedTablePair{db, table});
    (*response->mutable_table_rewrites())[key] = outcome.physical_db + "." + outcome.new_table;
    return true;
  }
  for (const auto &child : node->children)
    if (!rewriteHasColumnInTable(child, selection, originals, response)) return false;
  return true;
}

void doTableNameRewrite(
```

In `src/handlers/select.cc`, replace:

```cpp
  // An unqualified IN operand read through an SI physical execution context
  // stays with the SI namespace policy, which names the physical table.
```

with:

```cpp
  if (!rewriteHasColumnInTable(ast, selection, originals, response)) return;

  // An unqualified IN operand read through an SI physical execution context
  // stays with the SI namespace policy, which names the physical table.
```

In `src/handlers/select.cc`, replace:

```cpp
  const auto selection = findActiveTableRewrite(options);

  populateAccessedTables(originals, selection, response);
```

with:

```cpp
  const auto selection = findActiveTableRewrite(options);

  if (!rewriteHasColumnInTable(body, selection, originals, response)) return;
  populateAccessedTables(originals, selection, response);
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefLookup.*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 1090 tests`); corpus failures 251 → 238 (13 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/table_reference.cc src/handlers/table_reference.h src/handlers/select.cc
git commit -m "feat(table-ref): refuse string lookups and rewrite hasColumnInTable (T6)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 14: R5 SQL-bearing and dialect settings, and the setting-value rule

In a query-level SETTINGS clause and in the admitted session SET, a SQL-bearing setting (`additional_table_filters`, `additional_result_filter`, `parallel_replicas_custom_key`) or a dialect setting (`dialect`, `*_dialect`, any case) is refused with the table-setting text; a value that is not a numeric, string or boolean literal is `statement is not supported`.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/table_reference.h`
- Modify: `src/handlers/table_reference.cc`
- Modify: `src/handlers/select.cc`
- Modify: `src/handlers/writes.cc`

**Corpus cases:** 40: 37 M-R5set + 3 new (`si_tr_settings_map_value_rejected`, `…_v2_rejected`, `si_tr_set_map_value_rejected`).

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 §5 "settings values" (rewriter-go R5): a SQL-bearing or
// dialect setting is a T5 table-setting refusal, named as written; a value
// that is not a numeric, string or boolean literal is `statement is not
// supported`. The session-SET carve-out applies the same rule.
TEST(TableRefSettings, SqlBearingDialectAndValueRules) {
  struct Case { const char *sql; std::string message; };
  const std::vector<Case> refused = {
    {"SELECT * FROM db1.o SETTINGS additional_table_filters = {'db1.o': 'a > 1'}",
     "table setting additional_table_filters is not accepted"},
    {"SELECT * FROM db1.o SETTINGS additional_result_filter = 'a = 1'",
     "table setting additional_result_filter is not accepted"},
    {"SELECT * FROM db1.o SETTINGS parallel_replicas_custom_key = 'a'",
     "table setting parallel_replicas_custom_key is not accepted"},
    {"SELECT * FROM db1.o SETTINGS dialect = 1", "table setting dialect is not accepted"},
    {"SELECT * FROM db1.o SETTINGS allow_experimental_kusto_dialect = 1",
     "table setting allow_experimental_kusto_dialect is not accepted"},
    {"INSERT INTO db1.o SELECT * FROM db1.p SETTINGS additional_table_filters = {'db1.p': 'a > 1'}",
     "table setting additional_table_filters is not accepted"},
    {"INSERT INTO db1.o (a) SETTINGS `POLYGLOT_DIALECT` = 'sqlite' SELECT 1",
     "table setting POLYGLOT_DIALECT is not accepted"},
    {"SELECT * FROM db1.o SETTINGS max_threads = {'a': 'b'}", std::string(rewriter_handlers::kUnsupportedStatementMessage)},
  };
  for (const bool si : {false, true}) {
    for (const auto &c : refused) {
      SCOPED_TRACE(std::string(c.sql) + (si ? " [V2]" : " [inactive]"));
      const auto resp = RunTableRef(c.sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
      EXPECT_EQ(resp.message(), c.message);
    }
  }
  for (const auto &[sql, message] : std::vector<std::pair<std::string, std::string>>{
         {"SET dialect = 1", "table setting dialect is not accepted"},
         {"SET polyglot_dialect = 'sqlite'", "table setting polyglot_dialect is not accepted"},
         {"SET max_threads = {'a': 'b'}", std::string(rewriter_handlers::kUnsupportedStatementMessage)}}) {
    SCOPED_TRACE(sql);
    const auto resp = RunTableRef(sql);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.message(), message);
  }
  EXPECT_EQ(RunTableRef("SELECT * FROM db1.o SETTINGS max_threads = 1, join_algorithm = 'hash', use_query_cache = true").code(),
            rewriter::RewriteCode::Success);
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefSettings.*'
```

Expected: `TableRefSettings.SqlBearingDialectAndValueRules` fails (the settings pass). Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `TableRefSettings.SqlBearingDialectAndValueRules`.

- [ ] **Step 3: Implement.**

In `src/handlers/table_reference.h`, replace:

```cpp
// Dynamic-mode table-reference preflight, run right after parsing, in the
```

with:

```cpp
// R5 (rewriter-go internal/engine/settings.go): a SQL-bearing or dialect
// setting in a query-level SETTINGS clause or a session SET is a T5
// table-setting refusal; a value that is not a numeric, string or boolean
// literal is `statement is not supported`.
bool sqlBearingSetting(const std::string &name);
bool rejectQuerySettings(const DB::ASTPtr &ast, rewriter::RewriteSQLResponse *response);

// Dynamic-mode table-reference preflight, run right after parsing, in the
```

In `src/handlers/table_reference.cc`, replace:

```cpp
bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response) {
```

with:

```cpp
bool sqlBearingSetting(const std::string &name) {
  const std::string lower = lowerName(name);
  return lower == "additional_table_filters" || lower == "additional_result_filter"
    || lower == "parallel_replicas_custom_key" || lower == "dialect" || lower.ends_with("_dialect");
}

namespace {

// The only values a setting may carry in dynamic mode: a numeric, string or
// boolean literal (NULL included); a map, array, tuple or object is not.
bool plainSettingValue(const DB::Field &value) {
  switch (value.getType()) {
  case DB::Field::Types::Null:
  case DB::Field::Types::UInt64:
  case DB::Field::Types::Int64:
  case DB::Field::Types::Float64:
  case DB::Field::Types::UInt128:
  case DB::Field::Types::Int128:
  case DB::Field::Types::UInt256:
  case DB::Field::Types::Int256:
  case DB::Field::Types::String:
  case DB::Field::Types::Bool:
    return true;
  default:
    return false;
  }
}

// Query-level SETTINGS clauses and a session SET: every ASTSetQuery except a
// table's own storage settings and ALTER … MODIFY SETTING (T5 table settings).
void collectQuerySettings(const DB::IAST &node, std::vector<const DB::ASTSetQuery *> &out) {
  if (node.as<DB::ASTStorage>()) return;
  if (const auto *command = node.as<DB::ASTAlterCommand>();
      command && command->type == DB::ASTAlterCommand::MODIFY_SETTING) return;
  if (const auto *set = node.as<DB::ASTSetQuery>()) out.push_back(set);
  forEachChild(node, [&](const DB::ASTPtr &child) {
    if (child) collectQuerySettings(*child, out);
  });
}

} // namespace

bool rejectQuerySettings(const DB::ASTPtr &ast, rewriter::RewriteSQLResponse *response) {
  std::vector<const DB::ASTSetQuery *> clauses;
  collectQuerySettings(*ast, clauses);
  for (const auto *clause : clauses) {
    for (const auto &change : clause->changes) {
      if (sqlBearingSetting(change.name)) {
        response->set_code(rewriter::RewriteCode::UnsupportedStatement);
        response->set_message(tableSettingRefusedMessage(change.name));
        return true;
      }
      if (!plainSettingValue(change.value)) {
        response->set_code(rewriter::RewriteCode::UnsupportedStatement);
        response->set_message(std::string(kUnsupportedStatementMessage));
        return true;
      }
    }
  }
  return false;
}

bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response) {
```

In `src/handlers/table_reference.cc`, replace:

```cpp
  // T6 in both SI states: no SI handler examines a lookup call.
  if (rejectStringLookups(ast, response)) return true;
  return false;
}
```

with:

```cpp
  // T6 in both SI states: no SI handler examines a lookup call.
  if (rejectStringLookups(ast, response)) return true;
  // R5 while the surface is inactive; the SI handlers run it after their own
  // checks while it is active.
  if (!active && rejectQuerySettings(ast, response)) return true;
  return false;
}
```

In `src/handlers/select.cc`, replace:

```cpp
      && findActiveTableRewrite(request->options()).mode == TableRewriteMode::Dynamic) {
    setSuccessResponse(response, formatAst(ast), rewriter::STATEMENT_TYPE_UNSPECIFIED);
    return;
  }
```

with:

```cpp
      && findActiveTableRewrite(request->options()).mode == TableRewriteMode::Dynamic) {
    if (rejectQuerySettings(ast, response)) return;
    setSuccessResponse(response, formatAst(ast), rewriter::STATEMENT_TYPE_UNSPECIFIED);
    return;
  }
```

In `src/handlers/select.cc`, replace:

```cpp
    // T5 while the SI surface is active runs after the SI namespace policy.
    if (storageIntegritySurfaceActive(args) && rejectDisallowedCarriers(ast, response)) return;
```

with:

```cpp
    // T5 and R5 while the SI surface is active run after the SI namespace
    // policy.
    if (storageIntegritySurfaceActive(args)
        && (rejectDisallowedCarriers(ast, response) || rejectQuerySettings(ast, response))) return;
```

In `src/handlers/writes.cc`, replace:

```cpp
          || ast->as<DB::ASTUpdateQuery>() || ast->as<DB::ASTDeleteQuery>())
      && rejectDisallowedCarriers(ast, response))
    return WriteDispatchResult::Rejected;
```

with:

```cpp
          || ast->as<DB::ASTUpdateQuery>() || ast->as<DB::ASTDeleteQuery>())
      && (rejectDisallowedCarriers(ast, response) || rejectQuerySettings(ast, response)))
    return WriteDispatchResult::Rejected;
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefSettings.*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 1131 tests`); corpus failures 238 → 198 (40 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/table_reference.h src/handlers/table_reference.cc src/handlers/select.cc src/handlers/writes.cc
git commit -m "feat(table-ref): refuse SQL-bearing and dialect settings and non-literal values (R5)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 15: The SI reject annotator becomes structural (rewriter-go parity, preflight S11)

`annotateStorageIntegrityReject` upgraded a rejection's message whenever the SQL *lexically* named `hg_safe` or an Active table. It now names only a proven target: a statement target, or a table / namespace read found by the CTE-aware read walker (rewriter-go `AnnotateStorageIntegrityRejectAST`). An alias-qualified column `hg_safe.x`, a column alias, a CTE named like an Active table, an `ON CLUSTER hg_safe` or a setting named `hg_safe` keeps the caller's message. No corpus case moves (the corpus pins messages by substring); the parity test pins the behaviour.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/storage_integrity.cc`

**Corpus cases:** 0 corpus cases (parity only).

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// rewriter-go parity (TestAnnotateStorageIntegrityReject_*): only a proven
// statement target or a classified table / namespace read upgrades a
// rejection's message; a lexical lookalike keeps the caller's message.
TEST(TableRefAnnotation, OnlyProvenNamesUpgradeTheMessage) {
  rewriter::RewriteSQLRequest options;
  AddTableRefOption(options, /*si=*/true);
  const auto &args = options.options(0).table_name_args().dynamic_args();
  auto annotate = [&](const std::string &sql) {
    const char *begin = sql.data();
    const char *end = begin + sql.size();
    DB::ParserQuery parser(end, false);
    const auto ast = DB::parseQuery(parser, begin, end, "", 0, 0, 0);
    rewriter::RewriteSQLResponse resp;
    resp.set_code(rewriter::RewriteCode::UnsupportedStatement);
    resp.set_message("original");
    rewriter_handlers::annotateStorageIntegrityReject(&resp, ast, sql, args);
    return resp.message();
  };
  for (const char *sql : {
         "SELECT hg_safe.x FROM other.u AS hg_safe",
         "SELECT x FROM other.u AS hg_safe",
         "SELECT x AS hg_safe FROM other.u",
         "UPDATE other.u SET x = (WITH RECURSIVE t AS (SELECT count() FROM t) SELECT count() FROM t) WHERE id = 1",
         "SYSTEM RELOAD CONFIG ON CLUSTER hg_safe",
         "OPTIMIZE TABLE other.u FINAL SETTINGS hg_safe = 1",
       }) {
    SCOPED_TRACE(sql);
    EXPECT_EQ(annotate(sql), "original");
  }
  EXPECT_EQ(annotate("DESCRIBE (SELECT * FROM hg_safe.db1__t)"),
            "storage-integrity physical table hg_safe.db1__t is not directly addressable");
  EXPECT_EQ(annotate("CHECK TABLE db1.t"),
            "storage-integrity table db1.t accepts writes only through the signed statement lane");
  EXPECT_EQ(annotate("DETACH DATABASE hg_unsafe"),
            "storage-integrity physical database hg_unsafe is not directly addressable");
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefAnnotation.*:StorageIntegrity*'
```

Expected: `TableRefAnnotation.OnlyProvenNamesUpgradeTheMessage` fails on the six lookalike rows. Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `TableRefAnnotation.OnlyProvenNamesUpgradeTheMessage`.

- [ ] **Step 3: Implement.**

In `src/handlers/storage_integrity.cc`, replace:

```cpp
#include <Parsers/ASTDictionary.h>
```

with:

```cpp
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTViewTargets.h>
```

In `src/handlers/storage_integrity.cc`, replace:

```cpp
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

with:

```cpp
  // Spec 2026-09-26 parity with rewriter-go's AnnotateStorageIntegrityRejectAST:
  // only a proven name — a statement target or a table / namespace read the
  // CTE-aware read walker classifies — upgrades the message. A lexical
  // lookalike (an alias-qualified column `hg_safe.x`, a CTE named like an
  // Active table) keeps the caller's message.
  (void)sql;
  std::vector<std::pair<std::string, std::string>> targets;
  // A parameter target (table AST present, name empty) proves nothing.
  if (const auto *q = dynamic_cast<const DB::ASTQueryWithTableAndOutput *>(ast.get());
      q && (!q->table || !q->getTable().empty()))
    targets.emplace_back(q->getDatabase(), q->getTable());
  if (const auto *insert = ast->as<DB::ASTInsertQuery>())
    targets.emplace_back(insert->getDatabase(), insert->getTable());
  if (const auto *rename = ast->as<DB::ASTRenameQuery>()) {
    for (const auto &e : rename->getElements()) {
      targets.emplace_back(e.from.getDatabase(), rename->database ? "" : e.from.getTable());
      targets.emplace_back(e.to.getDatabase(), rename->database ? "" : e.to.getTable());
    }
  }
  if (const auto *drop = ast->as<DB::ASTDropQuery>(); drop && drop->database_and_tables) {
    for (const auto &single : drop->getRewrittenASTsOfSingleTable(ast)) {
      if (const auto *one = single->as<DB::ASTDropQuery>())
        targets.emplace_back(one->getDatabase(), one->getTable());
    }
  }
  if (const auto *create = ast->as<DB::ASTCreateQuery>()) {
    if (!create->as_table.empty()) targets.emplace_back(create->as_database, create->as_table);
    if (create->targets && create->hasTargetTableID(DB::ViewTarget::To)) {
      const auto to = create->getTargetTableID(DB::ViewTarget::To);
      targets.emplace_back(to.database_name, to.table_name);
    }
  }
  for (const auto &event : collectStorageIntegrityReadRefs(ast)) {
    if (event.kind == StorageIntegrityReadRef::Kind::Table)
      targets.emplace_back(event.database, event.table);
    else if (event.namespace_ref.resolved)
      targets.emplace_back(event.namespace_ref.database, event.namespace_ref.table);
  }
  for (const auto &[database, table] : targets) {
    if (database.empty() && table.empty()) continue;
    if (annotateStructuredStorageIntegrityTarget(response, database, table, args)) return;
  }
}
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefAnnotation.*:StorageIntegrity*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 1132 tests`); corpus failures 198 → 198 (0 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/storage_integrity.cc
git commit -m "refactor(storage-integrity): annotate a rejection only from proven names

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 16: R2 ungoverned reads in ALTER actions (opaque-text rule)

An ALTER action is opaque text to rewriter-go. A subquery, a table, a cross-table partition action (`FETCH`, `ATTACH|REPLACE PARTITION … FROM t`, `MOVE PARTITION … TO TABLE`), `MODIFY QUERY`, or an IN-family occurrence whose operand region is not literal-only is refused with `statement is not supported`: the infix region is the right operand, the callable region is the whole argument list. The preflight runs R2 while the surface is inactive; the SI write path runs it after the SI write preflight while active, so an SI object in an ALTER expression keeps its SI message.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/table_reference.h`
- Modify: `src/handlers/table_reference.cc`
- Modify: `src/handlers/writes.cc`

**Corpus cases:** 122: 116 M-R2 + 6 G-replaced ADD PROJECTION inputs.

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, replace:

```cpp
  const auto ordinary_resp = RunDirectRewrite(std::move(ordinary));
  EXPECT_EQ(ordinary_resp.code(), rewriter::RewriteCode::Success)
    << ordinary_resp.message();
  EXPECT_EQ(ordinary_resp.statement_type(), rewriter::STATEMENT_TYPE_ALTER_TABLE);
  EXPECT_EQ(ordinary_resp.message(), "success");
```

with:

```cpp
  const auto ordinary_resp = RunDirectRewrite(std::move(ordinary));
  // Spec 2026-09-26 R2: an ordinary subquery in an ALTER action is a read no
  // rewrite reaches, refused after the SI policy found nothing.
  EXPECT_EQ(ordinary_resp.code(), rewriter::RewriteCode::UnsupportedStatement)
    << ordinary_resp.message();
  EXPECT_EQ(ordinary_resp.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
  EXPECT_EQ(ordinary_resp.message(), rewriter_handlers::kUnsupportedStatementMessage);
```

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 R2 for ALTER actions (rewriter-go OpaqueTextIsUngoverned):
// a subquery, a table, a cross-table partition action, MODIFY QUERY, or an
// IN-family occurrence whose operand region is not literal-only is
// `statement is not supported`. The infix region is the right operand; the
// callable region is the whole argument list.
TEST(TableRefUngoverned, AlterActions) {
  for (const bool si : {false, true}) {
    for (const char *sql : {
           "ALTER TABLE db1.o DELETE WHERE a IN `db2.x`",
           "ALTER TABLE db1.o DELETE WHERE a IN +`db2.x`",
           "ALTER TABLE db1.o UPDATE b = (SELECT max(a) FROM `db2.x`) WHERE 1",
           "ALTER TABLE db1.o UPDATE b = (SELECT count() FROM remote('127.0.0.1', 'phys', 'db2.x')) WHERE 1",
           "ALTER TABLE db1.mv MODIFY QUERY SELECT * FROM `db2.x`",
           "ALTER TABLE db1.o FETCH PARTITION tuple() FROM '/clickhouse/tables/x'",
           "ALTER TABLE db1.o ATTACH PARTITION tuple() FROM db1.p",
           "ALTER TABLE db1.o MOVE PARTITION tuple() TO TABLE db1.p",
           "ALTER TABLE db1.o MODIFY TTL d + INTERVAL 1 DAY DELETE WHERE a IN `db2.x`",
           "ALTER TABLE db1.o ADD COLUMN c UInt8 DEFAULT a IN `db2.x`",
           "ALTER TABLE db1.o DELETE WHERE in(a, (1, 2))",
           "ALTER TABLE db1.o DELETE WHERE a IN (1, b)",
           "ALTER TABLE db1.o DELETE WHERE `in`((a, 0), `db2.x`)",
           "ALTER TABLE db1.o DELETE WHERE CASE WHEN a THEN 1 END IN (`db2.x`)",
           "ALTER TABLE db1.o ADD PROJECTION p (SELECT a ORDER BY date IN (`db2.x`))",
         }) {
      SCOPED_TRACE(std::string(sql) + (si ? " [V2]" : " [inactive]"));
      const auto resp = RunTableRef(sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
      EXPECT_EQ(resp.message(), rewriter_handlers::kUnsupportedStatementMessage);
      EXPECT_EQ(resp.sql_after_rewrite(), sql);
    }
    for (const char *sql : {
           "ALTER TABLE db1.o DELETE WHERE a IN (1, 2)", "ALTER TABLE db1.o DELETE WHERE in(42, (1, 2))",
           "ALTER TABLE db1.o DELETE WHERE in(42, [1, 2])", "ALTER TABLE db1.o DELETE WHERE a IN -1",
           "ALTER TABLE db1.o DELETE WHERE a IN (+1, -2)", "ALTER TABLE db1.o DELETE WHERE a NOT IN ('x', 'y')",
           "ALTER TABLE db1.o MODIFY TTL d + INTERVAL 1 DAY DELETE WHERE a IN [1, 2]",
           "ALTER TABLE db1.o ADD PROJECTION p (SELECT a ORDER BY b)",
           "ALTER TABLE db1.o UPDATE b = 1 WHERE a IN (1, 2)",
         }) {
      SCOPED_TRACE(std::string(sql) + (si ? " [V2]" : " [inactive]"));
      const auto resp = RunTableRef(sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
      EXPECT_EQ(resp.statement_type(), rewriter::STATEMENT_TYPE_ALTER_TABLE);
    }
  }
  // While the SI surface is active an SI object named in an ALTER action
  // keeps the SI message: the SI write preflight runs before R2.
  const auto physical = RunTableRef("ALTER TABLE db1.o DELETE WHERE a IN hg_safe.`db2.x`", /*si=*/true);
  EXPECT_EQ(physical.code(), rewriter::RewriteCode::UnsupportedStatement);
  EXPECT_EQ(physical.message(), "storage-integrity physical table hg_safe.db2.x is not directly addressable");
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefUngoverned.*:StorageIntegrityAlterExpressions*'
```

Expected: `TableRefUngoverned.AlterActions` fails on every refused row, and the flipped `StorageIntegrityAlterExpressions` expectation fails (`Success`). Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `StorageIntegrityAlterExpressions.RejectsEmbeddedLogicalAndPhysicalReads`, `TableRefUngoverned.AlterActions`.

- [ ] **Step 3: Implement.**

In `src/handlers/table_reference.h`, replace:

```cpp
// Dynamic-mode table-reference preflight, run right after parsing, in the
```

with:

```cpp
// R2 (rewriter-go rejectUngovernedReads): a read in a position no rewrite
// reaches — an ALTER action, a structured UPDATE / DELETE, a column /
// constraint / storage expression, INSERT … VALUES, an INSERT whose header
// carries SETTINGS, REFRESH views, FROM INFILE — is `statement is not
// supported`. The preflight runs it while the SI surface is inactive; the SI
// write path runs it after the SI write preflight while it is active.
bool rejectUngovernedReads(const DB::ASTPtr &ast, rewriter::RewriteSQLResponse *response);

// Dynamic-mode table-reference preflight, run right after parsing, in the
```

In `src/handlers/table_reference.cc`, replace:

```cpp
bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response) {
```

with:

```cpp
namespace {

// A literal, or a tuple / array constructor of literals: the only IN operand
// region a governed position may carry (rewriter-go OpaqueInRefusedAt:
// literals, signs, commas and brackets only).
bool literalOnly(const DB::IAST &node) {
  if (node.as<DB::ASTLiteral>()) return true;
  if (const auto *fn = node.as<DB::ASTFunction>()) {
    const std::string lower = lowerName(fn->name);
    if (lower != "tuple" && lower != "array" && lower != "negate") return false;
    if (!fn->arguments) return true;
    for (const auto &arg : fn->arguments->children)
      if (!literalOnly(*arg)) return false;
    return true;
  }
  return false;
}

// An IN-family occurrence whose operand region is not literal-only. The
// infix region is its right operand; the callable region is its whole
// argument list (`in(42, (1, 2))` passes, `in(a, (1, 2))` does not).
bool inRegionRefused(const DB::ASTFunction &fn) {
  if (!isInFamilyFunction(fn.name) || !fn.arguments) return false;
  const auto &args = fn.arguments->children;
  if (fn.isOperator() && args.size() == 2) return !literalOnly(*args[1]);
  for (const auto &arg : args)
    if (!literalOnly(*arg)) return true;
  return false;
}

bool expressionHasUngovernedRead(const DB::IAST &node) {
  if (node.as<DB::ASTSubquery>() || isSelectFamily(node) || node.as<DB::ASTTableIdentifier>()
      || node.as<DB::ASTTableExpression>())
    return true;
  if (const auto *fn = node.as<DB::ASTFunction>(); fn && inRegionRefused(*fn)) return true;
  bool found = false;
  forEachChild(node, [&](const DB::ASTPtr &child) {
    if (!found && child) found = expressionHasUngovernedRead(*child);
  });
  return found;
}

// An ALTER action is opaque text to the reference engine. MODIFY QUERY and
// the cross-table partition actions are always refused; ADD PROJECTION's
// SELECT has no FROM (ASTProjectionSelectQuery), so only its IN regions count.
bool alterCommandUngoverned(const DB::ASTAlterCommand &command) {
  switch (command.type) {
  case DB::ASTAlterCommand::MODIFY_QUERY:
  case DB::ASTAlterCommand::FETCH_PARTITION:
    return true;
  case DB::ASTAlterCommand::ATTACH_PARTITION:
  case DB::ASTAlterCommand::REPLACE_PARTITION:
    if (!command.from_table.empty()) return true;
    break;
  case DB::ASTAlterCommand::MOVE_PARTITION:
    if (command.move_destination_type == DB::DataDestinationType::TABLE) return true;
    break;
  case DB::ASTAlterCommand::MODIFY_SETTING:
    return false;
  default:
    break;
  }
  return expressionHasUngovernedRead(command);
}

} // namespace

bool rejectUngovernedReads(const DB::ASTPtr &ast, rewriter::RewriteSQLResponse *response) {
  bool ungoverned = false;
  if (const auto *alter = ast->as<DB::ASTAlterQuery>(); alter && alter->command_list) {
    for (const auto &raw : alter->command_list->children) {
      const auto *command = raw->as<DB::ASTAlterCommand>();
      if (command && alterCommandUngoverned(*command)) ungoverned = true;
    }
  }
  if (!ungoverned) return false;
  response->set_code(rewriter::RewriteCode::UnsupportedStatement);
  response->set_message(std::string(kUnsupportedStatementMessage));
  return true;
}

bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response) {
```

In `src/handlers/table_reference.cc`, replace:

```cpp
  if (!active && rejectQuerySettings(ast, response)) return true;
  return false;
}
```

with:

```cpp
  if (!active && (rejectQuerySettings(ast, response) || rejectUngovernedReads(ast, response)))
    return true;
  return false;
}
```

In `src/handlers/writes.cc`, replace:

```cpp
      && (rejectDisallowedCarriers(ast, response) || rejectQuerySettings(ast, response)))
    return WriteDispatchResult::Rejected;
```

with:

```cpp
      && (rejectDisallowedCarriers(ast, response) || rejectQuerySettings(ast, response)
          || rejectUngovernedReads(ast, response)))
    return WriteDispatchResult::Rejected;
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefUngoverned.*:StorageIntegrityAlterExpressions*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 1253 tests`); corpus failures 198 → 78 (120 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/table_reference.h src/handlers/table_reference.cc src/handlers/writes.cc
git commit -m "feat(table-ref): refuse ungoverned reads in ALTER actions (R2)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 17: R2 in structured UPDATE / DELETE, column / constraint / storage expressions, REFRESH views

These positions are structured in rewriter-go, so the rule is structural and CTE-aware: a table (not an in-scope CTE name), a table function, an IN operand naming a table or an Identifier parameter is refused; a table-free subquery `(SELECT 1)` and a tuple IN operand `a IN (1, b)` are not reads. A REFRESH view is always refused (it reruns its query outside any session). `StorageIntegrityCTEScope.RecursiveAliasesArePredeclared` keeps its `Success`: the recursive CTE reads no table.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/table_reference.cc`

**Corpus cases:** 12 M-R2.

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, replace:

```cpp
  EXPECT_EQ(ordinary_resp.code(), rewriter::RewriteCode::Success)
    << ordinary_resp.message();
  EXPECT_EQ(ordinary_resp.statement_type(), rewriter::STATEMENT_TYPE_CREATE_TABLE);
  EXPECT_EQ(ordinary_resp.message(), "success");
```

with:

```cpp
  // Spec 2026-09-26 R2: an ordinary subquery in a column default or a
  // constraint is a read no rewrite reaches, refused after the SI policy.
  EXPECT_EQ(ordinary_resp.code(), rewriter::RewriteCode::UnsupportedStatement)
    << ordinary_resp.message();
  EXPECT_EQ(ordinary_resp.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
  EXPECT_EQ(ordinary_resp.message(), rewriter_handlers::kUnsupportedStatementMessage);
```

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 R2 for structured UPDATE / DELETE, column / constraint /
// storage expressions and REFRESH views: a table, a table function or an IN
// operand naming a table there is `statement is not supported` in both SI
// states; a table-free subquery, a CTE name and an IN operand that is a tuple
// of expressions are not reads.
TEST(TableRefUngoverned, MutationsCreateExpressionsAndRefresh) {
  for (const bool si : {false, true}) {
    for (const char *sql : {
           "UPDATE db1.o SET b = 1 WHERE a IN `db2.x`",
           "UPDATE db1.o SET b = (SELECT max(a) FROM db1.p) WHERE 1",
           "UPDATE db1.o SET b = (SELECT count() FROM numbers(3)) WHERE 1",
           "UPDATE db1.o SET b = (WITH u AS (SELECT count() FROM u) SELECT count() FROM u) WHERE 1",
           "DELETE FROM db1.o WHERE a IN (SELECT a FROM `db2.x`)",
           "DELETE FROM db1.o WHERE in(a, `db2.x`)",
           "CREATE TABLE db1.n (a UInt64 DEFAULT (SELECT max(a) FROM db1.p)) ENGINE = Memory",
           "CREATE TABLE db1.n (a UInt64, CONSTRAINT c CHECK a IN (SELECT a FROM db1.p)) ENGINE = Memory",
           "CREATE TABLE db1.n (a UInt64) ENGINE = MergeTree ORDER BY a IN `db2.x`",
           "CREATE TABLE db1.n (d Date, a UInt64) ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY DELETE WHERE a IN `db2.x`",
           "CREATE MATERIALIZED VIEW db1.mv REFRESH EVERY 1 HOUR TO db1.p AS SELECT * FROM db1.o",
           "CREATE MATERIALIZED VIEW db1.`refresh` REFRESH AFTER 1 HOUR TO db1.p AS SELECT * FROM db1.o",
         }) {
      SCOPED_TRACE(std::string(sql) + (si ? " [V2]" : " [inactive]"));
      const auto resp = RunTableRef(sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
      EXPECT_EQ(resp.message(), rewriter_handlers::kUnsupportedStatementMessage);
      EXPECT_EQ(resp.sql_after_rewrite(), sql);
    }
    for (const char *sql : {
           "UPDATE db1.o SET b = 1 WHERE a IN (1, 2)",
           "DELETE FROM db1.o WHERE a NOT IN (-1, 2)",
           "DELETE FROM db1.o WHERE a IN (1, b)",
           "UPDATE db1.o SET b = a IN (1, c) WHERE 1",
           "UPDATE db1.o SET b = (SELECT 1) WHERE 1",
           "UPDATE db1.o SET b = (WITH RECURSIVE t AS (SELECT 1) SELECT count() FROM t) WHERE 1",
           "CREATE TABLE db1.n (a UInt64 DEFAULT 1, CONSTRAINT c CHECK a IN (1, 2)) ENGINE = Memory",
           "CREATE TABLE db1.n (a UInt64) ENGINE = MergeTree ORDER BY a",
         }) {
      SCOPED_TRACE(std::string(sql) + (si ? " [V2]" : " [inactive]"));
      const auto resp = RunTableRef(sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
    }
  }
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefUngoverned.*:StorageIntegrityCreateExpressions*:StorageIntegrityCTEScope*'
```

Expected: `TableRefUngoverned.MutationsCreateExpressionsAndRefresh` fails on every refused row; the flipped ordinary row of `StorageIntegrityCreateExpressions` fails. Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `StorageIntegrityCreateExpressions.RejectsEmbeddedColumnAndConstraintReads`, `TableRefUngoverned.MutationsCreateExpressionsAndRefresh`.

- [ ] **Step 3: Implement.**

In `src/handlers/table_reference.cc`, replace:

```cpp
bool alterCommandUngoverned(const DB::ASTAlterCommand &command) {
```

with:

```cpp
bool structuredShapeHasRead(const DB::IAST &node) {
  if (const auto *te = node.as<DB::ASTTableExpression>(); te && te->table_function) return true;
  if (const auto *id = dynamic_cast<const DB::ASTIdentifier *>(&node); id && id->isParam()) return true;
  bool found = false;
  forEachChild(node, [&](const DB::ASTPtr &child) {
    if (!found && child) found = structuredShapeHasRead(*child);
  });
  return found;
}

// rewriter-go ExpressionPositionHasReads, for the positions its parser
// structures (a structured UPDATE / DELETE, column / constraint / storage
// expressions, INSERT … VALUES rows): a table (an in-scope CTE name is not
// one), a namespace-bearing table function or IN-operand table (the CTE-aware
// SI read walker's events), any other table-function source, or an
// Identifier parameter. A subquery that reads no table — `(SELECT 1)` — and
// an IN operand that is a tuple of expressions — `a IN (1, b)` — are not
// reads, unlike under the opaque ALTER text rule.
bool structuredExpressionHasRead(const DB::IAST *node) {
  if (!node) return false;
  const DB::ASTPtr owned = const_cast<DB::IAST *>(node)->ptr();
  return !collectStorageIntegrityReadRefs(owned).empty() || structuredShapeHasRead(*node);
}

bool storageHasUngovernedRead(const DB::ASTStorage *storage) {
  if (!storage) return false;
  for (const DB::IAST *expr : {storage->partition_by, storage->primary_key, storage->order_by,
         storage->sample_by, storage->ttl_table})
    if (structuredExpressionHasRead(expr)) return true;
  return false;
}

bool alterCommandUngoverned(const DB::ASTAlterCommand &command) {
```

In `src/handlers/table_reference.cc`, replace:

```cpp
      if (command && alterCommandUngoverned(*command)) ungoverned = true;
    }
  }
  if (!ungoverned) return false;
```

with:

```cpp
      if (command && alterCommandUngoverned(*command)) ungoverned = true;
    }
  } else if (const auto *update = ast->as<DB::ASTUpdateQuery>()) {
    ungoverned = structuredExpressionHasRead(update->assignments.get())
      || structuredExpressionHasRead(update->predicate.get());
  } else if (const auto *del = ast->as<DB::ASTDeleteQuery>()) {
    ungoverned = structuredExpressionHasRead(del->predicate.get());
  } else if (const auto *create = ast->as<DB::ASTCreateQuery>()) {
    // A REFRESH view reruns its query on a schedule outside any session.
    ungoverned = create->refresh_strategy != nullptr
      || structuredExpressionHasRead(create->columns_list)
      || storageHasUngovernedRead(create->storage);
    if (create->targets) {
      for (const auto *inner : create->targets->getInnerEngines())
        ungoverned = ungoverned || storageHasUngovernedRead(inner);
    }
  }
  if (!ungoverned) return false;
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefUngoverned.*:StorageIntegrityCreateExpressions*:StorageIntegrityCTEScope*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 1268 tests`); corpus failures 78 → 64 (14 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/table_reference.cc
git commit -m "feat(table-ref): refuse ungoverned reads in mutations, column and storage expressions and REFRESH views (R2)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 18: R2 in INSERT: FROM INFILE, VALUES rows, header-SETTINGS queries

`FROM INFILE` is refused. Plain VALUES rows are parsed as an expression list and take the structural rule; rows that do not parse fall back to a lexical scan that fails closed. An INSERT whose column list is followed by `SETTINGS` keeps its query or rows as opaque text in rewriter-go, so it takes the literal-only rule (lexical for raw rows, AST for a query).

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/table_reference.cc`

**Corpus cases:** 30 M-R2.

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 R2 for INSERT: FROM INFILE, a read in a VALUES row, and a
// read in the rows or query of an INSERT whose column list is followed by a
// SETTINGS clause (rewriter-go keeps that text opaque and applies the
// literal-only IN rule) are `statement is not supported` in both SI states.
TEST(TableRefUngoverned, InsertInfileValuesAndHeaderSettings) {
  for (const bool si : {false, true}) {
    for (const char *sql : {
           "INSERT INTO db1.o FROM INFILE 'x.csv' FORMAT CSV",
           "INSERT INTO db1.o VALUES ((SELECT max(a) FROM db1.p))",
           "INSERT INTO db1.o (a) SETTINGS max_threads = 1 VALUES (in(42, `db2.x`))",
           "INSERT INTO db1.o (a) SETTINGS max_threads = 1 FORMAT Values (not in(1, `db2.x`))",
           "INSERT INTO db1.o (a) SETTINGS max_threads = 1 VALUES (3 IN +`db2.y`)",
           "INSERT INTO db1.o (a) SETTINGS max_threads = 1 SELECT a FROM `db2.x`",
           "INSERT INTO db1.o (a) SETTINGS max_threads = 1 SELECT in(42, `db2.x`)",
           "INSERT INTO db1.o (a) SETTINGS max_threads = 1 SELECT CASE WHEN 1 THEN 42 END IN (`db2.x`)",
           "INSERT INTO db1.o VALUES (in(42, `db2.x`))",
           "INSERT INTO db1.o (a) SETTINGS max_threads = 1 VALUES (1 IN (1, b))",
         }) {
      SCOPED_TRACE(std::string(sql) + (si ? " [V2]" : " [inactive]"));
      const auto resp = RunTableRef(sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
      EXPECT_EQ(resp.message(), rewriter_handlers::kUnsupportedStatementMessage);
    }
    for (const char *sql : {
           "INSERT INTO db1.o VALUES (1, 'x', NULL, -2, [1, 2], (3, 4))",
           "INSERT INTO db1.o VALUES (1 IN (1, 2))",
           "INSERT INTO db1.o VALUES (1 IN (1, b))",
           "INSERT INTO db1.o VALUES ({p:UInt8})",
           "INSERT INTO db1.o (a) SETTINGS max_threads = 1 SELECT 1",
           "INSERT INTO db1.o (a) SELECT a FROM db1.p SETTINGS max_threads = 1",
         }) {
      SCOPED_TRACE(std::string(sql) + (si ? " [V2]" : " [inactive]"));
      const auto resp = RunTableRef(sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
    }
  }
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefUngoverned.InsertInfileValuesAndHeaderSettings'
```

Expected: `TableRefUngoverned.InsertInfileValuesAndHeaderSettings` fails on every refused row. Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `TableRefUngoverned.InsertInfileValuesAndHeaderSettings`.

- [ ] **Step 3: Implement.**

In `src/handlers/table_reference.cc`, replace:

```cpp
} // namespace

bool rejectUngovernedReads(
```

with:

```cpp
// rewriter-go OpaqueInsertQueryIsUngoverned / OpaqueTextIsUngoverned over raw
// INSERT … VALUES text: a SELECT / WITH / FROM / JOIN keyword, an IN-family
// occurrence whose operand region is not literal-only, a lookup call, or a
// query parameter. A lexer error fails closed.
bool valuesTextIsUngoverned(const char *begin, const char *end) {
  std::vector<DB::Token> tokens;
  DB::Lexer lexer(begin, end);
  for (DB::Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken()) {
    if (token.isError()) return true;
    if (token.isSignificant()) tokens.push_back(token);
  }
  auto text = [](const DB::Token &t) { return std::string(t.begin, t.end); };
  auto upperText = [&](const DB::Token &t) {
    std::string value = text(t);
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return std::toupper(c); });
    return value;
  };
  auto unquoted = [&](const DB::Token &t) {
    std::string value = text(t);
    if (t.type == DB::TokenType::QuotedIdentifier && value.size() >= 2) value = value.substr(1, value.size() - 2);
    return value;
  };
  auto literalToken = [&](const DB::Token &t) {
    switch (t.type) {
    case DB::TokenType::Number: case DB::TokenType::StringLiteral: case DB::TokenType::Comma:
    case DB::TokenType::Minus: case DB::TokenType::Plus:
    case DB::TokenType::OpeningRoundBracket: case DB::TokenType::ClosingRoundBracket:
    case DB::TokenType::OpeningSquareBracket: case DB::TokenType::ClosingSquareBracket:
      return true;
    case DB::TokenType::BareWord: {
      const auto word = upperText(t);
      return word == "NULL" || word == "TRUE" || word == "FALSE";
    }
    default:
      return false;
    }
  };
  for (size_t i = 0; i < tokens.size(); ++i) {
    const auto &t = tokens[i];
    if (t.type == DB::TokenType::OpeningCurlyBrace) return true;
    const bool word = t.type == DB::TokenType::BareWord;
    const auto upper = upperText(t);
    if (word && (upper == "SELECT" || upper == "WITH" || upper == "FROM" || upper == "JOIN")) return true;
    const bool next_paren = i + 1 < tokens.size() && tokens[i + 1].type == DB::TokenType::OpeningRoundBracket;
    if ((word || t.type == DB::TokenType::QuotedIdentifier) && next_paren && isStringLookupName(unquoted(t)))
      return true;
    const bool in_keyword = word && upper == "IN";
    const bool in_callable = (word || t.type == DB::TokenType::QuotedIdentifier) && next_paren
      && isInFamilyFunction(unquoted(t));
    if (!in_keyword && !in_callable) continue;
    size_t j = i + 1;
    while (j < tokens.size() && (tokens[j].type == DB::TokenType::Plus || tokens[j].type == DB::TokenType::Minus)) ++j;
    if (j >= tokens.size()) return true;
    if (tokens[j].type == DB::TokenType::BareWord
        && (upperText(tokens[j]) == "TUPLE" || upperText(tokens[j]) == "ARRAY")
        && j + 1 < tokens.size() && tokens[j + 1].type == DB::TokenType::OpeningRoundBracket)
      ++j;
    if (tokens[j].type != DB::TokenType::OpeningRoundBracket && tokens[j].type != DB::TokenType::OpeningSquareBracket) {
      if (!literalToken(tokens[j])) return true;
      continue;
    }
    int depth = 0;
    size_t k = j;
    for (; k < tokens.size(); ++k) {
      const auto type = tokens[k].type;
      if (type == DB::TokenType::OpeningRoundBracket || type == DB::TokenType::OpeningSquareBracket) ++depth;
      if (type == DB::TokenType::ClosingRoundBracket || type == DB::TokenType::ClosingSquareBracket) --depth;
      if (!literalToken(tokens[k])) return true;
      if (depth == 0) break;
    }
    if (k >= tokens.size()) return true;  // unterminated region: fail closed
  }
  return false;
}

// rewriter-go measured: an INSERT whose column list is followed by a SETTINGS
// clause keeps its query as opaque text, so a read in that query is refused
// rather than rewritten (spec 2026-09-26 R2 / R5). A trailing SETTINGS on the
// SELECT itself is the ordinary form.
bool insertQueryIsOpaque(const DB::ASTInsertQuery &insert) {
  if (!insert.columns || !insert.settings_ast || !insert.select) return false;
  const auto *union_query = insert.select->as<DB::ASTSelectWithUnionQuery>();
  if (!union_query || !union_query->list_of_selects) return true;
  for (const auto &child : union_query->list_of_selects->children) {
    const auto *select = child->as<DB::ASTSelectQuery>();
    if (!select || select->settings()) return false;
  }
  return true;
}

bool opaqueInsertQueryIsUngoverned(const DB::ASTInsertQuery &insert) {
  const auto *union_query = insert.select->as<DB::ASTSelectWithUnionQuery>();
  if (!union_query || !union_query->list_of_selects || union_query->list_of_selects->children.size() != 1)
    return true;
  const auto *select = union_query->list_of_selects->children.front()->as<DB::ASTSelectQuery>();
  if (!select || select->tables() || select->with()) return true;
  for (const auto &child : select->children)
    if (child && expressionHasUngovernedRead(*child)) return true;
  return false;
}

} // namespace

bool rejectUngovernedReads(
```

In `src/handlers/table_reference.cc`, replace:

```cpp
        ungoverned = ungoverned || storageHasUngovernedRead(inner);
    }
  }
  if (!ungoverned) return false;
```

with:

```cpp
        ungoverned = ungoverned || storageHasUngovernedRead(inner);
    }
  } else if (const auto *insert = ast->as<DB::ASTInsertQuery>()) {
    // VALUES rows are read as rewriter-go's parser structures them; opaque
    // rows (a header SETTINGS clause) or rows that do not parse as
    // expressions take the lexical rule, which fails closed.
    const bool has_values = insert->data && insert->end > insert->data && insert->format == "Values";
    DB::ASTPtr rows = has_values && !valuesAreOpaque(*insert) ? parseValuesRows(*insert) : nullptr;
    ungoverned = insert->infile != nullptr
      || structuredExpressionHasRead(rows.get())
      || (has_values && !rows && valuesTextIsUngoverned(insert->data, insert->end))
      || (insertQueryIsOpaque(*insert) && opaqueInsertQueryIsUngoverned(*insert));
  }
  if (!ungoverned) return false;
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefUngoverned.InsertInfileValuesAndHeaderSettings'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 1299 tests`); corpus failures 64 → 34 (30 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/table_reference.cc
git commit -m "feat(table-ref): refuse ungoverned reads in INSERT (R2)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 19: R7 SHOW trailing clauses; SHOW COLUMNS / DESCRIBE resolve an unqualified target like FROM

The SHOW families the handlers forward verbatim keep `WHERE` / `LIMIT` outside `children`; a subquery, a non-operator function call, a non-literal IN region, a parameter or a quoted dotted name there is `statement is not supported` (with the surface active an SI physical name there takes the SI message first). An unqualified SHOW COLUMNS / SHOW INDEX / DESCRIBE target resolves through the logical context like a FROM reference (a bare quoted `db2.x` is the table `db2.x` in the session's database); ClickHouse's formatter prints SHOW COLUMNS with a `FROM <db>` clause.

**Files:**
- Modify: `tests/rewriter_test.cc`
- Modify: `src/handlers/table_reference.h`
- Modify: `src/handlers/table_reference.cc`
- Modify: `src/handlers/show_tables.cc`
- Modify: `src/handlers/describe.cc`

**Corpus cases:** 34: 32 M (25 M-R2 SHOW bodies, 5 M-R7, 2 M-T3) + 2 P.

- [ ] **Step 1: Write the failing tests.**

In `tests/rewriter_test.cc`, insert immediately before the `#if !REWRITER_RELEASE_MAIN` line that guards `int main(`:

```cpp
// Spec 2026-09-26 R7 for SHOW trailing clauses: a subquery, a function call,
// a non-literal IN region, a parameter or a quoted dotted name in WHERE /
// LIMIT is `statement is not supported`; with the SI surface active an SI
// physical name there carries the SI message first.
TEST(TableRefShow, TrailingClausesAreGoverned) {
  // With the SI surface active the session's logical database db1 is an SI
  // logical database, so a SHOW scoped to it is refused by the SI handler
  // first; the cluster-wide families still reach R7.
  for (const char *sql : {
         "SHOW CLUSTERS LIKE 'x' LIMIT (SELECT count() FROM `db2.x`)",
         "SHOW MERGES LIMIT (SELECT count() FROM `db2.x`)",
       }) {
    SCOPED_TRACE(std::string(sql) + " [V2]");
    const auto resp = RunTableRef(sql, /*si=*/true);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.message(), rewriter_handlers::kUnsupportedStatementMessage);
  }
  EXPECT_EQ(RunTableRef("SHOW CLUSTERS LIKE 'x' LIMIT 3", /*si=*/true).code(), rewriter::RewriteCode::Success);
  {
    const bool si = false;  // SI surface inactive
    for (const char *sql : {
           "SHOW COLUMNS FROM o LIMIT (SELECT count() FROM `db2.x`)",
           "SHOW FIELDS FROM o LIMIT 1 + (SELECT count() FROM `db2.x`)",
           "SHOW COLUMNS FROM o LIMIT throwIf(1)",
           "SHOW INDEX FROM o WHERE (SELECT count() FROM `db2.x`) = 2",
           "SHOW DICTIONARIES WHERE (SELECT count() FROM `db2.x`) = 2",
           "SHOW DICTIONARIES FROM default WHERE in(7, `db2.x`)",
           "SHOW CLUSTERS LIKE 'x' LIMIT (SELECT count() FROM `db2.x`)",
           "SHOW MERGES LIMIT (SELECT count() FROM `db2.x`)",
         }) {
      SCOPED_TRACE(std::string(sql) + (si ? " [V2]" : " [inactive]"));
      const auto resp = RunTableRef(sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
      EXPECT_EQ(resp.message(), rewriter_handlers::kUnsupportedStatementMessage);
    }
    for (const char *sql : {"SHOW DICTIONARIES LIKE 'a%'", "SHOW CLUSTERS LIKE 'x' LIMIT 3"}) {
      SCOPED_TRACE(std::string(sql) + (si ? " [V2]" : " [inactive]"));
      const auto resp = RunTableRef(sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
      EXPECT_EQ(resp.sql_after_rewrite(), sql);
    }
  }
  const auto physical = RunTableRef("SHOW DICTIONARIES FROM default WHERE in(7, hg_safe.db1__t)", /*si=*/true);
  EXPECT_EQ(physical.code(), rewriter::RewriteCode::UnsupportedStatement);
  EXPECT_EQ(physical.message(), "storage-integrity physical table hg_safe.db1__t is not directly addressable");
  const auto inactive = RunTableRef("SHOW DICTIONARIES FROM default WHERE in(7, hg_safe.db1__t)");
  EXPECT_EQ(inactive.code(), rewriter::RewriteCode::InvalidRewriteRequest);
  EXPECT_EQ(inactive.message(), "protected database hg_safe is not addressable");
}

// Spec 2026-09-26 R7: an unqualified SHOW COLUMNS / INDEX or DESCRIBE target
// resolves like a FROM reference — a bare quoted `db2.x` is the table named
// db2.x in the session's logical database. A qualified target stays verbatim.
TEST(TableRefShow, UnqualifiedTargetsResolveLikeFrom) {
  struct Case { const char *sql; bool si; const char *want; };
  for (const auto &c : std::vector<Case>{
         // ClickHouse's formatter spells the database as a FROM clause.
         {"SHOW COLUMNS FROM `db2.x`", false, "SHOW COLUMNS FROM `db1.db2.x` FROM phys"},
         {"SHOW COLUMNS FROM o WHERE name LIKE 'a%'", false, "SHOW COLUMNS FROM `db1.o` FROM phys WHERE name LIKE 'a%'"},
         {"SHOW COLUMNS FROM o LIMIT 5", false, "SHOW COLUMNS FROM `db1.o` FROM phys LIMIT 5"},
         {"DESCRIBE TABLE `db2.x`", false, "DESCRIBE TABLE phys.`db1.db2.x`"},
         {"DESCRIBE TABLE `db2.x`", true, "DESCRIBE TABLE phys.`db1.db2.x`"},
       }) {
    SCOPED_TRACE(std::string(c.sql) + (c.si ? " [V2]" : " [inactive]"));
    const auto resp = RunTableRef(c.sql, c.si);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
    EXPECT_EQ(resp.sql_after_rewrite(), c.want);
    ASSERT_EQ(resp.table_rewrites_size(), 1);
    EXPECT_EQ(resp.table_rewrites().begin()->second.rfind("phys.db1.", 0), 0u);
  }
}

```

- [ ] **Step 2: Run them and watch them fail.**

```bash
.superpowers/tableref-loop.sh 'TableRefShow.*'
```

Expected: Both `TableRefShow.*` tests fail (bodies forwarded; unqualified targets verbatim). Measured failing tests (all non-snapshot tests, this task's tests applied, implementation not): `TableRefShow.TrailingClausesAreGoverned`, `TableRefShow.UnqualifiedTargetsResolveLikeFrom`.

- [ ] **Step 3: Implement.**

In `src/handlers/table_reference.h`, replace:

```cpp
// Dynamic-mode table-reference preflight, run right after parsing, in the
```

with:

```cpp
// R7: a verbatim-forwarded SHOW statement's trailing clauses (WHERE / LIMIT)
// carry a subquery, a function call, a non-literal IN region, a parameter or
// a quoted dotted name.
bool showBodyIsUngoverned(const DB::IAST &show);
// The SHOW trailing-clause qualified name whose database is an SI physical
// database, for the SI-active message; empty when none.
std::pair<std::string, std::string> showBodyStorageIntegrityName(
  const DB::IAST &show, const rewriter::RewriteTableDynamicArgs &args);

// Dynamic-mode table-reference preflight, run right after parsing, in the
```

In `src/handlers/table_reference.cc`, replace:

```cpp
bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response) {
```

with:

```cpp
namespace {

// The trailing clauses ClickHouse keeps outside `children` for the SHOW
// families the handlers forward verbatim.
std::vector<const DB::IAST *> showBodies(const DB::IAST &show) {
  std::vector<const DB::IAST *> out;
  if (const auto *tables = show.as<DB::ASTShowTablesQuery>()) {
    out = {tables->where_expression.get(), tables->limit_length.get()};
  } else if (const auto *columns = show.as<DB::ASTShowColumnsQuery>()) {
    out = {columns->where_expression.get(), columns->limit_length.get()};
  } else if (const auto *indexes = show.as<DB::ASTShowIndexesQuery>()) {
    out = {indexes->where_expression.get()};
  }
  out.erase(std::remove(out.begin(), out.end(), nullptr), out.end());
  return out;
}

bool showExpressionUngoverned(const DB::IAST &node) {
  if (node.as<DB::ASTSubquery>() || isSelectFamily(node)) return true;
  if (const auto *fn = node.as<DB::ASTFunction>()) {
    if (!fn->isOperator() || inRegionRefused(*fn)) return true;
  }
  if (const auto *id = dynamic_cast<const DB::ASTIdentifier *>(&node)) {
    if (id->isParam()) return true;
    if (id->name_parts.size() == 1 && id->name_parts.front().find('.') != std::string::npos) return true;
  }
  bool found = false;
  forEachChild(node, [&](const DB::ASTPtr &child) {
    if (!found && child) found = showExpressionUngoverned(*child);
  });
  return found;
}

void showQualifiedNames(const DB::IAST &node, std::vector<std::pair<std::string, std::string>> &out) {
  if (const auto *id = dynamic_cast<const DB::ASTIdentifier *>(&node); id && id->name_parts.size() >= 2)
    out.emplace_back(id->name_parts[0], id->name_parts[1]);
  forEachChild(node, [&](const DB::ASTPtr &child) {
    if (child) showQualifiedNames(*child, out);
  });
}

} // namespace

bool showBodyIsUngoverned(const DB::IAST &show) {
  for (const auto *body : showBodies(show))
    if (showExpressionUngoverned(*body)) return true;
  return false;
}

std::pair<std::string, std::string> showBodyStorageIntegrityName(
  const DB::IAST &show, const rewriter::RewriteTableDynamicArgs &args) {
  std::vector<std::pair<std::string, std::string>> names;
  for (const auto *body : showBodies(show)) showQualifiedNames(*body, names);
  for (const auto &name : names)
    if (isStorageIntegrityPhysicalDatabase(name.first, args)) return name;
  return {};
}

bool preflightTableReferences(const DB::ASTPtr &ast,
  const rewriter::RewriteSQLRequest *request,
  rewriter::RewriteSQLResponse *response) {
```

In `src/handlers/show_tables.cc`, replace:

```cpp
#include "handlers/storage_integrity.h"

namespace rewriter_handlers {
```

with:

```cpp
#include "handlers/storage_integrity.h"
#include "handlers/table_reference.h"

namespace rewriter_handlers {
```

In `src/handlers/show_tables.cc`, replace:

```cpp
// The SHOW COLUMNS / INDEXES family's target, read straight off the AST.
```

with:

```cpp
// Spec 2026-09-26 R7 for a verbatim-forwarded SHOW statement's trailing
// clauses: with the SI surface active an SI physical name there is refused
// with the SI message; in both states any other ungoverned read is T7.
bool rejectShowBody(const DB::IAST &show,
                    const rewriter::RewriteTableDynamicArgs *dynamic,
                    rewriter::RewriteSQLResponse *response) {
  if (!dynamic) return false;
  if (storageIntegrityActive(dynamic)) {
    const auto [database, table] = showBodyStorageIntegrityName(show, *dynamic);
    if (!database.empty()) {
      recordPhysicalStorageIntegrityAccess(response, database, table, database);
      rejectUnsupported(response, storageIntegrityPhysicalRejectMessage(database + "." + table));
      return true;
    }
  }
  if (showBodyIsUngoverned(show)) {
    rejectUnsupported(response, std::string(kUnsupportedStatementMessage));
    return true;
  }
  return false;
}

// The SHOW COLUMNS / INDEXES family's target, read straight off the AST.
```

In `src/handlers/show_tables.cc`, replace:

```cpp
      || show_query->merges || show_query->caches) {
```

with:

```cpp
      || show_query->merges || show_query->caches) {
    if (rejectShowBody(*show_query, dynamic, response)) return true;
```

In `src/handlers/show_tables.cc`, replace:

```cpp
      return true;
    }
  }

  // The namespace is ordinary (or no storage-integrity contract is active):
```

with:

```cpp
      return true;
    }
  }

  if (rejectShowBody(*ast, dynamic, response)) return true;

  // Spec 2026-09-26 R7: an unqualified target resolves like a FROM reference
  // (a bare quoted `db2.x` is a table named db2.x in the session's logical
  // database). An explicit database stays verbatim, as does a name the
  // caller's databases do not map.
  if (dynamic && target.database.empty()) {
    const auto sel = findActiveTableRewrite(request->options());
    const auto outcome = applyDynamicRewrite("", target.table, *dynamic, "SHOW COLUMNS");
    if (outcome.status == DynamicRewriteStatus::OK) {
      recordAccessedTable(response, "", target.table, sel);
      if (auto *columns = ast->as<DB::ASTShowColumnsQuery>()) {
        columns->database = outcome.physical_db;
        columns->table = outcome.new_table;
      } else if (auto *indexes = ast->as<DB::ASTShowIndexesQuery>()) {
        indexes->database = outcome.physical_db;
        indexes->table = outcome.new_table;
      }
      recordTableRewrite(response, "", target.table, outcome.physical_db, outcome.new_table);
      setSuccessResponse(response, formatAst(ast), rewriter::STATEMENT_TYPE_SHOW_TABLES);
      return true;
    }
  }

  // The namespace is ordinary (or no storage-integrity contract is active):
```

In `src/handlers/describe.cc`, replace:

```cpp
#include <Parsers/TablePropertiesQueriesASTs.h>
```

with:

```cpp
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
```

In `src/handlers/describe.cc`, replace:

```cpp
      setSuccessResponse(response,
        describeMetadataSQL(hit->table->safe_table(), reservedRowIdColumn(args)),
        rewriter::STATEMENT_TYPE_DESCRIBE);
      return true;
    }
  }
```

with:

```cpp
      setSuccessResponse(response,
        describeMetadataSQL(hit->table->safe_table(), reservedRowIdColumn(args)),
        rewriter::STATEMENT_TYPE_DESCRIBE);
      return true;
    }
  }

  // Spec 2026-09-26 R7: an unqualified target resolves like a FROM reference
  // (a bare quoted `db2.x` is a table named db2.x in the session's logical
  // database). A qualified or unmapped target stays verbatim.
  if (selection.mode == TableRewriteMode::Dynamic && parsed->database.empty()) {
    const auto *describe = ast->as<DB::ASTDescribeQuery>();
    auto *te = describe->table_expression ? describe->table_expression->as<DB::ASTTableExpression>() : nullptr;
    auto *id = te && te->database_and_table_name
      ? te->database_and_table_name->as<DB::ASTTableIdentifier>() : nullptr;
    const auto outcome = applyDynamicRewrite("", parsed->table, *selection.dynamic_args, "DESCRIBE");
    if (id && outcome.status == DynamicRewriteStatus::OK) {
      id->resetTable(outcome.physical_db, outcome.new_table);
      recordTableRewrite(response, "", parsed->table, outcome.physical_db, outcome.new_table);
      setSuccessResponse(response, formatAst(ast), rewriter::STATEMENT_TYPE_DESCRIBE);
      return true;
    }
  }
```

- [ ] **Step 4: Run the task's tests, the whole non-snapshot suite and the corpus.**

```bash
.superpowers/tableref-loop.sh 'TableRefShow.*'
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*' | grep 'FAILED  \] SpecG' | sort -u | wc -l
```

Expected (measured): no non-snapshot failure other than corpus cases (`[  PASSED  ] 1335 tests`); corpus failures 34 → 0 (34 cases fixed).

- [ ] **Step 5: Commit.**

```bash
git add tests/rewriter_test.cc src/handlers/table_reference.h src/handlers/table_reference.cc src/handlers/show_tables.cc src/handlers/describe.cc
git commit -m "feat(table-ref): govern SHOW trailing clauses and resolve SHOW COLUMNS / DESCRIBE targets (R7)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 20: Documentation

`CLAUDE.md` and `AGENTS.md` carry the same architecture text; both get the same four edits. `README.md` gets one sentence.

**Files:**
- Modify: `CLAUDE.md`, `AGENTS.md`, `README.md`

- [ ] **Step 1: Apply the edits.**

In `CLAUDE.md`, replace:

```text
2. **Parse** — `DB::parseQuery(DB::ParserQuery, ...)` produces an `ASTPtr`. Parse errors return `RewriteCode::SyntaxError` with `Status::OK` (never a gRPC error).
```

with:

```text
2. **Parse** — `DB::parseQuery(DB::ParserQuery, ...)` produces an `ASTPtr`. Parse errors return `RewriteCode::SyntaxError` with `Status::OK` (never a gRPC error).
   - **Table-reference preflight (dynamic mode, housegate spec 2026-09-26)** — `preflightTableReferences` in [handlers/table_reference.cc](src/handlers/table_reference.cc) runs before any handler can rewrite a name, in the cross-engine precedence order. T2: an `{p:Identifier}` in any database or table position, or anywhere in an unmodelled class, is `InvalidRewriteRequest` `query parameters are not supported in a database or table position`. T7: an unmodelled statement class is `UnsupportedStatement` `statement is not supported` (with the SI surface active the SI write preflight runs first, then the SI catch-all text answers; a session `SET` is the one modelled pass-through while the surface is inactive). T3: a protected database — a `database_map` value, a `protected_databases` entry (each validated as a simple identifier), and while the SI surface is active any SI physical or reserved database — is `InvalidRewriteRequest` `protected database <db> is not addressable` in every read, target and carrier-argument position; while the surface is active an SI physical name defers to the SI handlers except inside a string-lookup argument. T5: the table-function, table-engine (matched case-sensitively) and `disk` / `storage_policy` table-setting allowlists. T6: every joinGet / joinGetOrNull / dictGet-family call, and every hasColumnInTable outside a rewritten SELECT body (where it is rewritten), is `InvalidRewriteRequest` `<fn> target "<arg>" does not resolve through the caller's databases`. R5: a SQL-bearing or dialect setting is a table-setting refusal and a non-literal setting value is `statement is not supported`. R2: a read in a position no rewrite reaches — ALTER actions and header-SETTINGS INSERT text (literal-only IN rule), structured UPDATE / DELETE, column / constraint / storage expressions and INSERT VALUES rows (a table, table function or table-naming IN operand), REFRESH views, `FROM INFILE` — is `statement is not supported`. While the SI surface is active, T5, R5 and R2 run inside the SI SELECT and write paths after the SI namespace policy, so an SI-owned message wins. The SELECT path also rewrites and reports IN operands that name a table (T4); SHOW COLUMNS / SHOW INDEX / DESCRIBE resolve an unqualified target like FROM, and a verbatim SHOW's WHERE / LIMIT refuses reads (R7).
```

In `CLAUDE.md`, replace:

```text
Non-SELECT statements that contain SELECT subqueries (e.g., `INSERT INTO t SELECT …`) currently bypass this pass. CREATE VIEW / MATERIALIZED VIEW bodies are the exception: they run the pass via `rewriteEmbeddedViewBody`.
```

with:

```text
INSERT … SELECT, CREATE TABLE … AS SELECT (EMPTY included) and CREATE VIEW / MATERIALIZED VIEW bodies run the pass through `rewriteEmbeddedBody` ([handlers/select.cc](src/handlers/select.cc)), which also rewrites them and reports their sources after the write target (spec 2026-09-26 T4); other non-SELECT statements bypass it.
```

In `CLAUDE.md`, replace:

```text
`build/tests/rewriter_tests`. The suite guard requires exactly 178
`SpecG/StorageIntegrityGolden` cases, two `StorageIntegrityCorpus` tests, and
eleven `SINormalize` tests.
```

with:

```text
`build/tests/rewriter_tests`. The suite guard requires exactly 966
`SpecG/StorageIntegrityGolden` cases, three `StorageIntegrityCorpus` tests (two
once the interim list below is gone), and eleven `SINormalize` tests.
```

In `CLAUDE.md`, replace:

```text
constants, and record `cmp` plus SHA-256 evidence in both PRs.
```

with:

```text
constants, and record `cmp` plus SHA-256 evidence in both PRs.

Interim, until the paired `want_sql_cpp` re-pin: the success cases listed in `tests/testdata/si_cpp_sql_pending.txt` compare their SQL through ClickHouse's own parser and formatter (`CanonicalClickHouseSQL`) instead. A listed case that matches its pin exactly fails, and `StorageIntegrityCorpus.InterimCanonicalListNamesUnpinnedSuccessCases` requires every entry to be an unpinned success case of the corpus. The re-pin deletes the file, the helper and both checks together.
```

In `AGENTS.md`, replace:

```text
2. **Parse** — `DB::parseQuery(DB::ParserQuery, ...)` produces an `ASTPtr`. Parse errors return `RewriteCode::SyntaxError` with `Status::OK` (never a gRPC error).
```

with:

```text
2. **Parse** — `DB::parseQuery(DB::ParserQuery, ...)` produces an `ASTPtr`. Parse errors return `RewriteCode::SyntaxError` with `Status::OK` (never a gRPC error).
   - **Table-reference preflight (dynamic mode, housegate spec 2026-09-26)** — `preflightTableReferences` in [handlers/table_reference.cc](src/handlers/table_reference.cc) runs before any handler can rewrite a name, in the cross-engine precedence order. T2: an `{p:Identifier}` in any database or table position, or anywhere in an unmodelled class, is `InvalidRewriteRequest` `query parameters are not supported in a database or table position`. T7: an unmodelled statement class is `UnsupportedStatement` `statement is not supported` (with the SI surface active the SI write preflight runs first, then the SI catch-all text answers; a session `SET` is the one modelled pass-through while the surface is inactive). T3: a protected database — a `database_map` value, a `protected_databases` entry (each validated as a simple identifier), and while the SI surface is active any SI physical or reserved database — is `InvalidRewriteRequest` `protected database <db> is not addressable` in every read, target and carrier-argument position; while the surface is active an SI physical name defers to the SI handlers except inside a string-lookup argument. T5: the table-function, table-engine (matched case-sensitively) and `disk` / `storage_policy` table-setting allowlists. T6: every joinGet / joinGetOrNull / dictGet-family call, and every hasColumnInTable outside a rewritten SELECT body (where it is rewritten), is `InvalidRewriteRequest` `<fn> target "<arg>" does not resolve through the caller's databases`. R5: a SQL-bearing or dialect setting is a table-setting refusal and a non-literal setting value is `statement is not supported`. R2: a read in a position no rewrite reaches — ALTER actions and header-SETTINGS INSERT text (literal-only IN rule), structured UPDATE / DELETE, column / constraint / storage expressions and INSERT VALUES rows (a table, table function or table-naming IN operand), REFRESH views, `FROM INFILE` — is `statement is not supported`. While the SI surface is active, T5, R5 and R2 run inside the SI SELECT and write paths after the SI namespace policy, so an SI-owned message wins. The SELECT path also rewrites and reports IN operands that name a table (T4); SHOW COLUMNS / SHOW INDEX / DESCRIBE resolve an unqualified target like FROM, and a verbatim SHOW's WHERE / LIMIT refuses reads (R7).
```

In `AGENTS.md`, replace:

```text
Non-SELECT statements that contain SELECT subqueries (e.g., `INSERT INTO t SELECT …`) currently bypass this pass. CREATE VIEW / MATERIALIZED VIEW bodies are the exception: they run the pass via `rewriteEmbeddedViewBody`.
```

with:

```text
INSERT … SELECT, CREATE TABLE … AS SELECT (EMPTY included) and CREATE VIEW / MATERIALIZED VIEW bodies run the pass through `rewriteEmbeddedBody` ([handlers/select.cc](src/handlers/select.cc)), which also rewrites them and reports their sources after the write target (spec 2026-09-26 T4); other non-SELECT statements bypass it.
```

In `AGENTS.md`, replace:

```text
`build/tests/rewriter_tests`. The suite guard requires exactly 178
`SpecG/StorageIntegrityGolden` cases, two `StorageIntegrityCorpus` tests, and
eleven `SINormalize` tests.
```

with:

```text
`build/tests/rewriter_tests`. The suite guard requires exactly 966
`SpecG/StorageIntegrityGolden` cases, three `StorageIntegrityCorpus` tests (two
once the interim list below is gone), and eleven `SINormalize` tests.
```

In `AGENTS.md`, replace:

```text
constants, and record `cmp` plus SHA-256 evidence in both PRs.
```

with:

```text
constants, and record `cmp` plus SHA-256 evidence in both PRs.

Interim, until the paired `want_sql_cpp` re-pin: the success cases listed in `tests/testdata/si_cpp_sql_pending.txt` compare their SQL through ClickHouse's own parser and formatter (`CanonicalClickHouseSQL`) instead. A listed case that matches its pin exactly fails, and `StorageIntegrityCorpus.InterimCanonicalListNamesUnpinnedSuccessCases` requires every entry to be an unpinned success case of the corpus. The re-pin deletes the file, the helper and both checks together.
```

In `README.md`, replace:

```text
The cross-engine golden contract is [tests/testdata/storage_integrity_cases.json]
```

with:

```text
In dynamic mode a table-reference policy (housegate spec 2026-09-26) runs before every handler: query parameters in table positions, unmodelled statement classes, databases named by `database_map` values or `protected_databases`, disallowed table functions, engines and settings, string lookups and reads in positions no rewrite reaches are refused with fixed cross-engine messages, while IN operands and INSERT … SELECT / CTAS / `view()` bodies are rewritten and reported like FROM. The cross-engine golden contract is [tests/testdata/storage_integrity_cases.json]
```

- [ ] **Step 2: Check.** `grep -c 'Table-reference preflight' CLAUDE.md AGENTS.md` prints `1` for each; `git diff --stat` touches only the three files; no hard line-wrapping in the new paragraphs.

- [ ] **Step 3: Commit.**

```bash
git add CLAUDE.md AGENTS.md README.md
git commit -m "docs: table-reference preflight, embedded bodies and the interim SQL rule

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 21: CI suite guard, full verification, push and PR

**Files:**
- Modify: `.github/scripts/snapshot-query-paired-ci.sh`

- [ ] **Step 1: Update the explicit suite counts** (the corpus grew from 256 to 966 cases and the interim meta-test is a third `StorageIntegrityCorpus` test):

In `.github/scripts/snapshot-query-paired-ci.sh`, replace:

```text
for spec in "SpecG/StorageIntegrityGolden.* 256" "StorageIntegrityCorpus.* 2" "SINormalize.* 11"; do
```

with:

```text
for spec in "SpecG/StorageIntegrityGolden.* 966" "StorageIntegrityCorpus.* 3" "SINormalize.* 11"; do
```

- [ ] **Step 2: Full local verification.**

```bash
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*:SINormalize.*'
ssh -p 30100 sentio@64.38.131.242 'cd /home/sentio/chen/rewriter-grpc && for f in "SpecG/StorageIntegrityGolden.*" "StorageIntegrityCorpus.*" "SINormalize.*"; do build/tests/rewriter_tests --gtest_filter="$f" --gtest_list_tests | grep -c "^  "; done'
```

Expected (measured on the staged tree): `[  PASSED  ] 1335 tests`, no failure; the corpus filter passes all 966 cases (109 of them through the interim rule); the three counts print `966`, `3`, `11`.

- [ ] **Step 3: Differential against the Go engine (informational until Task 22).** Build and start the Plan B server as in Task 22 Step 1, then from the rewriter-go follow-up checkout:

```bash
env POLYGLOT_SQL_FFI_PATH=$HOME/Library/Caches/housegate/rewriter-ffi/v0.13.0/libpolyglot_sql_ffi.dylib \
    REWRITER_ORACLE_ADDR=127.0.0.1:56151 \
  go test ./internal/harness/ -run '^TestStorageIntegrityGolden$' -count=1
```

Expected (measured): every divergence is a success case of the interim list whose SQL differs only in formatting (measured: 955 pass, 11 diverge, all on the interim list — 7 INSERT … SELECT / CTAS bodies that rewriter-go prints parenthesised and 4 `ALTER … ADD COLUMN` rows that ClickHouse prints as `(ADD COLUMN `c` …)`); no code, message, statement-type, accessed-table or table-rewrite divergence.

- [ ] **Step 4: Commit, push, open the PR.**

```bash
git add .github/scripts/snapshot-query-paired-ci.sh
git commit -m "ci: guard the 966-case corpus and the interim meta-test

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/table-reference-hardening
gh pr create --repo housegate/rewriter --base main --head feat/table-reference-hardening \
  --title "feat: table-reference hardening (spec 2026-09-26) — C++ engine" \
  --body-file .superpowers/planb-v2-pr-body.md
```

The PR body (written to `.superpowers/planb-v2-pr-body.md` first) states: the spec and this plan; the corpus SHA-256 `76d8db60921387db62452fd27661211da62e06fbbba894c90af5f7d7ccd230e0` copied from rewriter-go (PR #43 / `main`); the per-task case counts (Coverage); that 109 success cases pass through the interim rule until Task 22; the Step 3 differential result; and ends with the Claude Code attribution line. Do not merge: the PR merges only after Task 22, and only after the user says so.

### Task 22: Paired want_sql_cpp re-pin and zero divergences (interlock with rewriter-go follow-up Task 10)

This task and rewriter-go follow-up Task 10 are one procedure run from both repositories. Preconditions: rewriter-go PR #43 is merged (its corpus is on `main`, SHA-256 `76d8db60…`); this branch passes Task 21; the Plan B server built from this branch is the oracle.

| Step | Repository | Action | Gate |
|---|---|---|---|
| 1 | rewriter-grpc (box) | Build `clickhousegate_rewriter` from this branch, start it, tunnel `:56151` | server answers |
| 2 | rewriter-go (new branch off `main`) | Go Task 10 Step 2: `UPDATE_GOLDEN=1` with this server as oracle | only `want_sql*` / `allow_sql_divergence` change; exactly the 109 interim cases gain `want_sql_cpp` |
| 3 | rewriter-go | Go Task 10 Step 3: re-run without `UPDATE_GOLDEN` | zero divergences; record in spec §10.5 |
| 4 | rewriter-grpc | Copy the re-pinned corpus, re-pin `si_corpus.h`, remove the interim rule (this task's code) | corpus 966/966 exact, whole suite green |
| 5 | both | Record the same SHA-256 and `cmp -s` proof in both PR descriptions; ask the user to merge the rewriter-go re-pin PR, then this PR | user approval |

**Files:**
- Modify: `tests/testdata/storage_integrity_cases.json` (copied), `tests/si_corpus.h`, `tests/rewriter_test.cc`, `.github/scripts/snapshot-query-paired-ci.sh`, `CLAUDE.md`, `AGENTS.md`
- Delete: `tests/testdata/si_cpp_sql_pending.txt`

- [ ] **Step 1: Build and start the oracle** (the only `clickhousegate_rewriter` build of this plan; ~2.8 GB relink):

```bash
cd /Users/uranuswch/src/rewriter-grpc-tableref
.superpowers/tableref-loop.sh 'StorageIntegrityCorpus.*' rewriter_tests clickhousegate_rewriter
ssh -p 30100 sentio@64.38.131.242 'ss -ltn | grep -E ":5615[12] " || echo free'   # expect: free
ssh -p 30100 sentio@64.38.131.242 'cd /home/sentio/chen/rewriter-grpc && (nohup build/clickhousegate_rewriter 56151 56152 > /tmp/planb-v2-oracle.log 2>&1 &)'
ssh -f -N -o ExitOnForwardFailure=yes -p 30100 -L 56151:127.0.0.1:56151 sentio@64.38.131.242
```

- [ ] **Step 2: Run rewriter-go follow-up Task 10 Steps 2–3** in a rewriter-go worktree on a new branch off `main`:

```bash
GO=/Users/uranuswch/Dev/housegate/rewriter-go.fix-table-reference-cpp-pins
git -C /Users/uranuswch/Dev/housegate/rewriter-go fetch origin
git -C /Users/uranuswch/Dev/housegate/rewriter-go worktree add "$GO" -b fix/table-reference-cpp-pins origin/main
```

 Check the semantic diff: exactly the 109 names of Appendix B gain `allow_sql_divergence: true` and a `want_sql_cpp`, and every `want_sql_cpp` equals this server's `sql_after_rewrite`. The zero-divergence run is the gate; record it in spec §10.5.

- [ ] **Step 3: Copy the re-pinned corpus and compute its pins.**

```bash
GO=/Users/uranuswch/Dev/housegate/rewriter-go.fix-table-reference-cpp-pins
cp "$GO/internal/harness/testdata/storage_integrity_cases.json" tests/testdata/storage_integrity_cases.json
cmp -s tests/testdata/storage_integrity_cases.json "$GO/internal/harness/testdata/storage_integrity_cases.json" && echo identical
shasum -a 256 tests/testdata/storage_integrity_cases.json
wc -c < tests/testdata/storage_integrity_cases.json
python3 -c "import sys
h=0xcbf29ce484222325
for b in open(sys.argv[1],'rb').read(): h=((h^b)*0x100000001b3)&0xFFFFFFFFFFFFFFFF
print(h)" tests/testdata/storage_integrity_cases.json
```

(The FNV-1a/64 one-liner prints `10342073271660396945` for today's corpus; it is the same function as `si_corpus::Fingerprint`.) In `tests/si_corpus.h`, set `kCorpusFingerprint` to the printed FNV value with a `ULL` suffix, `kCorpusBytes` to the byte count, and keep `kCorpusCases = 966`. They must equal the three constants the rewriter-go re-pin branch writes into `internal/harness/sicorpus_test.go`.

- [ ] **Step 4: Remove the interim rule.** These edits reverse Task 1's own insertions exactly:

In `tests/rewriter_test.cc`, replace:

```cpp
class StorageIntegrityGolden : public ::testing::TestWithParam<SIGoldenCase> {};

// INTERIM — Plan B v2 Task 22 deletes this block, si_cpp_sql_pending.txt and
// the interim branch of MatchesSharedCorpus at the paired want_sql_cpp re-pin
// (rewriter-go follow-up Task 10). Until then a listed success case passes its
// SQL check when ClickHouse's own parser and formatter print the C++ output
// and the shared want_sql identically: the formatting-only differences (alias
// AS, ENGINE = spacing, IN-operand parentheses, parameter spacing) fold, while
// a difference in any name, operand or clause does not. An unparseable text
// never compares equal. Every other case compares exactly.
const std::set<std::string> &InterimCanonicalCases() {
  static const std::set<std::string> names = [] {
    std::set<std::string> out;
    std::ifstream in(std::string(REWRITER_TEST_DATA_DIR) + "/si_cpp_sql_pending.txt");
    for (std::string line; std::getline(in, line);)
      if (!line.empty() && line.front() != '#') out.insert(line);
    return out;
  }();
  return names;
}

std::string CanonicalClickHouseSQL(const std::string &sql) {
  try {
    const char *begin = sql.data();
    const char *end = begin + sql.size();
    DB::ParserQuery parser(end, false);
    const auto ast = DB::parseQuery(parser, begin, end, /*description=*/"", 0, 0, 0);
    std::string out = rewriter_handlers::formatAst(ast);
    if (const auto *insert = ast->as<DB::ASTInsertQuery>(); insert && insert->data)
      out += " " + std::string(insert->data, insert->end);
    return out;
  } catch (const DB::Exception &e) {
    return "<unparseable: " + e.displayText() + "> " + sql;
  }
}
```

with:

```cpp
class StorageIntegrityGolden : public ::testing::TestWithParam<SIGoldenCase> {};
```

In `tests/rewriter_test.cc`, replace:

```cpp
    const std::string expected = si_corpus::NormalizeSIIdentifierQuotes(c.ExpectedSQL());
    if (InterimCanonicalCases().count(c.name) == 0) {
      EXPECT_EQ(normalized_sql, expected) << "SQL pin mismatch for " << c.name;
    } else {
      // INTERIM (si_cpp_sql_pending.txt): the entry must still be needed…
      EXPECT_NE(normalized_sql, expected)
        << c.name << " now matches its pin exactly: remove it from si_cpp_sql_pending.txt";
      // …and the C++ SQL must be the pinned statement up to ClickHouse's own
      // formatting.
      EXPECT_EQ(CanonicalClickHouseSQL(resp.sql_after_rewrite()), CanonicalClickHouseSQL(c.want_sql))
        << "SQL mismatch for " << c.name << " (interim ClickHouse-canonical comparison)";
    }
```

with:

```cpp
    EXPECT_EQ(normalized_sql, si_corpus::NormalizeSIIdentifierQuotes(c.ExpectedSQL()))
      << "SQL pin mismatch for " << c.name;
```

In `tests/rewriter_test.cc`, replace:

```cpp
INSTANTIATE_TEST_SUITE_P(SpecG, StorageIntegrityGolden, ::testing::ValuesIn(loadSIGoldenCases()),
  [](const ::testing::TestParamInfo<SIGoldenCase> &info) { return info.param.name; });

// INTERIM: every si_cpp_sql_pending.txt entry names an unpinned success case
// of the corpus (a reject echoes its input; a divergence case is pinned).
TEST(StorageIntegrityCorpus, InterimCanonicalListNamesUnpinnedSuccessCases) {
  std::vector<std::string> ignored;
  const auto cases = si_corpus::LoadCases(
    std::string(REWRITER_TEST_DATA_DIR) + "/storage_integrity_cases.json", &ignored);
  std::map<std::string, const si_corpus::Case *> by_name;
  for (const auto &c : cases) by_name[c.name] = &c;
  for (const auto &name : InterimCanonicalCases()) {
    SCOPED_TRACE(name);
    const auto it = by_name.find(name);
    ASSERT_NE(it, by_name.end()) << "not a corpus case";
    EXPECT_EQ(it->second->want_code, "Success");
    EXPECT_FALSE(it->second->allow_sql_divergence);
  }
}
```

with:

```cpp
INSTANTIATE_TEST_SUITE_P(SpecG, StorageIntegrityGolden, ::testing::ValuesIn(loadSIGoldenCases()),
  [](const ::testing::TestParamInfo<SIGoldenCase> &info) { return info.param.name; });
```

Delete `tests/testdata/si_cpp_sql_pending.txt`:

```bash
git rm tests/testdata/si_cpp_sql_pending.txt
```

In `.github/scripts/snapshot-query-paired-ci.sh`, replace:

```text
"StorageIntegrityCorpus.* 3"
```

with:

```text
"StorageIntegrityCorpus.* 2"
```

In `CLAUDE.md`, replace:

```text
constants, and record `cmp` plus SHA-256 evidence in both PRs.

Interim, until the paired `want_sql_cpp` re-pin: the success cases listed in `tests/testdata/si_cpp_sql_pending.txt` compare their SQL through ClickHouse's own parser and formatter (`CanonicalClickHouseSQL`) instead. A listed case that matches its pin exactly fails, and `StorageIntegrityCorpus.InterimCanonicalListNamesUnpinnedSuccessCases` requires every entry to be an unpinned success case of the corpus. The re-pin deletes the file, the helper and both checks together.
```

with:

```text
constants, and record `cmp` plus SHA-256 evidence in both PRs.
```

In `CLAUDE.md`, replace:

```text
`build/tests/rewriter_tests`. The suite guard requires exactly 966
`SpecG/StorageIntegrityGolden` cases, three `StorageIntegrityCorpus` tests (two
once the interim list below is gone), and eleven `SINormalize` tests.
```

with:

```text
`build/tests/rewriter_tests`. The suite guard requires exactly 966
`SpecG/StorageIntegrityGolden` cases, two `StorageIntegrityCorpus` tests, and
eleven `SINormalize` tests.
```

In `AGENTS.md`, replace:

```text
constants, and record `cmp` plus SHA-256 evidence in both PRs.

Interim, until the paired `want_sql_cpp` re-pin: the success cases listed in `tests/testdata/si_cpp_sql_pending.txt` compare their SQL through ClickHouse's own parser and formatter (`CanonicalClickHouseSQL`) instead. A listed case that matches its pin exactly fails, and `StorageIntegrityCorpus.InterimCanonicalListNamesUnpinnedSuccessCases` requires every entry to be an unpinned success case of the corpus. The re-pin deletes the file, the helper and both checks together.
```

with:

```text
constants, and record `cmp` plus SHA-256 evidence in both PRs.
```

In `AGENTS.md`, replace:

```text
`build/tests/rewriter_tests`. The suite guard requires exactly 966
`SpecG/StorageIntegrityGolden` cases, three `StorageIntegrityCorpus` tests (two
once the interim list below is gone), and eleven `SINormalize` tests.
```

with:

```text
`build/tests/rewriter_tests`. The suite guard requires exactly 966
`SpecG/StorageIntegrityGolden` cases, two `StorageIntegrityCorpus` tests, and
eleven `SINormalize` tests.
```

- [ ] **Step 5: Verify.**

```bash
.superpowers/tableref-loop.sh '-*Snapshot*'
.superpowers/tableref-loop.sh 'SpecG/*:StorageIntegrityCorpus.*'
grep -rn 'si_cpp_sql_pending\|CanonicalClickHouseSQL\|InterimCanonical' tests src CLAUDE.md AGENTS.md .github || echo clean
```

Expected: all pass; all 966 corpus cases compare exactly; `StorageIntegrityCorpus.*` lists 2 tests; the grep prints `clean`. Then re-run the Go differential of Task 21 Step 3 against the restarted server: zero divergences.

- [ ] **Step 6: Stop the oracle and the tunnel.**

```bash
ssh -p 30100 sentio@64.38.131.242 'kill $(pgrep -f "^build/clickhousegate_rewriter 56151")'
pkill -f 'ssh -f -N -o ExitOnForwardFailure=yes -p 30100 -L 56151'
```

- [ ] **Step 7: Commit, push, and record the pairing.**

```bash
git add -A tests/testdata/storage_integrity_cases.json tests/testdata/si_cpp_sql_pending.txt tests/si_corpus.h tests/rewriter_test.cc .github/scripts/snapshot-query-paired-ci.sh CLAUDE.md AGENTS.md
git commit -m "test(corpus): pin want_sql_cpp (paired with rewriter-go) and drop the interim rule

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push
```

Edit both PR descriptions to carry the same SHA-256, the `cmp -s` result and the zero-divergence record. **Ask the user** before merging either PR; merge the rewriter-go re-pin PR first.

### Task 23: Release rewriter-grpc v0.16.0

Precondition: the user approved and merged this PR; rewriter-go v0.16.0 is released first (done 2026-10-01; v0.14.0 predates the hardening and v0.15.0 lacks the §13 fixes, so neither is a valid pairing). **Ask the user before each command that creates a tag or release.**

- [ ] **Step 1: Dry run.**

```bash
gh workflow run cut-release.yml --repo housegate/rewriter -f bump=minor -f dry_run=true
gh run watch --repo housegate/rewriter $(gh run list --repo housegate/rewriter --workflow cut-release.yml --limit 1 --json databaseId -q '.[0].databaseId')
```

Expected: the log prints next tag `v0.16.0` (latest tag today is `v0.15.0`).

- [ ] **Step 2: Cut the release** (after the user says yes):

```bash
gh workflow run cut-release.yml --repo housegate/rewriter -f bump=minor -f dry_run=false
```

`cut-release.yml` tags `main`, creates the GitHub release and dispatches `release.yml` for the Docker image. Watch both runs; confirm `gh release view v0.16.0 --repo housegate/rewriter` and the image tag. Plan C (housegate) then pins rewriter-go v0.16.0 and rewriter-grpc v0.16.0.

## Evidence

Everything below was measured on 2026-09-29 on the build box (`/home/sentio/chen/rewriter-grpc`, every `ninja` under `flock /home/sentio/ci/rewriter-build-box.lock`) from throwaway local edits of `/Users/uranuswch/src/rewriter-grpc-tableref`. Nothing was committed or pushed; the worktree and the box were returned to clean `56a17b7` afterwards (see the end of this section).

**How the plan's code was verified.** Every code block of Tasks 1–22 is rendered from a machine-applied edit script (`2026-09-29-table-reference-hardening-engine-cpp-v2-stage/`, next to this plan: `apply.py` plus one `tNN.py` per task). `apply.py` applies each "replace / insert before / insert after" only when its anchor occurs exactly once in the file at that point of the sequence, so every quoted "replace this" snippet was checked against the real files of `56a17b7` + the preceding tasks. Tasks 1–19 were then applied one task at a time — first the test edits alone (RED), then the implementation (GREEN) — and built and run after each half. Task 20–22's edits were applied to a copy of the final tree to check their anchors.

**Prototype history.** (1) A first full prototype (superseded; the diff next to this plan is now the staged tree of Tasks 0–21) passed all 966 corpus cases; against it the rewriter-go harness differential on the merged 987-case corpus showed 64 divergences, all owned elsewhere: G 46 and P-contract 6 and the SET message 1 (the rewriter-go follow-up), F 4 and 7 CTAS/INSERT parenthesisation formats (the paired re-pin). (2) Before writing this plan, every SQL row of its GoogleTests (about 400 statements × surface inactive / V2) was sent to both the rewriter-go follow-up engine (`248ecf2`, FFI v0.13.0) and the staged C++ server. The first run found six behaviour differences the prototype had: T2 relabelled by the SI annotator (fixed in Task 3), a blind SI name reported as logical (Task 5), `ALTER … IN hg_safe.`db2.x`` forwarded to R2 because the multi-part IN operand never split (Task 5's decode fix — the prototype had closed the hole only by accident of R2), INSERT … SELECT / CTAS bodies rewriting an SI-namespace IN operand that rewriter-go refuses (Task 9), `joinGet` in VALUES rows (Task 13), and the literal-only rule applied to structured UPDATE / DELETE / VALUES where rewriter-go structures the expression (Tasks 17–18, CTE-aware). All are fixed in the plan's code; the final run's residue is listed under "Out-of-corpus differences" below.

**Per-task measurements (Tasks 1–19).** "RED" is the whole non-snapshot suite with the task's tests applied and its implementation not; "GREEN" the same after the implementation; corpus failures count failing `SpecG/StorageIntegrityGolden` cases of the 966. Baseline (`56a17b7` + this corpus + Task 1's runner) fails 636 corpus cases (634 compared failures plus the 2 crash cases; measured with Task 1 applied and the crash cases filtered out).

| Task | RED (tests only) | GREEN non-snapshot passed | Corpus failures after | Fixed | Assigned |
|---:|---|---:|---:|---:|---:|
| 1 | crash in corpus run (v0.15.0 segfault) | crash* | 636 | 0 | 0 |
| 2 | 1 failing: StorageIntegrityCompatibility.ClauseKeywordIsNotAnImplicitAlias | crash* | 628 | 8 | 8 |
| 3 | build fails: 'handlers/table_reference.h' file not found | 724 | 587 | 41 | 41 |
| 4 | 3 failing: StorageIntegrityCatchAll.UnmodelledStatementIsRefusedWithoutStorageIntegrity, StorageIntegrityContractV2.V1EmptyMapStaysLegacy, TableRefClasses.UnmodelledClassesAreRefused | 740 | 572 | 15 | 19 |
| 5 | 4 failing: TableRefProtected.EntriesMustBeSimpleIdentifiers, TableRefProtected.ProtectedKnownPhysicalDatabaseIsNotAPassThrough, TableRefProtected.ProtectedLogicalContextIsRefused … | 851 | 466 | 106 | 102 |
| 6 | 1 failing: TableRefProtected.RefusedInStatementTargets | 914 | 404 | 62 | 62 |
| 7 | 1 failing: TableRefProtected.RefusedInCarrierArguments | 938 | 381 | 23 | 23 |
| 8 | 2 failing: StorageIntegrityNamespaces.IdentifierEscapesAreSemanticButLiteralsAreNotDecodedAgain, TableRefSources.InOperandsAreRewrittenAndReported | 994 | 327 | 54 | 54 |
| 9 | 3 failing: TableRefSources.EmbeddedBodiesAreRewrittenAndReported, WriteOp.CreateTable_AsSelect_ReturnsTargetAndSource, WriteOp.Insert_Select_ReturnsTargetAndSource | 1014 | 308 | 19 | 19 |
| 10 | 1 failing: TableRefParity.MaterializedViewToActiveTableIsRefused | 1017 | 306 | 2 | 2 |
| 11 | 4 failing: StorageIntegrityNamespaces.IdentifierEscapesAreSemanticButLiteralsAreNotDecodedAgain, TableRefAllowlist.StorageIntegrityMessagesPrecedeAllowlistsWhenActive, TableRefAllowlist.TableFunctions … | 1043 | 283 | 23 | 23 |
| 12 | 1 failing: TableRefAllowlist.EnginesAndTableSettings | 1076 | 251 | 32 | 32 |
| 13 | 1 failing: TableRefLookup.ResolvesOrRefuses | 1090 | 238 | 13 | 13 |
| 14 | 1 failing: TableRefSettings.SqlBearingDialectAndValueRules | 1131 | 198 | 40 | 40 |
| 15 | 1 failing: TableRefAnnotation.OnlyProvenNamesUpgradeTheMessage | 1132 | 198 | 0 | 0 |
| 16 | 2 failing: StorageIntegrityAlterExpressions.RejectsEmbeddedLogicalAndPhysicalReads, TableRefUngoverned.AlterActions | 1253 | 78 | 120 | 122 |
| 17 | 2 failing: StorageIntegrityCreateExpressions.RejectsEmbeddedColumnAndConstraintReads, TableRefUngoverned.MutationsCreateExpressionsAndRefresh | 1268 | 64 | 14 | 12 |
| 18 | 1 failing: TableRefUngoverned.InsertInfileValuesAndHeaderSettings | 1299 | 34 | 30 | 30 |
| 19 | 2 failing: TableRefShow.TrailingClausesAreGoverned, TableRefShow.UnqualifiedTargetsResolveLikeFrom | 1335 | 0 | 34 | 34 |

\* Tasks 1–2: the corpus run aborts on the v0.15.0 segfault (EXISTS / SHOW CREATE of an Identifier parameter), so the counts there are from a separate run with the two crash cases filtered out (`--gtest_filter='SpecG/*-*si_tr_param_exists_rejected:*si_tr_param_show_create_rejected'`: 634 failures after Task 1 and 626 after Task 2, plus the 2 crash cases each time; `StorageIntegrityCorpus.*` passes 3 tests after Task 1). Measured task totals differ from the assigned counts by a few cases where two tasks' rules overlap on one case (four cases assigned to Task 4 pass only once Task 5 lands; two assigned to Task 16 pass only once Task 17 lands); every case is assigned once, and the count reaches 0.

**Crash reproduction.** On `56a17b7` with this corpus the test binary dies (SIGSEGV) at `si_tr_param_show_create_rejected` / `si_tr_param_exists_rejected`; with Task 3 the same cases return `InvalidRewriteRequest` with the T2 text, and `TableRefParam.ShowCreateAndExistsNeverReachTheFormatterWithAnEmptyName` covers dynamic, static and no-rewrite mode.

**SI fail-open proof (B4).** `TableRefSources.InOperandChangeKeepsStorageIntegrityRefusals` (18 positions, parenthesised and bare) passes at every stage from Task 8 on, and the SI-active write-body rows of `TableRefSources.EmbeddedBodiesAreRewrittenAndReported` pass from Task 9 on. `decodeInFunction` still classifies every identifier operand; the only IN-ref exclusion is the SELECT root's `withoutInOperandRefs`, which runs after the SELECT walk has rewritten (and, for an SI physical context, refused) those operands.

**Final staged tree.** Whole suite: `[  PASSED  ] 1349 tests`, 641 failed — all `*Snapshot*` (the same 641 environment failures as `56a17b7`, which passes 610). Corpus: 966 / 966 pass, 109 through the interim rule. Go harness differential on the 966-case corpus (rewriter-go `248ecf2` against the staged server): 955 pass and 11 diverge — all 11 on the interim list and all SQL spelling: 7 INSERT … SELECT / CTAS bodies (rewriter-go prints the body parenthesised, `INSERT INTO t (SELECT …)` / `AS (SELECT …)`) and 4 `ALTER … ADD COLUMN c UInt8 DEFAULT a IN [1, 2]` rows (ClickHouse wraps the action in parentheses and quotes the column); no code, message, statement-type, accessed-table or table-rewrite divergence.

**Out-of-corpus differences (final probe: 412 statements × 2 = 824 requests, 136 differ).** 124 differ only in SQL spelling (quote style, `AS` before aliases, `INNER JOIN`, ClickHouse's `SHOW COLUMNS FROM t FROM db` and `ALTER … (…)` / `toIntervalDay` forms, the INSERT header-SETTINGS echo) — the kind the paired re-pin pins per engine. The rest are deliberate or pre-existing and none opens a read:
- `ALTER TABLE db1.o DELETE WHERE a IN (+1, -2)` under V2: rewriter-go refuses (`statement is not supported`), C++ rewrites. The spec's literal-only region admits signs, and rewriter-go admits the same statement with the surface inactive; this is a rewriter-go over-refusal, recorded here for the rewriter-go owner and deliberately not copied.
- `ALTER TABLE db1.o DELETE WHERE a IN db1.t` under V2: both refuse; rewriter-go says `accepts writes only through the signed statement lane`, C++ `is not directly addressable through IN table target` (more precise; the corpus pins the code only).
- `CREATE VIEW db1.v AS SELECT * FROM hg_*.`db2.x`` under V2: both refuse with the same message; code `RewriteError` (Go) vs `UnsupportedStatement` (C++, the pre-existing view-body code).
- `CREATE TABLE db1.n (a UInt64 DEFAULT (SELECT … FROM hg_*.`db2.x`))` under V2: same code and message; C++ also reports the SI physical object in `original_accessed_tables` (pre-existing column-definition preflight), Go reports none — spec §5 allows a partial list on a rejection.
- `INSERT INTO db.t …` / `UPDATE other.u …` with an unmapped database: same code; the pre-existing C++ legacy wording (`INSERT target references logical database 'db' which is not in database_map …`) vs Go's `logical db db not in database_map …`.

**Box and worktree restored.** Local: `git status` in `/Users/uranuswch/src/rewriter-grpc-tableref` is empty, the branch is at `56a17b7` with no commits, `third_party/rewriter-proto` is back at `192e43a`. Box: the clean `56a17b7` tree was rsynced back (a second sync transfers nothing), every source was touched and `rewriter_tests`, `clickhousegate_rewriter` and `clickhousegate_rewriter_cli` were rebuilt under the lock; the rebuilt baseline passes 610 tests (596 with `-*Snapshot*`) and fails the same 641 snapshot tests; no server listens on 56151/56152, the tunnel is closed, and the measurement directory `/home/sentio/chen/tableref-inv/v2stage` was removed (the directory's pre-existing files are untouched). Disk: 25 GB free, as before.

## Appendix A: corpus cases by task

Class from `corpus-divergence.tsv` (M, P, G = a replaced G input; `new` = added by the follow-up).

<details><summary>Task 2 — 8 cases</summary>

- `si_tr_show_columns_where_hg_safe_subquery_v2_rejected` (P)
- `si_tr_show_columns_where_remote_subquery_v2_rejected` (P)
- `si_tr_show_columns_where_merge_subquery_v2_rejected` (P)
- `si_tr_show_columns_where_dotted_subquery_v2_rejected` (P)
- `si_tr_show_index_where_subquery_v2_rejected` (P)
- `si_tr_r2_show_columns_limit_subquery_v2_rejected` (P)
- `si_tr_r2_show_fields_limit_subquery_v2_rejected` (P)
- `si_tr_r2_show_columns_from_db_limit_subquery_v2_rejected` (P)

</details>

<details><summary>Task 3 — 41 cases</summary>

- `si_tr_param_from_bare_rejected` (M)
- `si_tr_param_from_qualified_rejected` (M)
- `si_tr_param_db_rejected` (M)
- `si_tr_param_join_rejected` (M)
- `si_tr_param_subquery_rejected` (M)
- `si_tr_param_in_rejected` (M)
- `si_tr_param_in_qualified_rejected` (M)
- `si_tr_param_in_call_rejected` (M)
- `si_tr_param_insert_select_source_rejected` (M)
- `si_tr_param_ctas_source_rejected` (M)
- `si_tr_param_mv_to_rejected` (M)
- `si_tr_param_mv_name_rejected` (M)
- `si_tr_param_drop_bare_rejected` (M)
- `si_tr_param_drop_rejected` (M)
- `si_tr_param_insert_target_rejected` (M)
- `si_tr_param_create_target_rejected` (M)
- `si_tr_param_exists_rejected` (M)
- `si_tr_param_show_create_rejected` (M)
- `si_tr_param_describe_rejected` (M)
- `si_tr_param_rename_rejected` (M)
- `si_tr_param_show_tables_from_rejected` (M)
- `si_tr_param_use_rejected` (M)
- `si_tr_param_alter_update_rejected` (M)
- `si_tr_param_create_database_rejected` (M)
- `si_tr_param_drop_database_rejected` (M)
- `si_tr_in_nested_paren_param_rejected` (M)
- `si_tr_in_nested_paren_param_v2_rejected` (M)
- `si_tr_in_paren_param_rejected` (M)
- `si_tr_in_paren_param_v2_rejected` (M)
- `si_tr_alter_update_param_subquery_rejected` (M)
- `si_tr_alter_update_param_subquery_v2_rejected` (M)
- `si_tr_alter_update_param_assignment_rejected` (M)
- `si_tr_alter_update_param_assignment_v2_rejected` (M)
- `si_tr_alter_delete_in_param_rejected` (M)
- `si_tr_alter_delete_in_param_v2_rejected` (M)
- `si_tr_alter_modify_query_param_rejected` (M)
- `si_tr_alter_modify_query_param_v2_rejected` (M)
- `si_tr_explain_param_rejected` (M)
- `si_tr_explain_param_v2_rejected` (M)
- `si_tr_r5_alter_delete_plus_param_in_rejected` (M)
- `si_tr_r5_alter_delete_plus_param_in_v2_rejected` (M)

</details>

<details><summary>Task 4 — 19 cases</summary>

- `si_tr_system_inactive_rejected` (M)
- `si_tr_set_inactive_passthrough` (M)
- `si_tr_explain_rejected` (M)
- `si_tr_check_table_inactive_rejected` (M)
- `si_tr_create_user_inactive_rejected` (M)
- `si_tr_detach_rejected` (M)
- `si_tr_kill_rejected` (M)
- `si_tr_set_plain_values_passthrough` (G)
- `si_tr_describe_subquery_phys_rejected` (M)
- `si_tr_describe_subquery_hg_safe_rejected` (M)
- `si_tr_describe_table_subquery_rejected` (M)
- `si_tr_describe_table_subquery_v2_rejected` (G)
- `si_tr_detach_phys_rejected` (M)
- `si_tr_detach_phys_v2_rejected` (M)
- `si_tr_optimize_phys_rejected` (M)
- `si_tr_optimize_phys_v2_rejected` (M)
- `si_tr_kill_v2_rejected` (M)
- `si_tr_attach_v2_rejected` (M)
- `si_tr_detach_v2_rejected` (M)

</details>

<details><summary>Task 5 — 102 cases</summary>

- `si_tr_protected_phys_from_rejected` (M)
- `si_tr_protected_phys_join_rejected` (M)
- `si_tr_protected_phys_subquery_rejected` (M)
- `si_tr_protected_phys_cte_rejected` (M)
- `si_tr_protected_phys_in_rejected` (M)
- `si_tr_protected_phys_in_paren_rejected` (M)
- `si_tr_protected_phys_in_tuple_rejected` (M)
- `si_tr_protected_phys_in_call_rejected` (M)
- `si_tr_protected_phys_global_in_rejected` (M)
- `si_tr_protected_phys_insert_select_source_rejected` (M)
- `si_tr_protected_phys_ctas_source_rejected` (M)
- `si_tr_protected_phys_view_body_rejected` (M)
- `si_tr_protected_phys_describe_rejected` (M)
- `si_tr_protected_hg_safe_from_rejected` (M)
- `si_tr_protected_hg_safe_join_rejected` (M)
- `si_tr_protected_hg_safe_subquery_rejected` (M)
- `si_tr_protected_hg_safe_cte_rejected` (M)
- `si_tr_protected_hg_safe_in_rejected` (M)
- `si_tr_protected_hg_safe_in_paren_rejected` (M)
- `si_tr_protected_hg_safe_in_tuple_rejected` (M)
- `si_tr_protected_hg_safe_in_call_rejected` (M)
- `si_tr_protected_hg_safe_global_in_rejected` (M)
- `si_tr_protected_hg_safe_insert_select_source_rejected` (M)
- `si_tr_protected_hg_safe_ctas_source_rejected` (M)
- `si_tr_protected_hg_safe_view_body_rejected` (M)
- `si_tr_protected_hg_safe_describe_rejected` (M)
- `si_tr_protected_hg_unsafe_from_rejected` (M)
- `si_tr_protected_hg_unsafe_join_rejected` (M)
- `si_tr_protected_hg_unsafe_subquery_rejected` (M)
- `si_tr_protected_hg_unsafe_cte_rejected` (M)
- `si_tr_protected_hg_unsafe_in_rejected` (M)
- `si_tr_protected_hg_unsafe_in_paren_rejected` (M)
- `si_tr_protected_hg_unsafe_in_tuple_rejected` (M)
- `si_tr_protected_hg_unsafe_in_call_rejected` (M)
- `si_tr_protected_hg_unsafe_global_in_rejected` (M)
- `si_tr_protected_hg_unsafe_insert_select_source_rejected` (M)
- `si_tr_protected_hg_unsafe_ctas_source_rejected` (M)
- `si_tr_protected_hg_unsafe_view_body_rejected` (M)
- `si_tr_protected_hg_unsafe_describe_rejected` (M)
- `si_tr_protected_hg_promote_from_rejected` (M)
- `si_tr_protected_hg_promote_join_rejected` (M)
- `si_tr_protected_hg_promote_subquery_rejected` (M)
- `si_tr_protected_hg_promote_cte_rejected` (M)
- `si_tr_protected_hg_promote_in_rejected` (M)
- `si_tr_protected_hg_promote_in_paren_rejected` (M)
- `si_tr_protected_hg_promote_in_tuple_rejected` (M)
- `si_tr_protected_hg_promote_in_call_rejected` (M)
- `si_tr_protected_hg_promote_global_in_rejected` (M)
- `si_tr_protected_hg_promote_insert_select_source_rejected` (M)
- `si_tr_protected_hg_promote_ctas_source_rejected` (M)
- `si_tr_protected_hg_promote_view_body_rejected` (M)
- `si_tr_protected_hg_promote_describe_rejected` (M)
- `si_tr_protected_phys_from_v2_rejected` (M)
- `si_tr_protected_phys_join_v2_rejected` (M)
- `si_tr_protected_phys_subquery_v2_rejected` (M)
- `si_tr_protected_phys_cte_v2_rejected` (M)
- `si_tr_protected_phys_in_v2_rejected` (M)
- `si_tr_protected_phys_in_paren_v2_rejected` (M)
- `si_tr_protected_phys_in_tuple_v2_rejected` (P)
- `si_tr_protected_phys_in_call_v2_rejected` (M)
- `si_tr_protected_phys_global_in_v2_rejected` (M)
- `si_tr_protected_phys_insert_select_source_v2_rejected` (M)
- `si_tr_protected_phys_ctas_source_v2_rejected` (M)
- `si_tr_protected_phys_view_body_v2_rejected` (M)
- `si_tr_protected_phys_describe_v2_rejected` (M)
- `si_tr_phys_active_ordinary_in_v2_rejected` (M)
- `si_tr_phys_active_ordinary_insert_select_source_v2_rejected` (M)
- `si_tr_phys_active_ordinary_ctas_source_v2_rejected` (M)
- `si_tr_in_nested_paren_phys_rejected` (M)
- `si_tr_in_nested_paren_phys_v2_rejected` (M)
- `si_tr_in_nested_paren_hg_safe_rejected` (M)
- `si_tr_in_call_nested_paren_phys_rejected` (M)
- `si_tr_in_call_nested_paren_phys_v2_rejected` (M)
- `si_tr_alter_update_hg_promote_subquery_rejected` (M)
- `si_tr_alter_delete_in_phys_rejected` (M)
- `si_tr_alter_delete_in_phys_v2_rejected` (M)
- `si_tr_alter_delete_in_phys_subquery_rejected` (M)
- `si_tr_alter_delete_in_phys_subquery_v2_rejected` (M)
- `si_tr_alter_delete_in_hg_safe_rejected` (M)
- `si_tr_alter_multi_delete_in_phys_rejected` (M)
- `si_tr_alter_multi_delete_in_phys_v2_rejected` (M)
- `si_tr_create_default_hg_promote_subquery_rejected` (M)
- `si_tr_create_default_hg_safe_subquery_rejected` (M)
- `si_tr_create_default_phys_subquery_rejected` (M)
- `si_tr_create_default_phys_subquery_v2_rejected` (M)
- `si_tr_alter_modify_column_hg_unsafe_subquery_rejected` (M)
- `si_tr_alter_modify_query_hg_safe_rejected` (M)
- `si_tr_alter_modify_query_phys_rejected` (M)
- `si_tr_alter_modify_query_phys_v2_rejected` (M)
- `si_tr_show_columns_where_phys_subquery_rejected` (M)
- `si_tr_show_columns_where_phys_subquery_v2_rejected` (P)
- `si_tr_show_columns_where_hg_safe_subquery_rejected` (M)
- `si_tr_r2_insert_header_opaque_phys_rejected` (M)
- `si_tr_r2_insert_header_opaque_phys_v2_rejected` (M)
- `si_tr_r3_insert_opaque_hg_safe_rejected` (M)
- `si_tr_r3_insert_opaque_hg_unsafe_rejected` (M)
- `si_tr_r3_insert_opaque_hg_promote_rejected` (M)
- `si_tr_r3_show_body_hg_safe_rejected` (M)
- `si_tr_r3_show_full_body_hg_unsafe_rejected` (M)
- `si_tr_r3_alter_modify_column_hg_promote_rejected` (M)
- `si_tr_r5_alter_delete_plus_phys_in_rejected` (M)
- `si_tr_r5_alter_delete_plus_phys_in_v2_rejected` (M)

</details>

<details><summary>Task 6 — 62 cases</summary>

- `si_tr_protected_phys_mv_to_rejected` (M)
- `si_tr_protected_phys_insert_target_rejected` (M)
- `si_tr_protected_phys_drop_rejected` (M)
- `si_tr_protected_phys_create_target_rejected` (M)
- `si_tr_protected_phys_create_as_rejected` (M)
- `si_tr_protected_phys_rename_rejected` (M)
- `si_tr_protected_phys_exists_rejected` (M)
- `si_tr_protected_phys_show_create_rejected` (M)
- `si_tr_protected_phys_show_tables_from_rejected` (M)
- `si_tr_protected_phys_use_rejected` (M)
- `si_tr_protected_phys_create_database_rejected` (M)
- `si_tr_protected_phys_drop_database_rejected` (M)
- `si_tr_protected_hg_safe_mv_to_rejected` (M)
- `si_tr_protected_hg_safe_insert_target_rejected` (M)
- `si_tr_protected_hg_safe_drop_rejected` (M)
- `si_tr_protected_hg_safe_create_target_rejected` (M)
- `si_tr_protected_hg_safe_create_as_rejected` (M)
- `si_tr_protected_hg_safe_rename_rejected` (M)
- `si_tr_protected_hg_safe_exists_rejected` (M)
- `si_tr_protected_hg_safe_show_create_rejected` (M)
- `si_tr_protected_hg_safe_show_tables_from_rejected` (M)
- `si_tr_protected_hg_safe_use_rejected` (M)
- `si_tr_protected_hg_safe_create_database_rejected` (M)
- `si_tr_protected_hg_safe_drop_database_rejected` (M)
- `si_tr_protected_hg_unsafe_mv_to_rejected` (M)
- `si_tr_protected_hg_unsafe_insert_target_rejected` (M)
- `si_tr_protected_hg_unsafe_drop_rejected` (M)
- `si_tr_protected_hg_unsafe_create_target_rejected` (M)
- `si_tr_protected_hg_unsafe_create_as_rejected` (M)
- `si_tr_protected_hg_unsafe_rename_rejected` (M)
- `si_tr_protected_hg_unsafe_exists_rejected` (M)
- `si_tr_protected_hg_unsafe_show_create_rejected` (M)
- `si_tr_protected_hg_unsafe_show_tables_from_rejected` (M)
- `si_tr_protected_hg_unsafe_use_rejected` (M)
- `si_tr_protected_hg_unsafe_create_database_rejected` (M)
- `si_tr_protected_hg_unsafe_drop_database_rejected` (M)
- `si_tr_protected_hg_promote_mv_to_rejected` (M)
- `si_tr_protected_hg_promote_insert_target_rejected` (M)
- `si_tr_protected_hg_promote_drop_rejected` (M)
- `si_tr_protected_hg_promote_create_target_rejected` (M)
- `si_tr_protected_hg_promote_create_as_rejected` (M)
- `si_tr_protected_hg_promote_rename_rejected` (M)
- `si_tr_protected_hg_promote_exists_rejected` (M)
- `si_tr_protected_hg_promote_show_create_rejected` (M)
- `si_tr_protected_hg_promote_show_tables_from_rejected` (M)
- `si_tr_protected_hg_promote_use_rejected` (M)
- `si_tr_protected_hg_promote_create_database_rejected` (M)
- `si_tr_protected_hg_promote_drop_database_rejected` (M)
- `si_tr_protected_phys_mv_to_v2_rejected` (M)
- `si_tr_protected_phys_insert_target_v2_rejected` (M)
- `si_tr_protected_phys_drop_v2_rejected` (M)
- `si_tr_protected_phys_create_target_v2_rejected` (M)
- `si_tr_protected_phys_create_as_v2_rejected` (M)
- `si_tr_protected_phys_rename_v2_rejected` (M)
- `si_tr_protected_phys_exists_v2_rejected` (M)
- `si_tr_protected_phys_show_create_v2_rejected` (M)
- `si_tr_protected_phys_show_tables_from_v2_rejected` (M)
- `si_tr_protected_phys_use_v2_rejected` (M)
- `si_tr_protected_phys_create_database_v2_rejected` (M)
- `si_tr_protected_phys_drop_database_v2_rejected` (M)
- `si_tr_alter_replace_partition_phys_rejected` (M)
- `si_tr_alter_replace_partition_phys_v2_rejected` (M)

</details>

<details><summary>Task 7 — 23 cases</summary>

- `si_tr_protected_phys_merge_arg_rejected` (M)
- `si_tr_protected_phys_remote_arg_rejected` (M)
- `si_tr_protected_phys_engine_merge_arg_rejected` (M)
- `si_tr_protected_phys_joinget_arg_rejected` (M)
- `si_tr_protected_hg_safe_merge_arg_rejected` (M)
- `si_tr_protected_hg_safe_remote_arg_rejected` (M)
- `si_tr_protected_hg_safe_engine_merge_arg_rejected` (M)
- `si_tr_protected_hg_safe_joinget_arg_rejected` (M)
- `si_tr_protected_hg_unsafe_merge_arg_rejected` (M)
- `si_tr_protected_hg_unsafe_remote_arg_rejected` (M)
- `si_tr_protected_hg_unsafe_engine_merge_arg_rejected` (M)
- `si_tr_protected_hg_unsafe_joinget_arg_rejected` (M)
- `si_tr_protected_hg_promote_merge_arg_rejected` (M)
- `si_tr_protected_hg_promote_remote_arg_rejected` (M)
- `si_tr_protected_hg_promote_engine_merge_arg_rejected` (M)
- `si_tr_protected_hg_promote_joinget_arg_rejected` (M)
- `si_tr_protected_phys_merge_arg_v2_rejected` (M)
- `si_tr_protected_phys_remote_arg_v2_rejected` (M)
- `si_tr_protected_phys_engine_merge_arg_v2_rejected` (M)
- `si_tr_protected_phys_joinget_arg_v2_rejected` (M)
- `si_tr_joinget_phys_rejected` (M)
- `si_tr_dictget_phys_rejected` (M)
- `si_tr_hascolumnintable_phys_rejected` (M)

</details>

<details><summary>Task 8 — 54 cases</summary>

- `si_comma_si_with_offset_rejected` (M)
- `si_in_logical_table_rewritten` (M)
- `si_global_in_physical_table_rejected` (M)
- `si_not_in_physical_table_rejected` (M)
- `si_global_not_in_physical_table_rejected` (M)
- `si_callable_in_physical_table_rejected` (M)
- `si_callable_not_in_physical_table_rejected` (M)
- `si_callable_global_in_physical_table_rejected` (M)
- `si_callable_global_not_in_physical_table_rejected` (M)
- `si_callable_null_in_physical_table_rejected` (M)
- `si_callable_not_null_in_physical_table_rejected` (M)
- `si_callable_global_null_in_physical_table_rejected` (M)
- `si_callable_global_not_null_in_physical_table_rejected` (M)
- `si_callable_in_ignore_set_physical_table_rejected` (M)
- `si_callable_not_in_ignore_set_physical_table_rejected` (M)
- `si_callable_global_in_ignore_set_physical_table_rejected` (M)
- `si_callable_global_not_in_ignore_set_physical_table_rejected` (M)
- `si_callable_null_in_ignore_set_physical_table_rejected` (M)
- `si_callable_not_null_in_ignore_set_physical_table_rejected` (M)
- `si_callable_global_null_in_ignore_set_physical_table_rejected` (M)
- `si_callable_global_not_null_in_ignore_set_physical_table_rejected` (M)
- `si_ordinary_in_table_allowed` (M)
- `si_tr_in_own_rewritten` (M)
- `si_tr_in_paren_own_rewritten` (M)
- `si_tr_in_tuple_own_rewritten` (M)
- `si_tr_in_call_own_rewritten` (M)
- `si_tr_global_in_own_rewritten` (M)
- `si_tr_in_active_derived_read` (M)
- `si_tr_in_active_unsafe_latest_derived_read` (M)
- `si_tr_in_system_reported` (M)
- `si_tr_in_unqualified_rewritten` (M)
- `si_tr_in_own_table_active_surface_rewritten` (M)
- `si_tr_in_call_active_derived_read` (M)
- `si_tr_in_unqualified_active_derived_read` (M)
- `si_tr_in_unqualified_paren_active_derived_read` (M)
- `si_tr_in_unqualified_call_active_derived_read` (M)
- `si_tr_in_nested_paren_dotted_rewritten` (M)
- `si_tr_in_nested_paren_dotted_v2_rewritten` (M)
- `si_tr_in_dotted_rewritten` (M)
- `si_tr_in_dotted_v2_rewritten` (M)
- `si_tr_in_paren_dotted_rewritten` (M)
- `si_tr_in_paren_dotted_v2_rewritten` (M)
- `si_tr_in_call_dotted_rewritten` (M)
- `si_tr_in_call_dotted_v2_rewritten` (M)
- `si_tr_not_in_dotted_rewritten` (M)
- `si_tr_not_in_dotted_v2_rewritten` (M)
- `si_tr_global_in_dotted_rewritten` (M)
- `si_tr_global_in_dotted_v2_rewritten` (M)
- `si_tr_nullin_dotted_rewritten` (M)
- `si_tr_nullin_dotted_v2_rewritten` (M)
- `si_tr_tuple_in_dotted_rewritten` (M)
- `si_tr_tuple_in_dotted_v2_rewritten` (P)
- `si_tr_in_bare_identifier_rewritten` (M)
- `si_tr_in_bare_identifier_v2_rewritten` (M)

</details>

<details><summary>Task 9 — 19 cases</summary>

- `si_ctas_logical_source_rewritten` (M)
- `si_ctas_physical_source_rejected` (M)
- `si_insert_select_logical_source_rewritten` (M)
- `si_insert_select_physical_source_rejected` (M)
- `si_tr_insert_select_own_source_rewritten` (M)
- `si_tr_insert_select_unqualified_source_rewritten` (M)
- `si_tr_insert_select_active_source_derived_read` (M)
- `si_tr_insert_select_active_target_unchanged` (M)
- `si_tr_ctas_own_source_rewritten` (M)
- `si_tr_ctas_empty_own_source_rewritten` (G)
- `si_tr_ctas_empty_active_source_derived_read` (new)
- `si_tr_ctas_active_source_derived_read` (M)
- `si_tr_ctas_other_tenant_source_lenient` (M)
- `si_tr_insert_select_nested_in_rewritten` (M)
- `si_tr_insert_select_own_source_paren_rewritten` (M)
- `si_tr_ctas_own_source_paren_rewritten` (M)
- `si_tr_insert_select_active_source_paren_derived_read` (M)
- `si_tr_ctas_active_source_paren_derived_read` (M)
- `si_tr_tf_view_body_rewritten` (M)

</details>

<details><summary>Task 10 — 2 cases</summary>

- `si_tr_mv_to_active_rejected` (M)
- `si_tr_mv_append_to_active_rejected` (M)

</details>

<details><summary>Task 11 — 23 cases</summary>

- `si_ordinary_local_catalog_function_rejected` (M)
- `si_mysql_ordinary_database_rejected` (M)
- `si_tr_tf_merge_rejected` (M)
- `si_tr_tf_remote_rejected` (M)
- `si_tr_tf_remoteSecure_rejected` (M)
- `si_tr_tf_cluster_rejected` (M)
- `si_tr_tf_clusterAllReplicas_rejected` (M)
- `si_tr_tf_loop_rejected` (M)
- `si_tr_tf_dictionary_rejected` (M)
- `si_tr_tf_mergeTreeIndex_rejected` (M)
- `si_tr_tf_mergeTreeProjection_rejected` (M)
- `si_tr_tf_timeSeriesData_rejected` (M)
- `si_tr_tf_prometheusQuery_rejected` (M)
- `si_tr_tf_clickhouse_rejected` (M)
- `si_tr_tf_mysql_rejected` (M)
- `si_tr_tf_postgresql_rejected` (M)
- `si_tr_tf_mongodb_rejected` (M)
- `si_tr_tf_jdbc_rejected` (M)
- `si_tr_tf_odbc_rejected` (M)
- `si_tr_tf_executable_rejected` (M)
- `si_tr_tf_fuzzQuery_rejected` (M)
- `si_tr_tf_unknown_rejected` (M)
- `si_tr_tf_insert_function_remote_rejected` (M)

</details>

<details><summary>Task 12 — 32 cases</summary>

- `si_ordinary_remote_engine_rejected` (M)
- `si_tr_engine_Merge_rejected` (M)
- `si_tr_engine_Buffer_rejected` (M)
- `si_tr_engine_Distributed_rejected` (M)
- `si_tr_engine_URL_rejected` (M)
- `si_tr_engine_Dictionary_rejected` (M)
- `si_tr_engine_KeeperMap_rejected` (M)
- `si_tr_engine_EmbeddedRocksDB_rejected` (M)
- `si_tr_engine_Kafka_rejected` (M)
- `si_tr_engine_S3_rejected` (M)
- `si_tr_engine_File_rejected` (M)
- `si_tr_engine_replicated_with_args_rejected` (M)
- `si_tr_engine_storage_policy_rejected` (M)
- `si_tr_engine_disk_rejected` (M)
- `si_tr_setting_alter_modify_disk_rejected` (M)
- `si_tr_engine_unknown_rejected` (M)
- `si_tr_mv_engine_merge_rejected` (M)
- `si_tr_mv_engine_merge_v2_rejected` (M)
- `si_tr_mv_engine_buffer_rejected` (M)
- `si_tr_mv_engine_buffer_v2_rejected` (M)
- `si_tr_mv_engine_distributed_rejected` (M)
- `si_tr_mv_engine_distributed_v2_rejected` (M)
- `si_tr_mv_engine_kafka_rejected` (M)
- `si_tr_mv_engine_kafka_v2_rejected` (M)
- `si_tr_mv_setting_disk_rejected` (M)
- `si_tr_mv_setting_disk_v2_rejected` (M)
- `si_tr_mv_setting_storage_policy_rejected` (M)
- `si_tr_mv_setting_storage_policy_v2_rejected` (M)
- `si_tr_mv_engine_merge_regexp_rejected` (M)
- `si_tr_mv_engine_merge_regexp_v2_rejected` (M)
- `si_tr_engine_lowercase_memory_rejected` (M)
- `si_tr_engine_lowercase_memory_v2_rejected` (M)

</details>

<details><summary>Task 13 — 13 cases</summary>

- `si_tr_joinget_mapped_rejected` (M)
- `si_tr_joinget_unqualified_rejected` (M)
- `si_tr_joinget_non_literal_rejected` (M)
- `si_tr_dictget_mapped_rejected` (M)
- `si_tr_hascolumnintable_mapped_rewritten` (M)
- `si_tr_hascolumnintable_outside_select_rejected` (M)
- `si_tr_lookup_quoted_name_rejected` (M)
- `si_tr_joinget_active_rejected` (M)
- `si_tr_lookup_non_literal_select_rejected` (M)
- `si_tr_lookup_non_literal_select_v2_rejected` (M)
- `si_tr_lookup_non_literal_alter_rejected` (M)
- `si_tr_lookup_non_literal_alter_v2_rejected` (M)
- `si_tr_hascolumnintable_accessed_once` (M)

</details>

<details><summary>Task 14 — 40 cases</summary>

- `si_tr_settings_additional_table_filters_rejected` (M)
- `si_tr_settings_additional_table_filters_v2_rejected` (M)
- `si_tr_settings_additional_result_filter_rejected` (M)
- `si_tr_settings_additional_result_filter_v2_rejected` (M)
- `si_tr_settings_parallel_replicas_custom_key_rejected` (M)
- `si_tr_settings_parallel_replicas_custom_key_v2_rejected` (M)
- `si_tr_settings_map_value_rejected` (new)
- `si_tr_settings_map_value_v2_rejected` (new)
- `si_tr_insert_settings_additional_table_filters_rejected` (M)
- `si_tr_insert_settings_additional_table_filters_v2_rejected` (M)
- `si_tr_set_additional_table_filters_rejected` (M)
- `si_tr_set_map_value_rejected` (new)
- `si_tr_set_dialect_rejected` (M)
- `si_tr_settings_dialect_rejected` (M)
- `si_tr_settings_dialect_v2_rejected` (M)
- `si_tr_set_polyglot_dialect_rejected` (M)
- `si_tr_settings_polyglot_dialect_rejected` (M)
- `si_tr_settings_polyglot_dialect_v2_rejected` (M)
- `si_tr_set_allow_experimental_polyglot_dialect_rejected` (M)
- `si_tr_settings_allow_experimental_polyglot_dialect_rejected` (M)
- `si_tr_settings_allow_experimental_polyglot_dialect_v2_rejected` (M)
- `si_tr_set_allow_experimental_prql_dialect_rejected` (M)
- `si_tr_settings_allow_experimental_prql_dialect_rejected` (M)
- `si_tr_settings_allow_experimental_prql_dialect_v2_rejected` (M)
- `si_tr_set_allow_experimental_kusto_dialect_rejected` (M)
- `si_tr_settings_allow_experimental_kusto_dialect_rejected` (M)
- `si_tr_settings_allow_experimental_kusto_dialect_v2_rejected` (M)
- `si_tr_set_polyglot_dialect_sqlite_rejected` (M)
- `si_tr_r2_insert_header_additional_table_filters_rejected` (M)
- `si_tr_r2_insert_header_additional_table_filters_v2_rejected` (M)
- `si_tr_r2_insert_header_additional_result_filter_select_rejected` (M)
- `si_tr_r2_insert_header_additional_result_filter_select_v2_rejected` (M)
- `si_tr_r2_insert_header_additional_result_filter_values_rejected` (M)
- `si_tr_r2_insert_header_additional_result_filter_values_v2_rejected` (M)
- `si_tr_r2_insert_header_dialect_rejected` (M)
- `si_tr_r2_insert_header_dialect_v2_rejected` (M)
- `si_tr_r2_insert_header_polyglot_dialect_rejected` (M)
- `si_tr_r2_insert_header_polyglot_dialect_v2_rejected` (M)
- `si_tr_r2_insert_header_quoted_dialect_rejected` (M)
- `si_tr_r2_insert_header_quoted_dialect_v2_rejected` (M)

</details>

<details><summary>Task 16 — 122 cases</summary>

- `si_tr_alter_update_remote_subquery_rejected` (M)
- `si_tr_alter_update_remote_subquery_v2_rejected` (M)
- `si_tr_alter_update_merge_subquery_rejected` (M)
- `si_tr_alter_update_merge_subquery_v2_rejected` (M)
- `si_tr_alter_update_dotted_subquery_rejected` (M)
- `si_tr_alter_update_dotted_subquery_v2_rejected` (M)
- `si_tr_alter_delete_in_merge_subquery_rejected` (M)
- `si_tr_alter_delete_in_merge_subquery_v2_rejected` (M)
- `si_tr_alter_delete_in_dotted_rejected` (M)
- `si_tr_alter_delete_in_dotted_v2_rejected` (M)
- `si_tr_alter_multi_delete_in_dotted_rejected` (M)
- `si_tr_alter_multi_delete_in_dotted_v2_rejected` (M)
- `si_tr_alter_modify_ttl_in_dotted_rejected` (M)
- `si_tr_alter_modify_ttl_in_dotted_v2_rejected` (M)
- `si_tr_alter_add_column_in_dotted_rejected` (M)
- `si_tr_alter_add_column_in_dotted_v2_rejected` (M)
- `si_tr_alter_fetch_partition_rejected` (M)
- `si_tr_alter_fetch_partition_v2_rejected` (M)
- `si_tr_alter_attach_partition_from_rejected` (M)
- `si_tr_alter_attach_partition_from_v2_rejected` (M)
- `si_tr_alter_move_partition_to_table_rejected` (M)
- `si_tr_alter_move_partition_to_table_v2_rejected` (M)
- `si_tr_alter_modify_query_dotted_rejected` (M)
- `si_tr_alter_modify_query_dotted_v2_rejected` (M)
- `si_tr_quoted_in_alter_delete_rejected` (M)
- `si_tr_quoted_in_alter_delete_v2_rejected` (M)
- `si_tr_quoted_notin_alter_delete_rejected` (M)
- `si_tr_quoted_notin_alter_delete_v2_rejected` (M)
- `si_tr_quoted_globalnotin_alter_delete_rejected` (M)
- `si_tr_quoted_globalnotin_alter_delete_v2_rejected` (M)
- `si_tr_quoted_nullin_alter_delete_rejected` (M)
- `si_tr_quoted_nullin_alter_delete_v2_rejected` (M)
- `si_tr_quoted_in_alter_update_rejected` (M)
- `si_tr_quoted_in_alter_update_v2_rejected` (M)
- `si_tr_quoted_in_modify_ttl_rejected` (M)
- `si_tr_quoted_in_modify_ttl_v2_rejected` (M)
- `si_tr_quoted_in_modify_column_default_rejected` (M)
- `si_tr_quoted_in_modify_column_default_v2_rejected` (M)
- `si_tr_quoted_notin_modify_column_materialized_rejected` (M)
- `si_tr_quoted_notin_modify_column_materialized_v2_rejected` (M)
- `si_tr_quoted_in_after_projection_rejected` (M)
- `si_tr_quoted_in_after_projection_v2_rejected` (M)
- `si_tr_r2_mv_quoted_refresh_name_refresh_every_rejected` (M)
- `si_tr_r2_mv_quoted_refresh_name_refresh_every_v2_rejected` (M)
- `si_tr_r3_alter_update_keyword_in_rejected` (M)
- `si_tr_r3_alter_update_keyword_in_v2_rejected` (M)
- `si_tr_r3_alter_delete_keyword_in_rejected` (M)
- `si_tr_r3_alter_delete_keyword_in_v2_rejected` (M)
- `si_tr_r3_alter_delete_keyword_in_comment_parens_rejected` (M)
- `si_tr_r3_alter_delete_keyword_in_comment_parens_v2_rejected` (M)
- `si_tr_r3_alter_modify_ttl_keyword_in_rejected` (M)
- `si_tr_r3_alter_modify_ttl_keyword_in_v2_rejected` (M)
- `si_tr_r3_alter_modify_column_keyword_in_rejected` (M)
- `si_tr_r3_alter_modify_column_keyword_in_v2_rejected` (M)
- `si_tr_r4_alter_update_where_date_in_rejected` (M)
- `si_tr_r4_alter_update_where_date_in_v2_rejected` (M)
- `si_tr_r4_alter_update_multi_where_key_in_rejected` (M)
- `si_tr_r4_alter_update_multi_where_key_in_v2_rejected` (M)
- `si_tr_r4_alter_update_assignment_date_in_rejected` (M)
- `si_tr_r4_alter_update_assignment_date_in_v2_rejected` (M)
- `si_tr_r4_alter_delete_date_in_rejected` (M)
- `si_tr_r4_alter_delete_date_in_v2_rejected` (M)
- `si_tr_r4_alter_delete_key_in_rejected` (M)
- `si_tr_r4_alter_delete_key_in_v2_rejected` (M)
- `si_tr_r4_alter_delete_timestamp_in_rejected` (M)
- `si_tr_r4_alter_delete_timestamp_in_v2_rejected` (M)
- `si_tr_r4_alter_delete_case_end_in_rejected` (M)
- `si_tr_r4_alter_delete_case_end_in_v2_rejected` (M)
- `si_tr_r4_alter_multi_command_key_in_rejected` (M)
- `si_tr_r4_alter_multi_command_key_in_v2_rejected` (M)
- `si_tr_r4_alter_modify_ttl_date_in_rejected` (M)
- `si_tr_r4_alter_modify_ttl_date_in_v2_rejected` (M)
- `si_tr_r4_alter_modify_column_default_date_in_rejected` (M)
- `si_tr_r4_alter_modify_column_default_date_in_v2_rejected` (M)
- `si_tr_r4_alter_add_projection_date_in_rejected` (G)
- `si_tr_r4_alter_add_projection_date_in_v2_rejected` (G)
- `si_tr_r4_alter_delete_not_callable_in_rejected` (M)
- `si_tr_r4_alter_delete_not_callable_in_v2_rejected` (M)
- `si_tr_r4_alter_delete_upper_not_callable_in_rejected` (M)
- `si_tr_r4_alter_delete_upper_not_callable_in_v2_rejected` (M)
- `si_tr_r4_alter_delete_paren_not_callable_in_rejected` (M)
- `si_tr_r4_alter_delete_paren_not_callable_in_v2_rejected` (M)
- `si_tr_r4_alter_delete_global_callable_in_rejected` (M)
- `si_tr_r4_alter_delete_global_callable_in_v2_rejected` (M)
- `si_tr_r4_alter_update_where_not_callable_in_rejected` (M)
- `si_tr_r4_alter_update_where_not_callable_in_v2_rejected` (M)
- `si_tr_r4_alter_modify_ttl_not_callable_in_rejected` (M)
- `si_tr_r4_alter_modify_ttl_not_callable_in_v2_rejected` (M)
- `si_tr_r4_alter_modify_column_materialized_not_callable_in_rejected` (M)
- `si_tr_r4_alter_modify_column_materialized_not_callable_in_v2_rejected` (M)
- `si_tr_r4_alter_add_projection_not_callable_in_rejected` (G)
- `si_tr_r4_alter_add_projection_not_callable_in_v2_rejected` (G)
- `si_tr_r4_alter_delete_callable_in_column_region_rejected` (M)
- `si_tr_r4_alter_delete_callable_in_column_region_v2_rejected` (M)
- `si_tr_r4_alter_delete_in_column_after_literal_rejected` (M)
- `si_tr_r4_alter_delete_in_column_after_literal_v2_rejected` (M)
- `si_tr_r4_alter_delete_in_column_before_literal_rejected` (M)
- `si_tr_r4_alter_delete_in_column_before_literal_v2_rejected` (M)
- `si_tr_r5_alter_delete_plus_quoted_in_rejected` (M)
- `si_tr_r5_alter_delete_plus_quoted_in_v2_rejected` (M)
- `si_tr_r5_alter_delete_plus_paren_in_rejected` (M)
- `si_tr_r5_alter_delete_plus_paren_in_v2_rejected` (M)
- `si_tr_r5_alter_delete_plus_plus_in_rejected` (M)
- `si_tr_r5_alter_delete_plus_plus_in_v2_rejected` (M)
- `si_tr_r5_alter_delete_not_in_plus_rejected` (M)
- `si_tr_r5_alter_delete_not_in_plus_v2_rejected` (M)
- `si_tr_r5_alter_delete_minus_quoted_in_rejected` (M)
- `si_tr_r5_alter_delete_minus_quoted_in_v2_rejected` (M)
- `si_tr_r5_alter_update_where_plus_in_rejected` (M)
- `si_tr_r5_alter_update_where_plus_in_v2_rejected` (M)
- `si_tr_r5_alter_update_assignment_plus_in_rejected` (M)
- `si_tr_r5_alter_update_assignment_plus_in_v2_rejected` (M)
- `si_tr_r5_alter_modify_ttl_plus_in_rejected` (M)
- `si_tr_r5_alter_modify_ttl_plus_in_v2_rejected` (M)
- `si_tr_r5_alter_modify_column_default_plus_in_rejected` (M)
- `si_tr_r5_alter_modify_column_default_plus_in_v2_rejected` (M)
- `si_tr_r5_alter_modify_column_materialized_plus_in_rejected` (M)
- `si_tr_r5_alter_modify_column_materialized_plus_in_v2_rejected` (M)
- `si_tr_r5_alter_add_projection_plus_in_rejected` (G)
- `si_tr_r5_alter_add_projection_plus_in_v2_rejected` (G)
- `si_tr_r5_alter_delete_array_then_update_key_in_rejected` (M)
- `si_tr_r5_alter_delete_array_then_update_key_in_v2_rejected` (M)

</details>

<details><summary>Task 17 — 12 cases</summary>

- `si_tr_delete_in_dotted_subquery_rejected` (M)
- `si_tr_delete_in_dotted_subquery_v2_rejected` (M)
- `si_tr_update_in_dotted_rejected` (M)
- `si_tr_update_in_dotted_v2_rejected` (M)
- `si_tr_create_materialized_in_dotted_rejected` (M)
- `si_tr_create_materialized_in_dotted_v2_rejected` (M)
- `si_tr_mv_refresh_rejected` (M)
- `si_tr_mv_refresh_v2_rejected` (M)
- `si_tr_r2_mv_refresh_every_rejected` (M)
- `si_tr_r2_mv_refresh_every_v2_rejected` (M)
- `si_tr_r2_mv_refresh_after_rejected` (M)
- `si_tr_r2_mv_refresh_after_v2_rejected` (M)

</details>

<details><summary>Task 18 — 30 cases</summary>

- `si_tr_insert_values_subquery_rejected` (M)
- `si_tr_insert_values_subquery_v2_rejected` (M)
- `si_tr_insert_from_infile_rejected` (M)
- `si_tr_insert_from_infile_v2_rejected` (M)
- `si_tr_r2_insert_header_opaque_read_rejected` (M)
- `si_tr_r2_insert_header_opaque_read_v2_rejected` (M)
- `si_tr_r3_insert_opaque_keyword_in_rejected` (M)
- `si_tr_r3_insert_opaque_keyword_in_v2_rejected` (M)
- `si_tr_r3_insert_opaque_upper_keyword_in_rejected` (M)
- `si_tr_r3_insert_opaque_upper_keyword_in_v2_rejected` (M)
- `si_tr_r3_insert_opaque_values_keyword_in_rejected` (M)
- `si_tr_r3_insert_opaque_values_keyword_in_v2_rejected` (M)
- `si_tr_r3_insert_opaque_format_values_keyword_in_rejected` (M)
- `si_tr_r3_insert_opaque_format_values_keyword_in_v2_rejected` (M)
- `si_tr_r4_insert_opaque_select_case_end_in_rejected` (M)
- `si_tr_r4_insert_opaque_select_case_end_in_v2_rejected` (M)
- `si_tr_r4_insert_opaque_values_case_end_in_rejected` (M)
- `si_tr_r4_insert_opaque_values_case_end_in_v2_rejected` (M)
- `si_tr_r4_insert_opaque_select_not_callable_in_rejected` (M)
- `si_tr_r4_insert_opaque_select_not_callable_in_v2_rejected` (M)
- `si_tr_r4_insert_opaque_values_not_callable_in_rejected` (M)
- `si_tr_r4_insert_opaque_values_not_callable_in_v2_rejected` (M)
- `si_tr_r4_insert_opaque_format_values_not_callable_in_rejected` (M)
- `si_tr_r4_insert_opaque_format_values_not_callable_in_v2_rejected` (M)
- `si_tr_r5_insert_opaque_select_plus_in_rejected` (M)
- `si_tr_r5_insert_opaque_select_plus_in_v2_rejected` (M)
- `si_tr_r5_insert_opaque_values_plus_in_rejected` (M)
- `si_tr_r5_insert_opaque_values_plus_in_v2_rejected` (M)
- `si_tr_r5_insert_opaque_format_values_plus_in_rejected` (M)
- `si_tr_r5_insert_opaque_format_values_plus_in_v2_rejected` (M)

</details>

<details><summary>Task 19 — 34 cases</summary>

- `si_tr_describe_dotted_rewritten` (M)
- `si_tr_describe_dotted_v2_rewritten` (M)
- `si_tr_show_columns_dotted_rewritten` (M)
- `si_tr_show_columns_where_remote_subquery_rejected` (M)
- `si_tr_show_columns_where_merge_subquery_rejected` (M)
- `si_tr_show_columns_where_dotted_subquery_rejected` (M)
- `si_tr_show_index_where_subquery_rejected` (M)
- `si_tr_show_columns_where_plain_rewritten` (M)
- `si_tr_r2_show_columns_limit_subquery_rejected` (M)
- `si_tr_r2_show_fields_limit_subquery_rejected` (M)
- `si_tr_r2_show_extended_full_columns_limit_subquery_rejected` (M)
- `si_tr_r2_show_columns_limit_expr_subquery_rejected` (M)
- `si_tr_r2_show_columns_from_db_limit_subquery_rejected` (M)
- `si_tr_r2_show_columns_other_db_limit_subquery_rejected` (M)
- `si_tr_r2_show_columns_other_db_limit_subquery_v2_rejected` (P)
- `si_tr_r2_show_columns_limit_call_rejected` (M)
- `si_tr_r2_show_dictionaries_where_subquery_rejected` (M)
- `si_tr_r2_show_dictionaries_from_default_where_subquery_rejected` (M)
- `si_tr_r2_show_dictionaries_from_default_where_subquery_v2_rejected` (P)
- `si_tr_r2_show_dictionaries_limit_subquery_rejected` (M)
- `si_tr_r2_show_full_dictionaries_limit_subquery_rejected` (M)
- `si_tr_r2_show_clusters_limit_subquery_rejected` (M)
- `si_tr_r2_show_clusters_limit_subquery_v2_rejected` (M)
- `si_tr_r2_show_clusters_like_limit_subquery_rejected` (M)
- `si_tr_r2_show_clusters_like_limit_subquery_v2_rejected` (M)
- `si_tr_r2_show_merges_limit_subquery_rejected` (M)
- `si_tr_r2_show_merges_limit_subquery_v2_rejected` (M)
- `si_tr_r2_show_merges_like_limit_subquery_rejected` (M)
- `si_tr_r2_show_merges_like_limit_subquery_v2_rejected` (M)
- `si_tr_r2_show_columns_limit_plain_rewritten` (M)
- `si_tr_r3_show_body_keyword_in_rejected` (M)
- `si_tr_r3_show_body_keyword_in_v2_rejected` (M)
- `si_tr_r3_show_body_hg_safe_v2_rejected` (M)
- `si_tr_r3_show_full_body_hg_unsafe_v2_rejected` (M)

</details>

Out of scope (6 P-contract): `si_create_live_view_over_si_rejected`, `si_drop_dictionary_safe_rejected`, `si_truncate_all_tables_from_unsafe_rejected`, `si_truncate_database_safe_rejected`, `si_v2_drop_dictionary_rejected`, `si_v2_reserved_truncate_database_rejected`.

## Appendix B: the interim list (`tests/testdata/si_cpp_sql_pending.txt`, 109 cases)

Task 1 creates it with exactly these names; Task 22 deletes it.

```text
si_in_logical_table_rewritten
si_ordinary_in_table_allowed
si_tr_alter_add_projection_allowed
si_tr_alter_add_projection_v2_allowed
si_tr_alter_update_literal_in_allowed
si_tr_alter_update_literal_in_v2_allowed
si_tr_ctas_active_source_derived_read
si_tr_ctas_active_source_paren_derived_read
si_tr_ctas_empty_active_source_derived_read
si_tr_ctas_empty_own_source_rewritten
si_tr_ctas_other_tenant_source_lenient
si_tr_ctas_own_source_paren_rewritten
si_tr_ctas_own_source_rewritten
si_tr_engine_memory_allowed
si_tr_engine_mergetree_allowed
si_tr_engine_replicated_bare_allowed
si_tr_global_in_dotted_rewritten
si_tr_global_in_dotted_v2_rewritten
si_tr_global_in_own_rewritten
si_tr_hascolumnintable_accessed_once
si_tr_in_active_derived_read
si_tr_in_active_unsafe_latest_derived_read
si_tr_in_bare_identifier_rewritten
si_tr_in_bare_identifier_v2_rewritten
si_tr_in_call_active_derived_read
si_tr_in_call_dotted_rewritten
si_tr_in_call_dotted_v2_rewritten
si_tr_in_call_own_rewritten
si_tr_in_cte_alias_untouched
si_tr_in_dotted_rewritten
si_tr_in_dotted_v2_rewritten
si_tr_in_literal_value_list_active_allowed
si_tr_in_literal_value_list_allowed
si_tr_in_nested_paren_dotted_rewritten
si_tr_in_nested_paren_dotted_v2_rewritten
si_tr_in_own_rewritten
si_tr_in_own_table_active_surface_rewritten
si_tr_in_paren_dotted_rewritten
si_tr_in_paren_dotted_v2_rewritten
si_tr_in_paren_own_rewritten
si_tr_in_system_reported
si_tr_in_tuple_own_rewritten
si_tr_in_unqualified_active_derived_read
si_tr_in_unqualified_call_active_derived_read
si_tr_in_unqualified_paren_active_derived_read
si_tr_in_unqualified_rewritten
si_tr_insert_select_active_source_paren_derived_read
si_tr_insert_select_active_target_unchanged
si_tr_insert_select_nested_in_rewritten
si_tr_insert_select_own_source_paren_rewritten
si_tr_insert_select_own_source_rewritten
si_tr_insert_select_unqualified_source_rewritten
si_tr_mv_named_refresh_rewritten
si_tr_mv_named_refresh_v2_rewritten
si_tr_mv_to_ordinary_rewritten
si_tr_mv_to_refresh_rewritten
si_tr_mv_to_refresh_v2_rewritten
si_tr_not_in_dotted_rewritten
si_tr_not_in_dotted_v2_rewritten
si_tr_nullin_dotted_rewritten
si_tr_nullin_dotted_v2_rewritten
si_tr_param_column_allowed
si_tr_param_value_allowed
si_tr_r2_show_columns_limit_plain_rewritten
si_tr_r2_view_refresh_column_rewritten
si_tr_r2_view_refresh_column_v2_rewritten
si_tr_r3_alter_delete_keyword_in_literal_list_allowed
si_tr_r3_alter_delete_keyword_in_literal_list_v2_allowed
si_tr_r4_alter_delete_callable_in_array_literal_allowed
si_tr_r4_alter_delete_callable_in_array_literal_v2_allowed
si_tr_r4_alter_delete_in_literal_list_allowed
si_tr_r4_alter_delete_in_literal_list_v2_allowed
si_tr_r4_alter_delete_not_in_string_list_allowed
si_tr_r4_alter_delete_not_in_string_list_v2_allowed
si_tr_r5_alter_add_column_default_in_array3_literal_allowed
si_tr_r5_alter_add_column_default_in_array3_literal_v2_allowed
si_tr_r5_alter_add_column_default_in_array_literal_allowed
si_tr_r5_alter_add_column_default_in_array_literal_v2_allowed
si_tr_r5_alter_add_projection_in_array3_literal_allowed
si_tr_r5_alter_add_projection_in_array3_literal_v2_allowed
si_tr_r5_alter_add_projection_in_array_literal_allowed
si_tr_r5_alter_add_projection_in_array_literal_v2_allowed
si_tr_r5_alter_delete_in_array3_literal_allowed
si_tr_r5_alter_delete_in_array3_literal_v2_allowed
si_tr_r5_alter_delete_in_array_literal_allowed
si_tr_r5_alter_delete_in_array_literal_v2_allowed
si_tr_r5_alter_delete_in_minus_literal_allowed
si_tr_r5_alter_delete_in_minus_literal_v2_allowed
si_tr_r5_alter_delete_in_plus_literal_allowed
si_tr_r5_alter_delete_in_signed_literal_list_allowed
si_tr_r5_alter_modify_column_default_in_array3_literal_allowed
si_tr_r5_alter_modify_column_default_in_array3_literal_v2_allowed
si_tr_r5_alter_modify_column_default_in_array_literal_allowed
si_tr_r5_alter_modify_column_default_in_array_literal_v2_allowed
si_tr_r5_alter_modify_column_materialized_in_array3_literal_allowed
si_tr_r5_alter_modify_column_materialized_in_array3_literal_v2_allowed
si_tr_r5_alter_modify_column_materialized_in_array_literal_allowed
si_tr_r5_alter_modify_column_materialized_in_array_literal_v2_allowed
si_tr_r5_alter_modify_ttl_in_array3_literal_allowed
si_tr_r5_alter_modify_ttl_in_array3_literal_v2_allowed
si_tr_r5_alter_modify_ttl_in_array_literal_allowed
si_tr_r5_alter_modify_ttl_in_array_literal_v2_allowed
si_tr_settings_plain_values_allowed
si_tr_settings_plain_values_v2_allowed
si_tr_show_columns_dotted_rewritten
si_tr_show_columns_where_plain_rewritten
si_tr_tf_view_body_rewritten
si_tr_tuple_in_dotted_rewritten
si_tr_tuple_in_dotted_v2_rewritten
```
