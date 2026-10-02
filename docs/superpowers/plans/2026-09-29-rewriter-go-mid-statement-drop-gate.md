# rewriter-go Mid-Statement Drop Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** rewriter-go refuses a statement that Polyglot parsed in full but regenerates as a different statement, instead of answering `Success` with SQL that means something else. This is the second rewriter-go v0.14.0 release gate. At minimum it refuses `DELETE … IN PARTITION`, which Polyglot widens to every partition.

**Architecture:** One engine helper, `engine.CheckRegenerated(e, sql, ast)`, compares the input with Polyglot's identity regeneration `Generate(ast)`. Both are tokenized by the engine and reduced to a multiset of spellings. Punctuation is dropped, words are upper-cased, and a closed table of measured cosmetic respellings is applied. The two multisets must be equal. `finalize` in `native.go` runs the helper on every response about to answer `Success`, whatever handler built it and in every mode, and turns a mismatch into T7 `UnsupportedStatement` / `statement is not supported`. `command` and `raw` nodes regenerate their own text verbatim, so the helper passes them. The handlers that re-render a command from parsed fields therefore check their own coverage: EXISTS / SHOW CREATE / DESCRIBE, USE, SHOW TABLES and SHOW DATABASES refuse any clause they would drop. `doMaterializeSQL` runs the helper before it regenerates a materialized statement. No shared corpus case changes.

**Tech Stack:** Go 1.25, Polyglot FFI v0.13.0 asset (`POLYGLOT_SQL_FFI_PATH`), the rewriter-go harness (`internal/harness`), `clickhouse local` 26.2 / 26.7.5.10 via docker (measurement only), `gh`.

**Spec:** `/Users/uranuswch/src/tableref-docs/2026-09-26-table-reference-hardening-design.md`. This is the housegate `docs/superpowers/specs/2026-09-26-table-reference-hardening-design.md`. §1 is the invariant: an engine must not produce SQL that means something other than the input. Read also §5 (the policy table, and "every engine-internal error … is a coded `UnsupportedStatement` / `statement is not supported` rejection"), §10.1 (the shared corpus admits only inputs both engines answer identically) and §11 (ask before merging and before any release). The parse gate's ruling T2-I1 applies unchanged: a shape the native engine cannot represent faithfully is an engine-internal limit, refused as T7, kept out of the shared corpus, and the C++ engine need not match its code or message.

**Origin:** The final review of the whole-statement parse gate (`.superpowers/sdd/2026-09-29-rewriter-go-whole-statement-parse-gate/final-review.md` in `/Users/uranuswch/Dev/housegate/rewriter-go.fix-whole-statement-parse-gate`, item I2) found statements that pass the parse gate and still answer `Success` for a different statement. Polyglot consumes every token but parses a clause into nothing, or its generator prints the clause differently. The user ruled (parse-gate ledger, 2026-09-29) that mid-statement drops are also a v0.14.0 release gate, and that `DELETE … IN PARTITION` must be refused at minimum. The view / materialized-view `AS (…) COMMENT '…'` shape is a documented native limit owned by a separate follow-up and is out of scope here.

**Repository and working copy:**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git fetch origin
# Base: the parse-gate branch head, or origin/main once the parse-gate PR has merged.
BASE=$(git rev-parse origin/fix/whole-statement-parse-gate 2>/dev/null || git -C ../rewriter-go.fix-whole-statement-parse-gate rev-parse HEAD)
git worktree add -b fix/mid-statement-drop-gate \
  /Users/uranuswch/Dev/housegate/rewriter-go.fix-mid-statement-drop-gate "$BASE"
cd /Users/uranuswch/Dev/housegate/rewriter-go.fix-mid-statement-drop-gate
git submodule update --init third_party/polyglot-src
export POLYGLOT_SQL_FFI_PATH=$HOME/Library/Caches/housegate/rewriter-ffi/v0.13.0/libpolyglot_sql_ffi.dylib
```

If the parse-gate PR has merged by the time this plan runs, use `origin/main` as `BASE` instead. Every path below is relative to this worktree. Every snippet, expected value and count in this plan was run in a throwaway worktree of the parse-gate head `d5355e3` (after its final-review fix wave), and of `e02a879` before that wave. Both CI lanes were green at every task boundary, and every new test was red before its implementation step.

## Global Constraints

- CI runs `go vet ./...` and `env SNAPSHOT_QUERY_ORDINARY=1 go test ./...`, with and without `POLYGLOT_SQL_FFI_PATH`. Both must pass at every commit. Without `SNAPSHOT_QUERY_ORDINARY=1`, the snapshot-query tests fail on `SNAPSHOT_MEASURED_FFI`. That is environment, not a regression.
- The shared corpus does not change. `internal/harness/testdata/storage_integrity_cases.json` stays sha256 `8bb63224934bf60794ebc3569c0b1c4b53370abdebb2c280e150423b0228abb7`, 864998 bytes, 960 cases, FNV-1a/64 `17952022305366168916`, and `internal/harness/sicorpus_test.go` is not edited. If the base shows a different hash, the parse gate changed after this plan was written: stop and re-measure.
- Do not run `UPDATE_GOLDEN`. Do not add `want_sql_cpp` / `allow_sql_divergence`. Do not add a shared case: rewriter-grpc parses every refused input with ClickHouse's own parser, keeps the clause and answers `Success`, so no refused input can be answered identically by both engines.
- The five message families are cross-engine contract texts, listed in rewriter-go `AGENTS.md` under "Message families". This plan adds no text to them. Every rewrite refusal reuses T7 `engine.UnsupportedStatementMessage`. The detailed error text of `CheckRegenerated` appears only in `MaterializeSQL` messages and Go errors; it is engine-local and never pinned cross-engine.
- `spellingClass` and the input respellings in `fidelitySpellings` are a closed, measured table. Add an entry only together with the ClickHouse evidence that both spellings are the same statement (`system.functions` / `system.data_type_families` `alias_to`, or an execution comparison on 26.2 and 26.7.5.10) and a row in `TestCheckRegenerated`.
- Do not reformat unrelated files. `gofmt -l .` already flags `internal/handlers/dblevel.go` (a curly-quote comment) on the base; leave it alone.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Markdown has no hard line-wrapping.
- Spec §11: ask before merging the PR and before any release.

## Review Focus

These are inputs the change could get wrong. Each line names the test that pins it.

1. **Cosmetic respellings must keep passing.** Polyglot prints `NULLS LAST` as nothing (ClickHouse's default in both directions, measured by execution), `TOP n` as `LIMIT n`, `LIMIT n, m` as `LIMIT m OFFSET n`, a comma join as `CROSS JOIN`, an implicit alias with `AS`, `INTERVAL 1 DAY` as `INTERVAL '1' DAY`, `FORMAT … SETTINGS` as `SETTINGS … FORMAT`, `pow` / `substr` / `ceiling` / `lcase` / `DATE_TRUNC` as their ClickHouse aliases, and `INT` / `TEXT` / `VARCHAR(n)` / `BOOLEAN` / `TIMESTAMP` column types as the types they alias. Task 1: the "Regenerated faithfully" rows of `TestCheckRegenerated`; Task 2: the passing rows of `TestMidStatementDropGate`.
2. **Only a would-be `Success` changes.** The gate runs in `finalize` after every handler and policy check, so no existing refusal changes its code or message. Measured: across 6,861 distinct inputs in three modes no answer other than `Success` changed, and the 960 shared cases are unchanged. Task 2 Step 6 and Task 6 Step 3.
3. **`INSERT … FORMAT` and the signed lane.** The payload after the format name is data, and `INSERT INTO db1.t (a, b) FORMAT Native` (the agent's signed lane and the clickhouse-go batch shape) must pass with the storage-integrity surface active. Task 1: rows "insert format payload" and "insert select format comment"; Task 2: the SI-active passing rows.
4. **Command handlers.** A `command` node always passes the general check, so each handler that re-renders a command owns its coverage. `SHOW DATABASES [NOT] (I)LIKE '…'` is modelled and still passes. `EXISTS` / `SHOW CREATE` re-render their target in every mode, not only under dynamic args. Task 3: `TestCommandRerenderGate`, `TestCommandRerenderGateEveryMode`.
5. **The storage-integrity surface.** A converted refusal goes through the same `finalize` annotation as every other refusal, so a statement over an SI table keeps the T7 text and the contract acknowledgement. Task 2 / Task 3: the `db1.t` rows.
6. **Driver and writer traffic.** The sentio platform writer (`sentio/common/clickhouse/writer.go`, `lightDeleteEvents` with the `clickhouseDeleteWithPartition` feature) emits `DELETE FROM … IN PARTITION '<chain>' WHERE …`, which is now refused. The sentio indexer driver's patch-part probe (`sentio-core/common/chx/query.go`, `applyPatchesIfPiledUp`) uses `startsWith`, which Polyglot already regenerated as `STARTS_WITH` (ClickHouse rejects it); it is now refused instead. Task 6 Step 5 checks devnet2 logs for both before release.

## Decisions this plan records

**Measured class.** Four input sets were run at the parse-gate head through `doRewrite` in the dynamic (both SI states) and no-rewrite modes, through `CheckParsedInFull`, and through Polyglot's identity `Generate`. ClickHouse 26.2 and 26.7.5.10 `formatQuerySingleLineOrNull` gave the input's validity; the two versions disagree on 2 of 515 clause inputs (`NOT LIKE … ESCAPE`, `DESCRIBE TEMPORARY TABLE`). "Semantic" means the regenerated statement means something else. The classification was confirmed with ClickHouse's canonical formatting of input and output and, where the formatter differs but the meaning might not, by execution (`NULLS LAST`, `toStartOfDay` vs `dateTrunc`, `CHAR_LENGTH` vs `LENGTH`, `instr` vs `position`, `locate`, `group_concat`, `trim`, `SHOW FULL` / `TEMPORARY TABLES`). "Invalid output" means ClickHouse rejects the regenerated SQL, so nothing silent happens there.

1. Clause families: 515 hand-built statements, every statement family × the clauses Polyglot might drop. Answer at the parse-gate head: `Success` unless noted.

| Family | Inputs | Semantic (input valid) | Semantic (input ClickHouse refuses) | Cosmetic, passes | Cosmetic, refused | Other |
|---|---|---|---|---|---|---|
| SELECT clauses | 97 | 6 — `WITH TIES` before `FORMAT` / `SETTINGS`, in a subquery, in an IN subquery; `LIMIT n BY … LIMIT m` ×2 | 0 | 8 | 0 | |
| Expressions | 118 | 2 — `a::String` → `a::Nullable(String)`; `TIMESTAMP '…'` → `CAST(… AS Nullable(DateTime))` | 0 | 8 | 1 — unary `+` | 1 invalid output refused (`toTypeName` → `TYPEOF`) |
| INSERT | 22 | 4 — `(* EXCEPT (b))` ×2, `(COLUMNS('^a'))` column lists dropped; `SELECT … LIMIT n BY … LIMIT m` | 0 | 2 | 1 — `(*)` | |
| DELETE / UPDATE | 14 | 4 — `IN PARTITION` dropped (×4, incl. with `ON CLUSTER`) | 0 | 0 | 0 | the active SI surface already refused the structured DELETE |
| ALTER | 64 | 1 — `ADD COLUMN … EPHEMERAL` | 0 | 0 | 0 | |
| CREATE TABLE | 46 | 3 — `EPHEMERAL`, column `STATISTICS(…)`, column `SETTINGS (…)` dropped | 0 | 2 | 0 | |
| CREATE VIEW / MV | 23 | 1 — body `LIMIT n BY … LIMIT m` | 0 | 1 | 0 | 1 out of scope: `COMMENT 'x' AS SELECT` → `COMMENT='x'` (ClickHouse refuses; the view-COMMENT follow-up) |
| DROP / TRUNCATE | 29 | 7 — `ON CLUSTER` on DROP TABLE ×3 / DROP VIEW / DROP DATABASE; `DROP TEMPORARY TABLE n` → `DROP TABLE phys."db1.n"`; `TRUNCATE … SETTINGS` | 0 | 0 | 0 | |
| CREATE DATABASE | 6 | 1 — `ON CLUSTER` dropped from the synthesized statement | 0 | 0 | 0 | |
| RENAME / EXCHANGE | 6 | 0 | 0 | 0 | 0 | 2 invalid output passes: `ON CLUSTER c` is rewritten as the table `phys.\`db1.c\`` |
| Command handlers | 90 | 20 — EXISTS / SHOW CREATE `FORMAT` / `SETTINGS` / `INTO OUTFILE` dropped, `EXISTS` / `SHOW CREATE` / `DESCRIBE TEMPORARY TABLE n` qualified into `phys.\`db1.n\``, `SHOW TABLES` `LIKE` / `NOT (I)LIKE` / `LIMIT` / `FORMAT` / `INTO OUTFILE` / `FULL` / `TEMPORARY` dropped, `SHOW DATABASES` `LIMIT` / `FORMAT` dropped, SI `DESCRIBE … FORMAT` dropped | 9 — junk after EXISTS / SHOW CREATE / USE / SHOW, `EXISTS TABLE numbers(10)` | 0 | 0 | |
| **Total** | **515** | **49** | **9** | **21** | **2** | |

The repository sweep (below) added three more semantic shapes: `DROP TABLE IF EMPTY db1.t` loses `IF EMPTY` (a guarded drop becomes unconditional), `CREATE DEFINER=alice LIVE VIEW …` regenerates as `CREATE DEFINER=aliceLIVE VIEW …` (an ordinary view; surface inactive and no-rewrite mode only), and `CREATE MATERIALIZED VIEW … REFRESH … TO t` loses both the refresh clause and the `TO` target (no-rewrite mode; dynamic mode already refused it).

2. Functions: every ClickHouse 26.2 function name with one and two arguments (3,564 inputs; 3,426 answered `Success`). 10 semantic: `CHAR_LENGTH` / `CHARACTER_LENGTH` → `LENGTH` (characters → bytes), `instr` → `POSITION` (case-insensitive → case-sensitive), `toStartOfDay(x)` → `dateTrunc('DAY', x)` (a different type and value for `Date32` / `DateTime64`), `group_concat(s, sep)` → `GROUP_CONCAT(CONCAT(s, sep))`, `first_value` / `last_value` / `ntile` with an extra argument dropped, and `JSON_QUERY` / `JSON_VALUE` given a `'$'` path. 3 invalid output: `startsWith` → `STARTS_WITH`, `toTypeName` → `TYPEOF`, `trim(s, c)` → `TRIM(c FROM s)`. 1 cosmetic but refused: `locate(a, b)` → `POSITION(b, a)`, which is equal only under the default `function_locate_has_mysql_compatible_argument_order = 1`. 59 cosmetic respellings pass (case of case-insensitive names, and the aliases in `spellingClass`). 4 invalid-output respellings pass, because the check folds case: `match` → `MATCH` and `cume_dist` → `CUME_DIST`.

3. Types: 89 type spellings × {column type, `CAST(a AS T)`, `a::T`, `CAST(a, 'T')`} (356 inputs; 348 `Success`). 29 semantic: `::` and `CAST(… AS <SQL alias>)` wrap the type in `Nullable` (`a::String`, `CAST(a AS BIGINT)`, `a::Array(String)` → `Array(Nullable(String))`, …), a `BLOB` column becomes `Nullable(String)`, and `a::INT UNSIGNED` becomes `a::Nullable(Int32) AS UNSIGNED`. 8 invalid output: `Point` / `Polygon` / `MultiPolygon` / `LineString` → `GEOMETRY(…)`. 1 cosmetic but refused: a `DOUBLE PRECISION` column. 33 cosmetic respellings pass. 15 invalid-output respellings pass: case-sensitive type names upper-cased in casts (`TUPLE`, `VARIANT`, `OBJECT`, `BFLOAT16`, `QBIT`, `NOTHING`, `INTERVAL…`) and `INT UNSIGNED` → `Int32 UNSIGNED`.

4. Traffic: 141 statements. The sentio indexer driver's DDL / DML / reads from `sentio-core` `common/chx/{operator,query,field,table}.go` and `driver/entity/clickhouse/{create,entity,entity_list}.go` (57, bound values), the sentio platform writer's `common/clickhouse/writer.go` shapes (5), housegate's startup and snapshot probes plus the agent's inline-VALUES `FORMAT Native` and `VALUES(…)` helpers (19), and clickhouse-client / clickhouse-go / Grafana / Metabase shapes (60). 3 answers change, all `Success` → T7: the writer's `DELETE … IN PARTITION` (the required refusal), the driver's `startsWith(name, 'patch-')` probe (already broken: ClickHouse rejects `STARTS_WITH`), and a client `toTypeName` (already broken: `TYPEOF`).

| Question | Decision | Basis |
|---|---|---|
| Round-trip check, targeted refusals, or both | Both. A general check (`CheckRegenerated`) covers every AST Polyglot builds, and targeted coverage checks cover the handlers that re-render `command` nodes, which the general check cannot see | The class is open: 49 clause, 10 function and 29 type drops across every statement family, several not in the review's list (`DROP TABLE IF EMPTY`, `DROP TEMPORARY`, `CHAR_LENGTH`, `::` casts). No denylist can be complete. The command handlers regenerate from a few parsed fields, so their coverage is a simple "any token after what was parsed" rule |
| What the general check compares | The input with Polyglot's identity `Generate(ast)`, as multisets of spellings after a closed, measured normalisation, not the rewritten output | Every later rewrite (`RenameTables`, `QualifyTables`, SI wrapping) changes names only and uses the same generator, so a faithful identity regeneration is what the rewritten SQL relies on. Comparing the rewritten output would need to undo the rewrite |
| Multiset, not sequence | Multiset | Polyglot reorders cosmetically (`SETTINGS` / `FORMAT`, `NOT … ILIKE`, `ALL INNER JOIN`, `LIMIT n, m`). No measured drop is a pure reordering. Recorded residual below |
| Symmetric, not loss-only | Symmetric: an added spelling fails too, except an alias's `AS`, an `=` and a comma join's `CROSS JOIN` | A loss-only check misses `::String` → `::Nullable(String)`, `group_concat(s, sep)` → `GROUP_CONCAT(CONCAT(s, sep))` and `JSON_QUERY(j)` → `JSON_QUERY(j, '$')` |
| Where it runs | In `finalize`, on a response whose code is `Success`, in every mode and both SI states | Every `Success` leaves through `finalize`. Running last cannot change any existing refusal. The parse gate had to run first because no check can see an unparsed tail; here the AST is complete, and a refusal generates no SQL |
| Refusal | `UnsupportedStatement` / T7 `statement is not supported`, SQL echoed, `statement_type` cleared, `existence_clause` kept; with the SI surface active `finalize`'s annotation may name an SI object, as for every refusal | Same reasoning as the parse gate (T2-I1): the engine cannot represent the statement, an engine-internal limit, spec §5 |
| Precedence | Last: after step 9 (the final fallthrough). The handler coverage checks sit at the point each handler would render its `Success`, after that handler's own refusals | A statement that is both refused by a policy check and dropped keeps the policy check's code and message, the same answer the C++ engine gives |
| MaterializeSQL | `MaterializeUnsupportedStatement` with the detailed message, SQL echoed, no replacements, checked only when there are replacements to regenerate | Before the gate the agent signed `INSERT … SELECT toDateTime(…), a … LIMIT 1` for `… LIMIT 1 WITH TIES FORMAT JSON`. With no replacements the caller's own SQL is returned. housegate's materialize plugin fails open on a non-`Success` code and signs the original SQL |
| Snapshot analyser | Not gated | Its closed lexical profile (`snapshot_lexemes.go`) already refuses every probed drop shape (8 shapes). Adding the check there only changes those refusals from "outside the closed profile" to the statement refusal, and costs a third `Generate` that `TestSnapshotPrepareWholeDeadline` counts. The plugin is unwired |
| Shared cases added | None, and the corpus bytes do not change | Every refused input is answered `Success` with the clause kept by rewriter-grpc |

**False-positive sweep.** Every SQL string in the repository: the seven harness JSON corpora, `internal/corpus/testdata/seed.sql`, and every SQL-shaped string literal in every `_test.go` file, 2,333 distinct strings, each through the dynamic mode in both SI states and the no-rewrite mode. 87 strings change an answer, every change `Success` → T7. 83 are drops or mangles, inputs ClickHouse refuses, or test strings that are not SQL. 4 are cosmetic false positives, all the unary `+` in snapshot-corpus inputs in the no-rewrite mode only (the dynamic mode already refuses them as `InvalidRewriteRequest`). The 960 shared cases give identical answers (`TestStorageIntegrityGolden` unchanged). MaterializeSQL over 6,862 distinct inputs changes 3 answers, all semantic (`WITH TIES` + `FORMAT` dropped, `::Nullable`, `STARTS_WITH`). The complete list of cosmetic shapes the gate refuses is: unary `+`, `INSERT INTO t (*) VALUES`, `locate(a, b)` and a `DOUBLE PRECISION` column type. None appears in the driver, writer, probe or client traffic. Cost: `doRewrite` over the 681 distinct shared-corpus inputs went from 196 ms to 204 ms (+4%), and over 136 of the traffic statements from 103 ms to 112 ms (+8%, more of them reach `Success`).

**Known residuals.** None of these lets a statement reach ClickHouse with a different meaning.
- A pure reordering with no lost or added spelling passes. None was measured.
- Identifier case is folded, so a case-only identifier change would pass. Polyglot changes the case of function and type names only.
- Output that ClickHouse rejects passes where the check folds case: upper-cased case-sensitive names (`MATCH`, `CUME_DIST`, `TUPLE(…)` / `VARIANT(…)` / `OBJECT(…)` casts, `BFLOAT16`, `QBIT`), `Int32 UNSIGNED`, `COLLATE 'x' DESC` printed after `DESC`, `RENAME` / `EXCHANGE … ON CLUSTER c` rewriting the cluster as a table, and the view `COMMENT='x'` (the view-COMMENT follow-up). These already fail at ClickHouse. A follow-up can make the native generator print them correctly; the driver's `startsWith` probe needs that follow-up before native serves driver traffic in fail-closed mode.
- `SHOW TABLES … LIKE` is refused, not modelled. rewriter-grpc's `show_tables.cc` also ignores `LIKE` / `LIMIT` (read from source, not measured); that is a C++ follow-up for Plan B v2's owner and does not touch the shared corpus.

**Pin flips.**
- Shared corpus: none. `TestStorageIntegrityGolden`, `TestSICorpusContract` and `TestSICorpusIsBytePinned` pass unchanged.
- `native_test.go` `TestStorageIntegrityContract_EmptySILiveViewKeepsLegacyDispatch`: `Success` → T7 `UnsupportedStatement` / `statement is not supported`, contract still unspecified. Polyglot regenerates `CREATE DEFINER=alice LIVE VIEW other.v AS …` as `CREATE DEFINER=aliceLIVE VIEW other.v AS …`, an ordinary view (Task 2).
- `native_test.go` `TestStorageIntegrityLiveViewClassifierErrorFailsClosedOnlyForStructuredCreateView/unrelated_select`: the fake tokenizer's success budget goes from 1 to 3 (the parse gate plus the check's two calls). No behaviour change; a new subtest pins the fail-closed tokenizer error (Task 2).
- `internal/handlers/dblevel_test.go` `TestRewriteDBLevel_showTablePrefixesRetainPolicySemantics/{logical_protected,ordinary}`: `SHOW FULL TEMPORARY TABLES FROM db1` and `SHOW TEMPORARY TABLES FROM other` were answered with the plain `SHOW TABLES` enumeration; they are now T7. The physical case keeps its SI refusal (Task 3).

## Pin sequencing across the two repositories

| Lands in | What |
|---|---|
| The parse-gate PR (`fix/whole-statement-parse-gate`) | Unchanged by this plan. It moves the corpus to `8bb63224…` / 864998 / 960 / `17952022305366168916`. |
| **This PR** (rewriter-go `fix/mid-statement-drop-gate`, stacked on the parse gate) | The general check, the handler coverage checks, the MaterializeSQL check and native pins. The corpus is byte-identical to the parse gate's. v0.14.0 is not released. |
| **Plan B v2** (rewriter-grpc) | Nothing to re-copy because of this PR: it copies the parse gate's `8bb63224…` corpus as already planned, and this PR leaves those bytes unchanged. If Plan B v2 wants engine parity for these shapes, rewriter-grpc keeps answering `Success` for them (ClickHouse's parser keeps every clause), which spec §5 allows for an engine limit. |
| **Paired re-pin** (#43 plan Task 10) | Unaffected: no shared case changes answer on the Go side, so the zero-divergence differential sees no new row from this PR. |
| **Release** | rewriter-go v0.14.0 waits for this PR, the parse-gate PR and the C++ zero-divergence pairing. rewriter-grpc v0.16.0 follows. |

## File Structure

| File | Responsibility |
|---|---|
| `internal/engine/regenerated.go` (create) | `ErrNotRegeneratedFaithfully`, `CheckRegenerated`, the spelling normaliser and its closed tables |
| `internal/engine/regenerated_test.go` (create) | Unit pins: cosmetic respellings pass, measured drops fail with exact text |
| `native.go` (modify) | `finalize` refuses a `Success` whose regeneration differs |
| `native_midstmt_gate_test.go` (create) | Native pins of the refusal in every mode, the command-handler pins and the MaterializeSQL pin |
| `native_test.go` (modify) | Fake tokenizer budget, the check's fail-closed subtest, the live-view pin flip |
| `internal/engine/objtarget.go`, `internal/engine/dblevel.go` (modify) | `ObjectTarget.Trailing`, `DBLevelInfo.Trailing` |
| `internal/handlers/exists.go`, `internal/handlers/describe.go`, `internal/handlers/dblevel.go`, `internal/handlers/dblevel_test.go` (modify) | Command handlers refuse what they would drop |
| `materialize.go` (modify) | The check before a materialized statement is regenerated |
| `AGENTS.md`, `internal/engine/AGENTS.md`, `internal/handlers/AGENTS.md` (modify) | Documentation |

---

### Task 0: base and baseline

- [ ] **Step 1: Confirm the base**

```bash
git log -1 --format=%h                  # the parse-gate head (d5355e3 when this plan was written) or origin/main
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json
# 8bb63224934bf60794ebc3569c0b1c4b53370abdebb2c280e150423b0228abb7
test -f internal/engine/wholestmt.go && grep -n 'CheckParsedInFull(e, sql, ast)' native.go materialize.go
go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
```

Expected: the parse gate is present (`wholestmt.go` exists and `native.go` / `materialize.go` call it), the corpus hash matches, and both lanes are `ok` for every package. Record the base for Task 6: `mkdir -p tmp && git rev-parse HEAD > tmp/base-sha` (`tmp/` is git-ignored). Nothing to commit.

---

### Task 1: `engine.CheckRegenerated`

**Files:**
- Create: `internal/engine/regenerated.go`
- Test: `internal/engine/regenerated_test.go`

**Interfaces:**
- Consumes: `tokenizeRaw(e Engine, sql string) ([]rawToken, error)` (`internal/engine/writes.go`), `NodeKind(ast AST) (string, error)` and the `NodeCommand` / `NodeRaw` / `NodeInsert` constants (`internal/engine/ast.go`), and `newTestEngine(t)` in tests.
- Produces: `var ErrNotRegeneratedFaithfully error` and `func CheckRegenerated(e Engine, sql string, ast AST) error`. It returns nil when `Generate(ast)` spells the same statement as `sql`, an error wrapping `ErrNotRegeneratedFaithfully` whose text starts with `engine: generate: the regenerated statement differs from the input` otherwise, and a tokenizer or generator error as is.

- [ ] **Step 1: Write the failing test** — `internal/engine/regenerated_test.go`:

```go
package engine

import (
	"errors"
	"testing"
)

func TestCheckRegenerated(t *testing.T) {
	e := newTestEngine(t)
	for _, tc := range []struct {
		name string
		sql  string
		want string // "" = regenerated faithfully; otherwise the exact error text
	}{
		// Regenerated faithfully, including every measured cosmetic respelling.
		{"plain select", "SELECT a, count() FROM db1.o WHERE b = 1 GROUP BY a ORDER BY a DESC LIMIT 10", ""},
		{"nulls last is the default", "SELECT a FROM db1.o ORDER BY a DESC NULLS LAST, b ASC NULLS LAST", ""},
		{"nulls first is kept", "SELECT a FROM db1.o ORDER BY a NULLS FIRST", ""},
		{"select all", "SELECT ALL a FROM db1.o", ""},
		{"top", "SELECT TOP 5 a FROM db1.o", ""},
		{"limit n, m", "SELECT a FROM db1.o LIMIT 5, 10", ""},
		{"limit by n, m", "SELECT a FROM db1.o LIMIT 1, 2 BY a", ""},
		{"comma join", "SELECT * FROM db1.o, db1.p", ""},
		{"implicit alias", "SELECT a x FROM db1.o t1", ""},
		{"div mod", "SELECT a DIV 2, a MOD 2, mod(a, 2) FROM db1.o", ""},
		{"regexp", "SELECT a FROM db1.o WHERE s REGEXP 'x'", ""},
		{"null-safe equality", "SELECT a <=> b FROM db1.o", ""},
		{"ternary", "SELECT a > 1 ? 'x' : 'y' FROM db1.o WHERE b = {p:UInt8}", ""},
		{"position in", "SELECT POSITION('a' IN s) FROM db1.o", ""},
		{"interval number", "SELECT now() - INTERVAL 1 DAY", ""},
		{"heredoc", "SELECT * FROM merge($tag$hg_safe$tag$, 'db1__t')", ""},
		{"settings then format", "SELECT a FROM db1.o FORMAT JSON SETTINGS max_threads = 1", ""},
		{"function aliases", "SELECT pow(a, 2), log(a), ceiling(a), substr(s, 1), lcase(s), ucase(s), DATE_TRUNC('day', t) FROM db1.o", ""},
		{"type aliases", "CREATE TABLE db1.n (a INT, b BIGINT, c TEXT, d VARCHAR(255), e DOUBLE, f BOOLEAN, g TIMESTAMP, h NUMERIC(10, 2)) ENGINE = Memory", ""},
		{"truncate table keyword", "TRUNCATE db1.o", ""},
		{"insert into table", "INSERT INTO TABLE db1.o (a, b) VALUES (1, 'a')", ""},
		{"insert format payload", "INSERT INTO db1.o FORMAT CSV 1,(2", ""},
		{"insert select format comment", "INSERT INTO db1.o SELECT * FROM db1.p FORMAT JSON -- c", ""},
		{"definer string", "CREATE DEFINER='live' VIEW db1.v AS SELECT 1", ""},
		{"driver table", "CREATE TABLE `db1`.`p_e` (`id` String, `n` UInt64 DEFAULT 0 COMMENT 'c' CODEC(Delta, ZSTD(1)), INDEX `i` id TYPE bloom_filter GRANULARITY 1, PROJECTION `p` (SELECT * ORDER BY `n`)) ENGINE = ReplacingMergeTree(`n`) PARTITION BY `n` ORDER BY (`id`) SETTINGS index_granularity=8192 COMMENT 'h'", ""},
		{"command keeps its text", "SHOW CREATE TABLE db1.o XYZ", ""},

		// Dropped or respelled with a different meaning.
		{"delete in partition", "DELETE FROM db1.o IN PARTITION '2024-01' WHERE a = 1",
			"engine: generate: the regenerated statement differs from the input: lost [2024-01 IN PARTITION], added nothing"},
		{"with ties before format", "SELECT a FROM db1.o ORDER BY a LIMIT 1 WITH TIES FORMAT JSON",
			"engine: generate: the regenerated statement differs from the input: lost [TIES WITH], added nothing"},
		{"with ties in a subquery", "SELECT * FROM (SELECT a FROM db1.o ORDER BY a LIMIT 1 WITH TIES) AS s",
			"engine: generate: the regenerated statement differs from the input: lost [TIES WITH], added nothing"},
		{"drop on cluster", "DROP TABLE db1.o ON CLUSTER c",
			"engine: generate: the regenerated statement differs from the input: lost [C CLUSTER ON], added nothing"},
		{"drop temporary", "DROP TEMPORARY TABLE n",
			"engine: generate: the regenerated statement differs from the input: lost [TEMPORARY], added nothing"},
		{"drop if empty", "DROP TABLE IF EMPTY db1.o",
			"engine: generate: the regenerated statement differs from the input: lost [EMPTY IF], added nothing"},
		{"limit by then limit", "SELECT a FROM db1.o LIMIT 2 BY a LIMIT 10",
			"engine: generate: the regenerated statement differs from the input: lost [2 LIMIT], added nothing"},
		{"ephemeral", "CREATE TABLE db1.n (a Int32, e Int32 EPHEMERAL) ENGINE = Memory",
			"engine: generate: the regenerated statement differs from the input: lost [EPHEMERAL], added nothing"},
		{"insert column transformer", "INSERT INTO db1.o (* EXCEPT (b)) VALUES (1)",
			"engine: generate: the regenerated statement differs from the input: lost [STAR B EXCEPT], added nothing"},
		{"truncate settings", "TRUNCATE TABLE db1.o SETTINGS max_threads = 1",
			"engine: generate: the regenerated statement differs from the input: lost [1 EQ MAX_THREADS SETTINGS], added nothing"},
		{"cast gains nullable", "SELECT a::String FROM db1.o",
			"engine: generate: the regenerated statement differs from the input: lost nothing, added [NULLABLE]"},
		{"char length is not length", "SELECT CHAR_LENGTH(s) FROM db1.o",
			"engine: generate: the regenerated statement differs from the input: lost [CHAR_LENGTH], added [LENGTH]"},
		{"group_concat separator", "SELECT group_concat(s, '-') FROM db1.o",
			"engine: generate: the regenerated statement differs from the input: lost nothing, added [CONCAT]"},
		{"no such function", "SELECT startsWith(s, 'x') FROM db1.o",
			"engine: generate: the regenerated statement differs from the input: lost [STARTSWITH], added [STARTS_WITH]"},
		{"live view becomes a view", "CREATE DEFINER=alice LIVE VIEW db1.v AS SELECT 1",
			"engine: generate: the regenerated statement differs from the input: lost [ALICE LIVE], added [ALICELIVE]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := e.ParseOne(tc.sql)
			if err != nil {
				t.Fatalf("ParseOne: %v", err)
			}
			if err := CheckParsedInFull(e, tc.sql, ast); err != nil {
				t.Fatalf("CheckParsedInFull: %v (the input must pass the parse gate)", err)
			}
			err = CheckRegenerated(e, tc.sql, ast)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if err == nil || err.Error() != tc.want {
				t.Fatalf("err = %v, want %s", err, tc.want)
			}
			if !errors.Is(err, ErrNotRegeneratedFaithfully) {
				t.Fatalf("err = %v does not wrap ErrNotRegeneratedFaithfully", err)
			}
		})
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/engine -run '^TestCheckRegenerated$' -count=1`
Expected: FAIL to compile, with `undefined: CheckRegenerated` and `undefined: ErrNotRegeneratedFaithfully`.

- [ ] **Step 3: Write the implementation** — `internal/engine/regenerated.go`:

```go
package engine

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrNotRegeneratedFaithfully marks a statement whose regenerated SQL does
// not spell the input. The pinned Polyglot consumes every token of such a
// statement, so the whole-statement parse gate passes it, but it parses some
// clauses into nothing (DELETE … IN PARTITION, DROP … ON CLUSTER, DROP
// TEMPORARY, WITH TIES before FORMAT / SETTINGS or inside a subquery, a
// column's EPHEMERAL) and prints others differently (LIMIT n BY … LIMIT m as
// LIMIT m BY …, a :: cast wrapped in Nullable, CHAR_LENGTH as LENGTH). The SQL
// generated from that AST means something other than the input.
var ErrNotRegeneratedFaithfully = errors.New("engine: generate: the regenerated statement differs from the input")

// CheckRegenerated returns nil when Generate(ast), which e.ParseOne produced
// for sql, spells the same statement as sql, and an error wrapping
// ErrNotRegeneratedFaithfully otherwise. Tokenizer and generator errors are
// returned as is.
//
// Both texts are tokenized by the engine and reduced to a multiset of
// spellings (fidelitySpellings): commas, parentheses, dots and semicolons are
// dropped, words are upper-cased, strings and numbers stay distinct, the
// ClickHouse aliases in spellingClass are folded, and the measured cosmetic
// respellings of the input are applied. The multisets must be equal, apart
// from the neutral additions spellingDiff allows. Every rewrite a caller
// applies afterwards changes only names, so a faithful identity regeneration
// is what makes the rewritten SQL faithful.
//
// A command or raw node carries its text and is regenerated verbatim, so it
// always passes; the handlers that re-render a command from parsed fields
// check their own coverage. An INSERT statement is compared only up to the
// name after its last FORMAT keyword: what follows is data that
// GenerateInsert splices back verbatim (the parse gate's payload rule).
func CheckRegenerated(e Engine, sql string, ast AST) error {
	kind, _ := NodeKind(ast)
	if kind == NodeCommand || kind == NodeRaw {
		return nil
	}
	gen, err := e.Generate(ast)
	if err != nil {
		return err
	}
	in, err := tokenizeRaw(e, sql)
	if err != nil {
		return err
	}
	out, err := tokenizeRaw(e, gen)
	if err != nil {
		return err
	}
	if kind == NodeInsert {
		in, out = throughFormatName(in), throughFormatName(out)
	}
	lost, added := spellingDiff(fidelitySpellings(in, true), fidelitySpellings(out, false))
	if len(lost) == 0 && len(added) == 0 {
		return nil
	}
	return fmt.Errorf("%w: lost %s, added %s", ErrNotRegeneratedFaithfully, spellingList(lost), spellingList(added))
}

// throughFormatName keeps the tokens up to and including the name after the
// last FORMAT keyword.
func throughFormatName(toks []rawToken) []rawToken {
	for i := len(toks) - 1; i >= 0; i-- {
		if toks[i].TokenType == "FORMAT" && i+1 < len(toks) {
			return toks[:i+2]
		}
	}
	return toks
}

// spellingClass folds a ClickHouse alias the generator respells into the name
// it prints. Each pair resolves to the same data type or function in
// ClickHouse's system.data_type_families / system.functions alias_to
// (measured on 26.2 and 26.7.5.10). A respelling that is not an alias stays a
// difference: CHAR_LENGTH → LENGTH (characters → bytes), instr → POSITION
// (case-insensitive → case-sensitive), toStartOfDay → dateTrunc('DAY', …)
// (DateTime64 / Date32 results differ), startsWith → STARTS_WITH and
// toTypeName → TYPEOF (no such functions).
var spellingClass = map[string]string{
	"BOOLEAN":    "BOOL",
	"INT":        "INT32",
	"INTEGER":    "INT32",
	"TINYINT":    "INT8",
	"SMALLINT":   "INT16",
	"BIGINT":     "INT64",
	"FLOAT":      "FLOAT32",
	"REAL":       "FLOAT32",
	"DOUBLE":     "FLOAT64",
	"TEXT":       "STRING",
	"CHAR":       "STRING",
	"VARCHAR":    "STRING",
	"TIMESTAMP":  "DATETIME",
	"NUMERIC":    "DECIMAL",
	"POW":        "POWER",
	"LN":         "LOG",
	"CEILING":    "CEIL",
	"SUBSTR":     "SUBSTRING",
	"LCASE":      "LOWER",
	"UCASE":      "UPPER",
	"DATE_TRUNC": "DATETRUNC",
}

// fidelitySpellings reduces a token stream to the spellings the check
// compares. With input set it also applies the measured cosmetic respellings
// of the input, each one ClickHouse's own formatter or execution shows to be
// the same statement: NULLS LAST, ClickHouse's default in both sort
// directions, is dropped; SELECT ALL is SELECT; TOP n is LIMIT n; LIMIT n, m
// is LIMIT m OFFSET n; x DIV y is intDiv(x, y); x MOD y is x % y; x REGEXP y
// is match(x, y); a <=> b is a IS NOT DISTINCT FROM b; a ? b : c is
// if(a, b, c); POSITION(x IN y) is POSITION(y, x).
func fidelitySpellings(toks []rawToken, input bool) []string {
	var out []string
	last := func() string {
		if len(out) == 0 {
			return ""
		}
		return out[len(out)-1]
	}
	depth, positionAt, ternary := 0, -1, 0
	for i := 0; i < len(toks); i++ {
		tk := toks[i]
		switch tk.TokenType {
		case "L_PAREN":
			depth++
		case "R_PAREN":
			depth--
			if depth < positionAt {
				positionAt = -1
			}
		}
		k := spellingOf(tk, last())
		switch {
		case k == "" || k == "W:TABLE":
			continue // punctuation; TRUNCATE t / INSERT INTO TABLE t: TABLE is optional
		case last() == "W:STRING" && tk.TokenType == "NUMBER" && i > 0 && toks[i-1].TokenType == "L_PAREN":
			continue // VARCHAR(n) / CHAR(n): ClickHouse ignores the length
		case tk.TokenType == "STRING" && len(out) >= 2 && out[len(out)-2] == "W:DEFINER" && last() == "O:EQ":
			k = "W:" + strings.ToUpper(tk.Text) // DEFINER = 'u' names the same user as DEFINER = u
		}
		if input {
			callNext := i+1 < len(toks) && toks[i+1].TokenType == "L_PAREN"
			switch {
			case k == "W:NULLS" && i+1 < len(toks) && spellingOf(toks[i+1], k) == "W:LAST":
				i++
				continue
			case k == "W:ALL" && last() == "W:SELECT":
				continue
			case k == "W:TOP":
				k = "W:LIMIT"
			case k == "W:DIV":
				k = "W:INTDIV"
			case k == "W:MOD" && !callNext:
				k = "O:PERCENT"
			case k == "W:REGEXP" && !callNext:
				k = "W:MATCH"
			case k == "O:NULLSAFE_EQ":
				out = append(out, "W:IS", "W:NOT", "W:DISTINCT")
				k = "W:FROM"
			case tk.TokenType == "PARAMETER" && tk.Text == "?":
				k = "W:IF"
				ternary++
			case k == "O:COLON" && ternary > 0:
				ternary--
				continue
			case k == "W:POSITION" && callNext:
				positionAt = depth + 1
			case k == "W:IN" && positionAt == depth:
				positionAt = -1
				continue
			case k == "W:LIMIT" && i+3 < len(toks) && toks[i+2].TokenType == "COMMA":
				out = append(out, k, spellingOf(toks[i+1], k), spellingOf(toks[i+3], ""), "W:OFFSET")
				i += 3
				continue
			}
		}
		out = append(out, k)
	}
	return out
}

// spellingOf is one token's spelling: "" for punctuation, N:/S: for a number
// or a string (a heredoc's body; INTERVAL '1' DAY's '1' counts as the number
// the input wrote), W: for a word or identifier, O: for an operator.
func spellingOf(tk rawToken, prev string) string {
	switch tk.TokenType {
	case "COMMA", "L_PAREN", "R_PAREN", "DOT", "SEMICOLON":
		return ""
	case "NUMBER":
		return "N:" + tk.Text
	case "STRING", "NATIONAL_STRING", "TRIPLE_DOUBLE_QUOTED_STRING", "TRIPLE_SINGLE_QUOTED_STRING":
		if prev == "W:INTERVAL" && isDigits(tk.Text) {
			return "N:" + tk.Text
		}
		return "S:" + tk.Text
	case "DOLLAR_STRING":
		// The tokenizer reports a tagged heredoc as tag\x00body.
		if i := strings.IndexByte(tk.Text, 0); i >= 0 {
			return "S:" + tk.Text[i+1:]
		}
		return "S:" + tk.Text
	}
	if tk.Text == "" {
		return ""
	}
	if tk.TokenType == "VAR" || tk.TokenType == "QUOTED_IDENTIFIER" || isWordStart(tk.Text) {
		w := strings.ToUpper(tk.Text)
		if c, ok := spellingClass[w]; ok {
			w = c
		}
		return "W:" + w
	}
	return "O:" + tk.TokenType
}

func isDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}

func isWordStart(s string) bool {
	c := s[0]
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// spellingDiff returns the spellings of in missing from out and those of out
// missing from in, both sorted. The generator may add an alias's AS and the
// = of ENGINE = / SETTINGS k = v, and prints a comma join as CROSS JOIN, so
// those additions are neutral.
func spellingDiff(in, out []string) (lost, added []string) {
	count := map[string]int{}
	for _, s := range in {
		count[s]++
	}
	for _, s := range out {
		count[s]--
	}
	if c, j := count["W:CROSS"], count["W:JOIN"]; c < 0 && j < 0 {
		n := min(-c, -j)
		count["W:CROSS"] += n
		count["W:JOIN"] += n
	}
	for s, n := range count {
		if n < 0 && (s == "W:AS" || s == "O:EQ") {
			continue
		}
		for ; n > 0; n-- {
			lost = append(lost, s)
		}
		for ; n < 0; n++ {
			added = append(added, s)
		}
	}
	sort.Strings(lost)
	sort.Strings(added)
	return lost, added
}

// spellingList renders spellings for an error message, without their class
// prefix.
func spellingList(s []string) string {
	if len(s) == 0 {
		return "nothing"
	}
	r := make([]string, len(s))
	for i, v := range s {
		r[i] = v[2:]
	}
	return "[" + strings.Join(r, " ") + "]"
}
```

- [ ] **Step 4: Run the test to verify it passes, and both lanes**

```bash
go test ./internal/engine -run '^TestCheckRegenerated$' -count=1 -v
go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
```

Expected: all 41 subtests PASS (every input also passes `CheckParsedInFull`, which the test asserts), and both lanes are `ok`. Nothing calls the helper yet.

- [ ] **Step 5: Commit**

```bash
git add internal/engine/regenerated.go internal/engine/regenerated_test.go
git commit -m "feat(engine): CheckRegenerated compares a statement with its regeneration

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `finalize` refuses a `Success` whose regeneration differs

**Files:**
- Modify: `native.go` (`finalize`)
- Modify: `native_test.go`
- Create: `native_midstmt_gate_test.go`

**Interfaces:**
- Consumes: `engine.CheckRegenerated` (Task 1), `engine.UnsupportedStatementMessage`, and the root test helpers `tablerefCase`, `runTablerefCases`, `tablerefOpts(si bool)`, `tableRewriteStatic()`, `newEngine(t)`, `doRewrite`, `fakeEngine`.
- Produces: nothing new. Task 3 appends to `native_midstmt_gate_test.go`.

- [ ] **Step 1: Write the failing native pins** — `native_midstmt_gate_test.go`:

```go
package rewriter

import (
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)

// TestMidStatementDropGate pins the refusal of a statement Polyglot parsed in
// full but regenerates as a different statement (spec 2026-09-26 §1). Every
// refused input passes the whole-statement parse gate; rewriter-grpc parses
// each with ClickHouse's own parser and keeps the clause, so none of them can
// live in the shared corpus.
func TestMidStatementDropGate(t *testing.T) {
	const unsupported = "statement is not supported"
	var cases []tablerefCase
	for _, si := range []bool{false, true} {
		for _, sql := range []string{
			// The parse-gate final review's list.
			"DELETE FROM db1.o IN PARTITION 1 WHERE a = 1",
			"DELETE FROM db1.o IN PARTITION '2024-01' WHERE a = 1",
			"SELECT a FROM db1.o ORDER BY a LIMIT 1 WITH TIES FORMAT JSON",
			"SELECT a FROM db1.o ORDER BY a LIMIT 1 WITH TIES SETTINGS max_threads = 1",
			"SELECT * FROM (SELECT a FROM db1.o ORDER BY a LIMIT 1 WITH TIES) AS s",
			"DROP TABLE db1.o ON CLUSTER c",
			"DROP VIEW IF EXISTS db1.v ON CLUSTER 'c'",
			"SELECT a FROM db1.o LIMIT 2 BY a LIMIT 10",
			"CREATE TABLE db1.n (a Int32, e Int32 EPHEMERAL) ENGINE = Memory",
			"ALTER TABLE db1.o ADD COLUMN c Int32 EPHEMERAL",
			"INSERT INTO db1.o (* EXCEPT (b)) VALUES (1)",
			// Found by the measurement.
			"DROP TABLE IF EMPTY db1.o",
			"DROP TEMPORARY TABLE n",
			"TRUNCATE TABLE db1.o SETTINGS max_threads = 1",
			"CREATE TABLE db1.n (a Int32 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY a",
			"CREATE VIEW db1.v AS SELECT a FROM db1.o LIMIT 2 BY a LIMIT 3",
			"SELECT a::String FROM db1.o",
			"SELECT CHAR_LENGTH(s) FROM db1.o",
			"SELECT instr(s, 'x') FROM db1.o",
			"SELECT toStartOfDay(t) FROM db1.o",
			"SELECT group_concat(s, '-') FROM db1.o",
			"SELECT startsWith(s, 'x') FROM db1.o",
			// An SI table keeps the T7 text.
			"SELECT * FROM db1.t ORDER BY a LIMIT 1 WITH TIES FORMAT JSON",
		} {
			cases = append(cases, tablerefCase{name: sql, sql: sql, si: si,
				wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: unsupported, wantSQL: sql})
		}
		// Cosmetic respellings pass.
		for _, sql := range []string{
			"SELECT a FROM db1.o ORDER BY a DESC NULLS LAST",
			"SELECT TOP 5 a FROM db1.o",
			"SELECT a FROM db1.o LIMIT 5, 10",
			"SELECT * FROM db1.o, db1.p",
			"SELECT a FROM db1.o FORMAT JSON SETTINGS max_threads = 1",
			"SELECT pow(a, 2), substr(s, 1) FROM db1.o",
			"CREATE TABLE db1.n (a INT, b VARCHAR(255), c BOOLEAN) ENGINE = Memory",
			"INSERT INTO TABLE db1.o (a) VALUES (1)",
		} {
			cases = append(cases, tablerefCase{name: sql, sql: sql, si: si, wantCode: pb.RewriteCode_Success})
		}
		if !si {
			// The active surface refuses a structured DELETE for its own reasons.
			cases = append(cases, tablerefCase{name: "delete", sql: "DELETE FROM db1.o WHERE a = 1", wantCode: pb.RewriteCode_Success})
		}
	}
	runTablerefCases(t, cases)
}

// The gate refuses in every mode, through the response, never the Go error.
func TestMidStatementDropGateEveryMode(t *testing.T) {
	e := newEngine(t)
	for _, sql := range []string{
		"DELETE FROM db1.o IN PARTITION 1 WHERE a = 1",
		"SELECT a FROM db1.o ORDER BY a LIMIT 1 WITH TIES FORMAT JSON",
	} {
		for name, opts := range map[string][]*pb.RewriteOption{
			"no rewrite": nil,
			"static":     {tableRewriteStatic()},
			"dynamic":    tablerefOpts(false),
		} {
			t.Run(name+"/"+sql, func(t *testing.T) {
				resp, err := doRewrite(e, sql, opts)
				if err != nil {
					t.Fatalf("doRewrite: %v", err)
				}
				if resp.GetCode() != pb.RewriteCode_UnsupportedStatement || resp.GetMessage() != "statement is not supported" ||
					resp.GetSqlAfterRewrite() != sql || resp.GetStatementType() != pb.StatementType_STATEMENT_TYPE_UNSPECIFIED {
					t.Fatalf("resp = %+v", resp)
				}
			})
		}
	}
}
```

- [ ] **Step 2: Update `native_test.go`**

In `TestStorageIntegrityLiveViewClassifierErrorFailsClosedOnlyForStructuredCreateView`, in `t.Run("unrelated select", …)`, replace

```go
			tokenizeOK:  1, // the whole-statement parse gate
		}
		resp, err := doRewrite(e, "SELECT 1", opts)
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetCode() != pb.RewriteCode_Success {
```

with

```go
			tokenizeOK:  3, // the whole-statement parse gate and the regeneration check
		}
		resp, err := doRewrite(e, "SELECT 1", opts)
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetCode() != pb.RewriteCode_Success {
```

In the same function, insert this subtest immediately before `t.Run("raw live view with folded whitespace", …)`:

```go
	t.Run("regeneration check cannot tokenize", func(t *testing.T) {
		e := &fakeEngine{
			parseAST:    engine.AST(`{"select":{}}`),
			tokenizeErr: tokenErr,
			tokenizeOK:  1, // the whole-statement parse gate
		}
		resp, err := doRewrite(e, "SELECT 1", opts)
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetCode() != pb.RewriteCode_UnsupportedStatement || resp.GetMessage() != engine.UnsupportedStatementMessage ||
			resp.GetSqlAfterRewrite() != "SELECT 1" {
			t.Fatalf("resp = %+v, a Success the regeneration check cannot confirm must be refused", resp)
		}
	})

```

In `TestStorageIntegrityContract_EmptySILiveViewKeepsLegacyDispatch`, replace

```go
	if res.Code != pb.RewriteCode_Success ||
		res.StorageIntegrityContractVersion != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED {
		t.Fatalf("res = %+v, want legacy Success/unspecified contract", res)
	}
```

with

```go
	// Legacy dispatch, not the SI classifier: the contract stays unspecified
	// and the message is T7. Polyglot regenerates the statement as
	// `CREATE DEFINER=aliceLIVE VIEW …`, an ordinary view, so the
	// mid-statement drop gate refuses it instead of answering Success.
	if res.Code != pb.RewriteCode_UnsupportedStatement || res.Message != engine.UnsupportedStatementMessage ||
		res.StorageIntegrityContractVersion != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED {
		t.Fatalf("res = %+v, want legacy T7 refusal/unspecified contract", res)
	}
```

- [ ] **Step 3: Run the new pins to verify they fail**

```bash
go test . -run '^(TestMidStatementDropGate|TestMidStatementDropGateEveryMode)$' -count=1
go test . -run '^(TestStorageIntegrityContract_EmptySILiveViewKeepsLegacyDispatch|TestStorageIntegrityLiveViewClassifierErrorFailsClosedOnlyForStructuredCreateView)$' -count=1
```

Expected: FAIL. In `TestMidStatementDropGate`, 44 subtests fail: every refusal row answers `Success` except the two SI-active `DELETE … IN PARTITION` rows, which the active surface already refuses; every passing row passes. All 6 `TestMidStatementDropGateEveryMode` subtests fail. `EmptySILiveViewKeepsLegacyDispatch` and the `regeneration check cannot tokenize` subtest fail with `Success`.

- [ ] **Step 4: Implement the gate** — in `native.go`, replace the head of `finalize`

```go
// addressed (Spec I D2).
func finalize(resp *pb.RewriteSQLResponse, ast engine.AST, sql string, ec pb.ExistenceClause, siVersion pb.StorageIntegrityContractVersion, e engine.Engine, sel nameresolve.Selection) {
	resp.ExistenceClause = ec
	resp.StorageIntegrityContractVersion = siVersion
	if resp.GetCode() == pb.RewriteCode_Success {
		return
	}
```

with

```go
// addressed (Spec I D2).
//
// It is also the mid-statement drop gate: a response about to answer Success
// is refused as T7 when Polyglot's regeneration of the statement does not
// spell the input (engine.CheckRegenerated). Every Success leaves through
// here, whatever handler built it, in every mode.
func finalize(resp *pb.RewriteSQLResponse, ast engine.AST, sql string, ec pb.ExistenceClause, siVersion pb.StorageIntegrityContractVersion, e engine.Engine, sel nameresolve.Selection) {
	resp.ExistenceClause = ec
	resp.StorageIntegrityContractVersion = siVersion
	if resp.GetCode() == pb.RewriteCode_Success {
		if engine.CheckRegenerated(e, sql, ast) == nil {
			return
		}
		resp.Code = pb.RewriteCode_UnsupportedStatement
		resp.Message = engine.UnsupportedStatementMessage
		resp.SqlAfterRewrite = sql
	}
```

- [ ] **Step 5: Run the pins**

```bash
go test . -run '^(TestMidStatementDropGate|TestMidStatementDropGateEveryMode)$' -count=1 -v | grep -c -- '--- PASS'
go test . -count=1
```

Expected: 71 `--- PASS` lines (63 + 6 subtests and the two parents), and the root package is `ok`.

- [ ] **Step 6: Run both lanes**

```bash
go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
```

Expected: every package `ok`. `TestStorageIntegrityGolden` passes unchanged: no shared case answered `Success` for a statement Polyglot regenerates differently.

- [ ] **Step 7: Commit**

```bash
git add native.go native_test.go native_midstmt_gate_test.go
git commit -m "fix: refuse a statement Polyglot regenerates as a different statement

Polyglot consumes every token of some statements but drops a clause in the
middle (DELETE … IN PARTITION, DROP … ON CLUSTER, WITH TIES before FORMAT,
EPHEMERAL) or prints it differently (LIMIT n BY … LIMIT m, :: casts wrapped
in Nullable, CHAR_LENGTH as LENGTH). finalize now refuses every Success
whose identity regeneration does not spell the input, as T7, in every mode.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: command handlers refuse what they would drop

**Files:**
- Modify: `internal/engine/objtarget.go`, `internal/engine/dblevel.go`
- Modify: `internal/handlers/exists.go`, `internal/handlers/describe.go`, `internal/handlers/dblevel.go`, `internal/handlers/dblevel_test.go`
- Modify: `native_midstmt_gate_test.go`

**Interfaces:**
- Consumes: `engine.ParseObjectTarget`, `engine.ParseDBLevel`, `engine.TableDecision`, `engine.ActionRename`, `rejectUnsupported`, `rejectDBUnsupported`, `decideWriteTarget`.
- Produces: `ObjectTarget.Trailing bool`, `DBLevelInfo.Trailing bool`, and `handlers.rejectsRerender(t engine.ObjectTarget, d engine.TableDecision) bool`.

- [ ] **Step 1: Write the failing pins** — append to `native_midstmt_gate_test.go`:

```go
// TestCommandRerenderGate pins the handlers that re-render a command from
// parsed fields: a clause they do not model is refused instead of dropped.
func TestCommandRerenderGate(t *testing.T) {
	const unsupported = "statement is not supported"
	var cases []tablerefCase
	for _, si := range []bool{false, true} {
		for _, sql := range []string{
			"EXISTS TABLE db1.o FORMAT JSON",
			"EXISTS TABLE db1.o XYZ",
			"EXISTS TEMPORARY TABLE n",
			"SHOW CREATE TABLE db1.o SETTINGS max_threads = 1",
			"SHOW CREATE TABLE db1.o XYZ phys.x",
			"DESCRIBE TABLE t FORMAT JSON",
			"USE db1 XYZ",
			"SHOW TABLES FROM db1 LIKE 'x%'",
			"SHOW TABLES FROM db1 LIMIT 10",
			"SHOW FULL TABLES FROM db1",
			"SHOW TEMPORARY TABLES",
			"SHOW DATABASES LIMIT 1",
			"SHOW DATABASES FORMAT JSON",
			"EXISTS TABLE db1.t FORMAT JSON",
		} {
			cases = append(cases, tablerefCase{name: sql, sql: sql, si: si,
				wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: unsupported, wantSQL: sql})
		}
		// DESCRIBE re-renders an SI table as the metadata SELECT; an ordinary
		// qualified target is echoed verbatim and drops nothing.
		describeSI := tablerefCase{name: "describe si", sql: "DESCRIBE TABLE db1.t FORMAT JSON", si: si, wantCode: pb.RewriteCode_Success}
		if si {
			describeSI.wantCode, describeSI.wantMsg, describeSI.wantSQL = pb.RewriteCode_UnsupportedStatement, unsupported, "DESCRIBE TABLE db1.t FORMAT JSON"
		}
		cases = append(cases, describeSI)
		for _, sql := range []string{
			"EXISTS TABLE db1.o",
			"SHOW CREATE TABLE db1.o;",
			"DESCRIBE TABLE t",
			"USE db1",
			"SHOW TABLES FROM db1",
			"SHOW DATABASES LIKE 'd%'",
			"SHOW DATABASES NOT ILIKE 'd%'",
		} {
			cases = append(cases, tablerefCase{name: sql, sql: sql, si: si, wantCode: pb.RewriteCode_Success})
		}
	}
	runTablerefCases(t, cases)
}

// EXISTS re-renders its target in every mode, not only under dynamic args.
func TestCommandRerenderGateEveryMode(t *testing.T) {
	e := newEngine(t)
	const sql = "EXISTS TABLE db1.o FORMAT JSON"
	for name, opts := range map[string][]*pb.RewriteOption{
		"no rewrite": nil,
		"static":     {tableRewriteStatic()},
		"dynamic":    tablerefOpts(false),
	} {
		t.Run(name, func(t *testing.T) {
			resp, err := doRewrite(e, sql, opts)
			if err != nil {
				t.Fatalf("doRewrite: %v", err)
			}
			if resp.GetCode() != pb.RewriteCode_UnsupportedStatement || resp.GetMessage() != "statement is not supported" ||
				resp.GetSqlAfterRewrite() != sql {
				t.Fatalf("resp = %+v", resp)
			}
		})
	}
}
```

In `internal/handlers/dblevel_test.go` `TestRewriteDBLevel_showTablePrefixesRetainPolicySemantics`, replace

```go
			base, prefixed := rewrite(tc.baseSQL), rewrite(tc.prefixSQL)
			if !proto.Equal(prefixed, base) {
```

with

```go
			base, prefixed := rewrite(tc.baseSQL), rewrite(tc.prefixSQL)
			// A prefix never changes a policy refusal. Where the base answers
			// Success, the synthetic enumeration cannot express FULL (an extra
			// engine column) or TEMPORARY (session tables), so the prefixed
			// statement is refused rather than answered for SHOW TABLES.
			if base.GetCode() == pb.RewriteCode_Success {
				if prefixed.GetCode() != pb.RewriteCode_UnsupportedStatement || prefixed.GetMessage() != engine.UnsupportedStatementMessage {
					t.Fatalf("prefixed=%+v, want the T7 refusal", prefixed)
				}
				return
			}
			if !proto.Equal(prefixed, base) {
```

- [ ] **Step 2: Run them to verify they fail**

```bash
go test . -run '^(TestCommandRerenderGate|TestCommandRerenderGateEveryMode)$' -count=1
go test ./internal/handlers -run '^TestRewriteDBLevel_showTablePrefixesRetainPolicySemantics$' -count=1
```

Expected: FAIL. 29 `TestCommandRerenderGate` subtests fail (all 28 refusal rows, and the SI-active `describe si` row) with `Success`; the 14 passing rows pass. All 3 `TestCommandRerenderGateEveryMode` subtests fail. The handler test fails for `logical_protected` and `ordinary`.

- [ ] **Step 3: `ObjectTarget.Trailing`** — in `internal/engine/objtarget.go`, after the `Shape ObjectTargetShape` field of `ObjectTarget`, add:

```go
	// Trailing reports a token after the [db.]name target other than a
	// closing semicolon: a call's argument list, a FORMAT / SETTINGS /
	// INTO OUTFILE clause, or junk. A handler that re-renders the statement
	// from DB/Table would drop it.
	Trailing bool
```

In `ParseObjectTarget`, replace

```go
		out.Shape = ObjectTargetName
		if next < len(toks) && toks[next].TokenType == "L_PAREN" {
			out.Shape = ObjectTargetCall
		}
```

with

```go
		out.Shape = ObjectTargetName
		if next < len(toks) && toks[next].TokenType == "L_PAREN" {
			out.Shape = ObjectTargetCall
		}
		for _, tk := range toks[next:] {
			if tk.TokenType != "SEMICOLON" {
				out.Trailing = true
				break
			}
		}
```

- [ ] **Step 4: `DBLevelInfo.Trailing`** — in `internal/engine/dblevel.go`, after the `LikeCaseInsensitive bool   // ILIKE` field of `DBLevelInfo`, add:

```go
	// Trailing reports a token after the modelled head — `USE <db>`, or
	// `SHOW [EXTENDED] [FULL] [TEMPORARY] <kind> [{FROM|IN} <db>]` followed at
	// most by one `[NOT] (I)LIKE '<pattern>'` — other than a closing
	// semicolon: WHERE, LIMIT, FORMAT, SETTINGS, INTO OUTFILE or junk. A
	// handler that synthesizes SQL from these fields would drop it.
	Trailing bool
```

In `ParseDBLevel`, in `case "USE":`, insert `info.Trailing = tokensRemain(toks, 2)` directly before its `return info, nil`. In `case "SHOW":`, insert `info.Trailing = tokensRemain(toks, afterLikeClause(toks, i))` directly before the `for i < len(toks) {` loop that looks for `NOT` / `LIKE`. Append to the file:

```go
// tokensRemain reports a token other than a semicolon at or after toks[from].
func tokensRemain(toks []rawToken, from int) bool {
	for i := from; i < len(toks); i++ {
		if toks[i].TokenType != "SEMICOLON" {
			return true
		}
	}
	return false
}

// afterLikeClause returns the index after one `[NOT] (I)LIKE '<pattern>'`
// clause starting at toks[i], or i when none starts there.
func afterLikeClause(toks []rawToken, i int) int {
	j := i
	if j < len(toks) && toks[j].TokenType == "NOT" {
		j++
	}
	if j+1 < len(toks) && (toks[j].TokenType == "LIKE" || toks[j].TokenType == "I_LIKE") && toks[j+1].TokenType == "STRING" {
		return j + 2
	}
	return i
}
```

- [ ] **Step 5: EXISTS / SHOW CREATE** — in `internal/handlers/exists.go` `RewriteExistsShowCreate`, in the storage-integrity branch replace

```go
			db, table := splitPhysicalName(tbl.GetSafeTable())
			recordRewrite(resp.TableRewrites, tt, db, table)
			resp.SqlAfterRewrite = buildObjectSQL(keyword, t.Temporary, db, table)
```

with

```go
			if t.Trailing || t.Temporary {
				rejectUnsupported(resp, engine.UnsupportedStatementMessage) // see rejectsRerender
				return resp, true, nil
			}
			db, table := splitPhysicalName(tbl.GetSafeTable())
			recordRewrite(resp.TableRewrites, tt, db, table)
			resp.SqlAfterRewrite = buildObjectSQL(keyword, t.Temporary, db, table)
```

At the end of the function replace

```go
	db, table := t.DB, t.Table
	if d.Action == engine.ActionRename {
		db, table = d.NewDB, d.NewTable
	}
	resp.SqlAfterRewrite = buildObjectSQL(keyword, t.Temporary, db, table)
	return resp, true, nil
}
```

with

```go
	if rejectsRerender(t, d) {
		rejectUnsupported(resp, engine.UnsupportedStatementMessage)
		return resp, true, nil
	}
	db, table := t.DB, t.Table
	if d.Action == engine.ActionRename {
		db, table = d.NewDB, d.NewTable
	}
	resp.SqlAfterRewrite = buildObjectSQL(keyword, t.Temporary, db, table)
	return resp, true, nil
}

// rejectsRerender reports an EXISTS / SHOW CREATE / DESCRIBE target that
// buildObjectSQL cannot re-render faithfully (mid-statement drop gate): any
// token after the name (a FORMAT / SETTINGS / INTO OUTFILE clause, a call's
// arguments, junk) would be dropped, and a TEMPORARY table, which lives in no
// database, would be qualified into a different, ordinary table.
func rejectsRerender(t engine.ObjectTarget, d engine.TableDecision) bool {
	return t.Trailing || (t.Temporary && d.Action == engine.ActionRename)
}
```

- [ ] **Step 6: DESCRIBE** — in `internal/handlers/describe.go` `RewriteDescribe`, replace

```go
			resp.SqlAfterRewrite = describeMetadataSQL(tbl.GetSafeTable(), nameresolve.ReservedRowIDColumn(sel.Dynamic))
```

with

```go
			if t.Trailing || t.Temporary {
				// The metadata SELECT would drop the clause (see rejectsRerender).
				rejectUnsupported(resp, engine.UnsupportedStatementMessage)
				return resp, true, nil
			}
			resp.SqlAfterRewrite = describeMetadataSQL(tbl.GetSafeTable(), nameresolve.ReservedRowIDColumn(sel.Dynamic))
```

and replace

```go
		d, ok := decideWriteTarget(tt, "DESCRIBE TABLE", sel, resp)
		if !ok {
			return resp, true, nil
		}
		db, table := t.DB, t.Table
```

with

```go
		d, ok := decideWriteTarget(tt, "DESCRIBE TABLE", sel, resp)
		if !ok {
			return resp, true, nil
		}
		if rejectsRerender(t, d) {
			rejectUnsupported(resp, engine.UnsupportedStatementMessage)
			return resp, true, nil
		}
		db, table := t.DB, t.Table
```

The table-function / static-mode tail (`resp.SqlAfterRewrite = sql`) echoes the text and drops nothing, so it is unchanged.

- [ ] **Step 7: USE, SHOW TABLES, SHOW DATABASES** — in `internal/handlers/dblevel.go` `dispatchUse`, replace

```go
	if physical != origin {
		resp.SqlAfterRewrite = "USE " + physical
```

with

```go
	if physical != origin {
		if info.Trailing {
			// `USE <physical>` would drop the rest (mid-statement drop gate).
			rejectDBUnsupported(resp, engine.UnsupportedStatementMessage)
			return resp, true, nil
		}
		resp.SqlAfterRewrite = "USE " + physical
```

In `dispatchShowTables`, insert directly before `source := "system.tables"`:

```go
	if info.Trailing || info.HasLike || info.ShowExtended || info.ShowFull || info.ShowTemporary {
		// The synthetic enumeration models only SHOW TABLES [{FROM|IN} <db>]:
		// it would drop a LIKE / LIMIT / FORMAT / SETTINGS clause and the
		// EXTENDED / FULL / TEMPORARY variants (mid-statement drop gate).
		rejectDBUnsupported(resp, engine.UnsupportedStatementMessage)
		return resp, true, nil
	}
```

In `dispatchShowDatabases`, insert directly after the `if dyn == nil { return passthroughDB(e, ast, sql, resp) }` block:

```go
	if info.Trailing || info.ShowExtended || info.ShowFull || info.ShowTemporary {
		// The synthetic list models only an optional LIKE clause (mid-statement
		// drop gate): LIMIT / FORMAT / SETTINGS / WHERE would be dropped.
		rejectDBUnsupported(resp, engine.UnsupportedStatementMessage)
		return resp, true, nil
	}
```

Each check sits where the handler would render its `Success`, after the handler's own refusals, so no existing refusal changes. The pass-through paths echo the text or regenerate a `command` node verbatim and are unchanged.

- [ ] **Step 8: Run the pins and both lanes**

```bash
go test . -run '^(TestCommandRerenderGate|TestCommandRerenderGateEveryMode)$' -count=1 -v | grep -c -- '--- PASS'
go test ./internal/handlers -run '^TestRewriteDBLevel_showTablePrefixesRetainPolicySemantics$' -count=1
go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
```

Expected: 49 `--- PASS` lines (44 + 3 subtests and the two parents), the handler test passes, and every package is `ok` in both lanes.

- [ ] **Step 9: Commit**

```bash
git add internal/engine/objtarget.go internal/engine/dblevel.go internal/handlers/exists.go \
  internal/handlers/describe.go internal/handlers/dblevel.go internal/handlers/dblevel_test.go \
  native_midstmt_gate_test.go
git commit -m "fix: command handlers refuse a clause they would drop

EXISTS / SHOW CREATE / DESCRIBE, USE, SHOW TABLES and SHOW DATABASES
re-render a command from a few parsed fields, so FORMAT, SETTINGS, LIKE,
LIMIT, INTO OUTFILE, FULL / TEMPORARY and junk after the target were dropped
and a TEMPORARY table was qualified into an ordinary one. They are now T7.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: MaterializeSQL refuses a statement it would regenerate differently

**Files:**
- Modify: `materialize.go` (`doMaterializeSQL`)
- Modify: `native_midstmt_gate_test.go`

**Interfaces:**
- Consumes: `engine.CheckRegenerated` (Task 1), `doMaterializeSQL`.
- Produces: nothing new.

- [ ] **Step 1: Write the failing test** — append to `native_midstmt_gate_test.go`, and add `"strings"` to its import block (`import (` then `"strings"`, `"testing"`, a blank line, the `pb` import):

```go
// MaterializeSQL must not sign a statement the generator changed: before the
// gate it returned the regenerated SQL, WITH TIES dropped, as Success.
func TestMaterializeRefusesStatementNotRegeneratedFaithfully(t *testing.T) {
	e := newEngine(t)
	now := int64(1_700_000_000_000_000_000)
	for _, sql := range []string{
		"INSERT INTO db1.o SELECT now(), a FROM db1.p ORDER BY a LIMIT 1 WITH TIES FORMAT JSON",
		"INSERT INTO db1.o SELECT now(), a::String FROM db1.p",
	} {
		t.Run(sql, func(t *testing.T) {
			resp, err := doMaterializeSQL(e, &pb.MaterializeSQLRequest{Sql: sql,
				Inputs: &pb.MaterializationInputs{NowUnixNs: &now}})
			if err != nil {
				t.Fatal(err)
			}
			if resp.GetCode() != pb.MaterializeCode_MaterializeUnsupportedStatement ||
				!strings.Contains(resp.GetMessage(), "the regenerated statement differs from the input") ||
				resp.GetSqlAfterMaterialization() != sql || len(resp.GetReplacements()) != 0 {
				t.Fatalf("resp = %+v", resp)
			}
		})
	}
	resp, err := doMaterializeSQL(e, &pb.MaterializeSQLRequest{Sql: "INSERT INTO db1.o SELECT now(), a FROM db1.p ORDER BY a DESC NULLS LAST",
		Inputs: &pb.MaterializationInputs{NowUnixNs: &now}})
	if err != nil || resp.GetCode() != pb.MaterializeCode_MaterializeSuccess || len(resp.GetReplacements()) != 1 {
		t.Fatalf("a faithful statement still materializes: resp = %+v, err = %v", resp, err)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test . -run '^TestMaterializeRefusesStatementNotRegeneratedFaithfully$' -count=1`
Expected: FAIL. Both subtests answer `MaterializeSuccess`, with `INSERT INTO db1.o SELECT toDateTime(1700000000), a FROM db1.p ORDER BY a LIMIT 1` (`WITH TIES` and `FORMAT JSON` gone) and `INSERT INTO db1.o SELECT toDateTime(1700000000), a::Nullable(String) FROM db1.p`.

- [ ] **Step 3: Implement** — in `materialize.go` `doMaterializeSQL`, replace

```go
	if len(result.Replacements) == 0 {
		return resp, nil
	}
	out, err := e.Generate(result.AST)
```

with

```go
	if len(result.Replacements) == 0 {
		return resp, nil
	}
	if err := engine.CheckRegenerated(e, sql, ast); err != nil {
		// The materialized SQL would be regenerated by the same generator:
		// do not let the agent sign a statement the client never wrote.
		resp.Code = pb.MaterializeCode_MaterializeUnsupportedStatement
		resp.Message = err.Error()
		return resp, nil
	}
	out, err := e.Generate(result.AST)
```

- [ ] **Step 4: Run the test and both lanes**

```bash
go test . -run '^TestMaterializeRefusesStatementNotRegeneratedFaithfully$' -count=1
go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
```

Expected: PASS, and every package is `ok` in both lanes.

- [ ] **Step 5: Commit**

```bash
git add materialize.go native_midstmt_gate_test.go
git commit -m "fix: MaterializeSQL refuses a statement it would regenerate differently

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: documentation

**Files:**
- Modify: `AGENTS.md`, `internal/engine/AGENTS.md`, `internal/handlers/AGENTS.md`

- [ ] **Step 1: `AGENTS.md`**. Make these edits, one paragraph per line.

Directly below the bullet that begins ``- `internal/engine/wholestmt.go` —``, add:

```markdown
- `internal/engine/regenerated.go` — `CheckRegenerated`, the mid-statement drop gate (see below).
```

In the bullet that begins ``- `native.go` — dispatch order, the whole-statement parse gate,``, replace `the whole-statement parse gate,` with `the whole-statement parse gate, the mid-statement drop gate in `finalize`,`.

In the T7 message-family bullet, replace `a statement the engine did not parse in full (in every mode and both SI states),` with `a statement the engine did not parse in full or regenerates as a different statement (in every mode and both SI states), a command clause a handler cannot re-render,`.

Directly below the precedence step that begins `9. The final fallthrough`, add:

```markdown
10. Mid-statement drop gate — last of all, `finalize` refuses a response that would answer `Success` with T7 `statement is not supported` when `engine.CheckRegenerated` finds that Polyglot's regeneration of the statement does not spell the input, in every mode and both SI states. Every earlier refusal keeps its own code and message. The command handlers that re-render a statement from parsed fields (EXISTS / SHOW CREATE / DESCRIBE, USE, SHOW TABLES, SHOW DATABASES) apply the same rule at the point they would render their `Success`.
```

Replace the paragraph that begins `Known residual:` in the "Whole-statement parse gate" section (its exact text depends on the parse gate's final fix wave; it lists the class this plan closes) with:

```markdown
Known residual of this gate: a clause Polyglot consumes but drops or prints differently is not a consumption failure and passes it. The mid-statement drop gate below refuses those.
```

Insert this section directly below the "Whole-statement parse gate" section and above the next `## ` heading:

```markdown
## Mid-statement drop gate

The pinned Polyglot consumes every token of some statements but parses a clause into nothing or prints it differently, so the SQL generated from the AST means something else while the whole-statement parse gate passes it. Measured on the parse-gate head: `DELETE … IN PARTITION` dropped (widening a destructive statement to every partition), `DROP TABLE` / `VIEW` / `DATABASE … ON CLUSTER` dropped, `DROP TEMPORARY TABLE n` regenerated as `DROP TABLE n`, `DROP TABLE IF EMPTY` regenerated as `DROP TABLE`, `LIMIT … WITH TIES` dropped before `FORMAT` / `SETTINGS` and in subqueries, `LIMIT n BY … LIMIT m` regenerated as `LIMIT m BY …`, a column's `EPHEMERAL` / `STATISTICS` / `SETTINGS` dropped, `INSERT INTO t (* EXCEPT …)` / `(COLUMNS(…))` column lists dropped, `TRUNCATE … SETTINGS` dropped, `::T` and `CAST(x AS <SQL alias>)` wrapped in `Nullable`, and function respellings that are not ClickHouse aliases (`CHAR_LENGTH` → `LENGTH`, `instr` → `POSITION`, `toStartOfDay(x)` → `dateTrunc('DAY', x)`, `group_concat(s, sep)` → `GROUP_CONCAT(CONCAT(s, sep))`, `startsWith` → `STARTS_WITH`, `toTypeName` → `TYPEOF`).

`engine.CheckRegenerated(e, sql, ast)` tokenizes the input and Polyglot's identity `Generate(ast)` and compares them as multisets of spellings: commas, parentheses, dots and semicolons dropped, words upper-cased, strings and numbers distinct. A closed table of measured cosmetic respellings is applied: `NULLS LAST` (ClickHouse's default in both directions), `SELECT ALL`, `TOP n`, `LIMIT n, m`, `DIV` / `MOD`, `REGEXP`, `<=>`, `a ? b : c`, `POSITION(x IN y)`, `INTERVAL n`, heredoc bodies, `DEFINER = 'u'`, `VARCHAR(n)`, the optional `TABLE` keyword, and the ClickHouse aliases in `spellingClass`. An added `AS`, `=` or comma join's `CROSS JOIN` is neutral; any other lost or added spelling fails. `command` and `raw` nodes regenerate their own text and pass; an INSERT is compared only up to the name after its last `FORMAT`. `finalize` runs it on every would-be `Success` (precedence step 10), `doMaterializeSQL` runs it before regenerating a materialized statement (`MaterializeUnsupportedStatement`). The snapshot analyser is not gated: its closed lexical profile refuses every measured shape. Extend the tables only with ClickHouse evidence (an `alias_to` entry or an execution comparison) and a `TestCheckRegenerated` row.

Refused although cosmetic (fail closed, measured): unary `+`, `INSERT INTO t (*) VALUES`, `locate(a, b)` and a `DOUBLE PRECISION` column type. Not detected, none of which changes meaning silently: a pure reordering (none measured), an identifier whose case alone changes, and output ClickHouse itself rejects (upper-cased case-sensitive names such as `MATCH`, `CUME_DIST`, `TUPLE(…)` / `VARIANT(…)` casts; `Int32 UNSIGNED`; `COLLATE` after `DESC`; `RENAME … ON CLUSTER c` rewriting the cluster as a table; a view's `COMMENT='x'`). These shapes are engine-local: rewriter-grpc keeps every clause, so they stay out of the shared corpus.

```

- [ ] **Step 2: `internal/engine/AGENTS.md`** — below the `| Whole-statement parse gate | `wholestmt.go` | … |` row, add:

```markdown
| Mid-statement drop gate | `regenerated.go` | `CheckRegenerated`: refuse a statement whose regeneration does not spell the input |
```

Below `- Do not generate SQL from a user statement's AST without `CheckParsedInFull`: the ClickHouse dialect silently skips what it cannot parse.`, add:

```markdown
- Do not answer `Success` with SQL generated from a user statement's AST without `CheckRegenerated` (`finalize` does it for the rewrite pipeline): Polyglot drops or respells clauses it did consume.
```

- [ ] **Step 3: `internal/handlers/AGENTS.md`** — below the bullet that begins ``- `EXISTS` / `SHOW CREATE` / GRANT handling runs after DB-level dispatch``, add:

```markdown
- A handler that re-renders a command from parsed fields (EXISTS / SHOW CREATE / DESCRIBE via `ObjectTarget`, USE / SHOW TABLES / SHOW DATABASES via `DBLevelInfo`) must refuse, with T7, any token it does not model: `ObjectTarget.Trailing`, `DBLevelInfo.Trailing` and `rejectsRerender` exist for that. The general mid-statement drop gate cannot see a `command` node's re-rendering.
```

- [ ] **Step 4: Commit**

```bash
git add AGENTS.md internal/engine/AGENTS.md internal/handlers/AGENTS.md
git commit -m "docs: the mid-statement drop gate, its precedence and its measured tables

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: verification and PR

- [ ] **Step 1: Both CI lanes and the formatter**

```bash
gofmt -l . | grep -v third_party     # only internal/handlers/dblevel.go (pre-existing)
go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
```

Expected: every package `ok` in both lanes.

- [ ] **Step 2: Fuzz the rewrite for no-panic**

```bash
env SNAPSHOT_QUERY_ORDINARY=1 go test ./internal/harness -run '^$' -fuzz '^FuzzRewrite$' -fuzztime 30s
git status --short internal/harness/testdata/fuzz
```

Expected: `PASS` (measured: 1.79M execs). If the fuzzer wrote new files under `testdata/fuzz`, delete them. Do not commit fuzz output.

- [ ] **Step 3: Corpus identity**

```bash
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json
# 8bb63224934bf60794ebc3569c0b1c4b53370abdebb2c280e150423b0228abb7
git diff "$(cat tmp/base-sha)" --stat -- internal/harness/
go test ./internal/harness -run 'TestSICorpus(Contract|IsBytePinned)$|^TestStorageIntegrityGolden$' -count=1
```

Expected: the hash matches, `git diff` prints nothing, and the tests pass.

- [ ] **Step 4: The differential (when the build box is free)** — with rewriter-grpc v0.15.0 as the oracle on the build box:

```bash
POLYGLOT_SQL_FFI_PATH=… REWRITER_ORACLE_ADDR=<host:port> go test ./internal/harness -run '^TestStorageIntegrityGolden$' -count=1
```

Expected: exactly the divergences the parse gate's run showed, with no new row. The PR can open without this run. Record in the PR description whether it ran.

- [ ] **Step 5: Push and open the PR** — the PR is stacked on the parse gate until that merges:

```bash
git push -u origin fix/mid-statement-drop-gate
gh pr create --repo housegate/rewriter-go --base fix/whole-statement-parse-gate --head fix/mid-statement-drop-gate \
  --title "fix: refuse a statement Polyglot regenerates as a different statement" --body-file tmp/pr-body.md
```

If the parse gate has merged, use `--base main`; otherwise retarget the PR to `main` once it merges (`gh pr edit --base main`). `tmp/pr-body.md` (git-ignored) states:
- the defect and its origin (the parse gate's final review, item I2) and the user's ruling that it is a v0.14.0 release gate;
- the measured class and the false-positive sweep from "Decisions this plan records", including the four cosmetic shapes it refuses and the residuals;
- the refusal (T7 `UnsupportedStatement` / `statement is not supported`, last in precedence; `MaterializeUnsupportedStatement`) and why the snapshot analyser is not gated;
- that the corpus bytes and every shared answer are unchanged (`8bb63224…`), so Plan B v2 re-copies nothing because of this PR;
- the pin flips listed under "Pin flips";
- the driver / writer impact from Review Focus 6;
- the Review Focus list;
- whether the Step 4 differential ran.

End the body with:

```
🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

- [ ] **Step 6: Before release, check driver and writer traffic** — on devnet2 with rewriter-go at this PR's head and the native engine (the devnet runs grpc by default), search housegate logs for `statement is not supported` rejections. Look for `DELETE … IN PARTITION` from the sentio platform writer (`lightDeleteEvents` with `clickhouseDeleteWithPartition`), `ON CLUSTER` (`chx.WithCluster` has no caller today), the driver's `startsWith(name, 'patch-')` probe, `toStartOfDay`, `::` casts, `SHOW TABLES … LIKE` and `CHAR_LENGTH`. A hit needs a caller-side change (the writer's `WHERE` already restricts the partition, so dropping `IN PARTITION` there is safe) or a follow-up in the native generator, not a gate exception (spec §12 "Driver traffic").

- [ ] **Step 7: Stop for approval.** Merging this PR needs the user's explicit approval (spec §11). rewriter-go v0.14.0 is not released from this PR. The release waits for three things: this gate merged, the whole-statement parse gate merged, and the C++ pairing — Plan B v2 copies the corpus (`8bb63224…`), the paired re-pin (#43 plan Task 10) writes the `want_sql_cpp` pins, the differential shows zero divergences, and both repositories pin the same SHA-256. Only then is v0.14.0 released, followed by rewriter-grpc v0.16.0. Native service of sentio driver traffic additionally waits for the view-COMMENT follow-up and a fix for the `startsWith` respelling.
