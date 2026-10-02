# rewriter-go Whole-Statement Parse Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** rewriter-go refuses a statement that Polyglot did not consume in full, instead of rewriting and answering `Success` for the part it parsed. This is a release gate for rewriter-go v0.14.0.

**Architecture:** One engine helper, `engine.CheckParsedInFull(e, sql, ast)`, measures consumption, because Polyglot reports no position. It checks two things. Every bracket token must be closed by its own kind. Cutting the SQL before its last non-bracket token must change the AST: a byte-identical AST means the parser stopped at or before that token, or discarded it. Every entry point that generates SQL from user input calls the helper right after `ParseOne`. `rewriteStatement` refuses with T7 `UnsupportedStatement` / `statement is not supported` in every mode, before T2. `doMaterializeSQL` answers `MaterializeSyntaxError`. `AnalyzeSnapshot` returns its `INVALID_INPUT` statement refusal. The six shared-corpus `WITH OFFSET` cases, which relied on Polyglot dropping `WITH OFFSET`, leave the shared corpus and are pinned natively.

**Tech Stack:** Go 1.25, Polyglot FFI v0.13.0 asset (`POLYGLOT_SQL_FFI_PATH`), the rewriter-go harness (`internal/harness`), Python 3 (the corpus edit script, not committed), `clickhouse local` 26.2 / 26.7.5.10 via docker (parse check), `gh`.

**Spec:** `/Users/uranuswch/src/tableref-docs/2026-09-26-table-reference-hardening-design.md`. This is the housegate `docs/superpowers/specs/2026-09-26-table-reference-hardening-design.md`. Read §1 (the invariant), §5 (the policy table and precedence, and the "Mutation expressions…" paragraph: "In dynamic mode every engine-internal error … is a coded `UnsupportedStatement` / `statement is not supported` rejection") and §10.1. The #43 plan's decision G3 applies here: "a statement ClickHouse cannot parse reads nothing, whatever the engine says". That plan is `/Users/uranuswch/src/tableref-docs/2026-09-29-table-reference-hardening-engine-go-followup.md`.

**Origin:** The final review of rewriter-go PR #43 found this defect, and it is pre-existing on `main`. The pinned Polyglot's ClickHouse dialect ends `Parser::parse` with "ClickHouse fallback: consume unconsumed tokens until semicolon/EOF" (`third_party/polyglot-src/crates/polyglot-sql/src/parser.rs:1029-1040`). The same loop consumes and discards a trailing statement-level `SETTINGS` / `FORMAT` (`parser.rs:988-1011`). Polyglot also closes an open bracket at end of input. rewriter-grpc uses ClickHouse's parser and answers `SyntaxError` for every input ClickHouse refuses.

**Repository and working copy:**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git fetch origin
git worktree add -b fix/whole-statement-parse-gate \
  /Users/uranuswch/Dev/housegate/rewriter-go.fix-whole-statement-parse-gate origin/main
cd /Users/uranuswch/Dev/housegate/rewriter-go.fix-whole-statement-parse-gate
git submodule update --init third_party/polyglot-src
export POLYGLOT_SQL_FFI_PATH=$HOME/Library/Caches/housegate/rewriter-ffi/v0.13.0/libpolyglot_sql_ffi.dylib
```

`origin/main` is `57a87ee` (the PR #43 squash; its tree equals #43's head `248ecf2`). Every path below is relative to this worktree. Every snippet, expected value and fingerprint in this plan was run in a throwaway worktree of `57a87ee`. Both CI lanes were green there, and every new test was red before its implementation step.

## Global Constraints

- CI runs `go vet ./...` and `env SNAPSHOT_QUERY_ORDINARY=1 go test ./...`, with and without `POLYGLOT_SQL_FFI_PATH`. Both must pass at every commit. Without `SNAPSHOT_QUERY_ORDINARY=1`, the snapshot-query tests fail on `SNAPSHOT_MEASURED_FFI`. That is environment, not a regression.
- The corpus is edited only through `tmp/corpus_parse_gate.py` (Task 0). `tmp/` is git-ignored. The script deletes the six named elements and keeps every other byte. Copy the three constants it prints into `internal/harness/sicorpus_test.go` (`SICorpusFingerprint`, `SICorpusBytes`, `SICorpusCases`). The result must be sha256 `8bb63224934bf60794ebc3569c0b1c4b53370abdebb2c280e150423b0228abb7`, 864998 bytes, 960 cases, FNV-1a/64 `17952022305366168916`. If you get a different value, the base or the script differs: stop.
- Do not run `UPDATE_GOLDEN`. Do not add `want_sql_cpp` / `allow_sql_divergence`. This PR adds no shared case and changes no surviving pin.
- The five message families are cross-engine contract texts, listed in rewriter-go `AGENTS.md` under "Message families". This plan adds no text to them. The gate's rewrite refusal reuses T7 `engine.UnsupportedStatementMessage`. Its detailed error text appears only in `MaterializeSQL` messages and Go errors. That text is engine-local and is never pinned cross-engine.
- Corpus schema: no new keys. Every shared input must parse under ClickHouse 26.3 with `implicit_select = 0`, and the six grandfathered exceptions that remain are listed in Task 4.
- Do not reformat unrelated files. `gofmt -l .` already flags `internal/handlers/dblevel.go` on `origin/main`; leave it alone.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Markdown has no hard line-wrapping.
- Spec §11: ask before merging the PR and before any release.

## Review Focus

These are inputs the change could get wrong. Each line names the test that pins it.

1. **Driver traffic that ends in a statement-level `SETTINGS`.** `DROP TABLE … SETTINGS`, `CREATE DATABASE … SETTINGS` and `DELETE FROM … SETTINGS lightweight_deletes_sync = 2` are valid ClickHouse. Polyglot used to drop the `SETTINGS` silently, and these statements are now refused. That is correct under §1, but the sentio indexer driver must not depend on them. Task 2: rows in `TestWholeStatementParseGate`. Before release, check driver logs on devnet2 for `statement is not supported` (Task 5, step 6).
2. **A legitimate statement ending in a bracket or a recorded keyword.** Polyglot closes brackets at end of input, so a trailing `)` must never decide the check. `ASC`, a `WINDOW (…)` clause, nested calls and `{p:String}` / `[…]` / `{'a': 1}` must pass. Task 1: the "Parsed in full" rows of `TestCheckParsedInFull`.
3. **`INSERT … FORMAT <name>` with an inline payload.** The payload is data, and `GenerateInsert` splices it back, so an unbalanced `(` inside CSV must not trip the bracket rule. Task 1: rows "format payload is data" and "json payload is data".
4. **`CREATE TABLE … EMPTY AS SELECT`.** Polyglot truncates it at `EMPTY`. The gate must run on the `EMPTY`-stripped text, so the #43 fix keeps working. The clone form, which cannot be stripped, must be refused, not rewritten without its body. Task 2: the "empty form" rows and the clone-form row of `TestWholeStatementParseGate`.
5. **A tokenizer failure and the SI annotation.** A statement the gate cannot tokenize is refused. With the SI surface active, the final annotation still names an SI object the statement proves: `TRUNCATE ALL TABLES FROM hg_unsafe` keeps the pinned message of `si_truncate_all_tables_from_unsafe_rejected`, and the live-view classifier still runs first. Task 2: the `parse gate cannot tokenize` subtest, the `truncate all tables` rows, and the unchanged `TestStorageIntegrityContract_InvalidLiveViewShapesNeverBypassFailClosed`.

## Decisions this plan records

**Measured truncation classes.** The inputs were 104 hand-built statements over every statement family, each placing an unknown bare word, a second statement, an unclosed bracket or a statement-level `SETTINGS` / `FORMAT` at a clause boundary. Each was run through Polyglot, through `doRewrite` before and after the gate, and through `formatQuerySingleLineOrNull` on ClickHouse 26.2 and 26.7.5.10, which agreed on every row.

| Class | Inputs | Truncated | Silent before (Success) | Refused before | ClickHouse refuses / accepts |
|---|---|---|---|---|---|
| CREATE TABLE (engine, columns, ORDER BY, PARTITION BY, TTL, SETTINGS, COMMENT, AS SELECT, clone, second statement) | 12 | 12 | 12 | 0 | 12 / 0 |
| CREATE TABLE … EMPTY clone form | 1 | 1 | 1 | 0 | 1 / 0 |
| CREATE VIEW / MATERIALIZED VIEW (header, TO, ENGINE, body) | 5 | 5 | 5 | 0 | 5 / 0 |
| ALTER structured actions (ADD / DROP COLUMN, action list) | 3 | 3 | 3 | 0 | 3 / 0 |
| INSERT (VALUES tail, SELECT body, column list, second statement) | 4 | 4 | 4 | 0 | 3 / 1 |
| INSERT … FROM INFILE | 1 | 1 | 0 | 1 | 0 / 1 |
| SELECT (alias, WHERE, LIMIT, SETTINGS, FORMAT, UNION, CTE, second statement) | 10 | 10 | 10 | 0 | 10 / 0 |
| SELECT forms Polyglot drops (`LIMIT … WITH TIES`, `INTO OUTFILE`, `WITH OFFSET` ×2, `FETCH FIRST … ONLY`) | 5 | 5 | 5 | 0 | 4 / 1 |
| Unclosed bracket at end of input | 5 | 5 | 5 | 0 | 4 / 1 |
| Statement-level SETTINGS / FORMAT (DROP, CREATE DATABASE, DELETE, UPDATE, CREATE TABLE, double SETTINGS) | 7 | 7 | 7 | 0 | 0 / 7 |
| DROP / TRUNCATE / CREATE-DROP DATABASE / DELETE / UPDATE + junk | 7 | 7 | 6 | 1 | 7 / 0 |
| TRUNCATE ALL TABLES / TRUNCATE VIEW | 2 | 2 | 0 | 2 | 0 / 2 |
| **Total truncated** | | **62** | **58** | **4** | **49 / 13** |

The rest of the 104 inputs were not truncated: 17 ordinary ClickHouse statements Polyglot parses in full, and 10 `command`-node statements (RENAME, EXCHANGE, USE, SHOW, DESCRIBE, EXISTS, GRANT, SET, OPTIMIZE and `ALTER … DELETE/UPDATE/MODIFY SETTING` + junk). A `command` node keeps the whole text in the generated SQL, so nothing is dropped silently. One residual is out of scope: `TRUNCATE TABLE t SETTINGS …` is parsed in full, but the generator drops `SETTINGS`. The gate does not see it, because consumption is not the failure there.

A sweep of every SQL string in the repository's corpora found no false positive: 1255 inputs from the seven harness JSON corpora plus `internal/corpus/testdata/seed.sql`. It flagged 12, all real truncations. Six were the `WITH OFFSET` cases. Two were `CREATE TABLE … EMPTY` inputs, which `doRewrite` strips before the gate. The other four were `TRUNCATE ALL TABLES FROM hg_unsafe`, `INSERT … FROM INFILE`, `TRUNCATE VIEW db.v`, and `FUTURE SNAPSHOT QUERY`. All four were already refusals, and each still gives the same answer. None of housegate's startup-probe inputs, or the probe inputs in the housegate table-reference plan, is gated. Cost: one `Tokenize` and one `ParseOne` per statement. Over the 687 distinct shared-corpus inputs, `doRewrite` time rose by about 4%. The binary search that names the stop point runs only on the refusal path.

| Question | Decision | Basis |
|---|---|---|
| Refusal code and text | `UnsupportedStatement` / T7 `statement is not supported`, in every mode and both SI states. `finalize` stamps `existence_clause`, clears `statement_type`, echoes the SQL, and (surface active) lets the SI annotation name a proven SI object | 13 of the 62 truncated inputs are valid ClickHouse. The engine cannot tell a ClickHouse syntax error from a Polyglot grammar gap, and spec §5 makes an engine-internal limit a coded `UnsupportedStatement` / `statement is not supported`. `SyntaxError` would also flip three agreed shared pins (`si_truncate_all_tables_from_unsafe_rejected`, `si_tr_insert_from_infile{,_v2}_rejected`). The T7 text keeps them, because `INSERT … FROM INFILE` already uses the T7 text in both SI states |
| Why not the SI catch-all under an active surface | The truncated part is "a position no rewrite reaches" (T7), not an unmodelled class. The SI catch-all would flip `si_tr_insert_from_infile_v2_rejected` | AGENTS.md T7 family |
| Precedence | After request validation, `ParseOne` and (surface active) the live-view classifier; before T2 | "A parse failure precedes everything": no check can see the part that was not parsed. The live-view classifier reads the whole text and keeps its pinned catch-all (`TestStorageIntegrityContract_InvalidLiveViewShapesNeverBypassFailClosed`) |
| MaterializeSQL | `MaterializeSyntaxError` with the detailed message; SQL echoed | Before the gate it returned the materialized prefix as `Success`, so the agent would sign and forward a statement the client never wrote |
| Snapshot analyser | `snapshotStatement` (`INVALID_INPUT`) | It prepared the parsed prefix and dropped the rest |
| Shared cases added | None | Every truncated input ClickHouse refuses gets `SyntaxError` from rewriter-grpc, with ClickHouse's position-bearing message. Neither the code nor the text can match, and the corpus admits only ClickHouse-parseable inputs. Every valid-ClickHouse truncated input is either already pinned with an agreed answer or is one the native engine cannot model (engine-local, #43 G2) |
| `WITH OFFSET` ×6 (`si_with_offset{,_newline,_tab}_rejected`, `si_mixed_ordinary_with_offset_allowed`, `si_comma_si_with_offset_rejected`, `si_comma_ordinary_with_offset_allowed`) | Leave the shared corpus; pinned in `native_parse_gate_test.go` | ClickHouse refuses them (they were grandfathered). Polyglot never parsed `WITH OFFSET`: `want_sql_go` of `si_mixed_ordinary_with_offset_allowed` had lost the JOIN's `ON 1`. rewriter-grpc agreed only through its `StorageIntegritySQLCompatibility` shim, which mirrors the Go behaviour |
| `engine.WithOffsetTargets` / `HasWithOffset` and the SI select check | Kept, unreachable from `doRewrite` | Smallest change; their unit tests still pass. Removal is a follow-up |

## Pin sequencing across the two repositories

| Lands in | What |
|---|---|
| **This PR** (rewriter-go `fix/whole-statement-parse-gate`) | The gate, the native pins, the six deletions, and the corpus re-pin from `76d8db60…` / 872362 / 966 / `10342073271660396945` to `8bb63224934bf60794ebc3569c0b1c4b53370abdebb2c280e150423b0228abb7` / 864998 / 960 / `17952022305366168916`. No SQL pins change. v0.14.0 is not released. |
| **Plan B v2** (rewriter-grpc) | Copies the corpus from this PR once it is merged, byte for byte, instead of `76d8db60…`. The change from `76d8db60…` is deletion only, so no C++ runner expectation changes: the six `WITH OFFSET` rows disappear, including two `allow_sql_divergence` rows and the `si_comma_si_with_offset_rejected` M-order row of the divergence inventory. Plan B v2 sets `kCorpusFingerprint` / `kCorpusBytes` / `kCorpusCases` to `17952022305366168916` / `864998` / `960`. If it has already copied `76d8db60…`, it re-copies the file in the same PR. It may delete the `WITH OFFSET` branch of `StorageIntegritySQLCompatibility`, since no shared case exercises it any more. rewriter-grpc then answers ClickHouse's `SyntaxError`. Keeping the branch is also safe. |
| **Paired re-pin** (#43 plan Task 10, run with Plan B v2) | Starts from this PR's corpus: `UPDATE_GOLDEN=1` against the Plan B build writes the `want_sql_cpp` pins into `8bb63224…`'s file. The differential is re-run on 960 cases until it shows zero divergences. Both repositories then pin the same resulting SHA-256. rewriter-go v0.14.0 is released after that, then rewriter-grpc v0.16.0. |

## File Structure

| File | Responsibility |
|---|---|
| `internal/engine/wholestmt.go` (create) | `ErrNotParsedInFull`, `CheckParsedInFull` and its bracket / stop-point helpers |
| `internal/engine/wholestmt_test.go` (create) | Unit pins for the helper, and the snapshot-analyser pin |
| `native.go` (modify) | The gate in `rewriteStatement` |
| `native_parse_gate_test.go` (create) | Native pins of the refusal in every mode, including the six former corpus cases, plus the MaterializeSQL pin |
| `native_test.go` (modify) | `fakeEngine.tokenizeOK`, so the fake tokenizer can succeed for the gate's call, and the gate's fail-closed subtest |
| `materialize.go`, `internal/engine/snapshot_query.go` (modify) | The gate at the other two entry points |
| `internal/harness/testdata/storage_integrity_cases.json`, `internal/harness/sicorpus_test.go` (modify) | Six deletions and the fingerprint |
| `AGENTS.md`, `internal/harness/AGENTS.md`, `internal/engine/AGENTS.md` (modify) | Documentation |

---

### Task 0: base, baseline, and the corpus edit script

**Files:**
- Create (git-ignored): `tmp/corpus_parse_gate.py`

- [ ] **Step 1: Confirm the base**

```bash
git log -1 --format=%h                                          # 57a87ee
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json
# 76d8db60921387db62452fd27661211da62e06fbbba894c90af5f7d7ccd230e0
go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
```

Expected: both lanes `ok` for every package.

- [ ] **Step 2: Write `tmp/corpus_parse_gate.py`**

```python
#!/usr/bin/env python3
"""Whole-statement parse gate: WITH OFFSET cases leave the shared corpus.

Usage: python3 tmp/corpus_parse_gate.py internal/harness/testdata/storage_integrity_cases.json

Deletes the named elements and keeps every other byte. Prints the three
constants for internal/harness/sicorpus_test.go.
"""
import json
import sys

PATH = sys.argv[1]
raw = open(PATH, encoding="utf-8").read()
dec = json.JSONDecoder()

DELETE = {
    "si_with_offset_rejected",
    "si_with_offset_newline_rejected",
    "si_with_offset_tab_rejected",
    "si_mixed_ordinary_with_offset_allowed",
    "si_comma_si_with_offset_rejected",
    "si_comma_ordinary_with_offset_allowed",
}


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


elems = list(elements(raw))
names = {obj["name"] for _, _, obj in elems}
missing = DELETE - names
if missing:
    sys.exit("unknown case names: %s" % sorted(missing))
if elems[0][2]["name"] in DELETE:
    sys.exit("the first element cannot be deleted by this script")

out, cursor = [], 0
for start, end, obj in elems:
    if obj["name"] in DELETE:
        prev = raw.rindex("}", 0, start) + 1  # drop the separator before it
        out.append(raw[cursor:prev])
        cursor = end
out.append(raw[cursor:])
text = "".join(out)
cases = json.loads(text)
assert len(cases) == len(elems) - len(DELETE)
open(PATH, "w", encoding="utf-8").write(text)
data = text.encode("utf-8")
fnv = 14695981039346656037
for b in data:
    fnv = ((fnv ^ b) * 1099511628211) & 0xFFFFFFFFFFFFFFFF
print("SICorpusFingerprint uint64 = %d" % fnv)
print("SICorpusBytes       int    = %d" % len(data))
print("SICorpusCases       int    = %d" % len(cases))
```

- [ ] **Step 3: Prove the script reaches the target bytes (dry run on a copy)**

```bash
cp internal/harness/testdata/storage_integrity_cases.json tmp/dry.json
python3 tmp/corpus_parse_gate.py tmp/dry.json
shasum -a 256 tmp/dry.json && rm tmp/dry.json
```

Expected: `SICorpusFingerprint uint64 = 17952022305366168916`, `SICorpusBytes       int    = 864998`, `SICorpusCases       int    = 960`, sha256 `8bb63224934bf60794ebc3569c0b1c4b53370abdebb2c280e150423b0228abb7`. `diff` against the original shows exactly four deleted hunks (`972,1045d971`, `1069,1105d994`, `4221,4274d4109`, `4428,4526d4262`) and nothing else. Nothing to commit.

---

### Task 1: `engine.CheckParsedInFull`

**Files:**
- Create: `internal/engine/wholestmt.go`
- Test: `internal/engine/wholestmt_test.go`

**Interfaces:**
- Consumes: `tokenizeRaw(e Engine, sql string) ([]rawToken, error)` (byte-offset spans) and `insertHasFormatClause(ast AST) bool`, both existing in `internal/engine/writes.go`.
- Produces: `var ErrNotParsedInFull error` and `func CheckParsedInFull(e Engine, sql string, ast AST) error`. It returns nil when `ast` (from `e.ParseOne(sql)`) accounts for the whole statement, an error wrapping `ErrNotParsedInFull` when it does not, and the tokenizer's error as is. Its error texts start with `engine: parse: statement was not parsed in full`.

- [ ] **Step 1: Write the failing test** — `internal/engine/wholestmt_test.go`:

```go
package engine

import (
	"errors"
	"testing"
)

func TestCheckParsedInFull(t *testing.T) {
	e := newTestEngine(t)
	for _, tc := range []struct {
		name string
		sql  string
		want string // "" = parsed in full; otherwise the exact error text
	}{
		// Parsed in full.
		{"select literal", "SELECT 1", ""},
		{"order by asc", "SELECT * FROM t ORDER BY a ASC", ""},
		{"nested calls end in brackets", "SELECT f(g(a)) FROM t", ""},
		{"window ends in bracket", "SELECT * FROM t WINDOW w AS (ORDER BY a)", ""},
		{"brackets of every kind", "SELECT {p:String}, [1, 2], {'a': 1}, (1)", ""},
		{"alias after comment", "SELECT * FROM t /* c */ XYZ", ""},
		{"trailing semicolons", "SELECT 1;;", ""},
		{"trailing line comment", "SELECT * FROM t -- c", ""},
		{"values", "INSERT INTO t VALUES (1), (2)", ""},
		{"format without payload", "INSERT INTO t FORMAT CSV", ""},
		{"format payload is data", "INSERT INTO t FORMAT CSV 1,(2", ""},
		{"json payload is data", `INSERT INTO t FORMAT JSONEachRow {"a":1}`, ""},
		{"table settings", "CREATE TABLE t (a Int32) ENGINE = Memory SETTINGS max_threads = 1", ""},
		{"select settings then format", "SELECT * FROM t SETTINGS max_threads = 1 FORMAT JSON", ""},
		{"command keeps its text", "RENAME TABLE a TO b XYZ", ""},

		// Not parsed in full: the parser stopped early.
		{"engine then junk", "CREATE TABLE db1.n ENGINE = Memory XYZ AS SELECT * FROM phys.x",
			`engine: parse: statement was not parsed in full: the parser stopped before "XYZ AS SELECT * FROM phys.x"`},
		{"order by then junk", "CREATE TABLE db1.n (a Int32) ENGINE = MergeTree ORDER BY a XYZ SETTINGS storage_policy = 'x'",
			`engine: parse: statement was not parsed in full: the parser stopped before "XYZ SETTINGS storage_policy = 'x'"`},
		{"clone with empty", "CREATE TABLE db1.n AS db1.src ENGINE = Memory EMPTY AS SELECT * FROM phys.x",
			`engine: parse: statement was not parsed in full: the parser stopped before "EMPTY AS SELECT * FROM phys.x"`},
		{"two selects", "SELECT * FROM t SELECT * FROM phys.x",
			`engine: parse: statement was not parsed in full: the parser stopped before "SELECT * FROM phys.x"`},
		{"junk before brackets", "SELECT * FROM t AS a XYZ WHERE f(b)",
			`engine: parse: statement was not parsed in full: the parser stopped before "XYZ WHERE f(b)"`},
		{"with ties", "SELECT * FROM t LIMIT 1 WITH TIES",
			`engine: parse: statement was not parsed in full: the parser stopped before "WITH TIES"`},
		{"statement settings discarded", "DROP TABLE t SETTINGS max_threads = 1",
			`engine: parse: statement was not parsed in full: the parser stopped before "SETTINGS max_threads = 1"`},
		{"long tail is cut", "CREATE TABLE db1.n ENGINE = Memory XYZ AS SELECT a, b, c, d, e, f FROM phys.x",
			`engine: parse: statement was not parsed in full: the parser stopped before "XYZ AS SELECT a, b, c, d, e, f FROM phys…"`},

		// Not parsed in full: Polyglot closed a bracket the input left open.
		{"unclosed subquery", "SELECT * FROM (SELECT 1",
			`engine: parse: statement was not parsed in full: "(" is never closed in "(SELECT 1"`},
		{"unclosed in list", "SELECT * FROM t WHERE a IN (1, 2",
			`engine: parse: statement was not parsed in full: "(" is never closed in "(1, 2"`},
		{"stray closer", "SELECT (1))",
			`engine: parse: statement was not parsed in full: unmatched ")" before ")"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := e.ParseOne(tc.sql)
			if err != nil {
				t.Fatalf("ParseOne: %v", err)
			}
			err = CheckParsedInFull(e, tc.sql, ast)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if err == nil || err.Error() != tc.want {
				t.Fatalf("err = %v, want %s", err, tc.want)
			}
			if !errors.Is(err, ErrNotParsedInFull) {
				t.Fatalf("err = %v does not wrap ErrNotParsedInFull", err)
			}
		})
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/engine -run '^TestCheckParsedInFull$' -count=1`
Expected: FAIL to compile, with `undefined: CheckParsedInFull` and `undefined: ErrNotParsedInFull`.

- [ ] **Step 3: Write the implementation** — `internal/engine/wholestmt.go`:

```go
package engine

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrNotParsedInFull marks a statement Polyglot accepted without consuming
// all of it. The pinned Polyglot's ClickHouse dialect skips every token after
// the point where its grammar stops (parser.rs `parse`, "ClickHouse fallback:
// consume unconsumed tokens until semicolon/EOF"), discards a statement-level
// SETTINGS / FORMAT tail it does not model, and closes any bracket the input
// left open at end of input. The AST, and every SQL generated from it, then
// means something other than the input.
var ErrNotParsedInFull = errors.New("engine: parse: statement was not parsed in full")

// CheckParsedInFull returns nil when ast, which e.ParseOne produced for sql,
// accounts for the whole statement, and an error wrapping ErrNotParsedInFull
// otherwise. A tokenizer error is returned as is.
//
// Polyglot exposes no consumed position, so the check measures it:
//
//  1. Brackets. Every '(' '[' '{' token is closed by its own kind, in order,
//     before the end of the statement.
//  2. The final token. Cut sql just before its last token that is neither a
//     bracket nor a semicolon. If the cut text parses to a byte-identical AST,
//     that token did not contribute to the AST: the parser stopped at or
//     before it, or discarded it. The cut keeps every earlier token at its
//     offset, so the spans inside the two ASTs compare byte for byte.
//     Brackets are skipped because Polyglot closes an open bracket at end of
//     input, so removing a trailing ')' alone never changes the AST.
//
// An INSERT … FORMAT <name> statement is checked only up to the format name:
// the rest is the data payload, which Polyglot does not model and
// GenerateInsert splices back verbatim.
func CheckParsedInFull(e Engine, sql string, ast AST) error {
	toks, err := tokenizeRaw(e, sql)
	if err != nil {
		return err
	}
	if insertHasFormatClause(ast) {
		for i := len(toks) - 1; i >= 0; i-- {
			if toks[i].TokenType == "FORMAT" && i+1 < len(toks) {
				toks = toks[:i+2]
				break
			}
		}
	}
	for len(toks) > 0 && toks[len(toks)-1].TokenType == "SEMICOLON" {
		toks = toks[:len(toks)-1]
	}
	if err := checkBrackets(sql, toks); err != nil {
		return err
	}
	last := -1
	for i := len(toks) - 1; i >= 0; i-- {
		if !isBracketToken(toks[i].TokenType) {
			last = i
			break
		}
	}
	if last <= 0 {
		return nil // no earlier token to cut back to
	}
	cut, perr := e.ParseOne(sql[:toks[last].Span.Start])
	if perr != nil || !bytes.Equal(cut, ast) {
		return nil
	}
	stop := firstIgnoredToken(e, sql, ast, toks[:last+1])
	return fmt.Errorf("%w: the parser stopped before %q", ErrNotParsedInFull, excerpt(sql[toks[stop].Span.Start:]))
}

// firstIgnoredToken finds, by binary search over the cut points of toks, the
// first token whose removal (with everything after it) leaves ast unchanged,
// then skips closing brackets, which Polyglot supplies itself at end of input.
// toks[len(toks)-1] is known to be ignored. The result names the stop point
// in the message only; it never decides whether the statement is refused.
func firstIgnoredToken(e Engine, sql string, ast AST, toks []rawToken) int {
	lo, hi := 1, len(toks)-1
	for lo < hi {
		mid := (lo + hi) / 2
		cut, err := e.ParseOne(sql[:toks[mid].Span.Start])
		if err == nil && bytes.Equal(cut, ast) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	for hi < len(toks)-1 && isClosingBracket(toks[hi].TokenType) {
		hi++
	}
	return hi
}

func checkBrackets(sql string, toks []rawToken) error {
	var open []rawToken
	for _, tk := range toks {
		switch tk.TokenType {
		case "L_PAREN", "L_BRACKET", "L_BRACE":
			open = append(open, tk)
		case "R_PAREN", "R_BRACKET", "R_BRACE":
			if len(open) == 0 || closerOf(open[len(open)-1].TokenType) != tk.TokenType {
				return fmt.Errorf("%w: unmatched %q before %q", ErrNotParsedInFull, tk.Text, excerpt(sql[tk.Span.Start:]))
			}
			open = open[:len(open)-1]
		}
	}
	if len(open) > 0 {
		tk := open[len(open)-1]
		return fmt.Errorf("%w: %q is never closed in %q", ErrNotParsedInFull, tk.Text, excerpt(sql[tk.Span.Start:]))
	}
	return nil
}

func closerOf(open string) string {
	switch open {
	case "L_PAREN":
		return "R_PAREN"
	case "L_BRACKET":
		return "R_BRACKET"
	}
	return "R_BRACE"
}

func isClosingBracket(t string) bool { return t == "R_PAREN" || t == "R_BRACKET" || t == "R_BRACE" }

func isBracketToken(t string) bool {
	return isClosingBracket(t) || t == "L_PAREN" || t == "L_BRACKET" || t == "L_BRACE"
}

// excerpt is the first 40 characters of s, for messages.
func excerpt(s string) string {
	const max = 40
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}
```

- [ ] **Step 4: Run the test to verify it passes, and both lanes**

```bash
go test ./internal/engine -run '^TestCheckParsedInFull$' -count=1 -v
go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
```

Expected: all 26 subtests PASS, and both lanes are `ok`. Nothing calls the helper yet.

- [ ] **Step 5: Commit**

```bash
git add internal/engine/wholestmt.go internal/engine/wholestmt_test.go
git commit -m "feat(engine): CheckParsedInFull measures whether Polyglot consumed a statement

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: the rewrite pipeline refuses a statement not parsed in full

**Files:**
- Modify: `native.go` (`rewriteStatement`, after the live-view classification block and before `handlers.PreflightTableReferences`)
- Modify: `native_test.go` (`fakeEngine`, `TestStorageIntegrityLiveViewClassifierErrorFailsClosedOnlyForStructuredCreateView`)
- Create: `native_parse_gate_test.go`
- Modify: `internal/harness/testdata/storage_integrity_cases.json` (only through `tmp/corpus_parse_gate.py`), `internal/harness/sicorpus_test.go:250-254`

**Interfaces:**
- Consumes: `engine.CheckParsedInFull` and `engine.UnsupportedStatementMessage`. From the existing root test helpers: `tablerefCase`, `runTablerefCases`, `tablerefOpts(si bool)`, `tableRewriteStatic()`, `newEngine(t)`, and `doRewrite(e, sql, opts) (*pb.RewriteSQLResponse, error)`.
- Produces: `fakeEngine.tokenizeOK int`, the number of initial `Tokenize` calls that succeed before `tokenizeErr` applies. Task 3 relies on nothing here.

- [ ] **Step 1: Write the failing native pins** — `native_parse_gate_test.go` (the MaterializeSQL test is added in Task 3):

```go
package rewriter

import (
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)

// TestWholeStatementParseGate pins the refusal of a statement Polyglot did not
// consume in full. Every input here is one ClickHouse refuses or one whose
// meaning the pinned Polyglot changes, so none of them can live in the shared
// corpus: rewriter-grpc answers them with ClickHouse's own parser.
func TestWholeStatementParseGate(t *testing.T) {
	const unsupported = "statement is not supported"
	var cases []tablerefCase
	for _, si := range []bool{false, true} {
		refused := func(sql string) tablerefCase {
			return tablerefCase{name: sql, sql: sql, si: si,
				wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: unsupported, wantSQL: sql}
		}
		for _, sql := range []string{
			// The PR #43 review findings.
			"CREATE TABLE db1.n ENGINE = Memory XYZ AS SELECT * FROM phys.x",
			"CREATE TABLE db1.n (a Int32) ENGINE = MergeTree ORDER BY a XYZ SETTINGS storage_policy = 'x'",
			"CREATE TABLE db1.n AS db1.src ENGINE = Memory EMPTY AS SELECT * FROM phys.x",
			// Every statement family truncates the same way.
			"CREATE VIEW db1.v AS SELECT * FROM db1.o AS a XYZ",
			"CREATE MATERIALIZED VIEW db1.mv TO db1.o XYZ AS SELECT * FROM db1.p",
			"ALTER TABLE db1.o ADD COLUMN c Int32 DEFAULT 1 XYZ, DROP COLUMN d",
			"INSERT INTO db1.o SELECT * FROM db1.p SELECT * FROM phys.x",
			"INSERT INTO db1.o VALUES (1) XYZ",
			"SELECT * FROM db1.o SELECT * FROM phys.x",
			"SELECT * FROM db1.o LIMIT 1 WITH TIES",
			"DROP TABLE db1.o SETTINGS max_threads = 1",
			"DELETE FROM db1.o WHERE a = 1 SETTINGS lightweight_deletes_sync = 2",
			"CREATE DATABASE db2 SETTINGS max_threads = 1",
			"DELETE FROM db1.o WHERE a = 1 XYZ",
			// Polyglot closes a bracket the input left open.
			"SELECT * FROM db1.o WHERE a IN (1, 2",
			// Left the shared corpus with this change (spec 2026-09-26 §1:
			// ClickHouse cannot parse WITH OFFSET; Polyglot dropped it, and
			// with it a JOIN's ON clause).
			"SELECT * FROM db1.t WITH OFFSET AS off",
			"SELECT * FROM db1.t WITH\nOFFSET AS off",
			"SELECT * FROM db1.t WITH\tOFFSET AS off",
			"SELECT * FROM db1.t AS s JOIN db1.o WITH OFFSET AS off ON 1",
			"SELECT * FROM db1.o, db1.t WITH OFFSET AS off",
			"SELECT * FROM db1.t, db1.o WITH OFFSET AS off",
			// The gate precedes T2 and T3: no policy check sees a statement
			// the engine did not parse in full.
			"SELECT * FROM {p:Identifier} AS a XYZ",
			"SELECT * FROM db1.o AS a XYZ JOIN phys.x ON 1",
		} {
			cases = append(cases, refused(sql))
		}
		// With the surface active the final annotation still names an SI
		// object the statement proves.
		hgUnsafe := tablerefCase{name: "truncate all tables", sql: "TRUNCATE ALL TABLES FROM hg_unsafe", si: si,
			wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: unsupported, wantSQL: "TRUNCATE ALL TABLES FROM hg_unsafe"}
		hgSafe := refused("SELECT * FROM hg_safe.db1__t AS a XYZ")
		if si {
			hgUnsafe.wantMsg = "storage-integrity physical database hg_unsafe is not directly addressable"
			hgSafe.wantMsg = "storage-integrity physical table hg_safe.db1__t is not directly addressable"
		}
		cases = append(cases, hgUnsafe, hgSafe)
		// CREATE TABLE … EMPTY AS SELECT is parsed without its EMPTY keyword,
		// so it passes the gate.
		cases = append(cases, tablerefCase{name: "empty form", sql: "CREATE TABLE db1.n ENGINE = Memory EMPTY AS SELECT * FROM db1.p", si: si,
			wantCode: pb.RewriteCode_Success,
			wantSQL:  `CREATE TABLE phys."db1.n" ENGINE=Memory EMPTY AS (SELECT * FROM phys."db1.p" "db1.p")`})
	}
	runTablerefCases(t, cases)
}

// The gate refuses in every mode, through the response, never the Go error.
func TestWholeStatementParseGateEveryMode(t *testing.T) {
	e := newEngine(t)
	const sql = "CREATE TABLE db1.n ENGINE = Memory XYZ AS SELECT * FROM phys.x"
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
				resp.GetSqlAfterRewrite() != sql || resp.GetStatementType() != pb.StatementType_STATEMENT_TYPE_UNSPECIFIED {
				t.Fatalf("resp = %+v", resp)
			}
		})
	}
}
```

- [ ] **Step 2: Add the fail-closed fake-engine subtest** — in `native_test.go`, replace the `fakeEngine` struct and its `Tokenize` method:

```go
type fakeEngine struct {
	parseErr    error
	parseAST    engine.AST
	tokenizeErr error
	// tokenizeOK is the number of initial Tokenize calls that succeed before
	// tokenizeErr applies.
	tokenizeOK    int
	tokenizeCalls int
}
```

```go
func (f *fakeEngine) Tokenize(string) (engine.AST, error) {
	f.tokenizeCalls++
	if f.tokenizeErr != nil && f.tokenizeCalls > f.tokenizeOK {
		return nil, f.tokenizeErr
	}
	return engine.AST("[]"), nil
}
```

In `TestStorageIntegrityLiveViewClassifierErrorFailsClosedOnlyForStructuredCreateView`, insert this subtest immediately before `t.Run("raw live view with folded whitespace", …)`:

```go
	t.Run("parse gate cannot tokenize", func(t *testing.T) {
		e := &fakeEngine{
			parseAST:    engine.AST(`{"select":{}}`),
			tokenizeErr: tokenErr,
		}
		resp, err := doRewrite(e, "SELECT 1", opts)
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetCode() != pb.RewriteCode_UnsupportedStatement || resp.GetMessage() != engine.UnsupportedStatementMessage {
			t.Fatalf("resp = %+v, a statement the gate cannot check must be refused", resp)
		}
	})

```

- [ ] **Step 3: Run the new pins to verify they fail**

```bash
go test . -run '^(TestWholeStatementParseGate|TestWholeStatementParseGateEveryMode)$' -count=1
go test . -run '^TestStorageIntegrityLiveViewClassifierErrorFailsClosedOnlyForStructuredCreateView$/parse_gate_cannot_tokenize' -count=1
```

Expected: FAIL. On the base, the truncated rows answer `Success`, or another refusal code or message. Exactly five rows already pass: both "empty form" rows, the SI-active `truncate all tables` row, and the SI-active `DELETE FROM db1.o WHERE a = 1 XYZ` and `DELETE … SETTINGS lightweight_deletes_sync = 2` rows. The active surface already refuses a structured DELETE. The tokenizer subtest answers `Success`.

- [ ] **Step 4: Implement the gate** — in `native.go` `rewriteStatement`, insert between the closing `}` of the `if siVersion != … { liveViewClass, classifyErr := engine.ClassifyLiveView(…) … }` block and the `// Table-reference policy (spec 2026-09-26 §5): …` comment:

```go
	// Whole-statement parse gate: a statement Polyglot did not consume in
	// full has an AST, and would generate SQL, that means something other
	// than the input, and no later check can see the part that was not
	// parsed. It is refused in every mode, before any policy check, as a
	// position no rewrite reaches (T7 text). finalize still lets the SI
	// annotation name an SI object the statement addresses.
	if cerr := engine.CheckParsedInFull(e, sql, ast); cerr != nil {
		resp.Code = pb.RewriteCode_UnsupportedStatement
		resp.Message = engine.UnsupportedStatementMessage
		finalize(resp, ast, sql, ec, siVersion, e, selection)
		return resp, nil
	}

```

- [ ] **Step 5: Keep the fake-engine tests on their subject** — the gate is now the first `Tokenize` call for a statement the live-view classifier does not tokenize. In the same test function:
  - In `t.Run("unrelated select", …)`, add `tokenizeOK:  1, // the whole-statement parse gate` to the `fakeEngine` literal.
  - In the decoy loop (`raw string literal decoy` … `raw nested block comment decoy`), add `tokenizeOK:  1, // the whole-statement parse gate` to the `fakeEngine` literal. Then replace

```go
			if e.tokenizeCalls != 1 {
				t.Fatalf("Tokenize calls = %d, want final annotation only", e.tokenizeCalls)
```

with

```go
			if e.tokenizeCalls != 2 {
				t.Fatalf("Tokenize calls = %d, want the parse gate and the final annotation only", e.tokenizeCalls)
```

The `create view`, `raw live view with folded whitespace` and "raw live view split by …" subtests do not change: the live-view classifier runs before the gate and refuses them first.

- [ ] **Step 6: Run the root package and the harness**

```bash
go test . -count=1
go test ./internal/harness -run '^TestStorageIntegrityGolden$' -count=1 -v 2>&1 | grep -E '^\s+--- FAIL'
```

Expected: the root package is `ok`. The harness shows exactly six failures: `si_with_offset_rejected`, `si_with_offset_newline_rejected`, `si_with_offset_tab_rejected`, `si_mixed_ordinary_with_offset_allowed`, `si_comma_si_with_offset_rejected` and `si_comma_ordinary_with_offset_allowed`. Each now answers `UnsupportedStatement` / `statement is not supported` where the corpus pins `RewriteError` or `Success`. Those pins recorded the defect: `want_sql_go` of `si_mixed_ordinary_with_offset_allowed` is `… JOIN phys."other.u" "other.u"`, with `WITH OFFSET AS off ON 1` gone. Any other failure is unexpected: stop.

- [ ] **Step 7: The six cases leave the shared corpus**

```bash
python3 tmp/corpus_parse_gate.py internal/harness/testdata/storage_integrity_cases.json
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json
```

Expected: the three constants from Task 0 Step 3 and sha256 `8bb63224934bf60794ebc3569c0b1c4b53370abdebb2c280e150423b0228abb7`. In `internal/harness/sicorpus_test.go`, set:

```go
const (
	SICorpusFingerprint uint64 = 17952022305366168916
	SICorpusBytes       int    = 864998
	SICorpusCases       int    = 960
)
```

- [ ] **Step 8: Run both lanes**

```bash
go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
```

Expected: every package `ok` in both lanes. `TestWritesGolden/truncate_view_reject` still passes, because it pins `UnsupportedStatement` and the gate gives that code in static mode too.

- [ ] **Step 9: Commit**

```bash
git add native.go native_test.go native_parse_gate_test.go \
  internal/harness/testdata/storage_integrity_cases.json internal/harness/sicorpus_test.go
git commit -m "fix: refuse a statement Polyglot did not parse in full

The ClickHouse dialect skips every token after the point where its grammar
stops, discards an unmodelled statement-level SETTINGS / FORMAT tail and
closes brackets left open at end of input; the rewrite then answered Success
for a different statement. rewriteStatement now refuses it as T7 in every
mode, before T2. The six WITH OFFSET cases, whose pins recorded that
truncation, leave the shared corpus for native pins.

Corpus: sha256 8bb63224934bf60794ebc3569c0b1c4b53370abdebb2c280e150423b0228abb7,
864998 bytes, 960 cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: MaterializeSQL and the snapshot analyser refuse it too

**Files:**
- Modify: `materialize.go` (`doMaterializeSQL`, the `e.ParseOne(sql)` call)
- Modify: `internal/engine/snapshot_query.go` (`AnalyzeSnapshot`, the `e.ParseOne(sql)` call)
- Test: `native_parse_gate_test.go`, `internal/engine/wholestmt_test.go`

**Interfaces:**
- Consumes: `engine.CheckParsedInFull` (Task 1); `doMaterializeSQL(e engine.Engine, req *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error)`; `AnalyzeSnapshot(e Engine, sql string, opts SnapshotOptions) (*SnapshotPlan, error)` and the package error `snapshotStatement`.
- Produces: nothing new.

- [ ] **Step 1: Write the failing tests** — append to `native_parse_gate_test.go`, and add `"strings"` to its import block:

```go
// MaterializeSQL must not sign a statement it did not parse in full: before
// the gate it returned the materialized prefix as Success.
func TestMaterializeRefusesStatementNotParsedInFull(t *testing.T) {
	e := newEngine(t)
	now := int64(1_700_000_000_000_000_000)
	for _, sql := range []string{
		"INSERT INTO db1.o SELECT now() FROM db1.p AS a XYZ",
		"INSERT INTO db1.o SELECT now() FROM db1.p SELECT * FROM phys.x",
	} {
		t.Run(sql, func(t *testing.T) {
			resp, err := doMaterializeSQL(e, &pb.MaterializeSQLRequest{Sql: sql,
				Inputs: &pb.MaterializationInputs{NowUnixNs: &now}})
			if err != nil {
				t.Fatal(err)
			}
			if resp.GetCode() != pb.MaterializeCode_MaterializeSyntaxError ||
				!strings.Contains(resp.GetMessage(), "statement was not parsed in full") ||
				resp.GetSqlAfterMaterialization() != sql || len(resp.GetReplacements()) != 0 {
				t.Fatalf("resp = %+v", resp)
			}
		})
	}
}
```

Append to `internal/engine/wholestmt_test.go`:

```go
// The snapshot-query analyser admits only a statement parsed in full: before
// the gate it prepared the parsed prefix and dropped the rest.
func TestSnapshotRefusesStatementNotParsedInFull(t *testing.T) {
	e := newTestEngine(t)
	catalog := []SnapshotTable{
		{Database: "tenant", Name: "copy", ID: "target", Columns: []SnapshotColumn{{Name: "value", Type: "Int64", Ordinary: true}}},
		{Database: "tenant", Name: "events", ID: "source", Columns: []SnapshotColumn{{Name: "value", Type: "Int64", Ordinary: true}}},
	}
	for _, sql := range []string{
		"INSERT INTO tenant.copy SELECT value FROM tenant.events AS e XYZ",
		"INSERT INTO tenant.copy SELECT value FROM tenant.events SELECT * FROM phys.x",
	} {
		t.Run(sql, func(t *testing.T) {
			p, err := AnalyzeSnapshot(e, sql, SnapshotOptions{Database: "tenant", Catalog: catalog})
			if p != nil || !errors.Is(err, snapshotStatement) {
				t.Fatalf("plan = %v, err = %v; want the INVALID_INPUT statement refusal", p, err)
			}
		})
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

```bash
go test . -run '^TestMaterializeRefusesStatementNotParsedInFull$' -count=1
go test ./internal/engine -run '^TestSnapshotRefusesStatementNotParsedInFull$' -count=1
```

Expected: FAIL. MaterializeSQL answers `MaterializeSuccess` with `INSERT INTO db1.o SELECT toDateTime(1700000000) FROM db1.p AS a` (and `… FROM db1.p`). `AnalyzeSnapshot` returns a plan whose `Prepare` gives `SELECT value FROM scratch.r AS e` (and `SELECT value FROM scratch.r`).

- [ ] **Step 3: Implement** — in `materialize.go`, replace

```go
	ast, err := e.ParseOne(sql)
	if err != nil {
		resp.Code = pb.MaterializeCode_MaterializeSyntaxError
```

with

```go
	ast, err := e.ParseOne(sql)
	if err == nil {
		err = engine.CheckParsedInFull(e, sql, ast)
	}
	if err != nil {
		resp.Code = pb.MaterializeCode_MaterializeSyntaxError
```

In `internal/engine/snapshot_query.go` `AnalyzeSnapshot`, replace

```go
	ast, err := e.ParseOne(sql)
	if err != nil {
		return nil, snapshotStatement
	}
```

with

```go
	ast, err := e.ParseOne(sql)
	if err == nil {
		err = CheckParsedInFull(e, sql, ast)
	}
	if err != nil {
		return nil, snapshotStatement
	}
```

- [ ] **Step 4: Run the tests and both lanes**

```bash
go test . -run '^TestMaterializeRefusesStatementNotParsedInFull$' -count=1
go test ./internal/engine -run '^TestSnapshotRefusesStatementNotParsedInFull$' -count=1
go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
env -u POLYGLOT_SQL_FFI_PATH SNAPSHOT_QUERY_ORDINARY=1 go test ./... -count=1
```

Expected: PASS, and every package is `ok` in both lanes. The existing snapshot corpus row `FUTURE SNAPSHOT QUERY tenant.copy` was already a refusal, and nothing in `internal/harness/testdata/snapshot_query_*.json` changes. The Linux-only `SNAPSHOT_MEASURED_FFI` qualification cannot run on macOS. The CI Linux job runs it. The gate only turns an accepted truncated statement into a refusal, so a measured profile cannot gain an admission.

- [ ] **Step 5: Commit**

```bash
git add materialize.go internal/engine/snapshot_query.go native_parse_gate_test.go internal/engine/wholestmt_test.go
git commit -m "fix: MaterializeSQL and the snapshot analyser refuse a statement not parsed in full

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: documentation

**Files:**
- Modify: `AGENTS.md`, `internal/harness/AGENTS.md`, `internal/engine/AGENTS.md`

- [ ] **Step 1: `AGENTS.md`**. Make these exact replacements, one paragraph per line.

In the `internal/engine/create_empty.go` bullet, replace `no `EMPTY` is found, the statement falls through and is rewritten without its body, and Success is returned.` with `no `EMPTY` is found and the whole-statement parse gate refuses the original statement.`

Replace the bullet ``- `native.go` — dispatch order, the final unmodelled-class refusal and the SET carve-out, and `sealStorageIntegrityHandlerError`.`` with two bullets:

```markdown
- `internal/engine/wholestmt.go` — `CheckParsedInFull`, the whole-statement parse gate (see below).
- `native.go` — dispatch order, the whole-statement parse gate, the final unmodelled-class refusal and the SET carve-out, and `sealStorageIntegrityHandlerError`.
```

In the T7 message-family bullet, replace `(UnsupportedStatement): an unmodelled statement class,` with `(UnsupportedStatement): a statement the engine did not parse in full (in every mode and both SI states), an unmodelled statement class,`.

Insert this line directly above `1. T2 — an Identifier parameter …`, after `Precedence (first match wins):` and its blank line:

```markdown
0. Whole-statement parse gate — `engine.CheckParsedInFull` refuses a statement Polyglot did not consume in full with T7 `statement is not supported`, in every mode and both SI states, before any check below can look at an AST that lacks part of the statement. With the surface active the final annotation may still name an SI object the statement proves (`TRUNCATE ALL TABLES FROM hg_unsafe` keeps the SI physical-database message).
```

Replace `the live-view classification in `doRewrite` still runs before T2.` with `the live-view classification in `doRewrite` still runs before the parse gate and T2.`

Insert this section directly above `## ANTI-PATTERNS`:

```markdown
## Whole-statement parse gate

The pinned Polyglot's ClickHouse dialect accepts statements it has not consumed: `Parser::parse` skips every token after the point where its grammar stops ("ClickHouse fallback: consume unconsumed tokens until semicolon/EOF"), discards a statement-level `SETTINGS` / `FORMAT` tail the statement's own grammar does not model, and closes any bracket the input left open at end of input. Generating from that AST produces SQL that means something other than the input: `CREATE TABLE db1.n ENGINE = Memory XYZ AS SELECT * FROM phys.x` became `CREATE TABLE db1.n ENGINE=Memory`, and `… JOIN other.u WITH OFFSET AS off ON 1` lost its `ON` clause.

`engine.CheckParsedInFull(e, sql, ast)` measures consumption, because Polyglot reports no position: every `(` `[` `{` token must be closed by its own kind, and cutting `sql` before its last token that is neither a bracket nor a semicolon must change the AST (a byte-identical AST means the parser stopped at or before that token, or discarded it). An `INSERT … FORMAT <name>` payload is data and is not checked. A tokenizer failure fails closed. The binary search that names the stop point in the error text never decides the outcome.

Every entry point that turns user SQL into generated SQL runs it right after `ParseOne`: `rewriteStatement` (T7 refusal, precedence step 0), `doMaterializeSQL` (`MaterializeSyntaxError` with the detailed message) and `AnalyzeSnapshot` (the `INVALID_INPUT` statement refusal). `StripCreateTableEmpty` parses the original `EMPTY` statement without the gate: the gate runs on the `EMPTY`-stripped text that is actually rewritten.

Known residual: a clause Polyglot parses into the AST but its generator does not print (`TRUNCATE TABLE t SETTINGS …` loses `SETTINGS`) is not a consumption failure and passes the gate.

```

- [ ] **Step 2: `internal/harness/AGENTS.md`** — replace the sentence that begins `The only rows allowed to end in `\N` are the twelve pre-Spec-2026-09-26 cases` (through `si_create_live_view_over_si_rejected`.) with:

```markdown
The only rows allowed to end in `\N` are the six pre-Spec-2026-09-26 cases whose inputs predate this rule and agree across engines: `si_reserved_column_star_rename{,_source}_rejected`, `si_attach_grant_{physical,logical}_table_rejected`, `si_attach_safe_database_rejected`, `si_create_live_view_over_si_rejected`. The six `WITH OFFSET` cases left the corpus with the whole-statement parse gate: Polyglot never parsed `WITH OFFSET`, so the native engine now refuses them while rewriter-grpc answers through its compatibility shim; they are pinned in `native_parse_gate_test.go`.
```

- [ ] **Step 3: `internal/engine/AGENTS.md`** — below the `| Parser guard | `guard.go` | … |` row, add:

```markdown
| Whole-statement parse gate | `wholestmt.go` | `CheckParsedInFull`: refuse a statement Polyglot did not consume in full |
```

Below `- Do not bypass `exceedsNestingDepth` on new parse entrypoints.`, add:

```markdown
- Do not generate SQL from a user statement's AST without `CheckParsedInFull`: the ClickHouse dialect silently skips what it cannot parse.
```

- [ ] **Step 4: Check the ClickHouse parse rule on the new corpus** (the command in `internal/harness/AGENTS.md`, with the docker images on this machine):

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
for v in 26.2 26.7.5.10; do
  docker run --rm -v "$PWD/tmp:/w" --entrypoint clickhouse clickhouse/clickhouse-server:$v \
    local --queries-file /w/chparse.sql | grep '\\N$' | cut -f1
done
```

Expected: for both versions, exactly the six names in Step 2.

- [ ] **Step 5: Commit**

```bash
git add AGENTS.md internal/harness/AGENTS.md internal/engine/AGENTS.md
git commit -m "docs: the whole-statement parse gate, its precedence and the corpus parse rule

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: verification and PR

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

Expected: `PASS`. If the fuzzer wrote new files under `testdata/fuzz`, delete them. Do not commit fuzz output.

- [ ] **Step 3: Corpus identity**

```bash
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json
# 8bb63224934bf60794ebc3569c0b1c4b53370abdebb2c280e150423b0228abb7
git diff origin/main --stat -- internal/harness/testdata/storage_integrity_cases.json
go test ./internal/harness -run 'TestSICorpus(Contract|IsBytePinned)$' -count=1
```

Expected: the hash matches. The diff is deletions only (264 lines). Both tests pass.

- [ ] **Step 4: The differential (when the build box is free)** — the Plan B v2 agent owns the build box. With rewriter-grpc v0.15.0 there as the oracle, run

```bash
POLYGLOT_SQL_FFI_PATH=… REWRITER_ORACLE_ADDR=<host:port> go test ./internal/harness -run '^TestStorageIntegrityGolden$' -count=1
```

Expected: the #43 residual divergences minus the six deleted rows, with no new row. The PR can open without this run. Record in the PR description whether it ran.

- [ ] **Step 5: Push and open the PR**

```bash
git push -u origin fix/whole-statement-parse-gate
gh pr create --repo housegate/rewriter-go --base main --head fix/whole-statement-parse-gate \
  --title "fix: refuse a statement Polyglot did not parse in full" --body-file tmp/pr-body.md
```

`tmp/pr-body.md` states the following:
- the defect and its origin (the #43 final review; `parser.rs:1029-1040`);
- the measured table from "Decisions this plan records";
- the refusal (T7 `UnsupportedStatement` / `statement is not supported` in every mode; `MaterializeSyntaxError`; snapshot `INVALID_INPUT`) and why it is not `SyntaxError`;
- the six deleted cases, and why they leave the corpus;
- the corpus identity: sha256 `8bb63224934bf60794ebc3569c0b1c4b53370abdebb2c280e150423b0228abb7`, 864998 bytes, 960 cases, FNV-1a/64 `17952022305366168916`, a deletion-only change from `76d8db60…`;
- the Plan B v2 sequencing from "Pin sequencing across the two repositories";
- the Review Focus list;
- that the differential of Step 4 ran, or when it will.

End the body with:

```
🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

- [ ] **Step 6: Before release, check driver traffic** — on devnet2 with rewriter-go at this PR's head, search housegate logs for `statement is not supported` rejections from the sentio indexer driver's session. Any `DROP … SETTINGS`, `CREATE DATABASE … SETTINGS` or `DELETE … SETTINGS` found there needs a driver-side change, not a gate exception (spec §12 "Driver traffic").

- [ ] **Step 7: Stop for approval.** Merging this PR needs the user's explicit approval (spec §11). rewriter-go v0.14.0 is not released from this PR. The release waits for the C++ pairing: Plan B v2 copies this corpus (`8bb63224…`), the paired re-pin (#43 plan Task 10) writes the `want_sql_cpp` pins, the differential shows zero divergences, and both repositories pin the same SHA-256. Only then is v0.14.0 released, followed by rewriter-grpc v0.16.0.
