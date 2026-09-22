# Signed inline `INSERT ... VALUES` (Tier 2 of #153) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Sign inline `INSERT ... VALUES` statements in agent mode by materializing, lexically closing and evaluating the rows once through the tenant's ClickHouse, encoding them into Native blocks, rewriting the statement to `FORMAT Native` and forwarding it through a new relay lane, so the existing signed lane, ingress, replay and Arbiter see an ordinary signed streaming INSERT.

**Architecture:** Agent-only, default-off. `sicore` gains a parser and a lexical closure gate; `nativepayload` gains the decoder's inverse; `chproto` gains a rows-bearing Data packet writer; `sistatement` gains an evaluator over a dedicated upstream connection and an inline branch that installs a `SynthesizedInsert` plan; the relay gains `runSynthesizedInsert`, the reverse of `runDeferredInsert`. No server, wire, intake, replay or Arbiter change.

**Tech Stack:** Go, ch-go `proto` (pinned sentioxyz fork), Bazel 9 + Bzlmod, docker-bound integration tests with the pinned clickhouse-go fork and the ClickHouse CLI.

**Spec:** `docs/superpowers/specs/2026-09-23-signed-inline-values-design.md` (decisions D1–D12; every task cites the decision it implements).

## Global Constraints

- Default-off and agent-only: `storage_integrity.agent.inline_values.enabled` defaults to `false`; with the flag off every existing test and every observable behavior is byte-identical to today (D10, D12). The only server-side edit is the behavior-neutral move of the nondeterministic name list into `pkg/storageintegrity` (D3).
- No wire or contract change: `PayloadFormat` stays `clickhouse-native-data-v1`, `StatementKind` stays the INSERT kind, `settings_hash` stays `EmptySettingsHash`, no new `SQL_x_*` setting, `pkg/auth/testdata/statement_jws_v2.json` and `SharedStatementVectorsSHA256` untouched (D6, D7, D10).
- One column-type authority: every type mapping goes through `payloadexec.ResolveColumnProfile`; the encoder is the decoder's inverse and is pinned by round trip and CLI byte identity (D5).
- The signed payload is exactly the client Data packet bytes the relay writes upstream, framed at the upstream codec's negotiated revision (capped at 54470), hashed before `WriteQuery` (D7).
- Fail closed everywhere before signing; nothing is signed on a rejection and no `client_seq` is consumed by a rejected statement (D9); the relay never writes a second empty terminator.
- Materialization is mandatory for the lane (D2); the closure gate is lexical and deny-list based (D3); the evaluator never retries (D4).
- Tests are Bazel-driven: `bazel test //...` for unit targets; docker-bound integration targets are tagged `manual` and listed explicitly in `.github/workflows/ci.yml`; before claiming a regression, diff the failing set against a clean `main`.
- Conventions: English identifiers, comments and messages; Markdown docs one paragraph per line; structured logging via `pkg/log`; errors wrapped with `%w`; conventional commit subjects; every commit ends with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; PR bodies end with `Refs https://github.com/housegate/housegate/issues/153`, `Design: docs/superpowers/specs/2026-09-23-signed-inline-values-design.md` and `🤖 Generated with [Claude Code](https://claude.com/claude-code)`; one isolated worktree (`feat/153-tier2-inline-values`), never the main checkout; one PR per task, task review before merge, merge only on green CI; single serial lane.

---

## Design notes and deviations from the spec's wording

- **`Codec.WriteDataBlock` is realized as `EncodeClientDataPacket`.** The lane hashes the payload before it writes the Query (D7), so the codec method returns the framed packet bytes at its negotiated revision and the lane writes them later with `WriteRawPacket`; nothing else about D7 changes.
- **The two signed lanes share their post-input half.** Task 5 extracts everything from the strict input-complete hook onward (`forwardSignedInsert`: upstream bind, Query, one marker, the sample gate, payload, one terminator, `OnQueryInputComplete`, terminal arbitration) out of `runDeferredInsert` and has `runSynthesizedInsert` call it, instead of duplicating 278 lines of terminal arbitration. The existing deferred-lane suite is the behavior-neutrality gate and runs before any new test is written. `deferredSampleResult` gains `sampleRaw` so the synthesized lane can validate the consumed sample against `plan.SampleColumns`.
- **The agent signer's strict-hook gate widens.** `agent.Plugin.OnQueryInputCompleteStrict` returns early unless a deferred plan is set; Task 4 extends the condition to synthesized plans so `SQL_x_auth_token` is re-minted over the rewritten body.
- **Materialization is recorded, not enforced, by the materialize plugin.** It keeps falling open for the ordinary path and records its outcome in `qctx.Values[plugin.ValuesKeyMaterialized]`; the inline branch enforces D2 by refusing anything but `applied` or `noop`.
- **The cross-field `materialize.enabled` rule lives in `validateAgent`** through the root config, next to the other agent-block rules, rather than in a separate top-level check.
- **`ci.yml` needs no edit**: the new docker test file joins `//pkg/integration:integration_test`, which is already in the explicit list; Task 6 verifies that instead of editing the workflow.
- **The 25.x truncated shape stays exactly as today** (spec D1); Task 5 pins that with a raw-packet test and Task 1's parser returns `ErrNotInlineValues` for it.

---

### Task 1: Inline VALUES parser, lexical closure gate and the shared function-name lists (D3)

**Files:**
- Create: `pkg/storageintegrity/nondeterministic_names.go`, `nondeterministic_names_test.go`, `inline_values.go`, `inline_values_test.go`; `pkg/plugins/storageintegrity/nondeterminism_pin_test.go`
- Modify: `pkg/storageintegrity/sql.go` (`insertDataSource`, lines 95-131; `InsertPayloadEncoding` doc, lines 47-50); `pkg/plugins/storageintegrity/plugin.go` (order rationale, lines 195-199; `containsUnmaterializedNondeterminism`, lines 885-906; delete `isKnownNondeterministicName`, lines 908-970)

**Interfaces:**
- Consumes (already in `pkg/storageintegrity`): `ParseInsertTarget(sql string) (InsertTarget, error)`, `InsertColumnList(sql string) ([]string, bool, error)`, `InlineInsertSettingKeys(sql string) ([]string, error)`, `nextStorageSQLToken`, `isStorageSpace` / `isStorageIdentStart` / `isStorageIdentPart`
- Produces: `type InlineValuesInsert struct{ Target InsertTarget; Columns []string; Rows string }`; `var ErrNotInlineValues`; `func ParseInlineValuesInsert(sql string) (InlineValuesInsert, error)`; `func ValuesClosure(rows string) error`; `func IsKnownNondeterministicName(name string) bool`; `func IsServerStateFunctionName(name string) bool`; `const InlineValuesErrorPrefix = "storage_integrity inline VALUES: "`

No BUILD hand-edit: the new files add no dependency beyond `errors`/`fmt`/`strings`, and `//pkg/storageintegrity` is already a dep of `//pkg/plugins/storageintegrity` (lib and test). `srcs` are explicit, so `bazel run //:gazelle` must run *before* the first `bazel test` of a new file, or Bazel compiles the old file set and the step shows a false PASS.

- [ ] **Step 1: Write the failing test for the two shared name lists.** Create `pkg/storageintegrity/nondeterministic_names_test.go`:

```go
package storageintegrity

import "testing"

// movedNondeterministicNames is the name set moved out of
// pkg/plugins/storageintegrity/plugin.go:908-970, byte for byte. Adding or
// removing an entry is a policy change that must also change
// TestContainsUnmaterializedNondeterminism_PinnedAcrossMove there.
var movedNondeterministicNames = []string{
	"any", "anylast", "blocknumber", "blocksize", "curdate", "current_date", "current_timestamp",
	"datetimetouuidv7", "fuzzbits", "fuzzquery", "generaterandomstructure", "generateserialid",
	"generatesnowflakeid", "generateuuidv4", "generateuuidv7", "localtime", "localtimestamp",
	"now", "now64", "nowinblock", "nowinblock64", "obfuscatequery", "quantile", "quantiles",
	"rand", "rand32", "rand64", "randbernoulli", "randbinomial", "randcanonical", "randchisquared",
	"randconstant", "randexponential", "randfisherf", "randlognormal", "randnegativebinomial",
	"randnormal", "randpoisson", "randstudentt", "randuniform", "random", "randomfixedstring",
	"randomprintableascii", "randomstring", "randomstringutf8", "rownumberinallblocks",
	"rownumberinblock", "runningaccumulate", "runningconcurrency", "runningdifference",
	"runningdifferencestartingwithfirstvalue", "today", "utc_timestamp", "utctimestamp",
	"uuidv4", "yesterday",
}

// serverStateNames is the spec D3 deny-list: names whose value is known only
// to the server that executes the statement.
var serverStateNames = []string{
	"dictget", "dictgetstring", "dictgetuint64ordefault", "dictgethierarchy", "joinget",
	"joingetornull", "currentuser", "currentdatabase", "currentschemas", "hostname", "fqdn",
	"version", "uptime", "serveruuid", "getsetting", "getmacro", "tcpport", "hascolumnintable",
	"sleep", "sleepeachrow", "throwif",
}

func TestSharedFunctionNameLists(t *testing.T) {
	if got := len(movedNondeterministicNames); got != 56 {
		t.Fatalf("moved list has %d names, want the 56 of the ingress guard", got)
	}
	// Both take an already-lowercased name, so every case variant must miss.
	groups := []struct {
		label string
		fn    func(string) bool
		yes   []string
		no    []string
	}{
		{"nondeterministic", IsKnownNondeterministicName, movedNondeterministicNames,
			[]string{"", "tostring", "unhex", "randomize", "NOW", "Rand", "dictget"}},
		{"server state", IsServerStateFunctionName, serverStateNames,
			[]string{"", "dict", "dictionaryhelper", "tostring", "DictGet", "Sleep", "now"}},
	}
	for _, g := range groups {
		for _, name := range g.yes {
			if !g.fn(name) {
				t.Errorf("%s(%q) = false, want true", g.label, name)
			}
		}
		for _, name := range g.no {
			if g.fn(name) {
				t.Errorf("%s(%q) = true, want false", g.label, name)
			}
		}
	}
}
```

Run `bazel run //:gazelle`, then `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestSharedFunctionNameLists'` — expected failure: `undefined: IsKnownNondeterministicName`, `undefined: IsServerStateFunctionName`.

- [ ] **Step 2: Write the two list functions.** Create `pkg/storageintegrity/nondeterministic_names.go`. The names are the exact set of `pkg/plugins/storageintegrity/plugin.go:908-970` in the same order; only the per-line grouping and the exported spelling differ:

```go
package storageintegrity

import "strings"

// IsKnownNondeterministicName reports whether an already-lowercased ClickHouse
// name produces a value depending on the clock, on randomness, or on block/row
// position. Moved verbatim from the server-side ingress guard so that guard and
// the agent-mode inline VALUES closure gate share one list.
func IsKnownNondeterministicName(name string) bool {
	switch name {
	case "any", "anylast", "blocknumber", "blocksize", "curdate", "current_date",
		"current_timestamp", "datetimetouuidv7", "fuzzbits", "fuzzquery",
		"generaterandomstructure", "generateserialid", "generatesnowflakeid",
		"generateuuidv4", "generateuuidv7", "localtime", "localtimestamp", "now", "now64",
		"nowinblock", "nowinblock64", "obfuscatequery", "quantile", "quantiles", "rand",
		"rand32", "rand64", "randbernoulli", "randbinomial", "randcanonical",
		"randchisquared", "randconstant", "randexponential", "randfisherf", "randlognormal",
		"randnegativebinomial", "randnormal", "randpoisson", "randstudentt", "randuniform",
		"random", "randomfixedstring", "randomprintableascii", "randomstring",
		"randomstringutf8", "rownumberinallblocks", "rownumberinblock", "runningaccumulate",
		"runningconcurrency", "runningdifference",
		"runningdifferencestartingwithfirstvalue", "today", "utc_timestamp", "utctimestamp",
		"uuidv4", "yesterday":
		return true
	default:
		return false
	}
}

// IsServerStateFunctionName reports whether an already-lowercased name reads
// server, catalog or dictionary state (spec 2026-09-23 D3): a signed row must
// not embed a value only the executing server knows. Every dictGet* variant
// matches by prefix; the list is expected to grow and each addition is a policy
// change recorded in D3.
func IsServerStateFunctionName(name string) bool {
	if strings.HasPrefix(name, "dictget") {
		return true
	}
	switch name {
	case "joinget", "joingetornull", "currentuser", "currentdatabase", "currentschemas",
		"hostname", "fqdn", "version", "uptime", "serveruuid", "getsetting", "getmacro",
		"tcpport", "hascolumnintable", "sleep", "sleepeachrow", "throwif":
		return true
	default:
		return false
	}
}
```

Run `bazel run //:gazelle`, then `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestSharedFunctionNameLists'` — expected: PASS.

- [ ] **Step 3: Pin the ingress guard's behavior before touching it.** Create `pkg/plugins/storageintegrity/nondeterminism_pin_test.go`. It must PASS against today's in-package list and PASS unchanged after Step 4 — that equality is the evidence the move is behavior-neutral (D12).

```go
package storageintegrity

import "testing"

// TestContainsUnmaterializedNondeterminism_PinnedAcrossMove freezes the guard's
// answers on a fixture covering both regexp positions (function call and bare
// identifier), both surfaces (executable text and blanked literal/comment
// spans) and the list's first and last entries.
func TestContainsUnmaterializedNondeterminism_PinnedAcrossMove(t *testing.T) {
	cases := []struct{ sql, want string }{
		{"INSERT INTO db.t (a) VALUES (now())", "now"},
		{"INSERT INTO db.t (a) VALUES (GenerateUUIDv4())", "GenerateUUIDv4"},
		{"INSERT INTO db.t (a) SELECT today", "today"},
		{"INSERT INTO db.t (a) VALUES (current_timestamp)", "current_timestamp"},
		{"INSERT INTO db.t (a) SELECT any(x) FROM s", "any"},
		{"INSERT INTO db.t (a) VALUES (yesterday())", "yesterday"},
		{"INSERT INTO db.t (a) VALUES ('now()')", ""},
		{"INSERT INTO db.t (a) VALUES (1) -- now()", ""},
		{"INSERT INTO db.t (a) /* rand() */ VALUES (1)", ""},
		{"INSERT INTO db.t (a) VALUES (unhex('4142'))", ""},
		{"INSERT INTO db.t (a) VALUES (hostName())", ""},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			got, ok := containsUnmaterializedNondeterminism(tc.sql)
			if tc.want == "" && ok {
				t.Fatalf("got %q, want no match", got)
			}
			if tc.want != "" && (!ok || got != tc.want) {
				t.Fatalf("got (%q, %v), want (%q, true)", got, ok, tc.want)
			}
		})
	}
}
```

Run `bazel run //:gazelle`, then `bazel test //pkg/plugins/storageintegrity:storageintegrity_test --test_filter='TestContainsUnmaterializedNondeterminism_PinnedAcrossMove'` — expected: PASS (it characterizes existing behavior).

- [ ] **Step 4: Switch the ingress call site to the shared list and revise its now-false rationale.** In `pkg/plugins/storageintegrity/plugin.go`, inside `containsUnmaterializedNondeterminism` (lines 885-906), replace both occurrences of `if isKnownNondeterministicName(strings.ToLower(name)) {` with `if sicore.IsKnownNondeterministicName(strings.ToLower(name)) {` — the file already imports `sicore "github.com/housegate/housegate/pkg/storageintegrity"`. Everything else in that function (`stripSQLLiteralsAndComments`, the two regexp loops, the return values) stays byte for byte. Delete the whole `func isKnownNondeterministicName(name string) bool { ... }` (lines 908-970) and confirm `grep -rn "isKnownNondeterministicName" pkg/` reports no lowercase-`i` hit.

The guard's call **order** in `OnQuery` is unchanged, but its rationale at lines 195-199 is now false. Current text:

```go
	// Nondeterminism stays first because it is the only one of the three whose
	// coverage depends on running early. The sole way to write a function into
	// an INSERT is VALUES or SELECT, and both are unsignable shapes -- so
	// checking shape first would make this guard unreachable by construction
	// and quietly retire its tests.
```

Replacement (version-qualified per spec section 2; the paragraphs at lines 191-194 and 201-208 are untouched):

```go
	// Nondeterminism stays first because it is the only one of the three whose
	// coverage depends on running early. A function is written into an INSERT
	// through VALUES or SELECT, and none of those reaches this guard carrying a
	// payload: a 25.8 client truncates the SQL after VALUES and streams the
	// rows, a 26.3+ client sends the full statement text with no payload at
	// all, and an agent running storage_integrity.agent.inline_values evaluates
	// those inline rows and rewrites the statement to FORMAT Native before
	// signing it. Checking shape first would therefore make this guard
	// unreachable by construction and quietly retire its tests; it stays ahead
	// of the shape gate as the defence that holds if any of those facts change.
```

Run `bazel test //pkg/plugins/storageintegrity:storageintegrity_test` — expected: PASS with the pin test's expectations untouched.

- [ ] **Step 5: Write the failing test for `ParseInlineValuesInsert`.** Create `pkg/storageintegrity/inline_values_test.go`:

```go
package storageintegrity

import (
	"errors"
	"strings"
	"testing"
)

func TestParseInlineValuesInsert(t *testing.T) {
	cases := []struct {
		name, sql, wantDB, wantRows, wantErr string // wantErr "" = accept, "!" = ErrNotInlineValues
		wantCols                             []string
	}{
		{name: "no column list", sql: "INSERT INTO db.t VALUES (1), (2)", wantDB: "db", wantRows: "(1), (2)"},
		{name: "column list", sql: "INSERT INTO db.t (a, b) VALUES (1, 'x')", wantDB: "db", wantRows: "(1, 'x')", wantCols: []string{"a", "b"}},
		{name: "lowercase keywords", sql: "insert into db.t values (1)", wantDB: "db", wantRows: "(1)"},
		{name: "session database", sql: "INSERT INTO t (a) VALUES (1)", wantRows: "(1)", wantCols: []string{"a"}},
		{name: "quoted identifiers", sql: "INSERT INTO `db`.`t` (`a`) VALUES (1)", wantDB: "db", wantRows: "(1)", wantCols: []string{"a"}},
		{name: "expression rows", sql: "INSERT INTO db.t VALUES (1 + 2, unhex('4142'))", wantDB: "db", wantRows: "(1 + 2, unhex('4142'))"},
		{name: "trailing semicolon", sql: "INSERT INTO db.t VALUES (1);", wantDB: "db", wantRows: "(1)"},
		{name: "semicolon inside literal", sql: "INSERT INTO db.t VALUES ('a;b')", wantDB: "db", wantRows: "('a;b')"},
		{name: "25.x truncated shape", sql: "INSERT INTO db.t VALUES ", wantErr: "!"},
		{name: "25.x truncated no space", sql: "INSERT INTO db.t VALUES", wantErr: "!"},
		{name: "format native", sql: "INSERT INTO db.t FORMAT Native", wantErr: "!"},
		{name: "format values", sql: "INSERT INTO db.t FORMAT Values", wantErr: "!"},
		{name: "insert select", sql: "INSERT INTO db.t SELECT * FROM s", wantErr: "!"},
		{name: "insert with", sql: "INSERT INTO db.t WITH x AS (SELECT 1) SELECT * FROM x", wantErr: "!"},
		{name: "not an insert", sql: "SELECT 1", wantErr: "!"},
		{name: "empty text", sql: "", wantErr: "!"},
		{name: "trailing settings", sql: "INSERT INTO db.t VALUES (1) SETTINGS async_insert = 1", wantErr: "async_insert"},
		{name: "leading settings", sql: "INSERT INTO db.t SETTINGS async_insert = 1 VALUES (1)", wantErr: "async_insert"},
		{name: "second statement", sql: "INSERT INTO db.t VALUES (1); INSERT INTO db.t VALUES (2)", wantErr: "multi-statement"},
		{name: "insert into function", sql: "INSERT INTO FUNCTION remote('h', db.t) VALUES (1)", wantErr: "INSERT INTO FUNCTION"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseInlineValuesInsert(tc.sql)
			if tc.wantErr == "!" {
				if !errors.Is(err, ErrNotInlineValues) {
					t.Fatalf("err = %v, want ErrNotInlineValues", err)
				}
				return
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) ||
					!strings.HasPrefix(err.Error(), InlineValuesErrorPrefix) || errors.Is(err, ErrNotInlineValues) {
					t.Fatalf("err = %v, want a named %q-prefixed refusal containing %q", err, InlineValuesErrorPrefix, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Target.Database != tc.wantDB || got.Target.Table != "t" || got.Rows != tc.wantRows ||
				strings.Join(got.Columns, ",") != strings.Join(tc.wantCols, ",") {
				t.Fatalf("got %+v, want database %q table \"t\" rows %q columns %v", got, tc.wantDB, tc.wantRows, tc.wantCols)
			}
		})
	}
}
```

Run `bazel run //:gazelle`, then `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestParseInlineValuesInsert'` — expected failure: `undefined: ParseInlineValuesInsert`, `ErrNotInlineValues`, `InlineValuesErrorPrefix`.

- [ ] **Step 6: Expose the payload-source offset in `sql.go` and fix its doc.** So the parser reuses the existing keyword scan instead of re-parsing the target, rename `insertDataSource` (lines 95-131) to `insertDataSourceAt` and apply exactly these five mechanical changes, leaving the loop and depth tracking otherwise byte for byte:

1. signature → `func insertDataSourceAt(sql string) (source, format string, end int, ok bool)`, documented `// insertDataSourceAt is insertDataSource plus the byte offset just past the payload-source keyword, which is where an inline VALUES row list starts.`
2. `return "", "", false` (non-INSERT guard) → `return "", "", 0, false`
3. `return "", "", true` (the `!ok` end-of-input return) → `return "", "", pos, true`
4. FORMAT branch: `formatTok, _, formatOK :=` → `formatTok, formatEnd, formatOK :=`; `return "FORMAT", "", true` → `return "FORMAT", "", pos, true`; `return "FORMAT", formatTok.text, true` → `return "FORMAT", formatTok.text, formatEnd, true`
5. VALUES/SELECT/WITH branch: `return tok.text, "", true` → `return tok.text, "", pos, true`

Add the wrapper above it so `InsertPayloadEncoding` is untouched:

```go
func insertDataSource(sql string) (source, format string, ok bool) {
	source, format, _, ok = insertDataSourceAt(sql)
	return source, format, ok
}
```

The `InsertPayloadEncoding` doc paragraph at lines 47-50 is now false. Current text:

```go
// Note the asymmetry with `INSERT ... VALUES (1)`: written inline, the rows are
// part of the SQL text and no ClientData packet is sent at all, so there is no
// payload to sign. The same statement written as `FORMAT Values` with the rows
// on stdin is signable, because then the client streams them.
```

Replacement (version-qualified per spec section 2; lines 41-46 stay):

```go
// Note the version-qualified asymmetry with `INSERT ... VALUES (1)`: a 25.8
// client truncates the SQL after VALUES and streams the rows, the ordinary
// payload-local path; a 26.3+ client and the pinned clickhouse-go fork send the
// full statement text and no ClientData packet at all, so this function reports
// no payload encoding for that shape. An agent running
// storage_integrity.agent.inline_values evaluates those inline rows and
// rewrites the statement to FORMAT Native before it reaches this gate (spec
// 2026-09-23 D1). The same statement written as `FORMAT Values` with the rows
// on stdin has always been signable, because then the client streams them.
```

Run `bazel test //pkg/storageintegrity:storageintegrity_test` — expected: pre-existing tests PASS; `TestParseInlineValuesInsert` still fails to compile.

- [ ] **Step 7: Write the parser.** Create `pkg/storageintegrity/inline_values.go` (the gate is appended in Step 9):

```go
package storageintegrity

import (
	"errors"
	"fmt"
	"strings"
)

// InlineValuesErrorPrefix starts every named refusal of the signed inline
// VALUES lane, so an operator can tell an agent-side refusal from a server
// Exception. Errors from this file already carry it; callers forward them
// unchanged rather than wrapping them again.
const InlineValuesErrorPrefix = "storage_integrity inline VALUES: "

// ErrNotInlineValues means the statement is not a 26.x-shaped inline
// INSERT ... VALUES and keeps today's behavior: FORMAT, SELECT, WITH,
// non-INSERT text, and the 25.x truncated shape whose rows text is empty.
var ErrNotInlineValues = errors.New("not an inline VALUES insert")

// InlineValuesInsert is a decoded inline INSERT ... VALUES statement. Columns
// is nil when the statement has no column list. Rows is the verbatim text
// after the VALUES keyword with surrounding whitespace and at most one
// trailing ';' removed; the evaluator sends it byte for byte, so it must never
// be normalized here.
type InlineValuesInsert struct {
	Target  InsertTarget
	Columns []string
	Rows    string
}

// ParseInlineValuesInsert decodes a complete inline
// INSERT INTO [db.]t [(cols)] VALUES <rows> statement (spec 2026-09-23 D1),
// reusing ParseInsertTarget, the payload-source keyword scan and
// InsertColumnList so the target and column rules stay the SI lane's existing
// ones. ErrNotInlineValues means "leave this statement on the ordinary path";
// every other error is a named refusal carrying InlineValuesErrorPrefix.
func ParseInlineValuesInsert(sql string) (InlineValuesInsert, error) {
	target, err := ParseInsertTarget(sql)
	if err != nil {
		if errors.Is(err, ErrNotInsert) {
			return InlineValuesInsert{}, ErrNotInlineValues
		}
		return InlineValuesInsert{}, fmt.Errorf("%s%w", InlineValuesErrorPrefix, err)
	}
	source, _, end, ok := insertDataSourceAt(sql)
	if !ok || source != "VALUES" {
		return InlineValuesInsert{}, ErrNotInlineValues
	}
	keys, err := InlineInsertSettingKeys(sql)
	if err != nil {
		return InlineValuesInsert{}, fmt.Errorf("%sinspect inline SETTINGS: %w", InlineValuesErrorPrefix, err)
	}
	if len(keys) > 0 {
		return InlineValuesInsert{}, fmt.Errorf("%sINSERT ... VALUES ... SETTINGS is not supported on the signed inline lane (setting %q)", InlineValuesErrorPrefix, keys[0])
	}
	cols, _, err := InsertColumnList(sql)
	if err != nil {
		return InlineValuesInsert{}, fmt.Errorf("%s%w", InlineValuesErrorPrefix, err)
	}
	rows, err := trimTrailingStatement(strings.TrimSpace(sql[end:]))
	if err != nil {
		return InlineValuesInsert{}, err
	}
	if rows == "" {
		// The 25.x truncated shape: the client streams the rows instead.
		return InlineValuesInsert{}, ErrNotInlineValues
	}
	return InlineValuesInsert{Target: target, Columns: cols, Rows: rows}, nil
}

// trimTrailingStatement removes at most one terminating ';' and refuses text
// after it (D1: multi-statement input is out of scope). Single-quoted spans are
// skipped permissively so a ';' inside a literal stays data; the literal's own
// escapes are validated later by ValuesClosure.
func trimTrailingStatement(rows string) (string, error) {
	for i := 0; i < len(rows); {
		switch rows[i] {
		case '\'':
			j := i + 1
			for j < len(rows) {
				if rows[j] == '\\' {
					j += 2
					continue
				}
				if rows[j] == '\'' {
					if j+1 < len(rows) && rows[j+1] == '\'' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			i = j
		case ';':
			if strings.TrimSpace(rows[i+1:]) != "" {
				return "", fmt.Errorf("%smulti-statement input is not supported; text follows the ';' at byte offset %d", InlineValuesErrorPrefix, i)
			}
			return strings.TrimSpace(rows[:i]), nil
		default:
			i++
		}
	}
	return rows, nil
}
```

Run `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestParseInlineValuesInsert'` — expected: PASS.

- [ ] **Step 8: Write the failing table-driven test for `ValuesClosure`.** Append to `pkg/storageintegrity/inline_values_test.go` — the accept and refuse lists are spec D3 in full, with a case variant of a name from each deny-list:

```go
func TestValuesClosure(t *testing.T) {
	accept := []string{
		"(1), (2)",                                        // integer tuples
		"(-1, +2)",                                        // leading signs
		"(1.5, 2.5e-3, 4E7)",                              // decimal and exponent
		"(0xFF, 0x00)",                                    // hexadecimal
		"('it''s')",                                       // doubled quote
		`('\'', '\\', '\n', '\t', '\x41')`,                // every admitted escape
		"(true, FALSE, True)",                             // booleans in any case
		"(1 + 2 - 3 * 4 / 5 % 6)",                         // arithmetic operators
		"(1 = 1, 1 != 2, 1 <> 2, 1 < 2, 1 <= 2, 2 >= 1)",  // comparison operators
		"((1 + 2) * 3)",                                   // nested parentheses
		"(unhex('4142'), toInt64(1))",                     // deterministic calls
		"(toDateTime(fromUnixTimestamp64Milli(1758000)))", // nested calls
		"(1),\n\t(2)",                                     // multi-line rows
		"(dictionaryHelper(1))",                           // name merely starting with dict
	}
	for _, rows := range accept {
		t.Run("accept "+rows, func(t *testing.T) {
			if err := ValuesClosure(rows); err != nil {
				t.Fatalf("ValuesClosure(%q) = %v, want nil", rows, err)
			}
		})
	}
	refuse := []struct{ rows, want string }{
		{"((SELECT count() FROM system.tables))", `keyword "SELECT"`},
		{"(1 FROM t)", `keyword "FROM"`},
		{"(WITH x AS 1)", `keyword "WITH"`},
		{"(1 IN (1, 2))", `keyword "IN"`},
		{"(NULL)", `keyword "NULL"`},
		{"(DEFAULT)", `keyword "DEFAULT"`},
		{"(CAST(1 AS Int64))", `keyword "CAST"`},
		{"(1 AS x)", `keyword "AS"`},
		{"(INTERVAL 1 DAY)", `keyword "INTERVAL"`},
		{"(1 AND 1)", `keyword "AND"`},
		{"(1 OR 1)", `keyword "OR"`},
		{"(NOT 1)", `keyword "NOT"`},
		{"(1 and 1)", `keyword "and"`},
		{"(x)", `bare identifier "x"`},
		{"(a + 1)", `bare identifier "a"`},
		{"(now ())", `bare identifier "now"`},
		{"(now())", `nondeterministic function "now"`},
		{"(GenerateUUIDv4())", `nondeterministic function "GenerateUUIDv4"`},
		{"(hostName())", `server-state function "hostName"`},
		{"(SLEEP(1))", `server-state function "SLEEP"`},
		{"(dictGetString('d', 'k', 1))", `server-state function "dictGetString"`},
		{"(1) -- rest", "SQL comments are not accepted"},
		{"(1) # rest", "SQL comments are not accepted"},
		{"(1 /* x */)", "SQL comments are not accepted"},
		{"(1) // rest", "SQL comments are not accepted"},
		{"(`a`)", "quoted identifiers are not accepted"},
		{`("a")`, "quoted identifiers are not accepted"},
		{"($$abc$$)", "heredoc string literals are not accepted"},
		{"($tag$abc$tag$)", "heredoc string literals are not accepted"},
		{"({p:Identifier})", "query parameters are not accepted"},
		{"(?)", `token "?"`},
		{"(@x)", `token "@"`},
		{"(1::Int64)", `token ":"`},
		{"([1, 2])", `token "["`},
		{"(1); (2)", `token ";"`},
		{"('abc)", "unterminated string literal"},
		{`('\q')`, `string escape "\\q" is not accepted`},
		{`('\xZZ')`, `escape \x requires two hexadecimal digits`},
		{"(1", "unbalanced '('"},
		{"(1))", "unbalanced ')'"},
		{"", "the VALUES row list is empty"},
		{"   ", "the VALUES row list is empty"},
	}
	for _, tc := range refuse {
		t.Run("refuse "+tc.rows, func(t *testing.T) {
			err := ValuesClosure(tc.rows)
			if err == nil {
				t.Fatalf("ValuesClosure(%q) = nil, want a refusal", tc.rows)
			}
			if !strings.HasPrefix(err.Error(), InlineValuesErrorPrefix) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %q, want %q-prefixed and containing %q", err.Error(), InlineValuesErrorPrefix, tc.want)
			}
		})
	}
}
```

Run `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestValuesClosure'` — expected failure: `undefined: ValuesClosure`.

- [ ] **Step 9: Write the closure gate.** Append to `pkg/storageintegrity/inline_values.go`:

```go
// refusedValuesKeywords are SQL keywords refused even when a '(' follows, which
// is what stops CAST(...), IN(...) and NOT(...) from passing the function-call
// rule. Every other non-call word is refused as a bare identifier, so this set
// buys clearer errors, not coverage.
var refusedValuesKeywords = map[string]bool{
	"select": true, "from": true, "with": true, "in": true, "null": true, "default": true,
	"cast": true, "as": true, "interval": true, "and": true, "or": true, "not": true,
}

// ValuesClosure is the lexical closure gate of spec 2026-09-23 D3. It accepts
// only a closed expression alphabet in the verbatim rows text of an inline
// INSERT ... VALUES, so no signed row can depend on the clock, on randomness,
// on server state, or on another table. It is a deny-list policy gate, not a
// validator: ClickHouse's VALUES table function still refuses anything that is
// not a constant expression.
//
// Token grammar (the complete accepted alphabet; every other byte is refused):
//
//	rows     := token+                        ; parentheses must balance
//	token    := space | number | string | boolean | call | punct | operator
//	space    := ' ' | '\t' | '\n' | '\r' | '\f'
//	number   := '0' ('x'|'X') hex* | digit+ ('.' digit*)? exponent?
//	exponent := ('e'|'E') ('+'|'-')? digit+
//	string   := "'" ( "''" | escape | not("'" or '\') )* "'"
//	escape   := '\' ("'" | '\' | 'n' | 't') | '\' 'x' hex hex
//	boolean  := "true" | "false"              ; case-insensitive
//	call     := word '('                      ; '(' immediately after the word
//	word     := [A-Za-z_] [A-Za-z0-9_]*
//	punct    := '(' | ')' | ','
//	operator := '+' | '-' | '*' | '/' | '%' | '=' | '!=' | '<>' | '<' | '<=' | '>' | '>='
//
// Refused with a named error and its byte offset in rows: comments ('--', '#',
// '/*' and '//', which ClickHouse also reads as a line comment), quoted
// identifiers ('`' and '"'), heredocs ('$'), query parameters ('{'), every name
// in refusedValuesKeywords, a word not immediately followed by '(' that is
// neither true nor false, a call whose lowercased name
// IsKnownNondeterministicName or IsServerStateFunctionName accepts, and any
// other byte, including ';', '[', ']', '?', '@' and ':'.
//
// The one scanner here that models heredocs and {name:Type} parameters is
// package-private in pkg/plugins/sireserved, and a core package cannot import a
// plugin package, so this lexer recognises both itself: it refuses on the first
// '$' or '{' outside a string literal rather than delimiting their bodies.
func ValuesClosure(rows string) error {
	depth, seen := 0, false
	for i := 0; i < len(rows); {
		c := rows[i]
		switch {
		case isStorageSpace(c):
			i++
			continue
		case hasPrefixAt(rows, i, "--"), hasPrefixAt(rows, i, "/*"), hasPrefixAt(rows, i, "//"), c == '#':
			return closureErr("SQL comments are not accepted", i)
		case c == '`' || c == '"':
			return closureErr("quoted identifiers are not accepted", i)
		case c == '$':
			return closureErr("heredoc string literals are not accepted", i)
		case c == '{':
			return closureErr("query parameters are not accepted", i)
		case c == '\'':
			next, err := scanClosureString(rows, i)
			if err != nil {
				return err
			}
			i = next
		case c >= '0' && c <= '9':
			i = scanClosureNumber(rows, i)
		case isStorageIdentStart(c):
			start := i
			for i++; i < len(rows) && isStorageIdentPart(rows[i]); i++ {
			}
			word := rows[start:i]
			lower := strings.ToLower(word)
			if refusedValuesKeywords[lower] {
				return closureErr(fmt.Sprintf("keyword %q is not accepted", word), start)
			}
			if i < len(rows) && rows[i] == '(' {
				if IsKnownNondeterministicName(lower) {
					return closureErr(fmt.Sprintf("nondeterministic function %q is not accepted", word), start)
				}
				if IsServerStateFunctionName(lower) {
					return closureErr(fmt.Sprintf("server-state function %q is not accepted", word), start)
				}
				continue // the '(' is consumed by the next iteration
			}
			if lower != "true" && lower != "false" {
				return closureErr(fmt.Sprintf("bare identifier %q is not accepted", word), start)
			}
		case c == '(':
			depth++
			i++
		case c == ')':
			depth--
			if depth < 0 {
				return closureErr("unbalanced ')'", i)
			}
			i++
		case c == ',':
			i++
		case hasPrefixAt(rows, i, "!="), hasPrefixAt(rows, i, "<>"), hasPrefixAt(rows, i, "<="), hasPrefixAt(rows, i, ">="):
			i += 2
		case c == '+', c == '-', c == '*', c == '/', c == '%', c == '=', c == '<', c == '>':
			i++
		default:
			return closureErr(fmt.Sprintf("token %q is not accepted", string(c)), i)
		}
		seen = true
	}
	if !seen {
		return fmt.Errorf("%sthe VALUES row list is empty", InlineValuesErrorPrefix)
	}
	if depth != 0 {
		return fmt.Errorf("%sunbalanced '(' in the VALUES row list", InlineValuesErrorPrefix)
	}
	return nil
}

func closureErr(reason string, offset int) error {
	return fmt.Errorf("%s%s at byte offset %d of the VALUES row list", InlineValuesErrorPrefix, reason, offset)
}

// scanClosureString returns the offset just past a single-quoted literal,
// admitting only the ClickHouse escapes spec D3 names.
func scanClosureString(rows string, start int) (int, error) {
	for i := start + 1; i < len(rows); {
		switch rows[i] {
		case '\\':
			if i+1 >= len(rows) {
				return 0, closureErr("unterminated string literal", start)
			}
			switch rows[i+1] {
			case '\'', '\\', 'n', 't':
				i += 2
			case 'x':
				if i+3 >= len(rows) || !isClosureHex(rows[i+2]) || !isClosureHex(rows[i+3]) {
					return 0, closureErr(`escape \x requires two hexadecimal digits`, i)
				}
				i += 4
			default:
				return 0, closureErr(fmt.Sprintf("string escape %q is not accepted", rows[i:i+2]), i)
			}
		case '\'':
			if i+1 < len(rows) && rows[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1, nil
		default:
			i++
		}
	}
	return 0, closureErr("unterminated string literal", start)
}

// scanClosureNumber returns the offset just past a numeric literal; a leading
// sign is an operator token, not part of the number.
func scanClosureNumber(rows string, start int) int {
	if rows[start] == '0' && start+1 < len(rows) && (rows[start+1] == 'x' || rows[start+1] == 'X') {
		i := start + 2
		for ; i < len(rows) && isClosureHex(rows[i]); i++ {
		}
		return i
	}
	i := scanClosureDigits(rows, start)
	if i < len(rows) && rows[i] == '.' {
		i = scanClosureDigits(rows, i+1)
	}
	if i < len(rows) && (rows[i] == 'e' || rows[i] == 'E') {
		j := i + 1
		if j < len(rows) && (rows[j] == '+' || rows[j] == '-') {
			j++
		}
		if j < len(rows) && rows[j] >= '0' && rows[j] <= '9' {
			i = scanClosureDigits(rows, j)
		}
	}
	return i
}

func scanClosureDigits(rows string, i int) int {
	for ; i < len(rows) && rows[i] >= '0' && rows[i] <= '9'; i++ {
	}
	return i
}

func isClosureHex(ch byte) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

func hasPrefixAt(s string, i int, prefix string) bool {
	return strings.HasPrefix(s[i:], prefix)
}
```

Run `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestValuesClosure'` — expected: PASS.

- [ ] **Step 10: Run every touched package and commit.** Run `bazel run //:gazelle`, then `bazel test //pkg/storageintegrity:storageintegrity_test //pkg/plugins/storageintegrity:storageintegrity_test //pkg/plugins/sistatement:sistatement_test` — expected: all PASS, ingress tests unchanged. Then:

```
git add pkg/storageintegrity/inline_values.go pkg/storageintegrity/inline_values_test.go pkg/storageintegrity/nondeterministic_names.go pkg/storageintegrity/nondeterministic_names_test.go pkg/storageintegrity/sql.go pkg/storageintegrity/BUILD.bazel pkg/plugins/storageintegrity/plugin.go pkg/plugins/storageintegrity/nondeterminism_pin_test.go pkg/plugins/storageintegrity/BUILD.bazel
git commit -m "feat(storageintegrity): add inline VALUES parser and lexical closure gate" -m "Parse the 26.x inline INSERT ... VALUES shape into target, column list and verbatim rows text, and gate the rows through a hand-written lexer admitting only a closed literal/operator/call alphabet. Move the nondeterministic-name list into pkg/storageintegrity so the agent gate and the server ingress guard share it, add the server-state deny-list, and replace the two comments claiming inline VALUES is unsignable with the version-qualified fact. The ingress guard's behavior and call order are unchanged, pinned by a characterization test. Implements spec D3." -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

---

### Task 2: Native client Data packet encoder and the codec method (D5, D7)

**Files:**
- Create: `pkg/replay/nativepayload/encode.go`, `pkg/replay/nativepayload/encode_test.go`, `pkg/replay/nativepayload/testdata/insert_one_string_row_54470.hex`
- Modify: `pkg/chproto/codec.go` (append after `WriteSampleBlock`, codec.go:704-767), `pkg/chproto/codec_test.go` (append; `readerWriter` is at codec_test.go:38-44), `pkg/replay/nativepayload/BUILD.bazel`, `pkg/chproto/BUILD.bazel` (gazelle)

**Interfaces:**
- Consumes: `payloadexec.ResolveColumnProfile(typeName string) (ColumnProfile, error)` (field `NativeWireType string`); `nativepayload.Decode(schema payloadexec.TableSchema, revision int, payload []byte) ([]payloadexec.Row, error)`; `nativepayload.ErrUnsupported`; `proto.Block{Info BlockInfo; Columns, Rows int}.EncodeBlock(buf *proto.Buffer, version int, input []proto.InputColumn) error`; `proto.BlockInfo{Overflows bool; BucketNum int}`; `proto.InputColumn{Name string; Data ColInput}`; `(*chproto.Codec).Revision() int`, `.Compression() proto.Compression`; `chproto.ErrMalformed`.
- Produces: `func EncodeClientDataPacket(revision int, cols []proto.InputColumn) ([]byte, error)` in `nativepayload`; `func (c *Codec) EncodeClientDataPacket(cols []proto.InputColumn) ([]byte, error)` in `chproto`.

- [ ] **Step 1: Write the failing round-trip test**

Create `pkg/replay/nativepayload/encode_test.go`:

```go
package nativepayload

import (
	"bytes"
	"encoding/hex"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// encodeTestRevision is housegate's upstream cap (chproto.MaxSupportedRevision),
// which carries FeatureBlockInfo (51903) and FeatureCustomSerialization (54454).
const encodeTestRevision = 54470

func strCol(v ...string) *proto.ColStr {
	c := &proto.ColStr{}
	for _, s := range v {
		c.Append(s)
	}
	return c
}

func timeCol[T interface {
	Append(time.Time)
	proto.ColInput
}](c T, v ...time.Time) T {
	for _, t := range v {
		c.Append(t)
	}
	return c
}

// assertValueEqual compares floats bitwise so NaN and negative zero round trip
// exactly; every other admitted type compares by value.
func assertValueEqual(t *testing.T, i int, got, want any) {
	t.Helper()
	switch w := want.(type) {
	case float32:
		if g, ok := got.(float32); !ok || math.Float32bits(g) != math.Float32bits(w) {
			t.Fatalf("row %d: got %v (%T), want %v bitwise", i, got, got, w)
		}
	case float64:
		if g, ok := got.(float64); !ok || math.Float64bits(g) != math.Float64bits(w) {
			t.Fatalf("row %d: got %v (%T), want %v bitwise", i, got, got, w)
		}
	case time.Time:
		if g, ok := got.(time.Time); !ok || !g.Equal(w) {
			t.Fatalf("row %d: got %v (%T), want %v", i, got, got, w)
		}
	case []byte:
		if g, ok := got.([]byte); !ok || !bytes.Equal(g, w) {
			t.Fatalf("row %d: got %v (%T), want %x", i, got, got, w)
		}
	default:
		if got != want {
			t.Fatalf("row %d: got %#v (%T), want %#v", i, got, got, want)
		}
	}
}

func TestEncodeClientDataPacket_RoundTripsEveryAdmittedType(t *testing.T) {
	var fixed [32]byte
	copy(fixed[:], strings.Repeat("a", 32))
	fixedCol := &proto.ColFixedStr32{}
	fixedCol.Append(fixed)
	epochDate := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	farDate := time.Date(2149, time.June, 6, 0, 0, 0, 0, time.UTC)
	epoch := time.Unix(0, 0).UTC()
	farTime := time.Date(2106, time.February, 7, 6, 28, 15, 0, time.UTC)
	milli := time.Date(2026, time.September, 23, 12, 34, 56, 789000000, time.UTC)
	nano := time.Date(2200, time.January, 2, 3, 4, 5, 123456789, time.UTC)
	nz32, nz64 := float32(math.Copysign(0, -1)), math.Copysign(0, -1)

	for _, tc := range []struct {
		declared string
		col      proto.ColInput
		want     []any
	}{
		{"String", strCol("", "héllo 世界"), []any{"", "héllo 世界"}},
		{"FixedString(32)", fixedCol, []any{fixed[:]}},
		{"Bool", &proto.ColBool{true, false}, []any{true, false}},
		{"Float32", &proto.ColFloat32{float32(math.NaN()), nz32}, []any{float32(math.NaN()), nz32}},
		{"Float64", &proto.ColFloat64{math.NaN(), nz64}, []any{math.NaN(), nz64}},
		{"UInt8", &proto.ColUInt8{0, math.MaxUint8}, []any{uint8(0), uint8(math.MaxUint8)}},
		{"UInt16", &proto.ColUInt16{0, math.MaxUint16}, []any{uint16(0), uint16(math.MaxUint16)}},
		{"UInt32", &proto.ColUInt32{0, math.MaxUint32}, []any{uint32(0), uint32(math.MaxUint32)}},
		{"UInt64", &proto.ColUInt64{0, math.MaxUint64}, []any{uint64(0), uint64(math.MaxUint64)}},
		{"Int8", &proto.ColInt8{math.MinInt8, math.MaxInt8}, []any{int8(math.MinInt8), int8(math.MaxInt8)}},
		{"Int16", &proto.ColInt16{math.MinInt16, math.MaxInt16}, []any{int16(math.MinInt16), int16(math.MaxInt16)}},
		{"Int32", &proto.ColInt32{math.MinInt32, math.MaxInt32}, []any{int32(math.MinInt32), int32(math.MaxInt32)}},
		{"Int64", &proto.ColInt64{math.MinInt64, math.MaxInt64}, []any{int64(math.MinInt64), int64(math.MaxInt64)}},
		{"Date", timeCol(&proto.ColDate{}, epochDate, farDate), []any{epochDate, farDate}},
		{"DateTime", timeCol(&proto.ColDateTime{}, epoch, farTime), []any{epoch, farTime}},
		{"DateTime('UTC')", timeCol(&proto.ColDateTime{Location: time.UTC}, epoch, farTime), []any{epoch, farTime}},
		{"DateTime64(3)", timeCol((&proto.ColDateTime64{}).WithPrecision(proto.PrecisionMilli), epoch, milli), []any{epoch, milli}},
		{"DateTime64(3, 'UTC')", timeCol((&proto.ColDateTime64{}).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC), milli), []any{milli}},
		{"DateTime64(9)", timeCol((&proto.ColDateTime64{}).WithPrecision(proto.PrecisionNano), epoch, nano), []any{epoch, nano}},
	} {
		t.Run(tc.declared, func(t *testing.T) {
			if got := string(tc.col.Type()); got != tc.declared {
				t.Fatalf("ch-go column reports %q, case declares %q", got, tc.declared)
			}
			raw, err := EncodeClientDataPacket(encodeTestRevision, []proto.InputColumn{{Name: "c", Data: tc.col}})
			if err != nil {
				t.Fatalf("EncodeClientDataPacket: %v", err)
			}
			schema := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "c", Type: tc.declared}}}
			rows, err := Decode(schema, encodeTestRevision, raw)
			if err != nil || len(rows) != len(tc.want) {
				t.Fatalf("Decode returned %d rows (err %v), want %d", len(rows), err, len(tc.want))
			}
			for i, want := range tc.want {
				assertValueEqual(t, i, rows[i].Values[0], want)
			}
		})
	}
}

func TestEncodeClientDataPacket_RefusesUnencodableInput(t *testing.T) {
	uuidCol := &proto.ColUUID{}
	uuidCol.Append(uuid.UUID{})
	for _, tc := range []struct {
		name string
		rev  int
		cols []proto.InputColumn
		want string
	}{
		{"no revision", 0, []proto.InputColumn{{Name: "c", Data: strCol("x")}}, "client protocol revision is required"},
		{"no columns", encodeTestRevision, nil, "requires at least one column"},
		{"zero rows", encodeTestRevision, []proto.InputColumn{{Name: "c", Data: strCol()}}, "contains no rows"},
		{"reserved column", encodeTestRevision, []proto.InputColumn{{Name: "_hg_row_id", Data: strCol("x")}}, "reserved _hg_row_id"},
		{"duplicate column", encodeTestRevision, []proto.InputColumn{{Name: "c", Data: strCol("x")}, {Name: "c", Data: strCol("y")}}, "duplicate column"},
		{"ragged rows", encodeTestRevision, []proto.InputColumn{{Name: "a", Data: strCol("x", "y")}, {Name: "b", Data: strCol("z")}}, "has 1 rows, first column has 2"},
		{"unsupported type", encodeTestRevision, []proto.InputColumn{{Name: "c", Data: uuidCol}}, "unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := EncodeClientDataPacket(tc.rev, tc.cols)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want one mentioning %q", err, tc.want)
			}
		})
	}
}
```

Add `"github.com/google/uuid"` to the imports (the test target already depends on it). The `unsupported type` row carries one row so the refusal comes from `ResolveColumnProfile("UUID")`, not the row count.

- [ ] **Step 2: Run it and see it fail**

```bash
bazel test //pkg/replay/nativepayload:nativepayload_test --test_filter='TestEncodeClientDataPacket'
```

Expected: build failure `undefined: EncodeClientDataPacket` — the target does not compile, so no test runs.

- [ ] **Step 3: Write the encoder**

Create `pkg/replay/nativepayload/encode.go`:

```go
package nativepayload

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// EncodeClientDataPacket encodes one uncompressed client Data packet carrying
// cols: the packet code ClientCodeData, an empty external-table block name,
// BlockInfo{BucketNum: -1} and the block body framed for revision.
//
// It is Decode's inverse over the admitted column set. Every column type must
// resolve through payloadexec.ResolveColumnProfile and equal the profile's
// NativeWireType, so Decode of the returned bytes under a schema declaring the
// same types reproduces the same values. The BlockInfo prefix (revision >=
// 51903) and the per-column custom-serialization flag (revision >= 54454) are
// ch-go's own gates: callers pass the codec's negotiated revision, never a
// constant, because the signed payload is exactly these bytes.
func EncodeClientDataPacket(revision int, cols []proto.InputColumn) ([]byte, error) {
	if revision <= 0 {
		return nil, fmt.Errorf("%w: client protocol revision is required", ErrUnsupported)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("%w: native block requires at least one column", ErrUnsupported)
	}
	if cols[0].Data == nil {
		return nil, fmt.Errorf("%w: native block column %q has no data", ErrUnsupported, cols[0].Name)
	}
	rows := cols[0].Data.Rows()
	if rows == 0 {
		return nil, fmt.Errorf("%w: native payload contains no rows", ErrUnsupported)
	}
	seen := make(map[string]struct{}, len(cols))
	for _, col := range cols {
		switch {
		case col.Name == "":
			return nil, fmt.Errorf("%w: native block contains an unnamed column", ErrUnsupported)
		case col.Name == "_hg_row_id":
			return nil, fmt.Errorf("%w: native block must not contain reserved _hg_row_id", ErrUnsupported)
		case col.Data == nil:
			return nil, fmt.Errorf("%w: native block column %q has no data", ErrUnsupported, col.Name)
		}
		if _, dup := seen[col.Name]; dup {
			return nil, fmt.Errorf("%w: native block contains duplicate column %q", ErrUnsupported, col.Name)
		}
		seen[col.Name] = struct{}{}
		if got := col.Data.Rows(); got != rows {
			return nil, fmt.Errorf("%w: native block column %q has %d rows, first column has %d",
				ErrUnsupported, col.Name, got, rows)
		}
		declared := string(col.Data.Type())
		profile, err := payloadexec.ResolveColumnProfile(declared)
		if err != nil {
			return nil, fmt.Errorf("%w: native block column %q: %w", ErrUnsupported, col.Name, err)
		}
		if declared != profile.NativeWireType {
			return nil, fmt.Errorf("%w: native block column %q type %q is not the admitted wire type %q",
				ErrUnsupported, col.Name, declared, profile.NativeWireType)
		}
	}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	block := proto.Block{Info: proto.BlockInfo{BucketNum: -1}, Columns: len(cols), Rows: rows}
	if err := block.EncodeBlock(&buf, revision, cols); err != nil {
		return nil, fmt.Errorf("%w: encode native block: %w", ErrUnsupported, err)
	}
	return append([]byte(nil), buf.Buf...), nil
}
```

- [ ] **Step 4: Run gazelle and the tests**

```bash
bazel run //:gazelle
bazel test //pkg/replay/nativepayload:nativepayload_test --test_filter='TestEncodeClientDataPacket'
```

Expected: PASS — 19 round-trip subtests and 7 refusal subtests. Gazelle adds `encode.go` to `srcs` and `encode_test.go` to the test `srcs`.

- [ ] **Step 5: Write the failing golden and revision-tier tests**

Append to `encode_test.go`:

```go
// issue153MeasuredPrefix is the first 12 bytes measured on the wire in issue
// #153 for an inline INSERT of one String row: ClientCodeData(02), the empty
// block name(00), BlockInfo{Overflows:false, BucketNum:-1} (01 00 02 ffffffff
// 00), num_columns(01) and num_rows(01). It is identical at every revision
// >= 51903 and is what pins the packet header shape.
const issue153MeasuredPrefix = "0200010002ffffffff000101"

func TestEncodeClientDataPacket_MatchesCapturedOneStringRowPacket(t *testing.T) {
	raw, err := EncodeClientDataPacket(encodeTestRevision, []proto.InputColumn{{Name: "s", Data: strCol("hello")}})
	if err != nil {
		t.Fatalf("EncodeClientDataPacket: %v", err)
	}
	if got := hex.EncodeToString(raw[:12]); got != issue153MeasuredPrefix {
		t.Fatalf("packet prefix = %s, want the measured %s", got, issue153MeasuredPrefix)
	}
	goldenHex, err := os.ReadFile("testdata/insert_one_string_row_54470.hex")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	want, err := hex.DecodeString(strings.TrimSpace(string(goldenHex)))
	if err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("packet bytes\n got %x\nwant %x", raw, want)
	}
	schema := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "s", Type: "String"}}}
	rows, err := Decode(schema, encodeTestRevision, want)
	if err != nil || len(rows) != 1 || rows[0].Values[0] != "hello" {
		t.Fatalf("golden decodes to %v (err %v), want one row \"hello\"", rows, err)
	}
}

func TestEncodeClientDataPacket_HeaderBytesPerRevisionTier(t *testing.T) {
	// 54058 (FeatureTimezone) and 54453 (FeatureParallelReplicas) carry
	// BlockInfo but predate FeatureCustomSerialization (54454); 54454 and 54470
	// add one "false" flag byte per column after its type string.
	for _, rev := range []int{54058, 54453, 54454, 54470} {
		raw, err := EncodeClientDataPacket(rev, []proto.InputColumn{{Name: "v", Data: &proto.ColUInt64{1}}})
		if err != nil {
			t.Fatalf("rev %d: EncodeClientDataPacket: %v", rev, err)
		}
		if got := hex.EncodeToString(raw[:12]); got != issue153MeasuredPrefix {
			t.Fatalf("rev %d: prefix = %s, want %s", rev, got, issue153MeasuredPrefix)
		}
		// 12 header + (1+1) name "v" + (1+6) type "UInt64" + flag + 8 value bytes.
		wantLen, flagged := 12+2+7+8, proto.FeatureCustomSerialization.In(rev)
		if flagged {
			wantLen++
		}
		if len(raw) != wantLen {
			t.Fatalf("rev %d: packet is %d bytes (%x), want %d", rev, len(raw), raw, wantLen)
		}
		// Byte 21 is the position right after the type string.
		if flagged && raw[21] != 0x00 {
			t.Fatalf("rev %d: custom-serialization flag = %#x, want 0x00 (%x)", rev, raw[21], raw)
		}
		if !flagged && raw[21] == 0x00 {
			t.Fatalf("rev %d: value bytes start with a stray flag byte (%x)", rev, raw)
		}
	}
}
```

Create `pkg/replay/nativepayload/testdata/insert_one_string_row_54470.hex` with exactly one line, derived from the layout `WriteSampleBlock` and `EncodeBlock` already implement — the 12-byte measured prefix, name `s` (`0173`), type `String` (`06537472696e67`), the custom-serialization flag `00` because 54470 >= 54454, then the `ColStr` row `05` + `hello`:

```
0200010002ffffffff000101017306537472696e67000568656c6c6f
```

- [ ] **Step 6: Run the golden and tier tests**

```bash
bazel test //pkg/replay/nativepayload:nativepayload_test --test_filter='TestEncodeClientDataPacket_(MatchesCaptured|HeaderBytes)'
```

Expected: PASS. If a tier assertion fails, print `%x` and fix the byte index to the position right after the type string — the flag's position is the contract, do not weaken the check.

- [ ] **Step 7: Write the failing codec-method test**

Append to `pkg/chproto/codec_test.go` (add `"errors"`, `"strings"`, `"github.com/housegate/housegate/pkg/replay/nativepayload"` to its imports if absent):

```go
func TestCodec_EncodeClientDataPacket(t *testing.T) {
	values := proto.ColUInt64{1, 2, 3}
	cols := []proto.InputColumn{{Name: "v", Data: &values}}
	c := NewCodec(&readerWriter{r: &bytes.Buffer{}, w: &bytes.Buffer{}}, DirToUpstream)
	if _, err := c.EncodeClientDataPacket(cols); !errors.Is(err, ErrMalformed) {
		t.Fatalf("without SetRevision: err = %v, want ErrMalformed", err)
	}
	c.SetRevision(54470)
	raw, err := c.EncodeClientDataPacket(cols)
	if err != nil {
		t.Fatalf("EncodeClientDataPacket: %v", err)
	}
	want, err := nativepayload.EncodeClientDataPacket(54470, cols)
	if err != nil {
		t.Fatalf("nativepayload.EncodeClientDataPacket: %v", err)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("codec bytes\n got %x\nwant %x", raw, want)
	}
	info, err := InspectClientDataPacket(raw, proto.CompressionDisabled)
	if err != nil || info.BlockName != "" || info.Empty {
		t.Fatalf("InspectClientDataPacket = %+v, err %v; want an unnamed non-empty block", info, err)
	}
	// The codec frames at its own negotiated revision, never a constant: 54453
	// predates the per-column custom-serialization flag, so it is one byte shorter.
	c.SetRevision(54453)
	older, err := c.EncodeClientDataPacket(cols)
	if err != nil || len(older) != len(raw)-1 {
		t.Fatalf("54453 packet is %d bytes (err %v), want one fewer than 54470's %d", len(older), err, len(raw))
	}
	c.SetCompression(proto.CompressionEnabled)
	if _, err := c.EncodeClientDataPacket(cols); err == nil || !strings.Contains(err.Error(), "compress") {
		t.Fatalf("with compression: err = %v, want a refusal naming compression", err)
	}
}
```

- [ ] **Step 8: Run it and see it fail**

```bash
bazel test //pkg/chproto:chproto_test --test_filter='TestCodec_EncodeClientDataPacket'
```

Expected: build failure `c.EncodeClientDataPacket undefined (type *Codec has no field or method EncodeClientDataPacket)`.

- [ ] **Step 9: Write the codec method**

Append to `pkg/chproto/codec.go` after `WriteSampleBlock`, adding `"github.com/housegate/housegate/pkg/replay/nativepayload"` to the import block:

```go
// EncodeClientDataPacket frames one non-empty client Data packet carrying cols
// at the codec's negotiated revision and returns the bytes instead of writing
// them. It is the rows-bearing counterpart of WriteEmptyDataBlock and
// WriteSampleBlock: the signed INSERT lanes hash exactly these bytes into
// payload_hash and then forward them with WriteRawPacket, so the ingress
// captures byte for byte what was signed.
//
// Compression is refused. The storage-integrity signed lane never negotiates
// it, and a compressed frame would not be the payload the ingress stores.
func (c *Codec) EncodeClientDataPacket(cols []proto.InputColumn) ([]byte, error) {
	rev := c.Revision()
	if rev == 0 {
		return nil, fmt.Errorf("%w: EncodeClientDataPacket requires SetRevision first", ErrMalformed)
	}
	if c.Compression() == proto.CompressionEnabled {
		return nil, fmt.Errorf("%w: EncodeClientDataPacket cannot compress a signed client Data packet", ErrMalformed)
	}
	raw, err := nativepayload.EncodeClientDataPacket(rev, cols)
	if err != nil {
		return nil, fmt.Errorf("encode client data packet: %w", err)
	}
	return raw, nil
}
```

- [ ] **Step 10: Run gazelle and both suites**

```bash
bazel run //:gazelle
bazel test //pkg/chproto:chproto_test //pkg/replay/nativepayload:nativepayload_test
```

Expected: both PASS. Gazelle adds `//pkg/replay/nativepayload` to `chproto`'s library and test deps. No import cycle is possible: nothing under `pkg/replay/` imports `pkg/chproto`.

- [ ] **Step 11: Run the unit suite and commit**

```bash
bazel test //...
git add pkg/replay/nativepayload/encode.go pkg/replay/nativepayload/encode_test.go \
  pkg/replay/nativepayload/testdata/insert_one_string_row_54470.hex \
  pkg/replay/nativepayload/BUILD.bazel pkg/chproto/codec.go pkg/chproto/codec_test.go pkg/chproto/BUILD.bazel
git commit -m "$(cat <<'EOF'
feat(nativepayload): encode client Data packets as the decoder's inverse

EncodeClientDataPacket is Decode's inverse over the admitted column profile,
and Codec.EncodeClientDataPacket frames a rows-bearing client Data packet at
the codec's negotiated revision while refusing compression. The signed inline
VALUES lane hashes exactly these bytes.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
)"
```

Expected: `bazel test //...` matches the clean-`main` baseline (integration targets are `manual`).

---

---

### Task 3: `SynthesizedInsertPlan`, the materialize outcome record, the `inline_values` config block and the agent counters (D8 type, D2 record, D10, D11)

**Files:**
- Create: `pkg/plugin/synthesized_insert_test.go`, `pkg/plugins/sistatement/observer.go`, `pkg/proxy/observer_inline_values_test.go`
- Modify: `pkg/plugin/context.go` (field after `DeferredInsert *DeferredInsertPlan`, line 140; const after `SnapshotQueryAgentKey`, line 163; type after `DeferredInsertPlan`, lines 249-256); `pkg/plugins/materialize/materialize.go` (`Plugin.OnQuery`, lines 47-71); `pkg/plugins/materialize/materialize_test.go` (append); `pkg/config/storage_integrity_config.go` (consts line 13; `StorageIntegrityAgentConfig` lines 76-94; defaults lines 193-196; `validateAgent` lines 349-381); `pkg/config/storage_integrity_agent_config_test.go` (append); `pkg/proxy/observer.go` (vars lines 60-68, `init()` lines 70-84, methods after line 157)

**Interfaces:**
- Consumes: `chproto.SampleColumn{Name, Type string}`; ch-go `proto.InputColumn`; `rewriter.MaterializeOutcome{SQL, Changed, Code, Message}`; `Duration = cfgtypes.Duration`; `StorageIntegrityConfig.validateAgent(root *Config) error` — already called by `Config.Validate` at `pkg/config/config.go:309` in `ModeAgent`, so `config.go` itself needs no edit
- Produces: `type SynthesizedInsertPlan struct{ Blocks [][]proto.InputColumn; SampleColumns []chproto.SampleColumn; Rows uint64; PayloadBytes uint64; Packets [][]byte }`; `func (p *SynthesizedInsertPlan) Payload() []byte`; `QueryContext.SynthesizedInsert *SynthesizedInsertPlan`; `const ValuesKeyMaterialized = "materialize.outcome"`; `type StorageIntegrityInlineValuesConfig struct{ Enabled bool; EvaluationTimeout Duration; MaxRows uint64 }`; `StorageIntegrityAgentConfig.InlineValues`; `sistatement.Observer{ InlineValuesSynthesized(); InlineValuesEvaluationFailed(); InlineValuesClosureRefused() }`; `*proxy.MetricsObserver` implementing it over `clickhouse_proxy_agent_inline_values_total{result}`

- [ ] **Step 1: Write the failing test for the plan type and the Values key.** Create `pkg/plugin/synthesized_insert_test.go`:

```go
package plugin

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
)

func TestSynthesizedInsertPlan(t *testing.T) {
	plan := &SynthesizedInsertPlan{
		Blocks:        [][]proto.InputColumn{{{Name: "a", Data: &proto.ColInt64{1, 2}}}},
		SampleColumns: []chproto.SampleColumn{{Name: "a", Type: "Int64"}},
		Rows:          2,
		PayloadBytes:  4,
		Packets:       [][]byte{{0x02, 0x00}, {0x02, 0x01}},
	}
	if got, want := string(plan.Payload()), "\x02\x00\x02\x01"; got != want {
		t.Fatalf("Payload() = %q, want %q", got, want)
	}
	if got := uint64(len(plan.Payload())); got != plan.PayloadBytes {
		t.Fatalf("len(Payload()) = %d, want PayloadBytes %d", got, plan.PayloadBytes)
	}
	if got := (&SynthesizedInsertPlan{}).Payload(); len(got) != 0 {
		t.Fatalf("Payload() of an unencoded plan = %q, want empty", got)
	}
	var nilPlan *SynthesizedInsertPlan
	if got := nilPlan.Payload(); got != nil {
		t.Fatalf("nil plan Payload() = %q, want nil", got)
	}
	var qctx QueryContext
	if qctx.SynthesizedInsert != nil {
		t.Fatal("SynthesizedInsert must default nil so the ordinary path is unchanged")
	}
	if ValuesKeyMaterialized != "materialize.outcome" {
		t.Fatalf("ValuesKeyMaterialized = %q, want \"materialize.outcome\"", ValuesKeyMaterialized)
	}
}
```

Run `bazel run //:gazelle`, then `bazel test //pkg/plugin:plugin_test --test_filter='TestSynthesizedInsertPlan'` — expected failure: `undefined: SynthesizedInsertPlan`, `undefined: ValuesKeyMaterialized`.

- [ ] **Step 2: Add the field, the const and the type.** In `pkg/plugin/context.go` add `"github.com/ClickHouse/ch-go/proto"` to the imports (the package's other file aliases it `chgo`; `context.go` uses the plain name so the field reads as the contract spells it). Insert directly after `DeferredInsert *DeferredInsertPlan` (line 140):

```go
	// SynthesizedInsert, when set by a QueryPlugin during OnQuery, switches
	// Relay into synthesized-INSERT mode (spec 2026-09-23 D8): the agent
	// evaluated the rows, so Relay encodes the plan's blocks into client Data
	// packets with the upstream codec, hashes them through the strict
	// input-complete hook, and only then writes Query + packets + terminator
	// upstream. Mutually exclusive with DeferredInsert,
	// SuppressUpstreamExecution, AgentPrepare, QueryOnly and AbortWithSuccess;
	// Relay rejects a query that sets more than one of them.
	SynthesizedInsert *SynthesizedInsertPlan
```

After `SnapshotQueryAgentKey` (line 163):

```go
// ValuesKeyMaterialized is the Values key under which the agent-mode
// materialize plugin records its outcome: "applied", "noop" or
// "error:<reason>". The ordinary path ignores it and keeps failing open; the
// signed inline VALUES lane is fail-closed (spec 2026-09-23 D2) and refuses a
// statement whose key is absent or starts with "error:".
const ValuesKeyMaterialized = "materialize.outcome"
```

At the end of the file, after `DeferredInsertPlan`:

```go
// SynthesizedInsertPlan tells Relay how to run the synthesized-INSERT protocol
// for a statement whose rows the agent evaluated instead of the client
// streaming them. Blocks carry typed columns rather than bytes because the Data
// packet header depends on the upstream codec's negotiated revision; Packets
// and PayloadBytes are filled by the relay lane before
// OnQueryInputCompleteStrict, so the signed payload is exactly what goes on the
// wire. A SampleColumns mismatch against upstream is schema drift.
type SynthesizedInsertPlan struct {
	Blocks        [][]proto.InputColumn
	SampleColumns []chproto.SampleColumn
	Rows          uint64
	PayloadBytes  uint64
	Packets       [][]byte
}

// Payload returns the concatenation of Packets: the exact bytes the statement
// token hashes and the relay lane writes upstream.
func (p *SynthesizedInsertPlan) Payload() []byte {
	if p == nil || len(p.Packets) == 0 {
		return nil
	}
	out := make([]byte, 0, p.PayloadBytes)
	for _, packet := range p.Packets {
		out = append(out, packet...)
	}
	return out
}
```

Run `bazel test //pkg/plugin:plugin_test` — expected: PASS.

- [ ] **Step 3: Write the failing test for the materialize outcome record.** Append to `pkg/plugins/materialize/materialize_test.go`:

```go
func TestOnQuery_RecordsOutcomeForTheInlineLane(t *testing.T) {
	cases := []struct {
		name string
		mat  *fakeMat
		want string
	}{
		{"applied", &fakeMat{out: rewriter.MaterializeOutcome{SQL: "INSERT INTO t VALUES (toDateTime(1))", Changed: true, Code: pb.MaterializeCode_MaterializeSuccess}}, "applied"},
		{"noop", &fakeMat{out: rewriter.MaterializeOutcome{SQL: "INSERT INTO t VALUES (1)", Code: pb.MaterializeCode_MaterializeSuccess}}, "noop"},
		{"call error", &fakeMat{err: errors.New("dial tcp: refused")}, "error:dial tcp: refused"},
		{"non success", &fakeMat{out: rewriter.MaterializeOutcome{SQL: "INSERT INTO t VALUES (1)", Code: pb.MaterializeCode_MaterializeSyntaxError, Message: "boom"}}, "error:MaterializeSyntaxError: boom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			qctx := runOnQuery(t, &Plugin{Materializer: tc.mat}, "INSERT INTO t VALUES (1)")
			if got, ok := qctx.Values[plugin.ValuesKeyMaterialized].(string); !ok || got != tc.want {
				t.Fatalf("Values[%q] = %v, want %q", plugin.ValuesKeyMaterialized, qctx.Values[plugin.ValuesKeyMaterialized], tc.want)
			}
		})
	}
	// A disabled materializer must leave the key absent so the inline lane
	// refuses with "materialization did not run".
	if qctx := runOnQuery(t, &Plugin{}, "INSERT INTO t VALUES (1)"); qctx.Values[plugin.ValuesKeyMaterialized] != nil {
		t.Fatal("a nil Materializer must record nothing")
	}
}
```

Run `bazel test //pkg/plugins/materialize:materialize_test --test_filter='TestOnQuery_RecordsOutcomeForTheInlineLane'` — expected failure: `Values[...] = <nil>`, because `OnQuery` never writes the key (`runOnQuery` builds a `QueryContext` whose `Values` map is nil).

- [ ] **Step 4: Record the outcome in `materialize.OnQuery`.** Add the helper to `pkg/plugins/materialize/materialize.go` below `OnQuery`:

```go
// recordOutcome publishes this call's result for plugins that must fail closed
// on it (spec 2026-09-23 D2: the signed inline VALUES lane). The ordinary path
// never reads the key and keeps falling open.
func recordOutcome(qctx *plugin.QueryContext, outcome string) {
	if qctx.Values == nil {
		qctx.Values = make(map[string]any)
	}
	qctx.Values[plugin.ValuesKeyMaterialized] = outcome
}
```

Then add exactly four calls in `OnQuery`, each immediately after the `p.Observer` block of its branch and before that branch's log line, leaving every existing statement and every fail-open `return nil` untouched:

1. transport-error branch (after `p.Observer.MaterializeCallError()`): `recordOutcome(qctx, "error:"+err.Error())`
2. non-success branch (after `p.Observer.MaterializeNonSuccess(out.Code.String())`): `recordOutcome(qctx, "error:"+out.Code.String()+": "+out.Message)`
3. `out.Changed` branch (after `p.Observer.MaterializeApplied()`): `recordOutcome(qctx, "applied")`
4. final no-op branch (after `p.Observer.MaterializeNoop()`): `recordOutcome(qctx, "noop")`

The early `if p.Materializer == nil || qctx.Query == nil || qctx.Query.Body == "" { return nil }` guard stays first and records nothing, which is what makes a missing key mean "materialization did not run". Run `bazel test //pkg/plugins/materialize:materialize_test` — expected: PASS, pre-existing fail-open tests included.

- [ ] **Step 5: Write the failing config test.** Append to `pkg/config/storage_integrity_agent_config_test.go`, adding `"time"` and `materializeplugin "github.com/housegate/housegate/pkg/plugins/materialize"` to its imports (`//pkg/plugins/materialize` is already a dep of `//pkg/config:config_test`, so BUILD is unchanged):

```go
func inlineValuesBase() *Config {
	c := agentSIBase() // agent SI on, materialize off
	c.Materialize.Enabled = true
	c.Materialize.Engine = "native"
	c.StorageIntegrity.Agent.InlineValues.Enabled = true
	return c
}

func TestStorageIntegrityInlineValuesConfig(t *testing.T) {
	iv := Default().StorageIntegrity.Agent.InlineValues
	if iv.Enabled || iv.EvaluationTimeout.Duration != 10*time.Second || iv.MaxRows != 65536 {
		t.Fatalf("defaults = %+v, want disabled with 10s / 65536", iv)
	}
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"valid", func(*Config) {}, ""},
		{"materialize disabled", func(c *Config) { c.Materialize = materializeplugin.Config{} }, "requires materialize.enabled"},
		{"agent SI disabled", func(c *Config) { c.StorageIntegrity.Agent.Enabled = false }, "requires storage_integrity.agent.enabled"},
		{"sub-second timeout", func(c *Config) {
			c.StorageIntegrity.Agent.InlineValues.EvaluationTimeout = Duration{Duration: 900 * time.Millisecond}
		}, "evaluation_timeout must be at least 1s"},
		{"zero timeout", func(c *Config) {
			c.StorageIntegrity.Agent.InlineValues.EvaluationTimeout = Duration{}
		}, "evaluation_timeout must be at least 1s"},
		{"zero max_rows", func(c *Config) { c.StorageIntegrity.Agent.InlineValues.MaxRows = 0 }, "max_rows must be > 0"},
		{"disabled block ignores its own limits", func(c *Config) {
			c.StorageIntegrity.Agent.InlineValues = StorageIntegrityInlineValuesConfig{}
			c.Materialize = materializeplugin.Config{}
		}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := inlineValuesBase()
			tc.mutate(c)
			err := c.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}
```

Run `bazel test //pkg/config:config_test --test_filter='TestStorageIntegrityInlineValuesConfig'` — expected failure: `c.StorageIntegrity.Agent.InlineValues undefined`.

- [ ] **Step 6: Add the config block, its defaults and its validation.** In `pkg/config/storage_integrity_config.go` add to the `const` block at line 13:

```go
	defaultStorageIntegrityInlineValuesTimeout          = 10 * time.Second
	defaultStorageIntegrityInlineValuesMaxRows   uint64 = 65536
```

Add the field as the last member of `StorageIntegrityAgentConfig` (after `RequireNetworkState`) and the new type right below that struct:

```go
	// InlineValues turns on the signed inline INSERT ... VALUES lane
	// (spec 2026-09-23). Default off; it requires materialize.enabled.
	InlineValues StorageIntegrityInlineValuesConfig `json:"inline_values" yaml:"inline_values"`
}

// StorageIntegrityInlineValuesConfig is the agent-mode signed inline
// INSERT ... VALUES lane: the agent closes the rows lexically, evaluates them
// once through its own ClickHouse, encodes Native blocks and signs them.
// storage_integrity.agent.max_payload_bytes bounds the encoded payload.
type StorageIntegrityInlineValuesConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
	// EvaluationTimeout caps the helper evaluation query (default 10s, min 1s).
	EvaluationTimeout Duration `json:"evaluation_timeout" yaml:"evaluation_timeout"`
	// MaxRows bounds the evaluated row count (default 65536, must be > 0).
	MaxRows uint64 `json:"max_rows" yaml:"max_rows"`
}
```

Extend the `Agent` stanza of `defaultStorageIntegrityConfig` (lines 193-196):

```go
		Agent: StorageIntegrityAgentConfig{
			MaxPayloadBytes:     defaultStorageIntegrityMaxPayloadBytes,
			RequireNetworkState: true,
			InlineValues: StorageIntegrityInlineValuesConfig{
				EvaluationTimeout: Duration{Duration: defaultStorageIntegrityInlineValuesTimeout},
				MaxRows:           defaultStorageIntegrityInlineValuesMaxRows,
			},
		},
```

In `validateAgent` the inline-values rules must run even when `a.Enabled` is false (so `inline_values` on top of a disabled agent block is caught), so the early `if !a.Enabled { return nil }` moves below them. Replace the function head — from `a := c.Agent` down to and including `var errs []error` — with:

```go
	a := c.Agent
	var errs []error
	if iv := a.InlineValues; iv.Enabled {
		// Spec D2: the lane is fail-closed on materialization, so a disabled
		// materialize block would reject every inline statement at query time
		// instead of at startup.
		if !root.Materialize.Enabled {
			errs = append(errs, errors.New("storage_integrity.agent.inline_values requires materialize.enabled"))
		}
		if !a.Enabled {
			errs = append(errs, errors.New("storage_integrity.agent.inline_values requires storage_integrity.agent.enabled"))
		}
		if iv.EvaluationTimeout.Duration < time.Second {
			errs = append(errs, fmt.Errorf("storage_integrity.agent.inline_values.evaluation_timeout must be at least 1s, got %s", iv.EvaluationTimeout.Duration))
		}
		if iv.MaxRows == 0 {
			errs = append(errs, errors.New("storage_integrity.agent.inline_values.max_rows must be > 0"))
		}
	}
	if !a.Enabled {
		if joined := errors.Join(errs...); joined != nil {
			return fmt.Errorf("storage_integrity.agent: %w", joined)
		}
		return nil
	}
```

Every existing check below (`network_id`, `keeper_shard_id`, `state_dir`, `max_payload_bytes`, `agent.private_key_hex`, `network_state.source`) and the closing `errors.Join` tail stay exactly as they are. Run `bazel test //pkg/config:config_test` — expected: PASS, including `TestStorageIntegrityAgentConfig_Validate`'s `"disabled block ignored"` case, which now returns from the new early exit.

- [ ] **Step 7: Write the failing test for the observer and its counters.** Create `pkg/proxy/observer_inline_values_test.go`:

```go
package proxy

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/plugins/sistatement"
)

// The metric globals are init()-registered, so this asserts deltas rather than
// absolute values and never re-registers a collector.
func TestMetricsObserver_InlineValuesCounters(t *testing.T) {
	var obs sistatement.Observer = NewMetricsObserver()
	results := []string{"synthesized", "evaluation_failed", "closure_refused"}
	before := map[string]float64{}
	for _, result := range results {
		before[result] = testutil.ToFloat64(agentInlineValuesTotal.WithLabelValues(result))
	}
	obs.InlineValuesSynthesized()
	obs.InlineValuesEvaluationFailed()
	obs.InlineValuesClosureRefused()
	for _, result := range results {
		if delta := testutil.ToFloat64(agentInlineValuesTotal.WithLabelValues(result)) - before[result]; delta != 1 {
			t.Fatalf("result=%q counter moved by %v, want 1", result, delta)
		}
	}
	const want = `
# HELP clickhouse_proxy_agent_inline_values_total Agent-mode signed inline INSERT ... VALUES outcomes
# TYPE clickhouse_proxy_agent_inline_values_total counter
clickhouse_proxy_agent_inline_values_total{result="metric_name_probe"} 1
`
	agentInlineValuesTotal.WithLabelValues("metric_name_probe").Inc()
	probe := agentInlineValuesTotal.MustCurryWith(map[string]string{"result": "metric_name_probe"})
	if err := testutil.CollectAndCompare(probe, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}
```

Run `bazel run //:gazelle`, then `bazel test //pkg/proxy:proxy_test --test_filter='TestMetricsObserver_InlineValuesCounters'` — expected failure: `undefined: agentInlineValuesTotal`, `undefined: sistatement.Observer`.

- [ ] **Step 8: Add the `sistatement.Observer` interface and the counters.** Create `pkg/plugins/sistatement/observer.go`:

```go
package sistatement

// Observer is the narrow metrics surface of the signed inline
// INSERT ... VALUES lane (spec 2026-09-23 D11): statements synthesized,
// refusals from the evaluation step (exception, timeout, transport error,
// empty result, type mismatch, row/byte limit) and refusals from the lexical
// closure gate, which are raised before anything reaches ClickHouse.
// *proxy.MetricsObserver satisfies it; a nil Observer disables metrics.
type Observer interface {
	InlineValuesSynthesized()
	InlineValuesEvaluationFailed()
	InlineValuesClosureRefused()
}
```

In `pkg/proxy/observer.go` add to the package `var` block after `agentMaterializeTotal` (line 68):

```go
	agentInlineValuesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_agent_inline_values_total",
		Help: "Agent-mode signed inline INSERT ... VALUES outcomes",
	}, []string{"result"})
```

Add `prometheus.MustRegister(agentInlineValuesTotal)` as the last line of `init()`, and after `MaterializeCallError` (line 157):

```go
func (m *MetricsObserver) InlineValuesSynthesized() {
	agentInlineValuesTotal.WithLabelValues("synthesized").Inc()
}
func (m *MetricsObserver) InlineValuesEvaluationFailed() {
	agentInlineValuesTotal.WithLabelValues("evaluation_failed").Inc()
}
func (m *MetricsObserver) InlineValuesClosureRefused() {
	agentInlineValuesTotal.WithLabelValues("closure_refused").Inc()
}
```

Run `bazel run //:gazelle`, then `bazel test //pkg/proxy:proxy_test --test_filter='TestMetricsObserver_InlineValuesCounters'` — expected: PASS. Gazelle infers both new test deps from the imports; confirm `pkg/proxy/BUILD.bazel`'s `go_test` now lists `"//pkg/plugins/sistatement"` and `"@com_github_prometheus_client_golang//prometheus/testutil"`, and add them by hand if it did not (no cycle: `//pkg/plugins/sistatement` does not depend on `//pkg/proxy`).

- [ ] **Step 9: Run every touched package and commit.** Run `bazel run //:gazelle`, then `bazel test //pkg/plugin:plugin_test //pkg/plugins/materialize:materialize_test //pkg/plugins/sistatement:sistatement_test //pkg/config:config_test //pkg/proxy:proxy_test` — expected: all PASS, with the deferred-lane tests unaffected because the new field defaults nil. Then:

```
git add pkg/plugin/context.go pkg/plugin/synthesized_insert_test.go pkg/plugin/BUILD.bazel pkg/plugins/materialize/materialize.go pkg/plugins/materialize/materialize_test.go pkg/plugins/sistatement/observer.go pkg/plugins/sistatement/BUILD.bazel pkg/config/storage_integrity_config.go pkg/config/storage_integrity_agent_config_test.go pkg/proxy/observer.go pkg/proxy/observer_inline_values_test.go pkg/proxy/BUILD.bazel
git commit -m "feat(plugin): add SynthesizedInsertPlan, inline_values config and counters" -m "Add QueryContext.SynthesizedInsert and its plan type (mutually exclusive with the other lanes), record the materialize outcome under plugin.ValuesKeyMaterialized so the fail-closed inline lane can read it, add the default-off storage_integrity.agent.inline_values block with defaults and validation including the cross-field materialize.enabled rule, and declare sistatement.Observer implemented on *proxy.MetricsObserver over clickhouse_proxy_agent_inline_values_total. Implements spec D2 record, D8 type, D10 and D11." -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Evaluator over a dedicated upstream connection and the inline branch of `sistatement`

Implements D2 (materialization mandatory, fail-closed), D4 (rows evaluated once, no retry), D6 (the signed SQL is the rewritten statement), the agent half of D9 (nothing signed and no `client_seq` consumed on a rejection), D10's plugin-side options and D11's observer calls. Code blocks use single-line `if` bodies for density; run `gofmt -w` before committing.

**Files:** Create `pkg/plugins/sistatement/inline_values.go`, `inline_values_test.go`, `values_evaluator.go`, `values_evaluator_test.go`. Modify `pkg/plugins/sistatement/plugin.go` (`Options` `:12-19`, `Plugin` `:22-35`, `New` `:68-104`, `OnQuery` fallthrough `:75-81` and plan install `:132`, `OnQueryInputCompleteStrict` `:205-208`), `pkg/plugins/agent/signer.go` (`:100-105`), `pkg/plugins/agent/signer_test.go`, `pkg/plugins/sistatement/BUILD.bazel` (gazelle).

**Interfaces:**

- Consumes: `sicore.ParseInlineValuesInsert(sql string) (InlineValuesInsert, error)`, `sicore.ErrNotInlineValues`, `sicore.ValuesClosure(rows string) error`, `sicore.InlineValuesErrorPrefix` (Task 1); `plugin.SynthesizedInsertPlan`, `plugin.ValuesKeyMaterialized`, `sistatement.Observer` (Task 3); `chproto.Codec` (`WriteClientHello`/`ReadPacket`/`SetRevision`/`SetServerHelloRevisionHint`/`SetCompression`/`ResolveUpstreamAddendum`/`SendAddendum`/`WriteQuery`/`WriteEmptyDataBlock`/`Revision`/`Conn`), `chproto.ClientHelloForUpstream`, `chproto.SupportsAddendum`, `payloadexec.ResolveColumnProfile`, `sqlident.Quote`, `sqlident.NormalizePath`, `auth.Signer`.
- Produces: `type ValuesEvaluator interface { Evaluate(ctx context.Context, req ValuesEvaluation) ([][]proto.InputColumn, error) }`; `type ValuesEvaluation struct { Hello *chproto.ClientHello; Account string; Schema payloadexec.TableSchema; Columns []string; Rows string; Timeout time.Duration; MaxRows uint64; MaxBytes uint64 }`; `type InlineValuesOptions struct { Enabled bool; EvaluationTimeout time.Duration; MaxRows uint64 }`; `func NewUpstreamValuesEvaluator(dial func(ctx context.Context) (net.Conn, error), signer auth.Signer) *UpstreamValuesEvaluator`; `func (e *UpstreamValuesEvaluator) Evaluate(ctx context.Context, req ValuesEvaluation) ([][]proto.InputColumn, error)`; `func (e *UpstreamValuesEvaluator) Close() error`.

**Steps:**

- [ ] **Step 1: Write the failing `New` validation test.** Create `pkg/plugins/sistatement/inline_values_test.go`:

```go
package sistatement

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type fakeEvaluator struct {
	blocks [][]proto.InputColumn
	err    error
	seen   ValuesEvaluation
	calls  int
}

func (f *fakeEvaluator) Evaluate(_ context.Context, req ValuesEvaluation) ([][]proto.InputColumn, error) {
	f.calls++
	f.seen = req
	return f.blocks, f.err
}

// evaluatedBlock builds one row for testSchema (id UInt64, region String, amount Float64).
func evaluatedBlock(id uint64, region string, amount float64) []proto.InputColumn {
	i, r, a := &proto.ColUInt64{}, &proto.ColStr{}, &proto.ColFloat64{}
	i.Append(id)
	r.Append(region)
	a.Append(amount)
	return []proto.InputColumn{{Name: "id", Data: i}, {Name: "region", Data: r}, {Name: "amount", Data: a}}
}

func inlineOptions(t *testing.T, ev ValuesEvaluator, declare bool) (Options, *SeqCounter) {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	if declare { declareSchema(t, ns, testSchema()) }
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil { t.Fatal(err) }
	seq, err := OpenSeqCounter(t.TempDir(), signer.Address())
	if err != nil { t.Fatal(err) }
	return Options{
		Signer: signer, Schemas: ns, NetworkID: testNetworkID, Seq: seq, MaxPayloadBytes: 1 << 20, Evaluator: ev,
		InlineValues: InlineValuesOptions{Enabled: true, EvaluationTimeout: 5 * time.Second, MaxRows: 1000},
	}, seq
}

func newInlinePlugin(t *testing.T, ev ValuesEvaluator) (*Plugin, *SeqCounter) {
	t.Helper()
	opts, seq := inlineOptions(t, ev, true)
	p, err := New(opts)
	if err != nil { t.Fatalf("New: %v", err) }
	return p, seq
}

func inlineQctx(sess *fakeSession, sql string) *plugin.QueryContext {
	qctx := insertQctx(sess, sql)
	qctx.Values[plugin.ValuesKeyMaterialized] = "noop"
	return qctx
}

func TestNew_InlineValuesOptionsAreValidated(t *testing.T) {
	cases := []struct {
		name, wantErr string
		mutate        func(*Options)
	}{
		{"no evaluator", "values evaluator is required", func(o *Options) { o.Evaluator = nil }},
		{"timeout too small", "evaluation timeout", func(o *Options) { o.InlineValues.EvaluationTimeout = time.Millisecond }},
		{"zero max rows", "max rows", func(o *Options) { o.InlineValues.MaxRows = 0 }},
		{"disabled needs no evaluator", "", func(o *Options) { o.Evaluator, o.InlineValues = nil, InlineValuesOptions{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, _ := inlineOptions(t, &fakeEvaluator{}, false)
			tc.mutate(&opts)
			_, err := New(opts)
			if tc.wantErr == "" {
				if err != nil { t.Fatalf("New: %v", err) }
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("New err = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2: Run it and see it fail.** `bazel test //pkg/plugins/sistatement:sistatement_test --test_filter='TestNew_InlineValuesOptionsAreValidated'` — expected failure: compile errors `unknown field Evaluator in struct literal of type Options` and `undefined: InlineValuesOptions`.

- [ ] **Step 3: Add the option types and the `Options`/`Plugin`/`New` wiring.** Create `pkg/plugins/sistatement/inline_values.go` with the type block:

```go
package sistatement

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sqlident"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// ValuesEvaluation is one request to turn an inline VALUES row list into
// typed Native blocks (spec D4). Hello is a clone of the session's stored upstream hello with the signing database applied; Account is for logging only.
type ValuesEvaluation struct {
	Hello    *chproto.ClientHello
	Account  string
	Schema   payloadexec.TableSchema
	Columns  []string
	Rows     string
	Timeout  time.Duration
	MaxRows  uint64
	MaxBytes uint64
}

// ValuesEvaluator evaluates inline VALUES rows through the tenant's own
// ClickHouse. Every failure is terminal; the lane never retries.
type ValuesEvaluator interface {
	Evaluate(ctx context.Context, req ValuesEvaluation) ([][]proto.InputColumn, error)
}

// InlineValuesOptions is the plugin-side view of storage_integrity.agent.inline_values.
type InlineValuesOptions struct {
	Enabled           bool
	EvaluationTimeout time.Duration
	MaxRows           uint64
}

func inlineErrorf(format string, args ...any) error {
	return fmt.Errorf(sicore.InlineValuesErrorPrefix+format, args...)
}

func inlineWrap(err error) error { return fmt.Errorf("%s%w", sicore.InlineValuesErrorPrefix, err) }
```

  In `plugin.go` add `"time"` to the imports, append to `Options` before its closing brace:

```go
	// InlineValues configures the signed inline VALUES lane (spec D10).
	InlineValues InlineValuesOptions
	// Evaluator is required when InlineValues.Enabled.
	Evaluator ValuesEvaluator
	// Observer is the narrow metrics surface; nil disables it.
	Observer Observer
```

  replace the `Plugin` field line `	maxPayload    uint64` with `	maxPayload    uint64` followed by `	inline        InlineValuesOptions`, `	evaluator     ValuesEvaluator`, `	observer      Observer`; insert into `New` before `	if joined := errors.Join(errs...); joined != nil {`:

```go
	if opts.InlineValues.Enabled {
		if opts.Evaluator == nil { errs = append(errs, errors.New("values evaluator is required when inline_values is enabled")) }
		if opts.InlineValues.EvaluationTimeout < time.Second {
			errs = append(errs, fmt.Errorf("inline values evaluation timeout must be >= 1s, got %s", opts.InlineValues.EvaluationTimeout))
		}
		if opts.InlineValues.MaxRows == 0 { errs = append(errs, errors.New("inline values max rows must be > 0")) }
	}
```

  and replace `		maxPayload:    opts.MaxPayloadBytes,` in the returned literal with:

```go
		maxPayload:    opts.MaxPayloadBytes,
		inline:        opts.InlineValues,
		evaluator:     opts.Evaluator,
		observer:      opts.Observer,
```

- [ ] **Step 4: Run the validation test.** `bazel test //pkg/plugins/sistatement:sistatement_test --test_filter='TestNew_InlineValuesOptionsAreValidated'` — expected PASS.

- [ ] **Step 5: Write the failing inline-branch tests.** Append to `inline_values_test.go`. `TestPlugin_InlineValuesClaimsOnlyTheInlineShape` is the feature-on counterpart of the existing `TestPlugin_NonSILaneStatementsPassThrough` (`plugin_test.go:397-406`), which stays unchanged as the feature-off regression:

```go
func TestPlugin_InlineValuesInstallsSynthesizedPlan(t *testing.T) {
	ev := &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5), evaluatedBlock(2, "us", 2.5)}}
	p, seq := newInlinePlugin(t, ev)
	sql := "INSERT INTO shop.orders (id, region, amount) VALUES (1, 'eu', 1.5), (2, 'us', 2.5)"
	qctx := inlineQctx(newSession(7, ""), sql)
	if err := p.OnQuery(context.Background(), qctx); err != nil { t.Fatalf("OnQuery: %v", err) }
	if qctx.DeferredInsert != nil { t.Fatal("the inline lane must not install a deferred plan") }
	plan := qctx.SynthesizedInsert
	if plan == nil { t.Fatal("SynthesizedInsert is nil") }
	if want := "INSERT INTO shop.orders (id, region, amount) FORMAT Native"; qctx.Query.Body != want {
		t.Fatalf("rewritten body = %q, want %q", qctx.Query.Body, want)
	}
	if len(plan.Blocks) != 2 || plan.Rows != 2 { t.Fatalf("plan blocks=%d rows=%d, want 2/2", len(plan.Blocks), plan.Rows) }
	if len(plan.SampleColumns) != 3 || plan.SampleColumns[1].Name != "region" || plan.SampleColumns[1].Type != "String" {
		t.Fatalf("sample columns = %+v", plan.SampleColumns)
	}
	if ev.calls != 1 || ev.seen.Rows != "(1, 'eu', 1.5), (2, 'us', 2.5)" { t.Fatalf("calls=%d rows=%q", ev.calls, ev.seen.Rows) }
	if got := ev.seen.Columns; len(got) != 3 || got[0] != "id" || got[2] != "amount" { t.Fatalf("columns = %v", got) }
	if ev.seen.MaxRows != 1000 || ev.seen.MaxBytes != 1<<20 || ev.seen.Timeout != 5*time.Second {
		t.Fatalf("evaluation limits = %+v", ev.seen)
	}
	if _, s, _, err := sicore.ParseFlatStatementID(qctx.Query.ID); err != nil || s != 1 || seq.Last() != 1 {
		t.Fatalf("statement id %q: seq=%d last=%d err=%v", qctx.Query.ID, s, seq.Last(), err)
	}
}

// TestPlugin_InlineValuesClaimsOnlyTheInlineShape is the feature-on mirror
// of TestPlugin_NonSILaneStatementsPassThrough: with the flag on the inline VALUES statement of that list is claimed and the others still fall through (D1).
func TestPlugin_InlineValuesClaimsOnlyTheInlineShape(t *testing.T) {
	for _, sql := range []string{"SELECT 1", "INSERT INTO shop.orders SELECT * FROM src", "CREATE TABLE x (a UInt8) ENGINE=Memory"} {
		p, seq := newInlinePlugin(t, &fakeEvaluator{})
		qctx := inlineQctx(newSession(21, ""), sql)
		if err := p.OnQuery(context.Background(), qctx); err != nil { t.Fatalf("%q: OnQuery: %v", sql, err) }
		if qctx.SynthesizedInsert != nil || qctx.DeferredInsert != nil || qctx.Query.ID != "client-uuid-1" || seq.Last() != 0 {
			t.Fatalf("%q was claimed by the inline lane", sql)
		}
	}
	p, _ := newInlinePlugin(t, &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}})
	qctx := inlineQctx(newSession(22, ""), "INSERT INTO shop.orders VALUES (1, 'eu', 1.5)")
	if err := p.OnQuery(context.Background(), qctx); err != nil { t.Fatalf("OnQuery: %v", err) }
	if qctx.SynthesizedInsert == nil { t.Fatal("an inline VALUES INSERT without a column list must be claimed") }
	if want := "INSERT INTO shop.orders (id, region, amount) FORMAT Native"; qctx.Query.Body != want {
		t.Fatalf("rewritten body = %q, want the declared column order %q", qctx.Query.Body, want)
	}
}

func TestPlugin_InlineValuesRejectionsConsumeNoSeq(t *testing.T) {
	sql := "INSERT INTO shop.orders (id, region, amount) VALUES (1, 'eu', 1.5)"
	cases := []struct {
		name, wantErr string
		ev            *fakeEvaluator
		mutate        func(*plugin.QueryContext)
	}{
		{"closure refused", "currentDatabase", &fakeEvaluator{}, func(q *plugin.QueryContext) {
			q.Query.Body = "INSERT INTO shop.orders (id, region, amount) VALUES (1, currentDatabase(), 1.5)"
		}},
		{"evaluation failed", "code=62", &fakeEvaluator{err: errors.New("code=62 syntax error")}, nil},
		{"materialization missing", "materialization did not run", &fakeEvaluator{}, func(q *plugin.QueryContext) {
			delete(q.Values, plugin.ValuesKeyMaterialized)
		}},
		{"materialization failed", "materialization failed: engine unavailable", &fakeEvaluator{}, func(q *plugin.QueryContext) {
			q.Values[plugin.ValuesKeyMaterialized] = "error:engine unavailable"
		}},
		{"column type mismatch", "wire type", &fakeEvaluator{blocks: [][]proto.InputColumn{{
			{Name: "id", Data: &proto.ColStr{}}, {Name: "region", Data: &proto.ColStr{}}, {Name: "amount", Data: &proto.ColFloat64{}},
		}}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, seq := newInlinePlugin(t, tc.ev)
			qctx := inlineQctx(newSession(9, ""), sql)
			if tc.mutate != nil { tc.mutate(qctx) }
			body := qctx.Query.Body
			err := p.OnQuery(context.Background(), qctx)
			if err == nil { t.Fatal("OnQuery accepted a statement it must refuse") }
			if !strings.HasPrefix(err.Error(), sicore.InlineValuesErrorPrefix) || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %q, want prefix %q containing %q", err, sicore.InlineValuesErrorPrefix, tc.wantErr)
			}
			if qctx.SynthesizedInsert != nil || qctx.DeferredInsert != nil { t.Fatal("a refused statement installed a plan") }
			if qctx.Query.ID != "client-uuid-1" || qctx.Query.Body != body { t.Fatalf("a refused statement mutated the query") }
			if seq.Last() != 0 { t.Fatalf("a refused statement consumed client_seq (last=%d)", seq.Last()) }
		})
	}
}
```

- [ ] **Step 6: Run them and see them fail.** `bazel test //pkg/plugins/sistatement:sistatement_test --test_filter='TestPlugin_InlineValues'` — expected failure: `SynthesizedInsert is nil` in the first two tests, `OnQuery accepted a statement it must refuse` in every rejection subtest.

- [ ] **Step 7: Implement the inline branch.** Append to `inline_values.go`:

```go
// inlineValuesCandidate reports whether sql is an inline VALUES INSERT this
// lane claims. ok=false keeps today's fallthrough exactly (feature off, the 25.x truncated shape, FORMAT, SELECT, WITH, non-INSERT); an error is a prefixed refusal of an INSERT ... VALUES the lane will not sign.
func (p *Plugin) inlineValuesCandidate(sql string) (sicore.InlineValuesInsert, bool, error) {
	if !p.inline.Enabled { return sicore.InlineValuesInsert{}, false, nil }
	parsed, err := sicore.ParseInlineValuesInsert(sql)
	if err != nil {
		if errors.Is(err, sicore.ErrNotInlineValues) { return sicore.InlineValuesInsert{}, false, nil }
		return sicore.InlineValuesInsert{}, false, inlineWrap(err)
	}
	return parsed, true, nil
}

// requireMaterialized enforces spec D2: fail-closed on the materialize
// plugin's outcome, because the ingress would reject any residual volatile function anyway and an agent-side error is the clearer one.
func requireMaterialized(qctx *plugin.QueryContext) error {
	outcome, ok := qctx.Values[plugin.ValuesKeyMaterialized].(string)
	if !ok { return inlineErrorf("materialization did not run") }
	if reason, isErr := strings.CutPrefix(outcome, "error:"); isErr {
		return inlineErrorf("materialization failed: %s", reason)
	}
	return nil
}

// evaluateInlineValues runs the closure gate and the single evaluation, then
// validates the blocks against the INSERT column order and the declared wire types. It runs before statementIDFor, so nothing it refuses consumes a client_seq (spec D9).
func (p *Plugin) evaluateInlineValues(ctx context.Context, qctx *plugin.QueryContext, parsed sicore.InlineValuesInsert,
	schema payloadexec.TableSchema, cols []chproto.SampleColumn) (*plugin.SynthesizedInsertPlan, error) {
	if err := requireMaterialized(qctx); err != nil { return nil, err }
	if err := sicore.ValuesClosure(parsed.Rows); err != nil {
		p.observeInline(func(o Observer) { o.InlineValuesClosureRefused() })
		return nil, inlineWrap(err)
	}
	hello := qctx.Session.State().UpstreamHello()
	if hello == nil { return nil, inlineErrorf("session has no stored upstream hello; cannot open an evaluation connection") }
	if db := p.sessionDatabase(qctx.Session); db != "" { hello.Database = db }
	names := make([]string, len(cols))
	for i, c := range cols { names[i] = c.Name }
	evalCtx, cancel := context.WithTimeout(ctx, p.inline.EvaluationTimeout)
	defer cancel()
	blocks, err := p.evaluator.Evaluate(evalCtx, ValuesEvaluation{
		Hello: hello, Account: p.account, Schema: schema, Columns: names, Rows: parsed.Rows,
		Timeout: p.inline.EvaluationTimeout, MaxRows: p.inline.MaxRows, MaxBytes: p.maxPayload,
	})
	if err != nil {
		p.observeInline(func(o Observer) { o.InlineValuesEvaluationFailed() })
		return nil, inlineWrap(err)
	}
	plan := &plugin.SynthesizedInsertPlan{SampleColumns: cols}
	for _, block := range blocks {
		if len(block) != len(cols) {
			return nil, inlineErrorf("evaluated block has %d columns, the INSERT lists %d", len(block), len(cols))
		}
		rows := block[0].Data.Rows()
		if rows == 0 { continue }
		for i, col := range block {
			if col.Name != cols[i].Name {
				return nil, inlineErrorf("evaluated column %d is %q, the INSERT lists %q", i, col.Name, cols[i].Name)
			}
			if col.Data.Rows() != rows {
				return nil, inlineErrorf("evaluated column %q has %d rows, %q has %d", col.Name, col.Data.Rows(), block[0].Name, rows)
			}
			profile, perr := payloadexec.ResolveColumnProfile(cols[i].Type)
			if perr != nil { return nil, inlineErrorf("column %q: %s", cols[i].Name, perr) }
			if got := string(col.Data.Type()); got != profile.NativeWireType {
				return nil, inlineErrorf("evaluated column %q has wire type %q, the declared type %q requires %q",
					col.Name, got, cols[i].Type, profile.NativeWireType)
			}
		}
		plan.Blocks = append(plan.Blocks, block)
		plan.Rows += uint64(rows)
	}
	if plan.Rows == 0 { return nil, inlineErrorf("evaluation produced no rows") }
	if plan.Rows > p.inline.MaxRows {
		return nil, inlineErrorf("evaluation produced %d rows, max_rows is %d", plan.Rows, p.inline.MaxRows)
	}
	p.observeInline(func(o Observer) { o.InlineValuesSynthesized() })
	return plan, nil
}

// inlineInsertBody renders the signed statement text (spec D6): the resolved
// target and the INSERT column order, backtick-quoted only where ClickHouse needs it, followed by FORMAT Native.
func inlineInsertBody(target sicore.InsertTarget, cols []chproto.SampleColumn) string {
	names := make([]string, len(cols))
	for i, c := range cols { names[i] = sqlident.NormalizePath(c.Name) }
	return "INSERT INTO " + target.CanonicalID() + " (" + strings.Join(names, ", ") + ") FORMAT Native"
}

func (p *Plugin) observeInline(fn func(Observer)) {
	if p.observer != nil { fn(p.observer) }
}
```

  In `plugin.go`'s `OnQuery` replace the fallthrough block:

```go
	if _, err := sicore.InsertPayloadEncoding(sql); err != nil {
		if errors.Is(err, sicore.ErrInsertIntoFunction) || errors.Is(err, sicore.ErrBackslashEscapedIdentifier) {
			return fmt.Errorf("storage_integrity agent: %w", err)
		}
		// VALUES / SELECT / non-INSERT: ordinary path.
		return nil
	}
```

  with:

```go
	// Spec D1/D6: InsertPayloadEncoding refuses the 26.x inline VALUES shape
	// because no payload arrives on the wire. With the lane enabled the statement is claimed here and its rows are evaluated below; every other shape keeps falling through exactly as before.
	var inline *sicore.InlineValuesInsert
	if _, err := sicore.InsertPayloadEncoding(sql); err != nil {
		if errors.Is(err, sicore.ErrInsertIntoFunction) || errors.Is(err, sicore.ErrBackslashEscapedIdentifier) {
			return fmt.Errorf("storage_integrity agent: %w", err)
		}
		parsed, claimed, perr := p.inlineValuesCandidate(sql)
		if perr != nil { return perr }
		if !claimed {
			// VALUES / SELECT / non-INSERT: ordinary path.
			return nil
		}
		inline = &parsed
	}
```

  insert after the `proto.FeatureSettingsSerializedAsStrings` guard, before `	statementID, err := p.statementIDFor(qctx.Query.ID)`:

```go
	var synthesized *plugin.SynthesizedInsertPlan
	if inline != nil {
		synthesized, err = p.evaluateInlineValues(ctx, qctx, *inline, schema, cols)
		if err != nil { return err }
	}
```

  replace the install line `	qctx.DeferredInsert = &plugin.DeferredInsertPlan{SampleColumns: cols, MaxPayloadBytes: p.maxPayload}` with:

```go
	_, logger := log.FromContext(ctx)
	if synthesized != nil {
		qctx.Query.Body = inlineInsertBody(target, cols)
		qctx.SynthesizedInsert = synthesized
		// D11: the original statement text is debug-only, never info or above.
		logger.Debugw("sistatement: inline VALUES synthesized", "statement_id", statementID, "original_sql", sql)
	} else {
		qctx.DeferredInsert = &plugin.DeferredInsertPlan{SampleColumns: cols, MaxPayloadBytes: p.maxPayload}
	}
```

  and delete the now-duplicate `	_, logger := log.FromContext(ctx)` line that precedes the existing `logger.Debugw("sistatement: SI INSERT admitted for deferred signing", ...)` call.

- [ ] **Step 8: Run the branch tests and the feature-off regression.** `bazel test //pkg/plugins/sistatement:sistatement_test --test_filter='TestPlugin_InlineValues|TestPlugin_NonSILaneStatementsPassThrough'` — expected PASS. `TestPlugin_NonSILaneStatementsPassThrough` is untouched and still proves the fallthrough is byte-identical with `InlineValues.Enabled == false`.

- [ ] **Step 9: Write the failing strict-hook test.** Append to `inline_values_test.go`:

```go
func TestPlugin_InlineValuesStrictHookHashesPlanPayload(t *testing.T) {
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil { t.Fatal(err) }
	p, _ := newInlinePlugin(t, &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}})
	qctx := inlineQctx(newSession(13, ""), "INSERT INTO shop.orders (id, region, amount) VALUES (1, 'eu', 1.5)")
	if err := p.OnQuery(context.Background(), qctx); err != nil { t.Fatalf("OnQuery: %v", err) }
	plan := qctx.SynthesizedInsert // the relay lane fills Packets before firing the strict hook
	plan.Packets, plan.PayloadBytes = [][]byte{{0x02, 0x00, 0xaa}, {0x02, 0x00, 0xbb}}, 6
	if err := p.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil { t.Fatalf("strict hook: %v", err) }
	var token string
	for _, s := range qctx.Query.Settings {
		if s.Key == auth.StatementTokenSettingKey { token = strings.Trim(s.Value, "'") }
	}
	if token == "" { t.Fatal("no statement token was appended") }
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := auth.JWSStatementPayloadV2{
		NetworkID: testNetworkID, StatementID: qctx.Query.ID, SQLHash: replay.DigestString(qctx.Query.Body),
		SettingsHash: sicore.EmptySettingsHash, SchemaHash: payloadexec.TableSchemaHash(testNetworkID, testSchema()),
		PayloadHash: replay.DigestBytes(plan.Payload()), PayloadLength: uint64(len(plan.Payload())),
		PayloadFormat: sicore.PayloadEncodingClickHouseNativeData, ClientRevision: testRevision,
		TargetTableID: "shop.orders", RowIDProfileID: payloadexec.RowIDProfileID, StatementKind: sicore.StatementKindCodeInsert,
	}
	if got, err := validator.ValidateStatementV2(token, want); err != nil || got != signer.Address() {
		t.Fatalf("token does not bind the plan payload: signer=%s err=%v", got, err)
	}
}
```

- [ ] **Step 10: Run it, see it fail, implement the strict hook.** `bazel test //pkg/plugins/sistatement:sistatement_test --test_filter='TestPlugin_InlineValuesStrictHookHashesPlanPayload'` — expected failure: `storage_integrity agent: SI INSERT ... carried no payload`. Then in `plugin.go`'s `OnQueryInputCompleteStrict` replace:

```go
	if st.payload.Len() == 0 {
		return fmt.Errorf("storage_integrity agent: SI INSERT %s carried no payload", st.statementID)
	}
	payload := st.payload.Bytes()
```

  with:

```go
	payload := st.payload.Bytes()
	if plan := qctx.SynthesizedInsert; plan != nil {
		// Spec D7: the signed bytes are the client Data packets the relay
		// encoded with the upstream codec, not a client-streamed buffer.
		payload = plan.Payload()
	}
	if len(payload) == 0 {
		return fmt.Errorf("storage_integrity agent: SI INSERT %s carried no payload", st.statementID)
	}
```

  Run again — expected PASS.

- [ ] **Step 11: Write the failing agent-signer refresh test.** `agent.Plugin.OnQueryInputCompleteStrict` returns early unless `qctx.DeferredInsert != nil`, so `SQL_x_auth_token` would never be re-minted over the rewritten body of a synthesized plan. Append to `pkg/plugins/agent/signer_test.go`, modelled on `TestPlugin_RefreshesDeferredAuthTokenAtInputCompleteWithoutDuplicate`:

```go
func TestPlugin_RefreshesSynthesizedAuthTokenOverTheRewrittenBody(t *testing.T) {
	clock := &clockTokenSigner{now: time.Unix(100, 0)}
	p := &Plugin{Signer: clock}
	qctx := newTestQueryContext(newTestSession(t, 52), "INSERT INTO t VALUES (1)")
	if err := p.OnQuery(context.Background(), qctx); err != nil { t.Fatal(err) }
	// sistatement rewrites the body and installs the plan in its own OnQuery,
	// which runs before this plugin's; the relay then fires the strict hook.
	qctx.Query.Body = "INSERT INTO t (x) FORMAT Native"
	qctx.SynthesizedInsert = &plugin.SynthesizedInsertPlan{Rows: 1}
	clock.mu.Lock()
	clock.now = time.Unix(220, 0) // beyond the default one-minute max token age
	clock.mu.Unlock()
	if err := p.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil { t.Fatal(err) }
	var tokens []string
	for _, setting := range qctx.Query.Settings {
		if setting.Key == auth.AuthTokenSettingKey { tokens = append(tokens, setting.Value) }
	}
	if len(tokens) != 1 || tokens[0] != "'token-at-220'" { t.Fatalf("auth settings = %v, want one refreshed token", tokens) }
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if len(clock.calls) != 2 || clock.calls[1] != "INSERT INTO t (x) FORMAT Native" {
		t.Fatalf("signed SQL calls = %v, want the rewritten body last", clock.calls)
	}
}
```

- [ ] **Step 12: Run it, see it fail, extend the gate.** `bazel test //pkg/plugins/agent:agent_test --test_filter='TestPlugin_RefreshesSynthesizedAuthTokenOverTheRewrittenBody'` — expected failure: `auth settings = ['token-at-100'], want one refreshed token` (the hook returned early). Then in `pkg/plugins/agent/signer.go` replace `	if qctx == nil || qctx.DeferredInsert == nil || qctx.Query == nil || p.Signer == nil {` with:

```go
	// The synthesized lane needs the same refresh as the deferred one: row
	// evaluation and packet encoding can outlive the auth token's max age, and the body the token must bind is the rewritten FORMAT Native text.
	if qctx == nil || (qctx.DeferredInsert == nil && qctx.SynthesizedInsert == nil) || qctx.Query == nil || p.Signer == nil {
```

  Run again — expected PASS, together with the untouched deferred test.

- [ ] **Step 13: Write the failing `UpstreamValuesEvaluator` tests.** Create `pkg/plugins/sistatement/values_evaluator_test.go`:

```go
package sistatement

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// fakeUpstream is an in-process server leg: it speaks the ClientHello /
// ServerHello / addendum handshake through a chproto.Codec on one end of a net.Pipe, publishes the Query it receives and replays a scripted response.
type fakeUpstream struct {
	seen       chan *chproto.Query
	reply      func(srv *chproto.Codec, rev int) error
	handshakes int
}

func (f *fakeUpstream) dial(_ context.Context) (net.Conn, error) {
	client, server := net.Pipe()
	go f.serve(server)
	return client, nil
}

func (f *fakeUpstream) serve(conn net.Conn) {
	defer conn.Close()
	srv := chproto.NewCodec(conn, chproto.DirFromClient)
	pkt, err := srv.ReadPacket(uint64(chproto.ClientHelloCode))
	if err != nil { return }
	hello, ok := pkt.Decoded.(*chproto.ClientHello)
	if !ok { return }
	rev := int(hello.ProtocolVersion)
	srv.SetRevision(rev)
	f.handshakes++
	if err := srv.WriteServerHello(&chproto.ServerHello{Name: "fake", Major: 26, Minor: 3, Revision: rev, Timezone: "UTC", DisplayName: "fake"}); err != nil { return }
	if chproto.SupportsAddendum(rev) {
		if _, err := srv.NegotiateAddendum(chproto.AddendumOpts{ProposedRecv: "notchunked", ProposedSend: "notchunked"}); err != nil { return }
	}
	for {
		qpkt, err := srv.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil { return }
		q, ok := qpkt.Decoded.(*chproto.Query)
		if !ok { return }
		if _, err := srv.ReadPacket(); err != nil { return } // the client's empty terminator
		f.seen <- q
		if err := f.reply(srv, rev); err != nil { return }
	}
}

func writeServerBlock(srv *chproto.Codec, rev int, input proto.Input) error {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: 1, Columns: len(input)}).EncodeBlock(&buf, rev, input); err != nil { return err }
	return srv.WriteRawPacket(buf.Buf)
}

func writeEndOfStream(srv *chproto.Codec) error {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeEndOfStream))
	return srv.WriteRawPacket(buf.Buf)
}

// evalRow builds one (id, ts) result row; idAsString forces a wire-type mismatch.
func evalRow(v uint64, idAsString bool) proto.Input {
	ts := &proto.ColDateTime{Location: time.UTC}
	ts.Append(time.Unix(1758000000, 0).UTC())
	if idAsString {
		s := &proto.ColStr{}
		s.Append("1")
		return proto.Input{{Name: "id", Data: s}, {Name: "ts", Data: ts}}
	}
	id := &proto.ColUInt64{}
	id.Append(v)
	return proto.Input{{Name: "id", Data: id}, {Name: "ts", Data: ts}}
}

func evalRequest() ValuesEvaluation {
	return ValuesEvaluation{
		Hello: &chproto.ClientHello{Name: "c", Major: 1, Minor: 0, ProtocolVersion: testRevision, Database: "shop", User: "writer"},
		Schema: payloadexec.TableSchema{TableID: "shop.orders", Columns: []lthash.Column{
			{Name: "id", Type: "UInt64"}, {Name: "ts", Type: "DateTime('UTC')"},
		}},
		Account: "0xabc", Columns: []string{"id", "ts"}, Rows: "(1, toDateTime(1758000000))",
		Timeout: 3 * time.Second, MaxRows: 64, MaxBytes: 1 << 20,
	}
}

func newTestEvaluator(t *testing.T, reply func(*chproto.Codec, int) error) (*UpstreamValuesEvaluator, *fakeUpstream, *auth.RelaySigner) {
	t.Helper()
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil { t.Fatal(err) }
	up := &fakeUpstream{seen: make(chan *chproto.Query, 4), reply: reply}
	ev := NewUpstreamValuesEvaluator(up.dial, signer)
	t.Cleanup(func() { _ = ev.Close() })
	return ev, up, signer
}

// TestUpstreamValuesEvaluator_HelperQuery pins the helper SQL text
// (including the doubled quotes of DateTime('UTC') inside the structure literal), the four settings, compression off and the agent auth token, and also proves multiple result blocks are concatenated in order and the pool reuses one handshake.
func TestUpstreamValuesEvaluator_HelperQuery(t *testing.T) {
	ev, up, signer := newTestEvaluator(t, func(srv *chproto.Codec, rev int) error {
		for _, v := range []uint64{1, 2} {
			if err := writeServerBlock(srv, rev, evalRow(v, false)); err != nil { return err }
		}
		return writeEndOfStream(srv)
	})
	for i := 0; i < 2; i++ {
		blocks, err := ev.Evaluate(context.Background(), evalRequest())
		if err != nil { t.Fatalf("Evaluate #%d: %v", i, err) }
		if len(blocks) != 2 || len(blocks[0]) != 2 || blocks[0][0].Data.Rows() != 1 { t.Fatalf("#%d blocks = %+v", i, blocks) }
		<-up.seen
	}
	if up.handshakes != 1 { t.Fatalf("pool performed %d handshakes for two evaluations, want 1", up.handshakes) }
	q := <-up.seen // both evaluations pushed identical queries; inspect one
	want := "SELECT `id`, `ts` FROM VALUES('`id` UInt64, `ts` DateTime(''UTC'')', (1, toDateTime(1758000000)))"
	if q.Body != want { t.Fatalf("helper SQL =\n  %q\nwant\n  %q", q.Body, want) }
	if q.Compression != proto.CompressionDisabled { t.Fatalf("helper query must disable compression, got %v", q.Compression) }
	got := map[string]chproto.Setting{}
	for _, s := range q.Settings { got[s.Key] = s }
	for key, value := range map[string]string{
		"max_execution_time": "3", "max_result_rows": "64", "max_result_bytes": "1048576", "max_block_size": "64",
	} {
		if got[key].Value != value { t.Fatalf("setting %s = %q, want %q", key, got[key].Value, value) }
	}
	tok := got[auth.AuthTokenSettingKey]
	if !tok.Custom || !strings.HasPrefix(tok.Value, "'") { t.Fatalf("auth token setting = %+v", tok) }
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	if _, err := validator.ValidateQuery(auth.QueryMeta{Token: strings.Trim(tok.Value, "'"), SQL: q.Body}); err != nil {
		t.Fatalf("helper query is not signed by the agent key: %v", err)
	}
}

func TestUpstreamValuesEvaluator_Refusals(t *testing.T) {
	cases := []struct {
		name, wantErr string
		timeout       time.Duration
		reply         func(*chproto.Codec, int) error
	}{
		{"type mismatch", "wire type", 3 * time.Second, func(srv *chproto.Codec, rev int) error {
			if err := writeServerBlock(srv, rev, evalRow(1, true)); err != nil { return err }
			return writeEndOfStream(srv)
		}},
		{"exception relayed", "code=36", 3 * time.Second, func(srv *chproto.Codec, _ int) error {
			return srv.WriteException(&chproto.Exception{Code: 36, Name: "BAD_ARGUMENTS", Message: "is not a constant expression"})
		}},
		{"timeout", "", time.Second, func(srv *chproto.Codec, _ int) error {
			time.Sleep(2 * time.Second)
			return writeEndOfStream(srv)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, up, _ := newTestEvaluator(t, tc.reply)
			req := evalRequest()
			req.Timeout = tc.timeout
			start := time.Now()
			_, err := ev.Evaluate(context.Background(), req)
			if err == nil { t.Fatal("the evaluation must fail") }
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) { t.Fatalf("err = %v, want %q", err, tc.wantErr) }
			if tc.name == "exception relayed" && !strings.Contains(err.Error(), "is not a constant expression") {
				t.Fatalf("err = %v, want the upstream message", err)
			}
			if tc.name == "timeout" && time.Since(start) > 1500*time.Millisecond {
				t.Fatalf("timeout was not enforced (%s)", time.Since(start))
			}
			<-up.seen
		})
	}
}
```

- [ ] **Step 14: Run them and see them fail.** `bazel test //pkg/plugins/sistatement:sistatement_test --test_filter='TestUpstreamValuesEvaluator'` — expected failure: `undefined: NewUpstreamValuesEvaluator`.

- [ ] **Step 15: Implement the evaluator.** Create `pkg/plugins/sistatement/values_evaluator.go`:

```go
package sistatement

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sqlident"
)

// maxIdleEvaluatorConns bounds the per-(user, database) idle pool.
const maxIdleEvaluatorConns = 4

// UpstreamValuesEvaluator evaluates inline VALUES rows through the same
// server-mode proxy the session talks to (spec D4): a dedicated connection, the ordinary ClientHello handshake with the session's stored upstream hello, one signed helper SELECT. Any exception, timeout or transport error is terminal -- the connection is closed, never retried and never pooled again.
type UpstreamValuesEvaluator struct {
	dial   func(ctx context.Context) (net.Conn, error)
	signer auth.Signer
	mu     sync.Mutex
	idle   map[string][]*evaluatorConn
}

type evaluatorConn struct {
	conn  net.Conn
	codec *chproto.Codec
}

func NewUpstreamValuesEvaluator(dial func(ctx context.Context) (net.Conn, error), signer auth.Signer) *UpstreamValuesEvaluator {
	return &UpstreamValuesEvaluator{dial: dial, signer: signer, idle: map[string][]*evaluatorConn{}}
}

func (e *UpstreamValuesEvaluator) Evaluate(ctx context.Context, req ValuesEvaluation) ([][]proto.InputColumn, error) {
	if e == nil || e.dial == nil || e.signer == nil { return nil, fmt.Errorf("values evaluator is not configured") }
	if req.Hello == nil { return nil, fmt.Errorf("evaluation requires the session's upstream hello") }
	sql, err := buildValuesQuery(req)
	if err != nil { return nil, err }
	token, err := e.signer.SignToken(sql)
	if err != nil { return nil, fmt.Errorf("sign evaluation query: %w", err) }
	ec, err := e.acquire(ctx, req)
	if err != nil { return nil, err }
	blocks, err := runEvaluation(ec, req, sql, token)
	if err != nil {
		ec.close()
		return nil, err
	}
	e.release(poolKey(req), ec)
	return blocks, nil
}

// Close drops every pooled connection; buildAgent registers it as teardown.
func (e *UpstreamValuesEvaluator) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for key, pool := range e.idle {
		for _, ec := range pool { ec.close() }
		delete(e.idle, key)
	}
	return nil
}

// buildValuesQuery renders the helper SELECT (spec D4). The structure string
// is one single-quoted literal, so every single quote a declared type carries -- DateTime('UTC') -- is doubled inside it.
func buildValuesQuery(req ValuesEvaluation) (string, error) {
	if len(req.Columns) == 0 { return "", fmt.Errorf("evaluation requires at least one column") }
	declared := declaredTypes(req)
	selects := make([]string, len(req.Columns))
	structure := make([]string, len(req.Columns))
	for i, name := range req.Columns {
		typ, ok := declared[name]
		if !ok { return "", fmt.Errorf("column %q is not declared in the schema for %s", name, req.Schema.TableID) }
		if _, err := payloadexec.ResolveColumnProfile(typ); err != nil { return "", fmt.Errorf("column %q: %w", name, err) }
		quoted := sqlident.Quote(name)
		selects[i] = quoted
		structure[i] = quoted + " " + typ
	}
	return "SELECT " + strings.Join(selects, ", ") +
		" FROM VALUES(" + singleQuoted(strings.Join(structure, ", ")) + ", " + req.Rows + ")", nil
}

func singleQuoted(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

func poolKey(req ValuesEvaluation) string { return req.Hello.User + "\x00" + req.Hello.Database }

func declaredTypes(req ValuesEvaluation) map[string]string {
	out := make(map[string]string, len(req.Schema.Columns))
	for _, c := range req.Schema.Columns { out[c.Name] = c.Type }
	return out
}

func runEvaluation(ec *evaluatorConn, req ValuesEvaluation, sql, token string) ([][]proto.InputColumn, error) {
	_ = ec.conn.SetDeadline(time.Now().Add(req.Timeout))
	defer func() { _ = ec.conn.SetDeadline(time.Time{}) }()
	up := ec.codec
	q := &chproto.Query{
		Info: chproto.ClientInfo{
			Query: proto.ClientQueryInitial, InitialUser: req.Hello.User, InitialAddress: "127.0.0.1:0",
			Interface: proto.InterfaceTCP, OSUser: "housegate", ClientHostname: "housegate",
			ClientName: "housegate-inline-values", Major: 1, Minor: 0, ProtocolVersion: up.Revision(),
		},
		Stage: proto.StageComplete, Compression: proto.CompressionDisabled, Body: sql,
		Settings: []chproto.Setting{
			{Key: "max_execution_time", Value: strconv.FormatFloat(req.Timeout.Seconds(), 'f', -1, 64)},
			{Key: "max_result_rows", Value: strconv.FormatUint(req.MaxRows, 10)},
			{Key: "max_result_bytes", Value: strconv.FormatUint(req.MaxBytes, 10)},
			{Key: "max_block_size", Value: strconv.FormatUint(req.MaxRows, 10)},
			{Key: auth.AuthTokenSettingKey, Value: "'" + token + "'", Custom: true},
		},
	}
	if err := up.WriteQuery(q); err != nil { return nil, fmt.Errorf("write evaluation query: %w", err) }
	if err := up.WriteEmptyDataBlock(); err != nil { return nil, fmt.Errorf("write evaluation terminator: %w", err) }
	var blocks [][]proto.InputColumn
	for {
		pkt, err := up.ReadPacket(uint64(chproto.ServerExceptionCode))
		if err != nil { return nil, fmt.Errorf("read evaluation result: %w", err) }
		switch pkt.Type {
		case uint64(chproto.ServerEndOfStreamCode):
			return blocks, nil
		case uint64(chproto.ServerExceptionCode):
			exc, _ := pkt.Decoded.(*chproto.Exception)
			if exc == nil { return nil, fmt.Errorf("evaluation returned a malformed exception") }
			return nil, fmt.Errorf("ClickHouse refused the rows: code=%d %s: %s", exc.Code, exc.Name, exc.Message)
		case uint64(chproto.ServerDataCode):
			cols, derr := decodeServerDataColumns(pkt.Raw, up.Revision(), req)
			if derr != nil { return nil, derr }
			if cols != nil { blocks = append(blocks, cols) }
		}
	}
}

// decodeServerDataColumns decodes one server Data packet the way
// nativepayload decodes a client one: proto.Results.Auto() infers every column from its declared wire type, and that type must equal the profile's NativeWireType. Zero-row blocks (the leading header block, Totals, Extremes) are dropped.
func decodeServerDataColumns(raw []byte, revision int, req ValuesEvaluation) ([]proto.InputColumn, error) {
	pr := proto.NewReader(bytes.NewReader(raw))
	code, err := pr.UVarInt()
	if err != nil { return nil, fmt.Errorf("evaluation block code: %w", err) }
	if code != uint64(proto.ServerCodeData) { return nil, fmt.Errorf("evaluation packet type %d is not ServerData", code) }
	if _, err := pr.Str(); err != nil { return nil, fmt.Errorf("evaluation block name: %w", err) }
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(pr, revision, results.Auto()); err != nil { return nil, fmt.Errorf("decode evaluation block: %w", err) }
	if block.Rows == 0 { return nil, nil }
	if len(results) != len(req.Columns) {
		return nil, fmt.Errorf("evaluation block has %d columns, the INSERT lists %d", len(results), len(req.Columns))
	}
	declared := declaredTypes(req)
	out := make([]proto.InputColumn, len(results))
	for i, rc := range results {
		want := req.Columns[i]
		profile, perr := payloadexec.ResolveColumnProfile(declared[want])
		if perr != nil { return nil, fmt.Errorf("column %q: %w", want, perr) }
		if got := string(rc.Data.Type()); got != profile.NativeWireType {
			return nil, fmt.Errorf("evaluated column %q has wire type %q, the declared type %q requires %q",
				want, got, declared[want], profile.NativeWireType)
		}
		input, ok := rc.Data.(proto.ColInput)
		if !ok { return nil, fmt.Errorf("evaluated column %q (%T) cannot be re-encoded", want, rc.Data) }
		out[i] = proto.InputColumn{Name: want, Data: input}
	}
	return out, nil
}

func (e *UpstreamValuesEvaluator) acquire(ctx context.Context, req ValuesEvaluation) (*evaluatorConn, error) {
	key := poolKey(req)
	e.mu.Lock()
	if pool := e.idle[key]; len(pool) > 0 {
		ec := pool[len(pool)-1]
		e.idle[key] = pool[:len(pool)-1]
		e.mu.Unlock()
		return ec, nil
	}
	e.mu.Unlock()
	conn, err := e.dial(ctx)
	if err != nil { return nil, fmt.Errorf("dial evaluation upstream: %w", err) }
	ec := &evaluatorConn{conn: conn, codec: chproto.NewCodec(conn, chproto.DirToUpstream)}
	if err := ec.handshake(req.Hello); err != nil {
		ec.close()
		return nil, err
	}
	return ec, nil
}

func (e *UpstreamValuesEvaluator) release(key string, ec *evaluatorConn) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.idle[key]) >= maxIdleEvaluatorConns {
		ec.close()
		return
	}
	e.idle[key] = append(e.idle[key], ec)
}

func (ec *evaluatorConn) close() { _ = ec.conn.Close() }

// handshake mirrors Relay.handshakeFreshUpstream: the stored hello capped by
// ClientHelloForUpstream, the ServerHello revision floor, compression pinned off, and the one-way client addendum.
func (ec *evaluatorConn) handshake(stored *chproto.ClientHello) error {
	hello := chproto.ClientHelloForUpstream(stored)
	if err := ec.codec.WriteClientHello(hello); err != nil { return fmt.Errorf("write evaluation hello: %w", err) }
	ec.codec.SetServerHelloRevisionHint(int(hello.ProtocolVersion))
	pkt, err := ec.codec.ReadPacket(uint64(chproto.ServerHelloCode), uint64(chproto.ServerExceptionCode))
	if err != nil { return fmt.Errorf("read evaluation server hello: %w", err) }
	if exc, ok := pkt.Decoded.(*chproto.Exception); ok {
		return fmt.Errorf("evaluation upstream rejected hello: code=%d %s: %s", exc.Code, exc.Name, exc.Message)
	}
	srv, ok := pkt.Decoded.(*chproto.ServerHello)
	if !ok { return fmt.Errorf("unexpected evaluation hello packet type=%d", pkt.Type) }
	rev := int(hello.ProtocolVersion)
	if srv.Revision < rev { rev = srv.Revision }
	ec.codec.SetRevision(rev)
	ec.codec.SetCompression(proto.CompressionDisabled)
	if chproto.SupportsAddendum(rev) {
		res := ec.codec.ResolveUpstreamAddendum(chproto.AddendumResult{},
			chproto.AddendumOpts{ProposedRecv: "notchunked", ProposedSend: "notchunked"})
		if err := ec.codec.SendAddendum(res); err != nil { return fmt.Errorf("send evaluation addendum: %w", err) }
	}
	return nil
}

var _ ValuesEvaluator = (*UpstreamValuesEvaluator)(nil)
```

- [ ] **Step 16: Run the evaluator tests.** `bazel test //pkg/plugins/sistatement:sistatement_test --test_filter='TestUpstreamValuesEvaluator'` — expected PASS for both tests and all three refusal subtests.

- [ ] **Step 17: Format, re-sync Bazel, run the touched packages.** `gofmt -w pkg/plugins/sistatement/ pkg/plugins/agent/`, `bazel run //:gazelle` (registers the four new files and adds `//pkg/sqlident` and `//pkg/lthash`), then `bazel test //pkg/plugins/sistatement:sistatement_test //pkg/plugins/agent:agent_test //pkg/plugins/storageintegrity:storageintegrity_test` — expected PASS.

- [ ] **Step 18: Commit.**

```bash
git add pkg/plugins/sistatement/inline_values.go pkg/plugins/sistatement/inline_values_test.go \
  pkg/plugins/sistatement/values_evaluator.go pkg/plugins/sistatement/values_evaluator_test.go \
  pkg/plugins/sistatement/plugin.go pkg/plugins/sistatement/BUILD.bazel \
  pkg/plugins/agent/signer.go pkg/plugins/agent/signer_test.go
git commit -m "feat(sistatement): evaluate and sign inline INSERT ... VALUES

Adds the ValuesEvaluator seam, an UpstreamValuesEvaluator over a pooled
dedicated upstream connection, and the OnQuery branch that parses, gates,
evaluates and rewrites an inline VALUES INSERT into FORMAT Native before any
client_seq is consumed.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

---

### Task 5: `runSynthesizedInsert` relay lane (D8, D9; consumes D7)

**Files:**
- Create: `pkg/proxy/relay_synthesized.go`, `pkg/proxy/relay_synthesized_test.go`
- Modify: `pkg/proxy/relay.go` — `deferredSampleResult` (relay.go:128-141); the `AgentPrepare` guard (relay.go:1041), the `QueryOnly` guard (relay.go:1088), a new dispatch block before the `AbortWithSuccess` comment (relay.go:1114); `runDeferredInsert`'s second half (relay.go:1517-1794); the `ServerDataCode` arm of the deferred-sample branch (relay.go:2032-2036)
- Modify: `pkg/proxy/BUILD.bazel` (gazelle)

**Interfaces:**
- Consumes: `plugin.SynthesizedInsertPlan{Blocks [][]proto.InputColumn; SampleColumns []chproto.SampleColumn; Rows uint64; Packets [][]byte; PayloadBytes uint64}` and `(*SynthesizedInsertPlan).Payload() []byte` (Task 3); `(*chproto.Codec).EncodeClientDataPacket` (Task 2); `chproto.InspectClientDataPacket`, `.ReadPacketWithDataLimit`, `.WriteRawPacket`, `.WriteEmptyDataBlock`, `.Revision()`, `.SetCompression`; `chproto.ErrPacketTooLarge`, `chproto.SampleColumn`; `plugin.Hooks.ClientDataReadLimit / OnQueryAbort / OnQueryComplete`; relay internals `beginActiveQuery`, `armDeferredInput`, `armDeferredSample`, `finishDeferredInput`, `takeActiveQuery`, `closeCodec`, `upstreamAddr`, `clientPacketName`, `errQueryRejectedResume`, `writeExceptionToClient`, `r.obs`.
- Produces: `func (r *Relay) runSynthesizedInsert(ctx context.Context, qctx *plugin.QueryContext, compression proto.Compression) error` (the contract signature adapted to `runDeferredInsert`'s shape: `q := qctx.Query`, `plan := qctx.SynthesizedInsert`); `type signedInsertForward struct`; `func (r *Relay) forwardSignedInsert(ctx context.Context, qctx *plugin.QueryContext, fw signedInsertForward) error`; `parseSampleColumns`; `matchSampleColumns`.

- [ ] **Step 1: Extract the shared post-input half of `runDeferredInsert`**

A mechanical move with no behavior change; the 40+ existing deferred-lane tests are the regression net. Create `pkg/proxy/relay_synthesized.go`:

```go
package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
)

// signedInsertForward is the post-input half shared by the two signed INSERT
// lanes. runDeferredInsert collects the payload from the client and replays it;
// runSynthesizedInsert encodes it from an evaluated plan. Everything from the
// strict input-complete hook onwards — upstream bind, Query, exactly one
// external-tables marker, the sample gate, the payload, exactly one terminator,
// OnQueryInputComplete and the terminal arbitration — is identical.
type signedInsertForward struct {
	lane          string            // "deferred INSERT" | "synthesized INSERT"
	compression   proto.Compression
	markerRaw     []byte            // forwarded exactly once before the sample gate
	terminatorRaw []byte            // nil ⇒ up.WriteEmptyDataBlock() writes the only terminator
	payload       [][]byte
	payloadBytes  uint64
	sampleColumns []chproto.SampleColumn // non-nil ⇒ validate the consumed upstream sample
	rejectClose   func(error) error
	rejectResume  func(error) error
}

func (fw signedInsertForward) errf(format string, args ...any) error {
	return fmt.Errorf("%s "+format, append([]any{fw.lane}, args...)...)
}

func (r *Relay) forwardSignedInsert(ctx context.Context, qctx *plugin.QueryContext, fw signedInsertForward) error {
	_, logger := log.FromContext(ctx)
	client := r.sess.Client()
	q := qctx.Query
	// ... relay.go:1517-1794 moved verbatim, with the substitutions below ...
}
```

Cut relay.go:1517-1794 — from `if err := r.hooks.OnQueryInputCompleteStrict(ctx, qctx); err != nil {` through the closing brace of `runDeferredInsert` — into that body and apply exactly these substitutions:

| Moved text | Replacement |
|---|---|
| the parameter `compression` | `fw.compression` |
| `rejectClose(` / `rejectResume(` | `fw.rejectClose(` / `fw.rejectResume(` |
| `markerRaw := initialEmptyRaw` + `if markerRaw == nil { markerRaw = terminatorRaw }` (relay.go:1585-1588) | `markerRaw := fw.markerRaw` |
| `for _, raw := range buffered {` | `for _, raw := range fw.payload {` |
| `"packets", len(buffered), "payload_bytes", bufferedBytes` | `"packets", len(fw.payload), "payload_bytes", fw.payloadBytes` |
| every `fmt.Errorf("deferred INSERT <rest>", …)` | `fw.errf("<rest>", …)` — drop the literal prefix, keep the rest and the args |
| every other literal `deferred INSERT` in a message (`logger.Debugw`, `errors.New`, `"client cancelled deferred INSERT…"`, `"… before deferred INSERT sample"`, `"… before deferred INSERT terminal"`) | a `%s` verb with `fw.lane` as an argument, e.g. `fmt.Errorf("client cancelled %s while awaiting upstream sample", fw.lane)` |
| `if err := up.WriteRawPacket(terminatorRaw); err != nil { … }` (relay.go:1673-1678) | the terminator branch in Step 2 |

Then replace relay.go:1517-1794 inside `runDeferredInsert` with:

```go
	markerRaw := initialEmptyRaw
	if markerRaw == nil {
		markerRaw = terminatorRaw
	}
	return r.forwardSignedInsert(ctx, qctx, signedInsertForward{
		lane:          "deferred INSERT",
		compression:   compression,
		markerRaw:     markerRaw,
		terminatorRaw: terminatorRaw,
		payload:       buffered,
		payloadBytes:  bufferedBytes,
		rejectClose:   rejectClose,
		rejectResume:  rejectResume,
	})
}
```

- [ ] **Step 2: Add the terminator branch, the sample validation and the sample capture**

In `forwardSignedInsert`, the moved terminator write becomes:

```go
	if fw.terminatorRaw != nil {
		if err := up.WriteRawPacket(fw.terminatorRaw); err != nil {
			if inputGate.terminalObserved() {
				return abortAfterTerminal("writing terminator")
			}
			return forwardFail("forward terminator", err)
		}
	} else if err := up.WriteEmptyDataBlock(); err != nil {
		// The synthesized lane's client never sent a terminator, so this is the
		// one and only empty Data block this query puts on the upstream leg.
		if inputGate.terminalObserved() {
			return abortAfterTerminal("writing terminator")
		}
		return forwardFail("write terminator", err)
	}
```

Immediately after the moved `sampleResult.exception` block and before `for _, raw := range fw.payload {`, insert:

```go
	if fw.sampleColumns != nil {
		got, err := parseSampleColumns(sampleResult.sampleRaw, up.Revision())
		if err != nil {
			return forwardFail("decode upstream sample block", err)
		}
		if err := matchSampleColumns(fw.sampleColumns, got); err != nil {
			// Schema drift between network state and the physical table. The
			// upstream is waiting for data, so cancelling it is pointless:
			// name the first differing column to the client and close.
			r.writeExceptionToClient(ctx, fw.errf("%q upstream sample mismatch: %w", q.ID, err))
			return abortFromWriter("upstream sample mismatch", err)
		}
	}
```

Append the helpers to `relay_synthesized.go`:

```go
// parseSampleColumns reads the name/type pairs of a 0-row server Data packet.
// It mirrors chproto's BlockInfo compat decoder: ClickHouse 26.x added field 3
// (out_of_order_buckets) and BlockInfo field types are not self-describing, so
// an unknown field id is an error rather than a skip.
func parseSampleColumns(raw []byte, revision int) ([]chproto.SampleColumn, error) {
	if len(raw) == 0 {
		return nil, errors.New("upstream sample block was not captured")
	}
	r := proto.NewReader(bytes.NewReader(raw))
	code, err := r.UVarInt()
	if err != nil || code != uint64(proto.ServerCodeData) {
		return nil, fmt.Errorf("sample packet code %d is not ServerData: %w", code, err)
	}
	if _, err := r.Str(); err != nil {
		return nil, fmt.Errorf("sample block name: %w", err)
	}
	if proto.FeatureBlockInfo.In(revision) {
		if err := skipSampleBlockInfo(r); err != nil {
			return nil, err
		}
	}
	columns, err := r.UVarInt()
	if err != nil {
		return nil, fmt.Errorf("sample columns: %w", err)
	}
	rows, err := r.UVarInt()
	if err != nil || rows != 0 {
		return nil, fmt.Errorf("sample block carries %d rows (err %v), want 0", rows, err)
	}
	out := make([]chproto.SampleColumn, 0, columns)
	for i := uint64(0); i < columns; i++ {
		name, nameErr := r.Str()
		typ, typeErr := r.Str()
		if nameErr != nil || typeErr != nil {
			return nil, fmt.Errorf("sample column %d: %w", i, errors.Join(nameErr, typeErr))
		}
		if proto.FeatureCustomSerialization.In(revision) {
			if _, err := r.Bool(); err != nil {
				return nil, fmt.Errorf("sample column %d custom serialization flag: %w", i, err)
			}
		}
		out = append(out, chproto.SampleColumn{Name: name, Type: typ})
	}
	return out, nil
}

func skipSampleBlockInfo(r *proto.Reader) error {
	for {
		field, err := r.UVarInt()
		if err != nil {
			return fmt.Errorf("BlockInfo field id: %w", err)
		}
		switch field {
		case 0:
			return nil
		case 1:
			if _, err := r.Bool(); err != nil {
				return fmt.Errorf("BlockInfo overflows: %w", err)
			}
		case 2:
			if _, err := r.Int32(); err != nil {
				return fmt.Errorf("BlockInfo bucket_num: %w", err)
			}
		case 3:
			count, err := r.UVarInt()
			if err != nil {
				return fmt.Errorf("BlockInfo out_of_order_buckets count: %w", err)
			}
			for i := uint64(0); i < count; i++ {
				if _, err := r.Int32(); err != nil {
					return fmt.Errorf("BlockInfo out_of_order_buckets[%d]: %w", i, err)
				}
			}
		default:
			return fmt.Errorf("BlockInfo unknown field %d (cannot skip safely)", field)
		}
	}
}

// matchSampleColumns reports the first column on which the upstream sample and
// the plan disagree; the message is what the client sees.
func matchSampleColumns(want, got []chproto.SampleColumn) error {
	for i := range want {
		if i >= len(got) {
			return fmt.Errorf("column %d %q is missing upstream (upstream has %d columns, plan has %d)",
				i, want[i].Name, len(got), len(want))
		}
		if got[i].Name != want[i].Name || got[i].Type != want[i].Type {
			return fmt.Errorf("column %d: upstream has %q %q, plan expects %q %q",
				i, got[i].Name, got[i].Type, want[i].Name, want[i].Type)
		}
	}
	if len(got) != len(want) {
		return fmt.Errorf("upstream has %d columns, plan has %d (first extra is %q)",
			len(got), len(want), got[len(want)].Name)
	}
	return nil
}
```

In relay.go:128-141 add one field to `deferredSampleResult`:

```go
	sampleRaw []byte // the consumed sample Data packet, for lanes that validate it
```

and in `upstreamToClient`'s deferred-sample branch replace

```go
			case uint64(chproto.ServerDataCode):
				// The upstream's INSERT sample block for a deferred INSERT: the client already
				// received housegate's locally synthesized sample and is past its data phase,
				// so this one is consumed here.
				r.settleDeferredSample(deferredSampleResult{})
```

with

```go
			case uint64(chproto.ServerDataCode):
				// The upstream's INSERT sample block for a signed INSERT lane: the
				// client either already received housegate's locally synthesized
				// sample (deferred) or never had a data phase at all (synthesized),
				// so this one is consumed here. The raw bytes ride along so a lane
				// can compare the columns against its plan.
				r.settleDeferredSample(deferredSampleResult{sampleRaw: append([]byte(nil), pkt.Raw...)})
```

- [ ] **Step 3: Prove the extraction is behavior-neutral**

```bash
bazel run //:gazelle
bazel test //pkg/proxy:proxy_test --test_filter='TestRelay_DeferredInsert'
```

Expected: PASS, the same set as before the move. Do not continue on any failure — this is the gate for Steps 1-2.

- [ ] **Step 4: Write the failing fakes and the happy-path test**

Create `pkg/proxy/relay_synthesized_test.go`:

```go
package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
)

const synthesizedBody = "INSERT INTO db.t (v) FORMAT Native"

// synthesizedInsertHooks installs a SynthesizedInsert plan on every Query and
// counts the lifecycle hooks Relay fires.
type synthesizedInsertHooks struct {
	plugin.NoopHooks
	mu              sync.Mutex
	events          map[string]int
	limit           uint64
	enforce         bool
	alsoDefer       bool
	skipPlanFor     string // a Query body that must stay on the ordinary path
	payloadAtStrict []byte
	strictRan       atomic.Bool
	inputDone       chan struct{}
}

func newSynthesizedHooks() *synthesizedInsertHooks {
	return &synthesizedInsertHooks{events: map[string]int{}, inputDone: make(chan struct{}, 1)}
}

func (h *synthesizedInsertHooks) bump(name string) { h.mu.Lock(); h.events[name]++; h.mu.Unlock() }

func (h *synthesizedInsertHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if h.skipPlanFor != "" && qctx.Query.Body == h.skipPlanFor {
		return nil
	}
	cols := []chproto.SampleColumn{{Name: "v", Type: "UInt64"}}
	qctx.SynthesizedInsert = &plugin.SynthesizedInsertPlan{
		Blocks:        [][]proto.InputColumn{synthesizedBlock()},
		SampleColumns: cols,
		Rows:          3,
	}
	if h.alsoDefer {
		qctx.DeferredInsert = &plugin.DeferredInsertPlan{SampleColumns: cols, MaxPayloadBytes: 1 << 20}
	}
	return nil
}

func (h *synthesizedInsertHooks) ClientDataReadLimit(*plugin.QueryContext) (uint64, bool) {
	return h.limit, h.enforce
}

func (h *synthesizedInsertHooks) OnQueryInputCompleteStrict(_ context.Context, qctx *plugin.QueryContext) error {
	h.mu.Lock()
	h.events["strict"]++
	if qctx.SynthesizedInsert != nil {
		h.payloadAtStrict = qctx.SynthesizedInsert.Payload()
	}
	h.mu.Unlock()
	h.strictRan.Store(true)
	return nil
}

func (h *synthesizedInsertHooks) OnQueryInputComplete(context.Context, *plugin.QueryContext) {
	h.bump("input")
	select {
	case h.inputDone <- struct{}{}:
	default:
	}
}

func (h *synthesizedInsertHooks) OnQueryComplete(context.Context, chsession.Session) { h.bump("complete") }

func (h *synthesizedInsertHooks) OnQueryAbort(context.Context, *plugin.QueryContext) { h.bump("abort") }

func (h *synthesizedInsertHooks) OnQuerySuccess(context.Context, chsession.Session, string) {
	h.bump("success")
}

func (h *synthesizedInsertHooks) counts() (strict, inputs, completes, aborts, successes int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.events["strict"], h.events["input"], h.events["complete"], h.events["abort"], h.events["success"]
}

func synthesizedBlock() []proto.InputColumn {
	values := proto.ColUInt64{1, 2, 3}
	return []proto.InputColumn{{Name: "v", Data: &values}}
}

func synthesizedPacket(t *testing.T, rev int) []byte {
	t.Helper()
	raw, err := nativepayload.EncodeClientDataPacket(rev, synthesizedBlock())
	if err != nil {
		t.Fatalf("EncodeClientDataPacket: %v", err)
	}
	return raw
}

func encodeServerSampleNamed(t *testing.T, rev int, name string) []byte {
	t.Helper()
	values := proto.ColUInt64{}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: 0, Columns: 1}).EncodeBlock(&buf, rev, proto.Input{{Name: name, Data: &values}}); err != nil {
		t.Fatalf("encode sample: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

func encodeServerExceptionPacket(rev int, code int32, message string) []byte {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeException))
	(&chproto.Exception{Code: proto.Error(code), Name: "DB::Exception", Message: message}).EncodeAware(&buf, rev)
	return append([]byte(nil), buf.Buf...)
}

// encodeTableColumnsPacket is what ClickHouse sends ahead of the sample when
// input_format_defaults_for_omitted_fields is on: table name + columns
// description, both plain strings below revision 54481.
func encodeTableColumnsPacket(name, description string) []byte {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeTableColumns))
	buf.PutString(name)
	buf.PutString(description)
	return append([]byte(nil), buf.Buf...)
}

func newSynthesizedHarness(t *testing.T, hooks plugin.Hooks, rev int, chunked, tcp bool) *deferredHarness {
	t.Helper()
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	if tcp {
		clientProxy, proxyClient = tcpConnPair(t)
		upstreamProxy, proxyUpstream = tcpConnPair(t)
	}
	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if chunked {
		up.EnableChunked(true, true)
	}
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	r := &Relay{sess: sess, hooks: hooks}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	h := &deferredHarness{clientProxy: clientProxy, proxyClient: proxyClient, upstreamProxy: upstreamProxy, proxyUpstream: proxyUpstream, relay: r, loopErr: make(chan error, 2), cancel: cancel}
	go func() { h.loopErr <- r.clientToUpstream(ctx) }()
	go func() { h.loopErr <- r.upstreamToClient(ctx) }()
	t.Cleanup(func() { cancel(); clientProxy.Close(); upstreamProxy.Close() })
	return h
}

// synthUpstream plays a ClickHouse: it reads Query + exactly one empty marker,
// asserts the strict hook already ran, then runs stage.
func synthUpstream(t *testing.T, conn net.Conn, rev int, chunked bool, strictRan *atomic.Bool, stage func(up *chproto.Codec) error, done chan<- error) {
	up := chproto.NewCodec(conn, chproto.DirFromClient)
	up.SetRevision(rev)
	up.SetCompression(proto.CompressionDisabled)
	if chunked {
		up.EnableChunked(true, true)
	}
	done <- func() error {
		pkt, err := up.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil {
			return fmt.Errorf("read query: %w", err)
		}
		if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.Body != synthesizedBody {
			return fmt.Errorf("upstream query = %#v, want body %q", pkt.Decoded, synthesizedBody)
		}
		if !strictRan.Load() {
			return errors.New("Query reached upstream before OnQueryInputCompleteStrict")
		}
		marker, err := up.ReadPacket()
		if err != nil {
			return fmt.Errorf("read marker: %w", err)
		}
		if empty, err := chproto.ClientDataPacketIsEmpty(marker.Raw, proto.CompressionDisabled); err != nil || !empty {
			return fmt.Errorf("first packet after Query is not the empty marker (empty=%v err=%v)", empty, err)
		}
		return stage(up)
	}()
}

// readPayloadAndTerminator asserts the lane wrote the plan's packets and then
// exactly one empty terminator.
func readPayloadAndTerminator(up *chproto.Codec, want []byte) error {
	data, err := up.ReadPacket()
	if err != nil {
		return fmt.Errorf("read payload: %w", err)
	}
	if !bytes.Equal(data.Raw, want) {
		return fmt.Errorf("upstream payload = %x, want %x", data.Raw, want)
	}
	term, err := up.ReadPacket()
	if err != nil {
		return fmt.Errorf("read terminator: %w", err)
	}
	if empty, err := chproto.ClientDataPacketIsEmpty(term.Raw, proto.CompressionDisabled); err != nil || !empty {
		return fmt.Errorf("terminator is not an empty block (empty=%v err=%v)", empty, err)
	}
	return nil
}

func TestRelay_SynthesizedInsert_ForwardsPlanPayloadAfterSampleGate(t *testing.T) {
	for _, tc := range []struct {
		name                string
		rev                 int
		chunked, tcp, split bool
	}{
		{"non-chunked upstream leg", deferredTestRev, false, false, false},
		{"chunked upstream leg", 54470, true, false, false},
		{"tcp: query and marker coalesced in one segment", deferredTestRev, false, true, false},
		{"tcp: marker fragmented across segments", deferredTestRev, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hooks := newSynthesizedHooks()
			h := newSynthesizedHarness(t, hooks, tc.rev, tc.chunked, tc.tcp)
			payload := synthesizedPacket(t, tc.rev)
			upDone := make(chan error, 1)
			go synthUpstream(t, h.upstreamProxy, tc.rev, tc.chunked, &hooks.strictRan, func(up *chproto.Codec) error {
				if err := up.WriteRawPacket(encodeTableColumnsPacket("t", "v UInt64")); err != nil {
					return err
				}
				if err := up.WriteRawPacket(encodeServerSampleNamed(t, tc.rev, "v")); err != nil {
					return err
				}
				if err := readPayloadAndTerminator(up, payload); err != nil {
					return err
				}
				select {
				case <-hooks.inputDone:
				case <-time.After(time.Second):
					return errors.New("OnQueryInputComplete did not fire within 1s")
				}
				return up.WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)})
			}, upDone)

			query, marker := encodeInsertQuery(t, "qid", synthesizedBody), encodeEmptyClientData(t)
			switch {
			case tc.split:
				writeAllConn(t, h.clientProxy, query)
				for i := range marker {
					writeAllConn(t, h.clientProxy, marker[i:i+1])
				}
			case tc.tcp:
				writeAllConn(t, h.clientProxy, append(append([]byte(nil), query...), marker...))
			default:
				writeAllConn(t, h.clientProxy, query)
				writeAllConn(t, h.clientProxy, marker)
			}
			if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
				t.Fatalf("client got server packet %d, want EndOfStream", got[0])
			}
			if err := <-upDone; err != nil {
				t.Fatalf("upstream flow: %v", err)
			}
			strict, inputs, completes, aborts, successes := hooks.counts()
			if strict != 1 || inputs != 1 || completes != 1 || aborts != 0 || successes != 1 {
				t.Fatalf("hooks strict/input/complete/abort/success = %d/%d/%d/%d/%d, want 1/1/1/0/1",
					strict, inputs, completes, aborts, successes)
			}
			if !bytes.Equal(hooks.payloadAtStrict, payload) {
				t.Fatalf("payload hashed at the strict hook = %x, want %x", hooks.payloadAtStrict, payload)
			}
			for _, err := range h.close(t) {
				if err != nil && !errors.Is(err, io.EOF) {
					t.Logf("relay loop returned: %v", err)
				}
			}
		})
	}
}
```

This pins the whole contract: the strict hook before `WriteQuery`, the marker forwarded exactly once (fragmented and coalesced alike), the upstream `TableColumns` skipped and the sample consumed (the client only ever sees the single `EndOfStream` byte), the plan's packets then exactly one terminator, `OnQueryInputComplete`, and success on the framed `EndOfStream`, on both a chunked and a non-chunked upstream leg.

- [ ] **Step 5: Run it and see it fail**

```bash
bazel test //pkg/proxy:proxy_test --test_filter='TestRelay_SynthesizedInsert_ForwardsPlanPayloadAfterSampleGate'
```

Expected: build failure `r.runSynthesizedInsert undefined` (with Task 3 merged; without it, also `qctx.SynthesizedInsert undefined`).

- [ ] **Step 6: Write the lane**

Append to `pkg/proxy/relay_synthesized.go`:

```go
// synthesizedMarkerAllowance bounds the one client packet this lane reads: a
// single empty external-tables marker. Anything larger is an external table or
// a payload block, both of which the lane refuses.
const synthesizedMarkerAllowance uint64 = 4 << 10

// runSynthesizedInsert is runDeferredInsert in the reverse direction (spec
// 2026-09-23 D8): the rows come from an evaluated plan instead of the client.
// The client sent a complete inline INSERT ... VALUES statement plus exactly
// one empty Data marker, so the lane reads that marker, encodes the plan's
// blocks into client Data packets at the upstream codec's negotiated revision,
// and hands the shared forwarder a payload the strict hook signs before the
// Query reaches upstream.
func (r *Relay) runSynthesizedInsert(ctx context.Context, qctx *plugin.QueryContext, compression proto.Compression) error {
	_, logger := log.FromContext(ctx)
	client := r.sess.Client()
	plan := qctx.SynthesizedInsert
	q := qctx.Query
	rejectClose := func(err error) error {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		r.writeExceptionToClient(ctx, err)
		return err
	}
	// Nothing has been written upstream and the marker was consumed whole, so a
	// retryable rejection ends only this query and leaves both packet streams on
	// a clean boundary.
	rejectResume := func(err error) error {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		r.writeExceptionToClient(ctx, err)
		return fmt.Errorf("%w: %w", errQueryRejectedResume, err)
	}
	if len(plan.Blocks) == 0 || len(plan.SampleColumns) == 0 {
		return rejectClose(fmt.Errorf("query %q: synthesized INSERT plan carries %d blocks and %d sample columns, want both non-empty",
			q.ID, len(plan.Blocks), len(plan.SampleColumns)))
	}
	if compression == proto.CompressionEnabled {
		return rejectClose(fmt.Errorf("query %q: synthesized INSERT requires an uncompressed session", q.ID))
	}

	// 1. The client's single empty external-tables marker.
	var markerRaw []byte
	for markerRaw == nil {
		pkt, decErr := client.ReadPacketWithDataLimit(synthesizedMarkerAllowance, uint64(chproto.ClientQueryCode))
		if errors.Is(decErr, chproto.ErrPacketTooLarge) {
			return rejectClose(fmt.Errorf("synthesized INSERT %q received an oversized client packet: %w", q.ID, decErr))
		}
		if pkt == nil || decErr != nil {
			r.hooks.OnQueryAbort(ctx, qctx)
			r.hooks.OnQueryComplete(ctx, r.sess)
			if pkt == nil && decErr == nil {
				return io.EOF
			}
			return fmt.Errorf("read synthesized INSERT marker: %w", decErr)
		}
		if r.obs != nil {
			r.obs.ClientPacket(clientPacketName(pkt.Type))
			r.obs.BytesTransferred("client_to_upstream", float64(pkt.RawLen))
		}
		switch pkt.Type {
		case uint64(chproto.ClientDataCode):
			info, err := chproto.InspectClientDataPacket(pkt.Raw, compression)
			if err != nil {
				r.hooks.OnQueryAbort(ctx, qctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
				return fmt.Errorf("classify synthesized client data packet: %w", err)
			}
			if info.BlockName != "" {
				return rejectClose(fmt.Errorf(
					"synthesized INSERT %q received external table block %q; external tables are not supported on the storage-integrity signed lane",
					q.ID, info.BlockName))
			}
			if !info.Empty {
				// The rows are already inside the signed statement; a payload
				// block would be unsigned bytes riding the same INSERT.
				return rejectClose(fmt.Errorf(
					"synthesized INSERT %q received a client payload block; an inline VALUES statement streams no data", q.ID))
			}
			markerRaw = append([]byte(nil), pkt.Raw...)
		case uint64(chproto.ClientCancelCode):
			// Nothing reached upstream. ClickHouse answers a cancelled query with
			// EndOfStream; do the same locally and drop the plan.
			r.hooks.OnQueryAbort(ctx, qctx)
			r.hooks.OnQueryComplete(ctx, r.sess)
			logger.Debugw("synthesized INSERT cancelled by client before forwarding", "query_id", q.ID)
			if err := client.WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
				return fmt.Errorf("write end-of-stream after synthesized cancel: %w", err)
			}
			return nil
		default:
			return rejectClose(fmt.Errorf(
				"client sent packet type %d (%s) while synthesized INSERT %q was awaiting its external-tables marker",
				pkt.Type, clientPacketName(pkt.Type), q.ID))
		}
	}

	// 2. Encode the payload at the upstream codec's negotiated revision.
	up := r.sess.Upstream()
	if up == nil {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		return chsession.ErrNoUpstream
	}
	up.SetCompression(compression)
	packets := make([][]byte, 0, len(plan.Blocks))
	var payloadBytes uint64
	for i, cols := range plan.Blocks {
		raw, err := up.EncodeClientDataPacket(cols)
		if err != nil {
			return rejectClose(fmt.Errorf("synthesized INSERT %q: encode block %d: %w", q.ID, i, err))
		}
		payloadBytes += uint64(len(raw))
		packets = append(packets, raw)
	}
	// The chain's strict-data budget is this statement's max_payload_bytes.
	if limit, enforce := r.hooks.ClientDataReadLimit(qctx); enforce && payloadBytes > limit {
		return rejectResume(fmt.Errorf("synthesized INSERT %q payload of %d bytes exceeds limit of %d bytes",
			q.ID, payloadBytes, limit))
	}
	plan.Packets = packets
	plan.PayloadBytes = payloadBytes
	logger.Debugw("synthesized INSERT payload encoded",
		"query_id", q.ID, "packets", len(packets), "payload_bytes", payloadBytes, "revision", up.Revision())

	// 3-7. Strict input completion, Query, marker, sample gate, payload, exactly
	// one terminator, OnQueryInputComplete and terminal arbitration.
	return r.forwardSignedInsert(ctx, qctx, signedInsertForward{
		lane:          "synthesized INSERT",
		compression:   compression,
		markerRaw:     markerRaw,
		terminatorRaw: nil,
		payload:       packets,
		payloadBytes:  payloadBytes,
		sampleColumns: plan.SampleColumns,
		rejectClose:   rejectClose,
		rejectResume:  rejectResume,
	})
}
```

- [ ] **Step 7: Wire the dispatch and the exclusivity rule**

In the `AgentPrepare` guard (relay.go:1041) replace

```go
				if qctx.QueryOnly != nil || qctx.DeferredInsert != nil || qctx.SuppressUpstreamExecution || qctx.AbortWithSuccess {
```

with

```go
				if qctx.QueryOnly != nil || qctx.DeferredInsert != nil || qctx.SynthesizedInsert != nil || qctx.SuppressUpstreamExecution || qctx.AbortWithSuccess {
```

In the `QueryOnly` guard (relay.go:1088) replace

```go
				if agentPrepared != nil || qctx.DeferredInsert != nil || qctx.SuppressUpstreamExecution || qctx.AbortWithSuccess {
```

with

```go
				if agentPrepared != nil || qctx.DeferredInsert != nil || qctx.SynthesizedInsert != nil || qctx.SuppressUpstreamExecution || qctx.AbortWithSuccess {
```

and insert this block immediately before the `// AbortWithSuccess: a plugin (commitgate via ErrAbortWithSuccess)` comment at relay.go:1114:

```go
			if qctx.SynthesizedInsert != nil {
				if qctx.DeferredInsert != nil || qctx.SuppressUpstreamExecution || qctx.AbortWithSuccess {
					err := fmt.Errorf("query %q: SynthesizedInsert conflicts with another ownership plan", q.ID)
					r.hooks.OnQueryAbort(ctx, qctx)
					r.hooks.OnQueryComplete(ctx, r.sess)
					r.writeExceptionToClient(ctx, err)
					rejectedQctx = qctx
					continue
				}
				if err := r.runSynthesizedInsert(ctx, qctx, clientCompression); err != nil {
					if errors.Is(err, errQueryRejectedResume) {
						logger.Infow("synthesized INSERT rejected without closing the session",
							"query_id", q.ID, "err", err)
						continue
					}
					return err
				}
				continue
			}
```

- [ ] **Step 8: Run gazelle and the happy-path test**

```bash
bazel run //:gazelle
bazel test //pkg/proxy:proxy_test --test_filter='TestRelay_SynthesizedInsert_ForwardsPlanPayloadAfterSampleGate'
```

Expected: PASS for all four subtests. Gazelle adds the two new files and `//pkg/replay/nativepayload` to the `proxy_test` deps.

- [ ] **Step 9: Write the rejection tests**

Append to `relay_synthesized_test.go`:

```go
func readSomeConn(t *testing.T, c net.Conn) []byte {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return buf[:n]
}

func assertNoUpstreamBytes(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	buf := make([]byte, 1)
	if n, err := c.Read(buf); err == nil || n > 0 {
		t.Fatalf("upstream received %d bytes (%x), want nothing", n, buf[:n])
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Logf("upstream read ended with %v", err)
	}
}

func TestRelay_SynthesizedInsert_ClientFailuresBeforeUpstream(t *testing.T) {
	for _, tc := range []struct {
		name    string
		send    func(t *testing.T, h *deferredHarness)
		wantEOS bool
		wantExc bool
	}{
		{"marker never arrives, client disconnects", func(t *testing.T, h *deferredHarness) { h.clientProxy.Close() }, false, false},
		{"marker is a payload block", func(t *testing.T, h *deferredHarness) {
			writeAllConn(t, h.clientProxy, synthesizedPacket(t, deferredTestRev))
		}, false, true},
		{"cancel before the upstream query", func(t *testing.T, h *deferredHarness) {
			writeAllConn(t, h.clientProxy, []byte{byte(chproto.ClientCancelCode)})
		}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hooks := newSynthesizedHooks()
			h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
			tc.send(t, h)
			switch {
			case tc.wantEOS:
				if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
					t.Fatalf("client got packet %d, want EndOfStream", got[0])
				}
			case tc.wantExc:
				if got := readSomeConn(t, h.clientProxy); got[0] != byte(chproto.ServerExceptionCode) {
					t.Fatalf("client got packet %d, want Exception", got[0])
				}
			}
			assertNoUpstreamBytes(t, h.upstreamProxy)
			strict, inputs, _, aborts, successes := hooks.counts()
			if strict != 0 || inputs != 0 || aborts != 1 || successes != 0 {
				t.Fatalf("hooks strict/input/abort/success = %d/%d/%d/%d, want 0/0/1/0", strict, inputs, aborts, successes)
			}
			h.close(t)
		})
	}
}

func TestRelay_SynthesizedInsert_UpstreamSampleStepFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		answer   func(t *testing.T) []byte
		wantExc  bool
		wantText string
	}{
		{"upstream exception at the sample step", func(t *testing.T) []byte {
			return encodeServerExceptionPacket(deferredTestRev, 60, "Table db.t does not exist")
		}, true, "does not exist"},
		{"premature end of stream before the payload", func(t *testing.T) []byte {
			return []byte{byte(chproto.ServerEndOfStreamCode)}
		}, false, ""},
		{"sample names a different column", func(t *testing.T) []byte {
			return encodeServerSampleNamed(t, deferredTestRev, "other")
		}, true, "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hooks := newSynthesizedHooks()
			h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
			answer := tc.answer(t)
			upDone := make(chan error, 1)
			go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan,
				func(up *chproto.Codec) error { return up.WriteRawPacket(answer) }, upDone)
			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
			writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
			if tc.wantExc {
				got := readSomeConn(t, h.clientProxy)
				if got[0] != byte(chproto.ServerExceptionCode) || !bytes.Contains(got, []byte(tc.wantText)) {
					t.Fatalf("client got %x, want an Exception mentioning %q", got, tc.wantText)
				}
			}
			if err := <-upDone; err != nil {
				t.Fatalf("upstream flow: %v", err)
			}
			if _, _, _, _, successes := hooks.counts(); successes != 0 {
				t.Fatalf("OnQuerySuccess fired %d times, want 0", successes)
			}
			h.close(t)
		})
	}
}
```

- [ ] **Step 10: Run the rejection tests**

```bash
bazel test //pkg/proxy:proxy_test --test_filter='TestRelay_SynthesizedInsert_(ClientFailuresBeforeUpstream|UpstreamSampleStepFailures)'
```

Expected: PASS (6 subtests). If the premature-`EndOfStream` subtest hangs, `upstreamToClient`'s `ServerEndOfStreamCode` arm is not settling the sample gate — fix the lane, not the test.

- [ ] **Step 11: Write the back-pressure, interruption, exclusivity and truncated-shape tests**

Append to `relay_synthesized_test.go`:

```go
func TestRelay_SynthesizedInsert_BackpressureAfterPayloadKeepsSession(t *testing.T) {
	hooks := newSynthesizedHooks()
	h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
	payload := synthesizedPacket(t, deferredTestRev)
	upDone := make(chan error, 1)
	go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan, func(up *chproto.Codec) error {
		if err := up.WriteRawPacket(encodeServerSampleNamed(t, deferredTestRev, "v")); err != nil {
			return err
		}
		if err := readPayloadAndTerminator(up, payload); err != nil {
			return err
		}
		<-hooks.inputDone
		return up.WriteRawPacket(encodeServerExceptionPacket(deferredTestRev, chproto.CodeTooManyParts,
			"storage_integrity: back-pressure: unsafe parts over limit"))
	}, upDone)
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
	writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
	if got := readSomeConn(t, h.clientProxy); got[0] != byte(chproto.ServerExceptionCode) {
		t.Fatalf("client got packet %d, want Exception", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	if _, _, _, _, successes := hooks.counts(); successes != 0 {
		t.Fatalf("OnQuerySuccess fired %d times after a 252 rejection, want 0", successes)
	}
	// KeepSession: neither relay loop has returned.
	select {
	case err := <-h.loopErr:
		t.Fatalf("a relay loop exited after the session-preserving 252: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	h.close(t)
}

func TestRelay_SynthesizedInsert_ClientPacketsWhileAwaitingUpstream(t *testing.T) {
	for _, secondQuery := range []bool{false, true} {
		name := "cancel after the upstream query"
		if secondQuery {
			name = "second query while the lane is active"
		}
		t.Run(name, func(t *testing.T) {
			hooks := newSynthesizedHooks()
			h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
			upDone := make(chan error, 1)
			// The upstream never answers the sample, so the lane is parked in its
			// liveness-arbitration loop when the client packet arrives.
			go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan,
				func(up *chproto.Codec) error { return nil }, upDone)
			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
			writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
			if err := <-upDone; err != nil {
				t.Fatalf("upstream flow: %v", err)
			}
			send := []byte{byte(chproto.ClientCancelCode)}
			if secondQuery {
				send = encodeInsertQuery(t, "qid2", synthesizedBody)
			}
			writeAllConn(t, h.clientProxy, send)
			var got error
			for _, err := range h.close(t) {
				if err != nil && !errors.Is(err, io.EOF) {
					got = err
				}
			}
			if got == nil {
				t.Fatal("the lane kept running after the client interrupted the sample wait")
			}
			if _, _, _, _, successes := hooks.counts(); successes != 0 {
				t.Fatalf("OnQuerySuccess fired %d times, want 0", successes)
			}
		})
	}
}

func TestRelay_SynthesizedInsert_MutuallyExclusiveWithDeferredInsert(t *testing.T) {
	hooks := newSynthesizedHooks()
	hooks.alsoDefer = true
	h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
	got := readSomeConn(t, h.clientProxy)
	if got[0] != byte(chproto.ServerExceptionCode) || !bytes.Contains(got, []byte("conflicts with another ownership plan")) {
		t.Fatalf("client got %x, want an Exception naming the plan conflict", got)
	}
	assertNoUpstreamBytes(t, h.upstreamProxy)
	strict, _, completes, aborts, _ := hooks.counts()
	if strict != 0 || completes != 1 || aborts != 1 {
		t.Fatalf("hooks strict/complete/abort = %d/%d/%d, want 0/1/1", strict, completes, aborts)
	}
	h.close(t)
}

// The 25.x CLI truncates the statement after VALUES and streams the rows, so
// sistatement installs no plan: the query must travel the ordinary path exactly
// as it does today, and a stray Data packet after the terminator must still hit
// the "client Data packet has no active query" guard.
func TestRelay_SynthesizedInsert_TruncatedValuesShapeStaysOnOrdinaryPath(t *testing.T) {
	const truncated = "INSERT INTO t VALUES "
	hooks := newSynthesizedHooks()
	hooks.skipPlanFor = truncated
	h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
	rows, empty := synthesizedPacket(t, deferredTestRev), encodeEmptyClientData(t)
	upDone := make(chan error, 1)
	go func() {
		up := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		up.SetRevision(deferredTestRev)
		up.SetCompression(proto.CompressionDisabled)
		upDone <- func() error {
			pkt, err := up.ReadPacket(uint64(chproto.ClientQueryCode))
			if err != nil {
				return fmt.Errorf("read query: %w", err)
			}
			if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.Body != truncated {
				return fmt.Errorf("upstream query = %#v, want %q", pkt.Decoded, truncated)
			}
			for _, want := range [][]byte{rows, empty} {
				got, err := up.ReadPacket()
				if err != nil || !bytes.Equal(got.Raw, want) {
					return fmt.Errorf("upstream packet = %v (err %v), want %x", got, err, want)
				}
			}
			return nil
		}()
	}()
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", truncated))
	writeAllConn(t, h.clientProxy, rows)
	writeAllConn(t, h.clientProxy, empty)
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	if strict, _, _, _, _ := hooks.counts(); strict != 0 {
		t.Fatalf("the synthesized lane ran for the truncated shape (strict hook fired %d times)", strict)
	}
	writeAllConn(t, h.clientProxy, rows)
	var sawGuard bool
	for _, err := range h.close(t) {
		if err != nil && strings.Contains(err.Error(), "client Data packet has no active query") {
			sawGuard = true
		}
	}
	if !sawGuard {
		t.Fatal("a stray client Data packet no longer hits the no-active-query guard")
	}
}
```

- [ ] **Step 12: Run the whole synthesized suite**

```bash
bazel test //pkg/proxy:proxy_test --test_filter='TestRelay_SynthesizedInsert'
```

Expected: PASS — 15 subtests across 7 test functions.

- [ ] **Step 13: Run the package and unit suites, then commit**

```bash
bazel test //pkg/proxy:proxy_test
bazel test //...
git add pkg/proxy/relay_synthesized.go pkg/proxy/relay_synthesized_test.go pkg/proxy/relay.go pkg/proxy/BUILD.bazel
git commit -m "$(cat <<'EOF'
feat(proxy): add the synthesized INSERT relay lane

runSynthesizedInsert mirrors runDeferredInsert in reverse: it reads the client's
single external-tables marker, encodes the evaluated plan's blocks into client
Data packets at the upstream codec's negotiated revision, and forwards Query,
marker, payload and exactly one terminator once the strict input-complete hook
has signed those exact bytes. The post-input half of runDeferredInsert moves
into forwardSignedInsert so both lanes share the sample gate and the terminal
arbitration; the synthesized lane additionally validates the consumed upstream
sample against the plan's columns.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
)"
```

Expected: `bazel test //...` matches the clean-`main` baseline; the pre-existing `TestRelay_DeferredInsert*` set is unchanged, which is what proves the extraction was behavior-neutral.

---

### Task 6: `buildAgent` wiring, docker integration test, CI registration

Implements D10's wiring and startup refusal, D12's default-off agent-only surface, and spec section 6's integration cases.

**Files:** Modify `build.go` `buildAgentWithMaterializerBuilder` (`:1084-1217`), `pkg/integration/testenv/cli.go` (add `CLIVersion`), `pkg/integration/BUILD.bazel`; create `pkg/integration/storage_integrity_inline_values_test.go`; verify `.github/workflows/ci.yml` needs no edit.

**Interfaces:**

- Consumes: `sistatement.NewUpstreamValuesEvaluator`, `sistatement.ValuesEvaluator`, `sistatement.InlineValuesOptions`, `sistatement.Options.{Evaluator,Observer,InlineValues}` (Task 4); `config.StorageIntegrityInlineValuesConfig` (Task 3); `*proxy.MetricsObserver` implementing `sistatement.Observer` (Task 3); `testenv.StartServerProxy`, `testenv.StartAgentProxy`, `testenv.WithConfigMutator`, `testenv.WithRewriterMock`, `testenv.WithDatabasePermission`, `testenv.RunCLIStdin`, `testenv.ClickHouseCLI`; `nativepayload.Decode`, `nativepayload.Materializer`, `payloadexec.NewWithMaterializer`, `(*payloadexec.Executor).GenesisSnapshot`, `(*payloadexec.Executor).ApplyContext`.
- Produces: `func CLIVersion(t *testing.T) (major, minor int)` in `package testenv`.

**Steps:**

- [ ] **Step 1: Add the CLI version probe to `testenv`.** `testenv` has no version probe today; `ClickHouseCLI` only locates the binary. Append to `pkg/integration/testenv/cli.go` (its imports already carry `os/exec`, `strings`, `testing`; add `regexp` and `strconv`):

```go
// CLIVersion reports the major and minor version the installed clickhouse
// binary announces. CI's server image is pinned to clickhouse-server:25.8 while the client comes from clickhouse.com/install.sh, which pins no version -- so the CLI in CI is whatever 26.x is current. That asymmetry matters because only a 26.3+ client puts an inline INSERT ... VALUES statement's rows in the query text; a 25.x client parses them itself and streams Native blocks. Skipped, not failed, when the version is unreadable.
func CLIVersion(t *testing.T) (major, minor int) {
	t.Helper()
	out, err := exec.Command(ClickHouseCLI(t), "client", "--version").CombinedOutput()
	if err != nil { t.Skipf("clickhouse client --version failed: %v (%s)", err, strings.TrimSpace(string(out))) }
	m := regexp.MustCompile(`(\d+)\.(\d+)\.`).FindStringSubmatch(string(out))
	if m == nil { t.Skipf("cannot parse clickhouse version %q", strings.TrimSpace(string(out))) }
	major, _ = strconv.Atoi(m[1])
	minor, _ = strconv.Atoi(m[2])
	return major, minor
}
```

- [ ] **Step 2: Write the failing docker integration test.** Create `pkg/integration/storage_integrity_inline_values_test.go`. The clickhouse-go `Exec` cases always run — that driver produces the 26.x shape whatever the CLI is — and only the CLI case is version-gated:

```go
package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/cfgtypes"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/rewriter"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// startInlineValuesPair is startSIAgentPair plus the two agent-side blocks
// the inline lane needs: the native materializer (spec D2 makes it mandatory) and storage_integrity.agent.inline_values.
func startInlineValuesPair(t *testing.T, networkID string, serverOpts ...testenv.ProxyOption) (*testenv.TestProxy, *capturingConsumer) {
	t.Helper()
	ffi := strings.TrimSpace(os.Getenv("POLYGLOT_SQL_FFI_PATH"))
	if ffi == "" { t.Skip("POLYGLOT_SQL_FFI_PATH is unset; the inline VALUES lane requires the native materializer") }
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil { t.Fatal(err) }
	ch := openConn(t, chEnv.Addr)
	if err := ch.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS "+chEnv.Database+".si_events (id UInt64, region String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	consumer := &capturingConsumer{}
	rewriterOpt, rewriterMock := testenv.WithRewriterMock(t)
	rewriterMock.SetAccessedTables("INSERT INTO "+chEnv.Database+".si_events", []*pb.AccessedTable{{
		OriginalDatabase: chEnv.Database, OriginalTable: "si_events",
		LogicalDatabase: chEnv.Database, PhysicalDatabase: chEnv.Database, IsStorageIntegrity: true,
	}})
	server := testenv.StartServerProxy(t, chEnv.Addr, append([]testenv.ProxyOption{
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), chEnv.Database, registry.DbAuthWrite),
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
			cfg.StorageIntegrity.Ingress.NetworkID = networkID
		}),
		func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityAdmissionConsumer = consumer },
	}, serverOpts...)...)
	agentProxy := testenv.StartAgentProxy(t, authTestKey1, server.Addr,
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
			cfg.StorageIntegrity.Agent.InlineValues = config.StorageIntegrityInlineValuesConfig{
				Enabled: true, EvaluationTimeout: cfgtypes.Duration{Duration: 10 * time.Second}, MaxRows: 65536,
			}
			cfg.Materialize.Enabled = true
			cfg.Materialize.Engine = rewriter.EngineNative
			cfg.Materialize.NativeLibraryPath = ffi
		}),
	)
	return agentProxy, consumer
}

func waitAdmissions(t *testing.T, consumer *capturingConsumer, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		consumer.mu.Lock()
		n := len(consumer.seen)
		consumer.mu.Unlock()
		if n >= want { return }
		if time.Now().After(deadline) { t.Fatalf("saw %d admissions, want %d", n, want) }
		time.Sleep(20 * time.Millisecond)
	}
}

// inlineNativeRoot replays one admitted Native payload through the
// in-process executor under a fixed statement id, so two payloads carrying the same rows yield the same state root regardless of which statement shipped them.
func inlineNativeRoot(t *testing.T, networkID string, payload []byte) string {
	t.Helper()
	schema := siAgentSchema()
	exec := payloadexec.NewWithMaterializer(networkID, nativepayload.Materializer{NetworkID: networkID}, schema)
	gen, err := exec.GenesisSnapshot(0, "schema-1", "inline-values")
	if err != nil { t.Fatalf("genesis: %v", err) }
	sql := "INSERT INTO " + schema.TableID + " FORMAT Native"
	stmt := replay.Statement{
		StatementID: "inline-probe", StatementSeq: 1, SQL: sql, SQLHash: replay.DigestString(sql),
		SettingsHash: sicore.EmptySettingsHash, PayloadRef: "probe", PayloadHash: replay.DigestBytes(payload),
		PayloadLength: uint64(len(payload)), TargetTableID: schema.TableID,
		PayloadFormat: replay.PayloadFormatClickHouseNativeData, ClientRevision: 54460,
		SchemaHash: payloadexec.TableSchemaHash(networkID, schema),
	}
	job := replay.ReplayJob{
		BlockSeq: 1, PrevSafeSnapshotID: gen.SnapshotID, PrevStateRoot: gen.StateRoot,
		SchemaSnapshotID: gen.SchemaSnapshotID, ExecutorProfileID: gen.ExecutorProfileID,
		Statements: []replay.Statement{stmt},
	}
	_, res, err := exec.ApplyContext(context.Background(), gen, job, []replay.PreparedStatement{{Statement: stmt, Payload: payload}})
	if err != nil { t.Fatalf("replay: %v", err) }
	return res.ComputedStateRoot
}

// TestStorageIntegrity_InlineValuesSignedEndToEnd drives the pinned
// clickhouse-go Exec, which puts the full statement text on the wire followed by one empty Data block -- the 26.x inline shape. The rows carry a literal, an arithmetic expression and now(), which materialize must turn into a constant before the closure gate sees it. The same rows re-sent through the ordinary FORMAT Values stdin path must produce a byte-identical payload and therefore the same replay state root.
func TestStorageIntegrity_InlineValuesSignedEndToEnd(t *testing.T) {
	const networkID = "itest-inline"
	agentProxy, consumer := startInlineValuesPair(t, networkID)
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil { t.Fatal(err) }
	conn := openConnNoCompression(t, agentProxy.Addr)
	insert := fmt.Sprintf("INSERT INTO %s.si_events (id, region) VALUES (toUInt64(toUnixTimestamp(now())), 'eu'), (1 + 2, upper('us'))", chEnv.Database)
	if err := conn.Exec(context.Background(), insert); err != nil { t.Fatalf("inline INSERT through the agent: %v", err) }
	waitAdmissions(t, consumer, 1)
	consumer.mu.Lock()
	adm := consumer.seen[0]
	consumer.mu.Unlock()
	if want := "INSERT INTO " + chEnv.Database + ".si_events (id, region) FORMAT Native"; adm.SQL != want {
		t.Fatalf("signed SQL = %q, want %q", adm.SQL, want)
	}
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := auth.JWSStatementPayloadV2{
		NetworkID: networkID, StatementID: adm.StatementID, SQLHash: replay.DigestString(adm.SQL),
		SettingsHash: sicore.EmptySettingsHash, SchemaHash: adm.SchemaHash,
		PayloadHash: replay.DigestBytes(adm.Payload.Bytes), PayloadLength: uint64(len(adm.Payload.Bytes)),
		PayloadFormat: sicore.PayloadEncodingClickHouseNativeData, ClientRevision: uint32(adm.Payload.Revision),
		TargetTableID: chEnv.Database + ".si_events", RowIDProfileID: payloadexec.RowIDProfileID,
		StatementKind: sicore.StatementKindCodeInsert,
	}
	if got, err := validator.ValidateStatementV2(adm.UserJWS, want); err != nil || got != signer.Address() {
		t.Fatalf("the synthesized payload is not what was signed: signer=%s err=%v", got, err)
	}
	rows, err := nativepayload.Decode(siAgentSchema(), adm.Payload.Revision, adm.Payload.Bytes)
	if err != nil { t.Fatalf("synthesized bytes are not Native ClientData packets: %v", err) }
	if len(rows) != 2 || rows[0].Values[1] != "eu" || rows[1].Values[0] != uint64(3) || rows[1].Values[1] != "US" {
		t.Fatalf("decoded rows = %+v", rows)
	}
	// The reference text is rendered from the decoded rows, so the materialized
	// now() instant is reproduced exactly and the two payloads must match byte for byte (the literal-row byte-identity case of spec section 6).
	literals := make([]string, 0, len(rows))
	for _, r := range rows { literals = append(literals, fmt.Sprintf("(%d,'%s')", r.Values[0].(uint64), r.Values[1].(string))) }
	out, err := testenv.RunCLIStdin(t, testenv.ClickHouseCLI(t), agentProxy.Addr, "",
		"INSERT INTO "+chEnv.Database+".si_events FORMAT Values", strings.Join(literals, "")+"\n")
	if err != nil { t.Fatalf("FORMAT Values reference INSERT: %v\nout: %s", err, out) }
	waitAdmissions(t, consumer, 2)
	consumer.mu.Lock()
	ref := consumer.seen[1]
	consumer.mu.Unlock()
	if adm.Payload.SHA256 != ref.Payload.SHA256 || adm.Payload.Length != ref.Payload.Length {
		t.Fatalf("synthesized payload %s/%d != CLI payload %s/%d",
			adm.Payload.SHA256, adm.Payload.Length, ref.Payload.SHA256, ref.Payload.Length)
	}
	if got, wantRoot := inlineNativeRoot(t, networkID, adm.Payload.Bytes), inlineNativeRoot(t, networkID, ref.Payload.Bytes); got != wantRoot {
		t.Fatalf("inline replay root %s != FORMAT Values replay root %s", got, wantRoot)
	}
}

// TestStorageIntegrity_InlineValuesClosureRefused pins the D3/D9
// disposition: a server-state function never reaches ClickHouse and the client sees the prefixed error.
func TestStorageIntegrity_InlineValuesClosureRefused(t *testing.T) {
	agentProxy, _ := startInlineValuesPair(t, "itest-inline-closure")
	conn := openConnNoCompression(t, agentProxy.Addr)
	err := conn.Exec(context.Background(), "INSERT INTO "+chEnv.Database+".si_events (id, region) VALUES (1, currentDatabase())")
	if err == nil { t.Fatal("a server-state function must be refused before signing") }
	if !strings.Contains(err.Error(), sicore.InlineValuesErrorPrefix) || !strings.Contains(err.Error(), "currentDatabase") {
		t.Fatalf("error = %v, want the prefixed closure refusal naming currentDatabase", err)
	}
}

// TestCLI_InlineValuesShapeByClientVersion pins D1 from the CLI side: a
// 26.3+ client sends the rows in the query text and takes the new lane; an older client parses them itself and stays on the ordinary streaming path, which this test cannot exercise, so it skips with a printed reason.
func TestCLI_InlineValuesShapeByClientVersion(t *testing.T) {
	agentProxy, consumer := startInlineValuesPair(t, "itest-inline-cli",
		testenv.WithConfigMutator(func(cfg *config.Config) { cfg.Rewriter.PhysicalDatabase = chEnv.Database }))
	major, minor := testenv.CLIVersion(t)
	if major < 26 || (major == 26 && minor < 3) {
		t.Skipf("clickhouse client %d.%d parses inline VALUES client-side; the inline lane needs 26.3 or later", major, minor)
	}
	out, err := testenv.RunCLIStdin(t, testenv.ClickHouseCLI(t), agentProxy.Addr, "",
		"INSERT INTO "+chEnv.Database+".si_events (id, region) VALUES (11, 'eu')", "")
	if err != nil { t.Fatalf("26.3+ CLI inline INSERT was refused: %v\nout: %s", err, out) }
	waitAdmissions(t, consumer, 1)
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if want := "INSERT INTO " + chEnv.Database + ".si_events (id, region) FORMAT Native"; consumer.seen[0].SQL != want {
		t.Fatalf("CLI inline signed SQL = %q, want %q", consumer.seen[0].SQL, want)
	}
}
```

- [ ] **Step 3: Register the file with Bazel and run it to see it fail.** In `pkg/integration/BUILD.bazel` insert `        "storage_integrity_inline_values_test.go",` between `"storage_integrity_formats_test.go",` and `"storage_integrity_read_test.go",`, then run:

```bash
bazel test //pkg/integration:integration_test \
  --test_filter='TestStorageIntegrity_InlineValues|TestCLI_InlineValuesShapeByClientVersion' \
  --test_env=POLYGLOT_SQL_FFI_PATH --test_output=errors
```

  Expected failure: the agent proxy starts with `inline_values.enabled` but `sistatement` is constructed without an evaluator, so `StartAgentProxy` fails with `storage_integrity.agent: sistatement: values evaluator is required when inline_values is enabled`.

- [ ] **Step 4: Move the dialer construction above the SI block in `build.go`.** Delete these lines from their current position after the `chain := &plugin.PluginChain{...}` literal:

```go
	routingAccount := strings.ToLower(cfg.Agent.Owner)
	if routingAccount == "" {
		routingAccount = signer.Address()
	}
	dialer, err := buildAgentDialer(opts, rf, signer.Address(), routingAccount, obs)
	if err != nil {
		return nil, err
	}
```

  and re-insert them verbatim immediately after `	agentPlug := &agent.Plugin{Signer: signer, Observer: obs, Owner: cfg.Agent.Owner, IsDriver: cfg.Agent.Driver}`. The dialer is a pure function of `opts`/`rf`/`signer` and both of its closures ignore the session argument, so building it earlier changes nothing for the relay and makes it available to the evaluator.

- [ ] **Step 5: Build the evaluator and thread the options.** In `build.go` add `	var evaluatorClose func()` next to `	var materializerClose func()`, replace the build-failure defer with:

```go
	defer func() {
		if buildSucceeded {
			return
		}
		if materializerClose != nil {
			materializerClose()
		}
		if evaluatorClose != nil {
			evaluatorClose()
		}
	}()
```

  then, inside `if cfg.StorageIntegrity.Agent.Enabled {` immediately after the `seq, err := sistatement.OpenSeqCounter(...)` block, insert:

```go
		inlineCfg := cfg.StorageIntegrity.Agent.InlineValues
		var evaluator sistatement.ValuesEvaluator
		if inlineCfg.Enabled {
			// Spec D10: the lane cannot start without a materializer, a dialer and a
			// signer. Config.Validate already requires materialize.enabled; this is the injection-side half of the same refusal.
			if !cfg.Materialize.Enabled {
				return nil, fmt.Errorf("storage_integrity.agent.inline_values requires materialize.enabled")
			}
			if dialer == nil {
				return nil, fmt.Errorf("storage_integrity.agent.inline_values requires an upstream dialer")
			}
			up := sistatement.NewUpstreamValuesEvaluator(func(ctx context.Context) (net.Conn, error) {
				codec, derr := dialer(ctx, nil)
				if derr != nil {
					return nil, derr
				}
				conn, ok := codec.Conn().(net.Conn)
				if !ok {
					return nil, fmt.Errorf("agent dialer produced a %T upstream, not a net.Conn", codec.Conn())
				}
				return conn, nil
			}, signer)
			evaluatorClose = func() { _ = up.Close() }
			evaluator = up
			log.Infow("storage_integrity agent inline VALUES enabled",
				"evaluation_timeout", inlineCfg.EvaluationTimeout.Duration, "max_rows", inlineCfg.MaxRows)
		}
```

  replace `			MaxPayloadBytes: cfg.StorageIntegrity.Agent.MaxPayloadBytes,` in the `sistatement.New(sistatement.Options{...})` literal with:

```go
			MaxPayloadBytes: cfg.StorageIntegrity.Agent.MaxPayloadBytes,
			Evaluator:       evaluator,
			Observer:        obs,
			InlineValues: sistatement.InlineValuesOptions{
				Enabled:           inlineCfg.Enabled,
				EvaluationTimeout: inlineCfg.EvaluationTimeout.Duration,
				MaxRows:           inlineCfg.MaxRows,
			},
```

  and extend the `builtServer` teardown to call `materializerClose()` and `evaluatorClose()` when each is non-nil, in that order.

- [ ] **Step 6: Run the integration cases.**

```bash
bazel test //pkg/integration:integration_test \
  --test_filter='TestStorageIntegrity_InlineValuesSignedEndToEnd|TestStorageIntegrity_InlineValuesClosureRefused|TestCLI_InlineValuesShapeByClientVersion' \
  --test_env=POLYGLOT_SQL_FFI_PATH --test_output=errors
```

  Expected PASS: the two clickhouse-go `Exec` cases always run; the CLI case skips with a printed reason when the installed client is older than 26.3. All three skip when `POLYGLOT_SQL_FFI_PATH` is unset.

- [ ] **Step 7: Verify CI needs no change.** Run `grep -n 'integration:integration_test\|POLYGLOT_SQL_FFI_PATH\|install.sh\|clickhouse-server:25.8' .github/workflows/ci.yml` and confirm: `//pkg/integration:integration_test` is already in the explicit "Integration Tests" list, that step already passes `--test_env=POLYGLOT_SQL_FFI_PATH`, the FFI path is exported by the `fetch-rewriter-lib` step, the server image is pinned to `clickhouse/clickhouse-server:25.8`, and the client comes from an unpinned `install.sh` — so CI's client is 26.x and the gated CLI case executes. The new test file joins an already-registered target, so `ci.yml` is not edited.

- [ ] **Step 8: Format, re-sync Bazel, run the merge gate.** `gofmt -w build.go pkg/integration/`, `bazel run //:gazelle`, then `bazel build //...` and `bazel test //...` — expected PASS for every unit target (integration targets stay `manual`). Diff any failure against a clean `main` before calling it a regression.

- [ ] **Step 9: Commit.**

```bash
git add build.go pkg/integration/testenv/cli.go pkg/integration/testenv/BUILD.bazel \
  pkg/integration/storage_integrity_inline_values_test.go pkg/integration/BUILD.bazel
git commit -m "feat(agent): wire the inline VALUES evaluator into buildAgent

Builds UpstreamValuesEvaluator from the agent's own upstream dialer and signer,
threads storage_integrity.agent.inline_values and the metrics observer into the
sistatement plugin, refuses startup when materialize or the dialer is missing,
and adds testenv.CLIVersion plus the docker end-to-end cases.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

---

### Task 7: Documentation

Implements spec section 7 and records D12's operator-facing surface.

**Files:** Modify `README.md` (the unsupported-shapes sentence plus a config block) and `CLAUDE.md` (the sentence in the "signed INSERT lane's FORMAT allowlist" bullet); create `docs/agent-inline-values.md` and `docs/issue-153-tier2-measurement.md`.

**Interfaces:** documentation only; no Go symbols produced or consumed.

**Steps:**

- [ ] **Step 1: Replace the README's unsupported-shapes sentence and add the config block.** The exact current sentence (`grep -n 'Signed INSERT currently requires client-streamed rows' README.md`) is:

```
Signed INSERT currently requires client-streamed rows; inline `VALUES` and `INSERT ... SELECT` / `WITH` remain unsupported.
```

  Replace it with:

```
Signed INSERT accepts client-streamed rows and, when `storage_integrity.agent.inline_values.enabled` is set on the agent, an inline `INSERT ... VALUES` whose rows travel inside the query text — the shape a 26.3+ `clickhouse-client` and the pinned clickhouse-go `Exec` produce. `INSERT ... SELECT` / `WITH` remain unsupported.
```

  Then append after the existing `storage_integrity:` YAML example block:

````
```yaml
# Agent mode only; default off. Requires materialize.enabled.
storage_integrity:
  agent:
    enabled: true
    network_id: mainnet-1
    state_dir: /var/lib/housegate/si
    inline_values:
      enabled: true                # default false
      evaluation_timeout: 10s      # >= 1s; also the helper query's max_execution_time
      max_rows: 65536              # > 0; also max_result_rows and max_block_size
```

An inline VALUES statement is materialized, lexically closed, evaluated once through the tenant's own ClickHouse, encoded into Native blocks and re-signed as `INSERT INTO <db>.<table> (<columns>) FORMAT Native`; every refusal reaches the client as an exception whose message starts with `storage_integrity inline VALUES: `. Full flow: [docs/agent-inline-values.md](docs/agent-inline-values.md).
````

- [ ] **Step 2: Write the agent page.** Create `docs/agent-inline-values.md` (one paragraph per line, no hard wrapping):

```markdown
# Signed inline `INSERT ... VALUES` (agent mode)

Agent-only and default-off. It lets an agent sign a statement whose rows arrive inside the SQL text with no row payload on the wire — the shape a `clickhouse-client` 26.3 or later sends, and the shape the pinned clickhouse-go `Exec` sends. A 25.x client parses inline `VALUES` itself and already streams Native blocks, so it stays on the ordinary signed path and is unaffected by this feature.

## Flow

1. **Materialize.** The `materialize` plugin rewrites `now()`, `rand()`, `generateUUIDv4()` and friends to constant expressions. This lane is fail-closed on that outcome: `inline_values.enabled` requires `materialize.enabled` at startup, and a materialization error rejects the statement rather than falling open.
2. **Close.** A lexical gate accepts only literals, operators, parentheses and function calls. It refuses `SELECT`, `FROM`, `WITH`, `IN`, `NULL`, `DEFAULT`, `CAST`, `AS`, `INTERVAL`, `AND`, `OR`, `NOT`, bare identifiers, `{name:Type}` parameters, heredocs, comments, quoted identifiers, and every name on the nondeterministic and server-state deny lists (`now`, `rand`, `dictGet*`, `currentUser`, `currentDatabase`, `hostName`, `version`, `getSetting`, `sleep`, `throwIf`, …). Nothing reaches ClickHouse until this passes.
3. **Evaluate.** The agent opens a dedicated pooled connection through the same server-mode proxy the session uses and runs one ordinary signed query, `SELECT <columns> FROM VALUES('<structure>', <rows>)`, with `max_execution_time`, `max_result_rows`, `max_result_bytes` and `max_block_size` bounded by the configuration. There is no retry: any exception, timeout, transport error, empty result or type mismatch is a rejection.
4. **Sign and forward.** The statement body becomes `INSERT INTO <db>.<table> (<columns>) FORMAT Native`, the evaluated blocks are encoded into client Data packets at the upstream codec's negotiated revision, and the statement token is signed over exactly those bytes. From the server's point of view this is an ordinary signed streaming Native INSERT: the ingress, the intake journal, the replay executors and Arbiter are untouched.

## Configuration

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `storage_integrity.agent.inline_values.enabled` | bool | No | `false` | Enable the lane. Requires `storage_integrity.agent.enabled` and `materialize.enabled`. |
| `storage_integrity.agent.inline_values.evaluation_timeout` | duration | No | `10s` | Bounds one evaluation; must be at least `1s`. Also the helper query's `max_execution_time`. |
| `storage_integrity.agent.inline_values.max_rows` | int | No | `65536` | Must be positive. Also the helper query's `max_result_rows` and `max_block_size`. |

`storage_integrity.agent.max_payload_bytes` bounds the encoded payload and is also the helper query's `max_result_bytes`.

## Limits

The admitted column types are the storage-integrity column authority's set: `String`, `FixedString(32)`, `Bool`, `Float32/64`, `[U]Int8/16/32/64`, `Date`, `DateTime`, `DateTime(<tz>)`, `DateTime64(P)` and `DateTime64(P, <tz>)`. `NULL`, `Nullable`, arrays, tuples, `UUID`, `Decimal` and `Date32` are out of scope. So are `INSERT ... SELECT` and `WITH`, `INSERT ... VALUES ... SETTINGS`, query parameters, `async_insert`, multi-statement input, `FORMAT Values` with inline data after the format name, and deduplication of client retries. The original expression text is not carried in the signed record; it is logged at debug level with the statement id only.

## Errors and metrics

Every refusal before signing reaches the client as an exception whose message starts with `storage_integrity inline VALUES: ` and names the refused token or function, the parse problem, the server's own evaluation message, the type mismatch, or the row or byte limit. Nothing is signed on a rejection and no `client_seq` is consumed, so a retry is a fresh statement with a fresh evaluation. The agent exports `clickhouse_proxy_agent_inline_values_total{result="synthesized"|"evaluation_failed"|"closure_refused"}`.

## Rollout

Upgrade order is free: the feature is agent-only and default-off, and it introduces no wire, contract or server change. Upgrade the agent, confirm `materialize.enabled`, then set `inline_values.enabled`. Rollback is turning the flag off.

Design: [docs/superpowers/specs/2026-09-23-signed-inline-values-design.md](superpowers/specs/2026-09-23-signed-inline-values-design.md).
```

- [ ] **Step 3: Correct the CLAUDE.md sentence.** The exact current sentence (`grep -n 'send no .ClientData. packet at all' CLAUDE.md`, inside the "signed INSERT lane's FORMAT allowlist" bullet) is:

```
Inline `VALUES (...)` and `INSERT ... SELECT` send no `ClientData` packet at all and can never be signed.
```

  Replace it with:

```
A 25.x client parses inline `VALUES (...)` itself and streams the rows, so it is already on this path; a 26.3+ client and the pinned clickhouse-go `Exec` send the full statement text and only the empty external-tables marker, while `INSERT ... SELECT` sends no rows on any version. The agent-only, default-off `storage_integrity.agent.inline_values` lane signs the 26.x inline shape by evaluating the rows once through the tenant's ClickHouse and rewriting the statement to `FORMAT Native` (see [docs/agent-inline-values.md](docs/agent-inline-values.md)); `INSERT ... SELECT` still can never be signed.
```

- [ ] **Step 4: Write the issue-153 comment text.** Create `docs/issue-153-tier2-measurement.md`:

```markdown
# Issue #153 Tier 2 — comment text

Paste the block below as a comment on <https://github.com/housegate/housegate/issues/153>.

---

**Tier 2 (signed inline `INSERT ... VALUES`) is in progress.** The premise in the issue body — that an inline `INSERT ... VALUES` sends no `ClientData` packet — is version-dependent. Measured on 2026-09-22 with a raw TCP capture between a real server and a real client of the same version, `--compression 0`, table `t (x Int64)`:

| Client | Mode | Query text as sent | Client Data packets after the Query |
|---|---|---|---|
| 25.8 (rev 54479) | `--query` and interactive REPL | `INSERT INTO t VALUES ` (truncated after `VALUES`) | empty marker, one block with the rows, empty terminator |
| 26.3 (rev 54484) | `--query` and interactive REPL | the full statement including the rows | one empty marker only |
| 26.7.1 (rev 54487) | `--query` | the full statement including the rows | one empty marker only |
| 26.8.1 (rev 54488) | `--query` | the full statement including the rows | one empty marker only |
| any | `FORMAT Values` with rows on stdin | `INSERT INTO t FORMAT Values` | empty marker, one block with the rows, empty terminator |

The pinned clickhouse-go fork's `Exec` produces the 26.x shape. The change sits between revisions 54479 and 54484, where `BlockInfo` also gained a third field, so a synthesized block must follow the negotiated revision. `system.query_log` records `INSERT INTO t VALUES ` for both shapes because the server strips inline data when logging, so query logs cannot distinguish them — only a wire capture can.

Consequences: a 25.8 client is already on the ordinary streaming path and needs no work; production runs 26.3, so Tier 2 targets the 26.x shape only. The design is [docs/superpowers/specs/2026-09-23-signed-inline-values-design.md](https://github.com/housegate/housegate/blob/main/docs/superpowers/specs/2026-09-23-signed-inline-values-design.md): materialize, lexically close, evaluate the rows once through the tenant's own ClickHouse, encode Native blocks, rewrite the statement to `FORMAT Native` and forward it through a new relay lane. The feature is agent-only and default-off (`storage_integrity.agent.inline_values.enabled`); the server ingress, the intake journal, the replay executors and Arbiter are unchanged. Operator documentation: [docs/agent-inline-values.md](https://github.com/housegate/housegate/blob/main/docs/agent-inline-values.md).
```

- [ ] **Step 5: Check the edits landed and the conventions hold.** Run `grep -rn 'Signed INSERT currently requires\|send no .ClientData. packet at all' README.md CLAUDE.md` — expected: no matches. Run `awk 'length > 400 && $0 !~ /^\|/ {print FILENAME":"FNR}' docs/agent-inline-values.md docs/issue-153-tier2-measurement.md` to confirm the long paragraphs are single lines rather than hard-wrapped, and open both files once to confirm the tables render.

- [ ] **Step 6: Commit.**

```bash
git add README.md CLAUDE.md docs/agent-inline-values.md docs/issue-153-tier2-measurement.md
git commit -m "docs: describe the signed inline VALUES lane

Adds the agent page with the four-step flow, the configuration block, the error
prefix and the limits; corrects the README and CLAUDE.md claims that inline
VALUES never sends a ClientData packet; and stores the comment text for issue
#153 with the measured wire table.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Deferred (recorded, not placeholders)

- **The 25.x truncated shape.** A 25.8 client sends `INSERT INTO t VALUES ` and streams its rows; admitting it is a classifier change in `InsertPayloadEncoding` plus `queryMayStreamClientData`, deliberately not part of this plan (spec D1). `ParseInlineValuesInsert` returns `ErrNotInlineValues` for it and the existing fallthrough stays.
- **Server-state function names.** `IsServerStateFunctionName` starts with the list in spec D3; additions are policy changes recorded in the spec.
- **Interactive multi-statement input, `INSERT ... VALUES ... SETTINGS`, query parameters, `FORMAT Values` with inline data, `async_insert`, `NULL`/`Nullable`, arrays, tuples, `UUID`, `Decimal`, `Date32`.** Refused by the parser, the closure gate, or the column authority; none is planned.
- **Helper-connection pooling limits.** The evaluator pool is bounded by a fixed per-process cap (Task 4); tuning knobs are added only if production shows contention.

## Subsequent plans

| Plan | Scope | Written when |
|---|---|---|
| Tier 3 Wave 1a-3b' | arbiter-core reply mirror, client, vectors; the administrator-JWS token-age bound; housegate T2.2 export | per the TODO plan, independent of this plan |
| Tier 2 follow-ups | only if production demands them: the 25.x shape, expression audit fields in the intake journal | after this plan ships |

## Execution notes

- Single lane, one worktree `feat/153-tier2-inline-values` cut from `origin/main`; execute Tasks 1 → 7 in order; per-task PR branches `feat/153-tier2-inline-values-taskN` cut from the lane at the task's last commit; after each squash merge `git rebase --onto origin/main <old-head> feat/153-tier2-inline-values` before the next task starts.
- Model hints for a subagent-driven run: Tasks 1, 3 and 7 are complete-code transcription plus tests (a mid-tier model suffices); Tasks 2, 4, 5 and 6 need the most capable model (byte-level encoding, a new relay lane, a live-protocol helper connection, docker tests).
- The docker integration target is tagged `manual`; run it explicitly before opening Task 6's PR: `bazel test //pkg/integration:integration_test --test_filter='TestInlineValues' --test_output=errors`.

## Execution amendments (2026-09-23)

Execution used one serial URWT lane for Tasks 1 through 7 and one independently reviewed ready PR per task, with the lane rebased onto each squash merge before the next task. PRs [#196](https://github.com/housegate/housegate/pull/196) through [#203](https://github.com/housegate/housegate/pull/203) are merged. The final independent review covered the complete implementation from merged plan `6d54763` through reviewed head `14ef161`; the merged PR #203 tree was verified identical, all D1–D12 requirements passed, and the review found 0 Critical, 0 Important and one Minor comment mismatch corrected in this consolidated final wave. This record does not claim that its own final documentation PR or CI has completed, that the issue comment has been posted, or that production deployment or live production acceptance occurred. Optional AgentConnect reviews were not merge gates after exact-head independent approval and all three CI jobs passed; every late comment was nevertheless audited and Cursor usage-limit results were not counted as successful reviews. The controller accepted the cost that a late substantive automated finding would require another correction and PR before final acceptance; that cost occurred when PR #201's late payer/driver finding required fix PR #202. AgentConnect's late PR #203 documentation/evidence/YAML/link audit reported no blocking finding, its formal submission was canceled rather than recorded as a review, and every substantive late finding through PR #203 is resolved. The original checkout was preserved, the external URWT remains retained, and the implementation used the Codex attribution trailer instead of copying the plan's Claude trailer.

- **Task 1 — shared policy, parser, and lexical closure ([PR #196](https://github.com/housegate/housegate/pull/196), squash `90b1aa0`).** Implementation began reversibly from the read-only PR #195 plan snapshot while that plan was still open; the controller accepted the risk that later plan edits could require rework, then reconciled the lane with merged plan `6d54763` and confirmed identical plan text before Task 1 publication. C1 replaced the plan's token-only closure sample with an outer grammar of parenthesized row tuples separated by commas plus valid numeric token boundaries, while leaving expression evaluation to ClickHouse; the accepted cost was conservative rejection or parser rework if the lexical boundary proved too narrow. C2 preserved the exact lowercase 56-name nondeterminism list, tested every name and case variant through both consumers, and reported the intermediate compile failures honestly instead of claiming the package was green while parser symbols were still absent; the accepted cost was test-only rework. The initial independent review then found that the permissive scanner accepted a second statement or garbage before `VALUES`, and also found that the revised 25.8 comment still conflated streamed wire shape with supported admission. The controller ruled that the inline-only parser must validate the entire target/optional-column/`VALUES` prefix without changing the legacy shared scanner, at the cost of possible conservative inline-only rejection or parser rework. Fix round 1 added strict whole-prefix checks and corrected the comment, but re-review found that moving the SETTINGS check ahead of source classification wrongly claimed valid FORMAT/SELECT/WITH statements; fix round 2 restored `ErrNotInlineValues` for those forms while retaining named refusal for actual inline SETTINGS. Focused compile REDs and GREEN parser/closure tests, the three touched Bazel targets, formatting and diff checks passed; the final independent review approved with no open Critical or Important finding, exact-head Release tooling, Build and Integration all succeeded, and the late AgentConnect comment reported no blocking finding.

- **Task 2 — inverse Native encoder and codec wrapper ([PR #197](https://github.com/housegate/housegate/pull/197), squash `59eb370`).** C3 ruled that the plan's String hex was a derived layout fixture rather than an independent CLI capture, required hermetic Bazel data registration and real capture parity in Task 6, extended coverage across the supported revision boundaries, and limited this API to the uncompressed signed lane by explicitly refusing compression; the accepted cost was later API or test expansion if that scope was insufficient. The implementation kept `payloadexec.ResolveColumnProfile` as the only admitted-type authority, encoded `ClientCodeData`, an empty block name and `BlockInfo{Overflows:false, BucketNum:-1}`, checked 19 admitted type/boundary cases and 13 refusal cases, and pinned full encode/decode bytes at revisions 51802, 51902, 51903, 54058, 54453, 54454 and 54470. Encoder and codec compilation provided the intended REDs; both touched suites and `bazel test //...` passed 60/60. The independent review approved and retained only duplicate `-lm` linker warnings and Bazel test-size advice as nonblocking noise. The first exact-head CI Build then failed the unchanged `TestRedisLimiter_E2E_StaleReap`; one authorized same-head failed-job rerun succeeded and the dependent Integration job actually ran and passed, so the initially skipped Integration was not treated as acceptance and no new-scope failure was blindly rerun. Task 6 later closed C3 with raw independent CLI parity: a 60-byte two-column payload matched at negotiated revisions 54460 and 54470, and the live one-row String capture was 28 bytes and matched both the synthesized packet and this derived fixture without normalization. The historical 33-byte full capture was not recovered and is not claimed.

- **Task 3 — plan type, materialization state, configuration, and metrics ([PR #198](https://github.com/housegate/housegate/pull/198), squash `cc64b38`).** The task added `SynthesizedInsertPlan`, exact applied/noop/error materialization recording, default-off configuration with defaults `10s` and `65536`, validation, the observer seam, and the three bounded metric labels, without implementing the evaluator or relay early. The supplied Prometheus `MustCurryWith` assertion did not filter the process-global `CounterVec`, so the initial adaptation compared the full vector; independent review then found that hard-coded absolute values of one made the test fail on a second invocation or after prior observations. The controller ruled that the test must assert each collected series as its baseline plus the measured increment while preserving metric name, help, type, labels and series structure; the accepted cost was test-only rework with no production metric change. A focused `-test.count=2` run supplied the RED, the baseline-relative fix supplied the GREEN, and the full proxy target plus all five task-related Bazel targets passed. Scoped re-review approved with no open finding, all three exact-head CI jobs succeeded, and the late automated comment reported no blocking issue. No full-repository or integration run was attributed to this bounded task.

- **Task 4 — inline admission, one-shot evaluator, exact signing, and bounded ServerData support ([PR #199](https://github.com/housegate/housegate/pull/199), squash `926911e`).** C4 changed the plan's global/random helper dial into a stable current-session selected endpoint carried in `ValuesEvaluation` and included endpoint, user, database, signer/account, credential, revision, and later account context in the pool identity; the accepted cost was dialer-seam rework. C5 extended the effective deadline and cancellation across dial, hello, query and result, fenced close against active calls and late releases, bounded retained connections, and prohibited retries; the accepted cost was stricter reuse and extra dialing. C6 added nil, exact name/type/order/shape, zero-row schema, aggregate row and encoded-byte validation while keeping `ResolveColumnProfile` authoritative; the accepted cost was conservative rejection or decoder rework. C7 accepts only materialization outcomes `applied` and `noop`, with missing, error, or unknown outcomes refused before sequence allocation; the accepted cost was conservative refusal until producers aligned. C8 corrected the plan's deadlocking third channel receive, map/hello initialization, zero-row fixtures, auth API, 54470 hello tail and Bazel registration while preserving meaningful RED/GREEN intent; the accepted cost was test rework rather than treating fixture bugs as product behavior. C9 quotes every declared column as one identifier, escapes both structure-string layers, emits exactly one inline prefix for claimed-lane failures, and counts post-evaluation failures consistently; the accepted cost was quoting or metric compatibility rework. C10 limits the no-`client_seq` guarantee to OnQuery admission refusals, never rolls back a durable sequence after allocation, and binds the signed client revision to the actual upstream packet revision; the accepted cost was documented sequence gaps or revision-seam rework. ClickHouse 26 field 3 could not be decoded by the vanilla block path, so the controller authorized the smallest `pkg/chproto` captured-header normalization helper, reuse of the shared BlockInfo parser, and removal of field-3 count-based eager preallocation while leaving valid legacy wire behavior and type authority unchanged; revision, legacy, malformed-count, field-3 and touched chproto/proxy checks passed, and the evaluator proves nonchunked transport before using its bounded reader. The accepted cost of that ruling was chproto API/test rework plus an expanded chproto/proxy regression surface if the normalization seam proved wrong. A separate ruling retained the inherited limitation that ch-go string/column decoders can allocate from declared lengths before the byte cap fires: framing, row and encoded-byte limits are not an absolute hostile-upstream allocation ceiling, and the accepted cost is possible excess memory plus a future broad codec/dependency hardening task. Independent review found one P1: the fake success stream omitted ClickHouse's ordinary zero-column/zero-row end-of-data Data block, which production rejected as schema mismatch. The six-line fix skips only `block.End()`, continues to genuine EOS, retains schema-bearing zero-row and trailing-Exception checks, and passed focused RED/GREEN plus the full sistatement target; final scoped review approved. Five touched packages passed, the focused evaluator/inline race check passed at the implementation head, all three exact-head CI jobs passed, and duplicate `-lm`, cache/test-size warnings remain nonblocking rather than pristine output.

- **Task 5 — synthesized relay ownership and cancellation ([PR #200](https://github.com/housegate/housegate/pull/200), squash `5fecf1b`).** Before adding the new lane, the existing deferred post-input logic was extracted into `forwardSignedInsert` and `TestRelay_DeferredInsert` passed as a mandatory behavior-neutral gate. C11 overruled the copied plan's closing post-Query Cancel behavior only for the synthesized lane: preserve deferred behavior, forward a synthesized Cancel, drain framed EOS or Exception without success, keep one client reader, and prove reusable next-query state; the accepted cost was relay state-machine rework and regression tests. C12 added the omitted named/nonempty/missing marker, strict KeepSession versus close, payload overflow, all ownership conflicts, schema drift, multi-block ordering, cancellation and premature-terminal races, independent chunked legs, back-pressure 252, and real next-query scenarios; the accepted cost was additional tests, and “alive after a delay” was not accepted as reuse proof. The controller also permitted loopback TCP for real chunk flushes because synchronous `net.Pipe` could stall artificially, and changed the unchanged-25.x assertion from “zero strict hooks” to “no synthesized plan or payload” because the ordinary legacy path may legitimately run strict completion; the accepted cost was fixture/assertion rework with no legacy semantic change. Exact upstream-revision packets are installed and hashed before Query forwarding, the upstream sample is validated but hidden, and one marker plus one generated terminator are sent. Self-review found and fixed blocked-Cancel callback lifetime and stop/join races; an early race assertion that sampled hooks before both loops exited and a brief broad-text-replacement ownership-guard typo were retained as intermediate failed evidence, followed by focused race x10 and full proxy/chproto GREEN. Initial CI then failed the new fragmented-marker case because the fixture asserted lifecycle counts after only the upstream loop exited. Publication did not blindly rerun it: a deterministic barrier reproduced the zero-count RED, the test-only fix joined both loops while preserving exact hook assertions, focused ordinary and race runs each passed 100 repetitions, the full proxy target passed, and scoped re-review approved. The same PR was updated and merged only after fresh Release tooling, Build and actual Integration all succeeded. The 19 top-level scenario suite, four transport combinations and real NEXT checks remain codec-fixture evidence rather than live ClickHouse cancellation coverage; duplicate linker/cache/test-size warnings and expected fixture EOF/TCP reset output remain nonblocking.

- **Task 6 — root wiring, real protocol acceptance, and late account-context correction ([PR #201](https://github.com/housegate/housegate/pull/201), squash `a7e8706`; fix [PR #202](https://github.com/housegate/housegate/pull/202), squash `e67145f`).** C13 corrected comma-less tuple construction and the final filter that matched no tests; actual filters had to prove selected cases ran, at the accepted cost of fixture/filter rework. C14 required explicit FFI-backed non-skip integration, detected CLI version, enabled-prerequisite/startup-failure and default-off tests, exact selected-address dialing, lifecycle cleanup, and honest physical-landing evidence; the accepted cost was test-harness work. Runtime preparation first ran the existing native smoke against the old source-tree FFI candidate and failed on the missing `polyglot_build` symbol; repository `ffifetch` then downloaded the unchanged CI-pinned v0.11.0 library into the normal cache, its SHA-256 was recorded, and the exact smoke actually ran and passed without changing a dependency pin, overwriting the old library, or weakening TLS. The planned pre-wiring constructor RED was impossible because the old builder did not pass enabled inline options, so the controller accepted the actual real Docker inline-Exec rejection as the meaningful RED, at a test-expectation/report-only cost, while still requiring missing-prerequisite, failed-build cleanup and disabled-path coverage after wiring. The existing capture-only admission consumer could not prove rows landed because ingress intentionally suppresses nonempty upstream Data, so the controller authorized a test-only admission owner that decodes the admitted signed bytes, writes those rows to the exact test target, and then records admission; the accepted cost was fixture/adapter rework, and the resulting two-row then four-row read-back proves only this test ownership seam, not production ACK2, `hg_unsafe`, candidate parts or SourcePreparer. The first wired integration reached successful admission but incorrectly compared the existing admission digest text form `sha256:<hex>` with the JWS/replay `0x<hex>` form; the fixture comparison was corrected without changing any raw payload byte. Final local acceptance actually ran five top-level Docker tests with zero skips on server 25.8.28.1, CLI 26.8.1.368 and cached FFI v0.11.0; signatures, replay roots, exact rows, closure refusal, a 60-byte inline/CLI packet match at revisions 54460/54470, and a real 28-byte String capture passed without normalization. `bazel build //...` passed 143 targets and `bazel test //...` passed 60/60 targets, with 18 fresh and 42 cached; PR #201's exact-head three CI jobs also passed, using CLI 26.10.1.448 and FFI v0.11.0, although its errors-only output proves three configured targets rather than the five local case names or skip count. The truncated-shape test is exact Go-driver SQL rather than a real 25.x CLI; Task 5 separately pins the raw old wire path. A late AgentConnect comment then correctly found that helper SELECTs omitted configured `SQL_x_payer` and `SQL_sentio_driver`, so authorization, ordinary usage billing attribution, or driver bypass could differ from the original statement. PR #202 carried immutable Owner/Driver through build, statement options and evaluation, emitted the existing Custom settings while retaining the operator JWS, and added Owner/Driver to the pool key. The first fix-round root fixture closed its `net.Pipe` immediately after EOS and made deadline clearing fail; keeping the fake pooled connection open until teardown produced the meaningful owner/driver/pool RED, and no product behavior was changed for that fixture defect. Corrected REDs covered owner, driver, combined, default, disabled and cross-context reuse; GREEN covered decoded Native helper settings, SQL JWS validation, existing auth-validator behavior, three fresh affected targets and focused race x10. The helper remains table-free and therefore intentionally creates no `indexing_usage` INSERT report; that is distinct from ordinary `usage`, whose payer/driver context the fix preserves, and neither PR proves an external production charge. PR #202 passed all three exact-head CI jobs and late automated review found no remaining blocker. During this late fix, exactly the four Task 7 document paths were hash-recorded, path-scoped stashed, restored byte-for-byte after merge/rebase, and the temporary stash alone was dropped; no other dirty or stash state was touched. Expected CLI FORMAT Values materializer fail-open WARN, root external-rewriter fallback WARN, host `netstat`, duplicate `-lm`, test-size and race-cache noise remain disclosed rather than treated as failures.

- **Task 7 — operator and issue documentation ([PR #203](https://github.com/housegate/housegate/pull/203), squash `abf8a63`; consolidated final review correction and amendments).** C15 rejected the copied claim that 25.x was already on the signed path: the docs now separate the historical fact that 25.x streams row blocks from the unchanged unsupported truncated-VALUES admission, retain supported `FORMAT Values` stdin, and describe the 26.3+ full-query lane; the accepted cost was documentation correction without scope expansion. C16 requires all seven tasks, final independent whole-change review, accurate issue #153 measurement/status delivery, and a separate execution-amendments documentation PR; the accepted cost of an incomplete closure is a documentation-only follow-up. Issue #153 covers Tier 3 as well as this completed Tier 2 slice and therefore remains OPEN after every final review and publication step; only its stored Tier 2 measurement/completion comment waits for amendments publication. README, CLAUDE.md, `docs/agent-inline-values.md`, and `docs/issue-153-tier2-measurement.md` make the feature agent-only/default-off, include a usable materializer/SI-agent/NetworkState configuration, document stage-specific prefix and sequence behavior, preserve the selected endpoint plus payer/driver context, quote columns correctly, name the current type authority and three metrics, and distinguish implemented/tested repository behavior from production deployment or live billing/ACK2 acceptance. Historical 25.8/26.x measurements are attributed separately from Task 6's current server 25.8.28.1, CLI 26.8.1.368, FFI v0.11.0, 54460/54470, 60-byte and 28-byte results; the unrecovered historical 33-byte packet is not restated as current evidence. YAML parsing, stale-claim scans, one-paragraph checks, diff checks and GitHub GFM rendering passed; the initial verifier expected literal `<table>` and was corrected to accept GitHub's `<table role="table">`, a verifier defect rather than a document defect. Independent review found one Minor because “configured nondeterministic functions” implied an absent function-list option; the one-word `configured` to `supported` fix passed scoped diff verification and re-review approved with no open finding. During PR #202 publication, these exact four documents were the only path-scoped stash contents and were restored with all four SHA-256 values unchanged before Task 7 committed. PR #203 changed exactly those four files, all three exact-head CI jobs succeeded, and squash tree `0beaa220f7e82120e4b470b75835a3388ec428af` exactly equals reviewed head `14ef161`. The final independent whole-change review found all D1–D12 requirements satisfied, 0 Critical and 0 Important findings, and one Minor: `pkg/storageintegrity/sql.go` incorrectly said the 26.3+ and Go full-query shape sends no ClientData at all. This consolidated wave corrects the comment to one empty external-tables marker with no row-bearing ClientData and appends these amendments; because both changes are comments/documentation only, verification is limited to the scoped source diff, exact unchanged plan prefix, Markdown structure/rendering and whitespace checks, with no Go/Bazel, Docker, historical capture, production billing, deployment or live acceptance rerun.
