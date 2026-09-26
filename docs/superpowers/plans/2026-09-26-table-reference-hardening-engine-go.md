# Table-Reference Hardening — Plan A: rewriter-proto and rewriter-go

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the native engine (rewriter-go) implement the table-reference policy of the spec's §5 and author the shared corpus cases that pin it, so that the C++ engine (Plan B) and housegate (Plan C) can build on a released `rewriter-proto` field and a released rewriter-go v0.14.0.

**Architecture:** One pre-handler pass (`handlers.PreflightTableReferences`) refuses identifier parameters in table positions and protected database names before any handler runs; the SELECT walker learns to rewrite and report IN operands; INSERT … SELECT and CTAS bodies go through the same SELECT pipeline the view handler already uses; table functions, table engines and string-form lookups are decided by closed lists; the unmodelled-class pass-through becomes a refusal. Every behaviour lands as a shared corpus case in `internal/harness/testdata/storage_integrity_cases.json`.

**Tech Stack:** Go 1.25, Polyglot FFI (`POLYGLOT_SQL_FFI_PATH`), protobuf via `buf`, the rewriter-go harness (`internal/harness`), `go test`.

**Spec:** [docs/superpowers/specs/2026-09-26-table-reference-hardening-design.md](../specs/2026-09-26-table-reference-hardening-design.md) (in the housegate repository). Read §2, §4, §5, §6, §7 and §10.1 before starting.

**Repositories and working copies:**
- rewriter-proto: `/Users/uranuswch/Dev/housegate/rewriter-proto` (local `main` is 3 behind; `git fetch origin && git checkout -b feat/protected-databases origin/main`).
- rewriter-go: `/Users/uranuswch/Dev/housegate/rewriter-go` (local `main` is 5 behind; `git fetch origin && git checkout -b feat/table-reference-hardening origin/main`, then `git submodule update --init third_party/polyglot-src` so the `replace` in `go.mod` resolves).
- The FFI library does not change in this plan. Use the released v0.13.0 asset for every engine-backed test: `export POLYGLOT_SQL_FFI_PATH=$HOME/Library/Caches/housegate/rewriter-ffi/v0.13.0/libpolyglot_sql_ffi.dylib` (or `make ffi` to build it from the submodule).
- Housegate keeps its measurement probe in `.superpowers/sdd/2026-09-26-table-reference-hardening/measurements/` (git-ignored); `README.md` there explains how to re-run it against a rewriter-go checkout when a task needs to see current engine output.

## Global Constraints

- The corpus file `internal/harness/testdata/storage_integrity_cases.json` must stay byte-identical to rewriter-grpc's copy: this plan authors it; Plan B copies it verbatim and records the same SHA-256 in both PRs (spec §10.1, rewriter-go `internal/harness/AGENTS.md`).
- Every success case pins exact SQL after `NormalizeSIIdentifierQuotes`; every reject case has `reject: true`, `want_code != "Success"`, a non-empty `want_message_contains`, and no SQL pin (corpus rules R1–R8 in `sicorpus_test.go`).
- Message texts are part of the cross-engine contract and must be exactly: `query parameters are not supported in a database or table position`; `protected database <name> is not addressable`; `table function <name> is not accepted`; `table function <name> is not recognised`; `table engine <name> is not accepted`; `table setting <name> is not accepted`; `statement is not supported`; `joinGet target "<arg>" does not resolve through the caller's databases` (the same shape for `dictGet` / `hasColumnInTable`, with the function name substituted).
- Precedence (spec §5): parameter → unmodelled class → protected database (existing SI messages first while the SI surface is active) → table function / engine / setting → string lookup → ordinary rewrite.
- No existing corpus pin changes except the ones the spec lists as flipped (`si_ordinary_in_table_allowed`, the CTE-bound `IN t` case, `si_ordinary_local_catalog_function_allowed`, `insert_select_source_untouched` in `writes_cases.json`).
- `go vet ./...` and `env SNAPSHOT_QUERY_ORDINARY=1 go test ./...` (with and without `POLYGLOT_SQL_FFI_PATH`) must pass at every commit; CI runs exactly those.
- Markdown you write (README/AGENTS notes) has no hard line-wrapping.

## Review Focus

Inputs the spec implies but no task's tests would exercise unless listed here; each line names the test added to the owning task.

1. **`IN` operand bound to an in-scope CTE alias** (`WITH c AS (SELECT 1 AS a) SELECT * FROM db1.o WHERE a IN c`): must stay unrewritten and unreported, not be resolved as `db1.c`. Test in Task 5 (`si_tr_in_cte_alias_untouched`).
2. **A parameter in column position** (`SELECT {c:Identifier} FROM db1.o`, `WHERE a = {v:UInt64}`): must succeed; a position-blind check would refuse it. Tests in Task 3.
3. **A protected name that is also a legitimate column or alias** (`SELECT phys FROM db1.o`, `SELECT a AS hg_safe FROM db1.o`): the engine reasons on positions, so both succeed. Test in Task 4.
4. **The physical database used as the logical context** (`upstream_logical_database_in_context = "phys"` with an unqualified table): must be refused as protected, not resolved. Test in Task 4.
5. **Deeply nested sources** (`INSERT INTO db1.o SELECT * FROM (SELECT * FROM db1.o WHERE a IN db1.o)`): nested IN operands inside an embedded body must be rewritten too. Test in Task 6.

---

### Task 1: rewriter-proto — add `protected_databases`

**Files:**
- Modify: `proto/rewriter.proto` (message `RewriteTableDynamicArgs`, after `remote_upstreams = 8;`; comment on `known_physical_databases`; comment on `RewriteCode.UnsupportedStatement`; comment on `RewriteSQLResponse.original_accessed_tables`)
- Regenerate: `gen/pb/rewriter.pb.go`, `gen/pb/rewriter_grpc.pb.go`
- Test: `gen/pb/protected_databases_test.go`

**Interfaces:**
- Produces: `pb.RewriteTableDynamicArgs.ProtectedDatabases []string` (`GetProtectedDatabases()`), tag 9.

- [ ] **Step 1: Write the failing test**

```go
// gen/pb/protected_databases_test.go
package pb

import "testing"

func TestProtectedDatabasesFieldRoundTrips(t *testing.T) {
	in := &RewriteTableDynamicArgs{ProtectedDatabases: []string{"phys", "hg_safe"}}
	if got := in.GetProtectedDatabases(); len(got) != 2 || got[0] != "phys" || got[1] != "hg_safe" {
		t.Fatalf("protected_databases = %v, want [phys hg_safe]", got)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd /Users/uranuswch/Dev/housegate/rewriter-proto && go test ./gen/pb/ -run TestProtectedDatabasesFieldRoundTrips`
Expected: FAIL — `in.ProtectedDatabases undefined`.

- [ ] **Step 3: Add the field and the comments**

In `proto/rewriter.proto`, inside `message RewriteTableDynamicArgs`, directly after the `remote_upstreams = 8;` field:

```proto
    // Physical databases that caller SQL may never name in any position —
    // as a qualifier, a USE / SHOW … FROM target, a table-function or
    // table-engine argument, or a string-form lookup argument — whatever the
    // storage-integrity contract says. Housegate sends its shared physical
    // database plus the protocol-owned hg_* databases on every request. Each
    // entry must be a simple identifier. A name present here and in
    // `known_physical_databases` is protected: the pass-through role does not
    // apply to it. The engines add every value of `database_map` to this set.
    repeated string protected_databases = 9;
```

Change the `known_physical_databases` comment's last sentence to end with: `A name that also appears in protected_databases is protected, not passed through.`

In the `UnsupportedStatement` comment, delete the sentence `Callers may choose to pass the SQL through to ClickHouse unchanged (it'll either run or ClickHouse will reject it).` and replace it with `Housegate refuses the statement; the code distinguishes "not modelled" from "invalid" for logs and tests.`

In the `original_accessed_tables` comment, after the SELECT bullet add:

```proto
    //   * IN / GLOBAL IN table operands, the SELECT body of INSERT … SELECT
    //     and of CREATE TABLE … AS SELECT — one entry per source table,
    //     listed after the statement's own target(s).
```

- [ ] **Step 4: Regenerate and run the test**

Run: `cd /Users/uranuswch/Dev/housegate/rewriter-proto && make tools && make proto && make lint && go test ./...`
Expected: `buf lint` clean; `TestProtectedDatabasesFieldRoundTrips` PASS. `git diff --stat` shows `proto/rewriter.proto`, `gen/pb/rewriter.pb.go` and the new test.

- [ ] **Step 5: Commit, open the PR, tag the release**

```bash
git add proto/rewriter.proto gen/pb/rewriter.pb.go gen/pb/rewriter_grpc.pb.go gen/pb/protected_databases_test.go
git commit -m "feat(proto): protected_databases on RewriteTableDynamicArgs (table-reference hardening)"
git push -u origin feat/protected-databases
gh pr create --fill
```

After the user approves and merges: `git checkout main && git pull && git tag v0.4.0 && git push origin v0.4.0`. Record the commit hash; Tasks 2, Plan B Task 1 and Plan C Task 1 pin it. Ask the user before tagging.

---

### Task 2: rewriter-go — pin the proto and define the protected namespace

**Files:**
- Modify: `go.mod` (`github.com/housegate/rewriter-proto` → `v0.4.0`), `go.sum`
- Create: `internal/nameresolve/protected.go`
- Modify: `internal/nameresolve/resolve.go:75-88` (`resolvePhysicalDatabase`)
- Test: `internal/nameresolve/protected_test.go`

**Interfaces:**
- Produces:
  - `func ProtectedDatabase(db string, a *pb.RewriteTableDynamicArgs) bool`
  - `func ProtectedDatabaseRejectMessage(db string) string` → `"protected database " + db + " is not addressable"`
  - `func ValidateProtectedDatabases(a *pb.RewriteTableDynamicArgs) error`
  - `resolvePhysicalDatabase` no longer passes a protected `known_physical_databases` entry through.

- [ ] **Step 1: Pin the proto**

Run: `cd /Users/uranuswch/Dev/housegate/rewriter-go && go get github.com/housegate/rewriter-proto@v0.4.0 && go mod tidy && go build ./...`
Expected: builds; `go.mod` shows `v0.4.0`.

- [ ] **Step 2: Write the failing tests**

```go
// internal/nameresolve/protected_test.go
package nameresolve

import (
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)

func protectedArgs() *pb.RewriteTableDynamicArgs {
	return &pb.RewriteTableDynamicArgs{
		DatabaseMap:            map[string]string{"db1": "phys"},
		KnownPhysicalDatabases: []string{"phys"},
		ProtectedDatabases:     []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"},
		Delim:                  "_",
	}
}

func TestProtectedDatabase(t *testing.T) {
	a := protectedArgs()
	for db, want := range map[string]bool{
		"phys": true, "hg_safe": true, "hg_unsafe": true, "hg_promote": true,
		"db1": false, "system": false, "": false,
	} {
		if got := ProtectedDatabase(db, a); got != want {
			t.Errorf("ProtectedDatabase(%q) = %v, want %v", db, got, want)
		}
	}
	// database_map values are protected even when protected_databases is empty.
	bare := &pb.RewriteTableDynamicArgs{DatabaseMap: map[string]string{"db1": "testnet"}}
	if !ProtectedDatabase("testnet", bare) {
		t.Fatal("a database_map value must be protected without protected_databases")
	}
}

func TestProtectedKnownPhysicalIsNotAPassThrough(t *testing.T) {
	a := protectedArgs()
	if _, ok := resolvePhysicalDatabase("phys", a); ok {
		t.Fatal("phys is protected and must not resolve as a pass-through logical")
	}
	// An unprotected known-physical entry keeps the legacy pass-through role.
	legacy := &pb.RewriteTableDynamicArgs{KnownPhysicalDatabases: []string{"shared"}}
	if got, ok := resolvePhysicalDatabase("shared", legacy); !ok || got != "shared" {
		t.Fatalf("resolvePhysicalDatabase(shared) = %q,%v; want shared,true", got, ok)
	}
}

func TestValidateProtectedDatabases(t *testing.T) {
	a := protectedArgs()
	if err := ValidateProtectedDatabases(a); err != nil {
		t.Fatalf("valid entries rejected: %v", err)
	}
	a.ProtectedDatabases = append(a.ProtectedDatabases, "hg-promote")
	if err := ValidateProtectedDatabases(a); err == nil ||
		err.Error() != `protected_databases entry "hg-promote" must be a simple identifier` {
		t.Fatalf("err = %v, want the simple-identifier message", err)
	}
}

func TestProtectedDatabaseRejectMessage(t *testing.T) {
	if got := ProtectedDatabaseRejectMessage("phys"); got != "protected database phys is not addressable" {
		t.Fatalf("message = %q", got)
	}
}
```

- [ ] **Step 3: Run them to verify they fail**

Run: `go test ./internal/nameresolve/ -run 'TestProtected|TestValidateProtected'`
Expected: FAIL — `undefined: ProtectedDatabase`.

- [ ] **Step 4: Implement**

```go
// internal/nameresolve/protected.go
package nameresolve

import (
	"fmt"

	"github.com/housegate/rewriter-proto/gen/pb"
)

// ProtectedDatabase reports whether db may never be named by caller SQL in
// any position (spec 2026-09-26 §5): every database_map value, every
// protected_databases entry, and, while the storage-integrity surface is
// active, every SI physical or reserved database. It is independent of the
// SI contract so the policy holds when storage integrity is disabled.
func ProtectedDatabase(db string, a *pb.RewriteTableDynamicArgs) bool {
	if db == "" || a == nil {
		return false
	}
	for _, physical := range a.GetDatabaseMap() {
		if physical == db {
			return true
		}
	}
	for _, protected := range a.GetProtectedDatabases() {
		if protected == db {
			return true
		}
	}
	return IsStorageIntegrityPhysicalDatabase(db, a)
}

// ProtectedDatabaseRejectMessage is the cross-engine message for a protected
// database reference (spec 2026-09-26 T3).
func ProtectedDatabaseRejectMessage(db string) string {
	return "protected database " + db + " is not addressable"
}

// ValidateProtectedDatabases mirrors the reserved_databases rule: every entry
// is a simple identifier. Checked before any handler runs.
func ValidateProtectedDatabases(a *pb.RewriteTableDynamicArgs) error {
	for _, db := range a.GetProtectedDatabases() {
		if !simpleIdentifier(db) {
			return fmt.Errorf("protected_databases entry %q must be a simple identifier", db)
		}
	}
	return nil
}
```

In `resolve.go` replace the `known_physical` loop of `resolvePhysicalDatabase` with:

```go
	for _, k := range a.GetKnownPhysicalDatabases() {
		if k != logical {
			continue
		}
		// A protected name loses the pass-through role: caller SQL may not
		// address it as a database at all (spec 2026-09-26 T3).
		for _, protected := range a.GetProtectedDatabases() {
			if protected == k {
				return "", false
			}
		}
		return logical, true
	}
```

- [ ] **Step 5: Run the tests**

Run: `go test ./internal/nameresolve/`
Expected: PASS (all, including the existing ones).

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/nameresolve/protected.go internal/nameresolve/protected_test.go internal/nameresolve/resolve.go
git commit -m "feat(nameresolve): protected database namespace from protected_databases and database_map values"
```

---

### Task 3: identifier parameters in table positions are refused (T2)

**Files:**
- Create: `internal/engine/parameters.go`
- Modify: `internal/engine/nodes.go:650-655` (`readSourceVisitor` gains `parameter`), `nodes.go:1324-1332` (`emitTableSource`), `nodes.go:1543-1570` (`walkInExpression`), `nodes.go:1572-1596` (`walkFunctionExpression`)
- Modify: `internal/engine/writes.go` (`InspectWrite` sets `WriteInfo.ParameterTarget`)
- Create: `internal/handlers/preflight.go` (`PreflightTableReferences`, parameter half; Task 4 adds the protected half)
- Modify: `native.go:180-189` (call the preflight before `RewriteWrite`)
- Modify: `internal/engine/nodes_test.go:1154-1169`, `1297-1310` (flip)
- Test: `native_tableref_test.go` (new, shared by Tasks 3–8), `internal/engine/parameters_test.go`

**Interfaces:**
- Consumes: `nameresolve.FindActive`, `engine.NodeKind`, `engine.CommandSQL`.
- Produces:
  - `const engine.IdentifierParameterMessage = "query parameters are not supported in a database or table position"`
  - `func engine.TablePositionParameter(ast AST) (bool, error)` — true when any database/table position holds an Identifier parameter.
  - `func engine.IdentifierParameterInText(sql string) bool` — token-level `{name:Identifier}` scan for opaque `command` nodes (wraps the existing `identifierParameterEnd` in `lexical.go`).
  - `WriteInfo.ParameterTarget bool`.
  - `func handlers.PreflightTableReferences(e engine.Engine, ast engine.AST, sql string, opts []*pb.RewriteOption) (*pb.RewriteSQLResponse, bool, error)` — runs only in `ModeDynamic`; returns `(resp, true, nil)` with `InvalidRewriteRequest` on a hit.

- [ ] **Step 1: Write the end-to-end failing tests**

```go
// native_tableref_test.go
package rewriter

import (
	"strings"
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)

// tablerefDynamic is the request housegate sends after Plan C: db1 maps to
// phys, phys is known-physical AND protected, hg_* are protected; si adds the
// V2 surface with db1.t Active.
func tablerefDynamic(si bool) *pb.RewriteTableDynamicArgs {
	dyn := &pb.RewriteTableDynamicArgs{
		DatabaseMap:                      map[string]string{"db1": "phys"},
		KnownPhysicalDatabases:           []string{"phys"},
		UpstreamLogicalDatabaseInContext: "db1",
		Delim:                            "_",
		ProtectedDatabases:               []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"},
	}
	if si {
		dyn.StorageIntegrity = &pb.StorageIntegrityArgs{
			Tables: map[string]*pb.StorageIntegrityArgs_Table{
				"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
			},
			ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
			ReservedRowIdColumn: "_hg_row_id",
			ContractVersion:     pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2,
			ReservedDatabases:   []string{"hg_safe", "hg_unsafe", "hg_promote"},
		}
	}
	return dyn
}

func tablerefOpts(si bool) []*pb.RewriteOption {
	return []*pb.RewriteOption{tableRewriteDynamic(tablerefDynamic(si))}
}

type tablerefCase struct {
	name     string
	sql      string
	si       bool
	wantCode pb.RewriteCode
	wantMsg  string // substring; "" = don't check
	wantSQL  string // exact; "" = don't check
	wantAcc  []string // "db.table" in response order; nil = don't check
}

func runTablerefCases(t *testing.T, cases []tablerefCase) {
	t.Helper()
	e := newEngine(t)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, err := doRewrite(e, c.sql, tablerefOpts(c.si))
			if err != nil {
				t.Fatalf("doRewrite: %v", err)
			}
			if resp.GetCode() != c.wantCode {
				t.Fatalf("code = %s (%s), want %s", resp.GetCode(), resp.GetMessage(), c.wantCode)
			}
			if c.wantMsg != "" && !strings.Contains(resp.GetMessage(), c.wantMsg) {
				t.Fatalf("message = %q, want substring %q", resp.GetMessage(), c.wantMsg)
			}
			if c.wantSQL != "" && resp.GetSqlAfterRewrite() != c.wantSQL {
				t.Fatalf("sql = %q, want %q", resp.GetSqlAfterRewrite(), c.wantSQL)
			}
			if c.wantAcc != nil {
				var got []string
				for _, a := range resp.GetOriginalAccessedTables() {
					got = append(got, a.GetOriginalDatabase()+"."+a.GetOriginalTable())
				}
				if strings.Join(got, ",") != strings.Join(c.wantAcc, ",") {
					t.Fatalf("accessed = %v, want %v", got, c.wantAcc)
				}
			}
		})
	}
}

func TestTableRef_ParametersInTablePositionsAreRefused(t *testing.T) {
	const msg = "query parameters are not supported in a database or table position"
	var cases []tablerefCase
	for _, si := range []bool{false, true} {
		for _, sql := range []string{
			"SELECT * FROM {p:Identifier}",
			"SELECT * FROM db1.{p:Identifier}",
			"SELECT * FROM {d:Identifier}.t",
			"SELECT * FROM db1.o AS a JOIN {p:Identifier} AS b USING (a)",
			"SELECT * FROM (SELECT * FROM {p:Identifier})",
			"SELECT * FROM db1.o WHERE a IN {p:Identifier}",
			"SELECT * FROM db1.o WHERE a IN db1.{p:Identifier}",
			"SELECT * FROM db1.o WHERE in(a, {p:Identifier})",
			"INSERT INTO db1.o SELECT * FROM {p:Identifier}",
			"CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM {p:Identifier}",
			"CREATE MATERIALIZED VIEW db1.mv TO db1.{p:Identifier} AS SELECT * FROM db1.o",
			"CREATE MATERIALIZED VIEW {p:Identifier} ENGINE = Memory AS SELECT * FROM db1.o",
			"DROP TABLE {p:Identifier}",
			"DROP TABLE db1.{p:Identifier}",
			"INSERT INTO db1.{p:Identifier} VALUES (1)",
			"CREATE TABLE {p:Identifier} (a UInt64) ENGINE = Memory",
			"EXISTS TABLE db1.{p:Identifier}",
			"SHOW CREATE TABLE db1.{p:Identifier}",
			"DESCRIBE TABLE db1.{p:Identifier}",
			"RENAME TABLE db1.{p:Identifier} TO db1.z",
			"SHOW TABLES FROM {d:Identifier}",
			"USE {d:Identifier}",
		} {
			cases = append(cases, tablerefCase{name: sql, sql: sql, si: si,
				wantCode: pb.RewriteCode_InvalidRewriteRequest, wantMsg: msg, wantSQL: sql})
		}
	}
	runTablerefCases(t, cases)
}

func TestTableRef_ValueAndColumnParametersStayAllowed(t *testing.T) {
	runTablerefCases(t, []tablerefCase{
		{name: "value", sql: "SELECT * FROM db1.o WHERE a = {v:UInt64}", wantCode: pb.RewriteCode_Success,
			wantSQL: `SELECT * FROM phys."db1.o" "db1.o" WHERE a = {v: UInt64}`},
		{name: "column", sql: "SELECT {c:Identifier} FROM db1.o", wantCode: pb.RewriteCode_Success},
	})
}
```

Note: the exact `wantSQL` spellings (`{v: UInt64}` with a space, `phys."db1.o" "db1.o"`) are what the v0.13.0 generator emits; keep them and adjust only if the first green run shows a different spelling for the value case.

- [ ] **Step 2: Run them to verify they fail**

Run: `POLYGLOT_SQL_FFI_PATH=$HOME/Library/Caches/housegate/rewriter-ffi/v0.13.0/libpolyglot_sql_ffi.dylib go test ./ -run 'TestTableRef_Parameters|TestTableRef_ValueAndColumn' -v 2>&1 | tail -40`
Expected: the parameter cases FAIL with `code = Success … want InvalidRewriteRequest`; the allowed cases PASS.

- [ ] **Step 3: Write the engine unit test**

```go
// internal/engine/parameters_test.go
package engine

import "testing"

func TestTablePositionParameter(t *testing.T) {
	e := newTestEngine(t)
	for sql, want := range map[string]bool{
		"SELECT * FROM {p:Identifier}":                                true,
		"SELECT * FROM db1.{p:Identifier}":                            true,
		"SELECT * FROM {d:Identifier}.t":                              true,
		"SELECT * FROM db1.o WHERE a IN {p:Identifier}":               true,
		"SELECT * FROM db1.o WHERE in(a, db1.{p:Identifier})":         true,
		"INSERT INTO db1.{p:Identifier} VALUES (1)":                   true,
		"CREATE MATERIALIZED VIEW db1.mv TO {p:Identifier} AS SELECT 1": true,
		"SELECT {c:Identifier} FROM db1.o":                            false,
		"SELECT * FROM db1.o WHERE a = {v:UInt64}":                    false,
		"SELECT * FROM db1.o WHERE a IN (1, 2)":                       false,
	} {
		ast, err := e.ParseOne(sql)
		if err != nil {
			t.Fatalf("%s: parse: %v", sql, err)
		}
		got, err := TablePositionParameter(ast)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if got != want {
			t.Errorf("TablePositionParameter(%s) = %v, want %v", sql, got, want)
		}
	}
}

func TestIdentifierParameterInText(t *testing.T) {
	for sql, want := range map[string]bool{
		"EXISTS TABLE db1.{p:Identifier}":      true,
		"RENAME TABLE {d:Identifier}.t TO db1.z": true,
		"USE {d : Identifier}":                 true,
		"EXISTS TABLE db1.t":                   false,
		"SYSTEM RELOAD CONFIG":                 false,
		"SELECT '{p:Identifier}'":              false, // inside a string literal
	} {
		if got := IdentifierParameterInText(sql); got != want {
			t.Errorf("IdentifierParameterInText(%s) = %v, want %v", sql, got, want)
		}
	}
}
```

- [ ] **Step 4: Implement the engine half**

In `nodes.go`, extend the visitor:

```go
type readSourceVisitor struct {
	table     func(expr, table map[string]any, target TableTarget)
	function  func(function map[string]any, detail namespaceRefDetail)
	inTable   func(expression map[string]any, detail namespaceRefDetail)
	namespace func(expression map[string]any, detail namespaceRefDetail)
	// parameter fires once for every database or table position that holds
	// an Identifier query parameter (spec 2026-09-26 T2). Callers that do not
	// set it keep dropping those positions, as before.
	parameter func(node map[string]any)
}
```

`emitTableSource` becomes:

```go
func emitTableSource(expr, table map[string]any, scope readSourceScope, visitor readSourceVisitor) {
	if unresolvedIdentifierNode(table["name"]) ||
		(table["schema"] != nil && unresolvedIdentifierNode(table["schema"])) {
		if visitor.parameter != nil {
			visitor.parameter(table)
		}
		return
	}
	target := decodeTableTarget(table)
	if target.Table == "" || (target.DB == "" && scope.ctes[target.Table]) {
		return
	}
	if visitor.table != nil {
		visitor.table(expr, table, target)
	}
}
```

In `walkInExpression`, before the `if detail, ok := decodeInNamespaceRefDetail(inNode); ok {` line, add:

```go
	if isField, _ := inNode["is_field"].(bool); isField {
		if exprs, _ := inNode["expressions"].([]any); len(exprs) == 1 {
			if arg, ok := exprs[0].(map[string]any); ok && isNamespaceIdentifierArg(arg) && inOperandHoldsParameter(arg) {
				if visitor.parameter != nil {
					visitor.parameter(arg)
				}
			}
		}
	}
```

In `walkFunctionExpression`, inside the `recognized && detail.ref.Source == NamespaceRefInTable` branch is too late (the decoder drops parameter operands), so add before the `if detail, recognized := …` line:

```go
	if canonical, ok := canonicalCallableInName(strings.ToLower(nameOf(function))); ok && len(args) == 2 {
		if arg, ok := args[1].(map[string]any); ok && isNamespaceIdentifierArg(arg) && inOperandHoldsParameter(arg) {
			if visitor.parameter != nil {
				visitor.parameter(arg)
			}
		}
		_ = canonical
	}
```

(`nameOf(function)` is `function["name"].(string)`; write the helper if there is none.)

Create `parameters.go`:

```go
package engine

import (
	"encoding/json"
	"fmt"
)

// IdentifierParameterMessage is the cross-engine rejection text (spec T2).
const IdentifierParameterMessage = "query parameters are not supported in a database or table position"

// inOperandHoldsParameter reports whether an IN operand identifier (column or
// dot node) has an Identifier parameter in its table or database part.
func inOperandHoldsParameter(arg map[string]any) bool {
	if col, ok := arg["column"].(map[string]any); ok {
		return unresolvedIdentifierNode(col["name"]) || unresolvedIdentifierNode(col["table"])
	}
	if dot, ok := arg["dot"].(map[string]any); ok {
		if unresolvedIdentifierNode(dot["field"]) {
			return true
		}
		if inner, ok := dot["this"].(map[string]any); ok {
			return inOperandHoldsParameter(inner)
		}
	}
	return unresolvedIdentifierNode(arg)
}

// TablePositionParameter reports whether any database or table position of
// the statement holds an Identifier query parameter: FROM / JOIN / subquery /
// CTE / UNION table nodes, IN and callable-IN operands, every write slot, the
// MV TO target, and — for opaque command nodes — any Identifier parameter in
// the command text.
func TablePositionParameter(ast AST) (bool, error) {
	kind, err := NodeKind(ast)
	if err != nil {
		return false, err
	}
	if kind == NodeCommand {
		sql, err := CommandSQL(ast)
		if err != nil {
			return false, err
		}
		return IdentifierParameterInText(sql), nil
	}
	var root any
	if err := json.Unmarshal(ast, &root); err != nil {
		return false, fmt.Errorf("engine: decode statement: %w", err)
	}
	found := false
	if err := walkStatementObjects(root, readSourceScope{}, readSourceVisitor{
		parameter: func(map[string]any) { found = true },
	}); err != nil {
		return false, err
	}
	if found {
		return true, nil
	}
	info, err := InspectWrite(ast)
	if err != nil {
		return false, err
	}
	return info.ParameterTarget, nil
}

// IdentifierParameterInText scans SQL text for a `{name:Identifier}` token
// outside string literals, quoted identifiers and comments. It reuses the
// lexical scanner's identifierParameterEnd so the accepted spelling matches
// the parser's.
func IdentifierParameterInText(sql string) bool {
	for i := 0; i < len(sql); i++ {
		switch sql[i] {
		case '\'', '`', '"':
			i = skipQuotedSpan(sql, i)
		case '-':
			if i+1 < len(sql) && sql[i+1] == '-' {
				i = skipLineComment(sql, i)
			}
		case '/':
			if i+1 < len(sql) && sql[i+1] == '*' {
				i = skipBlockCommentSpan(sql, i)
			}
		case '{':
			if startsIdentifierQueryParameter(sql[i:]) { // lexical.go:1947, string-based
				return true
			}
		}
	}
	return false
}

// skipQuotedSpan returns the index of the closing delimiter of the span that
// opens at sql[i] ('…', `…` or "…"), honouring doubled delimiters and
// backslash escapes; an unterminated span skips to the end.
func skipQuotedSpan(sql string, i int) int {
	q := sql[i]
	for j := i + 1; j < len(sql); j++ {
		switch sql[j] {
		case '\\':
			j++
		case q:
			if j+1 < len(sql) && sql[j+1] == q {
				j++
				continue
			}
			return j
		}
	}
	return len(sql)
}

func skipLineComment(sql string, i int) int {
	for ; i < len(sql) && sql[i] != '\n'; i++ {
	}
	return i
}

func skipBlockCommentSpan(sql string, i int) int {
	depth := 0
	for j := i; j+1 < len(sql); j++ {
		switch {
		case sql[j] == '/' && sql[j+1] == '*':
			depth++
			j++
		case sql[j] == '*' && sql[j+1] == '/':
			depth--
			j++
			if depth == 0 {
				return j
			}
		}
	}
	return len(sql)
}
```

`identifierParameterEnd` at `lexical.go:2989` works on raw tokens, so it is not used here; `startsIdentifierQueryParameter(value)` (`lexical.go:1947`) is the string form and accepts exactly `{name:Identifier}` with no inner whitespace, so the test row `USE {d : Identifier}` belongs on the "not detected" side (ClickHouse itself does not parse that spelling as a parameter); change that row's expectation to `false`.

In `writes.go` `InspectWrite`, where slots are built from `writeSlots(kind, body, func(role WriteRole, tbl map[string]any) {…})`, add before decoding the target:

```go
		if unresolvedIdentifierNode(tbl["name"]) || (tbl["schema"] != nil && unresolvedIdentifierNode(tbl["schema"])) {
			info.ParameterTarget = true
			return
		}
```

and add `ParameterTarget bool // a write slot or MV TO target holds an Identifier parameter (spec T2)` to `WriteInfo`.

- [ ] **Step 5: Implement the handler half and wire it**

```go
// internal/handlers/preflight.go
package handlers

import (
	"github.com/housegate/rewriter-go/internal/engine"
	"github.com/housegate/rewriter-go/internal/nameresolve"
	"github.com/housegate/rewriter-proto/gen/pb"
)

// PreflightTableReferences applies the position-independent halves of the
// table-reference policy (spec 2026-09-26 §5) before any handler runs, in
// precedence order: identifier parameters (T2), then protected databases
// (T3, Task 4). Static mode and requests without dynamic args are untouched.
func PreflightTableReferences(e engine.Engine, ast engine.AST, sql string, opts []*pb.RewriteOption) (*pb.RewriteSQLResponse, bool, error) {
	sel := nameresolve.FindActive(opts)
	if sel.Mode != nameresolve.ModeDynamic {
		return nil, false, nil
	}
	hit, err := engine.TablePositionParameter(ast)
	if err != nil {
		return nil, false, err
	}
	if hit {
		resp := newWriteResp(pb.StatementType_STATEMENT_TYPE_UNSPECIFIED)
		rejectInvalid(resp, engine.IdentifierParameterMessage)
		resp.SqlAfterRewrite = sql
		return resp, true, nil
	}
	return nil, false, nil
}
```

In `native.go` `doRewrite`, immediately before the `// Phase 2: route writes` comment:

```go
	// Table-reference policy (spec 2026-09-26 §5): identifier parameters and
	// protected databases are refused before any handler can rewrite them.
	if presp, handled, perr := handlers.PreflightTableReferences(e, ast, sql, opts); perr != nil {
		return sealStorageIntegrityHandlerError(resp, ast, sql, ec, siVersion, e, selection, perr)
	} else if handled {
		finalize(presp, ast, sql, ec, siVersion, e, selection)
		return presp, nil
	}
```

Also in `doRewrite`, right after the SI validation block (before `ParseOne`), validate the new field for every dynamic request:

```go
	if selection.Mode == nameresolve.ModeDynamic {
		if err := nameresolve.ValidateProtectedDatabases(selection.Dynamic); err != nil {
			resp.Code = pb.RewriteCode_InvalidRewriteRequest
			resp.Message = err.Error()
			return resp, nil
		}
	}
```

- [ ] **Step 6: Flip the two engine pins**

`nodes_test.go:1154-1169` (`TestCollectEmbeddedReadSources_InTableOperandsRespectCTEScopeAndOpacity`) and `1297-1310` (`TestCollectEmbeddedReadSources_QualifiedOpaqueTableTargetIsNotFabricated`): keep the SQL, and replace the `want` assertions with a check that `TablePositionParameter` reports true for the same SQL and that `collectReadSourceViews` still returns the non-parameter sources. Rename them `…ParameterOperandsAreReported` / `…QualifiedParameterTargetIsReported`.

- [ ] **Step 7: Run everything**

Run: `POLYGLOT_SQL_FFI_PATH=$HOME/Library/Caches/housegate/rewriter-ffi/v0.13.0/libpolyglot_sql_ffi.dylib go test ./internal/engine/ ./ -run 'TestTablePositionParameter|TestIdentifierParameterInText|TestTableRef_|TestCollectEmbeddedReadSources' -v 2>&1 | tail -30 && env SNAPSHOT_QUERY_ORDINARY=1 POLYGLOT_SQL_FFI_PATH=$HOME/Library/Caches/housegate/rewriter-ffi/v0.13.0/libpolyglot_sql_ffi.dylib go test ./... 2>&1 | tail -15`
Expected: all PASS. If `TestStorageIntegrityGolden` reports a corpus case that now returns the parameter message, that case is a Task 8 flip: note its name and continue (the golden suite is regenerated in Task 8, so a temporary red there is acceptable only for cases Task 8 lists).

- [ ] **Step 8: Commit**

```bash
git add internal/engine/parameters.go internal/engine/parameters_test.go internal/engine/nodes.go internal/engine/writes.go internal/engine/nodes_test.go internal/handlers/preflight.go native.go native_tableref_test.go
git commit -m "feat(engine): refuse Identifier query parameters in database and table positions (spec T2)"
```

---

### Task 4: protected databases are refused in every position (T3)

**Files:**
- Modify: `internal/handlers/preflight.go` (protected half), `internal/handlers/dblevel.go:214-245` (`dispatchUse`), `internal/nameresolve/resolve.go` (`ApplyDynamic`: protected logical context)
- Create: `internal/engine/references.go` (`CollectDatabaseReferences`)
- Test: `native_tableref_test.go`, `internal/engine/references_test.go`

**Interfaces:**
- Produces: `func engine.CollectDatabaseReferences(e Engine, ast AST, sql string) ([]string, error)` — every database name the statement addresses, in document order, deduplicated: FROM/JOIN/subquery/CTE/UNION/IN/source table qualifiers, write slots and MV TO, `CREATE/DROP DATABASE` targets, USE / SHOW … FROM targets, EXISTS / SHOW CREATE / DESCRIBE targets, RENAME/EXCHANGE sides, table-function / table-engine / dictionary-source database arguments (resolved or database-only), `INSERT INTO FUNCTION` and `CREATE … AS function(…)` arguments.

- [ ] **Step 1: Write the failing tests**

Append to `native_tableref_test.go`:

```go
func TestTableRef_ProtectedDatabasesAreRefusedEverywhere(t *testing.T) {
	var cases []tablerefCase
	for _, db := range []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"} {
		msg := "protected database " + db + " is not addressable"
		for _, shape := range []string{
			"SELECT * FROM %s.`db2.x`",
			"SELECT * FROM db1.o AS a JOIN %s.`db2.x` AS b USING (a)",
			"SELECT * FROM (SELECT * FROM %s.`db2.x`)",
			"WITH c AS (SELECT * FROM %s.`db2.x`) SELECT * FROM c",
			"SELECT * FROM db1.o WHERE a IN %s.`db2.x`",
			"SELECT * FROM db1.o WHERE a IN (%s.`db2.x`)",
			"SELECT * FROM db1.o WHERE (a, b) IN %s.`db2.x`",
			"SELECT * FROM db1.o WHERE in(a, %s.`db2.x`)",
			"SELECT * FROM db1.o WHERE a GLOBAL IN %s.`db2.x`",
			"INSERT INTO db1.o SELECT * FROM %s.`db2.x`",
			"CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM %s.`db2.x`",
			"CREATE VIEW db1.v AS SELECT * FROM %s.`db2.x`",
			"CREATE MATERIALIZED VIEW db1.mv TO %s.`db2.x` AS SELECT * FROM db1.o",
			"INSERT INTO %s.`db2.x` VALUES (1)",
			"DROP TABLE %s.`db2.x`",
			"CREATE TABLE %s.`db2.x` (a UInt64) ENGINE = Memory",
			"CREATE TABLE db1.n AS %s.`db2.x`",
			"RENAME TABLE %s.`db2.x` TO db1.z",
			"EXISTS TABLE %s.`db2.x`",
			"SHOW CREATE TABLE %s.`db2.x`",
			"DESCRIBE TABLE %s.`db2.x`",
			"SHOW TABLES FROM %s",
			"USE %s",
			"CREATE DATABASE %s",
			"DROP DATABASE %s",
			"SELECT * FROM merge('%s', 'db2')",
			"SELECT * FROM remote('127.0.0.1:9000', '%s', 'db2.x')",
			"CREATE TABLE db1.n (a UInt64) ENGINE = Merge('%s', '^db2')",
			"SELECT joinGet('%s.`db2.x`', 'v', 1)",
		} {
			sql := strings.ReplaceAll(shape, "%s", db)
			// Under the active SI surface the hg_* names keep their existing
			// SI messages; the code is still a rejection.
			for _, si := range []bool{false, true} {
				want := msg
				if si && db != "phys" {
					want = "storage-integrity"
				}
				cases = append(cases, tablerefCase{name: sql, sql: sql, si: si, wantMsg: want,
					wantCode: rejectCodeFor(db, si)})
			}
		}
	}
	runTablerefCases(t, cases)
}

// rejectCodeFor: the protected rule answers InvalidRewriteRequest; an SI-owned
// hg_* rejection keeps whatever code the SI handler uses today, so only the
// non-Success property is asserted for those by comparing against the
// engine's own answer at the first green run. Start strict and relax per case.
func rejectCodeFor(db string, si bool) pb.RewriteCode {
	if si && db != "phys" {
		return pb.RewriteCode_RewriteError // most SI SELECT-side messages; write-side ones use UnsupportedStatement
	}
	return pb.RewriteCode_InvalidRewriteRequest
}

func TestTableRef_ProtectedNameAsColumnOrAliasIsAllowed(t *testing.T) {
	runTablerefCases(t, []tablerefCase{
		{name: "column named phys", sql: "SELECT phys FROM db1.o", wantCode: pb.RewriteCode_Success},
		{name: "alias named hg_safe", sql: "SELECT a AS hg_safe FROM db1.o", wantCode: pb.RewriteCode_Success},
	})
}

func TestTableRef_ProtectedLogicalContextIsRefused(t *testing.T) {
	e := newEngine(t)
	dyn := tablerefDynamic(false)
	dyn.UpstreamLogicalDatabaseInContext = "phys"
	resp, err := doRewrite(e, "SELECT * FROM o", []*pb.RewriteOption{tableRewriteDynamic(dyn)})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetCode() != pb.RewriteCode_InvalidRewriteRequest || resp.GetMessage() != "protected database phys is not addressable" {
		t.Fatalf("resp = %s %q", resp.GetCode(), resp.GetMessage())
	}
}
```

The SI-active `hg_*` rows are asserted loosely on purpose: `rejectCodeFor` starts strict; where the first green run shows a write-side `UnsupportedStatement`, change that row's expectation to the observed code (it is pre-existing behaviour the corpus already pins) rather than changing the engine.

- [ ] **Step 2: Run them to verify they fail**

Run: `POLYGLOT_SQL_FFI_PATH=$HOME/Library/Caches/housegate/rewriter-ffi/v0.13.0/libpolyglot_sql_ffi.dylib go test ./ -run 'TestTableRef_Protected' 2>&1 | grep -c 'want InvalidRewriteRequest'`
Expected: a large number (every `phys` row and every SI-off `hg_*` row fails with `Success`).

- [ ] **Step 3: Implement `CollectDatabaseReferences`**

```go
// internal/engine/references.go
package engine

import "encoding/json"

// CollectDatabaseReferences returns every database name the statement
// addresses, in document order and deduplicated, across every position the
// table-reference policy governs (spec 2026-09-26 §5). Unqualified names
// contribute nothing: the logical context is checked by the caller.
func CollectDatabaseReferences(e Engine, ast AST, sql string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	add := func(db string) {
		if db != "" && !seen[db] {
			seen[db] = true
			out = append(out, db)
		}
	}
	kind, err := NodeKind(ast)
	if err != nil {
		return nil, err
	}
	if kind == NodeCommand {
		// USE / SHOW … FROM / EXISTS / SHOW CREATE / DESCRIBE / RENAME / EXCHANGE.
		if info, err := ParseDBLevel(e, sql); err == nil {
			add(info.DB)
		}
		if t, err := ParseObjectTarget(e, sql); err == nil {
			if t.ObjType == "DATABASE" {
				add(t.Table)
			} else {
				add(t.DB)
			}
		}
		if raw, _, err := RawTableRefs(e, ast); err == nil {
			for _, tt := range raw {
				add(tt.DB)
			}
		}
		return out, nil
	}
	var root any
	if err := json.Unmarshal(ast, &root); err != nil {
		return nil, err
	}
	if err := walkStatementObjects(root, readSourceScope{}, readSourceVisitor{
		table: func(_, _ map[string]any, tt TableTarget) { add(tt.DB) },
		inTable: func(_ map[string]any, d namespaceRefDetail) { add(d.ref.Target.DB) },
		namespace: func(_ map[string]any, d namespaceRefDetail) { add(d.ref.Target.DB) },
	}); err != nil {
		return nil, err
	}
	targets, err := AllWriteTargets(e, ast)
	if err != nil {
		return nil, err
	}
	for _, tt := range targets {
		add(tt.DB)
	}
	return out, nil
}
```

`ParseDBLevel(e Engine, sql string) (DBLevelInfo, error)` (`internal/engine/dblevel.go:46`) and `ParseObjectTarget` both return an error for a command that is not their kind; that is why their errors are ignored here and only a parsed `DB` is added.

Unit test (`internal/engine/references_test.go`):

```go
package engine

import (
	"reflect"
	"testing"
)

func TestCollectDatabaseReferences(t *testing.T) {
	e := newTestEngine(t)
	for sql, want := range map[string][]string{
		"SELECT * FROM db1.o WHERE a IN phys.`x`":                {"db1", "phys"},
		"INSERT INTO db1.o SELECT * FROM hg_safe.db1__t":        {"db1", "hg_safe"},
		"CREATE TABLE db1.n (a UInt64) ENGINE = Merge('phys', 'x')": {"db1", "phys"},
		"SELECT * FROM merge('hg_promote', 'x')":                  {"hg_promote"},
		"USE phys":                                                {"phys"},
		"EXISTS TABLE phys.`x`":                                   {"phys"},
		"CREATE MATERIALIZED VIEW db1.mv TO phys.`x` AS SELECT 1": {"db1", "phys"},
		"SELECT * FROM o":                                         nil,
	} {
		ast, err := e.ParseOne(sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		got, err := CollectDatabaseReferences(e, ast, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v want %v", sql, got, want)
		}
	}
}
```

- [ ] **Step 4: Implement the protected half of the preflight**

In `preflight.go`, after the parameter check:

```go
	dbs, err := engine.CollectDatabaseReferences(e, ast, sql)
	if err != nil {
		return nil, false, err
	}
	if ctx := sel.Dynamic.GetUpstreamLogicalDatabaseInContext(); ctx != "" {
		dbs = append(dbs, ctx)
	}
	for _, db := range dbs {
		if !nameresolve.ProtectedDatabase(db, sel.Dynamic) {
			continue
		}
		if nameresolve.IsStorageIntegrityPhysicalDatabase(db, sel.Dynamic) {
			// The SI handlers own this name while the surface is active and
			// their messages are pinned by the existing corpus; let them fire.
			continue
		}
		resp := newWriteResp(pb.StatementType_STATEMENT_TYPE_UNSPECIFIED)
		recordAccessedDatabase(resp, db, sel.Dynamic)
		rejectInvalid(resp, nameresolve.ProtectedDatabaseRejectMessage(db))
		resp.SqlAfterRewrite = sql
		return resp, true, nil
	}
	return nil, false, nil
```

`IsStorageIntegrityPhysicalDatabase` is false when the surface is inactive (no `storage_integrity` block), so with storage integrity disabled the `hg_*` names take the protected message; with V2 they fall to the SI handlers, as the spec's precedence requires.

`dispatchUse` needs no change beyond this (the preflight runs first), but add a defensive line after the `IsStorageIntegrityPhysicalDatabase` check so a direct caller of `RewriteDBLevel` gets the same answer:

```go
	if nameresolve.ProtectedDatabase(origin, dyn) {
		recordAccessedDatabase(resp, origin, dyn)
		rejectDBInvalid(resp, nameresolve.ProtectedDatabaseRejectMessage(origin))
		return resp, true, nil
	}
```

- [ ] **Step 5: Run the tests, then the whole suite**

Run: `POLYGLOT_SQL_FFI_PATH=$HOME/Library/Caches/housegate/rewriter-ffi/v0.13.0/libpolyglot_sql_ffi.dylib go test ./internal/engine/ ./ -run 'TestCollectDatabaseReferences|TestTableRef_' -v 2>&1 | tail -40`
Expected: PASS. Then `env SNAPSHOT_QUERY_ORDINARY=1 POLYGLOT_SQL_FFI_PATH=… go test ./... 2>&1 | tail`: PASS except corpus cases listed for Task 8.

- [ ] **Step 6: Commit**

```bash
git add internal/engine/references.go internal/engine/references_test.go internal/handlers/preflight.go internal/handlers/dblevel.go native_tableref_test.go
git commit -m "feat(engine): refuse protected databases in every table position (spec T3)"
```

---

### Task 5: IN operands are rewritten and reported (T4, first half)

**Files:**
- Modify: `internal/engine/nodes.go` (`CollectSelectTables`, `RewriteSelectTables`, `walkInExpression`, `walkFunctionExpression`; new `applyInOperandDecision`)
- Modify: `internal/handlers/storage_integrity_policy.go` (IN-table refs no longer refused when they resolve to a mapped or Active table)
- Test: `native_tableref_test.go`, `internal/engine/nodes_test.go`

**Interfaces:**
- Produces: `CollectSelectTables` includes IN operands (infix, parenthesised, tuple, callable `in`/`notIn`/`globalIn`/`globalNotIn`/`nullIn`/`notNullIn`, `GLOBAL IN`) as `TableTarget`s; `RewriteSelectTables` applies the same `TableDecision` to them.

- [ ] **Step 1: The operand and subquery node shapes (measured 2026-09-26 on the pinned polyglot)**

Polyglot renders the four forms as follows (`…` elides fields); no probe is needed:

```text
a IN db1.o          → {"in": {"this": {"column": …}, "expressions": [{"column": {"name": {"name": "o"}, "table": {"name": "db1"}}}], "is_field": true}}
a IN (db1.o)        → the same without "is_field"
a IN (SELECT …)     → {"in": {"this": {"column": …}, "query": {"select": …}}}      (bare statement, no "subquery" wrapper, no "expressions")
in(a, db1.o)        → {"function": {"name": "in", "args": [{"column": …}, {"column": {"name": {"name": "o"}, "table": {"name": "db1"}}}]}}
```

So an `ActionRename` replaces the `column` node in place, and an `ActionSubquery` on the infix / parenthesised form deletes `expressions` and `is_field` and sets `query` to the decision's `{"select": …}` body. The callable form has no documented subquery spelling: install `{"subquery": {"this": body}}` as `args[1]` and pin the generated SQL with the `in(a, db1.t)` SI row below; if the generator cannot render it, refuse `in()`-callable operands that resolve to an Active table with `RewriteError` and the message `storage-integrity table db1.t is not directly addressable through IN table target` (today's text), and pin that instead.

- [ ] **Step 2: Write the failing tests**

Append to `native_tableref_test.go`:

```go
func TestTableRef_InOperandsAreRewrittenAndReported(t *testing.T) {
	runTablerefCases(t, []tablerefCase{
		{name: "infix", sql: "SELECT * FROM db1.o WHERE a IN db1.p", wantCode: pb.RewriteCode_Success,
			wantSQL: `SELECT * FROM phys."db1.o" "db1.o" WHERE a IN phys."db1.p"`, wantAcc: []string{"db1.o", "db1.p"}},
		{name: "paren", sql: "SELECT * FROM db1.o WHERE a IN (db1.p)", wantCode: pb.RewriteCode_Success,
			wantSQL: `SELECT * FROM phys."db1.o" "db1.o" WHERE a IN (phys."db1.p")`, wantAcc: []string{"db1.o", "db1.p"}},
		{name: "tuple", sql: "SELECT * FROM db1.o WHERE (a, b) IN db1.p", wantCode: pb.RewriteCode_Success,
			wantSQL: `SELECT * FROM phys."db1.o" "db1.o" WHERE (a, b) IN phys."db1.p"`, wantAcc: []string{"db1.o", "db1.p"}},
		{name: "callable", sql: "SELECT * FROM db1.o WHERE in(a, db1.p)", wantCode: pb.RewriteCode_Success,
			wantSQL: `SELECT * FROM phys."db1.o" "db1.o" WHERE in(a, phys."db1.p")`, wantAcc: []string{"db1.o", "db1.p"}},
		{name: "global", sql: "SELECT * FROM db1.o WHERE a GLOBAL IN db1.p", wantCode: pb.RewriteCode_Success,
			wantSQL: `SELECT * FROM phys."db1.o" "db1.o" WHERE a GLOBAL IN phys."db1.p"`, wantAcc: []string{"db1.o", "db1.p"}},
		{name: "unqualified", sql: "SELECT * FROM db1.o WHERE a IN p", wantCode: pb.RewriteCode_Success,
			wantSQL: `SELECT * FROM phys."db1.o" "db1.o" WHERE a IN phys."db1.p"`, wantAcc: []string{"db1.o", "db1.p"}},
		{name: "own table, SI active", sql: "SELECT * FROM db1.o WHERE a IN db1.o", si: true, wantCode: pb.RewriteCode_Success,
			wantSQL: `SELECT * FROM phys."db1.o" "db1.o" WHERE a IN phys."db1.o"`, wantAcc: []string{"db1.o"}},
		{name: "active table derived read", sql: "SELECT * FROM db1.o WHERE a IN db1.t", si: true, wantCode: pb.RewriteCode_Success,
			wantSQL: `SELECT * FROM phys."db1.o" "db1.o" WHERE a IN (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t)`, wantAcc: []string{"db1.o", "db1.t"}},
		{name: "callable active table derived read", sql: "SELECT * FROM db1.o WHERE in(a, db1.t)", si: true, wantCode: pb.RewriteCode_Success,
			wantAcc: []string{"db1.o", "db1.t"}},
		{name: "cte alias untouched", sql: "WITH c AS (SELECT 1 AS a) SELECT * FROM db1.o WHERE a IN c", wantCode: pb.RewriteCode_Success,
			wantSQL: `WITH c AS (SELECT 1 AS a) SELECT * FROM phys."db1.o" "db1.o" WHERE a IN c`, wantAcc: []string{"db1.o"}},
		{name: "system stays", sql: "SELECT * FROM db1.o WHERE a IN system.tables", wantCode: pb.RewriteCode_Success,
			wantSQL: `SELECT * FROM phys."db1.o" "db1.o" WHERE a IN system.tables`, wantAcc: []string{"db1.o", "system.tables"}},
	})
}
```

The `wantSQL` strings follow the v0.13.0 generator's spelling for FROM rewrites (`phys."db1.o" "db1.o"`); an IN operand takes no back-alias. If the generator spells the derived-read case differently (for example wraps the subquery once more), take the spelling the engine produces on the first green run as long as ClickHouse accepts it (Plan C's integration test proves that), and pin that.

- [ ] **Step 3: Run them to verify they fail**

Run: `POLYGLOT_SQL_FFI_PATH=… go test ./ -run TestTableRef_InOperands -v 2>&1 | tail -30`
Expected: FAIL — operands unrewritten and `accessed` missing `db1.p`.

- [ ] **Step 4: Implement**

In `nodes.go`:

1. Give `CollectSelectTables` and `RewriteSelectTables` an `inTable` callback. In `CollectSelectTables`:

```go
	if err := walkStatementObjects(root, readSourceScope{}, readSourceVisitor{
		table:   func(_, _ map[string]any, tt TableTarget) { out = append(out, tt) },
		inTable: func(_ map[string]any, d namespaceRefDetail) {
			if tt := d.ref.Target; tt.Table != "" && d.tableOrigin == namespaceValueIdentifier {
				out = append(out, tt)
			}
		},
	}); err != nil {
```

In `RewriteSelectTables`:

```go
		inTable: func(container map[string]any, d namespaceRefDetail) {
			tt := d.ref.Target
			if tt.Table == "" || d.tableOrigin != namespaceValueIdentifier {
				return
			}
			applyInOperandDecision(container, tt, decide(tt))
		},
```

2. Add `applyInOperandDecision`:

```go
// applyInOperandDecision rewrites the table operand of an IN expression. The
// container is either the `in` node (infix form: operand at
// expressions[0]) or the callable-IN `function` node (operand at args[1]).
// ActionRename installs a qualified column node; ActionSubquery replaces the
// operand with the derived-table body the FROM path uses, so an Active SI
// table is read through hg_safe / hg_unsafe here too (spec 2026-09-26 T4).
func applyInOperandDecision(container map[string]any, tt TableTarget, d TableDecision) {
	setOperand := func(node any) {
		if exprs, ok := container["expressions"].([]any); ok && len(exprs) == 1 {
			container["expressions"] = []any{node}
			return
		}
		if args, ok := container["args"].([]any); ok && len(args) == 2 {
			container["args"] = []any{args[0], node}
		}
	}
	switch d.Action {
	case ActionRename:
		col := map[string]any{"name": ident(d.NewTable)}
		if d.NewDB != "" {
			col["table"] = ident(d.NewDB)
		}
		setOperand(map[string]any{"column": col})
	case ActionSubquery:
		if len(d.Subquery) == 0 {
			return
		}
		var body any
		if err := json.Unmarshal(d.Subquery, &body); err != nil {
			return
		}
		if _, infix := container["expressions"]; infix {
			// Measured shape: `a IN (SELECT …)` is {"in": {"this": …, "query": {"select": …}}}.
			delete(container, "expressions")
			delete(container, "is_field")
			container["query"] = body
			return
		}
		// Callable form: no measured spelling; see Step 1.
		setOperand(map[string]any{"subquery": map[string]any{"this": body}})
	default:
		// ActionSkip / ActionRemote: leave the operand as written. Remote
		// operands are rejected by the SELECT handler's remote rules.
	}
}
```

Replace the placeholder comment with the real shape from Step 1 and adjust `setOperand` accordingly (for the infix form the `is_field` flag must be set to `false` when a subquery replaces the identifier).

3. Stop feeding IN operands to the SI *namespace* policy when they are plain identifiers: in `walkInExpression` and the callable branch of `walkFunctionExpression`, keep calling `visitor.inTable`, and call `visitor.namespace` only when `detail.tableOrigin != namespaceValueIdentifier` (a literal or expression operand cannot be rewritten and stays a namespace question). Note `isScopedCurrentDatabaseRef` (CTE-bound bare operand) stays as the guard that skips both callbacks.

4. In `handlers/storage_integrity_policy.go`, nothing changes structurally: with identifier operands no longer arriving as namespace refs, the remaining IN refs are literal/expression forms, which keep the existing fail-closed messages.

5. `select.go` `rewriteSelectCore`: `originals` now contains IN operands, so the existing SI loop (`LookupStorageIntegrityPhysical` → physical reject; `LookupStorageIntegrity` → authorization) and the `decide` callback (SI → `storageIntegrityDecision` → `ActionSubquery`; else `decideTable`) cover them with no new code. Verify that `storageIntegrityDecision` records `resp.TableRewrites["db1.t"] = "hg_safe.db1__t"` for the IN case exactly as it does for FROM.

- [ ] **Step 5: Run the tests, then the suite**

Run: `POLYGLOT_SQL_FFI_PATH=… go test ./internal/engine/ ./ -run 'TestTableRef_InOperands|TestCollectSelectTables|TestRewriteSelectTables' -v 2>&1 | tail -30 && env SNAPSHOT_QUERY_ORDINARY=1 POLYGLOT_SQL_FFI_PATH=… go test ./... 2>&1 | tail`
Expected: PASS; `si_ordinary_in_table_allowed` and the CTE-bound `IN t` corpus case now fail (Task 8 flips them). No other corpus case may change: if one does, the change is a bug in this task.

- [ ] **Step 6: Commit**

```bash
git add internal/engine/nodes.go internal/engine/nodes_test.go internal/handlers/storage_integrity_policy.go native_tableref_test.go
git commit -m "feat(engine): rewrite and report IN table operands like FROM references (spec T4)"
```

---

### Task 6: INSERT … SELECT and CTAS bodies are rewritten and reported (T4, second half)

**Files:**
- Modify: `internal/engine/writes.go` (new `ExtractInsertBody`, `SetInsertBody`, `ExtractCreateSelectBody`, `SetCreateSelectBody`)
- Modify: `internal/handlers/writes.go:516-539` (`dispatchInsert`), `:357-370` (`dispatchCreateTable`), `:206-222` (drop the SI-source preflight block)
- Modify: `internal/harness/testdata/writes_cases.json:133-142` (flip `insert_select_source_untouched`)
- Test: `native_tableref_test.go`, `internal/engine/writes_test.go`

**Interfaces:**
- Produces: `func ExtractInsertBody(ast AST) (AST, bool, error)`, `func SetInsertBody(ast, body AST) (AST, error)`, `func ExtractCreateSelectBody(ast AST) (AST, bool, error)`, `func SetCreateSelectBody(ast, body AST) (AST, error)` — same contract as `ExtractViewBody` / `SetViewBody` (`writes.go:558-600`), keyed on `insert.query` and on the CREATE TABLE key the probe below reveals.

- [ ] **Step 1: The body keys (measured 2026-09-26 on the pinned polyglot)**

```text
INSERT INTO db1.o SELECT * FROM db1.p                    → {"insert": {"table": …, "query": {"select": …}}}
CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM db1.p → {"create_table": {"name": …, "as_select": {"select": …}, "as_select_parenthesized": true, "properties": […]}}
CREATE TABLE … EMPTY AS SELECT * FROM db1.p               → no "as_select" at all: polyglot drops the body, and the generator emits the CREATE without it
INSERT … FORMAT CSV …                                     → "query" holds {"command": {"this": "FORMAT CSV"}}, not a SELECT
```

So `ExtractInsertBody` reads `insert.query` when it is a read body, and `ExtractCreateSelectBody` reads `create_table.as_select`. An `EMPTY AS SELECT` has nothing to rewrite or report: the forwarded SQL carries no source, so no cross-tenant read is possible.

- [ ] **Step 2: Write the failing tests**

```go
func TestTableRef_EmbeddedSourcesAreRewrittenAndReported(t *testing.T) {
	runTablerefCases(t, []tablerefCase{
		{name: "insert select own", sql: "INSERT INTO db1.o SELECT * FROM db1.p", wantCode: pb.RewriteCode_Success,
			wantSQL: `INSERT INTO phys."db1.o" SELECT * FROM phys."db1.p" "db1.p"`, wantAcc: []string{"db1.o", "db1.p"}},
		{name: "insert select unqualified", sql: "INSERT INTO db1.o SELECT * FROM p", wantCode: pb.RewriteCode_Success,
			wantSQL: `INSERT INTO phys."db1.o" SELECT * FROM phys."db1.p" "p"`, wantAcc: []string{"db1.o", "db1.p"}},
		{name: "insert select nested in", sql: "INSERT INTO db1.o SELECT * FROM (SELECT * FROM db1.p WHERE a IN db1.q)", wantCode: pb.RewriteCode_Success,
			wantAcc: []string{"db1.o", "db1.p", "db1.q"}},
		{name: "insert select active source", sql: "INSERT INTO db1.o SELECT * FROM db1.t", si: true, wantCode: pb.RewriteCode_Success,
			wantSQL: `INSERT INTO phys."db1.o" SELECT * FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS "db1.t"`, wantAcc: []string{"db1.o", "db1.t"}},
		{name: "insert into active target keeps signed-lane marking", sql: "INSERT INTO db1.t SELECT * FROM db1.o", si: true, wantCode: pb.RewriteCode_Success,
			wantAcc: []string{"db1.t", "db1.o"}},
		{name: "ctas own", sql: "CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM db1.p", wantCode: pb.RewriteCode_Success,
			wantSQL: `CREATE TABLE phys."db1.n" ENGINE=Memory AS (SELECT * FROM phys."db1.p" "db1.p")`, wantAcc: []string{"db1.n", "db1.p"}},
		{name: "ctas empty drops the body", sql: "CREATE TABLE db1.n ENGINE = Memory EMPTY AS SELECT * FROM db1.p", wantCode: pb.RewriteCode_Success,
			wantSQL: `CREATE TABLE phys."db1.n" ENGINE=Memory`, wantAcc: []string{"db1.n"}},
		{name: "ctas active source", sql: "CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM db1.t", si: true, wantCode: pb.RewriteCode_Success,
			wantAcc: []string{"db1.n", "db1.t"}},
		{name: "ctas into active target still refused", sql: "CREATE TABLE db1.t ENGINE = Memory AS SELECT * FROM db1.o", si: true,
			wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: "accepts writes only through the signed statement lane"},
	})
}
```

For the `insert into active target` row, also assert (in the same test body, after `runTablerefCases`, with a direct `doRewrite`) that the first accessed entry has `IsStorageIntegrity == true` and the code is `Success` — the signed lane's contract from the spec (§2, "INSERT remains an ordinary successful physical rewrite marked is_storage_integrity").

- [ ] **Step 3: Run them to verify they fail**

Run: `POLYGLOT_SQL_FFI_PATH=… go test ./ -run TestTableRef_EmbeddedSources -v 2>&1 | tail -30`
Expected: FAIL — sources unrewritten; `accessed` has only the target.

- [ ] **Step 4: Implement**

`internal/engine/writes.go`, after `SetViewBody`:

```go
// ExtractInsertBody returns the SELECT body of an INSERT … SELECT as a
// standalone statement AST (insert.query), or ok=false for VALUES / FORMAT.
func ExtractInsertBody(ast AST) (AST, bool, error) {
	kind, body, _, err := bodyOf(ast)
	if err != nil {
		return nil, false, err
	}
	if kind != NodeInsert {
		return nil, false, nil
	}
	q, ok := body["query"].(map[string]any)
	if !ok || !isReadBody(q) {
		return nil, false, nil // a FORMAT command node, not a SELECT
	}
	b, err := json.Marshal(q)
	if err != nil {
		return nil, false, fmt.Errorf("engine: encode insert body: %w", err)
	}
	return AST(b), true, nil
}

// SetInsertBody replaces insert.query with the rewritten body.
func SetInsertBody(ast AST, body AST) (AST, error) {
	kind, b, root, err := bodyOf(ast)
	if err != nil {
		return nil, err
	}
	if kind != NodeInsert {
		return nil, fmt.Errorf("engine: SetInsertBody on non-insert kind %q", kind)
	}
	var node map[string]any
	if err := json.Unmarshal(body, &node); err != nil {
		return nil, fmt.Errorf("engine: decode insert body: %w", err)
	}
	b["query"] = node
	out, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("engine: encode insert: %w", err)
	}
	return AST(out), nil
}

// isReadBody reports whether a node is a {"select"|"union"|"intersect"|"except": …} statement.
func isReadBody(n map[string]any) bool {
	for _, k := range []string{NodeSelect, NodeUnion, NodeIntersect, NodeExcept} {
		if _, ok := n[k]; ok {
			return true
		}
	}
	return false
}
```

`ExtractCreateSelectBody` / `SetCreateSelectBody` are the same two functions over `NodeCreateTable` and the key `"as_select"` (Step 1).

`internal/handlers/writes.go`:

```go
// rewriteEmbeddedBody runs the SELECT pipeline over an INSERT … SELECT or
// CREATE TABLE … AS SELECT body and merges its bookkeeping after the
// statement's own targets (spec 2026-09-26 T4). Mirrors dispatchView's body
// step; an SI source becomes the derived read the FROM path emits.
func rewriteEmbeddedBody(e engine.Engine, rewritten engine.AST, sql string, opts []*pb.RewriteOption,
	extract func(engine.AST) (engine.AST, bool, error), set func(engine.AST, engine.AST) (engine.AST, error),
	resp *pb.RewriteSQLResponse) (engine.AST, bool, error) {
	body, has, err := extract(rewritten)
	if err != nil || !has {
		return rewritten, err == nil, err
	}
	newBody, bodyResp, err := rewriteSelectCore(e, body, opts, sql)
	if err != nil {
		return nil, false, err
	}
	mergeViewBody(resp, bodyResp)
	if bodyResp.Code != pb.RewriteCode_Success {
		resp.Code, resp.Message = bodyResp.Code, bodyResp.Message
		return nil, false, nil
	}
	out, err := set(rewritten, newBody)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}
```

In `dispatchInsert`, between `applyStructuredSlots` and `engine.GenerateInsert`:

```go
	rewritten, ok, err = rewriteEmbeddedBody(e, rewritten, sql, opts, engine.ExtractInsertBody, engine.SetInsertBody, resp)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return resp, true, nil
	}
```

`dispatchInsert` does not receive `opts` today; thread `opts []*pb.RewriteOption` through from `RewriteWrite` the way `dispatchView` gets it. Do the same in `dispatchCreateTable`, replacing `finishStructured` with the slot step, the body step (`ExtractCreateSelectBody` / `SetCreateSelectBody`) and `e.Generate`.

Delete the block at `writes.go:206-222` (`if info.Kind == engine.NodeCreateTable || info.Kind == engine.NodeInsert { embeddedTables, … inspectTarget(tt, false) … }`): the body pipeline now decides sources. Keep the `CollectEmbeddedSelectSources` function in the engine (other callers use it).

Flip `writes_cases.json` `insert_select_source_untouched` → rename to `insert_select_source_rewritten`, `want_accessed` gains `{"original_database": "db", "original_table": "s", "logical_database": "", "physical_database": "db", "is_remote": false}` after the target, `want_table_rewrites` gains `"db.s": "db.s_phys"`, `want_sql` becomes `INSERT INTO db.t_phys SELECT * FROM db.s_phys AS "db.s"` (adjust to the exact static-mode spelling the first green run shows; static mode keeps the back-alias rule of `applyDecision`).

- [ ] **Step 5: Run the tests and the suite**

Run: `POLYGLOT_SQL_FFI_PATH=… go test ./ ./internal/... -run 'TestTableRef_EmbeddedSources|TestWritesGolden|TestExtract' -v 2>&1 | tail -30 && env SNAPSHOT_QUERY_ORDINARY=1 POLYGLOT_SQL_FFI_PATH=… go test ./... 2>&1 | tail`
Expected: PASS apart from the Task 8 corpus flips (`si_insert_select_*`, `si_ctas_*` SI-source rejections that become derived reads for an ordinary target: list them for Task 8).

- [ ] **Step 6: Commit**

```bash
git add internal/engine/writes.go internal/engine/writes_test.go internal/handlers/writes.go internal/harness/testdata/writes_cases.json native_tableref_test.go
git commit -m "feat(engine): rewrite and report INSERT … SELECT and CTAS sources (spec T4)"
```

---

### Task 7: table-function, table-engine and setting allowlists (T5), string lookups (T6), unmodelled classes (T7)

**Files:**
- Create: `internal/engine/allowlists.go`, `internal/engine/allowlists_test.go`
- Modify: `internal/engine/nodes.go` (`walkTableSource` function branch; `walkCreateProperties` engine/settings; `walkInsertObjects` function target; `walkCreateTableFunctionSource`)
- Create: `internal/handlers/lookups.go`
- Modify: `internal/handlers/preflight.go` (function / engine / setting / lookup steps after the protected step)
- Modify: `native.go:255-266` (pass-through → refusal; `SET` carve-out), `native.go:342-366` (`classifyCommand` recognises `SET`)
- Modify: `native_test.go:1294-1314`, `native_v2_test.go:59-74` (flip)
- Test: `native_tableref_test.go`

**Interfaces:**
- Produces:
  - `type engine.TableFunctionClass int` with `TableFunctionRefused`, `TableFunctionDataOnly`, `TableFunctionExternal`, `TableFunctionUnknown`; `func engine.ClassifyTableFunction(name string) TableFunctionClass`.
  - `func engine.TableEngineAllowed(name string, argCount int) bool`; `func engine.RefusedTableSetting(name string) bool`.
  - `func engine.CollectSourceFunctionNames(ast AST) ([]string, error)` — source-role function names (FROM/JOIN, `INSERT INTO FUNCTION`, `CREATE … AS function(…)`).
  - `func engine.CreateTableStorage(ast AST) (engineName string, argCount int, settings []string, ok bool, err error)`.
  - `func engine.StringLookupCalls(ast AST) ([]StringLookup, error)` and `func engine.RewriteStringLookups(ast AST, decide func(StringLookup) (string, bool)) (AST, error)` with `type StringLookup struct { Function string; Arg string; Literal bool }`.
  - `const engine.UnsupportedStatementMessage = "statement is not supported"`.

- [ ] **Step 1: Write the failing tests**

Append to `native_tableref_test.go`:

```go
func TestTableRef_TableFunctionsAndEnginesAreAllowlisted(t *testing.T) {
	var cases []tablerefCase
	for _, fn := range []string{
		"merge('db1', 'o')", "remote('h', 'db1', 'o')", "remoteSecure('h', 'db1', 'o')", "cluster('c', db1.o)",
		"clusterAllReplicas('c', db1.o)", "loop('db1', 'o')", "dictionary(db1.d)", "mergeTreeIndex('db1', 'o')",
		"mergeTreeProjection('db1', 'o', 'p')", "timeSeriesData('db1', 'o')", "prometheusQuery('db1', 'o', 'up')",
		"clickhouse('db1.o')", "mysql('h', 'db1', 'o', 'u', 'p')", "postgresql('h', 'db1', 'o', 'u', 'p')",
		"mongodb('h', 'db1', 'o', 'u', 'p', 'a UInt8')", "jdbc('ds', 'db1', 'o')", "odbc('ds', 'db1', 'o')",
		"executable('x.sh', 'TSV', 'a UInt8')", "fuzzQuery('SELECT 1')",
	} {
		name := fn[:strings.IndexByte(fn, '(')]
		cases = append(cases, tablerefCase{name: fn, sql: "SELECT * FROM " + fn,
			wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: "table function " + name + " is not accepted"})
	}
	cases = append(cases,
		tablerefCase{name: "unknown", sql: "SELECT * FROM frobnicate('x')", wantCode: pb.RewriteCode_UnsupportedStatement,
			wantMsg: "table function frobnicate is not recognised"},
		tablerefCase{name: "numbers", sql: "SELECT * FROM numbers(10)", wantCode: pb.RewriteCode_Success, wantAcc: []string{}},
		tablerefCase{name: "view body", sql: "SELECT * FROM view(SELECT * FROM db1.o)", wantCode: pb.RewriteCode_Success, wantAcc: []string{"db1.o"}},
		tablerefCase{name: "input", sql: "INSERT INTO db1.o SELECT * FROM input('a UInt8')", wantCode: pb.RewriteCode_Success},
		tablerefCase{name: "url unchanged (non-goal)", sql: "SELECT * FROM url('http://127.0.0.1/x', CSV)", wantCode: pb.RewriteCode_Success, wantAcc: []string{}},
		tablerefCase{name: "insert function", sql: "INSERT INTO FUNCTION remote('h', 'db1', 'o') VALUES (1)", wantCode: pb.RewriteCode_UnsupportedStatement},
	)
	for _, eng := range []string{
		"Merge('db1', '^o')", "Buffer(db1.o, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
		"Distributed(default, db1.o)", "URL('http://127.0.0.1/x', CSV)", "Dictionary(db1.d)", "KeeperMap('/x')",
		"EmbeddedRocksDB", "Kafka", "S3('http://127.0.0.1/x', CSV)", "File(CSV)",
		"ReplicatedMergeTree('/clickhouse/tables/x', 'r1')",
	} {
		name := eng
		if i := strings.IndexByte(eng, '('); i >= 0 {
			name = eng[:i]
		}
		cases = append(cases, tablerefCase{name: eng, sql: "CREATE TABLE db1.n (a UInt64) ENGINE = " + eng + " ORDER BY a",
			wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: "table engine " + name + " is not accepted"})
	}
	cases = append(cases,
		tablerefCase{name: "MergeTree", sql: "CREATE TABLE db1.n (a UInt64) ENGINE = MergeTree ORDER BY a", wantCode: pb.RewriteCode_Success},
		tablerefCase{name: "ReplicatedMergeTree bare", sql: "CREATE TABLE db1.n (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a", wantCode: pb.RewriteCode_Success},
		tablerefCase{name: "Memory", sql: "CREATE TABLE db1.n (a UInt64) ENGINE = Memory", wantCode: pb.RewriteCode_Success},
		tablerefCase{name: "storage_policy", sql: "CREATE TABLE db1.n (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 's3'",
			wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: "table setting storage_policy is not accepted"},
		tablerefCase{name: "disk", sql: "CREATE TABLE db1.n (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS disk = 'd'",
			wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: "table setting disk is not accepted"},
		tablerefCase{name: "alter modify setting disk", sql: "ALTER TABLE db1.o MODIFY SETTING disk = 'd'",
			wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: "table setting disk is not accepted"},
		tablerefCase{name: "unknown engine", sql: "CREATE TABLE db1.n (a UInt64) ENGINE = Frob", wantCode: pb.RewriteCode_UnsupportedStatement,
			wantMsg: "table engine Frob is not accepted"},
	)
	runTablerefCases(t, cases)
}

func TestTableRef_UnmodelledClassesAreRefusedWithoutSI(t *testing.T) {
	runTablerefCases(t, []tablerefCase{
		{name: "system", sql: "SYSTEM RELOAD CONFIG", wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: "statement is not supported"},
		{name: "explain", sql: "EXPLAIN SELECT * FROM db1.o", wantCode: pb.RewriteCode_UnsupportedStatement, wantMsg: "statement is not supported"},
		{name: "check", sql: "CHECK TABLE db1.o", wantCode: pb.RewriteCode_UnsupportedStatement},
		{name: "create user", sql: "CREATE USER u1", wantCode: pb.RewriteCode_UnsupportedStatement},
		{name: "create function", sql: "CREATE FUNCTION f AS x -> x + 1", wantCode: pb.RewriteCode_UnsupportedStatement},
		{name: "set passes when inactive", sql: "SET max_threads = 1", wantCode: pb.RewriteCode_Success, wantSQL: "SET max_threads = 1"},
		{name: "set refused under V2", sql: "SET max_threads = 1", si: true, wantCode: pb.RewriteCode_UnsupportedStatement},
		{name: "select 1", sql: "SELECT 1", wantCode: pb.RewriteCode_Success},
	})
}
```

The string-lookup rows are added in Step 6 after the measurement.

- [ ] **Step 2: Run them to verify they fail**

Run: `POLYGLOT_SQL_FFI_PATH=… go test ./ -run 'TestTableRef_TableFunctions|TestTableRef_Unmodelled' 2>&1 | grep -c FAIL`
Expected: non-zero (functions and engines pass today; SYSTEM returns Success).

- [ ] **Step 3: Implement the lists**

```go
// internal/engine/allowlists.go
package engine

import "strings"

// TableFunctionClass is the spec 2026-09-26 T5 classification of a
// source-role table function.
type TableFunctionClass int

const (
	TableFunctionUnknown  TableFunctionClass = iota // not on any list: refused as "not recognised"
	TableFunctionRefused                            // carries a table identity: refused as "not accepted"
	TableFunctionDataOnly                           // produces rows from its arguments only: allowed
	TableFunctionExternal                           // reads external data: non-goal, unchanged
)

var refusedTableFunctions = map[string]bool{
	"merge": true, "remote": true, "remotesecure": true, "cluster": true, "clusterallreplicas": true,
	"loop": true, "dictionary": true, "mergetreeindex": true, "mergetreeprojection": true,
	"timeseriesdata": true, "timeseriestags": true, "timeseriesmetrics": true, "timeseriesselector": true,
	"prometheusquery": true, "prometheusqueryrange": true, "clickhouse": true,
	"mysql": true, "postgresql": true, "mongodb": true, "jdbc": true, "odbc": true,
	"executable": true, "fuzzquery": true, "fuzzjson": true,
}

var dataOnlyTableFunctions = map[string]bool{
	"numbers": true, "numbers_mt": true, "generate_series": true, "generateseries": true,
	"generaterandom": true, "zeros": true, "zeros_mt": true, "null": true, "values": true,
	"format": true, "input": true, "view": true,
}

var externalTableFunctions = map[string]bool{
	"url": true, "s3": true, "gcs": true, "oss": true, "cosn": true, "file": true, "hdfs": true,
	"azureblobstorage": true, "iceberg": true, "deltalake": true, "hudi": true,
	"urlcluster": true, "s3cluster": true, "filecluster": true, "hdfscluster": true,
	"azureblobstoragecluster": true, "icebergcluster": true, "deltalakecluster": true, "hudicluster": true,
	"sqlite": true, "redis": true,
}

// ClassifyTableFunction classifies by ClickHouse's case-insensitive name.
func ClassifyTableFunction(name string) TableFunctionClass {
	lower := strings.ToLower(name)
	switch {
	case refusedTableFunctions[lower] || strings.HasPrefix(lower, "mergetree"):
		return TableFunctionRefused
	case dataOnlyTableFunctions[lower]:
		return TableFunctionDataOnly
	case externalTableFunctions[lower]:
		return TableFunctionExternal
	default:
		return TableFunctionUnknown
	}
}

var allowedTableEngines = map[string]bool{
	"mergetree": true, "replacingmergetree": true, "summingmergetree": true, "aggregatingmergetree": true,
	"collapsingmergetree": true, "versionedcollapsingmergetree": true, "graphitemergetree": true,
	"memory": true, "log": true, "tinylog": true, "stripelog": true, "null": true, "set": true, "join": true,
	"view": true, "materializedview": true, "liveview": true,
}

// TableEngineAllowed reports whether a CREATE TABLE engine is on the spec's
// list. A Replicated* MergeTree engine is allowed only without arguments.
func TableEngineAllowed(name string, argCount int) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "replicated") && allowedTableEngines[strings.TrimPrefix(lower, "replicated")] {
		return argCount == 0
	}
	return allowedTableEngines[lower]
}

var refusedTableSettings = map[string]bool{"disk": true, "storage_policy": true}

// RefusedTableSetting reports whether a CREATE / ALTER … MODIFY SETTING name
// stores data by reference.
func RefusedTableSetting(name string) bool { return refusedTableSettings[strings.ToLower(name)] }

// Cross-engine messages (spec 2026-09-26 §5).
const UnsupportedStatementMessage = "statement is not supported"

func TableFunctionRefusedMessage(name string) string   { return "table function " + name + " is not accepted" }
func TableFunctionUnknownMessage(name string) string   { return "table function " + name + " is not recognised" }
func TableEngineRefusedMessage(name string) string     { return "table engine " + name + " is not accepted" }
func TableSettingRefusedMessage(name string) string    { return "table setting " + name + " is not accepted" }
```

Unit test (`allowlists_test.go`): table-driven over `ClassifyTableFunction` (`Merge`→refused, `MERGETREEPARTS`→refused by prefix, `numbers`→data-only, `s3Cluster`→external, `frob`→unknown) and `TableEngineAllowed` (`MergeTree`→true, `ReplicatedMergeTree` with 0 args→true, with 2 args→false, `Buffer`→false, `Frob`→false) and `RefusedTableSetting`.

- [ ] **Step 4: Collect source functions, engines and settings**

In `nodes.go` add `CollectSourceFunctionNames` (a `walkStatementObjects` run with a `function` callback; today the callback fires only for *recognised* namespace functions, so change `walkTableSource`'s function branch, `walkInsertObjects`'s `function_target` branch and `walkCreateTableFunctionSource` to always call a new `visitor.sourceFunction(name string)` callback before the recognised-namespace logic) and `CreateTableStorage` (read `create_table.properties` for the `engine_property` node — the same path `decodeTableEngineNamespaceRef` reads: `property.this.anonymous.this.identifier` for the name and `anonymous.expressions` for the args — and for a `SETTINGS` property collect each setting key; for `alter_table.actions` read `MODIFY SETTING` keys through the ALTER actions the walker already visits). Write a unit test that parses the CREATE and ALTER samples from Step 1 and checks `(name, argCount, settings)`.

- [ ] **Step 5: Enforce in the preflight**

Append to `PreflightTableReferences`, after the protected step:

```go
	names, err := engine.CollectSourceFunctionNames(ast)
	if err != nil {
		return nil, false, err
	}
	for _, name := range names {
		switch engine.ClassifyTableFunction(name) {
		case engine.TableFunctionRefused:
			return refuse(sql, pb.RewriteCode_UnsupportedStatement, engine.TableFunctionRefusedMessage(name))
		case engine.TableFunctionUnknown:
			return refuse(sql, pb.RewriteCode_UnsupportedStatement, engine.TableFunctionUnknownMessage(name))
		}
	}
	if name, argc, settings, ok, err := engine.CreateTableStorage(ast); err != nil {
		return nil, false, err
	} else if ok {
		if !engine.TableEngineAllowed(name, argc) {
			return refuse(sql, pb.RewriteCode_UnsupportedStatement, engine.TableEngineRefusedMessage(name))
		}
		for _, s := range settings {
			if engine.RefusedTableSetting(s) {
				return refuse(sql, pb.RewriteCode_UnsupportedStatement, engine.TableSettingRefusedMessage(s))
			}
		}
	}
```

with the small helper

```go
func refuse(sql string, code pb.RewriteCode, msg string) (*pb.RewriteSQLResponse, bool, error) {
	resp := newWriteResp(pb.StatementType_STATEMENT_TYPE_UNSPECIFIED)
	resp.Code, resp.Message, resp.SqlAfterRewrite = code, msg, sql
	return resp, true, nil
}
```

Keep the existing `INSERT INTO FUNCTION` / `CREATE TABLE AS table_function` rejections in `writes.go` (they fire only if the function was somehow allowed, e.g. `input`); the corpus pins the preflight message for refused names.

- [ ] **Step 6: Measure the string-lookup form, then implement T6**

Measure once against ClickHouse (docker) which string form `joinGet` / `dictGet` / `hasColumnInTable` parse back to a dotted table name:

```bash
docker run -d --rm --name tr-measure -p 19000:9000 clickhouse/clickhouse-server:25.8
sleep 8
clickhouse-client --port 19000 -q "CREATE DATABASE phys; CREATE TABLE phys.\`db1.j\` (k UInt64, v String) ENGINE = Join(ANY, LEFT, k); INSERT INTO phys.\`db1.j\` VALUES (1, 'x')"
clickhouse-client --port 19000 -q "SELECT joinGet('phys.\`db1.j\`', 'v', 1)"      # form A
clickhouse-client --port 19000 -q "SELECT joinGet('phys.db1.j', 'v', 1)"          # form B
clickhouse-client --port 19000 -q "SELECT hasColumnInTable('phys', 'db1.j', 'v')" # two-argument form
docker stop tr-measure
```

Record which of A / B (or neither) returns `x` in a comment in `lookups.go`. If neither, skip the rewrite path: every qualified or resolvable lookup is refused with the unresolved message and the corpus pins that. If one works, implement:

```go
// internal/handlers/lookups.go
package handlers

// stringLookupFunctions take a table name as their first string argument
// (spec 2026-09-26 T6). Names are lower-cased for the match.
var stringLookupFunctions = map[string]bool{
	"joinget": true, "joingetornull": true, "dicthas": true, "dictgethierarchy": true, "dictisin": true,
	"dictgetchildren": true, "dictgetdescendants": true, "hascolumnintable": true,
}
func isStringLookup(name string) bool {
	l := strings.ToLower(name)
	return stringLookupFunctions[l] || strings.HasPrefix(l, "dictget")
}
```

and, in `rewriteSelectCore` before `RewriteSelectTables`: `engine.StringLookupCalls(ast)` → for each call: if `!Literal` → refuse (`InvalidRewriteRequest`, `<fn> target "<arg>" does not resolve through the caller's databases`); split `Arg` on the first `.` (qualified) or use the logical context; `nameresolve.Resolve(db, table, sel)` → `StatusRewrite` → emitted string in the measured form, record accessed via `buildAccessed` input and `recordRewrite`; anything else (SI table, protected, unmapped, remote) → refuse with the same message. `hasColumnInTable` takes `(db, table, column)` as two literals: resolve the pair. Apply via `engine.RewriteStringLookups`, which replaces the literal's value. Add the corpus-shaped rows to `TestTableRef_TableFunctionsAndEnginesAreAllowlisted`:

```go
		tablerefCase{name: "joinGet mapped", sql: "SELECT joinGet('db1.j', 'v', 1)", wantCode: pb.RewriteCode_Success,
			wantSQL: "SELECT joinGet('<measured form of phys/db1.j>', 'v', 1)", wantAcc: []string{"db1.j"}},
		tablerefCase{name: "joinGet unqualified", sql: "SELECT joinGet('j', 'v', 1)", wantCode: pb.RewriteCode_Success, wantAcc: []string{"db1.j"}},
		tablerefCase{name: "joinGet phys", sql: "SELECT joinGet('phys.`db2.x`', 'v', 1)", wantCode: pb.RewriteCode_InvalidRewriteRequest, wantMsg: "protected database phys is not addressable"},
		tablerefCase{name: "joinGet active", sql: "SELECT joinGet('db1.t', 'v', 1)", si: true, wantCode: pb.RewriteCode_InvalidRewriteRequest, wantMsg: `joinGet target "db1.t" does not resolve`},
		tablerefCase{name: "joinGet non-literal", sql: "SELECT joinGet(concat('db1', '.j'), 'v', 1)", wantCode: pb.RewriteCode_InvalidRewriteRequest, wantMsg: "does not resolve"},
		tablerefCase{name: "hasColumnInTable mapped", sql: "SELECT hasColumnInTable('db1', 'j', 'v')", wantCode: pb.RewriteCode_Success, wantAcc: []string{"db1.j"}},
```

(The `joinGet phys` row is answered by Task 4's protected step because `CollectDatabaseReferences` must also visit string-lookup arguments: add a `stringLookup` callback to the visitor that reports the qualifier of a literal argument, and call it from `walkFunctionExpression` for `isStringLookup` names.)

- [ ] **Step 7: Unmodelled classes**

In `native.go`, replace the pass-through tail of `doRewrite`:

```go
	// Spec 2026-09-26 T7: no handler modelled the statement. Without the SI
	// surface this used to pass through as Success; every unmodelled class is
	// now refused, except a session SET, which names no table and which
	// clients send routinely. Under an active SI surface SET stays refused
	// (H6), so the carve-out is inside the inactive branch only.
	if siVersion != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED {
		resp.Code = pb.RewriteCode_UnsupportedStatement
		resp.Message = StorageIntegrityUnmodelledMessage
		finalize(resp, ast, sql, ec, siVersion, e, selection)
		return resp, nil
	}
	if selection.Mode == nameresolve.ModeDynamic && !isSessionSet(ast, sql) {
		resp.Code = pb.RewriteCode_UnsupportedStatement
		resp.Message = engine.UnsupportedStatementMessage
		finalize(resp, ast, sql, ec, siVersion, e, selection)
		return resp, nil
	}
	if gen, gerr := e.Generate(ast); gerr == nil && gen != "" {
		resp.SqlAfterRewrite = gen
	}
	resp.Code = pb.RewriteCode_Success
	finalize(resp, ast, sql, ec, siVersion, e, selection)
	return resp, nil
```

with

```go
// isSessionSet reports a top-level SET statement. Measured 2026-09-26: the
// pinned polyglot renders `SET max_threads = 1` as
// {"command": {"this": "SET max_threads = 1"}}, so the check is the command text.
func isSessionSet(ast engine.AST, sql string) bool {
	kind, _ := engine.NodeKind(ast)
	if kind != engine.NodeCommand {
		return false
	}
	text, _ := engine.CommandSQL(ast)
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(text)), "SET ")
}
```

The `selection.Mode == ModeDynamic` guard keeps static-mode and option-less requests (the legacy round-trip tests) on the old pass-through.

Flip `native_test.go` `TestDoRewrite_UnmodelledStatementPassesThroughWithoutStorageIntegrity` → `…IsRefusedWithoutStorageIntegrity` expecting `UnsupportedStatement` + `statement is not supported`, and `native_v2_test.go` `TestStorageIntegrityContractV1_EmptyMapStaysLegacy`: `SYSTEM RELOAD CONFIG` → refused; `SET max_threads = 1` → still `Success` echo without acknowledgement (rename to `…EmptyMapRefusesUnmodelledButPassesSet`). Also `service_test.go:43` (`SET max_threads = 4`) keeps passing.

- [ ] **Step 8: Run everything**

Run: `POLYGLOT_SQL_FFI_PATH=… go test ./ ./internal/... -run 'TestTableRef_|TestClassifyTableFunction|TestTableEngineAllowed|TestDoRewrite_Unmodelled|TestStorageIntegrityContractV1' -v 2>&1 | tail -40 && env SNAPSHOT_QUERY_ORDINARY=1 POLYGLOT_SQL_FFI_PATH=… go test ./... 2>&1 | tail`
Expected: PASS; corpus: `si_ordinary_local_catalog_function_allowed` now fails (Task 8 flip). Anything else that changes is a bug here.

- [ ] **Step 9: Commit**

```bash
git add internal/engine/allowlists.go internal/engine/allowlists_test.go internal/engine/nodes.go internal/handlers/lookups.go internal/handlers/preflight.go internal/handlers/select.go native.go native_test.go native_v2_test.go native_tableref_test.go
git commit -m "feat(engine): allowlist table functions, engines and settings; resolve string lookups; refuse unmodelled classes (spec T5–T7)"
```

---

### Task 8: shared corpus cases and flipped pins

**Files:**
- Modify: `internal/harness/sicorpus_test.go:43-58` (`SIDynamic.ProtectedDatabases`), `internal/harness/storage_integrity_golden_test.go` (`c.options()` sends it), the corpus validator (`ValidateSICorpus`), `internal/harness/sicorpus_contract_test.go` (fingerprint / byte / case constants)
- Modify: `internal/harness/testdata/storage_integrity_cases.json`
- Modify: `internal/harness/AGENTS.md` (schema note)

**Interfaces:**
- Produces: the corpus JSON that Plan B copies byte-for-byte; new schema key `dynamic.protected_databases` (array of strings).

- [ ] **Step 1: Extend the schema and the validator (failing test first)**

Add to `sicorpus_test.go` a validator test:

```go
func TestValidateSICorpus_TableReferenceCasesCarryProtectedDatabases(t *testing.T) {
	cases := []SICase{{Name: "si_tr_x_rejected", SQL: "SELECT 1", ContractVersion: "V1", WantCode: "InvalidRewriteRequest",
		WantMessageContains: "x", Reject: true, Dynamic: &SIDynamic{DatabaseMap: map[string]string{"db1": "phys"}}}}
	if err := ValidateSICorpus(cases); err == nil || !strings.Contains(err.Error(), "protected_databases") {
		t.Fatalf("a si_tr_ case without protected_databases must fail validation: %v", err)
	}
}
```

Run it (FAIL: field undefined), then add `ProtectedDatabases []string `json:"protected_databases,omitempty"`` to `SIDynamic`, send it from `c.options()` (`dyn.ProtectedDatabases = c.Dynamic.ProtectedDatabases`), and add rule R9 to `ValidateSICorpus`: a case whose name starts with `si_tr_` must set a non-empty `dynamic.protected_databases`. Run it (PASS). Document R9 in `internal/harness/AGENTS.md` under the corpus contract (one line, no wrapping).

- [ ] **Step 2: Author the cases**

Every case below uses this `dynamic` block unless stated (copy it exactly; the JSON key order is `database_map`, `known_physical_databases`, `upstream_logical_database_in_context`, `delim`, `protected_databases`, `storage_integrity`):

```json
"dynamic": {
  "database_map": {"db1": "phys"},
  "known_physical_databases": ["phys"],
  "upstream_logical_database_in_context": "db1",
  "delim": "_",
  "protected_databases": ["phys", "hg_safe", "hg_unsafe", "hg_promote"],
  "storage_integrity": {"tables": {}, "read_mode": "SAFE", "reserved_row_id_column": "_hg_row_id", "reserved_databases": ["hg_safe", "hg_unsafe", "hg_promote"]}
}
```

with `"contract_version": "V1"` for the inactive surface (empty `tables` under V1 is legacy and sends no acknowledgement: set `"want_no_contract_ack": true`) and, for the V2 rows, `"contract_version": "V2"` with `"tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}}`.

Author, in this order, appending to the end of the array:

1. **Parameters** — one reject case per SQL in `TestTableRef_ParametersInTablePositionsAreRefused`, V1 inactive, named `si_tr_param_<position>_rejected` (positions: `from_bare`, `from_qualified`, `db`, `join`, `subquery`, `in`, `in_qualified`, `in_call`, `insert_select_source`, `ctas_source`, `mv_to`, `mv_name`, `drop_bare`, `drop`, `insert_target`, `create_target`, `exists`, `show_create`, `describe`, `rename`, `show_tables_from`, `use`), `want_code: "InvalidRewriteRequest"`, `want_message_contains: "query parameters are not supported in a database or table position"`, `want_stmt: ""`; plus `si_tr_param_value_allowed` (success, pins the `{v: UInt64}` spelling) and `si_tr_param_column_allowed`.
2. **Protected names** — for `db ∈ {phys, hg_safe, hg_unsafe, hg_promote}` and each shape in `TestTableRef_ProtectedDatabasesAreRefusedEverywhere`: V1 inactive reject with `want_message_contains: "protected database <db> is not addressable"`, named `si_tr_protected_<db>_<shape>_rejected` (`shape` ∈ `from`, `join`, `subquery`, `cte`, `in`, `in_paren`, `in_tuple`, `in_call`, `global_in`, `insert_select_source`, `ctas_source`, `view_body`, `mv_to`, `insert_target`, `drop`, `create_target`, `create_as`, `rename`, `exists`, `show_create`, `describe`, `show_tables_from`, `use`, `create_database`, `drop_database`, `merge_arg`, `remote_arg`, `engine_merge_arg`, `joinget_arg`). For `phys` add the same shapes under V2 (`si_tr_protected_phys_<shape>_v2_rejected`) and `si_tr_phys_active_ordinary_in_v2_rejected` / `…_insert_select_source_v2_rejected` / `…_ctas_source_v2_rejected` (`phys.`db1.t``). The `hg_*` V2 rows are already pinned by existing cases; do not duplicate them.
3. **Sources** — success cases from `TestTableRef_InOperandsAreRewrittenAndReported` and `TestTableRef_EmbeddedSourcesAreRewrittenAndReported`, with `want_sql` = the engine's output after `NormalizeSIIdentifierQuotes`, `want_accessed` in response order with full `logical_database` / `physical_database` / `is_storage_integrity` fields, `want_table_rewrites`. Names as listed in spec §10.1 (`si_tr_in_own_rewritten`, …, `si_tr_in_system_reported`, `si_tr_in_cte_alias_untouched`).
4. **Functions and engines** — from `TestTableRef_TableFunctionsAndEnginesAreAllowlisted` (`si_tr_tf_<name>_rejected`, `si_tr_tf_unknown_rejected`, `si_tr_tf_numbers_allowed`, `si_tr_tf_view_body_rewritten`, `si_tr_tf_input_allowed`, `si_tr_tf_url_unchanged`, `si_tr_engine_<name>_rejected`, `si_tr_engine_replicated_with_args_rejected`, `si_tr_engine_replicated_bare_allowed`, `si_tr_engine_storage_policy_rejected`, `si_tr_engine_disk_rejected`, `si_tr_engine_mergetree_allowed`, `si_tr_engine_unknown_rejected`).
5. **Lookups** — as measured in Task 7 Step 6 (`si_tr_joinget_*`, `si_tr_dictget_*`, `si_tr_hascolumnintable_*`).
6. **Classes** — `si_tr_system_inactive_rejected`, `si_tr_set_inactive_passthrough` (success, `want_sql: "SET max_threads = 1"`, `want_stmt: ""`, `want_no_contract_ack: true`), `si_tr_explain_rejected`, `si_tr_check_table_inactive_rejected`, `si_tr_create_user_inactive_rejected`, `si_tr_detach_rejected`, `si_tr_kill_rejected` (message `statement is not supported`).
7. **Parity** — `si_tr_mv_to_active_rejected` (V2, `CREATE MATERIALIZED VIEW db1.mv TO db1.t AS SELECT * FROM db1.o`, `UnsupportedStatement`, `accepts writes only through the signed statement lane`), `si_tr_mv_to_ordinary_rewritten`, `si_tr_mv_append_to_active_rejected` (`CREATE MATERIALIZED VIEW db1.mv REFRESH EVERY 1 HOUR APPEND TO db1.t AS SELECT * FROM db1.o`).
8. **Flips** — edit in place: `si_ordinary_in_table_allowed` (rewrite + accessed both), the CTE-bound `IN t` case (rewrite), `si_ordinary_local_catalog_function_allowed` → rename `si_ordinary_local_catalog_function_rejected` (`UnsupportedStatement`, `table function mergeTreeIndex is not accepted`), and every case Tasks 5–7 reported as changed (SI-source INSERT/CTAS rejections with an ordinary target become derived-read successes: keep the name, change the expectation, and note the flip in the commit message).

Obtain each success case's `want_sql` by running the golden test once with the case pinned to a placeholder and copying the reported actual (the runner prints `SQL pin mismatch` with both strings), then re-run to green. Do not use `UPDATE_GOLDEN=1` here (it requires the C++ oracle; Plan B runs it).

- [ ] **Step 3: Run the corpus suites and update the pins**

Run: `POLYGLOT_SQL_FFI_PATH=… go test ./internal/harness/ -run 'TestStorageIntegrityGolden|TestSICorpus' 2>&1 | tail -20`
Expected: `TestStorageIntegrityGolden` PASS; `TestSICorpusIsBytePinned` FAIL reporting the new FNV-1a/64, byte count and case count. Copy those three values into `SICorpusFingerprint`, `SICorpusBytes` and `SICorpusCases` (declared in `sicorpus_test.go`, checked by `sicorpus_contract_test.go`); rerun: PASS. Record `shasum -a 256 internal/harness/testdata/storage_integrity_cases.json` in the commit message and in the PR description (Plan B needs it).

- [ ] **Step 4: Full suite and vet**

Run: `go vet ./... && env SNAPSHOT_QUERY_ORDINARY=1 go test ./... && env SNAPSHOT_QUERY_ORDINARY=1 POLYGLOT_SQL_FFI_PATH=… go test ./...`
Expected: all PASS with and without the engine.

- [ ] **Step 5: Commit**

```bash
git add internal/harness/
git commit -m "test(corpus): table-reference hardening cases and flipped pins (sha256 <value>)"
```

---

### Task 9: documentation, PR, differential hand-off and release

**Files:**
- Modify: `README.md` (policy summary under the rewrite behaviour section), `AGENTS.md` (the new files and the precedence), `internal/harness/AGENTS.md` (R9, done in Task 8)

- [ ] **Step 1: Document**

Add to `AGENTS.md` a section "Table-reference policy (spec 2026-09-26)" listing `internal/handlers/preflight.go`, `internal/engine/parameters.go`, `references.go`, `allowlists.go`, `internal/handlers/lookups.go`, the message constants and the precedence order, one line each, no wrapping.

- [ ] **Step 2: Push and open the PR**

```bash
git push -u origin feat/table-reference-hardening
gh pr create --title "feat: table-reference hardening (spec 2026-09-26)" --body "$(cat <<'EOF'
Implements spec 2026-09-26 §5 for the native engine: identifier parameters, protected namespace, source rewriting, allowlists, string lookups, unmodelled classes. Corpus sha256: <value>. Paired with housegate/rewriter PR <link once open>.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Widen `harness.Compare` (spec §10.5), test first**

`internal/harness/compare_test.go`: add a case where two responses differ only in `message`, and one where they differ only in `original_accessed_tables` (same tables, different `is_storage_integrity`), asserting `Compare` reports each as a divergence. Run (`go test ./internal/harness/ -run TestCompare`): FAIL. Then extend `Compare` (`compare.go`, `func Compare`) to diff `message` exactly and `original_accessed_tables` field-by-field in order (`original_database`, `original_table`, `logical_database`, `physical_database`, `is_remote`, `is_storage_integrity`). Run: PASS. Commit: `git commit -am "test(harness): Compare diffs message and original_accessed_tables"`.

- [ ] **Step 4: Differential before release**

When Plan B's branch builds on the build box, run from this branch: `REWRITER_ORACLE_ADDR=<host:port> POLYGLOT_SQL_FFI_PATH=… go test ./internal/harness/ -run TestStorageIntegrityGolden -v 2>&1 | tee differential.log`. Zero divergences over the widened field set is the merge condition for both PRs; record the run in the spec's §10.5 (date, both commits, oracle binary sha256, suite counts, corpus fingerprint) — that edit lands in the housegate repository with Plan C.

- [ ] **Step 5: Release**

After the user approves the merge: merge, then `gh workflow run release.yml --ref main` and confirm the tag is `v0.14.0` (the FFI assets are rebuilt from the unchanged submodule, so their SHA256SUMS should equal v0.13.0's; verify and note it in the release description). Ask the user before merging and before cutting the release.
