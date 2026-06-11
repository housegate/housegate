# rewriter-go Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the SQL rewriter engine selectable per deployment — `rewriter.engine: grpc` (external service, default, unchanged) or `rewriter.engine: native` (in-process rewriter-go).

**Architecture:** Two repos, strictly ordered. First rewriter-go (at `/Users/uranuswch/Dev/housegate/rewriter-go`) gains a public stateless `Service` mirroring the gRPC contract. Then housegate adopts `rewriter-go/gen/pb` as its only generated proto package (deleting `protos/` to kill the dual-registration panic) and swaps `SentioNetworkFactory`'s gRPC client for a 2-method `backend` interface with grpc + native implementations. Everything above the factory (dynamic args, USE mirroring, fail-open, commitgate) is untouched.

**Tech Stack:** Go 1.25/1.26, Bazel 8.5.1 + Bzlmod + gazelle (housegate only), protobuf v1.36.11, polyglot FFI via purego (`CGO_ENABLED=0`), grpc-go.

**Spec:** [docs/superpowers/specs/2026-06-11-rewriter-go-integration-design.md](../specs/2026-06-11-rewriter-go-integration-design.md)

**Branches:** rewriter-go → `feat/service-api` (new). housegate → `feat/native-rewriter-engine` (already exists, spec committed).

---

## Phase A — rewriter-go (prerequisite)

All Task A paths are relative to `/Users/uranuswch/Dev/housegate/rewriter-go`. This repo uses plain `go test`, NOT Bazel. FFI-dependent tests skip themselves when `POLYGLOT_SQL_FFI_PATH` is unset.

### Task A0: Preflight

**Files:** none (verification only)

- [ ] **Step A0.1: Verify checkout state and create branch**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git status --short          # expect: empty (clean)
git checkout main && git pull
git checkout -b feat/service-api
git submodule update --init # polyglot submodule, pinned e3a8913
```

If the working tree is dirty, STOP and ask the user how to proceed.

- [ ] **Step A0.2: Ensure the FFI library exists, then baseline tests**

```bash
ls third_party/lib/libpolyglot_sql_ffi.* 2>/dev/null || make ffi   # cargo build, a few minutes on first run
set -x POLYGLOT_SQL_FFI_PATH $PWD/third_party/lib/libpolyglot_sql_ffi.dylib   # fish syntax; .so on Linux
go test ./...
```

Expected: all PASS (this is the baseline; if anything fails here it is pre-existing — record it, don't fix it).

### Task A1: Extract `doRewrite` (pure refactor, no behavior change)

**Files:**
- Modify: `native.go` (the `NativeRewriter.Rewrite` body and the `classify` method)

`NativeRewriter.Rewrite` currently inlines the whole handler pipeline (parse → classify → existence clause → writes → db-level → exists/show-create → grant → select → pass-through). Extract it into a free function so the upcoming `Service` can share it. `classify` is a method that uses no receiver state — make it a free function at the same time.

- [ ] **Step A1.1: Extract the pipeline into `doRewrite` and shrink `Rewrite`**

In `native.go`, replace the existing `func (r *NativeRewriter) Rewrite(...)` with:

```go
// doRewrite is the engine-level rewrite pipeline shared by NativeRewriter
// (per-connection, options via callback) and Service (stateless, options
// from the request). A non-nil error means an unexpected/internal failure
// the caller should treat as fail-open; rewrite rejections travel inside
// the response Code instead.
func doRewrite(e engine.Engine, sql string, opts []*pb.RewriteOption) (*pb.RewriteSQLResponse, error) {
	resp := &pb.RewriteSQLResponse{SqlAfterRewrite: sql} // SQL always set; echoes input
	ast, err := e.ParseOne(sql)
	if err != nil {
		resp.Code = pb.RewriteCode_SyntaxError
		resp.Message = err.Error()
		return resp, nil // SyntaxError is a code, not a Go error
	}
	resp.StatementType = classify(ast)

	// existence_clause is derived from the AST (IF [NOT] EXISTS) and stamped on
	// EVERY handled response below — it survives rejects (proto contract), unlike
	// statement_type. Computed once here; only a SyntaxError (handled above, no
	// AST) leaves it UNSPECIFIED.
	ec := pb.ExistenceClause_EXISTENCE_CLAUSE_UNSPECIFIED
	if inx, ix, _ := engine.ExistenceClause(ast); inx {
		ec = pb.ExistenceClause_EXISTENCE_CLAUSE_IF_NOT_EXISTS
	} else if ix {
		ec = pb.ExistenceClause_EXISTENCE_CLAUSE_IF_EXISTS
	}

	// Phase 2: route writes (CREATE/DROP/ALTER/INSERT/UPDATE/DELETE/RENAME/EXCHANGE/
	// views, + bare-rejects, + out-of-phase CREATE/DROP DATABASE) before SELECT.
	if wresp, handled, werr := handlers.RewriteWrite(e, ast, sql, opts); werr != nil {
		return nil, werr
	} else if handled {
		finalize(wresp, sql, ec)
		return wresp, nil
	}

	// Phase 3: route db-level statements (USE / SHOW TABLES / SHOW DATABASES /
	// CREATE DATABASE / DROP DATABASE) after writes, before SELECT.
	if dresp, handled, derr := handlers.RewriteDBLevel(e, ast, sql, opts); derr != nil {
		return nil, derr
	} else if handled {
		finalize(dresp, sql, ec)
		return dresp, nil
	}

	// Phase 4: EXISTS / SHOW CREATE (single-target), then GRANT / REVOKE.
	if xresp, handled, xerr := handlers.RewriteExistsShowCreate(e, ast, sql, opts); xerr != nil {
		return nil, xerr
	} else if handled {
		finalize(xresp, sql, ec)
		return xresp, nil
	}
	if gresp, handled, gerr := handlers.RewriteGrant(e, ast, sql, opts); gerr != nil {
		return nil, gerr
	} else if handled {
		finalize(gresp, sql, ec)
		return gresp, nil
	}

	// Phase 1: route SELECT to the real handler; everything else stays pass-through.
	if kind, _ := engine.NodeKind(ast); kind == engine.NodeSelect {
		hresp, herr := handlers.RewriteSelect(e, ast, opts)
		if herr != nil {
			return nil, herr
		}
		finalize(hresp, sql, ec) // SELECT never carries IF [NOT] EXISTS → ec stays UNSPECIFIED
		return hresp, nil
	}

	// Pass-through: regenerate (proves the engine round-trips); fall back to
	// the input on any generate hiccup so SQL is always runnable.
	if gen, gerr := e.Generate(ast); gerr == nil && gen != "" {
		resp.SqlAfterRewrite = gen
	}
	resp.Code = pb.RewriteCode_Success
	finalize(resp, sql, ec)
	return resp, nil
}

func (r *NativeRewriter) Rewrite(_ context.Context, sql, account string) (RewriteResult, error) {
	var opts []*pb.RewriteOption
	if r.options != nil {
		opts = r.options(account)
	}
	resp, err := doRewrite(r.engine, sql, opts)
	if err != nil {
		return RewriteResult{}, err // unexpected/internal → fail-open Go error
	}
	r.stash(sql, account, resp)
	return resultFromPB(resp), nil
}
```

Note the one intentional simplification: the old code stashed inside each branch; the new `Rewrite` stashes once after `doRewrite` returns. Every old branch that returned a response also stashed it, so behavior is identical.

- [ ] **Step A1.2: Make `classify` a free function**

In the same file change the two `classify` lines:

```go
// before
func (r *NativeRewriter) classify(ast engine.AST) pb.StatementType {
// after
func classify(ast engine.AST) pb.StatementType {
```

(`classifyCommand` is already free.) The old call site `r.classify(ast)` was deleted with the old `Rewrite` body; `doRewrite` already calls plain `classify(ast)`.

- [ ] **Step A1.3: Run the full test suite — refactor must be invisible**

```bash
go test ./...
```

Expected: identical result set to the A0.2 baseline (all PASS).

- [ ] **Step A1.4: Commit**

```bash
git add native.go
git commit -m "refactor: extract doRewrite pipeline from NativeRewriter.Rewrite

Pure extraction so the upcoming stateless Service can share the
handler pipeline. classify becomes a free function (no receiver
state). No behavior change."
```

### Task A2: `Service` — stateless Rewrite (TDD)

**Files:**
- Create: `service.go`
- Create: `service_test.go`

- [ ] **Step A2.1: Write the failing tests**

Create `service_test.go`:

```go
package rewriter

import (
	"context"
	"maps"
	"os"
	"testing"

	"github.com/housegate/rewriter-go/gen/pb"
)

// dynOpts builds the single dynamic-args RewriteOption shape housegate sends.
func dynOpts(dbMap map[string]string, known []string) []*pb.RewriteOption {
	return []*pb.RewriteOption{{
		Op: pb.RewriteOp_TableNameRewrite,
		Value: &pb.RewriteOption_TableNameArgs{TableNameArgs: &pb.RewriteTableNameArgs{
			DynamicArgs: &pb.RewriteTableDynamicArgs{
				DatabaseMap:            dbMap,
				KnownPhysicalDatabases: known,
			},
		}},
	}}
}

// TestServiceMatchesNativeRewriter is the parity gate: the stateless
// Service and the per-connection NativeRewriter must produce
// field-identical responses for the same SQL + options.
func TestServiceMatchesNativeRewriter(t *testing.T) {
	e := newEngine(t) // skips without POLYGLOT_SQL_FFI_PATH
	opts := dynOpts(map[string]string{"db1": "phys"}, []string{"phys"})
	nr := New(e, WithOptions(func(string) []*pb.RewriteOption { return opts }))
	svc := &Service{engine: e} // in-package: share the engine, skip a second FFI load

	cases := []string{
		"SELECT a FROM db1.t",
		"USE db1",
		"CREATE TABLE db1.t2 (x Int64) ENGINE = MergeTree ORDER BY x",
		"GRANT SELECT ON db1.* TO bob",
		"SET max_threads = 4",
		"SELECT FROM WHERE ((", // SyntaxError path
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			res, rerr := nr.Rewrite(context.Background(), sql, "acct")
			resp, serr := svc.Rewrite(context.Background(), &pb.RewriteSQLRequest{Sql: sql, Options: opts})
			if (rerr != nil) != (serr != nil) {
				t.Fatalf("error mismatch: native=%v service=%v", rerr, serr)
			}
			if rerr != nil {
				return
			}
			if res.Code != resp.GetCode() {
				t.Errorf("code: native=%v service=%v", res.Code, resp.GetCode())
			}
			if res.SQL != resp.GetSqlAfterRewrite() {
				t.Errorf("sql: native=%q service=%q", res.SQL, resp.GetSqlAfterRewrite())
			}
			if res.StatementType != resp.GetStatementType() {
				t.Errorf("stmt: native=%v service=%v", res.StatementType, resp.GetStatementType())
			}
			if !maps.Equal(res.TableRewrites, resp.GetTableRewrites()) {
				t.Errorf("table_rewrites: native=%v service=%v", res.TableRewrites, resp.GetTableRewrites())
			}
			if !maps.Equal(res.DatabaseRewrites, resp.GetDatabaseRewrites()) {
				t.Errorf("database_rewrites: native=%v service=%v", res.DatabaseRewrites, resp.GetDatabaseRewrites())
			}
			if res.ExistenceClause != resp.GetExistenceClause() {
				t.Errorf("existence_clause: native=%v service=%v", res.ExistenceClause, resp.GetExistenceClause())
			}
		})
	}
}

// TestNewServiceLoadsAndCloses exercises the public constructor end to end.
func TestNewServiceLoadsAndCloses(t *testing.T) {
	if os.Getenv("POLYGLOT_SQL_FFI_PATH") == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run via `make test`")
	}
	svc, err := NewService("") // "" → OpenDefault → honors POLYGLOT_SQL_FFI_PATH
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	resp, err := svc.Rewrite(context.Background(), &pb.RewriteSQLRequest{Sql: "SELECT 1"})
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if resp.GetCode() != pb.RewriteCode_Success {
		t.Fatalf("code = %v, want Success", resp.GetCode())
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
```

- [ ] **Step A2.2: Run tests to verify they fail**

```bash
go test . -run 'TestService|TestNewService' -count=1
```

Expected: compile FAILURE — `undefined: Service`, `undefined: NewService`.

- [ ] **Step A2.3: Implement `Service`**

Create `service.go`:

```go
package rewriter

import (
	"context"

	"github.com/housegate/rewriter-go/gen/pb"
	"github.com/housegate/rewriter-go/internal/engine"
	"github.com/housegate/rewriter-go/internal/reverse"
)

// Service is the stateless, process-shared entry point mirroring the
// rewriter-grpc service contract (one call = one request/response, all
// context travels in the request). Safe for concurrent use: the engine
// guards FFI calls with an RWMutex and Service holds no mutable state.
//
// Use Service when embedding the rewriter in a host that already speaks
// the proto contract (e.g. housegate's backend seam); use NativeRewriter
// for the per-connection, callback-driven shape.
//
// ctx is accepted for interface symmetry with the gRPC client but is not
// consulted mid-call: polyglot FFI calls cannot be interrupted. Calls are
// local and fast (no network), so timeouts effectively never fire.
type Service struct {
	engine engine.Engine
}

// NewService loads the polyglot FFI library and returns a ready Service.
// libPath == "" falls back to polyglot's default resolution
// (POLYGLOT_SQL_FFI_PATH, then standard install locations). The Service
// owns the engine; Close releases it.
func NewService(libPath string) (*Service, error) {
	e, err := engine.NewPolyglot(libPath)
	if err != nil {
		return nil, err
	}
	return &Service{engine: e}, nil
}

// Rewrite runs the shared doRewrite pipeline with the options carried in
// the request. Rejections travel in resp.Code; a non-nil error means an
// internal failure the caller should treat as fail-open.
func (s *Service) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	return doRewrite(s.engine, req.GetSql(), req.GetOptions())
}

// RewriteErrorMessage inverts physical names in a ClickHouse error message
// back to the logical names the client used. Stateless: the forward maps
// are re-derived by re-running the rewrite on req.Sql + req.Options (error
// paths are rare; one extra parse per exception is acceptable). When the
// forward rewrite is non-Success — or sql/message is empty — the message
// passes through unchanged, mirroring NativeRewriter's non-Success
// passthrough and the C++ doRewriteErrorMessage semantics.
func (s *Service) RewriteErrorMessage(_ context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error) {
	msg := req.GetErrorMessage()
	out := &pb.RewriteErrorMessageResponse{Code: pb.RewriteCode_Success, ErrorAfterRewrite: msg}
	if msg == "" || req.GetSql() == "" {
		return out, nil
	}
	resp, err := doRewrite(s.engine, req.GetSql(), req.GetOptions())
	if err != nil || resp.GetCode() != pb.RewriteCode_Success {
		return out, nil
	}
	out.ErrorAfterRewrite = reverse.Invert(msg, req.GetSql(), resp.GetSqlAfterRewrite(), resp.GetTableRewrites(), resp.GetDatabaseRewrites())
	return out, nil
}

// Close releases the engine. Safe to call once; the engine's own Close is
// idempotent.
func (s *Service) Close() error {
	return s.engine.Close()
}
```

- [ ] **Step A2.4: Run tests to verify they pass**

```bash
go test . -run 'TestService|TestNewService' -count=1 -v
```

Expected: PASS (or SKIP if `POLYGLOT_SQL_FFI_PATH` got unset).

- [ ] **Step A2.5: Commit**

```bash
git add service.go service_test.go
git commit -m "feat: add stateless Service mirroring the gRPC contract

Public constructor (NewService) finally makes the rewriter usable from
outside the module; Rewrite takes options from the request instead of
the WithOptions callback. Parity with NativeRewriter is asserted
field-by-field in service_test.go."
```

### Task A3: `Service.RewriteErrorMessage` test (TDD — test was deferred from A2 implementation)

**Files:**
- Modify: `service_test.go`

(The implementation already landed in A2.3 as one coherent file; this task adds its dedicated test. If you prefer strict test-first, write this step before A2.3 — both orders end green.)

- [ ] **Step A3.1: Add the error-inversion test**

Append to `service_test.go`:

```go
// TestServiceRewriteErrorMessage: physical names in an error message are
// inverted back to logical ones using maps re-derived from sql+options;
// unparseable SQL (non-Success forward rewrite) passes through unchanged.
func TestServiceRewriteErrorMessage(t *testing.T) {
	e := newEngine(t)
	svc := &Service{engine: e}
	opts := dynOpts(map[string]string{"db1": "phys"}, []string{"phys"})

	resp, err := svc.RewriteErrorMessage(context.Background(), &pb.RewriteErrorMessageRequest{
		Sql:          "SELECT a FROM db1.t",
		ErrorMessage: "Table phys.db1_t does not exist",
		Options:      opts,
	})
	if err != nil {
		t.Fatalf("RewriteErrorMessage: %v", err)
	}
	if got := resp.GetErrorAfterRewrite(); !strings.Contains(got, "db1.t") || strings.Contains(got, "phys.") {
		t.Errorf("inversion failed: %q", got)
	}

	resp2, err := svc.RewriteErrorMessage(context.Background(), &pb.RewriteErrorMessageRequest{
		Sql:          "SELECT FROM WHERE ((",
		ErrorMessage: "boom",
		Options:      opts,
	})
	if err != nil {
		t.Fatalf("RewriteErrorMessage(passthrough): %v", err)
	}
	if resp2.GetErrorAfterRewrite() != "boom" {
		t.Errorf("passthrough = %q, want \"boom\"", resp2.GetErrorAfterRewrite())
	}
}
```

Add `"strings"` to the test file imports.

- [ ] **Step A3.2: Run, expect pass**

```bash
go test . -run TestServiceRewriteErrorMessage -count=1 -v
```

Expected: PASS. If the inversion assertion fails on exact text, inspect what `reverse.Invert` produced and adjust the *assertion* (Contains-style) — do NOT change `reverse` (it is harness-validated).

- [ ] **Step A3.3: Full suite + commit**

```bash
go test ./...
git add service_test.go
git commit -m "test: cover Service.RewriteErrorMessage inversion + passthrough"
```

### Task A4: Document and push

**Files:**
- Modify: `README.md` (Layout table + a short Service paragraph)

- [ ] **Step A4.1: README — add Service to the layout table**

In the `## Layout` table change the first row:

```markdown
| `rewriter.go` / `native.go` / `service.go` | Public `Rewriter` interface + `RewriteResult`; the per-connection `NativeRewriter`; the stateless `Service` (request/response shape mirroring the gRPC contract — the embedding entry point for hosts like housegate) |
```

- [ ] **Step A4.2: Commit and push the branch**

```bash
git add README.md
git commit -m "docs: mention Service in README layout"
git push -u origin feat/service-api
git rev-parse HEAD   # record this SHA — Phase B pins it
```

Record the printed SHA as `<REWRITER_GO_SHA>` for Task B1. (If the user later squash-merges the PR, re-pin housegate to the merged main SHA — note this in the housegate PR description.)

---

## Phase B — housegate

All Task B paths relative to `/Users/uranuswch/Dev/housegate/housegate`, branch `feat/native-rewriter-engine`. Tests run through Bazel only (`go test ./...` is broken on main until B2 fixes it, and Bazel stays the ground truth after).

### Task B1: Add the rewriter-go dependency (no imports yet)

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel` (mechanical, tool-driven)

- [ ] **Step B1.1: Pin the dependency**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
go mod edit -require=github.com/housegate/rewriter-go@v0.0.0
GOFLAGS=-mod=mod go get github.com/housegate/rewriter-go@<REWRITER_GO_SHA>
go mod tidy
```

Expected `go.mod` deltas: `github.com/housegate/rewriter-go v0.0.0-<date>-<sha12>` added; `google.golang.org/grpc` bumps v1.78.0 → v1.81.1 (MVS from rewriter-go); `github.com/tobilg/polyglot/packages/go v0.5.1` + `github.com/ebitengine/purego v0.10.0` appear as indirect. The `replace` in rewriter-go's own go.mod is NOT inherited — polyglot v0.5.1 resolves from the module proxy, and its tag points at the exact submodule commit rewriter-go develops against (verified during design).

- [ ] **Step B1.2: Sync Bazel and verify the build is still green**

```bash
bazel mod tidy
bazel build //...
```

Expected: `MODULE.bazel`'s `use_repo(go_deps, ...)` list gains `com_github_housegate_rewriter_go` (and possibly the transitive repos); build PASSES. Nothing imports the new module yet, so any failure here is the grpc version bump — fix before proceeding (check `bazel mod tidy` reran cleanly).

- [ ] **Step B1.3: Commit**

```bash
git add go.mod go.sum MODULE.bazel
git commit -m "build(deps): add github.com/housegate/rewriter-go (grpc 1.78→1.81 via MVS)"
```

### Task B2: pb unification — adopt `gen/pb`, delete `protos/`

**Files:**
- Modify: `pkg/rewriter/args.go:4`, `pkg/rewriter/sentio.go:26`, `pkg/integration/commitgate_test.go:22`, `pkg/integration/permission_isolation_test.go:14`, `pkg/integration/testenv/rewriter_mock.go:13` (import line only, alias `pb` stays so bodies don't change)
- Delete: `protos/` (BUILD.bazel, rewriter.pb.go, rewriter.proto)
- Modify: `bazel/golang/BUILD.bazel` (drop the `//protos:protos.update_go_pb` entry)

- [ ] **Step B2.1: (best-effort spec §10.1 probe) confirm the dual-registration panic**

Add a throwaway file `pkg/rewriter/conflict_probe_test.go`:

```go
package rewriter

import (
	"testing"

	_ "github.com/housegate/rewriter-go/gen/pb"
)

func TestProbeDualRegistration(t *testing.T) { t.Log("if this binary even starts, there was no conflict") }
```

```bash
bazel run //:gazelle -- pkg/rewriter
bazel test //pkg/rewriter:rewriter_test --test_filter=TestProbeDualRegistration --test_output=all
```

Expected: test target PANICS at init with a protoregistry conflict over `rewriter.RewriteSQLRequest` (both `protos/rewriter.pb.go` and `gen/pb` are linked in). Record the panic text for the commit message, then delete the probe:

```bash
rm pkg/rewriter/conflict_probe_test.go
```

If it does NOT panic, the swap below is still correct (one contract source beats two) — note the surprise in the commit message and continue.

- [ ] **Step B2.2: Swap the import in all 5 files**

In each of the 5 files listed above, change exactly one line:

```go
// before
pb "housegate/housegate/protos"
// after
pb "github.com/housegate/rewriter-go/gen/pb"
```

(`testenv/rewriter_mock.go` keeps using `pb.UnimplementedRewriterServiceServer` / `pb.RegisterRewriterServiceServer` — the module's `gen/pb` ships the same grpc stubs.)

- [ ] **Step B2.3: Delete `protos/` and its Bazel hook**

```bash
git rm -r protos/
```

In `bazel/golang/BUILD.bazel`, delete the now-dangling `write_source_files` block (its only `additional_update_targets` entry was `//protos:protos.update_go_pb`):

```python
# DELETE this whole block:
write_source_files(
    name = "update_go_pb",
    additional_update_targets = [
        "//protos:protos.update_go_pb",
    ],
)
```

Also remove the matching `load("@aspect_bazel_lib//lib:write_source_files.bzl", "write_source_files")` line if nothing else in the file uses it, then check for other references:

```bash
grep -rn "update_go_pb\|//protos\|go_proto_library" --include="*.bazel" --include="*.bzl" . | grep -v bazel-
```

Expected after cleanup: only `bazel/golang/go_proto_library.bzl` self-references remain. If `go_proto_library.bzl` has no remaining `load`ers, delete it too (`git rm bazel/golang/go_proto_library.bzl`).

- [ ] **Step B2.4: Regenerate BUILD files and verify**

```bash
bazel mod tidy
bazel run //:gazelle
bazel build //...
bazel test //pkg/proxy:proxy_test //pkg/rewriter:rewriter_test --test_output=errors
```

Expected: gazelle rewrites `deps` in `pkg/rewriter/BUILD.bazel`, `pkg/integration/BUILD.bazel`, `pkg/integration/testenv/BUILD.bazel` from `//protos` to `@com_github_housegate_rewriter_go//gen/pb`; build + tests PASS.

- [ ] **Step B2.5: Check whether the `go test ./...` rough edge died**

```bash
go test ./pkg/rewriter/ ./pkg/sqlmeta/ 2>&1 | tail -5
```

Record the outcome (PASS or a *different* failure than the old protos init panic) — feeds the CLAUDE.md update in Task B6. Do not chase unrelated `go test` breakage; Bazel remains ground truth.

- [ ] **Step B2.6: Commit**

```bash
git add -A
git commit -m "refactor(protos): adopt rewriter-go/gen/pb as the single proto package

Both packages register proto package 'rewriter' globally; linking the
two panics at init (probe confirmed: <paste panic text>). housegate now
imports the contract from the rewriter-go module and protos/ is gone,
along with the update_go_pb write_source_files hook."
```

### Task B3: backend seam in `pkg/rewriter` (TDD)

**Files:**
- Create: `pkg/rewriter/backend.go`
- Create: `pkg/rewriter/backend_test.go`
- Modify: `pkg/rewriter/sentio.go` (factory fields, constructor, Close, 2 call sites)
- Modify: `pkg/rewriter/types.go` (Options gains Engine + NativeLibraryPath)

- [ ] **Step B3.1: Write the failing tests**

Create `pkg/rewriter/backend_test.go`:

```go
package rewriter

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-go/gen/pb"

	"housegate/housegate/pkg/network"
)

// fakeBackend lets tests script the transport without a gRPC server or
// FFI library — the first time sentioRewriter's request/response handling
// is unit-testable.
type fakeBackend struct {
	resp    *pb.RewriteSQLResponse
	err     error
	lastReq *pb.RewriteSQLRequest
}

func (f *fakeBackend) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	f.lastReq = req
	return f.resp, f.err
}

func (f *fakeBackend) RewriteErrorMessage(_ context.Context, _ *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error) {
	return &pb.RewriteErrorMessageResponse{Code: pb.RewriteCode_Success, ErrorAfterRewrite: "inverted"}, nil
}

func (f *fakeBackend) Close() error { return nil }

type fakeSession struct {
	account, logical, physical string
	setLogical                 []string
}

func (s *fakeSession) Account() string              { return s.account }
func (s *fakeSession) LogicalDatabaseName() string  { return s.logical }
func (s *fakeSession) PhysicalDatabaseName() string { return s.physical }
func (s *fakeSession) SetLogicalDatabase(n string)  { s.setLogical = append(s.setLogical, n) }

func newFakeFactory(be backend) *SentioNetworkFactory {
	st := network.NewInMemoryNetworkState()
	st.DatabaseInfos["db1"] = network.DatabaseInfo{DatabaseId: "db1"}
	return &SentioNetworkFactory{
		options:  Options{PhysicalDatabase: "phys", AuthEnabled: false},
		registry: st,
		backend:  be,
	}
}

func TestSentioRewriter_SuccessPopulatesResult(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:            pb.RewriteCode_Success,
		SqlAfterRewrite: "SELECT a FROM phys.db1_t",
		StatementType:   pb.StatementType_STATEMENT_TYPE_SELECT,
		TableRewrites:   map[string]string{"db1.t": "phys.db1_t"},
	}}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{})
	res, err := rw.Rewrite(context.Background(), "SELECT a FROM db1.t", "")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if res.SQL != "SELECT a FROM phys.db1_t" {
		t.Errorf("SQL = %q", res.SQL)
	}
	if res.TableRewrites["db1.t"] != "phys.db1_t" {
		t.Errorf("TableRewrites = %v", res.TableRewrites)
	}
	if be.lastReq.GetSql() != "SELECT a FROM db1.t" {
		t.Errorf("backend saw %q", be.lastReq.GetSql())
	}
}

func TestSentioRewriter_UnsupportedForwardsOriginal(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:    pb.RewriteCode_UnsupportedStatement,
		Message: "nope",
	}}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{})
	res, err := rw.Rewrite(context.Background(), "OPTIMIZE TABLE x", "")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if res.SQL != "OPTIMIZE TABLE x" {
		t.Errorf("SQL = %q, want original", res.SQL)
	}
}

func TestSentioRewriter_RejectIsAnError(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:    pb.RewriteCode_SyntaxError,
		Message: "parse failed",
	}}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{})
	if _, err := rw.Rewrite(context.Background(), "garbage((", ""); err == nil || !strings.Contains(err.Error(), "parse failed") {
		t.Fatalf("err = %v, want rewriter rejection", err)
	}
}

func TestSentioRewriter_BackendErrorPropagates(t *testing.T) {
	be := &fakeBackend{err: errors.New("transport down")}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{})
	if _, err := rw.Rewrite(context.Background(), "SELECT 1", ""); err == nil {
		t.Fatal("want error when backend fails (caller is fail-open)")
	}
}

func TestSentioRewriter_UseMirrorsLogicalDatabase(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:             pb.RewriteCode_Success,
		SqlAfterRewrite:  "USE phys",
		StatementType:    pb.StatementType_STATEMENT_TYPE_USE,
		DatabaseRewrites: map[string]string{"db1": "phys"},
	}}
	sess := &fakeSession{}
	rw := newFakeFactory(be).NewRewriter(sess)
	if _, err := rw.Rewrite(context.Background(), "USE db1", ""); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if len(sess.setLogical) != 1 || sess.setLogical[0] != "db1" {
		t.Errorf("SetLogicalDatabase calls = %v, want [db1]", sess.setLogical)
	}
}

func TestNewSentioNetworkFactory_UnknownEngine(t *testing.T) {
	_, err := NewSentioNetworkFactory(Options{Engine: "carrier-pigeon"}, network.NewInMemoryNetworkState())
	if err == nil || !strings.Contains(err.Error(), "carrier-pigeon") {
		t.Fatalf("err = %v, want unknown-engine rejection", err)
	}
}
```

- [ ] **Step B3.2: Run to verify failure**

```bash
bazel run //:gazelle -- pkg/rewriter
bazel test //pkg/rewriter:rewriter_test --test_output=errors
```

Expected: compile FAILURE — `undefined: backend`, `unknown field backend in struct literal`.

- [ ] **Step B3.3: Create `pkg/rewriter/backend.go`**

```go
package rewriter

import (
	"context"
	"fmt"
	"time"

	rewritergo "github.com/housegate/rewriter-go"
	pb "github.com/housegate/rewriter-go/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"housegate/housegate/pkg/log"
)

// Engine values for Options.Engine, selecting which backend
// NewSentioNetworkFactory constructs. Empty means EngineGRPC.
const (
	EngineGRPC   = "grpc"
	EngineNative = "native"
)

// backend abstracts the rewrite transport: the remote sql-rewriter gRPC
// service or the in-process rewriter-go engine. Both speak the same proto
// contract; sentioRewriter cannot tell them apart. All per-session logic
// (dynamic args, USE mirroring, the fail-open code trichotomy) lives
// above this seam and is shared by both implementations.
type backend interface {
	Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error)
	RewriteErrorMessage(ctx context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error)
	Close() error
}

// grpcBackend is the historical default: a shared client connection to
// the external sql-rewriter service.
type grpcBackend struct {
	conn   *grpc.ClientConn
	client pb.RewriterServiceClient
}

// newGRPCBackend dials the sql-rewriter service synchronously and fails
// fast if it cannot connect — the proxy treats a missing rewriter as
// "rewriting disabled" rather than retrying forever.
func newGRPCBackend(opts Options) (*grpcBackend, error) {
	if opts.ServiceAddr == "" {
		return nil, fmt.Errorf("rewriter service_addr is required when rewriter engine is %q", EngineGRPC)
	}
	kaParams := keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             5 * time.Second,
		PermitWithoutStream: true,
	}
	connectTimeout := opts.Timeout
	if connectTimeout == 0 {
		connectTimeout = 10 * time.Second
	}
	connectCtx, connectCancel := context.WithTimeout(context.Background(), connectTimeout)
	defer connectCancel()

	conn, err := grpc.DialContext(connectCtx, opts.ServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kaParams),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rewriter service at %s: %w", opts.ServiceAddr, err)
	}
	log.Infow("connected to rewriter service", "service_addr", opts.ServiceAddr)
	return &grpcBackend{conn: conn, client: pb.NewRewriterServiceClient(conn)}, nil
}

func (b *grpcBackend) Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	return b.client.Rewrite(ctx, req)
}

func (b *grpcBackend) RewriteErrorMessage(ctx context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error) {
	return b.client.RewriteErrorMessage(ctx, req)
}

func (b *grpcBackend) Close() error { return b.conn.Close() }

// newNativeBackend loads the in-process rewriter-go engine.
// *rewritergo.Service satisfies backend directly (the signatures mirror
// the gRPC client by construction). The FFI library resolution order is
// opts.NativeLibraryPath, then POLYGLOT_SQL_FFI_PATH, then the system
// default locations.
func newNativeBackend(opts Options) (backend, error) {
	svc, err := rewritergo.NewService(opts.NativeLibraryPath)
	if err != nil {
		return nil, fmt.Errorf("load native rewriter (lib=%q): %w", opts.NativeLibraryPath, err)
	}
	log.Infow("native rewriter engine loaded", "lib", opts.NativeLibraryPath)
	return svc, nil
}

// newBackend dispatches on Options.Engine ("" defaults to grpc).
func newBackend(opts Options) (backend, error) {
	switch opts.Engine {
	case "", EngineGRPC:
		return newGRPCBackend(opts)
	case EngineNative:
		return newNativeBackend(opts)
	default:
		return nil, fmt.Errorf("unknown rewriter engine %q (want %q or %q)", opts.Engine, EngineGRPC, EngineNative)
	}
}
```

- [ ] **Step B3.4: Rewire `sentio.go` onto the backend**

Four edits, all in `pkg/rewriter/sentio.go`:

1. Struct fields — replace `grpcConn *grpc.ClientConn` and `grpcClient pb.RewriterServiceClient` with:

```go
	backend      backend // transport: grpc service or in-process rewriter-go
```

2. `NewSentioNetworkFactory` — the body between the callback-addr resolution and the return currently dials gRPC inline. Replace everything from `kaParams := ...` through the `log.Infow("connected to rewriter service", ...)` with:

```go
	be, err := newBackend(opts)
	if err != nil {
		return nil, err
	}
```

and the return becomes:

```go
	return &SentioNetworkFactory{
		options:      opts,
		registry:     reg,
		backend:      be,
		callbackAddr: callbackAddr,
	}, nil
```

Also delete the now-unused `"google.golang.org/grpc"`, `"google.golang.org/grpc/credentials/insecure"`, `"google.golang.org/grpc/keepalive"` imports and the `ServiceAddr == ""` early-return (moved into `newGRPCBackend`).

3. `Close`:

```go
func (f *SentioNetworkFactory) Close() error {
	if f.backend == nil {
		return nil
	}
	return f.backend.Close()
}
```

4. The two call sites:

```go
// in callWithTimeout:
return r.factory.backend.Rewrite(ctxWithTimeout, req)
// in RewriteErrorMessage:
resp, err := r.factory.backend.RewriteErrorMessage(ctxWithTimeout, req)
```

- [ ] **Step B3.5: Add the Options fields (`pkg/rewriter/types.go`)**

In the `Options` struct, after `ServiceAddr`:

```go
	// Engine selects the rewrite backend: EngineGRPC ("" included) calls
	// the external sql-rewriter service at ServiceAddr; EngineNative runs
	// the in-process rewriter-go engine and ignores ServiceAddr.
	Engine string

	// NativeLibraryPath locates libpolyglot_sql_ffi.{so,dylib} for the
	// native engine. Empty falls back to the POLYGLOT_SQL_FFI_PATH env
	// var, then polyglot's standard install locations. Unused by grpc.
	NativeLibraryPath string
```

- [ ] **Step B3.6: Run tests, expect green**

```bash
bazel run //:gazelle -- pkg/rewriter
bazel test //pkg/rewriter:rewriter_test --test_output=errors
bazel build //...
```

Expected: all PASS (the new backend_test cases plus the existing buildDatabaseMap tests).

- [ ] **Step B3.7: Commit**

```bash
git add pkg/rewriter/
git commit -m "feat(rewriter): backend seam — grpc service or in-process rewriter-go

SentioNetworkFactory now holds a 2-method backend instead of a raw
gRPC client; Options.Engine (grpc|native) picks the implementation.
sentioRewriter's request/response handling gains its first unit tests
via a fake backend."
```

### Task B4: Config surface + buildRewriterFactory dispatch

**Files:**
- Modify: `pkg/plugins/rewrite/config.go`
- Modify: `pkg/config/config.go` (defaults block ~line 408, Validate)
- Modify: `build.go` (`buildRewriterFactory`)
- Test: `pkg/config/config_test.go` (extend the existing validate tests)

- [ ] **Step B4.1: Write the failing validate test**

In `pkg/config/config_test.go`, find the existing `TestConfigValidate` table/style and add a case in the same shape (adapt field scaffolding to neighboring cases — only the rewriter fields matter):

```go
	t.Run("rewriter_engine_invalid", func(t *testing.T) {
		cfg := validServerConfig() // or however neighboring cases build a passing server config
		cfg.Rewriter.Engine = "carrier-pigeon"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "rewriter.engine") {
			t.Fatalf("err = %v, want rewriter.engine rejection", err)
		}
	})

	t.Run("rewriter_engine_native_ok", func(t *testing.T) {
		cfg := validServerConfig()
		cfg.Rewriter.Engine = "native"
		if err := cfg.Validate(); err != nil && strings.Contains(err.Error(), "rewriter.engine") {
			t.Fatalf("native must be a valid engine, got %v", err)
		}
	})
```

```bash
bazel test //pkg/config:config_test --test_filter='TestConfigValidate' --test_output=errors
```

Expected: FAIL — `cfg.Rewriter.Engine` undefined (field doesn't exist yet) or the invalid value passes Validate.

- [ ] **Step B4.2: Add the config fields**

`pkg/plugins/rewrite/config.go`, after `ServiceAddr`:

```go
	// Engine selects the rewriter implementation: "grpc" (default, also
	// the empty value) calls the external sql-rewriter service at
	// ServiceAddr; "native" runs the in-process rewriter-go engine and
	// ignores ServiceAddr.
	Engine string `json:"engine" yaml:"engine"`

	// NativeLibraryPath is the path to libpolyglot_sql_ffi.{so,dylib}
	// for the native engine. Empty falls back to the
	// POLYGLOT_SQL_FFI_PATH env var, then standard install locations.
	NativeLibraryPath string `json:"native_library_path" yaml:"native_library_path"`
```

- [ ] **Step B4.3: Defaults + validation in `pkg/config/config.go`**

In the defaults block (the `Rewriter: rewrite.Config{...}` literal around line 408):

```go
		Rewriter: rewrite.Config{
			ServiceAddr: EnvOrDefault("HOUSEGATE_REWRITER_ADDR", "localhost:50051"),
			Engine:      EnvOrDefault("HOUSEGATE_REWRITER_ENGINE", ""),
			Timeout:     Duration{5 * time.Second},
		},
```

In `Validate()`, in the cross-mode section (after the mode switch — engine validity is mode-independent):

```go
	switch c.Rewriter.Engine {
	case "", rewriter.EngineGRPC, rewriter.EngineNative:
	default:
		errs = append(errs, fmt.Errorf("rewriter.engine %q is invalid (want %q or %q)",
			c.Rewriter.Engine, rewriter.EngineGRPC, rewriter.EngineNative))
	}
```

Add `"housegate/housegate/pkg/rewriter"` to config.go's imports (no cycle: pkg/rewriter does not import pkg/config).

- [ ] **Step B4.4: Forward through `buildRewriterFactory` (`build.go`)**

In the `rewriter.Options` literal add the two fields, and put the engine in the success log:

```go
	rwConfig := rewriter.Options{
		Enabled:           true,
		ServiceAddr:       cfg.Rewriter.ServiceAddr,
		Engine:            cfg.Rewriter.Engine,
		NativeLibraryPath: cfg.Rewriter.NativeLibraryPath,
		Upstream:          cfg.Upstream,
		Listen:            cfg.Listen,
		CallbackAddr:      cfg.CallbackUrl,
		Timeout:           cfg.Rewriter.Timeout.Duration,
		PhysicalDatabase:  cfg.Rewriter.PhysicalDatabase,
		AuthEnabled:       cfg.Auth.Enabled,
		Delim:             cfg.Rewriter.Delimiter,
	}
	rwf, err := rewriter.NewSentioNetworkFactory(rwConfig, reg)
	if err != nil {
		log.Warne(err, "failed to create rewriter factory, rewriting disabled")
		return nil
	}
	log.Infow("SQL rewriter enabled",
		"engine", cfg.Rewriter.Engine,
		"service_addr", cfg.Rewriter.ServiceAddr,
		"upstream", cfg.Upstream,
		"physical_database", cfg.Rewriter.PhysicalDatabase,
	)
```

(The warn-and-disable posture already covers the native engine: a missing FFI library surfaces as `NewSentioNetworkFactory` → `newNativeBackend` error → rewriting disabled, symmetric with a failed gRPC dial.)

- [ ] **Step B4.5: Run tests, expect green**

```bash
bazel run //:gazelle
bazel test //pkg/config:config_test --test_filter='TestConfigValidate' --test_output=errors
bazel build //...
```

Expected: PASS. (If the config test target has a different name, find it with `bazel query 'tests(//pkg/config:all)'`.)

- [ ] **Step B4.6: Commit**

```bash
git add pkg/plugins/rewrite/config.go pkg/config/ build.go
git commit -m "feat(config): rewriter.engine selects grpc or native rewriter

engine: grpc|native (default grpc, env HOUSEGATE_REWRITER_ENGINE) plus
native_library_path for the FFI lib. Native load failure keeps the
existing warn-and-disable fail-open posture."
```

### Task B5: Native engine smoke test (FFI-gated)

**Files:**
- Create: `pkg/rewriter/native_smoke_test.go`

- [ ] **Step B5.1: Write the smoke test**

```go
package rewriter

import (
	"context"
	"os"
	"strings"
	"testing"

	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/sqlmeta"
)

// TestNativeEngineSmoke drives the real in-process rewriter-go engine
// through the full factory path: dynamic args from network state →
// backend → RewriteResult. Skips when the polyglot FFI library is not
// available (CI); run locally with:
//
//	bazel test //pkg/rewriter:rewriter_test \
//	  --test_filter=TestNativeEngineSmoke \
//	  --test_env=POLYGLOT_SQL_FFI_PATH=$HOME/Dev/housegate/rewriter-go/third_party/lib/libpolyglot_sql_ffi.dylib
func TestNativeEngineSmoke(t *testing.T) {
	if os.Getenv("POLYGLOT_SQL_FFI_PATH") == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; native engine FFI lib unavailable")
	}
	st := network.NewInMemoryNetworkState()
	st.DatabaseInfos["db1"] = network.DatabaseInfo{DatabaseId: "db1"}

	f, err := NewSentioNetworkFactory(Options{
		Engine:           EngineNative,
		PhysicalDatabase: "phys",
		Listen:           ":9000",
	}, st)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory(native): %v", err)
	}
	defer f.Close()

	rw := f.NewRewriter(&fakeSession{})
	res, err := rw.Rewrite(context.Background(), "SELECT a FROM db1.t", "")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if res.StatementType != sqlmeta.StatementType(3) { // STATEMENT_TYPE_SELECT; use the named sqlmeta constant if one exists
		t.Errorf("StatementType = %v, want SELECT", res.StatementType)
	}
	if !strings.Contains(res.SQL, "db1_t") {
		t.Errorf("SQL = %q, want dynamic rename to contain db1_t", res.SQL)
	}
}
```

Before finalizing, check `pkg/sqlmeta` for the named SELECT constant (`grep -rn "Select\|SELECT" pkg/sqlmeta/*.go | head`) and use it instead of the numeric cast — the numeric form is a fallback only if no named constant exists.

- [ ] **Step B5.2: Run both ways**

```bash
# CI shape — must SKIP, not fail:
bazel test //pkg/rewriter:rewriter_test --test_filter=TestNativeEngineSmoke --test_output=all
# Local shape — must PASS (adjust the dylib path/extension to the machine):
bazel test //pkg/rewriter:rewriter_test --test_filter=TestNativeEngineSmoke --test_output=all \
  --test_env=POLYGLOT_SQL_FFI_PATH=/Users/uranuswch/Dev/housegate/rewriter-go/third_party/lib/libpolyglot_sql_ffi.dylib
```

Expected: first run SKIPPED; second run PASSED with the rewritten SQL containing `phys.db1_t`.

- [ ] **Step B5.3: Commit**

```bash
git add pkg/rewriter/native_smoke_test.go
git commit -m "test(rewriter): native-engine smoke test through the full factory path"
```

### Task B6: Documentation

**Files:**
- Modify: `CLAUDE.md` (4 spots)
- Modify: `README.md` (rewriter config block, if one exists — check first)

- [ ] **Step B6.1: CLAUDE.md updates**

1. **§4 "SQL rewriter is a separate gRPC service"** — retitle to "SQL rewriting is delegated (gRPC service or in-process rewriter-go)" and add after the first sentence: the engine is selected by `rewriter.engine` (`grpc` default = external service at `service_addr`; `native` = in-process [rewriter-go](https://github.com/housegate/rewriter-go) via the polyglot FFI lib located by `native_library_path` / `POLYGLOT_SQL_FFI_PATH`). Both engines speak the identical proto contract through a `backend` seam inside `SentioNetworkFactory`; everything downstream is engine-agnostic.
2. **Key Modules `protos/` row** — replace with: the proto contract now lives in the `github.com/housegate/rewriter-go` module (`gen/pb`, vendored from rewriter-grpc); housegate has no generated proto code of its own.
3. **Known Rough Edges** — update the "`go test ./...` does NOT work" / protos-init-panic claims to match what Step B2.5 actually observed; if the panic is gone, delete the rough-edge bullet and soften the Build & Test warning accordingly.
4. **Build & Test** — no command changes, but if B2.5 showed `go test` now works for leaf packages, note that Bazel remains ground truth.

- [ ] **Step B6.2: README config example (conditional)**

```bash
grep -n "rewriter" README.md | head
```

If README documents the `rewriter` config block, add `engine` + `native_library_path` lines mirroring the config.go comments. If it doesn't, skip.

- [ ] **Step B6.3: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "docs: document selectable rewriter engine (grpc | native)"
```

### Task B7: Full verification

- [ ] **Step B7.1: Full build + test sweep**

```bash
bazel build //...
bazel test //... --test_output=errors
```

- [ ] **Step B7.2: Diff the failing set against main**

Per CLAUDE.md: integration targets are `manual`-tagged and external-service tests (`rewriter_e2e_test.go` on :50051) may fail locally. Compare:

```bash
git stash list >/dev/null  # ensure clean tree
git checkout main && bazel test //... --test_output=errors 2>&1 | grep -E "FAILED|TIMEOUT" | sort > /tmp/baseline.txt
git checkout feat/native-rewriter-engine && bazel test //... --test_output=errors 2>&1 | grep -E "FAILED|TIMEOUT" | sort > /tmp/branch.txt
diff /tmp/baseline.txt /tmp/branch.txt
```

Expected: no diff (matching failure sets = no regression).

- [ ] **Step B7.3: Local end-to-end native smoke (operator-shaped)**

Re-run the B5.2 local-shape command once more from a clean state, plus the rewriter-go suite:

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go && go test ./... && cd -
```

Expected: PASS both sides. The work is then ready for two PRs (rewriter-go `feat/service-api` first; housegate `feat/native-rewriter-engine` second, mentioning the re-pin-after-squash caveat from A4.2).

---

## Self-review notes (already applied)

- Spec §5 (Service + doRewrite + parity test) → Tasks A1–A3. Spec §6.1 (pb swap, protos deletion, gazelle, rough-edge check) → Tasks B1–B2. Spec §6.2 (backend seam, two call sites, Options fields) → Task B3. Spec §6.3 (config, env, validate, warn-and-disable, log engine field) → Task B4. Spec §7 testing rows → A2/A3 (rewriter-go), B3 (fake backend), B5 (smoke), B7 (baseline diff). Spec §10.1 probe → Step B2.1 (best-effort, sandbox may force by-inspection fallback). Spec §8/§9 are operational notes — no code tasks by design.
- Type consistency: `backend` / `newBackend` / `EngineGRPC` / `EngineNative` / `Options.Engine` / `Options.NativeLibraryPath` / `rewrite.Config.Engine` / `rewrite.Config.NativeLibraryPath` are used with identical spelling across B3–B5; `doRewrite` / `classify` / `Service` across A1–A3.
- Known soft spots called out inline rather than hidden: exact `reverse.Invert` output text (A3.2 adjusts assertions, never the library), the sqlmeta SELECT constant name (B5.1 instructs to resolve it), the config test scaffold name (B4.1/B4.5 instruct to adapt to the existing table), README rewriter block existence (B6.2 conditional).
