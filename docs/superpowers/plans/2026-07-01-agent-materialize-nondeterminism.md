# Agent-Mode Non-Determinism Materialization (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** In agent mode, rewrite non-deterministic SQL functions (`now()`/`rand()`/`generateUUIDv4()`/…) to constants via rewriter-go `MaterializeSQL` before the agent signs the query — flag-gated, off by default, backend selectable (native or grpc).

**Architecture:** A new `QueryPlugin` (`pkg/plugins/materialize`) runs first in the agent-mode chain and rewrites `qctx.Query.Body` in place, so both the signed bytes and the forwarded bytes are the materialized SQL. It calls a thin `Materializer` wrapper added to `pkg/rewriter`, which reuses the existing grpc/native `backend` seam and supplies `now`/random/uuid inputs per call. Runtime failures fall open (original SQL); startup misconfiguration fails fast.

**Tech Stack:** Go, Bazel 8.5.1 + Bzlmod, `github.com/housegate/rewriter-go` v0.3.0 (`pb` proto contract), `github.com/google/uuid`, Prometheus client.

## Global Constraints

- **Bazel is the ground truth for tests.** Build with `bazel build //...`; test with `bazel test //...`. After dependency changes run `bazel mod tidy && bazel run //:gazelle`.
- **Dependency floor:** `github.com/housegate/rewriter-go` must be `v0.3.0` (the first version exposing `MaterializeSQL`). Promote `github.com/google/uuid` to a direct dependency.
- **Plugins MUST NOT import `pkg/proxy`.** The plugin depends on leaf packages (`pkg/plugin`, `pkg/rewriter`) only. The metrics `Observer` is a narrow interface the plugin declares; `*proxy.MetricsObserver` satisfies it from the wiring side.
- **English only** for identifiers, comments, and operator-facing log/error strings.
- **Error policy:** runtime = fail-open (any transport error or non-`Success` code leaves the body unchanged, logs warn, increments a metric, never rejects the query); startup = fail-fast (`materialize.enabled` true but backend unconstructable → `buildAgent` returns an error).
- **Markdown docs: no hard line-wrapping** (one paragraph per line).
- **Baseline discipline:** `pkg/rewriter:rewriter_test` has pre-existing failures needing an external `localhost:50051` rewriter, and one flaky proxy lifecycle test. Diff your failing set against a clean `main` Bazel build; matching set = no regression.

---

### Task 1: Bump rewriter-go to v0.3.0

**Files:**
- Modify: `go.mod` (rewriter-go version; uuid promoted to direct by tidy)
- Modify: `go.sum` (via tidy)
- Modify: `MODULE.bazel` (via `bazel mod tidy`)

**Interfaces:**
- Consumes: nothing.
- Produces: the symbols `pb.MaterializeSQLRequest`, `pb.MaterializeSQLResponse`, `pb.MaterializationInputs`, `pb.MaterializationPolicy`, `pb.MaterializedReplacement`, `pb.MaterializeCode_*`, and the `RewriterServiceClient.MaterializeSQL` method / `rewritergo.Service.MaterializeSQL` method — relied on by Task 2.

- [ ] **Step 1: Bump the module and tidy**

Run:
```bash
cd /Users/uranuswch/Dev/housegate/housegate
go get github.com/housegate/rewriter-go@v0.3.0
go mod tidy
```
Expected: `go.mod` now shows `github.com/housegate/rewriter-go v0.3.0`. (`google/uuid` stays `// indirect` until Task 3 imports it directly, then a later tidy reclassifies it — do not hand-edit.)

- [ ] **Step 2: Confirm the MaterializeSQL symbols resolve**

Run:
```bash
go doc github.com/housegate/rewriter-go/gen/pb.MaterializeSQLRequest
go doc github.com/housegate/rewriter-go.Service.MaterializeSQL
```
Expected: both print type/method docs (no "no such symbol"). This confirms v0.3.0 is wired.

- [ ] **Step 3: Sync Bazel module graph**

Run:
```bash
bazel mod tidy
bazel run //:gazelle
```
Expected: `MODULE.bazel` `use_repo(go_deps, …)` still lists `com_github_housegate_rewriter_go`; no error. (If `bazel mod tidy` rewrites the pin, that is expected.)

- [ ] **Step 4: Build the rewriter package**

Run: `bazel build //pkg/rewriter:rewriter`
Expected: SUCCESS (the existing package still compiles against v0.3.0).

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum MODULE.bazel
git commit -m "build(deps): bump rewriter-go to v0.3.0 for MaterializeSQL"
```

---

### Task 2: Add the Materializer seam to `pkg/rewriter`

**Files:**
- Modify: `pkg/rewriter/backend.go` (add `MaterializeSQL` to the `backend` interface + `grpcBackend` impl)
- Create: `pkg/rewriter/materialize.go` (`Materializer`, `MaterializeOutcome`, `NewMaterializer`, `sentioMaterializer`, input generators)
- Modify: `pkg/rewriter/backend_test.go` (add `MaterializeSQL` to `fakeBackend`)
- Create: `pkg/rewriter/materialize_test.go` (unit tests via `fakeBackend`)
- Modify: `pkg/rewriter/BUILD.bazel` (add `materialize.go` to srcs, `materialize_test.go` to test srcs, `@com_github_google_uuid//` dep)

**Interfaces:**
- Consumes: `pb.MaterializeSQLRequest/Response/…` (Task 1); the existing unexported `backend` interface and `newBackend(Options)` in `pkg/rewriter/backend.go`; the existing `Options` struct in `pkg/rewriter/types.go` (fields used: `Engine`, `ServiceAddr`, `NativeLibraryPath`, `Timeout`).
- Produces:
  - `type MaterializeOutcome struct { SQL string; Changed bool; Code pb.MaterializeCode; Message string }`
  - `type Materializer interface { Materialize(ctx context.Context, sql string) (MaterializeOutcome, error); Close() error }`
  - `func NewMaterializer(opts Options, poolSize int, profileID string) (Materializer, error)`
  — all relied on by Tasks 3 and 5.

- [ ] **Step 1: Add `MaterializeSQL` to the `fakeBackend` test double**

In `pkg/rewriter/backend_test.go`, add a captured-request field and the method. The struct currently is:
```go
type fakeBackend struct {
	resp       *pb.RewriteSQLResponse
	err        error
	lastReq    *pb.RewriteSQLRequest
	lastErrReq *pb.RewriteErrorMessageRequest
}
```
Extend it to:
```go
type fakeBackend struct {
	resp       *pb.RewriteSQLResponse
	err        error
	lastReq    *pb.RewriteSQLRequest
	lastErrReq *pb.RewriteErrorMessageRequest

	matResp    *pb.MaterializeSQLResponse
	matErr     error
	lastMatReq *pb.MaterializeSQLRequest
}

func (f *fakeBackend) MaterializeSQL(_ context.Context, req *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error) {
	f.lastMatReq = req
	return f.matResp, f.matErr
}
```

- [ ] **Step 2: Write the failing Materializer tests**

Create `pkg/rewriter/materialize_test.go`:
```go
package rewriter

import (
	"context"
	"errors"
	"testing"

	pb "github.com/housegate/rewriter-go/gen/pb"
)

func newTestMaterializer(be backend, poolSize int, profileID string) *sentioMaterializer {
	return &sentioMaterializer{be: be, poolSize: poolSize, profileID: profileID}
}

func TestMaterialize_SuccessWithReplacements(t *testing.T) {
	fb := &fakeBackend{matResp: &pb.MaterializeSQLResponse{
		Code:                    pb.MaterializeCode_MaterializeSuccess,
		SqlAfterMaterialization: "INSERT INTO t VALUES ('2026-07-01 00:00:00')",
		Replacements:            []*pb.MaterializedReplacement{{FunctionName: "now"}},
	}}
	m := newTestMaterializer(fb, 8, "")
	out, err := m.Materialize(context.Background(), "INSERT INTO t VALUES (now())")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !out.Changed || out.SQL != "INSERT INTO t VALUES ('2026-07-01 00:00:00')" {
		t.Fatalf("want changed materialized SQL, got %+v", out)
	}
	// Inputs must be populated so the engine can resolve now()/rand()/uuid.
	if fb.lastMatReq.GetInputs().GetNowUnixNs() == 0 {
		t.Fatalf("now_unix_ns not set")
	}
	if got := len(fb.lastMatReq.GetInputs().GetRandomUint64Values()); got != 8 {
		t.Fatalf("random_uint64 pool = %d, want 8", got)
	}
	if got := len(fb.lastMatReq.GetInputs().GetRandomFloat64Values()); got != 8 {
		t.Fatalf("random_float64 pool = %d, want 8", got)
	}
	if got := len(fb.lastMatReq.GetInputs().GetUuidValues()); got != 8 {
		t.Fatalf("uuid pool = %d, want 8", got)
	}
}

func TestMaterialize_SuccessNoReplacements(t *testing.T) {
	fb := &fakeBackend{matResp: &pb.MaterializeSQLResponse{
		Code:                    pb.MaterializeCode_MaterializeSuccess,
		SqlAfterMaterialization: "SELECT 1",
	}}
	m := newTestMaterializer(fb, 4, "")
	out, err := m.Materialize(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out.Changed {
		t.Fatalf("no replacements → Changed must be false, got %+v", out)
	}
	if out.SQL != "SELECT 1" {
		t.Fatalf("SQL should be original, got %q", out.SQL)
	}
}

func TestMaterialize_NonSuccessKeepsOriginal(t *testing.T) {
	fb := &fakeBackend{matResp: &pb.MaterializeSQLResponse{
		Code:    pb.MaterializeCode_MaterializeSyntaxError,
		Message: "parse error",
	}}
	m := newTestMaterializer(fb, 4, "")
	out, err := m.Materialize(context.Background(), "NOT SQL")
	if err != nil {
		t.Fatalf("non-success must not be an error (fail-open), got %v", err)
	}
	if out.Changed || out.SQL != "NOT SQL" {
		t.Fatalf("want original SQL unchanged, got %+v", out)
	}
	if out.Code != pb.MaterializeCode_MaterializeSyntaxError {
		t.Fatalf("code not propagated: %v", out.Code)
	}
}

func TestMaterialize_TransportErrorPropagates(t *testing.T) {
	fb := &fakeBackend{matErr: errors.New("grpc down")}
	m := newTestMaterializer(fb, 4, "")
	out, err := m.Materialize(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatalf("transport error must propagate so the caller falls open")
	}
	if out.SQL != "SELECT 1" {
		t.Fatalf("outcome SQL should be original on transport error, got %q", out.SQL)
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail to compile/pass**

Run: `bazel test //pkg/rewriter:rewriter_test --test_filter='TestMaterialize'`
Expected: FAIL — `sentioMaterializer` / `MaterializeSQL` undefined (interface method + wrapper not written yet). (You must add `materialize_test.go` to the test srcs in Step 6 before Bazel sees it; if so, the failure is a compile error, which counts as the red state.)

- [ ] **Step 4: Add `MaterializeSQL` to the `backend` interface and `grpcBackend`**

In `pkg/rewriter/backend.go`, extend the interface:
```go
type backend interface {
	Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error)
	RewriteErrorMessage(ctx context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error)
	MaterializeSQL(ctx context.Context, req *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error)
	Close() error
}
```
Add the `grpcBackend` method next to its `RewriteErrorMessage`:
```go
func (b *grpcBackend) MaterializeSQL(ctx context.Context, req *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error) {
	return b.client.MaterializeSQL(ctx, req)
}
```
(The native `*rewritergo.Service` already implements `MaterializeSQL` in v0.3.0, so `newNativeBackend` still satisfies the widened interface.)

- [ ] **Step 5: Write the Materializer wrapper**

Create `pkg/rewriter/materialize.go`:
```go
package rewriter

import (
	crand "crypto/rand"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
	pb "github.com/housegate/rewriter-go/gen/pb"
)

// MaterializeOutcome is the result of one materialization call. SQL is the
// text the caller should use downstream: the materialized SQL on Success,
// the original SQL on any non-Success code (fail-open). Changed is true
// only when the engine applied at least one replacement.
type MaterializeOutcome struct {
	SQL     string
	Changed bool
	Code    pb.MaterializeCode
	Message string
}

// Materializer rewrites non-deterministic functions in a SQL statement to
// literal constants (rewriter-go MaterializeSQL). It is the agent-side
// Phase-1 seam: SQL in → deterministic SQL out. Safe for concurrent use.
type Materializer interface {
	Materialize(ctx context.Context, sql string) (MaterializeOutcome, error)
	Close() error
}

// NewMaterializer builds a Materializer over the grpc/native backend
// selected by opts.Engine (reusing newBackend). poolSize is how many
// random/uuid values are supplied per call (must be > 0); profileID
// selects the materialization profile ("" → engine default).
func NewMaterializer(opts Options, poolSize int, profileID string) (Materializer, error) {
	if poolSize <= 0 {
		return nil, fmt.Errorf("materializer pool size must be > 0, got %d", poolSize)
	}
	be, err := newBackend(opts)
	if err != nil {
		return nil, err
	}
	return &sentioMaterializer{be: be, poolSize: poolSize, profileID: profileID}, nil
}

type sentioMaterializer struct {
	be        backend
	poolSize  int
	profileID string
}

func (m *sentioMaterializer) Materialize(ctx context.Context, sql string) (MaterializeOutcome, error) {
	now := time.Now().UnixNano()
	req := &pb.MaterializeSQLRequest{
		Sql: sql,
		Inputs: &pb.MaterializationInputs{
			NowUnixNs:           &now,
			RandomUint64Values:  randUint64Slice(m.poolSize),
			RandomFloat64Values: randFloat64Slice(m.poolSize),
			UuidValues:          uuidSlice(m.poolSize),
		},
		Policy: &pb.MaterializationPolicy{ProfileId: m.profileID},
	}
	resp, err := m.be.MaterializeSQL(ctx, req)
	if err != nil {
		return MaterializeOutcome{SQL: sql}, err
	}
	if resp.GetCode() != pb.MaterializeCode_MaterializeSuccess {
		return MaterializeOutcome{SQL: sql, Code: resp.GetCode(), Message: resp.GetMessage()}, nil
	}
	return MaterializeOutcome{
		SQL:     resp.GetSqlAfterMaterialization(),
		Changed: len(resp.GetReplacements()) > 0,
		Code:    resp.GetCode(),
	}, nil
}

func (m *sentioMaterializer) Close() error { return m.be.Close() }

// randUint64Slice / randFloat64Slice / uuidSlice generate the per-call
// input pools. The values need NOT be reproducible: the signed SQL carries
// the resolved constants, and replay reads those constants rather than
// re-materializing. crypto/rand is used for a good-quality, dependency-free
// source.
func randUint64Slice(n int) []uint64 {
	out := make([]uint64, n)
	var b [8]byte
	for i := range out {
		_, _ = crand.Read(b[:])
		out[i] = binary.LittleEndian.Uint64(b[:])
	}
	return out
}

func randFloat64Slice(n int) []float64 {
	out := make([]float64, n)
	var b [8]byte
	for i := range out {
		_, _ = crand.Read(b[:])
		// 53-bit mantissa → uniform in [0, 1).
		out[i] = float64(binary.LittleEndian.Uint64(b[:])>>11) / (1 << 53)
	}
	return out
}

func uuidSlice(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = uuid.NewString()
	}
	return out
}

var _ Materializer = (*sentioMaterializer)(nil)
```

- [ ] **Step 6: Register the new files in Bazel**

In `pkg/rewriter/BUILD.bazel`, add `"materialize.go"` to `go_library.srcs`, add `"materialize_test.go"` to `go_test.srcs`, and add `"@com_github_google_uuid//:uuid"` to `go_library.deps`. Then:
```bash
bazel run //:gazelle
```
Expected: gazelle keeps/normalizes the edits (it can also add the uuid dep automatically). If `@com_github_google_uuid` is unknown, run `bazel mod tidy` then re-run gazelle.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `bazel test //pkg/rewriter:rewriter_test --test_filter='TestMaterialize'`
Expected: PASS (4 tests).

- [ ] **Step 8: (Opt-in) Add a native smoke test**

Create `pkg/rewriter/materialize_smoke_test.go` (skips unless the FFI lib is provided, mirroring `native_smoke_test.go`):
```go
package rewriter

import (
	"context"
	"os"
	"strings"
	"testing"
)

// Runs the real native engine end-to-end. Skips unless POLYGLOT_SQL_FFI_PATH
// points at libpolyglot_sql_ffi.{so,dylib}.
func TestMaterialize_NativeSmoke(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; skipping native materialize smoke test")
	}
	m, err := NewMaterializer(Options{Engine: EngineNative, NativeLibraryPath: lib}, 16, "")
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	defer m.Close()
	out, err := m.Materialize(context.Background(), "SELECT now()")
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if !out.Changed || strings.Contains(strings.ToLower(out.SQL), "now(") {
		t.Fatalf("now() should have been materialized to a constant, got %q", out.SQL)
	}
}
```
Add `"materialize_smoke_test.go"` to `go_test.srcs`, run `bazel run //:gazelle`, then:
```bash
bazel test //pkg/rewriter:rewriter_test --test_filter='TestMaterialize_NativeSmoke'
```
Expected: SKIP (no FFI lib in the sandbox) — this is correct, not a failure.

- [ ] **Step 9: Commit**

```bash
git add pkg/rewriter/backend.go pkg/rewriter/materialize.go pkg/rewriter/materialize_test.go pkg/rewriter/materialize_smoke_test.go pkg/rewriter/backend_test.go pkg/rewriter/BUILD.bazel
git commit -m "feat(rewriter): add Materializer seam over grpc/native MaterializeSQL"
```

---

### Task 3: Create the `materialize` plugin

**Files:**
- Create: `pkg/plugins/materialize/materialize.go` (Plugin + hook + Observer/Materializer interfaces)
- Create: `pkg/plugins/materialize/config.go` (Config + Validate)
- Create: `pkg/plugins/materialize/materialize_test.go` (fake Materializer/Observer)
- Create: `pkg/plugins/materialize/BUILD.bazel`

**Interfaces:**
- Consumes: `rewriter.MaterializeOutcome` (Task 2); `plugin.QueryContext`, `plugin.QueryPlugin` (`pkg/plugin`); `chproto.Query` (test only).
- Produces:
  - `type Materializer interface { Materialize(ctx context.Context, sql string) (rewriter.MaterializeOutcome, error) }`
  - `type Observer interface { MaterializeApplied(); MaterializeNoop(); MaterializeNonSuccess(code string); MaterializeCallError() }`
  - `type Plugin struct { Materializer Materializer; PoolSize int; Observer Observer }` implementing `plugin.QueryPlugin`
  - `type Config struct { … }` (see Step 3) with `func (Config) Validate() error`
  — relied on by Tasks 4 and 5.

- [ ] **Step 1: Write the failing plugin test**

Create `pkg/plugins/materialize/materialize_test.go`:
```go
package materialize

import (
	"context"
	"errors"
	"testing"

	pb "github.com/housegate/rewriter-go/gen/pb"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/rewriter"
)

type fakeMat struct {
	out rewriter.MaterializeOutcome
	err error
}

func (f *fakeMat) Materialize(_ context.Context, _ string) (rewriter.MaterializeOutcome, error) {
	return f.out, f.err
}

type fakeObs struct{ applied, noop, callErr int; nonSuccess []string }

func (o *fakeObs) MaterializeApplied()               { o.applied++ }
func (o *fakeObs) MaterializeNoop()                  { o.noop++ }
func (o *fakeObs) MaterializeNonSuccess(code string) { o.nonSuccess = append(o.nonSuccess, code) }
func (o *fakeObs) MaterializeCallError()             { o.callErr++ }

func runOnQuery(t *testing.T, p *Plugin, body string) *plugin.QueryContext {
	t.Helper()
	qctx := &plugin.QueryContext{Query: &chproto.Query{Body: body}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery must never return an error (fail-open), got %v", err)
	}
	return qctx
}

func TestOnQuery_AppliedSwapsBody(t *testing.T) {
	obs := &fakeObs{}
	p := &Plugin{Materializer: &fakeMat{out: rewriter.MaterializeOutcome{
		SQL: "INSERT INTO t VALUES ('2026-07-01 00:00:00')", Changed: true,
		Code: pb.MaterializeCode_MaterializeSuccess,
	}}, Observer: obs}
	qctx := runOnQuery(t, p, "INSERT INTO t VALUES (now())")
	if qctx.Query.Body != "INSERT INTO t VALUES ('2026-07-01 00:00:00')" {
		t.Fatalf("body not swapped: %q", qctx.Query.Body)
	}
	if obs.applied != 1 {
		t.Fatalf("applied metric = %d, want 1", obs.applied)
	}
}

func TestOnQuery_NoopLeavesBody(t *testing.T) {
	obs := &fakeObs{}
	p := &Plugin{Materializer: &fakeMat{out: rewriter.MaterializeOutcome{
		SQL: "SELECT 1", Changed: false, Code: pb.MaterializeCode_MaterializeSuccess,
	}}, Observer: obs}
	qctx := runOnQuery(t, p, "SELECT 1")
	if qctx.Query.Body != "SELECT 1" {
		t.Fatalf("body changed on noop: %q", qctx.Query.Body)
	}
	if obs.noop != 1 {
		t.Fatalf("noop metric = %d, want 1", obs.noop)
	}
}

func TestOnQuery_NonSuccessFailsOpen(t *testing.T) {
	obs := &fakeObs{}
	p := &Plugin{Materializer: &fakeMat{out: rewriter.MaterializeOutcome{
		SQL: "NOT SQL", Changed: false, Code: pb.MaterializeCode_MaterializeSyntaxError,
	}}, Observer: obs}
	qctx := runOnQuery(t, p, "NOT SQL")
	if qctx.Query.Body != "NOT SQL" {
		t.Fatalf("body changed on non-success: %q", qctx.Query.Body)
	}
	if len(obs.nonSuccess) != 1 || obs.nonSuccess[0] != "SyntaxError" {
		t.Fatalf("nonSuccess metric = %v, want [SyntaxError]", obs.nonSuccess)
	}
}

func TestOnQuery_CallErrorFailsOpen(t *testing.T) {
	obs := &fakeObs{}
	p := &Plugin{Materializer: &fakeMat{err: errors.New("grpc down")}, Observer: obs}
	qctx := runOnQuery(t, p, "SELECT 1")
	if qctx.Query.Body != "SELECT 1" {
		t.Fatalf("body changed on call error: %q", qctx.Query.Body)
	}
	if obs.callErr != 1 {
		t.Fatalf("callErr metric = %d, want 1", obs.callErr)
	}
}

func TestOnQuery_NilMaterializerNoop(t *testing.T) {
	qctx := runOnQuery(t, &Plugin{}, "SELECT 1")
	if qctx.Query.Body != "SELECT 1" {
		t.Fatalf("nil materializer must be a clean no-op")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `bazel test //pkg/plugins/materialize:materialize_test`
Expected: FAIL — package/targets do not exist yet (write the plugin + BUILD in the next steps).

- [ ] **Step 3: Write the plugin and config**

Create `pkg/plugins/materialize/materialize.go`:
```go
// Package materialize implements the agent-mode Phase-1 plugin that
// rewrites non-deterministic SQL functions (now()/rand()/generateUUIDv4()/…)
// to constants before the agent signer runs, so the signed and forwarded
// SQL are identical and deterministic. Fail-open: any materialization
// failure leaves the query body unchanged.
package materialize

import (
	"context"

	pb "github.com/housegate/rewriter-go/gen/pb"

	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/rewriter"
)

// Materializer is the SQL→SQL seam this plugin depends on. *rewriter's
// Materializer implementation satisfies it; tests inject a fake.
type Materializer interface {
	Materialize(ctx context.Context, sql string) (rewriter.MaterializeOutcome, error)
}

// Observer is the narrow metrics surface. *proxy.MetricsObserver satisfies
// it. Optional — a nil Observer disables metrics.
type Observer interface {
	MaterializeApplied()
	MaterializeNoop()
	MaterializeNonSuccess(code string)
	MaterializeCallError()
}

// Plugin rewrites qctx.Query.Body in place before the agent signer runs.
// A nil Materializer makes it a no-op.
type Plugin struct {
	Materializer Materializer
	PoolSize     int
	Observer     Observer
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p.Materializer == nil || qctx.Query == nil || qctx.Query.Body == "" {
		return nil
	}
	_, logger := log.FromContext(ctx)
	out, err := p.Materializer.Materialize(ctx, qctx.Query.Body)
	if err != nil {
		if p.Observer != nil {
			p.Observer.MaterializeCallError()
		}
		logger.Warnw("materialize: call failed, forwarding original SQL", "err", err)
		return nil // fail-open
	}
	if out.Code != pb.MaterializeCode_MaterializeSuccess {
		if p.Observer != nil {
			p.Observer.MaterializeNonSuccess(out.Code.String())
		}
		logger.Warnw("materialize: engine non-success, forwarding original SQL",
			"code", out.Code.String(), "message", out.Message)
		return nil // fail-open
	}
	if out.Changed {
		qctx.Query.Body = out.SQL
		if p.Observer != nil {
			p.Observer.MaterializeApplied()
		}
		logger.Debugw("materialize: applied", "sql", out.SQL)
		return nil
	}
	if p.Observer != nil {
		p.Observer.MaterializeNoop()
	}
	return nil
}

var _ plugin.QueryPlugin = (*Plugin)(nil)
```

Create `pkg/plugins/materialize/config.go`:
```go
package materialize

import (
	"errors"
	"fmt"

	"housegate/housegate/pkg/cfgtypes"
	"housegate/housegate/pkg/rewriter"
)

// Config is the operator-tunable surface for the agent-mode materialize
// plugin. Read only in agent mode; default off.
type Config struct {
	// Enabled turns Phase-1 materialization on. Default false.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// Engine selects the backend: "grpc" dials ServiceAddr; "native" runs
	// the in-process rewriter-go engine. Required (no silent default) when
	// Enabled.
	Engine string `json:"engine" yaml:"engine"`

	// ServiceAddr is the sql-rewriter gRPC address (grpc engine).
	ServiceAddr string `json:"service_addr" yaml:"service_addr"`

	// NativeLibraryPath / NativeLibraryRelease / NativeLibrarySHA256 /
	// NativeLibraryReleaseBaseURL resolve the FFI library for the native
	// engine (same semantics as the rewriter block).
	NativeLibraryPath           string `json:"native_library_path" yaml:"native_library_path"`
	NativeLibraryRelease        string `json:"native_library_release" yaml:"native_library_release"`
	NativeLibrarySHA256         string `json:"native_library_sha256" yaml:"native_library_sha256"`
	NativeLibraryReleaseBaseURL string `json:"native_library_release_base_url" yaml:"native_library_release_base_url"`

	// Timeout caps each materialize call (grpc engine).
	Timeout cfgtypes.Duration `json:"timeout" yaml:"timeout"`

	// RandomPoolSize is how many random/uuid values are supplied per call.
	// <= 0 falls back to 16 at build time.
	RandomPoolSize int `json:"random_pool_size" yaml:"random_pool_size"`

	// ProfileID selects the materialization profile ("" → engine default).
	ProfileID string `json:"profile_id" yaml:"profile_id"`
}

// Validate is a no-op when disabled. When enabled it requires an explicit
// engine and (for grpc) a service address.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	var errs []error
	switch c.Engine {
	case rewriter.EngineGRPC:
		if c.ServiceAddr == "" {
			errs = append(errs, errors.New("materialize.service_addr is required when materialize.engine is \"grpc\""))
		}
	case rewriter.EngineNative:
		// ok
	case "":
		errs = append(errs, errors.New("materialize.engine is required when materialize.enabled (\"grpc\" or \"native\")"))
	default:
		errs = append(errs, fmt.Errorf("materialize.engine %q is invalid (want %q or %q)",
			c.Engine, rewriter.EngineGRPC, rewriter.EngineNative))
	}
	if c.RandomPoolSize < 0 {
		errs = append(errs, fmt.Errorf("materialize.random_pool_size must be >= 0, got %d", c.RandomPoolSize))
	}
	return errors.Join(errs...)
}
```

- [ ] **Step 4: Write the BUILD.bazel**

Create `pkg/plugins/materialize/BUILD.bazel`:
```python
load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "materialize",
    srcs = [
        "config.go",
        "materialize.go",
    ],
    importpath = "housegate/housegate/pkg/plugins/materialize",
    visibility = ["//visibility:public"],
    deps = [
        "//pkg/cfgtypes",
        "//pkg/log",
        "//pkg/plugin",
        "//pkg/rewriter",
        "@com_github_housegate_rewriter_go//gen/pb",
    ],
)

go_test(
    name = "materialize_test",
    srcs = ["materialize_test.go"],
    embed = [":materialize"],
    deps = [
        "//pkg/chproto",
        "//pkg/plugin",
        "//pkg/rewriter",
        "@com_github_housegate_rewriter_go//gen/pb",
    ],
)
```
Then run `bazel run //:gazelle` to normalize.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `bazel test //pkg/plugins/materialize:materialize_test`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add pkg/plugins/materialize/
git commit -m "feat(materialize): add agent-mode Phase-1 materialize plugin"
```

---

### Task 4: Wire the config block into root config

**Files:**
- Modify: `pkg/config/config.go` (import, `Config.Materialize` field, `Default()` seed, `Validate()` agent-mode gate)
- Modify: `pkg/config/config_test.go` (validation cases)
- Modify: `pkg/config/BUILD.bazel` (add `//pkg/plugins/materialize` dep to library + test)

**Interfaces:**
- Consumes: `materialize.Config` + `materialize.Config.Validate` (Task 3); the existing `Config.Mode()` and `ModeAgent` in `pkg/config/config.go`.
- Produces: `Config.Materialize materialize.Config` — relied on by Task 5.

- [ ] **Step 1: Write the failing config tests**

In `pkg/config/config_test.go`, add cases (adapt to the file's existing table/harness — it already has `Rewriter.Engine` cases to mirror). Use a valid agent-mode base config for each:
```go
func TestValidate_Materialize(t *testing.T) {
	base := func() *Config {
		c := Default()
		c.Agent.Mode = true
		c.Agent.PrivateKeyHex = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
		c.Agent.Upstream = "127.0.0.1:9000"
		return &c
	}
	t.Run("enabled without engine is rejected", func(t *testing.T) {
		c := base()
		c.Materialize.Enabled = true
		if err := c.Validate(); err == nil {
			t.Fatal("want error: engine required when enabled")
		}
	})
	t.Run("grpc without service_addr is rejected", func(t *testing.T) {
		c := base()
		c.Materialize.Enabled = true
		c.Materialize.Engine = "grpc"
		c.Materialize.ServiceAddr = ""
		if err := c.Validate(); err == nil {
			t.Fatal("want error: service_addr required for grpc")
		}
	})
	t.Run("native enabled validates", func(t *testing.T) {
		c := base()
		c.Materialize.Enabled = true
		c.Materialize.Engine = "native"
		if err := c.Validate(); err != nil {
			t.Fatalf("native enabled should validate: %v", err)
		}
	})
	t.Run("disabled block ignored", func(t *testing.T) {
		c := base()
		c.Materialize = materializeplugin.Config{} // all zero, Enabled=false
		if err := c.Validate(); err != nil {
			t.Fatalf("disabled materialize must not affect validation: %v", err)
		}
	})
	t.Run("default pool size is 16", func(t *testing.T) {
		if got := Default().Materialize.RandomPoolSize; got != 16 {
			t.Fatalf("default RandomPoolSize = %d, want 16", got)
		}
	})
}
```
Add the import alias to the test file if needed: `materializeplugin "housegate/housegate/pkg/plugins/materialize"`.

- [ ] **Step 2: Run to verify failure**

Run: `bazel test //pkg/config:config_test --test_filter='TestValidate_Materialize'`
Expected: FAIL — `Config.Materialize` field / import does not exist.

- [ ] **Step 3: Add the field, default, and validation gate**

In `pkg/config/config.go`:

Add the import (with the other plugin imports around lines 33-40):
```go
materializeplugin "housegate/housegate/pkg/plugins/materialize"
```
Add the struct field next to `Rewriter` (line ~136):
```go
Materialize materializeplugin.Config `json:"materialize" yaml:"materialize"`
```
Seed the default in `Default()` (next to the `Rewriter:` block, ~line 432):
```go
Materialize: materializeplugin.Config{
	Timeout:        Duration{5 * time.Second},
	RandomPoolSize: 16,
},
```
Add the agent-mode gate in `Validate()` (near the existing `Rewriter.Engine` switch, ~line 357). It must run only in agent mode so a stray block in a server config is ignored:
```go
if c.Mode() == ModeAgent {
	if err := c.Materialize.Validate(); err != nil {
		errs = append(errs, err)
	}
}
```
(Confirm the agent-mode constant is spelled `ModeAgent`; match the file. If `Validate()` collects into `errs []error` before `errors.Join`, append there as shown.)

- [ ] **Step 4: Update config BUILD deps**

In `pkg/config/BUILD.bazel`, add `"//pkg/plugins/materialize"` to the `deps` of both the `config` library and the `config_test` test. Then `bazel run //:gazelle`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `bazel test //pkg/config:config_test`
Expected: PASS (including the new `TestValidate_Materialize` subtests; the rest of the suite unchanged).

- [ ] **Step 6: Commit**

```bash
git add pkg/config/config.go pkg/config/config_test.go pkg/config/BUILD.bazel
git commit -m "feat(config): add agent-mode materialize block (default off)"
```

---

### Task 5: Metrics + build.go wiring

**Files:**
- Modify: `pkg/proxy/observer.go` (counter vec + 4 `MetricsObserver` methods)
- Modify: `build.go` (extract `resolveNativeLibraryPath`, add `buildMaterializer`, insert plugin into `buildAgent` chain + teardown)
- Modify: `BUILD.bazel` (root — add `//pkg/plugins/materialize` dep)

**Interfaces:**
- Consumes: `materialize.Plugin`, `materialize.Config` (Task 3); `rewriter.NewMaterializer`, `rewriter.Materializer` (Task 2); the existing `buildAgent` shape (`obs := proxy.NewMetricsObserver()`, the `QueryPlugins` slice, `builtServer.teardown`), and `ffifetch.Fetch` already imported in `build.go`.
- Produces: nothing downstream (final integration task).

- [ ] **Step 1: Add the materialize metric to `MetricsObserver`**

In `pkg/proxy/observer.go`, add a counter vec to the `var (...)` block (next to the agent counters, ~line 60):
```go
agentMaterializeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "clickhouse_proxy_agent_materialize_total",
	Help: "Agent-mode Phase-1 materialization outcomes",
}, []string{"result", "code"})
```
Register it in `init()` (next to the agent registrations, ~line 78):
```go
prometheus.MustRegister(agentMaterializeTotal)
```
Add the four methods (next to `AgentBootstrapFallback`, ~line 141):
```go
func (m *MetricsObserver) MaterializeApplied() { agentMaterializeTotal.WithLabelValues("applied", "").Inc() }
func (m *MetricsObserver) MaterializeNoop()    { agentMaterializeTotal.WithLabelValues("noop", "").Inc() }
func (m *MetricsObserver) MaterializeNonSuccess(code string) {
	agentMaterializeTotal.WithLabelValues("non_success", code).Inc()
}
func (m *MetricsObserver) MaterializeCallError() { agentMaterializeTotal.WithLabelValues("call_error", "").Inc() }
```

- [ ] **Step 2: Verify observer compiles and satisfies the plugin interface**

Add a compile-time assertion in `build.go` (near the top-level `var _ …` assertions, or inside `buildAgent`) to lock the contract:
```go
var _ materialize.Observer = (*proxy.MetricsObserver)(nil)
```
Run: `bazel build //pkg/proxy:proxy`
Expected: SUCCESS.

- [ ] **Step 3: Extract the shared native-library resolver**

In `build.go`, factor the inline release-fetch logic (currently inside `buildRewriterFactory`) into a reusable helper. Add:
```go
// resolveNativeLibraryPath returns the FFI library path for the native
// engine: an explicit path wins; otherwise, when a release tag is set, it
// is fetched (and cached) via ffifetch. Non-native engines or an explicit
// path short-circuit to (explicitPath, nil). Callers decide whether a
// fetch error is fatal (materializer) or fail-open (rewriter factory).
func resolveNativeLibraryPath(engine, explicitPath, release, sha256, baseURL string) (string, error) {
	if engine != rewriter.EngineNative || explicitPath != "" || release == "" {
		return explicitPath, nil
	}
	p, err := ffifetch.Fetch(context.Background(), ffifetch.Options{
		Tag:     release,
		SHA256:  sha256,
		BaseURL: baseURL,
	})
	if err != nil {
		return "", err
	}
	log.Infow("native library resolved from release", "tag", release, "path", p)
	return p, nil
}
```
Then rewrite the corresponding block in `buildRewriterFactory` to use it, preserving the existing warn-and-disable behavior:
```go
nativeLibPath, err := resolveNativeLibraryPath(
	cfg.Rewriter.Engine, cfg.Rewriter.NativeLibraryPath,
	cfg.Rewriter.NativeLibraryRelease, cfg.Rewriter.NativeLibrarySHA256,
	cfg.Rewriter.NativeLibraryReleaseBaseURL,
)
if err != nil {
	log.Warne(err, "failed to fetch native rewriter library, rewriting disabled")
	return nil
}
```
(Delete the old inline `if cfg.Rewriter.Engine == rewriter.EngineNative && … { ffifetch.Fetch(…) }` block it replaces.)

- [ ] **Step 4: Add `buildMaterializer`**

In `build.go`:
```go
// buildMaterializer constructs the agent-mode Phase-1 materializer from the
// materialize config. Startup fail-fast: a returned error stops buildAgent.
func buildMaterializer(cfg *config.Config) (rewriter.Materializer, error) {
	mc := cfg.Materialize
	libPath, err := resolveNativeLibraryPath(
		mc.Engine, mc.NativeLibraryPath,
		mc.NativeLibraryRelease, mc.NativeLibrarySHA256, mc.NativeLibraryReleaseBaseURL,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve native library: %w", err)
	}
	poolSize := mc.RandomPoolSize
	if poolSize <= 0 {
		poolSize = 16
	}
	return rewriter.NewMaterializer(rewriter.Options{
		Enabled:           true,
		Engine:            mc.Engine,
		ServiceAddr:       mc.ServiceAddr,
		NativeLibraryPath: libPath,
		Timeout:           mc.Timeout.Duration,
	}, poolSize, mc.ProfileID)
}
```

- [ ] **Step 5: Insert the plugin into the agent chain**

In `buildAgent`, after `metrics := metricsplugin.New(obs)` and before constructing `chain`, build the query-plugin slice conditionally:
```go
queryPlugins := []plugin.QueryPlugin{}
var materializerClose func()
if cfg.Materialize.Enabled {
	m, err := buildMaterializer(cfg)
	if err != nil {
		return nil, fmt.Errorf("materialize: %w", err) // startup fail-fast
	}
	materializerClose = func() { _ = m.Close() }
	queryPlugins = append(queryPlugins, &materialize.Plugin{
		Materializer: m,
		PoolSize:     cfg.Materialize.RandomPoolSize,
		Observer:     obs,
	})
	log.Infow("agent materialize enabled", "engine", cfg.Materialize.Engine)
}
queryPlugins = append(queryPlugins,
	&agent.Plugin{Signer: signer, Observer: obs, Owner: cfg.Agent.Owner, IsDriver: cfg.Agent.Driver},
	metrics,
)
```
Change the `chain` literal to use the slice:
```go
chain := &plugin.PluginChain{
	ConnLifecyclePlugins:     []plugin.ConnLifecyclePlugin{metrics},
	HandshakeCompletePlugins: []plugin.HandshakeCompletePlugin{metrics},
	QueryPlugins:             queryPlugins,
	ExceptionPlugins:         []plugin.ExceptionPlugin{metrics},
}
```
And set the returned `teardown` to close the materializer (replacing `teardown: func() {}`):
```go
teardown: func() {
	if materializerClose != nil {
		materializerClose()
	}
},
```

Add the `materialize` import to `build.go`:
```go
"housegate/housegate/pkg/plugins/materialize"
```

- [ ] **Step 6: Update root BUILD deps and sync**

In the root `BUILD.bazel`, add `"//pkg/plugins/materialize"` to the `//:housegate` library `deps`. Then:
```bash
bazel run //:gazelle
```

- [ ] **Step 7: Build everything and run the affected tests**

Run:
```bash
bazel build //...
bazel test //pkg/plugins/materialize:materialize_test //pkg/rewriter:rewriter_test //pkg/config:config_test //pkg/proxy:proxy_test
```
Expected: `bazel build //...` SUCCESS. Tests: materialize + config PASS; `pkg/rewriter` and `pkg/proxy` show only their pre-existing baseline failures (external `localhost:50051`, flaky lifecycle) — diff against a clean `main` build to confirm no new failures.

- [ ] **Step 8: Commit**

```bash
git add pkg/proxy/observer.go build.go BUILD.bazel
git commit -m "feat(agent): wire materialize plugin into the agent chain (default off)"
```

---

## Self-Review

**Spec coverage:**
- §1/§4 agent-mode materialize-before-sign, body swapped once → Task 3 (plugin `OnQuery`) + Task 5 (chain order: materialize before signer). ✅
- §2 flag-gated, default off → Task 3 (`Config.Enabled` default false) + Task 4 (`Default()` seeds no enable) + Task 5 (plugin added only when enabled). ✅
- §3 MaterializeSQL contract (inputs supplied by caller) → Task 2 (`sentioMaterializer` builds `MaterializationInputs`). ✅
- §5.1 reuse backend seam, `Materializer`/`NewMaterializer` → Task 2. ✅
- §5.2 plugin package → Task 3. ✅
- §5.3 top-level `materialize` config + validation → Tasks 3 (Config/Validate) + 4 (root field/Default/gate). ✅
- §5.4 build wiring + shared FFI resolver → Task 5. ✅
- §6 runtime fail-open / startup fail-fast → Task 3 (`OnQuery` never errors) + Task 5 (`buildMaterializer` error stops `buildAgent`). ✅
- §7 metrics `clickhouse_proxy_agent_materialize_total{result,code}` → Task 5 Step 1. ✅
- §8 tests (plugin, materializer, native smoke, config) → Tasks 2, 3, 4. ✅
- §9 rewriter-go v0.3.0 bump + uuid direct → Task 1. ✅

**Placeholder scan:** No TBD/TODO; every code step shows complete code. The two "confirm the spelling" notes (`ModeAgent`, `errs` accumulator) are verification instructions against real symbols, not placeholders.

**Type consistency:** `Materializer.Materialize` returns `rewriter.MaterializeOutcome` in both Task 2 (definition) and Task 3 (plugin interface). `Observer` method set identical in Task 3 (interface) and Task 5 (impl). `MaterializeOutcome` fields (`SQL`/`Changed`/`Code`/`Message`) consistent across Tasks 2-3. `NewMaterializer(Options, poolSize int, profileID string)` signature identical in Tasks 2 and 5. Metric name/labels identical in spec §7 and Task 5.

**Note on config import alias:** the plan uses `materializeplugin` as the import alias in `pkg/config` (Task 4) to avoid shadowing, while `build.go` (Task 5) imports it as `materialize` (no collision there). Both refer to `housegate/housegate/pkg/plugins/materialize`.
