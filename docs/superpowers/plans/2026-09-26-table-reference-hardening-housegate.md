# Table-Reference Hardening — Plan C: housegate

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make housegate send the protected namespace to the engines, fail closed on every engine rejection, guard ordinary sessions lexically, refuse escaped identifiers in `sitablestate`, know `hg_promote` everywhere, and refuse to start against an engine that does not prove the policy.

**Architecture:** `pkg/rewriter` sends `protected_databases` on every request and turns every non-`Success` answer into a `RejectedError`; a new `rewriter.fail_open_on_unavailable` switch (default off) is the only remaining fail-open, for transport failures with storage integrity disabled. `sireserved`'s lexical scanner moves to `pkg/sqlsurface`; a new `pkg/plugins/tablerefguard` plugin runs before `forward` and `rewrite` on ordinary sessions with the spec's five lexical rules and an `observe` mode. The startup probe runs for every rewriter factory and gains the policy cases. Engine pins move to rewriter-go v0.16.0 / rewriter-proto v0.4.0.

**Tech Stack:** Go 1.25, Bazel 9.1.0 + Bzlmod (`bazel test //...`, `bazel run //:gazelle`), Prometheus client, the docker-bound integration suite.

**Spec:** [docs/superpowers/specs/2026-09-26-table-reference-hardening-design.md](../specs/2026-09-26-table-reference-hardening-design.md), §9 and §10.3–10.5. Depends on Plan A (`rewriter-proto` v0.4.0, `rewriter-go` v0.16.0 with the new FFI asset) and Plan B (`rewriter-grpc` v0.16.0) being released; Task 1 pins them.

**Pending amendment (2026-10-01):** add a task for spec §9.7 (Query-packet settings): refuse the R5 setting names (incl. name-binding settings, `legacy_column_name_of_tuple_literal`, `profile`) and the analyzer-off values of `enable_analyzer` / `allow_experimental_analyzer` carried in the native Query packet, with unit and integration tests; and a deployment check/runbook note that every ClickHouse settings profile keeps the analyzer on. Write the task before executing this plan.

**Working copy:** `/Users/uranuswch/Dev/housegate/housegate`, branch `feat/table-reference-hardening` from `origin/main` (`git fetch origin && git checkout -b feat/table-reference-hardening origin/main`). Bazel is the ground truth: after adding files or deps run `bazel mod tidy && bazel run //:gazelle`. Unit targets: `bazel test //pkg/rewriter:rewriter_test //pkg/plugins/...`; the integration target is tagged `manual` and needs docker plus `--test_env=POLYGLOT_SQL_FFI_PATH=…`.

## Global Constraints

- `rewriter.fail_open_on_unavailable` defaults to `false`; `true` together with `storage_integrity.enabled` is a `Config.Validate` error (spec T8).
- The guard's five rules and their names are exactly `reserved_name`, `physical_database`, `carrier_callable`, `identifier_placeholder`, `escaped_identifier` (spec §9.2); the error message shape is `table-reference guard: <rule>: <detail>; the rewriter applies the same policy`.
- The guard never runs on maintenance, platform-operator, peer-trusted or forwarded-from-peer sessions; it runs on driver sessions.
- The protected list sent to the engine is `[rewriter.physical_database] ∪ sitable.ReservedDatabases()`; nothing lists `hg_*` names by hand.
- Minimum engine builds: rewriter-go v0.16.0 (native), rewriter-grpc v0.16.0; the probe's failure text names them.
- Follow CLAUDE.md conventions: `pkg/log` structured logging, `fmt.Errorf("…: %w")`, English comments, no hard-wrapped Markdown, gazelle-managed `BUILD.bazel`.

## Review Focus

1. **A physical database named `default`** with `a UInt8 DEFAULT 0` in a CREATE: the guard's `physical_database` rule must not fire on the `DEFAULT` keyword. Test in Task 5.
2. **A tenant string literal containing the physical database name** (`WHERE name = 'phys'`): allowed by the guard (G2 checks literals only inside carrier/lookup argument lists). Test in Task 5.
3. **A backslash in a string literal inside a CREATE header** (`DEFAULT 'a\nb'`) with a governed target: `sitablestate` must still classify the CREATE as schema-only. Test in Task 7.
4. **An engine that acknowledges V2 but ignores `protected_databases`**: the startup probe must refuse it even with storage integrity disabled. Test in Task 9.
5. **A rewriter outage with the switch on and storage integrity off**: the original SQL is forwarded (fail-open) and a warning is logged; with the switch off the client gets an Exception. Test in Task 3.

---

### Task 1: pin the released engines and the proto

**Files:**
- Modify: `go.mod`, `go.sum` (`rewriter-go` v0.16.0, `rewriter-proto` v0.4.0), `MODULE.bazel.lock` if Bazel rewrites it
- Modify: `CLAUDE.md` "Current ordinary native FFI release pin" paragraph (v0.16.0 assets and hashes, identical to v0.15.0's: linux/amd64 `sha256:0a2bfaef183441885a1462a3381401f7b191dab25d1fab1364193effa72d2745`, darwin/arm64 `sha256:f3a8b3f512ea93553d25b978532152912249733a717718e87c888171b0a73fac`, built from Polyglot v0.13.0), sample configs' `native_library_release` if they name a tag
- Modify: `pkg/ffifetch` tests or constants that pin v0.13.0 (grep `v0.13.0`)

- [ ] **Step 1: Bump and re-sync**

Run: `go get github.com/housegate/rewriter-go@v0.16.0 github.com/housegate/rewriter-proto@v0.4.0 && go mod tidy && bazel mod tidy && bazel run //:gazelle && bazel build //cmd:housegate`
Expected: builds. `grep -rn 'v0.13.0' --include=*.go --include=*.md --include=*.yaml . | grep -v docs/superpowers` lists every remaining pin; update each to v0.16.0 with the new asset hashes from the rewriter-go release page (`SHA256SUMS`).

- [ ] **Step 2: Baseline test run**

Run: `bazel test //... 2>&1 | tail -20`
Expected: the probe-related tests pass unchanged (the probe still sends the old SI cases only); note any failure to compare against `main` before continuing (CLAUDE.md: matching failing sets are not regressions).

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum MODULE.bazel.lock CLAUDE.md $(git ls-files -m)
git commit -m "chore(deps): rewriter-go v0.16.0, rewriter-proto v0.4.0 (table-reference hardening)"
```

---

### Task 2: send `protected_databases` on every request

**Files:**
- Modify: `pkg/rewriter/args.go:18-44` (`buildDynamicArgs` gains `protected []string`), `pkg/rewriter/sentio.go` (the three `buildDynamicArgs` call sites at `:302`, `:337`, `:519`), `pkg/rewriter/sentio.go` `buildDatabaseMap` (returns the protected list too, or a new `protectedDatabases()` method)
- Test: `pkg/rewriter/args_test.go` (new), `pkg/rewriter/backend_test.go`

**Interfaces:**
- Produces: `func (f *SentioNetworkFactory) protectedDatabases() []string` → `[f.options.PhysicalDatabase] ∪ sitable.ReservedDatabases()` (deduplicated, physical first); `buildDynamicArgs(databaseMap, knownPhysical, protected []string, logicalCtx, physicalCtx, delim, logicalToRemoteIndex, remoteUpstreams, si)`.

- [ ] **Step 1: Write the failing test**

```go
// pkg/rewriter/args_test.go
package rewriter

import (
	"context"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"
)

func TestRewriteSendsProtectedDatabases(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1"}}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{logical: "db1"})
	if _, err := rw.Rewrite(context.Background(), "SELECT 1", ""); err != nil {
		t.Fatal(err)
	}
	dyn := be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs()
	want := []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"}
	if p := dyn.GetProtectedDatabases(); len(p) != len(want) || p[0] != "phys" || p[1] != "hg_safe" || p[2] != "hg_unsafe" || p[3] != "hg_promote" {
		t.Fatalf("protected_databases = %v, want %v", p, want)
	}
}
```

`fakeBackend` (`backend_test.go:18`) already records `lastReq`; `newFakeFactory(be)` (`backend_test.go:90`) builds a `SentioNetworkFactory` with `PhysicalDatabase: "phys"` and `AuthEnabled: false` over an in-memory registry that knows `db1`; `fakeSession` is the session stub in the same file.

- [ ] **Step 2: Run it to verify it fails**

Run: `bazel test //pkg/rewriter:rewriter_test --test_filter=TestRewriteSendsProtectedDatabases --test_output=errors`
Expected: FAIL — `protected_databases = [] …`.

- [ ] **Step 3: Implement**

`args.go`: add the `protected []string` parameter after `knownPhysical` and set `ProtectedDatabases: protected`. `sentio.go`:

```go
// protectedDatabases is the namespace caller SQL may never address (spec
// 2026-09-26 T3): the shared physical database and the protocol-owned
// databases, sent on every request whatever storage_integrity.enabled says.
func (f *SentioNetworkFactory) protectedDatabases() []string {
	out := make([]string, 0, 1+len(sitable.ReservedDatabases()))
	if f.options.PhysicalDatabase != "" {
		out = append(out, f.options.PhysicalDatabase)
	}
	for _, db := range sitable.ReservedDatabases() {
		if db != f.options.PhysicalDatabase {
			out = append(out, db)
		}
	}
	return out
}
```

and pass `r.factory.protectedDatabases()` at all three call sites.

- [ ] **Step 4: Run the package tests**

Run: `bazel test //pkg/rewriter:rewriter_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/rewriter/args.go pkg/rewriter/args_test.go pkg/rewriter/sentio.go pkg/rewriter/backend_test.go
git commit -m "feat(rewriter): send protected_databases on every rewrite request"
```

---

### Task 3: fail closed on every rejection; one explicit transport switch (T8)

**Files:**
- Modify: `pkg/rewriter/sentio.go:381-397` (the `UnsupportedStatement` arm and the `default` arm), `sentio.go` `rewriteFailure`, `pkg/rewriter/types.go` (`Options.FailOpenOnUnavailable bool`)
- Modify: `pkg/plugins/rewrite/config.go` (`FailOpenOnUnavailable`), `pkg/plugins/rewrite/rewriter.go:186-198` (remove the unconditional fail-open branch), `pkg/config/config.go` `Validate` (switch × SI), `build.go:196-215` (thread the option; `FailClosedOnError` removed or set true)
- Test: `pkg/rewriter/backend_test.go:143` (`TestSentioRewriter_UnsupportedForwardsOriginal` flips), `pkg/plugins/rewrite/rewriter_test.go`, `pkg/config/config_test.go`

**Interfaces:**
- Produces: `rewriter.Options.FailOpenOnUnavailable bool`; `rewrite.Config.FailOpenOnUnavailable bool` (`fail_open_on_unavailable`); `rewrite.Plugin.FailClosedOnError` is deleted (always closed for rejections).

- [ ] **Step 1: Write the failing tests**

In `backend_test.go`, replace `TestSentioRewriter_UnsupportedForwardsOriginal` with:

```go
func TestSentioRewriter_UnsupportedIsRejected(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_UnsupportedStatement, Message: "statement is not supported"}}
	_, err := newFakeFactory(be).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "DETACH TABLE db1.o", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Code != pb.RewriteCode_UnsupportedStatement {
		t.Fatalf("err = %v, want RejectedError(UnsupportedStatement)", err)
	}
}

func TestSentioRewriter_InvalidRequestIsRejectedWithoutSI(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_InvalidRewriteRequest, Message: "protected database phys is not addressable"}}
	_, err := newFakeFactory(be).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "USE phys", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Message != "protected database phys is not addressable" {
		t.Fatalf("err = %v", err)
	}
}

func TestSentioRewriter_TransportFailureFollowsTheSwitch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failOpen bool
		wantRej  bool
	}{{"default fails closed", false, true}, {"switch on fails open", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			be := &fakeBackend{err: errors.New("dial tcp: connection refused")}
			f := newFakeFactory(be)
			f.options.FailOpenOnUnavailable = tc.failOpen
			_, err := f.NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
			var rej *RejectedError
			if got := errors.As(err, &rej); got != tc.wantRej {
				t.Fatalf("RejectedError = %v, want %v (err=%v)", got, tc.wantRej, err)
			}
			if !tc.wantRej && err == nil {
				t.Fatal("fail-open must still return the transport error for the plugin to log")
			}
		})
	}
}
```

In `pkg/config/config_test.go`:

```go
func TestValidate_FailOpenOnUnavailableRejectedWithStorageIntegrity(t *testing.T) {
	c := minimalServerConfig(t) // config_test.go:178
	c.Rewriter.FailOpenOnUnavailable = true
	c.StorageIntegrity.Enabled = true
	c.StorageIntegrity.Tables = []string{"db1.t"}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "rewriter.fail_open_on_unavailable cannot be combined with storage_integrity.enabled") {
		t.Fatalf("err = %v", err)
	}
}
```

In `pkg/plugins/rewrite/rewriter_test.go`, add:

```go
func TestOnQuery_TransportErrorIsForwardedOnlyWhenFailOpen(t *testing.T) {
	for _, failOpen := range []bool{false, true} {
		rw := &fakeRewriter{err: errors.New("dial tcp: connection refused")}
		p := &Plugin{Factory: &fakeFactory{rw: rw}, FailOpenOnUnavailable: failOpen}
		sess := newSessionForTest(t, 1)
		qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1", Query: &chproto.Query{Body: "SELECT 1"}}
		err := p.OnQuery(context.Background(), qctx)
		if failOpen && err != nil {
			t.Fatalf("fail-open: err = %v, want nil", err)
		}
		if !failOpen && err == nil {
			t.Fatal("fail-closed: transport error must reach the client")
		}
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel test //pkg/rewriter:rewriter_test //pkg/plugins/rewrite:rewrite_test //pkg/config:config_test --test_output=errors 2>&1 | grep -E 'FAIL|undefined' | head`
Expected: the three new tests FAIL (`FailOpenOnUnavailable undefined`, Unsupported returns nil error).

- [ ] **Step 3: Implement**

`sentio.go` `Rewrite`: delete the `case pb.RewriteCode_UnsupportedStatement:` arm; the `default:` arm becomes `return RewriteResult{}, &RejectedError{Code: resp.GetCode(), Message: resp.GetMessage()}`. `rewriteFailure`:

```go
// rewriteFailure classifies a transport or availability failure (spec
// 2026-09-26 T8): a RejectedError reaches the client as an Exception; with
// fail_open_on_unavailable set and storage integrity disabled the plain error
// is returned so the plugin can log and forward the original SQL.
func (r *sentioRewriter) rewriteFailure(err error) error {
	if r.factory.options.FailOpenOnUnavailable && !r.factory.options.StorageIntegrity.Enabled {
		return err
	}
	return &RejectedError{Code: pb.RewriteCode_RewriteError,
		Message: "rewrite unavailable: " + err.Error(), Cause: err}
}
```

Keep the SI-specific message where `StorageIntegrity.Enabled` (`storage-integrity rewrite classification unavailable: …`) by branching on it before the generic one.

`pkg/plugins/rewrite/rewriter.go`: replace the `if p.FailClosedOnError {…} logger.Warne(…); return nil` tail with:

```go
		if p.FailOpenOnUnavailable {
			logger.Warne(err, "rewriter unavailable; forwarding original SQL (rewriter.fail_open_on_unavailable)")
			return nil
		}
		logger.Errorw("rewriter unavailable (fail-closed)", "error", err)
		return fmt.Errorf("rewrite unavailable: %w", err)
```

and rename the field `FailClosedOnError` → delete it; add `FailOpenOnUnavailable bool`. `config.go` (rewrite plugin): add

```go
	// FailOpenOnUnavailable forwards the original SQL when the rewriter is
	// unreachable (dial, timeout, nil response). Default false: the client
	// gets an Exception. Rejections are never fail-open. Invalid together
	// with storage_integrity.enabled.
	FailOpenOnUnavailable bool `json:"fail_open_on_unavailable" yaml:"fail_open_on_unavailable"`
```

`pkg/config/config.go` `Validate`, next to the engine check:

```go
	if c.Rewriter.FailOpenOnUnavailable && c.StorageIntegrity.Enabled {
		errs = append(errs, errors.New("rewriter.fail_open_on_unavailable cannot be combined with storage_integrity.enabled"))
	}
```

(`StorageIntegrityConfig.Enabled` is the explicit switch from spec 2026-09-24 §6; use its accessor if it is a method.) `build.go`: `rwConfig.FailOpenOnUnavailable = cfg.Rewriter.FailOpenOnUnavailable`; `rewritePlug.FailOpenOnUnavailable = cfg.Rewriter.FailOpenOnUnavailable`; delete `FailClosedOnError: siOptions.Enabled`. Grep for every other `FailClosedOnError` use (tests at `rewriter_test.go:34,64`, `build_test.go`) and update.

- [ ] **Step 4: Run the tests**

Run: `bazel test //pkg/rewriter:rewriter_test //pkg/plugins/rewrite:rewrite_test //pkg/config:config_test //:housegate_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/rewriter pkg/plugins/rewrite pkg/config build.go build_test.go
git commit -m "feat(rewrite): fail closed on every rewriter rejection; rewriter.fail_open_on_unavailable for transport failures (spec T8)"
```

---

### Task 4: move the lexical scanner to `pkg/sqlsurface`

**Files:**
- Create: `pkg/sqlsurface/surface.go`, `pkg/sqlsurface/surface_test.go`, `pkg/sqlsurface/BUILD.bazel` (gazelle)
- Modify: `pkg/plugins/sireserved/plugin.go` (delete the moved functions; call `sqlsurface`), `pkg/plugins/sireserved/plugin_test.go` (scanner tests move), `pkg/plugins/sireserved/BUILD.bazel` (gazelle)

**Interfaces:**
- Produces (exported, behaviour unchanged):
  - `type sqlsurface.Surfaces struct { OutsideLiterals, WithLiterals string }`
  - `func sqlsurface.Scan(sql string) (Surfaces, error)` — was `scanSQLSurfaces`; same refusals (backslash in `'…'`, escaped quoted identifier, stray `$`, unterminated spans).
  - `func sqlsurface.Identifiers(surface string) []string` — was `identifiers`.
  - `func sqlsurface.ContainsIdentifierPlaceholder(surface string) bool` — was `containsIdentifierPlaceholder`.
  - `func sqlsurface.CallableNames(surface string) []string` — every `name(` occurrence on a surface, in order (generalises `objectCarrierCallable`, which becomes a filter over it in `sireserved`).
  - `func sqlsurface.CallableArgumentSpans(surface string, name string) []string` — the parenthesised argument text of every call to `name` (balanced parentheses), used by the guard's G2.
  - `func sqlsurface.IsIdentifierByte(b byte) bool`.

- [ ] **Step 1: Move with tests first**

Create `pkg/sqlsurface/surface_test.go` by moving every scanner-level test from `sireserved/plugin_test.go` (the ones calling `scanSQLSurfaces`, `consumeHeredoc`, `containsIdentifierPlaceholder`, `identifiers`), renamed to the exported names, plus:

```go
func TestCallableArgumentSpans(t *testing.T) {
	s, err := Scan("SELECT * FROM merge('phys', 'db2') AS m JOIN other(1, f(2)) USING (a)")
	if err != nil {
		t.Fatal(err)
	}
	if got := CallableArgumentSpans(s.WithLiterals, "merge"); len(got) != 1 || strings.TrimSpace(got[0]) != "phys , db2" {
		t.Fatalf("spans = %q", got)
	}
	if got := CallableArgumentSpans(s.OutsideLiterals, "other"); len(got) != 1 || got[0] != "1, f(2)" {
		t.Fatalf("nested spans = %q", got)
	}
}
```

(`WithLiterals` replaces `'phys'` with ` phys ` per the existing scanner, hence `phys , db2` after trimming; adjust the expectation to the scanner's actual spacing on the first run.)

- [ ] **Step 2: Run to verify it fails**

Run: `bazel run //:gazelle && bazel test //pkg/sqlsurface:sqlsurface_test --test_output=errors`
Expected: FAIL to build (package missing).

- [ ] **Step 3: Move the code**

Create `pkg/sqlsurface/surface.go` with package doc `// Package sqlsurface splits ClickHouse SQL into the two lexical surfaces the proxy-side guards reason on (Spec N D1): executable text without literals, and the same text with string and heredoc contents retained.` and move `scanSQLSurfaces` (→ `Scan`), `consumeStringLiteral`, `consumeHeredoc`, `isHeredocTagByte`, `consumeLineComment`, `consumeBlockComment`, `consumeQuotedIdentifier`, `identifiers` (→ `Identifiers`), `isIdentifierByte` (→ `IsIdentifierByte`), `hasPrefixAt`, `containsIdentifierPlaceholder` (→ `ContainsIdentifierPlaceholder`) verbatim. Add:

```go
// CallableNames returns every identifier immediately followed by "(" on the
// surface, in order.
func CallableNames(surface string) []string {
	var out []string
	for i := 0; i < len(surface); {
		if !IsIdentifierByte(surface[i]) {
			i++
			continue
		}
		start := i
		for i < len(surface) && IsIdentifierByte(surface[i]) {
			i++
		}
		call := i
		for call < len(surface) && (surface[call] == ' ' || surface[call] == '\t' || surface[call] == '\r' || surface[call] == '\n') {
			call++
		}
		if call < len(surface) && surface[call] == '(' {
			out = append(out, surface[start:i])
		}
	}
	return out
}

// CallableArgumentSpans returns the text between the balanced parentheses of
// every call to name (case-insensitive), in order. Unbalanced text yields no span.
func CallableArgumentSpans(surface, name string) []string {
	var out []string
	lower := strings.ToLower(surface)
	needle := strings.ToLower(name)
	for i := 0; i+len(needle) < len(lower); {
		j := strings.Index(lower[i:], needle)
		if j < 0 {
			return out
		}
		start := i + j
		end := start + len(needle)
		if (start > 0 && IsIdentifierByte(surface[start-1])) || end >= len(surface) || surface[end] != '(' {
			i = end
			continue
		}
		depth, k := 0, end
		for ; k < len(surface); k++ {
			if surface[k] == '(' {
				depth++
			} else if surface[k] == ')' {
				depth--
				if depth == 0 {
					out = append(out, surface[end+1:k])
					break
				}
			}
		}
		i = k + 1
	}
	return out
}
```

`sireserved/plugin.go` keeps its rules and calls `sqlsurface.Scan`, `sqlsurface.ContainsIdentifierPlaceholder`, `sqlsurface.Identifiers`; `objectCarrierCallable` becomes `for _, n := range sqlsurface.CallableNames(surface) { if isObjectCarrierName(n) { return n } }`. Export the carrier list as `sireserved.IsObjectCarrierName(name string) bool` so the guard reuses it (spec G3 mirrors T5's refused list: add `mergetreeprojection`, `executable`, `fuzzquery`, `fuzzjson` to it).

- [ ] **Step 4: Run both packages**

Run: `bazel run //:gazelle && bazel test //pkg/sqlsurface:sqlsurface_test //pkg/plugins/sireserved:sireserved_test --test_output=errors`
Expected: PASS; `sireserved`'s behaviour tests unchanged.

- [ ] **Step 5: Commit**

```bash
git add pkg/sqlsurface pkg/plugins/sireserved
git commit -m "refactor(sqlsurface): extract sireserved's lexical scanner for reuse"
```

---

### Task 5: the `tablerefguard` plugin (T9)

**Files:**
- Create: `pkg/plugins/tablerefguard/plugin.go`, `plugin_test.go`, `BUILD.bazel` (gazelle)
- Modify: `pkg/proxy/observer.go` (or a metrics file in the new package: register `clickhouse_proxy_tableref_guard_rejections_total{rule}` with `prometheus.MustRegister` in `init()`, the pattern the repo uses)

**Interfaces:**
- Consumes: `sqlsurface.Scan/Identifiers/ContainsIdentifierPlaceholder/CallableNames/CallableArgumentSpans`, `sireserved.IsObjectCarrierName`, `sitable.ReservedDatabases()`.
- Produces:

```go
package tablerefguard

type Mode string
const (
	ModeEnforce Mode = "enforce"
	ModeObserve Mode = "observe"
)

type Plugin struct {
	PhysicalDatabase  string
	ReservedDatabases []string
	Mode              Mode // "" = enforce
}
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error
func (*Plugin) RunOnForward() bool     { return true }
func (*Plugin) RunOnPeerTrust() bool   { return true }
func (*Plugin) RejectUndecodableQuery() bool { return true }
// Check is the pure rule engine, exported for tests and for build-time self-checks.
func Check(sql, physicalDatabase string, reserved []string) (rule, detail string, err error)
```

`Check` returns `rule == ""` when the SQL passes; `err` is a scan failure (unterminated span etc.), which counts as rule `escaped_identifier` or `scan` and is refused.

- [ ] **Step 1: Write the failing tests**

```go
// pkg/plugins/tablerefguard/plugin_test.go
package tablerefguard

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

var reserved = []string{"hg_safe", "hg_unsafe", "hg_promote"}

func TestCheck(t *testing.T) {
	for _, tc := range []struct {
		name, sql, phys, wantRule string
	}{
		// G1 reserved names, any surface.
		{"reserved from", "SELECT * FROM hg_safe.db1__t", "phys", "reserved_name"},
		{"reserved promote", "SELECT * FROM hg_promote.db1__t", "phys", "reserved_name"},
		{"reserved literal", "SELECT * FROM merge('hg_unsafe', 'x')", "phys", "reserved_name"},
		{"reserved in comment ignored", "SELECT 1 /* hg_safe */", "phys", ""},
		// G2 physical database.
		{"phys qualifier", "SELECT * FROM phys.`db2.x`", "phys", "physical_database"},
		{"phys quoted qualifier", "SELECT * FROM `phys`.`db2.x`", "phys", "physical_database"},
		{"phys use", "USE phys", "phys", "physical_database"},
		{"phys show tables", "SHOW TABLES FROM phys", "phys", "physical_database"},
		{"phys carrier literal", "SELECT * FROM merge('phys', 'x')", "phys", "carrier_callable"},
		{"phys lookup literal", "SELECT joinGet('phys.`db2.x`', 'v', 1)", "phys", "physical_database"},
		{"phys as column", "SELECT phys FROM db1.o", "phys", ""},
		{"phys in ordinary literal", "SELECT * FROM db1.o WHERE name = 'phys'", "phys", ""},
		{"default keyword with physical named default", "CREATE TABLE db1.n (a UInt8 DEFAULT 0) ENGINE = Memory", "default", ""},
		{"default as qualifier", "SELECT * FROM default.x", "default", "physical_database"},
		// G3 carriers.
		{"merge own", "SELECT * FROM merge('db1', 'o')", "phys", "carrier_callable"},
		{"remote", "SELECT * FROM remote('h', db1.o)", "phys", "carrier_callable"},
		{"numbers allowed", "SELECT * FROM numbers(10)", "phys", ""},
		{"url allowed (non-goal)", "SELECT * FROM url('http://x', CSV)", "phys", ""},
		// G4 placeholders.
		{"identifier placeholder", "SELECT * FROM {p:Identifier}", "phys", "identifier_placeholder"},
		{"column identifier placeholder (deliberate false positive)", "SELECT {c:Identifier} FROM db1.o", "phys", "identifier_placeholder"},
		{"value placeholder allowed", "SELECT * FROM db1.o WHERE a = {v:UInt64}", "phys", ""},
		// G5 escapes.
		{"escaped backtick identifier", "SELECT * FROM db1.`\\x74`", "phys", "escaped_identifier"},
		{"escaped double-quoted identifier", `SELECT * FROM db1."\x74"`, "phys", "escaped_identifier"},
		{"backslash in string literal allowed", `SELECT * FROM db1.o WHERE s = 'a\nb'`, "phys", ""},
		// Heredocs and comments per Spec N D1.
		{"heredoc carrier", "SELECT * FROM merge($$phys$$, 'x')", "phys", "carrier_callable"},
		{"stray dollar", "SELECT $ FROM db1.o", "phys", "scan"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule, _, err := Check(tc.sql, tc.phys, reserved)
			if err != nil {
				rule = "scan"
			}
			if rule != tc.wantRule {
				t.Fatalf("rule = %q (err=%v), want %q", rule, err, tc.wantRule)
			}
		})
	}
}

func newSessionForTest(t *testing.T) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return chsession.New(1, client)
}

func TestOnQuery_SessionGate(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved}
	sql := "SELECT * FROM phys.`db2.x`"
	for _, tc := range []struct {
		name string
		set  func(*chsession.SessionState)
		want bool // refused?
	}{
		{"ordinary", func(*chsession.SessionState) {}, true},
		{"driver", func(s *chsession.SessionState) { s.SetIsDriver(true) }, true},
		{"maintenance", func(s *chsession.SessionState) { s.SetMaintenance(true) }, false},
		{"operator", func(s *chsession.SessionState) { s.SetPlatformOperator(true) }, false},
		{"peer trusted", func(s *chsession.SessionState) { s.SetPeerTrust("peer:9001") }, false},
		{"forwarded from peer", func(s *chsession.SessionState) { s.SetPeerTrustForwarded("peer:9001", true) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSessionForTest(t)
			tc.set(sess.State())
			err := p.OnQuery(context.Background(), &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}})
			if (err != nil) != tc.want {
				t.Fatalf("refused = %v, want %v (%v)", err != nil, tc.want, err)
			}
			if err != nil && !strings.HasPrefix(err.Error(), "table-reference guard: physical_database:") {
				t.Fatalf("message = %q", err.Error())
			}
		})
	}
}

func TestOnQuery_ObserveModeCountsWithoutRefusing(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved, Mode: ModeObserve}
	sess := newSessionForTest(t)
	before := testCounterValue(t, "identifier_placeholder")
	err := p.OnQuery(context.Background(), &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT * FROM {p:Identifier}", Query: &chproto.Query{Body: "SELECT * FROM {p:Identifier}"}})
	if err != nil {
		t.Fatalf("observe mode must not refuse: %v", err)
	}
	if after := testCounterValue(t, "identifier_placeholder"); after != before+1 {
		t.Fatalf("counter = %v, want %v", after, before+1)
	}
}
```

`testCounterValue` reads the `rule`-labelled counter through `prometheus/testutil.ToFloat64(rejections.WithLabelValues(rule))`. The session setters are `SetMaintenance`, `SetPlatformOperator`, `SetIsDriver`, `SetPeerTrust(peerAddr)` and `SetPeerTrustForwarded(peerAddr, forwarded)` (`pkg/chsession/state.go:381-525`).

- [ ] **Step 2: Run to verify it fails**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/tablerefguard:tablerefguard_test --test_output=errors`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

```go
// Package tablerefguard is the ordinary-session defence-in-depth guard of
// spec 2026-09-26 §9.2. It refuses the lexically decidable subset of the
// table-reference policy before the rewriter runs; the engines remain the
// authority and the startup probe proves them.
package tablerefguard

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/sireserved"
	"github.com/housegate/housegate/pkg/sqlsurface"
)

type Mode string

const (
	ModeEnforce Mode = "enforce"
	ModeObserve Mode = "observe"
)

const (
	RuleReservedName          = "reserved_name"
	RulePhysicalDatabase      = "physical_database"
	RuleCarrierCallable       = "carrier_callable"
	RuleIdentifierPlaceholder = "identifier_placeholder"
	RuleEscapedIdentifier     = "escaped_identifier"
	RuleScan                  = "scan"
)

var rejections = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "clickhouse_proxy_tableref_guard_rejections_total",
	Help: "Statements the table-reference guard refused (or would refuse in observe mode), by rule.",
}, []string{"rule"})

func init() { prometheus.MustRegister(rejections) }

type Plugin struct {
	PhysicalDatabase  string
	ReservedDatabases []string
	Mode              Mode
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.Session == nil {
		return nil
	}
	snap := qctx.Session.State().Snapshot()
	if snap.Maintenance || snap.PlatformOperator || snap.IsPeerTrusted || snap.IsForwardedFromPeer {
		return nil // sireserved owns the privileged sessions; peers carry rewritten SQL
	}
	sql := qctx.OriginalSQL
	if qctx.Query != nil && qctx.Query.Body != "" {
		sql = qctx.Query.Body
	}
	rule, detail, err := Check(sql, p.PhysicalDatabase, p.ReservedDatabases)
	if err != nil {
		rule, detail = RuleScan, err.Error()
	}
	if rule == "" {
		return nil
	}
	rejections.WithLabelValues(rule).Inc()
	msg := fmt.Sprintf("table-reference guard: %s: %s; the rewriter applies the same policy", rule, detail)
	if p.Mode == ModeObserve {
		_, logger := log.FromContext(ctx)
		logger.Warnw("table-reference guard would refuse (observe mode)", "rule", rule, "detail", detail)
		return nil
	}
	return fmt.Errorf("%s", msg)
}

func (*Plugin) RunOnForward() bool          { return true }
func (*Plugin) RunOnPeerTrust() bool        { return true }
func (*Plugin) RejectUndecodableQuery() bool { return true }

// Check applies the five rules in order and returns the first hit.
func Check(sql, physicalDatabase string, reserved []string) (rule, detail string, err error) {
	surfaces, err := sqlsurface.Scan(sql)
	if err != nil {
		// sqlsurface refuses an escaped quoted identifier, a backslash in a
		// single-quoted literal, a stray $ and unterminated spans. Only the
		// first is a guard rule of its own; a backslash inside '…' is
		// legitimate tenant SQL, so rescan with literals blanked.
		if strings.Contains(err.Error(), "escaped quoted identifier") {
			return RuleEscapedIdentifier, "a backslash inside a quoted identifier is not accepted", nil
		}
		if strings.Contains(err.Error(), "backslash-bearing single-quoted string literal") {
			surfaces, err = scanWithBlankedLiterals(sql)
		}
		if err != nil {
			return "", "", err
		}
	}
	// G1: reserved names anywhere, including literals.
	for _, id := range sqlsurface.Identifiers(surfaces.WithLiterals) {
		for _, name := range reserved {
			if strings.EqualFold(id, name) {
				return RuleReservedName, fmt.Sprintf("reserved database %q is not addressable", name), nil
			}
		}
	}
	// G3: carrier callables, before G2 so merge('phys', …) is attributed to the carrier.
	for _, name := range sqlsurface.CallableNames(surfaces.OutsideLiterals) {
		if sireserved.IsObjectCarrierName(name) {
			return RuleCarrierCallable, fmt.Sprintf("table function %s is not accepted", name), nil
		}
	}
	// G2: the physical database as a qualifier, a USE / SHOW target, or a lookup argument.
	if physicalDatabase != "" {
		if physicalDatabaseAddressed(surfaces, physicalDatabase) {
			return RulePhysicalDatabase, fmt.Sprintf("protected database %s is not addressable", physicalDatabase), nil
		}
	}
	// G4: Identifier placeholders anywhere (position-blind by design).
	if sqlsurface.ContainsIdentifierPlaceholder(surfaces.OutsideLiterals) {
		return RuleIdentifierPlaceholder, "ClickHouse Identifier placeholders are not accepted", nil
	}
	return "", "", nil
}
```

`physicalDatabaseAddressed` (same file): tokenise `surfaces.OutsideLiterals` with `sqlsurface.Identifiers`-style scanning that also yields the byte offset of each identifier; report true when an identifier equal (case-sensitive; ClickHouse database names are) to the physical database is followed, after optional whitespace, by `.`; or is preceded by the word `USE`, or by `FROM`/`IN` inside a statement whose first word is `SHOW`; then, for every lookup function name in `lookupFunctions` (`joinget`, `joingetornull`, `dictget*` prefix, `dicthas`, `dictgethierarchy`, `dictisin`, `dictgetchildren`, `dictgetdescendants`, `hascolumnintable`), for each span from `sqlsurface.CallableArgumentSpans(surfaces.WithLiterals, name)`: report true when the span's first comma-separated argument, trimmed, equals the physical database or starts with it followed by `.`. `scanWithBlankedLiterals` re-runs the scan over a copy of `sql` whose single-quoted literals have been replaced by `' '` (walk the string, skip `''` pairs, do not interpret backslashes: the goal is only to blank them). Note the quoted-identifier form `` `phys`. `` is handled because `Scan` writes the identifier's text onto both surfaces followed by a space, so the offset-aware scan sees `phys .`; allow whitespace before the dot.

- [ ] **Step 4: Run the tests**

Run: `bazel test //pkg/plugins/tablerefguard:tablerefguard_test --test_output=errors`
Expected: PASS for every row. If `merge($$phys$$, 'x')` reports `physical_database` instead of `carrier_callable`, G3 runs before G2 as written and the test row is wrong; keep the order in the code.

- [ ] **Step 5: Commit**

```bash
git add pkg/plugins/tablerefguard
git commit -m "feat(tablerefguard): lexical table-reference guard for ordinary sessions (spec T9)"
```

---

### Task 6: wire the guard and its configuration

**Files:**
- Modify: `pkg/config/config.go` (a `TableRefGuard` block: `mode` string, validated to `enforce|observe|""`), `build.go:598-612` (register right after `sireserved`, whenever `rwFactory != nil`), `build_test.go`
- Modify: sample configs (`config.example.yaml` or whichever the repo ships) with a commented `table_reference_guard: {mode: enforce}`

- [ ] **Step 1: Write the failing test**

In `build_test.go`, modelled on `TestBuildServer_StorageIntegrityReservedGuardWiring` (`:169`):

```go
func TestBuildServer_TableReferenceGuardWiring(t *testing.T) {
	for _, si := range []bool{false, true} {
		cfg := minimalServerCfg(t)
		cfg.TableReferenceGuard.Mode = "observe"
		if si {
			cfg.StorageIntegrity.Tables = []string{"tenant.events"}
		}
		built, err := buildServer(Options{
			Config:       cfg,
			NetworkState: network.NewInMemoryNetworkState(),
			Rewriter:     siProbeStubRewriterFactory{},
		}, nil)
		if err != nil {
			t.Fatalf("si=%v: build: %v", si, err)
		}
		defer built.teardown()
		guardIndex, reservedIndex, forwardIndex := -1, -1, -1
		var guard *tablerefguard.Plugin
		for i, candidate := range requireExternalChain(t, built).QueryPlugins {
			switch typed := candidate.(type) {
			case *tablerefguard.Plugin:
				guard, guardIndex = typed, i
			case *sireserved.Plugin:
				reservedIndex = i
			case *forward.Plugin:
				forwardIndex = i
			}
		}
		if guard == nil {
			t.Fatalf("si=%v: tablerefguard not wired", si)
		}
		if guardIndex >= forwardIndex || (si && guardIndex <= reservedIndex) {
			t.Fatalf("si=%v: guard=%d reserved=%d forward=%d", si, guardIndex, reservedIndex, forwardIndex)
		}
		if guard.PhysicalDatabase != cfg.Rewriter.PhysicalDatabase || guard.Mode != tablerefguard.ModeObserve ||
			!reflect.DeepEqual(guard.ReservedDatabases, sitable.ReservedDatabases()) {
			t.Fatalf("guard = %+v", guard)
		}
	}
}

func TestValidate_TableReferenceGuardMode(t *testing.T) {
	c := minimalServerConfig(t) // pkg/config/config_test.go
	c.TableReferenceGuard.Mode = "audit"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "table_reference_guard.mode") {
		t.Fatalf("err = %v", err)
	}
}
```

`minimalServerCfg`, `buildServer(Options{…}, nil)`, `requireExternalChain(t, built).QueryPlugins`, `built.teardown()` and `siProbeStubRewriterFactory{}` are the helpers `TestBuildServer_StorageIntegrityReservedGuardWiring` (`build_test.go:169`) already uses. `siProbeStubRewriterFactory` must answer the new policy probes too once Task 9 lands; until then the guard test builds with it as it is.

- [ ] **Step 2: Run to verify it fails**

Run: `bazel test //:housegate_test --test_filter='TestBuildServer_TableReferenceGuardWiring|TestValidate_TableReferenceGuardMode' --test_output=errors`
Expected: FAIL (`TableReferenceGuard` undefined).

- [ ] **Step 3: Implement**

`config.go`: `TableReferenceGuard TableReferenceGuardConfig `json:"table_reference_guard" yaml:"table_reference_guard"`` with `type TableReferenceGuardConfig struct { Mode string `json:"mode" yaml:"mode"` }`; `Validate`: `switch c.TableReferenceGuard.Mode { case "", "enforce", "observe": default: errs = append(errs, fmt.Errorf("table_reference_guard.mode %q is invalid (want enforce or observe)", …)) }`. `build.go`, right after the `sireserved` block:

```go
	if rwFactory != nil {
		queryPlugins = append(queryPlugins, &tablerefguard.Plugin{
			PhysicalDatabase:  cfg.Rewriter.PhysicalDatabase,
			ReservedDatabases: sitable.ReservedDatabases(),
			Mode:              tablerefguard.Mode(cfg.TableReferenceGuard.Mode),
		})
		log.Infow("table-reference guard enabled", "mode", cfg.TableReferenceGuard.Mode)
	}
```

- [ ] **Step 4: Run**

Run: `bazel run //:gazelle && bazel test //:housegate_test //pkg/config:config_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add build.go build_test.go pkg/config BUILD.bazel $(git ls-files -m -o --exclude-standard | grep -E 'example|sample')
git commit -m "feat(build): wire the table-reference guard on every rewriter deployment"
```

---

### Task 7: `sitablestate` refuses backslashes inside quoted identifiers (T10)

**Files:**
- Modify: `pkg/plugins/sitablestate/lexer.go` (`readQuoted`)
- Test: `pkg/plugins/sitablestate/lexer_test.go`, `plugin_test.go`

- [ ] **Step 1: Write the failing tests**

In `lexer_test.go`:

```go
func TestQuotedIdentifierBackslashIsUnreadable(t *testing.T) {
	for sql, wantReadable := range map[string]bool{
		"CREATE MATERIALIZED VIEW db1.mv TO db1.`\\x74` AS SELECT a FROM db1.o":  false,
		"CREATE MATERIALIZED VIEW db1.`\\x74` ENGINE = Memory AS SELECT a FROM db1.o": false,
		`CREATE MATERIALIZED VIEW db1.mv TO db1."\x74" AS SELECT a FROM db1.o`:   false,
		"CREATE MATERIALIZED VIEW db1.mv TO db1.t AS SELECT a FROM db1.o":         true,
	} {
		if _, ok := materializedViewHeader(sql); ok != wantReadable {
			t.Errorf("%s: readable = %v, want %v", sql, ok, wantReadable)
		}
	}
	// A string literal keeps decoding escapes: the CREATE stays schema-only.
	if createTableCarriesData(`CREATE TABLE db1.n (a String DEFAULT 'a\nb') ENGINE = Memory`) {
		t.Fatal("a backslash in a string literal must not make the CREATE data-carrying")
	}
	if !createTableCarriesData("CREATE TABLE db1.`\\x74` ENGINE = Memory AS SELECT 1") {
		t.Fatal("an escaped target name must fail closed as data-carrying")
	}
}
```

In `plugin_test.go`'s `TestDataCarryingCreationIntoGovernedTables` table add:

```go
		{"MV TO an escaped pending name", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.`\\x70` AS SELECT a FROM db1.o", accessed("db1.mv", "db1.x70", "db1.o"), unreadableErr},
```

(`db1.x70` is what the engine would report for the literal reading; the point is the plugin refuses regardless.)

- [ ] **Step 2: Run to verify they fail**

Run: `bazel test //pkg/plugins/sitablestate:sitablestate_test --test_filter='TestQuotedIdentifierBackslashIsUnreadable|TestDataCarryingCreationIntoGovernedTables' --test_output=errors`
Expected: FAIL — the escaped headers are readable today.

- [ ] **Step 3: Implement**

In `readQuoted`:

```go
		case '\\':
			if quote != '\'' {
				// ClickHouse decodes escapes inside quoted identifiers; this
				// lexer does not model that table, so the header is
				// unreadable and the caller fails closed (spec 2026-09-26 T10).
				return "", len(sql), false
			}
			if i+1 >= len(sql) {
				return "", len(sql), false
			}
			b.WriteByte(sql[i+1])
			i++
```

Update the doc comment on `tokenize` to say so.

- [ ] **Step 4: Run the package**

Run: `bazel test //pkg/plugins/sitablestate:sitablestate_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/plugins/sitablestate
git commit -m "fix(sitablestate): a backslash inside a quoted identifier makes the header unreadable (spec T10)"
```

---

### Task 8: `hg_promote` in every reserved list (T11)

**Files:**
- Modify: `build.go:605-611` (`sireserved` wiring uses `sitable.ReservedDatabases()`), `pkg/rewriter/storage_integrity.go:271-288` (`NewStorageIntegrityScrubber`)
- Test: `build_test.go:169` (`TestBuildServer_StorageIntegrityReservedGuardWiring` asserts three names), `pkg/rewriter/storage_integrity_test.go:183` (`…EmptySnapshotStillRedactsReservedNames` adds `hg_promote`)

- [ ] **Step 1: Extend the two tests** — add `hg_promote` to the expected reserved list in the wiring test and assert `NewStorageIntegrityScrubber(sitable.NewSnapshot(nil, 1)).Scrub("hg_promote.db1__t is missing")` (use the real constructor and method names from the file) contains no `hg_promote`.

- [ ] **Step 2: Run to verify they fail**

Run: `bazel test //:housegate_test //pkg/rewriter:rewriter_test --test_filter='TestBuildServer_StorageIntegrityReservedGuardWiring|TestStorageIntegrityScrubber' --test_output=errors`
Expected: FAIL.

- [ ] **Step 3: Implement** — `build.go`: `ReservedDatabases: sitable.ReservedDatabases()`; scrubber: replace the two hard-coded pairs with a loop `for _, db := range sitable.ReservedDatabases() { pairs = append(pairs, db, storageIntegrityRedaction) }` (keep the qualified pairs first; capacity `len(active)*4 + 2*len(reserved) + 2`).

- [ ] **Step 4: Run** — same command: PASS.

- [ ] **Step 5: Commit**

```bash
git add build.go build_test.go pkg/rewriter/storage_integrity.go pkg/rewriter/storage_integrity_test.go
git commit -m "fix(storage-integrity): hg_promote joins the sireserved and scrubber reserved lists (spec T11)"
```

---

### Task 9: the behavioural probe runs always and proves the policy (T13)

**Files:**
- Modify: `pkg/rewriter/probe.go` (policy cases; `ProbeStorageIntegrityBuild` → `ProbeRewriterBuild(ctx, siEnabled bool)`; keep the old name as a wrapper for injected factories), `build.go:471-498` (run for every factory; SI cases only when enabled)
- Test: `pkg/rewriter/probe_test.go`, `build_test.go:282-392`

**Interfaces:**
- Produces: `type RewriterProbeFactory interface { Factory; ProbeRewriterBuild(ctx context.Context, storageIntegrity bool) error }`; `StorageIntegrityProbeFactory` stays as an alias whose method calls `ProbeRewriterBuild(ctx, true)`. Constants: `TableReferenceProbeRequiredBuild = "rewriter-go >= v0.16.0 or rewriter-grpc >= v0.16.0 (table-reference policy, spec 2026-09-26)"`.

- [ ] **Step 1: Write the failing tests**

```go
// pkg/rewriter/probe_test.go (extend)
func TestProbeRewriterBuild_PolicyCasesRunWithoutStorageIntegrity(t *testing.T) {
	be := &fakeBackend{script: policyProbeScript()} // answers every policy case correctly
	f := newTestFactory(t, be, Options{PhysicalDatabase: "phys"})
	if err := f.ProbeRewriterBuild(context.Background(), false); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if be.sawSICases {
		t.Fatal("SI cases must not run when storage integrity is disabled")
	}
}

func TestProbeRewriterBuild_EngineIgnoringProtectedDatabasesIsRefused(t *testing.T) {
	be := &fakeBackend{script: policyProbeScriptWithout("protected-in-operand")} // returns Success for the phys IN operand
	f := newTestFactory(t, be, Options{PhysicalDatabase: "phys"})
	err := f.ProbeRewriterBuild(context.Background(), false)
	if err == nil || !strings.Contains(err.Error(), "probe=protected-in-operand") || !strings.Contains(err.Error(), "rewriter-go >= v0.16.0") {
		t.Fatalf("err = %v", err)
	}
}
```

`policyProbeScript` is a map from probe SQL to the expected response built from the same table the implementation uses (export the table as `TableReferenceProbes` so the test can derive correct answers and then break one). Every probe request must carry `ProtectedDatabases: [phys hg_safe hg_unsafe hg_promote]`; assert it in the fake.

In `build_test.go`, add a case to the existing probe-mismatch test family (`TestBuildServer_RefusesStartupOnStorageIntegrityProbeMismatch`, `:333`): with no `storage_integrity.tables` and an injected factory whose policy probe fails, `buildServer` refuses startup. Every build test that injects `stubRewriterFactory{}` (a factory without the probe method) into a rewriter-bearing config now fails startup by design: switch those tests to `siProbeStubRewriterFactory{}` and teach that stub the `TableReferenceProbes` answers (`ProbeRewriterBuild` on the stub replays the expected code/statement/SQL/message per probe name).

- [ ] **Step 2: Run to verify they fail**

Run: `bazel test //pkg/rewriter:rewriter_test //:housegate_test --test_filter='TestProbeRewriterBuild|TestBuildServer_RefusesStartup' --test_output=errors`
Expected: FAIL (`ProbeRewriterBuild` undefined).

- [ ] **Step 3: Implement the policy cases**

In `probe.go` add, with the fixed args of `storageIntegrityProbeArgs` extended by `ProtectedDatabases: append([]string{"phys"}, sitable.ReservedDatabases()...)` and `UpstreamLogicalDatabaseInContext: "db1"`:

```go
// TableReferenceProbes prove the spec 2026-09-26 policy. They run for every
// rewriter factory, with or without storage integrity, over the ordinary
// (non-SI) probe arguments. sqlAfterByEngine pins the two quoting styles.
var TableReferenceProbes = []storageIntegrityBuildProbe{
	{name: "param-from", sql: "SELECT * FROM {p:Identifier}", code: pb.RewriteCode_InvalidRewriteRequest,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED, sqlAfter: "SELECT * FROM {p:Identifier}",
		message: "query parameters are not supported in a database or table position"},
	{name: "protected-in-operand", sql: "SELECT * FROM db1.o WHERE a IN phys.`db2.x`", code: pb.RewriteCode_InvalidRewriteRequest,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED, sqlAfter: "SELECT * FROM db1.o WHERE a IN phys.`db2.x`",
		message: "protected database phys is not addressable"},
	{name: "in-operand-rewritten", sql: "SELECT * FROM db1.o WHERE a IN db1.o", code: pb.RewriteCode_Success,
		statementType: pb.StatementType_STATEMENT_TYPE_SELECT, message: "success",
		sqlAfterByEngine: map[string]string{
			EngineNative: `SELECT * FROM phys."db1.o" "db1.o" WHERE a IN phys."db1.o"`,
			EngineGRPC:   "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN phys.`db1.o`",
		}},
	{name: "insert-select-source-rewritten", sql: "INSERT INTO db1.o SELECT * FROM db1.o", code: pb.RewriteCode_Success,
		statementType: pb.StatementType_STATEMENT_TYPE_INSERT, message: "success",
		sqlAfterByEngine: map[string]string{
			EngineNative: `INSERT INTO phys."db1.o" SELECT * FROM phys."db1.o" "db1.o"`,
			EngineGRPC:   "INSERT INTO phys.`db1.o` SELECT * FROM phys.`db1.o` AS `db1.o`",
		}},
	{name: "merge-refused", sql: "SELECT * FROM merge('phys', 'db2')", code: pb.RewriteCode_UnsupportedStatement,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED, sqlAfter: "SELECT * FROM merge('phys', 'db2')",
		message: "table function merge is not accepted"},
	{name: "use-protected", sql: "USE phys", code: pb.RewriteCode_InvalidRewriteRequest,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED, sqlAfter: "USE phys",
		message: "protected database phys is not addressable"},
	{name: "unmodelled-refused", sql: "SYSTEM RELOAD CONFIG", code: pb.RewriteCode_UnsupportedStatement,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED, sqlAfter: "SYSTEM RELOAD CONFIG",
		message: "statement is not supported"},
}
```

The two `sqlAfterByEngine` strings are the corpus pins from Plan A Task 8 (`si_tr_in_own_rewritten`, `si_tr_insert_select_own_source_rewritten`) as `want_sql` / `want_sql_cpp`; copy them from the released corpus rather than from this listing if they differ. `ProbeRewriterBuild(ctx, si)` runs `TableReferenceProbes` with the non-SI args (no `storage_integrity` block, expecting `StorageIntegrityContractVersion == UNSPECIFIED`), then, when `si`, the existing `storageIntegrityBuildProbes`. Error text: `rewriter build probe (engine=%s probe=%s): …; deploy %s` with `TableReferenceProbeRequiredBuild`. `build.go`: run `ProbeRewriterBuild(ctx, siOptions.Enabled)` whenever `rwFactory != nil` (an injected factory that does not implement the interface: refuse startup with the same message as today's SI check, now unconditional), log `rewriter build verified`.

- [ ] **Step 4: Run**

Run: `bazel test //pkg/rewriter:rewriter_test //:housegate_test --test_output=errors`
Expected: PASS; the existing SI-probe tests still pass through the wrapper.

- [ ] **Step 5: Commit**

```bash
git add pkg/rewriter/probe.go pkg/rewriter/probe_test.go build.go build_test.go
git commit -m "feat(rewriter): the startup probe runs for every engine and proves the table-reference policy (spec T13)"
```

---

### Task 10: integration tests against a real relay

**Files:**
- Create: `pkg/integration/table_reference_hardening_test.go` (in the existing `//pkg/integration:integration_test` target; tagged `manual` by the BUILD file)

- [ ] **Step 1: Write the tests** — with the `testenv` helpers the other integration tests use (a docker ClickHouse 25.8, a relay built with the native engine and `POLYGLOT_SQL_FFI_PATH`, a tenant session for `db1`), assert:

```go
func TestTableReference_TenantSourcesResolveEndToEnd(t *testing.T) {
	env := testenv.Start(t, testenv.WithNativeRewriter())   // reuse the real helper names
	env.Exec(t, "CREATE TABLE db1.o (a UInt64) ENGINE = Memory")
	env.Exec(t, "INSERT INTO db1.o VALUES (1), (2)")
	env.Exec(t, "INSERT INTO db1.o SELECT * FROM db1.o")            // could not resolve before this design
	if n := env.QueryUInt64(t, "SELECT count() FROM db1.o WHERE a IN db1.o"); n != 4 {
		t.Fatalf("count = %d, want 4", n)
	}
}

func TestTableReference_RefusalsReachTheClientAsExceptions(t *testing.T) {
	env := testenv.Start(t, testenv.WithNativeRewriter())
	for _, sql := range []string{
		"SELECT * FROM {p:Identifier}",
		"SELECT * FROM db1.o WHERE a IN phys.`db2.x`",
		"SELECT * FROM merge('phys', '.*')",
		"USE phys",
		"DETACH TABLE db1.o",
	} {
		if err := env.ExecErr(sql); err == nil {
			t.Fatalf("%s: expected an Exception", sql)
		}
	}
}

func TestTableReference_DriverTrafficIsUnaffected(t *testing.T) {
	env := testenv.Start(t, testenv.WithNativeRewriter())
	drv := env.DriverSession(t) // SQL_sentio_driver marker, the co-located signer
	drv.Exec(t, "CREATE TABLE db1.d (a UInt64) ENGINE = MergeTree ORDER BY a")
	drv.Exec(t, "INSERT INTO db1.d VALUES (1)")
	drv.Exec(t, "SELECT * FROM db1.d WHERE a IN db1.d")
}
```

Add an SI-enabled variant (`testenv.WithStorageIntegrity("db1.t")`): `SELECT * FROM db1.o WHERE a IN db1.t` returns the safe read's rows; `CREATE MATERIALIZED VIEW db1.mv TO db1.`\x74` AS SELECT * FROM db1.o` against a Pending `db1.t` is refused with the `sitablestate` unreadable-header message. Read `pkg/integration/storage_integrity_table_state_test.go` for the table-state fixture (`sitable.Fake`) and reuse it.

- [ ] **Step 2: Run** — `bazel test //pkg/integration:integration_test --test_filter='TestTableReference_' --test_env=POLYGLOT_SQL_FFI_PATH=$HOME/Library/Caches/housegate/rewriter-ffi/v0.16.0/libpolyglot_sql_ffi.dylib --test_output=errors`
Expected: PASS. This is also where the T6 `joinGet` emitted form is exercised end to end: add `SELECT joinGet('db1.j', 'v', 1)` against a `Join` table if Plan A shipped the rewrite path.

- [ ] **Step 3: Commit**

```bash
git add pkg/integration/table_reference_hardening_test.go pkg/integration/BUILD.bazel
git commit -m "test(integration): table-reference hardening end to end"
```

---

### Task 11: docs, CI list, spec record, PR

**Files:**
- Modify: `CLAUDE.md` §4 (policy, switch, guard, minimum builds; replace the "Known table-reference gaps … fix is design-only" rough edge with the shipped state), `docs/table-reference-guard.md` (new operator note: the five rules, `observe` mode, the counter, the switch), `docs/superpowers/specs/2026-09-26-table-reference-hardening-design.md` §10.5 (the differential record from Plan A/B)
- Modify: `.github/workflows/ci.yml` only if a new integration *target* was added (Task 10 extends the existing one, so no change expected)

- [ ] **Step 1: Write the docs** (no hard wrapping); update the `CLAUDE.md` rough-edge entry the pre-commit hook added on 2026-09-26 to describe the shipped behaviour.

- [ ] **Step 2: Full verification**

Run: `bazel build //... && bazel test //... 2>&1 | tail -5 && bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_env=POLYGLOT_SQL_FFI_PATH=… --test_output=errors 2>&1 | tail -5`
Expected: all PASS; compare any failure against `main`.

- [ ] **Step 3: Commit, push, open the PR**

```bash
git add CLAUDE.md docs
git commit -m "docs: table-reference hardening operator notes and differential record"
git push -u origin feat/table-reference-hardening
gh pr create --title "feat: table-reference hardening (spec 2026-09-26)" --body "$(cat <<'EOF'
Housegate half of the table-reference hardening: protected_databases on every request, fail-closed rewrite with rewriter.fail_open_on_unavailable, the tablerefguard plugin, sitablestate escape refusal, hg_promote everywhere, and the always-on engine probe. Pins rewriter-go v0.16.0 / rewriter-proto v0.4.0; requires rewriter-grpc v0.16.0.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Ask the user before merging. Rollout after merge follows spec §11 steps 5–6 (devnet2 with `table_reference_guard.mode: observe` for one cycle; watch `clickhouse_proxy_tableref_guard_rejections_total{rule="identifier_placeholder"|"escaped_identifier"}`).
