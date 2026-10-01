# Table-Reference Hardening — Plan C v2: housegate

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Supersedes:** [2026-09-26-table-reference-hardening-housegate.md](2026-09-26-table-reference-hardening-housegate.md) (Plan C v1). Pre-flight that motivated every change: `/Users/uranuswch/Dev/housegate/housegate.tableref-plan-c/.superpowers/sdd/2026-09-26-table-reference-hardening-housegate/preflight.md`.

**Goal:** Make housegate send the protected namespace to the engines, guard ordinary sessions lexically, refuse the R5 settings that arrive in the native Query packet, refuse escaped identifiers in `sitablestate`, know `hg_promote` everywhere, and refuse to start against an engine that does not prove the table-reference policy — on top of the fail-closed rewrite that #217 already shipped.

**Architecture:** `pkg/rewriter` sends `protected_databases = [physical] ∪ sitable.ReservedDatabases()` on every request. `sireserved`'s lexical scanner moves to a leaf package `pkg/sqlsurface`, which also emits a token stream; the new `pkg/plugins/tablerefguard` plugin applies the spec's five lexical rules to ordinary sessions before `forward` and `rewrite`, with an `observe` mode. The new `pkg/plugins/querysettings` plugin refuses Query-packet settings with the engines' R5 name and value rules. `buildServer` runs a new table-reference probe (`ProbeTableReferencePolicy`, 12 measured cases) for every rewriter factory, and the existing storage-integrity probe on top when storage integrity is enabled. Engine pins move to rewriter-go v0.16.0 / rewriter-proto v0.4.0; rewriter-grpc v0.16.0 is the gRPC minimum.

**Tech Stack:** Go 1.26.3, Bazel 9.1.0 + Bzlmod (`bazel test //...`, `bazel mod tidy && bazel run //:gazelle`), Prometheus client, ClickHouse 25.8 docker-bound integration suite, rewriter-go native engine via the polyglot FFI library.

**Spec:** [docs/superpowers/specs/2026-09-26-table-reference-hardening-design.md](../specs/2026-09-26-table-reference-hardening-design.md) §5 (incl. "The system database"), §9.1–§9.7, §10.3–§10.4, §11, §13. Engines already released: rewriter-go v0.16.0 (`e7ba13c`, corpus re-pinned on main `28bcc7f`, sha256 `2afa6c3a9b0942b6150a9e5854cfd57d3c429daa743384a0f354ac201251a585`, 1106 cases), rewriter-grpc v0.16.0 (`c24ed33`, image `us-west1-docker.pkg.dev/sentio-352722/sentio/housegate-rewriter:0.16.0`), rewriter-proto v0.4.0.

**Working copy:** worktree `/Users/uranuswch/Dev/housegate/housegate.tableref-plan-c`, branch `feat/table-reference-hardening` (= origin/main `174f0f7`, #217 merged). Do not create another branch. Bazel is the ground truth; after adding files or imports run `bazel mod tidy && bazel run //:gazelle`. Fetch the FFI library once for native-engine tests:

```bash
FFI="$(bazel run //cmd:housegate -- fetch-rewriter-lib --tag v0.16.0 | tail -n 1)"
echo "$FFI"   # e.g. $HOME/Library/Caches/housegate/rewriter-ffi/v0.16.0/libpolyglot_sql_ffi.dylib
```

The integration suite needs docker and a `clickhouse` client in `tests/bin/` (CI installs one; locally symlink an installed `clickhouse` binary into `tests/bin/clickhouse` for the run and remove the symlink afterwards).

## What #217 already did (Plan C v1 Task 3 is dropped)

housegate#217 (`174f0f7`) implemented all of spec T8 / §9.1 and more: every non-`Success` rewriter answer is a `RejectedError` in both SI states (`pkg/rewriter/sentio.go`), `rewriter.fail_open_on_unavailable` (default false, `Config.Validate` refuses it with storage integrity, `pkg/config/config.go:383-385`) governs only `*rewriter.UnavailableError` (request never reached the transport and gRPC status `Unavailable`/`DeadlineExceeded`/`Canceled`) and an unbuildable rewriter at startup (`build.go` `rewriterStartupFailure`), `FailClosedOnError` is gone, `RejectUndecodableQuery` is unconditional, invalid-UTF-8 and empty-statement answers are refused, client messages are generic, and `docs/rewriter-fail-closed.md`, README and CLAUDE.md describe it; relay and integration tests pin it. **Nothing of v1 Task 3 remains.** Plan C only adds: a built rewriter that fails the new policy probe refuses startup even with the switch on (Task 9).

## Spec deviations — the controller must ratify before Task 11 records them

- **D1 (§9.5)** `SELECT * FROM merge('phys', 'db2')` answers `InvalidRewriteRequest` / `protected database phys is not addressable` in both v0.16.0 engines (T3 precedes T5), not T5. The probe pins T3 for it and proves T5 with `merge('db1', 'o')`.
- **D2 (§9.5)** The two rewrite probes use `… WHERE a IN db1.p` and `INSERT INTO db1.o SELECT * FROM db1.p` (both engines pin those raw outputs for the probe's exact request: rewriter-grpc `tests/rewriter_test.cc:6833-6834, 7470-7471`); "two accessed entries" is not checked (native reports one entry for `IN db1.o`).
- **D3 (§9.5)** Four probes beyond §9.5: `` SELECT * FROM hg_promote.`db2.x` `` (the only case that refuses an engine ignoring `protected_databases` — `phys` is protected as a `database_map` value anyway), `SELECT * FROM system.processes` (step 1 allowlist), `SELECT * FROM db1.o SETTINGS allow_experimental_analyzer = 0` and an engine-argument subquery (both answered `Success` by rewriter-go v0.15.0, so the probe enforces the v0.16.0 floor).
- **D4 (§9.5) — overruled by the controller.** Proposed: the eight storage-integrity probes keep their request args (no `protected_databases`), because rewriter-grpc's answer for those exact V2 requests with the field was not pinned anywhere. Ruling: the SI probes carry `protected_databases` like every production request, and Task 9 measures the C++ answer for that exact request. As built (`b213b1d`), `storageIntegrityProbeArgs` sets `ProtectedDatabases: protectedDatabases("phys")`; both v0.16.0 engines answer all eight cases byte-identically with and without the field, so no SI pin changed.
- **D5 (§5 "The system database" housegate bullet, §9.6)** The `PermissionCommitGateObserver` system-exemption narrowing is **not** in this plan: the 2026-09-30 user ruling ("分两步") put it in step 2, housegate/housegate#218 (scope item 3).
- **D6 (§9.7)** The Query-packet check has no `observe` mode. As built (Task 5 and 6 review rounds), it and the guard are wired on every server that forwards to ClickHouse, rewriter or not.
- **D7 (§9.2)** The guard counts unscannable statements (stray `$`, unterminated spans) under a sixth counter label, `scan`.

## Global Constraints

- `fail_open_on_unavailable` semantics are #217's; do not touch them.
- Guard rule names are exactly `reserved_name`, `physical_database`, `carrier_callable`, `identifier_placeholder`, `escaped_identifier` (spec §9.2), plus the `scan` label (D7); error shape `table-reference guard: <rule>: <detail>; the rewriter applies the same policy`; counter `clickhouse_proxy_tableref_guard_rejections_total{rule}`; config key `tableref_guard.mode` ∈ {`enforce` (default, also empty), `observe`} (spec §11).
- Guard and Query-packet check never run on maintenance or platform-operator sessions; driver sessions are ordinary. The guard also skips peer-trusted and forwarded-from-peer sessions (spec §9.2); the Query-packet check skips peer-trusted sessions **except** forwarded-from-peer ones, which the receiving host's engine governs (spec §9.7).
- The Query-packet name/value rules are a verbatim copy of rewriter-go v0.16.0 `internal/engine/settings.go:46-66` (`sqlBearingSettings`), `:71-74` (`analyzerSettings`), `:87-93` (`TrueLiteralSpelling`), `:98-100` (`SettingRefused`), `:122-125` (`_dialect` suffix, case-insensitive), identical to rewriter-grpc v0.16.0 `src/handlers/table_reference.cc:1332-1361`. Do not add or drop names without the engines (spec §13: `allow_deprecated_syntax_for_merge_tree` is deliberately absent).
- The protected list is `protectedDatabases(physical)` = `[physical] ∪ sitable.ReservedDatabases()`, physical first, deduplicated; nothing lists `hg_*` names by hand.
- Probe pins are measured values (pre-flight "Task 9 probe measurement"); changing one needs a re-measurement against both engines. Minimum builds: rewriter-go v0.16.0 (native), rewriter-grpc v0.16.0.
- CLAUDE.md conventions: `pkg/log` structured logging, `fmt.Errorf("…: %w")`, English comments, no hard-wrapped Markdown, gazelle-managed `BUILD.bazel`, plugin-owned `Config` types.
- Every commit message ends with the attribution trailer your session's instructions specify. Never merge or release without the user's explicit approval. A failing test that also fails on a clean `main` build is not a regression (CLAUDE.md); compare before chasing it.

## Review Focus

1. **A physical database named `default`**: `a UInt8 DEFAULT 0` in a CREATE must not trip G2; `default.x` must (Task 4).
2. **The physical name in a tenant literal** (`WHERE name = 'phys'`, the driver's `system.tables WHERE database = 'phys'`) passes; in a lookup argument (`joinGet('phys.t', …)`) it is refused (Task 4).
3. **A dotted quoted identifier** `` db1.`phys.x` `` is not a qualifier; `` `phys`.x `` and `phys /* c */ .x` are (Task 4, token stream).
4. **A backslash in a string literal inside a CREATE header** stays schema-only; a backslash in a quoted identifier makes the header unreadable (Task 7).
5. **An engine that ignores `protected_databases`**, and **rewriter-go v0.15.0**, both refuse startup with SI disabled (Task 9).
6. **Query-packet analyzer values**: `1`, `true`, `'1'`, `'true'` (any case) pass; `0`, `false`, ` 1`, `1.0`, `0x1`, empty are refused; old-format (`< 54429`) values pass only when exactly 1; forwarded-from-peer sessions are still checked (Task 6).
7. **Test doubles answer the probe exactly and never record it** (integration `RewriterMock.SeenSQL`, Task 9).

---

### Task 1: pin rewriter-go v0.16.0 / rewriter-proto v0.4.0 and the v0.16.0 FFI library

**Files:**
- Modify: `go.mod`, `go.sum` (`MODULE.bazel` / `MODULE.bazel.lock` should not change: `go_deps.from_file`, both modules already in `use_repo`)
- Modify: `.github/workflows/ci.yml:111` (`fetch-rewriter-lib --tag v0.13.0` → `v0.16.0`)
- Modify: `configs/local.server.yaml:48`, `configs/local.server-mock-remote.yaml:48`, `configs/local.server.json:32` (`native_library_release` `v0.13.0` → `v0.16.0`)
- Modify: `pkg/integration/storage_integrity_read_test.go:47,201,343,455` (skip text `--tag v0.13.0` → `--tag v0.16.0`)
- Modify: `CLAUDE.md:148,179,180`, `README.md:170` (minimum builds and FFI pin)

- [ ] **Step 1: Bump the modules**

```bash
cd /Users/uranuswch/Dev/housegate/housegate.tableref-plan-c
go get github.com/housegate/rewriter-go@v0.16.0 github.com/housegate/rewriter-proto@v0.4.0
go mod tidy
grep -nE 'rewriter-go|rewriter-proto|tobilg/polyglot' go.mod
```

Expected: `github.com/housegate/rewriter-go v0.16.0`, `github.com/housegate/rewriter-proto v0.4.0`, `github.com/tobilg/polyglot/packages/go v0.13.0 // indirect`.

- [ ] **Step 2: Re-sync Bazel and build**

```bash
bazel mod tidy && bazel run //:gazelle && bazel build //cmd:housegate
git status --short MODULE.bazel MODULE.bazel.lock
```

Expected: build succeeds; `MODULE.bazel*` unchanged (if `bazel mod tidy` rewrites `use_repo`, keep its result).

- [ ] **Step 3: Move every v0.13.0 FFI reference to v0.16.0**

```bash
sed -i '' 's/fetch-rewriter-lib --tag v0.13.0/fetch-rewriter-lib --tag v0.16.0/' .github/workflows/ci.yml
sed -i '' 's/native_library_release: v0.13.0/native_library_release: v0.16.0/' configs/local.server.yaml configs/local.server-mock-remote.yaml
sed -i '' 's/"native_library_release": "v0.13.0"/"native_library_release": "v0.16.0"/' configs/local.server.json
sed -i '' 's/fetch-rewriter-lib --tag v0.13.0/fetch-rewriter-lib --tag v0.16.0/' pkg/integration/storage_integrity_read_test.go
grep -rn 'v0\.13\.0' .github configs pkg/integration | grep -v '^docs/'
```

Expected: the last grep prints nothing.

In `CLAUDE.md`, replace the whole "**Current ordinary native FFI release pin.**" bullet (line 180) with:

```markdown
- **Current ordinary native FFI release pin.** `rewriter-go` v0.16.0 publishes [linux/amd64](https://github.com/housegate/rewriter-go/releases/download/v0.16.0/libpolyglot_sql_ffi-linux-x86_64.so) (`sha256:0a2bfaef183441885a1462a3381401f7b191dab25d1fab1364193effa72d2745`) and [darwin/arm64](https://github.com/housegate/rewriter-go/releases/download/v0.16.0/libpolyglot_sql_ffi-macos-arm64.dylib) (`sha256:f3a8b3f512ea93553d25b978532152912249733a717718e87c888171b0a73fac`), both built from Polyglot v0.13.0 (byte-identical to the v0.15.0 assets) and carrying the ABI required by its Go binding, the storage-integrity contract V2 and the table-reference policy of spec 2026-09-26. The sample configs deliberately leave the single `native_library_sha256` field empty so `ffifetch` selects the correct platform artifact and verifies it against the release `SHA256SUMS`; a per-host explicit pin must use the hash for that host's asset. Snapshot-query native admission remains a separate explicit measured-library/profile path; source equivalence does not make a release asset a member of an existing measured profile.
```

In `CLAUDE.md:179` replace `The native engine requires an FFI library built from rewriter-go >= v0.13.0 (polyglot >= v0.12.1 — the go.mod floor)` with `The native engine requires an FFI library built from rewriter-go >= v0.16.0 (polyglot >= v0.13.0 — the go.mod floor)`. In `CLAUDE.md:148` replace `rewriter-go v0.13.0 native and rewriter-grpc v0.15.0 are the minimum released builds` with `rewriter-go v0.16.0 native and rewriter-grpc v0.16.0 are the minimum released builds (the table-reference policy of spec 2026-09-26 sets the floor)`. In `README.md:170` replace `(rewriter-go v0.13.0+ native, rewriter-grpc v0.15.0+)` with `(rewriter-go v0.16.0+ native, rewriter-grpc v0.16.0+)`. `docs/rewriter-fail-closed.md` keeps its v0.13.0 measurement tables (they are a dated record).

- [ ] **Step 4: Baseline the unit and native-smoke tests**

```bash
bazel test //... 2>&1 | tail -15
bazel test //pkg/rewriter:rewriter_test --test_filter='TestNativeEngine' --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors
```

Expected: all unit targets pass; `TestNativeEngineSmoke` and `TestNativeEngineProbeSmoke` pass against v0.16.0 (the eight SI probes answer as before — measured).

- [ ] **Step 5: Baseline the integration suite on the new engine**

```bash
bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors 2>&1 | tail -40
```

Expected: pass. If a native-engine test fails, run the same target on a clean `origin/main` checkout with the v0.13.0 library; a failure that exists only here is caused by a v0.16.0 refusal. Fix the test only when spec §5/§13 says the shape is now refused (record the case in the commit message); otherwise stop and report the statement and the engine answer.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum .github/workflows/ci.yml configs pkg/integration/storage_integrity_read_test.go CLAUDE.md README.md
git add -u   # MODULE.bazel / BUILD.bazel only if bazel mod tidy or gazelle changed them
git commit -m "chore(deps): rewriter-go v0.16.0, rewriter-proto v0.4.0 and the v0.16.0 FFI library (table-reference hardening)"
```

---

### Task 2: send `protected_databases` on every request

**Files:**
- Modify: `pkg/rewriter/args.go` (`buildDynamicArgs` gains `protected []string`; new `protectedDatabases`)
- Modify: `pkg/rewriter/sentio.go:315, :347, :549` (the three `buildDynamicArgs` call sites)
- Create: `pkg/rewriter/args_test.go`

**Interfaces:**
- Produces: `func protectedDatabases(physical string) []string` (package-private; Task 9 uses it for the probe args); `buildDynamicArgs(databaseMap map[string]string, knownPhysical, protected []string, logicalCtx, physicalCtx, delim string, logicalToRemoteIndex map[string]string, remoteUpstreams map[string]*pb.RewriteTableDynamicArgs_RemoteUpstream, si *pb.StorageIntegrityArgs) *pb.RewriteTableDynamicArgs`.

- [ ] **Step 1: Write the failing test**

```go
// pkg/rewriter/args_test.go
package rewriter

import (
	"context"
	"slices"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"
)

func TestProtectedDatabases(t *testing.T) {
	for _, tc := range []struct {
		physical string
		want     []string
	}{
		{"phys", []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"}},
		{"", []string{"hg_safe", "hg_unsafe", "hg_promote"}},
		{"hg_safe", []string{"hg_safe", "hg_unsafe", "hg_promote"}},
	} {
		if got := protectedDatabases(tc.physical); !slices.Equal(got, tc.want) {
			t.Errorf("protectedDatabases(%q) = %v, want %v", tc.physical, got, tc.want)
		}
	}
}

// TestRewriteSendsProtectedDatabases is spec 2026-09-26 T1/T3: the protected
// namespace rides on every request whatever storage_integrity says, including
// the error-message reverse map.
func TestRewriteSendsProtectedDatabases(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT}}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{logical: "db1"})
	if _, err := rw.Rewrite(context.Background(), "SELECT 1", ""); err != nil {
		t.Fatal(err)
	}
	want := []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"}
	if got := be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetProtectedDatabases(); !slices.Equal(got, want) {
		t.Fatalf("Rewrite protected_databases = %v, want %v", got, want)
	}
	if _, err := rw.RewriteErrorMessage(context.Background(), "Table phys.x does not exist"); err != nil {
		t.Fatal(err)
	}
	if got := be.lastErrReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetProtectedDatabases(); !slices.Equal(got, want) {
		t.Fatalf("RewriteErrorMessage protected_databases = %v, want %v", got, want)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bazel run //:gazelle && bazel test //pkg/rewriter:rewriter_test --test_filter='TestProtectedDatabases|TestRewriteSendsProtectedDatabases' --test_output=errors`
Expected: FAIL to compile (`undefined: protectedDatabases`).

- [ ] **Step 3: Implement**

In `pkg/rewriter/args.go` add the import `"github.com/housegate/housegate/pkg/sitable"`, the `protected []string` parameter after `knownPhysical` (set `ProtectedDatabases: protected` in the literal), and:

```go
// protectedDatabases is the namespace caller SQL may never address (spec
// 2026-09-26 T1/T3): the deployment's physical database and the
// protocol-owned databases, physical first, deduplicated. It is sent on every
// request whatever storage_integrity.enabled says; an empty physical name is
// omitted.
func protectedDatabases(physical string) []string {
	reserved := sitable.ReservedDatabases()
	out := make([]string, 0, 1+len(reserved))
	if physical != "" {
		out = append(out, physical)
	}
	for _, db := range reserved {
		if db != physical {
			out = append(out, db)
		}
	}
	return out
}
```

Update the doc comment of `buildDynamicArgs` with one sentence: "`protected` is the protected namespace (protectedDatabases)." In `pkg/rewriter/sentio.go` change all three call sites with one replacement:

```bash
sed -i '' 's/buildDynamicArgs(dbMap, knownPhys, r.sess/buildDynamicArgs(dbMap, knownPhys, protectedDatabases(r.factory.options.PhysicalDatabase), r.sess/' pkg/rewriter/sentio.go
grep -c 'protectedDatabases(r.factory.options.PhysicalDatabase)' pkg/rewriter/sentio.go   # expect 3
```

- [ ] **Step 4: Run the package tests**

Run: `bazel test //pkg/rewriter:rewriter_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/rewriter/args.go pkg/rewriter/args_test.go pkg/rewriter/sentio.go pkg/rewriter/BUILD.bazel
git commit -m "feat(rewriter): send protected_databases on every rewrite request (spec 2026-09-26 T1/T3)"
```

---

### Task 3: move the lexical scanner to `pkg/sqlsurface` and add a token stream

**Files:**
- Create: `pkg/sqlsurface/surface.go`, `pkg/sqlsurface/surface_test.go`, `pkg/sqlsurface/BUILD.bazel` (gazelle)
- Replace: `pkg/plugins/sireserved/plugin.go` (scanner removed, behaviour unchanged); `pkg/plugins/sireserved/BUILD.bazel` (gazelle)
- Unchanged: `pkg/plugins/sireserved/plugin_test.go` — every scanner test there goes through the exported `ReservedNamespaceViolation` and must stay green untouched.

**Interfaces (produced):**
- `type sqlsurface.TokenKind uint8` with `TokenWord` (bare word, keyword or number: `[A-Za-z0-9_]` and bytes `>= 0x80`), `TokenQuoted` (backtick or double-quoted identifier, delimiters removed, doubled delimiters collapsed), `TokenString` (`'…'` literal or heredoc body, raw, never decoded), `TokenPunct` (any other single non-space byte).
- `type sqlsurface.Token struct { Kind TokenKind; Text string }`
- `type sqlsurface.Surfaces struct { OutsideLiterals, WithLiterals string; Tokens []Token }` — the two surfaces are byte-for-byte what `scanSQLSurfaces` produced.
- `type sqlsurface.Options struct { AllowStringEscapes bool }` — when true a backslash inside `'…'` is kept raw and escapes the next byte (so `\'` does not close the literal) instead of being refused.
- `func Scan(sql string) (Surfaces, error)` (= `ScanWith(sql, Options{})`), `func ScanWith(sql string, opts Options) (Surfaces, error)`.
- `var ErrStringLiteralBackslash, ErrEscapedQuotedIdentifier, ErrStrayDollar error` — same texts `sireserved` always reported; unterminated spans return other errors.
- `func Identifiers(surface string) []string`, `func IsIdentifierByte(b byte) bool`, `func ContainsIdentifierPlaceholder(surface string) bool`.

- [ ] **Step 1: Write the failing tests**

```go
// pkg/sqlsurface/surface_test.go
package sqlsurface

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestScanSurfaces(t *testing.T) {
	s, err := Scan("SELECT 'hg_safe' AS x /* hg_unsafe */ FROM `db1`.t")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(Identifiers(s.WithLiterals), "hg_safe") {
		t.Fatalf("literal content missing from the literal surface: %q", s.WithLiterals)
	}
	if slices.Contains(Identifiers(s.OutsideLiterals), "hg_safe") {
		t.Fatalf("literal content leaked onto the executable surface: %q", s.OutsideLiterals)
	}
	for _, surface := range []string{s.OutsideLiterals, s.WithLiterals} {
		if strings.Contains(surface, "hg_unsafe") {
			t.Fatalf("comment content leaked: %q", surface)
		}
		if !slices.Contains(Identifiers(surface), "db1") {
			t.Fatalf("quoted identifier missing: %q", surface)
		}
	}
}

func TestScanTokens(t *testing.T) {
	s, err := Scan("SELECT a.b, `x.y`, 'it''s', $t$body$t$ FROM phys/* c */.t -- tail\nWHERE f (1) AND physé = 2")
	if err != nil {
		t.Fatal(err)
	}
	want := []Token{
		{TokenWord, "SELECT"}, {TokenWord, "a"}, {TokenPunct, "."}, {TokenWord, "b"}, {TokenPunct, ","},
		{TokenQuoted, "x.y"}, {TokenPunct, ","}, {TokenString, "it s"}, {TokenPunct, ","}, {TokenString, "body"},
		{TokenWord, "FROM"}, {TokenWord, "phys"}, {TokenPunct, "."}, {TokenWord, "t"},
		{TokenWord, "WHERE"}, {TokenWord, "f"}, {TokenPunct, "("}, {TokenWord, "1"}, {TokenPunct, ")"},
		{TokenWord, "AND"}, {TokenWord, "physé"}, {TokenPunct, "="}, {TokenWord, "2"},
	}
	if !reflect.DeepEqual(s.Tokens, want) {
		t.Fatalf("tokens =\n%v\nwant\n%v", s.Tokens, want)
	}
}

func TestScanAllowStringEscapes(t *testing.T) {
	s, err := ScanWith(`SELECT 'a\'b', 'c' FROM t`, Options{AllowStringEscapes: true})
	if err != nil {
		t.Fatal(err)
	}
	want := []Token{{TokenWord, "SELECT"}, {TokenString, `a\'b`}, {TokenPunct, ","}, {TokenString, "c"}, {TokenWord, "FROM"}, {TokenWord, "t"}}
	if !reflect.DeepEqual(s.Tokens, want) {
		t.Fatalf("tokens = %v, want %v", s.Tokens, want)
	}
}

func TestScanErrors(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		opts Options
		want error
	}{
		{`SELECT 'a\'b'`, Options{}, ErrStringLiteralBackslash},
		{"SELECT `a\\x74`", Options{AllowStringEscapes: true}, ErrEscapedQuotedIdentifier},
		{`SELECT "a\x74"`, Options{}, ErrEscapedQuotedIdentifier},
		{"SELECT $ FROM t", Options{}, ErrStrayDollar},
	} {
		if _, err := ScanWith(tc.sql, tc.opts); !errors.Is(err, tc.want) {
			t.Errorf("ScanWith(%q) err = %v, want %v", tc.sql, err, tc.want)
		}
	}
	for _, sql := range []string{"SELECT 'x", "SELECT /* x", "SELECT `x", "SELECT $$x", `SELECT 'abc\`} {
		if _, err := ScanWith(sql, Options{AllowStringEscapes: true}); err == nil {
			t.Errorf("ScanWith(%q) accepted an unterminated span", sql)
		}
	}
}

func TestContainsIdentifierPlaceholder(t *testing.T) {
	for sql, want := range map[string]bool{
		"SELECT * FROM {p:Identifier}":     true,
		"SELECT * FROM { p : identifier }": true,
		"SELECT {v:UInt64}":                false,
		"SELECT 1":                         false,
	} {
		if got := ContainsIdentifierPlaceholder(sql); got != want {
			t.Errorf("ContainsIdentifierPlaceholder(%q) = %v, want %v", sql, got, want)
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bazel run //:gazelle && bazel test //pkg/sqlsurface:sqlsurface_test --test_output=errors`
Expected: FAIL (package has no non-test source).

- [ ] **Step 3: Write `pkg/sqlsurface/surface.go`**

```go
// Package sqlsurface splits ClickHouse SQL into the lexical views the
// proxy-side guards reason on (Spec N D1): the executable text with string
// and heredoc contents blanked, the same text with those contents kept, and a
// token stream without whitespace or comments. It never parses or rewrites
// SQL, and every span it cannot model with certainty is an error.
package sqlsurface

import (
	"errors"
	"fmt"
	"strings"
)

// Errors a scan reports for spans the guards refuse. The texts are the ones
// the storage-integrity reserved-name guard has always reported.
var (
	ErrStringLiteralBackslash  = errors.New("backslash-bearing single-quoted string literal is not accepted by the storage-integrity guard")
	ErrEscapedQuotedIdentifier = errors.New("escaped quoted identifier is not accepted by the storage-integrity guard")
	ErrStrayDollar             = errors.New("stray $ is not a heredoc opener and is not accepted by the storage-integrity guard")
)

// TokenKind classifies a Token.
type TokenKind uint8

const (
	// TokenWord is a bare word, keyword or number: a run of [A-Za-z0-9_] and
	// non-ASCII bytes.
	TokenWord TokenKind = iota
	// TokenQuoted is a backtick or double-quoted identifier, delimiters
	// removed and doubled delimiters collapsed.
	TokenQuoted
	// TokenString is a single-quoted literal or a heredoc body, raw: escapes
	// are never decoded.
	TokenString
	// TokenPunct is any other single non-space byte.
	TokenPunct
)

// Token is one lexical token outside comments and whitespace.
type Token struct {
	Kind TokenKind
	Text string
}

// Surfaces are the lexical views of one statement.
type Surfaces struct {
	// OutsideLiterals keeps executable SQL but blanks comments and string
	// literals. It is the only surface used for placeholder syntax.
	OutsideLiterals string
	// WithLiterals additionally keeps string contents because ClickHouse
	// table functions interpret some literal arguments as identifiers.
	WithLiterals string
	// Tokens is the statement's token stream; comments and whitespace are
	// dropped, so adjacency in Tokens is adjacency in ClickHouse's lexer.
	Tokens []Token
}

// Options tune a scan.
type Options struct {
	// AllowStringEscapes keeps a backslash inside a single-quoted literal,
	// raw, and lets it escape the next byte (so \' does not close the
	// literal), instead of refusing the statement. Ordinary-session guards
	// set it: tenants legitimately write 'a\nb'. Privileged-session guards
	// leave it off, because decoding escapes is where an encoded name such
	// as hg\x5Fsafe would hide.
	AllowStringEscapes bool
}

// Scan is ScanWith with default options.
func Scan(sql string) (Surfaces, error) { return ScanWith(sql, Options{}) }

// ScanWith ignores every ClickHouse comment form, retains quoted identifiers,
// models heredoc string literals, and produces the two surfaces and the token
// stream. Case order is the lexer's precedence and matches the grammar: a
// single quote and a comment marker both outrank a heredoc opener, and a
// heredoc opener outranks everything inside its own body.
func ScanWith(sql string, opts Options) (Surfaces, error) {
	var outside, withLiterals strings.Builder
	outside.Grow(len(sql))
	withLiterals.Grow(len(sql))
	var tokens []Token
	word := -1 // start offset of the bare word being read, or -1
	flushWord := func(end int) {
		if word >= 0 {
			tokens = append(tokens, Token{Kind: TokenWord, Text: sql[word:end]})
			word = -1
		}
	}

	for i := 0; i < len(sql); {
		switch {
		case sql[i] == '\'':
			flushWord(i)
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, literal, err := consumeStringLiteral(sql, i, opts.AllowStringEscapes)
			if err != nil {
				return Surfaces{}, err
			}
			withLiterals.WriteString(literal)
			withLiterals.WriteByte(' ')
			tokens = append(tokens, Token{Kind: TokenString, Text: literal})
			i = next

		case hasPrefixAt(sql, i, "--") || hasPrefixAt(sql, i, "//") || sql[i] == '#':
			flushWord(i)
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			i = consumeLineComment(sql, i)

		case hasPrefixAt(sql, i, "/*"):
			flushWord(i)
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, err := consumeBlockComment(sql, i)
			if err != nil {
				return Surfaces{}, err
			}
			i = next

		case sql[i] == '`' || sql[i] == '"':
			flushWord(i)
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, identifier, err := consumeQuotedIdentifier(sql, i, sql[i])
			if err != nil {
				return Surfaces{}, err
			}
			outside.WriteString(identifier)
			outside.WriteByte(' ')
			withLiterals.WriteString(identifier)
			withLiterals.WriteByte(' ')
			tokens = append(tokens, Token{Kind: TokenQuoted, Text: identifier})
			i = next

		case sql[i] == '$':
			// ClickHouse heredoc: $$body$$ or $tag$body$tag$. The body is a
			// string literal, so it is blanked from OutsideLiterals and written
			// verbatim to WithLiterals -- table functions read literal arguments
			// as identifiers, so merge($$hg_safe$$, ...) must still be caught.
			// A `$` that opens no well-formed heredoc is refused: copying it
			// through is what let a comment marker inside a heredoc blank the
			// rest of a statement from both surfaces (Spec N D1).
			flushWord(i)
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, body, err := consumeHeredoc(sql, i)
			if err != nil {
				return Surfaces{}, err
			}
			withLiterals.WriteString(body)
			withLiterals.WriteByte(' ')
			tokens = append(tokens, Token{Kind: TokenString, Text: body})
			i = next

		default:
			b := sql[i]
			outside.WriteByte(b)
			withLiterals.WriteByte(b)
			switch {
			case isTokenWordByte(b):
				if word < 0 {
					word = i
				}
			case isSpace(b):
				flushWord(i)
			default:
				flushWord(i)
				tokens = append(tokens, Token{Kind: TokenPunct, Text: sql[i : i+1]})
			}
			i++
		}
	}
	flushWord(len(sql))
	return Surfaces{OutsideLiterals: outside.String(), WithLiterals: withLiterals.String(), Tokens: tokens}, nil
}

func consumeStringLiteral(sql string, start int, allowEscapes bool) (int, string, error) {
	var literal strings.Builder
	for i := start + 1; i < len(sql); {
		switch sql[i] {
		case '\\':
			if !allowEscapes {
				return 0, "", ErrStringLiteralBackslash
			}
			if i+1 >= len(sql) {
				return 0, "", fmt.Errorf("unterminated single-quoted string literal")
			}
			literal.WriteString(sql[i : i+2])
			i += 2
		case '\'':
			if i+1 < len(sql) && sql[i+1] == '\'' {
				literal.WriteByte(' ')
				i += 2
				continue
			}
			return i + 1, literal.String(), nil
		default:
			literal.WriteByte(sql[i])
			i++
		}
	}
	return 0, "", fmt.Errorf("unterminated single-quoted string literal")
}
```

Then move, **verbatim** from `pkg/plugins/sireserved/plugin.go`, these functions with their doc comments into `surface.go`: `consumeHeredoc` (lines 215-254; change its stray-`$` return to `return 0, "", ErrStrayDollar`), `isHeredocTagByte` (256-264), `containsIdentifierPlaceholder` (266-285, renamed `ContainsIdentifierPlaceholder`), `consumeLineComment` (340-346), `consumeBlockComment` (348-366), `consumeQuotedIdentifier` (368-387; change its backslash return to `return 0, "", ErrEscapedQuotedIdentifier`), `identifiers` (389-404, renamed `Identifiers`), `isIdentifierByte` (406-409, renamed `IsIdentifierByte`; update the one call inside `Identifiers`), `hasPrefixAt` (411-413). Add:

```go
// isTokenWordByte reports a byte of a bare word in the token stream: an ASCII
// identifier byte or any byte of a multi-byte UTF-8 sequence.
func isTokenWordByte(b byte) bool { return IsIdentifierByte(b) || b >= 0x80 }

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f' || b == '\v'
}
```

Give `ContainsIdentifierPlaceholder`, `Identifiers` and `IsIdentifierByte` one-line doc comments (exported symbols).

- [ ] **Step 4: Replace `pkg/plugins/sireserved/plugin.go`**

Keep `Plugin`, `OnQuery`, the three marker methods, `ReservedNamespaceViolation`, `reservedNamespaceViolationOnSurface`, `objectCarrierCallable`, `isObjectCarrierName` and the interface assertions with their comments; delete `sqlSurfaces`, `scanSQLSurfaces`, `consumeStringLiteral`, `consumeHeredoc`, `isHeredocTagByte`, `containsIdentifierPlaceholder`, `consumeLineComment`, `consumeBlockComment`, `consumeQuotedIdentifier`, `identifiers`, `isIdentifierByte`, `hasPrefixAt`. Then make these edits (import `"github.com/housegate/housegate/pkg/sqlsurface"`):

```go
// in OnQuery
	surfaces, err := sqlsurface.Scan(sql)
	if err != nil {
		return fmt.Errorf("storage-integrity guard could not scan the statement: %w", err)
	}
	if sqlsurface.ContainsIdentifierPlaceholder(surfaces.OutsideLiterals) {
		return fmt.Errorf("storage-integrity guard refuses ClickHouse Identifier placeholders on privileged proxy-bypass sessions; use a direct ClickHouse connection for physical access")
	}
	name := reservedNamespaceViolationOnSurface(surfaces.WithLiterals, p.ReservedDatabases, p.ReservedRowIDColumn)
	// … unchanged …
	if carrier := objectCarrierCallable(surfaces.OutsideLiterals); carrier != "" {

// in ReservedNamespaceViolation
	surfaces, err := sqlsurface.Scan(sql)
	if err != nil {
		return "", err
	}
	return reservedNamespaceViolationOnSurface(surfaces.WithLiterals, databases, rowIDColumn), nil

// in reservedNamespaceViolationOnSurface
	for _, identifier := range sqlsurface.Identifiers(surface) {

// in objectCarrierCallable (two places)
		if !sqlsurface.IsIdentifierByte(sql[i]) {
		for i < len(sql) && sqlsurface.IsIdentifierByte(sql[i]) {
```

`isObjectCarrierName` and its list stay exactly as they are (spec §9.2: `sireserved` keeps its rules).

- [ ] **Step 5: Run both packages**

Run: `bazel run //:gazelle && bazel test //pkg/sqlsurface:sqlsurface_test //pkg/plugins/sireserved:sireserved_test --test_output=errors`
Expected: PASS; `sireserved_test` unchanged.

- [ ] **Step 6: Commit**

```bash
git add pkg/sqlsurface pkg/plugins/sireserved
git commit -m "refactor(sqlsurface): extract sireserved's lexical scanner and add a token stream"
```

---

### Task 4: the `tablerefguard` plugin (T9)

**Files:**
- Create: `pkg/plugins/tablerefguard/plugin.go`, `pkg/plugins/tablerefguard/config.go`, `pkg/plugins/tablerefguard/plugin_test.go`, `pkg/plugins/tablerefguard/BUILD.bazel` (gazelle)

**Interfaces:**
- Consumes: `sqlsurface.ScanWith`, `Options{AllowStringEscapes: true}`, `Tokens`, `Identifiers`, `ContainsIdentifierPlaceholder`, `ErrEscapedQuotedIdentifier`, `ErrStrayDollar`.
- Produces: `type Mode string` (`ModeEnforce`, `ModeObserve`); `type Plugin struct { PhysicalDatabase string; ReservedDatabases []string; Mode Mode }`; `OnQuery`; `RunOnForward() == true`; `RunOnPeerTrust() == true`; `RejectUndecodableQuery() == true`; `func Check(sql, physicalDatabase string, reserved []string) (rule, detail string)` (`rule == ""` passes); rule constants `RuleReservedName`, `RulePhysicalDatabase`, `RuleCarrierCallable`, `RuleIdentifierPlaceholder`, `RuleEscapedIdentifier`, `RuleScan`; `type Config struct { Mode string }` with `Validate() error` (Task 5 wires it).

- [ ] **Step 1: Write the failing tests**

```go
// pkg/plugins/tablerefguard/plugin_test.go
package tablerefguard

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

var reserved = []string{"hg_safe", "hg_unsafe", "hg_promote"}

func TestCheck(t *testing.T) {
	for _, tc := range []struct{ name, sql, phys, want string }{
		// G1 reserved names: every surface but comments.
		{"reserved qualifier", "SELECT * FROM hg_safe.db1__t", "phys", RuleReservedName},
		{"reserved promote", "SELECT * FROM hg_promote.db1__t", "phys", RuleReservedName},
		{"reserved in a literal", "SELECT * FROM db1.o WHERE s = 'hg_unsafe'", "phys", RuleReservedName},
		{"reserved in a carrier literal", "SELECT * FROM merge('hg_unsafe', 'x')", "phys", RuleReservedName},
		{"reserved in a comment", "SELECT 1 /* hg_safe */", "phys", ""},
		// G2 physical database.
		{"phys qualifier", "SELECT * FROM phys.`db2.x`", "phys", RulePhysicalDatabase},
		{"phys backtick qualifier", "SELECT * FROM `phys`.`db2.x`", "phys", RulePhysicalDatabase},
		{"phys double-quote qualifier", `SELECT * FROM "phys".x`, "phys", RulePhysicalDatabase},
		{"phys qualifier across a comment", "SELECT * FROM phys/* c */.x", "phys", RulePhysicalDatabase},
		{"phys IN operand", "SELECT * FROM db1.o WHERE a IN phys.`db2.x`", "phys", RulePhysicalDatabase},
		{"use phys", "USE phys", "phys", RulePhysicalDatabase},
		{"use quoted phys", "USE `phys`", "phys", RulePhysicalDatabase},
		{"show tables from phys", "SHOW TABLES FROM phys", "phys", RulePhysicalDatabase},
		{"show columns in phys", "SHOW COLUMNS FROM t IN phys", "phys", RulePhysicalDatabase},
		{"lookup literal", "SELECT joinGet('phys.`db2.x`', 'v', 1)", "phys", RulePhysicalDatabase},
		{"lookup literal, space before the parenthesis", "SELECT dictGet ('phys.d', 'v', 1)", "phys", RulePhysicalDatabase},
		{"lookup database argument", "SELECT hasColumnInTable('phys', 't', 'c')", "phys", RulePhysicalDatabase},
		{"lookup on a tenant table", "SELECT hasColumnInTable('db1', 'o', 'a')", "phys", ""},
		{"phys as a column", "SELECT phys FROM db1.o", "phys", ""},
		{"phys in an ordinary literal", "SELECT * FROM db1.o WHERE name = 'phys'", "phys", ""},
		{"phys in the driver's metadata read", "SELECT name FROM system.tables WHERE database = 'phys' AND name LIKE 'db1.%'", "phys", ""},
		{"dotted quoted name is not a qualifier", "SELECT * FROM db1.`phys.x`", "phys", ""},
		{"show databases like phys", "SHOW DATABASES LIKE 'phys%'", "phys", ""},
		{"physical database named default, DEFAULT keyword", "CREATE TABLE db1.n (a UInt8 DEFAULT 0) ENGINE = Memory", "default", ""},
		{"physical database named default, qualifier", "SELECT * FROM default.x", "default", RulePhysicalDatabase},
		{"no physical database configured", "SELECT * FROM phys.x", "", ""},
		// G3 the §5 T5 refused table functions.
		{"merge on an own table", "SELECT * FROM merge('db1', 'o')", "phys", RuleCarrierCallable},
		{"merge naming phys", "SELECT * FROM merge('phys', 'x')", "phys", RuleCarrierCallable},
		{"remote", "SELECT * FROM remote('h', db1.o)", "phys", RuleCarrierCallable},
		{"mergeTreeProjection", "SELECT * FROM mergeTreeProjection('db1', 'o', 'p')", "phys", RuleCarrierCallable},
		{"executable", "SELECT * FROM executable('x.sh', CSV, 'a UInt8')", "phys", RuleCarrierCallable},
		{"heredoc argument", "SELECT * FROM merge($$phys$$, 'x')", "phys", RuleCarrierCallable},
		{"numbers", "SELECT * FROM numbers(10)", "phys", ""},
		{"url is a non-goal", "SELECT * FROM url('http://x', CSV)", "phys", ""},
		{"a column named merge", "SELECT merge FROM db1.o", "phys", ""},
		// G4 identifier placeholders, position-blind by design.
		{"identifier placeholder", "SELECT * FROM {p:Identifier}", "phys", RuleIdentifierPlaceholder},
		{"column identifier placeholder (deliberate)", "SELECT {c:Identifier} FROM db1.o", "phys", RuleIdentifierPlaceholder},
		{"value placeholder", "SELECT * FROM db1.o WHERE a = {v:UInt64}", "phys", ""},
		// G5 escapes.
		{"escaped backtick identifier", "SELECT * FROM db1.`\\x74`", "phys", RuleEscapedIdentifier},
		{"escaped double-quoted identifier", `SELECT * FROM db1."\x74"`, "phys", RuleEscapedIdentifier},
		{"backslash in a string literal", `SELECT * FROM db1.o WHERE s = 'a\nb'`, "phys", ""},
		{"escaped quote in a string literal", `SELECT * FROM db1.o WHERE s = 'it\'s'`, "phys", ""},
		// Spec N D1 spans.
		{"stray dollar", "SELECT $ FROM db1.o", "phys", RuleScan},
		{"unterminated literal", "SELECT 'x FROM db1.o", "phys", RuleScan},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule, detail := Check(tc.sql, tc.phys, reserved)
			if rule != tc.want {
				t.Fatalf("rule = %q (%s), want %q", rule, detail, tc.want)
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

func queryFor(sess chsession.Session, sql string) *plugin.QueryContext {
	return &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
}

func TestOnQuery_SessionGate(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved, Mode: ModeEnforce}
	for _, tc := range []struct {
		name string
		set  func(*chsession.SessionState)
		want bool // refused?
	}{
		{"ordinary", func(*chsession.SessionState) {}, true},
		{"driver", func(s *chsession.SessionState) { s.SetIsDriver(true) }, true},
		{"origin-side forwarding", func(s *chsession.SessionState) { s.SetForwarding(true) }, true},
		{"maintenance", func(s *chsession.SessionState) { s.SetMaintenance(true) }, false},
		{"platform operator", func(s *chsession.SessionState) { s.SetPlatformOperator(true) }, false},
		{"peer trusted", func(s *chsession.SessionState) { s.SetPeerTrust("peer:9001") }, false},
		{"forwarded from peer", func(s *chsession.SessionState) { s.SetPeerTrustForwarded("peer:9001", true) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSessionForTest(t)
			tc.set(sess.State())
			err := p.OnQuery(context.Background(), queryFor(sess, "SELECT * FROM phys.`db2.x`"))
			if (err != nil) != tc.want {
				t.Fatalf("refused = %v, want %v (%v)", err != nil, tc.want, err)
			}
			if err != nil && err.Error() != "table-reference guard: physical_database: protected database phys is not addressable; the rewriter applies the same policy" {
				t.Fatalf("message = %q", err.Error())
			}
		})
	}
}

func TestOnQuery_EmptyModeEnforces(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved}
	if err := p.OnQuery(context.Background(), queryFor(newSessionForTest(t), "USE phys")); err == nil {
		t.Fatal("an empty mode must enforce")
	}
}

func TestOnQuery_ObserveModeCountsWithoutRefusing(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved, Mode: ModeObserve}
	before := testutil.ToFloat64(rejections.WithLabelValues(RuleIdentifierPlaceholder))
	if err := p.OnQuery(context.Background(), queryFor(newSessionForTest(t), "SELECT * FROM {p:Identifier}")); err != nil {
		t.Fatalf("observe mode must not refuse: %v", err)
	}
	if after := testutil.ToFloat64(rejections.WithLabelValues(RuleIdentifierPlaceholder)); after != before+1 {
		t.Fatalf("counter = %v, want %v", after, before+1)
	}
}

func TestConfigValidate(t *testing.T) {
	for _, mode := range []string{"", "enforce", "observe"} {
		if err := (Config{Mode: mode}).Validate(); err != nil {
			t.Errorf("mode %q: %v", mode, err)
		}
	}
	if err := (Config{Mode: "audit"}).Validate(); err == nil || !strings.Contains(err.Error(), `tableref_guard.mode "audit" is invalid`) {
		t.Fatalf("err = %v", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/tablerefguard:tablerefguard_test --test_output=errors`
Expected: FAIL to build.

- [ ] **Step 3: Implement `config.go`**

```go
package tablerefguard

import "fmt"

// Config is the operator surface of the table-reference guard (yaml
// `tableref_guard`). The guard itself is wired whenever a SQL rewriter is.
type Config struct {
	// Mode is "enforce" (the default, also when empty) or "observe": observe
	// logs and counts what enforce would refuse, for rollout (spec 2026-09-26
	// §11 step 5).
	Mode string `json:"mode" yaml:"mode"`
}

// Validate rejects an unknown mode.
func (c Config) Validate() error {
	switch Mode(c.Mode) {
	case "", ModeEnforce, ModeObserve:
		return nil
	}
	return fmt.Errorf("tableref_guard.mode %q is invalid (want %q or %q)", c.Mode, ModeEnforce, ModeObserve)
}
```

- [ ] **Step 4: Implement `plugin.go`**

```go
// Package tablerefguard is the ordinary-session defence-in-depth guard of
// spec 2026-09-26 §9.2 (T9). It refuses the lexically decidable subset of the
// table-reference policy before forward and rewrite run; the rewriter engines
// remain the authority and the startup probe proves them.
package tablerefguard

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sqlsurface"
)

// Mode selects whether a rule hit refuses the statement or is only counted.
type Mode string

const (
	ModeEnforce Mode = "enforce"
	ModeObserve Mode = "observe"
)

// Rule names, used in the error and as the counter label.
const (
	RuleReservedName          = "reserved_name"
	RulePhysicalDatabase      = "physical_database"
	RuleCarrierCallable       = "carrier_callable"
	RuleIdentifierPlaceholder = "identifier_placeholder"
	RuleEscapedIdentifier     = "escaped_identifier"
	// RuleScan counts a statement the lexical model cannot scan (a stray $,
	// an unterminated quote, comment or heredoc).
	RuleScan = "scan"
)

var rejections = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "clickhouse_proxy_tableref_guard_rejections_total",
	Help: "Statements the table-reference guard refused, or would refuse in observe mode, by rule.",
}, []string{"rule"})

func init() { prometheus.MustRegister(rejections) }

// Plugin is the table-reference guard.
type Plugin struct {
	PhysicalDatabase  string
	ReservedDatabases []string
	Mode              Mode // "" enforces
}

// OnQuery applies Check to every ordinary session. Maintenance and
// platform-operator sessions are sireserved's; peer-trusted and
// forwarded-from-peer sessions carry SQL the origin already checked.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.Session == nil {
		return nil
	}
	snap := qctx.Session.State().Snapshot()
	if snap.Maintenance || snap.PlatformOperator || snap.IsPeerTrusted || snap.IsForwardedFromPeer {
		return nil
	}
	sql := qctx.OriginalSQL
	if qctx.Query != nil && qctx.Query.Body != "" {
		sql = qctx.Query.Body
	}
	rule, detail := Check(sql, p.PhysicalDatabase, p.ReservedDatabases)
	if rule == "" {
		return nil
	}
	rejections.WithLabelValues(rule).Inc()
	if p.Mode == ModeObserve {
		_, logger := log.FromContext(ctx)
		logger.Warnw("table-reference guard would refuse the statement (observe mode)", "rule", rule, "detail", detail)
		return nil
	}
	return fmt.Errorf("table-reference guard: %s: %s; the rewriter applies the same policy", rule, detail)
}

// RunOnForward keeps the guard on an origin-side forwarding session: the
// client is ordinary even after its session pivots to a peer.
func (*Plugin) RunOnForward() bool { return true }

// RunOnPeerTrust lets OnQuery decide; it skips peer-trusted sessions itself.
func (*Plugin) RunOnPeerTrust() bool { return true }

// RejectUndecodableQuery fails closed: an undecodable Query cannot be scanned.
func (*Plugin) RejectUndecodableQuery() bool { return true }

// Check applies the guard's rules in order and returns the first hit; an
// empty rule means the statement passes.
func Check(sql, physicalDatabase string, reserved []string) (rule, detail string) {
	surfaces, err := sqlsurface.ScanWith(sql, sqlsurface.Options{AllowStringEscapes: true})
	switch {
	case errors.Is(err, sqlsurface.ErrEscapedQuotedIdentifier):
		// G5: ClickHouse decodes escapes in quoted identifiers; the guard does not.
		return RuleEscapedIdentifier, "a backslash inside a quoted identifier is not accepted"
	case errors.Is(err, sqlsurface.ErrStrayDollar):
		return RuleScan, "a $ that opens no heredoc is not accepted"
	case err != nil:
		return RuleScan, "the statement has an unterminated quote, comment or heredoc"
	}
	// G1: a reserved database anywhere but a comment, string literals included.
	for _, id := range sqlsurface.Identifiers(surfaces.WithLiterals) {
		for _, name := range reserved {
			if name != "" && strings.EqualFold(id, name) {
				return RuleReservedName, fmt.Sprintf("reserved database %s is not addressable", name)
			}
		}
	}
	// G3 before G2, so merge('phys', …) is attributed to the carrier.
	if name := refusedTableFunctionCall(surfaces.Tokens); name != "" {
		return RuleCarrierCallable, fmt.Sprintf("table function %s is not accepted", name)
	}
	// G2: the physical database as a qualifier, a USE / SHOW target, or a
	// carrier or lookup argument.
	if physicalDatabase != "" && physicalDatabaseAddressed(surfaces.Tokens, physicalDatabase) {
		return RulePhysicalDatabase, fmt.Sprintf("protected database %s is not addressable", physicalDatabase)
	}
	// G4: Identifier placeholders, position-blind by design (spec §9.2).
	if sqlsurface.ContainsIdentifierPlaceholder(surfaces.OutsideLiterals) {
		return RuleIdentifierPlaceholder, "ClickHouse Identifier query parameters are not accepted"
	}
	return "", ""
}

func isName(t sqlsurface.Token) bool {
	return t.Kind == sqlsurface.TokenWord || t.Kind == sqlsurface.TokenQuoted
}

func isPunct(t sqlsurface.Token, p string) bool {
	return t.Kind == sqlsurface.TokenPunct && t.Text == p
}

func isWord(t sqlsurface.Token, w string) bool {
	return t.Kind == sqlsurface.TokenWord && strings.EqualFold(t.Text, w)
}

// refusedTableFunctionCall returns the first call to a §5 T5 refused table
// function.
func refusedTableFunctionCall(tokens []sqlsurface.Token) string {
	for i := 0; i+1 < len(tokens); i++ {
		if isName(tokens[i]) && isPunct(tokens[i+1], "(") && isRefusedTableFunction(tokens[i].Text) {
			return tokens[i].Text
		}
	}
	return ""
}

// isRefusedTableFunction is spec 2026-09-26 §5 T5's refused table-function
// list, matched case-insensitively as ClickHouse resolves the names.
func isRefusedTableFunction(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "mergetree") {
		return true
	}
	switch lower {
	case "merge", "remote", "remotesecure", "cluster", "clusterallreplicas", "loop", "dictionary",
		"timeseriesdata", "timeseriestags", "timeseriesmetrics", "timeseriesselector",
		"prometheusquery", "prometheusqueryrange", "clickhouse",
		"mysql", "postgresql", "mongodb", "jdbc", "odbc", "executable", "fuzzquery", "fuzzjson":
		return true
	}
	return false
}

// isLookupFunction is spec 2026-09-26 T6's string-form lookup list.
func isLookupFunction(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "dictget") {
		return true
	}
	switch lower {
	case "joinget", "joingetornull", "dicthas", "dictisin", "hascolumnintable":
		return true
	}
	return false
}

// physicalDatabaseAddressed reports the physical database used as a
// qualifier (phys.t, `phys`.t, "phys".t, comments and whitespace allowed
// before the dot), as the object of a leading USE, as a FROM / IN object of a
// SHOW statement, or inside the argument list of a carrier or lookup callable
// as a name or as a string naming it or a phys.-prefixed table. Elsewhere the
// name is allowed: a column or alias may share it, and DEFAULT is a keyword
// when the physical database is called default.
func physicalDatabaseAddressed(tokens []sqlsurface.Token, physical string) bool {
	named := func(t sqlsurface.Token) bool { return isName(t) && t.Text == physical }
	for i := 0; i+1 < len(tokens); i++ {
		if named(tokens[i]) && isPunct(tokens[i+1], ".") {
			return true
		}
	}
	if len(tokens) >= 2 && isWord(tokens[0], "USE") && named(tokens[1]) {
		return true
	}
	if len(tokens) > 0 && isWord(tokens[0], "SHOW") {
		for i := 1; i+1 < len(tokens); i++ {
			if (isWord(tokens[i], "FROM") || isWord(tokens[i], "IN")) && named(tokens[i+1]) {
				return true
			}
		}
	}
	for i := 0; i+1 < len(tokens); i++ {
		callee := tokens[i]
		if !isName(callee) || !isPunct(tokens[i+1], "(") {
			continue
		}
		if !isLookupFunction(callee.Text) && !isRefusedTableFunction(callee.Text) {
			continue
		}
		depth := 0
	args:
		for j := i + 1; j < len(tokens); j++ {
			t := tokens[j]
			switch {
			case isPunct(t, "("):
				depth++
			case isPunct(t, ")"):
				depth--
				if depth == 0 {
					break args
				}
			case named(t):
				return true
			case t.Kind == sqlsurface.TokenString && (t.Text == physical || strings.HasPrefix(t.Text, physical+".")):
				return true
			}
		}
	}
	return false
}

var (
	_ plugin.QueryPlugin             = (*Plugin)(nil)
	_ plugin.StrictQueryDecodePlugin = (*Plugin)(nil)
	_ plugin.ForwardAware            = (*Plugin)(nil)
	_ plugin.PeerTrustAware          = (*Plugin)(nil)
)
```

- [ ] **Step 5: Run the tests**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/tablerefguard:tablerefguard_test --test_output=errors`
Expected: PASS for every row.

- [ ] **Step 6: Commit**

```bash
git add pkg/plugins/tablerefguard
git commit -m "feat(tablerefguard): lexical table-reference guard for ordinary sessions (spec 2026-09-26 T9)"
```

---

### Task 5: wire the guard and `tableref_guard` configuration

**Files:**
- Modify: `pkg/config/config.go` (field after `Rewriter`; `Validate` call), create `pkg/config/tableref_guard_test.go`
- Modify: `build.go` (right after the `sireserved` block at `:643-650`), `build_test.go`
- Modify: `configs/local.server.yaml`, `configs/local.server-mock-remote.yaml` (commented example)

- [ ] **Step 1: Write the failing tests**

```go
// pkg/config/tableref_guard_test.go
package config

import (
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestValidate_TableRefGuardMode(t *testing.T) {
	for _, mode := range []string{"", "enforce", "observe"} {
		c := minimalServerConfig(t)
		c.TableRefGuard.Mode = mode
		if err := c.Validate(); err != nil {
			t.Errorf("mode %q: %v", mode, err)
		}
	}
	c := minimalServerConfig(t)
	c.TableRefGuard.Mode = "audit"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), `tableref_guard.mode "audit" is invalid`) {
		t.Fatalf("err = %v", err)
	}
	var fromYAML Config
	if err := yaml.Unmarshal([]byte("tableref_guard:\n  mode: observe\n"), &fromYAML); err != nil {
		t.Fatal(err)
	}
	if fromYAML.TableRefGuard.Mode != "observe" {
		t.Fatalf("yaml key tableref_guard.mode not read: %+v", fromYAML.TableRefGuard)
	}
}
```

In `build_test.go` add the import `"github.com/housegate/housegate/pkg/plugins/tablerefguard"` and:

```go
func TestBuildServer_TableRefGuardWiring(t *testing.T) {
	for _, si := range []bool{false, true} {
		cfg := minimalServerCfg(t)
		cfg.Rewriter.PhysicalDatabase = "phys"
		cfg.TableRefGuard.Mode = "observe"
		var factory rewriter.Factory = stubRewriterFactory{}
		if si {
			cfg.StorageIntegrity.Tables = []string{"tenant.events"}
			factory = siProbeStubRewriterFactory{}
		}
		bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), Rewriter: factory}, nil)
		if err != nil {
			t.Fatalf("si=%v: buildServer: %v", si, err)
		}
		guardIndex, reservedIndex, forwardIndex, rewriteIndex := -1, -1, -1, -1
		var guard *tablerefguard.Plugin
		for i, candidate := range requireExternalChain(t, bs).QueryPlugins {
			switch typed := candidate.(type) {
			case *tablerefguard.Plugin:
				guard, guardIndex = typed, i
			case *sireserved.Plugin:
				reservedIndex = i
			case *forward.Plugin:
				forwardIndex = i
			case *rewrite.Plugin:
				rewriteIndex = i
			}
		}
		bs.teardown()
		if guard == nil {
			t.Fatalf("si=%v: tablerefguard not wired", si)
		}
		if guardIndex >= forwardIndex || guardIndex >= rewriteIndex || (si && guardIndex <= reservedIndex) {
			t.Fatalf("si=%v: guard=%d sireserved=%d forward=%d rewrite=%d", si, guardIndex, reservedIndex, forwardIndex, rewriteIndex)
		}
		if guard.PhysicalDatabase != "phys" || guard.Mode != tablerefguard.ModeObserve || !reflect.DeepEqual(guard.ReservedDatabases, sitable.ReservedDatabases()) {
			t.Fatalf("si=%v: guard = %+v", si, guard)
		}
	}

	defaultMode := minimalServerCfg(t)
	bs, err := buildServer(Options{Config: defaultMode, NetworkState: network.NewInMemoryNetworkState(), Rewriter: stubRewriterFactory{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range requireExternalChain(t, bs).QueryPlugins {
		if g, ok := candidate.(*tablerefguard.Plugin); ok && g.Mode != tablerefguard.ModeEnforce {
			t.Fatalf("default mode = %q, want enforce", g.Mode)
		}
	}
	bs.teardown()

	routerOnly, err := buildServer(Options{Config: minimalRouterOnlyCfg(t), NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer routerOnly.teardown()
	for _, candidate := range requireExternalChain(t, routerOnly).QueryPlugins {
		if _, ok := candidate.(*tablerefguard.Plugin); ok {
			t.Fatal("a router-only server has no rewriter and must not wire the guard")
		}
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `bazel test //pkg/config:config_test //:housegate_test --test_filter='TestValidate_TableRefGuardMode|TestBuildServer_TableRefGuardWiring' --test_output=errors`
Expected: FAIL to compile (`TableRefGuard` undefined).

- [ ] **Step 3: Implement**

`pkg/config/config.go`: import `"github.com/housegate/housegate/pkg/plugins/tablerefguard"`; after the `Rewriter` field add

```go
	TableRefGuard    tablerefguard.Config     `json:"tableref_guard"    yaml:"tableref_guard"`
```

and in `Validate`, right after the `rewriter.engine` switch:

```go
	if err := c.TableRefGuard.Validate(); err != nil {
		errs = append(errs, err)
	}
```

`build.go`: import `"github.com/housegate/housegate/pkg/plugins/tablerefguard"`; immediately after the `if siOptions.Enabled { … sireserved … }` block add

```go
	// Spec 2026-09-26 T9: the lexical table-reference guard runs on ordinary
	// sessions before forward and rewrite whenever a rewriter is configured.
	if rwFactory != nil {
		guardMode := tablerefguard.Mode(cfg.TableRefGuard.Mode)
		if guardMode == "" {
			guardMode = tablerefguard.ModeEnforce
		}
		queryPlugins = append(queryPlugins, &tablerefguard.Plugin{
			PhysicalDatabase:  cfg.Rewriter.PhysicalDatabase,
			ReservedDatabases: sitable.ReservedDatabases(),
			Mode:              guardMode,
		})
		log.Infow("table-reference guard enabled", "mode", string(guardMode))
	}
```

Sample configs: in `configs/local.server.yaml` and `configs/local.server-mock-remote.yaml`, directly after the `rewriter:` block, add

```yaml
# Lexical table-reference guard on ordinary sessions (spec 2026-09-26 T9).
# observe logs and counts clickhouse_proxy_tableref_guard_rejections_total
# instead of refusing; see docs/table-reference-hardening.md.
# tableref_guard:
#   mode: enforce
```

- [ ] **Step 4: Run**

Run: `bazel run //:gazelle && bazel test //:housegate_test //pkg/config:config_test --test_output=errors`
Expected: PASS (existing wiring tests included; `sample_configs_test.go` still loads the samples).

- [ ] **Step 5: Commit**

```bash
git add build.go build_test.go pkg/config configs BUILD.bazel
git commit -m "feat(build): wire the table-reference guard on every rewriter deployment (tableref_guard.mode)"
```

---

### Task 6: refuse R5 settings carried in the native Query packet (spec §9.7)

**Files:**
- Create: `pkg/plugins/querysettings/plugin.go`, `pkg/plugins/querysettings/plugin_test.go`, `pkg/plugins/querysettings/BUILD.bazel` (gazelle)
- Modify: `build.go` (inside the Task 5 `if rwFactory != nil` block, right after the guard), `build_test.go`

**Where and for whom (spec §9.7 "before forwarding and in every session kind the engines would govern"):** a `QueryPlugin` placed after `auth` (which sets the maintenance / operator / driver flags) and before `forward` and `rewrite`, wired exactly when a rewriter is. It skips maintenance and platform-operator sessions (the rewriter bypasses them); `RunOnPeerTrust() == false`, so the chain skips `remote()` loopback peer sessions but — through the `IsForwardedFromPeer` override — still runs it on a forwarded-from-peer session, whose SQL the receiving host's engine governs; `RunOnForward() == true`, so the origin also checks before pivoting (an older receiving host cannot then let it through). The refusal is a plugin error: the relay answers an Exception with code 403 and the message `table setting <name> is not accepted (native-protocol query setting)`, and the session stays usable (`relay.go:1029-1035`). Setting names arrive decoded, so the escaped-spelling rule of R5 does not apply.

**Interfaces (produced):** `func Refused(name, value string) bool`; `func RefusedNames() []string` (sorted, the fixed names incl. both analyzer names); `type Plugin struct{}` with `OnQuery`, `RunOnPeerTrust() == false`, `RunOnForward() == true`, `RejectUndecodableQuery() == true`.

- [ ] **Step 1: Write the failing tests**

```go
// pkg/plugins/querysettings/plugin_test.go
package querysettings

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	rewritergo "github.com/housegate/rewriter-go"
	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

func TestRefused(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		want        bool
	}{
		{"additional_table_filters", "{'db1.o': 'a = 1'}", true},
		{"additional_result_filter", "a = 1", true},
		{"parallel_replicas_custom_key", "a", true},
		{"dialect", "kusto", true},
		{"polyglot_dialect", "x", true},
		{"allow_experimental_polyglot_dialect", "1", true},
		{"allow_experimental_prql_dialect", "1", true},
		{"allow_experimental_kusto_dialect", "1", true},
		{"some_future_dialect", "1", true},
		{"enable_global_with_statement", "1", true},
		{"enable_global_with_statement", "0", true},
		{"compatibility", "21.1", true},
		{"implicit_table_at_top_level", "x", true},
		{"promql_table", "x", true},
		{"promql_database", "x", true},
		{"legacy_column_name_of_tuple_literal", "0", true},
		{"profile", "default", true},
		{"PROFILE", "default", true},
		{"enable_analyzer", "1", false},
		{"enable_analyzer", "true", false},
		{"enable_analyzer", "TRUE", false},
		{"enable_analyzer", "'1'", false},
		{"enable_analyzer", "'true'", false},
		{"Enable_Analyzer", "1", false},
		{"enable_analyzer", "0", true},
		{"enable_analyzer", "false", true},
		{"enable_analyzer", "'0'", true},
		{"enable_analyzer", " 1", true},
		{"enable_analyzer", "1.0", true},
		{"enable_analyzer", "0x1", true},
		{"enable_analyzer", "", true},
		{"allow_experimental_analyzer", "1", false},
		{"allow_experimental_analyzer", "0", true},
		{"max_threads", "4", false},
		{"SQL_x_auth_token", "x", false},
		{"enable_scopes_for_with_statement", "0", false},
		{"allow_deprecated_syntax_for_merge_tree", "1", false}, // spec §13: deliberately not R5
	} {
		if got := Refused(tc.name, tc.value); got != tc.want {
			t.Errorf("Refused(%q, %q) = %v, want %v", tc.name, tc.value, got, tc.want)
		}
	}
}

func TestRefusedOld(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value uint64
		want  bool
	}{
		{"enable_analyzer", 1, false},
		{"enable_analyzer", 0, true},
		{"enable_analyzer", 2, true},
		{"enable_global_with_statement", 1, true},
		{"max_threads", 0, false},
	} {
		if got := refusedOld(tc.name, tc.value); got != tc.want {
			t.Errorf("refusedOld(%q, %d) = %v, want %v", tc.name, tc.value, got, tc.want)
		}
	}
}

func newSession(t *testing.T) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return chsession.New(1, client)
}

func queryWith(sess chsession.Session, settings ...chproto.Setting) *plugin.QueryContext {
	return &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1", Query: &chproto.Query{Body: "SELECT 1", Settings: settings}}
}

func TestOnQuery(t *testing.T) {
	p := &Plugin{}
	ctx := context.Background()
	err := p.OnQuery(ctx, queryWith(newSession(t), chproto.Setting{Key: "SQL_x_auth_token", Value: "'t'", Custom: true}, chproto.Setting{Key: "enable_analyzer", Value: "0", Important: true}))
	if err == nil || err.Error() != "table setting enable_analyzer is not accepted (native-protocol query setting)" {
		t.Fatalf("err = %v", err)
	}
	if err := p.OnQuery(ctx, queryWith(newSession(t), chproto.Setting{Key: "enable_analyzer", Value: "1", Important: true})); err != nil {
		t.Fatalf("analyzer on: %v", err)
	}
	old := &plugin.QueryContext{Session: newSession(t), OriginalSQL: "SELECT 1", Query: &chproto.Query{Body: "SELECT 1", OldSettings: []chproto.OldSetting{{Key: "enable_analyzer", Value: 0}}}}
	if err := p.OnQuery(ctx, old); err == nil {
		t.Fatal("an old-format analyzer-off setting must be refused")
	}
	if err := p.OnQuery(ctx, &plugin.QueryContext{Session: newSession(t)}); err != nil {
		t.Fatalf("no Query packet: %v", err)
	}
}

func TestChainSessionKinds(t *testing.T) {
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{&Plugin{}}}
	for _, tc := range []struct {
		name string
		set  func(*chsession.SessionState)
		want bool // refused?
	}{
		{"ordinary", func(*chsession.SessionState) {}, true},
		{"driver", func(s *chsession.SessionState) { s.SetIsDriver(true) }, true},
		{"origin-side forwarding", func(s *chsession.SessionState) { s.SetForwarding(true) }, true},
		{"forwarded from peer", func(s *chsession.SessionState) { s.SetPeerTrustForwarded("peer:9001", true) }, true},
		{"peer-trusted remote() loopback", func(s *chsession.SessionState) { s.SetPeerTrust("peer:9001") }, false},
		{"maintenance", func(s *chsession.SessionState) { s.SetMaintenance(true) }, false},
		{"platform operator", func(s *chsession.SessionState) { s.SetPlatformOperator(true) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSession(t)
			tc.set(sess.State())
			err := chain.OnQuery(context.Background(), queryWith(sess, chproto.Setting{Key: "compatibility", Value: "21.1", Important: true}))
			if (err != nil) != tc.want {
				t.Fatalf("refused = %v, want %v (%v)", err != nil, tc.want, err)
			}
		})
	}
}

// TestRefusedMatchesTheNativeEngine pins the copied lists against the pinned
// rewriter-go engine: every name housegate refuses in the Query packet is one
// the engine refuses in SQL, with the same value rule. Opt-in (needs the FFI
// library); the integration suite repeats it for every system.settings name.
func TestRefusedMatchesTheNativeEngine(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; native engine FFI lib unavailable")
	}
	svc, err := rewritergo.NewService(lib)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	args := &pb.RewriteTableDynamicArgs{
		DatabaseMap: map[string]string{"db1": "phys"}, KnownPhysicalDatabases: []string{"phys"},
		UpstreamLogicalDatabaseInContext: "db1", Delim: "_",
		ProtectedDatabases: []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"},
	}
	names := append(RefusedNames(), "some_future_dialect", "max_threads", "enable_scopes_for_with_statement", "allow_deprecated_syntax_for_merge_tree")
	for _, name := range names {
		for _, value := range []string{"0", "1"} {
			resp, err := svc.Rewrite(context.Background(), &pb.RewriteSQLRequest{
				Sql: fmt.Sprintf("SELECT 1 SETTINGS `%s` = %s", name, value),
				Options: []*pb.RewriteOption{{Op: pb.RewriteOp_TableNameRewrite,
					Value: &pb.RewriteOption_TableNameArgs{TableNameArgs: &pb.RewriteTableNameArgs{DynamicArgs: args}}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			engineRefuses := resp.GetMessage() == "table setting "+name+" is not accepted"
			if !engineRefuses && resp.GetCode() != pb.RewriteCode_Success {
				t.Errorf("%s = %s: unexpected engine answer %s %q", name, value, resp.GetCode(), resp.GetMessage())
				continue
			}
			if got := Refused(name, value); got != engineRefuses {
				t.Errorf("%s = %s: querysettings refuses %v, engine refuses %v", name, value, got, engineRefuses)
			}
		}
	}
	if !strings.Contains(strings.Join(RefusedNames(), ","), "enable_analyzer") {
		t.Fatal("RefusedNames must list the analyzer switches")
	}
}
```

In `build_test.go` add the import `"github.com/housegate/housegate/pkg/plugins/querysettings"` and `authplugin "github.com/housegate/housegate/pkg/plugins/auth"`, and:

```go
func TestBuildServer_QuerySettingsWiring(t *testing.T) {
	cfg := minimalServerCfg(t)
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), Rewriter: stubRewriterFactory{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.teardown()
	authIndex, settingsIndex, forwardIndex, rewriteIndex := -1, -1, -1, -1
	for i, candidate := range requireExternalChain(t, bs).QueryPlugins {
		switch candidate.(type) {
		case *authplugin.Plugin:
			authIndex = i
		case *querysettings.Plugin:
			settingsIndex = i
		case *forward.Plugin:
			forwardIndex = i
		case *rewrite.Plugin:
			rewriteIndex = i
		}
	}
	if settingsIndex < 0 || settingsIndex <= authIndex || settingsIndex >= forwardIndex || settingsIndex >= rewriteIndex {
		t.Fatalf("auth=%d querysettings=%d forward=%d rewrite=%d", authIndex, settingsIndex, forwardIndex, rewriteIndex)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/querysettings:querysettings_test //:housegate_test --test_filter='TestRefused|TestOnQuery|TestChainSessionKinds|TestBuildServer_QuerySettingsWiring' --test_output=errors`
Expected: FAIL to build.

- [ ] **Step 3: Implement `pkg/plugins/querysettings/plugin.go`**

```go
// Package querysettings refuses, before forwarding, the native-protocol
// Query-packet settings that the rewriter engines refuse in SQL (spec
// 2026-09-26 §9.7, R5 and §13 round 2). Query-packet settings never pass
// through the rewriter, so without this check a client could send
// enable_analyzer = 0 or enable_global_with_statement = 0 and change what a
// name the rewriter already trusted binds to.
package querysettings

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/plugin"
)

// refusedWhateverTheValue mirrors rewriter-go v0.16.0
// internal/engine/settings.go:46-66 (sqlBearingSettings) and rewriter-grpc
// v0.16.0 src/handlers/table_reference.cc:1332-1354 (sqlBearingSetting):
// settings whose value is SQL evaluated against tables, the dialect switches,
// the name-binding settings and the old analyzer's name resolution. Keep the
// three lists equal; the integration suite checks them against the engine.
var refusedWhateverTheValue = map[string]bool{
	"additional_table_filters":            true,
	"additional_result_filter":            true,
	"parallel_replicas_custom_key":        true,
	"dialect":                             true,
	"polyglot_dialect":                    true,
	"allow_experimental_polyglot_dialect": true,
	"allow_experimental_prql_dialect":     true,
	"allow_experimental_kusto_dialect":    true,
	"enable_global_with_statement":        true,
	"compatibility":                       true,
	"implicit_table_at_top_level":         true,
	"promql_table":                        true,
	"promql_database":                     true,
	"legacy_column_name_of_tuple_literal": true,
	"profile":                             true,
}

// analyzerSettings mirrors rewriter-go internal/engine/settings.go:71-74: the
// analyzer switch and its alias, refused unless the value keeps the new
// analyzer on.
var analyzerSettings = map[string]bool{
	"enable_analyzer":             true,
	"allow_experimental_analyzer": true,
}

// Refused reports whether a Query-packet setting is refused: a listed name, or
// any name ending in _dialect, whatever the value (names compared
// case-insensitively, a deliberate over-match like the engines'); or an
// analyzer switch whose value is not a closed true spelling.
func Refused(name, value string) bool {
	lower := strings.ToLower(name)
	if refusedWhateverTheValue[lower] || strings.HasSuffix(lower, "_dialect") {
		return true
	}
	return analyzerSettings[lower] && !trueSpelling(value)
}

// trueSpelling mirrors rewriter-go TrueLiteralSpelling
// (internal/engine/settings.go:87-93): 1, true, '1', 'true', any case. Every
// other spelling ClickHouse also reads as true is refused. In the Query
// packet a non-custom value arrives as the string clickhouse-go formats
// (fmt.Sprint) and a custom one as a Field dump; both land in this list.
func trueSpelling(value string) bool {
	switch strings.ToLower(value) {
	case "1", "true", "'1'", "'true'":
		return true
	}
	return false
}

// refusedOld applies Refused to a pre-54429 setting, whose value is a UInt64:
// only exactly 1 keeps the analyzer on.
func refusedOld(name string, value uint64) bool {
	spelling := "0"
	if value == 1 {
		spelling = "1"
	}
	return Refused(name, spelling)
}

// RefusedNames returns the fixed refused names, analyzer switches included,
// sorted. The _dialect suffix rule is not enumerable and is not listed.
func RefusedNames() []string {
	out := make([]string, 0, len(refusedWhateverTheValue)+len(analyzerSettings))
	for name := range refusedWhateverTheValue {
		out = append(out, name)
	}
	for name := range analyzerSettings {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Plugin refuses a query whose Query packet carries a refused setting.
type Plugin struct{}

// OnQuery checks every Query-packet setting. Maintenance and
// platform-operator sessions bypass the rewriter and therefore this check.
func (*Plugin) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.Query == nil {
		return nil
	}
	if qctx.Session != nil {
		snap := qctx.Session.State().Snapshot()
		if snap.Maintenance || snap.PlatformOperator {
			return nil
		}
	}
	for _, s := range qctx.Query.Settings {
		if Refused(s.Key, s.Value) {
			return refusal(s.Key)
		}
	}
	for _, s := range qctx.Query.OldSettings {
		if refusedOld(s.Key, s.Value) {
			return refusal(s.Key)
		}
	}
	return nil
}

func refusal(name string) error {
	return fmt.Errorf("table setting %s is not accepted (native-protocol query setting)", name)
}

// RunOnPeerTrust skips remote() loopback peer sessions, whose SQL the origin
// rewrote; the chain still runs the check on forwarded-from-peer sessions.
func (*Plugin) RunOnPeerTrust() bool { return false }

// RunOnForward checks at the origin too, before a session pivots to a peer.
func (*Plugin) RunOnForward() bool { return true }

// RejectUndecodableQuery fails closed: undecodable settings cannot be checked.
func (*Plugin) RejectUndecodableQuery() bool { return true }

var (
	_ plugin.QueryPlugin             = (*Plugin)(nil)
	_ plugin.StrictQueryDecodePlugin = (*Plugin)(nil)
	_ plugin.ForwardAware            = (*Plugin)(nil)
	_ plugin.PeerTrustAware          = (*Plugin)(nil)
)
```

`build.go`: import `"github.com/housegate/housegate/pkg/plugins/querysettings"` and, inside the Task 5 block right after the guard's `append`, add

```go
		// Spec 2026-09-26 §9.7: R5 settings in the native Query packet never
		// reach the rewriter; refuse them here, after auth sets the session
		// flags and before forward and rewrite.
		queryPlugins = append(queryPlugins, &querysettings.Plugin{})
```

- [ ] **Step 4: Run**

```bash
bazel run //:gazelle
bazel test //pkg/plugins/querysettings:querysettings_test //:housegate_test --test_output=errors
bazel test //pkg/plugins/querysettings:querysettings_test --test_filter=TestRefusedMatchesTheNativeEngine --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors
```

Expected: PASS (the last command must not report SKIPPED).

- [ ] **Step 5: Commit**

```bash
git add pkg/plugins/querysettings build.go build_test.go BUILD.bazel
git commit -m "feat(querysettings): refuse R5 settings carried in the native Query packet (spec 2026-09-26 §9.7)"
```

---

### Task 7: `sitablestate` refuses backslashes inside quoted identifiers (T10)

**Files:**
- Modify: `pkg/plugins/sitablestate/lexer.go` (`readQuoted`, `:160-183`; `tokenize` doc comment `:50-52`)
- Test: `pkg/plugins/sitablestate/lexer_test.go`, `pkg/plugins/sitablestate/plugin_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `lexer_test.go`:

```go
func TestQuotedIdentifierBackslashIsUnreadable(t *testing.T) {
	for sql, wantReadable := range map[string]bool{
		"CREATE MATERIALIZED VIEW db1.mv TO db1.`\\x74` AS SELECT a FROM db1.o":       false,
		"CREATE MATERIALIZED VIEW db1.`\\x74` ENGINE = Memory AS SELECT a FROM db1.o": false,
		`CREATE MATERIALIZED VIEW db1.mv TO db1."\x74" AS SELECT a FROM db1.o`:        false,
		"CREATE MATERIALIZED VIEW db1.mv TO db1.t AS SELECT a FROM db1.o":             true,
	} {
		if _, ok := materializedViewHeader(sql); ok != wantReadable {
			t.Errorf("%s: readable = %v, want %v", sql, ok, wantReadable)
		}
	}
	// A string literal keeps decoding escapes: the CREATE stays schema-only.
	if createTableCarriesData(`CREATE TABLE db1.n (a String DEFAULT 'a\nb') ENGINE = Memory`) {
		t.Fatal("a backslash in a string literal must not make the CREATE data-carrying")
	}
	// A schema copy is schema-only today; an escaped name makes it uncertain.
	if !createTableCarriesData("CREATE TABLE db1.`\\x74` AS db1.o ENGINE = Memory") {
		t.Fatal("an escaped target name must fail closed as data-carrying")
	}
}
```

In `plugin_test.go`'s `TestDataCarryingCreationIntoGovernedTables` table add:

```go
		// Spec 2026-09-26 T10: ClickHouse reads `\x70` as p, the lexer cannot; refused whatever the engine reported.
		{"MV TO an escaped pending name", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.`\\x70` AS SELECT a FROM db1.o", accessed("db1.mv", "db1.o"), unreadableErr},
```

- [ ] **Step 2: Run to verify they fail**

Run: `bazel test //pkg/plugins/sitablestate:sitablestate_test --test_filter='TestQuotedIdentifierBackslashIsUnreadable|TestDataCarryingCreationIntoGovernedTables' --test_output=errors`
Expected: FAIL (escaped headers read as `x74` / `x70` today).

- [ ] **Step 3: Implement**

Replace the `case '\\':` arm of `readQuoted` with:

```go
		case '\\':
			if quote != '\'' {
				// ClickHouse decodes escapes inside quoted identifiers; this
				// lexer does not model that table, so the header is unreadable
				// and the caller fails closed (spec 2026-09-26 T10).
				return "", len(sql), false
			}
			if i+1 >= len(sql) {
				return "", len(sql), false
			}
			b.WriteByte(sql[i+1])
			i++
```

and extend the `tokenize` doc comment with: "A backslash inside a backtick or double-quoted identifier is such a span (spec 2026-09-26 T10); inside a single-quoted literal it still escapes the next byte."

- [ ] **Step 4: Run the package**

Run: `bazel test //pkg/plugins/sitablestate:sitablestate_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/plugins/sitablestate
git commit -m "fix(sitablestate): a backslash inside a quoted identifier makes the header unreadable (spec 2026-09-26 T10)"
```

---

### Task 8: `hg_promote` in every reserved list (T11)

**Files:**
- Modify: `build.go:644-650` (`sireserved` wiring), `pkg/rewriter/storage_integrity.go:297-314` (`NewStorageIntegrityScrubber`)
- Test: `build_test.go:181` (`TestBuildServer_StorageIntegrityReservedGuardWiring`), `pkg/rewriter/storage_integrity_test.go`

- [ ] **Step 1: Extend the tests**

In `TestBuildServer_StorageIntegrityReservedGuardWiring` replace

```go
	if !reflect.DeepEqual(guard.ReservedDatabases, []string{config.StorageIntegritySafeDatabase, config.StorageIntegrityUnsafeDatabase}) ||
```

with

```go
	if !reflect.DeepEqual(guard.ReservedDatabases, sitable.ReservedDatabases()) ||
```

Append to `pkg/rewriter/storage_integrity_test.go`:

```go
// TestStorageIntegrityScrubber_RedactsPromoteDatabase is spec 2026-09-26 T11.
func TestStorageIntegrityScrubber_RedactsPromoteDatabase(t *testing.T) {
	s := NewStorageIntegrityScrubber(sitable.NewFake(sitable.Pending).Current())
	if got := s.Scrub("Table hg_promote.db1__t does not exist"); strings.Contains(got, "hg_promote") {
		t.Fatalf("Scrub = %q, want hg_promote redacted", got)
	}
}
```

(add `"strings"` to the file's imports if it is missing).

- [ ] **Step 2: Run to verify they fail**

Run: `bazel test //:housegate_test //pkg/rewriter:rewriter_test --test_filter='TestBuildServer_StorageIntegrityReservedGuardWiring|TestStorageIntegrityScrubber' --test_output=errors`
Expected: FAIL.

- [ ] **Step 3: Implement**

`build.go`, in the `sireserved.Plugin` literal:

```go
		queryPlugins = append(queryPlugins, &sireserved.Plugin{
			ReservedDatabases:   sitable.ReservedDatabases(),
			ReservedRowIDColumn: rewriter.DefaultReservedRowIDColumn,
		})
```

(if `config` becomes unused in `build.go`, `go vet` will say so — it is used elsewhere today). `NewStorageIntegrityScrubber`:

```go
func NewStorageIntegrityScrubber(snap sitable.Snapshot) *StorageIntegrityScrubber {
	active := snap.Active()
	reserved := sitable.ReservedDatabases()
	// Qualified names must precede their bare database prefixes.
	pairs := make([]string, 0, len(active)*4+2*len(reserved)+2)
	for _, table := range active {
		phys := sitable.PhysicalTable(table.ID)
		pairs = append(pairs,
			sitable.SafeDatabase+"."+phys, table.ID,
			sitable.UnsafeDatabase+"."+phys, table.ID,
		)
	}
	for _, db := range reserved {
		pairs = append(pairs, db, storageIntegrityRedaction)
	}
	pairs = append(pairs, DefaultReservedRowIDColumn, storageIntegrityRedaction)
	return &StorageIntegrityScrubber{replacer: strings.NewReplacer(pairs...)}
}
```

- [ ] **Step 4: Run** — same command as Step 2: PASS; then `bazel test //:housegate_test //pkg/rewriter:rewriter_test --test_output=errors`: PASS.

- [ ] **Step 5: Commit**

```bash
git add build.go build_test.go pkg/rewriter/storage_integrity.go pkg/rewriter/storage_integrity_test.go
git commit -m "fix(storage-integrity): hg_promote joins the sireserved and scrubber reserved lists (spec 2026-09-26 T11)"
```

---

### Task 9: the table-reference probe runs for every rewriter (T13)

**Files:**
- Modify: `pkg/rewriter/probe.go` (policy probe, shared answer check, `TableReferenceProbeAnswer`, required-build texts)
- Modify: `pkg/rewriter/probe_test.go`, `pkg/rewriter/native_smoke_test.go` (`TestNativeEngineProbeSmoke`), `probe_test.go` `TestReleasedGRPCStorageIntegrityProbeSmoke`
- Modify: `build.go` (probe block before the SI checks, `:509-537`), `build_test.go` (stub factories + new tests)
- Modify: `rewriter_startup_gate_test.go` (`stubRewriterService.Rewrite`, `:221-223`)
- Modify: `pkg/integration/testenv/rewriter_mock.go` (`Rewrite`, `:162`)

**Interfaces:**
- Produces: `type TableReferenceProbeFactory interface { Factory; ProbeTableReferencePolicy(ctx context.Context) error }`; `(*SentioNetworkFactory).ProbeTableReferencePolicy`; `const TableReferenceProbeRequiredBuild = "rewriter-go >= v0.16.0 or rewriter-grpc >= v0.16.0 (table-reference policy, spec 2026-09-26)"`; `func TableReferenceProbeAnswer(req *pb.RewriteSQLRequest, engine string) (*pb.RewriteSQLResponse, bool)` — the conforming answer to a policy-probe request, for test doubles. `StorageIntegrityProbeFactory` / `ProbeStorageIntegrityBuild` keep their names and behaviour (sentio-node's test factories implement them), so the SI probe is unchanged apart from its required-build text.
- Consumes: `protectedDatabases` (Task 2).

- [ ] **Step 1: Write the failing tests**

Append to `pkg/rewriter/probe_test.go`:

```go
// policyProbeBackend answers every table-reference probe as a conforming
// engine of its kind, except the SQL in override.
type policyProbeBackend struct {
	fakeBackend
	engine    string
	override  map[string]*pb.RewriteSQLResponse
	requests  []*pb.RewriteSQLRequest
	deadlines []bool
}

func (b *policyProbeBackend) Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	b.requests = append(b.requests, req)
	_, hasDeadline := ctx.Deadline()
	b.deadlines = append(b.deadlines, hasDeadline)
	if resp, ok := b.override[req.GetSql()]; ok {
		return resp, nil
	}
	resp, ok := TableReferenceProbeAnswer(req, b.engine)
	if !ok {
		return nil, fmt.Errorf("unexpected table-reference probe request %q", req.GetSql())
	}
	return resp, nil
}

func newPolicyProbeFactory(be *policyProbeBackend) *SentioNetworkFactory {
	f := newFakeFactory(be)
	f.options.Engine = be.engine
	return f
}

func TestProbeTableReferencePolicy(t *testing.T) {
	for _, engine := range []string{EngineGRPC, EngineNative} {
		t.Run("conforming "+engine+" build passes", func(t *testing.T) {
			be := &policyProbeBackend{engine: engine}
			if err := newPolicyProbeFactory(be).ProbeTableReferencePolicy(context.Background()); err != nil {
				t.Fatalf("probe: %v", err)
			}
			if len(be.requests) != len(tableReferenceProbes) {
				t.Fatalf("requests = %d, want %d", len(be.requests), len(tableReferenceProbes))
			}
			for i, req := range be.requests {
				dyn := req.GetOptions()[0].GetTableNameArgs().GetDynamicArgs()
				if got := strings.Join(dyn.GetProtectedDatabases(), ","); got != "phys,hg_safe,hg_unsafe,hg_promote" {
					t.Fatalf("request %d protected_databases = %s", i, got)
				}
				if dyn.GetStorageIntegrity() != nil {
					t.Fatalf("request %d carries StorageIntegrityArgs; the policy probe models storage integrity disabled", i)
				}
				if !be.deadlines[i] {
					t.Fatalf("request %d had no deadline", i)
				}
			}
		})
	}

	refused := func(t *testing.T, be *policyProbeBackend, probeName string, extra ...string) {
		t.Helper()
		err := newPolicyProbeFactory(be).ProbeTableReferencePolicy(context.Background())
		if err == nil {
			t.Fatalf("probe passed, want probe=%s refused", probeName)
		}
		for _, want := range append([]string{"rewriter table-reference probe", "probe=" + probeName, TableReferenceProbeRequiredBuild}, extra...) {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %q, want %q", err, want)
			}
		}
	}
	success := func(sql string) *pb.RewriteSQLResponse {
		return &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, StatementType: pb.StatementType_STATEMENT_TYPE_SELECT, SqlAfterRewrite: sql, Message: "success"}
	}
	t.Run("an engine that ignores protected_databases is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineNative, override: map[string]*pb.RewriteSQLResponse{
			"SELECT * FROM hg_promote.`db2.x`": success(`SELECT * FROM hg_promote."db2.x"`),
		}}, "protected-reserved-database")
	})
	t.Run("rewriter-go v0.15.0 is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineNative, override: map[string]*pb.RewriteSQLResponse{
			"SELECT * FROM db1.o SETTINGS allow_experimental_analyzer = 0": success(`SELECT * FROM phys."db1.o" "db1.o" SETTINGS allow_experimental_analyzer = 0`),
		}}, "analyzer-off-refused")
	})
	t.Run("an unmodelled-class pass-through is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineGRPC, override: map[string]*pb.RewriteSQLResponse{
			"SYSTEM RELOAD CONFIG": success("SYSTEM RELOAD CONFIG"),
		}}, "unmodelled-class-refused")
	})
	t.Run("T5 answered before T3 is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineGRPC, override: map[string]*pb.RewriteSQLResponse{
			"SELECT * FROM merge('phys', 'db2')": {Code: pb.RewriteCode_UnsupportedStatement, SqlAfterRewrite: "SELECT * FROM merge('phys', 'db2')", Message: "table function merge is not accepted"},
		}}, "protected-carrier-argument", "code=UnsupportedStatement")
	})
	t.Run("the other engine's quoting is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineGRPC, override: map[string]*pb.RewriteSQLResponse{
			"SELECT * FROM db1.o WHERE a IN db1.p": success(`SELECT * FROM phys."db1.o" "db1.o" WHERE a IN phys."db1.p"`),
		}}, "in-operand-rewritten", "SQL fingerprint mismatch")
	})
	t.Run("a storage-integrity acknowledgement is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineGRPC, override: map[string]*pb.RewriteSQLResponse{
			"SELECT * FROM {p:Identifier}": {Code: pb.RewriteCode_InvalidRewriteRequest, SqlAfterRewrite: "SELECT * FROM {p:Identifier}",
				Message: "query parameters are not supported in a database or table position", StorageIntegrityContractVersion: StorageIntegrityContractV2},
		}}, "param-table-position", "acknowledgement")
	})
	t.Run("no backend", func(t *testing.T) {
		if err := newFakeFactory(nil).ProbeTableReferencePolicy(context.Background()); err == nil || !strings.Contains(err.Error(), "no rewrite backend") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestTableReferenceProbeAnswer(t *testing.T) {
	probe := &pb.RewriteSQLRequest{Sql: "USE phys", Options: []*pb.RewriteOption{rewriteOption(tableReferenceProbeArgs())}}
	resp, ok := TableReferenceProbeAnswer(probe, EngineGRPC)
	if !ok || resp.GetCode() != pb.RewriteCode_InvalidRewriteRequest || resp.GetSqlAfterRewrite() != "USE phys" ||
		resp.GetMessage() != "protected database phys is not addressable" {
		t.Fatalf("answer = %v, %v", resp, ok)
	}
	production := &pb.RewriteSQLRequest{Sql: "USE phys", Options: []*pb.RewriteOption{rewriteOption(
		buildDynamicArgs(map[string]string{"db1": "phys"}, []string{"phys"}, protectedDatabases("phys"), "db1", "phys", "_", nil, nil, nil))}}
	if _, ok := TableReferenceProbeAnswer(production, EngineGRPC); ok {
		t.Fatal("a production request (physical context set) must not be taken for a probe")
	}
	if _, ok := TableReferenceProbeAnswer(&pb.RewriteSQLRequest{Sql: "SELECT 1", Options: probe.GetOptions()}, EngineGRPC); ok {
		t.Fatal("SQL outside the probe table must not be answered")
	}
}
```

In `build_test.go` add, next to `stubRewriterFactory`:

```go
func (stubRewriterFactory) ProbeTableReferencePolicy(context.Context) error { return nil }

// unprobedRewriterFactory is a Factory that cannot prove the table-reference
// policy; buildServer must refuse it with or without storage integrity.
type unprobedRewriterFactory struct{}

func (unprobedRewriterFactory) NewRewriter(rewriter.Session) rewriter.Rewriter { return stubRewriter{} }
func (unprobedRewriterFactory) Close() error                                 { return nil }

// policyProbeStubRewriterFactory overrides the policy probe of the SI-capable
// stub so tests can fail it or observe its context.
type policyProbeStubRewriterFactory struct {
	siProbeStubRewriterFactory
	policyErr        error
	calls            *int
	deadlineObserved *bool
}

func (f policyProbeStubRewriterFactory) ProbeTableReferencePolicy(ctx context.Context) error {
	if f.calls != nil {
		*f.calls++
	}
	if f.deadlineObserved != nil {
		_, ok := ctx.Deadline()
		*f.deadlineObserved = ok
	}
	return f.policyErr
}

func TestBuildServer_TableReferenceProbeRunsForEveryRewriter(t *testing.T) {
	t.Run("an unprobed factory is refused without storage integrity", func(t *testing.T) {
		_, err := buildServer(Options{Config: minimalServerCfg(t), NetworkState: network.NewInMemoryNetworkState(), Rewriter: unprobedRewriterFactory{}}, nil)
		if err == nil || !strings.Contains(err.Error(), "TableReferenceProbeFactory") {
			t.Fatalf("err = %v", err)
		}
	})
	for _, si := range []bool{false, true} {
		t.Run(fmt.Sprintf("a failing probe refuses startup (si=%v)", si), func(t *testing.T) {
			cfg := minimalServerCfg(t)
			if si {
				cfg.StorageIntegrity.Tables = []string{"tenant.events"}
			}
			_, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(),
				Rewriter: policyProbeStubRewriterFactory{policyErr: errors.New("rewriter table-reference probe (engine=grpc probe=x): code=Success")}}, nil)
			if err == nil || !strings.Contains(err.Error(), "rewriter table-reference probe") {
				t.Fatalf("err = %v", err)
			}
		})
	}
	t.Run("a passing probe runs once with a bounded context", func(t *testing.T) {
		calls, deadline := 0, false
		bs, err := buildServer(Options{Config: minimalServerCfg(t), NetworkState: network.NewInMemoryNetworkState(),
			Rewriter: policyProbeStubRewriterFactory{calls: &calls, deadlineObserved: &deadline}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		bs.teardown()
		if calls != 1 || !deadline {
			t.Fatalf("calls = %d, deadline = %v", calls, deadline)
		}
	})
}
```

(`fmt` is already imported by `build_test.go`? If not, add it.)

- [ ] **Step 2: Run to verify they fail**

Run: `bazel test //pkg/rewriter:rewriter_test //:housegate_test --test_filter='TestProbeTableReferencePolicy|TestTableReferenceProbeAnswer|TestBuildServer_TableReferenceProbeRunsForEveryRewriter' --test_output=errors`
Expected: FAIL to compile (`ProbeTableReferencePolicy` undefined).

- [ ] **Step 3: Implement the probe in `pkg/rewriter/probe.go`**

Change `storageIntegrityProbeRequiredBuild` to `"rewriter-go >= v0.16.0 or rewriter-grpc >= v0.16.0 (storage-integrity contract V2)"` and add the import `"google.golang.org/protobuf/proto"`. Add:

```go
// TableReferenceProbeRequiredBuild is what a failed table-reference probe tells
// the operator to deploy. rewriter-go v0.15.0 fails the analyzer-off and
// engine-argument cases.
const TableReferenceProbeRequiredBuild = "rewriter-go >= v0.16.0 or rewriter-grpc >= v0.16.0 (table-reference policy, spec 2026-09-26)"

// TableReferenceProbeFactory is a Factory whose engine can prove the spec
// 2026-09-26 table-reference policy at startup. buildServer requires it of
// every concrete or injected factory, with or without storage integrity (T13).
type TableReferenceProbeFactory interface {
	Factory
	ProbeTableReferencePolicy(ctx context.Context) error
}

// tableReferenceProbeArgs is the request an ordinary session of logical
// database db1 sends with storage integrity disabled: no StorageIntegrityArgs,
// and the protected namespace every production request carries.
func tableReferenceProbeArgs() *pb.RewriteTableDynamicArgs {
	return &pb.RewriteTableDynamicArgs{
		DatabaseMap:                      map[string]string{"db1": "phys"},
		KnownPhysicalDatabases:           []string{"phys"},
		UpstreamLogicalDatabaseInContext: "db1",
		Delim:                            "_",
		ProtectedDatabases:               protectedDatabases("phys"),
	}
}

func rejectedProbe(name, sql string, code pb.RewriteCode, message string) storageIntegrityBuildProbe {
	return storageIntegrityBuildProbe{name: name, sql: sql, code: code,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED, sqlAfter: sql, message: message}
}

// tableReferenceProbes are the policy cases, measured on 2026-10-01 against
// rewriter-go v0.16.0 (native) and pinned for rewriter-grpc v0.16.0 by its
// shared-corpus cases and raw-output unit tests (rewriter_test.cc 6833-6834,
// 7470-7471). Both engines echo the input SQL on a rejection and answer no
// contract acknowledgement without StorageIntegrityArgs.
var tableReferenceProbes = []storageIntegrityBuildProbe{
	rejectedProbe("param-table-position", "SELECT * FROM {p:Identifier}",
		pb.RewriteCode_InvalidRewriteRequest, "query parameters are not supported in a database or table position"),
	rejectedProbe("protected-in-operand", "SELECT * FROM db1.o WHERE a IN phys.`db2.x`",
		pb.RewriteCode_InvalidRewriteRequest, "protected database phys is not addressable"),
	// phys is protected as a database_map value anyway; only a name outside
	// the map proves the engine reads protected_databases.
	rejectedProbe("protected-reserved-database", "SELECT * FROM hg_promote.`db2.x`",
		pb.RewriteCode_InvalidRewriteRequest, "protected database hg_promote is not addressable"),
	{
		name: "in-operand-rewritten", sql: "SELECT * FROM db1.o WHERE a IN db1.p",
		code: pb.RewriteCode_Success, statementType: pb.StatementType_STATEMENT_TYPE_SELECT, message: "success",
		sqlAfterByEngine: map[string]string{
			EngineNative: `SELECT * FROM phys."db1.o" "db1.o" WHERE a IN phys."db1.p"`,
			EngineGRPC:   "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN (phys.`db1.p`)",
		},
	},
	{
		name: "insert-select-source-rewritten", sql: "INSERT INTO db1.o SELECT * FROM db1.p",
		code: pb.RewriteCode_Success, statementType: pb.StatementType_STATEMENT_TYPE_INSERT, message: "success",
		sqlAfterByEngine: map[string]string{
			EngineNative: `INSERT INTO phys."db1.o" SELECT * FROM phys."db1.p" "db1.p"`,
			EngineGRPC:   "INSERT INTO phys.`db1.o` SELECT * FROM phys.`db1.p` AS `db1.p`",
		},
	},
	// T3 precedes T5: a protected carrier argument names the database.
	rejectedProbe("protected-carrier-argument", "SELECT * FROM merge('phys', 'db2')",
		pb.RewriteCode_InvalidRewriteRequest, "protected database phys is not addressable"),
	rejectedProbe("table-function-refused", "SELECT * FROM merge('db1', 'o')",
		pb.RewriteCode_UnsupportedStatement, "table function merge is not accepted"),
	rejectedProbe("use-protected", "USE phys",
		pb.RewriteCode_InvalidRewriteRequest, "protected database phys is not addressable"),
	rejectedProbe("unmodelled-class-refused", "SYSTEM RELOAD CONFIG",
		pb.RewriteCode_UnsupportedStatement, "statement is not supported"),
	rejectedProbe("system-table-refused", "SELECT * FROM system.processes",
		pb.RewriteCode_UnsupportedStatement, "system table system.processes is not accessible"),
	// Spec §13 round 2; rewriter-go v0.15.0 answers Success.
	rejectedProbe("analyzer-off-refused", "SELECT * FROM db1.o SETTINGS allow_experimental_analyzer = 0",
		pb.RewriteCode_UnsupportedStatement, "table setting allow_experimental_analyzer is not accepted"),
	// Spec §13; rewriter-go v0.15.0 answers Success.
	rejectedProbe("engine-argument-read-refused", "CREATE TABLE db1.n (d Date, n UInt8) ENGINE = MergeTree(d, (SELECT max(n) FROM db1.o), 8192)",
		pb.RewriteCode_UnsupportedStatement, "statement is not supported"),
}

// expectedSQL is the probe's exact output for the given engine.
func (p storageIntegrityBuildProbe) expectedSQL(engine string) string {
	if p.sqlAfterByEngine != nil {
		return p.sqlAfterByEngine[engine]
	}
	return p.sqlAfter
}

func (f *SentioNetworkFactory) engineName() string {
	if f.options.Engine == "" {
		return EngineGRPC
	}
	return f.options.Engine
}

func (f *SentioNetworkFactory) probeTimeout() time.Duration {
	if f.options.Timeout == 0 {
		return 5 * time.Second
	}
	return f.options.Timeout
}

// checkProbeAnswer compares one probe answer field by field. The error never
// quotes SQL or engine messages, so it cannot leak protocol-owned names.
func checkProbeAnswer(label, engine string, probe storageIntegrityBuildProbe, wantAck pb.StorageIntegrityContractVersion, requiredBuild string, resp *pb.RewriteSQLResponse, err error) error {
	prefix := fmt.Sprintf("%s (engine=%s probe=%s)", label, engine, probe.name)
	switch {
	case err != nil:
		return fmt.Errorf("%s: %w; deploy %s", prefix, err, requiredBuild)
	case resp == nil:
		return fmt.Errorf("%s: nil response; deploy %s", prefix, requiredBuild)
	case resp.GetStorageIntegrityContractVersion() != wantAck:
		return fmt.Errorf("%s: contract acknowledgement %s, want %s; deploy %s", prefix, resp.GetStorageIntegrityContractVersion(), wantAck, requiredBuild)
	case resp.GetCode() != probe.code:
		return fmt.Errorf("%s: code=%s, want %s; deploy %s", prefix, resp.GetCode(), probe.code, requiredBuild)
	case resp.GetStatementType() != probe.statementType:
		return fmt.Errorf("%s: statement type=%s, want %s; deploy %s", prefix, resp.GetStatementType(), probe.statementType, requiredBuild)
	case resp.GetSqlAfterRewrite() != probe.expectedSQL(engine):
		return fmt.Errorf("%s: SQL fingerprint mismatch; deploy %s", prefix, requiredBuild)
	case resp.GetMessage() != probe.message:
		return fmt.Errorf("%s: message fingerprint mismatch; deploy %s", prefix, requiredBuild)
	}
	return nil
}

// ProbeTableReferencePolicy issues the fixed policy cases (spec 2026-09-26
// §9.5, T13) and requires each exact answer.
func (f *SentioNetworkFactory) ProbeTableReferencePolicy(ctx context.Context) error {
	engine := f.engineName()
	if f.backend == nil {
		return fmt.Errorf("rewriter table-reference probe (engine=%s): no rewrite backend; deploy %s", engine, TableReferenceProbeRequiredBuild)
	}
	probeCtx, cancel := context.WithTimeout(ctx, f.probeTimeout())
	defer cancel()
	for _, probe := range tableReferenceProbes {
		resp, err := f.backend.Rewrite(probeCtx, &pb.RewriteSQLRequest{
			Sql:     probe.sql,
			Options: []*pb.RewriteOption{rewriteOption(tableReferenceProbeArgs())},
		})
		if err := checkProbeAnswer("rewriter table-reference probe", engine, probe,
			pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED, TableReferenceProbeRequiredBuild, resp, err); err != nil {
			return err
		}
	}
	return nil
}

// TableReferenceProbeAnswer returns the answer a conforming engine of the
// given kind (EngineGRPC or EngineNative) gives to req when req is one of the
// startup policy probes, and false for any other request. Test doubles that
// stand in for an engine use it to pass the startup probe.
func TableReferenceProbeAnswer(req *pb.RewriteSQLRequest, engine string) (*pb.RewriteSQLResponse, bool) {
	opts := req.GetOptions()
	if len(opts) != 1 || !proto.Equal(opts[0], rewriteOption(tableReferenceProbeArgs())) {
		return nil, false
	}
	for _, probe := range tableReferenceProbes {
		if probe.sql == req.GetSql() {
			return &pb.RewriteSQLResponse{Code: probe.code, StatementType: probe.statementType,
				SqlAfterRewrite: probe.expectedSQL(engine), Message: probe.message}, true
		}
	}
	return nil, false
}

var _ TableReferenceProbeFactory = (*SentioNetworkFactory)(nil)
```

Rewrite the loop body of `ProbeStorageIntegrityBuild` to reuse the helpers (messages unchanged, so the existing SI tests keep passing):

```go
func (f *SentioNetworkFactory) ProbeStorageIntegrityBuild(ctx context.Context) error {
	engine := f.engineName()
	if f.backend == nil {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): no rewrite backend", engine)
	}
	probeCtx, cancel := context.WithTimeout(ctx, f.probeTimeout())
	defer cancel()

	for _, probe := range storageIntegrityBuildProbes {
		resp, err := f.backend.Rewrite(probeCtx, &pb.RewriteSQLRequest{
			Sql:     probe.sql,
			Options: []*pb.RewriteOption{rewriteOption(storageIntegrityProbeArgs(probe.emptyTables))},
		})
		if err := checkProbeAnswer("storage-integrity engine probe", engine, probe,
			StorageIntegrityContractV2, storageIntegrityProbeRequiredBuild, resp, err); err != nil {
			return err
		}
	}
	return nil
}
```

`storageIntegrityProbeArgs` is unchanged (decision D4). *Superseded: D4 was overruled; `storageIntegrityProbeArgs` also sets `ProtectedDatabases: protectedDatabases("phys")`, see the D4 entry above.*

- [ ] **Step 4: Gate startup in `build.go`**

Replace

```go
	if siOptions.Enabled && rwFactory == nil {
		return nil, fmt.Errorf("storage_integrity.enabled requires an available SQL rewriter; refusing fail-open startup")
	}
	if siOptions.Enabled {
```

with

```go
	if siOptions.Enabled && rwFactory == nil {
		return nil, fmt.Errorf("storage_integrity.enabled requires an available SQL rewriter; refusing fail-open startup")
	}
	probeTimeout := cfg.Rewriter.Timeout.Duration
	if probeTimeout <= 0 {
		probeTimeout = 5 * time.Second
	}
	// Spec 2026-09-26 T13: every rewriter, with or without storage integrity,
	// proves the table-reference policy before the server starts. A built
	// rewriter that fails it refuses startup even under
	// rewriter.fail_open_on_unavailable, which covers only an unbuildable one.
	if rwFactory != nil {
		prober, ok := rwFactory.(rewriter.TableReferenceProbeFactory)
		if !ok {
			return nil, fmt.Errorf("the SQL rewriter must implement rewriter.TableReferenceProbeFactory; refusing unverified startup (spec 2026-09-26 T13)")
		}
		probeCtx, cancelProbe := context.WithTimeout(context.Background(), probeTimeout)
		err := prober.ProbeTableReferencePolicy(probeCtx)
		cancelProbe()
		if err != nil {
			return nil, err
		}
		log.Info("rewriter table-reference policy verified")
	}
	if siOptions.Enabled {
```

and inside the `if siOptions.Enabled` block delete the now-duplicate lines

```go
		probeTimeout := cfg.Rewriter.Timeout.Duration
		if probeTimeout <= 0 {
			probeTimeout = 5 * time.Second
		}
```

- [ ] **Step 5: Teach every engine stand-in the probe**

`rewriter_startup_gate_test.go`, `stubRewriterService.Rewrite`:

```go
func (stubRewriterService) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	if resp, ok := rewriter.TableReferenceProbeAnswer(req, rewriter.EngineGRPC); ok {
		return resp, nil
	}
	return &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: req.GetSql()}, nil
}
```

`pkg/integration/testenv/rewriter_mock.go`: import `"github.com/housegate/housegate/pkg/rewriter"`, update the type comment's "What it does" list with "Answers the startup table-reference probe exactly (rewriter.TableReferenceProbeAnswer) and does not record it", and make the first lines of `Rewrite`:

```go
func (m *RewriterMock) Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	// The startup policy probe (spec 2026-09-26 T13) is answered as a
	// conforming gRPC engine would and kept out of SeenSQL / SeenDynamicArgs.
	if resp, ok := rewriter.TableReferenceProbeAnswer(req, rewriter.EngineGRPC); ok {
		return resp, nil
	}
	sql := req.GetSql()
```

`pkg/rewriter/native_smoke_test.go` `TestNativeEngineProbeSmoke`, after the SI probe call:

```go
	if err := f.ProbeTableReferencePolicy(context.Background()); err != nil {
		t.Fatalf("the pinned native engine failed the table-reference probe: %v", err)
	}
```

`probe_test.go` `TestReleasedGRPCStorageIntegrityProbeSmoke`, after its SI probe call:

```go
	if err := f.ProbeTableReferencePolicy(context.Background()); err != nil {
		t.Fatalf("released gRPC table-reference probe: %v", err)
	}
```

- [ ] **Step 6: Run**

```bash
bazel run //:gazelle
bazel test //pkg/rewriter:rewriter_test //:housegate_test //pkg/integration/testenv:testenv_test --test_output=errors
bazel test //pkg/rewriter:rewriter_test --test_filter=TestNativeEngineProbeSmoke --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors
```

Expected: PASS; the native smoke must not report SKIPPED (it proves the native pins against the real v0.16.0 engine).

- [ ] **Step 7: Prove the gRPC pins (required before rollout; needs a working gcloud login)**

```bash
docker run --rm -d --name hg-rewriter-016 -p 50051:50051 us-west1-docker.pkg.dev/sentio-352722/sentio/housegate-rewriter:0.16.0
bazel test //pkg/rewriter:rewriter_test --test_filter=TestReleasedGRPCStorageIntegrityProbeSmoke --test_env=HOUSEGATE_TEST_REWRITER_GRPC_ADDR=127.0.0.1:50051 --test_output=errors
docker stop hg-rewriter-016
```

Expected: PASS. If `docker pull` fails for credentials, record "gRPC probe smoke owed" in the PR description and in the SDD ledger; do not guess the pins. If it fails on a pin, stop and report the probe name and the answer (re-measure; never loosen the comparison).

- [ ] **Step 7b: Run the integration suite**

Run: `bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors`
Expected: PASS (mock-based servers now pass the startup probe through the mock).

- [ ] **Step 8: Commit**

```bash
git add pkg/rewriter build.go build_test.go rewriter_startup_gate_test.go pkg/integration/testenv/rewriter_mock.go
git commit -m "feat(rewriter): every rewriter must prove the table-reference policy at startup (spec 2026-09-26 T13)"
```

---

### Task 10: integration tests against a real relay and the native engine

**Files:**
- Create: `pkg/integration/table_reference_hardening_test.go`, `pkg/integration/query_settings_test.go` (both in the existing `//pkg/integration:integration_test` target; `bazel run //:gazelle` adds the deps)

- [ ] **Step 1: Write the table-reference tests**

```go
// pkg/integration/table_reference_hardening_test.go
package integration

import (
	"context"
	"os"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sitable"
)

func requireNativeLib(t *testing.T) string {
	t.Helper()
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run `go run ./cmd fetch-rewriter-lib --tag v0.16.0` and pass --test_env")
	}
	return lib
}

// startTableRefProxy starts a server proxy on the native engine whose
// physical database is phys (created fresh on ClickHouse), with logical
// database db1 registered and the guard in guardMode.
func startTableRefProxy(t *testing.T, phys, guardMode string, extra ...testenv.ProxyOption) *testenv.TestProxy {
	t.Helper()
	lib := requireNativeLib(t)
	ctx := context.Background()
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{"DROP DATABASE IF EXISTS " + phys, "CREATE DATABASE " + phys} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() { _ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys) })
	opts := append([]testenv.ProxyOption{
		testenv.WithExtraDatabases("db1"),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.TableRefGuard.Mode = guardMode
		}),
	}, extra...)
	return testenv.StartServerProxy(t, chEnv.Addr, opts...)
}

// TestTableReference_TenantSourcesResolveEndToEnd: own-table sources and IN
// operands are rewritten (spec T4); before the hardening the source kept its
// logical name and ClickHouse could not resolve it.
func TestTableReference_TenantSourcesResolveEndToEnd(t *testing.T) {
	proxy := startTableRefProxy(t, "phys_tr1", "enforce")
	conn := openConn(t, proxy.Addr)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE TABLE db1.o (a UInt64) ENGINE = MergeTree ORDER BY a",
		"INSERT INTO db1.o SELECT number + 1 FROM numbers(2)",
		"INSERT INTO db1.o SELECT * FROM db1.o",
	} {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM db1.o WHERE a IN db1.o").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("count = %d, want 4", n)
	}
	var has uint8
	if err := conn.QueryRow(ctx, "SELECT hasColumnInTable('db1', 'o', 'a')").Scan(&has); err != nil {
		t.Fatal(err)
	}
	if has != 1 {
		t.Fatalf("hasColumnInTable = %d, want 1 (T6 rewrite to the physical table)", has)
	}
}

// TestTableReference_RefusalsReachTheClientAsExceptions runs each refusal with
// the guard observing (the engine answers) and enforcing (the guard answers
// first where a rule applies); the connection survives every refusal.
func TestTableReference_RefusalsReachTheClientAsExceptions(t *testing.T) {
	const phys = "phys_tr2"
	cases := []struct{ sql, engine, guard string }{
		{"SELECT * FROM {p:Identifier}", "query parameters are not supported in a database or table position", "table-reference guard: identifier_placeholder:"},
		{"SELECT * FROM db1.o WHERE a IN " + phys + ".`db2.x`", "protected database " + phys + " is not addressable", "table-reference guard: physical_database:"},
		{"SELECT * FROM merge('" + phys + "', '.*')", "protected database " + phys + " is not addressable", "table-reference guard: carrier_callable:"},
		{"USE " + phys, "protected database " + phys + " is not addressable", "table-reference guard: physical_database:"},
		{"SELECT * FROM hg_promote.`db2.x`", "protected database hg_promote is not addressable", "table-reference guard: reserved_name:"},
		{"DETACH TABLE db1.o", "statement is not supported", "statement is not supported"},
		{"SELECT 1 SETTINGS enable_analyzer = 0", "table setting enable_analyzer is not accepted", "table setting enable_analyzer is not accepted"},
	}
	for _, mode := range []string{"observe", "enforce"} {
		t.Run(mode, func(t *testing.T) {
			proxy := startTableRefProxy(t, phys, mode)
			conn := openConn(t, proxy.Addr)
			ctx := context.Background()
			for _, tc := range cases {
				want := tc.engine
				if mode == "enforce" {
					want = tc.guard
				}
				if err := conn.Exec(ctx, tc.sql); err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("%s: err = %v, want it to contain %q", tc.sql, err, want)
				}
			}
			var one uint8
			if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
				t.Fatalf("the session must survive the refusals: %v", err)
			}
		})
	}
}

// TestTableReference_StorageIntegrity: an IN operand naming an Active table
// reads the safe surface; an escaped MV target naming a Pending table is
// refused by sitablestate (T10). The guard observes so G5 does not answer
// first.
func TestTableReference_StorageIntegrity(t *testing.T) {
	ctx := context.Background()
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"CREATE DATABASE IF NOT EXISTS hg_unsafe",
		"DROP TABLE IF EXISTS hg_safe.db1__t",
		"CREATE TABLE hg_safe.db1__t (_hg_row_id FixedString(32), a UInt64) ENGINE = MergeTree ORDER BY a",
		"INSERT INTO hg_safe.db1__t VALUES (repeat('a', 32), 1)",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_safe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_unsafe")
	})
	state := sitable.NewFake(sitable.Ordinary,
		sitable.Table{ID: "db1.t", Status: sitable.Active},
		sitable.Table{ID: "db1.p", Status: sitable.Pending},
	)
	proxy := startTableRefProxy(t, "phys_tr3", "observe",
		testenv.WithConfigMutator(func(cfg *config.Config) {
			enabled := true
			cfg.StorageIntegrity.Enabled = &enabled
		}),
		func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityTableState = state },
	)
	conn := openConn(t, proxy.Addr)
	for _, q := range []string{
		"CREATE TABLE db1.o (a UInt64) ENGINE = MergeTree ORDER BY a",
		"INSERT INTO db1.o SELECT number + 1 FROM numbers(3)",
	} {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM db1.o WHERE a IN db1.t").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("count = %d, want 1 (only a = 1 is in the safe read)", n)
	}
	err := conn.Exec(ctx, "CREATE MATERIALIZED VIEW db1.mv TO db1.`\\x70` AS SELECT a FROM db1.o")
	if err == nil || !strings.Contains(err.Error(), "the materialized view header cannot be read") {
		t.Fatalf("err = %v, want the sitablestate unreadable-header refusal", err)
	}
}

// TestTableReference_DriverTrafficIsUnaffected: an indexer-signed driver
// session (SQL_sentio_driver) is ordinary for the guard and the engine; its
// DDL, INSERT … SELECT, IN-operand reads and physical-name metadata reads
// (housegate#218) pass with the guard enforcing.
func TestTableReference_DriverTrafficIsUnaffected(t *testing.T) {
	const phys = "phys_trd"
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	proxy := startTableRefProxy(t, phys, "enforce",
		testenv.WithExtraDatabases("drv1"),
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), "drv1", registry.DbAuthOwner),
		func(_ *config.Config, opts *housegate.Options) { opts.Signer = signer },
	)
	conn := openSignedConn(t, proxy.Addr, signer)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		auth.DriverSettingKey: clickhouse.CustomSetting{Value: "1"},
	}))
	for _, q := range []string{
		"CREATE TABLE drv1.d (a UInt64) ENGINE = MergeTree ORDER BY a",
		"INSERT INTO drv1.d SELECT number FROM numbers(3)",
		"SELECT name FROM system.tables WHERE database = '" + phys + "' AND name LIKE 'drv1.%'",
	} {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("driver %s: %v", q, err)
		}
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM drv1.d WHERE a IN drv1.d").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("count = %d, want 3", n)
	}
}
```

- [ ] **Step 2: Write the Query-packet settings tests**

```go
// pkg/integration/query_settings_test.go
package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	rewritergo "github.com/housegate/rewriter-go"
	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/plugins/querysettings"
)

// TestQuerySettings_RefusedInTheQueryPacket is spec 2026-09-26 §9.7: R5
// settings sent in the native Query packet are refused before forwarding,
// with the engines' value rule for the analyzer switch.
func TestQuerySettings_RefusedInTheQueryPacket(t *testing.T) {
	proxy := startTableRefProxy(t, "phys_qs", "enforce")
	conn := openConn(t, proxy.Addr)
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		settings clickhouse.Settings
		refused  string
	}{
		{"analyzer off", clickhouse.Settings{"enable_analyzer": 0}, "enable_analyzer"},
		{"experimental analyzer false", clickhouse.Settings{"allow_experimental_analyzer": false}, "allow_experimental_analyzer"},
		{"legacy tuple names", clickhouse.Settings{"legacy_column_name_of_tuple_literal": 0}, "legacy_column_name_of_tuple_literal"},
		{"global WITH off", clickhouse.Settings{"enable_global_with_statement": 0}, "enable_global_with_statement"},
		{"compatibility", clickhouse.Settings{"compatibility": "21.1"}, "compatibility"},
		{"profile", clickhouse.Settings{"profile": "default"}, "profile"},
		{"implicit table", clickhouse.Settings{"implicit_table_at_top_level": "x"}, "implicit_table_at_top_level"},
		{"dialect", clickhouse.Settings{"dialect": "kusto"}, "dialect"},
		{"analyzer on", clickhouse.Settings{"enable_analyzer": 1}, ""},
		{"analyzer true", clickhouse.Settings{"enable_analyzer": true}, ""},
		{"ordinary setting", clickhouse.Settings{"max_threads": 2}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var one uint8
			err := conn.QueryRow(clickhouse.Context(ctx, clickhouse.WithSettings(tc.settings)), "SELECT 1").Scan(&one)
			if tc.refused == "" {
				if err != nil {
					t.Fatalf("err = %v, want accepted", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "table setting "+tc.refused+" is not accepted") {
				t.Fatalf("err = %v, want the %s refusal", err, tc.refused)
			}
		})
	}
	var one uint8
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("the session must survive the refusals: %v", err)
	}
}

// TestQuerySettings_MatchTheEngineForEverySetting keeps querysettings in
// lockstep with the pinned engine: for every setting ClickHouse knows (and
// every name housegate lists), housegate refuses a value in the Query packet
// exactly when the engine refuses the same assignment in SQL.
func TestQuerySettings_MatchTheEngineForEverySetting(t *testing.T) {
	lib := requireNativeLib(t)
	ctx := context.Background()
	svc, err := rewritergo.NewService(lib)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	rows, err := openConnNoDB(t, chEnv.Addr).Query(ctx, "SELECT name FROM system.settings")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(names) < 500 {
		t.Fatalf("system.settings returned %d names", len(names))
	}
	names = append(names, querysettings.RefusedNames()...)
	args := &pb.RewriteTableDynamicArgs{
		DatabaseMap: map[string]string{"db1": "phys"}, KnownPhysicalDatabases: []string{"phys"},
		UpstreamLogicalDatabaseInContext: "db1", Delim: "_",
		ProtectedDatabases: []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"},
	}
	for _, name := range names {
		for _, value := range []string{"0", "1"} {
			resp, err := svc.Rewrite(ctx, &pb.RewriteSQLRequest{
				Sql: fmt.Sprintf("SELECT 1 SETTINGS `%s` = %s", name, value),
				Options: []*pb.RewriteOption{{Op: pb.RewriteOp_TableNameRewrite,
					Value: &pb.RewriteOption_TableNameArgs{TableNameArgs: &pb.RewriteTableNameArgs{DynamicArgs: args}}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			engineRefuses := resp.GetMessage() == "table setting "+name+" is not accepted"
			if !engineRefuses && resp.GetCode() != pb.RewriteCode_Success {
				t.Errorf("%s = %s: unexpected engine answer %s %q", name, value, resp.GetCode(), resp.GetMessage())
				continue
			}
			if got := querysettings.Refused(name, value); got != engineRefuses {
				t.Errorf("%s = %s: housegate refuses %v, engine refuses %v", name, value, got, engineRefuses)
			}
		}
	}
}
```

- [ ] **Step 3: Run**

```bash
bazel run //:gazelle
bazel test //pkg/integration:integration_test --test_filter='TestTableReference_|TestQuerySettings_' --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors
```

Expected: PASS, none SKIPPED. If a helper signature differs from the one used here, adapt the call and keep the assertion. If the ClickHouse client or clickhouse-go sends one of the refused settings on its own (every integration test against a rewriter would then fail), stop and report the setting name instead of exempting it.

- [ ] **Step 4: Run the whole integration target**

Run: `bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors`
Expected: PASS; compare any failure with `main` before changing a pre-existing test.

- [ ] **Step 5: Commit**

```bash
git add pkg/integration/table_reference_hardening_test.go pkg/integration/query_settings_test.go pkg/integration/BUILD.bazel
git commit -m "test(integration): table-reference hardening and Query-packet settings end to end"
```

---

### Task 11: operator note, CLAUDE.md, spec and plan copies, PR

**Files:**
- Create: `docs/table-reference-hardening.md`
- Modify: `CLAUDE.md` (rewriter section, Key Modules, the "Known table-reference gaps" rough edge)
- Copy into `docs/superpowers/specs/`: `2026-09-26-table-reference-hardening-design.md`, `system-tables-survey.md` (the spec links it relatively)
- Copy into `docs/superpowers/plans/`: `2026-09-26-table-reference-hardening-engine-go.md`, `2026-09-26-table-reference-hardening-engine-cpp.md`, `2026-09-26-table-reference-hardening-housegate.md`, `2026-09-29-table-reference-hardening-engine-go-followup.md`, `2026-09-29-table-reference-hardening-engine-cpp-v2.md`, `2026-09-29-rewriter-go-mid-statement-drop-gate.md`, `2026-09-29-rewriter-go-whole-statement-parse-gate.md`, `2026-10-01-table-reference-hardening-housegate-v2.md`
- No `.github/workflows/ci.yml` target change (Task 10 extends the existing integration target; Task 1 moved the FFI tag).

- [ ] **Step 1: Write `docs/table-reference-hardening.md`** (one paragraph per line, no hard wrapping):

````markdown
# Table-reference hardening: operator note

Spec: [docs/superpowers/specs/2026-09-26-table-reference-hardening-design.md](superpowers/specs/2026-09-26-table-reference-hardening-design.md). Fail-closed rewriting and `rewriter.fail_open_on_unavailable` are described in [rewriter-fail-closed.md](rewriter-fail-closed.md).

## Minimum engines and the startup probe

HouseGate requires rewriter-go v0.16.0 (native engine, FFI library from the same release) or rewriter-grpc v0.16.0. Every server with a rewriter runs a table-reference probe at startup, with or without storage integrity: twelve fixed statements (identifier parameters, protected databases including `hg_promote`, IN-operand and `INSERT … SELECT` source rewriting, table functions, `USE`, an unmodelled class, a refused system table, the analyzer switch, an engine-argument subquery) whose exact code, statement type, SQL and message must match. An engine that fails any case refuses startup, naming the case (`probe=<name>`) and the required builds. `rewriter.fail_open_on_unavailable` does not cover a probe failure: it only lets a server start when the rewriter cannot be built at all. With storage integrity enabled the eight storage-integrity probes run after it. Roll the engines out before this HouseGate: an older engine stops HouseGate from starting.

## The table-reference guard (`tableref_guard`)

A lexical guard runs on ordinary sessions (tenants and the indexer driver; not maintenance, platform-operator or peer sessions) before the rewriter, whenever a rewriter is configured. It refuses: `reserved_name` — `hg_safe`, `hg_unsafe` or `hg_promote` anywhere outside a comment, string literals included; `physical_database` — the configured `rewriter.physical_database` used as a qualifier (`phys.t`), as the object of `USE`, as a `SHOW … FROM/IN` object, or inside the arguments of a lookup function (`joinGet`, `dictGet*`, `dictHas`, `dictIsIn`, `hasColumnInTable`); a column or a string literal elsewhere that equals the name is allowed; `carrier_callable` — a call to a table function the rewriter refuses (`merge`, `remote`, `cluster`, `mergeTree*`, `mysql`, `executable`, …); `identifier_placeholder` — any `{name:Identifier}` parameter, in any position; `escaped_identifier` — a backslash inside a backtick or double-quoted identifier (backslashes in string literals are allowed). A statement the guard cannot scan (a `$` that opens no heredoc, an unterminated quote, comment or heredoc) is refused under the label `scan`. A refusal reads `table-reference guard: <rule>: <detail>; the rewriter applies the same policy`. The rewriter enforces the same policy whatever the guard does; the guard is defence in depth.

```yaml
tableref_guard:
  mode: enforce   # default; observe logs and counts instead of refusing
```

Every hit, refused or observed, increments `clickhouse_proxy_tableref_guard_rejections_total{rule="…"}`. Rollout (spec §11): run one release cycle with `mode: observe` on devnet2 and require `rule="identifier_placeholder"` and `rule="escaped_identifier"` to stay at zero for driver and processor traffic before switching production to `enforce`.

## Settings in the native Query packet

ClickHouse clients can send per-query settings in the native Query packet, outside the SQL text, where the rewriter never sees them. HouseGate refuses, before forwarding, the settings the rewriter refuses in SQL: `additional_table_filters`, `additional_result_filter`, `parallel_replicas_custom_key`, `dialect`, `polyglot_dialect`, `allow_experimental_polyglot_dialect`, `allow_experimental_prql_dialect`, `allow_experimental_kusto_dialect`, any name ending in `_dialect`, `enable_global_with_statement`, `compatibility`, `implicit_table_at_top_level`, `promql_table`, `promql_database`, `legacy_column_name_of_tuple_literal` and `profile`, whatever the value; and `enable_analyzer` / `allow_experimental_analyzer` unless the value is `1`, `true`, `'1'` or `'true'` (any case). Names are compared case-insensitively. The client receives an Exception `table setting <name> is not accepted (native-protocol query setting)`; the connection stays usable. The check applies to the same sessions the rewriter governs (ordinary and driver sessions, and sessions forwarded from a peer), not to maintenance or platform-operator sessions, and has no observe mode.

## Keep the analyzer on in every ClickHouse settings profile

The rewriter's name-binding rules are proven for the new analyzer only; the old analyzer (and settings such as `compatibility` that restore old defaults) can make a name the rewriter trusted read another tenant's table. Neither the rewriter nor HouseGate can see a ClickHouse user's default settings profile, so every profile a HouseGate session can use — the profile of the shared user in `users.xml` / `users.d`, its parent profiles, and any profile a `SETTINGS PROFILE` grant attaches — must keep `enable_analyzer = 1` and must not set any setting in the list above. Check on every ClickHouse server behind HouseGate, connected as the user HouseGate uses:

```sql
SELECT name, value, changed
FROM system.settings
WHERE name IN ('enable_analyzer', 'allow_experimental_analyzer', 'compatibility', 'enable_global_with_statement',
               'legacy_column_name_of_tuple_literal', 'implicit_table_at_top_level', 'dialect',
               'additional_table_filters', 'additional_result_filter', 'parallel_replicas_custom_key',
               'promql_table', 'promql_database')
   OR endsWith(name, '_dialect');

SELECT profile_name, setting_name, value
FROM system.settings_profile_elements
WHERE setting_name IN ('enable_analyzer', 'allow_experimental_analyzer', 'compatibility', 'enable_global_with_statement',
                       'legacy_column_name_of_tuple_literal', 'implicit_table_at_top_level', 'dialect',
                       'additional_table_filters', 'additional_result_filter', 'parallel_replicas_custom_key',
                       'promql_table', 'promql_database')
   OR endsWith(setting_name, '_dialect');
```

Expected: `enable_analyzer` is `1`, every other row has `changed = 0`, and the second query returns no row (or only `enable_analyzer = 1`). ClickHouse 24.3 and later default `enable_analyzer` to `1`. Add this check to every ClickHouse deployment change; as of `sentioxyz/production` `d1a757627` no ClickHouse configuration sets any of these settings.
````

- [ ] **Step 2: Update `CLAUDE.md`**

Replace the "**Known table-reference gaps (measured 2026-09-26; …)**" rough-edge bullet with:

```markdown
- **Table-reference hardening (spec 2026-09-26) has shipped in HouseGate; two residuals remain.** The engines' policy is in force (rewriter-go v0.16.0 / rewriter-grpc v0.16.0, proven at startup), but the class (c) `system` tables the Sentio driver reads by physical name (`tables`, `columns`, `parts*`, `mutations`, …) still pass through verbatim and `PermissionCommitGateObserver` still exempts every `system` read — both are step 2, housegate/housegate#218. `url` / `s3` / `file` table functions remain a non-goal (ClickHouse user grants own egress).
```

Append to the paragraph that ends "…`resolveRemoteCredentials` helper." in §4 a new paragraph:

```markdown
**Table-reference policy (spec 2026-09-26).** Every rewrite request carries `protected_databases = [rewriter.physical_database] ∪ sitable.ReservedDatabases()` (`pkg/rewriter/args.go` `protectedDatabases`), so the engines refuse the physical and protocol-owned databases in every position whatever the SI state. `buildServer` requires every concrete or injected factory to implement `rewriter.TableReferenceProbeFactory` and pass `ProbeTableReferencePolicy` (twelve exact cases, `pkg/rewriter/probe.go` `tableReferenceProbes`, pinned per engine where quoting differs; the integration `RewriterMock` and test stubs answer it through `rewriter.TableReferenceProbeAnswer`) before the SI probe; a built rewriter that fails it refuses startup even under `fail_open_on_unavailable`. On ordinary sessions two query plugins run after `auth` and before `forward` / `rewrite` whenever a rewriter is configured: `tablerefguard` (lexical rules G1–G5 over `pkg/sqlsurface`, `tableref_guard.mode: enforce|observe`, counter `clickhouse_proxy_tableref_guard_rejections_total{rule}`) and `querysettings` (refuses R5 settings in the native Query packet: the engines' list copied from rewriter-go `internal/engine/settings.go`, the analyzer switch unless literal true; skips maintenance/operator and `remote()` peer sessions, runs on forwarded-from-peer ones). Deployments must keep the analyzer on in every ClickHouse settings profile: see [docs/table-reference-hardening.md](docs/table-reference-hardening.md).
```

In Key Modules, after the `pkg/plugins/` bullet's plugin list, add `tablerefguard` and `querysettings` with one clause each (as in the paragraph above), and add a bullet: `- **[pkg/sqlsurface/](pkg/sqlsurface/)** — the Spec N D1 lexical model shared by `sireserved` and `tablerefguard`: two surfaces (executable text without literals, and with literals) plus a token stream; `Options.AllowStringEscapes` keeps backslashes in string literals for ordinary sessions.` In the `sireserved` sentence of the SI paragraph, make the reserved list read "`hg_safe` / `hg_unsafe` / `hg_promote` (`sitable.ReservedDatabases()`)", and in the scrubber sentence change "scrubbed of `hg_safe` / `hg_unsafe` names" to "scrubbed of `hg_safe` / `hg_unsafe` / `hg_promote` names".

- [ ] **Step 3: Copy the spec and plans**

```bash
SRC=/Users/uranuswch/src/tableref-docs
cp "$SRC/2026-09-26-table-reference-hardening-design.md" "$SRC/system-tables-survey.md" docs/superpowers/specs/
for f in 2026-09-26-table-reference-hardening-engine-go.md 2026-09-26-table-reference-hardening-engine-cpp.md \
         2026-09-26-table-reference-hardening-housegate.md 2026-09-29-table-reference-hardening-engine-go-followup.md \
         2026-09-29-table-reference-hardening-engine-cpp-v2.md 2026-09-29-rewriter-go-mid-statement-drop-gate.md \
         2026-09-29-rewriter-go-whole-statement-parse-gate.md 2026-10-01-table-reference-hardening-housegate-v2.md; do
  cp "$SRC/$f" docs/superpowers/plans/
done
git status --short docs/superpowers
```

Then, **only if the controller ratified D1–D7** (see "Spec deviations"), append to `docs/superpowers/specs/2026-09-26-table-reference-hardening-design.md` a section `## 14. Amendment 2026-10-01: Plan C execution record` that lists D1–D7 verbatim from this plan's header, and tell the controller to mirror it into `/Users/uranuswch/src/tableref-docs/`. If they were not ratified, skip this paragraph and list the open items in the PR description.

- [ ] **Step 4: Full verification**

```bash
bazel build //... && bazel test //... 2>&1 | tail -5
bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors 2>&1 | tail -5
bazel test //pkg/rewriter:rewriter_test //pkg/plugins/querysettings:querysettings_test --test_filter='TestNativeEngine|TestRefusedMatchesTheNativeEngine' --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors
```

Expected: all PASS; compare any failure with `main` before acting.

- [ ] **Step 5: Commit, push, open the PR**

```bash
git add CLAUDE.md docs
git commit -m "docs: table-reference hardening operator note, CLAUDE.md, spec and plan copies"
git push -u origin feat/table-reference-hardening
gh pr create --title "feat: table-reference hardening, housegate half (spec 2026-09-26)" --body "$(cat <<'EOF'
Housegate half of the table-reference hardening (Plan C v2): protected_databases on every rewrite request; the tablerefguard lexical guard (tableref_guard.mode enforce|observe) over the extracted pkg/sqlsurface scanner; querysettings refusing R5 settings in the native Query packet (spec §9.7); sitablestate escape refusal; hg_promote in sireserved and the scrubber; and a table-reference startup probe for every rewriter. Fail-closed rewriting shipped earlier in #217. Pins rewriter-go v0.16.0 / rewriter-proto v0.4.0 and the v0.16.0 FFI library; requires rewriter-grpc v0.16.0.

Rollout: engines first (an older engine now fails the startup probe). Downstream: sentio-node's injected test factories (standalone/storage_integrity_schemas_test.go, storage_integrity_acceptance_ch_test.go) must add ProbeTableReferencePolicy when they bump housegate. Not included: PermissionCommitGateObserver system-exemption narrowing (housegate#218). Owed: <gRPC probe smoke result or "owed">; spec deviations D1–D7 <ratified / open>.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Fill the two `<…>` placeholders in the body from Task 9 Step 7 and Step 3 above before running the command. Ask the user before merging. Rollout after merge follows spec §11 steps 5–6.

---

## Self-review (against the spec)

- **Coverage.** §9.1 → #217 (recorded, no task). §9.2 → Tasks 3–5. §9.3 → Task 7. §9.4 → Task 8 (guard list in Task 5). §9.5 → Task 9 (deviations D1–D4). §9.6 → Tasks 5/6/9 wiring, Task 11 docs; commitgate narrowing → #218 (D5). §9.7 → Task 6 (+ Task 10 integration and lockstep, Task 11 runbook). §10.3 → unit tests in Tasks 2–9 (`sentio_test.go`'s T8 pins already exist from #217). §10.4 → Task 10 (tenant sources, refusals, SI `IN db1.t`, escaped MV against Pending, driver traffic). §11 step 5 → Task 11 note and the counter. §13 → probe discriminators (Task 9) and the analyzer rule (Task 6).
- **Placeholders.** None in code; the PR body's two `<…>` fields are filled from recorded results (explicit step).
- **Type consistency.** `protectedDatabases(string) []string` (Task 2) is used by Task 9's `tableReferenceProbeArgs` and `TestTableReferenceProbeAnswer`; `buildDynamicArgs`'s new signature is used in Task 9's test. `tablerefguard.Config/Mode/Plugin` (Task 4) match `config.Config.TableRefGuard` and `build.go` (Task 5) and the integration helper's `cfg.TableRefGuard.Mode` (Task 10). `querysettings.Refused/RefusedNames/Plugin` (Task 6) match Task 10. `rewriter.TableReferenceProbeAnswer/TableReferenceProbeFactory/ProbeTableReferencePolicy` (Task 9) match the stubs, mock and build gate. `sqlsurface.Token/TokenKind/Options/Err*` (Task 3) match Task 4.
