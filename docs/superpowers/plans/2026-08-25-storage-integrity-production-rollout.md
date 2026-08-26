# Storage-Integrity Production Rollout (Specs I–L) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. This plan spans **five repositories** and is **strictly sequential**: a red step stops the chain where it went red. Never bundle two repos into one round trip to "save time".

**Goal:** Make Specs I–N reachable from production. Split arbiter-core's `ErrPayloadMismatch` into a pre-write and a post-record sentinel that both still match the original, give it an explicit hard-parts disable, then walk the pin chain rewriter-go v0.9.1 → HouseGate v0.12.0 → arbiter-core → arbiter → sentio-node v0.1.0 — merging PR #141 on the way — and finish with the two acceptance tests Spec L §4 asked for and never got, each proved red against the pins it replaces.

**Architecture:** Four independent safety properties, delivered in dependency order. (1) **arbiter-core makes its refusals classifiable** — `ErrPayloadMismatchPreWrite` and `ErrPayloadMismatchPostRecord` both wrap `ErrPayloadMismatch`, so every existing `errors.Is` caller is unaffected while a new caller can ask the only question that matters downstream, *"could an unsafe write exist?"*, and `DisableHardParts` makes the back-pressure opt-out a thing an operator must say twice. (2) **The pin chain is one loud step at a time** — each repo's bump ends in a named command, and the FFI library's resolved path is asserted to carry the new tag because the tag-keyed cache would otherwise let a stale hit silently keep the old engine. (3) **sentio-node stops having two disagreeing derivations of the same fact** — the fail-open `ProtocolTablesMode` is deleted rather than fixed, and the test that computed its expectation from it is re-pointed at the production predicate. (4) **The host maps the split sentinel correctly** — only the pre-write class becomes `sicore.ErrPrepareTerminalReject`, because HouseGate treats terminal-reject as *provably no write* and hard-errors when the recovered candidate set is non-empty.

**Tech Stack:** Go 1.26 + Bazel 9 in all four Go repos (arbiter-core, housegate, arbiter, sentio-node); ClickHouse 25.8 in docker for every acceptance test; GitHub Actions `workflow_dispatch` for the two automated release cuts; a hand-pushed annotated tag for sentio-node, which has no release automation.

**Spec:** `docs/superpowers/specs/2026-08-25-storage-integrity-production-rollout-design.md` (Spec O). Roadmap: `docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md`. **Consumes:** `docs/superpowers/specs/2026-08-25-storage-integrity-lexical-namespace-closure-design.md` (Spec N) — Part C cannot start until Spec N has cut rewriter-go v0.9.1 *and* landed its D1 heredoc fix onto PR #141's branch. **Delivers the production reachability of:** Specs I / J / K / L.

## Global Constraints

Copied from Spec O, the roadmap's §4 decisions, and each repo's conventions. Every task's requirements implicitly include this section.

- **The chain is sequential and each failure is local.** Part order: A (arbiter-core) → B (gate on Spec N) → C (housegate) → D (arbiter) → E (sentio-node) → F (end-to-end). A is independent of B/C/D and only gates E, so it runs first to shorten the critical path; nothing else may be reordered and nothing may be bundled.
- **Every new guard ships with a step proving it fails against the unfixed code** (roadmap §4 item 9). Where the proof is a compile error rather than a test failure, the step says so explicitly — a compile error is a valid red, a *silently skipped* test is not.
- **One commit per task**, conventional-commit prefix, in the repo the task names. Never `git add -A`; add the exact paths the task lists.
- **`errors.Is(err, ErrPayloadMismatch)` must stay true for every existing caller.** Both new sentinels wrap it. The two new sentinels are mutually exclusive: `errors.Is(preWriteErr, ErrPayloadMismatchPostRecord)` must be false, and vice versa.
- **Terminal-reject means "provably no write", not "not retryable."** `housegate/pkg/storageintegrity/intake.go:1435` hard-errors with `intake: pre-write terminal prepare reject for %s unexpectedly has %d candidate parts` when `len(parts) != 0`. Any mapping that sends a post-record failure down that path converts a convergent retry into a permanently wedged one.
- **The FFI library is a separate artifact from the Go module** (`.claude/skills/upgrade-dependency/SKILL.md`, "Three Pin Mechanisms"). A rewriter-go bump touches `go.mod` + `go.sum` + `.github/workflows/ci.yml` + `configs/local.server.yaml` + `configs/local.server-mock-remote.yaml` + the `CLAUDE.md` version-floor sentence. All six in **one commit**.
- **The FFI cache is tag-keyed** (`os.UserCacheDir()/housegate/rewriter-ffi/<tag>/libpolyglot_sql_ffi.<ext>`, `pkg/ffifetch/ffifetch.go:39-40,96,98`). A cache hit does zero network I/O, and a `SHA256SUMS` asset that is missing is fail-open with only a warning. Asserting the printed path alone is therefore necessary but **not sufficient** — Part C Task 6 purges the new tag's cache directory first and re-checks the digest against the release manifest.
- **The main-baseline rule** (CLAUDE.md; skill "The Main-Baseline Rule"): never call an integration failure a regression until it has been measured on stashed-clean `main` with `--runs_per_test=10`, and record the ratio.
- **Bazel is the test ground truth** in all four repos. housegate: `bazel test //...`; arbiter-core and arbiter: `bazel test --build_tests_only --@rules_go//go/config:race //...`; sentio-node: `bazel test //... --test_output=errors`. Docker-bound targets are excluded from the plain run in every repo and must be invoked by name.
- **English only** for identifiers, comments, log messages, and operator-facing error strings, in all five repos.
- **Markdown docs are not hard-wrapped** — one paragraph per line.
- **Do not rewrite historical tags** inside `docs/superpowers/{plans,specs}/`; they are point-in-time records.

## Version facts this plan was written against (verified 2026-08-25)

Re-verify each before starting; a drifted fact changes a step, not the plan's shape.

| Fact | Value | Source |
|---|---|---|
| housegate `main` | `6fd56b8`, released `v0.11.0` | `git log`, `gh release list` |
| housegate `main` rewriter-go pin | `v0.7.1` (`go.mod:108`), CI `--tag v0.7.1` (`ci.yml:111`) | file read |
| **PR #141 branch already bumps to `v0.9.0`** | `go.mod`, `ci.yml`, `configs/*.yaml`, `CLAUDE.md` floor, polyglot `v0.8.1`→`v0.9.2` | `gh pr diff 141` |
| PR #141 state | `OPEN`, `MERGEABLE`, 11 commits, 3 required checks `SUCCESS` | `gh pr view 141` |
| rewriter-go latest | `v0.9.0` — **`v0.9.1` does not exist yet; Spec N cuts it** | `gh release list` |
| arbiter-core | `main` = `32b59a8`, latest tag `v0.5.1` (tagged 2026-08-25 01:57 UTC) | `git for-each-ref` |
| arbiter-core housegate pin | `v0.10.0` | `arbiter-core/go.mod:31` |
| arbiter | `main` = `c1d32f6`; pins housegate `v0.10.0`, arbiter-core `v0.5.1`, arbiter-proto `v0.6.0` | `arbiter/go.mod:18-20` |
| sentio-node | `main` = `58f5e5f`, **zero tags, no release-cut workflow** | `git tag --list`, `.github/workflows/` |
| sentio-node pins | housegate `v0.9.2`, arbiter-core `v0.4.0`, arbiter-proto `v0.5.0`, rewriter-proto `v0.2.0` | `sentio-node/go.mod:12-18` |
| `payloadexec.ValidateTableSchemaColumns` first appears in housegate | `v0.9.4` (absent at `v0.9.2`/`v0.9.3`) | `git cat-file -e <tag>:pkg/replay/payloadexec/column_types.go` |
| `chproto.ClientError.KeepSession` (non-closing 252) first appears in housegate | `v0.11.0` | `git grep KeepSession <tag> -- 'pkg/chproto/*.go'` |
| `storage_integrity...backpressure.refresh_timeout` first appears in housegate | `v0.11.0` | `git show <tag>:pkg/config/storage_integrity_config.go` |

## Deviations from Spec O, and why

Spec O was written before these six facts were checked at source. Each deviation below is a correction, not a scope change; all are re-stated in Task 20's closure record.

- **O-1 — the housegate pin bump belongs on PR #141's branch, not on `main`.** Spec O D5 step 2 reads as though `main` is bumped and then #141 is merged. PR #141 *already* carries `rewriter-go v0.9.0` in `go.mod` and `--tag v0.9.0` in `ci.yml`. Bumping `main` separately would produce a conflict, and merging #141 first would land `main` on v0.9.0 — an engine without Spec N's half — for however long the follow-up takes. **Part C therefore bumps v0.9.0 → v0.9.1 on `feature/si-surface-failclosed-housegate`, then merges, then tags.**
- **O-2 — `validatePrepareBindings` is reachable from *both* classes, so a line-keyed split is unsound.** Spec O D1 assigns `staged.go:194,198,202` to `PreWrite`. Those three lines live inside `validatePrepareBindings`, which `PrepareLocalStatement:79` calls before the journal load (pre-write) **and** which `validateRecordedBindings:212` calls on a record already loaded from the journal, from `RegisterPreparedClaim:342` and `AbortPreparedStatement:402` (post-record, and in `RegisterPreparedClaim`'s case the lifecycle is `UnsafeWritten`/`RCBound`, i.e. parts definitely exist). A line-keyed split would classify a definitely-written statement as pre-write and wedge it on `abortTerminalPrepareReject`'s hard error — the exact failure D1 exists to prevent. **Task 1 parameterises the class instead of keying on the line.**
- **O-3 — arbiter-core cannot be told to cut `v0.6.0`.** `scripts/next-version.sh` derives the version from the previous annotated tag's UTC day: same day → patch, later day → minor. `cut-release.yml` is a bare `workflow_dispatch` with no `bump` input. `v0.5.1` was tagged 2026-08-25 01:57 UTC, so a cut on 2026-08-25 yields **v0.5.2**, not v0.6.0. **Task 3 records whatever the script yields as `<arbiter-core-tag>` and states the one-line condition for getting v0.6.0** (cut on a later UTC day). Every later task refers to `<arbiter-core-tag>`, never to a literal.
- **O-4 — sentio-node has no release automation.** Spec O D5 step 5 says "Cut **v0.1.0** — its first tag" without saying how. `release-devnet.yml` / `release-testnet.yml` only push docker images, and there is no `scripts/next-version.sh`. **Task 18 pushes a hand-made annotated tag and publishes the release**, which is safe here for two reasons: nothing in the repo derives a version from the tag ledger, so a hand-made tag cannot desynchronise anything; and `ci.yml` triggers on `release: types: [published]`, so publishing re-runs the full build plus the ClickHouse integration job against the exact tagged tree.
- **O-5 — Spec O §4 item 4's end-to-end acceptance does not say which rewriter engine runs.** sentio-node has no FFI references at all and embeds housegate as a library, so the engine is whatever `rewriter.engine` says in the deployed YAML. Under `native` the acceptance proves the v0.9.1 Go engine; under `grpc` — the default, and the likely production setting — it proves the deployed `sql-rewriter` service, whose Spec N half ships in a **rewriter-grpc** release that no plan in this set pins. **Task 19 Step 0 makes the engine an explicit precondition and stops the acceptance if it is unstated.**
- **O-6 — the two acceptance tests keep sentio-node's env gate.** Spec O D6 says they should follow "HouseGate's explicit-target convention rather than env self-skip (see Spec P D6)", but Spec P D6 read at source says the opposite for this repo: sentio-node keeps `SENTIO_SI_CH_E2E` and hardens the CI filter, and "the three existing anti-skip guards are kept". **Tasks 12 and 17 follow the existing repo pattern** — env gate plus a named CI target, an exact `--test_filter`, and `--- SKIP:` / `no tests to run` / PASS-count guards that fail the job on the wrong answer. Full reasoning at the head of Part E.

## Dependency graph

```
Part A (arbiter-core: D1, D2, tag)  ─────────────────────────┐
                                                              │
Spec N ──► rewriter-go v0.9.1 + heredoc fix on #141's branch  │
              │                                               │
              └─► Part B (gate) ──► Part C (housegate v0.12.0)─┼─► Part E (sentio-node v0.1.0) ──► Part F
                                          │                    │
                                          └─► Part D (arbiter) ┘
```

Part A may be executed at any time, including in parallel with Spec N. Everything else is serial.

## File map

| Repo | Create | Modify |
|---|---|---|
| arbiter-core | `snode/payload_mismatch_class_test.go` | `snode/staged.go`, `snode/config.go`, `snode/recorded_bindings_test.go`, `snode/config_external_test.go` (or whichever file holds the existing config-validation table), `snode/staged_backpressure_test.go` |
| housegate | — | `go.mod`, `go.sum`, `.github/workflows/ci.yml`, `configs/local.server.yaml`, `configs/local.server-mock-remote.yaml`, `CLAUDE.md`, `docs/superpowers/specs/2026-08-25-storage-integrity-production-rollout-design.md`, `docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md` |
| arbiter | — | `go.mod`, `go.sum` |
| sentio-node | `standalone/storage_integrity_acceptance_ch_test.go` | `go.mod`, `go.sum`, `storageintegrityadapter/adapter.go`, `storageintegrityadapter/adapter_test.go`, `standalone/standalone.go`, `standalone/storage_integrity_bootstrap_test.go`, `standalone/storage_integrity_smoke_test.go`, `.github/workflows/ci.yml` |

---

## Part A — arbiter-core (D1 sentinel split, D2 hard-parts disable, release)

**Working directory for every Part A task:** `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

Part A is independent of Specs N and O's other parts. It only gates Part E. Run it first, or in parallel with Spec N.

> **Docker note for all of Part A:** `snode` has both pure-Go and ClickHouse-bound tests. `requireCH(t)` gates on `ARBITER_CH_INTEGRATION` (`snode/ch_test.go:14`) and Go reports a skip as `PASS`, so a green `bazel test //...` is **not** evidence the docker tests ran (roadmap §1f; Spec P D6 fixes the convention, not this plan). Every task below therefore says which of its assertions are pure-Go and which need `ARBITER_CH_INTEGRATION=1` + `CH_ADDR`.

- [x] **Task 0 (pre-flight, do once):** branch and prove the baseline is green.

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
git checkout main && git pull --ff-only
git checkout -b feat/si-payload-mismatch-class
bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors
git rev-parse --short HEAD
```

Expected: build and test green. Record the commit as `<arbiter-core-base>`; every "regression vs. baseline" judgement in Part A compares against this run's failing set (which should be empty).

### Task 1: D1 — split `ErrPayloadMismatch` by "could an unsafe write exist?"

Spec O §1e's finding: the remediation round's instruction to map `ErrPayloadMismatch` wholesale to terminal-reject is unsound, because in arbiter-core the sentinel spans two semantic classes and HouseGate's terminal-reject contract means *provably no write*, not *not retryable*.

**Deviation O-2 applies here and changes the mechanism.** Spec O D1 assigns raise sites to classes by line number: `staged.go:122,130,194,198,202` → `PreWrite`, `staged.go:242,245,248,251` → `PostRecord`. Lines 194/198/202 are inside `validatePrepareBindings`, which has **two** callers with opposite write-possibility:

| Caller | Reached | Class |
|---|---|---|
| `PrepareLocalStatement` (`staged.go:79`) | before `journal.load`, before `journal.save` | pre-write |
| `validateRecordedBindings` (`staged.go:212`) → `RegisterPreparedClaim` (`:342`), `AbortPreparedStatement` (`:402`), and the lookup/converge paths | only after a durable record was loaded; in `RegisterPreparedClaim` the lifecycle is already `UnsafeWritten` or `RCBound`, so candidate parts definitely exist | post-record |

The existing test file `snode/recorded_bindings_test.go` is the proof: `recordedBindingMutations()` drives exactly lines 194/198/202 through `LookupPreparedStatement`, `RegisterPreparedClaim`, `AbortPreparedStatement` and `ConvergeStartup`, all post-record, and asserts `want: ErrPayloadMismatch`. A line-keyed split would relabel all four as pre-write, HouseGate's `abortTerminalPrepareReject` would hard-error on the recovered parts, and the retry loop would wedge permanently — the exact defect D1 exists to prevent. **The class is therefore a parameter, not a line.**

**Files:**
- Modify: `snode/staged.go` (the `var` block at `:47-57`; `validatePrepareBindings` at `:189-205`; its two call sites at `:79` and `:212`; the two raises at `:122,130`; the four raises in `validateReplayRequest` at `:242,245,248,251`)
- Modify: `snode/recorded_bindings_test.go` (`recordedBindingMutations()`'s three `want: ErrPayloadMismatch` entries)
- Create: `snode/payload_mismatch_class_test.go`

**Interfaces:**
- Produces: `snode.ErrPayloadMismatchPreWrite` and `snode.ErrPayloadMismatchPostRecord`, both wrapping `snode.ErrPayloadMismatch`. sentio-node Task 16 consumes `ErrPayloadMismatchPreWrite` by name; nothing consumes `ErrPayloadMismatchPostRecord` except its own tests and the negative assertions.
- Changes (package-private): `validatePrepareBindings(req PrepareRequest, class error) (string, int, error)`.

- [x] **Step 1: Add the classification test (red)**

Create `snode/payload_mismatch_class_test.go`. It has three parts: a **mutual-exclusion** unit assertion, a **behavioural table** over every raise site reachable without docker, and a **source-text guard** so a future raise site cannot inherit the wrong class by copy-paste.

```go
package snode

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestPayloadMismatchSentinelsAreAdditiveAndDisjoint pins the contract every
// consumer relies on: both new sentinels still satisfy errors.Is against the
// original (so the split cannot silently break a caller), and neither
// satisfies errors.Is against the other (so "pre-write" and "post-record" are
// answerable questions rather than overlapping labels).
func TestPayloadMismatchSentinelsAreAdditiveAndDisjoint(t *testing.T) {
	if !errors.Is(ErrPayloadMismatchPreWrite, ErrPayloadMismatch) {
		t.Fatal("ErrPayloadMismatchPreWrite must still match ErrPayloadMismatch")
	}
	if !errors.Is(ErrPayloadMismatchPostRecord, ErrPayloadMismatch) {
		t.Fatal("ErrPayloadMismatchPostRecord must still match ErrPayloadMismatch")
	}
	if errors.Is(ErrPayloadMismatchPreWrite, ErrPayloadMismatchPostRecord) {
		t.Fatal("the two classes must be disjoint: pre-write matched post-record")
	}
	if errors.Is(ErrPayloadMismatchPostRecord, ErrPayloadMismatchPreWrite) {
		t.Fatal("the two classes must be disjoint: post-record matched pre-write")
	}
}

// TestStagedGoRaisesOnlyClassifiedPayloadMismatches is the copy-paste guard.
// staged.go may name the bare ErrPayloadMismatch in exactly three places: its
// own declaration and the two derived sentinels. Every raise site must name a
// class (or the `class` parameter), so adding a raise site that reuses the bare
// sentinel fails here rather than silently defaulting to an unknown class.
func TestStagedGoRaisesOnlyClassifiedPayloadMismatches(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "staged.go", nil, 0)
	if err != nil {
		t.Fatalf("parse staged.go: %v", err)
	}
	bare := 0
	ast.Inspect(file, func(n ast.Node) bool {
		ident, ok := n.(*ast.Ident)
		if ok && ident.Name == "ErrPayloadMismatch" {
			bare++
		}
		return true
	})
	if bare != 3 {
		t.Fatalf("staged.go references ErrPayloadMismatch %d times, want exactly 3 "+
			"(its declaration plus the two derived sentinels); every raise site must "+
			"use ErrPayloadMismatchPreWrite, ErrPayloadMismatchPostRecord, or the class parameter", bare)
	}

	// validatePrepareBindings must not hard-code a class: it is called from
	// both a pre-write and a post-record context (see the table in the plan).
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "validatePrepareBindings" {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			ident, ok := n.(*ast.Ident)
			if ok && (ident.Name == "ErrPayloadMismatchPreWrite" || ident.Name == "ErrPayloadMismatchPostRecord") {
				t.Fatalf("validatePrepareBindings names %s directly; it must wrap the caller-supplied class, "+
					"because PrepareLocalStatement calls it pre-write and validateRecordedBindings calls it post-record", ident.Name)
			}
			return true
		})
	}
}

// TestPayloadMismatchClassPerRaiseSite drives every raise site reachable
// without ClickHouse and asserts its class. The rule is not the message: it is
// "is this reachable after journal.save has succeeded for this statement id?".
func TestPayloadMismatchClassPerRaiseSite(t *testing.T) {
	// Pre-write: PrepareLocalStatement validates bindings at staged.go:79,
	// before journal.load and before any conn use, so unexpectedQueryConn is
	// enough. Cases correspond to staged.go:194, :198, :202 and :122.
	//
	// Post-record: the same three binding checks reached through
	// validateRecordedBindings (staged.go:212) from RegisterPreparedClaim and
	// AbortPreparedStatement, plus validateReplayRequest's four raises
	// (staged.go:242, :245, :248, :251) reached from PrepareLocalStatement
	// when journal.load returns an existing record.
	//
	// Build both halves on the existing helpers — newRecordedBindingTestRole
	// (recorded_bindings_test.go:57), recordedBindingMutations
	// (:16), stagedRequest and nativePayload — and for each case assert:
	//   errors.Is(err, ErrPayloadMismatch)               == true   (additive)
	//   errors.Is(err, <the expected class>)             == true
	//   errors.Is(err, <the other class>)                == false
	t.Fatal("implement per Step 1; this placeholder must be removed")
}
```

Write the third function against the helpers named in its comment; do not invent a new harness and do not leave the `t.Fatal` placeholder in the committed code. Cover at minimum: pre-write × {payload format, request encoding, client revision zero, revision disagreement, payload binding mismatch}; post-record × {the three `recordedBindingMutations` payload-mismatch entries via `RegisterPreparedClaim` and via `AbortPreparedStatement`, plus envelope-changed / encoding-changed / revision-changed / payload-binding-changed via a seeded journal record}. The one raise site not reachable without a payload store — `nativepayload.Decode` at `staged.go:130` — is covered by the source-text guard above; say so in a comment rather than pretending it is behaviourally covered.

- [x] **Step 2: Run the new test and confirm it fails**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
bazel test //snode:snode_test --test_filter='TestPayloadMismatch|TestStagedGoRaisesOnly' --test_output=all --nocache_test_results
```

Expected: **compile failure** — `undefined: ErrPayloadMismatchPreWrite`, `undefined: ErrPayloadMismatchPostRecord`. That is this guard's red: the sentinels do not exist yet. Record the exact compiler output.

- [x] **Step 3: Add the two sentinels**

In `snode/staged.go`, replace the `ErrPayloadMismatch` line inside the `var (...)` block at `:47-57` with:

```go
	ErrPayloadMismatch = errors.New("snode: payload does not match envelope")
	// ErrPayloadMismatchPreWrite is raised only where no unsafe write can
	// exist for this statement id yet: before journal.save has succeeded.
	// HouseGate maps this class — and only this class — to
	// sicore.ErrPrepareTerminalReject, whose contract is "provably no write";
	// its abortTerminalPrepareReject hard-errors on a non-empty candidate set.
	ErrPayloadMismatchPreWrite = fmt.Errorf("%w (proved before any unsafe write)", ErrPayloadMismatch)
	// ErrPayloadMismatchPostRecord is raised where a durable record already
	// exists, so an unsafe write may have happened. It must stay non-terminal
	// and go through the ordinary source-lookup path: "not retryable" is not
	// "did not write".
	ErrPayloadMismatchPostRecord = fmt.Errorf("%w (a durable record already exists)", ErrPayloadMismatch)
```

`fmt` is already imported (`staged.go:6`). Both derived values wrap the original with `%w`, so `errors.Is(x, ErrPayloadMismatch)` stays true for every existing caller; neither wraps the other, so the classes are disjoint.

- [x] **Step 4: Parameterise `validatePrepareBindings` and reclassify every raise site**

In `snode/staged.go`:

1. Change the signature and all three wraps:

```go
// validatePrepareBindings checks the signed payload-format and client-revision
// bindings. class selects which payload-mismatch sentinel its failures carry:
// a fresh prepare is pre-write, while a check re-run against a recovered
// journal record is post-record. The caller owns that fact; this function
// cannot observe it.
func validatePrepareBindings(req PrepareRequest, class error) (string, int, error) {
	if req.Envelope.PayloadFormat != stagedNativeEncoding {
		return "", 0, fmt.Errorf("signed payload format %q: %w", req.Envelope.PayloadFormat, ErrEncodingNotSupported)
	}
	if req.Envelope.ClientRevision == 0 {
		return "", 0, fmt.Errorf("signed client revision must be non-zero: %w", class)
	}
	if req.PayloadEncoding != req.Envelope.PayloadFormat {
		return "", 0, fmt.Errorf("request payload encoding %q does not match signed payload format %q: %w",
			req.PayloadEncoding, req.Envelope.PayloadFormat, class)
	}
	if req.Revision <= 0 || uint64(req.Revision) != uint64(req.Envelope.ClientRevision) {
		return "", 0, fmt.Errorf("request revision %d does not match signed client revision %d: %w",
			req.Revision, req.Envelope.ClientRevision, class)
	}
	return req.Envelope.PayloadFormat, int(req.Envelope.ClientRevision), nil
}
```

`ErrEncodingNotSupported` is **not** reclassified: it is already terminal-and-pre-write in every caller's eyes and HouseGate already maps it (`adapter.go:73`). Leave it alone.

2. `staged.go:79` becomes `payloadEncoding, revision, err := validatePrepareBindings(req, ErrPayloadMismatchPreWrite)`.
3. `staged.go:212` (inside `validateRecordedBindings`) becomes `_, _, err := validatePrepareBindings(PrepareRequest{...}, ErrPayloadMismatchPostRecord)`.
4. `staged.go:122` and `:130` swap `ErrPayloadMismatch` for `ErrPayloadMismatchPreWrite` — both are before `journal.save` at `:166`; the only way to reach `:121` with a record present is a `LifecycleCleaned` record, which by definition left no unsafe bytes.
5. `staged.go:242,245,248,251` (inside `validateReplayRequest`) swap `ErrPayloadMismatch` for `ErrPayloadMismatchPostRecord` — `validateReplayRequest` is called at `:94` only when `journal.load` returned `ok`.

- [x] **Step 5: Replace, do not extend, the existing post-record expectations**

In `snode/recorded_bindings_test.go`, change the three `want: ErrPayloadMismatch` entries in `recordedBindingMutations()` to `want: ErrPayloadMismatchPostRecord`. This is the same discipline Spec O D4 mandates on the sentio-node side: after this task no arbiter-core path raises the bare sentinel, so a test asserting its classification would be asserting dead behaviour. Leave the `ErrEncodingNotSupported` entry untouched.

If the surrounding assertions use `errors.Is(err, tc.want)` they need no other change — `ErrPayloadMismatchPostRecord` is a strictly narrower assertion than the old one.

- [x] **Step 6: Run the pure-Go suite**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors
```

Expected: green, with the same failing set as Task 0 (empty). The classification test's placeholder `t.Fatal` must be gone.

- [x] **Step 7: Run the ClickHouse-bound `snode` tests**

The pre-write raise sites are also asserted end-to-end by `TestPrepareLocalStatement_RejectsRequestEnvelopeBindingMismatchBeforeWrite` (`staged_prepare_test.go:273`), which additionally proves no parts and no journal record were created — the behavioural meaning of "pre-write".

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
# Start a ClickHouse with Keeper as .github/workflows/ci.yml's integration job does,
# export CH_ADDR, then:
ARBITER_CH_INTEGRATION=1 bazel test //snode:snode_test \
  --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR \
  --test_filter='TestPrepareLocalStatement' --test_arg=-test.v \
  --test_timeout=900 --test_output=all --nocache_test_results 2>&1 | tee /tmp/snode-prepare.log
grep -c -- '--- PASS: TestPrepareLocalStatement' /tmp/snode-prepare.log
grep -c -- '--- SKIP: TestPrepareLocalStatement' /tmp/snode-prepare.log
```

Expected: a non-zero PASS count and a **zero** SKIP count. A skip here is a red, not a pass — `requireCH(t)` silently skips when `ARBITER_CH_INTEGRATION` is unset and Go reports that as `PASS`.

**If any of Steps 6–7 is red, stop the chain here.** Do not start Part B, C, D or E; nothing downstream can be judged while arbiter-core's classification is unproven.

- [x] **Step 8: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
git add snode/staged.go snode/recorded_bindings_test.go snode/payload_mismatch_class_test.go
git commit -m "feat(snode): split ErrPayloadMismatch into pre-write and post-record classes (Spec O D1)"
```

The commit message body must carry the O-2 reasoning: the class is a parameter because `validatePrepareBindings` has one pre-write caller and one post-record caller, and HouseGate's terminal-reject contract means *provably no write*.

### Task 2: D2 — an explicit, doubly-stated hard-parts disable

Spec O §1f: `snode/config.go:107-112` rewrites `HardPartsPerPartition == 0` to `DefaultHardPartsPerPartition` (2950) and rejects negatives, so there is no way to express "disabled". `backpressure: {enabled: false, hard_parts_per_partition: 0}` passes both validators while `staged.go:150` still refuses at 2950.

**Files:**
- Modify: `snode/config.go` (the `Config` struct at `:22-46`; the `HardPartsPerPartition` branch of `validate()` at `:107-112`)
- Modify: `snode/staged.go` (the hard-limit loop at `:149-154`)
- Modify: `snode/config_external_test.go` (or the nearest existing config-validation test file — pick by reading, do not create a new one if a table already exists)

**Interfaces:**
- Produces: `snode.Config.DisableHardParts bool`. sentio-node Task 15 sets it from `storage_integrity.runtime.backpressure.enabled`.

- [x] **Step 1: Add the config tests (red)**

Append to the existing config-validation test file (`snode/config_external_test.go`, or wherever the current table lives — read before writing):

```go
func TestConfigRejectsHalfConfiguredHardPartsDisable(t *testing.T) {
	cfg := testConfigS(t)
	cfg.DisableHardParts = true
	cfg.HardPartsPerPartition = 2950
	err := cfg.validate()
	if err == nil {
		t.Fatal("DisableHardParts with a non-zero hard_parts_per_partition must be a validation error")
	}
	if !strings.Contains(err.Error(), "disable_hard_parts") {
		t.Fatalf("error must name the offending field, got %v", err)
	}
}

func TestConfigDisableHardPartsKeepsTheLimitZero(t *testing.T) {
	cfg := testConfigS(t)
	cfg.DisableHardParts = true
	cfg.HardPartsPerPartition = 0
	if err := cfg.validate(); err != nil {
		t.Fatalf("an explicit disable with a zero limit must validate, got %v", err)
	}
	if cfg.HardPartsPerPartition != 0 {
		t.Fatalf("validate must not default the limit to %d when the check is disabled, got %d",
			DefaultHardPartsPerPartition, cfg.HardPartsPerPartition)
	}
}

func TestConfigZeroHardPartsStillDefaultsWhenNotDisabled(t *testing.T) {
	cfg := testConfigS(t)
	cfg.HardPartsPerPartition = 0
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.HardPartsPerPartition != DefaultHardPartsPerPartition {
		t.Fatalf("HardPartsPerPartition = %d, want the safe default %d for anyone who did not opt out",
			cfg.HardPartsPerPartition, DefaultHardPartsPerPartition)
	}
}
```

`testConfigS(t)` is the existing helper (used by `recorded_bindings_test.go:60`). If it lives in a file that is not visible from the chosen test file, put the new tests in that file instead of moving the helper. `validate()` has a pointer receiver and mutates, so call it on an addressable `cfg`.

- [x] **Step 2: Add the back-pressure skip test (red)**

The config change alone does not disable anything; `staged.go:150` must honour it. Add, next to the existing back-pressure tests in `snode/staged_backpressure_test.go`:

```go
func TestPrepareLocalStatement_DisableHardPartsSkipsTheSourceRefusal(t *testing.T) {
	// Mirror the existing hard-limit test in this file, but with
	// cfg.DisableHardParts = true and cfg.HardPartsPerPartition = 0, and
	// assert the prepare SUCCEEDS where the enabled configuration refuses
	// with ErrBackpressure. This is a deliberate footgun made explicit
	// (Spec O D2): with it set, inserts fail at ClickHouse's own
	// parts_to_throw_insert instead of at the source.
	t.Fatal("implement per Step 2; this placeholder must be removed")
}
```

Read the existing hard-limit test in that file and mirror its setup exactly — same schema, same part-count seeding — changing only the two config fields and the expectation. It is ClickHouse-bound like its sibling.

- [x] **Step 3: Run both and confirm they fail**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
bazel test //snode:snode_test --test_filter='TestConfig.*HardParts' --test_output=all --nocache_test_results
```

Expected: **compile failure** — `cfg.DisableHardParts undefined`. Record it.

- [x] **Step 4: Implement**

In `snode/config.go`, add the field to `Config` immediately after `HardPartsPerPartition`:

```go
	// DisableHardParts turns off the source-side hard parts-per-partition
	// refusal entirely. It is a deliberate footgun made explicit: with it set,
	// inserts fail at ClickHouse's own parts_to_throw_insert instead of at the
	// source. Configuring it together with a non-zero HardPartsPerPartition is
	// a validation error, so a half-configured disable is not expressible.
	DisableHardParts bool
```

Replace the `HardPartsPerPartition` branch of `validate()` (`:107-112`) with:

```go
	switch {
	case c.HardPartsPerPartition < 0:
		errs = append(errs, errors.New("hard parts per partition must not be negative"))
	case c.DisableHardParts && c.HardPartsPerPartition != 0:
		errs = append(errs, errors.New("disable_hard_parts requires hard_parts_per_partition to be 0; a half-configured disable is not allowed"))
	case c.DisableHardParts:
		// Leave the limit at 0: nothing reads it while the check is skipped,
		// and defaulting it would make the disable invisible in a config dump.
	case c.HardPartsPerPartition == 0:
		c.HardPartsPerPartition = DefaultHardPartsPerPartition
	}
```

In `snode/staged.go`, guard the loop at `:149-154`:

```go
	if !r.cfg.DisableHardParts {
		for _, partitionID := range touched {
			if n := len(inventory[partitionID]); n >= r.cfg.HardPartsPerPartition {
				return PreparedLocalResult{}, fmt.Errorf("%w: %s.%s partition %s has %d active parts (hard limit %d)",
					ErrBackpressure, r.cfg.UnsafeDatabase, table, partitionID, n, r.cfg.HardPartsPerPartition)
			}
		}
	}
```

Keep the `before`/`inventory` computation outside the guard: `inventory` feeds `intakeRecord.PreWriteInventory` at `:163` and is not back-pressure state.

- [x] **Step 5: Verify**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors
# then, with ClickHouse up and CH_ADDR exported:
ARBITER_CH_INTEGRATION=1 bazel test //snode:snode_test \
  --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR \
  --test_filter='HardParts|Backpressure' --test_arg=-test.v \
  --test_timeout=900 --test_output=all --nocache_test_results 2>&1 | tee /tmp/snode-hardparts.log
grep -c -- '--- SKIP:' /tmp/snode-hardparts.log
```

Expected: pure-Go suite green; the filtered docker run shows PASS markers and a **zero** SKIP count.

**If red, stop the chain here.**

- [x] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
git add snode/config.go snode/staged.go snode/config_external_test.go snode/staged_backpressure_test.go
git commit -m "feat(snode): add an explicit, doubly-stated hard-parts disable (Spec O D2)"
```

### Task 3: merge Part A and cut the arbiter-core release

- [ ] **Step 1: Open and merge the PR**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
git push -u origin feat/si-payload-mismatch-class
gh pr create --fill --title "feat(snode): payload-mismatch classes and an explicit hard-parts disable (Spec O D1/D2)"
```

PR body must carry: the O-2 reasoning table (both callers of `validatePrepareBindings`), the red evidence from Task 1 Step 2 and Task 2 Step 3, and the docker run's PASS/SKIP counts. Merge only when CI (including the `integration-clickhouse` job) is green.

- [ ] **Step 2: Cut the tag**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
git checkout main && git pull --ff-only
bash scripts/next-version.sh   # prints the exact version Cut Release will use
gh workflow run cut-release.yml --ref main
```

**Deviation O-3.** `scripts/next-version.sh` derives the version from the previous annotated tag's UTC calendar day: same day → patch, later day → minor. `v0.5.1` was tagged `2026-08-25T01:57:52Z`, so a cut on 2026-08-25 UTC prints **v0.5.2**, and `cut-release.yml` is a bare `workflow_dispatch` with no bump override. Spec O D5's "**v0.6.0**" is only reachable by cutting on a **later UTC day**. Decide explicitly and record it:

- Want v0.6.0 (Spec O's literal): wait until the next UTC day, then dispatch.
- Want the chain to move now: dispatch today and accept v0.5.2.

Either way, **run `scripts/next-version.sh` first and record its output as `<arbiter-core-tag>`.** Every later task in this plan refers to `<arbiter-core-tag>`, never to a literal version. Do **not** hand-push a `v0.6.0` tag to force the number: annotated release tags are the ledger `next-version.sh` reads (`scripts/next-version.sh:10-11`), so a manual tag dated today would make the *next* real cut compute `v0.6.1` and would skip the workflow's validation, two-node ClickHouse integration run and release publication entirely.

- [ ] **Step 3: Verify the tag is fetchable as a Go module**

```bash
cd $(mktemp -d) && go mod init tagcheck >/dev/null
GOFLAGS=-mod=mod go get github.com/sentioxyz/arbiter-core@<arbiter-core-tag>
grep arbiter-core go.mod
```

Expected: the `require` line names `<arbiter-core-tag>`. A private-module fetch failure here is a proxy/auth problem, not a release problem — fix it now rather than inside Part E's three-pin bump, where it would be one failure among several.

**If red, stop the chain here.** Part E cannot start without a fetchable `<arbiter-core-tag>`.

---

## Part B — rewriter-go v0.9.1 (consumed, not produced here)

**This plan does not build rewriter-go.** Spec N produces v0.9.1 (Spec N §5: "rewriter-go and rewriter-grpc land D2/D4/D5 together with the corpus …, then rewriter-go cuts **v0.9.1**"), and separately lands its D1 heredoc fix **onto PR #141's own branch**, because D1 closes a hole PR #141 itself introduces. Part C is a hard block on both.

### Task 4: gate — prove Spec N's two outputs exist before touching housegate

**Files:** none. This task produces evidence, not a commit.

- [ ] **Step 1: The rewriter-go tag exists and is a real release**

```bash
gh release view v0.9.1 --repo housegate/rewriter-go --json tagName,publishedAt,assets \
  --jq '{tag: .tagName, published: .publishedAt, assets: [.assets[].name]}'
```

Expected: `tag = v0.9.1`, and the asset list contains **all three** of `libpolyglot_sql_ffi-linux-x86_64.so`, `libpolyglot_sql_ffi-macos-arm64.dylib`, `SHA256SUMS`. Those exact names come from `pkg/ffifetch/ffifetch.go:56-63` and `:139`; a release missing the `.so` breaks CI (linux/x64 runner), one missing the `.dylib` breaks local darwin/arm64 verification, and one missing `SHA256SUMS` silently degrades `ffifetch` to TLS-only trust with nothing but a warning.

- [ ] **Step 2: The Go module resolves at that tag**

```bash
cd $(mktemp -d) && go mod init tagcheck >/dev/null
GOFLAGS=-mod=mod go get github.com/housegate/rewriter-go@v0.9.1 && grep rewriter-go go.mod
```

Expected: the `require` line names `v0.9.1`.

- [ ] **Step 3: Spec N's D1 heredoc fix is on PR #141's branch**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git fetch origin feature/si-surface-failclosed-housegate
git log --oneline origin/main..origin/feature/si-surface-failclosed-housegate | cat
git show origin/feature/si-surface-failclosed-housegate:pkg/plugins/sireserved/plugin.go | grep -c 'heredoc\|\$tag\$\|scanHeredoc'
```

Expected: the commit list contains a heredoc/`sireserved` scanner commit **beyond** the 11 commits recorded in this plan's version table, and the grep count is non-zero. **If the heredoc fix is not on that branch, stop: the roadmap says #141 must not merge without it** (roadmap §3, "N first and alone").

**If any step is red, stop the chain here.** Parts C, D, E and F all depend on this gate.

---

## Part C — housegate (pin bump, #141 merge, v0.12.0)

**Working directory for every Part C task:** `/Users/uranuswch/Dev/housegate/housegate`

**Deviation O-1 governs this whole part.** PR #141's branch already carries `rewriter-go v0.9.0` in `go.mod` and `--tag v0.9.0` in `.github/workflows/ci.yml`. The bump this part performs is therefore **v0.9.0 → v0.9.1 on that branch**, not v0.7.1 → v0.9.1 on `main`. Bumping `main` separately would conflict with the PR; merging the PR first would put `main` on an engine without Spec N's half for however long the follow-up takes.

### Task 5: bump the rewriter-go pin to v0.9.1 — go.mod and the CI FFI tag in one commit

The upgrade skill calls a split pin the classic mistake, and Spec O D5 repeats it: "they are one pin expressed twice". Note that PR #141's own branch already violated this (`91ff17e build: bump rewriter-go to v0.9.0` and `7f6f053 fix(ci): pin rewriter FFI to v0.9.0` are two commits). Do not repeat it here.

**Files:**
- Modify: `go.mod`, `go.sum`
- Modify: `.github/workflows/ci.yml` (line ~111, the `fetch-rewriter-lib --tag` argument)
- Modify: `configs/local.server.yaml` (line ~48, `native_library_release:`)
- Modify: `configs/local.server-mock-remote.yaml` (the same key, commented out)
- Modify: `CLAUDE.md` (the `pkg/ffifetch` bullet's "requires an FFI library built from rewriter-go >= vX.Y.Z (polyglot >= vA.B.C — the go.mod floor)" sentence)

- [ ] **Step 1: Check out the PR branch and record the pre-bump state**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git fetch origin feature/si-surface-failclosed-housegate
git checkout feature/si-surface-failclosed-housegate
git pull --ff-only
grep -n 'rewriter-go\|tobilg/polyglot' go.mod
grep -n 'fetch-rewriter-lib' .github/workflows/ci.yml
grep -n 'native_library_release' configs/local.server.yaml configs/local.server-mock-remote.yaml
grep -n 'rewriter-go >=' CLAUDE.md
```

Expected: every hit says `v0.9.0` (and polyglot `v0.9.2`). Record the exact lines; Step 4 asserts none of them still says `v0.9.0`.

- [ ] **Step 2: Establish the pre-bump Bazel baseline**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel build //... && bazel test //... --test_output=errors 2>&1 | tee /tmp/hg-baseline.log
grep -E '^//.*(FAILED|TIMEOUT)' /tmp/hg-baseline.log | sort > /tmp/hg-baseline-failures.txt
wc -l /tmp/hg-baseline-failures.txt
```

Expected: zero failures (PR #141's three required checks are `SUCCESS`). This file is the baseline that Step 5 compares against — per CLAUDE.md, a failing set that matches the baseline is not a regression.

- [ ] **Step 3: Bump**

There is no `replace` directive for rewriter-go (it is a plain `require`; housegate's three replaces are `wasmer-go`, `ch-go` and `clickhouse-go/v2`, `go.mod:126-130`), so `go get` is the right tool:

```bash
cd /Users/uranuswch/Dev/housegate/housegate
go get github.com/housegate/rewriter-go@v0.9.1 && go mod tidy
bazel mod tidy && bazel run //:gazelle
sed -i '' 's|fetch-rewriter-lib --tag v0.9.0|fetch-rewriter-lib --tag v0.9.1|' .github/workflows/ci.yml
sed -i '' 's|native_library_release: v0.9.0|native_library_release: v0.9.1|' configs/local.server.yaml
sed -i '' 's|native_library_release: v0.9.0|native_library_release: v0.9.1|' configs/local.server-mock-remote.yaml
```

Then hand-edit the `CLAUDE.md` `pkg/ffifetch` bullet's floor sentence to name `rewriter-go >= v0.9.1` and whatever polyglot version `go.mod` now carries (read it — do not assume it is still `v0.9.2`; MVS may move it). Expect a wide transitive diff; that is MVS, not a mistake. **Do not hand-pin transitives back down.**

- [ ] **Step 4: Assert the pin is expressed consistently everywhere**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
! grep -rn 'v0\.9\.0' go.mod .github/workflows/ci.yml configs/local.server.yaml configs/local.server-mock-remote.yaml CLAUDE.md
grep -rn 'v0\.9\.1' go.mod .github/workflows/ci.yml configs/local.server.yaml configs/local.server-mock-remote.yaml CLAUDE.md
```

Expected: the first command succeeds (no `v0.9.0` survives in any of the five files) and the second prints **five or more** hits — one per file. A file left behind here is the split-pin failure mode the skill's common-mistakes table names.

- [ ] **Step 5: Compile, vet, and the full Bazel gate**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
go build ./...
go vet ./... 2>&1 | grep -v 'unkeyed fields'
bazel build //... && bazel test //... --test_output=errors 2>&1 | tee /tmp/hg-bumped.log
grep -E '^//.*(FAILED|TIMEOUT)' /tmp/hg-bumped.log | sort > /tmp/hg-bumped-failures.txt
diff /tmp/hg-baseline-failures.txt /tmp/hg-bumped-failures.txt && echo "no regression vs the pre-bump baseline"
```

The `config.Duration ... unkeyed fields` vet notes are pre-existing noise; everything else is real. `go build ./...` does not compile `_test.go` files — `go vet` and `bazel test` are what catch test-only API breakage.

**If the diff is non-empty, stop the chain here** and attribute each new failure before proceeding.

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add go.mod go.sum .github/workflows/ci.yml configs/local.server.yaml configs/local.server-mock-remote.yaml CLAUDE.md
git commit -m "build: bump rewriter-go and the CI FFI tag to v0.9.1 (Spec O D5)"
```

One commit, six files. The message body records that the Go module and the FFI binary are one pin expressed twice and must never drift.

### Task 6: prove the resolved FFI library is actually v0.9.1

Spec O §4 item 3: "the plan asserts the resolved library path, since a stale cache hit would silently keep the old engine." Path-shape alone is a weak assertion — `ffifetch` keys the cache on the tag (`<CacheDir>/<tag>/libpolyglot_sql_ffi.<ext>`) and a cache hit does zero network I/O, so a directory populated by an earlier bad fetch would still print a v0.9.1-shaped path. This task purges, re-fetches, and checks the digest against the release manifest.

**Files:** none. This task produces evidence, not a commit.

- [ ] **Step 1: Purge the tag's cache directory, then fetch**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
CACHE="$(go run ./cmd fetch-rewriter-lib --tag v0.9.1 2>/dev/null | tail -n 1)"
rm -rf "$(dirname "$CACHE")"
FFI="$(go run ./cmd fetch-rewriter-lib --tag v0.9.1 2>/dev/null | tail -n 1)"
echo "$FFI"
```

`fetch-rewriter-lib` prints the resolved path to stdout and logs to stderr (`cmd/main.go`), so `2>/dev/null | tail -n 1` is the resolved path. The first invocation exists only to learn the cache directory; the `rm -rf` then guarantees the second is a real download.

- [ ] **Step 2: Assert the path carries the tag and the bytes match the release manifest**

```bash
case "$FFI" in *"/v0.9.1/"*) echo "path carries the tag" ;; *) echo "STALE OR WRONG TAG: $FFI"; exit 1 ;; esac
test -s "$FFI"
ASSET="$(basename "$FFI" | sed 's/libpolyglot_sql_ffi/libpolyglot_sql_ffi-macos-arm64/')"   # linux: -linux-x86_64.so
gh release download v0.9.1 --repo housegate/rewriter-go --pattern SHA256SUMS --dir /tmp/ffi-sums --clobber
grep -F "$ASSET" /tmp/ffi-sums/SHA256SUMS
shasum -a 256 "$FFI"
```

Expected: the path contains `/v0.9.1/`, the file is non-empty, and `shasum`'s digest **equals** the `SHA256SUMS` entry for the platform asset. A mismatch means the release is corrupt or a mirror is stale — stop.

- [ ] **Step 3: Prove the native engine actually loaded (the tests must RUN, not skip)**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //pkg/rewriter:rewriter_test \
  --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" \
  --test_output=all --test_arg=-test.v --nocache_test_results 2>&1 | tee /tmp/hg-native.log
grep -c -- '--- PASS: TestNativeEngineSmoke' /tmp/hg-native.log
grep -c -- '--- SKIP: TestNativeEngineSmoke' /tmp/hg-native.log
```

Expected: PASS count 1, SKIP count 0. `TestNativeEngineSmoke` and `TestMaterialize_NativeSmoke` `t.Skip()` when `POLYGLOT_SQL_FFI_PATH` is unset, and a green `--test_output=errors` run proves nothing — this is the skill's "Green rewriter test without setting `POLYGLOT_SQL_FFI_PATH`" mistake.

- [ ] **Step 4: Prove the v0.9.1 engine closes the Spec I and Spec N holes end to end**

This is the behavioural half of "the new engine is actually loaded" — a digest proves the bytes, this proves the semantics.

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test \
  --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" \
  --test_env=DOCKER_HOST="$(docker context inspect --format '{{.Endpoints.docker.Host}}')" \
  --test_env=HOME --test_output=errors
```

Expected: pass, including `TestStorageIntegrityRead_CriticalStatementsAreRefused` (added by PR #141's Task 21) — which fails against v0.7.1. Then re-run the same command with the **v0.9.0** library to show the Spec N statements are still live on the old engine:

```bash
OLD="$(go run ./cmd fetch-rewriter-lib --tag v0.9.0 2>/dev/null | tail -n 1)"
bazel test //pkg/integration:integration_test \
  --test_filter='<the Spec N heredoc/SHOW regression test name from Spec N's plan>' \
  --test_env=POLYGLOT_SQL_FFI_PATH="$OLD" \
  --test_env=DOCKER_HOST="$(docker context inspect --format '{{.Endpoints.docker.Host}}')" \
  --test_env=HOME --test_output=all --nocache_test_results
```

Expected: **FAIL** against v0.9.0, pass against v0.9.1. Record both outputs in the PR — that pair is the evidence that the bump did something, not just moved a string. If Spec N's plan placed its regression coverage entirely in `pkg/plugins/sireserved` unit tests (engine-independent), say so and skip this half rather than inventing a test.

If an integration test fails, apply the main-baseline rule before calling it a regression: `git stash push`, re-run that single test with `--runs_per_test=10` on clean `main`, `git stash pop`, and record the ratio.

**If Steps 1–3 are red, stop the chain here.**

### Task 7: merge PR #141

**Files:** none in this repo's working tree; this task is a merge.

- [ ] **Step 1: Re-check the merge preconditions**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git push origin feature/si-surface-failclosed-housegate
gh pr view 141 --json state,mergeable,mergeStateStatus,statusCheckRollup \
  --jq '{state, mergeable, mergeStateStatus, checks: [.statusCheckRollup[] | {name, conclusion}]}'
```

Expected: `state=OPEN`, `mergeable=MERGEABLE`, and the three required checks (`Release tooling`, `Build`, `Integration (ClickHouse)`) all `SUCCESS` **on the commit that includes Task 5's bump and Spec N's heredoc fix**. The two `NEUTRAL` bot checks (`AgentConnect PR Review`, `Cursor Bugbot`) are not required.

Also confirm CI actually fetched the new library: open the `Fetch rewriter FFI lib` step's log and check it prints a path under `/v0.9.1/`. CI's step already asserts `test -n` and `test -s`; the tag is what this plan adds.

- [ ] **Step 2: Merge**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
gh pr merge 141 --squash --delete-branch=false
git checkout main && git pull --ff-only
grep -n 'rewriter-go' go.mod && grep -n 'fetch-rewriter-lib' .github/workflows/ci.yml
```

Expected: `main` now pins `rewriter-go v0.9.1` in both places. Use the repo's usual merge strategy if it is not squash — check with `gh repo view --json squashMergeAllowed,mergeCommitAllowed,rebaseMergeAllowed`.

- [ ] **Step 3: Post-merge gate on `main`**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel build //... && bazel test //... --test_output=errors
```

Expected: green. **If red, stop the chain here** — do not cut a release from a red `main`.

### Task 8: cut housegate v0.12.0

**Files:** none.

- [ ] **Step 1: Dispatch the release with an explicit minor bump**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git log --oneline -1 | cat
gh workflow run release.yml --ref main -f bump=minor
```

`bump=auto` gives minor on the first cut of a UTC day and **patch** on later cuts the same day (`.github/workflows/release.yml:11-15`). `v0.11.0` was cut 2026-08-25, so `auto` would yield `v0.11.1`. Pass `bump=minor` explicitly to get **v0.12.0**, which is the version Spec O D5, Part D and Part E all name.

- [ ] **Step 2: Watch it through, including the Homebrew chain**

```bash
gh run list --repo housegate/housegate --workflow release.yml --limit 1
gh run watch <run-id> --repo housegate/housegate
gh release view v0.12.0 --repo housegate/housegate --json tagName,assets --jq '{tag: .tagName, assets: [.assets[].name]}'
```

Expected: tag `v0.12.0`, both platform binaries plus `SHA256SUMS` attached. If **only** the Homebrew job fails, re-run *that job* — re-running the whole workflow cuts another tag (CLAUDE.md, CI section).

- [ ] **Step 3: Verify the tag resolves as a Go module**

```bash
cd $(mktemp -d) && go mod init tagcheck >/dev/null
GOFLAGS=-mod=mod go get github.com/housegate/housegate@v0.12.0 && grep housegate go.mod
```

Expected: the `require` line names `v0.12.0`.

**If red, stop the chain here.** Parts D, E and F all consume this tag.

---

## Part D — arbiter (housegate pin bump)

**Working directory for every Part D task:** `/Users/uranuswch/Dev/sentio_xyz/arbiter`

arbiter is not on sentio-node's dependency path (sentio-node requires `arbiter-core` and `arbiter-proto`, never `arbiter`), so this part cannot break Part E. It is here because Spec O D5 step 4 names it and because leaving the control plane on an older HouseGate than the data plane is exactly the drift this whole spec exists to remove.

### Task 9: bump arbiter's housegate pin to v0.12.0

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Branch and baseline**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git checkout main && git pull --ff-only
git checkout -b chore/bump-housegate-v0.12.0
grep -n 'housegate\|arbiter-core\|arbiter-proto' go.mod
bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors 2>&1 | tee /tmp/arbiter-baseline.log
grep -E '(FAILED|TIMEOUT)' /tmp/arbiter-baseline.log | sort > /tmp/arbiter-baseline-failures.txt
```

Expected: pins read housegate `v0.10.0`, arbiter-core `v0.5.1`, arbiter-proto `v0.6.0`. Record the baseline failing set.

- [ ] **Step 2: Bump housegate, and arbiter-core to `<arbiter-core-tag>`**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
go get github.com/housegate/housegate@v0.12.0
go get github.com/sentioxyz/arbiter-core@<arbiter-core-tag>
go mod tidy && bazel mod tidy && bazel run //:gazelle
grep -n 'housegate\|arbiter-core' go.mod
```

Spec O D5 step 4 names only the housegate pin. Bumping arbiter-core in the same commit is safe and correct here because Part A is purely additive to arbiter-core's exported surface — `ErrPayloadMismatchPreWrite` / `ErrPayloadMismatchPostRecord` are new values, `DisableHardParts` is a new field, and `validatePrepareBindings` is package-private. arbiter does not import `arbiter-core/snode`'s prepare path in a way Part A changes; confirm with `grep -rn 'ErrPayloadMismatch\|HardPartsPerPartition' --include='*.go' .` and, if that grep finds nothing, say so in the commit message. **If it finds something, split this into two commits** so a failure is attributable to one pin.

arbiter has `replace` directives for `ch-go` / `clickhouse-go/v2` / `wasmer-go` (`arbiter/go.mod:5-9`). Housegate v0.12.0's own replaces are **not** inherited — Go only honours the main module's. Confirm arbiter's fork pins still match housegate's (`sentioxyz/ch-go v0.73.0-sentioxyz-20260629`, `sentioxyz/clickhouse-go/v2 v2.47.0-sentioxyz-20260629`); if they have diverged, align them with `go mod edit -replace`, never with `go get` on the upstream path — that only moves an MVS label and changes nothing that compiles.

- [ ] **Step 3: Verify**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
go build ./... && go vet ./... 2>&1 | grep -v 'unkeyed fields'
bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors 2>&1 | tee /tmp/arbiter-bumped.log
grep -E '(FAILED|TIMEOUT)' /tmp/arbiter-bumped.log | sort > /tmp/arbiter-bumped-failures.txt
diff /tmp/arbiter-baseline-failures.txt /tmp/arbiter-bumped-failures.txt && echo "no regression vs baseline"
```

**If the diff is non-empty, stop the chain here.**

- [ ] **Step 4: Commit and merge**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git add go.mod go.sum
git commit -m "chore(deps): bump housegate to v0.12.0 and arbiter-core to <arbiter-core-tag> (Spec O D5)"
git push -u origin chore/bump-housegate-v0.12.0
gh pr create --fill --title "chore(deps): bump housegate to v0.12.0 (Spec O D5)"
```

Merge when CI is green.

---

## Part E — sentio-node (three-pin bump, D3, D4, D6, first tag)

**Working directory for every Part E task:** `/Users/uranuswch/Dev/sentio_xyz/sentio-node`

Part E requires **both** Part A's `<arbiter-core-tag>` and Part C's `housegate v0.12.0` to exist and be fetchable. Do not start it otherwise.

> **Deviation O-6 — the D6 tests keep the env gate and gain per-test anti-skip guards.** Spec O D6 says the two acceptance tests should follow "HouseGate's explicit-target convention rather than env self-skip (see Spec P D6)". Spec P D6, read at source, says the opposite for this repo: sentio-node keeps its `SENTIO_SI_CH_E2E` gate and hardens the CI `--test_filter` into a prefix filter plus a test-count assertion, and "the three existing anti-skip guards are kept". sentio-node's `ci.yml` already implements the explicit-target property the right way — it names `//standalone:standalone_test`, filters to the exact test, and then greps for `--- SKIP:`, `no tests to run`, and `--- PASS:` and fails the job on the wrong answer. **This plan follows the existing repo pattern**: env gate for local runs, `TestStorageIntegrity`-prefixed names so Spec P's prefix filter will pick them up unchanged, and a PASS-marker guard per test in CI.

### Task 10: pre-flight — branch, baseline, and the three silent-regression surveys

The empirical bump rehearsal found **two** compile breaks and **three** behaviour changes that compile and unit-test clean but change what production does. Surveying them before the bump is what turns a mystery outage into a known migration step.

**Files:** none. This task produces evidence, not a commit.

- [ ] **Step 1: Branch and baseline**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
git checkout main && git pull --ff-only
git checkout -b feat/si-production-rollout
grep -nE 'housegate|arbiter-core|arbiter-proto|rewriter-proto|rewriter-go' go.mod
bazel build //... && bazel test //... --test_output=errors 2>&1 | tee /tmp/sn-baseline.log
grep -E '(FAILED|TIMEOUT)' /tmp/sn-baseline.log | sort > /tmp/sn-baseline-failures.txt
wc -l /tmp/sn-baseline-failures.txt
```

Expected: pins read housegate `v0.9.2`, arbiter-core `v0.4.0`, arbiter-proto `v0.5.0`, rewriter-proto `v0.2.0`, rewriter-go `v0.7.1 // indirect`; **zero** baseline failures (a rehearsal measured 13 packages ok, 0 FAIL).

- [ ] **Step 2: Survey regression 1 — declared SI column types against the new whitelist**

arbiter-core `v0.5.x`'s `snode.Config.validate()` calls `payloadexec.ValidateTableSchemaColumns` (`arbiter-core/snode/config.go:75`), and `ddl.Intents` enforces the same through `CanonicalizeTableSchemaColumnTypes` (`arbiter-core/dataplane/ddl/build.go:83`). The whitelist is `String`, `FixedString(N)` for `0 < N <= 0xFFFFFF`, `Bool`, `Float32/64`, `[U]Int8/16/32/64`. A rehearsal confirmed these are **rejected**: `DateTime`, `DateTime64(3)`, `Date`, `Decimal`, `UUID`, `Nullable(...)`, `Array(...)`, `LowCardinality(...)`, `Enum8`, `Int128`, `UInt256`, `Tuple`, `Map`. This is Spec L D1 working exactly as designed, and it is also a **startup-blocking change for any deployment whose declared SI tables use one of those types**.

```bash
# For each environment that will run the bumped binary, enumerate the declared
# SI table schemas the node resolves (network_state contract snapshot, or the
# ClickHouse-derived schemas) and check every column type against the whitelist.
# Against a live ClickHouse:
clickhouse-client --host <host> --query "
  SELECT database, table, name, type
  FROM system.columns
  WHERE database IN ('hg_unsafe','hg_safe')
    AND name != '_hg_row_id'
    AND type NOT IN ('String','Bool','Float32','Float64',
                     'Int8','Int16','Int32','Int64','UInt8','UInt16','UInt32','UInt64')
    AND type NOT LIKE 'FixedString(%'
  ORDER BY database, table, name"
```

Expected: **zero rows**. Any row is a table that will refuse startup after the bump. Record the list; the remedy is a schema declaration change, which is outside this plan's scope — escalate it rather than widening the whitelist.

- [ ] **Step 3: Survey regression 2 — `hg_promote` must already exist**

arbiter-core `v0.5.x` dropped `snode/promote_replace.go`'s `CREATE TABLE IF NOT EXISTS <promote> AS <safe>` and now returns `snode.ErrPromoteTableMissing` when the table is absent, while `EnsureProtocolTables` builds and **verifies** a third `hg_promote` intent in *both* modes — including `ModeVerifyOnly`, which never creates anything. So under `schema_source: clickhouse` the promote tables must pre-exist and must match the pinned DDL. sentio-node references `ddl.ErrProtocolTableDrift` in seven places and has **no** handling for `ErrPromoteTableMissing`.

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
grep -rn 'ErrPromoteTableMissing\|ErrProtocolTableMissing\|ErrProtocolTableDrift' --include='*.go' . | grep -v bazel- 
clickhouse-client --host <host> --query "
  SELECT name FROM system.tables WHERE database = 'hg_promote' ORDER BY name"
clickhouse-client --host <host> --query "
  SELECT name FROM system.tables WHERE database = 'hg_safe' ORDER BY name"
```

Expected: the `hg_promote` table list is a superset of the `hg_safe` list for every declared SI table. If it is not, the node will fail closed at startup after the bump — which is the correct behaviour, but it must be a planned migration, not a surprise. Record the gap and either pre-create the tables from the pinned DDL or switch that environment to `schema_source: network_state` (which is `ModeCreateAndVerify`) for one bootstrap.

- [ ] **Step 4: Survey regression 3 — `schema_source: ""`**

`config/config.go:108-115` admits `""`, `"clickhouse"` and `"network_state"`, and `config_test.go:243` asserts `""` is valid. arbiter-core's `ddl.ModeFromSchemaSource("")` **errors**: `ddl: unknown schema source "" (want network_state|chain|clickhouse, or unmanaged in tests)`. A naive `SchemaSource: ddl.SchemaSource(si.SNode.SchemaSource)` in Task 13 would therefore make `snode.New` fail at startup for every deployment that omits `schema_source`. Task 13 Step 4 normalizes `""` → `ddl.SchemaSourceClickHouse` before conversion; this step just records how many deployments rely on the omission.

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
grep -rn 'schema_source' --include='*.go' --include='*.yaml' --include='*.yml' --include='*.md' . | grep -v bazel-
```

- [ ] **Step 5: Record the three findings**

Write the three surveys' results into the eventual PR body. They are the migration notes for whoever deploys `v0.1.0`, and none of them is predicted by Spec O.

### Task 11: prove nothing outside this repo set imports `ProtocolTablesMode` before deleting it

Spec O D3: "If deleting the export breaks an external consumer, the fix is to export the strict one under the same name — never to keep both." That decision only binds if there *is* an external consumer, so check before deleting.

**Files:** none. This task produces evidence, not a commit.

- [ ] **Step 1: The module path already answers most of it**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node && head -1 go.mod
```

Expected: `module compute-network-node`. That is a **bare, non-resolvable module path** — no domain, so `go get` cannot fetch it and no external Go module can import `compute-network-node/storageintegrityadapter` at all. Record this: it makes the deletion safe by construction, not merely by survey. (It is also the same non-resolvable-path problem housegate fixed by renaming to `github.com/housegate/housegate`; fixing it here is out of scope, recorded as debt.)

- [ ] **Step 2: Sweep the sibling repos and the org anyway**

```bash
for repo in /Users/uranuswch/Dev/sentio_xyz/arbiter-core /Users/uranuswch/Dev/sentio_xyz/arbiter /Users/uranuswch/Dev/housegate/housegate; do
  echo "== $repo"; grep -rn 'ProtocolTablesMode\|storageintegrityadapter' --include='*.go' "$repo" | grep -v bazel-
done
gh search code 'ProtocolTablesMode' --owner sentioxyz --owner housegate --limit 50
gh search code 'storageintegrityadapter' --owner sentioxyz --owner housegate --limit 50
```

Expected: the only hits are inside sentio-node itself (`adapter.go:36-44`, `adapter_test.go:196-199`, `standalone/storage_integrity_bootstrap_test.go:241`). **If a hit exists outside sentio-node, stop and apply Spec O D3's escape hatch**: export the strict derivation under the name `ProtocolTablesMode`, and never keep both.

### Task 12: D6 — write both acceptance tests and prove them red against the current pins

Spec O D6 and §4 item 2: both tests must be "green under docker and red against the old pins". Writing them **before** the bump is what makes that provable; writing them after would leave "it must fail against v0.9.2" as an untested claim.

Both tests compile against the current pins — the rehearsal confirmed housegate `v0.9.2` → `v0.11.0` removes **zero** exported identifiers, so `housegate.StorageIntegrityRuntimeOptions`, `chproto.CodeTooManyParts` and `sicore.ErrBackpressure` all already exist. The only thing missing at `v0.9.2` is `chproto.ClientError.KeepSession` (first appears at `v0.11.0`), and the new test must not name it.

**Files:**
- Create: `standalone/storage_integrity_acceptance_ch_test.go`

**Interfaces:**
- Produces: `TestStorageIntegrityMalformedColumnTypeCreatesNoTable` and `TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession`. Task 17 wires both names into CI; the `TestStorageIntegrity` prefix is required so Spec P D6's future prefix filter picks them up without another edit.

- [ ] **Step 1: Write the type-rejection acceptance test**

Model it on `standalone/storage_integrity_drift_ch_test.go:69-150` — same `SENTIO_SI_CH_E2E` gate, same `CH_ADDR` requirement, same `clickhouse.Open` + `Ping` + Keeper probe, same `newStorageIntegrityBootDeps` / `runStorageIntegrityProtocolPreflight` production wiring, same `t.Cleanup` drop. Change only the schema and the assertions:

```go
// TestStorageIntegrityMalformedColumnTypeCreatesNoTable is Spec L §1a's
// "permanent brick" turned into a guard: a declaration whose column type is
// outside the storage-integrity whitelist must be refused BEFORE any DDL runs,
// so the protocol tables for it do not exist afterwards. Without the refusal
// ClickHouse accepts the type, the created table drifts from the pinned shape,
// there is no auto-ALTER, and the node can never start again.
//
// Requires SENTIO_SI_CH_E2E=1 and CH_ADDR pointing at a Keeper-enabled
// ClickHouse (ReplicatedMergeTree cannot be created without one).
func TestStorageIntegrityMalformedColumnTypeCreatesNoTable(t *testing.T) {
	// ... the drift test's gate + conn + Keeper probe ...

	schema := payloadexec.TableSchema{
		TableID:     fmt.Sprintf("brick.t_%d", time.Now().UnixNano()),
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			// DateTime is outside the storage-integrity column whitelist
			// (String, FixedString(N), Bool, Float32/64, [U]Int8/16/32/64).
			{Name: "ts", Type: "DateTime"},
		},
	}
	physical := ddl.CHTableName(schema.TableID)
	// ... pinned, t.Cleanup dropping physical from every protocol database ...

	bootDeps := newStorageIntegrityBootDeps(
		conn, pinned, []payloadexec.TableSchema{schema}, ddl.ModeCreateAndVerify,
		nil, storageIntegritySchemaSets{}, "", nil, nil,
	)
	crossChecked := false
	ops := storageIntegrityBootOperationFuncs{
		ensure:     bootDeps.ensureProtocolTables,
		crossCheck: func(context.Context) error { crossChecked = true; return nil },
	}

	err := runStorageIntegrityProtocolPreflight(ctx, ops)
	require.Error(t, err, "a column type outside the whitelist must refuse the preflight")
	require.ErrorContains(t, err, "DateTime",
		"the refusal must name the offending type so an operator can fix the declaration")
	require.False(t, crossChecked,
		"the refusal must happen before the schema cross-check, i.e. before any DDL")

	for _, db := range []string{pinned.UnsafeDB, pinned.SafeDB, pinned.PromoteDB} {
		var exists uint8
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = ? AND name = ?", db, physical).Scan(&exists))
		require.Zero(t, exists,
			"a rejected declaration must leave no protocol table behind in %s; "+
				"a partially-created set is the permanent brick Spec L §1a describes", db)
	}
}
```

Read `newStorageIntegrityBootDeps`'s real signature before writing the call — the argument list above is copied from the drift test at the commit this plan was written against and may have drifted.

- [ ] **Step 2: Write the non-closing-252 acceptance test**

HouseGate already has this as a docker e2e (`housegate/pkg/integration/storage_backpressure_session_test.go:47`, `TestStorageIntegrity_BackpressureKeepsTheClientSession`). Spec O D6 asks for it **against the embedded proxy**, because that is what production runs. The refusal must originate from the real adapter, so the fake is at the `SNodeRole` seam, not at the HouseGate seam:

```go
// throttlingSNodeRole refuses the first prepare with arbiter-core's real
// hard-limit diagnostic, then behaves normally. The refusal therefore travels
// the production path: snode.ErrBackpressure -> storageintegrityadapter's
// parseSNodeBackpressure -> sicore.BackpressureError -> HouseGate's
// backpressureClientError -> ClickHouse exception 252.
type throttlingSNodeRole struct {
	mu   sync.Mutex
	seen int
	// embed or delegate to whatever minimal SNodeRole the test needs
}

// TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession is Spec L D6's
// acceptance against the embedded proxy: after a back-pressure refusal the
// SAME connection must answer a subsequent SELECT 42.
func TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession(t *testing.T) {
	// ... SENTIO_SI_CH_E2E gate + CH_ADDR, as above ...
	//
	// Build housegate.StorageIntegrityRuntimeOptions the way standalone.go:351
	// does, with SourcePreparer: storageintegrityadapter.NewSourcePreparer(role)
	// where role is the throttling fake. Start the embedded proxy via
	// housegate.New(housegate.Options{...}), then drive the official ClickHouse
	// client through it with a two-statement multiquery, exactly as HouseGate's
	// own e2e does:
	//
	//   "INSERT INTO <db>.<t> FORMAT CSVWithNames; SELECT 42"
	//
	// Assert, in order:
	//   1. the output contains "252" or "TOO_MANY_PARTS"
	//   2. the output contains "back-pressure"
	//   3. the output contains "42"      <- this is the assertion that is red at v0.9.2
	//   4. the fake saw exactly one refused prepare
	//
	// Do NOT reference chproto.ClientError.KeepSession: it does not exist at
	// housegate v0.9.2 and naming it would turn this test's red into a compile
	// error, which is a weaker proof than the behavioural failure.
}
```

The refusal message the fake returns must match arbiter-core's exact format, or `parseSNodeBackpressure` (`storageintegrityadapter/adapter.go:87-124`) falls back to the generic 252 without typed detail and assertion 2 gets weaker:

```go
fmt.Errorf("%w: hg_unsafe.%s partition p_eu has 2950 active parts (hard limit 2950)", snode.ErrBackpressure, physical)
```

- [ ] **Step 3: Run both against the CURRENT pins and record the red**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
# start a Keeper-enabled ClickHouse as .github/workflows/ci.yml's integration job does,
# export CH_ADDR, then:
SENTIO_SI_CH_E2E=1 bazel test //standalone:standalone_test \
  --test_filter='TestStorageIntegrityMalformedColumnTypeCreatesNoTable|TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession' \
  --test_env=SENTIO_SI_CH_E2E --test_env=CH_ADDR \
  --test_arg=-test.v --test_timeout=900 --test_output=all --nocache_test_results 2>&1 | tee /tmp/sn-d6-red.log
grep -c -- '--- SKIP:' /tmp/sn-d6-red.log
```

Expected — and **both must be behavioural failures, not skips and not compile errors**:

- `TestStorageIntegrityMalformedColumnTypeCreatesNoTable` **FAILS** at `require.Error` (arbiter-core `v0.4.0`'s `ddl.Intents` has no column-type validation, so the preflight succeeds) and, if you comment that assertion out to see further, the `system.tables` count is non-zero — the table was created. Either failure is the proof; record which one you got.
- `TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession` **FAILS** on assertion 3 only. Assertions 1 and 2 already pass at `v0.9.2` — `backpressureClientError` and `chproto.CodeTooManyParts` both exist there; what is missing is `KeepSession`, so the relay closes the connection after the Exception and the follow-up `SELECT 42` never runs.
- SKIP count must be **0**.

Paste both failure outputs into the PR body. This is the evidence Spec L §4 asked for and never got.

- [ ] **Step 4: Commit the red tests**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
git add standalone/storage_integrity_acceptance_ch_test.go
git commit -m "test(storage-integrity): add the two Spec L acceptance tests, red against the current pins (Spec O D6)"
```

Committing a knowingly-red docker-gated test is safe: it is `SENTIO_SI_CH_E2E`-gated and CI does not run it until Task 17 adds it to the job. State that in the commit body.

### Task 13: bump all three pins in one commit and fix the two compile breaks

Spec O D5 step 5. The bump is one commit because the three pins are **not independent**: arbiter-core `v0.5.x` `require`s housegate `v0.10.0`, and housegate `v0.11.0`+ `require`s arbiter-proto `v0.6.0`, so MVS drags the others along whichever one you move first. Splitting it would produce an intermediate commit whose pin set nobody chose.

**Files:**
- Modify: `go.mod`, `go.sum`
- Modify: `standalone/standalone.go` (the `snode.Config` literal at `:288-310`; a new schema-source mapping helper next to `storageIntegrityProtocolTablesMode` at `:810`)
- Modify: `standalone/storage_integrity_smoke_test.go` (the `ddl.Intents` call at `:218`)

- [ ] **Step 1: Bump**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
go get github.com/housegate/housegate@v0.12.0
go get github.com/sentioxyz/arbiter-core@<arbiter-core-tag>
go get github.com/sentioxyz/arbiter-proto@v0.6.0
go mod tidy
grep -nE 'housegate|arbiter-core|arbiter-proto|rewriter-go' go.mod
```

Expected: housegate `v0.12.0`, arbiter-core `<arbiter-core-tag>`, arbiter-proto `v0.6.0`, and `rewriter-go` moved from `v0.7.1` to `v0.9.1` as an indirect. arbiter-proto `v0.5.0` → `v0.6.0` is a **no-op**: a rehearsal diffed the exported Go surface and found 1410 identifiers on each side with zero added and zero removed; the only delta is a four-line comment about `statement_kind` being bound by `user_jws` from envelope v2.1. Expect no work from it.

sentio-node's five `replace` directives (`go.mod:144-153`) cover `ch-go`, `clickhouse-go/v2`, `go-ethereum`, `wasmer-go` and `sentio-core`. The two ClickHouse forks are pinned to the **same** versions housegate uses (`sentioxyz/ch-go v0.73.0-sentioxyz-20260629`, `sentioxyz/clickhouse-go/v2 v2.47.0-sentioxyz-20260629`). Re-check that after `go mod tidy` and realign with `go mod edit -replace` if MVS moved the `require` label — never with `go get` on the `ClickHouse/*` path, which only relabels an MVS node and changes nothing that compiles.

- [ ] **Step 2: See the two compile breaks**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
go build ./... ; go vet ./... 2>&1 | grep -v 'unkeyed fields'
```

Expected, verbatim (a rehearsal reproduced both):

```
# compute-network-node/standalone
standalone/standalone.go:302:5: unknown field ProtocolTables in struct literal of type snode.Config, but does have unexported protocolTables
```

```
vet: standalone/storage_integrity_smoke_test.go:218:35: assignment mismatch: 3 variables but ddl.Intents returns 4 values
```

That is the **complete** break set — arbiter-core `v0.4.0` → `v0.5.1` removes exactly four exported items (`ddl.BuildDDL` and `ddl.Intents` arity, `snode.Config.ProtocolTables`, `verifier.Config.ProtocolTables`), and sentio-node does not import `arbiter-core/verifier`, so the fourth is not a break here. Note that `go build ./...` alone finds only the first: the second is test-only and needs `go vet` or `bazel test`.

- [ ] **Step 3: Fix break 2 — `ddl.Intents` gained the `hg_promote` intent**

In `standalone/storage_integrity_smoke_test.go:218`:

```go
unsafeIntent, safeIntent, promoteIntent, err := ddl.Intents(pinned, declared)
```

The loop at `:259` and the verification at `:306-307` currently cover only unsafe and safe. Add `promoteIntent` to both — with `hg_promote` now built and verified in *both* DDL modes (Task 10 Step 3), a smoke test that ignores it is testing two thirds of the protocol-table set. If adding it makes the smoke test fail because the environment has no `hg_promote` tables, that is Task 10 Step 3's finding surfacing, not a new defect: fix the environment, do not drop the assertion.

- [ ] **Step 4: Fix break 1 — map the config string to `ddl.SchemaSource`, preserving `""`**

`snode.Config.ProtocolTables ddl.Mode` is gone; the mode is now **derived** from `SchemaSource ddl.SchemaSource` inside `validate()` via `ddl.ModeFromSchemaSource`, precisely so a deployment cannot silently land on the fail-open `ModeOff` zero (arbiter-core `snode/config.go:33-36`).

The naive fix `SchemaSource: ddl.SchemaSource(si.SNode.SchemaSource)` **is wrong**: a rehearsal proved `snode.New` then fails at startup for `schema_source: ""` with `snode config: ddl: unknown schema source ""`, and `config/config.go:109` explicitly admits `""` while `config_test.go:243` asserts it is valid. Add the normalizing mapper next to `storageIntegrityProtocolTablesMode` in `standalone/standalone.go`:

```go
// storageIntegritySchemaSource maps sentio-node's config string onto
// arbiter-core's typed schema source. It is a mapping, not a derivation: the
// mode itself is derived exactly once, by ddl.ModeFromSchemaSource.
//
// The empty string is sentio-node's documented compatibility spelling of
// "clickhouse" (config/config.go admits it; config_test.go pins it), and
// arbiter-core rejects it, so it is normalized here rather than leaking a
// startup failure into every deployment that omits the field.
// ddl.SchemaSourceUnmanaged is TEST/HARNESS ONLY and maps to the fail-open
// ModeOff; production config validation already rejects it, and this function
// refuses it a second time so a future config change cannot open that door.
func storageIntegritySchemaSource(schemaSource string) (ddl.SchemaSource, error) {
	switch schemaSource {
	case "", "clickhouse":
		return ddl.SchemaSourceClickHouse, nil
	case "network_state":
		return ddl.SchemaSourceNetworkState, nil
	default:
		return "", fmt.Errorf("unsupported storage-integrity schema source %q", schemaSource)
	}
}
```

Then, in the `snode.New(snode.Config{...})` literal (`standalone.go:288-310`), replace `ProtocolTables: protocolTablesMode,` with `SchemaSource: siSchemaSource,`, where `siSchemaSource` comes from a call placed beside the existing `protocolTablesMode` derivation at `:233`:

```go
			protocolTablesMode, err := storageIntegrityProtocolTablesMode(si.SNode.SchemaSource)
			if err != nil {
				return fmt.Errorf("storage integrity protocol-table mode: %w", err)
			}
			siSchemaSource, err := storageIntegritySchemaSource(si.SNode.SchemaSource)
			if err != nil {
				return fmt.Errorf("storage integrity schema source: %w", err)
			}
```

Keep the local `protocolTablesMode` variable: `standalone/storage_integrity_bootstrap_test.go` has an AST guard that pins it as a positional argument (around `:1321`), and Task 14 changes how it is *computed*, not whether it exists.

- [ ] **Step 5: Verify the bump**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
go build ./... && go vet ./... 2>&1 | grep -v 'unkeyed fields'
gofmt -l $(find . -name '*.go' -not -path './bazel-*')
bazel mod tidy && bazel run //:gazelle
bazel build //... && bazel test //... --test_output=errors 2>&1 | tee /tmp/sn-bumped.log
grep -E '(FAILED|TIMEOUT)' /tmp/sn-bumped.log | sort > /tmp/sn-bumped-failures.txt
diff /tmp/sn-baseline-failures.txt /tmp/sn-bumped-failures.txt && echo "no regression vs baseline"
```

`gofmt -l` must print nothing — sentio-node's CI fails the whole build on unformatted files (`.github/workflows/ci.yml`, "Check Go formatting"). Expected otherwise: the same failing set as Task 10 Step 1 (empty). A rehearsal with exactly these two fixes measured 13 packages ok, 0 FAIL, including the AST-guard tests in `storage_integrity_bootstrap_test.go`.

**If the diff is non-empty, stop the chain here.**

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
git add go.mod go.sum standalone/standalone.go standalone/storage_integrity_smoke_test.go
git commit -m "build(deps): bump housegate to v0.12.0, arbiter-core to <arbiter-core-tag>, arbiter-proto to v0.6.0 (Spec O D5)"
```

The message body carries: the two compile breaks and their fixes, the `""` normalization and why it is not cosmetic, the MVS chain that makes one commit correct, and the three migration surveys from Task 10.

### Task 14: D3 — delete the fail-open derivation and re-point the test that pinned it

Spec O §1d and roadmap §4 item 7. `storageintegrityadapter.ProtocolTablesMode` (`adapter.go:39-44`) silently returns `ddl.ModeVerifyOnly` for an unknown source; `standalone.go:810-819` errors on one. PR #179 switched production to the strict one but left the fail-open function in place, and `storage_integrity_bootstrap_test.go:241` still computes its expected value from the fail-open function — so the guard checks a tautology instead of the production predicate.

After Task 13 there are in fact **three** derivations of the same fact: the adapter's fail-open one, `standalone.go`'s strict one, and arbiter-core's `ddl.ModeFromSchemaSource` — which is now authoritative because `snode.Config.validate()` uses it. This task collapses them to one derivation plus one string mapping.

**Files:**
- Modify: `storageintegrityadapter/adapter.go` (delete `ProtocolTablesMode` at `:36-44`, and the now-unused `ddl` import if nothing else in the file uses it)
- Modify: `storageintegrityadapter/adapter_test.go` (delete `TestProtocolTablesModeFollowsSchemaSource` at `:196-199`)
- Modify: `standalone/standalone.go` (`storageIntegrityProtocolTablesMode` at `:810-819` delegates instead of re-deriving)
- Modify: `standalone/storage_integrity_bootstrap_test.go` (line `:241`)

- [ ] **Step 1: Re-point the bootstrap test first, and watch it stay green for the right reason**

In `standalone/storage_integrity_bootstrap_test.go:241`, replace:

```go
				mode := storageintegrityadapter.ProtocolTablesMode(tt.config.SNode.SchemaSource)
```

with:

```go
				mode, err := storageIntegrityProtocolTablesMode(tt.config.SNode.SchemaSource)
				require.NoError(t, err, "every table-driven config must use a supported schema source")
```

Drop the now-unused `storageintegrityadapter` import from that file if nothing else there needs it.

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
bazel test //standalone:standalone_test --test_filter='TestPrepareStorageIntegrityListenerPermitUsesRealConfigPredicate' \
  --test_arg=-test.v --test_output=all --nocache_test_results
```

Expected: green, and all four table rows still exercised — including `"empty source uses clickhouse verify-only compatibility"`, which is the row that proves the `""` normalization from Task 13 Step 4 survived. **The test's name already claims it "uses the real config predicate"; after this step that is true.**

- [ ] **Step 2: Prove the re-pointed test can actually go red**

A re-pointed tautology is still a tautology if the new source is also wrong. Temporarily break the production function — e.g. make `storageIntegrityProtocolTablesMode("network_state")` return `ddl.ModeVerifyOnly` — re-run Step 1's command, confirm the `"network-state create-and-verify"` row **FAILS**, then revert. Record the failure output. Without this step the re-pointing is unproven.

- [ ] **Step 3: Make `standalone.go`'s function delegate instead of re-deriving**

```go
// storageIntegrityProtocolTablesMode resolves the protocol-table DDL mode for
// a configured schema source. There is exactly one derivation in the system —
// arbiter-core's ddl.ModeFromSchemaSource, which snode.Config.validate() also
// uses — so the embedded proxy's preflight and the SNode role can never
// disagree about what mode this deployment runs in. This function only maps
// sentio-node's config string onto the typed source.
func storageIntegrityProtocolTablesMode(schemaSource string) (ddl.Mode, error) {
	source, err := storageIntegritySchemaSource(schemaSource)
	if err != nil {
		return 0, err
	}
	return ddl.ModeFromSchemaSource(source)
}
```

This preserves the existing behaviour for all three admitted strings (`""` and `"clickhouse"` → `ModeVerifyOnly`, `"network_state"` → `ModeCreateAndVerify`) and keeps the error on anything else, so `prepareStorageIntegrityListenerPermit`'s `switch` at `:836-843` is unchanged.

- [ ] **Step 4: Delete the fail-open duplicate**

Delete `ProtocolTablesMode` from `storageintegrityadapter/adapter.go:36-44` and `TestProtocolTablesModeFollowsSchemaSource` from `adapter_test.go:196-199`. Do **not** keep a deprecated alias — roadmap §4 item 7 is explicit that the fail-open one is deleted, not fixed, and Task 11 proved no external consumer exists.

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
grep -rn 'ProtocolTablesMode' --include='*.go' . | grep -v bazel-
```

Expected: only `standalone/standalone.go`'s `storageIntegrityProtocolTablesMode` definition and its call sites at `:233` and `:830`, plus the bootstrap test's new call. **No hit in `storageintegrityadapter/`.**

- [ ] **Step 5: Verify and commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
gofmt -l $(find . -name '*.go' -not -path './bazel-*')
bazel run //:gazelle && bazel build //... && bazel test //... --test_output=errors
git add storageintegrityadapter/adapter.go storageintegrityadapter/adapter_test.go \
        standalone/standalone.go standalone/storage_integrity_bootstrap_test.go
git commit -m "refactor(storage-integrity): delete the fail-open protocol-table mode derivation (Spec O D3)"
```

**If red, stop the chain here.**

### Task 15: D2 — map HouseGate's back-pressure switch onto `DisableHardParts`, and refuse a disagreement

Spec O D2: "sentio-node maps HouseGate's `storage_integrity.backpressure.enabled: false` onto it, and refuses to start if the two disagree."

There is a trap in the defaults. HouseGate's `defaultStorageIntegrityConfig()` sets `Backpressure.HardPartsPerPartition: 2950` **unconditionally** (`housegate/pkg/config/storage_integrity_config.go:183-190`), and its `validate()` only checks the limit fields when `bp.Enabled` (`:290`). So a config with `backpressure.enabled: false` still carries `HardPartsPerPartition == 2950`, and passing that through unchanged would trip arbiter-core's new half-configured-disable validation error from Task 2. The mapping must zero it explicitly.

**Files:**
- Modify: `standalone/standalone.go` (the `snode.Config` literal's `HardPartsPerPartition` line at `:305-306`)
- Modify: `standalone/storage_integrity_bootstrap_test.go` (or the nearest existing standalone unit-test file — add the mapping table there)

- [ ] **Step 1: Write the mapping test (red)**

```go
func TestStorageIntegrityHardPartsMappingFollowsBackpressureEnabled(t *testing.T) {
	for _, tc := range []struct {
		name        string
		enabled     bool
		configured  int
		wantDisable bool
		wantLimit   int
	}{
		{"enabled passes the configured limit through", true, 2950, false, 2950},
		{"enabled with a non-default limit", true, 2000, false, 2000},
		// HouseGate's defaults leave HardPartsPerPartition at 2950 even when
		// back-pressure is disabled, and its validate() does not check the
		// limit fields in that case. Passing it through would trip
		// arbiter-core's half-configured-disable validation error.
		{"disabled zeroes the limit and sets the explicit disable", false, 2950, true, 0},
		{"disabled with an already-zero limit", false, 0, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disable, limit := storageIntegrityHardParts(tc.enabled, tc.configured)
			require.Equal(t, tc.wantDisable, disable)
			require.Equal(t, tc.wantLimit, limit)
		})
	}
}

// The disable must be expressible only one way. Feed the mapping's own output
// into arbiter-core's validator and require it to accept, so the two halves of
// Spec O D2 are proved to agree rather than assumed to.
func TestStorageIntegrityHardPartsMappingSatisfiesSNodeValidation(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		cfg := testSNodeConfigForMappingTest(t)
		cfg.DisableHardParts, cfg.HardPartsPerPartition = storageIntegrityHardParts(enabled, 2950)
		_, err := snode.New(cfg, testSNodeDepsForMappingTest(t))
		require.NoError(t, err, "the mapping must never produce a half-configured disable (enabled=%v)", enabled)
	}
}
```

The second test needs a minimal valid `snode.Config` and `snode.Deps`; build them from whatever sentio-node's existing standalone tests already construct rather than inventing new fixtures. If no such fixture exists, assert against `cfg` shape only (`require.False(t, cfg.DisableHardParts && cfg.HardPartsPerPartition != 0)`) and say in a comment that the full validation is covered by arbiter-core's own Task 2 tests.

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
bazel test //standalone:standalone_test --test_filter='TestStorageIntegrityHardParts' --test_output=all --nocache_test_results
```

Expected: **compile failure** — `undefined: storageIntegrityHardParts`. Record it.

- [ ] **Step 2: Implement the mapping**

In `standalone/standalone.go`, next to the other storage-integrity helpers:

```go
// storageIntegrityHardParts maps HouseGate's back-pressure switch onto
// arbiter-core's source-side hard-parts refusal, so one setting governs both
// halves. Disabling requires saying so twice — the explicit flag AND a zero
// limit — which is what makes a half-configured disable inexpressible
// (Spec O D2). HouseGate's defaults leave the limit at 2950 even when
// back-pressure is off, so zeroing it here is load-bearing, not cosmetic.
func storageIntegrityHardParts(backpressureEnabled bool, configuredLimit int) (disable bool, limit int) {
	if !backpressureEnabled {
		return true, 0
	}
	return false, configuredLimit
}
```

Then, in the `snode.New(snode.Config{...})` literal, replace the `HardPartsPerPartition:` line with both fields:

```go
				DisableHardParts:      siDisableHardParts,
				HardPartsPerPartition: siHardPartsPerPartition,
```

computed just above the literal from `cfg.Housegate.StorageIntegrity.Runtime.Backpressure`:

```go
			bp := cfg.Housegate.StorageIntegrity.Runtime.Backpressure
			siDisableHardParts, siHardPartsPerPartition := storageIntegrityHardParts(bp.Enabled, bp.HardPartsPerPartition)
```

- [ ] **Step 3: Verify and commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
gofmt -l $(find . -name '*.go' -not -path './bazel-*')
bazel build //... && bazel test //... --test_output=errors
git add standalone/standalone.go standalone/storage_integrity_bootstrap_test.go
git commit -m "feat(storage-integrity): map backpressure.enabled onto the SNode hard-parts disable (Spec O D2)"
```

**If red, stop the chain here.**

### Task 16: D4 — map only the pre-write sentinel to terminal-reject

Spec O D4 and §1e. `adapter.go:73` classifies only `ErrSchemaHashMismatch` and `ErrEncodingNotSupported` as terminal; `adapter_test.go:149` asserts `{snode.ErrPayloadMismatch, false}`, so **CI is green because the behaviour is wrong** — the test actively pins the defect.

**The reasoning, which must be in the code comment and the commit message, not just here:** HouseGate's `sicore.ErrPrepareTerminalReject` does not mean "not retryable". It means **provably no write**. `housegate/pkg/storageintegrity/intake.go:1429-1447`'s `abortTerminalPrepareReject` calls `o.abortParts(rec)` and hard-errors with `intake: pre-write terminal prepare reject for %s unexpectedly has %d candidate parts` when the set is non-empty; its own doc comment says "cleans a source rejection that is known to have happened before any unsafe write". So mapping a post-record failure into that class does not merely mislabel it — it converts a convergent retry into a permanently wedged one, because every subsequent attempt re-enters the same hard error. That is why `ErrPayloadMismatchPostRecord` stays non-terminal and goes through the ordinary source-lookup fence at `intake.go:1119-1122`.

**Files:**
- Modify: `storageintegrityadapter/adapter.go` (`:73-77`)
- Modify: `storageintegrityadapter/adapter_test.go` (`TestPrepareErrorClassificationPreservesSentinels`, `:142-160`)

- [ ] **Step 1: Replace the defect-pinning row (red)**

In `adapter_test.go`'s table, **replace** `{snode.ErrPayloadMismatch, false}` — do not extend around it. Spec O D4 is explicit: after Part A no arbiter-core path raises the bare sentinel, so a row asserting its classification would assert dead behaviour. The new table, with the class stated per row:

```go
func TestPrepareErrorClassificationPreservesSentinels(t *testing.T) {
	for _, tc := range []struct {
		injected error
		terminal bool
		why      string
	}{
		{snode.ErrEncodingNotSupported, true, "pre-write: refused before the journal record exists"},
		{snode.ErrSchemaHashMismatch, true, "pre-write: refused before the journal record exists"},
		{snode.ErrPayloadMismatchPreWrite, true, "pre-write: arbiter-core proved no unsafe write can exist"},
		// Post-record must stay NON-terminal. HouseGate's terminal-reject
		// contract is "provably no write": abortTerminalPrepareReject hard-errors
		// when the recovered candidate set is non-empty, so classifying this as
		// terminal would wedge the retry loop instead of converging it.
		{snode.ErrPayloadMismatchPostRecord, false, "post-record: a durable record exists, so a write may have happened"},
		{snode.ErrConvergenceForeignRows, false, "operator intervention; never terminal"},
		{errors.New("dial tcp: connection refused"), false, "transport; the source may have written"},
	} {
		f := &fakeRole{prepErr: tc.injected}
		_, err := NewSourcePreparer(f).PrepareLocalStatement(t.Context(), validEnvelope("0xabc:1:x"), nil)
		require.Error(t, err)
		require.ErrorIs(t, err, tc.injected, "original sentinel must survive: %s", tc.why)
		require.Equal(t, tc.terminal, errors.Is(err, sicore.ErrPrepareTerminalReject),
			"terminal classification for %v (%s)", tc.injected, tc.why)
	}
}

// The split must stay additive for every caller that only knows the old name.
func TestPrepareClassificationKeepsTheBaseSentinelMatchable(t *testing.T) {
	for _, injected := range []error{snode.ErrPayloadMismatchPreWrite, snode.ErrPayloadMismatchPostRecord} {
		f := &fakeRole{prepErr: injected}
		_, err := NewSourcePreparer(f).PrepareLocalStatement(t.Context(), validEnvelope("0xabc:1:x"), nil)
		require.ErrorIs(t, err, snode.ErrPayloadMismatch,
			"%v must still match the base sentinel so existing callers are unaffected", injected)
	}
}
```

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
bazel test //storageintegrityadapter:storageintegrityadapter_test --test_output=all --nocache_test_results
```

Expected: the `ErrPayloadMismatchPreWrite` row **FAILS** with `terminal classification for ... : expected true, got false` — the adapter does not classify it yet. The `ErrPayloadMismatchPostRecord` row passes already (it falls through to the default), which is correct: this task must not change that row's behaviour. Record the failure.

- [ ] **Step 2: Implement**

In `storageintegrityadapter/adapter.go`, replace the `:73-77` block:

```go
		if errors.Is(err, snode.ErrSchemaHashMismatch) ||
			errors.Is(err, snode.ErrEncodingNotSupported) ||
			errors.Is(err, snode.ErrPayloadMismatchPreWrite) {
			// Terminal before any unsafe write: let the orchestrator abort the
			// exact empty candidate set instead of fencing a retry behind a
			// source lookup.
			//
			// ErrPayloadMismatchPostRecord is deliberately NOT here. HouseGate's
			// ErrPrepareTerminalReject means "provably no write", not "not
			// retryable": abortTerminalPrepareReject hard-errors when the
			// recovered candidate set is non-empty, so a post-record failure
			// classified as terminal wedges the retry loop permanently. It goes
			// through the ordinary source-lookup path instead (Spec O D4).
			return sicore.PreparedLocalResult{}, fmt.Errorf("%w: %w", sicore.ErrPrepareTerminalReject, err)
		}
```

Note the ordering constraint: this block must stay **after** the `snode.ErrBackpressure` block at `:57-72`. A back-pressure refusal is also pre-write, but it must reach the client as exception 252, not as a terminal reject.

- [ ] **Step 3: Verify and commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
gofmt -l $(find . -name '*.go' -not -path './bazel-*')
bazel build //... && bazel test //... --test_output=errors
git add storageintegrityadapter/adapter.go storageintegrityadapter/adapter_test.go
git commit -m "fix(storage-integrity): map only the pre-write payload mismatch to terminal reject (Spec K D8b, Spec O D4)"
```

The commit body must carry the reasoning, not just the mapping: terminal-reject is *provably no write*, `abortTerminalPrepareReject` hard-errors on a non-empty candidate set, and "not retryable" is not "did not write".

**If red, stop the chain here.**

### Task 17: D6 — turn the two acceptance tests green and wire them into CI

**Files:**
- Modify: `.github/workflows/ci.yml` (the `integration-clickhouse` job)
- Modify: `standalone/storage_integrity_acceptance_ch_test.go` only if the bump moved an API it uses

- [ ] **Step 1: Re-run the two tests against the bumped pins**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
# same Keeper-enabled ClickHouse and CH_ADDR as Task 12 Step 3
SENTIO_SI_CH_E2E=1 bazel test //standalone:standalone_test \
  --test_filter='TestStorageIntegrityMalformedColumnTypeCreatesNoTable|TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession' \
  --test_env=SENTIO_SI_CH_E2E --test_env=CH_ADDR \
  --test_arg=-test.v --test_timeout=900 --test_output=all --nocache_test_results 2>&1 | tee /tmp/sn-d6-green.log
grep -c -- '--- PASS: TestStorageIntegrity' /tmp/sn-d6-green.log
grep -c -- '--- SKIP:' /tmp/sn-d6-green.log
```

Expected: PASS count 2, SKIP count 0. Put `/tmp/sn-d6-red.log` and `/tmp/sn-d6-green.log` side by side in the PR — that pair is Spec O §4 item 2's "green under docker and red against the old pins".

- [ ] **Step 2: Add both tests to the CI integration job**

In `.github/workflows/ci.yml`'s `integration-clickhouse` job, add a step modelled byte-for-byte on the existing `storage-integrity protocol-table drift` step — same ClickHouse, same env, same three anti-skip guards — for the new filter. Keep the existing drift step untouched:

```yaml
      - name: storage-integrity acceptance (Spec L D1/D6)
        env:
          SENTIO_SI_CH_E2E: "1"
        run: |
          set -euo pipefail
          si_log="$(mktemp)"
          trap 'rm -f "${si_log}"' EXIT
          bazel test //standalone:standalone_test \
            --test_filter='TestStorageIntegrityMalformedColumnTypeCreatesNoTable|TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession' \
            --test_env=SENTIO_SI_CH_E2E \
            --test_env=CH_ADDR \
            --nocache_test_results \
            --test_arg=-test.v \
            --test_timeout=900 \
            --test_output=all 2>&1 | tee "${si_log}"
          if grep -Fq -- '--- SKIP: TestStorageIntegrity' "${si_log}"; then
            echo "storage-integrity acceptance skipped"; exit 1
          fi
          if grep -Fq -- 'no tests to run' "${si_log}"; then
            echo "storage-integrity acceptance filter matched no tests"; exit 1
          fi
          passes="$(grep -c -- '--- PASS: TestStorageIntegrity' "${si_log}" || true)"
          if [ "${passes}" -ne 2 ]; then
            echo "expected 2 storage-integrity acceptance PASS markers, saw ${passes}"; exit 1
          fi
```

The `-ne 2` count assertion is the piece that stops a renamed or deleted test from silently reducing coverage — the same defect Spec P D6 records against the existing pinned filter. Spec P will later widen both filters to a `TestStorageIntegrity.*` prefix with a count assertion; naming the tests with that prefix now means Spec P's edit is a filter change and nothing else.

- [ ] **Step 3: Full local gate, then open the PR**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
gofmt -l $(find . -name '*.go' -not -path './bazel-*')
bazel build //... && bazel test //... --test_output=errors
git add .github/workflows/ci.yml standalone/storage_integrity_acceptance_ch_test.go
git commit -m "ci(storage-integrity): run the two Spec L acceptance tests with anti-skip guards (Spec O D6)"
git push -u origin feat/si-production-rollout
gh pr create --fill --title "feat(storage-integrity): production rollout of Specs I–L (Spec O)"
```

PR body must carry, in this order: the three migration surveys from Task 10; the two compile breaks and their fixes; the D6 red/green log pair; the D4 reasoning; the `<arbiter-core-tag>` and `housegate v0.12.0` versions; and the Bazel baseline diff. Merge only when CI — including the `integration-clickhouse` job with the new step — is green.

**If red, stop the chain here.**

### Task 18: cut sentio-node v0.1.0

**Deviation O-4.** sentio-node has **zero** tags and **no** release-cut workflow: `release-devnet.yml` and `release-testnet.yml` only push docker images, and there is no `scripts/next-version.sh`. Spec O D5 step 5 says "Cut **v0.1.0** — its first tag" without saying how, so the tag is hand-pushed. Two facts make that safe here: nothing derives a version from the tag ledger, so a hand-made tag cannot desynchronise anything; and `ci.yml` triggers on `release: types: [published]`, so publishing the release re-runs the full build + integration suite against the exact tagged tree.

**Files:** none.

- [ ] **Step 1: Confirm `main` is the merged tree and is green**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
git checkout main && git pull --ff-only
git log --oneline -6 | cat
git tag --list | wc -l
bazel build //... && bazel test //... --test_output=errors
```

Expected: the six Part E commits are present, the tag count is `0`, and the suite is green.

- [ ] **Step 2: Push the annotated tag and publish the release**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
git tag -a v0.1.0 -m "sentio-node v0.1.0: first release tag; storage-integrity Specs I-N reachable in production (Spec O)"
git push origin v0.1.0
gh release create v0.1.0 --title v0.1.0 --notes "First release tag. Pins housegate v0.12.0, arbiter-core <arbiter-core-tag>, arbiter-proto v0.6.0. Delivers Spec O D3/D4/D6."
```

Use an **annotated** tag (`-a`), not a lightweight one: it records a tagger date, which is what every sibling repo's version derivation reads, and sentio-node will want the same automation eventually.

- [ ] **Step 3: Verify the release re-ran CI green**

```bash
gh run list --repo sentioxyz/sentio-node --limit 3
git tag --list
```

Expected: a CI run triggered by the `release` event, green. **If it is red, delete the release and the tag (`gh release delete v0.1.0 --yes && git push --delete origin v0.1.0 && git tag -d v0.1.0`) and fix `main` first** — a red first tag is worse than a late one.

---

## Part F — end-to-end acceptance and closure

**Working directory for Task 19:** a deployed sentio-node stack (devnet or a local docker compose of the same shape). **Working directory for Task 20:** `/Users/uranuswch/Dev/housegate/housegate`.

### Task 19: the stack acceptance (Spec O §4 item 4)

Everything before this point proves the pieces. This proves the assembled thing: a real client, against a real sentio-node running the tagged binary, against a real ClickHouse.

**Files:** none. This task produces evidence, not a commit.

- [ ] **Step 0 (precondition, deviation O-5): state which rewriter engine the stack runs**

```bash
# on the deployed stack's config
grep -nA3 '^rewriter:' <the housegate config block> | grep -E 'engine|native_library_release|native_library_path|service_addr'
```

Spec O §4 item 4 does not say which engine backs the acceptance, and sentio-node has **no** FFI references anywhere in the repo — it embeds HouseGate as a library and inherits `rewriter.engine` from the deployed YAML. The refusals below are produced by the *engine*, so the acceptance is meaningless without knowing which one answered:

- `engine: native` — the FFI library must be the **v0.9.1** one. Set `rewriter.native_library_release: v0.9.1` (or `POLYGLOT_SQL_FFI_PATH` to the path Part C Task 6 verified) and record the resolved path in the evidence.
- `engine: grpc` (the default, and the likely production setting) — the Go engine is **not** what runs. The `sql-rewriter` service must be the **rewriter-grpc** release carrying Spec N's C++ half (roadmap §2 lists rewriter-grpc among Spec N's repos; Spec N §5 ships it in the same PR pair as rewriter-go v0.9.1). Record that image tag. **This plan does not pin it** — nothing in Spec O's pin chain does, which is a gap worth reporting.

**If the engine is unstated or the corresponding artifact version is unknown, stop: the acceptance cannot be evaluated.**

- [ ] **Step 1: The two Spec I Critical statements are refused**

```sql
-- both must return an Exception, not a result set
SYSTEM START MERGES hg_unsafe.<physical_table>;
TRUNCATE DATABASE hg_safe;
```

Expected: both refused. Then prove the second one was refused *before* it did anything — `TRUNCATE DATABASE hg_safe` being forwarded verbatim after a target-less engine rejection is Spec I §1.1b's finding, and a refusal message alone does not prove the rows survived:

```sql
SELECT count() FROM hg_safe.<physical_table>;   -- run before and after; must be unchanged
```

- [ ] **Step 2: The Spec N heredoc statements are refused**

Each of these is valid ClickHouse and each blanked the operator guard before Spec N D1 (Spec N §1a):

```sql
SELECT $$--$$ AS x, count() FROM hg_safe.<physical_table>;
SELECT $$#$$ AS x, _hg_row_id FROM hg_unsafe.<physical_table>;
INSERT INTO <ordinary_db>.<ordinary_t> SELECT $$--$$, a FROM hg_safe.<physical_table>;
SELECT $tag$//$tag$ AS x, count() FROM hg_safe.<physical_table>;
```

Expected: all four refused. Also run the two controls from the same table and record their verdicts, so the guard is shown to be discriminating rather than refusing everything:

```sql
SELECT count() FROM hg_safe.<physical_table>;                 -- refused (correct)
SELECT * FROM merge($$hg_safe$$, '^<physical_table>$');       -- refused (correct)
```

- [ ] **Step 3: The happy path still works**

A guard that refuses everything is not a guard. Prove the lane is open:

```sql
-- through the agent-mode proxy, on the signed statement lane:
INSERT INTO <logical_db>.<logical_t> FORMAT CSVWithNames
<a small valid payload>;

-- then both read modes:
SET SQL_x_read_mode = 'safe';           -- or whatever the deployment's default is
SELECT count() FROM <logical_db>.<logical_t>;
SELECT count() FROM <logical_db>.<logical_t> SETTINGS SQL_x_read_mode = 'unsafe_latest';
```

Expected: the INSERT is admitted and reaches ACK2; the `safe` read succeeds; the `unsafe_latest` read succeeds and reflects the just-inserted rows. `unsafe_latest` requires `Options.StorageIntegrityReadState` to be wired — sentio-node does that at `standalone/standalone.go:361-367` — so a failure here is a wiring problem, not a rewriter problem.

- [ ] **Step 4: Record the evidence**

Write the whole transcript (statement, verdict, message) into the Spec O closure record in Task 20. Ten statements, ten verdicts. Anything that does not match the expectation above stops the acceptance.

### Task 20: tag-existence checks and closure records

**Files:**
- Modify: `docs/superpowers/specs/2026-08-25-storage-integrity-production-rollout-design.md` (status + delivery record)
- Modify: `docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md` (§2's Spec O row)

- [ ] **Step 1: Every tag exists and is fetchable (Spec O §4 item 5)**

```bash
gh release view v0.9.1  --repo housegate/rewriter-go     --json tagName --jq .tagName
gh release view v0.12.0 --repo housegate/housegate       --json tagName --jq .tagName
gh release view <arbiter-core-tag> --repo sentioxyz/arbiter-core --json tagName --jq .tagName
gh release view v0.1.0  --repo sentioxyz/sentio-node --json tagName --jq .tagName
```

Then prove each resolves as a Go module, not just as a GitHub object:

```bash
cd $(mktemp -d) && go mod init tagcheck >/dev/null
GOFLAGS=-mod=mod go get github.com/housegate/rewriter-go@v0.9.1
GOFLAGS=-mod=mod go get github.com/housegate/housegate@v0.12.0
GOFLAGS=-mod=mod go get github.com/sentioxyz/arbiter-core@<arbiter-core-tag>
grep -E 'rewriter-go|housegate|arbiter-core' go.mod
```

sentio-node's module path is the bare `compute-network-node`, so it has no `go get` form — say so rather than reporting a failure.

- [ ] **Step 2: Close Spec O**

In `docs/superpowers/specs/2026-08-25-storage-integrity-production-rollout-design.md`, change `**Status:** Proposed` to `**Status:** Implemented` and append a delivery record to §4 naming every tag and the acceptance result:

```markdown
**Delivered:** rewriter-go `v0.9.1`, housegate `v0.12.0` (PR #141 merged), arbiter-core `<arbiter-core-tag>`, arbiter `<arbiter-pr>`, sentio-node `v0.1.0`. Stack acceptance run `<date>` against `<engine>` `<engine-version>`: ten statements, ten expected verdicts. Deviations from this design as written, all recorded in the plan: the housegate pin bump landed on PR #141's branch rather than `main` (O-1); the payload-mismatch class is a parameter of `validatePrepareBindings` rather than a property of a line, because that function has one pre-write and one post-record caller (O-2); arbiter-core's version is `<arbiter-core-tag>` rather than v0.6.0, because `scripts/next-version.sh` derives it from the previous tag's UTC day (O-3); sentio-node's first tag was hand-pushed because the repo has no release automation (O-4); the acceptance's rewriter engine is stated as an explicit precondition (O-5); and the two acceptance tests keep sentio-node's env gate with per-test anti-skip guards, matching Spec P D6 rather than Spec O D6's wording (O-6).
```

Write it as one line, no hard wrapping.

Also record the three migration findings that Spec O did not predict, in §5 "Out of scope / recorded debt":

```markdown
**Discovered during rollout, not predicted by this design.** Three arbiter-core v0.4.0 → v0.5.x changes compile clean but change production behaviour: (a) `snode.Config.validate` and `ddl.Intents` now enforce the storage-integrity column-type whitelist, so any declared SI table using `DateTime`, `DateTime64`, `Date`, `Decimal`, `UUID`, `Nullable`, `Array`, `LowCardinality`, `Enum8`, `Int128`, `UInt256`, `Tuple` or `Map` refuses startup — this is Spec L D1 working as designed and is a planned migration, not a defect; (b) `hg_promote` is no longer auto-created and is now verified against the pinned DDL in **both** modes, so under `schema_source: clickhouse` (`ModeVerifyOnly`) the promote tables must pre-exist, and sentio-node has no handling for the new `snode.ErrPromoteTableMissing`; (c) `schema_source: ""` is admitted by sentio-node's config and rejected by `ddl.ModeFromSchemaSource`, so the mapping normalizes it to `clickhouse`. Also recorded: sentio-node's module path is the non-resolvable bare `compute-network-node`, and no plan in this set pins the rewriter-grpc service image that a `rewriter.engine: grpc` deployment actually runs.
```

- [ ] **Step 3: Update the roadmap**

In `docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md` §2, change Spec O's urgency cell from `**Blocker** — nothing shipped so far is reachable in production without it` to `**Shipped** — rewriter-go v0.9.1 / housegate v0.12.0 / arbiter-core <arbiter-core-tag> / sentio-node v0.1.0`.

Leave §3's diagram alone: it is the order, and the order did not change. Leave Specs I / K / L's status lines alone — Spec P D7 owns that bookkeeping and this plan must not race it.

- [ ] **Step 4: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add docs/superpowers/specs/2026-08-25-storage-integrity-production-rollout-design.md \
        docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md
git commit -m "docs(storage-integrity): close Spec O and record the rollout deviations"
```

---

## Self-review

Run after the plan is written, before execution.

**1. Spec coverage** — see the map below; every Spec O decision and every §4 acceptance item has at least one task.

**2. Placeholder scan** — no "TBD", no "add error handling", no "similar to Task N". Two kinds of intentional placeholder remain:

- **Release artifacts that cannot be known before execution:** `<arbiter-core-tag>` (produced by Task 3 Step 2, which prints it), `<arbiter-pr>` (Task 9), `<engine>` / `<engine-version>` (Task 19 Step 0), and `<physical_table>` / `<logical_db>` / `<ordinary_db>` in Task 19's SQL, which name whatever the deployed stack actually declares.
- **Deliberate hand-offs to the reading of live source**, each with the exact file and symbol to read named in the step: `newStorageIntegrityBootDeps`'s current argument list (Task 12 Step 1), the existing hard-limit test's setup in `snode/staged_backpressure_test.go` (Task 2 Step 2), the minimal `snode.Config`/`snode.Deps` fixtures (Task 15 Step 1), and the Spec N regression-test name (Task 6 Step 4). Three test bodies ship with an explicit `t.Fatal("implement per Step N; this placeholder must be removed")` so an unimplemented one cannot pass silently.

**3. Type and name consistency**
- `snode.ErrPayloadMismatchPreWrite` (Task 1) ↔ the `adapter.go` classification (Task 16 Step 2) ↔ the `adapter_test.go` table row (Task 16 Step 1) — same identifier, three repos' worth of distance apart.
- `snode.ErrPayloadMismatchPostRecord` (Task 1) is consumed only by negative assertions (Task 16) and by `recordedBindingMutations()` (Task 1 Step 5). Nothing maps it to terminal-reject; that is the point.
- `snode.Config.DisableHardParts` (Task 2) ↔ `storageIntegrityHardParts` (Task 15 Step 2) ↔ the `snode.Config` literal in `standalone.go` (Task 15 Step 2).
- `storageIntegritySchemaSource` (Task 13 Step 4) is consumed by `storageIntegrityProtocolTablesMode` (Task 14 Step 3) and by the `snode.Config` literal (Task 13 Step 4) — one mapping, two consumers, so the preflight and the SNode role cannot disagree.
- `TestStorageIntegrityMalformedColumnTypeCreatesNoTable` / `TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession` (Task 12) ↔ the CI `--test_filter` and the `-ne 2` PASS count (Task 17 Step 2). Renaming either test without editing the workflow makes CI red, which is the intended coupling.
- `v0.9.1` appears in exactly five housegate files after Task 5 and is asserted by Step 4's paired grep.

**4. Red-proof coverage** — every new guard has a step proving it fails against the unfixed code:

| Guard | Red proof | Kind |
|---|---|---|
| `ErrPayloadMismatchPreWrite` / `PostRecord` exist and are disjoint | Task 1 Step 2 | compile error |
| No raise site uses the bare sentinel | Task 1 Step 2 (same run) | compile error, then assertion |
| Each raise site's class | Task 1 Step 2 | compile error, then assertion |
| `DisableHardParts` half-configured rejection | Task 2 Step 3 | compile error |
| `DisableHardParts` actually skips the refusal | Task 2 Step 3 | compile error, then assertion |
| The v0.9.1 engine closes Spec N's holes | Task 6 Step 4 | behavioural: same test red on v0.9.0 |
| Malformed column type creates no table | Task 12 Step 3 | behavioural: red on arbiter-core v0.4.0 |
| A 252 leaves the session usable | Task 12 Step 3 | behavioural: red on housegate v0.9.2 |
| The bootstrap test checks the production predicate | Task 14 Step 2 | behavioural: deliberate break, confirm red, revert |
| Pre-write maps to terminal-reject | Task 16 Step 1 | behavioural: red row in the table |
| Post-record does **not** map to terminal-reject | — | passes before and after by construction; the guard is that the row exists and Task 1's split makes it reachable |

**5. Stop conditions** — every version-bump task and every acceptance task ends with an explicit "if red, stop the chain here": Tasks 1, 2, 3, 4, 6, 7, 8, 9, 13, 14, 15, 16, 17, 18, 19.

## Spec coverage map

| Spec O section | Requirement | Tasks |
|---|---|---|
| §1a | housegate pins the pre-Spec-I engine | 5, 6 |
| §1b | Spec I's HouseGate half is unmerged | 7 |
| §1c | sentio-node consumes none of it; the `ProtocolTables` break | 10, 13 |
| §1d | two disagreeing mode derivations; the test pins the wrong one | 14 |
| §1e | K D8b unimplemented and its prescription unsound | 1, 16 |
| §1f | Spec L D7's "explicit disable" cannot be expressed | 2, 15 |
| §3 D1 | split `ErrPayloadMismatch`, both still matching the original | 1 |
| §3 D1 | the raise-site classification test | 1 (Steps 1, 5) |
| §3 D2 | `DisableHardParts` + validation error on a half-configured disable | 2 |
| §3 D2 | sentio-node maps `backpressure.enabled` onto it and refuses a disagreement | 15 |
| §3 D3 | delete `ProtocolTablesMode`; re-point the bootstrap test | 11, 14 |
| §3 D4 | pre-write → terminal-reject; post-record stays non-terminal; the row is replaced | 16 |
| §3 D5 step 1 | rewriter-go v0.9.1 | 4 (consumed; Spec N produces it) |
| §3 D5 step 2 | go.mod + CI FFI tag in one commit; merge #141; cut v0.12.0 | 5, 6, 7, 8 |
| §3 D5 step 3 | arbiter-core D1 + D2, cut a tag | 1, 2, 3 |
| §3 D5 step 4 | arbiter bumps the housegate pin | 9 |
| §3 D5 step 5 | sentio-node's three pins in one commit; compile break; D3; D4; first tag | 13, 14, 16, 18 |
| §3 D6 | type rejection creates no table | 12, 17 |
| §3 D6 | a 252 refusal leaves the connection usable, against the embedded proxy | 12, 17 |
| §3 D6 | both red against the old pins | 12 (Step 3) |
| §4 item 1 | arbiter-core: classification test, config test, full suite | 1, 2 |
| §4 item 2 | sentio-node: builds, no dangling `ProtocolTablesMode`, re-pointed expectation, D4 table, D6 red/green | 11, 13, 14, 16, 17 |
| §4 item 3 | housegate green with the v0.9.1 library **actually fetched** | 6 |
| §4 item 4 | stack end-to-end: the two Criticals, the Spec N heredocs, INSERT + `unsafe_latest` | 19 |
| §4 item 5 | every tag exists and is fetchable | 3, 4, 8, 18, 20 |
| roadmap §4 item 5 | the split is additive for every existing caller | 1 (Step 1), 16 (Step 1) |
| roadmap §4 item 7 | the fail-open derivation is deleted, not fixed | 14 |
| roadmap §4 item 9 | every guard ships with a red proof | self-review table 4 |
| §5 | recorded debt | 20 (Step 2) |
