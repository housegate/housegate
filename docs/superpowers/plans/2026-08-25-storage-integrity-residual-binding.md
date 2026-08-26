# Storage-Integrity Residual Binding, Verification and Bookkeeping Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the seven residual findings the 2026-08-25 verification review left against Specs I/J/K/L — give the consensus **dispatch** path the same block-completeness proof the audit path already has so a corrupted reader cannot produce signed fraud evidence against an honest source (D1); stop the exact part-name inventory read from scanning the safe database, which moved Spec L D3(b)'s growth cliff to startup instead of closing it (D4); turn the accidental `SET` refusal into a recorded, corpus-pinned, end-to-end-tested contract (D2); prove the `SQL_x_read_mode` owned-key decision end to end on an ingress-configured deployment (D3); delete the last golden regenerator (D5); make three CI configurations stop reporting skips as passes (D6); and correct the plan/spec records that currently say the opposite of the truth (D7).

**Architecture:** Five independent lanes, ordered by integrity weight, not by convenience. (1) **arbiter FSM** gains one completeness-checked sibling to `blockStatements` and adopts it at exactly the call sites that derive a root or a monotone watermark — one caller at a time, each with its own reason, never a blanket replace; the pure side-effect iterators inside `apply.go` keep the unchecked helper and gain a doc comment that forces the next caller to choose deliberately (D1). The one remaining `-update` regenerator is deleted so `L3BlockHeader.ChainHash()`'s spent-IDs dependency can no longer be re-blessed by a flag (D5). (2) **housegate** splits the exact-name inventory scope from the bounded aggregate count scope so exact reads bind `hg_unsafe` only, after a task-gating enumeration proves no exact-name consumer reads a safe-database key (D4); adds the `SET` and read-mode end-to-end proofs (D2, D3). (3) **the two engine repos** carry one new byte-identical corpus case for `SET` (D2). (4) **three CI files** gain a count-and-skip assertion so a filter that matches fewer tests than expected, or a docker acceptance that self-skips, goes red (D6). (5) **bookkeeping** reconciles three plans' checkboxes against merged code, statement by statement, and is explicitly last (D7).

**Tech Stack:** Go 1.25 + Bazel 9 + gazelle (arbiter, arbiter-core, housegate, sentio-node), Go 1.25 + polyglot FFI via PureGo (rewriter-go), C++23 + ClickHouse parser + gtest built on the remote box (rewriter-grpc), ClickHouse 25.8 in docker for every integration proof, GitHub Actions on self-hosted `linux/x64` runners.

**Spec:** `docs/superpowers/specs/2026-08-25-storage-integrity-residual-binding-design.md` (Spec P). Roadmap: `docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md` — §4 decisions 8 and 9 bind this plan. Remediates residual findings against Spec I (`2026-08-19-storage-integrity-surface-failclosed-design.md`), Spec J (`…-verification-restoration-design.md`), Spec K (`…-commitment-durability-design.md`) and Spec L (`…-table-backpressure-hardening-design.md`).

## Global Constraints

Every task's requirements implicitly include this section.

- **Every new guard ships with a step proving it fails against the unfixed code** (roadmap §4 decision 9). D1 (Tasks 2, 3, 4), D3 (Task 10) and D4 (Task 8) each have a concrete named pre-fix failure. D2's and D5's are structural — a corpus case that does not exist and a flag that does — and each still gets an explicit red-then-green step.
- **Do not blanket-replace `blockStatements`** (roadmap §4 decision 8). Task 1 classifies all eleven call sites; Tasks 2–4 move exactly the four that derive a root, a verdict or a monotone watermark, one per step, each naming its reason. `apply.go`'s five `forEachBlockStatement` side-effect iterators and `reads_custody.go`'s payload-ref inventory deliberately keep the unchecked helper.
- **Refusing to dispatch is the chosen fix, not the best long-term fix.** `statement_count` / `statements_root` on `replay.ReplayJob` is the durable answer and is an arbiter-proto wire change across every verifier; it stays recorded debt (Spec P §5). No task in this plan touches `replay.ReplayJob`, arbiter-proto, or `pkg/replay`.
- **Corpus is one file, byte-identical in two repos:** `rewriter-go/internal/harness/testdata/storage_integrity_cases.json` and `rewriter-grpc/tests/testdata/storage_integrity_cases.json`. Pre-change state measured 2026-08-25: **210 cases**, sha256 `309d738050fd05edd8e1f51f59a071c9c593e7aae10ea79dc7d4781c708ce281`, identical in both repos. The Go copy is authoritative during authoring; the C++ copy is produced by `cp`, never hand-edited.
- **Spec N's plan also edits this corpus.** N and P run in parallel (roadmap §3) and both append cases. **Whichever lands second rebases its cases onto the other's** — re-run the authoring step against the then-current file, re-measure the count and sha256, and update the `require_test_count 'SpecG/StorageIntegrityGolden.*' <N>` line in `rewriter-grpc/.github/workflows/ci.yml:1381` to the new total. Do **not** hard-code an expected post-change count anywhere in this plan's steps: every step that needs one reads it from `python3 -c "import json;print(len(json.load(open(...))))"` at execution time. The only invariant this plan asserts is *"exactly one more case than the file had before this task"*.
- **Spec P's D2 HouseGate integration test depends on Spec O.** housegate `main` pins `rewriter-go v0.7.1` (`go.mod:108`) and has no `pkg/rewriter/probe.go`, so Spec I's HouseGate half (the fail-closed-on-any-non-`Success` posture) is still in open PR #141 and the engine that refuses `SET` under an active SI contract (rewriter-go ≥ v0.9.0) is not pinned. Task 9 is therefore **gated**: either base its branch on PR #141's branch and pass a v0.9.x FFI lib explicitly, or execute it after Spec O's HouseGate pin+merge task. Task 9 Step 0 states the gate and how to check it. Every other task in this plan is independent of N and O.
- **arbiter / arbiter-core / housegate / sentio-node are Bazel + gazelle.** After adding or renaming files run `bazel mod tidy && bazel run //:gazelle` in that repo. Bazel is the test ground truth; a plain `go test` result is not evidence.
- **rewriter-grpc builds only on the remote box** (`ssh -p 30100 sentio@64.38.131.242`, workdir `/home/sentio/chen/rewriter-grpc/`). Dev loop = rsync → `./scripts.sh rebuild` → `ctest --test-dir build --output-on-failure`. Single test: `./build/rewriter_tests --gtest_filter='<Suite.Name>'`. **Never run a local cmake.**
- **English only** for identifiers, comments, log messages and operator-facing error strings, in all five repos.
- **Markdown docs: no hard line-wrapping.** One paragraph per line, in every document this plan edits.
- **One commit per task**, conventional-commit prefix, in the repo that task names. Never `git add -A`; always `git add` the exact paths the task lists. Every task ends with its named verification command green.
- **Baseline rule for judging failures:** before calling any test failure a regression, diff the failing set against a clean `main` build of that repo. A failure that exists on `main` is not a regression but must not grow.

## Corrections to Spec P discovered while planning

Spec P was written against a reading of the source that is wrong in five places. Each is corrected below and the affected task implements the correction, not the spec's literal text.

1. **D1 misses two root-deriving callers.** Spec P names `BlockDispatchInfo` and `reevaluateBlock`. `fsm/reads_work.go:118` also derives `blockStateRoot(stmts)` — into `WorkSet.UnanchoredVerified[].StateRoot`, which is what the orchestrator **anchors on L2**; and `fsm/reads_work.go:46`'s `safePrefixLocked` silently treats a missing statement as "not blocking safety", so a short list can advance the safe prefix, the promotion frontier and the manifest-debt flag. Both are handled in Task 4.
2. **D4's file:line attribution is wrong.** `fullScope()` at `parts_pressure.go:324` has three callers: `Refresh` (`:337`), `RefreshCounts` (`:367`) and `RestoreBatch` (`:678`). `:367` is the **bounded aggregate** path, where including the safe database is deliberate and cheap (a `count()` GROUP BY bounded by tables × partitions) and is required by the `storage_integrity_safe_parts` gauge. `PartsPressureGuard.Refresh` (`:334`) has **no production caller** — the supervisor's own `Refresh` calls `RefreshCounts` + `RefreshLiveKeys` (`storage_integrity_backpressure.go:78-84`). The single production exact-name unbounded read is therefore `RestoreBatch` alone, i.e. the startup recovery boundary. The cliff is real; its blast radius is startup recovery, not steady-state polling.
3. **D4 cannot simply make `fullScope()` unsafe-only.** `RefreshCounts` passes `g.fullScope()` to `applyCountScopeLocked` / `reconcileScopeLocked` as the scope its counts are authoritative for, and `PartsScope.IsFull` (`parts_pressure_scope.go:60`) requires the safe database to be named before it will report "full" — which is what sets `lastFullOK` (gating `Snapshot()`), what selects `refreshGate.Lock()` over `RLock()` in `refreshScope`, and what `RestoreBatch` depends on. Task 7 therefore splits the constructor in two and relaxes `IsFull` to the unsafe surface, rather than mutating `fullScope()` in place.
4. **D3's premise is inverted.** `SQL_x_read_mode` is a *member* of `housegateOwnedSettingKeys` (`pkg/storageintegrity/settings.go:42`), so `RejectUserSettings` **accepts** it; and `RejectUserSettings` runs only on the signed-lane INSERT admission (`pkg/plugins/storageintegrity/plugin.go:239`, `pkg/plugins/sistatement/plugin.go:143`) — a read query never reaches it (`storageIntegrityKindFromSQL`'s `readLikePattern` returns not-handled). There is no "rejection path that stops a client from choosing its own read mode". The real untested property is the one Spec K D6 actually created: on an ingress-configured deployment a signed INSERT carrying `SQL_x_read_mode` must be **admitted** while any non-owned key must be **refused**. Task 10 proves that, in both directions, with a removal check for each.
5. **D7's "Spec L plan Task 14b" is Spec J's plan.** `Task 14b` exists only in `plans/2026-08-19-storage-integrity-verification-restoration.md:3100` (Spec J), which is the plan that is fully ticked (116/116). Spec L's plan has 164 unticked boxes and no Task 14b. Task 19 corrects Spec J's plan and records the mis-citation in Spec P.

## File map

| Repo | Create | Modify |
|---|---|---|
| arbiter | — | `fsm/apply.go`, `fsm/reads.go`, `fsm/reads_dispatch.go`, `fsm/reads_dispatch_test.go`, `fsm/threeway.go`, `fsm/threeway_test.go`, `fsm/reads_work.go`, `fsm/reads_work_test.go`, `fsm/accumulator/…` → `accumulator/vectors_test.go`, `integration/chpipeline/BUILD.bazel`, `.github/workflows/ci.yml` |
| arbiter-core | — | `.github/workflows/ci.yml` |
| housegate | — | `pkg/storageintegrity/parts_pressure.go`, `pkg/storageintegrity/parts_pressure_scope.go`, `pkg/storageintegrity/parts_pressure_test.go`, `pkg/storageintegrity/parts_pressure_scope_test.go`, `pkg/integration/storage_backpressure_test.go`, `pkg/integration/storage_backpressure_bounded_test.go`, `pkg/integration/storage_integrity_read_test.go`, `pkg/integration/storage_integrity_agent_test.go`, `pkg/integration/BUILD.bazel`, `CLAUDE.md` |
| rewriter-go | — | `internal/harness/testdata/storage_integrity_cases.json` |
| rewriter-grpc | — | `tests/testdata/storage_integrity_cases.json`, `.github/workflows/ci.yml` |
| sentio-node | — | `standalone/storage_integrity_drift_ch_test.go`, `.github/workflows/ci.yml`, `scripts/ci/require-ch-tests.sh` (new script — the one exception to "Create: —") |
| docs (housegate) | — | `specs/2026-08-19-storage-integrity-surface-failclosed-design.md`, `specs/2026-08-19-storage-integrity-commitment-durability-design.md`, `specs/2026-08-19-storage-integrity-table-backpressure-hardening-design.md`, `specs/2026-08-18-storage-integrity-signed-envelope-v2-design.md`, `specs/2026-08-18-storage-integrity-design-v4-reconciliation.md`, `specs/2026-08-25-storage-integrity-residual-binding-design.md`, `plans/2026-08-19-storage-integrity-surface-failclosed.md`, `plans/2026-08-19-storage-integrity-commitment-durability.md`, `plans/2026-08-19-storage-integrity-table-backpressure-hardening.md`, `plans/2026-08-19-storage-integrity-verification-restoration.md` |

---

## Part A — arbiter (the integrity-relevant lane)

**Working directory for every Part A task:** `/Users/uranuswch/Dev/sentio_xyz/arbiter`

- [ ] **Task 0 (pre-flight, do once, all repos):** create the branches and record the baselines every later task compares against. *(Part A round: the arbiter row only — branch `fix/si-dispatch-completeness` rather than the `fix/si-residual-binding` this plan names, and a 16/16-green `bazel test //...` baseline with an empty failing set. The other four repos' branches and baselines belong to the Part B–E rounds.)*

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter        && git checkout -b fix/si-residual-binding && bazel test //... --test_output=errors
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core   && git checkout -b fix/si-ci-honesty       && bazel test //... --test_output=errors
cd /Users/uranuswch/Dev/housegate/housegate       && git checkout -b fix/si-residual-binding && bazel test //... --test_output=errors
cd /Users/uranuswch/Dev/housegate/rewriter-go     && git checkout -b feat/si-set-corpus      && env -u REWRITER_ORACLE_ADDR make test
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node    && git checkout -b fix/si-ci-honesty       && bazel test //... --test_output=errors
python3 -c "import json;print(len(json.load(open('/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json'))))"
shasum -a 256 /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json \
              /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json
```

Record each repo's failing-target set (expected: empty) in the PR body. Record the corpus count and both sha256s — they must be equal to each other; at the time of writing they are 210 and `309d738050fd05edd8e1f51f59a071c9c593e7aae10ea79dc7d4781c708ce281`, but Spec N may have moved them (see Global Constraints).

### Task 1: D1 — classify every `blockStatements` caller and add the completeness-checked sibling

This is the whole safety property of Part A. It lands first and alone, and it deliberately changes **no** caller: Tasks 2–4 move exactly four of them, one step each.

**Files:**
- Modify: `fsm/apply.go` (`blockStatements` at `:86-100`, `forEachBlockStatement` at `:102-106`)
- Test: `fsm/seal_test.go` (next to the existing `TestL3BlockViewRejectsIncompleteBlock`, which is Spec K D3's mirror on the read path)

**Interfaces:**
- Produces: `(*FSM).blockStatementsComplete(blockSeq uint64) ([]*StatementState, bool)` — package-private, consumed by Tasks 2, 3 and 4.

- [x] **Step 1: Confirm the caller inventory is still exactly these eleven sites**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
grep -rn "forEachBlockStatement\|blockStatements\|blockStateRoot" --include="*.go" .
```

Expected — and if the set differs, classify the new entry before writing any code:

| # | Call site | What it derives | Decision | Reason |
|---|---|---|---|---|
| 1 | `apply.go:189` `applyMarkReplaying` | nothing — flips `Sequenced`/`UnsafeRegistered` → `Replaying` on statements that exist | **keep unchecked** | Pure side effect on present records. A statement the state no longer retains has no status to advance; refusing the whole command would wedge the pipeline on a block that is already corrupt without making anything safer. |
| 2 | `apply.go:348` `applyRecordAnchorFinal` | nothing — flips `QuorumVerified`/`FinalityWait` → `FinalityWait`/`Promotable` | **keep unchecked** | Same. The anchor content itself is not derived from this list; it comes from `c.Anchor`. |
| 3 | `apply.go:580` `applyOpenChallenge` | nothing — flips non-terminal statuses → `ChallengeReplay` | **keep unchecked** | Same, and a challenge is already the conservative direction. |
| 4 | `apply.go:594` `applyResolveChallenge` (Safe verdict) | nothing — status transition | **keep unchecked** | Same. |
| 5 | `apply.go:612` `applyResolveChallenge` (Rejected verdict) | nothing — status transition | **keep unchecked** | Same. |
| 6 | `reads_custody.go:28` `CustodyWorkSet` | payload `Refs` + `StatementSeqs` for custody pin/release | **keep unchecked** | An absent statement's `Env.PayloadRef` is unrecoverable from state, so a checked variant could only refuse the row — which would stall custody progression for every *other* block's payload too. The destructive half of custody (`custodyAuditStep`) is gated on `work.Safe`, which Task 4's `safePrefixLocked` fix already stalls for an incomplete block. Record this reasoning in the code comment. |
| 7 | `reads_dispatch.go:37` `BlockDispatchInfo` | `SourceClaimRoot = blockStateRoot(stmts)` **and** the `info.Statements` list shipped to verifiers | **→ checked (Task 2)** | Spec P §1a. A short list makes the verifier replay fewer statements than the source executed, mismatch, and — per the replay design's C.4 rule that a source-root mismatch is signed rather than errored — sign fraud evidence against an honest source. |
| 8 | `threeway.go:46` `reevaluateBlock` | the three-way verdict: `lastRC.SourceClaimRoot` (check 1), the `claimed` partition sums (check 2), `check3(stmts, scan)` | **→ checked (Task 3)** | A short list drops a statement's RC from checks 2 and 3 and can move which RC is `lastRC` for check 1. Its existing `ss.RC == nil` evaluability bail-out is the exact shape to mirror. |
| 9 | `reads_work.go:46` `safePrefixLocked` | the safe prefix → promotion frontier, manifest debt, `CustodyWork.Safe` | **→ checked (Task 4)** | Not named by Spec P. A missing statement is silently skipped, so `allSafe` stays true and the prefix advances over a block with an unaccounted statement. Conservative fix: stall. |
| 10 | `reads_work.go:73` → `:118` `WorkSet` | `UnanchoredVerified[].StateRoot = blockStateRoot(stmts)` — **the root the orchestrator anchors on L2** | **→ checked (Task 4)** | Not named by Spec P, and the highest-consequence of the four: quorum latches (`reevaluateBlock` returns early once `Verdict.Quorum`), so Task 3's fix does not protect a block corrupted *after* quorum. `WorkSet` already returns `(WorkSet, error)` and already errors on a `ChainHash()` failure two lines above, so an error return is the in-idiom refusal. |
| 11 | `reads_custody_test.go:118-119` | test-local expectation | **keep unchecked** | Test fixture that deliberately mirrors production's current derivation; not a production path. |

- [x] **Step 2: Write the failing test**

Append to `fsm/seal_test.go`:

```go
// TestBlockStatementsCompleteRejectsMissingStatement is the dispatch-path twin
// of TestL3BlockViewRejectsIncompleteBlock (Spec K D3). Both synthesize the
// same latent snapshot/state corruption; the audit read already refuses it,
// and Spec P D1 gives the dispatch read the same refusal.
func TestBlockStatementsCompleteRejectsMissingStatement(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	sealBlock(t, f, 2)

	if stmts, ok := f.blockStatementsComplete(1); !ok || len(stmts) != 2 {
		t.Fatalf("a complete sealed block must resolve: ok=%v n=%d", ok, len(stmts))
	}
	if _, ok := f.blockStatementsComplete(99); ok {
		t.Fatal("an out-of-range block seq must not resolve")
	}

	f.st.Statements[2] = nil // a retained-but-nil record is corruption too
	if _, ok := f.blockStatementsComplete(1); ok {
		t.Fatal("a nil retained statement must not resolve")
	}

	delete(f.st.Statements, 2)
	if _, ok := f.blockStatementsComplete(1); ok {
		t.Fatal("blockStatementsComplete must refuse a short statement list")
	}
	if got := len(f.blockStatements(1)); got != 1 {
		t.Fatalf("the unchecked helper still under-reports (%d of 2) — that is why the checked sibling exists", got)
	}
}
```

- [x] **Step 3: Run it to verify it fails**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel test //fsm:fsm_test --test_filter='TestBlockStatementsCompleteRejectsMissingStatement' \
  --test_output=all --nocache_test_results
```

Expected: FAIL — `f.blockStatementsComplete undefined`.

- [x] **Step 4: Add the checked sibling and document the distinction**

In `fsm/apply.go`, replace the doc comment on `blockStatements` and add the sibling immediately below it:

```go
// blockStatements returns the block's statements in statement_seq order,
// SILENTLY DROPPING any seq the state no longer retains. Use it only where the
// caller iterates for side effects on statements that exist and derives nothing
// from the list — no root, no verdict, no watermark, nothing keyed on its
// length or its last element. Every other caller must use
// blockStatementsComplete: a short list yields a root computed over fewer
// statements than the source executed, which a verifier reads as fraud
// (Spec P D1).
func (f *FSM) blockStatements(blockSeq uint64) []*StatementState {
	// ... unchanged body ...
}

// blockStatementsComplete returns the block's statements in statement_seq
// order, or ok=false when the header's StatementCount disagrees with the
// retained records or any retained record is nil. It is the dispatch-path
// mirror of L3BlockView's ErrL3BlockIncomplete assertion (Spec K D3): an
// incomplete block is local corruption, and refusing to act on it locally is
// correct, cheap, and crosses no protocol boundary. The durable fix — carrying
// statement_count / statements_root in replay.ReplayJob so the verifier can
// detect a short list itself — is an arbiter-proto wire change and is recorded
// debt (Spec P §5).
func (f *FSM) blockStatementsComplete(blockSeq uint64) ([]*StatementState, bool) {
	idx := int(blockSeq) - 1
	if idx < 0 || idx >= len(f.st.Blocks) {
		return nil, false
	}
	h := f.st.Blocks[idx]
	out := make([]*StatementState, 0, h.StatementCount)
	for seq := h.StatementSeqStart; seq < h.StatementSeqStart+uint64(h.StatementCount); seq++ {
		ss, ok := f.st.Statements[seq]
		if !ok || ss == nil {
			return nil, false
		}
		out = append(out, ss)
	}
	if uint32(len(out)) != h.StatementCount {
		return nil, false
	}
	return out, true
}
```

Also extend `forEachBlockStatement`'s doc so the helper it wraps is not mistaken for a general-purpose iterator:

```go
// forEachBlockStatement iterates the block's RETAINED statements for side
// effects only (see blockStatements). It is not a completeness-checked read.
```

- [x] **Step 5: Re-run the test**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel test //fsm:fsm_test --test_filter='TestBlockStatementsCompleteRejectsMissingStatement' --test_output=all
```

Expected: PASS. No other test changes behaviour — this task adds an unused function and two comments.

- [x] **Step 6: Full package + commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel test //fsm:fsm_test --test_output=errors
git add fsm/apply.go fsm/seal_test.go
git commit -m "fsm: add a completeness-checked block statement read (Spec P D1)"
```

**Verification command:** `bazel test //fsm:fsm_test --test_output=errors`

### Task 2: D1 — `BlockDispatchInfo` refuses to dispatch an incomplete block

**Files:**
- Modify: `fsm/reads_dispatch.go` (`:37`, and the now-unreachable `if ss == nil { continue }` at `:45-47`)
- Test: `fsm/reads_dispatch_test.go`

**Interfaces:**
- Consumes: `blockStatementsComplete` (Task 1).
- Produces: nothing new. `BlockDispatchInfo` keeps its `(BlockDispatchInfo, bool)` signature; `orchestrator/dispatch.go:49-51` already treats `ok == false` as "not dispatchable yet" and returns nil, so no orchestrator change is needed. Confirm that in Step 4.

- [x] **Step 1: Write the failing test**

Append to `fsm/reads_dispatch_test.go`:

```go
// TestBlockDispatchInfo_RefusesIncompleteBlock is Spec P D1's acceptance on the
// dispatch path, mirroring TestL3BlockViewRejectsIncompleteBlock's tampering on
// the audit path. Against the unfixed code the call succeeds and hands the
// orchestrator a SourceClaimRoot accumulated over the LAST SURVIVING statement
// plus a short statement list — material a verifier would replay, mismatch, and
// sign as fraud evidence against a source that did nothing wrong.
func TestBlockDispatchInfo_RefusesIncompleteBlock(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	for _, id := range []string{"v1", "v2", "v3"} {
		registerActive(t, f, id, arbiter.NodeRoleVerifier)
	}
	sealBlock(t, f, 2)
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rcFor(f, 1, "0xr1", lthashHex("rowA"))}})
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rcFor(f, 2, "0xr2", lthashHex("rowB"))}})

	info, ok := f.BlockDispatchInfo(1)
	if !ok || info.SourceClaimRoot != "0xr2" || len(info.Statements) != 2 {
		t.Fatalf("a complete block must dispatch: ok=%v root=%q n=%d", ok, info.SourceClaimRoot, len(info.Statements))
	}

	delete(f.st.Statements, 2) // synthesize latent snapshot/state corruption

	info, ok = f.BlockDispatchInfo(1)
	if ok {
		t.Fatalf("an incomplete block must not dispatch; got root=%q over %d statements (header declares 2)",
			info.SourceClaimRoot, len(info.Statements))
	}
	if info.SourceClaimRoot != "" || len(info.Statements) != 0 || len(info.CandidateParts) != 0 || info.ChainHash != "" {
		t.Fatalf("the refusal must return a zero BlockDispatchInfo, got %+v", info)
	}
}
```

- [x] **Step 2: Run it to verify it fails against the unfixed code**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel test //fsm:fsm_test --test_filter='TestBlockDispatchInfo_RefusesIncompleteBlock' \
  --test_output=all --nocache_test_results
```

Expected: **FAIL** at `an incomplete block must not dispatch; got root="0xr1" over 1 statements (header declares 2)`. Record that line verbatim in the PR body — it is the evidence that the forgery path was reachable.

- [x] **Step 3: Adopt the checked read**

In `fsm/reads_dispatch.go`, replace `:37`:

```go
	stmts, complete := f.blockStatementsComplete(blockSeq)
	if !complete {
		// Spec P D1: a short list would make SourceClaimRoot the accumulated
		// root of the last SURVIVING statement while info.Statements under-
		// reports what the source executed. ReplayJob carries neither
		// statement_count nor statements_root, so the verifier cannot detect
		// that itself and would sign a mismatch as fraud evidence. The
		// orchestrator already treats ok=false as "not dispatchable yet".
		return BlockDispatchInfo{}, false
	}
```

and delete the now-unreachable nil guard inside the loop (`:45-47`), because `blockStatementsComplete` already rejects a nil retained record:

```go
	for _, ss := range stmts {
		info.Statements = append(info.Statements, replay.Statement{
```

- [x] **Step 4: Confirm the orchestrator needs no change**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
sed -n '48,56p' orchestrator/dispatch.go
```

Expected: `info, ok := o.d.FSM.BlockDispatchInfo(be.BlockSeq); if !ok { return nil }` — the incomplete block is simply not dispatched this round and is re-examined on the next rescan. Do not add a log line here: `dispatchEvidence` is called once per rescan tick per block and would spam. If operator visibility is wanted, it belongs on Task 4's `WorkSet` error path, which is already rate-shaped by `o.d.Logger.Warn("workset scan failed", ...)` in `orchestrator/loop.go:120`.

- [x] **Step 5: Re-run the test and the package**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel test //fsm:fsm_test --test_filter='TestBlockDispatchInfo' --test_output=all
bazel test //fsm:fsm_test //orchestrator:orchestrator_test --test_output=errors
```

Expected: PASS. `orchestrator`'s dispatch fixtures (`orchestrator/dispatch_fixtures_test.go`, `orchestrator/evm_anchor_test.go`) all build complete blocks and must stay green; if one goes red it was silently relying on a short list — fix the fixture, not the guard.

- [x] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git add fsm/reads_dispatch.go fsm/reads_dispatch_test.go
git commit -m "fix(fsm): refuse to dispatch an incomplete L3 block (Spec P D1)"
```

**Verification command:** `bazel test //fsm:fsm_test //orchestrator:orchestrator_test --test_output=errors`

### Task 3: D1 — `reevaluateBlock` produces no verdict for an incomplete block

**Files:**
- Modify: `fsm/threeway.go` (`:46`)
- Test: `fsm/threeway_test.go`

**Interfaces:**
- Consumes: `blockStatementsComplete` (Task 1).
- Produces: `evidenceBlockN(t, f, n)` — a test helper generalising the existing one-statement `evidenceBlock` (`threeway_test.go:26`), consumed by Task 4's `WorkSet` test.

- [x] **Step 1: Write the failing test**

Append to `fsm/threeway_test.go`:

```go
// evidenceBlockN mirrors evidenceBlock with n statements. A tampering test
// needs n >= 2: with n == 1, removing the only statement leaves an empty list,
// which reevaluateBlock's existing len(stmts) == 0 bail-out already catches —
// so a one-statement fixture cannot distinguish the fix from the status quo.
func evidenceBlockN(t *testing.T, f *FSM, n int) (set []string, partHashes []string) {
	t.Helper()
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	for _, id := range []string{"v1", "v2", "v3"} {
		registerActive(t, f, id, arbiter.NodeRoleVerifier)
	}
	sealBlock(t, f, n)
	for seq := 1; seq <= n; seq++ {
		ph := lthashHex(fmt.Sprintf("row%d", seq))
		rc := rcFor(f, uint64(seq), "0xr00t", ph)
		rc.PartitionNewPartSums = []arbiter.PartitionLtHashSum{
			{TableID: "db.t", PartitionID: "p0", NewPartsLtHashSum: ph},
		}
		mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
		partHashes = append(partHashes, ph)
	}
	mustApply(t, f, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
	return f.st.Verifications[1].VerifierSet, partHashes
}

// TestThreeWay_IncompleteBlockProducesNoVerdict: the three-way predicate must
// refuse to evaluate a block whose retained statements disagree with its
// header, exactly as it already refuses one whose statements lack their bound
// RC. Against the unfixed code a verdict IS produced — computed over a subset
// of the block — and a verdict is what flips statuses and, once Quorum latches,
// what the anchor path trusts forever.
func TestThreeWay_IncompleteBlockProducesNoVerdict(t *testing.T) {
	f := newTestFSM(t)
	set, parts := evidenceBlockN(t, f, 2)

	delete(f.st.Statements, 2) // synthesize latent snapshot/state corruption

	receipt := receiptForBlock(f, "0xr00t")
	receipt.PartitionCommitmentsAfter = []replay.PartitionCommitment{
		{TableID: "db.t", PartitionID: "p0", Root: lthashHex("row1", "row2")},
	}
	scan := append(goodScan(parts[0]), goodScan(parts[1])...)
	for _, rid := range set[:2] {
		attest(t, f, rid, receipt)
		scanIn(t, f, rid, scan)
	}

	if v := f.st.Verifications[1].Verdict; v != nil {
		t.Fatalf("an incomplete block must produce no verdict at all, got %+v", v)
	}
	if got := f.st.Statements[1].Status; got != StatusReplaying {
		t.Fatalf("no statement may advance on an incomplete block, got %v", got)
	}
}
```

Add `"fmt"` to `threeway_test.go`'s imports.

- [x] **Step 2: Run it to verify it fails against the unfixed code**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel test //fsm:fsm_test --test_filter='TestThreeWay_IncompleteBlockProducesNoVerdict' \
  --test_output=all --nocache_test_results
```

Expected: **FAIL** at `an incomplete block must produce no verdict at all, got &{Replicas:map[...] Quorum:false}` — today the predicate evaluates the honest two-replica bundle against a one-statement claim and records a (failing) verdict rather than declining. Record the line. Note the direction matters both ways: with the tampering on the *evidence* side instead, the same short list can produce `Quorum: true` over an under-counted block.

- [x] **Step 3: Adopt the checked read**

In `fsm/threeway.go`, replace `:46-49`:

```go
	// Spec P D1: an incomplete block is not evaluable. A short list drops a
	// statement's RC from checks 2 and 3 and can change which RC is lastRC for
	// check 1, so the verdict would describe a subset of the block. This is the
	// same shape as the ss.RC == nil evaluability bail-out below.
	stmts, complete := f.blockStatementsComplete(blockSeq)
	if !complete || len(stmts) == 0 {
		return
	}
```

- [x] **Step 4: Re-run the test and the package**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel test //fsm:fsm_test --test_filter='TestThreeWay' --test_output=all
bazel test //fsm:fsm_test --test_output=errors
```

Expected: PASS, including every existing `TestThreeWay_*` — they all build complete blocks.

- [x] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git add fsm/threeway.go fsm/threeway_test.go
git commit -m "fix(fsm): decline the three-way verdict for an incomplete block (Spec P D1)"
```

**Verification command:** `bazel test //fsm:fsm_test --test_output=errors`

### Task 4: D1 — the two root/watermark callers Spec P did not name

Spec P's §1a names only `BlockDispatchInfo` and `reevaluateBlock`. Reading the source turned up two more, and one of them is strictly worse than either: `WorkSet` derives the **anchor** state root, and it does so only after `Verdict.Quorum` latches — at which point `reevaluateBlock` returns early and Task 3's guard no longer runs. A block corrupted after quorum therefore reaches L2 with a root computed over the surviving statements. This task closes both.

**Files:**
- Modify: `fsm/reads_work.go` (`safePrefixLocked` at `:41-58`; the `UnanchoredVerified` branch at `:106-124`)
- Test: `fsm/reads_work_test.go`

**Interfaces:**
- Consumes: `blockStatementsComplete` (Task 1), `evidenceBlockN` (Task 3), `fsm.ErrL3BlockIncomplete` (`reads.go:19`, already exported and already mapped by `server/safestate.go:54`).
- Produces: nothing new. `WorkSet` keeps its `(WorkSet, error)` signature.

- [x] **Step 1: Write the two failing tests**

Append to `fsm/reads_work_test.go`:

```go
// TestSafePrefix_StallsOnIncompleteBlock: a statement the state no longer
// retains is absent, not proven safe. Against the unfixed code it is silently
// skipped, so allSafe stays true and the safe prefix — which drives the
// promotion frontier, the manifest-debt flag and CustodyWork.Safe — advances
// over a block with an unaccounted statement.
func TestSafePrefix_StallsOnIncompleteBlock(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	sealBlock(t, f, 2)
	f.st.Statements[1].Status = StatusSafe
	f.st.Statements[2].Status = StatusSafe
	if got := safePrefixLocked(f); got != 1 {
		t.Fatalf("a complete all-safe block must advance the prefix, got %d", got)
	}

	delete(f.st.Statements, 2) // synthesize latent snapshot/state corruption

	if got := safePrefixLocked(f); got != 0 {
		t.Fatalf("an incomplete block must not advance the safe prefix, got %d", got)
	}
}

// TestWorkSet_RefusesAnchorRootForIncompleteBlock: quorum is monotone and
// latches, so reevaluateBlock returns early once it is reached and Spec P D1's
// verdict guard no longer runs. The anchor root must therefore be re-proven at
// the point it is derived. Against the unfixed code WorkSet returns no error
// and an UnanchoredVerified row whose StateRoot covers one of two statements.
func TestWorkSet_RefusesAnchorRootForIncompleteBlock(t *testing.T) {
	f := newTestFSM(t)
	set, parts := evidenceBlockN(t, f, 2)
	receipt := receiptForBlock(f, "0xr00t")
	receipt.PartitionCommitmentsAfter = []replay.PartitionCommitment{
		{TableID: "db.t", PartitionID: "p0", Root: lthashHex("row1", "row2")},
	}
	scan := append(goodScan(parts[0]), goodScan(parts[1])...)
	for _, rid := range set[:2] {
		attest(t, f, rid, receipt)
		scanIn(t, f, rid, scan)
	}
	if v := f.st.Verifications[1].Verdict; v == nil || !v.Quorum {
		t.Fatalf("honest 2-of-3 evidence must reach quorum before the tampering: %+v", v)
	}

	delete(f.st.Statements, 2) // corruption AFTER quorum latched

	if _, err := f.WorkSet(); !errors.Is(err, ErrL3BlockIncomplete) {
		t.Fatalf("WorkSet must refuse to derive an anchor state root for an incomplete block, got err=%v", err)
	}
}
```

Add `"errors"` and `"github.com/housegate/housegate/pkg/replay"` to `reads_work_test.go`'s imports if they are not already there.

- [x] **Step 2: Run both to verify they fail against the unfixed code**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel test //fsm:fsm_test \
  --test_filter='TestSafePrefix_StallsOnIncompleteBlock|TestWorkSet_RefusesAnchorRootForIncompleteBlock' \
  --test_output=all --nocache_test_results
```

Expected: **both FAIL** — `an incomplete block must not advance the safe prefix, got 1` and `WorkSet must refuse … got err=<nil>`. Record both lines.

- [x] **Step 3: Stall the safe prefix**

In `fsm/reads_work.go`, replace `safePrefixLocked`'s per-block body:

```go
	for idx := range f.st.Blocks {
		blockSeq := uint64(idx + 1)
		// Spec P D1: a statement the state no longer retains is absent, not
		// safe. Stalling the prefix is the conservative direction — it delays
		// promotion rather than promoting over an unaccounted statement.
		stmts, complete := f.blockStatementsComplete(blockSeq)
		if !complete {
			break
		}
		allSafe := true
		for _, ss := range stmts {
			if ss.Status != StatusSafe {
				allSafe = false
				break
			}
		}
		if !allSafe {
			break
		}
		safe = blockSeq
	}
```

- [x] **Step 4: Refuse the anchor root**

In the `UnanchoredVerified` branch of `WorkSet` (`:106`), re-resolve with the checked read before deriving the root. Keep the unchecked `stmts` from `:73` for the `AwaitingRC` / `SealedUnmarked` / `blockChallenged` classification above — those are advisory work rows, not derivations:

```go
		if bv.Verdict != nil && bv.Verdict.Quorum && !(bv.Finality && bv.LastMergeable) {
			chainHash, err := f.st.Blocks[idx].ChainHash()
			if err != nil {
				return WorkSet{}, err
			}
			// Spec P D1: this StateRoot is what gets anchored on L2. Quorum
			// latches, so reevaluateBlock's guard has already stopped running
			// for this block; re-prove completeness where the root is derived.
			anchorStmts, complete := f.blockStatementsComplete(blockSeq)
			if !complete {
				return WorkSet{}, fmt.Errorf("%w: l3 block %d cannot produce an anchor state root",
					ErrL3BlockIncomplete, blockSeq)
			}
			var ref arbiter.AnchorRef
			if bv.Anchor != nil {
				ref = *bv.Anchor
			}
			ws.UnanchoredVerified = append(ws.UnanchoredVerified, BlockAnchor{
				BlockSeq:      blockSeq,
				ChainHash:     chainHash,
				StateRoot:     blockStateRoot(anchorStmts),
				Ref:           ref,
				Anchored:      bv.Anchor != nil,
				Finality:      bv.Finality,
				LastMergeable: bv.LastMergeable,
			})
		}
```

Add `"fmt"` to `reads_work.go`'s imports.

- [x] **Step 5: Confirm the orchestrator surfaces the refusal**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
sed -n '117,124p' orchestrator/loop.go
```

Expected: `ws, err := o.d.FSM.WorkSet(); if err != nil { o.d.Logger.Warn("workset scan failed", "err", err); return nil }` — the refusal is logged once per rescan tick and nothing is anchored. That is the intended behaviour: loud, local, retried, and never a silent short root. No orchestrator change.

- [x] **Step 6: Re-run and commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel test //fsm:fsm_test //orchestrator:orchestrator_test //server:server_test --test_output=errors
git add fsm/reads_work.go fsm/reads_work_test.go
git commit -m "fix(fsm): re-prove block completeness at the anchor root and the safe prefix (Spec P D1)"
```

**Verification command:** `bazel test //fsm:fsm_test //orchestrator:orchestrator_test //server:server_test --test_output=errors`

### Task 5: D5 — `accumulator/vectors_test.go` loses its `-update` flag

The last golden regenerator in the tree. Spec K D1's `statements_root` goldens already have none, and `l3_commitment_golden_test.go:22-26` says why in as many words. This file's vectors sit one layer down the same laundering path: `SpentIDs.Root()` → `L3BlockHeader.SpentIDsRootAfter` (stamped at `fsm/apply.go:142`) → `L3BlockHeader.ChainHash()` (`fsm/state.go:159-165`) → the value anchored on L2.

**Files:**
- Modify: `accumulator/vectors_test.go`

**Interfaces:** none. `buildVectors()` stays — it is the deterministic construction the file documents and it keeps `testdata/spent_ids_vectors.json` reviewable — but nothing writes the file any more.

- [x] **Step 1: Prove the flag currently works (the structural pre-fix state)**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel run -- @rules_go//go test ./accumulator/ -run TestVectors -update 2>/dev/null || \
  (cd /Users/uranuswch/Dev/sentio_xyz/arbiter && go test ./accumulator/ -run TestVectors -update)
git status --short accumulator/testdata/spent_ids_vectors.json
```

Expected: the test passes and `git status` shows the file either unchanged (already in sync) or rewritten. Either way the flag exists and rewrites the freeze on demand — that is the defect. `git checkout accumulator/testdata/spent_ids_vectors.json` afterwards.

- [x] **Step 2: Delete the flag and its branch**

In `accumulator/vectors_test.go`:

- delete `"flag"` from the import block,
- delete `var update = flag.Bool("update", false, "regenerate testdata/spent_ids_vectors.json")` (`:23`),
- delete the whole `if *update { … }` block at the head of `TestVectors` (`:228-240`),
- delete `"os"`/`"path/filepath"` from the imports **only if** nothing else uses them — `os.ReadFile(vectorPath)` at `:241` still does, so `"os"` stays and `"path/filepath"` goes,
- replace the header comment (`:16-21`) with:

```go
// The vector file freezes the sentio-spent-ids-v1 profile for future
// implementations (design §9). There is deliberately NO regenerator: a change
// to this derivation changes SpentIDs.Root(), which fsm/apply.go stamps into
// L3BlockHeader.SpentIDsRootAfter, which L3BlockHeader.ChainHash() commits to,
// which is what gets anchored on L2. Every historical anchored value must stay
// recomputable forever, so regenerating these vectors is a versioned protocol
// migration with an explicit plan — edit testdata/spent_ids_vectors.json by
// hand, in a commit that says why — never a "-update" run (Spec P D5, matching
// fsm/l3_commitment_golden_test.go's rule for the statements_root goldens).
```

- also fix `:243`'s now-stale failure message: `t.Fatalf("read vectors (run with -update once to generate): %v", err)` → `t.Fatalf("read %s (frozen vectors; regenerating them is a protocol migration): %v", vectorPath, err)`.

- [x] **Step 3: Prove the flag is gone**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel test //accumulator:accumulator_test --test_output=errors
grep -rn 'flag.Bool' --include="*_test.go" . || echo "no test regenerator flags remain"
```

Expected: `accumulator_test` PASS; the grep prints the "no test regenerator flags remain" line (the two `flag.Bool` uses under `cmd/` are CLI flags on non-test files and are untouched).

- [x] **Step 4: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git add accumulator/vectors_test.go
git commit -m "test(accumulator): freeze the spent-ids vectors by deleting -update (Spec P D5)"
```

**Verification command:** `bazel test //accumulator:accumulator_test //fsm:fsm_test --test_output=errors`

### Task 5b: Part A close-out

- [x] **Step 1: Full repo gate**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel build //... && bazel test //... --test_output=errors
```

Expected: identical to Task 0's baseline plus the four new tests. Any newly failing target is a regression — fix it before opening the PR.

- [ ] **Step 2: Open the PR** *(Deferred: this round commits on `fix/si-dispatch-completeness` but does not push or open the PR. The four recorded pre-fix failure lines are in Part A's report; the eleven-row caller classification reproduced unchanged.)*

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
gh pr create --fill --title "fix(fsm): dispatch-path block completeness and a frozen vector file (Spec P D1/D5)"
```

The PR body must carry the four recorded pre-fix failure lines from Tasks 2, 3 and 4, the eleven-row caller classification from Task 1 Step 1, and an explicit note that `statement_count` / `statements_root` on `replay.ReplayJob` remains the durable fix and is recorded debt.

---

## Part B — housegate

**Working directory for every Part B task:** `/Users/uranuswch/Dev/housegate/housegate`

### Task 6: D4 — enumerate every exact-part-name consumer and prove each keys on `UnsafeDatabase`

**This task is a gate, not a change.** Spec P D4 says so explicitly: *"if any consumer reads a safe name, the finding is wrong and the task stops rather than proceeding."* Nothing in Tasks 7 and 8 may start until this enumeration reproduces. If it does not, stop, record the counter-example, and escalate — the reservation protocol's single-owner candidate accounting is what would break.

**Files:** none modified. This task produces evidence recorded in the PR body and in Task 7's code comment.

- [ ] **Step 1: Enumerate every read of the exact-name inventory**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
grep -n "activeParts" pkg/storageintegrity/parts_pressure.go | grep -v _test
grep -n "PartsKey{Database" pkg/storageintegrity/*.go | grep -v _test
grep -n "keys:\s*keys\|reservation.keys\|r.keys\[" pkg/storageintegrity/parts_pressure.go | grep -v _test
```

Expected classification — every entry must resolve to `UnsafeDatabase`, either by literal construction or transitively through `partsReservation.keys`:

| Site | Reads | Key origin | Unsafe-only? |
|---|---|---|---|
| `:756` `RestoreBatch` legacy-observation migration | `g.activeParts[key][candidate.PartName]` | `PartsKey{Database: g.cfg.UnsafeDatabase, …}` literal (`:755`) | yes, literal |
| `:793` `RestoreBatch` finalized bind loop | `g.activeParts[key][candidate.PartName]` | `reservation.keys[keyIndex]` | yes, transitively |
| `:892` `newReservationLocked` baseline capture | `copyPartNames(g.activeParts[key])` | `PartsKey{Database: g.cfg.UnsafeDatabase, …}` literal (`:889`) | yes, literal |
| `:1157` `PrepareCleanupProof` | `r.guard.activeParts[key][candidate.PartName]` | `r.keys[…]` | yes, transitively |
| `:1246` `remainingObservedCandidatesLocked` | `r.guard.activeParts[r.keys[keyIndex]]` | `r.keys[…]` | yes, transitively |
| `:1312` `bindCandidatePartsLocked` | `r.guard.activeParts[key][candidate.PartName]` | `r.keys[keyIndex]` | yes, transitively |
| `:1359` `reconcileCandidateClaimsLocked` | `g.activeParts[key]` | `g.candidateClaims` — sole writer is `bindCandidatePartsLocked` (`:1301-1324`) using `r.keys` | yes, transitively |
| `:1586` `coveredReservationSlotsLocked` | `g.activeParts[key][partName]` | `g.committedReservations` — sole writer is `commitLocked` (`:1497`) using `r.keys` | yes, transitively |
| `:1657` `rebaseLiveReservationsAfterCleanupLocked` | `copyPartNames(g.activeParts[key])` | `reservation.keys[keyIndex]` | yes, transitively |
| `:511` `markExactScopeFreshLocked`, `:545-576` `applyExactScopeLocked` | iterate `g.activeParts` under `scope.Covers` | write/bookkeeping, not a name consumer | n/a |
| `:599` `invalidatePendingObservationKeysLocked` | builds keys to invalidate | `PartsKey{Database: g.cfg.UnsafeDatabase, …}` literal | yes, literal |

- [ ] **Step 2: Prove `partsReservation.keys` can only ever hold unsafe keys**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
grep -n "keys = append(keys, key)\|keys:\s*keys," pkg/storageintegrity/parts_pressure.go
sed -n '884,905p' pkg/storageintegrity/parts_pressure.go
```

Expected: `newReservationLocked` (`:864-916`) is the **only** constructor of `partsReservation`, and its `keys` slice is built exclusively from `PartsKey{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID}` at `:889`. `tableScope()` (`:1036`) and `reservationScope()` (`:1041`) likewise hard-code `r.guard.cfg.UnsafeDatabase`.

- [ ] **Step 3: Prove capacity accounting never consults a safe key**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
sed -n '977,1002p' pkg/storageintegrity/parts_pressure.go
```

Expected: `checkAvailableLocked` / `checkCapacityLocked` build `PartsKey{Database: g.cfg.UnsafeDatabase, …}` literals (`:984`, `:991`) and read `g.snapshot[key] + g.reserved[key] + g.committed[key]`. Safe-database entries in `g.snapshot` exist only to feed the `storage_integrity_safe_parts` gauge (`storage_integrity_backpressure.go:87-93`) and are never summed into a limit.

- [ ] **Step 4: Record the verdict and unblock (or stop)**

Write the three tables/greps above into the PR body under the heading *"D4 enumeration"*, ending with one of:

- **PASS** — *"No consumer of exact part names reads a safe-database key. Every read keys on `UnsafeDatabase` by literal construction or transitively through `partsReservation.keys`, whose sole constructor hard-codes it. Proceeding to Task 7."*
- **FAIL** — *"`<site>` reads `<safe key>`. Spec P D4's finding is wrong; Tasks 7 and 8 are blocked."* Then **stop the task chain**, open an issue naming the site, and change nothing in `parts_pressure.go`.

**Verification command:** the three greps above reproduce the tables; no test is run and no file is changed. This task has no commit.

### Task 7: D4 — split the exact scope from the count scope so exact reads bind `hg_unsafe` only

**Files:**
- Modify: `pkg/storageintegrity/parts_pressure.go` (`fullScope` `:324-330`; `Refresh` `:337`; `RefreshCounts` `:367`; `RestoreBatch` `:678`; `BuildSnapshotQuery` `:257-270`)
- Modify: `pkg/storageintegrity/parts_pressure_scope.go` (`IsFull` `:58-63`)
- Test: `pkg/storageintegrity/parts_pressure_test.go` (invert `TestPartsPressureGuard_FullScopeCoversSafeDatabase` `:446-467`; delete `TestPartsPressureGuard_BuildSnapshotQuery` `:258`), `pkg/storageintegrity/parts_pressure_scope_test.go` (new `RestoreBatch` assertion)

**Interfaces:**
- Produces: `(*PartsPressureGuard).exactScope() PartsScope` and `(*PartsPressureGuard).countScope() PartsScope`, replacing `fullScope()`. Both package-private.
- Removes: the exported `(*PartsPressureGuard).BuildSnapshotQuery()`. Step 2 proves it has no caller anywhere.

> **Why not the spec's literal change.** Spec P D4 says *"`fullScope()` returns unsafe-only"*. That breaks three things at once: `RefreshCounts` passes `fullScope()` to `applyCountScopeLocked` / `reconcileScopeLocked` as the scope its **counts** are authoritative for and must keep covering the safe half; `PartsScope.IsFull` requires the safe database to be named before it reports "full", and `IsFull` is what sets `lastFullOK` (which gates `Snapshot()`), what selects `refreshGate.Lock()` over `RLock()` in `refreshScope`, and what `RestoreBatch`'s latch logic leans on. Splitting the constructor and relaxing `IsFull` to the capacity-relevant surface preserves all three.

- [ ] **Step 1: Write the failing tests**

Replace `TestPartsPressureGuard_FullScopeCoversSafeDatabase` in `pkg/storageintegrity/parts_pressure_test.go` with its inverse:

```go
// TestPartsPressureGuard_ExactScopeNeverNamesTheSafeDatabase is Spec P D4's
// acceptance. Task 6 proved every exact-name consumer keys on UnsafeDatabase,
// so the safe database's part names are dead weight in this read — and at the
// design's stated scale (10 tables x 12 partitions x 2500 parts) they are the
// row budget that makes the startup RestoreBatch pass time out and latch
// restoreBlocked.
func TestPartsPressureGuard_ExactScopeNeverNamesTheSafeDatabase(t *testing.T) {
	g := NewPartsPressureGuard(&fakePartsConn{}, PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe",
		SafeDatabase:   "hg_safe",
	})

	exact := g.exactScope()
	if exact.IncludeSafeDatabase || exact.SafeDatabase != "" {
		t.Fatalf("the exact-name scope must not name the safe database: %+v", exact)
	}
	if !exact.IsFull(g.cfg) {
		t.Fatal("the exact-name scope must still satisfy IsFull: hg_unsafe is the whole capacity-relevant surface")
	}
	if exact.Covers(PartsKey{Database: "hg_safe", Table: "db__t", Partition: "p_a"}) {
		t.Fatal("the exact-name scope must not claim authority over safe-database keys")
	}
	query, args := g.BuildExactPartsQuery(exact)
	if strings.Contains(query, "IN (?, ?)") {
		t.Fatalf("the exact-name read must bind one database: %s", query)
	}
	if len(args) != 1 || args[0] != "hg_unsafe" {
		t.Fatalf("exact-name args = %v, want [hg_unsafe]", args)
	}

	count := g.countScope()
	if !count.IncludeSafeDatabase || count.SafeDatabase != "hg_safe" {
		t.Fatalf("the bounded count scope must still cover the safe database for the gauge: %+v", count)
	}
	if !count.IsFull(g.cfg) {
		t.Fatal("the count scope must satisfy IsFull")
	}
}
```

Append to `pkg/storageintegrity/parts_pressure_scope_test.go`:

```go
// TestRestoreBatch_NeverReadsSafeDatabasePartNames pins the one production
// caller of the exact full read. RestoreBatch is the startup recovery
// boundary; before Spec P D4 it scanned every active part name in BOTH
// databases, which is where Spec L D3(b)'s growth cliff moved to.
func TestRestoreBatch_NeverReadsSafeDatabasePartNames(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__t", partition: "p0", partitionKey: "p", number: 2},
		fakePartsRow{database: "hg_safe", table: "db__t", partition: "p0", partitionKey: "p", number: 5},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	if _, err := g.RestoreBatch(context.Background(), nil); err != nil {
		t.Fatalf("RestoreBatch: %v", err)
	}
	queries := conn.recordedQueries()
	if len(queries) != 1 {
		t.Fatalf("RestoreBatch issued %d queries, want 1", len(queries))
	}
	if !strings.Contains(queries[0], "parts.name") {
		t.Fatalf("RestoreBatch must still read exact names: %s", queries[0])
	}
	if strings.Contains(queries[0], "IN (?, ?)") {
		t.Fatalf("RestoreBatch must bind only the unsafe database: %s", queries[0])
	}
	if args := conn.lastArgs(); len(args) != 1 || args[0] != "hg_unsafe" {
		t.Fatalf("RestoreBatch args = %v, want [hg_unsafe]", args)
	}
	g.mu.RLock()
	names := g.activeParts[PartsKey{Database: "hg_safe", Table: "db__t", Partition: "p_p0"}]
	ok := g.lastFullOK
	g.mu.RUnlock()
	if len(names) != 0 {
		t.Fatalf("safe-database part names were installed: %v", names)
	}
	if !ok {
		t.Fatal("an unsafe-only exact read must still latch lastFullOK; IsFull now describes the capacity surface")
	}
}
```

- [ ] **Step 2: Run them to verify they fail, and prove `BuildSnapshotQuery` is dead**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //pkg/storageintegrity:storageintegrity_test \
  --test_filter='TestPartsPressureGuard_ExactScopeNeverNamesTheSafeDatabase|TestRestoreBatch_NeverReadsSafeDatabasePartNames' \
  --test_output=all --nocache_test_results
grep -rn "BuildSnapshotQuery" --include="*.go" \
  /Users/uranuswch/Dev/housegate/housegate \
  /Users/uranuswch/Dev/sentio_xyz/sentio-node \
  /Users/uranuswch/Dev/sentio_xyz/arbiter-core \
  /Users/uranuswch/Dev/sentio_xyz/arbiter
```

Expected: **FAIL** — `g.exactScope undefined` / `g.countScope undefined`. And the grep must show `BuildSnapshotQuery` only in `parts_pressure.go` (its definition) and `parts_pressure_test.go` (its own unit test) — no production caller in any repo, so removing it is safe. If any external caller appears, keep the function and make it unsafe-only instead of deleting it, and say so in the commit message.

- [ ] **Step 3: Split the scope constructor**

In `pkg/storageintegrity/parts_pressure.go`, replace `fullScope()` (`:324-330`):

```go
// exactScope is the scope of every exact part-NAME read: the unsafe database
// only. Task 6's enumeration proved every exact-name consumer keys on
// UnsafeDatabase — by literal construction or transitively through
// partsReservation.keys — so safe-database names are provably dead weight
// here, and dropping them closes Spec L D3(b)'s growth cliff without touching
// the reservation protocol (Spec P D4).
func (g *PartsPressureGuard) exactScope() PartsScope {
	return PartsScope{Database: g.cfg.UnsafeDatabase}
}

// countScope is the scope of the bounded aggregate pass: BOTH databases. This
// read is a count() GROUP BY bounded by tables x partitions, not by part
// names, and storage_integrity_safe_parts needs the safe half.
func (g *PartsPressureGuard) countScope() PartsScope {
	return PartsScope{
		Database:            g.cfg.UnsafeDatabase,
		IncludeSafeDatabase: g.cfg.SafeDatabase != "",
		SafeDatabase:        g.cfg.SafeDatabase,
	}
}
```

Re-point the three call sites, one at a time, and re-read each in context before changing it:

- `Refresh` (`:337`): `return g.refreshScope(ctx, g.exactScope())` — exact names, no production caller today (the supervisor's own `Refresh` calls `RefreshCounts` + `RefreshLiveKeys`); kept for hosts and tests.
- `RefreshCounts` (`:367`): `scope := g.countScope()` — **unchanged behaviour**; this is the bounded aggregate and the gauge source.
- `RestoreBatch` (`:678`): `if _, err := g.refreshScope(ctx, g.exactScope()); err != nil {` — the one production exact full read, and the whole point of this task.

Then delete `BuildSnapshotQuery` (`:257-270`) and its unit test — it is the last query builder in the package that emits `parts.name` for the safe database, it has no caller, and leaving it would leave the invariant ungreppable.

- [ ] **Step 4: Relax `IsFull` to the capacity-relevant surface**

In `pkg/storageintegrity/parts_pressure_scope.go`:

```go
// IsFull reports whether the scope covers the whole capacity-relevant surface:
// every key in the unsafe database. The safe database is gauge-only — counts,
// never names — and is deliberately NOT required here, so the unsafe-only
// exact scope still latches lastFullOK, still takes refreshGate's write lock in
// refreshScope, and still clears RestoreBatch's latch (Spec P D4).
func (s PartsScope) IsFull(cfg PartsPressureConfig) bool {
	return s.Table == "" && s.Database == cfg.UnsafeDatabase
}
```

`countScope()` still satisfies it (`Table == ""`, `Database == cfg.UnsafeDatabase`), so `RefreshCounts`'s `lastFullOK` bookkeeping is unchanged. `RefreshLiveKeys`'s per-table scopes still do not (`Table != ""`), as before.

- [ ] **Step 5: Re-run the package**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel test //pkg/storageintegrity:storageintegrity_test --test_output=errors
```

Expected: PASS. Two existing tests are expected to need the edits above and no others; if a third goes red, read it before touching it — it is telling you something about `scope.Covers` no longer claiming safe keys.

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/storageintegrity/parts_pressure.go pkg/storageintegrity/parts_pressure_scope.go \
  pkg/storageintegrity/parts_pressure_test.go pkg/storageintegrity/parts_pressure_scope_test.go
git commit -m "fix(storage-integrity): read exact part names from hg_unsafe only (Spec P D4)"
```

**Verification command:** `bazel test //pkg/storageintegrity:storageintegrity_test --test_output=errors`

### Task 8: D4 — the scale proof against real `system.parts`, and the two docker assertions that must move

**Files:**
- Modify: `pkg/integration/storage_backpressure_bounded_test.go` (add the recovery-scale assertions)
- Modify: `pkg/integration/storage_backpressure_test.go` (`:93` asserts a safe-database count out of `guard.Refresh`)

**Interfaces:**
- Consumes: Task 7's `exactScope()` indirectly, through `guard.Refresh` / `guard.RestoreBatch`.

- [ ] **Step 1: Re-point the safe-count assertion that Task 7 breaks**

`pkg/integration/storage_backpressure_test.go:93` asserts `snapshot[PartsKey{Database: safeDB, …}] == 1` out of `guard.Refresh(ctx)`. After Task 7 the exact read no longer covers the safe database, so that entry is absent. The safe count is still real — it just comes from the bounded aggregate now. Split the assertion:

```go
	snapshot, err := guard.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if snapshot[sicore.PartsKey{Database: unsafeDB, Table: table, Partition: "p_p0"}] != 3 ||
		snapshot[sicore.PartsKey{Database: unsafeDB, Table: table, Partition: "p_tuple()"}] != 3 ||
		snapshot[sicore.PartsKey{Database: unsafeDB, Table: table, Partition: "p_p1"}] != 1 ||
		snapshot[sicore.PartsKey{Database: unsafeDB, Table: unpartitioned, Partition: "all"}] != 1 {
		t.Fatalf("snapshot = %v", snapshot)
	}
	// Spec P D4: the exact-name read is hg_unsafe-only. Safe counts are gauge
	// input and come from the bounded aggregate pass, never from a name scan.
	if _, present := snapshot[sicore.PartsKey{Database: safeDB, Table: table, Partition: "p_p0"}]; present {
		t.Fatalf("the exact-name read must not report safe-database keys: %v", snapshot)
	}
	counts, err := guard.RefreshCounts(ctx)
	if err != nil {
		t.Fatalf("RefreshCounts: %v", err)
	}
	if counts[sicore.PartsKey{Database: safeDB, Table: table, Partition: "p_p0"}] != 1 {
		t.Fatalf("the bounded aggregate must still supply the safe count: %v", counts)
	}
```

- [ ] **Step 2: Add the recovery-scale assertions**

Append to `TestPartsPressure_HotPathReadStaysBoundedWithManyParts` in `pkg/integration/storage_backpressure_bounded_test.go`, after the existing admission-latency assertion. The property asserted is **that the exact read does not scale with the safe database**, proved by row count rather than by wall clock so it cannot flake. At the design's stated 10 × 12 × 2500 ≈ 300k shape this difference is what separates a `refresh_timeout` expiry plus a latched `restoreBlocked` from a bounded read; the test proves the invariant at 3000 safe parts so CI stays fast.

```go
	// Spec P D4: the exact-name read is hg_unsafe-only, so it must not grow
	// with hg_safe. Compare the two query shapes against real system.parts.
	oldShapeQuery, oldShapeArgs := guard.BuildExactPartsQuery(sicore.PartsScope{
		Database: unsafeDB, IncludeSafeDatabase: true, SafeDatabase: safeDB,
	})
	newShapeQuery, newShapeArgs := guard.BuildExactPartsQuery(sicore.PartsScope{Database: unsafeDB})
	oldRows := countRows(oldShapeQuery, oldShapeArgs...)
	newRows := countRows(newShapeQuery, newShapeArgs...)
	if oldRows < noisyParts {
		t.Fatalf("fixture is not exercising the cliff: the both-database exact read returned %d rows", oldRows)
	}
	if newRows > 400 {
		t.Fatalf("the unsafe-only exact read returned %d rows; it must not scale with hg_safe (%d parts)", newRows, noisyParts)
	}

	// The production caller of the exact full read is the startup recovery
	// boundary. It must complete and must not latch restoreBlocked.
	if _, err := guard.RestoreBatch(ctx, nil); err != nil {
		t.Fatalf("RestoreBatch with %d parts in hg_safe: %v", noisyParts, err)
	}
	if _, ok := guard.Snapshot(); !ok {
		t.Fatal("RestoreBatch must leave a usable snapshot; a latched restoreBlocked is the startup failure Spec L D3(b) left open")
	}
```

- [ ] **Step 3: Run it and verify the pre-fix failure**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
export DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}')
bazel test //pkg/integration:integration_test \
  --test_filter='TestPartsPressure_HotPathReadStaysBoundedWithManyParts|TestPartsPressureGuard_AgainstRealSystemParts' \
  --test_env=DOCKER_HOST --test_env=HOME --test_output=all --nocache_test_results
```

Expected on the fixed tree: PASS. Then prove the guard is load-bearing:

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git stash push pkg/storageintegrity/parts_pressure.go pkg/storageintegrity/parts_pressure_scope.go
bazel test //pkg/integration:integration_test \
  --test_filter='TestPartsPressure_HotPathReadStaysBoundedWithManyParts' \
  --test_env=DOCKER_HOST --test_env=HOME --test_output=all --nocache_test_results
git stash pop
```

Expected on the unfixed guard: **FAIL** at `the unsafe-only exact read returned 3200 rows; it must not scale with hg_safe (3000 parts)` — because pre-fix `guard.Refresh` / `RestoreBatch` bind both databases. Record the line. (The stashed run may not compile if Task 7's new test names are already present in the package; in that case stash `pkg/storageintegrity/` wholesale, or perform the check on a scratch branch reverted to `main`'s `parts_pressure.go`.)

- [ ] **Step 4: Confirm CI already runs this target**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
grep -n "pkg/integration" .github/workflows/ci.yml
```

Expected: `//pkg/integration:integration_test` is already in CI's explicit list, so no workflow edit is needed.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/integration/storage_backpressure_bounded_test.go pkg/integration/storage_backpressure_test.go
git commit -m "test(storage-integrity): prove the exact part-name read does not scale with hg_safe (Spec P D4)"
```

**Verification command:** `bazel test //pkg/integration:integration_test --test_filter='TestPartsPressure' --test_env=DOCKER_HOST --test_env=HOME --test_output=errors`

### Task 9: D2 — the `SET` refusal becomes a recorded, tested property (HouseGate half)

Spec I D1's catch-all already produces this behaviour: `SET` is modelled by no handler in either engine, so under an active SI contract it reaches the catch-all and returns `UnsupportedStatement`, which HouseGate's D3 posture turns into a rejection. That is the right outcome and it is currently an accident with no test and no record. This task converts it into a contract. **No production code changes.**

**Files:**
- Modify: `pkg/integration/storage_integrity_read_test.go` (new test function)
- Modify: `docs/superpowers/specs/2026-08-19-storage-integrity-surface-failclosed-design.md` (D6's decision record gains one sentence)
- Modify: `CLAUDE.md` (the storage-integrity paragraph of "4. SQL rewriter is a pluggable backend")

- [ ] **Step 0: Check the Spec O gate before starting**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
grep -n "rewriter-go" go.mod
ls pkg/rewriter/probe.go 2>/dev/null || echo "Spec I's HouseGate half is NOT merged"
```

The test half that asserts refusal needs **both** an engine that rejects `SET` under an active SI contract (rewriter-go ≥ v0.9.0) **and** HouseGate's fail-closed-on-any-non-`Success` posture (Spec I D3, open PR #141). If `go.mod` still pins `v0.7.1` and `pkg/rewriter/probe.go` is absent, do one of:

- base this task's branch on PR #141's branch and run with `POLYGLOT_SQL_FFI_PATH=$(go run ./cmd fetch-rewriter-lib --tag v0.9.0)`; or
- defer this task until Spec O's HouseGate pin + merge task has landed, and execute Tasks 6-8 and 10 meanwhile.

Record which option was taken. The non-SI half of the test (Step 1's second case) passes on `main` today either way.

- [ ] **Step 1: Write the test**

Append to `pkg/integration/storage_integrity_read_test.go`:

```go
// TestStorageIntegrityRead_SessionLevelSetIsRefused records Spec P D2: an
// SI-configured deployment refuses session-level SET. This is a consequence of
// Spec I D1's catch-all (SET is modelled by no handler in either engine, so it
// reaches the catch-all and returns UnsupportedStatement) plus Spec I D3
// (HouseGate treats any non-Success as a rejection when SI tables are
// configured). It matters because settings_hash commits to the EMPTY user
// settings set: a session-level `SET async_insert=1` issued before an SI INSERT
// would be invisible to both the agent signer and the server ingress, which
// would honestly sign EmptySettingsHash for a statement that then executes
// under different settings. The refusal is what makes that unreachable, and
// this test is what stops a refactor from turning it back into an accident.
func TestStorageIntegrityRead_SessionLevelSetIsRefused(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run `go run ./cmd fetch-rewriter-lib --tag <si-engine-tag>` and pass --test_env")
	}
	ctx := context.Background()
	const phys = "phys_si_set"
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS " + phys,
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"CREATE DATABASE IF NOT EXISTS hg_unsafe",
		"DROP TABLE IF EXISTS hg_safe.db1__t",
		"DROP TABLE IF EXISTS hg_unsafe.db1__t",
		"CREATE TABLE hg_unsafe.db1__t (_hg_row_id FixedString(32), a UInt32) ENGINE = MergeTree ORDER BY a",
		"CREATE TABLE hg_safe.db1__t AS hg_unsafe.db1__t ENGINE = MergeTree ORDER BY a",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_safe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_unsafe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys)
	})

	withSI := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("db1"),
		testenv.WithStorageIntegrityReadState(&siReadStateStub{parts: map[string][]string{}}),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.StorageIntegrity.Tables = []string{"db1.t"}
			cfg.StorageIntegrity.Read.DefaultMode = string(rewriter.ReadModeSafe)
		}),
	)
	withoutSI := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("db1"),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
		}),
	)

	err := openConn(t, withSI.Addr).Exec(ctx, "SET async_insert = 1")
	if err == nil {
		t.Fatal("an SI-configured deployment must refuse session-level SET: settings_hash commits to the empty user-settings set")
	}
	if !strings.Contains(err.Error(), "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded") {
		t.Fatalf("SET must be refused with Spec I D1's generic catch-all message, got %v", err)
	}

	if err := openConn(t, withoutSI.Addr).Exec(ctx, "SET async_insert = 1"); err != nil {
		t.Fatalf("without SI configured the legacy pass-through must be unchanged, got %v", err)
	}
}
```

- [ ] **Step 2: Run it**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
FFI=$(go run ./cmd fetch-rewriter-lib --tag <si-engine-tag>)
bazel test //pkg/integration:integration_test \
  --test_filter='TestStorageIntegrityRead_SessionLevelSetIsRefused' \
  --test_env=POLYGLOT_SQL_FFI_PATH=$FFI \
  --test_env=DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}') \
  --test_env=HOME --test_output=all --nocache_test_results
```

Expected: PASS. `<si-engine-tag>` is the rewriter-go tag Spec O pins (≥ v0.9.0). Then prove the assertion is load-bearing by re-running with the pre-Spec-I lib:

```bash
OLD=$(go run ./cmd fetch-rewriter-lib --tag v0.7.1)
bazel test //pkg/integration:integration_test \
  --test_filter='TestStorageIntegrityRead_SessionLevelSetIsRefused' \
  --test_env=POLYGLOT_SQL_FFI_PATH=$OLD \
  --test_env=DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}') \
  --test_env=HOME --test_output=all --nocache_test_results
```

Expected: **FAIL** at `an SI-configured deployment must refuse session-level SET` (v0.7.1 has no catch-all), or at the D5 startup probe if PR #141's probe is in the branch — either is valid evidence. Record whichever you get.

- [ ] **Step 3: Record the decision in Spec I D6**

In `docs/superpowers/specs/2026-08-19-storage-integrity-surface-failclosed-design.md`, extend D6's decision record (`§3 D6`, the paragraph at `:46`) with one line, unwrapped:

```markdown
**Session-level `SET` (recorded 2026-08-25, Spec P D2).** A consequence of D1's catch-all that is now a contract, not an accident: with `storage_integrity.tables` non-empty a session-level `SET` is refused with D1's generic message, because `settings_hash` commits to the empty user-settings set and a `SET` issued before an SI `INSERT` would be invisible to both the agent signer and the server ingress. The same sessions D6 exempts from SI rewrite — peer-trusted, forwarded, maintenance and platform-operator — can still issue `SET`, which is part of what the documented network-isolation requirement is protecting. Pinned by `TestStorageIntegrityRead_SessionLevelSetIsRefused` and by corpus case `si_set_statement_rejected`.
```

- [ ] **Step 4: Record it in `CLAUDE.md`**

In `CLAUDE.md`, in the storage-integrity paragraph of "**4. SQL rewriter is a pluggable backend**", append to the sentence describing SI rejections:

```markdown
With a configured SI table set a session-level `SET` is also refused (it is modelled by no handler in either engine, so it reaches the catch-all): `settings_hash` commits to the empty user-settings set, and an unsigned session setting would otherwise change how a signed statement executes. Peer-trusted, forwarded, maintenance and platform-operator sessions bypass SI rewrite and can still issue `SET` — the recorded Spec I D6 exemption, mitigated by network isolation.
```

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/integration/storage_integrity_read_test.go CLAUDE.md \
  docs/superpowers/specs/2026-08-19-storage-integrity-surface-failclosed-design.md
git commit -m "test(storage-integrity): record and pin the session-level SET refusal (Spec P D2)"
```

**Verification command:** `bazel test //pkg/integration:integration_test --test_filter='TestStorageIntegrityRead' --test_env=POLYGLOT_SQL_FFI_PATH=$FFI --test_env=DOCKER_HOST --test_env=HOME --test_output=errors`

### Task 10: D3 — the owned-key set proved end to end on an ingress-configured deployment

> **Spec P D3 is inverted and this task implements the corrected property.** D3 says *"a client-supplied `SQL_x_read_mode` in query settings is rejected by `RejectUserSettings`"*. It is not: `ReadModeSettingKey` is a **member** of `housegateOwnedSettingKeys` (`pkg/storageintegrity/settings.go:42`), so `RejectUserSettings` accepts it — that is exactly what Spec K D6 added it for. And `RejectUserSettings` runs only on the signed-lane INSERT admission (`pkg/plugins/storageintegrity/plugin.go:239`, `pkg/plugins/sistatement/plugin.go:143`); a read never reaches it, because `storageIntegrityKindFromSQL`'s `readLikePattern` classifies it as not-handled. The untested property Spec K D6 actually created is the two-sided one: on an ingress-configured deployment a signed INSERT carrying `SQL_x_read_mode` must be **admitted**, and one carrying any non-owned key must be **refused**. Both directions get a removal check.

**Files:**
- Modify: `pkg/integration/storage_integrity_agent_test.go` (the owned-key end-to-end pair)
- Modify: `pkg/integration/storage_integrity_read_test.go` (the ingress-configured read variant)

**Interfaces:**
- Consumes: the existing `TestStorageIntegrity_AgentSignsEnvelopeV2EndToEnd` fixture — `capturingConsumer`, `siAgentSchema`, `withDeclaredSchema`, `authProxyConfig`, `testenv.WithRewriterMock`. It uses the rewriter **mock**, not the native engine, so this task needs no FFI lib and is independent of Specs N and O.

- [ ] **Step 1: Write the failing end-to-end pair**

Append to `pkg/integration/storage_integrity_agent_test.go`. Factor the server+agent construction out of `TestStorageIntegrity_AgentSignsEnvelopeV2EndToEnd` into a `startSIAgentPair(t, networkID) (*testenv.TestProxy, *capturingConsumer)` helper first, so both tests share exactly one fixture and a drift in one cannot silently diverge from the other.

```go
// TestStorageIntegrity_OwnedSettingKeysEndToEnd is Spec P D3. Spec K D6 added
// SQL_x_read_mode to the enumerated owned key set so a client may still choose
// its read mode on an SI-configured deployment; the enumeration (not a
// SQL_x_ / SQL_sentio_ prefix) is what keeps every OTHER client setting off the
// signed lane, because settings_hash commits to the empty user-settings set.
// Neither direction had an end-to-end proof.
func TestStorageIntegrity_OwnedSettingKeysEndToEnd(t *testing.T) {
	const networkID = "itest-net-settings"
	agentProxy, consumer := startSIAgentPair(t, networkID)
	conn := openConnNoCompression(t, agentProxy.Addr)

	// An owned key rides through: the lane admits it and still signs the
	// empty-settings digest.
	ownedCtx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"SQL_x_read_mode": clickhouse.CustomSetting{Value: "safe"},
	}))
	batch, err := conn.PrepareBatch(ownedCtx, "INSERT INTO "+chEnv.Database+".si_events")
	if err != nil {
		t.Fatalf("PrepareBatch with SQL_x_read_mode: %v", err)
	}
	if err := batch.Append(uint64(10), "eu"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("an owned setting key must not block the signed lane: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		consumer.mu.Lock()
		n := len(consumer.seen)
		consumer.mu.Unlock()
		if n == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	consumer.mu.Lock()
	seen := append([]siplugin.Admission(nil), consumer.seen...)
	consumer.mu.Unlock()
	if len(seen) != 1 {
		t.Fatalf("the ingress admitted %d statements, want 1", len(seen))
	}

	// A non-owned key is refused, naming itself.
	userCtx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"async_insert": 1,
	}))
	batch, err = conn.PrepareBatch(userCtx, "INSERT INTO "+chEnv.Database+".si_events")
	if err == nil {
		err = batch.Append(uint64(11), "us")
		if err == nil {
			err = batch.Send()
		}
	}
	if err == nil {
		t.Fatal("a non-owned client setting must be refused on the signed lane")
	}
	if !strings.Contains(err.Error(), "async_insert") {
		t.Fatalf("the refusal must name the setting, got %v", err)
	}
	consumer.mu.Lock()
	n := len(consumer.seen)
	consumer.mu.Unlock()
	if n != 1 {
		t.Fatalf("the refused statement must not be admitted; consumer saw %d", n)
	}
}
```

Add `clickhouse "github.com/ClickHouse/clickhouse-go/v2"` to the file's imports.

- [ ] **Step 2: Add the ingress-configured read variant**

Spec P D3's remaining clause — *"and that the configured default still applies"* — is a read-side property and belongs on the read test, which currently runs with **no** ingress configured, so `RejectUserSettings` never executes anywhere in it. Turn the ingress on there and re-assert the read behaviour is unchanged. In `pkg/integration/storage_integrity_read_test.go`'s `TestStorageIntegrityRead_SafeAndUnsafeLatest`, add to the `WithConfigMutator` block:

```go
			// Spec P D3: the read-mode surface must behave identically with the
			// signed ingress configured. Without this the only read-mode
			// integration test runs on a deployment where RejectUserSettings
			// is never reached, so SQL_x_read_mode's owned-key membership
			// (Spec K D6) is never exercised end to end.
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.NetworkID = "itest-net-read"
```

The existing assertions (`mustCount("", 0)` for the configured default, `mustCount("safe", …)`, `mustCount("unsafe_latest", …)`, and the invalid-mode rejection at `:142`) then all run on an ingress-configured proxy. If `Ingress.Enabled` requires `AllowedAddresses` or a schema source to pass `Config.Validate`, supply the same minimal values the agent test uses rather than weakening validation.

- [ ] **Step 3: Run both, then prove each direction is load-bearing**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
export DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}')
bazel test //pkg/integration:integration_test \
  --test_filter='TestStorageIntegrity_OwnedSettingKeysEndToEnd|TestStorageIntegrity_AgentSignsEnvelopeV2EndToEnd' \
  --test_env=DOCKER_HOST --test_env=HOME --test_output=all --nocache_test_results
```

Expected: PASS. Then two removal checks, each reverted immediately:

1. **`RejectUserSettings` removal** — comment out `sicore.RejectUserSettings(...)` at **both** `pkg/plugins/sistatement/plugin.go:143` and `pkg/plugins/storageintegrity/plugin.go:239` (removing only one leaves the other catching it, so a single-site removal is not a valid check) and re-run. Expected: **FAIL** at `a non-owned client setting must be refused on the signed lane`.
2. **Owned-key removal** — delete `ReadModeSettingKey: true` from `housegateOwnedSettingKeys` (`pkg/storageintegrity/settings.go:42`) and re-run. Expected: **FAIL** at `an owned setting key must not block the signed lane`, plus `pkg/storageintegrity`'s own `TestRejectUserSettingsAcceptsEveryOwnedKeyTogether` and `pkg/rewriter`'s `read_mode_key_test.go`.

Record both failure lines. Revert both edits and re-run to green.

- [ ] **Step 4: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/integration/storage_integrity_agent_test.go pkg/integration/storage_integrity_read_test.go
git commit -m "test(storage-integrity): prove the owned-setting-key set end to end on the ingress lane (Spec P D3)"
```

**Verification command:** `bazel test //pkg/integration:integration_test --test_filter='TestStorageIntegrity' --test_env=DOCKER_HOST --test_env=HOME --test_output=errors`

### Task 10b: Part B close-out

- [ ] **Step 1: Full Bazel gate**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
bazel build //... && bazel test //... --test_output=errors
```

Expected: identical to Task 0's baseline plus the new tests.

- [ ] **Step 2: Docker suite**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
FFI=$(go run ./cmd fetch-rewriter-lib --tag <si-engine-tag>)
bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test \
  --test_env=POLYGLOT_SQL_FFI_PATH=$FFI \
  --test_env=DOCKER_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}') \
  --test_env=HOME --test_output=errors
```

Expected: pass. Apply the main-baseline rule before calling any integration failure a regression: `git stash push`, re-run that single test with `--runs_per_test=10` on clean `main`, `git stash pop`, record the ratio.

- [ ] **Step 3: Open the PR**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
gh pr create --fill --title "fix(storage-integrity): unsafe-only exact part reads, SET and owned-key proofs (Spec P D2/D3/D4)"
```

The PR body must carry Task 6's enumeration verdict, Task 8's pre-fix row-count failure line, Task 9's pre-fix failure line and which Spec O gate option was taken, and Task 10's two removal-check failure lines.

---

## Part C — rewriter-go and rewriter-grpc (the shared corpus)

### Task 11: D2 — the `SET` corpus case (rewriter-go, authoritative copy)

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-go`

**Files:**
- Modify: `internal/harness/testdata/storage_integrity_cases.json` (exactly one new case)

**Interfaces:**
- Produces: corpus case `si_set_statement_rejected`, consumed byte-for-byte by Task 12 and referenced by Task 9's Spec I D6 decision record.

> **Oracle warning:** `TestStorageIntegrityGolden` diffs against the C++ oracle when `REWRITER_ORACLE_ADDR` is set. The C++ copy is not updated until Task 12, so **unset `REWRITER_ORACLE_ADDR`** for every command in this task.

- [ ] **Step 1: Confirm the gap is real and record the pre-change state**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
grep -c '"sql": "SET' internal/harness/testdata/storage_integrity_cases.json || echo "0 SET cases — the gap Spec P 1b describes"
ls internal/handlers/ | grep -i set || echo "no SET handler — SET reaches the catch-all"
BEFORE=$(python3 -c "import json;print(len(json.load(open('internal/harness/testdata/storage_integrity_cases.json'))))")
echo "cases before: $BEFORE"
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json
```

Expected: zero `SET` cases; no `set.go` in `internal/handlers/` (the handlers are `dblevel`, `describe`, `exists`, `grant`, `options`, `select`, `storage_integrity*`, `writes`); `$BEFORE` recorded for Step 3.

- [ ] **Step 2: Append the case**

Insert before the closing `]` of the JSON array (add a comma after the previous last object). The `dynamic` block is copied verbatim from the existing `si_unmodelled_statement_rejected` case so the two differ only in `sql` and `name`:

```json
  {
    "name": "si_set_statement_rejected",
    "sql": "SET async_insert = 1",
    "dynamic": {
      "database_map": {"db1": "phys", "other": "phys"},
      "known_physical_databases": ["phys"],
      "delim": "_",
      "storage_integrity": {
        "tables": {"db1.t": {"safe_table": "hg_safe.db1__t", "unsafe_table": "hg_unsafe.db1__t"}},
        "read_mode": "SAFE",
        "reserved_row_id_column": "_hg_row_id"
      }
    },
    "want_code": "UnsupportedStatement", "want_stmt": "", "reject": true,
    "want_message_contains": "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"
  }
```

Note what the case is *for*: it names nothing storage-integrity, and that is the point — under an active SI contract the engines must refuse a statement class they do not model even when it addresses no SI object, because `settings_hash` commits to the empty user-settings set and a session `SET` would change how a later signed statement executes without either signer seeing it. It is the D1 catch-all's generic message, not an SI-object-naming one, so D2's annotator must **not** rewrite it.

- [ ] **Step 3: Run it — and branch on the outcome**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make ffi >/dev/null && \
  env -u REWRITER_ORACLE_ADDR POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib \
  go test ./internal/harness/ -run 'TestStorageIntegrityGolden/si_set_statement_rejected' -v
AFTER=$(python3 -c "import json;print(len(json.load(open('internal/harness/testdata/storage_integrity_cases.json'))))")
python3 -c "
import json,sys
d=json.load(open('internal/harness/testdata/storage_integrity_cases.json'))
names=[c['name'] for c in d]
assert len(names)==len(set(names)), 'duplicate case name'
print('cases after:', len(d))
"
```

- **PASS** is the expected outcome: it confirms Spec P 1b's premise — the refusal is real today and this case converts the accident into a contract. `$AFTER` must equal `$BEFORE + 1`.
- **FAIL** is a finding, not a bug in this case: it means `SET` does **not** reach the catch-all under an active SI contract, so Spec P 1b's whole premise is wrong and D2 becomes a *fix*, not a *record*. Stop, record the actual `code`/`message` the engine returned, and escalate — the settings-hash hole would then still be open and belongs in Spec O's blast radius, not in a documentation task.

- [ ] **Step 4: Full suite and commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
env -u REWRITER_ORACLE_ADDR make test
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json
git add internal/harness/testdata/storage_integrity_cases.json
git commit -m "test(storage-integrity): pin the SET refusal under an active SI contract (Spec P D2)"
```

Record the new count and sha256 — Task 12 asserts byte-identity against them, and `rewriter-grpc`'s `require_test_count` line needs the count. If Spec N landed its corpus cases first, rebase onto them per Global Constraints and re-measure.

**Verification command:** `env -u REWRITER_ORACLE_ADDR make test`

### Task 12: D2 — mirror the corpus into rewriter-grpc and bump its case-count assertion

**Working directory:** `/Users/uranuswch/Dev/housegate/rewriter-grpc`

**Files:**
- Modify: `tests/testdata/storage_integrity_cases.json` (produced by `cp`, never hand-edited)
- Modify: `.github/workflows/ci.yml` (`require_test_count 'SpecG/StorageIntegrityGolden.*' <N>` at `:1381`)

- [ ] **Step 1: Copy and prove byte-identity**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git checkout -b feat/si-set-corpus
cp /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json tests/testdata/storage_integrity_cases.json
shasum -a 256 tests/testdata/storage_integrity_cases.json \
  /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json
```

Expected: the two sha256s are equal and match Task 11 Step 4's value.

- [ ] **Step 2: Rebuild remotely and run the new case**

```bash
# rsync the tree to the build box, then:
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./scripts.sh rebuild && \
   ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_set_statement_rejected'"
```

Expected: PASS with the byte-identical message. If the C++ engine returns a different code or message for `SET`, the two engines diverge on live input — record it, do **not** paper over it with `allow_sql_divergence` (this is a reject case; reject codes and messages are fully compared in C++ today and must match), and escalate to Spec N's cross-engine differential.

- [ ] **Step 3: Bump the case-count assertion**

In `.github/workflows/ci.yml:1381`, set `require_test_count 'SpecG/StorageIntegrityGolden.*' <N>` to the count Task 11 Step 3 printed. That line is Spec J's anti-skip pattern and is the reason a corpus case cannot be silently dropped; leaving it stale would make the whole run red, which is the intended behaviour and is why it must be updated in the same commit as the corpus.

- [ ] **Step 4: Full C++ suite and commit**

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ctest --test-dir build --output-on-failure"
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add tests/testdata/storage_integrity_cases.json .github/workflows/ci.yml
git commit -m "test(storage-integrity): mirror the SET corpus case and bump the count gate (Spec P D2)"
```

**Verification command:** `ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && ctest --test-dir build --output-on-failure"`

---

## Part D — CI honesty

Spec P D6 proposes *"the docker-bound tests move behind an explicit Bazel target (or an explicit `-tags integration` build tag)"*. Reading the three repos' BUILD files first, as instructed, shows **neither is available without a refactor this plan does not take**, in either sibling repo:

- **arbiter-core:** the docker-bound tests share their `go_test` targets with the packages' unit tests — `//snode:snode_test` has 19 test files of which 9 call `requireCH`, `//verifier:verifier_test` has 6 of which 2 do, `//dataplane/ddl:ddl_test` has 7 of which 2 do. Tagging those targets `manual` would remove most of the repo's unit coverage from the `bazel test //...` merge gate.
- **arbiter:** `//integration/chpipeline:chpipeline_test` is *almost* purely docker-bound — 8 of its 9 test functions go through `requireCH` — but `TestSignStatementV2AtMatchesRelaySigner` (`integration/chpipeline/harness_ops_test.go:176`) is a pure signing-drift unit test with no ClickHouse dependency, and it is exactly the kind of test that must stay in the merge gate. Moving it out means exporting `signStatementV2At` / `statementV2PayloadAt` across a package boundary.
- **a Go `integration` build tag** is worse in both: `requireCH` is defined in the same file as the CH tests and is called from 13 files across the three arbiter-core packages, several of which also hold pure unit tests, so the tag would have to be applied per-function (impossible) or per-file (drops unit tests). It would additionally need `# gazelle:build_tags integration` plus `--@rules_go//go/config:tags=integration` on every invocation.

The decision this plan takes instead — same purpose, no refactor: **derive the expected acceptance-test set from the source and assert it in the integration job**, using rewriter-grpc's `require_test_count` discipline (Spec J) and sentio-node's existing grep guards as the two halves of one script. A self-skipped acceptance, or a filter that matches fewer tests than the source declares, goes red. Record the rejected options and the reason in each repo's script header so the next reader does not re-litigate it.

### Task 13: D6 — sentio-node stops pinning one test name

**Working directory:** `/Users/uranuswch/Dev/sentio_xyz/sentio-node`

**Files:**
- Modify: `standalone/storage_integrity_drift_ch_test.go` (rename the test function)
- Create: `scripts/ci/require-ch-tests.sh`
- Modify: `.github/workflows/ci.yml` (the `storage-integrity protocol-table drift` step, `:105-129`)

**Interfaces:**
- Produces: the naming convention `TestStorageIntegrityCH*` for every `SENTIO_SI_CH_E2E`-gated test, enforced mechanically by the script rather than by comment.

- [ ] **Step 1: Establish the convention**

Rename `TestStorageIntegrityProtocolTableDriftFailsBootstrap` → `TestStorageIntegrityCHProtocolTableDriftFailsBootstrap` in `standalone/storage_integrity_drift_ch_test.go:69` and in its doc comment at `:59`. Add to the doc comment:

```go
// The TestStorageIntegrityCH prefix is load-bearing: CI filters on
// TestStorageIntegrityCH.* and scripts/ci/require-ch-tests.sh fails the build
// if any SENTIO_SI_CH_E2E-gated function does not carry it, so a ClickHouse
// acceptance added later cannot silently escape the job (Spec P D6).
```

Note why a broader `TestStorageIntegrity.*` filter — the shape Spec P D6 suggests — is the wrong choice here: `grep -c '^func TestStorageIntegrity' standalone/*_test.go` currently returns **16**, of which 15 are pure unit tests already covered by the `bazel test //...` merge gate. Filtering on that prefix would drag 15 unit tests into the docker job and make any count assertion churn on every new unit test, which is how a count assertion stops being read.

- [ ] **Step 2: Write the guard script**

Create `scripts/ci/require-ch-tests.sh`:

```bash
#!/usr/bin/env bash
# Spec P D6: a skipped ClickHouse acceptance must not be reported as a pass.
#
# Rejected alternatives, recorded so they are not re-litigated: a Bazel
# `manual` tag would have to be applied to //standalone:standalone_test, which
# is overwhelmingly unit tests; a Go `integration` build tag would have to be
# applied per-file to a file that also holds shared helpers. Deriving the
# expected set from the source instead costs nothing and cannot go stale.
set -euo pipefail

log="${1:?usage: require-ch-tests.sh <bazel test log>}"
prefix="TestStorageIntegrityCH"

# Every SENTIO_SI_CH_E2E-gated test function must carry the prefix the CI
# --test_filter uses, or it would silently never run.
gated="$(
  awk '/^func Test/ { fn = $2; sub(/\(.*/, "", fn) }
       /SENTIO_SI_CH_E2E/ && fn != "" { print fn; fn = "" }' standalone/*_test.go | sort -u
)"
if [ -z "$gated" ]; then
  echo "error: no SENTIO_SI_CH_E2E-gated test functions found; this guard has lost its subject" >&2
  exit 1
fi
expected=0
while read -r fn; do
  case "$fn" in
    ${prefix}*) ;;
    *) echo "error: $fn is gated on SENTIO_SI_CH_E2E but does not match the CI filter ${prefix}.*" >&2; exit 1 ;;
  esac
  expected=$((expected + 1))
done <<< "$gated"
echo "expected ClickHouse acceptance tests (${expected}):"
echo "$gated" | sed 's/^/  /'

if grep -Fq -- '--- SKIP: ' "$log"; then
  echo "error: a ClickHouse acceptance self-skipped; a skip is not a pass" >&2
  grep -F -- '--- SKIP: ' "$log" >&2
  exit 1
fi
if grep -Fq -- 'no tests to run' "$log"; then
  echo "error: the --test_filter matched no tests" >&2
  exit 1
fi
passed=0
while read -r fn; do
  if ! grep -Fq -- "--- PASS: ${fn} (" "$log"; then
    echo "error: no PASS marker for ${fn}" >&2
    exit 1
  fi
  passed=$((passed + 1))
done <<< "$gated"
if [ "$passed" -ne "$expected" ]; then
  echo "error: expected exactly ${expected} passing ClickHouse acceptance tests, saw ${passed}" >&2
  exit 1
fi
echo "all ${expected} ClickHouse acceptance tests ran and passed"
```

`chmod +x scripts/ci/require-ch-tests.sh`.

- [ ] **Step 3: Rewrite the CI step**

In `.github/workflows/ci.yml`, replace the body of the `storage-integrity protocol-table drift` step (`:105-129`) — keeping the `SENTIO_SI_CH_E2E: "1"` env block above it:

```yaml
        run: |
          set -euo pipefail
          si_log="$(mktemp)"
          trap 'rm -f "${si_log}"' EXIT
          bazel test //standalone:standalone_test \
            --test_filter='TestStorageIntegrityCH.*' \
            --test_env=SENTIO_SI_CH_E2E \
            --test_env=CH_ADDR \
            --nocache_test_results \
            --test_arg=-test.v \
            --test_timeout=900 \
            --test_output=all 2>&1 | tee "${si_log}"
          bash scripts/ci/require-ch-tests.sh "${si_log}"
```

The three guards Spec J added are preserved — they now live inside the script and apply to **every** gated test rather than to one pinned name, which is the whole finding (Spec P 1f).

- [ ] **Step 4: Prove the guard fails against the unfixed configuration**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
# 1. A gated test that escapes the filter must be caught.
sed -i.bak 's/func TestStorageIntegrityCHProtocolTableDriftFailsBootstrap/func TestSomeOtherCHAcceptance/' \
  standalone/storage_integrity_drift_ch_test.go
bash scripts/ci/require-ch-tests.sh /dev/null; echo "exit=$?"
mv standalone/storage_integrity_drift_ch_test.go.bak standalone/storage_integrity_drift_ch_test.go

# 2. A skipped acceptance must be caught.
printf -- '--- SKIP: TestStorageIntegrityCHProtocolTableDriftFailsBootstrap (0.00s)\n' > /tmp/skip.log
bash scripts/ci/require-ch-tests.sh /tmp/skip.log; echo "exit=$?"

# 3. A run with no PASS marker must be caught.
: > /tmp/empty.log
bash scripts/ci/require-ch-tests.sh /tmp/empty.log; echo "exit=$?"
```

Expected: all three print an `error:` line and `exit=1`. Record them — they are the proof the guard is load-bearing, which the pinned-name version was not.

- [ ] **Step 5: Run the real job locally and commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
bazel test //... --test_output=errors      # the rename must not break the merge gate
git add standalone/storage_integrity_drift_ch_test.go scripts/ci/require-ch-tests.sh .github/workflows/ci.yml
git commit -m "ci: assert every ClickHouse acceptance ran, not one pinned name (Spec P D6)"
```

**Verification command:** `bazel test //... --test_output=errors` plus the three `require-ch-tests.sh` negative cases from Step 4.

### Task 14: D6 — arbiter's ClickHouse pipeline job asserts its tests ran

**Working directory:** `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Create: `scripts/ci/require-ch-tests.sh`
- Modify: `.github/workflows/ci.yml` (the `data-plane integration (docker ClickHouse)` step)

- [ ] **Step 1: Record why the `manual` tag was rejected**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
grep -rn "^func Test" integration/chpipeline/*_test.go
grep -ln "requireCH" integration/chpipeline/*_test.go
```

Expected: 9 test functions; 8 reach ClickHouse through `requireCH`, and `TestSignStatementV2AtMatchesRelaySigner` (`harness_ops_test.go:176`) does not — it pins the test signer against `auth.RelaySigner` and has no ClickHouse dependency. Tagging `//integration/chpipeline:chpipeline_test` `manual` would drop it from `bazel test //...`. Record this in the script header; do not move the test.

- [ ] **Step 2: Write the guard script**

Create `scripts/ci/require-ch-tests.sh` — the same shape as sentio-node's, deriving the expected set from `requireCH` call sites rather than an env-var reference, because arbiter's gate is inside that helper:

```bash
#!/usr/bin/env bash
# Spec P D6: a skipped ClickHouse acceptance must not be reported as a pass.
# Go reports a t.Skip as PASS, so a locally green `bazel test //...` is not
# evidence that the docker acceptance ran.
#
# Rejected alternative, recorded so it is not re-litigated: tagging
# //integration/chpipeline:chpipeline_test `manual` would also drop
# TestSignStatementV2AtMatchesRelaySigner (harness_ops_test.go), a pure
# signing-drift unit test that belongs in the merge gate.
set -euo pipefail

log="${1:?usage: require-ch-tests.sh <bazel test log>}"
shift
pkgs=("$@")
[ "${#pkgs[@]}" -gt 0 ] || pkgs=(integration/chpipeline)

gated=""
for pkg in "${pkgs[@]}"; do
  gated="${gated}$(
    awk '/^func Test/ { fn = $2; sub(/\(.*/, "", fn) }
         /requireCH\(/ && fn != "" { print fn; fn = "" }' "${pkg}"/*_test.go
  )
"
done
gated="$(printf '%s' "$gated" | sed '/^$/d' | sort -u)"
if [ -z "$gated" ]; then
  echo "error: no requireCH-gated test functions found in ${pkgs[*]}; this guard has lost its subject" >&2
  exit 1
fi
if grep -Fq -- '--- SKIP: ' "$log"; then
  echo "error: a ClickHouse acceptance self-skipped; a skip is not a pass" >&2
  grep -F -- '--- SKIP: ' "$log" >&2
  exit 1
fi
n=0
while read -r fn; do
  if ! grep -Fq -- "--- PASS: ${fn} (" "$log"; then
    echo "error: no PASS marker for ${fn}" >&2
    exit 1
  fi
  n=$((n + 1))
done <<< "$gated"
echo "all ${n} ClickHouse acceptance tests ran and passed"
```

`chmod +x scripts/ci/require-ch-tests.sh`.

- [ ] **Step 3: Wire it into the integration job**

In `.github/workflows/ci.yml`, in the `data-plane integration (docker ClickHouse)` step, capture the log and assert:

```yaml
        run: |
          set -euo pipefail
          ch_log="$(mktemp)"
          trap 'rm -f "${ch_log}"' EXIT
          ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:9000 \
            bazel test //integration/chpipeline:chpipeline_test \
              --test_env=ARBITER_CH_INTEGRATION \
              --test_env=CH_ADDR \
              --test_env=DA_STORE_BIN \
              --nocache_test_results \
              --test_arg=-test.v \
              --test_timeout=900 \
              --test_output=all 2>&1 | tee "${ch_log}"
          bash scripts/ci/require-ch-tests.sh "${ch_log}" integration/chpipeline
```

Leave the `anchor-anvil` job alone: `//anchor/evm:evm_test`'s `TestAnvilE2E` self-skips in `bazel test //...` the same way, but it is outside storage integrity's blast radius and Spec P does not scope it. Note it in the PR body as the same class of dishonesty, un-addressed by choice.

- [ ] **Step 4: Prove the guard fails against a skipped run**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel test //integration/chpipeline:chpipeline_test --test_arg=-test.v --test_output=all \
  --nocache_test_results 2>&1 | tee /tmp/arbiter-noch.log || true
bash scripts/ci/require-ch-tests.sh /tmp/arbiter-noch.log integration/chpipeline; echo "exit=$?"
```

Expected: the bazel run reports **PASS** (every test self-skipped without `ARBITER_CH_INTEGRATION`) while the script prints `error: a ClickHouse acceptance self-skipped; a skip is not a pass` and `exit=1`. That contrast — a green bazel target and a red guard over the same log — is the exact finding of Spec P 1f. Record both lines.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git add scripts/ci/require-ch-tests.sh .github/workflows/ci.yml
git commit -m "ci: fail when a ClickHouse acceptance self-skips (Spec P D6)"
```

**Verification command:** the Step 4 contrast reproduces (bazel green, guard red), and `bazel test //... --test_output=errors` is unchanged from Task 0's baseline.

### Task 15: D6 — arbiter-core's data-plane job asserts its tests ran

**Working directory:** `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

**Files:**
- Create: `scripts/ci/require-ch-tests.sh`
- Modify: `.github/workflows/ci.yml` (the `data-plane integration` step)

- [ ] **Step 1: Record why the `manual` tag was rejected here too**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
for d in snode verifier dataplane/ddl; do
  echo "$d: $(ls $d/*_test.go | wc -l) test files, $(grep -ln requireCH $d/*_test.go | wc -l) with requireCH"
done
```

Expected: `snode: 19 / 9`, `verifier: 6 / 2`, `dataplane/ddl: 7 / 2`. All three `go_test` targets mix docker-bound and unit tests, so `manual` would strip the bulk of the repo's unit coverage out of the `bazel test --build_tests_only --@rules_go//go/config:race //...` merge gate. Record it in the script header.

Note while you are here, and do not change it: `verifier/scanner_test.go:1` carries a `//go:build !skip` constraint. It is unrelated to this work and is not the `integration` tag pattern; leave it and mention it in the PR body so the next reader does not mistake it for a partial implementation of D6.

- [ ] **Step 2: Copy the guard script from arbiter and generalise the package list**

Create `scripts/ci/require-ch-tests.sh` — identical to Task 14's, with the header's rejected-alternative paragraph rewritten for this repo (mixed unit/docker `go_test` targets) and the default package list `(snode verifier dataplane/ddl)`.

- [ ] **Step 3: Wire it into the `data-plane integration` step**

```yaml
        run: |
          set -euo pipefail
          ch_log="$(mktemp)"
          trap 'rm -f "${ch_log}"' EXIT
          bazel test \
            //dataplane/ddl:ddl_test \
            //snode:snode_test \
            //verifier:verifier_test \
            --test_env=ARBITER_CH_INTEGRATION \
            --test_env=ARBITER_CH_KEEPER \
            --test_env=ARBITER_CH_REPLICA \
            --test_env=CH_ADDR \
            --test_env=CH_REPLICA_ADDR \
            --nocache_test_results \
            --test_arg=-test.v \
            --test_timeout=900 \
            --test_output=all 2>&1 | tee "${ch_log}"
          bash scripts/ci/require-ch-tests.sh "${ch_log}" snode verifier dataplane/ddl
```

Two consequences to state in the commit message: the job's output changes from `errors` to `all` (needed for the PASS markers) and its results are no longer cached (`--nocache_test_results`), so it re-runs on every push. Both are the price of the assertion and both match what sentio-node already does.

Some `requireCH`-gated tests additionally gate on `ARBITER_CH_KEEPER` / `ARBITER_CH_REPLICA`. The CI job sets all three, so the derived set is exactly right there. If a test is ever added that gates only on one of the secondary variables, extend the script's awk pattern to include it — the script fails loudly (`no PASS marker for …`) rather than silently, which is the intended failure mode.

- [ ] **Step 4: Prove the guard fails against a skipped run**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
bazel test //snode:snode_test --test_arg=-test.v --test_output=all --nocache_test_results \
  2>&1 | tee /tmp/core-noch.log || true
bash scripts/ci/require-ch-tests.sh /tmp/core-noch.log snode; echo "exit=$?"
```

Expected: bazel reports the target **PASS** while the script prints `error: a ClickHouse acceptance self-skipped` and `exit=1`. Record both.

- [ ] **Step 5: Commit and open both PRs**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
git add scripts/ci/require-ch-tests.sh .github/workflows/ci.yml
git commit -m "ci: fail when a ClickHouse acceptance self-skips (Spec P D6)"
gh pr create --fill --title "ci: fail when a ClickHouse acceptance self-skips (Spec P D6)"
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node && gh pr create --fill --title "ci: assert every ClickHouse acceptance ran (Spec P D6)"
```

Each PR body must carry that repo's bazel-green / guard-red contrast from Step 4, and the recorded reason the `manual` tag and the `integration` build tag were both rejected for that repo.

**Verification command:** the Step 4 contrast reproduces, and `bazel test //... --test_output=errors` is unchanged from Task 0's baseline.

---

## Part E — bookkeeping (last, and never the reason the plan is "done")

Spec P D7 states the rule this part exists to enforce, and it is repeated here because it is the one that gets broken: **a plan whose only remaining unticked items are documentation is finished; a plan whose documentation is ticked while code is not is the failure mode this decision exists to prevent.** Do not start Part E until Parts A–D are merged. If any Part A–D task is still open when Part E's tasks are all ticked, the plan is **not** done — say so explicitly in the final report.

**Working directory for every Part E task:** `/Users/uranuswch/Dev/housegate/housegate`

Every reconciliation task below follows the same procedure, and it is **not** a bulk tick:

1. Read the plan task's **Files** block and confirm each named file exists in the merged tree with the described change (`git log --oneline -- <path>`, then read the region).
2. Run the plan task's own named **verification command**. If it is a remote-box or docker command that cannot be run now, verify the artifact instead (the test function exists and is in a CI-listed target).
3. Tick every step whose work is present and verified.
4. **Untick** every step whose work is absent, and append a one-line `> **Not delivered:** …` note directly under it naming what is missing and where it is tracked. Never delete a step.
5. Where a step was delivered *differently* from its text, leave it ticked and append a one-line `> **Delivered as:** …` note.

### Task 16: D7 — reconcile Spec I's plan and status

**Files:**
- Modify: `docs/superpowers/plans/2026-08-19-storage-integrity-surface-failclosed.md` (140 unticked, 0 ticked, 24 tasks as of 2026-08-25)
- Modify: `docs/superpowers/specs/2026-08-19-storage-integrity-surface-failclosed-design.md` (`Status:` at `:3`)

- [ ] **Step 1: Reconcile the rewriter-go half (Tasks 1-7)**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git log --oneline -5
grep -rn "StorageIntegrityUnmodelledMessage" --include="*.go" . | head -3
ls internal/engine/scope.go internal/engine/lexical.go internal/handlers/storage_integrity_reject.go
python3 -c "import json;d=json.load(open('internal/harness/testdata/storage_integrity_cases.json'));print(len(d))"
env -u REWRITER_ORACLE_ADDR make test
```

Expected: `548a950 fix(storage-integrity): fail closed on unmodelled SQL surfaces (#32)` and `23687cc … close protected SHOW namespace gaps (#33)` are in the log, the three created files exist, and the suite is green. Tick Tasks 1-7's steps accordingly.

- [ ] **Step 2: Reconcile the rewriter-grpc half (Tasks 8-14)**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git log --oneline -5
grep -n "kStorageIntegrityUnmodelledMessage\|annotateStorageIntegrityReject" src/handlers/storage_integrity.h
shasum -a 256 tests/testdata/storage_integrity_cases.json
grep -n "require_test_count" .github/workflows/ci.yml
```

Expected: `83c3a51 fix(storage-integrity): harden fail-closed surface parity (#52)` present; the two symbols declared; the corpus sha equal to rewriter-go's. Tick Tasks 8-14.

- [ ] **Step 3: Reconcile the housegate half (Tasks 16-22) — this is the one that must stay unticked**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
grep -n "rewriter-go" go.mod
ls pkg/rewriter/probe.go 2>/dev/null || echo "probe.go absent"
gh pr view 141 --json state,title,headRefName 2>/dev/null || echo "check PR #141 manually"
```

Expected on `main` today: `github.com/housegate/rewriter-go v0.7.1`, no `pkg/rewriter/probe.go`, PR #141 open. Every step of plan Tasks 16-22 therefore stays **unticked**, each with:

```markdown
> **Not delivered:** housegate `main` still pins rewriter-go v0.7.1 and this work is open in PR #141. Tracked by Spec O (production rollout).
```

If Spec O has merged #141 by the time this task runs, re-run the checks and tick instead — do not tick on the strength of this plan's expectation.

- [ ] **Step 4: Set the status honestly**

In `docs/superpowers/specs/2026-08-19-storage-integrity-surface-failclosed-design.md:3`, replace `**Status:** Proposed` with the two-sided form Spec J's already uses:

```markdown
**Status:** Partially Implemented — both engine halves shipped (rewriter-go `23687cc` v0.9.0, rewriter-grpc `a8ca4e7` v0.13.0+1); the HouseGate half (D3 fail-closed, D5 startup probe, D7e exception scrub) is open in PR #141 and unreachable on `main`, which still pins rewriter-go v0.7.1. Tracked by Spec O.
```

Adjust the tags and PR state to whatever the Step 1-3 checks actually reported.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add docs/superpowers/plans/2026-08-19-storage-integrity-surface-failclosed.md \
  docs/superpowers/specs/2026-08-19-storage-integrity-surface-failclosed-design.md
git commit -m "docs(storage-integrity): reconcile Spec I's plan and status with merged code (Spec P D7)"
```

**Verification command:** `grep -c '^- \[x\]' docs/superpowers/plans/2026-08-19-storage-integrity-surface-failclosed.md` is non-zero and the surviving `- [ ]` steps all carry a `> **Not delivered:**` note — check with `grep -A1 '^- \[ \]' <plan> | grep -c 'Not delivered'` matching the unticked count.

### Task 17: D7 — reconcile Spec K's plan and status

**Files:**
- Modify: `docs/superpowers/plans/2026-08-19-storage-integrity-commitment-durability.md` (111 unticked, 0 ticked, 24 tasks)
- Modify: `docs/superpowers/specs/2026-08-19-storage-integrity-commitment-durability-design.md` (`Status:` at `:3`)

- [ ] **Step 1: Reconcile the arbiter half**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git log --oneline -5
ls fsm/l3_commitment_golden_test.go fsm/testdata
grep -n "goldenStatementsRoot\|goldenHeaderChainHash" fsm/l3_commitment_golden_test.go
grep -n "StatementKind" fsm/state.go fsm/userjws_v2_test.go | head
bazel test //fsm:fsm_test --test_output=errors
```

Expected: `c1d32f6 fix(storage-integrity): harden commitment durability (#19)` present; the D1 goldens exist with their digests inline **and** in a fixture, with no regenerator; D7's `statement_kind` binding present. Tick the corresponding steps.

- [ ] **Step 2: Reconcile the housegate half**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
grep -n "StatementKind uint32" pkg/auth/types.go
grep -rn "SharedStatementVectorsSHA256" --include="*.go" pkg/auth | head -3
grep -n "ReadModeSettingKey" pkg/storageintegrity/settings.go
ls pkg/auth/testdata/statement_jws_v2.json
bazel test //pkg/auth:auth_test //pkg/storageintegrity:storageintegrity_test --test_output=errors
```

Expected: `statement_kind` bound (D7), the shared JWS vectors pinned by `SharedStatementVectorsSHA256`, `SQL_x_read_mode` in the enumerated owned key set (D6 — the decision Task 10 has now proved end to end). Tick.

- [ ] **Step 3: Reconcile the arbiter-core half**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
git log --oneline -3
grep -rln "StatementEnvelope" --include="*_test.go" . | head
bazel test //... --test_output=errors
```

Expected: `32b59a8 fix(storage-integrity): harden durability bindings (#17)`; the mirrored envelope field-order vector present. Tick.

- [ ] **Step 4: Untick what is genuinely absent**

Two candidates known from the 2026-08-25 review; verify each rather than assuming:

- **RFC-8785 canonical JSON** — D1 explicitly *defers* it, so any step describing it should be reworded as recorded debt rather than unticked. Confirm the plan does not claim it was done.
- **sentio-node's consumption** — `sentio-node` pins `arbiter-core v0.4.0` / `arbiter-proto v0.5.0` and consumes none of Spec K. Any plan step that claims a production host bump stays unticked with `> **Not delivered:** sentio-node still pins pre-Spec-K versions; tracked by Spec O D5.`

- [ ] **Step 5: Set the status and commit**

In `docs/superpowers/specs/2026-08-19-storage-integrity-commitment-durability-design.md:3`, replace `**Status:** Proposed` with the line below — or with the two-sided `Partially Implemented` form if Step 4 turned up a real gap:

```markdown
**Status:** Implemented — arbiter `c1d32f6`, arbiter-core `32b59a8`, housegate `6fd56b8`. Not yet consumed by sentio-node, which still pins pre-Spec-K arbiter-core / arbiter-proto; tracked by Spec O.
```

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add docs/superpowers/plans/2026-08-19-storage-integrity-commitment-durability.md \
  docs/superpowers/specs/2026-08-19-storage-integrity-commitment-durability-design.md
git commit -m "docs(storage-integrity): reconcile Spec K's plan and status with merged code (Spec P D7)"
```

**Verification command:** as Task 16 — every surviving `- [ ]` carries a `> **Not delivered:**` note.

### Task 18: D7 — reconcile Spec L's plan and status

**Files:**
- Modify: `docs/superpowers/plans/2026-08-19-storage-integrity-table-backpressure-hardening.md` (164 unticked, 0 ticked, 27 tasks)
- Modify: `docs/superpowers/specs/2026-08-19-storage-integrity-table-backpressure-hardening-design.md` (`Status:` at `:3`)

- [ ] **Step 1: Reconcile the housegate half**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git log --oneline -6
grep -n "RefreshTimeout\|SnapshotTTL" pkg/storageintegrity/parts_pressure.go | head
grep -n "KeepSession" pkg/chproto/*.go | head -3
grep -n "BackpressureError\|252" pkg/storageintegrity/parts_pressure.go | head -5
bazel test //pkg/storageintegrity:storageintegrity_test --test_output=errors
```

Expected: `6fd56b8 feat(storage-integrity): bounded pressure reads and a non-closing 252 (Spec L D3/D6/D7) (#140)` present; `refresh_timeout` / `snapshot_ttl` configurable; `ClientError.KeepSession` present; the aggregate count pass present. Tick D3, D6 and D7's steps.

- [ ] **Step 2: Record D3(b)'s half-closure and its resolution**

This is the one step that must be handled with care rather than ticked. Spec L D3(b) claimed *"`hg_safe` part names are never read"*, and until this plan's **Task 7** that was false — `fullScope()` named the safe database in the exact-name read. Two cases:

- If Task 7 is merged, leave D3(b)'s steps ticked and append the note:

```markdown
> **Completed by:** Spec P D4 (Task 7). Until then the exact-name read still bound `hg_safe`, which moved the growth cliff to startup rather than closing it.
```

- If Task 7 is not merged, **untick** them with the note below and do not proceed to Step 4:

```markdown
> **Not delivered:** the exact-name read still binds `hg_safe`; closed by Spec P D4 Task 7.
```

- [ ] **Step 3: Reconcile the arbiter-core / arbiter / sentio-node halves**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core && git log --oneline -3
cd /Users/uranuswch/Dev/sentio_xyz/arbiter && git log --oneline -3 && grep -rn "protocol-table mode" --include="*.go" cmd/ | head -3
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node && grep -n "housegate\|arbiter-core" go.mod | head -5
```

Expected: arbiter's `2865e2b feat(cmd): derive protocol-table mode from the schema source (Spec L D2) (#18)` present and ticked; sentio-node still pinning pre-Spec-L versions, so any host-bump step stays unticked with the Spec O tracking note.

- [ ] **Step 4: Set the status and commit**

```markdown
**Status:** Implemented — arbiter-core `32b59a8`, arbiter `c1d32f6`, housegate `6fd56b8` plus Spec P D4 (which closed D3(b)'s remaining exact-name read of `hg_safe`). Not yet consumed by sentio-node; tracked by Spec O.
```

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add docs/superpowers/plans/2026-08-19-storage-integrity-table-backpressure-hardening.md \
  docs/superpowers/specs/2026-08-19-storage-integrity-table-backpressure-hardening-design.md
git commit -m "docs(storage-integrity): reconcile Spec L's plan and status with merged code (Spec P D7)"
```

**Verification command:** as Task 16.

### Task 19: D7 — correct Spec J plan Task 14b Step 5

> **Spec P D7 cites the wrong plan.** It says *"Spec L plan Task 14b"*. `Task 14b` exists only in `docs/superpowers/plans/2026-08-19-storage-integrity-verification-restoration.md:3100` — **Spec J's** plan, which is the fully-ticked one (116/116). Spec L's plan has 164 unticked boxes and no Task 14b. `grep -rn "14b" docs/superpowers/plans/` confirms it in one command.

**Files:**
- Modify: `docs/superpowers/plans/2026-08-19-storage-integrity-verification-restoration.md` (Task 14b, `:3100-3255`)
- Modify: `docs/superpowers/specs/2026-08-25-storage-integrity-residual-binding-design.md` (correct the citation in §1g and D7)

- [ ] **Step 1: Establish what was actually delivered**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
grep -rn "storageIntegrityBootPlan\|bootPlan\|funcIdentity\|reflect.ValueOf\|sourceLineOf" --include="*.go" standalone/ \
  || echo "none of the plan's named symbols exist"
grep -n "storageIntegrityBootDeps\|ensureProtocolTables\|crossCheckSchemas\|registerRole" standalone/standalone.go | head
grep -n "TestRunStorageIntegrityProtocolPreflightUsesMandatoryOrder" -A 6 standalone/storage_integrity_bootstrap_test.go
grep -n "parser.ParseFile" standalone/storage_integrity_bootstrap_test.go | head -3
```

Expected: `storageIntegrityBootPlan`, `bootPlan`, `funcIdentity`, `reflect.ValueOf` and `sourceLineOf` are **absent**; what exists instead is `storageIntegrityBootDeps` (`standalone.go:600`) with named methods `ensureProtocolTables` (`:743`), `crossCheckSchemas` (`:750`) and `registerRole` (`:757`); a `storageIntegrityBootOperations` interface the production preflight consumes; `TestRunStorageIntegrityProtocolPreflightUsesMandatoryOrder` asserting `[]string{"ensure", "cross-check"}` from a recorder bound to production dependencies; mode-keyed mandatory-dependency validation; and a `go/parser` AST audit of `standalone.go` in place of the planned `sourceLineOf` string scan.

- [ ] **Step 2: Rewrite Step 5's text in place**

Replace Task 14b Step 5's body (`:3237-3239`) with what was delivered, keeping the checkbox ticked because the *property* was delivered:

```markdown
- [x] **Step 5: Prove the binding test is load-bearing**

> **Delivered as:** the `funcIdentity` / `reflect.ValueOf` approach in Steps 1 and 3 was never implemented — `storageIntegrityBootPlan`, `funcIdentity`, `reflect.ValueOf` and `sourceLineOf` do not exist in sentio-node. What shipped instead binds the production preflight to a named `storageIntegrityBootOperations` interface whose methods (`ensureProtocolTables`, `crossCheckSchemas`, `registerRole` on `storageIntegrityBootDeps`, `standalone/standalone.go:600-760`) are recorded in call order by a test recorder, plus mode-keyed mandatory-dependency validation and a `go/parser` AST audit of `standalone.go` that fails if the preflight call site is renamed, removed, or reordered relative to listener startup. That is sound and is **not** the tautology the 2026-08-19 review criticised — swapping two production bindings changes the recorded order and fails `TestRunStorageIntegrityProtocolPreflightUsesMandatoryOrder`. To prove it is load-bearing, swap the `ensure`/`cross-check` bindings in `standalone.go` and re-run `bazel test //standalone:standalone_test --test_filter='TestRunStorageIntegrityProtocolPreflight'`; restore afterwards.
```

Also add a one-line note under Steps 1 and 3 pointing at the same `> **Delivered as:**` paragraph, so a reader who lands on the code blocks does not go looking for symbols that never existed.

- [ ] **Step 3: Correct Spec P's own citation**

In `docs/superpowers/specs/2026-08-25-storage-integrity-residual-binding-design.md`, change both occurrences of "Spec L plan Task 14b" (§1g at `:21` and D7 at `:71`) to "Spec J plan Task 14b", and append to §1g: `(The plan is `2026-08-19-storage-integrity-verification-restoration.md`, Spec J's — Spec L's plan has no Task 14b.)`

- [ ] **Step 4: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add docs/superpowers/plans/2026-08-19-storage-integrity-verification-restoration.md \
  docs/superpowers/specs/2026-08-25-storage-integrity-residual-binding-design.md
git commit -m "docs(storage-integrity): describe what Task 14b actually delivered (Spec P D7)"
```

**Verification command:** `grep -rn "funcIdentity" docs/superpowers/plans/2026-08-19-storage-integrity-verification-restoration.md` returns only the historical code blocks, each now preceded by the `> **Delivered as:**` note; `grep -rn "Spec L plan Task 14b" docs/` returns nothing.

### Task 20: D7 — Spec A §4.2, Spec B's edit list, and closing Spec P

**Files:**
- Modify: `docs/superpowers/specs/2026-08-18-storage-integrity-signed-envelope-v2-design.md` (§4.2, `:73-97`)
- Modify: `docs/superpowers/specs/2026-08-18-storage-integrity-design-v4-reconciliation.md` (§2's edit table)
- Modify: `docs/superpowers/specs/2026-08-25-storage-integrity-residual-binding-design.md` (`Status:`)
- Modify: `docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md` (§2's Spec P row)

- [ ] **Step 1: Add `statement_kind` to Spec A §4.2**

The implementation binds fourteen fields; §4.2's JSON block lists thirteen. Confirm first, then edit:

```bash
cd /Users/uranuswch/Dev/housegate/housegate
grep -n "json:\"" pkg/auth/types.go | sed -n '/JWSStatementPayloadV2/,$p' | head -20
grep -n "StatementKind uint32 \`json:\"statement_kind\"\`" pkg/auth/types.go
```

In §4.2's JSON payload block, add after `"row_id_profile_id": "housegate-row-id-v1"`:

```json
  "statement_kind": 1
```

and extend the paragraph below it: `Spec K D7 additionally binds `statement_kind` (`pb.StatementKind`'s numeric value; 1 = INSERT), derived by the ingress from its own classification rather than taken from the client, so an operator cannot relabel a statement's kind after signing. `EthValidator.ValidateStatementV2` compares it with the other thirteen fields.`

- [ ] **Step 2: Refresh Spec B's edit list**

In `docs/superpowers/specs/2026-08-18-storage-integrity-design-v4-reconciliation.md` §2, the `§7 envelope` row's Edit column says *"**(after A)** replace the envelope block and the signing-payload block verbatim from Spec A §4"*. Update it to name the post-K field set and the two decision records this round added:

Append to the **`§7 envelope`** row's Edit cell:

```markdown
Spec A §4.2 as corrected by Spec K D7 and Spec P D7 — fourteen bound fields including `statement_kind`.
```

Append to the **`§7 settings_hash`** row's Edit cell:

```markdown
and the SI lane refuses session-level `SET` under an active contract (Spec P D2); the sessions that bypass rewrite — peer-trusted, forwarded, maintenance, platform-operator — can still issue it (Spec I D6).
```

Add one new row for the part-pressure read scope:

```markdown
| §12.2 | part-inventory reads | exact part NAMES are read from `hg_unsafe` only; `hg_safe` contributes counts for the growth gauge and never names (Spec P D4) | add one sentence to the throttle description. |
```

Keep every existing row; this is a refresh, not a rewrite.

- [ ] **Step 3: Close Spec P and update the roadmap**

In `2026-08-25-storage-integrity-residual-binding-design.md:3`, `**Status:** Proposed` → `**Status:** Implemented (plan docs/superpowers/plans/2026-08-25-storage-integrity-residual-binding.md)`, and append to §4:

```markdown
**Delivered:** arbiter `<arbiter-sha>` (D1, D5), housegate `<housegate-sha>` (D2 HouseGate half, D3, D4), rewriter-go `<rg-sha>` + rewriter-grpc `<rgrpc-sha>` (D2 corpus, `<N>` cases, sha256 `<corpus-sha>`), sentio-node `<sn-sha>` + arbiter `<arbiter-ci-sha>` + arbiter-core `<core-sha>` (D6), docs (D7). Five corrections to this spec's own source reading are recorded in the plan's "Corrections to Spec P" section.
```

In `2026-08-25-storage-integrity-closure-roadmap.md` §2, change Spec P's Urgency cell from `High — independent of N/O` to `**Shipped** — <arbiter-sha> / <housegate-sha> / <rg-sha> / <rgrpc-sha> / CI (three repos)`, and add a footnote that D2's HouseGate integration test was gated on Spec O, contrary to the roadmap's "independent of N/O" claim.

- [ ] **Step 4: Verify Part E did not become the reason the plan is "done"**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
grep -n '^- \[ \]' docs/superpowers/plans/2026-08-25-storage-integrity-residual-binding.md
```

Expected: **nothing outside Part E**. If any Part A-D step is still unticked, revert this task's status edits, leave Spec P `Proposed`, and report the plan as unfinished. This step is the enforcement of Spec P D7's closing rule and is not optional.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add docs/superpowers/specs/2026-08-18-storage-integrity-signed-envelope-v2-design.md \
  docs/superpowers/specs/2026-08-18-storage-integrity-design-v4-reconciliation.md \
  docs/superpowers/specs/2026-08-25-storage-integrity-residual-binding-design.md \
  docs/superpowers/specs/2026-08-25-storage-integrity-closure-roadmap.md
git commit -m "docs(storage-integrity): bind statement_kind in Spec A, refresh Spec B, close Spec P (Spec P D7)"
```

**Verification command:** `grep -n '^- \[ \]' docs/superpowers/plans/2026-08-25-storage-integrity-residual-binding.md` returns nothing.

---

## Self-review

Run after the plan is written, before execution.

**1. Spec coverage** — see the map below; every decision and every acceptance clause in Spec P §3 and §4 has a task.

**2. Placeholder scan** — no "TBD", no "add error handling", no "similar to Task N", no test described without its code. The intentional `<placeholders>` are release/measurement artifacts that cannot be known before execution and each is produced by a named earlier step: `<si-engine-tag>` (Spec O's rewriter-go pin, checked in Task 9 Step 0), `<N>` / `<corpus-sha>` (Task 11 Steps 1 and 3), and the seven `<…-sha>` values in Task 20 Step 3 (each part's close-out commit).

**3. Type and string consistency**
- `(*FSM).blockStatementsComplete` (Task 1) ↔ its three consumers (Tasks 2, 3, 4). Same `([]*StatementState, bool)` shape everywhere; no variant returns an error except `WorkSet`, which already had one.
- `fsm.ErrL3BlockIncomplete` (`reads.go:19`) is reused by Task 4 rather than a new sentinel, so `server/safestate.go:54`'s existing gRPC mapping covers the new case for free.
- `evidenceBlockN` (Task 3) ↔ its second consumer in Task 4; defined once, in `threeway_test.go`.
- `(*PartsPressureGuard).exactScope` / `.countScope` (Task 7) replace `fullScope` at exactly three call sites, each named in Step 3.
- `PartsScope.IsFull`'s relaxed predicate (Task 7 Step 4) must keep `countScope()` "full" — asserted in Task 7 Step 1's test, not left to reading.
- The D1 catch-all message string is quoted identically in Task 9's integration test, Task 11's corpus case, and Task 9 Step 3's spec text: `storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded`.
- `scripts/ci/require-ch-tests.sh` exists in three repos with the same interface (`<log> [pkg…]`) and three different derivation predicates: `SENTIO_SI_CH_E2E` (sentio-node) and `requireCH(` (arbiter, arbiter-core). The difference is deliberate and stated in each header.
- Corpus case name `si_set_statement_rejected` is referenced by Task 9's Spec I text, Task 11 and Task 12.

**4. Ordering** — Part A and Part C are independent of everything. Part B Task 9 is gated on Spec O (stated in Global Constraints and in Task 9 Step 0); Part B Tasks 6-8 and 10 are not. Part B Task 7 must not start before Task 6 passes. Part D is independent. Part E must be last and Task 20 Step 4 enforces it.

**5. What this plan does not do** — no change to `replay.ReplayJob`, arbiter-proto, `pkg/replay`, the reservation protocol, the `settings_hash` field set, the peer-trust exemption, or any engine handler. D2 and D7 change no production code at all.

## Spec coverage map

| Spec P section | Requirement | Tasks |
|---|---|---|
| §1a / §3 D1 | dispatch path can forge fraud evidence; `blockStatements` drops silently | 1 (helper + classification), 2 (`BlockDispatchInfo`), 3 (`reevaluateBlock`), 4 (`WorkSet` anchor root + `safePrefixLocked`, both unnamed by the spec) |
| §3 D1 acceptance | tampering test mirroring Spec K D3's, failing against today's code | 2 Step 2, 3 Step 2, 4 Step 2 |
| §3 D1 | `blockStatements` survives for the `forEachBlockStatement` callers, with a doc comment naming the distinction | 1 Steps 1 and 4 |
| §1b / §3 D2 | one corpus case, byte-identical in both engines | 11, 12 |
| §3 D2 | one HouseGate integration test: SI configured → refused; not configured → unchanged | 9 Steps 1-2 |
| §3 D2 | Spec I D6 record + `CLAUDE.md` gain the `SET` sentence, naming the bypassing sessions | 9 Steps 3-4 |
| §1c / §3 D3 | read-mode / owned-key rejection proved end to end; must fail if `RejectUserSettings` is removed | 10 (corrected: admit `SQL_x_read_mode`, refuse `async_insert`; both removal checks in Step 3) |
| §3 D3 | the configured default still applies | 10 Step 2 (ingress-configured read variant) |
| §1d / §3 D4 | exact-name scope stops reading the safe database; `IncludeSafeDatabase` stays for `RefreshCounts` | 7 |
| §3 D4 | enumerate every exact-name consumer first; **stop** if any reads a safe name | 6 (gate task, no commit) |
| §3 D4 acceptance | a test asserting the exact read issues no safe-database query | 7 Step 1 (`TestRestoreBatch_NeverReadsSafeDatabasePartNames`) |
| §3 D4 acceptance | a scale test at the 300k-row shape that now completes | 8 Step 2 (row-count invariant at the CI-affordable fixture, with the 300k rationale stated) + 8 Step 3 (pre-fix failure) |
| §1e / §3 D5 | delete the `-update` flag; comment names `ChainHash()`'s dependency through `SpentIDsRootAfter` | 5 |
| §1f / §3 D6 | sentio-node: package-or-prefix filter + `require_test_count`; keep the three anti-skip guards | 13 |
| §1f / §3 D6 | arbiter-core and arbiter: an explicit target/tag listed by name; `ARBITER_CH_INTEGRATION` stays as a second belt | 14, 15 (both record why the `manual` tag and the `integration` build tag were rejected against the repos' actual BUILD files, and implement the same purpose with a source-derived assertion) |
| §1g / §3 D7 | Specs I / K / L: status corrected, checkboxes reconciled against merged code — not bulk-ticked | 16, 17, 18 (each with the five-step procedure; unticked-but-done ticked, ticked-but-not-done unticked with a one-line note) |
| §3 D7 | Task 14b Step 5 rewritten to what was delivered | 19 (and corrects the spec's "Spec L" → "Spec J" mis-citation) |
| §3 D7 | Spec A §4.2 gains `statement_kind`; Spec B's edit list refreshed | 20 Steps 1-2 |
| §3 D7 | bookkeeping is last and may not be the reason the plan is "done" | Part E preamble + 20 Step 4 (mechanical enforcement) |
| §4 | every new guard has a step proving it fails against the unfixed code | 2, 3, 4, 8, 9, 10, 13, 14, 15 |
| §5 | `statement_count` / `statements_root` on `ReplayJob` stays debt | Global Constraints; Task 1's `blockStatementsComplete` doc comment; Task 5b's PR body |
