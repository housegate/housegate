# Storage Integrity: Residual Binding, Verification and Bookkeeping Closure

**Date:** 2026-08-25 **Status:** Proposed **Roadmap:** [closure roadmap](2026-08-25-storage-integrity-closure-roadmap.md) Spec P. **Remediates:** residual findings against [Spec I](2026-08-19-storage-integrity-surface-failclosed-design.md) / [J](2026-08-19-storage-integrity-verification-restoration-design.md) / [K](2026-08-19-storage-integrity-commitment-durability-design.md) / [L](2026-08-19-storage-integrity-table-backpressure-hardening-design.md) from the 2026-08-25 verification review. **Code base:** arbiter `c1d32f6` (v0.3.0), arbiter-core `32b59a8` (v0.5.1), housegate `6fd56b8` (v0.11.0), sentio-node `58f5e5f`. **Source of truth:** English version.

## 1. Problem

Each item below is a gap the previous specs either half-closed or closed in one code path and not its sibling. None is a blocker for the rollout in Spec O; two are integrity-relevant enough that they should not wait for another review cycle to be noticed again.

**1a — the consensus dispatch path can forge fraud evidence against an honest source (High).** Spec K D3 added a completeness assertion to the audit read (`fsm/reads.go:145` returns `ErrL3BlockIncomplete` when `len(envs) != header.StatementCount`) and proved it with a real snapshot-tampering test. The dispatch path did not get the same treatment. `fsm/apply.go:87-100`'s `blockStatements` silently drops a statement seq that is missing from `f.st.Statements`, and `fsm/reads_dispatch.go:37-62` builds `BlockDispatchInfo` straight from that short list: `SourceClaimRoot` becomes `blockStateRoot(stmts)` — the real accumulated root of the *last surviving* statement — while `info.Statements` is short. `orchestrator/dispatch.go:54` hands both to `replay.ReplayJob`, which carries neither `statement_count` nor `statements_root`, so the verifier has nothing to cross-check against. A verifier replaying that job computes a state root over fewer statements than the source executed, mismatches, and — per the replay design's C.4 rule that a source-root mismatch is *signed, not errored* — produces a non-repudiable attestation of fraud against a source that did nothing wrong. A corrupted or partially-restored reader is enough; no malice is required.

**1b — session-level `SET` and `settings_hash` (Medium).** The signed statement lane hashes an enumerated owned key set (`pkg/storageintegrity/settings.go`), and a session-level `SET async_insert=1` issued before an SI `INSERT` is invisible to both the agent signer and the server ingress, which honestly sign `EmptySettingsHash` for a statement that then executes under different settings. Spec I D1's catch-all closes this incidentally — `SET` is modelled by no handler in either engine, so under an active SI contract it reaches the catch-all and returns `UnsupportedStatement`, which PR #141's D3 turns into a rejection. That is the right outcome, but it is currently an *unintended consequence with no test*: the corpus has no `SET` case, no integration test asserts it, and nothing in the spec set records that SI-configured deployments refuse session-level `SET`. An accidental property is one refactor away from being an accidental regression. The exemption also has a real edge: peer-trusted, forwarded, maintenance and platform-operator sessions skip rewrite, so they can still `SET` — the same sessions Spec I D6 already exempted, but D6's record does not name `SET`.

**1c — `SQL_x_read_mode` has no end-to-end proof (Medium).** Spec K D6 added `SQL_x_read_mode` to the owned key set, but the only read-mode integration test runs without the ingress configured, so `RejectUserSettings` never executes in it. The rejection path that stops a client from choosing its own read mode is untested end to end.

**1d — Spec L D3(b)'s growth cliff moved rather than closed (Medium).** The spec says `hg_safe` part names are never read. `PartsPressureGuard.fullScope()` (`pkg/storageintegrity/parts_pressure.go:324-330`) still includes the safe database, and `fullScope()` is used by `Refresh`, by the startup path and by `RestoreBatch` (`:337`, `:367`, `:678`). At the spec's own stated scale — 10 tables × 12 partitions × 2500 parts ≈ 300k rows — that read times out, `restoreBlocked` latches, and the node fails to start. Fail-fast and visible with a configurable `refresh_timeout` is a genuine improvement over the old silent behaviour, but the structural cause remains. The reviewer's observation is the fix: every consumer of exact part *names* keys on `UnsafeDatabase` only, so the safe database's names are provably dead weight in that read, and removing them closes the cliff without touching the reservation protocol.

**1e — a golden vector with a regenerator (Medium).** Spec K D1's `statements_root` goldens have no `-update` flag, which is what makes them a real freeze. `arbiter/accumulator/vectors_test.go:23` does have one (`-update`, regenerating `testdata/spent_ids_vectors.json`), and `L3BlockHeader.ChainHash()` depends on that derivation through `SpentIDsRootAfter`. It is the same laundering path as D1's, one layer down: a change to the spent-IDs derivation can be re-blessed with a flag instead of failing.

**1f — CI reports skips as passes (Medium).** Three separate instances. `sentio-node/.github/workflows/ci.yml:112` pins `--test_filter='TestStorageIntegrityProtocolTableDriftFailsBootstrap'`, so any additional chain-free ClickHouse test added later is silently not run — the anti-skip guards Spec J added protect the one named function, not the job's stated purpose. arbiter-core's and arbiter's ClickHouse tests self-skip on `ARBITER_CH_INTEGRATION` (`snode/ch_test.go:14`, `verifier/scanner_test.go:29`, `dataplane/ddl/ch_test.go:17`) and Go reports a skip as `PASS`, so a locally green `bazel test //...` is not evidence that the docker acceptance ran. HouseGate's `manual`-tag convention makes exactly this distinction explicit; the sibling repos do not have it.

**1g — bookkeeping that misrepresents state (Low).** The plans for Specs I, K and L are 0-ticked with `Status: Proposed` while their code is merged, so the documents say the opposite of the truth and were useless as review inputs. Spec L plan Task 14b has all six checkboxes ticked, with Step 5 describing verification of `funcIdentity(plan[0].Run)` — `storageIntegrityBootPlan`, `funcIdentity` and `reflect.ValueOf` have never existed in sentio-node. What was delivered instead (production binding a named interface method, recording order, plus a source-text audit) is sound and is not the tautology the original review criticised, but the record describes work that was not done. Spec A §4.2 does not list `statement_kind` among the bound envelope fields even though the implementation binds it, and Spec B's edit list is stale.

## 2. Goals / non-goals

**Goals.** Give the dispatch path the same completeness proof the audit path has. Turn 1b's accidental property into a recorded, tested one. Close the read-mode and part-scan gaps. Remove the remaining golden regenerator. Make the three CI honesty gaps explicit. Correct the records.

**Non-goals.** Adding `statement_count` / `statements_root` to `ReplayJob` — see D1 for why the assertion is preferred for now, and §5 for the debt. Re-litigating the peer-trust exemption. Any change to what `settings_hash` covers.

## 3. Decisions

### D1 — the dispatch path asserts block completeness, and refuses to dispatch an incomplete block

`blockStatements` gains a completeness-checked sibling — `blockStatementsComplete(blockSeq) ([]*StatementState, bool)` — that returns false when the number of resolved statements does not equal `header.StatementCount`, or when any entry is nil. `BlockDispatchInfo` uses it and returns `(BlockDispatchInfo{}, false)` on incompleteness, which the orchestrator already handles as "not dispatchable yet". `reevaluateBlock` (`fsm/threeway.go:46`) uses it too and returns early, matching its existing `ss.RC == nil` evaluability bail-out.

The existing `blockStatements` stays for the `forEachBlockStatement` callers inside `apply.go`, which iterate for side effects on statements that exist and do not derive a root — but it gains a doc comment naming the distinction, so the next caller has to choose deliberately.

Preferring the assertion over extending `ReplayJob` is a scope decision, not a claim that it is the better long-term answer: an incomplete block is a local corruption, and refusing to dispatch is both correct and cheap, whereas `statement_count` / `statements_root` in the job is a wire-contract change across arbiter-proto and every verifier. §5 records it.

**Acceptance:** a test that removes one statement from a sealed block's `f.st.Statements` and asserts `BlockDispatchInfo` returns false and `reevaluateBlock` produces no verdict — mirroring Spec K D3's existing tampering test on the read path, and failing against today's code.

### D2 — the `SET` refusal becomes a recorded, tested property

- One corpus case: `SET async_insert = 1` under an active SI contract → `UnsupportedStatement` with the D1 generic message. Byte-identical in both engines.
- One HouseGate integration test against real ClickHouse: with SI configured, `SET async_insert=1` is refused with an exception; without SI configured, it passes through unchanged.
- Spec I D6's decision record and `CLAUDE.md` gain one sentence: an SI-configured deployment refuses session-level `SET`, and the sessions that bypass rewrite (peer-trusted, forwarded, maintenance, platform-operator) can still issue it — which is part of what the documented network-isolation requirement is protecting.

No code change. The point is to convert an accident into a contract.

### D3 — the read-mode rejection is proved end to end

The existing read-mode integration test gains an ingress-configured variant asserting that a client-supplied `SQL_x_read_mode` in query settings is rejected by `RejectUserSettings`, and that the configured default still applies. It must fail if `RejectUserSettings` is removed.

### D4 — `fullScope()` stops reading safe-database part names

`fullScope()` returns unsafe-only. The `IncludeSafeDatabase` / `SafeDatabase` fields stay on `PartsScope` for the bounded aggregate path (`RefreshCounts`), which still needs safe counts for the `storage_integrity_safe_parts` gauge — counts, not names. The change is to the exact-name scope only.

Before changing it, the plan enumerates every consumer of exact part names and proves each keys on `UnsafeDatabase`; if any consumer reads a safe name, the finding is wrong and the task stops rather than proceeding. **Acceptance:** a test asserting the exact-name read issues no query against the safe database, plus a scale test at the spec's stated 300k-row shape that now completes.

### D5 — `arbiter/accumulator/vectors_test.go` loses its `-update` flag

The flag and its regeneration branch are deleted; the vectors become a frozen file. Regenerating them becomes a deliberate act — edit the JSON, in a commit that says why — exactly as Spec K D1's goldens already are. A comment in the file names `L3BlockHeader.ChainHash()`'s dependency through `SpentIDsRootAfter`, so the blast radius is visible at the edit site.

### D6 — CI stops reporting skips as passes

- **sentio-node:** the pinned `--test_filter` becomes a package-or-prefix filter (e.g. `--test_filter='TestStorageIntegrity.*'`) plus the `require_test_count` assertion pattern Spec J already introduced in rewriter-grpc, so a filter matching fewer tests than expected goes red. The three existing anti-skip guards are kept.
- **arbiter-core and arbiter:** adopt HouseGate's convention — the docker-bound tests move behind an explicit Bazel target (or an explicit `-tags integration` build tag), listed by name in CI, so a plain local run visibly does not include them instead of silently passing them. The `ARBITER_CH_INTEGRATION` env check stays as a second belt.

### D7 — the records are corrected

- Specs I, K and L: `Status:` becomes `Implemented` (or `Partially Implemented` with the named remainder, the way Spec J's already reads), and their plans' checkboxes are reconciled against the merged code — every unticked-but-done step ticked, every ticked-but-not-done step untucked with a one-line note.
- Spec L plan Task 14b: Step 5's text is rewritten to describe what was actually delivered, with a note that the original `funcIdentity` approach was never implemented.
- Spec A §4.2: add `statement_kind` to the bound envelope field list.
- Spec B: refresh the edit list.

Bookkeeping is last in the plan and is not allowed to be the reason the plan is "done": a plan whose only remaining unticked items are documentation is finished; a plan whose documentation is ticked while code is not is the failure mode this decision exists to prevent.

## 4. Testing / acceptance

Each decision names its own acceptance above. The cross-cutting rule from the previous rounds applies unchanged: **every new guard needs a step proving it fails against the unfixed code.** D1, D3 and D4 each have a concrete pre-fix failure; D2 and D5's are structural (a corpus case that does not exist, a flag that does).

## 5. Out of scope / recorded debt

- **`statement_count` / `statements_root` in `ReplayJob`.** The durable fix for 1a is for the verifier to be able to detect a short statement list itself rather than relying on the dispatcher being uncorrupted. It is an arbiter-proto wire change and belongs with the next protocol revision.
- **RFC-8785 canonical JSON for every root** — carried forward from Spec K D1.
- **`system.*` metadata exposure** — see Spec N §6.
- **A real tokenizer behind `sireserved`** — see Spec N §6.
