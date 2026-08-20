# Verification Restoration: CI Execution, Parity Gates, Agent-Guidance Files

**Date:** 2026-08-19 **Status:** Proposed **Roadmap:** [remediation roadmap](2026-08-19-storage-integrity-remediation-roadmap.md) Spec J. **Remediates:** verification-coverage findings from the 2026-08-19 review across Specs A/C/G. **Code base:** housegate `621eaab`, rewriter-grpc `ddc24b9`, rewriter-go `dbac7bc`, sentio-node `ba136ea`, arbiter-core `b669ccd`. **Source of truth:** English version.

## 1. Problem

Every repo builds and every locally-runnable unit test passes. That fact is much weaker than it sounds, because most of those tests are not executed by any automation, and the one cross-engine gate that exists compares almost nothing.

**1a — HouseGate CI runs none of its unit tests.** `.github/workflows/ci.yml:54-55` still carries

```yaml
      # - name: Build & Test All
      #   run: bazel test --config=ci //...
```

The `--config=ci` settings exist in `.bazelrc` (L41-47), so this is a disable, not a dangling reference. The repo has 1202 `func Test*`; 1130 live outside `pkg/integration` in the 56 unit targets CI never runs. `bazel build //...` compiles the test binaries, so CI catches compile breaks and nothing else. The SI work (#123, #125, #126) added thousands of assertions into that blind spot; #126 edited this very file to add the FFI fetch and did not uncomment the line.

**1b — rewriter-grpc has no CI.** `.github/workflows/` holds only `cut-release.yml` and `release.yml`, both `workflow_dispatch`/tag-triggered, and `release.yml` only runs `./scripts.sh rebuild` plus a docker build/push. No `pull_request` or `push: branches: [main]` trigger exists anywhere. The C++ half of the SI read surface has zero automated test execution.

**1c — the shared corpus is not a parity gate.** `storage_integrity_cases.json` is byte-identical in both repos (sha256 `cb37d657…`, 145753 bytes, 178 cases) — the publication discipline worked. The enforcement did not:

- `rewriter-grpc/tests/rewriter_test.cc` loads `want_sql` at `:4608` and compares it at exactly one place, `:4686`, guarded by `sql_exact`. **One of 178 cases sets `sql_exact`.** Twelve of the thirteen `want_sql` strings are dead in C++, including every derived-table shape.
- `allow_sql_divergence` is never parsed by the C++ loader (`:4601-4637`), while in Go it *disables* the oracle SQL diff. Twenty-five cases set it, so for those neither engine's SQL is compared to anything but substrings.
- `normalizeSIIdentifierQuotes` (`:4541-4547`) does a global backtick→double-quote replace across the whole response including string literals, hiding quoting bugs.
- Seven non-reject cases have vacuous `want_sql_contains` whose expected substrings already appear in the input, so a no-op rewriter passes.

Net C++ coverage: 1 real SQL assertion, 147 "unchanged", 22 non-vacuous substrings, 7 vacuous, 1 none. It is not a coincidence that the only bug that shipped and needed a patch release (SI DESCRIBE, non-executable on ClickHouse 25.8) was in the single `sql_exact` case.

**1d — Spec C's only end-to-end proof has never run.** `sentio-node/standalone/storage_integrity_smoke_test.go` gates on `SENTIO_SI_E2E=1`; that variable appears in no workflow, and sentio-node's CI has no ClickHouse service. Its Phase 3 — tamper `parts_to_throw_insert`, reboot, assert `ErrProtocolTableDrift` — is the only assertion that a real node refuses to start on drift, and the plan itself records that the operator environment was unavailable, so it has never run manually either.

**1e — `pkg/replay/AGENTS.md` gives inverted architectural guidance and git cannot see it.** It states these packages are alias-only forwards to `github.com/sentioxyz/arbiter-core/replay` and that behaviour changes belong there. Every load-bearing sentence is false: `arbiter-core/replay` does not exist, housegate does not depend on arbiter-core (its only sentioxyz module is arbiter-proto), `pkg/replay` + `payloadexec` + `chexec` + `nativepayload` are ~4800 lines of real implementation, and arbiter-core imports housegate's `pkg/replay` in 63 places. It survived because `core.excludesFile = ~/.git/.gitignore` line 20 is a bare `AGENTS.md` rule matching every depth in every repo: housegate has eight nested `AGENTS.md` files and only the root one is tracked. An agent opening `pkg/replay` reads it and is told to stop writing real code there.

**1f — `rewriter-go`'s `make test` rewrites tracked fixtures.** `internal/engine/characterize_test.go` unconditionally overwrites `internal/engine/testdata/ast-shapes/*.json` and asserts nothing. The committed goldens date from before the polyglot v0.8.1 bump and differ semantically from what the pinned FFI now produces (e.g. `alter_delete`'s WHERE clause degrades to `Raw`). Tests pass before and after, so the drift is silent, and the suite is order-dependent between the writer and the two readers.

## 2. Goals / non-goals

Goals: (1) unit tests execute in CI in every repo that has them; (2) the shared corpus becomes a real parity gate — every case with a `want_sql` is compared in both engines, and no case can pass vacuously; (3) Spec C's drift acceptance actually executes; (4) agent-guidance files are correct and version-controlled; (5) no test mutates tracked fixtures.

Non-goals: raising coverage percentages; adding new behavioural tests beyond what Specs I/K/L bring; moving rewriter-grpc's build off its remote box; changing the user's global git configuration (a bounded task on the roadmap, not a repo change).

## 3. Decisions

**D1 — HouseGate CI runs `bazel test --config=ci //...`, and a failure blocks merge.** Uncomment it. If pre-existing failures surface, fix them; anything that cannot be fixed in this spec's scope is quarantined with an explicit `--test_tag_filters` exclusion **and** a tracking issue referenced in a comment — never left globally disabled. The docker job stays as it is. Rejected: a separate opt-in workflow — the whole finding is that opt-in verification does not get run.

**D2 — rewriter-grpc gets a `pull_request` + `push: main` CI job that runs the test binary.** It builds on the project's existing remote-build path (`scripts.sh`), so the job is "build on the box, run `ctest`/`rewriter_tests`, fail on non-zero". If the remote box cannot be reached from CI, the fallback is a self-hosted runner label matching the other repos. The job must run the storage-integrity corpus test specifically, not just compile.

**D3 — Corpus assertion contract, enforced by a schema check in both runners.** Every case must satisfy: it is either a reject case (`want_code != Success`) with a `want_message` substring, or it carries a `want_sql`. `want_sql_contains` becomes an *additional* assertion, never the only one, and a validator rejects any `want_sql_contains` entry that is already a substring of the input SQL (the vacuity check). `sql_exact` is deleted as a concept — comparison is always exact after a shared normalization. `allow_sql_divergence` is parsed by **both** runners and means exactly "the two engines legitimately differ here"; a case that sets it must carry a per-engine `want_sql_go` / `want_sql_cpp` so each side is still pinned. Rejected: keeping `sql_exact` as an opt-in — it inverts the default in the direction that produced the DESCRIBE bug.

**D4 — Normalization is shared and literal-aware.** The backtick→double-quote normalization moves into a documented function applied to identifiers only, and the two runners implement the same rule. A test asserts that a quoting change inside a string literal is *not* normalized away.

**D5 — sentio-node CI gains a ClickHouse service and sets `SENTIO_SI_E2E=1`.** Mirror arbiter-core's `integration-clickhouse` job, including the shared-Keeper configuration (the SI smoke creates ReplicatedMergeTree tables). The smoke's skip stays for local runs; CI sets the variable so the Phase 3 drift assertion executes. Rejected: converting the smoke to a mock — its entire value is that it boots a real node against a real ClickHouse.

**D6 — Agent-guidance files are corrected and tracked.** Rewrite `pkg/replay/AGENTS.md` to state the real dependency direction (housegate is the source of truth; arbiter-core consumes it; wire form is mirrored in arbiter-proto `replay.proto` with a field-name freeze enforced by arbiter-core conformance tests) and keep its invariant list. Force-add it and the other seven nested `AGENTS.md` files so review and CI can see them. Add a CI check that fails when an untracked `AGENTS.md` exists in the tree, so the global ignore rule cannot silently re-hide them.

**D7 — Capture tests never write tracked files.** `characterize_test.go` becomes a comparison test by default and regenerates only under an explicit `-update` flag (or `UPDATE_GOLDEN=1`), following the repo's own harness convention. Regenerate the stale `ast-shapes` fixtures once, in the same PR, so the committed state matches the pinned polyglot. Add a CI step asserting `git diff --exit-code` after the test run.

## 4. Sequencing note

D3/D4 change the corpus contract that Spec I extends. Land J's runner changes **before or together with** I's new cases; otherwise I's C++ coverage is nominal. D1 should land early for the same reason — K and L want their tests to actually run on the way in.

## 5. Testing / acceptance

- HouseGate: CI is red on a deliberately broken assertion in `pkg/storageintegrity`, green when reverted (prove the gate works, do not just observe green).
- rewriter-grpc: the same — a deliberately broken corpus expectation must fail the new job.
- Corpus: a meta-test in both repos asserts the D3 schema over every case (every case has `want_sql` or is a reject with `want_message`; no vacuous `want_sql_contains`; `allow_sql_divergence` implies both per-engine expectations). Running it against the pre-fix corpus must report the 7 vacuous cases and the 12 unasserted `want_sql`s.
- sentio-node: the SI smoke's Phase 3 executes and fails when `parts_to_throw_insert` is tampered.
- `git diff --exit-code` clean after `make test` in rewriter-go.
- `git status --porcelain` shows no untracked `AGENTS.md` in housegate.

## 6. Delivery

1. housegate: uncomment CI tests, fix or quarantine fallout, D6 files + untracked-`AGENTS.md` check.
2. rewriter-go + rewriter-grpc: D3 schema + validator + both runners, D4 normalization, D7 capture test and fixture regeneration, corpus meta-test.
3. rewriter-grpc: the new CI job.
4. sentio-node: ClickHouse service + `SENTIO_SI_E2E=1`.
