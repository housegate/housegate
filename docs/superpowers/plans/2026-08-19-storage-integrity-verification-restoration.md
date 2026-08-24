# Verification Restoration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every repo's tests actually execute in CI, turn the 178-case storage-integrity corpus into a real two-engine parity gate, correct and version-control the agent-guidance files, and stop `make test` from rewriting tracked fixtures.

**Architecture:** Five repos, four independent workstreams. (A) housegate: uncomment the disabled `bazel test` step, correct + force-add the nested `AGENTS.md` files, un-ignore them at the repo level so the user's global rule cannot hide future ones, and add a CI manifest check that a known file cannot be quietly deleted or untracked. (B) rewriter-go + rewriter-grpc: a frozen corpus schema with a validator implemented twice (Go and C++), a shared literal-aware identifier-quote normalization implemented twice, and per-engine SQL pins so every non-reject case is compared exactly on both sides. (C) rewriter-grpc: a `pull_request` + `push: main` job that drives the existing remote build box over SSH. (D) sentio-node: a ClickHouse + Keeper CI job modelled on arbiter-core's, plus a chain-free test that executes the protocol-table drift refusal the SI smoke's Phase 3 has never run.

**Tech Stack:** Bazel 9.1.0 + Bzlmod (housegate, sentio-node, arbiter-core), Go 1.22+ (`go test`, rewriter-go), CMake + Ninja + GoogleTest v1.14 + Poco JSON (rewriter-grpc), GitHub Actions (self-hosted `linux/x64` runners for housegate/arbiter-core/sentio-node; `ubuntu-latest` + SSH to the shared build box for rewriter-grpc), Docker (ClickHouse 25.8 with shared Keeper).

**Spec:** [docs/superpowers/specs/2026-08-19-storage-integrity-verification-restoration-design.md](../specs/2026-08-19-storage-integrity-verification-restoration-design.md) (Spec J). Roadmap context: [docs/superpowers/specs/2026-08-19-storage-integrity-remediation-roadmap.md](../specs/2026-08-19-storage-integrity-remediation-roadmap.md).

> **Execution note (2026-08-24):** All original repository implementation and verification steps are complete, including the live terminal required checks, across HouseGate [#132](https://github.com/housegate/housegate/pull/132), rewriter-go [#31](https://github.com/housegate/rewriter-go/pull/31), rewriter-grpc [#51](https://github.com/housegate/rewriter/pull/51), and sentio-node [#179](https://github.com/sentioxyz/sentio-node/pull/179). Spec J remains Partially Implemented only because D2's base-selected source-policy workflow cannot be enforced at the organization level on the current plan; that separate plan/licensing blocker is tracked as P1 [#137](https://github.com/housegate/housegate/issues/137).

## Global Constraints

- Repo checkouts on this machine: housegate `/Users/uranuswch/Dev/housegate/housegate`, rewriter-go `/Users/uranuswch/Dev/housegate/rewriter-go`, rewriter-grpc `/Users/uranuswch/Dev/housegate/rewriter-grpc`, sentio-node `/Users/uranuswch/Dev/sentio_xyz/sentio-node`, arbiter-core `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`.
- Baseline commits (from the spec): housegate `621eaab`, rewriter-grpc `ddc24b9`, rewriter-go `dbac7bc`, sentio-node `ba136ea`, arbiter-core `b669ccd`.
- `storage_integrity_cases.json` must stay **byte-identical** between `rewriter-go/internal/harness/testdata/` and `rewriter-grpc/tests/testdata/`. Pre-change sha256 `cb37d657ebedfb04f0308b136f257636833bd4edefb4675b2f2c83147537cf9f`, 145753 bytes, 178 cases.
- `sql_exact` is deleted as a concept. Comparison is always exact after the shared normalization (spec D3).
- `allow_sql_divergence` means exactly "the two engines legitimately differ here"; a case that sets it **must** carry both `want_sql_go` and `want_sql_cpp` (spec D3).
- `want_sql_contains` is an *additional* assertion, never the only one; an entry that is already a substring of the case's input SQL is a schema violation (spec D3 vacuity check).
- The shared normalization is **literal-aware**: a quoting change inside a single-quoted string literal must not be normalized away (spec D4).
- Tests must never write tracked files. Regeneration happens only under `UPDATE_GOLDEN=1` (spec D7).
- Every "prove the gate works" acceptance item in spec §5 is executed as a deliberate break → observe red → revert → observe green cycle, with the observed output recorded in the PR description. Observing green alone is not acceptance.
- Branch-mode development in every repo: cut `feat/<topic>` or `fix/<topic>`, land via PR, never commit feature work to `main`.
- English for identifiers, comments, log strings, and operator-facing messages in every repo.
- Markdown docs: no hard line-wrapping; one paragraph per line.
- Do not change the user's global git configuration. `~/.git/.gitignore` line 20's bare `AGENTS.md` rule is a user-environment decision recorded on the roadmap (§5 bounded tasks), explicitly out of scope here — this plan works *around* it with a repo-level `!AGENTS.md` negation (which takes precedence over `core.excludesFile`), `git add -f` for today's files, and a CI manifest check.

---

## Verified Baseline Facts

These were measured against the working trees listed above **before** this plan was written. An executor who observes something different must stop and re-measure rather than assume.

**housegate — the unit suite is currently green.** `bazel test --nocache_test_results --test_output=summary //...` on darwin/arm64 executed **56 of 56 test targets, all PASSED**, in 45.8s wall (`//:housegate_test` is the long pole at 36.6s). There are **no pre-existing failures to fix or quarantine**. Spec D1's contingency ("if pre-existing failures surface, fix them; anything that cannot be fixed is quarantined with an explicit `--test_tag_filters` exclusion and a tracking issue") therefore has no known trigger, but Task 1 still carries the quarantine procedure verbatim because the CI runner is `linux/x64` and this measurement was taken on darwin.

**`--config=ci` cannot be validated locally.** `.bazelrc:46` sets `build:ci --platforms=//:linux_amd64`. On darwin that cross-compiles the test binaries and Bazel then fails analysis with `No matching toolchains found for @@bazel_tools//tools/test:default_test_toolchain_type`. `//:linux_amd64` is declared at `BUILD.bazel:155`. On the self-hosted `[self-hosted, linux, x64]` runner it is the native platform, so the flag is correct there and only there. Local pre-flight must use plain `bazel test //...`.

**housegate `AGENTS.md` tracking.** Eight nested files exist and none is tracked; only the root `AGENTS.md` is. `git check-ignore -v pkg/replay/AGENTS.md` reports `/Users/uranuswch/.git/.gitignore:20:AGENTS.md`. The untracked set is exactly:

```
docs/superpowers/AGENTS.md
pkg/chproto/AGENTS.md
pkg/integration/AGENTS.md
pkg/plugins/AGENTS.md
pkg/proxy/AGENTS.md
pkg/replay/AGENTS.md
pkg/rewriter/AGENTS.md
tools/da-mvp/AGENTS.md
```

**`pkg/replay` dependency direction (for D6's replacement text).** `github.com/sentioxyz/arbiter-core/replay` does not exist (`ls replay` in arbiter-core: No such file or directory). housegate's only `sentioxyz` module requirement is `github.com/sentioxyz/arbiter-proto v0.5.0` (`go.mod:15`); the other `sentioxyz` lines are `replace` directives for wasmer-go / ch-go / clickhouse-go. arbiter-core imports `github.com/housegate/housegate/pkg/replay` in **63 import lines across 48 files**. The wire mirror lives in `arbiter-proto/proto/replay.proto` and is frozen by `arbiter-core/conformance/replay_wire_test.go`, whose package doc reads: pins the arbiter-proto wire mirror to housegate `pkg/replay`, field sets must stay identical.

**Corpus census (reproduced from the real file).** 178 cases. 147 reject (`want_code != Success`), 31 non-reject. Exactly **1** case sets `sql_exact` (`si_describe_metadata_select`). **13** cases carry `want_sql`; **12** of them lack `sql_exact` and are therefore dead in the C++ runner. **25** cases set `allow_sql_divergence`; the C++ loader never reads that key. **3** reject cases carry no `want_message_contains`: `si_optimize_rejected`, `si_detach_rejected`, `si_attach_rejected`. Exactly **7** non-reject cases have vacuous `want_sql_contains` (every expected substring already present in the input SQL). The full non-reject partition is:

| bucket | count | cases |
|---|---|---|
| `allow_sql_divergence` **and** `want_sql` | 8 | `si_safe_plain_select`, `si_safe_alias_join_non_si`, `si_unsafe_latest_no_excluded`, `si_unsafe_latest_two_excluded`, `si_safe_subquery_in_where`, `si_reserved_keyword_quoted`, `si_star_hides_rid`, `si_use_default_database` |
| `allow_sql_divergence`, no `want_sql` | 17 | `si_union_rewritten`, `si_with_offset_literal_allowed`, `si_remote_mapping_wins_and_clears_remote_flag`, `si_non_reserved_output_alias_allowed`, `si_intersect_rewritten`, `si_mixed_ordinary_final_allowed`, `si_mixed_ordinary_sample_allowed`, `si_mixed_ordinary_alias_columns_allowed`, `si_insert_authorized_values_allowed`, `si_insert_authorized_select_allowed`, `si_mixed_ordinary_with_offset_allowed`, `si_comma_ordinary_with_offset_allowed`, `si_ordinary_callable_in_values_allowed`, `si_ordinary_callable_in_ignore_set_values_allowed`, `si_ordinary_in_table_allowed`, `si_ordinary_local_catalog_function_allowed`, `si_ordinary_remote_engine_allowed` |
| `want_sql`, no divergence | 5 | `si_describe_metadata_select`, `si_exists_table_safe`, `si_insert_rewrites_like_today`, `non_si_table_unaffected`, `si_absent_args_ordinary_rewrite` |
| neither | 1 | `si_show_tables_unchanged` |

The **7 vacuous** cases and their offending `want_sql_contains` entries:

| case | vacuous entries |
|---|---|
| `si_insert_rewrites_like_today` | `db1.t`, `VALUES (1)` |
| `non_si_table_unaffected` | `other.u` |
| `si_ordinary_callable_in_values_allowed` | `other.u` |
| `si_ordinary_callable_in_ignore_set_values_allowed` | `other.u` |
| `si_ordinary_in_table_allowed` | `other.v` |
| `si_ordinary_local_catalog_function_allowed` | `mergeTreeIndex`, `'other'`, `'u'` |
| `si_ordinary_remote_engine_allowed` | `Remote`, `'other'`, `'u'` |

The **12 dead `want_sql`** cases (present but never compared by the pre-fix C++ runner): the 8 divergent ones above, plus `si_exists_table_safe`, `si_insert_rewrites_like_today`, `non_si_table_unaffected`, `si_absent_args_ordinary_rewrite`.

**rewriter-go's `ast-shapes` drift is exactly three files.** Running `POLYGLOT_SQL_FFI_PATH=$PWD/third_party/lib/libpolyglot_sql_ffi.dylib go test ./internal/engine/ -run TestCharacterizeAST` produced a diff in `alter_delete.json` (its `Delete { where_clause: … }` node collapses to `Raw { sql: "DELETE WHERE y=2" }`), `create_view.json` and `create_mv_to.json` (`"security_sql_style": true` → `false`). The other 38 fixtures are byte-stable. The fixtures were restored with `git checkout --` after measuring.

**rewriter-go has no existing `-update` flag convention.** There is no `flag.Bool` anywhere under `internal/`. The repo's only test-configuration convention is a **named env-var constant** — `harness.OracleAddrEnv = "REWRITER_ORACLE_ADDR"` in `internal/harness/oracle.go:16`, read via `os.Getenv`. D7's regeneration switch therefore follows that convention: an exported constant `UpdateGoldenEnv = "UPDATE_GOLDEN"`, honoured when its value is exactly `"1"`.

**rewriter-grpc builds only on the remote box.** `CLAUDE.md` §"Build / test / run": *ClickHouse does not compile on local dev machines* — every build crosses the network to `ssh -p 30100 sentio@64.38.131.242`. `release.yml` already automates that path from `ubuntu-latest` using `secrets.BUILD_HOST_SSH_KEY` + `secrets.BUILD_HOST_KNOWN_HOSTS`, a `concurrency: group: build-box` serializer, and a dedicated CI workdir `~/ci/rewriter`. `./scripts.sh test` calls `build` (a full ~15-20 min clean build), **not** `rebuild`; the CI job must therefore run `./scripts.sh rebuild` followed by `ctest` rather than `./scripts.sh test`. The gtest target is `rewriter_tests` and `tests/CMakeLists.txt` registers `add_test(NAME rewriter_tests COMMAND rewriter_tests)`.

**sentio-node's SI smoke needs far more than ClickHouse.** `standalone.Run` (`standalone/standalone.go:45`) starts sentio-core services, builds `common.NewNodeEnv` (Ethereum RPC), may call `env.EnsureRegistered` against `IndexerRegistry`, constructs a syncer, Redis accumulators and two RPC servers — all **before** it reaches `ddl.EnsureProtocolTables` at `standalone.go:319`. `TestSchemaRegistryPhaseBSmoke` additionally reads on-chain schema declarations via `ethclient` + `bindings.NewIDatabases` and talks to `arbiter` gRPC peers, and requires eleven `SENTIO_SI_*` env values including a signer key and two pre-minted JWSs. Adding a ClickHouse service to sentio-node CI, on its own, cannot make that test execute. Tasks 13-14 record the consequence and deliver the drift proof through the production bootstrap seam instead. See "Deviations from the spec" below.

**arbiter-core already proves the `ddl`-level drift refusal in CI.** `dataplane/ddl/ensure_ch_test.go:23` `TestEnsureProtocolTables_CreateVerifyTamperDrift` tampers `parts_to_throw_insert = 2999` and asserts `errors.Is(err, ErrProtocolTableDrift)`, and `//dataplane/ddl:ddl_test` is in the `integration-clickhouse` job's explicit target list (`.github/workflows/ci.yml:98`). What has *never* executed is sentio-node's own wiring — that `standalone`'s bootstrap path invokes `EnsureProtocolTables` in `ModeCreateAndVerify` and refuses to continue. That is what Tasks 13-14 add.

---

## Deviations from the spec (read before starting)

**DEV-1 (Task 1) — housegate has no fallout.** Spec D1 anticipates "pre-existing failures". There are none on darwin. The plan keeps the quarantine machinery as a conditional branch inside Task 1 in case the linux runner disagrees, and requires the executor to record the actual runner output either way.

**DEV-2 (Tasks 13-14) — sentio-node cannot run the existing SI smoke in CI.** Spec D5 says "Mirror arbiter-core's `integration-clickhouse` job … CI sets the variable so the Phase 3 drift assertion executes." Evidence above shows `SENTIO_SI_E2E=1` additionally requires an Ethereum RPC with deployed `IDatabases`/`IndexerRegistry` contracts, a running arbiter with gRPC peers, Redis, sentio-core services, a signer private key and two pre-minted query JWSs. Standing that up is a devnet, not a service container, and the spec's own non-goals do not budget for it. This plan therefore delivers spec §5's acceptance sentence ("the SI smoke's Phase 3 executes and fails when `parts_to_throw_insert` is tampered") through a new chain-free test in the same package that drives the **same production helper** `runStorageIntegrityProtocolBootstrap` with the **same** `ddl.EnsureProtocolTables(..., ddl.ModeCreateAndVerify, ...)` call against a real tampered ReplicatedMergeTree, gated by a new `SENTIO_SI_CH_E2E=1`. The existing `SENTIO_SI_E2E` smoke is left untouched and still skips.

> **Operator decision required (do not silently resolve):** running the full `SENTIO_SI_E2E=1` smoke in CI needs a devnet fixture (anvil or equivalent + deployed contracts + arbiter + Redis + provisioned keys). Task 15 Step 6 files that as an explicit follow-up rather than pretending Tasks 13-14 covered it.

**DEV-3 (Task 12) — rewriter-grpc CI runs on the shared build box.** Spec D2 offers "if the remote box cannot be reached from CI, the fallback is a self-hosted runner label matching the other repos". The box *is* reachable — `release.yml` proves it — so the job reuses that path. Consequence: PR CI and tag releases serialize on `concurrency: group: build-box`, and fork PRs are skipped because secrets are unavailable to them. Both are stated in the workflow comments.

---

## File Structure

**housegate** (`/Users/uranuswch/Dev/housegate/housegate`)

| File | Responsibility |
|---|---|
| `.github/workflows/ci.yml` | Modify: enable `bazel test --config=ci //...` in the `build` job; add the untracked-`AGENTS.md` check step to the `release-tooling` job. |
| `pkg/replay/AGENTS.md` | Modify: replace the inverted dependency guidance with the true one. |
| `docs/superpowers/AGENTS.md`, `pkg/chproto/AGENTS.md`, `pkg/integration/AGENTS.md`, `pkg/plugins/AGENTS.md`, `pkg/proxy/AGENTS.md`, `pkg/rewriter/AGENTS.md`, `tools/da-mvp/AGENTS.md` | Track: `git add -f`, content unchanged. |
| `scripts/agents-md-manifest.txt` | Create: the expected set of tracked `AGENTS.md` paths, generated from the tracked state under `LC_ALL=C`. |
| `scripts/check-agents-md-tracked.sh` | Create: two-way check — every manifest path is still tracked (the half a clean CI checkout can observe), and no `AGENTS.md` in the tree is untracked (the local half). Single responsibility, no other lint logic. |
| `CLAUDE.md` | Modify: CI section — record that the unit suite now runs. |

**rewriter-go** (`/Users/uranuswch/Dev/housegate/rewriter-go`)

| File | Responsibility |
|---|---|
| `internal/harness/sinormalize.go` | Create: the D4 shared normalization, `NormalizeSIIdentifierQuotes`. Its own file so the C++ port has one thing to mirror. |
| `internal/harness/sinormalize_test.go` | Create: D4 unit tests, including the literal-preservation assertion. |
| `internal/harness/sicorpus_test.go` | Create: the frozen corpus schema types + `LoadSICorpus` + `ValidateSICorpus` + `LegacyCoverageReport` + the pinned identity constants. Helper-only (no `Test*` functions) — it must be a `_test.go` file because the schema embeds `accessedJSON` and `remoteUpstreamJSON`, which are declared in `select_golden_test.go:56` and `dblevel_golden_test.go:62`. This mirrors how `select_golden_test.go` already hosts shared helpers for its siblings. |
| `internal/harness/sicorpus_contract_test.go` | Create: the D3 meta-test over the real corpus, the validator's violating-case unit tests, the byte-identity test, and the pre-fix coverage-report assertion. |
| `internal/harness/storage_integrity_golden_test.go` | Modify: delete the local `siCase`/`loadSICases` duplicates, consume `sicorpus.go`, compare exactly after normalization, honour per-engine pins, add `UPDATE_GOLDEN` regeneration. |
| `internal/harness/testdata/storage_integrity_cases.json` | Modify: migrated to the frozen schema. |
| `internal/engine/characterize_test.go` | Modify: compare by default, regenerate under `UPDATE_GOLDEN=1`. |
| `internal/engine/testdata/ast-shapes/{alter_delete,create_view,create_mv_to}.json` | Modify: regenerated against the pinned polyglot. |
| `.github/workflows/ci.yml` | Modify: add `git diff --exit-code` after `make test` in the `ffi` job. |
| `AGENTS.md`, `internal/harness/AGENTS.md` | Modify: document the corpus contract and the `UPDATE_GOLDEN` flow. |

**rewriter-grpc** (`/Users/uranuswch/Dev/housegate/rewriter-grpc`)

| File | Responsibility |
|---|---|
| `tests/si_normalize.h` | Create: the C++ half of D4, header-only so both the corpus test and any future test can include it. |
| `tests/si_corpus.h` | Create: the C++ corpus schema + loader + `ValidateSICorpus`, mirroring `sicorpus.go` rule-for-rule. |
| `tests/rewriter_test.cc` | Modify: delete `normalizeSIIdentifierQuotes` / `SIGoldenCase` / `loadSIGoldenCases` (lines ~4504-4639), include the two new headers, compare `want_sql` unconditionally, add the contract meta-test. |
| `tests/CMakeLists.txt` | Modify: nothing structural — the new headers are picked up via the existing `target_include_directories`; add `${CMAKE_CURRENT_SOURCE_DIR}` if not already reachable. |
| `tests/testdata/storage_integrity_cases.json` | Modify: byte-identical copy of the migrated rewriter-go corpus. |
| `.github/workflows/ci.yml` | Create: `pull_request` + `push: main` job driving the build box. |
| `CLAUDE.md` | Modify: document the CI job and the corpus contract. |

**sentio-node** (`/Users/uranuswch/Dev/sentio_xyz/sentio-node`)

| File | Responsibility |
|---|---|
| `standalone/storage_integrity_drift_ch_test.go` | Create: the chain-free ClickHouse+Keeper protocol-table drift test. |
| `standalone/BUILD.bazel` | Modify: add the new source to `go_test`. |
| `scripts/ci/clickhouse-keeper.xml` | Create: copied from arbiter-core `scripts/ci/clickhouse-keeper.xml`. |
| `.github/workflows/ci.yml` | Modify: add the `integration-clickhouse` job. |
| `README.md` | Modify: document `SENTIO_SI_CH_E2E` next to `SENTIO_SI_E2E`. |

---

## Task 1: housegate CI executes the unit suite

**Files:**
- Modify: `.github/workflows/ci.yml:38` (timeout), `:51-55` (the disabled step)
- Modify: `CLAUDE.md` (CI section)
- Temporarily modify for the gate proof: `pkg/storageintegrity/ack2_gate_test.go:21-30`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: a green-or-red merge gate over all 56 unit targets. Tasks 2, 3 and 16 rely on `.github/workflows/ci.yml` already containing the enabled `Build & Test All` step.

- [x] **Step 1: Record the local baseline**

Run from the housegate repo root:

```bash
bazel test --nocache_test_results --test_output=summary //... 2>&1 | tail -70
```

Expected: `Executed 56 out of 56 tests: 56 tests pass.` Save the tail of that output — it goes in the PR description as the "before" evidence. Do **not** run `--config=ci` locally; on darwin `--platforms=//:linux_amd64` has no matching test toolchain and Bazel aborts analysis before running anything.

- [x] **Step 2: Enable the step**

In `.github/workflows/ci.yml`, replace lines 54-55:

```yaml
      # - name: Build & Test All
      #   run: bazel test --config=ci //...
```

with:

```yaml
      # Spec J D1: the unit suite is a merge gate. `--config=ci` pins
      # --platforms=//:linux_amd64 (.bazelrc:46), which only resolves a test
      # toolchain on this linux/x64 self-hosted runner — do not try to
      # reproduce this invocation on darwin, use plain `bazel test //...`.
      # Integration targets are tagged `manual` and stay in their own job.
      - name: Build & Test All
        run: bazel test --config=ci //...
```

and change line 38's job timeout from `timeout-minutes: 15` to `timeout-minutes: 30` (a cold cache has to build and run 56 targets where it previously only built them).

- [x] **Step 3: Commit and push the branch, then read the runner output**

```bash
git checkout -b feat/ci-run-unit-suite
git add .github/workflows/ci.yml
git commit -m "ci: run the unit suite as a merge gate"
git push -u origin feat/ci-run-unit-suite
gh pr create --fill
```

Open the `Build` job log. Record the `Executed N out of M tests` line.

- [x] **Step 4: Handle fallout, if the linux runner disagrees with the darwin baseline**

If the runner is green (the expected outcome given Step 1), skip to Step 5.

If any target fails, for **each** failing target decide fix-or-quarantine and do exactly one of:

*Fix:* repair the test or the code in this same PR, then re-run.

*Quarantine:* only when the failure cannot be fixed inside this spec's scope. Add `tags = ["ci-quarantine"]` to the failing `go_test` target's `BUILD.bazel` rule, add `--test_tag_filters=-ci-quarantine` to the CI invocation, and file a tracking issue. The comment above the tag and the comment above the flag both use this exact format, with the real issue URL substituted:

```python
    # QUARANTINED (Spec J D1): fails on the linux/x64 CI runner while passing
    # on darwin. Tracking: https://github.com/housegate/housegate/issues/NNN
    # Remove this tag and the --test_tag_filters exclusion in ci.yml when the
    # issue is closed. Do not add new targets to this tag without an issue.
    tags = ["ci-quarantine"],
```

```yaml
        # One or more targets carry tags = ["ci-quarantine"] with a tracking
        # issue recorded next to the tag. This exclusion is never a blanket
        # disable — see Spec J D1.
        run: bazel test --config=ci //... --test_tag_filters=-ci-quarantine
```

- [x] **Step 5: Prove the gate is real — deliberately break an assertion**

Spec §5 requires red-on-break, not merely observed-green. Edit `pkg/storageintegrity/ack2_gate_test.go`, in `TestAck2Gate_AllFiveConditionsGrantAck2`, changing:

```go
	ok, reason := Ack2Ready(ackReadyFixture())
	if !ok {
		t.Fatalf("all five conditions satisfied must grant ACK2, got reject %q", reason)
	}
```

to:

```go
	ok, reason := Ack2Ready(ackReadyFixture())
	if ok {
		t.Fatalf("SPEC-J GATE PROOF: deliberate inversion, revert before merge (reason=%q)", reason)
	}
```

Commit and push:

```bash
git add pkg/storageintegrity/ack2_gate_test.go
git commit -m "test: deliberate break to prove the CI gate (revert next commit)"
git push
```

- [x] **Step 6: Observe red**

Wait for the `Build` job. Expected: FAILED on `//pkg/storageintegrity:storageintegrity_test`, with `SPEC-J GATE PROOF` in the log. Screenshot or copy the failing line into the PR description. If the job is green, the gate is not wired — stop and re-check Step 2 before continuing.

- [x] **Step 7: Revert the break and observe green**

```bash
git revert --no-edit HEAD
git push
```

Expected: the `Build` job returns to PASSED with `Executed 56 out of 56 tests`.

- [x] **Step 8: Update `CLAUDE.md`**

In the `## CI` section, replace the sentence beginning "- **Build** — `bazel build //...`." with:

```markdown
- **Build** — `bazel build //...` followed by `bazel test --config=ci //...`, which runs all 56 unit targets as a merge gate (Spec J D1). `--config=ci` pins `--platforms=//:linux_amd64`, so this exact invocation only resolves a test toolchain on the linux/x64 runner; locally use plain `bazel test //...`. Integration targets are tagged `manual` and are not reached by this step.
```

- [x] **Step 9: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: record that CI runs the unit suite"
git push
```

---

## Task 2: correct and track housegate's nested `AGENTS.md` files

**Files:**
- Modify: `pkg/replay/AGENTS.md`
- Track (content unchanged): `docs/superpowers/AGENTS.md`, `pkg/chproto/AGENTS.md`, `pkg/integration/AGENTS.md`, `pkg/plugins/AGENTS.md`, `pkg/proxy/AGENTS.md`, `pkg/rewriter/AGENTS.md`, `tools/da-mvp/AGENTS.md`

**Interfaces:**
- Consumes: nothing.
- Produces: all eight nested `AGENTS.md` files present in `git ls-files`, which is the precondition Task 3's CI check asserts.

- [x] **Step 1: Confirm the untracked set before touching anything**

```bash
{ git ls-files --others --ignored --exclude-standard -- '*AGENTS.md'
  git ls-files --others --exclude-standard -- '*AGENTS.md'; } \
  | grep -E '(^|/)AGENTS\.md$' | sort -u
```

Expected: exactly the eight paths listed in "Verified Baseline Facts". The `grep` filter matters — without it `git ls-files --others --ignored` also emits collapsed ignored directories such as `tools/da-mvp/contracts/lib/forge-std/`.

- [x] **Step 2: Replace `pkg/replay/AGENTS.md` in full**

Every load-bearing sentence in the current file is false. Overwrite it with:

````markdown
# REPLAY COMPATIBILITY GUIDE

## OVERVIEW
`pkg/replay`, `pkg/replay/payloadexec`, `pkg/replay/chexec` and
`pkg/replay/nativepayload` are the **canonical implementation** of the
replay-verifier core, not compatibility shims. Housegate is the source of
truth: it owns the `Verifier` / `Executor` seam, the order-canonicalized
data/state/manifest roots, the `_hg_row_id` derivation, the per-row LtHash
commitments and the ClickHouse-backed materializer.

`github.com/sentioxyz/arbiter-core/replay` does not exist. Housegate does not
depend on arbiter-core; its only `sentioxyz` module requirement is
`github.com/sentioxyz/arbiter-proto`. The dependency runs the other way:
arbiter-core imports `github.com/housegate/housegate/pkg/replay` in 63 places
across 48 files.

The wire form is mirrored in `arbiter-proto`'s `proto/replay.proto`. That
mirror is under a **field-name freeze** enforced by arbiter-core's
`conformance/replay_wire_test.go`, which compares the proto descriptor's field
names against the Go structs' `json` tags. Renaming or dropping a `json` tag in
this package breaks that conformance test in another repo.

## CONVENTIONS
- Behaviour changes land **here**, with their tests, and only then propagate.
- Adding, removing or renaming a field on a type mirrored in `replay.proto`
  requires the matching arbiter-proto change plus an arbiter-core conformance
  run; do not land a one-sided rename.
- `payloadexec.TableSchema` and `lthash.Column` `json` tags are the
  network-state content contract (`table_id` / `partition_by` / `columns`,
  `name` / `type`). `TableSchemaHash` hashes the decoded semantic fields, not
  the JSON bytes.
- Root digests are domain-versioned (`safe-snapshot-data-v2`); changing what
  enters a root requires a new domain string, never a silent redefinition.
- A source-root mismatch is **signed, not errored** — it is non-repudiable
  challenge evidence. Only a pre-receipt failure is a local refusal to attest.
- This is a verifier core: no proxy plugins, no ClickHouse daemon, no
  network I/O beyond the injected `Executor` / stores.

## COMMANDS
```bash
bazel test //pkg/replay:replay_test
bazel test //pkg/replay/payloadexec:payloadexec_test
bazel test //pkg/replay/nativepayload:nativepayload_test
bazel test //pkg/replay/chexec:chexec_test
bazel test //pkg/integration:integration_test --test_filter='TestCHReplay|Test.*Replay' --test_output=errors
```
````

- [x] **Step 3: Force-add all eight files**

The global ignore rule means plain `git add` is a silent no-op. Use `-f`:

```bash
git add -f \
  docs/superpowers/AGENTS.md \
  pkg/chproto/AGENTS.md \
  pkg/integration/AGENTS.md \
  pkg/plugins/AGENTS.md \
  pkg/proxy/AGENTS.md \
  pkg/replay/AGENTS.md \
  pkg/rewriter/AGENTS.md \
  tools/da-mvp/AGENTS.md
```

- [x] **Step 4: Verify all eight are now tracked**

```bash
git ls-files '*AGENTS.md'
```

Expected: nine lines — the root `AGENTS.md` plus the eight above. Then re-run the Step 1 detection command; expected: no output.

- [x] **Step 5: Commit**

```bash
git commit -m "docs: track nested AGENTS.md and correct pkg/replay guidance

pkg/replay/AGENTS.md claimed these packages are alias-only forwards to
github.com/sentioxyz/arbiter-core/replay. That package does not exist,
housegate does not depend on arbiter-core, and arbiter-core imports
housegate's pkg/replay in 63 places. Spec J D6."
```

---

## Task 3: Un-ignore `AGENTS.md` at the repo level, and add a CI manifest check

**Files:**
- Modify: `.gitignore` (repo root)
- Create: `scripts/agents-md-manifest.txt`
- Create: `scripts/check-agents-md-tracked.sh`
- Modify: `.github/workflows/ci.yml` (the `release-tooling` job)

**Interfaces:**
- Consumes: Task 2's tracked files — the check passes only because they are tracked.
- Produces: a repo-level negation that defeats the global ignore rule for this repo, plus `scripts/check-agents-md-tracked.sh` (exit 0 when clean, exit 1 with a per-file listing otherwise).

**Read this before writing the check.** A CI check cannot, on its own, stop a future ignored `AGENTS.md` from being hidden: an uncommitted file is simply absent from CI's checkout, so there is nothing for any script to find. Review of this plan flagged exactly that — the deliberate-untrack proof would pass in CI, not fail. The three controls have different jobs and only the first one protects future files:

1. **`.gitignore` negation (the real fix).** A repo `.gitignore` takes precedence over `core.excludesFile`, so `!AGENTS.md` in the root re-exposes every nested `AGENTS.md` in this repo regardless of anyone's global configuration. Verified: with the global bare `AGENTS.md` rule active a nested file is invisible to `git status`; after adding the negation the same file reports as `?? sub/AGENTS.md`.
2. **CI manifest check.** Asserts every path in the committed manifest is present and tracked. This is the half that works in a clean checkout, and it is the reason the manifest has to exist as a committed file: a script that only scans the working tree sees nothing when a path is deleted *and the deletion committed*, so it would pass exactly when it should fail. The manifest is the expected set; the tree is the actual set; the check compares them in both directions — manifest-minus-tree catches a committed deletion, and tree-minus-manifest catches a file added without being registered, which would otherwise leave the deletion gate with a hole for every file added after the manifest was written.
3. **Local visibility.** With (1) in place a newly created nested `AGENTS.md` appears in the author's own `git status`, which is where it has to be caught. The script's untracked scan serves that local case.

- [x] **Step 0: Add the repo-level negation and prove it works**

Append to the repo-root `.gitignore`:

```gitignore
# Agent-guidance files must stay visible in this repo even when a contributor's
# global core.excludesFile ignores them (a bare `AGENTS.md` rule is common).
# A repo .gitignore takes precedence over the global file, so this negation is
# what keeps future nested AGENTS.md files reviewable. Spec J D6.
!AGENTS.md
```

Prove it, since the whole control rests on this precedence:

```bash
git check-ignore -v pkg/replay/AGENTS.md; echo "exit=$?"
```
Expected before: `/Users/uranuswch/.git/.gitignore:20:AGENTS.md	pkg/replay/AGENTS.md`, `exit=0`.
Expected after: the negation line is reported (or no output), `exit=1` — the file is no longer ignored.

```bash
mkdir -p pkg/tmpcheck && echo x > pkg/tmpcheck/AGENTS.md
git status --porcelain --untracked-files=all | grep AGENTS.md
rm -rf pkg/tmpcheck
```
Expected: `?? pkg/tmpcheck/AGENTS.md` appears. Before the negation it does not.

- [x] **Step 0b: Create the manifest**

`scripts/agents-md-manifest.txt` — the expected set, one path per line. Generate it from the tracked state after Task 2, so it cannot disagree with reality at creation time:

```bash
# LC_ALL=C pins the collation. `comm` requires both inputs sorted the same way,
# and a manifest generated under one locale then compared under another (CI is
# routinely a different locale from a dev laptop) yields spurious differences.
# Every sort of this list in this task and in the script uses LC_ALL=C.
LC_ALL=C git ls-files '*AGENTS.md' | LC_ALL=C sort > scripts/agents-md-manifest.txt
cat scripts/agents-md-manifest.txt
```
Expected: 9 lines — the root `AGENTS.md` plus the 8 nested files Task 2 tracked.

- [x] **Step 0c: Prove the manifest check catches a committed deletion**

This is the case the working-tree scan cannot see, so it is worth proving before writing the script:

```bash
git rm -q pkg/replay/AGENTS.md
LC_ALL=C git ls-files '*AGENTS.md' | LC_ALL=C sort | diff - scripts/agents-md-manifest.txt; echo "diff exit=$?"
# Restore: a staged deletion needs the index reset BEFORE the worktree
# checkout — `git checkout -- .` alone will not bring the file back.
git reset -q HEAD pkg/replay/AGENTS.md
git checkout -- pkg/replay/AGENTS.md
git status --porcelain | grep AGENTS.md; echo "restore clean=$?"
```
Expected: `diff exit=1` with `> pkg/replay/AGENTS.md` — the manifest names a path the tree no longer tracks. Then `restore clean=1` (no AGENTS.md line in `git status`), confirming the file is back before continuing.

- [x] **Step 1: Write the failing check by hand first**

Before creating the script, prove the detection command distinguishes the two states. Untrack one file temporarily:

```bash
git rm --cached pkg/chproto/AGENTS.md
{ git ls-files --others --ignored --exclude-standard -- '*AGENTS.md'
  git ls-files --others --exclude-standard -- '*AGENTS.md'; } \
  | grep -E '(^|/)AGENTS\.md$' | sort -u
```

Expected: one line, `pkg/chproto/AGENTS.md`. Restore it:

```bash
git add -f pkg/chproto/AGENTS.md
```

- [x] **Step 2: Create the script**

`scripts/check-agents-md-tracked.sh`:

```bash
#!/usr/bin/env bash
# Fail when any AGENTS.md in the working tree is not tracked by git.
#
# A bare `AGENTS.md` line in a user's global core.excludesFile matches at every
# depth in every repo, so nested agent-guidance files can be silently invisible
# to review and to CI while still steering agents. This check makes that state
# a build failure instead of a surprise. Track a new file with `git add -f`.
#
# Spec J D6.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
manifest="scripts/agents-md-manifest.txt"
[ -f "$manifest" ] || { echo "error: $manifest is missing" >&2; exit 1; }

# Direction 1 (works in a clean CI checkout): every manifest path must still be
# tracked. This is what catches a deletion or untracking that was committed.
missing="$(
  LC_ALL=C comm -23 "$manifest" <(LC_ALL=C git ls-files '*AGENTS.md' | LC_ALL=C sort)
)"
if [ -n "$missing" ]; then
  echo "error: manifest lists AGENTS.md paths that are no longer tracked:" >&2
  printf '  %s\n' $missing >&2
  echo "If the removal is intentional, update scripts/agents-md-manifest.txt in the same commit." >&2
  exit 1
fi

# Direction 1b: a tracked AGENTS.md that the manifest does not list. Without
# this, a file added later never enters the manifest, and its eventual
# deletion is invisible to Direction 1 — the deletion gate would have a hole
# exactly for the files added after it was written.
extra="$(
  LC_ALL=C comm -13 "$manifest" <(LC_ALL=C git ls-files '*AGENTS.md' | LC_ALL=C sort)
)"
if [ -n "$extra" ]; then
  echo "error: tracked AGENTS.md paths missing from the manifest:" >&2
  printf '  %s\n' $extra >&2
  echo "Add them to scripts/agents-md-manifest.txt in the same commit so their removal is gated too." >&2
  exit 1
fi

# Direction 2 (local only): a file present but untracked. CI cannot see this —
# an uncommitted file is absent from its checkout — but a developer can, which
# is where a newly created AGENTS.md has to be caught. The repo-level
# !AGENTS.md negation is what makes it visible to git status at all.
# --others lists untracked files; adding --ignored also surfaces the ones a
# global ignore rule hides. --ignored collapses wholly-ignored directories into
# a single trailing-slash entry, so filter down to real AGENTS.md paths.
untracked="$(
  {
    git ls-files --others --ignored --exclude-standard -- '*AGENTS.md'
    git ls-files --others --exclude-standard -- '*AGENTS.md'
  } | grep -E '(^|/)AGENTS\.md$' | sort -u
)"

if [ -n "$untracked" ]; then
  echo "error: untracked AGENTS.md files found:" >&2
  printf '  %s\n' $untracked >&2
  echo >&2
  echo "Agent-guidance files must be tracked so review and CI can see them." >&2
  echo "Track them with: git add -f <path>" >&2
  exit 1
fi

echo "ok: every AGENTS.md in the tree is tracked"
```

```bash
chmod +x scripts/check-agents-md-tracked.sh
```

- [x] **Step 3: Run it — expect pass**

```bash
./scripts/check-agents-md-tracked.sh
```

Expected: `ok: every AGENTS.md in the tree is tracked`, exit 0.

- [x] **Step 4: Run it against an untracked file — expect fail**

```bash
git rm --cached pkg/proxy/AGENTS.md
./scripts/check-agents-md-tracked.sh; echo "exit=$?"
git add -f pkg/proxy/AGENTS.md
```

Expected: the error listing `pkg/proxy/AGENTS.md` and `exit=1`.

- [x] **Step 5: Wire it into CI**

The check needs no Bazel and no self-hosted runner, so it belongs in the `release-tooling` job where it also runs for fork PRs. In `.github/workflows/ci.yml`, append to the `release-tooling` job's steps (after the `Test Homebrew formula updater` step):

```yaml
      # Spec J D6: a bare `AGENTS.md` rule in a contributor's global
      # core.excludesFile silently untracks nested agent-guidance files. This
      # step makes that state red instead of invisible.
      - name: Check AGENTS.md files are tracked
        run: ./scripts/check-agents-md-tracked.sh
```

- [x] **Step 6: Commit and observe green in CI**

```bash
# Stage everything this task created. Omitting .gitignore or the manifest
# makes the pushed job fail immediately: the check exits 1 when the manifest
# is absent, and without the negation a future nested file stays invisible.
git add .gitignore scripts/agents-md-manifest.txt scripts/check-agents-md-tracked.sh .github/workflows/ci.yml
git status --porcelain -- .gitignore scripts/agents-md-manifest.txt scripts/check-agents-md-tracked.sh .github/workflows/ci.yml
git commit -m "ci: keep AGENTS.md visible and verify the tracked set"
git push
```

Expected: `Release tooling` job green with `ok: every AGENTS.md in the tree is tracked`.

- [x] **Step 7: Prove the gate — untrack a file in CI and observe red**

```bash
git rm --cached pkg/rewriter/AGENTS.md
git commit -m "test: deliberate untrack to prove the AGENTS.md gate (revert next)"
git push
```

Expected: the `Release tooling` job fails with `error: untracked AGENTS.md files found:` and `pkg/rewriter/AGENTS.md`. Copy that into the PR description.

- [x] **Step 8: Revert and observe green**

```bash
git revert --no-edit HEAD
git push
```

Expected: `Release tooling` green again.

- [x] **Step 9: Verify spec §5's `git status` acceptance line**

```bash
git status --porcelain | grep -i 'AGENTS.md' || echo "clean: no untracked AGENTS.md"
```

Expected: `clean: no untracked AGENTS.md`.

---

## Task 4: rewriter-go — the shared, literal-aware normalization (D4 Go half)

**Files:**
- Create: `/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/sinormalize.go`
- Test: `/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/sinormalize_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `func NormalizeSIIdentifierQuotes(sql string) string` in package `harness`. Tasks 5, 6 and 7 call it; Task 8 ports the identical state machine to C++.

- [x] **Step 1: Write the failing test**

Create `internal/harness/sinormalize_test.go`:

```go
package harness

import "testing"

func TestNormalizeSIIdentifierQuotes(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"no quotes", "SELECT 1", "SELECT 1"},
		{"bare identifier", "SELECT a FROM `db1.t`", `SELECT a FROM "db1.t"`},
		{"already ansi", `SELECT a FROM "db1.t"`, `SELECT a FROM "db1.t"`},
		{"qualified and aliased", "SELECT * FROM phys.`db1.t` AS `db1.t`", `SELECT * FROM phys."db1.t" AS "db1.t"`},
		// D4: bytes inside a single-quoted literal are copied verbatim.
		{"backtick inside literal", "SELECT '`' FROM t", "SELECT '`' FROM t"},
		{"double quote inside literal", `SELECT '"' FROM t`, `SELECT '"' FROM t`},
		{"literal then identifier", "SELECT '`x`' FROM `t`", "SELECT '`x`' FROM \"t\""},
		{"backslash-escaped quote in literal", "SELECT 'a\\'`b' FROM `t`", "SELECT 'a\\'`b' FROM \"t\""},
		{"double quote inside backtick identifier", "SELECT * FROM `a\"b`", `SELECT * FROM "a\"b"`},
		{"unterminated literal", "SELECT 'a", "SELECT 'a"},
		{"unterminated identifier", "SELECT `a", `SELECT "a`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := NormalizeSIIdentifierQuotes(c.in); got != c.want {
				t.Errorf("NormalizeSIIdentifierQuotes(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestNormalizeSIIdentifierQuotes_LiteralQuotingBugIsNotHidden is the explicit
// Spec J D4 assertion. The global backtick->double-quote replace the C++
// runner used to perform would make these two inputs compare equal, hiding a
// quoting bug inside a string literal. They must stay distinct.
func TestNormalizeSIIdentifierQuotes_LiteralQuotingBugIsNotHidden(t *testing.T) {
	correct := "SELECT * FROM t WHERE s = '`raw`'"
	buggy := `SELECT * FROM t WHERE s = '"raw"'`
	if NormalizeSIIdentifierQuotes(correct) == NormalizeSIIdentifierQuotes(buggy) {
		t.Fatal("normalization must not equate a backtick and a double quote inside a string literal")
	}
}
```

- [x] **Step 2: Run it to confirm it fails**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
go test ./internal/harness/ -run TestNormalizeSIIdentifierQuotes
```

Expected: FAIL with `undefined: NormalizeSIIdentifierQuotes`.

- [x] **Step 3: Write the implementation**

Create `internal/harness/sinormalize.go`:

```go
package harness

import "strings"

// NormalizeSIIdentifierQuotes canonicalizes ClickHouse backtick identifier
// quoting to ANSI double quotes so the Go engine's SQL and the C++ engine's
// SQL are byte-comparable.
//
// rewriter-go emits ANSI double-quoted identifiers; rewriter-grpc formats with
// IdentifierQuotingStyle::Backticks. That difference is pure spelling, so the
// shared corpus normalizes it away. Everything else — spacing, parentheses,
// aliasing, and the contents of string literals — is compared verbatim.
//
// The scan is literal-aware (Spec J D4). Bytes inside a single-quoted string
// literal are copied unchanged, so a quoting change inside a literal is never
// normalized away and a quoting bug inside a literal cannot hide. The C++ port
// in rewriter-grpc/tests/si_normalize.h implements this identical state
// machine; the two must be changed together.
//
// States and transitions:
//
//	normal:  '\''  -> emit, go to literal
//	         '`'   -> emit '"', go to ident
//	         other -> emit
//	literal: '\\'  -> emit it and the following byte verbatim
//	         '\''  -> emit, go to normal
//	         other -> emit verbatim (backticks unchanged)
//	ident:   '\\'  -> emit it and the following byte verbatim
//	         '`'   -> emit '"', go to normal
//	         '"'   -> emit '\\' then '"' (re-escape under the new quote char)
//	         other -> emit
//
// An unterminated literal or identifier copies the remainder verbatim; the
// function never fails.
func NormalizeSIIdentifierQuotes(sql string) string {
	const (
		stNormal = iota
		stLiteral
		stIdent
	)
	var b strings.Builder
	b.Grow(len(sql))
	state := stNormal
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch state {
		case stNormal:
			switch c {
			case '\'':
				b.WriteByte(c)
				state = stLiteral
			case '`':
				b.WriteByte('"')
				state = stIdent
			default:
				b.WriteByte(c)
			}
		case stLiteral:
			switch c {
			case '\\':
				b.WriteByte(c)
				if i+1 < len(sql) {
					i++
					b.WriteByte(sql[i])
				}
			case '\'':
				b.WriteByte(c)
				state = stNormal
			default:
				b.WriteByte(c)
			}
		default: // stIdent
			switch c {
			case '\\':
				b.WriteByte(c)
				if i+1 < len(sql) {
					i++
					b.WriteByte(sql[i])
				}
			case '`':
				b.WriteByte('"')
				state = stNormal
			case '"':
				b.WriteByte('\\')
				b.WriteByte('"')
			default:
				b.WriteByte(c)
			}
		}
	}
	return b.String()
}
```

- [x] **Step 4: Run the tests to verify they pass**

```bash
go test ./internal/harness/ -run TestNormalizeSIIdentifierQuotes -v
```

Expected: PASS, 13 subtests plus the literal-bug test.

- [x] **Step 5: Commit**

```bash
git checkout -b feat/si-corpus-contract
git add internal/harness/sinormalize.go internal/harness/sinormalize_test.go
git commit -m "test: add literal-aware SI identifier-quote normalization"
```

---

## Task 5: rewriter-go — the frozen corpus schema, validator, and coverage report (D3)

**Files:**
- Create: `/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/sicorpus_test.go` (schema + loader + validator + report; helper-only, no `Test*` functions — this mirrors how `select_golden_test.go` already hosts `accessedJSON`, `checkAccessed`, `eqStrMap` and `semanticSQLEq` for its siblings)
- Test: `/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/sicorpus_contract_test.go`

**Interfaces:**
- Consumes: `NormalizeSIIdentifierQuotes` (Task 4); the existing test-file helpers `accessedJSON` (`select_golden_test.go:56`) and `remoteUpstreamJSON` (`dblevel_golden_test.go:62`).
- Produces, in package `harness`:
  - `const SICorpusPathEnv = "SI_CORPUS_PATH"`, `const UpdateGoldenEnv = "UPDATE_GOLDEN"`
  - `func SICorpusPath() string`
  - `type SICase struct` / `type SIDynamic struct` / `type SIArgs struct` / `type SITable struct`
  - `func LoadSICorpus(t *testing.T) []SICase` (strict: rejects unknown keys)
  - `func ValidateSICorpus(cases []SICase) []string` (one violation string per problem)
  - `func LegacyCoverageReport(raw []byte) (vacuous, deadWantSQL []string, err error)`

  Task 6 calls `LoadSICorpus`/`SICorpusPath`; Task 7 calls `SICase` and its `options()`; Task 9/10 mirror `ValidateSICorpus`'s rules in C++.

- [x] **Step 1: Write the corpus schema and validator**

Create `internal/harness/sicorpus_test.go`:

```go
package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/housegate/rewriter-proto/gen/pb"
)

// SICorpusPathEnv overrides which corpus file the harness loads. Used by the
// contract meta-test to validate a candidate file before it is published to
// rewriter-grpc.
const SICorpusPathEnv = "SI_CORPUS_PATH"

// UpdateGoldenEnv, set to exactly "1", switches golden tests in this repo from
// comparing to regenerating. Named after the repo's existing env-var
// convention (see OracleAddrEnv in oracle.go).
const UpdateGoldenEnv = "UPDATE_GOLDEN"

// SICorpusPath resolves the storage-integrity corpus file.
func SICorpusPath() string {
	if p := os.Getenv(SICorpusPathEnv); p != "" {
		return p
	}
	return filepath.Join("testdata", "storage_integrity_cases.json")
}

// SITable is one logical storage-integrity table's physical mapping.
type SITable struct {
	SafeTable           string   `json:"safe_table"`
	UnsafeTable         string   `json:"unsafe_table"`
	ExcludedUnsafeParts []string `json:"excluded_unsafe_parts,omitempty"`
}

// SIArgs is the storage_integrity block of a case's dynamic args.
type SIArgs struct {
	Tables              map[string]SITable `json:"tables"`
	ReadMode            string             `json:"read_mode,omitempty"` // "" | "SAFE" | "UNSAFE_LATEST" | "INVALID_99"
	ReservedRowIDColumn string             `json:"reserved_row_id_column,omitempty"`
}

// SIDynamic is the dynamic-args block of a case.
type SIDynamic struct {
	DatabaseMap                          map[string]string             `json:"database_map,omitempty"`
	KnownPhysicalDatabases               []string                      `json:"known_physical_databases,omitempty"`
	UpstreamLogical                      string                        `json:"upstream_logical_database_in_context,omitempty"`
	UpstreamPhysical                     string                        `json:"upstream_physical_database_in_context,omitempty"`
	Delim                                string                        `json:"delim,omitempty"`
	LogicalDatabaseToRemoteUpstreamIndex map[string]string             `json:"logical_database_to_remote_upstream_index,omitempty"`
	RemoteUpstreams                      map[string]remoteUpstreamJSON `json:"remote_upstreams,omitempty"`
	StorageIntegrity                     *SIArgs                       `json:"storage_integrity,omitempty"`
}

// SICase is the frozen corpus schema. Every key is listed here; the loader
// rejects any other key, which is what deletes `sql_exact` as a concept.
//
// Contract (Spec J D3), enforced by ValidateSICorpus:
//   - a reject case carries want_code != "Success" and a want_message_contains
//     substring, and pins no SQL;
//   - a success case pins SQL exactly — one want_sql when the engines agree,
//     or allow_sql_divergence plus both want_sql_go and want_sql_cpp when they
//     legitimately differ;
//   - want_sql_contains is an additional assertion only, and may not contain a
//     substring that is already present in the input SQL.
type SICase struct {
	Name                string            `json:"name"`
	SQL                 string            `json:"sql"`
	Dynamic             *SIDynamic        `json:"dynamic,omitempty"`
	WantCode            string            `json:"want_code,omitempty"`
	WantStmt            string            `json:"want_stmt,omitempty"`
	WantSQL             string            `json:"want_sql,omitempty"`
	WantSQLGo           string            `json:"want_sql_go,omitempty"`
	WantSQLCPP          string            `json:"want_sql_cpp,omitempty"`
	WantSQLContains     []string          `json:"want_sql_contains,omitempty"`
	WantSQLNotContains  []string          `json:"want_sql_not_contains,omitempty"`
	WantMessageContains string            `json:"want_message_contains,omitempty"`
	WantTableRewrites   map[string]string `json:"want_table_rewrites,omitempty"`
	WantAccessed        []accessedJSON    `json:"want_accessed,omitempty"`
	Reject              bool              `json:"reject,omitempty"`
	AllowSQLDivergence  bool              `json:"allow_sql_divergence,omitempty"`
	WantNoContractAck   bool              `json:"want_no_contract_ack,omitempty"`
}

// LoadSICorpus reads and strictly decodes the corpus. DisallowUnknownFields is
// what freezes the schema: a stray or deleted key (notably `sql_exact`) is a
// load error, not a silently ignored field.
func LoadSICorpus(t *testing.T) []SICase {
	t.Helper()
	b, err := os.ReadFile(SICorpusPath())
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var cases []SICase
	if err := dec.Decode(&cases); err != nil {
		t.Fatalf("decode %s: %v", SICorpusPath(), err)
	}
	return cases
}

var siKnownCodes = map[string]pb.RewriteCode{
	"Success":               pb.RewriteCode_Success,
	"SyntaxError":           pb.RewriteCode_SyntaxError,
	"RewriteError":          pb.RewriteCode_RewriteError,
	"UnsupportedStatement":  pb.RewriteCode_UnsupportedStatement,
	"InvalidRewriteRequest": pb.RewriteCode_InvalidRewriteRequest,
}

// ValidateSICorpus returns one string per contract violation. An empty slice
// means the corpus satisfies Spec J D3. The rule ids are stable so the C++
// mirror can emit the same text.
func ValidateSICorpus(cases []SICase) []string {
	var out []string
	seen := map[string]bool{}
	for _, c := range cases {
		add := func(rule, detail string) {
			out = append(out, fmt.Sprintf("%s: %s: %s", c.Name, rule, detail))
		}
		if strings.TrimSpace(c.Name) == "" {
			out = append(out, "<unnamed>: R1: name must be non-empty")
			continue
		}
		if seen[c.Name] {
			add("R1", "duplicate case name")
		}
		seen[c.Name] = true
		if strings.TrimSpace(c.SQL) == "" {
			add("R2", "sql must be non-empty")
		}
		if c.WantCode != "" {
			if _, ok := siKnownCodes[c.WantCode]; !ok {
				add("R6", "unknown want_code "+c.WantCode)
			}
		}
		isReject := c.WantCode != "" && c.WantCode != "Success"
		switch {
		case isReject:
			if !c.Reject {
				add("R3", "want_code != Success must set reject: true")
			}
			if c.WantMessageContains == "" {
				add("R3", "a reject case must carry want_message_contains")
			}
			if c.WantSQL != "" || c.WantSQLGo != "" || c.WantSQLCPP != "" {
				add("R3", "a reject case must not pin SQL (it echoes the input)")
			}
		default:
			if c.Reject {
				add("R3", "reject: true requires want_code != Success")
			}
			if c.AllowSQLDivergence {
				if c.WantSQLGo == "" || c.WantSQLCPP == "" {
					add("R3", "allow_sql_divergence requires both want_sql_go and want_sql_cpp")
				}
				if c.WantSQL != "" {
					add("R3", "allow_sql_divergence forbids want_sql; pin each engine separately")
				}
			} else {
				if c.WantSQL == "" {
					add("R3", "a success case must carry want_sql")
				}
				if c.WantSQLGo != "" || c.WantSQLCPP != "" {
					add("R3", "per-engine pins require allow_sql_divergence: true")
				}
			}
		}
		normalizedInput := NormalizeSIIdentifierQuotes(c.SQL)
		for _, sub := range c.WantSQLContains {
			if sub == "" {
				add("R4", "want_sql_contains entry must be non-empty")
				continue
			}
			if strings.Contains(normalizedInput, sub) {
				add("R4", fmt.Sprintf("vacuous want_sql_contains %q: already a substring of the input SQL, so a no-op rewriter passes", sub))
			}
		}
		for _, sub := range c.WantSQLNotContains {
			if sub == "" {
				add("R5", "want_sql_not_contains entry must be non-empty")
			}
		}
	}
	return out
}

// legacySICase is the pre-migration shape, decoded leniently so
// LegacyCoverageReport can read a corpus that still carries `sql_exact`.
type legacySICase struct {
	Name            string   `json:"name"`
	SQL             string   `json:"sql"`
	WantSQL         string   `json:"want_sql"`
	SQLExact        bool     `json:"sql_exact"`
	Reject          bool     `json:"reject"`
	WantSQLContains []string `json:"want_sql_contains"`
}

// LegacyCoverageReport quantifies what the pre-migration corpus actually
// asserted, so Spec J §5's acceptance sentence is checkable rather than
// anecdotal. It returns the non-reject cases whose want_sql_contains entries
// are all already present in the input SQL (vacuous), and the cases carrying a
// want_sql that the pre-fix C++ runner never compared because it gated that
// single comparison on sql_exact (dead).
func LegacyCoverageReport(raw []byte) (vacuous, deadWantSQL []string, err error) {
	var cases []legacySICase
	if err := json.Unmarshal(raw, &cases); err != nil {
		return nil, nil, fmt.Errorf("decode legacy corpus: %w", err)
	}
	for _, c := range cases {
		if !c.Reject && len(c.WantSQLContains) > 0 {
			allPresent := true
			normalized := NormalizeSIIdentifierQuotes(c.SQL)
			for _, sub := range c.WantSQLContains {
				if !strings.Contains(normalized, sub) {
					allPresent = false
					break
				}
			}
			if allPresent {
				vacuous = append(vacuous, c.Name)
			}
		}
		if c.WantSQL != "" && !c.SQLExact {
			deadWantSQL = append(deadWantSQL, c.Name)
		}
	}
	return vacuous, deadWantSQL, nil
}
```

- [x] **Step 2: Write the failing contract meta-test**

Create `internal/harness/sicorpus_contract_test.go`:

```go
package harness

import (
	"os/exec"
	"strings"
	"testing"
)

// TestSICorpusContract is the Spec J D3 gate: the shared corpus must satisfy
// the frozen schema. It needs no engine and no oracle, so it runs in the
// pure-Go CI lane.
func TestSICorpusContract(t *testing.T) {
	cases := LoadSICorpus(t)
	if len(cases) == 0 {
		t.Fatal("corpus is empty; the shared behaviour contract cannot be empty")
	}
	if violations := ValidateSICorpus(cases); len(violations) > 0 {
		t.Fatalf("corpus contract violations (%d):\n%s", len(violations), strings.Join(violations, "\n"))
	}
}

func TestValidateSICorpus_RejectsVacuousContains(t *testing.T) {
	got := ValidateSICorpus([]SICase{{
		Name:            "vacuous",
		SQL:             "SELECT a FROM other.u",
		WantCode:        "Success",
		WantSQL:         `SELECT a FROM phys."other.u"`,
		WantSQLContains: []string{"other.u"},
	}})
	if len(got) != 1 || !strings.Contains(got[0], "R4") {
		t.Fatalf("want one R4 violation, got %v", got)
	}
}

func TestValidateSICorpus_RejectsDivergenceWithoutPerEnginePins(t *testing.T) {
	got := ValidateSICorpus([]SICase{{
		Name:               "half-pinned",
		SQL:                "SELECT a FROM db1.t",
		WantCode:           "Success",
		AllowSQLDivergence: true,
		WantSQLGo:          `SELECT a FROM phys."db1.t"`,
	}})
	if len(got) != 1 || !strings.Contains(got[0], "want_sql_go and want_sql_cpp") {
		t.Fatalf("want one R3 violation, got %v", got)
	}
}

func TestValidateSICorpus_RejectsSuccessWithoutWantSQL(t *testing.T) {
	got := ValidateSICorpus([]SICase{{Name: "unpinned", SQL: "SELECT 1", WantCode: "Success"}})
	if len(got) != 1 || !strings.Contains(got[0], "must carry want_sql") {
		t.Fatalf("want one R3 violation, got %v", got)
	}
}

func TestValidateSICorpus_RejectsRejectWithoutMessage(t *testing.T) {
	got := ValidateSICorpus([]SICase{{
		Name: "silent-reject", SQL: "OPTIMIZE TABLE db1.t",
		WantCode: "UnsupportedStatement", Reject: true,
	}})
	if len(got) != 1 || !strings.Contains(got[0], "want_message_contains") {
		t.Fatalf("want one R3 violation, got %v", got)
	}
}

// TestSICorpusLegacyCoverageReport pins Spec J §5's acceptance sentence:
// "Running it against the pre-fix corpus must report the 7 vacuous cases and
// the 12 unasserted want_sql's." The pre-fix corpus is recovered from git at
// the spec's baseline commit so this stays reproducible without checking a
// second 145 KB file into the repo.
func TestSICorpusLegacyCoverageReport(t *testing.T) {
	const baseline = "dbac7bc"
	raw, err := exec.Command("git", "show",
		baseline+":internal/harness/testdata/storage_integrity_cases.json").Output()
	if err != nil {
		t.Skipf("pre-fix corpus unavailable at %s: %v", baseline, err)
	}
	vacuous, dead, err := LegacyCoverageReport(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(vacuous) != 7 {
		t.Errorf("vacuous want_sql_contains cases = %d, want 7: %v", len(vacuous), vacuous)
	}
	if len(dead) != 12 {
		t.Errorf("want_sql cases dead in the pre-fix C++ runner = %d, want 12: %v", len(dead), dead)
	}
	t.Logf("pre-fix vacuous: %v", vacuous)
	t.Logf("pre-fix dead want_sql: %v", dead)
}
```

- [x] **Step 3: Run the meta-test — expect the corpus to FAIL and the report to PASS**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
go test ./internal/harness/ -run 'TestSICorpus|TestValidateSICorpus' -v
```

Expected, before Task 6 migrates the data:
- `TestSICorpusContract` FAILS at `LoadSICorpus` with `json: unknown field "sql_exact"` — proof that the schema freeze deletes the key.
- The four `TestValidateSICorpus_*` unit tests PASS.
- `TestSICorpusLegacyCoverageReport` PASSES, logging `pre-fix vacuous:` with the 7 names from the baseline table and `pre-fix dead want_sql:` with the 12.

Copy both `t.Logf` lines into the PR description — they are spec §5's "must report the 7 vacuous cases and the 12 unasserted `want_sql`s" evidence.

- [x] **Step 4: Commit (with the contract test knowingly red)**

The corpus migration is Task 6; committing a red meta-test between two commits on the same branch is fine because the branch is not merged until Task 7 finishes.

```bash
git add internal/harness/sicorpus_test.go internal/harness/sicorpus_contract_test.go
git commit -m "test: freeze the SI corpus schema and add its validator

TestSICorpusContract is red until the corpus is migrated in the next
commit. Spec J D3."
```

---

## Task 6: rewriter-go — regenerate and migrate the 178-case corpus

**Files:**
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/storage_integrity_golden_test.go`
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json`

**Interfaces:**
- Consumes: `LoadSICorpus`, `SICorpusPath`, `SICase`, `UpdateGoldenEnv`, `NormalizeSIIdentifierQuotes` (Tasks 4-5); `newWriteRewriter` (`writes_golden_test.go:131`), `DialOracle`/`Oracle.Rewrite` (`oracle.go`).
- Produces: `func (c SICase) options() []*pb.RewriteOption` on the new type (moved from the deleted `siCase`), and a migrated corpus file that satisfies `ValidateSICorpus`. Task 9 copies that exact file into rewriter-grpc.

- [x] **Step 1: Move `options()` onto `SICase` and delete the old local types**

In `storage_integrity_golden_test.go`, delete `siDynamicJSON`, `siArgsJSON`, `siTableJSON`, `siCase` (lines 15-55) and `loadSICases` (lines 122-133). Re-point the receiver and the field names of the existing `options()` method (lines 84-120) at the new types:

```go
func (c SICase) options() []*pb.RewriteOption {
	if c.Dynamic == nil {
		return nil
	}
	da := &pb.RewriteTableDynamicArgs{
		DatabaseMap:                          c.Dynamic.DatabaseMap,
		KnownPhysicalDatabases:               c.Dynamic.KnownPhysicalDatabases,
		UpstreamLogicalDatabaseInContext:     c.Dynamic.UpstreamLogical,
		Delim:                                c.Dynamic.Delim,
		LogicalDatabaseToRemoteUpstreamIndex: c.Dynamic.LogicalDatabaseToRemoteUpstreamIndex,
	}
	if c.Dynamic.UpstreamPhysical != "" {
		da.UpstreamPhysicalDatabaseInContext = &c.Dynamic.UpstreamPhysical
	}
	if c.Dynamic.RemoteUpstreams != nil {
		da.RemoteUpstreams = map[string]*pb.RewriteTableDynamicArgs_RemoteUpstream{}
		for k, u := range c.Dynamic.RemoteUpstreams {
			da.RemoteUpstreams[k] = &pb.RewriteTableDynamicArgs_RemoteUpstream{Addr: u.Addr, User: u.User, Password: u.Password}
		}
	}
	if si := c.Dynamic.StorageIntegrity; si != nil {
		args := &pb.StorageIntegrityArgs{
			Tables:              map[string]*pb.StorageIntegrityArgs_Table{},
			ReadMode:            siReadModeByName[si.ReadMode],
			ReservedRowIdColumn: si.ReservedRowIDColumn,
			ContractVersion:     pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1,
		}
		for k, t := range si.Tables {
			args.Tables[k] = &pb.StorageIntegrityArgs_Table{
				SafeTable: t.SafeTable, UnsafeTable: t.UnsafeTable, ExcludedUnsafeParts: t.ExcludedUnsafeParts,
			}
		}
		da.StorageIntegrity = args
	}
	return []*pb.RewriteOption{{Op: pb.RewriteOp_TableNameRewrite,
		Value: &pb.RewriteOption_TableNameArgs{TableNameArgs: &pb.RewriteTableNameArgs{DynamicArgs: da}}}}
}
```

- [x] **Step 2: Add the regeneration writer**

Append to `storage_integrity_golden_test.go`:

```go
// writeSICorpus rewrites the corpus file deterministically.
//
// SetEscapeHTML(false) is mandatory: the corpus contains SQL such as
// `WHERE a > 1`, and the default encoder would turn every `>` into >,
// producing a 145 KB diff of pure escaping noise.
func writeSICorpus(t *testing.T, cases []SICase) {
	t.Helper()
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cases); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(SICorpusPath(), out.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("rewrote %s: %d cases, %d bytes", SICorpusPath(), len(cases), out.Len())
}
```

- [x] **Step 3: Add the `UPDATE_GOLDEN` branch to the runner**

Inside `TestStorageIntegrityGolden`, immediately after `semEq := semanticSQLEq(e)`, insert:

```go
	update := os.Getenv(UpdateGoldenEnv) == "1"
	if update && oracle == nil {
		t.Fatalf("%s=1 requires %s to point at a running rewriter-grpc so want_sql_cpp can be regenerated",
			UpdateGoldenEnv, OracleAddrEnv)
	}
	cases := LoadSICorpus(t)
```

Change the loop header from `for _, c := range loadSICases(t) {` to `for i := range cases {` with `c := &cases[i]` as the first statement, and make the sub-test body start with the regeneration short-circuit:

```go
	for i := range cases {
		c := &cases[i]
		t.Run(c.Name, func(t *testing.T) {
			r := newWriteRewriter(e, c.options())
			res, err := r.Rewrite(context.Background(), c.SQL, "acct")
			if err != nil {
				t.Fatalf("rewrite: %v", err)
			}
			if update {
				if c.Reject {
					return // reject cases echo the input; nothing to pin
				}
				want, oerr := oracle.Rewrite(c.SQL, c.options())
				if oerr != nil {
					t.Fatalf("oracle: %v", oerr)
				}
				goSQL := NormalizeSIIdentifierQuotes(res.SQL)
				cppSQL := NormalizeSIIdentifierQuotes(want.GetSqlAfterRewrite())
				c.WantSQL, c.WantSQLGo, c.WantSQLCPP = "", "", ""
				if goSQL == cppSQL {
					c.AllowSQLDivergence = false
					c.WantSQL = goSQL
				} else {
					c.AllowSQLDivergence = true
					c.WantSQLGo, c.WantSQLCPP = goSQL, cppSQL
				}
				return
			}
			// ... existing assertions, rewritten in Task 7 ...
		})
	}
	if update {
		writeSICorpus(t, cases)
	}
```

Add `"bytes"` and `"os"` to the import block if not already present (`os` is).

- [x] **Step 4: Build the FFI lib and start the C++ oracle**

Regeneration needs both engines. Build the Go engine's FFI lib once:

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
make ffi
```

Start the C++ oracle on the remote build box and forward its gRPC port:

```bash
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./scripts.sh rebuild"
ssh -p 30100 -L 50051:127.0.0.1:50051 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./build/clickhousegate_rewriter 50051"
```

Leave that session open. In another shell, confirm it answers:

```bash
grpcurl -plaintext 127.0.0.1:50051 list 2>/dev/null || \
  curl -sf -XPOST 127.0.0.1:50052/healthz && echo " oracle up"
```

- [x] **Step 5: Save the pre-migration corpus for the semantic diff**

```bash
cp internal/harness/testdata/storage_integrity_cases.json \
   /tmp/si_cases_prefix.json
```

- [x] **Step 6: Regenerate**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
UPDATE_GOLDEN=1 \
REWRITER_ORACLE_ADDR=127.0.0.1:50051 \
POLYGLOT_SQL_FFI_PATH="$PWD/third_party/lib/libpolyglot_sql_ffi.dylib" \
  go test ./internal/harness/ -run TestStorageIntegrityGolden -count=1 -v 2>&1 | tail -20
```

Expected: PASS with a final `rewrote testdata/storage_integrity_cases.json: 178 cases, N bytes` log line.

Note that the file is reformatted wholesale — the committed corpus is hand-formatted with compact inline objects, and the encoder emits fully expanded two-space indentation. The raw `git diff` is therefore useless for review; Step 7 exists precisely for that.

- [x] **Step 7: Review the migration semantically, not textually**

Compare the pre- and post-migration corpora key by key, ignoring the three keys the migration is allowed to change:

```bash
python3 - <<'EOF'
import json
old = {c['name']: c for c in json.load(open('/tmp/si_cases_prefix.json'))}
new = {c['name']: c for c in json.load(open('internal/harness/testdata/storage_integrity_cases.json'))}
assert set(old) == set(new), ("case set changed",
                             sorted(set(old) ^ set(new)))
mutable = {'want_sql', 'want_sql_go', 'want_sql_cpp', 'allow_sql_divergence', 'sql_exact'}
for name in sorted(old):
    o, n = old[name], new[name]
    for k in (set(o) | set(n)) - mutable:
        if o.get(k) != n.get(k):
            print(f"UNEXPECTED CHANGE {name}.{k}:\n  old={o.get(k)!r}\n  new={n.get(k)!r}")
print("cases:", len(new))
print("sql_exact still present:", sum(1 for c in new.values() if 'sql_exact' in c))
print("want_sql:", sum(1 for c in new.values() if c.get('want_sql')))
print("per-engine pinned:", sum(1 for c in new.values() if c.get('allow_sql_divergence')))
print("rejects:", sum(1 for c in new.values() if c.get('reject')))
EOF
```

Expected: no `UNEXPECTED CHANGE` lines, `cases: 178`, `sql_exact still present: 0`, `rejects: 147`, and `want_sql + per-engine pinned == 31`.

If any `UNEXPECTED CHANGE` line appears, stop: the regeneration touched something it should not have, and the schema struct's `omitempty` tags or field set is wrong.

- [x] **Step 8: Fix the three reject cases that carry no `want_message_contains`**

`si_optimize_rejected`, `si_detach_rejected` and `si_attach_rejected` violate R3. Run the engine once to read each one's real message:

```bash
POLYGLOT_SQL_FFI_PATH="$PWD/third_party/lib/libpolyglot_sql_ffi.dylib" \
  go test ./internal/harness/ -run 'TestStorageIntegrityGolden/si_(optimize|detach|attach)_rejected' -v -count=1 2>&1 | grep -i 'message'
```

Then hand-edit those three cases in the corpus, adding a `want_message_contains` whose value is a distinctive, stable fragment of the engine's actual rejection message — the same signed-lane phrasing the neighbouring SI reject cases already assert. Pick the substring from the sibling cases so the corpus stays consistent:

```bash
python3 -c "
import json
cs=json.load(open('internal/harness/testdata/storage_integrity_cases.json'))
seen={}
for c in cs:
    m=c.get('want_message_contains')
    if m: seen[m]=seen.get(m,0)+1
for m,n in sorted(seen.items(), key=lambda kv:-kv[1])[:8]: print(n, repr(m))
"
```

Use the dominant phrasing that actually appears in each of the three messages.

- [x] **Step 9: Remove the seven vacuous `want_sql_contains` entries**

D3 makes each of these a hard violation. For each case, delete the vacuous entries; the newly generated `want_sql` / per-engine pins carry the assertion. For the five cases where a physically-rewritten form exists, replace the vacuous entry with the rewritten substring **read out of the regenerated pin** (do not invent it):

| case | delete | replace with (read from the regenerated `want_sql` / `want_sql_go`) |
|---|---|---|
| `si_insert_rewrites_like_today` | `db1.t`, `VALUES (1)` | the physical target as it appears in the pin, e.g. `phys."db1.t"` |
| `non_si_table_unaffected` | `other.u` | the physical form of `other.u` in the pin |
| `si_ordinary_callable_in_values_allowed` | `other.u` | the physical form of `other.u` in the pin |
| `si_ordinary_callable_in_ignore_set_values_allowed` | `other.u` | the physical form of `other.u` in the pin |
| `si_ordinary_in_table_allowed` | `other.v` | the physical form of `other.v` in the pin |
| `si_ordinary_local_catalog_function_allowed` | `mergeTreeIndex`, `'other'`, `'u'` | nothing — delete the key entirely; the pin is the assertion |
| `si_ordinary_remote_engine_allowed` | `Remote`, `'other'`, `'u'` | nothing — delete the key entirely; the pin is the assertion |

The replacement value must be verbatim from the file, and it must not be a substring of the case's input SQL — the validator re-checks that in Step 10.

- [x] **Step 10: Run the contract meta-test — expect green**

```bash
go test ./internal/harness/ -run 'TestSICorpus|TestValidateSICorpus' -v
```

Expected: `TestSICorpusContract` PASS, all four validator unit tests PASS, `TestSICorpusLegacyCoverageReport` PASS (it reads the baseline blob from git, not the working tree, so it still reports 7 and 12).

- [x] **Step 11: Commit**

```bash
git add internal/harness/storage_integrity_golden_test.go \
        internal/harness/testdata/storage_integrity_cases.json
git commit -m "test: migrate the SI corpus to the frozen schema

Deletes sql_exact, pins every non-reject case exactly (one want_sql when
the engines agree, want_sql_go + want_sql_cpp when they legitimately
differ), gives the three silent reject cases a want_message_contains, and
removes the seven vacuous want_sql_contains entries. Spec J D3."
```

---

## Task 7: rewriter-go — exact comparison after normalization (D3/D4 runner half)

**Files:**
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/storage_integrity_golden_test.go:185-227`

**Interfaces:**
- Consumes: everything from Tasks 4-6.
- Produces: a Go runner in which every non-reject case is compared byte-exactly after normalization, and `allow_sql_divergence` selects `want_sql_go`. Task 9 mirrors this comparison in C++.

- [x] **Step 1: Replace the SQL-comparison block**

In `TestStorageIntegrityGolden`, replace the `if c.Reject { … } else { switch { case c.SQLExact: … case c.WantSQL != "": … } }` block with:

```go
			if c.Reject {
				if res.SQL != c.SQL {
					t.Errorf("reject must echo original SQL:\n got %q\nwant %q", res.SQL, c.SQL)
				}
			} else {
				// Spec J D3: comparison is always exact, after the shared
				// literal-aware normalization. sql_exact no longer exists.
				want := c.WantSQL
				if c.AllowSQLDivergence {
					want = c.WantSQLGo
				}
				got := NormalizeSIIdentifierQuotes(res.SQL)
				if norm := NormalizeSIIdentifierQuotes(want); got != norm {
					t.Errorf("sql (exact after normalization):\n got %q\nwant %q", got, norm)
				}
			}
```

- [x] **Step 2: Delete the now-unused semantic path for `want_sql`**

`semanticSQLEq` is still used by the oracle comparison further down; leave that call site alone. Remove nothing else. Confirm the package still builds:

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
go vet ./internal/harness/
```

Expected: no output.

- [x] **Step 3: Run the golden suite with the engine — expect green**

```bash
POLYGLOT_SQL_FFI_PATH="$PWD/third_party/lib/libpolyglot_sql_ffi.dylib" \
  go test ./internal/harness/ -run TestStorageIntegrityGolden -count=1 2>&1 | tail -5
```

Expected: `ok  github.com/housegate/rewriter-go/internal/harness`.

- [x] **Step 4: Prove the gate — corrupt one pin and observe red**

```bash
python3 - <<'EOF'
import json, pathlib
p = pathlib.Path('internal/harness/testdata/storage_integrity_cases.json')
cs = json.loads(p.read_text())
for c in cs:
    if c['name'] == 'si_describe_metadata_select':
        c['want_sql'] = c['want_sql'].replace('hg_safe', 'hg_unsafe')
p.write_text(json.dumps(cs, indent=2, ensure_ascii=False) + '\n')
EOF
POLYGLOT_SQL_FFI_PATH="$PWD/third_party/lib/libpolyglot_sql_ffi.dylib" \
  go test ./internal/harness/ -run TestStorageIntegrityGolden/si_describe_metadata_select -count=1 2>&1 | tail -8
```

Expected: FAIL with `sql (exact after normalization)` showing `hg_safe` got vs `hg_unsafe` want. Copy that into the PR description.

- [x] **Step 5: Prove a *divergent* case is also gated**

The pre-fix runner compared these only semantically and the C++ side not at all, so this second proof matters:

```bash
python3 - <<'EOF'
import json, pathlib
p = pathlib.Path('internal/harness/testdata/storage_integrity_cases.json')
cs = json.loads(p.read_text())
for c in cs:
    if c['name'] == 'si_safe_plain_select':
        c['want_sql_go'] = c['want_sql_go'].replace('EXCEPT', 'INTERSECT')
p.write_text(json.dumps(cs, indent=2, ensure_ascii=False) + '\n')
EOF
POLYGLOT_SQL_FFI_PATH="$PWD/third_party/lib/libpolyglot_sql_ffi.dylib" \
  go test ./internal/harness/ -run TestStorageIntegrityGolden/si_safe_plain_select -count=1 2>&1 | tail -8
```

Expected: FAIL on the same message.

- [x] **Step 6: Restore and observe green**

```bash
git checkout -- internal/harness/testdata/storage_integrity_cases.json
POLYGLOT_SQL_FFI_PATH="$PWD/third_party/lib/libpolyglot_sql_ffi.dylib" \
  go test ./internal/harness/ -count=1 2>&1 | tail -5
```

Expected: `ok`.

- [x] **Step 7: Record the published corpus digest**

```bash
shasum -a 256 internal/harness/testdata/storage_integrity_cases.json | tee /tmp/si_corpus_sha256.txt
```

Task 9 copies this exact file into rewriter-grpc and Task 10 asserts this digest in both repos.

- [x] **Step 8: Commit and open the PR**

```bash
git add internal/harness/storage_integrity_golden_test.go
git commit -m "test: compare SI corpus SQL exactly after shared normalization"
git push -u origin feat/si-corpus-contract
gh pr create --fill
```

---

## Task 8: rewriter-grpc — the C++ half of the shared normalization (D4)

**Files:**
- Create: `/Users/uranuswch/Dev/housegate/rewriter-grpc/tests/si_normalize.h`
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-grpc/tests/rewriter_test.cc` (delete `normalizeSIIdentifierQuotes` at `:4541-4547`, add two `TEST`s)

**Interfaces:**
- Consumes: the state machine specified in Task 4's doc comment.
- Produces: `si_corpus::NormalizeSIIdentifierQuotes(const std::string&) -> std::string` in `tests/si_normalize.h`. Tasks 9 and 10 include it.

- [x] **Step 1: Create the header**

`tests/si_normalize.h`:

```cpp
#pragma once

#include <string>

namespace si_corpus {

// NormalizeSIIdentifierQuotes canonicalizes ClickHouse backtick identifier
// quoting to ANSI double quotes so this engine's SQL and rewriter-go's SQL are
// byte-comparable.
//
// This is a literal-aware state machine (Spec J D4), NOT a global replace. The
// previous global std::replace('`','"') rewrote backticks inside string
// literals too, so a quoting bug inside a literal compared equal to correct
// output and could never fail a test.
//
// Exact mirror of rewriter-go internal/harness/sinormalize.go — change both
// together, and keep the transition table below in sync with that file's doc
// comment:
//
//   normal:  '\''  -> emit, go to literal
//            '`'   -> emit '"', go to ident
//            other -> emit
//   literal: '\\'  -> emit it and the following byte verbatim
//            '\''  -> emit, go to normal
//            other -> emit verbatim (backticks unchanged)
//   ident:   '\\'  -> emit it and the following byte verbatim
//            '`'   -> emit '"', go to normal
//            '"'   -> emit '\\' then '"' (re-escape under the new quote char)
//            other -> emit
//
// An unterminated literal or identifier copies the remainder verbatim.
inline std::string NormalizeSIIdentifierQuotes(const std::string &sql) {
  enum State { kNormal, kLiteral, kIdent };
  std::string out;
  out.reserve(sql.size());
  State state = kNormal;
  for (size_t i = 0; i < sql.size(); ++i) {
    const char c = sql[i];
    switch (state) {
      case kNormal:
        if (c == '\'') { out.push_back(c); state = kLiteral; }
        else if (c == '`') { out.push_back('"'); state = kIdent; }
        else { out.push_back(c); }
        break;
      case kLiteral:
        if (c == '\\') {
          out.push_back(c);
          if (i + 1 < sql.size()) { ++i; out.push_back(sql[i]); }
        } else if (c == '\'') { out.push_back(c); state = kNormal; }
        else { out.push_back(c); }
        break;
      case kIdent:
        if (c == '\\') {
          out.push_back(c);
          if (i + 1 < sql.size()) { ++i; out.push_back(sql[i]); }
        } else if (c == '`') { out.push_back('"'); state = kNormal; }
        else if (c == '"') { out.push_back('\\'); out.push_back('"'); }
        else { out.push_back(c); }
        break;
    }
  }
  return out;
}

}  // namespace si_corpus
```

- [x] **Step 2: Delete the old global-replace helper**

In `tests/rewriter_test.cc`, delete the whole `normalizeSIIdentifierQuotes` function (`:4541-4547`) and add `#include "si_normalize.h"` to the file's include block.

- [x] **Step 3: Add the mirror tests**

Add near the top of the storage-integrity section of `tests/rewriter_test.cc` (outside the anonymous namespace is fine; keep it adjacent to the corpus test):

```cpp
TEST(SINormalize, MirrorsGoStateMachine) {
  using si_corpus::NormalizeSIIdentifierQuotes;
  EXPECT_EQ(NormalizeSIIdentifierQuotes("SELECT 1"), "SELECT 1");
  EXPECT_EQ(NormalizeSIIdentifierQuotes("SELECT a FROM `db1.t`"), "SELECT a FROM \"db1.t\"");
  EXPECT_EQ(NormalizeSIIdentifierQuotes("SELECT a FROM \"db1.t\""), "SELECT a FROM \"db1.t\"");
  EXPECT_EQ(NormalizeSIIdentifierQuotes("SELECT * FROM phys.`db1.t` AS `db1.t`"),
            "SELECT * FROM phys.\"db1.t\" AS \"db1.t\"");
  EXPECT_EQ(NormalizeSIIdentifierQuotes("SELECT '`' FROM t"), "SELECT '`' FROM t");
  EXPECT_EQ(NormalizeSIIdentifierQuotes("SELECT '\"' FROM t"), "SELECT '\"' FROM t");
  EXPECT_EQ(NormalizeSIIdentifierQuotes("SELECT '`x`' FROM `t`"), "SELECT '`x`' FROM \"t\"");
  EXPECT_EQ(NormalizeSIIdentifierQuotes("SELECT 'a\\'`b' FROM `t`"), "SELECT 'a\\'`b' FROM \"t\"");
  EXPECT_EQ(NormalizeSIIdentifierQuotes("SELECT * FROM `a\"b`"), "SELECT * FROM \"a\\\"b\"");
  EXPECT_EQ(NormalizeSIIdentifierQuotes("SELECT 'a"), "SELECT 'a");
  EXPECT_EQ(NormalizeSIIdentifierQuotes("SELECT `a"), "SELECT \"a");
}

// Spec J D4: the pre-fix global replace made these two compare equal, hiding a
// quoting bug inside a string literal. They must stay distinct.
TEST(SINormalize, LiteralQuotingBugIsNotHidden) {
  EXPECT_NE(si_corpus::NormalizeSIIdentifierQuotes("SELECT * FROM t WHERE s = '`raw`'"),
            si_corpus::NormalizeSIIdentifierQuotes("SELECT * FROM t WHERE s = '\"raw\"'"));
}
```

- [x] **Step 4: Build and run on the build box**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git checkout -b feat/si-corpus-contract
rsync -az --delete \
  --exclude='.git' --exclude='build/' --exclude='clickHouse/' \
  --exclude='contrib' --exclude='docs/' \
  -e "ssh -p 30100" \
  ./ sentio@64.38.131.242:/home/sentio/chen/rewriter-grpc/
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./scripts.sh rebuild && ./build/rewriter_tests --gtest_filter='SINormalize.*'"
```

Expected: `[  PASSED  ] 2 tests.`

- [x] **Step 5: Commit**

```bash
git add tests/si_normalize.h tests/rewriter_test.cc
git commit -m "test: replace the global backtick replace with a literal-aware normalization"
```

---

## Task 9: rewriter-grpc — compare `want_sql` unconditionally, publish the migrated corpus

**Files:**
- Create: `/Users/uranuswch/Dev/housegate/rewriter-grpc/tests/si_corpus.h`
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-grpc/tests/rewriter_test.cc` (`:4504-4517` structs, `:4595-4639` loader, `:4685-4689` comparisons)
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json`
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-grpc/tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `si_corpus::NormalizeSIIdentifierQuotes` (Task 8); the migrated corpus file produced by Task 6.
- Produces: `si_corpus::Case` (with `ExpectedSQL()`), `si_corpus::LoadCases(path, &violations)`, `si_corpus::ValidateCorpus(cases)` — Task 10's meta-test consumes them.

- [x] **Step 1: Publish the migrated corpus byte-identically**

```bash
cp /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json \
   /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json
diff <(shasum -a 256 < /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json) \
     <(shasum -a 256 < /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json) \
  && echo "byte-identical"
```

Expected: `byte-identical`.

- [x] **Step 2: Create the C++ corpus schema, loader and validator**

`tests/si_corpus.h`:

```cpp
#pragma once

// The storage-integrity corpus contract, mirrored from
// rewriter-go/internal/harness/sicorpus_test.go. The JSON file is
// byte-identical in both repos; these rules and their ids (R1..R6) are
// identical too, so a case one runner rejects the other rejects as well.
//
// Spec J D3.

#include <cstdint>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include "si_normalize.h"

namespace si_corpus {

// Fingerprint is FNV-1a/64 over the exact file bytes. It is the byte-identity
// gate between the two repos: both pin the same value, so a one-sided edit to
// the shared corpus fails the other repo's build. Deliberately dependency-free
// (no OpenSSL in the test target); `shasum -a 256` remains the human-readable
// cross-check recorded in each PR.
inline uint64_t Fingerprint(const std::string &raw) {
  uint64_t h = 14695981039346656037ULL;
  for (unsigned char b : raw) {
    h ^= static_cast<uint64_t>(b);
    h *= 1099511628211ULL;
  }
  return h;
}

struct Accessed {
  std::string original_database, original_table, logical_database, physical_database;
  bool is_remote = false, is_storage_integrity = false;
};

struct Case {
  std::string name, sql, want_code, want_stmt;
  std::string want_sql, want_sql_go, want_sql_cpp, want_message_contains;
  bool has_dynamic = false, reject = false;
  bool allow_sql_divergence = false, want_no_contract_ack = false;
  Poco::JSON::Object::Ptr dynamic;  // raw; bound to proto by the test
  std::map<std::string, std::string> want_table_rewrites;
  bool has_table_rewrites = false, has_accessed = false;
  std::vector<Accessed> want_accessed;
  std::vector<std::string> want_sql_contains, want_sql_not_contains;

  // ExpectedSQL is the pin this engine must match exactly, after
  // normalization. allow_sql_divergence selects the C++-specific pin.
  const std::string &ExpectedSQL() const {
    return allow_sql_divergence ? want_sql_cpp : want_sql;
  }
};

inline const std::set<std::string> &CaseKeys() {
  static const std::set<std::string> keys = {
    "name", "sql", "dynamic", "want_code", "want_stmt", "want_sql",
    "want_sql_go", "want_sql_cpp", "want_sql_contains", "want_sql_not_contains",
    "want_message_contains", "want_table_rewrites", "want_accessed", "reject",
    "allow_sql_divergence", "want_no_contract_ack",
  };
  return keys;
}

inline const std::set<std::string> &DynamicKeys() {
  static const std::set<std::string> keys = {
    "database_map", "known_physical_databases",
    "upstream_logical_database_in_context", "upstream_physical_database_in_context",
    "delim", "logical_database_to_remote_upstream_index", "remote_upstreams",
    "storage_integrity",
  };
  return keys;
}

inline const std::set<std::string> &StorageIntegrityKeys() {
  static const std::set<std::string> keys = {"tables", "read_mode", "reserved_row_id_column"};
  return keys;
}

inline const std::set<std::string> &KnownCodes() {
  static const std::set<std::string> codes = {
    "Success", "SyntaxError", "RewriteError", "UnsupportedStatement", "InvalidRewriteRequest",
  };
  return codes;
}

inline void RejectUnknownKeys(const Poco::JSON::Object::Ptr &o,
                              const std::set<std::string> &allowed,
                              const std::string &path,
                              std::vector<std::string> *violations) {
  if (!o) return;
  for (const auto &name : o->getNames()) {
    if (allowed.count(name) == 0) {
      violations->push_back(path + ": R7: unknown key \"" + name +
                            "\" (the corpus schema is frozen; `sql_exact` is deleted)");
    }
  }
}

inline std::vector<std::string> JsonStrings(const Poco::JSON::Object::Ptr &o, const std::string &key) {
  std::vector<std::string> out;
  if (!o || !o->has(key) || o->isNull(key)) return out;
  auto arr = o->getArray(key);
  for (size_t i = 0; i < arr->size(); ++i) out.push_back(arr->getElement<std::string>(i));
  return out;
}

// LoadCases parses the corpus. Schema-freeze violations (unknown keys) are
// appended to *violations rather than thrown, so the meta-test can report all
// of them at once.
inline std::vector<Case> LoadCases(const std::string &path, std::vector<std::string> *violations) {
  std::ifstream in(path);
  std::stringstream buf;
  buf << in.rdbuf();
  Poco::JSON::Parser parser;
  auto arr = parser.parse(buf.str()).extract<Poco::JSON::Array::Ptr>();
  std::vector<Case> out;
  for (size_t i = 0; i < arr->size(); ++i) {
    auto o = arr->getObject(i);
    Case c;
    c.name = o->getValue<std::string>("name");
    c.sql = o->getValue<std::string>("sql");
    if (violations) RejectUnknownKeys(o, CaseKeys(), c.name, violations);
    if (o->has("want_code")) c.want_code = o->getValue<std::string>("want_code");
    if (o->has("want_stmt")) c.want_stmt = o->getValue<std::string>("want_stmt");
    if (o->has("want_sql")) c.want_sql = o->getValue<std::string>("want_sql");
    if (o->has("want_sql_go")) c.want_sql_go = o->getValue<std::string>("want_sql_go");
    if (o->has("want_sql_cpp")) c.want_sql_cpp = o->getValue<std::string>("want_sql_cpp");
    if (o->has("want_message_contains")) c.want_message_contains = o->getValue<std::string>("want_message_contains");
    if (o->has("reject")) c.reject = o->getValue<bool>("reject");
    if (o->has("allow_sql_divergence")) c.allow_sql_divergence = o->getValue<bool>("allow_sql_divergence");
    if (o->has("want_no_contract_ack")) c.want_no_contract_ack = o->getValue<bool>("want_no_contract_ack");
    if (o->has("dynamic")) {
      c.has_dynamic = true;
      c.dynamic = o->getObject("dynamic");
      if (violations) {
        RejectUnknownKeys(c.dynamic, DynamicKeys(), c.name + ".dynamic", violations);
        if (c.dynamic->has("storage_integrity")) {
          RejectUnknownKeys(c.dynamic->getObject("storage_integrity"), StorageIntegrityKeys(),
                            c.name + ".dynamic.storage_integrity", violations);
        }
      }
    }
    if (o->has("want_table_rewrites")) {
      c.has_table_rewrites = true;
      auto m = o->getObject("want_table_rewrites");
      for (const auto &kv : *m) c.want_table_rewrites[kv.first] = kv.second.toString();
    }
    if (o->has("want_accessed")) {
      c.has_accessed = true;
      auto a = o->getArray("want_accessed");
      for (size_t j = 0; j < a->size(); ++j) {
        auto e = a->getObject(j);
        Accessed w;
        if (e->has("original_database")) w.original_database = e->getValue<std::string>("original_database");
        if (e->has("original_table")) w.original_table = e->getValue<std::string>("original_table");
        if (e->has("logical_database")) w.logical_database = e->getValue<std::string>("logical_database");
        if (e->has("physical_database")) w.physical_database = e->getValue<std::string>("physical_database");
        if (e->has("is_remote")) w.is_remote = e->getValue<bool>("is_remote");
        if (e->has("is_storage_integrity")) w.is_storage_integrity = e->getValue<bool>("is_storage_integrity");
        c.want_accessed.push_back(w);
      }
    }
    c.want_sql_contains = JsonStrings(o, "want_sql_contains");
    c.want_sql_not_contains = JsonStrings(o, "want_sql_not_contains");
    out.push_back(std::move(c));
  }
  return out;
}

// ValidateCorpus mirrors ValidateSICorpus in rewriter-go rule for rule.
inline std::vector<std::string> ValidateCorpus(const std::vector<Case> &cases) {
  std::vector<std::string> out;
  std::set<std::string> seen;
  for (const auto &c : cases) {
    auto add = [&](const char *rule, const std::string &detail) {
      out.push_back(c.name + ": " + rule + ": " + detail);
    };
    if (c.name.empty()) { out.push_back("<unnamed>: R1: name must be non-empty"); continue; }
    if (!seen.insert(c.name).second) add("R1", "duplicate case name");
    if (c.sql.empty()) add("R2", "sql must be non-empty");
    if (!c.want_code.empty() && KnownCodes().count(c.want_code) == 0)
      add("R6", "unknown want_code " + c.want_code);
    const bool is_reject = !c.want_code.empty() && c.want_code != "Success";
    if (is_reject) {
      if (!c.reject) add("R3", "want_code != Success must set reject: true");
      if (c.want_message_contains.empty()) add("R3", "a reject case must carry want_message_contains");
      if (!c.want_sql.empty() || !c.want_sql_go.empty() || !c.want_sql_cpp.empty())
        add("R3", "a reject case must not pin SQL (it echoes the input)");
    } else {
      if (c.reject) add("R3", "reject: true requires want_code != Success");
      if (c.allow_sql_divergence) {
        if (c.want_sql_go.empty() || c.want_sql_cpp.empty())
          add("R3", "allow_sql_divergence requires both want_sql_go and want_sql_cpp");
        if (!c.want_sql.empty())
          add("R3", "allow_sql_divergence forbids want_sql; pin each engine separately");
      } else {
        if (c.want_sql.empty()) add("R3", "a success case must carry want_sql");
        if (!c.want_sql_go.empty() || !c.want_sql_cpp.empty())
          add("R3", "per-engine pins require allow_sql_divergence: true");
      }
    }
    const std::string normalized_input = NormalizeSIIdentifierQuotes(c.sql);
    for (const auto &sub : c.want_sql_contains) {
      if (sub.empty()) { add("R4", "want_sql_contains entry must be non-empty"); continue; }
      if (normalized_input.find(sub) != std::string::npos)
        add("R4", "vacuous want_sql_contains \"" + sub +
                  "\": already a substring of the input SQL, so a no-op rewriter passes");
    }
    for (const auto &sub : c.want_sql_not_contains) {
      if (sub.empty()) add("R5", "want_sql_not_contains entry must be non-empty");
    }
  }
  return out;
}

}  // namespace si_corpus
```

- [x] **Step 3: Delete the duplicated structs and loader from `rewriter_test.cc`**

Delete `struct SIAccessed` and `struct SIGoldenCase` (`:4504-4517`), `jsonStrings` (`:4533-4539`) and `loadSIGoldenCases` (`:4595-4639`). Add `#include "si_corpus.h"` and, inside the anonymous namespace, the two aliases the rest of the file uses:

```cpp
using SIGoldenCase = si_corpus::Case;
using SIAccessed = si_corpus::Accessed;

std::vector<SIGoldenCase> loadSIGoldenCases() {
  std::vector<std::string> ignored;
  return si_corpus::LoadCases(std::string(REWRITER_TEST_DATA_DIR) + "/storage_integrity_cases.json",
                              &ignored);
}
```

`applyStorageIntegrityArgs` stays in `rewriter_test.cc` (it depends on the proto types) but must now use `si_corpus::JsonStrings` instead of the deleted local `jsonStrings`.

- [x] **Step 4: Make the runner compare `want_sql` unconditionally**

Replace `rewriter_test.cc:4685-4689`:

```cpp
  if (c.reject) EXPECT_EQ(resp.sql_after_rewrite(), c.sql);
  if (!c.reject && c.sql_exact) EXPECT_EQ(resp.sql_after_rewrite(), c.want_sql);
  const std::string normalized_sql = normalizeSIIdentifierQuotes(resp.sql_after_rewrite());
  for (const auto &s : c.want_sql_contains) EXPECT_NE(normalized_sql.find(s), std::string::npos) << "missing " << s;
  for (const auto &s : c.want_sql_not_contains) EXPECT_EQ(normalized_sql.find(s), std::string::npos) << "unexpected " << s;
```

with:

```cpp
  const std::string normalized_sql = si_corpus::NormalizeSIIdentifierQuotes(resp.sql_after_rewrite());
  if (c.reject) {
    EXPECT_EQ(resp.sql_after_rewrite(), c.sql) << "a rejected statement must echo the input SQL";
  } else {
    // Spec J D3: every non-reject case is compared exactly after the shared
    // normalization. There is no sql_exact opt-in; twelve of the thirteen
    // want_sql strings used to be dead here.
    EXPECT_EQ(normalized_sql, si_corpus::NormalizeSIIdentifierQuotes(c.ExpectedSQL()))
        << "SQL pin mismatch for " << c.name;
  }
  for (const auto &s : c.want_sql_contains)
    EXPECT_NE(normalized_sql.find(s), std::string::npos) << "missing " << s;
  for (const auto &s : c.want_sql_not_contains)
    EXPECT_EQ(normalized_sql.find(s), std::string::npos) << "unexpected " << s;
```

- [x] **Step 5: Make the new headers reachable from the test target**

In `tests/CMakeLists.txt`, add the test directory to the include path so `#include "si_corpus.h"` resolves regardless of the compiler's working directory:

```cmake
target_include_directories(rewriter_tests PRIVATE
    ${REWRITER_PROTO_GENERATED_DIR}
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}
)
```

- [x] **Step 6: Build and run the corpus suite on the build box**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
rsync -az --delete \
  --exclude='.git' --exclude='build/' --exclude='clickHouse/' \
  --exclude='contrib' --exclude='docs/' \
  -e "ssh -p 30100" \
  ./ sentio@64.38.131.242:/home/sentio/chen/rewriter-grpc/
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./scripts.sh rebuild && ./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.*' 2>&1 | tail -30"
```

Expected: `[  PASSED  ] 178 tests.` If a case fails, the C++ engine genuinely disagrees with the pin generated in Task 6 — that is a real finding, not a fixture problem. Diagnose it against the SI design; **do not** relax the pin to make it green. If the disagreement is legitimate engine divergence, the fix is to re-run Task 6 Step 6 so the case gets per-engine pins.

- [x] **Step 7: Commit**

```bash
git add tests/si_corpus.h tests/rewriter_test.cc tests/CMakeLists.txt tests/testdata/storage_integrity_cases.json
git commit -m "test: compare every SI corpus want_sql, delete sql_exact

The C++ runner gated its single want_sql comparison on sql_exact, which
exactly one of 178 cases set, so twelve want_sql strings were dead and
allow_sql_divergence was never even parsed. Spec J D3."
```

---

## Task 10: both repos — the corpus contract meta-test and the byte-identity gate

**Files:**
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-grpc/tests/rewriter_test.cc` (add two `TEST`s)
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-grpc/tests/si_corpus.h` (add the pinned constants)
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/sicorpus_test.go` (add the pinned constants)
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/sicorpus_contract_test.go` (add the digest test)

**Interfaces:**
- Consumes: `si_corpus::ValidateCorpus`, `si_corpus::LoadCases`, `si_corpus::Fingerprint` (Task 9); `ValidateSICorpus`, `SICorpusPath` (Task 5).
- Produces: `SICorpusFingerprint`/`SICorpusBytes`/`SICorpusCases` in Go and `kCorpusFingerprint`/`kCorpusBytes`/`kCorpusCases` in C++, pinned to the same three values.

- [x] **Step 1: Compute the three pinned values from the published file**

```bash
python3 - <<'EOF'
import json, pathlib
p = pathlib.Path('/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json')
raw = p.read_bytes()
h = 14695981039346656037
for b in raw:
    h = ((h ^ b) * 1099511628211) & 0xFFFFFFFFFFFFFFFF
print("fingerprint =", h)
print("bytes       =", len(raw))
print("cases       =", len(json.loads(raw)))
EOF
shasum -a 256 /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json
```

Record all four numbers. `cases` must be `178`.

- [x] **Step 2: Pin them in rewriter-go**

Append to `internal/harness/sicorpus_test.go`:

```go
// The shared corpus must stay byte-identical to
// rewriter-grpc/tests/testdata/storage_integrity_cases.json. These three
// constants are pinned to the same values in rewriter-grpc's tests/si_corpus.h,
// so a one-sided edit fails the other repo's build. Update both in the same
// pair of PRs, and record the sha256 in each PR description.
const (
	SICorpusFingerprint uint64 = 0 // <-- replace with the Step 1 value
	SICorpusBytes       int    = 0 // <-- replace with the Step 1 value
	SICorpusCases       int    = 178
)

// siCorpusFingerprint is FNV-1a/64 over the exact file bytes, mirrored by
// si_corpus::Fingerprint in rewriter-grpc.
func siCorpusFingerprint(raw []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, b := range raw {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
}
```

Append to `internal/harness/sicorpus_contract_test.go`:

```go
func TestSICorpusIsBytePinned(t *testing.T) {
	raw, err := os.ReadFile(SICorpusPath())
	if err != nil {
		t.Fatal(err)
	}
	if got := len(raw); got != SICorpusBytes {
		t.Errorf("corpus size = %d bytes, want %d", got, SICorpusBytes)
	}
	if got := siCorpusFingerprint(raw); got != SICorpusFingerprint {
		t.Errorf("corpus fingerprint = %d, want %d\n"+
			"The shared corpus changed. Copy it to rewriter-grpc/tests/testdata/ and update the pinned\n"+
			"constants in BOTH internal/harness/sicorpus_test.go and rewriter-grpc/tests/si_corpus.h.",
			got, SICorpusFingerprint)
	}
	if got := len(LoadSICorpus(t)); got != SICorpusCases {
		t.Errorf("corpus case count = %d, want %d", got, SICorpusCases)
	}
}
```

Add `"os"` to that file's imports.

- [x] **Step 3: Pin them in rewriter-grpc**

Append inside `namespace si_corpus` in `tests/si_corpus.h`:

```cpp
// Pinned to the same values as rewriter-go internal/harness/sicorpus_test.go.
// A one-sided edit to the shared corpus fails here. Update both in the same
// pair of PRs.
constexpr uint64_t kCorpusFingerprint = 0;  // <-- replace with the Step 1 value
constexpr size_t kCorpusBytes = 0;          // <-- replace with the Step 1 value
constexpr size_t kCorpusCases = 178;
```

- [x] **Step 4: Add the C++ meta-tests**

Add to `tests/rewriter_test.cc`, next to the corpus suite:

```cpp
TEST(StorageIntegrityCorpus, SatisfiesTheFrozenContract) {
  std::vector<std::string> violations;
  const auto cases = si_corpus::LoadCases(
      std::string(REWRITER_TEST_DATA_DIR) + "/storage_integrity_cases.json", &violations);
  ASSERT_FALSE(cases.empty()) << "the shared behaviour contract cannot be empty";
  for (const auto &v : si_corpus::ValidateCorpus(cases)) violations.push_back(v);
  std::string report;
  for (const auto &v : violations) report += "\n  " + v;
  EXPECT_TRUE(violations.empty()) << "corpus contract violations (" << violations.size() << "):" << report;
}

TEST(StorageIntegrityCorpus, IsBytePinnedToRewriterGo) {
  std::ifstream in(std::string(REWRITER_TEST_DATA_DIR) + "/storage_integrity_cases.json",
                   std::ios::binary);
  std::stringstream buf;
  buf << in.rdbuf();
  const std::string raw = buf.str();
  EXPECT_EQ(raw.size(), si_corpus::kCorpusBytes);
  EXPECT_EQ(si_corpus::Fingerprint(raw), si_corpus::kCorpusFingerprint)
      << "The shared corpus changed. Copy rewriter-go's file verbatim and update the pinned "
         "constants in BOTH tests/si_corpus.h and rewriter-go internal/harness/sicorpus_test.go.";
  std::vector<std::string> ignored;
  EXPECT_EQ(si_corpus::LoadCases(std::string(REWRITER_TEST_DATA_DIR) + "/storage_integrity_cases.json",
                                 &ignored).size(),
            si_corpus::kCorpusCases);
}
```

- [x] **Step 5: Run both meta-tests — expect green**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
go test ./internal/harness/ -run 'TestSICorpus' -v
```

Expected: `TestSICorpusContract` PASS, `TestSICorpusIsBytePinned` PASS, `TestSICorpusLegacyCoverageReport` PASS reporting 7 and 12.

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
rsync -az --delete --exclude='.git' --exclude='build/' --exclude='clickHouse/' \
  --exclude='contrib' --exclude='docs/' -e "ssh -p 30100" \
  ./ sentio@64.38.131.242:/home/sentio/chen/rewriter-grpc/
ssh -p 30100 sentio@64.38.131.242 \
  "cd /home/sentio/chen/rewriter-grpc && ./scripts.sh rebuild && ./build/rewriter_tests --gtest_filter='StorageIntegrityCorpus.*:SINormalize.*'"
```

Expected: `[  PASSED  ] 4 tests.`

- [x] **Step 6: Prove the vacuity check fires — introduce a violating case, observe red, revert**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
python3 - <<'EOF'
import json, pathlib
p = pathlib.Path('internal/harness/testdata/storage_integrity_cases.json')
cs = json.loads(p.read_text())
for c in cs:
    if c['name'] == 'si_star_hides_rid':
        c['want_sql_contains'] = ['db1.t']   # already present in "SELECT * FROM db1.t"
p.write_text(json.dumps(cs, indent=2, ensure_ascii=False) + '\n')
EOF
go test ./internal/harness/ -run TestSICorpusContract 2>&1 | tail -6
```

Expected: FAIL with `si_star_hides_rid: R4: vacuous want_sql_contains "db1.t"`. Copy that into the PR description, then:

```bash
git checkout -- internal/harness/testdata/storage_integrity_cases.json
go test ./internal/harness/ -run TestSICorpusContract
```

Expected: `ok`.

- [x] **Step 7: Prove the byte-identity gate fires**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
printf '\n' >> internal/harness/testdata/storage_integrity_cases.json
go test ./internal/harness/ -run TestSICorpusIsBytePinned 2>&1 | tail -6
git checkout -- internal/harness/testdata/storage_integrity_cases.json
```

Expected: FAIL naming both the size and the fingerprint mismatch, then a clean tree.

- [x] **Step 8: Commit in both repos**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git add internal/harness/sicorpus_test.go internal/harness/sicorpus_contract_test.go
git commit -m "test: pin the shared corpus bytes and case count"

cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add tests/si_corpus.h tests/rewriter_test.cc
git commit -m "test: add the corpus contract meta-test and byte-identity pin"
```

---

## Task 11: rewriter-go — capture test compares by default (D7)

**Files:**
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-go/internal/engine/characterize_test.go:68-96`
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-go/internal/engine/testdata/ast-shapes/alter_delete.json`, `create_view.json`, `create_mv_to.json`
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-go/.github/workflows/ci.yml`

**Interfaces:**
- Consumes: nothing from other tasks. `internal/harness` imports `internal/engine`, so the `UPDATE_GOLDEN` constant cannot be shared without an import cycle — it is duplicated locally with a cross-reference comment.
- Produces: a `TestCharacterizeAST` that never writes unless `UPDATE_GOLDEN=1`, and three regenerated fixtures. `internal/engine/ast_test.go` and `nodes_test.go` stop being order-dependent on it.

- [x] **Step 1: Verify the drift before changing anything**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
make ffi
POLYGLOT_SQL_FFI_PATH="$PWD/third_party/lib/libpolyglot_sql_ffi.dylib" \
  go test ./internal/engine/ -run TestCharacterizeAST -count=1
git diff --stat -- internal/engine/testdata/ast-shapes/
git checkout -- internal/engine/testdata/ast-shapes/
```

Expected: exactly three changed files — `alter_delete.json`, `create_mv_to.json`, `create_view.json`. If more files move, the pinned polyglot differs from the one measured for this plan; record the new set and continue with it.

- [x] **Step 2: Rewrite the test to compare by default**

Replace `TestCharacterizeAST` (`characterize_test.go:68-96`) with:

```go
// updateGoldenEnv mirrors harness.UpdateGoldenEnv. internal/harness imports
// internal/engine, so the constant cannot be shared without an import cycle.
const updateGoldenEnv = "UPDATE_GOLDEN"

// TestCharacterizeAST pins the polyglot AST shapes that ast_test.go and
// nodes_test.go read. It COMPARES by default and only regenerates under
// UPDATE_GOLDEN=1 (Spec J D7). It used to overwrite the fixtures on every run
// and assert nothing, which meant a semantic change in the pinned engine
// rewrote the goldens silently and left the suite order-dependent between this
// writer and its two readers.
func TestCharacterizeAST(t *testing.T) {
	if os.Getenv("POLYGLOT_SQL_FFI_PATH") == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run via `make test`")
	}
	update := os.Getenv(updateGoldenEnv) == "1"

	c, err := polyglot.OpenDefault()
	if err != nil {
		t.Fatalf("OpenDefault: %v", err)
	}
	defer c.Close()

	dir := filepath.Join("testdata", "ast-shapes")
	if update {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	names := make([]string, 0, len(characterizeCases))
	for name := range characterizeCases {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		sql := characterizeCases[name]
		t.Run(name, func(t *testing.T) {
			ast, err := c.ParseOne(sql, "clickhouse")
			if err != nil {
				t.Fatalf("ParseOne(%q): %v", sql, err)
			}
			// json.MarshalIndent over a RawMessage is how the committed
			// fixtures were produced; keep it byte-for-byte so a formatting
			// change never masquerades as an AST change.
			got, err := json.MarshalIndent(json.RawMessage(ast), "", "  ")
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			out := filepath.Join(dir, name+".json")
			if update {
				if err := os.WriteFile(out, got, 0o644); err != nil {
					t.Fatal(err)
				}
				t.Logf("wrote %s (%d bytes)", out, len(got))
				return
			}
			want, err := os.ReadFile(out)
			if err != nil {
				t.Fatalf("read fixture %s: %v\nregenerate with: %s=1 make test", out, err, updateGoldenEnv)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("%s is stale against the pinned polyglot.\n"+
					"--- committed ---\n%s\n--- produced ---\n%s\n"+
					"If the new shape is correct, regenerate with: %s=1 make test",
					out, want, got, updateGoldenEnv)
			}
		})
	}
}
```

Update the import block to:

```go
import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	polyglot "github.com/tobilg/polyglot/packages/go"
)
```

- [x] **Step 3: Run it — expect three failures**

```bash
POLYGLOT_SQL_FFI_PATH="$PWD/third_party/lib/libpolyglot_sql_ffi.dylib" \
  go test ./internal/engine/ -run TestCharacterizeAST -count=1 2>&1 | grep -E '^\s+---|is stale' | head
```

Expected: `alter_delete`, `create_mv_to` and `create_view` reported stale; the other 38 pass. This is the D7 gate proving itself before the fixtures are fixed — record the output.

- [x] **Step 4: Regenerate the stale fixtures**

```bash
UPDATE_GOLDEN=1 POLYGLOT_SQL_FFI_PATH="$PWD/third_party/lib/libpolyglot_sql_ffi.dylib" \
  go test ./internal/engine/ -run TestCharacterizeAST -count=1
git diff --stat -- internal/engine/testdata/ast-shapes/
```

Expected: exactly the same three files changed. Review the diffs: `alter_delete.json`'s `Delete { where_clause }` node becomes `Raw { sql: "DELETE WHERE y=2" }`, and `create_view.json` / `create_mv_to.json` flip `"security_sql_style"` from `true` to `false`. These are the pinned polyglot's real output; the committed files predate the v0.8.1 bump.

- [x] **Step 5: Run the whole engine and harness suites**

```bash
POLYGLOT_SQL_FFI_PATH="$PWD/third_party/lib/libpolyglot_sql_ffi.dylib" \
  go test ./internal/... -count=1 2>&1 | tail -10
```

Expected: all packages `ok`. In particular `ast_test.go`'s `TestNodeKindFromSnapshots` and `nodes_test.go`'s readers must still pass against the regenerated fixtures — `alter_delete` is not in `TestNodeKindFromSnapshots`'s map, and `nodes_test.go` reads `select*`, `use` and `create_*` shapes.

- [x] **Step 6: Prove `make test` no longer mutates the tree**

```bash
make test
git status --porcelain
```

Expected: no output from `git status --porcelain` other than the files you are intentionally editing in this task. Spec §5's "`git diff --exit-code` clean after `make test` in rewriter-go" acceptance line:

```bash
git stash --include-untracked
make test
git diff --exit-code --ignore-submodules=all && echo "clean after make test"
git stash pop
```

Expected: `clean after make test`.

- [x] **Step 7: Add the CI step**

In `.github/workflows/ci.yml`, in the `ffi` job, insert after `- run: make test`:

```yaml
      # Spec J D7: no test may rewrite a tracked file. characterize_test.go
      # used to overwrite internal/engine/testdata/ast-shapes/*.json on every
      # run and assert nothing, so fixture drift was invisible. It now compares
      # and regenerates only under UPDATE_GOLDEN=1; this step is what keeps it
      # honest. --ignore-submodules=all so a cargo build inside
      # third_party/polyglot-src cannot trip it.
      - name: assert tests did not mutate tracked files
        run: git diff --exit-code --ignore-submodules=all
```

- [x] **Step 8: Prove the CI step fires**

Locally reproduce what CI would see if the test still wrote:

```bash
UPDATE_GOLDEN=1 POLYGLOT_SQL_FFI_PATH="$PWD/third_party/lib/libpolyglot_sql_ffi.dylib" \
  go test ./internal/engine/ -run TestCharacterizeAST -count=1 >/dev/null
python3 -c "
import pathlib
p = pathlib.Path('internal/engine/testdata/ast-shapes/use.json')
p.write_text(p.read_text().replace('\"USE db\"', '\"USE tampered\"'))
"
git diff --exit-code --ignore-submodules=all; echo "exit=$?"
git checkout -- internal/engine/testdata/ast-shapes/use.json
```

Expected: a diff plus `exit=1`, then a clean tree.

- [x] **Step 9: Commit and push**

```bash
git add internal/engine/characterize_test.go \
        internal/engine/testdata/ast-shapes/alter_delete.json \
        internal/engine/testdata/ast-shapes/create_view.json \
        internal/engine/testdata/ast-shapes/create_mv_to.json \
        .github/workflows/ci.yml
git commit -m "test: characterize AST shapes by comparison, not by overwrite

The capture test rewrote its tracked fixtures on every run and asserted
nothing, so the committed goldens silently diverged from the pinned
polyglot (alter_delete's WHERE clause degrades to Raw; create_view and
create_mv_to flip security_sql_style). Regenerate with UPDATE_GOLDEN=1.
Spec J D7."
git push
```

---

## Task 12: rewriter-grpc gets CI (D2)

**Files:**
- Create: `/Users/uranuswch/Dev/housegate/rewriter-grpc/.github/workflows/ci.yml`

**Interfaces:**
- Consumes: the corpus suite and meta-tests from Tasks 8-10 — the job's value is that it runs them.
- Produces: a `test` job on `pull_request` and `push: main`, named so branch protection can require it.

- [x] **Step 1: One-time build-host prerequisite**

`release.yml` uses `~/ci/rewriter` for tag builds. A PR job must not check a PR head out of that workdir while a release is building, so give CI its own. Create it once:

```bash
ssh -p 30100 sentio@64.38.131.242 'bash -s' <<'REMOTE'
set -euo pipefail
if [ ! -d ~/ci/rewriter-ci/.git ]; then
  mkdir -p ~/ci
  git clone https://github.com/housegate/rewriter-grpc.git ~/ci/rewriter-ci
fi
cd ~/ci/rewriter-ci
# Reuse the existing ClickHouse checkout — it is ~20 GB and must not be cloned
# a second time. scripts.sh's ensure_symlinks re-establishes the inner
# self-symlink and the contrib link on every invocation.
rm -rf clickHouse
ln -s /home/sentio/chen/rewriter-grpc/clickHouse clickHouse
git submodule sync -- third_party/rewriter-proto
git submodule update --init --depth=1 third_party/rewriter-proto
./scripts.sh rebuild
REMOTE
```

Expected: the first `rebuild` takes 15-20 minutes and leaves a warm `build/` so later PR runs are incremental. Confirm the binary exists:

```bash
ssh -p 30100 sentio@64.38.131.242 "ls -l ~/ci/rewriter-ci/build/rewriter_tests"
```

- [x] **Step 2: Create the workflow**

`.github/workflows/ci.yml`:

```yaml
name: ci

on:
  push:
    branches: [main]
  pull_request:

# ClickHouse does not compile on GitHub-hosted runners, so every build crosses
# the network to the single shared build box. release.yml uses this same
# concurrency group, so a PR build and a tag release never occupy the box at
# once. cancel-in-progress stays false: a PR must never cancel a release.
concurrency:
  group: build-box
  cancel-in-progress: false

jobs:
  test:
    name: Build & test (build box)
    # Secrets are not exposed to fork PRs, so the SSH step could not run and
    # the job would fail for reasons unrelated to the change. Skip it instead,
    # matching the fork-safety gates in housegate and arbiter-core.
    if: >-
      github.event_name != 'pull_request' ||
      github.event.pull_request.head.repo.full_name == github.repository
    runs-on: ubuntu-latest
    timeout-minutes: 90
    steps:
      - name: Checkout (metadata only)
        uses: actions/checkout@v4

      - name: Configure SSH
        env:
          SSH_KEY: ${{ secrets.BUILD_HOST_SSH_KEY }}
          KNOWN_HOSTS: ${{ secrets.BUILD_HOST_KNOWN_HOSTS }}
        run: |
          set -euo pipefail
          mkdir -p ~/.ssh
          chmod 700 ~/.ssh
          printf '%s\n' "$SSH_KEY"     > ~/.ssh/id_ed25519
          printf '%s\n' "$KNOWN_HOSTS" > ~/.ssh/known_hosts
          chmod 600 ~/.ssh/id_ed25519
          chmod 644 ~/.ssh/known_hosts

      - name: Check out the commit under test on the build host
        env:
          SHA: ${{ github.event.pull_request.head.sha || github.sha }}
        run: |
          set -euo pipefail
          ssh -p 30100 -o BatchMode=yes sentio@64.38.131.242 \
            "SHA='$SHA' bash -s" <<'REMOTE'
          set -euo pipefail
          cd ~/ci/rewriter-ci
          # A PR head commit is not on a branch, so fetch the pull refs too.
          git fetch --force --prune origin \
            '+refs/heads/*:refs/remotes/origin/*' \
            '+refs/pull/*/head:refs/remotes/origin/pr/*'
          git checkout -f "$SHA"
          # Preserve build/ (incremental ninja) and the shared ClickHouse
          # checkout; clean everything else so a file deleted in the PR is
          # actually gone on the box.
          git clean -fdx -e build -e clickHouse -e contrib
          if [ ! -L clickHouse ]; then
            rm -rf clickHouse
            ln -s /home/sentio/chen/rewriter-grpc/clickHouse clickHouse
          fi
          git submodule sync -- third_party/rewriter-proto
          git submodule update --init --depth=1 third_party/rewriter-proto
          REMOTE

      - name: Build (incremental)
        # `./scripts.sh test` calls `build`, which wipes build/ and costs
        # 15-20 min every run. `rebuild` re-runs cmake configure then does an
        # incremental ninja, so an ordinary PR is a few minutes.
        run: |
          set -euo pipefail
          ssh -p 30100 -o BatchMode=yes sentio@64.38.131.242 \
            "cd ~/ci/rewriter-ci && ./scripts.sh rebuild"

      - name: ctest
        run: |
          set -euo pipefail
          ssh -p 30100 -o BatchMode=yes sentio@64.38.131.242 \
            "cd ~/ci/rewriter-ci && ctest --test-dir build --output-on-failure"

      - name: Storage-integrity corpus suite
        # Spec J D2 requires this specifically, not just a compile. The count
        # guard catches a corpus that silently fails to load: a parametrized
        # suite instantiated over zero cases is a green run that asserts
        # nothing, which is the failure mode this whole spec exists to close.
        run: |
          set -euo pipefail
          ssh -p 30100 -o BatchMode=yes sentio@64.38.131.242 'bash -s' <<'REMOTE'
          set -euo pipefail
          cd ~/ci/rewriter-ci
          ./build/rewriter_tests \
            --gtest_filter='SpecG/StorageIntegrityGolden.*:StorageIntegrityCorpus.*:SINormalize.*'
          n="$(./build/rewriter_tests --gtest_filter='SpecG/StorageIntegrityGolden.*' \
                 --gtest_list_tests | grep -c '^  ' || true)"
          echo "storage-integrity corpus cases: $n"
          if [ "$n" -lt 178 ]; then
            echo "error: expected at least 178 corpus cases, found $n" >&2
            exit 1
          fi
          REMOTE
```

- [x] **Step 3: Push and observe green**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add .github/workflows/ci.yml
git commit -m "ci: build and test every PR on the build box"
git push -u origin feat/si-corpus-contract
gh pr create --fill
```

Expected: the `Build & test (build box)` job passes, with `storage-integrity corpus cases: 178` in the last step's log.

- [x] **Step 4: Prove the gate — break a corpus expectation, observe red**

```bash
python3 - <<'EOF'
import json, pathlib
p = pathlib.Path('tests/testdata/storage_integrity_cases.json')
cs = json.loads(p.read_text())
for c in cs:
    if c['name'] == 'si_exists_table_safe':
        c['want_sql'] = c['want_sql'].replace('hg_safe', 'hg_unsafe')
p.write_text(json.dumps(cs, indent=2, ensure_ascii=False) + '\n')
EOF
git add tests/testdata/storage_integrity_cases.json
git commit -m "test: deliberate corpus break to prove the new CI job (revert next)"
git push
```

Expected: the job fails. Two assertions should fire, and both matter:
1. `SpecG/StorageIntegrityGolden.MatchesSharedCorpus/si_exists_table_safe` — `SQL pin mismatch`. Under the pre-fix runner this case's `want_sql` was dead and the break would have gone unnoticed; that is the D3 regression this proves closed.
2. `StorageIntegrityCorpus.IsBytePinnedToRewriterGo` — the corpus no longer matches rewriter-go.

Copy both failures into the PR description.

- [x] **Step 5: Revert and observe green**

```bash
git revert --no-edit HEAD
git push
```

Expected: the job returns to green.

- [x] **Step 6: Ask the operator to make the job required**

Branch protection is not a repo file. Record in the PR description: *"Set `Build & test (build box)` as a required status check on `main` before merging."* This is an operator action; do not claim D2 complete without it.

---

## Task 13: sentio-node — execute the protocol-table drift refusal (D5, half one)

**Files:**
- Create: `/Users/uranuswch/Dev/sentio_xyz/sentio-node/standalone/storage_integrity_drift_ch_test.go`
- Modify: `/Users/uranuswch/Dev/sentio_xyz/sentio-node/standalone/BUILD.bazel`
- Modify: `/Users/uranuswch/Dev/sentio_xyz/sentio-node/README.md`

**Interfaces:**
- Consumes: `runStorageIntegrityProtocolBootstrap(ctx, ensure, crossCheck, register) error` (`standalone/standalone.go:448`); `smokeEnv(key, fallback string) string` (`standalone/storage_integrity_smoke_test.go:521`); `ddl.Pinned` / `ddl.Intents` / `ddl.EnsureProtocolTables` / `ddl.ModeCreateAndVerify` / `ddl.ErrProtocolTableDrift` / `ddl.CHTableName` / `ddl.UnsafeSettings` (arbiter-core `dataplane/ddl`); `config.StorageIntegrity{Unsafe,Safe,Promote}Database`.
- Produces: `TestStorageIntegrityProtocolTableDriftFailsBootstrap`, gated on `SENTIO_SI_CH_E2E=1` + `CH_ADDR`. Task 14's CI job runs exactly this test by name.

Read DEV-2 above before starting: this test deliberately does **not** call `standalone.Run`, because `Run` needs a full devnet before it reaches the bootstrap.

- [x] **Step 1: Write the test**

Create `standalone/storage_integrity_drift_ch_test.go`:

```go
package standalone

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/stretchr/testify/require"

	"compute-network-node/config"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// pinnedUnsafeSetting returns the frozen value of one hg_unsafe setting so the
// test tampers and restores against the real pin rather than a literal.
func pinnedUnsafeSetting(t *testing.T, name string) string {
	t.Helper()
	for _, s := range ddl.UnsafeSettings() {
		if s.Name == name {
			return s.Value
		}
	}
	t.Fatalf("%s is not a pinned hg_unsafe setting", name)
	return ""
}

// TestStorageIntegrityProtocolTableDriftFailsBootstrap executes the assertion
// Spec C's Phase 3 has never run: with one pinned protocol-table setting
// tampered, the storage-integrity bootstrap refuses before the cross-check and
// registration steps that gate the listener.
//
// It drives the production helper runStorageIntegrityProtocolBootstrap with
// the same ddl.EnsureProtocolTables(..., ddl.ModeCreateAndVerify, ...) call
// standalone.Run makes at standalone.go:319. It deliberately does not call Run
// itself: Run first starts sentio-core services, builds a NodeEnv against an
// Ethereum RPC, may register on-chain, and constructs a syncer plus two RPC
// servers, so reaching the bootstrap needs a devnet rather than a ClickHouse
// container. See the Spec J plan, DEV-2.
//
// Requires SENTIO_SI_CH_E2E=1 and CH_ADDR pointing at a ClickHouse whose
// config supplies Keeper via <keeper_server>/<zookeeper>; ReplicatedMergeTree
// cannot be created without one.
func TestStorageIntegrityProtocolTableDriftFailsBootstrap(t *testing.T) {
	if os.Getenv("SENTIO_SI_CH_E2E") != "1" {
		t.Skip("set SENTIO_SI_CH_E2E=1 with a Keeper-enabled ClickHouse on CH_ADDR to run the protocol-table drift acceptance")
	}
	addr := strings.TrimSpace(os.Getenv("CH_ADDR"))
	require.NotEmpty(t, addr, "CH_ADDR is required when SENTIO_SI_CH_E2E=1")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Username: smokeEnv("CH_USER", "default"),
			Password: smokeEnv("CH_PASSWORD", ""),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	require.NoError(t, conn.Ping(ctx))

	var zkRoots uint64
	require.NoError(t,
		conn.QueryRow(ctx, "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&zkRoots),
		"the CI ClickHouse must have Keeper configured (<keeper_server> or <zookeeper>)")

	schema := payloadexec.TableSchema{
		TableID:     fmt.Sprintf("drift.t_%d", time.Now().UnixNano()),
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "v", Type: "UInt64"},
		},
	}
	pinned := ddl.Pinned{
		UnsafeDB:      config.StorageIntegrityUnsafeDatabase,
		SafeDB:        config.StorageIntegritySafeDatabase,
		PromoteDB:     config.StorageIntegrityPromoteDatabase,
		NodeID:        "drift-ci-node",
		KeeperShardID: 0,
	}
	tables := []payloadexec.TableSchema{schema}
	physical := ddl.CHTableName(schema.TableID)
	pinnedThrow := pinnedUnsafeSetting(t, "parts_to_throw_insert")

	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancelCleanup()
		for _, db := range []string{pinned.UnsafeDB, pinned.SafeDB} {
			_ = conn.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s SYNC",
				quoteSmokeIdentifier(db), quoteSmokeIdentifier(physical)))
		}
	})

	ensure := func(ctx context.Context) error {
		if err := ddl.EnsureProtocolTables(ctx, conn, pinned, tables, ddl.ModeCreateAndVerify, nil); err != nil {
			return fmt.Errorf("ensure storage-integrity protocol tables: %w", err)
		}
		return nil
	}
	crossChecked, registered := false, false
	crossCheck := func(context.Context) error { crossChecked = true; return nil }
	register := func(context.Context) error { registered = true; return nil }

	// Phase 1: a clean bootstrap creates, verifies, cross-checks, registers.
	require.NoError(t, runStorageIntegrityProtocolBootstrap(ctx, ensure, crossCheck, register))
	require.True(t, crossChecked, "a clean bootstrap must reach the local schema cross-check")
	require.True(t, registered, "a clean bootstrap must reach registration")

	// Phase 2: re-running over the existing tables is idempotent.
	require.NoError(t, runStorageIntegrityProtocolBootstrap(ctx, ensure, crossCheck, register))

	// Phase 3: tamper one pinned setting; startup must fail closed.
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.%s MODIFY SETTING parts_to_throw_insert = 2999",
		quoteSmokeIdentifier(pinned.UnsafeDB), quoteSmokeIdentifier(physical),
	)))
	crossChecked, registered = false, false
	driftErr := runStorageIntegrityProtocolBootstrap(ctx, ensure, crossCheck, register)
	require.Error(t, driftErr)
	require.ErrorIs(t, driftErr, ddl.ErrProtocolTableDrift)
	require.Contains(t, driftErr.Error(), "parts_to_throw_insert")
	require.Contains(t, driftErr.Error(), physical)
	require.False(t, crossChecked, "drift must fail closed before the local schema cross-check")
	require.False(t, registered, "drift must fail closed before registration")

	// Phase 4: restoring the pin makes the bootstrap green again, so the
	// refusal is drift-specific rather than a permanent poison.
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.%s MODIFY SETTING parts_to_throw_insert = %s",
		quoteSmokeIdentifier(pinned.UnsafeDB), quoteSmokeIdentifier(physical), pinnedThrow,
	)))
	require.NoError(t, runStorageIntegrityProtocolBootstrap(ctx, ensure, crossCheck, register))
	require.True(t, registered)
}
```

`quoteSmokeIdentifier` already exists at `standalone/storage_integrity_smoke_test.go:352` in this package; reuse it rather than adding a second quoter.

- [x] **Step 2: Regenerate the Bazel target**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
bazel run //:gazelle
git diff -- standalone/BUILD.bazel
```

Expected: `storage_integrity_drift_ch_test.go` added to `go_test.srcs` and `@housegate//pkg/lthash` added to `deps`. If gazelle does not run in this repo, add both by hand.

- [x] **Step 3: Confirm the test skips without the env gate**

```bash
bazel test //standalone:standalone_test --test_output=all \
  --test_filter='TestStorageIntegrityProtocolTableDriftFailsBootstrap' 2>&1 | tail -20
```

Expected: PASS with `--- SKIP` and the `set SENTIO_SI_CH_E2E=1` message. This is what keeps `bazel test //...` green for everyone else.

- [x] **Step 4: Run it for real against a local ClickHouse with Keeper**

```bash
docker rm -f sn-drift-ch >/dev/null 2>&1 || true
docker run -d --rm --name sn-drift-ch -p 19000:9000 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/scripts/ci/clickhouse-keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
  clickhouse/clickhouse-server:25.8
```

(The XML file arrives in Task 14 Step 1; if you are running Task 13 first, copy it from arbiter-core now: `mkdir -p scripts/ci && cp /Users/uranuswch/Dev/sentio_xyz/arbiter-core/scripts/ci/clickhouse-keeper.xml scripts/ci/`.)

```bash
for _ in $(seq 1 30); do
  docker exec sn-drift-ch clickhouse-client --query "SELECT count() FROM system.zookeeper WHERE path='/'" >/dev/null 2>&1 && break
  sleep 1
done
SENTIO_SI_CH_E2E=1 CH_ADDR=127.0.0.1:19000 \
  bazel test //standalone:standalone_test \
    --test_filter='TestStorageIntegrityProtocolTableDriftFailsBootstrap' \
    --test_env=SENTIO_SI_CH_E2E --test_env=CH_ADDR \
    --test_timeout=900 --test_output=all 2>&1 | tail -30
```

Expected: PASS. The whole point is Phase 3 — if the run passes without ever entering Phase 3, the gate did not execute.

- [x] **Step 5: Prove Phase 3 is load-bearing**

Temporarily change `2999` to the pinned value (`3000`) so nothing actually drifts:

```bash
sed -i.bak 's/parts_to_throw_insert = 2999/parts_to_throw_insert = 3000/' \
  standalone/storage_integrity_drift_ch_test.go
SENTIO_SI_CH_E2E=1 CH_ADDR=127.0.0.1:19000 \
  bazel test //standalone:standalone_test \
    --test_filter='TestStorageIntegrityProtocolTableDriftFailsBootstrap' \
    --test_env=SENTIO_SI_CH_E2E --test_env=CH_ADDR \
    --test_timeout=900 --test_output=all 2>&1 | tail -20
mv standalone/storage_integrity_drift_ch_test.go.bak standalone/storage_integrity_drift_ch_test.go
```

Expected: FAIL at `require.Error(t, driftErr)` — no drift means no refusal. That is the proof the assertion is real. Copy it into the PR description, then re-run Step 4 and confirm PASS again.

- [x] **Step 6: Tear down the local container**

```bash
docker rm -f sn-drift-ch
```

- [x] **Step 7: Document the new gate in the README**

In `README.md`, immediately after the `TestSchemaRegistryPhaseBSmoke` block, add:

````markdown
`TestSchemaRegistryPhaseBSmoke` needs the full stack — an Ethereum RPC with the deployed contracts, a running arbiter, Redis, sentio-core services and pre-minted JWSs — so it is an operator-run acceptance, not a CI job. Its Phase-3 assertion (a tampered pinned protocol-table setting must stop a node before it opens its listener) is additionally executed on its own in CI by `TestStorageIntegrityProtocolTableDriftFailsBootstrap`, which drives the same production bootstrap helper against a ClickHouse with Keeper and needs no chain:

```bash
SENTIO_SI_CH_E2E=1 CH_ADDR=127.0.0.1:9000 \
  go test ./standalone -run TestStorageIntegrityProtocolTableDriftFailsBootstrap -count=1 -timeout=5m
```
````

- [x] **Step 8: Commit**

```bash
git checkout -b feat/si-drift-ci
git add standalone/storage_integrity_drift_ch_test.go standalone/BUILD.bazel scripts/ci/clickhouse-keeper.xml README.md
git commit -m "test: execute the storage-integrity protocol-table drift refusal

Spec C's only end-to-end drift proof lived inside a smoke test that
needs a full devnet and has never run. This drives the same production
bootstrap helper and the same EnsureProtocolTables(ModeCreateAndVerify)
call against a real ClickHouse with Keeper, with no chain dependency.
Spec J D5."
```

---

## Task 14: sentio-node CI gains a ClickHouse service (D5, half two)

**Files:**
- Create: `/Users/uranuswch/Dev/sentio_xyz/sentio-node/scripts/ci/clickhouse-keeper.xml`
- Modify: `/Users/uranuswch/Dev/sentio_xyz/sentio-node/.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `TestStorageIntegrityProtocolTableDriftFailsBootstrap` (Task 13) and the `SENTIO_SI_CH_E2E` / `CH_ADDR` contract.
- Produces: an `integration-clickhouse` job whose shape mirrors arbiter-core's.

- [x] **Step 1: Copy the Keeper config**

sentio-node's own README already points at this exact file as the reference configuration:

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
mkdir -p scripts/ci
cp /Users/uranuswch/Dev/sentio_xyz/arbiter-core/scripts/ci/clickhouse-keeper.xml scripts/ci/
head -5 scripts/ci/clickhouse-keeper.xml
```

Expected: the `<clickhouse><keeper_server><tcp_port>9181</tcp_port>` preamble.

Note on scope: arbiter-core's job runs **two** nodes with `clickhouse-shared-keeper-{server,client}.xml` because its `//verifier` and `//snode` tests assert cross-replica replication. The drift acceptance needs one node with a Keeper, which is what `clickhouse-keeper.xml` provides and what sentio-node's README already prescribes. Record in the job comment that the two-node shared-Keeper pair is the shape to copy if sentio-node later adds a replication assertion.

- [x] **Step 2: Add the job**

Append to `.github/workflows/ci.yml`, after the `build` job and before `docker-push`:

```yaml
  integration-clickhouse:
    # Modelled on arbiter-core's integration-clickhouse job. That one runs two
    # nodes with scripts/ci/clickhouse-shared-keeper-{server,client}.xml
    # because its tests assert cross-replica replication; the storage-integrity
    # drift acceptance needs one node that has a Keeper, so it uses the
    # single-node clickhouse-keeper.xml the README already prescribes. Copy the
    # two-node pair here if a replication assertion is ever added.
    runs-on: [self-hosted]
    timeout-minutes: 30
    permissions:
      contents: read
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          submodules: recursive

      - name: Setup Bazel
        uses: bazelbuild/setup-bazelisk@v3

      - name: start ClickHouse with Keeper
        run: |
          set -euo pipefail
          suffix="${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
          ch="sentio-node-ci-clickhouse-${suffix}"
          docker rm -f "${ch}" >/dev/null 2>&1 || true
          docker run -d --rm \
            --name "${ch}" \
            --hostname "${ch}" \
            -p 127.0.0.1::9000 \
            -e CLICKHOUSE_SKIP_USER_SETUP=1 \
            -v "$PWD/scripts/ci/clickhouse-keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
            clickhouse/clickhouse-server:25.8
          port="$(docker inspect --format '{{(index (index .NetworkSettings.Ports "9000/tcp") 0).HostPort}}' "${ch}")"
          {
            echo "CH_ADDR=127.0.0.1:${port}"
            echo "CH_CONTAINER=${ch}"
          } >> "$GITHUB_ENV"

      - name: wait for ClickHouse and Keeper
        run: |
          for _ in $(seq 1 30); do
            if docker exec "${CH_CONTAINER}" clickhouse-client --query "SELECT 1" >/dev/null 2>&1 && \
               docker exec "${CH_CONTAINER}" clickhouse-client --query "SELECT count() FROM system.zookeeper WHERE path = '/'" >/dev/null 2>&1; then
              exit 0
            fi
            sleep 1
          done
          docker logs "${CH_CONTAINER}"
          exit 1

      - name: storage-integrity protocol-table drift
        env:
          SENTIO_SI_CH_E2E: "1"
        run: |
          bazel test //standalone:standalone_test \
            --test_filter='TestStorageIntegrityProtocolTableDriftFailsBootstrap' \
            --test_env=SENTIO_SI_CH_E2E \
            --test_env=CH_ADDR \
            --test_timeout=900 \
            --test_output=all

      - name: stop ClickHouse
        if: always()
        run: |
          suffix="${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
          docker rm -f "${CH_CONTAINER:-sentio-node-ci-clickhouse-${suffix}}" >/dev/null 2>&1 || true
```

- [x] **Step 3: Push and observe green**

```bash
git add scripts/ci/clickhouse-keeper.xml .github/workflows/ci.yml
git commit -m "ci: run the storage-integrity drift acceptance against ClickHouse"
git push -u origin feat/si-drift-ci
gh pr create --fill
```

Expected: `integration-clickhouse` green, and the `--test_output=all` log shows the test **running**, not skipping. Confirm the log does not contain `SKIP`.

- [x] **Step 4: Prove the gate — remove the tamper, observe red**

```bash
sed -i.bak 's/parts_to_throw_insert = 2999/parts_to_throw_insert = 3000/' \
  standalone/storage_integrity_drift_ch_test.go
rm -f standalone/storage_integrity_drift_ch_test.go.bak
git commit -am "test: deliberate no-op tamper to prove the drift gate (revert next)"
git push
```

Expected: `integration-clickhouse` fails at `require.Error(t, driftErr)`. Copy the failure into the PR description.

- [x] **Step 5: Revert and observe green**

```bash
git revert --no-edit HEAD
git push
```

Expected: `integration-clickhouse` green again with the test running.

- [x] **Step 6: Confirm the existing suite is unaffected**

The `build` job runs `bazel test //...`, which now includes the new test with `SENTIO_SI_CH_E2E` unset. Check its log shows the target passing (the test skips inside it). Expected: no new failures in the `build` job.

---

## Task 14b: sentio-node — prove the production wiring, not just the helper (D5, half three)

**Files:**
- Modify: `standalone/standalone.go`
- Modify: `standalone/storage_integrity_bootstrap_test.go`

**Interfaces:**
- Consumes: `runStorageIntegrityProtocolBootstrap` (`standalone/standalone.go:448`) and its call site at `:316`.
- Produces: `storageIntegrityBootPlan` — an ordered, named step list that `Run` executes and the test asserts against.

**Why this task exists.** Task 13 proves the drift *logic* by driving the same production helper. It does not prove that `standalone.Run` calls that helper, or that it calls it before opening listeners — and the existing `storage_integrity_bootstrap_test.go` cannot close that gap: the 2026-08-19 review found it tautological, because it asserts an order over three closures the test itself constructs. Swapping the first two arguments at the real call site leaves it green. Review of this plan raised the same objection about Task 13's substitute. This task makes the ordering a value produced by production code.

- [x] **Step 1: Write the failing test**

The previous draft of this task was wrong in two ways, both caught in review, and the replacement exists to avoid them: a plan built with no runnables cannot satisfy a `NotNil` assertion, and — the substantive point — assigning names to closures *by position* means swapping the closures at the call site just mislabels them, so the test still passes. The names must be bound to the functions in production code, and the test must check that binding, not the order of a list it was handed.

Replace the body of `standalone/storage_integrity_bootstrap_test.go`:

```go
// funcIdentity returns the fully-qualified name of f, so a step's binding can
// be compared against the function production code is supposed to have put
// there. Anonymous closures would defeat this, which is why the three steps
// are named methods.
func funcIdentity(f func(context.Context) error) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

// The plan is built by production code with each name bound to a named
// method, so swapping two bindings at the call site changes what this test
// observes. The earlier version of this test asserted an order over closures
// it defined itself and could not detect that swap.
func TestStorageIntegrityBootPlanBindsEachNameToItsStep(t *testing.T) {
	deps := &storageIntegrityBootDeps{}
	plan := deps.bootPlan()

	var names []string
	for _, step := range plan {
		names = append(names, step.Name)
		require.NotNil(t, step.Run, "step %q must carry a runnable", step.Name)
	}
	require.Equal(t, []string{"ensure-protocol-tables", "cross-check-schemas", "register-role"}, names)

	// Each name must be bound to the method of the same meaning.
	require.Contains(t, funcIdentity(plan[0].Run), "ensureProtocolTables")
	require.Contains(t, funcIdentity(plan[1].Run), "crossCheckSchemas")
	require.Contains(t, funcIdentity(plan[2].Run), "registerRole")
}

// Execution order is the plan's order, and a failing step stops the ones
// after it — a node whose protocol tables drifted must never register.
func TestRunStorageIntegrityProtocolBootstrapStopsAtFirstFailure(t *testing.T) {
	var ran []string
	plan := []storageIntegrityBootStep{
		{Name: "ensure-protocol-tables", Run: func(context.Context) error {
			ran = append(ran, "ensure")
			return ddl.ErrProtocolTableDrift
		}},
		{Name: "cross-check-schemas", Run: func(context.Context) error { ran = append(ran, "cross"); return nil }},
		{Name: "register-role", Run: func(context.Context) error { ran = append(ran, "register"); return nil }},
	}
	err := runStorageIntegrityProtocolBootstrap(context.Background(), plan)
	require.ErrorIs(t, err, ddl.ErrProtocolTableDrift)
	require.Contains(t, err.Error(), "ensure-protocol-tables", "the failing step must name itself")
	require.Equal(t, []string{"ensure"}, ran, "later steps must not run after a failure")
}

// The bootstrap must complete before any listener opens, so a drifted node
// never serves traffic.
func TestStorageIntegrityBootstrapPrecedesListeners(t *testing.T) {
	require.Less(t,
		sourceLineOf(t, "runStorageIntegrityProtocolBootstrap("),
		sourceLineOf(t, "startHousegateListener("),
		"protocol-table bootstrap must run before the housegate listener starts")
}
```

`sourceLineOf` is a small helper in the same file that scans `standalone.go` for the first occurrence of a literal and fails if it is absent, so renaming or deleting either call site fails the test rather than silently passing.

- [x] **Step 2: Run it to verify it fails**

Run: `bazel test //standalone:standalone_test --test_filter='TestStorageIntegrityBootPlan|TestRunStorageIntegrityProtocolBootstrapStops|TestStorageIntegrityBootstrapPrecedes' --test_output=all`
Expected: FAIL — `undefined: storageIntegrityBootDeps`.

- [x] **Step 3: Bind the names in production code**

In `standalone/standalone.go`, replace the three inline closures at `:316-340` with named methods on a deps struct, and have the plan bind each name to its method:

```go
// storageIntegrityBootStep is one named phase of the SI bootstrap. The name is
// bound to the method here, in production code, so a test can verify the
// binding rather than an order it was handed.
type storageIntegrityBootStep struct {
	Name string
	Run  func(context.Context) error
}

type storageIntegrityBootDeps struct {
	conn       clickhouse.Conn
	pinned     ddl.Pinned
	tables     []payloadexec.TableSchema
	mode       ddl.Mode
	netState   registry.Registry
	schemaSets storageIntegritySchemaSets
	networkID  string
	role       *snode.Role
	logger     *slog.Logger
}

func (d *storageIntegrityBootDeps) ensureProtocolTables(ctx context.Context) error {
	if err := ddl.EnsureProtocolTables(ctx, d.conn, d.pinned, d.tables, d.mode, d.logger); err != nil {
		return fmt.Errorf("ensure storage-integrity protocol tables: %w", err)
	}
	return nil
}

func (d *storageIntegrityBootDeps) crossCheckSchemas(ctx context.Context) error { /* existing body */ }

func (d *storageIntegrityBootDeps) registerRole(ctx context.Context) error { /* existing body */ }

// bootPlan is the frozen order. Each entry pairs a name with the method that
// implements it; swapping two pairings is what the binding test detects.
func (d *storageIntegrityBootDeps) bootPlan() []storageIntegrityBootStep {
	return []storageIntegrityBootStep{
		{Name: "ensure-protocol-tables", Run: d.ensureProtocolTables},
		{Name: "cross-check-schemas", Run: d.crossCheckSchemas},
		{Name: "register-role", Run: d.registerRole},
	}
}
```

Change `runStorageIntegrityProtocolBootstrap` to take `[]storageIntegrityBootStep`, execute in order, and wrap any error as `fmt.Errorf("storage-integrity bootstrap step %q: %w", step.Name, err)`. Update the call site at `:316` to `runStorageIntegrityProtocolBootstrap(ctx, deps.bootPlan())`. Behaviour is unchanged; the binding becomes inspectable.

- [x] **Step 4: Run the tests**

Run: `bazel test //standalone:standalone_test --test_filter='TestStorageIntegrityBootPlan|TestRunStorageIntegrityProtocolBootstrapStops|TestStorageIntegrityBootstrapPrecedes' --test_output=all`
Expected: PASS.

- [x] **Step 5: Prove the binding test is load-bearing**

Temporarily swap the `ensureProtocolTables` and `crossCheckSchemas` bindings inside `bootPlan` and re-run. Expected: FAIL at `require.Contains(funcIdentity(plan[0].Run), "ensureProtocolTables")` — the name is now bound to the wrong method. Restore and re-run: PASS. This is the exact scenario the previous positional design could not detect.

- [x] **Step 6: Full package + commit**

Run: `bazel test //standalone:standalone_test --test_output=errors`
Expected: PASS.

```bash
git add standalone/standalone.go standalone/storage_integrity_bootstrap_test.go
git commit -m "test(storage-integrity): assert the production boot bindings, not a test-local order"
```

## Task 15: documentation and the spec §5 acceptance sweep

**Files:**
- Modify: `/Users/uranuswch/Dev/housegate/housegate/docs/superpowers/specs/2026-08-19-storage-integrity-verification-restoration-design.md` (status line)
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-go/AGENTS.md`, `/Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/AGENTS.md`
- Modify: `/Users/uranuswch/Dev/housegate/rewriter-grpc/CLAUDE.md`

**Interfaces:**
- Consumes: every preceding task.
- Produces: nothing code depends on.

- [x] **Step 1: Document the corpus contract where the corpus lives**

In `rewriter-go/internal/harness/AGENTS.md`, add:

````markdown
## STORAGE-INTEGRITY CORPUS CONTRACT

`testdata/storage_integrity_cases.json` is the frozen Go/C++ behaviour
contract, byte-identical to `rewriter-grpc/tests/testdata/`. Schema and rules
live in `sicorpus_test.go` (`SICase`, `ValidateSICorpus`); `TestSICorpusContract`
enforces them and `TestSICorpusIsBytePinned` enforces the cross-repo identity.

- Every case is either a reject (`want_code != "Success"` plus a
  `want_message_contains`, pinning no SQL) or a success that pins SQL exactly.
- Success cases pin one `want_sql` when both engines agree, or set
  `allow_sql_divergence: true` with **both** `want_sql_go` and `want_sql_cpp`.
  There is no `sql_exact`; comparison is always exact after
  `NormalizeSIIdentifierQuotes`.
- `want_sql_contains` is an extra assertion only. An entry already present in
  the case's input SQL is a hard violation — it passes for a no-op rewriter.
- Unknown JSON keys fail the load: the schema is frozen.

Regenerate the pins (needs both engines) with:

```bash
make ffi
UPDATE_GOLDEN=1 REWRITER_ORACLE_ADDR=<host:port> \
  go test ./internal/harness -run TestStorageIntegrityGolden -count=1
```

Then copy the file verbatim into rewriter-grpc and update the pinned
fingerprint/size constants in **both** `sicorpus_test.go` and
`rewriter-grpc/tests/si_corpus.h`.

## GOLDEN FIXTURES

No test writes a tracked file. `internal/engine/characterize_test.go` compares
and regenerates only under `UPDATE_GOLDEN=1`; CI asserts
`git diff --exit-code` after `make test`.
````

Add a one-line pointer in the repo-root `rewriter-go/AGENTS.md` to that section.

- [x] **Step 2: Document the CI job and the corpus contract in rewriter-grpc**

In `rewriter-grpc/CLAUDE.md`, under `### Tests`, append:

```markdown
`.github/workflows/ci.yml` runs `pull_request` and `push: main` through the build box: `./scripts.sh rebuild` (never `./scripts.sh test`, which wipes `build/`), then `ctest`, then the storage-integrity corpus suite by name with a case-count guard. It shares `concurrency: group: build-box` with `release.yml` and uses its own workdir `~/ci/rewriter-ci`, and it skips fork PRs because the SSH secrets are unavailable to them.

The shared corpus is a real parity gate. `tests/si_corpus.h` holds the frozen schema, the validator (rule ids R1-R7, mirrored from rewriter-go `internal/harness/sicorpus_test.go`) and the pinned FNV-1a fingerprint that keeps the two copies byte-identical. `tests/si_normalize.h` holds the literal-aware identifier-quote normalization — it replaced a global backtick→double-quote `std::replace` that also rewrote string literals. `sql_exact` no longer exists: every non-reject case is compared exactly after normalization, and `allow_sql_divergence` selects `want_sql_cpp`.
```

- [x] **Step 3: Flip the spec status**

In `docs/superpowers/specs/2026-08-19-storage-integrity-verification-restoration-design.md`, change `**Status:** Proposed` to `**Status:** Implemented` and append a short "Deviations" subsection at the end recording DEV-1, DEV-2 and DEV-3 from this plan, each with one sentence of rationale.

- [x] **Step 4: Run the full spec §5 acceptance checklist and record each result**

Work through the spec's six acceptance bullets in order and paste the observed output for each into the closing PR description:

1. **housegate red-on-break / green-on-revert** — Task 1 Steps 5-7.
2. **rewriter-grpc red-on-break / green-on-revert** — Task 12 Steps 4-5.
3. **Corpus meta-test in both repos** — Task 10 Step 5, plus Task 10 Step 6's vacuity proof.
4. **Pre-fix corpus report shows 7 and 12** — Task 5 Step 3's `TestSICorpusLegacyCoverageReport` log lines.
5. **sentio-node Phase 3 executes and fails on tamper** — Task 14 Steps 3-5.
6. **`git diff --exit-code` clean after `make test` in rewriter-go** — Task 11 Step 6.
7. **`git status --porcelain` shows no untracked `AGENTS.md` in housegate** — Task 3 Step 9.

- [x] **Step 5: Re-verify the cross-repo corpus identity one final time**

```bash
shasum -a 256 \
  /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json \
  /Users/uranuswch/Dev/housegate/rewriter-grpc/tests/testdata/storage_integrity_cases.json
```

Expected: identical digests. Record the value.

- [x] **Step 6: File the follow-ups this spec deliberately did not do**

Open one tracking issue per item, referencing this plan:

- **sentio-node full `SENTIO_SI_E2E` smoke in CI** (DEV-2). Needs a devnet fixture: an Ethereum node with the `IDatabases` / `IndexerRegistry` contracts deployed, a running arbiter reachable over gRPC, Redis, sentio-core services, and provisioned signer keys plus two pre-minted query JWSs. Assign to the operator who owns the devnet.
- **housegate CI does not exercise the native rewriter engine.** `//pkg/rewriter:rewriter_test`'s `TestNativeEngineSmoke` skips because only the `integration` job fetches the FFI lib. Moving the `Fetch rewriter FFI lib` step into the `build` job and passing `--test_env=POLYGLOT_SQL_FFI_PATH` would close that, at the cost of coupling the unit job to a GitHub release download. Out of scope for Spec J, which is about executing the tests that already exist.
- **`~/.git/.gitignore` line 20's bare `AGENTS.md` rule** — a user-environment change, already recorded on the roadmap (§5 bounded tasks). The repo-side fix and the CI check shipped here; tightening the global rule remains the user's call.

- [x] **Step 7: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git add AGENTS.md internal/harness/AGENTS.md
git commit -m "docs: record the SI corpus contract and the UPDATE_GOLDEN flow"

cd /Users/uranuswch/Dev/housegate/rewriter-grpc
git add CLAUDE.md
git commit -m "docs: record the CI job and the corpus parity gate"

cd /Users/uranuswch/Dev/housegate/housegate
git add docs/superpowers/specs/2026-08-19-storage-integrity-verification-restoration-design.md
git commit -m "docs: close Spec J with its recorded deviations"
```

---

## Self-Review

**1. Spec coverage.** Every section of the spec maps to at least one task; see the coverage map below. No requirement is unassigned.

**2. Placeholder scan.** Searched for `TBD`, `TODO`, `implement later`, `add appropriate error handling`, `write tests for the above`, `similar to Task N`. The only intentional fill-in-the-blank values are Task 10's two pinned constants (`SICorpusFingerprint` / `SICorpusBytes` and `kCorpusFingerprint` / `kCorpusBytes`), which cannot be known before the corpus is regenerated — Task 10 Step 1 is the exact command that computes them, and both call sites carry a `<-- replace with the Step 1 value` marker. Task 6 Steps 8-9 likewise instruct the executor to read specific values out of the freshly regenerated file rather than inventing SQL; the cases and the keys to change are enumerated by name.

**3. Type consistency.**
- `NormalizeSIIdentifierQuotes` — same name in Go (Task 4, package `harness`) and C++ (Task 8, namespace `si_corpus`), same state machine, referenced by Tasks 5, 6, 7, 9, 10.
- `SICase` (Go, Task 5) ↔ `si_corpus::Case` (C++, Task 9): identical JSON key set; the C++ side adds only `ExpectedSQL()`.
- `ValidateSICorpus` (Go) ↔ `si_corpus::ValidateCorpus` (C++): identical rule ids R1-R7 and identical violation text.
- `UpdateGoldenEnv = "UPDATE_GOLDEN"` (Task 5, `harness`) and the duplicated `updateGoldenEnv` (Task 11, `engine`) — the duplication is deliberate and documented, because `harness` imports `engine`.
- `SICorpusFingerprint`/`SICorpusBytes`/`SICorpusCases` (Go) ↔ `kCorpusFingerprint`/`kCorpusBytes`/`kCorpusCases` (C++): same three values, same FNV-1a/64 definition.
- `runStorageIntegrityProtocolBootstrap(ctx, ensure, crossCheck, register) error`, `smokeEnv`, `quoteSmokeIdentifier`, `ddl.UnsafeSettings()` — all consumed in Task 13 exactly as they are declared in the real sources cited in that task's Interfaces block.

**4. Adjustments made during review.** Task 5's schema/validator lives in `sicorpus_test.go` rather than a non-test `sicorpus.go`, because it embeds `accessedJSON` and `remoteUpstreamJSON`, which are declared in `select_golden_test.go` and `dblevel_golden_test.go`. Task 6's writer must use `json.Encoder` with `SetEscapeHTML(false)`; `MarshalIndent` would rewrite every `>` in the corpus SQL as `>`. Task 11's CI step uses `--ignore-submodules=all` so a `cargo build` inside `third_party/polyglot-src` cannot trip it.

## Spec Coverage Map

| Spec section | Requirement | Task(s) |
|---|---|---|
| §1a | HouseGate CI runs none of its unit tests | Task 1 |
| §1b | rewriter-grpc has no CI | Task 12 |
| §1c | the shared corpus is not a parity gate | Tasks 5, 6, 7, 9, 10 |
| §1d | Spec C's only end-to-end proof has never run | Tasks 13, 14 |
| §1e | `pkg/replay/AGENTS.md` is wrong and git cannot see it | Tasks 2, 3 |
| §1f | `make test` rewrites tracked fixtures | Task 11 |
| §2 goal 1 | unit tests execute in CI in every repo that has them | Tasks 1, 12, 14 |
| §2 goal 2 | every `want_sql` compared in both engines; no vacuous pass | Tasks 5, 6, 7, 9, 10 |
| §2 goal 3 | Spec C's drift acceptance actually executes | Tasks 13, 14 |
| §2 goal 4 | agent-guidance files correct and version-controlled | Tasks 2, 3 |
| §2 goal 5 | no test mutates tracked fixtures | Task 11 |
| §3 D1 | `bazel test --config=ci //...`, failure blocks merge, fix-or-quarantine with a tracking issue | Task 1 (Steps 2, 4) |
| §3 D2 | `pull_request` + `push: main` job running the test binary, SI corpus test specifically | Task 12 |
| §3 D3 | corpus assertion contract, schema check in both runners, `sql_exact` deleted, per-engine pins, vacuity check | Tasks 5 (validator), 6 (migration), 7 (Go runner), 9 (C++ runner), 10 (meta-tests) |
| §3 D4 | shared, literal-aware normalization; a literal-internal quoting change is not normalized away | Tasks 4 (Go), 8 (C++) |
| §3 D5 | ClickHouse service + the Phase-3 drift assertion executing | Tasks 13, 14 (see DEV-2) |
| §3 D6 | corrected `pkg/replay/AGENTS.md`, force-add the eight files, CI check for untracked `AGENTS.md` | Tasks 2, 3 |
| §3 D7 | capture test compares by default, `-update`-equivalent flag, fixtures regenerated in the same PR, `git diff --exit-code` CI step | Task 11 |
| §4 | D3/D4 land before or with Spec I's new cases; D1 lands early | Task ordering: 1 first, then 4-11 before Spec I |
| §5 acceptance 1 | housegate red on a deliberately broken `pkg/storageintegrity` assertion, green on revert | Task 1 Steps 5-7 |
| §5 acceptance 2 | rewriter-grpc red on a deliberately broken corpus expectation | Task 12 Steps 4-5 |
| §5 acceptance 3 | corpus meta-test in both repos over every case | Task 10 Steps 5-6 |
| §5 acceptance 4 | pre-fix corpus run reports the 7 vacuous and 12 unasserted `want_sql`s | Task 5 Step 3 (`TestSICorpusLegacyCoverageReport`) |
| §5 acceptance 5 | sentio-node's Phase 3 executes and fails on tamper | Tasks 13 Step 5, 14 Steps 3-5 |
| §5 acceptance 6 | `git diff --exit-code` clean after `make test` in rewriter-go | Task 11 Steps 6-8 |
| §5 acceptance 7 | `git status --porcelain` shows no untracked `AGENTS.md` in housegate | Task 3 Step 9 |
| §6 delivery 1 | housegate: CI, fallout, D6 files + check | Tasks 1, 2, 3 |
| §6 delivery 2 | rewriter-go + rewriter-grpc: schema, validator, both runners, normalization, capture test, meta-test | Tasks 4-11 |
| §6 delivery 3 | rewriter-grpc: the new CI job | Task 12 |
| §6 delivery 4 | sentio-node: ClickHouse service + the drift acceptance | Tasks 13, 14 |
| §2 non-goals | not raising coverage %, not moving rewriter-grpc off its box, not changing global git config | Honoured; the global-ignore item is re-filed in Task 15 Step 6 |
