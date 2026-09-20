# Signed INSERT ... SELECT Remediation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repair what the 2026-09-20 intermediate review found in the merged issue #153 work (housegate #156–#171, arbiter #50–#83, arbiter-core #20–#29, arbiter-proto #5/#8/#9, rewriter-proto #4, rewriter-go #36, rewriter #56): unreproducible dependency pins, Bazel/vet hygiene, stale guidance docs, and eight places where merged code contradicts the design or the component plans. Nothing here enables the lane; every runtime gate stays off.

**Architecture:** Each task is one PR in one repository, small enough that a reviewer can reject it alone. Phase 0 makes every repository buildable from a clean machine and honest about what Bazel compiles. Phase 1 fixes merged behaviour that deviates from the design (read-set provenance, forward/submit authorization separation, query-only session reuse, reservation drain, lookup semantics, transition receipts, verifier ordering, canonical command bytes). Phase 2 aligns the cross-repository contract points and the design text. Existing frozen vectors are never regenerated; the one pre-release canonical change (Task 15) is explicitly decision-gated.

**Tech Stack:** Go (Bazel 9.1.0 + Bzlmod, gazelle), ClickHouse C++ AST (rewriter), protobuf, ed25519/secp256k1 JWS, ClickHouse native TCP relay.

**Spec:** [2026-09-16 design](../specs/2026-09-16-signed-insert-select-design.md) (D1, D5, D8, D9, D10, §4), the [master plan](2026-09-16-signed-insert-select.md) (Global Constraints, "Review and verification rules"), the component plans [A](2026-09-16-signed-insert-select-contracts.md), [B](2026-09-16-signed-insert-select-replay.md), [C](2026-09-16-signed-insert-select-coordination.md), [D](2026-09-16-signed-insert-select-integration.md), and the review findings summarized in each task's "Why" line. The review itself was delivered in chat on 2026-09-20; its factual basis is the repositories at the SHAs below.

## Global Constraints

- Repository aliases and the exact `main` commits this plan was written against: HG `housegate/housegate` 18340e79a2ef (`/Users/uranuswch/Dev/housegate/housegate`), AR `sentioxyz/arbiter` ce541edad8a3 (`/Users/uranuswch/Dev/sentio_xyz/arbiter`), AC `sentioxyz/arbiter-core` 17f5a1ed7761 (`/Users/uranuswch/Dev/sentio_xyz/arbiter-core`; its local checkout sits on an old branch, always branch from `origin/main`), AP `sentioxyz/arbiter-proto` 1ac937bd4aaa, RP `housegate/rewriter-proto` d3844a56d7b4 (`/Users/uranuswch/Dev/housegate/rewriter-proto`), RG `housegate/rewriter-go` a25c526eefc8 (`/Users/uranuswch/Dev/housegate/rewriter-go`), RC `housegate/rewriter` 517ebd963a90 (local dir `/Users/uranuswch/Dev/housegate/rewriter-grpc`). Re-fetch `origin/main` before starting a task and record the actual base in the PR.
- One isolated worktree per task and repository (`git worktree add` or the URWT skill); never edit the user's main checkouts in place. One PR per task; conventional commit subject; body ends with `Refs https://github.com/housegate/housegate/issues/153` and links the design PR https://github.com/housegate/housegate/pull/154.
- Preserve every existing v2 byte: `housegate-statement-v2`, `clickhouse-native-data-v1`, INSERT kind `1`, `safe-snapshot-data-v2`, `replay-statement-root`, `replay-execution-receipt`, `housegate-row-id-v1`, `pkg/auth/testdata/statement_jws_v2.json`, `auth.SharedStatementVectorsSHA256`. Never regenerate a historical vector; Task 15 is the only canonical-byte change and it is gated.
- Keep the frozen new domains exactly as spelled in the master plan: `snapshot-query-read-set-v1`, `snapshot-query-input-v1`, `snapshot-query-output-v1`, `snapshot-query-statement-root-v1`, `snapshot-query-receipt-v1`, `executor-profile-transition-v1`, `snapshot-query-profile-v1`, `snapshot-query-abort-v1`, `snapshot-query-artifact-ready-v1`, `snapshot-query-claim-v1`.
- Every runtime gate stays default-off. No task wires `sisnapshotquery`, the query-only host plan, the intake, or any Arbiter RPC into `build.go`, config, or `server.go` dispatch.
- HG must not import arbiter-core or sentio-node. AR/AC may import HG (`pkg/replay`, `pkg/auth`); RG/RC never import HG.
- Bazel is the test authority for HG, AR and AC: run `bazel test //...` in the task's worktree before opening the PR (local macOS uses plain `bazel test //...`, never `--config=ci`). RG uses `go test ./...` with the env the repo documents; RC builds only on the remote box per its `CLAUDE.md` ("Build / test / run").
- After changing Go deps in HG run `bazel mod tidy && bazel run //:gazelle`; in AR/AC keep `go.mod` and `MODULE.bazel` `git_override` commits identical.
- Documentation is English, one paragraph per line, no hard wrapping.
- SQL parsing stays in the rewriter engines; no task adds a regex SQL parser to the agent or ingress.

---

## File map

| Task | Repo | Files |
|---|---|---|
| 1 | RG | Modify `go.mod`, `go.sum` |
| 2 | RC | Modify `third_party/rewriter-proto` (gitlink), `tests/testdata/snapshot_query_ci_pins.json` |
| 3 | AR | Modify `server/BUILD.bazel`, `server/server_test.go` |
| 4 | AC | Modify `go.mod`, `go.sum`, `MODULE.bazel` |
| 5 | AR | Modify `go.mod`, `go.sum`, `MODULE.bazel` |
| 6 | HG | Modify `pkg/plugins/sisnapshotquery/BUILD.bazel`, `pkg/auth/BUILD.bazel`, `pkg/replay/snapshotquery/BUILD.bazel`, `pkg/rewriter/snapshot_query.go`, `pkg/rewriter/snapshot_query_test.go` |
| 7 | HG | Modify `CLAUDE.md`, `pkg/plugins/AGENTS.md`, `pkg/replay/AGENTS.md`, `pkg/proxy/AGENTS.md`, `pkg/rewriter/AGENTS.md` |
| 8 | HG | Modify `pkg/plugins/sisnapshotquery/plugin.go`, `pkg/plugins/sisnapshotquery/plugin_test.go` |
| 9 | HG | Modify `pkg/storageintegrity/snapshot_query_journal.go`, `pkg/storageintegrity/snapshot_query_intake_phases.go`, `pkg/storageintegrity/snapshot_query_intake.go`, `pkg/storageintegrity/snapshot_query_intake_test.go`, `pkg/plugins/sisnapshotquery/plugin.go`, `pkg/plugins/sisnapshotquery/plugin_test.go` |
| 10 | HG | Modify `pkg/proxy/relay.go`, `pkg/proxy/relay_query_only.go`, `pkg/proxy/relay_query_only_test.go` |
| 11 | AR | Modify `fsm/state.go`, `fsm/artifact_reservation_drain.go`, `fsm/artifact_reservation_grant.go`, `fsm/artifact_reservation_drain_test.go`, `fsm/artifact_reservation_grant_test.go` |
| 12 | AR | Modify `server/snapshot_query_reservation_projection.go`, `server/snapshot_query_reservation_projection_test.go` |
| 13 | AR | Modify `fsm/apply_artifact_disposition.go`, `fsm/apply_artifact_disposition_test.go` |
| 14 | AC | Modify `verifier/verifier.go`, `verifier/verifier_test.go`, `verifier/BUILD.bazel`, `conformance/BUILD.bazel` |
| 15 | AC | Modify `wire/artifact_disposition.go`, `wire/artifact_disposition_test.go`, `docs/compatibility/snapshot-query-raft-allocation.md` |
| 16 | AR | Modify `server/snapshot_query_control.go`, `server/snapshot_query_reservation_projection.go`, `server/server_test.go`, `server/snapshot_query_reservation_projection_test.go`, `server/BUILD.bazel`; Create `server/snapshot_query_control_crossrepo_test.go` |
| 17 | RG + RC | Modify RG `internal/harness/testdata/snapshot_query_cases.json`, `internal/harness/snapshot_query_test.go`; RC `tests/testdata/snapshot_query_cases.json`, `tests/snapshot_query_test.cc`, `tests/testdata/snapshot_query_ci_pins.json`, `.github/scripts/snapshot-query-ci.py`, `src/handlers/snapshot_query.cc` |
| 18 | HG | Modify `docs/superpowers/specs/2026-09-16-signed-insert-select-design.md` |

Dependency order: Tasks 1–3 are independent. Task 5 should land after Task 4 so AR can pin AC's post-bump commit. Task 16 requires Task 5 (AR's HG pin must contain `pkg/auth/snapshot_query_control.go`, first shipped in HG #163). Task 18 depends on the decisions recorded for Tasks 10 and 15. Everything else is independent. Suggested model per task: Sonnet for 1–7, 12, 16, 17, 18; Opus for 8–11, 13–15.

---

## Phase 0 — Reproducible pins and build hygiene

### Task 1: Pin rewriter-proto to a commit on its main (RG)

**Why:** RG `go.mod:6` pins `github.com/housegate/rewriter-proto v0.2.1-0.20260916180039-a86e62230aaf`; `a86e622` is the head of RP PR #4 and is not an ancestor of RP `main`. `go mod download` fails for it under `GOPROXY=direct`, which is the effective mode on any machine with `GOPRIVATE=github.com/housegate`. RP main `d3844a56d7b4` has the identical tree (`f9672e7`), so the fix changes no generated code.

**Files:**
- Modify: `go.mod:6`, `go.sum`

**Interfaces:** none.

- [ ] **Step 1: Prove the current pin is unreachable and the target tree is identical**

Run (from the RG worktree):
```bash
git -C /Users/uranuswch/Dev/housegate/rewriter-proto fetch origin main --quiet
git -C /Users/uranuswch/Dev/housegate/rewriter-proto merge-base --is-ancestor a86e62230aaf origin/main; echo "ancestor exit=$?"
git -C /Users/uranuswch/Dev/housegate/rewriter-proto rev-parse 'a86e62230aaf^{tree}' 'd3844a56d7b4^{tree}'
```
Expected: `ancestor exit=1`; the two tree ids are equal.

- [ ] **Step 2: Re-pin**

```bash
go get github.com/housegate/rewriter-proto@d3844a56d7b436ceaba46f949f7e6721ffdeef23
go mod tidy
grep -n rewriter-proto go.mod
```
Expected `go.mod` line: `github.com/housegate/rewriter-proto v0.2.1-0.20260918115349-d3844a56d7b4`.

- [ ] **Step 3: Verify a clean fetch and run the tests**

```bash
GOMODCACHE=$(mktemp -d) GOFLAGS=-mod=mod GOPROXY=direct go mod download github.com/housegate/rewriter-proto
SNAPSHOT_QUERY_ORDINARY=1 go test ./...
```
Expected: download succeeds; tests pass with the same pass/skip set as before the change.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "chore(deps): pin rewriter-proto to a main commit"
```

### Task 2: Pin the rewriter-proto submodule to a commit on its main (RC)

**Why:** RC's `third_party/rewriter-proto` gitlink is `a86e62230aaf09fcf43381950cbf5c2732b38c12` (RP PR-head, not on RP main) and `tests/testdata/snapshot_query_ci_pins.json:7` records the same SHA as `"rp"`. Same identical-tree argument as Task 1.

**Files:**
- Modify: `third_party/rewriter-proto` (gitlink), `tests/testdata/snapshot_query_ci_pins.json:7`

**Interfaces:** none.

- [ ] **Step 1: Move the submodule**

```bash
git submodule update --init third_party/rewriter-proto
git -C third_party/rewriter-proto fetch origin main --quiet
git -C third_party/rewriter-proto checkout d3844a56d7b436ceaba46f949f7e6721ffdeef23
git -C third_party/rewriter-proto rev-parse 'HEAD^{tree}' 'a86e62230aaf^{tree}'
git add third_party/rewriter-proto
```
Expected: both tree ids equal; `git status` shows the gitlink modified.

- [ ] **Step 2: Update the CI pin record**

In `tests/testdata/snapshot_query_ci_pins.json` change the line `"rp": "a86e62230aaf09fcf43381950cbf5c2732b38c12",` to `"rp": "d3844a56d7b436ceaba46f949f7e6721ffdeef23",`. Change nothing else in that file (the corpus, file and image digests are unaffected because the RP tree is identical).

- [ ] **Step 3: Build and run the snapshot-query tests on the remote box**

Follow `CLAUDE.md` "Build / test / run": rsync the worktree to the dev workdir, then:
```bash
ssh <build-box> "cd /home/sentio/chen/rewriter-grpc && ./scripts.sh rebuild && ./build/rewriter_tests --gtest_filter='SnapshotQuery*'"
```
Expected: all `SnapshotQuery*` tests pass; the contract test still reports the corpus digest `3d7c1f1077b1ac6c1bd73d7703b135e95c92bde10d1ce4393c8cb091cca1c933`.

- [ ] **Step 4: Commit**

```bash
git add third_party/rewriter-proto tests/testdata/snapshot_query_ci_pins.json
git commit -m "chore(deps): pin the rewriter-proto submodule to a main commit"
```

### Task 3: Compile the reservation projection under Bazel and fix the vet copylocks (AR)

**Why:** PR #78 added `server/snapshot_query_reservation_projection.go` (183 lines) and its test (192 lines) but neither is listed in `server/BUILD.bazel`, so CI never compiled or ran them; the PR's "bazel test //server:server_test" passed vacuously. `server/server_test.go:421` copies a `pb.GetArtifactDispositionRequest` by value (`wrongNetwork := *request`), which `go vet` rejects (copylocks: the message embeds `protoimpl.MessageState`).

**Files:**
- Modify: `server/BUILD.bazel` (`go_library` `srcs` at lines 5–20 and the `go_test` `srcs`), `server/server_test.go:421-423`

**Interfaces:** none.

- [ ] **Step 1: Observe both failures**

```bash
bazel query 'kind(go_library, //server:all)' --output=build | grep -c snapshot_query_reservation_projection   # expect 0
GOFLAGS=-mod=mod go vet ./server/                                                                            # expect copylocks at server_test.go:421
```

- [ ] **Step 2: List the orphaned files**

In `server/BUILD.bazel` add `"snapshot_query_reservation_projection.go",` to the `go_library` `srcs` list (alphabetical, after `"snapshot_query_grant_coordinator.go",`) and `"snapshot_query_reservation_projection_test.go",` to the `go_test` `srcs` list in the same position.

- [ ] **Step 3: Stop copying the proto message**

Replace lines 421–423 of `server/server_test.go`:
```go
	wrongNetwork := proto.Clone(request).(*pb.GetArtifactDispositionRequest)
	wrongNetwork.NetworkId = "other-network"
	if _, err := client.GetArtifactDisposition(context.Background(), wrongNetwork); status.Code(err) != codes.FailedPrecondition {
```
Add the import `"google.golang.org/protobuf/proto"` to the test file. If `server/BUILD.bazel`'s `go_test` `deps` lacks `"@org_golang_google_protobuf//proto"`, add it.

- [ ] **Step 4: Verify**

```bash
GOFLAGS=-mod=mod go vet ./server/ ./fsm/
bazel test //server:server_test //fsm:fsm_test
bazel query 'kind(go_library, //server:all)' --output=build | grep -c snapshot_query_reservation_projection   # expect 1
```
Expected: vet clean, both targets PASS, the projection tests now execute (check `bazel-testlogs/server/server_test/test.log` for `TestSnapshotQueryReservationProjection`).

- [ ] **Step 5: Commit**

```bash
git add server/BUILD.bazel server/server_test.go
git commit -m "fix(server): compile the reservation projection under Bazel and stop copying a proto message"
```

### Task 4: Consume housegate main (AC)

**Why:** AC `go.mod:45` and `MODULE.bazel:21` pin HG `41418b326629` (#160). That commit's `go.mod` still carries the unreachable rewriter pins (`rewriter-go ...896e4344a91b`, `rewriter-proto ...a86e62230aaf`), so AC's indirect pins fail `GOPROXY=direct` fetches; it also predates HG #163 (`pkg/auth/snapshot_query_control.go`) and #170 (`pkg/replay/snapshotquery` policy source) that AC #29 re-implemented locally.

**Files:**
- Modify: `go.mod` (housegate line and the two indirect rewriter lines), `go.sum`, `MODULE.bazel:19-21`

**Interfaces:** none.

- [ ] **Step 1: Observe the current clean-fetch failure**

```bash
GOMODCACHE=$(mktemp -d) GOFLAGS=-mod=mod GOPROXY=direct go mod download github.com/housegate/rewriter-go@v0.11.1-0.20260917041414-896e4344a91b; echo "exit=$?"
```
Expected: `unknown revision 896e4344a91b`, non-zero exit.

- [ ] **Step 2: Bump the Go pin**

```bash
go get github.com/housegate/housegate@18340e79a2efd86730438ec56d3b91ee567e8e89
go mod tidy
grep -n -E 'housegate/housegate|rewriter-go|rewriter-proto' go.mod
```
Expected: `github.com/housegate/housegate v0.13.2-0.20260919141014-18340e79a2ef`; the indirect rewriter lines now end in `a25c526eefc8` and `d3844a56d7b4`.

- [ ] **Step 3: Bump the Bazel pin to the same commit**

In `MODULE.bazel` replace the housegate override block's commit and comment:
```python
    module_name = "housegate",
    # Resolved Housegate v0.13.2-0.20260919141014-18340e79a2ef; source is pinned by the commit below.
    commit = "18340e79a2efd86730438ec56d3b91ee567e8e89",
```
Then run `bazel mod tidy` and check `git diff MODULE.bazel` contains only that block.

- [ ] **Step 4: Verify clean fetch and the full suite**

```bash
GOMODCACHE=$(mktemp -d) GOFLAGS=-mod=mod GOPROXY=direct go mod download all; echo "exit=$?"
bazel build //... && bazel test //...
```
Expected: exit 0 and 12/12 targets PASS. If a compile error appears against `@housegate//pkg/replay/snapshotquery` or `@housegate//pkg/auth`, stop and report the exact symbol; do not patch HG from this task.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum MODULE.bazel
git commit -m "chore(deps): consume housegate main 18340e7 with reachable rewriter pins"
```

### Task 5: Consume housegate main and arbiter-core main (AR)

**Why:** AR pins HG `41418b3` (#160, unreachable indirect rewriter pins as in Task 4) and AC `4a3436c` (#27, before AC #28/#29). Task 16 needs HG's `pkg/auth/snapshot_query_control.go` (#163) from AR tests.

**Files:**
- Modify: `go.mod` (housegate, arbiter-core, indirect rewriter lines), `go.sum`, `MODULE.bazel:20-22` (arbiter_core commit) and `:27-29` (housegate commit)

**Interfaces:** none.

- [ ] **Step 1: Bump both Go pins**

Use Task 4's merge commit for AC if it has merged; otherwise `17f5a1ed7761686f5f3992d1a311ad534ac9640e`.
```bash
go get github.com/housegate/housegate@18340e79a2efd86730438ec56d3b91ee567e8e89
go get github.com/sentioxyz/arbiter-core@<AC commit>
go mod tidy
grep -n -E 'housegate/housegate|arbiter-core|rewriter-go|rewriter-proto' go.mod
```
Expected: housegate `...-18340e79a2ef`, arbiter-core at the chosen commit, indirect rewriter lines ending `a25c526eefc8` / `d3844a56d7b4`.

- [ ] **Step 2: Bump both `git_override` commits in `MODULE.bazel`**

```python
    module_name = "arbiter_core",
    # Resolved arbiter-core <pseudo-version from go.mod>; source is pinned by the commit below.
    commit = "<AC commit, full 40 hex>",
```
```python
    module_name = "housegate",
    # Resolved Housegate v0.13.2-0.20260919141014-18340e79a2ef; source is pinned by the commit below.
    commit = "18340e79a2efd86730438ec56d3b91ee567e8e89",
```
Run `bazel mod tidy`.

- [ ] **Step 3: Verify**

```bash
GOMODCACHE=$(mktemp -d) GOFLAGS=-mod=mod GOPROXY=direct go mod download all; echo "exit=$?"
bazel build //... && bazel test //...
```
Expected: exit 0, 19/19 PASS (20 after Task 3 lands, since `//server:server_test` gains files but not targets). On a compile error in AR code against the new HG/AC APIs, stop and report the symbol.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum MODULE.bazel
git commit -m "chore(deps): consume housegate 18340e7 and arbiter-core main"
```

### Task 6: Re-sync gazelle and wrap probe errors with %w (HG)

**Why:** `pkg/plugins/sisnapshotquery/BUILD.bazel`'s `go_test` lacks `@com_github_clickhouse_ch_go//proto` although `plugin_test.go:10` imports it (it compiles only through `embed`); `pkg/auth/BUILD.bazel` and `pkg/replay/snapshotquery/BUILD.bazel` have non-gazelle ordering. `pkg/rewriter/snapshot_query.go:436` and `:448` wrap errors with `%v`, `:444` uses `fmt.Errorf` for a constant string; CLAUDE.md requires `%w`.

**Files:**
- Modify: `pkg/plugins/sisnapshotquery/BUILD.bazel`, `pkg/auth/BUILD.bazel`, `pkg/replay/snapshotquery/BUILD.bazel`, `pkg/rewriter/snapshot_query.go:436,444,448`
- Test: `pkg/rewriter/snapshot_query_test.go`

**Interfaces:** none.

- [ ] **Step 1: Write the failing test**

Append to `pkg/rewriter/snapshot_query_test.go` (reuse the file's existing fake `SnapshotQueryAnalyzer` if one exists; otherwise add this one):
```go
type probeFailingAnalyzer struct{ err error }

func (a *probeFailingAnalyzer) AnalyzeSnapshotQuery(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	return nil, a.err
}
func (a *probeFailingAnalyzer) ClassifySnapshotQuery(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	return nil, a.err
}
func (a *probeFailingAnalyzer) PrepareSnapshotQuery(context.Context, *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
	return nil, a.err
}
func (a *probeFailingAnalyzer) Close() error { return nil }

func TestExpectSnapshotProbeRejectionWrapsCause(t *testing.T) {
	sentinel := errors.New("transport down")
	err := expectSnapshotProbeRejection(context.Background(), &probeFailingAnalyzer{err: sentinel}, &pb.AnalyzeSnapshotQueryRequest{}, pb.SnapshotQueryCode_INVALID_INPUT)
	if !errors.Is(err, sentinel) {
		t.Fatalf("probe error does not wrap its cause: %v", err)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bazel test //pkg/rewriter:rewriter_test --test_filter=TestExpectSnapshotProbeRejectionWrapsCause`
Expected: FAIL with "probe error does not wrap its cause".

- [ ] **Step 3: Fix the three lines**

`pkg/rewriter/snapshot_query.go:436`: `return fmt.Errorf("snapshot query capability probe: final ordinary analysis got %w, want typed %s refusal", err, pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY)`
`:444`: `return errors.New("unexpected success")`
`:448`: `return fmt.Errorf("got %w, want code %s", err, code)`

- [ ] **Step 4: Re-run gazelle and confirm the expected BUILD diff**

```bash
bazel mod tidy && bazel run //:gazelle
git diff --stat -- '*BUILD.bazel'
grep -n 'com_github_clickhouse_ch_go//proto' pkg/plugins/sisnapshotquery/BUILD.bazel
```
Expected: only the three BUILD files change; the grep matches inside the `go_test` `deps`.

- [ ] **Step 5: Verify**

Run: `bazel test //pkg/rewriter:rewriter_test //pkg/plugins/sisnapshotquery:sisnapshotquery_test //pkg/auth:auth_test //pkg/replay/snapshotquery:snapshotquery_test`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/plugins/sisnapshotquery/BUILD.bazel pkg/auth/BUILD.bazel pkg/replay/snapshotquery/BUILD.bazel pkg/rewriter/snapshot_query.go pkg/rewriter/snapshot_query_test.go
git commit -m "chore(build): resync gazelle and wrap snapshot query probe errors"
```

### Task 7: Document the merged snapshot-query surface (HG)

**Why:** 27k lines landed in HG without touching `CLAUDE.md`'s Key Modules or any `AGENTS.md`; the new packages (`pkg/replay/snapshotquery`, `pkg/plugins/sisnapshotquery`, `pkg/storageintegrity/snapshot_query_*.go`, `pkg/auth/snapshot_query_control.go`, `pkg/proxy/relay_query_only.go`, `pkg/proxy/relay_agent_prepare.go`, `pkg/rewriter/snapshot_query.go`, `pkg/plugin/query_only_host.go`, `tools/snapshot-query-profile`) are invisible to the guidance files that CLAUDE.md says "describe the current state".

**Files:**
- Modify: `CLAUDE.md` (Key Modules), `pkg/plugins/AGENTS.md:23`, `pkg/replay/AGENTS.md`, `pkg/proxy/AGENTS.md` (WHERE TO LOOK table), `pkg/rewriter/AGENTS.md` (table)

**Interfaces:** none.

- [ ] **Step 1: CLAUDE.md**

Insert a new bullet directly after the `- **[pkg/plugins/](pkg/plugins/)**` bullet in "Key Modules":
```markdown
- **Signed INSERT ... SELECT lane (issue #153, default-off, not wired).** `pkg/replay/snapshotquery` owns the v3 snapshot-query contracts, canonical output sorting, the snapshot-aware executor and verifier (design D1–D7); `pkg/auth/snapshot_query_control.go` signs the C2 control JWS (`acquire`/`lookup`/`release`, purpose `housegate-snapshot-query-control-v1`); `pkg/storageintegrity/snapshot_query_{journal,intake,intake_phases,prepared_output}.go` is the sequence-first C4 intake; `pkg/plugins/sisnapshotquery` is the agent-side preparation plugin (injection-only `Options`, no `Config`, deliberately absent from `build.go`); `pkg/proxy/relay_agent_prepare.go` / `relay_query_only.go` are the D1/D2 relay primitives and `pkg/plugin/query_only_host.go` the host plan seam; `pkg/rewriter/snapshot_query.go` is the fail-closed analysis wrapper and capability probe; `tools/snapshot-query-profile` prints measured profile records. Remaining work and gates: [docs/superpowers/plans/2026-09-20-signed-insert-select-remediation.md](docs/superpowers/plans/2026-09-20-signed-insert-select-remediation.md).
```

- [ ] **Step 2: pkg/plugins/AGENTS.md**

After line 23 (`|-- sistatement/ ...`) insert:
```
|-- sisnapshotquery/ # agent-side signed INSERT ... SELECT preparation (default-off, injection-only ports)
```
Add a table row after the "Agent USE state" row:
```
| Snapshot-query agent lane | `sisnapshotquery/` | OnQuery installs an async AgentPrepare plan; classification is local, analysis/reservation/catalog/journal are injected ports; nothing wires it in build.go. |
```

- [ ] **Step 3: pkg/replay/AGENTS.md**

Append one paragraph at the end:
```
`pkg/replay/snapshotquery` is the issue #153 snapshot-query executor/verifier (design D1–D7). It reuses `ResolveColumnProfile`, `lthash.EncodeRow`, `payloadexec.RowID`, `PartitionIDForRow` and `RowElementHash` and adds the frozen v3 records in `snapshot_query_types.go`, the canonical hashes in `snapshot_query_hash.go`, and the executor-profile transition validator. Its `SnapshotReadStore`, `ProfileRegistry` and `QueryUseAdmission` ports have no production implementation yet; arbiter-core supplies them.
```

- [ ] **Step 4: pkg/proxy/AGENTS.md**

Add two rows to the WHERE TO LOOK table after "Upstream to client packets":
```
| Agent async preparation | `relay_agent_prepare.go` | AgentPrepare plan: sole-reader Cancel/EOF polling while a worker prepares, serialized forward gate, durable ForwardAuthorized before the Query is written. |
| Query-only host execution | `relay_query_only.go` | Local completion without an upstream Query: drains the empty external-table marker, never fires OnQuerySuccess, see the plan for session reuse after local completion. |
```

- [ ] **Step 5: pkg/rewriter/AGENTS.md**

Add a row after "Agent materializer":
```
| Snapshot-query analysis | `snapshot_query.go` | Fail-closed AnalyzeSnapshotQuery/PrepareSnapshotQuery wrapper and startup capability probe over the rewriter-proto snapshot contract; measured native/gRPC parity test is env-gated. |
```

- [ ] **Step 6: Verify and commit**

```bash
git diff --check
awk 'FNR==1{f=FILENAME} /^- \*\*Signed INSERT/{print f": "length($0)}' CLAUDE.md   # one line, no hard wrap
bazel test //:housegate_test
git add CLAUDE.md pkg/plugins/AGENTS.md pkg/replay/AGENTS.md pkg/proxy/AGENTS.md pkg/rewriter/AGENTS.md
git commit -m "docs: describe the merged snapshot-query lane in CLAUDE.md and AGENTS.md"
```

---

## Phase 1 — Merged behaviour that contradicts the design

### Task 8: Sign the analyzer's read closure, not the whole catalog (HG)

**Why:** `pkg/plugins/sisnapshotquery/plugin.go:636` signs `ReadSet: catalog.ReadSet`, the read set returned by `CatalogPort.LoadSnapshotQueryCatalog(ctx, reservation)`, which never sees the SQL; `Analysis` (`plugin.go:48-51`) carries no table ids. Design D5: "The agent builds and signs this descriptor… Do not infer the read set from the target or a top-level FROM". The executor already compares the analyzer closure against the signed descriptor (`pkg/replay/snapshotquery/executor.go:294-298`, `checkOutputIdentity`), so the agent side is the only place that ignores the closure.

**Files:**
- Modify: `pkg/plugins/sisnapshotquery/plugin.go:48-51` (Analysis), `:594-640` (prepare)
- Test: `pkg/plugins/sisnapshotquery/plugin_test.go`

**Interfaces:**
- Produces: `Analysis.ReadTableIDs []string` — the analyzer's complete base-table closure, one catalog `table_id` per entry, order irrelevant, no duplicates. Fake analyzers in tests must set it.

- [ ] **Step 1: Write the failing tests**

In `plugin_test.go`, find the existing prepare test that builds a `Catalog{ReadSet: ...}` fixture (grep `ReadSet`). Extend the shared `fakeAnalyzer.value` used by those tests with `ReadTableIDs` listing every fixture table id so their expectations remain valid, then add:
```go
func TestPrepareSignsOnlyTheAnalyzedReadClosure(t *testing.T) {
	catalog := replay.SnapshotReadSet{ReadSnapshot: fixturePin(), Tables: []replay.SnapshotReadTable{
		{TableID: "0x01", Database: "tenant", Table: "a"},
		{TableID: "0x02", Database: "tenant", Table: "b"},
		{TableID: "0x03", Database: "tenant", Table: "c"},
	}}
	analyzer := &fakeAnalyzer{value: Analysis{SQL: "INSERT INTO tenant.a SELECT value FROM tenant.c JOIN tenant.a USING value", TargetTableID: "0x01", SchemaHash: "0xaa", RowIDProfileID: "row-v1", ReadTableIDs: []string{"0x03", "0x01"}}}
	p, state := newPreparedPlugin(t, catalog, analyzer) // helper used by the existing prepare tests
	prepared, err := p.prepare(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	op, _, _ := state.snapshot()
	got := op.Envelope.Input.ReadSet
	if len(got.Tables) != 2 || got.Tables[0].TableID != "0x03" || got.Tables[1].TableID != "0x01" {
		t.Fatalf("signed read set = %+v, want exactly the closure [0x03 0x01]", got.Tables)
	}
	root, err := replay.SnapshotQueryReadSetRoot(got)
	if err != nil || root != op.Envelope.Input.Binding.ReadSetRoot {
		t.Fatalf("read_set_root %s does not commit the signed tables (%v)", op.Envelope.Input.Binding.ReadSetRoot, err)
	}
	_ = prepared
}

func TestPrepareRefusesClosureOutsideCatalog(t *testing.T) {
	catalog := replay.SnapshotReadSet{ReadSnapshot: fixturePin(), Tables: []replay.SnapshotReadTable{{TableID: "0x01"}}}
	analyzer := &fakeAnalyzer{value: Analysis{SQL: "INSERT INTO tenant.a SELECT 1", TargetTableID: "0x01", ReadTableIDs: []string{"0x09"}}}
	p, state := newPreparedPlugin(t, catalog, analyzer)
	_, err := p.prepare(context.Background(), state)
	if err == nil || !strings.Contains(err.Error(), "not in the pinned catalog") {
		t.Fatalf("closure outside catalog = %v, want refusal", err)
	}
	if signer := p.opts.StatementSigner.(*fakeSigner); signer.calls != 0 {
		t.Fatal("refused statement was signed")
	}
}

func TestPrepareConstantSelectSignsEmptyReadSet(t *testing.T) {
	catalog := replay.SnapshotReadSet{ReadSnapshot: fixturePin(), Tables: []replay.SnapshotReadTable{{TableID: "0x01"}}}
	analyzer := &fakeAnalyzer{value: Analysis{SQL: "INSERT INTO tenant.a SELECT 1", TargetTableID: "0x01", ReadTableIDs: nil}}
	p, state := newPreparedPlugin(t, catalog, analyzer)
	if _, err := p.prepare(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	op, _, _ := state.snapshot()
	if op.Envelope.Input.ReadSet.Tables == nil || len(op.Envelope.Input.ReadSet.Tables) != 0 {
		t.Fatalf("constant SELECT read set = %#v, want non-nil empty", op.Envelope.Input.ReadSet.Tables)
	}
}
```
`fixturePin`, `newPreparedPlugin` and `fakeSigner` name the helpers the existing prepare tests already use; if their names differ, use the existing ones and keep the assertions.

- [ ] **Step 2: Run to verify they fail**

Run: `bazel test //pkg/plugins/sisnapshotquery:sisnapshotquery_test --test_filter='TestPrepare(SignsOnly|Refuses|ConstantSelect)'`
Expected: compile failure on `ReadTableIDs` (acceptable first failure), then assertion failures once the field exists.

- [ ] **Step 3: Implement**

`plugin.go:48-51`:
```go
type Analysis struct {
	SQL, TargetTableID, SchemaHash, RowIDProfileID string
	ClientRevision                                 uint32
	// ReadTableIDs is the analyzer's complete base-table closure (design D5).
	// The signed read set is exactly these pinned-catalog tables; an id
	// outside the catalog or a duplicate refuses the statement. An empty
	// closure (constant SELECT) signs an explicit empty table list.
	ReadTableIDs []string
}
```
Add below the `Analyzer` interface:
```go
// signedReadSet projects the pinned catalog onto the analyzer's closure. It
// never infers tables from SQL text, from the target, or from the catalog size.
func signedReadSet(catalog replay.SnapshotReadSet, closure []string) (replay.SnapshotReadSet, error) {
	byID := make(map[string]replay.SnapshotReadTable, len(catalog.Tables))
	for _, t := range catalog.Tables {
		if _, dup := byID[t.TableID]; dup {
			return replay.SnapshotReadSet{}, fmt.Errorf("sisnapshotquery: catalog table %q is duplicated", t.TableID)
		}
		byID[t.TableID] = t
	}
	out := replay.SnapshotReadSet{ReadSnapshot: catalog.ReadSnapshot, Tables: []replay.SnapshotReadTable{}}
	seen := make(map[string]bool, len(closure))
	for _, id := range closure {
		if seen[id] {
			return replay.SnapshotReadSet{}, fmt.Errorf("sisnapshotquery: read closure table %q is duplicated", id)
		}
		seen[id] = true
		t, ok := byID[id]
		if !ok {
			return replay.SnapshotReadSet{}, fmt.Errorf("sisnapshotquery: read closure table %q is not in the pinned catalog", id)
		}
		out.Tables = append(out.Tables, t)
	}
	return out, nil
}
```
In `prepare`, after `analysis, err := p.opts.Analyzer.PrepareSnapshotQuery(...)` succeeds:
```go
	readSet, err := signedReadSet(catalog.ReadSet, analysis.ReadTableIDs)
	if err != nil {
		return plugin.PreparedAgentQuery{}, err
	}
```
and in the `replay.SnapshotQueryInput{...}` literal replace `ReadSet: catalog.ReadSet` with `ReadSet: readSet`.

- [ ] **Step 4: Run the package tests**

Run: `bazel test //pkg/plugins/sisnapshotquery:sisnapshotquery_test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/plugins/sisnapshotquery/plugin.go pkg/plugins/sisnapshotquery/plugin_test.go
git commit -m "fix(sisnapshotquery): sign the analyzer's read closure instead of the whole catalog"
```

### Task 9: Persist ForwardAuthorized separately from SubmitAuthorized (HG)

**Why:** `pkg/plugins/sisnapshotquery/plugin.go:391` implements the relay's `AuthorizeForward` callback by calling `phasePort.AuthorizeSubmit(ctx)`, and `AgentPrepareCallbacks()` in `pkg/storageintegrity/snapshot_query_intake_phases.go:68-77` maps `AuthorizeForward: p.AuthorizeSubmit`. The intake's `Recover` (`snapshot_query_intake.go:186-204`) treats a `SubmitAuthorized` record as authority to Submit to the sequencer. Master plan: "After a gate win, persist separate ForwardAuthorized/SubmitAuthorized outside the reader/lock before I/O". Today the agent's forward gate win becomes host submit authority, so a crash after forwarding would submit on recovery even though no host submit gate ever ran.

**Files:**
- Modify: `pkg/storageintegrity/snapshot_query_journal.go` (stage + record field), `pkg/storageintegrity/snapshot_query_intake_phases.go` (new `AuthorizeForward`, `AuthorizeSubmit` precondition, callbacks map), `pkg/storageintegrity/snapshot_query_intake.go:178-183` (recover), `pkg/plugins/sisnapshotquery/plugin.go:88-95,391`
- Test: `pkg/storageintegrity/snapshot_query_intake_test.go`, `pkg/plugins/sisnapshotquery/plugin_test.go`

**Interfaces:**
- Produces: `SnapshotQueryStageForwardAuthorized SnapshotQueryJournalStage = "ForwardAuthorized"`; `SnapshotQueryJournalRecord.ForwardAuthorization *SnapshotQueryLaunchAuthorization`; `func (p *SnapshotQueryIntakePhasePort) AuthorizeForward(ctx context.Context) error`; the plugin-side `SnapshotQueryIntakePhasePort` interface gains `AuthorizeForward(context.Context) error`.
- Contract: `AuthorizeSubmit` now requires stage `ForwardAuthorized` (or is idempotent at `SubmitAuthorized`); `Recover` treats `ForwardAuthorized` like `SubmitIntent` (lookup/fence only, never Submit); `SubmitAfterAuthorization` still requires `SubmitAuthorized`. Superseded by follow-up F4 (final-review finding): `AuthorizeSubmit` admits both `SubmitIntent` (host lane, D8 `SubmitIntent → SubmitAuthorized`) and `ForwardAuthorized` (agent lane); the agent lane stays type-enforced because the plugin-side port interface omits `AuthorizeSubmit`.

- [ ] **Step 1: Write the failing tests**

`pkg/storageintegrity/snapshot_query_intake_test.go` (model the record construction and the recording fake sequencer on the existing `...IntentOnlyRecoveryDoesNotSubmitAfterNotFound` test):
```go
func TestRecoverForwardAuthorizedNeverSubmits(t *testing.T) {
	env := validSnapshotQueryEnvelope(t)
	journal := newMemoryJournal()
	rec := newSnapshotQueryRecordAtIntent(env)
	rec.Stage = SnapshotQueryStageForwardAuthorized
	rec.ForwardAuthorization = &SnapshotQueryLaunchAuthorization{InputRoot: env.InputRoot, OriginalJWSHash: replay.DigestString(env.UserJWS), ReservationID: env.Input.Binding.ReservationID, FencingGeneration: env.Input.Binding.FencingGeneration}
	if err := journal.Save(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	seq := &recordingSequencer{lookup: replay.SnapshotQueryStatus{Found: false}}
	intake := newTestIntake(t, journal, seq)
	if err := intake.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if seq.submits != 0 {
		t.Fatalf("ForwardAuthorized recovery submitted %d times; forward authorization is not submit authority", seq.submits)
	}
	if seq.lookups == 0 {
		t.Fatal("ForwardAuthorized recovery must look up and fence the request identity")
	}
}

func TestAuthorizeSubmitRequiresForwardAuthorization(t *testing.T) {
	env := validSnapshotQueryEnvelope(t)
	intake := newTestIntake(t, newMemoryJournal(), &recordingSequencer{accept: true})
	port, err := intake.NewSnapshotQueryIntakePhasePort(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := port.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if err := port.PersistSubmitIntent(ctx); err != nil {
		t.Fatal(err)
	}
	if err := port.AuthorizeSubmit(ctx); err == nil || !strings.Contains(err.Error(), `cannot authorize snapshot query submit from stage "SubmitIntent"`) {
		t.Fatalf("submit authorized without forward authorization: %v", err)
	}
	if err := port.AuthorizeForward(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := port.SubmitAfterAuthorization(ctx); err == nil || !strings.Contains(err.Error(), "requires durable authorization") {
		t.Fatalf("ForwardAuthorized alone allowed Submit: %v", err)
	}
	if err := port.AuthorizeSubmit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := port.SubmitAfterAuthorization(ctx); err != nil {
		t.Fatal(err)
	}
}
```
`validSnapshotQueryEnvelope`, `newMemoryJournal`, `recordingSequencer`, `newTestIntake` are the existing helper names in that test file; if they differ, use the existing ones.

`pkg/plugins/sisnapshotquery/plugin_test.go`: extend the existing fake phase port with call counters and add:
```go
func TestAuthorizeForwardCallbackDoesNotAuthorizeSubmit(t *testing.T) {
	port := &fakePhasePort{}
	p, qctx := newPluginWithPhasePort(t, port) // helper used by the existing phase-port tests
	prepared, err := qctx.AgentPrepare.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := qctx.AgentPrepare.PersistForwardIntent(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	if err := qctx.AgentPrepare.AuthorizeForward(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	if port.authorizeForwardCalls != 1 || port.authorizeSubmitCalls != 0 {
		t.Fatalf("forward callback calls: forward=%d submit=%d, want 1/0", port.authorizeForwardCalls, port.authorizeSubmitCalls)
	}
	_ = p
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestRecoverForwardAuthorizedNeverSubmits|TestAuthorizeSubmitRequiresForwardAuthorization'` and `bazel test //pkg/plugins/sisnapshotquery:sisnapshotquery_test --test_filter=TestAuthorizeForwardCallbackDoesNotAuthorizeSubmit`
Expected: compile failures on the new stage/method (acceptable first failure).

- [ ] **Step 3: Journal**

In `snapshot_query_journal.go` add to the stage const block, after `SnapshotQueryStageSubmitIntent`:
```go
	// ForwardAuthorized records the agent-side forward gate win. It is not
	// submit authority: recovery may only look up and fence, and only a host
	// submit gate may advance it to SubmitAuthorized.
	SnapshotQueryStageForwardAuthorized SnapshotQueryJournalStage = "ForwardAuthorized"
```
Add to `SnapshotQueryJournalRecord` after `LaunchAuthorization`:
```go
	ForwardAuthorization      *SnapshotQueryLaunchAuthorization `json:"forward_authorization,omitempty"`
```

- [ ] **Step 4: Phase port**

In `snapshot_query_intake_phases.go` add:
```go
// AuthorizeForward durably records the agent-side forward gate win. It is a
// distinct boundary from SubmitAuthorized (master plan: "persist separate
// ForwardAuthorized/SubmitAuthorized"): forwarding a Query to the host never
// authorizes the host to Submit.
func (p *SnapshotQueryIntakePhasePort) AuthorizeForward(ctx context.Context) error {
	return p.withStatementLock(ctx, func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		serviceCtx, cancel := p.intake.recoveryAttemptContext()
		defer cancel()
		if err := p.prepareLocked(serviceCtx); err != nil {
			return err
		}
		switch p.record.Stage {
		case SnapshotQueryStageForwardAuthorized, SnapshotQueryStageSubmitAuthorized:
			return verifyForwardAuthorization(p.record)
		case SnapshotQueryStageSubmitIntent:
			p.record.Stage = SnapshotQueryStageForwardAuthorized
			p.record.ForwardAuthorization = snapshotQueryLaunchAuthorization(p.env)
			if err := p.saveLocked(serviceCtx); err != nil {
				unknownErr := p.persistAuthorizationPersistenceUnknownAndReconcileLocked(serviceCtx)
				return errors.Join(err, unknownErr)
			}
			return nil
		default:
			return fmt.Errorf("storageintegrity: cannot authorize snapshot query forward from stage %q", p.record.Stage)
		}
	})
}
```
In `AuthorizeSubmit` change `case SnapshotQueryStageSubmitIntent:` to `case SnapshotQueryStageForwardAuthorized:` and insert `if err := verifyForwardAuthorization(p.record); err != nil { return err }` as the first statement of that case. In `AgentPrepareCallbacks` change `AuthorizeForward: p.AuthorizeSubmit,` to `AuthorizeForward: p.AuthorizeForward,`.

In `snapshot_query_intake.go` add next to `verifyLaunchAuthorization`:
```go
func verifyForwardAuthorization(rec SnapshotQueryJournalRecord) error {
	a := rec.ForwardAuthorization
	if a == nil || a.InputRoot != rec.Envelope.InputRoot || a.OriginalJWSHash != replay.DigestString(rec.Envelope.UserJWS) || a.ReservationID != rec.Envelope.Input.Binding.ReservationID || a.FencingGeneration != rec.Envelope.Input.Binding.FencingGeneration {
		return errors.New("storageintegrity: snapshot query durable forward authorization does not bind the original envelope")
	}
	return nil
}
```
and in `recoverRecord` add `SnapshotQueryStageForwardAuthorized` to the first `case` list (the one that returns `s.reconcileIntent(ctx, rec)`).

- [ ] **Step 5: Plugin**

In `plugin.go` add `AuthorizeForward(context.Context) error` to the `SnapshotQueryIntakePhasePort` interface (line 88–95) and change line 391 to `err = phasePort.AuthorizeForward(ctx)`. Give every test fake of that interface the new method.

- [ ] **Step 6: Verify**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test //pkg/plugins/sisnapshotquery:sisnapshotquery_test //pkg/proxy:proxy_test`
Expected: PASS, including the existing phase-chain tests updated to call `AuthorizeForward` before `AuthorizeSubmit`.

- [ ] **Step 7: Commit**

```bash
git add pkg/storageintegrity pkg/plugins/sisnapshotquery
git commit -m "fix(intake): persist forward authorization separately from submit authorization"
```

### Task 10: Reuse the connection after local query-only completion, per plan D2 (HG) — decision-gated

**Why:** HG #171 marks the session terminal on success, cancel and failure (`pkg/proxy/relay_query_only.go:98-118`, `relay.go:969-971`), so every INSERT ... SELECT connection becomes single-use. Integration plan D2 says: "drain a late empty marker … until the next Query packet; clear that allowance at the next query", and design D8 says framing is reusable after a supported terminal boundary, which local completion is. A late empty marker is unambiguous before the next Query packet: the next query's own marker can only follow its Query packet.

**Decision (uranuswch, 2026-09-20): A.** Implement the plan's allowance exactly as written below; Task 18 records the same decision in design D8.

**Files:**
- Modify: `pkg/proxy/relay.go:53-57,969-971`, `pkg/proxy/relay_query_only.go:98-118,170-180`
- Test: `pkg/proxy/relay_query_only_test.go:185-227,354-416`

**Interfaces:**
- Produces (unexported): `Relay.queryOnlyLateMarkerAllowed bool` replacing `queryOnlySessionTerminal`; `markQueryOnlyLateMarkerAllowed()`; `consumeQueryOnlyLateMarker(pkt *chproto.Packet) (drained bool, err error)`.

- [ ] **Step 1: Rewrite the two tests that freeze the single-use behaviour**

Replace `TestRelayQueryOnly_LocalSuccessMakesSessionTerminal` with:
```go
func TestRelayQueryOnly_LateMarkerIsDrainedAndNextQueryIsServed(t *testing.T) {
	clientProxy, clientPeer := net.Pipe()
	upstreamProxy, upstreamPeer := net.Pipe()
	defer clientProxy.Close()
	defer clientPeer.Close()
	defer upstreamProxy.Close()
	defer upstreamPeer.Close()
	sess := chsession.New(4, clientProxy)
	sess.Client().SetRevision(deferredTestRev)
	upstream := chproto.NewCodec(upstreamPeer, chproto.DirToUpstream)
	upstream.SetRevision(deferredTestRev)
	if err := sess.BindUpstream(context.Background(), upstream); err != nil {
		t.Fatalf("bind upstream: %v", err)
	}
	h := &queryOnlyHooks{host: newQueryOnlyHost(t, func(context.Context, *chproto.Query) (plugin.SnapshotQueryHostAdmission, error) {
		return plugin.SnapshotQueryHostAdmission{Run: func(context.Context) error { return nil }, CancelClient: func() {}, MaxControlBytes: 1024}, nil
	})}
	r := NewRelay(sess, h, nil, nil)
	done := make(chan error, 1)
	go func() { done <- r.clientToUpstream(context.Background()) }()
	if _, err := clientPeer.Write(encodeInsertQuery(t, "local", "INSERT INTO target SELECT 1")); err != nil {
		t.Fatalf("write local Query: %v", err)
	}
	if got := readExact(t, clientPeer, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("local terminal=%d, want EOS", got[0])
	}
	// Plan D2: one late empty marker is drained; it must never reach upstream.
	if _, err := clientPeer.Write(encodeEmptyClientData(t)); err != nil {
		t.Fatalf("write late query-only marker: %v", err)
	}
	_ = upstreamProxy.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	if _, err := upstreamProxy.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("late marker reached upstream: %v", err)
	}
	// The next ordinary Query clears the allowance and is forwarded upstream.
	if _, err := clientPeer.Write(encodeInsertQuery(t, "next", "SELECT 2")); err != nil {
		t.Fatalf("write next Query: %v", err)
	}
	_ = upstreamProxy.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := upstreamProxy.Read(make([]byte, 1)); err != nil {
		t.Fatalf("next Query did not reach upstream: %v", err)
	}
	if r.queryOnlyLateMarkerAllowed {
		t.Fatal("next Query did not clear the late-marker allowance")
	}
	clientPeer.Close()
	<-done
}
```
In `TestRelayQueryOnly_DelayedPacketAfterSuccessClosesSession` change the table: remove the `empty_marker` and `query` rows (now covered above) and add `{name: "second_empty_marker", raw: func(t *testing.T) []byte { return append(encodeEmptyClientData(t), encodeEmptyClientData(t)...) }}`; keep `named_marker`, `nonempty_marker`, `cancel`, `unknown` as close-the-session cases.

- [ ] **Step 2: Run to verify the new test fails**

Run: `bazel test //pkg/proxy:proxy_test --test_filter='TestRelayQueryOnly_(LateMarkerIsDrained|DelayedPacketAfterSuccess)'`
Expected: `LateMarkerIsDrained...` fails ("late marker" makes the loop return the terminal error), `second_empty_marker` passes for the wrong reason (terminal) — acceptable until Step 3.

- [ ] **Step 3: Implement the allowance**

`relay.go:53-57`: replace the field and comment with
```go
	// queryOnlyLateMarkerAllowed is set by a local query-only completion. The
	// client may still deliver this operation's empty external-table marker;
	// exactly one such marker is drained before the next Query packet, and any
	// Query packet clears the allowance (plan D2). Guarded by queryMu.
	queryOnlyLateMarkerAllowed bool
```
`relay.go:969-971`: replace the terminal check with
```go
		if drained, err := r.consumeQueryOnlyLateMarker(pkt); err != nil {
			return err
		} else if drained {
			continue
		}
```
`relay_query_only.go`: rename `markQueryOnlySessionTerminal` to `markQueryOnlyLateMarkerAllowed` (sets the new field) and `queryOnlySessionIsTerminal` is deleted; add
```go
// consumeQueryOnlyLateMarker applies the plan-D2 allowance. It returns
// drained=true for the one permitted late empty marker. A Query packet clears
// the allowance and is handled by the caller. Anything else while the
// allowance is set is a protocol violation and closes the connection.
func (r *Relay) consumeQueryOnlyLateMarker(pkt *chproto.Packet) (bool, error) {
	r.queryMu.Lock()
	allowed := r.queryOnlyLateMarkerAllowed
	if allowed {
		r.queryOnlyLateMarkerAllowed = false
	}
	r.queryMu.Unlock()
	if !allowed {
		return false, nil
	}
	switch pkt.Type {
	case uint64(chproto.ClientQueryCode):
		return false, nil
	case uint64(chproto.ClientDataCode):
		info, err := chproto.InspectClientDataPacket(pkt.Raw, r.sess.Client().Compression())
		if err != nil {
			return false, fmt.Errorf("classify late query-only marker: %w", err)
		}
		if info.BlockName != "" || !info.Empty {
			return false, errors.New("nonempty or external-table client Data after query-only local completion")
		}
		return true, nil
	default:
		return false, fmt.Errorf("client packet %s after query-only local completion; connection is not reusable", clientPacketName(pkt.Type))
	}
}
```
Keep the calls in `finishSuccess`, `finishCanceled` and `fail` (they now set the allowance instead of the terminal flag) and update their comments to cite plan D2.

- [ ] **Step 4: Verify**

Run: `bazel test //pkg/proxy:proxy_test`
Expected: PASS after rewriting the two remaining single-use tests to the plan-D2 behaviour: `TestRelayQueryOnly_CancelThenNextQueryClosesSession` becomes `TestRelayQueryOnly_CancelThenNextQueryIsServed` (a canceled local operation ends with EOS, then the next Query reaches upstream), and `TestRelayQueryOnly_RunErrorMakesPipelinedMarkerTerminal` becomes `TestRelayQueryOnly_RunErrorDrainsPipelinedMarker` (after the Exception, one pipelined empty marker is drained and the next Query is served). A Cancel packet arriving after local completion still closes the connection (the `cancel` row of the table test).

- [ ] **Step 5: Commit**

```bash
git add pkg/proxy/relay.go pkg/proxy/relay_query_only.go pkg/proxy/relay_query_only_test.go
git commit -m "fix(proxy): drain one late marker after query-only completion and serve the next query"
```

### Task 11: Grant a reservation only after this request's drain reached the published safe frontier (AR)

**Why:** `fsm/artifact_reservation_grant.go:58-66` grants from an idle barrier (no drain at all), and `fsm/artifact_reservation_drain.go:106-159` promotes draining→granted on fence equality only, never consulting `SafeWatermark`, claims or promotions. Design D1: the selected snapshot is the safe snapshot "after all previously admitted SI work in the network/shard has been published safe or resolved by a committed protocol abort. An intake's terminal ACK2 is not sufficient." Until C3's committed abort exists, "published safe" is `f.st.SafeWatermark.SafeBlockSeq` reaching the block frontier captured when the drain began.

**Files:**
- Modify: `fsm/state.go` (`ArtifactDispositionReservationState`, ~line 622), `fsm/artifact_reservation_drain.go:55-100,106-159`, `fsm/artifact_reservation_grant.go:20-100`
- Test: `fsm/artifact_reservation_drain_test.go`, `fsm/artifact_reservation_grant_test.go`

**Interfaces:**
- Produces: `ArtifactDispositionReservationState.DrainFrontierBlockSeq *uint64` (`json:"drain_frontier_block_seq,omitempty"`), captured by `applyBeginReservationDrain` as `uint64(len(f.st.Blocks))`; `applyCompleteReservationDrain` and `applyGrantReservation` reject unless `f.st.SafeWatermark.SafeBlockSeq >= *DrainFrontierBlockSeq`; `applyGrantReservation` requires an existing draining record for the same (client, statement, request) owned by the barrier and completes it (no idle→granted path remains).
- Snapshot format: no bump; the new field is `omitempty` and there are no production draining records to migrate.

- [ ] **Step 1: Write the failing tests**

Append to `fsm/artifact_reservation_drain_test.go`:
```go
func TestReservationDrainCompletionWaitsForPublishedSafeFrontier(t *testing.T) {
	f := newTestFSM(t)
	f.st.Blocks = append(f.st.Blocks, L3BlockHeader{L3BlockSeq: 1}, L3BlockHeader{L3BlockSeq: 2})
	begin := drainBegin()
	if got := f.applyBeginReservationDrain(begin, 41); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	key, _ := artifactDispositionReservationKey(begin.ClientAccount, begin.StatementID, begin.RequestID)
	record := f.st.ArtifactDisposition.Reservations[key]
	if record.DrainFrontierBlockSeq == nil || *record.DrainFrontierBlockSeq != 2 {
		t.Fatalf("drain frontier = %v, want the sequenced block frontier 2", record.DrainFrontierBlockSeq)
	}
	f.st.SafeWatermark.SafeBlockSeq = 1
	got := f.applyCompleteReservationDrain(drainCompletion(1), 47)
	if rejected, ok := got.(Rejected); !ok || !strings.Contains(rejected.Reason, "not published safe") {
		t.Fatalf("completion below the frontier = %#v, want frontier rejection", got)
	}
	if f.st.ArtifactDisposition.Reservations[key].State != ArtifactReservationDraining {
		t.Fatal("rejected completion mutated the draining record")
	}
	f.st.SafeWatermark.SafeBlockSeq = 2
	if got := f.applyCompleteReservationDrain(drainCompletion(1), 48); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("completion at the frontier = %#v", got)
	}
	restored := restoreSnapshot(t, snapshotBytes(t, f))
	if fr := restored.st.ArtifactDisposition.Reservations[key].DrainFrontierBlockSeq; fr == nil || *fr != 2 {
		t.Fatalf("drain frontier did not survive the snapshot round trip: %v", fr)
	}
}
```
`restoreSnapshot` is the existing helper that loads `snapshotBytes` output into a fresh FSM (grep `Restore(` in `fsm/snapshot_test.go` for its name).

In `fsm/artifact_reservation_grant_test.go` rewrite `TestApplyGrantReservationAtomicallyEstablishesServerOwnedGrant` so that it first calls `f.applyBeginReservationDrain(...)` for the same client/statement/request, sets `f.st.SafeWatermark.SafeBlockSeq` to the captured frontier, then asserts the grant is Applied and the record's `FencingGeneration` equals the drain fence (not fence+1). Add:
```go
func TestApplyGrantReservationRefusesWithoutThisRequestsDrain(t *testing.T) {
	f := newTestFSM(t)
	got := f.applyGrantReservation(grantCommand(), 7, "server-reservation")
	if rejected, ok := got.(Rejected); !ok || !strings.Contains(rejected.Reason, "requires this request's completed drain") {
		t.Fatalf("grant from idle = %#v, want drain requirement", got)
	}
	if f.st.ArtifactDisposition.ReservationBarrier.State != "" && f.st.ArtifactDisposition.ReservationBarrier.State != ArtifactReservationIdle {
		t.Fatal("rejected grant moved the barrier")
	}
}
```
`grantCommand()` is the existing fixture builder in that test file (use its real name).

- [ ] **Step 2: Run to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestReservationDrainCompletionWaitsForPublishedSafeFrontier|TestApplyGrantReservation'`
Expected: compile failure on `DrainFrontierBlockSeq`, then rejection-reason mismatches.

- [ ] **Step 3: State and drain**

`fsm/state.go`, inside `ArtifactDispositionReservationState`, after `BlockSeq`:
```go
	// DrainFrontierBlockSeq is the sequenced L3 block frontier captured when the
	// drain began. Completion and grant require the published safe watermark to
	// reach it (design D1). nil means the record predates this field and can
	// never complete.
	DrainFrontierBlockSeq *uint64 `json:"drain_frontier_block_seq,omitempty"`
```
Also copy it in `cloneArtifactDispositionReservationState` (copy the pointed-to value, not the pointer).

`applyBeginReservationDrain`: when building `record`, add `DrainFrontierBlockSeq: &frontier` where `frontier := uint64(len(f.st.Blocks))` is computed before the clone.

`applyCompleteReservationDrain`: after the `if active.State != ArtifactReservationDraining` check add
```go
	if active.DrainFrontierBlockSeq == nil || f.st.SafeWatermark.SafeBlockSeq < *active.DrainFrontierBlockSeq {
		return Rejected{Reason: "artifact disposition reservation drain frontier is not published safe"}
	}
```

- [ ] **Step 4: Grant**

In `applyGrantReservation` replace everything from `next := cloneArtifactDispositionForReservationGrant(d)` through the `record := &ArtifactDispositionReservationState{...}` / `putArtifactDispositionReservation` / `putArtifactDispositionReservationBarrier` block with:
```go
	reservationKey, _ := artifactDispositionReservationKey(grant.ClientAccount, grant.StatementID, cmd.RequestID)
	if d.ReservationTombstones[reservationKey] != nil {
		return Rejected{Reason: "artifact disposition reservation is terminal"}
	}
	draining := d.Reservations[reservationKey]
	barrier := normalizedArtifactDispositionReservationBarrier(d.ReservationBarrier)
	if draining == nil || draining.State != ArtifactReservationDraining || barrier.State != ArtifactReservationDraining || !barrierOwnsReservationDrain(barrier, draining) {
		return Rejected{Reason: "reservation grant requires this request's completed drain"}
	}
	if draining.DrainFrontierBlockSeq == nil || f.st.SafeWatermark.SafeBlockSeq < *draining.DrainFrontierBlockSeq {
		return Rejected{Reason: "artifact disposition reservation drain frontier is not published safe"}
	}
	generation := draining.FencingGeneration
	reservation := &replay.SnapshotQueryReservation{
		ReservationID: reservationID, FencingGeneration: generation,
		ClientAccount: grant.ClientAccount, StatementID: grant.StatementID,
	}
	if got := f.applyCompleteReservationDrain(ArtifactDispositionReservationDrainComplete{
		ClientAccount: grant.ClientAccount, StatementID: grant.StatementID, RequestID: cmd.RequestID,
		ControlBindingDigest: draining.ControlBindingDigest, FencingGeneration: generation, BlockSeq: draining.BlockSeq,
		Reservation: reservation,
	}, index); !reflect.DeepEqual(got, Applied{}) {
		return got
	}
	next := cloneArtifactDispositionForReservationGrant(&f.st.ArtifactDisposition)
	ensureDispositionMaps(next)
```
and keep the existing `next.Operations[operationKey] = ...`, `LastAppliedIndex` and `f.st.ArtifactDisposition = *next` tail. Delete the now-unused `math` import if nothing else in the file uses it.

- [ ] **Step 5: Verify**

Run: `bazel test //fsm:fsm_test`
Expected: PASS; `TestApplyGrantReservationRejectionsAreSnapshotInert` still passes (rejections happen before any mutation).

- [ ] **Step 6: Commit**

```bash
git add fsm/state.go fsm/artifact_reservation_drain.go fsm/artifact_reservation_grant.go fsm/artifact_reservation_drain_test.go fsm/artifact_reservation_grant_test.go
git commit -m "fix(fsm): grant a snapshot query reservation only after its drain reaches the published safe frontier"
```

### Task 12: Report a lookup identity conflict instead of absence (AR)

**Why:** `server/snapshot_query_reservation_projection.go:106-113,120-124` returns the NotFound projection when the caller's `reservation_id`/`fencing_generation` differ from the stored reservation. Coordination plan C2: a mismatched lookup is "a state conflict for lookup"; `found=false` "is not inferred". An agent recovering from a lost response must be able to distinguish "my reservation is gone" from "you are asking about the wrong generation".

**Files:**
- Modify: `server/snapshot_query_reservation_projection.go:106-124`
- Test: `server/snapshot_query_reservation_projection_test.go`

**Interfaces:**
- Produces: the projection function returns `status.Error(codes.FailedPrecondition, "snapshot query reservation identity conflict")` when identity matches but the bound reservation/generation does not; the empty-binding lookup and exact-match behaviours are unchanged.

- [ ] **Step 1: Write the failing test**

```go
func TestSnapshotQueryReservationProjectionReportsIdentityConflictNotAbsence(t *testing.T) {
	for _, tc := range []struct {
		name          string
		reservationID string
		generation    uint64
	}{
		{"wrong_generation", "reservation", 8},
		{"wrong_reservation", "other", 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := snapshotQueryReservationProjectionRequestFixture()
			request.binding.ReservationID, request.binding.FencingGeneration = tc.reservationID, tc.generation
			reader := &recordingSnapshotQueryReservationProjectionReader{view: fsm.ArtifactDispositionReservationReadView{NetworkID: "net", Active: snapshotQueryReservationProjectionActiveFixture()}, found: true}
			_, err := projectSnapshotQueryReservation(reader.read, request) // the function under test in this file
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("mismatched lookup = %v, want FailedPrecondition conflict", err)
			}
		})
	}
}
```
Use the real name of the projection function and the reader-injection shape the existing tests in this file use.

- [ ] **Step 2: Run to verify it fails**

Run: `bazel test //server:server_test --test_filter=TestSnapshotQueryReservationProjectionReportsIdentityConflictNotAbsence`
Expected: FAIL (`err == nil`, NotFound result returned).

- [ ] **Step 3: Implement**

Replace both `if !matchesSnapshotQueryReservationProjectionGeneration(...) { return result, nil }` blocks with
```go
		if !matchesSnapshotQueryReservationProjectionGeneration(view.Active.Reservation, view.Active.FencingGeneration, request.binding, true) {
			return snapshotQueryReservationProjectionResult{}, status.Error(codes.FailedPrecondition, "snapshot query reservation identity conflict")
		}
```
(and the same for `view.Terminal` with `false`).

- [ ] **Step 4: Verify and commit**

Run: `bazel test //server:server_test`
```bash
git add server/snapshot_query_reservation_projection.go server/snapshot_query_reservation_projection_test.go
git commit -m "fix(server): report a snapshot query lookup identity conflict instead of absence"
```

### Task 13: Verify executor-profile transition receipts and require a verifier quorum (AR)

**Why:** `fsm/apply_artifact_disposition.go:589-596` accepts any receipt whose `Signature` is a non-empty string; no key is checked and one receipt suffices. Design D10: "Validators independently … recompute the new state root with the new profile id, and seal a child manifest"; the transition is a consensus-critical activation and must carry a verifier quorum of real signatures, following the FSM's existing evidence convention (`verifyNodeSig`: ed25519 over the hash string bytes, hex signature, `VerifierQuorum = 2`).

**Files:**
- Modify: `fsm/apply_artifact_disposition.go:589-596`
- Test: `fsm/apply_artifact_disposition_test.go`

**Interfaces:**
- Contract: a transition receipt is `ed25519.Sign(replicaKey, []byte(transitionRoot))` hex-encoded by a node registered in `f.st.Nodes`; publication requires at least `VerifierQuorum` valid receipts from distinct replicas (receipts remain sorted unique by `ReplicaID`). Task 18 writes this convention into the design.

- [ ] **Step 1: Write the failing test**

Find the existing publish-transition test in `fsm/apply_artifact_disposition_test.go` (grep `TransitionReceipts`); it currently uses placeholder signatures. Add a helper and a test:
```go
func signedTransitionReceipts(t *testing.T, f *FSM, root string, replicas ...string) []replay.ExecutorProfileTransitionReceipt {
	t.Helper()
	out := make([]replay.ExecutorProfileTransitionReceipt, 0, len(replicas))
	for _, id := range replicas {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		f.st.Nodes[id] = &NodeInfo{Registration: arbiter.NodeRegistration{NodeID: id, Ed25519Pubkey: pub}}
		out = append(out, replay.ExecutorProfileTransitionReceipt{TransitionRoot: root, ReplicaID: id, Signature: hex.EncodeToString(ed25519.Sign(priv, []byte(root)))})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReplicaID < out[j].ReplicaID })
	return out
}

func TestPublishTransitionRequiresVerifiedQuorumReceipts(t *testing.T) {
	f, publish, root := newPublishTransitionFixture(t) // the existing fixture builder for the transition publish case
	publish.TransitionReceipts = []replay.ExecutorProfileTransitionReceipt{{TransitionRoot: root, ReplicaID: "v1", Signature: "sig"}}
	if reason := f.validatePublishTransition(publish, candidateFor(t, f, publish)); !strings.Contains(reason, "ed25519") && !strings.Contains(reason, "unknown node") {
		t.Fatalf("unverified receipt accepted: %q", reason)
	}
	publish.TransitionReceipts = signedTransitionReceipts(t, f, root, "v1")
	if reason := f.validatePublishTransition(publish, candidateFor(t, f, publish)); !strings.Contains(reason, "requires 2 valid receipts") {
		t.Fatalf("single receipt accepted: %q", reason)
	}
	publish.TransitionReceipts = signedTransitionReceipts(t, f, root, "v1", "v2")
	if reason := f.validatePublishTransition(publish, candidateFor(t, f, publish)); reason != "" {
		t.Fatalf("quorum of valid receipts rejected: %q", reason)
	}
}
```
`newPublishTransitionFixture` and `candidateFor` stand for the fixture helpers that the existing transition test uses; reuse them under their real names. Check `arbiter.NodeRegistration`'s field names in `fsm/state.go` / the `arbiter` package (`Ed25519Pubkey` is what `verifyNodeSig` reads).

- [ ] **Step 2: Run to verify it fails**

Run: `bazel test //fsm:fsm_test --test_filter=TestPublishTransitionRequiresVerifiedQuorumReceipts`
Expected: FAIL at the first assertion (placeholder signature accepted).

- [ ] **Step 3: Implement**

Replace the receipt loop at `fsm/apply_artifact_disposition.go:589-596` with:
```go
	lastReplica, valid := "", 0
	for _, receipt := range a.TransitionReceipts {
		if receipt.TransitionRoot != root || receipt.ReplicaID == "" || receipt.Signature == "" {
			return "transition receipt mismatch"
		}
		if lastReplica != "" && lastReplica >= receipt.ReplicaID {
			return "transition receipts must be sorted unique"
		}
		lastReplica = receipt.ReplicaID
		if err := f.verifyNodeSig(receipt.ReplicaID, root, receipt.Signature); err != nil {
			return fmt.Sprintf("transition receipt %s: %v", receipt.ReplicaID, err)
		}
		valid++
	}
	if valid < VerifierQuorum {
		return fmt.Sprintf("transition requires %d valid receipts, got %d", VerifierQuorum, valid)
	}
	return ""
```

- [ ] **Step 4: Update the existing transition tests to use signed receipts, verify, commit**

Run: `bazel test //fsm:fsm_test`
```bash
git add fsm/apply_artifact_disposition.go fsm/apply_artifact_disposition_test.go
git commit -m "fix(fsm): verify executor profile transition receipts and require a verifier quorum"
```

### Task 14: Authenticate the v3 envelope before consulting the historical gate (AC)

**Why:** `verifier/verifier.go:274` calls `SnapshotQueryReference` (the #29 historical gate, which the plan says will "durably register and fund the exact invocation") before `VerifySnapshotQuery` (`:287`) verifies the user's signature. Master plan / B5: "verifies the original A2 signature and roots, then history". Today the adapter is a read-only copy of `CheckJob`, so the harm is bounded, but once funding is real an unsigned job could consume it.

**Files:**
- Modify: `verifier/verifier.go:261-303`, `verifier/BUILD.bazel` (deps `@housegate//pkg/auth`, test `data`), `conformance/BUILD.bazel` (export the fixture)
- Test: `verifier/verifier_test.go`

**Interfaces:**
- Produces (unexported): `verifySnapshotQueryEnvelope(envelope replay.SnapshotQueryEnvelope) (account string, err error)`.

- [ ] **Step 1: Export the identity fixture to the verifier tests**

In `conformance/BUILD.bazel` add:
```python
filegroup(
    name = "snapshot_query_testdata",
    srcs = glob(["testdata/snapshot_query_*.json"]),
    visibility = ["//verifier:__pkg__"],
)
```
In `verifier/BUILD.bazel` add `data = ["//conformance:snapshot_query_testdata"],` to the `go_test` and `"@housegate//pkg/auth"` to both `deps` lists.

- [ ] **Step 2: Write the failing tests**

In `verifier/verifier_test.go` add:
```go
type snapshotIdentityFixture struct {
	Input      replay.SnapshotQueryInput `json:"input"`
	InputRoot  string                    `json:"input_root"`
	Identities []struct {
		UserJWS string `json:"user_jws"`
	} `json:"identities"`
}

func signedSnapshotQueryJob(t *testing.T) replay.SnapshotQueryJob {
	t.Helper()
	raw, err := os.ReadFile("../conformance/testdata/snapshot_query_identity_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var f snapshotIdentityFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return replay.SnapshotQueryJob{BlockSeq: 19, Statement: replay.SnapshotQueryStatement{StatementSeq: 71, Envelope: replay.SnapshotQueryEnvelope{Input: f.Input, InputRoot: f.InputRoot, UserJWS: f.Identities[0].UserJWS}}}
}

func TestHandleSnapshotQueryJobRejectsUnsignedJobBeforeReference(t *testing.T) {
	core := &fakeSnapshotQueryCore{results: []replay.SnapshotQueryAttestation{{ReplicaID: "v1"}}}
	references := &fakeSnapshotQueryReferenceProvider{references: []string{"registered"}}
	role, _ := newRoleHarnessVWithSnapshotQuery(t, core, references)
	job := signedSnapshotQueryJob(t)
	job.Statement.Envelope.UserJWS = ""
	err := role.handleSnapshotQueryJob(context.Background(), wire.SnapshotQueryJobToPB(job))
	if err == nil {
		t.Fatal("unsigned job was accepted")
	}
	if got := references.snapshot(); len(got) != 0 {
		t.Fatalf("reference provider consulted for an unsigned job: %d calls", len(got))
	}
	if jobs, _ := core.snapshot(); len(jobs) != 0 {
		t.Fatal("core invoked for an unsigned job")
	}
}
```
Change `TestRun_SnapshotQueryJobUsesTrustedReferenceAndSubmitsOnce` to build its job with `signedSnapshotQueryJob(t)` (keep `Reservation.ReservationID = "must-not-be-reference"`), so the happy path carries a valid signature.

- [ ] **Step 3: Run to verify the new test fails**

Run: `bazel test //verifier:verifier_test --test_filter='TestHandleSnapshotQueryJobRejectsUnsignedJobBeforeReference|TestRun_SnapshotQueryJobUsesTrustedReferenceAndSubmitsOnce'`
Expected: the unsigned-job test fails ("reference provider consulted").

- [ ] **Step 4: Implement**

In `verifier/verifier.go`, right after `job := wire.SnapshotQueryJobFromPB(m)`:
```go
	if _, err := verifySnapshotQueryEnvelope(job.Statement.Envelope); err != nil {
		r.d.Logger.Warn("snapshot query job signature rejected; refusing before any historical read", "block", m.GetBlockSeq(), "err", err)
		return err
	}
```
and add:
```go
// verifySnapshotQueryEnvelope authenticates the user's v3 envelope before any
// historical record, reference or funding decision runs (plan B5: signature
// and roots first, then history). The HG verifier repeats the same check; this
// copy keeps unsigned jobs away from the trusted reference provider.
func verifySnapshotQueryEnvelope(envelope replay.SnapshotQueryEnvelope) (string, error) {
	if err := replay.ValidateSnapshotQueryInput(envelope.Input); err != nil {
		return "", fmt.Errorf("validate complete input: %w", err)
	}
	root, err := replay.SnapshotQueryInputRoot(envelope.Input)
	if err != nil {
		return "", fmt.Errorf("recompute input root: %w", err)
	}
	if root != envelope.InputRoot {
		return "", fmt.Errorf("input_root mismatch")
	}
	account, err := auth.VerifyStatementV3Signature(envelope.UserJWS, auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding: envelope.Input.Binding, InputRoot: root})
	if err != nil {
		return "", err
	}
	if account != envelope.Input.Binding.ClientAccount {
		return "", fmt.Errorf("client_account does not match signature")
	}
	return account, nil
}
```
Import `github.com/housegate/housegate/pkg/auth`.

- [ ] **Step 5: Verify and commit**

Run: `bazel test //verifier:verifier_test //conformance:conformance_test`
```bash
git add verifier/verifier.go verifier/verifier_test.go verifier/BUILD.bazel conformance/BUILD.bazel
git commit -m "fix(verifier): verify the v3 signature before the historical gate"
```

### Task 15: Keep storage locations out of the disposition command root (AC) — decision-gated

**Why:** `wire/artifact_disposition.go:78` puts `storage_refs` into `canonicalPartManifestEntry`, `rejectNilSlices` demands it non-nil, and `wire/artifact_disposition_test.go:118-123` asserts it binds the root. Master plan: "Canonical records contain no storage-location hints"; acceptance A5: "A changed location serving the same authenticated bytes is usable". Binding locations into the command root means relocating an artifact changes the identity of an otherwise identical registration.

**Decision (uranuswch, 2026-09-20): A.** Exclude storage refs and re-freeze the vector once, as written below. This is allowed only because nothing consumes `artifact-disposition-command-v1` in production yet (AR's `Capability` has no setter); record the old and new roots in the compatibility note.

**Files:**
- Modify: `wire/artifact_disposition.go:70-79,92-100`, `wire/artifact_disposition_test.go:100-123`, `docs/compatibility/snapshot-query-raft-allocation.md`

**Interfaces:** the canonical JSON of every `RegisterCandidate` command loses `"storage_refs":[]`; `ArtifactDispositionCommandRoot` changes for such commands.

- [ ] **Step 1: Flip the test expectation first**

In the test replace the last block with:
```go
	mutated = command
	mutated.Action.RegisterCandidate.Manifest.Tables[0].ActiveParts[0].StorageRefs = []string{"s3://bucket/object"}
	changed, err = ArtifactDispositionCommandRoot(mutated)
	if err != nil || changed != root {
		t.Fatalf("storage refs bound the command root: %q %v", changed, err)
	}
```
and delete `,"storage_refs":[]` from `wantJSON`. Run `bazel test //wire:wire_test --test_filter=TestArtifactDispositionCommandRoot` (use the real test name); expected FAIL on `wantJSON` and on the root.

- [ ] **Step 2: Remove the field**

Delete the `StorageRefs []string` line from `canonicalPartManifestEntry` and `StorageRefs: part.StorageRefs,` from `canonicalManifest`. Update the comment above `canonicalSafeSnapshotManifest` to say locations are deliberately excluded (design D5: "StorageRefs are fetch hints").

- [ ] **Step 3: Freeze the new root once**

Temporarily add `t.Logf("root=%s", root)` before the `wantRoot` comparison, run the test, copy the printed value into `wantRoot`, remove the log line, run again. Record the old and new roots in `docs/compatibility/snapshot-query-raft-allocation.md` under a new "artifact-disposition-command-v1 canonical change (pre-release)" paragraph with the date and this task's PR number.

- [ ] **Step 4: Verify and commit**

Run: `bazel test //wire:wire_test //conformance:conformance_test`
```bash
git add wire/artifact_disposition.go wire/artifact_disposition_test.go docs/compatibility/snapshot-query-raft-allocation.md
git commit -m "fix(wire): keep storage locations out of the artifact disposition command root"
```

---

## Phase 2 — Cross-repository contract alignment and design text

### Task 16: Align the C2 control operation names and prove them with Housegate's signer (AR)

**Why:** HG freezes the signed control operations as `acquire`/`lookup`/`release` (`pkg/auth/snapshot_query_control.go:19-21`); AR uses `"get"` (`server/snapshot_query_control.go:71,103`, `server/snapshot_query_reservation_projection.go:131`) and its tests hard-code `"get"`, so no test crosses the boundary. Requires Task 5 (AR's HG pin must include #163).

**Files:**
- Modify: `server/snapshot_query_control.go:71,103`, `server/snapshot_query_reservation_projection.go:131`, `server/server_test.go:385-387`, `server/snapshot_query_reservation_projection_test.go:47`, `server/BUILD.bazel` (`go_test` deps add `"@housegate//pkg/auth"`)
- Create: `server/snapshot_query_control_crossrepo_test.go`

**Interfaces:** AR's `wire.SnapshotQueryControlBinding.Operation` for the reservation lookup RPC becomes `"lookup"`.

- [ ] **Step 1: Write the failing cross-repo test**

```go
package server

import (
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/sentioxyz/arbiter-core/wire"
)

// Every binding the server derives from a request must be byte-for-byte what
// Housegate's control signer signs; otherwise the agent's control JWS can never
// validate here. The conversion below is legal because both structs declare
// identical field names and types.
func TestSnapshotQueryControlBindingsMatchHousegateSignedOperations(t *testing.T) {
	signer, err := auth.NewRelaySigner("0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318")
	if err != nil {
		t.Fatal(err)
	}
	base := snapshotQueryControlRequestFields{networkID: "net", clientAccount: signer.Address(), statementID: "statement", requestID: "request", controlJWS: "unused", reservationID: "reservation", fencingGeneration: 7}
	for _, tc := range []struct {
		op  string
		got func() (wire.SnapshotQueryControlBinding, error)
	}{
		{auth.SnapshotQueryControlOperationAcquire, func() (wire.SnapshotQueryControlBinding, error) { return acquireSnapshotQueryBinding(base.acquire()) }},
		{auth.SnapshotQueryControlOperationLookup, func() (wire.SnapshotQueryControlBinding, error) { return getSnapshotQueryReservationBinding(base.get()) }},
		{auth.SnapshotQueryControlOperationRelease, func() (wire.SnapshotQueryControlBinding, error) { return releaseSnapshotQueryBinding(base.release()) }},
	} {
		t.Run(tc.op, func(t *testing.T) {
			got, err := tc.got()
			if err != nil {
				t.Fatal(err)
			}
			if got.Operation != tc.op {
				t.Fatalf("server binding operation %q, housegate signs %q", got.Operation, tc.op)
			}
			token, err := signer.SignSnapshotQueryControl(auth.SnapshotQueryControlBinding(got))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := auth.VerifySnapshotQueryControlSignature(token, auth.SnapshotQueryControlBinding(got)); err != nil {
				t.Fatalf("housegate cannot verify its own token against the server binding: %v", err)
			}
			if err := validSnapshotQueryControlBinding(got, token); err != nil {
				t.Fatalf("server rejects the housegate-signed binding: %v", err)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bazel test //server:server_test --test_filter=TestSnapshotQueryControlBindingsMatchHousegateSignedOperations`
Expected: FAIL on the `lookup` case (`server binding operation "get"`).

- [ ] **Step 3: Rename**

Change `Operation: "get"` to `Operation: "lookup"` at `snapshot_query_control.go:71`, the allowlist at `:103` to `binding.Operation != "acquire" && binding.Operation != "lookup" && binding.Operation != "release"`, the projection check at `snapshot_query_reservation_projection.go:131` to `!= "lookup"` with the message "requires lookup control binding", and the two test fixtures to `"lookup"`. Add a comment above the allowlist: `// Frozen by housegate pkg/auth: SnapshotQueryControlOperation{Acquire,Lookup,Release}.`

- [ ] **Step 4: Verify and commit**

Run: `bazel test //server:server_test`
```bash
git add server/snapshot_query_control.go server/snapshot_query_reservation_projection.go server/server_test.go server/snapshot_query_reservation_projection_test.go server/snapshot_query_control_crossrepo_test.go server/BUILD.bazel
git commit -m "fix(server): use housegate's frozen lookup control operation and prove it with its signer"
```

### Task 17: Make ordinary-statement classification identical in both engines (RG + RC)

**Why:** For GRANT/REVOKE/TRUNCATE/DESCRIBE/SHOW the Go engine returns `NOT_SNAPSHOT_QUERY` (`internal/engine/snapshot_query.go:243-248`) while the C++ engine returns `INVALID_INPUT` (`src/handlers/snapshot_query.cc:857-866` only recognizes select/union/use/create/drop/alter/update/delete). Plan A4 requires "byte-identical contract cases on both engines"; the shared corpus has only four ordinary cases, none of these. HG's capability probe treats `NOT_SNAPSHOT_QUERY` as the ordinary-statement acknowledgement, so the gRPC engine is wrong.

**Files:**
- RG: Modify `internal/harness/testdata/snapshot_query_cases.json`, `internal/harness/snapshot_query_test.go:48` (digest)
- RC: Modify `tests/testdata/snapshot_query_cases.json` (byte-identical copy), `tests/snapshot_query_test.cc:67` (digest), `tests/testdata/snapshot_query_ci_pins.json:13` (`sha256` and `analyze` count), `.github/scripts/snapshot-query-ci.py:41` (`"analyze": 636` → `641`), `src/handlers/snapshot_query.cc:857-866`

**Interfaces:** the corpus grows from 636 to 641 analyze cases; the new digest is computed once from the RG file and pinned in all four places.

- [ ] **Step 1: Add the cases in RG**

Append five cases to the `cases` array of `internal/harness/testdata/snapshot_query_cases.json`, each a copy of `ordinary_select` with only `name` and `request.sql` changed:
```json
{"name": "ordinary_grant",    "request": {"...same as ordinary_select...", "sql": "GRANT SELECT ON tenant.events TO reader"}, "expected_response": {"...same as ordinary_select..."}},
{"name": "ordinary_revoke",   "request": {"...", "sql": "REVOKE SELECT ON tenant.events FROM reader"}, ...},
{"name": "ordinary_truncate", "request": {"...", "sql": "TRUNCATE TABLE tenant.events"}, ...},
{"name": "ordinary_describe", "request": {"...", "sql": "DESCRIBE TABLE tenant.events"}, ...},
{"name": "ordinary_show",     "request": {"...", "sql": "SHOW TABLES FROM tenant"}, ...}
```
(`expected_response` is exactly `ordinary_select`'s: `code` `NOT_SNAPSHOT_QUERY`, empty message/SQL/target/columns/read ids.) Compute the digest: `shasum -a 256 internal/harness/testdata/snapshot_query_cases.json` and replace the constant at `internal/harness/snapshot_query_test.go:48`.

- [ ] **Step 2: Run RG**

Run: `SNAPSHOT_QUERY_ORDINARY=1 go test ./internal/harness/ -run SnapshotQuery`
Expected: PASS (the Go engine already classifies these as ordinary).

- [ ] **Step 3: Copy the corpus to RC and observe the failure**

Copy the RG file byte-for-byte to `tests/testdata/snapshot_query_cases.json`; update the digest at `tests/snapshot_query_test.cc:67`, `tests/testdata/snapshot_query_ci_pins.json:13` (and its `"analyze": 636` to `641`), and `.github/scripts/snapshot-query-ci.py:41` (`"analyze": 641`). On the build box run `./scripts.sh rebuild && ./build/rewriter_tests --gtest_filter='SnapshotQuery*'`.
Expected: five failures reporting `INVALID_INPUT` where `NOT_SNAPSHOT_QUERY` is expected.

- [ ] **Step 4: Fix the C++ classification**

In `src/handlers/snapshot_query.cc` add includes (confirm the exact paths on the pinned ClickHouse submodule with `grep -rl "class ASTGrantQuery\|class ASTDescribeQuery\|class ASTShowTablesQuery" clickHouse/src/Parsers`):
```cpp
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
```
and extend the ordinary condition in `analyze()`:
```cpp
      if (!outer && (root->as<DB::ASTSelectWithUnionQuery>() || root->as<DB::ASTUseQuery>() ||
                      root->as<DB::ASTCreateQuery>() || root->as<DB::ASTDropQuery>() ||
                      root->as<DB::ASTAlterQuery>() || root->as<DB::ASTUpdateQuery>() ||
                      root->as<DB::ASTDeleteQuery>() || root->as<DB::ASTGrantQuery>() ||
                      root->as<DB::ASTDescribeQuery>() || root->as<DB::ASTShowTablesQuery>())) {
```
(`TRUNCATE` and `REVOKE` parse to `ASTDropQuery` and `ASTGrantQuery` respectively.)

- [ ] **Step 5: Verify both engines and commit each repo**

RC: `./scripts.sh rebuild && ./build/rewriter_tests --gtest_filter='SnapshotQuery*'` → PASS. RG: Step 2 command → PASS. Confirm the two corpus files are identical: `shasum -a 256 <RG file> <RC file>`.
```bash
# RG
git add internal/harness/testdata/snapshot_query_cases.json internal/harness/snapshot_query_test.go
git commit -m "test(harness): pin ordinary DCL/DDL/metadata statements as NOT_SNAPSHOT_QUERY"
# RC
git add tests/testdata/snapshot_query_cases.json tests/snapshot_query_test.cc tests/testdata/snapshot_query_ci_pins.json .github/scripts/snapshot-query-ci.py src/handlers/snapshot_query.cc
git commit -m "fix(snapshot-query): classify GRANT/REVOKE/TRUNCATE/DESCRIBE/SHOW as ordinary like the Go engine"
```
Open the RC PR referencing the RG PR.

### Task 18: Amend the design for the decisions the code already made (HG)

**Why:** The engines implement D6's integer-only operator set while D3 still promises "deterministic scalar expressions" and type-agnostic JOIN keys; §1's table cites `SafeSnapshotManifest.ActiveParts` (the field lives on `TableManifest`, and `StorageRefs` on `PartManifestEntry`); D8 must record the Task 10 decision; D10 must state the receipt convention from Task 13; D1 must state the interim drain rule from Task 11.

**Files:**
- Modify: `docs/superpowers/specs/2026-09-16-signed-insert-select-design.md`

**Interfaces:** none.

- [ ] **Step 1: D3 → D6 catalog**

In D3, replace the sentence beginning "The initial profile admits a single INSERT target with SELECT projections and filters, deterministic scalar expressions, …" so that "deterministic scalar expressions" becomes "the scalar operations enumerated in D6 (column and literal projection, integer comparisons, boolean composition; no arithmetic in the initial profile)", and change "explicit `ALL INNER JOIN` on equality conditions between admitted relations" to "explicit `ALL INNER JOIN` on equality of D6-admitted key types". Append: "D6 is the single operator catalog; D3 names shapes, not operators." Also append to the `SQL_x_read_mode` paragraph of D3: "The agent lane refuses every user setting (`sisnapshotquery` rejects a non-empty settings set), which subsumes the `unsafe_latest` refusal; the executing host's admission (D2) must repeat the refusal independently because the engines' analysis contract does not carry settings."

- [ ] **Step 2: Field path**

In the §1 table row "Previous safe state" and in §4's sentence "`SafeSnapshotManifest.ActiveParts` and `StorageRefs` alone do not guarantee it", replace with "`SafeSnapshotManifest.Tables[].ActiveParts` and `PartManifestEntry.StorageRefs`".

- [ ] **Step 3: D1 interim drain rule**

Append to D1: "Until the committed abort transition (D9) exists, the Arbiter drains by requiring the published safe watermark to reach the L3 block frontier captured when the drain began; committed aborts join that rule when C3 lands. A grant is only reachable through a completed drain."

- [ ] **Step 4: D8 session reuse**

Append to D8 (decision A, taken 2026-09-20): "After local completion the relay drains at most one late empty external-table marker and clears that allowance at the next Query packet; the connection stays reusable. A Cancel or any other packet in that window closes the connection."

- [ ] **Step 5: D10 receipt convention**

Append to D10: "A transition receipt is the replica's ed25519 signature, hex-encoded, over the UTF-8 bytes of `transition_root`, keyed by the replica's registered node key; publication requires at least `VerifierQuorum` valid receipts from distinct replicas, sorted unique by replica id."

- [ ] **Step 6: Verify and commit**

```bash
git diff --check
for l in $(grep -o '\](\.\./[^)#]*' docs/superpowers/specs/2026-09-16-signed-insert-select-design.md | sed 's/](//' | sort -u); do test -f "docs/superpowers/specs/$l" || echo "BROKEN $l"; done
git add docs/superpowers/specs/2026-09-16-signed-insert-select-design.md
git commit -m "docs(storage-integrity): align the design with the implemented operator catalog, drain rule, receipts and session reuse"
```

---

## Not tasks: decisions for uranuswch

- **HG #157** (7,418-line "native qualification carrier" under `.github/ci2/`): no plan or spec asks for it; the integration plan asks for a manual target in the existing `ci.yml`. Recommend closing it and deleting its branch.
- **HG #156's v2 append change**: the PR states the shared append core now "preserves legal empty partitions the old algorithm omitted" (`payloadexec/executor.go`, −171 lines). Ask the replay owner to confirm, with a devnet2 manifest, that no existing chain's data root or state root changes when the new binary re-assembles an old block; if any does, that is a consensus-affecting change that needs its own D10-style transition.
- **AC `wire/command.go` `omitempty` on `UpdateConsensusParams`**: accepted as-is; the legacy fixture in `wire/consensus_snapshot_test.go:104` freezes the new rendering and Raft never hashes encoded command bytes.
- **Dead scaffolding** (9 AR interfaces with no implementation, HG ports with none, `ApplyServerOwned*` bypasses): not deleted here. Rule for the next PRs: a new interface or exported symbol lands together with its production caller, or not at all.
- **RG guidance drift** (rewriter-go): the 1,255-line snapshot profile engine sits in `internal/engine/` against that directory's AGENTS.md, plain `go test ./...` now fails without `SNAPSHOT_QUERY_ORDINARY=1` against the root AGENTS.md promise, and `internal/harness/SNAPSHOT_QUERY.md` still opens with "intentionally RED". Ask the RG owner for one PR that moves the policy into `internal/handlers/`, restores the plain-`go test` skip behaviour, and refreshes the three docs.
- **Stale compatibility notes** (arbiter-proto and arbiter-core `docs/compatibility/snapshot-query-raft-allocation.md`): both still say Raft tag 28 is unallocated although AP #8 / AC #23 allocated it, and the two files diverge. Fold the AC side into Task 15 if decision A is taken; otherwise one doc-only PR per repo.
