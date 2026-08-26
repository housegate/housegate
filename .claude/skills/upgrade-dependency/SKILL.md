---
name: upgrade-dependency
description: Use when bumping a Go dependency in housegate (the ClickHouse proxy in this repo) or in a repo that consumes it — rewriter-go, rewriter-proto, housegate, arbiter-core, the sentioxyz ch-go / clickhouse-go forks, or any module — covers replace-directive pins, the out-of-band FFI binary release, the MODULE.bazel bazel_dep/git_override pin that downstream repos add, the Bazel/gazelle re-sync, and the main-baseline rule for judging test failures.
---

# Upgrading a Dependency in Housegate

A dependency is pinned in **up to four different mechanisms**, and a naive `go get` only touches one of them. This skill is the checklist that keeps the others in sync and the verification ladder that proves the bump is safe.

Three of the four apply inside housegate. The fourth appears in repos that consume housegate or arbiter-core as **Bazel modules** (sentio-node does), and it is the dangerous one: its failure mode is a green test suite running against the sources you did not bump.

## When to Use

- "Bump rewriter-go to the latest release."
- "Update the sentioxyz ch-go / clickhouse-go forks."
- Any `go.mod` version change, including transitive fallout from one.

Not for: adding a *brand-new* dependency (that's `go get` + `bazel mod tidy` + adding it to `use_repo`, no version-pin fan-out), or for bumping Bazel module deps in `MODULE.bazel` (`bazel_dep` lines are hand-edited and unrelated).

## The Pin Mechanisms

| Mechanism | Modules | Bumped by | Gotcha |
|---|---|---|---|
| Plain `require` | `housegate/rewriter-go`, `housegate/rewriter-proto`, everything else | `go get <mod>@<ver>` | — |
| `replace` directive | `ClickHouse/ch-go` → `sentioxyz/ch-go`, `ClickHouse/clickhouse-go/v2` → `sentioxyz/clickhouse-go/v2`, `wasmerio/wasmer-go` → `sentioxyz/wasmer-go` | `go mod edit -replace` | **`go get` looks like it worked but changes nothing that compiles** — it only moves the `require` line, which is an MVS node label. The replace target is the code you actually build. |
| Out-of-band binary release | rewriter-go's polyglot **FFI lib** (`pkg/ffifetch`) | editing tag strings in configs + docs | The Go module and the `.so`/`.dylib` are versioned by the *same* rewriter-go tag but shipped separately. Bumping `go.mod` alone leaves the native engine on an old, possibly ABI-mismatched lib. |
| **Bazel module pin** (`bazel_dep` + `git_override` in `MODULE.bazel`) | in **downstream** repos that consume housegate / arbiter-core as Bazel modules — sentio-node does | hand-editing both the `bazel_dep` version and the `git_override` commit | **Housegate itself does not have this** (see Step 3), which is exactly what makes it easy to miss when you bump a dependency *elsewhere*. Where Bazel is the test ground truth, editing only `go.mod` leaves `bazel build` compiling the OLD sources while `go build` compiles the new ones — the suite passes against code you are not shipping, silently. Measured in sentio-node, 2026-08-26. |

## Recipe

### Step 1 — Find current pins and latest releases

```bash
grep -nE "rewriter-go|rewriter-proto|sentioxyz" go.mod
```

```bash
gh release list --repo housegate/rewriter-go --limit 5
```

Repeat `gh release list` for `housegate/rewriter-proto`, `sentioxyz/ch-go`, `sentioxyz/clickhouse-go`. Read the release notes (`gh release view <tag> --repo <repo>`) — they name the breaking changes you are about to hit.

**Never predict the next version number.** rewriter-go's and arbiter-core's `next-version.sh` derive it from the calendar day (`Asia/Shanghai` / UTC): the first cut of a day bumps the minor, later cuts the same day bump the patch. A document that names the tag it expects is wrong as soon as the cut slips past midnight — `v0.9.0` + one day is **v0.10.0**, not `v0.9.1`. Write a placeholder and resolve it at cut time.

**Verify the tag by content, not by its title.** A squash-merged PR means the individual commits are not ancestors of the tag, so `git merge-base --is-ancestor` says "not in" for changes that *are* in. Check the artifacts instead — the file that should have changed, a pinned fixture's checksum, a constant:

```bash
git show <tag>:<path/to/changed/file> | shasum -a 256
```

### Step 2 — Edit go.mod

Do the `replace` edits **first**, then `go get`, so `go get` re-resolves the graph the forks actually produce:

```bash
go mod edit -replace github.com/ClickHouse/ch-go=github.com/sentioxyz/ch-go@<tag> -replace github.com/ClickHouse/clickhouse-go/v2=github.com/sentioxyz/clickhouse-go/v2@<tag>
```

```bash
go get github.com/housegate/rewriter-go@<tag> && go mod tidy
```

Expect a wide transitive diff (otel, `golang.org/x/*`, testcontainers). That's MVS, not a mistake. **Do not** hand-pin transitives back down to reduce diff noise.

The `require` line for a replaced module drifting away from its replace target (e.g. `require ClickHouse/clickhouse-go/v2 v2.46.0` next to `replace ... => sentioxyz/clickhouse-go/v2 v2.47.0-sentioxyz-...`) is **normal and correct** — leave it alone unless MVS moves it.

### Step 3 — Re-sync Bazel

```bash
bazel mod tidy && bazel run //:gazelle
```

**In housegate**, `MODULE.bazel` uses `go_deps.from_file(go_mod = "//:go.mod")`, so a pure version bump needs **no `MODULE.bazel` edit and no `MODULE.bazel.lock` change** (the lock does not record `go_deps` results). Only a bump that promotes a module to a *direct* dependency changes the `use_repo(...)` list — `bazel mod tidy` handles that for you.

**In a downstream repo, check before you assume that.** `grep -n "bazel_dep\|git_override" MODULE.bazel`: if the module you are bumping appears there, it is pinned a second and third time — by version and by commit — and `go get` touches neither. Move both, and prove the commit is the tag rather than trusting a comment:

```bash
git -C <path-to-dep> rev-parse <tag>^{commit}
```

That hash must equal the `git_override` commit exactly. This is the one mechanism whose failure is completely silent: no compile error, no test failure, just a suite that green-lights the wrong sources.

If gazelle rewrites unrelated `load()` ordering, that's pre-existing drift; keep it (it's what the next gazelle run produces anyway) and say so in the PR.

### Step 4 — Chase the version string through configs and docs

```bash
grep -rn "<old-tag>" --include="*.go" --include="*.yaml" --include="*.json" --include="*.md" . | grep -v "^./bazel-" | grep -v docs/superpowers/plans
```

For a rewriter-go bump, the known fan-out is:

| File | What to update |
|---|---|
| `configs/local.server.yaml` | `rewriter.native_library_release:` sample tag |
| `configs/local.server-mock-remote.yaml` | same key, commented out |
| `CLAUDE.md` (`pkg/ffifetch` bullet) | the "requires an FFI library built from rewriter-go >= vX.Y.Z (polyglot >= vA.B.C — the go.mod floor)" sentence |

Historical tags inside `docs/superpowers/plans/` and `docs/superpowers/specs/` are a record of what was true then — **do not** rewrite them.

## Verification Ladder

Run in this order; each rung is cheaper than the next and localises the breakage.

| Rung | Command | Notes |
|---|---|---|
| 1. Compile | `go build ./...` | Fastest signal for removed/renamed API. |
| 2. Vet | `go vet ./... 2>&1 \| grep -v "unkeyed fields"` | The `config.Duration ... unkeyed fields` notes are **pre-existing noise** — filter them or you will chase ghosts. Vet catches test-only breakage that `go build` misses. |
| 3. Bazel build | `bazel build //...` | Matches CI. |
| 4. Bazel test | `bazel test //...` | Ground truth. Integration targets are `manual`-tagged and skipped here. |
| 5. Native FFI smoke | see below | **Only rung that exercises the new `.dylib`/`.so`.** |
| 6. Docker integration | see below | Only rung that exercises the real clickhouse-go wire path. |

### Rung 5 — native engine against the NEW FFI lib

The lib is cached per tag, so pointing at the new tag is the whole test:

```bash
go run ./cmd fetch-rewriter-lib --tag <new-tag>
```

It prints the resolved path to stdout (logs go to stderr). Feed that into Bazel:

```bash
bazel test //pkg/rewriter:rewriter_test --test_env=POLYGLOT_SQL_FFI_PATH=<printed-path> --test_output=all --test_arg=-test.v --nocache_test_results
```

**Confirm the tests actually RAN.** `TestNativeEngineSmoke` and `TestMaterialize_NativeSmoke` `t.Skip()` when `POLYGLOT_SQL_FFI_PATH` is unset — a green `--test_output=errors` run proves nothing. Grep the verbose output for `--- PASS: TestNativeEngineSmoke`.

### Rung 6 — docker integration

The Bazel sandbox does not inherit the docker socket — pass it explicitly. Find it with `docker context ls` (OrbStack on macOS: `unix://$HOME/.orbstack/run/docker.sock`):

```bash
bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_output=errors --test_env=DOCKER_HOST=<socket-from-docker-context-ls> --test_env=HOME
```

CLI-driven subtests self-skip unless `tests/bin/clickhouse` exists (CI installs it); that's fine locally.

## The Main-Baseline Rule

CLAUDE.md: *"Before claiming a regression, diff your failing-test set against a clean `main` build — matching set = no regression."* Housegate's integration suite has genuinely flaky tests. **Never** report an integration failure as caused by your bump until you have measured it on `main`.

**Do not use `git stash` for this.** An earlier version of this skill recommended it. The stash stack is shared across **every worktree of the repo**, and this repo routinely has several live at once: a `stash push` of already-committed paths creates nothing, and the following `pop` then applies *someone else's* stash into your tree. That happened on 2026-08-26 and cost a recovery. Check out the baseline paths instead — same directory, so Bazel's output base is unchanged and the rebuild stays warm:

```bash
git checkout origin/main -- <paths-you-changed>
```

```bash
bazel test //pkg/integration:integration_test --test_filter='<TheFailingTest>' --runs_per_test=10 --nocache_test_results --test_output=summary --test_env=DOCKER_HOST=<socket> --test_env=HOME
```

```bash
git checkout HEAD -- <paths-you-changed>
```

`--runs_per_test=10` is the point — a single run can't distinguish "broken by my change" from "fails half the time anyway". Staying in the same directory matters: Bazel keys its output base on workspace path, so a worktree forces a full cold rebuild.

Record the ratio in the commit message and PR (e.g. "fails 5/10 on clean main, 1/5 on this branch — pre-existing flake"). A bare "it's flaky" is not evidence.

## Commit and PR

Commit message should carry: the version table, **the API break and why the fix is behaviour-preserving**, and the verification results including the baseline ratio.

`gh` gotchas in this repo:
- `origin` is `git@sentio:housegate/housegate.git` (an SSH host alias for github.com). Pushing works with the SSH key; `gh` uses its own token and may be a *different* identity.
- If `gh pr create` fails with `GraphQL: must be a collaborator`, the active `gh` account lacks access. Check with `gh api user --jq '.login'` and `gh api repos/housegate/housegate --jq '.permissions'`.
- **`gh auth switch` must be verified in the same shell invocation as the command that depends on it.** The active account has been observed reverting between separate tool calls — always chain `gh auth switch --user <x>; gh api user --jq '.login'; <command>`.

## Common Mistakes

| Mistake | Fix |
|---|---|
| `go get github.com/ClickHouse/ch-go@<tag>` to bump the sentio fork | It's a `replace`. Use `go mod edit -replace ...=github.com/sentioxyz/ch-go@<tag>`. |
| Bumping rewriter-go in `go.mod` and stopping | The FFI binary is a separate artifact. Update `native_library_release` in configs + the version-floor line in CLAUDE.md, and run the rung-5 smoke against the new tag. |
| Trusting `go build ./...` alone | It doesn't compile `_test.go` files. ch-go v0.73.0's removal of `proto.Exception.Nested` only surfaced under `go vet` / `bazel test`. |
| Green rewriter test without setting `POLYGLOT_SQL_FFI_PATH` | The native smoke tests skipped. Pass `--test_env=POLYGLOT_SQL_FFI_PATH=...` and read the verbose output. |
| Calling an integration failure a regression after one run | Measure it on stashed-clean `main` with `--runs_per_test=10` first. |
| Hand-pinning transitive bumps back down | MVS chose them; fighting it produces an unbuildable graph. Let the diff be wide. |
| Editing `MODULE.bazel` to match new versions | It reads `go.mod`. Only `bazel mod tidy` touches it, and usually it has nothing to do. |
| Rewriting old tags in `docs/superpowers/{plans,specs}/` | Those are point-in-time records, not live config. |
| Bumping `go.mod` in a repo whose `MODULE.bazel` also pins the module | Move the `bazel_dep` version and the `git_override` commit too, and check the commit equals the tag. Bazel is the test ground truth; only `go.mod` moving is a silent split. |
| Treating a version floor as enforced because it is written down | A pin governs the artifact you *fetch*; it proves nothing about an engine someone points at with an env var — and two releases can ship byte-identical artifacts while their behaviour differs, because the change lived in the Go module. If a floor matters for correctness, add a startup probe that fails against the old build, and prove it fails by downgrading. |

## Red Flags — Stop and Reconsider

- "The replace target and the require version disagree, let me align them" → That's the normal shape. The replace target wins; the require line is an MVS label.
- "Tests are green, ship it" → Did rung 5 actually run, or did it skip? Did rung 6 run at all?
- "This integration test broke, must be the bump" → Not until you've run it 10× on clean `main`.
- "I'll reduce the diff by pinning the transitives back" → No. Report the fallout in the PR instead.
- "I'll update CLAUDE.md later" → The FFI version floor is load-bearing for the next person; it goes in the same commit.
- "`bazel test` is green, so the bump is fine" → In a repo with a `bazel_dep`/`git_override` pin, green Bazel after a `go.mod`-only bump means it compiled the **old** sources. Confirm `MODULE.bazel` moved.
- "The tag's title says it contains the fix" → Squash merges make that the only evidence, and it is not evidence. Diff a file.
