# Dynamic SI Table Set devnet2 Upgrade Rollout Implementation Plan (sub-project 5b)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring every devnet2 component the dynamic storage-integrity table set needs onto its released build with behaviour unchanged, and prepare (not perform) registry enablement: the Arbiter voters and verifiers move to a tagged Arbiter release by a stop-all cutover, indexer-a moves to sentio-node `2af43bd` with the contract-V2 in-pod rewriter and the v0.15.0 agent sidecar asking the storage RPC for table status, consensus administration is enabled with no update committed, and the production charts can render a disabled `table_registry` section with one archive-node `rpc_url` per voter.

**Architecture:** One production PR carries every shared change and is inert on merge: `charts/sentio-node` gains a registry-driven shape selected by values (no `housegate.storageIntegrity.tables` → `enabled: true`; an `http(s)` sidecar `networkStateSource` → no declared schemas, port tied to `node.ports.storage_rpc`) while the legacy shape renders byte-identically; `charts/storage-integrity` gains an optional `arbiter.tableRegistry` section whose `rpc_url` the existing per-pod `render-config` init container substitutes by StatefulSet ordinal; the devnet2 overlay turns the inert section on; the render tests name the stage pins in constants; a new runbook is added and the si-v2 runbook is marked superseded where it conflicts. Each live stage is then one values commit (with its test-constant mirror) pushed to `main` and applied with `helmfile sync`, executed step by step by the user. A local rehearsal of the stop-all cutover (three v0.7.1 voters with an old-protocol data plane and an EVM anchor, then the release, then the rollback) supplies the evidence and the offline `statedigest` gate stage 1 runs on the real backups.

**Tech Stack:** Helm 4 charts and helmfile (production repo), Python 3.12 + PyYAML 6.0.2 render tests, the Arbiter (`arbiter`, `arbiter-admin`, `arbiter-verifier`, `arbiter-anchor`) built with Go from `v0.7.1` and `$ARBITER_RELEASE_TAG`, anvil (Foundry) for the rehearsal's EVM anchor, kubectl against `sentio-sea` / `sentio-network-devnet2`, `docker buildx imagetools`, `gh`.

**Spec:** `docs/superpowers/specs/2026-09-28-dynamic-si-table-set-devnet2-rollout-design.md` (binding: decisions R1–R10, stages 0–2b, §4.6 operating rules, §5 testing). Background: the 5a spec and plan (PR housegate/housegate#213), sub-project 4's plan handoff, and the arbiter README "Rollout (P11)". Out of scope: stage 3 (registry, auditors, watcher), which this plan documents in the runbook and hands off.

## Global Constraints

- **Repositories and bases.** production `origin/main` at `739619f06` or later, one branch `dev/poetry/si-dynamic-table-set-devnet2` for Tasks 4–7 (production `CLAUDE.md` branch convention); arbiter `origin/main` at `cf38430` or later; sentio-node `2af43bd9906e76c91e892579ec687463ef6cfdb7`; housegate docs PRs #213 and #215. Never switch branches in `/Users/uranuswch/Dev/housegate/housegate`; use worktrees.
- **Pins the controller fills in** (never predict them): `$ARBITER_RELEASE_TAG` (Task 1; `scripts/next-version.sh` makes it `v0.8.0` when cut on a UTC day after 2026-09-17), `$ARBITER_RELEASE_SHA` (its commit), `$ARBITER_RELEASE_DIGEST` (its image index digest), `$ARBITER_RELEASE_AMD64_DIGEST` (its linux/amd64 manifest digest), `$SENTIO_NODE_DIGEST` (Task 2, the index digest of `ghcr.io/sentioxyz/sentio-node:sha-2af43bd9906e76c91e892579ec687463ef6cfdb7`). In shell blocks they are variables set by Tasks 1–2; wherever they appear inside file content, write the recorded literal value.
- **Fixed pins.** In-pod rewriter `us-west1-docker.pkg.dev/sentio-352722/sentio/housegate-rewriter:0.15.0@sha256:57812c8cf40603dab0fa84f8f0c1c6270990c2a74820d1450e20d65c618e4989`; sidecar `ghcr.io/housegate/housegate:v0.15.0@sha256:1b51de76b7b7a15a8006d0ac06febb10012c74c92d4950d07e4e5a5b77b3570f` (index digest measured 2026-09-28; re-verified in Task 11); Arbiter v0.7.1 index `sha256:0a83dded9bea0b5ab7b58f48a131fbc7f5a1427040579790fd4ddca75f6af32c`, its amd64 manifest `sha256:b36e44407ae9d5c356bf1188940a36bcb83cc221e3c920ff83f6bc0774da8ac4`.
- **Blast radius (R8, the user's rule).** Shared `charts/` changes, the devnet2 inert block and the runbook go through the one production PR (Task 8). Stage 1, 2 and 2b are single values commits pushed directly to `main`; each carries its render-test mirror (`DEVNET2_ARBITER_DIGEST`, `FRESH_SOURCE_IMAGE` and the `docs/examples` copy, `DEVNET2_SIDECAR_VERSION`, `DEVNET2_CONSENSUS_UPDATES_ENABLED`) so `main` CI stays green and `git revert` of that commit is the stage's rollback.
- **Deploy path.** Manual `helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=<release>` from the repository root; nothing deploys on push. `sync`, not `apply`: it restores a StatefulSet scaled to zero.
- **Live infrastructure.** Only the HUMAN-GATED tasks (9–12) touch `sentio-sea`, and only as the user runs or explicitly confirms each step; an agent never advances them. Every other task is local or GitHub-only.
- **Shell.** HUMAN-GATED blocks are bash, run from the production repository root in one dedicated session started with `bash` (from fish) and then `set -eo pipefail`; a failed check ends the session on purpose. Re-run the runbook §0 helper block after any restart of the session.
- **Tests.** `python3 -B charts/storage-integrity/tests/test_render.py` (58 tests after Task 6) and the CI lint profiles in `.github/workflows/storage-integrity-chart.yml`. Inertness is proven by rendering all ten consumer releases before and after (`render-all.sh`, Task 4).
- **Working directories.** The production worktree for Tasks 4–8 is `PROD="$HOME/src/production-si-5b"` (branch `dev/poetry/si-dynamic-table-set-devnet2`). `R="$HOME/si-devnet2-5b-rehearsal"` holds the rehearsal worktrees and binaries; keep it until Task 10 is verified (its `statedigest` binaries gate stage 1).
- **Conventions.** Markdown not hard-wrapped; English; every commit message ends with a blank line and `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; PR descriptions end with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

## Review Focus

1. **v0.7.1 silently ignores `table_registry`.** Its `config.Load` is a plain `yaml.Unmarshal`, so rendering the block before stage 1 is harmless, and the same property would make an auditor or watcher enabled on a v0.7.1 voter a no-op that looks active. Checks: Task 3 Step 4 starts three v0.7.1 voters with the block and Step 9 loads every rendered per-voter config with both builds; Task 5 `test_enabled_registry_refuses_the_runtime_that_ignores_it` (the chart refuses audit/watcher on either v0.7.1 digest); Task 10 Step 8 checks every pod's image digest out of band, because the capability probe answers `protocol_version 1` for both builds.
2. **The cutover is only reversible from complete, verified backups.** The release writes v14 snapshots that v0.7.1 refuses (`unsupported snapshot version 14`), so a partial or unverified backup turns a failed cutover into a lost network. Checks: Task 9 sizes free space and backup time; Task 10 Step 5 takes the backups only with every voter stopped, verifies each extracted tarball against an in-pod sha256 manifest, and Step 6 replays every backup offline with both builds and requires equal state before anything starts; Task 3 Steps 6–8 rehearse the refusal and the full-cluster restore.
3. **StatefulSet mechanics do not do what they seem to.** A sync may leave the arbiter StatefulSet at the zero replicas `kubectl scale` set; `OnDelete` means a sync never replaces running Arbiter pods (stage 2b); verifiers are RollingUpdate and roll with the same sync. Checks: Task 10 Step 7 scales to 3 explicitly and Step 8 lists every arbiter and verifier image; Task 12 deletes each Arbiter pod (followers first, leader last) and probes `updates_enabled: true` on each voter's own address.
4. **The agent's status source must be this pod's storage RPC.** A wrong port or host makes every status lookup fail, so the agent sends unsigned INSERTs that the server refuses (733); a leftover YAML source would keep answering Ordinary for tables the registry later governs. Checks: Task 4 `test_registry_shape_chart_guards` (port must equal `node.ports.storage_rpc`, declared schemas refused with an RPC source, a localhost source needs the SNode) and `test_registry_driven_source_binds` (validator rejects any other source); Task 11 Step 4 calls the RPC from inside the pod and reads the sidecar's `network state loaded from RPC` log and its failure counter, and Step 5 proves the signed lane with an INSERT that becomes safe.
5. **The chart PR must be inert for every consumer.** `charts/sentio-node` is shared with testnet-v2 and the si-v1 overlay is the rollback surface; any render change would roll or break releases on their next sync. Checks: Task 4 Step 6 and Task 5 Step 5 require all ten consumer renders byte-identical, Task 6 Step 6 allows exactly the inert ConfigMap block, one init-container `case`/`sed` addition and the arbiter `checksum/config` change; Task 5 `test_default_renders_no_table_registry`; Task 4 `test_registry_shape_changes_only_the_table_source` (old/new overlay equivalence).

## Plan decisions

- **P1 — Chart shape by values, not by chart version (item 1).** The spec asks the chart to stop rendering `housegate.storage_integrity.tables`, render `enabled: true` and relax the sidecar guards, and also requires the devnet2 overlays to render byte-identically. Both hold only if the shape follows the values: a non-empty `tables` list renders exactly as today (the si-v1 overlay, today's si-v2 values and any rollback), an empty list renders `enabled: true`. Changing the rendering unconditionally would also hand `enabled: true` without tables to the running housegate v0.14.1 on the next sync. The "`tables` is required" guard is dropped; the equality guard (`node.yaml:35-36`) is kept for a non-empty list only, because sentio-node `184bbf6`/`2af43bd` fails startup on any other list (its README) — failing at render time is earlier, and it constrains nothing the spec needs. Measured: all ten consumer renders (seven devnet2, three testnet-v2) are byte-identical after Tasks 4 and 5; the stage 2 shape changes exactly `tables` → `enabled: true`, the agent source, the dropped `housegate-agent-network-state.yaml` key, three images and the checksum.
- **P2 — Per-voter `rpc_url` through the existing init container (item 2).** The Arbiter config loader has no environment expansion (`config.Load`: `yaml.Unmarshal`, then `os.Getenv` for the two private keys only). The chart already renders one ConfigMap template and substitutes per-pod values (`__NODE_ID__`, `__BOOTSTRAP__` from the ordinal) in the `render-config` init container. The section renders `rpc_url: "__TABLE_REGISTRY_RPC_URL__"` and the init container selects `arbiter.tableRegistry.rpcURLs[ordinal]` with a `case` and one more `sed`; both are emitted only when `rpcURLs` is set, and the section enters `arbiterConfigChecksum` only then, so charts without it keep their checksum. Verified by executing the rendered init script per ordinal (Task 5 `init_rendered_arbiter_config`) and by loading each result with v0.7.1 (ignores the key) and the release (`TableRegistry.Enabled()` false, `rpc_url` per voter).
- **P3 — No L2 change (item 2).** `charts/sentio-network/sentio-network/templates/op-reth.yaml` already renders single-instance ClusterIP Services `sentio-node-op-reth-{0,1,2}` (selector `app.kubernetes.io/name: op-reth-<i>`, port 8545) beside the load-balanced `sentio-node-op-reth`; ordinals 0 and 1 run op-geth `--gcmode=archive`, ordinal 2 runs op-reth without archive flags. Voters map to op-reth-0, op-reth-1, op-reth-0 (R4: spread over both archives; the chart requires at least two distinct URLs). Task 9 confirms both answer `eth_chainId` `0x786d4d` and a historical `eth_getBalance`.
- **P4 — Sidecar source `http://localhost:32003` (item 3).** sentio-node `standalone.go:458` binds the storage RPC on `0.0.0.0:<storage_node_rpc_port>`, rendered from `node.ports.storage_rpc` (32003 for indexer-a); the sidecar shares the pod network namespace. housegate v0.15.0 treats an `http(s)://` source as `RpcNetworkState` (`pkg/config/config.go` `IsRpcSource`), calls `sentio_getStorageIntegrityTableStatus` lazily (no startup call, so container start order does not matter) and logs `network state loaded from RPC`. The chart ties a localhost source's port to `node.ports.storage_rpc` and refuses declared schemas with an RPC source.
- **P5 — Stop-all mechanics (item 5).** Intake pause = no SI writer plus a drained Arbiter (`open_block_statements 0`, `unanchored_verified_blocks 0`, safe = sealed on all voters); indexer-a stays up and its calls retry. Voters stop by `kubectl scale --replicas=0`. Each `data-storage-integrity-si-v2-arbiter-<i>` PVC is `local-path` (node-local, 2Gi), so a helper pod mounting it read-only is scheduled onto the right node; the backup streams `tar` to the operator with an in-pod sha256 manifest (a copy inside the 2Gi PVC might not fit: the rehearsal's snapshots were 40–60 MB for ~70 blocks). The arbiter v0.7.1 image carries `tar`, `sha256sum`, `wget`, `od`, `du`, `df` (checked). Leader and term come from the `election won` log line; the leader is the voter that answers `consensus show`.
- **P6 — Offline replay gate.** A throwaway `statedigest` tool restores a copy of one voter's data directory into an FSM (latest snapshot, then every log command) and hashes each top-level key of the resulting snapshot document. Built against v0.7.1 and the release, it must agree per voter before any release pod starts. Rehearsal: all three voters and both builds agreed on 17 shared keys plus the spent-ID accumulator; the release adds only `artifact_disposition`.
- **P7 — Rehearsal fidelity.** External processes, not the in-process harness: three `arbiter` processes with the chart's config shape (EVM anchor on anvil chain 7892301 with an AnchorRegistry deployed by v0.7.1's `arbiter-anchor`, a disabled `table_registry` block, chart raft timings except `snapshot_interval 5s`, `snapshot_threshold 64`, `trailing_logs 128` so snapshots and a trailing log exist). Traffic is the integration package's fake SNode and three fake verifiers built against v0.7.1 (arbiter-core v0.8.0, the old-protocol data plane), with one patch: v0.7.1 refuses a later promotion ack that lacks the complete safe inventory, so the fake SNode keeps it in a file restored with the backups.
- **P8 — The release image.** `cut-release.yml` tags `main` and chains `docker-push.yml` with `ref: <tag>`, a fresh build from the tagged tree, so its digest differs from the CI image `sha-cf38430@sha256:115e81b3…` of the same tree. The pin is the tag's index digest after `imagetools` shows OCI `revision` equal to the tag's commit and `version` equal to the tag.
- **P9 — Stage 2 pins sentio-node `2af43bd` and moves the table source.** `2af43bd` (#189) is `184bbf6` plus the I1 fix (every genesis id outside the create-mode preflight loads its genesis schema by hash under `network_state`), with no dependency change; its main-CI image was still queued at planning time (run 36413822831), so Task 2 records the digest. Stage 2 also removes the legacy `tables` list and the sidecar `tableSchemas` (the chart refuses schemas beside an RPC source), so stage 3 needs no indexer-a values change.
- **P10 — Test constants name the stage pins.** The chart PR introduces `DEVNET2_ARBITER_DIGEST`, `DEVNET2_SIDECAR_VERSION` and `DEVNET2_CONSENSUS_UPDATES_ENABLED` and makes the agent-state assertion accept both shapes, so each stage commit changes one constant beside its values. Simulated on a local branch: stage 1 changes exactly the five Arbiter image references; stage 2 changes exactly P1's list; stage 2b changes `consensus_updates_enabled` and the arbiter checksum; the suite passes after each.
- **P11 — Stage 2b replaces pods one at a time.** All voters run the same release after stage 1, so a rolling replacement (followers first, leader last, sealed tip equal before the next) keeps a quorum; the rehearsal replaced all three with `consensus_updates_enabled: true`, each probe reported `updates_enabled: true`, and commits continued. `consensus show` from the release adds `artifact_disposition_capability` and `table_registry` (null) under `bootstrap`/`current`; the runbook compares a stable projection.
- **P12 — Chart deny list, not allow list.** The release digest does not exist when the chart PR is written, so `validate.yaml` refuses audit/watcher on the two v0.7.1 digests rather than allowing only the release; the devnet2 digest itself is pinned by `DEVNET2_ARBITER_DIGEST`.

## File Structure

- **Production, Task 4:** modify `charts/sentio-node/templates/node.yaml`, `charts/storage-integrity/tests/validate_source_binding.py`; test `charts/storage-integrity/tests/test_render.py`.
- **Production, Task 5:** modify `charts/storage-integrity/values.yaml`, `values.schema.json`, `templates/arbiter.yaml`, `templates/_helpers.tpl`, `templates/validate.yaml`; test `tests/test_render.py`.
- **Production, Task 6:** modify `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml`; test `charts/storage-integrity/tests/test_render.py`.
- **Production, Task 7:** create `docs/storage-integrity-devnet2-dynamic-table-set.md`; modify `docs/storage-integrity-devnet2-si-v2.md`.
- **Production, Tasks 10–12 (direct pushes):** `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml`, `sentio-node-indexer-a-storage-integrity-si-v2.yaml`, `sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml`, `docs/examples/storage-integrity-fresh-source-image.yaml`, `charts/storage-integrity/tests/test_render.py` (constants only).
- **Local only (never committed), Task 3:** `$R/arbiter-old` (v0.7.1) and `$R/arbiter-new` (`$ARBITER_RELEASE_TAG`) worktrees with `integration/rehearsal_driver_test.go`, the fake SNode patch, `cmd/statedigest/main.go` and `cmd/cfgcheck/main.go`; `$R/rehearsal.sh`; Task 4's `$R/render-all.sh`.
- **housegate, Task 13:** this plan's execution record (PR #215).

---

## Task 1: Cut the Arbiter release and record its pins (controller-only, gated)

The implementer does not run this task.

**Files:** none. **Interfaces:** Produces `$ARBITER_RELEASE_TAG`, `$ARBITER_RELEASE_SHA`, `$ARBITER_RELEASE_DIGEST`, `$ARBITER_RELEASE_AMD64_DIGEST`.

- [ ] **Step 1: Confirm `main` is releasable.**

```bash
A=/Users/uranuswch/Dev/sentio_xyz/arbiter
git -C "$A" fetch origin --tags
git -C "$A" merge-base --is-ancestor cf38430 origin/main
git -C "$A" show origin/main:go.mod | grep -E '^\s*github.com/sentioxyz/arbiter-core v0\.10\.1$'
gh run list --repo sentioxyz/arbiter --branch main --workflow ci.yml --limit 1
```

Expected: both git checks exit 0, the grep prints the `arbiter-core v0.10.1` line, and the latest `ci` run on `main` is `completed success` (on 2026-09-28 it was run 36249483081 for `cf38430`).

- [ ] **Step 2: Cut the tag.**

```bash
before="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
gh workflow run cut-release.yml --repo sentioxyz/arbiter --ref main
run_id=""
until [ -n "$run_id" ]; do
  sleep 3
  run_id="$(gh run list --repo sentioxyz/arbiter --workflow cut-release.yml --limit 5 --json databaseId,createdAt -q ".[] | select(.createdAt > \"$before\") | .databaseId" | head -n 1)"
done
gh run watch "$run_id" --repo sentioxyz/arbiter --exit-status
```

Expected: the `tag and release` job and the chained `push versioned image` job succeed (the tag job took under a minute on 2026-09-17). If only the image job fails, re-run that job alone (`gh run rerun "$run_id" --repo sentioxyz/arbiter --failed`); re-dispatching the workflow would cut another tag.

- [ ] **Step 3: Verify the tag by content and the image by provenance, and record the pins.**

```bash
git -C "$A" fetch origin --tags
ARBITER_RELEASE_TAG="$(git -C "$A" tag --list 'v0.*' --sort=-v:refname | grep -E '^v0\.[0-9]+\.[0-9]+$' | head -n 1)"
ARBITER_RELEASE_SHA="$(git -C "$A" rev-parse "$ARBITER_RELEASE_TAG^{commit}")"
git -C "$A" merge-base --is-ancestor cf38430 "$ARBITER_RELEASE_SHA"
git -C "$A" show "$ARBITER_RELEASE_TAG:go.mod" | grep -q 'github.com/sentioxyz/arbiter-core v0.10.1'
img="ghcr.io/sentioxyz/arbiter:$ARBITER_RELEASE_TAG"
ARBITER_RELEASE_DIGEST="$(docker buildx imagetools inspect "$img" | sed -n 's/^Digest: *//p')"
ARBITER_RELEASE_AMD64_DIGEST="$(docker buildx imagetools inspect "$img" --format '{{json .Manifest}}' | jq -r '.manifests[] | select(.platform.architecture=="amd64") | .digest')"
docker buildx imagetools inspect "$img" --format '{{json .Image}}' | jq -r '.config.Labels["org.opencontainers.image.revision"], .config.Labels["org.opencontainers.image.version"]'
echo "$ARBITER_RELEASE_TAG $ARBITER_RELEASE_SHA $ARBITER_RELEASE_DIGEST $ARBITER_RELEASE_AMD64_DIGEST"
```

Expected: every command exits 0; the tag is `v0.8.0` unless another cut happened first; the label lines print `$ARBITER_RELEASE_SHA` and `$ARBITER_RELEASE_TAG`; both digests are `sha256:` + 64 hex and differ from `sha256:0a83dded…f32c` and from the CI image `sha256:115e81b3…` (P8). Record the four values for every later task. No commit.

## Task 2: Record the sentio-node `2af43bd` image pin (controller-only, gated)

**Files:** none. **Interfaces:** Produces `$SENTIO_NODE_DIGEST`.

- [ ] **Step 1: Wait for the main-push CI run of `2af43bd`.** On 2026-09-28 it was run 36413822831 (`CI Build`, queued).

```bash
run_id="$(gh run list --repo sentioxyz/sentio-node --branch main --commit 2af43bd9906e76c91e892579ec687463ef6cfdb7 --limit 1 --json databaseId -q '.[0].databaseId')"
gh run watch "$run_id" --repo sentioxyz/sentio-node --exit-status
gh run view "$run_id" --repo sentioxyz/sentio-node --log | grep -oE 'pushing manifest for ghcr\.io/sentioxyz/sentio-node:sha-2af43bd9906e76c91e892579ec687463ef6cfdb7@sha256:[0-9a-f]{64}' | head -n 1
```

Expected: jobs `build`, `integration-clickhouse` and `docker-push` succeed; the grep prints the pushed tag with its digest (this is how the `184bbf6` digest `sha256:5ba1a69b…` was found in run 36391141088).

- [ ] **Step 2: Resolve and verify the digest.**

```bash
img=ghcr.io/sentioxyz/sentio-node:sha-2af43bd9906e76c91e892579ec687463ef6cfdb7
SENTIO_NODE_DIGEST="$(docker buildx imagetools inspect "$img" | sed -n 's/^Digest: *//p')"
docker buildx imagetools inspect "$img" --format '{{json .Image}}' | jq -r '.config.Labels["org.opencontainers.image.revision"]'
echo "$SENTIO_NODE_DIGEST"
```

Expected: the revision label is `2af43bd9906e76c91e892579ec687463ef6cfdb7` and the digest equals the one Step 1 printed. Record it. No commit.

## Task 3: Local stop-all rehearsal (evidence only, nothing committed)

Reproduces, against the real release, the rehearsal behind P5–P7 and builds the `statedigest` binaries Task 10 needs. Nothing touches devnet2; nothing is committed to arbiter.

**Files (all under `$R`, local):** create `arbiter-old` and `arbiter-new` worktrees; create `arbiter-old/integration/rehearsal_driver_test.go`; patch `arbiter-old/integration/fake_snode_test.go`; create `cmd/statedigest/main.go` and `cmd/cfgcheck/main.go` in both worktrees; create `rehearsal.sh`.

**Interfaces:** Consumes `$ARBITER_RELEASE_TAG`. Produces `$R/bin/{old,new}/statedigest` and the rehearsal evidence for Task 13.

- [ ] **Step 1: Worktrees.**

```bash
R="$HOME/si-devnet2-5b-rehearsal"
A=/Users/uranuswch/Dev/sentio_xyz/arbiter
mkdir -p "$R/bin/old" "$R/bin/new"
git -C "$A" fetch origin --tags
git -C "$A" worktree add --detach "$R/arbiter-old" v0.7.1
git -C "$A" worktree add --detach "$R/arbiter-new" "$ARBITER_RELEASE_TAG"
```

- [ ] **Step 2: The traffic driver.** Create `$R/arbiter-old/integration/rehearsal_driver_test.go`:

```go
package integration

// Throwaway stop-all rehearsal driver (sub-project 5b). It reuses this
// package's fake SNode and fake verifiers, built against arbiter v0.7.1's
// arbiter-core and arbiter-proto (the old-protocol data plane), and points
// them at three externally started arbiter processes.
//
//   REHEARSAL_GRPC        comma-separated gRPC addresses of arb-1,arb-2,arb-3
//   REHEARSAL_STATEMENTS  statements to submit in this run (default 40)
//   REHEARSAL_METRICS     one voter's /metrics URL
//   REHEARSAL_INVENTORY   file keeping the fake SNode's complete safe inventory
//
// TestRehearsalSchemaRoot prints the genesis schema_root for the rehearsal
// table so the arbiter YAML can pin it.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sentioxyz/arbiter/config"
)

func TestRehearsalSchemaRoot(t *testing.T) {
	cfg := config.Config{
		Genesis: config.GenesisConfig{NetworkID: "testnet-integration", SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "housegate-replay-mvp-v0"},
		Tables: []config.TableConfig{{
			TableID: testTable,
			Columns: []config.ColumnConfig{{Name: "value", Type: "UInt64"}},
		}},
	}
	fmt.Printf("SCHEMA_ROOT=%s\n", integrationGenesis(t, cfg).SchemaRoot)
}

func externalCluster(t *testing.T) *cluster {
	t.Helper()
	raw := os.Getenv("REHEARSAL_GRPC")
	if raw == "" {
		t.Skip("REHEARSAL_GRPC is not set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cl := &cluster{
		ctx: ctx, cancel: cancel, nodes: map[string]*testApp{},
		conns: map[string]*grpc.ClientConn{}, dead: map[string]bool{},
		sealMaxStatements: 2,
	}
	for i, addr := range strings.Split(raw, ",") {
		id := fmt.Sprintf("arb-%d", i+1)
		conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("grpc client %s: %v", id, err)
		}
		cl.nodes[id] = &testApp{id: id}
		cl.conns[id] = conn
	}
	t.Cleanup(cl.close)
	return cl
}

func TestRehearsalDrive(t *testing.T) {
	cl := externalCluster(t)
	n := 40
	if raw := os.Getenv("REHEARSAL_STATEMENTS"); raw != "" {
		var err error
		if n, err = strconv.Atoi(raw); err != nil {
			t.Fatalf("REHEARSAL_STATEMENTS: %v", err)
		}
	}
	before, err := leaderWatermark(cl)
	if err != nil {
		t.Fatalf("watermark before: %v", err)
	}
	sn := startFakeSNode(t, cl)
	verifiers := make([]*fakeVerifier, 0, 3)
	for i := 1; i <= 3; i++ {
		verifiers = append(verifiers, startFakeVerifier(t, cl, i))
	}
	key, account := newAccount(t)
	for seq := uint64(1); seq <= uint64(n); seq++ {
		submitStatement(t, cl, sn, key, account, seq)
	}
	if n > 0 {
		waitWatermark(t, cl, sn, before.GetSafeBlockSeq()+1, 120*time.Second, verifiers...)
		waitCleanupAcked(t, sn, 30*time.Second)
	}
	// A block may carry more than seal.max_statements, so wait until every
	// sealed block is safe and nothing is open.
	waitFor(t, "safe watermark == sealed tip", 300*time.Second, func() bool {
		failOnBackground(t, sn, verifiers)
		m := rehearsalMetrics(os.Getenv("REHEARSAL_METRICS"))
		return m["arbiter_sealed_block_seq"] > 0 &&
			m["arbiter_safe_watermark_block_seq"] == m["arbiter_sealed_block_seq"] &&
			m["arbiter_open_block_statements"] == 0
	})
	after, err := leaderWatermark(cl)
	if err != nil {
		t.Fatalf("watermark after: %v", err)
	}
	fmt.Printf("DRIVE statements=%d safe_before=%d safe_after=%d snapshot_id=%s\n",
		n, before.GetSafeBlockSeq(), after.GetSafeBlockSeq(), after.GetSnapshotId())
}

var rehearsalInventoryMu sync.Mutex

func rehearsalLoadInventory() map[string][]*pb.SafePartMapping {
	inv := map[string][]*pb.SafePartMapping{}
	if raw, err := os.ReadFile(os.Getenv("REHEARSAL_INVENTORY")); err == nil {
		_ = json.Unmarshal(raw, &inv)
	}
	return inv
}

// rehearsalInventory returns the complete post-promotion inventory (the
// previous safe parts plus this promotion's), or nil outside a rehearsal run.
// v0.7.1 refuses a later promotion ack that carries only the candidates.
func rehearsalInventory(table, partition string, parts []*pb.SafePartMapping) []*pb.SafePartMapping {
	if os.Getenv("REHEARSAL_INVENTORY") == "" {
		return nil
	}
	rehearsalInventoryMu.Lock()
	defer rehearsalInventoryMu.Unlock()
	old := rehearsalLoadInventory()[table+"/"+partition]
	seen := map[string]bool{}
	out := make([]*pb.SafePartMapping, 0, len(old)+len(parts))
	for _, part := range append(append([]*pb.SafePartMapping(nil), old...), parts...) {
		if !seen[part.GetPartRowLthash()] {
			seen[part.GetPartRowLthash()] = true
			out = append(out, part)
		}
	}
	return out
}

func rehearsalSaveInventory(table, partition string, complete []*pb.SafePartMapping) {
	rehearsalInventoryMu.Lock()
	defer rehearsalInventoryMu.Unlock()
	inv := rehearsalLoadInventory()
	inv[table+"/"+partition] = complete
	raw, _ := json.Marshal(inv)
	_ = os.WriteFile(os.Getenv("REHEARSAL_INVENTORY"), raw, 0o600)
}

func rehearsalMetrics(url string) map[string]uint64 {
	out := map[string]uint64{}
	resp, err := http.Get(url)
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && strings.HasPrefix(fields[0], "arbiter_") {
			if v, err := strconv.ParseFloat(fields[1], 64); err == nil {
				out[fields[0]] = uint64(v)
			}
		}
	}
	return out
}
```

Save this patch as `$R/fake_snode.patch` and apply it with `git -C "$R/arbiter-old" apply "$R/fake_snode.patch"`; `git -C "$R/arbiter-old" status --short` then lists `M integration/fake_snode_test.go` and `?? integration/rehearsal_driver_test.go`:

```diff
diff --git a/integration/fake_snode_test.go b/integration/fake_snode_test.go
index a508439..e013dfe 100644
--- a/integration/fake_snode_test.go
+++ b/integration/fake_snode_test.go
@@ -84,10 +84,21 @@ func (s *fakeSNode) ackPromotion(ctx context.Context, promote *pb.PromoteSafePar
 		PostPartitionCommitment: postRoot,
 		Parts:                   parts, Applied: true,
 	}
-	return s.cl.withLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
+	// Rehearsal: acks after the first promotion must carry the complete safe
+	// inventory, which a real SNode reads from hg_safe. Keep it in a file so
+	// it survives driver runs and is restored with the Raft backups.
+	complete := rehearsalInventory(promote.GetTableId(), promote.GetPartitionId(), parts)
+	if complete != nil {
+		ack.SafePartitionParts = complete
+	}
+	err = s.cl.withLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
 		_, err := pb.NewPromotionGatewayClient(conn).AckPromotion(ctx, ack)
 		return err
 	})
+	if err == nil && complete != nil {
+		rehearsalSaveInventory(promote.GetTableId(), promote.GetPartitionId(), complete)
+	}
+	return err
 }
 
 func (s *fakeSNode) ackCleanup(ctx context.Context, cleanup *pb.UnsafeCleanup) error {
```

- [ ] **Step 3: The two throwaway tools, in both worktrees.** Create `$R/arbiter-old/cmd/statedigest/main.go`:

```go
// Throwaway offline state digest (sub-project 5b rehearsal). Built inside an
// arbiter worktree, it restores the newest Raft snapshot of a COPY of one
// voter's raft.data_dir into that worktree's FSM, applies every log command
// after it, and prints a sha256 per top-level key of the resulting snapshot
// document. Running the v0.7.1 build and the release build on the same copy
// shows whether the release replays that voter's state identically.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/sentioxyz/arbiter/fsm"
)

type sink struct {
	bytes.Buffer
}

func (s *sink) ID() string    { return "digest" }
func (s *sink) Cancel() error { return nil }
func (s *sink) Close() error  { return nil }

func main() {
	dir := flag.String("data-dir", "", "copy of one voter's raft.data_dir")
	network := flag.String("network-id", "", "genesis.network_id")
	snapshotID := flag.String("schema-snapshot-id", "", "genesis.schema_snapshot_id")
	profile := flag.String("executor-profile-id", "", "genesis.executor_profile_id")
	maxWriters := flag.Uint("max-writers", 1, "genesis.max_writers")
	authority := flag.String("authority", "", "authority.allowed_addresses (comma-separated)")
	flag.Parse()
	if err := run(*dir, fsm.Params{
		NetworkID: *network, SchemaSnapshotID: *snapshotID, ExecutorProfileID: *profile,
		MaxWriters: uint64(*maxWriters), AuthorityAddresses: strings.Split(*authority, ","),
	}); err != nil {
		fmt.Fprintln(os.Stderr, "statedigest:", err)
		os.Exit(1)
	}
}

func run(dir string, params fsm.Params) error {
	state, err := fsm.New(params)
	if err != nil {
		return fmt.Errorf("fsm: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(dir, 2, io.Discard)
	if err != nil {
		return fmt.Errorf("snapshot store: %w", err)
	}
	metas, err := snaps.List()
	if err != nil {
		return fmt.Errorf("list snapshots: %w", err)
	}
	var from uint64
	if len(metas) > 0 {
		_, rc, err := snaps.Open(metas[0].ID)
		if err != nil {
			return fmt.Errorf("open snapshot %s: %w", metas[0].ID, err)
		}
		if err := state.Restore(rc); err != nil {
			return fmt.Errorf("restore snapshot %s: %w", metas[0].ID, err)
		}
		from = metas[0].Index
		fmt.Printf("snapshot_index %d\n", from)
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		return fmt.Errorf("bolt: %w", err)
	}
	defer store.Close()
	last, err := store.LastIndex()
	if err != nil {
		return err
	}
	first, err := store.FirstIndex()
	if err != nil {
		return err
	}
	if from+1 < first && from < last {
		return fmt.Errorf("log gap: snapshot %d, first log %d", from, first)
	}
	applied := 0
	for i := from + 1; i <= last; i++ {
		var entry raft.Log
		if err := store.GetLog(i, &entry); err != nil {
			return fmt.Errorf("log %d: %w", i, err)
		}
		if entry.Type != raft.LogCommand {
			continue
		}
		if res, ok := state.Apply(&entry).(error); ok && res != nil {
			fmt.Printf("apply_error %d %v\n", i, res)
		}
		applied++
	}
	fmt.Printf("last_index %d\napplied_commands %d\n", last, applied)
	snap, err := state.Snapshot()
	if err != nil {
		return err
	}
	var out sink
	if err := snap.Persist(&out); err != nil {
		return err
	}
	raw := out.Bytes()
	if len(raw) < 13 || string(raw[:4]) != "AFSM" {
		return fmt.Errorf("unexpected snapshot container")
	}
	n := binary.BigEndian.Uint64(raw[5:13])
	fmt.Printf("written_version %d\n", raw[4])
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw[13:13+n], &doc); err != nil {
		return err
	}
	keys := make([]string, 0, len(doc))
	for key := range doc {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		var v any
		if err := json.Unmarshal(doc[key], &v); err != nil {
			return err
		}
		canonical, _ := json.Marshal(v)
		fmt.Printf("key %s %x\n", key, sha256.Sum256(canonical))
	}
	fmt.Printf("spent_ids %x\n", sha256.Sum256(raw[13+n:]))
	return nil
}
```

and `$R/arbiter-old/cmd/cfgcheck/main.go`:

```go
// Throwaway (sub-project 5b): load an Arbiter YAML exactly as cmd/arbiter
// does and print the parsed configuration as JSON.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/sentioxyz/arbiter/config"
)

func main() {
	cfg, err := config.Load(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "config.Load:", err)
		os.Exit(1)
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal:", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}
```

Copy both into the release worktree and build:

```bash
mkdir -p "$R/arbiter-new/cmd/statedigest" "$R/arbiter-new/cmd/cfgcheck"
cp "$R/arbiter-old/cmd/statedigest/main.go" "$R/arbiter-new/cmd/statedigest/main.go"
cp "$R/arbiter-old/cmd/cfgcheck/main.go" "$R/arbiter-new/cmd/cfgcheck/main.go"
(cd "$R/arbiter-old" && go build -o "$R/bin/old/" ./cmd/arbiter ./cmd/arbiter-admin ./cmd/arbiter-anchor ./cmd/arbiter-verifier ./cmd/statedigest ./cmd/cfgcheck \
  && go test -c -o "$R/bin/driver.test" ./integration)
(cd "$R/arbiter-new" && go build -o "$R/bin/new/" ./cmd/arbiter ./cmd/arbiter-admin ./cmd/arbiter-verifier ./cmd/statedigest ./cmd/cfgcheck)
"$R/bin/driver.test" -test.run 'TestRehearsalSchemaRoot$' | grep SCHEMA_ROOT
```

Expected: both builds succeed and the last line prints `SCHEMA_ROOT=0x24fb225d93d5f24237a4a6c50063dcc4a81e2b0e285b262cd3c7355e1fea0da7`.

- [ ] **Step 4: The rehearsal script; old cluster, traffic, baseline.** Create `$R/rehearsal.sh` (mode 755):

```bash
#!/usr/bin/env bash
# Stop-all Arbiter cutover rehearsal (sub-project 5b). Throwaway: everything
# lives under $R. Binaries: $R/bin/old (v0.7.1) and $R/bin/new (the release).
# Usage: rehearsal.sh chain | start <old|new> | stop | restart <1-3> <old|new>
#        | drive <n> | probe <old|new> | backup <label> | restore <label>
#        | digest <label> <old|new>
set -euo pipefail

R="${R:?set R to the rehearsal directory}"
REH="$R/run"
BIN="$R/bin"
# anvil's deterministic dev accounts; nothing here is a real key.
DEPLOYER_KEY=ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80        # account 0
POSTER_KEY=59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d          # account 1
POSTER_ADDR=0x70997970C51812dc3A010C7d01b50e0d17dc79C8
AUTH_KEY=5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a            # account 2
AUTH_ADDR=0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC
CHAIN_ID=7892301
RPC=http://127.0.0.1:18545
# TestRehearsalSchemaRoot: network testnet-integration, table db.t (value UInt64).
SCHEMA_ROOT=0x24fb225d93d5f24237a4a6c50063dcc4a81e2b0e285b262cd3c7355e1fea0da7
GRPC=127.0.0.1:17081,127.0.0.1:17082,127.0.0.1:17083

config() { # $1 = voter 1..3. Mirrors charts/storage-integrity's arbiter.yaml.tmpl.
  local i="$1" bootstrap=false
  [ "$i" = 1 ] && bootstrap=true
  cat <<EOF
node_id: arb-$i
grpc_listen: 127.0.0.1:1708$i
consensus_updates_enabled: ${UPDATES:-false}
metrics_listen: "127.0.0.1:1909$i"
apply_timeout: 5s
raft:
  listen: 127.0.0.1:1709$i
  advertise: 127.0.0.1:1709$i
  data_dir: $REH/arb-$i/raft
  bootstrap: $bootstrap
  peers:
    - id: "arb-1"
      addr: "127.0.0.1:17091"
    - id: "arb-2"
      addr: "127.0.0.1:17092"
    - id: "arb-3"
      addr: "127.0.0.1:17093"
  election_timeout: 2s
  heartbeat_timeout: 1s
  leader_lease_timeout: 500ms
  commit_timeout: 50ms
  # devnet2 renders 120s / 8192 / 10240. Lowered so a short run leaves
  # snapshots and a trailing log on every voter.
  snapshot_interval: 5s
  snapshot_threshold: 64
  trailing_logs: 128
seal: {max_statements: 2, max_age: 2s}
ingress: {max_statement_age: 5m}
dispatch: {retry_interval: 5s}
anchor:
  backend: evm
  evm:
    rpc_url: "$RPC"
    chain_id: $CHAIN_ID
    contract_address: $(cat "$REH/anchor-contract")
    finality_mode: confirmations
    finality_confirmations: 3
    last_mergeable_confirmations: 0
    resubmit_after: 30s
    gas_bump_percent: 25
    max_fee_per_gas_gwei: 100
    rpc_timeout: 10s
authority:
  allowed_addresses: ["$AUTH_ADDR"]
genesis:
  network_id: "testnet-integration"
  schema_snapshot_id: "schema-genesis"
  executor_profile_id: "housegate-replay-mvp-v0"
  schema_root: "$SCHEMA_ROOT"
  max_writers: 1
tables:
  - table_id: "db.t"
    partition_by: ""
    columns:
      - name: value
        type: UInt64
table_registry:
  rpc_url: "http://127.0.0.1:1854$i"
  rpc_timeout: 10s
  poll_interval: 5s
  max_log_range: 10000
  cursor_advance_interval: 1m
  deploy_block: 0
  watcher:
    enabled: false
  audit:
    enabled: false
EOF
}

launch() { # $1 = voter, $2 = old|new
  mkdir -p "$REH/arb-$1"
  config "$1" >"$REH/arb-$1/arbiter.yaml"
  ARBITER_AUTHORITY_PRIVATE_KEY_HEX=$AUTH_KEY ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX=$POSTER_KEY \
    nohup "$BIN/$2/arbiter" -config "$REH/arb-$1/arbiter.yaml" >>"$REH/arb-$1/$2.log" 2>&1 &
  echo $! >"$REH/arb-$1/pid"
}

halt() { # $1 = voter
  kill -TERM "$(cat "$REH/arb-$1/pid")" 2>/dev/null || true
  while kill -0 "$(cat "$REH/arb-$1/pid")" 2>/dev/null; do sleep 0.5; done
}

case "${1:-}" in
chain)
  mkdir -p "$REH"
  nohup anvil --port 18545 --chain-id "$CHAIN_ID" --block-time 1 >"$REH/anvil.log" 2>&1 &
  echo $! >"$REH/anvil.pid"
  sleep 2
  ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX=$DEPLOYER_KEY "$BIN/old/arbiter-anchor" deploy \
    -rpc-url "$RPC" -chain-id "$CHAIN_ID" -poster "$POSTER_ADDR" -expect-nonce 0 -wait 1m >"$REH/deploy.log"
  sed -n 's/^contract address: //p' "$REH/deploy.log" >"$REH/anchor-contract"
  echo "AnchorRegistry $(cat "$REH/anchor-contract")"
  ;;
start)
  for i in 1 2 3; do launch "$i" "$2"; done
  sleep 8
  for i in 1 2 3; do kill -0 "$(cat "$REH/arb-$i/pid")" 2>/dev/null && echo "arb-$i up ($2)" || echo "arb-$i EXITED ($2)"; done
  ;;
stop)
  for i in 1 2 3; do halt "$i"; echo "arb-$i down"; done
  ;;
restart)
  halt "$2"
  launch "$2" "$3"
  sleep 6
  "$BIN/$3/arbiter-admin" consensus capability --address "127.0.0.1:1708$2"
  ;;
drive)
  REHEARSAL_GRPC=$GRPC REHEARSAL_STATEMENTS="$2" REHEARSAL_METRICS=http://127.0.0.1:19091/metrics \
    REHEARSAL_INVENTORY="$REH/inventory.json" \
    "$BIN/driver.test" -test.run 'TestRehearsalDrive$' -test.v -test.timeout 10m 2>&1 |
    grep -E '^(DRIVE|--- |FAIL)|Error' || true
  ;;
probe)
  for i in 1 2 3; do
    "$BIN/$2/arbiter-admin" consensus capability --address "127.0.0.1:1708$i" | tr -d '\n '; echo
    curl -s "http://127.0.0.1:1909$i/metrics" |
      grep -E '^arbiter_(safe_watermark_block_seq|sealed_block_seq|open_block_statements|nodes\{role="(snode|verifier)",status="active"\})' | tr '\n' ' '; echo
  done
  for i in 1 2 3; do
    "$BIN/$2/arbiter-admin" consensus show --address "127.0.0.1:1708$i" 2>/dev/null |
      jq -c '{network_id, genesis_snapshot_id, epoch, params_digest, promotion_seq, authority: .current.authority_addresses, max_writers: .current.max_writers}' && break
  done
  ;;
backup)
  for i in 1 2 3; do
    tar -C "$REH/arb-$i" -czf "$REH/arb-$i-raft-$2.tgz" raft
    echo "arb-$i $(du -h "$REH/arb-$i-raft-$2.tgz" | cut -f1) $(shasum -a 256 "$REH/arb-$i-raft-$2.tgz" | cut -c1-16)"
  done
  cp "$REH/inventory.json" "$REH/inventory-$2.json"
  ;;
restore)
  for i in 1 2 3; do
    rm -rf "$REH/arb-$i/raft"
    tar -C "$REH/arb-$i" -xzf "$REH/arb-$i-raft-$2.tgz"
  done
  cp "$REH/inventory-$2.json" "$REH/inventory.json"
  echo "restored $2"
  ;;
digest)
  for i in 1 2 3; do
    work="$(mktemp -d)"
    tar -C "$work" -xzf "$REH/arb-$i-raft-$2.tgz"
    "$BIN/$3/statedigest" -data-dir "$work/raft" -network-id testnet-integration \
      -schema-snapshot-id schema-genesis -executor-profile-id housegate-replay-mvp-v0 \
      -max-writers 1 -authority "$(printf '%s' "$AUTH_ADDR" | tr 'A-F' 'a-f')" \
      >"$REH/digest-$2-$3-arb-$i.txt" 2>/dev/null
    rm -rf "$work"
    grep -E '^(snapshot_index|last_index|written_version)' "$REH/digest-$2-$3-arb-$i.txt" | tr '\n' ' '
    echo "arb-$i $3 state=$(grep -E '^(key|spent_ids)' "$REH/digest-$2-$3-arb-$i.txt" | grep -v '^key artifact_disposition ' | shasum -a 256 | cut -c1-16)"
  done
  ;;
*)
  sed -n '2,7p' "$0" >&2
  exit 2
  ;;
esac
```

```bash
export R
cd "$R"
./rehearsal.sh chain
./rehearsal.sh start old
./rehearsal.sh drive 40
./rehearsal.sh drive 200
./rehearsal.sh drive 4
./rehearsal.sh probe old | tee run/baseline.txt
grep -h 'election won' run/arb-*/old.log | tail -1
```

Expected (measured with `cf38430` standing in for the tag): `AnchorRegistry 0x5FbDB2315678afecb367f032d93F642f64180aa3`; three `up (old)` lines although every config carries a `table_registry` block; three `--- PASS: TestRehearsalDrive` runs (safe 0→14→59→61 in the recorded run; block counts vary); every voter reports the same `arbiter_safe_watermark_block_seq` and `arbiter_sealed_block_seq`, `open_block_statements 0`, one active SNode, three active verifiers, `updates_enabled:false`; one `election won` line.

- [ ] **Step 5: Stop all, back up, replay offline with both builds.**

```bash
./rehearsal.sh stop
for i in 1 2 3; do ls run/arb-$i/raft/snapshots; shasum -a 256 run/arb-$i/raft/consensus-protocol.json | cut -c1-16; done
./rehearsal.sh backup pre
./rehearsal.sh digest pre old
./rehearsal.sh digest pre new
```

Expected: each voter has two snapshot directories and the same marker hash; three backups (112 MB each in the recorded run); the six digest lines share one `state=` value, with `written_version 4` for `old` and `14` for `new` (recorded: `state=83ec6c7238cb784e` on snapshot indexes 1964/1977/1964, last index 1978). A differing `state=` is a stop: it would mean the release replays v0.7.1 state differently.

- [ ] **Step 6: Start the release, compare, drive the old data plane, rehearse stage 2b.**

```bash
./rehearsal.sh start new
grep -h -E 'starting restore|restored from snapshot|level=ERROR' run/arb-*/new.log
for i in 1 2 3; do shasum -a 256 run/arb-$i/raft/consensus-protocol.json | cut -c1-16; done
./rehearsal.sh probe new > run/after-cutover.txt
diff run/baseline.txt run/after-cutover.txt && echo "BASELINE == AFTER CUTOVER"
./rehearsal.sh drive 40
export UPDATES=true
for i in 2 3 1; do ./rehearsal.sh restart $i new | jq -c .; done
./rehearsal.sh drive 4
unset UPDATES
```

Expected: each voter logs `restored from snapshot` for its newest snapshot (about 120 ms for 40 MB) and no `level=ERROR`; marker hashes unchanged; `BASELINE == AFTER CUTOVER`; the old-protocol driver passes (safe 61→73 recorded); each restarted voter reports `"updates_enabled":true` and the last drive passes.

- [ ] **Step 7: The old build refuses a directory the release wrote.**

```bash
./rehearsal.sh stop
./rehearsal.sh start old
grep -h 'unsupported snapshot version' run/arb-*/old.log | tail -1
```

Expected: three `EXITED (old)` lines and `error="unsupported snapshot version 14 (require 3 or 4) …"`.

- [ ] **Step 8: Roll back from the backups and resume.**

```bash
./rehearsal.sh restore pre
./rehearsal.sh start old
./rehearsal.sh probe old > run/after-rollback.txt
diff run/baseline.txt run/after-rollback.txt && echo "BASELINE == AFTER ROLLBACK"
./rehearsal.sh drive 20
./rehearsal.sh stop
kill "$(cat run/anvil.pid)"
```

Expected: `BASELINE == AFTER ROLLBACK` and a passing drive (61→70 recorded). Blocks the release sealed are lost and stay anchored as orphans; the restored network anchors its new blocks normally.

- [ ] **Step 9: Load the chart's rendered devnet2 configs with both builds.** Needs Task 6 on the production branch (run it then; keep `$R`):

```bash
PROD="$HOME/src/production-si-5b"
helm template storage-integrity-si-v2 "$PROD/charts/storage-integrity" --namespace sentio-network-devnet2 \
  -f "$PROD/k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml" > "$R/run/si-v2-control.yaml"
python3 - "$R/run/si-v2-control.yaml" "$R/run/rendered" <<'PY'
import os, pathlib, subprocess, sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
cm = next(d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "storage-integrity-si-v2-arbiter-config")
sts = next(d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "storage-integrity-si-v2-arbiter")
script = sts["spec"]["template"]["spec"]["initContainers"][0]["args"][0]
for i in range(3):
    root = pathlib.Path(sys.argv[2]) / str(i)
    (root / "template").mkdir(parents=True, exist_ok=True)
    (root / "config").mkdir(exist_ok=True)
    (root / "template/arbiter.yaml.tmpl").write_text(cm["data"]["arbiter.yaml.tmpl"])
    body = script.replace("/template/", f"{root}/template/").replace("/config/", f"{root}/config/")
    env = dict(os.environ, HOSTNAME=f"storage-integrity-si-v2-arbiter-{i}", ANCHOR_CONTRACT_ADDRESS="0x" + "1" * 40)
    subprocess.run(["/bin/sh", "-ec", body], env=env, check=True)
PY
for i in 0 1 2; do for v in old new; do
  printf '%s %s ' "$i" "$v"
  env ARBITER_AUTHORITY_PRIVATE_KEY_HEX=5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a \
    ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX=59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d \
    "$R/bin/$v/cfgcheck" "$R/run/rendered/$i/config/arbiter.yaml" |
    jq -c '{node: .NodeID, registry: (.TableRegistry // "absent" | if type=="object" then {rpc: .RPCURL, watcher: .Watcher.Enabled, audit: .Audit.Enabled} else . end)}'
done; done
for v in old new; do
  python3 -c "import sys,yaml; d=[x for x in yaml.safe_load_all(open(sys.argv[1])) if x]; print(next(x for x in d if x['metadata']['name']=='storage-integrity-si-v2-bootstrap-config')['data']['schema.yaml'])" "$R/run/si-v2-control.yaml" > "$R/run/schema.yaml"
  echo "$v $("$R/bin/$v/arbiter-verifier" -config "$R/run/schema.yaml" -print-schema-root 2>/dev/null | tail -1)"
done
```

The two private keys are anvil's public development keys, needed only because `config.Load` validates key presence. Expected:

```
0 old {"node":"storage-integrity-si-v2-arbiter-0","registry":"absent"}
0 new {"node":"storage-integrity-si-v2-arbiter-0","registry":{"rpc":"http://sentio-node-op-reth-0:8545","watcher":false,"audit":false}}
1 old {"node":"storage-integrity-si-v2-arbiter-1","registry":"absent"}
1 new {"node":"storage-integrity-si-v2-arbiter-1","registry":{"rpc":"http://sentio-node-op-reth-1:8545","watcher":false,"audit":false}}
2 old {"node":"storage-integrity-si-v2-arbiter-2","registry":"absent"}
2 new {"node":"storage-integrity-si-v2-arbiter-2","registry":{"rpc":"http://sentio-node-op-reth-0:8545","watcher":false,"audit":false}}
old 0x4604a800560a07d08516b13b7084e8ee00436055fdc8698912f8b60937eb2732
new 0x4604a800560a07d08516b13b7084e8ee00436055fdc8698912f8b60937eb2732
```

The last two lines are the bootstrap hook's `-print-schema-root`, which the stage 1 sync reruns with the release image. Record Steps 4–9 in Task 13. Keep `$R` until Task 10 is verified.

## Task 4: `charts/sentio-node` — the registry-driven table-set shape

**Files:**

- Modify: `charts/sentio-node/templates/node.yaml:1` (new variables), `:5-7` (the `tables` guard), `:35` (the equality guard), `:70-72` (sidecar schema guard, plus a new block after it), `:228-230` (the `tables` rendering), `:327-337` (the agent network-state key)
- Modify: `charts/storage-integrity/tests/validate_source_binding.py:102-103`, `:149-152`
- Test: `charts/storage-integrity/tests/test_render.py:533-604` (`render_source` split into `render_source` and `source_values`), two tests before `:732`, one test before `:923`

**Interfaces:**

- Produces values semantics: `housegate.storageIntegrity.tables` empty → rendered `housegate.storage_integrity.enabled: true` (requires `storageIntegrity.enabled`); non-empty → rendered as today and must equal `storageIntegrity.snode.tableIDs`. `housegateSidecar.storageIntegrity.networkStateSource` matching `^https?://` → no `housegate-agent-network-state.yaml` key, `tableSchemas` must be empty, and a `localhost`/`127.0.0.1` host must use `node.ports.storage_rpc` and needs `storageIntegrity.enabled`.
- Produces `FreshSourceBindingTest.source_values(control, namespace, *, name, qualified, registry)`; `validate_binding` accepts the registry shape.

Work in a new production worktree; every production command of Tasks 4–8 runs from it (line numbers below are `origin/main`'s):

```bash
git -C /Users/uranuswch/Dev/sentio_xyz/production fetch origin
git -C /Users/uranuswch/Dev/sentio_xyz/production worktree add -b dev/poetry/si-dynamic-table-set-devnet2 "$HOME/src/production-si-5b" origin/main
cd "$HOME/src/production-si-5b"
```

- [ ] **Step 1: Baseline renders.** Save this as `$R/render-all.sh` (mode 755) and render `origin/main` before any edit:

```bash
#!/usr/bin/env bash
# Render every helmfile release that uses charts/sentio-node or
# charts/storage-integrity. $1 = production checkout, $2 = output directory.
set -euo pipefail
repo="$1" out="$2"
mkdir -p "$out"
render() { # $1 = directory, $2 = helmfile, $3 = release
  local name="$(basename "$1")--${2%.yaml}--$3"
  (cd "$repo/k8s-sea/$1" && helmfile -f "$2" -l "name=$3" template --skip-deps >"$out/$name.yaml" 2>"$out/$name.err") ||
    { echo "FAILED $name"; cat "$out/$name.err"; return 1; }
  printf '%-90s %s\n' "$name" "$(wc -c <"$out/$name.yaml")"
}
d=sentio-network-devnet2
render $d storage-integrity-si-v2.helmfile.yaml storage-integrity-si-v2
render $d storage-integrity-si-v2.helmfile.yaml sentio-node-devnet2-indexer-a
render $d storage-integrity.helmfile.yaml storage-integrity
render $d storage-integrity.helmfile.yaml sentio-node-devnet2-indexer-a
render $d helmfile.yaml sentio-node-devnet2-indexer-a
render $d helmfile.yaml sentio-node-devnet2-indexer-b
render $d helmfile.yaml sentio-node-devnet2-observer
t=sentio-network-testnet-v2
render $t helmfile.yaml sentio-node-testnet-v2-indexer-a
render $t helmfile.yaml sentio-node-testnet-v2-indexer-b
render $t helmfile.yaml sentio-node-observer
```

```bash
"$R/render-all.sh" "$PWD" "$R/renders/base"
```

Expected: ten lines, each naming a release with a byte count (on `739619f06`: 32096, 28632, 31005, 28274, 23019, 23359, 9819, 23278, 23361, 9577).

- [ ] **Step 2: Write the failing tests.** In `charts/storage-integrity/tests/test_render.py`, in `FreshSourceBindingTest`, replace:

```python
    @classmethod
    def render_source(cls, control, namespace, *, name="storage-integrity", qualified=False):
        arbiter = arbiter_config(control, name)
```

with:

```python
    @classmethod
    def render_source(cls, control, namespace, *, name="storage-integrity", qualified=False, registry=False):
        values = cls.source_values(control, namespace, name=name, qualified=qualified, registry=registry)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.yaml"
            path.write_text(yaml.safe_dump(values))
            result = subprocess.run([
                "helm", "template", "fresh-source", str(ROOT / "charts/sentio-node"),
                "--namespace", namespace, "-f", str(path),
            ], capture_output=True, text=True, check=False)
        return documents(result)

    @classmethod
    def source_values(cls, control, namespace, *, name="storage-integrity", qualified=False, registry=False):
        arbiter = arbiter_config(control, name)
```

In the same method, replace:

```python
                "payloadSpoolDir": f"{state}/housegate-spool",
            },
        }}
        values["housegateSidecar"] = {"storageIntegrity": {
```

with:

```python
                "payloadSpoolDir": f"{state}/housegate-spool",
            },
        }}
        if registry:
            # sentio-node 184bbf6+: the table set comes from the embedded SNode.
            del values["housegate"]["storageIntegrity"]["tables"]
        values["housegateSidecar"] = {"storageIntegrity": {
```

and replace its tail:

```python
                "schemaJSON": '{"table_id":"devnet101.swap_new2","partition_by":"","columns":[{"name":"value","type":"Int64"}]}',
            }],
        }}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.yaml"
            path.write_text(yaml.safe_dump(values))
            result = subprocess.run([
                "helm", "template", "fresh-source", str(ROOT / "charts/sentio-node"),
                "--namespace", namespace, "-f", str(path),
            ], capture_output=True, text=True, check=False)
        return documents(result)
```

with:

```python
                "schemaJSON": '{"table_id":"devnet101.swap_new2","partition_by":"","columns":[{"name":"value","type":"Int64"}]}',
            }],
        }}
        if registry:
            port = yaml.safe_load((ROOT / "charts/sentio-node/values.yaml").read_text())["node"]["ports"]["storage_rpc"]
            values["housegateSidecar"]["storageIntegrity"]["networkStateSource"] = f"http://localhost:{port}"
            del values["housegateSidecar"]["storageIntegrity"]["tableSchemas"]
        return values
```

Insert immediately before `    def test_legacy_genesis_is_rejected_but_the_shared_l2_network_id_is_not(self):`:

```python
    def test_registry_driven_source_binds(self):
        source = self.render_source(self.control, RENDER_NAMESPACE, registry=True)
        self.assertEqual(self.validate(source=source), FRESH_SOURCE_IMAGE)
        host = yaml.safe_load(next(
            doc for doc in source.values() if "standalone.yaml" in doc.get("data", {})
        )["data"]["standalone.yaml"])
        self.assertIs(host["housegate"]["storage_integrity"]["enabled"], True)
        self.assertNotIn("tables", host["housegate"]["storage_integrity"])
        self.assertFalse(any("housegate-agent-network-state.yaml" in doc.get("data", {}) for doc in source.values()))
        for url in ("http://localhost:1", "http://storage-rpc.elsewhere:10002"):
            with self.subTest(url=url), self.assertRaisesRegex(ValueError, "agent network state source"):
                self.validate(source=with_config(source, "housegate-agent.yaml", set_path(("network_state", "source"), url)))
        with self.assertRaisesRegex(ValueError, "must be enabled"):
            self.validate(source=with_config(
                source, "standalone.yaml", set_path(("housegate", "storage_integrity", "enabled"), False),
            ))

    def test_registry_shape_chart_guards(self):
        registry_values = self.source_values(self.control, RENDER_NAMESPACE, registry=True)

        def render_with(overrides):
            with tempfile.TemporaryDirectory() as directory:
                base = Path(directory) / "base.yaml"
                extra = Path(directory) / "extra.yaml"
                base.write_text(yaml.safe_dump(registry_values))
                extra.write_text(yaml.safe_dump(overrides))
                return subprocess.run([
                    "helm", "template", "fresh-source", str(ROOT / "charts/sentio-node"),
                    "--namespace", RENDER_NAMESPACE, "-f", str(base), "-f", str(extra),
                ], capture_output=True, text=True, check=False)
        schema = {
            "databaseID": "devnet101", "tableID": "swap_new2", "version": 1,
            "schemaHash": "0x" + "a" * 64, "schemaJSON": "{}",
        }
        for overrides, error in (
            ({"storageIntegrity": {"enabled": False}, "housegateSidecar": {"storageIntegrity": {"networkStateSource": "http://sn.example:10002"}}},
             "takes its table set from the embedded SNode"),
            ({"housegateSidecar": {"storageIntegrity": {"tableSchemas": [schema]}}}, "unused with an http(s) networkStateSource"),
            ({"housegateSidecar": {"storageIntegrity": {"networkStateSource": "http://localhost:32003"}}}, "storage RPC port 10002"),
            ({"housegateSidecar": {"storageIntegrity": {"networkStateSource": "http://127.0.0.1"}}}, "storage RPC port 10002"),
            ({"housegate": {"storageIntegrity": {"tables": ["other.table"]}}}, "must exactly match"),
        ):
            with self.subTest(error=error):
                result = render_with(overrides)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn(error, result.stderr)
        self.assertEqual(render_with({}).returncode, 0)

```

In `ActualFreshNetworkTest`, insert immediately before `    def test_actual_control_values_pass_strict_lint(self):`:

```python
    def test_registry_shape_changes_only_the_table_source(self):
        """Old/new indexer-a overlays: the registry-driven shape differs from the
        static one only in HouseGate's table source and the agent's status source."""
        storage_rpc = yaml.safe_load((DEVNET2 / "sentio-node-indexer-a.yaml").read_text())["node"]["ports"]["storage_rpc"]
        static = {
            "housegate": {"storageIntegrity": {"tables": ["devnet101.swap_new2"]}},
            "housegateSidecar": {"storageIntegrity": {
                "networkStateSource": "/config/housegate-agent-network-state.yaml",
                "tableSchemas": [{
                    "databaseID": "devnet101", "tableID": "swap_new2", "version": 1,
                    "schemaHash": DEVNET2_TABLE_SCHEMA_HASH,
                    "schemaJSON": '{"table_id":"devnet101.swap_new2","partition_by":"","columns":[{"name":"value","type":"Int64"}]}',
                }],
            }},
        }
        registry = {
            "housegate": {"storageIntegrity": {"tables": []}},
            "housegateSidecar": {"storageIntegrity": {
                "networkStateSource": f"http://localhost:{storage_rpc}", "tableSchemas": [],
            }},
        }
        rendered = {}
        for label, override in (("static", static), ("registry", registry)):
            release = dict(self.source_release)
            release["values"] = list(self.source_release["values"]) + [override]
            rendered[label] = documents(self.render_release(release))
            image_values = yaml.safe_load((FRESH_HELMFILE.parent / self.source_release["values"][-1]).read_text())
            self.assertEqual(
                validate_binding(list(self.control.values()), list(rendered[label].values()), image_values, LEGACY_NAMESPACE),
                FRESH_SOURCE_IMAGE,
            )

        def data(docs):
            return next(doc for doc in docs.values() if "standalone.yaml" in doc.get("data", {}))["data"]

        old, new = data(rendered["static"]), data(rendered["registry"])
        old_host, new_host = yaml.safe_load(old["standalone.yaml"]), yaml.safe_load(new["standalone.yaml"])
        self.assertEqual(old_host["housegate"]["storage_integrity"].pop("tables"), ["devnet101.swap_new2"])
        self.assertIs(new_host["housegate"]["storage_integrity"].pop("enabled"), True)
        self.assertEqual(new_host, old_host)
        old_agent, new_agent = yaml.safe_load(old["housegate-agent.yaml"]), yaml.safe_load(new["housegate-agent.yaml"])
        self.assertEqual(new_agent.pop("network_state"), {"source": f"http://localhost:{storage_rpc}"})
        old_agent.pop("network_state")
        self.assertEqual(new_agent, old_agent)
        self.assertIn("housegate-agent-network-state.yaml", old)
        self.assertNotIn("housegate-agent-network-state.yaml", new)

```

- [ ] **Step 3: Run them and verify they fail.** `python3 -B charts/storage-integrity/tests/test_render.py`. Expected: `Ran 51 tests`, `FAILED (failures=7)`: `test_registry_shape_changes_only_the_table_source` and `test_registry_driven_source_binds` with `Error: execution error at (sentio-node/templates/node.yaml:6:4): housegate.storageIntegrity.tables is required`, and five `test_registry_shape_chart_guards` subtests (four messages not found in that same error, and `AssertionError: 1 != 0` for the unmodified render).

- [ ] **Step 4: Change the chart and the validator.** In `charts/sentio-node/templates/node.yaml`, replace:

```
{{- $isIndexer := eq .Values.node.mode "indexer" -}}
```

with:

```
{{- $isIndexer := eq .Values.node.mode "indexer" -}}
{{- $agentSource := toString .Values.housegateSidecar.storageIntegrity.networkStateSource -}}
{{- $agentRPC := regexMatch "^https?://" $agentSource -}}
```

Replace:

```
{{- if and .Values.housegate.storageIntegrity.enabled (empty .Values.housegate.storageIntegrity.tables) -}}
{{- fail "housegate.storageIntegrity.tables is required" -}}
{{- end -}}
```

with:

```
{{- /* An empty housegate.storageIntegrity.tables selects the registry-driven
table set: sentio-node (184bbf6 and later) injects HouseGate's table state from
its embedded SNode, so that shape needs the SNode. A non-empty list is the
legacy static set (older images require it); sentio-node 184bbf6 accepts it
only when it equals storageIntegrity.snode.tableIDs. */ -}}
{{- if and .Values.housegate.storageIntegrity.enabled (empty .Values.housegate.storageIntegrity.tables) (not .Values.storageIntegrity.enabled) -}}
{{- fail "housegate.storageIntegrity without tables takes its table set from the embedded SNode: enable storageIntegrity or list the static tables" -}}
{{- end -}}
```

Replace:

```
{{- if and .Values.housegate.storageIntegrity.enabled .Values.storageIntegrity.enabled (not (deepEqual .Values.housegate.storageIntegrity.tables .Values.storageIntegrity.snode.tableIDs)) -}}
```

with:

```
{{- if and .Values.housegate.storageIntegrity.enabled .Values.storageIntegrity.enabled (not (empty .Values.housegate.storageIntegrity.tables)) (not (deepEqual .Values.housegate.storageIntegrity.tables .Values.storageIntegrity.snode.tableIDs)) -}}
```

Replace:

```
{{- if and $isIndexer .Values.housegateSidecar.storageIntegrity.enabled (empty .Values.housegateSidecar.storageIntegrity.tableSchemas) -}}
{{- fail "housegateSidecar.storageIntegrity.tableSchemas is required" -}}
{{- end -}}
```

with:

```
{{- if and $isIndexer .Values.housegateSidecar.storageIntegrity.enabled (not $agentRPC) (empty .Values.housegateSidecar.storageIntegrity.tableSchemas) -}}
{{- fail "housegateSidecar.storageIntegrity.tableSchemas is required" -}}
{{- end -}}
{{- /* With an http(s) networkStateSource the agent asks the storage-node
JSON-RPC (sentio_getStorageIntegrityTableStatus) for every table's status, so
declared schemas would be dead configuration. The co-located storage RPC listens
on 0.0.0.0:node.ports.storage_rpc in the same pod. */ -}}
{{- if and $isIndexer .Values.housegateSidecar.storageIntegrity.enabled $agentRPC -}}
{{- if not (empty .Values.housegateSidecar.storageIntegrity.tableSchemas) -}}
{{- fail "housegateSidecar.storageIntegrity.tableSchemas is unused with an http(s) networkStateSource: the storage node answers table status; remove it" -}}
{{- end -}}
{{- $agentHost := get (urlParse $agentSource) "host" -}}
{{- if or (hasPrefix "localhost" $agentHost) (hasPrefix "127.0.0.1" $agentHost) -}}
{{- if not .Values.storageIntegrity.enabled -}}
{{- fail "a localhost housegateSidecar.storageIntegrity.networkStateSource requires storageIntegrity.enabled: only a storage-integrity node answers table status" -}}
{{- end -}}
{{- if ne $agentHost (printf "%s:%v" (first (splitList ":" $agentHost)) .Values.node.ports.storage_rpc) -}}
{{- fail (printf "housegateSidecar.storageIntegrity.networkStateSource %s must use this node's storage RPC port %v" $agentSource .Values.node.ports.storage_rpc) -}}
{{- end -}}
{{- end -}}
{{- end -}}
```

Replace:

```
      storage_integrity:
        tables:
          {{- toYaml .Values.housegate.storageIntegrity.tables | nindent 10 }}
```

with:

```
      storage_integrity:
        {{- if .Values.housegate.storageIntegrity.tables }}
        tables:
          {{- toYaml .Values.housegate.storageIntegrity.tables | nindent 10 }}
        {{- else }}
        enabled: true
        {{- end }}
```

Replace:

```

  housegate-agent-network-state.yaml: |
    table_schemas:
```

with:

```

  {{- if not $agentRPC }}

  housegate-agent-network-state.yaml: |
    table_schemas:
```

and replace:

```
        schema_json: {{ .schemaJSON | quote }}
      {{- end }}
  {{- end }}
---
```

with:

```
        schema_json: {{ .schemaJSON | quote }}
      {{- end }}
  {{- end }}
  {{- end }}
---
```

In `charts/storage-integrity/tests/validate_source_binding.py`, replace:

```python
    if housegate["tables"] != snode["table_ids"]:
        raise ValueError("HouseGate tables differ from source SNode table identities")
```

with:

```python
    # An empty or absent list is the registry-driven table set: sentio-node
    # injects HouseGate's table state from the embedded SNode. A legacy list
    # must still equal the SNode's genesis tables.
    if not housegate.get("tables"):
        if housegate.get("enabled") is not True:
            raise ValueError("HouseGate storage integrity must be enabled when its table set comes from the SNode")
    elif housegate["tables"] != snode["table_ids"]:
        raise ValueError("HouseGate tables differ from source SNode table identities")
```

and replace:

```python
        schemas = config_data(source, "housegate-agent-network-state.yaml", "HouseGate agent network state")
        declared = {f'{entry["database_id"]}.{entry["table_id"]}' for entry in (schemas.get("table_schemas") or {}).values()}
        if not set(snode["table_ids"]) <= declared:
            raise ValueError("HouseGate agent table schemas do not cover the source SNode tables")
```

with:

```python
        source_url = str(agent_config["network_state"]["source"])
        if re.match(r"https?://", source_url):
            # The agent asks the co-located storage-node RPC for table status.
            port = host["node"]["storage_node_rpc_port"]
            if not re.fullmatch(rf"https?://(localhost|127\.0\.0\.1):{port}/?", source_url):
                raise ValueError("HouseGate agent network state source must be this node's storage RPC")
            if config_data(source, "housegate-agent-network-state.yaml", "HouseGate agent network state", required=False) is not None:
                raise ValueError("HouseGate agent with an RPC network state source must not render declared table schemas")
        else:
            schemas = config_data(source, "housegate-agent-network-state.yaml", "HouseGate agent network state")
            declared = {f'{entry["database_id"]}.{entry["table_id"]}' for entry in (schemas.get("table_schemas") or {}).values()}
            if not set(snode["table_ids"]) <= declared:
                raise ValueError("HouseGate agent table schemas do not cover the source SNode tables")
```

- [ ] **Step 5: Run the suite.** `python3 -B charts/storage-integrity/tests/test_render.py`. Expected: `Ran 51 tests`, `OK`.

- [ ] **Step 6: Prove the chart change is inert.**

```bash
"$R/render-all.sh" "$PWD" "$R/renders/task4"
for f in "$R"/renders/base/*.yaml; do cmp -s "$f" "$R/renders/task4/$(basename "$f")" && echo "SAME $(basename "$f")" || echo "DIFF $(basename "$f")"; done
```

Expected: ten `SAME` lines.

- [ ] **Step 7: Commit.**

```bash
git add charts/sentio-node/templates/node.yaml charts/storage-integrity/tests/validate_source_binding.py charts/storage-integrity/tests/test_render.py
git commit -F - <<'EOF'
feat(sentio-node): render the registry-driven storage-integrity table set

An empty housegate.storageIntegrity.tables renders enabled: true (the embedded
SNode supplies the table set); an http(s) sidecar networkStateSource drops the
declared schemas and must name this node's storage RPC port. Today's shape
renders byte-identically.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 5: `charts/storage-integrity` — an optional per-voter `table_registry` section

**Files:**

- Modify: `charts/storage-integrity/values.yaml:138-139` (after `finalityConfirmations`), `values.schema.json:53-57` (after `metrics`), `templates/arbiter.yaml:64-66` (after the `tables` block), `:176-182` (init container), `templates/_helpers.tpl:59-61` (checksum), `templates/validate.yaml:108` (before the signing guard)
- Test: `charts/storage-integrity/tests/test_render.py:6` (import), `:23` (constant), before `:83` (helper), before `:395` (`TableRegistryTest`)

**Interfaces:**

- Produces values `arbiter.tableRegistry.{rpcURLs[], rpcTimeout, pollInterval, maxLogRange, cursorAdvanceInterval, deployBlock, watcher.enabled, audit.enabled}` (defaults `[]`, `10s`, `5s`, `10000`, `1m`, `0`, `false`, `false`), rendered as the Arbiter `table_registry:` section only when `rpcURLs` is set, one URL per voter ordinal. Guards: `pinned-genesis-v1` only; exactly `arbiter.replicas` URLs of shape `http(s)://host[:port]`; at least two distinct; audit/watcher need explicit URLs; watcher needs audit; audit/watcher refused on either v0.7.1 digest.
- Produces the test helper `init_rendered_arbiter_config(docs, ordinal, name)`, which executes the rendered init script.

- [ ] **Step 1: Write the failing tests.** In `test_render.py`, replace `import json\nimport re\n` with `import json\nimport os\nimport re\n`; after the `HARDENED_DIGEST = …` line add:

```python
V071_AMD64_DIGEST = "sha256:b36e44407ae9d5c356bf1188940a36bcb83cc221e3c920ff83f6bc0774da8ac4"
```

Insert immediately before `def config_for(docs, role, name="storage-integrity"):`:

```python
def init_rendered_arbiter_config(docs, ordinal, name="storage-integrity"):
    """Run the Arbiter render-config init container for one voter ordinal."""
    config_map = docs[("ConfigMap", f"{name}-arbiter-config")]
    script = pod_template(docs, "arbiter", name)["spec"]["initContainers"][0]["args"][0]
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "template").mkdir()
        (root / "config").mkdir()
        (root / "template/arbiter.yaml.tmpl").write_text(config_map["data"]["arbiter.yaml.tmpl"])
        script = script.replace("/template/", f"{root}/template/").replace("/config/", f"{root}/config/")
        env = {
            "PATH": os.environ["PATH"],
            "HOSTNAME": f"{name}-arbiter-{ordinal}",
            "ANCHOR_CONTRACT_ADDRESS": "0x" + "1" * 40,
        }
        subprocess.run(["/bin/sh", "-ec", script], env=env, check=True, capture_output=True, text=True)
        return yaml.safe_load((root / "config/arbiter.yaml").read_text())


```

Insert immediately before `class LegacyCompatibilityTest(unittest.TestCase):`:

```python
class TableRegistryTest(unittest.TestCase):
    """The optional table_registry section of the dynamic SI table set."""

    URLS = ["http://el-a:8545", "http://el-b:8545", "http://el-a:8545"]

    @classmethod
    def setUpClass(cls):
        cls.docs = documents(render())
        cls.registry = documents(render({"arbiter": {"tableRegistry": {"rpcURLs": cls.URLS}}}))

    def assert_rejected(self, overrides, error):
        result = render(overrides)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(error, result.stderr)

    def test_default_renders_no_table_registry(self):
        self.assertNotIn("table_registry", arbiter_config(self.docs))
        script = pod_template(self.docs)["spec"]["initContainers"][0]["args"][0]
        self.assertNotIn("TABLE_REGISTRY", script)
        self.assertNotIn("table_registry", init_rendered_arbiter_config(self.docs, 0))

    def test_each_voter_renders_its_own_rpc_url_with_watcher_and_audit_off(self):
        for ordinal, url in enumerate(self.URLS):
            with self.subTest(ordinal=ordinal):
                config = init_rendered_arbiter_config(self.registry, ordinal)
                self.assertEqual(config["node_id"], f"storage-integrity-arbiter-{ordinal}")
                self.assertEqual(config["table_registry"], {
                    "rpc_url": url, "rpc_timeout": "10s", "poll_interval": "5s",
                    "max_log_range": 10000, "cursor_advance_interval": "1m", "deploy_block": 0,
                    "watcher": {"enabled": False}, "audit": {"enabled": False},
                })

    def test_registry_changes_only_the_arbiter_config(self):
        before = arbiter_config(self.docs)
        after = arbiter_config(self.registry)
        after.pop("table_registry")
        self.assertEqual(after, before)
        for role in ("arbiter", "verifier"):
            old = pod_template(self.docs, role)["metadata"]["annotations"]["checksum/config"]
            new = pod_template(self.registry, role)["metadata"]["annotations"]["checksum/config"]
            if role == "arbiter":
                self.assertNotEqual(old, new)
            else:
                self.assertEqual(old, new)
        audited = documents(render({"arbiter": {
            "image": {"digest": "sha256:" + "c" * 64},
            "tableRegistry": {"rpcURLs": self.URLS, "audit": {"enabled": True}},
        }}))
        self.assertNotEqual(
            pod_template(audited)["metadata"]["annotations"]["checksum/config"],
            pod_template(self.registry)["metadata"]["annotations"]["checksum/config"],
        )

    def test_invalid_registry_settings_fail_closed(self):
        urls = self.URLS
        for overrides, error in (
            ({"rpcURLs": urls[:2]}, "exactly one URL per Arbiter voter ordinal"),
            ({"rpcURLs": ["http://el-a:8545"] * 3}, "at least two archive nodes"),
            ({"rpcURLs": [urls[0], urls[1], "http://el-c:8545/path"]}, "rpcURLs"),
            ({"rpcURLs": [urls[0], urls[1], "http://a|b:8545"]}, "rpcURLs"),
            ({"rpcURLs": urls, "watcher": {"enabled": True}}, "requires audit.enabled"),
            ({"audit": {"enabled": True}}, "require explicit rpcURLs"),
            ({"rpcURLs": urls, "deployBlock": -1}, "deployBlock"),
            ({"rpcURLs": urls, "audit": {"enabled": "true"}}, "enabled"),
        ):
            with self.subTest(overrides=overrides):
                self.assert_rejected({"arbiter": {
                    "image": {"digest": "sha256:" + "c" * 64}, "tableRegistry": overrides,
                }}, error)

    def test_enabled_registry_refuses_the_runtime_that_ignores_it(self):
        for digest in (HARDENED_DIGEST, V071_AMD64_DIGEST):
            for block in ("audit", "watcher"):
                with self.subTest(digest=digest, block=block):
                    self.assert_rejected({"arbiter": {"image": {"digest": digest}, "tableRegistry": {
                        "rpcURLs": self.URLS, "audit": {"enabled": True}, block: {"enabled": True},
                    }}}, "v0.7.1 ignores table_registry")
        # Rendering the inert block on v0.7.1 is allowed: that is devnet2 until stage 1.
        documents(render({"arbiter": {"image": {"digest": HARDENED_DIGEST}, "tableRegistry": {"rpcURLs": self.URLS}}}))

    def test_legacy_profile_rejects_a_table_registry(self):
        result = render_legacy({"arbiter": {"tableRegistry": {"rpcURLs": self.URLS}}})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires pinned-genesis-v1", result.stderr)


```

- [ ] **Step 2: Run them and verify they fail.** `python3 -B charts/storage-integrity/tests/test_render.py`. Expected: `Ran 57 tests`, `FAILED (failures=13, errors=4)`: `KeyError: 'table_registry'` from the three per-ordinal subtests and `test_registry_changes_only_the_arbiter_config`; `AssertionError: 0 == 0` (the render succeeded) for the eight invalid-settings subtests, the four v0.7.1 subtests and `test_legacy_profile_rejects_a_table_registry`.

- [ ] **Step 3: Change the chart.** In `values.yaml`, replace:

```yaml
    finalityConfirmations: 3
  storageClassName: local-path
```

with:

```yaml
    finalityConfirmations: 3
  # Optional table_registry section of the dynamic SI table set (arbiter
  # v0.8.0 and later; v0.7.1 ignores the key). Rendered only when rpcURLs is
  # set: one L2 JSON-RPC URL per voter ordinal, each a single archive node that
  # answers historical eth_call, never a load-balanced Service. The Databases
  # contract, chain, SI indexer and activation block are consensus parameters,
  # not configuration. Keep audit and watcher off until the registry is enabled
  # (docs/storage-integrity-devnet2-dynamic-table-set.md, stage 3).
  tableRegistry:
    rpcURLs: []
    rpcTimeout: 10s
    pollInterval: 5s
    maxLogRange: 10000
    cursorAdvanceInterval: 1m
    deployBlock: 0
    watcher:
      enabled: false
    audit:
      enabled: false
  storageClassName: local-path
```

In `values.schema.json`, replace:

```json
              "description": "Pod metrics port; must not collide with gRPC or Raft."
            }
          }
        }
      }
```

with:

```json
              "description": "Pod metrics port; must not collide with gRPC or Raft."
            }
          }
        },
        "tableRegistry": {
          "type": "object",
          "required": ["rpcURLs", "rpcTimeout", "pollInterval", "maxLogRange", "cursorAdvanceInterval", "deployBlock", "watcher", "audit"],
          "properties": {
            "rpcURLs": {
              "type": "array",
              "items": {"type": "string", "pattern": "^https?://[A-Za-z0-9.-]+(:[0-9]+)?$"},
              "description": "One single-archive-node L2 JSON-RPC URL per voter ordinal; never a load balancer."
            },
            "rpcTimeout": {"type": "string", "pattern": "^[0-9]+(ms|s|m|h)$"},
            "pollInterval": {"type": "string", "pattern": "^[0-9]+(ms|s|m|h)$"},
            "maxLogRange": {"type": "integer", "minimum": 1},
            "cursorAdvanceInterval": {"type": "string", "pattern": "^[0-9]+(ms|s|m|h)$"},
            "deployBlock": {"type": "integer", "minimum": 0},
            "watcher": {"type": "object", "required": ["enabled"], "properties": {"enabled": {"type": "boolean"}}},
            "audit": {"type": "object", "required": ["enabled"], "properties": {"enabled": {"type": "boolean"}}}
          }
        }
      }
```

In `templates/arbiter.yaml`, replace:

```
    tables:
{{ include "storage-integrity.tables" . | indent 6 }}
    {{- end }}
---
```

with:

```
    tables:
{{ include "storage-integrity.tables" . | indent 6 }}
    {{- end }}
    {{- with .Values.arbiter.tableRegistry }}
    {{- if .rpcURLs }}
    table_registry:
      rpc_url: "__TABLE_REGISTRY_RPC_URL__"
      rpc_timeout: {{ .rpcTimeout }}
      poll_interval: {{ .pollInterval }}
      max_log_range: {{ int64 .maxLogRange }}
      cursor_advance_interval: {{ .cursorAdvanceInterval }}
      deploy_block: {{ int64 .deployBlock }}
      watcher:
        enabled: {{ .watcher.enabled }}
      audit:
        enabled: {{ .audit.enabled }}
    {{- end }}
    {{- end }}
---
```

and replace:

```
              advertise="${HOSTNAME}.{{ include "storage-integrity.fullname" . }}-arbiter-raft.{{ .Release.Namespace }}.svc.cluster.local:7090"
              sed \
                -e "s/__NODE_ID__/${HOSTNAME}/g" \
                -e "s|__RAFT_ADVERTISE__|${advertise}|g" \
                -e "s/__BOOTSTRAP__/${bootstrap}/g" \
                -e "s/__ANCHOR_CONTRACT__/${ANCHOR_CONTRACT_ADDRESS}/g" \
                /template/arbiter.yaml.tmpl > /config/arbiter.yaml
```

with:

```
              advertise="${HOSTNAME}.{{ include "storage-integrity.fullname" . }}-arbiter-raft.{{ .Release.Namespace }}.svc.cluster.local:7090"
              {{- with .Values.arbiter.tableRegistry.rpcURLs }}
              # Each voter reads the registry's L2 from its own archive node.
              case "$ordinal" in
              {{- range $i, $url := . }}
                {{ $i }}) table_registry_rpc_url={{ $url | squote }} ;;
              {{- end }}
                *) echo "no arbiter.tableRegistry.rpcURLs entry for ordinal $ordinal" >&2; exit 1 ;;
              esac
              {{- end }}
              sed \
                -e "s/__NODE_ID__/${HOSTNAME}/g" \
                -e "s|__RAFT_ADVERTISE__|${advertise}|g" \
                -e "s/__BOOTSTRAP__/${bootstrap}/g" \
                -e "s/__ANCHOR_CONTRACT__/${ANCHOR_CONTRACT_ADDRESS}/g" \
                {{- if .Values.arbiter.tableRegistry.rpcURLs }}
                -e "s|__TABLE_REGISTRY_RPC_URL__|${table_registry_rpc_url}|g" \
                {{- end }}
                /template/arbiter.yaml.tmpl > /config/arbiter.yaml
```

In `templates/_helpers.tpl`, replace:

```
{{- $_ := set $config "consensusUpdatesEnabled" .Values.arbiter.consensusUpdatesEnabled -}}
{{- end -}}
```

with:

```
{{- $_ := set $config "consensusUpdatesEnabled" .Values.arbiter.consensusUpdatesEnabled -}}
{{- end -}}
{{- /* Only a rendered table_registry section enters the checksum, so charts
without one keep their existing checksum. */ -}}
{{- if .Values.arbiter.tableRegistry.rpcURLs -}}
{{- $_ := set $config "tableRegistry" .Values.arbiter.tableRegistry -}}
{{- end -}}
```

In `templates/validate.yaml`, insert immediately before `{{- if and .Values.arbiter.authoritySigningEnabled (empty .Values.arbiter.authoritySecret.name) }}`:

```
{{- /* The table registry section. v0.7.1 parses Arbiter YAML without
rejecting unknown keys, so a table_registry block on that image is silently
ignored: rendering the block early is harmless, but an enabled auditor or
watcher on it would be a no-op that looks active. */ -}}
{{- with .Values.arbiter.tableRegistry }}
{{- if .rpcURLs }}
{{- if eq $.Values.arbiter.protocol "legacy" }}
{{- fail "arbiter.tableRegistry requires pinned-genesis-v1: the legacy v0.6.1 runtime has no table registry" }}
{{- end }}
{{- if ne (len .rpcURLs) (int $.Values.arbiter.replicas) }}
{{- fail "arbiter.tableRegistry.rpcURLs needs exactly one URL per Arbiter voter ordinal" }}
{{- end }}
{{- range .rpcURLs }}
{{- if not (regexMatch "^https?://[A-Za-z0-9.-]+(:[0-9]+)?$" (toString .)) }}
{{- fail "arbiter.tableRegistry.rpcURLs entries must be http(s)://host[:port] of one archive node" }}
{{- end }}
{{- end }}
{{- if lt (len (uniq .rpcURLs)) 2 }}
{{- fail "arbiter.tableRegistry.rpcURLs must name at least two archive nodes so the voters' auditors check independently" }}
{{- end }}
{{- end }}
{{- if and (or .watcher.enabled .audit.enabled) (empty .rpcURLs) }}
{{- fail "arbiter.tableRegistry audit/watcher require explicit rpcURLs: the fallback, anchor.rpcURL, may be a load balancer" }}
{{- end }}
{{- if and .watcher.enabled (not .audit.enabled) }}
{{- fail "arbiter.tableRegistry.watcher.enabled requires audit.enabled: enable every auditor first (arbiter README, Rollout P11(e))" }}
{{- end }}
{{- if and (or .watcher.enabled .audit.enabled) (has $.Values.arbiter.image.digest (list "sha256:0a83dded9bea0b5ab7b58f48a131fbc7f5a1427040579790fd4ddca75f6af32c" "sha256:b36e44407ae9d5c356bf1188940a36bcb83cc221e3c920ff83f6bc0774da8ac4")) }}
{{- fail "arbiter.tableRegistry audit/watcher require a table-registry Arbiter release: v0.7.1 ignores table_registry" }}
{{- end }}
{{- end }}
```

- [ ] **Step 4: Run the suite and the CI lint profiles.**

```bash
python3 -B charts/storage-integrity/tests/test_render.py
helm lint charts/storage-integrity -f k8s-sea/sentio-network-devnet2/storage-integrity.yaml --namespace sentio-network-devnet2 --strict
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --strict
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --set arbiter.consensusUpdatesEnabled=true --set arbiter.authoritySigningEnabled=false --strict
```

Expected: `Ran 57 tests`, `OK`; each lint `1 chart(s) linted, 0 chart(s) failed`.

- [ ] **Step 5: Prove the chart change is still inert.** Rerun Task 4 Step 6 into `$R/renders/task5`. Expected: ten `SAME` lines.

- [ ] **Step 6: Commit.**

```bash
git add charts/storage-integrity/values.yaml charts/storage-integrity/values.schema.json charts/storage-integrity/templates/arbiter.yaml charts/storage-integrity/templates/_helpers.tpl charts/storage-integrity/templates/validate.yaml charts/storage-integrity/tests/test_render.py
git commit -F - <<'EOF'
feat(storage-integrity): render an optional per-voter table_registry section

arbiter.tableRegistry.rpcURLs gives each Arbiter voter ordinal its own archive
node; the render-config init container substitutes it. Watcher and audit
default off and are refused on arbiter v0.7.1, which ignores the section.
Charts without rpcURLs render and checksum exactly as before.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 6: devnet2 overlay — render the inert registry section and name the stage pins

**Files:**

- Modify: `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml:89-91` (after `finalityConfirmations: 3`)
- Test: `charts/storage-integrity/tests/test_render.py` constants after `V071_AMD64_DIGEST`, `:826-827`, `:873-878`, `:900`, and a new test before `test_registry_shape_changes_only_the_table_source`

**Interfaces:** Produces the constants `DEVNET2_ARBITER_DIGEST`, `DEVNET2_SIDECAR_VERSION`, `DEVNET2_CONSENSUS_UPDATES_ENABLED`, which Tasks 10–12 change beside their values.

- [ ] **Step 1: Write the failing tests.** After the `V071_AMD64_DIGEST = …` line add:

```python
# The Arbiter runtime devnet2-si-v2 runs. Stage 1 of
# docs/storage-integrity-devnet2-dynamic-table-set.md changes it together with
# arbiter.image.digest in storage-integrity-si-v2.yaml.
DEVNET2_ARBITER_DIGEST = HARDENED_DIGEST
# The HouseGate agent sidecar release indexer-a runs; stage 2 changes it.
DEVNET2_SIDECAR_VERSION = "v0.14.1"
# arbiter.consensusUpdatesEnabled on devnet2-si-v2; stage 2b changes it.
DEVNET2_CONSENSUS_UPDATES_ENABLED = False
```

In `ActualFreshNetworkTest.test_control_plane_is_a_new_genesis_with_isolated_state`, replace:

```python
        self.assertIs(arbiter["consensus_updates_enabled"], False)
        self.assertEqual(runtime_images(self.control, FRESH_RELEASE), ["ghcr.io/sentioxyz/arbiter@" + HARDENED_DIGEST] * 5)
```

with:

```python
        self.assertIs(arbiter["consensus_updates_enabled"], DEVNET2_CONSENSUS_UPDATES_ENABLED)
        self.assertEqual(runtime_images(self.control, FRESH_RELEASE), ["ghcr.io/sentioxyz/arbiter@" + DEVNET2_ARBITER_DIGEST] * 5)
```

In `test_indexer_a_binds_to_the_fresh_control_plane`, replace:

```python
        agent_state = yaml.safe_load(next(
            doc for doc in self.source.values() if "housegate-agent-network-state.yaml" in doc.get("data", {})
        )["data"]["housegate-agent-network-state.yaml"])
        self.assertEqual(
            agent_state["table_schemas"]["devnet101/swap_new2@1"]["schema_hash"], DEVNET2_TABLE_SCHEMA_HASH,
        )
```

with:

```python
        source = self.agent_config(self.source)["network_state"]["source"]
        if source.startswith("http"):
            # Registry-driven shape (stage 2): the agent asks this pod's storage RPC.
            storage_rpc = yaml.safe_load((DEVNET2 / "sentio-node-indexer-a.yaml").read_text())["node"]["ports"]["storage_rpc"]
            self.assertEqual(source, f"http://localhost:{storage_rpc}")
        else:
            agent_state = yaml.safe_load(next(
                doc for doc in self.source.values() if "housegate-agent-network-state.yaml" in doc.get("data", {})
            )["data"]["housegate-agent-network-state.yaml"])
            self.assertEqual(
                agent_state["table_schemas"]["devnet101/swap_new2@1"]["schema_hash"], DEVNET2_TABLE_SCHEMA_HASH,
            )
```

In `test_indexer_a_agent_signs_inline_values_with_pod_rewriter_materialization`, replace:

```python
        self.assertRegex(sidecar["image"], r"^ghcr\.io/housegate/housegate:v0\.14\.1@sha256:[0-9a-f]{64}$")
```

with:

```python
        self.assertRegex(sidecar["image"], rf"^ghcr\.io/housegate/housegate:{re.escape(DEVNET2_SIDECAR_VERSION)}@sha256:[0-9a-f]{{64}}$")
```

Insert immediately before `    def test_registry_shape_changes_only_the_table_source(self):`:

```python
    def test_voters_read_the_registry_from_separate_archive_nodes(self):
        arbiter = arbiter_config(self.control, FRESH_RELEASE)
        self.assertEqual(arbiter["table_registry"]["watcher"], {"enabled": False})
        self.assertEqual(arbiter["table_registry"]["audit"], {"enabled": False})
        urls = [
            init_rendered_arbiter_config(self.control, ordinal, FRESH_RELEASE)["table_registry"]["rpc_url"]
            for ordinal in range(3)
        ]
        self.assertEqual(urls, [
            "http://sentio-node-op-reth-0:8545",
            "http://sentio-node-op-reth-1:8545",
            "http://sentio-node-op-reth-0:8545",
        ])
        # The anchor endpoint is the load-balanced Service over all three ELs.
        self.assertNotIn(arbiter["anchor"]["evm"]["rpc_url"], urls)

```

- [ ] **Step 2: Run them and verify the new one fails.** `python3 -B charts/storage-integrity/tests/test_render.py`. Expected: `Ran 58 tests`, `FAILED (errors=1)`: `test_voters_read_the_registry_from_separate_archive_nodes` with `KeyError: 'table_registry'`.

- [ ] **Step 3: Add the inert section.** In `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml`, replace:

```yaml
    finalityConfirmations: 3

verifier:
```

with:

```yaml
    finalityConfirmations: 3
  # Rendered but inert: watcher and audit stay off (chart defaults) until the
  # registry is enabled in stage 3 of docs/storage-integrity-devnet2-dynamic-table-set.md.
  # Each voter reads the Databases contract from one archive op-geth through its
  # single-instance Service, never the load-balanced sentio-node-op-reth, and
  # the voters are spread over both archives so their auditors check
  # independently. Ordinal 2 shares op-reth-0: ordinal 2 of the L2 (op-reth) is
  # not an archive node.
  tableRegistry:
    rpcURLs:
      - http://sentio-node-op-reth-0:8545
      - http://sentio-node-op-reth-1:8545
      - http://sentio-node-op-reth-0:8545

verifier:
```

- [ ] **Step 4: Run the suite and the strict lint of the actual values.**

```bash
python3 -B charts/storage-integrity/tests/test_render.py
helm lint charts/storage-integrity -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml --namespace sentio-network-devnet2 --strict
```

Expected: `Ran 58 tests`, `OK`; `1 chart(s) linted, 0 chart(s) failed`.

- [ ] **Step 5: Run Task 3 Step 9** against this worktree (it needs the rendered configs). Expected: the output listed there.

- [ ] **Step 6: The only render change is the inert section.**

```bash
"$R/render-all.sh" "$PWD" "$R/renders/task6"
for f in "$R"/renders/base/*.yaml; do cmp -s "$f" "$R/renders/task6/$(basename "$f")" || { echo "DIFF $(basename "$f")"; diff "$f" "$R/renders/task6/$(basename "$f")"; }; done
```

Expected: only `sentio-network-devnet2--storage-integrity-si-v2.helmfile--storage-integrity-si-v2.yaml` differs, with exactly: the eleven-line `table_registry:` block after `tables:` in the ConfigMap (`rpc_url: "__TABLE_REGISTRY_RPC_URL__"`, `rpc_timeout: 10s`, `poll_interval: 5s`, `max_log_range: 10000`, `cursor_advance_interval: 1m`, `deploy_block: 0`, `watcher.enabled: false`, `audit.enabled: false`); the arbiter pod's `checksum/config` (`4f60e868…` → `d66ae395…` on `739619f06`); the seven-line `case "$ordinal"` block mapping 0 and 2 to `http://sentio-node-op-reth-0:8545` and 1 to `http://sentio-node-op-reth-1:8545`; and one `-e "s|__TABLE_REGISTRY_RPC_URL__|${table_registry_rpc_url}|g" \` line. The verifier and indexer-a renders are unchanged, so merging moves no running pod (the Arbiter StatefulSet is `OnDelete`).

- [ ] **Step 7: Commit.**

```bash
git add k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml charts/storage-integrity/tests/test_render.py
git commit -F - <<'EOF'
feat(devnet2): render the inert per-voter table_registry section

Voters 0/1/2 read the Databases contract from archive op-geth 0/1/0 through
their single-instance Services; watcher and audit stay off until stage 3.
The render tests name the Arbiter digest, sidecar version and
consensus-updates flag the rollout stages change.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 7: Runbook and superseded markers

**Files:**

- Create: `docs/storage-integrity-devnet2-dynamic-table-set.md`
- Modify: `docs/storage-integrity-devnet2-si-v2.md:10` (after the intro paragraph), `:334` (`## Rollback`), `:356` (the consensus-administration paragraph)

**Interfaces:** Consumes every pin from Tasks 1–2 (written as literals).

- [ ] **Step 1: Write the runbook.** Create `docs/storage-integrity-devnet2-dynamic-table-set.md` with this content, replacing `$ARBITER_RELEASE_TAG`, `$ARBITER_RELEASE_SHA`, `$ARBITER_RELEASE_DIGEST`, `$ARBITER_RELEASE_AMD64_DIGEST` and `$SENTIO_NODE_DIGEST` by the recorded values:

````markdown
# Storage integrity devnet2: dynamic SI table set upgrade

This runbook moves devnet2-si-v2 (releases `storage-integrity-si-v2` and `sentio-node-devnet2-indexer-a` in `sentio-network-devnet2`) onto the builds the dynamic storage-integrity table set needs, with behaviour unchanged, and prepares registry enablement. Design: housegate `docs/superpowers/specs/2026-09-28-dynamic-si-table-set-devnet2-rollout-design.md` (sub-project 5b). It supersedes the Arbiter version, consensus-administration and rollback statements of [storage-integrity-devnet2-si-v2.md](storage-integrity-devnet2-si-v2.md).

| Stage | Change | Way back |
| --- | --- | --- |
| 0 | Arbiter release `$ARBITER_RELEASE_TAG`; the chart PR (inert: devnet2 renders unchanged except a disabled `table_registry` block); measurements | Revert the PR |
| 1 | Arbiter voters and verifiers v0.7.1 → `$ARBITER_RELEASE_TAG`, stop-all cutover in a maintenance window | Only by restoring every voter's pre-cutover backup, and only before intake resumes (§2.10) |
| 2 | indexer-a: sentio-node `2af43bd`, in-pod rewriter 0.15.0, sidecar v0.15.0 asking the storage RPC for table status | Revert the stage 2 commit and sync (§3.5) |
| 2b | `consensusUpdatesEnabled: true`; no consensus update is committed | Not needed: nothing changes until an update commits |
| 3 | Registry, auditors, watcher | Not part of this upgrade (§5) |

After stage 2b the network behaves exactly as before: `devnet101.swap_new2` is the only storage-integrity table, it is signed and promoted, and every other table is ordinary.

Every command block is bash. Run them from the repository root, in order, in one dedicated `bash` session started with `set -eo pipefail` (from fish, run `bash` first). Never run two stages in one window.

## 0. Session helpers

Define these in every session.

```bash
si_kubectl() { kubectl --context sentio-sea -n sentio-network-devnet2 "$@"; }
si_arbiter() { si_kubectl exec "storage-integrity-si-v2-arbiter-$1" -c arbiter -- "${@:2}"; }
si_voter() { printf 'storage-integrity-si-v2-arbiter-%s.storage-integrity-si-v2-arbiter-raft.sentio-network-devnet2.svc.cluster.local:7080' "$1"; }
si_metrics() {
  si_arbiter "$1" wget -qO- http://127.0.0.1:9090/metrics |
    grep -E '^arbiter_(safe_watermark_block_seq|sealed_block_seq|open_block_statements|unanchored_verified_blocks|nodes\{role="(snode|verifier)",status="active"\}) '
}
si_capability() { si_arbiter "$1" /usr/local/bin/arbiter-admin consensus capability --address "$(si_voter "$1")" | jq -c .; }
si_leader() {
  for i in 0 1 2; do
    si_arbiter "$i" /usr/local/bin/arbiter-admin consensus show --address "$(si_voter "$i")" >/dev/null 2>&1 && { echo "$i"; return 0; }
  done
  return 1
}
si_show() { si_arbiter "$(si_leader)" /usr/local/bin/arbiter-admin consensus show --address "$(si_voter "$(si_leader)")"; }
si_show_stable() {
  si_show | jq -c '{network_id, genesis_snapshot_id, epoch, params_digest, promotion_seq, authority: .current.authority_addresses, max_writers: .current.max_writers}'
}
si_arbiter_pods() { si_kubectl get pods -l app.kubernetes.io/instance=storage-integrity-si-v2,app.kubernetes.io/component=arbiter -o name; }
si_images() {
  si_kubectl get pods -l app.kubernetes.io/instance=storage-integrity-si-v2 \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[0].image}{"\t"}{.status.containerStatuses[0].imageID}{"\n"}{end}' |
    grep -E '^storage-integrity-si-v2-(arbiter|verifier)-[0-9][[:space:]]'
}
si_indexer() { si_kubectl exec sentio-node-devnet2-indexer-a-0 -c sentio-node -- "$@"; }
si_rpc() { si_indexer wget -qO- --header 'Content-Type: application/json' --post-data "$1" http://localhost:32003; }
si_ch() { # one ClickHouse client query through indexer-a's agent sidecar
  si_kubectl run "si-5b-ch-$(date +%s)" -i --rm --restart=Never \
    --image=us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:25.8-lts-decimal512-2026-03-27@sha256:89839a322a59489fa14ca12e9d00f53d860b08c5486808f652a9484d7c247797 \
    --overrides='{"apiVersion":"v1","spec":{"imagePullSecrets":[{"name":"google-image-secret"}]}}' \
    --command -- clickhouse-client --compression=0 --host sentio-node-devnet2-indexer-a --port 9011 --query "$1"
}
si_backup="$HOME/si-devnet2-si-v2-arbiter-cutover"
```

## 1. Stage 0: preparation

### 1.1 Pins

| Component | Pin | Evidence |
| --- | --- | --- |
| Arbiter voters and verifiers | `ghcr.io/sentioxyz/arbiter@$ARBITER_RELEASE_DIGEST` (`$ARBITER_RELEASE_TAG`, commit `$ARBITER_RELEASE_SHA`, linux/amd64 `$ARBITER_RELEASE_AMD64_DIGEST`) | `docker buildx imagetools inspect`: OCI revision equals the tag's commit, version equals the tag |
| sentio-node | `ghcr.io/sentioxyz/sentio-node:sha-2af43bd9906e76c91e892579ec687463ef6cfdb7@$SENTIO_NODE_DIGEST` | main CI `docker-push`; OCI revision `2af43bd9906e76c91e892579ec687463ef6cfdb7` |
| In-pod rewriter (Gate R4) | `us-west1-docker.pkg.dev/sentio-352722/sentio/housegate-rewriter:0.15.0@sha256:57812c8cf40603dab0fa84f8f0c1c6270990c2a74820d1450e20d65c618e4989` | housegate/rewriter release run 36021827765 |
| Agent sidecar | `ghcr.io/housegate/housegate:v0.15.0@sha256:1b51de76b7b7a15a8006d0ac06febb10012c74c92d4950d07e4e5a5b77b3570f` | index digest of tag `v0.15.0` (housegate `04fb8c0`) |

### 1.2 Measurements (read-only)

The window of stage 1 is sized by these numbers. Nothing here writes to devnet2.

```bash
for i in 0 1 2; do
  echo "== arbiter-$i"
  si_arbiter "$i" sh -c 'du -sh /data/raft /data/raft/raft.db; df -h /data
    for d in /data/raft/snapshots/*/; do [ -f "$d/state.bin" ] || continue
      echo "$d $(wc -c < "$d/state.bin") bytes, version byte $(head -c 5 "$d/state.bin" | od -An -tu1 | awk "{print \$5}")"
      cat "$d/meta.json"; echo; done'
  si_kubectl logs "storage-integrity-si-v2-arbiter-$i" -c arbiter |
    grep -E 'starting restore|restored from snapshot|election won|entering (leader|follower) state' | head -6 || true
  /usr/bin/time -p sh -c "kubectl --context sentio-sea -n sentio-network-devnet2 exec storage-integrity-si-v2-arbiter-$i -c arbiter -- tar -C /data -czf - raft | wc -c"
done
for el in sentio-node-op-reth-0 sentio-node-op-reth-1; do
  for body in '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' \
              '{"jsonrpc":"2.0","id":2,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","0x1"]}'; do
    echo "$el $(si_arbiter 0 wget -qO- --header 'Content-Type: application/json' --post-data "$body" "http://$el:8545")"
  done
done
si_images
```

Record, per voter: `raft.db` size, each snapshot's size, index and version byte (4 for v0.7.1), free space on `/data`, the last restore duration (`starting restore` to `restored from snapshot`) and the compressed backup size and stream time. Both archive nodes must answer `eth_chainId` with `0x786d4d` and `eth_getBalance` at block 1 with a `result` (a pruned node answers `missing trie node`).

Stop if any voter has less free space on `/data` than twice its newest `state.bin`: the release writes a new snapshot beside the retained ones, and the PVC must first be grown. Stop if either archive check fails: the per-voter registry endpoints in `storage-integrity-si-v2.yaml` would be wrong.

## 2. Stage 1: Arbiter stop-all cutover

The release restores v0.7.1's v4 snapshots and replays its log to identical state, and proposes nothing new while the registry and consensus updates are off. It always writes v14 snapshots, which v0.7.1 refuses, so the only way back is restoring every voter's pre-cutover backup (§2.10), and only until intake resumes.

### 2.1 Preconditions

- The chart PR is merged and the stage 0 measurements are recorded.
- `git switch main && git pull --ff-only` is clean.
- The operator machine has room for three backups of the measured size.
- The window is announced; nobody may send INSERTs to `devnet101.swap_new2` until §2.9.

### 2.2 Drain

```bash
for i in 0 1 2; do echo "== arbiter-$i"; si_metrics "$i"; done
```

Proceed when every voter reports the same values with `arbiter_open_block_statements 0`, `arbiter_unanchored_verified_blocks 0`, `arbiter_safe_watermark_block_seq` equal to `arbiter_sealed_block_seq`, one active SNode and three active verifiers. Otherwise wait and repeat; a watermark that does not converge is an incident, not a reason to cut over.

### 2.3 Baseline

```bash
mkdir -m 700 "$si_backup"
for i in 0 1 2; do si_metrics "$i" > "$si_backup/metrics-before-$i.txt"; done
si_show_stable > "$si_backup/show-before.json"
for i in 0 1 2; do si_capability "$i"; done > "$si_backup/capability-before.txt"
for i in 0 1 2; do echo "arbiter-$i $(si_kubectl logs "storage-integrity-si-v2-arbiter-$i" -c arbiter | grep 'election won' | tail -1)"; done > "$si_backup/leader-before.txt"
si_images > "$si_backup/images-before.txt"
cat "$si_backup"/metrics-before-0.txt "$si_backup/show-before.json" "$si_backup/leader-before.txt"
```

`mkdir` fails if a previous attempt left the directory: move it aside, never overwrite a backup.

### 2.4 Stop every voter

```bash
si_kubectl scale statefulset/storage-integrity-si-v2-arbiter --replicas=0
until [ -z "$(si_arbiter_pods)" ]; do sleep 2; done
echo "all voters stopped"
```

Indexer-a and the verifiers keep running; their calls to the Arbiter fail and retry until §2.7.

### 2.5 Back up every `raft.data_dir`

Each voter's PVC is node-local (`local-path`); a helper pod mounting it read-only is scheduled onto that node.

```bash
for i in 0 1 2; do
si_kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: si-arbiter-backup-$i
spec:
  automountServiceAccountToken: false
  restartPolicy: Never
  containers:
    - name: backup
      image: ghcr.io/sentioxyz/arbiter@sha256:0a83dded9bea0b5ab7b58f48a131fbc7f5a1427040579790fd4ddca75f6af32c
      command: [sleep, "86400"]
      volumeMounts:
        - {name: data, mountPath: /data, readOnly: true}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: data-storage-integrity-si-v2-arbiter-$i, readOnly: true}
EOF
done
si_kubectl wait --for=condition=Ready pod/si-arbiter-backup-0 pod/si-arbiter-backup-1 pod/si-arbiter-backup-2 --timeout=5m
for i in 0 1 2; do
  si_kubectl exec "si-arbiter-backup-$i" -- tar -C /data -czf - raft > "$si_backup/arbiter-$i-raft.tgz"
  si_kubectl exec "si-arbiter-backup-$i" -- sh -c 'cd /data && find raft -type f -exec sha256sum {} + | sort -k 2' > "$si_backup/arbiter-$i-raft.sha256"
  check="$(mktemp -d)"
  tar -C "$check" -xzf "$si_backup/arbiter-$i-raft.tgz"
  (cd "$check" && shasum -a 256 -c --quiet "$si_backup/arbiter-$i-raft.sha256")
  rm -rf "$check"
  echo "arbiter-$i: $(wc -l < "$si_backup/arbiter-$i-raft.sha256") files, $(du -h "$si_backup/arbiter-$i-raft.tgz" | cut -f1)"
  grep -E ' raft/(raft\.db|consensus-protocol\.json)$' "$si_backup/arbiter-$i-raft.sha256"
done
si_kubectl delete pod si-arbiter-backup-0 si-arbiter-backup-1 si-arbiter-backup-2 --wait
```

Every voter must list `raft/raft.db` and `raft/consensus-protocol.json`, plus the snapshot files §1.2 recorded, and the extracted copy must verify against the manifest. Stop on any failure; the voters are still stopped and unchanged, so restart them with `si_kubectl scale statefulset/storage-integrity-si-v2-arbiter --replicas=3` on v0.7.1.

### 2.6 Offline replay check

The throwaway `statedigest` tool (source in the housegate 5b plan, Task 3) restores a copy of each backup into the v0.7.1 FSM and into the release's FSM and hashes the resulting state. Run it from the rehearsal directory `R` of that task.

```bash
R="$HOME/si-devnet2-5b-rehearsal"
for i in 0 1 2; do
  work="$(mktemp -d)"
  tar -C "$work" -xzf "$si_backup/arbiter-$i-raft.tgz"
  for v in old new; do
    "$R/bin/$v/statedigest" -data-dir "$work/raft" -network-id devnet2 -schema-snapshot-id devnet2-si-v2 \
      -executor-profile-id housegate-replay-mvp-v0 -max-writers 1 \
      -authority 0x9ef3a259d1d87c864431cab5ed5f6578ad5ad705 > "$si_backup/digest-$i-$v.txt" 2> "$si_backup/digest-$i-$v.err" || { echo "statedigest $v failed for arbiter-$i:"; cat "$si_backup/digest-$i-$v.err"; exit 1; }
    echo "arbiter-$i $v $(grep -E '^(snapshot_index|last_index|written_version)' "$si_backup/digest-$i-$v.txt" | tr '\n' ' ')state=$(grep -E '^(key|spent_ids)' "$si_backup/digest-$i-$v.txt" | grep -v '^key artifact_disposition ' | shasum -a 256 | cut -c1-16)"
  done
  rm -rf "$work"
done
```

For each voter the `old` and `new` lines must show the same `state=` (the release adds only the empty `artifact_disposition` key, which the hash excludes); `written_version` is 4 for `old` and 14 for `new`. The three voters normally agree as well because §2.2 drained them. Stop on any `old`/`new` difference, or on any statedigest error (it prints its stderr and ends the session): the release would not replay this voter to the same state. Either way, scale back to 3 on v0.7.1 and end the window.

### 2.7 Pin the release and start

Push the values change (one commit: `arbiter.image.digest` in `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml` and its mirror `DEVNET2_ARBITER_DIGEST` in `charts/storage-integrity/tests/test_render.py`, both `$ARBITER_RELEASE_DIGEST`), after `python3 -B charts/storage-integrity/tests/test_render.py` passes. Then:

```bash
stage1_commit="$(git rev-parse HEAD)"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
si_kubectl scale statefulset/storage-integrity-si-v2-arbiter --replicas=3
until [ "$(si_arbiter_pods | wc -l)" -eq 3 ]; do sleep 2; done
si_kubectl wait --for=condition=Ready pod/storage-integrity-si-v2-arbiter-0 pod/storage-integrity-si-v2-arbiter-1 pod/storage-integrity-si-v2-arbiter-2 --timeout=15m
si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
```

The sync rolls the verifiers (RollingUpdate) and reruns the bootstrap hook with the release image; the `scale` is a no-op when the sync already restored three replicas.

### 2.8 Verify (intake still paused)

```bash
release_digest="$(sed -n 's/^    digest: //p' k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml)"
si_images
for i in 0 1 2; do
  echo "== arbiter-$i"
  si_kubectl logs "storage-integrity-si-v2-arbiter-$i" -c arbiter | grep -E 'starting restore|restored from snapshot|failed to restore|election won|level=ERROR' || true
  echo "marker $(si_arbiter "$i" sha256sum /data/raft/consensus-protocol.json | cut -c1-64) backup $(grep ' raft/consensus-protocol.json$' "$si_backup/arbiter-$i-raft.sha256" | cut -c1-64)"
  diff <(si_metrics "$i") "$si_backup/metrics-before-$i.txt" && echo "metrics equal the baseline"
  si_capability "$i"
done
diff <(si_show_stable) "$si_backup/show-before.json" && echo "consensus show equals the baseline"
```

- `si_images` lists three arbiter and three verifier pods, each running `ghcr.io/sentioxyz/arbiter@$release_digest` with an `imageID` ending in `$ARBITER_RELEASE_DIGEST` or `$ARBITER_RELEASE_AMD64_DIGEST`.
- Each voter that had a snapshot in §1.2 logs `restored from snapshot` for its newest snapshot id and no `failed to restore` or `level=ERROR`; one voter logs `election won`.
- Each marker hash equals its backup's.
- Metrics and `consensus show` equal the baseline, and every capability reports its own `node_id`, `protocol_version: 1` and `updates_enabled: false`. The capability is identical for v0.7.1 and the release: the image check above is what proves the version.
- `for n in 0 1 2; do si_kubectl logs "storage-integrity-si-v2-verifier-$n" -c verifier --since=15m | grep -c 'level=ERROR' || true; done` prints `0` three times once the voters are Ready.

Any failure here: roll back (§2.10) before §2.9.

### 2.9 Resume intake

Send one signed INSERT through indexer-a (still sentio-node `1494fb1`, the old data-plane protocol) and wait for it to become safe:

```bash
base_safe="$(sed -n 's/^arbiter_safe_watermark_block_seq //p' "$si_backup/metrics-before-0.txt")"
si_kubectl run si-5b-stage1-acceptance -i --restart=Never \
  --image=us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:25.8-lts-decimal512-2026-03-27@sha256:89839a322a59489fa14ca12e9d00f53d860b08c5486808f652a9484d7c247797 \
  --overrides='{"apiVersion":"v1","spec":{"imagePullSecrets":[{"name":"google-image-secret"}]}}' \
  --command -- sh -ec '
    clickhouse-local --query "SELECT toInt64(501) AS value FORMAT Native" > /tmp/row.native
    clickhouse-client --compression=0 --host sentio-node-devnet2-indexer-a --port 9011 \
      --query "INSERT INTO devnet101.swap_new2 (value) FORMAT Native" < /tmp/row.native
  '
for n in $(seq 1 90); do
  safe="$(si_metrics 0 | sed -n 's/^arbiter_safe_watermark_block_seq //p')"
  [ "$safe" -gt "$base_safe" ] && break
  sleep 10
done
for i in 0 1 2; do si_metrics "$i"; done
si_ch "SELECT count() FROM devnet101.swap_new2 WHERE value = 501 SETTINGS SQL_x_read_mode='safe'"
si_kubectl delete pod si-5b-stage1-acceptance
```

Every voter's safe watermark must pass `base_safe` and equal its sealed tip, and the safe read returns `1`. From here on the cutover is final: treat any problem as an incident, not a rollback. Keep `$si_backup` until stage 2b is verified.

### 2.10 Rollback (only before §2.9)

This loses nothing that matters: intake was paused, so nothing was sealed since the baseline.

```bash
si_kubectl scale statefulset/storage-integrity-si-v2-arbiter --replicas=0
until [ -z "$(si_arbiter_pods)" ]; do sleep 2; done
for i in 0 1 2; do
si_kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: si-arbiter-restore-$i
spec:
  automountServiceAccountToken: false
  restartPolicy: Never
  containers:
    - name: restore
      image: ghcr.io/sentioxyz/arbiter@sha256:0a83dded9bea0b5ab7b58f48a131fbc7f5a1427040579790fd4ddca75f6af32c
      command: [sleep, "86400"]
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: data-storage-integrity-si-v2-arbiter-$i}
EOF
done
si_kubectl wait --for=condition=Ready pod/si-arbiter-restore-0 pod/si-arbiter-restore-1 pod/si-arbiter-restore-2 --timeout=5m
for i in 0 1 2; do
  si_kubectl exec -i "si-arbiter-restore-$i" -- sh -ec 'rm -rf /data/raft && tar -C /data -xzf -' < "$si_backup/arbiter-$i-raft.tgz"
  si_kubectl exec -i "si-arbiter-restore-$i" -- sh -ec 'cd /data && sha256sum -c -s -' < "$si_backup/arbiter-$i-raft.sha256" && echo "arbiter-$i restored"
done
si_kubectl delete pod si-arbiter-restore-0 si-arbiter-restore-1 si-arbiter-restore-2 --wait
git show --stat "$stage1_commit"   # must be the stage 1 digest commit
git revert --no-edit "$stage1_commit"
python3 -B charts/storage-integrity/tests/test_render.py
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
si_kubectl scale statefulset/storage-integrity-si-v2-arbiter --replicas=3
until [ "$(si_arbiter_pods | wc -l)" -eq 3 ]; do sleep 2; done
si_kubectl wait --for=condition=Ready pod/storage-integrity-si-v2-arbiter-0 pod/storage-integrity-si-v2-arbiter-1 pod/storage-integrity-si-v2-arbiter-2 --timeout=15m
for i in 0 1 2; do diff <(si_metrics "$i") "$si_backup/metrics-before-$i.txt" && echo "arbiter-$i at baseline"; done
diff <(si_show_stable) "$si_backup/show-before.json" && echo "consensus show at baseline"
```

Every voter must restore from its backup: v0.7.1 refuses a data directory the release has written (`unsupported snapshot version 14`), and a voter restored alone cannot rejoin a cluster whose leader is the release.

## 3. Stage 2: indexer-a sync

One commit moves sentio-node, the in-pod rewriter and the sidecar together: the embedded HouseGate v0.15.0 refuses a contract-V1 rewriter at startup (Gate R4), and the V2 rewriter serves V1 requests unchanged. The sidecar switches to the storage RPC while the registry is disabled, when its answers equal today's YAML, so stage 3 needs no sidecar change.

### 3.1 Preconditions

- Stage 1 is verified and §2.9 passed.
- The three pins in §1.1 resolve (the rewriter needs `gcloud auth login` and `gcloud auth configure-docker us-west1-docker.pkg.dev` first); each command prints the pinned digest:

  ```bash
  docker buildx imagetools inspect ghcr.io/sentioxyz/sentio-node:sha-2af43bd9906e76c91e892579ec687463ef6cfdb7 | sed -n 's/^Digest: *//p'
  docker buildx imagetools inspect us-west1-docker.pkg.dev/sentio-352722/sentio/housegate-rewriter:0.15.0 | sed -n 's/^Digest: *//p'
  docker buildx imagetools inspect ghcr.io/housegate/housegate:v0.15.0 | sed -n 's/^Digest: *//p'
  ```
- No SI writer is running; processor indexing may continue.
- Record the running driver tasks: `si_kubectl exec sentio-node-devnet2-indexer-a-0 -c sentio-node -- wget -qO- http://sentio-node-devnet2-indexer-a-dind:2375/tasks | jq '[.[] | select(.Status.State=="running")] | length'`.

### 3.2 Values commit

One commit, pushed to `main` after `python3 -B charts/storage-integrity/tests/test_render.py` passes:

- `sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml` and its mirrors `docs/examples/storage-integrity-fresh-source-image.yaml` and `FRESH_SOURCE_IMAGE` in `test_render.py`: sentio-node `sha-2af43bd9906e76c91e892579ec687463ef6cfdb7@$SENTIO_NODE_DIGEST`.
- `sentio-node-indexer-a-storage-integrity-si-v2.yaml`: rewriter `0.15.0@sha256:57812c8c…`; no `housegate.storageIntegrity.tables` (the table set comes from the embedded SNode); sidecar `v0.15.0@sha256:1b51de76…`; `networkStateSource: http://localhost:32003`; no `tableSchemas`. `DEVNET2_SIDECAR_VERSION` in `test_render.py` becomes `v0.15.0`.

### 3.3 Sync

```bash
stage2_commit="$(git rev-parse HEAD)"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
```

### 3.4 Verify

```bash
si_kubectl get pod sentio-node-devnet2-indexer-a-0 -o jsonpath='{range .spec.containers[*]}{.name}{"\t"}{.image}{"\n"}{end}'
si_kubectl logs sentio-node-devnet2-indexer-a-0 -c housegate-sidecar | grep 'network state loaded from RPC'
si_indexer wget -qO- http://localhost:9091/metrics | grep -E '^sentio_node_storage_integrity_(registry_enabled|registry_follower_connected|registry_version|tables)'
si_indexer wget -qO- http://localhost:9092/metrics | grep -E '^clickhouse_proxy_agent_si_table_status_failures_total' || echo "no status failures recorded"
si_rpc '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityTableStatus","params":["devnet101","swap_new2"]}' | jq -c '.result | {status, schema_hash, registry_version}'
si_rpc '{"jsonrpc":"2.0","id":2,"method":"sentio_getStorageIntegrityTableStatus","params":["devnet101","sp5b_no_such_table"]}' | jq -c '.result.status'
```

- Containers: `housegate-rewriter` `…:0.15.0@sha256:57812c8c…`, `housegate-sidecar` `…:v0.15.0@sha256:1b51de76…`, `sentio-node` `…:sha-2af43bd…@$SENTIO_NODE_DIGEST`.
- The sidecar logs `network state loaded from RPC` with `endpoint=http://localhost:32003`.
- `sentio_node_storage_integrity_registry_enabled 0`, `…_registry_follower_connected 1` (the watch waits while the registry is disabled), `…_registry_version 0`, `…_tables{status="active"} 1`. The registry follower treats the Arbiter's `table registry is disabled` answer as a normal state and does not log it; these metrics are the check.
- `swap_new2` answers `{"status":"active","schema_hash":"0x37f5a671528eab2f6f3448bb093e924f67096a46f3e96155e17760e55d0ba3ed","registry_version":0}`; the missing table answers `"ordinary"`. These calls run inside the pod's network namespace, which the sidecar shares.

Then a signed INSERT, promoted to safe:

```bash
base_safe="$(si_metrics 0 | sed -n 's/^arbiter_safe_watermark_block_seq //p')"
si_kubectl run si-5b-stage2-acceptance -i --restart=Never \
  --image=us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:25.8-lts-decimal512-2026-03-27@sha256:89839a322a59489fa14ca12e9d00f53d860b08c5486808f652a9484d7c247797 \
  --overrides='{"apiVersion":"v1","spec":{"imagePullSecrets":[{"name":"google-image-secret"}]}}' \
  --command -- sh -ec '
    clickhouse-local --query "SELECT toInt64(502) AS value FORMAT Native" > /tmp/row.native
    clickhouse-client --compression=0 --host sentio-node-devnet2-indexer-a --port 9011 \
      --query "INSERT INTO devnet101.swap_new2 (value) FORMAT Native" < /tmp/row.native
  '
for n in $(seq 1 90); do
  safe="$(si_metrics 0 | sed -n 's/^arbiter_safe_watermark_block_seq //p')"
  [ "$safe" -gt "$base_safe" ] && break
  sleep 10
done
for i in 0 1 2; do si_metrics "$i"; done
si_ch "SELECT count() FROM devnet101.swap_new2 WHERE value = 502 SETTINGS SQL_x_read_mode='safe'"
si_kubectl delete pod si-5b-stage2-acceptance
```

The safe watermark passes `base_safe` on every voter and the safe read returns `1`. An unsigned INSERT would have been refused with code 733 (`requires a signed INSERT`), so this proves the sidecar reached the storage RPC and signed.

Processor indexing continues: the running driver task count equals §3.1's (repeat the §3.1 command after a few minutes).

Creation blocks are recorded (the stage 3 `activation_block` must follow this block):

```bash
si_ch "CREATE TABLE devnet101.sp5b_created_block_probe (value Int64) ENGINE = MergeTree ORDER BY tuple()"
for n in $(seq 1 30); do
  probe="$(si_rpc '{"jsonrpc":"2.0","id":3,"method":"sentio_getDatabaseInfoById","params":["devnet101"]}' | jq -c '.result.tables[] | select(.tableId=="sp5b_created_block_probe")')"
  [ -n "$probe" ] && break
  sleep 10
done
echo "$probe"
si_ch "DROP TABLE devnet101.sp5b_created_block_probe"
```

`createdBlock` is non-zero; record it as the stage 2 block (`stage2_block`).

### 3.5 Rollback

Valid until stage 3: no genesis table can be dropped while the registry is disabled.

```bash
git show --stat "$stage2_commit"   # must be the stage 2 commit
git revert --no-edit "$stage2_commit"
python3 -B charts/storage-integrity/tests/test_render.py
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
```

The revert restores the legacy `tables` list, the YAML `networkStateSource` and `tableSchemas` together with the old images; the chart renders both shapes.

## 4. Stage 2b: consensus administration

Enable updates only after every voter runs the release (stage 1) and has passed its direct capability probe. Enabling commits nothing; no consensus update is part of this upgrade.

Push `arbiter.consensusUpdatesEnabled: true` in `storage-integrity-si-v2.yaml` with its mirror `DEVNET2_CONSENSUS_UPDATES_ENABLED = True` in `test_render.py` (one commit, tests passing), then:

```bash
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
leader="$(si_leader)"
for i in $(printf '0\n1\n2\n' | grep -vx "$leader") "$leader"; do
  si_kubectl delete pod "storage-integrity-si-v2-arbiter-$i" --wait
  until si_kubectl get pod "storage-integrity-si-v2-arbiter-$i" >/dev/null 2>&1; do sleep 2; done
  si_kubectl wait --for=condition=Ready "pod/storage-integrity-si-v2-arbiter-$i" --timeout=15m
  si_capability "$i"
  for j in 0 1 2; do si_metrics "$j" | grep sealed_block_seq; done
done
si_show | tee "$HOME/si-devnet2-si-v2-consensus-show-2b.json" | jq -c '{epoch, params_digest, authority: .current.authority_addresses, max_writers: .current.max_writers, table_registry: .current.table_registry}'
```

Followers go first and the leader last, one pod at a time, so two voters always form a quorum; the sync alone does not replace `OnDelete` pods. Each capability must report `updates_enabled: true` for its own `node_id`, and the sealed tip must match on all three voters before the next deletion. Record the `consensus show` answer: epoch `0`, authority `["0x9ef3a259d1d87c864431cab5ed5f6578ad5ad705"]`, `max_writers` `1`, `table_registry` `null`. Consensus-administration requests on `grpc_listen` are now accepted from anyone who can reach it and holds the authority key; the key stays only in `storage-integrity-devnet2-si-v2-authority`.

## 5. Stage 3: registry, auditors, watcher (not part of this upgrade)

Preconditions:

- The table-reference hardening (rewriter-go v0.14.0, rewriter-grpc v0.16.0 and its housegate release) is live on devnet2, first in `tableref_guard.mode: observe`.
- Stage 2 is done: sentio-node `2af43bd` carries the I1 follow-up (every genesis id outside the create-mode preflight, Retiring and Purging included, loads its genesis schema by hash under `schema_source: network_state`).
- indexer-a is on-chain indexer 0 and no unrelated database is bound to indexer 0: `si_rpc '{"jsonrpc":"2.0","id":4,"method":"sentio_getIndexerInfoById","params":[0]}' | jq -c .result` names `sentio-node-devnet2-indexer-a`, and `cast call "$databases" 'getDatabases()((string,bool,uint8,address,address,address,uint256,string,bool)[])' --rpc-url "$rpc"` lists only indexer-a's databases with the seventh field `0`.

Lookups, through `si_kubectl port-forward svc/sentio-node-op-reth-0 18545:8545` (an archive node) in another session:

```bash
rpc=http://127.0.0.1:18545
databases="$(cast call 0xb7e9E56DFC27bcfA63698F2F5890e186426A0123 'getAddress(string)(address)' databases --rpc-url "$rpc")"
lo=0; hi="$(cast block-number --rpc-url "$rpc")"
while [ $((hi - lo)) -gt 1 ]; do
  mid=$(((lo + hi) / 2))
  if [ "$(cast code "$databases" --block "$mid" --rpc-url "$rpc")" = "0x" ]; then lo=$mid; else hi=$mid; fi
done
echo "Databases proxy $databases deployed at block $hi"
cast call "$databases" 'getTableSchema(string,string,uint32)((bytes32,string))' devnet101 swap_new2 1 --rpc-url "$rpc"
cast block safe --field number --rpc-url "$rpc"
```

- `deploy_block`: the proxy's deployment block found above (or 0), and at most `activation_block - 1`.
- `activation_block`: after the stage 2 block (§3.4) and at least about one hour of L2 blocks (1800 at 2 s) after the safe head when the update is submitted; it is set once.
- The version-1 declaration of `devnet101.swap_new2` must carry `0x37f5a671528eab2f6f3448bb093e924f67096a46f3e96155e17760e55d0ba3ed`.

Enable from the leader pod, which holds the current authority key in `ARBITER_AUTHORITY_PRIVATE_KEY_HEX` while signing is enabled (the complete target authority set and writer limit are mandatory; the five `--table-registry-*` flags go together; `stage2_block` is §3.4's):

```bash
activation_block=$(( $(cast block safe --field number --rpc-url "$rpc") + 1800 ))
[ "$activation_block" -gt "$stage2_block" ]
leader="$(si_leader)"
epoch="$(si_show | jq -r .epoch)"
si_arbiter "$leader" /usr/local/bin/arbiter-admin consensus update --address "$(si_voter "$leader")" \
  --expected-epoch "$epoch" --authority 0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705 --max-writers 1 \
  --table-registry-chain-id 7892301 --table-registry-contract "$databases" \
  --table-registry-si-indexer-id 0 --table-registry-activation-block "$activation_block" \
  --table-registry-confirmation safe --timeout 10s
si_show | jq -c '{epoch, table_registry: .current.table_registry}'
```

Then, in order: set `arbiter.tableRegistry.deployBlock` and `audit.enabled: true` (chart refuses audit on the v0.7.1 digest) and replace every Arbiter pod; confirm `arbiter_table_registry_audit_mismatches_total` stays flat; set `watcher.enabled: true` and replace every Arbiter pod; confirm `sentio_node_storage_integrity_registry_enabled 1` on indexer-a before the L2 safe head reaches `activation_block`; confirm the seed; confirm `sentio_getStorageIntegrityTableStatus("devnet101","swap_new2")` still answers `active` with `schema_hash` `0x37f5a671…3ed`, equal to the contract's version-1 declaration above (the seeded genesis hash must equal `payloadexec.TableSchemaHash("devnet2", <declaration>)`; startup fails closed if they differ) before any genesis table is dropped; then run the end-to-end check with no restart and no config edit: CREATE → retryable `pending activation` → Active → signed INSERT → promotion → DROP → retire → purge → same-name CREATE → Active.

## 6. Operating rules

- Recreating a genesis table's name before its purge completes is safe only on sentio-node `2af43bd` or later (stage 2); on older images it can break the next restart.
- Never declare a malformed schema on a genesis table's name: startup still validates the latest declaration of every genesis id.
- Never roll indexer-a back past `2af43bd` once a genesis table has been dropped.
- An evicted source SNode rejoins only with a wiped state directory.
- Do not declare schemas for processor tables: declaring one brings it into storage integrity and the driver's unsigned writes are then refused.
- Keep verifiers on inline schemas (`table:` in `storage-integrity-si-v2.yaml`).
- Blocks discarded by a §2.10 rollback stay anchored on the L2 AnchorRegistry as orphans; record their hashes from the release's logs before restoring.
````

- [ ] **Step 2: Check it.** `grep -nE '\$(ARBITER_RELEASE|SENTIO_NODE)_' docs/storage-integrity-devnet2-dynamic-table-set.md` prints nothing; `grep -c '^```bash' docs/storage-integrity-devnet2-dynamic-table-set.md` prints `19`; every `](` link target exists (`docs/storage-integrity-devnet2-si-v2.md`).

- [ ] **Step 3: Mark the superseded parts of the si-v2 runbook.** In `docs/storage-integrity-devnet2-si-v2.md`, replace:

```markdown
Nothing belonging to the legacy release is deleted.
```

with:

```markdown
Nothing belonging to the legacy release is deleted.

> **Superseded in part (2026-09-28).** The Arbiter v0.7.1 pin, the consensus-administration statements and the Arbiter rollback below describe devnet2-si-v2 before the dynamic SI table set upgrade. From that upgrade on, follow [storage-integrity-devnet2-dynamic-table-set.md](storage-integrity-devnet2-dynamic-table-set.md): the Arbiter runs the release pinned in `storage-integrity-si-v2.yaml`, consensus administration is enabled (stage 2b), and the only Arbiter rollback is its §2.10.
```

Replace:

```markdown
## Rollback
```

with:

```markdown
## Rollback

> **Superseded for the Arbiter (2026-09-28).** Once the Arbiter release has run as leader, v0.7.1 cannot read its data directories; the only way back is restoring every voter's pre-cutover backup ([dynamic table set runbook §2.10](storage-integrity-devnet2-dynamic-table-set.md#210-rollback-only-before-29)), and only before intake resumes.
```

and replace:

```markdown
Consensus administration stays disabled. Enabling it, and authority rotation,
```

with:

```markdown
> **Superseded (2026-09-28).** Consensus administration is enabled by stage 2b of the [dynamic table set runbook](storage-integrity-devnet2-dynamic-table-set.md#4-stage-2b-consensus-administration); the next paragraph describes the state before it.

Consensus administration stays disabled. Enabling it, and authority rotation,
```

- [ ] **Step 4: Run the suite** (docs are not tested, but the workflow runs on this path set): `python3 -B charts/storage-integrity/tests/test_render.py`. Expected: `Ran 58 tests`, `OK`.

- [ ] **Step 5: Commit.**

```bash
git add docs/storage-integrity-devnet2-dynamic-table-set.md docs/storage-integrity-devnet2-si-v2.md
git commit -F - <<'EOF'
docs(storage-integrity): devnet2 dynamic SI table set upgrade runbook

Stages 0-2b (Arbiter stop-all cutover with verified backups and an offline
replay gate, the indexer-a sync, consensus administration), the stage 3
outline and the operating rules; the conflicting si-v2 runbook sections are
marked superseded.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 8: Open the production PR and merge it (controller-only, gated)

**Files:** none.

- [ ] **Step 1: Full local verification.** From the production worktree: `python3 -B charts/storage-integrity/tests/test_render.py` (`Ran 58 tests`, `OK`), the three lint commands of Task 5 Step 4 plus Task 6 Step 4's, and Task 6 Step 6 (only the inert section differs).
- [ ] **Step 2: Push and open the PR.**

Write `$R/pr-body.md`:

```markdown
Chart support and runbook for the devnet2 upgrade of the dynamic storage-integrity table set (housegate spec `docs/superpowers/specs/2026-09-28-dynamic-si-table-set-devnet2-rollout-design.md`, sub-project 5b).

- `charts/sentio-node`: the table-set shape follows the values. An empty `housegate.storageIntegrity.tables` renders `enabled: true` (sentio-node takes the table set from its embedded SNode); an `http(s)` sidecar `networkStateSource` drops the declared schemas and must use this node's `node.ports.storage_rpc`. Today's shape renders byte-identically.
- `charts/storage-integrity`: optional `arbiter.tableRegistry`, one archive-node `rpc_url` per voter ordinal, substituted by the existing `render-config` init container; watcher and audit default off and are refused on arbiter v0.7.1, which ignores the section.
- devnet2: the inert section (voters 0/1/2 → `sentio-node-op-reth-0/1/0`, both archive op-geth), and render-test constants for the pins the rollout stages change.
- docs: `storage-integrity-devnet2-dynamic-table-set.md`; the conflicting parts of the si-v2 runbook are marked superseded.

Inert on merge: all ten consumer releases (devnet2 and testnet-v2) render byte-identically except the devnet2 si-v2 control plane's disabled `table_registry` block, its init-container `case`/`sed` lines and the Arbiter `checksum/config`; the Arbiter StatefulSet is `OnDelete`, so no pod moves. The per-voter configs load with arbiter v0.7.1 (section ignored) and the release (registry reads off, `rpc_url` per voter).

The stop-all cutover was rehearsed locally (three v0.7.1 voters with an EVM anchor and an old-protocol data plane, stop, back up, offline replay with both builds, start the release, rollback from backups); the record is in the housegate plan `docs/superpowers/plans/2026-09-28-si-devnet2-rollout.md`.

Stages 1, 2 and 2b follow as direct values pushes to `main`, each with its render-test constant.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

```bash
git push -u origin dev/poetry/si-dynamic-table-set-devnet2
gh pr create --repo sentioxyz/production --base main --head dev/poetry/si-dynamic-table-set-devnet2 \
  --title "storage-integrity: dynamic SI table set chart support and devnet2 upgrade runbook" --body-file "$R/pr-body.md"
```

- [ ] **Step 3: Merge** after review and a green `Storage-integrity chart validation` workflow. Record the merge commit for Task 13. Merging changes no running pod; do not sync anything.

## Task 9: HUMAN-GATED — stage 0 measurement on devnet2 (read-only)

Executed by the user, or with the user confirming each block. Nothing in this task writes to devnet2.

**Files:** none. **Interfaces:** Produces the window size and the go/no-go for Task 10.

- [ ] **Step 1: Session.** From the production repository root (after Task 8), run `bash`, then `set -eo pipefail`, then the runbook §0 helper block:

```bash
si_kubectl() { kubectl --context sentio-sea -n sentio-network-devnet2 "$@"; }
si_arbiter() { si_kubectl exec "storage-integrity-si-v2-arbiter-$1" -c arbiter -- "${@:2}"; }
si_voter() { printf 'storage-integrity-si-v2-arbiter-%s.storage-integrity-si-v2-arbiter-raft.sentio-network-devnet2.svc.cluster.local:7080' "$1"; }
si_metrics() {
  si_arbiter "$1" wget -qO- http://127.0.0.1:9090/metrics |
    grep -E '^arbiter_(safe_watermark_block_seq|sealed_block_seq|open_block_statements|unanchored_verified_blocks|nodes\{role="(snode|verifier)",status="active"\}) '
}
si_capability() { si_arbiter "$1" /usr/local/bin/arbiter-admin consensus capability --address "$(si_voter "$1")" | jq -c .; }
si_leader() {
  for i in 0 1 2; do
    si_arbiter "$i" /usr/local/bin/arbiter-admin consensus show --address "$(si_voter "$i")" >/dev/null 2>&1 && { echo "$i"; return 0; }
  done
  return 1
}
si_show() { si_arbiter "$(si_leader)" /usr/local/bin/arbiter-admin consensus show --address "$(si_voter "$(si_leader)")"; }
si_show_stable() {
  si_show | jq -c '{network_id, genesis_snapshot_id, epoch, params_digest, promotion_seq, authority: .current.authority_addresses, max_writers: .current.max_writers}'
}
si_arbiter_pods() { si_kubectl get pods -l app.kubernetes.io/instance=storage-integrity-si-v2,app.kubernetes.io/component=arbiter -o name; }
si_images() {
  si_kubectl get pods -l app.kubernetes.io/instance=storage-integrity-si-v2 \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[0].image}{"\t"}{.status.containerStatuses[0].imageID}{"\n"}{end}' |
    grep -E '^storage-integrity-si-v2-(arbiter|verifier)-[0-9][[:space:]]'
}
si_indexer() { si_kubectl exec sentio-node-devnet2-indexer-a-0 -c sentio-node -- "$@"; }
si_rpc() { si_indexer wget -qO- --header 'Content-Type: application/json' --post-data "$1" http://localhost:32003; }
si_ch() { # one ClickHouse client query through indexer-a's agent sidecar
  si_kubectl run "si-5b-ch-$(date +%s)" -i --rm --restart=Never \
    --image=us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:25.8-lts-decimal512-2026-03-27@sha256:89839a322a59489fa14ca12e9d00f53d860b08c5486808f652a9484d7c247797 \
    --overrides='{"apiVersion":"v1","spec":{"imagePullSecrets":[{"name":"google-image-secret"}]}}' \
    --command -- clickhouse-client --compression=0 --host sentio-node-devnet2-indexer-a --port 9011 --query "$1"
}
si_backup="$HOME/si-devnet2-si-v2-arbiter-cutover"
```

- [ ] **Step 2: Measure** (runbook §1.2).

```bash
for i in 0 1 2; do
  echo "== arbiter-$i"
  si_arbiter "$i" sh -c 'du -sh /data/raft /data/raft/raft.db; df -h /data
    for d in /data/raft/snapshots/*/; do [ -f "$d/state.bin" ] || continue
      echo "$d $(wc -c < "$d/state.bin") bytes, version byte $(head -c 5 "$d/state.bin" | od -An -tu1 | awk "{print \$5}")"
      cat "$d/meta.json"; echo; done'
  si_kubectl logs "storage-integrity-si-v2-arbiter-$i" -c arbiter |
    grep -E 'starting restore|restored from snapshot|election won|entering (leader|follower) state' | head -6 || true
  /usr/bin/time -p sh -c "kubectl --context sentio-sea -n sentio-network-devnet2 exec storage-integrity-si-v2-arbiter-$i -c arbiter -- tar -C /data -czf - raft | wc -c"
done
for el in sentio-node-op-reth-0 sentio-node-op-reth-1; do
  for body in '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' \
              '{"jsonrpc":"2.0","id":2,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","0x1"]}'; do
    echo "$el $(si_arbiter 0 wget -qO- --header 'Content-Type: application/json' --post-data "$body" "http://$el:8545")"
  done
done
si_images
```

Expected per voter: `du`/`df` lines; for each snapshot `… bytes, version byte 4` and its `meta.json` (`Index`, `Term`, `Size`); up to six log lines (`starting restore` / `restored from snapshot` when the pod started from a snapshot, and one voter's `election won`); `real <seconds>` and the compressed byte count of the backup stream. Then two lines per archive node: `{"jsonrpc":"2.0","id":1,"result":"0x786d4d"}` and `{"jsonrpc":"2.0","id":2,"result":"0x…"}`. `si_images` lists three arbiter and three verifier pods on `ghcr.io/sentioxyz/arbiter@sha256:0a83dded…f32c`.

- [ ] **Step 3: Decide.** Stop (do not schedule stage 1) if any voter's free `/data` space is under twice its newest `state.bin`, or an archive check answers an error. Otherwise size the window as: stop (under a minute) + 3 × the measured backup stream time + the offline replay (Task 10 Step 6; about the backup time) + sync and start (about 5 minutes) + the longest measured restore + 30 minutes of verification. Record every number in the Task 13 execution record.

## Task 10: HUMAN-GATED — stage 1 Arbiter stop-all cutover

Executed by the user in the maintenance window, one step at a time. **Point of no return:** Step 7's first start of a release voter; from then on the only way back is Step 10 (before Step 9) or an incident.

**Files:** `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml`, `charts/storage-integrity/tests/test_render.py` (constant only), pushed to `main`.

**Interfaces:** Consumes `$ARBITER_RELEASE_*`, Task 3's `$R/bin/{old,new}/statedigest`, Task 9's measurements.

- [ ] **Step 1: Session and preconditions.** Run `bash`, `set -eo pipefail`, the runbook §0 helper block (Task 9 Step 1) and `git switch main && git pull --ff-only`; confirm Tasks 3, 8 and 9 are recorded and the window is announced (no INSERTs to `devnet101.swap_new2` until Step 9).
- [ ] **Step 2: Drain** (runbook §2.2).

```bash
for i in 0 1 2; do echo "== arbiter-$i"; si_metrics "$i"; done
```

Expected: all three voters print identical values with `arbiter_open_block_statements 0`, `arbiter_unanchored_verified_blocks 0`, safe = sealed, `…{role="snode",status="active"} 1`, `…{role="verifier",status="active"} 3`. Not drained within 15 minutes: stop and investigate; nothing has changed.

- [ ] **Step 3: Baseline** (runbook §2.3).

```bash
mkdir -m 700 "$si_backup"
for i in 0 1 2; do si_metrics "$i" > "$si_backup/metrics-before-$i.txt"; done
si_show_stable > "$si_backup/show-before.json"
for i in 0 1 2; do si_capability "$i"; done > "$si_backup/capability-before.txt"
for i in 0 1 2; do echo "arbiter-$i $(si_kubectl logs "storage-integrity-si-v2-arbiter-$i" -c arbiter | grep 'election won' | tail -1)"; done > "$si_backup/leader-before.txt"
si_images > "$si_backup/images-before.txt"
cat "$si_backup"/metrics-before-0.txt "$si_backup/show-before.json" "$si_backup/leader-before.txt"
```

Expected: `$si_backup` created (a pre-existing directory aborts the session: move it aside), the printed baseline metrics, one `consensus show` projection (`network_id` `devnet2`, `genesis_snapshot_id` `0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06`, `authority` `["0x9ef3a259d1d87c864431cab5ed5f6578ad5ad705"]`, `max_writers` `"1"`) and each voter's `election won` line (the leader's is non-empty).

- [ ] **Step 4: Stop every voter** (runbook §2.4).

```bash
si_kubectl scale statefulset/storage-integrity-si-v2-arbiter --replicas=0
until [ -z "$(si_arbiter_pods)" ]; do sleep 2; done
echo "all voters stopped"
```

Expected: `all voters stopped`.

- [ ] **Step 5: Back up** (runbook §2.5).

```bash
for i in 0 1 2; do
si_kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: si-arbiter-backup-$i
spec:
  automountServiceAccountToken: false
  restartPolicy: Never
  containers:
    - name: backup
      image: ghcr.io/sentioxyz/arbiter@sha256:0a83dded9bea0b5ab7b58f48a131fbc7f5a1427040579790fd4ddca75f6af32c
      command: [sleep, "86400"]
      volumeMounts:
        - {name: data, mountPath: /data, readOnly: true}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: data-storage-integrity-si-v2-arbiter-$i, readOnly: true}
EOF
done
si_kubectl wait --for=condition=Ready pod/si-arbiter-backup-0 pod/si-arbiter-backup-1 pod/si-arbiter-backup-2 --timeout=5m
for i in 0 1 2; do
  si_kubectl exec "si-arbiter-backup-$i" -- tar -C /data -czf - raft > "$si_backup/arbiter-$i-raft.tgz"
  si_kubectl exec "si-arbiter-backup-$i" -- sh -c 'cd /data && find raft -type f -exec sha256sum {} + | sort -k 2' > "$si_backup/arbiter-$i-raft.sha256"
  check="$(mktemp -d)"
  tar -C "$check" -xzf "$si_backup/arbiter-$i-raft.tgz"
  (cd "$check" && shasum -a 256 -c --quiet "$si_backup/arbiter-$i-raft.sha256")
  rm -rf "$check"
  echo "arbiter-$i: $(wc -l < "$si_backup/arbiter-$i-raft.sha256") files, $(du -h "$si_backup/arbiter-$i-raft.tgz" | cut -f1)"
  grep -E ' raft/(raft\.db|consensus-protocol\.json)$' "$si_backup/arbiter-$i-raft.sha256"
done
si_kubectl delete pod si-arbiter-backup-0 si-arbiter-backup-1 si-arbiter-backup-2 --wait
```

Expected per voter: no `shasum` failure, `arbiter-<i>: <n> files, <size>`, and the `raft/raft.db` and `raft/consensus-protocol.json` manifest lines; the helper pods are deleted. **Stop line:** any failure → `si_kubectl scale statefulset/storage-integrity-si-v2-arbiter --replicas=3` (still v0.7.1, data untouched) and end the window.

- [ ] **Step 6: Offline replay gate** (runbook §2.6).

```bash
R="$HOME/si-devnet2-5b-rehearsal"
for i in 0 1 2; do
  work="$(mktemp -d)"
  tar -C "$work" -xzf "$si_backup/arbiter-$i-raft.tgz"
  for v in old new; do
    "$R/bin/$v/statedigest" -data-dir "$work/raft" -network-id devnet2 -schema-snapshot-id devnet2-si-v2 \
      -executor-profile-id housegate-replay-mvp-v0 -max-writers 1 \
      -authority 0x9ef3a259d1d87c864431cab5ed5f6578ad5ad705 > "$si_backup/digest-$i-$v.txt" 2> "$si_backup/digest-$i-$v.err" || { echo "statedigest $v failed for arbiter-$i:"; cat "$si_backup/digest-$i-$v.err"; exit 1; }
    echo "arbiter-$i $v $(grep -E '^(snapshot_index|last_index|written_version)' "$si_backup/digest-$i-$v.txt" | tr '\n' ' ')state=$(grep -E '^(key|spent_ids)' "$si_backup/digest-$i-$v.txt" | grep -v '^key artifact_disposition ' | shasum -a 256 | cut -c1-16)"
  done
  rm -rf "$work"
done
```

Expected: six lines; per voter the `old` and `new` `state=` values are equal, `written_version 4` / `14`. **Stop line:** any per-voter difference, or any statedigest error (printed from `digest-<i>-<build>.err`) → scale back to 3 on v0.7.1 as in Step 5 and end the window.

- [ ] **Step 7: Pin the release and start.** In `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml`, replace:

```yaml
    # arbiter v0.7.1 (e654d832), pinned here as well as in the chart default so a
    # later chart pin cannot change this network's consensus runtime unreviewed.
    digest: sha256:0a83dded9bea0b5ab7b58f48a131fbc7f5a1427040579790fd4ddca75f6af32c
```

with (literal values from Task 1):

```yaml
    # arbiter $ARBITER_RELEASE_TAG ($ARBITER_RELEASE_SHA), pinned here only: the chart default stays
    # the fresh-profile v0.7.1. Voters moved by the stop-all cutover in
    # docs/storage-integrity-devnet2-dynamic-table-set.md stage 1; v0.7.1
    # cannot read this release's snapshots, so rollback is the pre-cutover
    # Raft backups, never this line alone.
    digest: $ARBITER_RELEASE_DIGEST
```

In `charts/storage-integrity/tests/test_render.py`, replace `DEVNET2_ARBITER_DIGEST = HARDENED_DIGEST` with `DEVNET2_ARBITER_DIGEST = "$ARBITER_RELEASE_DIGEST"` (literal). Then:

```bash
python3 -B charts/storage-integrity/tests/test_render.py
git add k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml charts/storage-integrity/tests/test_render.py
git commit -F - <<'EOF'
chore(devnet2): move the storage-integrity-si-v2 Arbiter to the table-registry release

Stage 1 of docs/storage-integrity-devnet2-dynamic-table-set.md (stop-all
cutover). Rollback is restoring the pre-cutover Raft backups (runbook 2.10),
valid only before intake resumes.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

Expected: `Ran 58 tests`, `OK`. Then push, sync and start (runbook §2.7; it records `stage1_commit`):

```bash
stage1_commit="$(git rev-parse HEAD)"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
si_kubectl scale statefulset/storage-integrity-si-v2-arbiter --replicas=3
until [ "$(si_arbiter_pods | wc -l)" -eq 3 ]; do sleep 2; done
si_kubectl wait --for=condition=Ready pod/storage-integrity-si-v2-arbiter-0 pod/storage-integrity-si-v2-arbiter-1 pod/storage-integrity-si-v2-arbiter-2 --timeout=15m
si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
```

Expected: `helmfile` reports `UPDATED RELEASES: storage-integrity-si-v2`; three arbiter pods Ready; `statefulset rolling update complete 3 pods` for the verifiers.

- [ ] **Step 8: Verify with intake paused** (runbook §2.8).

```bash
release_digest="$(sed -n 's/^    digest: //p' k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml)"
si_images
for i in 0 1 2; do
  echo "== arbiter-$i"
  si_kubectl logs "storage-integrity-si-v2-arbiter-$i" -c arbiter | grep -E 'starting restore|restored from snapshot|failed to restore|election won|level=ERROR' || true
  echo "marker $(si_arbiter "$i" sha256sum /data/raft/consensus-protocol.json | cut -c1-64) backup $(grep ' raft/consensus-protocol.json$' "$si_backup/arbiter-$i-raft.sha256" | cut -c1-64)"
  diff <(si_metrics "$i") "$si_backup/metrics-before-$i.txt" && echo "metrics equal the baseline"
  si_capability "$i"
done
diff <(si_show_stable) "$si_backup/show-before.json" && echo "consensus show equals the baseline"
```

Expected: six `si_images` lines on `…@$ARBITER_RELEASE_DIGEST` with `imageID` ending in `$ARBITER_RELEASE_DIGEST` or `$ARBITER_RELEASE_AMD64_DIGEST`; `restored from snapshot` for each voter that had a snapshot in Task 9 and no `failed to restore`/`level=ERROR`; one `election won`; equal marker hashes; `metrics equal the baseline` three times; three capabilities `{"node_id":"storage-integrity-si-v2-arbiter-<i>","protocol_version":1,"updates_enabled":false}`; `consensus show equals the baseline`; the verifier `level=ERROR` counts `0`. **Stop line:** any mismatch → Step 10.

- [ ] **Step 9: Resume intake** (runbook §2.9).

```bash
base_safe="$(sed -n 's/^arbiter_safe_watermark_block_seq //p' "$si_backup/metrics-before-0.txt")"
si_kubectl run si-5b-stage1-acceptance -i --restart=Never \
  --image=us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:25.8-lts-decimal512-2026-03-27@sha256:89839a322a59489fa14ca12e9d00f53d860b08c5486808f652a9484d7c247797 \
  --overrides='{"apiVersion":"v1","spec":{"imagePullSecrets":[{"name":"google-image-secret"}]}}' \
  --command -- sh -ec '
    clickhouse-local --query "SELECT toInt64(501) AS value FORMAT Native" > /tmp/row.native
    clickhouse-client --compression=0 --host sentio-node-devnet2-indexer-a --port 9011 \
      --query "INSERT INTO devnet101.swap_new2 (value) FORMAT Native" < /tmp/row.native
  '
for n in $(seq 1 90); do
  safe="$(si_metrics 0 | sed -n 's/^arbiter_safe_watermark_block_seq //p')"
  [ "$safe" -gt "$base_safe" ] && break
  sleep 10
done
for i in 0 1 2; do si_metrics "$i"; done
si_ch "SELECT count() FROM devnet101.swap_new2 WHERE value = 501 SETTINGS SQL_x_read_mode='safe'"
si_kubectl delete pod si-5b-stage1-acceptance
```

Expected: every voter's safe watermark passes the baseline and equals its sealed tip; the safe read prints `1`. From this point the cutover is final.

- [ ] **Step 10 (only on a Step 8 failure): Roll back** (runbook §2.10; `stage1_commit` comes from Step 7's session, otherwise set it to the stage 1 commit's hash).

```bash
si_kubectl scale statefulset/storage-integrity-si-v2-arbiter --replicas=0
until [ -z "$(si_arbiter_pods)" ]; do sleep 2; done
for i in 0 1 2; do
si_kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: si-arbiter-restore-$i
spec:
  automountServiceAccountToken: false
  restartPolicy: Never
  containers:
    - name: restore
      image: ghcr.io/sentioxyz/arbiter@sha256:0a83dded9bea0b5ab7b58f48a131fbc7f5a1427040579790fd4ddca75f6af32c
      command: [sleep, "86400"]
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: data-storage-integrity-si-v2-arbiter-$i}
EOF
done
si_kubectl wait --for=condition=Ready pod/si-arbiter-restore-0 pod/si-arbiter-restore-1 pod/si-arbiter-restore-2 --timeout=5m
for i in 0 1 2; do
  si_kubectl exec -i "si-arbiter-restore-$i" -- sh -ec 'rm -rf /data/raft && tar -C /data -xzf -' < "$si_backup/arbiter-$i-raft.tgz"
  si_kubectl exec -i "si-arbiter-restore-$i" -- sh -ec 'cd /data && sha256sum -c -s -' < "$si_backup/arbiter-$i-raft.sha256" && echo "arbiter-$i restored"
done
si_kubectl delete pod si-arbiter-restore-0 si-arbiter-restore-1 si-arbiter-restore-2 --wait
git show --stat "$stage1_commit"   # must be the stage 1 digest commit
git revert --no-edit "$stage1_commit"
python3 -B charts/storage-integrity/tests/test_render.py
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
si_kubectl scale statefulset/storage-integrity-si-v2-arbiter --replicas=3
until [ "$(si_arbiter_pods | wc -l)" -eq 3 ]; do sleep 2; done
si_kubectl wait --for=condition=Ready pod/storage-integrity-si-v2-arbiter-0 pod/storage-integrity-si-v2-arbiter-1 pod/storage-integrity-si-v2-arbiter-2 --timeout=15m
for i in 0 1 2; do diff <(si_metrics "$i") "$si_backup/metrics-before-$i.txt" && echo "arbiter-$i at baseline"; done
diff <(si_show_stable) "$si_backup/show-before.json" && echo "consensus show at baseline"
```

Expected: `arbiter-<i> restored` three times, the revert commit pushed, `arbiter-<i> at baseline` three times and `consensus show at baseline`. Then record the incident; do not retry in the same window.

## Task 11: HUMAN-GATED — stage 2 indexer-a sync

**Files:** `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml`, `docs/examples/storage-integrity-fresh-source-image.yaml`, `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2.yaml`, `charts/storage-integrity/tests/test_render.py` (constants only), pushed to `main` in one commit.

**Interfaces:** Consumes `$SENTIO_NODE_DIGEST`. Produces `stage2_block` (the probe table's `createdBlock`) for stage 3.

- [ ] **Step 1: Session and preconditions** (runbook §3.1). Run `bash`, `set -eo pipefail`, the runbook §0 helper block and `git switch main && git pull --ff-only`; Task 10 Step 9 passed. After `gcloud auth login` and `gcloud auth configure-docker us-west1-docker.pkg.dev`:

```bash
docker buildx imagetools inspect ghcr.io/sentioxyz/sentio-node:sha-2af43bd9906e76c91e892579ec687463ef6cfdb7 | sed -n 's/^Digest: *//p'
docker buildx imagetools inspect us-west1-docker.pkg.dev/sentio-352722/sentio/housegate-rewriter:0.15.0 | sed -n 's/^Digest: *//p'
docker buildx imagetools inspect ghcr.io/housegate/housegate:v0.15.0 | sed -n 's/^Digest: *//p'
```

```bash
si_kubectl exec sentio-node-devnet2-indexer-a-0 -c sentio-node -- wget -qO- http://sentio-node-devnet2-indexer-a-dind:2375/tasks | jq '[.[] | select(.Status.State=="running")] | length'
```

Expected: `$SENTIO_NODE_DIGEST`, `sha256:57812c8cf40603dab0fa84f8f0c1c6270990c2a74820d1450e20d65c618e4989`, `sha256:1b51de76b7b7a15a8006d0ac06febb10012c74c92d4950d07e4e5a5b77b3570f`. **Stop line:** any other digest. The last command prints the running driver task count; record it.

- [ ] **Step 2: The values commit.** Replace the whole of `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml` with (literal digest):

```yaml
# Merged LAST into indexer-a by storage-integrity-si-v2.helmfile.yaml: the
# embedded source SNode must run arbiter-core v0.8.0 or later; this pin runs
# housegate v0.15.0 and arbiter-core v0.10.1 (the host side of the dynamic SI
# table set: registry follower, table state, storage-RPC table status, genesis
# schemas by hash).
# A copy of the verified input in docs/examples/storage-integrity-fresh-source-image.yaml
# (sentio-node 2af43bd9, official main CI; provenance recorded in
# docs/storage-integrity-devnet2-dynamic-table-set.md). Keep the two identical;
# the render tests check it.
node:
  image:
    repository: ghcr.io/sentioxyz/sentio-node
    tag: "sha-2af43bd9906e76c91e892579ec687463ef6cfdb7@$SENTIO_NODE_DIGEST"
    pullPolicy: IfNotPresent
```

Replace the whole of `docs/examples/storage-integrity-fresh-source-image.yaml` with:

```yaml
# Merge LAST into a separately reviewed fresh-network sentio-node release.
# This is not a deployment file for legacy devnet2.
# sentio-node 2af43bd9: housegate v0.15.0, arbiter-core v0.10.1 and
# arbiter-proto v0.8.0 (sentio-node PRs 186, 188 and 189).
# Published by official main CI; source revision and the immutable image were
# independently verified. Run validate_source_binding.py against both renders.
node:
  image:
    repository: ghcr.io/sentioxyz/sentio-node
    tag: "sha-2af43bd9906e76c91e892579ec687463ef6cfdb7@$SENTIO_NODE_DIGEST"
    pullPolicy: IfNotPresent
```

In `charts/storage-integrity/tests/test_render.py`, replace:

```python
FRESH_SOURCE_IMAGE = (
    "ghcr.io/sentioxyz/sentio-node:sha-1494fb1cffe8c297bbc4b366b0bf260211158840"
    "@sha256:ce55024c3a2b5271a13872ffe67a0dc4023236e95bcd7649f8f8e175fb9ad30b"
)
```

with:

```python
FRESH_SOURCE_IMAGE = (
    "ghcr.io/sentioxyz/sentio-node:sha-2af43bd9906e76c91e892579ec687463ef6cfdb7"
    "@$SENTIO_NODE_DIGEST"
)
```

and `DEVNET2_SIDECAR_VERSION = "v0.14.1"` with `DEVNET2_SIDECAR_VERSION = "v0.15.0"`. In `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2.yaml`, replace:

```yaml
    # Same rewriter-grpc v0.14.0 pin as devnet2-si-v1.
    tag: "0.14.0@sha256:c04ecad6e33e5d7e45e17d6b55c30b03307dbb6f936d7c02082015fe983a5278"
```

with:

```yaml
    # rewriter-grpc v0.15.0: storage-integrity contract V2, which the embedded
    # HouseGate v0.15.0 requires at startup (Gate R4); it answers V1 requests
    # unchanged. Rollback target: 0.14.0@sha256:c04ecad6e33e5d7e45e17d6b55c30b03307dbb6f936d7c02082015fe983a5278
    tag: "0.15.0@sha256:57812c8cf40603dab0fa84f8f0c1c6270990c2a74820d1450e20d65c618e4989"
```

replace:

```yaml
  storageIntegrity:
    enabled: true
    tables:
      - devnet101.swap_new2
    read:
```

with:

```yaml
  storageIntegrity:
    # No tables list: sentio-node injects HouseGate's table state from the
    # embedded SNode (the genesis set while the arbiter's table registry is
    # disabled).
    enabled: true
    read:
```

replace:

```yaml
    # housegate v0.14.1: the signed inline INSERT ... VALUES lane
    # (docs/storage-integrity-inline-values.md), including inline rows the grpc
    # materializer renders as FORMAT Values(...). The statement envelope,
    # payload format and settings hash are unchanged from v0.12.0. Rollback
    # targets: v0.14.0@sha256:8f7aca74fbac9f59ee36f4be9eefb9ac1f6a591a95aba4e9a6412ef5db031c29,
    # v0.12.0@sha256:4efa5cc58c2f973835c7e055aaff1648b8598699b7fd25aad33943051ca812a3
    tag: "v0.14.1@sha256:c1e5b9aa32b706ee086aaa7b28f2ced1f2ad1747eae6ad781ebf09aa123041a0"
```

with:

```yaml
    # housegate v0.15.0: the agent asks the storage-node RPC for each table's
    # storage-integrity status (sentio_getStorageIntegrityTableStatus) and signs
    # only Active tables. The statement envelope, payload format and settings
    # hash are unchanged from v0.14.1. Rollback target (with a YAML
    # networkStateSource and tableSchemas):
    # v0.14.1@sha256:c1e5b9aa32b706ee086aaa7b28f2ced1f2ad1747eae6ad781ebf09aa123041a0
    tag: "v0.15.0@sha256:1b51de76b7b7a15a8006d0ac06febb10012c74c92d4950d07e4e5a5b77b3570f"
```

and replace:

```yaml
    networkStateSource: /config/housegate-agent-network-state.yaml
    inlineValues:
      enabled: true
    tableSchemas:
      - databaseID: devnet101
        tableID: swap_new2
        version: 1
        # TableSchemaHash("devnet2", schema): the on-chain declaration, unchanged.
        schemaHash: "0x37f5a671528eab2f6f3448bb093e924f67096a46f3e96155e17760e55d0ba3ed"
        schemaJSON: '{"table_id":"devnet101.swap_new2","partition_by":"","columns":[{"name":"value","type":"Int64"}]}'
```

with:

```yaml
    # This pod's storage-node JSON-RPC (node.ports.storage_rpc, bound on
    # 0.0.0.0 in the shared pod network namespace). It answers the same table
    # status the embedded HouseGate enforces, so no schemas are declared here.
    networkStateSource: http://localhost:32003
    inlineValues:
      enabled: true
```

Then:

```bash
python3 -B charts/storage-integrity/tests/test_render.py
git add k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml docs/examples/storage-integrity-fresh-source-image.yaml k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2.yaml charts/storage-integrity/tests/test_render.py
git commit -F - <<'EOF'
chore(devnet2): move indexer-a to sentio-node 2af43bd with rewriter 0.15.0 and sidecar v0.15.0

Stage 2 of docs/storage-integrity-devnet2-dynamic-table-set.md. The rewriter
moves with sentio-node (Gate R4); HouseGate takes its table set from the
embedded SNode and the agent asks the pod's storage RPC for table status.
Rollback: revert this commit and sync indexer-a.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

Expected: `Ran 58 tests`, `OK` (simulated at planning time with a stand-in digest).

- [ ] **Step 3: Sync** (runbook §3.3; it records `stage2_commit`).

```bash
stage2_commit="$(git rev-parse HEAD)"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
```

Expected: `UPDATED RELEASES: sentio-node-devnet2-indexer-a`; `statefulset rolling update complete 1 pods`. **Stop line:** the pod crash-loops → runbook §3.5.

- [ ] **Step 4: Verify** (runbook §3.4).

```bash
si_kubectl get pod sentio-node-devnet2-indexer-a-0 -o jsonpath='{range .spec.containers[*]}{.name}{"\t"}{.image}{"\n"}{end}'
si_kubectl logs sentio-node-devnet2-indexer-a-0 -c housegate-sidecar | grep 'network state loaded from RPC'
si_indexer wget -qO- http://localhost:9091/metrics | grep -E '^sentio_node_storage_integrity_(registry_enabled|registry_follower_connected|registry_version|tables)'
si_indexer wget -qO- http://localhost:9092/metrics | grep -E '^clickhouse_proxy_agent_si_table_status_failures_total' || echo "no status failures recorded"
si_rpc '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityTableStatus","params":["devnet101","swap_new2"]}' | jq -c '.result | {status, schema_hash, registry_version}'
si_rpc '{"jsonrpc":"2.0","id":2,"method":"sentio_getStorageIntegrityTableStatus","params":["devnet101","sp5b_no_such_table"]}' | jq -c '.result.status'
```

Expected (the registry follower does not log the Arbiter's `table registry is disabled` answer, so the metrics are the check): the three container images of §3.4; `network state loaded from RPC … endpoint=http://localhost:32003`; `sentio_node_storage_integrity_registry_enabled 0`, `…_registry_follower_connected 1`, `…_registry_version 0`, `…_tables{status="active"} 1`; `no status failures recorded` (or the counter at `0`); `{"status":"active","schema_hash":"0x37f5a671528eab2f6f3448bb093e924f67096a46f3e96155e17760e55d0ba3ed","registry_version":0}` and `"ordinary"`.

- [ ] **Step 5: Signed INSERT, processors, creation block** (runbook §3.4). The signed INSERT:

```bash
base_safe="$(si_metrics 0 | sed -n 's/^arbiter_safe_watermark_block_seq //p')"
si_kubectl run si-5b-stage2-acceptance -i --restart=Never \
  --image=us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:25.8-lts-decimal512-2026-03-27@sha256:89839a322a59489fa14ca12e9d00f53d860b08c5486808f652a9484d7c247797 \
  --overrides='{"apiVersion":"v1","spec":{"imagePullSecrets":[{"name":"google-image-secret"}]}}' \
  --command -- sh -ec '
    clickhouse-local --query "SELECT toInt64(502) AS value FORMAT Native" > /tmp/row.native
    clickhouse-client --compression=0 --host sentio-node-devnet2-indexer-a --port 9011 \
      --query "INSERT INTO devnet101.swap_new2 (value) FORMAT Native" < /tmp/row.native
  '
for n in $(seq 1 90); do
  safe="$(si_metrics 0 | sed -n 's/^arbiter_safe_watermark_block_seq //p')"
  [ "$safe" -gt "$base_safe" ] && break
  sleep 10
done
for i in 0 1 2; do si_metrics "$i"; done
si_ch "SELECT count() FROM devnet101.swap_new2 WHERE value = 502 SETTINGS SQL_x_read_mode='safe'"
si_kubectl delete pod si-5b-stage2-acceptance
```

Expected: the safe watermark passes `base_safe` on all voters and the safe read prints `1`. After a few minutes, rerun Step 1's last command: the running driver task count equals Step 1's. Then the creation block:

```bash
si_ch "CREATE TABLE devnet101.sp5b_created_block_probe (value Int64) ENGINE = MergeTree ORDER BY tuple()"
for n in $(seq 1 30); do
  probe="$(si_rpc '{"jsonrpc":"2.0","id":3,"method":"sentio_getDatabaseInfoById","params":["devnet101"]}' | jq -c '.result.tables[] | select(.tableId=="sp5b_created_block_probe")')"
  [ -n "$probe" ] && break
  sleep 10
done
echo "$probe"
si_ch "DROP TABLE devnet101.sp5b_created_block_probe"
```

Expected: one JSON object for `sp5b_created_block_probe` with a non-zero `createdBlock`; the DROP succeeds. Record `stage2_block`. **Stop line:** INSERT refused (733 `requires a signed INSERT`) or not promoted in 15 minutes → runbook §3.5.

- [ ] **Step 6 (only on a failure): Roll back** (runbook §3.5).

```bash
git show --stat "$stage2_commit"   # must be the stage 2 commit
git revert --no-edit "$stage2_commit"
python3 -B charts/storage-integrity/tests/test_render.py
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
```

Expected: the revert pushed, indexer-a rolled back to `1494fb1`, 0.14.0 and v0.14.1 with its YAML source; repeat Task 10 Step 9's INSERT to confirm.

## Task 12: HUMAN-GATED — stage 2b consensus administration

**Files:** `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml`, `charts/storage-integrity/tests/test_render.py` (constant only), pushed to `main`.

- [ ] **Step 1: Session and preconditions.** Run `bash`, `set -eo pipefail`, the runbook §0 helper block and `git switch main && git pull --ff-only`; Task 11 verified. Then:

```bash
for i in 0 1 2; do si_capability "$i"; done
si_images
```

Expected: three `"updates_enabled":false` lines and six pods on `ghcr.io/sentioxyz/arbiter@$ARBITER_RELEASE_DIGEST`.

- [ ] **Step 2: The values commit.** In `storage-integrity-si-v2.yaml`, replace:

```yaml
  # Enable only after every voter's direct capability probe passes, then
  # explicitly replace all Arbiter pods (OnDelete).
  consensusUpdatesEnabled: false
```

with:

```yaml
  # Enabled in stage 2b of docs/storage-integrity-devnet2-dynamic-table-set.md
  # after every voter ran the arbiter release and passed its direct capability
  # probe; every Arbiter pod was then replaced (OnDelete).
  consensusUpdatesEnabled: true
```

and in `test_render.py` `DEVNET2_CONSENSUS_UPDATES_ENABLED = False` with `DEVNET2_CONSENSUS_UPDATES_ENABLED = True`. Then:

```bash
python3 -B charts/storage-integrity/tests/test_render.py
git add k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml charts/storage-integrity/tests/test_render.py
git commit -F - <<'EOF'
chore(devnet2): enable consensus administration on storage-integrity-si-v2

Stage 2b of docs/storage-integrity-devnet2-dynamic-table-set.md: every voter
runs the arbiter release. No consensus update is committed.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

Expected: `Ran 58 tests`, `OK`.

- [ ] **Step 3: Push, sync, replace the pods one by one** (runbook §4).

```bash
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
leader="$(si_leader)"
for i in $(printf '0\n1\n2\n' | grep -vx "$leader") "$leader"; do
  si_kubectl delete pod "storage-integrity-si-v2-arbiter-$i" --wait
  until si_kubectl get pod "storage-integrity-si-v2-arbiter-$i" >/dev/null 2>&1; do sleep 2; done
  si_kubectl wait --for=condition=Ready "pod/storage-integrity-si-v2-arbiter-$i" --timeout=15m
  si_capability "$i"
  for j in 0 1 2; do si_metrics "$j" | grep sealed_block_seq; done
done
si_show | tee "$HOME/si-devnet2-si-v2-consensus-show-2b.json" | jq -c '{epoch, params_digest, authority: .current.authority_addresses, max_writers: .current.max_writers, table_registry: .current.table_registry}'
```

Expected per replaced voter (followers first, leader last): `{"node_id":"storage-integrity-si-v2-arbiter-<i>","protocol_version":1,"updates_enabled":true}` and three equal `arbiter_sealed_block_seq` values; finally `{"epoch":"0","params_digest":"…","authority":["0x9ef3a259d1d87c864431cab5ed5f6578ad5ad705"],"max_writers":"1","table_registry":null}`. **Stop line:** a pod not Ready in 15 minutes or sealed tips diverging → do not delete the next pod; investigate (the change itself is inert: pods on `false` and `true` apply the same log).

- [ ] **Step 4: Record** `$HOME/si-devnet2-si-v2-consensus-show-2b.json` in Task 13. Delete `$si_backup` and `$R` only after this step (`git -C /Users/uranuswch/Dev/sentio_xyz/arbiter worktree remove --force "$R/arbiter-old"` and `… "$R/arbiter-new"`, then `rm -rf "$R"`).

## Task 13: Close out — record the pins, merge housegate #213 and #215 (controller-only, gated)

**Files:** modify this plan (`docs/superpowers/plans/2026-09-28-si-devnet2-rollout.md`) in PR #215: add an "Execution record" subsection under "Verification record".

- [ ] **Step 1: Execution record.** In a worktree of PR #215's branch (`git -C /Users/uranuswch/Dev/housegate/housegate fetch origin && git -C /Users/uranuswch/Dev/housegate/housegate worktree add "$HOME/src/housegate-5b-docs" docs/dynamic-si-table-set-devnet2-rollout`), record: `$ARBITER_RELEASE_TAG`/`SHA`/`DIGEST`/`AMD64_DIGEST`; `$SENTIO_NODE_DIGEST`; the production PR number and merge commit; the stage 1, 2 and 2b commits on production `main`; Task 3 Steps 4–9 outputs; Task 9 measurements and the chosen window; Task 10 Step 3/8 baseline and verification; Task 11 `stage2_block`; Task 12's `consensus show`. Commit with `docs(plan): record the 5b devnet2 rollout execution` and the trailer, push to PR #215's branch.
- [ ] **Step 2: Merge #213 then #215.**

```bash
gh pr checks 213 --repo housegate/housegate && gh pr merge 213 --repo housegate/housegate --squash --delete-branch
gh pr update-branch 215 --repo housegate/housegate
gh pr checks 215 --repo housegate/housegate --watch && gh pr merge 215 --repo housegate/housegate --squash --delete-branch
```

Expected: both merged with green checks (docs only).

---

## Spec coverage

| Spec item | Where |
| --- | --- |
| §1 end state: tagged release ≥ `cf38430` on voters and verifiers | Task 1, Task 10 (P8) |
| §1 end state: indexer-a on the new sentio-node, rewriter 0.15.0, sidecar v0.15.0 with the storage-RPC source | Task 11 (`2af43bd` per the controller update, P9) |
| §1 end state: consensus administration enabled, no update | Task 12 |
| §1 end state: charts render a disabled `table_registry` with per-voter `rpc_url` | Tasks 5, 6 (P2, P3) |
| §1 end state: behaviour unchanged, genesis table signed and promoted, others Ordinary | Task 10 Step 9, Task 11 Steps 4–5 |
| R1 stop at "upgraded, registry disabled" | Tasks 10–12 stop there; runbook §5 and "Handoff to stage 3" |
| R2 stop-all cutover, backups, verifiers in the same sync | Task 10 (P5, P6), rehearsed in Task 3 |
| R3 tagged release, tag + digest pinned | Task 1, Task 10 Step 7 |
| R4 per-voter archive `rpc_url`, rendered but disabled | Tasks 5, 6; Task 9 archive check |
| R5 voters before indexer-a | Task order 10 → 11 |
| R6 Gate R4 in the same sync | Task 11 Step 2 (one commit) |
| R7 sidecar to the storage-RPC source in stage 2 | Task 4 (chart), Task 11 (P4) |
| R8 one PR for charts, direct pushes for values/images | Task 8; Tasks 10–12 (P10) |
| R9 new runbook, superseded markers | Task 7 |
| R10 human-gated stages | Tasks 9–12 |
| §4.1 chart acceptance: overlays byte-identical except the disabled block | Task 4 Step 6, Task 5 Step 5, Task 6 Step 6 |
| §4.1 si-v1 overlay still renders | `render-all.sh` includes `storage-integrity.helmfile.yaml` (both releases) |
| §4.1 L2 single-instance Services | Existing objects (P3); no change needed |
| §4.1 pre-stage measurement | Task 9 |
| §4.2–§4.4 stage steps, verification, rollback lines | Tasks 10, 11, 12 and runbook §2–§4 |
| §4.5 stage 3 (runbook only) | Runbook §5, "Handoff to stage 3" |
| §4.6 operating rules | Runbook §6 (updated for `2af43bd`) |
| §5 testing: render equivalence, disabled block, per-voter rendering; rehearsal | Tasks 4–6 tests; Task 3 |
| §6 risks | Review Focus 1–5; Task 9 sizing; Task 11 reachability checks |

## Verification record

How this plan was verified before it was written (2026-09-28, macOS arm64, Go 1.27.1, helm v4.3.0, helmfile, Docker via OrbStack, anvil from Foundry, PyYAML 6.0.3). No live devnet2 infrastructure was queried; registry reads (`docker buildx imagetools inspect`) were the only remote calls besides GitHub.

- **Chart tasks, red then green.** On a fresh production worktree of `739619f06`, a script applied exactly Task 4 Step 2's test edits (51 tests, `FAILED (failures=7)` with the listed messages), Step 4's chart and validator edits (`OK`), Task 5 Step 1 (57 tests, `FAILED (failures=13, errors=4)`), Step 3 (`OK`), Task 6 Step 1 (58 tests, `FAILED (errors=1)`) and Step 3 (`OK`). The resulting tree was byte-identical to the prototype. The three CI lint profiles and the strict lint of the actual si-v2 values passed.
- **Inertness.** `render-all.sh` rendered the ten consumer releases (devnet2: si-v2 control and indexer-a, si-v1 control and indexer-a, ordinary indexer-a, indexer-b, observer; testnet-v2: indexer-a, indexer-b, observer) before and after: identical after Tasks 4 and 5; after Task 6 only the si-v2 control render differs, exactly as Task 6 Step 6 lists.
- **Stage commits.** On top of the chart commits, the stage 1 (stand-in digest), stage 2 (stand-in sentio-node digest) and stage 2b edits of Tasks 10–12 were applied and committed locally; the suite passed after each; the render diffs were: stage 1, five Arbiter image references; stage 2, `tables` → `enabled: true`, the agent source, the dropped network-state key, three images and the indexer-a checksum; stage 2b, `consensus_updates_enabled` and the arbiter checksum. Nothing was pushed.
- **Rehearsal.** Task 3 Steps 1–8 were run as written with arbiter `cf38430` (the release candidate) in place of `$ARBITER_RELEASE_TAG`; results are the "recorded" values quoted there. Step 9's config loads and schema roots were run on the Task 6 render. Everything was stopped and removed afterwards.
- **Facts checked in code.** Arbiter `config.Load` (no env expansion, non-strict YAML in v0.7.1 and main); `table_registry` keys and defaults (`config/raw.go`); `TableRegistryConfig.Enabled()` = watcher or audit; snapshot write version 14 while the registry is off (`fsm/snapshot.go`, README); arbiter-core `RegistryFollower` (disabled registry → Ready, watch open → connected); sentio-node `standalone.go:458` storage-RPC bind; housegate v0.15.0 `IsRpcSource` and lazy RPC calls; `sentio-network` per-instance op-reth Services and archive flags; `cut-release.yml`/`docker-push.yml`/`next-version.sh`; arbiter v0.7.1 image tools (`tar`, `sha256sum -c -s`, `wget`, `od`).
- **Not run.** Anything against `sentio-sea`; the release tag itself (not cut yet); the rewriter image inspection (needs interactive `gcloud` re-authentication); sentio-node `2af43bd`'s image (CI queued); the stage 3 `cast` commands.

## Handoff to stage 3

Stage 3 (registry, auditors, watcher) is the runbook's §5; it is a separate plan. What 5b leaves in place and what stage 3 must still do:

- **Already in place after Task 12.** Every voter and verifier runs `$ARBITER_RELEASE_TAG`; consensus administration is enabled with epoch 0, authority `0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705`, `max_writers` 1 and no table-registry parameter; each voter's config already carries `table_registry` with its own archive `rpc_url` (voter 0/2 → `sentio-node-op-reth-0`, voter 1 → `sentio-node-op-reth-1`), watcher and audit off, `deploy_block` 0; indexer-a runs sentio-node `2af43bd` (I1 fixed), rewriter 0.15.0 and the sidecar on the storage-RPC source; `stage2_block` is recorded.
- **Preconditions still open.** The table-reference hardening (rewriter-go v0.14.0, rewriter-grpc v0.16.0 and its housegate release) live on devnet2, first with `tableref_guard.mode: observe`. The I1 precondition of the spec is met by stage 2's image. Confirm indexer-a is on-chain indexer 0 and that no unrelated database reports indexer id 0 (`si_indexer_id` 0 is also the zero value).
- **Values stage 3 changes.** `arbiter.tableRegistry.deployBlock` (the Databases proxy deployment block found in runbook §5, or 0), then `audit.enabled: true` on every node, then `watcher.enabled: true` on every voter, each followed by replacing every Arbiter pod (`OnDelete`); the chart refuses audit/watcher on a v0.7.1 digest and a watcher without audit. Indexer-a needs no change.
- **Governance.** One `consensus update` from the leader pod with the complete authority set, `--max-writers 1`, `--table-registry-chain-id 7892301 --table-registry-contract <Databases proxy> --table-registry-si-indexer-id 0 --table-registry-activation-block <N> --table-registry-confirmation safe`; `activation_block` after `stage2_block` and about 1800 L2 blocks after the safe head at submission; the parameter is set once. Confirm `sentio_node_storage_integrity_registry_enabled 1` on indexer-a before the safe head reaches `activation_block`.
- **Checks added by the #189 review.** Never declare a malformed schema on a genesis table's name (startup validates the latest declaration of every genesis id). Before the first genesis drop, confirm the arbiter's seeded genesis `schema_hash` for `devnet101.swap_new2` equals `payloadexec.TableSchemaHash("devnet2", <contract declaration>)`, i.e. `0x37f5a671528eab2f6f3448bb093e924f67096a46f3e96155e17760e55d0ba3ed` in both the storage RPC answer and the contract's version-1 declaration; startup fails closed if they differ.
- **Lifted by stage 2.** The operating rule "do not recreate a genesis table's name before its purge completes" applies only to images older than `2af43bd`; never roll indexer-a back past `2af43bd` once a genesis table has been dropped.
- **End-to-end check.** CREATE → retryable `pending activation` → Active → signed INSERT → promotion → DROP → retire → purge → same-name CREATE → Active, with no restart and no config edit; watch `arbiter_table_registry_audit_mismatches_total`, `arbiter_table_registry_watcher_halts_total` and `sentio_node_storage_integrity_tables{status}`.
- **Facts only the user can supply.** The Databases proxy address and deploy block (a live lookup, runbook §5), the chosen `activation_block`, and who holds the authority key for the update (it is in `storage-integrity-devnet2-si-v2-authority`, mounted in every Arbiter pod while signing is enabled).
