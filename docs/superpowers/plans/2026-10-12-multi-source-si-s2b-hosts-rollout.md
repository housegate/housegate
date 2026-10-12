# Multi-source SI network — Plan S2-B: hosts and devnet2 rollout of stage 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put the stage-2 consensus layer (Plan S2-A) to work on real hosts: sentio-node reports its own Active tables as Refused while the arbiter has quarantined its indexer and says so in `sentio_getStorageIntegrityInfo`, production documents the stage-2 runbook and retires the stage-1 coupling statements, and devnet2 is upgraded, commits the irreversible tracks activation, shows that a stuck indexer-b stalls only its own track, and exercises quarantine and release of indexer-b.

**Architecture:** sentio-node pins the stage-2 releases, maps the registry's new `quarantined_indexers` into its table state, reports the quarantined host's own Active tables as Refused with the host code `indexer_quarantined` while keeping their schema for HouseGate's journal recovery, and adds an always-present `quarantined` to the info RPC; its embedded SNode advertises `track_manifests_v1` through arbiter-core (Task 16). production lists the stage-2 sentio-node image for writer authorization, pins that the chart renders no grouping window, rewrites the coupling wording of the devnet2 helmfiles and the two earlier runbooks, and adds the stage-2 runbook (Task 17). The live rollout replays the voters' logs offline under the old and new arbiter, upgrades the verifiers, indexer-a's and indexer-b's sentio-node, then the voters one at a time, re-registers the data plane with the new leader, commits `tracks_enabled`, verifies the cutover, the first track-aware manifests and snapshot v20, holds indexer-b's track stuck while indexer-a keeps reaching safe, restores it, quarantines and releases indexer-b, and records the outcome (Task 18).

**Tech Stack:** Go 1.26 + Bazel 9.1.0/Bzlmod (sentio-node); Helm charts with `test_render.py` and helmfile (production); `kubectl`, `arbiter-admin`, the devnet2 runbook helpers and the throwaway `statedigest` / `v20check` tools (live steps).

**Spec:** housegate `docs/superpowers/specs/2026-10-10-multi-source-si-network-design.md` (main `534a8b4`) — binding: §7 (as seen from the hosts), §9 (the `INDEXER_QUARANTINED` mapping on every SI host), §10 (stage 2: `quarantined`, Refused reporting), §12, §13 stage 2, §14 sub-projects 10–11, §15–§17, as clarified by Plan S2-A's *Spec clarifications* C1–C13 (C12 and C13 above all). **Prerequisite:** Plan S2-A (`2026-10-12-multi-source-si-s2a-tracks-consensus.md`) — its Global Constraints and its Appendix A (the binding contract, `contract §N` below) apply here unchanged, and Tasks 16–18 consume its release ledger (`PROTO_TAG`, `HG_TAG`, `CORE_TAG`, `ARBITER_TAG` / `ARBITER_DIGEST`) and its exported names verbatim.

**Base revisions.** Line anchors refer to sentio-node `2db130e` (Plan S1-B's `SN_COMMIT`) and production `d9e02a534`. devnet2 as read on 2026-10-12 (Plan S2-A header): epoch 4, `si_indexers` = {0: `snode-1`, activation 5508931; 1: `snode-2`, activation 5883985}, `verifiers` = `verifier-1..3`, `max_writers` 2, `client_lanes` 256, `artifact_disposition_capability` 0, safe watermark = sealed tip = 56, open block empty; all six arbiter pods on `v0.11.0` (`sha256:7a06d389…`), both indexers on sentio-node `2db130e` (HouseGate `v0.18.0`).

## Global Constraints

- **Plan S2-A applies.** Every Global Constraint of Plan S2-A applies (activation model, strict-decoder rule, arbiter-proto numbers, feature string `track_manifests_v1`, signing domains, consensus-update rules, one owner per block, per-track progress, track-aware manifests, quarantine, rejected-part cleanup, snapshot v20, golden files, worktrees, commits, remote actions), and through it those of Plans S1-A and S1-B.
- **Host quarantine reporting (verbatim, contract §4):** while `tablestate.Registry.QuarantinedIndexers` lists this host's indexer, every incarnation `evaluate` would report Active is reported `sitable.Refused` with `RefusedCode` `indexer_quarantined` and `RefusedReason` `storage-integrity indexer <id> is quarantined`, keeping `Schema` and `SchemaHash`; `Snapshot.Schema` answers such a table (C13). Pending, Gone, Ordinary and admission-Refused answers and every other indexer's tables are unchanged. `sentio_getStorageIntegrityInfo` always carries `quarantined` (`false` unless this enrolled indexer is quarantined).
- **Client refusals on devnet2 (verbatim):** a quarantined indexer's tables are refused with ClickHouse code 392 and one of `storage_integrity: table <id> was refused: indexer_quarantined: storage-integrity indexer <n> is quarantined` (the host's table state, the usual one), `storage_integrity: table <id> is not active; the signed lane admits only active tables` (a signed INSERT from an agent whose 5 s status cache still said active) or `storage_integrity: the indexer that owns table <id> is quarantined; writes to it are refused until the indexer is released (not retryable)` (the arbiter's `INDEXER_QUARANTINED`, contract §2); checks match the message, never the code alone (`sipeerguard` also answers 392). After a release the first write may get the retryable 733 `storage_integrity: table <id> is being activated; retry shortly (retryable)`.
- **Pins:** sentio-node pins housegate `HG_TAG` and arbiter-core `CORE_TAG` three times each (`go.mod` require, `MODULE.bazel` `bazel_dep` version, `git_override` commit verified with `git -C <dep> rev-parse <tag>^{commit}`) and arbiter-proto `PROTO_TAG` in `go.mod` only (contract §4).
- **Ordering (contract §5):** offline replay check; verifiers; indexer-a's then indexer-b's sentio-node; voters one at a time (followers first, leader last); every data-plane node re-registers with the new leader; drain; tracks activation (`--tracks-enabled --max-writers 2`, irreversible). Every voter, verifier and SNode runs the stage-2 builds before `tracks_enabled` commits (the CLI's and the leader's tracks gates enforce it; runbook §6.2 also checks both host images), so both SI hosts carry `HG_TAG` through `SN_DIGEST` before a quarantine can exist.
- **production rule:** the runbook, the render-test and the comment changes go through one production PR that is inert on merge; the three image pins are single-service commits pushed to `main` after the PR merges. Every live mutation (pushes to production `main`, `helmfile sync`, `kubectl scale`, pod deletions and `rollout restart`, the agent's DDL and INSERTs, `arbiter-admin consensus update`, `indexer quarantine`, `indexer release`, PR creation and merge) requires explicit user confirmation in chat, given for that step only; the tracks activation is irreversible.
- **Runbook conventions (carried from the stage-1 runbook):** every command block is bash, run in one dedicated bash session from a production checkout at `origin/main`; one stage per window; every block that mutates anything is preceded by the line `Confirm before running.`; every cross-block value lives under `$si_backup`. Commands handed to the user in chat are fish (the user's shell); the runbook's own blocks stay bash.
- **Evidence:** `$HOME/si-devnet2-multisource-tracks` (`si_backup`) and `$HOME/si-devnet2-multisource-tracks-rehearsal` (`R`), mode 700, created fresh; the stage-1 directories `$HOME/si-devnet2-multisource*` are read (tool sources) and never written.

## Release ledger (consumed and produced)

| Name | Meaning | Resolved by |
|---|---|---|
| `PROTO_TAG`, `HG_TAG`, `CORE_TAG`, `ARBITER_TAG` / `ARBITER_DIGEST` | Plan S2-A releases | Plan S2-A |
| `SN_COMMIT` / `SN_DIGEST` | sentio-node main commit containing Task 16 and its image index digest | Task 16, release step |
| production PR | the merged Task 17 PR (stage-2 runbook, allowlisted image, coupling statements) | Task 17, Step 6 |
| `TRACKS_EPOCH`, `TRACKS_CUTOVER` | the epoch of devnet2's tracks activation and its cutover `{block_seq, promotion_seq}` | Task 18, Steps 10–11 |

References written `contract §N` or `CONTRACT §N` point to Appendix A of Plan S2-A (the binding cross-task contract); `S1 contract §N` points to Plan S1-A's Appendix A; "Contract conflicts", "Drafting decisions" or "Facts" of this group point to Appendix B below.

## Review Focus

1. **A quarantined indexer's host.** Expected: while `quarantined_indexers` lists this host's indexer, every incarnation it would report Active (ready or not) answers Refused with `refused_code` `indexer_quarantined` and `refused_reason` `storage-integrity indexer <id> is quarantined`, keeping its schema and schema hash, so `Snapshot.Schema` still resolves it for HouseGate's journal recovery and cleanup (C13); a schema that fails its self-check stays Pending; Pending, Gone, unrecorded and admission-Refused answers do not change, an admission-Refused table still carries no schema, the peer host is unaffected, and quarantine and release each reach the snapshot as one content change. Tests: Task 16 `TestLookupQuarantinedIndexerFromEachHost`, `TestRegistryIndexerView`, `TestStorageIntegrityTableStateServesAQuarantinedHostsTablesAsRefused`.
2. **The info RPC and the follower view.** Expected: `quarantined` is present in every answer (false without storage integrity, while not enrolled, and while not quarantined), true only for an enrolled, quarantined indexer; the follower's `quarantined_indexers` is copied, never aliased, nil while empty; agents ignore the new key (lenient decoding). Tests: Task 16 `TestStorageIntegrityInfoWireShape`, `TestStorageIntegrityInfoWithoutStorageIntegrity` (exact key sets), `TestStorageIntegrityTableStateServesTheInfoMethod`, `TestFromFollowerMapsEveryField`.
3. **The irreversible tracks activation on devnet2.** Expected: before it, the voters' logs replay identically under `v0.11.0` and `ARBITER_TAG` with `written_version 19` and no `key tracks`; every voter, verifier and SNode advertises `track_manifests_v1` to the current leader and both hosts run the stage-2 image when the update is submitted; the update restates `--max-writers 2` and changes only `tracks_enabled`; afterwards the cutover equals the sealed tip and `PromotionSeq` recorded right before it, both indexers' initial entries equal the cutover, the registry version does not move, and a replay writes v20 that the previous release refuses. Checks: runbook §2.3 (`old and new replay identically`), §6.1, §6.2's image guard, §6.3, §6.5 (`v20check`, `written_version 20`); tests: Task 17 `test_the_tracks_release_image_is_reviewed_before_either_pin_moves`; Plan S2-A Task 15's unchanged pre-activation integration tests.
4. **A stuck indexer-b track on devnet2.** Expected: with indexer-b stopped after its write was admitted and before that block's promotion was acknowledged, the block stays sealed and unsafe, the global prefix and indexer-b's track stay below it, indexer-a's acceptance write advances track 0's published frontier past it, and after indexer-b returns the re-sent promotion lets both converge; the global watermark is never taken as proof (the per-track helpers fail on a timeout), and a write that reached safe before the stop is detected as a miss. Checks: runbook §7.1–§7.3; tests: Plan S2-A Task 15 `TestTwoTracksStoppingOneSNodeStallsOnlyItsTrack`, Task 8 `TestCleanupWaitsForTheOwnTrackFrontier`.
5. **Quarantine and release on devnet2.** Expected: each command names the generation read from `consensus tracks` and moves it by one; the hosts see the registry change; indexer-b's table answers Refused and the owner's write and read get 392 with a quarantine message (matched on the message); indexer-a keeps reaching safe; after the release the table is Active again and writes resume (at most one retryable 733 first). Replays of old commands and the rejected-block path are proven only by tests. Checks: runbook §8; tests: Task 16 `TestLookupQuarantinedIndexerFromEachHost`; Plan S2-A Task 10 `TestQuarantineReplayAfterReleaseIsRefused`, `TestReleaseReplayAfterLaterQuarantineIsRefused`, `TestReleaseSignedBeforeAutomaticQuarantineIsRefused`, `TestDrainGateSkipsAQuarantinedTrack`; Task 15 `TestQuarantineAndReleaseOverTheWire`, `TestRejectedBlockQuarantinesAndReleaseCleansItsParts`.

## Task order and dependencies

Plan S2-A complete (`PROTO_TAG`, `HG_TAG`, `CORE_TAG`, `ARBITER_TAG` / `ARBITER_DIGEST` recorded) → 16 (sentio-node, `SN_COMMIT` / `SN_DIGEST`) → 17 (production PR) → 18 (devnet2 live). Task 16 may start as soon as `HG_TAG` and `CORE_TAG` exist (it does not need `ARBITER_TAG`); Task 17 needs `SN_COMMIT` / `SN_DIGEST` and `ARBITER_TAG` / `ARBITER_DIGEST` (the runbook's pins and the allowlisted image); Task 18 needs the merged Task 17 PR.

## File Structure

**sentio-node** (Task 16): `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock` (pins); `storageintegrityadapter/tablestate/model.go` (`Registry.QuarantinedIndexers`, `IndexerView.Quarantined`), `arbitercore.go` (mapping), `snapshot.go` (`RefusedCodeIndexerQuarantined`, the Refused report, `Snapshot.Schema`); `rpc/storage_integrity_info.go`, `standalone/storage_integrity_info.go` (`quarantined`); tests beside each (`state_test.go`, `arbitercore_test.go`, `rpc/storage_integrity_info_test.go`, `standalone/storage_integrity_table_state_test.go`); `README.md`.

**production** (Task 17): `docs/storage-integrity-devnet2-multi-source-tracks.md` (new runbook); `docs/storage-integrity-devnet2-multi-source.md`, `docs/storage-integrity-devnet2-dynamic-table-set.md` (stage rows, coupling statements, operating rules); `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml`, `k8s-sea/sentio-network-devnet2/helmfile.yaml` (coupling wording); `charts/storage-integrity/tests/test_render.py` (allowlisted image, its mirror constant, two tests, one comment).

**devnet2** (Task 18): the single-service values commits the runbook prescribes (`storage-integrity-si-v2.yaml` with `DEVNET2_ARBITER_DIGEST`; indexer-a's source-image file with `docs/examples/storage-integrity-fresh-source-image.yaml` and `FRESH_SOURCE_IMAGE`; indexer-b's source-image file with `INDEXER_B_SOURCE_IMAGE`), the outcome record PR, local evidence under `$HOME/si-devnet2-multisource-tracks*`.

## Spec coverage map

| Spec item | Tasks |
|---|---|
| §10 sentio-node stage 2: `quarantined` in the info RPC, own Active tables reported Refused while quarantined | 16 |
| §9 housegate: the `INDEXER_QUARANTINED` mapping present on every SI host before a quarantine can exist | 16 (`HG_TAG` pin), 18 (§6.2 image guard) |
| C13 a quarantined host refuses reads too, keeps the schema for recovery | 16, 17 (runbook §8, §11) |
| §12 production: no chart change (no grouping-window value), the allowlisted host image, render tests | 17 |
| §13 stage 2 steps 1–4 (data plane then voters, drain and `tracks_enabled`, a stopped indexer-b, quarantine and release) | 17 (runbook), 18 |
| §13 stage 2 step 5 (the rejected-block path) | Plan S2-A Task 15 (integration, fault injection); the runbook says so |
| §14 sub-projects 10–11 | 16, 17 |
| §15 sentio-node and production tests | 16, 17 |
| §16 risks (the second irreversible upgrade, v20) | 17 (runbook §10, §11), 18 |
| §17 third-party enrolment gate | 17 (runbook §11) |
| C12 authority rotation needs the SNode allowlist | 17 (runbook §11) |

## Follow-ups (not in this plan)

- A ClickHouse-backed two-source `chpipeline` topology (Plan S2-A follow-up); with it the stuck track and the rejected-block path could be shown on real ClickHouse without devnet2's timing race (Appendix B, G7-2).
- A live rejected-block exercise: it needs a fault-injection knob on a test network, which no component has.
- Moving both agent sidecars to a HouseGate release that keeps the client session on the 392 quarantine refusal and the 733 refusals (they stay on v0.15.0); a separate single-service change.
- Carrying the arbiter's `quarantine_reason` to the hosts (the registry snapshot carries only the ids, so the host's refused reason is the fixed `storage-integrity indexer <id> is quarantined`).
- The stage-1 follow-ups that remain (`NodeFeatureEntry` public keys and registration sequences) and Plan S2-A's (track-aware snapshot-query lane, spec §17's other gates).

---

## Task 16: sentio-node — quarantine reporting in the table state and the info RPC, release SN_COMMIT (S2)

Spec §10, stage 2: "`sentio_getStorageIntegrityInfo` … Stage 2 adds `quarantined`, and the host reports its own Active tables as Refused while quarantined." The registry follower already delivers the whole committed registry at every version; arbiter-core `CORE_TAG` adds `wire.TableRegistrySnapshot.QuarantinedIndexers` (proto `TableRegistrySnapshot.quarantined_indexers = 9`, ascending; every applied quarantine, release and automatic quarantine bumps the registry once, contract §1, §3.5), so a quarantine reaches the host as one ordinary registry version, waking the table state's refresh loop (`storageintegrityadapter/tablestate/state.go:245-275`). This task maps it into `tablestate.Registry` and `IndexerView` (contract §4), makes `evaluate`'s Active branch report the host's own tables as Refused with the host-synthesized code `indexer_quarantined` while keeping their schema and schema hash, lets `Snapshot.Schema` answer such a table so HouseGate's journal-recovery and cleanup resolver (`housegate:storage_integrity_runtime.go:234-246`) still resolves it (C13), and adds the always-present `quarantined` to `rpc.StorageIntegrityInfo`. The pins move to the stage-2 releases; the embedded SNode advertises `track_manifests_v1` through arbiter-core's `LocalNodeFeatures()` with no sentio-node code (sentio-node always hands the SNode a claim signer, Plan S1-B Task 14). Then the sentio-node release (`SN_COMMIT`, `SN_DIGEST`).

Before any quarantine nothing changes: `QuarantinedIndexers` is nil, every `IndexerView.Quarantined` is false, `evaluate` and `Snapshot.Schema` answer exactly as at `2db130e`, and the info RPC adds only `"quarantined":false`. A host still on the stage-1 image would ignore field 9 (`TableRegistrySnapshotFromPB` ignores unknown fields) and keep reporting Active; correctness would still hold, because the arbiter refuses the statements (`INDEXER_QUARANTINED`), but runbook §6.2 refuses the activation unless both hosts run this release.

**Files:**
- Modify: `go.mod` (`:9` housegate `v0.18.0`, `:14` arbiter-core `v0.13.0`, `:15` arbiter-proto `v0.10.0`), `go.sum`, `MODULE.bazel` (`bazel_dep` `:12-19`; `git_override` `:29-41` with the `# Resolved …` comments `:31`, `:38`), `MODULE.bazel.lock`
- Modify: `storageintegrityadapter/tablestate/model.go` (`Registry` `:58-73`; `IndexerView` `:87-102`; `Registry.Indexer` `:112-121`), `storageintegrityadapter/tablestate/arbitercore.go` (`followerSource.View`, after `:63`), `storageintegrityadapter/tablestate/snapshot.go` (new constant after the `tableKey` type `:13-16`; `evaluate` doc `:143-146` and `case IncarnationActive` `:157-165`; `Snapshot.Schema` `:282-289`)
- Test: `storageintegrityadapter/tablestate/state_test.go` (`TestRegistryIndexerView` `:806-829`; new `TestLookupQuarantinedIndexerFromEachHost` appended after `TestLookupIdleNonEnrolledHostIsOrdinaryEverywhere` `:941-965`), `storageintegrityadapter/tablestate/arbitercore_test.go` (`TestFromFollowerMapsEveryField` `:20-57`)
- Modify: `rpc/storage_integrity_info.go` (`StorageIntegrityInfo` `:8-30`), `standalone/storage_integrity_info.go` (`StorageIntegrityInfo` `:64-73`)
- Test: `rpc/storage_integrity_info_test.go` (`TestStorageIntegrityInfoWireShape` `:42-66`, `TestStorageIntegrityInfoWithoutStorageIntegrity` `:68-77`), `standalone/storage_integrity_table_state_test.go` (new helper `quarantinedTenantRegistry` after `withClientLanes` `:90-93`; new `TestStorageIntegrityTableStateServesAQuarantinedHostsTablesAsRefused` after `TestStorageIntegrityTableStateServesTheStatusMethod` `:243-261`; one case in `TestStorageIntegrityTableStateServesTheInfoMethod` `:270-298`)
- Modify: `README.md` (`:115`, `:119`)

**Interfaces:**
- Consumes: `CORE_TAG` — `wire.TableRegistrySnapshot.QuarantinedIndexers []uint64` (contract §2 "Package wire"; `FromPB` refuses an unsorted or duplicated list, empty → nil), `arbiter.TrackManifestsFeature`, `arbiter.LocalNodeFeatures()` = `[client_lanes_v1 signed_claims_v1 track_manifests_v1]`, the SNode's `RegisterNode` feature filter (drops only `signed_claims_v1` without a claim signer). `HG_TAG` — the `pkg/sitable` Refused doc (a Refused table may carry a schema, C13), `chproto.IndexerQuarantinedMessage`, `storageintegrity.AdmissionCodeIndexerQuarantined` (contract §2, housegate ingress). `PROTO_TAG` (a `go.mod` pin only). Existing: `tablestate.Registry`, `IndexerView`, `Registry.Indexer`, `Inputs.self()`, `evaluate`, `resolveSchema`, `Snapshot`, `cloneTable`, `State.StorageIntegrityTableStatus`; tablestate test fakes and helpers `newFakeRegistry` (`fakeRegistry.set`), `fakeReadiness`, `fakeChain`, `schemaFor`, `schemaJSON`, `stateFor`, `activeIDs`, `testLogger`, `staticFollower`, constants `testNetwork`, `siIndexer`, `otherIndexer`, `activation`; standalone helpers `newScriptedRegistryFollower`, `activeTenantRegistry`, `withClientLanes`, `testTableStateDeps`, `newTestTableStateRuntime`, `newStartupTransaction`, `storageIntegrityIngressInfoFrom`; rpc helpers `fixedStorageIntegrityInfo`, `getInfo`, `serveForTest`, `startStorageRPC`; `hgregistry.TableStatusRefused` (`housegate:pkg/registry/table_status.go:10`).
- Produces:
  - `tablestate.Registry.QuarantinedIndexers []uint64`; `tablestate.IndexerView.Quarantined bool`; `const tablestate.RefusedCodeIndexerQuarantined = "indexer_quarantined"`; the refused reason `storage-integrity indexer <id> is quarantined` (the arbiter's admission message, contract §3.5).
  - `(*tablestate.Snapshot).Schema` answers Active, Gone, and a Refused table that carries a schema (`Schema.TableID != ""`).
  - `rpc.StorageIntegrityInfo.Quarantined bool` (`json:"quarantined"`), always present.
  - Test helper `quarantinedTenantRegistry(t *testing.T) wire.TableRegistrySnapshot` (standalone tests).
  - Release `SN_COMMIT`, `SN_DIGEST`.

Notes:
- **Readiness under quarantine (Appendix B, G7-1).** The quarantine check sits after the schema resolution and before the readiness check: a schema that fails its self-check stays Pending with an Issue (contract §4), and an Active incarnation this host has not made ready yet is Refused too, because the arbiter refuses every statement of a quarantined indexer and a retryable Pending would only invite retries.
- **What Refused does on the host (C13).** HouseGate's `sitablestate` refuses reads, writes, ALTER and MV targets of a Refused table with 392 `storage_integrity: table <id> was refused: <code>: <reason>` and allows DROP (`housegate:pkg/plugins/sitablestate/plugin.go:114-123`); the rewriter and the merge supervisor drop it (they read `Active()`), `sipeerguard` keeps protecting it; the agent passes an INSERT for a refused table through unsigned (`housegate:pkg/plugins/sistatement/plugin.go:519-522`), so the server's 392 is what the client sees. After the release the first write gets the retryable 733 until the merge supervisor re-asserts the table (`housegate:storage_integrity_ingress.go:665-672`).
- **The status RPC is unchanged.** `StorageIntegrityTableStatus` (`tablestate/status.go:15-30`) already copies `refused_code` and `refused_reason` and serves `schema_json` only for Active and Gone tables; it stays so (pinned in the new test), the contract widens only `Snapshot.Schema`.
- **The SNode feature needs no code.** `LocalNodeFeatures()` of `CORE_TAG` carries `track_manifests_v1`, and sentio-node always sets `snode.Deps.ClaimSigner`, so the SNode advertises all three features once the pins move; Task 18 checks it in the leader's feature book.
- **No config, chart or CLI change.** Agents decode `sentio_getStorageIntegrityInfo` leniently (`housegate:pkg/network/rpc.go:156`, `pkg/registry/si_info.go:12-24` has no `quarantined`), so the new key changes nothing for them.

**Amendment from the PR #234 review (binding):**
- A32 (Plan S2-A Appendix A §7): `wire.TableRegistrySnapshot.QuarantinedIndexers` lists every indexer that is quarantined or released with rejected parts not yet cleaned, so `IndexerView.Quarantined` and the info RPC's `quarantined` mean "quarantined or draining"; the host keeps answering Refused until the arbiter drops it from the list. No code differs from the text below; the README paragraph and the `quarantined` field comment say so.

- [ ] **Step 1: Worktree and contract check**

```bash
git -C /Users/uranuswch/Dev/sentio_xyz/sentio-node fetch origin
bash -c '
repo=$1 runtime=$2 name=$3 branch=${4:-urwt/$2/$3} base=${5:-origin/main}
case $name in ""|[._-]*|*[!A-Za-z0-9._-]*) echo "urwt: invalid task name: $name" >&2; exit 2;; esac
if [[ $branch == pr:* || $branch == https://* ]] ||
   git -C "$repo" show-ref --quiet --verify "refs/heads/$branch" ||
   git -C "$repo" show-ref --quiet --verify "refs/remotes/origin/$branch"; then
  create=()
else
  create=(--create --base "$base")
fi
command wt -C "$repo" \
  --config-set "worktree-path = \"~/src/remotesrc/$runtime/{{ repo }}/$name\"" \
  switch "${create[@]}" "$branch" --no-cd --format=json
' urwt /Users/uranuswch/Dev/sentio_xyz/sentio-node claude multi-source-s2
```

Expected: one JSON object with `"action": "created"` whose `path` is `~/src/remotesrc/claude/sentio-node/multi-source-s2` (expanded). `W` below is that path; the Bash tool keeps no variables between calls, so prefix every command with `W=<that path>;` and the ledger values.

```bash
W=<worktree path>; CORE_TAG=<ledger>; HG_TAG=<ledger>; PROTO_TAG=<ledger>
C=/Users/uranuswch/Dev/sentio_xyz/arbiter-core; H=/Users/uranuswch/Dev/housegate/housegate; PR=/Users/uranuswch/Dev/sentio_xyz/arbiter-proto
git -C "$W" log --oneline -1
git -C "$C" fetch -q --tags && git -C "$H" fetch -q --tags && git -C "$PR" fetch -q --tags
git -C "$C" grep -n 'QuarantinedIndexers' "$CORE_TAG" -- wire/table_registry_snapshot.go
git -C "$C" grep -nE 'TrackManifestsFeature = "track_manifests_v1"|ClientLanesFeature, SignedClaimsFeature, TrackManifestsFeature' "$CORE_TAG" -- consensus.go
git -C "$C" show "$CORE_TAG":go.mod | grep 'housegate/housegate '
git -C "$H" show "$HG_TAG":pkg/chproto/client_error.go | grep -c 'func IndexerQuarantinedMessage'
git -C "$H" show "$HG_TAG":pkg/storageintegrity/arbiter_proto.go | grep -c 'ADMISSION_CODE_INDEXER_QUARANTINED'
git -C "$H" show "$HG_TAG":go.mod | grep 'sentioxyz/arbiter-proto'
git -C "$PR" show "$PROTO_TAG":proto/table_registry.proto | grep -c 'repeated uint64 quarantined_indexers = 9;'
```

Expected: HEAD is `2db130e` or a descendant; `QuarantinedIndexers` in the registry view type and its converters; both feature lines; arbiter-core's `go.mod` requires `github.com/housegate/housegate <HG_TAG>` (contract §6: arbiter-core pins `HG_TAG`); `1`; at least `1`; `github.com/sentioxyz/arbiter-proto <PROTO_TAG>`; `1`. If a name differs, stop and reconcile with Plan S2-A Tasks 1, 3 and 4 instead of improvising.

- [ ] **Step 2: Pins**

```bash
W=<worktree path>; HG_TAG=<ledger>; CORE_TAG=<ledger>; PROTO_TAG=<ledger>
HG_SHA="$(git -C /Users/uranuswch/Dev/housegate/housegate rev-parse "$HG_TAG^{commit}")"
CORE_SHA="$(git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core rev-parse "$CORE_TAG^{commit}")"
echo "$HG_SHA $CORE_SHA"
cd "$W" && GOWORK=off go get "github.com/housegate/housegate@$HG_TAG" "github.com/sentioxyz/arbiter-core@$CORE_TAG" "github.com/sentioxyz/arbiter-proto@$PROTO_TAG" && GOWORK=off go mod tidy
```

Edit `MODULE.bazel` (fill the tags without the leading `v` and the two commits printed above):

```diff
 bazel_dep(
     name = "arbiter_core",
-    version = "0.13.0",
+    version = "<CORE_TAG without v>",
 )
 bazel_dep(
     name = "housegate",
-    version = "0.18.0",
+    version = "<HG_TAG without v>",
 )
@@
 git_override(
     module_name = "housegate",
-    # Resolved Housegate v0.18.0; source is pinned by the commit below.
-    commit = "a6547cc559d52bde183f9251bddc8d77d719d75b",
+    # Resolved Housegate <HG_TAG>; source is pinned by the commit below.
+    commit = "<HG_SHA>",
     remote = "https://github.com/housegate/housegate",
 )
@@
 git_override(
     module_name = "arbiter_core",
-    # Resolved arbiter-core v0.13.0; source is pinned by the commit below.
-    commit = "e864aac3a3f72561ab00c26c90c3d1c692b63f76",
+    # Resolved arbiter-core <CORE_TAG>; source is pinned by the commit below.
+    commit = "<CORE_SHA>",
     remote = "https://github.com/sentioxyz/arbiter-core",
 )
```

arbiter-proto has no `bazel_dep` / `git_override` here (go_deps resolves it from `go.mod`, `use_repo` entry `com_github_sentioxyz_arbiter_proto` at `MODULE.bazel:76`), so `go.mod` is its only pin.

Run: `cd "$W" && bazel run @rules_go//go -- mod tidy && bazel mod tidy && bazel run //:gazelle && bazel build //... && grep -n "$HG_SHA\|$CORE_SHA" MODULE.bazel && grep -nE 'housegate/housegate |arbiter-core |arbiter-proto ' go.mod`
Expected: the build succeeds; `grep` prints both `commit = …` lines (a stale `git_override` compiles the old sources silently) and the three `go.mod` requires at `HG_TAG`, `CORE_TAG`, `PROTO_TAG`. Then `cd "$W" && bazel test //...` — Expected: PASS (the new fields and the new feature string are additive; nothing in sentio-node pins the SNode's feature list).

```bash
git -C "$W" status --short && git -C "$W" add -A && git -C "$W" commit -m "chore(deps): pin housegate $HG_TAG, arbiter-core $CORE_TAG and arbiter-proto $PROTO_TAG

Stage-2 multi-source SI builds (housegate spec 2026-10-10 §14): housegate
maps INDEXER_QUARANTINED to a non-retryable 392 and carries
track_frontiers, arbiter-core carries the registry's quarantined set,
track-aware manifests and track_manifests_v1, which the embedded SNode
now advertises. housegate and arbiter-core are pinned in go.mod,
bazel_dep and git_override ($HG_SHA, $CORE_SHA); arbiter-proto only in
go.mod.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 3: Write the failing table-state tests** (per amendment A10 (proposed as G7-1))

In `storageintegrityadapter/tablestate/state_test.go`, replace `TestRegistryIndexerView` (`:806-829`, with its doc comment) with:

```go
// TestRegistryIndexerView is housegate spec 2026-10-10 §10's per-indexer view:
// before the signed-claims activation the founding indexer is the only
// enrolled one, with the registry's own seed flag and activation block;
// afterwards each entry has its own activation block and seed, and an entry
// the arbiter lists in quarantined_indexers is Quarantined (spec §7.4).
func TestRegistryIndexerView(t *testing.T) {
	before := Registry{SIIndexerID: siIndexer, ActivationBlock: activation, Seeded: true}
	require.Equal(t, IndexerView{Enrolled: true, ActivationBlock: activation, Seeded: true}, before.Indexer(siIndexer))
	require.Equal(t, IndexerView{}, before.Indexer(otherIndexer))

	after := Registry{
		SIIndexerID: siIndexer, ActivationBlock: activation, Seeded: true,
		SIIndexers: []SIIndexer{
			{IndexerID: siIndexer, ActivationBlock: activation, Signer: "0xaa", SNodeNodeID: "snode-1"},
			{IndexerID: otherIndexer, ActivationBlock: 2000, Signer: "0xbb", SNodeNodeID: "snode-b"},
		},
		SeededIndexers: []uint64{siIndexer},
	}
	require.Equal(t, IndexerView{Enrolled: true, ActivationBlock: activation, Seeded: true, Signer: "0xaa", SNodeNodeID: "snode-1"}, after.Indexer(siIndexer))
	require.Equal(t, IndexerView{Enrolled: true, ActivationBlock: 2000, Signer: "0xbb", SNodeNodeID: "snode-b"}, after.Indexer(otherIndexer),
		"an enrolled indexer stays unseeded until its own seed commits")
	after.SeededIndexers = []uint64{siIndexer, otherIndexer}
	require.True(t, after.Indexer(otherIndexer).Seeded)

	after.QuarantinedIndexers = []uint64{otherIndexer}
	require.Equal(t, IndexerView{Enrolled: true, ActivationBlock: 2000, Seeded: true, Signer: "0xbb", SNodeNodeID: "snode-b", Quarantined: true},
		after.Indexer(otherIndexer))
	require.False(t, after.Indexer(siIndexer).Quarantined, "only the listed indexer is quarantined")
	require.Equal(t, IndexerView{}, after.Indexer(42), "an indexer without an entry is not enrolled and never quarantined")
	after.QuarantinedIndexers = nil
	require.False(t, after.Indexer(otherIndexer).Quarantined, "a release empties the list")
}
```

Append to the same file, after `TestLookupIdleNonEnrolledHostIsOrdinaryEverywhere`:

```go
// TestLookupQuarantinedIndexerFromEachHost covers housegate spec 2026-10-10
// §7.4 and §10 with clarification C13, in the style of
// TestLookupTwoIndexerRegistryFromEachHost: while the arbiter has
// quarantined indexer B, B's host reports every table it would report Active
// (ready or not) as Refused with the host's code indexer_quarantined, keeping
// the schema and its hash so journal recovery and cleanup still resolve the
// table. A schema that fails its self-check stays Pending; Pending, Gone,
// unrecorded and admission-Refused answers do not change, and A's host is
// unaffected. Quarantine and release reach the host as registry versions.
func TestLookupQuarantinedIndexerFromEachHost(t *testing.T) {
	const (
		indexerA, indexerB       = uint64(0), uint64(1)
		aDatabase, bDatabase     = "devnet101", "devuser1"
		activationA, activationB = uint64(1000), uint64(2000)
	)
	owned := func(seq uint64, database, table string, status IncarnationStatus, owner uint64) Incarnation {
		schema := schemaFor(database + "." + table)
		return Incarnation{Seq: seq, DatabaseID: database, TableID: table, Origin: OriginChain, Status: status,
			SchemaJSON: schemaJSON(t, schema), SchemaHash: payloadexec.TableSchemaHash(testNetwork, schema), OwnerIndexerID: &owner}
	}
	refusedAtAdmission := owned(6, bDatabase, "b_refused", IncarnationRefused, indexerB)
	refusedAtAdmission.RefusedCode, refusedAtAdmission.RefusedReason = "column_type", "bad type"
	badHash := owned(7, bDatabase, "b_bad_hash", IncarnationActive, indexerB)
	badHash.SchemaHash = "0xdeadbeef"
	registry := func(version uint64, quarantined ...uint64) Registry {
		return Registry{
			Version: version, Seeded: true, SIIndexerID: indexerA, ActivationBlock: activationA,
			SIIndexers: []SIIndexer{
				{IndexerID: indexerA, ActivationBlock: activationA, Signer: "0xaa", SNodeNodeID: "snode-1"},
				{IndexerID: indexerB, ActivationBlock: activationB, Signer: "0xbb", SNodeNodeID: "snode-2"},
			},
			SeededIndexers:      []uint64{indexerA, indexerB},
			QuarantinedIndexers: quarantined,
			Incarnations: []Incarnation{
				owned(1, aDatabase, "a_active", IncarnationActive, indexerA),
				owned(2, bDatabase, "b_active", IncarnationActive, indexerB),
				owned(3, bDatabase, "b_unready", IncarnationActive, indexerB),
				owned(4, bDatabase, "b_pending", IncarnationPending, indexerB),
				owned(5, bDatabase, "b_retiring", IncarnationRetiring, indexerB),
				refusedAtAdmission,
				badHash,
			},
		}
	}
	chain := &fakeChain{dbs: map[string]ChainDatabase{
		aDatabase: {IndexerID: indexerA, Tables: map[string]ChainTable{"a_active": {CreatedBlock: 1500}}},
		bDatabase: {IndexerID: indexerB, Tables: map[string]ChainTable{
			"b_active": {CreatedBlock: 2500}, "b_unready": {CreatedBlock: 2500}, "b_pending": {CreatedBlock: 2500},
			"b_refused": {CreatedBlock: 2500}, "b_bad_hash": {CreatedBlock: 2500}, "b_new": {CreatedBlock: 2500},
		}},
	}}
	ready := &fakeReadiness{}
	for _, id := range []string{aDatabase + ".a_active", bDatabase + ".b_active", bDatabase + ".b_bad_hash"} {
		ready.set(id, true)
	}

	quarantined := registry(60, indexerB)
	fromB := stateFor(t, indexerB, quarantined, ready, chain)
	for table, want := range map[string]sitable.Status{
		"b_active":   sitable.Refused,
		"b_unready":  sitable.Refused, // local readiness does not matter while quarantined
		"b_pending":  sitable.Pending,
		"b_retiring": sitable.Gone,
		"b_refused":  sitable.Refused,
		"b_bad_hash": sitable.Pending, // the schema self-check fails before the quarantine is consulted
		"b_new":      sitable.Pending, // unrecorded: default deny
	} {
		require.Equalf(t, want, fromB.Lookup(bDatabase, table).Status, "host B, %s", table)
	}
	require.Equal(t, sitable.Ordinary, fromB.Lookup(aDatabase, "a_active").Status, "A's table stays Ordinary on B")
	require.Empty(t, activeIDs(fromB), "a quarantined host serves no Active table")

	got := fromB.Lookup(bDatabase, "b_active")
	require.Equal(t, "indexer_quarantined", got.RefusedCode)
	require.Equal(t, RefusedCodeIndexerQuarantined, got.RefusedCode)
	require.Equal(t, "storage-integrity indexer 1 is quarantined", got.RefusedReason)
	require.Equal(t, bDatabase+".b_active", got.Schema.TableID, "the Refused entry keeps the table's schema")
	require.Equal(t, payloadexec.TableSchemaHash(testNetwork, schemaFor(bDatabase+".b_active")), got.SchemaHash)
	require.Equal(t, payloadexec.TableSchemaHash(testNetwork, got.Schema), got.SchemaHash)
	recovered, ok := fromB.Schema(bDatabase + ".b_active")
	require.True(t, ok, "journal recovery and cleanup still resolve a quarantined host's table (C13)")
	require.Equal(t, got, recovered)
	require.Equal(t, "column_type", fromB.Lookup(bDatabase, "b_refused").RefusedCode, "an admission refusal keeps the arbiter's code")
	_, ok = fromB.Schema(bDatabase + ".b_refused")
	require.False(t, ok, "a table the arbiter refused at admission carries no schema")
	gone, ok := fromB.Schema(bDatabase + ".b_retiring")
	require.True(t, ok)
	require.Equal(t, sitable.Gone, gone.Status)

	fromA := stateFor(t, indexerA, quarantined, ready, chain)
	require.Equal(t, sitable.Active, fromA.Lookup(aDatabase, "a_active").Status, "B's quarantine leaves A's tables Active")
	require.Equal(t, []string{aDatabase + ".a_active"}, activeIDs(fromA))
	for _, table := range []string{"b_active", "b_pending", "b_new"} {
		require.Equalf(t, sitable.Ordinary, fromA.Lookup(bDatabase, table).Status, "host A, %s", table)
	}
	peerQuarantined := stateFor(t, indexerB, registry(61, indexerA), ready, chain)
	require.Equal(t, sitable.Active, peerQuarantined.Lookup(bDatabase, "b_active").Status, "A's quarantine leaves B's tables Active")

	// The quarantine and the release reach the host as registry versions.
	self := indexerB
	reg := newFakeRegistry(true, registry(59))
	state, err := New(Config{Registry: reg, Readiness: ready, Chain: chain, NetworkID: testNetwork, Self: &self, Logger: testLogger(t)})
	require.NoError(t, err)
	require.NoError(t, state.Refresh(context.Background()))
	require.Equal(t, sitable.Active, state.Current().Lookup(bDatabase, "b_active").Status)
	version := state.Current().Version()
	reg.set(true, quarantined)
	require.NoError(t, state.Refresh(context.Background()))
	require.Equal(t, version+1, state.Current().Version(), "the quarantine is a content change")
	status := state.StorageIntegrityTableStatus(bDatabase, "b_active")
	require.Equal(t, "refused", status.Status)
	require.Equal(t, RefusedCodeIndexerQuarantined, status.RefusedCode)
	require.Equal(t, "storage-integrity indexer 1 is quarantined", status.RefusedReason)
	require.EqualValues(t, 60, status.RegistryVersion)
	require.Empty(t, status.SchemaJSON, "the status method serves a schema only for Active and Gone tables, as before")
	reg.set(true, registry(62))
	require.NoError(t, state.Refresh(context.Background()))
	require.Equal(t, version+2, state.Current().Version(), "the release is a content change")
	require.Equal(t, sitable.Active, state.Current().Lookup(bDatabase, "b_active").Status)
	require.Equal(t, []string{bDatabase + ".b_active"}, activeIDs(state.Current()))
}
```

In `storageintegrityadapter/tablestate/arbitercore_test.go`, replace the first statements of `TestFromFollowerMapsEveryField` (`:21-38`, from `source := FromFollower(` through the closing `}, got)`) with:

```go
	// quarantined_indexers names only enrolled indexers on a real arbiter; this
	// test checks the mapping alone.
	snapshot := wire.TableRegistrySnapshot{
		Params:  arbiter.TableRegistryParams{ChainID: 1, SIIndexerID: 7, ActivationBlock: 1000, Confirmation: "safe"},
		Version: 12,
		Seeded:  true,
		Incarnations: []wire.TableIncarnation{{
			Seq: 3, DatabaseID: "tenant", TableID: "t", Origin: wire.TableOriginChain, Status: wire.TableStatusRefused,
			SchemaJSON: "{}", SchemaHash: "0x01", RefusedCode: "column_type", RefusedReason: "bad type",
		}},
		QuarantinedIndexers: []uint64{7},
	}
	source := FromFollower(staticFollower{enabled: true, snapshot: snapshot})
	got, enabled := source.View()
	require.True(t, enabled)
	require.Equal(t, Registry{
		Version: 12, Seeded: true, SIIndexerID: 7, ActivationBlock: 1000,
		Incarnations: []Incarnation{{
			Seq: 3, DatabaseID: "tenant", TableID: "t", Origin: OriginChain, Status: IncarnationRefused,
			SchemaJSON: "{}", SchemaHash: "0x01", RefusedCode: "column_type", RefusedReason: "bad type",
		}},
		QuarantinedIndexers: []uint64{7},
	}, got)
	snapshot.QuarantinedIndexers[0] = 8
	require.Equal(t, []uint64{7}, got.QuarantinedIndexers, "the view must not alias the follower's shared snapshot")
	unquarantined, _ := FromFollower(staticFollower{enabled: true, snapshot: wire.TableRegistrySnapshot{Version: 13}}).View()
	require.Nil(t, unquarantined.QuarantinedIndexers, "nil while no indexer is quarantined")
```

(the status and origin loops, `source.Connected()` and the disabled-follower check that follow stay unchanged).

Run: `cd "$W" && go test ./storageintegrityadapter/tablestate/ -run 'TestRegistryIndexerView|TestLookupQuarantinedIndexerFromEachHost|TestFromFollowerMapsEveryField' -v`
Expected: FAIL to compile — `unknown field QuarantinedIndexers in struct literal of type Registry`, `unknown field Quarantined in struct literal of type IndexerView`, `undefined: RefusedCodeIndexerQuarantined`. (With the two fields and the constant declared but nothing else, `b_active` and `b_unready` still answer `active` / `pending` on B, `Registry.Indexer` never sets `Quarantined`, and the mapping test sees a nil list.)

- [ ] **Step 4: Implement the quarantined set and the Refused report** (per amendment A10 (proposed as G7-1))

`storageintegrityadapter/tablestate/model.go` — in `Registry`, between `SeededIndexers` and `ClientLanes` (`:69-70`):

```go
	// QuarantinedIndexers lists, ascending, the SI indexers the arbiter has
	// quarantined (housegate spec 2026-10-10 §7.4; arbiter-proto
	// TableRegistrySnapshot.quarantined_indexers). nil while none is, which is
	// always the case before the tracks activation.
	QuarantinedIndexers []uint64
```

in `IndexerView`, after `SNodeNodeID` (`:101`):

```go
	// Quarantined: the arbiter has quarantined this enrolled indexer
	// (QuarantinedIndexers; spec 2026-10-10 §7.4, §10). Its host then reports
	// its own Active tables as Refused, and the arbiter refuses their
	// statements with INDEXER_QUARANTINED until an authority-signed release.
	// False for an indexer that is not enrolled.
	Quarantined bool
```

and in `Registry.Indexer`, the entry literal (`:114-120`) gains the field:

```go
			return IndexerView{
				Enrolled:        true,
				ActivationBlock: entry.ActivationBlock,
				Seeded:          slices.Contains(r.SeededIndexers, id),
				Signer:          entry.Signer,
				SNodeNodeID:     entry.SNodeNodeID,
				Quarantined:     slices.Contains(r.QuarantinedIndexers, id),
			}
```

The founding branch (`:106-111`) stays as it is: a quarantine needs the tracks activation, which needs `si_indexers`, so that branch never sees one.

`storageintegrityadapter/tablestate/arbitercore.go` — after `out.SeededIndexers = append([]uint64(nil), snapshot.SeededIndexers...)` (`:63`):

```go
	out.QuarantinedIndexers = append([]uint64(nil), snapshot.QuarantinedIndexers...)
```

`storageintegrityadapter/tablestate/snapshot.go` — after the `tableKey` type (`:13-16`):

```go
// RefusedCodeIndexerQuarantined is the refused_code this host reports for its
// own tables while the arbiter has quarantined its indexer (housegate spec
// 2026-10-10 §7.4, §10, clarification C13). The host synthesizes it; it is not
// part of the arbiter's admission-rules-v1 refused_code vocabulary.
const RefusedCodeIndexerQuarantined = "indexer_quarantined"
```

replace the doc comment of `evaluate` (`:143-146`) with:

```go
// evaluate applies steps 3 and 4 of the Lookup order to one table, given its
// live incarnation (nil when the registry has none) and what the registry
// says about this indexer, including whether the arbiter has quarantined it.
// The table is governed, or its live incarnation is neither Legacy nor Purged.
```

and its `case IncarnationActive:` (`:157-165`) with:

```go
		case IncarnationActive:
			schema, err := resolveSchema(in, *inc)
			if err != nil {
				return sitable.Table{ID: id, Status: sitable.Pending}, &Issue{TableID: id, Reason: err.Error()}
			}
			if me.Quarantined {
				// The arbiter quarantined this indexer (housegate spec
				// 2026-10-10 §7.4, §10): its own Active tables answer Refused,
				// which refuses reads as well as writes, including the
				// unsafe_latest reads that would serve its unverified data
				// (clarification C13). The entry keeps the schema and its hash
				// so journal recovery and cleanup still resolve the table
				// (Snapshot.Schema). Local readiness does not matter: the
				// arbiter refuses every statement of a quarantined indexer.
				return sitable.Table{
					ID:            id,
					Status:        sitable.Refused,
					RefusedCode:   RefusedCodeIndexerQuarantined,
					RefusedReason: fmt.Sprintf("storage-integrity indexer %d is quarantined", in.self()),
					Schema:        schema,
					SchemaHash:    inc.SchemaHash,
				}, nil
			}
			if in.Ready == nil || !in.Ready(id) {
				return sitable.Table{ID: id, Status: sitable.Pending}, nil
			}
			return sitable.Table{ID: id, Status: sitable.Active, Schema: schema, SchemaHash: inc.SchemaHash}, nil
```

and `Snapshot.Schema` (`:282-289`) with:

```go
// Schema answers a table that carries a schema: Active and Gone tables, and a
// Refused table of a quarantined host, which keeps its schema so HouseGate's
// journal recovery and cleanup still resolve it (housegate spec 2026-10-10
// clarification C13). A table the arbiter refused at admission carries none.
func (s *Snapshot) Schema(id string) (sitable.Table, bool) {
	database, table, _ := strings.Cut(id, ".")
	t, ok := s.content.tables[tableKey{database, table}]
	if !ok {
		return sitable.Table{}, false
	}
	switch {
	case t.Status == sitable.Active, t.Status == sitable.Gone:
	case t.Status == sitable.Refused && t.Schema.TableID != "":
	default:
		return sitable.Table{}, false
	}
	return cloneTable(t), true
}
```

`content.equal` already compares `Status`, `RefusedCode`, `RefusedReason`, `SchemaHash` and `Schema.TableID` (`:226-243`), so a quarantine and a release each publish a new snapshot version; nothing else changes.

Run: `cd "$W" && gofmt -l storageintegrityadapter/ ; go test ./storageintegrityadapter/tablestate/ -v 2>&1 | tail -40`
Expected: no file listed by `gofmt`; PASS — the three tests above and every existing one (`TestLookupMatrix` checks no Schema of an admission-Refused table; `TestStorageIntegrityTableStatusShape` still sees `schema_json` empty for `r`; `TestLookupTwoIndexerRegistryFromEachHost` and the metrics tests carry no quarantine).

```bash
git -C "$W" add storageintegrityadapter && git -C "$W" commit -m "feat(storage-integrity): report a quarantined indexer's own Active tables as Refused

The table state maps the arbiter registry's quarantined_indexers into
Registry and IndexerView (housegate spec 2026-10-10 §7.4, §10). While
its indexer is quarantined, a host reports every table it would report
Active, ready or not, as Refused with the host code indexer_quarantined
and the reason 'storage-integrity indexer <id> is quarantined', keeping
the schema and its hash; Snapshot.Schema answers such a table so
HouseGate's journal recovery and cleanup still resolve it (C13). A
schema that fails its self-check stays Pending; Pending, Gone,
unrecorded and admission-Refused answers and the peer's tables do not
change.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Write the failing RPC and runtime tests**

`rpc/storage_integrity_info_test.go` — replace `TestStorageIntegrityInfoWireShape` (`:42-66`, with its doc comment) with:

```go
// TestStorageIntegrityInfoWireShape pins the exact JSON object of housegate
// spec 2026-10-09 §6.7 with the 2026-10-10 additions (enrolled, quarantined):
// every field present, nothing else; the handler, not the source, owns
// self_indexer_id and server_unix_time.
func TestStorageIntegrityInfoWireShape(t *testing.T) {
	siIndexer, wrongSelf := uint64(0), uint64(99)
	env := &common.NodeEnv{IndexerRuntime: &common.IndexerRuntime{IndexerId: big.NewInt(0)}}
	server, err := NewStorageNodeRPCServer(nil, env, WithStorageIntegrityInfo(fixedStorageIntegrityInfo{
		Enabled: true, NetworkID: "devnet2", SIIndexerID: &siIndexer, SelfIndexerID: &wrongSelf, Enrolled: true, Quarantined: true,
		IngressMaxPayloadBytes: 67108864, MaxTokenAgeSeconds: 60, DefaultReadMode: "unsafe_latest",
		RegistryVersion: 42, ServerUnixTime: 1,
	}))
	require.NoError(t, err)
	url := serveForTest(t, server)

	got, before, serverTime, after := getInfo(t, url)
	require.GreaterOrEqual(t, serverTime, before)
	require.LessOrEqual(t, serverTime, after)
	require.Equal(t, map[string]any{
		"enabled": true, "network_id": "devnet2", "keeper_shard_id": float64(0),
		"si_indexer_id": float64(0), "self_indexer_id": float64(0),
		"client_lanes_enabled": false, "ingress_max_payload_bytes": float64(67108864),
		"max_token_age_seconds": float64(60), "default_read_mode": "unsafe_latest",
		"registry_version": float64(42), "enrolled": true, "quarantined": true,
	}, got)
}
```

and in `TestStorageIntegrityInfoWithoutStorageIntegrity` (`:70-75`) the expected map's last line becomes `"registry_version": float64(0), "enrolled": false, "quarantined": false,`.

`standalone/storage_integrity_table_state_test.go` — after `withClientLanes` (`:90-93`) add:

```go
// quarantinedTenantRegistry is activeTenantRegistry after the signed-claims
// and tracks activations with its founding indexer 7 quarantined (housegate
// spec 2026-10-10 §7.4): the quarantine is one more registry version.
func quarantinedTenantRegistry(t *testing.T) wire.TableRegistrySnapshot {
	t.Helper()
	snapshot := activeTenantRegistry(t)
	snapshot.Version = 6
	snapshot.SIIndexers = []arbiter.SIIndexerEntry{{IndexerID: 7, ActivationBlock: 100, Signer: "0x00000000000000000000000000000000000000aa", SNodeNodeID: "snode-1", EnrollmentJWS: "x"}}
	snapshot.SeededIndexers = []uint64{7}
	snapshot.QuarantinedIndexers = []uint64{7}
	return snapshot
}
```

after `TestStorageIntegrityTableStateServesTheStatusMethod` (`:243-261`) add:

```go
// TestStorageIntegrityTableStateServesAQuarantinedHostsTablesAsRefused pins
// housegate spec 2026-10-10 §10 and clarification C13 through the runtime
// wiring: the status method, the info method and the snapshot HouseGate reads
// all see this indexer's quarantine, and the snapshot keeps the table's schema
// for journal recovery and cleanup.
func TestStorageIntegrityTableStateServesAQuarantinedHostsTablesAsRefused(t *testing.T) {
	runtime := newTestTableStateRuntime(t, newScriptedRegistryFollower(quarantinedTenantRegistry(t), true, true))
	tx := newStartupTransaction(t.Context())
	t.Cleanup(tx.stopAndWait)
	require.NoError(t, runtime.begin(tx.Context(), tx))

	service := rpc.NewStorageNodeService(nil, nil, runtime.storageRPCOptions()...)
	status, err := service.GetStorageIntegrityTableStatus(t.Context(), "tenant", "t")
	require.NoError(t, err)
	require.Equal(t, hgregistry.TableStatusRefused, status.Status)
	require.Equal(t, tablestate.RefusedCodeIndexerQuarantined, status.RefusedCode)
	require.Equal(t, "storage-integrity indexer 7 is quarantined", status.RefusedReason)
	require.EqualValues(t, 6, status.RegistryVersion)
	info, err := service.GetStorageIntegrityInfo(t.Context())
	require.NoError(t, err)
	require.True(t, info.Enrolled)
	require.True(t, info.Quarantined)
	table, ok := runtime.tableState().Current().Schema("tenant.t")
	require.True(t, ok, "HouseGate's journal recovery and cleanup still resolve the quarantined host's table")
	require.Equal(t, sitable.Refused, table.Status)
	require.NotEmpty(t, table.SchemaHash)
}
```

and add this case to the table of `TestStorageIntegrityTableStateServesTheInfoMethod`, after `"registry enabled with client lanes"` (`:287-292`):

```go
		"registry enabled, this indexer quarantined": {
			follower:       newScriptedRegistryFollower(quarantinedTenantRegistry(t), true, true),
			ingressEnabled: true,
			want: rpc.StorageIntegrityInfo{Enabled: true, NetworkID: "net", SIIndexerID: &siIndexer, Enrolled: true, Quarantined: true,
				IngressMaxPayloadBytes: 1 << 20, MaxTokenAgeSeconds: 90, DefaultReadMode: "safe", RegistryVersion: 6},
		},
```

Run: `cd "$W" && go test ./rpc/ -run 'StorageIntegrityInfo' -v; go test ./standalone/ -run 'TestStorageIntegrityTableStateServes' -v`
Expected: FAIL to compile — `unknown field Quarantined in struct literal of type fixedStorageIntegrityInfo` in `rpc`, `unknown field Quarantined in struct literal of type rpc.StorageIntegrityInfo` and `info.Quarantined undefined` in `standalone`. With the field declared but not set, the new info case and the runtime test fail on `quarantined` false while the status method already answers `refused` (Step 4).

- [ ] **Step 6: Implement the RPC field**

`rpc/storage_integrity_info.go` — the type comment's last sentence (`:11-12`) becomes "SIIndexerID is this node's indexer id while it is enrolled, else null; Quarantined is true while the arbiter has quarantined this enrolled indexer." and after `Enrolled` (`:23`) add:

```go
	// Quarantined reports whether the arbiter has quarantined this enrolled
	// indexer (housegate spec 2026-10-10 §7.4, §10): its own Active tables
	// then answer Refused with refused_code indexer_quarantined until the
	// authority releases it. Always present; false while not enrolled.
	Quarantined bool `json:"quarantined"`
```

(gofmt realigns the struct). `standalone/storage_integrity_info.go` (`:64-73`) becomes:

```go
	if view, enabled := tablestate.FromFollower(r.follower).View(); enabled {
		info.RegistryVersion = view.Version
		// si_indexer_id names this indexer while it is enrolled (housegate
		// spec 2026-10-10 §10); a host that is not enrolled serves no SI table.
		if me := view.Indexer(r.self); me.Enrolled {
			id := r.self
			info.SIIndexerID = &id
			info.Enrolled = true
			// Its own Active tables answer Refused meanwhile (spec §7.4, §10).
			info.Quarantined = me.Quarantined
		}
	}
```

Run: `cd "$W" && go test ./rpc/ ./standalone/ -run 'StorageIntegrityInfo|StorageIntegrityTableState|RegistryActivation|ClientLanes|SelfIndexer' -v 2>&1 | tail -40`
Expected: PASS, including the unchanged `TestStorageIntegrityInfoSelfIndexerIDIsNullUnlessResolved`, `TestHostedDiscoveryMatchesHousegateClient` (housegate's client ignores the new key) and `TestStorageIntegrityInfoReportsThisIndexersEnrollment`.

```bash
git -C "$W" add rpc standalone && git -C "$W" commit -m "feat(storage-integrity): report quarantined in sentio_getStorageIntegrityInfo

The info method always carries quarantined: true while the arbiter has
quarantined this enrolled indexer (housegate spec 2026-10-10 §7.4, §10),
read from the same registry view as enrolled. The status method answers
the quarantined host's tables as refused with refused_code
indexer_quarantined, and the snapshot HouseGate reads keeps their schema.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 7: README**

`README.md` (each paragraph stays on one line):
- `:115`: replace "- **Refused** with the arbiter's `refused_code` and `refused_reason`; **Active** otherwise; **Gone** while the incarnation is Retiring or Purging." with "- **Refused** with the arbiter's `refused_code` and `refused_reason`, and, while the arbiter has quarantined this indexer (housegate spec 2026-10-10 §7.4, §10), for every table it would otherwise report Active, with the host's `refused_code` `indexer_quarantined` and `refused_reason` `storage-integrity indexer <id> is quarantined`: reads and writes of those tables are refused with the non-retryable 392, the entry keeps the table's schema so HouseGate's journal recovery and cleanup still resolve it, and after the release the first write may get the retryable `is being activated` refusal until the merge guard re-asserts the table; **Active** otherwise; **Gone** while the incarnation is Retiring or Purging."
- `:119`: replace "`sentio_getStorageIntegrityInfo` reports `si_indexer_id` as this indexer's id while it is enrolled, else null, and `enrolled`." with "`sentio_getStorageIntegrityInfo` reports `si_indexer_id` as this indexer's id while it is enrolled, else null, `enrolled`, and `quarantined` (true while the arbiter has quarantined this enrolled indexer, from the registry's `quarantined_indexers`)."

- [ ] **Step 8: Full verification and commit**

Run: `cd "$W" && test -z "$(gofmt -l $(git ls-files '*.go'))" && bazel run //:gazelle && bazel build //... && bazel test //... --test_output=errors`
Expected: no unformatted file; all PASS (the CH-bound tests skip without `SENTIO_SI_CH_E2E`; the PR's `integration-clickhouse` job runs them).

```bash
git -C "$W" add -A && git -C "$W" commit -m "docs(readme): quarantined hosts report their tables as Refused; quarantined in the info method

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 9: PR and merge (requires explicit user confirmation)**

sentio-node always goes through a PR (`CLAUDE.md` "Pull Request Workflow"). Ask before each remote action. Then:

```bash
cd "$W" && git push -u origin urwt/claude/multi-source-s2
gh pr create --repo sentioxyz/sentio-node --base main --head urwt/claude/multi-source-s2 \
  --title 'feat(storage-integrity): multi-source SI stage 2 host side (quarantine reporting)' \
  --body "$(cat <<'EOF'
Host side of stage 2 of the multi-source storage-integrity network (housegate spec `docs/superpowers/specs/2026-10-10-multi-source-si-network-design.md` §7.4, §10).

- Pins housegate `HG_TAG`, arbiter-core `CORE_TAG`, arbiter-proto `PROTO_TAG` (go.mod, and bazel_dep + git_override for housegate and arbiter-core). The embedded SNode now advertises `track_manifests_v1` through arbiter-core.
- The table state maps the registry's `quarantined_indexers`; while this host's indexer is quarantined, every table it would report Active answers Refused with `refused_code` `indexer_quarantined`, keeping its schema so HouseGate's journal recovery and cleanup still resolve it (clarification C13).
- `sentio_getStorageIntegrityInfo` always carries `quarantined`.

Nothing changes before a quarantine, and a quarantine needs the arbiter's tracks activation.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Wait for `build` and `integration-clickhouse` (`gh pr checks <n> --repo sentioxyz/sentio-node --watch`), then merge after review and an explicit yes (`gh pr merge <n> --repo sentioxyz/sentio-node --squash`).

- [ ] **Step 10: Record `SN_COMMIT` and `SN_DIGEST`**

`SN_COMMIT` is the merge commit on `main` (`gh pr view <n> --repo sentioxyz/sentio-node --json mergeCommit --jq .mergeCommit.oid`, 40 hex). Wait for that push's `docker-push` job (`gh run list --repo sentioxyz/sentio-node --branch main --limit 3`; `gh run watch <id> --repo sentioxyz/sentio-node`), which tags `ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>` (`.github/workflows/ci.yml:189`, `type=sha,format=long` at `:221`). Then:

```bash
SN_COMMIT=<sha>; HG_TAG=<ledger>; CORE_TAG=<ledger>
docker buildx imagetools inspect "ghcr.io/sentioxyz/sentio-node:sha-$SN_COMMIT" | sed -n '1,4p'
docker buildx imagetools inspect "ghcr.io/sentioxyz/sentio-node:sha-$SN_COMMIT" --format '{{json .Image}}' \
  | jq -r 'if has("config") then .config.Labels["org.opencontainers.image.revision"] else (to_entries[0].value.config.Labels["org.opencontainers.image.revision"]) end'
git -C /Users/uranuswch/Dev/sentio_xyz/sentio-node fetch -q origin && git -C /Users/uranuswch/Dev/sentio_xyz/sentio-node show "$SN_COMMIT":go.mod | grep -E "housegate/housegate $HG_TAG|arbiter-core $CORE_TAG"
```

Expected: the first prints `Name:`, `MediaType:` and `Digest: sha256:…` — record that digest as `SN_DIGEST`; the second prints exactly `SN_COMMIT`; the third prints both requires. Record both in the release ledger; Task 17 allowlists `sha-$SN_COMMIT@$SN_DIGEST` and Task 18 pins it.

---

## Task 17: production — stage-2 runbook, coupling statements, the allowlisted host image (S2, PR)

Contract §5 and spec §12–§13: the chart needs no change (the grouping window keeps the binary default, the hosts change only by image), so this PR carries the stage-2 runbook `docs/storage-integrity-devnet2-multi-source-tracks.md`, which continues `docs/storage-integrity-devnet2-multi-source.md`, the stage rows of the two earlier runbooks, the supersession of the stage-1 coupling statements (design D12), the D12 stall wording of the two devnet2 helmfiles ("until the tracks activation"), and the render-test changes the live pins need: the stage-2 sentio-node image appended to `WRITER_AUTHORIZATION_IMAGES` before either indexer's pin moves (both overlays set `writerAuthorization: true`), and a test that the chart renders no grouping window. Merging it syncs and applies nothing.

**Files** (anchors are production `origin/main` at `d9e02a534`):
- Create: `docs/storage-integrity-devnet2-multi-source-tracks.md`
- Modify: `docs/storage-integrity-devnet2-multi-source.md` (stage table after `:14`; coupling statements `:16`, `:651`, `:808`, `:823`, `:827`)
- Modify: `docs/storage-integrity-devnet2-dynamic-table-set.md` (stage table after `:16`; §8 after `:943`)
- Modify: `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml` (header `:15-18`), `k8s-sea/sentio-network-devnet2/helmfile.yaml` (comment `:134-141`)
- Modify: `charts/storage-integrity/tests/test_render.py` (`WRITER_AUTHORIZATION_IMAGES` `:56-65`; new `DEVNET2_TRACKS_SOURCE_IMAGE` after `INDEXER_B_SOURCE_IMAGE` `:99-106`; comment `:1363-1365`; two tests appended to `ActualFreshNetworkTest` after `test_devnet2_verifier_state_follows_the_rollout` `:1845-1855`)

**Interfaces:**
- Consumes: release ledger `PROTO_TAG`, `HG_TAG`, `CORE_TAG`, `ARBITER_TAG`, `ARBITER_DIGEST` (Plan S2-A), `SN_COMMIT`, `SN_DIGEST` (Task 16). Plan S2-A names used by the runbook: `arbiter-admin consensus update … --tracks-enabled` with its CLI gate, `arbiter-admin consensus tracks --address <leader>` (`GetTrackState` as protojson, `UseProtoNames` + `EmitUnpopulated`, uint64 values as strings), `arbiter-admin indexer quarantine --address <leader> --indexer <id> --expected-generation <n> --reason <text>` and `indexer release --address <leader> --indexer <id> --expected-generation <n> [--acknowledge-rejected <block>]…` (contract §3.9), the leader's tracks gate refusal and the post-activation registration refusal (contract §3.8), the FSM's activation refusals (contract §3.1), `quarantine` / generation refusals (contract §3.5), the metrics `arbiter_track_safe_block_seq`, `arbiter_track_published_block_seq`, `arbiter_track_quarantined`, `arbiter_track_quarantine_generation` (contract §3.8), `ingress.submission_grouping_window` (default 50 ms), snapshot v20 (contract §3.7), C12, C13. Task 16's `quarantined` and Refused reporting. The stage-1 and dynamic-table-set runbooks' §0 helpers (`si_kubectl`, `si_arbiter`, `si_voter`, `si_metrics`, `si_capability`, `si_leader`, `si_show`, `si_images`, `si_indexer`, `si_rpc`, `si_ch`, `si_indexer_b`, `si_rpc_b`, `si_rpc_on`, `si_info`, `si_status`, `si_source_ch`, `si_verifier_ch`, `si_dind_b`, `si_b_quiesced`, `si_safe_head`, `si_features`, `si_replace_voter`, `si_accept_a`, `si_node_image`). Existing test module pieces `WRITER_AUTHORIZATION_IMAGES`, `FRESH_SOURCE_IMAGE`, `INDEXER_B_SOURCE_IMAGE`, `ActualFreshNetworkTest` (`self.control`), `arbiter_config`, `documents`, `render`, `FRESH_RELEASE`.
- Produces: the PR; the runbook Task 18 executes, with its §0 helpers `si_stage2_image`, `si_sealed`, `si_features_tracks`, `si_tracks`, `si_track`, `si_track_published`, `si_global`, `si_wait_track`, `si_accept_a_track`, `si_track_metrics`, `si_refusal`, `si_agent_start`, `si_agent_stop`, `si_agent_ch` (and a refusing `si_stage1_image`); evidence directories `$HOME/si-devnet2-multisource-tracks` and `$HOME/si-devnet2-multisource-tracks-rehearsal`; test constant `DEVNET2_TRACKS_SOURCE_IMAGE`; tests `test_the_tracks_release_image_is_reviewed_before_either_pin_moves`, `test_devnet2_leaves_the_submission_grouping_window_at_the_binary_default`.

Notes:
- **Inert on merge.** No chart, values or overlay file changes; the helmfile changes are comments. Step 4 proves that every devnet2 release renders byte-identically to `origin/main`.
- **The allowlist moves first (Appendix B, drafting decisions).** Contract §5 has Task 18 move each pin in its own single-service commit "with the new image appended to `WRITER_AUTHORIZATION_IMAGES` first"; appending it here, in the reviewed PR, keeps each live commit to its own pin and mirrors, as the stage-1 PR did for indexer-b.
- **The stuck track is best effort (Appendix B, G7-2), the host checks need indexer-b running (G7-3).** The runbook writes and stops indexer-b in one block, detects a write that reached safe before the stop, repeats once, and restores indexer-b before the quarantine so the host's quarantine reporting can be observed.
- **Section numbers.** The stage-1 runbook's stages 1–7 and the design's "stage 2" collide; the new runbook's title and stage table avoid "stage 2" for its own stages.

- [ ] **Step 1: Worktree and ledger inputs**

Requires Task 16 to have recorded `SN_COMMIT` / `SN_DIGEST` and Plan S2-A Task 15 `ARBITER_TAG` / `ARBITER_DIGEST`. Stop if any is missing.

```bash
git -C /Users/uranuswch/Dev/sentio_xyz/production fetch origin
bash -c '
repo=$1 runtime=$2 name=$3 branch=${4:-urwt/$2/$3} base=${5:-origin/main}
case $name in ""|[._-]*|*[!A-Za-z0-9._-]*) echo "urwt: invalid task name: $name" >&2; exit 2;; esac
if [[ $branch == pr:* || $branch == https://* ]] ||
   git -C "$repo" show-ref --quiet --verify "refs/heads/$branch" ||
   git -C "$repo" show-ref --quiet --verify "refs/remotes/origin/$branch"; then
  create=()
else
  create=(--create --base "$base")
fi
command wt -C "$repo" \
  --config-set "worktree-path = \"~/src/remotesrc/$runtime/{{ repo }}/$name\"" \
  switch "${create[@]}" "$branch" --no-cd --format=json
' urwt /Users/uranuswch/Dev/sentio_xyz/production claude multi-source-s2
```

Expected: one JSON object with `"action": "created"` whose `path` is `~/src/remotesrc/claude/production/multi-source-s2` (expanded). `W` below is that path; prefix every command with `W=<that path>;` and the ledger values (`ARBITER_TAG=…; ARBITER_DIGEST=sha256:…; PROTO_TAG=…; CORE_TAG=…; HG_TAG=…; SN_COMMIT=<40 hex>; SN_DIGEST=sha256:…;`).

```bash
cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
docker buildx imagetools inspect "ghcr.io/sentioxyz/sentio-node:sha-$SN_COMMIT" | sed -n 's/^Digest: *//p'
docker buildx imagetools inspect "ghcr.io/sentioxyz/arbiter:$ARBITER_TAG" | sed -n 's/^Digest: *//p'
```

Expected: `Ran 84 tests` and `OK` (84 test methods at `d9e02a534`); the two digests equal `SN_DIGEST` and `ARBITER_DIGEST`.

- [ ] **Step 2: Write the failing render tests**

In `charts/storage-integrity/tests/test_render.py`, insert after `INDEXER_B_SOURCE_IMAGE` (`:99-106`):

```python
# The multi-source stage-2 sentio-node build (housegate <HG_TAG>, arbiter-core
# <CORE_TAG>: track_manifests_v1, quarantine reporting). indexer-a and then
# indexer-b move to it in single-service commits
# (docs/storage-integrity-devnet2-multi-source-tracks.md §4); both overlays set
# writerAuthorization: true, so it is reviewed here before either pin moves.
DEVNET2_TRACKS_SOURCE_IMAGE = (
    "ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>"
    "@<SN_DIGEST>"
)
```

and append to `ActualFreshNetworkTest`, after `test_devnet2_verifier_state_follows_the_rollout` (`:1845-1855`, before the two blank lines and `if __name__ == "__main__":`):

```python

    def test_the_tracks_release_image_is_reviewed_before_either_pin_moves(self):
        # docs/storage-integrity-devnet2-multi-source-tracks.md §4 moves
        # indexer-a and then indexer-b to the stage-2 image in two single-service
        # commits. Both overlays set writerAuthorization: true, so each commit
        # passes test_writer_authorization_needs_a_reviewed_image only if the
        # image is listed already; so are the images the pins name today.
        self.assertRegex(DEVNET2_TRACKS_SOURCE_IMAGE, r"^ghcr\.io/sentioxyz/sentio-node:sha-[0-9a-f]{40}@sha256:[0-9a-f]{64}$")
        for image in (DEVNET2_TRACKS_SOURCE_IMAGE, FRESH_SOURCE_IMAGE, INDEXER_B_SOURCE_IMAGE):
            self.assertIn(image, WRITER_AUTHORIZATION_IMAGES)

    def test_devnet2_leaves_the_submission_grouping_window_at_the_binary_default(self):
        # Multi-source stage 2 (housegate spec 2026-10-10 §7.1): the leader groups
        # submissions by owner for ingress.submission_grouping_window, a
        # leader-local tunable whose binary default (50ms) applies. The chart
        # exposes no value for it (docs/storage-integrity-devnet2-multi-source-tracks.md
        # §11), so every rendered ingress section is exactly the statement-age limit.
        for docs, name in ((self.control, FRESH_RELEASE), (documents(render()), "storage-integrity")):
            with self.subTest(release=name):
                self.assertEqual(arbiter_config(docs, name)["ingress"], {"max_statement_age": "5m"})
```

Fill the ledger values, then run:

```bash
cd "$W" && sed -i '' -e "s/<SN_COMMIT>/$SN_COMMIT/g" -e "s/<SN_DIGEST>/$SN_DIGEST/g" -e "s/<HG_TAG>/$HG_TAG/g" -e "s/<CORE_TAG>/$CORE_TAG/g" charts/storage-integrity/tests/test_render.py
! git grep -nE '<(SN_COMMIT|SN_DIGEST|HG_TAG|CORE_TAG)>' -- charts
python3 -B charts/storage-integrity/tests/test_render.py ActualFreshNetworkTest 2>&1 | grep -E '^(FAIL|ERROR|Ran|OK)|AssertionError' | head
```

Expected: `FAIL: test_the_tracks_release_image_is_reviewed_before_either_pin_moves` with `AssertionError: 'ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>@<SN_DIGEST>' not found in (…)`; `test_devnet2_leaves_the_submission_grouping_window_at_the_binary_default` passes already (it pins the rendered `ingress: {max_statement_age: 5m}` of `templates/arbiter.yaml:37`).

- [ ] **Step 3: Allow the stage-2 image and update the coupling comment**

`WRITER_AUTHORIZATION_IMAGES` (`:56-65`) gains, before its closing `)`:

```python
    "ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>"
    "@<SN_DIGEST>",
```

(the image is built on `HG_TAG`, which keeps housegate v0.17.0's writer authorization and denylist). In `test_indexer_a_is_declared_only_here_and_merges_the_source_pin_last`, the comment (`:1363-1365`) becomes:

```python
        # indexer-a is SI indexer 0 (snode-1): the ordinary helmfile must not
        # declare it, or a sync of that file would stop its SNode and stall every
        # indexer's safe progress until the tracks activation, and its own track
        # after it.
```

```bash
cd "$W" && sed -i '' -e "s/<SN_COMMIT>/$SN_COMMIT/g" -e "s/<SN_DIGEST>/$SN_DIGEST/g" charts/storage-integrity/tests/test_render.py
! git grep -nE '<(SN_COMMIT|SN_DIGEST)>' -- charts
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
git -C "$W" add charts/storage-integrity/tests/test_render.py && git -C "$W" commit -m "test(storage-integrity): allow the multi-source stage-2 sentio-node image; no grouping window

Both devnet2 SI overlays set writerAuthorization: true, so the stage-2
image (sentio-node $SN_COMMIT on housegate $HG_TAG) is listed in
WRITER_AUTHORIZATION_IMAGES before the single-service pin commits of
docs/storage-integrity-devnet2-multi-source-tracks.md §4 move either
indexer. A render test pins that the chart exposes no submission
grouping window (housegate spec 2026-10-10 §7.1: the binary default
applies).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: `Ran 86 tests` and `OK`.

- [ ] **Step 4: The coupling wording of the devnet2 helmfiles, and the inertness proof**

`k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml`, lines `:15-18`:

```diff
 # the only entrypoint for both; helmfile.yaml declares neither, because a sync
 # there (no storage integrity, no SNode) would stop that SNode and stall every
-# indexer's safe progress (sections 10.2 and 11). Always sync one release with
-# --selector name=<release>.
+# indexer's safe progress until the tracks activation, and that indexer's own
+# track after it (sections 10.2 and 11;
+# docs/storage-integrity-devnet2-multi-source-tracks.md). Always sync one
+# release with --selector name=<release>.
```

`k8s-sea/sentio-network-devnet2/helmfile.yaml`, lines `:138-141`:

```diff
   # storage-integrity-si-v2.helmfile.yaml. A release here (no storage
-  # integrity, no SNode) would stop that indexer's SNode, stall every indexer's
-  # safe progress, and run it against its SI source without the signed ingress
-  # (docs/storage-integrity-devnet2-multi-source.md sections 10.2 and 11).
+  # integrity, no SNode) would stop that indexer's SNode, stall every indexer's
+  # safe progress until the tracks activation (and that indexer's own track
+  # after it), and run it against its SI source without the signed ingress
+  # (docs/storage-integrity-devnet2-multi-source.md sections 10.2 and 11;
+  # docs/storage-integrity-devnet2-multi-source-tracks.md).
```

Prove that nothing renders differently from `origin/main`:

```bash
cd "$W" && base="$(mktemp -d)"; git -C "$W" archive origin/main charts k8s-sea/sentio-network-devnet2 | tar -x -C "$base"
render() { (cd "$1" && helmfile -f "k8s-sea/sentio-network-devnet2/$2" template --selector "name=$3" --skip-deps 2>/dev/null); }
for pair in storage-integrity-si-v2.helmfile.yaml:storage-integrity-si-v2 storage-integrity-si-v2.helmfile.yaml:sentio-node-devnet2-indexer-a storage-integrity-si-v2.helmfile.yaml:sentio-node-devnet2-indexer-b helmfile.yaml:sentio-node-devnet2-observer; do
  render "$base" "${pair%%:*}" "${pair#*:}" > "$base/old.yaml"; render "$W" "${pair%%:*}" "${pair#*:}" > "$base/new.yaml"
  if [ -s "$base/new.yaml" ] && cmp -s "$base/old.yaml" "$base/new.yaml"; then echo "$pair renders unchanged"; else echo "$pair DIFFERS"; fi
done
rm -rf "$base"
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -2
git -C "$W" add k8s-sea/sentio-network-devnet2 && git -C "$W" commit -m "chore(devnet2): the indexer coupling ends with the tracks activation

The helmfile comments that explain why neither SI indexer is declared in
helmfile.yaml now say a stopped SNode stalls every indexer only until
the tracks activation, and its own track after it
(docs/storage-integrity-devnet2-multi-source-tracks.md). Comments only:
every release renders byte-identically.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: four `… renders unchanged` lines and `OK`.

- [ ] **Step 5: The runbook and the earlier runbooks** (runbook §7.1 per amendment A11 (proposed as G7-2); §7.3 to §9 per amendment A12 (proposed as G7-3))

Create `docs/storage-integrity-devnet2-multi-source-tracks.md` with the content below.

````markdown
# Storage integrity devnet2: multi-source tracks and quarantine

This runbook moves devnet2-si-v2 (releases `storage-integrity-si-v2`, `sentio-node-devnet2-indexer-a` and `sentio-node-devnet2-indexer-b` in `sentio-network-devnet2`) onto the stage-2 builds of the multi-source storage-integrity design and turns tracks on: from the tracks activation every L3 block belongs to one SI indexer's track; safe progress, promotion, manifest publication and cleanup are per track; and an authority can quarantine and release an indexer. It then shows on devnet2 that a stuck indexer-b stalls only its own track, and exercises quarantine and release of indexer-b. Design: housegate `docs/superpowers/specs/2026-10-10-multi-source-si-network-design.md`, stage 2 (§7, §9, §10, §13), as clarified by the arbiter stage-2 plan (housegate `docs/superpowers/plans/2026-10-12-multi-source-si-s2a-tracks-consensus.md`, clarifications C1 to C13). It continues [storage-integrity-devnet2-multi-source.md](storage-integrity-devnet2-multi-source.md) (multi-source stage 1, done on 2026-10-11: both indexers enrolled, signed claims active). The stage numbers below are this runbook's own; "stage 2" of the design is what the whole runbook rolls out.

| Stage | Change | Way back |
| --- | --- | --- |
| 0 | PR: this runbook, the stage rows of the two earlier runbooks, their coupling statements superseded, the coupling wording of both devnet2 helmfiles, the stage-2 sentio-node image listed for writer authorization, a render test that the chart sets no grouping window; inert until a pin moves | Revert the PR |
| 1 | Offline replay check; verifiers to arbiter `<ARBITER_TAG>` (§2, §3) | Before stage 4: revert (§10.1) |
| 2 | indexer-a, then indexer-b: sentio-node `<SN_COMMIT>` (HouseGate `<HG_TAG>`, arbiter-core `<CORE_TAG>`), advertising `track_manifests_v1` and reporting `quarantined` (§4) | Before stage 4: revert (§10.1) |
| 3 | Voters one at a time to `<ARBITER_TAG>`, followers first, leader last (§5) | Before stage 4: replace them back (§10.1) |
| 4 | Tracks activation: every data-plane node re-registers with the leader, drain, `tracks_enabled` (§6.1, §6.2) | None: irreversible (§10.2) |
| 5 | Verify: the activation, the first track-aware manifests, indexer-b's test table, snapshot v20 (§6.3 to §6.5) | None needed |
| 6 | A stuck indexer-b track: indexer-b stopped holding an unpromoted write of its own while indexer-a keeps reaching safe; then indexer-b restored (§7) | Restore indexer-b (§7.3) |
| 7 | Quarantine and release of indexer-b (§8) | Release (§8.3) |
| 8 | End: drop the test table, final checks (§9) | None needed |

Before stage 4 nothing changes for any table: the stage-2 leader proposes no new field and the voters keep writing v19 snapshots. From stage 4 on, a stuck indexer stalls only its own track. The global safe watermark (`arbiter_safe_watermark_block_seq`, `consensus tracks`' `global_safe_block_seq`) stays the conservative contiguous prefix over every block, so it stops while any track is stuck and is no longer the proof that a given indexer's writes reached safe; §0's per-track helpers are. This supersedes the coupling statements of the stage-1 runbook (design D12).

The automatic quarantine of a Rejected block and the release that acknowledges it (design §7.5, §13 stage 2 step 5) are exercised only in the integration environment, with fault injection: arbiter `//integration:integration_test`, `TestRejectedBlockQuarantinesAndReleaseCleansItsParts`. devnet2 never injects a fault; §11 says how to handle a real one.

Every command block is bash. Run them from the root of a production checkout at `origin/main`, in order, in one dedicated `bash` session started with `set -eo pipefail` (from fish, run `bash` first). Every value a later block needs is written under `$si_backup`, so a block also runs in a new session once §0 is defined. Never run two stages in one window. From §5 until §6.3 has passed, freeze every other change to `storage-integrity-si-v2`: no other sync of it and no pod deletion outside this runbook.

Every block that pushes to production `main`, syncs a release, deletes, scales or restarts a resource, runs DDL or writes data, or submits a consensus update, a quarantine or a release needs explicit user confirmation in chat before it runs; each such block is preceded by the line `Confirm before running.`. The update of §6.2 is additionally irreversible.

Writes in this runbook: indexer-a's acceptance INSERTs into `devnet101.swap_new2` through indexer-a's sidecar, values `1001` to `1005`, one per step; and the `devuser1` owner's writes through its own local agent into `devuser1.s2_e2e`, rows 1 to 5. Never re-run a block that writes: run `si_show` and `si_tracks` first. Retryable `SOURCE_UNAVAILABLE` windows are expected for indexer-a's tables in §4.1 and §6.1, and for indexer-b's in §4.2, §6.1, §7.1 and §7.3, while that indexer's SNode restarts or is stopped.

## 0. Session helpers

Define the helpers of §0 of [storage-integrity-devnet2-dynamic-table-set.md](storage-integrity-devnet2-dynamic-table-set.md), then those of §0 of [storage-integrity-devnet2-multi-source.md](storage-integrity-devnet2-multi-source.md), unchanged, then these. They replace `si_backup` and `R`, so the stage-1 evidence stays untouched:

```bash
si_backup="$HOME/si-devnet2-multisource-tracks"
R="$HOME/si-devnet2-multisource-tracks-rehearsal"
si_stage2_image() { # the stage-2 sentio-node image recorded in §1.1
  printf 'ghcr.io/sentioxyz/sentio-node:sha-%s@%s\n' "$(sed -n 3p "$si_backup/pins")" "$(sed -n 4p "$si_backup/pins")"
}
si_stage1_image() { echo "si_stage1_image reads the stage-1 pins; this runbook uses si_stage2_image" >&2; return 1; }
si_sealed() { si_metrics 0 | sed -n 's/^arbiter_sealed_block_seq //p'; }
si_features_tracks() { # the current leader's feature book: voter ids, and whether each data-plane node advertised signed_claims_v1 and track_manifests_v1
  local l
  l="$(si_leader)"
  si_arbiter "$l" /usr/local/bin/arbiter-admin consensus features --address "$(si_voter "$l")" |
    jq -c '{voters: .raft_voter_ids, nodes: [.entries[] | {id: .node_id, claims: ((.features // []) | index("signed_claims_v1") != null), tracks: ((.features // []) | index("track_manifests_v1") != null)}]}'
}
si_tracks() { # the leader's GetTrackState (arbiter-admin consensus tracks) as one JSON line; uint64 values are strings
  local l
  l="$(si_leader)"
  si_arbiter "$l" /usr/local/bin/arbiter-admin consensus tracks --address "$(si_voter "$l")" | jq -c .
}
si_track() { # $1 indexer id: that indexer's track, numbers as numbers
  si_tracks | jq -c --arg id "$1" '.indexers[] | select(.indexer_id == $id) | {indexer_id: (.indexer_id | tonumber), prefix: (.prefix_block_seq | tonumber), published: (.published.safe_block_seq | tonumber), applied_promotion_seq: (.published.applied_promotion_seq | tonumber), generation: (.quarantine_generation | tonumber), quarantined, reason: .quarantine_reason, rejected: .rejected_blocks, acknowledged: .acknowledged_rejected_blocks}'
}
si_track_published() { # $1 indexer id: that track's frontier in the latest published manifest (its initial entry before the first track-aware one)
  si_tracks | jq -r --arg id "$1" '.indexers[] | select(.indexer_id == $id) | .published.safe_block_seq'
}
si_global() { # the global safe prefix, the published manifest's safe_block_seq and the sealed tip
  printf '%s sealed=%s\n' "$(si_tracks | jq -r '"global=\(.global_safe_block_seq) published=\(.published_safe_block_seq)"')" "$(si_sealed)"
}
si_wait_track() { # $1 indexer id, $2 base: waits up to 8 minutes for that track's published frontier to pass base; fails otherwise
  local n now=""
  for n in $(seq 1 48); do
    now="$(si_track_published "$1")"
    if [ -n "$now" ] && [ "$now" -gt "$2" ]; then echo "track $1 published $2 -> $now"; return 0; fi
    sleep 10
  done
  echo "track $1's published frontier stayed at ${now:-unknown} (base $2) for 8 minutes; stop"
  return 1
}
si_accept_a_track() { # $1 value: one signed Native INSERT into devnet101.swap_new2 through indexer-a's sidecar; waits for track 0's published frontier, fails on a timeout, reads the row back
  local base
  base="$(si_track_published 0)"
  [ -n "$base" ] || { echo "indexer 0 has no track: tracks are not active"; return 1; }
  si_kubectl delete pod si-ms-acceptance --ignore-not-found
  si_kubectl run si-ms-acceptance -i --restart=Never \
    --image=us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:25.8-lts-decimal512-2026-03-27@sha256:89839a322a59489fa14ca12e9d00f53d860b08c5486808f652a9484d7c247797 \
    --overrides='{"apiVersion":"v1","spec":{"imagePullSecrets":[{"name":"google-image-secret"}]}}' \
    --command -- sh -ec "
      clickhouse-local --query 'SELECT toInt64($1) AS value FORMAT Native' > /tmp/row.native
      clickhouse-client --compression=0 --host sentio-node-devnet2-indexer-a --port 9011 \
        --query 'INSERT INTO devnet101.swap_new2 (value) FORMAT Native' < /tmp/row.native"
  si_kubectl delete pod si-ms-acceptance
  si_wait_track 0 "$base" || return 1
  si_ch "SELECT count() FROM devnet101.swap_new2 WHERE value = $1 SETTINGS SQL_x_read_mode='safe'"
}
si_track_metrics() { # $1 voter ordinal: the per-track gauges (emitted only after the tracks activation)
  si_arbiter "$1" wget -qO- http://127.0.0.1:9090/metrics | grep -E '^arbiter_track_' || true
}
si_refusal() { # $1 a|b, $2 database, $3 table: that indexer's answer with its refusal fields
  si_rpc_on "$1" "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sentio_getStorageIntegrityTableStatus\",\"params\":[\"$2\",\"$3\"]}" | jq -c '.result | {status, refused_code, refused_reason, registry_version}'
}
si_agent_start() { # $1 key file of devuser1's owner: its local agent, the key only in HOUSEGATE_AGENT_KEY; si_agent_stop ends it, and so does the end of the shell
  [ -r "$1" ] || { echo "usage: si_agent_start <readable key file of devuser1's owner>"; return 1; }
  HOUSEGATE_AGENT_KEY="$(cat "$1")" housegate >> "$si_backup/agent.log" 2>&1 &
  si_agent_pid=$!
  trap 'kill "$si_agent_pid" 2>/dev/null || true' EXIT
  sleep 5
  kill -0 "$si_agent_pid" 2>/dev/null || { echo "the agent exited; see $si_backup/agent.log"; return 1; }
}
si_agent_stop() { kill "$si_agent_pid" 2>/dev/null || true; wait "$si_agent_pid" 2>/dev/null || true; trap - EXIT; }
si_agent_ch() { clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 "$@"; }
```

## 1. Preparation

### 1.1 Pins

| Component | Pin | Evidence |
| --- | --- | --- |
| Arbiter voters and verifiers | `ghcr.io/sentioxyz/arbiter:<ARBITER_TAG>@<ARBITER_DIGEST>` (arbiter-core `<CORE_TAG>`, arbiter-proto `<PROTO_TAG>`, housegate `<HG_TAG>`) | arbiter release workflow; `docker buildx imagetools inspect`: OCI revision equals the tag's commit |
| sentio-node, indexer-a and indexer-b | `ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>@<SN_DIGEST>` (housegate `<HG_TAG>`, arbiter-core `<CORE_TAG>`) | main CI `docker-push`; OCI revision equals `<SN_COMMIT>`; index digest from `docker buildx imagetools inspect` |
| In-pod rewriter, both indexers | unchanged: `us-west1-docker.pkg.dev/sentio-352722/sentio/housegate-rewriter:0.17.0@sha256:b3b227fa4a28bd9dcd5d3d6aa172890ff66d7bf60e1c7039428efe4dabebb50a` | stage-1 runbook §1.1 |
| Agent sidecar, both indexers | unchanged: `ghcr.io/housegate/housegate:v0.15.0@sha256:1b51de76b7b7a15a8006d0ac06febb10012c74c92d4950d07e4e5a5b77b3570f` | stage-1 runbook §1.1 |

Before this upgrade the voters and verifiers run arbiter `v0.11.0` (`sha256:7a06d389b6c9a313ca4ce48837b5fcf2193184a21c5f0b68c91e2123f78a17fb`) and both indexers run sentio-node `2db130e63b0c662feae1b225341921c404927fb7` (`sha256:fbb1ef327212fa6ba0c779728dead77ed4506c0955508e2e8e6d0d25d4c8c185`, HouseGate `v0.18.0`); those are the rollback targets of stages 1 to 3. HouseGate `<HG_TAG>` raises no rewriter engine floor above rewriter-grpc v0.17.0 (check its release notes; otherwise the in-pod rewriter moves together with the sentio-node pin, as in stage 2c of the dynamic-table-set runbook).

Every voter, verifier and SNode must run the stage-2 builds before the tracks activation of §6.2 commits: `arbiter-admin`'s and the leader's tracks gates refuse the update until every voter and every registered data-plane node advertises `track_manifests_v1`, and an older SNode or verifier cannot read a track-aware manifest. Both SI hosts carry HouseGate `<HG_TAG>` through the sentio-node pin, which maps the arbiter's `INDEXER_QUARANTINED` (code 11) to the non-retryable 392; an older embedded HouseGate would map it to an unknown outcome. Both agent sidecars stay on housegate v0.15.0: on a 392 refusal of the signed lane they close the client session, and the refusal is final either way.

```bash
mkdir -p -m 700 "$si_backup"
ARBITER_TAG='<ARBITER_TAG>'; ARBITER_DIGEST='<ARBITER_DIGEST>'; SN_COMMIT='<SN_COMMIT>'; SN_DIGEST='<SN_DIGEST>'
case "$ARBITER_TAG$ARBITER_DIGEST$SN_COMMIT$SN_DIGEST" in *'<'*) echo "pins are still placeholders; stop"; exit 1;; esac
[ "$(docker buildx imagetools inspect "ghcr.io/sentioxyz/arbiter:$ARBITER_TAG" | sed -n 's/^Digest: *//p')" = "$ARBITER_DIGEST" ]
[ "$(docker buildx imagetools inspect "ghcr.io/sentioxyz/sentio-node:sha-$SN_COMMIT" | sed -n 's/^Digest: *//p')" = "$SN_DIGEST" ]
grep -qF "\"ghcr.io/sentioxyz/sentio-node:sha-$SN_COMMIT\"" charts/storage-integrity/tests/test_render.py
printf '%s\n' "$ARBITER_TAG" "$ARBITER_DIGEST" "$SN_COMMIT" "$SN_DIGEST" > "$si_backup/pins"
echo "pins resolve"
```

Expected: `pins resolve` (the last check finds the image in `WRITER_AUTHORIZATION_IMAGES`, stage 0).

### 1.2 Identities

Unchanged since §1.2 of the stage-1 runbook:

| Identity | Value |
| --- | --- |
| Network | `devnet2`, schema snapshot `devnet2-si-v2`, genesis snapshot id `0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06` |
| SI indexer 0 (track 0) | indexer-a, signer `0x22a67f498e669cbde18a0eecfff74a72c48331f4`, activation block `5508931`, SNode `snode-1`, source `storage-integrity-si-v2-source:9000`, database `devnet101` among its own |
| SI indexer 1 (track 1) | indexer-b, signer `0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3`, activation block `5883985`, SNode `snode-2`, source `storage-integrity-si-v2-indexer-b-source:9000`, database `devuser1` among its own |
| Verifiers | `verifier-1`, `verifier-2`, `verifier-3`, state in `/state` |
| Voters | `storage-integrity-si-v2-arbiter-0`, `-1`, `-2` |
| Authority | `0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705`; key in `storage-integrity-devnet2-si-v2-authority`, present in every voter as `ARBITER_AUTHORITY_PRIVATE_KEY_HEX`; it signs the activation, the quarantine and the release |

The SNode id ledger stays the stage-1 runbook's (§1.2 there): `snode-1`, `snode-2`; next free id `snode-3`.

```bash
for id in 0 1; do si_rpc "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sentio_getIndexerInfoById\",\"params\":[$id]}" | jq -c '.result | {indexerId, signer: (.signer | ascii_downcase), storageNodeRpcPort}'; done
si_rpc '{"jsonrpc":"2.0","id":1,"method":"sentio_getDatabaseInfoById","params":["devuser1"]}' | jq -c '.result | {databaseId, indexerId}'
```

Expected: `{"indexerId":0,"signer":"0x22a67f498e669cbde18a0eecfff74a72c48331f4","storageNodeRpcPort":32003}`, `{"indexerId":1,"signer":"0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3","storageNodeRpcPort":33003}` and `{"databaseId":"devuser1","indexerId":1}`. A different signer or owner: stop.

### 1.3 Preconditions and baseline (read-only)

- The stage-1 runbook is done: `si_indexers` names indexers 0 and 1, `verifiers` the three verifiers.
- The stage 0 PR is merged and the checkout is at `origin/main`.
- `$R` does not exist yet (§2.1 creates it) or already holds `bin/{old,new}/{statedigest,v20check}` from an earlier §2.1 of this runbook.
- No SI writer runs during §2, §6.1 and §6.2 except this runbook's acceptance INSERTs; the `devuser1` owner writes only where §6.4 to §9 say so.

```bash
si_show | tee "$si_backup/show-before.json" | jq -c '{genesis_snapshot_id, epoch, params_digest, promotion_seq, max_writers: .current.max_writers, artifact_disposition_capability: .current.artifact_disposition_capability, client_lanes: .current.client_lanes, tracks_enabled: .current.tracks_enabled, si_indexers: [.current.si_indexers[] | {indexer_id, activation_block, snode_node_id}], verifiers: [.current.verifiers[].node_id]}'
for i in 0 1 2; do si_metrics "$i"; si_capability "$i"; done | tee "$si_backup/voters-before.txt"
for i in 0 1 2; do echo "arbiter-$i track gauges: $(si_track_metrics "$i" | wc -l)"; done
si_images | tee "$si_backup/images-before.txt"
for x in a b; do printf '%s %s\n' "$x" "$(si_node_image "$x")"; done | tee "$si_backup/indexer-images-before.txt"
for x in a b; do si_info "$x"; done | tee "$si_backup/info-before.json"
si_features | tee "$si_backup/features-before.json"
```

Expected: genesis snapshot id `0x63700cf1…1c06`; epoch `"4"` (read on 2026-10-12), `max_writers` `"2"`, `artifact_disposition_capability` `0`, `client_lanes` `{"max_lanes_per_account":256}`, `tracks_enabled` `null` (the `v0.11.0` admin does not know it), entries 0 (`5508931`, `snode-1`) and 1 (`5883985`, `snode-2`), verifiers `verifier-1..3`; every voter with the same sealed tip, the safe watermark equal to it, `arbiter_open_block_statements 0`, two active SNodes and three active verifiers, `features` `["client_lanes_v1","signed_claims_v1"]` and `0` track gauges; all six arbiter pods on `sha256:7a06d389…`; both indexers on `…:sha-2db130e63b0c662feae1b225341921c404927fb7@sha256:fbb1ef32…`; `si_info a` with `"si_indexer_id":0,"self_indexer_id":0,"enrolled":true` and `si_info b` with `"si_indexer_id":1,"self_indexer_id":1,"enrolled":true`, neither with a `quarantined` key (the stage-1 image); `si_features` lists `snode-1`, `snode-2` and `verifier-1..3`, each `claims: true`, or with no features at all where a node last registered with an earlier leader (§6.1 restarts every node anyway).

## 2. Offline replay check

The throwaway `statedigest` tool (§2 of the stage-1 runbook; §2.6 and §7.2 of the dynamic-table-set runbook) restores a copy of each voter's `raft` directory into one arbiter build and hashes the state after applying every logged command, one `key <member> <sha256>` line per member of the written snapshot document; `v20check` restores one raw snapshot container (`state.bin`) into one build and prints its version byte and the outcome. `old` is the voters' live build, `v0.11.0`; `new` is `<ARBITER_TAG>`. Neither tool is committed anywhere.

### 2.1 Rehearsal binaries (local only)

The tool sources are the stage-1 ones, `$HOME/si-devnet2-multisource-rehearsal/statedigest/main.go` and `$HOME/si-devnet2-multisource-rehearsal/v19check/main.go`, the latter copied unchanged as `v20check` (it prints the container's own version byte). Each build copies them into a `git archive` export of a tag, which carries no `.git`, so `GOFLAGS=-buildvcs=false` is required; a tool that no longer compiles against a release's FSM must be adapted first. The block refuses an existing `$R`: when an earlier run of it already built both binaries, skip it and run only the check that follows. Set `arbiter_repo` to a local arbiter clone:

```bash
OLD_TAG=v0.11.0; NEW_TAG='<ARBITER_TAG>'; arbiter_repo=/path/to/arbiter
S1R="$HOME/si-devnet2-multisource-rehearsal"
[ -d "$arbiter_repo/.git" ] || { echo "set arbiter_repo to a local arbiter clone first"; exit 1; }
[ -f "$S1R/statedigest/main.go" ] && [ -f "$S1R/v19check/main.go" ] || { echo "the stage-1 tool sources are missing under $S1R"; exit 1; }
[ ! -e "$R" ] || { echo "$R exists: move it aside; a rehearsal directory is never reused"; exit 1; }
mkdir -p -m 700 "$R"
mkdir -p "$R/statedigest" "$R/v20check"
cp "$S1R/statedigest/main.go" "$R/statedigest/"; cp "$S1R/v19check/main.go" "$R/v20check/"
git -C "$arbiter_repo" fetch --tags origin
for build in "old $OLD_TAG" "new $NEW_TAG"; do
  set -- $build
  src="$R/src/$1-$2"; mkdir -p "$src" "$R/bin/$1"
  git -C "$arbiter_repo" archive "$2" | tar -x -C "$src"
  mkdir -p "$src/cmd/statedigest" "$src/cmd/v20check"
  cp "$R/statedigest/main.go" "$src/cmd/statedigest/"; cp "$R/v20check/main.go" "$src/cmd/v20check/"
  (cd "$src" && GOFLAGS=-buildvcs=false go build -o "$R/bin/$1/" ./cmd/statedigest ./cmd/v20check)
done
echo "$OLD_TAG" > "$R/old_tag.txt"
ls "$R/bin/old" "$R/bin/new"
```

Expected: both directories list `statedigest` and `v20check`. Then check `v20check` on a live v19 container (the voters write v19 since the signed-claims activation): both builds must read it, and the version it prints must be the container's own byte:

```bash
si_arbiter 0 sh -c 'cat "$(ls -dt /data/raft/snapshots/*/ | head -1)state.bin"' > "$si_backup/v19-state.bin"
head -c 5 "$si_backup/v19-state.bin" | od -An -tu1 | awk '{print $5}'
"$R/bin/old/v20check" "$si_backup/v19-state.bin"
"$R/bin/new/v20check" "$si_backup/v19-state.bin"
```

Expected: `19`, then `restore_ok version 19` from both builds. A tool that prints another version than the byte above reads a constant instead of the container: fix it before §6.5 relies on it.

### 2.2 Baseline

Re-measure the voters' live build first: a stale `old` binary compares the wrong build and proves nothing. Every voter must run one image, the published digest of the release `$R/old_tag.txt` names; the check records the verified baseline in `$si_backup/replay-baseline`, which §2.3 requires:

```bash
si_images | tee "$si_backup/images-replay.txt"
live="$(awk '$1 ~ /^storage-integrity-si-v2-arbiter-/ {sub(/.*@/, "", $3); print $3}' "$si_backup/images-replay.txt" | sort -u)"
old_tag="$(cat "$R/old_tag.txt")"
echo "old_tag $old_tag; voters run $live"
[ -n "$live" ] && [ "$(printf '%s\n' "$live" | wc -l)" -eq 1 ] || { echo "the voters do not run exactly one image; stop"; exit 1; }
old_digest="$(docker buildx imagetools inspect "ghcr.io/sentioxyz/arbiter:$old_tag" | sed -n 's/^Digest: *//p')"
[ "$live" = "$old_digest" ] || { echo "the voters run $live, but \$R/old_tag.txt names $old_tag ($old_digest): move \$R aside and rerun §2.1 with OLD_TAG set to the voters' live release"; exit 1; }
printf '%s %s\n' "$old_tag" "$live" > "$si_backup/replay-baseline"
echo "old matches the voters' live release $old_tag"
```

Expected: `old_tag v0.11.0; voters run sha256:7a06d389b6c9a313ca4ce48837b5fcf2193184a21c5f0b68c91e2123f78a17fb` and `old matches the voters' live release v0.11.0`. Never continue to §2.3 without the success line.

### 2.3 Replay

The replay loads each voter's `arbiter.yaml` with arbiter's `config.Load`, which requires `anchor.evm.private_key_hex`; in the pods it comes from `ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX`, so the block exports a throwaway random key (the offline replay signs nothing). Signed claims are active since epoch 3, so a `key registration_seqs` line is expected now (the stage-1 guard against it does not apply); a `key tracks` line is not, before §6.2:

```bash
read -r base_tag base_digest < "$si_backup/replay-baseline" || { echo "run the baseline check of §2.2 first"; exit 1; }
[ "$(cat "$R/old_tag.txt")" = "$base_tag" ] || { echo "\$R/old_tag.txt no longer names $base_tag: rerun §2.2"; exit 1; }
[ "$(si_images | awk '$1 ~ /^storage-integrity-si-v2-arbiter-/ {sub(/.*@/, "", $3); print $3}' | sort -u)" = "$base_digest" ] || { echo "the voters no longer run $base_digest: rerun §2.2"; exit 1; }
for v in old new; do [ -x "$R/bin/$v/statedigest" ] || { echo "missing $R/bin/$v/statedigest"; exit 1; }; done
export ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX="$(openssl rand -hex 32)"  # config.Load needs one; the offline replay signs nothing
si_show | tee "$si_backup/show-replay.json" | jq -c '{epoch, params_digest, promotion_seq}'
for i in 0 1 2; do
  si_kubectl exec "storage-integrity-si-v2-arbiter-$i" -c arbiter -- tar -C /data -czf - raft > "$si_backup/arbiter-$i-raft.tgz"
  si_kubectl exec "storage-integrity-si-v2-arbiter-$i" -c arbiter -- cat /config/arbiter.yaml > "$si_backup/arbiter-$i.yaml"
  work="$(mktemp -d)"
  tar -C "$work" -xzf "$si_backup/arbiter-$i-raft.tgz"
  for v in old new; do
    out="$si_backup/digest-$i-$v"
    "$R/bin/$v/statedigest" -data-dir "$work/raft" -config "$si_backup/arbiter-$i.yaml" > "$out.txt" 2> "$out.err" || { echo "statedigest $v failed for arbiter-$i (a raft.db that fails to open is a torn copy; re-copy):"; cat "$out.err"; exit 1; }
    grep -E '^(snapshot_index|last_index|applied_commands|rejected_commands|written_version|genesis_configured|live_check|apply_error|apply_rejected|key|spent_ids|snapshot_sha256)' "$out.txt" > "$out.cmp"
    if grep -q '^apply_error ' "$out.txt"; then echo "arbiter-$i $v: apply_error"; exit 1; fi
    if grep -q '^key tracks ' "$out.txt"; then echo "arbiter-$i $v: tracks state before the activation"; exit 1; fi
  done
  if cmp -s "$si_backup/digest-$i-old.cmp" "$si_backup/digest-$i-new.cmp"; then
    echo "arbiter-$i: old and new replay identically"
  else
    echo "arbiter-$i: DIFFERENT; stop"; diff "$si_backup/digest-$i-old.cmp" "$si_backup/digest-$i-new.cmp"; exit 1
  fi
  grep -E '^(applied_commands|written_version|live_check)' "$si_backup/digest-$i-new.txt"
  rm -rf "$work"
done
```

Expected: three `old and new replay identically` lines; for each voter `applied_commands` above zero, `written_version 19` (the new build writes v20 only once `tracks_enabled` is committed), a `key registration_seqs` line in both builds and no `key tracks` line, no `apply_error`, and `live_check network_id=devnet2 genesis_snapshot_id=0x63700cf1… epoch=4 params_digest=<show-replay.json> promotion_seq=<show-replay.json>`. Any difference: stop before §3.

## 3. Verifiers

One commit in `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml` and `charts/storage-integrity/tests/test_render.py`: `arbiter.image.digest` set to `<ARBITER_DIGEST>` (the version comment names `<ARBITER_TAG>` and the previous pin `v0.11.0 sha256:7a06d389…`) with its mirror `DEVNET2_ARBITER_DIGEST`. `verifier.state` is on since stage 1, so this is a plain digest roll: the sync rolls the verifiers one at a time (`RollingUpdate`) and no StatefulSet is deleted; the voters share the digest but are `OnDelete`, so they keep `v0.11.0` until §5.

Confirm before running.

```bash
python3 -B charts/storage-integrity/tests/test_render.py
[ "$(git diff --name-only HEAD~1 HEAD | sort | tr '\n' ' ')" = "charts/storage-integrity/tests/test_render.py k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml " ] || { echo "HEAD is not the verifier commit"; exit 1; }
git rev-parse HEAD > "$si_backup/stage2-verifiers-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
si_images
for i in 0 1 2; do si_metrics "$i"; done
si_features_tracks
for n in 0 1 2; do si_kubectl exec "storage-integrity-si-v2-verifier-$n" -c verifier -- cat /state/registration.json; echo; done
```

Expected: the three verifiers run `<ARBITER_DIGEST>`, the voters keep `sha256:7a06d389…`; every voter reports `arbiter_nodes{role="verifier",status="active"} 3` again within a few minutes and the safe watermark keeps following the sealed tip; `si_features_tracks` lists `verifier-1..3` with `claims: true` and `tracks: true` (they re-registered with the current `v0.11.0` leader, which records a feature it does not use yet) and the two SNodes with `tracks: false` (or without features); each `registration.json` holds a higher `registration_seq` than before. Do not delete a voter in this step.

## 4. SNodes

Both SI indexers move to sentio-node `<SN_COMMIT>`, indexer-a first. The image embeds HouseGate `<HG_TAG>` and arbiter-core `<CORE_TAG>`: its SNode advertises `track_manifests_v1`, reads track-aware manifests and echoes rejected-part cleanups, and the host reports `quarantined` in `sentio_getStorageIntegrityInfo` and its own Active tables as Refused while the arbiter has quarantined its indexer. Before §6.2 none of this is used. `WRITER_AUTHORIZATION_IMAGES` already lists the image (stage 0), so each commit touches only its own pin and mirrors; the sidecars and the in-pod rewriters do not change. Each sync restarts that indexer's pod, and its tables answer `SOURCE_UNAVAILABLE` (retryable) until its SNode is active again.

### 4.1 indexer-a

One commit: sentio-node `sha-<SN_COMMIT>@<SN_DIGEST>` in `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml`, its mirrors `docs/examples/storage-integrity-fresh-source-image.yaml` and `FRESH_SOURCE_IMAGE` in `test_render.py`, with the header comments naming `<SN_COMMIT>` (housegate `<HG_TAG>`, arbiter-core `<CORE_TAG>`) and the rollback target `sha-2db130e63b0c662feae1b225341921c404927fb7@sha256:fbb1ef327212fa6ba0c779728dead77ed4506c0955508e2e8e6d0d25d4c8c185` (only before §6.2).

Confirm before running.

```bash
python3 -B charts/storage-integrity/tests/test_render.py
[ "$(git diff --name-only HEAD~1 HEAD | sort | tr '\n' ' ')" = "charts/storage-integrity/tests/test_render.py docs/examples/storage-integrity-fresh-source-image.yaml k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml " ] || { echo "HEAD is not the indexer-a image commit"; exit 1; }
git rev-parse HEAD > "$si_backup/stage2-indexer-a-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
si_kubectl get pod sentio-node-devnet2-indexer-a-0 -o jsonpath='{range .spec.containers[*]}{.name}{"\t"}{.image}{"\n"}{end}'
[ "$(si_node_image a)" = "$(si_stage2_image)" ] && echo "indexer-a runs the stage-2 image"
si_info a
si_features_tracks
si_accept_a 1001
```

Expected: `sentio-node` runs `…:sha-<SN_COMMIT>@<SN_DIGEST>` (`indexer-a runs the stage-2 image`), the other two containers are unchanged, and the pod is Ready; the info answer carries `"si_indexer_id":0,"self_indexer_id":0,"enrolled":true,"quarantined":false` (the new key proves the stage-2 host) and the `registry_version` of §1.3; `si_features_tracks` lists `snode-1` with `claims: true` and `tracks: true`; the acceptance INSERT reaches safe and reads `1` (before §6.2 the global watermark is still the proof, so the stage-1 helper `si_accept_a` applies).

### 4.2 indexer-b

One commit: the same image in `k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml` and its mirror `INDEXER_B_SOURCE_IMAGE` in `test_render.py`, with the header comments rewritten the same way.

Confirm before running.

```bash
python3 -B charts/storage-integrity/tests/test_render.py
[ "$(git diff --name-only HEAD~1 HEAD | sort | tr '\n' ' ')" = "charts/storage-integrity/tests/test_render.py k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml " ] || { echo "HEAD is not the indexer-b image commit"; exit 1; }
git rev-parse HEAD > "$si_backup/stage2-indexer-b-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-b
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-b --timeout=15m
si_kubectl get pod sentio-node-devnet2-indexer-b-0 -o jsonpath='{range .spec.containers[*]}{.name}{"\t"}{.image}{"\n"}{end}'
[ "$(si_node_image b)" = "$(si_stage2_image)" ] && echo "indexer-b runs the stage-2 image"
si_kubectl get pod sentio-node-devnet2-indexer-b-0 -o jsonpath='{.status.containerStatuses[?(@.name=="sentio-node")].restartCount}{"\n"}'
si_info b
si_features_tracks
si_status b devuser1 t
for i in 0 1 2; do si_metrics "$i"; done
```

Expected: `indexer-b runs the stage-2 image` and the pod Ready with restart count `0`; `"si_indexer_id":1,"self_indexer_id":1,"enrolled":true,"quarantined":false`; `snode-2` with `claims: true` and `tracks: true`; `devuser1.t` still `ordinary` (Legacy); every voter reports two active SNodes. indexer-b's write path on the new image is first used after §6.2 (§6.4), where a fault would stay in its own track.

## 5. Voters

Followers first, the leader last, one pod at a time, so two voters always form a quorum; the sealed tip must agree on all three before the next deletion. The voters are `OnDelete`, so the deletion moves a pod onto the digest §3 committed. From here until §6.3 has passed, freeze voter rollouts. Each deletion is confirmed on its own.

Confirm before running.

```bash
leader="$(si_leader)"
for i in $(printf '0\n1\n2\n' | grep -vx "$leader") "$leader"; do si_replace_voter "$i"; done
si_images
for i in 0 1 2; do si_capability "$i"; done
```

Expected: every capability line names its own `node_id` with `protocol_version` `1`, `updates_enabled` `true` and `features` `["client_lanes_v1","signed_claims_v1","track_manifests_v1"]`, and `si_images` shows all six arbiter pods on `<ARBITER_DIGEST>`. The replaced voters still write v19 snapshots and the leader proposes no new field, so until §6.2 the previous image can take a voter back the same way (§10.1).

## 6. Tracks activation

### 6.1 Re-register the data plane and drain

A new leader starts with an empty feature book, its gate accepts only features advertised to it in its current term, and §5 replaced the leader last. Restart every verifier and both SNodes so that they re-register with the current leader; each SNode restart gives its indexer's tables a short retryable `SOURCE_UNAVAILABLE` window.

Confirm before running.

```bash
si_kubectl rollout restart statefulset/storage-integrity-si-v2-verifier
si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
si_kubectl rollout restart statefulset/sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
si_kubectl rollout restart statefulset/sentio-node-devnet2-indexer-b
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-b --timeout=15m
si_features_tracks | tee "$si_backup/features-before-activation.json"
si_tracks | tee "$si_backup/tracks-before-activation.json"
si_show | jq -c '{epoch, promotion_seq, artifact_disposition_capability: .current.artifact_disposition_capability, tracks_enabled: .current.tracks_enabled}'
for x in a b; do si_info "$x"; done | tee "$si_backup/info-before-activation.json"
for i in 0 1 2; do si_metrics "$i"; done
```

Expected: the three voter ids and exactly the nodes `snode-1`, `snode-2`, `verifier-1`, `verifier-2` and `verifier-3`, each `claims: true` and `tracks: true`; `si_tracks` answers `"tracks_enabled":false` with no indexers; `artifact_disposition_capability` `0` and `tracks_enabled` `false`. Any other node in the book must be explained before §6.2: the gates require the feature from every registered data-plane node. `GetNodeFeatures` refuses while the Raft configuration holds a non-voter; inspect it first. Drained means every voter reports `arbiter_open_block_statements 0`, `arbiter_unanchored_verified_blocks 0` and a safe watermark equal to the sealed tip; the activation also needs capability 0 and an idle snapshot-query barrier (the lane is not wired on devnet2). An undrained or otherwise refused update changes nothing. A missed restart shows in the leader's refusal: `tracks gate refused: data-plane nodes <ids> have not registered with this leader advertising track_manifests_v1; restart them on a stage-2 release`.

Residual risk: a second process configured with a voter's `node_id` defeats the voter-identity check, because the gate asks each node who it is. Confirm that nothing but the three voters runs `arbiter` with their ids.

### 6.2 Activate

Stop here and obtain explicit confirmation. The update is **irreversible**: once it commits, `tracks_enabled` can never be lowered, the voters write v20 snapshots that no older build reads, every later update carries `tracks_enabled`, the snapshot-query lane is refused, a data-plane node without `track_manifests_v1` is refused at registration, and no voter, verifier or SNode may run an older image again. The rollbacks of §10.1 stop being a way back.

This update changes only `tracks_enabled`. `--max-writers 2` restates the current runtime limit (the flag is mandatory, and `1` is below the two enrolled writers); `--si-indexer` and `--verifier` are omitted, so the committed lists are carried, as are the table registry and the client lanes. Never combine it with another parameter change. Do not restart a voter, verifier or SNode while it runs.

Confirm before running.

```bash
[ "$(si_node_image a)" = "$(si_stage2_image)" ] && [ "$(si_node_image b)" = "$(si_stage2_image)" ] || { echo "both SI indexers must run the stage-2 image (HouseGate <HG_TAG>) before the activation; stop"; exit 1; }
[ "$(si_tracks | jq -r .tracks_enabled)" = false ] || { echo "tracks are already enabled; stop"; exit 1; }
si_show > "$si_backup/show-before-activation.json"
[ "$(jq -r .current.artifact_disposition_capability "$si_backup/show-before-activation.json")" = 0 ] || { echo "artifact_disposition_capability is not 0; the activation refuses it; stop"; exit 1; }
printf 'sealed %s promotion_seq %s\n' "$(si_sealed)" "$(jq -r .promotion_seq "$si_backup/show-before-activation.json")" | tee "$si_backup/activation-tip.txt"
leader="$(si_leader)"
epoch="$(jq -r .epoch "$si_backup/show-before-activation.json")"
si_arbiter "$leader" /usr/local/bin/arbiter-admin consensus update --address "$(si_voter "$leader")" \
  --expected-epoch "$epoch" --authority 0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705 --max-writers 2 --tracks-enabled \
  --voter "$(si_voter 0)" --voter "$(si_voter 1)" --voter "$(si_voter 2)" --timeout 20s | tee "$si_backup/activate.out"
```

`arbiter-admin` runs its own tracks gate before signing (every `--voter` and every node of the leader's feature book advertises `track_manifests_v1`, and `GetTrackState` answers `tracks_enabled: false`) and submits nothing on a refusal; the leader's gate (§6.1) and the FSM decide again. `InvalidArgument` means the FSM refused and names the first failing rule: `tracks activation requires an empty open block`, `tracks activation requires every sealed block to be safe`, `tracks activation requires the published manifest to cover the sealed tip`, `tracks activation requires artifact disposition capability 0`, `tracks activation requires an idle snapshot-query barrier`, or the drain gate's `consensus update requires all promotions to be acknowledged` and its cleanup and promoted-unsafe-part twins: wait for the drain, run `si_show`, and rerun the block, which reads the new epoch. On "commit outcome may be unknown", run `si_show` before anything else: an epoch one above `show-before-activation.json` with `tracks_enabled: true` means it committed.

### 6.3 Verify

```bash
si_show | tee "$si_backup/show-after-activation.json" | jq -c '{epoch, max_writers: .current.max_writers, tracks_enabled: .current.tracks_enabled, si_indexers: [.current.si_indexers[] | {indexer_id, snode_node_id}], verifiers: [.current.verifiers[].node_id]}'
cat "$si_backup/activation-tip.txt"
si_tracks | tee "$si_backup/tracks-after-activation.json" | jq -c '{tracks_enabled, cutover_block_seq, cutover_promotion_seq, global_safe_block_seq, published_safe_block_seq}'
for id in 0 1; do si_track "$id"; done
jq -c '.indexers[] | {id: .indexer_id, initial}' "$si_backup/tracks-after-activation.json"
for x in a b; do si_info "$x"; done
for i in 0 1 2; do si_track_metrics "$i"; done
```

Expected: epoch one above `show-before-activation.json`'s, `max_writers` `"2"`, `tracks_enabled` `true`, the two entries and three verifiers unchanged; `cutover_block_seq` and `cutover_promotion_seq` equal the `sealed` and `promotion_seq` of `activation-tip.txt`, and `global_safe_block_seq` and `published_safe_block_seq` equal the cutover block; for indexers 0 and 1 `prefix` and `published` equal the cutover block, `applied_promotion_seq` the cutover promotion sequence, `generation` `0`, `quarantined` `false`, no rejected or acknowledged blocks, and `initial` `{"indexer_id":…,"safe_block_seq":<cutover block>,"applied_promotion_seq":<cutover promotion seq>}` (the manifest published before the activation has no track entries, so `published` is the initial entry); both hosts `"quarantined":false` with the `registry_version` of `info-before-activation.json` (the activation does not bump the registry); every voter emits `arbiter_track_safe_block_seq`, `arbiter_track_published_block_seq`, `arbiter_track_quarantined` and `arbiter_track_quarantine_generation` for `indexer_id` `0` and `1`.

### 6.4 First track-aware manifests

indexer-a's acceptance write advances track 0 and triggers the first track-aware publication, which carries an entry for every enrolled indexer, idle indexer-b included (its initial entry):

Confirm before running.

```bash
si_accept_a_track 1002
si_tracks | tee "$si_backup/tracks-first-publication.json" | jq -c '{global: .global_safe_block_seq, published: .published_safe_block_seq, tracks: [.indexers[] | {id: .indexer_id, prefix: .prefix_block_seq, published: .published.safe_block_seq, applied: .published.applied_promotion_seq}]}'
si_global
```

Expected: `track 0 published <cutover> -> <n>` and `1`; track 0's `prefix` and `published` equal `n`, track 1's `published` and `applied` still the cutover values (Initialised), and `global` = `published` = `sealed`.

Then indexer-b's own track. devuser1's owner creates the test table and writes two rows through its own local agent (housegate `<HG_TAG>` or any release with the 2026-10-09 agent; the key only in `HOUSEGATE_AGENT_KEY`). The table enters the registry with owner 1, so its table-set transition is a block of track 1, and it turns Active on indexer-b once track 1's published frontier covers that block:

Confirm before running.

```bash
key_file=/path/to/devuser1-owner.key
si_agent_start "$key_file"
si_track 1
si_agent_ch --database devuser1 --query 'CREATE TABLE s2_e2e (id UInt64, v String) ENGINE = MergeTree ORDER BY id'
for n in $(seq 1 60); do s="$(si_status b devuser1 s2_e2e | jq -r .status)"; echo "$(date -u +%H:%M:%S) $s"; [ "$s" = active ] && break; sleep 15; done
[ "$s" = active ] || { si_agent_stop; echo "devuser1.s2_e2e did not turn active on indexer-b; stop"; exit 1; }
si_status a devuser1 s2_e2e
si_track 1
base1="$(si_track_published 1)"
printf '1,a\n2,b\n' | si_agent_ch --database devuser1 --query 'INSERT INTO s2_e2e FORMAT CSV'
si_wait_track 1 "$base1"
si_source_ch b 'SELECT count() FROM hg_safe.devuser1__s2_e2e'
si_agent_ch --database devuser1 --query "SELECT count() FROM s2_e2e SETTINGS SQL_x_read_mode='safe'"
si_agent_stop
si_global
```

Expected: the table answers `pending` on indexer-b until its transition is published and its SNode has it ready, then `active`, while indexer-a answers `ordinary`; the second `si_track 1` shows `published` above the cutover (the transition block); `track 1 published <x> -> <y>` after the INSERT; `2` rows in indexer-b's `hg_safe` and from the safe read; `global` = `published` = `sealed` again. An `ordinary` answer on indexer-b means the agent did not route the CREATE to indexer-b: stop.

### 6.5 Snapshot v20

After the next Raft snapshot (the first comes only after the log passes `snapshot_threshold`, 8192 entries; repeat this block later until one exists), every voter's newest `state.bin` carries version byte 20, which the previous release refuses; a replay of a fresh copy under the new build writes v20 and holds the track state at once:

```bash
for i in 0 1 2; do si_arbiter "$i" sh -c 'for d in $(ls -dt /data/raft/snapshots/*/); do echo "$d $(head -c 5 "$d/state.bin" | od -An -tu1 | awk "{print \$5}")"; done'; done
si_arbiter 0 sh -c 'cat "$(ls -dt /data/raft/snapshots/*/ | head -1)state.bin"' > "$si_backup/v20-state.bin"
head -c 5 "$si_backup/v20-state.bin" | od -An -tu1 | awk '{print $5}'
"$R/bin/old/v20check" "$si_backup/v20-state.bin" || true
"$R/bin/new/v20check" "$si_backup/v20-state.bin"
export ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX="$(openssl rand -hex 32)"  # config.Load needs one; the offline replay signs nothing
work="$(mktemp -d)"
si_kubectl exec storage-integrity-si-v2-arbiter-0 -c arbiter -- tar -C /data -czf - raft > "$si_backup/arbiter-0-raft-after.tgz"
si_kubectl exec storage-integrity-si-v2-arbiter-0 -c arbiter -- cat /config/arbiter.yaml > "$si_backup/arbiter-0-after.yaml"
tar -C "$work" -xzf "$si_backup/arbiter-0-raft-after.tgz"
"$R/bin/new/statedigest" -data-dir "$work/raft" -config "$si_backup/arbiter-0-after.yaml" > "$si_backup/digest-0-after.txt" 2> "$si_backup/digest-0-after.err" || { echo "statedigest failed (a raft.db that fails to open is a torn copy; re-copy):"; cat "$si_backup/digest-0-after.err"; exit 1; }
grep -E '^(applied_commands|written_version|live_check|apply_error|key tracks )' "$si_backup/digest-0-after.txt"
rm -rf "$work"
```

Expected: the replay prints `written_version 20`, a `key tracks` line, no `apply_error` and a `live_check` with the epoch of §6.3. Once a snapshot exists: the first line of each voter (its newest snapshot) ends in `20`, the copied container's byte is `20`, `old` prints `restore_refused version 20: …` (unsupported snapshot version) and `new` prints `restore_ok version 20`; until then the loop prints only `19`s.

## 7. A stuck indexer-b track

Design §13 stage 2 step 3. Stopping an idle indexer-b would prove nothing: with no indexer-b block in flight nothing could stall even without tracks. So indexer-b is stopped right after a write of its own was admitted (the INSERT returns once the statement is admitted and its result claim registered) and before that block's promotion reaches its SNode: indexer-b's track then holds a sealed block that cannot become safe and the global prefix stops below it, while indexer-a must keep reaching safe on its own track. Seal-to-safe takes about 5 s on devnet2 (dynamic-table-set runbook §2.10), so the stop is issued in the same block as the INSERT, and §7.1 checks afterwards whether it came in time; a write that reached safe first is a miss, repeated once after §7.3. indexer-b's source ClickHouse keeps running, so the verifiers still fetch and verify the block; only its promotion waits for `snode-2`. While indexer-b is stopped, its ordinary service is down too, and every alert keyed on the global watermark (`arbiter_safe_watermark_block_seq` against `arbiter_sealed_block_seq`) reports a stall: that is the point; per-track progress is `arbiter_track_published_block_seq`.

### 7.1 Stop indexer-b holding an unpromoted write

Read-only precondition: every block is safe and published, and the test table is Active on indexer-b:

```bash
si_global
si_status b devuser1 s2_e2e
si_source_ch b 'SELECT count() FROM hg_safe.devuser1__s2_e2e'
```

Expected: `global`, `published` and `sealed` equal, `active`, and the rows so far (`2` on the first attempt).

Then the write and the stop, in one block. If anything fails after the scale command, indexer-b stays stopped: continue with §7.3.

Confirm before running.

```bash
key_file=/path/to/devuser1-owner.key
si_agent_start "$key_file"
before="$(si_sealed)"; rows="$(si_source_ch b 'SELECT count() FROM hg_safe.devuser1__s2_e2e')"
[ "$(si_tracks | jq -r .global_safe_block_seq)" = "$before" ] || { si_agent_stop; echo "not every sealed block is safe yet; wait and retry"; exit 1; }
printf '%s %s\n' "$before" "$rows" > "$si_backup/stuck-before.txt"
printf '3,stuck\n' | si_agent_ch --database devuser1 --query 'INSERT INTO s2_e2e FORMAT CSV'
si_kubectl scale statefulset/sentio-node-devnet2-indexer-b --replicas=0
si_agent_stop
for n in $(seq 1 60); do p="$(si_kubectl get pod sentio-node-devnet2-indexer-b-0 --ignore-not-found -o name)" && [ -z "$p" ] && break; sleep 5; done
si_b_quiesced
```

Expected: the INSERT succeeds and `indexer-b is quiesced`. Two minutes later, read-only:

```bash
sleep 120
read -r before rows < "$si_backup/stuck-before.txt"
sealed="$(si_sealed)"
si_tracks | tee "$si_backup/tracks-stuck.json" | jq -c '{global: .global_safe_block_seq, published: .published_safe_block_seq, tracks: [.indexers[] | {id: .indexer_id, prefix: .prefix_block_seq, published: .published.safe_block_seq}]}'
now_rows="$(si_source_ch b 'SELECT count() FROM hg_safe.devuser1__s2_e2e')"
echo "sealed before $before, now $sealed; hg_safe rows before $rows, now $now_rows"
for i in 0 1 2; do si_metrics "$i"; done
[ "$sealed" -gt "$before" ] || { echo "no block sealed after the write; stop"; exit 1; }
if [ "$(jq -r .global_safe_block_seq "$si_backup/tracks-stuck.json")" != "$before" ] || [ "$now_rows" != "$rows" ]; then
  echo "MISSED: the write reached safe before indexer-b stopped; restore indexer-b (§7.3), then repeat §7.1 once with the row 13,stuck-again"; exit 1
fi
echo "indexer-b's track holds block $((before + 1)), which cannot become safe while snode-2 is down"
```

Expected: `sealed` above `before`; the global prefix, and so `published`, still `before`; track 1's `prefix` and `published` at most `before`; the `hg_safe` rows unchanged; every voter's `arbiter_safe_watermark_block_seq` equal to `before` while `arbiter_sealed_block_seq` has moved; the final line. On `MISSED`, run §7.3 and repeat §7.1 once (the missed row is safe, so a second row is fine); on a second miss stop the experiment (arbiter's `TestTwoTracksStoppingOneSNodeStallsOnlyItsTrack` remains the proof) and continue with §8.

### 7.2 indexer-a keeps going

Confirm before running.

```bash
read -r before rows < "$si_backup/stuck-before.txt"
si_accept_a_track 1003
si_tracks | tee "$si_backup/tracks-isolated.json" | jq -c '{global: .global_safe_block_seq, published: .published_safe_block_seq, tracks: [.indexers[] | {id: .indexer_id, prefix: .prefix_block_seq, published: .published.safe_block_seq}]}'
jq -e --argjson b "$before" '(.global_safe_block_seq | tonumber) == $b and (.published_safe_block_seq | tonumber) == $b and ([.indexers[] | select(.indexer_id == "0") | .published.safe_block_seq | tonumber][0] > $b + 1) and ([.indexers[] | select(.indexer_id == "1") | .prefix_block_seq | tonumber][0] <= $b)' "$si_backup/tracks-isolated.json" >/dev/null || { echo "track 0 did not pass indexer-b's stuck block, or the global prefix moved; stop"; exit 1; }
echo "track 0 published past block $((before + 1)); track 1 and the global prefix stayed at $before"
for i in 0 1 2; do si_metrics "$i"; si_track_metrics "$i"; done
```

Expected: `track 0 published <p> -> <q>` with `q` above `before + 1`, and `1` (the global watermark did not move, so the stage-1 `si_accept_a` would have waited out its 8 minutes in vain); the success line; `arbiter_safe_watermark_block_seq` still `before` on every voter while `arbiter_track_published_block_seq{indexer_id="0"}` is `q` and `{indexer_id="1"}` unchanged. Every manifest published meanwhile carried track 1's entry unchanged.

### 7.3 Restore indexer-b

The StatefulSet goes back to one replica; `snode-2` re-registers (it is enrolled and advertises `track_manifests_v1`), and the leader re-sends the promotion `snode-2` never acknowledged, so track 1 passes its block and the global prefix catches up with the sealed tip.

Confirm before running.

```bash
si_kubectl scale statefulset/sentio-node-devnet2-indexer-b --replicas=1
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-b --timeout=15m
[ "$(si_node_image b)" = "$(si_stage2_image)" ] && echo "indexer-b runs the stage-2 image"
sealed="$(si_sealed)"
for n in $(seq 1 60); do [ "$(si_tracks | jq -r .published_safe_block_seq)" -ge "$sealed" ] && break; sleep 10; done
si_tracks | tee "$si_backup/tracks-restored.json" | jq -c '{global: .global_safe_block_seq, published: .published_safe_block_seq, tracks: [.indexers[] | {id: .indexer_id, prefix: .prefix_block_seq, published: .published.safe_block_seq}]}'
[ "$(jq -r .published_safe_block_seq "$si_backup/tracks-restored.json")" -ge "$sealed" ] || { echo "the published global prefix did not reach the sealed tip $sealed within 10 minutes; stop"; exit 1; }
si_source_ch b 'SELECT count() FROM hg_safe.devuser1__s2_e2e'
si_features_tracks
si_info b
for i in 0 1 2; do si_metrics "$i"; done
```

Expected: `indexer-b runs the stage-2 image`; within minutes `published` and `global` reach the sealed tip, track 1's `prefix` and `published` pass the stuck block, and each track's `published` equals its `prefix`; `hg_safe` holds one row more than `stuck-before.txt` recorded; `snode-2` is back in the feature book with `tracks: true`; `si_info b` `"quarantined":false`; every voter reports two active SNodes and the safe watermark equal to the sealed tip.

## 8. Quarantine and release

`QuarantineIndexer` and `ReleaseIndexer` are authority-signed commands outside the consensus update (design §7.4). Each names the indexer's current `quarantine_generation`, which every applied quarantine, release and automatic quarantine increments, so an old command can never be replayed; read it from `consensus tracks` right before signing. Each bumps the table registry once, which carries the quarantined set to every host. Replays of old commands and the rejected-block path are exercised only by arbiter's tests (`TestQuarantineReplayAfterReleaseIsRefused`, `TestReleaseReplayAfterLaterQuarantineIsRefused`, `TestReleaseSignedBeforeAutomaticQuarantineIsRefused`, `TestQuarantineAndReleaseOverTheWire`, `TestRejectedBlockQuarantinesAndReleaseCleansItsParts`), not here.

### 8.1 Quarantine indexer-b

Confirm before running.

```bash
si_global
[ "$(si_track 1 | jq -r .quarantined)" = false ] || { echo "indexer 1 is already quarantined; stop"; exit 1; }
g="$(si_tracks | jq -r '.indexers[] | select(.indexer_id == "1") | .quarantine_generation')"
for x in a b; do si_info "$x"; done | tee "$si_backup/info-before-quarantine.json"
leader="$(si_leader)"
si_arbiter "$leader" /usr/local/bin/arbiter-admin indexer quarantine --address "$(si_voter "$leader")" \
  --indexer 1 --expected-generation "$g" --reason "devnet2 multi-source tracks runbook §8: quarantine exercise" --timeout 20s | tee "$si_backup/quarantine.out"
```

Then, read-only (the hosts follow the registry within seconds):

```bash
sleep 10
si_track 1 | tee "$si_backup/track-1-quarantined.json"
si_track 0
for i in 0 1 2; do si_track_metrics "$i" | grep 'indexer_id="1"'; done
for x in a b; do si_info "$x"; done | tee "$si_backup/info-quarantined.json"
si_refusal b devuser1 s2_e2e
si_refusal a devuser1 s2_e2e
```

Expected: `quarantine.out` holds the committed answer naming indexer 1 and the expected generation; track 1 with `generation` one above `g`, `quarantined` `true` and the reason; track 0 unchanged (`generation` `0`); `arbiter_track_quarantined{indexer_id="1"} 1` and the new generation on every voter; `si_info b` `"quarantined":true` and `si_info a` `"quarantined":false`, both with `registry_version` one above `info-before-quarantine.json`; indexer-b answers `{"status":"refused","refused_code":"indexer_quarantined","refused_reason":"storage-integrity indexer 1 is quarantined",…}` for its table and indexer-a `ordinary`. `FailedPrecondition` with `expected_generation <n> differs from quarantine generation <m> of indexer 1` means the generation moved since it was read: read it again.

### 8.2 What a quarantined indexer serves

The `devuser1` owner tries a write and a read (the agent caches a table status for 5 s; §8.1 waited longer). Both are refused with the non-retryable ClickHouse code 392. The usual message comes from indexer-b's own table state, which reports the table Refused (arbiter stage-2 plan C13): `storage_integrity: table devuser1.s2_e2e was refused: indexer_quarantined: storage-integrity indexer 1 is quarantined`. In the seconds after §8.1 in which a host or an agent still saw the table Active, the write may instead get `storage_integrity: table devuser1.s2_e2e is not active; the signed lane admits only active tables` or the arbiter's own refusal `storage_integrity: the indexer that owns table devuser1.s2_e2e is quarantined; writes to it are refused until the indexer is released (not retryable)`. `sipeerguard` answers 392 too, so the check matches the message, never the code alone:

Confirm before running.

```bash
key_file=/path/to/devuser1-owner.key
si_agent_start "$key_file"
quarantine='was refused: indexer_quarantined: storage-integrity indexer 1 is quarantined|is not active; the signed lane admits only active tables|the indexer that owns table devuser1\.s2_e2e is quarantined; writes to it are refused until the indexer is released \(not retryable\)'
if out="$(printf '4,refused\n' | si_agent_ch --database devuser1 --query 'INSERT INTO s2_e2e FORMAT CSV' 2>&1)"; then si_agent_stop; echo "the INSERT was accepted while indexer 1 is quarantined; stop"; exit 1; fi
printf '%s\n' "$out" | tail -n 2
printf '%s\n' "$out" | grep -q 'Code: 392' && printf '%s\n' "$out" | grep -qE "$quarantine" || { si_agent_stop; echo "not the quarantine refusal of the write; stop"; exit 1; }
if out="$(si_agent_ch --database devuser1 --query 'SELECT count() FROM s2_e2e' 2>&1)"; then si_agent_stop; echo "the read was served while indexer 1 is quarantined; stop"; exit 1; fi
printf '%s\n' "$out" | tail -n 2
printf '%s\n' "$out" | grep -q 'Code: 392' && printf '%s\n' "$out" | grep -qF 'was refused: indexer_quarantined:' || { si_agent_stop; echo "not the quarantine refusal of the read; stop"; exit 1; }
si_agent_stop
si_accept_a_track 1004
for i in 0 1 2; do si_arbiter "$i" wget -qO- http://127.0.0.1:9090/metrics | grep -E '^arbiter_admission_rejects_total' || true; done
```

Expected: both refusals carry 392 and one of the messages above (normally the first); indexer-a's acceptance write reaches track 0's published frontier and reads `1`; the `ADMISSION_CODE_INDEXER_QUARANTINED` series of `arbiter_admission_rejects_total` is absent or counts only a write a host still let through.

### 8.3 Release indexer-b

A release must acknowledge exactly the rejected blocks the track lists; after a manual quarantine there are none, so no `--acknowledge-rejected` is given. On the host the table turns Active again, and the first write may get the retryable 733 `storage_integrity: table devuser1.s2_e2e is being activated; retry shortly (retryable)` until HouseGate's merge guard has re-asserted the table (within its 30 s reassert interval).

Confirm before running.

```bash
[ "$(si_track 1 | jq -r .quarantined)" = true ] || { echo "indexer 1 is not quarantined; stop"; exit 1; }
[ "$(si_track 1 | jq -r '.rejected | length')" = 0 ] || { echo "indexer 1 lists rejected blocks: review them and release with --acknowledge-rejected for exactly those (§11); stop"; exit 1; }
g="$(si_tracks | jq -r '.indexers[] | select(.indexer_id == "1") | .quarantine_generation')"
leader="$(si_leader)"
si_arbiter "$leader" /usr/local/bin/arbiter-admin indexer release --address "$(si_voter "$leader")" \
  --indexer 1 --expected-generation "$g" --timeout 20s | tee "$si_backup/release.out"
```

Then the writes resume:

Confirm before running.

```bash
sleep 10
si_track 1 | tee "$si_backup/track-1-released.json"
for x in a b; do si_info "$x"; done
for n in $(seq 1 24); do s="$(si_status b devuser1 s2_e2e | jq -r .status)"; [ "$s" = active ] && break; sleep 5; done
echo "indexer-b answers $s"
key_file=/path/to/devuser1-owner.key
si_agent_start "$key_file"
base1="$(si_track_published 1)"
if ! out="$(printf '5,resumed\n' | si_agent_ch --database devuser1 --query 'INSERT INTO s2_e2e FORMAT CSV' 2>&1)"; then
  printf '%s\n' "$out" | tail -n 2
  printf '%s\n' "$out" | grep -qF 'is being activated; retry shortly (retryable)' || { si_agent_stop; echo "not the retryable activation refusal; stop"; exit 1; }
  sleep 35
  printf '5,resumed\n' | si_agent_ch --database devuser1 --query 'INSERT INTO s2_e2e FORMAT CSV'
fi
si_wait_track 1 "$base1"
si_agent_ch --database devuser1 --query "SELECT count() FROM s2_e2e SETTINGS SQL_x_read_mode='safe'"
si_agent_stop
si_global
```

Expected: `release.out` holds the committed answer; track 1 with `generation` one above `g`, `quarantined` `false` and an empty reason; both hosts `"quarantined":false` with `registry_version` one above `info-quarantined.json`; indexer-b answers `active`; the INSERT is accepted (at most one retryable 733 first); `track 1 published …` advances; the safe read returns the rows written so far (`4`: rows 1, 2, 3 and 5, when §7.1 needed one attempt); `global` = `published` = `sealed`.

## 9. End

devuser1's owner drops the test table: the drop is a table-set transition of track 1, after which the table is purged from indexer-b's source and every verifier. Then the final state:

Confirm before running.

```bash
key_file=/path/to/devuser1-owner.key
si_agent_start "$key_file"
si_agent_ch --database devuser1 --query 'DROP TABLE s2_e2e'
si_agent_stop
for n in $(seq 1 90); do s="$(si_status b devuser1 s2_e2e | jq -r .status)"; echo "$(date -u +%H:%M:%S) $s"; [ "$s" = pending ] && break; sleep 20; done
si_source_ch b "SELECT database, name FROM system.tables WHERE name = 'devuser1__s2_e2e'"
for n in 1 2 3; do si_verifier_ch "$n" "SELECT database, name FROM system.tables WHERE name = 'devuser1__s2_e2e'"; done
si_accept_a_track 1005
si_tracks | tee "$si_backup/tracks-end.json" | jq -c '{global: .global_safe_block_seq, published: .published_safe_block_seq, tracks: [.indexers[] | {id: .indexer_id, prefix: .prefix_block_seq, published: .published.safe_block_seq, generation: .quarantine_generation, quarantined}]}'
si_global
for i in 0 1 2; do si_metrics "$i"; si_track_metrics "$i"; si_arbiter "$i" wget -qO- http://127.0.0.1:9090/metrics | grep -E '^arbiter_admission_rejects_total' || true; done
```

Expected: the table answers `gone`, then `pending` once purged (a purged name is unrecorded, and indexer-b governs `devuser1`), and no `devuser1__s2_e2e` table is left on indexer-b's source or any verifier; indexer-a's acceptance write reaches track 0 and reads `1`; `global` = `published` = `sealed`, each track's `published` equals its `prefix`, track 0 `generation` `"0"` and track 1 `"2"`, neither quarantined; `ADMISSION_CODE_SOURCE_UNAVAILABLE` only from the windows named at the top.

## 10. Rollback

### 10.1 Before §6.2

Revert in reverse order of the commits: indexer-b's, indexer-a's, then the verifier commit, then replace the voters back. Until the activation the voters write v19 snapshots and no command carries a new field, so the previous release reads everything they wrote, and the stage-1 sentio-node image serves both hosts as before. A step that has not run has no commit file and is skipped.

Confirm before running.

```bash
git fetch origin main
git merge --ff-only origin/main
for x in indexer-b indexer-a verifiers; do
  f="$si_backup/stage2-$x-commit"
  [ -f "$f" ] || { echo "no $x commit: skipped"; continue; }
  c="$(cat "$f")"; git show --stat "$c"
  git revert --no-edit "$c"
  python3 -B charts/storage-integrity/tests/test_render.py
  git push origin HEAD:main
  if [ "$x" = verifiers ]; then
    helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
    si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
  else
    helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector "name=sentio-node-devnet2-$x"
    si_kubectl rollout status "statefulset/sentio-node-devnet2-$x" --timeout=15m
  fi
done
leader="$(si_leader)"
for i in $(printf '0\n1\n2\n' | grep -vx "$leader") "$leader"; do si_replace_voter "$i"; done
si_images
```

The reverted pin is the voters' template again, so each deleted voter returns on `v0.11.0` (skip the loop if `si_images` shows them there already). `WRITER_AUTHORIZATION_IMAGES` keeps listing the stage-2 image (stage 0), which is harmless.

### 10.2 After §6.2

There is no rollback. The committed `tracks_enabled`, the v20 snapshots and the track rules cannot be undone (`consensus update must carry tracks_enabled once set`); a voter, verifier or SNode without `track_manifests_v1` is refused at registration (`tracks are active; this binary does not support track-aware manifests`). Fix forward. A stuck or misbehaving indexer stays in its own track: bring its SNode back on its PVC, and quarantine it (§8) when a consensus update must pass the drain gate meanwhile or its tables must stop being served.

### 10.3 During §7 and §8

`si_kubectl scale statefulset/sentio-node-devnet2-indexer-b --replicas=1` ends a stop of §7 at any time (§7.3); `arbiter-admin indexer release` with the generation `consensus tracks` shows ends a quarantine of §8 (§8.3).

## 11. Operating rules

These extend §11 of the stage-1 runbook, which stays in force except where noted.

- After §6.2, never run a voter, verifier or SNode below `<ARBITER_TAG>` / `<SN_COMMIT>`: tracks cannot be turned off, a data-plane node without `track_manifests_v1` is refused at registration, and no older build reads v20.
- Every later consensus update carries `tracks_enabled`; `arbiter-admin consensus update` carries it whether or not `--tracks-enabled` is given. Restated `si_indexers` entries stay byte-identical (taken from `consensus show`), as before.
- A stuck indexer stalls only its own track; this supersedes the stage-1 coupling rule (design D12). The global safe watermark stays the contiguous prefix over every block and stops while any track is stuck: judge an indexer's progress by its track (`arbiter-admin consensus tracks`, `arbiter_track_published_block_seq{indexer_id}`), never by the global watermark alone.
- The drain gate of a consensus update still waits for every unacknowledged promotion, pending cleanup and promoted unsafe part of a track that is not quarantined. To pass an update while an indexer's SNode is down and holds such work, quarantine that indexer first.
- Quarantine and release (`arbiter-admin indexer quarantine|release --address <leader> --indexer <id> --expected-generation <n> …`): read the generation from `consensus tracks` right before signing; a quarantine needs a reason. While an indexer is quarantined its host refuses reads and writes of its own tables (392, `refused_code` `indexer_quarantined`), including the default `unsafe_latest` reads; its pending promotions and cleanups stay recorded but are not dispatched; the arbiter refuses its statements with `INDEXER_QUARANTINED`. After a release the suspended work is re-sent, re-signed by the current authority, and the first write may get the retryable 733 until the merge guard re-asserts the table.
- A challenge that resolves a block of a track as Rejected quarantines that track automatically, with reason `block <n> rejected`, and lists the block under `rejected_blocks` in `consensus tracks`. Review the rejection first; then release with `--acknowledge-rejected <n>` for exactly the listed blocks and the generation `consensus tracks` shows (a release signed before the automatic quarantine is refused by the generation). The release counts the block as passed and schedules the cleanup of its candidate parts on that indexer's SNode.
- Before any authority rotation, every SNode's overlay must list the new authority in `storageIntegrity.snode.authorityAddresses`, synced and restarted: SNodes authorize promotions and cleanups against that static list, with no epoch, so work suspended by a quarantine and re-sent after the rotation, signed by the new authority, is refused otherwise (arbiter stage-2 plan C12).
- The leader groups submissions by owner within its submission grouping window (`ingress.submission_grouping_window`, binary default 50 ms); the chart exposes no value for it, and `test_render.py` keeps it that way.
- The snapshot-query lane stays off: the arbiter refuses `BeginSnapshotQuery` while tracks are enabled (C7).
- SNode node ids come from the stage-1 runbook's §1.2 ledger (next: `snode-3`) and are never reused. An indexer enrolled after §6.2 starts its track at the global safe prefix and `PromotionSeq` of its enrolment commit, and its SNode must run a stage-2 image.
- Tracks satisfy only the first condition of design §17. Enrol a third-party indexer only once the other gates there exist (TLS, the cross-operator replication transport, signed and quota-bounded DA writes, per-indexer admission rate limits, verifier capacity); until then enrol only self-operated indexers.
- Agent keys go only through `HOUSEGATE_AGENT_KEY`, never on argv.
````

In `docs/storage-integrity-devnet2-multi-source.md` (each paragraph stays on one line):
- After the stage-7 row of the stage table (`:14`) add `| 8 | Multi-source stage 2 (tracks and quarantine): see [storage-integrity-devnet2-multi-source-tracks.md](storage-integrity-devnet2-multi-source-tracks.md) | See that runbook (§10) |`.
- `:16`: append " The tracks activation of [storage-integrity-devnet2-multi-source-tracks.md](storage-integrity-devnet2-multi-source-tracks.md) ends that coupling: from it on a stuck indexer stalls only its own track, and that runbook supersedes every coupling statement here (§7, §10.4, §11)."
- `:651`: replace "a stuck indexer-b stalls the safe progress of every indexer until stage 2." with "a stuck indexer-b stalls the safe progress of every indexer until the tracks activation ([storage-integrity-devnet2-multi-source-tracks.md](storage-integrity-devnet2-multi-source-tracks.md)), and only its own track afterwards." and "until it returns (§11)." with "until it returns, or, after the tracks activation, until indexer-b is quarantined (§11)."
- `:808`: replace "stalls every indexer's safe progress until indexer-b returns." with "stalls every indexer's safe progress until indexer-b returns (after the tracks activation of [storage-integrity-devnet2-multi-source-tracks.md](storage-integrity-devnet2-multi-source-tracks.md), only indexer-b's own track)."
- `:823`: replace the whole bullet with "- Between the signed-claims activation and the tracks activation of [storage-integrity-devnet2-multi-source-tracks.md](storage-integrity-devnet2-multi-source-tracks.md), an enrolled indexer whose SNode is down blocks the drain gate of every later consensus update (a verifier or signer rotation included) and the safe prefix of every indexer until it returns; until then there is no authority command to set its promotions or cleanups aside. Stopping indexer-b's SNode after its tables hold SI writes therefore stalls every indexer's safe progress until it returns (design D12). After the tracks activation it stalls only its own track, and `arbiter-admin indexer quarantine` takes its work out of the drain gate (§11 of that runbook, which supersedes this rule). Evicting a node needs `arbiter-admin node evict --expected-registration-seq` with its committed `registration_seq` (its `state.json` or `registration.json`; 0 for a node that has not registered since the activation), and never frees its id."
- `:827`: append " The tracks activation ([storage-integrity-devnet2-multi-source-tracks.md](storage-integrity-devnet2-multi-source-tracks.md)) satisfies only the first condition of design §17; the others still gate any third-party enrolment (§11 there)."

In `docs/storage-integrity-devnet2-dynamic-table-set.md`: after the stage-6 row (`:16`) add `| 7 | Multi-source stage 2: tracks and quarantine; see [storage-integrity-devnet2-multi-source-tracks.md](storage-integrity-devnet2-multi-source-tracks.md) | See that runbook (§10) |`, and append to §8 Operating rules (after `:943`):

```markdown
- Multi-source stage 2 ([storage-integrity-devnet2-multi-source-tracks.md](storage-integrity-devnet2-multi-source-tracks.md) §11): once tracks are active, never run a voter, verifier or SNode below `<ARBITER_TAG>` / `<SN_COMMIT>`; a stuck indexer stalls only its own track, so judge each indexer by its track, not by the global safe watermark; quarantine an indexer before a consensus update has to pass its unacknowledged work.
```

Fill the ledger values and check the runbook's conventions (the §0 helpers form exactly one bash block, which Task 18's extractor takes; every block that mutates anything is preceded by `Confirm before running.`):

```bash
cd "$W" && sed -i '' -e "s/<ARBITER_TAG>/$ARBITER_TAG/g" -e "s/<ARBITER_DIGEST>/$ARBITER_DIGEST/g" -e "s/<PROTO_TAG>/$PROTO_TAG/g" -e "s/<CORE_TAG>/$CORE_TAG/g" \
  -e "s/<HG_TAG>/$HG_TAG/g" -e "s/<SN_COMMIT>/$SN_COMMIT/g" -e "s/<SN_DIGEST>/$SN_DIGEST/g" \
  docs/storage-integrity-devnet2-multi-source-tracks.md docs/storage-integrity-devnet2-multi-source.md docs/storage-integrity-devnet2-dynamic-table-set.md
! git grep -nE '<(ARBITER_TAG|ARBITER_DIGEST|PROTO_TAG|CORE_TAG|HG_TAG|SN_COMMIT|SN_DIGEST)>' -- docs
python3 -I - docs/storage-integrity-devnet2-multi-source-tracks.md <<'EOF'
import re, sys
text = open(sys.argv[1]).read()
start = text.index("## 0. Session helpers")
end = text.index("\n## ", start + 1)
blocks = list(re.finditer(r"```bash\n(.*?)```", text, re.S))
assert len([b for b in blocks if start < b.start() < end]) == 1, "section 0 must hold exactly one bash block"
mutating = re.compile(r"git push|helmfile .* sync|si_kubectl scale|rollout restart|si_replace_voter|consensus update|indexer quarantine|indexer release|si_agent_start|si_accept_a")
missing = []
for block in blocks:
    if start < block.start() < end or not mutating.search(block.group(1)):
        continue
    previous = [line for line in text[:block.start()].splitlines() if line.strip()][-1]
    if previous != "Confirm before running.":
        missing.append(block.group(1).splitlines()[0])
if missing:
    sys.exit("mutating blocks without 'Confirm before running.': " + " | ".join(missing))
print("helpers block found; every mutating block asks for confirmation")
EOF
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -2
git -C "$W" add docs && git -C "$W" commit -m "docs(storage-integrity): devnet2 multi-source tracks and quarantine runbook

Offline replay check under v0.11.0 and $ARBITER_TAG, verifiers, indexer-a
and indexer-b on sentio-node $SN_COMMIT, the voters one at a time, the
re-registration and drain, the irreversible tracks activation, the
first track-aware manifests and snapshot v20, a deliberately stuck
indexer-b track while indexer-a keeps reaching safe, and quarantine and
release of indexer-b (housegate spec 2026-10-10 §13 stage 2). The
stage-1 runbook's coupling statements and the dynamic-table-set
runbook's stage table point to it.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: no placeholder left; `helpers block found; every mutating block asks for confirmation`; `OK`.

- [ ] **Step 6: PR (requires explicit user confirmation)**

`docs/`, the helmfile comments and the render tests go through a PR on `sentioxyz/production` (the user's rule: runbooks and chart tests need a PR; the live pins of Task 18 are single-service commits to `main` after it merges):

```bash
cd "$W" && git push -u origin HEAD
gh pr create --repo sentioxyz/production --base main --title "storage-integrity: devnet2 multi-source tracks and quarantine runbook (stage 2)" --body "$(cat <<'EOF'
## Summary
- New runbook `docs/storage-integrity-devnet2-multi-source-tracks.md` (housegate spec 2026-10-10 §13, stage 2): offline replay check, verifiers, both SNodes, voters, the irreversible tracks activation (`--tracks-enabled --max-writers 2`), v20, a deliberately stuck indexer-b track, quarantine and release of indexer-b, rollback and operating rules.
- The stage-1 runbook's coupling statements (design D12) and the devnet2 helmfile comments now end with the tracks activation; both earlier runbooks gain a stage row.
- `test_render.py`: the stage-2 sentio-node image is listed in `WRITER_AUTHORIZATION_IMAGES` before either pin moves; a test pins that the chart renders no submission grouping window (the binary default applies).

Inert on merge: no chart, values or overlay change; every devnet2 release renders byte-identically. The image pins are single-service commits made by the runbook after this merges.

## Test plan
- [x] `python3 -B charts/storage-integrity/tests/test_render.py` (86 tests)
- [x] every devnet2 release renders byte-identically to `origin/main`
- [x] the runbook's §0 helpers form one block; every mutating block asks for confirmation

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: the PR URL; the `Storage-integrity chart validation` workflow passes. Merge only after review and an explicit user yes; record the PR number in the release ledger.

---

## Task 18: devnet2 — stage-2 rollout: replay check, verifiers, SNodes, voters, tracks activation, stuck track, quarantine and release (S2, live)

**Interfaces:**
- Consumes: the merged Task 17 PR (runbook `docs/storage-integrity-devnet2-multi-source-tracks.md` with its §0 helpers, the allowlisted image, the render tests); `ARBITER_TAG` / `ARBITER_DIGEST`, `CORE_TAG`, `HG_TAG`, `PROTO_TAG` (Plan S2-A); `SN_COMMIT` / `SN_DIGEST` (Task 16); Plan S2-A Task 14's `arbiter-admin consensus update … --tracks-enabled` (with `--voter` accepted for its gate), `consensus tracks`, `indexer quarantine`, `indexer release`; Task 13's leader tracks gate, `GetTrackState`, the `arbiter_track_*` gauges and the post-activation registration refusal; Task 12's snapshot v20; Task 16's `quarantined` and Refused reporting; the stage-1 tool sources `$HOME/si-devnet2-multisource-rehearsal/statedigest/main.go` and `$HOME/si-devnet2-multisource-rehearsal/v19check/main.go` (Plan S1-A Task 12 Step 7); the §0 helpers of the dynamic-table-set and stage-1 runbooks (`si_replace_voter`, `si_b_quiesced`, `si_accept_a`, …).
- Produces: tracks active on devnet2 (`TRACKS_EPOCH`, `TRACKS_CUTOVER` = `{block_seq, promotion_seq}`), the three pin commits on production `main`, the evidence under `$HOME/si-devnet2-multisource-tracks` and `$HOME/si-devnet2-multisource-tracks-rehearsal`, and the outcome record in a production docs PR.

**Conventions for this task.**
- The agent runs every bash block with the Bash tool exactly as

  ```bash
  bash -eo pipefail <<'EOF'
  source "$HOME/si-devnet2-multisource-tracks/helpers.sh"
  cd "$P"
  <the block>
  EOF
  ```

  `helpers.sh` (Step 1) holds the three §0 helper blocks (dynamic-table-set, stage-1, stage-2 runbooks, in that order) and `P`, the live production worktree; every cross-block value lives in `$si_backup` (`$HOME/si-devnet2-multisource-tracks`). A block that may wait longer than 10 minutes runs with `run_in_background: true`; read its output before the next step.
- Every mutation waits for an explicit yes in chat, given for that step only: pushes to production `main`, `helmfile sync`, `kubectl scale`, pod deletions and `rollout restart`, the acceptance INSERTs, `arbiter-admin consensus update`, `indexer quarantine`, `indexer release`, PR creation and merge. Before asking, the agent states exactly what will change (each step's "Gate:" sentence). Read-only steps need no confirmation.
- The `devuser1` owner's statements run in the user's own fish terminal, with the key only in `HOUSEGATE_AGENT_KEY` (never in chat or on argv); the runbook blocks that start that agent (§6.4's second block, §7.1's write block, §8.2, §8.3's second block, §9) are split: the user runs the agent's statements in fish, and the agent runs the remaining lines. The user keeps that fish session (and its background agent) open from Step 12 to Step 19.
- **IRREVERSIBLE POINT: Step 10 (the tracks activation).** Before it the way back is runbook §10.1; after it, none (§10.2). The stop of Step 14 and the quarantine of Step 17 are undone within the task (Steps 16 and 18).
- Stop on any unexpected output and report it; never improvise a fix on the cluster.

- [ ] **Step 1: Live worktree and helpers (local only)**

After the Task 17 PR has merged:

```bash
git -C /Users/uranuswch/Dev/sentio_xyz/production fetch origin
bash -c '
repo=$1 runtime=$2 name=$3 branch=${4:-urwt/$2/$3} base=${5:-origin/main}
case $name in ""|[._-]*|*[!A-Za-z0-9._-]*) echo "urwt: invalid task name: $name" >&2; exit 2;; esac
if [[ $branch == pr:* || $branch == https://* ]] ||
   git -C "$repo" show-ref --quiet --verify "refs/heads/$branch" ||
   git -C "$repo" show-ref --quiet --verify "refs/remotes/origin/$branch"; then
  create=()
else
  create=(--create --base "$base")
fi
command wt -C "$repo" \
  --config-set "worktree-path = \"~/src/remotesrc/$runtime/{{ repo }}/$name\"" \
  switch "${create[@]}" "$branch" --no-cd --format=json
' urwt /Users/uranuswch/Dev/sentio_xyz/production claude multi-source-s2-live
```

With `P=<path from the JSON>`:

```bash
P='<path from the JSON>'   # replace with the "path" field printed above
case "$P" in '<'*) echo "set P first"; exit 1;; esac
[ ! -e "$HOME/si-devnet2-multisource-tracks" ] || { echo "$HOME/si-devnet2-multisource-tracks exists: an evidence directory is never reused; move it aside"; exit 1; }
mkdir -p -m 700 "$HOME/si-devnet2-multisource-tracks"
{ python3 -I - "$P/docs/storage-integrity-devnet2-dynamic-table-set.md" "$P/docs/storage-integrity-devnet2-multi-source.md" "$P/docs/storage-integrity-devnet2-multi-source-tracks.md" <<'PY'
import re, sys
for path in sys.argv[1:]:
    section = open(path).read().split("## 0. Session helpers", 1)[1].split("\n## ", 1)[0]
    blocks = re.findall(r"```bash\n(.*?)```", section, re.S)
    if not blocks:
        sys.exit(f"{path}: no bash block in section 0")
    print(blocks[0])
PY
  printf 'P=%q\n' "$P"; } > "$HOME/si-devnet2-multisource-tracks/helpers.sh"
bash -c 'source "$HOME/si-devnet2-multisource-tracks/helpers.sh" && type si_show si_replace_voter si_b_quiesced si_accept_a si_features_tracks si_tracks si_track si_wait_track si_accept_a_track si_stage2_image si_refusal si_agent_start >/dev/null && echo "helpers ok: si_backup=$si_backup R=$R P=$P"'
git -C "$P" log --oneline -1 && git -C "$P" grep -c 'si_features_tracks()' -- docs/storage-integrity-devnet2-multi-source-tracks.md
```

Expected: `helpers ok: si_backup=$HOME/si-devnet2-multisource-tracks R=$HOME/si-devnet2-multisource-tracks-rehearsal P=…/multi-source-s2-live`; the head is `origin/main` with the merged PR; count `1`. Before every later commit: `git -C "$P" fetch origin && git -C "$P" merge --ff-only origin/main`.

- [ ] **Step 2: Pins, identities, baseline (read-only)** — runbook §1.1, §1.2 and §1.3 blocks.

Expected: `pins resolve`; the identities of §1.2 exactly (indexer 0 signer `0x22a6…31f4` / 32003, indexer 1 signer `0x20c8…16f3` / 33003, `devuser1` → indexer 1); the §1.3 baseline: epoch `"4"` (re-read; record whatever it is), `max_writers` `"2"`, capability `0`, client lanes 256, `tracks_enabled` `null`, entries 0 and 1, `verifier-1..3`, safe = sealed, six arbiter pods on `sha256:7a06d389…`, both indexers on `2db130e…@sha256:fbb1ef32…`, voters' `features` `["client_lanes_v1","signed_claims_v1"]`, no track gauges, no `quarantined` key in either `si_info`. Record the leader (`si_leader`), the epoch and `registry_version`.

- [ ] **Step 3: Rehearsal binaries (local only; reads one snapshot container)** — runbook §2.1 with `arbiter_repo=/Users/uranuswch/Dev/sentio_xyz/arbiter` and `NEW_TAG` = `ARBITER_TAG`.

Expected: both builds compile both tools; `19` and `restore_ok version 19` twice from the live container. A tool that fails to compile against `ARBITER_TAG` (an FSM API it calls changed) is adapted in `$R/src/new-<tag>/cmd/…` only, mirroring that tag's `cmd/arbiter`, and the change is reported.

- [ ] **Step 4: Offline replay check (read-only on the cluster; local CPU)** — runbook §2.2 and §2.3.

Expected: `old matches the voters' live release v0.11.0`; three `old and new replay identically`; `written_version 19` from both builds, a `key registration_seqs` line and no `key tracks`; `live_check … epoch=<Step 2's epoch> …` equal to `show-replay.json`. Record the date (UTC, `YYYY-MM-DD`) as `REPLAY_DATE` for Step 5. Any difference: stop; nothing has changed yet.

- [ ] **Step 5: Verifiers to `ARBITER_TAG` (requires explicit user confirmation)**

In `$P`, one commit. In `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml`, lines 72-83 become:

```yaml
    # arbiter <ARBITER_TAG> (<ARBITER_COMMIT_SHORT>), pinned here rather than in the
    # chart default so a later chart pin cannot change this network's consensus
    # runtime unreviewed. It carries arbiter-core <CORE_TAG> and the multi-source
    # stage-2 FSM (tracks: one owner per block, per-track safe progress and
    # manifests, authority-signed quarantine and release, snapshot v20), inert
    # until the authority-signed tracks activation. The live voters' Raft logs
    # replay identically under v0.11.0 and <ARBITER_TAG> (offline statedigest
    # check, <DATE>). The verifiers (RollingUpdate) move on sync while the voters
    # (OnDelete) keep running v0.11.0 until they are replaced one at a time; see
    # docs/storage-integrity-devnet2-multi-source-tracks.md §3 and §5.
    # Previous pin: v0.11.0 sha256:7a06d389b6c9a313ca4ce48837b5fcf2193184a21c5f0b68c91e2123f78a17fb
    digest: <ARBITER_DIGEST>
```

and in `charts/storage-integrity/tests/test_render.py`, lines 30-33 become:

```python
# The Arbiter runtime devnet2-si-v2 runs. Each arbiter upgrade
# (docs/storage-integrity-devnet2-multi-source-tracks.md §3 and the runbooks
# before it) changes it together with arbiter.image.digest in
# storage-integrity-si-v2.yaml.
DEVNET2_ARBITER_DIGEST = "<ARBITER_DIGEST>"
```

```bash
ARBITER_TAG=<ledger>; ARBITER_DIGEST=<ledger>; CORE_TAG=<ledger>; REPLAY_DATE=<Step 4>
ARBITER_COMMIT_SHORT="$(git -C /Users/uranuswch/Dev/sentio_xyz/arbiter rev-parse --short=8 "$ARBITER_TAG^{commit}")"
cd "$P" && sed -i '' -e "s/<ARBITER_TAG>/$ARBITER_TAG/g" -e "s/<ARBITER_COMMIT_SHORT>/$ARBITER_COMMIT_SHORT/g" -e "s/<CORE_TAG>/$CORE_TAG/g" -e "s/<DATE>/$REPLAY_DATE/g" -e "s/<ARBITER_DIGEST>/$ARBITER_DIGEST/g" \
  k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml charts/storage-integrity/tests/test_render.py
! git -C "$P" grep -nE '<(ARBITER_TAG|ARBITER_COMMIT_SHORT|CORE_TAG|DATE|ARBITER_DIGEST)>' -- k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml charts/storage-integrity/tests/test_render.py
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -1
git -C "$P" add -A && git -C "$P" commit -m "chore(devnet2): arbiter $ARBITER_TAG for multi-source stage 2 (tracks)

Verifiers move on sync; voters are OnDelete and keep v0.11.0 until
replaced one at a time (docs/storage-integrity-devnet2-multi-source-tracks.md
§3, §5). The voters' logs replay identically under v0.11.0 and
$ARBITER_TAG; nothing changes before the tracks activation.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: `OK`. Gate: "push the verifier commit to production main and sync release storage-integrity-si-v2: the three verifiers roll one at a time onto <ARBITER_TAG>; the voters (OnDelete) and both indexers do not change". After a yes, run runbook §3's block. Expected: §3's (verifiers on `ARBITER_DIGEST`, three active verifiers, safe following sealed, `verifier-1..3` with `tracks: true`).

- [ ] **Step 6: indexer-a to `SN_COMMIT` (requires explicit user confirmation)**

In `$P`, one commit. `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml` becomes:

```yaml
# Merged LAST into indexer-a by storage-integrity-si-v2.helmfile.yaml: the
# embedded source SNode must run arbiter-core v0.8.0 or later; this pin runs
# sentio-node <SN_COMMIT>: housegate <HG_TAG> (INDEXER_QUARANTINED as a
# non-retryable 392 and track_frontiers in pkg/replay, on top of v0.18.0's
# SOURCE_UNAVAILABLE refusal, client_seq lanes, contract writer authorization,
# SI ingress denylist and sipeerguard), rewriter-proto/rewriter-go as resolved
# by that build, arbiter-core <CORE_TAG> (track_manifests_v1, track-aware
# manifests, rejected-part cleanup acknowledgements; inert until the tracks
# activation). While the arbiter has quarantined indexer 0 the host reports
# its own Active tables as Refused, and sentio_getStorageIntegrityInfo reports
# quarantined. indexer-b runs the same image. Its startup probe needs
# rewriter-grpc v0.17.0 or later, so it moves together with the in-pod
# rewriter pinned in sentio-node-indexer-a-storage-integrity-si-v2.yaml.
# Rollback target of docs/storage-integrity-devnet2-multi-source-tracks.md
# (only before the tracks activation, §10.1):
# sha-2db130e63b0c662feae1b225341921c404927fb7@sha256:fbb1ef327212fa6ba0c779728dead77ed4506c0955508e2e8e6d0d25d4c8c185.
# A copy of the verified input in docs/examples/storage-integrity-fresh-source-image.yaml
# (sentio-node <SN_COMMIT>, official main CI). Keep the two identical; the
# render tests check it.
node:
  image:
    repository: ghcr.io/sentioxyz/sentio-node
    tag: "sha-<SN_COMMIT>@<SN_DIGEST>"
    pullPolicy: IfNotPresent
```

`docs/examples/storage-integrity-fresh-source-image.yaml` becomes:

```yaml
# Merge LAST into a separately reviewed fresh-network sentio-node release.
# This is not a deployment file for legacy devnet2.
# sentio-node <SN_COMMIT>: housegate <HG_TAG>, rewriter-proto/rewriter-go as resolved
# by that build, arbiter-core <CORE_TAG> (multi-source stage 2: tracks and
# quarantine). Its embedded HouseGate refuses to start against a rewriter older
# than rewriter-grpc v0.17.0.
# Published by official main CI; source revision and the immutable image were
# independently verified. Run validate_source_binding.py against both renders.
node:
  image:
    repository: ghcr.io/sentioxyz/sentio-node
    tag: "sha-<SN_COMMIT>@<SN_DIGEST>"
    pullPolicy: IfNotPresent
```

and `FRESH_SOURCE_IMAGE` in `test_render.py` (`:46-49`) becomes:

```python
FRESH_SOURCE_IMAGE = (
    "ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>"
    "@<SN_DIGEST>"
)
```

```bash
SN_COMMIT=<ledger>; SN_DIGEST=<ledger>; HG_TAG=<ledger>; CORE_TAG=<ledger>
cd "$P" && sed -i '' -e "s/<SN_COMMIT>/$SN_COMMIT/g" -e "s/<SN_DIGEST>/$SN_DIGEST/g" -e "s/<HG_TAG>/$HG_TAG/g" -e "s/<CORE_TAG>/$CORE_TAG/g" \
  k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml docs/examples/storage-integrity-fresh-source-image.yaml charts/storage-integrity/tests/test_render.py
! git -C "$P" grep -nE '<(SN_COMMIT|SN_DIGEST|HG_TAG|CORE_TAG)>' -- k8s-sea docs/examples charts
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -1
git -C "$P" add -A && git -C "$P" commit -m "chore(devnet2): indexer-a sentio-node $SN_COMMIT for multi-source stage 2

Embeds HouseGate $HG_TAG (INDEXER_QUARANTINED as a non-retryable 392)
and arbiter-core $CORE_TAG (track_manifests_v1), and reports quarantined;
inert until the tracks activation. Rollback target 2db130e until then
(docs/storage-integrity-devnet2-multi-source-tracks.md §4.1, §10.1).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: `OK`. Gate: "push the indexer-a image commit and sync release sentio-node-devnet2-indexer-a: its pod restarts onto sentio-node <SN_COMMIT> (indexer-a's tables answer the retryable SOURCE_UNAVAILABLE for about a minute); one acceptance INSERT value 1001 into devnet101.swap_new2". After a yes, run runbook §4.1's block. Expected: §4.1's (`indexer-a runs the stage-2 image`, `"quarantined":false`, `snode-1` with `tracks: true`, safe read `1`). A missing `quarantined` key or `tracks: false` means the pod does not run the new image: stop.

- [ ] **Step 7: indexer-b to `SN_COMMIT` (requires explicit user confirmation)**

In `$P`, one commit. `k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml` becomes:

```yaml
# Merged LAST into indexer-b by storage-integrity-si-v2.helmfile.yaml: the
# multi-source stage-2 build (housegate spec 2026-10-10 §7, §13), sentio-node
# <SN_COMMIT> on housegate <HG_TAG> (INDEXER_QUARANTINED as a non-retryable 392
# and track_frontiers, on top of v0.18.0's SOURCE_UNAVAILABLE refusal, lanes,
# writer authorization, denylist and sipeerguard) and arbiter-core <CORE_TAG>
# (track_manifests_v1, track-aware manifests; inert until the tracks
# activation). While the arbiter has quarantined indexer 1 the host reports its
# own Active tables as Refused. indexer-a runs the same image
# (docs/storage-integrity-devnet2-multi-source-tracks.md §4). test_render.py
# mirrors this pin as INDEXER_B_SOURCE_IMAGE and lists it in
# WRITER_AUTHORIZATION_IMAGES. Its startup probe needs rewriter-grpc v0.17.0 or
# later, pinned in sentio-node-indexer-b-storage-integrity-si-v2.yaml.
# Way back: only before the tracks activation, to
# sha-2db130e63b0c662feae1b225341921c404927fb7@sha256:fbb1ef327212fa6ba0c779728dead77ed4506c0955508e2e8e6d0d25d4c8c185
# (§10.1); afterwards never run an image without track_manifests_v1 on this node.
node:
  image:
    repository: ghcr.io/sentioxyz/sentio-node
    tag: "sha-<SN_COMMIT>@<SN_DIGEST>"
    pullPolicy: IfNotPresent
```

and in `test_render.py` the comment and value of `INDEXER_B_SOURCE_IMAGE` (`:99-106`) become:

```python
# The source image of indexer-b's SI release, the multi-source stage-2 build
# (track_manifests_v1, quarantine reporting). Mirrors
# sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml; indexer-a
# runs the same image (docs/storage-integrity-devnet2-multi-source-tracks.md §4).
INDEXER_B_SOURCE_IMAGE = (
    "ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>"
    "@<SN_DIGEST>"
)
```

```bash
SN_COMMIT=<ledger>; SN_DIGEST=<ledger>; HG_TAG=<ledger>; CORE_TAG=<ledger>
cd "$P" && sed -i '' -e "s/<SN_COMMIT>/$SN_COMMIT/g" -e "s/<SN_DIGEST>/$SN_DIGEST/g" -e "s/<HG_TAG>/$HG_TAG/g" -e "s/<CORE_TAG>/$CORE_TAG/g" \
  k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml charts/storage-integrity/tests/test_render.py
! git -C "$P" grep -nE '<(SN_COMMIT|SN_DIGEST|HG_TAG|CORE_TAG)>' -- k8s-sea charts
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -1
git -C "$P" add -A && git -C "$P" commit -m "chore(devnet2): indexer-b sentio-node $SN_COMMIT for multi-source stage 2

The same image as indexer-a (docs/storage-integrity-devnet2-multi-source-tracks.md
§4.2); inert until the tracks activation, rollback target 2db130e until
then.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: `OK`. Gate: "push the indexer-b image commit and sync release sentio-node-devnet2-indexer-b: its pod restarts onto sentio-node <SN_COMMIT> (indexer-b's ordinary service and its tables pause for about a minute)". After a yes, run runbook §4.2's block. Expected: §4.2's (`indexer-b runs the stage-2 image`, restart count `0`, `"si_indexer_id":1,"self_indexer_id":1,"enrolled":true,"quarantined":false`, `snode-2` with `tracks: true`, `devuser1.t` `ordinary`, two active SNodes).

- [ ] **Step 8: Replace the voters one at a time (requires explicit user confirmation for each deletion)**

```bash
si_leader | tee "$si_backup/leader-before-replacement"
```

For each ordinal in order — the two followers, then the recorded leader — ask "delete voter storage-integrity-si-v2-arbiter-<i> (OnDelete) to move it to <ARBITER_TAG>", and after a yes run `si_replace_voter <i>` (it waits up to 15 minutes; run it in the background and read its output). Expected after each: its capability line with `features` `["client_lanes_v1","signed_claims_v1","track_manifests_v1"]` and three equal `arbiter_sealed_block_seq` lines; a failure prints `sealed tips still differ after 5 minutes; stop` and the procedure stops. After the third: `si_images` shows all six arbiter pods on `ARBITER_DIGEST` and `for i in 0 1 2; do si_capability "$i"; done` shows the three features on every voter. Voter rollouts are frozen from here until Step 11.

- [ ] **Step 9: Re-register the data plane and drain (requires explicit user confirmation for the restarts)**

Gate: "rollout restart the three verifiers, indexer-a and indexer-b so that they re-register with the current leader; each indexer's tables answer the retryable SOURCE_UNAVAILABLE for about a minute while its SNode restarts". After a yes, run runbook §6.1's block. Expected: §6.1's — the three voter ids; exactly `snode-1`, `snode-2`, `verifier-1..3`, each `claims: true` and `tracks: true`; `si_tracks` `tracks_enabled` `false` with no indexers; capability `0`; drained metrics (open block `0`, unanchored `0`, safe = sealed on every voter). Any further node in the book: stop and report it.

- [ ] **Step 10: Tracks activation (requires explicit user confirmation; IRREVERSIBLE POINT)**

Re-run `si_features_tracks`, `si_tracks | jq -c .tracks_enabled` and `for i in 0 1 2; do si_metrics "$i"; done` immediately before asking. Gate, stated in these words: "commit the tracks activation (epoch <e> → <e+1>): tracks_enabled true, max_writers 2 restated, si_indexers, verifiers, the table registry and client lanes carried unchanged; the cutover becomes the sealed tip <tip> and PromotionSeq <p>. This is irreversible: v20 snapshots, every later update carries tracks_enabled, the snapshot-query lane is refused, and no voter, verifier or SNode may run an older image again." After a yes, run runbook §6.2's block; its first lines refuse unless both indexers run the stage-2 image, tracks are off and the capability is 0. Expected: `activation-tip.txt` and an `activate.out` holding the committed answer. On a CLI or leader gate refusal or an `InvalidArgument`: nothing changed; report, fix (restart the named nodes, wait for the drain) and ask again. On "commit outcome may be unknown": `si_show` first.

- [ ] **Step 11: Verify the activation (read-only)** — runbook §6.3.

Expected: §6.3's — epoch one above Step 10's (record `TRACKS_EPOCH`), `tracks_enabled` `true`, `max_writers` `"2"`, lists unchanged; `cutover_block_seq` / `cutover_promotion_seq` equal `activation-tip.txt` (record `TRACKS_CUTOVER`); both indexers' `initial`, `prefix` and `published` at the cutover, generation `0`; both hosts `"quarantined":false` with an unchanged `registry_version`; the four track gauges for indexers 0 and 1 on every voter. The voter-rollout freeze ends here.

- [ ] **Step 12: First track-aware manifests (requires explicit user confirmation for the acceptance INSERT; the user writes in fish)**

Gate: "one acceptance INSERT value 1002 into devnet101.swap_new2 through indexer-a's sidecar". After a yes, run the first block of runbook §6.4. Expected: `track 0 published <cutover> -> <n>`, `1`; track 1 still at the cutover values; `global` = `published` = `sealed`.

Then indexer-b's table. The agent records `si_track 1 | tee "$si_backup/track-1-before-create.json"`, then asks the user to run, in their own fish terminal (housegate `HG_TAG` or any release with the 2026-10-09 agent; the key never pasted into chat):

```fish
set -l key_file /path/to/devuser1-owner.key
begin
    set -lx HOUSEGATE_AGENT_KEY (cat $key_file)
    housegate > ~/si-devnet2-multisource-tracks/agent.log 2>&1 &
end
sleep 5; tail -n 20 ~/si-devnet2-multisource-tracks/agent.log
clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query 'CREATE TABLE s2_e2e (id UInt64, v String) ENGINE = MergeTree ORDER BY id'
```

The agent then runs (in the background; up to 15 minutes):

```bash
for n in $(seq 1 60); do s="$(si_status b devuser1 s2_e2e | jq -r .status)"; echo "$(date -u +%H:%M:%S) $s"; [ "$s" = active ] && break; sleep 15; done
[ "$s" = active ] || { echo "devuser1.s2_e2e did not turn active on indexer-b; stop"; exit 1; }
si_status a devuser1 s2_e2e
si_track 1
si_track_published 1 > "$si_backup/track-1-base"
```

Expected: `pending`, then `active`; indexer-a `ordinary`; track 1's `published` above the cutover (its transition block). Then the user runs:

```fish
printf '1,a\n2,b\n' | clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query 'INSERT INTO s2_e2e FORMAT CSV'
```

and the agent runs `si_wait_track 1 "$(cat "$si_backup/track-1-base")"`, `si_source_ch b 'SELECT count() FROM hg_safe.devuser1__s2_e2e'` and `si_global`. Expected: `track 1 published <x> -> <y>`, `2`, `global` = `published` = `sealed`. The user reads back:

```fish
clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query "SELECT count() FROM s2_e2e SETTINGS SQL_x_read_mode='safe'"
```

Expected: `2`.

- [ ] **Step 13: Snapshot v20 (read-only; repeat later)** — runbook §6.5.

Expected: `written_version 20`, a `key tracks` line, no `apply_error`, `live_check` at `TRACKS_EPOCH`. Once a Raft snapshot exists (repeat later; it needs 8192 log entries): every voter's newest snapshot ends in `20`, `old` prints `restore_refused version 20: …`, `new` prints `restore_ok version 20`.

- [ ] **Step 14: A stuck indexer-b track (the user writes and stops indexer-b in fish; requires explicit user confirmation)** (per amendment A11 (proposed as G7-2))

The agent runs the read-only precondition of runbook §7.1 and records the base:

```bash
si_global
si_status b devuser1 s2_e2e
before="$(si_sealed)"; rows="$(si_source_ch b 'SELECT count() FROM hg_safe.devuser1__s2_e2e')"
[ "$(si_tracks | jq -r .global_safe_block_seq)" = "$before" ] || { echo "not every sealed block is safe yet; wait and retry"; exit 1; }
printf '%s %s\n' "$before" "$rows" > "$si_backup/stuck-before.txt"; cat "$si_backup/stuck-before.txt"
```

Expected: `global`, `published` and `sealed` equal, `active`, and `<tip> 2`. Gate: "the user runs, in one command line, the INSERT of row 3 into devuser1.s2_e2e and, right after it succeeds, scales indexer-b to zero: indexer-b's ordinary service and its SNode stop until Step 16, and its tables answer the retryable SOURCE_UNAVAILABLE meanwhile". After a yes, the user runs:

```fish
printf '3,stuck\n' | clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query 'INSERT INTO s2_e2e FORMAT CSV'; and kubectl --context sentio-sea -n sentio-network-devnet2 scale statefulset/sentio-node-devnet2-indexer-b --replicas=0
```

The agent then runs:

```bash
for n in $(seq 1 60); do p="$(si_kubectl get pod sentio-node-devnet2-indexer-b-0 --ignore-not-found -o name)" && [ -z "$p" ] && break; sleep 5; done
si_b_quiesced
```

and then the read-only check block of runbook §7.1. Expected: `indexer-b is quiesced`, then the global prefix still at `before`, the `hg_safe` rows still `2`, and `indexer-b's track holds block <before + 1>, which cannot become safe while snode-2 is down`. If anything fails after the scale-down, go to Step 16 (ask first). On `MISSED`: run Step 16, then repeat this step once with the row `13,stuck-again` in place of `3,stuck`; on a second miss, record it, run Step 16 and continue with Step 17 (Plan S2-A's `TestTwoTracksStoppingOneSNodeStallsOnlyItsTrack` is then the proof).

- [ ] **Step 15: indexer-a keeps going (requires explicit user confirmation for the acceptance INSERT)**

Gate: "one acceptance INSERT value 1003 into devnet101.swap_new2 while indexer-b is stopped". After a yes, run runbook §7.2's block. Expected: `track 0 published <p> -> <q>` with `q` above `before + 1`, `1`, and `track 0 published past block <before + 1>; track 1 and the global prefix stayed at <before>`; `arbiter_safe_watermark_block_seq` still `before`.

- [ ] **Step 16: Restore indexer-b (requires explicit user confirmation)** (per amendment A12 (proposed as G7-3))

Gate: "scale indexer-b back to one replica: its pod starts on the same image, snode-2 re-registers, and the leader re-sends the promotion it never acknowledged". After a yes, run runbook §7.3's block (in the background; up to 25 minutes). Expected: §7.3's — `indexer-b runs the stage-2 image`; `published` and `global` reach the sealed tip; track 1 passes the stuck block; `hg_safe` rows `3`; `snode-2` with `tracks: true`; `"quarantined":false`.

- [ ] **Step 17: Quarantine indexer-b (requires explicit user confirmation; the user writes in fish)** (per amendment A12 (proposed as G7-3))

Gate: "quarantine indexer 1 with expected generation <g> (read from consensus tracks) and reason 'devnet2 multi-source tracks runbook §8: quarantine exercise': indexer-b's own tables answer refused (reads and writes) and the arbiter refuses indexer-b's statements until the release in Step 18; indexer-a is unaffected". After a yes, run the first block of runbook §8.1, then its read-only block. Expected: §8.1's — generation `g + 1`, `quarantined: true`, the reason; `arbiter_track_quarantined{indexer_id="1"} 1`; `si_info b` `"quarantined":true`, `si_info a` `"quarantined":false`, `registry_version` + 1 on both; indexer-b's answer `refused` / `indexer_quarantined` / `storage-integrity indexer 1 is quarantined`.

Then the user runs (and pastes the two outputs, which carry no secret):

```fish
printf '4,refused\n' | clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query 'INSERT INTO s2_e2e FORMAT CSV' 2>&1 | tail -n 2
clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query 'SELECT count() FROM s2_e2e' 2>&1 | tail -n 2
```

Expected: both `Code: 392`; the write with one of the three quarantine messages of runbook §8.2 (normally `storage_integrity: table devuser1.s2_e2e was refused: indexer_quarantined: storage-integrity indexer 1 is quarantined`), the read with that first one. Any other message, or a success: stop. Then gate "one acceptance INSERT value 1004 into devnet101.swap_new2 while indexer 1 is quarantined", and after a yes run `si_accept_a_track 1004` and `for i in 0 1 2; do si_arbiter "$i" wget -qO- http://127.0.0.1:9090/metrics | grep -E '^arbiter_admission_rejects_total' || true; done`. Expected: `track 0 published …` and `1`.

- [ ] **Step 18: Release indexer-b (requires explicit user confirmation; the user writes in fish)**

Gate: "release indexer 1 with expected generation <g + 1> and no acknowledged rejected blocks (consensus tracks lists none): indexer-b's tables turn Active again and its writes resume". After a yes, run the first block of runbook §8.3; then the agent runs the read-only lines of its second block:

```bash
sleep 10
si_track 1 | tee "$si_backup/track-1-released.json"
for x in a b; do si_info "$x"; done
for n in $(seq 1 24); do s="$(si_status b devuser1 s2_e2e | jq -r .status)"; [ "$s" = active ] && break; sleep 5; done
echo "indexer-b answers $s"
si_track_published 1 > "$si_backup/track-1-base"
```

Expected: generation `g + 2`, `quarantined: false`, empty reason; both hosts `"quarantined":false`, `registry_version` + 1; `indexer-b answers active`. The user runs:

```fish
printf '5,resumed\n' | clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query 'INSERT INTO s2_e2e FORMAT CSV'
```

(on `Code: 733` with `is being activated; retry shortly (retryable)`, wait 35 s and run it once more; any other error: stop). The agent runs `si_wait_track 1 "$(cat "$si_backup/track-1-base")"` and `si_global`, and the user reads back with Step 12's safe read. Expected: `track 1 published …`, `global` = `published` = `sealed`, and the safe read `4` (or `5` after a repeated Step 14).

- [ ] **Step 19: End (the user drops the table in fish; requires explicit user confirmation for the acceptance INSERT)**

The user runs:

```fish
clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query 'DROP TABLE s2_e2e'
```

The agent runs the read-only lines of runbook §9's block (the status poll until `pending`, the two `system.tables` checks), then asks "one acceptance INSERT value 1005 into devnet101.swap_new2", and after a yes runs the rest of the block from `si_accept_a_track 1005` on. Finally the user stops the agent (`jobs`, then `kill (jobs -p)`). Expected: every line of runbook §9's expected paragraph — `gone` then `pending`, no `devuser1__s2_e2e` table on indexer-b's source or any verifier, `1` from the acceptance read, `global` = `published` = `sealed`, each track's `published` equal to its `prefix`, generations `"0"` and `"2"`, no quarantine, `ADMISSION_CODE_SOURCE_UNAVAILABLE` only inside the named windows.

- [ ] **Step 20: Record the outcome (requires explicit user confirmation for the push and the PR)**

In `$P` (after `merge --ff-only origin/main`), one docs commit on a new branch: in the runbook, a paragraph at the end of §9 starting "Record of the first run:" with the dates, `ARBITER_TAG` and `SN_COMMIT`, `TRACKS_EPOCH`, `TRACKS_CUTOVER`, the first track-aware publication (track 0's first frontier, track 1 Initialised), the stuck-track outcome (attempts, the stuck block, track 0's frontier past it, the time to converge after the restore), the quarantine and release generations (`0 → 1 → 2`) and the refusal messages seen, the rows read back, and the admission-reject counts. Open it as a small PR (`docs(storage-integrity): record the devnet2 multi-source tracks rollout`), body ending with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. Keep `$HOME/si-devnet2-multisource-tracks*` (backups, digests, track snapshots) until the next stage starts; `wt remove` the live worktree only when the user asks.

**What rollback means here.** Before Step 10: runbook §10.1 (revert Step 7, then Step 6, then Step 5, then replace the voters back one at a time; the voters wrote only v19 and no command carried a new field). From Step 10 on: no rollback of any voter, verifier or SNode image (§10.2); the activation cannot be undone. Within Steps 14–18: the stop of indexer-b ends with Step 16 (`scale --replicas=1`), the quarantine with Step 18 (the release, generation read from `consensus tracks`).

---

## Appendix B: drafting decisions and cross-task facts

Plan S2-A adopted this plan's contract conflicts as binding amendments (Plan S2-A Appendix A §7): G7-1 as A10, G7-2 as A11, G7-3 as A12, and the CLI facts as A13. The notes below keep the drafting record.

Group G7 drafted Tasks 16–18 against the binding contract (Plan S2-A Appendix A). Where a note here disagrees with the Global Constraints or with Plan S2-A Appendix A, those win until the proposed amendment is accepted.

### Group G7 notes

#### Contract conflicts

- G7-1: Contract §4, "`evaluate`, `case IncarnationActive`, when `me.Quarantined`: … Refused … (schema resolved exactly as for Active; a resolution failure stays Pending with an Issue)", does not say where the readiness check goes — the Active branch resolves the schema and then requires `in.Ready` (`sentio-node:storageintegrityadapter/tablestate/snapshot.go:157-165`), so an unready Active incarnation of a quarantined host could be read as Refused or as Pending. Proposed amendment, appended to contract §4's `evaluate` bullet: "The quarantine check runs after the schema resolution and before the readiness check, so an Active incarnation the host has not made ready is Refused too while its indexer is quarantined (the arbiter refuses every statement of a quarantined indexer; a retryable Pending would only invite retries)." Affected steps: Task 16 Steps 3–4 (`b_unready` in `TestLookupQuarantinedIndexerFromEachHost`), drafted per this amendment.
- G7-2: Contract §5, "per-track isolation with a deliberately stuck indexer-b track", cannot be a deterministic step on devnet2: the INSERT returns once the statement is admitted and its result claim registered, the seal loop seals within `max_age: 2s` (`production:charts/storage-integrity/templates/arbiter.yaml:36`), seal-to-safe was measured at about 4.8 s (`production:docs/storage-integrity-devnet2-dynamic-table-set.md:333`), no arbiter, SNode or chart knob holds one indexer's promotion back, and stopping indexer-b before its write leaves nothing to stall (admission answers `SOURCE_UNAVAILABLE`). Proposed amendment, in contract §5's rollout bullet: replace "per-track isolation with a deliberately stuck indexer-b track" with "per-track isolation with a deliberately stuck indexer-b track (the write and indexer-b's stop in one block, then a check that the write's block is sealed but not safe — global prefix and `hg_safe` row count unchanged — with one repeat after a miss; Plan S2-A's `TestTwoTracksStoppingOneSNodeStallsOnlyItsTrack` remains the proof if both attempts miss)". Affected steps: Task 17 Step 5 (runbook §7.1), Task 18 Step 14 (per amendment A11 (proposed as G7-2)).
- G7-3: Contract §5's order "…; per-track isolation with a deliberately stuck indexer-b track; quarantine and release of indexer-b; restore" leaves indexer-b stopped through the quarantine, but the host checks this plan must run during it — `quarantined` in the info RPC, the table answered Refused, the owner's INSERT refused with 392 — all need indexer-b running: every host helper execs into `sentio-node-devnet2-indexer-b-0` (`production:docs/storage-integrity-devnet2-multi-source.md:29-37`) and the INSERT goes through indexer-b's HouseGate. Proposed amendment: "…; per-track isolation with a deliberately stuck indexer-b track, then indexer-b restored and converged; quarantine and release of indexer-b while it runs; final checks." Affected steps: Task 17 Step 5 (runbook §7.3 to §9), Task 18 Steps 16–19 (per amendment A12 (proposed as G7-3)).

#### Drafting decisions

- **The allowlisted image moves in the PR.** Contract §5 has the new image "appended to `WRITER_AUTHORIZATION_IMAGES` first"; Task 17 appends it in the reviewed PR (test-only, inert), guarded by `test_the_tracks_release_image_is_reviewed_before_either_pin_moves`, so each of Task 18's indexer commits touches only its own pin and mirrors and matches runbook §4's file-list guard, as the stage-1 indexer-a commit did.
- **No grouping-window value, pinned by a test.** Contract §5 says the chart exposes none; `test_devnet2_leaves_the_submission_grouping_window_at_the_binary_default` turns the rule into a named test (green at once, a guard like `test_devnet2_genesis_writer_cap_stays_one`).
- **The table-status RPC is unchanged.** Contract §4 widens only `Snapshot.Schema`; `StorageIntegrityTableStatus` (`sentio-node:storageintegrityadapter/tablestate/status.go:24`) keeps serving `schema_json` only for Active and Gone, and Task 16's test pins it: the agent sees a quarantined table as `refused` with `refused_code` `indexer_quarantined`.
- **"Carries a schema" is `Schema.TableID != ""`**, the test the status method (`status.go:24`) and HouseGate's resolver (`housegate:storage_integrity_runtime.go:241`) already apply.
- **`Registry.Indexer`'s founding branch keeps `Quarantined` false**: a quarantine needs the tracks activation, which needs `si_indexers`.
- **No live consensus update during the quarantine and no live stale-generation attempt.** The drain-gate skip and the generation guards are proven by Plan S2-A's `TestDrainGateSkipsAQuarantinedTrack`, `TestQuarantineAndReleaseOverTheWire` and the three replay tests; on devnet2 an extra update would only consume an epoch, and a stale-generation attempt that a defect let through would quarantine indexer-b again.
- **The rehearsal tools are built by Task 18 Step 3** (runbook §2.1) from the stage-1 sources: `v19check` (Plan S1-A Task 12 Step 7) prints the container's own version byte (`data[4]`) and the restore outcome, so it serves unchanged as `v20check`; the self-test on a live v19 container and §6.5's comparison with `od`'s byte catch a copy that does not. If Plan S2-A's release task builds the stage-2 tools, Step 3 only verifies them.
- **v20 is checked twice:** at once through a replay of a fresh copy (`written_version 20`, `key tracks`), and on the live containers once a Raft snapshot exists (`snapshot_threshold: 8192`, `production:charts/storage-integrity/templates/arbiter.yaml:34`), which may take long on devnet2.
- **The global watermark is never the proof after the activation:** `si_accept_a_track` and `si_wait_track` wait for one track's published frontier (from `consensus tracks`) and fail after 8 minutes; the stage-1 `si_accept_a` (global, silent on a timeout) is used only before the activation (§4.1).
- **Runbook title and stages.** The file name is contract §5's; the title says "tracks and quarantine" and the stages are numbered 0–8 on their own, so neither collides with the stage-1 runbook's "stage 2" (the indexer-a image) nor with the design's stage 2.
- **Three accepted quarantine messages.** A 392 can come from the host's table state (the usual case), from the signed lane's Active check when the agent's 5 s status cache (`housegate:build.go:1538`) still said active, or from the arbiter's `INDEXER_QUARANTINED`; the runbook accepts exactly those three and never the code alone (`sipeerguard` also answers 392).

#### Facts for other tasks

- **Plan S2-A Task 14 (`arbiter-admin`).** Task 18 runs exactly `consensus update --address <leader> --expected-epoch <e> --authority 0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705 --max-writers 2 --tracks-enabled --voter <v0> --voter <v1> --voter <v2> --timeout 20s` — so `--voter` must be accepted together with `--tracks-enabled`, while today's cross-check accepts it only with lanes or `--si-indexer` (`arbiter:cmd/arbiter-admin/main.go:369-411`) — plus `consensus tracks --address <leader>`, `indexer quarantine --address <leader> --indexer 1 --expected-generation <g> --reason <text> --timeout 20s` and `indexer release --address <leader> --indexer 1 --expected-generation <g> --timeout 20s` (`--timeout` as `node evict` has it; if Task 14 drops it, drop it from runbook §8). The runbook parses `consensus tracks` with jq: `.tracks_enabled` (a bool, `false` before the activation thanks to `EmitUnpopulated`), `.cutover_block_seq`, `.cutover_promotion_seq`, `.global_safe_block_seq`, `.published_safe_block_seq`, and per `.indexers[]` `.indexer_id`, `.initial`, `.quarantine_generation`, `.quarantined`, `.quarantine_reason`, `.rejected_blocks`, `.acknowledged_rejected_blocks`, `.prefix_block_seq`, `.published.safe_block_seq`, `.published.applied_promotion_seq`; uint64 values arrive as JSON strings (protojson) and `published` is always set after the activation (the initial entry until the first track-aware manifest). The quarantine and release answers are only recorded; the runbook verifies through `consensus tracks`.
- **Plan S2-A Task 13.** Runbook expectations: every replaced voter's `GetProtocolInfo.features` is `["client_lanes_v1","signed_claims_v1","track_manifests_v1"]`; the gauges `arbiter_track_safe_block_seq`, `arbiter_track_published_block_seq`, `arbiter_track_quarantined` and `arbiter_track_quarantine_generation` with label `indexer_id` appear only after the activation; the leader's refusal reads `tracks gate refused: data-plane nodes <ids> have not registered with this leader advertising track_manifests_v1; restart them on a stage-2 release`, the post-activation registration refusal `tracks are active; this binary does not support track-aware manifests`; an SNode back from a stop receives the promotion it never acknowledged, re-signed, through the existing resend path.
- **Plan S2-A Tasks 6 and 10.** The runbook quotes verbatim the five `tracks activation requires …` reasons (contract §3.1), `consensus update must carry tracks_enabled once set`, and `expected_generation <n> differs from quarantine generation <m> of indexer <id>`; the arbiter's admission message `storage-integrity indexer <id> is quarantined` equals sentio-node's refused reason.
- **Plan S2-A Task 3 (housegate).** The runbook matches `chproto.IndexerQuarantinedMessage("devuser1.s2_e2e")` verbatim (contract §2) next to `sitablestate`'s `storage_integrity: table <id> was refused: <code>: <reason>` (`housegate:pkg/plugins/sitablestate/plugin.go:117`) and the signed lane's `storage_integrity: table <id> is not active; the signed lane admits only active tables` (`housegate:pkg/plugins/storageintegrity/plugin.go:1087`); the post-release 733 is `chproto.TableActivatingMessage`. Task 3's `pkg/sitable` Refused doc must allow a schema (C13): sentio-node's `Snapshot.Schema` now returns one.
- **Plan S2-A Tasks 4–5 (arbiter-core).** sentio-node reads `wire.TableRegistrySnapshot.QuarantinedIndexers` (ascending, empty → nil, from proto field 9) and relies on `LocalNodeFeatures()` carrying `track_manifests_v1` and on the SNode's feature filter keeping it whenever a claim signer is set; no sentio-node config changes.
- **Plan S2-A Task 12 and the rehearsal tool.** On devnet2's log both builds print `written_version 19` before the activation and the new build `written_version 20` after it; the tool prints one `key <member>` line per snapshot-document member, so `snapshotDoc.Tracks` (`json:"tracks,omitempty"`) shows as `key tracks` only after the activation, and `key registration_seqs` is present on both sides since the signed-claims activation.
- **Plan S2-A Task 14 (README)** links the production runbook `docs/storage-integrity-devnet2-multi-source-tracks.md` (contract §5).
- **Task 16 names:** `tablestate.RefusedCodeIndexerQuarantined`, `Registry.QuarantinedIndexers`, `IndexerView.Quarantined`, `rpc.StorageIntegrityInfo.Quarantined` (`json:"quarantined"`, placed after `enrolled`, so `si_info` prints `…"enrolled":true,"quarantined":false,…`), test helper `quarantinedTenantRegistry`.
- **devnet2 values of the runbook:** acceptance values `1001` to `1005`; the test table `devuser1.s2_e2e` with rows `1,a`, `2,b`, `3,stuck` (or `13,stuck-again`), `4,refused` (refused), `5,resumed`; evidence under `$si_backup` (`pins`, `show-*.json`, `tracks-*.json`, `activation-tip.txt`, `activate.out`, `stuck-before.txt`, `quarantine.out`, `release.out`, `digest-*`, `v19-state.bin`, `v20-state.bin`, `agent.log`, `helpers.sh`, `stage2-*-commit`, `track-1-*`).
- **production tests:** 84 test methods at `d9e02a534`, 86 after Task 17; Task 18's pin commits keep 86.

## Appendix C: execution record

Empty until Plan S2-B is executed.
