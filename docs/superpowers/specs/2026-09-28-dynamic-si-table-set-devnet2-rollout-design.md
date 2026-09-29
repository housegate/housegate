# Dynamic SI Table Set — Sub-project 5b: devnet2 Upgrade Rollout

**Status:** approved design (2026-09-28). **Scope:** devnet2 (`sentio-network-devnet2`, release `storage-integrity-si-v2` and `sentio-node-devnet2-indexer-a`); the production repo (charts, values, runbook); an arbiter release tag. No product code changes. **Umbrella:** `2026-09-23-dynamic-si-table-set-design.md`. **Predecessors:** sub-project 4 (`2026-09-25-dynamic-si-table-set-data-plane-design.md` §12, plan handoffs) and 5a (`2026-09-26-dynamic-si-table-set-host-gates-design.md`, implemented in sentio-node `184bbf6`, PR sentioxyz/sentio-node#188).

## 1. Goal and end state

Bring every devnet2 component that the dynamic table set needs onto its released build, with behaviour unchanged, and prepare — but do not perform — registry enablement.

At the end of 5b:

- arbiter voters and verifiers run a tagged arbiter release built from `cf38430` or later (arbiter `10ab917` + arbiter-core v0.10.1);
- indexer-a runs sentio-node `184bbf6`, the in-pod rewriter `housegate-rewriter:0.15.0` (Gate R4) and the housegate sidecar `v0.15.0` with the storage-RPC table-status source;
- consensus administration is enabled on every voter; no consensus update has been committed;
- the production charts can render a `table_registry` block with per-voter `rpc_url`, disabled;
- devnet2 behaves exactly like today's static table set: the genesis table `devnet101.swap_new2` is signed and promoted, every other table is Ordinary.

Registry enablement, auditors and the watcher form **stage 3**, documented in the runbook but out of 5b's scope, gated on the table-reference hardening and the I1 follow-up (§6).

## 2. Measured facts

Sources: the production repo `origin/main`, arbiter `origin/main`, sentio-node `origin/main` `184bbf6`, housegate specs and plans, and the arbiter upgrade spike (local 3-voter cluster, 2026-09-26). No live devnet2 infrastructure was queried.

- **Current pins.** arbiter voters and verifiers share `ghcr.io/sentioxyz/arbiter@sha256:0a83dded…f32c` (v0.7.1), `consensusUpdatesEnabled: false`, anchor backend `evm` on `http://sentio-node-op-reth:8545` (chain 7892301, 3 confirmations), raft `snapshot_threshold 8192` / `trailing_logs 10240` hardcoded in the chart, arbiter StatefulSet `OnDelete`, verifier StatefulSet RollingUpdate under `pinned-genesis-v1`, one shared image digest (`charts/storage-integrity/_helpers.tpl:19-21`). indexer-a: sentio-node `sha-1494fb1…@sha256:ce55024c…` (housegate v0.14.1, arbiter-core v0.9.0), sidecar `v0.14.1@sha256:c1e5b9aa…` with a static YAML `networkStateSource`, in-pod rewriter `0.14.0@sha256:c04ecad6…` (gRPC contract V1). Genesis: one table `devnet101.swap_new2`, `schemaSource: network_state`, `snode.nodeID snode-1`.
- **Target images.** sentio-node `ghcr.io/sentioxyz/sentio-node:sha-184bbf655c1c5dda33db75142865c344ebfe3df7@sha256:5ba1a69bb7df240288fdaff8cab838a7e854cadfb57ffc9162169945fe8a865f`; rewriter `housegate-rewriter:0.15.0@sha256:57812c8cf40603dab0fa84f8f0c1c6270990c2a74820d1450e20d65c618e4989`; sidecar `ghcr.io/housegate/housegate:v0.15.0` (digest verified with `docker buildx imagetools inspect` at execution); arbiter: the release tag cut in stage 0 (CI image `sha-cf38430@sha256:115e81b3…` is the same tree).
- **Arbiter upgrade (spike).** main restores v0.7.1's v4 snapshots and replays its log to byte-identical state; with registry, consensus updates and default-off lanes off, main proposes no new command types; main always writes v14 snapshots, which v0.7.1 cannot read. Rollback past the first main-leader election is only a whole-cluster restore of pre-upgrade backups. Old-protocol data-plane clients work against main; arbiter-core 0.10.x clients against v0.7.1 are compatible by code reading. SafeState reads are leader-only on main.
- **Rewriter.** Contract-V2 engines answer contract-V1 requests unchanged, so the V2 rewriter can run under the old sentio-node; the new housegate refuses a V1 rewriter at startup (Gate R4).
- **Chart guards.** `charts/sentio-node/templates/node.yaml:5-6,35-36` require housegate `tables` to equal `snode.tableIDs`; `:70-71,88-96` require sidecar `tableSchemas`; housegate `enabled` is never rendered; `validate_source_binding.py:101-103,140-152` enforce the same. sentio-node `184bbf6` accepts a legacy `tables` list equal to `table_ids` with a deprecation warning. `charts/storage-integrity/templates/arbiter.yaml` cannot render a `table_registry:` block.
- **Registry RPC.** `http://sentio-node-op-reth:8545` load-balances three execution nodes (`-0`/`-1` archive geth, `-2` reth); arbiter's README forbids a load balancer for `table_registry.rpc_url`, which must answer historical `eth_call`.
- **Why the registry waits for the watcher.** The seed of pre-activation tables and every `AddTable` are proposed by the watcher. With the registry enabled and the watcher off, the registry never seeds; after L2 passes `activation_block`, every table created in an indexer-a user database is unrecorded and answers Pending (default deny) until the watcher registers it — a new-table outage. `activation_block` is set once. Auditors only compare ranges the watcher committed.

## 3. Decisions

- **R1 — 5b ends at "upgraded, registry disabled".** Registry enablement, auditors and the watcher happen together in stage 3, after the table-reference hardening (rewriter-go v0.14.0, rewriter-grpc v0.16.0, housegate release) is live and the sentio-node I1 follow-up is deployed.
- **R2 — Arbiter: stop-all cutover.** All voters stop, every data dir is backed up, all start on the new release together; verifiers roll in the same sync (shared digest, compatible both ways). Chosen over a rolling upgrade for procedural simplicity; the spike shows no safety difference — rollback is the backups either way.
- **R3 — Tag an arbiter release** with its `cut-release` workflow (expected `v0.8.0`) from `cf38430` or later and pin tag + digest.
- **R4 — Registry RPC per voter.** Each voter's `table_registry.rpc_url` points at a single-instance Service of an archive node, spread across `op-reth-0` and `op-reth-1`, so auditors check independently. Rendered now, used from stage 3.
- **R5 — Voters before the data plane.** Stage 1 (arbiter) precedes stage 2 (indexer-a): it follows the data-plane spec's order and uses the measured old-SNode-against-main combination.
- **R6 — Gate R4 with the sentio-node sync.** Rewriter 0.15.0, sentio-node `184bbf6` and sidecar v0.15.0 move in one indexer-a sync.
- **R7 — Sidecar switches to the storage-RPC status source in stage 2**, while the registry is disabled and the answers equal the YAML's, so stage 3 needs no sidecar change.
- **R8 — Charts through one PR, values and images pushed directly.** Shared `charts/` changes go through a production PR (the user's rule); per-stage value flips and image bumps are single values-file edits pushed to `main`.
- **R9 — New runbook.** `docs/storage-integrity-devnet2-dynamic-table-set.md` in the production repo; the conflicting sections of `docs/storage-integrity-devnet2-si-v2.md` (consensus administration stays disabled, v0.7.1 statements, rollback) are marked superseded with a link.
- **R10 — Human-gated operations.** Stages 1, 2 and 2b act on devnet2 and are irreversible or disruptive; they are executed or explicitly confirmed by the user step by step, never advanced automatically.

## 4. Stages

Every stage has preconditions, steps with exact commands, verification, and a stop/rollback line in the runbook. A stage starts only after the previous one verified.

### 4.1 Stage 0 — preparation (no behaviour change)

1. **Arbiter release.** Run arbiter `cut-release.yml` on `main` (≥ `cf38430`). Confirm the tag's tree carries arbiter-core v0.10.1 and that the release published an image; record tag and digest.
2. **Production chart PR.**
   - `charts/sentio-node`: drop the `tables == tableIDs` guards (`node.yaml:5-6,35-36`); stop rendering `housegate.storage_integrity.tables` and render `enabled: true` for SI nodes; relax the sidecar `tableSchemas` guards when `networkStateSource` is an `http(s)://` URL; update `validate_source_binding.py` and `test_render.py`; keep the si-v1 overlay rendering (rollback surface).
   - `charts/storage-integrity`: an optional `table_registry:` block in the arbiter config template (keys `rpc_url`, `rpc_timeout`, `poll_interval`, `max_log_range`, `cursor_advance_interval`, `deploy_block`, `watcher.enabled`, `audit.enabled`; `watcher`/`audit` default false), included in `arbiterConfigChecksum`; `rpc_url` settable per voter ordinal; digest allow/deny lists in `validate.yaml` updated.
   - L2: single-instance Services for `sentio-node-op-reth-0` and `-1`.
   - Acceptance: the devnet2 overlays render byte-identically before and after, except the new disabled `table_registry` block.
3. **Runbook** (R9) and **pre-stage measurement**: devnet2 raft snapshot sizes and restore/replay time per voter, which size the maintenance window.

### 4.2 Stage 1 — arbiter stop-all cutover (maintenance window)

1. Pause indexer-a SI intake (method in the runbook: stop the writers, relying on HouseGate's retryable refusals).
2. Record the baseline: safe watermark, latest safe block, leader and term.
3. Stop every voter (scale the StatefulSet to 0 or delete pods), confirm all are down, and back up each voter's `raft.data_dir` (`raft.db`, `snapshots/`, `consensus-protocol.json`).
4. Push the values change pinning the stage-0 arbiter release digest; start every voter; verifiers roll with the sync.
5. Verify: every voter restores its v4 snapshot and replays; `consensus-protocol.json` unchanged; a leader is elected and the cluster commits; after intake resumes the safe watermark advances past the baseline; no SNode or verifier errors; every voter's image digest checked out of band (the capability probe reports protocol version 1 for both builds).
6. Rollback: stop all, restore every backup, pin v0.7.1, start all — losing every commit since the cutover. Valid only before intake resumes; after that, treat any problem as an incident, not a rollback.

### 4.3 Stage 2 — indexer-a sync

1. One values push: sentio-node `sha-184bbf6…@sha256:5ba1a69b…` (and its mirrors `docs/examples/storage-integrity-fresh-source-image.yaml`, `test_render.py` `FRESH_SOURCE_IMAGE`), rewriter `0.15.0@sha256:57812c8c…`, sidecar `v0.15.0@<verified digest>`, `networkStateSource: http://localhost:32003` (the storage RPC).
2. Verify:
   - startup logs the registry as disabled (arbiter answers `FailedPrecondition "table registry is disabled"`) and `sentio_node_storage_integrity_registry_enabled` is 0;
   - the storage RPC is reachable from the sidecar and `sentio_getStorageIntegrityTableStatus` answers `active` for `devnet101.swap_new2` and `ordinary` for others;
   - a signed INSERT into `devnet101.swap_new2` is accepted and promoted to safe;
   - processor indexing continues;
   - a freshly created test table shows a non-zero `CreatedBlock` in the mirror.
3. Rollback: restore the previous images and `networkStateSource`. Valid because no genesis table can be dropped before stage 3.

### 4.4 Stage 2b — enable consensus administration

Push `consensusUpdatesEnabled: true`; replace each voter pod (OnDelete); `arbiter-admin consensus capability --address <voter>:7080` reports `updates_enabled: true` on every voter; record `consensus show` (epoch, authority `0x9Ef3…d705`, `max_writers`). Commit no consensus update.

### 4.5 Stage 3 — registry, auditors, watcher (runbook only; out of 5b)

Preconditions: the table-reference hardening is released and live on devnet2 (its own rollout starts in `tableref_guard.mode: observe`); the sentio-node I1 follow-up (by-hash schema for every genesis id under `network_state`) is deployed on indexer-a.

Steps: look up the Databases proxy address (`AddressBook` key `databases` on chain 7892301) and its deploy block; choose `deploy_block` (≤ `activation_block − 1`; the proxy deploy block, or 0); choose `activation_block` (after the stage-2 block, and at least ~1 h of L2 blocks in the future when the update commits); run `arbiter-admin consensus update … --table-registry-chain-id 7892301 --table-registry-contract <proxy> --table-registry-si-indexer-id 0 --table-registry-activation-block <N> --table-registry-confirmation safe` with the complete `--authority`/`--max-writers` set; enable `audit` on every node, then `watcher` on every voter; confirm `registry_enabled == 1` on indexer-a before L2 reaches `activation_block`; confirm the seed; run the end-to-end check (CREATE → retryable `pending activation` → Active → signed INSERT → promotion → DROP → retire → purge → same-name CREATE → Active).

Before stage 3, confirm indexer-a's on-chain indexer id is 0 and that no unrelated mirrored database reports indexer id 0 (`si_indexer_id` 0 is a zero value).

### 4.6 Operating rules (runbook)

- Do not recreate a genesis table's name before its purge completes (lifted once I1 ships).
- An evicted source SNode rejoins only with a wiped state directory.
- Do not declare schemas for processor tables (declaring brings them into SI).
- Never roll indexer-a back past `184bbf6` once a genesis table has been dropped.
- Keep verifiers on inline schemas.

## 5. Testing

- Chart: `test_render.py` and `validate_source_binding.py` cover old/new overlay render equivalence, the disabled `table_registry` block and per-voter `rpc_url` rendering.
- Arbiter release: a local stop-all rehearsal outside devnet2 — three v0.7.1 voters with traffic, all stopped, all started on the release, state digests compared (the spike's method).
- Stage verification lists are executable commands in the runbook.

## 6. Risks

- **Irreversible arbiter cutover.** Once intake resumes, the only way back is a whole-cluster backup restore that loses commits. Mitigation: intake paused, baseline recorded, verification complete before intake resumes.
- **Unmeasured snapshot size / restore time** sizes the window — measured in stage 0.
- **Sidecar depends on the local storage RPC.** If it is unreachable the sidecar sends unsigned INSERTs that the server refuses; stage 2 verifies reachability first.
- **I1 (5a follow-up).** A genesis id still Retiring or Purging loads its schema from the contract's latest version; a refused or unrecorded same-name recreation then breaks restart. Cannot trigger while the watcher is off (no retirement); fixed before stage 3.
- **Hardening gap.** Until the table-reference hardening is live, the in-pod gRPC engine accepts `CREATE MATERIALIZED VIEW … TO <Active table>`; with only the genesis table Active and the watcher off, the exposure equals today's.

## 7. Deliverables

- arbiter release tag (no code change);
- production PR: `charts/sentio-node`, `charts/storage-integrity`, L2 single-instance Services, render tests, runbook, superseded markers in the si-v2 runbook;
- direct values pushes per stage (images, `networkStateSource`, `consensusUpdatesEnabled`);
- housegate: this spec; the 5a spec and plan (PR housegate/housegate#213) merged alongside.
