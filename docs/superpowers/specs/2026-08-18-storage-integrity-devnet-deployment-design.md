# Devnet2 Storage-Integrity Deployment (first non-docker end-to-end)

**Date:** 2026-08-18 **Status:** Proposed **Roadmap:** [v1 closure roadmap](2026-08-18-storage-integrity-v1-closure-roadmap.md) Spec F. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §1, §12.1, §12.3; [2026-07-15 P1d EVM anchor](2026-07-15-arbiter-p1d-evm-anchor-design.md); [2026-07-27 DA client](2026-07-27-arbiter-da-client-design.md); Specs A, C, D, G. **Code base:** production `c626552c9` (`k8s-sea/sentio-network-devnet2/`, `k8s-sea/clickhouse/sentio-devnet2-clickhouse.yaml`, `charts/sentio-node/`), arbiter `edd23c3` (`Dockerfile`, `configs/*.yaml`, `cmd/arbiter-anchor`), network-da `8b67059`, sentio-node `9f12620`. **Source of truth:** English version.

## 1. Problem

Nothing of the storage-integrity path is deployed. `k8s-sea/sentio-network-devnet2` runs two `sentio-node` indexers (`indexer-a`, `indexer-b`) + an observer, each with its own **single-replica, zookeeper-less** ClickHouse (`clickhouse-devnet2` CHI, clusters `node-a`/`node-b`), a housegate agent sidecar (`v0.4.0`), a gRPC rewriter sidecar, and no arbiter, no da-store, no `hg_*` tables, no `storage_integrity.*` config. Route A cannot even be switched on there: `hg_unsafe` is `ReplicatedMergeTree` and needs a Keeper; verifiers need their own ClickHouse holding the RMT replicas. Every e2e proof so far lives in docker on CI runners.

## 2. Target topology (devnet2, namespace `sentio-network-devnet2`)

```
                          ┌───────────────── ClickHouse Keeper (3) ─────────────────┐
                          │  reuse ns clickhouse/clickhouse-keeper-extra-{0,1,2}      │
                          └───────────┬─────────────┬─────────────┬─────────────┬───┘
   sentio-node indexer-a  ── CH si-a  │             │             │             │
     (SNode = SOURCE +    (RMT hg_unsafe replica, hg_safe, hg_promote)          │
      safe replica,        │                                                     │
      housegate server SI) │        arbiter-verifier-{1,2,3} ── CH si-v{1,2,3}   │
                           │        (VERIFIER + safe replica; RMT hg_unsafe replicas, hg_safe)
   sentio-node indexer-b (unchanged, non-SI, control)          da-store (1)  arbiter (3, raft)
   AnchorRegistry on devnet2 chain 7892301 (poster = arbiter authority key)
```

- **4 SI ClickHouse instances**: one for the source SNode (`clickhouse-si-a`) and three for the verifiers (`clickhouse-si-v1..3`), all in one CHI `clickhouse-devnet2-si` with a `zookeeper:` block pointing at `clickhouse-keeper-extra-*.clickhouse.svc` (same shape as `k8s-sea/clickhouse/clickhouse-extra.yaml`). Four so that Spec D's safe replicas and the roadmap's "verifier pool ≥ 4" option both fit without redeploying. Storage: same storage class as the existing CHI; `hg_unsafe`/`hg_promote`/`hg_safe` must share **one disk/volume** (§12.2 hardlink promotion) — single default policy, no tiering.
- **`indexer-a` becomes the SI indexer**: its housegate config gains `storage_integrity.{ingress, runtime, tables, read}`; its `sentio-node` config gains `storage_integrity.{enabled, arbiter_peers, payload_store_data_addr, snode.*}`; its upstream ClickHouse becomes `clickhouse-si-a`. `indexer-b` stays as today (control group; proves the base proxy path is unaffected).
- **arbiter**: 3-replica StatefulSet from the arbiter image (all four binaries are in the image; entrypoint `arbiter`), headless service for raft peers, one gRPC Service `arbiter:7080` for data-plane clients (`NotLeader` re-homing handles follower hits), PVC per replica for raft/bolt. Config from `configs/local.yaml` shape with `raft.peers` = the three pod DNS names, `payload_store` set, `anchor.backend: evm` (`rpc_url` = devnet2 RPC already used by the contracts upgrade workflow, `contract_address` from the deploy step, `finality: confirmations` with a small depth for devnet), `authority.private_key_hex` from a Secret, `genesis.{network_id, schema_snapshot_id, executor_profile_id, schema_root}` where `schema_root` is computed from the declared table set at bootstrap (`cmd/arbiter --print-genesis` or the existing `table_ids` tooling — the executing agent picks the existing helper).
- **arbiter-verifier ×3**: Deployment (or StatefulSet, one per SI CH), `schema_source: chain` once the 07-30 spec lands, else `network_state`; `--ensure-tables=create` (Spec C); `safe_replica: true` (Spec D).
- **da-store**: 1 replica, image `ghcr.io/sentioxyz/network-da:devnet`, `fs` backend on a PVC for devnet (GCS later), ports 9001 (data) / 9002 (control), plaintext in-cluster (TLS is a da-store stub — accepted for devnet).
- **AnchorRegistry**: deployed to devnet2 chain 7892301 with `cmd/arbiter-anchor deploy` + `set-poster <authority address>` — deploy the **v2** contract from Spec H (`anchor(bytes32,bytes32,string daRef)`) if H's contract has landed, so devnet2 never carries a v1 anchor history; address recorded in the arbiter ConfigMap and in the production repo (`rollup.json` neighbour, e.g. `storage-integrity.json`).
- **Keeper**: reuse `clickhouse-keeper-extra`; the RMT zk root `/sentio/0/unsafe/...` is namespaced by Spec C's DDL, so sharing the ensemble with other CHIs is safe. Replication-plane forwarding via housegate (§12.1) is **not** deployed on devnet2 (network policy isolates the ns instead) — recorded as a conscious omission.

## 3. Config values (the parts that are policy, not plumbing)

| Component | Key | Value | Why |
|---|---|---|---|
| housegate (indexer-a) | `storage_integrity.read.default_mode` | `unsafe_latest` | dogfooding freshness (Spec G D1) |
| housegate | `storage_integrity.runtime.backpressure.*` | defaults | Spec C |
| housegate | `storage_integrity.tables[]` | the devnet SI tables (start with one, `devnet101.si_smoke`) | explicit membership |
| housegate agent sidecar | `storage_integrity.agent.{enabled, network_id, state_dir}` | on; PVC-backed `state_dir` | Spec A seq durability |
| sentio-node (indexer-a) | `storage_integrity.snode.schema_source` | `network_state` (Phase B) | matches verifiers |
| arbiter | `seal.max_age` | 2s | short blocks for demo latency |
| arbiter | `anchor.evm.finality` | `confirmations: 3` | devnet |
| arbiter | `ingress.max_statement_age` | 5m | as local |
| verifiers | pool | 3 registered, `VerifierSelectN=3` | roadmap decision 7 |

Images: pin **immutable** tags everywhere (`:devnet-<sha>` for sentio-node / network-da / arbiter; the housegate sidecar to the release that contains Spec A). Do not use `:devnet` / `:latest` in these manifests.

## 4. Sequencing

1. Prereqs merged and released: Spec A (envelope v2), Spec C (tables + backpressure), Spec G (read rewrite) at minimum; Spec D before adding the second/third safe replica by rolling restart; Spec E's `INSERT … SELECT` + `DEFAULT now()` guards preferred.
2. `production`: CHI `clickhouse-devnet2-si` (4 replicas, zookeeper block) → wait Ready → verify `system.zookeeper` reachable from each.
3. da-store Deployment + PVC + Service → smoke `GetStoreLimits`.
4. AnchorRegistry (v2 per Spec H when available) deploy on 7892301; record address.
5. arbiter StatefulSet (bootstrap on replica 0, join 1–2) → `SafeState.GetSafeWatermark` answers; leader elected.
6. Declare the smoke table schema on chain (existing `sentio-node declare-table-schemas` / Phase B path) → verifiers start with `--ensure-tables=create` → `hg_unsafe`/`hg_safe` exist on all 4 SI CH nodes with pinned settings.
7. Switch `indexer-a` to `clickhouse-si-a` + SI config; roll.
8. Smoke (a Job in the ns, also runnable from a laptop via port-forward): agent-mode `clickhouse-client` INSERT of 1000 rows into `devnet101.si_smoke` → ACK2 within seconds → `SELECT count()` (unsafe_latest) = 1000 immediately → after finality (~confirmations × block time) `GetSafeWatermark` advances, `SELECT count() SETTINGS SQL_x_read_mode='safe'` = 1000 → `hg_unsafe` part count returns to 0 after cleanup → the three verifier `hg_safe` partition roots equal the source's (Spec D). Fraud drill: run the arbiter chpipeline "swap payload after signing" fixture against devnet2 ingress → `INVALID_SIGNATURE`.
9. Dashboards: `storage_integrity_unsafe_parts`, `..._safe_parts`, `..._backpressure_total`, `..._replica_applied_seq/issued_seq`, arbiter promotion/quorum counters (add basic ones if absent — arbiter has none today; the executing agent adds a Prometheus registry to `cmd/arbiter` and the reference binaries as part of this spec, minimal set: admitted/rejected by code, blocks sealed, quorum verified, promotions issued/acked, anchor lag).

## 5. Acceptance

The smoke in §4.8 passes from a clean namespace using only committed manifests + the documented secrets; `indexer-b` continues to pass its existing checks; a `kubectl rollout restart` of one verifier leaves it `Behind` then `CaughtUp` (Spec D) without operator action; the runbook (`production/docs/storage-integrity-devnet2.md`, new) covers: bootstrap order, adding a safe replica, rotating the authority key (allowlist overlap), what to do when `backpressure_total` rises (promotion lag → check arbiter leader/anchor RPC), and the known devnet omissions (no TLS, no replication-plane forwarding, single da-store).

## 6. Delivery

1. production: CHI + Keeper wiring; da-store; arbiter StatefulSet + config/secret templates; verifier Deployments; `indexer-a` config; smoke Job; runbook.
2. arbiter: minimal Prometheus metrics + `/metrics` in `cmd/arbiter`, `cmd/arbiter-verifier`, `cmd/arbiter-snode` (reuse housegate `pkg/metricshttp`).
3. sentio-node: chart branch for `storage_integrity.*` in `charts/sentio-node/templates/node.yaml` (currently renders none of it) + values.
