# Storage Integrity v1 Closure — Roadmap and Spec Index

**Date:** 2026-08-18 **Status:** Proposed **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) (v3) + [2026-06-30 Sentio Arbiter design](2026-06-30-sentio-arbiter-design.md) + P0b–P1e sub-specs + the 2026-08-18 progress review (arbiter `edd23c3`, arbiter-core `829c44f`, arbiter-proto `v0.4.0`, housegate `c6f7a6d`, sentio-node `9f12620`, rewriter-go `v0.6.0`, rewriter-grpc `1656832`, compute-network-contracts `a834736`, network-da `8b67059`, production `c626552c9`). **Source of truth:** English version.

This document is the index for the work that closes v1 (INSERT end-to-end, deployed) after the 2026-08-18 review. It decomposes the review's findings into independently executable specs, fixes their order, and lists the bounded tasks that need no spec. Every spec below is written to be handed to an implementing agent as-is; the decisions each one takes are stated inline with the alternative that was rejected, so a reviewer can override before execution.

## 1. What the review found (one paragraph)

Route A's INSERT path is implemented and tested end-to-end in arbiter / arbiter-core / housegate (three fraud-rejection classes proved in docker), but (a) nothing of it is deployed, (b) three v1-mandatory pieces of the base design are missing — Phase-2 physical rewrite / read modes, §12.3 admission back-pressure + pinned DDL, §12.5 lag replay / bootstrap — and (c) two protocol shortcuts weaken the base design's core argument: the user JWS binds only the SQL text (not `statement_id`, `payload_hash`, target table, purpose), and the L3 block header does not commit statement content, so an untrusted ingress can swap payloads under a valid signature and the anchored chain hash does not pin what was sequenced. Several documented-vs-implemented mismatches also accumulated (row-id injection point, accumulator construction, physical naming, hash family, challenge semantics, materializer profiles). Two further gaps surfaced while writing these specs: the network has exactly **one** `hg_safe` copy (single-writer, single safe replica — verifiers hold `hg_unsafe` for the byte-side scan but never promote; the promotion stream is live-only, so even that one replica cannot recover missed promotions), and the user-facing **read side of SI tables is unrewritten** (an SI INSERT lands only in `hg_unsafe`/`hg_safe`; the user's `SELECT` still hits the empty multi-tenant table).

## 2. Spec set

| # | Spec | Repos touched | Blocks | Blocked by | Kind |
|---|---|---|---|---|---|
| A | [Signed statement envelope v2 and agent-side payload commitment](2026-08-18-storage-integrity-signed-envelope-v2-design.md) | arbiter-proto, arbiter, arbiter-core, housegate, sentio-node | F (deployment freezes on v2), everything that stores envelopes | — | protocol change, hard cutover |
| B | [Design v4 reconciliation](2026-08-18-storage-integrity-design-v4-reconciliation.md) | housegate docs, designs repo, rewriter docs, contracts docs | — | A (v4 text must describe A's outcome) | docs only |
| C | [Protocol-owned physical tables, pinned settings, admission back-pressure](2026-08-18-storage-integrity-physical-table-lifecycle-design.md) | arbiter-core, housegate, sentio-node | F, D | — (independent of A) | data-plane feature |
| D | [Multi-replica safe set: promotion fan-out, lag replay, cold bootstrap](2026-08-18-storage-integrity-lag-replay-and-bootstrap-design.md) | arbiter-proto, arbiter, arbiter-core, sentio-node | F's second/third safe replica | A (`GetL3Block`, native payloads), C | control+data plane feature |
| E | [Rewriter convergence](2026-08-18-rewriter-convergence-design.md) | rewriter-proto, rewriter-go, rewriter-grpc, housegate | — | — | engine fixes |
| F | [Devnet2 storage-integrity deployment](2026-08-18-storage-integrity-devnet-deployment-design.md) | production, sentio-node config, arbiter configs | first real e2e outside docker | A, C, G (D before >1 safe replica) | ops |
| G | [Storage-integrity read surface rewrite](2026-08-18-storage-integrity-read-surface-rewrite-design.md) (safe / unsafe_latest, `_hg_row_id` hiding, non-lane write rejection) | rewriter-proto, rewriter-go, rewriter-grpc, housegate, sentio-node | F (without it an SI table is write-only for users) | — (shares the proto bump with E) | contract + engine + proxy |

Already-written specs that are ready to execute and are **not** re-specified here:

- [2026-07-30 arbiter chain schema source](2026-07-30-arbiter-chain-schema-source-design.md) + its [plan](../plans/2026-07-30-arbiter-chain-schema-source.md) — `schema_source: chain` in `cmd/arbiter-verifier` / `cmd/arbiter-snode`, delete the inline `tables` mode. Spec C depends on it only loosely (C reads schemas through the same `registry.TableSchemas` seam) — they can run in parallel.
- [2026-07-01 P2 bounded UPDATE/DELETE](2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md) §4 — not started; gated on the row-profile decision in §4 below.

## 3. Execution order

```
   A (envelope v2) ─────────┐
   C (tables/backpressure) ──┼──► F (devnet2 deploy, 1 source + 1 safe replica) ──► first non-docker e2e
   G (read rewrite) ─────────┘                    │
   E (rewriter) ── shares G's proto bump; INSERT…SELECT + DEFAULT now() guards before F's smoke
   D (multi-replica safe set) ── after A + C; before F adds the 2nd/3rd safe replica (verifiers)
   B (docs v4) ── PROGRESS.md / CLAUDE.md fixes now; base-design sections after each spec merges
   07-30 chain schema source ── any time; F prefers it done
```

A, C, G are the critical path to a user-visible devnet e2e (write via A, tables via C, read via G). A first because it is a wire break — every later phase that persists envelopes (FSM snapshots, journals, DA payload refs, EVM anchors on devnet2) would otherwise have to migrate. Nothing is deployed today, so A is a **hard cutover** (proto minor bump, FSM snapshot version bump, no dual-read). D is required before the network has more than one `hg_safe` copy; F is staged so that its first milestone (one safe replica) does not wait on D.

Parallelism: A ‖ C ‖ E+G can be developed concurrently by three agents; D starts once A's proto lands (it needs `GetL3Block` and native payloads) and C's `EnsureProtocolTables` is available on verifiers.

## 4. Decisions this roadmap takes (override before executing)

1. **Canonical signed payload = the Native `ClientData` wire bytes** (`clickhouse-native-data-v1`), not the ingress-derived `csv-with-names-v1`. Rationale and the rejected alternative are in Spec A §3. Consequence: the ingress CSV bridge is deleted; SNode/verifier decode Native.
2. **The agent-mode HouseGate is the signer for the SI lane** and therefore synthesizes the INSERT sample block from the network-state table schema (Phase B) so it can buffer + hash the payload before forwarding the Query. SDK-side signing (an SDK that builds the envelope itself) is compatible but not built here.
3. **`settings_hash` in v1 commits to an empty user-settings set**: the SI lane rejects any client-supplied query setting other than the housegate-owned `SQL_x_*` keys. Replay currently applies no settings; admitting settings would need per-setting determinism review (P2+).
4. **`schema_snapshot_id` stays block-level (genesis param)**; the per-statement envelope carries `schema_hash` (the Phase-B table schema hash the agent encoded against) instead. Base design §7's per-statement `schema_snapshot_id` is deferred to the DDL lane.
5. **Row profile stays `housegate-row-mvp-v0` (name-keyed) for v1 closure.** Upgrading to a `column_id`-keyed `housegate-row-v1` (needed for `RENAME COLUMN` metadata-only, base design §7/§8) is a prerequisite of P2/DDL lane, not of v1, and gets its own spec then. B records this explicitly.
6. **`hg_safe` background merges stay stopped in v1** (no §12.4 ledger-gated merge). C adds a growth metric; controlled compaction is P4.
7. **Verifier pool stays exactly 3, quorum 2** (tolerates 0 malicious verifiers). Raising the pool to ≥4 with deterministic 3-selection is a one-constant + membership change; F sizes devnet2 for 4 verifiers so this can flip later without redeploy.
8. **Challenge remains reject-only in v1** (no independent challenger dispatch). Recorded in B; the `ChallengeReplay → Safe` edge is documented as unreachable in v1.
9. **Read mode is a config policy** (`storage_integrity.read.default_mode`), shipped default `safe` per base design §11, devnet2 set to `unsafe_latest` per the 07-08 product preference. **Needs the product owner's call** before F; the mechanism supports both (Spec G D1).
10. **Manifest publication keeps single-source-ack latency under multi-replica promotion**; other safe replicas converge asynchronously and cleanup waits for Active/Behind replicas only (Spec D D2/D3). Rejected: replica-quorum before publish.
11. **Cold bootstrap = L3 replay through the executor from DA**, not peer part copy (Spec D D5). Depends on DA never releasing AUDIT pins in v1 (already the case).

## 5. Bounded tasks (no spec; do directly)

- housegate: re-enable `bazel test --config=ci //...` in `.github/workflows/ci.yml` (line ~55 is commented). Fix whatever it surfaces before merging.
- network-da: add a `test` job (`make test`) to `release-devnet.yml` and make the image push depend on it.
- sentio-node: run `standalone/storage_integrity_smoke_test.go` in CI behind a docker ClickHouse service (mirror arbiter's `integration-clickhouse` job); keep `SENTIO_SI_E2E=1` as the gate the job sets.
- production: delete or fix `scripts/redeploy-sentio-node-devnet-indexer.sh` (targets the removed `sentio-network-devnet` namespace/dir but runs `helm uninstall` + PVC deletion first). Pin testnet-v2 to an immutable `sentio-node:devnet-<sha>` tag instead of the moving `:devnet`.
- production: move the committed plaintext ClickHouse password / Harbor robot password / sidecar private key out of git (out of scope for SI, in the blast radius of F).
- arbiter-core: replace `verifier/backends.go`'s private `chTableName` with the exported `snode.CHTableName` (07-30 plan step 1, not landed).
- sentio-node: `declare-table-schemas` backfill must not require `storage_integrity.enabled` and must resolve the physical table the same way the live hook does (`DeclarePhysical`), otherwise the same logical table can produce two `schema_hash`es. Tick #175's post-deploy verification.
- housegate CLAUDE.md: add the `pkg/storageintegrity` / `pkg/plugins/storageintegrity` / `storage_integrity.*` section (B lists the content); fix `pkg/replay/AGENTS.md`.

## 6. Out of scope for v1 closure (tracked, not specified)

P2 bounded mutations; P3 SafeAudit / read-set gating; P4 controlled compaction; the DDL / schema-transition lane; multi-Raft sharding; threshold authority signing; independent challenger dispatch; `INSERT ... SELECT` support (v2); per-account fair scheduling in the admission throttle; peer-copy cold bootstrap (D specifies L3-replay bootstrap only).
