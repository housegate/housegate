# HouseGate Storage Integrity Partition-Bounded Mutation Admission

Date: 2026-07-20

## Purpose

This change adds the HouseGate-local UPDATE/DELETE bounded-admission classifier: a pure function that runs the full design section 4.2 support/reject matrix over the rewriter-produced statement type and accessed tables, the signed materialized SQL shape, a caller-supplied provably-affected partition set, and a latest-manifest cost snapshot, and either accepts a bounded `MutationPlan` (with canonical-order barrier keys and estimated touched parts/bytes) or returns a typed `MutationRejection`. Only mutations whose affected partitions are provable and whose manifest cost is within limits are admitted.

## Companion Gate Status

This PR's classifier is pure HouseGate-local logic and runs green today. The only companion-gated surface is submitting a classified plan to the Arbiter's mutation-consensus FSM (SubmitMutation, partition-barrier install, 2-of-3 post-state quorum), which is C2 — absent (arbiter/arbiter-proto are INSERT-only, mutation is P2+ future). The plugin package declares its own `CompanionMutationConsensusAvailable = false` and `requireCompanionMutationConsensus` (distinct from the INSERT C1 gate and from the core package's gate, so the plugin-side submission test is gated without importing the core package), and the single end-to-end submission test skips closed behind it. This PR touches no proto and never fabricates the mutation-consensus wire shape.

## Design Anchors

This contract implements the support/reject matrix and barrier granularity of section 4 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`):

- Section 4.2 support/reject matrix: the supported `ALTER TABLE ... UPDATE/DELETE` and normalizable `UPDATE/DELETE` forms, and every v1 reject — unbounded predicate, affected-partitions / touched-parts / touched-bytes over limit, protocol-column or `_hg_row_id` or key-column modification, lightweight DELETE, TRUNCATE / DROP PARTITION / direct `hg_safe` modification, unstable (remote / join / subquery / table-function) expressions, unmaterialized nondeterministic functions after signing, and schema-snapshot vs worker-schema-root mismatch. The bounded unit is the partition, and cost is estimated from the latest manifest's active parts/bytes, not from a data-skipping index.
- Section 4.5 barrier granularity: barriers are `(table_id, partition_id)`; multi-partition mutations acquire all barriers in canonical order.

It changes no Arbiter proto and adds no RPC. HouseGate does not parse SQL — the caller supplies the rewriter's statement type / accessed tables and the resolved affected-partition set, exactly as the design says admission estimates cost against a provided latest-manifest snapshot.

## The Classifier

`ClassifyMutation(cfg, req)` is pure. It rejects a non-mutation kind or a statement-type/kind mismatch (`RejectUnsupportedKind`), a TRUNCATE / DROP PARTITION or direct `hg_safe` modification, a lightweight DELETE, an unstable access (remote table or more than the single target — join/subquery/table-function), an unmaterialized nondeterministic function, an assigned protocol column / `_hg_row_id` / key column, a schema-root mismatch, an empty (unprovable) affected-partition set, and any bounded-cost overrun. On acceptance it returns a `MutationPlan` whose `BarrierKeys` are one per affected partition, sorted canonically so the Arbiter acquires all barriers at once without deadlock, plus the estimated touched parts/bytes.

`EstimateMutationCost(snapshot, affectedPartitions)` sums the active parts and bytes over exactly the affected partitions from the latest manifest, and returns the affected partitions absent from the snapshot so the classifier can fail closed (`RejectManifestPartitionMissing`). Cost is whole-partition active inventory — never a data-skipping-index or per-granule estimate.

The `MutationSubmitter` port (returning the existing `SubmitOutcome`) is the gated seam that submits an accepted plan to the Arbiter; no implementation exists. HouseGate only drives the FSM through it and never installs barriers, decides the 2/3 quorum, or publishes a manifest itself — the emitted `BarrierKeys` are a canonical-order plan hint the Arbiter acquires, not barriers HouseGate holds.

## Reuse

The classifier reuses the plugin package's existing `Kind`, the `sqlmeta.StatementType` / `sqlmeta.AccessedTable` the rewriter already produces for INSERT admission, and the existing `containsUnmaterializedNondeterminism` nondeterministic-function scan, so mutation admission and INSERT admission agree on statement classification and nondeterminism detection. The reserved-column literals `_hg_row_id` / `_hg_*` match the native-payload reserved-column guard. The gated `MutationSubmitter` returns the core `SubmitOutcome` so later consensus wiring reuses the same outcome trichotomy.

## Non-Scope

This change implements no partition-barrier install, 2/3 quorum, manifest publication, read-set decision, or compaction decision — those are Arbiter-side and only driven via the gated port. It does not resolve the partition predicate from raw SQL (the caller supplies the provable affected-partition set), does not derive cost from a data-skipping index, does not wire anything into `build.go` or the ingress plugin (storage integrity stays default-off, main unchanged), and touches no proto. Local replay and the signed claim are PR10; the worker rebind logic is PR11; the canonical publication artifact is PR12.

## Verification

Focused gate:

```bash
go test ./pkg/storageintegrity ./pkg/plugins/storageintegrity -count=1
```

Bazel gate:

```bash
bazel test //pkg/plugins/storageintegrity:storageintegrity_test
```

Green today: the accept cases (ALTER UPDATE, ALTER DELETE, normalizable UPDATE, multi-partition with canonical barrier order and summed cost); `EstimateMutationCost` summing only affected partitions and reporting missing ones; the reject matrix with one subtest per design 4.2 bullet (18 rejects); the rejection error-string; and the meta-test asserting the reject-reason set is complete and distinct. Gated: `TestSubmitMutation_SequencesPlan` skips closed under `requireCompanionMutationConsensus`.
