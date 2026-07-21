# HouseGate Storage Integrity: PR22 — Controlled Compaction Publication

Date: 2026-07-21
Branch: `feat/housegate-sentio-pr19-25-safeaudit-quarantine`
Design: `docs/superpowers/specs/2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md` §6

## 1. Scope

v1 requires `STOP MERGES` on both `hg_safe` and `hg_unsafe`; stock ClickHouse background merges have no Arbiter pre-commit gate. Any later safe merge must go through **controlled compaction** (design §6): the Arbiter selects the same partition's input safe parts from the current manifest, the worker builds output parts in an `hg_compact` shadow, the LtHash ledger equation is checked, the Arbiter signs a partition-level publication, the worker publishes via signed base-CAS `REPLACE PARTITION`, and a new content-addressed manifest is published. A native un-ledgered merge is an immediate active-set mismatch → stop serving + repair/quarantine.

## 2. Pure logic (`compactor.go`)

- **`CompactionPlan`** — the Arbiter-selected input: `{CompactionID, PublicationSeq, TableID, PartitionID, BaseSafeSnapshotID, BasePartitionRoots, InputParts}`. `Valid()` fails closed on blanks, a zero seq, an empty base binding, an empty input set, or any input part outside the compaction partition.
- **`CompactionOutput`** — the `hg_compact` output parts for the same partition. `Valid()` rejects an empty or cross-partition output.
- **`VerifyCompactionEquation`** — the LtHash ledger equation (design §6 step 3): `sum(part_row_lthash(input)) == sum(part_row_lthash(output))`. It folds each side's hex-encoded `PartRowLtHash` (the raw 2048-byte accumulator) into an `lthash` accumulator and compares. Controlled compaction may only re-lay-out rows; adding or dropping content breaks the equation and is rejected. The fold is commutative, so the check is order-insensitive.
- **`BuildCompactionReplacePlan`** — validates the equation + output, then builds the signed base-CAS `REPLACE PARTITION` via the shared `BuildReplacePartitionPlan` (design §6 step 4): compaction publishes through the same REPLACE path as a mutation, keyed by `(compaction id, publication seq)`.
- **`BuildCompactionManifestID`** — the new content-addressed manifest id over the canonicalized output mapping (design §6 step 5), independently recomputable.
- **`DetectActiveSetMismatch` / `DecideCompactionQuarantine`** — a native un-ledgered merge diverges the worker's observed active-part names from the manifest-declared set; the mismatch (order-insensitive set comparison) forces a fail-closed stop-serving + exclude-from-read-set + repair-required decision (design §6 final paragraph).

## 3. Gated driver (`compaction_worker.go`)

`CompactionWorker` holds two gated **C4** ports:

- **`ControlledCompactionDriver`** — builds the shadow output parts.
- **`CompactionPublicationDriver`** — publishes via signed base-CAS REPLACE, returning the per-worker ack.

`RunCompaction` drives execute → equation → publish → new-manifest-id, but fails closed while `CompanionMutationConsensusAvailable == false` — the companion controlled-compaction (C4) seam is absent from arbiter/arbiter-proto. The worker never selects input parts (the Arbiter does) nor commits the manifest.

## 4. Config

`storage_integrity.safe_merges` gains `enabled` + `mode`. It defaults off; enabling it is **rejected in v1** (server-mode-only + "not runnable in v1: companion controlled-compaction (C4) seam absent"), validated independently of ingress and mirroring the mutation toggle. `mode` accepts only `controlled_compaction`. `allow_native_background_merges` remains the existing fail-closed escape hatch.

## 5. Reused types (never redefined)

`replay.PartManifestEntry`, `replay.PartitionCommitment`, `replay.CanonicalDigest`, `lthash` accumulator, `BuildReplacePartitionPlan` + `PartitionInstallPlan` + `PublicationActionReplacePartition` (PR14), `PublicationAck`, `requireCompanionMutationConsensus`.

## 6. Tests

- **Pure (green today):** `VerifyCompactionEquation` preserves-content / order-insensitive / drop-fails / add-fails / empty-fails (using real `lthash` accumulators); `BuildCompactionReplacePlan` valid + equation-violation-blocks + cross-partition-rejected; `BuildCompactionManifestID` deterministic + content-addressed; `DetectActiveSetMismatch` + `DecideCompactionQuarantine`; `CompactionPlan`/`CompactionOutput` validity; `NewCompactionWorker` wiring; config v1-rejection + server-mode + unsupported-mode.
- **Gated (skip-closed):** the real shadow build + signed REPLACE publication.
