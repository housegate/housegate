# HouseGate Storage Integrity: PR21 — Repair/Sync and Audited Read-Set Re-Entry

Date: 2026-07-21
Branch: `feat/housegate-sentio-pr19-25-safeaudit-quarantine`
Design: `docs/superpowers/specs/2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md` §5.2 / §5.3

## 1. Scope

A lagging or excluded worker recovers **only** from an authoritative manifest or a canonical peer, and stays **out of the read set** until it has synced from a legal source, exactly verified its readback against the target manifest, **and** re-passed the serving audit (design §5.2: "落后 worker repair/sync 到 latest manifest 并重新通过 serving audit 后才能加入 read set"; §5.3 rollback rule 3). This PR is the pure re-entry decision plus the gated repair driver.

## 2. Pure logic (`repair_sync.go`)

- **`RepairSource`** — the recovery source: `AuthoritativeManifest` or `CanonicalPeer` are the only legal sources; anything else keeps the worker excluded.
- **`RepairStage`** — the progression `Excluded → Syncing → Synced → Verified → AuditPending → AuditPassed → Reentered`. A worker joins the read set only at the terminal `Reentered` stage.
- **`VerifyReadbackAgainstManifest`** — pure exact-parts equality: the worker's readback must exactly equal the target manifest's active parts by the `CandidatePart` fields (table/partition/part name/row-lthash/rows/bytes), order-insensitive, both directions. Any drift fails closed.
- **`DecideReadSetReentry`** — total, fail-closed. It walks the gates in order and returns the stage reached plus eligibility. `EligibleForReadSet` is true **only** when the worker is not quarantined **and** repaired from a legal source **and** its readback exactly matches **and** its serving audit passed. Quarantine dominates the whole progression; a worker that synced, verified, and passed audit is still held out while quarantined. Each failure carries a typed `ReentryDenyReason`.

## 3. Gated driver (`repair_worker.go`)

`RepairWorker` holds two gated ports:

- **`RepairSyncer`** — syncs a worker to a target manifest from an authoritative source and returns its exact readback.
- **`ServingReentryAuditor`** — the gated **C3** SafeAudit port reporting whether the repaired worker re-passed the serving audit.

`Recover` drives sync → verify → audit → the pure `DecideReadSetReentry`, but fails closed while `CompanionMutationConsensusAvailable == false` — no real syncer/auditor exists, so the worker cannot recover end to end. It never implements the Arbiter read-set cut.

## 4. Reused types (never redefined)

`replay.PartManifestEntry`, `CandidatePart`, `AffectedPartition`, `auditPartKey` (PR19), `requireCompanionMutationConsensus`.

## 5. Tests

- **Pure (green today):** `VerifyReadbackAgainstManifest` exact/order-insensitive/every-drift; `DecideReadSetReentry` eligible-only-when-all-gates-pass; the fail-closed matrix (quarantine dominates, illegal source, exact-verify fail, audit-not-passed); quarantine-dominates-even-when-otherwise-ready; stable enum strings; `NewRepairWorker` wiring.
- **Gated (skip-closed):** the real authoritative-sync + SafeAudit re-entry path.
