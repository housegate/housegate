# HouseGate Storage Integrity Mutation Rollback And Partial-Apply Recovery

Date: 2026-07-20

## Purpose

This change adds the pure HouseGate rollback classifier: it maps a worker's mutation evidence (command issued? apply status known/applied? durable ack? manifest committed?) to a four-state `RollbackState` and a `RollbackDecision` that routes a partial local apply to read-set exclusion and repair — never a silent rebind — and permits treating a worker as un-executed only when it provably did not apply.

## Companion Gate Status

This PR is a blocked skeleton. The classifier and decision are pure HouseGate-local logic and run green today. The only companion-gated surface is driving the real read-set exclude + repair, which needs the C2 mutation-consensus seam (absent). It reuses the existing `CompanionMutationConsensusAvailable` gate; the repair-driver test skips closed. Evidence bits like `DurableAckPresent` / `ManifestCommitted` are truthfully populated only once the companion seam lands; green-today tests construct evidence directly (testing the pure classifier).

## Design Anchors

This contract implements the rollback / partial-apply-recovery rules of sections 4.5 and 5.3 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`):

- Not applied (no command, or command issued but provably not applied) → cancel/rebind/retry (the only state where treating the worker as un-executed is legal).
- Command issued, local apply unknown → query the durable ack and hold; authority/watermark/CAS still gate a stale action; never rebind while unknown (section 4.5 PublicationBlocked "unknown ack").
- Partial local apply (applied but manifest not committed) → exclude from the read set and repair; a worker that applied may never be treated as un-executed (section 4.5 line and PublicationBlocked "partial apply") — the rollback-time expression of PR11's `DecisionLocallyAppliedNoRebind`.
- Manifest committed → done; the only undo is a reversing statement through the normal mutation path (HouseGate reports done and drives no reverse statement here).

It changes no Arbiter proto. HouseGate drives the repair through a gated port and never implements the Arbiter read-set cut.

## The Classifier

`ClassifyRollback` is total and fail-closed toward the most-committed consistent state (manifest-committed dominates, then partial local apply, then command-issued-unknown, else not-applied), so a worker that touched storage is never under-classified. `DecideRollback` maps the state to an action and enforces the invariant matrix: `MayRebindAsUnexecuted` is true only for `RollbackNotApplied`; a partial local apply always sets `ExcludeFromReadSet` and `RepairRequired` and never `MayRebindAsUnexecuted`. `RepairScope` is a defensive copy. The gated `MutationRepairDriver` is the port HouseGate drives to exclude + repair.

## Non-Scope

This change drives no real exclude/repair (gated), drives no reversing statement for a committed manifest, and implements no Arbiter read-set cut / FSM. It does not redefine `PublicationAck`, `AffectedPartition`, `MutationDecision`, or the companion gate (all reused), wires nothing into `build.go` (storage integrity default-off, main unchanged), touches no proto, and does not flip the companion gate.

## Verification

Focused gate:

```bash
go test ./pkg/storageintegrity -count=1
go test -race ./pkg/storageintegrity -count=1
```

Bazel gate:

```bash
bazel test //pkg/storageintegrity:storageintegrity_test
```

Green today: `ClassifyRollback` over every state (no command, command-not-applied, command-unknown, command-durable-ack-unknown, partial apply, committed, and manifest-dominates-partial); `DecideRollback` routing not-applied→rebind, command-issued→query/hold, partial-apply→exclude+repair-never-rebind (the headline invariant), committed→done; the defensive `RepairScope` copy; the may-rebind-only-for-not-applied matrix; and the stable enum strings. Gated: the real exclude+repair driver test skips closed under `requireCompanionMutationConsensus`.
