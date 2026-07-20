# HouseGate Storage Integrity MutationWorker Pending/Stale/Rebind

Date: 2026-07-20

## Purpose

This change adds the MutationWorker pending/stale/rebind decision layer: pure functions that, from the snapshot re-queried before and after execution plus a pending-insert signal, decide whether to proceed, block on a pending INSERT, rebind before execute, supersede after execute, or hold a locally-applied worker unserviceable — so the worker never reuses an old scratch or submits a claim computed against a superseded base. The `MutationWorker` orchestration wires the gated scratch executor strictly between the two snapshot re-queries and drops any claim that a base advance superseded.

## Companion Gate Status

This PR's decision logic is pure HouseGate-local state-machine code and runs green today. The only companion-gated surface is the worker's actual execution path — the scratch clone/execute and claim production — which needs the C2 mutation-consensus seam (SubmitMutation / MutationTask dispatch / SubmitMutationClaim) that is absent (arbiter/arbiter-proto are INSERT-only). It reuses the existing `CompanionMutationConsensusAvailable` gate and `requireCompanionMutationConsensus` helper; the `MutationWorker.Run` end-to-end tests skip closed behind it.

The `SnapshotQuerier` used by the decision tests is a read-only local safe-snapshot reader (a real adapter would project a `replay.SafeSnapshotManifest` into a `SnapshotView`), not the mutation-consensus seam — so the pure decision tests it feeds run green without the gate.

## Design Anchors

This contract implements the state-machine rules of section 4.5 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`):

- A same-partition earlier INSERT not yet Safe/Rejected keeps the mutation in the pending write queue — it must not execute on a snapshot that cannot see that INSERT.
- When the snapshot advances before dispatch/execution, the old scratch and claims are superseded and the task must re-clone/re-execute (rebind before execute; supersede after execute).
- A worker that already locally applied may NOT be treated as un-executed and directly rebound; it stays unserviceable until a publication cut accepts its readback or a repair.

It changes no Arbiter proto and defines no wire schema; the real mutation task/claim contract is owned by the companion C2 profile. HouseGate implements none of the Arbiter FSM/quorum/barrier logic — it only decides proceed/block/supersede/rebind and drives ports.

## The Decision Layer

`StaleAgainst(task, view)` reports staleness when the snapshot id advanced, an affected partition's base root changed, or an affected partition is missing from the current view (which cannot be validated, so it is treated as stale — fail closed). `PendingInsertBlocks(task, view)` reports a block when an unresolved same-partition INSERT is present on an affected partition. `DecideRebind(task, before, after, locallyApplied)` composes them: a pending insert in the before-view blocks; a stale before-view rebinds (never running the executor); a stale after-view supersedes if the worker had not applied, or holds it unserviceable (`DecisionLocallyAppliedNoRebind`) if it had; otherwise proceed. These reuse the PR08/PR10 `MutationTask` and `PartitionCommitment` types.

## The Worker

`MutationWorker.Run` queries the snapshot before execution, returns a block or rebind result without touching the executor when the before-view blocks or is stale, otherwise clones+executes via the gated `WorkerScratchExecutor`, re-queries the snapshot after, and applies `DecideRebind`. Only a `DecisionProceed` result carries the signed claim; a blocked/rebind/superseded/locally-applied result carries none — the worker never submits a claim computed against a superseded base.

## Non-Scope

This change implements no real scratch execution (gated behind the port), no Arbiter FSM / 2-of-3 quorum / barrier install / manifest publication, and no claim-submission wire. It touches no proto, defines no wire schema, wires nothing into `build.go` (storage integrity default-off, main unchanged), and does not flip the companion gate. The canonical publication artifact is PR12.

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

Green today: `StaleAgainst` on a snapshot-id advance / changed base root / missing partition / identical base; `PendingInsertBlocks` on an unresolved / resolved / different-partition INSERT; and `DecideRebind` for the five verdicts (block, rebind-before, supersede-after, locally-applied-no-rebind, proceed). Gated: the `MutationWorker.Run` tests (proceed returns a claim and queries before+after; a pending insert blocks without calling the executor; a superseded after-view returns no claim) skip closed under `requireCompanionMutationConsensus`; the underlying decision logic is already covered green.
