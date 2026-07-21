# HouseGate Storage Integrity Atomic Safe-Cut View And Safe SELECT Gating

Date: 2026-07-20

## Purpose

This change adds the pure HouseGate consumer of the Arbiter's committed atomic safe cut: a `SafeCutView` value projecting the one atomic transition (manifest, global watermark, per-worker watermarks, read-set membership, route-cache epoch); a `SafeReadGate` that answers "may worker W serve at snapshot S?" against that committed cut and only that cut; and `VerifyPublicationEquation`, the pure predicate mirror of the design's publication equation. Together they forbid serving from a local apply or a single-worker ack — a worker may serve only if the committed read-set says so.

## Companion Gate Status

This PR is a blocked skeleton. The cut view, the read gate, and the equation verifier are pure HouseGate-local logic and run green today. The only companion-gated surface is consuming a genuine Arbiter-published cut end-to-end, which needs the C2 mutation-consensus seam (absent). It reuses the existing `CompanionMutationConsensusAvailable` gate; the end-to-end tests skip closed. The plugin safe-SELECT rewrite integration is deferred to the PR18 runtime wiring — this PR touches no plugin or `build.go`.

## Design Anchors

This contract implements the atomic safe cut of section 4.8 and the read-set gating of section 5.2 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`): `PublishMutationSafeCut` commits the manifest, global/per-worker watermarks, read-set membership, and route-cache epoch in one atomic transition; a safe SELECT may route only to a worker in the committed read-set whose local watermark covers the requested snapshot and that is not quarantined, and never to a worker on the strength of a local apply or a single-worker ack. The publication equation (`RetainedServingSet ⊆ AppliedEquivalentSet`, `RequiredServingSet = RetainedServingSet ⊎ ExcludedBeforeCut`, `size(Retained) ≥ floor`, all retained readbacks == canonical) gates the cut.

It changes no Arbiter proto; HouseGate consults these predicates but does not commit the cut, decide the quorum, publish the manifest, or advance watermarks — those are Arbiter FSM work.

## The Safe-Cut View And Read Gate

`SafeCutView` defensively copies its maps, and `Valid()` fails closed on a blank manifest id, a zero global watermark or route-cache epoch, a nil read-set, or a read-set worker without a per-worker watermark. `SafeReadGate.MayServe(worker, snapshot)` returns yes only when the worker is in the committed read-set, is not quarantined, and both its per-worker watermark and the global watermark cover the requested snapshot; every no carries a typed reason. Because the gate consults only the committed cut, a worker that locally applied but is absent from the read-set — a single-worker ack not yet committed — is denied.

`VerifyPublicationEquation(input)` fails closed unless the retained set is a subset of the applied-equivalent set, the retained and excluded sets are disjoint and their union set-equals the required set, the retained set meets the serving-availability floor (defaulting to `MutationServingAvailabilityFloor`), and every retained worker's readback digest equals the canonical digest. It takes a dedicated `PublicationEquationInput` (adding the applied-equivalent set and readback digests) rather than mutating the existing driver input.

## Non-Scope

This change commits no cut, decides no quorum, publishes no manifest, and advances no watermark — those are Arbiter FSM (deferred to the companion seam). It does not wire the safe-SELECT rewrite into the plugin chain (PR18), touches no proto, does not modify or redefine any PR08–15 type (reuses `MutationServingAvailabilityFloor`, the companion gate, etc.), wires nothing into `build.go` (storage integrity default-off, main unchanged), and does not flip the companion gate.

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

Green today: `SafeCutView.Valid` (accept + each reject), `NewSafeCutView`/`Clone` defensive copies; `SafeReadGate.MayServe` allowing an in-read-set covering-watermark worker and denying not-in-read-set, quarantined, worker-watermark-behind, global-watermark-behind, and a single-worker-ack worker absent from the committed cut; `VerifyPublicationEquation` accepting a valid equation, defaulting the floor to the profile constant, and rejecting floor-unmet, non-subset, overlap, coverage-mismatch, missing-readback, and readback-mismatch; and the stable deny-reason strings. Gated: consuming a real Arbiter-published cut and the end-to-end serve-only-after-committed-cut tests skip closed under `requireCompanionMutationConsensus`.
