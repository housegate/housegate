# HouseGate Storage Integrity P2 Mutation Runtime Wiring And E2E

Date: 2026-07-20

## Purpose

This change adds the P2 mutation runtime shell — a root-package `StorageIntegrityMutation` that holds the four mutation ports (submit, claim, publication ack, safe-cut) plus the publication driver and the PR16 safe-read gate, validates them (nil-port rejection, exactly three distinct workers), and exposes the versioned serving floor — plus a default-off `storage_integrity.mutation` config and a build-time proof that the runtime is not constructed while disabled. It is analogous to PR07's P1e ingress shell.

## Companion Gate Status

This PR is a blocked skeleton. The runtime constructor validation, the publication-input preflight (delegating to the pure `BuildCanonicalPublicationSet`), the config default-off and v1-rejection, and the build-time no-op-when-off proof all run green today. The runtime cannot execute end to end because the C2 mutation-consensus seam is absent: every mutation port has no real implementation, so `RunMutation` fails closed, and enabling the mutation config is a startup error. It reuses the exported `sicore.CompanionMutationConsensusAvailable` gate (via a root-package skip wrapper); the one 3-worker E2E test skips closed.

## Design Anchors

This contract implements the P2 runtime-wiring responsibilities of section 4 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`): three serving workers complete bounded admission, independent replay, a 2/3 claim quorum, canonical publication, per-worker ack, and an atomic safe cut. The 3-worker set and the serving-availability floor (2) are the fixed versioned P2 v1 profile, not runtime-tunable.

It changes no Arbiter proto and constructs no Verifier/Promoter — the runtime holds only ports and the safe-read gate, and drives the Arbiter-owned FSM/quorum/manifest/cut through the ports.

## The Runtime Shell

`NewStorageIntegrityMutation` requires every mutation port and the safe-read gate non-nil and exactly three distinct worker ids (consistent with `MutationServingAvailabilityFloor == 2`), defensively copying the ids. `ServingAvailabilityFloor()` returns the profile constant, proving HouseGate reads the versioned profile rather than inventing a runtime-mutable floor. `validatePublicationInputs` runs the canonical-artifact preflight through the shared `BuildCanonicalPublicationSet` so the runtime cannot silently accept a malformed artifact. `RunMutation` fails closed while `CompanionMutationConsensusAvailable` is false — it never partially orchestrates a flow that would look like driving the Arbiter FSM.

The `storage_integrity.mutation` config defaults off, is server-mode only, and enabling it is rejected in v1 (the companion seam is absent) — both in `Config.Validate` and, defense-in-depth, in `buildServer`. So with the toggle off (the only allowed state) no mutation runtime is constructed and the plugin chain is byte-identical to a non-storage-integrity build.

## Non-Scope

This change implements no real mutation flow, no Arbiter FSM/quorum/manifest/safe-cut, and no injectable mutation ports (they are not wired until C2 lands). It does not add a worker-count or floor config knob (fixed P2 v1 profile), does not redefine any PR08–17 type (all reused via `sicore`), touches no proto, and does not flip the companion gate. The safe-SELECT plugin rewrite remains future work.

## Verification

Focused gate:

```bash
go test . ./pkg/config ./pkg/plugins/storageintegrity ./pkg/storageintegrity -count=1
go test -race ./pkg/storageintegrity . -count=1
```

Bazel gate:

```bash
bazel test //pkg/storageintegrity:storageintegrity_test
```

Green today: the config default-off, the v1-rejection on enable, and the server-mode-only rejection; the constructor holding the ports and floor with a defensive worker-id copy, rejecting each nil port/gate, and requiring exactly three distinct workers; the publication-input preflight passing a well-formed artifact and failing a malformed one via the pure builder; `RunMutation` failing closed while C2 is absent; and the build-time proofs that a mutation-off build succeeds with an unchanged chain and a mutation-on build is rejected. Gated: `TestMutationRuntimeEndToEnd_ThreeWorkerIngressToAck3` skips closed via the exported `sicore.CompanionMutationConsensusAvailable`.
