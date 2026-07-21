# HouseGate Storage Integrity Mutation Publication Authority And Base-CAS Preflight

Date: 2026-07-20

## Purpose

This change adds the pure HouseGate-local preflight a retained mutation-publication worker runs before emitting any REPLACE / DROP PARTITION SQL: it verifies the Arbiter-issued signed publication command's authority (a valid ed25519 signature), a monotonic publication sequence, a non-empty required serving set, the artifact commitment matching the canonical artifact, and a per-partition base-CAS against the current safe roots. Any failure is fail-closed with a typed reason.

## Companion Gate Status

This PR is a blocked skeleton. The publication preflight and command verification are pure HouseGate-local logic and run green today. The only companion-gated surface is driving a genuine Arbiter-issued signed publication command end-to-end, which needs the C2 mutation-consensus seam (`RecordMutationPublicationIssued` / the signed publication command) that is absent (arbiter/arbiter-proto are INSERT-only). It reuses the existing `CompanionMutationConsensusAvailable` gate and `requireCompanionMutationConsensus`; the one end-to-end test skips closed behind it.

## Design Anchors

This contract implements the publication authority + base-CAS gate of section 4.8 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`): the Arbiter's `RecordMutationPublicationIssued` persists the publication sequence, required serving set, majority equality key, canonical artifact commitment/source, and base partition roots; a retained worker must verify the signed command, the sequence, the serving set, the artifact commitment, and each per-partition base root before any publication SQL. HouseGate does not recompute the majority, the quorum, or the FSM transition.

It changes no Arbiter proto and defines no wire schema — the real command is Arbiter-issued; HouseGate only mirrors its authority-bearing fields as a pure value type.

## The Preflight

`PublicationCommand` mirrors the command's authority fields (`MutationID`, `PublicationSeq`, `RequiredServingSet`, `ArtifactCommitment`, `BasePartitionRoots`), stamped with the pinned contract version and validated by `Valid()`. `SignedPublicationCommand` wraps it with a versioned canonical hash and a detached ed25519 signature, signed and verified via the existing `MutationClaimSigner` port and `replay.CanonicalDigest` (versioned domain `mutation-publication-command-v1`); the hash is order-insensitive across the serving set and base roots.

`PreflightPublication(signedCommand, artifact, current, pubKey)` is the pure gate. In order, it fails closed on: an invalid command; a bad signature; a non-monotonic publication sequence (strictly-greater-than-current required); an empty required serving set; an artifact commitment that does not match the canonical artifact for the same mutation and sequence; a base root missing from the current safe state; or a base-CAS mismatch (a current safe root that differs from the command's bound base root). It reads a caller-supplied `CanonicalArtifact` and `PreflightCurrentState` (the local committed sequence and per-partition safe roots) — it never invents the Arbiter command, the majority artifact, or the ack.

## Non-Scope

This change implements no Arbiter FSM, quorum, majority selection, manifest publication, or safe-cut transition; no real REPLACE/DROP execution; and no ack persistence (PR14/PR15). It touches no proto, wires nothing into `build.go` (storage integrity default-off, main unchanged), and does not flip the companion gate.

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

Green today: `PublicationCommand.Valid` (accept + each missing field); the order-insensitive command hash; sign/verify round trip with tampered-command and wrong-key rejection; `PreflightPublication` accepting a valid command and rejecting each of the seven failure classes (invalid command, bad signature, non-monotonic seq, empty serving set, artifact commitment mismatch incl. wrong-mutation artifact, missing base root, base-CAS mismatch). Gated: `TestPreflightPublication_DrivesRealArbiterCommand` skips closed under `requireCompanionMutationConsensus`.
