# HouseGate Storage Integrity Empty Partition Signed DROP And Durable Ack

Date: 2026-07-20

## Purpose

This change adds the empty-partition-DELETE publication contract as pure HouseGate-local value types and validation: a `SignedDropAction` (the Arbiter-signed internal DROP-PARTITION command binding a base snapshot/root, the partition, a zero post commitment, a monotonic publication seq, and a signature), a fail-closed validator, a versioned digest + signature verifier, an empty-readback ack builder and verifier, the pure base-CAS decision, and a gated driver that returns a durable ack idempotently or executes the signed drop.

## Companion Gate Status

This PR is a blocked skeleton. The drop-action construction/validation, the digest and signature round-trip, the empty-readback ack build/verify, and the base-CAS decision are pure HouseGate-local logic and run green today. The only companion-gated surface is executing the real internal DROP PARTITION and persisting the durable ack, which needs the C2 mutation-consensus seam (absent) — including the Arbiter/leader that issues the authoritative signature. It reuses the existing `CompanionMutationConsensusAvailable` gate and PR14's `PublicationAckStore`; the real-execution tests skip closed.

## Design Anchors

This contract implements the empty-partition-DELETE path of section 4.8 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`): an empty partition after DELETE has no post partition to REPLACE, so an Arbiter-signed internal DROP PARTITION action is used, binding the base snapshot/root, the partition id, a zero post commitment, and a monotonic publication seq; the worker recomputes its local current root and sets `Applied=false` on a base-CAS mismatch; the new manifest records `active_parts=[]` and a zero LtHash partition root.

It changes no Arbiter proto and does not modify `PublicationAck` — the ack's existing `Valid()` already accepts the zero-post empty-DELETE shape. HouseGate never issues the authoritative signature (the real signer is the absent Arbiter/leader seam); it only verifies.

## The Signed Drop Action

`BuildSignedDropAction` stamps the contract version, builds the zero post commitment and the DROP-action install plan, and computes the versioned action digest, leaving the signature blank for the Arbiter/leader to sign. `Valid()` fails closed unless the action is a signed, zero-post, DROP-plan (no canonical parts) command with a bound base and a digest that recomputes — a DROP may execute only when signed. `ComputeDropActionDigest` (domain `mutation-empty-drop-action-v1`, order-insensitive over the base roots) is what the signature covers; `VerifyDropActionSignature` recomputes and ed25519-verifies it, failing closed on tamper or wrong key.

`BuildEmptyDropAck` emits the empty-DELETE `PublicationAck` — empty readback, empty post commitments, blank post state root — with `Applied` reflecting the base-CAS outcome (an `Applied=false` ack on a base-CAS mismatch is still a valid empty ack). `AssertEmptyDropAck` verifies a supposedly-empty drop ack is genuinely empty and bound to the action; a non-empty readback on a DROP is corrupt and fails closed. `DecideDropApplied` is the pure base-CAS decision (every bound base root must match the worker's recomputed current root exactly). `DriveEmptyDrop` returns a durable ack for a repeated `(mutation_id, worker_id, publication_seq)` without re-executing (via PR14's `PublicationAckStore`), else calls the gated `EmptyDropExecutor`.

## Non-Scope

This change implements no real DROP PARTITION execution (gated `EmptyDropExecutor`, no implementation), no Arbiter signature issuance, no safe-cut commit (PR16), and no Arbiter FSM/quorum. It does not redefine `PublicationAck`/`PublicationAckStore`/`PublicationAction` (all reused), does not modify `mutation_contract.go`/`mutation_artifact.go`, touches no proto, wires nothing into `build.go` (storage integrity default-off, main unchanged), and does not flip the companion gate.

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

Green today: `BuildSignedDropAction` stamping a zero-post DROP-plan; `Valid()` accepting a signed action and rejecting each of the failure classes (missing signature, non-zero post commitment, non-DROP plan, plan with parts, zero seq, missing base, blank version, tampered digest); the order-insensitive digest and its change on a seq change; the signature round trip with tamper/wrong-key rejection; the empty-readback ack build (Applied true/false), `AssertEmptyDropAck` rejecting a non-empty/mismatched ack, the base-CAS decision, and the base-root mapper. Gated: `DriveEmptyDrop` real-execution and idempotent-durable-ack tests skip closed under `requireCompanionMutationConsensus`.
