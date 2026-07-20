# HouseGate Storage Integrity Versioned Mutation Contract Projection

Date: 2026-07-20

## Purpose

This change adds the HouseGate-side pure-Go projection of the versioned P2 mutation contract: the value types that mirror the frozen mutation statement envelope, per-worker task, post-state claim, publication ack, and safe-cut input, plus the ports HouseGate drives to reach the mutation ACKs. It pins a single versioned-contract identity so HouseGate consumes only a version-tagged shape and rejects any ad-hoc or legacy shape fail closed. It also adds the pure equality-key derivation and order-insensitive comparison the Arbiter's 2/3 claim grouping relies on — as a HouseGate helper, never as a HouseGate-side quorum decision.

## Companion Gate Status

This PR is a blocked skeleton. The P2 mutation-consensus seam it projects does not exist in the Sentio companion repos: arbiter/arbiter-proto are INSERT-only (the StatementKind enum is INSERT-only with a comment that DDL/mutation kinds arrive with P2+, the FSM has no mutation lane, and there is no mutation service/message/RPC). So no real `MutationSubmitter` / `MutationClaimSubmitter` / `MutationPublicationAcker` / `MutationSafeCutPublisher` implementation exists.

It introduces a NEW honest gate — `CompanionMutationConsensusAvailable = false` and `requireCompanionMutationConsensus` — deliberately independent of the C1 `CompanionStagedIntakeAvailable` gate: mutation consensus (C2) is a distinct companion capability from staged prepare (C1), and coupling them would let a C1-only landing wrongly un-skip mutation tests.

Because HouseGate must not fabricate the mutation protocol, this PR ships:

1. this scoped spec;
2. the pure HouseGate-core value types, the version-stamp validation, the equality-key derivation/comparison, and the bare port interfaces (no implementations);
3. contract tests. The types, version projection, and equality-key logic run green today; the end-to-end mutation-consensus tests (SubmitMutation → ACK1, 2/3 quorum → ACK2, safe cut → ACK3) are gated by `requireCompanionMutationConsensus` and skip closed while the companion seam is absent.

When the companion mutation seam lands, real adapters implement the four ports, `CompanionMutationConsensusAvailable` flips to true, and the gated tests become the executable spec. No local HTTP or fake gRPC shape is added in the meantime.

## Design Anchors

This contract projects the P2 mutation contract of section 4 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`):

- Section 4.1: the versioned protocol-extension boundary — mutation must be a versioned statement-kind/envelope projection, not a HouseGate-local bypass of the Arbiter.
- Section 4.3: the mutation ACK ladder (ACK1 Sequenced / ACK2 Provisional / ACK3 Safe).
- Section 4.7: the post-state quorum equality key — the exact 8-tuple `{post_state_root, partition_deltas, post_partition_commitments, schema_snapshot_id, executor_profile_id, prev_safe_snapshot_id, base_partition_roots, affected_partitions}`; grouping on `post_state_root` alone is forbidden.
- Section 4.8: the publication ack's ten bound fields and the safe-cut publication equation.

It changes no Arbiter proto, imports nothing from arbiter-proto, and adds no in-repo proto. The version projection is asserted purely by the local `MutationContractVersion` constant and the `Valid()` / `ValidateContractVersion` checks.

## The Versioned Contract

`MutationContractVersion = "sentio-mutation-contract-v1"` is the single versioned identity. Every projected value carries a `ContractVersion` field, and each type's `Valid()` calls `ValidateContractVersion`, which fails closed on a blank or non-pinned version. This is what makes HouseGate consume only the versioned shape: a producer that fails to stamp the exact version is rejected, closing the door on an ad-hoc or legacy HTTP mock shape.

The projected types are HouseGate-core mirrors, not wire messages: `MutationStatementEnvelope` (payload-free — a mutation writes no `hg_unsafe`), `MutationTask`, `MutationClaim` (carrying the full 8-field equality key), `PublicationAck` (all ten section-4.8 bound fields), and `PublishMutationSafeCutInput`. `PublicationAck.ExactActivePartsReadback` reuses the existing `CandidatePart` type, and the mutation kinds reuse `Kind` (only UPDATE/DELETE are accepted; INSERT is rejected). `PartitionDelta` is a dedicated mutation type carrying both add and remove LtHash sums plus rows-updated/deleted, because the single-sum `PartitionLtHashSum` cannot express a mutation delta.

## Equality Key

`DeriveEqualityKey(claim)` projects exactly the 8 equality-key fields, and `EqualityKey.CanonicalString` serializes them with the partition-keyed slices sorted by `(TableID, PartitionID)`, so `EqualityKey.Equal` is order-insensitive. This is the pure comparison the Arbiter's 2/3 grouping relies on — HouseGate exposes it as a helper but does not decide the quorum: the 2/3 grouping, the `RequiredServingSet` / `RetainedServingSet` computation, and the atomic cut are Arbiter FSM decisions consumed through the ports. The tests prove the key cannot collapse on `post_state_root` alone (a differing delta or base root makes keys unequal) and is insensitive to per-worker slice order.

## Non-Scope

This change implements no mutation FSM, quorum, manifest publication, read-set decision, or safe-cut transition — those are Arbiter-side and consumed through the ports. It carries no payload contract on the mutation envelope (mutations write no `hg_unsafe`), reuses no INSERT payload fields, wires nothing into `build.go` or the ingress plugin (storage integrity stays default-off, main behavior unchanged), touches no proto, and does not flip any companion gate. Partition-bounded admission is PR09; local replay and the signed claim are PR10; the worker rebind logic is PR11; the canonical publication artifact is PR12.

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

Green today: the pinned-version guard; envelope/claim/ack `Valid()` accepting a fully-stamped value and rejecting a blank/wrong version, an INSERT kind, and each missing bound field; the empty-DELETE ack accept; the equality-key derivation projecting exactly 8 fields; `Equal` true for identical claims, order-insensitive across permuted slices, and false on a differing post-state-root / delta / base-root; the `CandidatePart` readback round-trip; and the serving-floor profile constant. Gated: the four end-to-end mutation-consensus tests (`SubmitMutation` → ACK1, 2/3 quorum → ACK2, `PublishMutationSafeCut` → ACK3, and the full ingress→ACK3 walkthrough) skip closed under `requireCompanionMutationConsensus`, naming the missing SubmitMutation / SubmitMutationClaim / PublishMutationSafeCut seam. The suite is race-clean.
