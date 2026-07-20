# HouseGate Storage Integrity Canonical Publication Artifact Materialization

Date: 2026-07-20

## Purpose

This change adds the pure HouseGate-local rule that a retained mutation-publication worker installs the exact canonical ActiveParts inventory taken from the ledger's majority-claim canonical artifact — never from its own local validation scratch. It ships a `CanonicalArtifact` value type, `BuildCanonicalPublicationSet` which derives the exact per-partition install plan solely from the artifact's canonical parts, and `AssertRetainedWorkersInstallSame` which enforces that every retained worker's readback equals the single canonical inventory (a logical root that matches but a physical part inventory that differs fails closed).

## Companion Gate Status

This PR's canonical-artifact construction and sameness invariant are pure HouseGate-local logic and run green today. The real `REPLACE PARTITION` / signed `DROP PARTITION` execution, the exact-parts readback, and the signed `PublicationAck` round-trip are gated behind the existing `CompanionMutationConsensusAvailable` C2 gate — absent (arbiter/arbiter-proto are INSERT-only). The gated end-to-end publication tests skip closed under `requireCompanionMutationConsensus`.

Two kinds of gated test appear across the mutation PRs, both honest: fake-driven tests (PR10/PR11) that exercise real decision/assembly logic through a test double and pass when the gate is temporarily flipped true; and pure end-to-end placeholder tests (PR08/PR12's publication and safe-cut paths) whose body is an explicit "unreachable until the companion seam lands" failure — they skip when the gate is off and fail loudly if the gate is flipped true prematurely, so a premature flip can never be mistaken for a working end-to-end path.

## Design Anchors

This contract implements the mutation-publication rules of section 4 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`):

- Section 4.4 / 4.8: the canonical publication shadow is separate from the validation scratches; retained workers install the canonical majority artifact, and a non-empty post-state uses `REPLACE PARTITION` while an empty-partition DELETE uses a signed `DROP PARTITION`.
- Section 4.8: the current P2 manifest profile covers a single `ActiveParts` inventory — "logical root same but different part names is NOT supported"; supporting per-worker physical inventory would require versioning the manifest profile.

It changes no Arbiter proto and defines no wire schema. The `CanonicalArtifact` is a value the Arbiter hands HouseGate — its commitment/source are opaque strings HouseGate stores, not recomputes; HouseGate does not select the majority group or compute the 2/3 quorum.

## The Canonical Artifact

`BuildCanonicalPublicationSet(art)` validates the artifact is complete and derives one `PartitionInstallPlan` per affected partition solely from `art.CanonicalParts` — grouped by partition, sorted canonically, `REPLACE PARTITION` when a partition has canonical parts and a signed `DROP PARTITION` when it has none and a zero post commitment. It has no local-scratch parameter, so a worker's scratch cannot influence the plan. It fails closed on a part outside the affected partitions, a drop partition with a non-zero post commitment, duplicate part names, or an incomplete artifact. The parts reuse `replay.PartManifestEntry`, so the HouseGate install plan is the exact shape the global `SafeSnapshotManifest.ActiveParts` covers.

`AssertRetainedWorkersInstallSame(canonical, retainedWorkers, readbacks)` requires every retained worker's readback to equal the single canonical set byte for byte — comparing every content-addressed part field (part name, phys hash, row LtHash, row count, bytes). A worker whose logical root matches but whose physical inventory differs fails closed with a message naming the versioned-profile rule; a missing retained worker or an empty readback map is an error, never a silent pass.

The gated `MutationPublicationDriver` port is what a retained worker's publication executor implements (base-CAS, `REPLACE`/signed `DROP`, durable watermark, exact-parts readback, signed `PublicationAck`); no implementation exists. HouseGate only drives it.

## Non-Scope

This change implements no real `REPLACE`/`DROP PARTITION` execution (gated), no Arbiter majority selection / 2-of-3 quorum / publication-equation / atomic safe cut, and no `PublicationAck` signing. It touches no proto, wires nothing into `build.go` (storage integrity default-off, main unchanged), and does not flip the companion gate. Runtime wiring of the full P2 topology is PR18, gated on C2.

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

Green today: `BuildCanonicalPublicationSet` producing the exact canonical parts from the artifact, being unaffected by any local scratch, yielding a DROP for an empty partition and a REPLACE for a non-empty one, rejecting a part outside the affected set / a drop with a non-zero post commitment / an incomplete artifact / duplicate part names, and being order-deterministic; `AssertRetainedWorkersInstallSame` passing on identical readbacks and failing closed on a different part name, a missing worker, or an empty map; and the defensive clone. Gated: the `PublishRetainedWorker` tests (REPLACE from canonical shadow, empty-partition signed DROP, readback equals canonical input) skip closed under `requireCompanionMutationConsensus`.
