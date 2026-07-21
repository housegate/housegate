# HouseGate Storage Integrity Non-Empty Mutation REPLACE And Durable Ack

Date: 2026-07-20

## Purpose

This change adds two pure HouseGate-local seams for publishing a non-empty mutation post-state: `BuildReplacePartitionPlan`, which turns a canonical REPLACE-action install plan into the exact `ALTER TABLE hg_safe.<t> REPLACE PARTITION ... FROM hg_mutation_publish.<shadow>` instruction; and `PublicationAckStore`, a durable per-worker ack store keyed by `(mutation_id, worker_id, publication_seq)` whose first `Put` verifies the ack's exact-active-parts readback equals the canonical inventory and whose repeated `Put` is idempotent (returns the stored ack, never re-executes).

## Companion Gate Status

This PR is a blocked skeleton. The REPLACE plan build and the ack store (including the readback-equals-canonical verification and idempotency) are pure HouseGate-local logic and run green today. The only companion-gated surface is actually executing the REPLACE PARTITION against ClickHouse and driving the real per-worker ack, which needs the C2 mutation-consensus seam (absent). It reuses the existing `CompanionMutationConsensusAvailable` gate; the real-execution tests skip closed behind it.

## Design Anchors

This contract implements the non-empty publication path of section 4.8 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`): a retained worker executes `REPLACE PARTITION ID '<partition>' FROM hg_mutation_publish.<mutation_id>__<publication_seq>`; it durably saves `{publication_seq, ack, local watermark}` before sending the ack, and a duplicate task keyed by `(mutation_id, worker_id, publication_seq)` returns the same ack without re-executing; all retained readbacks must equal the canonical manifest input.

It changes no Arbiter proto. The REPLACE builder emits SQL only — actually running it is the gated `MutationPublicationDriver.PublishRetainedWorker` path.

## The REPLACE Plan Builder

`BuildReplacePartitionPlan(mutationID, publicationSeq, plan)` fails closed on a blank mutation id, a zero publication seq, a non-REPLACE action (DROP and Unspecified are out of scope — the signed-DROP path is PR15), an empty canonical part set, or a blank table/partition. It emits the design's verbatim REPLACE SQL against `hg_safe`, replacing from the `hg_mutation_publish.<mutation_id>__<publication_seq>` shadow, and carries the canonical parts defensively copied and sorted (the input plan slice is never mutated).

## The Durable Ack Store

`PublicationAckStore` persists a `PublicationAck` keyed by `(mutation_id, worker_id, publication_seq)`. The first `Put` runs `PublicationAck.Valid()` then `verifyReadbackEqualsCanonical`, which checks the ack's `ExactActivePartsReadback` (a `[]CandidatePart`) equals the canonical set's parts — comparing on the fields `CandidatePart` carries (table, partition, part name, row LtHash, rows, bytes), order-insensitive, and binding the readback to the same mutation and publication seq — then durably stores a defensive copy. A repeated `Put` for the same key returns the already-stored ack unchanged, without re-verifying or overwriting, so a worker that saved its ack before sending it recovers by returning the same ack. `MemPublicationAckStore` is the green-today in-memory implementation.

## Non-Scope

This change implements no real REPLACE PARTITION execution (gated), no signed DROP PARTITION (PR15), no safe-cut commit (PR16), and no Arbiter FSM/quorum. It touches no proto, wires nothing into `build.go` (storage integrity default-off, main unchanged), and does not flip the companion gate.

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

Green today: `BuildReplacePartitionPlan` emitting the canonical REPLACE SQL, rejecting a non-REPLACE / empty / blank-identity plan, and not mutating its input; `MemPublicationAckStore` put-then-get, idempotent duplicate-key Put (returns the first ack even when the second call passes a divergent body), order-insensitive readback-equals-canonical, and fail-closed on a readback mismatch (extra/missing part, row-LtHash/bytes mismatch, wrong seq/mutation), a blank key, or an invalid ack. Gated: the real REPLACE execution and the real-driver durable-persist tests skip closed under `requireCompanionMutationConsensus`.
