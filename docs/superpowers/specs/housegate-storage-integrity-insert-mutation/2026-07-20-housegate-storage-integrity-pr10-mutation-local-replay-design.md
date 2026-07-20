# HouseGate Storage Integrity Manifest-Bound Scratch Local Replay And Signed Claim

Date: 2026-07-20

## Purpose

This change adds the HouseGate-side MutationWorker local-replay and signed-claim layer: a `MutationExecutor` that drives a gated scratch-executor port to clone the frozen manifest base into a per-worker scratch, execute the mutation, and read back the post-state; and a pure claim-assembly + equality-key-digest + ed25519-signing layer that binds a complete per-worker `MutationClaim` and signs it. It reuses the PR08 mutation contract types (`MutationTask`, `MutationClaim`, `PartitionCommitment`, `PartitionDelta`, `EqualityKey`, `DeriveEqualityKey`) rather than redefining them.

## Companion Gate Status

This PR is a blocked skeleton. The claim assembly, equality-key digest, claim hash, and signing/verification are pure HouseGate-local logic and run green today. The only companion-gated surface is the real ClickHouse clone/execute/readback (`ATTACH PARTITION FROM hg_safe`, the mutation, the `system.mutations` wait, the post-commitment recompute), which needs a live ClickHouse and the versioned P2 executor profile — the C2 seam that is absent (arbiter/arbiter-proto are INSERT-only). It reuses the PR08 `CompanionMutationConsensusAvailable` gate and `requireCompanionMutationConsensus` helper; the executor tests skip closed behind it.

Because HouseGate must not fabricate the mutation protocol, this PR ships the pure claim layer, the bare `MutationScratchExecutor` port (no production implementation), and contract tests: the pure functions run green; the executor-drives-real-scratch tests are gated.

## Design Anchors

This contract implements the MutationWorker responsibilities of section 4 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`):

- Section 4.6 (MutationWorker steps 1-7): clone the frozen base into scratch, verify the scratch initial commitment equals the manifest base root, execute, wait for completion, recompute post commitments/deltas, and sign a claim.
- Section 4.7 (post-state quorum equality key): the claim binds the full 8-tuple, and the equality-key digest is the pure grouping key.

It changes no Arbiter proto and defines no wire schema — the real mutation task/claim wire contract is owned by the companion C2 profile. HouseGate only produces a per-worker signed claim; it never groups claims or decides a quorum.

## The Claim Layer

`AssembleMutationClaim(task, res, workerID)` binds a scratch replay result into a complete `MutationClaim` and fails closed if the kind is not a mutation, any equality-key field is blank/empty, or an affected partition lacks a post commitment — no tolerated wildcard. `MutationEqualityKeyDigest` is the versioned canonical digest of the section-4.7 8-tuple only (order-insensitive across the partition-keyed slices, insensitive to non-key fields like worker/mutation id), so two logically equal claims from physically distinct workers share a digest — modelling the 2/3 grouping key without making the grouping decision. `MutationClaimHash` digests the full claim (including the non-key fields) so the ed25519 signature attests everything. `Ed25519ClaimSigner` wraps the existing `payloadexec.Ed25519Signer`, and `VerifyMutationClaimSignature` recomputes both digests from the claim and verifies the signature, failing closed on any mismatch. Both digests use versioned domain strings so a future profile can never silently collide.

## The Executor

`MutationExecutor.Execute` drives the gated `MutationScratchExecutor` port, then `verifyScratchBaseRoots` checks the scratch's recomputed initial base roots match the task's frozen manifest base roots exactly (present, equal, same set) — the section-4.6-step-4 fail-closed guard — before `AssembleMutationClaim` + `SignAssembledClaim` produce the signed claim. It returns an error and no claim on any coordination or verification failure; it never fabricates a claim. It holds no ClickHouse or Arbiter state and produces only per-worker evidence.

## Non-Scope

This change implements no real ClickHouse scratch clone/execute/readback (gated behind the port), no claim grouping / 2-of-3 quorum / manifest publication (Arbiter FSM work), and no new post-state-root or LtHash formula. It touches no proto, defines no wire schema, wires nothing into `build.go` (storage integrity default-off, main unchanged), and does not flip the companion gate. The worker pending/stale/rebind logic is PR11; the canonical publication artifact is PR12.

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

Green today: `AssembleMutationClaim` binding all equality-key fields, rejecting an INSERT kind, and rejecting each incomplete result; `verifyScratchBaseRoots` passing on a match and failing closed on a differing/missing/extra root; the equality-key digest being deterministic, order-insensitive, ignoring non-key fields, and matching across physically distinct workers; the claim hash covering all fields; the ed25519 sign/verify round trip and its tampered-claim / wrong-key rejections; and `SignAssembledClaim` populating the digest, hash, worker id, and signature. Gated: the `MutationExecutor.Execute` tests (clones frozen base and produces a claim; scratch base-root mismatch fails closed) skip closed under `requireCompanionMutationConsensus`; the pure fail-closed base-root logic is already covered green.
