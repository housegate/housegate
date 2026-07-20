# HouseGate Storage Integrity RC Late Binding And ACK2 Dual Gate

Date: 2026-07-20

## Purpose

This change formalizes the single ACK2 dual gate that decides whether a storage-integrity intake may report success to the client. Building on the PR03 staged-prepare orchestration, it factors the ACK2 decision out of the orchestrator's inline branch logic into one pure, exhaustively tested predicate — `Ack2Ready` — that requires all five design-section-3.4 conditions to hold together. The orchestrator's `registerAndFinish` now grants ACK2 in exactly one place, by consulting that gate, so a client can never be told a statement succeeded from a partially satisfied state (for example a completed unsafe write with no bound result claim).

The gate is fail-closed by construction: every one of the five conditions is necessary, a blank sub-field is a mismatch rather than a tolerated wildcard, and the RC's bound source must be present and equal to the source that prepared the local write.

## Companion Gate Status

This PR is scoped as a blocked skeleton, on the same basis as PR03. The companion staged-prepare seam that HouseGate ingress must drive — `PrepareLocalStatement` / `RegisterPreparedClaim` / `AbortPreparedStatement`, exposed as an Arbiter/SNode wire RPC — does not exist in the current Sentio Arbiter companion repos (arbiter `03aa035`, arbiter-proto `2fa9263`, re-verified 2026-07-20). SNode local intake is still the single-shot in-process `snode.Role.SubmitLocalStatement`; the proto exposes only `RegisterResultClaim`, with no prepare / register-later / abort split and no abort/cancel command in the Arbiter alphabet.

Because HouseGate must not fabricate the Arbiter/SNode protocol, this PR ships:

1. this scoped spec;
2. the pure HouseGate-local ACK2 dual gate (`Ack2Inputs` + `Ack2Ready`) and its wiring into the orchestrator's RC-late-binding path;
3. contract tests. The gate's own unit tests are pure HouseGate logic and run green today. The end-to-end accepted-submit → bound-claim → ACK2 orchestration tests remain gated by `requireCompanionStagedIntake`, so they skip closed while the companion seam is absent and a red (skipped) run is never mistaken for a green one.

When the companion seam lands, the `CompanionStagedIntakeAvailable` constant flips to true, the two adapters are implemented, and the gated orchestration tests — which already exercise the accepted → ACK2 path through the gate — become the executable spec for the real intake. No local HTTP or fake gRPC shape is added to make the gated tests pass in the meantime.

## Design Anchors

This contract implements the ACK2 gate defined in section 3.4 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`):

> ACK 2 只能在以下条件全部成立时返回：
> `payload_durable && unsafe_write_complete && SubmitStatement in {Accepted, ExactIdempotentReAck} && RegisterResultClaim in {Bound, ExactIdempotentAcceptance} && local_intake_journal.lifecycle == RCBound`

It also carries the RC-late-binding rule from section 3.2 (register the result claim only after `SubmitStatement` is accepted) and the outcome classification of section 3.4 (only the success / exact-idempotent categories keep ACK2 reachable; retryable and unknown never ACK2 and never clean; only a terminal reject aborts).

This contract does not change the Sentio Arbiter design or require any Arbiter API/FSM change, and it introduces no new Arbiter command.

## The Dual Gate

`Ack2Ready(Ack2Inputs) (bool, string)` is the single predicate. `Ack2Inputs` is a pure value the orchestrator populates from the durable intake record and the two protocol outcomes:

| Input | Source | Meaning |
| --- | --- | --- |
| `PayloadDurable` | intake record | the content-addressed payload was durably put on the source (`payload_durable`) |
| `UnsafeWriteDone` | intake record | the source's unsafe INSERT completed and its exact candidate inventory was persisted (`unsafe_write_complete`) |
| `Submit` | Arbiter `SubmitStatement` | ACK2 reachable only for `{Accepted, ExactIdempotent}` |
| `Claim` | SNode `RegisterResultClaim` | ACK2 reachable only for `{Bound (Accepted), ExactIdempotentAcceptance}` |
| `JournalLifecycle` | intake journal | must be exactly `RCBound` |
| `PreparedSource` | `PreparedLocalResult.SourceNode` | the source the local write was prepared on |
| `ClaimBoundSource` | `ClaimOutcome.BoundSource` | the source the FSM recorded for the statement |

The gate returns `true` only when every condition holds:

1. payload durable,
2. unsafe write complete,
3. submit in `{Accepted, ExactIdempotent}` (via `OutcomeCategory.PermitsAck2`),
4. claim in `{Accepted, ExactIdempotent}`,
5. journal lifecycle `== RCBound`,

plus the source-agreement invariant: `PreparedSource` and `ClaimBoundSource` are both non-empty and equal. On any failure it returns `false` and a short reason naming the earliest unmet condition (checks are ordered earliest-lifecycle-fact first), which the orchestrator surfaces as diagnostic-only `IntakeResult.Reason`. The reason is diagnostic; the authoritative protocol outcome stays in `Submit`/`Claim` and `Ack2`.

The exact-idempotent category is treated identically to a fresh accept on both gates, so an idempotent re-ack or a byte-identical RC re-bind is ACK2-eligible — matching the design's `{Accepted, ExactIdempotentReAck}` / `{Bound, ExactIdempotentAcceptance}` success classes and the ACK2-semantics requirement that a re-ack is equivalent.

## Single ACK2 Decision Point

Before this PR, `registerAndFinish` set `Ack2 = true` inline after its own sequence of branch checks. That inline grant is now replaced by a call to `Ack2Ready`: after the RC has been classified as bound (not retryable/unknown/terminal) and the accept-path source-agreement check has passed, the orchestrator advances the journal to `RCBound` and then asks the gate whether ACK2 is warranted, passing the durable facts (`PayloadDurable`/`UnsafeWriteDone` are true because reaching this point means the prepared candidate was cached, i.e. the local write is complete and durable). ACK2 is granted only if the gate says so.

The branch checks that trigger *cleanup* (a terminal RC reject, or a blank/mismatched bound source) remain in `registerAndFinish`, because those must call `AbortPreparedStatement`, which is an action, not merely a withheld ack. The gate is the withhold-or-grant decision for the success path; the abort branches are the terminal-reject path. This keeps a single positive ACK2 grant while preserving the exact cleanup semantics. The defensive gate check in `registerAndFinish` cannot fire today (the branch checks above it already exclude every non-ack input), but it fails closed rather than acking on an unexpected input, and does not mark the outcome terminal so a retry can still converge.

## RC Outcome Classification Ordering

Wiring the ACK2 gate onto the RC-late-binding path exposed and fixes an ordering defect in `registerAndFinish`. The RC outcome must be classified by category before its bound `source_node` is inspected, because a retryable or unknown RC (for example `NotLeader`) legitimately carries an empty `BoundSource` — the FSM has not bound a source yet. Checking the source first would misread that empty `BoundSource` as a terminal "source mismatch" and abort the retryable claim, violating design section 3.4, which requires that a retryable/unknown outcome returns no ACK2 **and performs no candidate-part cleanup** (it reuses the journal and retries, holding the source frontier).

The corrected order is: (1) terminal reject → abort the exact candidate; (2) retryable/unknown → return with no ACK2 and no abort; (3) accept path only — validate that the prepared source and the RC's bound source are both non-empty and equal, aborting on a genuine inconsistency; (4) advance the journal to `RCBound` and consult `Ack2Ready`. The source-agreement invariant is thus evaluated only for a claim that actually bound (`Accepted` / `ExactIdempotent`). An accepted claim with a blank or mismatched bound source is still a terminal inconsistency and still aborts — `TestOrchestrate_CommittedSourceMismatchFailsClosed` (accepted RC bound to a different source) continues to fail closed under the new order, now reached one branch later on the accept path. This ordering is what lets a statement whose submit was accepted but whose first RC was retryable resume from the `SubmitAccepted` stage and converge to ACK2 on a later attempt, instead of being wrongly cleaned.

## Unsafe-Write-Alone Safety

The design explicitly forbids acking a statement on the strength of a completed local write alone. The gate enforces this directly: `PayloadDurable && UnsafeWriteDone` with a retryable submit, an unbound claim, and an `UnsafeWritten` lifecycle returns `false`. A client is told the storage-integrity statement succeeded only after the Arbiter has sequenced it and the source claim is bound and journaled as `RCBound`.

## Non-Scope

This change does not implement the durable intake journal storage, the ClickHouse unsafe write, the payload-store client, the real gRPC clients for the Arbiter and SNode, crash recovery scanning, or the ingress-plugin wiring that delivers ACK2 to the client connection. The full retryable/unknown convergence machinery — query-before-resend for unknown outcomes, frontier accounting across retries, and idempotent re-send bookkeeping — is PR05; terminal-reject abort and exact cleanup is PR06; P1e runtime wiring and E2E is PR07. Each of those depends on the companion staged-prepare seam existing. This PR is limited to the ACK2 dual-gate predicate, its integration into the existing orchestration path, and the narrow RC-outcome-classification ordering fix that path requires (a retryable/unknown RC must not be aborted); it does not add PR05's broader convergence behavior, which the PR03 orchestrator already carries for the submit path.

## Verification

Focused gate:

```bash
go test ./pkg/storageintegrity ./pkg/plugins/storageintegrity -count=1
go test -race ./pkg/storageintegrity -count=1
```

Bazel gate:

```bash
bazel test //pkg/storageintegrity:storageintegrity_test //pkg/plugins/storageintegrity:storageintegrity_test
```

The `Ack2Ready` unit tests are pure HouseGate-local logic and run green today: the single all-conditions-present positive case; the necessity of each of the five conditions (each dropped or invalidated in turn withholds ACK2 with a reason); exact-idempotent equivalence on both gates; the non-empty-and-equal bound-source invariant; and the unsafe-write-alone safety property. The end-to-end accepted → ACK2 orchestration tests stay gated by `requireCompanionStagedIntake` and skip closed while the companion `PrepareLocalStatement` / `RegisterPreparedClaim` / `AbortPreparedStatement` seam is absent, so a red run is never mistaken for a green one; among them, `TestOrchestrate_ResumeFromSubmitAcceptedReachesAck2` pins the resume path (accepted submit, first RC retryable, retry converges to ACK2 through the gate without re-running the unsafe write) and is the regression guard for the RC-ordering fix. The existing `TestOrchestrate_CommittedSourceMismatchFailsClosed` still fails closed under the reordered checks. The suite is race-clean.
