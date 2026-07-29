# HouseGate Storage Integrity RC Late Binding And ACK2 Dual Gate

Date: 2026-07-20

Last updated: 2026-07-29

## Purpose

This change formalizes the single ACK2 dual gate that decides whether a storage-integrity intake may report success to the client. Building on the staged-prepare orchestration, it factors the ACK2 decision out of the orchestrator's inline branch logic into one pure, exhaustively tested predicate — `Ack2Ready` — that requires all five design-section-3.4 conditions to hold together. The orchestrator's `registerAndFinish` now grants ACK2 in exactly one place, by consulting that gate, so a client can never be told a statement succeeded from a partially satisfied state (for example a completed unsafe write with no bound result claim).

The gate is fail-closed by construction: every one of the five conditions is necessary, a blank sub-field is a mismatch rather than a tolerated wildcard, and the RC's bound source must be present and equal to the source that prepared the local write.

## Companion Capability Status

The staged SNode methods now exist in arbiter-core, and
`ArbiterIngress.GetStatementStatus` exists in arbiter-proto `v0.4.0`.
HouseGate's built-in runtime requires a host-owned source adapter with prepared
lookup and wires the Arbiter submit/status adapters itself. ACK2 orchestration
tests therefore run normally; production startup relies on dynamic capability
validation rather than a compile-time companion gate.

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

The source the FSM recorded for the statement is read directly from `Claim.BoundSource` — the authoritative field on the outcome the gate already inspects — rather than a separate copy, so the gate cannot be told the sources agree while `Claim` carries a different bound source.

The gate returns `true` only when every condition holds:

1. payload durable,
2. unsafe write complete,
3. submit in `{Accepted, ExactIdempotent}` (via `OutcomeCategory.PermitsAck2`),
4. claim in `{Accepted, ExactIdempotent}`,
5. journal lifecycle `== RCBound`,

plus the source-agreement invariant: `PreparedSource` and `Claim.BoundSource` are both non-empty and equal. On any failure it returns `false` and a short reason naming the earliest unmet condition (checks are ordered earliest-lifecycle-fact first), which the orchestrator surfaces as diagnostic-only `IntakeResult.Reason`. The reason is diagnostic; the authoritative protocol outcome stays in `Submit`/`Claim` and `Ack2`.

The exact-idempotent category is treated identically to a fresh accept on both gates, so an idempotent re-ack or a byte-identical RC re-bind is ACK2-eligible — matching the design's `{Accepted, ExactIdempotentReAck}` / `{Bound, ExactIdempotentAcceptance}` success classes and the ACK2-semantics requirement that a re-ack is equivalent.

## Single ACK2 Decision Point

Before this slice, `registerAndFinish` set `Ack2 = true` inline after its own sequence of branch checks. That inline grant is now replaced by a call to `Ack2Ready`: after the RC has been classified as bound (not retryable/unknown/terminal) and the accept-path source-agreement check has passed, the orchestrator advances the journal to `RCBound` and then asks the gate whether ACK2 is warranted, passing the durable facts (`PayloadDurable`/`UnsafeWriteDone` are true because reaching this point means the prepared candidate was cached, i.e. the local write is complete and durable). ACK2 is granted only if the gate says so.

The branch checks that trigger *cleanup* (a terminal RC reject, or a blank/mismatched bound source) remain in `registerAndFinish`, because those must call `AbortPreparedStatement`, which is an action, not merely a withheld ack. The gate is the withhold-or-grant decision for the success path; the abort branches are the terminal-reject path. This keeps a single positive ACK2 grant while preserving the exact cleanup semantics. The defensive gate check in `registerAndFinish` cannot fire today (the branch checks above it already exclude every non-ack input), but it fails closed rather than acking on an unexpected input, and does not mark the outcome terminal so a retry can still converge.

The submit outcome the gate reads is the authoritative one, not a synthesized `Accepted`. When `SubmitStatement` first returns an ACK2-permitting outcome, the orchestrator caches that outcome **verbatim** in the durable intake record as it advances to the `SubmitAccepted` stage, and `resultFor` restores it on every later attempt. So a resume from `SubmitAccepted` — which does not re-run `SubmitStatement` — still reports whether the submit was a fresh `Accepted` or an `ExactIdempotent` re-ack, and `IntakeResult.Submit` remains the true protocol outcome a caller can distinguish. Non-accepting submit outcomes are never cached (they do not reach `SubmitAccepted`).

## RC Outcome Classification Ordering

Wiring the ACK2 gate onto the RC-late-binding path exposed and fixes an ordering defect in `registerAndFinish`. The RC outcome must be classified by category before its bound `source_node` is inspected, because a retryable or unknown RC (for example `NotLeader`) legitimately carries an empty `BoundSource` — the FSM has not bound a source yet. Checking the source first would misread that empty `BoundSource` as a terminal "source mismatch" and abort the retryable claim, violating design section 3.4, which requires that a retryable/unknown outcome returns no ACK2 **and performs no candidate-part cleanup** (it reuses the journal and retries, holding the source frontier).

The corrected order is: (1) terminal reject → abort the exact candidate; (2) retryable/unknown → return with no ACK2 and no abort; (3) accept path only — validate that the prepared source and the RC's bound source are both non-empty and equal, aborting on a genuine inconsistency; (4) advance the journal to `RCBound` and consult `Ack2Ready`. The source-agreement invariant is thus evaluated only for a claim that actually bound (`Accepted` / `ExactIdempotent`). An accepted claim with a blank or mismatched bound source is still a terminal inconsistency and still aborts — `TestOrchestrate_CommittedSourceMismatchFailsClosed` (accepted RC bound to a different source) continues to fail closed under the new order, now reached one branch later on the accept path. This ordering is what lets a statement whose submit was accepted but whose first RC was retryable resume from the `SubmitAccepted` stage and converge to ACK2 on a later attempt, instead of being wrongly cleaned.

## Unsafe-Write-Alone Safety

The design explicitly forbids acking a statement on the strength of a completed local write alone. The gate enforces this directly: `PayloadDurable && UnsafeWriteDone` with a retryable submit, an unbound claim, and an `UnsafeWritten` lifecycle returns `false`. A client is told the storage-integrity statement succeeded only after the Arbiter has sequenced it and the source claim is bound and journaled as `RCBound`.

## Non-Scope

The ACK2 gate itself remains limited to the pure dual-gate predicate and its
orchestration decision point. Durable journal recovery, payload-store clients,
query-before-resend convergence, exact abort, and runtime wiring are implemented
by their corresponding slices. ClickHouse unsafe execution remains behind the
host-owned source adapter.

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

The `Ack2Ready` unit tests and accepted-to-ACK2 orchestration tests run without
skips. They include resume after retryable RC, exact-idempotent outcome
preservation, committed-source mismatch, and unsafe-write-alone rejection. The
suite remains race-clean.
