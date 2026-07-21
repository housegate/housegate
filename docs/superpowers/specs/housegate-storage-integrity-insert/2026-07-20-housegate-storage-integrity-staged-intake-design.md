# HouseGate Storage Integrity Staged Prepare And SubmitStatement Orchestration

Date: 2026-07-20

## Purpose

This change defines the HouseGate-local orchestration contract that turns a consumed storage-integrity INSERT admission into a durable, ACK2-eligible intake by running two independent conversations against the Sentio companion topology: a staged local prepare against the selected deterministic-source SNode, and a statement submission against the Arbiter. The two conversations run in parallel, but the source result claim is registered only after the Arbiter has accepted the statement.

The orchestration is intentionally fail-closed at every boundary that would otherwise let HouseGate return ACK2 without a bound result claim, leave an unrevocable orphan claim, re-execute an unsafe write, or clean candidate parts on a non-terminal outcome.

## Companion Gate Status

This design slice is scoped as a blocked skeleton. The companion staged-prepare seam that HouseGate ingress must drive does not exist in the current Sentio Arbiter companion repos:

- The design (section 3.2) names three source-side seams — `PrepareLocalStatement`, `RegisterPreparedClaim`, and `AbortPreparedStatement` — that split the P1c one-shot local intake into a durable prepare, a late-bound claim registration, and an exact abort.
- The companion repos expose only `ArbiterIngress.SubmitStatement` and `SourceClaims.RegisterResultClaim`. SNode local intake is the single-shot in-process `snode.Role.SubmitLocalStatement`, which performs the unsafe write and registers the result claim in one call and is not reachable as an RPC. There is no prepare/register-later/abort split, and no abort/cancel/revoke command exists in the Arbiter command alphabet.

Because HouseGate must not fabricate the Arbiter/SNode protocol, this slice ships:

1. this scoped spec;
2. the HouseGate-side adapter interfaces (ports) that declare the staged seam HouseGate depends on, plus the pure orchestration types and the orchestrator constructor;
3. contract tests that pin the orchestration invariants and are explicitly not green while the companion `PrepareLocalStatement` / `RegisterPreparedClaim` / `AbortPreparedStatement` seam is absent.

When the companion seam lands, the adapter is implemented against it and the same contract tests become the executable spec for the real orchestration. No local HTTP or fake gRPC shape is added to make the contract tests pass in the meantime.

## Design Anchors

This contract implements the HouseGate ingress responsibilities in section 3 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`):

- Section 3.2: the parallel `PrepareLocalStatement` / `SubmitStatement` orchestration and the RC-late-binding gate.
- Section 3.3: source-claim input completeness (the RC carries the frozen `SourceClaimRoot`, `PartitionNewPartSums`, and exact candidate parts produced by prepare).
- Section 3.4: the ACK2 gate, terminal-reject cleanup, and the crash-safe intake lifecycle.

This contract does not change the Sentio Arbiter design or require any Arbiter API/FSM change. It does not introduce `DiscardPendingRC` or any other new Arbiter command; the RC-late-binding gate is precisely what makes that unnecessary within P1e scope.

## Orchestration Boundary

The intake orchestrator consumes one completed `storageintegrity.Admission` (statement id, kind, logical table id, signed SQL, signer, and the exact captured Native payload with its deterministic `sha256:<hex>` hash, length, and pinned client revision). It drives four ports:

- `StatementSubmitter.SubmitStatement(ctx, StatementEnvelope) (SubmitOutcome, error)` — Arbiter route-A sequencing.
- `SourcePreparer.PrepareLocalStatement(ctx, StatementEnvelope, payload) (PreparedLocalResult, error)` — selected-SNode durable prepare.
- `SourcePreparer.RegisterPreparedClaim(ctx, statementID) (ClaimOutcome, error)` — late-bound RC registration triggered on the SNode.
- `SourcePreparer.AbortPreparedStatement(ctx, statementID, reason) error` — exact-candidate abort.

The `StatementEnvelope` HouseGate builds from the admission mirrors the frozen source envelope semantics: statement id, statement kind, signed SQL and its hash, target table id, content-addressed `payload_ref`, `payload_hash`, `payload_length`, the pinned payload encoding/profile, and the captured client protocol `revision`. The exact same envelope value feeds both `PrepareLocalStatement` and `SubmitStatement`, so the payload identity a source claim binds is byte-identical to the identity the Arbiter sequences.

The `revision` is carried because the source preparer must decode the captured Native payload with exactly the client protocol revision that produced it (`DecodeNativePayload` rejects revision 0 and uses the revision to read the block). Without it in the envelope, a real SNode adapter would have to guess or configure a revision, which would break the exact-payload contract for any client that negotiated a different one. A payload-bearing (INSERT) admission is therefore rejected at envelope construction if its revision is 0.

## Parallelism And The RC Gate

`PrepareLocalStatement` and `SubmitStatement` are started concurrently. This preserves route-A local-execution parallelism: the source can be putting the payload, injecting `_hg_row_id`, and doing the unsafe write while the Arbiter is sequencing the statement.

The single hard ordering rule is the RC gate: HouseGate calls `RegisterPreparedClaim` (which triggers the SNode's `RegisterResultClaim` to the Arbiter) only after `SubmitStatement` returns an outcome in `{Accepted, ExactIdempotentReAck}`. Registering a claim before the statement is accepted would, under the current P1a command alphabet, risk an orphan pending claim that cannot be revoked without a new Arbiter command (`DiscardPendingRC`), which is out of P1e scope. The gate makes the orphan impossible.

Within `PrepareLocalStatement`, a fixed internal order holds and is asserted by the source-side contract, not re-implemented by HouseGate: payload hash/length verification and the durable `PayloadStore.Put` complete before the unsafe INSERT, so a failed `Put` never produces an `hg_unsafe` part.

## Payload And Source Consistency

- Before the RC gate opens, the prepared result must bind the submitted statement **completely and exactly**. The prepared result carries its own statement id; it must be non-empty and equal to the submitted statement id — a stale or buggy preparer that answers for a different statement (even one with the same payload and source) is rejected before any claim is registered, so the wrong candidate can never be bound. For a payload-carrying INSERT statement every field of the `{payload_ref, payload_hash, payload_length, payload_encoding, revision}` tuple must be present and equal to the value the envelope carries; a missing (blank or zero) field is treated as a mismatch, never a tolerated wildcard. The revision equality ensures the source decoded the payload with the same client protocol revision the statement pins, since a different revision can yield a different materialization. A prepared result with blank binding fields therefore fails closed and can never take the accepted path to ACK2.
- The prepared result must carry a non-empty `SourceNode`, and it must equal the deterministic source the FSM recorded (`OrchestratorConfig.ExpectedSource`) when one is configured. The RC's bound `source_node` must in turn be non-empty and equal to the source that prepared the write. A committed-source mismatch — or a blank source on either side — is terminal: no ACK2, and the intake proceeds to exact-candidate cleanup via `AbortPreparedStatement`.
- The payload encoding/profile is threaded explicitly into the prepare envelope (Native vs CSV), not merely declared supported at ingress, so the staged SNode intake and the replay profile agree by construction.

## Outcome Classification And ACK2

`SubmitStatement` and `RegisterResultClaim` outcomes are classified into four categories, and only the success/exact-idempotent path reaches ACK2:

| Category | Examples | ACK2 and local handling |
| --- | --- | --- |
| Success or exact idempotent | accepted, same-envelope re-ack, byte-identical RC re-bind | ACK2 only after both gates complete |
| Retryable | `NotLeader`, transient unavailability, explicit retryable error | no ACK2, no cleanup; reuse the prepared record and retry |
| Unknown | timeout, broken connection with indeterminate server state | no ACK2, no cleanup; query or idempotently re-send by `statement_id` to converge |
| Terminal reject | conflicting duplicate, source mismatch, malformed, gap budget exceeded | no ACK2; enter exact-candidate cleanup via `AbortPreparedStatement` |

ACK2 is returned only when all of the following hold: payload durable, unsafe write complete, `SubmitStatement` in `{Accepted, ExactIdempotentReAck}`, `RegisterResultClaim` in `{Bound, ExactIdempotentAcceptance}`, and the local intake journal lifecycle is `RCBound`. Retryable and unknown outcomes never return ACK2 and never clean candidate parts. Only a terminal reject triggers abort, and abort deletes only the exact candidate part names the prepared record froze — never a whole partition.

Abort is treated as a step that can fail. A terminal reject whose `AbortPreparedStatement` succeeds is a terminal outcome (`Cleaned`) and is recorded for idempotent replay. A terminal reject whose abort **fails** is not terminal: the orchestrator surfaces the error and does not record the outcome, so a later call retries the cleanup rather than silently leaving orphan candidate parts behind an `AbortPending` record that is never revisited.

## Idempotency And Serial Frontier

The durable per-statement record is bound to the **first** complete statement envelope it was opened with. Every later call for the same `statement_id` must present a byte-identical envelope — identical SQL, target table, statement kind, payload identity and raw payload bytes, signer, and JWS. A call that reuses the id with any differing field is rejected fail closed and never runs: this closes the attack where a retry reuses an id whose prepared unsafe write already exists but supplies a different envelope to submit and sequence a different statement. A retry always resumes against the stored original envelope, never the newly supplied one, so a resumed submit/RC-registration can only ever advance the statement the id first prepared.

Prepare runs at most once per `statement_id` — under concurrency **and across retries**. The orchestrator keeps the durable per-statement record described above. It caches the successful prepared result the first time `PrepareLocalStatement` returns; every later attempt for that statement reuses the cached record and re-runs only the unresolved stage (the submit if it was not yet accepted, the RC registration if the claim was not yet bound, or the abort if cleanup did not complete). A durable prepare is therefore never repeated after an indeterminate remote outcome, and a second unsafe write is impossible. A concurrent call that races an in-flight attempt for the same id blocks on it and observes the same result. Only terminal outcomes (`Ack2`, or a successfully `Cleaned` abort) are recorded as final; a repeated request for such a statement returns the recorded outcome without re-running anything. Retryable/unknown outcomes and failed aborts are intentionally not final, so they reconverge — or retry their cleanup — on a later call, always reusing the cached prepared record.

v1 enforces the P1c serial source-statement constraint with a per-source frontier gate. Each intake acquires the source frontier **before** its source write (the prepare), so on the same source (the configured deterministic source) at most one intake performs a source write at a time. A different statement on the same source blocks until the holding intake reaches a terminal stage (`RCBound`/`Ack2` on success, or `Cleaned` after abort); a retryable/unknown or still-`AbortPending` intake keeps the frontier held, because its contribution to the source claim view is not yet decided. The gate is re-entrant for the holder: the holding statement's own retry passes straight through the frontier it already holds (no self-deadlock), while distinct statements queue FIFO so none starves. A caller blocked on the frontier honors context cancellation and dequeues cleanly without stranding the gate for the statements behind it.

## Non-Scope

This change does not implement the durable intake journal storage, the ClickHouse unsafe write, the payload-store client, the real gRPC clients for the Arbiter and SNode, `_hg_row_id` injection, part LtHash / source-claim-root computation (owned by `payloadexec` / `chexec` / `pkg/lthash`), crash recovery scanning, ACK2 delivery wiring into the ingress plugin, merge-guard startup, or any Arbiter FSM / quorum / manifest / consensus behavior. Those arrive in the later ACK2 gate, unknown-convergence, terminal-abort, and P1e runtime slices, and each depends on the companion staged-prepare seam existing.

## Verification

Focused gate:

```bash
go test ./pkg/storageintegrity ./pkg/plugins/storageintegrity -count=1
```

Bazel gate:

```bash
bazel test //pkg/storageintegrity:storageintegrity_test //pkg/plugins/storageintegrity:storageintegrity_test
```

While the companion C1 staged-prepare seam is absent, the end-to-end intake contract tests (the accepted-submit → ACK2 path) skip closed with a message naming the missing `PrepareLocalStatement` / `RegisterPreparedClaim` / `AbortPreparedStatement` seam, so a red run is never mistaken for a green one.

The HouseGate-local invariants do not depend on the companion seam and are covered by tests that run and pass today: envelope construction and payload-identity equality between the prepare and submit envelopes; outcome classification; the complete-and-exact prepared-binding requirement including the prepared-statement-id check (`preparedConsistencyReject` plus the statement-id gate — a blank binding field or a blank/mismatched prepared statement id is a mismatch); single-prepare under concurrent same-statement calls; reuse of the cached prepared record on a retry after a retryable outcome (no second unsafe write); rejection of a statement-id reuse that presents a different envelope (SQL/target/kind/signer/JWS or payload bytes), with the resume submitting only the original bound envelope; the serial source frontier blocking a different statement until the holder is terminal, the holder's own retry re-entering without deadlock, and a blocked waiter cancelling cleanly without stranding the gate; and the retry-not-record semantics of a failed abort (which also does not re-prepare). None of these assert an accepted ACK2 — the one behavior that genuinely needs the companion seam — so they add real coverage of HouseGate's own coordination and fail-closed logic without claiming a working staged intake. The suite is race-clean (`go test -race`).
