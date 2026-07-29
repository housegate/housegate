# HouseGate Storage Integrity Staged Prepare And SubmitStatement Orchestration

Date: 2026-07-20

## Purpose

This change defines the HouseGate-local orchestration contract that turns a consumed storage-integrity INSERT admission into a durable, ACK2-eligible intake by running two independent conversations against the Sentio companion topology: a staged local prepare against the selected deterministic-source SNode, and a statement submission against the Arbiter. The two conversations run in parallel, but the source result claim is registered only after the Arbiter has accepted the statement.

The orchestration is intentionally fail-closed at every boundary that would otherwise let HouseGate return ACK2 without a bound result claim, leave an unrevocable orphan claim, re-execute an unsafe write, or clean candidate parts on a non-terminal outcome.

## Production Wiring Status

HouseGate owns the staged-intake orchestration and exposes source-side ports for
`PrepareLocalStatement`, `RegisterPreparedClaim`, `AbortPreparedStatement`, and
prepared-record lookup. The production host owns the selected-SNode adapter:
HouseGate deliberately does not import `arbiter-core`.

Production construction validates this capability set dynamically. It requires
a `SourcePreparer` that also implements `PreparedStatementLookup`, an Arbiter
status-query path, durable journal and payload-spool storage, payload-store
wiring, and the merge guard. Missing capabilities fail startup closed rather
than hiding the path behind a repository-version constant.

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

This orchestration contract does not implement the ClickHouse unsafe write,
`_hg_row_id` injection, part LtHash / source-claim-root computation (owned by
`arbiter-core` source execution), or Arbiter FSM / quorum / manifest /
consensus behavior. The production host supplies the source adapter and
PayloadStore client. HouseGate supplies the durable journal, payload spool,
recovery, merge-guard startup, Arbiter status adapter, and ACK2 ingress wiring.

## Verification

Focused gate:

```bash
go test ./pkg/storageintegrity ./pkg/plugins/storageintegrity -count=1
```

Bazel gate:

```bash
bazel test //pkg/storageintegrity:storageintegrity_test //pkg/plugins/storageintegrity:storageintegrity_test
```

The staged-intake contract tests run normally. They cover the accepted
submit-to-ACK2 path, envelope and payload identity, status convergence,
prepared-record recovery, immutable source ordering, single prepare across
retry and restart, exact abort cleanup, payload leases, and fail-closed runtime
construction. Production startup still requires the host-owned source adapter
and the configured external services.
