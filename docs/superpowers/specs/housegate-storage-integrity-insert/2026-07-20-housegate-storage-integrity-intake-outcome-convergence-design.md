# HouseGate Storage Integrity Retryable/Unknown Intake Outcome Convergence

Date: 2026-07-20

## Purpose

This change adds deterministic convergence for an **indeterminate** intake outcome. When a `SubmitStatement` or `RegisterResultClaim` returns `Unknown` — a timeout or broken connection where HouseGate cannot tell whether the server accepted the operation — a resume must not blindly re-send. Building on the staged-intake and ACK2-gate orchestration, this slice introduces an optional status-query port so a resume can query the server by `statement_id` and collapse the unknown into a definite category (accepted → converge forward, rejected → route to the terminal path, not-found/transient → safe to idempotently re-send) before deciding whether to re-send at all.

The change is purely additive and honors the two convergence paths design section 3.4 permits for an unknown outcome: "先按同一 `statement_id` 查询" (query first, the deterministic path this slice adds when a querier is wired) **or** "幂等重试" (idempotent retry, the existing path, which remains the behavior when no querier is present). Neither path ever repeats the unsafe write, ACKs a not-yet-converged statement, or cleans candidate parts.

## Companion Gate Status

2026-07-22 implementation update: the paired Arbiter branch now provides the staged SNode prepare/register/abort split, and this HouseGate branch enables `CompanionStagedIntakeAvailable`. The status-query seam remains absent from `arbiter-proto`, so `IntakeStatusQuerier` stays optional and nil unless an embedding topology can answer status by `statement_id`.

Because HouseGate must not fabricate the companion protocol, this slice ships:

1. this scoped spec;
2. the pure HouseGate-local convergence logic: the optional `IntakeStatusQuerier` port, the `NewOrchestratorWithQuerier` constructor, the `classifyQueryConvergence` mapping, the record's `submitUnknown` / `claimUnknown` state, and the two query-before-resend branches in the orchestrator;
3. contract tests. The convergence mapping, frontier/no-abort/no-ack behaviors, and query-then-converge orchestration tests now run under the companion gate using in-test status doubles.

Remaining companion work is a real query-status RPC and adapter. No local HTTP shape is introduced.

## Design Anchors

This contract implements the retryable/unknown handling of section 3.4 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`), specifically the 结果未知 outcome row and recovery rules 6 and 7:

- 结果未知: "不返回 ACK 2，也不能清理；先按同一 `statement_id` 查询或幂等重试以收敛结果" — no ACK2, no cleanup; converge by query **or** idempotent retry.
- Rule 6: retryable/unknown intake records hold the source claim frontier until the outcome converges to `RCBound` or a completed `Cleaned`.
- Rule 7: "任何状态都不能重复写入 unsafe" — no state ever repeats the unsafe write; a resume reuses the cached prepare.

It changes no Arbiter design, requires no Arbiter API/FSM change, and adds no new Arbiter command or proto RPC.

## What Already Existed Versus the Delta

Most of the retryable/unknown handling was already implemented and tested by the earlier staged-intake and ACK2-gate slices, so this slice is a narrow delta:

- **Already present:** a non-accepting submit/RC holds the frontier and never aborts or ACKs; a retry reuses the cached statement record and prepared candidate and never repeats the unsafe write; distinct statements on the same source serialize on the frontier. Before this slice, `OutcomeRetryable` and `OutcomeUnknown` were handled **identically** — both simply returned non-terminal, and a later attempt blind-re-sent.
- **This slice's delta:** distinguish `Unknown` from `Retryable`, and give an unknown outcome a deterministic query-before-resend convergence when a querier is wired.

## The Status-Query Port

`IntakeStatusQuerier` is an optional port, separate from `StatementSubmitter` / `SourcePreparer`:

```go
type IntakeStatusQuerier interface {
    QuerySubmitStatus(ctx, statementID) (SubmitOutcome, error)
    QueryClaimStatus(ctx, statementID) (ClaimOutcome, error)
}
```

It is a separate optional port (not new methods on the existing interfaces) for three reasons: the query capability is orthogonal to submit/prepare; it keeps the existing constructor `NewOrchestrator(submitter, preparer, cfg)` and every existing fake untouched (a nil querier reproduces today's behavior exactly); and it keeps the honesty story clean — only the gated convergence tests supply a querier, and no production fake claims a companion capability it lacks. It is stored as an explicit nil-able `Orchestrator.querier` field, never discovered by a runtime type-assertion on the submitter/preparer, so the nil-fallback branch is visible and testable.

The querier only **reads** status; it never mutates protocol state, and it never returns a category the two operations could not themselves return (it reuses `SubmitOutcome` / `ClaimOutcome`, no new category). `NewOrchestratorWithQuerier(submitter, preparer, querier, cfg)` is the additive constructor.

## Convergence Mapping

`classifyQueryConvergence(OutcomeCategory) queryConvergence` collapses a queried status into a resend decision:

| Queried category | Convergence | Action |
| --- | --- | --- |
| `Accepted` / `ExactIdempotent` | `convergeForward` | the server already landed it — proceed without re-sending |
| `TerminalReject` | `convergeReject` | route to the existing terminal-reject path (abort) |
| `Unspecified` (not found) / `Retryable` / `Unknown` | `convergeResend` | no record or still transient — an idempotent re-send is safe |

A "not found" (`Unspecified`) result is resend-safe, **never** a synthesized terminal — a missing record means the operation never landed, not that it was rejected. A query that itself errors is treated as "still unknown": the error is surfaced, the frontier stays held, and a later attempt retries.

## Convergence Flow

The record gains `submitUnknown` / `claimUnknown`, set at the non-accepting submit/RC branch only when the category is exactly `Unknown` (a plain `Retryable` does not set them, so it does not trigger a query). Two resume points consult them:

- **Unknown submit** (stage below `SubmitAccepted`): `convergeUnknownSubmit` runs before any re-submit. With a querier and `submitUnknown`, it queries submit status. `convergeForward` hands the queried outcome to `afterSubmit` as the effective submit result — reaching the RC gate with **no second `SubmitStatement` and no second `PrepareLocalStatement`**. `convergeResend` falls through to exactly one idempotent re-submit (still no re-prepare). `convergeReject` routes to abort. Without a querier, it is a no-op and the existing idempotent re-submit runs.
- **Unknown RC** (stage `SubmitAccepted`): `convergeUnknownClaim` runs before any re-register. With a querier and `claimUnknown`, it queries claim status and finalizes the result **directly** via the shared `finalizeClaim` helper — a queried-`Bound` claim reaches ACK2 with **no second `RegisterPreparedClaim`**. `convergeResend` falls through to a single re-register. Without a querier, it is a no-op and the existing re-register runs.

`registerAndFinish` was refactored to split registration from finalization: `finalizeClaim` runs the category classification, the accept-path source-agreement check, and the ACK2 dual gate, and is shared by both the fresh-registration path and the query-convergence path so a queried-Bound claim is never re-registered.

## Safety Invariants Preserved

- **Never repeats the unsafe write.** Every convergence path reuses the cached prepare; `PrepareLocalStatement` is only ever called in the not-yet-prepared branch, which no unknown resume takes.
- **Never ACKs a non-converged statement.** ACK2 is still granted only through the single `Ack2Ready` dual gate in `finalizeClaim`.
- **Never cleans on a non-terminal outcome.** Only `TerminalReject` (fresh or queried) reaches abort.
- **Holds the frontier until convergence.** An unknown outcome that stays non-terminal returns a non-terminal result, so `Orchestrate` does not release the frontier; a different statement on the same source stays blocked.
- **Nil-querier is spec-compliant.** With no querier, an unknown outcome follows section 3.4's "或幂等重试" branch — an idempotent re-send that reuses the cached prepare.

## Non-Scope

This change does not implement the real query RPC or a production `IntakeStatusQuerier`, the durable intake journal, crash-recovery journal scanning (design rules 2 and 5), or a companion proto RPC. It does not introduce a new `OutcomeCategory`. The paired Arbiter branch owns the ClickHouse unsafe write and staged abort execution; this HouseGate branch owns the query-convergence port and orchestration behavior.

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

The convergence mapping and local behaviors run green today: `TestClassifyQueryConvergence` (every category -> resend decision, including not-found -> resend-safe); `TestOrchestrate_UnknownSubmitDoesNotAbortOrAck` (unknown submit -> no abort, no ACK2); `TestOrchestrate_UnknownOutcomeHoldsFrontier` (an unknown intake blocks a different statement on the same source); and the deterministic query-then-converge orchestration tests under `requireCompanionStagedIntake`: `TestOrchestrate_UnknownSubmitQueriesBeforeResend`, `TestOrchestrate_UnknownSubmitQueryFindsAcceptedConvergesForward`, `TestOrchestrate_UnknownSubmitQueryNotFoundAllowsResend`, and `TestOrchestrate_UnknownRCQueriesBeforeReregister`. Existing staged-intake and ACK2-gate tests are unchanged when the querier is nil. The suite is race-clean.
