# HouseGate Storage Integrity Retryable/Unknown Intake Outcome Convergence

Date: 2026-07-20

Last updated: 2026-07-29

## Purpose

This change adds deterministic convergence for an **indeterminate** intake outcome. When a `SubmitStatement` or `RegisterResultClaim` returns `Unknown` — a timeout or broken connection where HouseGate cannot tell whether the server accepted the operation — a resume must not blindly re-send. Building on the staged-intake and ACK2-gate orchestration, this slice introduces an optional status-query port so a resume can query the server by `statement_id` and collapse the unknown into a definite category (accepted → converge forward, rejected → route to the terminal path, not-found/transient → safe to idempotently re-send) before deciding whether to re-send at all.

The change is purely additive and honors the two convergence paths design section 3.4 permits for an unknown outcome: "先按同一 `statement_id` 查询" (query first, the deterministic path this slice adds when a querier is wired) **or** "幂等重试" (idempotent retry, the existing path, which remains the behavior when no querier is present). Neither path ever repeats the unsafe write, ACKs a not-yet-converged statement, or cleans candidate parts.

## Companion Capability Status

Arbiter-proto `v0.4.0` now exposes
`ArbiterIngress.GetStatementStatus(statement_id)`. HouseGate adapts this RPC
through `NewArbiterIntakeStatusQuerier`; production runtime assembly
auto-constructs the querier from `ArbiterIngressClient` and otherwise requires
an explicitly injected `IntakeStatusQuerier`.

The probe exposes a sequenced statement view and an `rc_bound` bit, not an
independent durable rejection ledger for each RPC. HouseGate therefore maps it
conservatively:

- submit query: `found=true` means the original submit landed, even if the
  statement later reached FSM status `Rejected`;
- claim query: only `rc_bound=true` with a non-empty `bound_source` proves that
  claim registration landed;
- `found=false`, or found-but-not-bound for a claim, means resend-safe
  `Unspecified`, never terminal rejection;
- malformed responses and RPC failures remain errors, retaining the frontier.

The staged SNode capability also exists in arbiter-core. Its adapter remains
host-owned because HouseGate does not import arbiter-core.

## Design Anchors

This contract implements the retryable/unknown handling of section 3.4 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`), specifically the 结果未知 outcome row and recovery rules 6 and 7:

- 结果未知: "不返回 ACK 2，也不能清理；先按同一 `statement_id` 查询或幂等重试以收敛结果" — no ACK2, no cleanup; converge by query **or** idempotent retry.
- Rule 6: retryable/unknown intake records hold the source claim frontier until the outcome converges to `RCBound` or a completed `Cleaned`.
- Rule 7: "任何状态都不能重复写入 unsafe" — no state ever repeats the unsafe write; a resume reuses the cached prepare.

It changes no Arbiter FSM state or command. HouseGate consumes the read probe
already published by arbiter-proto `v0.4.0`.

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

It remains an optional core port (not new methods on the existing interfaces)
so embedders may use the design-permitted idempotent retry path. The built-in
production runtime is stricter and requires the port. It is stored as an
explicit nil-able `Orchestrator.querier`, never discovered by type assertion on
the submitter/preparer.

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

This change does not add a claim-specific status RPC or infer terminal claim
rejection from the statement's later FSM status. It does not introduce a new
`OutcomeCategory`, alter Arbiter state, or move the host-owned SNode adapter into
HouseGate.

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

The convergence mapping and orchestration tests run without a static companion
skip. Proto-adapter tests additionally pin found/not-found, bound/unbound,
rejected-after-sequencing, malformed response, and RPC-error behavior.
