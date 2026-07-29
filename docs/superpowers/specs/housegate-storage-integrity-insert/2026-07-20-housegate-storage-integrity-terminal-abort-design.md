# HouseGate Storage Integrity Terminal-Reject Abort And Exact Cleanup

Date: 2026-07-20

## Purpose

This change makes terminal-reject cleanup exact and HouseGate-bounded. When a `SubmitStatement` or `RegisterResultClaim` is terminally rejected, the intake aborts the prepared candidate, and this slice ensures that abort drops **exactly** the frozen candidate part names the journal recorded, never a whole partition and never parts inferred source-side. It does so by threading the record's frozen `CandidateParts` through the abort seam, so the cleanup surface is dictated by HouseGate's own inventory rather than trusted to a companion-side lookup by `statement_id`.

Most of the abort control flow already existed from the staged-intake slice (abort on terminal reject, retryable/unknown never abort, failed-abort retried not recorded, in-process resume from `AbortPending`). This slice's delta is narrow but load-bearing: it closes the gap where the exact-candidate contract was documented but not enforced.

## Companion Gate Status

This design slice is a blocked skeleton, on the same basis as the staged-intake, ACK2-gate, and unknown-convergence slices. The abort seam it drives — `SourcePreparer.AbortPreparedStatement` — is part of the C1 staged-prepare split (`PrepareLocalStatement` / `RegisterPreparedClaim` / `AbortPreparedStatement`) that the Sentio companion repos do not expose (arbiter `03aa035`, arbiter-proto `2fa9263`, re-verified 2026-07-20; SNode intake is still the one-shot in-process `SubmitLocalStatement`, and the Arbiter command alphabet has no abort/cancel/revoke command).

Because HouseGate must not fabricate the companion protocol, this slice ships:

1. this scoped spec;
2. the pure HouseGate-local change: the `AbortPreparedStatement` port signature now carries the exact `[]CandidatePart`, the `abortParts` helper that sources exactly the frozen inventory, and the `abort()` call threading it through;
3. contract tests. The exact-parts cleanup bookkeeping is pure HouseGate logic — which parts get handed to the seam — and runs green today. The end-to-end accepted → ACK2 orchestration tests remain gated by `requireCompanionStagedIntake` and skip closed while the companion seam is absent, so a red (skipped) run is never mistaken for a green one.

When the companion seam lands, a real `AbortPreparedStatement` is implemented against the abort RPC / `ALTER TABLE hg_unsafe.<table> DROP PART` execution, and it receives the exact parts to drop. No local mock shape is added in the meantime.

## Design Anchors

This contract implements the terminal-reject cleanup of section 3.4 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`), the 终态拒绝 outcome row and recovery rules 3–4:

- 终态拒绝: "不返回 ACK 2；进入 exact candidate cleanup".
- Rule 3: on a terminal reject, atomically mark `AbortPending` and logically exclude the candidates from the source absolute view's unpromoted sums, completing that durable step before the physical delete.
- Rule 4: physical cleanup may only run idempotent `ALTER TABLE hg_unsafe.<table> DROP PART '<exact_part_name>'` per journal exact part name — **never a whole partition** — and a part that does not exist is treated as already cleaned.

It changes no Arbiter design, requires no Arbiter API/FSM change, and adds no new Arbiter command or proto RPC.

## What Already Existed Versus the Delta

- **Already present:** `abort()` is invoked only on a terminal reject (or a source/prepared-id inconsistency that is itself terminal); retryable and unknown outcomes never abort; a failed abort keeps the record at `AbortPending` and returns an error (not recorded terminal) so a later attempt re-runs only the abort, never the unsafe write; the in-process resume from `AbortPending` re-invokes abort with the cached candidate. These are covered by `TestOrchestrate_NoClaimWhenSubmitTerminalReject`, `TestOrchestrate_RetryableSubmitDoesNotAbortOrAck`, `TestOrchestrate_FailedAbortIsRetriedNotRecorded`, and `TestOrchestrate_PreparedStatementIdMismatchRejected`.
- **This slice's delta:** the exact-candidate contract was documented on `CandidatePart` ("abort drops these part names and nothing else") but **not enforced** — `abort()` passed only `statementID` and `reason` to the seam, never the `CandidateParts` list, so HouseGate implicitly trusted the source to look up the parts. This slice hands the frozen exact parts to the seam, making "只使用 journal exact parts / 不做 partition-wide cleanup" a HouseGate-enforced contract.

## The Exact-Parts Contract

The abort port now carries the exact candidate parts:

```go
AbortPreparedStatement(ctx, statementID string, parts []CandidatePart, reason string) error
```

`abort()` sources `parts` from `abortParts(rec)`, the single point that decides the cleanup surface: it returns exactly `rec.prepared.CandidateParts` (a defensive copy) when a prepare is cached with a non-empty inventory, and `nil` otherwise. Consequences:

- **Bounded to the journal.** The drop set is exactly the parts the prepare froze — never a bare partition id, never extra parts, never parts from another statement. The seam is told *what* to drop rather than inferring it from `statement_id`, so a partition-wide cleanup is impossible on the HouseGate side.
- **A defensive copy.** The slice handed out is a copy, so a caller or the seam mutating it cannot corrupt the record's frozen inventory, which a retry must reuse verbatim.
- **Empty is a no-op cleanup.** A terminal reject whose candidate inventory is empty (or absent) hands `nil` and still reaches `Cleaned` — a part not present is already clean (rule 4).
- **Retry hands the identical set.** Because `rec.prepared` is immutable after caching, a resume from `AbortPending` calls `abortParts` again and gets the same frozen parts; the failed attempt and the retry drop exactly the same names.

The `PartitionNewPartSums` needed for the design's logical-exclusion step (rule 3) already live in `PreparedLocalResult`; that exclusion is a source-side computation the seam performs, so this slice carries the exact parts (the minimum HouseGate must dictate) and leaves the sum-exclusion to the source implementation behind the seam.

## Crash Recovery Scope

Design rule 5 ("进程重启时扫描 `Preparing` 和 `AbortPending` 并继续未完成步骤") requires a durable intake journal that survives process restart. The current `intakeRecord` is in-memory and is lost on restart, so true cross-restart recovery scanning depends on the durable-journal store that arrives with the P1e runtime wiring. This slice therefore models recovery as **in-process resume from the durable stage**: a mid-abort failure leaves a resumable `AbortPending` record, and a later `Orchestrate` call for the same statement completes the exact cleanup and reaches `Cleaned`, reusing the frozen parts and never re-preparing. Durable-journal persistence and cross-restart `Preparing` / `AbortPending` scanning (rules 2 and 5) are deferred to the runtime wiring.

## Non-Scope

This change does not implement a durable journal store or any persistence layer, real ClickHouse `ALTER TABLE … DROP PART` execution, a real gRPC abort client, cross-process-restart recovery scanning (rules 2 and 5), or the `Preparing`-stage `_hg_row_id` partition rescan of rule 2. It does not extend abort to retryable/unknown outcomes (abort stays terminal-only), does not carry or apply the source-side `PartitionNewPartSums` exclusion, does not flip `CompanionStagedIntakeAvailable`, and adds no companion proto RPC.

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

The exact-parts cleanup behaviors run green today (they exercise HouseGate-local bookkeeping — which parts get handed to the seam — not an accepted → ACK2 path): `TestOrchestrate_AbortPassesExactCandidateParts` (abort targets exactly the frozen `CandidateParts`, no extras, none dropped), `TestOrchestrate_AbortNeverTargetsWholePartition` (every target is a concrete part name, never a bare partition), `TestOrchestrate_EmptyCandidatePartsAbortCleansIdempotently` (empty inventory still reaches `Cleaned`), `TestOrchestrate_AbortPendingResumesExactPartsAfterFailure` (the failed attempt and the retry hand the identical frozen set; retry → `Cleaned`; no re-prepare), and `TestOrchestrate_RetryableThenTerminalAbortsExactParts` (a retryable submit that later terminally rejects aborts exactly the cached parts, no re-prepare). Pre-existing staged-intake, ACK2-gate, and unknown-convergence tests are unchanged (the fakes' `AbortPreparedStatement` gained the parts parameter but ignore it). The suite is race-clean.
