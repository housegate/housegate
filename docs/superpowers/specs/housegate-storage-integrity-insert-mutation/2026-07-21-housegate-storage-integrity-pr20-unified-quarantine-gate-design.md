# HouseGate Storage Integrity: PR20 — Unified Quarantine and Role/Read-Set Enforcement

Date: 2026-07-21
Branch: `feat/housegate-sentio-pr19-25-safeaudit-quarantine`
Design: `docs/superpowers/specs/2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md` §5.3 (with §5.2 note)

## 1. Scope

A single unified quarantine that uniformly blocks a worker from every affected role: submitting replay / byte-side / mutation / promotion / SafeAudit evidence, claiming a task in a quarantined role, and serving the corresponding safe read set (design §5.3). Crucially, it does **not** rewrite P1a active membership: `MarkActive` controls source/Verifier selection membership; the quarantine read-serving axis controls safe-SELECT serving eligibility only (design §5.2 note). These are different axes.

## 2. `WorkerQuarantine` record

Mirrors the design §5.3 record: `{WorkerID, Reason, EvidenceRef, AffectedRoles []QuarantineRole, SinceBlock, RepairRequired}`. `Valid()` fails closed on a blank worker/reason, a zero `SinceBlock`, an empty affected-role set (a quarantine that blocks nothing is invalid), or an unknown role. `Blocks(role)` is the pure membership predicate; `Clone()` deep-copies the role set.

`QuarantineRole` is a closed enum: `replay`, `byte_side`, `mutation`, `promotion`, `serving_audit`, and `serving` (the safe-SELECT eligibility axis, distinct from the evidence roles and from P1a `MarkActive`).

## 3. `QuarantineGate`

Holds `map[worker]WorkerQuarantine`; `NewQuarantineGate` validates every entry and rejects a key/worker mismatch (fail-closed). It answers the unified decision with a typed `QuarantineDecision` mirroring `GateDecision`:

- **`MaySubmitEvidence(worker, role)`** — a quarantined role's evidence must be rejected before it reaches the Arbiter FSM.
- **`MayClaimTask(worker, role)`** — task claim blocked for a covered role.
- **`MayServe(worker, snapshot, gate SafeReadGate)`** — composes this gate's `RoleServing` quarantine with the *existing* `SafeReadGate`: a worker quarantined for serving is denied here even if the safe cut's own `QuarantinedWorkers` set has not yet been updated (two independent fail-closed checks); otherwise it delegates to `SafeReadGate.MayServe` and maps a cut-quarantine denial to this gate's typed reason. This makes quarantine one unified decision even before the next atomic safe cut installs it into `QuarantinedWorkers`.

## 4. Blocked-skeleton status

The quarantine *decision* logic is pure HouseGate-local and green today — it reuses the PR16 `SafeReadGate`/`SafeCutView` (whose `QuarantinedWorkers`/`GateDenyQuarantined` already exist). What is gated on the absent companion **C3** seam is the *runtime enforcement* across the live evidence-submission and task-claim RPCs (there is no such RPC in arbiter/arbiter-proto yet). That end-to-end path is a single `t.Skip`+`t.Fatal("unreachable ...")` placeholder; no protocol is fabricated.

## 5. Reused types (never redefined)

`SafeReadGate`, `SafeCutView`, `GateDecision`, `GateDenyQuarantined`, `requireCompanionMutationConsensus`.

## 6. Tests

- **Pure (green today):** `WorkerQuarantine.Valid` matrix; `Blocks`/`Clone`; `NewQuarantineGate` entry validation + key/worker-mismatch; unified role blocking (evidence + claim, named vs unnamed role, quarantined vs un-quarantined worker); `MayServe` composing with `SafeReadGate` (quarantine-gate deny before cut install, delegated allow, cut-quarantine mapping, non-quarantine reasons surfaced); stable enum strings.
- **Gated (skip-closed):** enforcement across the live evidence/claim paths.
