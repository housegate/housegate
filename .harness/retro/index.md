# Retro Index

Frequency tracking and pending proposals across harness tasks in this repo.
Seeded 2026-06-05 from the `downstream-metrics` retro (first retro in this repo).

## Error Pattern Frequency

| Category | Total | Last 10 | Trend | Status |
|----------|-------|---------|-------|--------|
| infra: backend sub-agent outage (Generator/Evaluator spawn drops) | 1 | 1 | new | Monitoring |
| process: commit/checkpoint machinery sweeps unrelated working-tree path | 2 | 2 | new | Proposed rule (review-loop scoping) |
| process: docs_vs_ci_drift (host docs describe non-existent CI) | 1 | 1 | new | Proposed rule (host CLAUDE.md) |
| process: stale builder/symbol reference in host doc | 1 | 1 | new | Proposed rule (host SKILL.md) |
| process: coverage scoping tension (file-level vs new-code) | 1 | 1 | new | Proposed rule (task-scoped coverage) |
| process: review-loop cross-model peer blocked / stdin-broken | 1 | 1 | new | Proposed (skill defects SD-1, SD-2) |

Notes:
- The "commit/checkpoint sweeps unrelated path" row counts 2 occurrences within
  this one task: the CP01 local-git auto-stage of `CLAUDE.md` (process,
  self-recovered) and the review-loop preflight `git add -A` sweeping
  `tools/da-mvp/pkg/celestia/client.go` (harness skill defect SD-3). Two
  instances is approaching the 3+ actionability bar — watch for recurrence to
  escalate from Proposed to active rule.

## Pending Rule Proposals

| ID | Title | target_repo | Severity | Status | Issue-ready |
|----|-------|-------------|----------|--------|-------------|
| P1 | Task-scoped coverage for features added to a large repo | harness | medium | Proposed | yes |
| P2 | Document degraded-mode orchestrator-implements + independent-evaluates | harness | medium | Proposed | yes |
| P3 | Fix stale CI + Conventions documentation in host CLAUDE.md | host | high | Proposed | yes |
| P4 | Fix stale `buildForwarding` reference in add-housegate-plugin SKILL | host | medium | Proposed | yes |

Source retro: `.harness/retro/2026-06-05-downstream-metrics.md`.

## Pending Principle Proposals

| ID | Title | Status | Evidence count |
|----|-------|--------|----------------|
| PR1 | A final cross-perspective review earns its place even after all automated gates are green | Candidate (1 data point) | 1 |

Promote PR1 to an enacted principle if review-loop surfaces a real post-all-green
bug in 3+ tasks. This task's data point: review-loop round 1 caught the
`ch_os_cpu_seconds` counter→gauge monotonicity bug that 9 checkpoint evals + E2E
+ full-verify all missed.

## Rule Lifecycle Tracker

| Rule | Stage | Since | Notes |
|------|-------|-------|-------|
| (none enacted yet) | — | — | First retro; all items are at Proposed/Monitoring/Candidate stage. |

Stages: observation → monitoring → proposed → active → retired.

## Skill Defect Log

| ID | Skill | Defect | target_repo | Severity | Status |
|----|-------|--------|-------------|----------|--------|
| SD-1 | review-loop | Codex peer (`codex exec --dangerously-bypass-approvals-and-sandbox`) blocked by Claude Code permission classifier in auto mode; silently degrades to same-model review | harness | high | Proposed |
| SD-2 | review-loop | `peer-invoke.sh` run_with_timeout fallback (no GNU timeout, e.g. macOS) drops codex stdin → "No prompt provided via stdin" | harness | high | Proposed |
| SD-3 | review-loop | Preflight `git add -A` checkpoint commit sweeps unrelated working-tree paths into the feature branch / PR diff | harness | high | Proposed |

All three map to retro Proposals 5–7 in
`.harness/retro/2026-06-05-downstream-metrics.md`.

## Filed Issues

(none yet — Orchestrator files issues for `Issue-ready: true` items; record URLs here)
