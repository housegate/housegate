# Multi-source SI stage 3: handoff (2026-10-16)

What is done, what comes next, how to do it, and the context a new session needs. Repository paths are housegate paths unless a repository is named; local clones live at `/Users/uranuswch/Dev/projects/sentio-network/<repo>`. The detailed status of Plans S3-C1 … S3-F, the binding cross-plan conventions and decisions, and the drafters' open questions are in `2026-10-15-multi-source-si-s3cf-status.md`; this file is the operational summary.

## 1. Done

### 1.1 Design

- Spec `docs/superpowers/specs/2026-10-14-multi-source-si-stage3-database-groups-design.md` (and `.zh-CN.md`), merged as housegate#236 (`fb83a97`). The merge included every review fix:
  - two member users;
  - `table_engines_require_grant` at P2's restart;
  - latched re-selection refusal;
  - strict and non-strict Leaving release;
  - domain counting;
  - `checkpoint_seq`;
  - the P2 exit soak in integration (user decision K38);
  - the P4 rebuild keeping the signer key (user decision K51);
  - the member `CREATE DATABASE` and `READ ON S3` grants.
- The first PR of the stage-3 plans stack (§1.4) amends §8.2's `hg_rstage` rights to an explicit list (D36).

### 1.2 Plan S3-A (P0 code layer): executed

Plan `2026-10-14-multi-source-si-s3a-p0-guards.md` (Tasks 1–11), merged as housegate#237. It was executed through housegate#238–#242 and its Appendix C on `main` records:

| Name | Value |
|---|---|
| `HG_TAG` | housegate `v0.20.0` (`bc92f91`, #238) |
| `PROTO_TAG` | arbiter-proto `v0.12.0` (`7a73f25`, arbiter-proto#20) |
| `CORE_TAG` | arbiter-core `v0.15.0` (`cfd4f42`, arbiter-core#50) |
| `ARBITER_TAG` / `ARBITER_DIGEST` | arbiter `v0.13.0` (`526a721`, arbiter#133) / `sha256:caf48b9a…` |
| Deployed-image mechanics run (K9) | 13 tests passed on `clickhouse-server:26.3-lts-decimal512-release-2026-09-16` |
| Offline devnet2 replay | recorded |

The original rehearsal inputs (`~/si-devnet2-multisource-{rehearsal,s2-rehearsal,tracks}`) were not on the machine that executed S3-A. By the user's decision they were rebuilt there (see S3-A Appendix C, run records).

### 1.3 Plan S3-B (P0 hosts and rollout, lockdown L, indexer-c E): written, not started

Plan `2026-10-14-multi-source-si-s3b-p0-hosts-rollout.md` (Tasks 12–21), merged as housegate#237. Its Appendix C is empty: no task has run.

### 1.4 Plans S3-C1 … S3-F (P1–P6): drafts

The drafts are in six stacked PRs. They replace housegate#243, which held all of them but was too large for the automated reviewers (about 179,000 added lines). Each PR targets the branch of the PR before it; each PR's description gives the numbers of the whole stack.

| # | Branch | Contents |
|---|---|---|
| 1 | `urwt/claude/ms-si-s3cf-1-context-p1` | spec §8.2 amendment (D36), S3-B label and name fixes, the status, scoping and names-index docs, this handoff, Plan S3-C1 |
| 2 | `urwt/claude/ms-si-s3cf-2-p2` | Plans S3-C2, S3-C3 |
| 3 | `urwt/claude/ms-si-s3cf-3-p3` | Plan S3-C4 |
| 4 | `urwt/claude/ms-si-s3cf-4-p4` | Plan S3-D |
| 5 | `urwt/claude/ms-si-s3cf-5-p5-wire-encoding` | Plans S3-E1, S3-E2 |
| 6 | `urwt/claude/ms-si-s3cf-6-p5-groups-p6` | Plans S3-E3, S3-E4, S3-F |

Merge them in order, 1 first. The repository squash-merges and deletes the merged branch, so GitHub then retargets the next PR to `main`. Before merging that PR, merge `main` into its branch: no conflicts are expected, and its diff shrinks to its own files.

64 of 68 tasks are drafted. Every drafted task was verified only by its drafter, in scratch exports. No task has had its adversarial review, and no plan has had its consistency or cross-plan pass. Every plan file says so in a status banner.

| Plan | Phase | Tasks | Drafted | Missing |
|---|---|---|---|---|
| S3-C1 | P1 backups | 22–28 | all | — |
| S3-C2 | P2 member code | 29–36 | all | — |
| S3-C3 | P2 hosts and rollout | 37–41 | all | — |
| S3-C4 | P3 member reads | 42–48 | all | — |
| S3-D | P4 settle, rebuild, hardening | 49–56 | all | — |
| S3-E1 | P5 wire and data plane | 57–63 | all | — |
| S3-E2 | P5 activation and v21 | 64–70 | all | — |
| S3-E3 | P5 groups and checkpoints | 71–75 | all | — |
| S3-E4 | P5 floor, release, activation | 76–82 | 76, 77, 80, 81 | 78, 79, 82 |
| S3-F | P6 switch-on | 83–89 | none (front matter and contract only) | 83–89 |

Task 76 was written but its drafter's summary was lost to a usage limit, so its Appendix B entry is empty.

## 2. Next: two tracks

Tracks A and B can run at the same time. They touch different repositories and branches; only the ledgers in plan files are shared.

### Track A: execute Plan S3-B (devnet2)

1. **Task 12, sentio-node.**
   - Pin housegate `v0.20.0`, arbiter-core `v0.15.0` and arbiter-proto `v0.12.0`.
   - Add the SI ClickHouse login, the owner-audit mode, Pending-with-schema and the owner-audit metrics.
   - Release `SN_COMMIT` / `SN_DIGEST`.
   - It reads `HG_TAG`, `PROTO_TAG` and `CORE_TAG` from S3-A's Appendix C with `s3b-ledger.py` (S3-B contract §15).
2. **Tasks 13–14, production P0 PR.**
3. **Task 15, the P0 live rollout.**
   - Before it starts, make sure the runbook's local inputs exist on the machine that runs it. S3-B uses `R="$HOME/si-devnet2-multisource-s3a-rehearsal"` (read only) and the S2-B evidence directories. S3-A rebuilt these on another machine, so copy them over or rebuild them. Runbook §1 stops if they are missing.
4. **Tasks 16–18, lockdown.**
5. **Tasks 19–21, indexer-c.** The enrolment in Task 21 is irreversible.

Method: superpowers:subagent-driven-development, one task at a time. Every push, PR, merge, release, sync and consensus update needs the user's confirmation in chat. Commands handed to the user are fish.

### Track B: finish Plans S3-C1 … S3-F

1. **Draft the ten missing tasks.** The deliverables come from each plan's task list:
   - **78 (arbiter):**
     - the golden file `fsm/testdata/pre_stage3_golden.json` and its test;
     - fake-cluster activation tests;
     - the chpipeline stage-3 harness: three SNodes, members and the pool, every switch in order;
     - CI job `integration-clickhouse-stage3`;
     - offline replay of the devnet2 logs;
     - release `ARBITER_TAG_P5` / `ARBITER_DIGEST_P5`;
     - `$R5` rebuilt (D37).
   - **79 (sentio-node; widened scope, D30):**
     - pins `HG_TAG_P5`, `CORE_TAG_P5` and `PROTO_TAG_P5`;
     - `member-consent-statement`;
     - inert consensus key files;
     - the reconciler role `si-member --role reconciler`;
     - member consensus metrics and `keeper_findings`;
     - `replica_hosts` from `DatabaseGroup` with the served `host_indexer_id` (D27);
     - release `SN_COMMIT_P5` / `SN_DIGEST_P5`.
   - **82 (devnet2):** the P5 live rollout, ending with the irreversible v21 activation.
   - **83 (production):** member consensus values; the `si_member_ddl` user with its const-merge profile; `make-member-keys.sh`; the exit alert. All inert on merge.
   - **84 (production):** the P6 runbook `docs/storage-integrity-devnet2-stage3-p6-switches.md` and the P6 PR.
   - **85–89 (devnet2):** the P6 live steps 1–3, 4–5, the Keeper gate with step 6, 7–9, and step 10 with the 7-day exit.
2. **Adversarially review and fix all 68 tasks**, one reviewer per task (§3.3).
3. **Run one consistency pass per plan** (§3.4). It writes the final Appendices B and C and removes the status banner.
4. **Run the cross-plan pass** (§3.5).
5. **Open one follow-up PR per finished plan**, or one for all of them. Update `2026-10-15-multi-source-si-s3cf-status.md` as plans finish.

Execution of Plan S3-C1 starts only after S3-C1 has passed steps 2–4. Its own task order (S3-C1 front matter) then applies:
- Tasks 22–24 need only S3-A's releases and may run alongside Track A.
- Task 25 waits for S3-B's `SN_COMMIT`.
- Task 26 waits for `E_PR` merged and for `E_END` and `INDEXER_C_ID` to be recorded.

## 3. How to do Track B

### 3.1 Setup

- Merge the stack (§1.4) first, then work from `main`.
- If a plan must change before the stack has merged, commit the fix on the branch of the PR that holds that plan, then merge it forward into every later branch of the stack. The worktree `~/src/remotesrc/claude/housegate/ms-si-s3cf-split` holds the stack's branches.
- Once the stack has merged, create a fresh URWT worktree from `main`:

  ```sh
  bash ~/.agents/skills/urwt/scripts/urwt-create.sh /Users/uranuswch/Dev/projects/sentio-network/housegate claude ms-si-stage3-plans-finish
  ```
- Work in scratch directories outside the repositories (the session's scratchpad). Export bases with `git -C <repo> archive origin/main`.
- Never edit the real clones or other worktrees, never fetch or push from an agent, and never run anything against the cluster.

### 3.2 Inputs every agent reads first

- The plan file it works on: front matter, Global Constraints, clarifications, ledger, task list, Appendix A (binding names, verbatim) and its other tasks.
- `2026-10-15-multi-source-si-s3cf-status.md` §4.1, which holds the cross-plan conventions and decisions K38, K51 and D25–D40.
- `2026-10-15-multi-source-si-s3cf-names-index.md`: produced and consumed names per plan.
- `2026-10-15-multi-source-si-s3cf-scoping.md` §1, the plan's section.
- The spec, read in full.
- Plans S3-A and S3-B. Their tasks are the style reference: task format, TDD steps, release steps, runbook tasks with complete runbook text, live tasks with exact commit diffs, confirmations and stop conditions.

### 3.3 Instructions per stage

**Draft a task.** Use the writing-plans format of S3-A/S3-B:
- Start with `## Task N: <repo> — <title> (<plan>)`.
- **Files** (exact paths and anchors at `origin/main`) and **Interfaces** (Consumes and Produces, with exact names and types).
- `- [ ] **Step k:**` steps, each carrying complete code or text.
- Exact commands with their expected output.
- TDD order: failing test, run and see it fail, implement, run and see it pass, commit.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Every remote or live action is preceded by a line saying it needs explicit user confirmation in chat.

Runbooks:
- Bash blocks, with `Confirm before running.` before every mutating block.
- Every helper in the first bash block of §0.
- Live tasks run each block from a block file, never wrapped in a heredoc (D24).
- Commands handed to the user are fish.
- Passwords and keys only through the user's own terminal and Secrets, never printed and never on argv.

Then verify what you wrote by applying it in a scratch export and running what the repository runs:
- Go: `GOWORK=off GOFLAGS=-mod=mod GOPROXY=off go test`, stubbing upstream APIs that are not released yet from their contracts.
- production: `python3 -B charts/storage-integrity/tests/test_render.py` and `helm lint --strict`.
- Runbooks: `bash -n` on every block.

**Review and fix a task.** Find every defect and fix it in place. Classes to look for:
- stale anchors (re-read them at `origin/main`);
- code that would not compile;
- tests that would not fail or pass as stated;
- violations of the contract, the conventions or a decision;
- placeholders;
- cross-task and cross-plan name mismatches;
- safety-rule breaks;
- operational mistakes that would break devnet2.

Verify by applying this plan's earlier tasks in the same repository first, then this task's hunks verbatim. Run the chains end to end:
- arbiter-proto → arbiter-core → arbiter (S3-D, S3-E1 → S3-E4);
- housegate → arbiter-core → sentio-node (S3-C1, S3-C2, S3-C4, S3-E1/E4);
- the production chart chain: C1 Task 26 → C3 Tasks 37–38 → C4 Task 47 → D Task 53 → E4 Task 80 → F Task 83, with `test_render.py` green at every live commit.

### 3.4 Plan consistency pass

Per plan:
1. Resolve the reviewers' cross-task notes.
2. Check each of these:
   - every consumed name exists in its producing task;
   - every contract name is implemented by its owning task;
   - the Review Focus test names exist;
   - the task list, file structure and coverage map match the tasks;
   - every ledger row is recorded by exactly one task and read only from the ledger copy with `s3-ledger.py`;
   - every runbook section a live task cites exists;
   - `bash -n` passes on every runbook block.
3. Write the final Appendix B: drafting facts plus review facts.
4. Write Appendix C in the S3-B D23 shape: one row per produced ledger name, empty, and one run-record item per recording task.
5. Remove the status banner.

### 3.5 Cross-plan pass

- Every name a task consumes from another plan exists with that spelling and shape in the producing plan's tasks, not only in its contract.
- Release order and dependencies agree.
- Ledger names are unique across all twelve plans.
- Clarification ranges do not overlap.
- Never-edit rules hold (D25).
- The activation model holds: no consensus change before P5 except D26.
- Runbook acceptance values are disjoint across every runbook.

### 3.6 Throughput

The 16-wide multi-agent run hit the account's usage limits again and again. Each drafting or review agent spends about one to two million tokens on verification.

- Run at most four to six agents at a time.
- Do one plan's reviews at a time, in dependency order: C1, C2, C3, C4, D, E1, E2, E3, E4, F.
- Keep each agent's output in a file, so an interrupted run can resume without redoing finished work.

### 3.7 Assembly and delivery

- Each plan file holds, in order: front matter, `---`, its tasks in task order separated by `---`, `## Appendix A`, `## Appendix B`, `## Appendix C`.
- Insert newly drafted tasks at their position. Replace Appendices B and C in the consistency pass.
- Before committing, check:
  - every fence is closed;
  - there is no placeholder;
  - no scratch path is left;
  - `git diff --check` passes (trailing spaces inside diff hunks of blank context lines are expected).
- Commit messages end with the Co-Authored-By line, and PR bodies end with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. The housegate pre-commit hook runs gitleaks.
- After pushing, the user may turn on Auto-fix for the PR.

## 4. Context and pitfalls

- **Ledger.**
  - S3-A and S3-B read and write their ledger on `origin/main` (S3-B D23, `s3b-ledger.py`).
  - The new plans use `s3-ledger.py`, which S3-C1 contract §19 defines and Task 22 writes. `s3-ledger.py` takes as ledger copy the first of its `REFS` (`origin/main`, `origin/urwt/claude/ms-si-stage3-plans`, `origin/urwt/claude/ms-si-stage3-plans-cf`) that holds the plan file (D34).
    - Before the stack PR holding a plan merges, that is `origin/urwt/claude/ms-si-stage3-plans-cf`, the branch of the closed housegate#243. Keep that branch on origin until the whole stack has merged.
    - After that, it is `origin/main`.
    - "The stage-3 plans PR" in the plans means the stack PR that holds the plan.
    - Do not execute a plan before its PR has merged.
  - Record values only through the procedure. Never predict a value.
- **Runbooks.**
  - Do not wrap a block that contains heredocs in another heredoc: run blocks from files (D24).
  - The promotion budget estimator reads size and sequence from one finished, CRC-checked snapshot (S3-B §13.4). Never pair `arbiter_state_bytes` with the live sequence.
  - Never run helmfile against `k8s-sea/clickhouse` (S3-B K22).
- **Production rule.** Use a PR for anything that widens the blast radius: shared charts, the Gateway, new services. Push straight to `main` only for convergent single-service value or pin changes.
- **User decisions still open:**
  - P1 backup-key delete (K24) and list confinement (K25);
  - P6 Keeper thresholds (K86) and the promotion budget for the 7-day soak;
  - see `2026-10-15-multi-source-si-s3cf-status.md` §3.
- **Security, outside the plans.** Commented YAML in production `k8s-sea/clickhouse/clickhouse-extra.yaml` and `k8s-sea/clickhouse/clickhouse.yaml` holds a GCP service-account private key and a plaintext ClickHouse password. Rotate both and purge the history. P1 never reuses that service account, and no plan copies those blocks.
- **Irreversible steps.**
  - S3-B Task 21: indexer-c's on-chain registration at first start, then the enrolment.
  - S3-E4 Task 82: the v21 activation.
  - The one-way switches of S3-F Tasks 85–89.

## 5. File map

| File | Role |
|---|---|
| `docs/superpowers/specs/2026-10-14-multi-source-si-stage3-database-groups-design.md` (+ `.zh-CN.md`) | the stage-3 spec |
| `docs/superpowers/plans/2026-10-14-multi-source-si-s3a-p0-guards.md` | Plan S3-A, executed (Appendix C filled) |
| `docs/superpowers/plans/2026-10-14-multi-source-si-s3b-p0-hosts-rollout.md` | Plan S3-B, next to execute |
| `docs/superpowers/plans/2026-10-15-multi-source-si-s3c1-…` to `…-s3f-p6-switch-on.md` | Plans S3-C1 … S3-F, drafts |
| `docs/superpowers/plans/2026-10-15-multi-source-si-s3cf-status.md` | detailed status, conventions and decisions D25–D40, open questions |
| `docs/superpowers/plans/2026-10-15-multi-source-si-s3cf-scoping.md` | scoping proposal (per-plan tasks, ledgers, clarifications) |
| `docs/superpowers/plans/2026-10-15-multi-source-si-s3cf-names-index.md` | cross-plan names index |
| `docs/superpowers/plans/2026-10-16-multi-source-si-stage3-handoff.md` | this file |
