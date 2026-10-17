# Multi-source SI network — Plan S3-F: stage-3 P6 switch-on in the order of §10.4, members in consensus and k = 2 on devnet2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Status (2026-10-17): draft.** Task 83 is drafted (2026-10-17; drafting facts at the end of Appendix B; contract §59 updated with it, including the Cursor Bugbot grant-probe order fix) and was verified by its drafter in scratch exports; Tasks 84, 85, 86, 87, 88, 89 are not drafted yet; no task has had its adversarial review yet; the plan consistency pass (which also writes Appendices B and C in final form) and the cross-plan pass have not run. Do not execute this plan before those passes; `2026-10-15-multi-source-si-s3cf-status.md` lists what remains and how to finish it.

**Goal:** Turn stage 3 on, on devnet2, in the order of spec §10.4, starting from the state Plan S3-E4's v21 activation left (`stage3` committed at `ACTIVATION_EPOCH` with every switch off): switch 1 `purge`, switch 2 `eviction_voiding` and switch 3 `owner_aware_selection` (every SI host checked on `SN_COMMIT_P5` first); step 4, the three member identities of `MEMBER_IDS` listed in `verifiers` with their keys, consents and the failure domains of `DEVNET2_FAILURE_DOMAINS`, then switched to consensus mode, registered and marked active one host at a time, entering no ring; switch 5 `placement` after a placement preview, every group verified and the cutover block recorded; the Keeper headroom gate on the shared `clickhouse-keeper-extra`; step 6, the second member user `si_member_ddl` one installation at a time and each member's `hg_member_unsafe` reconciler one host at a time, until every member is ready in every group it belongs to; switches 7 `reselection`, 8 `checkpoints` and 9 `narrowed_drain`; and step 10, `min_safe_copies` 2 after its precondition, the P4 anti-affinity proof and a Keeper re-check, held for 7 days with a bounded held-cleanup age and `safe_copies` ≥ 2 on distinct nodes. Each step is one authority-signed update, or one non-consensus rollout, confirmed in chat. No code is written or released: every behaviour this plan turns on shipped, inert, in Plans S3-E1 to S3-E4 (K66), the member side included (Plan S3-E4 Task 79 carries the consent command, the member consensus mode, the reconciler role, the member consensus metrics and the keeper-findings export, decision D30); production adds values, the second member user, two user scripts, one alert and the P6 runbook.

**Architecture:** Task 83 (production) adds the inert pieces the live steps flip: in `charts/sentio-node`, `siMember.consensus` (the member's ed25519 seed and `replica_signer` key from one Secret into the `si-member` container only, its consent file, the consensus-mode keys of `SN_COMMIT_P5`) and `siMember.reconciler.passwordSecret` (the `si_member_ddl` password into the `si-member-reconciler` container only); in `charts/storage-integrity`, the `si_member_ddl` user with exactly its three grants and a profile that makes `max_bytes_to_merge_at_max_space_in_pool` const (`clickhouse.access.memberDDL`, off), its grant-probe rows, and the exit alert `ArbiterFloorHeldCleanupAgeAboveExitBound` (`monitoring.stage3.exitAlert`, off); the user's scripts `make-member-keys.sh` (new; the `replica_signer` key from Foundry `cast wallet new` or from a key file the user made with an equivalent tool, decision D40) and `make-clickhouse-logins.sh` (gains `si_member_ddl`); render tests that keep every member value out of the host checksum and every secret in its one container. Task 84 writes the P6 runbook `docs/storage-integrity-devnet2-stage3-p6-switches.md` (§0–§16: one section per step with its prerequisites, the update under its hold, the verification reads and the acceptance writes; the Keeper gate; the exit soak; a forward-fix playbook per one-way change; the spec §16 operator playbook) with the source of its throwaway read tool `stage3reads`, and opens the P6 PR (`P6_PR`). Task 85 runs steps 1–3, Task 86 steps 4 and 5, Task 87 the Keeper gate and step 6, Task 88 steps 7–9, Task 89 step 10, the 7-day exit soak and the record PR; each records its ledger values with Plan S3-C1's `s3-ledger.py`.

**Tech Stack:** Helm charts with `test_render.py` (Python 3, PyYAML), `helm lint --strict`, helmfile and `render-inertness.sh` (production); ClickHouse 26.3 (`26.3-lts-decimal512-release-2026-09-16`) under the Altinity operator 0.27.2 (frozen, S3-B K22); ClickHouse Keeper `clickhouse/clickhouse-keeper:26.3.12.3` (`clickhouse-keeper-extra`, read with `clickhouse-keeper-client`); VictoriaMetrics VMRule; `arbiter-admin` of `ARBITER_TAG_P5`, run inside the leader's arbiter container; sentio-node `SN_COMMIT_P5` (`si-member`, `storage-integrity member-consent-statement`); in the user's terminal (member keys, D40), an OpenSSL 3 or later with the `KECCAK-256` digest (Homebrew `openssl` 4.0.3 or `openssl@3` 3.6.4; with secp256k1 it derives and checks every `replica_signer` address) and Foundry `cast wallet new` or an equivalent secp256k1 key generator the user prefers; Go 1.26 for the throwaway `stage3reads` tool built from `CORE_TAG_P5`, never committed.

**Spec:** housegate `docs/superpowers/specs/2026-10-14-multi-source-si-stage3-database-groups-design.md` (merged into housegate `main` as housegate#236, `fb83a97`; Plans S3-A and S3-B followed as housegate#237, `838de2d`; this plan's branch `urwt/claude/ms-si-stage3-plans-cf` is aligned with `main` (decision D34) and amends spec §8.2 with the explicit `hg_rstage` grants of `si_member`, decision D36, which leaves `si_member_ddl` unchanged) — binding: §10.4 (steps 1–10, their order and the confirmation of each), §16 row P6 and its operator playbook, and, for what each step turns on, §6.1 (member entries, consent, weight, failure domains), §6.2–§6.4 (group records, placement and backfill, `RebalanceGroup`, `MemberReady`, Leaving), §7.1–§7.4 (seal key and header `group_key`, grouping window, eligible rings and `VERIFIERS_UNAVAILABLE`, re-selection), §8.2 (`si_member_ddl`, the `<replicated_merge_tree>` merge default, `unsafe_inc`), §8.3, §8.8, §9.1–§9.3 (checkpoints, classes, the floor, raising k, failure independence), §10.1 (narrowed drain gate and issuance hold), §11 (`replica_hosts` from placement on), §12 (no third-party member), §18 (risks); as clarified by Plans S3-A and S3-B (K1–K22), Plans S3-C1 to S3-E4 (K23–K82) and K83–K87 below. **Prerequisites:** Plans S3-A, S3-B, S3-C1, S3-C2, S3-C3, S3-C4, S3-D, S3-E1, S3-E2, S3-E3 and S3-E4, executed through `P5_END`: their Global Constraints, clarifications and Appendix A (contract §0–§57) apply here unchanged. This plan's Appendix A continues the numbering (§58–§61); §58 names every earlier name this plan consumes with the contract that owns it, and Tasks 85–89 consume the ledger values of Plans S3-B, S3-C3, S3-D, S3-E1 and S3-E4 verbatim from the ledger copy.

**Base revisions.** Line anchors refer to production `75514e3de` (its SI paths byte-identical to `9d3d0326b`, Plan S3-B's base: `git diff --stat 9d3d0326b 75514e3de -- charts/storage-integrity charts/sentio-node k8s-sea/sentio-network-devnet2 k8s-sea/clickhouse terraform docs scripts/storage-integrity` prints nothing) and, for what the runbook calls, arbiter `fb42260` (v0.12.0), arbiter-core `8f0bc43` (v0.14.1), arbiter-proto `1c6ae67` (v0.11.0), housegate `66d4df7` (v0.19.0), sentio-node `5d2f169`. None of Plans S3-A to S3-E4 is executed at drafting, so every production hunk of this plan applies on top of their production edits (the hunk rule below), and every code behaviour it relies on is named by the contract section that ships it (§58), not by a line at these bases. devnet2 when Task 85 starts, as the earlier plans' exit records leave it (every value read from the ledger copy at execution, never predicted):
- **Consensus.** `stage3` committed at `ACTIVATION_EPOCH` with all seven switches off and the parameter values of Plan S3-E4's activation record (`group_size` 3, `network_fill_default` true, `min_safe_copies` 1, `strict_copies` false, `indexer_failure_domains` equal to `DEVNET2_FAILURE_DOMAINS`, `exclude_quarantined_hosts` false, and its `max_lag_publications`, `lag_low`, `evidence_timeout_l2_blocks` and `max_uncleaned_parts_*` values); every voter writes v21; tracks enabled; `si_indexers` {0: `snode-1`, 1: `snode-2`, `INDEXER_C_ID`: `snode-3`}, `max_writers` 3; `verifiers` = `verifier-1`, `verifier-2`, `verifier-3` (the network pool, no host-bound entry); authority `0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705`; no track quarantined; the registry cursor advances every `cursorAdvanceInterval` 1m (production `charts/storage-integrity/values.yaml:156`, not overridden by the devnet2 overlay; arbiter `config/raw.go:180`).
- **Images.** The three voters and the three verifiers on `ARBITER_DIGEST_P5`; hosts indexer-a, indexer-b and indexer-c on `ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT_P5>@<SN_DIGEST_P5>` (HouseGate `HG_TAG_P5`, arbiter-core `CORE_TAG_P5` with `stage3_v1`); each si-member on the image Plan S3-E4's P5 record names.
- **ClickHouse.** Every SI installation locked (S3-B posture C) and restarted once in P2 with `table_engines_require_grant`, the `<replicated_merge_tree>` default `max_bytes_to_merge_at_max_space_in_pool` 0, `keep_free_space_bytes`, the member disk and policy and the `NOT OVERRIDABLE` collections (S3-C3 K39); engine grants by name; `si_member` on every SI installation; no `si_member_ddl` anywhere; `/sentio/0/unsafe_inc` under the Keeper root `/storage-integrity/devnet2/devnet2-si-v2` exists with no child (`UNSAFE_INC_ROOT`).
- **Members.** `MEMBER_IDS` names one member per host (S3-C2 K29: `member-a`, `member-b`, `member-c`), each its own si-member StatefulSet (S3-C2 K30), unregistered and off consensus, holding copies of the other two owners' databases at the latest published entry and serving them under P3's host flags (`P2_END`, `P3_END`); each member's status token lives in its own Secret `<release>-si-member-status-token`, apart from the Secret of its `si_member` password (decision D29, as Plans S3-C3 and S3-C4 mount it).
- **Network.** P4 complete (`P4_END`): interserver credentials at step 2, the SI CiliumNetworkPolicies and the Keeper ingress allowlist enforced (member pulls on 9000 and fetches on 9009 between SI ClickHouses admitted), node pins equal to `DEVNET2_FAILURE_DOMAINS`; the three source ClickHouses on pairwise distinct nodes and no verifier ClickHouse on a source's node; the three verifier ClickHouses on distinct nodes too, unless devnet2 lacks six pairwise-distinct nodes, in which case Plan S3-D let verifiers share a node with each other (decision D39, S3-D K55; its `DEVNET2_CLICKHOUSE_NODES` and `DEVNET2_NODE_PINS` on `origin/main` record which).
- **Shared Keeper.** `clickhouse-keeper-extra`: 3 replicas, memory request 1Gi and limit 4Gi, a 1Gi `local-path` volume each (not enforced), four-letter words allowed (production `k8s-sea/clickhouse/clickhouse-extra-keeper.yaml:21-26`, `:46-53`, `:89-98`), shared with production `clickhouse-extra`; every SI table has 4 replicas on it (its owner and the three pool verifiers).
- **Monitoring.** Plan S3-B's rules, Plan S3-C3's member rules and Plan S3-E4's stage-3 rules loaded, with delivery proven (S3-B K13).

## Global Constraints

- **Earlier plans apply.** Every Global Constraint and clarification of Plans S3-A to S3-E4 applies, and through them those of Plans S1-A, S1-B, S2-A and S2-B (strict decoder, signed claims, tracks, quarantine, golden files, passwords, worktrees, commits, remote actions); a line below replaces one only where it says so.
- **Decisions D25–D40 (cross-plan conventions, binding).** Those that touch this plan are applied in its text: D26 (`ConsensusAdmin.RebalanceGroup` is method 16 and `MarkMemberLagging` 17, contract §58.2), D27 (the registry view's `GroupMember.host_indexer_id`, read by `stage3reads -groups`, `si_safe_copies` and the replica-hosts check instead of a `hostOf` mapping), D29 (the member status token in its own Secret, never beside a member key or password), D30 (S3-E4 Task 79's widened scope, so this plan writes no code), D31 (`lag_advances` saturates at `max_lag_publications + 1`), D33 (only served read fields; contract §58.3), D34 (the ledger copy and the budget estimator on housegate `main`), D39 (the anti-affinity rule of `si_antiaffinity`) and D40 (member keys and the Keeper thresholds' agreement are user actions). D25, D28, D32, D35, D37 and D38 do not touch it: it edits no conformance file or S3-B text, sets no member disk budget, runs no mechanics test, and reads neither the P2 soak nor the P5 rehearsal directory; D36 amends only spec §8.2's `si_member` grants on `hg_rstage`, which this plan neither grants nor probes.
- **Activation model (contract §58.1).** No code is written and nothing is released: every behaviour turned on here shipped inert in `ARBITER_TAG_P5`, `CORE_TAG_P5`, `HG_TAG_P5` and `SN_COMMIT_P5` (K66, D30). Every consensus change is a `ConsensusParamsUpdate` of `stage3` or `verifiers` inside snapshot v21 (no Raft tag, field, snapshot version, header byte or feature string changes); the only image moved is `SN_DIGEST_P5` on the member pods.
- **Order (spec §10.4, contract §58.4).** Steps 1 → 10 one at a time, each in its own window, each only after the previous step's verification and its ledger record (R4); `purge` and `owner_aware_selection` before any member entry (Apply refuses host-bound entries earlier, S3-E2 K68); the Keeper headroom gate before step 6; step 10 only after `P4_END`; members (step 4) and reconcilers (step 6) one host at a time, indexer-b, then indexer-c, then indexer-a.
- **Irreversible steps.** Switches 1, 2, 3, 5, 7, 8 and 9 and `min_safe_copies` 2 never go back; member node ids are permanent and a recorded `MemberReady` stays recorded; from the placement cutover every sealed header carries `group_key`. Each update is preceded in the runbook by `Stop here and obtain explicit confirmation.` and runs only on the user's confirmation in chat for that step; there is no rollback, only the forward fixes of runbook §15.
- **Holds (K83).** The updates of steps 1–9 run under `--hold drain`; the step-10 update and every later update under `--hold issuance`; `arbiter-admin group rebalance` and `member lagging` take no hold.
- **Self-operated members only (spec §12, S3-E2 K68).** Every member entry is hosted by one of devnet2's three self-operated SI indexers; listing a third party's member is refused by this plan's operating rule, since the FSM cannot check it without `operator_id`.
- **One container per secret (S3-C2 K30, D29).** A member's ed25519 seed and `replica_signer` key reach only its `si-member` container, `si_member_ddl`'s password only its `si-member-reconciler` container, and no member value enters a host pod or its `checksum/config` (production `charts/sentio-node/templates/node.yaml:486`), so no member change restarts an SNode. The member status token keeps its own Secret `<release>-si-member-status-token` (D29), which this plan neither mounts nor changes; neither new Secret may be that Secret or the `si_member` password Secret (contract §59.1 guards).
- **Passwords and keys (S3-B K16, D40).** `si_member_ddl` passwords and member keys are generated and stored only by the user's scripts in the user's own terminal (`make-clickhouse-logins.sh`, `make-member-keys.sh`; the `replica_signer` key from Foundry `cast wallet new` inside the script, or from a key file the user made with an equivalent tool and passes to it by path, never on argv); git and the CHIs carry only SHA-256 hashes and public values (node ids, ed25519 public keys, `replica_signer` addresses, consent JWS); agents never read a Secret's data except by piping it straight into `kubectl exec -i` stdin (S3-B `si_ch_admin`), never print, echo, log or pass a secret on argv, and never show one in chat.
- **production rule.** Tasks 83 and 84 go through one PR (`P6_PR`), inert on merge (`render-inertness.sh <base-ref>`: every `charts/sentio-node` release and `storage-integrity-si-v2` render byte-identically); each live flip is a single-service commit pushed to `main` after the PR merged (one host overlay, one CHI manifest or the network overlay per commit, never batched), with its `test_render.py` constant in the same commit; CHI manifests are applied by hand (`kubectl apply --dry-run=server` first); the closing record PR (Task 89) is docs only; nothing under `k8s-sea/clickhouse` changes and no helmfile runs there (S3-B K22).
- **Runbook conventions (S3-B contract §15, D20, D24).** bash blocks run in one session (`set -eo pipefail`) from the `-live` checkout, a live task's agent from block files after sourcing `helpers.sh` (the first bash block of §0 along its chain); `Confirm before running.` before every mutating block; `si_backup="$HOME/si-devnet2-stage3-p6"` (mode 700, created fresh); read-only checks in separate re-runnable blocks that `tee` into `$si_backup`; writes are never re-run; one stage per window; commands handed to the user are fish; `yq` is not used.
- **Promotion budget (S3-B runbook §13.4, as fixed in housegate#237).** Runbook §1.4 computes the budget to L/4 with that estimator (the text on housegate `main` since housegate#237, `838de2d`, which this plan cites (D34); it is byte-identical to the plans-branch commit `9003d72` that first fixed it, which is not an ancestor of `main`; run through S3-C3's `si_budget_capture` and `si_budget`: the leader's newest finished snapshot with `state.bin` matching `meta.json`'s Size and CRC, its state length and `promotion_seq` read from the container, the live sequence read afterwards, S = a·n² fitted through it, minus every promotion made since; `arbiter_state_bytes` never paired with the live sequence); this plan's 18 acceptance writes plus the 7-day soak's projected traffic spend at most half of it, or the plan stops before step 1.
- **User actions (D40).** Member keys (made in the user's terminal with Foundry `cast wallet new` or an equivalent the user prefers), `si_member_ddl` logins, the agreement of the Keeper thresholds (K86) with the owner of `clickhouse-extra`, and every confirmation are the user's; runbooks only read their results.
- **Ledger (contract §61, D34).** Each recording task writes its values and its run record into this plan's Appendix C with `$HOME/.cache/s3-ledger/s3-ledger.py` (Plan S3-C1 contract §19) by S3-B's record procedure R1–R4 (D23); the ledger copy is the one `s3-ledger.py ref` names (the plans-cf branch until the stage-3 plans PR merges, then `main`); every reader takes values only from the ledger copy, never from a local checkout, a pending PR or memory.
- **Hunks are anchored on the base revisions.** Plans S3-B (Tasks 13, 16, 19), S3-C1 (Task 26), S3-C3 (Tasks 37–38), S3-C4 (Task 47), S3-D (Task 53), S3-E4 (Task 80) and this plan's Task 83 all edit `test_render.py`, `charts/sentio-node`, `charts/storage-integrity` and the devnet2 overlays; apply the later intent on top of the earlier change. The contract and the named tests are binding; line anchors are guidance.
- **References.** `contract §N` points to Appendix A of Plan S3-A for §0–§6, of Plan S3-B for §7–§15, of the plan owning that range for §16–§57 (S3-C1 §16–§19, S3-C2 §20–§24, S3-C3 §25–§28, S3-C4 §29–§32, S3-D §33–§37, S3-E1 §38–§43, S3-E2 §44–§48, S3-E3 §49–§52, S3-E4 §53–§57) and of this plan for §58–§61; a task group's "Facts" point to Appendix B; Appendix C records the ledger values and the run records as they are made.

## Plan clarifications (binding)

The spec and Plans S3-A to S3-E4 leave these P6 details open; each is resolved here and binding for Plan S3-F.

- **K83 — Holds per step (spec §10.1, §10.2; S3-A K6; S3-E4 K79).** The spec gives every update an issuance hold (§10.1) and keeps leader drain mode "for every update before the `narrowed_drain` switch" (§10.2); the scoping proposal put drain through step 8 and issuance from step 9. Resolution: the updates of steps 1–9 pass `--hold drain` explicitly, step 9's included, because the update that turns `narrowed_drain` on is still judged by the stage-2 drain gate, which only a drain reaches without waiting forever on a half-promoted multi-partition block (K6); once `narrowed_drain` is committed, the step-10 update and every later update pass `--hold issuance` (the default of `ARBITER_TAG_P5`'s `arbiter-admin` once the leader reports the switch, S3-E4 Task 77), because a `stage3`-only update touches no track and a drain would stop issuance on every track for nothing. `arbiter-admin group rebalance` and `member lagging` send authority commands outside `ConsensusParamsUpdate` with no `expected_promotion_seq` and take no hold. Why: each update takes the one hold that becomes ready soonest without weakening the gate Apply runs.
- **K84 — Every SI host on `SN_COMMIT_P5` before switch 3 (spec §7.3, §10.4 step 3).** `VERIFIERS_UNAVAILABLE` (code 12) is a retryable refusal with state unchanged, but a HouseGate older than `HG_TAG_P5` maps an unknown admission code to `OutcomeUnknown` (housegate `pkg/storageintegrity/arbiter_proto.go:426-427` at `66d4df7`) and sends the statement into status probing; the stage3 gate checks data-plane registrations, never the hosts' HouseGate. Resolution: runbook §4.1 refuses the step-3 update unless the `sentio-node` container of every SI host pod (`sentio-node-devnet2-indexer-a-0`, `-b-0`, `-c-0`) runs `ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT_P5>@<SN_DIGEST_P5>` in its pod spec and its running `imageID` carries `<SN_DIGEST_P5>`; the agent sidecars need nothing (the host answers the code). Why: code 12 can be returned only from switch 3 on, and the check costs one read per host.
- **K85 — Member consents are signed inside the host pods (spec §6.1).** The consent is an ES256K JWS by the host's `si_indexers` signer, which is the indexer's on-chain key `PRIVATE_KEY` held only by sentio-node (sentio-node `standalone/storage_integrity_enrollment.go:36-47`); the spec does not say where it is signed. Resolution: runbook §5.2 runs `sentio-node storage-integrity member-consent-statement` (S3-E4 Task 79) in each host's `sentio-node` container for the member that host runs, with the member's node id, ed25519 public key and `replica_signer` address from the user's public file; stdout carries only the JWS, which `si_jws` decodes (purpose `arbiter-group-member-v1` and the six fields `{network_id, genesis_snapshot_id, host_indexer_id, node_id, ed25519_pubkey, replica_signer}`) and `arbiter-admin` verifies before it submits. The member keys themselves are made in the user's terminal by the user's `make-member-keys.sh` (D40: the `replica_signer` key from Foundry `cast wallet new`, or from a key file the user made with an equivalent tool; the script derives and checks its address with OpenSSL either way), never by a host or an agent. Why: the indexer key never leaves its Secret, exactly as the enrolment statements were made (production `docs/storage-integrity-devnet2-multi-source.md` §5.2 and §6.5).
- **K86 — Keeper headroom thresholds and abort criteria (spec §16 P6, §18).** The spec asks that "the measured znode count, memory and request latency of `clickhouse-keeper-extra` at the projected replica count (owner, pool and members per table) leave headroom against its limits" and gives no numbers. Resolution: runbook §7 samples every Keeper replica (`mntr` through `clickhouse-keeper-client`'s `flwc`, `get_all_children_number` of the SI Keeper root, the container's memory working set, `du` of `/var/lib/clickhouse-keeper`, `df` of the node disk under it) at least 6 times over at least 24 h with a sample in every 4-hour window, and projects step 6 with R0 = 4 and R1 = 6 replicas per SI table: every SI znode scaled by R1/R0, every other measure scaled by the projected over the measured znode count. The gate passes only if on every replica the projected working set is ≤ 2 GiB (50 % of the 4Gi limit), the projected `zk_approximate_data_size` ≤ 1 GiB, the projected `du` ≤ 1 GiB (the nominal size of the unenforced `local-path` volume) and ≤ half the node disk's free bytes, the maximum `zk_avg_latency` ≤ 10 ms and the maximum `zk_outstanding_requests` ≤ 10; the user confirms these numbers with the owner of `clickhouse-extra` before the verdict counts (a user action, D40; the runbook records the user's confirmation and never contacts the owner). During step 6 the runbook samples after each host's reconciler and every 10 minutes for an hour, and aborts the step (no further host; runbook §15.4) on any replica whose `zk_avg_latency` exceeds max(10 ms, twice the gate's maximum), whose `zk_outstanding_requests` exceeds 10 in two consecutive samples or whose working set exceeds 2 GiB, on a Keeper leader change, or on a Keeper pod restart; nothing is removed automatically. The same thresholds on fresh samples gate step 10 (§12.3) and are re-read daily in the exit soak. Why: Keeper keeps its whole tree in memory and the volume's size is not enforced, so memory and the node disk are the binding limits; scaling every SI znode by R1/R0 over-counts table-level znodes and errs safe; the ensemble also serves production `clickhouse-extra`, whose owner must accept the margin.
- **K87 — The exit measures (spec §9.3 failure independence, §16 P6 exit).** "Bounded held-cleanup age" and "`safe_copies` ≥ 2 on distinct nodes" have no definitions. Resolution: (a) the held-cleanup age is S3-E4's leader-local gauge `arbiter_floor_held_cleanup_age_seconds` (K78); it is bounded when `max(max_over_time(arbiter_floor_held_cleanup_age_seconds{network="devnet2-si-v2"}[1d]))` stays at or below `1800` on each of the 7 days, alerted by `ArbiterFloorHeldCleanupAgeAboveExitBound`; a breach restarts the 7 days once explained, unless the user decides otherwise. (b) `safe_copies` of an Active incarnation is 1 (its owner's `hg_safe`) plus the number of distinct `node:` failure domains other than the owner's among the counting members of its group whose class is CaughtUp at one of two reads 60 s apart and whose checkpoint excludes none of its tables, each domain checked against the node its member's host's source ClickHouse pod actually runs on (the host taken from the registry view's `GroupMember.host_indexer_id`, D27, the domain from that member's `verifiers` entry); `si_safe_copies` computes it from `GetTableRegistry` and the live pods, never from a gauge, and the exit needs ≥ 2 for every Active incarnation at every daily check. (c) The anti-affinity is re-checked with `si_antiaffinity` at step 10 and on day 7, under D39's rule: the three source ClickHouses on pairwise distinct nodes, each on the node of its `DEVNET2_FAILURE_DOMAINS` domain, no verifier ClickHouse on a source's node, and the verifiers on pairwise distinct nodes unless S3-D's constants on `origin/main` record that they share one; only the source nodes enter `safe_copies`, since every copy that counts lives on a source ClickHouse. Why: failure domains are declared `node:<hostname>` values (S3-D K55), so a count of domains is a count of Kubernetes nodes only once the pods are checked against them; with two members per group and k − 1 = 1, one member down holds no cleanup, so the age measures publication to the first member checkpoint, normally seconds, and 30 minutes covers two consecutive member restarts with their startup audits.

## Release ledger (consumed and produced)

| Name | Meaning | Resolved by |
|---|---|---|
| `INDEXER_C_ID` | indexer-c's contract-assigned SI indexer id | Plan S3-B Task 21, Appendix C |
| `MEMBER_IDS` | the member node id of each host (`member-a`, `member-b`, `member-c`, S3-C2 K29) | Plan S3-C3, Appendix C |
| `UNSAFE_INC_ROOT` | the created, empty `/sentio/0/unsafe_inc` root (S3-C3 K42) | Plan S3-C3, Appendix C |
| `P2_END` | the end of P2 on devnet2 (members running off consensus) | Plan S3-C3, Appendix C |
| `DEVNET2_FAILURE_DOMAINS` | each SI indexer's source-ClickHouse node, `node:<hostname>` (S3-D K55) | Plan S3-D, Appendix C |
| `P4_END` | the end of P4 on devnet2 (hardening and pins done; needed by step 10) | Plan S3-D, Appendix C |
| `HG_TAG_P5`, `CORE_TAG_P5` | the P5 housegate and arbiter-core releases (`stage3_v1`, member consensus mode, scoped Keeper paths) | Plan S3-E1, Appendix C |
| `ARBITER_TAG_P5` / `ARBITER_DIGEST_P5` | the P5 arbiter release and its image index digest (every switch, `arbiter-admin`) | Plan S3-E4 Task 78, Appendix C |
| `SN_COMMIT_P5` / `SN_DIGEST_P5` | the P5 sentio-node commit and image digest (hosts and members) | Plan S3-E4 Task 79, Appendix C |
| `ACTIVATION_EPOCH`, `P5_END` | the v21 activation epoch and the end of P5 | Plan S3-E4 Task 82, Appendix C |
| `P6_PR` | the number of the merged production PR carrying Tasks 83 and 84 | Task 84 |
| `PURGE_EPOCH`, `VOIDING_EPOCH`, `SELECTION_EPOCH` | the epochs of switches 1, 2 and 3 | Task 85 |
| `MEMBERS_EPOCH` | the epoch of step 4's `verifiers` update | Task 86 |
| `PLACEMENT_EPOCH`, `PLACEMENT_CUTOVER_BLOCK` | the epoch of switch 5 and the cutover block it recorded | Task 86 |
| `KEEPER_HEADROOM` | the date the Keeper headroom gate passed (its record: baseline, projection, agreed thresholds, step-6 re-measures) | Task 87 |
| `STEP6_END` | the date every member was ready in every group it belongs to, with the Keeper re-measured | Task 87 |
| `RESELECTION_EPOCH`, `CHECKPOINTS_EPOCH`, `NARROWED_DRAIN_EPOCH` | the epochs of switches 7, 8 and 9 | Task 88 |
| `K2_EPOCH` | the epoch of `min_safe_copies` 2 | Task 89 |
| `P6_END` | the end date of P6 on devnet2 (its record: `Step 10 and the exit soak`) | Task 89 |

Every produced value is written into Appendix C by the task that makes it, with that task's run record, in one commit made by the record procedure of contract §61 (S3-B decision D23), and read afterwards only from the ledger copy with `s3-ledger.py get` (this plan's on the plans-cf branch until the stage-3 plans PR merges, then on `main`, D34); none is predicted. Appendix C's final shape (the produced rows above in order, then one run-record item per recording task) is fixed before execution. No release is cut in this plan.

## Review Focus

1. **A step out of order, or a member identity ahead of its switches.** Expected: Apply refuses a switch-on whose prerequisite switch is off, and a host-bound `verifiers` entry before `owner_aware_selection` (S3-E2 K68), with state unchanged; the runbook's prerequisite block stops before anything is signed; every switch update carries the complete committed `stage3` and changes exactly its field; the step-4 update restates `verifier-1..3` byte-identically from `consensus show`, because an update that omits a registered verifier evicts it (arbiter `fsm/signed_claims.go:277-299`), and no later planned update passes `--verifier`. Checks: Task 84 checker `switch-order`, `hold-modes`, `restates-verifiers`, `stop-before-irreversible`; runbook §2.1, §3.1, §4.1, §5.3, §6.2, §9.1, §10.1, §11.1, §12.4 with `diff-step<N>.txt` naming only the step's field; Plan S3-E2 Task 64's switch-prerequisite and host-bound-entry tests.
2. **A member secret, or any member value, in the wrong place.** Expected: the ed25519 seed and `replica_signer` key reach only the `si-member` container, the `si_member_ddl` password only `si-member-reconciler`; neither Secret is the `si_member` password Secret or the member status token Secret `<release>-si-member-status-token` (D29), which this plan leaves as S3-C3 and S3-C4 mount it; the `replica_signer` address in the public file is derived by the script with OpenSSL, whichever tool made the key (D40); no member value changes a host pod's `checksum/config`, so syncing a host release for a member change replaces only the member pod; `si_member_ddl` holds exactly its three grants, its profile refuses a merges-allowed replica with 452 and accepts `ddl.UnsafeSettings`' explicit 0; the CHIs carry only a hash; no block reads Secret data except into `kubectl exec -i` stdin. Tests: Task 83 `test_member_consensus_mounts_its_keys_only_into_the_member_container`, `test_member_reconciler_takes_its_password_only_from_its_secret`, `test_member_changes_never_touch_the_host_checksum`, `test_member_reconciler_needs_consensus_mode`, `test_member_consensus_guards_fail_closed`, `test_si_member_ddl_holds_exactly_its_grants`, `test_si_member_ddl_profile_makes_merges_const`, `test_si_member_ddl_carries_only_a_literal_hash`, `test_grant_probe_covers_si_member_ddl`, `test_devnet2_member_secrets_are_per_host`; Task 84 checker `no-secret-data-in-blocks`; `make-member-keys.sh`'s address self-check (§59.5); runbook §8.2 (`member-ddl-probe-<installation>.tsv`), §5.4 and §8.3 (host pod start time unchanged).
3. **The shared Keeper under the members' replicas.** Expected: the K86 gate passes on ≥ 24 h of samples, with the thresholds agreed by the user, before any `si_member_ddl` exists; reconcilers start one host at a time with a sample after each and every 10 minutes for an hour; after each host the owner-side replica set of every SI table is exactly its owner, the three pool verifiers and the members of its group whose reconciler already runs (after indexer-b 5 replicas for the tables of owners a and c and 4 for indexer-b's own, after indexer-c 6 for owner a's tables and 5 for those of b and c, after indexer-a 6 for every table), so the check never expects a member that is still off; an abort criterion stops the rollout at that host with nothing removed automatically, and the forward fix removes that host's member from its groups with `group rebalance` (Leaving; at k = 1 its release needs no other member's checkpoint, since min(k − 1, |E|) = 0 in spec §6.4, so it is recorded at the first evaluation and the member then drops its replicas); the same thresholds gate step 10 and hold through the soak. Checks: Task 84 checker `keeper-gate-first`; runbook §7.1–§7.3 (`keeper-baseline.tsv`, `keeper-projection.txt`, `keeper-thresholds-agreed.txt`, `keeper-verdict.txt`), §8.3 (`owner-replicas-after-<x>.tsv` from `si_unsafe_replicas` with the hosts done so far) and §8.5 (`keeper-after-<x>.tsv`, `si_keeper_abort`, and `keeper-findings-<x>.txt`: every host's `sentio_node_storage_integrity_keeper_findings{reason}` stays 0 for both reasons, S3-E4 Task 79's export, D30), §12.3, §13.1, §15.4.
4. **Groups and sealing at steps 5 and 6.** Expected: the placement preview equals the switch-on result: every (owner, database) with a physical incarnation, genesis-origin included, gets the other two hosts' members with `network_fill` forced on, and no Legacy or Refused database gets a group; headers carry `group_key` exactly after `PLACEMENT_CUTOVER_BLOCK`; before step 6 no member is ready, so every group's key is the same and each owner's blocks seal as before, and rings stay the pool; `replica_hosts` stay the other two hosts at step 5 and through step 6 (from placement on read from the groups: every current member's host from its served `host_indexer_id`, D27, whether or not it has sent `MemberReady`, which attests only the `hg_member_unsafe` replicas that reads never use, S3-E4 §56; a host serves a failed-over read only under its own manifest-gated conditions, spec §11); during step 6 `arbiter_block_seal_reason{reason="group_switch"}` rises only while an owner's groups differ in readiness and stops once all are ready, and each later block's eligible ring holds its group's ready members and the pool (ready verify members derived from `ready_generation` and the `verifiers` entries, since `ReadyVerifyMembers` is not served, D33). Checks: runbook §6.1, §6.3 (`placement-preview.json` against `groups-after-step5.json`, `cutover.txt`, `block-<seq>.json` on both sides of the cutover, `replica-hosts-step5.jsonl`), §8.4 (`ready-<x>.jsonl`, `rings-after-<x>.jsonl`, `seal-reasons.txt`, `replica-hosts-after-<x>.jsonl` equal to `replica-hosts-step5.jsonl`); Plan S3-E3 Tasks 71–73's placement, key and readiness tests; Plan S3-E4 Task 79's `TestReplicaHostsListEveryCurrentMemberWhateverItsReadiness`.
5. **The copy floor at k = 2 with a member down.** Expected: with two members per group and k − 1 = 1, one member down holds no cleanup; both members of a group down hold its cleanups until each is Lagging (once its `lag_advances` reaches `max_lag_publications + 1`, where it saturates, D31, or by `member lagging` per the playbook), `under_replicated` is raised and the pressure bypass keeps the owner writing (strict mode stays off); hosts and members restart strictly one at a time; the held-cleanup age stays ≤ 1800 s and `safe_copies` ≥ 2 on distinct nodes at every daily check, or the 7 days restart; the precondition preview passes before the step-10 update and Apply's own check stays authoritative. Tests and checks: Task 83 `test_p6_exit_alert_reads_the_held_cleanup_gauge`, `test_devnet2_p6_exit_alert_follows_the_rollout`; runbook §12.1 (`k2-precondition.jsonl`), §12.2 (`antiaffinity-step10.txt`, D39's rule: sources on pairwise distinct nodes, no verifier on a source's node), §13.1 (`soak-day<d>.txt`, `safe-copies-day<d>.jsonl`), §16.1, §16.2; Plan S3-E4 Task 76's floor, escape and k-raise tests.

## Task order and dependencies

Plans S3-A to S3-E4 executed through P5 (`P5_END`, `P4_END` and the other consumed names on the ledger copy) → 83 → 84 (`P6_PR`) → 85 (`PURGE_EPOCH`, `VOIDING_EPOCH`, `SELECTION_EPOCH`) → 86 (`MEMBERS_EPOCH`, `PLACEMENT_EPOCH`, `PLACEMENT_CUTOVER_BLOCK`) → 87 (`KEEPER_HEADROOM`, `STEP6_END`) → 88 (`RESELECTION_EPOCH`, `CHECKPOINTS_EPOCH`, `NARROWED_DRAIN_EPOCH`) → 89 (`K2_EPOCH`, `P6_END`). Tasks 83 and 84 share one branch, created from production `origin/main` once Plan S3-E4's P5 PR merged (its stage-3 rules and the P5 reviewed-image constant are then on `main`); they may be drafted in parallel and land in one PR, and both need the spellings of contract §58.3 settled by the owning plans' contracts. Task 85 starts after `P6_PR` merged. Task 87's §7 sampling starts once `PLACEMENT_EPOCH` is recorded and lasts at least 24 h. Task 89 needs `P4_END` and `NARROWED_DRAIN_EPOCH`. A task that reads a ledger value starts only once the recording task's record has passed its R4 check (contract §61). Each step's window ends with its verification; a failed verification stops the plan at that step (runbook §15).

## Task list

| Task | Repository | Deliverable |
|---|---|---|
| 83 | production | `charts/sentio-node` `siMember.consensus` (keys Secret into `si-member` only, consent file, consensus-mode keys) and `siMember.reconciler.passwordSecret` (`si_member_ddl`'s password into `si-member-reconciler` only); the `si_member_ddl` user, grants and const-merge profile (`clickhouse.access.memberDDL`, off); the grant-probe rows with `constrained`; `make-member-keys.sh` (new; Foundry `cast wallet new` or a key file from an equivalent tool, D40) and `make-clickhouse-logins.sh` with `si_member_ddl`; the exit alert `ArbiterFloorHeldCleanupAgeAboveExitBound` (off); render tests; inert on merge |
| 84 | production | runbook `docs/storage-integrity-devnet2-stage3-p6-switches.md` (§0–§16, the `stage3reads` source, the K86 and K87 helpers, the forward fixes and the operator playbook); the pointer row in the tracks runbook; the runbook checker; the P6 PR (`P6_PR`) |
| 85 | devnet2 | P6 live, steps 1–3: preparation and budget, `purge` with the scoped acceptance tables, `eviction_voiding`, the host check and `owner_aware_selection` (`PURGE_EPOCH`, `VOIDING_EPOCH`, `SELECTION_EPOCH`) |
| 86 | devnet2 | P6 live, steps 4–5: member keys (user), consents, the `verifiers` update, consensus mode one host at a time; placement preview, `placement`, groups, cutover and replica hosts verified (`MEMBERS_EPOCH`, `PLACEMENT_EPOCH`, `PLACEMENT_CUTOVER_BLOCK`) |
| 87 | devnet2 | P6 live, the Keeper headroom gate and step 6: `si_member_ddl` logins (user) and users one installation at a time, reconcilers one host at a time, `MemberReady`, eligible rings, the Keeper re-measure (`KEEPER_HEADROOM`, `STEP6_END`) |
| 88 | devnet2 | P6 live, steps 7–9: `reselection`, `checkpoints` with classes verified, `narrowed_drain` (`RESELECTION_EPOCH`, `CHECKPOINTS_EPOCH`, `NARROWED_DRAIN_EPOCH`) |
| 89 | devnet2 | P6 live, step 10 and the exit: precondition preview, anti-affinity proof, Keeper check, `min_safe_copies` 2, the exit alert, the 7-day soak, the acceptance tables purged, the record PR (`K2_EPOCH`, `P6_END`) |

## File Structure

Line anchors in the tasks refer to the base revisions above.

**production, P6 code** (Tasks 83–84)
- `charts/sentio-node/values.yaml` (`siMember.consensus`, `siMember.reconciler.passwordSecret`, commented), the `siMember` template Plan S3-C3 adds (expected `templates/si-member.yaml`), `templates/si-member.yaml` (values, define `sentio-node.siMember.consensus` and guards; `templates/_helpers.tpl` is not touched); `templates/node.yaml` is not touched.
- `charts/storage-integrity/values.yaml`, `values.schema.json`, `templates/clickhouse.yaml` (`si_member_ddl` user and profile), `templates/validate.yaml` (refusals), `templates/monitoring.yaml` (`<fullname>.stage3-p6`).
- `charts/storage-integrity/tests/test_render.py` (constants, `MemberConsensusTest`, the `ActualFreshNetworkTest` additions), `charts/storage-integrity/tests/clickhouse_grant_probe.tsv` (`si_member_ddl` rows, `constrained`).
- `scripts/storage-integrity/make-member-keys.sh` (new), `scripts/storage-integrity/make-clickhouse-logins.sh` (`si_member_ddl`).
- `docs/storage-integrity-devnet2-stage3-p6-switches.md` (new), `docs/storage-integrity-devnet2-multi-source-tracks.md` (pointer row).
- `.github/workflows/storage-integrity-chart.yml`: unchanged by Task 83 (its paths already hold `charts/**` and `scripts/storage-integrity/**`); Task 84 adds the P6 runbook path, whose heading the exit alert's test reads.

**devnet2, P6** (Tasks 85–89): single-service commits pushed to production `main` after `P6_PR` merged, each with its `test_render.py` constant: per host b, c, a, `k8s-sea/sentio-network-devnet2/sentio-node-indexer-<x>-storage-integrity-si-v2.yaml` for consensus mode (Task 86, `DEVNET2_MEMBER_CONSENSUS`) and for the reconciler (Task 87, `DEVNET2_MEMBER_RECONCILER`); per installation `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2-indexer-b-source.yaml`, `storage-integrity-si-v2-indexer-c-source.yaml` and `storage-integrity-si-v2.yaml` for `si_member_ddl` (Task 87, `DEVNET2_MEMBER_DDL_USERS`); `storage-integrity-si-v2.yaml` for `monitoring.stage3.exitAlert` (Task 89, `DEVNET2_P6_EXIT_ALERT`). The record PR (Task 89, docs only): `docs/storage-integrity-devnet2-stage3-p6-switches.md` §14, `docs/storage-integrity-devnet2-multi-source.md` §1.2. Evidence under `$HOME/si-devnet2-stage3-p6`; Secrets made only by the user's scripts.

**housegate** (Tasks 84–89): Appendix C of this plan file (`docs/superpowers/plans/2026-10-15-multi-source-si-s3f-p6-switch-on.md`) on the ledger copy (`origin/urwt/claude/ms-si-stage3-plans-cf` until the stage-3 plans PR merges, then `origin/main`, D34), one record commit per recording task from a URWT worktree `multi-source-s3f-ledger-<N>` (contract §61).

**arbiter-core** (Task 84, read only): `git archive` of `CORE_TAG_P5` for the throwaway `stage3reads` build; nothing is committed. No other repository changes.

## Spec coverage map

| Spec item | Tasks |
|---|---|
| §10.4 step 1 `purge` (§8.8 scoped Keeper paths, member-aware purge sets) | 84 (§2), 85; 89 (§14 purge of the acceptance tables) |
| §10.4 step 2 `eviction_voiding` (§14) | 84 (§3), 85 |
| §10.4 step 3 `owner_aware_selection` (§7.3), `VERIFIERS_UNAVAILABLE` at every host's ingress | 84 (§4, K84), 85 |
| §10.4 step 4: member entries with consents and failure domains (§6.1), registration, no ring | 83 (keys, consent, consensus mode, `make-member-keys.sh`), 84 (§5, K85), 86 |
| §10.4 step 5 `placement` and the seal key (§6.2, §6.3, §7.1, §7.2) | 84 (§6), 86 |
| §11 `replica_hosts` from `DatabaseGroup` once placement is on (every current member's host, never filtered by `MemberReady`, S3-E4 §56) | 84 (§6.3, §8.4), 86, 87 |
| §16 P6 Keeper capacity check before step 6 (and §18 shared-Keeper risk) | 84 (§7, §8.5, K86), 87 |
| §10.4 step 6: `si_member_ddl` only from here (§8.2), reconcilers (§8.3), `MemberReady` (§6.4) | 83 (`si_member_ddl`, reconciler Secret, probe rows), 84 (§8), 87 |
| §10.4 step 7 `reselection` (§7.4) | 84 (§9), 88 |
| §10.4 step 8 `checkpoints` (§9.1–§9.3, classes) | 84 (§10), 88 |
| §10.4 step 9 `narrowed_drain` (§10.1, issuance hold) | 84 (§11, §16.3, K83), 88 |
| §10.4 step 10, §9.3 raising k (precondition, P4 anti-affinity, Keeper check) | 84 (§12), 89 |
| §16 P6 exit (k = 2 for 7 days, bounded held-cleanup age, anti-affinity re-checked, `safe_copies` ≥ 2 on distinct nodes), §9.3 failure independence | 83 (exit alert), 84 (§13, K87), 89 |
| §16 operator playbook (down member, lagging member, Diverged, Foreign; restarts one at a time; confirmation of every live step) | 84 (§15, §16), 85–89 |
| §12 no third-party member (operating rule) | 84 (§16), 86 |
| §18 one-way switches, the shared Keeper | 84 (§7, §15), 85–89 |

## Follow-ups (not in this plan)

- A continuous scrape of `clickhouse-keeper-extra` (its port 7000 is not scraped and the P4 allowlist would have to admit the monitoring namespace) with alert rules; this plan measures it by hand (K86).
- Per-path Keeper ACLs or a dedicated SI Keeper ensemble (spec §14, §18): until then a leaked `si_member_ddl` credential is a residual, and `si_member_ddl` and `si_member` are installation-wide on the network CHI's verifier hosts (S3-C3 K43).
- `operator_id` and the other §12 prerequisites before any third-party member verifies or counts; until then the refusal is an operating rule.
- Retiring the network pool per group (P8) needs three ready peer verify members on distinct hosts, so at least four self-operated SI indexers; devnet2's groups keep `network_fill` on.
- Placement weights stay 1 on devnet2; the capacity unit and who sets it stay open (spec §19), as do `exclude_quarantined_hosts`' default and whether every verify member replicates.
- Strict mode (`strict_copies`) stays off on devnet2; its backpressure (code 13) is exercised only in integration (Plan S3-E4).
- Member key rotation (an ed25519 key by an entry update, a `replica_signer` rotation that restarts `checkpoint_seq`) has no drilled procedure.
- Whether the P2 shadow verifier keeps running as a canary after step 6 (Plan S3-C2).
- A voter resize if the state budget of runbook §1.4 does not cover the soak (S3-B K14 operating rule, irreversible per voter volume).
- Enumerated egress in the SI NetworkPolicies (S3-D K57).
- `unsafe_latest` answered from copies, the mutation-era publication cut (§8.9), P7/v22 (owner rebase and takeover, history pruning, O(1) acknowledgement records, the case (a) revert) and P8 (DA rematerialisation, per-operator transport).

---

## Task 83: production — member consensus mode and the reconciler's Secret, `si_member_ddl` with its const-merge profile and probe rows, the member-keys and logins scripts, the P6 exit alert, render tests (S3-F)

Contract §59.1–§59.6 (the production part of P6, binding) and the §58.3 rows it implements (member consensus-mode config, reconciler process, `siMember` values, single-user logins, test constants); spec §6.1 (member entries, keys, consent), §8.2 (`si_member_ddl`: `CREATE DATABASE` without `ENGINE`, `CREATE TABLE` and `DROP TABLE` on `hg_member_unsafe.*`, `TABLE ENGINE ON ReplicatedMergeTree`, nothing else; its profile's `merge_tree_` constraint; the residual), §8.3 (the reconciler creates replicas only while registered), §10.4 steps 4 and 6, §16 row P6 (the exit), §18; clarifications K85 (keys in the user's terminal, consent in the host pod) and K87 (a) (the bound 1800 s); S3-C2 K30 and D29 (one container per secret, the status token's own Secret), S3-C3 K39 (the `<replicated_merge_tree>` default 0 of P2's restart) and K43 (installation-wide users); decisions D30 (S3-E4 Task 79 ships the member side) and D40 (keys and logins are the user's). This task puts every chart, test, corpus and script change of P6 on branch `urwt/claude/multi-source-s3f` (production worktree `multi-source-s3f`); Task 84 adds the P6 runbook `docs/storage-integrity-devnet2-stage3-p6-switches.md` to the same branch and opens `P6_PR`. (1) **`charts/sentio-node`:** `siMember.consensus.{enabled, keysSecret{name, seedKey, replicaSignerKey}, consentJWS}` renders the member's `storage_integrity.member.consensus` keys, its `consent.jws` and the `si-member-keys` volume, mounted read-only (mode `0400`) into the `si-member` container only; `siMember.reconciler.{enabled, passwordSecret{name, key}, metricsPort, resources}` renders the `si-member-reconciler` container (`--role reconciler`), which alone takes `STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD`; the guards of §59.1 replace Plan S3-C3's refusal of the reconciler. (2) **`charts/storage-integrity`:** `clickhouse.access.memberDDL` renders the user `si_member_ddl` with exactly its three grants and the profile `si_member_ddl` whose merge constraint is const; `monitoring.stage3.{exitAlert, heldCleanupAgeBoundSeconds, p6RunbookURL}` renders the group `<fullname>.stage3-p6` with `ArbiterFloorHeldCleanupAgeAboveExitBound`. (3) **The grant probe:** thirteen `si_member_ddl` rows with the new outcome `constrained`, ordered so every denial meets an existing table (the Bugbot PR 249 fix of contract §59.3). (4) **Scripts:** `make-member-keys.sh` (new) and `make-clickhouse-logins.sh --add-user si_member_ddl`. (5) **Tests** that keep each secret in its one container and every member value out of the host pod, and that hold through Tasks 86, 87 and 89's live commits.

**Files** (anchors: production `origin/main` once Plan S3-E4's `P5_PR` has merged, i.e. `75514e3de` (SI paths byte-identical to Plan S3-B's base `9d3d0326b`) with the hunks of Plans S3-B, S3-C1, S3-C3, S3-C4, S3-D and S3-E4 applied, as rebuilt while drafting; line numbers in the diff headers are that rebuild's and are guidance only; the context lines are the anchor, and a hunk that meets a live commit of an earlier plan on the same lines applies its intent on top of it, the hunk rule of the Global Constraints):
- Create: `scripts/storage-integrity/make-member-keys.sh` (mode 755)
- Modify: `charts/sentio-node/values.yaml` (the `siMember` block of Plan S3-C3: `siMember.reconciler` `:346-349` becomes `siMember.consensus` and the full `siMember.reconciler`)
- Modify: `charts/sentio-node/templates/si-member.yaml` (the header comment `:14-15`; `sentio-node.siMember.config` `:50-53`; the reconciler refusal `:76-79` becomes the define `sentio-node.siMember.consensus` and the P6 guards; the `siMember.config` key guard `:160-164`; `$config` `:175`; the ConfigMap `:184-185`; the checksum `:205`; the `si-member` container's mounts and the pod's volumes `:275-280`)
- Modify: `charts/storage-integrity/values.yaml` (`clickhouse.access.passwordSha256.si_member_ddl` after `si_member` `:94`; the `member` comment `:115`; `memberDDL` after Plan S3-D's `snodeRestoreGrant` `:133`; `monitoring.stage3` after Plan S3-E4's `stage3Consensus` `:446`)
- Modify: `charts/storage-integrity/values.schema.json` (`clickhouse.access.required` `:115`, `passwordSha256.si_member_ddl` `:131`, `memberDDL` after `snodeRestoreGrant` `:148`; `monitoring.required` `:235` and `monitoring.properties.stage3` after `stage3Consensus` `:258`)
- Modify: `charts/storage-integrity/templates/_helpers.tpl` (`storage-integrity.clickhouseUsers` after `si_member` `:232`; `storage-integrity.grants.si_member_ddl` before `storage-integrity.memberGrants` `:260`)
- Modify: `charts/storage-integrity/templates/clickhouse.yaml` (the profiles, after `si_member`'s `:150`)
- Modify: `charts/storage-integrity/templates/validate.yaml` (Plan S3-C3's `si_member_ddl` refusal `:361-364`; one block appended after the last line `:443`)
- Modify: `charts/storage-integrity/templates/monitoring.yaml` (the group `<fullname>.stage3-p6` before the file's last `{{- end }}` `:717`, after Plan S3-E4's `arbiter-stage3` group)
- Modify: `charts/storage-integrity/tests/clickhouse_grant_probe.tsv` (two header comment lines after Plan S3-D's `:12`; thirteen rows before the first `default` cleanup row `:260`)
- Modify: `scripts/storage-integrity/make-clickhouse-logins.sh` (header `:19-20`, `usage` `:39`, `--add-user` `:51`, `plan` `:81`)
- Test: `charts/storage-integrity/tests/test_render.py` (Step 2): the P6 constants after Plan S3-E4's `P5_REHEARSAL_POD` `:1117`; `probe_atoms` `:1611`; Plan S3-C3's `p2_overrides` `:1819-1827`, `without_p2_users` `:1940-1946`, `without_p2_chi` `:1992`, `p2_chi` `:2014-2029`, `_yaml_scalar` `:2104-2107` (a helper after it), `p2_manifest_text` `:2174-2176` and `p2_overlay_text` `:2216-2217`; the P6 helpers before `class DeploymentContractTest` `:2785`; `MonitoringTest.test_default_renders_no_monitoring` `:3194` and `test_alert_names_are_the_reviewed_set` `:3351`; `P2ServerTest.test_si_member_ddl_never_exists_in_p2` `:4087-4102`; `ClickHouseAccessTest.test_grant_probe_covers_every_protocol_grant` `:4542-4544`; `ClickHouseLoginsScriptTest.ADDED` `:4782-4787` and its refusals `:4817`; `SIMemberTest.test_default_renders_no_si_member` `:5291` and `test_si_member_reconciler_stays_off_in_p2` `:5536-5538`; the new class `MemberConsensusTest` before `class ReplicaReadsTest` `:5557`; Plan S3-B's `test_indexer_b_source_clickhouse_carries_the_network_source_profile` `:7680`, `test_indexer_c_source_clickhouse_carries_the_network_source_profile_and_a_member_disk` `:7733`, `test_devnet2_clickhouse_logins_follow_the_rollout` `:8228` and `test_si_clients_take_clickhouse_passwords_only_from_secrets` `:8853`; Plan S3-C3's `member_values` `:2490`, `_p2_strip_manifest_lines` `:2077`, `SIMemberTest.setUpClass` `:5275`, `test_engine_grants_follow_the_census_constants` `:9077` and `test_si_member_config_names_the_hosts_own_source_clickhouse` `:9913`; Plan S3-C4's `ReplicaReadsTest.test_replica_reads_guards_fail_closed` `:6512`, `test_member_status_token_comes_only_from_a_secret` `:6330` and `test_devnet2_replica_reads_follow_the_rollout` `:10113`; six tests at the end of `ActualFreshNetworkTest`, before the two blank lines and `if __name__ == "__main__":` `:10372`.
- Local only, never committed: `$HOME/.cache/s3f-task83/` (mode 700): `bin/` (the stand-ins of Step 5), `rollout.py` and Step 6's throwaway clone `rehearse.<random>/repo`, its path in `rehearse-path`.
- Leave unchanged: `charts/sentio-node/templates/node.yaml` (its `checksum/config` hashes `node.config`, `housegate`, `storageIntegrity` and `housegateSidecar` only, so no `siMember` value reaches the host pod), every devnet2 overlay and CHI manifest (their P6 lines are Tasks 86, 87 and 89's single-service commits), `.github/workflows/storage-integrity-chart.yml` (its paths already hold `charts/storage-integrity/**`, `charts/sentio-node/**` and `scripts/storage-integrity/**`; Task 84 adds the P6 runbook, whose heading the exit alert's test reads), `scripts/storage-integrity/render-inertness.sh`.

**Interfaces:**
- Consumes (sentio-node `SN_COMMIT_P5`, Plan S3-E4 contract §56 and Task 79): the si-member's `storage_integrity.member.consensus.{register, create_replicas, ed25519_seed_file, replica_signer_key_file, consent_file}` (the seed file 64 lowercase hex, the key file 64 hex without `0x`, the consent file one compact JWS; a seed or key file readable by group or others is refused); `storage_integrity.member.reconciler.clickhouse_username` and, as Task 79 refines §56 (two listeners cannot share a port in one pod), `storage_integrity.member.reconciler.metrics_listen` (default `:9097`, refused when it equals the member's `metrics_listen` or `serve.listen` port); `sentio-node si-member --config-path <path> --role reconciler`, which reads no member key, the member state directory read-only for `quiescence.json` and its password only from `STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD`. The image `ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT_P5>@<SN_DIGEST_P5>` runs as root (production `Dockerfile` of sentio-node has no `USER`), so a `0400` Secret file is readable by it and by nothing else.
- Consumes (production, the earlier plans): Plan S3-C3's `siMember` block, `templates/si-member.yaml` (`sentio-node.siMember.config`, the ConfigMap `<fullname>-si-member-config` mounted at `/config`, the state volume `si-member-state` at `/data/si-member`, `checksum/si-member-config`, `$tokenSecret`), `si_member` and `SI_MEMBER_PROFILE`, `clickhouse.access.member`, `clickhouse.server`, `P2_REPLICATED_MERGE_TREE_XML`, `p2_overrides`, `p2_chi`, `without_p2_users`, `without_p2_chi`, `p2_manifest_text`, `p2_overlay_text`, `_block_end`, `_yaml_scalar`, `member_values`, `si_release_values`, `render_si_release`, `si_member_objects`, `si_member_config`, `DEVNET2_INDEXER_IDS`, `DEVNET2_MEMBER_CHIS`, `DEVNET2_MEMBER_ROLLOUT`, `DEVNET2_MEMBERS_ENABLED`, `DEVNET2_P2_STAGE`, `P2_STAGES`, `P2_STAGING_CHI`, `MEMBER_LIVE_PROBE`, `TEST_P2_PASSWORD_SHA256`; Plan S3-B's `clickhouse.access.*`, `clickhouse_grant_probe.tsv`, `grant_probe_rows`, `grant_atoms`, `probe_atoms`, `probe_object`, `atom_granted`, `atom_exercises`, `clickhouse_access`, `merge_values`, `render`, `render_legacy`, `render_sentio_node`, `documents`, `secret_references`, `vm_rules`, `alert_rule`, `promql_selectors`, `markdown_anchor`, `yaml_strings`, `MonitoringTest.MONITORED`, `MONITORING_ALERTS`, `DEVNET2_CLICKHOUSE_POSTURE`, `DEVNET2_MONITORING_ENABLED`, `ClickHouseLoginsScriptTest.ADDED`, `MAKE_CLICKHOUSE_LOGINS`, `LOCKDOWN_STAGING_CHI`, `INDEXER_B_CHI`, `INDEXER_C_CHI`, `render-inertness.sh`; Plan S3-C1's `make-clickhouse-logins.sh --add-user`; Plan S3-D's `snodeRestoreGrant`, `P4_STAGING_CHI`; Plan S3-E4's `DEVNET2_STAGE3_P5_SOURCE_IMAGE` (in `MEMBER_IMAGES`), `monitoring.stage3Consensus`, `DEVNET2_STAGE3_CONSENSUS_MONITORING`, `P5_REHEARSAL_POD`; `$HOME/.cache/s3-ledger/s3-ledger.py` (Plan S3-C1 contract §19).
- Consumes (ledger, read only with `s3-ledger.py get`): `P5_PR` (Plan S3-E4 Task 81, merged) and `P5_END` (Task 82); `P6_PR` empty. No other value enters a file: every hash, key and consent of this task is a render-only stand-in.
- Produces (contract §59, verbatim): the values `siMember.consensus.enabled` (`false`), `.keysSecret` (`{name: "", seedKey: ed25519-seed, replicaSignerKey: replica-signer-key}`), `.consentJWS` (`""`), `siMember.reconciler.passwordSecret` (`{name: "", key: password}`); the nine guards of §59.1 verbatim; the ConfigMap key `consent.jws`, the volume `si-member-keys` at `/etc/si-member-keys` (`defaultMode: 0400`), the container `si-member-reconciler`; `clickhouse.access.memberDDL` (`false`), `passwordSha256.si_member_ddl`, the user and profile `si_member_ddl` and the refusal `clickhouse.access.memberDDL requires clickhouse.access.lockDefault: si_member_ddl exists only on a locked installation`; the probe outcome `constrained` and the `si_member_ddl` rows; `monitoring.stage3.exitAlert` (`false`), `.heldCleanupAgeBoundSeconds` (`1800`), `.p6RunbookURL`, the group `<fullname>.stage3-p6`, `ArbiterFloorHeldCleanupAgeAboveExitBound` and the refusal `monitoring.stage3.heldCleanupAgeBoundSeconds must be a positive integer`; `make-member-keys.sh <a|b|c> <member-id> <public-file> [<replica-signer-key-file>]` and `make-clickhouse-logins.sh --add-user si_member_ddl <installation> <hashes-file>`; the constants `SI_MEMBER_DDL_GRANTS`, `SI_MEMBER_DDL_CONSTRAINT`, `DEVNET2_MEMBER_HOSTS`, `DEVNET2_MEMBER_KEY_SECRETS`, `DEVNET2_MEMBER_DDL_SECRETS`, `DEVNET2_MEMBER_CONSENSUS`, `DEVNET2_MEMBER_RECONCILER`, `DEVNET2_MEMBER_DDL_USERS`, `P6_HELD_CLEANUP_AGE_BOUND_SECONDS`, `DEVNET2_P6_EXIT_ALERT`; the class `MemberConsensusTest` with its fourteen tests and the six `ActualFreshNetworkTest` tests of §59.6.
- Produces (this task's names, recorded in Appendix B): the values `siMember.reconciler.metricsPort` (`9097`, rendered as `storage_integrity.member.reconciler.metrics_listen`) and `siMember.reconciler.resources`; the container port `rec-metrics`; the guards `siMember.consensus.enabled and siMember.reconciler.enabled must be true or false`, `siMember.reconciler.metricsPort must be a port other than siMember.metricsPort and siMember.statusPort: the reconciler listens beside the si-member in one pod`, `siMember.config may not set consensus or reconciler: they come from siMember.consensus and siMember.reconciler values`, `clickhouse.access.memberDDL requires clickhouse.access.member: …`, `clickhouse.access.passwordSha256.si_member_ddl must be 64 lowercase hex characters`, `monitoring.stage3.exitAlert requires monitoring.enabled`; Plan S3-C3's `si_member_ddl is not created before stage 3 P6 step 6 (Plan S3-F)` kept for a non-empty hash without `memberDDL`; the define `sentio-node.siMember.consensus` and `storage-integrity.grants.si_member_ddl`; the constants `SI_MEMBER_DDL_PROFILE`, `TEST_P6_PASSWORD_SHA256`, `TEST_CONSENT_JWS`, `DEVNET2_MEMBER_DDL_ROLLOUT`, `STAGE3_P6_RUNBOOK`, `STAGE3_P6_EXIT_SECTION`, `MAKE_MEMBER_KEYS`, `MONITORING_ALERTS["stage3-p6"]`, `P6_MANIFEST_PROFILE_COMMENT`, `P6_OVERLAY_COMMENT`; the helpers `member_consensus_values(name, reconciler=False, consent=TEST_CONSENT_JWS)`, `member_ddl_users(digest)`, `p6_values(member_ddl=True, digest=None)`, `p6_probe_atoms(statement)`, `p6_member_ddl_users_lines(digest)`, `p6_manifest_text(text, member_ddl_hash=None)`, `p6_overlay_text(text, member_ddl_hash=None)` and `_before_member_ddl(lines, start, end)`; `p2_overrides`' keywords `member_ddl` and `si_member_ddl_hash`; the local tool `rollout.py`.
- For Task 84 (the runbook) and Tasks 86, 87, 89 (the live commits): each host's consensus commit appends `consensus: {enabled: true, keysSecret: {name: sentio-node-devnet2-indexer-<x>-si-member-keys}, consentJWS: <consent-x.jws>}` under its overlay's `siMember` and flips `DEVNET2_MEMBER_CONSENSUS`; each installation's `si_member_ddl` commit is `p6_manifest_text(text, <hash>)` of its manifest (indexer-b, indexer-c) or `p6_overlay_text(text, <hash>)` of `storage-integrity-si-v2.yaml` (network), with `DEVNET2_MEMBER_DDL_USERS`; each host's reconciler commit appends `reconciler: {enabled: true, passwordSecret: {name: sentio-node-devnet2-indexer-<x>-si-member-ddl}}` under `siMember` with `DEVNET2_MEMBER_RECONCILER`; the exit commit puts `stage3: {exitAlert: true}` into the network overlay's `monitoring` block with `DEVNET2_P6_EXIT_ALERT`. Step 6 makes each of them on a throwaway clone.

Notes:
- **Inert on merge.** Every new value defaults to off or empty and renders nothing: `render-inertness.sh` prints `<release> identical` for all eight releases after each commit, and merging syncs or applies nothing (no GitOps on sea, validation-only workflows). The P6 changes on devnet2 are Tasks 86, 87 and 89's single-service commits, each confirmed in chat.
- **No remote or live action without confirmation.** Step 1 fetches production and housegate and reads GitHub: each of those blocks needs explicit user confirmation in chat before it runs. This task pushes nothing, opens no PR, makes no Secret and runs no `helmfile`, `kubectl` or registry call; Task 84 pushes the branch and opens `P6_PR`, each confirmed. Step 5 runs `make-member-keys.sh` only against local stand-ins of `kubectl` and `cast`, never against the cluster: the user runs the real one in Task 86 (D40).
- **One container per secret (S3-C2 K30, D29).** The keys Secret is a volume of the member pod mounted into `si-member` alone; the reconciler takes `si_member_ddl`'s password as one `secretKeyRef` and mounts only the ConfigMap and the state volume, both read-only; `si-member` keeps exactly its P2–P5 environment. The guards refuse either Secret being `siMember.secret.name` (the `si_member` password) or `<fullname>-si-member-status-token` (whose name the chart computes the same way Plan S3-C3's guard does), and the two being one Secret; they are checked whether or not the member is enabled, so a bad value never waits for the member to start. `siMember` is top-level, so the host pod's `checksum/config` never sees it (`test_member_changes_never_touch_the_host_checksum` renders every host object byte-identically with consensus and the reconciler on).
- **The keys' mode.** Kubernetes writes a Secret volume's files with `defaultMode`, but a pod `fsGroup` would add group read, which Task 79's loader refuses (`… must not be readable by group or others (mode %#o)`); the member pod sets no security context (test). The image runs as root, so `0400` leaves the files readable by the member process only.
- **The consent and the checksum.** `consent.jws` is a second key of the member's ConfigMap (public data); S3-C3's `checksum/si-member-config` hashed `si-member.yaml` alone, so with consensus mode it hashes `si-member.yaml` and `consent.jws: <jws>` together and a new consent alone replaces the member pod, while a release without consensus mode keeps its checksum. Secret data never enters any checksum.
- **The reconciler's own listener.** Contract §56 says the reconciler serves its series on `metrics_listen`, but it is a second container of the si-member's pod, which already binds `metricsPort` (9095) and `statusPort` (9096). Task 79 adds `storage_integrity.member.reconciler.metrics_listen` (default `:9097`); this chart renders it from `siMember.reconciler.metricsPort` and refuses a port the member uses. The member Service keeps its two ports: the runbook reads the reconciler's series inside the pod (Task 84). If Task 79's final text drops the key, Step 3's hunk drops the line (cross-plan note).
- **`si_member_ddl` (spec §8.2, S3-C3 K39).** The user is rendered exactly when `memberDDL` is set, which needs `lockDefault` (the contract's refusal, checked first) and `member` (so the installation restarted with `table_engines_require_grant` and the `<replicated_merge_tree>` default 0, Plan S3-C3); its hash is required then, and a non-empty hash without it keeps Plan S3-C3's refusal; the empty `passwordSha256.si_member_ddl` is the chart default like every other user's. Its profile inherits `default` and holds `merge_tree_max_bytes_to_merge_at_max_space_in_pool` const: ClickHouse skips a constraint whose value does not change, so `ddl.UnsafeSettings`' explicit 0 passes on an installation whose default is 0, and any other value answers 452. The user is installation-wide (K43): on the network installation it also exists on the three verifier ClickHouses, a residual until per-path Keeper ACLs (spec §18).
- **The probe rows (contract §59.3, fixed for Cursor Bugbot on housegate#249).** As first written, the `ok` rows ran as a group with `DROP TABLE` last, so the `denied` DML met a dropped table. Measured on `us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:26.3-lts-decimal512-release-2026-09-16` (26.3.12.1, the devnet2 image) with the profile, the grants, `table_engines_require_grant` and the merge default 0 of this task: after the drop, `INSERT` and `TRUNCATE` answer 60 (UNKNOWN_TABLE), before any access check, while `OPTIMIZE`, `DROP PARTITION`, `SYSTEM SYNC REPLICA` and the rest answer 497 either way. So the rows run in file order: `CREATE DATABASE IF NOT EXISTS hg_member_unsafe` and the `probe_ddl` replica (`ok`), the nine denials against that existing table and the other objects (`denied`, each 497), the merge row (`constrained`, 452), and `DROP TABLE … SYNC` last (`ok`); `test_grant_probe_covers_si_member_ddl` holds that order, and the runbook's helper judges `denied` by 497 alone. In that order every row answered as its `expect` column says on the local ClickHouse, and the Keeper path `/probe/si_member_ddl` was left without children.
- **The member keys (D40, contract §59.5).** `make-member-keys.sh` takes the `replica_signer` key from `cast wallet new --json` or from the user's own key file, never makes it from anything else, and derives both public values with OpenSSL after checking each pipeline against a known vector (RFC 8032 test 1; the secp256k1 key 1, whose address is `0x7e5f4552091a69125d5dfcb7b8c2659029395bdf`) and, with `cast`, against the address `cast` printed. The keys reach OpenSSL and `kubectl` only through pipes and files; the one `printf` of a fixed prefix is a shell builtin, and the range check reads the key into a shell variable that no process sees. Contract §59.5 asks for an `openssl version` starting `OpenSSL 3`, but on the user's Mac `openssl` is Homebrew's OpenSSL 4.0.3 (3.6.4 is `openssl@3`), which has the same `KECCAK-256` digest and gives the same vectors: the script accepts OpenSSL 3 or later and refuses LibreSSL (Appendix B; the contract text is amended).
- **The exit alert (K87 (a)).** It reads the leader's gauge as `max by (cluster, network) (…) > 1800` for 5 minutes, the form of every chart rule (S3-B's conventions keep `cluster` and `network` in each aggregation for the Grafana delivery rule; contract §59.4's bare `max(…)` would drop them), and points at `#131-daily-checks`, which `markdown_anchor("13.1 Daily checks")` gives and which Task 84's runbook carries (checked once the file exists).
- **The tests hold through the rollout.** The devnet2 constants are all off in this PR, but the earlier plans' tests read the devnet2 files as Tasks 86, 87 and 89 leave them, and Step 6's rehearsal found every one that would turn red; each is amended in place, keeping its assertions: Plan S3-C3's `member_values` builds a P2 member with consensus mode and the reconciler off (as it drops P3's `serve`), `SIMemberTest`'s member-off renders turn them off too and its reconciler test follows `DEVNET2_MEMBER_RECONCILER`; Plan S3-C4's two token tests take the member's container by name; Plan S3-B's password-source test admits the reconciler's `si_member_ddl` password, Plan S3-C3's config-key test the P5 image's `consensus` and `reconciler` keys, and its census test leaves `si_member_ddl`'s own engine grant out. For the CHI manifests and the network overlay, `without_p2_users` and `without_p2_chi` leave `si_member_ddl` out as they leave `si_member` out, `p2_chi` keeps it as it keeps the P3 and P4 grants, `p2_overrides` states `memberDDL` explicitly (off by default, or each manifest's own state with its own hash), and `p2_manifest_text` (which keeps `si_member_ddl`'s block line for line, its engine grant included) and `p2_overlay_text` write `si_member`'s lines before `si_member_ddl`'s, which `p6_manifest_text` and `p6_overlay_text` put right after them. Step 6 makes Tasks 86, 87 and 89's ten commits on a throwaway clone and runs the suite after each, and ten out-of-order or half-made changes that must fail.
- **Verified while drafting (2026-10-17).** On a scratch production tree rebuilt from `origin/main` (`febc6c4ac`) with Plan S3-B's Tasks 16, 19 and 20 applied from their text and stand-ins for its live commits (repair mode, the lockdown's posture C and logins, indexer-c's monitoring and ledger row), then Plans S3-C1 Task 26, S3-C3 Tasks 37–38, S3-C4 Task 47, S3-D Task 53 and S3-E4 Task 80 applied from their text with stand-in ledger values (local corrections recorded in Appendix B), without the two key-bearing `k8s-sea/clickhouse` manifests: `test_render.py` ran 237 tests OK at the base; after Step 2 `Ran 231 tests` / `FAILED (failures=21, errors=14)`; after Step 3 the sentio-node half of `MemberConsensusTest` passed; after Step 4 the suite failed only on the logins script; after Step 5 `Ran 257 tests` / `OK`; the three `helm lint --strict` profiles passed; `render-inertness.sh` printed the eight `identical` lines; Step 6's rehearsal (on top of stand-ins for the P2 end state and Plan S3-E4 Task 80's own P5 rehearsal, whose `MON` state was made by hand because the stand-in has no P3 live `replicaReads`) printed `OK (257 tests)` for all ten states and `refused` for all ten bites; its first run went red at `C-b` and found the nine earlier tests the Notes list, which this text amends; and a replay of Steps 2–5 from this task's text on a second clone of the base ended in the same tree. `make-member-keys.sh` ran against stand-in `kubectl` and `cast` under OpenSSL 3.6.4 and 4.0.3 (derived addresses of the keys 2 and 3 as published, the key never in a call, every refusal of §59.5) and LibreSSL 3.3.6 was refused. Every bash block of this task passes `bash -n`, `/bin/bash -n` (3.2) and `shellcheck -S warning`; both scripts pass `shellcheck` at every severity.

- [ ] **Step 1: Worktree, identity and base checks**

Requires: Plan S3-E4's `P5_PR` merged into production `main` and `P5_END` recorded (Plan S3-E4 Task 82's record past its R4 check), and `$HOME/.cache/s3-ledger/s3-ledger.py` (Plan S3-C1 contract §19; if it is missing, write it with the Write tool from the end of that section, verbatim). The commands are written for the Bash tool's shell (zsh) as well as bash; the Bash tool keeps no variables between calls, so every block sets `W` (and `T`) itself.

This block fetches production (a remote read): it needs explicit user confirmation in chat before it runs.

```bash
P=/Users/uranuswch/Dev/projects/sentio-network/production
git -C "$P" fetch -q origin
out="$(bash ~/.agents/skills/urwt/scripts/urwt-create.sh "$P" claude multi-source-s3f)"
printf '%s\n' "$out"
W="$(printf '%s\n' "$out" | jq -r .path)"
[ "$(printf '%s\n' "$out" | jq -r .action)" = created ] || { echo "the worktree is not new (an earlier attempt?): stop and ask"; exit 1; }
git -C "$W" config extensions.worktreeConfig true && git -C "$W" config --worktree user.name poetry && git -C "$W" config --worktree user.email chen@sentio.xyz
git -C "$W" status --porcelain | wc -l | tr -d ' '
git -C "$W" branch --show-current
git -C "$W" config --worktree --get user.email
[ "$(git -C "$W" rev-parse HEAD)" = "$(git -C "$P" rev-parse origin/main)" ] && echo "the branch starts at origin/main"
```

Expected: one JSON object with `"action":"created"` and the path `/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f`; `0`; `urwt/claude/multi-source-s3f`; `chen@sentio.xyz`; `the branch starts at origin/main`. Every commit of this branch (Tasks 83 and 84) is authored `poetry <chen@sentio.xyz>`.

The P5 PR, `P5_END` and the empty `P6_PR` row (remote reads: the housegate fetch and GitHub; explicit user confirmation in chat):

```bash
h=/Users/uranuswch/Dev/projects/sentio-network/housegate; L="$HOME/.cache/s3-ledger"; W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f
git -C "$h" fetch -q origin
pr="$(python3 -I "$L/s3-ledger.py" get "$h" P5_PR | sed -n 's/^P5_PR=//p')"
[[ "$pr" =~ ^[0-9]+$ ]] || { echo "P5_PR is not recorded on the ledger copy: stop"; exit 1; }
merge="$(gh pr view "$pr" --repo sentioxyz/production --json state,mergeCommit --jq 'select(.state == "MERGED") | .mergeCommit.oid')"
[ -n "$merge" ] && git -C "$W" merge-base --is-ancestor "$merge" HEAD && echo "P5_PR #$pr is in the base"
python3 -I "$L/s3-ledger.py" get "$h" P5_END | grep -E '^P5_END=20[0-9]{2}-[01][0-9]-[0-3][0-9]$' || { echo "P5_END is not recorded: stop"; exit 1; }
python3 -I "$L/s3-ledger.py" empty "$h" P6_PR
```

Expected: `P5_PR #<n> is in the base`; `P5_END=<date>`; then `s3-ledger: P6_PR not recorded yet in <ref>:docs/superpowers/plans/2026-10-15-multi-source-si-s3f-p6-switch-on.md`. A `stop`, a PR that is not merged or not in the base, or `P6_PR` recorded already: stop and ask.

The base (local only):

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f
cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
python3 -I -B - <<'PY'
import sys
from pathlib import Path

import yaml

sys.path.insert(0, "charts/storage-integrity/tests")
import test_render as t

member = yaml.safe_load(Path("charts/sentio-node/values.yaml").read_text())["siMember"]
access = yaml.safe_load(Path("charts/storage-integrity/values.yaml").read_text())["clickhouse"]["access"]
print("reconciler", member["reconciler"], "consensus" in member)
print("access keys", sorted(access))
print("P5 member image", t.DEVNET2_STAGE3_P5_SOURCE_IMAGE in t.MEMBER_IMAGES, "groups", sorted(t.MONITORING_ALERTS))
print("ddl rows", [row for row in t.grant_probe_rows() if row[0] == "si_member_ddl"])
PY
grep -c 'si_member_ddl is not created before stage 3 P6 step 6 (Plan S3-F)' charts/storage-integrity/templates/validate.yaml
grep -c 'needs the si_member_ddl user of spec §10.4 step 6 (Plan S3-F); it stays false in P2' charts/sentio-node/templates/si-member.yaml
ls scripts/storage-integrity/make-member-keys.sh docs/storage-integrity-devnet2-stage3-p6-switches.md 2>&1 | sed 's/^ls: //'
```

Expected: `Ran <N0> tests` and `OK` (write down `N0`; 237 in the drafting rebuild); `reconciler {'enabled': False} False`; `access keys ['backupUser', 'bootstrapPasswordSecret', 'engineGrants', 'lockDefault', 'member', 'passwordSha256', 'replicaReads', 'snodeRestoreGrant', 'users']`; `P5 member image True groups ['arbiter-promotion', 'arbiter-stage3', 'arbiter-state-size', 'arbiter-voters', 'backups', 'clickhouse', 'hosts', 'members', 'replica-reads']`; `ddl rows []`; `1`; `1`; and two `No such file or directory` lines. Any other answer means an earlier plan's PR text differs from what this task builds on: stop and ask.

- [ ] **Step 2: Write the failing tests**

`charts/storage-integrity/tests/test_render.py`: the P6 constants after Plan S3-E4's (`DEVNET2_MEMBER_HOSTS` is derived from Plan S3-C3's constants, which hold the ledger's `INDEXER_C_ID` once, so no ledger value is written again); `probe_atoms` asks `p6_probe_atoms` after `p2_probe_atoms`; Plan S3-C3's P2 helpers learn `si_member_ddl` (left out of the S3-B and S3-C1 comparisons, kept by `p2_chi`, stated by `p2_overrides`, written after `si_member` by P2's text writers); the P6 helpers before `class DeploymentContractTest`; the amended defaults, alert names, P2 rule, probe roles, logins-script users and P2 reconciler refusal; the earlier tests that read the devnet2 files as Tasks 86, 87 and 89 leave them (`member_values` and `SIMemberTest` turn P6's modes off with P2's member, the token, password-source, config-key and census tests, `_p2_strip_manifest_lines`); `MemberConsensusTest` before `class ReplicaReadsTest`; Plan S3-B's three manifest comparisons at each manifest's own `si_member_ddl` state; and six `ActualFreshNetworkTest` tests at the end of the class:

```diff
diff --git a/charts/storage-integrity/tests/test_render.py b/charts/storage-integrity/tests/test_render.py
--- a/charts/storage-integrity/tests/test_render.py
+++ b/charts/storage-integrity/tests/test_render.py
@@ -1115,6 +1115,73 @@ DEVNET2_STAGE3_CONSENSUS_MONITORING = False
 # runbook applies it by hand (§3.2) and deletes it (§3.6); no chart or
 # helmfile renders it.
 P5_REHEARSAL_POD = "docs/examples/storage-integrity-p5-rehearsal-pod.yaml"
+# Stage 3 P6 (housegate spec 2026-10-14 §8.2, §8.3, §10.4 steps 4 and 6, §16
+# P6; Plan S3-F contract §59): each si-member's consensus mode (its keys Secret
+# and its host's consent), its si-member-reconciler with si_member_ddl's
+# password, the user si_member_ddl with its const-merge profile, and the exit
+# alert. All off in the P6 PR; docs/storage-integrity-devnet2-stage3-p6-switches.md
+# flips each mirror below in the same single-service commit as its values file.
+SI_MEMBER_DDL_GRANTS = (
+    "GRANT CREATE DATABASE ON hg_member_unsafe.*",
+    "GRANT CREATE TABLE, DROP TABLE ON hg_member_unsafe.*",
+    "GRANT TABLE ENGINE ON ReplicatedMergeTree",
+)
+SI_MEMBER_DDL_CONSTRAINT = "merge_tree_max_bytes_to_merge_at_max_space_in_pool"
+# si_member_ddl's settings profile: default's settings, the merge setting const.
+SI_MEMBER_DDL_PROFILE = {
+    "si_member_ddl/profile": "default",
+    f"si_member_ddl/constraints/{SI_MEMBER_DDL_CONSTRAINT}/const": "",
+}
+# Render-only password hash of si_member_ddl.
+TEST_P6_PASSWORD_SHA256 = {"si_member_ddl": hashlib.sha256(b"render-test-p6-si_member_ddl").hexdigest()}
+# A render-only consent: one compact JWS shape (three base64url segments).
+TEST_CONSENT_JWS = "aGVhZGVy.cGF5bG9hZA.c2lnbmF0dXJl"
+# Each devnet2 SI host release: (its host letter, its SI indexer id, the CHI of
+# its own source ClickHouse), from Plan S3-C3's constants, which hold the
+# ledger's INDEXER_C_ID once.
+DEVNET2_MEMBER_HOSTS = {
+    name: (name.rsplit("-", 1)[1], DEVNET2_INDEXER_IDS[name], DEVNET2_MEMBER_CHIS[name]) for name in DEVNET2_SI_RELEASES
+}
+# The Secrets the user's scripts make per host (Plan S3-F contract §59.1): the
+# member keys (make-member-keys.sh) and si_member_ddl's password
+# (make-clickhouse-logins.sh --add-user si_member_ddl). Never siMember.secret
+# (the si_member password) or <release>-si-member-status-token (decision D29).
+DEVNET2_MEMBER_KEY_SECRETS = {name: f"{name}-si-member-keys" for name in DEVNET2_SI_RELEASES}
+DEVNET2_MEMBER_DDL_SECRETS = {name: f"{name}-si-member-ddl" for name in DEVNET2_SI_RELEASES}
+# siMember.consensus.enabled of each devnet2 SI host release: runbook §5.4
+# turns it on per host in the order of DEVNET2_MEMBER_ROLLOUT.
+DEVNET2_MEMBER_CONSENSUS = {
+    "sentio-node-devnet2-indexer-a": False,
+    "sentio-node-devnet2-indexer-b": False,
+    "sentio-node-devnet2-indexer-c": False,
+}
+# siMember.reconciler.enabled of each devnet2 SI host release: runbook §8.3
+# turns it on per host in the order of DEVNET2_MEMBER_ROLLOUT, once every
+# installation has si_member_ddl.
+DEVNET2_MEMBER_RECONCILER = {
+    "sentio-node-devnet2-indexer-a": False,
+    "sentio-node-devnet2-indexer-b": False,
+    "sentio-node-devnet2-indexer-c": False,
+}
+# si_member_ddl on each SI installation, by CHI name: runbook §8.2 adds it per
+# installation in the order of DEVNET2_MEMBER_DDL_ROLLOUT (indexer-b's and
+# indexer-c's manifests by hand, the network's by clickhouse.access.memberDDL).
+DEVNET2_MEMBER_DDL_ROLLOUT = ("storage-integrity-si-v2-indexer-b", "storage-integrity-si-v2-indexer-c", "storage-integrity-si-v2")
+DEVNET2_MEMBER_DDL_USERS = {
+    "storage-integrity-si-v2": False,
+    "storage-integrity-si-v2-indexer-b": False,
+    "storage-integrity-si-v2-indexer-c": False,
+}
+# The P6 exit bound of the held-cleanup age (K87 (a)) and
+# monitoring.stage3.exitAlert on devnet2-si-v2 (runbook §12.5 turns it on).
+P6_HELD_CLEANUP_AGE_BOUND_SECONDS = 1800
+DEVNET2_P6_EXIT_ALERT = False
+MONITORING_ALERTS["stage3-p6"] = frozenset({"ArbiterFloorHeldCleanupAgeAboveExitBound"})
+# The P6 runbook the exit alert's runbook_url points into (Task 84 writes it).
+STAGE3_P6_RUNBOOK = ROOT / "docs/storage-integrity-devnet2-stage3-p6-switches.md"
+STAGE3_P6_EXIT_SECTION = "13.1 Daily checks"
+# The user's member-keys script (runbook §5.1, run in the user's own terminal).
+MAKE_MEMBER_KEYS = ROOT / "scripts/storage-integrity/make-member-keys.sh"
 
 
 SENTIO_NODE_CHART = ROOT / "charts/sentio-node"
@@ -1609,6 +1676,9 @@ def probe_columns(select, where):
 def probe_atoms(statement):
     """The (privilege, object, columns) atoms the access check of one probe statement needs."""
     atoms = p2_probe_atoms(statement)
+    if atoms is not None:
+        return atoms
+    atoms = p6_probe_atoms(statement)
     if atoms is not None:
         return atoms
     name = r"(\w+)\.(\w+)"
@@ -1816,15 +1886,19 @@ def p2_password_env(chi_name):
     ]
 
 
-def p2_overrides(chi_name, stage, si_member_hash=None, gateway_engines=None):
+def p2_overrides(chi_name, stage, si_member_hash=None, gateway_engines=None, member_ddl=False, si_member_ddl_hash=None):
     """The storage-integrity values that put the network chart at one P2 stage
     with one installation's own collections, environment and member disk, every
-    P2 value explicit so the overlay's own stage never leaks into the render."""
+    P2 value explicit so the overlay's own stage never leaks into the render;
+    with member_ddl, also stage 3 P6's si_member_ddl (Plan S3-F contract §59.2),
+    which is stated off otherwise."""
     level = p2_level(stage)
     hashes = {"si_member": (si_member_hash or TEST_P2_PASSWORD_SHA256["si_member"]) if level >= 3 else ""}
+    hashes["si_member_ddl"] = (si_member_ddl_hash or TEST_P6_PASSWORD_SHA256["si_member_ddl"]) if member_ddl else ""
     return {"clickhouse": {
         "access": {
             "engineGrants": p2_engine_grants(stage, gateway_engines), "member": level >= 3, "passwordSha256": hashes,
+            "memberDDL": member_ddl,
             # Stage 3 P3 (Plan S3-C4 K48): an indexer installation's own
             # si_gateway hg_replica grant (DEVNET2_REPLICA_READ_GRANTS); the
             # network installation's comes from its own overlay.
@@ -1940,10 +2014,10 @@ def p2_env(chi_name, stage):
 def without_p2_users(users):
     """An installation's users without what P2 adds: no si_member, and no TABLE
     ENGINE grant in any grants/query list (Plans S3-B's and S3-C1's tests compare
-    the rest)."""
+    the rest); nor stage 3 P6's si_member_ddl (Plan S3-F contract §59.2)."""
     stripped = {}
     for key, value in users.items():
-        if key.startswith("si_member/"):
+        if key.startswith(("si_member/", "si_member_ddl/")):
             continue
         if key.endswith("/grants/query"):
             # Stage 3 P3's si_gateway grant (Plan S3-C4 K48) and stage 3 P4's
@@ -1989,7 +2063,7 @@ def without_p2_chi(chi):
         files["zz-pool-settings.xml"],
     )
     configuration["users"] = without_p2_users(configuration["users"])
-    configuration["profiles"] = {key: value for key, value in configuration["profiles"].items() if not key.startswith("si_member/")}
+    configuration["profiles"] = {key: value for key, value in configuration["profiles"].items() if not key.startswith(("si_member/", "si_member_ddl/"))}
     clickhouse_container(chi).pop("env", None)
     return chi
 
@@ -2015,6 +2089,11 @@ def p2_chi(chi, chi_name, stage, si_member_hash=None, gateway_engines=None):
     """A CHI at one P2 stage, from that CHI at any stage."""
     replica = SI_GATEWAY_REPLICA_GRANT in chi["spec"]["configuration"]["users"].get("si_gateway/grants/query", [])
     restore = SI_SNODE_RESTORE_GRANT in chi["spec"]["configuration"]["users"].get("si_snode/grants/query", [])
+    # Stage 3 P6's si_member_ddl (Plan S3-F contract §59.2) is kept as it is.
+    member_ddl = {
+        part: {key: value for key, value in chi["spec"]["configuration"][part].items() if key.startswith("si_member_ddl/")}
+        for part in ("users", "profiles")
+    }
     chi = without_p2_chi(chi)
     configuration = chi["spec"]["configuration"]
     configuration["files"].update(p2_files(chi_name, stage))
@@ -2027,6 +2106,8 @@ def p2_chi(chi, chi_name, stage, si_member_hash=None, gateway_engines=None):
         configuration["users"]["si_snode/grants/query"] = configuration["users"]["si_snode/grants/query"] + [SI_SNODE_RESTORE_GRANT]
     if p2_level(stage) >= 3:
         configuration["profiles"].update(SI_MEMBER_PROFILE)
+    for part, entries in member_ddl.items():
+        configuration[part].update(entries)
     env = p2_env(chi_name, stage)
     if env is not None:
         clickhouse_container(chi)["env"] = env
@@ -2077,6 +2158,13 @@ def _p2_strip_manifest_lines(lines, chi_name):
         if (in_users or in_profiles) and line.startswith("      si_member/"):
             index = _block_end(lines, index, 6)
             continue
+        if in_users and line.startswith("      si_member_ddl/"):
+            # Stage 3 P6's si_member_ddl (Plan S3-F) is kept line for line,
+            # its TABLE ENGINE grant included.
+            end = _block_end(lines, index, 6)
+            out.extend(lines[index:end])
+            index = end
+            continue
         if in_users and re.fullmatch(r'\s+- "?(GRANT TABLE ENGINE ON [A-Za-z0-9]+|GRANT SELECT ON hg_replica\.\*|GRANT READ ON S3)"?', line):
             index += 1
             continue
@@ -2105,6 +2193,15 @@ def _yaml_scalar(value):
     return json.dumps(value)
 
 
+def _before_member_ddl(lines, start, end):
+    """Where si_member's lines go between start and end: before stage 3 P6's
+    si_member_ddl lines (Plan S3-F, p6_manifest_text), which follow them."""
+    first, last = lines.index(start), lines.index(end)
+    ddl = [index for index in range(first, last) if lines[index].startswith("      si_member_ddl/")
+           or lines[index] in P6_MANIFEST_PROFILE_COMMENT]
+    return ddl[0] if ddl else last
+
+
 def _snode_restore_lines(lines):
     """si_snode's restore grant line in a CHI manifest's users (Plan S3-D
     K52), or none; si_member's own GRANT READ ON S3 is not it."""
@@ -2171,9 +2268,9 @@ def p2_manifest_text(text, chi_name, stage, si_member_hash=None, gateway_engines
             "      si_member/profile: si_member",
             "      si_member/grants/query:",
         ] + [f"        - {_yaml_scalar(grant)}" for grant in si_member_grants([name for name, _, _ in DEVNET2_NAMED_COLLECTIONS[chi_name]])]
-        at = lines.index("    profiles:")
+        at = _before_member_ddl(lines, "    users:", "    profiles:")
         lines[at:at] = member
-        at = lines.index("  templates:")
+        at = _before_member_ddl(lines, "    profiles:", "  templates:")
         lines[at:at] = [f"      {key}: {_yaml_scalar(value)}" for key, value in SI_MEMBER_PROFILE.items()]
     if replica:
         # It stays si_gateway's last grant line.
@@ -2214,7 +2311,10 @@ def p2_overlay_text(text, stage, si_member_hash=None, gateway_engines=None):
         if not re.fullmatch(r"[0-9a-f]{64}", si_member_hash or ""):
             raise ValueError("the member stage needs si_member's SHA-256 (hashes-network.yaml)")
         hashes_end = _block_end(lines, hashes, 4)
-        lines[hashes_end:hashes_end] = [f'      si_member: "{si_member_hash}"']
+        # Before stage 3 P6's si_member_ddl hash (Plan S3-F, p6_overlay_text), which follows it.
+        ddl = [index for index in range(hashes, hashes_end) if lines[index].startswith("      si_member_ddl: ")]
+        at = ddl[0] if ddl else hashes_end
+        lines[at:at] = [f'      si_member: "{si_member_hash}"']
     return "\n".join(lines) + "\n"
 
 
@@ -2489,8 +2589,11 @@ def member_values(name, budget=107374182400):
         "secret": {"name": f"storage-integrity-si-v2-member-{installation}"},
         "statusTokenSecret": {"name": f"{name}-si-member-status-token"},
         "restoreSecret": {"name": f"storage-integrity-si-v2-restore-{suffix}"},
-        # Stage 3 P3's serving endpoint (Plan S3-C4 contract §32.1) is not P2's.
+        # Stage 3 P3's serving endpoint (Plan S3-C4 contract §32.1) is not P2's,
+        # nor stage 3 P6's consensus mode and reconciler (Plan S3-F contract §59.1).
         "serve": None,
+        "consensus": {"enabled": False},
+        "reconciler": {"enabled": False},
         "config": {
             "host_indexer_id": own,
             "genesis_snapshot_id": host["snode"]["genesisSnapshotID"],
@@ -2782,6 +2885,138 @@ def p4_staging_hashes():
     return found[0]
 
 
+def member_consensus_values(name, reconciler=False, consent=TEST_CONSENT_JWS):
+    """One devnet2 SI host's member in stage 3 P6 (Plan S3-F contract §59.1):
+    consensus mode on the reviewed P5 image with its own keys Secret and a
+    consent, and with reconciler its si_member_ddl password Secret; merged
+    over member_values(name)."""
+    values = {"siMember": {
+        "image": DEVNET2_STAGE3_P5_SOURCE_IMAGE,
+        "consensus": {"enabled": True, "keysSecret": {"name": DEVNET2_MEMBER_KEY_SECRETS[name]}, "consentJWS": consent},
+    }}
+    if reconciler:
+        values["siMember"]["reconciler"] = {"enabled": True, "passwordSecret": {"name": DEVNET2_MEMBER_DDL_SECRETS[name]}}
+    return values
+
+
+def member_ddl_users(digest):
+    """si_member_ddl's users.xml entries (Plan S3-F contract §59.2): its literal
+    hash, any address, its own profile and exactly SI_MEMBER_DDL_GRANTS."""
+    return {
+        "si_member_ddl/password_sha256_hex": digest,
+        "si_member_ddl/networks/ip": ["::/0"],
+        "si_member_ddl/profile": "si_member_ddl",
+        "si_member_ddl/grants/query": list(SI_MEMBER_DDL_GRANTS),
+    }
+
+
+def p6_values(member_ddl=True, digest=None):
+    """storage-integrity values of a locked installation at the P2 member stage,
+    with si_member_ddl (stage 3 P6 step 6) when member_ddl."""
+    values = merge_values(clickhouse_access("C"), p2_overrides(FRESH_RELEASE, "member"))
+    return merge_values(values, {"clickhouse": {"access": {
+        "memberDDL": member_ddl,
+        "passwordSha256": {"si_member_ddl": (digest or TEST_P6_PASSWORD_SHA256["si_member_ddl"]) if member_ddl else ""},
+    }}})
+
+
+def p6_probe_atoms(statement):
+    """The (privilege, object, columns) atoms of the statement forms the
+    si_member_ddl rows add to the grant probe (Plan S3-F contract §59.3),
+    measured on ClickHouse 26.3 with table_engines_require_grant: a replicated
+    table names its Keeper path and replica and may set the merge setting (its
+    profile's constraint is checked after the access check), a database with an
+    ENGINE also needs TABLE ENGINE on it, DROP TABLE ... SYNC is a DROP TABLE.
+    None for any other form."""
+    name = r"(\w+)\.(\w+)"
+    forms = (
+        (rf"CREATE TABLE {name} \(.+\) ENGINE = (\w+)(?:\('[^']*', '[^']*'\))? ORDER BY \w+"
+         r"(?: SETTINGS max_bytes_to_merge_at_max_space_in_pool = [0-9]+)?",
+         lambda m: {("CREATE TABLE", probe_object(m[1], m[2]), None), ("TABLE ENGINE", m[3], None)}),
+        (r"CREATE DATABASE (\w+) ENGINE = (\w+)\(.*\)",
+         lambda m: {("CREATE DATABASE", f"{m[1]}.*", None), ("TABLE ENGINE", m[2], None)}),
+        (rf"DROP TABLE {name} SYNC", lambda m: {("DROP TABLE", probe_object(m[1], m[2]), None)}),
+    )
+    for pattern, atoms in forms:
+        match = re.fullmatch(pattern, statement)
+        if match:
+            return atoms(match)
+    return None
+
+
+def p6_member_ddl_users_lines(digest):
+    """si_member_ddl's users lines in a CHI manifest, as p6_manifest_text writes them."""
+    return [
+        f'      si_member_ddl/password_sha256_hex: "{digest}"',
+        "      si_member_ddl/networks/ip:",
+        '        - "::/0"',
+        "      si_member_ddl/profile: si_member_ddl",
+        "      si_member_ddl/grants/query:",
+    ] + [f"        - {_yaml_scalar(grant)}" for grant in SI_MEMBER_DDL_GRANTS]
+
+
+# Comment lines p6_manifest_text writes above si_member_ddl's profile.
+P6_MANIFEST_PROFILE_COMMENT = (
+    "      # Stage 3 P6 step 6 (Plan S3-F contract §59.2): si_member_ddl's merge",
+    "      # setting is const at the replicated_merge_tree default 0 above.",
+)
+
+
+def p6_manifest_text(text, member_ddl_hash=None):
+    """A source CHI manifest's text with or without si_member_ddl (runbook §8.2,
+    the commit of indexer-b's and indexer-c's installations): every other line
+    kept line for line; with a hash, the user right before the profiles block
+    (after si_member's lines) and its profile right before the templates block
+    (after si_member's profile)."""
+    lines = text.splitlines()
+    out, index = [], 0
+    while index < len(lines):
+        line = lines[index]
+        if line.startswith("      si_member_ddl/"):
+            index = _block_end(lines, index, 6)
+            continue
+        if line in P6_MANIFEST_PROFILE_COMMENT:
+            index += 1
+            continue
+        out.append(line)
+        index += 1
+    if member_ddl_hash is not None:
+        if not re.fullmatch(r"[0-9a-f]{64}", member_ddl_hash):
+            raise ValueError("si_member_ddl needs its SHA-256 (hashes-member-ddl-<installation>.yaml)")
+        at = out.index("    profiles:")
+        out[at:at] = p6_member_ddl_users_lines(member_ddl_hash)
+        at = out.index("  templates:")
+        out[at:at] = list(P6_MANIFEST_PROFILE_COMMENT) + [
+            f"      {key}: {_yaml_scalar(value)}" for key, value in SI_MEMBER_DDL_PROFILE.items()
+        ]
+    return "\n".join(out) + "\n"
+
+
+# Comment lines p6_overlay_text writes above clickhouse.access.memberDDL.
+P6_OVERLAY_COMMENT = (
+    "    # Stage 3 P6 step 6 (docs/storage-integrity-devnet2-stage3-p6-switches.md",
+    "    # §8.2): si_member_ddl and its const-merge profile, with its hash below.",
+)
+
+
+def p6_overlay_text(text, member_ddl_hash=None):
+    """storage-integrity-si-v2.yaml's text with or without si_member_ddl
+    (runbook §8.2, the network installation's commit): memberDDL right after
+    member, its hash last in passwordSha256, every other line kept."""
+    lines = [line for line in text.splitlines()
+             if line not in P6_OVERLAY_COMMENT and not line.startswith(("    memberDDL: ", "      si_member_ddl: "))]
+    if member_ddl_hash is not None:
+        if not re.fullmatch(r"[0-9a-f]{64}", member_ddl_hash):
+            raise ValueError("si_member_ddl needs its SHA-256 (hashes-member-ddl-network.yaml)")
+        access = lines.index("  access:")
+        member = next(index for index in range(access, len(lines)) if lines[index].startswith("    member: "))
+        lines[member + 1:member + 1] = list(P6_OVERLAY_COMMENT) + ["    memberDDL: true"]
+        hashes = lines.index("    passwordSha256:", access)
+        end = _block_end(lines, hashes, 4)
+        lines[end:end] = [f'      si_member_ddl: "{member_ddl_hash}"']
+    return "\n".join(lines) + "\n"
+
+
 class DeploymentContractTest(unittest.TestCase):
     @classmethod
     def setUpClass(cls):
@@ -3196,6 +3431,10 @@ class MonitoringTest(unittest.TestCase):
                 "enabled": False, "heldCleanupAgeWarnSeconds": 3600,
                 "runbookURL": "https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p5-v21.md",
             },
+            "stage3": {
+                "exitAlert": False, "heldCleanupAgeBoundSeconds": 1800,
+                "p6RunbookURL": "https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p6-switches.md",
+            },
             "clickhouse": {"installations": [], "diskFreeWarnRatio": 0.2, "diskFreeCriticalRatio": 0.1},
             "backups": {
                 "cronJobs": [], "maxAgeSeconds": 93600,
@@ -3351,6 +3590,8 @@ class MonitoringTest(unittest.TestCase):
         rules.update(vm_rules(self.replica_reads_monitored()))
         # The arbiter-stage3 group renders only with its flag (Plan S3-E4 contract §56).
         rules.update(vm_rules(documents(render({"monitoring": {**self.MONITORED["monitoring"], "stage3Consensus": {"enabled": True}}}))))
+        # The stage3-p6 group renders only with its flag (Plan S3-F contract §59.4).
+        rules.update(vm_rules(documents(render({"monitoring": {**self.MONITORED["monitoring"], "stage3": {"exitAlert": True}}}))))
         self.assertEqual(set(rules), {f"storage-integrity.{group}" for group in MONITORING_ALERTS})
         for group, names in MONITORING_ALERTS.items():
             with self.subTest(group=group):
@@ -4087,19 +4328,24 @@ class P2ServerTest(unittest.TestCase):
         self.assertEqual(overlay["access"]["member"], DEVNET2_P2_STAGE[FRESH_RELEASE] == "member")
 
     def test_si_member_ddl_never_exists_in_p2(self):
-        for value in (TEST_P2_PASSWORD_SHA256["si_member"], ""):
-            with self.subTest(value=value):
-                values = self.values("member")
-                values["clickhouse"]["access"]["passwordSha256"]["si_member_ddl"] = value
-                self.assert_rejected(values, "si_member_ddl is not created before stage 3 P6 step 6 (Plan S3-F)")
+        # Without memberDDL (stage 3 P6 step 6, Plan S3-F contract §59.2) a hash
+        # of si_member_ddl is refused, and the empty default renders nothing.
+        values = self.values("member")
+        values["clickhouse"]["access"]["passwordSha256"]["si_member_ddl"] = TEST_P2_PASSWORD_SHA256["si_member"]
+        self.assert_rejected(values, "si_member_ddl is not created before stage 3 P6 step 6 (Plan S3-F)")
         for stage in P2_STAGES:
             self.assertNotIn("si_member_ddl", render(self.values(stage)).stdout)
-        # No user, key or value names it (the posture comment says it does not exist yet).
-        for path in (DEVNET2 / "storage-integrity-si-v2.yaml", INDEXER_B_CHI, INDEXER_C_CHI, P2_STAGING_CHI):
-            with self.subTest(path=path.name):
-                self.assertNotIn("si_member_ddl", "\n".join(yaml_strings(yaml.safe_load(path.read_text()))))
-        for path in (GRANT_PROBE, MEMBER_LIVE_PROBE):
-            self.assertNotIn("si_member_ddl", path.read_text())
+        # No P2 installation or probe names it: the P2 staging installation and the
+        # member's live probe never do, and a devnet2 installation only once
+        # DEVNET2_MEMBER_DDL_USERS says so (test_devnet2_member_ddl_users_follow_the_rollout).
+        self.assertNotIn("si_member_ddl", "\n".join(yaml_strings(yaml.safe_load(P2_STAGING_CHI.read_text()))))
+        self.assertNotIn("si_member_ddl", MEMBER_LIVE_PROBE.read_text())
+        for path in (DEVNET2 / "storage-integrity-si-v2.yaml", INDEXER_B_CHI, INDEXER_C_CHI):
+            name = yaml.safe_load(path.read_text())["metadata"]["name"] if path != DEVNET2 / "storage-integrity-si-v2.yaml" else FRESH_RELEASE
+            if not DEVNET2_MEMBER_DDL_USERS[name]:
+                with self.subTest(path=path.name):
+                    self.assertNotIn("si_member_ddl", "\n".join(
+                        value for value in yaml_strings(yaml.safe_load(path.read_text())) if value != "si_member_ddl"))
         for stage in P2_STAGES:
             self.assertFalse([key for key in p2_staging_chi(stage, TEST_P2_PASSWORD_SHA256)["spec"]["configuration"]["users"]
                               if key.startswith("si_member_ddl/")])
@@ -4541,9 +4787,11 @@ class ClickHouseAccessTest(unittest.TestCase):
     def test_grant_probe_covers_every_protocol_grant(self):
         rows = grant_probe_rows()
         roles = [role for role, _, _ in rows]
-        self.assertEqual(set(roles), {"default", *SI_PROBE_GRANTS, "si_member"})
-        self.assertTrue(all(expect in ("ok", "denied", "override") for _, expect, _ in rows))
+        self.assertEqual(set(roles), {"default", *SI_PROBE_GRANTS, "si_member", "si_member_ddl"})
+        self.assertTrue(all(expect in ("ok", "denied", "override", "constrained") for _, expect, _ in rows))
         self.assertEqual({role for role, expect, _ in rows if expect == "override"}, {"si_member"})
+        # Stage 3 P6 (Plan S3-F contract §59.3): only si_member_ddl's merge row is constrained.
+        self.assertEqual({role for role, expect, _ in rows if expect == "constrained"}, {"si_member_ddl"})
         # default (the admin) only sets the referenced tables up first and drops them last.
         protocol = [index for index, role in enumerate(roles) if role != "default"]
         setup = [row for row in rows[:protocol[0]]]
@@ -4785,6 +5033,12 @@ sys.exit(f"stub kubectl: unexpected call {args}")
             installation: {f"storage-integrity-si-v2-clickhouse-backup-{installation}": {"password": "si_backup"}}
             for installation in ("network", "indexer-b", "indexer-c")
         },
+        # Stage 3 P6 step 6: si_member_ddl, whose password reaches only the
+        # si-member-reconciler of the host whose source ClickHouse it is.
+        "si_member_ddl": {
+            installation: {f"sentio-node-devnet2-indexer-{host}-si-member-ddl": {"password": "si_member_ddl"}}
+            for installation, host in (("network", "a"), ("indexer-b", "b"), ("indexer-c", "c"))
+        },
     }
 
     def test_logins_script_adds_one_user_and_only_a_known_one(self):
@@ -4817,6 +5071,8 @@ sys.exit(f"stub kubectl: unexpected call {args}")
             ("--add-user", "si_restore", "indexer-b", "hashes.yaml"),
             ("--add-user", "si_backup", "staging", "hashes.yaml"),
             ("--add-user", "si_backup", "p1-staging", "hashes.yaml"),
+            ("--add-user", "si_member_ddl", "staging", "hashes.yaml"),
+            ("--add-user", "si_member_ddl", "p1-staging", "hashes.yaml"),
             ("--add-user", "si_backup", "network"),
             ("si_backup", "network", "hashes.yaml"),
         ):
@@ -5274,7 +5530,9 @@ class SIMemberTest(unittest.TestCase):
 
     @classmethod
     def setUpClass(cls):
-        cls.off = {name: documents(render_si_release(name, {"siMember": {"enabled": False}})) for name in DEVNET2_SI_RELEASES}
+        cls.off = {name: documents(render_si_release(name, {"siMember": {
+            "enabled": False, "consensus": {"enabled": False}, "reconciler": {"enabled": False},
+        }})) for name in DEVNET2_SI_RELEASES}
         cls.on = {name: documents(render_si_release(name, member_values(name))) for name in DEVNET2_SI_RELEASES}
 
     def assert_rejected(self, result, error):
@@ -5290,7 +5548,16 @@ class SIMemberTest(unittest.TestCase):
             "config": {}, "stateSize": "1Gi", "storageClassName": "local-path",
             "resources": {"requests": {"cpu": "250m", "memory": "512Mi"}, "limits": {"cpu": "2", "memory": "2Gi"}},
             "nodeSelector": {"sentio.xyz/general": "true"}, "metricsPort": 9095, "statusPort": 9096,
-            "reconciler": {"enabled": False},
+            # Stage 3 P6 (Plan S3-F contract §59.1): consensus mode and the reconciler, off.
+            "consensus": {
+                "enabled": False,
+                "keysSecret": {"name": "", "seedKey": "ed25519-seed", "replicaSignerKey": "replica-signer-key"},
+                "consentJWS": "",
+            },
+            "reconciler": {
+                "enabled": False, "passwordSecret": {"name": "", "key": "password"}, "metricsPort": 9097,
+                "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}},
+            },
         })
         self.assertEqual(si_member_objects(documents(render_sentio_node())), {})
         for name, docs in self.off.items():
@@ -5536,17 +5803,22 @@ class SIMemberTest(unittest.TestCase):
 
     def test_si_member_reconciler_stays_off_in_p2(self):
         # The si-member-reconciler container and si_member_ddl belong to spec
-        # §10.4 step 6 (Plan S3-F): the value is a refused slot in P2.
-        error = "siMember.reconciler.enabled needs the si_member_ddl user of spec §10.4 step 6 (Plan S3-F); it stays false in P2"
+        # §10.4 step 6: Plan S3-F's guard refuses it without consensus mode, which
+        # no P2 member has.
+        error = ("siMember.reconciler.enabled requires siMember.consensus.enabled: a member creates "
+                 "hg_member_unsafe replicas only while it is registered (spec §8.3)")
         reconciler = {"siMember": {"reconciler": {"enabled": True}}}
         self.assert_rejected(render_sentio_node(reconciler), error)
         name = "sentio-node-devnet2-indexer-b"
-        for overrides in ((reconciler,), (member_values(name), reconciler)):
+        p2 = {"siMember": {"consensus": {"enabled": False}}}
+        for overrides in ((p2, reconciler), (member_values(name), reconciler)):
             with self.subTest(overrides=overrides):
                 self.assert_rejected(render_si_release(name, *overrides), error)
         for name, docs in self.on.items():
             with self.subTest(release=name):
-                self.assertIs(si_release_values(name)["siMember"]["reconciler"]["enabled"], False)
+                # The overlay's own reconciler is stage 3 P6's (DEVNET2_MEMBER_RECONCILER);
+                # member_values renders the P2 member without it.
+                self.assertIs(si_release_values(name)["siMember"]["reconciler"]["enabled"], DEVNET2_MEMBER_RECONCILER[name])
                 pod = docs[("StatefulSet", f"{name}-si-member")]["spec"]["template"]["spec"]
                 self.assertEqual([container["name"] for container in pod["containers"]], ["si-member"])
                 rendered = yaml.safe_dump(list(si_member_objects(docs).values()))
@@ -5554,6 +5826,409 @@ class SIMemberTest(unittest.TestCase):
                 self.assertNotIn("reconciler", rendered)
 
 
+class MemberConsensusTest(unittest.TestCase):
+    """Plan S3-F contract §59.1-§59.4 (housegate spec 2026-10-14 §8.2, §8.3,
+    §10.4 steps 4 and 6, §16 P6): each si-member's consensus mode with its own
+    keys and its host's consent, its si-member-reconciler with si_member_ddl's
+    password, the user si_member_ddl with its const-merge profile and its grant
+    probe rows, and the exit alert; every one off by default, each secret in
+    its one container, no member value in the host pod."""
+
+    @classmethod
+    def setUpClass(cls):
+        cls.p2 = {name: documents(render_si_release(name, member_values(name))) for name in DEVNET2_SI_RELEASES}
+        cls.consensus = {
+            name: documents(render_si_release(name, member_values(name), member_consensus_values(name)))
+            for name in DEVNET2_SI_RELEASES
+        }
+        cls.reconciler = {
+            name: documents(render_si_release(name, member_values(name), member_consensus_values(name, reconciler=True)))
+            for name in DEVNET2_SI_RELEASES
+        }
+
+    def assert_rejected(self, result, error):
+        self.assertNotEqual(result.returncode, 0, "the chart rendered")
+        self.assertIn(error, result.stderr)
+
+    @staticmethod
+    def member_pod(docs, name):
+        return docs[("StatefulSet", f"{name}-si-member")]["spec"]["template"]
+
+    @staticmethod
+    def containers(pod):
+        return {container["name"]: container for container in pod["spec"]["containers"]}
+
+    def test_default_renders_no_member_consensus(self):
+        member = yaml.safe_load((SENTIO_NODE_CHART / "values.yaml").read_text())["siMember"]
+        self.assertEqual(member["consensus"], {
+            "enabled": False,
+            "keysSecret": {"name": "", "seedKey": "ed25519-seed", "replicaSignerKey": "replica-signer-key"},
+            "consentJWS": "",
+        })
+        self.assertEqual(member["reconciler"], {
+            "enabled": False, "passwordSecret": {"name": "", "key": "password"}, "metricsPort": 9097,
+            "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}},
+        })
+        access = yaml.safe_load((CHART / "values.yaml").read_text())["clickhouse"]["access"]
+        self.assertIs(access["memberDDL"], False)
+        self.assertEqual(access["passwordSha256"]["si_member_ddl"], "")
+        self.assertNotIn("si_member_ddl", render().stdout)
+        for name, docs in self.p2.items():
+            with self.subTest(release=name):
+                config_map = docs[("ConfigMap", f"{name}-si-member-config")]
+                self.assertEqual(list(config_map["data"]), ["si-member.yaml"])
+                member_config = si_member_config(docs)["storage_integrity"]["member"]
+                self.assertNotIn("consensus", member_config)
+                self.assertNotIn("reconciler", member_config)
+                pod = self.member_pod(docs, name)
+                self.assertEqual(list(self.containers(pod)), ["si-member"])
+                self.assertEqual([volume["name"] for volume in pod["spec"]["volumes"]], ["config"])
+                for value in (DEVNET2_MEMBER_KEY_SECRETS[name], DEVNET2_MEMBER_DDL_SECRETS[name], "si_member_ddl", "si-member-keys"):
+                    self.assertNotIn(value, yaml.safe_dump(list(docs.values())))
+
+    def test_member_consensus_renders_the_consensus_mode_keys(self):
+        keys = {
+            "register": True,
+            "ed25519_seed_file": "/etc/si-member-keys/ed25519-seed",
+            "replica_signer_key_file": "/etc/si-member-keys/replica-signer-key",
+            "consent_file": "/config/consent.jws",
+        }
+        for name in DEVNET2_SI_RELEASES:
+            for reconciler, docs in ((False, self.consensus[name]), (True, self.reconciler[name])):
+                with self.subTest(release=name, reconciler=reconciler):
+                    member_config = si_member_config(docs)["storage_integrity"]["member"]
+                    self.assertEqual(member_config["consensus"], {**keys, **({"create_replicas": True} if reconciler else {})})
+                    if reconciler:
+                        self.assertEqual(member_config["reconciler"], {"clickhouse_username": "si_member_ddl", "metrics_listen": ":9097"})
+                    else:
+                        self.assertNotIn("reconciler", member_config)
+                    # Everything else of the member's config is P2's.
+                    p2_config = si_member_config(self.p2[name])["storage_integrity"]["member"]
+                    self.assertEqual({key: value for key, value in member_config.items() if key not in ("consensus", "reconciler")}, p2_config)
+                    config_map = docs[("ConfigMap", f"{name}-si-member-config")]
+                    self.assertEqual(list(config_map["data"]), ["si-member.yaml", "consent.jws"])
+                    self.assertEqual(config_map["data"]["consent.jws"], TEST_CONSENT_JWS + "\n")
+                    # The pod's checksum covers the config and the consent, never Secret data.
+                    self.assertEqual(self.member_pod(docs, name)["metadata"]["annotations"], {
+                        "checksum/si-member-config": hashlib.sha256(
+                            (config_map["data"]["si-member.yaml"] + f"consent.jws: {TEST_CONSENT_JWS}\n").encode()
+                        ).hexdigest(),
+                    })
+            # A new consent alone replaces the member pod.
+            other = documents(render_si_release(name, member_values(name), member_consensus_values(name, consent="b3RoZXI.Y29uc2VudA.c2ln")))
+            self.assertNotEqual(
+                self.member_pod(other, name)["metadata"]["annotations"],
+                self.member_pod(self.consensus[name], name)["metadata"]["annotations"],
+            )
+
+    def test_member_consensus_mounts_its_keys_only_into_the_member_container(self):
+        for name in DEVNET2_SI_RELEASES:
+            keys = DEVNET2_MEMBER_KEY_SECRETS[name]
+            for docs in (self.consensus[name], self.reconciler[name]):
+                with self.subTest(release=name):
+                    pod = self.member_pod(docs, name)
+                    volumes = {volume["name"]: volume for volume in pod["spec"]["volumes"]}
+                    self.assertEqual(volumes["si-member-keys"], {"name": "si-member-keys", "secret": {
+                        "secretName": keys, "defaultMode": 0o400,
+                        "items": [
+                            {"key": "ed25519-seed", "path": "ed25519-seed"},
+                            {"key": "replica-signer-key", "path": "replica-signer-key"},
+                        ],
+                    }})
+                    # Group read on a key file is refused by the member (Plan S3-E4
+                    # contract §56); no fsGroup or security context adds it.
+                    self.assertNotIn("securityContext", pod["spec"])
+                    containers = self.containers(pod)
+                    self.assertIn({"name": "si-member-keys", "mountPath": "/etc/si-member-keys", "readOnly": True},
+                                  containers["si-member"]["volumeMounts"])
+                    for container_name, container in containers.items():
+                        mounts = [mount["name"] for mount in container.get("volumeMounts", [])]
+                        self.assertEqual("si-member-keys" in mounts, container_name == "si-member", container_name)
+                    # Only the member's StatefulSet names the keys Secret: no host
+                    # object, no Service, no ConfigMap.
+                    for key, resource in docs.items():
+                        self.assertEqual(keys in secret_references(resource), key == ("StatefulSet", f"{name}-si-member"), key)
+
+    def test_member_reconciler_takes_its_password_only_from_its_secret(self):
+        for name, docs in self.reconciler.items():
+            with self.subTest(release=name):
+                values = si_release_values(name, member_values(name), member_consensus_values(name, reconciler=True))["siMember"]
+                ddl = DEVNET2_MEMBER_DDL_SECRETS[name]
+                containers = self.containers(self.member_pod(docs, name))
+                self.assertEqual(list(containers), ["si-member", "si-member-reconciler"])
+                reconciler = containers["si-member-reconciler"]
+                self.assertEqual(reconciler, {
+                    "name": "si-member-reconciler",
+                    "image": values["image"],
+                    "imagePullPolicy": "IfNotPresent",
+                    "command": ["/sentio-node"],
+                    "args": ["si-member", "--config-path", "/config/si-member.yaml", "--role", "reconciler"],
+                    "env": [{"name": "STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD", "valueFrom": {
+                        "secretKeyRef": {"name": ddl, "key": "password"},
+                    }}],
+                    "ports": [{"name": "rec-metrics", "containerPort": 9097, "protocol": "TCP"}],
+                    "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}},
+                    "volumeMounts": [
+                        {"name": "config", "mountPath": "/config", "readOnly": True},
+                        {"name": "si-member-state", "mountPath": "/data/si-member", "readOnly": True},
+                    ],
+                })
+                # The member keeps its own two secrets and never sees si_member_ddl's.
+                member = containers["si-member"]
+                self.assertEqual({item["valueFrom"]["secretKeyRef"]["name"] for item in member["env"]} & {ddl}, set())
+                self.assertEqual(
+                    [item["name"] for item in member["env"]][:2],
+                    ["STORAGE_INTEGRITY_MEMBER_CLICKHOUSE_PASSWORD", "STORAGE_INTEGRITY_MEMBER_STATUS_TOKEN"],
+                )
+                for key, resource in docs.items():
+                    self.assertEqual(ddl in secret_references(resource), key == ("StatefulSet", f"{name}-si-member"), key)
+                # The Service keeps the member's two ports: the reconciler's series are read in the pod.
+                self.assertEqual([port["name"] for port in docs[("Service", f"{name}-si-member")]["spec"]["ports"]], ["metrics", "status"])
+
+    def test_member_reconciler_needs_consensus_mode(self):
+        error = ("siMember.reconciler.enabled requires siMember.consensus.enabled: a member creates "
+                 "hg_member_unsafe replicas only while it is registered (spec §8.3)")
+        name = "sentio-node-devnet2-indexer-b"
+        reconciler = {"siMember": {"reconciler": {"enabled": True, "passwordSecret": {"name": DEVNET2_MEMBER_DDL_SECRETS[name]}}}}
+        self.assert_rejected(render_sentio_node(reconciler), error)
+        self.assert_rejected(render_si_release(name, member_values(name), reconciler), error)
+        self.assert_rejected(render_si_release(name, member_values(name), member_consensus_values(name, reconciler=True),
+                                               {"siMember": {"consensus": {"enabled": False}}}), error)
+        self.assertEqual(render_si_release(name, member_values(name), member_consensus_values(name, reconciler=True)).returncode, 0)
+
+    def test_member_changes_never_touch_the_host_checksum(self):
+        # A member value never enters a host object, so syncing a host release
+        # for a member change replaces only the member pod (Plan S3-C2 K30).
+        for name in DEVNET2_SI_RELEASES:
+            with self.subTest(release=name):
+                for docs in (self.consensus[name], self.reconciler[name]):
+                    self.assertEqual(
+                        {key: doc for key, doc in docs.items() if key not in si_member_objects(docs)},
+                        {key: doc for key, doc in self.p2[name].items() if key not in si_member_objects(self.p2[name])},
+                    )
+                host = self.reconciler[name][("StatefulSet", name)]["spec"]["template"]
+                self.assertEqual(host["metadata"]["annotations"]["checksum/config"],
+                                 self.p2[name][("StatefulSet", name)]["spec"]["template"]["metadata"]["annotations"]["checksum/config"])
+
+    def test_member_consensus_guards_fail_closed(self):
+        name = "sentio-node-devnet2-indexer-b"
+        token = f"{name}-si-member-status-token"
+        member_secret = "storage-integrity-si-v2-member-indexer-b"
+        keys, ddl = DEVNET2_MEMBER_KEY_SECRETS[name], DEVNET2_MEMBER_DDL_SECRETS[name]
+        on = member_consensus_values(name, reconciler=True)
+        for overrides, error in (
+            ({"siMember": {"enabled": False}}, "siMember.consensus.enabled requires siMember.enabled"),
+            ({"siMember": {"consensus": {"keysSecret": {"name": ""}}}},
+             "siMember.consensus.keysSecret.name is required when siMember.consensus.enabled"),
+            ({"siMember": {"consensus": {"consentJWS": ""}}},
+             "siMember.consensus.consentJWS must be one compact JWS (three base64url segments) when siMember.consensus.enabled"),
+            ({"siMember": {"consensus": {"consentJWS": "aGVhZGVy.cGF5bG9hZA"}}}, "must be one compact JWS"),
+            ({"siMember": {"consensus": {"consentJWS": "aGVhZGVy.cGF5bG9hZA.c2lnbmF0dXJl="}}}, "must be one compact JWS"),
+            ({"siMember": {"consensus": {"consentJWS": "aGVhZGVy.cGF5bG9hZA.c2ln\n"}}}, "must be one compact JWS"),
+            ({"siMember": {"consensus": {"enabled": "true"}}},
+             "siMember.consensus.enabled and siMember.reconciler.enabled must be true or false"),
+            ({"siMember": {"reconciler": {"passwordSecret": {"name": ""}}}},
+             "siMember.reconciler.passwordSecret.name is required when siMember.reconciler.enabled"),
+            ({"siMember": {"reconciler": {"passwordSecret": {"name": member_secret}}}},
+             "siMember.reconciler.passwordSecret must not be the si-member password Secret: "
+             "si_member_ddl's password is mounted only into the reconciler (spec §8.2)"),
+            ({"siMember": {"consensus": {"keysSecret": {"name": ddl}}}},
+             "siMember.consensus.keysSecret must not be siMember.reconciler.passwordSecret"),
+            ({"siMember": {"consensus": {"keysSecret": {"name": member_secret}}}},
+             f"siMember.consensus.keysSecret must not be the si-member password Secret or {token}: "
+             "the member keys are mounted only into si-member"),
+            ({"siMember": {"consensus": {"keysSecret": {"name": token}}}},
+             f"siMember.consensus.keysSecret must not be the si-member password Secret or {token}"),
+            ({"siMember": {"reconciler": {"passwordSecret": {"name": token}}}},
+             f"siMember.reconciler.passwordSecret must not be {token}: the host's sentio-node mounts the status token (D29)"),
+            ({"siMember": {"reconciler": {"metricsPort": 9095}}},
+             "siMember.reconciler.metricsPort must be a port other than siMember.metricsPort and siMember.statusPort"),
+            ({"siMember": {"reconciler": {"metricsPort": 9096}}}, "siMember.reconciler.metricsPort must be a port other"),
+            ({"siMember": {"config": {"consensus": {"register": True}}}},
+             "siMember.config may not set consensus or reconciler: they come from siMember.consensus and siMember.reconciler values"),
+            ({"siMember": {"config": {"reconciler": {"clickhouse_username": "si_member_ddl"}}}}, "may not set consensus or reconciler"),
+        ):
+            with self.subTest(error=error, overrides=overrides):
+                self.assert_rejected(render_si_release(name, member_values(name), on, overrides), error)
+        # The keys Secret is refused even while the member is off, and a value of
+        # an off mode is never rendered.
+        self.assert_rejected(render_sentio_node({"siMember": {"consensus": {"enabled": True, "keysSecret": {"name": keys}}}}),
+                             "siMember.consensus.enabled requires siMember.enabled")
+        self.assertEqual(render_si_release(name, member_values(name), on).returncode, 0)
+
+    def test_unset_member_consensus_renders_unchanged(self):
+        # Every new value written out at its default renders the P2 member byte
+        # for byte (render-inertness.sh proves the same for every release).
+        unset = {"siMember": {
+            "consensus": {"enabled": False, "keysSecret": {"name": "", "seedKey": "ed25519-seed", "replicaSignerKey": "replica-signer-key"},
+                          "consentJWS": ""},
+            "reconciler": {"enabled": False, "passwordSecret": {"name": "", "key": "password"}, "metricsPort": 9097},
+        }}
+        for name in DEVNET2_SI_RELEASES:
+            with self.subTest(release=name):
+                written, default = render_si_release(name, member_values(name), unset), render_si_release(name, member_values(name))
+                self.assertEqual((written.returncode, default.returncode), (0, 0), written.stderr + default.stderr)
+                self.assertEqual(written.stdout, default.stdout)
+                config_map = self.p2[name][("ConfigMap", f"{name}-si-member-config")]
+                self.assertEqual(self.member_pod(self.p2[name], name)["metadata"]["annotations"], {
+                    "checksum/si-member-config": hashlib.sha256(config_map["data"]["si-member.yaml"].encode()).hexdigest(),
+                })
+        written = render(p6_values(member_ddl=False))
+        default = render(merge_values(clickhouse_access("C"), p2_overrides(FRESH_RELEASE, "member")))
+        self.assertEqual((written.returncode, default.returncode), (0, 0), written.stderr + default.stderr)
+        self.assertEqual(written.stdout, default.stdout)
+
+    def test_si_member_ddl_holds_exactly_its_grants(self):
+        # Spec §8.2: CREATE DATABASE (without ENGINE), CREATE TABLE and DROP TABLE
+        # on hg_member_unsafe.* and TABLE ENGINE ON ReplicatedMergeTree, nothing else.
+        self.assertEqual(SI_MEMBER_DDL_GRANTS, (
+            "GRANT CREATE DATABASE ON hg_member_unsafe.*",
+            "GRANT CREATE TABLE, DROP TABLE ON hg_member_unsafe.*",
+            "GRANT TABLE ENGINE ON ReplicatedMergeTree",
+        ))
+        atoms = set().union(*(grant_atoms(grant) for grant in SI_MEMBER_DDL_GRANTS))
+        self.assertEqual(atoms, {
+            ("CREATE DATABASE", "hg_member_unsafe.*", None), ("CREATE TABLE", "hg_member_unsafe.*", None),
+            ("DROP TABLE", "hg_member_unsafe.*", None), ("TABLE ENGINE", "ReplicatedMergeTree", None),
+        })
+        off = documents(render(p6_values(member_ddl=False)))[("ClickHouseInstallation", "storage-integrity")]["spec"]["configuration"]
+        on = documents(render(p6_values()))[("ClickHouseInstallation", "storage-integrity")]["spec"]["configuration"]
+        self.assertEqual({key: value for key, value in on["users"].items() if key.startswith("si_member_ddl/")},
+                         member_ddl_users(TEST_P6_PASSWORD_SHA256["si_member_ddl"]))
+        # It adds exactly that user: every other user keeps its grants.
+        self.assertEqual({key: value for key, value in on["users"].items() if not key.startswith("si_member_ddl/")}, off["users"])
+        self.assertEqual([key for key in off["users"] if key.startswith("si_member_ddl/")], [])
+
+    def test_si_member_ddl_profile_makes_merges_const(self):
+        on = documents(render(p6_values()))[("ClickHouseInstallation", "storage-integrity")]["spec"]["configuration"]
+        off = documents(render(p6_values(member_ddl=False)))[("ClickHouseInstallation", "storage-integrity")]["spec"]["configuration"]
+        self.assertEqual({key: value for key, value in on["profiles"].items() if key.startswith("si_member_ddl/")}, SI_MEMBER_DDL_PROFILE)
+        self.assertEqual({key: value for key, value in on["profiles"].items() if not key.startswith("si_member_ddl/")}, off["profiles"])
+        # The constraint holds the replicated_merge_tree default 0 of the server
+        # bundle (Plan S3-C3 K39): ddl.UnsafeSettings' explicit 0 passes, any
+        # other value answers 452 (the corpus's constrained row).
+        self.assertEqual(on["files"]["zz-si-replicated-merge-tree.xml"], P2_REPLICATED_MERGE_TREE_XML)
+        self.assertEqual(on["files"], off["files"])
+
+    def test_si_member_ddl_carries_only_a_literal_hash(self):
+        users = documents(render(p6_values()))[("ClickHouseInstallation", "storage-integrity")]["spec"]["configuration"]["users"]
+        self.assertEqual(users["si_member_ddl/password_sha256_hex"], TEST_P6_PASSWORD_SHA256["si_member_ddl"])
+        self.assertEqual([key for key in users if key.startswith("si_member_ddl/") and "password" in key],
+                         ["si_member_ddl/password_sha256_hex"])
+        for digest in ("", TEST_P6_PASSWORD_SHA256["si_member_ddl"].upper(), TEST_P6_PASSWORD_SHA256["si_member_ddl"][:63], "plaintext"):
+            with self.subTest(digest=digest):
+                result = render(merge_values(p6_values(), {"clickhouse": {"access": {"passwordSha256": {"si_member_ddl": digest}}}}))
+                self.assertNotEqual(result.returncode, 0)
+                self.assertIn("clickhouse.access.passwordSha256.si_member_ddl must be 64 lowercase hex characters", result.stderr)
+        # A hash without the user is refused (Plan S3-C3's guard, kept); an empty one is the default.
+        result = render(merge_values(p6_values(member_ddl=False), {"clickhouse": {"access": {"passwordSha256": {"si_member_ddl": TEST_P6_PASSWORD_SHA256["si_member_ddl"]}}}}))
+        self.assertNotEqual(result.returncode, 0)
+        self.assertIn("si_member_ddl is not created before stage 3 P6 step 6 (Plan S3-F)", result.stderr)
+
+    def test_si_member_ddl_needs_a_locked_installation(self):
+        for overrides, error in (
+            (merge_values(clickhouse_access("B"), {"clickhouse": {"access": {"memberDDL": True, "passwordSha256": TEST_P6_PASSWORD_SHA256}}}),
+             "clickhouse.access.memberDDL requires clickhouse.access.lockDefault: si_member_ddl exists only on a locked installation"),
+            (merge_values(merge_values(clickhouse_access("C"), p2_overrides(FRESH_RELEASE, "restarted")),
+                          {"clickhouse": {"access": {"memberDDL": True, "passwordSha256": TEST_P6_PASSWORD_SHA256}}}),
+             "clickhouse.access.memberDDL requires clickhouse.access.member"),
+        ):
+            with self.subTest(error=error):
+                result = render(overrides)
+                self.assertNotEqual(result.returncode, 0)
+                self.assertIn(error, result.stderr)
+        result = render_legacy({"clickhouse": {"access": {"memberDDL": True}}})
+        self.assertNotEqual(result.returncode, 0)
+        result = render({"clickhouse": {"access": {"memberDDL": "true"}}})
+        self.assertNotEqual(result.returncode, 0)
+        self.assertIn("memberDDL", result.stderr)
+
+    def test_grant_probe_covers_si_member_ddl(self):
+        rows = [(expect, statement) for role, expect, statement in grant_probe_rows() if role == "si_member_ddl"]
+        held = set().union(*(grant_atoms(grant) for grant in SI_MEMBER_DDL_GRANTS))
+        mine = [(expect, statement, probe_atoms(statement)) for expect, statement in rows]
+        for expect, statement, atoms in mine:
+            with self.subTest(statement=statement):
+                granted = all(atom_granted(atom, held) for atom in atoms)
+                # ok and constrained rows pass the access check (constrained fails
+                # after it, 452); denied rows need a right si_member_ddl lacks.
+                self.assertEqual(granted, expect in ("ok", "constrained"), f"si_member_ddl {expect}: {statement}")
+                for path in re.findall(r"'(/[^']*)'", statement):
+                    self.assertTrue(path.startswith("/probe/si_member_ddl/"), path)
+        # Every grant has an ok row; the denials named by the contract each have a row.
+        for grant in SI_MEMBER_DDL_GRANTS:
+            for grant_atom in grant_atoms(grant):
+                with self.subTest(grant=grant, atom=grant_atom):
+                    self.assertTrue(any(expect == "ok" and any(atom_exercises(atom, grant_atom) for atom in atoms)
+                                        for expect, _, atoms in mine))
+        denied = [atoms for expect, _, atoms in mine if expect == "denied"]
+        for wanted in (
+            lambda atom: atom[0] == "INSERT", lambda atom: atom[0] == "OPTIMIZE", lambda atom: atom[0] == "TRUNCATE",
+            lambda atom: atom[0] == "ALTER DELETE", lambda atom: atom[0] == "SYSTEM SYNC REPLICA",
+            lambda atom: atom[0] == "SYSTEM DROP REPLICA", lambda atom: atom[:2] == ("TABLE ENGINE", "MergeTree"),
+            lambda atom: atom[:2] == ("CREATE TABLE", "hg_replica.*"), lambda atom: atom[:2] == ("TABLE ENGINE", "Replicated"),
+        ):
+            self.assertTrue(any(any(wanted(atom) for atom in atoms) for atoms in denied))
+        constrained = [statement for expect, statement in rows if expect == "constrained"]
+        self.assertEqual(len(constrained), 1)
+        self.assertRegex(constrained[0], r" SETTINGS max_bytes_to_merge_at_max_space_in_pool = [1-9][0-9]*$")
+        # File order: the table exists for every row that names it but its own
+        # CREATE, and its DROP runs last, so a denial meets the table and answers
+        # 497, never 60 (Plan S3-F contract §59.3 as fixed for Bugbot PR 249).
+        statements = [statement for _, statement in rows]
+        create = next(index for index, statement in enumerate(statements) if statement.startswith("CREATE TABLE hg_member_unsafe.probe_ddl "))
+        self.assertEqual(statements[0], "CREATE DATABASE IF NOT EXISTS hg_member_unsafe")
+        self.assertEqual(rows[-1], ("ok", "DROP TABLE hg_member_unsafe.probe_ddl SYNC"))
+        self.assertEqual([index for index, statement in enumerate(statements) if "DROP TABLE" in statement], [len(rows) - 1])
+        for index, statement in enumerate(statements):
+            if "hg_member_unsafe.probe_ddl" in statement and index != create:
+                self.assertGreater(index, create, statement)
+        ok_create = statements[create]
+        self.assertTrue(ok_create.endswith(" SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0"))
+
+    def test_p6_exit_alert_reads_the_held_cleanup_gauge(self):
+        stage3 = yaml.safe_load((CHART / "values.yaml").read_text())["monitoring"]["stage3"]
+        self.assertEqual(stage3, {
+            "exitAlert": False, "heldCleanupAgeBoundSeconds": P6_HELD_CLEANUP_AGE_BOUND_SECONDS,
+            "p6RunbookURL": "https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p6-switches.md",
+        })
+        monitored = {"monitoring": {**MonitoringTest.MONITORED["monitoring"]}}
+        self.assertNotIn("storage-integrity.stage3-p6", vm_rules(documents(render(monitored))))
+        docs = documents(render({"monitoring": {**monitored["monitoring"], "stage3": {"exitAlert": True}}}))
+        rules = vm_rules(docs)["storage-integrity.stage3-p6"]
+        self.assertEqual(rules, [{
+            "alert": "ArbiterFloorHeldCleanupAgeAboveExitBound",
+            "expr": 'max by (cluster, network) (arbiter_floor_held_cleanup_age_seconds{network="render-test-v1"}) > 1800',
+            "for": "5m",
+            "labels": {"severity": "warning", "network": "render-test-v1"},
+            "annotations": {
+                "summary": "the copy floor has held an unsafe-part cleanup for longer than the P6 exit bound",
+                "runbook_url": f"{stage3['p6RunbookURL']}#{markdown_anchor(STAGE3_P6_EXIT_SECTION)}",
+            },
+        }])
+        self.assertEqual(markdown_anchor(STAGE3_P6_EXIT_SECTION), "131-daily-checks")
+        if STAGE3_P6_RUNBOOK.exists():
+            self.assertIn(f"### {STAGE3_P6_EXIT_SECTION}", STAGE3_P6_RUNBOOK.read_text().splitlines())
+        # It reads only this network's leader gauge, keeping cluster.
+        self.assertEqual(promql_selectors(rules[0]["expr"]), [
+            ("arbiter_floor_held_cleanup_age_seconds", {"network": ("=", "render-test-v1")}),
+        ])
+        bound = documents(render({"monitoring": {**monitored["monitoring"], "stage3": {"exitAlert": True, "heldCleanupAgeBoundSeconds": 600}}}))
+        self.assertTrue(alert_rule(bound, "ArbiterFloorHeldCleanupAgeAboveExitBound")["expr"].endswith(") > 600"))
+        for overrides, error in (
+            ({"monitoring": {"stage3": {"exitAlert": True}}}, "monitoring.stage3.exitAlert requires monitoring.enabled"),
+            ({"monitoring": {"stage3": {"heldCleanupAgeBoundSeconds": 0}}}, "monitoring.stage3.heldCleanupAgeBoundSeconds must be a positive integer"),
+            ({"monitoring": {"stage3": {"heldCleanupAgeBoundSeconds": -5}}}, "monitoring.stage3.heldCleanupAgeBoundSeconds must be a positive integer"),
+            ({"monitoring": {"stage3": {"heldCleanupAgeBoundSeconds": 1.5}}}, "heldCleanupAgeBoundSeconds"),
+            ({"monitoring": {"stage3": {"exitAlert": "yes"}}}, "exitAlert"),
+            ({"monitoring": {"stage3": None}}, "stage3"),
+        ):
+            with self.subTest(error=error):
+                result = render(overrides)
+                self.assertNotEqual(result.returncode, 0)
+                self.assertIn(error, result.stderr)
+
+
 class ReplicaReadsTest(unittest.TestCase):
     """housegate spec 2026-10-14 §11 (stage 3 P3; Plan S3-C4 contract §32.1 to
     §32.3): a host's replica reads with the member status token from its own
@@ -5667,7 +6342,9 @@ class ReplicaReadsTest(unittest.TestCase):
                 for word in (self.TOKEN_SECRET, "status-token", "member_status_token", "STORAGE_INTEGRITY_MEMBER_STATUS_TOKEN"):
                     self.assertNotIn(word, text, key)
         # The si-member takes the same token from the same Secret (Plan S3-C3).
-        (member,) = self.full[("StatefulSet", f"{self.RELEASE}-si-member")]["spec"]["template"]["spec"]["containers"]
+        # The si-member container (stage 3 P6 adds si-member-reconciler beside it).
+        (member,) = [container for container in self.full[("StatefulSet", f"{self.RELEASE}-si-member")]["spec"]["template"]["spec"]["containers"]
+                     if container["name"] == "si-member"]
         self.assertEqual(self.env(member, "STORAGE_INTEGRITY_MEMBER_STATUS_TOKEN")["valueFrom"], token)
         # The key defaults to status-token.
         keyless = {"storageIntegrity": {"replicaReads": {"memberStatusURL": self.URL, "memberStatusTokenSecret": {"name": self.TOKEN_SECRET}}}}
@@ -5849,7 +6526,7 @@ class ReplicaReadsTest(unittest.TestCase):
             "storageIntegrity": {"enabled": False, "clickhouseUsername": None, "clickhousePasswordSecret": None},
             "housegate": {"storageIntegrity": {"enabled": False}},
             "housegateSidecar": {"storageIntegrity": {"enabled": False}},
-            "siMember": {"enabled": False},
+            "siMember": {"enabled": False, "consensus": {"enabled": False}, "reconciler": {"enabled": False}},
         }
         self.assert_host_rejected("storageIntegrity.replicaReads requires storageIntegrity.enabled", off, self.HOST)
         url = "storageIntegrity.replicaReads.memberStatusURL must be http://<host>:<port>"
@@ -7677,7 +8354,9 @@ class ActualFreshNetworkTest(unittest.TestCase):
                 # indexer-c's member disk and node pin are checked by
                 # test_indexer_c_source_clickhouse_carries_the_network_source_profile_and_a_member_disk.
                 name = chi["metadata"]["name"]
-                p2 = p2_overrides(name, DEVNET2_P2_STAGE[name], configuration["users"].get("si_member/password_sha256_hex"))
+                p2 = p2_overrides(name, DEVNET2_P2_STAGE[name], configuration["users"].get("si_member/password_sha256_hex"),
+                                  member_ddl=DEVNET2_MEMBER_DDL_USERS[name],
+                                  si_member_ddl_hash=configuration["users"].get("si_member_ddl/password_sha256_hex"))
                 # Each installation moves through P4 on its own (Plan S3-D): its
                 # interserver step, node pins and restore grant are compared by
                 # the P4 rollout tests, so both sides drop them here.
@@ -7730,7 +8409,9 @@ class ActualFreshNetworkTest(unittest.TestCase):
         hashes = {key: value for key, value in configuration["users"].items() if key.endswith("/password_sha256_hex")}
         backup = DEVNET2_SI_BACKUP_USER["storage-integrity-si-v2-indexer-c"]
         stage = DEVNET2_P2_STAGE["storage-integrity-si-v2-indexer-c"]
-        p2 = p2_overrides("storage-integrity-si-v2-indexer-c", stage, configuration["users"].get("si_member/password_sha256_hex"))
+        p2 = p2_overrides("storage-integrity-si-v2-indexer-c", stage, configuration["users"].get("si_member/password_sha256_hex"),
+                          member_ddl=DEVNET2_MEMBER_DDL_USERS["storage-integrity-si-v2-indexer-c"],
+                          si_member_ddl_hash=configuration["users"].get("si_member_ddl/password_sha256_hex"))
         network_users = self.network_at_posture("C", backup, p2)["spec"]["configuration"]["users"]
         self.assertEqual(sorted(hashes), sorted(
             key for key in network_users if key.endswith("/password_sha256_hex") and not key.startswith("si_verifier/")
@@ -8188,9 +8869,13 @@ class ActualFreshNetworkTest(unittest.TestCase):
                 # installation's own Secret, in its own pod (Plan S3-C3 §26.2).
                 if DEVNET2_MEMBERS_ENABLED.get(name):
                     secret = si_release_values(name)["siMember"]["secret"]
+                    # Stage 3 P6 (Plan S3-F contract §59.1): its reconciler's si_member_ddl password.
+                    reconciler = {"STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD": {
+                        "name": DEVNET2_MEMBER_DDL_SECRETS[name], "key": "password",
+                    }} if DEVNET2_MEMBER_RECONCILER[name] else {}
                     self.assertEqual(member, {"STORAGE_INTEGRITY_MEMBER_CLICKHOUSE_PASSWORD": {
                         "name": secret["name"], "key": secret["passwordKey"],
-                    }})
+                    }, **reconciler})
                 else:
                     self.assertEqual(member, {})
                 # No rendered config carries a ClickHouse password key, and the
@@ -8225,7 +8910,7 @@ class ActualFreshNetworkTest(unittest.TestCase):
         # and si_backup's once the installation has that user (stage 3 P1).
         network_backup = DEVNET2_SI_BACKUP_USER[FRESH_RELEASE]
         self.assertEqual(
-            {user for user, value in (access.get("passwordSha256") or {}).items() if value and user != "si_member"},
+            {user for user, value in (access.get("passwordSha256") or {}).items() if value and user not in ("si_member", "si_member_ddl")},
             set(SI_CLICKHOUSE_USERS["network"][1:] if network_posture != "A" else ()) | ({"default"} if network_posture == "C" else set())
             | ({"si_backup"} if network_backup else set()),
         )
@@ -8410,7 +9095,7 @@ class ActualFreshNetworkTest(unittest.TestCase):
         installations += [(name, chi["spec"]["configuration"]["users"], stage) for _, name, chi, stage in self.p2_manifests()]
         for name, chi_users, stage in installations:
             for key, grants in chi_users.items():
-                if not key.endswith("/grants/query") or key.startswith("si_member/"):
+                if not key.endswith("/grants/query") or key.startswith(("si_member/", "si_member_ddl/")):
                     continue
                 user = key.split("/", 1)[0]
                 with self.subTest(installation=name, user=user):
@@ -9245,8 +9930,10 @@ class ActualFreshNetworkTest(unittest.TestCase):
                     (host["arbiter_peers"], host["payload_store_data_addr"]),
                 )
                 member = config["member"]
-                # The P3 member image also decodes serve (Plan S3-C4 contract §31.3).
-                self.assertLessEqual(set(member), SI_MEMBER_P3_CONFIG_KEYS if values["image"] in REPLICA_READ_HOST_IMAGES else SI_MEMBER_CONFIG_KEYS)
+                # The P3 member image also decodes serve (Plan S3-C4 contract §31.3),
+                # the P5 image consensus and reconciler (Plan S3-E4 contract §56).
+                self.assertLessEqual(set(member), (SI_MEMBER_P3_CONFIG_KEYS if values["image"] in REPLICA_READ_HOST_IMAGES else SI_MEMBER_CONFIG_KEYS)
+                                     | ({"consensus", "reconciler"} if values["image"] == DEVNET2_STAGE3_P5_SOURCE_IMAGE else set()))
                 self.assertEqual({key: member[key] for key in (
                     "node_id", "host_indexer_id", "state_dir", "network_id", "genesis_snapshot_id", "genesis_tables",
                     "clickhouse_dsn", "clickhouse_username", "storage_policy", "restore", "shadow_verification",
@@ -9446,7 +10133,8 @@ class ActualFreshNetworkTest(unittest.TestCase):
                     self.assertFalse(host["member_endpoint"] or host["member_serve"] or host["replica_reads"], f"{name}: P3 values without its si-member")
                     continue
                 # One token Secret per host release serves the host and its member (D29).
-                (member,) = docs[("StatefulSet", f"{name}-si-member")]["spec"]["template"]["spec"]["containers"]
+                (member,) = [container for container in docs[("StatefulSet", f"{name}-si-member")]["spec"]["template"]["spec"]["containers"]
+                             if container["name"] == "si-member"]
                 (member_token,) = [item for item in member["env"] if item["name"] == "STORAGE_INTEGRITY_MEMBER_STATUS_TOKEN"]
                 self.assertEqual(member_token["valueFrom"]["secretKeyRef"]["name"], token_secret)
                 service = docs[("Service", f"{name}-si-member")]
@@ -10369,6 +11057,174 @@ class ActualFreshNetworkTest(unittest.TestCase):
             if path.suffix in (".yaml", ".yml") and path.is_file():
                 self.assertNotIn("storage-integrity-si-v2-p5-rehearsal", path.read_text(errors="ignore"), name)
 
+    def test_member_consensus_needs_a_reviewed_image(self):
+        # Only the stage-3 P5 member build reads storage_integrity.member.consensus
+        # and runs --role reconciler (Plan S3-E4 contract §56); an older member's
+        # strict decoder refuses the keys, so a values file may turn consensus
+        # mode on only with DEVNET2_STAGE3_P5_SOURCE_IMAGE.
+        self.assertIn(DEVNET2_STAGE3_P5_SOURCE_IMAGE, MEMBER_IMAGES)
+        checked = []
+        for helmfile in sentio_node_helmfiles():
+            for release in yaml.safe_load(helmfile.read_text())["releases"]:
+                if (helmfile.parent / release["chart"]).resolve() != SENTIO_NODE_CHART:
+                    continue
+                label = f"{helmfile.relative_to(ROOT)}:{release['name']}"
+                member = release_chart_values(helmfile, release, label)[0]["siMember"]
+                checked.append(label)
+                if member["consensus"]["enabled"] or member["reconciler"]["enabled"]:
+                    self.assertEqual(member["image"], DEVNET2_STAGE3_P5_SOURCE_IMAGE, f"{label}: member consensus mode needs the P5 member image")
+        for name in DEVNET2_SI_RELEASES:
+            self.assertIn(f"{FRESH_HELMFILE.relative_to(ROOT)}:{name}", checked)
+
+    def test_devnet2_member_consensus_follows_the_rollout(self):
+        # docs/storage-integrity-devnet2-stage3-p6-switches.md §5.4 turns each
+        # member's consensus mode on after the step-4 verifiers update, member-b,
+        # member-c, then member-a, each in a single-service commit that flips
+        # DEVNET2_MEMBER_CONSENSUS; only a running member, with its own keys
+        # Secret and its host's consent.
+        self.require_every_si_host()
+        self.assertEqual(set(DEVNET2_MEMBER_CONSENSUS), set(DEVNET2_SI_RELEASES))
+        on = [name for name in DEVNET2_MEMBER_ROLLOUT if DEVNET2_MEMBER_CONSENSUS[name]]
+        self.assertEqual(on, list(DEVNET2_MEMBER_ROLLOUT[:len(on)]))
+        for name in DEVNET2_SI_RELEASES:
+            with self.subTest(release=name):
+                member = si_release_values(name)["siMember"]
+                self.assertIs(member["consensus"]["enabled"], DEVNET2_MEMBER_CONSENSUS[name])
+                docs = self.si_hosts[name]
+                if DEVNET2_MEMBER_CONSENSUS[name]:
+                    self.assertTrue(DEVNET2_MEMBERS_ENABLED[name], f"{name}: consensus mode before its member runs")
+                    self.assertEqual(member["image"], DEVNET2_STAGE3_P5_SOURCE_IMAGE)
+                    self.assertEqual(member["consensus"]["keysSecret"]["name"], DEVNET2_MEMBER_KEY_SECRETS[name])
+                    self.assertRegex(member["consensus"]["consentJWS"], r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")
+                    self.assertTrue(si_member_config(docs)["storage_integrity"]["member"]["consensus"]["register"])
+                else:
+                    self.assertEqual(member["consensus"]["keysSecret"]["name"], "")
+                    self.assertEqual(member["consensus"]["consentJWS"], "")
+                    if DEVNET2_MEMBERS_ENABLED[name]:
+                        self.assertNotIn("consensus", si_member_config(docs)["storage_integrity"]["member"])
+
+    def test_devnet2_member_reconcilers_follow_the_rollout(self):
+        # Runbook §8.3 (spec §10.4 step 6): after the Keeper headroom gate and
+        # once every installation has si_member_ddl (§8.2), each member's
+        # reconciler starts, member-b, member-c, then member-a, each in a
+        # single-service commit that flips DEVNET2_MEMBER_RECONCILER.
+        self.require_every_si_host()
+        self.assertEqual(set(DEVNET2_MEMBER_RECONCILER), set(DEVNET2_SI_RELEASES))
+        on = [name for name in DEVNET2_MEMBER_ROLLOUT if DEVNET2_MEMBER_RECONCILER[name]]
+        self.assertEqual(on, list(DEVNET2_MEMBER_ROLLOUT[:len(on)]))
+        if on:
+            self.assertTrue(all(DEVNET2_MEMBER_CONSENSUS.values()), "step 6 comes after every member's consensus mode (step 4)")
+            self.assertTrue(all(DEVNET2_MEMBER_DDL_USERS.values()), "a reconciler starts only once every installation has si_member_ddl")
+        for name in DEVNET2_SI_RELEASES:
+            with self.subTest(release=name):
+                reconciler = si_release_values(name)["siMember"]["reconciler"]
+                self.assertIs(reconciler["enabled"], DEVNET2_MEMBER_RECONCILER[name])
+                pod = self.si_hosts[name].get(("StatefulSet", f"{name}-si-member"))
+                containers = [container["name"] for container in pod["spec"]["template"]["spec"]["containers"]] if pod else []
+                self.assertEqual("si-member-reconciler" in containers, DEVNET2_MEMBER_RECONCILER[name])
+                if DEVNET2_MEMBER_RECONCILER[name]:
+                    self.assertEqual(reconciler["passwordSecret"]["name"], DEVNET2_MEMBER_DDL_SECRETS[name])
+                else:
+                    self.assertEqual(reconciler["passwordSecret"]["name"], "")
+
+    def test_devnet2_member_ddl_users_follow_the_rollout(self):
+        # Runbook §8.2: si_member_ddl joins indexer-b's, indexer-c's and then the
+        # network installation, one single-service commit each that flips
+        # DEVNET2_MEMBER_DDL_USERS: by p6_manifest_text for the manifests applied
+        # by hand, by clickhouse.access.memberDDL (p6_overlay_text) for the network
+        # installation; only on a locked installation at its P2 member stage, and
+        # only after every member's consensus mode (step 4) and placement (step 5).
+        self.assertEqual(set(DEVNET2_MEMBER_DDL_USERS), set(DEVNET2_CLICKHOUSE_POSTURE))
+        self.assertEqual(sorted(DEVNET2_MEMBER_DDL_ROLLOUT), sorted(DEVNET2_MEMBER_DDL_USERS))
+        on = [name for name in DEVNET2_MEMBER_DDL_ROLLOUT if DEVNET2_MEMBER_DDL_USERS[name]]
+        self.assertEqual(on, list(DEVNET2_MEMBER_DDL_ROLLOUT[:len(on)]))
+        if on:
+            self.assertTrue(all(DEVNET2_MEMBER_CONSENSUS.values()), "si_member_ddl comes at step 6, after every member's consensus mode")
+        for name in DEVNET2_MEMBER_DDL_USERS:
+            with self.subTest(installation=name):
+                if DEVNET2_MEMBER_DDL_USERS[name]:
+                    self.assertEqual((DEVNET2_CLICKHOUSE_POSTURE[name], DEVNET2_P2_STAGE[name]), ("C", "member"))
+        # The network installation follows storage-integrity-si-v2.yaml.
+        overlay_path = DEVNET2 / "storage-integrity-si-v2.yaml"
+        text = overlay_path.read_text()
+        access = yaml.safe_load(text)["clickhouse"]["access"]
+        network = self.control[("ClickHouseInstallation", FRESH_RELEASE)]["spec"]["configuration"]
+        self.assertIs(access.get("memberDDL", False), DEVNET2_MEMBER_DDL_USERS[FRESH_RELEASE])
+        digest = (access.get("passwordSha256") or {}).get("si_member_ddl") or None
+        self.assertEqual(digest is not None, DEVNET2_MEMBER_DDL_USERS[FRESH_RELEASE])
+        self.assertEqual(p6_overlay_text(p6_overlay_text(text), digest), text)
+        self.assertEqual({key: value for key, value in network["users"].items() if key.startswith("si_member_ddl/")},
+                         member_ddl_users(digest) if digest else {})
+        self.assertEqual({key: value for key, value in network["profiles"].items() if key.startswith("si_member_ddl/")},
+                         SI_MEMBER_DDL_PROFILE if digest else {})
+        # indexer-b's and indexer-c's manifests carry exactly the chart's user and
+        # profile, with their own literal hash, once their constant says so.
+        hashes = {} if digest is None else {FRESH_RELEASE: digest}
+        for path in (INDEXER_B_CHI, INDEXER_C_CHI):
+            text = path.read_text()
+            chi = yaml.safe_load(text)
+            name = chi["metadata"]["name"]
+            with self.subTest(installation=name):
+                configuration = chi["spec"]["configuration"]
+                own = configuration["users"].get("si_member_ddl/password_sha256_hex")
+                self.assertEqual(own is not None, DEVNET2_MEMBER_DDL_USERS[name])
+                self.assertEqual(p6_manifest_text(p6_manifest_text(text), own), text)
+                self.assertEqual({key: value for key, value in configuration["users"].items() if key.startswith("si_member_ddl/")},
+                                 member_ddl_users(own) if own else {})
+                self.assertEqual({key: value for key, value in configuration["profiles"].items() if key.startswith("si_member_ddl/")},
+                                 SI_MEMBER_DDL_PROFILE if own else {})
+                if own:
+                    hashes[name] = own
+        # One password per installation (spec §14): no two share a hash.
+        self.assertEqual(len(set(hashes.values())), len(hashes))
+        # The staging installations of the earlier phases never get it.
+        for path in (LOCKDOWN_STAGING_CHI, P2_STAGING_CHI, P4_STAGING_CHI):
+            self.assertNotIn("si_member_ddl", path.read_text())
+
+    def test_devnet2_member_secrets_are_per_host(self):
+        # Plan S3-F contract §59.1 and decision D29: each host's member keys and
+        # si_member_ddl password are its own Secrets, never the si_member
+        # password Secret or the host's status token Secret, and no two hosts
+        # share one.
+        names = list(DEVNET2_MEMBER_KEY_SECRETS.values()) + list(DEVNET2_MEMBER_DDL_SECRETS.values())
+        self.assertEqual(len(set(names)), 2 * len(DEVNET2_SI_RELEASES))
+        for name in DEVNET2_SI_RELEASES:
+            with self.subTest(release=name):
+                member = si_release_values(name)["siMember"]
+                apart = {member["secret"]["name"], f"{name}-si-member-status-token", member["statusTokenSecret"]["name"]}
+                self.assertEqual({DEVNET2_MEMBER_KEY_SECRETS[name], DEVNET2_MEMBER_DDL_SECRETS[name]} & apart, set())
+                self.assertEqual(DEVNET2_MEMBER_KEY_SECRETS[name], f"{name}-si-member-keys")
+                self.assertEqual(DEVNET2_MEMBER_DDL_SECRETS[name], f"{name}-si-member-ddl")
+                self.assertEqual(member["consensus"]["keysSecret"]["name"], DEVNET2_MEMBER_KEY_SECRETS[name] if DEVNET2_MEMBER_CONSENSUS[name] else "")
+                self.assertEqual(member["reconciler"]["passwordSecret"]["name"], DEVNET2_MEMBER_DDL_SECRETS[name] if DEVNET2_MEMBER_RECONCILER[name] else "")
+                for other in DEVNET2_SI_RELEASES:
+                    if other != name:
+                        self.assertNotIn(DEVNET2_MEMBER_KEY_SECRETS[name], yaml.safe_dump(si_release_values(other)["siMember"]))
+        # The user's scripts make exactly these Secrets: make-member-keys.sh the
+        # keys (below), make-clickhouse-logins.sh --add-user si_member_ddl the
+        # passwords (ClickHouseLoginsScriptTest.ADDED).
+        self.assertIn('secret="sentio-node-devnet2-indexer-${host}-si-member-keys"', MAKE_MEMBER_KEYS.read_text())
+        self.assertEqual(ClickHouseLoginsScriptTest.ADDED["si_member_ddl"], {
+            installation: {DEVNET2_MEMBER_DDL_SECRETS[name]: {"password": "si_member_ddl"}}
+            for installation, name in (("network", "sentio-node-devnet2-indexer-a"), ("indexer-b", "sentio-node-devnet2-indexer-b"),
+                                       ("indexer-c", "sentio-node-devnet2-indexer-c"))
+        })
+
+    def test_devnet2_p6_exit_alert_follows_the_rollout(self):
+        # Runbook §12.5 turns the exit alert on in its own commit once
+        # min_safe_copies is 2, after every reconciler and with the P5 rules on.
+        overlay = yaml.safe_load((DEVNET2 / "storage-integrity-si-v2.yaml").read_text())["monitoring"]
+        stage3 = {**yaml.safe_load((CHART / "values.yaml").read_text())["monitoring"]["stage3"], **(overlay.get("stage3") or {})}
+        self.assertIs(stage3["exitAlert"], DEVNET2_P6_EXIT_ALERT)
+        self.assertEqual(stage3["heldCleanupAgeBoundSeconds"], P6_HELD_CLEANUP_AGE_BOUND_SECONDS)
+        groups = vm_rules(self.control, FRESH_RELEASE) if ("VMRule", FRESH_RELEASE) in self.control else {}
+        self.assertEqual(f"{FRESH_RELEASE}.stage3-p6" in groups, DEVNET2_P6_EXIT_ALERT)
+        if DEVNET2_P6_EXIT_ALERT:
+            self.assertTrue(DEVNET2_MONITORING_ENABLED and DEVNET2_STAGE3_CONSENSUS_MONITORING)
+            self.assertTrue(all(DEVNET2_MEMBER_RECONCILER.values()), "the exit alert comes after step 6")
+            self.assertTrue(alert_rule(self.control, "ArbiterFloorHeldCleanupAgeAboveExitBound", FRESH_RELEASE)["expr"].endswith(
+                f") > {P6_HELD_CLEANUP_AGE_BOUND_SECONDS}"))
+
 
 if __name__ == "__main__":
     unittest.main()
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f
cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | grep -E '^(FAIL|ERROR):|^Ran |^OK|^FAILED' | sed 's/ (__main__\.[^)]*)//' | sed -E 's/ \((release|installation|stage|user|grant|error|value)=.*//' | sort | uniq -c
```

Expected:

```text
   2 ERROR: setUpClass
   3 ERROR: test_devnet2_member_consensus_follows_the_rollout
   3 ERROR: test_devnet2_member_reconcilers_follow_the_rollout
   4 ERROR: test_devnet2_member_secrets_are_per_host
   1 ERROR: test_devnet2_p6_exit_alert_follows_the_rollout
   1 ERROR: test_member_consensus_needs_a_reviewed_image
   1 FAIL: test_alert_names_are_the_reviewed_set
   1 FAIL: test_default_renders_no_monitoring
   1 FAIL: test_default_renders_no_si_member
   1 FAIL: test_grant_probe_covers_every_protocol_grant
   2 FAIL: test_indexer_b_source_clickhouse_carries_the_network_source_profile
   1 FAIL: test_indexer_c_source_clickhouse_carries_the_network_source_profile_and_a_member_disk
   3 FAIL: test_logins_script_adds_one_user_and_only_a_known_one
   1 FAIL: test_node_pins_render_one_pod_template_per_cluster
   3 FAIL: test_p2_server_bundle_files_are_identical_across_installations_by_role
   1 FAIL: test_si_clickhouse_users_hold_exactly_the_protocol_grants
   4 FAIL: test_si_gateway_reads_hg_replica_only_with_the_replica_grant
   1 FAIL: test_si_member_reconciler_stays_off_in_p2
   1 FAIL: test_snode_restore_grant_appends_read_on_s3
   1 FAILED (failures=21, errors=14)
   1 Ran 231 tests in 58.639s
```

- [ ] **Step 3: Consensus mode and the reconciler in `charts/sentio-node`**

`charts/sentio-node/values.yaml`, the `siMember` block (Plan S3-C3's reconciler slot becomes the two P6 blocks):

```diff
diff --git a/charts/sentio-node/values.yaml b/charts/sentio-node/values.yaml
--- a/charts/sentio-node/values.yaml
+++ b/charts/sentio-node/values.yaml
@@ -343,10 +343,50 @@ siMember:
   # stage-3 P3 image serves on.
   metricsPort: 9095
   statusPort: 9096
-  # -- The si-member-reconciler container (si_member_ddl, spec §10.4 step 6)
-  # belongs to stage 3 P6 (Plan S3-F); the chart refuses it while true.
+  # -- Member consensus mode (housegate spec 2026-10-14 §6.1, §10.4 step 4;
+  # Plan S3-F contract §59.1; docs/storage-integrity-devnet2-stage3-p6-switches.md
+  # §5.4): the member registers as a VERIFIER with its own keys and its host's
+  # consent. keysSecret holds the ed25519 seed (64 hex) and the replica_signer
+  # secp256k1 key (64 hex, no 0x) under seedKey and replicaSignerKey, made only
+  # by scripts/storage-integrity/make-member-keys.sh in the user's own
+  # terminal; it is mounted read-only (mode 0400) into the si-member container
+  # alone, never into si-member-reconciler or the host pod. consentJWS is the
+  # public consent statement of the host's si_indexers signer (sentio-node
+  # storage-integrity member-consent-statement), rendered into the member's
+  # ConfigMap as consent.jws. Neither Secret may be siMember.secret or the
+  # status token's Secret (decision D29). Off by default: the member runs off
+  # consensus as in stage 3 P2 to P5; only the stage-3 P5 member image reads
+  # these keys (test_render.py, DEVNET2_STAGE3_P5_SOURCE_IMAGE).
+  consensus:
+    enabled: false
+    keysSecret:
+      name: ""
+      seedKey: ed25519-seed
+      replicaSignerKey: replica-signer-key
+    consentJWS: ""
+  # -- The si-member-reconciler container (housegate spec 2026-10-14 §8.2,
+  # §8.3, §10.4 step 6; Plan S3-F contract §59.1): the si-member image running
+  # si-member --role reconciler, which creates and drops this member's
+  # hg_member_unsafe replicas as si_member_ddl. Needs consensus.enabled (a
+  # member creates replicas only while it is registered). passwordSecret holds
+  # si_member_ddl's password (make-clickhouse-logins.sh --add-user
+  # si_member_ddl), passed to this container only, as
+  # STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD. metricsPort is its own
+  # metrics listener (storage_integrity.member.reconciler.metrics_listen),
+  # beside the member's metricsPort and statusPort in the same pod.
   reconciler:
     enabled: false
+    passwordSecret:
+      name: ""
+      key: password
+    metricsPort: 9097
+    resources:
+      requests:
+        cpu: 100m
+        memory: 128Mi
+      limits:
+        cpu: "1"
+        memory: 512Mi
   # -- (optional) The member's serving endpoint (housegate spec 2026-10-14
   # §11, stage 3 P3; Plan S3-C4 contract §31.3), rendered as
   # storage_integrity.member.serve into the si-member's own config file, so a
```

`charts/sentio-node/templates/si-member.yaml`: the header, the consensus keys at the end of the member's config, the define and the P6 guards in place of Plan S3-C3's refusal (`$fullname` moves above them, since the guards name the status token's Secret), the config key guard, the checksum over the consent, the ConfigMap's `consent.jws`, the keys mount, the reconciler container and the keys volume:

```diff
diff --git a/charts/sentio-node/templates/si-member.yaml b/charts/sentio-node/templates/si-member.yaml
--- a/charts/sentio-node/templates/si-member.yaml
+++ b/charts/sentio-node/templates/si-member.yaml
@@ -11,8 +11,11 @@ siMember.secret; the status token comes only from its own Secret
 decision D29); the restore key only from siMember.restoreSecret. The config
 file carries no secret, and the chart creates no Secret: the user's
 scripts/storage-integrity/make-p2-logins.sh and Plan S3-C1's
-make-si-backup-keys.sh do. The si-member-reconciler container (si_member_ddl)
-belongs to spec §10.4 step 6 (Plan S3-F) and is refused here.
+make-si-backup-keys.sh do. From stage 3 P6 (Plan S3-F contract §59.1), consensus
+mode mounts the member's keys (siMember.consensus.keysSecret) into the
+si-member container alone, and the si-member-reconciler container takes
+si_member_ddl's password (siMember.reconciler.passwordSecret) and no other
+secret; neither may be the si_member password Secret or the status token's.
 */ -}}
 {{- define "sentio-node.siMember.selectorLabels" -}}
 app.kubernetes.io/name: {{ include "sentio-node.fullname" . }}-si-member
@@ -50,6 +53,9 @@ storage_integrity:
     {{- with include "sentio-node.siMember.serve" . }}
     {{- . | nindent 4 }}
     {{- end }}
+    {{- with include "sentio-node.siMember.consensus" . }}
+    {{- . | nindent 4 }}
+    {{- end }}
 {{- end }}
 {{- /* storage_integrity.member.serve (housegate spec 2026-10-14 §11, stage 3
 P3; Plan S3-C4 contract §31.3), rendered only with siMember.serve.endpoint:
@@ -73,9 +79,78 @@ serve:
   {{- end }}
 {{- end }}
 {{- end }}
+{{- /* storage_integrity.member.consensus and .reconciler (housegate spec
+2026-10-14 §6.1, §8.3, §10.4 steps 4 and 6; Plan S3-F contract §59.1; the keys
+sentio-node SN_COMMIT_P5 reads, Plan S3-E4 contract §56), rendered only with
+siMember.consensus.enabled: the key files of the si-member-keys volume, the
+consent from this ConfigMap, create_replicas and the reconciler's login and
+listener only with siMember.reconciler.enabled. Never a key or a password: the
+files and the reconciler's environment carry them. */ -}}
+{{- define "sentio-node.siMember.consensus" -}}
+{{- $member := .Values.siMember -}}
+{{- if $member.consensus.enabled -}}
+consensus:
+  register: true
+  {{- if $member.reconciler.enabled }}
+  create_replicas: true
+  {{- end }}
+  ed25519_seed_file: /etc/si-member-keys/ed25519-seed
+  replica_signer_key_file: /etc/si-member-keys/replica-signer-key
+  consent_file: /config/consent.jws
+{{- if $member.reconciler.enabled }}
+reconciler:
+  clickhouse_username: si_member_ddl
+  metrics_listen: {{ printf ":%d" (int $member.reconciler.metricsPort) | quote }}
+{{- end }}
+{{- end }}
+{{- end }}
 {{- $member := .Values.siMember -}}
-{{- if $member.reconciler.enabled -}}
-{{- fail "siMember.reconciler.enabled needs the si_member_ddl user of spec §10.4 step 6 (Plan S3-F); it stays false in P2" -}}
+{{- $fullname := include "sentio-node.fullname" . -}}
+{{- /* Stage 3 P6 (Plan S3-F contract §59.1): consensus mode and the
+reconciler, refused whether or not the member is enabled, so a bad value never
+waits for the member to start; every Secret of them is its own. */ -}}
+{{- $consensus := $member.consensus | default dict -}}
+{{- $reconciler := $member.reconciler | default dict -}}
+{{- if not (and (kindIs "bool" ($consensus.enabled | default false)) (kindIs "bool" ($reconciler.enabled | default false))) -}}
+{{- fail "siMember.consensus.enabled and siMember.reconciler.enabled must be true or false" -}}
+{{- end -}}
+{{- $keysSecret := toString (dig "keysSecret" "name" "" $consensus) -}}
+{{- $passwordSecret := toString (dig "passwordSecret" "name" "" $reconciler) -}}
+{{- $tokenSecret := printf "%s-si-member-status-token" $fullname -}}
+{{- if $consensus.enabled -}}
+{{- if not $member.enabled -}}
+{{- fail "siMember.consensus.enabled requires siMember.enabled" -}}
+{{- end -}}
+{{- if eq $keysSecret "" -}}
+{{- fail "siMember.consensus.keysSecret.name is required when siMember.consensus.enabled" -}}
+{{- end -}}
+{{- if not (regexMatch "^[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$" (toString $consensus.consentJWS)) -}}
+{{- fail "siMember.consensus.consentJWS must be one compact JWS (three base64url segments) when siMember.consensus.enabled" -}}
+{{- end -}}
+{{- end -}}
+{{- if $reconciler.enabled -}}
+{{- if not $consensus.enabled -}}
+{{- fail "siMember.reconciler.enabled requires siMember.consensus.enabled: a member creates hg_member_unsafe replicas only while it is registered (spec §8.3)" -}}
+{{- end -}}
+{{- if eq $passwordSecret "" -}}
+{{- fail "siMember.reconciler.passwordSecret.name is required when siMember.reconciler.enabled" -}}
+{{- end -}}
+{{- $port := $reconciler.metricsPort -}}
+{{- if or (has (int $port) (list (int $member.metricsPort) (int $member.statusPort))) (lt (int $port) 1) (gt (int $port) 65535) -}}
+{{- fail "siMember.reconciler.metricsPort must be a port other than siMember.metricsPort and siMember.statusPort: the reconciler listens beside the si-member in one pod" -}}
+{{- end -}}
+{{- end -}}
+{{- if and (ne $passwordSecret "") (eq $passwordSecret (toString $member.secret.name)) -}}
+{{- fail "siMember.reconciler.passwordSecret must not be the si-member password Secret: si_member_ddl's password is mounted only into the reconciler (spec §8.2)" -}}
+{{- end -}}
+{{- if and (ne $keysSecret "") (eq $keysSecret $passwordSecret) -}}
+{{- fail "siMember.consensus.keysSecret must not be siMember.reconciler.passwordSecret" -}}
+{{- end -}}
+{{- if and (ne $keysSecret "") (or (eq $keysSecret (toString $member.secret.name)) (eq $keysSecret $tokenSecret)) -}}
+{{- fail (printf "siMember.consensus.keysSecret must not be the si-member password Secret or %s: the member keys are mounted only into si-member" $tokenSecret) -}}
+{{- end -}}
+{{- if and (ne $passwordSecret "") (eq $passwordSecret $tokenSecret) -}}
+{{- fail (printf "siMember.reconciler.passwordSecret must not be %s: the host's sentio-node mounts the status token (D29)" $tokenSecret) -}}
 {{- end -}}
 {{- /* The serving endpoint (stage 3 P3): its values are refused whether or not
 the member is enabled, so a bad value never waits for the member to start. */ -}}
@@ -117,7 +192,6 @@ the member is enabled, so a bad value never waits for the member to start. */ -}
 {{- end -}}
 {{- end -}}
 {{- if $member.enabled -}}
-{{- $fullname := include "sentio-node.fullname" . -}}
 {{- if ne .Values.node.mode "indexer" -}}
 {{- fail "siMember is supported only in indexer mode" -}}
 {{- end -}}
@@ -142,7 +216,6 @@ the member is enabled, so a bad value never waits for the member to start. */ -}
 {{- if empty $member.secret.name -}}
 {{- fail "siMember.secret.name is required: the si_member password comes only from that Secret" -}}
 {{- end -}}
-{{- $tokenSecret := printf "%s-si-member-status-token" $fullname -}}
 {{- if ne (toString $member.statusTokenSecret.name) $tokenSecret -}}
 {{- fail (printf "siMember.statusTokenSecret.name must be %s: the status token has its own Secret" $tokenSecret) -}}
 {{- end -}}
@@ -162,6 +235,9 @@ the member is enabled, so a bad value never waits for the member to start. */ -}
 {{- fail "siMember.config may not set node_id, state_dir, network_id, clickhouse_dsn, clickhouse_username or metrics_listen: they come from siMember values" -}}
 {{- end -}}
 {{- end -}}
+{{- if or (hasKey $member.config "consensus") (hasKey $member.config "reconciler") -}}
+{{- fail "siMember.config may not set consensus or reconciler: they come from siMember.consensus and siMember.reconciler values" -}}
+{{- end -}}
 {{- /* Any key, at any depth, whose name holds password, token or secret or
 ends in key. toJson writes every key right after "{" or ",", and a string
 value never right before ":". */ -}}
@@ -173,6 +249,12 @@ value never right before ":". */ -}}
 {{- fail "siMember.restoreSecret.name and siMember.config.restore are set together: the restore source needs its bucket and its key" -}}
 {{- end -}}
 {{- $config := printf "%s\n" (include "sentio-node.siMember.config" .) -}}
+{{- /* The pod checksum covers the consent too (consensus mode only, so a
+release without it keeps its checksum), never Secret data. */ -}}
+{{- $checksum := $config -}}
+{{- if $consensus.enabled -}}
+{{- $checksum = printf "%sconsent.jws: %s\n" $config $consensus.consentJWS -}}
+{{- end -}}
 apiVersion: v1
 kind: ConfigMap
 metadata:
@@ -183,6 +265,10 @@ metadata:
 data:
   si-member.yaml: |
     {{- $config | trimSuffix "\n" | nindent 4 }}
+  {{- if $consensus.enabled }}
+  consent.jws: |
+    {{ $consensus.consentJWS }}
+  {{- end }}
 ---
 apiVersion: apps/v1
 kind: StatefulSet
@@ -202,7 +288,7 @@ spec:
       annotations:
         # The rendered si-member.yaml only, so a host value the member does
         # not read never rolls it.
-        checksum/si-member-config: {{ $config | sha256sum }}
+        checksum/si-member-config: {{ $checksum | sha256sum }}
       labels:
         {{- include "sentio-node.siMember.selectorLabels" . | nindent 8 }}
     spec:
@@ -274,10 +360,58 @@ spec:
               readOnly: true
             - name: si-member-state
               mountPath: /data/si-member
+            {{- if $consensus.enabled }}
+            # The member's keys, for si-member alone (Plan S3-F contract §59.1).
+            - name: si-member-keys
+              mountPath: /etc/si-member-keys
+              readOnly: true
+            {{- end }}
+        {{- if $reconciler.enabled }}
+        # Stage 3 P6 step 6 (spec §8.3): this member's hg_member_unsafe replicas,
+        # as si_member_ddl; it reads the member's quiescence.json from the state
+        # volume and mounts neither the member's keys nor si_member's password.
+        - name: si-member-reconciler
+          image: {{ $member.image | quote }}
+          imagePullPolicy: IfNotPresent
+          command: ["/sentio-node"]
+          args: ["si-member", "--config-path", "/config/si-member.yaml", "--role", "reconciler"]
+          env:
+            - name: STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD
+              valueFrom:
+                secretKeyRef:
+                  name: {{ $reconciler.passwordSecret.name | quote }}
+                  key: {{ $reconciler.passwordSecret.key | quote }}
+          ports:
+            - name: rec-metrics
+              containerPort: {{ int $reconciler.metricsPort }}
+              protocol: TCP
+          {{- with $reconciler.resources }}
+          resources:
+            {{- toYaml . | nindent 12 }}
+          {{- end }}
+          volumeMounts:
+            - name: config
+              mountPath: /config
+              readOnly: true
+            - name: si-member-state
+              mountPath: /data/si-member
+              readOnly: true
+        {{- end }}
       volumes:
         - name: config
           configMap:
             name: {{ $fullname }}-si-member-config
+        {{- if $consensus.enabled }}
+        - name: si-member-keys
+          secret:
+            secretName: {{ $consensus.keysSecret.name | quote }}
+            defaultMode: 0400
+            items:
+              - key: {{ $consensus.keysSecret.seedKey | quote }}
+                path: ed25519-seed
+              - key: {{ $consensus.keysSecret.replicaSignerKey | quote }}
+                path: replica-signer-key
+        {{- end }}
   volumeClaimTemplates:
     - metadata:
         name: si-member-state
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f
cd "$W" && helm lint charts/sentio-node --strict | tail -1
python3 -B charts/storage-integrity/tests/test_render.py MemberConsensusTest SIMemberTest 2>&1 | grep -E '^(FAIL|ERROR):|^Ran |^OK|^FAILED' | sed 's/ (__main__\.[^)]*)//' | sed -E 's/ \((release|grant|error)=.*//' | sort | uniq -c
bash scripts/storage-integrity/render-inertness.sh origin/main; echo "exit $?"
```

Expected: `1 chart(s) linted, 0 chart(s) failed`; then

```text
   1 ERROR: test_default_renders_no_member_consensus
   1 ERROR: test_p6_exit_alert_reads_the_held_cleanup_gauge
   5 FAIL: test_grant_probe_covers_si_member_ddl
   1 FAIL: test_si_member_ddl_carries_only_a_literal_hash
   1 FAIL: test_si_member_ddl_holds_exactly_its_grants
   3 FAIL: test_si_member_ddl_needs_a_locked_installation
   1 FAIL: test_si_member_ddl_profile_makes_merges_const
   1 FAIL: test_unset_member_consensus_renders_unchanged
   1 FAILED (failures=12, errors=2)
   1 Ran 20 tests in 9.694s
```

then the eight lines `<release> identical` and `exit 0`. Every sentio-node test of the class passes; what still fails needs Step 4's chart.

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f
git -C "$W" add charts/sentio-node/values.yaml charts/sentio-node/templates/si-member.yaml && git -C "$W" commit -m "feat(sentio-node): si-member consensus mode and its reconciler, off

siMember.consensus renders the member's storage_integrity.member.consensus
keys (register and the key and consent files), its consent.jws and the
si-member-keys volume, mounted read-only (mode 0400) into the si-member
container only; siMember.reconciler renders the si-member-reconciler
container (si-member --role reconciler), which alone takes
STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD from its own Secret
(housegate spec 2026-10-14 §6.1, §8.2, §8.3, §10.4 steps 4 and 6; Plan
S3-F contract §59.1). Neither Secret may be the si_member password Secret
or <release>-si-member-status-token, the guards replace Plan S3-C3's
refusal of the reconciler, and no member value enters the host pod.
Every release renders unchanged.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C "$W" log -1 --format='%an <%ae>'
```

Expected: one commit, `2 files changed`; `poetry <chen@sentio.xyz>`.

- [ ] **Step 4: `si_member_ddl`, its probe rows and the exit alert in `charts/storage-integrity`**

`charts/storage-integrity/values.yaml` (the hash after `si_member`'s, `memberDDL` after Plan S3-D's `snodeRestoreGrant`, `monitoring.stage3` after Plan S3-E4's `stage3Consensus`):

```diff
diff --git a/charts/storage-integrity/values.yaml b/charts/storage-integrity/values.yaml
--- a/charts/storage-integrity/values.yaml
+++ b/charts/storage-integrity/values.yaml
@@ -92,6 +92,7 @@ clickhouse:
       si_reader: ""
       si_backup: ""
       si_member: ""
+      si_member_ddl: ""
     # When name is set, the bootstrap-source hook (every sync) logs in as
     # si_gateway with CLICKHOUSE_PASSWORD from this Secret. Requires users.
     bootstrapPasswordSecret:
@@ -112,7 +113,7 @@ clickhouse:
     # §8.2's grants, one GRANT NAMED COLLECTION per server.namedCollections
     # entry, and its settings profile. Only once this installation restarted
     # with server.enabled and the Keeper znode /sentio/<shard>/unsafe_inc
-    # exists. si_member_ddl does not exist before stage 3 P6 step 6 (Plan S3-F).
+    # exists. si_member_ddl does not exist before stage 3 P6 step 6 (memberDDL).
     member: false
     # Stage 3 P3 (housegate spec 2026-10-14 §11; Plan S3-C4 K48;
     # docs/storage-integrity-devnet2-stage3-p3-replica-reads.md §3): si_gateway,
@@ -131,6 +132,21 @@ clickhouse:
     # INSERT INTO FUNCTION s3() still needs WRITE ON S3. Requires users; a users
     # reload, no restart.
     snodeRestoreGrant: false
+    # Stage 3 P6 step 6 (housegate spec 2026-10-14 §8.2, §8.3, §10.4 step 6;
+    # Plan S3-F contract §59.2; docs/storage-integrity-devnet2-stage3-p6-switches.md
+    # §8.2): si_member_ddl (passwordSha256.si_member_ddl), the user each
+    # si-member's reconciler creates and drops its hg_member_unsafe replicas
+    # with: CREATE DATABASE (without ENGINE), CREATE TABLE and DROP TABLE on
+    # hg_member_unsafe.* and TABLE ENGINE ON ReplicatedMergeTree, nothing else.
+    # Its profile si_member_ddl makes merge_tree_max_bytes_to_merge_at_max_space_in_pool
+    # const, holding the replicated_merge_tree default 0 of the server bundle,
+    # so it can create no replica with merges allowed (ClickHouse refuses any
+    # other value with code 452). Requires lockDefault and member; a users
+    # reload, no restart. Installation-wide, so on the network installation it
+    # also exists on the verifier ClickHouses (a residual until per-path Keeper
+    # ACLs, spec §18). Its password comes from make-clickhouse-logins.sh
+    # --add-user si_member_ddl.
+    memberDDL: false
   # Stage 3 P2's server bundle (housegate spec 2026-10-14 §8.1, §8.2, §8.6;
   # docs/storage-integrity-devnet2-stage3-p2-members.md §5): config.d files a
   # ClickHouse reads only at start and the reader passwords in the clickhouse
@@ -444,4 +460,16 @@ monitoring:
     # floor holds) stays above this many seconds for 15 minutes.
     heldCleanupAgeWarnSeconds: 3600
     runbookURL: https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p5-v21.md
+  # The stage-3 P6 exit alert, group <fullname>.stage3-p6 (housegate spec
+  # 2026-10-14 §16 P6 exit; Plan S3-F K87 (a), contract §59.4):
+  # ArbiterFloorHeldCleanupAgeAboveExitBound fires when the leader's
+  # arbiter_floor_held_cleanup_age_seconds stays above heldCleanupAgeBoundSeconds
+  # for 5 minutes; a breach restarts the 7-day exit soak
+  # (docs/storage-integrity-devnet2-stage3-p6-switches.md §13.1). The P6
+  # runbook turns exitAlert on in its own commit once min_safe_copies is 2
+  # (§12.5). Needs monitoring.enabled.
+  stage3:
+    exitAlert: false
+    heldCleanupAgeBoundSeconds: 1800
+    p6RunbookURL: https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p6-switches.md
   runbookURL: https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p0.md
```

`charts/storage-integrity/values.schema.json` (`memberDDL` and `stage3` are required, so a null override is refused instead of dropping a block):

```diff
diff --git a/charts/storage-integrity/values.schema.json b/charts/storage-integrity/values.schema.json
--- a/charts/storage-integrity/values.schema.json
+++ b/charts/storage-integrity/values.schema.json
@@ -112,7 +112,7 @@
       "properties": {
         "access": {
           "type": "object",
-          "required": ["users", "lockDefault", "backupUser", "passwordSha256", "bootstrapPasswordSecret", "engineGrants", "member", "replicaReads", "snodeRestoreGrant"],
+          "required": ["users", "lockDefault", "backupUser", "passwordSha256", "bootstrapPasswordSecret", "engineGrants", "member", "replicaReads", "snodeRestoreGrant", "memberDDL"],
           "properties": {
             "users": {"type": "boolean"},
             "lockDefault": {"type": "boolean"},
@@ -128,7 +128,7 @@
                 "si_reader": {"type": "string"},
                 "si_backup": {"type": "string"},
                 "si_member": {"type": "string"},
-                "si_member_ddl": {"type": "string", "description": "Refused by validate.yaml: si_member_ddl is not created before stage 3 P6 step 6 (Plan S3-F)."}
+                "si_member_ddl": {"type": "string", "description": "Required exactly when memberDDL is set (stage 3 P6 step 6, Plan S3-F); validate.yaml refuses a non-empty hash without memberDDL."}
               },
               "description": "SHA-256 (64 lowercase hex, checked in validate.yaml) of each SI ClickHouse user's password; only the users the chart renders."
             },
@@ -145,7 +145,8 @@
             },
             "member": {"type": "boolean"},
             "replicaReads": {"type": "boolean", "description": "Stage 3 P3: GRANT SELECT ON hg_replica.* as si_gateway's last grant; requires users."},
-            "snodeRestoreGrant": {"type": "boolean", "description": "Stage 3 P4: GRANT READ ON S3 as si_snode's last grant (the owner rebuild); requires users."}
+            "snodeRestoreGrant": {"type": "boolean", "description": "Stage 3 P4: GRANT READ ON S3 as si_snode's last grant (the owner rebuild); requires users."},
+            "memberDDL": {"type": "boolean", "description": "Stage 3 P6 step 6: the user si_member_ddl and its const-merge profile; requires lockDefault and member."}
           }
         },
         "interserver": {
@@ -232,7 +233,7 @@
     },
     "monitoring": {
       "type": "object",
-      "required": ["enabled", "network", "interval", "scrapeTimeout", "labels", "stateLimitBytes", "hosts", "clickhouse", "backups", "members", "runbookURL", "stage3Consensus"],
+      "required": ["enabled", "network", "interval", "scrapeTimeout", "labels", "stateLimitBytes", "hosts", "clickhouse", "backups", "members", "runbookURL", "stage3Consensus", "stage3"],
       "properties": {
         "enabled": {"type": "boolean"},
         "network": {"type": "string"},
@@ -256,6 +257,18 @@
             "runbookURL": {"type": "string", "pattern": "^https://[^\\s#\"]+$"}
           }
         },
+        "stage3": {
+          "type": "object",
+          "required": ["exitAlert", "heldCleanupAgeBoundSeconds", "p6RunbookURL"],
+          "properties": {
+            "exitAlert": {"type": "boolean"},
+            "heldCleanupAgeBoundSeconds": {
+              "type": "integer",
+              "description": "Seconds of arbiter_floor_held_cleanup_age_seconds above which ArbiterFloorHeldCleanupAgeAboveExitBound fires (Plan S3-F K87 (a)); validate.yaml requires a positive value."
+            },
+            "p6RunbookURL": {"type": "string", "pattern": "^https://[^\\s#\"]+$"}
+          }
+        },
         "hosts": {
           "type": "object",
           "required": ["releases"],
```

`charts/storage-integrity/templates/_helpers.tpl` (the user after `si_member`, its grants before `si_member`'s):

```diff
diff --git a/charts/storage-integrity/templates/_helpers.tpl b/charts/storage-integrity/templates/_helpers.tpl
--- a/charts/storage-integrity/templates/_helpers.tpl
+++ b/charts/storage-integrity/templates/_helpers.tpl
@@ -230,6 +230,14 @@ si_member/profile: si_member
 si_member/grants/query:
 {{ include "storage-integrity.memberGrants" $ | indent 2 }}
 {{- end }}
+{{- if $access.memberDDL }}
+si_member_ddl/password_sha256_hex: {{ index $access.passwordSha256 "si_member_ddl" | quote }}
+si_member_ddl/networks/ip:
+  - "::/0"
+si_member_ddl/profile: si_member_ddl
+si_member_ddl/grants/query:
+{{ include "storage-integrity.grants.si_member_ddl" $ | indent 2 }}
+{{- end }}
 {{- end }}
 {{- end }}
 
@@ -257,6 +265,20 @@ tables. Called with (dict "root" $ "user" <user>). */ -}}
 {{- end }}
 {{- end }}
 
+{{- /* si_member_ddl's grants (housegate spec 2026-10-14 §8.2, stage 3 P6 step
+6; Plan S3-F contract §59.2; test_render.py SI_MEMBER_DDL_GRANTS keeps them
+equal): the member's reconciler creates its hg_member_unsafe database (without
+ENGINE, which ClickHouse 26.3 would check as TABLE ENGINE ON the database
+engine) and creates and drops its replicated replicas there, and nothing else:
+no INSERT, ALTER, OPTIMIZE, TRUNCATE or SYSTEM anywhere, no CREATE TABLE in any
+other database, nothing on hg_safe, hg_unsafe or hg_promote, no SYSTEM DROP
+REPLICA. Its profile (templates/clickhouse.yaml) holds the merge setting. */ -}}
+{{- define "storage-integrity.grants.si_member_ddl" -}}
+- "GRANT CREATE DATABASE ON hg_member_unsafe.*"
+- "GRANT CREATE TABLE, DROP TABLE ON hg_member_unsafe.*"
+- "GRANT TABLE ENGINE ON ReplicatedMergeTree"
+{{- end }}
+
 {{- /* si_member's grants (spec §8.2 as amended; Plan S3-C2 contract §23.2;
 test_render.py SI_MEMBER_GRANTS keeps the sixteen fixed entries equal): copies
 in hg_replica, scratch in hg_rstage with its seven rights named (never ALL: no
```

`charts/storage-integrity/templates/clickhouse.yaml` (the profile after `si_member`'s):

```diff
diff --git a/charts/storage-integrity/templates/clickhouse.yaml b/charts/storage-integrity/templates/clickhouse.yaml
--- a/charts/storage-integrity/templates/clickhouse.yaml
+++ b/charts/storage-integrity/templates/clickhouse.yaml
@@ -148,6 +148,14 @@ they hold for this installation from its own restart:
       si_member/max_threads: "4"
       si_member/priority: "10"
       {{- end }}
+      {{- if .Values.clickhouse.access.memberDDL }}
+      {{- /* Stage 3 P6 step 6 (spec §8.2; Plan S3-F contract §59.2): the
+      merge setting is const at the replicated_merge_tree default 0 of the
+      server bundle, so si_member_ddl's explicit 0 (ddl.UnsafeSettings) passes
+      and any other value is refused with code 452. */}}
+      si_member_ddl/profile: default
+      si_member_ddl/constraints/merge_tree_max_bytes_to_merge_at_max_space_in_pool/const: ""
+      {{- end }}
   templates:
     {{- /* The default template, then one per pinned cluster (Plan S3-D K55):
     the same pod with the chart's nodeSelector plus kubernetes.io/hostname. */}}
```

`charts/storage-integrity/templates/validate.yaml` (Plan S3-C3's refusal becomes the `memberDDL` rule; the exit alert's checks appended after the last line):

```diff
diff --git a/charts/storage-integrity/templates/validate.yaml b/charts/storage-integrity/templates/validate.yaml
--- a/charts/storage-integrity/templates/validate.yaml
+++ b/charts/storage-integrity/templates/validate.yaml
@@ -359,9 +359,24 @@ renders it.
 {{- end }}
 {{- end }}
 {{- $p2hashes := $p2access.passwordSha256 | default dict }}
-{{- if hasKey $p2hashes "si_member_ddl" }}
+{{- /* si_member_ddl (stage 3 P6 step 6; Plan S3-F contract §59.2): rendered
+exactly when memberDDL is set, and only on a locked installation whose member
+stage gave it si_member; its hash is required then and refused without it. */ -}}
+{{- $memberDDLHash := toString (index $p2hashes "si_member_ddl" | default "") }}
+{{- if and (ne $memberDDLHash "") (not $p2access.memberDDL) }}
 {{- fail "si_member_ddl is not created before stage 3 P6 step 6 (Plan S3-F)" }}
 {{- end }}
+{{- if $p2access.memberDDL }}
+{{- if not $p2access.lockDefault }}
+{{- fail "clickhouse.access.memberDDL requires clickhouse.access.lockDefault: si_member_ddl exists only on a locked installation" }}
+{{- end }}
+{{- if not $p2access.member }}
+{{- fail "clickhouse.access.memberDDL requires clickhouse.access.member: si_member_ddl comes at stage 3 P6 step 6, after the member stage restarted this installation with the replicated_merge_tree merge default 0 (spec §8.2)" }}
+{{- end }}
+{{- if not (regexMatch "^[0-9a-f]{64}$" $memberDDLHash) }}
+{{- fail "clickhouse.access.passwordSha256.si_member_ddl must be 64 lowercase hex characters" }}
+{{- end }}
+{{- end }}
 {{- $memberHash := toString (index $p2hashes "si_member" | default "") }}
 {{- if and (or $p2access.member (ne $memberHash "")) (not (regexMatch "^[0-9a-f]{64}$" $memberHash)) }}
 {{- fail "clickhouse.access.passwordSha256.si_member must be 64 lowercase hex characters" }}
@@ -441,3 +456,16 @@ whole number of seconds, checked whenever it is given.
 {{- if or (lt (float64 $heldAge) 1.0) (ne (float64 (int64 $heldAge)) (float64 $heldAge)) }}
 {{- fail "monitoring.stage3Consensus.heldCleanupAgeWarnSeconds must be a positive integer" }}
 {{- end }}
+{{- /*
+The stage-3 P6 exit alert (Plan S3-F contract §59.4, K87 (a)) is a group of
+the network's VMRule, so it needs monitoring itself; the bound is a whole
+number of seconds, checked whenever it is given.
+*/ -}}
+{{- $stage3 := .Values.monitoring.stage3 }}
+{{- if and $stage3.exitAlert (not .Values.monitoring.enabled) }}
+{{- fail "monitoring.stage3.exitAlert requires monitoring.enabled" }}
+{{- end }}
+{{- $exitBound := $stage3.heldCleanupAgeBoundSeconds }}
+{{- if or (lt (float64 $exitBound) 1.0) (ne (float64 (int64 $exitBound)) (float64 $exitBound)) }}
+{{- fail "monitoring.stage3.heldCleanupAgeBoundSeconds must be a positive integer" }}
+{{- end }}
```

`charts/storage-integrity/templates/monitoring.yaml` (the group before the file's last `{{- end }}`):

```diff
diff --git a/charts/storage-integrity/templates/monitoring.yaml b/charts/storage-integrity/templates/monitoring.yaml
--- a/charts/storage-integrity/templates/monitoring.yaml
+++ b/charts/storage-integrity/templates/monitoring.yaml
@@ -714,4 +714,24 @@ spec:
             summary: "A database group member has been Lagging for 30 minutes: more than max_lag_publications advances behind its owner, or marked lagging"
             runbook_url: "{{ $stage3Runbook }}#11-operating-rules"
     {{- end }}
+    {{- if .Values.monitoring.stage3.exitAlert }}
+    {{- $p6Runbook := .Values.monitoring.stage3.p6RunbookURL }}
+    {{- $exitBound := int64 .Values.monitoring.stage3.heldCleanupAgeBoundSeconds }}
+    # Stage 3 P6 exit (housegate spec 2026-10-14 §16 P6; Plan S3-F K87 (a),
+    # contract §59.4): the held-cleanup age is bounded while k = 2 holds for 7
+    # days. arbiter_floor_held_cleanup_age_seconds is the leader's alone (0 on a
+    # follower), so max reads the leader's value; a breach restarts the soak.
+    - name: {{ $fullname }}.stage3-p6
+      rules:
+        - alert: ArbiterFloorHeldCleanupAgeAboveExitBound
+          expr: |-
+            max by (cluster, network) (arbiter_floor_held_cleanup_age_seconds{network="{{ $network }}"}) > {{ $exitBound }}
+          for: 5m
+          labels:
+            severity: warning
+            network: {{ $network | quote }}
+          annotations:
+            summary: "the copy floor has held an unsafe-part cleanup for longer than the P6 exit bound"
+            runbook_url: "{{ $p6Runbook }}#131-daily-checks"
+    {{- end }}
 {{- end }}
```

`charts/storage-integrity/tests/clickhouse_grant_probe.tsv` (tabs between the three fields; two comment lines after Plan S3-D's, the thirteen rows right before the first `default` cleanup row):

```diff
diff --git a/charts/storage-integrity/tests/clickhouse_grant_probe.tsv b/charts/storage-integrity/tests/clickhouse_grant_probe.tsv
--- a/charts/storage-integrity/tests/clickhouse_grant_probe.tsv
+++ b/charts/storage-integrity/tests/clickhouse_grant_probe.tsv
@@ -10,6 +10,8 @@
 # expect override (si_member only): refused with code 36, Override not allowed for '<key>', after the access check. Plan S3-C1's si_restore rows ran on the P1 staging installation only, before any restart; the P2 staging run skips them.
 # Measured on clickhouse/clickhouse-server:26.3 (26.3.51.7) with the bundle: every refusal of a table engine names TABLE ENGINE ON <engine> and comes before ClickHouse touches Keeper; CREATE DATABASE ... ENGINE = Replicated is checked as TABLE ENGINE ON Replicated; a temporary table needs TABLE ENGINE ON Memory, remote(), s3() and BACKUP do not; CREATE TABLE ... AS needs SHOW COLUMNS on the source and its engine; ClickHouse grants CREATE VIEW with CREATE TABLE, so si_member's plain view and materialized view over hg_rstage are accepted (Plan S3-C2 contract §23.2), while a dictionary, dictGet, a definer, SQL SECURITY NONE and SYSTEM are refused.
 # Stage 3 P4 (Plan S3-D K52): the two s3() rows of si_snode above the cleanup need clickhouse.access.snodeRestoreGrant (a CHI manifest's GRANT READ ON S3 line): docs/storage-integrity-devnet2-stage3-p4-owner-rebuild.md §4.1 runs them on each installation once its restore grant is on, and every earlier run skips them. They take the form of Plan S3-C1's rows; measured on clickhouse/clickhouse-server:26.3 (26.3.51.7): without the grant the read answers 497, with it 499 (after the access check), and the write 497 either way (WRITE ON S3). A one-segment URL such as http://127.0.0.1:1/p4 answers 36 (bucket name too short) before the access check, for every user.
+# Stage 3 P6 (Plan S3-F contract §59.3): the si_member_ddl rows are run only by docs/storage-integrity-devnet2-stage3-p6-switches.md §8.2 (si_member_ddl_probe), on each live installation right after its si_member_ddl exists, in file order and alone: no setup row runs first, so they create hg_member_unsafe.probe_ddl, run every denial against that existing table (an INSERT or TRUNCATE of a missing table answers 60 before its access check), and drop it last. denied means code 497 only, never 60 or 81.
+# expect constrained (si_member_ddl only): refused with code 452 by the const merge_tree_max_bytes_to_merge_at_max_space_in_pool of its profile, after the access check; the table is never created. Every Keeper path is relative to the installation's root (/probe/si_member_ddl); the helper refuses to start while that path has children. Earlier corpus-wide helpers never run these rows: each runs only the roles its staging installation has.
 role	expect	statement
 default	ok	CREATE DATABASE IF NOT EXISTS hg_unsafe
 default	ok	CREATE DATABASE IF NOT EXISTS hg_safe
@@ -257,6 +259,19 @@ si_member	override	SELECT count() FROM remote(si_owner_staging, user = 'default'
 si_member	override	SELECT count() FROM remote(si_owner_staging, password = 'p2-probe', database = 'hg_safe', table = 'si_grant_probe')
 si_snode	ok	SELECT * FROM s3('http://127.0.0.1:1/probe.json', 'k', 's', 'RawBLOB') SETTINGS s3_retry_attempts = 0
 si_snode	denied	INSERT INTO FUNCTION s3('http://127.0.0.1:1/probe.json', 'k', 's', 'RawBLOB') SETTINGS async_insert = 0, s3_retry_attempts = 0 VALUES ('{}')
+si_member_ddl	ok	CREATE DATABASE IF NOT EXISTS hg_member_unsafe
+si_member_ddl	ok	CREATE TABLE hg_member_unsafe.probe_ddl (x UInt8) ENGINE = ReplicatedMergeTree('/probe/si_member_ddl/probe_ddl', 'probe') ORDER BY x SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0
+si_member_ddl	denied	INSERT INTO hg_member_unsafe.probe_ddl VALUES (1)
+si_member_ddl	denied	OPTIMIZE TABLE hg_member_unsafe.probe_ddl FINAL
+si_member_ddl	denied	TRUNCATE TABLE hg_member_unsafe.probe_ddl
+si_member_ddl	denied	ALTER TABLE hg_member_unsafe.probe_ddl DROP PARTITION tuple()
+si_member_ddl	denied	SYSTEM SYNC REPLICA hg_member_unsafe.probe_ddl
+si_member_ddl	denied	SYSTEM DROP REPLICA 'probe' FROM ZKPATH '/probe/si_member_ddl/none'
+si_member_ddl	denied	CREATE TABLE hg_member_unsafe.probe_mt (x UInt8) ENGINE = MergeTree ORDER BY x
+si_member_ddl	denied	CREATE TABLE hg_replica.probe_ddl (x UInt8) ENGINE = ReplicatedMergeTree('/probe/si_member_ddl/replica', 'probe') ORDER BY x
+si_member_ddl	denied	CREATE DATABASE probe_replicated ENGINE = Replicated('/probe/si_member_ddl/db', 's', 'r')
+si_member_ddl	constrained	CREATE TABLE hg_member_unsafe.probe_merge (x UInt8) ENGINE = ReplicatedMergeTree('/probe/si_member_ddl/probe_merge', 'probe') ORDER BY x SETTINGS max_bytes_to_merge_at_max_space_in_pool = 161061273600
+si_member_ddl	ok	DROP TABLE hg_member_unsafe.probe_ddl SYNC
 default	ok	DROP TABLE IF EXISTS hg_unsafe.si_grant_probe
 default	ok	DROP TABLE IF EXISTS hg_safe.si_grant_probe
 default	ok	DROP TABLE IF EXISTS hg_promote.si_grant_probe
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f
cd "$W" && python3 -I -c 'import json, sys; json.load(open(sys.argv[1]))' charts/storage-integrity/values.schema.json && echo "schema parses"
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | grep -E '^(FAIL|ERROR):|^Ran |^OK|^FAILED' | sed 's/ (__main__\.[^)]*)//' | sed -E 's/ \((release|installation|user)=.*//' | sort | uniq -c
T="$(mktemp -d)"
printf 'clickhouse: {access: {memberDDL: true}}\n' > "$T/o.yaml"
helm template s charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml -f "$T/o.yaml" --namespace storage-integrity-render-test 2>&1 | grep '^Error' | sed 's/validate.yaml:[0-9]*:[0-9]*/validate.yaml/'
printf 'monitoring: {stage3: {heldCleanupAgeBoundSeconds: 0}}\n' > "$T/o.yaml"
helm template s charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml -f "$T/o.yaml" --namespace storage-integrity-render-test 2>&1 | grep '^Error' | sed 's/validate.yaml:[0-9]*:[0-9]*/validate.yaml/'
rm -rf -- "$T"
bash scripts/storage-integrity/render-inertness.sh origin/main; echo "exit $?"
```

Expected: `schema parses`; then

```text
   1 ERROR: test_devnet2_member_secrets_are_per_host
   3 FAIL: test_logins_script_adds_one_user_and_only_a_known_one
   1 FAILED (failures=3, errors=1)
   1 Ran 257 tests in 68.812s
```

(the logins script learns `si_member_ddl` in Step 5); `Error: execution error at (storage-integrity/templates/validate.yaml): clickhouse.access.memberDDL requires clickhouse.access.lockDefault: si_member_ddl exists only on a locked installation`; `Error: execution error at (storage-integrity/templates/validate.yaml): monitoring.stage3.heldCleanupAgeBoundSeconds must be a positive integer`; the eight lines `<release> identical` and `exit 0`.

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f
git -C "$W" add charts/storage-integrity/values.yaml charts/storage-integrity/values.schema.json charts/storage-integrity/templates/_helpers.tpl charts/storage-integrity/templates/clickhouse.yaml charts/storage-integrity/templates/validate.yaml charts/storage-integrity/templates/monitoring.yaml charts/storage-integrity/tests/clickhouse_grant_probe.tsv charts/storage-integrity/tests/test_render.py && git -C "$W" commit -m "feat(storage-integrity): si_member_ddl with its const-merge profile and the P6 exit alert, off

clickhouse.access.memberDDL renders si_member_ddl with exactly CREATE
DATABASE, CREATE TABLE and DROP TABLE on hg_member_unsafe.* and TABLE
ENGINE ON ReplicatedMergeTree, and the profile si_member_ddl that holds
merge_tree_max_bytes_to_merge_at_max_space_in_pool const at the
replicated_merge_tree default 0, only on a locked installation at its P2
member stage (housegate spec 2026-10-14 §8.2, §10.4 step 6; Plan S3-F
contract §59.2). The grant probe gains its rows, in an order that keeps
every denial on an existing table, and the outcome constrained (452).
monitoring.stage3.exitAlert renders ArbiterFloorHeldCleanupAgeAboveExitBound
(K87 (a), contract §59.4). The render tests cover consensus mode, the
reconciler, the user and the exit alert and hold through the P6 commits.
Every release renders unchanged.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C "$W" log -1 --format='%an <%ae>'
```

Expected: one commit, `8 files changed`; `poetry <chen@sentio.xyz>`.

- [ ] **Step 5: The member-keys script and `si_member_ddl`'s logins**

Create `scripts/storage-integrity/make-member-keys.sh` (Step 5's commit makes it mode 755):

```bash
#!/usr/bin/env bash
# Creates the consensus-mode keys of one si-member of devnet2-si-v2 (housegate
# spec 2026-10-14 §6.1, §10.4 step 4; Plan S3-F contract §59.5, decision D40;
# docs/storage-integrity-devnet2-stage3-p6-switches.md §5.1). Run it in your own
# terminal (from fish too), from the root of the production checkout:
#
#   bash scripts/storage-integrity/make-member-keys.sh <a|b|c> <member-id> <public-file>
#   bash scripts/storage-integrity/make-member-keys.sh <a|b|c> <member-id> <public-file> <replica-signer-key-file>
#
# It makes the member's ed25519 seed (openssl rand -hex 32) and takes its
# replica_signer secp256k1 key either from Foundry's cast wallet new (three
# arguments) or from <replica-signer-key-file>, a key you made with another
# tool you prefer: a regular file of yours with no group or other permission
# bits, holding 64 hex characters (an optional 0x and newline). It never
# modifies or deletes that file; remove it yourself once this script has
# printed its line. Both keys go only into the Secret
# sentio-node-devnet2-indexer-<x>-si-member-keys (keys ed25519-seed and
# replica-signer-key, context sentio-sea, namespace sentio-network-devnet2),
# which charts/sentio-node mounts into the si-member container alone
# (siMember.consensus.keysSecret). It derives the ed25519 public key and the
# replica_signer address itself with OpenSSL (3 or later, with the KECCAK-256
# digest; macOS's LibreSSL is refused), checking each pipeline against a known
# vector first, and with cast also against the address cast printed. It writes
# the public values to <public-file>, one JSON object:
#
#   {"host":"<x>","node_id":"<member-id>","ed25519_pubkey":"<64 hex>","replica_signer":"0x<40 hex>","secret_uid":"<uid>"}
#
# It never prints a key and never passes one on a command line: the keys exist
# only in a mode-700 temporary directory that it removes on exit, and reach
# OpenSSL and kubectl through pipes and files. It refuses to overwrite an
# existing Secret or public file.
set -euo pipefail
umask 077
export LC_ALL=C

readonly context=sentio-sea
readonly namespace=sentio-network-devnet2
# secp256k1's group order n: a private key is in [1, n - 1].
readonly secp256k1_n=fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141

usage() {
  echo "usage: bash scripts/storage-integrity/make-member-keys.sh a|b|c <member-id> <public-file> [<replica-signer-key-file>]" >&2
  exit 2
}

[ "$#" -eq 3 ] || [ "$#" -eq 4 ] || usage
host=$1
member=$2
public_file=$3
key_file=${4:-}
case "$host" in
  a|b|c) ;;
  *) usage ;;
esac
[[ "$member" =~ ^member-[a-z0-9]+$ ]] || { echo "make-member-keys: <member-id> must match ^member-[a-z0-9]+\$" >&2; exit 2; }
secret="sentio-node-devnet2-indexer-${host}-si-member-keys"

tools="openssl kubectl jq xxd"
[ -n "$key_file" ] || tools="$tools cast"
for tool in $tools; do
  command -v "$tool" >/dev/null || { echo "make-member-keys: $tool is required" >&2; exit 1; }
done
version=$(openssl version)
[[ "$version" =~ ^OpenSSL\ ([3-9]|[1-9][0-9])\. ]] || { echo "make-member-keys: OpenSSL 3 or later is required (found: $version)" >&2; exit 1; }
[ "$(printf '' | openssl dgst -KECCAK-256 -r 2>/dev/null | cut -d ' ' -f 1)" = c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470 ] \
  || { echo "make-member-keys: this OpenSSL has no KECCAK-256 digest" >&2; exit 1; }
if [ -n "$key_file" ]; then
  [ -f "$key_file" ] && [ ! -L "$key_file" ] || { echo "make-member-keys: $key_file must be a regular file" >&2; exit 1; }
  [ -O "$key_file" ] || { echo "make-member-keys: $key_file must be owned by you" >&2; exit 1; }
  mode=$(stat -c '%a' "$key_file" 2>/dev/null || stat -f '%Lp' "$key_file")
  [ $((8#$mode & 8#077)) -eq 0 ] || { echo "make-member-keys: $key_file must have no group or other permission bits (chmod 600)" >&2; exit 1; }
fi
if [ -e "$public_file" ]; then
  echo "make-member-keys: $public_file exists; refusing to overwrite it" >&2
  exit 1
fi
[ -d "$(dirname "$public_file")" ] || { echo "make-member-keys: the directory of $public_file does not exist" >&2; exit 1; }

kube() {
  kubectl --context "$context" --namespace "$namespace" "$@"
}
if [ -n "$(kube get secret "$secret" --ignore-not-found -o name)" ]; then
  echo "make-member-keys: Secret $secret exists in $namespace; refusing to overwrite it (nothing was created)" >&2
  exit 1
fi

tmp=$(mktemp -d)
chmod 700 "$tmp"
trap 'rm -rf "$tmp"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ed25519: the RFC 8032 PKCS#8 wrapping of a seed, the public key its last 32
# bytes (production docs/storage-integrity-devnet2-multi-source.md §5.2).
ed25519_pubkey() { # stdin: the seed, 64 hex; stdout: the public key, 64 hex
  { printf '302e020100300506032b657004220420'; cat; } | xxd -r -p \
    | openssl pkey -inform DER -pubout -outform DER | tail -c 32 | xxd -p -c 64
}
# secp256k1: the SEC1 DER of a key, its uncompressed public key's 64 bytes
# through KECCAK-256, the address the last 20 bytes.
eth_address() { # stdin: the key, 64 hex; stdout: 0x and the address, 40 lowercase hex
  { printf '302e0201010420'; cat; printf 'a00706052b8104000a'; } | xxd -r -p \
    | openssl ec -inform DER -pubout -outform DER -conv_form uncompressed 2>/dev/null \
    | tail -c 64 | openssl dgst -KECCAK-256 -r | cut -d ' ' -f 1 | cut -c 25-64 | sed 's/^/0x/'
}
[ "$(printf '9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60' | ed25519_pubkey)" \
  = d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a ] \
  || { echo "make-member-keys: the ed25519 pipeline fails RFC 8032 test 1" >&2; exit 1; }
[ "$(printf '0000000000000000000000000000000000000000000000000000000000000001' | eth_address)" \
  = 0x7e5f4552091a69125d5dfcb7b8c2659029395bdf ] \
  || { echo "make-member-keys: the secp256k1 pipeline fails the key-1 vector" >&2; exit 1; }

openssl rand -hex 32 | tr -d '\n' > "$tmp/ed25519-seed"
grep -Eq '^[0-9a-f]{64}$' "$tmp/ed25519-seed" || { echo "make-member-keys: openssl did not produce 32 random bytes" >&2; exit 1; }

if [ -n "$key_file" ]; then
  if [ "$(grep -c '' "$key_file")" -ne 1 ] || ! grep -Eq '^(0x)?[0-9a-fA-F]{64}$' "$key_file"; then
    echo "make-member-keys: $key_file must hold one key of 64 hex characters (an optional 0x and newline)" >&2
    exit 1
  fi
  tr -d '\n' < "$key_file" | sed 's/^0x//' | tr 'A-F' 'a-f' > "$tmp/replica-signer-key"
else
  cast wallet new --json > "$tmp/cast.json"
  jq -r 'if type == "array" and length == 1 then .[0].private_key else error("cast wallet new --json printed an unexpected shape") end' \
    "$tmp/cast.json" | tr -d '\n' | sed 's/^0x//' > "$tmp/replica-signer-key"
  jq -r '.[0].address' "$tmp/cast.json" | tr 'A-F' 'a-f' | tr -d '\n' > "$tmp/cast-address"
fi
grep -Eq '^[0-9a-f]{64}$' "$tmp/replica-signer-key" || { echo "make-member-keys: the replica_signer key is not 64 hex characters" >&2; exit 1; }
# Fixed-width lowercase hex compares as text (LC_ALL=C) as it does as a
# number, so this is 0 < key < n.
key=$(cat "$tmp/replica-signer-key")
if [[ "$key" =~ ^0{64}$ ]] || ! [[ "$key" < "$secp256k1_n" ]]; then
  echo "make-member-keys: the replica_signer key is not in [1, n - 1] of secp256k1" >&2
  exit 1
fi
unset key

pubkey=$(ed25519_pubkey < "$tmp/ed25519-seed")
address=$(eth_address < "$tmp/replica-signer-key")
[[ "$pubkey" =~ ^[0-9a-f]{64}$ ]] || { echo "make-member-keys: no ed25519 public key derived" >&2; exit 1; }
[[ "$address" =~ ^0x[0-9a-f]{40}$ ]] || { echo "make-member-keys: no replica_signer address derived" >&2; exit 1; }
if [ -z "$key_file" ] && [ "$address" != "$(cat "$tmp/cast-address")" ]; then
  echo "make-member-keys: the derived address differs from the one cast wallet new printed" >&2
  exit 1
fi

kube create secret generic "$secret" \
  --from-file=ed25519-seed="$tmp/ed25519-seed" --from-file=replica-signer-key="$tmp/replica-signer-key" >/dev/null
uid=$(kube get secret "$secret" -o jsonpath='{.metadata.uid}')
[ -n "$uid" ] || { echo "make-member-keys: Secret $secret has no uid; check it with kubectl get secret" >&2; exit 1; }
jq -cn --arg host "$host" --arg node_id "$member" --arg ed25519_pubkey "$pubkey" --arg replica_signer "$address" --arg secret_uid "$uid" \
  '{host: $host, node_id: $node_id, ed25519_pubkey: $ed25519_pubkey, replica_signer: $replica_signer, secret_uid: $secret_uid}' \
  > "$tmp/public.json"
(set -C; cat "$tmp/public.json" > "$public_file")
echo "make-member-keys: created $secret for $member; public values in $public_file"
```

`scripts/storage-integrity/make-clickhouse-logins.sh` (`--add-user` learns `si_member_ddl`, whose Secret is its host's):

```diff
diff --git a/scripts/storage-integrity/make-clickhouse-logins.sh b/scripts/storage-integrity/make-clickhouse-logins.sh
--- a/scripts/storage-integrity/make-clickhouse-logins.sh
+++ b/scripts/storage-integrity/make-clickhouse-logins.sh
@@ -16,8 +16,13 @@
 # <hashes-file>. --add-user does the same for one more user of an installation
 # whose logins exist already: si_backup (stage 3 P1, runbook §6) on network,
 # indexer-b or indexer-c, whose password goes to the Secret
-# storage-integrity-si-v2-clickhouse-backup-<installation>. The hashes file
-# then holds that user only:
+# storage-integrity-si-v2-clickhouse-backup-<installation>; si_member_ddl
+# (stage 3 P6 step 6, docs/storage-integrity-devnet2-stage3-p6-switches.md
+# §8.1) on network, indexer-b or indexer-c, whose password goes to the Secret
+# sentio-node-devnet2-indexer-<x>-si-member-ddl (key password) of the host
+# whose source ClickHouse that installation is (indexer-a's for network), read
+# only by that host's si-member-reconciler. The hashes file then holds that
+# user only:
 #
 #   passwordSha256:
 #     <user>: "<64 lowercase hex>"
@@ -36,7 +41,7 @@ readonly namespace=sentio-network-devnet2
 
 usage() {
   echo "usage: bash scripts/storage-integrity/make-clickhouse-logins.sh network|indexer-b|indexer-c|staging|p1-staging <hashes-file>" >&2
-  echo "       bash scripts/storage-integrity/make-clickhouse-logins.sh --add-user si_backup network|indexer-b|indexer-c <hashes-file>" >&2
+  echo "       bash scripts/storage-integrity/make-clickhouse-logins.sh --add-user si_backup|si_member_ddl network|indexer-b|indexer-c <hashes-file>" >&2
   exit 2
 }
 
@@ -48,7 +53,7 @@ if [ "${1:-}" = --add-user ]; then
   hashes_file=$4
   # The users --add-user knows, and the installations each one may join.
   case "$added" in
-    si_backup)
+    si_backup|si_member_ddl)
       case "$installation" in
         network|indexer-b|indexer-c) ;;
         *) usage ;;
@@ -79,6 +84,10 @@ plan() {
   if [ -n "$added" ]; then
     case "$added" in
       si_backup) printf '%s %s %s %s\n' "storage-integrity-si-v2-clickhouse-backup-$installation" password si_backup plain ;;
+      si_member_ddl)
+        if [ "$installation" = network ]; then host=sentio-node-devnet2-indexer-a; else host="sentio-node-devnet2-$installation"; fi
+        printf '%s %s %s %s\n' "$host-si-member-ddl" password si_member_ddl plain
+        ;;
     esac
     return
   fi
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f
cd "$W" && chmod 755 scripts/storage-integrity/make-member-keys.sh
for f in scripts/storage-integrity/make-member-keys.sh scripts/storage-integrity/make-clickhouse-logins.sh; do bash -n "$f" && /bin/bash -n "$f" && shellcheck "$f" && echo "$f clean"; done
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
```

Expected: `scripts/storage-integrity/make-member-keys.sh clean`, `scripts/storage-integrity/make-clickhouse-logins.sh clean`; `Ran <N0+20> tests` and `OK`.

The member-keys script against local stand-ins of `kubectl` and `cast` (local only: no cluster, no Secret; the stand-in `cast` prints the secp256k1 key 2, whose address is `0x2b5ad5c4795c026514f8317c7a215e218dccd6cf`, and the key file holds the key 3, whose address is `0x6813eb9362372eef6200f3b1dbc3f819671cba69`). It needs an OpenSSL 3 or later with `KECCAK-256` first on `PATH` (Homebrew's `openssl` or `openssl@3`):

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f; T="$HOME/.cache/s3f-task83"
mkdir -p "$T/bin" && chmod 700 "$T"
cat > "$T/bin/kubectl" <<'PY'
#!/usr/bin/env python3
"""Stand-in for kubectl: logs each call, keeps the Secrets it creates, answers a uid."""
import json, os, sys
from pathlib import Path
state = Path(os.environ["KSTATE"])
args = sys.argv[1:]
with (state / "calls.jsonl").open("a") as log:
    log.write(json.dumps(args) + "\n")
if args[:4] != ["--context", "sentio-sea", "--namespace", "sentio-network-devnet2"]:
    sys.exit(f"stand-in kubectl: unexpected context or namespace {args}")
verb, path = args[4:], state / "secrets.json"
secrets = json.loads(path.read_text()) if path.exists() else {}
if verb[:2] == ["get", "secret"] and verb[3:] == ["--ignore-not-found", "-o", "name"]:
    if verb[2] in secrets or verb[2] in os.environ.get("KEXISTING", "").split():
        print(f"secret/{verb[2]}")
    sys.exit(0)
if verb[:2] == ["get", "secret"] and verb[3:] == ["-o", "jsonpath={.metadata.uid}"]:
    print(f"uid-of-{verb[2]}", end="")
    sys.exit(0)
if verb[:3] == ["create", "secret", "generic"]:
    secrets[verb[3]] = {flag.removeprefix("--from-file=").split("=", 1)[0]: Path(flag.split("=", 2)[2]).read_text() for flag in verb[4:]}
    path.write_text(json.dumps(secrets))
    sys.exit(0)
sys.exit(f"stand-in kubectl: unexpected call {args}")
PY
cat > "$T/bin/cast" <<'SH'
#!/bin/sh
# Stand-in for Foundry 1.x cast wallet new --json, with the secp256k1 key 2.
[ "$*" = "wallet new --json" ] || { echo "stand-in cast: unexpected cast $*" >&2; exit 1; }
printf '[{"address":"0x2B5AD5c4795c026514f8317c7a215E218DcCD6cF","private_key":"0x0000000000000000000000000000000000000000000000000000000000000002"}]\n'
SH
chmod 755 "$T/bin/kubectl" "$T/bin/cast"
cd "$W" || exit 1
run() { rm -rf "$T/state" "$T/out" && mkdir -p "$T/state" "$T/out" && env PATH="$T/bin:$PATH" KSTATE="$T/state" KEXISTING="${KEXISTING:-}" bash scripts/storage-integrity/make-member-keys.sh "$@" 2>&1 | sed "s#$T#T#g"; }
run b member-b "$T/out/member-public-b.json"; jq -c '{replica_signer, secret_uid}' "$T/out/member-public-b.json"
jq -r '.[] | keys[]' "$T/state/secrets.json" | sort | tr '\n' ' '; echo
grep -c '0000000000000000000000000000000000000000000000000000000000000002' "$T/state/calls.jsonl"
printf '0x%s\n' 0000000000000000000000000000000000000000000000000000000000000003 > "$T/key3" && chmod 600 "$T/key3"
run c member-c "$T/out/member-public-c.json" "$T/key3"; jq -r .replica_signer "$T/out/member-public-c.json"; ls "$T/key3"
chmod 644 "$T/key3"; run c member-c "$T/out/p.json" "$T/key3"
KEXISTING=sentio-node-devnet2-indexer-a-si-member-keys run a member-a "$T/out/p.json"
env PATH="$T/bin:/usr/bin:/bin" KSTATE="$T/state" bash scripts/storage-integrity/make-member-keys.sh a member-a "$T/out/p.json" 2>&1 | sed "s#$T#T#g"
rm -rf -- "$T/state" "$T/out" "$T/key3"
```

Expected: `make-member-keys: created sentio-node-devnet2-indexer-b-si-member-keys for member-b; public values in T/out/member-public-b.json`; `{"replica_signer":"0x2b5ad5c4795c026514f8317c7a215e218dccd6cf","secret_uid":"uid-of-sentio-node-devnet2-indexer-b-si-member-keys"}`; `ed25519-seed replica-signer-key`; `0` (the key in no call); the created line for member-c and `0x6813eb9362372eef6200f3b1dbc3f819671cba69`, with `T/key3` still there; `make-member-keys: T/key3 must have no group or other permission bits (chmod 600)`; `make-member-keys: Secret sentio-node-devnet2-indexer-a-si-member-keys exists in sentio-network-devnet2; refusing to overwrite it (nothing was created)`; and, with macOS's own `/usr/bin/openssl` first, `make-member-keys: OpenSSL 3 or later is required (found: LibreSSL 3.3.6)`.

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f
git -C "$W" add scripts/storage-integrity/make-member-keys.sh scripts/storage-integrity/make-clickhouse-logins.sh && git -C "$W" commit -m "feat(storage-integrity): member-keys script and si_member_ddl logins

make-member-keys.sh makes a member's ed25519 seed and takes its
replica_signer key from cast wallet new or from the user's own key file,
derives both public values with OpenSSL after checking each pipeline
against a known vector, and stores the keys only in the Secret
sentio-node-devnet2-indexer-<x>-si-member-keys; it writes the public
values to a file and never prints a key or puts one on a command line
(Plan S3-F contract §59.5, decision D40). make-clickhouse-logins.sh
--add-user si_member_ddl puts the password into the reconciler's Secret
sentio-node-devnet2-indexer-<x>-si-member-ddl. Run only by the user.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C "$W" log -1 --format='%an <%ae>'
```

Expected: one commit, `2 files changed`, `create mode 100755 scripts/storage-integrity/make-member-keys.sh`; `poetry <chen@sentio.xyz>`.

- [ ] **Step 6: Rehearse Tasks 86, 87 and 89's file changes (local only)**

Every live commit of Tasks 86, 87 and 89 moves one file and its mirror. This step makes each of them, in order, on a throwaway clone of the branch, runs the whole suite after each, and makes ten out-of-order or half-made changes that must fail. It writes only the clone under `$HOME/.cache/s3f-task83`, never the worktree, and pushes, syncs and applies nothing. At `origin/main` the devnet2 files hold the P5 end state (members on, every installation at its P2 member stage, the P5 images and rules), which the P6 commits need; a refusal of the first state (`C-b`) that names a P2 to P5 constant means `origin/main` is not at `P5_END`: stop and ask.

Write `$HOME/.cache/s3f-task83/rollout.py` with the Write tool (local, never committed):

```python
"""Rehearse the devnet2 file changes of Plan S3-F Tasks 86, 87 and 89 on a
scratch clone of the P6 branch (a local tool, never committed).

usage: rollout.py <scratch clone> forward|bites

forward: makes the single-service changes of contract §58.4 and §60 in their
order, one local commit each, and runs the whole test_render.py after each:
consensus mode per host b, c, a (C-b, C-c, C-a; Task 86, runbook §5.4),
si_member_ddl per installation indexer-b, indexer-c, network (D-b, D-c, D-n;
Task 87, §8.2), the reconcilers per host b, c, a (R-b, R-c, R-a; §8.3) and the
exit alert (X; Task 89, §12.5). Every state must print OK.

bites: from the right earlier state, makes each out-of-order or half-made
change a careless commit could make and requires test_render.py to fail.
Nothing is committed; the clone is reset after each case.

The clone is the only tree written. Nothing is pushed, synced or applied, and
every hash and consent is a stand-in.
"""
import hashlib
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(sys.argv[1]).resolve()
MODE = sys.argv[2]
TESTS = ROOT / "charts/storage-integrity/tests/test_render.py"
DEVNET2 = ROOT / "k8s-sea/sentio-network-devnet2"
NETWORK = DEVNET2 / "storage-integrity-si-v2.yaml"
CHIS = {"b": DEVNET2 / "storage-integrity-si-v2-indexer-b-source.yaml", "c": DEVNET2 / "storage-integrity-si-v2-indexer-c-source.yaml"}
CHI_NAMES = {"b": "storage-integrity-si-v2-indexer-b", "c": "storage-integrity-si-v2-indexer-c", "n": "storage-integrity-si-v2"}
sys.path.insert(0, str(ROOT / "charts/storage-integrity/tests"))
import test_render as t  # noqa: E402


def stand_in(label):
    return hashlib.sha256(f"rehearsal-{label}".encode()).hexdigest()


def git(*args):
    command = ["git", "-C", str(ROOT), "-c", "user.name=rehearsal", "-c", "user.email=rehearsal@localhost", "-c", "commit.gpgsign=false", *args]
    return subprocess.run(command, capture_output=True, text=True, check=True).stdout


def mirror(name, key, value):
    """Flip one entry of a dict constant of test_render.py."""
    text = TESTS.read_text()
    start = text.index(f"\n{name} = {{\n")
    end = text.index("\n}\n", start)
    block = text[start:end]
    old = re.compile(rf'^(    "{re.escape(key)}": )(True|False)(,)$', re.M)
    if len(old.findall(block)) != 1:
        sys.exit(f"rollout: {name}[{key}] is not one True/False line")
    TESTS.write_text(text[:start] + old.sub(lambda m: f"{m.group(1)}{value}{m.group(3)}", block) + text[end:])


def flag(name, value):
    text = TESTS.read_text()
    old = re.compile(rf"^{name} = (True|False)$", re.M)
    if len(old.findall(text)) != 1:
        sys.exit(f"rollout: {name} is not one boolean constant")
    TESTS.write_text(old.sub(f"{name} = {value}", text))


def host_overlay(x):
    return DEVNET2 / f"sentio-node-indexer-{x}-storage-integrity-si-v2.yaml"


def consensus(x, keys=None, constant=True):
    """Task 86 §5.4: member-<x>'s consensus mode with its keys and its host's consent."""
    name = f"sentio-node-devnet2-indexer-{x}"
    path = host_overlay(x)
    text = path.read_text().rstrip("\n")
    path.write_text(text + f"""
  # Stage 3 P6 step 4 (docs/storage-integrity-devnet2-stage3-p6-switches.md
  # §5.4): member-{x} in consensus mode with its own keys and indexer-{x}'s consent.
  consensus:
    enabled: true
    keysSecret:
      name: {keys or t.DEVNET2_MEMBER_KEY_SECRETS[name]}
    consentJWS: {t.TEST_CONSENT_JWS}
""")
    if constant:
        mirror("DEVNET2_MEMBER_CONSENSUS", name, True)


def member_ddl(x, constant=True):
    """Task 87 §8.2: si_member_ddl on installation <x> (b, c or n)."""
    digest = stand_in(f"member-ddl-{x}")
    if x == "n":
        NETWORK.write_text(t.p6_overlay_text(NETWORK.read_text(), digest))
    else:
        CHIS[x].write_text(t.p6_manifest_text(CHIS[x].read_text(), digest))
    if constant:
        mirror("DEVNET2_MEMBER_DDL_USERS", CHI_NAMES[x], True)


def reconciler(x, secret=None, constant=True):
    """Task 87 §8.3: member-<x>'s reconciler with its si_member_ddl Secret."""
    name = f"sentio-node-devnet2-indexer-{x}"
    path = host_overlay(x)
    text = path.read_text().rstrip("\n")
    path.write_text(text + f"""
  # Stage 3 P6 step 6 (runbook §8.3): member-{x}'s hg_member_unsafe replicas, as si_member_ddl.
  reconciler:
    enabled: true
    passwordSecret:
      name: {secret or t.DEVNET2_MEMBER_DDL_SECRETS[name]}
""")
    if constant:
        mirror("DEVNET2_MEMBER_RECONCILER", name, True)


def exit_alert(constant=True):
    """Task 89 §12.5: monitoring.stage3.exitAlert on devnet2-si-v2."""
    text = NETWORK.read_text()
    anchor = "\nmonitoring:\n  enabled: true\n"
    if text.count(anchor) != 1:
        sys.exit("rollout: the network overlay's monitoring block does not start with enabled: true")
    NETWORK.write_text(text.replace(anchor, anchor + "  # Stage 3 P6 exit (runbook §12.5, §13): the held-cleanup age bound.\n  stage3:\n    exitAlert: true\n"))
    if constant:
        flag("DEVNET2_P6_EXIT_ALERT", True)


FORWARD = [
    ("C-b", lambda: consensus("b")), ("C-c", lambda: consensus("c")), ("C-a", lambda: consensus("a")),
    ("D-b", lambda: member_ddl("b")), ("D-c", lambda: member_ddl("c")), ("D-n", lambda: member_ddl("n")),
    ("R-b", lambda: reconciler("b")), ("R-c", lambda: reconciler("c")), ("R-a", lambda: reconciler("a")),
    ("X", lambda: exit_alert()),
]
START = git("rev-parse", "HEAD").strip()


def run_tests(*classes):
    result = subprocess.run(["python3", "-B", str(TESTS), *classes], capture_output=True, text=True, cwd=ROOT)
    return result.returncode, result.stderr


def forward():
    for label, change in FORWARD:
        change()
        git("commit", "-q", "-am", f"rehearsal {label}")
        code, output = run_tests()
        ran = re.search(r"^Ran (\d+) tests", output, re.M)
        print(f"{label}: {'OK' if code == 0 else 'FAILED'} ({ran.group(1) if ran else '?'} tests)", flush=True)
        if code:
            print(output[-4000:])
            sys.exit(1)


def upto(count):
    """Reset the clone to the start and make the first count changes of FORWARD."""
    git("reset", "-q", "--hard", START)
    for _, change in FORWARD[:count]:
        change()


# (how many FORWARD changes come first, the careless change, what it is)
BITES = [
    (0, lambda: consensus("c"), "consensus mode out of order (c before b)"),
    (0, lambda: consensus("b", keys="sentio-node-devnet2-indexer-c-si-member-keys"), "another host's keys Secret"),
    (0, lambda: consensus("b", constant=False), "consensus mode without its mirror"),
    (2, lambda: member_ddl("b"), "si_member_ddl before every member's consensus mode"),
    (3, lambda: member_ddl("c"), "si_member_ddl out of order (c before b)"),
    (3, lambda: member_ddl("b", constant=False), "si_member_ddl in a manifest without its mirror"),
    (4, lambda: reconciler("b"), "a reconciler before every installation has si_member_ddl"),
    (6, lambda: reconciler("b", secret="storage-integrity-si-v2-member-indexer-b"), "the si_member password Secret as the reconciler's"),
    (6, lambda: reconciler("c"), "reconciler out of order (c before b)"),
    (8, lambda: exit_alert(), "the exit alert before every reconciler"),
]


def bites():
    # Each bite starts from the right earlier state, so only the bite is wrong.
    for count, change, what in BITES:
        upto(count)
        change()
        code, output = run_tests("ActualFreshNetworkTest", "MemberConsensusTest", "SIMemberTest", "P2ServerTest")
        first = next((line for line in output.splitlines() if line.startswith(("FAIL:", "ERROR:"))), "no failing test")
        print(f"{'refused' if code else 'ACCEPTED'}: {what}: {first[:160]}", flush=True)
        git("reset", "-q", "--hard", START)
        if not code:
            sys.exit(1)


{"forward": forward, "bites": bites}[MODE]()
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f; T="$HOME/.cache/s3f-task83"
R="$(mktemp -d "$T/rehearse.XXXXXX")" && printf '%s\n' "$R" > "$T/rehearse-path"
git clone -q --no-hardlinks --branch urwt/claude/multi-source-s3f "$W" "$R/repo"
python3 -I -B "$T/rollout.py" "$R/repo" bites
```

Expected: ten lines, each `refused: <what>: <the first failing test>`: consensus mode out of order (c before b), another host's keys Secret, consensus mode without its mirror, `si_member_ddl` before every member's consensus mode, `si_member_ddl` out of order (c before b), `si_member_ddl` in a manifest without its mirror, a reconciler before every installation has `si_member_ddl`, the `si_member` password Secret as the reconciler's, reconciler out of order (c before b), the exit alert before every reconciler; each names the rollout test that refuses it (`test_devnet2_member_consensus_follows_the_rollout`, `…_ddl_users_…`, `…_reconcilers_…`, `test_devnet2_p6_exit_alert_follows_the_rollout`), except the `si_member` password Secret, which the chart itself refuses, so its first failure is `ERROR: setUpClass (__main__.ActualFreshNetworkTest)`. An `ACCEPTED` line names a commit the tests would let through: fix the test in the worktree, commit, and run this step again with a new clone.

The forward run takes about thirteen minutes (ten full suites); run it in the background and read its output when it exits:

```bash
T="$HOME/.cache/s3f-task83"; R="$(cat "$T/rehearse-path")"
python3 -I -B "$T/rollout.py" "$R/repo" forward
git -C "$R/repo" log --oneline -10 | cat
```

Expected: ten lines `C-b: OK (<N0+20> tests)`, `C-c: OK (…)`, `C-a: OK (…)`, `D-b: OK (…)`, `D-c: OK (…)`, `D-n: OK (…)`, `R-b: OK (…)`, `R-c: OK (…)`, `R-a: OK (…)`, `X: OK (…)`, then the ten `rehearsal <label>` commits. A `FAILED` state prints the suite's tail: one of the P6 commits would turn the suite red; fix the test or the helper in the worktree, commit, and run this step again with a new clone.

The clone stays under `$HOME/.cache/s3f-task83` (local, rehearsal commits only); nothing reads it again, and a new run of this step makes a new one. The user may delete it.

- [ ] **Step 7: Full production checks**

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3f
cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
helm lint charts/storage-integrity -f charts/storage-integrity/tests/legacy-devnet2-values.yaml --namespace sentio-network-devnet2 --strict | tail -1
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --strict | tail -1
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --set arbiter.consensusUpdatesEnabled=true --set arbiter.authoritySigningEnabled=false --strict | tail -1
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --set monitoring.enabled=true --set monitoring.network=probe --set monitoring.stage3.exitAlert=true --strict | tail -1
helm lint charts/sentio-node --strict | tail -1
bash scripts/storage-integrity/render-inertness.sh origin/main; echo "exit $?"
for f in scripts/storage-integrity/make-member-keys.sh scripts/storage-integrity/make-clickhouse-logins.sh; do bash -n "$f" && shellcheck -S warning "$f" && echo "$f clean"; done
git -C "$W" status --porcelain | wc -l | tr -d ' '
git -C "$W" log --format='%an <%ae> %s' origin/main..HEAD | cat
```

Expected: `Ran <N0+20> tests` and `OK`; five times `1 chart(s) linted, 0 chart(s) failed`; the eight lines `<release> identical` and `exit 0`; both scripts `clean`; `0`; three lines, each `poetry <chen@sentio.xyz> feat(…)`, the commits of Steps 3, 4 and 5. Nothing is pushed: Task 84 adds the runbook to this branch, runs these checks again with the runbook checker, pushes and opens `P6_PR`, each remote step confirmed in chat.

---

## Appendix A: binding contract (continues the Appendix A of Plans S3-A to S3-E4)

### 58. What Plan S3-F changes and what it consumes (Tasks 83–89)

Appendix A of Plans S3-A to S3-E4 (§0–§57) stays binding; this appendix continues the numbering, so `contract §N` means the same thing in every stage-3 plan. Every name below is binding verbatim for every task of Plan S3-F. A task that needs a name not listed here adds it in its own section and records it in Appendix B; it never renames one listed here.

**58.1 Activation model (read first).**
- No code and no release. Nothing in this plan changes a repository other than production and the housegate plan file's Appendix C; every behaviour turned on shipped in `ARBITER_TAG_P5`, `CORE_TAG_P5`, `HG_TAG_P5` and `SN_COMMIT_P5` (S3-E1 K66), inert until its switch. The member side this plan switches on (the consent command, the member consensus mode, the `--role reconciler` process, the member consensus metrics and the host's keeper-findings export) is S3-E4 Task 79's, whose widened scope exists so that this plan needs no code (decision D30). The throwaway tool `stage3reads` (§59.7) is built from `CORE_TAG_P5` and never committed.
- Consensus: nine authority-signed `ConsensusParamsUpdate`s (seven switches, the step-4 `verifiers` update, the step-10 `min_safe_copies` raise), all inside snapshot v21; no Raft tag, field, snapshot version, header byte or feature string changes. `group rebalance` and `member lagging` (tags 38 and 42, through `ConsensusAdmin` methods 16 and 17 of 18, D26) are used only by the forward fixes and the operator playbook, never by a planned step.
- Images: only the member pods move, to `SN_DIGEST_P5`, with their consensus mode (Task 86). Voters, verifiers and hosts keep the images P5 left.
- Every planned step is one-way (§58.4); a failure is handled forward (runbook §15), never by rolling an image or a value back.

**58.2 Consumed names whose spelling the spec or the scoping proposal fixes (binding).**

| Name | What | Owner |
|---|---|---|
| `INDEXER_C_ID` | indexer-c's contract-assigned SI indexer id | S3-B §14 (Task 21) |
| `MEMBER_IDS` | the member node id of each host (`member-a` on indexer-a, `member-b` on indexer-b, `member-c` on indexer-c, K29) | S3-C3 §25–§28 |
| `UNSAFE_INC_ROOT` | the created, empty Keeper znode `/storage-integrity/devnet2/devnet2-si-v2/sentio/0/unsafe_inc` (the value recorded; `/sentio/0/unsafe_inc` as ClickHouse addresses it under its Keeper root, K42) | S3-C3 §28.1 (Task 40) |
| `P2_END` | the end of P2 on devnet2 | S3-C3 §25–§28 |
| `DEVNET2_FAILURE_DOMAINS` | each SI indexer's source-ClickHouse node as `node:<hostname>` (K55) | S3-D §33–§37 |
| `P4_END` | the end of P4 on devnet2 | S3-D §33–§37 |
| `HG_TAG_P5`, `CORE_TAG_P5` | the P5 housegate and arbiter-core releases | S3-E1 §38–§43 |
| `ARBITER_TAG_P5` / `ARBITER_DIGEST_P5` | the P5 arbiter release (Tasks 64–78) and its image index digest | S3-E4 §53–§57 (Task 78) |
| `SN_COMMIT_P5` / `SN_DIGEST_P5` | the P5 sentio-node commit and image digest | S3-E4 §53–§57 (Task 79) |
| `ACTIVATION_EPOCH`, `P5_END` | the v21 activation epoch and the end of P5 | S3-E4 §53–§57 (Task 82) |
| `stage3` fields | `group_size`, `network_fill_default`, `min_safe_copies`, `strict_copies`, `indexer_failure_domains`, `max_lag_publications`, `lag_low`, `evidence_timeout_l2_blocks`, `max_uncleaned_parts_per_partition`, `max_uncleaned_parts_per_table`, `exclude_quarantined_hosts`; switches `purge`, `eviction_voiding`, `owner_aware_selection`, `placement`, `reselection`, `checkpoints`, `narrowed_drain` (spec §6.1, §10.4) | S3-E1 §39 (`Stage3Params` 1–18) |
| `verifiers` entry fields | `host_indexer_id` (optional, field 3), `replica_signer`, `consent_jws`, `weight`, `failure_domain`, `verify`, `replicate`, `counts`, `shared_rmt` (spec §6.1; K58: explicit on host-bound entries, absent on pool entries) | S3-E1 §39 |
| Group records | `DatabaseGroup{owner_indexer_id, database_id, members, network_fill, generation, past_members, leaving, group_key, placement_salt}` in `TableRegistrySnapshot.groups = 10`, `member_checkpoint_seqs = 11`, `stage3 = 12`; `GroupMember{node_id, joined_generation, ready_generation, member_class, checkpoint, observed_entry, lag_advances, host_indexer_id}` (`host_indexer_id` filled at serve time from the member's `VerifierEntry`, fixed for the entry's life, never stored in FSM state and not Raft-reachable, D27; `lag_advances` saturates at `max_lag_publications + 1`, the value at which a CaughtUp or Behind member becomes Lagging, D31), `LeavingMember{node_id, removed_generation, release_entry, frozen_members, frozen_domains, released}`; `TableIncarnation.scoped_keeper_path = 19`, `purge_members = 20`; classes `MEMBER_CLASS_BOOTSTRAPPING`, `_CAUGHT_UP`, `_BEHIND`, `_LAGGING` (a Leaving member appears only in `leaving`) | S3-E1 §39 (`host_indexer_id`, D27), S3-E3 §50–§52 (`lag_advances`, D31) |
| Block reads | `L3BlockTrack.group_key = 2`; `BlockVerification` fields `eligible_ring` 14, `ring_position` 15, `marked_cursor_block` 16, `group_key` 17, `group_members` 18, `group_fill` 19, `group_databases` 20, `reselections` 21, `reselections_at_latch` 22 | S3-E1 §39 |
| Admission codes | `ADMISSION_CODE_VERIFIERS_UNAVAILABLE = 12`, `ADMISSION_CODE_COPIES_BACKPRESSURE = 13` | S3-E1 §39 (K59) |
| Feature and purposes | `stage3_v1`; `arbiter-group-member-v1` (consent, ES256K by the host's `si_indexers` signer), `arbiter-member-ready-v1` (ed25519), `arbiter-member-checkpoint-v1` (ES256K `replica_signer`), `arbiter-rebalance-group-v1`, `arbiter-reselect-verifiers-v1`, `arbiter-mark-member-lagging-v1`; placement and key digests `arbiter-group-placement-v1`, `arbiter-group-key-v1` | S3-E1 §38–§43 (K63), spec §6.3, §7.1 |
| Commands and RPCs | `RebalanceGroup` (tag 38), `ReselectVerifiers` (39), `MemberReady` (40), `MemberCheckpoint` (41), `MarkMemberLagging` (42); `ConsensusAdmin.RebalanceGroup` (method 16), `ConsensusAdmin.MarkMemberLagging` (method 17; 18 methods, after S3-D's `SettleTrack` 14 and `GetTrackSettle` 15 on `PROTO_TAG_P4`, D26); `VerifierGateway.SubmitMemberReady`, `SubmitMemberCheckpoint`; no `GetStage3State` (K64): `GetConsensusParams`, `GetTableRegistry`, `GetBlockVerification` carry every runbook read | S3-E1 §39, S3-E3 §49–§52 |
| arbiter-admin commands | `arbiter-admin consensus groups`, `arbiter-admin group rebalance`, `arbiter-admin member lagging`, the local placement preview; `consensus update --stage3` and its switch flags, the extended `--verifier`, `--indexer-failure-domain`; default `--hold issuance` once the leader reports `narrowed_drain` | S3-E2 §45 (Task 65), S3-E3 (Tasks 71, 75), S3-E4 (Task 77) |
| Metrics | `arbiter_block_seal_reason{reason}` with `group_switch` (spec §7.2), `arbiter_group_members{owner,class}`, `arbiter_floor_held_cleanup_age_seconds` (K78), `arbiter_snapshot_encoding_fallback_total`; `sentio_node_si_member_shadow_{blocks,mismatches,missed_scans}_total`; the member consensus series `sentio_node_si_member_consensus_registered`, `sentio_node_si_member_ready_groups`, `sentio_node_si_member_checkpoint_seq{owner}`, `sentio_node_si_member_checkpoints_total{owner,result}` (`accepted`, `switch_off`, `refused`) on the `si-member` registry, the reconciler's `sentio_node_si_member_unsafe_replicas` and `sentio_node_si_member_replica_already_exists_total`, and the host's `sentio_node_storage_integrity_keeper_findings{reason}` (`unknown_replica`, `non_incarnation_root`, both always exported) on the embedded HouseGate registry `:9091` | S3-E3 (Tasks 71–72), S3-E4 (Task 76), S3-E2 (Task 67), S3-C2 (§24.4); the member consensus, reconciler and keeper-findings series named by S3-E1 §42.1 and §42.3 and registered or exported by S3-E4 Task 79 (§56, D30) |
| sentio-node | `sentio-node si-member --config-path` (separate process), the `storage_integrity.member` config block, `sentio-node storage-integrity member-consent-statement` (the enrolment-statement pattern), the member consensus-mode config keys (ed25519 seed file, `replica_signer` key file, consent file) | S3-C2 (Task 35), S3-E4 (Task 79) |
| production | the `siMember` StatefulSet of each host release with its own ConfigMap, PVC, checksum, image pin, metrics, status Service and bearer token, containers `si-member` (as `si_member`) and the `si-member-reconciler` slot (off until step 6); the `si_member` user and its Secret `storage-integrity-si-v2-member-<installation>` (key `clickhouse-password`) made by `make-p2-logins.sh`; the member status token in its own Secret `<release>-si-member-status-token` (`sentio-node-devnet2-indexer-<x>-si-member-status-token`), never the `si_member` password Secret, mounted by the host's `sentio-node` (D29; how S3-C3 and S3-C4 make and mount it is theirs, and this plan only keeps every member Secret apart from it); the member alerts | S3-C3 (Tasks 37–38), S3-C4 (Task 47), D29 |
| Runbooks | P5 runbook `docs/storage-integrity-devnet2-stage3-p5-v21.md` (the start of this runbook's §0 chain), P2 runbook `docs/storage-integrity-devnet2-stage3-p2-members.md`, P3 runbook `docs/storage-integrity-devnet2-stage3-p3-replica-reads.md`; S3-B's runbook §13.4 budget estimator as fixed in housegate#237 | S3-C3, S3-C4, S3-E4 |
| Ledger tool | `$HOME/.cache/s3-ledger/s3-ledger.py` with `ref`, `show`, `get`, `empty`, `set` and S3-B's record procedure | S3-C1 §19 (Task 22) |
| Earlier names | `--hold none\|issuance\|drain`, `--hold-timeout`, `consensus hold-status`, `consensus hold-release` (S3-A §4); `si_ch_admin`, `si_grant_probe`, `clickhouse_grant_probe.tsv`, `SI_CLICKHOUSE_GRANTS`, `clickhouse.access.*`, `DEVNET2_CLICKHOUSE_POSTURE`, `render-inertness.sh`, `MONITORING_ALERTS`, `monitoring.*`, `STAGE3_HOST_IMAGES` (S3-B §9, §11, §15) | S3-A §4, S3-B §9–§15 |

**58.3 Names this plan needs whose spelling the owning plan's contract fixes.** The scoping proposal names these capabilities but not their spelling. The spelling below is the owning plan's, as the cross-plan consistency pass reconciled it; where an owning contract later changes one, the owner's spelling wins and this plan (the runbook, the helpers and the tests) is rewritten before Task 83 starts.

| Capability | Spelling used here | Owner |
|---|---|---|
| One switch on, every other `stage3` field carried from the committed params | `consensus update --stage3-switch <name>` with `<name>` one of `purge`, `eviction_voiding`, `owner_aware_selection`, `placement`, `reselection`, `checkpoints`, `narrowed_drain` (one switch per update; the committed `stage3` and host-bound entries are carried) | S3-E2 §45 (Task 65) |
| Raise k | `consensus update --stage3-min-safe-copies 2` | S3-E2 §45 (Task 65) |
| A host-bound entry | `--verifier "node=<id>,pubkey=<64 hex>,host=<indexer id>,replica-signer=0x<40 hex>,consent=<path>,weight=1,domain=<failure domain>,verify=true,replicate=true,counts=true,shared-rmt=true"`; a pool entry keeps `node=<id>,pubkey=<64 hex>` | S3-E2 §45 (Task 65) |
| Whether a switch update needs `--voter` | not passed (only the activation's stage3 gate probes voters) | S3-E2 §45 |
| Committed `stage3` in `consensus show` | `.current.stage3.<field>` (proto field names, absent booleans read as false), read by S3-E4's helper `si_stage3` | S3-E2 §45 (Task 65), S3-E4 §57 |
| The placement cutover block | `consensus show`'s `.stage3_state.switches[]` entry whose `switch_name` is `placement`, field `cutover_block_seq` (`Stage3SwitchCutover`, recorded after the switch-on's own seal), cross-checked on block headers | S3-E1 §39, S3-E2 §45 (Task 65), S3-E3 §51 |
| Groups view | `arbiter-admin consensus groups --data-address <leader data-plane address>` (JSON: `registry_version`, `groups`, `member_checkpoint_seqs`) | S3-E3 §50 (Task 71) |
| Placement preview | `arbiter-admin group preview --address <leader> --data-address <leader data-plane address>` (JSON, the exported pure `PlaceGroup` over the committed entries) | S3-E3 §50 (Task 71) |
| Playbook commands | `arbiter-admin group rebalance --address <leader> --owner <id> --database <db> --expected-generation <g> [--add <node>]... [--remove <node>]... --network-fill true\|false`; `arbiter-admin member lagging --address <leader> --node <id> --owner <id> --database <db> --observed-safe-block-seq <s> --observed-applied-promotion-seq <p>` | S3-E3 §50, §52 (Tasks 71, 75) |
| Refusal texts the runbook quotes | the switch-prerequisite refusal `consensus update cannot turn on stage3 switch %s before %s`, the host-bound-entry refusal `consensus update cannot list host-bound verifier %s before the stage3 owner_aware_selection switch is committed` (K68), the k-raise precondition refusal `consensus update raises stage3.min_safe_copies to %d: …`, `VERIFIERS_UNAVAILABLE` | S3-E2 §45, S3-E4 §53 |
| Consent statement | `/sentio-node --config-path /config/standalone.yaml storage-integrity member-consent-statement --member-node-id <id> --member-ed25519-pubkey <64 hex> --member-replica-signer 0x<40 hex>`, JWS alone on stdout | S3-E4 §56 (Task 79) |
| Member consensus-mode config | `storage_integrity.member.consensus.register`, `.create_replicas`, `.ed25519_seed_file`, `.replica_signer_key_file`, `.consent_file` | S3-E4 §56 (Task 79) |
| Reconciler process | `sentio-node si-member --config-path <path> --role reconciler`, config `storage_integrity.member.reconciler.clickhouse_username`, password env `STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD`, `create_replicas` from `storage_integrity.member.consensus` | S3-E4 §56 (Task 79), S3-E1 §42.3 (Task 62) |
| Replica hosts after placement | each host's status RPC `replica_hosts` per database from the registry's `DatabaseGroup`: the host of every current member, whatever its `ready_generation` (`MemberReady` attests only `hg_member_unsafe` replicas), taken from the served `GroupMember.host_indexer_id` (D27; no `hostOf` mapping from the `verifiers` entries; a member served without one skipped, Leaving records never listed) | S3-E1 §39 (D27), S3-E4 §56 (Task 79) |
| Member status endpoint fields | S3-C4's `serve.ServingView` (`GET /v1/serving`): `node_id`, `databases`, and S3-E1's `consensus` with `mode`, `registered` and per group `owner_indexer_id`, `database_id`, `generation`, `ready`, `class`, `checkpoint_seq`; read with S3-C4's `si_member_serving a\|b\|c` (run in the host's `sentio-node` container, which holds the status token of D29; no agent block reads the token). The status carries no key: a member registered and Active as a VERIFIER holds its listed `ed25519_pubkey` (registration requires the listed key, `verifierSignatureLocked`, S3-E2 K68) | S3-C4 §30.3 (Task 45), S3-E1 §42.3 (Task 63) |
| `siMember` values | `siMember.enabled`, `siMember.image`, `siMember.reconciler.enabled`, `siMember.secret` (`{name, passwordKey: clickhouse-password}`, the `si_member` password Secret `storage-integrity-si-v2-member-<installation>`, which no longer holds the status token, D29); the status token Secret `<release>-si-member-status-token` (D29; the value that names it is S3-C3's and S3-C4's spelling, which this plan takes unchanged); StatefulSet `<fullname>-si-member`, pod `<fullname>-si-member-0`; template `charts/sentio-node/templates/si-member.yaml`; the member ConfigMap `<fullname>-si-member-config` (key `si-member.yaml`) mounted read-only at `/config` and the state volume `si-member-state` at `/data/si-member` | S3-C3 §26 (Task 38) |
| Single-user logins | `make-clickhouse-logins.sh --add-user <user> <network\|indexer-b\|indexer-c> <hashes-file>` (the single-user mode S3-C1 adds for `si_backup`) | S3-C1 §18 (Task 26) |
| test constants | `DEVNET2_MEMBER_IDS` (release → member id); `DEVNET2_CLICKHOUSE_NODES` and `DEVNET2_NODE_PINS` (the nodes `DEVNET2_FAILURE_DOMAINS` names); `DEVNET2_STAGE3_P5_SOURCE_IMAGE` (`ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT_P5>@<SN_DIGEST_P5>`, in `STAGE3_HOST_IMAGES`); test `test_si_clickhouses_run_on_distinct_nodes` | S3-C3 §26, S3-D §36.5 (Task 53), S3-E4 §56 (Task 80) |
| Ledger value forms | `MEMBER_IDS` = `member-a:0,member-b:1,member-c:<INDEXER_C_ID>` (member id `:` host indexer id); `DEVNET2_FAILURE_DOMAINS` = `<indexer id>=node:<hostname>` pairs joined by `,` in indexer-id order; the epochs and blocks unsigned integers | S3-C3 §28.1, S3-D §37.2, S3-C1 §19 |

**Served fields only (D33).** Every read of this plan uses a field the arbiter or a member serves at `ARBITER_TAG_P5` / `SN_COMMIT_P5`: `GetConsensusParams` (`.current.stage3`, `.stage3_state` with `activation_*` and per switch `switch_name`, `epoch`, `cutover_block_seq`, `cutover_promotion_seq`), `GetTableRegistry` (incarnations with `scoped_keeper_path` and `purge_members`, `groups` with the `GroupMember` and `LeavingMember` fields of §58.2, `member_checkpoint_seqs`, `stage3`), `GetBlockVerification` (fields 14–22, `reselections` as served), the member's `ServingView` and the series of §58.2. It reads none of the fields D33 leaves unserved: the cutover's `IncarnationSeq` (S3-E2), `GroupView.ReadyVerifyMembers` (S3-E3; a group's ready verify members are derived from each member's `ready_generation` and its `verifiers` entry's `verify` flag, as S3-E3 §51 says readers do) and `Reselection.ExpectedSet` (S3-E3; a block's `reselections` are printed without it). This plan writes no code, so a read it would need beyond these is added to the owning plan's view and recorded there, never here.

**58.4 Steps, holds and records (binding order).**

| Step | Change | Hold | Prerequisites checked before signing | Runbook | Task | Ledger |
|---|---|---|---|---|---|---|
| 1 | `stage3.purge` on | drain | every registered, non-evicted data-plane node advertises `stage3_v1`; no host-bound `verifiers` entry; `UNSAFE_INC_ROOT` exists with no child | §2 | 85 | `PURGE_EPOCH` |
| 2 | `stage3.eviction_voiding` on | drain | `purge` on; no member registered | §3 | 85 | `VOIDING_EPOCH` |
| 3 | `stage3.owner_aware_selection` on | drain | `eviction_voiding` on; every SI host on `SN_COMMIT_P5` (K84); three Active pool verifiers | §4 | 85 | `SELECTION_EPOCH` |
| 4 | `verifiers` = the three pool entries restated byte-identically plus `member-a`, `member-b`, `member-c`; then consensus mode per host b, c, a | drain | `owner_aware_selection` on; committed `indexer_failure_domains` = `DEVNET2_FAILURE_DOMAINS`; keys, public file and consent per member | §5 | 86 | `MEMBERS_EPOCH` |
| 5 | `stage3.placement` on (seals the open block, places every database, records the cutover) | drain | three members registered and Active, in no ring; preview saved | §6 | 86 | `PLACEMENT_EPOCH`, `PLACEMENT_CUTOVER_BLOCK` |
| gate | Keeper headroom (K86) | none | `placement` on; user agreement of the thresholds | §7 | 87 | `KEEPER_HEADROOM` |
| 6 | `si_member_ddl` per installation (indexer-b, indexer-c, network), then the reconciler per host (b, c, a); `MemberReady` | none (no consensus update) | gate passed; `<replicated_merge_tree>` default 0 on every SI installation | §8 | 87 | `STEP6_END` |
| 7 | `stage3.reselection` on | drain | every member ready in every group it belongs to | §9 | 88 | `RESELECTION_EPOCH` |
| 8 | `stage3.checkpoints` on | drain | `reselection` on | §10 | 88 | `CHECKPOINTS_EPOCH` |
| 9 | `stage3.narrowed_drain` on | drain | `checkpoints` on; every member CaughtUp or Behind in every group | §11 | 88 | `NARROWED_DRAIN_EPOCH` |
| 10 | `stage3.min_safe_copies` 2 | issuance | `narrowed_drain` on; precondition preview; anti-affinity proof; Keeper check; `P4_END` | §12 | 89 | `K2_EPOCH` |
| exit | 7 days at k = 2 | — | — | §13, §14 | 89 | `P6_END` |

Each consensus row's update changes exactly its field: `show-after-step<N>.json` differs from `show-before-step<N>.json` only in the epoch, the params digest and that field (`verifiers` for step 4).

### 59. production, P6 (Tasks 83–84)

**59.1 charts/sentio-node (Task 83).**
- Values, under the `siMember` block of S3-C3 (documented as comments in `values.yaml`, off or empty by default): `siMember.consensus.enabled` (`false`); `siMember.consensus.keysSecret` (`{name: "", seedKey: ed25519-seed, replicaSignerKey: replica-signer-key}`); `siMember.consensus.consentJWS` (`""`); `siMember.reconciler.passwordSecret` (`{name: "", key: password}`), beside S3-C3's `siMember.reconciler.enabled`; `siMember.reconciler.metricsPort` (`9097`, rendered as `storage_integrity.member.reconciler.metrics_listen`, S3-E4 Task 79's own listener of the reconciler beside the member's 9095 and 9096; container port `rec-metrics`) and `siMember.reconciler.resources` (requests 100m/128Mi, limits 1/512Mi).
- With `siMember.consensus.enabled`: the member ConfigMap `<fullname>-si-member-config` gains, under `storage_integrity.member.consensus`, `register: true`, `ed25519_seed_file: /etc/si-member-keys/ed25519-seed`, `replica_signer_key_file: /etc/si-member-keys/replica-signer-key` and `consent_file: /config/consent.jws` (§58.3), and the key `consent.jws` (the value of `consentJWS` and a newline), which S3-C3's mount at `/config` exposes; a Secret volume `si-member-keys` (items `ed25519-seed` and `replica-signer-key` from `keysSecret`, `defaultMode: 0400`) is mounted read-only at `/etc/si-member-keys` in the `si-member` container only; the pod's `checksum/si-member-config` then hashes `si-member.yaml` and `consent.jws: <jws>` together (a release without consensus mode keeps S3-C3's checksum).
- With `siMember.reconciler.enabled` (S3-C3's value; this plan replaces S3-C3's guard `siMember.reconciler.enabled needs the si_member_ddl user of spec §10.4 step 6 (Plan S3-F); it stays false in P2` with the guards below): the ConfigMap gains `storage_integrity.member.consensus.create_replicas: true` and `storage_integrity.member.reconciler.clickhouse_username: si_member_ddl` and `metrics_listen: ":<metricsPort>"`, and the `si-member-reconciler` container (same image as `si-member`) runs `/sentio-node si-member --config-path /config/si-member.yaml --role reconciler` (§58.3) with the env `STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD` from `passwordSecret`, on that container only; it mounts the ConfigMap at `/config` and S3-C3's `si-member-state` volume read-only at `/data/si-member` (the main process's `quiescence.json`, S3-E1 §42.3), neither `si-member-keys` nor the `si_member` password, and `si-member` carries no reference to `passwordSecret`.
- Guards (verbatim): `siMember.consensus.enabled requires siMember.enabled`; `siMember.consensus.keysSecret.name is required when siMember.consensus.enabled`; `siMember.consensus.consentJWS must be one compact JWS (three base64url segments) when siMember.consensus.enabled`; `siMember.reconciler.enabled requires siMember.consensus.enabled: a member creates hg_member_unsafe replicas only while it is registered (spec §8.3)`; `siMember.reconciler.passwordSecret.name is required when siMember.reconciler.enabled`; `siMember.reconciler.passwordSecret must not be the si-member password Secret: si_member_ddl's password is mounted only into the reconciler (spec §8.2)`; `siMember.consensus.keysSecret must not be siMember.reconciler.passwordSecret`; `siMember.consensus.keysSecret must not be the si-member password Secret or <release>-si-member-status-token: the member keys are mounted only into si-member`; `siMember.reconciler.passwordSecret must not be <release>-si-member-status-token: the host's sentio-node mounts the status token (D29)`. In the last two, `<release>` is rendered as the release's fullname (`sentio-node-devnet2-indexer-<x>` on devnet2), and "the si-member password Secret" is `siMember.secret.name`. Added by Task 83 (Appendix B): `siMember.consensus.enabled and siMember.reconciler.enabled must be true or false`; `siMember.reconciler.metricsPort must be a port other than siMember.metricsPort and siMember.statusPort: the reconciler listens beside the si-member in one pod`; `siMember.config may not set consensus or reconciler: they come from siMember.consensus and siMember.reconciler values`. Every guard is checked whether or not `siMember.enabled` is set.
- Checksums: no `siMember` value enters the host pod's `checksum/config` (`templates/node.yaml:486`); the member StatefulSet's own checksum covers the new ConfigMap keys and the consent, never Secret data. A release with every new value unset renders byte-identically.
- Secrets (namespace `sentio-network-devnet2`), made only by the user's scripts (§59.5): `sentio-node-devnet2-indexer-a-si-member-keys`, `sentio-node-devnet2-indexer-b-si-member-keys`, `sentio-node-devnet2-indexer-c-si-member-keys` (keys `ed25519-seed`, `replica-signer-key`); `sentio-node-devnet2-indexer-a-si-member-ddl`, `sentio-node-devnet2-indexer-b-si-member-ddl`, `sentio-node-devnet2-indexer-c-si-member-ddl` (key `password`). Each is distinct from the `si_member` password Secret `storage-integrity-si-v2-member-<installation>` and from the status token Secret `sentio-node-devnet2-indexer-<x>-si-member-status-token` (D29), which this plan neither makes nor mounts.

**59.2 `si_member_ddl` (Task 83; live per installation in Task 87).**
- charts/storage-integrity value `clickhouse.access.memberDDL` (`false`) renders, on the network CHI, the user and the profile below; `clickhouse.access.passwordSha256.si_member_ddl` (default `""`, like every other user's) is required exactly when it is rendered. Refusals (verbatim), in this order: `clickhouse.access.memberDDL requires clickhouse.access.lockDefault: si_member_ddl exists only on a locked installation`; `clickhouse.access.memberDDL requires clickhouse.access.member: si_member_ddl comes at stage 3 P6 step 6, after the member stage restarted this installation with the replicated_merge_tree merge default 0 (spec §8.2)`; `clickhouse.access.passwordSha256.si_member_ddl must be 64 lowercase hex characters`; and Plan S3-C3's `si_member_ddl is not created before stage 3 P6 step 6 (Plan S3-F)` for a non-empty hash without `memberDDL`.
- User (S3-B §11 form): `si_member_ddl/password_sha256_hex: "<64 lowercase hex>"`, `si_member_ddl/networks/ip: ["::/0"]`, `si_member_ddl/profile: si_member_ddl`, `si_member_ddl/grants/query`, exactly and in this order: `GRANT CREATE DATABASE ON hg_member_unsafe.*`; `GRANT CREATE TABLE, DROP TABLE ON hg_member_unsafe.*`; `GRANT TABLE ENGINE ON ReplicatedMergeTree` (spec §8.2: no `INSERT`, `ALTER`, `OPTIMIZE`, `TRUNCATE` or `SYSTEM` anywhere, no `CREATE TABLE` elsewhere, nothing on `hg_safe`, `hg_unsafe` or `hg_promote`, no `SYSTEM DROP REPLICA`).
- Profile `si_member_ddl`: `si_member_ddl/profile: default` and `si_member_ddl/constraints/merge_tree_max_bytes_to_merge_at_max_space_in_pool/const: ""` (rendered into the operator's users.d profiles file as an empty `<const>` element, a reload without restart); the constraint holds the setting at the `<replicated_merge_tree>` default 0 that P2's restart set (S3-C3 K39), so `ddl.UnsafeSettings`' explicit 0 passes and any other value is refused with code 452.
- The same user and profile enter `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2-indexer-b-source.yaml` and `storage-integrity-si-v2-indexer-c-source.yaml` only through Task 87's commits; the PR leaves both manifests unchanged, and `test_render.py` compares them with the chart rendered at their own `DEVNET2_MEMBER_DDL_USERS` value.
- Residual (S3-C3 K43 style, recorded in the runbook §16.5): `si_member_ddl` is installation-wide, so on the network CHI it exists on the verifier hosts too; a leaked `si_member_ddl` credential stays a residual until per-path Keeper ACLs (spec §8.2, §18).

**59.3 Grant probe rows (Task 83).** Corpus `charts/storage-integrity/tests/clickhouse_grant_probe.tsv` (columns `role`, `expect`, `statement`) gains thirteen rows for `si_member_ddl` and the `expect` value `constrained` (ClickHouse code 452, after the access check), beside `ok` (no error) and `denied` (code 497 only; an unknown table, 60, or database, 81, is an error, never a denial). The rows run alone (no setup row runs first) and in file order, so every denial meets an existing table:
- `ok`: `CREATE DATABASE IF NOT EXISTS hg_member_unsafe`; then `CREATE TABLE hg_member_unsafe.probe_ddl (x UInt8) ENGINE = ReplicatedMergeTree('/probe/si_member_ddl/probe_ddl', 'probe') ORDER BY x SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0`.
- `denied`, while `probe_ddl` exists: `INSERT INTO hg_member_unsafe.probe_ddl VALUES (1)`; `OPTIMIZE TABLE hg_member_unsafe.probe_ddl FINAL`; `TRUNCATE TABLE hg_member_unsafe.probe_ddl`; `ALTER TABLE hg_member_unsafe.probe_ddl DROP PARTITION tuple()`; `SYSTEM SYNC REPLICA hg_member_unsafe.probe_ddl`; `SYSTEM DROP REPLICA 'probe' FROM ZKPATH '/probe/si_member_ddl/none'`; `CREATE TABLE hg_member_unsafe.probe_mt (x UInt8) ENGINE = MergeTree ORDER BY x`; `CREATE TABLE hg_replica.probe_ddl (x UInt8) ENGINE = ReplicatedMergeTree('/probe/si_member_ddl/replica', 'probe') ORDER BY x`; `CREATE DATABASE probe_replicated ENGINE = Replicated('/probe/si_member_ddl/db', 's', 'r')`. That `si_member_ddl` holds nothing on `hg_safe`, `hg_unsafe` or `hg_promote` is read from `system.grants` (equal to `SI_MEMBER_DDL_GRANTS`, never `SHOW GRANTS`), not probed.
- `constrained`: `CREATE TABLE hg_member_unsafe.probe_merge (x UInt8) ENGINE = ReplicatedMergeTree('/probe/si_member_ddl/probe_merge', 'probe') ORDER BY x SETTINGS max_bytes_to_merge_at_max_space_in_pool = 161061273600`.
- `ok`, last: `DROP TABLE hg_member_unsafe.probe_ddl SYNC`.
- Measured on ClickHouse 26.3.12.1 (the devnet2 image) with this user, profile and server bundle: in this order every row answers as its `expect` says; with the `DROP` before the denials, `INSERT` and `TRUNCATE` answer 60 before their access check (Cursor Bugbot, housegate#249). The Keeper paths are relative to the installation's Keeper root, so the probe writes only under `/storage-integrity/devnet2/devnet2-si-v2/probe/si_member_ddl`; the helper runs the rows in file order, judges `ok` by 0, `denied` by 497 alone and `constrained` by 452, and refuses to start while that path has children.

**59.4 Monitoring (Task 83; on in Task 89).**
- Values under `monitoring`: `stage3.exitAlert` (`false`), `stage3.heldCleanupAgeBoundSeconds` (`1800`), `stage3.p6RunbookURL` (`https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p6-switches.md`).
- With `monitoring.enabled` and `monitoring.stage3.exitAlert`, `templates/monitoring.yaml` renders the rule group `<fullname>.stage3-p6` with one alert, `ArbiterFloorHeldCleanupAgeAboveExitBound`: `expr: max by (cluster, network) (arbiter_floor_held_cleanup_age_seconds{network="<network>"}) > <heldCleanupAgeBoundSeconds>` (S3-B's rule conventions: every aggregation keeps `cluster` and `network`), `for: 5m`, labels `severity: warning`, annotations `summary: the copy floor has held an unsafe-part cleanup for longer than the P6 exit bound` and `runbook_url: <p6RunbookURL>#131-daily-checks`. `MONITORING_ALERTS` gains it; refusals (verbatim) `monitoring.stage3.heldCleanupAgeBoundSeconds must be a positive integer` and `monitoring.stage3.exitAlert requires monitoring.enabled`.

**59.5 Scripts (Task 83), run only by the user.**
- `scripts/storage-integrity/make-member-keys.sh <a|b|c> <member-id> <public-file> [<replica-signer-key-file>]` (new; D40: the user makes the `replica_signer` key in the user's own terminal, with Foundry `cast wallet new` or an equivalent tool the user prefers): refuses unless `<member-id>` matches `^member-[a-z0-9]+$`, `openssl version` starts with `OpenSSL ` and a major version of 3 or more and the first field of `printf '' | openssl dgst -KECCAK-256 -r` is `c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470` (an OpenSSL 3 or later with the `KECCAK-256` digest: the user's Homebrew `openssl` is 4.0.3 and `openssl@3` 3.6.4, both measured; macOS's `/usr/bin/openssl` is LibreSSL 3.3.6 and is refused), `jq` and `xxd` exist, and the Secret `sentio-node-devnet2-indexer-<x>-si-member-keys` does not exist; in a fresh `mktemp -d` directory (mode 700, removed on exit) it writes `openssl rand -hex 32` as `ed25519-seed` and, as `replica-signer-key` (64 lowercase hex without `0x`), either the private key of `cast wallet new --json` (three arguments; `cast` must exist; Foundry 1.x prints a one-element array `[{"address","private_key"}]`) or the key read from `<replica-signer-key-file>` (four arguments; `cast` is not needed): a regular file owned by the user with no group or other permission bits, holding 64 hex characters with an optional `0x` and an optional trailing newline, a key in [1, n − 1] for the secp256k1 order n `fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141`; the script never modifies or deletes that file, and runbook §5.1 has the user remove it once the Secret exists. Either way it derives the `replica_signer` address itself with OpenSSL only (the SEC1 DER `302e0201010420<key>a00706052b8104000a` through `openssl ec -inform DER -pubout -outform DER -conv_form uncompressed`, the last 64 bytes through `openssl dgst -KECCAK-256`, `0x` and its last 40 hex), checks that pipeline first against the key `0000000000000000000000000000000000000000000000000000000000000001`, which must give `0x7e5f4552091a69125d5dfcb7b8c2659029395bdf`, and with `cast` requires the derived address to equal the `address` `cast wallet new` printed, lowercased; it checks its ed25519 pipeline against RFC 8032 test 1 (`d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a`) and derives the public key with it (the pipeline of production `docs/storage-integrity-devnet2-multi-source.md` §5.2); it runs `kubectl --context sentio-sea -n sentio-network-devnet2 create secret generic sentio-node-devnet2-indexer-<x>-si-member-keys --from-file=ed25519-seed=<file> --from-file=replica-signer-key=<file>`; it writes `<public-file>` as one JSON object `{"host":"<x>","node_id":"<member-id>","ed25519_pubkey":"<64 lowercase hex>","replica_signer":"0x<40 lowercase hex>","secret_uid":"<the Secret's metadata.uid>"}`; it never prints a key and prints only `make-member-keys: created <secret> for <member-id>; public values in <public-file>`; it refuses a public file that exists.
- `scripts/storage-integrity/make-clickhouse-logins.sh` gains `si_member_ddl` for the installations `network` (Secret `sentio-node-devnet2-indexer-a-si-member-ddl`), `indexer-b` (`...-indexer-b-si-member-ddl`) and `indexer-c` (`...-indexer-c-si-member-ddl`), key `password`, in S3-C1's single-user mode of §58.3 (`--add-user si_member_ddl <installation> <hashes-file>`), writing `passwordSha256: {si_member_ddl: <hex>}` to the hashes file; the user's hashes files are `$HOME/si-devnet2-stage3-p6/hashes-member-ddl-<installation>.yaml`.

**59.6 test_render.py (Task 83).**
- Constants: `SI_MEMBER_DDL_GRANTS = ("GRANT CREATE DATABASE ON hg_member_unsafe.*", "GRANT CREATE TABLE, DROP TABLE ON hg_member_unsafe.*", "GRANT TABLE ENGINE ON ReplicatedMergeTree")`; `SI_MEMBER_DDL_CONSTRAINT = "merge_tree_max_bytes_to_merge_at_max_space_in_pool"`; `DEVNET2_MEMBER_HOSTS = {"sentio-node-devnet2-indexer-a": ("a", 0, "storage-integrity-si-v2"), "sentio-node-devnet2-indexer-b": ("b", 1, "storage-integrity-si-v2-indexer-b"), "sentio-node-devnet2-indexer-c": ("c", <INDEXER_C_ID>, "storage-integrity-si-v2-indexer-c")}` (the id written as an integer literal from the ledger); `DEVNET2_MEMBER_KEY_SECRETS` (release → `<release>-si-member-keys`); `DEVNET2_MEMBER_DDL_SECRETS` (release → `<release>-si-member-ddl`); `DEVNET2_MEMBER_CONSENSUS` (release → `False` | `True`; all `False` in the PR); `DEVNET2_MEMBER_RECONCILER` (release → bool; all `False`); `DEVNET2_MEMBER_DDL_USERS` (CHI → bool; all `False`); `P6_HELD_CLEANUP_AGE_BOUND_SECONDS = 1800`; `DEVNET2_P6_EXIT_ALERT = False`; added (Appendix B): `SI_MEMBER_DDL_PROFILE`, `TEST_P6_PASSWORD_SHA256`, `TEST_CONSENT_JWS`, `DEVNET2_MEMBER_DDL_ROLLOUT`, `STAGE3_P6_RUNBOOK`, `STAGE3_P6_EXIT_SECTION`, `MAKE_MEMBER_KEYS`, `MONITORING_ALERTS["stage3-p6"]`; `DEVNET2_MEMBER_HOSTS` is derived from Plan S3-C3's `DEVNET2_INDEXER_IDS` and `DEVNET2_MEMBER_CHIS`, so `INDEXER_C_ID` is not written again.
- New class `MemberConsensusTest` (after S3-C3's member test class): `test_default_renders_no_member_consensus`, `test_member_consensus_renders_the_consensus_mode_keys`, `test_member_consensus_mounts_its_keys_only_into_the_member_container`, `test_member_reconciler_takes_its_password_only_from_its_secret`, `test_member_reconciler_needs_consensus_mode`, `test_member_changes_never_touch_the_host_checksum`, `test_member_consensus_guards_fail_closed`, `test_unset_member_consensus_renders_unchanged`, `test_si_member_ddl_holds_exactly_its_grants`, `test_si_member_ddl_profile_makes_merges_const`, `test_si_member_ddl_carries_only_a_literal_hash`, `test_si_member_ddl_needs_a_locked_installation`, `test_grant_probe_covers_si_member_ddl` (every grant of `SI_MEMBER_DDL_GRANTS` has an `ok` row; `denied` rows for `INSERT`, `OPTIMIZE`, `TRUNCATE`, `DROP PARTITION`, `SYSTEM`, a `MergeTree` table, a table in `hg_replica`, a `Replicated` database; one `constrained` row), `test_p6_exit_alert_reads_the_held_cleanup_gauge`.
- In `ActualFreshNetworkTest`: `test_member_consensus_needs_a_reviewed_image` (a release with `siMember.consensus.enabled` runs its member on `DEVNET2_STAGE3_P5_SOURCE_IMAGE`), `test_devnet2_member_consensus_follows_the_rollout`, `test_devnet2_member_reconcilers_follow_the_rollout`, `test_devnet2_member_ddl_users_follow_the_rollout`, `test_devnet2_member_secrets_are_per_host` (each enabled release names exactly its `DEVNET2_MEMBER_KEY_SECRETS` and `DEVNET2_MEMBER_DDL_SECRETS` entries, and neither is its `siMember.secret.name` or its `<release>-si-member-status-token`, D29), `test_devnet2_p6_exit_alert_follows_the_rollout`.
- Earlier tests amended in place (each keeping its assertions): S3-B's `test_default_renders_no_monitoring`, `test_alert_names_are_the_reviewed_set`, `test_grant_probe_covers_every_protocol_grant`, `test_devnet2_clickhouse_logins_follow_the_rollout`, `test_indexer_b_source_clickhouse_carries_the_network_source_profile`, `test_indexer_c_source_clickhouse_carries_the_network_source_profile_and_a_member_disk`, `ClickHouseLoginsScriptTest.ADDED`, `test_si_clients_take_clickhouse_passwords_only_from_secrets`; S3-C3's `SIMemberTest.setUpClass`, `test_default_renders_no_si_member`, `test_si_member_reconciler_stays_off_in_p2`, `test_si_member_ddl_never_exists_in_p2`, `test_engine_grants_follow_the_census_constants`, `test_si_member_config_names_the_hosts_own_source_clickhouse`, and its helpers `member_values`, `p2_overrides`, `without_p2_users`, `without_p2_chi`, `p2_chi`, `_p2_strip_manifest_lines`, `p2_manifest_text`, `p2_overlay_text`; S3-C4's `test_replica_reads_guards_fail_closed`, `test_member_status_token_comes_only_from_a_secret`, `test_devnet2_replica_reads_follow_the_rollout`; S3-B's `probe_atoms` asks `p6_probe_atoms`.

**59.7 Runbook `docs/storage-integrity-devnet2-stage3-p6-switches.md` (Task 84).**
- Sections (binding for Tasks 85–89 and for every `runbook_url`; each top-level heading `## <n>. <title>` with exactly these titles): §0 Session helpers; §1 Preparation (§1.1 Pins and ledger values, §1.2 Identities, §1.3 Preconditions and baseline, §1.4 Promotion budget); §2 Step 1: purge (§2.1 Prerequisites, §2.2 Update, §2.3 Verify and acceptance tables); §3 Step 2: eviction voiding (§3.1 Prerequisites, §3.2 Update, §3.3 Verify); §4 Step 3: owner-aware selection (§4.1 Host check, §4.2 Update, §4.3 Verify); §5 Step 4: member entries and registration (§5.1 Member keys (user action), §5.2 Consent statements, §5.3 Verifiers update, §5.4 Consensus mode, one host at a time, §5.5 Verify); §6 Step 5: placement (§6.1 Placement preview, §6.2 Update, §6.3 Verify groups, cutover and replica hosts); §7 Keeper headroom gate (§7.1 Baseline, §7.2 Projection and thresholds, §7.3 Verdict); §8 Step 6: member replicas and readiness (§8.1 si_member_ddl logins (user action), §8.2 si_member_ddl users, one installation at a time, §8.3 Reconcilers, one host at a time, §8.4 MemberReady and eligible rings, §8.5 Keeper re-measure); §9 Step 7: re-selection (§9.1 Update, §9.2 Verify); §10 Step 8: checkpoints (§10.1 Update, §10.2 Verify); §11 Step 9: narrowed drain (§11.1 Update, §11.2 Verify); §12 Step 10: raising k to 2 (§12.1 Precondition preview, §12.2 Anti-affinity proof, §12.3 Keeper check, §12.4 Update, §12.5 Verify); §13 Exit soak (§13.1 Daily checks, §13.2 Exit); §14 End; §15 Forward fixes (§15.1 Steps 1–3, §15.2 Step 4, §15.3 Step 5, §15.4 Step 6, §15.5 Steps 7–9, §15.6 Step 10); §16 Operating rules (§16.1 Restarts one at a time, §16.2 Operator playbook, §16.3 Holds, §16.4 Promotion budget, §16.5 Keeper). Alert anchors used: `131-daily-checks`, `162-operator-playbook`.
- Runbook constants (first bash block of §0): `si_backup="$HOME/si-devnet2-stage3-p6"`; `KEEPER_ROOT=/storage-integrity/devnet2/devnet2-si-v2`; `KEEPER_R0=4`; `KEEPER_R1=6`; `KEEPER_MAX_WORKING_SET_BYTES=2147483648`; `KEEPER_MAX_DATA_BYTES=1073741824`; `KEEPER_MAX_DU_BYTES=1073741824`; `KEEPER_MAX_AVG_LATENCY_MS=10`; `KEEPER_MAX_OUTSTANDING=10`; `P6_HELD_CLEANUP_AGE_BOUND_SECONDS=1800`; `P6_SOAK_DAYS=7`; `SI_AUTHORITY=0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705`; `SI_MAX_WRITERS=3`.
- §0 chain (S3-B D20): `helpers.sh` holds the first bash block of §0 of the P5 runbook along its own chain, then this runbook's; a P6 helper never redefines an earlier one (§0 refuses with `declare -F <name> && { echo "<name> is defined earlier in the chain; stop"; exit 1; }`). Earlier helpers this runbook calls: `si_kubectl`, `si_leader`, `si_voter`, `si_arbiter`, `si_show`, `si_put_voters`, `si_jws`, `si_indexer_b`, `si_indexer_c`, `si_node_image`, `si_info`, `si_source_ch`, `si_verifier_ch`, `si_ch_admin`, `si_keeper`, `si_state_size`, `si_series_value`, `si_vm_query`, `si_vm_value`, `si_head_is`, the P2 runbook's `si_member_pod`, `si_member_ch`, `si_member_metrics` and `si_budget_capture` / `si_budget`, the P3 runbook's `si_member_serving`, and the P5 runbook's `si_stage3`.
- P6 helpers (all in the first bash block of §0; `si_stage3` is the P5 runbook's and `si_member_pod a|b|c` the P2 runbook's, both reached through the chain): `si_switch_is <switch> true|false` (refuses unless the committed switch has that value); `si_stage3_update <step> drain|issuance <flags...>` (mutating; reads `show-before-step<step>.json`, refuses unless its authority set is exactly `[SI_AUTHORITY]` lowercased and `max_writers` is `"3"`, then runs inside the leader's arbiter container `arbiter-admin consensus update --address <leader> --expected-epoch <epoch> --authority $SI_AUTHORITY --max-writers $SI_MAX_WRITERS --hold <mode> --hold-timeout 10m <flags...> --timeout 15m`, `tee` into `update-step<step>.out`, then writes `show-after-step<step>.json`); `si_step_diff <step>` (prints the `.current` fields that differ between the two show files, the epoch and digest aside); `si_member a|b|c <cmd...>` (`si_kubectl exec -i <pod> -c si-member -- <cmd...>`); `si_member_id a|b|c` (from `MEMBER_IDS`'s `member-<x>:<indexer id>` pairs); `si_host_id a|b|c` (0, 1, `INDEXER_C_ID`); `si_failure_domain <indexer id>` (from `DEVNET2_FAILURE_DOMAINS`); `si_stage3reads_build` (local); `si_stage3reads <flags...>`; `si_groups` (`si_stage3reads -groups`); `si_block <seq>` (`si_stage3reads -block <seq>`); `si_scoped_paths` (registry incarnations with `scoped_keeper_path` true and their replicas' `zookeeper_path` from `system.replicas` on the owner's source ClickHouse); `si_unsafe_replicas [a|b|c]...` (read-only; the arguments are the hosts whose reconciler runs; for every Active SI incarnation of a database with a group, on its owner's source ClickHouse, one TSV row from `system.replicas` of its `hg_unsafe` table: incarnation, owner, database, table, `total_replicas`, `active_replicas` and `arraySort(mapKeys(replica_is_active))` joined by `,`; exit 1 unless every row's names are exactly the owner's SNode node id in the committed `si_indexers`, `verifier-1`, `verifier-2`, `verifier-3` and the `node_id` of each member of the table's group, read from `si_groups`, whose `host_indexer_id` is `si_host_id` of an argument, and `total_replicas` equals their count; a replica's name is the node id its role registers with the arbiter, arbiter-core `dataplane/ddl/pinned.go:11-16` and `build.go:105` at `origin/main`, and a member's is its `si_member_id`); `si_keeper_pods` (the three pods with label `clickhouse-keeper.altinity.com/chk=clickhouse-keeper-extra`, sorted by replica index); `si_keeper_on <0|1|2> <keeper-client query>`; `si_keeper_sample` (one TSV row per replica: time, replica, `zk_server_state`, `zk_znode_count`, `zk_approximate_data_size`, `zk_avg_latency`, `zk_max_latency`, `zk_outstanding_requests`, `zk_watch_count`, SI znodes (`get_all_children_number $KEEPER_ROOT/sentio`), working-set bytes (`memory.current` minus `inactive_file` of the container's cgroup), `du -sb /var/lib/clickhouse-keeper`, free bytes of `df -B1 /var/lib/clickhouse-keeper`, container restart count); `si_keeper_verdict <tsv>` (local, K86 projection and thresholds; prints `PASS` or one `FAIL <replica> <measure> <value> > <limit>` line per breach); `si_keeper_abort <tsv> <baseline-tsv>` (local, K86 abort criteria; exit 1 on any); `si_safe_copies` (K87 (b); each member's host from its served `host_indexer_id` (D27) and that host's source ClickHouse pod's node from `si_node_of`; one JSON line per Active incarnation `{"incarnation","owner","database","table","safe_copies","domains":[...]}`; exit 1 if any is below 2); `si_k_precondition 2` (spec §9.3 raising k, from `si_groups` and `-watermark`; one JSON line per Active incarnation with `ok` and the reflecting domains); `si_node_of <pod>`; `si_antiaffinity` (D39: every SI source ClickHouse pod and verifier ClickHouse pod on the node its pin names, each source on the node of its `DEVNET2_FAILURE_DOMAINS` domain, the three sources on pairwise distinct nodes and no verifier on a source's node; the verifiers on pairwise distinct nodes unless S3-D's `DEVNET2_CLICKHOUSE_NODES` and `DEVNET2_NODE_PINS` on `origin/main` place two of them on one node, which it then reports as `verifiers share <node> (D39)`; exit 1 on any other breach); `si_member_ddl_probe network|indexer-b|indexer-c` (the `si_member_ddl` rows of the corpus in file order, as `si_member_ddl` with the password piped from its Secret `sentio-node-devnet2-indexer-<x>-si-member-ddl` into `kubectl exec -i` stdin, judging `ok` by 0, `denied` by 497 alone and `constrained` by 452; it refuses to start while `$KEEPER_ROOT/probe/si_member_ddl` has children); `si_keeper_findings a|b|c` (the two `sentio_node_storage_integrity_keeper_findings{reason}` lines of that host's `:9091`, read with `wget -qO- http://127.0.0.1:9091/metrics` in its `sentio-node` container; exit 1 unless both are present and 0; D30); `si_acceptance <step>` (mutating: one INSERT of value 6000 + 100 × step + i, i = 1, 2, 3 for owners a, b, c, through each owner's agent, then waits for safe).
- Throwaway tool `stage3reads`: the runbook's only go block (in §0); built by `si_stage3reads_build` from a `git archive` of arbiter-core `CORE_TAG_P5` (`CGO_ENABLED=0 GOOS=linux GOARCH=amd64`) into `$si_backup/tools/stage3reads`, never committed; `si_stage3reads` copies it into the leader's arbiter container at `/tmp/stage3reads` when missing or stale (by SHA-256) and runs it with `-peers` naming the three voters' data-plane addresses on `:7080`. It uses `dataplane.Client.LeaderConn` with the generated `TableRegistry` and `SafeState` clients and the S3-E1 wire converters. Flags `-peers`, `-timeout` (default `20s`), exactly one of `-registry`, `-groups`, `-watermark`, `-block <seq>`. JSON lines (binding for the runbook, whatever the wire types call the fields): `-registry` → `{"incarnation","database_id","table","status","origin","owner","scoped_keeper_path","purge_members"}` per incarnation; `-groups` → `{"owner","database","generation","network_fill","members":[{"node_id","host_indexer_id","joined_generation","ready_generation","class","lag_advances","checkpoint_seq","checkpoint_entry":{"safe_block_seq","applied_promotion_seq"},"excluded"}],"past_members","leaving":[{"node_id","release_recorded"}]}` per group, then one `{"member_checkpoint_seqs":[{"node_id","owner","checkpoint_seq"}]}`; `-watermark` → `{"snapshot_id","safe_block_seq","track_frontiers":[{"indexer_id","safe_block_seq","applied_promotion_seq"}]}`; `-block` → `{"block_seq","owner","group_key","verifier_set","eligible_ring","ring_position","group_members","group_fill","group_databases","reselections","quorum","finality"}` (uint64 values as JSON strings, as `consensus show` prints them; only served fields, §58.3 and D33, so `reselections` carry no `expected_set`).
- Evidence files under `$si_backup`: `pins` (the consumed ledger values, one `NAME=value` line each), `ledger-values.txt`, `images-before.txt`, `stage3-before.json`, `budget-voter`, `budget-snapshots.tgz`, `budget-live-seq`, `budget.txt` (S3-C3's `si_budget_capture` and `si_budget`), `acceptance-databases.tsv` (`<indexer id>\t<database>`), `features-step<N>.json`, `show-before-step<N>.json`, `update-step<N>.out`, `show-after-step<N>.json`, `diff-step<N>.txt`, `verify-step<N>.txt`, `scoped-paths.jsonl`, `host-images-step3.txt`, `block-<seq>.json`, `member-public-<x>.json`, `consent-<x>.jws`, `step4-<x>-commit`, `member-identity-<x>.json`, `placement-preview.json`, `groups-after-step5.json`, `cutover.txt`, `replica-hosts-step5.jsonl`, `keeper-baseline.tsv`, `keeper-projection.txt`, `keeper-thresholds-agreed.txt` (the user's confirmation, quoted with its time), `keeper-verdict.txt`, `step6-ddl-<installation>-commit`, `member-ddl-probe-<installation>.tsv`, `step6-reconciler-<x>-commit`, `member-replicas-<x>.tsv`, `owner-replicas-after-<x>.tsv`, `ready-<x>.jsonl`, `replica-hosts-after-<x>.jsonl`, `keeper-after-<x>.tsv`, `keeper-findings-<x>.txt`, `rings-after-<x>.jsonl`, `seal-reasons.txt`, `classes-step8.jsonl`, `k2-precondition.jsonl`, `antiaffinity-step10.txt`, `keeper-step10.tsv`, `exit-alert-commit`, `soak-day<d>.txt`, `safe-copies-day<d>.jsonl`, `keeper-day<d>.tsv`, `antiaffinity-day7.txt`, `purge-end.jsonl`, `p6-end.txt`.
- Acceptance writes: one table `s3_p6` in one database per owner, chosen in §1.2 from the registry (`acceptance-databases.tsv`), created right after step 1 (so each lives on a scoped Keeper path) through that database owner's agent with the statement shape of the P0 runbook's `devuser1.s3_p0`, and dropped in §14 (exercising the member-aware purge sets); values 6101–6103 (§2.3), 6301–6303 (§4.3), 6501–6503 (§6.3), 6601–6603 (§8.4), 6801–6803 (§10.2), 7001–7003 (§12.5), one confirmed block per write.
- Pointer: one row in the stage table of `docs/storage-integrity-devnet2-multi-source-tracks.md`, after the last stage-3 row, naming this runbook.

**59.8 Runbook checker (Task 84).** `$W.runbook-check.py` (local, beside the worktree, never committed, S3-B's pattern) checks: `headings` (exactly §59.7's headings, unique anchors), `bash-parse` (every bash block under bash 5 and `/bin/bash` 3.2.57), `confirm-before-mutating`, `stop-before-irreversible` (every block that calls `si_stage3_update` follows `Stop here and obtain explicit confirmation.`), `hold-modes` (§2–§11 pass `drain`, §12 `issuance`), `switch-order` (the switch flags appear in §2, §3, §4, §6, §9, §10, §11 in that order and nowhere else), `restates-verifiers` (the only `--verifier` flags are §5.3's, built from `show-before-step4.json` and the three public files), `keeper-gate-first` (§8's first mutating block refuses unless `keeper-verdict.txt` is `PASS` and `keeper-thresholds-agreed.txt` exists), `no-secret-data-in-blocks` (no block reads `.data` of a Secret except into `kubectl exec -i` stdin), `runbook-urls` (every `runbook_url` anchor the chart renders exists); plus `shellcheck -S warning` over every block.

**59.9 The P6 PR (Task 84).** Branch `urwt/claude/multi-source-s3f` from production `origin/main` after Plan S3-E4's P5 PR merged and `P5_END` is recorded; title `storage-integrity: multi-source SI stage 3 P6 (member consensus values, si_member_ddl, exit alert, switch-on runbook)`; it carries Tasks 83 and 84 and renders every release byte-identically (`render-inertness.sh origin/main`).

### 60. devnet2, P6 (Tasks 85–89)

- **Common to every live task.** Run from the URWT worktree `multi-source-s3f-live` (detached, freshly fetched `origin/main`, checked clean before every commit; commits pushed as `HEAD:main`), every block from a block file after sourcing `helpers.sh` (S3-B D24); every consensus step through `si_stage3_update`; after each update `si_step_diff <N>` must print exactly the step's field (§58.4) and `verify-step<N>.txt` the expected reads; the step's ledger value is the committed epoch of `show-after-step<N>.json`. A refused update changes nothing: read `update-step<N>.out`, wait or fix, rerun the block. "commit outcome may be unknown" is answered by `si_show` before anything else.
- **Task 85: steps 1–3 (runbook §1–§4).** §1: pins and ledger values, identities, read-only baseline (`stage3-before.json` equal to S3-E4's activation record, no member registered, `indexer_failure_domains` = `DEVNET2_FAILURE_DOMAINS`, `UNSAFE_INC_ROOT` without children, every node in `consensus features` with `stage3_v1`), the promotion budget (`budget.txt`; stop if the plan's 18 writes plus the projected 7-day traffic exceed half of it). Step 1 (`--stage3-switch purge`), then the three `s3_p6` tables and the 6101–6103 writes: each new incarnation `scoped_keeper_path` true and its owner's and the pool's `hg_unsafe` replicas under `/sentio/0/unsafe_inc/<db__table>/<incarnation seq>`, no incarnation recorded before `PURGE_EPOCH` scoped, every row safe. Step 2 (`--stage3-switch eviction_voiding`). Step 3: §4.1 host check (K84, `host-images-step3.txt`), `--stage3-switch owner_aware_selection`, then the 6301–6303 writes: each block marked after `SELECTION_EPOCH` stores `eligible_ring` `["verifier-1","verifier-2","verifier-3"]` and a `verifier_set` from it. Records `PURGE_EPOCH`, `VOIDING_EPOCH`, `SELECTION_EPOCH`; run record `Steps 1–3 run`.
- **Task 86: steps 4 and 5 (runbook §5–§6).** §5.1: the user runs, in the user's own terminal (fish), `bash scripts/storage-integrity/make-member-keys.sh b member-b $HOME/si-devnet2-stage3-p6/member-public-b.json`, then the same for `c member-c` and `a member-a` (each id as `MEMBER_IDS` records it); a user who prefers another key generator to Foundry `cast` (D40) makes the secp256k1 key with it into a mode-600 file and passes that file's path as the fourth argument, then deletes the file once the script has printed its line; the runbook checks each public file (`member-public-<x>.json`: `node_id` = `si_member_id <x>`, a 64-hex public key, a 40-hex address, all three keys and addresses distinct) and its `secret_uid` against the Secret's `metadata.uid`, never its data. §5.2: one consent per host (K85, `consent-<x>.jws`, decoded by `si_jws`). §5.3: one `--verifier`-only update (`--hold drain`) restating `verifier-1..3` from `show-before-step4.json` and adding the three member entries (`host` = `si_host_id <x>`, `domain` = `si_failure_domain <host>`, `weight=1`, every flag true); `MEMBERS_EPOCH`. §5.4, per host b, c, a: one commit of that host's overlay `k8s-sea/sentio-network-devnet2/sentio-node-indexer-<x>-storage-integrity-si-v2.yaml` setting the member image to `DEVNET2_STAGE3_P5_SOURCE_IMAGE`, `siMember.consensus.enabled: true`, `keysSecret.name: sentio-node-devnet2-indexer-<x>-si-member-keys` and `consentJWS` from `consent-<x>.jws`, with `DEVNET2_MEMBER_CONSENSUS`; `helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-<x>`; the host pod's start time unchanged, the member pod replaced, the member registered as VERIFIER with `stage3_v1` and Active (so its key is the listed one), its `si_member_serving` view showing `consensus.mode` `consensus` and `consensus.registered` true (`member-identity-<x>.json`), `si_member_metrics <x> '^sentio_node_si_member_consensus_registered '` printing exactly the line `sentio_node_si_member_consensus_registered 1` (`verify-step4.txt`, D30), no ring or verify set naming it. Step 5: §6.1 preview (`placement-preview.json`: every (owner, database) with a physical incarnation, genesis-origin included, has `members` = the other two hosts' member ids sorted and `network_fill` true; Legacy and Refused databases none), `--stage3-switch placement`, then `groups-after-step5.json` equal to the preview, `cutover.txt` (the served cutover; the cutover block's header without `group_key`, the next statement block's with it), every host's status RPC `replica_hosts` per peer database unchanged (`replica-hosts-step5.jsonl`; from placement on taken from the groups, every current member's host from its served `host_indexer_id`, D27, although no member is ready yet, S3-E4 §56), the 6501–6503 writes. Records `MEMBERS_EPOCH`, `PLACEMENT_EPOCH`, `PLACEMENT_CUTOVER_BLOCK`; run record `Steps 4–5 run`.
- **Task 87: the Keeper gate and step 6 (runbook §7–§8).** §7: `keeper-baseline.tsv` (≥ 6 samples over ≥ 24 h, one per 4-hour window), `keeper-projection.txt`, the user's agreement (`keeper-thresholds-agreed.txt`), `keeper-verdict.txt` `PASS`, else stop. §8.1: the user runs `make-clickhouse-logins.sh --add-user si_member_ddl <installation> <hashes-file>` for `indexer-b`, `indexer-c`, `network`. §8.2, per installation indexer-b, indexer-c, network: one commit adding the user and profile (the indexer-b and indexer-c manifests by hand after `--dry-run=server`; the network CHI by `clickhouse.access.memberDDL: true` and its hash in `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml`, synced with `--selector name=storage-integrity-si-v2`), with `DEVNET2_MEMBER_DDL_USERS`; no ClickHouse pod restarted; `system.grants` of `si_member_ddl` equal to `SI_MEMBER_DDL_GRANTS`; `system.replicated_merge_tree_settings` `max_bytes_to_merge_at_max_space_in_pool` `0`; `member-ddl-probe-<installation>.tsv` all as expected. §8.3, per host b, c, a: one overlay commit with `siMember.reconciler.enabled: true` and `passwordSecret.name: sentio-node-devnet2-indexer-<x>-si-member-ddl`, with `DEVNET2_MEMBER_RECONCILER`; sync; the host pod untouched; `member-replicas-<x>.tsv` (one `hg_member_unsafe` replica named `si_member_id <x>` per SI table of its groups, at `/sentio/0/unsafe/<db__table>` for an unscoped incarnation and `/sentio/0/unsafe_inc/<db__table>/<seq>` for a scoped one, `is_readonly` 0); `owner-replicas-after-<x>.tsv` (`si_unsafe_replicas` with every host done so far, x included: on the owner's source ClickHouse each SI table's replicas are exactly its owner's SNode, the three pool verifiers and the members of its group whose reconciler runs, so after b the tables of owners a and c have 5 (member-b their only member replica) and indexer-b's own 4, after c owner a's tables 6 and those of b and c 5, and after a every table 6 with both members; a count is never checked against a member still off); `ready-<x>.jsonl` (`ready_generation` = `generation` in every group of the member); `keeper-after-<x>.tsv` and `si_keeper_abort` clean for an hour, and `keeper-findings-<x>.txt` (`si_keeper_findings` of all three hosts, both reasons 0, D30), else stop at that host (§15.4). §8.4: `rings-after-<x>.jsonl` (each later block of the two owners has the ready member in `eligible_ring` with the pool), `replica-hosts-after-<x>.jsonl` (every host's status RPC `replica_hosts` still equal to `replica-hosts-step5.jsonl`: `MemberReady` changes no list, S3-E4 §56), `seal-reasons.txt` (`group_switch` seals only while an owner's groups differ in readiness), the 6601–6603 writes. §8.5: the Keeper re-measure against the K86 thresholds. Records `KEEPER_HEADROOM`, `STEP6_END`; run record `Keeper headroom gate and step 6 run`.
- **Task 88: steps 7–9 (runbook §9–§11).** `--stage3-switch reselection`; `--stage3-switch checkpoints`, then the 6801–6803 writes and `classes-step8.jsonl` (every member CaughtUp in every group after the next publication, `checkpoint_seq` published per (member, owner) and increasing, `arbiter_floor_held_cleanup_age_seconds` near 0, cleanups continuing), and in `verify-step8.txt` each member's `sentio_node_si_member_checkpoints_total{result="accepted"}` rising per owner with `refused` unchanged (D30); `--stage3-switch narrowed_drain` (still `--hold drain`, K83), then `narrowed_drain` reported by the leader. Records `RESELECTION_EPOCH`, `CHECKPOINTS_EPOCH`, `NARROWED_DRAIN_EPOCH`; run record `Steps 7–9 run`.
- **Task 89: step 10 and the exit (runbook §12–§14).** §12.1 `k2-precondition.jsonl` all `ok`; §12.2 `antiaffinity-step10.txt` (D39's rule, §59.7) and S3-D's `test_si_clickhouses_run_on_distinct_nodes` passing on `origin/main`; §12.3 `keeper-step10.tsv` passing K86; §12.4 `--stage3-min-safe-copies 2 --hold issuance` (a precondition refusal is retried at most three times, a checkpoint interval apart, then stop); §12.5 `min_safe_copies` 2, no `under_replicated` event, the 7001–7003 writes, then one commit setting `monitoring.stage3.exitAlert: true` with `DEVNET2_P6_EXIT_ALERT` (`exit-alert-commit`). §13: one `soak-day<d>.txt` per day 1–7 (held-cleanup daily maximum ≤ 1800, `under_replicated` events none or each explained, `safe-copies-day<d>.jsonl` all ≥ 2, every class CaughtUp or Behind, `keeper-day<d>.tsv` within K86, `si_keeper_findings` 0 on every host, a state-size sample against the budget); a breach restarts the count (K87). §13.2: day 7 plus `antiaffinity-day7.txt`. §14: the three `s3_p6` tables dropped, each incarnation Purged after its owner, the pool and every member of its group reported (`purge-end.jsonl`), its scoped Keeper path gone; `p6-end.txt`; the record PR. Records `K2_EPOCH`, `P6_END`; run record `Step 10 and the exit soak`.
- **Exit (spec §16 P6).** k = 2 for 7 days with the held-cleanup age bounded (K87 (a)); anti-affinity re-checked (K87 (c)); `safe_copies` ≥ 2 on distinct nodes for every Active incarnation (K87 (b)).
- **Record PR (Task 89, docs only).** `docs/storage-integrity-devnet2-stage3-p6-switches.md` §14 gains the run record; `docs/storage-integrity-devnet2-multi-source.md` §1.2 gains each member's public values (member id, host indexer, failure domain, ed25519 public key, `replica_signer`, since `MEMBERS_EPOCH`).

### 61. Release ledger, repositories and commands (Tasks 83–89)

- **Consumed** (from the ledger copy, never predicted): `INDEXER_C_ID` (Plan S3-B); `MEMBER_IDS`, `UNSAFE_INC_ROOT`, `P2_END` (Plan S3-C3); `DEVNET2_FAILURE_DOMAINS`, `P4_END` (Plan S3-D); `HG_TAG_P5`, `CORE_TAG_P5` (Plan S3-E1); `ARBITER_TAG_P5`, `ARBITER_DIGEST_P5`, `SN_COMMIT_P5`, `SN_DIGEST_P5`, `ACTIVATION_EPOCH`, `P5_END` (Plan S3-E4).
- **Produced, in order:** `P6_PR` (Task 84); `PURGE_EPOCH`, `VOIDING_EPOCH`, `SELECTION_EPOCH` (Task 85); `MEMBERS_EPOCH`, `PLACEMENT_EPOCH`, `PLACEMENT_CUTOVER_BLOCK` (Task 86); `KEEPER_HEADROOM`, `STEP6_END` (Task 87); `RESELECTION_EPOCH`, `CHECKPOINTS_EPOCH`, `NARROWED_DRAIN_EPOCH` (Task 88); `K2_EPOCH`, `P6_END` (Task 89). Value forms: `P6_PR` a PR number (`[1-9][0-9]*`); every `*_EPOCH` and `PLACEMENT_CUTOVER_BLOCK` an unsigned integer (`[0-9]+`); `KEEPER_HEADROOM`, `STEP6_END`, `P6_END` dates (`20[0-9]{2}-[01][0-9]-[0-3][0-9]`): the day the gate passed, the day every member was ready with the Keeper re-measured, the end date of runbook §14. The names are unique among the stage-3 plan files, as `s3-ledger.py` requires.
- **Appendix C** keeps S3-B's shape (D23): one heading `## Appendix C: execution record`; one table `| Name | Value | Recorded by |` with one row per produced name above, in that order, `| \`NAME\` |  | Task N |` until recorded; then "Run records" with one item per recording task: `- P6 PR (Task 84):`, `- Steps 1–3 run (Task 85):`, `- Steps 4–5 run (Task 86):`, `- Keeper headroom gate and step 6 run (Task 87):`, `- Steps 7–9 run (Task 88):`, `- Step 10 and the exit soak (Task 89):`, each filled in the same commit as its task's rows. The `KEEPER_HEADROOM` run record holds the baseline maxima, the projection, the agreed thresholds and every step-6 re-measure; the Task 89 record the seven daily lines.
- **Ledger tool and record procedure.** `$HOME/.cache/s3-ledger/s3-ledger.py`, written verbatim from Plan S3-C1 contract §19 by each recording task's first ledger step; `s3-ledger.py ref <housegate> s3f`, `get` (`NAME=value` lines), `empty`, `set <plan file> <task> <record file> NAME=VALUE...`. R1: `git -C /Users/uranuswch/Dev/projects/sentio-network/housegate fetch -q origin`; `ref` (`python3 -I "$HOME/.cache/s3-ledger/s3-ledger.py" ref /Users/uranuswch/Dev/projects/sentio-network/housegate s3f`); a URWT worktree `multi-source-s3f-ledger-<N>` of housegate at `$ref` (`bash ~/.agents/skills/urwt/scripts/urwt-create.sh /Users/uranuswch/Dev/projects/sentio-network/housegate claude multi-source-s3f-ledger-<N> urwt/claude/multi-source-s3f-ledger-<N> "$ref"`), its path and `$ref` in `$HOME/.cache/s3-ledger/record-<N>`; the run record in `$HOME/.cache/s3-ledger/record-<N>.md`; `s3-ledger.py set <worktree>/docs/superpowers/plans/2026-10-15-multi-source-si-s3f-p6-switch-on.md <N> $HOME/.cache/s3-ledger/record-<N>.md <NAME=VALUE for each of the task's names>`; one commit of the plan file only, `docs(plan): record <names> of Plan S3-F (Task <N>)`. R2 (explicit confirmation): when `$ref` is a plans branch, `git push origin HEAD:<that branch>` (never forced; a rejected push means the branch moved: stop and ask); when it is `origin/main`, `git push -u origin HEAD` and `gh pr create --repo housegate/housegate --base main` with the commit's subject and the body file `$HOME/.cache/s3-ledger/record-<N>-pr.md`. R3 (explicit confirmation, `origin/main` only): `gh pr merge <number> --repo housegate/housegate --squash`. R4: fetch again and `s3-ledger.py get` the task's names, which must print the values just written, else stop and ask. A task ends only after R4. Ledger copy (D34, as S3-C1 §19 states it): this plan's is `origin/urwt/claude/ms-si-stage3-plans-cf` until the stage-3 plans PR merges, then `origin/main`, so R2 pushes to that branch before the merge and goes through a docs PR after it; the consumed names of Plans S3-B to S3-E4 are read from their own ledger copies, which `ref` names per plan (S3-B's on `origin/main` since housegate#237). When R1's `ref` stops with `no plan 's3f'`, the task stops and asks the user to push the plans branch; it never records into a local checkout.
- **Repositories and worktrees.** Local repositories `/Users/uranuswch/Dev/projects/sentio-network/<repo>`. production: `multi-source-s3f` (Tasks 83–84; `bash ~/.agents/skills/urwt/scripts/urwt-create.sh /Users/uranuswch/Dev/projects/sentio-network/production claude multi-source-s3f`, branch `urwt/claude/multi-source-s3f`, its JSON `path` is `$W`) and `multi-source-s3f-live` (Tasks 85–89, detached at a freshly fetched `origin/main`, never a phase branch, never the user's checkout); housegate: `multi-source-s3f-ledger-<N>` (N = 84 to 89). arbiter-core: none (`stage3reads` is built from `git -C /Users/uranuswch/Dev/projects/sentio-network/arbiter-core archive <CORE_TAG_P5>` into a temporary directory).
- **production checks (Tasks 83–84 and every live commit).** `python3 -B charts/storage-integrity/tests/test_render.py` (all tests; the count printed is recorded); `helm lint charts/storage-integrity -f charts/storage-integrity/tests/legacy-devnet2-values.yaml --namespace sentio-network-devnet2 --strict`, the same with `-f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --strict` and with `--set arbiter.consensusUpdatesEnabled=true --set arbiter.authoritySigningEnabled=false`; `bash scripts/storage-integrity/render-inertness.sh origin/main` printing `<release> identical` for every release (the PR); `bash -n` and `shellcheck -S warning` on both scripts; the runbook checker of §59.8.
- **Commits and PRs.** Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; PR bodies end with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. Markdown is never hard-wrapped; runbook text and comments in English.
- **Remote and live actions** — pushes, PRs, merges, `helmfile sync`, `kubectl apply`/`patch`/`delete`/`exec` that writes, consensus updates, Grafana actions — require explicit user confirmation in chat, each for its own step; commands handed to the user are fish (`bash <script> <args>` or `bash -c '...'` wrappers allowed).

## Appendix B: drafting decisions and cross-task facts

Facts the drafters recorded while drafting and verifying each task in scratch exports. The plan consistency pass replaces this appendix with its final form; until then it is the drafters' record only. `<scratch>` stands for local verification directories that are not part of the plan.

### Review fixes (AgentConnect round 1)

- Finding 2 (P2) and Cursor Bugbot's "Replica hosts empty after placement" (one defect): Plan S3-E4 contract §56 built `replica_hosts` after placement from `Ready()` members only. Every member is placed at `ready_generation` 0, and `MemberReady` comes only in step 6 after the Keeper gate (K86, at least 24 hours), so every list would have gone empty at `PLACEMENT_EPOCH`, removing P3's read failover and failing this plan's step-5 check (`replica-hosts-step5.jsonl` unchanged). The fix is S3-E4 §56's: from placement on, every current member's host from its served `host_indexer_id`, never filtered by `MemberReady`, which attests only the `hg_member_unsafe` replicas reads never use; serving stays each member host's own manifest-gated decision (spec §11). This plan now says so in Review Focus 4, the §58.3 row, the coverage map and Task 86's step-5 check, and Task 87's §8.4 adds `replica-hosts-after-<x>.jsonl` (the lists stay equal to `replica-hosts-step5.jsonl` after each reconciler and `MemberReady`), listed in §59.7's evidence files. Verified with a Go model of the drafted and the fixed `ReplicaHostsFromGroups` on the devnet2 groups (Plan S3-E4 Appendix B); Tasks 84–87 are not drafted, so no runbook block exists to run.
- Finding 3 (P2, check replica counts for the current rollout stage): Task 87's §8.3 required `total_replicas` 6 on the owner after each host, but reconcilers start one host at a time (b, c, a), so after indexer-b the tables of owners a and c hold their owner, the three pool verifiers and member-b only (5), and the mandatory check would stop the rollout before indexer-c. §8.3 now checks, after each host, `owner-replicas-after-<x>.tsv` from the new read-only helper `si_unsafe_replicas` (§59.7): each SI table's replica names on its owner's source ClickHouse (`mapKeys(replica_is_active)` of `system.replicas`, ClickHouse's map from replica name to its active flag) are exactly the owner's SNode, `verifier-1..3` and the members of its group whose reconciler runs, and `total_replicas` is their count, so 5/4/5 after b, 6/5/5 after c and 6 for every table after a (owners a/b/c). Review Focus 3 and the evidence list say so. Verified: replica names are the registering node ids (arbiter-core `dataplane/ddl/pinned.go:11-16`, `build.go:105` at `origin/main`); the `system.replicas` columns `total_replicas` (UInt32) and `replica_is_active` (Map(String, UInt8)) against the ClickHouse system-table documentation; a `python3 -I` model of the three stages reproduced the counts and the drafted check's failure after b.

### Drafting facts (Track B, 2026-10-17; the plan consistency pass consolidates these into the per-task entries above)

#### Task 83

- Task 83 has 7 steps: worktree with the `poetry <chen@sentio.xyz>` identity and base checks (remote reads confirmed), the failing tests, `charts/sentio-node` (commit), `charts/storage-integrity` with the probe rows and the tests (commit), the two scripts (commit, with a stand-in run of `make-member-keys.sh`), a local rehearsal of Tasks 86, 87 and 89's ten commits with ten bites, and the full checks. It pushes nothing; Task 84 adds the runbook to `urwt/claude/multi-source-s3f` and opens `P6_PR`.
- `charts/sentio-node`: `siMember.consensus.{enabled, keysSecret{name, seedKey, replicaSignerKey}, consentJWS}` and `siMember.reconciler.{enabled, passwordSecret{name, key}, metricsPort 9097, resources}` are real defaults (off/empty), not comments; `siMember` is top-level, so the host's `checksum/config` never sees them. The define `sentio-node.siMember.consensus` renders `consensus: {register, [create_replicas], ed25519_seed_file, replica_signer_key_file, consent_file}` and, with the reconciler, `reconciler: {clickhouse_username: si_member_ddl, metrics_listen}` at the end of `storage_integrity.member`. `consent.jws` is the ConfigMap's second key; `checksum/si-member-config` hashes `si-member.yaml` plus `consent.jws: <jws>` only with consensus mode. The keys volume `si-member-keys` (`defaultMode: 0400`, items `ed25519-seed`, `replica-signer-key`) is mounted read-only into `si-member` alone; the pod has no security context (Task 79 refuses group-readable keys; the image runs as root). `si-member-reconciler` runs `/sentio-node si-member --config-path /config/si-member.yaml --role reconciler` with one env (`STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD`), port `rec-metrics`, the ConfigMap and the state volume read-only, no readiness probe (its readiness never removes the member's Service endpoints); the Service keeps `metrics` and `status`.
- The nine §59.1 guards are verbatim and checked whether or not the member is enabled; `$fullname` moved above them (the two token guards name `<fullname>-si-member-status-token`, as S3-C3's guard computes it). Added: the boolean guard, the reconciler port guard, the `siMember.config` consensus/reconciler key guard. S3-C3's reconciler refusal is gone (its test expects the new guard).
- `charts/storage-integrity`: `memberDDL` sits after S3-D's `snodeRestoreGrant`; `passwordSha256.si_member_ddl: ""` is a default; S3-C3's refusal text stays for a non-empty hash without `memberDDL`. The user is rendered after `si_member` with `profile: si_member_ddl`; the profile `si_member_ddl/profile: default` and `si_member_ddl/constraints/merge_tree_max_bytes_to_merge_at_max_space_in_pool/const: ""` after `si_member`'s profile. `memberDDL` needs `lockDefault` (verbatim, first) and `member`. `monitoring.stage3` sits after S3-E4's `stage3Consensus`; the group `<fullname>.stage3-p6` is the VMRule's last; its expr keeps `by (cluster, network)`; `monitoring.stage3.exitAlert requires monitoring.enabled` is added. Schema: `memberDDL` and `stage3` required.
- Grant probe: the order create → nine denials → `constrained` → `DROP … SYNC` (the Bugbot fix of §59.3), measured on ClickHouse 26.3.12.1 (the devnet2 image): contract order gave 60 for `INSERT` and `TRUNCATE`; fixed order 497 × 9, 452, 0. `p6_probe_atoms` models the new statement forms (replicated create with Keeper path, replica and optional merge setting; `CREATE DATABASE … ENGINE`; `DROP TABLE … SYNC`), asked by `probe_atoms` after `p2_probe_atoms`. `si_member_ddl` is not in `SI_PROBE_GRANTS` (its rows never run on a staging installation); its own test checks every grant, the denials named by the contract, the constrained row and the order.
- Scripts: `make-member-keys.sh` accepts OpenSSL 3 or later (the user's `openssl` is 4.0.3; contract §59.5 said "OpenSSL 3"), checks the KECCAK, RFC 8032 and key-1 vectors, refuses a key file that is a symlink, not owned by the user, has group/other bits, has more than one line or is outside [1, n − 1], and an existing public file; keys flow only through pipes and files (`cast wallet new --json` to a file, `jq` to a file); tested with stand-ins on OpenSSL 3.6.4 and 4.0.3 and LibreSSL. `make-clickhouse-logins.sh --add-user si_member_ddl` writes `sentio-node-devnet2-indexer-<x>-si-member-ddl` (key `password`; network → indexer-a's) and is covered by extending `ClickHouseLoginsScriptTest.ADDED`.
- Tests: 20 new (14 `MemberConsensusTest`, 6 `ActualFreshNetworkTest`); amended earlier tests and helpers as listed in contract §59.6's new bullet, nine of them found only by Step 6's rehearsal at the P6 live state (they turned a member off over consensus mode, unpacked exactly one member container, or did not know the reconciler, the P5 config keys or `si_member_ddl`'s engine grant); the rollout writers `p6_manifest_text` and `p6_overlay_text` (si_member_ddl's lines right after si_member's) are what runbook §8.2 uses; `p2_manifest_text`/`p2_overlay_text` keep that order (`_before_member_ddl`).
- Names added while drafting: `siMember.reconciler.metricsPort`, `siMember.reconciler.resources`, port `rec-metrics`, define `sentio-node.siMember.consensus`, `storage-integrity.grants.si_member_ddl`, the guards and refusals listed above, `SI_MEMBER_DDL_PROFILE`, `TEST_P6_PASSWORD_SHA256`, `TEST_CONSENT_JWS`, `DEVNET2_MEMBER_DDL_ROLLOUT`, `STAGE3_P6_RUNBOOK`, `STAGE3_P6_EXIT_SECTION`, `MAKE_MEMBER_KEYS`, `MONITORING_ALERTS["stage3-p6"]`, `P6_MANIFEST_PROFILE_COMMENT`, `P6_OVERLAY_COMMENT`, `member_consensus_values`, `member_ddl_users`, `p6_values`, `p6_probe_atoms`, `p6_member_ddl_users_lines`, `p6_manifest_text`, `p6_overlay_text`, `_before_member_ddl`, `p2_overrides(member_ddl=, si_member_ddl_hash=)`, the local tool `$HOME/.cache/s3f-task83/rollout.py`.
- Verified while drafting (2026-10-17): see Task 83's Notes. Scratch chain: production `febc6c4ac` + S3-B Tasks 16, 19, 20 from their text and stand-ins for S3-B's live commits, then S3-C1 Task 26, S3-C3 Tasks 37–38, S3-C4 Task 47, S3-D Task 53, S3-E4 Task 80 from their text with stand-in ledger values (local corrections: S3-C4 Task 47's schema `required` keeps `backupUser`; S3-D Task 53's three hunks applied with `patch -F3` and two rejects by hand, its overlay values placed without the P2/P3 live states; S3-E4 Task 80's values after `monitoring.replicaReads` and schema `required` with `members`); base 237 OK, after Task 83 257 OK.

## Appendix C: execution record

Written by the plan consistency pass from the release ledger table above (one row per ledger name this plan produces, empty until recorded, in the S3-B D23 shape), together with the run-record list. Not written yet: see `2026-10-15-multi-source-si-s3cf-status.md`.
