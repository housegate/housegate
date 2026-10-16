# Multi-source SI network — Plan S3-E4: stage-3 P5 copy floor, narrowed drain gate, P5 releases and the v21 activation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Status (2026-10-16): draft.** Tasks 76, 77, 80, 81 are drafted and were verified by their drafters in scratch exports; Tasks 78, 79, 82 are not drafted yet; no task has had its adversarial review yet, and the plan consistency pass (which also writes Appendices B and C in final form) and the cross-plan pass have not run. Do not execute this plan before those passes; `2026-10-15-multi-source-si-s3cf-status.md` lists what remains and how to finish it.

**Goal:** Finish the P5 code layer of stage 3 and activate it on devnet2. The arbiter enforces the copy floor of spec §9.3 in Apply and in the leader's WorkSet through one predicate, with the purge and pressure escapes, strict mode and its retryable `COPIES_BACKPRESSURE` refusal, the `under_replicated` signals, the release of a Leaving member (§6.4) and the precondition for raising k; it narrows the drain gate to the tracks an update touches, with floor-held unsafe parts excluded (§10.1), and `arbiter-admin` defaults to an issuance hold once that switch is on. The whole stage-3 arbiter branch (Tasks 64–78) is proven in an integration harness with three SNodes, real members in consensus mode and the network pool, pinned against a frozen pre-stage-3 history, replayed offline against copies of the devnet2 logs and released once as `ARBITER_TAG_P5`. sentio-node ships the `stage3_v1` hosts, the member consent statement and the inert member key configuration (`SN_COMMIT_P5`). production adds the stage-3 alert rules and the plain-body writer value. devnet2 then runs an offline replay and a rehearsal on copies of the live state, rolls every component while it still writes v20, and commits the irreversible v21 activation with every switch off; the plan ends when every voter writes v21.

**Architecture:**
- Task 76 (arbiter): new `fsm/floor.go` with `floorLocked(pp)`, the one copy-floor predicate. `applyScheduleUnsafeCleanup` (Apply) and the WorkSet's `PublishedUncleaned` both call it, so the leader never proposes a cleanup Apply refuses. Strict mode turns the pressure bypass into admission code 13. The raise-k precondition sits in the Apply-only half of `applyUpdateConsensusParams`. The Leaving release is evaluated after every command that can satisfy it and is recorded once. Replicated gauges come through `MetricsSnapshot`; the leader-local `under_replicated` counter and held-cleanup age come through a new orchestrator `FloorMetrics`. Everything is inert until the `checkpoints` switch.
- Task 77 (arbiter): new `fsm/drain_narrowed.go` with `TouchedTracks`, the narrowed gate and the floor-only exclusion. `DrainViewFor(draft)` answers exactly what Apply's drain gate will answer for that draft, while S3-A's `DrainView()` keeps the stage-2 answer. A consensus hold keeps its normalized draft and judges readiness on it. `arbiter-admin consensus update` picks issuance or drain when `--hold` is not given (K79).
- Task 78 (arbiter): the frozen `fsm/testdata/pre_stage3_golden.json` and `TestPreActivationHistoryIsByteIdenticalAcrossStage3`; fake-cluster activation tests (`integration/stage3_p5_test.go`); the chpipeline stage-3 harness (three ClickHouse 26.3 servers, one Keeper, members in consensus mode, the pool) with the §10.4 switch order and the §17 integration list (K80); a new CI job; offline replay of copies of the devnet2 voter logs; full verification; one PR of branch `urwt/claude/multi-source-s3e`; release `ARBITER_TAG_P5` / `ARBITER_DIGEST_P5`. After the cut it rebuilds the one P5 rehearsal directory `$R5` from `ARBITER_TAG_P5`, including the `p5rehearsal` binary, so it holds every binary the P5 runbook runs (decision D37).
- Task 79 (sentio-node): pins `HG_TAG_P5`, `CORE_TAG_P5` and `PROTO_TAG_P5`, so every SNode advertises `stage3_v1` and the embedded ingress answers codes 12 and 13 retryably. It adds `storage-integrity member-consent-statement`, the inert `storage_integrity.member.consensus` keys (files only, never inline keys), the reconciler role `si-member --role reconciler`, the member consensus metrics and the keeper-findings export, and `replica_hosts` from the registry's `DatabaseGroup` once placed, read from each ready member's served `host_indexer_id` (decision D27). It keeps this widened scope so that Plan S3-F needs no code (decision D30), and runs as seven step groups, each test-first with its own commit (File Structure). Release `SN_COMMIT_P5` / `SN_DIGEST_P5`.
- Task 80 (production): `charts/storage-integrity` gains the stage-3 consensus rule group behind `monitoring.stage3Consensus.enabled` (off) and the `arbiter.raft.snapshotBody` writer value (unset). It adds the rehearsal Pod example `docs/examples/storage-integrity-p5-rehearsal-pod.yaml`, the P5 image constants and their render tests. The voter sizing stays pinned; its re-check is a runbook precondition.
- Task 81 (production): the P5 runbook `docs/storage-integrity-devnet2-stage3-p5-v21.md` (replay, rehearsal, verifiers, hosts and si-members, voters, activation, v21 check, rollback, operating rules) and the P5 PR (`P5_PR`), inert on merge. The runbook only reads `$R5` and keeps its live evidence under its own `$si_backup` (D37).
- Task 82 (devnet2, live): offline replay, rehearsal (`P5_REHEARSAL`), verifiers, hosts b → c → a and their si-members, voters (followers first, leader last), re-registration, the activation under a drain hold (`ACTIVATION_EPOCH`), monitoring, every voter writing v21 (`P5_END`).

**Tech Stack:** Go 1.26; Bazel 9.1.0 + Bzlmod + gazelle (arbiter, sentio-node); hashicorp/raft + raft-boltdb; ClickHouse 26.3 (`clickhouse/clickhouse-server:26.3.37.3` and its Keeper in the new stage-3 integration job; the existing 25.8 job is unchanged); Prometheus client_golang; Helm charts with `test_render.py` (Python 3, PyYAML), `helm lint --strict` and helmfile (production); VictoriaMetrics operator CRDs (VMRule); `kubectl`, `arbiter-admin`, the devnet2 runbook helpers and throwaway local tools (`statedigest`, `v20check`, `v21check`, `snapdump`, `p5rehearsal`), never committed.

**Spec:** housegate `docs/superpowers/specs/2026-10-14-multi-source-si-stage3-database-groups-design.md` (PR housegate#236, current, with every review fix). It is binding as clarified by Plan S3-A K1–K10, Plan S3-B K11–K22, the clarifications K23–K77 of Plans S3-C1 … S3-E3 where they apply (in particular K29 member ids, K30 the si-member StatefulSet, K50 static replica hosts, K55 failure-domain naming, K58 explicit host-bound flags, K59 code 13, K63 purposes, K65 the `stage3_v1` release rule, K66 member consensus mode in `CORE_TAG_P5`, K67 member RPC gating, K68 host-bound entries refused until `owner_aware_selection`, K69 k > 1 needs `checkpoints` and `narrowed_drain`, K70 v21 framing, K71 the memory term, K73–K77 groups, Leaving records, re-selection and classes), and K78–K82 below. Binding sections:
- §9.3: the floor in Apply and the WorkSet, base(P), the three escapes, E and |E| by distinct failure domains, strict mode, raising k.
- §6.4: the Leaving release (strict and non-strict, recorded once, never withdrawn).
- §9.2: classes, consumed from S3-E3.
- §10.1: the narrowed drain gate and the issuance hold.
- §10.3: the activation, its gate and the rehearsal.
- §10.4: switch 9's semantics and the step-10 precondition (its execution is Plan S3-F).
- §13.2: through S3-E2's encoding.
- §11: `replica_hosts` after placement.
- §6.1: the consent statement and member keys.
- §15 for arbiter, sentio-node and production.
- §16 row P5.
- §17: the FSM items for the floor, escapes, strict mode, the domain collision, the strict Leaving release, the raise precondition, the narrowed drain gate with the issuance hold and activation with a quarantined track; the offline digest replay of the devnet2 log; the whole integration list.
- §18 (v21 is irreversible).

**Prerequisites:** Plans S3-A and S3-B and the new plans S3-C1, S3-C2, S3-C3, S3-C4, S3-D, S3-E1, S3-E2 and S3-E3. Their Global Constraints and clarifications apply, and so do their contracts: §0–§6 and §7–§15 (S3-A, S3-B), §16–§19 (S3-C1, including the ledger tool of §19), §20–§24 (S3-C2, member config and process), §25–§28 (S3-C3, `siMember`, `MEMBER_IDS`), §29–§32 (S3-C4, Replica status and `replica_hosts`), §33–§37 (S3-D, `ARBITER_TAG_P4`, `SN_COMMIT_P4`, `DEVNET2_FAILURE_DOMAINS`, `P4_END`), §38–§43 (S3-E1, every proto number, canonical type, admission code, wire tag and the member consensus mode, `PROTO_TAG_P5`, `HG_TAG_P5`, `CORE_TAG_P5`), §44–§48 (S3-E2, `Params.Stage3`, the stage3 gate, the `arbiter-admin` stage-3 flags, the v21 container and interned body, the shared arbiter branch) and §49–§52 (S3-E3, groups, Leaving records, seal reason, checkpoints, classes). Tasks 76–78 continue the arbiter branch of Tasks 64–75; this plan's Appendix A continues the numbering (§53–§57), and every consumed name is listed at its head with its source section.

**Base revisions.** Line anchors refer to the revisions read at `origin/main` on 2026-10-15:
- arbiter `fb42260` (v0.12.0), arbiter-core `8f0bc43` (v0.14.1), arbiter-proto `1c6ae67` (v0.11.0), housegate `66d4df7` (v0.19.0), sentio-node `5d2f169`. These are the spec's and Plan S3-A's bases. S3-A to S3-E3 move many of these lines, so the real base of Tasks 76–78 is branch `urwt/claude/multi-source-s3e` after Task 75, on top of `ARBITER_TAG_P4`.
- production `75514e3de`. Its SI paths are byte-identical to Plan S3-B's base `9d3d0326b`: `git diff --stat 9d3d0326b 75514e3de -- charts/storage-integrity charts/sentio-node k8s-sea/sentio-network-devnet2 k8s-sea/clickhouse terraform docs scripts/storage-integrity` prints nothing.

devnet2 expected when Task 82 starts, as the exit records of S3-B (`E_END`), S3-C1 (`P1_END`), S3-C3 (`P2_END`), S3-C4 (`P3_END`) and S3-D (`P4_END`) leave it:
- **Consensus parameters:** epoch `ENROLMENT_EPOCH`, because no plan between S3-B's enrolment and this one commits a parameter update; the runbook reads the epoch live and stops on any other value. Tracks enabled. `si_indexers` {0: `snode-1`, 1: `snode-2`, `INDEXER_C_ID`: `snode-3`}, verifiers `verifier-1..3`, `max_writers` 3, `stage3` unset, snapshot v20.
- **Tracks:** no track quarantined; the P3 and P4 drills released indexer-c.
- **Images and voters:** the three voters at 2Gi memory and 2Gi PVC; all six arbiter pods on `ARBITER_DIGEST_P4`; hosts a, b and c on `SN_DIGEST_P4` with `owner_audit: repair`.
- **ClickHouse:** every SI ClickHouse in posture C, with `table_engines_require_grant`, `si_member` and `/sentio/<shard>/unsafe_inc` from P2.
- **Members:** si-members `member-a`, `member-b`, `member-c` (`MEMBER_IDS`) in P2/P3 mode, unregistered, on the member image the P4 exit record names.
- **Earlier phases:** the P1 backups and the P3 replica-read flag as their exit records leave them.
- **P4 hardening:** interserver step 2, the CNPs and the Keeper allowlist enforced, node pins, `DEVNET2_FAILURE_DOMAINS` recorded. The node pins are six pairwise-distinct nodes or, where devnet2 lacks them, the fallback of decision D39: verifiers share nodes only with each other, never with a source ClickHouse. The P5 rolls change images only and keep every pin.
- **Monitoring:** live with the P0–P4 rule groups.

## Global Constraints

- **Earlier plans apply.** Every Global Constraint and clarification of Plans S3-A and S3-B applies, and through them those of S1-A, S1-B, S2-A and S2-B. So do the cross-plan conventions and the Global Constraints of S3-C1 … S3-E3 (strict decoder, request-only signatures, signed claims, tracks, quarantine, golden files, worktrees, commits, remote actions). So do the cross-plan decisions D25–D40, which continue S3-B's D1–D24. This plan cites by number those that touch it: D25–D27, D30, D31, D33, D34, D36, D37, D39 and D40.
- **Activation model.** Tasks 76–79 add behaviour only behind committed state: the `checkpoints` switch (Task 76's floor, metrics and admission refusal), `strict_copies` and a raised `min_safe_copies` (Task 76), `narrowed_drain` (Task 77) and the default-off `storage_integrity.member.consensus` keys (Task 79). With `stage3` unset or every switch off, every Apply answer, WorkSet, drain refusal and hold readiness is byte-identical to the P4 base, and the P5 binary writes v20 until `stage3` commits (S3-E2 contract §46). This plan adds no proto number, Raft tag, field or `ConsensusAdmin` method; every number it uses is S3-E1 contract §39's. Those numbers sit on the `PROTO_TAG_P4` base that S3-D produces unconditionally, where `RebalanceGroup` is `ConsensusAdmin` method 16 and `MarkMemberLagging` 17, of 18 (decision D26).
- **Ordering.** Tasks 76 → 77 → 78 continue `urwt/claude/multi-source-s3e` after Task 75, and no arbiter release is cut from `main` between Task 64's pin and Task 78's cut (K65). Task 79 may start once `CORE_TAG_P5` and `HG_TAG_P5` exist and `SN_COMMIT_P4` is recorded (its branch starts from sentio-node `origin/main` at or after `SN_COMMIT_P4`, keeping the sentio-node release order). Task 80's branch starts from `origin/main` after `P4_PR` merged. Task 82 starts only once `P4_END` is on the ledger copy and `P5_PR` has merged. On devnet2 the order is replay and rehearsal first, then verifiers, hosts (b, c, a) with their si-members, voters (followers first, leader last), re-registration and the activation (K81).
- **One irreversible step.** The activation update of runbook §7.2 is the only consensus change of this plan and cannot be undone: from it on the voters write v21, which `ARBITER_TAG_P4` refuses, every later update carries `stage3`, and no component may run an image older than this plan's again. Before it, every component can return to its P4 image. After it, the only lever is the plain-body writer value (runbook §11).
- **production rule (as Plan S3-B).** Chart, test, example-manifest and runbook changes go through one PR, `P5_PR` (Tasks 80–81), inert on merge (render-and-cmp of every release against `origin/main`). Image pins and single-value flips are single-service commits pushed to `main` after it merges. The outcome record is a docs-only PR. The rehearsal Pod is applied by hand (`kubectl apply --dry-run=server` first) and deleted in the same runbook section. No helmfile runs on `k8s-sea/clickhouse` (K22). Every live mutation needs explicit user confirmation in chat, for that step only.
- **Runbook conventions (as Plan S3-B).**
  - Every command block is bash with `set -eo pipefail`, from a production checkout at `origin/main`; a live task runs each block from its own block file after sourcing `helpers.sh` (D24), and `helpers.sh` holds the §0 chain of D20.
  - One stage per window; `Confirm before running.` precedes every mutating block.
  - Every cross-block value lives under `$si_backup` (mode 700, created fresh); read-only checks are separate re-runnable blocks that `tee` into `$si_backup`; writes are never re-run.
  - Commands handed to the user in chat are fish; `yq` is not installed.
  - **Rehearsal directory (decision D37).** There is one P5 rehearsal directory, `$R5` = `$HOME/si-devnet2-multisource-s3e-rehearsal`. Task 78 writes everything in it that the runbook runs. The runbook only reads it (it checks tags and SHA-256s, then runs the binaries) and never writes, rebuilds or deletes anything there. The live rehearsal's evidence, copies and containers stay under the runbook's own `$si_backup`.
- **Keys and passwords.**
  - The authority key never leaves the voter pods and the rehearsal Pod; both read it as `ARBITER_AUTHORITY_PRIVATE_KEY_HEX` from Secret `storage-integrity-devnet2-si-v2-authority`.
  - The rehearsal's signed token and its outputs other than digests, version bytes and counts never leave the Pod.
  - Member keys (ed25519 seed, `replica_signer` key) exist only as files mounted from Secrets, never in YAML, on argv or in logs; the consent command prints only the JWS.
  - A member's `replica_signer` key is made by the user in their own terminal, with Foundry `cast wallet new` or an equivalent the user prefers (decision D40). It reaches a host only as a Secret-mounted file. No task of this plan makes one: devnet2 registers no member in P5, and the tests use the S3-E1 fixture keys.
  - Agents never print, echo or pass a password or key on argv.
- **Never edit.** The golden and vector files that S2-A, S3-A and the conventions list (`pre_tracks_golden.json`, the `l3_track_golden_test.go` constants, `conformance/stage3_reads_test.go`) and the frozen fixtures of S3-E1 to S3-E3. Decision D25's exception lets only S3-D and S3-E1 change the count literals of `conformance/stage3_reads_test.go`. This plan changes nothing in that file: it adds no method, tag or field. New frozen fixture: `fsm/testdata/pre_stage3_golden.json`, captured once from `ARBITER_TAG_P4` and never rewritten.
- **Read views (decision D33).** This plan serves no new read-view field. `DrainView.Narrowed`, `TrackDrainView.Touched`, `WorkSet.FloorHeld` and `CleanupWork.UnderReplicated` are in-process Go fields with no proto change, and the floor reaches operators only through the contract §53 metrics. Its runbook and live task read only fields that are already served, such as `consensus show`'s parameters with `stage3`, the feature book, S3-A's drain and hold reads, and the metrics. S3-E2's cutover `IncarnationSeq`, S3-E3's `ReadyVerifyMembers` and `Reselection.ExpectedSet` stay unserved. The only registry-view field this plan needs that was not served before is `GroupMember.host_indexer_id`, which S3-E1 adds (decision D27).
- **Ledger.** Values are recorded only by the tool and procedure of S3-C1 contract §19: `$HOME/.cache/s3-ledger/s3-ledger.py` and the record steps R1–R4 of S3-B decision D23, into this plan's Appendix C, which keeps the `| Name | Value | Recorded by |` shape. Each value is read only from the ledger copy, never predicted. Under decision D34 the ledger copy of this plan is housegate branch `urwt/claude/ms-si-stage3-plans-cf` until the plans PR merges, and `main` from then on. That is the first ref holding the plan file, as `s3-ledger.py ref` resolves it.
- **Repository commands, worktrees, commits, releases:** contract §57, Plan S3-A contract §6 and Plan S3-B contract §15. Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; PR bodies end with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`; code comments and runbook text are in English; markdown is never hard-wrapped; nothing uses placeholders.
- **Hunk rule.** Hunks are anchored on the base revisions, and tasks were drafted in parallel with S3-C1 … S3-E3. When a hunk overlaps an earlier edit, apply the later intent on top. That covers S3-A Tasks 7–8 (`fsm/reads_drain.go`, `server/consensus_hold.go`, `cmd/arbiter-admin`), S3-E2/E3's edits of `apply.go`, `admission.go`, `consensus_updates.go`, `reads_work.go` and `metrics`, and S3-B/S3-C3/S3-D's edits of `monitoring.yaml` and `test_render.py`. The contract and the named tests are binding; line anchors are guidance.
- **References.** `contract §N` means Appendix A: §0–§6 of Plan S3-A, §7–§15 of Plan S3-B, §16–§52 of Plans S3-C1 … S3-E3 and §53–§57 of this plan. A task group's "Facts" point to Appendix B; Appendix C records the release ledger values and the run records as they are made.

## Plan clarifications (binding)

The spec and the earlier plans leave these details open; each is resolved here and binds Plan S3-E4 and, where named, Plan S3-F.

- **K78 — Floor signals and held-cleanup age (spec §9.3, §16 P6).**
  - **Spec text and gap:** §9.3 says "Every escape, and |E| falling below k − 1, raises an `under_replicated` event". The P6 exit requires "bounded held-cleanup age", which no metric measures. FSM events are hint-only and dropped on a full channel (`arbiter:fsm/watch.go:5-7`), and the FSM has no clock.
  - **Resolution, replicated gauges:** `under_replicated` is signalled three ways. The first is deterministic gauges every replica derives from committed state through `MetricsSnapshot`: `arbiter_under_replicated_groups`, `arbiter_floor_held_cleanups` and `arbiter_safe_copies_min{indexer_id}`, beside S3-E3's `arbiter_group_members{owner,class}`.
  - **Resolution, leader-local signals:** the second is the counter `arbiter_under_replicated_total{reason}`, incremented once per cleanup the leader proposes that Apply answers Applied under a reason. The third is the hint `EventUnderReplicated`. Held-cleanup age is the gauge `arbiter_floor_held_cleanup_age_seconds`: the maximum, over promotions the floor alone holds, of the time since this leader's loop first saw each one held. It is 0 on a follower and restarts at a leadership change, so it can understate after a failover and never overstates.
  - **Why:** a count carried by droppable events would be wrong, and a replicated timestamp would be a new consensus field outside S3-E1's numbers. Plan S3-F's exit uses this gauge (K87).
- **K79 — The sunset of S3-A's K6 (spec §10.1).**
  - **Spec text and gap:** §10.1 has `arbiter-admin` take an issuance hold before signing. S3-A K6 made `--hold drain` the default "until the narrowed drain gate exists", because an issuance hold can wait forever on a half-promoted multi-partition frontier block.
  - **Resolution:** without an explicit `--hold`, `arbiter-admin consensus update` reads the leader's committed `stage3`. It takes `issuance` when `narrowed_drain` is on and the update touches no track (`TouchedTracks` of the committed and target `si_indexers` is empty), and `drain` otherwise, so before switch 9 the default stays drain. It prints its choice and the reason on stderr; an explicit `--hold` always wins.
  - **Why:** once narrowed, the readiness of an update that touches no track does not depend on any track's in-flight work, so issuance cannot wait on it. An enrolment or a signer rotation still drains the track it touches, where K6's hazard remains.
- **K80 — Integration scope (spec §17 Integration, P5 row).**
  - **Spec text:** §17 asks for "three SNodes with members and the network pool on one Keeper; concurrent writes to every owner; killing one member mid-stream; quarantining an owner while its tables are read from members; one indexer down stalling only its own track".
  - **Resolution, harness:** Task 78's harness runs three SNodes, each on its own ClickHouse `26.3.37.3` with `table_engines_require_grant` and the member users. Those users get S3-C2 §23.2's grant lists, and on `hg_rstage.*` the explicit rights of decision D36, never `ALL`. The harness also runs one member per peer host in consensus mode from `CORE_TAG_P5` (K66), the three pool verifiers and one Keeper. It activates with every switch off and turns each switch on in the order of §10.4: steps 4 and 6 included, then k = 2, the floor holding a cleanup and the narrowed gate committing an update over it. Concurrent writes go to every owner, plus the four §17 failure scenarios.
  - **Resolution, outside the harness:** strict mode and every single-rule edge case stay FSM tests of their own tasks (64–77), because devnet2 never turns strict mode on in this stage. The quarantined-owner case checks the member's serving state (S3-C4's `member/serve`), not a HouseGate read, which S3-C4 already proves.
  - **Why:** the harness proves the composition the spec lists, while CI cost stays bounded.
- **K81 — The rehearsal (spec §10.3).**
  - **Spec text and gap:** §10.3 asks for "a disposable three-voter cluster restored from a copy of the live state, including voter restarts from v21 snapshots and a forced-mismatch run". It does not say where the cluster runs, how it signs without the authority key leaving the cluster, or how an older binary's refusal is shown.
  - **Resolution, the tool:** the rehearsal is the throwaway in-package test binary `p5rehearsal`. Task 78 builds it from a `git archive` export of `ARBITER_TAG_P5` into `$R5/bin/new/p5rehearsal`, the one P5 rehearsal directory (decision D37). The runbook only checks it and copies it. It runs inside the throwaway Pod `storage-integrity-si-v2-p5-rehearsal` in `sentio-network-devnet2`, which reads the authority key from the voters' Secret. Its input is copies of the three voters' `raft` directories, taken when each copy's drain gate passes.
  - **Resolution, the cluster:** the three voters are three FSMs in one process, each restored from its own copy. They are fed the same signed activation entry in the same order, which is everything Raft guarantees an FSM; stage 3 changes no Raft code, only the snapshot container. Each FSM persists v21 and then restarts by restoring its own v21 container. A forced-mismatch run through S3-E2 contract §46's interner seam must persist the plain body and count the fallback. Finally `ARBITER_TAG_P4`'s `v20check` (`$R5/bin/old/v20check`) must refuse the v21 container at its version byte (`arbiter:fsm/snapshot.go:274`).
  - **Resolution, isolation:** the tool has no network peer, orchestrator or anchor. Nothing leaves the Pod but digests, version bytes and counts, and the Pod is deleted before any component rolls. The rehearsal runs before the first roll, so a failure costs no rollback. The real voters' restarts from v21 are proven in integration (Task 78) and observed live after the activation (runbook §8).
  - **Why:** a real Raft cluster restored from copies of the live logs carries the live voters' addresses and could disrupt them, and the authority key must not leave the cluster.
- **K82 — Activation preconditions on devnet2 (spec §10.3).**
  - **Spec text:** §10.3 requires the stage-2 drain gate, the leader's voter probe over the exact voter set, and every registered, non-evicted data-plane node not on a quarantined track advertising `stage3_v1`.
  - **Resolution, before signing:** the runbook requires, before signing:
    - (i) every registered data-plane node, a quarantined track's SNode included, upgraded and re-registered with the current leader advertising `stage3_v1`;
    - (ii) no `MEMBER_IDS` id registered: P2/P3 members are not data-plane registrations and do not count, and K68 refuses a host-bound entry anyway;
    - (iii) every voter on `ARBITER_DIGEST_P5` and exactly the three Raft voters probed (metadata key of S3-E2 §45);
    - (iv) the stage-2 drain gate reached under `--hold drain`;
    - (v) `P4_END` on the ledger copy;
    - (vi) `arbiter_state_bytes` at most L/4 and the rehearsal's Persist peak at most L.
  - **Resolution, the exemption and its consequence:** the quarantined-track exemption is exercised only in integration (Task 78). An unupgraded quarantined SNode can never register again after the activation, by design, which runbook §11 records as an operating rule.
  - **Why:** these preconditions make the irreversible update depend on nothing the gate cannot see.

## Release ledger (consumed and produced)

| Name | Meaning | Resolved by |
|---|---|---|
| `PROTO_TAG_P5`, `HG_TAG_P5`, `CORE_TAG_P5` | the S3-E1 releases: every v21 number, codes 12 and 13 at the ingress, canonical types, wire tags 38–42, member consensus mode, `stage3_v1` in `LocalNodeFeatures` | Plan S3-E1, Appendix C |
| `ARBITER_TAG_P4` / `ARBITER_DIGEST_P4` | the arbiter release on every arbiter pod at `P4_END`; the replay's `old` build, the rollback image and the base of branch `urwt/claude/multi-source-s3e` | Plan S3-D, Appendix C |
| `SN_COMMIT_P4` / `SN_DIGEST_P4` | the sentio-node release on every host at `P4_END`; the hosts' rollback image | Plan S3-D, Appendix C |
| `DEVNET2_FAILURE_DOMAINS` | each enrolled indexer's `node:<hostname>`; the activation's `indexer_failure_domains` | Plan S3-D, Appendix C |
| `P4_END` | the end of P4 on devnet2; Task 82 starts only after it | Plan S3-D, Appendix C |
| `MEMBER_IDS` | `member-a`, `member-b`, `member-c`; none may be registered at the activation (K82) | Plan S3-C3, Appendix C |
| `INDEXER_C_ID`, `ENROLMENT_EPOCH` | indexer-c's id (restated in the domains) and the epoch the activation expects | Plan S3-B, Appendix C |
| `ARBITER_TAG_P5` / `ARBITER_DIGEST_P5` | the arbiter release of the whole stage-3 branch (Tasks 64–78) and its image index digest | Task 78, release step |
| `SN_COMMIT_P5` / `SN_DIGEST_P5` | sentio-node `main` commit with Task 79 and its image index digest | Task 79, release step |
| `P5_PR` | the merged production PR carrying Tasks 80 and 81 | Task 81 |
| `P5_REHEARSAL` | the date the rehearsal on copies of the live state printed `rehearsal ok` (its record: the run record) | Task 82 |
| `ACTIVATION_EPOCH` | the epoch the irreversible v21 activation committed | Task 82 |
| `P5_END` | the end of P5 on devnet2, every voter writing v21 (its record: `P5 run and the v21 activation`) | Task 82 |

Task 82 writes its values only once all three exist. Every value is read afterwards only from the ledger copy with `s3-ledger.py get` (S3-C1 contract §19); none is predicted. Plan S3-F consumes `ARBITER_DIGEST_P5`, `SN_DIGEST_P5`, `ACTIVATION_EPOCH` and `P5_END`.

## Review Focus

1. **Apply and the WorkSet disagree about a cleanup.**
   - Expected with `checkpoints` off: both are byte-identical to the base.
   - Expected with it on: both call `floorLocked`. At k = 1 only the new publication clauses bite, identically in both. At k = 2 a cleanup waits until members of E in k − 1 distinct failure domains have checkpoints that reflect it and do not exclude its table. Two members in one domain count once. A non-Active incarnation and the pressure bounds escape, and each escape raises `under_replicated`.
   - Expected for exempt work: rejected-part cleanups are never gated, and a cleanup scheduled before the switch is not re-checked. A disagreement would make the leader propose refused cleanups forever, or clean before the floor holds.
   - Tests: Task 76 `TestFloorApplyAndWorkSetAgreeOnEveryCleanup`, `TestFloorSwitchOffIsByteIdentical`, `TestFloorIsDormantAtKOne`, `TestFloorHoldsCleanupUntilKMinusOneDomainsReflect`, `TestFloorCountsTwoMembersInOneDomainOnce`, `TestFloorParamsUpdateThatCollidesDomainsOnlyLowersTheCount`, `TestFloorPurgeExemptionForANonActiveIncarnation`, `TestFloorPressureBypassPerPartitionAndPerTable`, `TestRejectedPartCleanupIsNeverGatedByTheFloor`, `TestCleanupScheduledBeforeTheCheckpointsSwitchIsNotRechecked`; Task 78 `TestPipeline_Stage3FloorAtKTwoHoldsCleanupUntilAMemberCheckpoints`.
2. **Strict mode, the Leaving release and raising k at their edges.**
   - Expected in strict mode: the count needs k − 1 domains, `lagging` members stay in E, and pressure turns into a retryable `COPIES_BACKPRESSURE` that leaves state unchanged and clears once the floor is met.
   - Expected for the Leaving release: outside strict mode it uses the E frozen at the `RebalanceGroup`. In strict mode it is counted as E is now, never counting the Leaving member, and it waits when the sole caught-up member is replaced by a Bootstrapping one. It is recorded once and never withdrawn, and it is evaluated after every checkpoint, parameter update, rebalance, lagging mark and eviction.
   - Expected for raising k: refused, with state unchanged, unless every Active incarnation has caught-up counting members in k − 1 distinct domains other than its owner's whose checkpoints exclude nothing.
   - Tests: Task 76 `TestStrictModeRequiresKMinusOneDomains`, `TestStrictModeKeepsLaggingMembersInE`, `TestStrictModeTurnsPressureIntoCopiesBackpressure`, `TestCopiesBackpressureLeavesStateUnchanged`, `TestCopiesBackpressureClearsOnceTheFloorIsMet`, `TestNonStrictLeavingReleaseUsesTheFrozenE`, `TestStrictLeavingReleaseWaitsWhenTheSoleCaughtUpMemberIsReplacedByABootstrappingOne`, `TestLeavingReleaseIsRecordedOnceAndNeverWithdrawn`, `TestLeavingReleaseIsEvaluatedAfterEveryTriggeringCommand`, `TestRaisingKRequiresCaughtUpCountingMembersInKMinusOneDomains`, `TestRaisingKRefusalLeavesStateUnchanged`.
3. **A consensus update after switch 9.**
   - Expected for an update that touches no track (verifiers, `stage3`, authority): it commits while another track has an unacknowledged promotion and while the floor holds a cleanup.
   - Expected for an update that rotates a signer or enrols an indexer: it waits for that track's in-flight work, floor-held parts excepted.
   - Expected for the switch-on and the views: the update that turns `narrowed_drain` on is still judged by the stage-2 gate. `DrainViewFor(draft)` answers exactly Apply's refusal, while `DrainView()` keeps the stage-2 answer.
   - Expected for holds: readiness follows the draft, and a DRAIN hold records only touched tracks' frontiers. `arbiter-admin` defaults to issuance only once `narrowed_drain` is on and the update touches no track, and an explicit `--hold` wins.
   - Tests: Task 77 `TestNarrowedDrainGateRefusesOnlyTouchedTracksWork`, `TestNarrowedDrainGateIgnoresFloorOnlyHeldParts`, `TestNarrowedDrainGateCountsPartsHeldForAnotherReason`, `TestNarrowedDrainSwitchOnIsJudgedByTheStage2Gate`, `TestAnUpdateThatTouchesNoTrackPassesWithInFlightWork`, `TestDrainViewForEqualsApplysDrainRefusal`, `TestDrainViewKeepsTheStage2Refusal`, `TestIssuanceHoldIsReadyForAnUpdateThatTouchesNoTrack`, `TestDrainHoldRecordsOnlyTouchedTracksAfterTheNarrowedDrainSwitch`, `TestConsensusUpdateDefaultsToIssuanceOnceTheLeaderReportsNarrowedDrain`, `TestConsensusUpdateDefaultsToDrainForAnUpdateThatTouchesATrack`, `TestExplicitHoldFlagOverridesTheDefault`; Task 78 `TestNarrowedDrainUpdateCommitsWhileTheFloorHoldsACleanup`.
4. **The irreversible activation on devnet2.**
   - Expected for the pre-stage-3 history: it replays byte-identically under the P5 binary, which writes v20 until `stage3` commits, and the copies of the live logs replay to identical digests under `ARBITER_TAG_P4` and `ARBITER_TAG_P5`.
   - Expected for the rehearsal, before any roll: three equal digests, the activation applied, interned v21 persisted and restored with equal digests, a forced mismatch persisting plain with the fallback counted, and the previous release refusing the v21 container at its version byte. The signed token never leaves the rehearsal Pod, and the Pod is gone before §4.
   - Expected for the update: the activation with every switch off changes nothing but the encoding. It commits only when every data-plane node advertises `stage3_v1` to the current leader, no member id is registered and every voter is probed; a quarantined track's SNode is exempt (proven in integration only).
   - Tests: Task 78 `TestPreActivationHistoryIsByteIdenticalAcrossStage3`, `TestStage3ActivationWithEverySwitchOffChangesNothingButTheEncoding`, `TestStage3ActivationExemptsAQuarantinedTracksSNode`, `TestStage3ActivationRefusesAnUnupgradedDataPlaneNode`, `TestStage3VotersRestartFromV21Snapshots`, `TestStage3ForcedPlainBodyAfterActivation`, `TestPipeline_Stage3SwitchOnInOrder`; checks: P5 runbook §2, §3, §7, §8.
5. **Member keys and the consent statement on a host.**
   - Expected for keys: an inline seed or `replica_signer` key in YAML is refused at load, a key file readable by group or others is refused at start, and the consensus keys are off by default, so nothing registers in P5.
   - Expected for the consent command: it binds exactly the member's public values to the host's network, genesis and indexer id; it refuses the host's own SNode id and malformed values; it verifies its JWS before printing and writes only the JWS to stdout. A consent file that does not bind the member's keys is refused.
   - Expected for `replica_hosts`: it keeps S3-C4's static list until the registry holds a group. From then on it follows the group's ready members through their served `host_indexer_id` (decision D27), never through a node-id-to-host mapping of its own.
   - Tests: Task 79 `TestLoadSIMemberConfigRefusesAnInlineMemberKey`, `TestStorageIntegrityMemberConsensusValidation`, `TestMemberConsensusModeIsOffByDefault`, `TestMemberKeyFilesMustNotBeGroupOrWorldReadable`, `TestMemberConsentFileMustBindTheMembersKeys`, `TestMemberConsentStatementPrintsOnlyTheJWS`, `TestMemberConsentStatementBindsTheMembersPublicValues`, `TestMemberConsentStatementRefusesTheHostsSNodeNodeID`, `TestReplicaHostsKeepTheStaticListBeforePlacement`, `TestReplicaHostsFollowTheDatabaseGroupOncePlaced`.

## Task order and dependencies

Plan S3-E3 complete on the branch (Task 75), with the S3-E1 ledger (`PROTO_TAG_P5`, `HG_TAG_P5`, `CORE_TAG_P5`) and `ARBITER_TAG_P4` on the ledger copy:
- 76 → 77 → 78 (arbiter, one branch `urwt/claude/multi-source-s3e`, released once as `ARBITER_TAG_P5` / `ARBITER_DIGEST_P5`).
- 79 (sentio-node, `SN_COMMIT_P5` / `SN_DIGEST_P5`) needs `CORE_TAG_P5`, `HG_TAG_P5` and a sentio-node `main` at or after `SN_COMMIT_P4`, so it may run in parallel with 76–78.
- 80 → 81 (production, one branch from `origin/main` after `P4_PR` merged, `P5_PR`). Task 80 needs `ARBITER_DIGEST_P5` and `SN_COMMIT_P5` / `SN_DIGEST_P5` for its constants and the rehearsal Pod image; its other hunks may be prepared earlier.
- 82 (devnet2) needs `P5_PR` merged and `P4_END` recorded.

Within Task 82, the order is fixed: replay → rehearsal (`P5_REHEARSAL`) → verifiers → hosts b, c, a → their si-members → voters → re-register → activation (`ACTIVATION_EPOCH`) → monitoring → v21 on every voter (`P5_END`). Task 82's replay and rehearsal run the binaries Task 78 left in `$R5` after its rebuild from `ARBITER_TAG_P5`, and only read them (decision D37). Tasks 76 and 77 touch the same `fsm` files as S3-E2/E3 and S3-A Tasks 7–8 and apply on top of them (hunk rule). A task that reads a ledger value starts only once the recording task's record has passed R4. Plan S3-F starts when `P5_END` exists.

## Task list

| Task | Repository | Deliverable |
|---|---|---|
| 76 | arbiter | `fsm/floor.go`: `floorLocked(pp)` shared by `applyScheduleUnsafeCleanup` and the WorkSet; base(P) publication clauses; purge and pressure escapes; E counted by distinct failure domains; strict mode and code-13 `COPIES_BACKPRESSURE`; the raise-k precondition; Leaving release evaluation; `under_replicated` gauges, counter and hint; `arbiter_floor_held_cleanup_age_seconds` (K78) |
| 77 | arbiter | `fsm/drain_narrowed.go`: `TouchedTracks`, the narrowed gate, floor-only holds not in flight; `DrainViewFor(draft)` keeping `DrainView()`; hold readiness on the draft; `arbiter-admin` default `--hold` (K79) |
| 78 | arbiter | `fsm/testdata/pre_stage3_golden.json` with `TestPreActivationHistoryIsByteIdenticalAcrossStage3`; fake-cluster activation tests; the chpipeline stage-3 harness with three SNodes, members and the pool, every switch in order (K80); CI job `integration-clickhouse-stage3`; offline replay of the devnet2 logs; full verification; PR; release `ARBITER_TAG_P5` / `ARBITER_DIGEST_P5`; `$R5` rebuilt from `ARBITER_TAG_P5` with `p5rehearsal` and `p5-files.sha256` (D37) |
| 79 | sentio-node | Widened scope (D30), in seven step groups: pins `HG_TAG_P5`, `CORE_TAG_P5`, `PROTO_TAG_P5` (`stage3_v1` hosts, codes 12/13 at the ingress); `storage-integrity member-consent-statement`; inert `storage_integrity.member.consensus` key files; the reconciler role `si-member --role reconciler` (`storage_integrity.member.reconciler`, `STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD`); member consensus metrics and `sentio_node_storage_integrity_keeper_findings`; `replica_hosts` from `DatabaseGroup`, readiness and the served `host_indexer_id` (D27); release `SN_COMMIT_P5` / `SN_DIGEST_P5` |
| 80 | production | stage-3 consensus rule group (off), `arbiter.raft.snapshotBody` (unset), P5 image constants, rehearsal Pod example, render tests; voter sizing unchanged |
| 81 | production | runbook `docs/storage-integrity-devnet2-stage3-p5-v21.md`; the P5 PR (`P5_PR`) |
| 82 | devnet2 | P5 live: replay, rehearsal, verifiers, hosts and si-members, voters, the irreversible v21 activation, monitoring, v21 on every voter (`P5_REHEARSAL`, `ACTIVATION_EPOCH`, `P5_END`) |

## File Structure

Line anchors in the tasks refer to the base revisions of the header.

**arbiter** (Tasks 76–78, branch `urwt/claude/multi-source-s3e`)
- **Task 76:**
  - `fsm/floor.go` (new), `fsm/floor_test.go`, `fsm/floor_strict_test.go`, `fsm/floor_leaving_test.go`, `fsm/floor_raise_test.go` (new).
  - `fsm/apply.go` (`applyScheduleUnsafeCleanup` `:960-1020`; `applyEvictNode` `:121-149`), `fsm/reads_work.go` (`:317-364`), `fsm/reads_work_types.go` (`CleanupWork`, `WorkSet`), `fsm/admission.go` (`:133-158`), `fsm/consensus_updates.go` (`:51-87`, before `ConsensusEpoch++`), `fsm/watch.go` (`EventUnderReplicated`), `fsm/reads_metrics.go`.
  - S3-E3's `applyMemberCheckpoint`, `applyRebalanceGroup` and `applyMarkMemberLagging` files (release hook).
  - `metrics/metrics.go`, `metrics/floor.go` (new); `orchestrator/deps.go`, `orchestrator/promotion.go`, `orchestrator/floor_metrics.go` (new); `cmd/arbiter/services.go` (wiring); `README.md`.
- **Task 77:**
  - `fsm/drain_narrowed.go`, `fsm/drain_narrowed_test.go` (new); `fsm/reads_drain.go` (S3-A Task 7); `fsm/consensus_updates.go` (`:74-78`).
  - `server/consensus_hold.go`, `server/consensus_hold_rpc.go` (S3-A Task 8), `server/consensus_hold_test.go`.
  - `cmd/arbiter-admin/main.go`, `cmd/arbiter-admin/consensus_hold_test.go`; `README.md`.
- **Task 78:**
  - `fsm/pre_stage3_golden_test.go`, `fsm/testdata/pre_stage3_golden.json` (new).
  - `integration/stage3_p5_test.go` (new), `integration/tracks_harness_test.go`; `integration/chpipeline/harness_stage3_test.go`, `integration/chpipeline/stage3_test.go` (new), `integration/chpipeline/BUILD.bazel`.
  - `scripts/ci/stage3-pipeline.sh`, `scripts/ci/stage3-clickhouse-config.xml`, `scripts/ci/stage3-clickhouse-users.xml`, `scripts/ci/stage3-keeper.xml` (new); `.github/workflows/ci.yml`; `README.md`.
  - Throwaway, never committed: the PR body beside the worktree, and `$R5` = `$HOME/si-devnet2-multisource-s3e-rehearsal`. `$R5` is the one P5 rehearsal directory (decision D37): S3-E2 Task 67's directory, created by this task if it is missing. It gets `new_tag.txt`, `bin/new` rebuilt from `ARBITER_TAG_P5`, `p5rehearsal/p5rehearsal_test.go`, `bin/new/p5rehearsal`, `v21-fixture.container`, `merge-commit`, `arbiter-release`, `cut-release.sh` and, last, `p5-files.sha256`.

**sentio-node** (Task 79, branch `urwt/claude/multi-source-s3e4`). Task 79 keeps its widened scope (decision D30). Its drafter writes it as these seven step groups, in this order. Each group is test-first (failing test, run, code, run) and ends in its own commit, and no group starts before the previous one is green:
1. **Pins.** `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock`; tests `TestPinnedArbiterCoreAdvertisesStage3V1` and `TestEmbeddedIngressAnswersStage3AdmissionCodesRetryably`.
2. **Consent command.** `commands/storage_integrity.go`; `standalone/storage_integrity_member_consent.go`, `standalone/storage_integrity_member_consent_test.go` (new); `standalone/BUILD.bazel`; the four `TestMemberConsentStatement…` tests.
3. **Member consensus keys.** `config/si_member.go` (S3-C2's file: `StorageIntegrityMemberConsensus`, the `Consensus` field, the inline-key refusals), `config/si_member_test.go`; `simember/consensus_keys.go`, `simember/consensus_keys_test.go` (new: `loadMemberConsensusConfig`, the mode and consent checks); `simember/run.go` (sets `member.Config.Consensus`); tests `TestLoadSIMemberConfigRefusesAnInlineMemberKey`, `TestStorageIntegrityMemberConsensusValidation`, `TestMemberConsensusModeIsOffByDefault`, `TestMemberKeyFilesMustNotBeGroupOrWorldReadable`, `TestMemberConsentFileMustBindTheMembersKeys`.
4. **Reconciler role.** `config/si_member.go` (`StorageIntegrityMemberReconciler`, the `Reconciler` field, `StorageIntegrityMemberDDLClickhousePasswordEnv`); `commands/si_member.go` (`--role`); `simember/reconciler.go`, `simember/reconciler_test.go` (new); tests `TestSIMemberReconcilerRoleRunsTheUnsafeReconcilerAsItsOwnUser` and `TestSIMemberReconcilerRoleRefusals`.
5. **Metrics.** `simember/run.go` and `simember/metrics.go` (S3-C2's: `role.ConsensusMetrics()` on the si-member registry); the host's keeper-findings export on the embedded HouseGate registry. That export goes in the existing `standalone/storage_integrity_reconciler_metrics.go` collector (`storageIntegrityReconcilerCollector`, which already reads `tableset.Stats` through `TableSetStats()`), with `standalone/storage_integrity_reconciler_metrics_test.go`; tests `TestSIMemberRegistersTheConsensusMetrics` and `TestKeeperFindingsAreExportedWithEveryReason`.
6. **`replica_hosts`.** `storageintegrityadapter/tablestate/replica_hosts.go`, `replica_hosts_test.go` (new: `ReplicaHostsFromGroups` over the served `host_indexer_id`, decision D27), and S3-C4's status and info wiring; the three `TestReplicaHosts…` tests.
7. **Docs and release.** `README.md` (member key files, with the user-made `replica_signer` key of decision D40; the consent command; the reconciler role); S3-B §15's checks; the PR; `SN_COMMIT_P5` / `SN_DIGEST_P5` and their ledger record.

**production** (Tasks 80–81, branch `urwt/claude/multi-source-s3e4`; Task 82 from `multi-source-s3e4-live`)
- **Task 80:**
  - `charts/storage-integrity/values.yaml`, `values.schema.json`, `templates/arbiter.yaml` (`raft:` block), `templates/monitoring.yaml` (S3-B Task 13, rule group `<fullname>.arbiter-stage3`), `templates/validate.yaml`.
  - `charts/storage-integrity/tests/test_render.py`; `docs/examples/storage-integrity-p5-rehearsal-pod.yaml` (new); `.github/workflows/storage-integrity-chart.yml` (paths).
- **Task 81:** `docs/storage-integrity-devnet2-stage3-p5-v21.md` (new); `docs/storage-integrity-devnet2-multi-source-tracks.md` (pointer to the P5 runbook in §11).
- **Task 82 commits:**
  - `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml` (verifier and voter digest, `monitoring.stage3Consensus.enabled`);
  - the three hosts' source-image files and `docs/examples/storage-integrity-fresh-source-image.yaml`;
  - the three hosts' `siMember` image values;
  - the `test_render.py` constants;
  - the outcome record PR (the runbook's §9 record, docs only).
- Evidence lives under `$HOME/si-devnet2-stage3-p5`.

**housegate** (Tasks 78, 79, 81, 82): Appendix C of this plan file on the ledger copy, one record commit per recording task from a URWT worktree `multi-source-s3e4-ledger-<N>` (S3-C1 contract §19).

## Spec coverage map

| Spec item | Tasks |
|---|---|
| §9.3 base(P) in Apply and the WorkSet, behind `checkpoints` | 76 |
| §9.3 purge exemption and pressure bypass; `under_replicated` for every escape and for \|E\| < k − 1 | 76 (K78) |
| §9.3 E and \|E\| over distinct failure domains, eviction and delisting leave E in the same Apply | 76 |
| §9.3 strict mode: k − 1 domains, Lagging in E, backpressure (K59 code 13) | 76 |
| §9.3 raising k precondition | 76; execution: Plan S3-F |
| §9.3 failure independence, `safe_copies` per distinct node | 76 (`arbiter_safe_copies_min`); the P4 anti-affinity proof, with the D39 fallback (verifiers share nodes only with each other, never with a source ClickHouse): Plan S3-D; kept by every P5 roll: 82; re-check: Plan S3-F |
| §6.4 Leaving release (strict and non-strict, recorded once) | 76 (evaluation); record shape: Plan S3-E3 |
| §10.1 narrowed drain gate, touched tracks, floor-held parts not in flight | 77 |
| §10.1 issuance hold with an exact `expected_promotion_seq` | 77 (readiness on the draft, default, K79); hold: Plan S3-A |
| §10.3 activation with every switch off, gate, `stage3_v1`, voter probe | 78 (integration), 81, 82 (K82); gate: Plan S3-E2 |
| §10.3 v21 write-then-verify, plain body, fallback, writer setting | 78 (integration), 80 (`arbiter.raft.snapshotBody`, alert), 81, 82; codec: Plan S3-E2 |
| §10.3 rehearsal on a disposable three-voter cluster, restarts from v21, forced mismatch | 78 (`p5rehearsal` built into `$R5`, D37), 81, 82 (K81) |
| §10.4 switch order, exercised end to end | 78 (K80); on devnet2: Plan S3-F |
| §6.1 consent statement, member keys | 79 |
| §11 `replica_hosts` from `DatabaseGroup` once placed | 79 (the served `host_indexer_id`, D27; the field: Plan S3-E1) |
| §15 arbiter: floor, narrowed drain, metrics, `arbiter-admin` | 76, 77, 78 |
| §15 sentio-node: consent, key handling, pins | 79 |
| §15 production: alert rules for floor, copies, encoding fallback and members | 80 |
| §16 P5 (every voter writes v21; rehearsal passed) | 81, 82 |
| §17 FSM: floor and escapes, strict mode, domain collision, strict Leaving release, raise precondition, narrowed drain and issuance hold, activation with a quarantined track, offline digest replay of the devnet2 log | 76, 77, 78 |
| §17 Integration (three SNodes, members, pool, one Keeper; concurrent writes; a member killed mid-stream; a quarantined owner read from members; one indexer down) | 78 (K80) |
| §18 v21 irreversible, the plain-body fallback as the only lever | 81 (§10, §11), 82 |

## Follow-ups (not in this plan)

- The step-by-step switch-on of §10.4 on devnet2, the Keeper headroom gate, k = 2 for 7 days and the alert `ArbiterSafeCopiesBelowTwo` on `arbiter_safe_copies_min`: Plan S3-F. The Keeper thresholds are agreed with the clickhouse-extra owner, and the member `replica_signer` keys are made in the user's terminal; both are the user's actions (decision D40).
- `arbiter_floor_held_cleanup_age_seconds` restarts at every leadership change (K78). A replicated age would need a consensus-visible clock and belongs to v22 at the earliest.
- `ReselectVerifiers` has no operator RPC (§7.4 has only the leader propose it); add one if the P6 playbook needs it.
- In non-strict mode, a Leaving member whose frozen E is evicted entirely keeps its copies until it is evicted itself (§6.4 is silent on it): revisit with the P6 operating rules.
- The rehearsal Pod reads the live authority key from the voters' Secret. A rehearsal-only authority would need a signed rotation, so it is not done.
- Verified `hg_safe` merges and history pruning (v22) bound the super-linear `REPLACE`-renamed part term that v21 still carries (§13.2).
- Per-path Keeper ACLs and a dedicated SI Keeper ensemble need their own design (§14, §18); until then a leaked `si_member_ddl` credential stays the residual Plan S3-F records.
- The consumed names at the head of Appendix A were reconciled with the contracts of S3-C1 … S3-E3 by the cross-plan consistency pass:
  - S3-E2's forced-mismatch seam is `internSnapshotBodyFunc`.
  - The switch flag is `--stage3-switch`.
  - S3-E3 exports the per-class gauge `arbiter_group_members{owner,class}`.
  - The open question of a host id on the group view is closed by decision D27. S3-E1 adds `host_indexer_id` to the registry-view `GroupMember`, filled at serve time from the member's `VerifierEntry`, and `replica_hosts` reads it; no `hostOf` mapping remains.- Plan S3-A's and S3-B's remaining follow-ups, unchanged.

---

## Task 76: arbiter — the copy floor, strict backpressure, the Leaving release and the raise-k precondition (S3-E4)

Spec §9.3 makes a promotion's unsafe-part cleanup wait, from the `checkpoints` switch on, until enough counting members' checkpoints hold it ("Cleanup of promotion P of owner O requires, in Apply (`applyScheduleUnsafeCleanup`) and in the WorkSet alike: base(P) … and one of" the purge exemption, the pressure bypass and the count over distinct failure domains), with strict mode, the `under_replicated` signals and the precondition for raising k; §6.4 releases a Leaving member's copies once enough other copies hold its entry. Contract §53 and clarification K78 fix the names and texts. Today `applyScheduleUnsafeCleanup` (`fsm/apply.go:960-1020`) checks only the quarantine, the WorkSet (`fsm/reads_work.go:317-364`) lists a cleanup once the owner's published entry covers the promotion's block, nothing ever sets `LeavingMember.Released` (Plan S3-E3 §50 leaves it to this task) and any committed `min_safe_copies` is accepted. This task adds `fsm/floor.go` with the one predicate `floorLocked(pp)` that both callers ask, strict mode's code-13 `COPIES_BACKPRESSURE` at admission (`fsm/admission.go`, after Plan S3-E2 Task 70's `VERIFIERS_UNAVAILABLE` check), the raise-k precondition in the Apply-only half of `applyUpdateConsensusParams`, the Leaving release evaluated after every command that can meet it, the replicated gauges through `MetricsSnapshot`, the hint `EventUnderReplicated`, and the leader-local counter `arbiter_under_replicated_total{reason}` and gauge `arbiter_floor_held_cleanup_age_seconds` through a new `orchestrator.FloorMetrics`. With the switch off every Apply answer, WorkSet, admission answer and update is the one Task 75's branch gives.

Worktree: Plan S3-E2 Task 64's URWT worktree (`W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e`, branch `urwt/claude/multi-source-s3e`, contract §48), with `HEAD` at Plan S3-E3 Task 75's last commit; every command below starts with `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e;` in its own Bash call. Line anchors are arbiter `526a721` (`v0.13.0`, `origin/main` when this plan was drafted, Plan S3-A merged); on the branch every hunk is found by its quoted lines under the hunk rule of the Global Constraints: Plan S3-E2 Tasks 64–70 and Plan S3-E3 Tasks 71–75 edit other lines of `fsm/apply.go`, `fsm/admission.go`, `fsm/consensus_updates.go`, `fsm/reads_work.go`, `fsm/reads_work_types.go`, `metrics/metrics.go`, `orchestrator/loop.go`, `cmd/arbiter/services.go` and the README, and the files `fsm/stage3.go`, `fsm/stage3_groups.go`, `fsm/stage3_fixture_test.go` and `fsm/stage3_params_test.go` are theirs. The task ends with two local commits; no step reaches a remote, so none needs a confirmation in chat: Task 64 did the only fetch, and Task 78 pushes the branch and opens its PR. If a `go` or `bazel` command below reports that it downloads or fetches anything, stop: that is a remote read and needs explicit user confirmation in chat before it runs.

**Files:**
- Create: `fsm/floor.go` (contract §53's `UnderReplicatedReason` and its constants, `FloorVerdict`, `floorActiveLocked`, `floorLocked`, `floorIncarnationLocked`, `floorMembersLocked`, `checkpointReflectsLocked`, `uncleanedPartsLocked`, `floorOnlyHeldLocked`, `raiseKRefusalLocked`, `leavingReleaseHoldsLocked`, `releaseLeavingMembersLocked`, and this task's helpers)
- Create: `fsm/floor_test.go` (the copy-floor fixture and sixteen tests), `fsm/floor_strict_test.go` (five), `fsm/floor_raise_test.go` (three), `fsm/floor_leaving_test.go` (five)
- Modify: `fsm/apply.go` — `applyScheduleUnsafeCleanup` (`:960-1020`: the floor right after the quarantine check `:997-1004`, before the pending-duplicate check `:1005`; the hint after `EventCleanupScheduled` `:1018`); `applyEvictNode` (`:121-149`: the registry version after `n.Reason = c.Reason` `:139`, the release after the purge completion, before the final `EventMembershipChanged` `:147`)
- Modify: `fsm/reads_work_types.go` — `CleanupWork` (`:54-57`) gains `UnderReplicated`; `WorkSet` (`:72-94`, Plan S3-E2 Task 70's `Unmarkable` last on the branch) gains `FloorHeld` as its last field
- Modify: `fsm/reads_work.go` — the `PublishedUncleaned` loop of `WorkSet` (`:324-367`)
- Modify: `fsm/admission.go` — strict backpressure right before the lane budget (`:148`), after Plan S3-E2 Task 70's `VERIFIERS_UNAVAILABLE` block
- Modify: `fsm/consensus_updates.go` — `applyUpdateConsensusParams`: the raise-k refusal after the tracks-activation refusal (`:79-87`), the registry version before `wasEnabled` (`:88`), the release before `ConsensusEpoch++` (`:149`)
- Modify: `fsm/stage3_groups.go` (Plan S3-E3 Task 71) — the first statement of `groupsChangedLocked`
- Modify: `fsm/watch.go` — `EventUnderReplicated` after `EventIndexerQuarantineChanged` (`:28`) and its `String` case (`:64-65`)
- Modify: `fsm/reads_metrics.go` — `MetricsSnapshot` (`:28-50`) gains three fields and `MetricSafeCopies`; `MetricsSnapshot()` fills them before `return m` (`:167`)
- Modify: `fsm/stage3_params_test.go` (Plan S3-E2 Task 64) — the three tests that commit `min_safe_copies` 2 with no member (Step 6)
- Modify: `fsm/watch_test.go` — `TestEventKindStringNamesEveryTransition` (`:247-265`) runs to `EventUnderReplicated`
- Create: `metrics/floor.go`, `metrics/floor_test.go`
- Modify: `metrics/metrics.go` — `Metrics` (last fields after `snapshotFailures` `:64`), `New` (after the hold `MustRegister` `:150`), `collector` (last fields), `newCollector` (last entries), `Describe` (`:263-267`), `Collect` (its end, `:335-343`)
- Create: `orchestrator/floor_metrics.go`, `orchestrator/floor_metrics_test.go`
- Modify: `orchestrator/deps.go` — `FloorMetrics` after `IssuanceGate` (`:72-74`), `Deps.Floor` after `Issuance` (`:93`)
- Modify: `orchestrator/loop.go` — the `floorHeldSince` field before `custodyStage` (`:43`), `FloorMetrics()` after `IssuanceGate()` (`:67`), the per-term reset after `resetSealTimes` in `Run` (`:85`)
- Modify: `orchestrator/cleanup.go` — `scheduleCleanup` (`:13-42`) becomes a wrapper of the new `proposeCleanup`
- Modify: `orchestrator/promotion.go` — `handlePromotionRows` (`:15`, the `PublishedUncleaned` loop `:39-43`)
- Create: `cmd/arbiter/floor_metrics_wiring_test.go`
- Modify: `cmd/arbiter/app.go` (the assertion after `:42`, `run` `:82`, `:89`, `:107`), `cmd/arbiter/services.go` (`serviceRuntime` after `holdMetrics` `:44`, `Deps.Floor` after `Issuance` `:162`)
- Modify: `README.md` — two paragraphs at the end of Plan S3-E2 Task 65's section `## Multi-source storage integrity (stage-3 activation)` (right before `## Artifact disposition admission`, `:396` at `526a721`) and one after the consensus-hold paragraph of `## Metrics` (`:435`)
- Modify (gazelle): `fsm/BUILD.bazel`, `metrics/BUILD.bazel`, `orchestrator/BUILD.bazel`, `cmd/arbiter/BUILD.bazel` (new files only, no dependency)
- Test: the files above; unchanged and green: every golden test (`TestPreActivationHistoryIsByteIdenticalAcrossTracks`, `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`, the L3 preimage tests, Plan S3-E2 Task 66's and Task 67's v21 goldens, Plan S3-E3's placement and group-key goldens), every `TestDeterminism_*`, Plan S3-E3's group, checkpoint and class tests, Plan S3-A's drain and hold tests, `orchestrator/signer_test.go` and `orchestrator/rejected_cleanup_test.go` (which call `scheduleCleanup`)

**Interfaces:**
- Consumes:
  - Plan S3-E3 (contract §49–§52, through Task 75): `func (f *FSM) checkpointsOnLocked() bool`; `func (f *FSM) groupLocked(owner uint64, database string) *DatabaseGroup`; `func (f *FSM) copyFloorSetLocked(g *DatabaseGroup, strict bool, exclude string) (nodes []string, domains uint32)` (E: listed, `Replicate && Counts`, not evicted, `caught_up` or `behind`, `lagging` too when strict, failure domain other than the owner's); `func (f *FSM) groupsChangedLocked()` (every group-changing Apply calls it once, last; contract §50 has this task insert its release evaluation at its start); `func indexerFailureDomain(params Params, id uint64) string`; `func entryCompare(a, b replay.TrackFrontier) int`; `Stage3State.Groups []*DatabaseGroup`, `DatabaseGroup{Owner, DatabaseID, Generation, NetworkFill, Members, Leaving}`, `GroupMember{NodeID, Class, Checkpoint *MemberCheckpointRecord}`, `LeavingMember{Member, Entry, FrozenE, FrozenEDomains, Released}`, `MemberCheckpointRecord{Checkpoint arbiter.MemberCheckpoint}`, `MemberBootstrapping`, `MemberCaughtUp`, `MemberLagging`; `applyMemberCheckpoint` (refusals 1–14 of §52.3: the fixture signs checkpoints that meet each), `applyRebalanceGroup` (Leaving records with `Entry` and the frozen E), `applyMarkMemberLagging`, and Task 71's purge completion in `applyEvictNode`.
  - Plan S3-E2 (contract §44–§47): `Stage3SwitchCheckpoints`, `Stage3SwitchNarrowedDrain`, `Stage3SwitchOwnerAwareSelection`, `Stage3SwitchPlacement`, `Stage3SwitchReselection`, `func (f *FSM) verifierEntryLocked(nodeID string) (arbiter.VerifierEntry, bool)`, `func findVerifierEntry(entries []arbiter.VerifierEntry, nodeID string) (arbiter.VerifierEntry, bool)` (Task 64, `fsm/stage3.go`), `cloneStage3Params`, `verifyStage3Transition`, Task 70's `VERIFIERS_UNAVAILABLE` block in admission; Task 64's fixture (`fsm/stage3_fixture_test.go`) `stage3FSM`, `stage3Update`, `turnOnStage3Switch`, `turnOnStage3SwitchesThrough`, `stage3MemberKey`, `stage3ReplicaSigner`, `memberConsent`, `withVerifier`, `rejectStage3Unchanged`, and in `fsm/stage3_params_test.go` `stage3Previous` and `requireErrorContains`.
  - Plan S3-E1 (contract §39–§41, through Task 64's pin of `CORE_TAG_P5`): `arbiter.AdmissionCodeCopiesBackpressure` (13); `arbiter.Stage3Params{MinSafeCopies, StrictCopies, MaxUncleanedPartsPerPartition, MaxUncleanedPartsPerTable, IndexerFailureDomains, LagLow}`, `arbiter.IndexerFailureDomain`; `arbiter.VerifierEntry{NodeID, Ed25519Pubkey, HostIndexerID, ReplicaSigner, ConsentJWS, Weight, FailureDomain, Verify, Replicate, Counts, SharedRMT}`; `arbiter.MemberCheckpoint`, `arbiter.CheckpointGroup`, `arbiter.ExcludedTable`, `arbiter.ExclusionNotInstalled`, `arbiter.ExclusionDiverged`, `arbiter.EvictNodeCommand`; `(*authority.Signer).SignMemberCheckpoint`, `SignRebalanceGroupWithContext`, `SignMarkMemberLaggingWithContext`; `wire.RebalanceGroup` and `wire.MarkMemberLagging` with `Canonical()`, `wire.MemberCheckpoint`.
  - Base (`526a721`): `promotionTrackLocked` (`fsm/tracks.go:334`), `publishedTrackFrontierLocked` (`:215`), `indexerQuarantinedLocked` (`:344`), `incarnationOwnerLocked` (`:98`), `promotionBlockLocked` (`fsm/reads_promotions.go:106`, Plan S3-A), `splitTableKey` (`fsm/table_registry.go:233`), the scan of `manifestTableOwnerLocked` (`fsm/reads_manifest.go:448-463`), `registryChangedLocked` (`fsm/apply_table_registry.go:28`), `SubmitResult`, `Event`, `emit`; fsm test helpers `mustApply`, `mkLog`, `applyExpectReject` (`fsm/fsm_test.go`), `snapshotBytes` (`fsm/snapshot_test.go:30`), `rejectTracksUnchanged`, `trackSNode`, `trackEnvelope`, `admitTrack`, `bindTrackClaim`, `verifyBlock`, `promoteTrack`, `ackTrackPromotion`, `passTrackBlock`, `setPublishedTrackFrontiers` (`fsm/tracks_fixture_test.go`), `signedConsensusUpdate` (`fsm/consensus_updates_test.go:26`), `testAccount`, `submit` (`fsm/admission_test.go`), `siEntry` (`fsm/signed_claims_params_test.go:45`), `snodeSigner`, `partCleanup`, `partCleanupAck` (`fsm/pre_tracks_golden_test.go`), `signedRC` (`fsm/rc_owner_parking_test.go:15`), `lthashHex` (`fsm/threeway_test.go:18`), `mustSeal`, `anchorFinal`, `publishManifestInputs`, `ownedAddCmd`, `ev`, `retireCmd`, `rejectBlockQ`, `trackQ`, `quarantineQ`, `releaseQ`, `rejectedCleanupQ`, `messageContextFor`, `authorityContextFor`, `drain` (`fsm/watch_test.go:22`), `authoritytest.MustSigner`, `authoritytest.Iat`; orchestrator test helpers `newHarness`, `authorityParamsO`, `seedGenesisWriterAndVerifiers`, `driveToQuorum`, `ackPromotionViaNode`, `testParamsO`, and `(*Loop).anchorBlock`, `issuePromotion`, `publishManifest`, `handlePromotionRows`, `afterRescan`, `now`; metrics test helper `laneSnapshot` (`metrics/client_lanes_test.go:12`); cmd/arbiter test helpers `genesisTestConfig`, `applyTestDefaults`, `newFSM`, `newServerAndLoop`.
- Produces (contract §53 verbatim; the names marked "added" are this task's own and are recorded in Appendix B):
  - fsm (`fsm/floor.go`): `type UnderReplicatedReason string` with `UnderReplicatedNone` (`""`), `UnderReplicatedInactiveIncarnation` (`inactive_incarnation`), `UnderReplicatedPressurePartition` (`pressure_partition`), `UnderReplicatedPressureTable` (`pressure_table`), `UnderReplicatedDomainsBelowK` (`domains_below_k`); `FloorVerdict{BaseRefusal, Escape, K, Required, EDomains, ReflectingDomains, Allowed, FloorOnly, UnderReplicated}`; `floorActiveLocked() bool`, `floorLocked(pp *PendingPromotion) FloorVerdict`, `floorIncarnationLocked(tableID string) *TableIncarnation`, `floorMembersLocked(g *DatabaseGroup, strict bool) map[string][]string`, `checkpointReflectsLocked(cp *MemberCheckpointRecord, databaseID, tableID string, pp *PendingPromotion) bool`, `uncleanedPartsLocked(tableID string) (perPartition map[string]int, total int)`, `floorOnlyHeldLocked() []uint64`, `raiseKRefusalLocked(next Params) string`, `leavingReleaseHoldsLocked(g *DatabaseGroup, m *LeavingMember) bool`, `releaseLeavingMembersLocked() bool`. Added: `var UnderReplicatedReasons []UnderReplicatedReason`, `func floorNeed(k uint32) uint32`, `func (f *FSM) floorRefusalLocked(pp *PendingPromotion, v FloorVerdict) string`, `func groupMemberByID(g *DatabaseGroup, node string) *GroupMember`, `func (f *FSM) promotionUncleanedLocked(pp *PendingPromotion) bool`, `func (f *FSM) copiesBackpressureLocked(tableID string) (SubmitResult, bool)`, `func (f *FSM) caughtUpDomainsLocked(g *DatabaseGroup, params Params) map[string]bool`, `func (f *FSM) nodeEvictedLocked(node string) bool`, `func checkpointExcludesNothing(cp *MemberCheckpointRecord, databaseID string) bool`, `func (f *FSM) registryVersionLocked() uint64`, `func (f *FSM) releaseLeavingAtEndLocked(version uint64)`, `func (f *FSM) floorMetricsLocked() (held, underReplicatedGroups uint64, safeCopies []MetricSafeCopies)`.
  - fsm, elsewhere: `CleanupWork.UnderReplicated UnderReplicatedReason`; `WorkSet.FloorHeld []uint64`; `EventUnderReplicated` (`String()` `under_replicated`); `MetricsSnapshot.FloorHeldCleanups uint64`, `.UnderReplicatedGroups uint64`, `.SafeCopiesMin []MetricSafeCopies`; `type MetricSafeCopies struct{ IndexerID, Copies uint64 }`.
  - Refusals, verbatim: `copy floor: promotion %d is not acknowledged`; `copy floor: promotion %d covers block %d above owner %d's published safe_block_seq %d`; `copy floor: promotion %d is above owner %d's published applied_promotion_seq %d`; `copy floor: promotion %d of %s needs reflecting checkpoints in %d distinct failure domains other than owner %d's, has %d`; `consensus update raises stage3.min_safe_copies to %d: %s (incarnation %d, owner %d) has caught-up counting members in %d distinct failure domains other than its owner's, needs %d`; `SubmitResult{Code: arbiter.AdmissionCodeCopiesBackpressure}` with `storage-integrity copy floor: partition %s of %s holds %d uncleaned promoted parts (max_uncleaned_parts_per_partition %d) whose cleanup waits for member checkpoints (strict_copies); retry later` or `storage-integrity copy floor: %s holds %d uncleaned promoted parts (max_uncleaned_parts_per_table %d) whose cleanup waits for member checkpoints (strict_copies); retry later`; the quarantine text `indexer %d is quarantined; no new cleanup is scheduled`, unchanged. Added: `copy floor: promotion %d belongs to no track`.
  - orchestrator: `type FloorMetrics interface{ UnderReplicatedCleanup(reason string); FloorHeldCleanups(count int, oldest time.Duration) }`, `Deps.Floor FloorMetrics` (nil disables), the leader-local `Loop.floorHeldSince map[uint64]time.Time`. Added: `func (o *Loop) FloorMetrics() FloorMetrics`, `func (o *Loop) proposeCleanup(ctx context.Context, cw fsm.CleanupWork) (applied bool, err error)` (`scheduleCleanup` keeps its signature as its wrapper), `func (o *Loop) resetFloorHeld()`, `func (o *Loop) observeFloorHeld(held []uint64)`.
  - metrics: collector series `arbiter_floor_held_cleanups`, `arbiter_under_replicated_groups`, `arbiter_safe_copies_min{indexer_id}`; `(*Metrics).UnderReplicatedCleanup(reason string)` and `FloorHeldCleanups(count int, oldest time.Duration)` as `arbiter_under_replicated_total{reason}` (counter, every reason at 0 from the start) and `arbiter_floor_held_cleanup_age_seconds` (gauge). Added: `func (m *Metrics) registerFloorMetrics()`.
  - cmd/arbiter: `serviceRuntime.floorMetrics orchestrator.FloorMetrics`, mapped to `Deps.Floor`; the assertion `var _ orchestrator.FloorMetrics = (*metrics.Metrics)(nil)`.
  - Test constants and fixture (contract §53): `floorDomainOwner`, `floorDomainB`, `floorDomainC`, `floorDomainD`; `floorFixtureOptions{K uint32; Strict bool; MemberDomains map[string]string; MaxPerPartition, MaxPerTable uint32}`; `newFloorFixture(t *testing.T, o floorFixtureOptions) *floorFixture`. Added: `floorMemberB`, `floorMemberC`, `floorMemberD`, `floorOwner`; `newFloorFixtureBeforeCheckpoints`, `floorHostSigner`, `groupMemberNodes`, `floorRefusalText`, `requireFloorCleanup`, `coverText`, `underReplicatedEvents`, `requireFloorAgreement`, `floorRetired`, `floorPressure`, `strictBackpressure`, `raiseKCmd`, `raiseKText`, `floorLeavingB`, and the `floorFixture` methods `turnOnCheckpoints`, `memberEntry`, `register`, `setDomains`, `updateCmd`, `update`, `promote`, `promoteTwoParts`, `issue`, `publish`, `cleanupCmd`, `clean`, `checkpointCmd`, `checkpoint`, `checkpointAll`, `rebalanceCmd`, `laggingCmd`, `evictCmd`, `group`, `leaving`, `verdict`, `work`, `workSet`; orchestrator `floorHeldCall`, `floorMetricsRecorder`.
  - Tests: the 29 fsm, two orchestrator and one metrics tests of contract §53; added `TestNewServerAndLoopWiresTheFloorMetrics` (cmd/arbiter); edited Plan S3-E2 Task 64's `TestStage3MinSafeCopiesAndGroupSizeNeverDecrease`, `TestStage3FloorAboveOneNeedsCheckpointsAndNarrowedDrain`, `TestStage3RulesReplayOnRestore` and the base `TestEventKindStringNamesEveryTransition`.

Notes:
- **Switch-off identity (contract §53).** Every new branch tests `floorActiveLocked()`, the committed `checkpoints` switch, first: `applyScheduleUnsafeCleanup` skips the floor, the WorkSet keeps today's rows with an empty reason and no `FloorHeld`, admission returns before counting anything, `releaseLeavingMembersLocked` records nothing and `floorMetricsLocked` returns zeros. `raiseKRefusalLocked` acts only on an update that raises `min_safe_copies`, which Plan S3-E2's Params-only rule refuses before the `checkpoints` and `narrowed_drain` switches. No state field, snapshot member, Raft tag or proto number is added (`CleanupWork.UnderReplicated` and `WorkSet.FloorHeld` are in-process Go fields, decision D33), so every container stays byte-identical and the frozen goldens run unchanged; `TestFloorSwitchOffIsByteIdentical` pins the stage-2 answers in the cases where the floor would answer otherwise.
- **One predicate (Review Focus 1).** `floorLocked` checks base(P) in contract §53's order (acknowledged; the owner's published entry covers the promotion's block, then its sequence; the owner not quarantined), then counts the distinct failure domains of the members of E whose checkpoint for the group reflects the promotion and does not exclude its table. `Required` is min(k − 1, |E|), k − 1 in strict mode. Apply refuses with `BaseRefusal` or the count text and leaves state unchanged; the WorkSet lists a cleanup exactly when the verdict allows it, with the verdict's reason, and puts a `FloorOnly` promotion in `FloorHeld`. `TestFloorApplyAndWorkSetAgreeOnEveryCleanup` drives five histories that reach every verdict and checks, for every acknowledged promotion with uncleaned parts, that Apply schedules exactly what the WorkSet lists and that `FloorHeld` equals `floorOnlyHeldLocked()`.
- **An escape is recorded only where the count would hold.** The escapes are alternatives to the count clause (spec §9.3 "and one of"), so `floorLocked` evaluates them only for a promotion the count does not allow, in contract §53's order: not Active, then, outside strict mode, the partition above `max_uncleaned_parts_per_partition`, then the table above `max_uncleaned_parts_per_table` ("above" is strictly greater). At k = 1 the count always holds, so nothing escapes and nothing raises `under_replicated`: the floor is dormant and only the publication clauses bite (`TestFloorIsDormantAtKOne`, which also exceeds the table bound). A cleanup allowed while |E| < k − 1 is `domains_below_k` (recorded in Appendix B).
- **base(P) for a promotion without a track.** `floorLocked` refuses it with the added text `copy floor: promotion %d belongs to no track`. Such a promotion predates the tracks activation, whose drain gate left none of them with an uncleaned part, and the WorkSet never lists one after that activation (`fsm/reads_work.go:336-341`), so the two callers keep agreeing (recorded in Appendix B). A promotion Apply cannot find is `not acknowledged`; `expectedSourceLocked` refuses it before the floor on every committed history.
- **A pending cleanup is not checked again.** The floor runs only for a cleanup with no pending command, so the idempotent re-schedule of a pending one (the legacy audit-proof path included) and a cleanup scheduled before the switch keep today's answers (`TestCleanupScheduledBeforeTheCheckpointsSwitchIsNotRechecked`, whose cleanup stays pending across the switch-on under a quarantine, the only way the stage-2 drain gate lets one).
- **Strict mode.** The count needs k − 1 domains, `lagging` members stay in E (S3-E3's `copyFloorSetLocked(g, true, "")`) and both pressure escapes are off; the purge exemption remains. Admission refuses a statement for table T with code 13 and state unchanged while T holds a partition above the partition bound or more parts than the table bound and some acknowledged promotion of T is `FloorOnly`; the bounds are counted first and the held promotions only above a bound, which is the same conjunction and keeps admission cheap. The partition message names the first such partition in ascending id. `env.TargetTableID` is the table checked.
- **Raising k.** `raiseKRefusalLocked` runs after the drain gate and the tracks-activation refusal and before any mutation, for every Active incarnation in registry order, and counts with `caughtUpDomainsLocked` under `next`, so an update that raises k and also stops a member counting or moves its domain is judged on what it commits (`TestRaisingKRequiresCaughtUpCountingMembersInKMinusOneDomains`). Plan S3-E2 Task 64 has three tests that commit `min_safe_copies` 2 on a network without members; Step 6 moves each onto this precondition (the copy-floor fixture commits k = 2 with a caught-up member, or the update sets only `strict_copies`, which has no state precondition) and keeps what each pins. A later test of the branch that commits a raise without member checkpoints fails with the raise refusal and needs the same move.
- **The Leaving release.** Outside strict mode it counts the members of the frozen E that are still current members of the group, listed and non-evicted, with checkpoints at or past the Leaving member's entry that exclude none of the group's tables, over distinct current failure domains other than the owner's; a frozen member that is itself Leaving no longer counts, so two members removed together never release each other. In strict mode it counts the group's current E (lagging included). k and `strict_copies` are the committed ones; at k = 1 the release holds at once. Contract §50 has this task evaluate the release at the start of `groupsChangedLocked`, which `applyMemberCheckpoint`, `applyRebalanceGroup` and `applyMarkMemberLagging` call last, so the release is recorded at their end inside their own bump; `applyUpdateConsensusParams` (before `ConsensusEpoch++`) and `applyEvictNode` (after the purge completion) call `releaseLeavingAtEndLocked`, which bumps the registry version for a release only if the Apply has not bumped it already (one bump per Apply). A recorded release is never cleared. `TestLeavingReleaseIsEvaluatedAfterEveryTriggeringCommand` clears the record by hand before each of the five commands, the one direct state edit of these tests: a met condition that no Apply evaluated yet, which committed histories never hold once the floor is on (recorded in Appendix B, with the choices of this paragraph).
- **Metrics (K78).** `arbiter_floor_held_cleanups` and `arbiter_under_replicated_groups` are exported from the start (0 before the switch), `arbiter_safe_copies_min{indexer_id}` only for owners with a group once the switch is on. The leader-local counter counts a `PublishedUncleaned` row's reason only when `proposeCleanup` reports that its proposal returned no error, and every reason series exists at 0 from the start. The held-cleanup age is the oldest age over `WorkSet.FloorHeld`, measured from the first pass of this leader term that saw each promotion held; `Run` resets it when a term starts and when it ends, so a follower reports 0. `FloorHeldCleanups`' count argument is not exported again (the replicated gauge is the record).
- **The fixture.** Contract §53 builds `newFloorFixture` "on S3-E3's checkpoint fixture (§52)", which S3-E3's contract does not name; it is built here on Plan S3-E2 Task 64's fixture through committed commands only: it enrols one host indexer per member (keys from `floorHostSigner`, SNodes never registered, activation blocks 1000 + id), lists the members with Task 64's keys and a consent of the host's key, registers them, turns the switches on in §10.4 order, lets Plan S3-E3's placement backfill place them, signs every checkpoint with `stage3ReplicaSigner` through `applyMemberCheckpoint`, and drives Leaving records and lagging members through signed `RebalanceGroup` and `MarkMemberLagging`. Members never send `MemberReady`, so every eligible ring stays the network pool and `verifyBlock`'s pool evidence stays valid. Indexer 1 (`db.t`, the genesis table of database `db`) is the owner the tests promote; indexer 0's group `G(0, db0)` holds the same members and is checkpointed too, because raising k needs every Active incarnation. A track that owes a manifest entry issues no promotion, so every promotion but the last of a history is published before the next.
- **fsm red lines.** `fsm/floor.go` imports `cmp`, `fmt`, `slices`, `sort` and arbiter-core only: no arbiter-proto, no clock; `floorLocked` and its helpers only read, so the WorkSet, `MetricsSnapshot` and Task 77's `DrainViewFor` call them under the read lock.
- **Checked while drafting** (2026-10-16): every code block of this task was applied, step by step, to a `git archive` export of arbiter `526a721` carrying Plan S3-E2 Tasks 64–70 and Plan S3-E3 Tasks 71–72 as their drafting trees had them that day, a scratch stand-in of Plan S3-E3 Task 75 written from contract §52.3 (the record per listed group, the durable `checkpoint_seq`, the acceptance checks the fixture meets, the classes at acceptance and `MarkMemberLagging`; no lag accrual) and arbiter-core and arbiter-proto replace modules carrying Plan S3-E1's contract, with `GOWORK=off GOFLAGS=-mod=mod GOPROXY=off go test`. Observed: Step 8's build failure; Step 19's 33 passes under `-race` and the whole fsm package passing; Steps 22, 27 and 35's build failures and Steps 25, 33 and 38's passes; `go test ./... -race -count=1` passing, `go vet ./...` clean, `gofmt` silent, no `time.Now` in `fsm/`. Each of 38 single-point mutations (no floor in Apply or in the WorkSet; the floor re-checking a pending cleanup; members counted instead of domains; k − 1 outside strict mode; an escape taken where the count holds; either bound at "at or above"; pressure in strict mode; no purge exemption; each base clause dropped; exclusions ignored; reflection ignoring the block; no `domains_below_k`; no hint; no backpressure, or backpressure without a floor-held promotion; no raise check, the raise judged on the committed listing, ignoring exclusions or counting non-Active incarnations; the release on the current E outside strict mode or on the frozen E in strict mode, while the floor is off, withdrawn, missing after an eviction, an update or in `groupsChangedLocked`, or bumping twice; the owner's domain counted in E; the held gauge at zero; counting a refused cleanup; no age reset at the start or at the end of a term; ages not kept across passes; no wiring) fails at least one named test, and the age test passed 200 runs under `-race`. Bazel was not run (no offline module for the stubbed tags); Step 40 runs it on the branch.

- [ ] **Step 1: Check the worktree, the branch head and the seams this task builds on**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" || exit 1
git rev-parse --abbrev-ref HEAD
git status --porcelain
git log -1 --format=%s
CORE="$(GOWORK=off go list -m -f '{{.Dir}}' github.com/sentioxyz/arbiter-core)"
for p in 'func (f *FSM) checkpointsOnLocked(' 'func (f *FSM) groupLocked(' 'func (f *FSM) copyFloorSetLocked(' 'func (f *FSM) groupsChangedLocked(' 'func indexerFailureDomain(' 'func entryCompare(' 'func findVerifierEntry(' 'func (f *FSM) verifierEntryLocked(' 'func (f *FSM) applyMemberCheckpoint(' 'func (f *FSM) applyRebalanceGroup(' 'func (f *FSM) applyMarkMemberLagging(' 'type MemberCheckpointRecord struct' 'func (f *FSM) verifiersUnavailableLocked(' 'func stage3FSM(' 'func stage3Update(' 'func turnOnStage3Switch(' 'func turnOnStage3SwitchesThrough(' 'func stage3MemberKey(' 'func stage3ReplicaSigner(' 'func memberConsent(' 'func withVerifier(' 'func rejectStage3Unchanged(' 'func stage3Previous(' 'func requireErrorContains('; do
  grep -rqF -- "$p" fsm || echo "missing in fsm: $p"
done
grep -rqE 'Checkpoint +\*MemberCheckpointRecord' fsm || echo "missing in fsm: GroupMember.Checkpoint"
grep -rqE 'Released +bool' fsm || echo "missing in fsm: LeavingMember.Released"
grep -rqE 'AdmissionCodeCopiesBackpressure +AdmissionCode += 13' "$CORE" || echo "missing in arbiter-core: AdmissionCodeCopiesBackpressure"
for p in 'func (s *Signer) SignMemberCheckpoint(' 'func (s *Signer) SignRebalanceGroupWithContext(' 'func (s *Signer) SignMarkMemberLaggingWithContext(' 'ExclusionNotInstalled' 'ExclusionDiverged'; do
  grep -rqF -- "$p" "$CORE" || echo "missing in arbiter-core: $p"
done
GOWORK=off go test ./fsm/ ./orchestrator/ ./metrics/ ./cmd/arbiter/ -count=1
```

Expected: `urwt/claude/multi-source-s3e`; no status line; the subject `fsm: MemberCheckpoint, member classes and MarkMemberLagging (S3-E3 Task 75)`; no `missing` line; `ok` for the four packages. Stop on anything else: Plans S3-E2 and S3-E3 land first, in order, on this one branch (contract §48), and every name above is consumed below.

- [ ] **Step 2: Write the copy-floor fixture and the floor tests**

The fixture builds everything through committed commands (Notes, **The fixture**); the tests use the contract's names and texts verbatim.

Create `fsm/floor_test.go`:

```go
package fsm

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The copy-floor fixture (housegate spec 2026-10-14 §9.3, §6.4; plan S3-E4
// contract §53) is stage3FSM's two-owner network: indexer 1 (SNode s1) owns
// the genesis table db.t of database db, indexer 0 (s0) the chain table
// db0.x, and v1..v3 are the network pool. The fixture enrols one host
// indexer per member of floorFixtureOptions.MemberDomains (indexers 2, 3, ...
// in node order, SNodes s2, s3, ... never registered), gives indexer 1 the
// failure domain floorDomainOwner and indexer 0 node:n0, turns every switch
// through reselection on, lists each member (every flag true, weight 1, its
// host's consent), registers it with stage3MemberKey and places it in both
// groups G(1, db) and G(0, db0). Members whose requested domains collide,
// with each other or with an owner, are placed under a provisional domain of
// their own and moved by one later consensus update, the only way such a
// collision arises (placement and RebalanceGroup refuse one). With the
// checkpoints switch on, one block of db.t is promoted, published and
// cleaned, and every member checkpoints both groups at the published
// entries, so every member is caught_up. Last, narrowed_drain is turned on
// when K > 1 or Strict, and one update sets min_safe_copies K, strict_copies
// and the uncleaned-parts bounds (0 keeps 2000 and 20000).

const (
	floorDomainOwner = "node:owner"
	floorDomainB     = "node:b"
	floorDomainC     = "node:c"
	floorDomainD     = "node:d"

	floorMemberB = "member-b"
	floorMemberC = "member-c"
	floorMemberD = "member-d"

	// floorOwner is the owner whose table db.t the floor tests promote.
	floorOwner uint64 = 1
)

type floorFixtureOptions struct {
	K                            uint32
	Strict                       bool
	MemberDomains                map[string]string
	MaxPerPartition, MaxPerTable uint32
}

type floorFixture struct {
	t       *testing.T
	f       *FSM
	a       *authority.Signer
	hosts   map[string]uint64 // member node id -> host indexer id
	key     *ecdsa.PrivateKey // the statement account
	account string
	next    uint64            // the account's next client seq
	seqs    map[string]uint64 // "<node>/<owner>" -> last checkpoint_seq signed
}

// newFloorFixture is the whole fixture.
func newFloorFixture(t *testing.T, o floorFixtureOptions) *floorFixture {
	t.Helper()
	x := newFloorFixtureBeforeCheckpoints(t, o)
	x.turnOnCheckpoints(o)
	return x
}

// newFloorFixtureBeforeCheckpoints stops after the reselection switch: the
// floor is off and every member is bootstrapping.
func newFloorFixtureBeforeCheckpoints(t *testing.T, o floorFixtureOptions) *floorFixture {
	t.Helper()
	f, a := stage3FSM(t)
	key, account := testAccount(t)
	x := &floorFixture{t: t, f: f, a: a, hosts: map[string]uint64{}, key: key, account: account, next: 1, seqs: map[string]uint64{}}
	nodes := slices.Sorted(maps.Keys(o.MemberDomains))
	provisional := map[string]string{}
	used := map[string]bool{floorDomainOwner: true, "node:n0": true}
	collides := false
	for i, node := range nodes {
		host := uint64(2 + i)
		x.hosts[node] = host
		domain := o.MemberDomains[node]
		if used[domain] {
			domain, collides = fmt.Sprintf("node:host-%d", host), true
		}
		used[domain] = true
		provisional[node] = domain
	}
	x.update(func(u *arbiter.ConsensusParamsUpdate) {
		u.Stage3.IndexerFailureDomains = []arbiter.IndexerFailureDomain{{IndexerID: 0, FailureDomain: "node:n0"}, {IndexerID: floorOwner, FailureDomain: floorDomainOwner}}
		for _, node := range nodes {
			host := x.hosts[node]
			u.SIIndexers = append(u.SIIndexers, siEntry(t, f, floorHostSigner(t, host), host, 1000+host, trackSNode(host)))
			u.Stage3.IndexerFailureDomains = append(u.Stage3.IndexerFailureDomains, arbiter.IndexerFailureDomain{IndexerID: host, FailureDomain: provisional[node]})
		}
		u.MaxWriters = uint64(len(u.SIIndexers))
	})
	turnOnStage3SwitchesThrough(t, f, a, Stage3SwitchOwnerAwareSelection)
	x.update(func(u *arbiter.ConsensusParamsUpdate) {
		for _, node := range nodes {
			*u = withVerifier(*u, x.memberEntry(node, provisional[node]))
		}
	})
	for _, node := range nodes {
		x.register(node)
	}
	turnOnStage3Switch(t, f, a, Stage3SwitchPlacement)
	for _, owner := range []uint64{0, floorOwner} {
		database := map[uint64]string{0: "db0", floorOwner: "db"}[owner]
		if got := groupMemberNodes(x.group(owner, database)); !slices.Equal(got, nodes) {
			t.Fatalf("G(%d, %s) members %v, want %v", owner, database, got, nodes)
		}
	}
	if collides {
		x.setDomains(o.MemberDomains)
	}
	turnOnStage3Switch(t, f, a, Stage3SwitchReselection)
	return x
}

// turnOnCheckpoints turns the checkpoints switch on and completes the
// fixture as newFloorFixture describes.
func (x *floorFixture) turnOnCheckpoints(o floorFixtureOptions) {
	t := x.t
	t.Helper()
	turnOnStage3Switch(t, x.f, x.a, Stage3SwitchCheckpoints)
	p := x.promote(floorOwner)
	x.publish()
	x.clean(p)
	x.checkpointAll()
	if o.K > 1 || o.Strict {
		turnOnStage3Switch(t, x.f, x.a, Stage3SwitchNarrowedDrain)
	}
	if o.K > 1 || o.Strict || o.MaxPerPartition != 0 || o.MaxPerTable != 0 {
		x.update(func(u *arbiter.ConsensusParamsUpdate) {
			u.Stage3.MinSafeCopies = max(o.K, 1)
			u.Stage3.StrictCopies = o.Strict
			if o.MaxPerPartition != 0 {
				u.Stage3.MaxUncleanedPartsPerPartition = o.MaxPerPartition
			}
			if o.MaxPerTable != 0 {
				u.Stage3.MaxUncleanedPartsPerTable = o.MaxPerTable
			}
		})
	}
}

// floorHostSigner is the enrolment key of host indexer host.
func floorHostSigner(t *testing.T, host uint64) *authority.Signer {
	t.Helper()
	key := sha256.Sum256([]byte(fmt.Sprintf("copy floor fixture host key %d", host)))
	return authoritytest.MustSigner(t, hex.EncodeToString(key[:]))
}

// memberEntry is node's verifiers entry on its host in domain: every flag
// true, weight 1, stage3MemberKey and stage3ReplicaSigner, its host's
// consent.
func (x *floorFixture) memberEntry(node, domain string) arbiter.VerifierEntry {
	t := x.t
	t.Helper()
	host := x.hosts[node]
	entry := arbiter.VerifierEntry{NodeID: node, Ed25519Pubkey: stage3MemberKey(node).Public().(ed25519.PublicKey), HostIndexerID: &host,
		ReplicaSigner: strings.ToLower(stage3ReplicaSigner(t, node).Address()), Weight: 1, FailureDomain: domain,
		Verify: true, Replicate: true, Counts: true, SharedRMT: true}
	entry.ConsentJWS = memberConsent(t, x.f, floorHostSigner(t, host), entry)
	return entry
}

// register registers member node at registration seq 1, signed with
// stage3MemberKey(node).
func (x *floorFixture) register(node string) {
	t := x.t
	t.Helper()
	key := stage3MemberKey(node)
	reg := wire.RegistrationFromPB(wire.RegistrationToPB(arbiter.NodeRegistration{NodeID: node, Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier},
		Ed25519Pubkey: key.Public().(ed25519.PublicKey), RegistrationSeq: 1}))
	sig, err := authority.SignVerifierMessage(key, authority.VerifierMessageRegistration, messageContextFor(t, x.f), reg)
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, x.f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: reg, Ed25519Signature: sig}})
}

// setDomains moves each named member, and its host indexer, to the given
// failure domain in one consensus update. The domain moves only; every other
// field of the entry, the consent included, is the committed one.
func (x *floorFixture) setDomains(domains map[string]string) {
	x.t.Helper()
	x.update(func(u *arbiter.ConsensusParamsUpdate) {
		for i, v := range u.Verifiers {
			if domain, ok := domains[v.NodeID]; ok {
				u.Verifiers[i].FailureDomain = domain
			}
		}
		for i, d := range u.Stage3.IndexerFailureDomains {
			for node, domain := range domains {
				if x.hosts[node] == d.IndexerID {
					u.Stage3.IndexerFailureDomains[i].FailureDomain = domain
				}
			}
		}
	})
}

// updateCmd is stage3Update(t, f) edited by edit and signed by the authority.
func (x *floorFixture) updateCmd(edit func(u *arbiter.ConsensusParamsUpdate)) wire.Command {
	x.t.Helper()
	u := stage3Update(x.t, x.f)
	edit(&u)
	return signedConsensusUpdate(x.t, x.a, u)
}

// update commits updateCmd(edit).
func (x *floorFixture) update(edit func(u *arbiter.ConsensusParamsUpdate)) {
	x.t.Helper()
	mustApply(x.t, x.f, x.updateCmd(edit))
}

// promote passes one block of one statement of owner's table and returns
// its promotion: acknowledged, not published.
func (x *floorFixture) promote(owner uint64) uint64 {
	x.t.Helper()
	_, promotions := passTrackBlock(x.t, x.f, x.a, x.key, x.account, owner, x.next)
	x.next++
	return promotions[0]
}

// promoteTwoParts is promote with a claim of two parts in the statement's own
// partition p<statement seq>, promoted together.
func (x *floorFixture) promoteTwoParts(owner uint64) uint64 {
	t, f := x.t, x.f
	t.Helper()
	stmt := admitTrack(t, f, x.key, x.account, x.next, owner)
	x.next++
	block := mustSeal(t, f).BlockSeq
	ss := f.st.Statements[stmt]
	partition := fmt.Sprintf("p%d", stmt)
	labelA, labelB := fmt.Sprintf("floor-row-%d-a", stmt), fmt.Sprintf("floor-row-%d-b", stmt)
	_, claim := signedRC(t, f, snodeSigner(t, ss.SourceNode), arbiter.RCRecord{
		StatementID: ss.Env.StatementID, SourceNode: ss.SourceNode, SourceClaimRoot: "0xclaim",
		CandidateParts: []arbiter.CandidatePart{
			{TableID: ss.Env.TargetTableID, PartitionID: partition, PartName: fmt.Sprintf("all_%d_0", stmt), PartRowLtHash: lthashHex(labelA), RowCount: 1, Bytes: 32},
			{TableID: ss.Env.TargetTableID, PartitionID: partition, PartName: fmt.Sprintf("all_%d_1", stmt), PartRowLtHash: lthashHex(labelB), RowCount: 1, Bytes: 32},
		},
		PartitionNewPartSums: []arbiter.PartitionLtHashSum{{TableID: ss.Env.TargetTableID, PartitionID: partition, NewPartsLtHashSum: lthashHex(labelA, labelB)}},
	})
	mustApply(t, f, claim)
	verifyBlock(t, f, block)
	anchorFinal(t, f, block)
	var parts []arbiter.PartRef
	for _, cp := range f.st.Statements[stmt].RC.CandidateParts {
		parts = append(parts, arbiter.PartRef{TableID: cp.TableID, PartitionID: cp.PartitionID, PartRowLtHash: cp.PartRowLtHash})
	}
	promote := arbiter.PromoteSafePartition{TableID: ss.Env.TargetTableID, PartitionID: partition, PromotionSeq: f.st.PromotionSeq + 1, CandidateParts: parts}
	token, err := x.a.SignPromotionWithContextAt(promote, authorityContextFor(t, f), authoritytest.Iat)
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, f, wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: promote, AuthorityJWS: token}})
	ackTrackPromotion(t, f, promote.PromotionSeq)
	return promote.PromotionSeq
}

// issue is promote without the acknowledgement: the promotion is issued and
// unacknowledged.
func (x *floorFixture) issue(owner uint64) uint64 {
	t, f := x.t, x.f
	t.Helper()
	stmt := admitTrack(t, f, x.key, x.account, x.next, owner)
	x.next++
	block := mustSeal(t, f).BlockSeq
	bindTrackClaim(t, f, stmt)
	verifyBlock(t, f, block)
	anchorFinal(t, f, block)
	return promoteTrack(t, f, x.a, stmt)
}

// publish publishes the manifest the FSM's inputs describe.
func (x *floorFixture) publish() {
	x.t.Helper()
	publishManifestInputs(x.t, x.f)
}

// cleanupCmd is the authority-signed cleanup of promotion p's parts.
func (x *floorFixture) cleanupCmd(p uint64) wire.Command {
	x.t.Helper()
	return partCleanup(x.t, x.f, x.a, p)
}

// clean schedules promotion p's cleanup and applies its acknowledgement.
func (x *floorFixture) clean(p uint64) {
	x.t.Helper()
	mustApply(x.t, x.f, x.cleanupCmd(p))
	mustApply(x.t, x.f, partCleanupAck(x.t, x.f, p))
}

// checkpointCmd is member node's next checkpoint for owner: every group of
// owner it is a current member of, at its current generation, the latest
// published manifest and owner's entry in it, with excluded placed in the
// group of each table's database, signed by stage3ReplicaSigner(node).
func (x *floorFixture) checkpointCmd(node string, owner uint64, excluded ...arbiter.ExcludedTable) wire.Command {
	t, f := x.t, x.f
	t.Helper()
	var groups []arbiter.CheckpointGroup
	for _, g := range f.st.Stage3.Groups {
		if g.Owner != owner || groupMemberByID(g, node) == nil {
			continue
		}
		group := arbiter.CheckpointGroup{DatabaseID: g.DatabaseID, GroupGeneration: g.Generation}
		for _, e := range excluded {
			if database, _, _ := splitTableKey(e.TableID); database == g.DatabaseID {
				group.Excluded = append(group.Excluded, e)
			}
		}
		groups = append(groups, group)
	}
	if len(groups) == 0 {
		t.Fatalf("%s is a member of no group of indexer %d", node, owner)
	}
	key := fmt.Sprintf("%s/%d", node, owner)
	x.seqs[key]++
	cp := arbiter.MemberCheckpoint{NodeID: node, OwnerIndexerID: owner, CheckpointSeq: x.seqs[key], Groups: groups,
		SnapshotID: f.st.SafeWatermark.SnapshotID, Entry: f.publishedTrackFrontierLocked(owner)}
	jws, err := stage3ReplicaSigner(t, node).SignMemberCheckpoint(messageContextFor(t, f), cp)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{MemberCheckpoint: &wire.MemberCheckpoint{Checkpoint: cp, ReplicaJWS: jws}}
}

// checkpoint applies checkpointCmd.
func (x *floorFixture) checkpoint(node string, owner uint64, excluded ...arbiter.ExcludedTable) {
	x.t.Helper()
	mustApply(x.t, x.f, x.checkpointCmd(node, owner, excluded...))
}

// checkpointAll checkpoints every member for both owners, skipping an owner
// none of whose groups lists it.
func (x *floorFixture) checkpointAll() {
	x.t.Helper()
	for _, node := range slices.Sorted(maps.Keys(x.hosts)) {
		for _, owner := range []uint64{0, floorOwner} {
			for _, g := range x.f.st.Stage3.Groups {
				if g.Owner == owner && groupMemberByID(g, node) != nil {
					x.checkpoint(node, owner)
					break
				}
			}
		}
	}
}

// rebalanceCmd is the authority-signed RebalanceGroup of G(owner, database)
// at its current generation, adding add and removing remove (each sorted),
// with network_fill unchanged.
func (x *floorFixture) rebalanceCmd(owner uint64, database string, add, remove []string) wire.Command {
	t := x.t
	t.Helper()
	g := x.group(owner, database)
	cmd := wire.RebalanceGroup{OwnerIndexerID: owner, DatabaseID: database, ExpectedGeneration: g.Generation, Add: add, Remove: remove, NetworkFill: g.NetworkFill}
	token, err := x.a.SignRebalanceGroupWithContext(cmd.Canonical(), authorityContextFor(t, x.f))
	if err != nil {
		t.Fatal(err)
	}
	cmd.AuthorityJWS = token
	return wire.Command{RebalanceGroup: &cmd}
}

// laggingCmd is the authority-signed MarkMemberLagging of node in
// G(owner, database), observed at owner's latest published entry.
func (x *floorFixture) laggingCmd(node string, owner uint64, database string) wire.Command {
	t := x.t
	t.Helper()
	cmd := wire.MarkMemberLagging{NodeID: node, OwnerIndexerID: owner, DatabaseID: database, ObservedEntry: x.f.publishedTrackFrontierLocked(owner)}
	token, err := x.a.SignMarkMemberLaggingWithContext(cmd.Canonical(), authorityContextFor(t, x.f))
	if err != nil {
		t.Fatal(err)
	}
	cmd.AuthorityJWS = token
	return wire.Command{MarkMemberLagging: &cmd}
}

// evictCmd is the authority eviction of node at its registration seq 1.
func (x *floorFixture) evictCmd(node string) wire.Command {
	t := x.t
	t.Helper()
	cmd := arbiter.EvictNodeCommand{NodeID: node, ExpectedRegistrationSeq: 1, Reason: "decommissioned"}
	token, err := x.a.SignEvictNodeWithContext(cmd, authorityContextFor(t, x.f))
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{EvictNode: &wire.EvictNode{NodeID: node, ExpectedRegistrationSeq: 1, Reason: cmd.Reason, AuthorityJWS: token}}
}

// group is G(owner, database), which must exist.
func (x *floorFixture) group(owner uint64, database string) *DatabaseGroup {
	x.t.Helper()
	g := x.f.groupLocked(owner, database)
	if g == nil {
		x.t.Fatalf("no group for indexer %d database %q", owner, database)
	}
	return g
}

// leaving is node's Leaving record in G(owner, database), which must exist.
func (x *floorFixture) leaving(owner uint64, database, node string) *LeavingMember {
	x.t.Helper()
	g := x.group(owner, database)
	for i := range g.Leaving {
		if g.Leaving[i].Member.NodeID == node {
			return &g.Leaving[i]
		}
	}
	x.t.Fatalf("%s is not leaving G(%d, %s)", node, owner, database)
	return nil
}

// verdict is the floor's verdict for promotion p now.
func (x *floorFixture) verdict(p uint64) FloorVerdict {
	x.t.Helper()
	pp := x.f.st.PendingPromotions[p]
	if pp == nil {
		x.t.Fatalf("promotion %d is not recorded", p)
	}
	return x.f.floorLocked(pp)
}

// work is the WorkSet's PublishedUncleaned row for promotion p.
func (x *floorFixture) work(p uint64) (CleanupWork, bool) {
	x.t.Helper()
	ws, err := x.f.WorkSet()
	if err != nil {
		x.t.Fatal(err)
	}
	for _, row := range ws.PublishedUncleaned {
		if row.PromotionSeq == p {
			return row, true
		}
	}
	return CleanupWork{}, false
}

// workSet is the WorkSet now.
func (x *floorFixture) workSet() WorkSet {
	x.t.Helper()
	ws, err := x.f.WorkSet()
	if err != nil {
		x.t.Fatal(err)
	}
	return ws
}

// groupMemberNodes lists g's current members.
func groupMemberNodes(g *DatabaseGroup) []string {
	var out []string
	for _, m := range g.Members {
		out = append(out, m.NodeID)
	}
	return out
}

// floorRefusalText is the count clause's refusal for promotion p of table
// with required and reflecting domains, owner floorOwner.
func floorRefusalText(p uint64, table string, required, has int) string {
	return fmt.Sprintf("copy floor: promotion %d of %s needs reflecting checkpoints in %d distinct failure domains other than owner %d's, has %d",
		p, table, required, floorOwner, has)
}

// requireFloorCleanup applies promotion p's cleanup and requires Apply's
// answer to be want ("" for Applied), with state unchanged on a refusal.
func requireFloorCleanup(t *testing.T, x *floorFixture, p uint64, want string) {
	t.Helper()
	if want != "" {
		rejectTracksUnchanged(t, x.f, x.cleanupCmd(p), want)
		return
	}
	mustApply(t, x.f, x.cleanupCmd(p))
	if x.f.st.PendingCleanups[p] == nil {
		t.Fatalf("promotion %d has no pending cleanup after an applied schedule", p)
	}
}

// coverText is base(P)'s block-clause refusal for promotion p of floorOwner.
func coverText(x *floorFixture, p uint64) string {
	pp := x.f.st.PendingPromotions[p]
	return fmt.Sprintf("copy floor: promotion %d covers block %d above owner %d's published safe_block_seq %d",
		p, x.f.promotionBlockLocked(pp), floorOwner, x.f.publishedTrackFrontierLocked(floorOwner).SafeBlockSeq)
}

// underReplicatedEvents lists the promotions of the EventUnderReplicated
// hints in events.
func underReplicatedEvents(events []Event) []uint64 {
	var out []uint64
	for _, e := range events {
		if e.Kind == EventUnderReplicated {
			out = append(out, e.PromotionSeq)
		}
	}
	return out
}

// TestFloorSwitchOffIsByteIdentical: before the checkpoints switch every
// answer is the stage-2 one (spec §9.3, contract §53): Apply schedules the
// cleanup of a promotion no manifest covers, the WorkSet lists a published
// cleanup no member reflects with no reason and holds nothing, the floor
// gauges are zero, no under_replicated hint is emitted and a Leaving member
// is not released. The checkpoints switch-on update is the first that
// evaluates the release.
func TestFloorSwitchOffIsByteIdentical(t *testing.T) {
	x := newFloorFixtureBeforeCheckpoints(t, floorFixtureOptions{MemberDomains: map[string]string{floorMemberB: floorDomainB}})
	f := x.f
	events := make(chan Event, 256)
	f.notify = events
	early := x.promote(floorOwner)
	requireFloorCleanup(t, x, early, "")
	mustApply(t, f, partCleanupAck(t, f, early))
	x.publish()
	p := x.promote(floorOwner)
	x.publish()
	ws := x.workSet()
	if row, listed := x.work(p); !listed || row.UnderReplicated != UnderReplicatedNone || ws.FloorHeld != nil {
		t.Fatalf("row %+v listed %v, FloorHeld %v; want listed with no reason and nothing held", row, listed, ws.FloorHeld)
	}
	if m := f.MetricsSnapshot(); m.FloorHeldCleanups != 0 || m.UnderReplicatedGroups != 0 || m.SafeCopiesMin != nil {
		t.Fatalf("floor gauges %d %d %v before the switch", m.FloorHeldCleanups, m.UnderReplicatedGroups, m.SafeCopiesMin)
	}
	x.clean(p)
	mustApply(t, f, x.rebalanceCmd(floorOwner, "db", nil, []string{floorMemberB}))
	if got := underReplicatedEvents(drain(events)); got != nil {
		t.Fatalf("under_replicated hints %v before the switch", got)
	}
	if x.leaving(floorOwner, "db", floorMemberB).Released {
		t.Fatal("a Leaving member was released before the checkpoints switch")
	}
	turnOnStage3Switch(t, f, x.a, Stage3SwitchCheckpoints)
	if !x.leaving(floorOwner, "db", floorMemberB).Released {
		t.Fatal("the checkpoints switch-on did not release a Leaving member at k = 1")
	}
}

// TestFloorIsDormantAtKOne: with the switch on and k = 1 only the new
// publication clauses bite, in Apply and the WorkSet alike: an unpublished
// promotion is refused and not listed, a published one is cleaned although
// no member reflects it, and nothing escapes or is under-replicated, even
// above the uncleaned-parts bound.
func TestFloorIsDormantAtKOne(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 1, MemberDomains: map[string]string{floorMemberB: floorDomainB}, MaxPerTable: 1})
	f := x.f
	events := make(chan Event, 256)
	f.notify = events
	p1 := x.promote(floorOwner)
	if _, listed := x.work(p1); listed {
		t.Fatal("the WorkSet lists an unpublished promotion's cleanup")
	}
	requireFloorCleanup(t, x, p1, coverText(x, p1))
	x.publish()
	p2 := x.promote(floorOwner)
	x.publish()
	for _, p := range []uint64{p1, p2} {
		v := x.verdict(p)
		row, listed := x.work(p)
		if !v.Allowed || v.Escape != UnderReplicatedNone || v.UnderReplicated != UnderReplicatedNone || v.Required != 0 || !listed || row.UnderReplicated != UnderReplicatedNone {
			t.Fatalf("promotion %d: verdict %+v, row %+v listed %v; want allowed with nothing escaped", p, v, row, listed)
		}
		requireFloorCleanup(t, x, p, "")
	}
	if held := x.workSet().FloorHeld; held != nil {
		t.Fatalf("FloorHeld %v at k = 1", held)
	}
	if got := underReplicatedEvents(drain(events)); got != nil {
		t.Fatalf("under_replicated hints %v at k = 1", got)
	}
}

// requireFloorAgreement checks, for every acknowledged promotion with
// uncleaned parts and no pending cleanup, that the WorkSet lists its cleanup
// exactly when Apply schedules it, with the verdict's reason, and holds it
// exactly when the floor alone does (contract §53, Review Focus 1). It
// returns the kinds of verdict it saw.
func requireFloorAgreement(t *testing.T, x *floorFixture) map[string]bool {
	t.Helper()
	f := x.f
	ws := x.workSet()
	if !slices.Equal(ws.FloorHeld, f.floorOnlyHeldLocked()) {
		t.Fatalf("WorkSet.FloorHeld %v, floorOnlyHeldLocked %v", ws.FloorHeld, f.floorOnlyHeldLocked())
	}
	listed := map[uint64]CleanupWork{}
	for _, row := range ws.PublishedUncleaned {
		listed[row.PromotionSeq] = row
	}
	kinds := map[string]bool{}
	for _, seq := range slices.Sorted(maps.Keys(f.st.PendingPromotions)) {
		pp := f.st.PendingPromotions[seq]
		if !pp.Acked || f.st.PendingCleanups[seq] != nil || !f.promotionUncleanedLocked(pp) {
			continue
		}
		v := f.floorLocked(pp)
		row, ok := listed[seq]
		if ok != v.Allowed || (ok && row.UnderReplicated != v.UnderReplicated) || slices.Contains(ws.FloorHeld, seq) != v.FloorOnly {
			t.Fatalf("promotion %d: verdict %+v, row %+v listed %v, FloorHeld %v", seq, v, row, ok, ws.FloorHeld)
		}
		res := f.Apply(mkLog(t, x.cleanupCmd(seq)))
		rejected, refused := res.(Rejected)
		if refused == v.Allowed || (refused && rejected.Reason != f.floorRefusalLocked(pp, v)) {
			t.Fatalf("promotion %d: Apply answered %+v for verdict %+v", seq, res, v)
		}
		switch {
		case v.BaseRefusal != "":
			kinds["base"] = true
		case v.FloorOnly:
			kinds["floor_only"] = true
		case v.UnderReplicated != UnderReplicatedNone:
			kinds[string(v.UnderReplicated)] = true
		default:
			kinds["allowed"] = true
		}
	}
	return kinds
}

// TestFloorApplyAndWorkSetAgreeOnEveryCleanup: across histories that reach
// every verdict (allowed, held by the count, refused by base(P), each escape
// and |E| below k - 1) the leader proposes exactly the cleanups Apply takes.
func TestFloorApplyAndWorkSetAgreeOnEveryCleanup(t *testing.T) {
	two := map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC}
	seen := map[string]bool{}
	for _, tc := range []struct {
		name  string
		o     floorFixtureOptions
		build func(x *floorFixture)
	}{
		{"reflected, held and unpublished", floorFixtureOptions{K: 2, MemberDomains: two}, func(x *floorFixture) {
			x.promote(floorOwner)
			x.publish()
			x.checkpoint(floorMemberB, floorOwner)
			x.promote(floorOwner)
			x.publish()
			x.promote(floorOwner)
		}},
		{"table pressure", floorFixtureOptions{K: 2, MemberDomains: two, MaxPerTable: 2}, func(x *floorFixture) {
			for range 3 {
				x.promote(floorOwner)
				x.publish()
			}
		}},
		{"partition pressure", floorFixtureOptions{K: 2, MemberDomains: two, MaxPerPartition: 1}, func(x *floorFixture) {
			x.promoteTwoParts(floorOwner)
			x.publish()
			x.promote(floorOwner)
			x.publish()
		}},
		{"retired table", floorFixtureOptions{K: 2, MemberDomains: two}, func(x *floorFixture) {
			x.promote(floorOwner)
			x.publish()
			mustApply(x.t, x.f, retireCmd("db", wire.TableRetireReasonTableDeleted, ev(400, "0x400", 0), "t"))
		}},
		{"no counting domain left", floorFixtureOptions{K: 2, MemberDomains: map[string]string{floorMemberB: floorDomainB}}, func(x *floorFixture) {
			x.promote(floorOwner)
			x.publish()
			mustApply(x.t, x.f, x.laggingCmd(floorMemberB, floorOwner, "db"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := newFloorFixture(t, tc.o)
			tc.build(x)
			maps.Copy(seen, requireFloorAgreement(t, x))
		})
	}
	want := []string{"allowed", "base", "domains_below_k", "floor_only", "inactive_incarnation", "pressure_partition", "pressure_table"}
	if got := slices.Sorted(maps.Keys(seen)); !slices.Equal(got, want) {
		t.Fatalf("verdict kinds seen %v, want %v", got, want)
	}
}

// TestFloorPublicationClausesRefuseAnUnpublishedPromotion: base(P)'s new
// clauses refuse, with their own texts and state unchanged, the cleanup of a
// promotion that is not acknowledged, whose block the owner's published
// entry does not cover, or whose sequence is above the entry's cutoff; the
// WorkSet lists none of them.
func TestFloorPublicationClausesRefuseAnUnpublishedPromotion(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 1, MemberDomains: map[string]string{floorMemberB: floorDomainB}})
	f := x.f
	p := x.issue(floorOwner)
	requireFloorCleanup(t, x, p, fmt.Sprintf("copy floor: promotion %d is not acknowledged", p))
	ackTrackPromotion(t, f, p)
	requireFloorCleanup(t, x, p, coverText(x, p))
	if _, listed := x.work(p); listed {
		t.Fatal("the WorkSet lists a cleanup the publication clauses refuse")
	}
	x.publish()
	published := f.publishedTrackFrontierLocked(floorOwner)
	cutoff := published
	cutoff.AppliedPromotionSeq = p - 1
	frontiers := slices.Clone(f.st.Manifests[f.st.SafeWatermark.SnapshotID].TrackFrontiers)
	for i := range frontiers {
		if frontiers[i].IndexerID == floorOwner {
			frontiers[i] = cutoff
		}
	}
	setPublishedTrackFrontiers(t, f, frontiers...)
	requireFloorCleanup(t, x, p, fmt.Sprintf("copy floor: promotion %d is above owner %d's published applied_promotion_seq %d", p, floorOwner, p-1))
	if _, listed := x.work(p); listed {
		t.Fatal("the WorkSet lists a cleanup above the published cutoff")
	}
	frontiers = slices.Clone(frontiers)
	for i := range frontiers {
		if frontiers[i].IndexerID == floorOwner {
			frontiers[i] = published
		}
	}
	setPublishedTrackFrontiers(t, f, frontiers...)
	requireFloorCleanup(t, x, p, "")
}

// TestFloorHoldsCleanupUntilKMinusOneDomainsReflect: at k = 3 with three
// counting members in three domains a published promotion's cleanup waits,
// refused with the count text and held in the WorkSet, until members in two
// distinct domains have checkpoints that reflect it.
func TestFloorHoldsCleanupUntilKMinusOneDomainsReflect(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 3, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC, floorMemberD: floorDomainD}})
	p := x.promote(floorOwner)
	x.publish()
	for i, step := range []func(){nil, func() { x.checkpoint(floorMemberB, floorOwner) }} {
		if step != nil {
			step()
		}
		v := x.verdict(p)
		if !v.FloorOnly || v.Required != 2 || v.EDomains != 3 || len(v.ReflectingDomains) != i {
			t.Fatalf("after %d checkpoints: verdict %+v", i, v)
		}
		if held := x.workSet().FloorHeld; !slices.Equal(held, []uint64{p}) {
			t.Fatalf("FloorHeld %v, want [%d]", held, p)
		}
		requireFloorCleanup(t, x, p, floorRefusalText(p, "db.t", 2, i))
	}
	x.checkpoint(floorMemberC, floorOwner)
	if v := x.verdict(p); !v.Allowed || !slices.Equal(v.ReflectingDomains, []string{floorDomainB, floorDomainC}) || v.UnderReplicated != UnderReplicatedNone {
		t.Fatalf("verdict %+v, want allowed by node:b and node:c", v)
	}
	if row, listed := x.work(p); !listed || row.UnderReplicated != UnderReplicatedNone {
		t.Fatalf("row %+v listed %v", row, listed)
	}
	requireFloorCleanup(t, x, p, "")
}

// TestFloorCountsTwoMembersInOneDomainOnce: two members in one failure
// domain count once, whichever of them reflects the promotion.
func TestFloorCountsTwoMembersInOneDomainOnce(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 3, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainB, floorMemberD: floorDomainD}})
	p := x.promote(floorOwner)
	x.publish()
	x.checkpoint(floorMemberB, floorOwner)
	x.checkpoint(floorMemberC, floorOwner)
	if v := x.verdict(p); !v.FloorOnly || v.EDomains != 2 || v.Required != 2 || !slices.Equal(v.ReflectingDomains, []string{floorDomainB}) {
		t.Fatalf("verdict %+v, want node:b counted once", v)
	}
	requireFloorCleanup(t, x, p, floorRefusalText(p, "db.t", 2, 1))
	x.checkpoint(floorMemberD, floorOwner)
	requireFloorCleanup(t, x, p, "")
}

// TestFloorParamsUpdateThatCollidesDomainsOnlyLowersTheCount: an update that
// puts two members of a group in one domain, or a member in its owner's
// domain, is accepted (spec §9.3); it only lowers |E|, and a group whose |E|
// falls below k - 1 is under-replicated.
func TestFloorParamsUpdateThatCollidesDomainsOnlyLowersTheCount(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 3, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC, floorMemberD: floorDomainD}})
	f := x.f
	g := x.group(floorOwner, "db")
	domains := func() int { return len(f.floorMembersLocked(g, false)) }
	if domains() != 3 {
		t.Fatalf("|E| %d, want 3", domains())
	}
	x.setDomains(map[string]string{floorMemberC: floorDomainB})
	if domains() != 2 || f.MetricsSnapshot().UnderReplicatedGroups != 0 {
		t.Fatalf("|E| %d after the collision, want 2 and no group under-replicated", domains())
	}
	x.setDomains(map[string]string{floorMemberD: floorDomainOwner})
	if domains() != 1 || f.MetricsSnapshot().UnderReplicatedGroups != 1 {
		t.Fatalf("|E| %d after a member joined the owner's domain, want 1 and G(1, db) under-replicated", domains())
	}
	p := x.promote(floorOwner)
	x.publish()
	requireFloorCleanup(t, x, p, floorRefusalText(p, "db.t", 1, 0))
	x.checkpoint(floorMemberD, floorOwner)
	requireFloorCleanup(t, x, p, floorRefusalText(p, "db.t", 1, 0))
	x.checkpoint(floorMemberC, floorOwner)
	if v := x.verdict(p); !v.Allowed || v.UnderReplicated != UnderReplicatedDomainsBelowK {
		t.Fatalf("verdict %+v, want allowed and domains_below_k", v)
	}
	requireFloorCleanup(t, x, p, "")
}

// TestFloorExcludedTableDoesNotCount: a checkpoint whose group excludes the
// promotion's table does not reflect it, and a later one that no longer
// excludes it does.
func TestFloorExcludedTableDoesNotCount(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 2, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC}})
	p := x.promote(floorOwner)
	x.publish()
	x.checkpoint(floorMemberB, floorOwner, arbiter.ExcludedTable{TableID: "db.t", Reason: arbiter.ExclusionNotInstalled})
	if v := x.verdict(p); !v.FloorOnly || len(v.ReflectingDomains) != 0 {
		t.Fatalf("verdict %+v, want an excluding checkpoint not counted", v)
	}
	requireFloorCleanup(t, x, p, floorRefusalText(p, "db.t", 1, 0))
	x.checkpoint(floorMemberB, floorOwner)
	requireFloorCleanup(t, x, p, "")
}

// floorRetired is the purge-exemption history: a published promotion the
// count holds, then db.t retired, so its incarnation is no longer Active.
func floorRetired(t *testing.T) (*floorFixture, uint64) {
	t.Helper()
	x := newFloorFixture(t, floorFixtureOptions{K: 2, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC}})
	p := x.promote(floorOwner)
	x.publish()
	if !x.verdict(p).FloorOnly {
		t.Fatalf("verdict %+v before the retire, want held", x.verdict(p))
	}
	mustApply(t, x.f, retireCmd("db", wire.TableRetireReasonTableDeleted, ev(400, "0x400", 0), "t"))
	return x, p
}

// TestFloorPurgeExemptionForANonActiveIncarnation: once the promotion's
// incarnation is not Active the floor lets its cleanup through as an
// escape, so a purge never waits on members.
func TestFloorPurgeExemptionForANonActiveIncarnation(t *testing.T) {
	x, p := floorRetired(t)
	if inc := x.f.floorIncarnationLocked("db.t"); inc == nil || inc.Status == TableStatusActive {
		t.Fatalf("db.t's incarnation %+v, want retired", inc)
	}
	if v := x.verdict(p); !v.Allowed || v.Escape != UnderReplicatedInactiveIncarnation {
		t.Fatalf("verdict %+v, want the purge exemption", v)
	}
	requireFloorCleanup(t, x, p, "")
}

// floorPressure is the pressure history at k = 2 with no reflecting member
// and bounds 1 per partition and 2 per table: two single-part promotions,
// which sit at both bounds and so stay held, then a two-part promotion in
// one partition.
func floorPressure(t *testing.T) (x *floorFixture, single []uint64, double uint64) {
	t.Helper()
	x = newFloorFixture(t, floorFixtureOptions{K: 2, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC},
		MaxPerPartition: 1, MaxPerTable: 2})
	for range 2 {
		single = append(single, x.promote(floorOwner))
		x.publish()
	}
	for _, p := range single {
		if v := x.verdict(p); !v.FloorOnly {
			t.Fatalf("promotion %d: verdict %+v at both bounds, want held", p, v)
		}
	}
	double = x.promoteTwoParts(floorOwner)
	x.publish()
	return x, single, double
}

// TestFloorPressureBypassPerPartitionAndPerTable: outside strict mode a
// partition above max_uncleaned_parts_per_partition, else a table above
// max_uncleaned_parts_per_table, lets the held cleanup through; "above" is
// strictly greater.
func TestFloorPressureBypassPerPartitionAndPerTable(t *testing.T) {
	x, single, double := floorPressure(t)
	perPartition, total := x.f.uncleanedPartsLocked("db.t")
	if total != 4 || perPartition[x.f.st.PendingPromotions[double].Promote.PartitionID] != 2 {
		t.Fatalf("uncleaned parts %v total %d, want 4 with 2 in one partition", perPartition, total)
	}
	if v := x.verdict(double); !v.Allowed || v.Escape != UnderReplicatedPressurePartition {
		t.Fatalf("two-part promotion: verdict %+v, want the partition bypass", v)
	}
	for _, p := range single {
		if v := x.verdict(p); !v.Allowed || v.Escape != UnderReplicatedPressureTable {
			t.Fatalf("promotion %d: verdict %+v, want the table bypass", p, v)
		}
	}
	for _, p := range append(single, double) {
		requireFloorCleanup(t, x, p, "")
	}
}

// TestFloorEscapesRaiseUnderReplicated: a cleanup Apply takes through an
// escape emits EventUnderReplicated for its promotion, and the WorkSet row
// carries the escape as its reason.
func TestFloorEscapesRaiseUnderReplicated(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(t *testing.T) (*floorFixture, map[uint64]UnderReplicatedReason)
	}{
		{"inactive_incarnation", func(t *testing.T) (*floorFixture, map[uint64]UnderReplicatedReason) {
			x, p := floorRetired(t)
			return x, map[uint64]UnderReplicatedReason{p: UnderReplicatedInactiveIncarnation}
		}},
		{"pressure", func(t *testing.T) (*floorFixture, map[uint64]UnderReplicatedReason) {
			x, single, double := floorPressure(t)
			return x, map[uint64]UnderReplicatedReason{single[0]: UnderReplicatedPressureTable, single[1]: UnderReplicatedPressureTable,
				double: UnderReplicatedPressurePartition}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, want := tc.build(t)
			events := make(chan Event, 256)
			x.f.notify = events
			for _, p := range slices.Sorted(maps.Keys(want)) {
				if row, listed := x.work(p); !listed || row.UnderReplicated != want[p] {
					t.Fatalf("promotion %d: row %+v listed %v, want reason %s", p, row, listed, want[p])
				}
				requireFloorCleanup(t, x, p, "")
			}
			if got := underReplicatedEvents(drain(events)); !slices.Equal(got, slices.Sorted(maps.Keys(want))) {
				t.Fatalf("under_replicated hints %v, want %v", got, slices.Sorted(maps.Keys(want)))
			}
		})
	}
}

// TestFloorDomainsBelowKRaisesUnderReplicated: when |E| falls below k - 1
// the count asks only for |E| domains, so the cleanup is taken, flagged
// domains_below_k, hinted and counted in the group gauge.
func TestFloorDomainsBelowKRaisesUnderReplicated(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 2, MemberDomains: map[string]string{floorMemberB: floorDomainB}})
	p := x.promote(floorOwner)
	x.publish()
	mustApply(t, x.f, x.laggingCmd(floorMemberB, floorOwner, "db"))
	v := x.verdict(p)
	if !v.Allowed || v.EDomains != 0 || v.Required != 0 || v.UnderReplicated != UnderReplicatedDomainsBelowK {
		t.Fatalf("verdict %+v, want allowed with domains_below_k", v)
	}
	if row, listed := x.work(p); !listed || row.UnderReplicated != UnderReplicatedDomainsBelowK {
		t.Fatalf("row %+v listed %v", row, listed)
	}
	if got := x.f.MetricsSnapshot().UnderReplicatedGroups; got != 1 {
		t.Fatalf("arbiter_under_replicated_groups %d, want 1", got)
	}
	events := make(chan Event, 16)
	x.f.notify = events
	requireFloorCleanup(t, x, p, "")
	if got := underReplicatedEvents(drain(events)); !slices.Equal(got, []uint64{p}) {
		t.Fatalf("under_replicated hints %v, want [%d]", got, p)
	}
}

// TestRejectedPartCleanupIsNeverGatedByTheFloor: a rejected-part cleanup
// (promotion seq 0, C4) is scheduled at k = 2 although no member reflects
// anything, and the WorkSet lists it.
func TestRejectedPartCleanupIsNeverGatedByTheFloor(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 2, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC}})
	f := x.f
	stmt := admitTrack(t, f, x.key, x.account, x.next, floorOwner)
	x.next++
	block := mustSeal(t, f).BlockSeq
	bindTrackClaim(t, f, stmt)
	verifyBlock(t, f, block)
	rejectBlockQ(t, f, block)
	track := trackQ(t, f, floorOwner)
	if !track.Quarantined {
		t.Fatalf("track %d = %+v, want quarantined by the rejected block", floorOwner, track)
	}
	mustApply(t, f, releaseQ(t, f, x.a, floorOwner, track.QuarantineGeneration, block))
	ws := x.workSet()
	if len(ws.RejectedUncleaned) != 1 || ws.RejectedUncleaned[0].Cleanup.RejectedBlockSeq != block {
		t.Fatalf("RejectedUncleaned %+v, want block %d's partition", ws.RejectedUncleaned, block)
	}
	mustApply(t, f, rejectedCleanupQ(t, f, x.a, ws.RejectedUncleaned[0].Cleanup))
}

// TestCleanupScheduledBeforeTheCheckpointsSwitchIsNotRechecked: a cleanup
// scheduled before the switch (here kept pending by a quarantine across the
// switch-on) is not judged by the floor: its re-proposal is the idempotent
// re-schedule and its acknowledgement cleans the parts, although the floor
// would refuse a new one (its promotion is not covered by a manifest).
func TestCleanupScheduledBeforeTheCheckpointsSwitchIsNotRechecked(t *testing.T) {
	x := newFloorFixtureBeforeCheckpoints(t, floorFixtureOptions{MemberDomains: map[string]string{floorMemberB: floorDomainB}})
	f := x.f
	p := x.promote(floorOwner)
	requireFloorCleanup(t, x, p, "")
	mustApply(t, f, quarantineQ(t, f, x.a, floorOwner, 0, "stuck before the floor"))
	turnOnStage3Switch(t, f, x.a, Stage3SwitchCheckpoints)
	mustApply(t, f, releaseQ(t, f, x.a, floorOwner, 1))
	if v := x.verdict(p); v.BaseRefusal == "" {
		t.Fatalf("verdict %+v, want base(P) refused: no manifest covers the promotion", v)
	}
	before := snapshotBytes(t, f)
	if res := f.Apply(mkLog(t, x.cleanupCmd(p))); res != (Applied{}) {
		t.Fatalf("re-scheduling the pending cleanup answered %+v, want the idempotent Applied", res)
	}
	if !slices.Equal(before, snapshotBytes(t, f)) {
		t.Fatal("the idempotent re-schedule changed state")
	}
	ws := x.workSet()
	if len(ws.ScheduledUnackedCleanups) != 1 || ws.ScheduledUnackedCleanups[0].PromotionSeq != p || ws.FloorHeld != nil {
		t.Fatalf("ScheduledUnackedCleanups %+v FloorHeld %v, want promotion %d resent and nothing held", ws.ScheduledUnackedCleanups, ws.FloorHeld, p)
	}
	mustApply(t, f, partCleanupAck(t, f, p))
	if f.promotionUncleanedLocked(f.st.PendingPromotions[p]) {
		t.Fatal("the acknowledged cleanup left promoted unsafe parts")
	}
}

// TestFloorKeepsTheQuarantineRefusal: a quarantined owner's cleanup keeps
// today's refusal text, ahead of every floor clause, and is not held.
func TestFloorKeepsTheQuarantineRefusal(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 2, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC}})
	f := x.f
	p := x.promote(floorOwner)
	x.publish()
	unpublished := x.promote(floorOwner)
	mustApply(t, f, quarantineQ(t, f, x.a, floorOwner, 0, "operator"))
	want := fmt.Sprintf("indexer %d is quarantined; no new cleanup is scheduled", floorOwner)
	for _, seq := range []uint64{p, unpublished} {
		if v := x.verdict(seq); seq == p && (v.BaseRefusal != want || v.FloorOnly) {
			t.Fatalf("verdict %+v, want the quarantine refusal", v)
		}
		requireFloorCleanup(t, x, seq, want)
	}
	if held := x.workSet().FloorHeld; held != nil {
		t.Fatalf("FloorHeld %v while the owner is quarantined", held)
	}
}

// TestFloorMetricsSnapshot: the replicated floor gauges (K78) are zero
// before the switch and then follow committed state: the held promotions,
// the groups below k - 1 domains and each owner's fewest safe copies.
func TestFloorMetricsSnapshot(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 2, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC}})
	f := x.f
	type gauges struct {
		held, under uint64
		copies      []MetricSafeCopies
	}
	read := func() gauges {
		m := f.MetricsSnapshot()
		return gauges{m.FloorHeldCleanups, m.UnderReplicatedGroups, m.SafeCopiesMin}
	}
	expect := func(step string, want gauges) {
		t.Helper()
		if got := read(); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: gauges %+v, want %+v", step, got, want)
		}
	}
	expect("caught up", gauges{0, 0, []MetricSafeCopies{{0, 3}, {floorOwner, 3}}})
	p := x.promote(floorOwner)
	x.publish()
	expect("a promotion published", gauges{1, 0, []MetricSafeCopies{{0, 3}, {floorOwner, 1}}})
	x.checkpoint(floorMemberB, floorOwner)
	expect("one member caught up again", gauges{0, 0, []MetricSafeCopies{{0, 3}, {floorOwner, 2}}})
	x.clean(p)
	mustApply(t, f, x.laggingCmd(floorMemberC, floorOwner, "db"))
	mustApply(t, f, x.evictCmd(floorMemberB))
	expect("one member lagging, the other evicted", gauges{0, 1, []MetricSafeCopies{{0, 2}, {floorOwner, 1}}})
}
```

- [ ] **Step 3: Write the strict-mode tests**

Create `fsm/floor_strict_test.go`:

```go
package fsm

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestStrictModeRequiresKMinusOneDomains: in strict mode the count asks for
// k - 1 reflecting domains even when E has fewer, where outside strict mode
// it asks for |E| and flags the cleanup domains_below_k.
func TestStrictModeRequiresKMinusOneDomains(t *testing.T) {
	for _, strict := range []bool{true, false} {
		t.Run(fmt.Sprintf("strict=%v", strict), func(t *testing.T) {
			x := newFloorFixture(t, floorFixtureOptions{K: 3, Strict: strict,
				MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC, floorMemberD: floorDomainD}})
			mustApply(t, x.f, x.evictCmd(floorMemberC))
			mustApply(t, x.f, x.evictCmd(floorMemberD))
			p := x.promote(floorOwner)
			x.publish()
			x.checkpoint(floorMemberB, floorOwner)
			v := x.verdict(p)
			if v.EDomains != 1 || len(v.ReflectingDomains) != 1 {
				t.Fatalf("verdict %+v, want |E| 1 reflected by node:b", v)
			}
			if strict {
				if !v.FloorOnly || v.Required != 2 {
					t.Fatalf("strict verdict %+v, want held for two domains", v)
				}
				requireFloorCleanup(t, x, p, floorRefusalText(p, "db.t", 2, 1))
				return
			}
			if !v.Allowed || v.Required != 1 || v.UnderReplicated != UnderReplicatedDomainsBelowK {
				t.Fatalf("verdict %+v, want allowed with domains_below_k", v)
			}
			requireFloorCleanup(t, x, p, "")
		})
	}
}

// TestStrictModeKeepsLaggingMembersInE: a lagging member stays in E in
// strict mode, so its checkpoint still reflects what it covered; outside
// strict mode it leaves E.
func TestStrictModeKeepsLaggingMembersInE(t *testing.T) {
	for _, strict := range []bool{true, false} {
		t.Run(fmt.Sprintf("strict=%v", strict), func(t *testing.T) {
			x := newFloorFixture(t, floorFixtureOptions{K: 2, Strict: strict, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC}})
			p1 := x.promote(floorOwner)
			x.publish()
			x.checkpoint(floorMemberB, floorOwner)
			x.promote(floorOwner)
			x.publish()
			mustApply(t, x.f, x.laggingCmd(floorMemberB, floorOwner, "db"))
			g := x.group(floorOwner, "db")
			if m := groupMemberByID(g, floorMemberB); m.Class != MemberLagging {
				t.Fatalf("%s is %s, want lagging", floorMemberB, m.Class)
			}
			inE := slices.Contains(x.f.floorMembersLocked(g, strict)[floorDomainB], floorMemberB)
			v := x.verdict(p1)
			if strict {
				if !inE || !v.Allowed || !slices.Equal(v.ReflectingDomains, []string{floorDomainB}) {
					t.Fatalf("strict: %s in E %v, verdict %+v; want the lagging member counted", floorMemberB, inE, v)
				}
				requireFloorCleanup(t, x, p1, "")
				return
			}
			if inE || !v.FloorOnly {
				t.Fatalf("%s in E %v, verdict %+v; want the lagging member left out", floorMemberB, inE, v)
			}
			requireFloorCleanup(t, x, p1, floorRefusalText(p1, "db.t", 1, 0))
		})
	}
}

// strictBackpressure is the strict-mode backpressure history at k = 2 with
// max_uncleaned_parts_per_table 1: two published promotions of db.t that no
// member reflects, so db.t holds two uncleaned parts the floor alone holds.
// It returns the fixture and the next statement's envelope.
func strictBackpressure(t *testing.T, strict bool) (*floorFixture, arbiter.StatementEnvelope) {
	t.Helper()
	x := newFloorFixture(t, floorFixtureOptions{K: 2, Strict: strict, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC},
		MaxPerTable: 1})
	for range 2 {
		x.promote(floorOwner)
		x.publish()
	}
	return x, trackEnvelope(t, x.f, x.key, x.account, "", x.next, floorOwner)
}

// TestStrictModeTurnsPressureIntoCopiesBackpressure: in strict mode the
// pressure bypass is off and the owner's next statement for a table whose
// floor-held parts exceed a bound gets the retryable COPIES_BACKPRESSURE
// (K59), naming the partition bound first; outside strict mode the bypass
// lets the cleanups through and the statement is admitted.
func TestStrictModeTurnsPressureIntoCopiesBackpressure(t *testing.T) {
	t.Run("table", func(t *testing.T) {
		x, env := strictBackpressure(t, true)
		for _, p := range x.f.floorOnlyHeldLocked() {
			if v := x.verdict(p); v.Escape != UnderReplicatedNone {
				t.Fatalf("promotion %d escaped in strict mode: %+v", p, v)
			}
		}
		r := submit(t, x.f, env)
		want := "storage-integrity copy floor: db.t holds 2 uncleaned promoted parts (max_uncleaned_parts_per_table 1) whose cleanup waits for member checkpoints (strict_copies); retry later"
		if r.Code != arbiter.AdmissionCodeCopiesBackpressure || r.Message != want {
			t.Fatalf("submit = %+v, want COPIES_BACKPRESSURE %q", r, want)
		}
	})
	t.Run("partition", func(t *testing.T) {
		x := newFloorFixture(t, floorFixtureOptions{K: 2, Strict: true, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC},
			MaxPerPartition: 1})
		p := x.promoteTwoParts(floorOwner)
		x.publish()
		partition := x.f.st.PendingPromotions[p].Promote.PartitionID
		r := submit(t, x.f, trackEnvelope(t, x.f, x.key, x.account, "", x.next, floorOwner))
		want := fmt.Sprintf("storage-integrity copy floor: partition %s of db.t holds 2 uncleaned promoted parts (max_uncleaned_parts_per_partition 1) whose cleanup waits for member checkpoints (strict_copies); retry later", partition)
		if r.Code != arbiter.AdmissionCodeCopiesBackpressure || r.Message != want {
			t.Fatalf("submit = %+v, want COPIES_BACKPRESSURE %q", r, want)
		}
	})
	t.Run("outside strict mode", func(t *testing.T) {
		x, env := strictBackpressure(t, false)
		if held := x.f.floorOnlyHeldLocked(); held != nil {
			t.Fatalf("held %v, want the pressure bypass", held)
		}
		if r := submit(t, x.f, env); r.Code != arbiter.AdmissionCodeAccepted {
			t.Fatalf("submit = %+v, want ACCEPTED", r)
		}
	})
}

// TestCopiesBackpressureLeavesStateUnchanged: the refusal changes nothing,
// so the client_seq stays unspent and the same statement is admitted later.
func TestCopiesBackpressureLeavesStateUnchanged(t *testing.T) {
	x, env := strictBackpressure(t, true)
	before := snapshotBytes(t, x.f)
	for range 2 {
		if r := submit(t, x.f, env); r.Code != arbiter.AdmissionCodeCopiesBackpressure {
			t.Fatalf("submit = %+v, want COPIES_BACKPRESSURE", r)
		}
	}
	if !bytes.Equal(before, snapshotBytes(t, x.f)) {
		t.Fatal("a COPIES_BACKPRESSURE refusal changed state")
	}
}

// TestCopiesBackpressureClearsOnceTheFloorIsMet: once a member's checkpoint
// reflects the held promotions the statement is admitted, with the client
// seq it was refused with, although the uncleaned parts still exceed the
// bound, and the cleanups are scheduled.
func TestCopiesBackpressureClearsOnceTheFloorIsMet(t *testing.T) {
	x, env := strictBackpressure(t, true)
	held := x.f.floorOnlyHeldLocked()
	if len(held) != 2 {
		t.Fatalf("held %v, want two promotions", held)
	}
	x.checkpoint(floorMemberB, floorOwner)
	if _, total := x.f.uncleanedPartsLocked("db.t"); total != 2 {
		t.Fatalf("uncleaned parts %d, want still 2", total)
	}
	if r := submit(t, x.f, env); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("submit = %+v, want ACCEPTED once the floor is met", r)
	}
	for _, p := range held {
		requireFloorCleanup(t, x, p, "")
	}
}
```

- [ ] **Step 4: Write the raise-k tests**

Create `fsm/floor_raise_test.go`:

```go
package fsm

import (
	"fmt"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// raiseKCmd is the update that sets min_safe_copies to k, edited by edit.
func raiseKCmd(x *floorFixture, k uint32, edit func(u *arbiter.ConsensusParamsUpdate)) wire.Command {
	x.t.Helper()
	return x.updateCmd(func(u *arbiter.ConsensusParamsUpdate) {
		u.Stage3.MinSafeCopies = k
		if edit != nil {
			edit(u)
		}
	})
}

// raiseKText is the refusal for raising k with domains counted on db.t
// (incarnation 1, owner floorOwner).
func raiseKText(k uint32, has int) string {
	return fmt.Sprintf("consensus update raises stage3.min_safe_copies to %d: db.t (incarnation 1, owner %d) has caught-up counting members in %d distinct failure domains other than its owner's, needs %d",
		k, floorOwner, has, k-1)
}

// TestRaisingKRequiresCaughtUpCountingMembersInKMinusOneDomains: raising k
// needs, for every Active incarnation, members in k - 1 distinct domains
// other than the owner's whose checkpoints equal the owner's latest
// published entry and exclude nothing; an excluding checkpoint does not
// count, and neither does a member the same update stops counting.
func TestRaisingKRequiresCaughtUpCountingMembersInKMinusOneDomains(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 1, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC}})
	f := x.f
	turnOnStage3Switch(t, f, x.a, Stage3SwitchNarrowedDrain)
	p := x.promote(floorOwner)
	x.publish()
	x.clean(p)
	rejectStage3Unchanged(t, f, raiseKCmd(x, 2, nil), raiseKText(2, 0))
	x.checkpoint(floorMemberC, floorOwner, arbiter.ExcludedTable{TableID: "db.t", Reason: arbiter.ExclusionDiverged})
	rejectStage3Unchanged(t, f, raiseKCmd(x, 2, nil), raiseKText(2, 0))
	x.checkpoint(floorMemberB, floorOwner)
	mustApply(t, f, raiseKCmd(x, 2, nil))
	rejectStage3Unchanged(t, f, raiseKCmd(x, 3, nil), raiseKText(3, 1))
	x.checkpoint(floorMemberC, floorOwner)
	stopCounting := func(u *arbiter.ConsensusParamsUpdate) {
		for i := range u.Verifiers {
			if u.Verifiers[i].NodeID == floorMemberC {
				u.Verifiers[i].Counts = false
			}
		}
	}
	rejectStage3Unchanged(t, f, raiseKCmd(x, 3, stopCounting), raiseKText(3, 1))
	mustApply(t, f, raiseKCmd(x, 3, nil))
	if k := f.st.Params.Stage3.MinSafeCopies; k != 3 {
		t.Fatalf("min_safe_copies %d, want 3", k)
	}
}

// TestRaisingKRefusalLeavesStateUnchanged: a refused raise commits nothing:
// the snapshot, the stage-3 record, the epoch and the registry version stay
// as they were.
func TestRaisingKRefusalLeavesStateUnchanged(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 1, MemberDomains: map[string]string{floorMemberB: floorDomainB}})
	f := x.f
	turnOnStage3Switch(t, f, x.a, Stage3SwitchNarrowedDrain)
	epoch, version := f.st.ConsensusEpoch, f.st.TableRegistry.Version
	rejectStage3Unchanged(t, f, raiseKCmd(x, 3, nil), raiseKText(3, 1))
	if f.st.ConsensusEpoch != epoch || f.st.TableRegistry.Version != version || f.st.Params.Stage3.MinSafeCopies != 1 {
		t.Fatalf("epoch %d version %d k %d after a refused raise, want %d %d 1", f.st.ConsensusEpoch, f.st.TableRegistry.Version,
			f.st.Params.Stage3.MinSafeCopies, epoch, version)
	}
}

// TestRaisingKIgnoresNonActiveIncarnations: a Pending incarnation whose new
// group holds only bootstrapping members, and the Legacy db.old, do not
// hold back a raise that every Active incarnation supports.
func TestRaisingKIgnoresNonActiveIncarnations(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 1, MemberDomains: map[string]string{floorMemberB: floorDomainB}})
	f := x.f
	turnOnStage3Switch(t, f, x.a, Stage3SwitchNarrowedDrain)
	mustApply(t, f, ownedAddCmd(t, f, "dbn", "a", ev(300, "0x300", 0), ev(301, "0x301", 0), floorOwner))
	if inc := f.st.TableRegistry.Live("dbn.a"); inc == nil || inc.Status != TableStatusPending {
		t.Fatalf("dbn.a = %+v, want Pending", inc)
	}
	if m := groupMemberByID(x.group(floorOwner, "dbn"), floorMemberB); m == nil || m.Class != MemberBootstrapping {
		t.Fatalf("G(1, dbn) member %+v, want %s bootstrapping", m, floorMemberB)
	}
	mustApply(t, f, raiseKCmd(x, 2, nil))
}
```

- [ ] **Step 5: Write the Leaving-release tests**

Create `fsm/floor_leaving_test.go`:

```go
package fsm

import (
	"fmt"
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// floorLeavingB is the Leaving history at k = 2 outside strict mode with
// members b and c: one promotion published at E1 and cleaned (b's
// checkpoint reflects it), then b removed from G(1, db), so b is Leaving
// with entry E1 and frozen E {c}, whose checkpoint is still at E0.
func floorLeavingB(t *testing.T) *floorFixture {
	t.Helper()
	x := newFloorFixture(t, floorFixtureOptions{K: 2, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC}})
	p := x.promote(floorOwner)
	x.publish()
	x.checkpoint(floorMemberB, floorOwner)
	x.clean(p)
	mustApply(t, x.f, x.rebalanceCmd(floorOwner, "db", nil, []string{floorMemberB}))
	m := x.leaving(floorOwner, "db", floorMemberB)
	if m.Released || !slices.Equal(m.FrozenE, []string{floorMemberC}) || m.FrozenEDomains != 1 || m.Entry != x.f.publishedTrackFrontierLocked(floorOwner) {
		t.Fatalf("Leaving record %+v, want unreleased with frozen E [%s] at the published entry", m, floorMemberC)
	}
	return x
}

// TestNonStrictLeavingReleaseUsesTheFrozenE: outside strict mode the release
// counts the E frozen at the RebalanceGroup: a frozen member that has since
// become lagging, and so left the current E, still releases the Leaving
// member once its checkpoint reaches the entry.
func TestNonStrictLeavingReleaseUsesTheFrozenE(t *testing.T) {
	x := floorLeavingB(t)
	f := x.f
	mustApply(t, f, x.laggingCmd(floorMemberC, floorOwner, "db"))
	if x.leaving(floorOwner, "db", floorMemberB).Released {
		t.Fatal("released before the frozen member's checkpoint reached the entry")
	}
	x.checkpoint(floorMemberC, floorOwner)
	g := x.group(floorOwner, "db")
	if m := groupMemberByID(g, floorMemberC); m.Class != MemberLagging {
		t.Fatalf("%s is %s, want still lagging", floorMemberC, m.Class)
	}
	if nodes, _ := f.copyFloorSetLocked(g, false, ""); len(nodes) != 0 {
		t.Fatalf("current E %v, want empty", nodes)
	}
	if !x.leaving(floorOwner, "db", floorMemberB).Released {
		t.Fatal("the frozen E's checkpoint did not release the Leaving member")
	}
}

// TestStrictLeavingReleaseWaitsWhenTheSoleCaughtUpMemberIsReplacedByABootstrappingOne:
// in strict mode a Leaving member is released only by the group's current E,
// so when the one caught-up member is replaced by a bootstrapping one it
// waits for the newcomer's checkpoint; outside strict mode the frozen E of
// that removal is empty and the release is immediate.
func TestStrictLeavingReleaseWaitsWhenTheSoleCaughtUpMemberIsReplacedByABootstrappingOne(t *testing.T) {
	for _, strict := range []bool{true, false} {
		t.Run(fmt.Sprintf("strict=%v", strict), func(t *testing.T) {
			x := newFloorFixture(t, floorFixtureOptions{K: 2, Strict: strict, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC}})
			f := x.f
			mustApply(t, f, x.rebalanceCmd(floorOwner, "db", nil, []string{floorMemberC}))
			if !x.leaving(floorOwner, "db", floorMemberC).Released {
				t.Fatalf("%s was not released while %s holds the entry", floorMemberC, floorMemberB)
			}
			mustApply(t, f, x.rebalanceCmd(floorOwner, "db", []string{floorMemberC}, []string{floorMemberB}))
			g := x.group(floorOwner, "db")
			if m := groupMemberByID(g, floorMemberC); m == nil || m.Class != MemberBootstrapping {
				t.Fatalf("re-added %s = %+v, want bootstrapping", floorMemberC, m)
			}
			b := x.leaving(floorOwner, "db", floorMemberB)
			if !strict {
				if len(b.FrozenE) != 0 || !b.Released {
					t.Fatalf("Leaving %s = %+v, want an empty frozen E and an immediate release", floorMemberB, b)
				}
				return
			}
			if b.Released {
				t.Fatalf("%s was released with no caught-up member left", floorMemberB)
			}
			x.checkpoint(floorMemberC, floorOwner)
			if m := groupMemberByID(g, floorMemberC); m.Class != MemberCaughtUp {
				t.Fatalf("%s is %s after its checkpoint, want caught_up", floorMemberC, m.Class)
			}
			if !x.leaving(floorOwner, "db", floorMemberB).Released {
				t.Fatal("the newcomer's checkpoint did not release the Leaving member")
			}
		})
	}
}

// TestLeavingReleaseIsRecordedOnceAndNeverWithdrawn: once recorded, a
// release survives the eviction of the member that met it and strict mode,
// under which it would no longer hold.
func TestLeavingReleaseIsRecordedOnceAndNeverWithdrawn(t *testing.T) {
	x := floorLeavingB(t)
	f := x.f
	x.checkpoint(floorMemberC, floorOwner)
	if !x.leaving(floorOwner, "db", floorMemberB).Released {
		t.Fatal("not released by the frozen E's checkpoint")
	}
	mustApply(t, f, x.evictCmd(floorMemberC))
	x.update(func(u *arbiter.ConsensusParamsUpdate) { u.Stage3.StrictCopies = true })
	m := x.leaving(floorOwner, "db", floorMemberB)
	if f.leavingReleaseHoldsLocked(x.group(floorOwner, "db"), m) {
		t.Fatal("the release condition still holds; the test needs it not to")
	}
	if !m.Released {
		t.Fatal("a recorded release was withdrawn")
	}
}

// TestLeavingReleaseIsEvaluatedAfterEveryTriggeringCommand: each command
// that can meet a release evaluates it in its own Apply, folded into that
// Apply's one registry bump. The record is cleared by hand before each
// command: a met condition that no Apply evaluated yet, which committed
// histories never hold once the floor is on.
func TestLeavingReleaseIsEvaluatedAfterEveryTriggeringCommand(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(x *floorFixture)
		command func(x *floorFixture) wire.Command
	}{
		{"MemberCheckpoint", nil, func(x *floorFixture) wire.Command { return x.checkpointCmd(floorMemberC, floorOwner) }},
		{"RebalanceGroup", nil, func(x *floorFixture) wire.Command {
			return x.rebalanceCmd(0, "db0", nil, []string{floorMemberC})
		}},
		{"MarkMemberLagging", func(x *floorFixture) {
			x.promote(0)
			x.publish()
		}, func(x *floorFixture) wire.Command { return x.laggingCmd(floorMemberC, 0, "db0") }},
		{"UpdateConsensusParams", nil, func(x *floorFixture) wire.Command {
			return x.updateCmd(func(u *arbiter.ConsensusParamsUpdate) { u.Stage3.LagLow = 3 })
		}},
		{"EvictNode", nil, func(x *floorFixture) wire.Command { return x.evictCmd(floorMemberB) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := floorLeavingB(t)
			x.checkpoint(floorMemberC, floorOwner)
			if tc.prepare != nil {
				tc.prepare(x)
			}
			cmd := tc.command(x)
			m := x.leaving(floorOwner, "db", floorMemberB)
			if !m.Released {
				t.Fatal("the fixture's release did not record")
			}
			m.Released = false
			version := x.f.st.TableRegistry.Version
			mustApply(t, x.f, cmd)
			if !x.leaving(floorOwner, "db", floorMemberB).Released {
				t.Fatalf("%s did not evaluate the release", tc.name)
			}
			if got := x.f.st.TableRegistry.Version; got != version+1 {
				t.Fatalf("registry version %d after %s, want one bump from %d", got, tc.name, version)
			}
		})
	}
}

// TestLeavingReleaseAtKOneIsImmediate: at k = 1 the release needs no
// checkpoint, so the RebalanceGroup that creates the Leaving record records
// it, in the same single registry bump.
func TestLeavingReleaseAtKOneIsImmediate(t *testing.T) {
	x := newFloorFixture(t, floorFixtureOptions{K: 1, MemberDomains: map[string]string{floorMemberB: floorDomainB, floorMemberC: floorDomainC}})
	version := x.f.st.TableRegistry.Version
	mustApply(t, x.f, x.rebalanceCmd(floorOwner, "db", nil, []string{floorMemberB}))
	if !x.leaving(floorOwner, "db", floorMemberB).Released {
		t.Fatal("not released by the RebalanceGroup at k = 1")
	}
	if got := x.f.st.TableRegistry.Version; got != version+1 {
		t.Fatalf("registry version %d, want one bump from %d", got, version)
	}
}
```

- [ ] **Step 6: Move Plan S3-E2 Task 64's three k-raising tests onto the precondition**

Each of these tests commits `min_safe_copies` 2 on a network without members, which the raise-k precondition of this task refuses. Each keeps what it pins.

In `fsm/stage3_params_test.go` (the end of `TestStage3MinSafeCopiesAndGroupSizeNeverDecrease`), replace:

```go
	turnOnStage3SwitchesThrough(t, f, a, Stage3SwitchNarrowedDrain)
	floor := stage3Update(t, f)
	floor.Stage3.MinSafeCopies = 2
	mustApply(t, f, signedConsensusUpdate(t, a, floor))
	back := stage3Update(t, f)
	back.Stage3.MinSafeCopies = 1
	rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, back), "consensus update cannot lower stage3 min_safe_copies (2 -> 1)")
	if p := f.st.Params.Stage3; p.GroupSize != 4 || p.MinSafeCopies != 2 {
		t.Fatalf("stage3 = %+v, want group_size 4 and min_safe_copies 2", p)
	}
}
```

with:

```go
	if p := f.st.Params.Stage3; p.GroupSize != 4 {
		t.Fatalf("stage3 = %+v, want group_size 4", p)
	}
	// Committing k = 2 needs caught-up member checkpoints since Plan S3-E4
	// Task 76 (raiseKRefusalLocked); the copy-floor fixture commits it.
	x := newFloorFixture(t, floorFixtureOptions{K: 2, MemberDomains: map[string]string{floorMemberB: floorDomainB}})
	back := stage3Update(t, x.f)
	back.Stage3.MinSafeCopies = 1
	rejectStage3Unchanged(t, x.f, signedConsensusUpdate(t, x.a, back), "consensus update cannot lower stage3 min_safe_copies (2 -> 1)")
	if k := x.f.st.Params.Stage3.MinSafeCopies; k != 2 {
		t.Fatalf("min_safe_copies %d, want 2", k)
	}
}
```

In `fsm/stage3_params_test.go` (the end of `TestStage3FloorAboveOneNeedsCheckpointsAndNarrowedDrain`), replace:

```go
	turnOnStage3Switch(t, f, a, Stage3SwitchNarrowedDrain)
	raised := stage3Update(t, f)
	raised.Stage3.MinSafeCopies = 2
	raised.Stage3.StrictCopies = true
	mustApply(t, f, signedConsensusUpdate(t, a, raised))
}
```

with:

```go
	turnOnStage3Switch(t, f, a, Stage3SwitchNarrowedDrain)
	// After both switches the Params-only rule admits k = 2 with
	// strict_copies. Committing k = 2 also needs the state precondition of
	// Plan S3-E4 Task 76 (raiseKRefusalLocked), which no member checkpoint
	// meets here; strict_copies alone has none and commits.
	previous, next = stage3Previous(f)
	raisedParams := cloneStage3Params(previous.Stage3)
	raisedParams.MinSafeCopies, raisedParams.StrictCopies = 2, true
	if _, err := verifyStage3Transition(previous, next, raisedParams); err != nil {
		t.Fatalf("k = 2 with strict_copies after both switches: %v", err)
	}
	raised := stage3Update(t, f)
	raised.Stage3.StrictCopies = true
	mustApply(t, f, signedConsensusUpdate(t, a, raised))
}
```

In `fsm/stage3_params_test.go` (the subtest `a history with every switch replays` of `TestStage3RulesReplayOnRestore`), replace:

```go
		raised := stage3Update(t, f)
		raised.Stage3.GroupSize, raised.Stage3.MinSafeCopies = 5, 2
		mustApply(t, f, signedConsensusUpdate(t, a, raised))
```

with:

```go
		raised := stage3Update(t, f)
		// strict_copies, not k = 2: raising k needs member checkpoints since
		// Plan S3-E4 Task 76 (raiseKRefusalLocked), and this history has no
		// member.
		raised.Stage3.GroupSize, raised.Stage3.StrictCopies = 5, true
		mustApply(t, f, signedConsensusUpdate(t, a, raised))
```

- [ ] **Step 7: Name the new event kind in the event-kind test**

In `fsm/watch_test.go` (`TestEventKindStringNamesEveryTransition`, `:249`), replace:

```go
	for k := EventAdmitted; k <= EventIndexerQuarantineChanged; k++ {
```

with:

```go
	for k := EventAdmitted; k <= EventUnderReplicated; k++ {
```

In `fsm/watch_test.go` (`:260-262`), replace:

```go
	if len(seen) != int(EventIndexerQuarantineChanged) {
		t.Errorf("named %d kinds, want %d", len(seen), EventIndexerQuarantineChanged)
	}
```

with:

```go
	if len(seen) != int(EventUnderReplicated) {
		t.Errorf("named %d kinds, want %d", len(seen), EventUnderReplicated)
	}
	if got := EventUnderReplicated.String(); got != "under_replicated" {
		t.Errorf("EventUnderReplicated is %q, want under_replicated", got)
	}
```

If Plans S3-E2 or S3-E3 appended an event kind after `EventIndexerQuarantineChanged` and moved this loop's bound to it, replace that kind instead: `EventUnderReplicated` is appended last (Step 17).

- [ ] **Step 8: Run the FSM tests and see them fail**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && GOWORK=off go test ./fsm/ -count=1 2>&1 | head -6
```

Expected: the package does not build: `# github.com/sentioxyz/arbiter/fsm [github.com/sentioxyz/arbiter/fsm.test]`, then `undefined:` lines such as `undefined: FloorVerdict`, `undefined: groupMemberByID`, `undefined: UnderReplicatedDomainsBelowK` and `x.f.floorMembersLocked undefined (type *FSM has no field or method floorMembersLocked)`, and `FAIL github.com/sentioxyz/arbiter/fsm [build failed]`.

- [ ] **Step 9: Create the copy floor**

Create `fsm/floor.go`:

```go
package fsm

import (
	"cmp"
	"fmt"
	"slices"
	"sort"

	"github.com/sentioxyz/arbiter-core"
)

// The copy floor (housegate spec 2026-10-14 §9.3, §6.4; plan S3-E4 contract
// §53). A promotion's unsafe parts are cleaned only once enough counting
// members' checkpoints reflect it, so a verified row of an Active incarnation
// stays on at least min(k, 1 + |E|) ClickHouse hosts outside the escapes.
// Apply (applyScheduleUnsafeCleanup) and the leader's WorkSet both ask
// floorLocked, so the leader never proposes a cleanup Apply refuses. Every
// rule here runs only once the checkpoints switch is committed; before it
// nothing in this file changes an answer.

// UnderReplicatedReason names why a cleanup the floor allowed, or a group,
// is under-replicated. The values are metric labels.
type UnderReplicatedReason string

const (
	UnderReplicatedNone                UnderReplicatedReason = ""
	UnderReplicatedInactiveIncarnation UnderReplicatedReason = "inactive_incarnation"
	UnderReplicatedPressurePartition   UnderReplicatedReason = "pressure_partition"
	UnderReplicatedPressureTable       UnderReplicatedReason = "pressure_table"
	UnderReplicatedDomainsBelowK       UnderReplicatedReason = "domains_below_k"
)

// UnderReplicatedReasons lists every non-empty reason, in export order.
var UnderReplicatedReasons = []UnderReplicatedReason{UnderReplicatedInactiveIncarnation, UnderReplicatedPressurePartition,
	UnderReplicatedPressureTable, UnderReplicatedDomainsBelowK}

// FloorVerdict is spec §9.3 for one acknowledged promotion.
type FloorVerdict struct {
	BaseRefusal       string                // "" when base(P) holds
	Escape            UnderReplicatedReason // inactive_incarnation, pressure_partition or pressure_table, else ""
	K                 uint32                // min_safe_copies
	Required          uint32                // min(k-1, |E|); k-1 in strict mode
	EDomains          uint32                // |E|
	ReflectingDomains []string              // sorted distinct failure domains of the reflecting members of E
	Allowed           bool                  // base && (escape || len(ReflectingDomains) >= Required)
	FloorOnly         bool                  // base holds, no escape, the count clause fails
	UnderReplicated   UnderReplicatedReason // Escape, or domains_below_k when Allowed with |E| < k-1, else ""
}

// floorActiveLocked reports whether the copy floor applies: the committed
// checkpoints switch (plan S3-E3's checkpointsOnLocked).
func (f *FSM) floorActiveLocked() bool { return f.checkpointsOnLocked() }

// floorNeed is k - 1, the reflecting domains the floor asks for at
// min_safe_copies k.
func floorNeed(k uint32) uint32 {
	if k <= 1 {
		return 0
	}
	return k - 1
}

// floorLocked is the copy floor for promotion pp (spec §9.3). base(P): pp is
// acknowledged, belongs to a track O whose latest published entry S covers
// its block and its sequence, and O is not quarantined. Then the count
// clause: the members of E (copyFloorSetLocked) whose checkpoints reflect P
// and do not exclude its table must cover Required distinct failure domains,
// two members of one domain counting once. An escape is recorded only for a
// promotion the count clause would hold, in this order: an incarnation that
// is not Active, then, outside strict mode, a partition and a table above
// their uncleaned-parts bounds. At k = 1 Required is 0, so nothing is held
// and nothing escapes: only the publication clauses bite.
func (f *FSM) floorLocked(pp *PendingPromotion) FloorVerdict {
	params := f.st.Params.Stage3
	v := FloorVerdict{K: params.MinSafeCopies}
	seq := pp.Promote.PromotionSeq
	if !pp.Acked {
		v.BaseRefusal = fmt.Sprintf("copy floor: promotion %d is not acknowledged", seq)
		return v
	}
	owner, ok := f.promotionTrackLocked(seq)
	if !ok {
		v.BaseRefusal = fmt.Sprintf("copy floor: promotion %d belongs to no track", seq)
		return v
	}
	published := f.publishedTrackFrontierLocked(owner)
	if block := f.promotionBlockLocked(pp); block > published.SafeBlockSeq {
		v.BaseRefusal = fmt.Sprintf("copy floor: promotion %d covers block %d above owner %d's published safe_block_seq %d", seq, block, owner, published.SafeBlockSeq)
		return v
	}
	if seq > published.AppliedPromotionSeq {
		v.BaseRefusal = fmt.Sprintf("copy floor: promotion %d is above owner %d's published applied_promotion_seq %d", seq, owner, published.AppliedPromotionSeq)
		return v
	}
	if f.indexerQuarantinedLocked(owner) {
		v.BaseRefusal = fmt.Sprintf("indexer %d is quarantined; no new cleanup is scheduled", owner)
		return v
	}
	table := pp.Promote.TableID
	database, _, _ := splitTableKey(table)
	need := floorNeed(params.MinSafeCopies)
	var domains map[string][]string
	if g := f.groupLocked(owner, database); g != nil {
		domains = f.floorMembersLocked(g, params.StrictCopies)
		for domain, nodes := range domains {
			for _, node := range nodes {
				if m := groupMemberByID(g, node); m != nil && f.checkpointReflectsLocked(m.Checkpoint, database, table, pp) {
					v.ReflectingDomains = append(v.ReflectingDomains, domain)
					break
				}
			}
		}
		sort.Strings(v.ReflectingDomains)
	}
	v.EDomains = uint32(len(domains))
	v.Required = need
	if !params.StrictCopies {
		v.Required = min(need, v.EDomains)
	}
	if uint32(len(v.ReflectingDomains)) >= v.Required {
		v.Allowed = true
		if v.EDomains < need {
			v.UnderReplicated = UnderReplicatedDomainsBelowK
		}
		return v
	}
	if inc := f.floorIncarnationLocked(table); inc == nil || inc.Status != TableStatusActive {
		v.Escape = UnderReplicatedInactiveIncarnation
	} else if !params.StrictCopies {
		perPartition, total := f.uncleanedPartsLocked(table)
		if perPartition[pp.Promote.PartitionID] > int(params.MaxUncleanedPartsPerPartition) {
			v.Escape = UnderReplicatedPressurePartition
		} else if total > int(params.MaxUncleanedPartsPerTable) {
			v.Escape = UnderReplicatedPressureTable
		}
	}
	if v.Escape != UnderReplicatedNone {
		v.Allowed = true
		v.UnderReplicated = v.Escape
		return v
	}
	v.FloorOnly = true
	return v
}

// floorRefusalLocked is Apply's refusal for verdict v of pp when it does not
// allow the cleanup: base(P)'s own text, else the count clause's.
func (f *FSM) floorRefusalLocked(pp *PendingPromotion, v FloorVerdict) string {
	if v.BaseRefusal != "" {
		return v.BaseRefusal
	}
	owner, _ := f.promotionTrackLocked(pp.Promote.PromotionSeq)
	return fmt.Sprintf("copy floor: promotion %d of %s needs reflecting checkpoints in %d distinct failure domains other than owner %d's, has %d",
		pp.Promote.PromotionSeq, pp.Promote.TableID, v.Required, owner, len(v.ReflectingDomains))
}

// groupMemberByID is g's current member node, nil when node is not one.
func groupMemberByID(g *DatabaseGroup, node string) *GroupMember {
	for i := range g.Members {
		if g.Members[i].NodeID == node {
			return &g.Members[i]
		}
	}
	return nil
}

// floorIncarnationLocked is the incarnation of tableID a promotion belongs
// to: manifestTableOwnerLocked's scan, the newest incarnation that is not
// Refused and ever entered a table set. It never builds the lazy live
// index, so the WorkSet may call it under the read lock.
func (f *FSM) floorIncarnationLocked(tableID string) *TableIncarnation {
	reg := f.st.TableRegistry
	if reg == nil || f.st.Params.TableRegistry == nil {
		return nil
	}
	for i := len(reg.Incarnations) - 1; i >= 0; i-- {
		inc := reg.Incarnations[i]
		if inc == nil || inc.Key() != tableID || inc.Status == TableStatusRefused {
			continue
		}
		if inc.Origin == TableOriginGenesis || inc.AddBlockSeq != 0 {
			return inc
		}
	}
	return nil
}

// floorMembersLocked is E of group g (S3-E3's copyFloorSetLocked(g, strict,
// "")) grouped by failure domain: domain -> the sorted node ids of E in it.
// len of the result is |E|.
func (f *FSM) floorMembersLocked(g *DatabaseGroup, strict bool) map[string][]string {
	nodes, _ := f.copyFloorSetLocked(g, strict, "")
	out := make(map[string][]string, len(nodes))
	for _, node := range nodes {
		entry, _ := f.verifierEntryLocked(node)
		out[entry.FailureDomain] = append(out[entry.FailureDomain], node)
	}
	return out
}

// checkpointReflectsLocked reports whether checkpoint record cp reflects
// promotion pp (spec §9.2): its entry covers pp's block and sequence, and
// its group of databaseID does not exclude tableID.
func (f *FSM) checkpointReflectsLocked(cp *MemberCheckpointRecord, databaseID, tableID string, pp *PendingPromotion) bool {
	if cp == nil {
		return false
	}
	entry := cp.Checkpoint.Entry
	if entry.SafeBlockSeq < f.promotionBlockLocked(pp) || entry.AppliedPromotionSeq < pp.Promote.PromotionSeq {
		return false
	}
	for _, group := range cp.Checkpoint.Groups {
		if group.DatabaseID != databaseID {
			continue
		}
		return !slices.ContainsFunc(group.Excluded, func(x arbiter.ExcludedTable) bool { return x.TableID == tableID })
	}
	return false
}

// uncleanedPartsLocked counts tableID's promoted unsafe parts whose cleanup
// is not acknowledged yet: per partition and in all.
func (f *FSM) uncleanedPartsLocked(tableID string) (perPartition map[string]int, total int) {
	perPartition = map[string]int{}
	for tp, parts := range f.st.PromotedUnsafe {
		if tp.TableID != tableID {
			continue
		}
		for _, pending := range parts {
			if pending {
				perPartition[tp.PartitionID]++
				total++
			}
		}
	}
	return perPartition, total
}

// promotionUncleanedLocked reports whether some candidate part of pp is
// still a promoted unsafe part, the WorkSet's rule for listing a cleanup.
func (f *FSM) promotionUncleanedLocked(pp *PendingPromotion) bool {
	reg := f.st.PromotedUnsafe[arbiter.TablePartition{TableID: pp.Promote.TableID, PartitionID: pp.Promote.PartitionID}]
	return slices.ContainsFunc(pp.Promote.CandidateParts, func(part arbiter.PartRef) bool { return reg[part.PartRowLtHash] })
}

// floorOnlyHeldLocked lists, ascending, the promotions whose cleanup only
// the floor holds: acknowledged, no cleanup pending, a part still uncleaned
// and FloorOnly. These are WorkSet.FloorHeld; empty while the floor is off.
func (f *FSM) floorOnlyHeldLocked() []uint64 {
	if !f.floorActiveLocked() {
		return nil
	}
	seqs := make([]uint64, 0, len(f.st.PendingPromotions))
	for seq, pp := range f.st.PendingPromotions {
		if pp != nil && pp.Acked && f.st.PendingCleanups[seq] == nil {
			seqs = append(seqs, seq)
		}
	}
	slices.Sort(seqs)
	var held []uint64
	for _, seq := range seqs {
		pp := f.st.PendingPromotions[seq]
		if f.promotionUncleanedLocked(pp) && f.floorLocked(pp).FloorOnly {
			held = append(held, seq)
		}
	}
	return held
}

// copiesBackpressureLocked is strict mode's admission refusal (spec §9.3,
// plan S3-E1 K59): a statement for table T is refused, retryably and with
// state unchanged, while T holds a partition above
// max_uncleaned_parts_per_partition or more than max_uncleaned_parts_per_table
// uncleaned promoted parts in all and some acknowledged promotion of T is
// held by the floor alone. Partitions are checked first, in ascending id.
func (f *FSM) copiesBackpressureLocked(tableID string) (SubmitResult, bool) {
	if !f.floorActiveLocked() || !f.st.Params.Stage3.StrictCopies {
		return SubmitResult{}, false
	}
	params := f.st.Params.Stage3
	perPartition, total := f.uncleanedPartsLocked(tableID)
	partitions := make([]string, 0, len(perPartition))
	for p, n := range perPartition {
		if n > int(params.MaxUncleanedPartsPerPartition) {
			partitions = append(partitions, p)
		}
	}
	if len(partitions) == 0 && total <= int(params.MaxUncleanedPartsPerTable) {
		return SubmitResult{}, false
	}
	held := false
	for _, seq := range f.floorOnlyHeldLocked() {
		if f.st.PendingPromotions[seq].Promote.TableID == tableID {
			held = true
			break
		}
	}
	if !held {
		return SubmitResult{}, false
	}
	if len(partitions) != 0 {
		sort.Strings(partitions)
		p := partitions[0]
		return SubmitResult{Code: arbiter.AdmissionCodeCopiesBackpressure, Message: fmt.Sprintf(
			"storage-integrity copy floor: partition %s of %s holds %d uncleaned promoted parts (max_uncleaned_parts_per_partition %d) whose cleanup waits for member checkpoints (strict_copies); retry later",
			p, tableID, perPartition[p], params.MaxUncleanedPartsPerPartition)}, true
	}
	return SubmitResult{Code: arbiter.AdmissionCodeCopiesBackpressure, Message: fmt.Sprintf(
		"storage-integrity copy floor: %s holds %d uncleaned promoted parts (max_uncleaned_parts_per_table %d) whose cleanup waits for member checkpoints (strict_copies); retry later",
		tableID, total, params.MaxUncleanedPartsPerTable)}, true
}

// raiseKRefusalLocked is the precondition for raising min_safe_copies to k
// (spec §9.3), in the Apply-only half of applyUpdateConsensusParams: every
// Active incarnation, in registry order, needs listed, non-evicted,
// replicate and counting members of its group, other than its owner's
// failure domain, in k - 1 distinct failure domains, whose checkpoints equal
// the owner's latest published entry and exclude nothing. Listing, flags and
// domains are next's, the parameters the raised k would run under. Restore
// never runs it: it reads state that legitimately changes afterwards.
func (f *FSM) raiseKRefusalLocked(next Params) string {
	current, raised := f.st.Params.Stage3, next.Stage3
	if current == nil || raised == nil || raised.MinSafeCopies <= current.MinSafeCopies {
		return ""
	}
	k := raised.MinSafeCopies
	need := floorNeed(k)
	reg := f.st.TableRegistry
	if reg == nil {
		return ""
	}
	for _, inc := range reg.Incarnations {
		if inc == nil || inc.Status != TableStatusActive {
			continue
		}
		owner := f.incarnationOwnerLocked(inc)
		var domains map[string]bool
		if g := f.groupLocked(owner, inc.DatabaseID); g != nil {
			domains = f.caughtUpDomainsLocked(g, next)
		}
		if uint32(len(domains)) < need {
			return fmt.Sprintf("consensus update raises stage3.min_safe_copies to %d: %s (incarnation %d, owner %d) has caught-up counting members in %d distinct failure domains other than its owner's, needs %d",
				k, inc.Key(), inc.Seq, owner, len(domains), need)
		}
	}
	return ""
}

// caughtUpDomainsLocked is the set of failure domains, other than the
// owner's, of g's members that params lists with replicate and counts, that
// are not evicted, and whose checkpoints equal the owner's latest published
// entry and exclude nothing: the count of the raise-k precondition and of
// arbiter_safe_copies_min.
func (f *FSM) caughtUpDomainsLocked(g *DatabaseGroup, params Params) map[string]bool {
	published := f.publishedTrackFrontierLocked(g.Owner)
	ownerDomain := indexerFailureDomain(params, g.Owner)
	domains := map[string]bool{}
	for _, m := range g.Members {
		entry, listed := findVerifierEntry(params.Verifiers, m.NodeID)
		if !listed || !entry.Replicate || !entry.Counts || entry.FailureDomain == ownerDomain || f.nodeEvictedLocked(m.NodeID) {
			continue
		}
		if m.Checkpoint == nil || entryCompare(m.Checkpoint.Checkpoint.Entry, published) != 0 || !checkpointExcludesNothing(m.Checkpoint, g.DatabaseID) {
			continue
		}
		domains[entry.FailureDomain] = true
	}
	return domains
}

// nodeEvictedLocked reports whether node is a registered, Evicted node.
func (f *FSM) nodeEvictedLocked(node string) bool {
	n := f.st.Nodes[node]
	return n != nil && n.Status == NodeEvicted
}

// checkpointExcludesNothing reports whether cp's group of databaseID lists
// no excluded table. A record without that group excludes it all.
func checkpointExcludesNothing(cp *MemberCheckpointRecord, databaseID string) bool {
	for _, group := range cp.Checkpoint.Groups {
		if group.DatabaseID == databaseID {
			return len(group.Excluded) == 0
		}
	}
	return false
}

// leavingReleaseHoldsLocked is the release condition of Leaving member m of
// group g (spec §6.4). Outside strict mode: the members of m's frozen E that
// are still current members of g, listed and non-evicted, with checkpoints at
// or past m's entry that exclude none of g's tables, cover
// min(k - 1, frozen |E|) distinct current failure domains other than the
// owner's. In strict mode: the members of g counted as E is now (lagging
// included; m is never a current member), with such checkpoints, cover
// k - 1 distinct failure domains. k and strict_copies are the committed ones.
func (f *FSM) leavingReleaseHoldsLocked(g *DatabaseGroup, m *LeavingMember) bool {
	params := f.st.Params.Stage3
	need := floorNeed(params.MinSafeCopies)
	var candidates []string
	required := need
	if params.StrictCopies {
		candidates, _ = f.copyFloorSetLocked(g, true, m.Member.NodeID)
	} else {
		required = min(need, m.FrozenEDomains)
		ownerDomain := indexerFailureDomain(f.st.Params, g.Owner)
		for _, node := range m.FrozenE {
			entry, listed := f.verifierEntryLocked(node)
			if node == m.Member.NodeID || !listed || f.nodeEvictedLocked(node) || entry.FailureDomain == ownerDomain {
				continue
			}
			candidates = append(candidates, node)
		}
	}
	if required == 0 {
		return true
	}
	domains := map[string]bool{}
	for _, node := range candidates {
		member := groupMemberByID(g, node)
		if member == nil || member.Checkpoint == nil {
			continue
		}
		entry := member.Checkpoint.Checkpoint.Entry
		if entry.SafeBlockSeq < m.Entry.SafeBlockSeq || entry.AppliedPromotionSeq < m.Entry.AppliedPromotionSeq || !checkpointExcludesNothing(member.Checkpoint, g.DatabaseID) {
			continue
		}
		listed, _ := f.verifierEntryLocked(node)
		domains[listed.FailureDomain] = true
	}
	return uint32(len(domains)) >= required
}

// releaseLeavingMembersLocked records every Leaving member whose release
// condition holds for the first time (LeavingMember.Released, never
// cleared), in group and node order, and reports whether it recorded one.
// It runs at the end of every command that can satisfy a release; the
// caller folds a recorded release into its Apply's one registry bump.
func (f *FSM) releaseLeavingMembersLocked() bool {
	if f.st.Stage3 == nil || !f.floorActiveLocked() {
		return false
	}
	released := false
	for _, g := range f.st.Stage3.Groups {
		for i := range g.Leaving {
			m := &g.Leaving[i]
			if !m.Released && f.leavingReleaseHoldsLocked(g, m) {
				m.Released = true
				released = true
			}
		}
	}
	return released
}

// registryVersionLocked is the committed table registry version, 0 without
// a registry.
func (f *FSM) registryVersionLocked() uint64 {
	if f.st.TableRegistry == nil {
		return 0
	}
	return f.st.TableRegistry.Version
}

// releaseLeavingAtEndLocked evaluates the Leaving releases at the end of an
// Apply whose registry version was version before its mutations, and bumps
// the version for a recorded release unless the Apply bumped it already
// (one bump per Apply).
func (f *FSM) releaseLeavingAtEndLocked(version uint64) {
	if f.releaseLeavingMembersLocked() && f.registryVersionLocked() == version {
		f.registryChangedLocked(f.st.TableRegistry)
	}
}

// floorMetricsLocked derives the replicated floor gauges (plan S3-E4
// contract §53, K78): the promotions only the floor holds, the groups whose
// |E| is below k - 1, and per owner the fewest safe copies over its groups:
// 1 + the distinct failure domains other than the owner's among listed,
// non-evicted, replicate and counting members whose checkpoints equal the
// owner's latest published entry and exclude nothing. All are zero or empty
// while the floor is off.
func (f *FSM) floorMetricsLocked() (held, underReplicatedGroups uint64, safeCopies []MetricSafeCopies) {
	if f.st.Stage3 == nil || !f.floorActiveLocked() {
		return 0, 0, nil
	}
	params := f.st.Params.Stage3
	held = uint64(len(f.floorOnlyHeldLocked()))
	need := floorNeed(params.MinSafeCopies)
	minimum := map[uint64]uint64{}
	for _, g := range f.st.Stage3.Groups {
		if _, domains := f.copyFloorSetLocked(g, params.StrictCopies, ""); domains < need {
			underReplicatedGroups++
		}
		copies := 1 + uint64(len(f.caughtUpDomainsLocked(g, f.st.Params)))
		if current, ok := minimum[g.Owner]; !ok || copies < current {
			minimum[g.Owner] = copies
		}
	}
	for owner, copies := range minimum {
		safeCopies = append(safeCopies, MetricSafeCopies{IndexerID: owner, Copies: copies})
	}
	slices.SortFunc(safeCopies, func(a, b MetricSafeCopies) int { return cmp.Compare(a.IndexerID, b.IndexerID) })
	return held, underReplicatedGroups, safeCopies
}
```

- [ ] **Step 10: Gate applyScheduleUnsafeCleanup**

In `fsm/apply.go` (`applyScheduleUnsafeCleanup`, right after the quarantine check, `:1005`), replace:

```go
	if pending, dup := f.st.PendingCleanups[c.Cleanup.PromotionSeq]; dup {
```

with:

```go
	// The copy floor (housegate spec 2026-10-14 §9.3, plan S3-E4 contract
	// §53): once the checkpoints switch is on, a new promotion cleanup is
	// scheduled only when floorLocked allows it, the predicate the WorkSet
	// lists cleanups by. A cleanup already pending, scheduled before the
	// switch included, is not checked again.
	var underReplicated UnderReplicatedReason
	if _, pending := f.st.PendingCleanups[c.Cleanup.PromotionSeq]; f.floorActiveLocked() && !pending {
		pp := f.st.PendingPromotions[c.Cleanup.PromotionSeq]
		if pp == nil {
			return Rejected{Reason: fmt.Sprintf("copy floor: promotion %d is not acknowledged", c.Cleanup.PromotionSeq)}
		}
		verdict := f.floorLocked(pp)
		if !verdict.Allowed {
			return Rejected{Reason: f.floorRefusalLocked(pp, verdict)}
		}
		underReplicated = verdict.UnderReplicated
	}
	if pending, dup := f.st.PendingCleanups[c.Cleanup.PromotionSeq]; dup {
```

In `fsm/apply.go` (the end of `applyScheduleUnsafeCleanup`, `:1018-1019`), replace:

```go
	f.emit(Event{Kind: EventCleanupScheduled, PromotionSeq: cl.PromotionSeq})
	return Applied{}
```

with:

```go
	f.emit(Event{Kind: EventCleanupScheduled, PromotionSeq: cl.PromotionSeq})
	if underReplicated != UnderReplicatedNone {
		f.emit(Event{Kind: EventUnderReplicated, PromotionSeq: cl.PromotionSeq})
	}
	return Applied{}
```

- [ ] **Step 11: Add the WorkSet fields**

In `fsm/reads_work_types.go` (`:54-57`), replace:

```go
type CleanupWork struct {
	PromotionSeq uint64
	Cleanup      arbiter.UnsafeCleanup
}
```

with:

```go
type CleanupWork struct {
	PromotionSeq uint64
	Cleanup      arbiter.UnsafeCleanup
	// UnderReplicated is set on a PublishedUncleaned row that the copy floor
	// allows through an escape or with |E| below k - 1 (plan S3-E4 contract
	// §53); "" otherwise, and always while the checkpoints switch is off.
	UnderReplicated UnderReplicatedReason
}
```

In `fsm/reads_work_types.go` (the end of `WorkSet`, after Plan S3-E2 Task 70's `Unmarkable`), replace:

```go
	Unmarkable []uint64
}
```

with:

```go
	Unmarkable []uint64
	// FloorHeld lists, ascending, the acknowledged promotions with uncleaned
	// parts whose cleanup only the copy floor holds (FloorVerdict.FloorOnly,
	// plan S3-E4 contract §53); the leader times them (K78). Nil while the
	// checkpoints switch is off.
	FloorHeld []uint64
}
```

If Plan S3-E3 appended fields after `Unmarkable`, append `FloorHeld` after the last of them instead (hunk rule): it is the struct's last field.

- [ ] **Step 12: Gate the WorkSet's cleanup rows**

In `fsm/reads_work.go` (`WorkSet`, `:324-325`), replace:

```go
	ws.NeedManifest = manifestDebt
	for _, seq := range pendingSeqs {
```

with:

```go
	ws.NeedManifest = manifestDebt
	floor := f.floorActiveLocked()
	for _, seq := range pendingSeqs {
```

In `fsm/reads_work.go` (`:355-366`), replace:

```go
		if len(parts) == 0 {
			continue
		}
		ws.PublishedUncleaned = append(ws.PublishedUncleaned, CleanupWork{
			PromotionSeq: seq,
			Cleanup: arbiter.UnsafeCleanup{
				TableID:      pp.Promote.TableID,
				PartitionID:  pp.Promote.PartitionID,
				PromotionSeq: seq,
				Parts:        parts,
			},
		})
```

with:

```go
		if len(parts) == 0 {
			continue
		}
		// The copy floor (spec 2026-10-14 §9.3): the same predicate as
		// applyScheduleUnsafeCleanup, so the leader never proposes a cleanup
		// Apply refuses. A promotion only the floor holds is FloorHeld.
		var underReplicated UnderReplicatedReason
		if floor {
			verdict := f.floorLocked(pp)
			if !verdict.Allowed {
				if verdict.FloorOnly {
					ws.FloorHeld = append(ws.FloorHeld, seq)
				}
				continue
			}
			underReplicated = verdict.UnderReplicated
		}
		ws.PublishedUncleaned = append(ws.PublishedUncleaned, CleanupWork{
			PromotionSeq: seq,
			Cleanup: arbiter.UnsafeCleanup{
				TableID:      pp.Promote.TableID,
				PartitionID:  pp.Promote.PartitionID,
				PromotionSeq: seq,
				Parts:        parts,
			},
			UnderReplicated: underReplicated,
		})
```

- [ ] **Step 13: Add strict backpressure to admission**

In `fsm/admission.go` (`applySubmitStatement`, right after Plan S3-E2 Task 70's `VERIFIERS_UNAVAILABLE` block, `:148`), replace:

```go
	// Lane budget, before the only state mutation (spec §5.5 step 4; the
```

with:

```go
	// 5a''. Copy backpressure (housegate spec 2026-10-14 §9.3, plan S3-E4
	// contract §53): in strict mode the pressure bypass becomes a retryable
	// COPIES_BACKPRESSURE for a table whose cleanup the floor holds above an
	// uncleaned-parts bound, before the lane budget and the only state
	// mutation, so the client_seq stays unspent until the floor is met.
	if refusal, refused := f.copiesBackpressureLocked(env.TargetTableID); refused {
		return refusal
	}
	// Lane budget, before the only state mutation (spec §5.5 step 4; the
```

- [ ] **Step 14: Add the raise-k precondition and the release to the consensus update**

In `fsm/consensus_updates.go` (`applyUpdateConsensusParams`, `:83-87`), replace:

```go
	if tracksActivation {
		if reason := f.tracksActivationRefusalLocked(); reason != "" {
			return Rejected{Reason: reason}
		}
	}
```

with:

```go
	if tracksActivation {
		if reason := f.tracksActivationRefusalLocked(); reason != "" {
			return Rejected{Reason: reason}
		}
	}
	// Raising stage3.min_safe_copies (spec 2026-10-14 §9.3, plan S3-E4
	// contract §53) needs every Active incarnation's group to hold k - 1
	// caught-up counting failure domains first; Apply-only, before any
	// mutation, never on restore.
	if reason := f.raiseKRefusalLocked(next); reason != "" {
		return Rejected{Reason: reason}
	}
```

In `fsm/consensus_updates.go` (`:88`, where Plan S3-E2 put `previousStage3` right before it), replace:

```go
	wasEnabled := f.st.Params.TableRegistry != nil
```

with:

```go
	registryVersion := f.registryVersionLocked()
	wasEnabled := f.st.Params.TableRegistry != nil
```

In `fsm/consensus_updates.go` (`:149-150`, after Plan S3-E2's `recordStage3SwitchesLocked` block), replace:

```go
	f.st.ConsensusEpoch++
	f.st.ConsensusUpdates = append(f.st.ConsensusUpdates, ConsensusParamsChange{Update: update, AuthorityJWS: c.AuthorityJWS})
```

with:

```go
	// A new k, strict_copies, failure domain or listing, or the checkpoints
	// switch-on itself, may meet a Leaving member's release (spec §6.4); a
	// recorded release folds into this Apply's one registry bump.
	f.releaseLeavingAtEndLocked(registryVersion)
	f.st.ConsensusEpoch++
	f.st.ConsensusUpdates = append(f.st.ConsensusUpdates, ConsensusParamsChange{Update: update, AuthorityJWS: c.AuthorityJWS})
```

- [ ] **Step 15: Evaluate the release after an eviction**

In `fsm/apply.go` (`applyEvictNode`, `:138-139`), replace:

```go
	n.Status = NodeEvicted
	n.Reason = c.Reason
```

with:

```go
	n.Status = NodeEvicted
	n.Reason = c.Reason
	registryVersion := f.registryVersionLocked()
```

In `fsm/apply.go` (the end of `applyEvictNode`, `:147-149`), replace:

```go
	f.emit(Event{Kind: EventMembershipChanged})
	return Applied{}
}

// blockStatements returns the block's statements in statement_seq order,
```

with:

```go
	// The eviction re-evaluates the Leaving releases (spec §6.4, plan S3-E4
	// contract §53) after the purge completion, in the same single bump.
	f.releaseLeavingAtEndLocked(registryVersion)
	f.emit(Event{Kind: EventMembershipChanged})
	return Applied{}
}

// blockStatements returns the block's statements in statement_seq order,
```

Plan S3-E3 Task 71's purge-completion block between the two insertions stays as it is.

- [ ] **Step 16: Evaluate the release in groupsChangedLocked**

In `fsm/stage3_groups.go` (Plan S3-E3 Task 71's `groupsChangedLocked`; its other statements stay), replace:

```go
func (f *FSM) groupsChangedLocked() {
```

with:

```go
func (f *FSM) groupsChangedLocked() {
	// A checkpoint, a rebalance or a lagging mark may meet a Leaving
	// member's release (plan S3-E4 contract §53); the bump below covers it.
	f.releaseLeavingMembersLocked()
```

- [ ] **Step 17: Add EventUnderReplicated**

In `fsm/watch.go` (`:24-29`), replace:

```go
	EventIndexerQuarantineChanged
)
```

with:

```go
	EventIndexerQuarantineChanged
	// EventUnderReplicated: an applied cleanup the copy floor allowed
	// through an escape or with |E| below k - 1 (housegate spec 2026-10-14
	// §9.3); PromotionSeq names the promotion. A hint like every event; the
	// replicated gauges and the leader's counter are the record (K78).
	EventUnderReplicated
)
```

In `fsm/watch.go` (`String`, `:64-65`), replace:

```go
	case EventIndexerQuarantineChanged:
		return "indexer_quarantine_changed"
```

with:

```go
	case EventIndexerQuarantineChanged:
		return "indexer_quarantine_changed"
	case EventUnderReplicated:
		return "under_replicated"
```

- [ ] **Step 18: Add the floor gauges to MetricsSnapshot**

In `fsm/reads_metrics.go` (the end of `MetricsSnapshot`, `:49-50`), replace:

```go
	Tracks []MetricTrack
}
```

with:

```go
	Tracks []MetricTrack

	// The copy floor (plan S3-E4 contract §53, K78), zero or empty until the
	// checkpoints switch: FloorHeldCleanups counts the promotions whose
	// cleanup only the floor holds, UnderReplicatedGroups the groups whose
	// |E| is below k - 1, and SafeCopiesMin has one row per owner with a
	// group, sorted by indexer id.
	FloorHeldCleanups     uint64
	UnderReplicatedGroups uint64
	SafeCopiesMin         []MetricSafeCopies
}

// MetricSafeCopies is one owner's fewest safe copies over its groups: 1 +
// the distinct failure domains other than its own whose listed, non-evicted,
// counting replicate members have checkpoints at its latest published entry
// that exclude nothing.
type MetricSafeCopies struct{ IndexerID, Copies uint64 }
```

In `fsm/reads_metrics.go` (the end of `MetricsSnapshot()`, `:167-168`), replace:

```go
	return m
}
```

with:

```go
	m.FloorHeldCleanups, m.UnderReplicatedGroups, m.SafeCopiesMin = f.floorMetricsLocked()
	return m
}
```

- [ ] **Step 19: Run the FSM tests and see them pass**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && GOWORK=off go test ./fsm/ -run '^(TestFloorSwitchOffIsByteIdentical|TestFloorIsDormantAtKOne|TestFloorApplyAndWorkSetAgreeOnEveryCleanup|TestFloorPublicationClausesRefuseAnUnpublishedPromotion|TestFloorHoldsCleanupUntilKMinusOneDomainsReflect|TestFloorCountsTwoMembersInOneDomainOnce|TestFloorParamsUpdateThatCollidesDomainsOnlyLowersTheCount|TestFloorExcludedTableDoesNotCount|TestFloorPurgeExemptionForANonActiveIncarnation|TestFloorPressureBypassPerPartitionAndPerTable|TestFloorEscapesRaiseUnderReplicated|TestFloorDomainsBelowKRaisesUnderReplicated|TestRejectedPartCleanupIsNeverGatedByTheFloor|TestCleanupScheduledBeforeTheCheckpointsSwitchIsNotRechecked|TestFloorKeepsTheQuarantineRefusal|TestStrictModeRequiresKMinusOneDomains|TestStrictModeKeepsLaggingMembersInE|TestStrictModeTurnsPressureIntoCopiesBackpressure|TestCopiesBackpressureLeavesStateUnchanged|TestCopiesBackpressureClearsOnceTheFloorIsMet|TestRaisingKRequiresCaughtUpCountingMembersInKMinusOneDomains|TestRaisingKRefusalLeavesStateUnchanged|TestRaisingKIgnoresNonActiveIncarnations|TestNonStrictLeavingReleaseUsesTheFrozenE|TestStrictLeavingReleaseWaitsWhenTheSoleCaughtUpMemberIsReplacedByABootstrappingOne|TestLeavingReleaseIsRecordedOnceAndNeverWithdrawn|TestLeavingReleaseIsEvaluatedAfterEveryTriggeringCommand|TestLeavingReleaseAtKOneIsImmediate|TestFloorMetricsSnapshot|TestEventKindStringNamesEveryTransition|TestStage3FloorAboveOneNeedsCheckpointsAndNarrowedDrain|TestStage3RulesReplayOnRestore|TestStage3MinSafeCopiesAndGroupSizeNeverDecrease)$' -race -count=1 -v 2>&1 | grep -c '^--- PASS'
cd "$W" && GOWORK=off go test ./fsm/ -race -count=1 && GOWORK=off go vet ./fsm/ && echo vet-clean
cd "$W" && grep -rn 'time\.Now' fsm/ ; grep -n 'arbiter-proto' fsm/floor.go ; echo red-lines-checked
```

Expected: `33` (the 29 tests of contract §53, the event-kind test and Task 64's three tests); `ok  	github.com/sentioxyz/arbiter/fsm`, every golden, determinism, Plan S3-E2 and Plan S3-E3 test included; `vet-clean`; only `red-lines-checked` (no clock and no arbiter-proto in the fsm library).

- [ ] **Step 20: Run gazelle and commit the FSM half**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && test -z "$(gofmt -l $(git ls-files '*.go') fsm/floor.go fsm/floor_test.go fsm/floor_strict_test.go fsm/floor_raise_test.go fsm/floor_leaving_test.go)" && echo gofmt-clean
cd "$W" && bazel run //:gazelle && git diff -- fsm/BUILD.bazel
cd "$W" && git add fsm/floor.go fsm/floor_test.go fsm/floor_strict_test.go fsm/floor_raise_test.go fsm/floor_leaving_test.go fsm/apply.go fsm/reads_work_types.go fsm/reads_work.go fsm/admission.go fsm/consensus_updates.go fsm/stage3_groups.go fsm/watch.go fsm/reads_metrics.go fsm/stage3_params_test.go fsm/watch_test.go fsm/BUILD.bazel
cd "$W" && git status --short
cd "$W" && git commit -m "feat(stage3): the copy floor, strict backpressure, the Leaving release and the raise-k precondition (Task 76)

From the checkpoints switch on, applyScheduleUnsafeCleanup and the
WorkSet judge every new promotion cleanup with one predicate,
floorLocked: base(P) (acknowledged, covered by the owner's published
entry, the owner not quarantined), then reflecting checkpoints in
min(k-1, |E|) distinct failure domains, k-1 in strict mode, with the
purge exemption and the uncleaned-parts bypass as escapes that raise
under_replicated. Strict mode turns the bypass into the retryable
COPIES_BACKPRESSURE at admission. Raising min_safe_copies needs
caught-up counting members in k-1 domains for every Active
incarnation, and a Leaving member's release is evaluated after every
command that can meet it and recorded once. MetricsSnapshot carries the
floor gauges (housegate spec 2026-10-14 §9.3, §6.4; plan S3-E4 contract
§53, K78).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
cd "$W" && git status --porcelain && git log --oneline -2
```

Expected: `gofmt-clean`; gazelle adds `"floor.go",` to `go_library(name = "fsm")` (before `"fsm.go",`) and `"floor_leaving_test.go",`, `"floor_raise_test.go",`, `"floor_strict_test.go",`, `"floor_test.go",` to `go_test(name = "fsm_test")` (before `"fsm_test.go",`), with no dependency change; the status before the commit lists the sixteen staged paths, `A` for the five new files and `M` for the rest; after it the status is empty and the log shows this commit on Plan S3-E3 Task 75's last commit.

- [ ] **Step 21: Write the failing metrics test**

Create `metrics/floor_test.go`:

```go
package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sentioxyz/arbiter/fsm"
)

// TestFloorMetrics: the replicated floor gauges follow MetricsSnapshot, the
// two counts from the start and arbiter_safe_copies_min only once it lists
// an owner; the leader-local counter has every reason series from the start
// and the age gauge follows the loop's report (plan S3-E4 contract §53, K78).
func TestFloorMetrics(t *testing.T) {
	before := New(laneSnapshot{fsm.MetricsSnapshot{}})
	if n, err := testutil.GatherAndCount(before.registry, "arbiter_safe_copies_min"); err != nil || n != 0 {
		t.Fatalf("arbiter_safe_copies_min before the switch: %d series, %v", n, err)
	}
	counts := `
# HELP arbiter_floor_held_cleanups Acknowledged promotions whose unsafe-part cleanup only the copy floor holds in this replica's FSM: published, not quarantined, and waiting for member checkpoints. 0 before the stage-3 checkpoints switch.
# TYPE arbiter_floor_held_cleanups gauge
arbiter_floor_held_cleanups 0
# HELP arbiter_under_replicated_groups Database groups whose counting members (E) span fewer than min_safe_copies - 1 failure domains other than their owner's. 0 before the stage-3 checkpoints switch.
# TYPE arbiter_under_replicated_groups gauge
arbiter_under_replicated_groups 0
`
	if err := testutil.GatherAndCompare(before.registry, strings.NewReader(counts), "arbiter_floor_held_cleanups", "arbiter_under_replicated_groups"); err != nil {
		t.Fatal(err)
	}
	after := New(laneSnapshot{fsm.MetricsSnapshot{FloorHeldCleanups: 2, UnderReplicatedGroups: 1,
		SafeCopiesMin: []fsm.MetricSafeCopies{{IndexerID: 0, Copies: 3}, {IndexerID: 1, Copies: 1}}}})
	want := `
# HELP arbiter_floor_held_cleanups Acknowledged promotions whose unsafe-part cleanup only the copy floor holds in this replica's FSM: published, not quarantined, and waiting for member checkpoints. 0 before the stage-3 checkpoints switch.
# TYPE arbiter_floor_held_cleanups gauge
arbiter_floor_held_cleanups 2
# HELP arbiter_safe_copies_min Per owner, the fewest safe copies over its database groups: 1 plus the distinct failure domains other than the owner's of listed, non-evicted, replicate and counting members whose checkpoints equal its latest published entry and exclude nothing. Declared domains; they are distinct Kubernetes nodes only through anti-affinity. Only after the stage-3 checkpoints switch; indexer_id is an enrolled SI indexer.
# TYPE arbiter_safe_copies_min gauge
arbiter_safe_copies_min{indexer_id="0"} 3
arbiter_safe_copies_min{indexer_id="1"} 1
# HELP arbiter_under_replicated_groups Database groups whose counting members (E) span fewer than min_safe_copies - 1 failure domains other than their owner's. 0 before the stage-3 checkpoints switch.
# TYPE arbiter_under_replicated_groups gauge
arbiter_under_replicated_groups 1
`
	if err := testutil.GatherAndCompare(after.registry, strings.NewReader(want), "arbiter_floor_held_cleanups", "arbiter_safe_copies_min", "arbiter_under_replicated_groups"); err != nil {
		t.Fatal(err)
	}
	m := New(laneSnapshot{fsm.MetricsSnapshot{}})
	m.UnderReplicatedCleanup("pressure_table")
	m.UnderReplicatedCleanup("pressure_table")
	m.FloorHeldCleanups(3, 90*time.Second)
	local := `
# HELP arbiter_floor_held_cleanup_age_seconds Seconds since this leader's loop first saw the oldest promotion whose cleanup only the copy floor holds; 0 when none is held and on a follower. It restarts at a leadership change, so it can understate after a failover and never overstates.
# TYPE arbiter_floor_held_cleanup_age_seconds gauge
arbiter_floor_held_cleanup_age_seconds 90
# HELP arbiter_under_replicated_total Cleanups this leader proposed that Apply took although the copy floor was not met, by reason: inactive_incarnation (the purge exemption), pressure_partition and pressure_table (the uncleaned-parts bypass), domains_below_k (|E| below min_safe_copies - 1). Process-local; only a leader moves it.
# TYPE arbiter_under_replicated_total counter
arbiter_under_replicated_total{reason="domains_below_k"} 0
arbiter_under_replicated_total{reason="inactive_incarnation"} 0
arbiter_under_replicated_total{reason="pressure_partition"} 0
arbiter_under_replicated_total{reason="pressure_table"} 2
`
	if err := testutil.GatherAndCompare(m.registry, strings.NewReader(local), "arbiter_floor_held_cleanup_age_seconds", "arbiter_under_replicated_total"); err != nil {
		t.Fatal(err)
	}
	m.FloorHeldCleanups(0, 0)
	if got := testutil.ToFloat64(m.floorHeldCleanupAge); got != 0 {
		t.Fatalf("age %v after a report of none, want 0", got)
	}
}
```

- [ ] **Step 22: Run it and see it fail**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && GOWORK=off go test ./metrics/ -run '^TestFloorMetrics$' -count=1 2>&1 | head -4
```

Expected: the build fails with `m.UnderReplicatedCleanup undefined (type *Metrics has no field or method UnderReplicatedCleanup)`, `m.FloorHeldCleanups undefined …` and `FAIL github.com/sentioxyz/arbiter/metrics [build failed]` (the gauge half compiles: `MetricsSnapshot` already has the fields).

- [ ] **Step 23: Create the leader-local floor series**

Create `metrics/floor.go`:

```go
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sentioxyz/arbiter/fsm"
)

// registerFloorMetrics builds and registers the copy floor's leader-local
// series (housegate spec 2026-10-14 §9.3; plan S3-E4 contract §53, K78).
// Every reason series of the counter exists from the start, so an alert on
// its increase never reads an absent series as no escape.
func (m *Metrics) registerFloorMetrics() {
	m.underReplicatedCleanups = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "arbiter_under_replicated_total",
		Help: "Cleanups this leader proposed that Apply took although the copy floor was not met, by reason: inactive_incarnation (the purge exemption), pressure_partition and pressure_table (the uncleaned-parts bypass), domains_below_k (|E| below min_safe_copies - 1). Process-local; only a leader moves it.",
	}, []string{"reason"})
	for _, reason := range fsm.UnderReplicatedReasons {
		m.underReplicatedCleanups.WithLabelValues(string(reason))
	}
	m.floorHeldCleanupAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "arbiter_floor_held_cleanup_age_seconds",
		Help: "Seconds since this leader's loop first saw the oldest promotion whose cleanup only the copy floor holds; 0 when none is held and on a follower. It restarts at a leadership change, so it can understate after a failover and never overstates.",
	})
	m.registry.MustRegister(m.underReplicatedCleanups, m.floorHeldCleanupAge)
}

// UnderReplicatedCleanup counts one cleanup Apply took under reason
// (orchestrator.FloorMetrics).
func (m *Metrics) UnderReplicatedCleanup(reason string) {
	m.underReplicatedCleanups.WithLabelValues(reason).Inc()
}

// FloorHeldCleanups sets the held-cleanup age to oldest
// (orchestrator.FloorMetrics). The count is the leader's view of
// arbiter_floor_held_cleanups, which every replica exports from committed
// state, so it is not exported a second time.
func (m *Metrics) FloorHeldCleanups(_ int, oldest time.Duration) {
	m.floorHeldCleanupAge.Set(oldest.Seconds())
}
```

- [ ] **Step 24: Export the floor series from metrics.go**

In `metrics/metrics.go` (the end of `Metrics`, `:64-65`; if Plan S3-E3 appended fields after `snapshotFailures`, append after the last of them), replace:

```go
	snapshotFailures         *prometheus.CounterVec
}
```

with:

```go
	snapshotFailures         *prometheus.CounterVec

	// The copy floor's leader-local signals (orchestrator.FloorMetrics,
	// metrics/floor.go).
	underReplicatedCleanups *prometheus.CounterVec
	floorHeldCleanupAge     prometheus.Gauge
}
```

In `metrics/metrics.go` (`New`, `:150`), replace:

```go
	m.registry.MustRegister(m.holdActive, m.holdsEnded, m.heldPromotions)
```

with:

```go
	m.registry.MustRegister(m.holdActive, m.holdsEnded, m.heldPromotions)
	m.registerFloorMetrics()
```

In `metrics/metrics.go` (the end of `collector`, after its last field), replace:

```go
}

func newCollector(source snapshotReader) *collector {
```

with:

```go
	// The copy floor's replicated gauges (plan S3-E4 contract §53, K78).
	floorHeldCleanups     *prometheus.Desc
	underReplicatedGroups *prometheus.Desc
	safeCopiesMin         *prometheus.Desc
}

func newCollector(source snapshotReader) *collector {
```

In `metrics/metrics.go` (the end of `newCollector`'s literal, after its last entry), replace:

```go
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
```

with:

```go
		// The copy floor (housegate spec 2026-10-14 §9.3).
		floorHeldCleanups:     prometheus.NewDesc("arbiter_floor_held_cleanups", "Acknowledged promotions whose unsafe-part cleanup only the copy floor holds in this replica's FSM: published, not quarantined, and waiting for member checkpoints. 0 before the stage-3 checkpoints switch.", nil, nil),
		underReplicatedGroups: prometheus.NewDesc("arbiter_under_replicated_groups", "Database groups whose counting members (E) span fewer than min_safe_copies - 1 failure domains other than their owner's. 0 before the stage-3 checkpoints switch.", nil, nil),
		safeCopiesMin:         prometheus.NewDesc("arbiter_safe_copies_min", "Per owner, the fewest safe copies over its database groups: 1 plus the distinct failure domains other than the owner's of listed, non-evicted, replicate and counting members whose checkpoints equal its latest published entry and exclude nothing. Declared domains; they are distinct Kubernetes nodes only through anti-affinity. Only after the stage-3 checkpoints switch; indexer_id is an enrolled SI indexer.", []string{"indexer_id"}, nil),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
```

In `metrics/metrics.go` (the end of `Describe`, `:265-267`), replace:

```go
		ch <- desc
	}
}
```

with:

```go
		ch <- desc
	}
	ch <- c.floorHeldCleanups
	ch <- c.underReplicatedGroups
	ch <- c.safeCopiesMin
}
```

In `metrics/metrics.go` (the end of `Collect`, `:338-343`), replace:

```go
			for _, component := range s.Components {
				emit(c.stateComponentBytes, component.Bytes, component.Component)
			}
		}
	}
}
```

with:

```go
			for _, component := range s.Components {
				emit(c.stateComponentBytes, component.Bytes, component.Component)
			}
		}
	}
	// The copy floor (K78): the two counts are exported from the start, 0
	// until the checkpoints switch; SafeCopiesMin is empty until then, so no
	// arbiter_safe_copies_min series exists before it.
	emit(c.floorHeldCleanups, m.FloorHeldCleanups)
	emit(c.underReplicatedGroups, m.UnderReplicatedGroups)
	for _, row := range m.SafeCopiesMin {
		emit(c.safeCopiesMin, row.Copies, strconv.FormatUint(row.IndexerID, 10))
	}
}
```

- [ ] **Step 25: Run the metrics tests and see them pass**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && GOWORK=off go test ./metrics/ -count=1 -race && GOWORK=off go test ./metrics/ -run '^TestFloorMetrics$' -count=1 -v 2>&1 | grep -E '^--- '
```

Expected: `ok  	github.com/sentioxyz/arbiter/metrics` (Plan S3-A's state-size, hold and track tests and Plan S3-E3's group tests among them) and `--- PASS: TestFloorMetrics`.

- [ ] **Step 26: Write the failing orchestrator tests**

Create `orchestrator/floor_metrics_test.go`:

```go
package orchestrator

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter/fsm"
)

// floorHeldCall is one FloorMetrics.FloorHeldCleanups call.
type floorHeldCall struct {
	count  int
	oldest time.Duration
}

// floorMetricsRecorder records the loop's copy-floor signals.
type floorMetricsRecorder struct {
	mu      sync.Mutex
	reasons []string
	held    []floorHeldCall
}

func (r *floorMetricsRecorder) UnderReplicatedCleanup(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reasons = append(r.reasons, reason)
}

func (r *floorMetricsRecorder) FloorHeldCleanups(count int, oldest time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.held = append(r.held, floorHeldCall{count, oldest})
}

func (r *floorMetricsRecorder) calls() ([]string, []floorHeldCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reasons), slices.Clone(r.held)
}

// TestLoopCountsUnderReplicatedCleanupsOnlyWhenApplied: the loop counts a
// PublishedUncleaned row's under_replicated reason only once its proposal
// returned no error (plan S3-E4 contract §53, K78): a row Apply refuses and a
// row without a reason count nothing.
func TestLoopCountsUnderReplicatedCleanupsOnlyWhenApplied(t *testing.T) {
	params, signer := authorityParamsO(t)
	f, node, _, _, _, o, _ := newHarness(t, params, signer)
	ctx := context.Background()
	seedGenesisWriterAndVerifiers(t, node)
	driveToQuorum(t, node, f)
	ws, err := f.WorkSet()
	if err != nil || len(ws.UnanchoredVerified) != 1 {
		t.Fatalf("unanchored work: %+v, %v", ws.UnanchoredVerified, err)
	}
	if err := o.anchorBlock(ctx, ws.UnanchoredVerified[0]); err != nil {
		t.Fatal(err)
	}
	if ws, err = f.WorkSet(); err != nil || len(ws.PromotablePending) != 1 {
		t.Fatalf("promotable work: %+v, %v", ws.PromotablePending, err)
	}
	if err := o.issuePromotion(ctx, ws.PromotablePending[0]); err != nil {
		t.Fatal(err)
	}
	ackPromotionViaNode(t, node, f, 1)
	if err := o.publishManifest(ctx); err != nil {
		t.Fatal(err)
	}
	if ws, err = f.WorkSet(); err != nil || len(ws.PublishedUncleaned) != 1 {
		t.Fatalf("cleanup work: %+v, %v", ws.PublishedUncleaned, err)
	}
	floor := &floorMetricsRecorder{}
	o.d.Floor = floor
	refused := ws.PublishedUncleaned[0]
	refused.Cleanup.Parts = slices.Clone(refused.Cleanup.Parts)
	refused.Cleanup.Parts[0].PartitionID = "elsewhere"
	refused.UnderReplicated = fsm.UnderReplicatedDomainsBelowK
	taken := ws.PublishedUncleaned[0]
	taken.UnderReplicated = fsm.UnderReplicatedPressureTable
	again := ws.PublishedUncleaned[0]
	rows := fsm.WorkSet{PublishedUncleaned: []fsm.CleanupWork{refused, taken, again}}
	if err := o.handlePromotionRows(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if reasons, _ := floor.calls(); !slices.Equal(reasons, []string{"pressure_table"}) {
		t.Fatalf("counted %v, want [pressure_table]", reasons)
	}
	if ws, err = f.WorkSet(); err != nil || len(ws.ScheduledUnackedCleanups) != 1 {
		t.Fatalf("scheduled cleanups %+v, %v; want the taken one", ws.ScheduledUnackedCleanups, err)
	}
}

// TestFloorHeldCleanupAgeIsLeaderLocalAndRestartsOnLeadershipChange: a
// held promotion's age counts from the first pass of this leader term that
// saw it held; a leader run starts with no ages and reports none when it
// ends, so a follower's gauge is 0 and a new term restarts every age (K78).
func TestFloorHeldCleanupAgeIsLeaderLocalAndRestartsOnLeadershipChange(t *testing.T) {
	_, _, _, _, _, o, _ := newHarness(t, testParamsO())
	floor := &floorMetricsRecorder{}
	o.d.Floor = floor
	now := time.Unix(1000, 0)
	o.now = func() time.Time { return now }
	o.observeFloorHeld([]uint64{5})
	now = now.Add(30 * time.Second)
	o.observeFloorHeld([]uint64{5, 7})
	now = now.Add(10 * time.Second)
	o.observeFloorHeld([]uint64{7})
	if _, held := floor.calls(); !slices.Equal(held, []floorHeldCall{{1, 0}, {2, 30 * time.Second}, {1, 10 * time.Second}}) {
		t.Fatalf("reported %+v", held)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, earlier := floor.calls()
	var atFirstPass []floorHeldCall
	var first sync.Once
	o.afterRescan = func() {
		first.Do(func() {
			_, atFirstPass = floor.calls()
			// The term sees promotion 7 held, as a pass over a held cleanup
			// does; the run then ends, as a leadership loss ends it.
			now = now.Add(5 * time.Second)
			o.observeFloorHeld([]uint64{7})
			cancel()
		})
	}
	_ = o.Run(ctx)
	if got := atFirstPass[len(earlier):]; !slices.Equal(got, []floorHeldCall{{0, 0}, {0, 0}}) {
		t.Fatalf("a leader run reported %+v up to its first pass, want none before the pass and the pass's none", got)
	}
	_, held := floor.calls()
	if len(o.floorHeldSince) != 0 || held[len(held)-1] != (floorHeldCall{0, 0}) {
		t.Fatalf("after a leader run: since %v, last report %+v; want no ages and 0", o.floorHeldSince, held[len(held)-1])
	}
	now = now.Add(time.Minute)
	o.observeFloorHeld([]uint64{7})
	if _, held := floor.calls(); held[len(held)-1] != (floorHeldCall{1, 0}) {
		t.Fatalf("after a new term: %+v, want the age restarted at 0", held[len(held)-1])
	}
}
```

- [ ] **Step 27: Run them and see them fail**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && GOWORK=off go test ./orchestrator/ -run 'TestLoopCountsUnderReplicatedCleanupsOnlyWhenApplied|TestFloorHeldCleanupAgeIsLeaderLocalAndRestartsOnLeadershipChange' -count=1 2>&1 | head -4
```

Expected: the build fails with `o.d.Floor undefined (type Deps has no field or method Floor)` and `o.observeFloorHeld undefined (type *Loop has no field or method observeFloorHeld)`, then `FAIL github.com/sentioxyz/arbiter/orchestrator [build failed]`.

- [ ] **Step 28: Add FloorMetrics and Deps.Floor**

In `orchestrator/deps.go` (`:72-74`), replace:

```go
type IssuanceGate interface {
	BeginPromotionIssuance(fsm.PromotionWork) (release func(), held bool, reason string)
}
```

with:

```go
type IssuanceGate interface {
	BeginPromotionIssuance(fsm.PromotionWork) (release func(), held bool, reason string)
}

// FloorMetrics receives the leader-local copy-floor signals (housegate spec
// 2026-10-14 §9.3; plan S3-E4 contract §53, K78): UnderReplicatedCleanup
// once per cleanup this leader proposed that Apply took under an
// under_replicated reason, and FloorHeldCleanups once per pass with the
// promotions only the floor holds and the age of the oldest, measured from
// the first pass of this leader term that saw it held. *metrics.Metrics
// implements it.
type FloorMetrics interface {
	UnderReplicatedCleanup(reason string)
	FloorHeldCleanups(count int, oldest time.Duration)
}
```

In `orchestrator/deps.go` (the end of `Deps`, `:93-94`), replace:

```go
	Issuance IssuanceGate
}
```

with:

```go
	Issuance IssuanceGate
	// Floor receives the copy floor's leader-local signals; nil disables
	// them.
	Floor FloorMetrics
}
```

- [ ] **Step 29: Create the held-cleanup timing**

Create `orchestrator/floor_metrics.go`:

```go
package orchestrator

import "time"

// resetFloorHeld forgets every held cleanup's start time and reports none,
// at the start and the end of a leader term (K78): ages restart at a
// leadership change, and a node that stops leading reports 0.
func (o *Loop) resetFloorHeld() {
	o.floorHeldSince = make(map[uint64]time.Time)
	if o.d.Floor != nil {
		o.d.Floor.FloorHeldCleanups(0, 0)
	}
}

// observeFloorHeld records one pass's WorkSet.FloorHeld (plan S3-E4
// contract §53): a promotion's age counts from the first pass of this term
// that saw it held, a promotion that left the list is forgotten, and the
// count and the oldest age are reported. The age can understate after a
// failover and never overstates.
func (o *Loop) observeFloorHeld(held []uint64) {
	if o.d.Floor == nil {
		return
	}
	now := o.now()
	since := make(map[uint64]time.Time, len(held))
	var oldest time.Duration
	for _, seq := range held {
		first, seen := o.floorHeldSince[seq]
		if !seen {
			first = now
		}
		since[seq] = first
		oldest = max(oldest, now.Sub(first))
	}
	o.floorHeldSince = since
	o.d.Floor.FloorHeldCleanups(len(held), oldest)
}
```

- [ ] **Step 30: Reset the ages per leader term and expose the receiver**

In `orchestrator/loop.go` (`Loop`, `:43`), replace:

```go
	custodyStage map[uint64]custodyStage
```

with:

```go
	// floorHeldSince is when this leader term first saw each promotion whose
	// cleanup only the copy floor holds (K78); Run resets it with the seal
	// times, and observeFloorHeld replaces it on every pass.
	floorHeldSince map[uint64]time.Time

	custodyStage map[uint64]custodyStage
```

In `orchestrator/loop.go` (`:67`), replace:

```go
func (o *Loop) IssuanceGate() IssuanceGate { return o.d.Issuance }
```

with:

```go
func (o *Loop) IssuanceGate() IssuanceGate { return o.d.Issuance }

// FloorMetrics is the receiver of the loop's copy-floor signals;
// cmd/arbiter's wiring test reads it.
func (o *Loop) FloorMetrics() FloorMetrics { return o.d.Floor }
```

In `orchestrator/loop.go` (`Run`, `:84-85`), replace:

```go
	o.resetCustodyStages()
	o.resetSealTimes()
```

with:

```go
	o.resetCustodyStages()
	o.resetSealTimes()
	// A held cleanup's age is this term's (K78): it restarts here, and a node
	// that stops leading reports none.
	o.resetFloorHeld()
	defer o.resetFloorHeld()
```

- [ ] **Step 31: Split the cleanup proposal from its wrapper**

In `orchestrator/cleanup.go` (`:13-42`), replace:

```go
func (o *Loop) scheduleCleanup(ctx context.Context, cw fsm.CleanupWork) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !o.authoritySignerReady("cleanup issuance") {
		return nil
	}
	if err := o.d.Node.VerifyLeader(); err != nil {
		return fmt.Errorf("verify leader before cleanup sign: %w", err)
	}
	signingContext, ready := o.authoritySigningContext("cleanup signing")
	if !ready {
		return nil
	}
	jws, err := o.d.Signer.SignCleanupWithContext(cw.Cleanup, signingContext)
	if err != nil {
		o.d.Logger.Warn("sign cleanup failed", "promotion_seq", cw.PromotionSeq, "rejected_block_seq", cw.Cleanup.RejectedBlockSeq, "err", err)
		return nil
	}
	if _, err := o.propose(ctx, wire.Command{ScheduleUnsafeCleanup: &wire.ScheduleUnsafeCleanup{Cleanup: cw.Cleanup, AuthorityJWS: jws}}); err != nil {
		if errors.Is(err, ErrRejected) {
			return nil
		}
		if cw.Cleanup.RejectedBlockSeq != 0 {
			return fmt.Errorf("schedule unsafe cleanup rejected_block_seq %d: %w", cw.Cleanup.RejectedBlockSeq, err)
		}
		return fmt.Errorf("schedule unsafe cleanup seq %d: %w", cw.PromotionSeq, err)
	}
	return o.streamCleanupSigned(ctx, cw.Cleanup, jws)
}
```

with:

```go
func (o *Loop) scheduleCleanup(ctx context.Context, cw fsm.CleanupWork) error {
	_, err := o.proposeCleanup(ctx, cw)
	return err
}

// proposeCleanup signs cw's cleanup, proposes it and streams it to its
// source. applied reports whether the proposal returned no error, which is
// what the copy floor's under_replicated counter counts (K78); a streaming
// error after it is still returned.
func (o *Loop) proposeCleanup(ctx context.Context, cw fsm.CleanupWork) (applied bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !o.authoritySignerReady("cleanup issuance") {
		return false, nil
	}
	if err := o.d.Node.VerifyLeader(); err != nil {
		return false, fmt.Errorf("verify leader before cleanup sign: %w", err)
	}
	signingContext, ready := o.authoritySigningContext("cleanup signing")
	if !ready {
		return false, nil
	}
	jws, err := o.d.Signer.SignCleanupWithContext(cw.Cleanup, signingContext)
	if err != nil {
		o.d.Logger.Warn("sign cleanup failed", "promotion_seq", cw.PromotionSeq, "rejected_block_seq", cw.Cleanup.RejectedBlockSeq, "err", err)
		return false, nil
	}
	if _, err := o.propose(ctx, wire.Command{ScheduleUnsafeCleanup: &wire.ScheduleUnsafeCleanup{Cleanup: cw.Cleanup, AuthorityJWS: jws}}); err != nil {
		if errors.Is(err, ErrRejected) {
			return false, nil
		}
		if cw.Cleanup.RejectedBlockSeq != 0 {
			return false, fmt.Errorf("schedule unsafe cleanup rejected_block_seq %d: %w", cw.Cleanup.RejectedBlockSeq, err)
		}
		return false, fmt.Errorf("schedule unsafe cleanup seq %d: %w", cw.PromotionSeq, err)
	}
	return true, o.streamCleanupSigned(ctx, cw.Cleanup, jws)
}
```

- [ ] **Step 32: Count and time the floor in handlePromotionRows**

In `orchestrator/promotion.go` (`:15`), replace:

```go
func (o *Loop) handlePromotionRows(ctx context.Context, ws fsm.WorkSet) error {
```

with:

```go
func (o *Loop) handlePromotionRows(ctx context.Context, ws fsm.WorkSet) error {
	o.observeFloorHeld(ws.FloorHeld)
```

In `orchestrator/promotion.go` (`:39-43`; the rejected-part loop below keeps `scheduleCleanup`), replace:

```go
	for _, cw := range ws.PublishedUncleaned {
		if err := o.scheduleCleanup(ctx, cw); err != nil {
			return err
		}
	}
```

with:

```go
	// A cleanup the copy floor let through an escape, or with |E| below
	// k - 1, counts as under-replicated only once Apply took it (K78).
	for _, cw := range ws.PublishedUncleaned {
		applied, err := o.proposeCleanup(ctx, cw)
		if applied && cw.UnderReplicated != fsm.UnderReplicatedNone && o.d.Floor != nil {
			o.d.Floor.UnderReplicatedCleanup(string(cw.UnderReplicated))
		}
		if err != nil {
			return err
		}
	}
```

- [ ] **Step 33: Run the orchestrator tests and see them pass**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && GOWORK=off go test ./orchestrator/ -race -count=1 && GOWORK=off go test ./orchestrator/ -run 'TestLoopCountsUnderReplicatedCleanupsOnlyWhenApplied|TestFloorHeldCleanupAgeIsLeaderLocalAndRestartsOnLeadershipChange' -race -count=50 2>&1 | tail -1
```

Expected: `ok  	github.com/sentioxyz/arbiter/orchestrator` twice (the signer, rejected-cleanup, metrics and Plan S3-E3 member-lagging tests among the first run; fifty repetitions of the two new tests in the second).

- [ ] **Step 34: Write the failing wiring test**

Create `cmd/arbiter/floor_metrics_wiring_test.go`:

```go
package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/sentioxyz/arbiter/metrics"
	"github.com/sentioxyz/arbiter/orchestrator"
)

// TestNewServerAndLoopWiresTheFloorMetrics: the orchestrator reports the
// copy floor's leader-local signals to the runtime's metrics (plan S3-E4
// contract §53), and to nothing when metrics are off.
func TestNewServerAndLoopWiresTheFloorMetrics(t *testing.T) {
	cfg := genesisTestConfig()
	applyTestDefaults(&cfg)
	state, _, err := newFSM(cfg)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(state)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, loop, _, err := newServerAndLoop(serviceRuntime{cfg: cfg, fsm: state, logger: logger, floorMetrics: m})
	if err != nil {
		t.Fatal(err)
	}
	if loop.FloorMetrics() != orchestrator.FloorMetrics(m) {
		t.Fatal("the orchestrator does not report the copy floor to the metrics")
	}
	_, off, _, err := newServerAndLoop(serviceRuntime{cfg: cfg, fsm: state, logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	if off.FloorMetrics() != nil {
		t.Fatal("the orchestrator reports the copy floor with metrics off")
	}
}
```

- [ ] **Step 35: Run it and see it fail**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && GOWORK=off go test ./cmd/arbiter/ -run '^TestNewServerAndLoopWiresTheFloorMetrics$' -count=1 2>&1 | head -3
```

Expected: the build fails with `unknown field floorMetrics in struct literal of type serviceRuntime` and `FAIL github.com/sentioxyz/arbiter/cmd/arbiter [build failed]`.

- [ ] **Step 36: Hand the metrics to the runtime in cmd/arbiter/app.go**

In `cmd/arbiter/app.go` (`:42`), replace:

```go
var _ raftnode.SnapshotObserver = (*metrics.Metrics)(nil)
```

with:

```go
var _ raftnode.SnapshotObserver = (*metrics.Metrics)(nil)

// *metrics.Metrics receives the orchestrator's copy-floor signals (plan
// S3-E4 contract §53); metrics does not import orchestrator.
var _ orchestrator.FloorMetrics = (*metrics.Metrics)(nil)
```

In `cmd/arbiter/app.go` (`run`, `:82`), replace:

```go
	var holdMetrics server.ConsensusHoldMetrics
```

with:

```go
	var holdMetrics server.ConsensusHoldMetrics
	var floorMetrics orchestrator.FloorMetrics
```

In `cmd/arbiter/app.go` (`:89`), replace:

```go
		promotionMetrics, registryMetrics, streamMetrics, evidenceMetrics, admissionMetrics, holdMetrics = m, m, m, m, m, m
```

with:

```go
		promotionMetrics, registryMetrics, streamMetrics, evidenceMetrics, admissionMetrics, holdMetrics = m, m, m, m, m, m
		floorMetrics = m
```

In `cmd/arbiter/app.go` (the `serviceRuntime` literal, `:107`), replace:

```go
		holdMetrics:      holdMetrics,
```

with:

```go
		holdMetrics:      holdMetrics,
		floorMetrics:     floorMetrics,
```

- [ ] **Step 37: Map the runtime's metrics to Deps.Floor in cmd/arbiter/services.go**

In `cmd/arbiter/services.go` (`serviceRuntime`, `:43-44`), replace:

```go
	// holdMetrics counts consensus holds; nil when metrics are off.
	holdMetrics server.ConsensusHoldMetrics
```

with:

```go
	// holdMetrics counts consensus holds; nil when metrics are off.
	holdMetrics server.ConsensusHoldMetrics
	// floorMetrics receives the copy floor's leader-local signals; nil when
	// metrics are off.
	floorMetrics orchestrator.FloorMetrics
```

In `cmd/arbiter/services.go` (`newServerAndLoop`, `:162`), replace:

```go
		Issuance:  srv.ConsensusHolds(),
```

with:

```go
		Issuance:  srv.ConsensusHolds(),
		Floor:     rt.floorMetrics,
```

- [ ] **Step 38: Run the wiring test and see it pass**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && GOWORK=off go test ./cmd/arbiter/ -count=1 && GOWORK=off go test ./cmd/arbiter/ -run '^TestNewServerAndLoopWiresTheFloorMetrics$' -count=1 -v 2>&1 | grep -E '^--- '
```

Expected: `ok  	github.com/sentioxyz/arbiter/cmd/arbiter` and `--- PASS: TestNewServerAndLoopWiresTheFloorMetrics`.

- [ ] **Step 39: Document the copy floor in the README**

In `README.md` (the end of Plan S3-E2 Task 65's section `## Multi-source storage integrity (stage-3 activation)`, right before `:396`), replace:

```markdown
## Artifact disposition admission
```

with:

```markdown
**Copy floor** (housegate spec 2026-10-14 §9.3; Plan S3-E4 Task 76). From the `checkpoints` switch on, a promotion's unsafe parts are cleaned only once enough member copies hold it. Apply (`ScheduleUnsafeCleanup`) and the leader's WorkSet judge every new promotion cleanup with one predicate, so the leader never proposes a cleanup Apply refuses. First base(P): the promotion is acknowledged, the owner's entry in the latest published manifest covers its block and its sequence, and the owner is not quarantined (refusals `copy floor: promotion N is not acknowledged`, `copy floor: promotion N covers block B above owner O's published safe_block_seq S`, `copy floor: promotion N is above owner O's published applied_promotion_seq A`, `copy floor: promotion N belongs to no track`, and the quarantine text as before). Then the count: E is the group's listed, non-evicted, replicate and counting members whose class is `caught_up` or `behind` and whose failure domain differs from the owner's, |E| the number of distinct domains among them, and the members of E whose checkpoints reflect the promotion (entry at or past its block and sequence) and do not exclude its table must cover min(k − 1, |E|) distinct domains, k = `stage3.min_safe_copies` (refusal `copy floor: promotion N of TABLE needs reflecting checkpoints in R distinct failure domains other than owner O's, has H`). Two members of one domain count once; an update that puts two members, or a member and its owner, in one domain is accepted and only lowers |E|. A cleanup the count holds still goes through, recorded as under-replicated, when its incarnation is not Active (the purge exemption) or, outside strict mode, when its partition holds more than `max_uncleaned_parts_per_partition` or its table more than `max_uncleaned_parts_per_table` uncleaned promoted parts; one allowed while |E| < k − 1 is recorded as `domains_below_k`. At k = 1 the count always holds, so only the publication clauses bite. Rejected-part cleanups are never gated, and a cleanup scheduled before the switch is not checked again.

**Strict mode, raising k and Leaving members.** With `strict_copies` the count needs k − 1 domains whatever |E| is, `lagging` members stay in E, and the pressure bypass becomes admission backpressure: while a table holds more uncleaned promoted parts than a bound and the floor alone holds one of its promotions, its statements get the retryable `COPIES_BACKPRESSURE` (code 13) with state unchanged, until a checkpoint meets the floor. An update that raises `min_safe_copies` to k is refused (`consensus update raises stage3.min_safe_copies to K: TABLE (incarnation I, owner O) has caught-up counting members in N distinct failure domains other than its owner's, needs K-1`) unless every Active incarnation's group has counting members in k − 1 domains other than the owner's whose checkpoints equal the owner's latest published entry and exclude nothing, judged on the listing and domains the update itself sets. A member a `RebalanceGroup` removed is released (it may drop its copies) the first time enough other copies hold the entry that was latest at its removal: outside strict mode, members of the E frozen then in min(k − 1, frozen |E|) distinct domains; in strict mode, the group's current E in k − 1 domains, never the leaving member itself. The release is evaluated after every checkpoint, rebalance, lagging mark, consensus update and eviction, is recorded once (`released` in the registry view) and is never withdrawn; at k = 1 the removal releases at once.

## Artifact disposition admission
```

In `README.md` (the end of the consensus-hold paragraph of `## Metrics`, `:435`), replace:

```markdown
A hold that stays active well past the drain an operator expects is the signal to read `consensus hold-status` and quarantine a stuck track.
```

with:

```markdown
A hold that stays active well past the drain an operator expects is the signal to read `consensus hold-status` and quarantine a stuck track.

The copy floor (Plan S3-E4 Task 76, K78) adds three replicated gauges, derived from committed state by every replica: `arbiter_floor_held_cleanups` (promotions whose cleanup only the floor holds) and `arbiter_under_replicated_groups` (groups whose |E| is below k − 1), both 0 before the `checkpoints` switch, and `arbiter_safe_copies_min{indexer_id}` (per owner, the fewest safe copies over its groups: 1 plus the distinct failure domains other than its own whose counting members' checkpoints equal its latest published entry and exclude nothing; absent before the switch). Declared domains are distinct Kubernetes nodes only through anti-affinity. Two series are leader-local: `arbiter_under_replicated_total{reason}` (`inactive_incarnation`, `pressure_partition`, `pressure_table`, `domains_below_k`; every reason exported from the start) counts the cleanups this leader proposed that Apply took under that reason, and `arbiter_floor_held_cleanup_age_seconds` is the age of the oldest promotion only the floor holds, measured from the first pass of this leader term that saw it held: 0 on a follower, it restarts at a leadership change, so it can understate after a failover and never overstates. An applied escape also emits the hint-only event `under_replicated`.
```

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && grep -c '^\*\*Copy floor\*\* (housegate spec 2026-10-14' README.md && grep -c '^The copy floor (Plan S3-E4 Task 76, K78) adds' README.md`
Expected: `1` and `1`.

- [ ] **Step 40: Full verification**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && test -z "$(gofmt -l $(git ls-files '*.go') $(git ls-files -o --exclude-standard '*.go'))" && echo gofmt-clean
cd "$W" && GOWORK=off go vet ./... && echo vet-clean
cd "$W" && GOWORK=off go test ./... -race -count=1 2>&1 | grep -E '^(--- FAIL|FAIL|panic)' || echo no-failures
cd "$W" && bazel run //:gazelle && git status --short
cd "$W" && bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors
cd "$W" && git diff --exit-code HEAD~1 -- fsm/testdata accumulator/testdata fsm/l3_track_golden_test.go && echo goldens-unchanged
cd "$W" && bazel query 'deps(//fsm:fsm, 1)' | grep com_github_sentioxyz_arbiter_proto; grep -rn 'time\.Now' fsm/; echo red-lines-checked
```

Expected: `gofmt-clean`; `vet-clean`; `no-failures`; gazelle adds `"floor.go",` to `go_library(name = "metrics")` (before `"metrics.go",`), `"floor_test.go",` to `go_test(name = "metrics_test")` (after `"consensus_hold_test.go",`), `"floor_metrics.go",` to `go_library(name = "orchestrator")` (after `"dispatch_ledger.go",`), `"floor_metrics_test.go",` to `go_test(name = "orchestrator_test")` (after `"evm_anchor_test.go",`) and `"floor_metrics_wiring_test.go",` to `go_test(name = "arbiter_test")` (after `"consensus_restart_test.go",`), no dependency, and the status lists exactly ` M README.md`, ` M cmd/arbiter/BUILD.bazel`, ` M cmd/arbiter/app.go`, ` M cmd/arbiter/services.go`, ` M metrics/BUILD.bazel`, ` M metrics/metrics.go`, ` M orchestrator/BUILD.bazel`, ` M orchestrator/cleanup.go`, ` M orchestrator/deps.go`, ` M orchestrator/loop.go`, ` M orchestrator/promotion.go`, `?? cmd/arbiter/floor_metrics_wiring_test.go`, `?? metrics/floor.go`, `?? metrics/floor_test.go`, `?? orchestrator/floor_metrics.go` and `?? orchestrator/floor_metrics_test.go`; Bazel builds and every test target passes under race (CI's command, `.github/workflows/ci.yml`); `goldens-unchanged` (no golden or vector file differs from Plan S3-E3 Task 75's last commit, `HEAD~1`); only `red-lines-checked`.

- [ ] **Step 41: Commit the metrics, orchestrator and wiring half**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && git add README.md cmd/arbiter/BUILD.bazel cmd/arbiter/app.go cmd/arbiter/services.go cmd/arbiter/floor_metrics_wiring_test.go metrics/BUILD.bazel metrics/metrics.go metrics/floor.go metrics/floor_test.go orchestrator/BUILD.bazel orchestrator/cleanup.go orchestrator/deps.go orchestrator/loop.go orchestrator/promotion.go orchestrator/floor_metrics.go orchestrator/floor_metrics_test.go
cd "$W" && git status --short
cd "$W" && git commit -m "feat(stage3): copy-floor metrics, the under_replicated counter and the held-cleanup age (Task 76)

The collector exports arbiter_floor_held_cleanups,
arbiter_under_replicated_groups and arbiter_safe_copies_min{indexer_id}
from MetricsSnapshot on every replica. The leader counts
arbiter_under_replicated_total{reason} for each cleanup Apply took
under an under_replicated reason (proposeCleanup reports whether the
proposal returned no error) and times arbiter_floor_held_cleanup_age_seconds
over WorkSet.FloorHeld from the first pass of its term that saw each
promotion held; a new term restarts the ages and a follower reports 0.
cmd/arbiter wires *metrics.Metrics to orchestrator.Deps.Floor, and the
README documents the floor (housegate spec 2026-10-14 §9.3, §16 P6;
plan S3-E4 contract §53, K78).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
cd "$W" && git status --porcelain && git log --oneline -3
```

Expected: the status before the commit lists the sixteen staged paths, `A` for the five new files and `M` for the rest; after it the status is empty and the log shows this task's two commits on Plan S3-E3 Task 75's last commit, the newest subject ending in `(Task 76)` (Task 77's base check reads it). Nothing is pushed: Task 78 pushes the branch and opens its PR (contract §48, §55). Task 77 starts from here.

---

## Task 77: arbiter — narrowed drain gate, drain views for a draft, holds that judge their draft and the arbiter-admin default hold, stage-3 switch 9 (S3-E4)

Spec §10.1 ("After the `narrowed_drain` switch, the drain gate refuses an update only for in-flight work of the tracks it touches"; an update touches track T when it changes T's `si_indexers` entry, and changes to `verifiers` and `stage3` touch no track; a promoted unsafe part that only the copy floor holds is not in-flight work), §10.2 (leader drain mode records "each affected track's" frontier), §10.4 step 9 and Review Focus 3, as fixed by contract §54 and clarification K79 (the sunset of Plan S3-A's K6). Today `applyUpdateConsensusParams` runs the stage-2 gate `drainGateRefusalLocked` (`fsm/quarantine.go:203-256`) for every update (`fsm/consensus_updates.go:74-78`): any acknowledged promotion that still has a promoted unsafe part refuses every update, and from the `checkpoints` switch on (Task 76) a part that the copy floor holds stays promoted until a member checkpoints, so with k = 2 no update could ever pass. Plan S3-A's `DrainView` and both hold modes read that one gate, and `arbiter-admin consensus update` defaults to `--hold drain`. This task adds `fsm/drain_narrowed.go`: `TouchedTracks`, the narrowed gate (per touched track, in the stage-2 order, floor-held parts excluded), `floorHeldUnsafePartsLocked` and `DrainViewFor(draft)`, the answer Apply's drain gate gives that draft, while `DrainView()` keeps the stage-2 answer. Apply calls `drainRefusalForUpdateLocked(next.SIIndexers)`, which reads the committed switch, so the update that turns `narrowed_drain` on is still judged by the stage-2 gate. A consensus hold keeps its normalized draft and judges readiness and `drain_refusal` on it; a drain hold over a narrowed view records only the touched tracks' frontiers. `arbiter-admin` resolves an omitted `--hold` from the leader's committed `stage3`: issuance once `narrowed_drain` is on and the update touches no track, drain otherwise. With the switch off every Apply answer, drain view, hold and CLI default is the base's.

This task continues the P5 branch `urwt/claude/multi-source-s3e`: its three commits land on Task 76's last commit. Line anchors are arbiter `526a721` (`v0.13.0`, `origin/main` when this plan was drafted, Plan S3-A merged), which holds every Plan S3-A file edited here unchanged since Plan S3-A; `fsm/stage3.go` and `fsm/stage3_fixture_test.go` are Plan S3-E2 Task 64's, and `fsm/floor.go` is Task 76's. On the branch every hunk is found by its quoted lines under the hunk rule of the Global Constraints: Plan S3-E2 Task 65 edits other lines of `cmd/arbiter-admin/main.go` (the stage-3 flags, the carried `stage3`) and of `server/consensus_admin.go`, Plan S3-E3 and Task 76 add cases to `onStage3SwitchOnLocked` and paragraphs to the README's stage-3 section, and Task 76 edits other lines of `fsm/consensus_updates.go` (the raise-k precondition right after the drain gate and the tracks-activation refusal, the Leaving release before `ConsensusEpoch++`). No step of this task reaches a remote, so none needs a confirmation: Task 64 did the only fetch and Task 78 pushes the branch and opens its PR.

**Files:**
- Create: `fsm/drain_narrowed.go` (`TouchedTracks`, `narrowedDrainOnLocked`, `drainRefusalForUpdateLocked`, `narrowedDrainRefusalLocked`, `floorHeldUnsafePartsLocked`, `DrainViewFor`)
- Create: `fsm/drain_narrowed_test.go` (the ten fsm tests of contract §54 and their fixtures)
- Modify: `fsm/reads_drain.go` (Plan S3-A Task 7) — `DrainView` gains `Narrowed` (after `Tracks` `:31`), `TrackDrainView` gains `Touched` (`:38-44`), `DrainView()` (`:46-77`) becomes a wrapper of the new `drainViewLocked(refusal, floorHeld)`, `drainWorkBlocksLocked` (`:90-120`) takes `floorHeld` and skips a promotion that only the floor holds (`promotionFloorHeldLocked`, new, before `sortedBlocks` `:139`)
- Modify: `fsm/consensus_updates.go` — the drain gate of `applyUpdateConsensusParams` (`:74-78`) calls `drainRefusalForUpdateLocked(next.SIIndexers)`
- Modify: `fsm/stage3.go` (Plan S3-E2 Task 64) — the `narrowed_drain` case of `onStage3SwitchOnLocked`
- Modify: `fsm/BUILD.bazel` (gazelle adds `drain_narrowed.go` to `//fsm:fsm` and `drain_narrowed_test.go` to `//fsm:fsm_test`)
- Modify: `server/server.go` — the import block (`:18-20`) and the test seam `holdViewFor` after `holdView` (`:134-137`)
- Modify: `server/consensus_hold.go` (Plan S3-A Task 8) — the import block (`:13-18`), `consensusHold` gains `draft` (`:95-109`), `start` (`:208-228`) takes the draft and records only the touched tracks over a narrowed view
- Modify: `server/consensus_hold_rpc.go` (Plan S3-A Task 8) — the import block (`:8-15`), `PrepareConsensusUpdate` normalizes its draft (after `:37-40`) and reads `drainViewFor(draft)` (`:52-56`), `GetConsensusHold` reads `drainViewFor(hold.draft)` (`:89`), `drainViewFor` new after `drainView` (`:122-127`)
- Modify: `server/consensus_hold_test.go` (Plan S3-A Task 8) — two imports and three tests at the end
- Modify: `cmd/arbiter-admin/main.go` — the import block (`:17`), the `--hold` paragraph of the usage text (`:167-175`), the `--hold` flag (`:406`), `holdDefault` (after `:446`), the `--hold` validation (`:493-503`), the deadline (`:530-533`), the default's resolution after the draft is signed (`:727-731`), `defaultHoldMode` and `touchedIndexers` before `holdModeName` (`:1144`)
- Modify: `cmd/arbiter-admin/consensus_hold_test.go` (Plan S3-A Task 8) — two imports and five tests at the end
- Modify: `README.md` — the first sentence of the `**CLI.**` paragraph of `## Stage-3 reads and consensus holds` (Plan S3-A), and a paragraph `**Narrowed drain (switch 9).**` right before the `**CLI.**` paragraph of `## Multi-source storage integrity (stage-3 activation)` (Plan S3-E2 Task 65)
- Test: the files above; unchanged and green: `fsm/reads_drain_test.go` (Plan S3-A's `TestDrainViewRefusalEqualsTheDrainGate` among them), `fsm/quarantine_test.go`, `fsm/stage3_params_test.go`, `fsm/stage3_activation_test.go`, every hold test of Plan S3-A in `server/consensus_hold_test.go` and `cmd/arbiter-admin/consensus_hold_test.go`, `cmd/arbiter-admin/consensus_integration_test.go`, every golden test and every `TestDeterminism_*`

**Interfaces:**
- Consumes:
  - Task 76 (contract §53): `func (f *FSM) floorActiveLocked() bool`; `func (f *FSM) floorLocked(pp *PendingPromotion) FloorVerdict` with `FloorVerdict.FloorOnly` (base(P) holds, no escape, the count clause fails) and `FloorVerdict.Allowed`. It is called under the read lock too (`DrainViewFor`), which contract §53 allows: `floorIncarnationLocked` scans and never builds the lazy live index.
  - Plan S3-E2 Task 64 (contract §44–§45): `Stage3SwitchNarrowedDrain = "narrowed_drain"`, `Stage3SwitchCheckpoints`, `func (f *FSM) stage3SwitchOnLocked(name string) bool`, `func (f *FSM) Stage3SwitchOn(name string) bool`, the seam `func (f *FSM) onStage3SwitchOnLocked(name string)`, `Params.Stage3 *arbiter.Stage3Params`; the fixture (`fsm/stage3_fixture_test.go`) `func stage3FSM(t *testing.T) (*FSM, *authority.Signer)` (tracksFSM: indexer 1 on s1 owns `db.t`, indexer 0 on s0 owns `db0.x`, verifiers v1..v3; plus the activation of `stage3ActivationParams`), `func stage3Update(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate`, `func stage3WithSwitch(p *arbiter.Stage3Params, name string) *arbiter.Stage3Params`, `func turnOnStage3SwitchesThrough(t *testing.T, f *FSM, a *authority.Signer, last string)`. Plan S3-E2 Task 65: the server serves `current.stage3` in `GetConsensusParams` (`TestConsensusMutableParamsCarryStage3`) and `arbiter-admin` carries the committed `stage3` into every update.
  - Plan S3-E1 (contract §39, `PROTO_TAG_P5` and `CORE_TAG_P5`, pinned by Task 64): `pb.ConsensusMutableParams.Stage3` (field 9), `pb.Stage3Params` with `NarrowedDrain` (field 18), `pb.IndexerFailureDomain`; `arbiter.ConsensusParamsUpdate.Stage3`; `authority.NormalizeConsensusParamsUpdate` normalizing and validating `stage3`.
  - Plan S3-A (contract §4; arbiter `526a721`): `fsm.DrainView{PromotionSeq, Refusal, TracksActive, GlobalPrefix, GlobalFrontier, InFlightBlocks, Tracks}`, `fsm.TrackDrainView{IndexerID, Quarantined, Prefix, Frontier, InFlightBlocks}`, `(*FSM).DrainView()`, `drainWorkBlocksLocked`, `promotionHoldsDrainLocked`, `globalPromotionFrontierLocked`, `sortedBlocks`; server `ConsensusHolds`, `consensusHold`, `(*ConsensusHolds).start`, `(*Server).drainView()`, `Server.holdView`, `consensusHoldToPB`, `PrepareConsensusUpdate`, `GetConsensusHold`; arbiter-admin `holdName`, `holdMode`, `parseHoldMode`, `holdModeName`, `takeConsensusHold`, the `sign` closure and `currentSIIndexers` of `run`.
  - Existing arbiter: `drainGateRefusalLocked` (unchanged), `promotionTrackLocked`, `manifestTableOwnerLocked`, `blockTrackLocked`, `indexerQuarantinedLocked`, `publishedTrackFrontierLocked`, `manifestTrackIDsLocked`, `wire.SIIndexerEntriesFromPB`; fsm test helpers `passTrackBlock`, `admitTrack`, `bindTrackClaim`, `verifyBlock`, `promoteTrack`, `ackTrackPromotion`, `cleanTrackPromotion`, `setPublishedTrackFrontiers`, `tracksFSM` (`fsm/tracks_fixture_test.go`), `partCleanup`, `partCleanupAck` (`fsm/pre_tracks_golden_test.go`), `fixedClient` (`fsm/admission_source_test.go`), `mustSeal`, `anchorFinal`, `rejectClaimedBlockQ`, `releaseQ`, `trackQ`, `quarantineQ` (`fsm/quarantine_fixture_test.go`), `enrollment` (`fsm/signed_claims_params_test.go`), `authorityFixture` (`fsm/promotion_test.go`), `signedConsensusUpdate`, `rejectConsensusUnchanged` (`fsm/consensus_updates_test.go`), `mustApply` (`fsm/fsm_test.go`); server test helpers `newHoldFixture`, `holdWork`, `issuanceMode`, `drainMode`, `activateServerTracks`, `serverUpdateRequest`; arbiter-admin test helpers `holdLeader`, `requireSignedBy`, `fastHoldPolls`, `issuanceHold`, `drainHold`, `captureStderr`, `serveAdminImpl`, `startAdminServer`, `adminTestServer`, `testConsensusState`, `adminKey`, `nextAuthority`, `writeEnrollment`.
- Produces (contract §54; the names marked "added" are this task's own and are recorded in Appendix B):
  - fsm: `func TouchedTracks(previous, next []arbiter.SIIndexerEntry) []uint64`; `func (f *FSM) narrowedDrainOnLocked() bool`; `func (f *FSM) narrowedDrainRefusalLocked(touched []uint64) string`; `func (f *FSM) drainRefusalForUpdateLocked(next []arbiter.SIIndexerEntry) string`; `func (f *FSM) floorHeldUnsafePartsLocked() map[arbiter.TablePartition]map[string]bool`; `func (f *FSM) DrainViewFor(draft arbiter.ConsensusParamsUpdate) DrainView`; `DrainView.Narrowed bool`, `TrackDrainView.Touched bool`; the refusals, verbatim: `consensus update touches track %d, which has an unacknowledged promotion`, `consensus update touches track %d, which has an unacknowledged cleanup command`, `consensus update touches track %d, which has a promoted unsafe part waiting for something other than the copy floor`, `consensus update touches track %d, which has an unacknowledged rejected-part cleanup`; the `narrowed_drain` case of `onStage3SwitchOnLocked` (no state change). Added: `func (f *FSM) drainViewLocked(refusal string, floorHeld map[arbiter.TablePartition]map[string]bool) DrainView`, `func (f *FSM) promotionFloorHeldLocked(seq uint64, pp *PendingPromotion, floorHeld map[arbiter.TablePartition]map[string]bool) bool`, and `drainWorkBlocksLocked(floorHeld map[arbiter.TablePartition]map[string]bool)` (was `drainWorkBlocksLocked()`; its one caller is `drainViewLocked`).
  - server: `consensusHold.draft arbiter.ConsensusParamsUpdate`; `func (h *ConsensusHolds) start(mode pb.ConsensusHoldMode, term uint64, timeout time.Duration, draft arbiter.ConsensusParamsUpdate, view fsm.DrainView) (consensusHold, error)` (was `start(mode, term, draftEpoch, timeout, view)`; `draftEpoch` is now `draft.ExpectedEpoch`); `func (s *Server) drainViewFor(draft arbiter.ConsensusParamsUpdate) fsm.DrainView`; `drainView()` kept. Added: the tests-only seam `Server.holdViewFor func(draft arbiter.ConsensusParamsUpdate) fsm.DrainView`, read before `holdView`; the refusal `consensus hold draft: %v` (InvalidArgument; unreachable after the authorization, which hashes the normalized draft).
  - arbiter-admin: `--hold` defaults to `""`, help text verbatim `consensus hold before signing: none, issuance or drain (default: issuance once the leader reports stage3.narrowed_drain and the update touches no track, else drain)`; `func defaultHoldMode(state *pb.ConsensusParamsState, committed, target []arbiter.SIIndexerEntry) (pb.ConsensusHoldMode, string)` with the reasons, verbatim, `the leader reports stage3.narrowed_drain and the update touches no track`, `the leader does not report stage3.narrowed_drain`, `the update touches track %s` (ids joined by `, `); `func touchedIndexers(committed, target []arbiter.SIIndexerEntry) []uint64`; the stderr line `info: --hold defaults to <issuance|drain>: <reason>`. Added: `holdDefault` in `run`; an explicit `--hold ""` is refused by `parseHoldMode` (`--hold must be none, issuance or drain`), and `--hold-timeout` is accepted with the default.
  - README: the paragraph `**Narrowed drain (switch 9).**` and the rewritten first sentence of Plan S3-A's `**CLI.**` paragraph.
  - Tests: fsm `TestTouchedTracksAreTheChangedSIIndexersEntries`, `TestNarrowedDrainGateRefusesOnlyTouchedTracksWork`, `TestNarrowedDrainGateIgnoresFloorOnlyHeldParts`, `TestNarrowedDrainGateCountsPartsHeldForAnotherReason`, `TestNarrowedDrainSwitchOnIsJudgedByTheStage2Gate`, `TestNarrowedDrainSkipsQuarantinedTouchedTracks`, `TestAnUpdateThatTouchesNoTrackPassesWithInFlightWork`, `TestDrainViewForEqualsApplysDrainRefusal`, `TestDrainViewKeepsTheStage2Refusal`, `TestDrainViewForBeforeTheSwitchIsTheStage2Gate`; server `TestIssuanceHoldIsReadyForAnUpdateThatTouchesNoTrack`, `TestHoldReadinessFollowsTheDraft`, `TestDrainHoldRecordsOnlyTouchedTracksAfterTheNarrowedDrainSwitch`; arbiter-admin `TestConsensusUpdateDefaultsToIssuanceOnceTheLeaderReportsNarrowedDrain`, `TestConsensusUpdateDefaultsToDrainBeforeNarrowedDrain`, `TestConsensusUpdateDefaultsToDrainForAnUpdateThatTouchesATrack`, `TestExplicitHoldFlagOverridesTheDefault`, `TestTouchedIndexersMatchesTheFSM`. Added test helpers: fsm `narrowedFSM`, `holdFloorAtKTwoStrict`, `issueTrackPromotion`, `rejectTrackBlock`, `publishTrackEntry`, `rotationKey`, `touchingUpdate` and the constants `narrowedPromotionRefusal0`, `narrowedPromotionRefusal1`, `narrowedCleanupRefusal1`, `narrowedPartRefusal1`, `narrowedRejectedRefusal0`; server `narrowedHoldTracks`; arbiter-admin `defaultHoldArgs`, `stage3LeaderState`, `readyHoldLeader`.

Notes:
- **Switch-off identity (contract §54).** Until `narrowed_drain` is committed `drainRefusalForUpdateLocked` is `drainGateRefusalLocked`, `DrainViewFor(draft)` is `DrainView()` for every draft (`TestDrainViewForBeforeTheSwitchIsTheStage2Gate`, on the tracks network and with every switch through `checkpoints`), a hold's view is never `Narrowed` so `start` records every non-quarantined track as before, and the CLI's default is `drain` (Plan S3-A K6). `DrainView()` never sets `Narrowed` or `Touched`, so Plan S3-A's `reflect.DeepEqual` drain tests pass unchanged, and nothing here touches a snapshot byte, a Raft tag or a proto number.
- **The switch-on meets the stage-2 gate.** The gate reads the committed switch from `f.st.Params` before `f.st.Params = next`, so the update that turns `narrowed_drain` on, which touches no track, still needs every track drained (`TestNarrowedDrainSwitchOnIsJudgedByTheStage2Gate`). That is why it runs under `--hold drain` (Plan S3-F K83), which the CLI's default gives it: the committed `stage3` does not yet report `narrowed_drain`.
- **Touched tracks.** `TouchedTracks` compares by `IndexerID` with `reflect.DeepEqual`, so a signer rotation, an enrolment and any other field change (an `enrollment_jws` restated alone) touch the entry's track. A nil list stands for the committed one. A removed entry is not a touch: `verifySIIndexersTransition` refuses the removal before the drain gate runs. In Apply `next` is the verified parameters, in `DrainViewFor` the draft's normalized list, in the CLI the signed draft's list against `current.si_indexers`; `TestTouchedIndexersMatchesTheFSM` pins the CLI's copy to the FSM's.
- **The narrowed refusal.** Each touched track is checked in ascending order, and each in the stage-2 order: an unacknowledged promotion, a scheduled cleanup (dispatched or not), a promoted unsafe part that waits for anything but the copy floor, a rejected partition (clarification C4). A quarantined touched track is skipped, as the stage-2 gate skips a quarantined track's work (spec 2026-10-10 §7.4); its work stays recorded. Work resolves its track exactly as in the stage-2 gate, and work whose track does not resolve counts for every touched track, the stage-2 gate's fail-closed rule ("work whose track does not resolve counts"), so it holds no update that touches no track (recorded in Appendix B; the white-box case of `TestDrainViewForEqualsApplysDrainRefusal`). Each check asks only whether matching work exists, so the answer does not depend on map order.
- **Floor-held parts.** A promoted unsafe part is floor-held exactly when an acknowledged promotion without a pending cleanup lists it and `floorLocked(pp).FloorOnly` holds. Contract §54 does not say what happens when two acknowledged promotions list the same part hash (identical rows in two statements give the same `PartRowLtHash`); `floorHeldUnsafePartsLocked` keeps such a part in flight unless every such promotion is FloorOnly, because the other one may be waiting for its own cleanup (recorded in Appendix B; the `another promotion of the part` subtest). `narrowed_drain` needs `checkpoints` (spec §10.4 order), so the floor is active whenever the narrowed gate runs; the function returns nil while it is not. The parts are computed once per answer and only when a touched track has a promoted unsafe part.
- **Drain views.** `DrainViewFor` uses the same in-flight rules as `DrainView` except that, once narrowed, a promotion whose remaining parts only the floor holds is not in flight (`promotionFloorHeldLocked`), so a hold reports the blocks that can still delay it. `Touched` is set only in a narrowed view. `DrainView()` keeps Plan S3-A §4's stage-2 answer for every caller that has no draft (`TestDrainViewKeepsTheStage2Refusal`).
- **Holds judge their draft.** `PrepareConsensusUpdate` normalizes the authorized draft (the authorization hashes the normalized form, so the normalization cannot fail there) and keeps it in the hold; the issuance check, `start` and every `GetConsensusHold` read `drainViewFor(hold.draft)`. Over a narrowed view a drain hold records only the touched, non-quarantined tracks. An untouched track has no recorded frontier, so its promotions are held for the hold's life (Plan S3-A's refusal `drain mode refuses promotions of indexer %d, which had no recorded frontier when the hold was taken`): `promotion_seq` stops moving once the hold is ready, which the exact `expected_promotion_seq` needs, and readiness covers only the touched tracks. `TrackDrain` still lists every track, an untouched one with frontier 0 and nothing in flight, as for a track quarantined at hold start; no proto changes. `drainView()` keeps no server caller after this task; contract §54 keeps it for callers without a draft.
- **The server tests use a draft-aware seam.** Plan S3-A's seam `holdView func() fsm.DrainView` cannot see the draft, so this task adds `holdViewFor`, read first; Plan S3-A's tests keep `holdView` unchanged. The server tests prove that every read gets the hold's normalized draft (the request spells a rotated signer in upper case) and what a hold does with a narrowed view; the FSM half is proven by the fsm tests, and Task 78's `TestNarrowedDrainUpdateCommitsWhileTheFloorHoldsACleanup` runs the whole path over real members.
- **The CLI default (K79).** Without `--hold` the CLI resolves the mode after it signs the draft, from `current.stage3.narrowed_drain` of the `GetConsensusParams` it already read and the signed draft's `si_indexers`, prints `info: --hold defaults to <mode>: <reason>` on stderr and then takes that hold exactly as an explicit `--hold` would. A default hold always extends `--timeout` by `--hold-timeout`, which may now be given without `--hold`. An explicit `--hold` is parsed by `parseHoldMode` as before and prints nothing; `--hold ""` is refused like any other bad value. Plan S3-A's tests and every update-argument helper pass `--hold none` and keep their flow; `TestConsensusUpdateHoldUnimplementedNamesHoldNone`'s update without `--hold` now resolves to `drain` (its fake reports no `stage3`) and still meets the same Unimplemented refusal.
- **The k = 2 fixture is white-box.** A floor-held part needs k ≥ 2, and raising k the committed way needs caught-up counting members in k − 1 failure domains (Task 76's precondition), which Task 76's floor fixture builds and this file does not. `holdFloorAtKTwoStrict` sets `min_safe_copies` 2 and `strict_copies` on the committed parameters after `narrowed_drain`; in strict mode a group without counting members needs one reflecting domain and has none, so `floorLocked` answers FloorOnly for every acknowledged, published promotion of an Active table, and `publishTrackEntry` stands in for the publication (`setPublishedTrackFrontiers`, the tracks fixture's stand-in). Every later update carries those values unchanged, so no update raises k, and no test restores such an FSM. The other fsm tests run at k = 1 with every switch committed through Apply.
- **fsm red lines.** `fsm/drain_narrowed.go` imports `fmt`, `reflect`, `slices` and arbiter-core only: no arbiter-proto, no clock.

- [ ] **Step 1: Check the worktree, its head and the inputs**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; git -C "$W" status --porcelain; git -C "$W" branch --show-current; git -C "$W" log -1 --format=%s`
Expected: no status line; `urwt/claude/multi-source-s3e`; a subject ending in `(Task 76)`. Otherwise stop: Tasks 64–76 land first, in order, on this one branch (contract §48), and this task needs Task 76's floor.

Run:

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" || exit 1
grep -c -e 'func (f \*FSM) floorActiveLocked() bool' -e 'func (f \*FSM) floorLocked(pp \*PendingPromotion) FloorVerdict' fsm/floor.go
grep -c -e 'func (f \*FSM) stage3SwitchOnLocked(name string) bool' -e 'func (f \*FSM) onStage3SwitchOnLocked(name string)' -e 'Stage3SwitchNarrowedDrain *= "narrowed_drain"' fsm/stage3.go
grep -c -e 'func stage3FSM(t \*testing.T) (\*FSM, \*authority.Signer)' -e 'func stage3Update(t \*testing.T, f \*FSM) arbiter.ConsensusParamsUpdate' -e 'func stage3WithSwitch(p \*arbiter.Stage3Params, name string) \*arbiter.Stage3Params' -e 'func turnOnStage3SwitchesThrough(t \*testing.T, f \*FSM, a \*authority.Signer, last string)' fsm/stage3_fixture_test.go
grep -c -e 'func (f \*FSM) drainWorkBlocksLocked() (all map\[uint64\]bool' -e 'Refusal:        f.drainGateRefusalLocked(),' fsm/reads_drain.go
grep -c 'if reason := f.drainGateRefusalLocked(); reason != "" {' fsm/consensus_updates.go
grep -c 'func (h \*ConsensusHolds) start(mode pb.ConsensusHoldMode, term, draftEpoch uint64, timeout time.Duration, view fsm.DrainView)' server/consensus_hold.go
grep -c -e 'view := s.drainView()' -e 'return consensusHoldToPB(hold, svc.s.drainView()), nil' server/consensus_hold_rpc.go
grep -c 'fs.StringVar(&holdName, "hold", "drain", ' cmd/arbiter-admin/main.go
grep -c '^\*\*CLI\.\*\* `arbiter-admin consensus update ... --stage3 ' README.md
grep -c '^\*\*CLI\.\*\* `arbiter-admin consensus update` takes a hold by default (`--hold drain`): ' README.md
grep -rn -e 'func TouchedTracks' -e 'DrainViewFor' -e 'holdViewFor' -e 'func defaultHoldMode' fsm/ server/ cmd/
go doc github.com/sentioxyz/arbiter-proto/gen/pb Stage3Params.GetNarrowedDrain | tail -1
```

Expected: `2`, `3`, `4`, `2`, `1`, `1`, `2`, `1`, `1`, `1`, nothing from the recursive grep, then `func (x *Stage3Params) GetNarrowedDrain() bool`. If a count differs, stop: Task 76's floor predicate, Task 64's switch seam and fixture, Plan S3-A's drain view and holds, the CLI's `--hold drain` default, both README anchors and the pinned `PROTO_TAG_P5` are this task's inputs, and nothing of this task exists yet.

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./fsm/ ./server/ ./cmd/arbiter-admin/ -count=1`
Expected: `ok` for the three packages (the branch is green before this task starts).

- [ ] **Step 2: Write the failing fsm tests**

Create `fsm/drain_narrowed_test.go`:

```go
package fsm

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"slices"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
)

// The narrowed drain fixtures (housegate spec 2026-10-14 §10.1, plan S3-E4
// contract §54) build on stage3FSM (plan S3-E2 contract §45): indexer 1 on
// s1 owns db.t, indexer 0 on s0 owns db0.x, and verifiers v1..v3 are the
// network pool. Each track's work is made with the tracks fixture's helpers.

const (
	narrowedPromotionRefusal0 = "consensus update touches track 0, which has an unacknowledged promotion"
	narrowedPromotionRefusal1 = "consensus update touches track 1, which has an unacknowledged promotion"
	narrowedCleanupRefusal1   = "consensus update touches track 1, which has an unacknowledged cleanup command"
	narrowedPartRefusal1      = "consensus update touches track 1, which has a promoted unsafe part waiting for something other than the copy floor"
	narrowedRejectedRefusal0  = "consensus update touches track 0, which has an unacknowledged rejected-part cleanup"
)

// narrowedFSM is stage3FSM with every switch through narrowed_drain
// committed, one update each in spec §10.4 order, at k = 1.
func narrowedFSM(t *testing.T) (*FSM, *authority.Signer) {
	t.Helper()
	f, a := stage3FSM(t)
	turnOnStage3SwitchesThrough(t, f, a, Stage3SwitchNarrowedDrain)
	return f, a
}

// holdFloorAtKTwoStrict sets min_safe_copies 2 and strict_copies on f's
// committed stage3 directly. Raising k the committed way needs caught-up
// counting members in k - 1 failure domains (plan S3-E4 contract §53), which
// Task 76's floor fixture builds and this one does not. In strict mode the
// count needs k - 1 = 1 reflecting domain, and a group without counting
// members has none, so the copy floor alone holds the cleanup of every
// acknowledged, published promotion of an Active table (FloorOnly): the work
// the narrowed gate must tell apart from in-flight work. Every later update
// carries these values unchanged, so no update raises k.
func holdFloorAtKTwoStrict(f *FSM) {
	f.st.Params.Stage3.MinSafeCopies = 2
	f.st.Params.Stage3.StrictCopies = true
}

// issueTrackPromotion admits account's statement seq for owner's table into
// a block of its own, binds its claim, verifies and finalizes the block and
// issues the promotion of its part without acknowledging it. It returns the
// block and the promotion.
func issueTrackPromotion(t *testing.T, f *FSM, a *authority.Signer, key *ecdsa.PrivateKey, account string, owner, seq uint64) (uint64, uint64) {
	t.Helper()
	stmt := admitTrack(t, f, key, account, seq, owner)
	block := mustSeal(t, f).BlockSeq
	bindTrackClaim(t, f, stmt)
	verifyBlock(t, f, block)
	anchorFinal(t, f, block)
	return block, promoteTrack(t, f, a, stmt)
}

// rejectTrackBlock admits account's statement seq for owner's table into a
// block of its own, binds its claim, rejects the block (which quarantines the
// track) and releases the track acknowledging the block, so the block's
// rejected partition waits for its rejected-part cleanup. It returns the
// block.
func rejectTrackBlock(t *testing.T, f *FSM, a *authority.Signer, key *ecdsa.PrivateKey, account string, owner, seq uint64) uint64 {
	t.Helper()
	stmt := admitTrack(t, f, key, account, seq, owner)
	block := mustSeal(t, f).BlockSeq
	bindTrackClaim(t, f, stmt)
	rejectClaimedBlockQ(t, f, block)
	mustApply(t, f, releaseQ(t, f, a, owner, trackQ(t, f, owner).QuarantineGeneration, block))
	if len(f.st.Tracks.RejectedParts) == 0 {
		t.Fatalf("fixture: block %d left no rejected partition", block)
	}
	return block
}

// publishTrackEntry stands in for a publication of owner's entry {block,
// promotion} (setPublishedTrackFrontiers), keeping every other track's
// published entry.
func publishTrackEntry(t *testing.T, f *FSM, owner, block, promotion uint64) {
	t.Helper()
	var frontiers []replay.TrackFrontier
	for _, id := range f.manifestTrackIDsLocked() {
		entry := f.publishedTrackFrontierLocked(id)
		if id == owner {
			entry = replay.TrackFrontier{IndexerID: owner, SafeBlockSeq: block, AppliedPromotionSeq: promotion}
		}
		frontiers = append(frontiers, entry)
	}
	setPublishedTrackFrontiers(t, f, frontiers...)
}

// rotationKey is a fresh signer for indexer id at f's current epoch,
// distinct from every fixture key and from every earlier rotation.
func rotationKey(t *testing.T, f *FSM, id uint64) *authority.Signer {
	t.Helper()
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], id)
	binary.BigEndian.PutUint64(b[8:], f.st.ConsensusEpoch)
	seed := sha256.Sum256(append([]byte("narrowed drain rotation key "), b[:]...))
	return authoritytest.MustSigner(t, hex.EncodeToString(seed[:]))
}

// touchingUpdate is f's next update (stage3Update: every committed value
// carried) with max_writers raised by one, so that it always changes
// something, and the signer of each indexer in ids rotated to a fresh key: a
// signer rotation touches its track (spec §10.1).
func touchingUpdate(t *testing.T, f *FSM, ids ...uint64) arbiter.ConsensusParamsUpdate {
	t.Helper()
	u := stage3Update(t, f)
	u.MaxWriters = f.st.Params.maxWriters() + 1
	for i := range u.SIIndexers {
		e := &u.SIIndexers[i]
		if slices.Contains(ids, e.IndexerID) {
			key := rotationKey(t, f, e.IndexerID)
			e.Signer, e.EnrollmentJWS = key.Address(), enrollment(t, f, key, e.IndexerID, e.SNodeNodeID)
		}
	}
	return u
}

func TestTouchedTracksAreTheChangedSIIndexersEntries(t *testing.T) {
	e0 := arbiter.SIIndexerEntry{IndexerID: 0, ActivationBlock: 150, Signer: "0xsigner0", SNodeNodeID: "s0", EnrollmentJWS: "jws-0"}
	e1 := arbiter.SIIndexerEntry{IndexerID: 1, ActivationBlock: 100, Signer: "0xsigner1", SNodeNodeID: "s1", EnrollmentJWS: "jws-1"}
	e2 := arbiter.SIIndexerEntry{IndexerID: 2, ActivationBlock: 300, Signer: "0xsigner2", SNodeNodeID: "s2", EnrollmentJWS: "jws-2"}
	rotated := e1
	rotated.Signer, rotated.EnrollmentJWS = "0xsigner1b", "jws-1b"
	restated := e0
	restated.EnrollmentJWS = "jws-0b"
	committed := []arbiter.SIIndexerEntry{e0, e1}
	for _, tc := range []struct {
		name           string
		previous, next []arbiter.SIIndexerEntry
		want           []uint64
	}{
		{"a nil list stands for the committed one", committed, nil, nil},
		{"unchanged", committed, []arbiter.SIIndexerEntry{e0, e1}, nil},
		{"a signer rotation", committed, []arbiter.SIIndexerEntry{e0, rotated}, []uint64{1}},
		{"an enrolment", committed, []arbiter.SIIndexerEntry{e0, e1, e2}, []uint64{2}},
		{"any other field", committed, []arbiter.SIIndexerEntry{restated, e1}, []uint64{0}},
		{"ascending whatever the order", committed, []arbiter.SIIndexerEntry{e2, rotated, restated}, []uint64{0, 1, 2}},
		{"the signed-claims activation", nil, []arbiter.SIIndexerEntry{e0, e1}, []uint64{0, 1}},
	} {
		if got := TouchedTracks(tc.previous, tc.next); !slices.Equal(got, tc.want) {
			t.Fatalf("%s: TouchedTracks = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestNarrowedDrainGateRefusesOnlyTouchedTracksWork: spec §10.1. Track 1 has
// a scheduled cleanup and track 0 an unacknowledged promotion, so the
// stage-2 gate refuses every update; the narrowed gate refuses an update for
// the work of the tracks it touches only, per track in ascending order, and
// an update touching track 1 commits once its cleanup is acknowledged while
// track 0's promotion still is not.
func TestNarrowedDrainGateRefusesOnlyTouchedTracksWork(t *testing.T) {
	f, a := narrowedFSM(t)
	key, account := fixedClient(t)
	block, promotions := passTrackBlock(t, f, a, key, account, 1, 1)
	publishTrackEntry(t, f, 1, block, promotions[0])
	mustApply(t, f, partCleanup(t, f, a, promotions[0]))
	_, pending := issueTrackPromotion(t, f, a, key, account, 0, 2)
	if got := f.DrainView().Refusal; got != "consensus update requires all promotions to be acknowledged" {
		t.Fatalf("stage-2 refusal %q", got)
	}
	for _, tc := range []struct {
		touch []uint64
		want  string
	}{
		{[]uint64{0}, narrowedPromotionRefusal0},
		{[]uint64{1}, narrowedCleanupRefusal1},
		{[]uint64{0, 1}, narrowedPromotionRefusal0},
	} {
		rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, touchingUpdate(t, f, tc.touch...)), tc.want)
	}
	mustApply(t, f, partCleanupAck(t, f, promotions[0]))
	mustApply(t, f, signedConsensusUpdate(t, a, touchingUpdate(t, f, 1)))
	if pp := f.st.PendingPromotions[pending]; pp == nil || pp.Acked {
		t.Fatalf("track 0's promotion %d = %+v, want it still unacknowledged", pending, pp)
	}
}

// TestNarrowedDrainGateIgnoresFloorOnlyHeldParts: spec §10.1. A promoted
// unsafe part whose cleanup only the copy floor holds is not in-flight work:
// the stage-2 gate still refuses, while an update touching the part's track
// commits and leaves the part promoted.
func TestNarrowedDrainGateIgnoresFloorOnlyHeldParts(t *testing.T) {
	f, a := narrowedFSM(t)
	holdFloorAtKTwoStrict(f)
	key, account := fixedClient(t)
	block, promotions := passTrackBlock(t, f, a, key, account, 1, 1)
	publishTrackEntry(t, f, 1, block, promotions[0])
	pp := f.st.PendingPromotions[promotions[0]]
	if v := f.floorLocked(pp); !v.FloorOnly {
		t.Fatalf("fixture: floor verdict %+v, want FloorOnly", v)
	}
	tp := arbiter.TablePartition{TableID: pp.Promote.TableID, PartitionID: pp.Promote.PartitionID}
	hash := pp.Promote.CandidateParts[0].PartRowLtHash
	if got, want := f.floorHeldUnsafePartsLocked(), map[arbiter.TablePartition]map[string]bool{tp: {hash: true}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("floor-held parts %v, want %v", got, want)
	}
	if got := f.DrainView().Refusal; got != "consensus update requires all promoted unsafe parts to be cleaned" {
		t.Fatalf("stage-2 refusal %q", got)
	}
	mustApply(t, f, signedConsensusUpdate(t, a, touchingUpdate(t, f, 1)))
	if !f.st.PromotedUnsafe[tp][hash] {
		t.Fatal("the update cleaned the floor-held part")
	}
}

// TestNarrowedDrainGateCountsPartsHeldForAnotherReason: spec §10.1. A
// promoted unsafe part stays in-flight work while it waits for anything but
// the copy floor: its track's publication, the cleanup the floor already
// allows, or another acknowledged promotion that lists it and that the floor
// does not hold alone.
func TestNarrowedDrainGateCountsPartsHeldForAnotherReason(t *testing.T) {
	t.Run("its publication", func(t *testing.T) {
		f, a := narrowedFSM(t)
		holdFloorAtKTwoStrict(f)
		key, account := fixedClient(t)
		block, promotions := passTrackBlock(t, f, a, key, account, 1, 1)
		rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, touchingUpdate(t, f, 1)), narrowedPartRefusal1)
		publishTrackEntry(t, f, 1, block, promotions[0])
		mustApply(t, f, signedConsensusUpdate(t, a, touchingUpdate(t, f, 1)))
	})
	t.Run("a cleanup the floor allows", func(t *testing.T) {
		f, a := narrowedFSM(t)
		key, account := fixedClient(t)
		block, promotions := passTrackBlock(t, f, a, key, account, 1, 1)
		publishTrackEntry(t, f, 1, block, promotions[0])
		if v := f.floorLocked(f.st.PendingPromotions[promotions[0]]); !v.Allowed || v.FloorOnly {
			t.Fatalf("fixture: floor verdict %+v at k = 1, want Allowed", v)
		}
		rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, touchingUpdate(t, f, 1)), narrowedPartRefusal1)
	})
	t.Run("another promotion of the part", func(t *testing.T) {
		f, a := narrowedFSM(t)
		holdFloorAtKTwoStrict(f)
		key, account := fixedClient(t)
		block, promotions := passTrackBlock(t, f, a, key, account, 1, 1)
		publishTrackEntry(t, f, 1, block, promotions[0])
		// A second acknowledged promotion of the same part, above the
		// published applied_promotion_seq, so the floor's base clause holds
		// it rather than the floor alone.
		twin := *f.st.PendingPromotions[promotions[0]]
		twin.Promote.PromotionSeq = promotions[0] + 1
		f.st.PendingPromotions[twin.Promote.PromotionSeq] = &twin
		if held := f.floorHeldUnsafePartsLocked(); len(held) != 0 {
			t.Fatalf("floor-held parts %v, want none while another promotion waits on the part", held)
		}
		rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, touchingUpdate(t, f, 1)), narrowedPartRefusal1)
	})
}

// TestNarrowedDrainSwitchOnIsJudgedByTheStage2Gate: spec §10.4 step 9. The
// gate reads the committed switch, so the update that turns narrowed_drain
// on, which touches no track, still waits for every track to drain; the next
// update is judged by the narrowed gate.
func TestNarrowedDrainSwitchOnIsJudgedByTheStage2Gate(t *testing.T) {
	f, a := stage3FSM(t)
	turnOnStage3SwitchesThrough(t, f, a, Stage3SwitchCheckpoints)
	key, account := fixedClient(t)
	block, p := issueTrackPromotion(t, f, a, key, account, 1, 1)
	switchOn := func() arbiter.ConsensusParamsUpdate {
		u := stage3Update(t, f)
		u.Stage3 = stage3WithSwitch(u.Stage3, Stage3SwitchNarrowedDrain)
		return u
	}
	if v := f.DrainViewFor(switchOn()); v.Narrowed || v.Refusal != "consensus update requires all promotions to be acknowledged" {
		t.Fatalf("view of the switch-on = %+v, want the stage-2 refusal", v)
	}
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, switchOn()), "consensus update requires all promotions to be acknowledged")
	ackTrackPromotion(t, f, p)
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, switchOn()), "consensus update requires all promoted unsafe parts to be cleaned")
	publishTrackEntry(t, f, 1, block, p)
	cleanTrackPromotion(t, f, a, p)
	mustApply(t, f, signedConsensusUpdate(t, a, switchOn()))
	if !f.Stage3SwitchOn(Stage3SwitchNarrowedDrain) {
		t.Fatal("narrowed_drain did not turn on")
	}
	issueTrackPromotion(t, f, a, key, account, 0, 2)
	mustApply(t, f, signedConsensusUpdate(t, a, touchingUpdate(t, f)))
}

// TestNarrowedDrainSkipsQuarantinedTouchedTracks: like the stage-2 gate
// (spec 2026-10-10 §7.4), the narrowed gate skips a quarantined touched
// track: its work stays recorded and no longer holds an update that touches
// it.
func TestNarrowedDrainSkipsQuarantinedTouchedTracks(t *testing.T) {
	f, a := narrowedFSM(t)
	key, account := fixedClient(t)
	_, p := issueTrackPromotion(t, f, a, key, account, 0, 1)
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, touchingUpdate(t, f, 0)), narrowedPromotionRefusal0)
	mustApply(t, f, quarantineQ(t, f, a, 0, 0, "stuck source"))
	mustApply(t, f, signedConsensusUpdate(t, a, touchingUpdate(t, f, 0, 1)))
	if pp := f.st.PendingPromotions[p]; pp == nil || pp.Acked {
		t.Fatalf("the quarantined track's promotion %d = %+v, want it kept", p, pp)
	}
}

// TestAnUpdateThatTouchesNoTrackPassesWithInFlightWork: spec §10.1. Changes
// to max_writers, stage3 and the authority set touch no track, so each
// commits while both tracks have in-flight work and the stage-2 gate still
// refuses.
func TestAnUpdateThatTouchesNoTrackPassesWithInFlightWork(t *testing.T) {
	f, a := narrowedFSM(t)
	holdFloorAtKTwoStrict(f)
	key, account := fixedClient(t)
	passTrackBlock(t, f, a, key, account, 1, 1)
	issueTrackPromotion(t, f, a, key, account, 0, 2)
	other, _ := authorityFixture(t)
	for _, tc := range []struct {
		name string
		edit func(*arbiter.ConsensusParamsUpdate)
	}{
		{"max_writers", func(*arbiter.ConsensusParamsUpdate) {}},
		{"stage3 lag_low", func(u *arbiter.ConsensusParamsUpdate) { u.Stage3.LagLow++ }},
		{"stage3 failure domain", func(u *arbiter.ConsensusParamsUpdate) { u.Stage3.IndexerFailureDomains[0].FailureDomain = "node:moved" }},
		{"authority set", func(u *arbiter.ConsensusParamsUpdate) {
			u.AuthorityAddresses = append(slices.Clone(u.AuthorityAddresses), other.Address())
		}},
	} {
		u := touchingUpdate(t, f)
		tc.edit(&u)
		if v := f.DrainViewFor(u); v.Refusal != "" || !v.Narrowed {
			t.Fatalf("%s: view %+v, want narrowed with no refusal", tc.name, v)
		}
		mustApply(t, f, signedConsensusUpdate(t, a, u))
	}
	if got := f.DrainView().Refusal; got != "consensus update requires all promotions to be acknowledged" {
		t.Fatalf("stage-2 refusal after the updates %q", got)
	}
}

// TestDrainViewForEqualsApplysDrainRefusal: DrainViewFor(draft).Refusal is
// exactly what Apply's drain gate answers for the signed draft, for every
// kind of in-flight work and every touched set: a refused draft leaves state
// unchanged with that reason, and a draft with no refusal commits.
func TestDrainViewForEqualsApplysDrainRefusal(t *testing.T) {
	type work func(t *testing.T, f *FSM, a *authority.Signer)
	unackedOnTrack0 := func(t *testing.T, f *FSM, a *authority.Signer) {
		key, account := fixedClient(t)
		issueTrackPromotion(t, f, a, key, account, 0, 1)
	}
	cleanupOnTrack1 := func(t *testing.T, f *FSM, a *authority.Signer) {
		key, account := fixedClient(t)
		block, promotions := passTrackBlock(t, f, a, key, account, 1, 1)
		publishTrackEntry(t, f, 1, block, promotions[0])
		mustApply(t, f, partCleanup(t, f, a, promotions[0]))
	}
	publishedPartOnTrack1 := func(t *testing.T, f *FSM, a *authority.Signer) {
		key, account := fixedClient(t)
		block, promotions := passTrackBlock(t, f, a, key, account, 1, 1)
		publishTrackEntry(t, f, 1, block, promotions[0])
	}
	unpublishedPartOnTrack1 := func(t *testing.T, f *FSM, a *authority.Signer) {
		key, account := fixedClient(t)
		passTrackBlock(t, f, a, key, account, 1, 1)
	}
	rejectedOnTrack0 := func(t *testing.T, f *FSM, a *authority.Signer) {
		key, account := fixedClient(t)
		rejectTrackBlock(t, f, a, key, account, 0, 1)
	}
	// A pending promotion that names no statement, as one of statements
	// sealed before the tracks activation would: its track does not resolve
	// (promotionTrackLocked), so it counts for every touched track.
	unresolvedPromotion := func(t *testing.T, f *FSM, a *authority.Signer) {
		seq := f.st.PromotionSeq + 1
		f.st.PendingPromotions[seq] = &PendingPromotion{Promote: arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p-unresolved", PromotionSeq: seq}}
		if _, ok := f.promotionTrackLocked(seq); ok {
			t.Fatal("fixture: the promotion's track resolves")
		}
	}
	for _, tc := range []struct {
		name  string
		floor bool
		build work
		touch []uint64
		want  string
	}{
		{"an unacknowledged promotion, no track touched", false, unackedOnTrack0, nil, ""},
		{"an unacknowledged promotion, its track touched", false, unackedOnTrack0, []uint64{0}, narrowedPromotionRefusal0},
		{"an unacknowledged promotion, another track touched", false, unackedOnTrack0, []uint64{1}, ""},
		{"an unacknowledged promotion, both tracks touched", false, unackedOnTrack0, []uint64{1, 0}, narrowedPromotionRefusal0},
		{"a scheduled cleanup, its track touched", false, cleanupOnTrack1, []uint64{1}, narrowedCleanupRefusal1},
		{"a scheduled cleanup, another track touched", false, cleanupOnTrack1, []uint64{0}, ""},
		{"a part whose cleanup the floor allows", false, publishedPartOnTrack1, []uint64{1}, narrowedPartRefusal1},
		{"a part only the floor holds", true, publishedPartOnTrack1, []uint64{1}, ""},
		{"a part before its publication", true, unpublishedPartOnTrack1, []uint64{1}, narrowedPartRefusal1},
		{"a rejected partition, its track touched", false, rejectedOnTrack0, []uint64{0}, narrowedRejectedRefusal0},
		{"a rejected partition, another track touched", false, rejectedOnTrack0, []uint64{1}, ""},
		{"a promotion without a track, a track touched", false, unresolvedPromotion, []uint64{1}, narrowedPromotionRefusal1},
		{"a promotion without a track, no track touched", false, unresolvedPromotion, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, a := narrowedFSM(t)
			if tc.floor {
				holdFloorAtKTwoStrict(f)
			}
			tc.build(t, f, a)
			u := touchingUpdate(t, f, tc.touch...)
			if got := f.DrainViewFor(u).Refusal; got != tc.want {
				t.Fatalf("DrainViewFor refusal %q, want %q", got, tc.want)
			}
			if cmd := signedConsensusUpdate(t, a, u); tc.want == "" {
				mustApply(t, f, cmd)
			} else {
				rejectConsensusUnchanged(t, f, cmd, tc.want)
			}
		})
	}
}

// TestDrainViewKeepsTheStage2Refusal: plan S3-A contract §4. DrainView keeps
// the stage-2 answer after the switch (never Narrowed, no track Touched,
// floor-held work in flight), while DrainViewFor of a draft touching track
// 0 is narrowed: track 0 touched, and the floor-held block of track 1 no
// longer in flight.
func TestDrainViewKeepsTheStage2Refusal(t *testing.T) {
	f, a := narrowedFSM(t)
	holdFloorAtKTwoStrict(f)
	key, account := fixedClient(t)
	block1, promotions := passTrackBlock(t, f, a, key, account, 1, 1)
	publishTrackEntry(t, f, 1, block1, promotions[0])
	block0, _ := issueTrackPromotion(t, f, a, key, account, 0, 2)
	stage2 := f.DrainView()
	if stage2.Refusal != "consensus update requires all promotions to be acknowledged" || stage2.Narrowed || len(stage2.Tracks) != 2 ||
		stage2.Tracks[0].Touched || stage2.Tracks[1].Touched || !slices.Equal(stage2.InFlightBlocks, []uint64{block1, block0}) ||
		!slices.Equal(stage2.Tracks[1].InFlightBlocks, []uint64{block1}) {
		t.Fatalf("DrainView = %+v", stage2)
	}
	want := stage2
	want.Refusal, want.Narrowed, want.InFlightBlocks = narrowedPromotionRefusal0, true, []uint64{block0}
	want.Tracks = slices.Clone(stage2.Tracks)
	want.Tracks[0].Touched, want.Tracks[1].InFlightBlocks = true, nil
	if got := f.DrainViewFor(touchingUpdate(t, f, 0)); !reflect.DeepEqual(got, want) {
		t.Fatalf("DrainViewFor = %+v, want %+v", got, want)
	}
	if got := f.DrainView(); !reflect.DeepEqual(got, stage2) {
		t.Fatalf("DrainView after DrainViewFor = %+v, want %+v", got, stage2)
	}
}

// TestDrainViewForBeforeTheSwitchIsTheStage2Gate: until narrowed_drain is
// committed DrainViewFor of any draft is DrainView, after the tracks
// activation and with every earlier switch on alike.
func TestDrainViewForBeforeTheSwitchIsTheStage2Gate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(t *testing.T) (*FSM, *authority.Signer)
	}{
		{"tracks", tracksFSM},
		{"stage3 through checkpoints", func(t *testing.T) (*FSM, *authority.Signer) {
			f, a := stage3FSM(t)
			turnOnStage3SwitchesThrough(t, f, a, Stage3SwitchCheckpoints)
			return f, a
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, a := tc.build(t)
			key, account := fixedClient(t)
			issueTrackPromotion(t, f, a, key, account, 0, 1)
			want := f.DrainView()
			if want.Refusal == "" || want.Narrowed {
				t.Fatalf("fixture: DrainView = %+v", want)
			}
			for _, touch := range [][]uint64{nil, {0}, {1}} {
				if got := f.DrainViewFor(touchingUpdate(t, f, touch...)); !reflect.DeepEqual(got, want) {
					t.Fatalf("touching %v: DrainViewFor = %+v, want DrainView %+v", touch, got, want)
				}
			}
			rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, touchingUpdate(t, f)), want.Refusal)
		})
	}
}
```

- [ ] **Step 3: Run them and see them fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./fsm/ -run 'TestTouchedTracks|TestNarrowedDrain|TestAnUpdateThatTouchesNoTrack|TestDrainViewFor|TestDrainViewKeepsTheStage2Refusal' -count=1`
Expected: the test binary does not build, with (the compiler stops after ten errors):

```text
# github.com/sentioxyz/arbiter/fsm [github.com/sentioxyz/arbiter/fsm.test]
fsm/drain_narrowed_test.go:154:13: undefined: TouchedTracks
fsm/drain_narrowed_test.go:209:20: f.floorHeldUnsafePartsLocked undefined (type *FSM has no field or method floorHeldUnsafePartsLocked)
fsm/drain_narrowed_test.go:258:16: f.floorHeldUnsafePartsLocked undefined (type *FSM has no field or method floorHeldUnsafePartsLocked)
fsm/drain_narrowed_test.go:279:12: f.DrainViewFor undefined (type *FSM has no field or method DrainViewFor)
fsm/drain_narrowed_test.go:335:13: f.DrainViewFor undefined (type *FSM has no field or method DrainViewFor)
fsm/drain_narrowed_test.go:412:16: f.DrainViewFor undefined (type *FSM has no field or method DrainViewFor)
fsm/drain_narrowed_test.go:437:95: stage2.Narrowed undefined (type DrainView has no field or method Narrowed)
fsm/drain_narrowed_test.go:438:20: stage2.Tracks[0].Touched undefined (type TrackDrainView has no field or method Touched)
fsm/drain_narrowed_test.go:438:48: stage2.Tracks[1].Touched undefined (type TrackDrainView has no field or method Touched)
fsm/drain_narrowed_test.go:443:21: want.Narrowed undefined (type DrainView has no field or method Narrowed)
fsm/drain_narrowed_test.go:443:21: too many errors
FAIL	github.com/sentioxyz/arbiter/fsm [build failed]
FAIL
```

- [ ] **Step 4: Create `fsm/drain_narrowed.go`**

Create `fsm/drain_narrowed.go`:

```go
package fsm

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/sentioxyz/arbiter-core"
)

// The narrowed drain gate (housegate spec 2026-10-14 §10.1, plan S3-E4
// contract §54, switch 9 of §10.4). Once narrowed_drain is committed, a
// consensus update is refused for drain only for the in-flight work of the
// tracks it touches: a track whose si_indexers entry it adds or changes,
// because that track's acknowledgements would then be verified under another
// signer. A promoted unsafe part whose cleanup only the copy floor holds is
// not in-flight work. The switch is read from the committed parameters, so
// every update up to and including the one that turns it on meets the
// stage-2 gate (drainGateRefusalLocked), which this file leaves unchanged.

// TouchedTracks lists, ascending, the indexer ids whose si_indexers entry next
// adds or changes against previous (spec §10.1): an enrolment or a signer
// rotation. verifiers, stage3, authority and max_writers changes touch none.
// An entry counts as changed when it is not reflect.DeepEqual to previous's
// entry of its id; a nil next stands for the committed list and touches
// none. Removing an entry is refused before the drain gate
// (verifySIIndexersTransition), so it is not a touch.
func TouchedTracks(previous, next []arbiter.SIIndexerEntry) []uint64 {
	var touched []uint64
	for _, e := range next {
		i := slices.IndexFunc(previous, func(p arbiter.SIIndexerEntry) bool { return p.IndexerID == e.IndexerID })
		if i < 0 || !reflect.DeepEqual(previous[i], e) {
			touched = append(touched, e.IndexerID)
		}
	}
	slices.Sort(touched)
	return slices.Compact(touched)
}

// narrowedDrainOnLocked reports the committed narrowed_drain switch. The
// caller holds f.mu.
func (f *FSM) narrowedDrainOnLocked() bool {
	return f.stage3SwitchOnLocked(Stage3SwitchNarrowedDrain)
}

// drainRefusalForUpdateLocked is Apply's drain gate for an update whose
// si_indexers list is next: the narrowed gate over the tracks it touches
// against the committed list once narrowed_drain is committed, else the
// stage-2 gate. The caller holds f.mu.
func (f *FSM) drainRefusalForUpdateLocked(next []arbiter.SIIndexerEntry) string {
	if !f.narrowedDrainOnLocked() {
		return f.drainGateRefusalLocked()
	}
	return f.narrowedDrainRefusalLocked(TouchedTracks(f.st.Params.SIIndexers, next))
}

// narrowedDrainRefusalLocked is the narrowed gate over the touched tracks,
// ascending. It skips a quarantined touched track, as the stage-2 gate skips
// a quarantined track's work (spec 2026-10-10 §7.4), and checks each other
// one in the stage-2 order: an unacknowledged promotion, a scheduled cleanup
// (dispatched or not), a promoted unsafe part that waits for anything but the
// copy floor, a rejected partition (clarification C4). Work resolves its
// track as in the stage-2 gate (a promotion and its cleanup through
// promotionTrackLocked, a part through manifestTableOwnerLocked, a rejected
// partition through its block), and work whose track does not resolve counts
// for every touched track. Each check only asks whether any matching work
// exists, so the answer does not depend on map order. The caller holds f.mu.
func (f *FSM) narrowedDrainRefusalLocked(touched []uint64) string {
	var floorHeld map[arbiter.TablePartition]map[string]bool
	floorHeldRead := false
	for _, id := range touched {
		if f.indexerQuarantinedLocked(id) {
			continue
		}
		counts := func(track uint64, ok bool) bool { return !ok || track == id }
		for seq, pending := range f.st.PendingPromotions {
			if pending != nil && !pending.Acked && counts(f.promotionTrackLocked(seq)) {
				return fmt.Sprintf("consensus update touches track %d, which has an unacknowledged promotion", id)
			}
		}
		for seq := range f.st.PendingCleanups {
			if counts(f.promotionTrackLocked(seq)) {
				return fmt.Sprintf("consensus update touches track %d, which has an unacknowledged cleanup command", id)
			}
		}
		for tp, parts := range f.st.PromotedUnsafe {
			if !counts(f.manifestTableOwnerLocked(tp.TableID)) {
				continue
			}
			for part, pending := range parts {
				if !pending {
					continue
				}
				if !floorHeldRead {
					floorHeld, floorHeldRead = f.floorHeldUnsafePartsLocked(), true
				}
				if !floorHeld[tp][part] {
					return fmt.Sprintf("consensus update touches track %d, which has a promoted unsafe part waiting for something other than the copy floor", id)
				}
			}
		}
		if f.st.Tracks != nil {
			for _, rp := range f.st.Tracks.RejectedParts {
				if counts(f.blockTrackLocked(rp.BlockSeq)) {
					return fmt.Sprintf("consensus update touches track %d, which has an unacknowledged rejected-part cleanup", id)
				}
			}
		}
	}
	return ""
}

// floorHeldUnsafePartsLocked lists, per table partition, the promoted unsafe
// parts whose cleanup only the copy floor holds (spec §10.1): the parts still
// in PromotedUnsafe of an acknowledged promotion without a pending cleanup
// whose floor verdict is FloorOnly. A part that another acknowledged
// promotion without a pending cleanup also lists, one the floor does not hold
// alone, stays in flight. Nil while the floor is inactive (before the
// checkpoints switch). The caller holds f.mu.
func (f *FSM) floorHeldUnsafePartsLocked() map[arbiter.TablePartition]map[string]bool {
	if !f.floorActiveLocked() {
		return nil
	}
	held, waiting := map[arbiter.TablePartition]map[string]bool{}, map[arbiter.TablePartition]map[string]bool{}
	for seq, pp := range f.st.PendingPromotions {
		if pp == nil || !pp.Acked || f.st.PendingCleanups[seq] != nil {
			continue
		}
		tp := arbiter.TablePartition{TableID: pp.Promote.TableID, PartitionID: pp.Promote.PartitionID}
		reg := f.st.PromotedUnsafe[tp]
		if len(reg) == 0 {
			continue
		}
		into := waiting
		if f.floorLocked(pp).FloorOnly {
			into = held
		}
		for _, part := range pp.Promote.CandidateParts {
			if !reg[part.PartRowLtHash] {
				continue
			}
			if into[tp] == nil {
				into[tp] = map[string]bool{}
			}
			into[tp][part.PartRowLtHash] = true
		}
	}
	for tp, parts := range waiting {
		for part := range parts {
			delete(held[tp], part)
		}
		if len(held[tp]) == 0 {
			delete(held, tp)
		}
	}
	return held
}

// DrainViewFor is the drain state a consensus hold judges draft by (plan
// S3-E4 contract §54). Refusal is exactly the refusal Apply's drain gate
// gives draft on this state (drainRefusalForUpdateLocked of its
// si_indexers). Once narrowed_drain is committed the view is Narrowed, each
// track draft touches is Touched, and the in-flight blocks leave out the
// promotions whose remaining parts only the copy floor holds. Before the
// switch it is DrainView, whatever the draft. The view is detached and the
// read never writes.
func (f *FSM) DrainViewFor(draft arbiter.ConsensusParamsUpdate) DrainView {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.narrowedDrainOnLocked() {
		return f.drainViewLocked(f.drainGateRefusalLocked(), nil)
	}
	v := f.drainViewLocked(f.drainRefusalForUpdateLocked(draft.SIIndexers), f.floorHeldUnsafePartsLocked())
	v.Narrowed = true
	touched := TouchedTracks(f.st.Params.SIIndexers, draft.SIIndexers)
	for i := range v.Tracks {
		v.Tracks[i].Touched = slices.Contains(touched, v.Tracks[i].IndexerID)
	}
	return v
}
```

- [ ] **Step 5: Narrow the drain views in `fsm/reads_drain.go`**

Apply the six replacements below, in this order.

In `fsm/reads_drain.go` (`DrainView`, `:29-32`), replace

```go
	// Tracks has one entry per enrolled indexer, sorted by id, after the
	// tracks activation; nil before it.
	Tracks []TrackDrainView
}
```

with

```go
	// Tracks has one entry per enrolled indexer, sorted by id, after the
	// tracks activation; nil before it.
	Tracks []TrackDrainView
	// Narrowed is true only in DrainViewFor's view once the narrowed_drain
	// switch is committed (plan S3-E4 contract §54): Refusal is then the
	// narrowed gate's for that view's draft, and the in-flight blocks leave
	// out promotions whose remaining parts only the copy floor holds.
	// DrainView never sets it.
	Narrowed bool
}
```

In `fsm/reads_drain.go` (`TrackDrainView`, `:38-44`), replace

```go
type TrackDrainView struct {
	IndexerID      uint64
	Quarantined    bool
	Prefix         uint64
	Frontier       uint64
	InFlightBlocks []uint64
}
```

with

```go
type TrackDrainView struct {
	IndexerID      uint64
	Quarantined    bool
	Prefix         uint64
	Frontier       uint64
	InFlightBlocks []uint64
	// Touched marks, in a Narrowed view, a track whose si_indexers entry the
	// view's draft adds or changes (TouchedTracks); false otherwise.
	Touched bool
}
```

In `fsm/reads_drain.go` (`DrainView()`, `:46-60`), replace

```go
// DrainView reads the drain state under the read lock. The view is detached
// and the read never writes.
func (f *FSM) DrainView() DrainView {
	f.mu.RLock()
	defer f.mu.RUnlock()

	prefix := safePrefixLocked(f)
	v := DrainView{
		PromotionSeq:   f.st.PromotionSeq,
		Refusal:        f.drainGateRefusalLocked(),
		TracksActive:   f.tracksActiveLocked(),
		GlobalPrefix:   prefix,
		GlobalFrontier: f.globalPromotionFrontierLocked(prefix),
	}
	all, byTrack := f.drainWorkBlocksLocked()
```

with

```go
// DrainView reads the drain state under the read lock: Refusal is the
// stage-2 gate's, also after the narrowed_drain switch (DrainViewFor judges a
// draft). The view is detached and the read never writes.
func (f *FSM) DrainView() DrainView {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.drainViewLocked(f.drainGateRefusalLocked(), nil)
}

// drainViewLocked is the drain state with refusal; its in-flight blocks leave
// out the promotions whose remaining parts are all in floorHeld (nil: none).
// The caller holds f.mu.
func (f *FSM) drainViewLocked(refusal string, floorHeld map[arbiter.TablePartition]map[string]bool) DrainView {
	prefix := safePrefixLocked(f)
	v := DrainView{
		PromotionSeq:   f.st.PromotionSeq,
		Refusal:        refusal,
		TracksActive:   f.tracksActiveLocked(),
		GlobalPrefix:   prefix,
		GlobalFrontier: f.globalPromotionFrontierLocked(prefix),
	}
	all, byTrack := f.drainWorkBlocksLocked(floorHeld)
```

In `fsm/reads_drain.go` (`drainWorkBlocksLocked`, `:90-92`), replace

```go
// drainWorkBlocksLocked collects the blocks whose promotion work holds the
// drain gate: every block, and per track for work whose track resolves.
func (f *FSM) drainWorkBlocksLocked() (all map[uint64]bool, byTrack map[uint64]map[uint64]bool) {
```

with

```go
// drainWorkBlocksLocked collects the blocks whose promotion work holds the
// drain gate: every block, and per track for work whose track resolves. A
// promotion whose remaining work is only parts in floorHeld holds nothing
// (DrainViewFor's narrowed view; nil before the switch).
func (f *FSM) drainWorkBlocksLocked(floorHeld map[arbiter.TablePartition]map[string]bool) (all map[uint64]bool, byTrack map[uint64]map[uint64]bool) {
```

In `fsm/reads_drain.go` (its promotion loop, `:107-110`), replace

```go
	for seq, pp := range f.st.PendingPromotions {
		if pp == nil || !f.promotionHoldsDrainLocked(seq, pp) {
			continue
		}
```

with

```go
	for seq, pp := range f.st.PendingPromotions {
		if pp == nil || !f.promotionHoldsDrainLocked(seq, pp) || f.promotionFloorHeldLocked(seq, pp, floorHeld) {
			continue
		}
```

In `fsm/reads_drain.go` (before `sortedBlocks`, `:139`), replace

```go
// sortedBlocks lists set's blocks ascending; nil when it is empty.
```

with

```go
// promotionFloorHeldLocked reports whether acknowledged promotion seq has no
// pending cleanup and every part of it still in PromotedUnsafe is in
// floorHeld, so that only the copy floor holds its remaining work.
func (f *FSM) promotionFloorHeldLocked(seq uint64, pp *PendingPromotion, floorHeld map[arbiter.TablePartition]map[string]bool) bool {
	if len(floorHeld) == 0 || !pp.Acked || f.st.PendingCleanups[seq] != nil {
		return false
	}
	tp := arbiter.TablePartition{TableID: pp.Promote.TableID, PartitionID: pp.Promote.PartitionID}
	reg := f.st.PromotedUnsafe[tp]
	for _, part := range pp.Promote.CandidateParts {
		if reg[part.PartRowLtHash] && !floorHeld[tp][part.PartRowLtHash] {
			return false
		}
	}
	return true
}

// sortedBlocks lists set's blocks ascending; nil when it is empty.
```

- [ ] **Step 6: Call the narrowed gate from Apply in `fsm/consensus_updates.go`**

In `fsm/consensus_updates.go`, `applyUpdateConsensusParams` (`:74-78`), replace

```go
	// The drain gate; after the tracks activation a quarantined track's work
	// does not count (spec 2026-10-10 §7.4).
	if reason := f.drainGateRefusalLocked(); reason != "" {
		return Rejected{Reason: reason}
	}
```

with

```go
	// The drain gate; after the tracks activation a quarantined track's work
	// does not count (spec 2026-10-10 §7.4), and once narrowed_drain is
	// committed only the in-flight work of the tracks this update touches
	// does (housegate spec 2026-10-14 §10.1, plan S3-E4 contract §54).
	if reason := f.drainRefusalForUpdateLocked(next.SIIndexers); reason != "" {
		return Rejected{Reason: reason}
	}
```

Task 76's raise-k refusal follows the tracks-activation refusal right after this block; it stays where it is.

- [ ] **Step 7: Add the `narrowed_drain` case to `onStage3SwitchOnLocked` in `fsm/stage3.go`**

In `fsm/stage3.go`, inside `onStage3SwitchOnLocked`, insert this case right before the switch's `default:` line, after every case the earlier tasks added (spec §10.4 order puts `narrowed_drain` last):

```go
	case Stage3SwitchNarrowedDrain:
		// Nothing to install: the drain gate reads the committed switch
		// (drainRefusalForUpdateLocked), so the update that turns it on was
		// judged by the stage-2 gate and every later one is narrowed (plan
		// S3-E4 contract §54).
```

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && grep -c 'Nothing to install: the drain gate reads the committed switch' fsm/stage3.go`
Expected: `1`.

- [ ] **Step 8: Run the fsm tests and see them pass**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test -z "$(gofmt -l fsm)" && bazel run //:gazelle && git -C "$W" status --porcelain`
Expected: gofmt lists nothing (if it lists a file, run `gofmt -w` on it and repeat); gazelle adds `"drain_narrowed.go"` to the `srcs` of `//fsm:fsm` and `"drain_narrowed_test.go"` to the `srcs` of `//fsm:fsm_test`, in sorted position, and no dependency; the status lists exactly ` M fsm/BUILD.bazel`, ` M fsm/consensus_updates.go`, ` M fsm/reads_drain.go`, ` M fsm/stage3.go`, `?? fsm/drain_narrowed.go` and `?? fsm/drain_narrowed_test.go`.

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./fsm/ -run 'TestTouchedTracks|TestNarrowedDrain|TestAnUpdateThatTouchesNoTrack|TestDrainViewFor|TestDrainViewKeepsTheStage2Refusal' -race -count=1 -v | grep -E '^--- |^ok|^FAIL'`
Expected (timings omitted):

```text
--- PASS: TestTouchedTracksAreTheChangedSIIndexersEntries
--- PASS: TestNarrowedDrainGateRefusesOnlyTouchedTracksWork
--- PASS: TestNarrowedDrainGateIgnoresFloorOnlyHeldParts
--- PASS: TestNarrowedDrainGateCountsPartsHeldForAnotherReason
--- PASS: TestNarrowedDrainSwitchOnIsJudgedByTheStage2Gate
--- PASS: TestNarrowedDrainSkipsQuarantinedTouchedTracks
--- PASS: TestAnUpdateThatTouchesNoTrackPassesWithInFlightWork
--- PASS: TestDrainViewForEqualsApplysDrainRefusal
--- PASS: TestDrainViewKeepsTheStage2Refusal
--- PASS: TestDrainViewForBeforeTheSwitchIsTheStage2Gate
ok  	github.com/sentioxyz/arbiter/fsm
```

- [ ] **Step 9: Run the fsm checks**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go vet ./fsm/ && go test -race ./fsm/ -count=1 && go test ./fsm/ -run 'TestDrainView|TestPreActivationHistoryIsByteIdentical|TestL3CommitmentPreimagesAreFrozen|TestL3TrackHeaderPreimagesAreFrozen|TestL3TransitionHeaderPreimageIsFrozen|TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease|TestDeterminism' -count=1 -v | grep -E '^--- (PASS|FAIL)'`
Expected: vet prints nothing; `ok` for `fsm` with no data race; then only `--- PASS` lines, among them Plan S3-A's `TestDrainViewBeforeTheTracksActivation`, `TestDrainViewReportsInFlightBlocksPerTrack`, `TestDrainViewRefusalEqualsTheDrainGate` and `TestDrainViewOfAQuarantinedTrack` (unchanged), this task's three `TestDrainViewFor…`/`TestDrainViewKeeps…` tests, `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, `TestPreActivationHistoryIsByteIdenticalAcrossTracks`, `TestL3CommitmentPreimagesAreFrozen`, `TestL3TrackHeaderPreimagesAreFrozen`, `TestL3TransitionHeaderPreimageIsFrozen`, `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease` and every `TestDeterminism_*`, and no `--- FAIL`.

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && grep -n -e 'arbiter-proto' -e 'time\.' fsm/drain_narrowed.go; git -C "$W" status --porcelain -- fsm/testdata`
Expected: nothing, then nothing (no proto import, no clock, no golden file changed).

- [ ] **Step 10: Commit the fsm half**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" add fsm/drain_narrowed.go fsm/drain_narrowed_test.go fsm/reads_drain.go fsm/consensus_updates.go fsm/stage3.go fsm/BUILD.bazel
git -C "$W" commit -m "feat(stage3): narrowed drain gate and DrainViewFor, switch 9 (Task 77)

Once the stage3 narrowed_drain switch is committed, Apply refuses a
consensus update for drain only for the in-flight work of the tracks
whose si_indexers entry it adds or changes (TouchedTracks): per touched
track in ascending order, skipping a quarantined one, an
unacknowledged promotion, a scheduled cleanup, a promoted unsafe part
that waits for anything but the copy floor, or a rejected partition.
Parts whose cleanup only the floor holds are not in flight. The gate
reads the committed switch, so the update that turns it on meets the
stage-2 gate. DrainViewFor(draft) answers exactly Apply's refusal for
the draft, with Narrowed and Touched and without floor-held work in
flight; DrainView keeps the stage-2 answer (housegate spec 2026-10-14
§10.1, §10.4; plan S3-E4 contract §54). With the switch off nothing
changes.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C "$W" status --porcelain
```

Expected: one new commit whose subject is `feat(stage3): narrowed drain gate and DrainViewFor, switch 9 (Task 77)`, and an empty status.

- [ ] **Step 11: Write the failing server tests**

In `server/consensus_hold_test.go` (the standard imports), replace

```go
import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
```

with

```go
import (
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"
```

In `server/consensus_hold_test.go` (the arbiter-core imports), replace

```go
	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
```

with

```go
	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
```

and append to the end of `server/consensus_hold_test.go`:

```go
// narrowedHoldTracks compares a hold's tracks with want, entry by entry.
func narrowedHoldTracks(t *testing.T, got, want []*pb.TrackDrain) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("hold tracks %v, want %v", got, want)
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("hold track %d = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestIssuanceHoldIsReadyForAnUpdateThatTouchesNoTrack: plan S3-E4 K79. Over
// a narrowed view an issuance hold for an update that touches no track is
// ready at once although a track has work in flight, because the hold
// judges its own draft; it still refuses every new promotion.
func TestIssuanceHoldIsReadyForAnUpdateThatTouchesNoTrack(t *testing.T) {
	fx := newHoldFixture(t, nil)
	var drafts []arbiter.ConsensusParamsUpdate
	fx.s.holdViewFor = func(draft arbiter.ConsensusParamsUpdate) fsm.DrainView {
		drafts = append(drafts, draft)
		return fsm.DrainView{Narrowed: true, TracksActive: true, GlobalPrefix: 4, GlobalFrontier: 5, InFlightBlocks: []uint64{5},
			Tracks: []fsm.TrackDrainView{{IndexerID: 1, Prefix: 4, Frontier: 5, InFlightBlocks: []uint64{5}}}}
	}
	draft := fx.draft(t, 2)
	hold, err := fx.prepare(t.Context(), issuanceMode, draft, 0)
	if err != nil || !hold.GetReady() || hold.GetDrainRefusal() != "" || hold.GetPromotionSeq() != draft.GetUpdate().GetExpectedPromotionSeq() ||
		len(hold.GetTracks()) != 0 || len(hold.GetInFlightBlocks()) != 0 {
		t.Fatalf("issuance hold = %v, %v; want ready at once with no drain state", hold, err)
	}
	want, err := authority.NormalizeConsensusParamsUpdate(wire.ConsensusParamsUpdateFromPB(draft.GetUpdate()))
	if err != nil {
		t.Fatal(err)
	}
	if len(drafts) != 1 || !reflect.DeepEqual(drafts[0], want) {
		t.Fatalf("the view was read for %+v, want the draft %+v", drafts, want)
	}
	if held, reason := fx.begin(holdWork(5, &fsm.L3BlockTrack{IndexerID: 1}, "p0")); !held || reason == "" {
		t.Fatalf("a ready issuance hold admitted a promotion (held=%v, %q)", held, reason)
	}
}

// TestHoldReadinessFollowsTheDraft: plan S3-E4 contract §54. A hold keeps
// the normalized draft it was taken with and every read judges readiness and
// drain_refusal by it: the leader's view refuses a draft that rotates
// indexer 2's signer while indexer 2 has work in flight, so the hold over
// that draft stays not ready, whatever an update touching nothing would get,
// until the work is acknowledged.
func TestHoldReadinessFollowsTheDraft(t *testing.T) {
	fx := newHoldFixture(t, nil)
	activateServerTracks(t, fx.node.fakeNode)
	params, err := fx.node.f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	const refusal = "consensus update touches track 2, which has an unacknowledged promotion"
	inFlight := true
	var drafts []arbiter.ConsensusParamsUpdate
	fx.s.holdViewFor = func(draft arbiter.ConsensusParamsUpdate) fsm.DrainView {
		drafts = append(drafts, draft)
		v := fsm.DrainView{Narrowed: true, TracksActive: true}
		if inFlight && slices.Contains(fsm.TouchedTracks(params.Current.SIIndexers, draft.SIIndexers), 2) {
			v.Refusal = refusal
		}
		return v
	}
	rotated, err := authority.NewSignerFromHex("0000000000000000000000000000000000000000000000000000000000000c33")
	if err != nil {
		t.Fatal(err)
	}
	// The hold only authorizes its draft, so the rotated entry's enrolment
	// statement is never verified here; Apply would verify it.
	draft := serverUpdateRequest(t, fx.node.fakeNode, func(u *arbiter.ConsensusParamsUpdate) {
		u.MaxWriters = 3
		u.SIIndexers = slices.Clone(u.SIIndexers)
		for i := range u.SIIndexers {
			if u.SIIndexers[i].IndexerID == 2 {
				u.SIIndexers[i].Signer, u.SIIndexers[i].EnrollmentJWS = rotated.Address(), "rotated-enrollment"
			}
		}
	})
	want, err := authority.NormalizeConsensusParamsUpdate(wire.ConsensusParamsUpdateFromPB(draft.GetUpdate()))
	if err != nil {
		t.Fatal(err)
	}
	// The request spells the rotated signer in upper case; the hold keeps the
	// normalized draft.
	for _, e := range draft.GetUpdate().GetSiIndexers() {
		if e.GetIndexerId() == 2 {
			e.Signer = "0x" + strings.ToUpper(strings.TrimPrefix(e.GetSigner(), "0x"))
		}
	}
	hold, err := fx.prepare(t.Context(), issuanceMode, draft, 0)
	if err != nil || hold.GetReady() || hold.GetDrainRefusal() != refusal {
		t.Fatalf("issuance hold over the rotation = %v, %v; want not ready with %q", hold, err, refusal)
	}
	get := func() *pb.ConsensusHold {
		t.Helper()
		got, err := fx.svc.GetConsensusHold(t.Context(), &pb.ConsensusHoldRef{HoldId: hold.GetHoldId()})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := get(); got.GetReady() || got.GetDrainRefusal() != refusal {
		t.Fatalf("hold while indexer 2's work is in flight = %v", got)
	}
	inFlight = false
	if got := get(); !got.GetReady() || got.GetDrainRefusal() != "" {
		t.Fatalf("hold once indexer 2's work is acknowledged = %v", got)
	}
	if len(drafts) != 3 {
		t.Fatalf("the view was read %d times, want 3 (prepare and two reads)", len(drafts))
	}
	for i, d := range drafts {
		if !reflect.DeepEqual(d, want) {
			t.Fatalf("read %d judged %+v, want the normalized draft %+v", i, d, want)
		}
	}
}

// TestDrainHoldRecordsOnlyTouchedTracksAfterTheNarrowedDrainSwitch: spec
// §10.2 and plan S3-E4 contract §54. Over a narrowed view a drain hold
// records the frontier of the touched tracks only: an untouched track
// records none, so its promotions are held for the hold's life and its
// prefix never blocks readiness, and it reports frontier 0 with nothing in
// flight. Over a view that is not narrowed every non-quarantined track is
// recorded, as before the switch.
func TestDrainHoldRecordsOnlyTouchedTracksAfterTheNarrowedDrainSwitch(t *testing.T) {
	fx := newHoldFixture(t, nil)
	view := fsm.DrainView{Narrowed: true, TracksActive: true, Tracks: []fsm.TrackDrainView{
		{IndexerID: 1, Prefix: 3, Frontier: 5, InFlightBlocks: []uint64{5}},
		{IndexerID: 2, Prefix: 4, Frontier: 6, InFlightBlocks: []uint64{6}, Touched: true},
	}}
	fx.s.holdViewFor = func(arbiter.ConsensusParamsUpdate) fsm.DrainView { return view }
	hold, err := fx.prepare(t.Context(), drainMode, fx.draft(t, 2), 0)
	if err != nil || hold.GetReady() {
		t.Fatalf("drain hold = %v, %v; want not ready while track 2 is below its frontier", hold, err)
	}
	narrowedHoldTracks(t, hold.GetTracks(), []*pb.TrackDrain{
		{IndexerId: 1, PrefixBlockSeq: 3},
		{IndexerId: 2, PrefixBlockSeq: 4, FrontierBlockSeq: 6, InFlightBlocks: []uint64{6}},
	})
	one, two := &fsm.L3BlockTrack{IndexerID: 1}, &fsm.L3BlockTrack{IndexerID: 2}
	for _, tc := range []struct {
		work fsm.PromotionWork
		held bool
	}{
		{holdWork(5, one, "p0"), true},
		{holdWork(6, two, "p0"), false},
		{holdWork(7, two, "p0"), true},
	} {
		if held, _ := fx.begin(tc.work); held != tc.held {
			t.Fatalf("%+v: held=%v, want %v", tc.work, held, tc.held)
		}
	}
	view.Tracks[1] = fsm.TrackDrainView{IndexerID: 2, Prefix: 6, Frontier: 7, InFlightBlocks: []uint64{7}, Touched: true}
	got, err := fx.svc.GetConsensusHold(t.Context(), &pb.ConsensusHoldRef{HoldId: hold.GetHoldId()})
	if err != nil || !got.GetReady() {
		t.Fatalf("hold once track 2 reached its frontier = %v, %v; want ready although track 1 never drained", got, err)
	}
	narrowedHoldTracks(t, got.GetTracks(), []*pb.TrackDrain{{IndexerId: 1, PrefixBlockSeq: 3}, {IndexerId: 2, PrefixBlockSeq: 6, FrontierBlockSeq: 6}})
	if _, err := fx.svc.ReleaseConsensusHold(t.Context(), &pb.ConsensusHoldRef{HoldId: hold.GetHoldId()}); err != nil {
		t.Fatal(err)
	}
	view.Narrowed = false
	view.Tracks[1] = fsm.TrackDrainView{IndexerID: 2, Prefix: 4, Frontier: 6, InFlightBlocks: []uint64{6}, Touched: true}
	again, err := fx.prepare(t.Context(), drainMode, fx.draft(t, 2), 0)
	if err != nil {
		t.Fatal(err)
	}
	narrowedHoldTracks(t, again.GetTracks(), []*pb.TrackDrain{
		{IndexerId: 1, PrefixBlockSeq: 3, FrontierBlockSeq: 5, InFlightBlocks: []uint64{5}},
		{IndexerId: 2, PrefixBlockSeq: 4, FrontierBlockSeq: 6, InFlightBlocks: []uint64{6}},
	})
}
```

- [ ] **Step 12: Run them and see them fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./server/ -run 'TestIssuanceHoldIsReadyForAnUpdateThatTouchesNoTrack|TestHoldReadinessFollowsTheDraft|TestDrainHoldRecordsOnlyTouchedTracksAfterTheNarrowedDrainSwitch' -count=1`
Expected: the test binary does not build. The compiler reports `fx.s.holdViewFor undefined (type *Server has no field or method holdViewFor)` three times, once per new test, at column 7 of its seam assignment in `server/consensus_hold_test.go`, then `FAIL	github.com/sentioxyz/arbiter/server [build failed]`.

- [ ] **Step 13: Add the draft-aware seam in `server/server.go`**

In `server/server.go` (the import block, `:18-20`), replace

```go
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/raftnode"
```

with

```go
	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/raftnode"
```

In `server/server.go` (`Server`, `:134-137`), replace

```go
	// holdView reads the drain state a hold records and reports; nil reads
	// FSM.DrainView. Only tests set it.
	holdView func() fsm.DrainView
}
```

with

```go
	// holdView reads the drain state a hold records and reports; nil reads
	// FSM.DrainView. Only tests set it.
	holdView func() fsm.DrainView
	// holdViewFor is holdView for one draft, read first when set; nil reads
	// FSM.DrainViewFor (drainViewFor). Only tests set it.
	holdViewFor func(draft arbiter.ConsensusParamsUpdate) fsm.DrainView
}
```

- [ ] **Step 14: Keep the draft in the hold in `server/consensus_hold.go`**

In `server/consensus_hold.go` (the import block, `:13-18`), replace

```go
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter/fsm"
)
```

with

```go
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter/fsm"
)
```

In `server/consensus_hold.go` (`consensusHold`, `:95-104`), replace

```go
// consensusHold is one granted hold. A drain hold taken after the tracks
// activation records each non-quarantined track's promotion frontier; one
// taken before it, the global frontier.
type consensusHold struct {
	id             string
	mode           pb.ConsensusHoldMode
	term           uint64
	promotionSeq   uint64
	draftEpoch     uint64
	deadline       time.Time
```

with

```go
// consensusHold is one granted hold. A drain hold taken after the tracks
// activation records each non-quarantined track's promotion frontier, and
// after the narrowed_drain switch only each touched one's (plan S3-E4
// contract §54); one taken before it, the global frontier. draft is the
// normalized draft the hold was taken with: every read judges readiness and
// drain_refusal by it (drainViewFor). It is never mutated after start, so
// snapshots share it.
type consensusHold struct {
	id             string
	mode           pb.ConsensusHoldMode
	term           uint64
	promotionSeq   uint64
	draftEpoch     uint64
	draft          arbiter.ConsensusParamsUpdate
	deadline       time.Time
```

In `server/consensus_hold.go` (`start`, `:208-224`), replace

```go
// start installs a hold of mode in term for timeout, taken over view and
// authorized by a draft signed at draftEpoch. The caller holds the lease
// exclusively and read view after a barrier.
func (h *ConsensusHolds) start(mode pb.ConsensusHoldMode, term, draftEpoch uint64, timeout time.Duration, view fsm.DrainView) (consensusHold, error) {
	id, err := h.newID()
	if err != nil {
		return consensusHold{}, status.Errorf(codes.Internal, "consensus hold id: %v", err)
	}
	hold := &consensusHold{id: id, mode: mode, term: term, promotionSeq: view.PromotionSeq, draftEpoch: draftEpoch, deadline: h.now().Add(timeout), tracks: view.TracksActive}
	if mode == pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_DRAIN {
		if view.TracksActive {
			hold.frontiers = make(map[uint64]uint64, len(view.Tracks))
			for _, tr := range view.Tracks {
				if !tr.Quarantined {
					hold.frontiers[tr.IndexerID] = tr.Frontier
				}
			}
```

with

```go
// start installs a hold of mode in term for timeout, taken over view and
// authorized by draft, normalized, which was signed at draft.ExpectedEpoch.
// The caller holds the lease exclusively and read view, drainViewFor(draft),
// after a barrier. Over a Narrowed view a drain hold records only the touched
// tracks' frontiers: the narrowed gate ignores every other track's work, and
// the other tracks' promotions are held for the hold's life (they have no
// recorded frontier), so promotion_seq still stops moving once it is ready.
func (h *ConsensusHolds) start(mode pb.ConsensusHoldMode, term uint64, timeout time.Duration, draft arbiter.ConsensusParamsUpdate, view fsm.DrainView) (consensusHold, error) {
	id, err := h.newID()
	if err != nil {
		return consensusHold{}, status.Errorf(codes.Internal, "consensus hold id: %v", err)
	}
	hold := &consensusHold{id: id, mode: mode, term: term, promotionSeq: view.PromotionSeq, draftEpoch: draft.ExpectedEpoch, draft: draft,
		deadline: h.now().Add(timeout), tracks: view.TracksActive}
	if mode == pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_DRAIN {
		if view.TracksActive {
			hold.frontiers = make(map[uint64]uint64, len(view.Tracks))
			for _, tr := range view.Tracks {
				if !tr.Quarantined && (!view.Narrowed || tr.Touched) {
					hold.frontiers[tr.IndexerID] = tr.Frontier
				}
			}
```

- [ ] **Step 15: Judge every read by the hold's draft in `server/consensus_hold_rpc.go`**

In `server/consensus_hold_rpc.go` (the import block, `:13-15`), replace

```go
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
)
```

with

```go
	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
)
```

In `server/consensus_hold_rpc.go` (`PrepareConsensusUpdate`, `:37-40`), replace

```go
	draft := wire.ConsensusParamsUpdateFromPB(req.GetDraft().GetUpdate())
	if err := s.authorizeConsensusParamsUpdate(ctx, draft, req.GetDraft().GetAuthorityJws()); err != nil {
		return nil, err
	}
```

with

```go
	draft := wire.ConsensusParamsUpdateFromPB(req.GetDraft().GetUpdate())
	if err := s.authorizeConsensusParamsUpdate(ctx, draft, req.GetDraft().GetAuthorityJws()); err != nil {
		return nil, err
	}
	// The hold judges readiness on the draft as Apply normalizes it (plan
	// S3-E4 contract §54). The authorization above hashed the normalized
	// draft, so this cannot fail on a draft that reached here.
	draft, err := authority.NormalizeConsensusParamsUpdate(draft)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "consensus hold draft: %v", err)
	}
```

In `server/consensus_hold_rpc.go` (`PrepareConsensusUpdate`, `:52-56`), replace

```go
	view := s.drainView()
	if mode == pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_ISSUANCE && view.PromotionSeq != draft.ExpectedPromotionSeq {
		return nil, status.Errorf(codes.FailedPrecondition, "promotion_seq moved to %d while the hold was taken; sign the update again", view.PromotionSeq)
	}
	hold, err := s.holds.start(mode, s.raftTerm(), draft.ExpectedEpoch, s.consensusHoldTimeout(mode, req.GetTimeoutMs()), view)
```

with

```go
	view := s.drainViewFor(draft)
	if mode == pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_ISSUANCE && view.PromotionSeq != draft.ExpectedPromotionSeq {
		return nil, status.Errorf(codes.FailedPrecondition, "promotion_seq moved to %d while the hold was taken; sign the update again", view.PromotionSeq)
	}
	hold, err := s.holds.start(mode, s.raftTerm(), s.consensusHoldTimeout(mode, req.GetTimeoutMs()), draft, view)
```

In `server/consensus_hold_rpc.go` (`GetConsensusHold`, `:89-90`), replace

```go
	return consensusHoldToPB(hold, svc.s.drainView()), nil
}
```

with

```go
	return consensusHoldToPB(hold, svc.s.drainViewFor(hold.draft)), nil
}
```

In `server/consensus_hold_rpc.go` (`drainView`, `:122-127`), replace

```go
func (s *Server) drainView() fsm.DrainView {
	if s.holdView != nil {
		return s.holdView()
	}
	return s.d.FSM.DrainView()
}
```

with

```go
func (s *Server) drainView() fsm.DrainView {
	if s.holdView != nil {
		return s.holdView()
	}
	return s.d.FSM.DrainView()
}

// drainViewFor is the drain state a hold over draft records and reports: the
// FSM's answer for that draft (fsm.DrainViewFor), which is DrainView until
// the narrowed_drain switch and the narrowed gate's from it on. The test
// seams holdViewFor and then holdView take precedence.
func (s *Server) drainViewFor(draft arbiter.ConsensusParamsUpdate) fsm.DrainView {
	switch {
	case s.holdViewFor != nil:
		return s.holdViewFor(draft)
	case s.holdView != nil:
		return s.holdView()
	}
	return s.d.FSM.DrainViewFor(draft)
}
```

- [ ] **Step 16: Run the server tests and see them pass**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test -z "$(gofmt -l server)" && go vet ./server/ && go test ./server/ -run 'TestIssuanceHoldIsReadyForAnUpdateThatTouchesNoTrack|TestHoldReadinessFollowsTheDraft|TestDrainHoldRecordsOnlyTouchedTracksAfterTheNarrowedDrainSwitch' -race -count=1 -v | grep -E '^--- |^ok|^FAIL'`
Expected (timings omitted):

```text
--- PASS: TestIssuanceHoldIsReadyForAnUpdateThatTouchesNoTrack
--- PASS: TestHoldReadinessFollowsTheDraft
--- PASS: TestDrainHoldRecordsOnlyTouchedTracksAfterTheNarrowedDrainSwitch
ok  	github.com/sentioxyz/arbiter/server
```

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test -race ./server/ -count=1 && go test -race ./server/ -run 'Hold|Drain|Issuance' -count=20 && bazel run //:gazelle && git -C "$W" status --porcelain`
Expected: `ok` twice with no data race (every Plan S3-A hold test passes unchanged: none sets `holdViewFor`, and `holdView` keeps its meaning); gazelle changes nothing (no new file, every import is already a dependency); the status lists exactly ` M server/consensus_hold.go`, ` M server/consensus_hold_rpc.go`, ` M server/consensus_hold_test.go` and ` M server/server.go`.

- [ ] **Step 17: Commit the server half**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" add server/server.go server/consensus_hold.go server/consensus_hold_rpc.go server/consensus_hold_test.go
git -C "$W" commit -m "feat(stage3): consensus holds judge the draft they were taken with (Task 77)

PrepareConsensusUpdate keeps the normalized draft in the hold, and the
issuance check, start and every GetConsensusHold read
drainViewFor(draft), the FSM's DrainViewFor: once narrowed_drain is
committed an issuance hold for an update that touches no track is
ready although tracks have work in flight, and a drain hold over a
narrowed view records only the touched tracks' frontiers, holding the
other tracks' promotions while it lives (housegate spec 2026-10-14
§10.1-§10.2; plan S3-E4 contract §54). The tests-only seam
holdViewFor sees the draft. Before the switch nothing changes.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C "$W" status --porcelain
```

Expected: one new commit whose subject is `feat(stage3): consensus holds judge the draft they were taken with (Task 77)`, and an empty status.

- [ ] **Step 18: Write the failing arbiter-admin tests**

In `cmd/arbiter-admin/consensus_hold_test.go` (the standard imports), replace

```go
import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
```

with

```go
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
```

In `cmd/arbiter-admin/consensus_hold_test.go` (the arbiter-core imports), replace

```go
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
```

with

```go
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
```

and append to the end of `cmd/arbiter-admin/consensus_hold_test.go`:

```go
// defaultHoldArgs is a consensus update without --hold, so the CLI picks the
// hold from the leader's committed stage3 (plan S3-E4 K79).
func defaultHoldArgs(addr string, extra ...string) []string {
	return append([]string{"consensus", "update", "--address", addr, "--expected-epoch", "7", "--authority", nextAuthority, "--max-writers", "3"}, extra...)
}

// stage3LeaderState is testConsensusState after the tracks activation with
// indexer 0 enrolled on s1 under founder and stage3 committed with every
// switch through checkpoints on, and narrowed_drain as given.
func stage3LeaderState(t *testing.T, founder *authority.Signer, narrowedDrain bool) *pb.ConsensusParamsState {
	t.Helper()
	jws, err := founder.SignSNodeEnrollment(authority.SNodeEnrollmentStatement{NetworkID: "testnet", GenesisSnapshotID: "genesis-committed", IndexerID: 0, SNodeNodeID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	s := testConsensusState()
	s.Current.SiIndexers = []*pb.SIIndexerEntry{{IndexerId: 0, ActivationBlock: 100, Signer: founder.Address(), SnodeNodeId: "s1", EnrollmentJws: jws}}
	s.Current.TracksEnabled = true
	s.Current.Stage3 = &pb.Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1,
		IndexerFailureDomains: []*pb.IndexerFailureDomain{{IndexerId: 0, FailureDomain: "node:n0"}},
		MaxLagPublications:    8, LagLow: 2, EvidenceTimeoutL2Blocks: 30, MaxUncleanedPartsPerPartition: 2000, MaxUncleanedPartsPerTable: 20000,
		Purge: true, EvictionVoiding: true, OwnerAwareSelection: true, Placement: true, Reselection: true, Checkpoints: true, NarrowedDrain: narrowedDrain}
	return s
}

// readyHoldLeader is a holdLeader answering every GetConsensusParams with a
// copy of state and granting holds that are ready at once at the draft's
// promotion_seq.
func readyHoldLeader(state *pb.ConsensusParamsState) *holdLeader {
	return &holdLeader{
		state: func(int) *pb.ConsensusParamsState { return proto.Clone(state).(*pb.ConsensusParamsState) },
		prepare: func(n int, req *pb.PrepareConsensusUpdateRequest) (*pb.ConsensusHold, error) {
			return &pb.ConsensusHold{HoldId: fmt.Sprintf("hold-%d", n), Mode: req.GetMode(), PromotionSeq: req.GetDraft().GetUpdate().GetExpectedPromotionSeq(), Ready: true}, nil
		},
	}
}

// TestConsensusUpdateDefaultsToIssuanceOnceTheLeaderReportsNarrowedDrain:
// plan S3-E4 K79. Without --hold, once the leader reports
// stage3.narrowed_drain and the update touches no track, the CLI takes an
// issuance hold, says why on stderr and submits the draft it was taken with;
// --hold-timeout applies to the default hold.
func TestConsensusUpdateDefaultsToIssuanceOnceTheLeaderReportsNarrowedDrain(t *testing.T) {
	signer := adminKey(t)
	warnings := captureStderr(t)
	leader := readyHoldLeader(stage3LeaderState(t, authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0), true))
	var out bytes.Buffer
	if err := run(context.Background(), defaultHoldArgs(serveAdminImpl(t, leader), "--hold-timeout", "90s"), &out); err != nil {
		t.Fatal(err)
	}
	v := leader.view()
	if len(v.prepared) != 1 || v.prepared[0].GetMode() != issuanceHold || v.prepared[0].GetTimeoutMs() != 90_000 ||
		len(v.submitted) != 1 || !proto.Equal(v.submitted[0], v.prepared[0].GetDraft()) || !slices.Equal(v.holdIDs[0], []string{"hold-1"}) {
		t.Fatalf("leader saw %+v", v)
	}
	requireSignedBy(t, signer, v.submitted[0])
	if w := warnings.String(); !strings.Contains(w, "info: --hold defaults to issuance: the leader reports stage3.narrowed_drain and the update touches no track\n") {
		t.Fatalf("stderr lacks the default's reason:\n%s", w)
	}
	var result struct{ Status, Hold string }
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || result.Status != "committed" || result.Hold != "issuance" {
		t.Fatalf("result %s, %v", &out, err)
	}
}

// TestConsensusUpdateDefaultsToDrainBeforeNarrowedDrain: plan S3-A K6 holds
// until switch 9. Without --hold a leader that does not report
// stage3.narrowed_drain, with stage3 uncommitted or with every earlier switch
// on, gets a drain hold.
func TestConsensusUpdateDefaultsToDrainBeforeNarrowedDrain(t *testing.T) {
	founder := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
	for _, tc := range []struct {
		name  string
		state *pb.ConsensusParamsState
	}{
		{"stage3 not committed", testConsensusState()},
		{"every switch through checkpoints", stage3LeaderState(t, founder, false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_ = adminKey(t)
			warnings := captureStderr(t)
			leader := readyHoldLeader(tc.state)
			if err := run(context.Background(), defaultHoldArgs(serveAdminImpl(t, leader)), &bytes.Buffer{}); err != nil {
				t.Fatal(err)
			}
			if v := leader.view(); len(v.prepared) != 1 || v.prepared[0].GetMode() != drainHold || len(v.submitted) != 1 {
				t.Fatalf("leader saw %+v", v)
			}
			if w := warnings.String(); !strings.Contains(w, "info: --hold defaults to drain: the leader does not report stage3.narrowed_drain\n") {
				t.Fatalf("stderr lacks the default's reason:\n%s", w)
			}
		})
	}
	t.Run("the default hold extends --timeout", func(t *testing.T) {
		fastHoldPolls(t)
		_ = adminKey(t)
		_ = captureStderr(t)
		var granted time.Time
		leader := &holdLeader{
			prepare: func(_ int, req *pb.PrepareConsensusUpdateRequest) (*pb.ConsensusHold, error) {
				granted = time.Now()
				return &pb.ConsensusHold{HoldId: "hold-slow", Mode: req.GetMode(), PromotionSeq: 12}, nil
			},
			poll: func(int, *pb.ConsensusHoldRef) (*pb.ConsensusHold, error) {
				return &pb.ConsensusHold{HoldId: "hold-slow", Mode: drainHold, PromotionSeq: 12, Ready: time.Since(granted) > 300*time.Millisecond}, nil
			},
		}
		if err := run(context.Background(), defaultHoldArgs(serveAdminImpl(t, leader), "--timeout", "100ms", "--hold-timeout", "5s"), &bytes.Buffer{}); err != nil {
			t.Fatalf("a default hold ready after 300ms with --timeout 100ms --hold-timeout 5s: %v", err)
		}
		if v := leader.view(); len(v.submitted) != 1 {
			t.Fatalf("leader saw %+v", v)
		}
	})
}

// TestConsensusUpdateDefaultsToDrainForAnUpdateThatTouchesATrack: plan S3-E4
// K79. After narrowed_drain an update that rotates indexer 0's signer
// touches track 0, which must still drain, so the default is a drain hold.
func TestConsensusUpdateDefaultsToDrainForAnUpdateThatTouchesATrack(t *testing.T) {
	_ = adminKey(t)
	warnings := captureStderr(t)
	leader := readyHoldLeader(stage3LeaderState(t, authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0), true))
	rotated := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)
	args := defaultHoldArgs(serveAdminImpl(t, leader), "--si-indexer", fmt.Sprintf("id=0,activation=100,signer=%s,node=s1,enrollment=%s", rotated.Address(),
		writeEnrollment(t, rotated, "testnet", "genesis-committed", 0, "s1")))
	if err := run(context.Background(), args, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	v := leader.view()
	if len(v.prepared) != 1 || v.prepared[0].GetMode() != drainHold || len(v.submitted) != 1 ||
		v.submitted[0].GetUpdate().GetSiIndexers()[0].GetSigner() != rotated.Address() {
		t.Fatalf("leader saw %+v", v)
	}
	if w := warnings.String(); !strings.Contains(w, "info: --hold defaults to drain: the update touches track 0\n") {
		t.Fatalf("stderr lacks the default's reason:\n%s", w)
	}
}

// TestExplicitHoldFlagOverridesTheDefault: an explicit --hold is parsed as
// before and wins over the default, which then prints nothing; an empty
// --hold is refused before the leader is contacted.
func TestExplicitHoldFlagOverridesTheDefault(t *testing.T) {
	founder := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
	for _, tc := range []struct {
		name  string
		state *pb.ConsensusParamsState
		hold  string
		want  pb.ConsensusHoldMode
	}{
		{"drain over narrowed_drain", stage3LeaderState(t, founder, true), "drain", drainHold},
		{"issuance before narrowed_drain", testConsensusState(), "issuance", issuanceHold},
		{"none over narrowed_drain", stage3LeaderState(t, founder, true), "none", pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_UNSPECIFIED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_ = adminKey(t)
			warnings := captureStderr(t)
			leader := readyHoldLeader(tc.state)
			if err := run(context.Background(), defaultHoldArgs(serveAdminImpl(t, leader), "--hold", tc.hold), &bytes.Buffer{}); err != nil {
				t.Fatal(err)
			}
			v := leader.view()
			if len(v.submitted) != 1 {
				t.Fatalf("leader saw %+v", v)
			}
			if tc.want == pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_UNSPECIFIED {
				if len(v.prepared) != 0 || len(v.holdIDs[0]) != 0 {
					t.Fatalf("--hold none took a hold: %+v", v)
				}
			} else if len(v.prepared) != 1 || v.prepared[0].GetMode() != tc.want {
				t.Fatalf("prepare requests = %v, want mode %v", v.prepared, tc.want)
			}
			if w := warnings.String(); strings.Contains(w, "--hold defaults to") {
				t.Fatalf("an explicit --hold printed the default:\n%s", w)
			}
		})
	}
	_ = adminKey(t)
	impl := &adminTestServer{}
	err := run(context.Background(), defaultHoldArgs(startAdminServer(t, impl), "--hold", ""), &bytes.Buffer{})
	if err == nil || err.Error() != "--hold must be none, issuance or drain" || impl.reads.Load() != 0 {
		t.Fatalf("--hold \"\" = %v after %d reads, want the flag refusal before any read", err, impl.reads.Load())
	}
}

// TestTouchedIndexersMatchesTheFSM: the CLI's copy of fsm.TouchedTracks
// answers like it on every kind of si_indexers change.
func TestTouchedIndexersMatchesTheFSM(t *testing.T) {
	e0 := arbiter.SIIndexerEntry{IndexerID: 0, ActivationBlock: 150, Signer: "0xsigner0", SNodeNodeID: "s0", EnrollmentJWS: "jws-0"}
	e1 := arbiter.SIIndexerEntry{IndexerID: 1, ActivationBlock: 100, Signer: "0xsigner1", SNodeNodeID: "s1", EnrollmentJWS: "jws-1"}
	e2 := arbiter.SIIndexerEntry{IndexerID: 2, ActivationBlock: 300, Signer: "0xsigner2", SNodeNodeID: "s2", EnrollmentJWS: "jws-2"}
	rotated := e1
	rotated.Signer, rotated.EnrollmentJWS = "0xsigner1b", "jws-1b"
	restated := e0
	restated.EnrollmentJWS = "jws-0b"
	committed := []arbiter.SIIndexerEntry{e0, e1}
	for _, tc := range []struct {
		previous, next []arbiter.SIIndexerEntry
		want           []uint64
	}{
		{committed, nil, nil},
		{committed, []arbiter.SIIndexerEntry{e0, e1}, nil},
		{committed, []arbiter.SIIndexerEntry{e0, rotated}, []uint64{1}},
		{committed, []arbiter.SIIndexerEntry{e0, e1, e2}, []uint64{2}},
		{committed, []arbiter.SIIndexerEntry{restated, e1}, []uint64{0}},
		{committed, []arbiter.SIIndexerEntry{e2, rotated, restated}, []uint64{0, 1, 2}},
		{nil, []arbiter.SIIndexerEntry{e0, e1}, []uint64{0, 1}},
	} {
		got, fsmGot := touchedIndexers(tc.previous, tc.next), fsm.TouchedTracks(tc.previous, tc.next)
		if !slices.Equal(got, tc.want) || !slices.Equal(fsmGot, tc.want) {
			t.Fatalf("%v -> %v: touchedIndexers %v, fsm.TouchedTracks %v, want %v", tc.previous, tc.next, got, fsmGot, tc.want)
		}
	}
}
```

- [ ] **Step 19: Run them and see them fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./cmd/arbiter-admin/ -run 'TestConsensusUpdateDefaultsTo|TestExplicitHoldFlagOverridesTheDefault|TestTouchedIndexersMatchesTheFSM' -count=1`
Expected: the test binary does not build. The compiler reports `undefined: touchedIndexers` once, in `TestTouchedIndexersMatchesTheFSM` of `cmd/arbiter-admin/consensus_hold_test.go`, then `FAIL	github.com/sentioxyz/arbiter/cmd/arbiter-admin [build failed]`.

- [ ] **Step 20: Resolve the default hold in `cmd/arbiter-admin/main.go`**

Apply the eight replacements below, in this order.

In `cmd/arbiter-admin/main.go` (the import block, `:17`), replace

```go
	"os/signal"
	"slices"
```

with

```go
	"os/signal"
	"reflect"
	"slices"
```

In `cmd/arbiter-admin/main.go` (the `--hold` paragraph of `usage`, `:167-175`), replace

```go
--hold takes a consensus hold on the leader before the update is signed
(housegate spec 2026-10-14 §10.1-§10.2). drain, the default, has the leader
record each track's promotion frontier block, finish every block up to it and
start no later one; once the drain gate passes and every track reached its
frontier the CLI reads the frozen promotion_seq, signs the update with it and
submits it. issuance has the leader issue no promotion at all and submits the
draft the hold was taken with once the drain gate passes; it can wait until it
expires on a half-promoted block, so prefer drain. none keeps the old flow and
is the only mode an older leader supports. --hold-timeout bounds the hold (the
```

with

```go
--hold takes a consensus hold on the leader before the update is signed
(housegate spec 2026-10-14 §10.1-§10.2). drain has the leader record each
track's promotion frontier block (once stage3.narrowed_drain is committed,
each touched track's), finish every block up to it and start no later one;
once the drain gate passes and every recorded track reached its frontier the
CLI reads the frozen promotion_seq, signs the update with it and submits it.
issuance has the leader issue no promotion at all and submits the draft the
hold was taken with once the drain gate passes; before stage3.narrowed_drain
it can wait until it expires on a half-promoted block. Without --hold the CLI
takes issuance once the leader reports stage3.narrowed_drain and the update
touches no track (changes no si_indexers entry), and drain otherwise, and
prints its choice on stderr. none keeps the old flow and is the only mode an
older leader supports. --hold-timeout bounds the hold (the
```

In `cmd/arbiter-admin/main.go` (the `--hold` flag, `:406`), replace

```go
		fs.StringVar(&holdName, "hold", "drain", "consensus hold the leader takes before the update is signed: drain (default), issuance or none")
```

with

```go
		fs.StringVar(&holdName, "hold", "", "consensus hold before signing: none, issuance or drain (default: issuance once the leader reports stage3.narrowed_drain and the update touches no track, else drain)")
```

In `cmd/arbiter-admin/main.go` (`run`, `:445-446`), replace

```go
	var signer *authority.Signer
	var holdMode pb.ConsensusHoldMode
```

with

```go
	var signer *authority.Signer
	var holdMode pb.ConsensusHoldMode
	// holdDefault is set when --hold is not given: the mode is then resolved
	// from the leader's committed stage3 once the update is drafted
	// (defaultHoldMode), and it is always a hold.
	var holdDefault bool
```

In `cmd/arbiter-admin/main.go` (`run`, the `--hold` validation, `:493-503`), replace

```go
		mode, err := parseHoldMode(holdName)
		if err != nil {
			return err
		}
		holdMode = mode
		if holdTimeout <= 0 {
			return errors.New("--hold-timeout must be positive")
		}
		if present["hold-timeout"] && holdMode == pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_UNSPECIFIED {
			return errors.New("--hold-timeout is only used with --hold issuance or --hold drain")
		}
```

with

```go
		holdDefault = !present["hold"]
		if !holdDefault {
			mode, err := parseHoldMode(holdName)
			if err != nil {
				return err
			}
			holdMode = mode
		}
		if holdTimeout <= 0 {
			return errors.New("--hold-timeout must be positive")
		}
		if present["hold-timeout"] && !holdDefault && holdMode == pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_UNSPECIFIED {
			return errors.New("--hold-timeout is only used with --hold issuance or --hold drain")
		}
```

In `cmd/arbiter-admin/main.go` (`run`, the deadline, `:530-533`), replace

```go
	operationTimeout := *timeout
	if holdMode != pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_UNSPECIFIED {
		operationTimeout += holdTimeout
	}
```

with

```go
	operationTimeout := *timeout
	if holdDefault || holdMode != pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_UNSPECIFIED {
		operationTimeout += holdTimeout
	}
```

In `cmd/arbiter-admin/main.go` (`run`, after the draft is signed, `:727-731`), replace

```go
	request, err := sign(state.GetPromotionSeq())
	if err != nil {
		return err
	}
	// The consensus hold (plan S3-A contract §4) is taken with the draft and
```

with

```go
	request, err := sign(state.GetPromotionSeq())
	if err != nil {
		return err
	}
	// Without --hold the mode follows the leader's committed stage3 and the
	// si_indexers list of the normalized draft (plan S3-E4 K79).
	if holdDefault {
		var reason string
		holdMode, reason = defaultHoldMode(state, currentSIIndexers, wire.SIIndexerEntriesFromPB(request.GetUpdate().GetSiIndexers()))
		fmt.Fprintf(stderr, "info: --hold defaults to %s: %s\n", holdModeName(holdMode), reason)
	}
	// The consensus hold (plan S3-A contract §4) is taken with the draft and
```

In `cmd/arbiter-admin/main.go` (before `holdModeName`, `:1144`), replace

```go
// holdModeName is --hold's spelling of mode, "" for none.
```

with

```go
// defaultHoldMode resolves --hold when it is not given (plan S3-E4 K79,
// the sunset of plan S3-A K6). Once the leader reports
// stage3.narrowed_drain, the drain gate ignores the in-flight work of every
// track an update does not touch, so an update that touches no track (whose
// target si_indexers list changes no committed entry) takes an issuance hold,
// which no track's in-flight work can keep from becoming ready. Otherwise it
// takes a drain hold: before the switch every track must drain, and after it
// a touched track must. It returns the mode and the reason the CLI prints.
func defaultHoldMode(state *pb.ConsensusParamsState, committed, target []arbiter.SIIndexerEntry) (pb.ConsensusHoldMode, string) {
	if !state.GetCurrent().GetStage3().GetNarrowedDrain() {
		return pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_DRAIN, "the leader does not report stage3.narrowed_drain"
	}
	if touched := touchedIndexers(committed, target); len(touched) > 0 {
		ids := make([]string, len(touched))
		for i, id := range touched {
			ids[i] = strconv.FormatUint(id, 10)
		}
		return pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_DRAIN, fmt.Sprintf("the update touches track %s", strings.Join(ids, ", "))
	}
	return pb.ConsensusHoldMode_CONSENSUS_HOLD_MODE_ISSUANCE, "the leader reports stage3.narrowed_drain and the update touches no track"
}

// touchedIndexers mirrors fsm.TouchedTracks, because arbiter-admin does not
// import the FSM (TestTouchedIndexersMatchesTheFSM): the indexer ids,
// ascending, whose si_indexers entry target adds or changes against
// committed; a nil target stands for the committed list and touches none.
func touchedIndexers(committed, target []arbiter.SIIndexerEntry) []uint64 {
	var touched []uint64
	for _, e := range target {
		i := slices.IndexFunc(committed, func(c arbiter.SIIndexerEntry) bool { return c.IndexerID == e.IndexerID })
		if i < 0 || !reflect.DeepEqual(committed[i], e) {
			touched = append(touched, e.IndexerID)
		}
	}
	slices.Sort(touched)
	return slices.Compact(touched)
}

// holdModeName is --hold's spelling of mode, "" for none.
```

- [ ] **Step 21: Run the arbiter-admin tests and see them pass**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test -z "$(gofmt -l cmd)" && go vet ./cmd/arbiter-admin/ && go test ./cmd/arbiter-admin/ -run 'TestConsensusUpdateDefaultsTo|TestExplicitHoldFlagOverridesTheDefault|TestTouchedIndexersMatchesTheFSM' -race -count=1 -v | grep -E '^--- |^ok|^FAIL'`
Expected (timings omitted):

```text
--- PASS: TestConsensusUpdateDefaultsToIssuanceOnceTheLeaderReportsNarrowedDrain
--- PASS: TestConsensusUpdateDefaultsToDrainBeforeNarrowedDrain
--- PASS: TestConsensusUpdateDefaultsToDrainForAnUpdateThatTouchesATrack
--- PASS: TestExplicitHoldFlagOverridesTheDefault
--- PASS: TestTouchedIndexersMatchesTheFSM
ok  	github.com/sentioxyz/arbiter/cmd/arbiter-admin
```

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test -race ./cmd/arbiter-admin/ -count=1 && go test -race ./cmd/arbiter-admin/ -run 'TestConsensusUpdateDefaultsTo|TestExplicitHoldFlag' -count=10`
Expected: `ok` twice with no data race: every Plan S3-A, S3-E2 and S3-E3 arbiter-admin test passes unchanged, among them `TestConsensusUpdateHoldUnimplementedNamesHoldNone` (its update without `--hold` now resolves to drain, then meets the same Unimplemented refusal), `TestConsensusUpdateRejectsABadHoldFlag`, `TestCLIConsensusUpdateHoldsThroughServerAndFSM` and `TestCLIStage3ActivationThroughServerAndFSM` (the activation under the default, still drain).

- [ ] **Step 22: Document the switch in `README.md`**

In `README.md`, in the section `## Stage-3 reads and consensus holds`, replace the first sentence of its `**CLI.**` paragraph

```markdown
**CLI.** `arbiter-admin consensus update` takes a hold by default (`--hold drain`): it signs the draft, takes the hold, polls it every second (progress and the hold id on stderr), re-reads the parameters, signs the final update with the frozen `promotion_seq`, re-runs its gates and submits under the hold.
```

with

```markdown
**CLI.** `arbiter-admin consensus update` takes a hold by default: without `--hold` it takes `--hold drain` until the leader reports `stage3.narrowed_drain`, and from then on `--hold issuance` for an update that touches no track (Plan S3-E4 K79; the section on the stage-3 activation has the rule), printing its choice on stderr. With `--hold drain` it signs the draft, takes the hold, polls it every second (progress and the hold id on stderr), re-reads the parameters, signs the final update with the frozen `promotion_seq`, re-runs its gates and submits under the hold.
```

and in the section `## Multi-source storage integrity (stage-3 activation)` insert this paragraph as one line right before the paragraph that begins ``**CLI.** `arbiter-admin consensus update ... --stage3 ``, with one empty line after it:

```markdown
**Narrowed drain (switch 9).** Once `narrowed_drain` is committed (housegate spec 2026-10-14 §10.1), the drain gate refuses a consensus update only for the in-flight work of the tracks it touches. An update touches a track when it adds or changes that indexer's `si_indexers` entry (an enrolment or a signer rotation); a change of `verifiers`, `stage3`, the authority set or `max_writers` touches none, so it commits while any track has work in flight. For each touched track, in ascending order and skipping a quarantined one as the stage-2 gate does, the refusal names the first kind of work found: `consensus update touches track <id>, which has an unacknowledged promotion`, `… which has an unacknowledged cleanup command`, `… which has a promoted unsafe part waiting for something other than the copy floor` or `… which has an unacknowledged rejected-part cleanup`. A promoted unsafe part whose cleanup only the copy floor holds is not in-flight work; one that waits for its track's publication, for a cleanup the floor already allows or for another acknowledged promotion that lists it still is, and work whose track does not resolve counts for every touched track. The gate reads the committed switch, so the update that turns `narrowed_drain` on still meets the stage-2 gate. A consensus hold judges the draft it was taken with: its `ready` and `drain_refusal` are what Apply would answer for that draft, and a drain hold records the promotion frontiers of the touched tracks only, so the other tracks issue no promotion while it lives (their `tracks[]` entries report frontier 0 and nothing in flight) and never delay its readiness. Without `--hold`, `arbiter-admin consensus update` takes an issuance hold once the leader reports `stage3.narrowed_drain` and the update touches no track, and a drain hold otherwise, and prints `info: --hold defaults to <issuance|drain>: <reason>` on stderr; an explicit `--hold` wins.
```

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && grep -c '^\*\*Narrowed drain (switch 9)\.\*\*' README.md && grep -c 'takes a hold by default: without `--hold` it takes `--hold drain` until' README.md && git diff --numstat -- README.md`
Expected: `1`, `1`, then `3	1	README.md` (the new paragraph and its empty line, and the rewritten paragraph).

- [ ] **Step 23: Run the full checks**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test -z "$(git ls-files -z '*.go' | xargs -0 gofmt -l)" && go vet ./... && go test ./... -count=1 2>&1 | grep -v 'no test files'`
Expected: gofmt and vet print nothing; `ok` for every package, `integration` and `integration/chpipeline` included (the chpipeline ClickHouse tests skip without their environment, as in CI's unit job).

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && bazel run //:gazelle && git -C "$W" status --porcelain && bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors`
Expected: gazelle changes nothing; the status lists exactly ` M README.md`, ` M cmd/arbiter-admin/consensus_hold_test.go` and ` M cmd/arbiter-admin/main.go`; the build succeeds and every test target PASSes (CI's two Bazel steps).

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && bazel query 'deps(//fsm:fsm, 1)' | grep -c com_github_sentioxyz_arbiter_proto; grep -rn 'time\.Now' fsm/; git -C "$W" status --porcelain -- fsm/testdata conformance`
Expected: `0`, then nothing, then nothing (the CI red lines hold, and no golden file and no `conformance` count changed: this task adds no proto method, tag or field).

- [ ] **Step 24: Commit the CLI half and the README**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" add cmd/arbiter-admin/main.go cmd/arbiter-admin/consensus_hold_test.go README.md
git -C "$W" commit -m "feat(stage3): arbiter-admin defaults to an issuance hold after narrowed_drain (Task 77)

Without --hold, consensus update reads the leader's committed stage3:
it takes an issuance hold once stage3.narrowed_drain is reported and
the signed draft's si_indexers touch no track (touchedIndexers, the
CLI's copy of fsm.TouchedTracks), and a drain hold otherwise, so
before switch 9 the default stays drain (plan S3-A K6). It prints its
choice and the reason on stderr; an explicit --hold wins, and a
default hold extends --timeout by --hold-timeout (plan S3-E4 K79,
contract §54). The README documents the narrowed drain gate, holds
that judge their draft and the new default.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C "$W" status --porcelain
git -C "$W" log --format=%s -3
```

Expected: an empty status, then the three subjects of this task, newest first: `feat(stage3): arbiter-admin defaults to an issuance hold after narrowed_drain (Task 77)`, `feat(stage3): consensus holds judge the draft they were taken with (Task 77)`, `feat(stage3): narrowed drain gate and DrainViewFor, switch 9 (Task 77)`. Task 78 starts from this commit.

---

## Task 80: production — the stage-3 consensus rule group, the snapshot body writer value, the P5 image constants and the rehearsal Pod example (S3-E4)

Contract §56 (the production chart, binding) and §57 (the runbook section titles and the live commits these tests hold to); spec §10.3 (the plain-body fallback, the operational writer setting, the rehearsal), §13.2, §15 (production: alert rules for floor, copies and encoding fallback), §16 row P5; clarifications K78 (the floor signals), K81 (the rehearsal) and K82 (vi) (the voter budget is re-checked by the runbook, so the voter sizing stays pinned); decisions D34 (the ledger copy) and D37 (the one rehearsal directory, which this task does not touch). This task puts every chart, test and example change of P5 on branch `urwt/claude/multi-source-s3e4` (production worktree `multi-source-s3e4`); Task 81 adds the P5 runbook `docs/storage-integrity-devnet2-stage3-p5-v21.md` to the same branch and opens `P5_PR`. (1) **The writer value:** `arbiter.raft.snapshotBody` (unset by default; `auto` or `plain`) renders arbiter's node-local `raft.snapshot_body` as the last key of the voters' `raft:` block, only when set, enters the voters' config checksum only when set, and is refused with any other value and on the legacy profile. (2) **The consensus rules:** `monitoring.stage3Consensus.{enabled, heldCleanupAgeWarnSeconds, runbookURL}` renders the rule group `<fullname>.arbiter-stage3` with the eight alerts of contract §56, each pointing at the P5 runbook's `#11-operating-rules`, off by default and refused without `monitoring.enabled`. (3) **The P5 image constants:** `DEVNET2_STAGE3_P5_ARBITER_DIGEST`, `DEVNET2_STAGE3_P5_SOURCE_IMAGE` (listed in every image set a devnet2 host or member key needs, so Task 82's commits stay green), `STAGE3_P5_HOST_IMAGES`, `DEVNET2_SNAPSHOT_BODY`, `DEVNET2_STAGE3_CONSENSUS_MONITORING`, and the tests that hold Task 82's order (arbiter pin, hosts b, c, a, their si-members, then the rules). (4) **The rehearsal Pod example** `docs/examples/storage-integrity-p5-rehearsal-pod.yaml`, which mounts nothing but an emptyDir and reads only the voters' authority key. (5) The chart workflow's paths gain the P5 runbook and the example. Merging the PR syncs and applies nothing: every release renders byte-identically to `origin/main` (Steps 3, 5, 8 and 11).

**Files** (anchors: production `origin/main` once Plan S3-D's `P4_PR` has merged, i.e. `75514e3de` (SI paths byte-identical to Plan S3-B's base `9d3d0326b`) with the hunks of Plans S3-B, S3-C1, S3-C3, S3-C4 and S3-D applied, as rebuilt while drafting; line numbers in parentheses are those of that rebuild and are guidance only; the quoted text is the anchor, and a hunk that meets an earlier plan's edit of the same lines applies its intent on top of it, the hunk rule of the Global Constraints):
- Create: `docs/examples/storage-integrity-p5-rehearsal-pod.yaml`
- Modify: `charts/storage-integrity/values.yaml` (`arbiter.raft` after the `tableRegistry` block's `    audit:` / `      enabled: false`, before the arbiter's `  storageClassName: local-path` `:292`; `monitoring.stage3Consensus` before the monitoring block's `  clickhouse:` and its comment `# ClickHouseInstallation names (label clickhouse_altinity_com_chi of the` `:398`)
- Modify: `charts/storage-integrity/values.schema.json` (`arbiter.required` `:19` and `arbiter.properties.raft`; `monitoring.required` `:225` and `monitoring.properties.stage3Consensus` before `        "hosts": {` `:259`)
- Modify: `charts/storage-integrity/templates/arbiter.yaml` (the `raft:` block of `arbiter.yaml.tmpl`, after `      trailing_logs: 10240` `:35`)
- Modify: `charts/storage-integrity/templates/_helpers.tpl` (`storage-integrity.arbiterConfigChecksum`, after the `tableRegistry` clause `:64-66`)
- Modify: `charts/storage-integrity/templates/validate.yaml` (two blocks appended after its last line `:422`)
- Modify: `charts/storage-integrity/templates/monitoring.yaml` (the group `<fullname>.arbiter-stage3` before the file's last `{{- end }}` `:623`, after Plan S3-C4's `replica-reads` group)
- Modify: `.github/workflows/storage-integrity-chart.yml` (both `paths` lists, before `      - charts/storage-integrity/**` `:9`, `:31`)
- Test: `charts/storage-integrity/tests/test_render.py`. Step 2: the class `SnapshotBodyTest` before `class LegacyCompatibilityTest(unittest.TestCase):` (`:6144`). Step 4: the stage-3 rule constants before the blank line and `SENTIO_NODE_CHART = ROOT / "charts/sentio-node"` (`:1068`); `MonitoringTest.test_default_renders_no_monitoring` (`:3133`) and `MonitoringTest.test_alert_names_are_the_reviewed_set` (`:3293`); the class `Stage3ConsensusMonitoringTest` before `class LegacyCompatibilityTest(unittest.TestCase):`, after `SnapshotBodyTest`. Step 7: `DEVNET2_STAGE3_P5_ARBITER_DIGEST` before `# arbiter.consensusUpdatesEnabled on devnet2-si-v2; stage 2b changes it.` (`:38`); `DEVNET2_STAGE3_P5_SOURCE_IMAGE` before `# sentio-node images that read storage_integrity.replica_reads (the host) and` (`:86`); `STAGE3_P5_HOST_IMAGES` after `STAGE3_HOST_IMAGES` (`:118`); the three devnet2 constants after `STAGE3_P5_RUNBOOK_SECTIONS` (Step 4); five tests at the end of `ActualFreshNetworkTest`, before the blank lines and `if __name__ == "__main__":` (`:9867`). Step 8: `REPLICA_READ_HOST_IMAGES` (`:92`), `WRITER_AUTHORIZATION_IMAGES` (`:112`), `STAGE3_HOST_IMAGES` (`:118`) and `MEMBER_IMAGES` (`:717`) gain the P5 image.
- Local only, never committed: `$HOME/.cache/s3e4-task80/` (mode 700) with `ledger.env`, `fill.py`, `rollout.py`, and Step 10's throwaway clone `rehearse.<random>/repo` with its path in `rehearse-path`.
- Leave unchanged: `templates/verifier.yaml` (verifiers persist no snapshot), every devnet2 overlay and source-image file (their P5 lines are Task 82's single-service commits), the voter sizing and `test_devnet2_voter_sizing_is_pinned`, `REBUILD_SAFE_IMAGES` (Plan S3-D's `test_rebuild_safe_needs_a_reviewed_image` requires it to equal the P4 image, and `rebuildSafe` is off on every host at `P4_END`), `MONITORING_GROUP_RUNBOOKS` (its test renders only the members group), `scripts/storage-integrity/render-inertness.sh`.

**Interfaces:**
- Consumes (ledger, read only with `s3-ledger.py get`, never typed): `ARBITER_TAG_P5` (`vX.Y.Z`) and `ARBITER_DIGEST_P5` (`sha256:` + 64 hex) (Task 78), `SN_COMMIT_P5` (40 hex) and `SN_DIGEST_P5` (Task 79), `HG_TAG_P5` and `CORE_TAG_P5` (Plan S3-E1; comments only), `P4_PR` (Plan S3-D Task 54, merged); `P5_PR` empty.
- Consumes (arbiter at `ARBITER_TAG_P5`, Plans S3-A, S3-E2, S3-E3 and Task 76): the node config `raft.snapshot_body: auto|plain` (Plan S3-E2 contract §46, Task 67: raw default `auto`, refusal `raft.snapshot_body must be auto or plain, got %q`; arbiter decodes its YAML leniently, so a release before stage 3 ignores the key); the series `arbiter_snapshot_encoding_fallback_total` (counter, node-local), `arbiter_block_seal_reason{reason}` (counter, every reason exported, `reason="group_switch"`), `arbiter_admission_rejects_total{code}` (counter, `code` the admission code's proto name, here `ADMISSION_CODE_VERIFIERS_UNAVAILABLE` and `ADMISSION_CODE_COPIES_BACKPRESSURE`), `arbiter_under_replicated_total{reason}` and `arbiter_floor_held_cleanup_age_seconds` (leader-local, K78), `arbiter_under_replicated_groups` (replicated), `arbiter_group_members{owner,class}` (`class="lagging"`); Plan S3-A's `arbiter_snapshot_doc_bytes` and `arbiter_snapshot_doc_read_cap_bytes`, unchanged; the arbiter image (debian `bookworm-slim`: `sleep`, `sh`, `tar`; entrypoint `/usr/local/bin/arbiter`) and the voters' env `ARBITER_AUTHORITY_PRIVATE_KEY_HEX` from `arbiter.authoritySecret` (`storage-integrity-devnet2-si-v2-authority`, key `private-key`).
- Consumes (production, the earlier plans): Plan S3-B's `templates/monitoring.yaml` (`$fullname`, `$network`, the scrape adding `network` and `si_component`), `MONITORING_ALERTS`, `MonitoringTest.MONITORED`, `render`, `render_legacy`, `documents`, `arbiter_config`, `init_rendered_arbiter_config`, `pod_template`, `arbiter_env`, `vm_rules`, `alert_rule`, `promql_selectors`, `markdown_anchor`, `secret_references`, `ActualFreshNetworkTest.{control_release, control, si_hosts, render_with, require_every_si_host}`, `DEVNET2_ARBITER_DIGEST`, `DEVNET2_MONITORING_ENABLED`, `DEVNET2_MONITORING_NETWORK`, `DEVNET2_VOTER_MEMORY_LIMIT`, `FRESH_SOURCE_IMAGE`, `INDEXER_B_SOURCE_IMAGE`, `INDEXER_C_SOURCE_IMAGE`, `SOURCE_IMAGE_VALUES`, `WRITER_AUTHORIZATION_IMAGES`, `STAGE3_HOST_IMAGES`, `LEGACY_NAMESPACE`, `LEGACY_DIGEST`, `HARDENED_DIGEST`, `V071_AMD64_DIGEST`, `ARBITER_V0120_DIGEST`, `render-inertness.sh`; Plan S3-C1's `STAGE3_BACKUP_IMAGES`; Plan S3-C3's `MEMBER_IMAGES`, `DEVNET2_SI_RELEASES`, `si_release_values`; Plan S3-C4's `REPLICA_READ_HOST_IMAGES`, `monitoring.replicaReads`, `MonitoringTest.replica_reads_monitored`; Plan S3-D's `DEVNET2_STAGE3_P4_SOURCE_IMAGE`; `$HOME/.cache/s3-ledger/s3-ledger.py` (Plan S3-C1 contract §19).
- Produces (contract §56, verbatim): the value `arbiter.raft.snapshotBody` and its guards `arbiter.raft.snapshotBody must be auto or plain` and `arbiter.raft.snapshotBody requires arbiter.protocol=pinned-genesis-v1`; the values `monitoring.stage3Consensus.enabled` (false), `.heldCleanupAgeWarnSeconds` (3600), `.runbookURL` and their guards `monitoring.stage3Consensus.enabled requires monitoring.enabled` and `monitoring.stage3Consensus.heldCleanupAgeWarnSeconds must be a positive integer`; the group `<fullname>.arbiter-stage3` with `ArbiterSnapshotEncodingFallback`, `ArbiterBlockSealGroupSwitch`, `ArbiterVerifiersUnavailable`, `ArbiterCopiesBackpressure`, `ArbiterUnderReplicatedCleanups`, `ArbiterGroupsUnderReplicated`, `ArbiterFloorHeldCleanupAgeHigh`, `ArbiterMemberLagging`; the constants `STAGE3_CONSENSUS_ALERTS` (also `MONITORING_ALERTS["arbiter-stage3"]`), `DEVNET2_STAGE3_P5_ARBITER_DIGEST`, `DEVNET2_STAGE3_P5_SOURCE_IMAGE`, `STAGE3_P5_HOST_IMAGES`, `DEVNET2_SNAPSHOT_BODY`, `DEVNET2_STAGE3_CONSENSUS_MONITORING`, `P5_REHEARSAL_POD`; the classes `Stage3ConsensusMonitoringTest` and `SnapshotBodyTest` with their seven tests and the five `ActualFreshNetworkTest` tests of §56; the Pod `storage-integrity-si-v2-p5-rehearsal`.
- Produces (this task's names, recorded in Appendix B): `STAGE3_P5_RUNBOOK`, `STAGE3_P5_RUNBOOK_SECTIONS` (contract §57's twelve titles), `SnapshotBodyTest.RAFT_KEYS`, `Stage3ConsensusMonitoringTest.{RUNBOOK_URL, stage3, expected}`; the checksum key `raftSnapshotBody`; `DEVNET2_STAGE3_P5_SOURCE_IMAGE` in `REPLICA_READ_HOST_IMAGES` and `MEMBER_IMAGES` as well; the local tools `fill.py` and `rollout.py`.

Notes:
- **Inert on merge.** `arbiter.raft.snapshotBody` defaults to the empty string, which renders nothing and enters no checksum; `monitoring.stage3Consensus.enabled` defaults to false, and the group renders only with `monitoring.enabled` too; the image constants and the Pod example are read by tests only. Steps 3, 5, 8 and 11 prove with `render-inertness.sh` that every release renders byte-identically to `origin/main`, and merging syncs or applies nothing (no GitOps on sea, validation-only workflows). The live changes are Task 82's single-service commits, each confirmed in chat.
- **No remote or live action without confirmation.** Step 1 fetches production and housegate and reads GitHub; Step 6 fetches housegate, arbiter and sentio-node and reads the registry: each of those blocks needs explicit user confirmation in chat before it runs. This task pushes nothing, opens no PR and runs no `helmfile sync`, `kubectl` or registry write; Task 81 pushes the branch and opens `P5_PR`, each confirmed.
- **Ledger values** enter `test_render.py` and the Pod example only through Step 6's `fill.py`, from the `ledger.env` that `s3-ledger.py get` writes; a value that is not recorded stops Step 6, and every token the tool fills is checked gone afterwards.
- **Why the rules look like this.** As Plan S3-B's arbiter groups: every expression selects `network="<network>"` (the chart's own scrape adds it; the cluster's pod-annotation scrape of the voters has none), no expression selects `job`, every `by()` keeps `cluster` and `network`, and every alert carries `network` as a rule label for the Grafana delivery rule. The counters are read as `increase(...) > 0` over the windows of contract §56 and the gauges as `max(...)`, each inside `max by (cluster, network, …)` so a series of each voter collapses into one alert. `ArbiterSnapshotEncodingFallback` keeps `pod`: the writer setting and the fallback are node-local, so the alert names the voter that fell back. `ArbiterUnderReplicatedCleanups` keeps `reason` and `ArbiterMemberLagging` keeps `owner`. The leader-local series are 0 on a follower, so `max` reads the leader's value. Every `runbook_url` is `<monitoring.stage3Consensus.runbookURL>#11-operating-rules`; the group stays out of `MONITORING_GROUP_RUNBOOKS`, whose test renders only the members group, and `test_stage3_consensus_rules_read_only_this_networks_series` checks the anchor against `STAGE3_P5_RUNBOOK_SECTIONS` and, once Task 81 has written the runbook, against its `## ` headings.
- **The read-cap rule stays on the plain document.** Plan S3-E2 left to this task whether `ArbiterSnapshotDocNearReadCap` moves to `arbiter_snapshot_body_bytes`. It does not: after a fallback, and under `arbiter.raft.snapshotBody: plain` (the only lever after the activation), a voter persists the plain body, so the plain document must stay below the 1 GiB read cap whatever the interned body's size. `test_stage3_consensus_rules_read_only_this_networks_series` pins the rule to `arbiter_snapshot_doc_bytes`.
- **Why the snapshot body value looks like this.** The value is the voters' node-local writer setting, read at start: it enters `storage-integrity.arbiterConfigChecksum` only when set (the `tableRegistry` pattern), so an unset value keeps every checksum, and setting it moves the voters' pod template while the voters, `OnDelete`, take it one at a time as the runbook replaces them; the verifiers' checksum never moves. The empty default is a setting of nothing, not a third value. `values.schema.json` requires `arbiter.raft.snapshotBody` as a string, so a null or a non-string is refused with its path; `validate.yaml` refuses every other string with the contract's text and the legacy profile, whose v0.6.1 runtime has no v21 writer. arbiter decodes its YAML leniently, so an older image would ignore the key: `test_snapshot_body_needs_a_stage3_arbiter` lets devnet2 set it only while `DEVNET2_ARBITER_DIGEST` is `DEVNET2_STAGE3_P5_ARBITER_DIGEST`, mirrors it in `DEVNET2_SNAPSHOT_BODY`, and refuses the key in any other tracked manifest.
- **The P5 image in four sets.** Contract §56 names `WRITER_AUTHORIZATION_IMAGES` and `STAGE3_HOST_IMAGES`. Task 82's host and si-member commits also need `REPLICA_READ_HOST_IMAGES` (Plan S3-C4's tests require a host with `replicaReads` and a member with a serving endpoint to run one of them, and `REPLICA_READ_HOST_IMAGES <= STAGE3_HOST_IMAGES & MEMBER_IMAGES & WRITER_AUTHORIZATION_IMAGES`) and `MEMBER_IMAGES` (Plan S3-C3's `siMember.image` check), so the P5 image joins those two as well; `test_stage3_p5_host_images_are_reviewed` requires all four and keeps it out of `STAGE3_BACKUP_IMAGES`. `REBUILD_SAFE_IMAGES` stays the P4 image alone (Plan S3-D pins it, and no host runs `rebuildSafe` in P5).
- **The rehearsal Pod.** It follows the anchor-bootstrap Job's pattern (`storage-integrity-si-v2-anchor-bootstrap.yaml`): the public `ghcr.io/sentioxyz/arbiter` image by digest without `imagePullSecrets`, `automountServiceAccountToken: false`, `restartPolicy: Never`, the key only through `secretKeyRef`. It runs on the SI node pool (`nodeSelector` equal to devnet2's), requests the voters' whole memory limit so node pressure cannot evict it below the limit it measures under, and keeps the image's tools (`sleep` holds the container while the runbook execs the binary it copies in). The exact spec equality of `test_p5_rehearsal_pod_mounts_only_the_authority_secret` is the "no other volume, port or Secret" rule of contract §56.
- **The tests hold through the rollout.** Task 82 moves, one single-service commit each: the arbiter pin (`arbiter.image.digest` with `DEVNET2_ARBITER_DIGEST`), each host b, c, a (its source-image file with `INDEXER_B_SOURCE_IMAGE`, `INDEXER_C_SOURCE_IMAGE` or `FRESH_SOURCE_IMAGE`, indexer-a's with `docs/examples/storage-integrity-fresh-source-image.yaml` too), each si-member b, c, a (`siMember.image`), the rules (`monitoring.stage3Consensus.enabled` with `DEVNET2_STAGE3_CONSENSUS_MONITORING`), and runbook §11's lever (`arbiter.raft.snapshotBody: plain` with `DEVNET2_SNAPSHOT_BODY`). Step 10 makes each of those commits on a local clone and runs the suite after each, and makes ten out-of-order or half-made changes that must fail; the reverts of runbook §10.1 walk the same states backwards.
- **Verified while drafting (2026-10-16).** On a scratch production tree rebuilt from `origin/main` (`206c42357`, SI paths byte-identical to `9d3d0326b`) with stand-ins for Plans S3-B, S3-C1, S3-C3, S3-C4 and S3-D (their drafts applied as their tasks say, and their live commits up to the P4 host images and restore grants), with the key-bearing ClickHouse manifests removed, and with stand-in ledger values (`ARBITER_TAG_P5=v0.13.0`, digests and commits of repeated hex): `test_render.py` ran 224 tests OK at the base, 3 with 13 failures and 1 error after Step 2, 227 OK after Step 3, 6 with 3 failures and 3 errors after Step 4, 231 OK after Step 5, 5 with 1 failure and 1 error after Step 7 and 236 OK after Step 8; the three `helm lint --strict` profiles and a fourth with `monitoring.enabled`, `monitoring.stage3Consensus.enabled` and `arbiter.raft.snapshotBody=plain` passed; `render-inertness.sh` printed the eight `identical` lines after each commit; the 26 rendered rule expressions of the fresh values with both monitoring flags on parsed with the upstream PromQL parser (`github.com/prometheus/prometheus/promql/parser` v0.55.0) without an error; `actionlint` passed on the workflow; Step 10's rehearsal printed `OK` for all ten states (236 tests each) and `refused` for all ten bites; and a replay of Steps 2 to 9 from this task's text on a second clone of the base ended in the same tree. Every bash block of this task passes `bash -n`, `/bin/bash -n` (3.2), `zsh -n` and `shellcheck -S warning`. Not run: anything against GitHub, a registry or the cluster (Steps 1 and 6's remote reads), and `kubectl apply --dry-run=server` of the Pod (Task 82, runbook §3.2).
- **Contract notes.** §56 lists the five `ActualFreshNetworkTest` tests and the two classes; this task adds no other test. `heldCleanupAgeWarnSeconds` is checked whenever it is given (also with the group off), and `values.schema.json` makes both new blocks required (`arbiter.raft`, `monitoring.stage3Consensus`), so a null override is refused instead of silently dropping the block. `STAGE3_P5_RUNBOOK_SECTIONS` restates contract §57's twelve titles for the anchor check; Task 81's runbook must use them verbatim.

- [ ] **Step 1: Worktree and base checks**

Requires: Plan S3-D's `P4_PR` merged into production `main` (Plan S3-D Task 54's record past its R4 check) and `$HOME/.cache/s3-ledger/s3-ledger.py` (Plan S3-C1 contract §19; if it is missing, write it with the Write tool from the end of that section, verbatim). Plan S3-D Task 56 (the P4 live rollout) may still be committing to `main`; this task does not need `P4_END`, and Task 81 rebases the branch onto the then `origin/main` before it opens the PR. The commands are written for the Bash tool's shell (zsh) as well as bash; the Bash tool keeps no variables between calls, so every later block sets `W` (and `T`) itself.

This block fetches production (a remote read): it needs explicit user confirmation in chat before it runs.

```bash
P=/Users/uranuswch/Dev/projects/sentio-network/production
git -C "$P" fetch -q origin
out="$(bash ~/.agents/skills/urwt/scripts/urwt-create.sh "$P" claude multi-source-s3e4)"
printf '%s\n' "$out"
W="$(printf '%s\n' "$out" | jq -r .path)"
[ "$(printf '%s\n' "$out" | jq -r .action)" = created ] || { echo "the worktree is not new (an earlier attempt?): stop and ask"; exit 1; }
git -C "$W" status --porcelain | wc -l | tr -d ' '
git -C "$W" branch --show-current
[ "$(git -C "$W" rev-parse HEAD)" = "$(git -C "$P" rev-parse origin/main)" ] && echo "the branch starts at origin/main"
```

Expected: one JSON object with `"action":"created"` and the path `/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4`; `0`; `urwt/claude/multi-source-s3e4`; `the branch starts at origin/main`.

The P4 PR and the empty `P5_PR` row (remote reads: the housegate fetch and GitHub; explicit user confirmation in chat):

```bash
h=/Users/uranuswch/Dev/projects/sentio-network/housegate; L="$HOME/.cache/s3-ledger"; W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4
git -C "$h" fetch -q origin
pr="$(python3 -I "$L/s3-ledger.py" get "$h" P4_PR | sed -n 's/^P4_PR=//p')"
[[ "$pr" =~ ^[0-9]+$ ]] || { echo "P4_PR is not recorded on the ledger copy: stop"; exit 1; }
merge="$(gh pr view "$pr" --repo sentioxyz/production --json state,mergeCommit --jq 'select(.state == "MERGED") | .mergeCommit.oid')"
[ -n "$merge" ] && git -C "$W" merge-base --is-ancestor "$merge" HEAD && echo "P4_PR #$pr is in the base"
python3 -I "$L/s3-ledger.py" empty "$h" P5_PR
```

Expected: `P4_PR #<n> is in the base`, then `s3-ledger: P5_PR not recorded yet in <ref>:docs/superpowers/plans/2026-10-15-multi-source-si-s3e4-p5-floor-release-activation.md`. A `stop`, a PR that is not merged or not in the base, or `P5_PR` recorded already: stop and ask.

The base (local only):

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4
cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
python3 -I -B - <<'PY'
import sys
from pathlib import Path

import yaml

sys.path.insert(0, "charts/storage-integrity/tests")
import test_render as t

sets = ("WRITER_AUTHORIZATION_IMAGES", "STAGE3_HOST_IMAGES", "REPLICA_READ_HOST_IMAGES", "MEMBER_IMAGES", "REBUILD_SAFE_IMAGES")
print("P4 build in", [name for name in sets if t.DEVNET2_STAGE3_P4_SOURCE_IMAGE in getattr(t, name)])
print("SI releases", list(t.DEVNET2_SI_RELEASES))
values = yaml.safe_load(Path("charts/storage-integrity/values.yaml").read_text())
print("monitoring keys", sorted(values["monitoring"]))
print("arbiter keys with raft", "raft" in values["arbiter"], "arbiter-stage3 group", "arbiter-stage3" in t.MONITORING_ALERTS)
PY
ls docs/examples/storage-integrity-p5-rehearsal-pod.yaml docs/storage-integrity-devnet2-stage3-p5-v21.md 2>&1 | sed 's/^ls: //'
git grep -c "snapshotBody\|stage3Consensus" -- charts docs k8s-sea | wc -l | tr -d ' '
```

Expected: `Ran <N0> tests` and `OK` (write down `N0`, the count every later step adds to; 224 in the drafting rebuild); `P4 build in ['WRITER_AUTHORIZATION_IMAGES', 'STAGE3_HOST_IMAGES', 'REPLICA_READ_HOST_IMAGES', 'MEMBER_IMAGES', 'REBUILD_SAFE_IMAGES']`; `SI releases ['sentio-node-devnet2-indexer-a', 'sentio-node-devnet2-indexer-b', 'sentio-node-devnet2-indexer-c']`; `monitoring keys ['backups', 'clickhouse', 'enabled', 'hosts', 'interval', 'labels', 'members', 'network', 'replicaReads', 'runbookURL', 'scrapeTimeout', 'stateLimitBytes']`; `arbiter keys with raft False arbiter-stage3 group False`; `docs/examples/storage-integrity-p5-rehearsal-pod.yaml: No such file or directory` and `docs/storage-integrity-devnet2-stage3-p5-v21.md: No such file or directory`; `0`. Anything else: stop and ask (an earlier plan's state differs from the one this task's anchors assume).

- [ ] **Step 2: Failing tests for the snapshot body writer value**

In `charts/storage-integrity/tests/test_render.py`, insert before `class LegacyCompatibilityTest(unittest.TestCase):` (`:6144`, after the class that precedes it and its two blank lines), followed by two blank lines:

```python
class SnapshotBodyTest(unittest.TestCase):
    """The voters' snapshot body writer (housegate spec 2026-10-14 §10.3, §13.2;
    Plan S3-E4 contract §56): arbiter.raft.snapshotBody renders the node-local
    raft.snapshot_body only when set, auto or plain, and only on the fresh protocol."""

    RAFT_KEYS = [
        "listen", "advertise", "data_dir", "bootstrap", "peers", "election_timeout", "heartbeat_timeout",
        "leader_lease_timeout", "commit_timeout", "snapshot_interval", "snapshot_threshold", "trailing_logs",
    ]

    @classmethod
    def setUpClass(cls):
        cls.docs = documents(render())
        cls.bodies = {body: documents(render({"arbiter": {"raft": {"snapshotBody": body}}})) for body in ("auto", "plain")}

    def assert_rejected(self, overrides, error, *, legacy=False):
        result = render_legacy(overrides) if legacy else render(overrides)
        self.assertNotEqual(result.returncode, 0, f"{overrides} rendered")
        self.assertIn(error, result.stderr)

    def test_unset_snapshot_body_renders_unchanged(self):
        # Unset, the voters keep the binary's default (auto) and their config and
        # checksum keep the bytes they had before the value existed: the raft
        # block ends with trailing_logs on both profiles, and an explicit empty
        # value is the default, not a setting.
        self.assertEqual(yaml.safe_load((CHART / "values.yaml").read_text())["arbiter"]["raft"], {"snapshotBody": ""})
        for profile, docs in (("fresh", self.docs), ("legacy", documents(render_legacy()))):
            with self.subTest(profile=profile):
                self.assertEqual(list(arbiter_config(docs)["raft"]), self.RAFT_KEYS)
                self.assertNotIn("snapshot_body", docs[("ConfigMap", "storage-integrity-arbiter-config")]["data"]["arbiter.yaml.tmpl"])
                self.assertNotIn("snapshot_body", init_rendered_arbiter_config(docs, 0)["raft"])
        self.assertEqual(documents(render({"arbiter": {"raft": {"snapshotBody": ""}}})), self.docs)

    def test_snapshot_body_renders_only_when_set(self):
        checksums = set()
        for body, docs in self.bodies.items():
            with self.subTest(body=body):
                config = arbiter_config(docs)
                # The last key of the raft block; nothing else of the config moves.
                self.assertEqual(list(config["raft"]), self.RAFT_KEYS + ["snapshot_body"])
                self.assertEqual(config["raft"]["snapshot_body"], body)
                template = docs[("ConfigMap", "storage-integrity-arbiter-config")]["data"]["arbiter.yaml.tmpl"]
                self.assertIn(f"  trailing_logs: 10240\n  snapshot_body: {body}\n", template)
                del config["raft"]["snapshot_body"]
                self.assertEqual(config, arbiter_config(self.docs))
                for ordinal in range(3):
                    self.assertEqual(init_rendered_arbiter_config(docs, ordinal)["raft"]["snapshot_body"], body)
                # A voter reads it at start: the voters' config checksum moves (they
                # are OnDelete, so each takes it when it is replaced), the
                # verifiers' does not, and no other object changes.
                checksum = pod_template(docs)["metadata"]["annotations"]["checksum/config"]
                self.assertNotEqual(checksum, pod_template(self.docs)["metadata"]["annotations"]["checksum/config"])
                checksums.add(checksum)
                self.assertEqual(
                    pod_template(docs, "verifier")["metadata"]["annotations"]["checksum/config"],
                    pod_template(self.docs, "verifier")["metadata"]["annotations"]["checksum/config"],
                )
                self.assertEqual(set(docs), set(self.docs))
                self.assertEqual({key for key in docs if docs[key] != self.docs[key]}, {
                    ("ConfigMap", "storage-integrity-arbiter-config"), ("StatefulSet", "storage-integrity-arbiter"),
                })
        self.assertEqual(len(checksums), 2)

    def test_snapshot_body_guard_fails_closed(self):
        for body in ("Plain", "PLAIN", "interned", "none", " plain", "auto,plain"):
            with self.subTest(body=body):
                self.assert_rejected({"arbiter": {"raft": {"snapshotBody": body}}}, "arbiter.raft.snapshotBody must be auto or plain")
        for body in (True, 1, ["plain"]):
            with self.subTest(body=body):
                self.assert_rejected({"arbiter": {"raft": {"snapshotBody": body}}}, "snapshotBody")
        self.assert_rejected({"arbiter": {"raft": "plain"}}, "raft")
        self.assert_rejected({"arbiter": {"raft": None}}, "raft")
        # The legacy v0.6.1 runtime has no v21 writer.
        for body in ("auto", "plain"):
            with self.subTest(legacy=body):
                self.assert_rejected(
                    {"arbiter": {"raft": {"snapshotBody": body}}},
                    "arbiter.raft.snapshotBody requires arbiter.protocol=pinned-genesis-v1", legacy=True,
                )
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4
cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py SnapshotBodyTest 2>&1 | grep -E '^(FAIL|ERROR|Ran|FAILED|OK)|Error' | head -40
```

Expected: `Ran 3 tests` and `FAILED (failures=13, errors=1)`, with these lines: `ERROR: test_unset_snapshot_body_renders_unchanged` (`KeyError: 'raft'`); ten `FAIL: test_snapshot_body_guard_fails_closed` lines, nine subtests (`body='Plain'`, `'PLAIN'`, `'interned'`, `'none'`, `' plain'`, `'auto,plain'`, `True`, `1`, `['plain']`) and one without a subtest, each with `AssertionError: 0 == 0 : {'arbiter': {'raft': {'snapshotBody': 'Plain'}}} rendered` and the like (the chart renders all of them today; the failure outside a subtest, `{'arbiter': {'raft': 'plain'}} rendered`, ends that test before its legacy subtests); and three `FAIL: test_snapshot_body_renders_only_when_set`, two subtests (`body='auto'`, `body='plain'`) with `AssertionError: Lists differ: [...'trailing_logs'] != [...'trailing_logs', 'snapshot_body']` and one with `AssertionError: 0 != 2`. Any other failing test: stop (the base is not what Step 1 checked).

- [ ] **Step 3: The snapshot body writer value**

`charts/storage-integrity/values.yaml`, after the `tableRegistry` block, before the arbiter's `  storageClassName: local-path` (`:292`):

```diff
       enabled: false
     audit:
       enabled: false
+  raft:
+    # The voters' snapshot body writer, arbiter's node-local raft.snapshot_body
+    # (housegate spec 2026-10-14 §10.3, §13.2; Plan S3-E4 contract §56):
+    # rendered as the last key of the raft block, only when set. Unset, the
+    # binary's default auto applies: a v21 snapshot persists the interned body
+    # when it rehydrates to the plain body's digest, else the plain body, and
+    # counts arbiter_snapshot_encoding_fallback_total. plain forces the plain
+    # body; after the irreversible v21 activation it is the only lever
+    # (docs/storage-integrity-devnet2-stage3-p5-v21.md §11), and it can be
+    # unset again. A voter reads it at start: it enters the voters' config
+    # checksum, and each voter takes it when it is replaced (OnDelete). An
+    # arbiter release before stage 3 ignores the key, so a values file sets it
+    # only with a stage-3 arbiter digest. Fresh protocol only.
+    snapshotBody: ""
   storageClassName: local-path
   storageSize: 2Gi
   resources:
```

`charts/storage-integrity/values.schema.json`, `arbiter` (`:19`):

```diff
     },
     "arbiter": {
       "type": "object",
-      "required": ["protocol", "image", "metrics", "consensusUpdatesEnabled", "authoritySigningEnabled", "authoritySecret", "anchor", "storageClassName", "storageSize", "resources"],
+      "required": ["protocol", "image", "metrics", "consensusUpdatesEnabled", "authoritySigningEnabled", "authoritySecret", "anchor", "storageClassName", "storageSize", "resources", "raft"],
       "properties": {
+        "raft": {
+          "type": "object",
+          "required": ["snapshotBody"],
+          "properties": {
+            "snapshotBody": {
+              "type": "string",
+              "description": "The voters' raft.snapshot_body: empty (the binary's default, auto), auto or plain; validate.yaml refuses any other value and the legacy profile."
+            }
+          }
+        },
         "storageClassName": {"type": "string", "minLength": 1},
         "storageSize": {
           "type": "string",
```

`charts/storage-integrity/templates/arbiter.yaml`, the `raft:` block of `arbiter.yaml.tmpl` (`:35`):

```diff
       snapshot_interval: 120s
       snapshot_threshold: 8192
       trailing_logs: 10240
+      {{- with .Values.arbiter.raft.snapshotBody }}
+      snapshot_body: {{ . }}
+      {{- end }}
     seal: {max_statements: 256, max_age: 2s}
     ingress: {max_statement_age: 5m}
     dispatch: {retry_interval: 5s}
```

`charts/storage-integrity/templates/_helpers.tpl`, `storage-integrity.arbiterConfigChecksum`, after the `tableRegistry` clause (`:64-66`):

```diff
 {{- if .Values.arbiter.tableRegistry.rpcURLs -}}
 {{- $_ := set $config "tableRegistry" .Values.arbiter.tableRegistry -}}
 {{- end -}}
+{{- /* Likewise only a set raft.snapshotBody (Plan S3-E4 contract §56): a
+voter reads it at start, and releases without it keep their checksum. */ -}}
+{{- with .Values.arbiter.raft.snapshotBody -}}
+{{- $_ := set $config "raftSnapshotBody" . -}}
+{{- end -}}
 {{- toYaml $config | sha256sum -}}
 {{- end }}
 
```

`charts/storage-integrity/templates/validate.yaml`, appended after its last line (`:422`, Plan S3-D's `snodeRestoreGrant` refusal at the drafting base):

```yaml
{{- /*
The voters' snapshot body writer (housegate spec 2026-10-14 §10.3; Plan S3-E4
contract §56). Only auto and plain exist: arbiter refuses any other value at
start, and an empty value is the default, not a setting. The legacy v0.6.1
runtime has no v21 writer.
*/ -}}
{{- $snapshotBody := .Values.arbiter.raft.snapshotBody }}
{{- if $snapshotBody }}
{{- if not (has $snapshotBody (list "auto" "plain")) }}
{{- fail "arbiter.raft.snapshotBody must be auto or plain" }}
{{- end }}
{{- if ne .Values.arbiter.protocol "pinned-genesis-v1" }}
{{- fail "arbiter.raft.snapshotBody requires arbiter.protocol=pinned-genesis-v1" }}
{{- end }}
{{- end }}
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4
cd "$W" && python3 -I -c 'import json, sys; json.load(open(sys.argv[1]))' charts/storage-integrity/values.schema.json && echo "schema parses"
python3 -B charts/storage-integrity/tests/test_render.py SnapshotBodyTest 2>&1 | tail -3
T="$(mktemp -d)"
for v in "arbiter: {raft: {snapshotBody: true}}" "arbiter: {raft: null}" "arbiter: {raft: {snapshotBody: Plain}}"; do
  printf '%s\n' "$v" > "$T/o.yaml"
  helm template s charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml -f "$T/o.yaml" --namespace storage-integrity-render-test 2>&1 | grep -E "^(Error|- at)" | sed 's/validate.yaml:[0-9]*:[0-9]*/validate.yaml/' | head -2
done
printf 'arbiter: {raft: {snapshotBody: plain}}\n' > "$T/o.yaml"
helm template s charts/storage-integrity -f charts/storage-integrity/tests/legacy-devnet2-values.yaml -f "$T/o.yaml" --namespace sentio-network-devnet2 2>&1 | grep "^Error" | sed 's/validate.yaml:[0-9]*:[0-9]*/validate.yaml/'
rm -rf -- "$T"
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
bash scripts/storage-integrity/render-inertness.sh origin/main; echo "exit $?"
```

Expected: `schema parses`; `Ran 3 tests` and `OK`; `Error: values don't meet the specifications of the schema(s) in the following chart(s):` with `- at '/arbiter/raft/snapshotBody': got boolean, want string`, the same first line with `- at '/arbiter': missing property 'raft'`, and `Error: execution error at (storage-integrity/templates/validate.yaml): arbiter.raft.snapshotBody must be auto or plain`; then `Error: execution error at (storage-integrity/templates/validate.yaml): arbiter.raft.snapshotBody requires arbiter.protocol=pinned-genesis-v1`; `Ran <N0+3> tests` and `OK`; the eight lines `storage-integrity-si-v2 identical`, `sentio-node-devnet2-indexer-a identical`, `sentio-node-devnet2-indexer-b identical`, `sentio-node-devnet2-observer identical`, `sentio-node-testnet-v2-indexer-a identical`, `sentio-node-testnet-v2-indexer-b identical`, `sentio-node-observer identical`, `sentio-node-devnet2-indexer-c identical`, and `exit 0`. A release that renders differently: the change is not inert; stop and find the hunk.

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4
git -C "$W" add charts/storage-integrity/values.yaml charts/storage-integrity/values.schema.json charts/storage-integrity/templates/arbiter.yaml charts/storage-integrity/templates/_helpers.tpl charts/storage-integrity/templates/validate.yaml charts/storage-integrity/tests/test_render.py && git -C "$W" commit -m "feat(storage-integrity): the voters' snapshot body writer value, unset

arbiter.raft.snapshotBody (empty by default) renders arbiter's
node-local raft.snapshot_body as the last key of the voters' raft
block, only when set (housegate spec 2026-10-14 §10.3, §13.2; Plan
S3-E4 contract §56). plain forces the plain v21 body, the only lever
after the v21 activation; auto is the binary's default. A set value
enters the voters' config checksum, so each OnDelete voter takes it when
it is replaced; the verifiers' checksum never moves. Any other value
and the legacy profile are refused. Every release renders unchanged.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: one commit, `6 files changed`.

- [ ] **Step 4: Failing tests for the stage-3 consensus rules**

In `charts/storage-integrity/tests/test_render.py`, insert before the blank line and `SENTIO_NODE_CHART = ROOT / "charts/sentio-node"` (`:1068`, after the last constant of Plan S3-D, `MAKE_INTERSERVER_SECRET`, at the drafting base):

```python
# Stage 3 P5 (housegate spec 2026-10-14 §9.3, §10.3, §15; Plan S3-E4 contract
# §56): the consensus rule group <fullname>.arbiter-stage3, which reads series
# only a stage-3 arbiter release exports: the v21 encoding fallback, group-switch
# seals, the VERIFIERS_UNAVAILABLE and COPIES_BACKPRESSURE refusals, the copy
# floor's under_replicated signals and held-cleanup age, and lagging members.
STAGE3_CONSENSUS_ALERTS = frozenset({
    "ArbiterSnapshotEncodingFallback", "ArbiterBlockSealGroupSwitch", "ArbiterVerifiersUnavailable",
    "ArbiterCopiesBackpressure", "ArbiterUnderReplicatedCleanups", "ArbiterGroupsUnderReplicated",
    "ArbiterFloorHeldCleanupAgeHigh", "ArbiterMemberLagging",
})
MONITORING_ALERTS["arbiter-stage3"] = STAGE3_CONSENSUS_ALERTS
# The P5 runbook every stage-3 consensus runbook_url points into (its §11), and
# its top-level sections (Plan S3-E4 contract §57; Task 81 writes it).
STAGE3_P5_RUNBOOK = ROOT / "docs/storage-integrity-devnet2-stage3-p5-v21.md"
STAGE3_P5_RUNBOOK_SECTIONS = (
    "0. Session helpers", "1. Preparation", "2. Offline replay check", "3. Rehearsal", "4. Verifiers", "5. Hosts",
    "6. Voters", "7. Activation", "8. Monitoring and snapshot v21", "9. End", "10. Rollback", "11. Operating rules",
)
```

`MonitoringTest.test_default_renders_no_monitoring` (`:3133`) states every monitoring value; it gains the new block after Plan S3-C4's `replicaReads`:

```diff
             "replicaReads": {
                 "enabled": False,
                 "runbookURL": "https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p3-replica-reads.md",
             },
+            "stage3Consensus": {
+                "enabled": False, "heldCleanupAgeWarnSeconds": 3600,
+                "runbookURL": "https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p5-v21.md",
+            },
             "clickhouse": {"installations": [], "diskFreeWarnRatio": 0.2, "diskFreeCriticalRatio": 0.1},
```

`MonitoringTest.test_alert_names_are_the_reviewed_set` (`:3293`) renders every group of `MONITORING_ALERTS`; it renders the new one with its flag:

```diff
         # The replica-reads group renders only with its flag (Plan S3-C4 contract §32.2).
         rules.update(vm_rules(self.replica_reads_monitored()))
+        # The arbiter-stage3 group renders only with its flag (Plan S3-E4 contract §56).
+        rules.update(vm_rules(documents(render({"monitoring": {**self.MONITORED["monitoring"], "stage3Consensus": {"enabled": True}}}))))
         self.assertEqual(set(rules), {f"storage-integrity.{group}" for group in MONITORING_ALERTS})
```

Insert before `class LegacyCompatibilityTest(unittest.TestCase):` (after `SnapshotBodyTest` of Step 2 and its two blank lines), followed by two blank lines:

```python
class Stage3ConsensusMonitoringTest(unittest.TestCase):
    """The stage-3 P5 consensus rules (housegate spec 2026-10-14 §9.3, §10.3, §15;
    Plan S3-E4 contract §56): the rule group <fullname>.arbiter-stage3, rendered
    only with monitoring.stage3Consensus.enabled."""

    RUNBOOK_URL = "https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p5-v21.md"

    @staticmethod
    def stage3(stage3_consensus, **monitoring):
        """MonitoringTest.MONITORED with monitoring.stage3Consensus and other monitoring values merged last."""
        return {"monitoring": {**MonitoringTest.MONITORED["monitoring"], **monitoring, "stage3Consensus": stage3_consensus}}

    @staticmethod
    def expected(network, held_age):
        """Each alert's expression, severity and for (contract §56)."""
        selector = f'network="{network}"'
        return {
            "ArbiterSnapshotEncodingFallback": (
                f"max by (cluster, network, pod) (increase(arbiter_snapshot_encoding_fallback_total{{{selector}}}[30m])) > 0", "critical", None),
            "ArbiterBlockSealGroupSwitch": (
                f'max by (cluster, network) (increase(arbiter_block_seal_reason{{{selector}, reason="group_switch"}}[1h])) > 0', "warning", None),
            "ArbiterVerifiersUnavailable": (
                f'max by (cluster, network) (increase(arbiter_admission_rejects_total{{{selector}, code="ADMISSION_CODE_VERIFIERS_UNAVAILABLE"}}[15m])) > 0',
                "warning", None),
            "ArbiterCopiesBackpressure": (
                f'max by (cluster, network) (increase(arbiter_admission_rejects_total{{{selector}, code="ADMISSION_CODE_COPIES_BACKPRESSURE"}}[15m])) > 0',
                "warning", None),
            "ArbiterUnderReplicatedCleanups": (
                f"max by (cluster, network, reason) (increase(arbiter_under_replicated_total{{{selector}}}[1h])) > 0", "warning", None),
            "ArbiterGroupsUnderReplicated": (
                f"max by (cluster, network) (arbiter_under_replicated_groups{{{selector}}}) > 0", "warning", "30m"),
            "ArbiterFloorHeldCleanupAgeHigh": (
                f"max by (cluster, network) (arbiter_floor_held_cleanup_age_seconds{{{selector}}}) > {held_age}", "warning", "15m"),
            "ArbiterMemberLagging": (
                f'max by (cluster, network, owner) (arbiter_group_members{{{selector}, class="lagging"}}) > 0', "warning", "30m"),
        }

    @classmethod
    def setUpClass(cls):
        cls.monitored = documents(render(MonitoringTest.MONITORED))
        cls.on = documents(render(cls.stage3({"enabled": True})))

    def assert_rejected(self, overrides, error):
        result = render(overrides)
        self.assertNotEqual(result.returncode, 0, f"{overrides} rendered")
        self.assertIn(error, result.stderr)

    def test_stage3_consensus_rules_render_only_when_enabled(self):
        self.assertEqual(yaml.safe_load((CHART / "values.yaml").read_text())["monitoring"]["stage3Consensus"], {
            "enabled": False, "heldCleanupAgeWarnSeconds": 3600, "runbookURL": self.RUNBOOK_URL,
        })
        self.assertNotIn("storage-integrity.arbiter-stage3", vm_rules(self.monitored))
        # On, it is the VMRule's last group and changes no other group or object,
        # so turning it on or off restarts nothing.
        rules = vm_rules(self.on)
        self.assertEqual(list(rules)[-1], "storage-integrity.arbiter-stage3")
        self.assertEqual({group: group_rules for group, group_rules in rules.items() if group != "storage-integrity.arbiter-stage3"}, vm_rules(self.monitored))
        self.assertEqual(set(self.on), set(self.monitored))
        self.assertEqual({key for key in self.on if self.on[key] != self.monitored[key]}, {("VMRule", "storage-integrity")})
        # It reads only the voters' series, so it needs no listed host, member,
        # installation or CronJob.
        bare = documents(render({"monitoring": {"enabled": True, "stage3Consensus": {"enabled": True}}}))
        self.assertEqual(list(vm_rules(bare)), [
            "storage-integrity.arbiter-state-size", "storage-integrity.arbiter-promotion",
            "storage-integrity.arbiter-voters", "storage-integrity.arbiter-stage3",
        ])

    def test_stage3_consensus_alert_names_are_the_reviewed_set(self):
        self.assertEqual(MONITORING_ALERTS["arbiter-stage3"], STAGE3_CONSENSUS_ALERTS)
        rules = vm_rules(self.on)["storage-integrity.arbiter-stage3"]
        self.assertEqual([rule for rule in rules if "alert" not in rule], [])
        alerts = [rule["alert"] for rule in rules]
        self.assertEqual(len(alerts), len(set(alerts)))
        self.assertEqual(set(alerts), STAGE3_CONSENSUS_ALERTS)
        self.assertEqual(set(alerts), set(self.expected("render-test-v1", 3600)))
        for alert in alerts:
            # The Grafana delivery rule matches alertname=~"Arbiter.*|SentioNode.*|StorageIntegrity.*".
            self.assertRegex(alert, r"^Arbiter[A-Za-z]+$")
        for group, names in MONITORING_ALERTS.items():
            if group != "arbiter-stage3":
                self.assertEqual(names & STAGE3_CONSENSUS_ALERTS, frozenset(), group)

    def test_stage3_consensus_rules_read_only_this_networks_series(self):
        # As Plan S3-B's groups: every selector names this network and never job,
        # every by() keeps cluster and network, and every alert carries the
        # network as a rule label for the Grafana delivery rule. Every
        # runbook_url is the P5 runbook's operating rules.
        self.assertEqual(self.RUNBOOK_URL, f"https://github.com/sentioxyz/production/blob/main/{STAGE3_P5_RUNBOOK.relative_to(ROOT)}")
        self.assertIn("11-operating-rules", {markdown_anchor(section) for section in STAGE3_P5_RUNBOOK_SECTIONS})
        if STAGE3_P5_RUNBOOK.exists():
            self.assertEqual(tuple(re.findall(r"^## (.+)$", STAGE3_P5_RUNBOOK.read_text(), re.M)), STAGE3_P5_RUNBOOK_SECTIONS)
        probe = self.stage3({"enabled": True, "heldCleanupAgeWarnSeconds": 7200, "runbookURL": "https://example.com/p5.md"}, network="probe-net")
        probe["fullnameOverride"] = "si-probe"
        for docs, name, network, held_age, runbook in (
            (self.on, "storage-integrity", "render-test-v1", 3600, self.RUNBOOK_URL),
            (documents(render(probe, namespace="si-probe-ns", release="si-probe")), "si-probe", "probe-net", 7200, "https://example.com/p5.md"),
        ):
            rules = {rule["alert"]: rule for rule in vm_rules(docs, name)[f"{name}.arbiter-stage3"]}
            for alert, (expr, severity, wait) in self.expected(network, held_age).items():
                with self.subTest(release=name, alert=alert):
                    rule = rules[alert]
                    self.assertEqual((rule["expr"], rule.get("for")), (expr, wait))
                    for clause in re.findall(r"\bby \(([^()]*)\)", expr):
                        self.assertEqual([label.strip() for label in clause.split(",")][:2], ["cluster", "network"])
                    ((metric, matchers),) = promql_selectors(expr)
                    self.assertTrue(metric.startswith("arbiter_"), metric)
                    self.assertEqual(matchers["network"], ("=", network))
                    self.assertNotIn("job", matchers)
                    self.assertEqual(rule["labels"], {"severity": severity, "network": network})
                    self.assertNotEqual(rule["annotations"]["summary"].strip(), "")
                    self.assertEqual(rule["annotations"]["runbook_url"], f"{runbook}#11-operating-rules")
        # The read-cap alert keeps comparing the plain document with the cap: the
        # plain-body fallback and arbiter.raft.snapshotBody plain persist the plain
        # body, so it must stay below the cap whatever the interned body's size.
        expr = alert_rule(self.on, "ArbiterSnapshotDocNearReadCap")["expr"]
        self.assertEqual([metric for metric, _ in promql_selectors(expr)], ["arbiter_snapshot_doc_bytes", "arbiter_snapshot_doc_read_cap_bytes"])

    def test_stage3_consensus_guards_fail_closed(self):
        self.assert_rejected({"monitoring": {"stage3Consensus": {"enabled": True}}}, "monitoring.stage3Consensus.enabled requires monitoring.enabled")
        positive = "monitoring.stage3Consensus.heldCleanupAgeWarnSeconds must be a positive integer"
        for age in (0, -1, -3600):
            for enabled in (True, False):
                with self.subTest(age=age, enabled=enabled):
                    self.assert_rejected(self.stage3({"enabled": enabled, "heldCleanupAgeWarnSeconds": age}), positive)
        for stage3_consensus, error in (
            ({"enabled": "true"}, "enabled"),
            ({"heldCleanupAgeWarnSeconds": "3600"}, "heldCleanupAgeWarnSeconds"),
            ({"heldCleanupAgeWarnSeconds": 1.5}, "heldCleanupAgeWarnSeconds"),
            ({"runbookURL": "http://example.com/p5.md"}, "runbookURL"),
            ({"runbookURL": "https://example.com/p5.md#11-operating-rules"}, "runbookURL"),
            (None, "stage3Consensus"),
        ):
            with self.subTest(stage3Consensus=stage3_consensus):
                self.assert_rejected(self.stage3(stage3_consensus), error)
        # The legacy profile refuses monitoring as a whole.
        result = render_legacy(self.stage3({"enabled": True}))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("monitoring requires arbiter.protocol=pinned-genesis-v1", result.stderr)
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4
cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py Stage3ConsensusMonitoringTest MonitoringTest.test_default_renders_no_monitoring MonitoringTest.test_alert_names_are_the_reviewed_set 2>&1 | grep -E '^(FAIL|ERROR|Ran|FAILED|OK)|Error' | head -20
```

Expected: `Ran 6 tests` and `FAILED (failures=3, errors=3)`, with these lines: `ERROR: test_stage3_consensus_alert_names_are_the_reviewed_set` and `ERROR: test_stage3_consensus_rules_read_only_this_networks_series`, each with `KeyError: 'storage-integrity.arbiter-stage3'`; `ERROR: test_stage3_consensus_rules_render_only_when_enabled` with `KeyError: 'stage3Consensus'`; `FAIL: test_stage3_consensus_guards_fail_closed` with `AssertionError: 0 == 0 : {'monitoring': {'stage3Consensus': {'enabled': True}}} rendered`; `FAIL: test_default_renders_no_monitoring` with `AssertionError: {…} != {…'stage3Consensus': {'enabled': False, …}`; and `FAIL: test_alert_names_are_the_reviewed_set` with `AssertionError: Items in the second set but not the first:` (the new group). Any other failing test: stop.

- [ ] **Step 5: The stage-3 consensus rule group**

`charts/storage-integrity/values.yaml`, before the monitoring block's `  clickhouse:` (`:398`; after Plan S3-C4's `replicaReads` at the drafting base):

```diff
   replicaReads:
     enabled: false
     runbookURL: https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p3-replica-reads.md
+  # The stage-3 P5 consensus rule group <fullname>.arbiter-stage3 (housegate
+  # spec 2026-10-14 §9.3, §10.3, §15; Plan S3-E4 contract §56): the v21
+  # snapshot encoding fallback, group-switch seals, the admission refusals
+  # VERIFIERS_UNAVAILABLE and COPIES_BACKPRESSURE, the copy floor's
+  # under_replicated signals and held-cleanup age, and lagging members. Every
+  # series it reads is exported only by a stage-3 arbiter release, and an
+  # absent series never fires, so it is turned on only once every voter runs
+  # that release (docs/storage-integrity-devnet2-stage3-p5-v21.md §8). It reads
+  # the voters' series alone, so it needs no listed host or member. Every
+  # runbook_url points into runbookURL.
+  stage3Consensus:
+    enabled: false
+    # ArbiterFloorHeldCleanupAgeHigh fires when the leader's
+    # arbiter_floor_held_cleanup_age_seconds (the oldest cleanup only the copy
+    # floor holds) stays above this many seconds for 15 minutes.
+    heldCleanupAgeWarnSeconds: 3600
+    runbookURL: https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p5-v21.md
   clickhouse:
     # ClickHouseInstallation names (label clickhouse_altinity_com_chi of the
     # cluster's pod-annotation scrape) whose restarts and default-disk space
```

`charts/storage-integrity/values.schema.json`, `monitoring.required` (`:225`):

```diff
     },
     "monitoring": {
       "type": "object",
-      "required": ["enabled", "network", "interval", "scrapeTimeout", "labels", "stateLimitBytes", "hosts", "clickhouse", "backups", "runbookURL"],
+      "required": ["enabled", "network", "interval", "scrapeTimeout", "labels", "stateLimitBytes", "hosts", "clickhouse", "backups", "runbookURL", "stage3Consensus"],
       "properties": {
         "enabled": {"type": "boolean"},
         "network": {"type": "string"},
```

and `monitoring.properties`, before `        "hosts": {` (`:259`, after Plan S3-C3's `members` at the drafting base):

```diff
             "runbookURL": {"type": "string", "pattern": "^https://[^\\s#\"]+$"}
           }
         },
+        "stage3Consensus": {
+          "type": "object",
+          "required": ["enabled", "heldCleanupAgeWarnSeconds", "runbookURL"],
+          "properties": {
+            "enabled": {"type": "boolean"},
+            "heldCleanupAgeWarnSeconds": {
+              "type": "integer",
+              "description": "Seconds of arbiter_floor_held_cleanup_age_seconds above which ArbiterFloorHeldCleanupAgeHigh fires; validate.yaml requires a positive value."
+            },
+            "runbookURL": {"type": "string", "pattern": "^https://[^\\s#\"]+$"}
+          }
+        },
         "hosts": {
           "type": "object",
           "required": ["releases"],
```

`charts/storage-integrity/templates/validate.yaml`, appended after the snapshot body block of Step 3:

```yaml
{{- /*
The stage-3 P5 consensus rules (Plan S3-E4 contract §56) are a group of the
network's VMRule, so they need monitoring itself; the held-cleanup age is a
whole number of seconds, checked whenever it is given.
*/ -}}
{{- $stage3Consensus := .Values.monitoring.stage3Consensus }}
{{- if and $stage3Consensus.enabled (not .Values.monitoring.enabled) }}
{{- fail "monitoring.stage3Consensus.enabled requires monitoring.enabled" }}
{{- end }}
{{- $heldAge := $stage3Consensus.heldCleanupAgeWarnSeconds }}
{{- if or (lt (float64 $heldAge) 1.0) (ne (float64 (int64 $heldAge)) (float64 $heldAge)) }}
{{- fail "monitoring.stage3Consensus.heldCleanupAgeWarnSeconds must be a positive integer" }}
{{- end }}
```

`charts/storage-integrity/templates/monitoring.yaml`, before the file's last line `{{- end }}` (`:623`, which closes `{{- if .Values.monitoring.enabled }}`; after Plan S3-C4's `replica-reads` group at the drafting base):

```yaml
    {{- if .Values.monitoring.stage3Consensus.enabled }}
    {{- $stage3Runbook := .Values.monitoring.stage3Consensus.runbookURL }}
    {{- $heldAge := int64 .Values.monitoring.stage3Consensus.heldCleanupAgeWarnSeconds }}
    # Stage 3 P5 (housegate spec 2026-10-14 §9.3, §10.3, §15; Plan S3-E4
    # contract §56, K78): the v21 snapshot encoding, group-switch seals, the
    # two stage-3 admission refusals, the copy floor's under_replicated signals
    # and held-cleanup age, and lagging members. Every series is the voters'
    # own, scraped by <fullname>-arbiter; arbiter_under_replicated_total and
    # arbiter_floor_held_cleanup_age_seconds are the leader's alone (0 on a
    # follower; the age restarts at a leadership change, so it can understate
    # after a failover and never overstates). Every runbook_url points into the
    # P5 runbook's operating rules.
    - name: {{ $fullname }}.arbiter-stage3
      rules:
        - alert: ArbiterSnapshotEncodingFallback
          expr: |-
            max by (cluster, network, pod) (increase(arbiter_snapshot_encoding_fallback_total{network="{{ $network }}"}[30m])) > 0
          labels:
            severity: critical
            network: {{ $network | quote }}
          annotations:
            summary: "A voter persisted a v21 snapshot with the plain body: the interned body did not rehydrate to the plain body's digest (the snapshot is intact; the codec is not)"
            runbook_url: "{{ $stage3Runbook }}#11-operating-rules"
        - alert: ArbiterBlockSealGroupSwitch
          expr: |-
            max by (cluster, network) (increase(arbiter_block_seal_reason{network="{{ $network }}", reason="group_switch"}[1h])) > 0
          labels:
            severity: warning
            network: {{ $network | quote }}
          annotations:
            summary: "The Arbiter sealed blocks early because a statement of another group key of the same owner arrived: divergent groups split blocks and add anchors and replays"
            runbook_url: "{{ $stage3Runbook }}#11-operating-rules"
        - alert: ArbiterVerifiersUnavailable
          expr: |-
            max by (cluster, network) (increase(arbiter_admission_rejects_total{network="{{ $network }}", code="ADMISSION_CODE_VERIFIERS_UNAVAILABLE"}[15m])) > 0
          labels:
            severity: warning
            network: {{ $network | quote }}
          annotations:
            summary: "The Arbiter refused statements with VERIFIERS_UNAVAILABLE: a database has too few eligible verifiers outside its owner's host; clients retry"
            runbook_url: "{{ $stage3Runbook }}#11-operating-rules"
        - alert: ArbiterCopiesBackpressure
          expr: |-
            max by (cluster, network) (increase(arbiter_admission_rejects_total{network="{{ $network }}", code="ADMISSION_CODE_COPIES_BACKPRESSURE"}[15m])) > 0
          labels:
            severity: warning
            network: {{ $network | quote }}
          annotations:
            summary: "The Arbiter refused an owner's statements with COPIES_BACKPRESSURE: in strict mode a table's uncleaned promoted parts passed their bound while the copy floor holds their cleanup; clients retry until members checkpoint"
            runbook_url: "{{ $stage3Runbook }}#11-operating-rules"
        - alert: ArbiterUnderReplicatedCleanups
          expr: |-
            max by (cluster, network, reason) (increase(arbiter_under_replicated_total{network="{{ $network }}"}[1h])) > 0
          labels:
            severity: warning
            network: {{ $network | quote }}
          annotations:
            summary: "The leader scheduled cleanups through an escape of the copy floor (an inactive incarnation or partition or table pressure) or with fewer than k - 1 failure domains in E; reason names which"
            runbook_url: "{{ $stage3Runbook }}#11-operating-rules"
        - alert: ArbiterGroupsUnderReplicated
          expr: |-
            max by (cluster, network) (arbiter_under_replicated_groups{network="{{ $network }}"}) > 0
          for: 30m
          labels:
            severity: warning
            network: {{ $network | quote }}
          annotations:
            summary: "A database group has had counting members in fewer than k - 1 distinct failure domains other than its owner's for 30 minutes"
            runbook_url: "{{ $stage3Runbook }}#11-operating-rules"
        - alert: ArbiterFloorHeldCleanupAgeHigh
          expr: |-
            max by (cluster, network) (arbiter_floor_held_cleanup_age_seconds{network="{{ $network }}"}) > {{ $heldAge }}
          for: 15m
          labels:
            severity: warning
            network: {{ $network | quote }}
          annotations:
            summary: "A cleanup that only the copy floor holds has waited longer than monitoring.stage3Consensus.heldCleanupAgeWarnSeconds on this leader: its group's members do not checkpoint"
            runbook_url: "{{ $stage3Runbook }}#11-operating-rules"
        - alert: ArbiterMemberLagging
          expr: |-
            max by (cluster, network, owner) (arbiter_group_members{network="{{ $network }}", class="lagging"}) > 0
          for: 30m
          labels:
            severity: warning
            network: {{ $network | quote }}
          annotations:
            summary: "A database group member has been Lagging for 30 minutes: more than max_lag_publications advances behind its owner, or marked lagging"
            runbook_url: "{{ $stage3Runbook }}#11-operating-rules"
    {{- end }}
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4
cd "$W" && python3 -I -c 'import json, sys; json.load(open(sys.argv[1]))' charts/storage-integrity/values.schema.json && echo "schema parses"
python3 -B charts/storage-integrity/tests/test_render.py Stage3ConsensusMonitoringTest MonitoringTest.test_default_renders_no_monitoring MonitoringTest.test_alert_names_are_the_reviewed_set 2>&1 | tail -3
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
bash scripts/storage-integrity/render-inertness.sh origin/main; echo "exit $?"
git -C "$W" add charts/storage-integrity/values.yaml charts/storage-integrity/values.schema.json charts/storage-integrity/templates/validate.yaml charts/storage-integrity/templates/monitoring.yaml charts/storage-integrity/tests/test_render.py && git -C "$W" commit -m "feat(storage-integrity): the stage-3 consensus rule group, off

monitoring.stage3Consensus (enabled false, heldCleanupAgeWarnSeconds
3600, runbookURL) renders the rule group <fullname>.arbiter-stage3
(housegate spec 2026-10-14 §9.3, §10.3, §15; Plan S3-E4 contract §56):
the v21 snapshot encoding fallback, group-switch seals, the
VERIFIERS_UNAVAILABLE and COPIES_BACKPRESSURE refusals, the copy
floor's under_replicated cleanups and groups, its held-cleanup age and
lagging members, each on this network's series and pointing at the P5
runbook's operating rules. It needs monitoring.enabled and is off on
devnet2 until every voter runs the stage-3 arbiter release. The
read-cap alert keeps reading the plain document: the plain-body
fallback persists it. Every release renders unchanged.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: `schema parses`; `Ran 6 tests` and `OK`; `Ran <N0+7> tests` and `OK`; the eight `identical` lines of Step 3 and `exit 0`; one commit, `5 files changed`.

- [ ] **Step 6: Ledger inputs and their registry checks**

Requires Tasks 78 and 79's records past their R4 check (`ARBITER_TAG_P5`, `ARBITER_DIGEST_P5`, `SN_COMMIT_P5`, `SN_DIGEST_P5`) and Plan S3-E1's (`HG_TAG_P5`, `CORE_TAG_P5`). The housegate fetch reads a remote: this block needs explicit user confirmation in chat before it runs. The values go into `$HOME/.cache/s3e4-task80/ledger.env` exactly as `s3-ledger.py get` prints them, and every later block reads them from there, so no value is ever typed:

```bash
h=/Users/uranuswch/Dev/projects/sentio-network/housegate; L="$HOME/.cache/s3-ledger"; T="$HOME/.cache/s3e4-task80"
git -C "$h" fetch -q origin
mkdir -p "$T" && chmod 700 "$T"
python3 -I "$L/s3-ledger.py" get "$h" ARBITER_TAG_P5 ARBITER_DIGEST_P5 SN_COMMIT_P5 SN_DIGEST_P5 HG_TAG_P5 CORE_TAG_P5 > "$T/ledger.env.new" && mv "$T/ledger.env.new" "$T/ledger.env"
cat "$T/ledger.env"
```

Expected: six lines `ARBITER_TAG_P5=v<x.y.z>`, `ARBITER_DIGEST_P5=sha256:<64 hex>`, `SN_COMMIT_P5=<40 hex>`, `SN_DIGEST_P5=sha256:<64 hex>`, `HG_TAG_P5=v<x.y.z>`, `CORE_TAG_P5=v<x.y.z>`. A `stop` means a value is not recorded on its ledger copy yet: stop and ask; never type a value in.

The registry and source reads (remote reads: the arbiter and sentio-node fetches and the registry; explicit user confirmation in chat first):

```bash
T="$HOME/.cache/s3e4-task80"; set -a; . "$T/ledger.env"; set +a
printf '%s %s %s\n' "$ARBITER_TAG_P5" "$HG_TAG_P5" "$CORE_TAG_P5" | grep -Eqx 'v[0-9]+\.[0-9]+\.[0-9]+ v[0-9]+\.[0-9]+\.[0-9]+ v[0-9]+\.[0-9]+\.[0-9]+' \
  && printf '%s %s\n' "$ARBITER_DIGEST_P5" "$SN_DIGEST_P5" | grep -Eqx 'sha256:[0-9a-f]{64} sha256:[0-9a-f]{64}' \
  && printf '%s\n' "$SN_COMMIT_P5" | grep -Eqx '[0-9a-f]{40}' && echo "ledger values well-formed"
a=/Users/uranuswch/Dev/projects/sentio-network/arbiter; s=/Users/uranuswch/Dev/projects/sentio-network/sentio-node
git -C "$a" fetch -q --tags origin && git -C "$s" fetch -q origin
[ "$(docker buildx imagetools inspect "ghcr.io/sentioxyz/arbiter:$ARBITER_TAG_P5" --format '{{json .Manifest}}' | jq -r .digest)" = "$ARBITER_DIGEST_P5" ] && echo "ARBITER_DIGEST_P5 is $ARBITER_TAG_P5"
rev="$(docker buildx imagetools inspect "ghcr.io/sentioxyz/arbiter@$ARBITER_DIGEST_P5" --format '{{json .Image}}' | jq -r 'if .config then .config.Labels["org.opencontainers.image.revision"] else .["linux/amd64"].config.Labels["org.opencontainers.image.revision"] end')"
[ "$rev" = "$(git -C "$a" rev-parse "$ARBITER_TAG_P5^{commit}")" ] && echo "the arbiter image is $ARBITER_TAG_P5's commit"
[ "$(docker buildx imagetools inspect "ghcr.io/sentioxyz/sentio-node:sha-$SN_COMMIT_P5" --format '{{json .Manifest}}' | jq -r .digest)" = "$SN_DIGEST_P5" ] && echo "SN_DIGEST_P5 resolves"
rev="$(docker buildx imagetools inspect "ghcr.io/sentioxyz/sentio-node@$SN_DIGEST_P5" --format '{{json .Image}}' | jq -r 'if .config then .config.Labels["org.opencontainers.image.revision"] else .["linux/amd64"].config.Labels["org.opencontainers.image.revision"] end')"
[ "$rev" = "$SN_COMMIT_P5" ] && git -C "$s" merge-base --is-ancestor "$SN_COMMIT_P5" origin/main && echo "the sentio-node image is SN_COMMIT_P5, on main"
git -C "$s" show "$SN_COMMIT_P5:go.mod" | grep -cE "^(require )?[[:space:]]*github.com/(housegate/housegate $HG_TAG_P5|sentioxyz/arbiter-core $CORE_TAG_P5)( //.*)?$"
```

Expected: `ledger values well-formed`, `ARBITER_DIGEST_P5 is v<x.y.z>`, `the arbiter image is v<x.y.z>'s commit`, `SN_DIGEST_P5 resolves`, `the sentio-node image is SN_COMMIT_P5, on main`, `2` (the P5 host build pins both P5 releases, contract §56). Anything else: stop; nothing is listed.

Write the fill tool `$HOME/.cache/s3e4-task80/fill.py` with the Write tool (local, never committed):

```python
"""Fill the ledger values of Plan S3-E4 Task 80 into files, only from the
NAME=value lines that s3-ledger.py get wrote. A local tool, never committed.

usage: fill.py <ledger.env> <file>...

Every <NAME> token whose NAME is one of the six names below is replaced by its
value; a file that would still hold one of the tokens is not written, and the
tool stops.
"""
import re
import sys
from pathlib import Path

TOKENS = ("ARBITER_TAG_P5", "ARBITER_DIGEST_P5", "SN_COMMIT_P5", "SN_DIGEST_P5", "HG_TAG_P5", "CORE_TAG_P5")
env = {}
for line in Path(sys.argv[1]).read_text().splitlines():
    name, sep, value = line.partition("=")
    if not sep or name not in TOKENS or name in env or not re.fullmatch(r"[A-Za-z0-9:.]+", value):
        sys.exit(f"fill: unexpected line {line!r} in {sys.argv[1]}; stop")
    env[name] = value
if set(env) != set(TOKENS):
    sys.exit(f"fill: {sys.argv[1]} lacks {', '.join(sorted(set(TOKENS) - set(env)))}; stop")
for name in sys.argv[2:]:
    path = Path(name)
    text = path.read_text()
    for key, value in env.items():
        text = text.replace(f"<{key}>", value)
    left = sorted(token for token in TOKENS if f"<{token}>" in text)
    if left:
        sys.exit(f"fill: {name} would still hold {', '.join('<' + t + '>' for t in left)}; nothing written; stop")
    path.write_text(text)
    print(f"filled {name}")
```

- [ ] **Step 7: Failing tests for the P5 images, their rollout order and the rehearsal Pod**

In `charts/storage-integrity/tests/test_render.py`, insert before `# arbiter.consensusUpdatesEnabled on devnet2-si-v2; stage 2b changes it.` (`:38`, right after `DEVNET2_ARBITER_DIGEST`):

```python
# The multi-source stage-3 P5 arbiter release, arbiter <ARBITER_TAG_P5> (Plan
# S3-E4 Task 78, ARBITER_DIGEST_P5): snapshot v21 behind the stage3 parameters,
# groups, owner-aware selection, checkpoints, the copy floor and the narrowed
# drain gate, every switch off until P6; it writes v20 until the activation
# commits stage3. docs/storage-integrity-devnet2-stage3-p5-v21.md moves
# DEVNET2_ARBITER_DIGEST to it (§4) and runs the rehearsal Pod on it (§3).
# arbiter.raft.snapshotBody and the stage-3 consensus rules need it: an earlier
# release ignores the key and exports none of those series.
DEVNET2_STAGE3_P5_ARBITER_DIGEST = "<ARBITER_DIGEST_P5>"
```

Insert before `# sentio-node images that read storage_integrity.replica_reads (the host) and` (`:86`, after Plan S3-D's `DEVNET2_STAGE3_P4_SOURCE_IMAGE`):

```python
# The multi-source stage-3 P5 sentio-node build (Plan S3-E4 Task 79:
# SN_COMMIT_P5 / SN_DIGEST_P5, on housegate <HG_TAG_P5> and arbiter-core
# <CORE_TAG_P5>): every SNode and si-member advertises stage3_v1, the embedded
# ingress answers the admission codes 12 and 13 retryably, and the member
# consensus keys stay off. It carries everything of the stage-3 P4 build and
# reads every host and member key the devnet2 overlays set, so the image sets
# below list it before docs/storage-integrity-devnet2-stage3-p5-v21.md moves
# any host (§5.1 to §5.3) or si-member (§5.4) to it.
DEVNET2_STAGE3_P5_SOURCE_IMAGE = (
    "ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT_P5>"
    "@<SN_DIGEST_P5>"
)
```

Insert after `STAGE3_HOST_IMAGES = frozenset({...})` (`:118`, before `DEVNET2 = ROOT / "k8s-sea/sentio-network-devnet2"`):

```python
# sentio-node images whose SNode advertises stage3_v1 (housegate spec 2026-10-14
# §10.3; Plan S3-E1 K65): the activation commits only once every registered
# data-plane node advertises it to the leader, so every SI host runs one of
# these before the P5 runbook's §7.2.
STAGE3_P5_HOST_IMAGES = frozenset({DEVNET2_STAGE3_P5_SOURCE_IMAGE})
```

Insert after `STAGE3_P5_RUNBOOK_SECTIONS` (Step 4), before the blank line and `SENTIO_NODE_CHART`:

```python
# arbiter.raft.snapshotBody on devnet2-si-v2: None (unset, the binary's default
# auto) or the value set. Only the P5 runbook's §11 lever sets it (plain),
# together with this mirror, and unsets it again.
DEVNET2_SNAPSHOT_BODY = None
# monitoring.stage3Consensus.enabled on devnet2-si-v2: the P5 runbook (§8)
# turns it on once every voter runs DEVNET2_STAGE3_P5_ARBITER_DIGEST.
DEVNET2_STAGE3_CONSENSUS_MONITORING = False
# The throwaway P5 rehearsal Pod (Plan S3-E4 K81), relative to ROOT: the P5
# runbook applies it by hand (§3.2) and deletes it (§3.6); no chart or
# helmfile renders it.
P5_REHEARSAL_POD = "docs/examples/storage-integrity-p5-rehearsal-pod.yaml"
```

Append to `ActualFreshNetworkTest`, after its last test (before the blank lines and `if __name__ == "__main__":`, `:9867`):

```python
    def test_stage3_p5_host_images_are_reviewed(self):
        # The P5 build is the image every SI host runs before the activation
        # (runbook §5) and every si-member after it (§5.4): it reads every key
        # the devnet2 overlays set from P0 to P4, so it is a reviewed
        # writer-authorization, stage-3 host, replica-read host and member image.
        self.assertEqual(STAGE3_P5_HOST_IMAGES, frozenset({DEVNET2_STAGE3_P5_SOURCE_IMAGE}))
        for image in STAGE3_P5_HOST_IMAGES:
            self.assertRegex(image, r"^ghcr\.io/sentioxyz/sentio-node:sha-[0-9a-f]{40}@sha256:[0-9a-f]{64}$")
            self.assertIn(image, WRITER_AUTHORIZATION_IMAGES)
            self.assertIn(image, STAGE3_HOST_IMAGES)
            self.assertIn(image, REPLICA_READ_HOST_IMAGES)
            self.assertIn(image, MEMBER_IMAGES)
            self.assertNotIn(image, STAGE3_BACKUP_IMAGES)
            self.assertNotEqual(image, DEVNET2_STAGE3_P4_SOURCE_IMAGE)
        self.assertRegex(DEVNET2_STAGE3_P5_ARBITER_DIGEST, r"^sha256:[0-9a-f]{64}$")
        self.assertNotIn(DEVNET2_STAGE3_P5_ARBITER_DIGEST, {LEGACY_DIGEST, HARDENED_DIGEST, V071_AMD64_DIGEST, ARBITER_V0120_DIGEST})

    def test_devnet2_stage3_p5_images_follow_the_rollout(self):
        # docs/storage-integrity-devnet2-stage3-p5-v21.md moves devnet2 to the P5
        # builds in single-service commits, each moving its file and its mirror
        # together (Plan S3-E4 contract §57): the arbiter digest first (§4; the
        # verifiers roll on its sync, the voters follow under the same pin in
        # §6), then each host in the order b, c, a (§5.1 to §5.3), then each
        # si-member in the same order (§5.4), and the stage-3 rules last (§8).
        # Every state the files declare is a prefix of that order, and each
        # revert of §10.1 walks it back.
        self.require_every_si_host()
        order = ("sentio-node-devnet2-indexer-b", "sentio-node-devnet2-indexer-c", "sentio-node-devnet2-indexer-a")
        self.assertEqual(set(order), set(DEVNET2_SI_RELEASES))
        mirrors = {
            "sentio-node-devnet2-indexer-a": FRESH_SOURCE_IMAGE,
            "sentio-node-devnet2-indexer-b": INDEXER_B_SOURCE_IMAGE,
            "sentio-node-devnet2-indexer-c": INDEXER_C_SOURCE_IMAGE,
        }
        overlay = yaml.safe_load((DEVNET2 / "storage-integrity-si-v2.yaml").read_text())
        self.assertEqual(overlay["arbiter"]["image"]["digest"], DEVNET2_ARBITER_DIGEST)
        hosts, members = {}, {}
        for name in order:
            values = si_release_values(name)
            image = values["node"]["image"]
            hosts[name] = f'{image["repository"]}:{image["tag"]}'
            members[name] = values["siMember"]["image"]
            self.assertEqual(hosts[name], mirrors[name], f"{name}: its source-image file and its mirror move together")
        self.assertEqual(yaml.safe_load(SOURCE_IMAGE_VALUES.read_text())["node"]["image"]["tag"], FRESH_SOURCE_IMAGE.split(":", 1)[1])
        on_p5 = [name for name in order if hosts[name] in STAGE3_P5_HOST_IMAGES]
        members_on_p5 = [name for name in order if members[name] in STAGE3_P5_HOST_IMAGES]
        self.assertEqual(on_p5, list(order[:len(on_p5)]), "the hosts move to the P5 build in the order b, c, a (runbook §5)")
        self.assertEqual(members_on_p5, list(order[:len(members_on_p5)]), "the si-members move to the P5 build in the order b, c, a (runbook §5.4)")
        if on_p5:
            self.assertEqual(DEVNET2_ARBITER_DIGEST, DEVNET2_STAGE3_P5_ARBITER_DIGEST, "a host moves to the P5 build only after the arbiter pin (runbook §4)")
        if members_on_p5:
            self.assertEqual(on_p5, list(order), "an si-member moves to the P5 build only after every host (runbook §5.4)")
        if DEVNET2_STAGE3_CONSENSUS_MONITORING:
            self.assertEqual(members_on_p5, list(order), "the stage-3 rules come after every host and si-member (runbook §8)")

    def test_snapshot_body_needs_a_stage3_arbiter(self):
        # An arbiter release before stage 3 decodes its YAML leniently and ignores
        # raft.snapshot_body, so the value would only look set: devnet2-si-v2 sets
        # it only while its arbiter pin is the P5 release, and
        # DEVNET2_SNAPSHOT_BODY mirrors it. The P5 runbook's §11 sets plain, the
        # only lever after the v21 activation, and unsets it again.
        overlay = yaml.safe_load((DEVNET2 / "storage-integrity-si-v2.yaml").read_text())
        body = (overlay["arbiter"].get("raft") or {}).get("snapshotBody") or None
        self.assertEqual(body, DEVNET2_SNAPSHOT_BODY)
        self.assertIn(DEVNET2_SNAPSHOT_BODY, (None, "auto", "plain"))
        raft = arbiter_config(self.control, FRESH_RELEASE)["raft"]
        if DEVNET2_SNAPSHOT_BODY is None:
            self.assertNotIn("snapshot_body", raft)
        else:
            self.assertEqual(raft["snapshot_body"], DEVNET2_SNAPSHOT_BODY)
            self.assertEqual(DEVNET2_ARBITER_DIGEST, DEVNET2_STAGE3_P5_ARBITER_DIGEST, "arbiter.raft.snapshotBody needs the stage-3 arbiter pin")
        # The lever renders on the devnet2 values and moves only the voters' config.
        plain = documents(self.render_with(self.control_release, {"arbiter": {"raft": {"snapshotBody": "plain"}}}))
        self.assertEqual(arbiter_config(plain, FRESH_RELEASE)["raft"]["snapshot_body"], "plain")
        self.assertEqual({key for key in plain if plain[key] != self.control.get(key)} - {
            ("ConfigMap", f"{FRESH_RELEASE}-arbiter-config"), ("StatefulSet", f"{FRESH_RELEASE}-arbiter"),
        }, set())
        # No other file sets the key: dead configuration a later release could pick up unreviewed.
        tracked = subprocess.run(["git", "-C", str(ROOT), "ls-files", "k8s-*"], capture_output=True, text=True, check=True).stdout.split()
        for name in tracked:
            path = ROOT / name
            if path.suffix in (".yaml", ".yml") and path != DEVNET2 / "storage-integrity-si-v2.yaml" and path.is_file():
                self.assertNotIn("snapshotBody", path.read_text(errors="ignore"), name)

    def test_stage3_consensus_monitoring_needs_a_stage3_arbiter(self):
        # Every series of the arbiter-stage3 group is exported only by the P5
        # arbiter release, and an absent series never fires: the runbook (§8)
        # turns the group on only once every voter runs it, with
        # DEVNET2_STAGE3_CONSENSUS_MONITORING.
        monitoring = yaml.safe_load((DEVNET2 / "storage-integrity-si-v2.yaml").read_text())["monitoring"]
        self.assertIs((monitoring.get("stage3Consensus") or {}).get("enabled", False), DEVNET2_STAGE3_CONSENSUS_MONITORING)
        group = f"{FRESH_RELEASE}.arbiter-stage3"
        self.assertEqual(group in (vm_rules(self.control, FRESH_RELEASE) if DEVNET2_MONITORING_ENABLED else {}), DEVNET2_STAGE3_CONSENSUS_MONITORING)
        if DEVNET2_STAGE3_CONSENSUS_MONITORING:
            self.assertTrue(DEVNET2_MONITORING_ENABLED)
            self.assertEqual(DEVNET2_ARBITER_DIGEST, DEVNET2_STAGE3_P5_ARBITER_DIGEST, "the stage-3 rules need the stage-3 arbiter pin")
        # Turned on, it renders on the devnet2 values with this network's selector
        # and the P5 runbook's operating rules.
        on = documents(self.render_with(self.control_release, {"monitoring": {"enabled": True, "stage3Consensus": {"enabled": True}}}))
        rules = vm_rules(on, FRESH_RELEASE)[group]
        self.assertEqual({rule["alert"] for rule in rules}, STAGE3_CONSENSUS_ALERTS)
        runbook = yaml.safe_load((CHART / "values.yaml").read_text())["monitoring"]["stage3Consensus"]["runbookURL"]
        for rule in rules:
            self.assertIn(f'network="{DEVNET2_MONITORING_NETWORK}"', rule["expr"], rule["alert"])
            self.assertEqual(rule["labels"]["network"], DEVNET2_MONITORING_NETWORK)
            self.assertEqual(rule["annotations"]["runbook_url"], f"{runbook}#11-operating-rules")

    def test_p5_rehearsal_pod_mounts_only_the_authority_secret(self):
        # Plan S3-E4 K81: the rehearsal signs the activation inside the cluster
        # with the voters' own authority key, and nothing but digests, version
        # bytes and counts leaves the Pod. It runs the P5 arbiter build under the
        # voters' memory limit, in their namespace, without a port, a service
        # account token, another volume or another Secret, and with one label no
        # Service selects.
        (pod,) = [document for document in yaml.safe_load_all((ROOT / P5_REHEARSAL_POD).read_text()) if document]
        overlay = yaml.safe_load((DEVNET2 / "storage-integrity-si-v2.yaml").read_text())
        secret = overlay["arbiter"]["authoritySecret"]
        self.assertEqual(secret, {"name": "storage-integrity-devnet2-si-v2-authority", "key": "private-key"})
        self.assertEqual(arbiter_env(self.control, FRESH_RELEASE)["ARBITER_AUTHORITY_PRIVATE_KEY_HEX"], secret)
        limits = overlay["arbiter"]["resources"]["limits"]
        self.assertEqual(limits, {"cpu": "2", "memory": DEVNET2_VOTER_MEMORY_LIMIT})
        self.assertEqual((pod["apiVersion"], pod["kind"]), ("v1", "Pod"))
        self.assertEqual(pod["metadata"], {
            "name": "storage-integrity-si-v2-p5-rehearsal",
            "namespace": LEGACY_NAMESPACE,
            "labels": {"storage-integrity.sentio.xyz/rehearsal": "p5"},
        })
        self.assertEqual(pod["spec"], {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "nodeSelector": overlay["nodeSelector"],
            "containers": [{
                "name": "rehearsal",
                "image": f"ghcr.io/sentioxyz/arbiter@{DEVNET2_STAGE3_P5_ARBITER_DIGEST}",
                "command": ["sleep", "86400"],
                "env": [{"name": "ARBITER_AUTHORITY_PRIVATE_KEY_HEX", "valueFrom": {"secretKeyRef": secret}}],
                "resources": {"requests": {"cpu": "500m", "memory": limits["memory"]}, "limits": limits},
                "volumeMounts": [{"name": "rehearsal", "mountPath": "/rehearsal"}],
            }],
            "volumes": [{"name": "rehearsal", "emptyDir": {"sizeLimit": "8Gi"}}],
        })
        self.assertEqual(secret_references(pod), {secret["name"]})
        labels = pod["metadata"]["labels"]
        for release, docs in (("storage-integrity-si-v2", self.control), *self.si_hosts.items()):
            for (kind, name), resource in docs.items():
                if kind == "Service":
                    selector = resource["spec"].get("selector") or {}
                    self.assertFalse(selector and selector.items() <= labels.items(), f"{release}: Service {name} selects the rehearsal Pod")
        # No tracked manifest under k8s-* names it: the runbook applies it by hand.
        tracked = subprocess.run(["git", "-C", str(ROOT), "ls-files", "k8s-*"], capture_output=True, text=True, check=True).stdout.split()
        for name in tracked:
            path = ROOT / name
            if path.suffix in (".yaml", ".yml") and path.is_file():
                self.assertNotIn("storage-integrity-si-v2-p5-rehearsal", path.read_text(errors="ignore"), name)
```

Fill the ledger values and run (local only):

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4; T="$HOME/.cache/s3e4-task80"
cd "$W" && python3 -I "$T/fill.py" "$T/ledger.env" charts/storage-integrity/tests/test_render.py
git -C "$W" grep -cE '<(ARBITER_TAG_P5|ARBITER_DIGEST_P5|SN_COMMIT_P5|SN_DIGEST_P5|HG_TAG_P5|CORE_TAG_P5)>' -- charts docs | wc -l | tr -d ' '
python3 -B charts/storage-integrity/tests/test_render.py ActualFreshNetworkTest.test_stage3_p5_host_images_are_reviewed ActualFreshNetworkTest.test_devnet2_stage3_p5_images_follow_the_rollout ActualFreshNetworkTest.test_snapshot_body_needs_a_stage3_arbiter ActualFreshNetworkTest.test_stage3_consensus_monitoring_needs_a_stage3_arbiter ActualFreshNetworkTest.test_p5_rehearsal_pod_mounts_only_the_authority_secret 2>&1 | grep -E '^(FAIL|ERROR|Ran|FAILED|OK)|Error' | cut -c1-160
```

Expected: `filled charts/storage-integrity/tests/test_render.py`; `0`; `Ran 5 tests` and `FAILED (failures=1, errors=1)`: `ERROR: test_p5_rehearsal_pod_mounts_only_the_authority_secret` with `FileNotFoundError: [Errno 2] No such file or directory: '…/docs/examples/storage-integrity-p5-rehearsal-pod.yaml'`, and `FAIL: test_stage3_p5_host_images_are_reviewed` with `AssertionError: 'ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT_P5>@<SN_DIGEST_P5>' not found in (…)` (the build is in no reviewed set yet). The other three pass already: nothing has moved, and no file sets the new keys. Any other failing test: stop.

- [ ] **Step 8: The P5 image in the reviewed sets, and the rehearsal Pod example**

In `charts/storage-integrity/tests/test_render.py`, the P5 build joins the four sets the devnet2 hosts and members need (`:92`, `:112`, `:118`, `:717`):

```diff
-REPLICA_READ_HOST_IMAGES = frozenset({DEVNET2_P3_HOST_IMAGE, DEVNET2_STAGE3_P4_SOURCE_IMAGE})
+REPLICA_READ_HOST_IMAGES = frozenset({DEVNET2_P3_HOST_IMAGE, DEVNET2_STAGE3_P4_SOURCE_IMAGE, DEVNET2_STAGE3_P5_SOURCE_IMAGE})
```

```diff
     DEVNET2_STAGE3_SOURCE_IMAGE,
     DEVNET2_P3_HOST_IMAGE,
     DEVNET2_STAGE3_P4_SOURCE_IMAGE,
+    DEVNET2_STAGE3_P5_SOURCE_IMAGE,
 )
```

```diff
-STAGE3_HOST_IMAGES = frozenset({DEVNET2_STAGE3_SOURCE_IMAGE, DEVNET2_P3_HOST_IMAGE, DEVNET2_STAGE3_P4_SOURCE_IMAGE})
+STAGE3_HOST_IMAGES = frozenset({DEVNET2_STAGE3_SOURCE_IMAGE, DEVNET2_P3_HOST_IMAGE, DEVNET2_STAGE3_P4_SOURCE_IMAGE, DEVNET2_STAGE3_P5_SOURCE_IMAGE})
```

```diff
-MEMBER_IMAGES = frozenset({DEVNET2_MEMBER_IMAGE, DEVNET2_P3_HOST_IMAGE, DEVNET2_STAGE3_P4_SOURCE_IMAGE})
+MEMBER_IMAGES = frozenset({DEVNET2_MEMBER_IMAGE, DEVNET2_P3_HOST_IMAGE, DEVNET2_STAGE3_P4_SOURCE_IMAGE, DEVNET2_STAGE3_P5_SOURCE_IMAGE})
```

Create `docs/examples/storage-integrity-p5-rehearsal-pod.yaml` with the Write tool:

```yaml
# The throwaway P5 rehearsal Pod of devnet2-si-v2 (housegate spec 2026-10-14
# §10.3; Plan S3-E4 K81 and contract §56). Not a deployment file: no chart or
# helmfile renders it. docs/storage-integrity-devnet2-stage3-p5-v21.md applies
# it by hand (§3.2, kubectl apply --dry-run=server first) and deletes it
# (§3.6) before any component rolls.
#
# It runs arbiter <ARBITER_TAG_P5>'s throwaway in-package test binary
# p5rehearsal over copies of the three voters' raft directories, which the
# runbook copies into /rehearsal: three FSMs in one process, each restored from
# its own copy, fed the same signed activation entry, persisted as v21 and
# restored from it, and a forced-mismatch run. It has no network peer,
# orchestrator or anchor. It reads the voters' authority key the way the
# voters do, from their Secret, so the signed activation token never leaves the
# Pod; the runbook reads back only digests, version bytes and counts. Its
# memory limit is the voters' own: the rehearsal measures Persist's peak
# against the voters' budget. Its only label is one no Service selects, and it
# has no port, no service account token and no volume but its emptyDir.
apiVersion: v1
kind: Pod
metadata:
  name: storage-integrity-si-v2-p5-rehearsal
  namespace: sentio-network-devnet2
  labels:
    storage-integrity.sentio.xyz/rehearsal: p5
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  nodeSelector:
    sentio.xyz/general: "true"
  containers:
    - name: rehearsal
      # arbiter <ARBITER_TAG_P5> (ARBITER_DIGEST_P5, Plan S3-E4 Appendix C): the
      # voters' P5 release. The image's entrypoint is the arbiter node, so the
      # command keeps the container idle until the runbook execs the binary.
      image: ghcr.io/sentioxyz/arbiter@<ARBITER_DIGEST_P5>
      command: ["sleep", "86400"]
      env:
        - name: ARBITER_AUTHORITY_PRIVATE_KEY_HEX
          valueFrom:
            secretKeyRef:
              name: storage-integrity-devnet2-si-v2-authority
              key: private-key
      resources:
        requests:
          cpu: 500m
          memory: 2Gi
        limits:
          cpu: "2"
          memory: 2Gi
      volumeMounts:
        - name: rehearsal
          mountPath: /rehearsal
  volumes:
    - name: rehearsal
      emptyDir:
        sizeLimit: 8Gi
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4; T="$HOME/.cache/s3e4-task80"
cd "$W" && python3 -I "$T/fill.py" "$T/ledger.env" docs/examples/storage-integrity-p5-rehearsal-pod.yaml
git -C "$W" grep -cE '<(ARBITER_TAG_P5|ARBITER_DIGEST_P5|SN_COMMIT_P5|SN_DIGEST_P5|HG_TAG_P5|CORE_TAG_P5)>' -- charts docs | wc -l | tr -d ' '
python3 -B charts/storage-integrity/tests/test_render.py ActualFreshNetworkTest.test_stage3_p5_host_images_are_reviewed ActualFreshNetworkTest.test_devnet2_stage3_p5_images_follow_the_rollout ActualFreshNetworkTest.test_snapshot_body_needs_a_stage3_arbiter ActualFreshNetworkTest.test_stage3_consensus_monitoring_needs_a_stage3_arbiter ActualFreshNetworkTest.test_p5_rehearsal_pod_mounts_only_the_authority_secret 2>&1 | tail -3
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
bash scripts/storage-integrity/render-inertness.sh origin/main; echo "exit $?"
git -C "$W" add charts/storage-integrity/tests/test_render.py docs/examples/storage-integrity-p5-rehearsal-pod.yaml && git -C "$W" commit -m "feat(storage-integrity): P5 image constants and the rehearsal Pod example

test_render.py names the stage-3 P5 arbiter release (ARBITER_DIGEST_P5)
and sentio-node build (SN_COMMIT_P5) and lists the build in every image
set a devnet2 host or member key needs, so the P5 runbook's host and
si-member commits stay green (Plan S3-E4 contract §56). New tests hold
the rollout order (arbiter pin, hosts b, c, a, their si-members, then
the stage-3 rules), let arbiter.raft.snapshotBody and the stage-3 rules
be set only on the stage-3 arbiter pin, and pin the throwaway P5
rehearsal Pod: the P5 arbiter image, the voters' authority key from its
Secret and nothing else, the voters' memory limit, one emptyDir, no
port, no token, a label no Service selects (K81).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: `filled docs/examples/storage-integrity-p5-rehearsal-pod.yaml`; `0`; `Ran 5 tests` and `OK`; `Ran <N0+12> tests` and `OK`; the eight `identical` lines of Step 3 and `exit 0`; one commit, `2 files changed`.

- [ ] **Step 9: The workflow paths**

`.github/workflows/storage-integrity-chart.yml`: both `paths` lists (`pull_request` `:9`, `push` `:31`) gain the P5 runbook, whose headings `test_stage3_consensus_rules_read_only_this_networks_series` reads once Task 81 writes it, and the rehearsal Pod example, which `test_p5_rehearsal_pod_mounts_only_the_authority_secret` reads. In each list:

```diff
+      - docs/storage-integrity-devnet2-stage3-p5-v21.md
+      - docs/examples/storage-integrity-p5-rehearsal-pod.yaml
       - charts/storage-integrity/**
       - charts/sentio-node/**
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4
cd "$W" && python3 -I -c 'import sys, yaml; on = yaml.safe_load(open(sys.argv[1]))[True]; print([sum(p in on[k]["paths"] for p in ("docs/storage-integrity-devnet2-stage3-p5-v21.md", "docs/examples/storage-integrity-p5-rehearsal-pod.yaml")) for k in ("pull_request", "push")])' .github/workflows/storage-integrity-chart.yml
if command -v actionlint >/dev/null; then actionlint .github/workflows/storage-integrity-chart.yml && echo "actionlint clean"; fi
git -C "$W" add .github/workflows/storage-integrity-chart.yml && git -C "$W" commit -m "ci(storage-integrity): chart validation runs on the P5 files

The storage-integrity chart workflow now also runs when the stage-3 P5
runbook or the P5 rehearsal Pod example changes: test_render.py reads
both (Plan S3-E4 Tasks 80 and 81).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: `[2, 2]`; `actionlint clean` (when installed); one commit, `1 file changed, 4 insertions(+)`.

- [ ] **Step 10: Rehearse Task 82's file changes (local only)**

Every live commit of Task 82 moves one file and its mirror. This step makes each of them, in order, on a throwaway clone of the branch, runs the whole suite after each, and makes ten out-of-order or half-made changes that must fail. It writes only the clone under `$HOME/.cache/s3e4-task80`, never the worktree, and pushes, syncs and applies nothing.

Write `$HOME/.cache/s3e4-task80/rollout.py` with the Write tool (local, never committed):

```python
"""Rehearse the devnet2 file changes of Plan S3-E4 Task 82 on a scratch clone
of the Task 80 branch (a local tool, never committed).

usage: rollout.py <ledger.env> <scratch clone> forward|bites

forward: makes the single-service changes of contract §57 in their order, one
local commit each, and runs the whole test_render.py after each one: the
verifier and voter pin (V), the hosts b, c, a (H-b, H-c, H-a), their
si-members (M-b, M-c, M-a), the stage-3 rules (MON), and runbook §11's lever
on and off (PLAIN, UNPLAIN). Every state must print OK. The reverts of runbook
§10.1 walk the same states backwards.

bites: from the right earlier state, makes each out-of-order or half-made
change a careless commit could make and requires ActualFreshNetworkTest to
fail with the message that names it. Nothing is committed; the clone is reset
after each case.

The clone is the only tree written. Nothing is pushed, synced or applied.
"""
import re
import subprocess
import sys
from pathlib import Path

import yaml

ENV = dict(line.split("=", 1) for line in Path(sys.argv[1]).read_text().split())
ROOT = Path(sys.argv[2]).resolve()
MODE = sys.argv[3]
TESTS = ROOT / "charts/storage-integrity/tests/test_render.py"
DEVNET2 = ROOT / "k8s-sea/sentio-network-devnet2"
NETWORK = DEVNET2 / "storage-integrity-si-v2.yaml"
P5_TAG = f'sha-{ENV["SN_COMMIT_P5"]}@{ENV["SN_DIGEST_P5"]}'
P5_IMAGE = f"ghcr.io/sentioxyz/sentio-node:{P5_TAG}"
MIRRORS = {"a": "FRESH_SOURCE_IMAGE", "b": "INDEXER_B_SOURCE_IMAGE", "c": "INDEXER_C_SOURCE_IMAGE"}


def git(*args):
    command = ["git", "-C", str(ROOT), "-c", "user.name=rehearsal", "-c", "user.email=rehearsal@localhost", *args]
    return subprocess.run(command, capture_output=True, text=True, check=True).stdout


def sub(path, old, new, count=1):
    text = path.read_text()
    if text.count(old) != count:
        sys.exit(f"rollout: {path.relative_to(ROOT)}: {old[:70]!r} found {text.count(old)} times, want {count}")
    path.write_text(text.replace(old, new))


def constant(name, value):
    """Set a test_render.py constant written as one string or as two string lines in parentheses."""
    text = TESTS.read_text()
    pattern = re.compile(rf'^{name} = (?:"[^"\n]*"|\(\n    "[^"\n]*"\n    "[^"\n]*"\n\))$', re.M)
    if len(pattern.findall(text)) != 1:
        sys.exit(f"rollout: {name} is not one plain string constant in test_render.py")
    TESTS.write_text(pattern.sub(lambda _: f"{name} = {value}", text))


def arbiter():
    old = yaml.safe_load(NETWORK.read_text())["arbiter"]["image"]["digest"]
    sub(NETWORK, f"    digest: {old}\n", f'    digest: {ENV["ARBITER_DIGEST_P5"]}\n')
    constant("DEVNET2_ARBITER_DIGEST", f'"{ENV["ARBITER_DIGEST_P5"]}"')


def host(x, mirror=True):
    path = DEVNET2 / f"sentio-node-indexer-{x}-storage-integrity-si-v2-source-image.yaml"
    old = yaml.safe_load(path.read_text())["node"]["image"]["tag"]
    sub(path, f'    tag: "{old}"\n', f'    tag: "{P5_TAG}"\n')
    if x == "a":
        sub(ROOT / "docs/examples/storage-integrity-fresh-source-image.yaml", f'    tag: "{old}"\n', f'    tag: "{P5_TAG}"\n')
    if mirror:
        constant(MIRRORS[x], f'(\n    "ghcr.io/sentioxyz/sentio-node:sha-{ENV["SN_COMMIT_P5"]}"\n    "@{ENV["SN_DIGEST_P5"]}"\n)')


def member(x):
    path = DEVNET2 / f"sentio-node-indexer-{x}-storage-integrity-si-v2.yaml"
    old = yaml.safe_load(path.read_text())["siMember"]["image"]
    sub(path, f"  image: {old}\n", f"  image: {P5_IMAGE}\n")


def monitoring(mirror=True):
    sub(NETWORK, "  replicaReads:\n    enabled: true\n", "  replicaReads:\n    enabled: true\n  stage3Consensus:\n    enabled: true\n")
    if mirror:
        sub(TESTS, "DEVNET2_STAGE3_CONSENSUS_MONITORING = False\n", "DEVNET2_STAGE3_CONSENSUS_MONITORING = True\n")


def plain(on=True, mirror=True):
    if on:
        sub(NETWORK, "  storageClassName: local-path\n  storageSize: 2Gi\n", "  raft:\n    snapshotBody: plain\n  storageClassName: local-path\n  storageSize: 2Gi\n")
        if mirror:
            sub(TESTS, "DEVNET2_SNAPSHOT_BODY = None\n", 'DEVNET2_SNAPSHOT_BODY = "plain"\n')
    else:
        sub(NETWORK, "  raft:\n    snapshotBody: plain\n", "")
        sub(TESTS, 'DEVNET2_SNAPSHOT_BODY = "plain"\n', "DEVNET2_SNAPSHOT_BODY = None\n")


FORWARD = [
    ("V", arbiter), ("H-b", lambda: host("b")), ("H-c", lambda: host("c")), ("H-a", lambda: host("a")),
    ("M-b", lambda: member("b")), ("M-c", lambda: member("c")), ("M-a", lambda: member("a")),
    ("MON", monitoring), ("PLAIN", plain), ("UNPLAIN", lambda: plain(on=False)),
]


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
            print(output[-3000:])
            sys.exit(1)


def upto(label):
    git("reset", "-q", "--hard", START)
    for name, change in FORWARD:
        if name == label:
            return
        change()
    sys.exit(f"rollout: no step {label}")


BITES = [
    ("V", lambda: host("b"), "a host moves to the P5 build only after the arbiter pin (runbook §4)"),
    ("H-b", lambda: host("c"), "the hosts move to the P5 build in the order b, c, a (runbook §5)"),
    ("H-c", lambda: member("b"), "an si-member moves to the P5 build only after every host (runbook §5.4)"),
    ("M-b", lambda: member("c"), "the si-members move to the P5 build in the order b, c, a (runbook §5.4)"),
    ("M-a", monitoring, "the stage-3 rules come after every host and si-member (runbook §8)"),
    ("V", monitoring, "the stage-3 rules need the stage-3 arbiter pin"),
    ("V", plain, "arbiter.raft.snapshotBody needs the stage-3 arbiter pin"),
    ("H-b", lambda: host("b", mirror=False), "its source-image file and its mirror move together"),
    ("MON", lambda: plain(mirror=False), "'plain' != None"),
    ("H-b", lambda: monitoring(mirror=False), "True is not False"),
]


def bites():
    failed = 0
    for state, change, message in BITES:
        upto(state)
        change()
        code, output = run_tests("ActualFreshNetworkTest")
        caught = code != 0 and message in output
        failed += not caught
        print(f"{'refused' if caught else 'NOT REFUSED'}: before {state}, {message}", flush=True)
    git("reset", "-q", "--hard", START)
    sys.exit(1 if failed else 0)


START = git("rev-parse", "HEAD").strip()
{"forward": forward, "bites": bites}[MODE]()
```

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4; T="$HOME/.cache/s3e4-task80"
R="$(mktemp -d "$T/rehearse.XXXXXX")" && printf '%s\n' "$R" > "$T/rehearse-path"
git clone -q --no-hardlinks --branch urwt/claude/multi-source-s3e4 "$W" "$R/repo"
python3 -I -B "$T/rollout.py" "$T/ledger.env" "$R/repo" bites
```

Expected: ten lines, each `refused: …`: `refused: before V, a host moves to the P5 build only after the arbiter pin (runbook §4)`, `refused: before H-b, the hosts move to the P5 build in the order b, c, a (runbook §5)`, `refused: before H-c, an si-member moves to the P5 build only after every host (runbook §5.4)`, `refused: before M-b, the si-members move to the P5 build in the order b, c, a (runbook §5.4)`, `refused: before M-a, the stage-3 rules come after every host and si-member (runbook §8)`, `refused: before V, the stage-3 rules need the stage-3 arbiter pin`, `refused: before V, arbiter.raft.snapshotBody needs the stage-3 arbiter pin`, `refused: before H-b, its source-image file and its mirror move together`, `refused: before MON, 'plain' != None` and `refused: before H-b, True is not False`; exit 0. A `NOT REFUSED` line: the test that should catch that change does not; stop and fix it.

The forward run takes about twelve minutes (ten full suites); run it in the background and read its output when it exits:

```bash
T="$HOME/.cache/s3e4-task80"; R="$(cat "$T/rehearse-path")"
python3 -I -B "$T/rollout.py" "$T/ledger.env" "$R/repo" forward
git -C "$R/repo" log --oneline -10 | cat
```

Expected: ten lines `V: OK (<N0+12> tests)`, `H-b: OK (…)`, `H-c: OK (…)`, `H-a: OK (…)`, `M-b: OK (…)`, `M-c: OK (…)`, `M-a: OK (…)`, `MON: OK (…)`, `PLAIN: OK (…)`, `UNPLAIN: OK (…)`, then the ten `rehearsal <label>` commits. A `FAILED` state prints the suite's tail: one of Task 82's commits would turn the suite red; fix the test or the constant in the worktree, commit, and run this step (a new clone) and Step 11 again.

The clone stays under `$HOME/.cache/s3e4-task80` (local, rehearsal commits only); nothing reads it again, and a new run of this step makes a new one. The user may delete it.

- [ ] **Step 11: Full production checks**

The production checks of Plan S3-B contract §15 on the finished branch (`origin/main` as fetched in Step 1), with a fourth lint profile that turns every new value on:

```bash
W=/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4
cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
helm lint charts/storage-integrity -f charts/storage-integrity/tests/legacy-devnet2-values.yaml --namespace sentio-network-devnet2 --strict 2>&1 | tail -1
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --strict 2>&1 | tail -1
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --set arbiter.consensusUpdatesEnabled=true --set arbiter.authoritySigningEnabled=false --strict 2>&1 | tail -1
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --set monitoring.enabled=true --set monitoring.stage3Consensus.enabled=true --set arbiter.raft.snapshotBody=plain --strict 2>&1 | tail -1
bash -n scripts/storage-integrity/render-inertness.sh && bash scripts/storage-integrity/render-inertness.sh origin/main; echo "exit $?"
git -C "$W" status --short | wc -l | tr -d ' '
git -C "$W" log --oneline origin/main..HEAD | cat
```

Expected: `Ran <N0+12> tests` and `OK` (236 in the drafting rebuild); four times `1 chart(s) linted, 0 chart(s) failed`; the eight `identical` lines of Step 3 and `exit 0`; `0`; four commits (`ci(storage-integrity): chart validation runs on the P5 files`, `feat(storage-integrity): P5 image constants and the rehearsal Pod example`, `feat(storage-integrity): the stage-3 consensus rule group, off`, `feat(storage-integrity): the voters' snapshot body writer value, unset`).

Keep for Task 81, which adds the runbook to this branch, reruns these checks for its PR body and writes them into the `P5 PR` run record with `P5_PR`: the branch head commit, `Ran <N0+12> tests` (`N0` at `origin/main`), the four lint results, the eight `identical` lines and Step 10's two results. The branch stays local; Task 80 records no ledger value and pushes nothing.

---

## Task 81: production — the P5 runbook and the P5 PR (S3-E4)

Contract §57 (runbook, names, evidence, the activation command, the rehearsal, `P5_PR`), clarifications K81 and K82 and decision D37: Task 80 put the stage-3 rule group, the `arbiter.raft.snapshotBody` value, the rehearsal Pod example, the P5 image constants and their render tests on the branch `urwt/claude/multi-source-s3e4`; this task adds the runbook Task 82 executes and every `runbook_url` of Task 80's rule group points into, `docs/storage-integrity-devnet2-stage3-p5-v21.md` (sections exactly as contract §57 lists them, helpers and evidence files as it names them), points the stage-2 runbook's image-floor rule at it, and takes the P5 PR (Tasks 80 and 81 together) through push, review, CI and merge, then records `P5_PR`. Merging it applies, syncs and turns on nothing: every live change of P5 is a single-service commit, the hand-applied rehearsal Pod or the activation, which the runbook makes after the merge, each confirmed, and the PR renders every release byte-identically (`render-inertness.sh origin/main`).

**Files** (anchors are production `origin/main` at `75514e3de`, whose SI paths equal today's `origin/main` `206c42357`; the branch is Task 80's `urwt/claude/multi-source-s3e4`, which starts from an `origin/main` that carries the runbooks of Plans S3-B, S3-C1, S3-C3, S3-C4 and S3-D; the hunk rule applies):
- Create: `docs/storage-integrity-devnet2-stage3-p5-v21.md`
- Modify: `docs/storage-integrity-devnet2-multi-source-tracks.md` (§11, the first bullet, which begins `- After §6.2, never run a voter, verifier or SNode below` at base `:1072`; match it by its text: later plans may have moved it; no plan before this one edits it)
- Modify after the merge (housegate, on this plan's ledger copy, with the record procedure of Plan S3-C1 contract §19): Appendix C of `docs/superpowers/plans/2026-10-15-multi-source-si-s3e4-p5-floor-release-activation.md` (the `P5_PR` row and the run record `P5 PR (Task 81)`), one commit in the URWT worktree `multi-source-s3e4-ledger-81`
- Test (local, never committed, in `$T` = `$HOME/.cache/s3e4-task81/`, mode 700): the conventions check `check_runbook.py` (Step 2, the failing test), the fill `fill_runbook.py` (Step 5), the fixture builder `fixtures.bash` and the helper self-test `selftest.bash` (Step 6), the ledger values `ledger.txt` (Step 1); `charts/storage-integrity/tests/test_render.py` is unchanged by this task (its count stays Task 80's), as are the three `helm lint --strict` profiles and `scripts/storage-integrity/render-inertness.sh`, which Step 6 runs.

**Interfaces:**
- Consumes (Task 80, contract §56, verbatim): values `arbiter.raft.snapshotBody` (unset) and `monitoring.stage3Consensus.{enabled, heldCleanupAgeWarnSeconds, runbookURL}` (`runbookURL` = `https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p5-v21.md`); the rule group `<fullname>.arbiter-stage3` (`storage-integrity-si-v2.arbiter-stage3` on devnet2) with `ArbiterSnapshotEncodingFallback`, `ArbiterBlockSealGroupSwitch`, `ArbiterVerifiersUnavailable`, `ArbiterCopiesBackpressure`, `ArbiterUnderReplicatedCleanups`, `ArbiterGroupsUnderReplicated`, `ArbiterFloorHeldCleanupAgeHigh`, `ArbiterMemberLagging`, each with `runbook_url` `<runbookURL>#11-operating-rules`; `docs/examples/storage-integrity-p5-rehearsal-pod.yaml` (Pod `storage-integrity-si-v2-p5-rehearsal`, label `storage-integrity.sentio.xyz/rehearsal: p5`, container `rehearsal` on `ghcr.io/sentioxyz/arbiter@<ARBITER_DIGEST_P5>` running `["sleep", "86400"]`, env `ARBITER_AUTHORITY_PRIVATE_KEY_HEX` from Secret `storage-integrity-devnet2-si-v2-authority` key `private-key`, emptyDir `rehearsal` `sizeLimit: 8Gi` at `/rehearsal`, limits cpu `"2"` memory `2Gi`, `restartPolicy: Never`, `automountServiceAccountToken: false`); the `test_render.py` constants `DEVNET2_STAGE3_P5_ARBITER_DIGEST`, `DEVNET2_STAGE3_P5_SOURCE_IMAGE` (in `WRITER_AUTHORIZATION_IMAGES` and `STAGE3_HOST_IMAGES`, and in Plan S3-C3's `MEMBER_IMAGES`: see the contract note below), `STAGE3_P5_HOST_IMAGES`, `DEVNET2_SNAPSHOT_BODY = None`, `DEVNET2_STAGE3_CONSENSUS_MONITORING = False`, `P5_REHEARSAL_POD`, and the earlier `DEVNET2_ARBITER_DIGEST`, `INDEXER_B_SOURCE_IMAGE`, `INDEXER_C_SOURCE_IMAGE`, `FRESH_SOURCE_IMAGE`, `DEVNET2_STATE_LIMIT_BYTES`.
- Consumes (Task 78, contract §55 and §57, decision D37): `$R5` = `$HOME/si-devnet2-multisource-s3e-rehearsal` (mode 700) with `old_tag.txt`, `new_tag.txt`, `arbiter-release` (line 1 tag, line 2 digest), `bin/old/{statedigest,v20check,snapdump}`, `bin/new/{statedigest,v21check,snapdump,p5rehearsal}`, `p5rehearsal/p5rehearsal_test.go`, `v21-fixture.container` and `p5-files.sha256` (the `shasum -a 256` lines of those eight files, paths relative to `$R5`); `v21check`'s `restore_ok version <n> body <none|plain|interned>` and `restore_refused version <n>: <error>`; `statedigest -data-dir -config` (lines `snapshot_index`, `last_index`, `applied_commands`, `written_version`, `live_check`, `key`, `spent_ids`, `snapshot_sha256`, `apply_error`), `snapdump -data-dir -config -out`, `v20check <container>`; `p5rehearsal -test.run '^TestP5Rehearsal$' -test.v -rehearsal.dir /rehearsal -rehearsal.params /rehearsal/stage3-params.json` with the result lines of contract §57 (`voter <i> restored epoch=<e> promotion_seq=<p> drain_refusal=<…>`, `digest <i> <sha256>`, `activation applied epoch=<e+1>`, `persist <i> version 21 body interned bytes=<n> fallback=0 persist_peak_heap_bytes=<n>`, `restart <i> restore_ok version 21 body interned digest_equal=true`, `mismatch <i> version 21 body plain fallback=1 digest_equal=true`, `rehearsal ok`).
- Consumes (Task 79, contract §56): sentio-node `SN_COMMIT_P5` (every SNode and si-member advertises `stage3_v1`; `sentio_node_storage_integrity_keeper_findings{reason}` on `:9091`, reasons `unknown_replica` and `non_incarnation_root`; `storage_integrity.member.consensus` off and absent by default).
- Consumes (Plan S3-E2 contract §45–§46): `arbiter-admin consensus update --stage3 --indexer-failure-domain 'id=N,domain=D' --stage3-max-lag-publications --stage3-lag-low --stage3-evidence-timeout-l2-blocks --stage3-max-uncleaned-parts-per-partition --stage3-max-uncleaned-parts-per-table --voter …` (its gate `stage3 gate refused; nothing was submitted:`, the leader's `stage3 gate refused: …`, metadata `arbiter-stage3-voters`, the post-commit reports), `consensus show`'s `current.stage3` (proto names, uint64 as strings) and `stage3_state` (`activation_epoch`, `activation_block_seq`, `activation_promotion_seq`, `switches`), the refusals `consensus update must carry stage3 once set` and `stage3 is active; this binary does not support stage-3 consensus`, the v21 container (version byte 21, body byte 0 plain or 1 interned, 14-byte envelope), `arbiter_snapshot_encoding_fallback_total`, `arbiter_snapshot_body_bytes{kind}`, `raft.snapshot_body`; Plan S3-A's hold lines (`took a consensus hold of mode drain on the leader (hold id <id>, expires <t>)`, `waiting for the leader's drain hold: <progress>`, `the leader drained at promotion_seq <n>; signing the update`, `released consensus hold <id>`).
- Consumes (the ledger, read with `s3-ledger.py get`, contract §57): `ARBITER_TAG_P5`, `ARBITER_DIGEST_P5` (Task 78); `SN_COMMIT_P5`, `SN_DIGEST_P5` (Task 79); `CORE_TAG_P5`, `HG_TAG_P5`, `PROTO_TAG_P5` (Plan S3-E1); `ARBITER_TAG_P4`, `ARBITER_DIGEST_P4`, `SN_COMMIT_P4`, `SN_DIGEST_P4`, `DEVNET2_FAILURE_DOMAINS` (Plan S3-D); `MEMBER_IDS` (Plan S3-C3); `INDEXER_C_ID`, `ENROLMENT_EPOCH` (Plan S3-B). The runbook itself reads `P4_END` and `P5_PR` at run time (§1.1).
- Consumes (the earlier runbooks' §0 helpers along the chain dynamic-table-set → multi-source → multi-source-tracks → stage3-p0 → lockdown → indexer-c → p1 → p2 → p3 → p4-hardening → p4-owner-rebuild, D20): `si_kubectl`, `si_arbiter`, `si_voter`, `si_metrics`, `si_capability`, `si_leader`, `si_show`, `si_images` (dynamic-table-set); `si_node_image`, `si_replace_voter`, `si_status` (multi-source); `si_tracks`, `si_track_published`, `si_global`, `si_sealed`, `si_wait_track`, `si_accept_a_track`, `si_agent_start`, `si_agent_stop`, `si_agent_ch` (multi-source-tracks); `si_series_value`, `si_voter_series`, `si_state_size`, `si_vm_query`, `si_vm_value`, `si_vmalert_groups`, `si_head_is`, `si_revert_stage3` (stage-3 P0); `si_secret_keys` and, through `si_render_diff_is`, `si_render_diff` (lockdown); `si_rpc_on`, `si_info`, `si_audit` (indexer-c, with `c`); `si_member_pod`, `si_member_metrics`, `si_copies_check`, `si_budget_capture`, `si_budget` (P2); `si_container_image`, `si_pod_state`, `si_replica_hosts`, `si_host_replica_metrics`, `si_render_diff_is` (P3); the P1 runbook's `devuser1.s3_p1` (`id UInt64, p UInt8, v String`, owned by indexer-b, kept for good) and the indexer-c runbook's `<db>.s3e_e2e` (`$HOME/si-devnet2-stage3-indexer-c/acceptance-db`); `$HOME/si-devnet2-stage3-p4-rebuild/p4-end.txt` (P4).
- Produces (contract §57, verbatim): the runbook with its sections; `si_backup="$HOME/si-devnet2-stage3-p5"`, `R5="$HOME/si-devnet2-multisource-s3e-rehearsal"`; the helpers `si_stage3`, `si_features_stage3`, `si_v21check <ordinal>`, `si_encoding <ordinal>`, `si_rehearsal <cmd…>`, `si_member_sts <a|b|c>`, `si_p5_image`; `$si_backup/pins` (one value per line: `ARBITER_TAG_P5`, `ARBITER_DIGEST_P5`, `SN_COMMIT_P5`, `SN_DIGEST_P5`, `ARBITER_TAG_P4`, `ARBITER_DIGEST_P4`, `SN_COMMIT_P4`, `SN_DIGEST_P4`) and `$si_backup/stage3-params.json`; the evidence files of contract §57; the activation command of §7.2; the stage-2 runbook's pointer sentence; the PR title `feat(storage-integrity): devnet2 stage 3 P5 (snapshot v21 activation runbook, monitoring, rehearsal pod)`; the ledger value `P5_PR` (Step 11).
- Produces (this task's names, recorded in Appendix B): helpers `si_p5_value <NAME>` (reads `$si_backup/ledger-values.txt`), `si_p5_owner a|b|c`, `si_p5_member_keys a|b|c`, `si_p5_keeper_findings a|b|c`, `si_p5_put <file> <pod path>`, `si_p5_accept <key-file|-> a|b|c <n> <tag>`, `si_p5_py` (subcommands `params`, `stage3`, `features`, `activation`, `rehearsal`, `header`, `pod`, `pod-live`); the acceptance values `5401` (`devuser1.s3_p1`, §5.1), `5402` (`<db>.s3e_e2e`, §5.2), `5403` (§5.3) and `5404` (§7.3) (`devnet101.swap_new2`), 4 promotions in all; the rehearsal Pod's inputs `/rehearsal/{p5rehearsal,stage3-params.json,rehearsal-copy-<i>.tgz,arbiter-<i>.yaml}`, its log `/rehearsal/rehearsal.log` and the containers it is expected to write, `/rehearsal/rehearsal-v21-<i>.container`; the line `P5_REHEARSAL=<date>` in `rehearsal.txt`; the extra evidence files `ledger-values.txt`, `stage3-params.txt`, `identities.txt`, `show-before.json`, `images-before.txt`, `member-images-before.txt`, `features-before.json`, `budget-voter`, `budget-snapshots.tgz`, `budget-live-seq`, `budget-start.txt`, `replay-baseline.txt`, `show-replay.json`, `arbiter-<i>-raft.tgz`, `arbiter-<i>.yaml`, `digest-<i>-<old|new>.{cmp,err}`, `written-<i>-<old|new>.container`, `replay.txt`, `rehearsal-show-{before,after}.json`, `rehearsal-arbiter-<i>.yaml`, `rehearsal-digest-<i>.{txt,err,cmp}`, `rehearsal-copies.txt`, `p5rehearsal_test.go`, `rehearsal-pod-check.txt`, `rehearsal-pod-applied`, `rehearsal-pod.json`, `rehearsal-inputs.txt`, `rehearsal-activation-check.txt`, `rehearsal-mismatch-check.txt`, `verify-verifiers.txt`, `member-<x>-uid`, `replica-hosts-<x>-{before,after}.json`, `verify-indexer-<x>.txt`, `accept-b-5401.txt`, `accept-c-5402.txt`, `accept-a-5403.txt`, `accept-a-5404.txt`, `member-window-<x>.txt`, `member-window-<x>-host-after.txt`, `verify-member-<x>.txt`, `replace-voter-<k>.txt`, `verify-voters.txt`, `state-<i>.bin`, `tracks-before-activation.json`, `verify-before-activation.txt`, `features-at-activation.json`, `tracks-at-activation.json`, `verify-after-activation.txt`, `monitoring-before.txt`, `vmalert-stage3.txt`, `vmrule-live.json`, `vmrule-stage3-selectors.txt`, `monitoring-after.txt`, `voter-order-rollback`, `rollback-voter-<k>.txt`, `verify-rollback.txt`; the local test files of **Files**.

Notes (drafting decisions, Appendix B):
- **Inert on merge.** This task changes two documents. With Task 80's commits the PR adds values that are unset or off, an example nothing applies (no GitOps on sea, validation-only workflows) and constants that only the render tests read, and every release renders byte-identically to `origin/main` (Step 6 runs the inertness script). The live steps are Task 82's, each confirmed in chat.
- **PR-time values and the rehearsal's source.** The runbook text holds the ledger values in angle brackets (`<ARBITER_TAG_P5>`, `<ARBITER_DIGEST_P5>`, `<SN_COMMIT_P5>`, `<SN_DIGEST_P5>`, `<ARBITER_TAG_P4>`, `<ARBITER_DIGEST_P4>`, `<SN_COMMIT_P4>`, `<SN_DIGEST_P4>`, `<CORE_TAG_P5>`, `<HG_TAG_P5>`, `<PROTO_TAG_P5>`, `<INDEXER_C_ID>`, `<ENROLMENT_EPOCH>` and `<ENROLMENT_EPOCH + 1>`, `<DEVNET2_FAILURE_DOMAINS>`) and the rehearsal's source as the single line `<P5REHEARSAL_SOURCE>` of §3.2's go block with its checksum `<P5REHEARSAL_SHA256>`; Step 5 fills them and proves none is left. Task 78 is the only source of the rehearsal's text (contract §57: "Task 78's step is the source of its text"), so Step 5 inserts `$R5/p5rehearsal/p5rehearsal_test.go` byte for byte, refuses it unless its SHA-256 is that file's `p5-files.sha256` line, and §3.2 checks at run time that the go block, that line, `<P5REHEARSAL_SHA256>` and `$R5`'s file are one text. Runtime values (`/path/to/housegate`, `/path/to/devuser1-owner.key`, `<x>`, `<i>`, `<n>`, `<db>`) stay: every block that needs one refuses its placeholder or sets it in its first line.
- **Pins and the earlier helpers.** Contract §57 fixes `$si_backup/pins` as eight bare values in order (`si_p5_image` reads lines 3 and 4, the stage-1 runbook's shape); several earlier helpers read a pins, repos or previous-pins file of their own runbook's shape under `$si_backup` (`si_stage1_image` … `si_p4_accept`, 52 in the chain, among them the hardening runbook's `si_pin` and `si_indexer_c_id` and therefore `si_owner_id` and `si_quiet_point`). None of them is called: the runbook's ledger values go into `ledger-values.txt` (`si_p5_value`), indexer-c's id comes from `si_p5_owner c`, and the conventions check (Step 2) computes that set from the chain and fails on any block or helper of this runbook that reaches it. No helper of this runbook redefines an earlier one (also checked), so Plan S3-F's chain, which starts here, reaches every earlier helper unchanged.
- **What leaves the rehearsal Pod.** K81 and the Keys constraint allow only digests, version bytes and counts out of the Pod, while contract §57 names the evidence file `rehearsal-v21-0.container` and §3.5's check `"$R5/bin/old/v20check" "$si_backup/rehearsal-v21-0.container"`. `ARBITER_TAG_P4`'s reader refuses a container on its version byte before reading anything after it (`arbiter:fsm/snapshot.go:274`: magic, then the version byte, then the refusal), so §3.5 copies out only the first 14 bytes (magic, version 21, body byte 1, document length) under that name and runs exactly that check; the body, which holds the rehearsal's signed update, stays in the Pod, and Task 78's integration fixture `$R5/v21-fixture.container`, which holds no live state, is refused in full by the same tool in §2.1. §3.3 runs contract §57's command inside the Pod with its output into `/rehearsal/rehearsal.log` and copies out only the result lines, the test verdict and failure lines without a JWS (`eyJ`), so a token printed by mistake never reaches the operator's disk; `si_p5_py rehearsal` refuses a `rehearsal.txt` that holds one anyway.
- **Equal copies.** The rehearsal compares three digests before it applies anything, so the three copies must hold the same applied log; the registry cursor advances once a minute, so three sequential copies can straddle an entry. §3.1 replays each copy locally with `$R5/bin/new/statedigest` and requires one `last_index`, one set of `key` lines and one `snapshot_sha256` (and an unchanged epoch and promotion sequence around the copies), and is re-runnable; a copy whose drain gate does not pass shows as `drain_refusal=` other than `""` and sends the run back to §3.1. The voters' `arbiter.yaml` copies (no secret: `config.Load` takes the anchor key from the environment) go into the Pod beside the raft copies, so the rehearsal can build each FSM's genesis parameters as `statedigest` does.
- **Acceptance writes and the budget.** One write per rolled host before the activation (indexer-b into `devuser1.s3_p1`, which the P1 runbook keeps for good, indexer-c into `<db>.s3e_e2e`, indexer-a into `devnet101.swap_new2`) and one after it: 4 promotions, no table-set transition, so §1.3 needs a budget of at least 4. The writes of indexer-b and indexer-c run in the user's terminal (the `devuser1` owner key), as in every earlier stage-3 runbook. `si_p5_accept` is the P4 runbooks' `si_p4_accept` over this runbook's tables and `si_p5_owner`; the P0 runbook's `si_host_series` accepts only `a` and `b`, so `si_p5_keeper_findings` reads indexer-c's `:9091` itself.
- **si-members.** Contract §57's member commits change `siMember.image` only, in each host's overlay, as Plan S3-C4's member window did (its `W-<x>-m` commits touched only the overlay); Plan S3-C3's render test lets a values file name only an image of `MEMBER_IMAGES`, so the P5 image must be in that set before §5.4 (§1.1 and Step 1 check it). The members stay in P2/P3 mode: `si_p5_member_keys` reads each member's rendered `si-member.yaml` and expects no `consensus:` key, before the rolls (§1.3) and after each (§5.4).
- **`si_v21check` on a v20 container.** Contract §57 has it print `version <byte> body <byte>`; a v20 container has no body byte (byte 5 is the first byte of its document length), so the helper prints `body -` there and the body byte only for version 21. It skips a newest snapshot directory ending in `.tmp` (a snapshot being written), as the S3-B §13.4 estimator does.
- **The freeze after the activation.** Writes stop from §3.1 until §7.3 (copies, rolls, drain); after §7.3 they may resume, because the first v21 snapshot needs `snapshot_threshold` (8192) new log entries and §8 may wait days for it otherwise. No consensus update runs until §9 records `P5_END` (Plan S3-F starts there).
- **Monitoring.** The stage-3 group is turned on after the activation, once every voter exports `arbiter_snapshot_encoding_fallback_total` (S3-E2 exports it from P5 on); the other seven rules read series that appear with Plan S3-F's switches (a labelled counter only at its first event), so `none` is their healthy answer in §8 and the runbook says so instead of predicting which ones the P5 build exports at zero.
- **The budget after v21.** The stage-3 P0 runbook's §13.4 estimator (`si_budget`, used by every runbook since P1) reads the 13-byte v20 envelope and a JSON document; on a v21 container (a body byte after the version byte, and an interned body) it fails. This runbook needs no budget after §7.2 (its one post-activation write is counted in §1.3's budget); §11 records the limit, and Plan S3-F's §1.4 needs a v21-aware reader (contract note below).
- **Contract notes for other tasks.** Task 80 must list `DEVNET2_STAGE3_P5_SOURCE_IMAGE` in `MEMBER_IMAGES` (contract §56 names only `WRITER_AUTHORIZATION_IMAGES` and `STAGE3_HOST_IMAGES`; without it §5.4's member commits fail `test_render.py`). Task 78's `p5rehearsal` must read `<dir>/rehearsal-copy-<i>.tgz` (a gzip tar holding `raft/`, as `tar -C /data -czf - raft` writes it) for `i` = 0, 1, 2 and may read `<dir>/arbiter-<i>.yaml`; decode `-rehearsal.params` with `encoding/json` into `arbiter.Stage3Params` (field names the proto names, numbers as numbers, `indexer_failure_domains` as `{indexer_id, failure_domain}` objects, the file of runbook §1.2); read the key only from `ARBITER_AUTHORITY_PRIVATE_KEY_HEX`; print `drain_refusal=""` for a passing drain gate; write the interned v21 container of voter `i` to `<dir>/rehearsal-v21-<i>.container`; and never print the signed update or any part of a JWS. Plan S3-F's promotion budget after the activation cannot use `si_budget` unchanged.
- **Verification while drafting** (2026-10-16, never on a repository or a worktree): the scratch tree was a `git archive` export of production `origin/main` `206c42357` (git-initialised), with the eleven earlier runbooks extracted from the current plan texts (Plan S3-B Tasks 14, 17 and 20, Plan S3-C1 Task 27, Plan S3-C3 Task 39, Plan S3-C4 Task 47, Plan S3-D Task 54) and this task's runbook and pointer; Step 2's check passed on the unfilled runbook (33 headings, 59 bash blocks, 30 confirmed, 4 Python programs, the chain of 290 functions with 14 of this runbook's, 61 earlier helpers reached, none of the 52 that read another runbook's pins) and refused, each in turn, a removed confirmation, a redefined earlier helper, a call of `si_owner_id`, a changed activation command and a helper calling `si_pin`. A second scratch tree stood in for Task 80's branch (the P5 constants appended to `test_render.py`, a Pod example written from contract §56) and a stand-in `$R5` for Task 78's (a placeholder rehearsal source, stand-in tools that print the real tools' contract lines, a linux/amd64 stand-in binary, a synthetic v21 fixture); there Step 5's fill, the `--filled` check and Step 6's self-test passed (46 checks under bash 5.3.15 and `/bin/bash` 3.2.57: the chain, every `si_p5_py` check with its refusals, §1.1's constants check, §2.1's and §3.2's read-only blocks against the stand-ins, `si_v21check` behind a newer `.tmp` directory, the in-pod result filter dropping a JWS line, `si_p5_put`, `si_p5_member_keys`, `si_p5_keeper_findings`, `si_p5_accept`); `shellcheck` 0.11.0 was clean on all 59 blocks (SC2154 excluded: names defined along the chain); `test_render.py` ran 86 tests OK with the two documents; the three `helm lint --strict` profiles passed. Not run: `render-inertness.sh` (Plan S3-B's script, not on `origin/main` yet), Task 80's own render tests and Task 78's real tools (both drafted in parallel), and anything against a cluster, a registry or GitHub.

- [ ] **Step 1: Worktree, Task 80's commits and the ledger inputs (read-only)**

Requires Task 80's commits on the branch `urwt/claude/multi-source-s3e4` (which started from `origin/main` after `P4_PR` merged), the records of Task 78 (`ARBITER_TAG_P5`, `ARBITER_DIGEST_P5`, and `$R5` rebuilt from that tag with `p5rehearsal` and `p5-files.sha256`) and Task 79 (`SN_COMMIT_P5`, `SN_DIGEST_P5`) on this plan's ledger copy, and `DEVNET2_FAILURE_DOMAINS` (Plan S3-D Task 55) on Plan S3-D's. Stop if any is missing. The fetches below read remotes (GitHub): they need explicit user confirmation in chat before they run. The worktree is Task 80's (URWT; the script reuses the existing branch):

```bash
git -C /Users/uranuswch/Dev/projects/sentio-network/production fetch -q origin
W="$(bash ~/.agents/skills/urwt/scripts/urwt-create.sh /Users/uranuswch/Dev/projects/sentio-network/production claude multi-source-s3e4 | jq -r .path)"; echo "$W"
git -C "$W" status --porcelain | wc -l | tr -d ' '
git -C "$W" log --oneline origin/main..HEAD
git -C "$W" diff --name-only origin/main...HEAD
```

Expected: `/Users/uranuswch/src/remotesrc/claude/production/multi-source-s3e4`, `0` (clean), Task 80's commits and nothing else, and exactly the files of Task 80's **Files** list, neither of this task's two documents. Prefix every later command with `W=<that path>; T="$HOME/.cache/s3e4-task81";`. The ledger values: run `mkdir -p -m 700 "$HOME/.cache/s3-ledger"` and write `$HOME/.cache/s3-ledger/s3-ledger.py` with the Write tool (the ledger tool at the end of Plan S3-C1 contract §19, verbatim; an earlier copy is the same text and is overwritten), then:

```bash
h=/Users/uranuswch/Dev/projects/sentio-network/housegate; L="$HOME/.cache/s3-ledger"; T="$HOME/.cache/s3e4-task81"
mkdir -p -m 700 "$T"
git -C "$h" fetch -q origin
python3 -I "$L/s3-ledger.py" get "$h" ARBITER_TAG_P5 ARBITER_DIGEST_P5 SN_COMMIT_P5 SN_DIGEST_P5 ARBITER_TAG_P4 ARBITER_DIGEST_P4 SN_COMMIT_P4 SN_DIGEST_P4 CORE_TAG_P5 HG_TAG_P5 PROTO_TAG_P5 INDEXER_C_ID ENROLMENT_EPOCH DEVNET2_FAILURE_DOMAINS MEMBER_IDS > "$T/ledger.txt" || { rm -f "$T/ledger.txt"; exit 1; }
python3 -I "$L/s3-ledger.py" empty "$h" P5_PR
cat "$T/ledger.txt"
```

Expected: `s3-ledger: P5_PR not recorded yet in <ref>:docs/superpowers/plans/2026-10-15-multi-source-si-s3e4-p5-floor-release-activation.md` and fifteen `NAME=value` lines in that order; a `stop`: stop and ask, never type a value in. Then Task 78's rehearsal directory, the runbooks the new one chains on, and Task 80's constants in the shape the runbook reads (local only):

```bash
cd "$W" && T="$HOME/.cache/s3e4-task81"; R5="$HOME/si-devnet2-multisource-s3e-rehearsal"
v() { sed -n "s/^$1=//p" "$T/ledger.txt"; }
[ "$(cat "$R5/new_tag.txt")" = "$(v ARBITER_TAG_P5)" ] && [ "$(sed -n 2p "$R5/arbiter-release")" = "$(v ARBITER_DIGEST_P5)" ] || { echo "$R5 is not Task 78's rebuild from ARBITER_TAG_P5; stop"; exit 1; }
(cd "$R5" && shasum -a 256 -c p5-files.sha256) | grep -c ': OK$'
grep -c '  p5rehearsal/p5rehearsal_test.go$' "$R5/p5-files.sha256"
test ! -e docs/storage-integrity-devnet2-stage3-p5-v21.md && echo "no P5 runbook yet"
for d in dynamic-table-set multi-source multi-source-tracks stage3-p0 stage3-lockdown stage3-indexer-c stage3-p1-backups stage3-p2-members stage3-p3-replica-reads stage3-p4-hardening stage3-p4-owner-rebuild; do printf '%s ' "$(grep -c '^## 0. Session helpers$' "docs/storage-integrity-devnet2-$d.md")"; done; echo
grep -c '^- After §6.2, never run a voter, verifier or SNode below ' docs/storage-integrity-devnet2-multi-source-tracks.md
test -s docs/examples/storage-integrity-p5-rehearsal-pod.yaml && echo "Task 80's Pod example is present"
python3 -I -B - "$(v ARBITER_DIGEST_P5)" "$(v SN_COMMIT_P5)" "$(v SN_DIGEST_P5)" <<'EOF'
import sys
sys.path.insert(0, "charts/storage-integrity/tests")
import test_render as t
image = "ghcr.io/sentioxyz/sentio-node:sha-%s@%s" % (sys.argv[2], sys.argv[3])
assert t.DEVNET2_STAGE3_P5_ARBITER_DIGEST == sys.argv[1] and t.DEVNET2_STAGE3_P5_SOURCE_IMAGE == image, "Task 80's P5 constants are not the ledger's"
assert t.STAGE3_P5_HOST_IMAGES == frozenset({image}) and image in t.WRITER_AUTHORIZATION_IMAGES and image in t.STAGE3_HOST_IMAGES
assert image in t.MEMBER_IMAGES, "Task 80 must list the P5 image in MEMBER_IMAGES: runbook §5.4 moves the si-members onto it"
assert t.DEVNET2_SNAPSHOT_BODY is None and t.DEVNET2_STAGE3_CONSENSUS_MONITORING is False and t.P5_REHEARSAL_POD == "docs/examples/storage-integrity-p5-rehearsal-pod.yaml"
print("Task 80's constants name the ledger's P5 images, for hosts and members; snapshot body unset; stage-3 monitoring off")
EOF
grep -c 'runbookURL: https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p5-v21.md$' charts/storage-integrity/values.yaml
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
```

Expected: `8` (every file of `$R5/p5-files.sha256` checks), `1`; `no P5 runbook yet`; `1 1 1 1 1 1 1 1 1 1 1` (each earlier runbook has its §0); `1`; `Task 80's Pod example is present`; `Task 80's constants name the ledger's P5 images, for hosts and members; snapshot body unset; stage-3 monitoring off`; `1`; `Ran <N> tests` and `OK`, `<N>` the count Task 80 reported (keep it for Step 6). An `AssertionError` or a missing line: the runbook and Task 80's files disagree; reconcile them (contract §56 is binding; the `MEMBER_IMAGES` entry is a Task 80 change, Appendix B) before Step 2.

- [ ] **Step 2: The failing conventions check**

This task changes documentation only, so its failing test is a check of the conventions the runbook keeps; Steps 3, 4 and 6 run it again. It asserts contract §57's section list, exactly one bash block in §0 and that it is the runbook's first block, exactly one go block (the rehearsal's source, in §3.2), `Confirm before running.` in front of every block that writes and only there, `Stop here and obtain explicit confirmation.` opening §7.2 with contract §57's activation command verbatim in its block, no hard-wrapped prose, `bash -n` under bash and `/bin/bash` (3.2) on every bash block and a compile of every embedded Python program; it builds the §0 chain from the eleven earlier runbooks and refuses a helper of this runbook that redefines an earlier one, a `si_*` name the chain does not define, and any call, direct or through a helper, of an earlier helper that reads another runbook's pins, repos or previous-pins file; it requires the stage-2 runbook's pointer; with `--filled` it also refuses a PR-time placeholder. It writes every block, the chain and two lists into its output directory for `shellcheck` and the self-test. Write `$T/check_runbook.py` with the Write tool, with exactly this content:

````python
"""Conventions check of the stage-3 P5 runbook (Plan S3-E4 Task 81; local, never committed).

usage: python3 -I check_runbook.py [--filled] <out-dir> <docs-dir>

It reads <docs-dir>/storage-integrity-devnet2-stage3-p5-v21.md and asserts: the sections of Plan S3-E4
contract §57, in order, with unique anchors; exactly one bash block in §0 and that it is the runbook's first
block; exactly one go block, in §3.2; the line `Confirm before running.` in front of every block that writes
and only there; `Stop here and obtain explicit confirmation.` opening §7.2, whose block is the only one that
runs `arbiter-admin consensus update`, with contract §57's activation command verbatim; no hard-wrapped
prose; `bash -n` under bash and /bin/bash (3.2) on every bash block, `fish --no-execute` on every fish block
and a compile of every embedded Python program; with --filled, no PR-time placeholder left. It then builds
the §0 chain from the eleven earlier runbooks in <docs-dir> and this one's §0 and asserts that no helper of
this runbook redefines an earlier one, that every si_* name a block calls is defined by the chain, and that
no block or helper of this runbook calls an earlier helper that reads another runbook's pins, repos or
previous-pins file. It writes every block and the chain into <out-dir> for shellcheck and the self-test, and
the tracks runbook's P5 pointer must be in place.
"""
import os
import py_compile
import re
import shutil
import subprocess
import sys

NAME = "storage-integrity-devnet2-stage3-p5-v21.md"
SECTIONS = [
    "## 0. Session helpers",
    "## 1. Preparation", "### 1.1 Pins", "### 1.2 Identities and the stage-3 parameters", "### 1.3 Preconditions and baseline",
    "## 2. Offline replay check", "### 2.1 Rehearsal binaries", "### 2.2 Baseline", "### 2.3 Replay",
    "## 3. Rehearsal", "### 3.1 Copies", "### 3.2 Rehearsal pod", "### 3.3 Activation and restarts from v21", "### 3.4 Forced mismatch",
    "### 3.5 The previous release refuses v21", "### 3.6 Cleanup",
    "## 4. Verifiers",
    "## 5. Hosts", "### 5.1 indexer-b", "### 5.2 indexer-c", "### 5.3 indexer-a", "### 5.4 si-members",
    "## 6. Voters",
    "## 7. Activation", "### 7.1 Re-register the data plane", "### 7.2 Activate", "### 7.3 Verify",
    "## 8. Monitoring and snapshot v21",
    "## 9. End",
    "## 10. Rollback", "### 10.1 Before §7.2", "### 10.2 After §7.2",
    "## 11. Operating rules",
]
CHAIN = [
    "dynamic-table-set", "multi-source", "multi-source-tracks", "stage3-p0", "stage3-lockdown", "stage3-indexer-c",
    "stage3-p1-backups", "stage3-p2-members", "stage3-p3-replica-reads", "stage3-p4-hardening", "stage3-p4-owner-rebuild",
]
ACTIVATION = """si_arbiter "$leader" /usr/local/bin/arbiter-admin consensus update --address "$(si_voter "$leader")" \\
  --expected-epoch "$epoch" --authority 0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705 --max-writers 3 \\
  --stage3 "${domains[@]}" --stage3-max-lag-publications 64 --stage3-lag-low 16 \\
  --stage3-evidence-timeout-l2-blocks 300 --stage3-max-uncleaned-parts-per-partition 2000 \\
  --stage3-max-uncleaned-parts-per-table 20000 \\
  --voter "$(si_voter 0)" --voter "$(si_voter 1)" --voter "$(si_voter 2)" \\
  --hold drain --hold-timeout 10m --timeout 20s 2> "$si_backup/activate.err" | tee "$si_backup/activate.out\""""
PLACEHOLDERS = re.compile(r"<(ARBITER_TAG_P5|ARBITER_DIGEST_P5|SN_COMMIT_P5|SN_DIGEST_P5|ARBITER_TAG_P4|ARBITER_DIGEST_P4|SN_COMMIT_P4|SN_DIGEST_P4|CORE_TAG_P5|HG_TAG_P5|PROTO_TAG_P5|INDEXER_C_ID|ENROLMENT_EPOCH|ENROLMENT_EPOCH \+ 1|DEVNET2_FAILURE_DOMAINS|P5REHEARSAL_SHA256|P5REHEARSAL_SOURCE)>")
# A block writes when it pushes, syncs, applies, restarts or deletes a resource, writes into a pod, runs the
# rehearsal, writes data, submits a consensus update, or calls one of the mutating helpers.
WRITES = re.compile(
    r"git push|gh pr (create|merge)|helmfile [^\n]* sync"
    r"|\b(si_kubectl|kubectl)\b[^\n|;]*?\s(apply|delete|patch|label|scale|create|run|debug)\s"
    r"|rollout restart"
    r"|\bsi_(p5_put|p5_accept|replace_voter|revert_stage3|agent_start|accept_a_track)\b"
    r"|arbiter-admin consensus update|/rehearsal/p5rehearsal -test"
    r"|INSERT INTO|CREATE TABLE|DROP TABLE"
)
HELPERS = "## 0. Session helpers"
FUNC = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\(\)\s*\{", re.M)
CALL = re.compile(r"(?<![A-Za-z0-9_$./-])(si_[a-z0-9_]+)\b(?!:)")
FOREIGN_FILES = re.compile(r'\$si_backup/(pins|repos|previous-pins)\b|"\$si_backup/pins"')
# Variables of the chain that the blocks may name (si_backup and the lockdown directory), not functions.
VARIABLES = {"si_backup", "si_lockdown_dir", "si_ch_image", "si_agent_pid", "si_p2_logins_dir", "si_p2_root", "si_p2_unsafe_inc",
             "si_p2_staging_chi", "si_p2_staging_pod", "si_p2_staging_root", "si_p2_helmfile", "si_p1_logins_dir"}


def fail(msg):
    sys.exit(msg)


def blocks_of(lines):
    """(start index, language, body, previous non-empty line, section) of every fenced block."""
    out, i, section = [], 0, ""
    while i < len(lines):
        if re.match(r"#{2,3} ", lines[i]):
            section = lines[i]
        m = re.match(r"^```(\w+)\s*$", lines[i])
        if not m:
            i += 1
            continue
        j = i + 1
        while lines[j] != "```":
            j += 1
        prev = [x for x in lines[:i] if x.strip()]
        out.append((i, m.group(1), "\n".join(lines[i + 1:j]) + "\n", prev[-1].strip() if prev else "", section))
        i = j + 1
    return out


def first_helper_block(text):
    lines = text.split("\n")
    i = lines.index(HELPERS)
    j = i + 1
    while not lines[j].startswith("```"):
        j += 1
    assert lines[j] == "```bash", f"§0's first block is {lines[j]!r}"
    k = j + 1
    while lines[k] != "```":
        k += 1
    return "\n".join(lines[j + 1:k]) + "\n"


def function_bodies(text):
    """name -> body of every function the chain text defines (the last definition wins)."""
    bodies, names = {}, list(FUNC.finditer(text))
    for n, m in enumerate(names):
        end = names[n + 1].start() if n + 1 < len(names) else len(text)
        bodies[m.group(1)] = text[m.start():end]
    return bodies


def main(argv):
    filled = "--filled" in argv
    argv = [a for a in argv if a != "--filled"]
    out, docs = argv
    path = os.path.join(docs, NAME)
    text = open(path).read()
    lines = text.splitlines()
    got = [l for l in lines if re.match(r"#{2,3} ", l)]
    if got != SECTIONS:
        fail(f"{NAME}: sections differ: missing {[w for w in SECTIONS if w not in got]}, extra {[g for g in got if g not in SECTIONS]}, or out of order")
    anchors = [re.sub(r"[^a-z0-9 _-]", "", s.split(" ", 1)[1].lower()).replace(" ", "-") for s in got]
    if len(set(anchors)) != len(anchors):
        fail(f"{NAME}: duplicate anchors")
    if "11-operating-rules" not in anchors:
        fail(f"{NAME}: no anchor 11-operating-rules for the stage-3 rule group's runbook_url")
    blocks = blocks_of(lines)
    in_helpers = [b for b in blocks if b[4] == HELPERS]
    if len(in_helpers) != 1 or in_helpers[0][1] != "bash" or blocks[0] is not in_helpers[0]:
        fail(f"{NAME}: §0 must hold exactly one block, a bash block, and it must be the runbook's first block")
    gos = [b for b in blocks if b[1] == "go"]
    if len(gos) != 1 or gos[0][4] != "### 3.2 Rehearsal pod":
        fail(f"{NAME}: exactly one go block, the rehearsal's source in §3.2")
    langs = {b[1] for b in blocks} - {"bash", "go", "fish"}
    if langs:
        fail(f"{NAME}: only bash, fish and the one go block are allowed: {sorted(langs)}")
    missing, needless = [], []
    for pos, lang, body, prev, sec in blocks:
        if lang != "bash" or sec == HELPERS:
            continue
        hits = [l for l in body.splitlines() if WRITES.search(l) and "--dry-run=server" not in l and not l.lstrip().startswith("#")]
        ok = prev == "Confirm before running."
        if hits and not ok:
            missing.append(f"{pos + 1}: {hits[0].strip()[:90]}")
        if ok and not hits:
            needless.append(str(pos + 1))
    if missing:
        fail(f"{NAME}: writing blocks without 'Confirm before running.': " + " | ".join(missing))
    if needless:
        fail(f"{NAME}: 'Confirm before running.' before a block that writes nothing: lines " + ", ".join(needless))
    updates = [b for b in blocks if "arbiter-admin consensus update" in b[2]]
    if len(updates) != 1 or updates[0][4] != "### 7.2 Activate":
        fail(f"{NAME}: exactly one block, in §7.2, runs arbiter-admin consensus update")
    if ACTIVATION not in updates[0][2]:
        fail(f"{NAME}: §7.2's block does not carry contract §57's activation command verbatim")
    s72 = lines.index("### 7.2 Activate")
    first = [l for l in lines[s72 + 1:] if l.strip()][0]
    if not first.startswith("Stop here and obtain explicit confirmation."):
        fail(f"{NAME}: §7.2 must open with 'Stop here and obtain explicit confirmation.'")
    in_fence, wrapped = False, []
    prose = re.compile(r"^[A-Za-z`(\[*\"']")
    for n, l in enumerate(lines):
        if l.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence or n == 0:
            continue
        if prose.match(l) and prose.match(lines[n - 1]):
            wrapped.append(n + 1)
    if wrapped:
        fail(f"{NAME}: hard-wrapped prose at lines {wrapped[:10]}")
    if filled and PLACEHOLDERS.search(text):
        fail(f"{NAME}: PR-time placeholders left: {sorted(set(PLACEHOLDERS.findall(text)))}")

    shutil.rmtree(out, ignore_errors=True)
    os.makedirs(os.path.join(out, "py"))
    bad, snippets = [], 0
    for n, (pos, lang, body, prev, sec) in enumerate(blocks):
        kind = "helpers" if sec == HELPERS else ("c" if prev == "Confirm before running." else "r")
        f = os.path.join(out, f"p5-{n:03d}-{kind}.{lang}")
        open(f, "w").write(body)
        if lang == "bash":
            for shell in ("bash", "/bin/bash"):
                r = subprocess.run([shell, "-n", f], capture_output=True, text=True)
                if r.returncode:
                    bad.append((f, shell, r.stderr.strip()[:300]))
            for k, py in enumerate(re.findall(r"python3 -I(?: -B)? - [^\n]*<<'(EOF|PY)'[^\n]*\n(.*?)\n\1\n", body, re.S)):
                snippets += 1
                p = os.path.join(out, "py", f"p5-{n:03d}-{k}.py")
                open(p, "w").write(py[1] + "\n")
                py_compile.compile(p, doraise=True)
        elif lang == "fish":
            r = subprocess.run(["fish", "--no-execute", f], capture_output=True, text=True)
            if r.returncode:
                bad.append((f, "fish", r.stderr.strip()[:300]))
    if bad:
        fail(f"{NAME}: syntax: {bad}")

    earlier = []
    for stem in CHAIN:
        earlier.append(first_helper_block(open(os.path.join(docs, f"storage-integrity-devnet2-{stem}.md")).read()))
    own = first_helper_block(text)
    chain = "".join(earlier) + own
    open(os.path.join(out, "helpers.sh"), "w").write(chain)
    earlier_names = set(FUNC.findall("".join(earlier)))
    own_names = FUNC.findall(own)
    redefined = sorted(set(own_names) & earlier_names)
    if redefined:
        fail(f"{NAME}: §0 redefines earlier helpers {redefined}")
    bodies = function_bodies(chain)
    foreign = {n for n, b in bodies.items() if FOREIGN_FILES.search(b) and n not in own_names}
    changed = True
    while changed:
        changed = False
        for n, b in bodies.items():
            if n in foreign or n in own_names:
                continue
            if any(c in foreign for c in CALL.findall(b) if c != n):
                foreign.add(n)
                changed = True
    for n in own_names:
        calls = sorted({c for c in CALL.findall(bodies[n]) if c in foreign})
        if calls:
            fail(f"{NAME}: helper {n} calls {calls}, which read another runbook's pins")
    used = set()
    for pos, lang, body, prev, sec in blocks:
        if lang != "bash" or sec == HELPERS:
            continue
        code = "\n".join(l for l in body.splitlines() if not l.lstrip().startswith("#"))
        for c in CALL.findall(code):
            used.add(c)
    undefined = sorted(c for c in used if c not in bodies and c not in VARIABLES)
    if undefined:
        fail(f"{NAME}: blocks call names the chain does not define: {undefined}")
    calls_foreign = sorted(used & foreign)
    if calls_foreign:
        fail(f"{NAME}: blocks call {calls_foreign}, which read another runbook's pins")
    reach, todo = set(), [c for c in used if c in bodies]
    for n in own_names:
        todo.extend(c for c in CALL.findall(bodies[n]) if c in bodies)
    while todo:
        n = todo.pop()
        if n in reach:
            continue
        reach.add(n)
        todo.extend(c for c in CALL.findall(bodies[n]) if c in bodies and c != n)
    called_earlier = sorted(n for n in reach if n in earlier_names)
    open(os.path.join(out, "earlier-helpers-called.txt"), "w").write("\n".join(called_earlier) + "\n")
    open(os.path.join(out, "foreign-helpers.txt"), "w").write("\n".join(sorted(foreign)) + "\n")

    tracks = open(os.path.join(docs, "storage-integrity-devnet2-multi-source-tracks.md")).read()
    if "[storage-integrity-devnet2-stage3-p5-v21.md](storage-integrity-devnet2-stage3-p5-v21.md)" not in tracks:
        fail("the tracks runbook's §11 does not point to the P5 runbook")

    bash = [b for b in blocks if b[1] == "bash"]
    confirmed = sum(1 for b in bash if b[3] == "Confirm before running.")
    print(f"{NAME}: {len(got)} headings as contract §57 lists them; one helper block, §0's first; one go block in §3.2; {len(bash)} bash blocks ({confirmed} confirmed, every write confirmed), {sum(1 for b in blocks if b[1] == 'fish')} fish blocks; §7.2 stops first and carries the activation command verbatim; no hard-wrapped prose; bash -n (bash and /bin/bash) and fish --no-execute pass; {snippets} python programs compile")
    print(f"chain: {len(bodies)} functions; {len(own_names)} of this runbook's, none redefining an earlier one; every si_* name the blocks call is defined; {len(called_earlier)} earlier helpers reached, none of the {len(foreign)} that read another runbook's pins; the tracks runbook points here")
    print(f"blocks in {out}")


main(sys.argv[1:])
````

Run it (local only, with Step 1's prefix):

```bash
cd "$W" && python3 -I "$T/check_runbook.py" "$T/blocks" docs 2>&1 | tail -1
```

Expected: `FileNotFoundError: [Errno 2] No such file or directory: 'docs/storage-integrity-devnet2-stage3-p5-v21.md'`.

- [ ] **Step 3: Write the runbook**

Create `docs/storage-integrity-devnet2-stage3-p5-v21.md` with exactly the content below. The PR-time values (`<ARBITER_TAG_P5>`, `<ARBITER_DIGEST_P5>`, `<SN_COMMIT_P5>`, `<SN_DIGEST_P5>`, `<ARBITER_TAG_P4>`, `<ARBITER_DIGEST_P4>`, `<SN_COMMIT_P4>`, `<SN_DIGEST_P4>`, `<CORE_TAG_P5>`, `<HG_TAG_P5>`, `<PROTO_TAG_P5>`, `<INDEXER_C_ID>`, `<ENROLMENT_EPOCH>`, `<ENROLMENT_EPOCH + 1>`, `<DEVNET2_FAILURE_DOMAINS>`, `<P5REHEARSAL_SHA256>` and the go block's single line `<P5REHEARSAL_SOURCE>`) stay in angle brackets until Step 5; every other `<…>` is the runbook's own notation, read or set at run time. Each paragraph stays on one line.

````markdown
# Storage integrity devnet2: stage 3 P5, the snapshot v21 activation

This runbook moves devnet2-si-v2 (the voters and verifiers of `storage-integrity-si-v2`, the hosts `sentio-node-devnet2-indexer-a`, `-b` and `-c` and their si-members, all in `sentio-network-devnet2`) onto the P5 builds of stage 3 and commits the irreversible v21 activation with every switch off: one authority-signed consensus update sets `stage3` (group size 3, network fill on, `min_safe_copies` 1, strict mode off, devnet2's failure domains, every one-way switch off), after which the voters write snapshot v21, every later update carries `stage3`, and no component may run an older image again. Nothing else changes while every switch is off: placement, member readiness, checkpoints, the copy floor and the narrowed drain gate are inert until Plan S3-F turns them on one at a time. Before any component rolls, the runbook replays copies of the voters' logs offline under both releases and rehearses the activation on copies of the live state in a throwaway Pod: three FSMs restored from the three voters' copies apply the same signed activation, persist v21, restart from it, persist again through a forced interner mismatch, and the previous release refuses the result at its version byte. Design: housegate `docs/superpowers/specs/2026-10-14-multi-source-si-stage3-database-groups-design.md`, §10.3 "Activation", §13.2 "Lossless encoding (v21)", §16 row P5 and §18, as clarified by Plan S3-E4 (housegate `docs/superpowers/plans/2026-10-15-multi-source-si-s3e4-p5-floor-release-activation.md`, clarifications K81 and K82 and decision D37). It continues [storage-integrity-devnet2-stage3-p4-owner-rebuild.md](storage-integrity-devnet2-stage3-p4-owner-rebuild.md) (done: `$HOME/si-devnet2-stage3-p4-rebuild/p4-end.txt`); the switches follow in Plan S3-F's runbook.

| Stage | Change | Way back |
| --- | --- | --- |
| 0 | PR (P5): chart values `arbiter.raft.snapshotBody` (unset) and `monitoring.stage3Consensus` (off) with the rule group `storage-integrity-si-v2.arbiter-stage3`, the rehearsal Pod example `docs/examples/storage-integrity-p5-rehearsal-pod.yaml`, the P5 image constants and their render tests, this runbook; inert on merge | Revert the PR |
| 1 | Offline replay of fresh copies of the voters' logs under `<ARBITER_TAG_P4>` and `<ARBITER_TAG_P5>` (§2) | None needed |
| 2 | Rehearsal on copies of the live state in the throwaway Pod `storage-integrity-si-v2-p5-rehearsal`, deleted at its end (§3) | Delete the Pod (§3.6) |
| 3 | Verifiers to `<ARBITER_TAG_P5>` (§4) | Revert (§10.1) |
| 4 | Hosts to sentio-node `<SN_COMMIT_P5>`: indexer-b, indexer-c, indexer-a, then their si-members in the same order, still in P2/P3 mode (§5) | Revert each (§10.1) |
| 5 | Voters to `<ARBITER_TAG_P5>`, followers first, leader last; they still write v20 (§6) | Replace them back (§10.1) |
| 6 | Re-registration of every data-plane node with the leader, then the v21 activation under a drain hold (§7) | None: irreversible (§10.2) |
| 7 | The stage-3 alert rules on; every voter writes v21 (§8) | Revert the monitoring commit; the plain body (§11) |
| 8 | End record (§9) | None needed |

Before §7.2 every component can return to its P4 image: the P5 builds write v20 while `stage3` is unset and propose nothing new (§2 proves it on devnet2's own log). §7.2 is the one irreversible step of this runbook. Its only lever afterwards is the operational plain-body writer setting of §11; it changes how a voter encodes its next snapshot, never the format.

Every command block is bash. Run them from the root of a production checkout at `origin/main`, in order, in one dedicated `bash` session started with `set -eo pipefail` (from fish, run `bash` first); a live agent runs each block from a block file after sourcing the §0 helpers along their chain. Every value a later block needs is written under `$si_backup`, so a block also runs in a new session once §0 is defined. Never run two stages in one window. From §3.1 until §7.3 has passed, freeze every other change to `storage-integrity-si-v2`, the three host releases and their si-members: no other sync or apply, no pod deletion, no SI write but this runbook's and no consensus update outside this runbook. From §7.1 until §7.3 has passed, also make no Raft membership change: no AddVoter or RemoveServer, and no voter added, removed or replaced (§7.2 says why). After §7.3, SI writes may resume while §8 waits for the voters' first v21 snapshots; every other freeze stays until §9, and no consensus update runs before it (Plan S3-F starts from §9).

Every block that pushes to production `main`, syncs a release, applies, restarts or deletes a resource, writes into a pod, runs the rehearsal, writes data or submits a consensus update needs explicit user confirmation in chat before it runs; each such block is preceded by the line `Confirm before running.`. The update of §7.2 is additionally irreversible and preceded by `Stop here and obtain explicit confirmation.` as well. Never re-run a confirmed block only to see its output: a re-run repeats its writes. The read-only checks sit in blocks of their own, are safe to re-run, and `tee` their output into `$si_backup`; read that file instead. `kubectl apply --dry-run=server` writes nothing and sits in read-only blocks.

The user's own actions are the writes of the `devuser1` owner: a confirmed block that sets `key_file` runs in the user's own terminal (the owner key never leaves it), in a bash session started with `set -eo pipefail` and §0 defined, from the root of the same checkout and with the same `$HOME`, so it writes into the same `$si_backup`; it replaces `/path/to/devuser1-owner.key` with the key file's path there, and the read-only blocks around it run in the operator's session. The authority key never leaves the voter pods and the rehearsal Pod, which both read it as `ARBITER_AUTHORITY_PRIVATE_KEY_HEX` from Secret `storage-integrity-devnet2-si-v2-authority` (key `private-key`); the activation is signed inside the leader's arbiter container and the rehearsal inside its Pod. The rehearsal's signed update and every output of it other than digests, version bytes and counts never leave the Pod: §3.3 copies out only its result lines and §3.5 only the first 14 bytes of one container (magic, version byte, body byte and document length). No password or key appears in chat, on argv or in a terminal; the member keys of Plan S3-F are not made here.

Writes in this runbook: the single-service commits of §4, §5 and §8 and their syncs (each SI host and si-member restarts once, every verifier once, every voter once in §6); the restarts of §7.1; the rehearsal Pod with its copies (§3.2, deleted in §3.6); the activation (§7.2); indexer-a's acceptance INSERTs into `devnet101.swap_new2` through indexer-a's sidecar, values `5403` (§5.3) and `5404` (§7.3); and the `devuser1` owner's writes through its own local agent: row `5401` of `devuser1.s3_p1` (indexer-b's table of the P1 runbook, kept since then; `p` 1) in §5.1 and row `5402` of indexer-c's acceptance table `<db>.s3e_e2e` (the indexer-c runbook's §8; `<db>` is the name in `$HOME/si-devnet2-stage3-indexer-c/acceptance-db`) in §5.2, each with `v` = `p5-<section>-<id>`. These values are disjoint from every earlier runbook's (the stage-3 P0 runbook's `3001` to `3004`, the indexer-c runbook's `3101`, `3102` and its `s3e_e2e` rows `1` to `3`, the lockdown runbook's `4101` to `4104`, the P1 runbook's `5101` to `5104`, the P2 runbook's `5201` to `5299`, the P3 runbook's `5301` and its `s3e_e2e` row `4`, the hardening runbook's `4201` to `4299` and the owner-rebuild runbook's `4001` to `4099`). The runbook makes 4 promotions and no table-set transition. Retryable `SOURCE_UNAVAILABLE` windows are expected for a host's own tables while that host restarts (§5, §7.1), and Pending (733, retryable) answers until its startup owner audit is clean (`owner_audit: repair`).

## 0. Session helpers

Define the helpers of §0 of [storage-integrity-devnet2-dynamic-table-set.md](storage-integrity-devnet2-dynamic-table-set.md), [storage-integrity-devnet2-multi-source.md](storage-integrity-devnet2-multi-source.md), [storage-integrity-devnet2-multi-source-tracks.md](storage-integrity-devnet2-multi-source-tracks.md), [storage-integrity-devnet2-stage3-p0.md](storage-integrity-devnet2-stage3-p0.md), [storage-integrity-devnet2-stage3-lockdown.md](storage-integrity-devnet2-stage3-lockdown.md), [storage-integrity-devnet2-stage3-indexer-c.md](storage-integrity-devnet2-stage3-indexer-c.md), [storage-integrity-devnet2-stage3-p1-backups.md](storage-integrity-devnet2-stage3-p1-backups.md), [storage-integrity-devnet2-stage3-p2-members.md](storage-integrity-devnet2-stage3-p2-members.md), [storage-integrity-devnet2-stage3-p3-replica-reads.md](storage-integrity-devnet2-stage3-p3-replica-reads.md), [storage-integrity-devnet2-stage3-p4-hardening.md](storage-integrity-devnet2-stage3-p4-hardening.md) and [storage-integrity-devnet2-stage3-p4-owner-rebuild.md](storage-integrity-devnet2-stage3-p4-owner-rebuild.md), unchanged and in that order (the first bash block of each §0), then these. None of these redefines an earlier helper. They replace `si_backup`, so the earlier evidence stays untouched (the lockdown runbook's lock records stay in `$si_lockdown_dir`, which `si_ch_admin` reads). This runbook's `$si_backup/pins` holds, one value per line, `ARBITER_TAG_P5`, `ARBITER_DIGEST_P5`, `SN_COMMIT_P5`, `SN_DIGEST_P5`, `ARBITER_TAG_P4`, `ARBITER_DIGEST_P4`, `SN_COMMIT_P4` and `SN_DIGEST_P4`; the earlier helpers that read a pins or repos file of their own runbook's shape (`si_stage1_image`, `si_stage2_image`, `si_stage3_image`, `si_previous_image`, `si_partstates_build`, `si_p1_pin`, `si_p1_image`, `si_p2_image`, `si_owner_id`, `si_quiet_point`, `si_pin`, `si_indexer_c_id`, `si_p3_pin`, `si_p3_image`, `si_p3_sidecar`, `si_c_id`, `si_local_agent_version`, `si_run_record`, `si_p4_image`, `si_p4_accept` and the helpers built on them) are never called here; this runbook reads indexer-c's id with `si_p5_owner c` and its other ledger values with `si_p5_value`. The earlier helpers this runbook calls are `si_kubectl`, `si_arbiter`, `si_voter`, `si_metrics`, `si_capability`, `si_leader`, `si_show`, `si_images` (dynamic-table-set); `si_node_image`, `si_replace_voter`, `si_status` (multi-source); `si_tracks`, `si_track_published`, `si_global`, `si_sealed`, `si_wait_track`, `si_accept_a_track`, `si_agent_start`, `si_agent_stop`, `si_agent_ch` (multi-source-tracks); `si_series_value`, `si_voter_series`, `si_state_size`, `si_vm_query`, `si_vm_value`, `si_vmalert_groups`, `si_head_is`, `si_revert_stage3` (stage-3 P0); `si_secret_keys` (lockdown); `si_rpc_on`, `si_info`, `si_audit` with `c` (indexer-c); `si_member_pod`, `si_member_metrics`, `si_copies_check`, `si_budget_capture`, `si_budget` (P2); `si_container_image`, `si_pod_state`, `si_replica_hosts`, `si_host_replica_metrics`, `si_render_diff_is` (P3).

`si_v21check <ordinal>` copies the voter's newest finished snapshot (a directory under `/data/raft/snapshots` whose name does not end in `.tmp`) to `$si_backup/state-<ordinal>.bin` and prints `arbiter-<ordinal> version <byte> body <byte>` (the body byte only for version 21, else `-`) and `$R5/bin/new/v21check`'s line, or `arbiter-<ordinal> no snapshot yet`. `si_p5_accept` writes one acceptance row and reads it back in safe mode, as the P4 runbooks' `si_p4_accept` does, into this runbook's tables. `si_p5_put` copies a local file into the rehearsal Pod through `kubectl exec` stdin and checks the copy's SHA-256. `si_p5_member_keys` counts the `consensus` keys of an si-member's config (0 in P2/P3 mode) and `si_p5_keeper_findings` prints a host's keeper-findings series; the stage-3 P0 runbook's `si_host_series` serves only indexer-a and indexer-b. `si_p5_py` holds this runbook's local checks: the stage-3 parameters, the rehearsal Pod document, the rehearsal's result lines, the container header, the feature book and the committed `stage3`.

```bash
si_backup="$HOME/si-devnet2-stage3-p5"
# shellcheck disable=SC2034 # read by the blocks of §2, §3, §8 and by si_v21check
R5="$HOME/si-devnet2-multisource-s3e-rehearsal"
si_p5_image() { # the P5 host and member image: ghcr.io/sentioxyz/sentio-node:sha-<line 3>@<line 4> of $si_backup/pins (§1.1)
  local c d
  c="$(sed -n 3p "$si_backup/pins" 2>/dev/null)"; d="$(sed -n 4p "$si_backup/pins" 2>/dev/null)"
  [ -n "$c" ] && [ -n "$d" ] || { echo "si_p5_image: $si_backup/pins holds no SN_COMMIT_P5 and SN_DIGEST_P5 (§1.1)" >&2; return 1; }
  printf 'ghcr.io/sentioxyz/sentio-node:sha-%s@%s\n' "$c" "$d"
}
si_p5_value() { # $1 a ledger name §1.1 read: its value from $si_backup/ledger-values.txt
  local v
  v="$(sed -n "s/^$1=//p" "$si_backup/ledger-values.txt" 2>/dev/null)"
  [ -n "$v" ] || { echo "si_p5_value: $1 is not in $si_backup/ledger-values.txt (§1.1)" >&2; return 1; }
  printf '%s\n' "$v"
}
si_p5_owner() { # $1 a|b|c: that owner's SI indexer id (indexer-c's from the ledger, §1.1)
  case "$1" in
    a) echo 0 ;;
    b) echo 1 ;;
    c) si_p5_value INDEXER_C_ID ;;
    *) echo "si_p5_owner: the owner is a, b or c, not '$1'" >&2; return 2 ;;
  esac
}
si_stage3() { si_show | jq -c '.current.stage3'; } # the leader's committed stage3 (consensus show, proto field names), one JSON line; null before the activation
si_features_stage3() { # the current leader's feature book: voter ids and, per registered data-plane node, whether it advertised stage3_v1
  local l
  l="$(si_leader)" || { echo "no leader answers" >&2; return 1; }
  si_arbiter "$l" /usr/local/bin/arbiter-admin consensus features --address "$(si_voter "$l")" |
    jq -c '{voters: .raft_voter_ids, nodes: [.entries[] | {id: .node_id, stage3: ((.features // []) | index("stage3_v1") != null)}]}'
}
# shellcheck disable=SC2016 # the in-pod script is single-quoted on purpose: it expands inside the voter's container
si_v21check() { # $1 voter ordinal: its newest finished snapshot copied to $si_backup/state-<ordinal>.bin; prints "arbiter-<ordinal> version <byte> body <byte|->" and v21check's line, or "arbiter-<ordinal> no snapshot yet"
  local d f v b
  case "$1" in 0|1|2) ;; *) echo "usage: si_v21check 0|1|2" >&2; return 2 ;; esac
  f="$si_backup/state-$1.bin"
  d="$(si_arbiter "$1" sh -c 'for d in $(ls -dt /data/raft/snapshots/*/ 2>/dev/null); do case "$d" in *.tmp/) ;; *) echo "$d"; exit 0 ;; esac; done')" || return 1
  [ -n "$d" ] || { echo "arbiter-$1 no snapshot yet"; return 0; }
  si_arbiter "$1" cat "${d}state.bin" > "$f" || return 1
  v="$(od -An -tu1 -j4 -N1 "$f" | tr -d ' ')"
  b=-
  [ "$v" != 21 ] || b="$(od -An -tu1 -j5 -N1 "$f" | tr -d ' ')"
  echo "arbiter-$1 version $v body $b"
  "$R5/bin/new/v21check" "$f"
}
si_encoding() { si_voter_series "$1" 'arbiter_snapshot_encoding_fallback_total|arbiter_snapshot_body_bytes'; } # $1 voter ordinal: its snapshot-encoding series
si_rehearsal() { si_kubectl exec storage-integrity-si-v2-p5-rehearsal -c rehearsal -- "$@"; } # a command in the rehearsal Pod
si_member_sts() { # $1 a|b|c: that host's si-member StatefulSet
  case "$1" in
    a|b|c) echo "sentio-node-devnet2-indexer-$1-si-member" ;;
    *) echo "si_member_sts: the host is a, b or c, not '$1'" >&2; return 2 ;;
  esac
}
si_p5_member_keys() { # $1 a|b|c: how many consensus keys that si-member's config holds (0 in P2/P3 mode); fails when its ConfigMap cannot be read
  local cfg
  cfg="$(si_kubectl get configmap "$(si_member_sts "$1")-config" -o jsonpath='{.data.si-member\.yaml}')" || return 1
  [ -n "$cfg" ] || { echo "the si-member config of host $1 is empty" >&2; return 1; }
  printf '%s\n' "$cfg" | grep -c '^ *consensus:' || true
}
si_p5_keeper_findings() { # $1 a|b|c: the host's sentio_node_storage_integrity_keeper_findings{reason} lines (:9091); none before the P5 image; fails only when the endpoint cannot be read
  local m
  case "$1" in a|b|c) ;; *) echo "usage: si_p5_keeper_findings a|b|c" >&2; return 2 ;; esac
  m="$(si_kubectl exec "sentio-node-devnet2-indexer-$1-0" -c sentio-node -- wget -qO- http://127.0.0.1:9091/metrics)" || { echo "cannot read indexer-$1's :9091 metrics" >&2; return 1; }
  printf '%s\n' "$m" | grep -E '^sentio_node_storage_integrity_keeper_findings\{' || true
}
si_p5_put() { # $1 local file, $2 absolute path in the rehearsal Pod: a write into the Pod, only in a confirmed block; fails unless the copy's SHA-256 equals the file's
  local sum
  sum="$(shasum -a 256 "$1" | cut -d' ' -f1)" || return 1
  # shellcheck disable=SC2016 # $1 expands inside the Pod's shell
  si_kubectl exec -i storage-integrity-si-v2-p5-rehearsal -c rehearsal -- sh -c 'cat > "$1.new" && mv "$1.new" "$1"' sh "$2" < "$1" || return 1
  [ "$(si_rehearsal sha256sum "$2" | cut -d' ' -f1)" = "$sum" ] || { echo "the Pod's $2 is not $sum" >&2; return 1; }
  echo "$2 $sum"
}
si_p5_accept() { # $1 key file of devuser1's owner (- for indexer-a), $2 a|b|c, $3 value (a) or row id (b, c), $4 section tag: a write, only in a confirmed block. indexer-a: si_accept_a_track; indexer-b: one row (id, 1, "p5-<tag>-<id>") into devuser1.s3_p1; indexer-c: one row (id, "p5-<tag>-<id>") into <db>.s3e_e2e; both through devuser1's own agent (in the user's terminal), then it waits for that owner's track and reads the row back in safe mode
  local db t row id base n
  [[ "$3" =~ ^[0-9]+$ ]] || { echo "usage: si_p5_accept <key-file|-> a|b|c <n> <tag>" >&2; return 2; }
  case "$2" in
    a) si_accept_a_track "$3"; return ;;
    b) db=devuser1; t=s3_p1; row="$3,1,p5-$4-$3" ;;
    c) db="$(cat "$HOME/si-devnet2-stage3-indexer-c/acceptance-db")" || return 1; t=s3e_e2e; row="$3,p5-$4-$3" ;;
    *) echo "usage: si_p5_accept <key-file|-> a|b|c <n> <tag>" >&2; return 2 ;;
  esac
  id="$(si_p5_owner "$2")" || return 1
  base="$(si_track_published "$id")"
  [ -n "$base" ] || { echo "indexer $id has no track" >&2; return 1; }
  si_agent_start "$1" || return 1
  if ! printf '%s\n' "$row" | si_agent_ch --database "$db" --query "INSERT INTO $t FORMAT CSV"; then si_agent_stop; return 1; fi
  si_wait_track "$id" "$base" || { si_agent_stop; return 1; }
  n="$(si_agent_ch --database "$db" --query "SELECT count() FROM $t WHERE id = $3 SETTINGS SQL_x_read_mode='safe'")"
  si_agent_stop
  echo "indexer-$2 row $3 in $db.$t: safe count $n"
  [ "$n" = 1 ]
}
si_p5_py() { # the parameters, Pod, rehearsal, header, feature-book and stage3 checks of this runbook (local only)
  python3 -I -B - "$@" <<'PY'
import json
import re
import sys

VOTERS = ["storage-integrity-si-v2-arbiter-0", "storage-integrity-si-v2-arbiter-1", "storage-integrity-si-v2-arbiter-2"]
MEMBERS = {"member-a", "member-b", "member-c"}
AUTHORITY = "0x9ef3a259d1d87c864431cab5ed5f6578ad5ad705"
SWITCHES = ["purge", "eviction_voiding", "owner_aware_selection", "placement", "reselection", "checkpoints", "narrowed_drain"]
FIXED = {"group_size": 3, "network_fill_default": True, "min_safe_copies": 1, "strict_copies": False,
         "max_lag_publications": 64, "lag_low": 16, "evidence_timeout_l2_blocks": 300,
         "max_uncleaned_parts_per_partition": 2000, "max_uncleaned_parts_per_table": 20000,
         "exclude_quarantined_hosts": False}
ORDER = ["group_size", "network_fill_default", "min_safe_copies", "strict_copies", "indexer_failure_domains",
         "max_lag_publications", "lag_low", "evidence_timeout_l2_blocks", "max_uncleaned_parts_per_partition",
         "max_uncleaned_parts_per_table", "exclude_quarantined_hosts"] + SWITCHES
NODE = re.compile(r"node:[a-z0-9]([-a-z0-9.]*[a-z0-9])?")


def die(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def load(path):
    with open(path) as f:
        return json.load(f)


def cmd_params(domains, c_id, out):
    """stage3-params.json: the activation's arbiter.Stage3Params (Go JSON, proto field names, numbers as numbers)."""
    c = int(c_id)
    entries = []
    for item in domains.split(","):
        key, sep, dom = item.partition("=")
        if not sep or not key.isdigit() or not NODE.fullmatch(dom):
            die(f"DEVNET2_FAILURE_DOMAINS entry {item!r} is not <indexer id>=node:<hostname>")
        entries.append({"indexer_id": int(key), "failure_domain": dom})
    ids = [e["indexer_id"] for e in entries]
    if ids != sorted(set(ids)) or set(ids) != {0, 1, c}:
        die(f"DEVNET2_FAILURE_DOMAINS names indexers {ids}, not 0, 1 and {c} in ascending order")
    if len({e["failure_domain"] for e in entries}) != len(entries):
        die("two indexers share a failure domain in DEVNET2_FAILURE_DOMAINS: the P4 node pins put every source on its own node; stop")
    params = dict(FIXED)
    params["indexer_failure_domains"] = entries
    for s in SWITCHES:
        params[s] = False
    params = {k: params[k] for k in ORDER}
    with open(out, "w") as f:
        f.write(json.dumps(params, indent=2) + "\n")
    for e in entries:
        print(f"indexer {e['indexer_id']} failure domain {e['failure_domain']}")
    print(f"{out}: group_size 3, network_fill_default true, min_safe_copies 1, strict_copies false, max_lag_publications 64, lag_low 16, evidence_timeout_l2_blocks 300, max_uncleaned_parts_per_partition 2000, max_uncleaned_parts_per_table 20000, exclude_quarantined_hosts false, every switch off")


def num(value):
    return int(value) if isinstance(value, str) else value


def normalized(stage3):
    """consensus show's current.stage3 (protojson: uint64 as strings, absent fields unset) as stage3-params.json writes it."""
    out = {}
    for k in ORDER:
        v = stage3.get(k)
        if k == "indexer_failure_domains":
            out[k] = [{"indexer_id": int(e["indexer_id"]), "failure_domain": e["failure_domain"]} for e in (v or [])]
        elif isinstance(FIXED.get(k, False), bool):
            out[k] = bool(v)
        else:
            out[k] = num(v or 0)
    return out


def cmd_stage3(show_path, params_path, activate_path, tip_path):
    """The committed stage3 equals the activation's parameters; stage3_state names the activation."""
    show, params, activate = load(show_path), load(params_path), load(activate_path)
    if activate.get("status") != "committed":
        die(f"{activate_path} does not hold status committed")
    stage3 = (show.get("current") or {}).get("stage3")
    if not stage3:
        die("consensus show has no current.stage3: the activation did not commit")
    got = normalized(stage3)
    if got != params:
        diffs = [k for k in ORDER if got.get(k) != params.get(k)]
        die(f"current.stage3 differs from {params_path} in {diffs}: {json.dumps(got)}")
    st = show.get("stage3_state") or {}
    epoch = int(show["epoch"])
    if int(activate["committed_epoch"]) != epoch or int(st.get("activation_epoch", 0)) != epoch:
        die(f"epoch {epoch}, committed_epoch {activate['committed_epoch']}, stage3_state.activation_epoch {st.get('activation_epoch')}: they differ")
    if st.get("switches"):
        die(f"stage3_state records switch cutovers {st['switches']}: the activation turns none on")
    m = re.fullmatch(r"sealed (\d+) promotion_seq (\d+)\n?", open(tip_path).read())
    if not m:
        die(f"{tip_path} is not 'sealed <n> promotion_seq <n>'")
    block, seq = int(st.get("activation_block_seq", 0)), int(st.get("activation_promotion_seq", 0))
    print(f"stage3 committed at epoch {epoch} as {params_path} states it, every switch off; activation block {block}, promotion {seq} (tip before signing: sealed {m.group(1)}, promotion {m.group(2)})")


def cmd_features(path, c_id):
    """The leader's feature book: exactly the three voters, and exactly the six data-plane nodes, each advertising stage3_v1."""
    book = json.loads(open(path).read().strip().splitlines()[-1])
    if sorted(book.get("voters") or []) != VOTERS:
        die(f"the feature book's voters are {book.get('voters')}, not {VOTERS}")
    nodes = {n["id"]: n["stage3"] for n in book.get("nodes") or []}
    want = {"snode-1", "snode-2", "snode-3", "verifier-1", "verifier-2", "verifier-3"}
    if MEMBERS & set(nodes):
        die(f"member ids {sorted(MEMBERS & set(nodes))} are registered: no member registers before Plan S3-F (K82); stop")
    if set(nodes) != want:
        die(f"the feature book lists {sorted(nodes)}, not {sorted(want)}: restart the missing nodes (§7.1) or explain the extra ones")
    lacking = sorted(n for n, ok in nodes.items() if not ok)
    if lacking:
        die(f"{lacking} have not advertised stage3_v1 to this leader: restart them on the P5 release (§7.1)")
    print(f"feature book: voters {', '.join(VOTERS)}; snode-1, snode-2, snode-3 (indexers 0, 1, {c_id}) and verifier-1..3, each advertising stage3_v1; no member id")


def cmd_activation(show_path, tracks_path, epoch):
    """show-before-activation.json and the track state allow exactly the update §7.2 signs."""
    show, tracks = load(show_path), load(tracks_path)
    cur = show.get("current") or {}
    if [a.lower() for a in cur.get("authority_addresses") or []] != [AUTHORITY]:
        die(f"the committed authority set is {cur.get('authority_addresses')}, not [{AUTHORITY}]: the update would change it; stop")
    if str(cur.get("max_writers")) != "3":
        die(f"max_writers is {cur.get('max_writers')!r}, not \"3\": the update would change it; stop")
    if cur.get("stage3") is not None:
        die("stage3 is committed already; stop")
    if cur.get("tracks_enabled") is not True:
        die("tracks are not enabled; stop")
    if str(show.get("epoch")) != str(epoch):
        die(f"epoch {show.get('epoch')} is not ENROLMENT_EPOCH {epoch}: a consensus update committed since the enrolment; stop and ask")
    quarantined = [t["indexer_id"] for t in tracks.get("indexers") or [] if t.get("quarantined")]
    if quarantined:
        die(f"tracks {quarantined} are quarantined: this runbook activates with every track unquarantined; stop")
    print(f"epoch {show['epoch']}, promotion_seq {show['promotion_seq']}, authority {AUTHORITY}, max_writers \"3\", stage3 null, tracks enabled, no track quarantined")


def rehearsal_lines(path):
    return [l.rstrip("\n") for l in open(path) if l.strip()]


def cmd_rehearsal(path, epoch, limit, part):
    """The rehearsal's result lines (Plan S3-E4 contract §57)."""
    e, limit = int(epoch), int(limit)
    lines = rehearsal_lines(path)
    if any("eyJ" in l for l in lines):
        die(f"{path} holds a line that looks like a JWS: nothing but digests, version bytes and counts may leave the Pod; delete the file and stop")
    if part in ("activation", "all"):
        restored = [re.fullmatch(r'voter ([0-2]) restored epoch=(\d+) promotion_seq=(\d+) drain_refusal=(.*)', l) for l in lines]
        restored = [m for m in restored if m]
        if sorted(m.group(1) for m in restored) != ["0", "1", "2"]:
            die("the rehearsal did not restore three voters")
        for m in restored:
            if int(m.group(2)) != e:
                die(f"voter {m.group(1)} restored epoch {m.group(2)}, not ENROLMENT_EPOCH {e}")
            if m.group(4) != '""':
                die(f"voter {m.group(1)}'s copy does not pass the stage-2 drain gate ({m.group(4)}): wait for its work to finish and take the copies again (§3.1)")
        if len({m.group(3) for m in restored}) != 1:
            die("the three copies restored different promotion sequences")
        digests = [m for m in (re.fullmatch(r"digest ([0-2]) ([0-9a-f]{64})", l) for l in lines) if m]
        if sorted(m.group(1) for m in digests) != ["0", "1", "2"] or len({m.group(2) for m in digests}) != 1:
            die("the three restored voters do not have one digest")
        if f"activation applied epoch={e + 1}" not in lines:
            die(f"no 'activation applied epoch={e + 1}' line")
        persists = [m for m in (re.fullmatch(r"persist ([0-2]) version 21 body interned bytes=(\d+) fallback=0 persist_peak_heap_bytes=(\d+)", l) for l in lines) if m]
        if sorted(m.group(1) for m in persists) != ["0", "1", "2"]:
            die("not every voter persisted an interned v21 container without a fallback")
        peak = max(int(m.group(3)) for m in persists)
        if peak > limit:
            die(f"the rehearsal's Persist peak {peak} is above L = {limit}: the voters' sizing does not carry the activation (K82 (vi)); stop")
        restarts = [l for l in lines if re.fullmatch(r"restart [0-2] restore_ok version 21 body interned digest_equal=true", l)]
        if len(restarts) != 3:
            die("not every voter restarted from its interned v21 container with an equal digest")
        print(f"rehearsal activation ok: epoch {e} -> {e + 1}, promotion_seq {restored[0].group(3)}, digest {digests[0].group(2)}, interned v21 persisted and restored by voters 0, 1, 2, Persist peak {peak} bytes (L {limit})")
    if part in ("mismatch", "all"):
        mismatches = [l for l in lines if re.fullmatch(r"mismatch [0-2] version 21 body plain fallback=1 digest_equal=true", l)]
        if len(mismatches) != 3:
            die("the forced mismatch did not persist the plain body with one fallback on every voter")
        if "rehearsal ok" not in lines:
            die("the rehearsal did not print 'rehearsal ok'")
        dates = [l for l in lines if re.fullmatch(r"P5_REHEARSAL=20\d\d-[01]\d-[0-3]\d", l)]
        if len(dates) != 1:
            die("rehearsal.txt holds no P5_REHEARSAL date")
        print(f"rehearsal forced mismatch ok: voters 0, 1, 2 persisted the plain body with fallback=1 and an equal digest; rehearsal ok; {dates[0]}")


def cmd_header(path):
    """The first 14 bytes of a v21 container: magic AFSM, version 21, body kind 1 (interned), document length."""
    data = open(path, "rb").read()
    if len(data) != 14 or data[:4] != b"AFSM":
        die(f"{path} is not the 14-byte header of an arbiter snapshot container")
    if data[4] != 21 or data[5] != 1:
        die(f"{path}: version {data[4]} body {data[5]}, not version 21 body 1")
    print(f"header AFSM version 21 body 1 doc_length {int.from_bytes(data[6:14], 'big')}")


def pod_container(doc, digest, live):
    spec = doc.get("spec") or {}
    meta = doc.get("metadata") or {}
    problems = []
    if doc.get("kind") != "Pod" or meta.get("name") != "storage-integrity-si-v2-p5-rehearsal" or meta.get("namespace") != "sentio-network-devnet2":
        problems.append("not Pod sentio-network-devnet2/storage-integrity-si-v2-p5-rehearsal")
    labels = dict(meta.get("labels") or {})
    if labels != {"storage-integrity.sentio.xyz/rehearsal": "p5"}:
        problems.append(f"labels {labels}")
    if spec.get("restartPolicy") != "Never" or spec.get("automountServiceAccountToken") is not False:
        problems.append("restartPolicy Never and automountServiceAccountToken false are required")
    containers = spec.get("containers") or []
    if len(containers) != 1 or spec.get("initContainers") or spec.get("ephemeralContainers"):
        problems.append("exactly one container is allowed")
    c = containers[0] if containers else {}
    if c.get("name") != "rehearsal" or c.get("image") != f"ghcr.io/sentioxyz/arbiter@{digest}":
        problems.append(f"container {c.get('name')} image {c.get('image')}, not rehearsal ghcr.io/sentioxyz/arbiter@{digest}")
    if c.get("command") != ["sleep", "86400"] or c.get("args"):
        problems.append(f"command {c.get('command')} args {c.get('args')}")
    env = c.get("env") or []
    ref = {"secretKeyRef": {"name": "storage-integrity-devnet2-si-v2-authority", "key": "private-key"}}
    if len(env) != 1 or env[0].get("name") != "ARBITER_AUTHORITY_PRIVATE_KEY_HEX" or "value" in env[0] or env[0].get("valueFrom") != ref or c.get("envFrom"):
        problems.append("the only environment variable must be ARBITER_AUTHORITY_PRIVATE_KEY_HEX from the authority Secret's private-key")
    if c.get("ports"):
        problems.append("no port is allowed")
    mounts = [(m.get("name"), m.get("mountPath")) for m in c.get("volumeMounts") or []]
    if mounts != [("rehearsal", "/rehearsal")]:
        problems.append(f"volume mounts {mounts}")
    volumes = spec.get("volumes") or []
    if len(volumes) != 1 or volumes[0].get("name") != "rehearsal" or str((volumes[0].get("emptyDir") or {}).get("sizeLimit")) != "8Gi":
        problems.append(f"volumes {volumes}: only the emptyDir rehearsal (sizeLimit 8Gi)")
    limits = (c.get("resources") or {}).get("limits") or {}
    if str(limits.get("cpu")) != "2" or str(limits.get("memory")) != "2Gi":
        problems.append(f"limits {limits}, not cpu 2 and memory 2Gi")
    if live and (doc.get("status") or {}).get("phase") != "Running":
        problems.append(f"phase {(doc.get('status') or {}).get('phase')}, not Running")
    return problems


def cmd_pod(path, digest):
    """The rehearsal Pod example (Plan S3-E4 contract §56): one Pod, the authority Secret's key as its only Secret."""
    import yaml
    docs = [d for d in yaml.safe_load_all(open(path)) if d]
    if len(docs) != 1:
        die(f"{path} holds {len(docs)} documents, not one Pod")
    problems = pod_container(docs[0], digest, False)
    if problems:
        die(f"{path}: " + "; ".join(problems))
    print(f"{path}: Pod storage-integrity-si-v2-p5-rehearsal, image ghcr.io/sentioxyz/arbiter@{digest}, the authority Secret's private-key as its only Secret, one emptyDir")


def cmd_pod_live(path, digest):
    problems = pod_container(load(path), digest, True)
    if problems:
        die("the live rehearsal Pod: " + "; ".join(problems))
    print("the live rehearsal Pod runs as the example states it")


COMMANDS = {"params": cmd_params, "stage3": cmd_stage3, "features": cmd_features, "activation": cmd_activation,
            "rehearsal": cmd_rehearsal, "header": cmd_header, "pod": cmd_pod, "pod-live": cmd_pod_live}
if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
    die("usage: si_p5_py " + "|".join(COMMANDS) + " ...")
COMMANDS[sys.argv[1]](*sys.argv[2:])
PY
}
```

## 1. Preparation

### 1.1 Pins

| Component | Pin | Evidence |
| --- | --- | --- |
| Arbiter voters and verifiers | `ghcr.io/sentioxyz/arbiter:<ARBITER_TAG_P5>@<ARBITER_DIGEST_P5>` (arbiter-core `<CORE_TAG_P5>`, arbiter-proto `<PROTO_TAG_P5>`, housegate `<HG_TAG_P5>`): the stage-3 branch of Plans S3-E2 to S3-E4 (snapshot v21 with the interned body, groups, owner-aware selection, re-selection, checkpoints, the copy floor, the narrowed drain gate), every behaviour behind `stage3` and its switches; it writes v20 until `stage3` commits | arbiter release workflow; OCI revision equals the tag's commit; Plan S3-E4 Task 78's offline replay; `DEVNET2_STAGE3_P5_ARBITER_DIGEST` in `test_render.py` |
| sentio-node, every host and si-member | `ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT_P5>@<SN_DIGEST_P5>` (housegate `<HG_TAG_P5>`, arbiter-core `<CORE_TAG_P5>`, arbiter-proto `<PROTO_TAG_P5>`): every SNode and si-member advertises `stage3_v1`, the embedded ingress answers admission codes 12 and 13 retryably, `storage-integrity member-consent-statement`, the member consensus keys (off), the reconciler role, the keeper-findings export, `replica_hosts` from the registry's groups once placed | main CI `docker-push`; `DEVNET2_STAGE3_P5_SOURCE_IMAGE` in `test_render.py`, listed in `WRITER_AUTHORIZATION_IMAGES`, `STAGE3_HOST_IMAGES`, `STAGE3_P5_HOST_IMAGES` and `MEMBER_IMAGES` |
| Rehearsal tools | `$R5` = `$HOME/si-devnet2-multisource-s3e-rehearsal`: `bin/old/{statedigest,v20check,snapdump}` from `<ARBITER_TAG_P4>`, `bin/new/{statedigest,v21check,snapdump,p5rehearsal}` from `<ARBITER_TAG_P5>`, `p5-files.sha256` | Plan S3-E4 Task 78 (decision D37); this runbook only reads it |

The previous pins, the way back of §10.1: arbiter `<ARBITER_TAG_P4>` (`<ARBITER_DIGEST_P4>`) and sentio-node `sha-<SN_COMMIT_P4>@<SN_DIGEST_P4>` on every host; each si-member's previous image is whatever `member-images-before.txt` (§1.3) records, the image its own commit of §5.4 names as its rollback target.

The values also live in the stage-3 plans' release ledger (Appendix C of each plan file on housegate), which the next block reads with the ledger tool `$HOME/.cache/s3-ledger/s3-ledger.py` (Plan S3-C1 contract §19) and compares with the pins above; it also needs `P4_END` and `P5_PR` recorded and nothing of this runbook recorded yet. Set `housegate_repo` to a local housegate clone (the block only fetches from it and reads), then:

```bash
ARBITER_TAG_P5='<ARBITER_TAG_P5>'; ARBITER_DIGEST_P5='<ARBITER_DIGEST_P5>'; SN_COMMIT_P5='<SN_COMMIT_P5>'; SN_DIGEST_P5='<SN_DIGEST_P5>'
ARBITER_TAG_P4='<ARBITER_TAG_P4>'; ARBITER_DIGEST_P4='<ARBITER_DIGEST_P4>'; SN_COMMIT_P4='<SN_COMMIT_P4>'; SN_DIGEST_P4='<SN_DIGEST_P4>'
CORE_TAG_P5='<CORE_TAG_P5>'; HG_TAG_P5='<HG_TAG_P5>'; PROTO_TAG_P5='<PROTO_TAG_P5>'
INDEXER_C_ID='<INDEXER_C_ID>'; ENROLMENT_EPOCH='<ENROLMENT_EPOCH>'; DEVNET2_FAILURE_DOMAINS='<DEVNET2_FAILURE_DOMAINS>'
housegate_repo=/path/to/housegate
case "$ARBITER_TAG_P5$ARBITER_DIGEST_P5$SN_COMMIT_P5$SN_DIGEST_P5$ARBITER_TAG_P4$ARBITER_DIGEST_P4$SN_COMMIT_P4$SN_DIGEST_P4$CORE_TAG_P5$HG_TAG_P5$PROTO_TAG_P5$INDEXER_C_ID$ENROLMENT_EPOCH$DEVNET2_FAILURE_DOMAINS" in *'<'*) echo "pins are still placeholders; stop"; exit 1 ;; esac
git -C "$housegate_repo" rev-parse --git-dir > /dev/null 2>&1 || { echo "set housegate_repo to a local housegate clone first"; exit 1; }
[ ! -e "$si_backup" ] || { echo "$si_backup exists: this runbook starts a fresh evidence directory; move the old one aside"; exit 1; }
ledger="$HOME/.cache/s3-ledger/s3-ledger.py"
[ -r "$ledger" ] || { echo "the ledger tool $ledger is missing: write it from Plan S3-C1 contract §19 first"; exit 1; }
git -C "$housegate_repo" fetch -q origin
values="$(python3 -I "$ledger" get "$housegate_repo" ARBITER_TAG_P5 ARBITER_DIGEST_P5 SN_COMMIT_P5 SN_DIGEST_P5 ARBITER_TAG_P4 ARBITER_DIGEST_P4 SN_COMMIT_P4 SN_DIGEST_P4 CORE_TAG_P5 HG_TAG_P5 PROTO_TAG_P5 INDEXER_C_ID ENROLMENT_EPOCH DEVNET2_FAILURE_DOMAINS MEMBER_IDS P4_END P5_PR)" || { echo "the ledger refused (above): stop and ask, never type a value in"; exit 1; }
python3 -I "$ledger" empty "$housegate_repo" P5_REHEARSAL ACTIVATION_EPOCH P5_END
for v in ARBITER_TAG_P5 ARBITER_DIGEST_P5 SN_COMMIT_P5 SN_DIGEST_P5 ARBITER_TAG_P4 ARBITER_DIGEST_P4 SN_COMMIT_P4 SN_DIGEST_P4 CORE_TAG_P5 HG_TAG_P5 PROTO_TAG_P5 INDEXER_C_ID ENROLMENT_EPOCH DEVNET2_FAILURE_DOMAINS; do
  printf '%s\n' "$values" | grep -qxF "$v=${!v}" || { echo "the ledger's $v is not this runbook's pin ${!v}: stop"; exit 1; }
done
printf '%s\n' "$values" | grep -qxF "MEMBER_IDS=member-a:0,member-b:1,member-c:$INDEXER_C_ID" || { echo "MEMBER_IDS is not member-a:0,member-b:1,member-c:$INDEXER_C_ID: stop"; exit 1; }
for p in "arbiter:$ARBITER_TAG_P5 $ARBITER_DIGEST_P5" "arbiter:$ARBITER_TAG_P4 $ARBITER_DIGEST_P4" "sentio-node:sha-$SN_COMMIT_P5 $SN_DIGEST_P5" "sentio-node:sha-$SN_COMMIT_P4 $SN_DIGEST_P4"; do
  read -r ref digest <<< "$p"
  [ "$(docker buildx imagetools inspect "ghcr.io/sentioxyz/$ref" | sed -n 's/^Digest: *//p')" = "$digest" ] || { echo "ghcr.io/sentioxyz/$ref does not resolve to $digest; stop"; exit 1; }
done
python3 -I -B - "$ARBITER_DIGEST_P5" "$SN_COMMIT_P5" "$SN_DIGEST_P5" "$ARBITER_DIGEST_P4" "$SN_COMMIT_P4" "$SN_DIGEST_P4" <<'EOF'
import sys
sys.path.insert(0, "charts/storage-integrity/tests")
import test_render as t
arbiter_p5, p5 = sys.argv[1], "ghcr.io/sentioxyz/sentio-node:sha-%s@%s" % (sys.argv[2], sys.argv[3])
arbiter_p4, p4 = sys.argv[4], "ghcr.io/sentioxyz/sentio-node:sha-%s@%s" % (sys.argv[5], sys.argv[6])
assert t.DEVNET2_STAGE3_P5_ARBITER_DIGEST == arbiter_p5, t.DEVNET2_STAGE3_P5_ARBITER_DIGEST
assert t.DEVNET2_STAGE3_P5_SOURCE_IMAGE == p5, t.DEVNET2_STAGE3_P5_SOURCE_IMAGE
assert t.STAGE3_P5_HOST_IMAGES == frozenset({p5}), t.STAGE3_P5_HOST_IMAGES
assert p5 in t.WRITER_AUTHORIZATION_IMAGES and p5 in t.STAGE3_HOST_IMAGES and p5 in t.MEMBER_IMAGES, "the P5 image is not reviewed for hosts and members"
assert t.DEVNET2_ARBITER_DIGEST == arbiter_p4, "the live arbiter pin is not ARBITER_DIGEST_P4: %s" % t.DEVNET2_ARBITER_DIGEST
assert t.INDEXER_B_SOURCE_IMAGE == p4 and t.INDEXER_C_SOURCE_IMAGE == p4 and t.FRESH_SOURCE_IMAGE == p4, "a host pin is not the P4 image"
assert t.DEVNET2_SNAPSHOT_BODY is None and t.DEVNET2_STAGE3_CONSENSUS_MONITORING is False, "the snapshot body or the stage-3 monitoring is set already"
assert t.P5_REHEARSAL_POD == "docs/examples/storage-integrity-p5-rehearsal-pod.yaml" and t.DEVNET2_STATE_LIMIT_BYTES == 536870912
print("test_render.py reviews the P5 images; the live pins are P4; snapshot body unset; stage-3 monitoring off; L = 536870912")
EOF
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
mkdir -m 700 "$si_backup"
printf '%s\n' "$values" > "$si_backup/ledger-values.txt"
printf '%s\n' "$ARBITER_TAG_P5" "$ARBITER_DIGEST_P5" "$SN_COMMIT_P5" "$SN_DIGEST_P5" "$ARBITER_TAG_P4" "$ARBITER_DIGEST_P4" "$SN_COMMIT_P4" "$SN_DIGEST_P4" > "$si_backup/pins"
cat "$si_backup/ledger-values.txt"
echo "pins resolve"
```

Expected: `s3-ledger: P5_REHEARSAL ACTIVATION_EPOCH P5_END not recorded yet in <ref>`, `test_render.py reviews the P5 images; the live pins are P4; snapshot body unset; stage-3 monitoring off; L = 536870912`, `Ran <N> tests` and `OK`, the seventeen `NAME=value` lines and `pins resolve`. `$si_backup/pins` holds the eight pins in the order above (`si_p5_image` reads lines 3 and 4), `$si_backup/ledger-values.txt` every value the ledger gave (`si_p5_value` reads it). A `stop` from the ledger tool: stop and ask, never type a value in.

### 1.2 Identities and the stage-3 parameters

Unchanged since the indexer-c runbook and the P2 runbook:

| Identity | Value |
| --- | --- |
| Network | `devnet2`, schema snapshot `devnet2-si-v2`, genesis snapshot id `0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06` |
| SI indexers | 0 indexer-a (`snode-1`), 1 indexer-b (`snode-2`), `<INDEXER_C_ID>` indexer-c (`snode-3`) |
| Verifiers (the network pool) | `verifier-1`, `verifier-2`, `verifier-3` |
| Voters | `storage-integrity-si-v2-arbiter-0`, `-1`, `-2` |
| si-members | `member-a` (host 0), `member-b` (host 1), `member-c` (host `<INDEXER_C_ID>`), in P2/P3 mode and unregistered; none may be registered at the activation (K82) |
| Authority | `0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705`; key in `storage-integrity-devnet2-si-v2-authority`, present in every voter and in the rehearsal Pod as `ARBITER_AUTHORITY_PRIVATE_KEY_HEX`; it signs the rehearsal's activation and the activation |
| Failure domains | `<DEVNET2_FAILURE_DOMAINS>` (Plan S3-D, one `node:<hostname>` per enrolled indexer, the node its source ClickHouse is pinned to) |

The activation's stage-3 parameters (Plan S3-E4 contract §57), every field at its spec default except where a value is given: `group_size` 3, `network_fill_default` true, `min_safe_copies` 1, `strict_copies` false, `exclude_quarantined_hosts` false, every switch false (`purge`, `eviction_voiding`, `owner_aware_selection`, `placement`, `reselection`, `checkpoints`, `narrowed_drain`); `indexer_failure_domains` from `DEVNET2_FAILURE_DOMAINS` for indexers 0, 1 and `<INDEXER_C_ID>`; `max_lag_publications` 64 and `lag_low` 16; `evidence_timeout_l2_blocks` 300, about 10 advances of the registry cursor at `cursorAdvanceInterval: 1m`; `max_uncleaned_parts_per_partition` 2000, below the hosts' `softPartsPerPartition` 2400; `max_uncleaned_parts_per_table` 20000. The rehearsal (§3) and the activation (§7.2) both read them from `$si_backup/stage3-params.json`, which this block writes once, in the JSON of the arbiter's `Stage3Params` (proto field names, numbers as numbers), the file `p5rehearsal -rehearsal.params` decodes (local only):

```bash
[ ! -e "$si_backup/stage3-params.json" ] || { echo "stage3-params.json exists already: it is written once"; cat "$si_backup/stage3-params.json"; exit 1; }
si_p5_py params "$(si_p5_value DEVNET2_FAILURE_DOMAINS)" "$(si_p5_owner c)" "$si_backup/stage3-params.json" 2>&1 | tee "$si_backup/stage3-params.txt"
jq -c . "$si_backup/stage3-params.json"
```

Expected: three `indexer <id> failure domain node:<hostname>` lines (0, 1 and `<INDEXER_C_ID>`, three different nodes), the summary line and the parameters as one JSON line with `"indexer_failure_domains":[{"indexer_id":0,…},{"indexer_id":1,…},{"indexer_id":<INDEXER_C_ID>,…}]`. Two indexers on one node: stop (Plan S3-D's node pins put every source on its own node).

The indexer and database ids, read-only:

```bash
for id in 0 1 "$(si_p5_owner c)"; do si_rpc_on a "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sentio_getIndexerInfoById\",\"params\":[$id]}" | jq -c '.result | {indexerId, signer: (.signer | ascii_downcase)}'; done | tee "$si_backup/identities.txt"
```

Expected: three lines with `indexerId` 0, 1 and `<INDEXER_C_ID>` and the signers of the indexer-c runbook's §1.2 table. A different id or signer: stop.

### 1.3 Preconditions and baseline

- The owner-rebuild runbook is done (`P4_END` is recorded, §1.1); every track is unquarantined; every host's owner audit is clean in repair mode; the consensus epoch is `<ENROLMENT_EPOCH>` (no consensus update committed since the indexer-c enrolment) and `stage3` is unset.
- All six arbiter pods run `<ARBITER_DIGEST_P4>`, all three hosts `sha-<SN_COMMIT_P4>@<SN_DIGEST_P4>`, the three si-members one image, with no `storage_integrity.member.consensus` key in their configs.
- The voters still have 2Gi of memory and a 2Gi volume (L = 512 MiB, the stage-3 P0 runbook's §13.1), and every voter's `arbiter_state_bytes` is at most L/4 (K82 (vi)).
- `$R5` exists (Plan S3-E4 Task 78) and no rehearsal Pod runs.

```bash
db_c="$(cat "$HOME/si-devnet2-stage3-indexer-c/acceptance-db")"
{
  si_show | tee "$si_backup/show-before.json" | jq -c '{epoch, promotion_seq, max_writers: .current.max_writers, tracks_enabled: .current.tracks_enabled, stage3: .current.stage3, stage3_state, si_indexers: [.current.si_indexers[] | {indexer_id, snode_node_id}], verifiers: [.current.verifiers[].node_id]}'
  si_tracks | jq -c '[.indexers[] | {id: .indexer_id, published: .published.safe_block_seq, generation: .quarantine_generation, quarantined, rejected: .rejected_blocks}]'
  si_images | tee "$si_backup/images-before.txt"
  for i in 0 1 2; do si_capability "$i"; si_metrics "$i"; printf 'arbiter-%s state_bytes %s\n' "$i" "$(si_state_size "$i" | si_series_value arbiter_state_bytes)"; done
  si_kubectl get statefulset storage-integrity-si-v2-arbiter -o jsonpath='voter memory {.spec.template.spec.containers[?(@.name=="arbiter")].resources.limits.memory} volume {.spec.volumeClaimTemplates[0].spec.resources.requests.storage}{"\n"}'
  for x in a b c; do
    printf 'indexer-%s %s\n' "$x" "$(si_node_image "$x")"
    si_audit "$x" | grep -E 'owner_audit_(mode|unclean_tables)'
    printf 'member-%s %s\n' "$x" "$(si_container_image "$(si_member_pod "$x")" si-member)" | tee -a "$si_backup/member-images-before.txt"
    keys="$(si_p5_member_keys "$x")"
    printf 'member-%s consensus keys in its config: %s\n' "$x" "$keys"
  done
  si_features_stage3 | tee "$si_backup/features-before.json"
  si_status b devuser1 s3_p1
  si_status c "$db_c" s3e_e2e
  grep '^P4_END=' "$HOME/si-devnet2-stage3-p4-rebuild/p4-end.txt"
  si_p5_value P4_END
  si_kubectl get pod storage-integrity-si-v2-p5-rehearsal --ignore-not-found -o name | sed 's/^/rehearsal pod still present: /'
  ls -ld "$R5"
} 2>&1 | tee "$si_backup/baseline-p5.txt"
```

Expected: epoch `"<ENROLMENT_EPOCH>"`, `max_writers` `"3"`, `tracks_enabled` `true`, `stage3` and `stage3_state` `null`, the entries 0, 1 and `<INDEXER_C_ID>` with `snode-1`, `snode-2` and `snode-3`, verifiers `verifier-1..3`; every track unquarantined with no rejected block; all six arbiter pods on `<ARBITER_DIGEST_P4>`; every capability with `features` `["client_lanes_v1","signed_claims_v1","track_manifests_v1"]`, the same sealed tip on every voter, three active SNodes and three active verifiers; every `state_bytes` at most `134217728` (L/4); `voter memory 2Gi volume 2Gi`; all three hosts on `ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT_P4>@<SN_DIGEST_P4>`, every audit `mode="repair"` and `unclean_tables 0`; the three members on one digest-pinned image, each with `consensus keys in its config: 0`; `si_features_stage3` listing the three voters and the data-plane nodes that registered with the current leader (the six, or fewer when some registered with an earlier leader; §7.1 restarts every one), each `stage3: false`; `devuser1.s3_p1` `active` on indexer-b and `<db>.s3e_e2e` `active` on indexer-c; `P4_END=<date>` twice with the same date; no `rehearsal pod still present` line; `$R5` as `drwx------`. A `state_bytes` above L/4, other voter sizes, or a member with consensus keys: stop and ask.

The promotion budget, read-only (the P2 runbook's `si_budget_capture` and `si_budget`, the stage-3 P0 runbook's §13.4 estimator):

```bash
si_budget_capture
si_budget 2>&1 | tee "$si_backup/budget-start.txt"
```

Expected: `a drill spends at most <n>` with `<n>` at least 4. Less: stop.

## 2. Offline replay check

### 2.1 Rehearsal binaries

Plan S3-E4 Task 78 built every tool this runbook runs into `$R5` and recorded their SHA-256 in `$R5/p5-files.sha256` (decision D37): `bin/old/{statedigest,v20check,snapdump}` from `<ARBITER_TAG_P4>`, `bin/new/{statedigest,v21check,snapdump}` and the linux/amd64 rehearsal binary `bin/new/p5rehearsal` from `<ARBITER_TAG_P5>`, and the rehearsal's source `p5rehearsal/p5rehearsal_test.go`. This runbook builds nothing and never writes, rebuilds or deletes anything under `$R5`. The block checks the directory, the tags, the release file and every checksum, then shows on Task 78's v21 fixture (a container written by the integration harness, holding no live state) that the previous release refuses v21 at its version byte and the new one reads it (local only):

```bash
{
  [ -d "$R5" ] || { echo "$R5 is missing: Plan S3-E4 Task 78 builds it; stop"; exit 1; }
  mode="$(stat -f %Lp "$R5" 2>/dev/null || stat -c %a "$R5")"
  [ "$mode" = 700 ] || { echo "$R5 is mode $mode, not 700; stop"; exit 1; }
  [ "$(cat "$R5/old_tag.txt")" = "$(sed -n 5p "$si_backup/pins")" ] || { echo "$R5/old_tag.txt is not ARBITER_TAG_P4; stop"; exit 1; }
  [ "$(cat "$R5/new_tag.txt")" = "$(sed -n 1p "$si_backup/pins")" ] || { echo "$R5/new_tag.txt is not ARBITER_TAG_P5; stop"; exit 1; }
  [ "$(sed -n 1p "$R5/arbiter-release")" = "$(sed -n 1p "$si_backup/pins")" ] && [ "$(sed -n 2p "$R5/arbiter-release")" = "$(sed -n 2p "$si_backup/pins")" ] || { echo "$R5/arbiter-release does not name ARBITER_TAG_P5 and ARBITER_DIGEST_P5; stop"; exit 1; }
  listed="$(awk '{print $2}' "$R5/p5-files.sha256" | LC_ALL=C sort | tr '\n' ' ')"
  [ "$listed" = "bin/new/p5rehearsal bin/new/snapdump bin/new/statedigest bin/new/v21check bin/old/snapdump bin/old/statedigest bin/old/v20check p5rehearsal/p5rehearsal_test.go " ] || { echo "p5-files.sha256 lists $listed, not exactly the eight P5 files; stop"; exit 1; }
  (cd "$R5" && shasum -a 256 -c p5-files.sha256)
  file "$R5/bin/new/p5rehearsal" | grep -q 'ELF 64-bit LSB executable, x86-64' || { echo "bin/new/p5rehearsal is not a linux/amd64 executable; stop"; exit 1; }
  echo "old_tag $(cat "$R5/old_tag.txt"); new_tag $(cat "$R5/new_tag.txt"); release $(tr '\n' ' ' < "$R5/arbiter-release")"
  "$R5/bin/old/v20check" "$R5/v21-fixture.container" || true
  "$R5/bin/new/v21check" "$R5/v21-fixture.container"
  cp "$R5/p5-files.sha256" "$si_backup/p5-files.sha256"
  echo "rehearsal binaries ok"
} 2>&1 | tee "$si_backup/rehearsal-binaries.txt"
```

Expected: eight `…: OK` lines from `shasum`, `old_tag <ARBITER_TAG_P4>; new_tag <ARBITER_TAG_P5>; release <ARBITER_TAG_P5> <ARBITER_DIGEST_P5> `, `restore_refused version 21: unsupported snapshot version 21 …` from the old build, `restore_ok version 21 body interned` from the new one, and `rehearsal binaries ok`. A failed checksum or a different tag: stop; Task 78 rebuilds `$R5`, never this runbook.

### 2.2 Baseline

Every voter runs one image, the published digest of `$R5/old_tag.txt` (read-only):

```bash
{
  old_tag="$(cat "$R5/old_tag.txt")"
  live="$(si_images | awk '$1 ~ /^storage-integrity-si-v2-arbiter-/ {sub(/.*@/, "", $3); print $3}' | sort -u)"
  echo "old_tag $old_tag; voters run $live"
  [ -n "$live" ] && [ "$(printf '%s\n' "$live" | wc -l | tr -d ' ')" = 1 ] || { echo "the voters do not run exactly one image; stop"; exit 1; }
  [ "$live" = "$(sed -n 6p "$si_backup/pins")" ] || { echo "the voters run $live, not ARBITER_DIGEST_P4; stop"; exit 1; }
  printf '%s %s\n' "$old_tag" "$live" > "$si_backup/replay-baseline"
  echo "old matches the voters' live release $old_tag"
} 2>&1 | tee "$si_backup/replay-baseline.txt"
```

Expected: `old matches the voters' live release <ARBITER_TAG_P4>`.

### 2.3 Replay

Fresh read-only copies of all three voters' `raft` directories replay under both builds, local CPU only. The replay loads each voter's `arbiter.yaml` with `config.Load`, which needs `anchor.evm.private_key_hex`, so a throwaway random key is exported; the replay signs nothing. Each build also writes the container it would persist, and each restores the other's:

```bash
{
  read -r base_tag base_digest < "$si_backup/replay-baseline" || { echo "run §2.2 first"; exit 1; }
  [ "$(si_images | awk '$1 ~ /^storage-integrity-si-v2-arbiter-/ {sub(/.*@/, "", $3); print $3}' | sort -u)" = "$base_digest" ] || { echo "the voters no longer run $base_digest ($base_tag): run §2.2 again"; exit 1; }
  ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX="$(openssl rand -hex 32)"; export ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX
  si_show | tee "$si_backup/show-replay.json" | jq -c '{epoch, params_digest, promotion_seq}'
  for i in 0 1 2; do
    si_kubectl exec "storage-integrity-si-v2-arbiter-$i" -c arbiter -- tar -C /data -czf - raft > "$si_backup/arbiter-$i-raft.tgz"
    si_kubectl exec "storage-integrity-si-v2-arbiter-$i" -c arbiter -- cat /config/arbiter.yaml > "$si_backup/arbiter-$i.yaml"
    work="$(mktemp -d)"
    tar -C "$work" -xzf "$si_backup/arbiter-$i-raft.tgz"
    for v in old new; do
      out="$si_backup/digest-$i-$v"
      "$R5/bin/$v/statedigest" -data-dir "$work/raft" -config "$si_backup/arbiter-$i.yaml" > "$out.txt" 2> "$out.err" || { echo "statedigest $v failed for arbiter-$i (a raft.db that fails to open is a torn copy; run the block again):"; cat "$out.err"; exit 1; }
      grep -E '^(snapshot_index|last_index|applied_commands|rejected_commands|written_version|genesis_configured|live_check|apply_error|apply_rejected|key|spent_ids|snapshot_sha256)' "$out.txt" > "$out.cmp"
      if grep -q '^apply_error ' "$out.txt"; then echo "arbiter-$i $v: apply_error"; exit 1; fi
      "$R5/bin/$v/snapdump" -data-dir "$work/raft" -config "$si_backup/arbiter-$i.yaml" -out "$si_backup/written-$i-$v.container" || { echo "snapdump $v failed for arbiter-$i"; exit 1; }
    done
    if cmp -s "$si_backup/digest-$i-old.cmp" "$si_backup/digest-$i-new.cmp"; then echo "arbiter-$i: old and new replay identically"; else echo "arbiter-$i: DIFFERENT; stop"; diff "$si_backup/digest-$i-old.cmp" "$si_backup/digest-$i-new.cmp"; exit 1; fi
    cmp -s "$si_backup/written-$i-old.container" "$si_backup/written-$i-new.container" || { echo "arbiter-$i: the containers differ; stop"; exit 1; }
    echo "arbiter-$i: both builds write the same v20 container"
    "$R5/bin/old/v20check" "$si_backup/written-$i-new.container"
    "$R5/bin/new/v21check" "$si_backup/written-$i-old.container"
    grep -E '^(applied_commands|written_version|live_check|snapshot_sha256)' "$si_backup/digest-$i-new.txt"
    rm -rf "$work"
  done
} 2>&1 | tee "$si_backup/replay.txt"
```

Expected, for each voter: `old and new replay identically` (`snapshot_sha256` included), `both builds write the same v20 container`, `restore_ok version 20` from the old build, `restore_ok version 20 body none` from the new one, `applied_commands` above zero, `written_version 20` and the `live_check` lines naming the epoch and `promotion_seq` of `show-replay.json` (or a later sequence if a promotion landed between the read and the copy); no `apply_error`. Any difference: stop before §3.

## 3. Rehearsal

The rehearsal (K81) runs the activation on copies of the live state before any component rolls, so a failure costs no rollback. `p5rehearsal` is a throwaway test binary of the arbiter's `fsm` package, built by Plan S3-E4 Task 78 from `<ARBITER_TAG_P5>` and reproduced in §3.2 for review. It restores three FSMs in one process, each from its own voter's copy, prints each one's epoch, promotion sequence and stage-2 drain-gate answer and their state digests, signs one activation update with `stage3-params.json` under the authority key the Pod reads from the voters' Secret, and feeds that one entry to all three in the same order, which is everything Raft guarantees an FSM (stage 3 changes no Raft code, only the snapshot container). Each FSM then persists v21 through the write-then-verify path and restarts by restoring its own container; a second persist through Plan S3-E2's interner seam (`internSnapshotBodyFunc`) forces a mismatch and must fall back to the plain body and count it. It has no network peer, orchestrator or anchor. It reads the copies, the parameters and the voters' non-secret `arbiter.yaml` copies from the Pod's `/rehearsal`, writes its containers there (`rehearsal-v21-<i>.container`, interned) and prints one result line per step; nothing leaves the Pod but those lines and, in §3.5, the first 14 bytes of voter 0's container, and §3.6 deletes the Pod, its signed update and its containers before any component rolls. The real voters' restarts from v21 are proven in Plan S3-E4 Task 78's integration and observed live in §8.

### 3.1 Copies

The three copies must hold the same applied log, because the rehearsal compares their digests before it applies anything, and each copy must pass the stage-2 drain gate, because the activation is judged by it: with no SI write running (the freeze above) both hold within minutes of the last write. The block takes the three copies back to back, refuses them if a consensus update or promotion landed meanwhile, and replays each locally with the new build to compare their last log index and state digests (read-only on the cluster; local CPU; it overwrites an earlier attempt's copies, so it may be run again):

```bash
{
  ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX="$(openssl rand -hex 32)"; export ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX
  si_show | jq -c '{epoch, promotion_seq}' > "$si_backup/rehearsal-show-before.json"
  for i in 0 1 2; do
    si_kubectl exec "storage-integrity-si-v2-arbiter-$i" -c arbiter -- tar -C /data -czf - raft > "$si_backup/rehearsal-copy-$i.tgz"
    si_kubectl exec "storage-integrity-si-v2-arbiter-$i" -c arbiter -- cat /config/arbiter.yaml > "$si_backup/rehearsal-arbiter-$i.yaml"
  done
  si_show | jq -c '{epoch, promotion_seq}' > "$si_backup/rehearsal-show-after.json"
  cmp -s "$si_backup/rehearsal-show-before.json" "$si_backup/rehearsal-show-after.json" || { echo "a consensus update or a promotion landed during the copies: run this block again"; exit 1; }
  for i in 0 1 2; do
    work="$(mktemp -d)"
    tar -C "$work" -xzf "$si_backup/rehearsal-copy-$i.tgz"
    "$R5/bin/new/statedigest" -data-dir "$work/raft" -config "$si_backup/rehearsal-arbiter-$i.yaml" > "$si_backup/rehearsal-digest-$i.txt" 2> "$si_backup/rehearsal-digest-$i.err" || { echo "statedigest failed for copy $i (a torn copy: run this block again):"; cat "$si_backup/rehearsal-digest-$i.err"; exit 1; }
    grep -E '^(last_index|written_version|live_check|key|spent_ids|snapshot_sha256)' "$si_backup/rehearsal-digest-$i.txt" > "$si_backup/rehearsal-digest-$i.cmp"
    rm -rf "$work"
  done
  for i in 1 2; do cmp -s "$si_backup/rehearsal-digest-0.cmp" "$si_backup/rehearsal-digest-$i.cmp" || { echo "copies 0 and $i do not hold the same applied log (a log entry landed between them): run this block again"; exit 1; }; done
  cat "$si_backup/rehearsal-show-before.json"
  grep -E '^(last_index|written_version|live_check|snapshot_sha256)' "$si_backup/rehearsal-digest-0.cmp"
  (cd "$si_backup" && shasum -a 256 rehearsal-copy-0.tgz rehearsal-copy-1.tgz rehearsal-copy-2.tgz)
  echo "the three copies hold the same state"
} 2>&1 | tee "$si_backup/rehearsal-copies.txt"
```

Expected: the epoch `<ENROLMENT_EPOCH>` and a `promotion_seq`; one `last_index`, `written_version 20`, the `live_check` lines with that epoch and sequence and one `snapshot_sha256`; three checksums and `the three copies hold the same state`. `a log entry landed between them` happens when the registry cursor advanced during the copies (once a minute): run the block again.

### 3.2 Rehearsal pod

The rehearsal's source, as Plan S3-E4 Task 78 built it into `$R5/bin/new/p5rehearsal` (its SHA-256 is `<P5REHEARSAL_SHA256>`, the `p5rehearsal/p5rehearsal_test.go` line of `p5-files.sha256`):

```go
<P5REHEARSAL_SOURCE>
```

The Pod is the reviewed example `docs/examples/storage-integrity-p5-rehearsal-pod.yaml`: `storage-integrity-si-v2-p5-rehearsal` in `sentio-network-devnet2`, labelled only `storage-integrity.sentio.xyz/rehearsal: p5` (no Service selects it), one container `rehearsal` on `ghcr.io/sentioxyz/arbiter@<ARBITER_DIGEST_P5>` running `sleep 86400`, `ARBITER_AUTHORITY_PRIVATE_KEY_HEX` from Secret `storage-integrity-devnet2-si-v2-authority` key `private-key` as its only Secret, an emptyDir `rehearsal` (`sizeLimit: 8Gi`) at `/rehearsal`, the voters' limits (cpu `"2"`, memory `2Gi`), `restartPolicy: Never`, `automountServiceAccountToken: false`, no port. First the read-only checks: the source above is the checked one, the example is that Pod, and the API server accepts it (`--dry-run=server` persists nothing):

```bash
{
  python3 -I -B - docs/storage-integrity-devnet2-stage3-p5-v21.md "$si_backup/p5rehearsal_test.go" <<'EOF'
import re, sys
blocks = re.findall(r"^```go\n(.*?)^```$", open(sys.argv[1], encoding="utf-8", newline="").read(), re.S | re.M)
assert len(blocks) == 1, "the runbook must hold exactly one go block, the rehearsal's source"
open(sys.argv[2], "w", encoding="utf-8", newline="").write(blocks[0])
EOF
  sum="$(shasum -a 256 "$si_backup/p5rehearsal_test.go" | cut -d' ' -f1)"
  echo "runbook source $sum"
  grep -qxF "$sum  p5rehearsal/p5rehearsal_test.go" "$si_backup/p5-files.sha256" || { echo "the source in this runbook is not the one p5-files.sha256 records; stop"; exit 1; }
  [ "$sum" = '<P5REHEARSAL_SHA256>' ] || { echo "the source in this runbook is not the reviewed one (<P5REHEARSAL_SHA256>); stop"; exit 1; }
  cmp -s "$si_backup/p5rehearsal_test.go" "$R5/p5rehearsal/p5rehearsal_test.go" || { echo "$R5/p5rehearsal/p5rehearsal_test.go differs from the source in this runbook; stop"; exit 1; }
  si_p5_py pod docs/examples/storage-integrity-p5-rehearsal-pod.yaml "$(sed -n 2p "$si_backup/pins")"
  [ -z "$(si_kubectl get pod storage-integrity-si-v2-p5-rehearsal --ignore-not-found -o name)" ] || { echo "a rehearsal Pod exists already: delete it (§3.6) before anything else; stop"; exit 1; }
  printf 'authority Secret keys: %s\n' "$(si_secret_keys storage-integrity-devnet2-si-v2-authority)"
  si_kubectl apply --dry-run=server -f docs/examples/storage-integrity-p5-rehearsal-pod.yaml
} 2>&1 | tee "$si_backup/rehearsal-pod-check.txt"
```

Expected: `runbook source <P5REHEARSAL_SHA256>`, the Pod example line from `si_p5_py pod`, `authority Secret keys: private-key ` and `pod/storage-integrity-si-v2-p5-rehearsal created (server dry run)`. Then the Pod and its inputs: the apply, the wait until it runs, its live spec, and the copies of the binary, the parameters, the three voter copies and the three voters' `arbiter.yaml` (no secret in them: the anchor key comes from the environment) into `/rehearsal` (a write into the Pod), each checked by its SHA-256:

Confirm before running.

```bash
[ ! -e "$si_backup/rehearsal-pod-applied" ] || { echo "the rehearsal Pod was applied already in this run; never re-run this block (§3.6 deletes it; then start again at §3.1)"; exit 1; }
si_kubectl apply -f docs/examples/storage-integrity-p5-rehearsal-pod.yaml
date -u +%Y-%m-%dT%H:%M:%SZ > "$si_backup/rehearsal-pod-applied"
si_kubectl wait --for=condition=Ready pod/storage-integrity-si-v2-p5-rehearsal --timeout=5m
si_kubectl get pod storage-integrity-si-v2-p5-rehearsal -o json > "$si_backup/rehearsal-pod.json"
si_p5_py pod-live "$si_backup/rehearsal-pod.json" "$(sed -n 2p "$si_backup/pins")"
{
  si_p5_put "$R5/bin/new/p5rehearsal" /rehearsal/p5rehearsal
  si_rehearsal chmod 700 /rehearsal/p5rehearsal
  si_p5_put "$si_backup/stage3-params.json" /rehearsal/stage3-params.json
  for i in 0 1 2; do si_p5_put "$si_backup/rehearsal-copy-$i.tgz" "/rehearsal/rehearsal-copy-$i.tgz"; si_p5_put "$si_backup/rehearsal-arbiter-$i.yaml" "/rehearsal/arbiter-$i.yaml"; done
  si_rehearsal ls -l /rehearsal
} 2>&1 | tee "$si_backup/rehearsal-inputs.txt"
grep -qxF "$(sed -n 's#^\([0-9a-f]*\)  bin/new/p5rehearsal$#/rehearsal/p5rehearsal \1#p' "$si_backup/p5-files.sha256")" "$si_backup/rehearsal-inputs.txt" || { echo "the Pod's p5rehearsal is not the checked binary; delete the Pod (§3.6) and stop"; exit 1; }
```

Expected: `pod/storage-integrity-si-v2-p5-rehearsal created`, `condition met`, `the live rehearsal Pod runs as the example states it`, eight `/rehearsal/<file> <sha256>` lines (the binary's equals its `p5-files.sha256` line, the copies' equal `rehearsal-copies.txt`) and the listing.

### 3.3 Activation and restarts from v21

The run, inside the Pod. Its full log stays in the Pod's `/rehearsal/rehearsal.log`; only the result lines, the test verdict and failure lines without a token leave it (`rehearsal.txt`), and `P5_REHEARSAL=<date>` is appended once `rehearsal ok` printed:

Confirm before running.

```bash
[ ! -e "$si_backup/rehearsal.txt" ] || { echo "the rehearsal ran already in this Pod; never re-run this block (§3.6 deletes the Pod; then start again at §3.1)"; exit 1; }
# shellcheck disable=SC2016 # the in-pod script is single-quoted on purpose: it expands inside the Pod
si_rehearsal sh -c '/rehearsal/p5rehearsal -test.run '\''^TestP5Rehearsal$'\'' -test.v -rehearsal.dir /rehearsal -rehearsal.params /rehearsal/stage3-params.json > /rehearsal/rehearsal.log 2>&1; echo "exit $?" >> /rehearsal/rehearsal.log' || true
si_rehearsal grep -E '^(voter [0-2] restored |digest [0-2] |activation applied |persist [0-2] |restart [0-2] |mismatch [0-2] |rehearsal ok$|--- (PASS|FAIL): TestP5Rehearsal |exit [0-9]+$|[[:space:]]+p5rehearsal_test\.go:[0-9]+: )' /rehearsal/rehearsal.log | grep -v 'eyJ' > "$si_backup/rehearsal.txt" || true
if grep -qx 'rehearsal ok' "$si_backup/rehearsal.txt" && grep -qx 'exit 0' "$si_backup/rehearsal.txt"; then echo "P5_REHEARSAL=$(date -u +%Y-%m-%d)" >> "$si_backup/rehearsal.txt"; fi
cat "$si_backup/rehearsal.txt"
```

Then the read-only check of the activation and the restarts:

```bash
si_p5_py rehearsal "$si_backup/rehearsal.txt" "$(si_p5_value ENROLMENT_EPOCH)" 536870912 activation 2>&1 | tee "$si_backup/rehearsal-activation-check.txt"
```

Expected in `rehearsal.txt`: `voter 0 restored epoch=<ENROLMENT_EPOCH> promotion_seq=<p> drain_refusal=""` and the same for voters 1 and 2, three `digest <i> <sha256>` lines with one digest, `activation applied epoch=<ENROLMENT_EPOCH + 1>`, three `persist <i> version 21 body interned bytes=<n> fallback=0 persist_peak_heap_bytes=<n>`, three `restart <i> restore_ok version 21 body interned digest_equal=true`, three `mismatch <i> version 21 body plain fallback=1 digest_equal=true`, `rehearsal ok`, `--- PASS: TestP5Rehearsal (<s>s)`, `exit 0` and `P5_REHEARSAL=<date>`; then `rehearsal activation ok: epoch <ENROLMENT_EPOCH> -> <ENROLMENT_EPOCH + 1>, …, Persist peak <n> bytes (L 536870912)`. A `drain_refusal` other than `""`: a copy still held work of the last write; delete the Pod (§3.6), wait a few minutes and start again at §3.1. Any other failure (a different digest, a refused activation, a refused restore, a peak above L): delete the Pod (§3.6) and stop; nothing rolled, so nothing needs a rollback.

### 3.4 Forced mismatch

The forced-mismatch run of §3.3 replaced the interner's output in each FSM's second persist, so the write-then-verify comparison failed: the writer must persist the plain body, count one fallback, and the container must still restore to the same digest (read-only):

```bash
{
  grep -E '^mismatch ' "$si_backup/rehearsal.txt"
  si_p5_py rehearsal "$si_backup/rehearsal.txt" "$(si_p5_value ENROLMENT_EPOCH)" 536870912 mismatch
} 2>&1 | tee "$si_backup/rehearsal-mismatch-check.txt"
```

Expected: the three `mismatch` lines and `rehearsal forced mismatch ok: voters 0, 1, 2 persisted the plain body with fallback=1 and an equal digest; rehearsal ok; P5_REHEARSAL=<date>`.

### 3.5 The previous release refuses v21

`<ARBITER_TAG_P4>`'s reader refuses a container by its version byte before reading anything after it (`fsm/snapshot.go:274`), so the first 14 bytes of voter 0's interned v21 container (magic, version byte 21, body byte 1, document length) show the refusal without the container's body, which holds the rehearsal's signed update, leaving the Pod. Read-only on the Pod; local:

```bash
{
  si_rehearsal head -c 14 /rehearsal/rehearsal-v21-0.container > "$si_backup/rehearsal-v21-0.container"
  si_p5_py header "$si_backup/rehearsal-v21-0.container"
  "$R5/bin/old/v20check" "$si_backup/rehearsal-v21-0.container" || true
} 2>&1 | tee "$si_backup/rehearsal-old-refuses-v21.txt"
```

Expected: `header AFSM version 21 body 1 doc_length <n>` and `restore_refused version 21: unsupported snapshot version 21 …` (the previous release's version list ends at 20).

### 3.6 Cleanup

The Pod goes before any component rolls, and with it its emptyDir: the signed rehearsal update, the containers and the log:

Confirm before running.

```bash
si_kubectl delete pod storage-integrity-si-v2-p5-rehearsal --ignore-not-found --wait
```

Then, read-only:

```bash
{
  [ -z "$(si_kubectl get pod storage-integrity-si-v2-p5-rehearsal --ignore-not-found -o name)" ] && echo "rehearsal Pod deleted at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  si_kubectl get pods -l storage-integrity.sentio.xyz/rehearsal=p5 -o name | sed 's/^/still labelled: /'
} 2>&1 | tee "$si_backup/rehearsal-pod-deleted.txt"
```

Expected: `rehearsal Pod deleted at <time>` and no `still labelled` line. Go on to §4 only with `rehearsal.txt` holding `rehearsal ok` and both checks of §3.3 and §3.4 passed; after a failed rehearsal start again at §3.1 (move the earlier `rehearsal*` files aside first: `mv "$si_backup/rehearsal.txt" "$si_backup/rehearsal-failed-$(date -u +%Y%m%dT%H%M%SZ).txt"; rm "$si_backup/rehearsal-pod-applied"`).

## 4. Verifiers

One commit `chore(devnet2): arbiter <ARBITER_TAG_P5> for stage 3 P5 (verifiers now, voters in §6)` in `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml` and `charts/storage-integrity/tests/test_render.py`: `arbiter.image.digest` set to `<ARBITER_DIGEST_P5>` with its mirror `DEVNET2_ARBITER_DIGEST`; the version comment names `<ARBITER_TAG_P5>` (arbiter-core `<CORE_TAG_P5>`, arbiter-proto `<PROTO_TAG_P5>`, housegate `<HG_TAG_P5>`: snapshot v21 after the `stage3` activation, every stage-3 switch off) and the previous pin `<ARBITER_TAG_P4> <ARBITER_DIGEST_P4>`, a valid way back until §7.2. The sync rolls the verifiers one at a time (`RollingUpdate`); the voters share the digest but are `OnDelete`, so they keep `<ARBITER_TAG_P4>` until §6.

Confirm before running.

```bash
[ ! -e "$si_backup/stage3-p5-verifiers-commit" ] || { echo "the verifier commit was pushed already; never re-run this block"; exit 1; }
{ grep -qx 'rehearsal ok' "$si_backup/rehearsal.txt" && grep -q "^rehearsal Pod deleted at " "$si_backup/rehearsal-pod-deleted.txt"; } || { echo "the rehearsal has not passed or its Pod is not deleted (§3); stop"; exit 1; }
python3 -B charts/storage-integrity/tests/test_render.py
si_head_is charts/storage-integrity/tests/test_render.py k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml
si_render_diff_is storage-integrity-si-v2 "changed Job/storage-integrity-si-v2-bootstrap-source" "changed StatefulSet/storage-integrity-si-v2-arbiter" "changed StatefulSet/storage-integrity-si-v2-verifier"
git rev-parse HEAD > "$si_backup/stage3-p5-verifiers-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
```

Then, read-only:

```bash
{
  si_images
  for i in 0 1 2; do si_metrics "$i"; done
  si_features_stage3
} 2>&1 | tee "$si_backup/verify-verifiers.txt"
```

Expected: the three verifiers on `<ARBITER_DIGEST_P5>`, the voters on `<ARBITER_DIGEST_P4>`; three active verifiers on every voter within minutes and the safe watermark following the sealed tip; `si_features_stage3` listing `verifier-1..3` with `stage3: true` (they re-registered with the `<ARBITER_TAG_P4>` leader, which records a feature it does not use) and the three SNodes with `stage3: false`.

## 5. Hosts

One commit per host, indexer-b, then indexer-c, then indexer-a: sentio-node `sha-<SN_COMMIT_P5>@<SN_DIGEST_P5>` (`DEVNET2_STAGE3_P5_SOURCE_IMAGE`) in that host's `k8s-sea/sentio-network-devnet2/sentio-node-indexer-<x>-storage-integrity-si-v2-source-image.yaml` and its mirror in `test_render.py` (`INDEXER_B_SOURCE_IMAGE`, `INDEXER_C_SOURCE_IMAGE`; for indexer-a `FRESH_SOURCE_IMAGE` and `docs/examples/storage-integrity-fresh-source-image.yaml`), with the header comment naming `<SN_COMMIT_P5>` (housegate `<HG_TAG_P5>`, arbiter-core `<CORE_TAG_P5>`: `stage3_v1`, admission codes 12 and 13 retryable at the ingress, the member consent command, the inert member consensus keys) and the rollback target `sha-<SN_COMMIT_P4>@<SN_DIGEST_P4>`. Each sync restarts that host's pod (its si-member is its own StatefulSet and keeps running); its tables answer `SOURCE_UNAVAILABLE` (retryable) until its SNode is active again and Pending (733) until the startup owner audit is clean. The SNode advertises `stage3_v1` from its first start; the `<ARBITER_TAG_P4>` leader records it. Then the si-members, in the same order (§5.4).

### 5.1 indexer-b

Confirm before running.

```bash
[ ! -e "$si_backup/stage3-p5-indexer-b-commit" ] || { echo "indexer-b's commit was pushed already; never re-run this block"; exit 1; }
python3 -B charts/storage-integrity/tests/test_render.py
si_head_is charts/storage-integrity/tests/test_render.py k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml
si_render_diff_is sentio-node-devnet2-indexer-b "changed StatefulSet/sentio-node-devnet2-indexer-b"
si_kubectl get pod "$(si_member_pod b)" -o jsonpath='{.metadata.uid}{"\n"}' > "$si_backup/member-b-uid"
si_replica_hosts b > "$si_backup/replica-hosts-b-before.json"
git rev-parse HEAD > "$si_backup/stage3-p5-indexer-b-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-b
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-b --timeout=15m
```

After each host's sync (this one, §5.2's and §5.3's), read-only, with `x` set to that host (re-run until its audit series are back):

```bash
x=b
{
  [ "$(si_node_image "$x")" = "$(si_p5_image)" ] && echo "indexer-$x runs the P5 image"
  si_kubectl get pod "sentio-node-devnet2-indexer-$x-0" -o jsonpath='{range .status.containerStatuses[*]}{.name} ready={.ready} restarts={.restartCount}{"\n"}{end}'
  printf 'si-member uid %s (was %s)\n' "$(si_kubectl get pod "$(si_member_pod "$x")" -o jsonpath='{.metadata.uid}')" "$(cat "$si_backup/member-$x-uid")"
  si_info "$x"
  si_audit "$x"
  si_p5_keeper_findings "$x"
  si_replica_hosts "$x" | tee "$si_backup/replica-hosts-$x-after.json"
  cmp -s "$si_backup/replica-hosts-$x-before.json" "$si_backup/replica-hosts-$x-after.json" && echo "replica_hosts unchanged (the static list until placement)"
  si_features_stage3
} 2>&1 | tee "$si_backup/verify-indexer-$x.txt"
```

Expected: `indexer-<x> runs the P5 image`, every container ready; the si-member's uid unchanged; `enrolled` and `"quarantined":false`; the audit `mode="repair"` with `unclean_tables 0` and one more `runs_total{trigger="startup"}`; `sentio_node_storage_integrity_keeper_findings{reason="non_incarnation_root"} 0` and `{reason="unknown_replica"} 0` (a finding is a warn log naming its path; the audit drops nothing: stop and read it); `replica_hosts unchanged (the static list until placement)`; this host's SNode (`snode-2` for indexer-b, `snode-3` for indexer-c, `snode-1` for indexer-a) with `stage3: true`. Then that host's acceptance write, in its own confirmed block (indexer-b's here, in the user's terminal):

Confirm before running.

```bash
key_file=/path/to/devuser1-owner.key
si_p5_accept "$key_file" b 5401 5.1 2>&1 | tee "$si_backup/accept-b-5401.txt"
```

Expected: `track 1 published <x> -> <y>` and `indexer-b row 5401 in devuser1.s3_p1: safe count 1`.

### 5.2 indexer-c

After indexer-b's checks and write:

Confirm before running.

```bash
[ ! -e "$si_backup/stage3-p5-indexer-c-commit" ] || { echo "indexer-c's commit was pushed already; never re-run this block"; exit 1; }
python3 -B charts/storage-integrity/tests/test_render.py
si_head_is charts/storage-integrity/tests/test_render.py k8s-sea/sentio-network-devnet2/sentio-node-indexer-c-storage-integrity-si-v2-source-image.yaml
si_render_diff_is sentio-node-devnet2-indexer-c "changed StatefulSet/sentio-node-devnet2-indexer-c"
si_kubectl get pod "$(si_member_pod c)" -o jsonpath='{.metadata.uid}{"\n"}' > "$si_backup/member-c-uid"
si_replica_hosts c > "$si_backup/replica-hosts-c-before.json"
git rev-parse HEAD > "$si_backup/stage3-p5-indexer-c-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-c
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-c --timeout=15m
```

Then §5.1's read-only block with `x=c`, and indexer-c's acceptance write, in the user's terminal:

Confirm before running.

```bash
key_file=/path/to/devuser1-owner.key
si_p5_accept "$key_file" c 5402 5.2 2>&1 | tee "$si_backup/accept-c-5402.txt"
```

Expected: `track <INDEXER_C_ID> published <x> -> <y>` and `indexer-c row 5402 in <db>.s3e_e2e: safe count 1`.

### 5.3 indexer-a

After indexer-c's checks and write:

Confirm before running.

```bash
[ ! -e "$si_backup/stage3-p5-indexer-a-commit" ] || { echo "indexer-a's commit was pushed already; never re-run this block"; exit 1; }
python3 -B charts/storage-integrity/tests/test_render.py
si_head_is charts/storage-integrity/tests/test_render.py docs/examples/storage-integrity-fresh-source-image.yaml k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml
si_render_diff_is sentio-node-devnet2-indexer-a "changed StatefulSet/sentio-node-devnet2-indexer-a"
si_kubectl get pod "$(si_member_pod a)" -o jsonpath='{.metadata.uid}{"\n"}' > "$si_backup/member-a-uid"
si_replica_hosts a > "$si_backup/replica-hosts-a-before.json"
git rev-parse HEAD > "$si_backup/stage3-p5-indexer-a-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
```

Then §5.1's read-only block with `x=a`, and indexer-a's acceptance write from this session:

Confirm before running.

```bash
si_p5_accept - a 5403 5.3 2>&1 | tee "$si_backup/accept-a-5403.txt"
```

Expected: the INSERT's duration, `acceptance pod phase: Succeeded`, `track 0 published <x> -> <y>` and `1`.

### 5.4 si-members

One commit per si-member, member-b, then member-c, then member-a: in `k8s-sea/sentio-network-devnet2/sentio-node-indexer-<x>-storage-integrity-si-v2.yaml`, `siMember.image` = `ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT_P5>@<SN_DIGEST_P5>`, its comment naming the stage-3 P5 member image (consensus mode off) and the image it replaces (`member-images-before.txt`), the rollback target. Nothing else of `siMember` changes: the member stays in its P2/P3 mode, `storage_integrity.member.consensus` stays absent, so it registers nothing, signs nothing and creates no Keeper replica; its serving endpoint and its serving setting stay as the P3 runbook left them. Each sync restarts that member's pod only (the host's checksum does not cover the member); while it restarts its copies are not served and its leases end, so the host answers its peers' tables from their owners as usual. Read-only, before each member's commit:

```bash
x=b
si_pod_state "sentio-node-devnet2-indexer-$x-0" | tee "$si_backup/member-window-$x.txt"
```

member-b:

Confirm before running.

```bash
[ ! -e "$si_backup/stage3-p5-member-b-commit" ] || { echo "member-b's commit was pushed already; never re-run this block"; exit 1; }
python3 -B charts/storage-integrity/tests/test_render.py
si_head_is k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2.yaml
si_render_diff_is sentio-node-devnet2-indexer-b "changed StatefulSet/sentio-node-devnet2-indexer-b-si-member"
git rev-parse HEAD > "$si_backup/stage3-p5-member-b-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-b
si_kubectl rollout status "statefulset/$(si_member_sts b)" --timeout=15m
```

member-c, after member-b's checks:

Confirm before running.

```bash
[ ! -e "$si_backup/stage3-p5-member-c-commit" ] || { echo "member-c's commit was pushed already; never re-run this block"; exit 1; }
python3 -B charts/storage-integrity/tests/test_render.py
si_head_is k8s-sea/sentio-network-devnet2/sentio-node-indexer-c-storage-integrity-si-v2.yaml
si_render_diff_is sentio-node-devnet2-indexer-c "changed StatefulSet/sentio-node-devnet2-indexer-c-si-member"
git rev-parse HEAD > "$si_backup/stage3-p5-member-c-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-c
si_kubectl rollout status "statefulset/$(si_member_sts c)" --timeout=15m
```

member-a, after member-c's checks:

Confirm before running.

```bash
[ ! -e "$si_backup/stage3-p5-member-a-commit" ] || { echo "member-a's commit was pushed already; never re-run this block"; exit 1; }
python3 -B charts/storage-integrity/tests/test_render.py
si_head_is k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2.yaml
si_render_diff_is sentio-node-devnet2-indexer-a "changed StatefulSet/sentio-node-devnet2-indexer-a-si-member"
git rev-parse HEAD > "$si_backup/stage3-p5-member-a-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status "statefulset/$(si_member_sts a)" --timeout=15m
```

After each member's sync, read-only, with `x` set to that host (re-run after a minute while a copy is still behind):

```bash
x=b
{
  si_pod_state "sentio-node-devnet2-indexer-$x-0" > "$si_backup/member-window-$x-host-after.txt"
  cmp -s "$si_backup/member-window-$x.txt" "$si_backup/member-window-$x-host-after.txt" && echo "host indexer-$x unchanged"
  si_pod_state "$(si_member_pod "$x")"
  [ "$(si_container_image "$(si_member_pod "$x")" si-member)" = "$(si_p5_image)" ] && echo "member-$x runs the P5 image"
  keys="$(si_p5_member_keys "$x")"
  printf 'member-%s consensus keys in its config: %s\n' "$x" "$keys"
  si_member_metrics "$x" 'sentio_node_si_member_(ready|audit_clean|copies|foreign_tables|diverged_tables|shadow_mismatches_total)'
  si_host_replica_metrics "$x"
  si_copies_check
} 2>&1 | tee "$si_backup/verify-member-$x.txt"
```

Expected: `host indexer-<x> unchanged`; a new member pod, `member-<x> runs the P5 image` and `consensus keys in its config: 0`; `sentio_node_si_member_ready 1`, `sentio_node_si_member_audit_clean 1` once its startup audit ran, every `foreign_tables` and `diverged_tables` `0` and every `shadow_mismatches_total` `0` (the counters restart with the pod); `sentio_node_storage_integrity_member_status_up 1` on the host; `copies equal: 6 of 6` once the member caught up. A `member_status_up 0` or a foreign or diverged table: stop (the P2 runbook's §14 and the P3 runbook's §9 say what each means).

## 6. Voters

Followers first, the leader last, one pod at a time, so two voters always form a quorum; the sealed tip must agree on all three before the next deletion. The voters are `OnDelete`, so each deletion moves a pod onto the digest §4 committed. A P5 voter writes v20 until `stage3` commits and proposes nothing new, so until §7.2 the previous image can take a voter back the same way (§10.1). First the order, read-only: the two followers, then the current leader, one ordinal per line in `$si_backup/voter-order`. The block never overwrites the file; to choose a new order deliberately, only before the first deletion, remove it (`rm "$si_backup/voter-order"`) and run the block again:

```bash
if [ -s "$si_backup/voter-order" ]; then echo "$si_backup/voter-order exists already, so the order stays:"; cat "$si_backup/voter-order"; echo "stop"; exit 1; fi
leader="$(si_leader)"
{ printf '0\n1\n2\n' | grep -vx "$leader"; echo "$leader"; } > "$si_backup/voter-order.new"
[ "$(sort "$si_backup/voter-order.new" | tr '\n' ' ')" = "0 1 2 " ] || { echo "no order of the three voters around leader '$leader'; stop"; exit 1; }
mv "$si_backup/voter-order.new" "$si_backup/voter-order"
cat "$si_backup/voter-order"
```

Then each voter in its own confirmed block, the next only once the previous returned with matching sealed tips. The first follower:

Confirm before running.

```bash
si_replace_voter "$(sed -n 1p "$si_backup/voter-order")" 2>&1 | tee "$si_backup/replace-voter-1.txt"
```

The second follower:

Confirm before running.

```bash
si_replace_voter "$(sed -n 2p "$si_backup/voter-order")" 2>&1 | tee "$si_backup/replace-voter-2.txt"
```

The leader:

Confirm before running.

```bash
si_replace_voter "$(sed -n 3p "$si_backup/voter-order")" 2>&1 | tee "$si_backup/replace-voter-3.txt"
```

Then, read-only:

```bash
{
  si_images
  for i in 0 1 2; do si_capability "$i"; si_encoding "$i"; si_v21check "$i"; done
  si_stage3
} 2>&1 | tee "$si_backup/verify-voters.txt"
```

Expected: all six arbiter pods on `<ARBITER_DIGEST_P5>`; every capability with its own `node_id`, `protocol_version` `1`, `updates_enabled` `true` and `features` `["client_lanes_v1","signed_claims_v1","track_manifests_v1","stage3_v1"]`; every voter exporting `arbiter_snapshot_encoding_fallback_total 0` and both `arbiter_snapshot_body_bytes{kind="plain"}` and `{kind="interned"}`; every voter's newest snapshot `version 20 body -` with `restore_ok version 20 body none` (or `no snapshot yet` for a voter that has not written one since its restart); `si_stage3` `null`.

## 7. Activation

### 7.1 Re-register the data plane

A new leader starts with an empty feature book, its gate accepts only features advertised to it in its current term, and §6 replaced the leader last. Restart every verifier and the three SNodes so that each registers with the current leader advertising `stage3_v1` (K82 (i)); each host restart gives its tables a short retryable `SOURCE_UNAVAILABLE` window and a Pending hold until its startup audit is clean. The si-members are not data-plane registrations and are not restarted. From here until §7.3 has passed, make no Raft membership change.

Confirm before running.

```bash
si_kubectl rollout restart statefulset/storage-integrity-si-v2-verifier
si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
for x in b c a; do
  si_kubectl rollout restart "statefulset/sentio-node-devnet2-indexer-$x"
  si_kubectl rollout status "statefulset/sentio-node-devnet2-indexer-$x" --timeout=15m
done
```

Then the read-only checks (re-run until every host's audit is clean again):

```bash
{
  si_features_stage3 | tee "$si_backup/features-before-activation.json"
  si_p5_py features "$si_backup/features-before-activation.json" "$(si_p5_owner c)"
  si_tracks | tee "$si_backup/tracks-before-activation.json" | jq -c '[.indexers[] | {id: .indexer_id, published: .published.safe_block_seq, quarantined}]'
  for i in 0 1 2; do si_metrics "$i"; done
  for x in a b c; do si_info "$x"; si_audit "$x" | grep -E 'owner_audit_(mode|unclean_tables)'; done
} 2>&1 | tee "$si_backup/verify-before-activation.txt"
```

Expected: `feature book: voters storage-integrity-si-v2-arbiter-0, …-1, …-2; snode-1, snode-2, snode-3 (indexers 0, 1, <INDEXER_C_ID>) and verifier-1..3, each advertising stage3_v1; no member id`; every track unquarantined; every voter with `arbiter_nodes{role="snode",status="active"} 3` and `arbiter_nodes{role="verifier",status="active"} 3` (a registered member would be a fourth verifier), the same sealed tip and the safe watermark equal to it; every host `"quarantined":false` with the audit in repair mode and `unclean_tables 0`. A missed restart shows here as a node with `stage3: false`, and in the leader's refusal as `stage3 gate refused: data-plane nodes <ids> have not registered with this leader advertising stage3_v1; restart them on a stage-3 release`. `GetNodeFeatures` refuses while the Raft configuration holds a non-voter; inspect it first.

Residual risk: a second process configured with a voter's `node_id` defeats the voter-identity check, because the gate asks each node who it is. Confirm that nothing but the three voters runs `arbiter` with their ids.

### 7.2 Activate

Stop here and obtain explicit confirmation. The update is **irreversible**: once it commits, `stage3` can never be unset and each of its switches only turned on, the voters write v21 snapshots that `<ARBITER_TAG_P4>` refuses at the version byte, every later update carries `stage3`, a data-plane node without `stage3_v1` is refused at registration (`stage3 is active; this binary does not support stage-3 consensus`), and no voter, verifier, SNode or si-member may run an image older than this runbook's again. The rollbacks of §10.1 stop being a way back.

This update sets only `stage3`, with the parameters of `stage3-params.json` and every switch off. `--max-writers 3` and `--authority` restate the current values (both flags are mandatory); `--si-indexer` and `--verifier` are omitted, so the committed lists are carried, as are the table registry, the client lanes and `tracks_enabled`. Never combine it with another parameter change. Do not restart a voter, verifier or SNode while it runs. `--hold drain` takes the stage-2 drain hold the activation needs (K82 (iv)): the leader records each track's promotion frontier, finishes every block up to it, and the CLI signs with the frozen `promotion_seq` once the drain gate passes. The block stops before signing unless the committed authority set (compared lowercased) and `max_writers` in `show-before-activation.json` are exactly the values the update restates, `stage3` is null, the epoch is `<ENROLMENT_EPOCH>`, every track is unquarantined, every node of the leader's feature book advertises `stage3_v1` and none is a member (K82 (i), (ii)), every arbiter pod runs `<ARBITER_DIGEST_P5>` (K82 (iii)), `P4_END` is recorded (K82 (v)), every voter's `arbiter_state_bytes` is at most L/4 and the rehearsal's Persist peak at most L (K82 (vi)), the rehearsal passed and its Pod is gone.

Where it runs: inside the leader's own arbiter container (`si_arbiter`), naming each voter by its own in-cluster admin address, `--voter "$(si_voter <n>)"`, that is `storage-integrity-si-v2-arbiter-<n>.storage-integrity-si-v2-arbiter-raft.sentio-network-devnet2.svc.cluster.local:7080` for `n` = 0, 1 and 2. `arbiter-admin`'s gate probes each `--voter`, and the leader probes the same addresses itself, which the CLI sends it in the gRPC metadata `arbiter-stage3-voters`; the answering node ids must be exactly the Raft voters, each reporting `stage3_v1` (K82 (iii)). Never run it from a workstation through `kubectl port-forward`: a `localhost` address passes the CLI's probe but fails the leader's. Raft membership stays frozen from §7.1 until §7.3 has passed: the leader refuses while the configuration holds a non-voter, and a membership change in this window would add a voter the gate never probed.

Confirm before running.

```bash
[ ! -e "$si_backup/activate.out" ] || { echo "the activation ran already ($si_backup/activate.out): run si_show; never re-run this block"; exit 1; }
{ grep -qx 'rehearsal ok' "$si_backup/rehearsal.txt" && grep -q "^rehearsal Pod deleted at " "$si_backup/rehearsal-pod-deleted.txt"; } || { echo "the rehearsal has not passed or its Pod is not deleted (§3); stop"; exit 1; }
si_p5_value P4_END > /dev/null || { echo "P4_END is not recorded (§1.1); stop"; exit 1; }
live="$(si_images | awk '{sub(/.*@/, "", $3); print $3}' | sort -u)"
[ "$live" = "$(sed -n 2p "$si_backup/pins")" ] || { echo "not every arbiter pod runs ARBITER_DIGEST_P5 ($live); stop"; exit 1; }
for x in a b c; do [ "$(si_node_image "$x")" = "$(si_p5_image)" ] && [ "$(si_container_image "$(si_member_pod "$x")" si-member)" = "$(si_p5_image)" ] || { echo "indexer-$x or its si-member does not run the P5 image; stop"; exit 1; }; done
for i in 0 1 2; do
  b="$(si_state_size "$i" | si_series_value arbiter_state_bytes)"
  [ "$b" -le 134217728 ] || { echo "arbiter-$i's arbiter_state_bytes $b is above L/4 (134217728); stop"; exit 1; }
done
si_p5_py rehearsal "$si_backup/rehearsal.txt" "$(si_p5_value ENROLMENT_EPOCH)" 536870912 all
si_features_stage3 > "$si_backup/features-at-activation.json"
si_p5_py features "$si_backup/features-at-activation.json" "$(si_p5_owner c)"
si_show > "$si_backup/show-before-activation.json"
si_tracks > "$si_backup/tracks-at-activation.json"
si_p5_py activation "$si_backup/show-before-activation.json" "$si_backup/tracks-at-activation.json" "$(si_p5_value ENROLMENT_EPOCH)"
domains=()
while IFS=$'\t' read -r id dom; do domains+=(--indexer-failure-domain "id=$id,domain=$dom"); done < <(jq -r '.indexer_failure_domains[] | [.indexer_id, .failure_domain] | @tsv' "$si_backup/stage3-params.json")
[ "${#domains[@]}" = 6 ] || { echo "stage3-params.json does not hold three failure domains; stop"; exit 1; }
printf 'sealed %s promotion_seq %s\n' "$(si_sealed)" "$(jq -r .promotion_seq "$si_backup/show-before-activation.json")" | tee "$si_backup/activation-tip.txt"
for i in 0 1 2; do printf 'voter %s admin address %s\n' "$i" "$(si_voter "$i")"; done | tee "$si_backup/activation-voters.txt"
leader="$(si_leader)"
epoch="$(jq -r .epoch "$si_backup/show-before-activation.json")"
set +e
si_arbiter "$leader" /usr/local/bin/arbiter-admin consensus update --address "$(si_voter "$leader")" \
  --expected-epoch "$epoch" --authority 0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705 --max-writers 3 \
  --stage3 "${domains[@]}" --stage3-max-lag-publications 64 --stage3-lag-low 16 \
  --stage3-evidence-timeout-l2-blocks 300 --stage3-max-uncleaned-parts-per-partition 2000 \
  --stage3-max-uncleaned-parts-per-table 20000 \
  --voter "$(si_voter 0)" --voter "$(si_voter 1)" --voter "$(si_voter 2)" \
  --hold drain --hold-timeout 10m --timeout 20s 2> "$si_backup/activate.err" | tee "$si_backup/activate.out"
rc=$?
set -e
cat "$si_backup/activate.err"
[ "$rc" = 0 ] || { echo "the update did not report a commit (exit $rc): read activate.err; on 'commit outcome may be unknown' run si_show before anything else"; exit 1; }
```

`arbiter-admin` runs its stage-3 gate before signing (every `--voter` and every node of the leader's feature book advertises `stage3_v1`, except the SNode of a quarantined track, of which this runbook has none) and submits nothing on a refusal (`stage3 gate refused; nothing was submitted:`, then one problem per line). It then takes the drain hold and prints on stderr `took a consensus hold of mode drain on the leader (hold id <id>, expires <time>)`, any number of `waiting for the leader's drain hold: <progress>` lines and `the leader drained at promotion_seq <n>; signing the update`. The leader decides again before it proposes anything and refuses with `FailedPrecondition` `stage3 gate refused: …`, checking in this order: a non-voter in the Raft configuration; the FSM's own refusal (`stage3 activation requires the tracks activation`, or the stage-2 drain gate's text); a data-plane node that has not registered with it advertising `stage3_v1` (§7.1); its own voter probe (`stage3 gate refused: voter at <address> could not be probed: …`, or `voter <id> (<address>) lacks stage3_v1; restart it on a stage-3 release`). `InvalidArgument` means Apply refused the entry and names the first failing rule, for example a drain-gate text (`consensus update requires all promotions to be acknowledged`) when the chain moved between the hold and Apply. The CLI re-runs its gate right before it submits (`stage3 re-check before submit refused; the cluster changed since the gate ran; nothing was submitted:`), and a hold that is not ready within `--timeout` plus `--hold-timeout` ends with `the consensus hold was not ready within --timeout plus --hold-timeout (<progress>)` and `released consensus hold <id>`. None of these commits anything, not even the epoch: wait for the drain or for the change to settle, run `si_show`, move `activate.out` and `activate.err` aside (`mv "$si_backup/activate.out" "$si_backup/activate-refused-$(date -u +%Y%m%dT%H%M%SZ).out"; mv "$si_backup/activate.err" "$si_backup/activate-refused-$(date -u +%Y%m%dT%H%M%SZ).err"`) and run the block again, after its own confirmation. On "commit outcome may be unknown", run `si_show` before anything else: an epoch one above `show-before-activation.json`'s with `current.stage3` set means it committed.

After the JSON result the CLI runs its gate once more and may add one of three reports to `activate.err`, each of which means the update has committed: `info: the stage3 post-commit re-check differs from the gate observation, but every gate condition still holds: …` is information only; `WARNING: the stage3 update committed (irreversible), but the post-commit re-check could not run: …` asks for the checks by hand (`si_features_stage3`, `for i in 0 1 2; do si_capability "$i"; done`); `WARNING: the stage3 update committed (irreversible), but the post-commit re-check no longer holds: …` lists the nodes to restart on a P5 release; never downgrade one.

### 7.3 Verify

```bash
{
  si_show | tee "$si_backup/show-after-activation.json" | jq -c '{epoch, promotion_seq, max_writers: .current.max_writers, tracks_enabled: .current.tracks_enabled, stage3_state, si_indexers: [.current.si_indexers[] | {indexer_id, snode_node_id}], verifiers: [.current.verifiers[].node_id]}'
  si_p5_py stage3 "$si_backup/show-after-activation.json" "$si_backup/stage3-params.json" "$si_backup/activate.out" "$si_backup/activation-tip.txt"
  si_stage3
  jq -c '{status, committed_epoch}' "$si_backup/activate.out"
  si_tracks | jq -c '[.indexers[] | {id: .indexer_id, published: .published.safe_block_seq, quarantined}]'
  si_global
  for i in 0 1 2; do si_metrics "$i"; si_capability "$i"; done
  si_features_stage3
  for x in a b c; do si_info "$x"; done
} 2>&1 | tee "$si_backup/verify-after-activation.txt"
```

Expected: `activate.out` holds `"status": "committed"` and `"committed_epoch": "<ENROLMENT_EPOCH + 1>"` with the signed `update` carrying `stage3`; the epoch one above `show-before-activation.json`'s, `max_writers` `"3"`, `tracks_enabled` `true`, the three entries and the three verifiers unchanged; `stage3 committed at epoch <ENROLMENT_EPOCH + 1> as …/stage3-params.json states it, every switch off; activation block <n>, promotion <p> (tip before signing: sealed <n>, promotion <p>)` with the activation block and promotion equal to the tip (no writer ran, so nothing sealed in between); `si_stage3` with `group_size` `3`, `min_safe_copies` `1`, every switch `false` and the three failure domains; every track unquarantined; every voter with three active SNodes and three active verifiers; the feature book as in §7.1; every host `"quarantined":false`. Then indexer-a's acceptance write, which proves promotion and publication continue under `stage3`:

Confirm before running.

```bash
si_p5_accept - a 5404 7.3 2>&1 | tee "$si_backup/accept-a-5404.txt"
```

Expected: `acceptance pod phase: Succeeded`, `track 0 published <x> -> <y>` and `1`.

## 8. Monitoring and snapshot v21

The chart's rule group `storage-integrity-si-v2.arbiter-stage3` (stage 0, off) holds eight alerts, each with `runbook_url` `…/storage-integrity-devnet2-stage3-p5-v21.md#11-operating-rules`: `ArbiterSnapshotEncodingFallback` (critical: a v21 snapshot persisted plain because its interned body did not verify), `ArbiterBlockSealGroupSwitch`, `ArbiterVerifiersUnavailable`, `ArbiterCopiesBackpressure`, `ArbiterUnderReplicatedCleanups`, `ArbiterGroupsUnderReplicated`, `ArbiterFloorHeldCleanupAgeHigh` and `ArbiterMemberLagging`. It is turned on now, once every voter runs the P5 build and exports the encoding series; the other seven read series the switches of Plan S3-F create, so they stay silent until then. First the read-only checks:

```bash
{
  echo "vmalert groups of this network: $(si_vmalert_groups | wc -l | tr -d ' ')"
  si_vmalert_groups | jq -r .group | grep -c '^storage-integrity-si-v2\.arbiter-stage3$' || true
  si_vm_value 'count(arbiter_snapshot_encoding_fallback_total{network="devnet2-si-v2"})'
} 2>&1 | tee "$si_backup/monitoring-before.txt"
```

Expected: the earlier groups' count, `0` for the stage-3 group and `3` for the encoding counter (the voters' scrape of the stage-3 P0 runbook adds `network`). `none`: stop; the voters' series do not reach VictoriaMetrics.

The commit: `monitoring.stage3Consensus.enabled: true` in `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml` with its mirror `DEVNET2_STAGE3_CONSENSUS_MONITORING = True` in `test_render.py`, subject `chore(devnet2): stage-3 consensus alert rules on (P5)`. It changes only the VMRule (the monitoring values sit outside every checksum, so no pod moves):

Confirm before running.

```bash
[ ! -e "$si_backup/stage3-p5-monitoring-commit" ] || { echo "the monitoring commit was pushed already; never re-run this block"; exit 1; }
[ "$(jq -r .status "$si_backup/activate.out")" = committed ] || { echo "the activation has not committed (§7.2); stop"; exit 1; }
python3 -B charts/storage-integrity/tests/test_render.py
si_head_is charts/storage-integrity/tests/test_render.py k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml
si_render_diff_is storage-integrity-si-v2 "changed VMRule/storage-integrity-si-v2"
git rev-parse HEAD > "$si_backup/stage3-p5-monitoring-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
```

Then, read-only (the block waits up to five minutes for vmalert to load the group):

```bash
{
  for _ in $(seq 1 30); do si_vmalert_groups | jq -r .group | grep -qx 'storage-integrity-si-v2.arbiter-stage3' && break; sleep 10; done
  si_vmalert_groups | jq -c 'select(.group == "storage-integrity-si-v2.arbiter-stage3")' | tee "$si_backup/vmalert-stage3.txt"
  si_kubectl get vmrule storage-integrity-si-v2 -o json > "$si_backup/vmrule-live.json"
  python3 -I - "$si_backup/vmrule-live.json" > "$si_backup/vmrule-stage3-selectors.txt" <<'EOF'
import json, re, sys
seen = []
for group in json.load(open(sys.argv[1]))["spec"]["groups"]:
    if group["name"] != "storage-integrity-si-v2.arbiter-stage3":
        continue
    for rule in group["rules"]:
        for name, matchers in re.findall(r"([A-Za-z_:][A-Za-z0-9_:]*)\{([^}]*)\}", rule["expr"]):
            selector = "%s{%s}" % (name, matchers)
            if selector not in seen:
                seen.append(selector)
print("\n".join(seen))
EOF
  while read -r sel; do printf '%s %s\n' "$(si_vm_value "count($sel)" < /dev/null)" "$sel"; done < "$si_backup/vmrule-stage3-selectors.txt"
  si_vm_query 'ALERTS{network="devnet2-si-v2", alertname=~"ArbiterSnapshotEncodingFallback|ArbiterBlockSealGroupSwitch|ArbiterVerifiersUnavailable|ArbiterCopiesBackpressure|ArbiterUnderReplicatedCleanups|ArbiterGroupsUnderReplicated|ArbiterFloorHeldCleanupAgeHigh|ArbiterMemberLagging"}' | jq -c '.data.result[] | .metric'
} 2>&1 | tee "$si_backup/monitoring-after.txt"
```

Expected: one group `storage-integrity-si-v2.arbiter-stage3` with the eight rules, each `health` `ok` and an empty `lastError`; a count of `3` for `arbiter_snapshot_encoding_fallback_total{network="devnet2-si-v2"}`; for each other selector either a count or `none` (each reads series that appear with the switches of Plan S3-F, a counter at its first event, so `none` is the healthy answer now); no `ALERTS` series.

Then the v21 check of every voter: after its next Raft snapshot (the voters snapshot once `snapshot_threshold`, 8192 entries, have accumulated since their last one, checked every `snapshot_interval`, 120 s, so the first v21 snapshot may come hours or days after the activation), each voter's newest `state.bin` carries version byte 21 and body byte 1, `$R5/bin/new/v21check` restores it, the previous release refuses it, and no fallback was counted. The block is read-only; repeat it, at most hourly, until every voter has written a v21 snapshot:

```bash
{
  for i in 0 1 2; do si_v21check "$i"; si_encoding "$i"; done
  if [ -s "$si_backup/state-0.bin" ] && [ "$(od -An -tu1 -j4 -N1 "$si_backup/state-0.bin" | tr -d ' ')" = 21 ]; then
    "$R5/bin/old/v20check" "$si_backup/state-0.bin" || true
  fi
} 2>&1 | tee "$si_backup/v21-check-$(date -u +%Y%m%dT%H%M%SZ).txt"
```

Expected, once every voter has written one: `arbiter-<i> version 21 body 1` and `restore_ok version 21 body interned` for 0, 1 and 2, every `arbiter_snapshot_encoding_fallback_total 0`, `arbiter_snapshot_body_bytes{kind="interned"}` above 0 and below `{kind="plain"}`, and `restore_refused version 21: unsupported snapshot version 21 …` from the previous release. `version 20 body -` or `no snapshot yet`: that voter has not snapshotted since the activation; repeat later. `version 21 body 0` with a fallback above 0: the alert has fired; the snapshot is complete and readable (plain), so nothing is lost: §11 says what to do.

## 9. End

Once a §8 v21 check shows all three voters on version 21 with the interned body and no fallback, the end record, read-only:

```bash
{
  for i in 0 1 2; do
    line="$(si_v21check "$i" | tr '\n' ' ')"
    echo "$line"
    case "$line" in "arbiter-$i version 21 body 1 restore_ok version 21 body interned ") ;; *) echo "arbiter-$i has not written an interned v21 snapshot yet: repeat §8's check later"; exit 1 ;; esac
    [ "$(si_encoding "$i" | si_series_value arbiter_snapshot_encoding_fallback_total)" = 0 ] || { echo "arbiter-$i counted an encoding fallback: §11"; exit 1; }
  done
  si_stage3
  si_tracks | jq -c '[.indexers[] | {id: .indexer_id, published: .published.safe_block_seq, quarantined}]'
  si_global
  si_images
  for x in a b c; do printf 'indexer-%s %s member %s\n' "$x" "$(si_node_image "$x")" "$(si_container_image "$(si_member_pod "$x")" si-member)"; done
  grep '^P5_REHEARSAL=' "$si_backup/rehearsal.txt"
  printf 'ACTIVATION_EPOCH=%s\n' "$(jq -r .committed_epoch "$si_backup/activate.out")"
  printf 'P5_END=%s\n' "$(date -u +%Y-%m-%d)"
} 2>&1 | tee "$si_backup/p5-end.txt"
```

Expected: three `arbiter-<i> version 21 body 1 restore_ok version 21 body interned ` lines; `si_stage3` with every switch `false`; every track unquarantined and published; all six arbiter pods on `<ARBITER_DIGEST_P5>` and every host and member on the P5 image; `P5_REHEARSAL=<date>`, `ACTIVATION_EPOCH=<ENROLMENT_EPOCH + 1>` and `P5_END=<date>`. Plan S3-E4 Task 82 records `P5_REHEARSAL`, `ACTIVATION_EPOCH` and `P5_END` from this file (run record `P5 run and the v21 activation`) and adds the run's record at the end of this section in a docs-only PR. Plan S3-F starts from here.

## 10. Rollback

### 10.1 Before §7.2

Revert in reverse order of the commits: the si-members (member-a, member-c, member-b), the hosts (indexer-a, indexer-c, indexer-b), then the verifier commit, then replace the voters back. Until the activation the voters write v20 snapshots and no command carries a new field, so `<ARBITER_TAG_P4>` reads everything they wrote (§2.3 proved both ways), and the P4 sentio-node image serves every host as before. A step that has not run has no commit file, and `si_revert_stage3` then reverts nothing. Each revert and each voter replacement is confirmed on its own; each revert starts from the current `origin/main`, reverts one commit, runs the render tests, pushes, syncs that release and waits for its rollout (after a push it only syncs and waits again).

The si-members:

Confirm before running.

```bash
si_revert_stage3 stage3-p5-member-a-commit sentio-node-devnet2-indexer-a "statefulset/$(si_member_sts a)"
```

Confirm before running.

```bash
si_revert_stage3 stage3-p5-member-c-commit sentio-node-devnet2-indexer-c "statefulset/$(si_member_sts c)"
```

Confirm before running.

```bash
si_revert_stage3 stage3-p5-member-b-commit sentio-node-devnet2-indexer-b "statefulset/$(si_member_sts b)"
```

The hosts:

Confirm before running.

```bash
si_revert_stage3 stage3-p5-indexer-a-commit sentio-node-devnet2-indexer-a statefulset/sentio-node-devnet2-indexer-a
```

Confirm before running.

```bash
si_revert_stage3 stage3-p5-indexer-c-commit sentio-node-devnet2-indexer-c statefulset/sentio-node-devnet2-indexer-c
```

Confirm before running.

```bash
si_revert_stage3 stage3-p5-indexer-b-commit sentio-node-devnet2-indexer-b statefulset/sentio-node-devnet2-indexer-b
```

Then the verifier commit, which also takes the voters' template back to `<ARBITER_TAG_P4>`:

Confirm before running.

```bash
si_revert_stage3 stage3-p5-verifiers-commit storage-integrity-si-v2 statefulset/storage-integrity-si-v2-verifier
```

Then the voters, as in §6: the reverted pin is their template again, so each deleted voter returns on `<ARBITER_TAG_P4>`. First the order, read-only, in a file of its own (it never overwrites the file; remove `$si_backup/voter-order-rollback` only to choose a new order before the first of these deletions):

```bash
si_images
if [ -s "$si_backup/voter-order-rollback" ]; then echo "$si_backup/voter-order-rollback exists already, so the order stays:"; cat "$si_backup/voter-order-rollback"; echo "stop"; exit 1; fi
leader="$(si_leader)"
{ printf '0\n1\n2\n' | grep -vx "$leader"; echo "$leader"; } > "$si_backup/voter-order-rollback.new"
[ "$(sort "$si_backup/voter-order-rollback.new" | tr '\n' ' ')" = "0 1 2 " ] || { echo "no order of the three voters around leader '$leader'; stop"; exit 1; }
mv "$si_backup/voter-order-rollback.new" "$si_backup/voter-order-rollback"
cat "$si_backup/voter-order-rollback"
```

Skip the three replacements when `si_images` shows every voter on `<ARBITER_DIGEST_P4>` already (§6 never ran). The first follower:

Confirm before running.

```bash
si_replace_voter "$(sed -n 1p "$si_backup/voter-order-rollback")" 2>&1 | tee "$si_backup/rollback-voter-1.txt"
```

The second follower:

Confirm before running.

```bash
si_replace_voter "$(sed -n 2p "$si_backup/voter-order-rollback")" 2>&1 | tee "$si_backup/rollback-voter-2.txt"
```

The leader:

Confirm before running.

```bash
si_replace_voter "$(sed -n 3p "$si_backup/voter-order-rollback")" 2>&1 | tee "$si_backup/rollback-voter-3.txt"
```

Then, read-only:

```bash
{
  si_images
  for i in 0 1 2; do si_capability "$i"; si_v21check "$i"; done
  for x in a b c; do printf 'indexer-%s %s member %s\n' "$x" "$(si_node_image "$x")" "$(si_container_image "$(si_member_pod "$x")" si-member)"; done
} 2>&1 | tee "$si_backup/verify-rollback.txt"
```

Expected: the arbiter pods on `<ARBITER_DIGEST_P4>`, every capability with `features` `["client_lanes_v1","signed_claims_v1","track_manifests_v1"]`, every newest snapshot `version 20`, the hosts on `sha-<SN_COMMIT_P4>@<SN_DIGEST_P4>` and each member on its `member-images-before.txt` image (for the commits that were reverted). The rehearsal leaves nothing to undo: its Pod is gone (§3.6) and its copies are local.

### 10.2 After §7.2

There is no rollback. The committed `stage3` and the v21 snapshots cannot be undone (`consensus update must carry stage3 once set`); a data-plane node without `stage3_v1` is refused at registration; `<ARBITER_TAG_P4>` refuses every v21 snapshot. Fix forward. The monitoring commit alone can be reverted (`si_revert_stage3 stage3-p5-monitoring-commit storage-integrity-si-v2`, confirmed): it only stops the stage-3 alerts. The only lever on the format's encoding is the plain-body writer setting of §11.

## 11. Operating rules

These extend §11 of the stage-1, stage-2, stage-3 P0, lockdown and indexer-c runbooks, the P2 runbook's §14, the P3 runbook's §9, the hardening runbook's §13 and the owner-rebuild runbook's §8, which stay in force.

- From §7.2 on, never run a voter or verifier below `<ARBITER_TAG_P5>`, or an SNode or si-member below sentio-node `<SN_COMMIT_P5>`: `stage3` cannot be unset, a data-plane node without `stage3_v1` is refused at registration (`stage3 is active; this binary does not support stage-3 consensus`), and no older build reads v21. An SNode of a quarantined track that was not upgraded can never register again; upgrade it before releasing its track. Every later consensus update carries `stage3`; `arbiter-admin consensus update` carries it and the committed host-bound entries whether or not a `--stage3-*` flag is given, and restates `si_indexers` and `verifiers` from `consensus show` as before.
- Every switch stays off until Plan S3-F turns it on, one authority-signed update per switch in the order of spec §10.4, each needing explicit confirmation: `purge`, `eviction_voiding`, `owner_aware_selection`, member entries (not a switch), `placement`, member readiness (not a switch), `reselection`, `checkpoints`, `narrowed_drain`, then raising `min_safe_copies`. Never set `strict_copies` or raise `min_safe_copies` on devnet2 outside Plan S3-F (the FSM refuses both before `checkpoints` and `narrowed_drain`). Until `narrowed_drain`, `arbiter-admin`'s `--hold` stays `drain` (Plan S3-A K6); from then on it defaults to `issuance` for an update that touches no track (Plan S3-E4 K79).
- Snapshot encoding: every v21 snapshot is written with the interned body when its rehydration verifies against the plain document and with the plain body otherwise; every v21 binary reads both, and a fallback never cancels a snapshot. `ArbiterSnapshotEncodingFallback` (`increase(arbiter_snapshot_encoding_fallback_total[30m]) > 0`) fires on a fallback: the snapshot is complete, so nothing is lost, but the interner disagreed with the document, which is a bug to report with the voter's newest `state.bin` (`si_v21check <ordinal>` copies it) and its log. While it is investigated, or whenever the interned body must not be written, force the plain body: one single-value commit `arbiter.raft.snapshotBody: plain` in `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml` with its mirror `DEVNET2_SNAPSHOT_BODY = "plain"` in `test_render.py` (it renders `snapshot_body: plain` as the last key of the voters' `raft:` block; `si_render_diff storage-integrity-si-v2` must name `ConfigMap/storage-integrity-si-v2-arbiter-config` and no object other than it and `StatefulSet/storage-integrity-si-v2-arbiter`), confirmed, pushed and synced, then each voter replaced with `si_replace_voter` in a voter order of its own (followers first, leader last), because a voter reads the setting at start. It is node-local and never replicated, so it changes no consensus byte. Remove it (back to unset, which means `auto`) the same way once the cause is fixed. It never makes a voter write v20 again.
- State size: `arbiter_state_bytes` stays the length of the plain document under the interned body, so the state-size alerts and L = 512 MiB of the stage-3 P0 runbook's §13.1 keep their meaning; the interned body only shrinks what is written and read (`arbiter_snapshot_body_bytes`). The promotion-budget estimator of the stage-3 P0 runbook's §13.4 (`si_budget`) reads the 13-byte v20 envelope and a JSON document, so it fails on a v21 container (a body byte after the version byte, and an interned body); a budget after §7.2 needs a v21-aware reader of the snapshot store.
- The stage-3 alerts other than the encoding fallback (`ArbiterBlockSealGroupSwitch`, `ArbiterVerifiersUnavailable`, `ArbiterCopiesBackpressure`, `ArbiterUnderReplicatedCleanups`, `ArbiterGroupsUnderReplicated`, `ArbiterFloorHeldCleanupAgeHigh`, `ArbiterMemberLagging`) cannot fire while every switch is off: they read series of placement, owner-aware selection, strict mode, checkpoints and the copy floor. When one fires after Plan S3-F's switches, follow Plan S3-F's runbook (its operator playbook).
- Admission codes 12 (`VERIFIERS_UNAVAILABLE`) and 13 (`COPIES_BACKPRESSURE`) cannot occur before `owner_aware_selection` and strict mode; the hosts' embedded HouseGate answers both retryably since §5, while an older client or agent maps them to an unknown outcome.
- The si-members stay in P2/P3 mode, with `storage_integrity.member.consensus` absent, until Plan S3-F's step 4; no member is registered, and the member ids `member-a`, `member-b`, `member-c` stay allocated. Their `replica_signer` keys are made by the user in their own terminal (decision D40), never by an agent, and reach a host only as Secret-mounted files.
- After a leader change, the new leader's feature book is empty: before any gated update (the next is Plan S3-F's), restart the verifiers and SNodes so that they re-register with it, as §7.1 does.
- The rehearsal Pod never stays: an existing `storage-integrity-si-v2-p5-rehearsal` is deleted before anything else (§3.6), and it is never applied outside §3. Its copies under `$si_backup` hold live state; keep `$si_backup` at mode 700. `$R5` stays read-only for this runbook; only Plan S3-E4 Task 78 writes it.
- Restarts: a host restart gives its tables a short retryable `SOURCE_UNAVAILABLE` window and, in repair mode, a Pending (733) hold until its startup audit is clean; a member restart pauses its copies and ends its serving leases. Every SI ClickHouse restart still follows the P2 runbook's restart window; this runbook restarts none.
````

Then run Step 2's check again (local only): it now reads the runbook and fails only on the pointer Step 4 adds:

```bash
cd "$W" && python3 -I "$T/check_runbook.py" "$T/blocks" docs 2>&1 | tail -1
```

Expected: `the tracks runbook's §11 does not point to the P5 runbook`.

- [ ] **Step 4: The P5 pointer in the stage-2 runbook**

`docs/storage-integrity-devnet2-multi-source-tracks.md`, §11, the first bullet, which begins `- After §6.2, never run a voter, verifier or SNode below`: append one sentence after its last sentence, `… and no older build reads v20.` (Edit tool; match the bullet by its text). Against `origin/main` (`206c42357`) the change is:

```diff
diff --git a/docs/storage-integrity-devnet2-multi-source-tracks.md b/docs/storage-integrity-devnet2-multi-source-tracks.md
--- a/docs/storage-integrity-devnet2-multi-source-tracks.md
+++ b/docs/storage-integrity-devnet2-multi-source-tracks.md
@@ -1071,3 +1071,3 @@ These extend §11 of the stage-1 runbook, which stays in force except where note
 
-- After §6.2, never run a voter, verifier or SNode below `v0.12.0` / `5d2f1695e33d911cb642bf60b49b71cdb74f62a0`: tracks cannot be turned off, a data-plane node without `track_manifests_v1` is refused at registration, and no older build reads v20.
+- After §6.2, never run a voter, verifier or SNode below `v0.12.0` / `5d2f1695e33d911cb642bf60b49b71cdb74f62a0`: tracks cannot be turned off, a data-plane node without `track_manifests_v1` is refused at registration, and no older build reads v20. Since the stage-3 P5 activation ([storage-integrity-devnet2-stage3-p5-v21.md](storage-integrity-devnet2-stage3-p5-v21.md) §7.2, irreversible as well) the floor is that runbook's: its §11 names the arbiter and sentio-node releases below which no voter, verifier, SNode or si-member may run, because `stage3` cannot be unset and no older build reads v21.
 - After a leader change, registration reads the new leader's applied state, so until that leader has applied the activation's log entry a data-plane node without `track_manifests_v1` can still register with it. Only that node's own liveness suffers (it cannot read a track-aware manifest); never start one, and restart the verifiers and SNodes after a leader change as before.
```

Then (local only):

```bash
cd "$W" && grep -c 'Since the stage-3 P5 activation (\[storage-integrity-devnet2-stage3-p5-v21.md\](storage-integrity-devnet2-stage3-p5-v21.md) §7.2, irreversible as well)' docs/storage-integrity-devnet2-multi-source-tracks.md
git -C "$W" diff --numstat -- docs/storage-integrity-devnet2-multi-source-tracks.md
python3 -I "$T/check_runbook.py" "$T/blocks" docs
```

Expected: `1`; `1	1	docs/storage-integrity-devnet2-multi-source-tracks.md` (one line changed); then the check's three lines: `storage-integrity-devnet2-stage3-p5-v21.md: 33 headings as contract §57 lists them; one helper block, §0's first; one go block in §3.2; 59 bash blocks (30 confirmed, every write confirmed), 0 fish blocks; §7.2 stops first and carries the activation command verbatim; no hard-wrapped prose; bash -n (bash and /bin/bash) and fish --no-execute pass; 4 python programs compile`, `chain: <n> functions; 14 of this runbook's, none redefining an earlier one; every si_* name the blocks call is defined; <k> earlier helpers reached, none of the <m> that read another runbook's pins; the tracks runbook points here` (`<n>`, `<k>` and `<m>` follow the earlier runbooks as merged: 290, 61 and 52 with their plan texts of 2026-10-16; read `$T/blocks/earlier-helpers-called.txt` and `foreign-helpers.txt` if they differ) and `blocks in <T>/blocks`.

- [ ] **Step 5: Fill the PR-time values**

The fourteen ledger values of Step 1, the next epoch, and Task 78's rehearsal source with its checksum replace their placeholders. Write `$T/fill_runbook.py` with the Write tool, with exactly this content:

````python
"""Fill the P5 runbook's PR-time values (Plan S3-E4 Task 81 Step 5; local, never committed).

usage: python3 -I fill_runbook.py <runbook> <ledger NAME=value file> <p5rehearsal_test.go> <p5-files.sha256>

It replaces every PR-time placeholder of docs/storage-integrity-devnet2-stage3-p5-v21.md with its ledger
value (<ENROLMENT_EPOCH + 1> with the next epoch), the go block's single line <P5REHEARSAL_SOURCE> with
the rehearsal's source as Plan S3-E4 Task 78 left it in $R5, and <P5REHEARSAL_SHA256> with that file's
SHA-256, which must be the p5rehearsal/p5rehearsal_test.go line of $R5/p5-files.sha256. It refuses a
source that does not end in exactly one newline or holds a line starting with three backquotes, a value
that is missing, and a placeholder left afterwards.
"""
import hashlib
import re
import sys

NAMES = ["ARBITER_TAG_P5", "ARBITER_DIGEST_P5", "SN_COMMIT_P5", "SN_DIGEST_P5", "ARBITER_TAG_P4", "ARBITER_DIGEST_P4",
         "SN_COMMIT_P4", "SN_DIGEST_P4", "CORE_TAG_P5", "HG_TAG_P5", "PROTO_TAG_P5", "INDEXER_C_ID", "ENROLMENT_EPOCH",
         "DEVNET2_FAILURE_DOMAINS"]
LEFT = re.compile(r"<(" + "|".join(NAMES) + r"|ENROLMENT_EPOCH \+ 1|P5REHEARSAL_SHA256|P5REHEARSAL_SOURCE)>")


def main(runbook, ledger, source, sums):
    values = {}
    for line in open(ledger):
        name, sep, value = line.rstrip("\n").partition("=")
        if sep:
            values[name] = value
    missing = [n for n in NAMES if not values.get(n)]
    if missing:
        sys.exit(f"the ledger file lacks {missing}")
    code = open(source, "rb").read()
    if not code.endswith(b"\n") or code.endswith(b"\n\n"):
        sys.exit(f"{source} must end in exactly one newline")
    text_code = code.decode("utf-8")
    if any(l.startswith("```") for l in text_code.splitlines()):
        sys.exit(f"{source} holds a line starting with three backquotes; it cannot sit in a fenced block")
    digest = hashlib.sha256(code).hexdigest()
    if f"{digest}  p5rehearsal/p5rehearsal_test.go" not in open(sums).read().splitlines():
        sys.exit(f"{source}'s SHA-256 {digest} is not the p5rehearsal/p5rehearsal_test.go line of {sums}")
    text = open(runbook, encoding="utf-8", newline="").read()
    if text.count("```go\n<P5REHEARSAL_SOURCE>\n```\n") != 1:
        sys.exit("the runbook does not hold the go block with the single line <P5REHEARSAL_SOURCE>")
    text = text.replace("<P5REHEARSAL_SHA256>", digest)
    text = text.replace("<ENROLMENT_EPOCH + 1>", str(int(values["ENROLMENT_EPOCH"]) + 1))
    for n in NAMES:
        text = text.replace(f"<{n}>", values[n])
    left = sorted(set(LEFT.findall(text)) - {"P5REHEARSAL_SOURCE"})
    if left:
        sys.exit(f"placeholders left: {left}")
    # The source goes in last, so no value replacement ever touches it.
    text = text.replace("```go\n<P5REHEARSAL_SOURCE>\n```\n", "```go\n" + text_code + "```\n")
    open(runbook, "w", encoding="utf-8", newline="").write(text)
    print(f"filled {runbook}: {len(NAMES)} ledger values, the next epoch {int(values['ENROLMENT_EPOCH']) + 1}, the rehearsal source ({len(text_code.splitlines())} lines, sha256 {digest})")


main(*sys.argv[1:])
````

Run it (local only; it reads `$R5` and writes only the runbook):

```bash
cd "$W" && R5="$HOME/si-devnet2-multisource-s3e-rehearsal"
python3 -I "$T/fill_runbook.py" docs/storage-integrity-devnet2-stage3-p5-v21.md "$T/ledger.txt" "$R5/p5rehearsal/p5rehearsal_test.go" "$R5/p5-files.sha256"
v() { sed -n "s/^$1=//p" "$T/ledger.txt"; }
grep -c "^ARBITER_TAG_P5='$(v ARBITER_TAG_P5)'; ARBITER_DIGEST_P5='$(v ARBITER_DIGEST_P5)'; SN_COMMIT_P5='$(v SN_COMMIT_P5)'; SN_DIGEST_P5='$(v SN_DIGEST_P5)'$" docs/storage-integrity-devnet2-stage3-p5-v21.md
grep -c "^INDEXER_C_ID='$(v INDEXER_C_ID)'; ENROLMENT_EPOCH='$(v ENROLMENT_EPOCH)'; DEVNET2_FAILURE_DOMAINS='$(v DEVNET2_FAILURE_DOMAINS)'$" docs/storage-integrity-devnet2-stage3-p5-v21.md
grep -cF "[ \"\$sum\" = '$(sed -n 's/^\([0-9a-f]*\)  p5rehearsal\/p5rehearsal_test.go$/\1/p' "$R5/p5-files.sha256")' ]" docs/storage-integrity-devnet2-stage3-p5-v21.md
python3 -I "$T/check_runbook.py" --filled "$T/blocks" docs | head -2
```

Expected: `filled docs/storage-integrity-devnet2-stage3-p5-v21.md: 14 ledger values, the next epoch <ENROLMENT_EPOCH + 1>, the rehearsal source (<lines> lines, sha256 <64 hex>)`, then `1`, `1` and `1` (the two pins lines of §1.1 and §3.2's checksum test), then the check's two summary lines as in Step 4 (`--filled` adds the placeholder test). A refusal from the fill (a source that is not `p5-files.sha256`'s, a missing value) writes nothing: stop and ask.

- [ ] **Step 6: Verify**

First the self-test. `fixtures.bash` builds stand-ins under `$T/fx` only: a rehearsal directory with stand-in tools that print the real tools' contract lines, a linux/amd64 stand-in binary and a synthetic v21 fixture, with the real rehearsal source copied from `$R5` (which it only reads), and a tree holding copies of the filled runbook, Task 80's Pod example and the constants of this checkout's `test_render.py`. `selftest.bash` sets `HOME` to the stand-in home (so `$si_backup` and `$R5` point into `$T/fx`), sources the chain the check wrote, replaces the cluster commands with stubs that answer from files, and runs every check helper of §0 with its refusals, §1.1's constants check, §2.1's and §3.2's read-only blocks, §7.2's flag loop and the rehearsal's in-pod filter. Write `$T/fixtures.bash` with the Write tool, with exactly this content:

````bash
# Stand-in inputs of the P5 runbook's self-test (Plan S3-E4 Task 81 Step 6; local, never committed).
# usage: bash fixtures.bash <production worktree> <ledger NAME=value file> <real $R5> <fixture dir>
# It reads the real $R5 (only its p5rehearsal/p5rehearsal_test.go, which the filled runbook carries) and the
# worktree (the filled runbook, Task 80's Pod example and test_render.py's constants) and writes, under the
# fixture dir only: home/ with a stand-in rehearsal directory (stand-in tools that print what the real ones
# print, a linux/amd64 stand-in binary, a synthetic v21 fixture), tree/ with copies of the runbook and the Pod
# example and a constants-only test_render.py, and ledger.txt.
set -eo pipefail
WT="$1"; LEDGER="$2"; REAL_R5="$3"; FX="$4"
case "$FX" in "$HOME"/.cache/*) ;; *) echo "the fixture dir must live under \$HOME/.cache" >&2; exit 1 ;; esac
rm -rf "$FX"
R5="$FX/home/si-devnet2-multisource-s3e-rehearsal"
mkdir -p "$R5/bin/old" "$R5/bin/new" "$R5/p5rehearsal" "$FX/tree/docs/examples" "$FX/tree/charts/storage-integrity/tests" "$FX/standin"
chmod 700 "$R5"
v() { sed -n "s/^$1=//p" "$LEDGER"; }
cp "$REAL_R5/p5rehearsal/p5rehearsal_test.go" "$R5/p5rehearsal/p5rehearsal_test.go"
for t in statedigest snapdump; do for b in old new; do printf '#!/bin/sh\necho "stand-in %s %s"\n' "$t" "$b" > "$R5/bin/$b/$t"; chmod 755 "$R5/bin/$b/$t"; done; done
cat > "$R5/bin/old/v20check" <<'EOF'
#!/bin/sh
# stand-in of the S3-A v20check built from ARBITER_TAG_P4: refuses every version above 20 at its version byte
v=$(od -An -tu1 -j4 -N1 "$1" | tr -d ' ')
if [ "$v" -le 20 ]; then echo "restore_ok version $v"; else echo "restore_refused version $v: unsupported snapshot version $v (require 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 or 20); pre-v3 state requires a backup and coordinated re-genesis"; exit 1; fi
EOF
cat > "$R5/bin/new/v21check" <<'EOF'
#!/bin/sh
# stand-in of the S3-E2 v21check built from ARBITER_TAG_P5
v=$(od -An -tu1 -j4 -N1 "$1" | tr -d ' ')
case "$v" in
  21) k=$(od -An -tu1 -j5 -N1 "$1" | tr -d ' '); case "$k" in 0) echo "restore_ok version 21 body plain" ;; 1) echo "restore_ok version 21 body interned" ;; *) echo "restore_refused version 21: unsupported snapshot v21 body kind $k"; exit 1 ;; esac ;;
  *) echo "restore_ok version $v body none" ;;
esac
EOF
chmod 755 "$R5/bin/old/v20check" "$R5/bin/new/v21check"
printf 'package main\n\nimport "fmt"\n\nfunc main() { fmt.Println("stand-in") }\n' > "$FX/standin/main.go"
printf 'module standin\n\ngo 1.22\n' > "$FX/standin/go.mod"
(cd "$FX/standin" && GOWORK=off GOFLAGS=-mod=mod GOPROXY=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$R5/bin/new/p5rehearsal" .)
python3 -I - "$R5/v21-fixture.container" <<'EOF'
import struct, sys
doc = b'{"format":"arbiter-snapshot-interned-v1"}'
open(sys.argv[1], "wb").write(b"AFSM" + bytes([21, 1]) + struct.pack(">Q", len(doc)) + doc + b"\x00" * 8)
EOF
v ARBITER_TAG_P4 > "$R5/old_tag.txt"
v ARBITER_TAG_P5 > "$R5/new_tag.txt"
printf '%s\n%s\n' "$(v ARBITER_TAG_P5)" "$(v ARBITER_DIGEST_P5)" > "$R5/arbiter-release"
(cd "$R5" && shasum -a 256 p5rehearsal/p5rehearsal_test.go bin/old/statedigest bin/old/v20check bin/old/snapdump bin/new/statedigest bin/new/v21check bin/new/snapdump bin/new/p5rehearsal > p5-files.sha256)
cp "$WT/docs/storage-integrity-devnet2-stage3-p5-v21.md" "$FX/tree/docs/"
cp "$WT/docs/examples/storage-integrity-p5-rehearsal-pod.yaml" "$FX/tree/docs/examples/"
(cd "$WT" && python3 -I -B - "$FX/tree/charts/storage-integrity/tests/test_render.py" <<'EOF'
import sys
sys.path.insert(0, "charts/storage-integrity/tests")
import test_render as t
names = ["DEVNET2_STAGE3_P5_ARBITER_DIGEST", "DEVNET2_STAGE3_P5_SOURCE_IMAGE", "STAGE3_P5_HOST_IMAGES", "WRITER_AUTHORIZATION_IMAGES",
         "STAGE3_HOST_IMAGES", "MEMBER_IMAGES", "DEVNET2_ARBITER_DIGEST", "INDEXER_B_SOURCE_IMAGE", "INDEXER_C_SOURCE_IMAGE",
         "FRESH_SOURCE_IMAGE", "DEVNET2_SNAPSHOT_BODY", "DEVNET2_STAGE3_CONSENSUS_MONITORING", "P5_REHEARSAL_POD", "DEVNET2_STATE_LIMIT_BYTES"]
with open(sys.argv[1], "w") as f:
    f.write("# The constants of this checkout's test_render.py that runbook §1.1 reads (self-test copy).\n")
    for n in names:
        f.write(f"{n} = {getattr(t, n)!r}\n")
print(f"constants copied: {len(names)}")
EOF
)
cp "$LEDGER" "$FX/ledger.txt"
echo "fixtures in $FX"
````

Write `$T/selftest.bash` with the Write tool, with exactly this content:

````bash
# Self-test of the P5 runbook's helpers and local blocks (Plan S3-E4 Task 81 Step 6; local, never committed).
# usage: bash selftest.bash <blocks dir from check_runbook.py> <fixture dir>
# The fixture dir holds home/ (a stand-in $R5 under it), tree/ (a stand-in checkout with the filled runbook,
# the Pod example and test_render.py's P5 constants) and ledger.txt. Cluster commands are replaced by stubs
# that answer from files under the fixture dir; nothing reaches a cluster, a registry or GitHub.
set -eo pipefail
B="$1"; FX="$2"
export HOME="$FX/home"
pass=0
ok() { pass=$((pass + 1)); echo "PASS $*"; }
die() { echo "FAIL $*"; exit 1; }
expect_fail() { # $1 label, then a command that must fail
  local label="$1"; shift
  if ( "$@" ) > "$FX/out.txt" 2>&1; then cat "$FX/out.txt"; die "$label: succeeded"; fi
  ok "$label: refused ($(tail -n 1 "$FX/out.txt" | cut -c1-120))"
}
blk() { grep -l -F -- "$1" "$B"/p5-*-r.bash | head -n 1; } # the read-only block that holds $1
pyblk() { grep -l -F -- "$1" "$B"/py/*.py | head -n 1; } # the embedded python program that holds $1
# shellcheck disable=SC1091
. "$B/helpers.sh"
[ "$si_backup" = "$HOME/si-devnet2-stage3-p5" ] && [ "$R5" = "$HOME/si-devnet2-multisource-s3e-rehearsal" ] || die "si_backup or R5"
ok "chain sourced under $BASH_VERSION: $(declare -F | wc -l | tr -d ' ') functions"
for f in si_p5_image si_p5_value si_p5_owner si_stage3 si_features_stage3 si_v21check si_encoding si_rehearsal si_member_sts si_p5_member_keys si_p5_keeper_findings si_p5_put si_p5_accept si_p5_py; do declare -F "$f" > /dev/null || die "$f undefined"; done
while read -r f; do declare -F "$f" > /dev/null || die "earlier helper $f undefined"; done < "$B/earlier-helpers-called.txt"
ok "every helper of this runbook and every earlier helper it reaches is defined"

rm -rf "$HOME/si-devnet2-stage3-p5"; mkdir -m 700 "$HOME/si-devnet2-stage3-p5"
cp "$FX/ledger.txt" "$si_backup/ledger-values.txt"
for n in ARBITER_TAG_P5 ARBITER_DIGEST_P5 SN_COMMIT_P5 SN_DIGEST_P5 ARBITER_TAG_P4 ARBITER_DIGEST_P4 SN_COMMIT_P4 SN_DIGEST_P4; do sed -n "s/^$n=//p" "$FX/ledger.txt"; done > "$si_backup/pins"
[ "$(si_p5_image)" = "ghcr.io/sentioxyz/sentio-node:sha-$(sed -n 3p "$si_backup/pins")@$(sed -n 4p "$si_backup/pins")" ] || die si_p5_image
[ "$(si_p5_owner c)" = "$(sed -n 's/^INDEXER_C_ID=//p' "$FX/ledger.txt")" ] && [ "$(si_p5_owner a)" = 0 ] && [ "$(si_p5_owner b)" = 1 ] && [ "$(si_p5_value MEMBER_IDS)" = "$(sed -n 's/^MEMBER_IDS=//p' "$FX/ledger.txt")" ] || die "si_p5_owner or si_p5_value"
expect_fail "si_p5_value of an unread name" si_p5_value NOPE
expect_fail "si_p5_owner d" si_p5_owner d
[ "$(si_member_sts c)" = sentio-node-devnet2-indexer-c-si-member ] || die si_member_sts
ok "pins, ledger values, owners and member StatefulSets"

# §1.1's python against the stand-in test_render.py, then with the P5 image missing from MEMBER_IMAGES.
cd "$FX/tree"
v() { sed -n "s/^$1=//p" "$FX/ledger.txt"; }
py11="$(pyblk 'test_render.py reviews the P5 images')"
python3 -I -B "$py11" "$(v ARBITER_DIGEST_P5)" "$(v SN_COMMIT_P5)" "$(v SN_DIGEST_P5)" "$(v ARBITER_DIGEST_P4)" "$(v SN_COMMIT_P4)" "$(v SN_DIGEST_P4)" | grep -q '^test_render.py reviews the P5 images' || die "§1.1 python"
cp charts/storage-integrity/tests/test_render.py "$FX/test_render.orig"
sed -i '' 's/^MEMBER_IMAGES = .*/MEMBER_IMAGES = frozenset()/' charts/storage-integrity/tests/test_render.py
expect_fail "§1.1 python with the P5 image not in MEMBER_IMAGES" python3 -I -B "$py11" "$(v ARBITER_DIGEST_P5)" "$(v SN_COMMIT_P5)" "$(v SN_DIGEST_P5)" "$(v ARBITER_DIGEST_P4)" "$(v SN_COMMIT_P4)" "$(v SN_DIGEST_P4)"
cp "$FX/test_render.orig" charts/storage-integrity/tests/test_render.py
ok "§1.1's test_render check"

# §1.2: the parameters.
si_p5_py params "0=node:sea-g-5,1=node:sea-g-6,2=node:sea-g-7" 2 "$si_backup/stage3-params.json" > "$FX/out.txt"
python3 -I - "$si_backup/stage3-params.json" <<'EOF' || die "stage3-params.json"
import json, sys
p = json.load(open(sys.argv[1]))
assert list(p)[:5] == ["group_size", "network_fill_default", "min_safe_copies", "strict_copies", "indexer_failure_domains"], list(p)
assert p["indexer_failure_domains"] == [{"indexer_id": 0, "failure_domain": "node:sea-g-5"}, {"indexer_id": 1, "failure_domain": "node:sea-g-6"}, {"indexer_id": 2, "failure_domain": "node:sea-g-7"}]
assert p["evidence_timeout_l2_blocks"] == 300 and p["max_uncleaned_parts_per_partition"] == 2000 and p["max_uncleaned_parts_per_table"] == 20000
assert p["max_lag_publications"] == 64 and p["lag_low"] == 16 and p["group_size"] == 3 and p["min_safe_copies"] == 1
assert p["network_fill_default"] is True and not any(p[k] for k in ("strict_copies", "exclude_quarantined_hosts", "purge", "eviction_voiding", "owner_aware_selection", "placement", "reselection", "checkpoints", "narrowed_drain"))
assert len(p) == 18
EOF
expect_fail "params with two indexers on one node" si_p5_py params "0=node:sea-g-5,1=node:sea-g-5,2=node:sea-g-7" 2 "$FX/p.json"
expect_fail "params without indexer-c" si_p5_py params "0=node:sea-g-5,1=node:sea-g-6" 2 "$FX/p.json"
expect_fail "params out of order" si_p5_py params "1=node:sea-g-6,0=node:sea-g-5,2=node:sea-g-7" 2 "$FX/p.json"
expect_fail "params with a domain that is not node:<hostname>" si_p5_py params "0=sea-g-5,1=node:sea-g-6,2=node:sea-g-7" 2 "$FX/p.json"
ok "stage3-params.json: Stage3Params as arbiter-admin's flags set it, 18 fields, every switch off"

# §7.2: the failure-domain flags from the parameters file (the block's own loop).
domains=()
while IFS=$'\t' read -r id dom; do domains+=(--indexer-failure-domain "id=$id,domain=$dom"); done < <(jq -r '.indexer_failure_domains[] | [.indexer_id, .failure_domain] | @tsv' "$si_backup/stage3-params.json")
[ "${#domains[@]}" = 6 ] && [ "${domains[1]}" = "id=0,domain=node:sea-g-5" ] && [ "${domains[5]}" = "id=2,domain=node:sea-g-7" ] || die "domains: ${domains[*]}"
ok "§7.2's --indexer-failure-domain flags: ${domains[*]}"

# §7.2 and §7.3: the show, track, feature-book and stage3 checks.
python3 -I - "$FX" <<'EOF'
import json, sys
fx = sys.argv[1]
show = {"epoch": "6", "promotion_seq": "140", "current": {"authority_addresses": ["0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705"], "max_writers": "3", "tracks_enabled": True, "stage3": None}, "stage3_state": None}
json.dump(show, open(f"{fx}/show-before.json", "w"))
tracks = {"indexers": [{"indexer_id": "0", "quarantined": False}, {"indexer_id": "1", "quarantined": False}, {"indexer_id": "2", "quarantined": False}]}
json.dump(tracks, open(f"{fx}/tracks.json", "w"))
tracks["indexers"][2]["quarantined"] = True
json.dump(tracks, open(f"{fx}/tracks-q.json", "w"))
bad = json.loads(json.dumps(show)); bad["current"]["max_writers"] = "2"; json.dump(bad, open(f"{fx}/show-mw.json", "w"))
bad = json.loads(json.dumps(show)); bad["current"]["stage3"] = {"group_size": 3}; json.dump(bad, open(f"{fx}/show-s3.json", "w"))
voters = ["storage-integrity-si-v2-arbiter-1", "storage-integrity-si-v2-arbiter-0", "storage-integrity-si-v2-arbiter-2"]
nodes = [{"id": n, "stage3": True} for n in ("snode-1", "snode-2", "snode-3", "verifier-1", "verifier-2", "verifier-3")]
open(f"{fx}/features.json", "w").write(json.dumps({"voters": voters, "nodes": nodes}) + "\n")
open(f"{fx}/features-member.json", "w").write(json.dumps({"voters": voters, "nodes": nodes + [{"id": "member-b", "stage3": True}]}) + "\n")
nodes2 = [dict(n) for n in nodes]; nodes2[2]["stage3"] = False
open(f"{fx}/features-old.json", "w").write(json.dumps({"voters": voters, "nodes": nodes2}) + "\n")
open(f"{fx}/features-missing.json", "w").write(json.dumps({"voters": voters, "nodes": nodes[1:]}) + "\n")
after = {"epoch": "7", "promotion_seq": "140", "current": {"stage3": {"group_size": 3, "network_fill_default": True, "min_safe_copies": 1, "strict_copies": False,
         "indexer_failure_domains": [{"indexer_id": "0", "failure_domain": "node:sea-g-5"}, {"indexer_id": "1", "failure_domain": "node:sea-g-6"}, {"indexer_id": "2", "failure_domain": "node:sea-g-7"}],
         "max_lag_publications": 64, "lag_low": 16, "evidence_timeout_l2_blocks": "300", "max_uncleaned_parts_per_partition": 2000, "max_uncleaned_parts_per_table": 20000,
         "exclude_quarantined_hosts": False, "purge": False, "eviction_voiding": False, "owner_aware_selection": False, "placement": False, "reselection": False, "checkpoints": False, "narrowed_drain": False}},
         "stage3_state": {"activation_epoch": "7", "activation_block_seq": "212", "activation_promotion_seq": "140"}}
json.dump(after, open(f"{fx}/show-after.json", "w"))
bad = json.loads(json.dumps(after)); bad["current"]["stage3"]["purge"] = True; json.dump(bad, open(f"{fx}/show-after-purge.json", "w"))
bad = json.loads(json.dumps(after)); bad["current"]["stage3"]["evidence_timeout_l2_blocks"] = "30"; json.dump(bad, open(f"{fx}/show-after-timeout.json", "w"))
json.dump({"status": "committed", "committed_epoch": "7", "update": {"stage3": {}}}, open(f"{fx}/activate.out", "w"), indent=2)
open(f"{fx}/tip.txt", "w").write("sealed 212 promotion_seq 140\n")
EOF
si_p5_py activation "$FX/show-before.json" "$FX/tracks.json" 6 | grep -q '^epoch 6, promotion_seq 140' || die "activation check"
expect_fail "activation check with max_writers 2" si_p5_py activation "$FX/show-mw.json" "$FX/tracks.json" 6
expect_fail "activation check with stage3 set" si_p5_py activation "$FX/show-s3.json" "$FX/tracks.json" 6
expect_fail "activation check at another epoch" si_p5_py activation "$FX/show-before.json" "$FX/tracks.json" 5
expect_fail "activation check with a quarantined track" si_p5_py activation "$FX/show-before.json" "$FX/tracks-q.json" 6
si_p5_py features "$FX/features.json" 2 | grep -q '^feature book: ' || die "features"
expect_fail "feature book with a member" si_p5_py features "$FX/features-member.json" 2
expect_fail "feature book with an SNode without stage3_v1" si_p5_py features "$FX/features-old.json" 2
expect_fail "feature book without snode-1" si_p5_py features "$FX/features-missing.json" 2
si_p5_py stage3 "$FX/show-after.json" "$si_backup/stage3-params.json" "$FX/activate.out" "$FX/tip.txt" | grep -q '^stage3 committed at epoch 7 ' || die "stage3 check"
expect_fail "stage3 check with a switch on" si_p5_py stage3 "$FX/show-after-purge.json" "$si_backup/stage3-params.json" "$FX/activate.out" "$FX/tip.txt"
expect_fail "stage3 check with another timeout" si_p5_py stage3 "$FX/show-after-timeout.json" "$si_backup/stage3-params.json" "$FX/activate.out" "$FX/tip.txt"
ok "the activation, feature-book and stage3 checks"

# §3.3, §3.4: the rehearsal's result lines and the in-pod filter.
cat > "$FX/rehearsal.log" <<'EOF'
=== RUN   TestP5Rehearsal
voter 0 restored epoch=6 promotion_seq=140 drain_refusal=""
voter 1 restored epoch=6 promotion_seq=140 drain_refusal=""
voter 2 restored epoch=6 promotion_seq=140 drain_refusal=""
digest 0 1111111111111111111111111111111111111111111111111111111111111111
digest 1 1111111111111111111111111111111111111111111111111111111111111111
digest 2 1111111111111111111111111111111111111111111111111111111111111111
    p5rehearsal_test.go:88: signed eyJhbGciOiJFUzI1NksifQ.eyJ0ZXN0Ijp0cnVlfQ.c2ln
activation applied epoch=7
persist 0 version 21 body interned bytes=1500000 fallback=0 persist_peak_heap_bytes=91000000
persist 1 version 21 body interned bytes=1500000 fallback=0 persist_peak_heap_bytes=92000000
persist 2 version 21 body interned bytes=1500000 fallback=0 persist_peak_heap_bytes=90000000
restart 0 restore_ok version 21 body interned digest_equal=true
restart 1 restore_ok version 21 body interned digest_equal=true
restart 2 restore_ok version 21 body interned digest_equal=true
mismatch 0 version 21 body plain fallback=1 digest_equal=true
mismatch 1 version 21 body plain fallback=1 digest_equal=true
mismatch 2 version 21 body plain fallback=1 digest_equal=true
rehearsal ok
--- PASS: TestP5Rehearsal (12.31s)
PASS
exit 0
EOF
grep -E '^(voter [0-2] restored |digest [0-2] |activation applied |persist [0-2] |restart [0-2] |mismatch [0-2] |rehearsal ok$|--- (PASS|FAIL): TestP5Rehearsal |exit [0-9]+$|[[:space:]]+p5rehearsal_test\.go:[0-9]+: )' "$FX/rehearsal.log" | grep -v 'eyJ' > "$si_backup/rehearsal.txt"
if grep -qx 'rehearsal ok' "$si_backup/rehearsal.txt" && grep -qx 'exit 0' "$si_backup/rehearsal.txt"; then echo "P5_REHEARSAL=$(date -u +%Y-%m-%d)" >> "$si_backup/rehearsal.txt"; fi
grep -q 'eyJ' "$si_backup/rehearsal.txt" && die "the filter let a JWS through"
[ "$(wc -l < "$si_backup/rehearsal.txt" | tr -d ' ')" = 20 ] || die "rehearsal.txt holds $(wc -l < "$si_backup/rehearsal.txt") lines"
si_p5_py rehearsal "$si_backup/rehearsal.txt" 6 536870912 activation | grep -q 'Persist peak 92000000 bytes' || die "rehearsal activation"
si_p5_py rehearsal "$si_backup/rehearsal.txt" 6 536870912 mismatch | grep -q '^rehearsal forced mismatch ok' || die "rehearsal mismatch"
si_p5_py rehearsal "$si_backup/rehearsal.txt" 6 536870912 all > /dev/null || die "rehearsal all"
sed 's/drain_refusal=""/drain_refusal=consensus update requires all promotions to be acknowledged/' "$si_backup/rehearsal.txt" > "$FX/r-drain.txt"
expect_fail "rehearsal with a copy that does not drain" si_p5_py rehearsal "$FX/r-drain.txt" 6 536870912 activation
sed 's/^digest 2 1111/digest 2 2222/' "$si_backup/rehearsal.txt" > "$FX/r-digest.txt"
expect_fail "rehearsal with two digests" si_p5_py rehearsal "$FX/r-digest.txt" 6 536870912 activation
expect_fail "rehearsal with a Persist peak above L" si_p5_py rehearsal "$si_backup/rehearsal.txt" 6 50000000 activation
expect_fail "rehearsal at another epoch" si_p5_py rehearsal "$si_backup/rehearsal.txt" 5 536870912 activation
grep -v '^mismatch 1 ' "$si_backup/rehearsal.txt" > "$FX/r-mm.txt"
expect_fail "rehearsal without voter 1's forced mismatch" si_p5_py rehearsal "$FX/r-mm.txt" 6 536870912 mismatch
grep -v '^P5_REHEARSAL=' "$si_backup/rehearsal.txt" > "$FX/r-nodate.txt"
expect_fail "rehearsal without its date" si_p5_py rehearsal "$FX/r-nodate.txt" 6 536870912 mismatch
cp "$si_backup/rehearsal.txt" "$FX/r-jws.txt"; echo 'leak eyJhbGciOi.x.y' >> "$FX/r-jws.txt"
expect_fail "rehearsal.txt holding a JWS" si_p5_py rehearsal "$FX/r-jws.txt" 6 536870912 all
ok "the rehearsal's in-pod filter and result checks"

# §3.5: the 14-byte header.
head -c 14 "$R5/v21-fixture.container" > "$FX/header.bin"
si_p5_py header "$FX/header.bin" | grep -q '^header AFSM version 21 body 1 doc_length [0-9][0-9]*$' || die "header"
out="$("$R5/bin/old/v20check" "$FX/header.bin" || true)"
case "$out" in "restore_refused version 21: unsupported snapshot version 21 "*) ;; *) die "the previous release on the header: $out" ;; esac
python3 -I -c 'import struct, sys; open(sys.argv[1], "wb").write(b"AFSM" + bytes([20]) + struct.pack(">Q", 2) + b"{}" )' "$FX/v20.bin"
head -c 14 "$FX/v20.bin" > "$FX/header20.bin"
expect_fail "header of a v20 container" si_p5_py header "$FX/header20.bin"
ok "§3.5's header check"

# §3.2: the Pod example and a live Pod.
si_p5_py pod docs/examples/storage-integrity-p5-rehearsal-pod.yaml "$(sed -n 2p "$si_backup/pins")" | grep -q 'the authority Secret' || die "pod example"
expect_fail "pod example against another digest" si_p5_py pod docs/examples/storage-integrity-p5-rehearsal-pod.yaml "sha256:$(printf e%.0s $(seq 64))"
python3 -I - docs/examples/storage-integrity-p5-rehearsal-pod.yaml "$FX" <<'EOF'
import sys, yaml
fx = sys.argv[2]
def variant(name, change):
    d = yaml.safe_load(open(sys.argv[1]))
    change(d)
    yaml.safe_dump(d, open(f"{fx}/{name}.yaml", "w"))
variant("pod-port", lambda d: d["spec"]["containers"][0].__setitem__("ports", [{"containerPort": 80}]))
variant("pod-env", lambda d: d["spec"]["containers"][0]["env"].append({"name": "EXTRA", "value": "x"}))
variant("pod-volume", lambda d: d["spec"]["volumes"].append({"name": "keys", "secret": {"secretName": "other"}}))
variant("pod-token", lambda d: d["spec"].__setitem__("automountServiceAccountToken", True))
EOF
expect_fail "pod example with a port" si_p5_py pod "$FX/pod-port.yaml" "$(sed -n 2p "$si_backup/pins")"
expect_fail "pod example with a second Secret volume" si_p5_py pod "$FX/pod-volume.yaml" "$(sed -n 2p "$si_backup/pins")"
expect_fail "pod example with the service account token" si_p5_py pod "$FX/pod-token.yaml" "$(sed -n 2p "$si_backup/pins")"
expect_fail "pod example with a second variable" si_p5_py pod "$FX/pod-env.yaml" "$(sed -n 2p "$si_backup/pins")"
python3 -I - docs/examples/storage-integrity-p5-rehearsal-pod.yaml "$FX/pod-live.json" <<'EOF'
import json, sys, yaml
d = yaml.safe_load(open(sys.argv[1]))
d["metadata"]["uid"] = "u"
d["spec"]["containers"][0]["resources"]["requests"] = {"cpu": "2", "memory": "2Gi"}
d["status"] = {"phase": "Running"}
json.dump(d, open(sys.argv[2], "w"))
EOF
si_p5_py pod-live "$FX/pod-live.json" "$(sed -n 2p "$si_backup/pins")" | grep -q 'runs as the example states it' || die "pod-live"
ok "the Pod checks"

# §3.2's read-only block against stubs: the go block, its checksum, the Pod example, the dry run.
cp "$R5/p5-files.sha256" "$si_backup/p5-files.sha256"
cat > "$FX/stub-3.2.bash" <<'EOF'
si_kubectl() { case "$*" in "get pod storage-integrity-si-v2-p5-rehearsal --ignore-not-found -o name") ;; "apply --dry-run=server -f docs/examples/storage-integrity-p5-rehearsal-pod.yaml") echo "pod/storage-integrity-si-v2-p5-rehearsal created (server dry run)" ;; "describe secret storage-integrity-devnet2-si-v2-authority") printf 'Name: x\nprivate-key:  64 bytes\n' ;; *) echo "unexpected kubectl $*" >&2; return 1 ;; esac; }
EOF
b32="$(blk 'runbook source')"
bash -c 'set -eo pipefail; . "$1/helpers.sh"; . "$2"; . "$3"' sh "$B" "$FX/stub-3.2.bash" "$b32" > "$FX/out.txt" 2>&1 || { cat "$FX/out.txt"; die "§3.2 check block"; }
grep -q '^runbook source ' "$FX/out.txt" && grep -q '^authority Secret keys: private-key $' "$FX/out.txt" && grep -q 'created (server dry run)' "$FX/out.txt" || { cat "$FX/out.txt"; die "§3.2 check output"; }
cp docs/storage-integrity-devnet2-stage3-p5-v21.md "$FX/runbook.orig"
python3 -I - docs/storage-integrity-devnet2-stage3-p5-v21.md <<'EOF'
import re, sys
t = open(sys.argv[1]).read()
m = re.search(r"^```go\n", t, re.M)
open(sys.argv[1], "w").write(t[:m.end()] + "// changed\n" + t[m.end():])
EOF
if bash -c 'set -eo pipefail; . "$1/helpers.sh"; . "$2"; . "$3"' sh "$B" "$FX/stub-3.2.bash" "$b32" > "$FX/out.txt" 2>&1; then die "§3.2 accepted a changed source"; fi
grep -q 'is not the one p5-files.sha256 records' "$FX/out.txt" || { cat "$FX/out.txt"; die "§3.2 refusal text"; }
cp "$FX/runbook.orig" docs/storage-integrity-devnet2-stage3-p5-v21.md
ok "§3.2's source, checksum, Pod and dry-run checks (and the refusal of a changed source)"

# §2.1's block against the stand-in $R5, then a tampered binary.
b21="$(blk 'rehearsal binaries ok')"
bash -c 'set -eo pipefail; . "$1/helpers.sh"; . "$2"' sh "$B" "$b21" > "$FX/out.txt" 2>&1 || { cat "$FX/out.txt"; die "§2.1 block"; }
grep -q '^rehearsal binaries ok$' "$FX/out.txt" && grep -q '^restore_refused version 21: unsupported snapshot version 21 ' "$FX/out.txt" && grep -q '^restore_ok version 21 body interned$' "$FX/out.txt" && [ "$(grep -c ': OK$' "$FX/out.txt")" = 8 ] || { cat "$FX/out.txt"; die "§2.1 output"; }
cp "$R5/bin/old/snapdump" "$FX/snapdump.orig"; echo '# changed' >> "$R5/bin/old/snapdump"
if bash -c 'set -eo pipefail; . "$1/helpers.sh"; . "$2"' sh "$B" "$b21" > "$FX/out.txt" 2>&1; then die "§2.1 accepted a changed binary"; fi
cp "$FX/snapdump.orig" "$R5/bin/old/snapdump"
chmod 755 "$R5"
if bash -c 'set -eo pipefail; . "$1/helpers.sh"; . "$2"' sh "$B" "$b21" > "$FX/out.txt" 2>&1; then die "§2.1 accepted mode 755"; fi
chmod 700 "$R5"
ok "§2.1's checks of \$R5 (and its refusals of a changed binary and mode 755)"

# si_v21check against stand-in voter containers: 0 v21 interned with a newer .tmp, 1 v20, 2 none.
for i in 0 1 2; do rm -rf "$FX/voter$i"; mkdir -p "$FX/voter$i/data/raft/snapshots"; done
mkdir -p "$FX/voter0/data/raft/snapshots/2-9000-1"; cp "$R5/v21-fixture.container" "$FX/voter0/data/raft/snapshots/2-9000-1/state.bin"
sleep 1; mkdir -p "$FX/voter0/data/raft/snapshots/2-9500-2.tmp"
mkdir -p "$FX/voter1/data/raft/snapshots/2-8192-1"; cp "$FX/v20.bin" "$FX/voter1/data/raft/snapshots/2-8192-1/state.bin"
si_arbiter() { local o="$1" a args=(); shift; for a in "$@"; do case "$a" in "$FX"*) args+=("$a") ;; *) args+=("${a//\/data\/raft/$FX/voter$o/data/raft}") ;; esac; done; "${args[@]}"; }
[ "$(si_v21check 0 | tr '\n' ' ')" = "arbiter-0 version 21 body 1 restore_ok version 21 body interned " ] || die "si_v21check 0: $(si_v21check 0)"
[ "$(si_v21check 1 | tr '\n' ' ')" = "arbiter-1 version 20 body - restore_ok version 20 body none " ] || die "si_v21check 1: $(si_v21check 1)"
[ "$(si_v21check 2)" = "arbiter-2 no snapshot yet" ] || die "si_v21check 2"
cmp -s "$si_backup/state-0.bin" "$R5/v21-fixture.container" || die "state-0.bin"
line="$(si_v21check 0 | tr '\n' ' ')"
case "$line" in "arbiter-0 version 21 body 1 restore_ok version 21 body interned ") ;; *) die "§9's pattern" ;; esac
ok "si_v21check (a finished v21 snapshot behind a newer .tmp, a v20 one, none) and §9's pattern"

# si_p5_put, si_rehearsal, si_p5_member_keys and si_p5_keeper_findings against a stand-in Pod and ConfigMap.
rm -rf "$FX/pod"; mkdir -p "$FX/pod/rehearsal"
si_kubectl() {
  local a args=() seen=""
  case "$1 $2" in
    "get configmap") cat "$FX/member-config.yaml"; return ;;
  esac
  for a in "$@"; do
    if [ -n "$seen" ]; then
      a="${a//\/rehearsal/$FX/pod/rehearsal}"
      [ "$a" = sha256sum ] && { args+=(shasum -a 256); continue; }
      args+=("$a")
    fi
    if [ "$a" = -- ]; then seen=1; fi
  done
  case "${args[*]}" in
    "wget -qO- http://127.0.0.1:9091/metrics") printf '# HELP x\nsentio_node_storage_integrity_keeper_findings{reason="non_incarnation_root"} 0\nsentio_node_storage_integrity_keeper_findings{reason="unknown_replica"} 0\nother 1\n'; return ;;
  esac
  "${args[@]}"
}
printf 'binary\n' > "$FX/blob"
out="$(si_p5_put "$FX/blob" /rehearsal/p5rehearsal)"
[ "$out" = "/rehearsal/p5rehearsal $(shasum -a 256 "$FX/blob" | cut -d' ' -f1)" ] && cmp -s "$FX/blob" "$FX/pod/rehearsal/p5rehearsal" || die "si_p5_put: $out"
printf 'storage_integrity:\n  member:\n    node_id: member-b\n    serve:\n      endpoint: true\n' > "$FX/member-config.yaml"
[ "$(si_p5_member_keys b)" = 0 ] || die "si_p5_member_keys 0"
printf 'storage_integrity:\n  member:\n    node_id: member-b\n    consensus:\n      register: false\n' > "$FX/member-config.yaml"
[ "$(si_p5_member_keys b)" = 1 ] || die "si_p5_member_keys 1"
: > "$FX/member-config.yaml"
expect_fail "si_p5_member_keys of an empty config" si_p5_member_keys b
[ "$(si_p5_keeper_findings c | wc -l | tr -d ' ')" = 2 ] || die "si_p5_keeper_findings"
ok "si_p5_put, si_p5_member_keys and si_p5_keeper_findings"

# si_p5_accept: the rows it writes and where.
si_track_published() { echo 10; }
si_wait_track() { echo "track $1 published $2 -> 11"; }
si_agent_start() { [ "$1" = /k ] || return 1; }
si_agent_stop() { :; }
si_agent_ch() { case "$*" in *"INSERT INTO"*) { echo "$*"; cat; } >> "$FX/agent.txt" ;; *"SELECT count()"*) echo 1 ;; esac; }
si_accept_a_track() { echo "a-track $1"; }
mkdir -p "$HOME/si-devnet2-stage3-indexer-c"; echo s3edb > "$HOME/si-devnet2-stage3-indexer-c/acceptance-db"
: > "$FX/agent.txt"
si_p5_accept /k b 5401 5.1 | grep -qx 'indexer-b row 5401 in devuser1.s3_p1: safe count 1' || die "accept b"
si_p5_accept /k c 5402 5.2 | grep -qx 'indexer-c row 5402 in s3edb.s3e_e2e: safe count 1' || die "accept c"
[ "$(si_p5_accept - a 5403 5.3)" = "a-track 5403" ] || die "accept a"
[ "$(tr '\n' '|' < "$FX/agent.txt")" = "--database devuser1 --query INSERT INTO s3_p1 FORMAT CSV|5401,1,p5-5.1-5401|--database s3edb --query INSERT INTO s3e_e2e FORMAT CSV|5402,p5-5.2-5402|" ] || die "accept rows: $(cat "$FX/agent.txt")"
expect_fail "si_p5_accept with a non-numeric id" si_p5_accept /k b x 5.1
ok "si_p5_accept: indexer-b's row (id, 1, v) into devuser1.s3_p1, indexer-c's (id, v) into <db>.s3e_e2e, indexer-a through si_accept_a_track"

echo "self-test passed: $pass checks under bash $BASH_VERSION"
````

Then run the self-test under bash 5 and `/bin/bash` 3.2, `shellcheck` over every block, and Plan S3-B §15's production checks (local only):

```bash
cd "$W" && bash "$T/fixtures.bash" "$W" "$T/ledger.txt" "$HOME/si-devnet2-multisource-s3e-rehearsal" "$T/fx"
(cd "$T/fx/tree" && bash "$T/selftest.bash" "$T/blocks" "$T/fx" | tail -n 1 && /bin/bash "$T/selftest.bash" "$T/blocks" "$T/fx" | tail -n 1)
n=0; for f in "$T"/blocks/p5-*.bash; do n=$((n + 1)); shellcheck -s bash -e SC2154 "$f"; done; echo "shellcheck: $n blocks clean"
python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
helm lint charts/storage-integrity -f charts/storage-integrity/tests/legacy-devnet2-values.yaml --namespace sentio-network-devnet2 --strict | tail -1
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --strict | tail -1
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --set arbiter.consensusUpdatesEnabled=true --set arbiter.authoritySigningEnabled=false --strict | tail -1
bash scripts/storage-integrity/render-inertness.sh origin/main
git -C "$W" status --porcelain
```

Expected: `constants copied: 14` and `fixtures in <T>/fx`; `self-test passed: 46 checks under bash 5.<x>` and `self-test passed: 46 checks under bash 3.2.57(1)-release`; `shellcheck: 59 blocks clean` with no finding printed (SC2154 is excluded: the names are defined along the chain); `Ran <N> tests` and `OK` with Step 1's `<N>` (this task changes no test); `1 chart(s) linted, 0 chart(s) failed` three times; `<release> identical` for every release the script lists; and `git status` showing exactly ` M docs/storage-integrity-devnet2-multi-source-tracks.md` and `?? docs/storage-integrity-devnet2-stage3-p5-v21.md`. A `FAIL` line of the self-test names the check; fix the runbook (Step 3's text) and run Steps 4 to 6 again, never the test.

- [ ] **Step 7: Commit**

```bash
cd "$W" && git add docs/storage-integrity-devnet2-stage3-p5-v21.md docs/storage-integrity-devnet2-multi-source-tracks.md
git commit -q -m "docs(storage-integrity): devnet2 stage-3 P5 runbook (rehearsal and the v21 activation)

The runbook Plan S3-E4 Task 82 executes: the offline replay of the
voters' logs under ARBITER_TAG_P4 and ARBITER_TAG_P5, the rehearsal on
copies of the live state in a throwaway Pod (activation, restarts from
v21, a forced interner mismatch, the previous release refusing v21),
verifiers, hosts and si-members, voters, re-registration, the
irreversible v21 activation with every stage-3 switch off under a drain
hold, the stage-3 alert rules, every voter writing v21, rollback and
operating rules. The stage-2 runbook's image-floor rule points to it.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git log --oneline origin/main..HEAD
git diff --name-only origin/main...HEAD
git status --porcelain | wc -l | tr -d ' '
```

Expected: Task 80's commits followed by this one; Task 80's files and the two documents; `0`.

- [ ] **Step 8: Push (requires explicit user confirmation)**

This step needs explicit user confirmation in chat. It publishes the branch; nothing is applied or synced. Gate: "push branch urwt/claude/multi-source-s3e4 (Tasks 80 and 81 of Plan S3-E4: the stage-3 rule group, the snapshot-body value, the rehearsal Pod example, the P5 image constants and render tests, and the P5 runbook) to sentioxyz/production; nothing on sea changes".

```bash
cd "$W" && git push -u origin HEAD
```

Expected: `branch 'urwt/claude/multi-source-s3e4' set up to track 'origin/urwt/claude/multi-source-s3e4'` (or the update of a branch an earlier attempt pushed).

- [ ] **Step 9: PR (requires explicit user confirmation)**

This step needs explicit user confirmation in chat. The PR carries Tasks 80 and 81: shared-chart values, alert rules and a runbook that ends in an irreversible consensus update go through review (the user's production rule). Gate: "open the P5 PR from urwt/claude/multi-source-s3e4 into sentioxyz/production main, titled 'feat(storage-integrity): devnet2 stage 3 P5 (snapshot v21 activation runbook, monitoring, rehearsal pod)'". The body (written by this block into a temporary file, the test count and the two pins filled in):

```bash
cd "$W" && count="$(python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | sed -n 's/^Ran \([0-9]*\) tests.*/\1/p')" && [ -n "$count" ] && body="$(mktemp)" && cat > "$body" <<'BODY'
## Summary
- `charts/storage-integrity`: `arbiter.raft.snapshotBody` (unset; `auto` or `plain` renders `snapshot_body` as the last key of the voters' `raft:` block, the operational plain-body writer setting of housegate spec 2026-10-14 §10.3) and `monitoring.stage3Consensus` (off) with the rule group `<fullname>.arbiter-stage3`: `ArbiterSnapshotEncodingFallback` (critical), `ArbiterBlockSealGroupSwitch`, `ArbiterVerifiersUnavailable`, `ArbiterCopiesBackpressure`, `ArbiterUnderReplicatedCleanups`, `ArbiterGroupsUnderReplicated`, `ArbiterFloorHeldCleanupAgeHigh` and `ArbiterMemberLagging`, each reading this network's series and pointing into the P5 runbook; guards that fail closed.
- `docs/examples/storage-integrity-p5-rehearsal-pod.yaml`: the throwaway rehearsal Pod (arbiter ARBITER_TAG_P5 by digest, the authority key from its Secret as its only Secret, one emptyDir, the voters' limits), applied by hand and deleted within the runbook's §3.
- `test_render.py`: the P5 images (arbiter ARBITER_TAG_P5, sentio-node `sha-SN_COMMIT_P5`) reviewed for writer authorization, stage-3 hosts and si-members before any pin moves, and the render tests of the new values; the voter sizing stays pinned.
- New runbook `docs/storage-integrity-devnet2-stage3-p5-v21.md`: the offline replay of all three voters' logs under the P4 and P5 releases; the rehearsal on copies of the live state in the throwaway Pod (activation, restarts from v21, a forced interner mismatch, the previous release refusing v21 at its version byte); verifiers, hosts and si-members, voters, re-registration; the irreversible v21 activation with every stage-3 switch off under a drain hold; the stage-3 alert rules; every voter writing v21; rollback and operating rules (the plain-body lever). The stage-2 runbook's image-floor rule points to it.

Inert on merge: nothing on sea applies or syncs on merge, and every release renders byte-identically to `origin/main`. The live changes are the runbook's single-service commits, the hand-applied rehearsal Pod and the activation, each confirmed on its own.

## Test plan
- [x] `python3 -B charts/storage-integrity/tests/test_render.py` (TEST_COUNT tests)
- [x] the three `helm lint --strict` runs
- [x] `scripts/storage-integrity/render-inertness.sh origin/main`: every release identical
- [x] the runbook: the sections of Plan S3-E4 contract §57, `Confirm before running.` before every block that writes, the activation command verbatim, `bash -n` under bash and `/bin/bash`, `shellcheck`, every Python program compiled, no helper redefining or reaching another runbook's pins; its helpers run against stand-ins of the rehearsal tools and the cluster under bash 3.2 and 5

🤖 Generated with [Claude Code](https://claude.com/claude-code)
BODY
sed -i '' -e "s/TEST_COUNT/$count/" -e "s/ARBITER_TAG_P5/$(sed -n 's/^ARBITER_TAG_P5=//p' "$T/ledger.txt")/g" -e "s/SN_COMMIT_P5/$(sed -n 's/^SN_COMMIT_P5=//p' "$T/ledger.txt")/g" "$body"
! grep -n 'TEST_COUNT\|ARBITER_TAG_P5\|SN_COMMIT_P5' "$body"
gh pr create --repo sentioxyz/production --base main --head urwt/claude/multi-source-s3e4 --title "feat(storage-integrity): devnet2 stage 3 P5 (snapshot v21 activation runbook, monitoring, rehearsal pod)" --body-file "$body"
gh pr view urwt/claude/multi-source-s3e4 --repo sentioxyz/production --json number,url -q '"\(.number) \(.url)"'
```

Expected: no line from the placeholder check, the PR URL, then `<number> <url>`. Prefix the commands of Steps 10 and 11 with `pr=<number>;` as well.

- [ ] **Step 10: CI (read-only)**

It reads GitHub: it needs explicit user confirmation in chat before it runs.

```bash
[[ "${pr:-}" =~ ^[0-9]+$ ]] || { echo "set pr to the PR number of Step 9 first"; exit 1; }
cd "$W" && gh pr checks "$pr" --repo sentioxyz/production --watch
```

Expected: `Storage-integrity chart validation` passes (Task 80's files trigger it; this task's documents trigger nothing new). A failure: read its log with `gh run view --repo sentioxyz/production --log-failed`, fix on the branch (a new commit, pushed only after its own confirmation) and watch again; a failure is judged against a run of the same workflow on `main` before it is called a regression, and never fixed by weakening a test.

- [ ] **Step 11: Merge and record `P5_PR` (requires explicit user confirmation for the merge, and again for the ledger record's push or docs PR and its merge)**

This step needs explicit user confirmation in chat, after review. It merges into production `main`; nothing on sea changes until Task 82 runs the runbook. Gate: "squash-merge sentioxyz/production#<pr> (Plan S3-E4 Tasks 80 and 81) into main; inert on merge".

```bash
[[ "${pr:-}" =~ ^[0-9]+$ ]] || { echo "set pr to the PR number of Step 9 first"; exit 1; }
cd "$W" && gh pr merge "$pr" --repo sentioxyz/production --squash
gh pr view "$pr" --repo sentioxyz/production --json state,mergeCommit,mergedAt --jq '.state + " " + .mergeCommit.oid + " " + .mergedAt'
```

Expected: the merge, then `MERGED <40 hex> <time>`. Then record `P5_PR`, the PR number, with the record procedure of Plan S3-C1 contract §19 (S3-B decision D23). First write the run record `$HOME/.cache/s3-ledger/record-s3e4-81.md` with the Write tool, one paragraph: `sentioxyz/production#<pr>` (Plan S3-E4 Tasks 80 and 81) squash-merged as `<sha>` at <mergedAt> UTC; `test_render.py` `<N>` tests OK (Task 80's count); the three `helm lint --strict` profiles passed; `render-inertness.sh origin/main` printed every release `identical`; the runbook check passed (33 headings, 59 bash blocks, 30 confirmed, the chain's counts of Step 4) and the self-test passed 46 checks under bash 3.2 and 5; the rehearsal source is `$R5/p5rehearsal/p5rehearsal_test.go` (sha256 `<P5REHEARSAL_SHA256 as filled>`); CI `Storage-integrity chart validation` passed. Then R1 (local; its housegate fetch reads a remote, so it needs explicit user confirmation in chat, like every remote read of this task), with `pr=<number>;` as its prefix:

```bash
[[ "${pr:-}" =~ ^[0-9]+$ ]] || { echo "set pr to the PR number of Step 9 first"; exit 1; }
[ "$(gh pr view "$pr" --repo sentioxyz/production --json state --jq .state)" = MERGED ] || { echo "the P5 PR is not merged: stop"; exit 1; }
h=/Users/uranuswch/Dev/projects/sentio-network/housegate; L="$HOME/.cache/s3-ledger"; n=81
rel=docs/superpowers/plans/2026-10-15-multi-source-si-s3e4-p5-floor-release-activation.md
git -C "$h" fetch -q origin
ref="$(python3 -I "$L/s3-ledger.py" ref "$h" s3e4)" || exit 1
out="$(bash ~/.agents/skills/urwt/scripts/urwt-create.sh "$h" claude "multi-source-s3e4-ledger-$n" "urwt/claude/multi-source-s3e4-ledger-$n" "$ref")"
d="$(printf '%s\n' "$out" | jq -r .path)"
[ "$(printf '%s\n' "$out" | jq -r .action)" = created ] && [ "$(git -C "$d" rev-parse HEAD)" = "$(git -C "$h" rev-parse "$ref")" ] && [ -z "$(git -C "$d" status --porcelain)" ] ||
  { printf '%s\n' "$out"; echo "the ledger worktree is not a new, clean checkout of $ref (an earlier attempt?): stop and ask"; exit 1; }
printf '%s\n%s\n' "$d" "$ref" > "$L/record-s3e4-$n"
python3 -I "$L/s3-ledger.py" set "$d/$rel" "$n" "$L/record-s3e4-$n.md" P5_PR="$pr" || exit 1
git -C "$d" commit -q -m "docs(plan): record P5_PR of Plan S3-E4 (Task 81)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>" -- "$rel"
git -C "$d" show --stat --oneline HEAD | cat
```

Expected: `s3-ledger: Task 81 recorded P5_PR=<pr> and its run record 'P5 PR' in <file>`, then one commit of the plan file only. A `stop` writes nothing: report it and ask.

R2 (remote write): needs explicit user confirmation in chat before running. Gate: "push Task 81's ledger record (Appendix C of Plan S3-E4: P5_PR and the run record 'P5 PR'; docs only) to housegate, straight to urwt/claude/ms-si-stage3-plans-cf when that branch is the ledger copy, else as a docs PR into main". After a yes:

```bash
L="$HOME/.cache/s3-ledger"; n=81
d="$(sed -n 1p "$L/record-s3e4-$n")"; ref="$(sed -n 2p "$L/record-s3e4-$n")"
[ "$(git -C "$d" rev-list --count "$ref..HEAD")" = 1 ] && [ -z "$(git -C "$d" status --porcelain)" ] || { echo "the ledger worktree $d is not one clean record commit ahead of $ref: stop"; exit 1; }
if [ "$ref" = origin/main ]; then
  git -C "$d" push -u origin HEAD
  printf '%s\n\n%s\n' "$(git -C "$d" log -1 --format=%s): Appendix C of Plan S3-E4 only, docs only (Plan S3-C1 contract §19)." '🤖 Generated with [Claude Code](https://claude.com/claude-code)' > "$L/record-s3e4-$n-pr.md"
  (cd "$d" && gh pr create --repo housegate/housegate --base main --title "$(git log -1 --format=%s)" --body-file "$L/record-s3e4-$n-pr.md") | tee "$L/record-s3e4-$n-pr-url"
else
  git -C "$d" push origin "HEAD:${ref#origin/}"
fi
```

Expected: the push to `urwt/claude/ms-si-stage3-plans-cf`, or the push and the docs PR URL. A rejected push (the plans branch moved since R1) is never forced: stop and ask. R3, on `origin/main` only: the PR's merge needs its own explicit confirmation in chat (Gate: "squash-merge housegate PR <url>: Task 81's ledger record"); after a yes, `gh pr merge <number> --repo housegate/housegate --squash`. Then R4 (read-only; a remote read, so it needs explicit user confirmation in chat):

```bash
h=/Users/uranuswch/Dev/projects/sentio-network/housegate; L="$HOME/.cache/s3-ledger"
git -C "$h" fetch -q origin
python3 -I "$L/s3-ledger.py" get "$h" P5_PR
```

Expected: `P5_PR=<the value written>`. Anything else (for example the plans PR merged after R1, so the record went to a branch that is no longer the ledger copy): stop and ask. This task is done only once R4 prints `P5_PR`. Task 82 reads the runbook from a production checkout at a freshly fetched `origin/main` in its live worktree (`multi-source-s3e4-live`, detached), never from this worktree, and reads `P5_PR` from the ledger copy only.

---

## Appendix A: binding contract (continues the Appendix A of Plans S3-A to S3-E3)

Plan S3-A's §0–§6, Plan S3-B's §7–§15 and the contracts of S3-C1 (§16–§19), S3-C2 (§20–§24), S3-C3 (§25–§28), S3-C4 (§29–§32), S3-D (§33–§37), S3-E1 (§38–§43), S3-E2 (§44–§48) and S3-E3 (§49–§52) stay binding. This appendix continues the numbering. Every name below binds every task of Plan S3-E4 verbatim. A task that needs a name not listed here adds it in its own section and records it in Appendix B; it never renames one listed here.

**Consumed names, by source.** S3-C1 … S3-E3 were drafted in parallel with this plan; the cross-plan consistency pass reconciled this list with their contracts (recorded in Appendix B). A name that ever differs from its source contract takes the source's spelling; a task never renames one.
- S3-A §1, §4, §6:
  - **FSM drain view:** `fsm.DrainView{PromotionSeq, Refusal, TracksActive, GlobalPrefix, GlobalFrontier, InFlightBlocks, Tracks}`, `fsm.TrackDrainView{IndexerID, Quarantined, Prefix, Frontier, InFlightBlocks}`, `(*fsm.FSM).DrainView()`, `(*FSM).promotionBlockLocked(pp *PendingPromotion) uint64`.
  - **Server holds:** `server.ConsensusHolds`, `consensusHold`, `(*ConsensusHolds).start(mode, term, timeout, view)`, `(*Server).drainView()`, `Server.holdView` (tests only), `ConsensusHoldMetadataKey = "arbiter-consensus-hold"`.
  - **arbiter-admin:** `consensus update --hold none|issuance|drain` (`holdName`, default `drain`) and `--hold-timeout`; `parseHoldMode`, `holdModeName`, `consensusHoldSession{…, state *pb.ConsensusParamsState, …}`, `takeConsensusHold`; its stderr lines `took a consensus hold of mode <mode> on the leader (hold id <id>, expires <t>)`, `waiting for the leader's drain hold: <progress>`, `the leader drained at promotion_seq <n>; signing the update`, `released consensus hold <id>`; `consensus hold-status`, `consensus hold-release`.
  - **Metrics:** `arbiter_state_bytes`, `arbiter_snapshot_persist_seconds`.
  - **§6 and the rehearsal tools:** the release and pin commands; the S3-A rehearsal sources `$HOME/si-devnet2-multisource-s3a-rehearsal/{statedigest,v20check,snapdump}/main.go`.
- S3-B §8–§15:
  - **Monitoring chart:** the `monitoring` values, `templates/monitoring.yaml`, the rule conventions (`severity`, `summary`, `runbook_url`, `network="<network>"` selectors, no `job`), `MONITORING_ALERTS`, `render-inertness.sh`.
  - **test_render.py constants:** `DEVNET2_ARBITER_DIGEST`, `DEVNET2_STAGE3_SOURCE_IMAGE`, `STAGE3_HOST_IMAGES`, `WRITER_AUTHORIZATION_IMAGES`, `FRESH_SOURCE_IMAGE`, `INDEXER_B_SOURCE_IMAGE`, `INDEXER_C_SOURCE_IMAGE`, `DEVNET2_SNODE_IDS`, `DEVNET2_STATE_LIMIT_BYTES`, `test_devnet2_voter_sizing_is_pinned`.
  - **Runbook procedure:** decisions D20, D23 and D24.
  - **Runbook helpers:** `si_kubectl`, `si_arbiter`, `si_voter`, `si_metrics`, `si_capability`, `si_leader`, `si_show`, `si_images`, `si_rpc_on`, `si_info`, `si_node_image`, `si_replace_voter`, `si_features_tracks`, `si_tracks`, `si_track`, `si_global`, `si_sealed`, `si_track_metrics`, `si_state_size`, `si_hold_metrics`, `si_series_value`, `si_vm_query`, `si_registry_names`, `si_audit`, `si_ch_admin`.
  - **Ledger:** `INDEXER_C_ID`, `ENROLMENT_EPOCH`.
- S3-C1 §19: `$HOME/.cache/s3-ledger/s3-ledger.py` with `ref`/`show`/`get`/`empty`/`set` and the record procedure.
- S3-C2 §23–§24: the grant lists `SI_MEMBER_GRANTS` and `SI_MEMBER_DDL_GRANTS` (§23.2). On `hg_rstage.*` they grant the explicit `SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE`, never `ALL` (decision D36). Also the sentio-node si-member config `config.SIMemberConfig` loaded by `config.LoadSIMemberConfig` (YAML block `storage_integrity.member`, Go `config.StorageIntegrityMemberConfig`), `sentio-node si-member --config-path` (`commands.NewSIMemberCommand`, package `simember` with `simember.Run` and the collector `siMemberCollector`), arbiter-core `member.Role`, `member.Config`.
- S3-C3 §26–§27: the `charts/sentio-node` value block `siMember` with its own image pin `siMember.image`, rendering StatefulSet `<fullname>-si-member`; ledger `MEMBER_IDS` (`member-a:0,member-b:1,member-c:<INDEXER_C_ID>`, K29); runbook `docs/storage-integrity-devnet2-stage3-p2-members.md`.
- S3-C4 §31: sentio-node `rpc.StorageIntegrityInfo` field `ReplicaHosts` (`replica_hosts`), `tablestate.ReplicaHosts` and the static replica-host list (K50); arbiter-core `member/serve` (`serve.ServingView`).
- S3-D §34–§37: `ARBITER_TAG_P4` / `ARBITER_DIGEST_P4`, `SN_COMMIT_P4` / `SN_DIGEST_P4`, `DEVNET2_FAILURE_DOMAINS` (`<indexer id>=node:<hostname>` per enrolled indexer, comma-separated, ascending ids, K55), `P4_END`.
- S3-E1 §39–§43:
  - **Proto:** `Stage3Params` fields 1–18 (`group_size` … `narrowed_drain`), `stage3` = 14 on `ConsensusParamsUpdate` and 9 on `ConsensusMutableParams`, `ConsensusParamsState.stage3_state` = 9, `ADMISSION_CODE_VERIFIERS_UNAVAILABLE = 12`, `ADMISSION_CODE_COPIES_BACKPRESSURE = 13`, `TableRegistrySnapshot.groups = 10`, `stage3 = 12`.
  - **Canonical Go types:** `arbiter.Stage3Params{GroupSize, NetworkFillDefault, MinSafeCopies, StrictCopies, IndexerFailureDomains, MaxLagPublications, LagLow, EvidenceTimeoutL2Blocks, MaxUncleanedPartsPerPartition, MaxUncleanedPartsPerTable, ExcludeQuarantinedHosts, Purge, EvictionVoiding, OwnerAwareSelection, Placement, Reselection, Checkpoints, NarrowedDrain}`, `arbiter.IndexerFailureDomain{IndexerID, FailureDomain}`, `arbiter.VerifierEntry{NodeID, Ed25519Pubkey, HostIndexerID *uint64, ReplicaSigner, ConsentJWS, Weight, FailureDomain, Verify, Replicate, Counts, SharedRMT}`, `arbiter.ExcludedTable`, `arbiter.AdmissionCodeVerifiersUnavailable`, `arbiter.AdmissionCodeCopiesBackpressure`, `arbiter.Stage3Feature = "stage3_v1"`.
  - **Consent:** `authority.GroupMemberConsentStatement{NetworkID, GenesisSnapshotID, HostIndexerID, NodeID, Ed25519Pubkey, ReplicaSigner}`, `(*authority.Signer).SignGroupMemberConsent`, `authority.VerifyGroupMemberConsent(stmt, jws, signer)`, purpose `arbiter-group-member-v1`.
  - **Wire and member mode:** `wire.TableRegistrySnapshot.Groups []wire.DatabaseGroup`, `wire.DatabaseGroup{OwnerIndexerID, DatabaseID, Members []GroupMember, NetworkFill, Generation, PastMembers, Leaving []LeavingMember, GroupKey, PlacementSalt}`, `wire.GroupMember{NodeID, JoinedGeneration, ReadyGeneration, Class, Checkpoint, ObservedEntry, LagAdvances, HostIndexerID}` with `Ready()`. Its `HostIndexerID uint64` is the registry-view field `GroupMember.host_indexer_id`, which S3-E1 adds and fills at serve time from the member's `VerifierEntry.HostIndexerID` (decision D27). That value is fixed for the entry's life (K68), is not FSM state and is not Raft-reachable. `LagAdvances` saturates at `max_lag_publications + 1` (decision D31). Also `wire.LeavingMember{NodeID, RemovedGeneration, ReleaseEntry, FrozenMembers, FrozenDomains, Released}`; `member.Config.Consensus *consensus.Config` with `consensus.Config{NodeID, NetworkID, GenesisSnapshotID, StateDir, Ed25519Seed []byte, ReplicaSignerKeyHex string, KeeperShardID, Tables, Register bool, ReadinessInterval, CheckpointInterval, ResolveInterval}`, `consensus.ReconcilerConfig{NodeID, StateDir, KeeperShardID, CreateReplicas bool, Interval}`, `consensus.NewUnsafeReconciler`, `consensus.Metrics`, `tableset.Stats.KeeperFindings` and the reserved series `sentio_node_storage_integrity_keeper_findings{reason}`.
  - **Releases:** `PROTO_TAG_P5`, `HG_TAG_P5`, `CORE_TAG_P5`.
- S3-E2 §44–§48:
  - **FSM state:** `fsm.Params.Stage3 *arbiter.Stage3Params`, `fsm.State.Stage3 *Stage3State`, `Stage3SwitchCheckpoints`, `Stage3SwitchNarrowedDrain`, `stage3SwitchOnLocked(name)`, `onStage3SwitchOnLocked(name)` (this plan adds `narrowed_drain`), `verifierEntryLocked(nodeID) (arbiter.VerifierEntry, bool)`, `stage3PendingProtoFields` (Task 78 asserts it empty).
  - **Stage3 gate:** `server/stage3_gate.go` with metadata key `arbiter-stage3-voters` and refusals prefixed `stage3 gate refused: `; the post-activation registration refusal.
  - **Snapshot v21:** `snapshotVersionV21 = 21`, the body-kind byte right after the version byte (0 plain, 1 interned), node config `raft.snapshot_body: auto|plain`, `arbiter_snapshot_encoding_fallback_total`, `arbiter_snapshot_body_bytes{kind}`, the forced-mismatch seam `internSnapshotBodyFunc` of Task 67.
  - **arbiter-admin:** `--stage3`, `--indexer-failure-domain id=N,domain=D` (repeated), `--stage3-max-lag-publications`, `--stage3-lag-low`, `--stage3-evidence-timeout-l2-blocks`, `--stage3-max-uncleaned-parts-per-partition`, `--stage3-max-uncleaned-parts-per-table`, `--stage3-switch NAME`, `--stage3-min-safe-copies`.
  - **Branch:** `urwt/claude/multi-source-s3e` in worktree `~/src/remotesrc/claude/arbiter/multi-source-s3e`.
  - **Rehearsal directory:** `$HOME/si-devnet2-multisource-s3e-rehearsal` (§48) with its tools (`v21check` among them) and `rehearsal_build`. This plan calls it `$R5` and keeps it as the one P5 rehearsal directory (decision D37, §55).
- S3-E3 §49–§52:
  - **Group state:** `Stage3State.Groups []*DatabaseGroup`, `DatabaseGroup{Owner uint64; DatabaseID string; Generation uint64; Salt string; NetworkFill bool; Members []GroupMember; Leaving []LeavingMember; PastMembers []string}`, `GroupMember{NodeID string; JoinedGeneration uint64; Class MemberClass; ReadyGeneration uint64; ReadySignature string; ReadyEpoch uint64; Checkpoint *MemberCheckpointRecord; ObservedEntry *replay.TrackFrontier}`.
  - **Classes:** `MemberClass` with `MemberBootstrapping`, `MemberCaughtUp`, `MemberBehind`, `MemberLagging`, `MemberLeaving` (spelled `bootstrapping`, `caught_up`, `behind`, `lagging`, `leaving`), `MemberClasses`.
  - **Checkpoints and Leaving records:** `MemberCheckpointRecord{Checkpoint arbiter.MemberCheckpoint; ReplicaJWS string; Epoch uint64}` (the checkpoint's `Entry` and its groups' `Excluded`), `LeavingMember{Member GroupMember; RemovedGeneration uint64; Entry replay.TrackFrontier; FrozenE []string; FrozenEDomains uint32; Released bool}` in `DatabaseGroup.Leaving`.
  - **Helpers and Apply arms:** `(*FSM).groupLocked(owner uint64, database string) *DatabaseGroup`, `(*FSM).copyFloorSetLocked(g *DatabaseGroup, strict bool, exclude string) (nodes []string, domains uint32)` (E), `(*FSM).groupsChangedLocked()`, `(*FSM).checkpointsOnLocked()`, `memberLagLocked`, `entryCompare`, `applyMemberCheckpoint`, `applyRebalanceGroup`, `applyMarkMemberLagging`.
  - **Metrics:** `arbiter_block_seal_reason{reason}` with `reason="group_switch"`; `arbiter_database_groups{owner}` and `arbiter_group_members{owner,class}` (from `FSM.GroupStats()`).

### 53. Copy floor, strict mode, Leaving release and raising k (Task 76)

**When it applies.**
- Every rule below runs only while `func (f *FSM) floorActiveLocked() bool` holds: S3-E3's `f.checkpointsOnLocked()` (the committed `checkpoints` switch).
- Otherwise `applyScheduleUnsafeCleanup` (`arbiter:fsm/apply.go:960-1020`), the WorkSet's `PublishedUncleaned` (`fsm/reads_work.go:317-364`), admission (`fsm/admission.go:133-158`) and `applyUpdateConsensusParams` (`fsm/consensus_updates.go:51-87`) are byte-identical to the base.
- Rejected-part cleanups (`RejectedBlockSeq != 0`, promotion seq 0, C4) are never gated. Cleanups scheduled before the switch are not re-checked.

**File `fsm/floor.go` (new):**
```go
type UnderReplicatedReason string

const (
	UnderReplicatedNone                UnderReplicatedReason = ""
	UnderReplicatedInactiveIncarnation UnderReplicatedReason = "inactive_incarnation"
	UnderReplicatedPressurePartition   UnderReplicatedReason = "pressure_partition"
	UnderReplicatedPressureTable       UnderReplicatedReason = "pressure_table"
	UnderReplicatedDomainsBelowK       UnderReplicatedReason = "domains_below_k"
)

// FloorVerdict is spec §9.3 for one acknowledged promotion.
type FloorVerdict struct {
	BaseRefusal       string                // "" when base(P) holds
	Escape            UnderReplicatedReason // inactive_incarnation, pressure_partition or pressure_table, else ""
	K                 uint32                // min_safe_copies
	Required          uint32                // min(k-1, |E|); k-1 in strict mode
	EDomains          uint32                // |E|
	ReflectingDomains []string              // sorted distinct failure domains of the reflecting members of E
	Allowed           bool                  // base && (escape || len(ReflectingDomains) >= Required)
	FloorOnly         bool                  // base holds, no escape, the count clause fails
	UnderReplicated   UnderReplicatedReason // Escape, or domains_below_k when Allowed with |E| < k-1, else ""
}

func (f *FSM) floorActiveLocked() bool
func (f *FSM) floorLocked(pp *PendingPromotion) FloorVerdict
func (f *FSM) floorIncarnationLocked(tableID string) *TableIncarnation // manifestTableOwnerLocked's scan; never the lazy live index
func (f *FSM) floorMembersLocked(g *DatabaseGroup, strict bool) map[string][]string // failure domain -> sorted node ids of E: S3-E3's copyFloorSetLocked(g, strict, "") grouped by FailureDomain
func (f *FSM) checkpointReflectsLocked(cp *MemberCheckpointRecord, databaseID, tableID string, pp *PendingPromotion) bool
func (f *FSM) uncleanedPartsLocked(tableID string) (perPartition map[string]int, total int)
func (f *FSM) floorOnlyHeldLocked() []uint64 // ascending promotion seqs whose cleanup only the floor holds
func (f *FSM) raiseKRefusalLocked(next Params) string
func (f *FSM) leavingReleaseHoldsLocked(g *DatabaseGroup, m *LeavingMember) bool
func (f *FSM) releaseLeavingMembersLocked() bool // true when it recorded a release (one registry bump)
```

**base(P).** `pp.Acked` (today's rule in both callers); let O = `promotionTrackLocked(seq)` and S = `publishedTrackFrontierLocked(O)`; then `promotionBlockLocked(pp) ≤ S.SafeBlockSeq` and `pp.Promote.PromotionSeq ≤ S.AppliedPromotionSeq`; and O is not quarantined. The quarantine refusal keeps today's text, `indexer %d is quarantined; no new cleanup is scheduled`. The new `Rejected.Reason` texts, verbatim, equal `BaseRefusal`:
- `copy floor: promotion %d is not acknowledged`
- `copy floor: promotion %d covers block %d above owner %d's published safe_block_seq %d`
- `copy floor: promotion %d is above owner %d's published applied_promotion_seq %d`

**E and the count clause.**
- **P's group:** G(O, d), with d the database of `pp.Promote.TableID` (`splitTableKey`).
- **E:** S3-E3's `copyFloorSetLocked(g, strict, "")` (§50): the group's `Members` whose verifier entry (`verifierEntryLocked`) is listed, `Replicate && Counts` and non-evicted, whose class is `caught_up` or `behind` (in strict mode also `lagging`), and whose `FailureDomain` differs from O's `IndexerFailureDomains` value. |E| is its `domains`, the number of distinct `FailureDomain` values among them.
- **Reflects:** the member's `Checkpoint` (`*MemberCheckpointRecord`) for this group has `Checkpoint.Entry.SafeBlockSeq ≥ promotionBlockLocked(pp)`, `Checkpoint.Entry.AppliedPromotionSeq ≥ pp.Promote.PromotionSeq`, and its `CheckpointGroup` of d has an `Excluded` list that does not name P's table.
- **Count:** `Required` = min(k − 1, |E|) (k − 1 in strict mode), counted over distinct domains, never over members. A group that does not exist gives |E| = 0.
- **Refusal** (Apply, `FloorOnly`), verbatim: `copy floor: promotion %d of %s needs reflecting checkpoints in %d distinct failure domains other than owner %d's, has %d`.

**Escapes, in this order:**
1. `inactive_incarnation`: `floorIncarnationLocked(table)` is nil or not `TableStatusActive`.
2. `pressure_partition`: outside strict mode, the true entries of `f.st.PromotedUnsafe[{table, partition}]` number more than `MaxUncleanedPartsPerPartition`.
3. `pressure_table`: outside strict mode, their sum over the table is above `MaxUncleanedPartsPerTable`.

"Above" means strictly greater.

**Callers.**
- `applyScheduleUnsafeCleanup` calls `floorLocked` right after the quarantine check (`apply.go:997-1004`) for promotion cleanups while `floorActiveLocked`. It refuses with `BaseRefusal` or the count refusal and leaves state unchanged. On `Applied` with `UnderReplicated != ""` it emits `Event{Kind: EventUnderReplicated, PromotionSeq: seq}`.
- The WorkSet loop lists a `PublishedUncleaned` entry only when `floorLocked(pp).Allowed`, with the same function under the read lock. It sets `CleanupWork.UnderReplicated UnderReplicatedReason` and fills `WorkSet.FloorHeld []uint64`, the ascending seqs with `FloorOnly`. Both fields are new in `fsm/reads_work_types.go` and stay zero while the switch is off.
- `EventKind` gains `EventUnderReplicated`, appended after `EventIndexerQuarantineChanged`, with `String()` `under_replicated`.

**Strict mode (`StrictCopies`).**
- The count clause needs k − 1, `lagging` members stay in E, and both pressure escapes are off; the purge exemption remains.
- Admission refusal, placed after S3-E2's `VERIFIERS_UNAVAILABLE` check and before the lane budget (`admission.go:148-158`), with state unchanged. When `floorActiveLocked && StrictCopies`, the statement's target table T has a partition above `MaxUncleanedPartsPerPartition` or a total above `MaxUncleanedPartsPerTable`, and some acknowledged promotion of T is `FloorOnly`, the answer is `SubmitResult{Code: arbiter.AdmissionCodeCopiesBackpressure, Message: m}`.
- Partitions are checked first, in ascending partition id. `m` is verbatim:
  - `storage-integrity copy floor: partition %s of %s holds %d uncleaned promoted parts (max_uncleaned_parts_per_partition %d) whose cleanup waits for member checkpoints (strict_copies); retry later`
  - `storage-integrity copy floor: %s holds %d uncleaned promoted parts (max_uncleaned_parts_per_table %d) whose cleanup waits for member checkpoints (strict_copies); retry later`

**Raising k.**
- `raiseKRefusalLocked` runs in the Apply-only half of `applyUpdateConsensusParams`, after the drain gate (`:74-78`) and the tracks-activation refusal (`:79-87`), before any mutation, and only when `next.Stage3.MinSafeCopies` > the current value.
- For every Active incarnation (registry order) and its group G(O, d), it counts the distinct failure domains other than O's among listed, non-evicted, `Replicate && Counts` members whose `Checkpoint.Entry` equals `publishedTrackFrontierLocked(O)` and whose `Excluded` is empty. That count must be at least k − 1.
- Refusal, verbatim: `consensus update raises stage3.min_safe_copies to %d: %s (incarnation %d, owner %d) has caught-up counting members in %d distinct failure domains other than its owner's, needs %d`.
- Restore never runs it; it reads state that legitimately changes afterwards.

**Leaving release.**
- **Non-strict** (`StrictCopies` false at the check): the members of `m.FrozenE` (never m itself) that are still listed and non-evicted, whose `Checkpoint` for g is at or past `m.Entry` (both `SafeBlockSeq` and `AppliedPromotionSeq` ≥) and excludes none of g's tables, cover at least min(k − 1, `m.FrozenEDomains`) distinct current failure domains.
- **Strict:** the current `Members` counted as E is counted now (current k, `lagging` included, never m), with checkpoints at or past the entry that exclude none of g's tables, cover at least k − 1 distinct domains.
- **Recording:** the first time the condition holds it sets `m.Released = true` (S3-E3's `LeavingMember.Released`); nothing ever clears it.
- **Callers:** `releaseLeavingMembersLocked` runs at the end of `applyMemberCheckpoint`, `applyRebalanceGroup`, `applyMarkMemberLagging`, `applyUpdateConsensusParams` (before `ConsensusEpoch++`) and `applyEvictNode` (`apply.go:121-149`, after `completePurgesLocked`). A release bumps the registry once per Apply.
- **At k = 1** the release is recorded in the `RebalanceGroup` Apply that creates the Leaving record.

**Metrics.**
- **Replicated, every replica.** `fsm.MetricsSnapshot` (`fsm/reads_metrics.go:28-55`) gains `FloorHeldCleanups uint64`, `UnderReplicatedGroups uint64` and `SafeCopiesMin []MetricSafeCopies` with `type MetricSafeCopies struct{ IndexerID, Copies uint64 }`. They are empty or zero until the checkpoints switch. Member classes are S3-E3's `arbiter_group_members{owner,class}` (§50), not redefined here.
- **Collector series** (`metrics/metrics.go:130-171`):
  - `arbiter_floor_held_cleanups`
  - `arbiter_under_replicated_groups`: groups whose |E| < k − 1.
  - `arbiter_safe_copies_min{indexer_id}`: per owner, the minimum over its groups of 1 + the distinct failure domains other than its own among listed, non-evicted, counting replicate members whose checkpoint equals its latest published entry and excludes nothing.
- **Leader-local.**
  - `orchestrator.FloorMetrics interface{ UnderReplicatedCleanup(reason string); FloorHeldCleanups(count int, oldest time.Duration) }` and `orchestrator.Deps.Floor FloorMetrics` (nil disables).
  - `handlePromotionRows` (`orchestrator/promotion.go:39-43`) counts a `PublishedUncleaned` entry with a reason only when its proposal returns no error.
  - The loop keeps `floorHeldSince map[uint64]time.Time`, reset where `resetSealTimes` runs (`orchestrator/metrics.go:16-20`), and reports `ws.FloorHeld` each pass.
  - `metrics.Metrics` implements `FloorMetrics` as `arbiter_under_replicated_total{reason}` (counter) and `arbiter_floor_held_cleanup_age_seconds` (gauge), and `cmd/arbiter` wires it to `Deps.Floor`.

**Test constants:** `floorDomainOwner = "node:owner"`, `floorDomainB = "node:b"`, `floorDomainC = "node:c"`, `floorDomainD = "node:d"`. Fixture `newFloorFixture(t *testing.T, o floorFixtureOptions) *floorFixture` with `floorFixtureOptions{K uint32; Strict bool; MemberDomains map[string]string; MaxPerPartition, MaxPerTable uint32}`, built on S3-E3's checkpoint fixture (§52).

**Named tests.**
- **fsm:** `TestFloorSwitchOffIsByteIdentical`, `TestFloorIsDormantAtKOne`, `TestFloorApplyAndWorkSetAgreeOnEveryCleanup`, `TestFloorPublicationClausesRefuseAnUnpublishedPromotion`, `TestFloorHoldsCleanupUntilKMinusOneDomainsReflect`, `TestFloorCountsTwoMembersInOneDomainOnce`, `TestFloorParamsUpdateThatCollidesDomainsOnlyLowersTheCount`, `TestFloorExcludedTableDoesNotCount`, `TestFloorPurgeExemptionForANonActiveIncarnation`, `TestFloorPressureBypassPerPartitionAndPerTable`, `TestFloorEscapesRaiseUnderReplicated`, `TestFloorDomainsBelowKRaisesUnderReplicated`, `TestRejectedPartCleanupIsNeverGatedByTheFloor`, `TestCleanupScheduledBeforeTheCheckpointsSwitchIsNotRechecked`, `TestFloorKeepsTheQuarantineRefusal`, `TestStrictModeRequiresKMinusOneDomains`, `TestStrictModeKeepsLaggingMembersInE`, `TestStrictModeTurnsPressureIntoCopiesBackpressure`, `TestCopiesBackpressureLeavesStateUnchanged`, `TestCopiesBackpressureClearsOnceTheFloorIsMet`, `TestRaisingKRequiresCaughtUpCountingMembersInKMinusOneDomains`, `TestRaisingKRefusalLeavesStateUnchanged`, `TestRaisingKIgnoresNonActiveIncarnations`, `TestNonStrictLeavingReleaseUsesTheFrozenE`, `TestStrictLeavingReleaseWaitsWhenTheSoleCaughtUpMemberIsReplacedByABootstrappingOne`, `TestLeavingReleaseIsRecordedOnceAndNeverWithdrawn`, `TestLeavingReleaseIsEvaluatedAfterEveryTriggeringCommand`, `TestLeavingReleaseAtKOneIsImmediate`, `TestFloorMetricsSnapshot`.
- **orchestrator:** `TestLoopCountsUnderReplicatedCleanupsOnlyWhenApplied`, `TestFloorHeldCleanupAgeIsLeaderLocalAndRestartsOnLeadershipChange`.
- **metrics:** `TestFloorMetrics`.

### 54. Narrowed drain gate and the issuance-hold default (Task 77)

**File `fsm/drain_narrowed.go` (new):**
```go
// TouchedTracks lists, ascending, the indexer ids whose si_indexers entry next
// adds or changes against previous (spec §10.1): an enrolment or a signer
// rotation. verifiers, stage3, authority and max_writers changes touch none.
func TouchedTracks(previous, next []arbiter.SIIndexerEntry) []uint64

func (f *FSM) narrowedDrainOnLocked() bool // S3-E2's stage3SwitchOnLocked(Stage3SwitchNarrowedDrain) on the committed params
func (f *FSM) narrowedDrainRefusalLocked(touched []uint64) string
func (f *FSM) drainRefusalForUpdateLocked(next []arbiter.SIIndexerEntry) string // narrowed when on, else drainGateRefusalLocked()
func (f *FSM) floorHeldUnsafePartsLocked() map[arbiter.TablePartition]map[string]bool
func (f *FSM) DrainViewFor(draft arbiter.ConsensusParamsUpdate) DrainView
```

**Changed when.** An entry counts as changed when it is not `reflect.DeepEqual`. A nil `next` stands for the committed list.

**Call site.** `consensus_updates.go:74-78` calls `f.drainRefusalForUpdateLocked(next.SIIndexers)` instead of `f.drainGateRefusalLocked()`. `drainGateRefusalLocked` (`fsm/quarantine.go:203-256`) is unchanged. Narrowing depends on the committed switch, so the update that turns `narrowed_drain` on is still judged by the stage-2 gate.

**Narrowed refusals,** verbatim. They are checked per touched track in ascending order, each in the stage-2 order, and a quarantined touched track is skipped as in the stage-2 gate:
- `consensus update touches track %d, which has an unacknowledged promotion`
- `consensus update touches track %d, which has an unacknowledged cleanup command`
- `consensus update touches track %d, which has a promoted unsafe part waiting for something other than the copy floor`
- `consensus update touches track %d, which has an unacknowledged rejected-part cleanup`

A promoted unsafe part is floor-held, and so not in flight, exactly when it belongs to an acknowledged (`pp.Acked`) promotion with no pending cleanup whose `floorLocked(pp).FloorOnly` holds. Unacknowledged promotions, scheduled cleanups, rejected-part cleanups and parts waiting for anything else stay in flight.

**Drain views.**
- `DrainView` gains `Narrowed bool`, and `TrackDrainView` gains `Touched bool`.
- `DrainViewFor(draft)` sets `Refusal = drainRefusalForUpdateLocked(draft.SIIndexers)`, `Narrowed = narrowedDrainOnLocked()`, and `Touched` for the touched tracks when narrowed.
- `DrainView()` keeps S3-A §4's meaning: `Refusal` is the stage-2 gate, and `Narrowed` and every `Touched` are false. S3-A's `TestDrainViewRefusalEqualsTheDrainGate` stays green unchanged.

**Holds** (`server/consensus_hold.go`, S3-A Task 8).
- `consensusHold` gains `draft arbiter.ConsensusParamsUpdate`, the normalized draft of the `PrepareConsensusUpdate` request.
- `(*ConsensusHolds).start(mode pb.ConsensusHoldMode, term uint64, timeout time.Duration, draft arbiter.ConsensusParamsUpdate, view fsm.DrainView)`.
- `func (s *Server) drainViewFor(draft arbiter.ConsensusParamsUpdate) fsm.DrainView` returns `s.holdView()` when the test seam is set, else `s.d.FSM.DrainViewFor(draft)`. `drainView()` stays for callers without a draft.
- Readiness and `drain_refusal` of both modes come from `drainViewFor(hold.draft)`.
- A DRAIN hold over a `Narrowed` view records frontiers only for touched, non-quarantined tracks, and its readiness covers only them.
- No proto change: `TrackDrain` reports every track as before.

**arbiter-admin** (`cmd/arbiter-admin/main.go`, S3-A's hold code).
- `holdName` now defaults to `""`, with help text verbatim: `consensus hold before signing: none, issuance or drain (default: issuance once the leader reports stage3.narrowed_drain and the update touches no track, else drain)`.
- `func defaultHoldMode(state *pb.ConsensusParamsState, committed, target []arbiter.SIIndexerEntry) (pb.ConsensusHoldMode, string)` resolves `""` after the parameters read. Its reasons, verbatim:
  - `the leader reports stage3.narrowed_drain and the update touches no track`
  - `the leader does not report stage3.narrowed_drain`
  - `the update touches track %s` (ids comma-separated)
- It prints `info: --hold defaults to <issuance|drain>: <reason>` on stderr.
- `func touchedIndexers(committed, target []arbiter.SIIndexerEntry) []uint64` mirrors `fsm.TouchedTracks`, because arbiter-admin does not import `fsm`.
- An explicit `--hold` is parsed by `parseHoldMode` exactly as before.

**Named tests.**
- **fsm:** `TestTouchedTracksAreTheChangedSIIndexersEntries`, `TestNarrowedDrainGateRefusesOnlyTouchedTracksWork`, `TestNarrowedDrainGateIgnoresFloorOnlyHeldParts`, `TestNarrowedDrainGateCountsPartsHeldForAnotherReason`, `TestNarrowedDrainSwitchOnIsJudgedByTheStage2Gate`, `TestNarrowedDrainSkipsQuarantinedTouchedTracks`, `TestAnUpdateThatTouchesNoTrackPassesWithInFlightWork`, `TestDrainViewForEqualsApplysDrainRefusal`, `TestDrainViewKeepsTheStage2Refusal`, `TestDrainViewForBeforeTheSwitchIsTheStage2Gate`.
- **server:** `TestIssuanceHoldIsReadyForAnUpdateThatTouchesNoTrack`, `TestHoldReadinessFollowsTheDraft`, `TestDrainHoldRecordsOnlyTouchedTracksAfterTheNarrowedDrainSwitch`.
- **arbiter-admin:** `TestConsensusUpdateDefaultsToIssuanceOnceTheLeaderReportsNarrowedDrain`, `TestConsensusUpdateDefaultsToDrainBeforeNarrowedDrain`, `TestConsensusUpdateDefaultsToDrainForAnUpdateThatTouchesATrack`, `TestExplicitHoldFlagOverridesTheDefault`, `TestTouchedIndexersMatchesTheFSM`.

### 55. Integration harness, frozen fixture and the P5 arbiter release (Task 78)

**Frozen history.**
- **Files and names:** `fsm/pre_stage3_golden_test.go` with `const preStage3GoldenPath = "testdata/pre_stage3_golden.json"`, `func runPreStage3History(t *testing.T) legacyGolden` (the type of `fsm/legacy_lane_golden_test.go:32`) and `func TestPreActivationHistoryIsByteIdenticalAcrossStage3(t *testing.T)`, in the `pre_tracks_golden_test.go:404-432` pattern.
- **History:** a post-tracks network that enrols a third indexer, promotes and publishes on every track, quarantines and releases one, cleans a rejected block, and updates the verifiers under the stage-2 drain gate. It carries no stage-3 field.
- **Capture:** `ARBITER_CAPTURE_PRE_STAGE3_GOLDEN=1` captures it once, in a detached worktree at `ARBITER_TAG_P4` with the test file copied in uncommitted, and the JSON is committed on the branch.
- **Refusals,** verbatim: `%s exists; it is captured once from the release before stage 3 and never rewritten` and `pre-activation history replays differently from the release before stage 3:\n%s`.
- **Assertions:** `SnapshotVersion == snapshotVersionV20` and the answer and block counts of the capture (recorded in Appendix B).

**Fake-cluster integration** (`integration/stage3_p5_test.go`, new): `TestStage3ActivationWithEverySwitchOffChangesNothingButTheEncoding`, `TestStage3ActivationExemptsAQuarantinedTracksSNode`, `TestStage3ActivationRefusesAnUnupgradedDataPlaneNode`, `TestStage3VotersRestartFromV21Snapshots`, `TestStage3ForcedPlainBodyAfterActivation`, `TestNarrowedDrainUpdateCommitsWhileTheFloorHoldsACleanup`. With `ARBITER_WRITE_V21_FIXTURE=<path>`, `TestStage3VotersRestartFromV21Snapshots` also writes the leader's v21 container to `<path>`.

**chpipeline harness** (`integration/chpipeline/harness_stage3_test.go`, `integration/chpipeline/stage3_test.go`, new).
- **Harness:** `type stage3Harness struct`, `func startStage3Harness(t *testing.T) *stage3Harness`, `func (h *stage3Harness) startConsensusMember(t *testing.T, ownerIndex int, nodeID string)`, `func (h *stage3Harness) update(t *testing.T, mutate func(*arbiter.Stage3Params))`, `func (h *stage3Harness) switchOn(t *testing.T, step string)`.
- **Steps:** `activation`, `purge`, `eviction_voiding`, `owner_aware_selection`, `members_listed`, `placement`, `members_ready`, `reselection`, `checkpoints`, `narrowed_drain`, `raise_k`.
- **Member ids:** `chMemberA = "member-a"`, `chMemberB = "member-b"`, `chMemberC = "member-c"`.
- **Tests:** `TestPipeline_Stage3SwitchOnInOrder` (one subtest per step), `TestPipeline_Stage3FloorAtKTwoHoldsCleanupUntilAMemberCheckpoints`, `TestPipeline_Stage3ReselectionOnAQuietTrack`, `TestPipeline_Stage3EvictionVoidingKeepsALatchedVerdict`, `TestPipeline_Stage3KillOneMemberMidStream`, `TestPipeline_Stage3OneIndexerDownStallsOnlyItsTrack`, `TestPipeline_Stage3QuarantinedOwnersCopiesStayServed`.
- **Environment:** opted in by `ARBITER_CH_STAGE3=1`, after which missing prerequisites fail: `CH_STAGE3_ADDRS` (three `host:port`, comma-separated, servers a, b, c) and `CH_STAGE3_KEEPER` (`host:port`).

**CI** (`.github/workflows/ci.yml`).
- New job `integration-clickhouse-stage3`, added to the final `needs`. It runs `bash scripts/ci/stage3-pipeline.sh clickhouse/clickhouse-server:26.3.37.3`, which starts three servers and one Keeper from `scripts/ci/stage3-clickhouse-config.xml` (`table_engines_require_grant`, `keep_free_space_bytes`, disk and policy `member`, `<replicated_merge_tree><max_bytes_to_merge_at_max_space_in_pool>0`), `scripts/ci/stage3-clickhouse-users.xml` and `scripts/ci/stage3-keeper.xml`. The users file holds the protocol users with S3-C3's by-name engine grants, `si_member` with exactly S3-C2 §23.2's `SI_MEMBER_GRANTS` and `si_member_ddl` with exactly `SI_MEMBER_DDL_GRANTS` and its profile constraint. On `hg_rstage.*` that is `SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE`, never `ALL`, so no dictionary, view, `SYSTEM` or definer right (decision D36).
- It requires the log lines `--- PASS: TestPipeline_Stage3SwitchOnInOrder `, `--- PASS: TestPipeline_Stage3KillOneMemberMidStream ` and `--- PASS: TestPipeline_Stage3OneIndexerDownStallsOnlyItsTrack `. The 25.8 job is unchanged.

**Offline replay.**
- **Directory (decision D37):** `R5="$HOME/si-devnet2-multisource-s3e-rehearsal"` (mode 700) is the one P5 rehearsal directory, S3-E2 §48's `$R`.
  - **Who writes it:** Task 78 creates it fresh with mode 700 if it does not exist. If S3-E2 Task 67 already made it, Task 78 continues it after checking mode 700 and `old_tag.txt`. There is never a second P5 rehearsal directory. Task 78 writes everything in it that the P5 runbook runs, and the runbook only reads it.
  - **Tags:** `old_tag.txt` (`ARBITER_TAG_P4`) and `new_tag.txt` (`ARBITER_TAG_P5`, written by the rebuild after the cut).
  - **Binaries:** `bin/old/{statedigest,v20check,snapdump}`, built from `ARBITER_TAG_P4`. `bin/new/{statedigest,v21check,snapdump}`, built from the branch head with S3-E2's `rehearsal_build new` and rebuilt from the `ARBITER_TAG_P5` export after the cut.
  - **Rehearsal:** `p5rehearsal/p5rehearsal_test.go` and `bin/new/p5rehearsal` (§57), built from the `ARBITER_TAG_P5` export after the cut.
  - **Checksums:** `p5-files.sha256`, written last by Task 78: the `shasum -a 256` lines of `p5rehearsal/p5rehearsal_test.go` and of every file under `bin/old` and `bin/new`, with paths relative to `$R5`.
- **`v21check`:** S3-E2 §48's `$R5/v21check/main.go`, the same tool in the same directory, never a second definition. A successful restore prints `restore_ok version <n> body <none|plain|interned>`. A refused restore prints `restore_refused version <n>: <error>` and exits non-zero.
- **Replay:** fresh read-only copies of the three voters' `raft` directories (`si_kubectl exec … tar`) replay to identical `statedigest` lines (`snapshot_sha256` included) under old and new, with `written_version 20`.
- **v21 fixture:** `"$R5/bin/old/v20check" "$R5/v21-fixture.container"` prints `restore_refused version 21: unsupported snapshot version 21 …`, and `"$R5/bin/new/v21check"` prints `restore_ok version 21 body interned`.
- **Release files:** `$R5/merge-commit` and `$R5/arbiter-release` (line 1 tag, line 2 digest), written by `$R5/cut-release.sh`, which follows the S3-A Task 11 Step 12 pattern.

**Release.**
- **PR:** one PR of `urwt/claude/multi-source-s3e` (Tasks 64–78) to arbiter `main`, titled `multi-source SI stage 3 P5: snapshot v21, groups, owner-aware selection, checkpoints, copy floor, narrowed drain (every switch off)`, body file `~/src/remotesrc/claude/arbiter/multi-source-s3e.pr-body.md`. Before it merges, `ARBITER_TAG_P4` must be on `main` (S3-D) and the branch must contain it.
- **Merge and cut:** squash merge, then `gh workflow run cut-release.yml --repo sentioxyz/arbiter --ref main`. The tag is recorded from `gh release view`, never predicted. `ARBITER_DIGEST_P5` is the `docker buildx imagetools inspect ghcr.io/sentioxyz/arbiter:$ARBITER_TAG_P5` digest, and its `org.opencontainers.image.revision` must equal the tag's commit.

### 56. sentio-node and the production chart (Tasks 79–80)

**sentio-node (Task 79).**
- **Pins:**
  - housegate `HG_TAG_P5` and arbiter-core `CORE_TAG_P5`, three times each (`go.mod` require; `MODULE.bazel` `bazel_dep` without the leading `v`; the `git_override` commit with its `# Resolved …` comment, the commit checked with `git -C <dep> rev-parse <TAG>^{commit}`);
  - arbiter-proto `PROTO_TAG_P5`, in `go.mod` only, replacing the `PROTO_TAG_P4` pin of `SN_COMMIT_P4` (decision D26).

  Every SNode and si-member then advertises `stage3_v1` through `arbiter.LocalNodeFeatures()`, and the embedded ingress answers codes 12 and 13 retryably.
- **Command:** `sentio-node storage-integrity member-consent-statement --member-node-id ID --member-ed25519-pubkey HEX --member-replica-signer 0x…`, added in `commands/storage_integrity.go:13-20` by `newMemberConsentStatementCommand(configPath *string) *cobra.Command`. It is backed by:
  - `type MemberConsentRequest struct{ NodeID string; Ed25519Pubkey []byte; ReplicaSigner string }`;
  - `func PrintStorageIntegrityMemberConsentStatement(ctx context.Context, cfg *config.Config, req MemberConsentRequest, out io.Writer) error`;
  - `func writeStorageIntegrityMemberConsentStatement(out io.Writer, signer *authority.Signer, stmt authority.GroupMemberConsentStatement) error`;

  all in `standalone/storage_integrity_member_consent.go`, new, in the `storage_integrity_enrollment_statement.go:17-68` pattern.
- **Statement:** signed with `storageIntegrityClaimSigner(&cfg.Node.PrivateKey)`. `NetworkID` and `GenesisSnapshotID` come from `storage_integrity.snode`, and `HostIndexerID` from `IndexerRegistry.GetIndexerBySigner`. The command verifies its own JWS before printing; stdout carries only the JWS and a newline, and logs go to stderr.
- **Refusals,** verbatim:
  - `member-consent-statement requires an indexer node config (node.node_type: indexer): the statement is signed with the indexer key`
  - `member-consent-statement requires storage_integrity.enabled: the statement binds storage_integrity.snode.network_id`
  - `member-consent-statement requires storage_integrity.snode.genesis_snapshot_id: the statement binds the network's genesis snapshot id`
  - `--member-node-id %q equals this host's SNode node id; a member needs its own node id`
  - `--member-node-id must be non-empty and free of whitespace and control characters`
  - `--member-ed25519-pubkey must be 32 bytes of lowercase hex, got %d bytes`
  - `--member-replica-signer must be a non-zero lowercase 0x-prefixed 20-byte address`
- **Member consensus mode, inert.** It adds the type below as `config.StorageIntegrityMemberConfig.Consensus` (S3-C2 §24.2's `storage_integrity.member` block, loaded by `config.LoadSIMemberConfig`) with tag `yaml:"consensus"`, and `StorageIntegrityMemberConfig.Reconciler StorageIntegrityMemberReconciler` with tag `yaml:"reconciler"` (`type StorageIntegrityMemberReconciler struct{ ClickhouseUsername string \`yaml:"clickhouse_username"\` }`), all off and empty by default:
  ```go
  type StorageIntegrityMemberConsensus struct {
  	Register             bool   `yaml:"register"`
  	CreateReplicas       bool   `yaml:"create_replicas"`
  	Ed25519SeedFile      string `yaml:"ed25519_seed_file"`
  	ReplicaSignerKeyFile string `yaml:"replica_signer_key_file"`
  	ConsentFile          string `yaml:"consent_file"`
  }
  ```
  - **File formats:** the seed file holds 64 lowercase hex characters (a 32-byte ed25519 seed) with an optional newline, the key file a secp256k1 private key as 64 hex characters, and the consent file one compact JWS. The user makes the `replica_signer` key in their own terminal, with Foundry `cast wallet new` or an equivalent they prefer (decision D40), and puts it into a Secret. `README.md` says to drop the `0x` prefix `cast` prints. No task of this plan makes a member key.
  - **Loader:** `func loadMemberConsensusConfig(c config.StorageIntegrityMemberConsensus, cfg *config.SIMemberConfig) (*consensus.Config, error)` (in `simember/consensus_keys.go`, new; S3-E1 §42.3's `consensus.Config`, set as `member.Config.Consensus`) reads no file while `Register` is false and returns nil. Once it is true, the consent must verify under the host's `si_indexers` signer (read from the registry snapshot) for exactly {network, genesis, host id, member node id, the seed's public key, the key's address}.
  - **Refusals,** verbatim:
    - `storage_integrity.member.consensus.ed25519_seed is not read from the config file; mount the seed as a file and set storage_integrity.member.consensus.ed25519_seed_file` (`config.LoadSIMemberConfig`'s key walk, any value)
    - `storage_integrity.member.consensus.replica_signer_key is not read from the config file; mount the key as a file and set storage_integrity.member.consensus.replica_signer_key_file`
    - `storage_integrity.member.consensus.register requires ed25519_seed_file, replica_signer_key_file and consent_file`
    - `storage_integrity.member.consensus.create_replicas requires storage_integrity.member.consensus.register`
    - `storage_integrity.member.consensus.%s %s must not be readable by group or others (mode %#o)`
    - `storage_integrity.member.consensus.consent_file does not bind this member's keys: %v`
- **Reconciler process.** `sentio-node si-member --config-path <path> --role reconciler` (flag `--role member|reconciler`, default `member`, on S3-C2's `commands.NewSIMemberCommand`) runs `consensus.NewUnsafeReconciler` (S3-E1 §42.3) as `storage_integrity.member.reconciler.clickhouse_username` with the password from `STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD` (`config.StorageIntegrityMemberDDLClickhousePasswordEnv`, env only), `CreateReplicas` from `storage_integrity.member.consensus.create_replicas`, and the member state directory read-only for `quiescence.json`; it serves its two series on `metrics_listen`. Refusals, verbatim: `storage_integrity.member.reconciler.clickhouse_password is not read from the config file; set STORAGE_INTEGRITY_MEMBER_DDL_CLICKHOUSE_PASSWORD instead`; `--role reconciler requires storage_integrity.member.reconciler.clickhouse_username`; `--role reconciler requires storage_integrity.member.consensus.register: a member creates hg_member_unsafe replicas only while it is registered`.
- **Metrics wiring.** `simember.Run` registers `role.ConsensusMetrics()` (S3-E1 §42.3) on the si-member registry; the host's runtime exports `tableset.Stats.KeeperFindings` as `sentio_node_storage_integrity_keeper_findings{reason}` (S3-E1 §42.1, both reasons always exported) on the embedded HouseGate registry (`:9091`). It does so through the existing `storageIntegrityReconcilerCollector` (`sentio-node:standalone/storage_integrity_reconciler_metrics.go`, registered by `registerMetrics` in `standalone/storage_integrity_table_state.go:203-217`), which already reads `TableSetStats()`.
- **`replica_hosts`.**
  - **Function:** `func ReplicaHostsFromGroups(snap wire.TableRegistrySnapshot, owner uint64, database string) (hosts []uint64, placed bool)` in `storageintegrityadapter/tablestate/replica_hosts.go`, new.
  - **Before placement:** `placed` is false when the snapshot has no group for (owner, database), and S3-C4's static list (K50) is served.
  - **After placement:** the sorted, distinct `HostIndexerID` values of the group's `Members` that are `Ready()` (S3-E1's `wire.GroupMember.Ready`). Each is the member's served `host_indexer_id` (decision D27), read from the registry snapshot itself. Nothing maps node ids to hosts through `verifiers` entries or the consensus parameters. `Leaving` records are never candidates.
- **Named tests:** `TestLoadSIMemberConfigRefusesAnInlineMemberKey`, `TestStorageIntegrityMemberConsensusValidation`, `TestMemberConsensusModeIsOffByDefault`, `TestMemberKeyFilesMustNotBeGroupOrWorldReadable`, `TestMemberConsentFileMustBindTheMembersKeys`, `TestMemberConsentStatementPrintsOnlyTheJWS`, `TestMemberConsentStatementBindsTheMembersPublicValues`, `TestMemberConsentStatementRefusesTheHostsSNodeNodeID`, `TestMemberConsentStatementRefusesMalformedPublicValues`, `TestPinnedArbiterCoreAdvertisesStage3V1`, `TestEmbeddedIngressAnswersStage3AdmissionCodesRetryably`, `TestReplicaHostsKeepTheStaticListBeforePlacement`, `TestReplicaHostsFollowTheDatabaseGroupOncePlaced`, `TestReplicaHostsSkipAMemberNotReadyAtTheCurrentGeneration`, `TestSIMemberReconcilerRoleRunsTheUnsafeReconcilerAsItsOwnUser`, `TestSIMemberReconcilerRoleRefusals`, `TestSIMemberRegistersTheConsensusMetrics`, `TestKeeperFindingsAreExportedWithEveryReason`.
- **Release:**
  - **Branch:** worktree `~/src/remotesrc/claude/sentio-node/multi-source-s3e4`, branch `urwt/claude/multi-source-s3e4`, PR title `feat(storage-integrity): multi-source SI stage 3 P5 host side (stage3_v1, member consent, member keys)`.
  - **Values:** `SN_COMMIT_P5` is the squash-merge commit on `main`; `SN_DIGEST_P5` is the index digest of `ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT_P5>`, checked against its revision label.

**production chart (Task 80).**
- **Snapshot body:** `arbiter.raft.snapshotBody` (default `""`; allowed `auto`, `plain`) renders `snapshot_body: <value>` as the last key of the `raft:` block of `arbiter.yaml.tmpl` (`charts/storage-integrity/templates/arbiter.yaml:18-35`), only when set. Guards, verbatim: `arbiter.raft.snapshotBody must be auto or plain`; `arbiter.raft.snapshotBody requires arbiter.protocol=pinned-genesis-v1`.
- **Monitoring values:** `monitoring.stage3Consensus.enabled` (false), `.heldCleanupAgeWarnSeconds` (3600), `.runbookURL` (`https://github.com/sentioxyz/production/blob/main/docs/storage-integrity-devnet2-stage3-p5-v21.md`). Guards, verbatim: `monitoring.stage3Consensus.enabled requires monitoring.enabled`; `monitoring.stage3Consensus.heldCleanupAgeWarnSeconds must be a positive integer`.
- **Rule group** `<fullname>.arbiter-stage3`, rendered only when enabled. The alerts form test constant `STAGE3_CONSENSUS_ALERTS`, appended to `MONITORING_ALERTS`; every expression selects `network="<network>"`:
  - `ArbiterSnapshotEncodingFallback` (critical): `increase(arbiter_snapshot_encoding_fallback_total[30m]) > 0`.
  - `ArbiterBlockSealGroupSwitch` (warning): `increase(arbiter_block_seal_reason{reason="group_switch"}[1h]) > 0`.
  - `ArbiterVerifiersUnavailable` (warning): `increase(arbiter_admission_rejects_total{code="ADMISSION_CODE_VERIFIERS_UNAVAILABLE"}[15m]) > 0`.
  - `ArbiterCopiesBackpressure` (warning): `increase(arbiter_admission_rejects_total{code="ADMISSION_CODE_COPIES_BACKPRESSURE"}[15m]) > 0`.
  - `ArbiterUnderReplicatedCleanups` (warning): `increase(arbiter_under_replicated_total[1h]) > 0`.
  - `ArbiterGroupsUnderReplicated` (warning, for 30m): `max(arbiter_under_replicated_groups) > 0`.
  - `ArbiterFloorHeldCleanupAgeHigh` (warning, for 15m): `max(arbiter_floor_held_cleanup_age_seconds) > <heldCleanupAgeWarnSeconds>`.
  - `ArbiterMemberLagging` (warning, for 30m): `max(arbiter_group_members{class="lagging"}) > 0`.

  Each carries `runbook_url` `<stage3Consensus.runbookURL>#11-operating-rules`.
- **test_render.py constants:**
  - `DEVNET2_STAGE3_P5_ARBITER_DIGEST` (= `ARBITER_DIGEST_P5`);
  - `DEVNET2_STAGE3_P5_SOURCE_IMAGE` (`ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT_P5>@<SN_DIGEST_P5>`, appended to `WRITER_AUTHORIZATION_IMAGES` and `STAGE3_HOST_IMAGES`);
  - `STAGE3_P5_HOST_IMAGES = frozenset({DEVNET2_STAGE3_P5_SOURCE_IMAGE})`;
  - `DEVNET2_SNAPSHOT_BODY = None`;
  - `DEVNET2_STAGE3_CONSENSUS_MONITORING = False`;
  - `P5_REHEARSAL_POD = "docs/examples/storage-integrity-p5-rehearsal-pod.yaml"`.
- **Rehearsal Pod** (`docs/examples/storage-integrity-p5-rehearsal-pod.yaml`, new). Pod `storage-integrity-si-v2-p5-rehearsal` in `sentio-network-devnet2`, carrying only the label `storage-integrity.sentio.xyz/rehearsal: p5`, which no Service selects.
  - **Container:** `rehearsal`, image `ghcr.io/sentioxyz/arbiter@<ARBITER_DIGEST_P5>`, command `["sleep", "86400"]`, env `ARBITER_AUTHORITY_PRIVATE_KEY_HEX` from Secret `storage-integrity-devnet2-si-v2-authority` key `private-key`.
  - **Storage:** emptyDir `rehearsal` (`sizeLimit: 8Gi`) at `/rehearsal`.
  - **Pod settings:** limits cpu `"2"` and memory `2Gi` (the voters' sizing), `restartPolicy: Never`, `automountServiceAccountToken: false`, no other volume, port or Secret.
- **Tests:**
  - `Stage3ConsensusMonitoringTest`: `test_stage3_consensus_rules_render_only_when_enabled`, `test_stage3_consensus_alert_names_are_the_reviewed_set`, `test_stage3_consensus_rules_read_only_this_networks_series`, `test_stage3_consensus_guards_fail_closed`.
  - `SnapshotBodyTest`: `test_snapshot_body_renders_only_when_set`, `test_snapshot_body_guard_fails_closed`, `test_unset_snapshot_body_renders_unchanged`.
  - In `ActualFreshNetworkTest`: `test_snapshot_body_needs_a_stage3_arbiter` (set only while `DEVNET2_ARBITER_DIGEST == DEVNET2_STAGE3_P5_ARBITER_DIGEST`), `test_stage3_consensus_monitoring_needs_a_stage3_arbiter`, `test_devnet2_stage3_p5_images_follow_the_rollout`, `test_stage3_p5_host_images_are_reviewed`, `test_p5_rehearsal_pod_mounts_only_the_authority_secret`.
- **Voter sizing:** `test_devnet2_voter_sizing_is_pinned` passes unchanged; the voters stay at 2Gi.

### 57. P5 runbook, devnet2 activation, release ledger and commands (Tasks 81–82)

**Runbook `docs/storage-integrity-devnet2-stage3-p5-v21.md`.** Every top-level section is `## <n>. <title>` with exactly these titles; live tasks and `runbook_url` cite them:
- §0 Session helpers
- §1 Preparation: §1.1 Pins, §1.2 Identities and the stage-3 parameters, §1.3 Preconditions and baseline
- §2 Offline replay check: §2.1 Rehearsal binaries, §2.2 Baseline, §2.3 Replay
- §3 Rehearsal: §3.1 Copies, §3.2 Rehearsal pod, §3.3 Activation and restarts from v21, §3.4 Forced mismatch, §3.5 The previous release refuses v21, §3.6 Cleanup
- §4 Verifiers
- §5 Hosts: §5.1 indexer-b, §5.2 indexer-c, §5.3 indexer-a, §5.4 si-members
- §6 Voters
- §7 Activation: §7.1 Re-register the data plane, §7.2 Activate, §7.3 Verify
- §8 Monitoring and snapshot v21
- §9 End
- §10 Rollback: §10.1 Before §7.2, §10.2 After §7.2
- §11 Operating rules

**Names.**
- `si_backup="$HOME/si-devnet2-stage3-p5"` holds every output of the runbook, the live rehearsal's included.
- `R5="$HOME/si-devnet2-multisource-s3e-rehearsal"` is the one P5 rehearsal directory, Task 78's, and the runbook only reads it (decision D37). §2.1 builds nothing. It stops unless all of these hold:
  - `$R5` is mode 700;
  - `$R5/old_tag.txt` and `$R5/new_tag.txt` equal the ledger's `ARBITER_TAG_P4` and `ARBITER_TAG_P5`;
  - `$R5/arbiter-release` holds `ARBITER_TAG_P5` and `ARBITER_DIGEST_P5`;
  - `shasum -a 256 -c p5-files.sha256`, run in `$R5`, passes.

  It tees the result into `$si_backup/rehearsal-binaries.txt` and copies `p5-files.sha256` to `$si_backup`. No block writes, rebuilds or deletes anything under `$R5`.
- §0 defines the helpers of §0 of `storage-integrity-devnet2-dynamic-table-set.md`, `storage-integrity-devnet2-multi-source.md`, `storage-integrity-devnet2-multi-source-tracks.md`, `storage-integrity-devnet2-stage3-p0.md`, `storage-integrity-devnet2-stage3-lockdown.md`, `storage-integrity-devnet2-stage3-indexer-c.md`, `storage-integrity-devnet2-stage3-p1-backups.md`, `storage-integrity-devnet2-stage3-p2-members.md`, `storage-integrity-devnet2-stage3-p3-replica-reads.md`, `storage-integrity-devnet2-stage3-p4-hardening.md` and `storage-integrity-devnet2-stage3-p4-owner-rebuild.md`, in that order and unchanged (so Plan S3-F's chain, which starts here, reaches the P2 and P3 member helpers), then its own in one bash block (D20); none of its own helpers redefines an earlier one. Its own helpers:
  - `si_stage3`: the leader's committed `stage3` from `consensus show`, as one JSON line, `null` before the activation.
  - `si_features_stage3`: the leader's feature book, with voter ids and, per node, `stage3: true|false`.
  - `si_v21check <ordinal>`: copies the voter's newest `state.bin` to `$si_backup/state-<ordinal>.bin`, then prints `arbiter-<ordinal> version <byte> body <byte>` and `$R5/bin/new/v21check`'s line, or `arbiter-<ordinal> no snapshot yet`.
  - `si_encoding <ordinal>`: the voter's `arbiter_snapshot_encoding_fallback_total` and `arbiter_snapshot_body_bytes{kind=…}` exposition lines.
  - `si_rehearsal <cmd…>`: `si_kubectl exec storage-integrity-si-v2-p5-rehearsal -c rehearsal -- <cmd…>`.
  - `si_member_sts <a|b|c>`: prints `sentio-node-devnet2-indexer-<x>-si-member`.
  - `si_p5_image`: `ghcr.io/sentioxyz/sentio-node:sha-<line 3>@<line 4>` of `$si_backup/pins`.
- `$si_backup/pins`, one value per line: `ARBITER_TAG_P5`, `ARBITER_DIGEST_P5`, `SN_COMMIT_P5`, `SN_DIGEST_P5`, `ARBITER_TAG_P4`, `ARBITER_DIGEST_P4`, `SN_COMMIT_P4`, `SN_DIGEST_P4`, all from the ledger copy.

**Stage-3 parameters of the activation** (binding, restated in §1.2 and in `$si_backup/stage3-params.json`). Every field keeps its spec default except where a value is given:
- `group_size` 3, `network_fill_default` true, `min_safe_copies` 1, `strict_copies` false, `exclude_quarantined_hosts` false, every switch false;
- `indexer_failure_domains` from `DEVNET2_FAILURE_DOMAINS` for indexers 0, 1 and `INDEXER_C_ID`;
- `max_lag_publications` 64, `lag_low` 16;
- `evidence_timeout_l2_blocks` 300 (about 10 advances of the registry cursor at `cursorAdvanceInterval: 1m`, `charts/storage-integrity/values.yaml:156`);
- `max_uncleaned_parts_per_partition` 2000, below the hosts' `softPartsPerPartition` 2400 (`housegate:pkg/config/storage_integrity_config.go:241-242`);
- `max_uncleaned_parts_per_table` 20000.

**The activation command (§7.2)**, run inside the leader's arbiter container:
```bash
si_arbiter "$leader" /usr/local/bin/arbiter-admin consensus update --address "$(si_voter "$leader")" \
  --expected-epoch "$epoch" --authority 0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705 --max-writers 3 \
  --stage3 "${domains[@]}" --stage3-max-lag-publications 64 --stage3-lag-low 16 \
  --stage3-evidence-timeout-l2-blocks 300 --stage3-max-uncleaned-parts-per-partition 2000 \
  --stage3-max-uncleaned-parts-per-table 20000 \
  --voter "$(si_voter 0)" --voter "$(si_voter 1)" --voter "$(si_voter 2)" \
  --hold drain --hold-timeout 10m --timeout 20s 2> "$si_backup/activate.err" | tee "$si_backup/activate.out"
```
- `domains` holds one `--indexer-failure-domain id=<N>,domain=<D>` pair per entry of `DEVNET2_FAILURE_DOMAINS`.
- `--si-indexer` and `--verifier` are omitted, so the committed lists are carried.
- The block stops unless `show-before-activation.json` holds exactly this authority and `max_writers` 3, `stage3` is null, every track is unquarantined, and every node of `si_features_stage3` has `stage3: true`.

**Rehearsal (K81).**
- **Source:** `$R5/p5rehearsal/p5rehearsal_test.go` is package `fsm` and throwaway. Task 78's step is the source of its text and also builds it: Task 78 copies it into the `ARBITER_TAG_P5` export as `fsm/p5rehearsal_test.go` and builds it with `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -c -o "$R5/bin/new/p5rehearsal" ./fsm`. Runbook §3.2 reproduces the text verbatim for review and gives its SHA-256, which must equal the `p5-files.sha256` line §2.1 checked. The runbook never builds it (decision D37).
- **Inputs:** `$R5/bin/new/p5rehearsal`, `$si_backup/stage3-params.json` and the three copies `$si_backup/rehearsal-copy-<i>.tgz` are copied into the Pod's `/rehearsal`.
- **Run:** `si_rehearsal /rehearsal/p5rehearsal -test.run '^TestP5Rehearsal$' -test.v -rehearsal.dir /rehearsal -rehearsal.params /rehearsal/stage3-params.json`.
- **Output lines** (the runbook's Expected):
  - `voter <i> restored epoch=<e> promotion_seq=<p> drain_refusal=<"" or the gate's text>`
  - `digest <i> <sha256>` (three equal)
  - `activation applied epoch=<e+1>`
  - `persist <i> version 21 body interned bytes=<n> fallback=0 persist_peak_heap_bytes=<n>`
  - `restart <i> restore_ok version 21 body interned digest_equal=true`
  - `mismatch <i> version 21 body plain fallback=1 digest_equal=true`
  - `rehearsal ok`
- **Previous-release check (§3.5):** `"$R5/bin/old/v20check" "$si_backup/rehearsal-v21-0.container"` must print `restore_refused version 21: unsupported snapshot version 21 …`.
- **Cleanup:** the Pod is deleted in §3.6, before §4.

**Evidence files** (under `$si_backup`, never under `$R5`): `pins`, `rehearsal-binaries.txt`, `p5-files.sha256`, `stage3-params.json`, `baseline-p5.txt`, `replay-baseline`, `digest-<i>-{old,new}.txt`, `rehearsal-copy-<i>.tgz`, `rehearsal.txt`, `rehearsal-v21-0.container`, `rehearsal-old-refuses-v21.txt`, `rehearsal-pod-deleted.txt`, `stage3-p5-verifiers-commit`, `stage3-p5-indexer-{b,c,a}-commit`, `stage3-p5-member-{b,c,a}-commit`, `voter-order`, `features-before-activation.json`, `show-before-activation.json`, `activation-tip.txt`, `activation-voters.txt`, `activate.out`, `activate.err`, `show-after-activation.json`, `stage3-p5-monitoring-commit`, `v21-check-<UTC stamp>.txt`, `p5-end.txt`.

**Live commits** (Task 82; single-service, pushed as `HEAD:main` from the live worktree after `P5_PR` merges, each confirmed):
1. Verifiers: `arbiter.image.digest` = `ARBITER_DIGEST_P5` together with `DEVNET2_ARBITER_DIGEST`.
2. One commit per host, b then c then a: its source-image file = `DEVNET2_STAGE3_P5_SOURCE_IMAGE` together with `INDEXER_B_SOURCE_IMAGE`, `INDEXER_C_SOURCE_IMAGE` or `FRESH_SOURCE_IMAGE`.
3. One commit per si-member, b then c then a: its `siMember` image = the P5 sentio-node image, in P2/P3 mode with `storage_integrity.member.consensus` absent.
4. Monitoring, after §6: `monitoring.stage3Consensus.enabled: true` together with `DEVNET2_STAGE3_CONSENSUS_MONITORING`.
5. The docs-only record PR.

Voters are replaced with `si_replace_voter` in `voter-order`. A host restart gives its tables a short retryable `SOURCE_UNAVAILABLE` window and, in repair mode, a Pending hold until the startup audit is clean.

**Exit (`P5_END`).** `activate.out` holds `"status": "committed"`. `si_stage3` shows every switch false. Every voter's newest `state.bin` reads `version 21`, with `v21check` printing `restore_ok version 21 body interned` and `arbiter_snapshot_encoding_fallback_total 0`. The §8 block repeats until every voter has written a snapshot after the activation.

**Release ledger.** This plan's Appendix C table `| Name | Value | Recorded by |`, one row per name in this order, each written by the S3-C1 §19 procedure:

| Name | Value form | Recorded by |
|---|---|---|
| `ARBITER_TAG_P5` | `vX.Y.Z` | Task 78 |
| `ARBITER_DIGEST_P5` | `sha256:<64 hex>` | Task 78 |
| `SN_COMMIT_P5` | 40 hex | Task 79 |
| `SN_DIGEST_P5` | `sha256:<64 hex>` | Task 79 |
| `P5_PR` | PR number | Task 81 |
| `P5_REHEARSAL` | `YYYY-MM-DD`, the date the rehearsal printed `rehearsal ok` | Task 82 |
| `ACTIVATION_EPOCH` | uint, the `committed_epoch` of `activate.out` | Task 82 |
| `P5_END` | `YYYY-MM-DD`, the date of runbook §9 | Task 82 |

Run-record items: `arbiter P5 release` (Task 78), `sentio-node P5 release` (Task 79), `P5 PR` (Task 81), `P5 run and the v21 activation` (Task 82). Ledger record worktrees are URWT `multi-source-s3e4-ledger-<N>` of housegate.

**Repositories and commands.**
- **Local repositories:** `/Users/uranuswch/Dev/projects/sentio-network/<repo>`.
- **Worktrees** (URWT, `bash ~/.agents/skills/urwt/scripts/urwt-create.sh <repo-path> claude <name>`, whose JSON `path` is `$W`):
  - arbiter `multi-source-s3e` (consumed, S3-E2 §48);
  - sentio-node `multi-source-s3e4`;
  - production `multi-source-s3e4`, branch `urwt/claude/multi-source-s3e4` from `origin/main` after `P4_PR` merged, for Tasks 80–81;
  - production `multi-source-s3e4-live`, held at a detached, freshly fetched `origin/main` for Task 82 and checked clean before every commit.
- **Checks:**
  - arbiter: `bazel build //... && bazel test //...` and `bazel test --build_tests_only --@rules_go//go/config:race //...`; after dependency changes, `bazel mod tidy && bazel run //:gazelle`.
  - sentio-node: S3-B §15's checks.
  - production: S3-B §15's checks, then `bash scripts/storage-integrity/render-inertness.sh origin/main`, which must print `<release> identical` for every release.
- **P5 PR** (production): title `feat(storage-integrity): devnet2 stage 3 P5 (snapshot v21 activation runbook, monitoring, rehearsal pod)`, merged with the repository's convention after explicit confirmation.
- **Remote and live actions:** pushes, PRs, merges, release workflows, `helmfile sync`, `kubectl apply`/`delete`/`exec` that writes, and the consensus update. Each needs explicit user confirmation in chat for its own step; commands handed to the user are fish.

## Appendix B: drafting decisions and cross-task facts

Facts the drafters recorded while drafting and verifying each task in scratch exports. The plan consistency pass replaces this appendix with its final form; until then it is the drafters' record only. `<scratch>` stands for local verification directories that are not part of the plan.

### Task 76

- The drafter wrote this task but its summary was not recorded (the run hit a usage limit before it returned).

### Task 77

- arbiter origin/main is 526a721 (v0.13.0) with Plan S3-A merged: fsm/reads_drain.go, server/consensus_hold.go, server/consensus_hold_rpc.go and the arbiter-admin hold code exist there; line anchors of Task 77 cite 526a721, not fb42260 (where those files do not exist).
- At 526a721 the hold start signature is start(mode, term, draftEpoch uint64, timeout, view) (not S3-A contract's start(mode, term, timeout, view)); Task 77 implements contract §54's start(mode, term, timeout, draft, view) and derives draftEpoch from draft.ExpectedEpoch. Its only caller is PrepareConsensusUpdate.
- PrepareConsensusUpdate does not normalize its draft at 526a721; authorization hashes the normalized form (authority.ConsensusParamsUpdateHash normalizes), so Task 77's NormalizeConsensusParamsUpdate after authorization cannot fail in practice.
- After Task 77, server.drainView() has no caller; it is kept because contract §54 says so.
- The stage-2 gate resolves a promoted unsafe part's track through manifestTableOwnerLocked(tp.TableID), a promotion/cleanup through promotionTrackLocked, a rejected partition through blockTrackLocked; the narrowed gate uses the same resolvers, and unresolvable work counts for every touched track (fail-closed mirror of the stage-2 rule).
- verifySIIndexersTransition refuses removing an si_indexers entry before the drain gate runs, so TouchedTracks treats only added or changed (reflect.DeepEqual) entries as touched; a nil next touches none.
- A narrowed DRAIN hold records frontiers only for touched, non-quarantined tracks; untouched tracks' promotions are refused for the hold's life by S3-A's existing 'no recorded frontier' refusal, which keeps promotion_seq exact once ready; TrackDrain reports frontier 0 for them; no proto change.
- A part listed by two acknowledged promotions without pending cleanup stays in flight unless both are FloorOnly (refinement of contract §54's existential wording).
- DrainViewFor, once narrowed, also leaves floor-held-only promotions out of InFlightBlocks (Task 77 deliverable 'floor-only holds not in flight'); DrainView() never sets Narrowed/Touched so S3-A's reflect.DeepEqual drain tests pass unchanged.
- onStage3SwitchOnLocked contains Stage3SwitchNarrowedDrain twice in fsm/stage3.go overall (stage3SwitchValue also has a case), so Step 7's check greps the unique comment text.
- Floor-held parts need k >= 2, which the committed way needs member checkpoints (Task 76 precondition); Task 77's fsm tests set min_safe_copies 2 and strict_copies white-box after narrowed_drain (strict + no counting members => FloorOnly per contract §53), and publication via the existing setPublishedTrackFrontiers stand-in; no such FSM is restored.
- arbiter-admin default is resolved after the draft is signed, comparing wire.SIIndexerEntriesFromPB(request.Update.SiIndexers) (normalized) with currentSIIndexers; reasons joined with ', '.
- Every command block passes bash -n; shellcheck only reports SC2016 (info) for intentional single-quoted backticks; the gofmt check uses git ls-files -z | xargs -0 gofmt -l to avoid SC2046.
- Bazel cannot run offline here (bazel query --nofetch fails on missing external repos); gazelle's expected effect (two srcs lines in fsm/BUILD.bazel, no deps) was derived from fsm/BUILD.bazel deps at origin/main, which already contain every import of the new files.
- Scratch verification tree: <scratch> (arb = E3 task-73-r2 stand-in tree on 526a721 + a Task 76 floorLocked stand-in commit 4aee917; Task 77 commits 744cd4c, cc76f10, 6a56b84, ee147b3; generator gen77.py/hunks77.py; mdapply.py re-applies the markdown).
- Names added while drafting: fsm: drainViewLocked(refusal string, floorHeld map[arbiter.TablePartition]map[string]bool) DrainView; fsm: promotionFloorHeldLocked(seq uint64, pp *PendingPromotion, floorHeld map[arbiter.TablePartition]map[string]bool) bool; fsm: drainWorkBlocksLocked(floorHeld map[arbiter.TablePartition]map[string]bool) (signature change of the Plan S3-A helper; only caller drainViewLocked); fsm: the narrowed_drain case of onStage3SwitchOnLocked (comment only, no state change); server: Server.holdViewFor func(draft arbiter.ConsensusParamsUpdate) fsm.DrainView (tests-only seam, read before holdView); server: refusal `consensus hold draft: %v` (InvalidArgument, unreachable after authorization); server: consensusHold.draftEpoch is now set from draft.ExpectedEpoch inside start; arbiter-admin: holdDefault in run; explicit --hold "" refused by parseHoldMode; --hold-timeout accepted with the default; a default hold extends --timeout by --hold-timeout; fsm test helpers: narrowedFSM, holdFloorAtKTwoStrict, issueTrackPromotion, rejectTrackBlock, publishTrackEntry, rotationKey, touchingUpdate; constants narrowedPromotionRefusal0, narrowedPromotionRefusal1, narrowedCleanupRefusal1, narrowedPartRefusal1, narrowedRejectedRefusal0; server test helper: narrowedHoldTracks; arbiter-admin test helpers: defaultHoldArgs, stage3LeaderState, readyHoldLeader; README: paragraph **Narrowed drain (switch 9).** before the stage-3 activation section's **CLI.** paragraph; rewritten first sentence of Plan S3-A's **CLI.** paragraph; commit subjects: feat(stage3): narrowed drain gate and DrainViewFor, switch 9 (Task 77); feat(stage3): consensus holds judge the draft they were taken with (Task 77); feat(stage3): arbiter-admin defaults to an issuance hold after narrowed_drain (Task 77)
- Verified while drafting: Applied the task markdown itself (parsed Create/replace/append/insert blocks, mdapply.py) to a fresh export of the scratch base 4aee917; diff -r against the verified tree is empty (MD-IDENTICAL). / Red phases observed with the stated messages: fsm build fails with undefined TouchedTracks/floorHeldUnsafePartsLocked/DrainViewFor/Narrowed/Touched at the exact listed lines of the new test file; server build fails with fx.s.holdViewFor undefined x3; arbiter-admin build fails with undefined: touchedIndexers. / Green: all 10 fsm, 3 server and 5 arbiter-admin named tests pass with -race; GOWORK=off GOFLAGS=-mod=mod GOPROXY=off go vet ./... clean, gofmt clean, go test ./... -count=1 passes for every package including integration and integration/chpipeline. / go test -race ./fsm ./server ./cmd/arbiter-admin full packages pass; server hold tests -race -count=20 and arbiter-admin default tests -race -count=10 pass; Plan S3-A drain tests, goldens and TestDeterminism_* pass unchanged. / Mutation checks: 9 FSM mutations (call site reverted, floor ignored, shared-part exclusion removed, quarantine skip removed, in-flight exclusion removed, every track counts, unresolved work ignored, Touched never set, rejected parts ignored) each caught; 4 server mutations (Narrowed ignored in start, GetConsensusHold/Prepare using drainView, no normalization) caught; 2 arbiter-admin mutations (committed list as target, timeout not extended) caught. / Step 1 check block run against the base export prints 2,3,4,2,1,1,2,1,1,1, nothing, and go doc prints func (x *Stage3Params) GetNarrowedDrain() bool. / bash -n on all 4 bash blocks and 18 Run lines; shellcheck -S warning clean after replacing the gofmt $(...) form. / Heuristic check: no hard-wrapped prose lines outside code blocks; no placeholders (<line>, TBD, TODO) remain. / Bazel build/test not run (not offline-capable in this environment); the task keeps the repository's bazel build //... && bazel test race steps for the executor.

### Task 80

- Task 80 has 11 steps: worktree/base checks (remote reads confirmed), failing SnapshotBodyTest, snapshotBody implementation + commit, failing stage-3 monitoring tests, rule group implementation + commit, ledger read + registry checks (remote reads confirmed) + fill.py, failing P5 image/rollout/Pod tests, image sets + Pod example + commit, workflow paths + commit, local rehearsal of Task 82's commits (rollout.py forward and bites), full production checks. It pushes nothing; Task 81 opens P5_PR.
- arbiter.raft.snapshotBody (default "") renders `snapshot_body: <v>` as the last key of the raft block in templates/arbiter.yaml only when set; enters arbiterConfigChecksum only when set (tableRegistry pattern), so voters (OnDelete) take it on replacement and the verifier checksum never moves; schema requires arbiter.raft.snapshotBody as a string; validate.yaml refuses other strings ('arbiter.raft.snapshotBody must be auto or plain') and legacy ('... requires arbiter.protocol=pinned-genesis-v1').
- monitoring.stage3Consensus {enabled false, heldCleanupAgeWarnSeconds 3600, runbookURL .../docs/storage-integrity-devnet2-stage3-p5-v21.md} renders group <fullname>.arbiter-stage3 as the VMRule's last group (appended before monitoring.yaml's final end); 8 alerts, every runbook_url '#11-operating-rules'; schema makes stage3Consensus required and heldCleanupAgeWarnSeconds an integer; validate.yaml refuses enabled without monitoring.enabled and a non-positive age (checked whenever given).
- Rendered expressions: max by (cluster, network, pod) (increase(arbiter_snapshot_encoding_fallback_total{network=N}[30m])) > 0 critical; group_switch seal reason [1h]; arbiter_admission_rejects_total code=ADMISSION_CODE_VERIFIERS_UNAVAILABLE / ADMISSION_CODE_COPIES_BACKPRESSURE [15m]; arbiter_under_replicated_total by reason [1h]; arbiter_under_replicated_groups > 0 for 30m; arbiter_floor_held_cleanup_age_seconds > heldCleanupAgeWarnSeconds for 15m; arbiter_group_members{class=lagging} by owner for 30m.
- Decision recorded (S3-E2 left it to Task 80): ArbiterSnapshotDocNearReadCap stays on arbiter_snapshot_doc_bytes (the plain document), because the fallback and snapshotBody plain persist the plain body; a test pins it.
- arbiter_admission_rejects_total{code} exists at arbiter origin/main with the proto enum name as code (metrics/metrics.go:105, server tests), so the contract's code label values are right.
- arbiter decodes its config YAML leniently (config/config.go:278 yaml.Unmarshal), so an older release ignores raft.snapshot_body: test_snapshot_body_needs_a_stage3_arbiter allows it on devnet2 only while DEVNET2_ARBITER_DIGEST == DEVNET2_STAGE3_P5_ARBITER_DIGEST.
- Rehearsal Pod: storage-integrity-si-v2-p5-rehearsal in sentio-network-devnet2, only label storage-integrity.sentio.xyz/rehearsal: p5, container rehearsal on ghcr.io/sentioxyz/arbiter@<ARBITER_DIGEST_P5>, command [sleep, 86400] (debian bookworm-slim base, entrypoint /usr/local/bin/arbiter), env ARBITER_AUTHORITY_PRIVATE_KEY_HEX from storage-integrity-devnet2-si-v2-authority/private-key, requests cpu 500m memory 2Gi, limits cpu 2 memory 2Gi, emptyDir rehearsal sizeLimit 8Gi at /rehearsal, restartPolicy Never, automountServiceAccountToken false, nodeSelector sentio.xyz/general true, no imagePullSecrets (anchor-bootstrap Job precedent); the test asserts exact spec equality.
- REBUILD_SAFE_IMAGES is left unchanged: S3-D's test_rebuild_safe_needs_a_reviewed_image asserts it equals frozenset({DEVNET2_STAGE3_P4_SOURCE_IMAGE}).
- test_devnet2_stage3_p5_images_follow_the_rollout enforces: arbiter pin before any host; hosts b, c, a as a prefix; si-members b, c, a only after all hosts; stage-3 rules only after all members; each host file moves with its mirror (FRESH_SOURCE_IMAGE also with docs/examples/storage-integrity-fresh-source-image.yaml).
- Two existing S3-B tests are amended (hunk on top): MonitoringTest.test_default_renders_no_monitoring gains the stage3Consensus block and test_alert_names_are_the_reviewed_set renders the new group; the group is kept out of MONITORING_GROUP_RUNBOOKS because that test renders only the members group.
- Test counts in the scratch rebuild: N0=224, +3 (SnapshotBodyTest), +4 (Stage3ConsensusMonitoringTest), +5 (ActualFreshNetworkTest) = 236; the task states counts as <N0+k> with N0 recorded in Step 1.
- Names added while drafting: STAGE3_P5_RUNBOOK; STAGE3_P5_RUNBOOK_SECTIONS; SnapshotBodyTest.RAFT_KEYS; Stage3ConsensusMonitoringTest.RUNBOOK_URL; Stage3ConsensusMonitoringTest.stage3; Stage3ConsensusMonitoringTest.expected; checksum key raftSnapshotBody (storage-integrity.arbiterConfigChecksum); DEVNET2_STAGE3_P5_SOURCE_IMAGE added to REPLICA_READ_HOST_IMAGES and MEMBER_IMAGES (beyond contract §56's two sets); local tools $HOME/.cache/s3e4-task80/{ledger.env, fill.py, rollout.py, rehearse-path, rehearse.<random>/repo}
- Verified while drafting: Scratch base: git archive of a P4-state scratch production repo (S3-B, C1, C3, C4, D drafts and P4 live stand-ins, from work/d/task-53-r5/fix57 e64f1de) copied into work/e4/task-80/p4; the key-bearing ClickHouse manifests are absent there; baseline test_render.py 224 OK. / TDD replay on work/e4/task-80/dev: Step 2 SnapshotBodyTest Ran 3 FAILED (failures=13, errors=1); Step 3 227 OK; Step 4 named selection Ran 6 FAILED (failures=3, errors=3); Step 5 Ran 6 OK, full 231 OK; Step 7 Ran 5 FAILED (failures=1, errors=1); Step 8 Ran 5 OK, full 236 OK. / A second replay on a fresh clone of the base (work/e4/task-80/replay) using the same snippets ended in a tree identical to dev (diff -r). / render-inertness.sh origin/main printed the 8 identical lines (exit 0) after each commit and at the end. / helm lint --strict passed on legacy, fresh, fresh+consensusUpdates/signing-off, and fresh with monitoring.enabled, stage3Consensus.enabled and snapshotBody=plain. / Helm refusal texts were captured: schema '- at /arbiter/raft/snapshotBody: got boolean, want string', '- at /arbiter: missing property raft', and both validate.yaml guard texts. / The 26 rendered rule expressions (fresh values with both monitoring flags on) parsed with prometheus promql/parser v0.55.0 offline (GOPROXY=off, cached modules): 0 errors. / actionlint passed on the edited workflow; the paths lists carry both new files ([2, 2]). / rollout.py (final text in the task) bites: 10/10 refused; forward: V, H-b, H-c, H-a, M-b, M-c, M-a, MON, PLAIN, UNPLAIN all OK at 236 tests each; also re-run with the clone --branch command of Step 10. / Every bash block of the rendered task passes bash -n, /bin/bash -n (3.2), zsh -n and shellcheck -S warning; no hard-wrapped prose found; every diff block's old side was found in the base and its new side in dev; every included code block appears verbatim in dev (after the stand-in fill) or is a local tool.

### Task 81

- The runbook has exactly contract §57's 33 headings (§0–§11 with subsections), one bash block in §0 (first block), one go block (§3.2, the rehearsal source), 59 bash blocks of which 30 are confirmed; §7.2 opens with 'Stop here and obtain explicit confirmation.' and carries contract §57's activation command verbatim (wrapped in set +e / rc=$? / set -e so activate.err is shown on failure).
- §0 chains the eleven earlier runbooks' first §0 blocks (dynamic-table-set … p4-owner-rebuild) then its own 14 helpers; none redefines an earlier one (290 functions in the chain with the plan texts of 2026-10-16).
- Contract §57's positional $si_backup/pins (8 bare values; si_p5_image reads lines 3-4) is incompatible with 52 earlier helpers that read their own runbook's pins/repos/previous-pins shape (incl. hardening's si_pin → si_indexer_c_id → si_owner_id → si_quiet_point, si_p4_accept); the runbook never calls them and the Step 2 checker computes that set transitively and fails on any reach.
- Earlier helper limits found: the P0 runbook's si_host/si_host_series accept only a|b, hence si_p5_keeper_findings; indexer-c's runbook redefines si_info/si_rpc_on/si_audit/si_source_ch for a|b|c; P3's si_render_diff_is, si_pod_state, si_container_image, si_replica_hosts, si_host_replica_metrics are generic.
- ARBITER_TAG_P4's reader (arbiter origin/main fsm/snapshot.go:274) reads magic then the version byte and refuses an unknown version before reading further, so §3.5 copies only the 14-byte v21 envelope prefix out of the Pod under the contract name rehearsal-v21-0.container; the body (holding the rehearsal's signed update) stays in the Pod.
- §3.3 runs contract §57's p5rehearsal command inside the Pod with output to /rehearsal/rehearsal.log and copies out only result lines, the test verdict, exit status and failure lines without 'eyJ'; si_p5_py rehearsal refuses a rehearsal.txt holding a JWS-like line.
- §3.1 takes three back-to-back voter copies, refuses them if epoch/promotion_seq moved, and replays each with $R5/bin/new/statedigest requiring identical last_index, key, spent_ids, snapshot_sha256 lines (the registry cursor advances once a minute, so copies can straddle an entry).
- v21 container layout (S3-E2 §46): AFSM, version byte 21, body byte (0 plain, 1 interned), 8-byte big-endian document length (14-byte envelope); S3-B's §13.4 estimator (si_budget) assumes the 13-byte v20 envelope and a JSON doc, so it fails after activation (recorded as an operating rule).
- arbiter-admin at origin/main prints 'took a consensus hold of mode …', 'waiting for the leader's … hold: …', 'the leader drained at promotion_seq …; signing the update' and only on early exit 'released consensus hold <id>'; activation gate messages are '<name> gate refused; nothing was submitted:' etc. with name 'stage3' (S3-E2 Task 65 reuses activationGate).
- The leader's feature book records advertised features verbatim (server/membership.go features.record), so P5 verifiers/SNodes show stage3: true already under the P4 leader; §7.1 still restarts every data-plane node after the voter roll.
- consensus show prints current.stage3 with proto names, uint32 as numbers and uint64 (evidence_timeout_l2_blocks, indexer_id) as strings; stage3_state.activation_block_seq = len(Blocks) = the sealed tip (OpenBlock is separate in fsm/state.go).
- The P1 runbook keeps devuser1.s3_p1 (id UInt64, p UInt8, v String, PARTITION BY p) for good, so indexer-b's P5 write goes there; indexer-c's goes to <db>.s3e_e2e.
- Freeze runs from §3.1 to §7.3; writes may resume afterwards because the first v21 snapshot needs snapshot_threshold 8192 new entries (snapshot_interval 120s in the chart).
- Verifier StatefulSet is RollingUpdate in pinned-genesis mode (OnDelete only for legacy); voters are OnDelete; arbiterConfigChecksum is a dict of selected values, so whether arbiter.raft.snapshotBody changes the voters' StatefulSet depends on Task 80 (§11 wording allows either).
- Pointer: one sentence appended to the first §11 bullet of docs/storage-integrity-devnet2-multi-source-tracks.md (base :1072); no earlier plan edits that bullet.
- Names added while drafting: si_p5_value <NAME> (reads $si_backup/ledger-values.txt); si_p5_owner a|b|c (indexer-c's id from the ledger); si_p5_member_keys a|b|c (count of consensus: keys in the si-member's rendered si-member.yaml); si_p5_keeper_findings a|b|c (sentio_node_storage_integrity_keeper_findings lines from :9091; si_host_series only serves a|b); si_p5_put <file> <pod path> (copy into the rehearsal Pod via kubectl exec stdin, SHA-256 checked); si_p5_accept <key-file|-> a|b|c <n> <tag>; si_p5_py subcommands params, stage3, features, activation, rehearsal, header, pod, pod-live; $si_backup/ledger-values.txt (17 NAME=value lines read in §1.1); acceptance values 5401 (devuser1.s3_p1, p=1, §5.1), 5402 (<db>.s3e_e2e, §5.2), 5403 (§5.3) and 5404 (§7.3) in devnet101.swap_new2; 4 promotions, no table-set transition; rehearsal Pod inputs /rehearsal/{p5rehearsal,stage3-params.json,rehearsal-copy-<i>.tgz,arbiter-<i>.yaml}, log /rehearsal/rehearsal.log, expected outputs /rehearsal/rehearsal-v21-<i>.container; line P5_REHEARSAL=<YYYY-MM-DD> appended to rehearsal.txt once rehearsal ok and exit 0 printed; evidence: baseline-p5.txt (contract name for §1.3), stage3-params.txt, identities.txt, show-before.json, images-before.txt, member-images-before.txt, features-before.json, budget-start.txt, replay-baseline.txt, show-replay.json, arbiter-<i>-raft.tgz, arbiter-<i>.yaml, digest-<i>-<v>.{cmp,err}, written-<i>-<v>.container, replay.txt, rehearsal-show-{before,after}.json, rehearsal-arbiter-<i>.yaml, rehearsal-digest-<i>.{txt,err,cmp}, rehearsal-copies.txt, p5rehearsal_test.go, rehearsal-pod-check.txt, rehearsal-pod-applied, rehearsal-pod.json, rehearsal-inputs.txt, rehearsal-activation-check.txt, rehearsal-mismatch-check.txt, verify-verifiers.txt, member-<x>-uid, replica-hosts-<x>-{before,after}.json, verify-indexer-<x>.txt, accept-b-5401.txt, accept-c-5402.txt, accept-a-5403.txt, accept-a-5404.txt, member-window-<x>.txt, member-window-<x>-host-after.txt, verify-member-<x>.txt, replace-voter-<k>.txt, verify-voters.txt, state-<i>.bin, tracks-before-activation.json, verify-before-activation.txt, features-at-activation.json, tracks-at-activation.json, verify-after-activation.txt, monitoring-before.txt, vmalert-stage3.txt, vmrule-live.json, vmrule-stage3-selectors.txt, monitoring-after.txt, voter-order-rollback, rollback-voter-<k>.txt, verify-rollback.txt; commit subjects: chore(devnet2): arbiter <ARBITER_TAG_P5> for stage 3 P5 (verifiers now, voters in §6); chore(devnet2): stage-3 consensus alert rules on (P5); task commit docs(storage-integrity): devnet2 stage-3 P5 runbook (rehearsal and the v21 activation); ledger commit docs(plan): record P5_PR of Plan S3-E4 (Task 81); local test files in $HOME/.cache/s3e4-task81/: check_runbook.py, fill_runbook.py, fixtures.bash, selftest.bash, ledger.txt, blocks/, fx/; ledger record worktree multi-source-s3e4-ledger-81, record files $HOME/.cache/s3-ledger/record-s3e4-81{,.md,-pr.md,-pr-url}; PR-time placeholders <ENROLMENT_EPOCH + 1>, <P5REHEARSAL_SHA256>, <P5REHEARSAL_SOURCE> (plus the 14 ledger names), filled in Step 5
- Verified while drafting: Scratch tree: git archive of production origin/main 206c42357 (git-initialised) under work/e4/task-81/production, plus the eleven earlier runbooks extracted from the current plan texts (S3-B Tasks 14/17/20, S3-C1 Task 27, S3-C3 Task 39, S3-C4 Task 47, S3-D Task 54); the §0 chain sourced cleanly (bash -n OK). / Step 2 checker: failed first with FileNotFoundError, then (runbook, no pointer) with 'the tracks runbook's §11 does not point to the P5 runbook', then passed: 33 headings, 59 bash blocks (30 confirmed), 4 Python programs compile, chain 290 functions / 14 own / 61 earlier reached / none of 52 pins-readers; mutation tests refused a removed confirmation, a redefined earlier helper (si_tracks), a call of si_owner_id, a changed activation command (--hold issuance) and a helper calling si_pin. / Simulated Task 80 branch (stand-in P5 constants appended to test_render.py, a Pod example written from contract §56) and stand-in $R5 (placeholder rehearsal source, stand-in statedigest/snapdump/v20check/v21check printing the contract lines, a linux/amd64 Go stand-in binary, a synthetic v21 fixture); every step's local commands were run as written from the extracted task text (embedded files round-trip byte-identically). / Step 5 fill: '14 ledger values, the next epoch 7, the rehearsal source (10 lines, sha256 …)'; the three grep checks print 1; --filled check passes. / Step 6 self-test: 46 checks passed under bash 5.3.15 and /bin/bash 3.2.57 (si_p5_py params/activation/features/stage3/rehearsal/header/pod/pod-live with refusals, §1.1 constants check incl. MEMBER_IMAGES refusal, §2.1 block against stand-in $R5 incl. refusal of a changed binary and mode 755, §3.2 read-only block incl. refusal of a changed go source, si_v21check behind a newer .tmp dir and on v20/none, §7.2 flag loop, the in-pod JWS filter, si_p5_put, si_p5_member_keys, si_p5_keeper_findings, si_p5_accept rows). / In-pod sh -c quoting of the rehearsal command verified (argument reaches the Pod shell with '^TestP5Rehearsal$' single-quoted). / shellcheck 0.11.0 clean on all 59 blocks (SC2154 excluded); python3 -B charts/storage-integrity/tests/test_render.py: 86 tests OK with the two documents on the base tree; the three helm lint --strict profiles passed; helmfile template of storage-integrity-si-v2 rendered. / Task markdown checked: no hard-wrapped prose outside fences, every fence closed, no TBD/TODO; 50 confirmation lines/notes, Co-Authored-By on both commits. / Not run: render-inertness.sh (S3-B script, not on origin/main), Task 80's real render tests and Task 78's real tools (not drafted yet), anything against a cluster, registry or GitHub; nothing written to real repositories, worktrees or $HOME (a fake HOME under the scratch dir was used).

## Appendix C: execution record

Written by the plan consistency pass from the release ledger table above (one row per ledger name this plan produces, empty until recorded, in the S3-B D23 shape), together with the run-record list. Not written yet: see `2026-10-15-multi-source-si-s3cf-status.md`.
