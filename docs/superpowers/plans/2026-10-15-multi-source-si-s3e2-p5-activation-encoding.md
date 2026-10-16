# Multi-source SI network — Plan S3-E2: stage-3 P5 arbiter activation, v21 encoding and switches 1–3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Status (2026-10-16): draft.** Tasks 64, 65, 66, 67, 68, 69, 70 are drafted and were verified by their drafters in scratch exports; no task has had its adversarial review yet, and the plan consistency pass (which also writes Appendices B and C in final form) and the cross-plan pass have not run. Do not execute this plan before those passes; `2026-10-15-multi-source-si-s3cf-status.md` lists what remains and how to finish it.

**Goal:** Give the arbiter everything the v21 activation and the first three switches of spec §10.4 need, all inert until an authority-signed update turns them on: the `stage3` consensus parameter with its one-way switches and its rules, the host-bound `verifiers` entry rules, and the activation itself (an update that sets `stage3` with every switch off, judged by the stage-2 drain gate, a leader gate that probes every voter and every data-plane node for `stage3_v1`, and a registration refusal afterwards); snapshot v21 with a body-kind byte, a plain body and a lossless interned body that `Persist` writes only after it has verified it, falling back to the plain body (and counting the fallback) on any mismatch; eviction voiding (switch 2); incarnation-scoped Keeper path marking and the pool-plus-members purge-required set (switch 1); and owner-aware, pool-only selection with the `VERIFIERS_UNAVAILABLE` admission refusal and unmarkable blocks leaving the WorkSet (switch 3). Before `stage3` is committed the P5 binary is byte-identical to `ARBITER_TAG_P4` (same Apply results, same v20 containers); after the activation with every switch off only the snapshot container changes. Nothing is released and nothing runs on devnet2 in this plan.

**Architecture:** Every task is arbiter code on the one P5 branch `urwt/claude/multi-source-s3e` (K65 of Plan S3-E1), which Plans S3-E3 and S3-E4 continue and S3-E4 Task 78 releases as `ARBITER_TAG_P5`.
- Task 64 pins `PROTO_TAG_P5`, `HG_TAG_P5` and `CORE_TAG_P5`; adds `Params.Stage3` and `State.Stage3` (`Stage3State`: the activation point and one cutover record per switch); the Params-only `stage3` rules in `verifyConsensusTransition`, which restore replays (one-way switches turned on one per update in §10.4 order, carry-once-set, non-decreasing `min_safe_copies` and `group_size`, `min_safe_copies > 1` and `strict_copies` only after `checkpoints` and `narrowed_drain`, a failure domain for every enrolled indexer from `placement` on); the host-bound `verifiers` entry rules (consent, replica signer, weight, failure domain, `shared_rmt`, refused until `owner_aware_selection` is committed, K68); the every-field `cloneVerifiers` fix (the scoping proposal's defect D3, whose other two rebuild sites are Plan S3-E1's Tasks 59 and 60); the Apply-only half `stage3MembershipRefusalLocked` (a member node id unused by every registered node; Plan S3-E3 Task 71 adds the per-group verifier floor there); and the activation Apply, which needs no empty open block and no global prefix (unlike `tracksActivationRefusalLocked`).
- Task 65 adds `server/stage3_gate.go` on the tracks gate's pattern (no non-voter, the FSM's activation refusal, `stage3_v1` from every registered, non-evicted data-plane node except the SNode of a quarantined track, every voter probed through `arbiter-stage3-voters`), the post-activation registration refusal, `stage3` in `consensusMutableParams`, and the `arbiter-admin consensus update` flags `--stage3`, `--stage3-*`, `--stage3-switch`, `--indexer-failure-domain` and the extended `--verifier`, with an activation-gate exemption hook for the SNodes of quarantined tracks.
- Task 66 adds snapshot v21 (`AFSM`, version 21, a body-kind byte, length, body, spent-ids), writes v20 until `Params.Stage3` is committed and v21 with the plain body afterwards, refuses every stage-3 field under an older version byte (`stage3FieldsPresent`), adds `fsm/stage3_restore.go` (`validateStage3State`, which every later stage-3 task extends), and freezes the plain v21 container golden.
- Task 67 adds the interned body (`arbiter-snapshot-interned-v1`, generic JSON value interning, K70), the write-then-verify in `Persist` outside the FSM lock on Plan S3-A Task 9's snapshot path, the node-local writer setting `raft.snapshot_body: auto|plain`, `arbiter_snapshot_encoding_fallback_total` and `arbiter_snapshot_body_bytes{kind}`, a forced-mismatch test, the frozen interned container golden, and an offline replay and size measurement over local copies of the devnet2 voters' logs.
- Task 68 adds eviction voiding as explicit checks at each signer's call site (result claims, attestations, byte-side scans, promotion, cleanup and rejected-cleanup acknowledgements, parked claims), never inside `snodeSignatureLocked`, which registration shares; `reevaluateBlock` skips an evicted verifier's evidence unless quorum latched or a statement is challenged; `EvictNode` and the switch-on re-evaluate the blocks that name evicted verifiers; the WorkSet treats voided evidence as missing.
- Task 69 marks every chain incarnation recorded after the `purge` switch `scoped_keeper_path`, gives `TableIncarnation` its `purge_members` list (filled by Plan S3-E3 Task 71), makes the purge-required set the owner's SNode, the network pool and `purge_members` once the switch is on, serves both fields in the registry view, and adds their restore rules.
- Task 70 adds owner-aware selection: with the switch on, `MarkReplaying` draws from the eligible ring (pool-only in this plan; Plan S3-E3 Task 73 adds the group half) with today's block-seeded start, and stores the ring, the ring position and the registry cursor block in `BlockVerification`; admission answers `VERIFIERS_UNAVAILABLE` right after `SOURCE_UNAVAILABLE`; a sealed block whose ring is below three leaves the WorkSet, so the orchestrator stops proposing it; the read RPC serves the ring; with the switch off selection is byte-identical.

**Tech Stack:** Go 1.26 (`go 1.26.3`); Bazel 9.1.0 + Bzlmod + gazelle; hashicorp/raft + raft-boltdb; protobuf consumed through arbiter-core `wire` only (the fsm import red line: `bazel query 'deps(//fsm:fsm, 1)'` never names arbiter-proto, and `fsm/` never calls `time.Now`); `encoding/json` and `crypto/sha256` for the interned body; Prometheus client_golang; throwaway local rehearsal tools built with `go build` from `git archive` exports (never committed).

**Spec:** housegate `docs/superpowers/specs/2026-10-14-multi-source-si-stage3-database-groups-design.md` (housegate#236, with every review fix) — binding: §6.1 (the `verifiers` entry fields and rules and the `stage3` parameter rules; the per-group verifier floor is Plan S3-E3's), §7.3 (owner-aware selection for databases without a group, the owner-hosted exclusion, `VERIFIERS_UNAVAILABLE`, unmarkable blocks; the group half is Plan S3-E3's), §8.8 (the FSM half of the `purge` switch: scoped marking and the purge-required set; the data-plane half is Plan S3-E1's), §10.3 (the activation, the v21 container with both bodies, write-then-verify, the fallback metric, the operational writer setting; the rehearsal and the live activation are Plan S3-E4's), §10.4 (the switch rules and steps 1–3), §13.2 (the lossless encoding), §14 bullet 5 (eviction voiding), and the §17 FSM tests for these items, as clarified by Plan S3-A's K1–K10, Plan S3-B's K11–K22, Plan S3-E1's K58–K67 and this plan's K68–K72. **Prerequisites:** Plan S3-E1 (`2026-10-15-multi-source-si-s3e1-p5-wire-dataplane.md`): its releases `PROTO_TAG_P5`, `HG_TAG_P5` and `CORE_TAG_P5` and its contract §38–§43 (the proto numbers of §39 verbatim, the canonical types and authority purposes, the wire converters, `stage3_v1`); Plan S3-D (`2026-10-15-multi-source-si-s3d-p4-settle-rebuild-hardening.md`): `ARBITER_TAG_P4` cut from arbiter `main` before this plan's branch starts, and `PROTO_TAG_P4`, produced unconditionally (decision D26) and the base against which `PROTO_TAG_P5` passes `buf breaking`; Plan S3-A's Appendix A §0–§6 (the reads, holds, state-size gauges and snapshot observer this plan extends) and Plan S3-B's §7–§15 apply unchanged where this plan does not replace them; Plan S3-C1's contract §19 defines the ledger tool. This plan's Appendix A continues the numbering (§44–§48); Plans S3-E3 (§49–§52) and S3-E4 (§53–§57) build on it.

**Base revisions.** Line anchors refer to arbiter `fb42260` (v0.12.0), arbiter-core `8f0bc43` (v0.14.1), arbiter-proto `1c6ae67` (v0.11.0), housegate `66d4df7` (v0.19.0), sentio-node `5d2f169` and production `75514e3de` (its storage-integrity paths byte-identical to Plan S3-B's base `9d3d0326b`), the spec's revisions. The branch itself starts from arbiter `main` at or after `ARBITER_TAG_P4`, which carries Plan S3-A's Tasks 7–11 (`fsm/reads_*.go`, `fsm/state_size.go`, `fsm/snapshot.go`'s observer path, `server/safestate_stage3.go`, `server/consensus_hold*.go`, `orchestrator/*`, `raftnode/snapshot_observer.go`, `cmd/arbiter-admin/main.go`'s `--hold`), Plan S3-C2's commit `ARBITER_PIN_P2` (the `CORE_TAG_P2` pin and the `chpipeline` member harness) and Plan S3-D's Task 51 (leader-local settle); every hunk of this plan on those files is re-anchored there under the hunk rule. devnet2 is not touched by this plan; when it starts, the earlier plans' exit records leave it as Plan S3-B's `E run and the steady state after E` describes it, changed by no consensus format through `P4_END` (the activation model of the cross-plan conventions, whose only exception is Plan S3-D's `PROTO_TAG_P4` settle surface: `SettleTrack` and the additive, non-Raft `SNodeHello.settle` / `PromotionCommand.settle` stream fields, decision D26): tracks enabled with cutover 60/52, `si_indexers` {0: `snode-1`, 1: `snode-2`, `INDEXER_C_ID`: `snode-3`}, verifiers `verifier-1..3`, `max_writers` 3, epoch `ENROLMENT_EPOCH` or later, snapshot v20, no `stage3` parameter, voters on `ARBITER_DIGEST` or `ARBITER_DIGEST_P4`, the si-members `MEMBER_IDS` running off consensus and unregistered. Task 67's replay reads only local copies made by earlier plans (contract §48).

## Global Constraints

- **Earlier plans apply.** Every Global Constraint and clarification of Plans S3-A and S3-B applies (and through them those of Plans S1-A, S1-B, S2-A and S2-B: strict decoder, request-only signatures, signed claims, tracks, quarantine, golden files, worktrees, commits, remote actions), together with the cross-plan conventions of Plans S3-C1 … S3-F and Plan S3-E1's contract.
- **Cross-plan decisions D25–D40** (the conventions' list, binding) apply where they name this plan: D25 (the `conformance/stage3_reads_test.go` exception, below), D26 (`PROTO_TAG_P4` is the unconditional proto base; `RebalanceGroup` and `MarkMemberLagging` are `ConsensusAdmin` methods 16 and 17, contract §48), D27 (the registry view's `GroupMember.host_indexer_id` rests on K68's fixed host), D33 (the purge cutover's `IncarnationSeq` stays unserved, contract §45), D34 (the ledger copy, contract §48) and D37 (the one P5 rehearsal directory `$R5` is created by Plan S3-E4 Task 78; Task 67 replays in its own work directory `$T` and removes it, contract §48).
- **Activation model (contract §44).** Before `Params.Stage3` is committed the P5 binary replays every history to the same Apply results and state digests as `ARBITER_TAG_P4` and writes the same v20 containers, which `ARBITER_TAG_P4` restores; after the v21 activation with every switch off only the snapshot container differs. Every new Apply branch tests its switch first and returns a fixed refusal, or today's behaviour, before it; each task pins its switch-off identity with a named test.
- **Ordering.** Switches are turned on one per update, each after every earlier switch of §10.4 is committed: `purge` < `eviction_voiding` < `owner_aware_selection` < `placement` < `reselection` < `checkpoints` < `narrowed_drain` (K69); a host-bound `verifiers` entry is refused until `owner_aware_selection` is committed (K68). Tasks land in numeric order on one branch (§48).
- **Irreversible steps.** None runs in this plan. The code introduces one irreversible format (snapshot v21, live from the activation) and seven one-way switches; Plan S3-E4 executes the activation and Plan S3-F the switches, each as its own confirmed step.
- **Release rule (K65).** No arbiter release from Task 64's pin through Task 78: `CORE_TAG_P5` puts `stage3_v1` into `GetProtocolInfo` the moment it is pinned. This plan opens no PR and merges nothing to `main`; its last step pushes the branch (explicit confirmation). Plan S3-E4 Task 78 merges and releases.
- **Production rule.** This plan changes no production file and runs no live command. The v21 writer value, the encoding-fallback alert and the voter budget re-check are Plan S3-E4 Task 80's, under the user's production rule (direct push only for convergent single-value changes; a PR for everything else).
- **Runbook conventions.** No runbook is written here. The only shell work outside the worktree is Task 67's throwaway work directory `$T` and its two evidence files beside the worktree (contract §48): bash blocks, `Confirm before running.` before every block that writes outside `$T` and those two files, removes `$T` or reaches a remote, evidence teed into the two files, nothing committed; commands handed to the user in chat are fish. The one P5 rehearsal directory `$R5` (`$HOME/si-devnet2-multisource-s3e-rehearsal`) is created by Plan S3-E4 Task 78 and only read by the P5 runbook (decision D37); no task of this plan creates, writes or reads it.
- **Passwords and keys.** None is handled. Tests sign only with `authoritytest` keys; the live authority key never leaves the voter pods and no plan step reads it; the offline replay exports a throwaway random `ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX` because `config.Load` requires one (Plan S3-B runbook §3.3's rule).
- **Never edit golden or vector files:** everything Plans S2-A and S3-A list, plus `fsm/testdata/pre_tracks_golden.json`, the `fsm/l3_track_golden_test.go` constants and arbiter-proto's `conformance/stage3_reads_test.go` (decision D25: only its count literals change, by Plans S3-D and S3-E1, every existing entry byte-identical; this plan changes nothing in arbiter-proto); the existing golden tests (`TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, `TestPreActivationHistoryIsByteIdenticalAcrossTracks`, `TestL3CommitmentPreimagesAreFrozen`, `TestL3TrackHeaderPreimagesAreFrozen`, `TestL3TransitionHeaderPreimageIsFrozen`, `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`, the shared vector tests) pass unchanged. New frozen fixtures: `fsm/testdata/stage3_v21_plain.snapshot` (Task 66) and `fsm/testdata/stage3_v21_interned.snapshot` (Task 67), captured once and never rewritten.
- **Consumed names.** Names of Plans S3-A, S3-B, S3-C1 and S3-D are spelled as their contracts give them; names of Plan S3-E1 are spelled as contract §48 lists them, reconciled with Plan S3-E1 §39–§42 by the cross-plan consistency pass; Plan S3-E1 binds if a name ever differs, never the other way.
- **Hunks are anchored on the base revisions.** Tasks were drafted in parallel; when a later task's hunk overlaps an earlier task's edit (Tasks 64, 66, 68, 69 and 70 all edit `fsm/state.go`, `fsm/snapshot.go` or `fsm/stage3_restore.go`; Tasks 68 and 70 both edit `fsm/apply.go` and `fsm/reads_work.go`), apply the later intent on top of the earlier change. The contract and the named tests are binding; line anchors are guidance.
- **Repository commands, worktrees, commits:** contract §48. Remote actions (fetches included) require explicit user confirmation in chat; commands handed to the user are fish.
- **References.** `contract §N` points to Plan S3-A's Appendix A for §0–§6, Plan S3-B's for §7–§15, each Stage 3 plan's own range for §16–§61 (this plan: §44–§48); a task group's "Facts" point to Appendix B; Appendix C of this plan has no ledger row (this plan records nothing).

## Plan clarifications (binding)

The spec, Plans S3-A and S3-B and Plan S3-E1 leave these P5 details open; each is resolved here and binding for Plans S3-E2, S3-E3 and S3-E4.

- **K68 — Host-bound entries enter no ring and no purge set before their step (spec §10.4, line 272).** The spec says "from the activation, entries with a `host_indexer_id` enter no eligible ring until step 6", but no switch enforces it: `selectVerifiers` takes every Active VERIFIER (`arbiter:fsm/select.go:60-76`) and `purgeRequiredLocked` counts every non-evicted VERIFIER (`arbiter:fsm/apply_table_registry.go:475-489`), so a member that registered early would verify its own owner's blocks and hold every purge network-wide. Resolution: `verifyConsensusTransition` refuses a `verifiers` entry with a `host_indexer_id` until `owner_aware_selection` is committed in the previous parameters (`consensus update cannot list host-bound verifier %s before the stage3 owner_aware_selection switch is committed`). Registration already requires listing (`verifierSignatureLocked`, `arbiter:fsm/signed_claims.go:461-473`), so no member registers before switches 1–3; from switch 1 the purge-required set counts only the pool and `purge_members` (Task 69), from switch 3 selection draws only from the pool (Task 70), and a member enters a ring only through Plan S3-E3's group half once ready (step 6). An entry's `host_indexer_id` never changes for its life (nil ↔ set included), so a pool verifier cannot become a member in place; Plan S3-E1's registry-view `GroupMember.host_indexer_id` is filled at serve time from the member's `VerifierEntry` on this guarantee and is neither FSM state nor Raft-reachable (decision D27). Why: it machine-enforces step 4's place after step 3 with no change to switch-off behaviour, because no host-bound entry can exist before switch 3. Spec §12's "listing a third party's member is refused" has no `operator_id` to check and stays an operating rule of Plan S3-F.
- **K69 — Consensus rules: what replays, what reads state, and the order (spec §6.1, §9.3, §10.4).** Every `stage3` rule that compares parameters alone runs in `verifyConsensusTransition` (`verifyStage3Transition`, `verifyHostBoundVerifiers`), so `validateConsensusHistory` replays it on restore; only rules that read committed node or registry state run in the Apply-only half (`stage3MembershipRefusalLocked`, after `signedClaimsMembershipLocked`), like the writer and drain gates. The rules: `stage3` is first set only after the tracks activation, with every switch off, `strict_copies` false and `min_safe_copies` 1; once set every update carries it; switches and `strict_copies` are one-way; `min_safe_copies` and `group_size` never decrease; an update turns on at most one switch, and only when every earlier switch of §10.4 is committed in the previous parameters (the spec's "each requires every earlier switch" read as committed earlier, so each switch has its own epoch, cutover and forward-fix playbook); `min_safe_copies > 1` and `strict_copies` need `checkpoints` and `narrowed_drain` on, because before the narrowed gate the stage-2 drain gate refuses every update while a promoted unsafe part is uncleaned (`arbiter:fsm/quarantine.go:233-242`), and a floor-held part would never be cleaned, deadlocking governance; from the `placement` switch-on every si_indexers entry has an `indexer_failure_domains` entry, and the map never names an indexer that is not enrolled. Why: restore must accept exactly the histories Apply accepts, and every rule that cannot be replayed from parameters would otherwise refuse legitimate histories.
- **K70 — v21 framing and the interned body (spec §10.3, §13.2).** The container is `AFSM` (4 bytes), version byte 21, a body-kind byte (0 plain, 1 interned), the body length (8 bytes, big-endian), the body and the spent-ids dump; v3–v20 keep their 13-byte envelope. The plain body is exactly the JSON document v20 lays out plus the member `stage3`; the interned body is the JSON document `{"format":"arbiter-snapshot-interned-v1","strings":[…],"values":[…],"root":…}` in which every JSON string token of at least 64 bytes is replaced by `{"$s":i}` and every object or array whose compact encoding is at least 256 bytes by `{"$v":i}`, children before parents, tokens copied verbatim, so rehydration reproduces the plain bytes exactly and an unchanged partition entry, an LtHash accumulator and a part triple are each one table entry. The writer writes v20 until `Params.Stage3` is committed, so a voter can return to `ARBITER_TAG_P4` at any point before the activation; a v21 binary reads v3–v20 and both v21 bodies; an older binary refuses v21 at the version byte (`arbiter:fsm/snapshot.go:274`). Why: one version byte keeps rollback until the activation, the kind byte lets one binary write either body, and generic interning needs no change when Plans S3-E3 and S3-E4 add state, which a structural codec would (every new member would need its own interned form).
- **K71 — Memory budget and digests (spec §13.2, line 317).** The interned codec lives in arbiter `fsm` (`snapshotDoc` is arbiter's; no member or data-plane node needs it). `Snapshot()` keeps marshalling the plain container under the read lock, unchanged in cost; `Persist`, outside the lock, interns the plain body, rehydrates the result and compares its SHA-256 with the SHA-256 of the plain body that `Snapshot()` captured (the digest of bytes captured under the lock, so it is the spec's digest "captured under the read lock"; `encoding/json` output is deterministic for `snapshotDoc`). `arbiter_state_bytes`, `arbiter_snapshot_doc_bytes` and every Plan S3-A size gauge keep describing the plain document; the new `arbiter_snapshot_body_bytes{kind}` reports the persisted body. The 1 GiB read cap applies to the stored body, so v21 lowers the read-cap and PVC terms of L; a rehydrated document above `maxSnapshotRehydratedDocBytes` (4 GiB) is refused as corrupt. Node-local in-memory interning is out of scope: the in-memory state stays plain, so the memory term of L is unchanged, and `Persist`'s transient memory (about three plain documents: the plain bytes, the interned bytes and the rehydrated bytes) is measured by Plan S3-E4's rehearsal against the voter budget. Why: the verify step makes a codec bug cost only compaction efficiency, never correctness, and keeping the gauges' meaning stops the alerts of Plan S3-B from changing silently.
- **K72 — Parked claims of an evicted SNode (spec §14).** `adoptParkedClaimsLocked` (`arbiter:fsm/signed_claims.go:674-695`) discards, and never binds, a parked result claim whose `SourceNode` is Evicted once `eviction_voiding` is on. Today the path is unreachable through admission: admission binds only an Active owner SNode (`ownerSourceLocked`, `arbiter:fsm/select.go:84-94`) and adopts in the same Apply, so a parked claim of an evicted SNode never matches the bound source. The check is still explicit, at the adoption site, and pinned by a white-box test. Why: §14 refuses every result claim of an evicted node, and keeping each call site explicit means a later change to source binding cannot reopen the hole silently; a re-registered node (higher `registration_seq`) is no longer evicted and adopts as today, since voiding parks a node and is not a key fence.

## Release ledger (consumed and produced)

| Name | Meaning | Resolved by |
|---|---|---|
| `PROTO_TAG_P5` | arbiter-proto release with every v21 number (contract §39 of Plan S3-E1), `buf breaking` clean against `PROTO_TAG_P4` | Plan S3-E1 Task 57, its Appendix C |
| `HG_TAG_P5` | housegate release pinning `PROTO_TAG_P5`, with admission codes 12 and 13 classified at the ingress | Plan S3-E1 Task 58, its Appendix C |
| `CORE_TAG_P5` | arbiter-core release with the canonical stage-3 types, the authority purposes, the wire converters and tags 38–42, the scoped Keeper paths, the member consensus mode and `stage3_v1` in `LocalNodeFeatures` | Plan S3-E1 Task 63, its Appendix C |
| `ARBITER_TAG_P4` / `ARBITER_DIGEST_P4` | arbiter release of Plan S3-D (leader-local settle), cut from `main` before this plan's branch starts; the "old" build of Task 67's replay | Plan S3-D Task 51, its Appendix C |
| local rehearsal inputs | Plan S3-A's tools (`$HOME/si-devnet2-multisource-s3a-rehearsal/{statedigest,v20check,snapdump}`), Plan S3-B's voter copies (`$HOME/si-devnet2-stage3-p0/arbiter-{0,1,2}-raft.tgz`, `arbiter-{0,1,2}.yaml`), Plan S2-B's copy and samples (contract §48) | Plan S3-A Task 11, Plan S3-B Task 15 |
| — (produced) | none: this plan records no ledger value and releases nothing; its Appendix C table has no row, and its branch head is the base of Plan S3-E3 | — |

Every consumed value is read from the ledger copy with `s3-ledger.py get` (contract §48, Plan S3-C1 §19; the plans-cf branch until the stage-3 plans PR merges, then `origin/main`, decision D34) by Task 64 before its first edit, never predicted, and never read from a local checkout or a pending PR. Task 67's measurements are two local evidence files beside the worktree (`$W.replay-e2.txt`, `$W.v21size-e2.txt`, contract §48); its work directory `$T` is removed at the end of Task 67. The one P5 rehearsal directory `$R5` (`$HOME/si-devnet2-multisource-s3e-rehearsal`) is created by Plan S3-E4 Task 78 and only read by the P5 runbook of Task 81, which keeps its live rehearsal evidence under its own `$si_backup` (decision D37).

## Review Focus

1. **A pre-activation history, and every switch off.** The P5 binary replays a history without `stage3`, and a history with the activation and no switch. Expected: before the activation the same Apply results, state digests and v20 container as `ARBITER_TAG_P4`, each build restoring the other's container; after the activation with every switch off the same results for every command as without it, apart from `Params.Stage3`, `State.Stage3`, the consensus history and the v21 container; no stage-3 field written before its switch; the existing golden tests unchanged. Tests: Task 64 `TestStage3ActivationWithEverySwitchOffChangesNoBehaviour`, `TestPoolVerifierCarriesNoHostBoundFields`; Task 66 `TestSnapshotWritesV20UntilStage3IsCommitted`, `TestRestoreReadsEveryEarlierVersionUnchanged`; Task 69 `TestPurgeSwitchOffKeepsEveryIncarnationUnscoped`, `TestPurgeSwitchOnWithNoMembersChangesNoPurge`, `TestPurgeNodeSetIsUnchangedByThePurgeSwitch`; Task 70 `TestOwnerAwareSelectionOffSelectsByteIdentically`, `TestOwnerAwareSelectionWithOnlyThePoolPicksTodaysSet`; Task 67's offline replay (`$W.replay-e2.txt`).
2. **A consensus update out of order, regressing, or losing a field.** Inputs: a switch before its prerequisite, two switches in one update, a switch turned off, `min_safe_copies` or `group_size` lowered, `min_safe_copies` 2 or `strict_copies` before `checkpoints` and `narrowed_drain`, a host-bound entry before `owner_aware_selection`, one with a consent by another signer, weight 0, a domain other than its host's, `shared_rmt` false with `verify`, a pool entry with host-bound fields, a changed `host_indexer_id`, `placement` with an indexer that has no domain, a member node id that a registered node holds, and an entry whose new fields a clone would drop. Expected: each refused with its verbatim text and state unchanged; restore replays the Params-only rules to the same decisions; `cloneVerifiers`, `cloneParams` and `normalizeConsensusParams` keep every field, so the leader signs exactly what Apply records. Tests: Task 64 `TestStage3OneSwitchPerUpdate`, `TestStage3SwitchNeedsEveryEarlierSwitchCommitted`, `TestStage3SwitchesAndStrictCopiesAreOneWay`, `TestStage3MinSafeCopiesAndGroupSizeNeverDecrease`, `TestStage3FloorAboveOneNeedsCheckpointsAndNarrowedDrain`, `TestHostBoundVerifierRefusedBeforeOwnerAwareSelection`, `TestHostBoundVerifierRules`, `TestVerifierHostIndexerIDNeverChanges`, `TestStage3PlacementNeedsADomainForEveryIndexer`, `TestMemberNodeIDMustBeUnusedByEveryRegisteredNode`, `TestCloneVerifiersCopiesEveryField`, `TestStage3RulesReplayOnRestore`; Task 65 `TestConsensusUpdateStage3FlagRefusals`, `TestConsensusUpdateCarriesTheCommittedStage3AndHostBoundVerifiers`.
3. **The interned body disagrees with the plain body, or a container is forged.** Inputs: an interner that changes one byte, a plain document holding a key that begins with `$`, an interned body with a forward reference or a doubling chain of references, a v21 body kind 2, a pre-v21 container carrying `stage3`, a host-bound entry, a scoped incarnation or an eligible ring, and switch cutovers that disagree with the consensus history. Expected: `Persist` writes the plain body, counts `arbiter_snapshot_encoding_fallback_total`, and never cancels the snapshot; `raft.snapshot_body: plain` writes plain and counts nothing; restore refuses every forged or corrupt container with its text; `arbiter_state_bytes` stays the plain length. Tests: Task 67 `TestPersistFallsBackToThePlainBodyOnAMismatch`, `TestPersistFallsBackWhenThePlainDocumentHoldsAReservedKey`, `TestPersistWritesThePlainBodyWhenForcedPlain`, `TestRehydrateRefusesAForwardReference`, `TestRehydrateRefusesAnExpansionAboveTheCap`, `TestStateBytesStayThePlainLengthUnderTheInternedBody`; Task 66 `TestRestoreRefusesStage3FieldsInAPreV21Container`, `TestRestoreRefusesAnUnknownV21BodyKind`, `TestValidateStage3StateRefusesSwitchCutoversThatAreNotTheHistorys`; Task 69 `TestRestoreRefusesAScopedIncarnationBeforeThePurgeCutover`; Task 70 `TestRestoreRefusesAnEligibleRingThatDoesNotYieldTheVerifierSet`.
4. **An evicted node keeps speaking.** Inputs, with `eviction_voiding` on: an evicted SNode's result claim and its promotion, cleanup and rejected-cleanup acknowledgements, an evicted verifier's attestation and byte-side scan, a parked claim of an evicted SNode; evidence an evicted verifier recorded before its eviction in an unlatched block, a latched block and a challenged block; a re-registration with a higher `registration_seq`. Expected: every new message refused with `%s from %s: the node is evicted (stage3 eviction_voiding)` and state unchanged; an unlatched, unchallenged verdict loses the evidence and the WorkSet lists its block as `EvidenceIncomplete`, never `QuorumFailed`; a latched verdict survives a snapshot and restore; a challenged block keeps its evidence; registration and MarkActive keep their checks and a re-registered node speaks again; with the switch off nothing changes. Tests: Task 68 `TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode`, `TestEvictionVoidingOffKeepsTodaysAdmission`, `TestEvictionVoidingKeepsALatchedVerdictAcrossSnapshotRestore`, `TestEvictionVoidingDropsAnEvictedVerifiersEvidenceFromAnUnlatchedVerdict`, `TestEvictionVoidingKeepsTheEvidenceOfAChallengedBlock`, `TestWorkSetTreatsAnEvictedVerifiersEvidenceAsMissing`, `TestAdoptParkedClaimsDiscardsAClaimOfAnEvictedSNode`, `TestReRegisteredNodeIsNoLongerVoided`.
5. **A block that cannot find three verifiers.** Inputs, with `owner_aware_selection` on: the Active pool drops below three (an eviction, a verifier back in Syncing), a statement arrives for an owner, a sealed block waits while the pool recovers. Expected: admission answers `VERIFIERS_UNAVAILABLE` right after `SOURCE_UNAVAILABLE`, with the coordinate unspent and no seal prepared; `MarkReplaying` refuses with `eligible verifier ring below 3 (spec §7.3 floor)`; the WorkSet lists the block under `Unmarkable`, not `SealedUnmarked`, so the orchestrator proposes nothing; the first pass after the ring reaches three marks it with the block-seeded set, and the ring, position and cursor are stored. Tests: Task 70 `TestAdmissionAnswersVerifiersUnavailableBelowThreeEligible`, `TestVerifiersUnavailableIsCheckedRightAfterSourceUnavailable`, `TestVerifiersUnavailableLeavesTheCoordinateUnspent`, `TestMarkReplayingRefusesAnEligibleRingBelowThree`, `TestWorkSetDropsAnUnmarkableBlockUntilTheEligibleCountChanges`, `TestOrchestratorProposesNoMarkReplayingForAnUnmarkableBlock`, `TestOwnerAwareSelectionStoresTheRingThePositionAndTheCursor`.

## Task order and dependencies

Plan S3-E1 complete (`PROTO_TAG_P5`, `HG_TAG_P5`, `CORE_TAG_P5` on the ledger copy) and `ARBITER_TAG_P4` recorded by Plan S3-D → 64 → 65 → 66 → 67 → 68 → 69 → 70, applied in this order on the one branch `urwt/claude/multi-source-s3e` (contract §48), each task's commits on top of the previous task's. Data dependencies: 65 needs 64 (`Stage3Active`, `Stage3ActivationRefusal`, `Stage3ActivationNodes`, `Stage3Switches`); 66 needs 64 (`Stage3State`); 67 needs 66 (the v21 framing and kind byte); 68 needs 64 (`stage3SwitchOnLocked`, `onStage3SwitchOnLocked`); 69 and 70 need 64 (switches, cutover records, `poolVerifierLocked`) and 66 (`stage3FieldsPresent`, `validateStage3State`). Drafting: 65, 67 and 68 may be drafted in parallel with 66 once §44–§46 are fixed; 69 and 70 once §46 is; every overlap is resolved by the hunk rule. Plan S3-E3 continues on the same worktree after Task 70's push and may be drafted once §44–§47 are fixed; Plan S3-E4 Task 78 releases the branch. No arbiter release, PR or merge happens between Task 64's pin and Task 78 (K65).

## Task list

| Task | Repository | Deliverable |
|---|---|---|
| 64 | arbiter | worktree and base check; pins `PROTO_TAG_P5`, `HG_TAG_P5`, `CORE_TAG_P5` with the pin-fallout map `stage3PendingProtoFields`; `Params.Stage3`, `State.Stage3` (`Stage3State`, `Stage3SwitchCutover`), the switch names; the Params-only `stage3` rules and host-bound `verifiers` rules (K68, K69); the every-field `cloneVerifiers`, `cloneParams` and `normalizeConsensusParams`; the Apply-only `stage3MembershipRefusalLocked`; the activation Apply and the cutover records; the stage-3 FSM fixture |
| 65 | arbiter | `server/stage3_gate.go` (`arbiter-stage3-voters`, `probeFeatureVoters`), the post-activation registration refusal, `stage3` in `consensusMutableParams`, `ConsensusParamsState.stage3_state` and `TableRegistrySnapshot.stage3` served; `arbiter-admin consensus update --stage3`, `--stage3-*`, `--stage3-switch`, `--indexer-failure-domain`, the extended `--verifier`, the `stage3Activation` gate with its quarantined-track exemption |
| 66 | arbiter | snapshot v21 (`snapshotVersionV21`, the body-kind byte, the 14-byte envelope), v20 until `stage3` commits, the `stage3` document member, `stage3FieldsPresent`, `fsm/stage3_restore.go` with `validateStage3State` and `validateStage3Activation`, the frozen `stage3_v21_plain.snapshot` |
| 67 | arbiter | the interned body (`InternSnapshotBody`, `RehydrateSnapshotBody`), write-then-verify in `Persist`, `raft.snapshot_body`, `SnapshotEncodingStats`, `arbiter_snapshot_encoding_fallback_total`, `arbiter_snapshot_body_bytes{kind}`, the frozen `stage3_v21_interned.snapshot`, the forced-mismatch test; the offline replay and size measurement over local voter copies in the throwaway work directory `$T` (removed at the end), evidence `$W.replay-e2.txt` and `$W.v21size-e2.txt` beside the worktree |
| 68 | arbiter | eviction voiding (switch 2): explicit refusals at every signer's call site, parked claims (K72), `reevaluateBlock` skipping voided evidence, re-evaluation on eviction and at switch-on, voided evidence missing in the WorkSet |
| 69 | arbiter | purge sets (switch 1): `ScopedKeeperPath` marking in `applyAddTable`, `PurgeMembers`, the owner + pool + members purge-required set, the switch-on rerun of `completePurgesLocked`, the registry view and `tableRegistryToPB`, `validateStage3Incarnations` |
| 70 | arbiter | owner-aware selection (switch 3): pool-only eligible ring with today's block-seeded start, stored ring, position and cursor block, `VERIFIERS_UNAVAILABLE` after `SOURCE_UNAVAILABLE`, `WorkSet.Unmarkable`, the read RPC's ring, `validateStage3Selection`; the branch push |

## File Structure

Line anchors in the tasks refer to the base revisions of the header, re-anchored on the branch base under the hunk rule.

**arbiter** (Tasks 64–70; branch `urwt/claude/multi-source-s3e`)
- Task 64: `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock` (pins); `fsm/state.go` (`Params.Stage3`, `State.Stage3`); `fsm/stage3.go` (new: `Stage3State`, `Stage3SwitchCutover`, switch names, `verifyStage3Transition`, `verifyHostBoundVerifiers`, `stage3MembershipRefusalLocked`, `stage3ActivationRefusalLocked`, `activateStage3Locked`, `recordStage3SwitchesLocked`, `onStage3SwitchOnLocked`, `poolVerifierLocked`, `verifierEntryLocked`, `Stage3ActivationNodes`); `fsm/consensus_updates.go`, `fsm/signed_claims.go` (`cloneVerifiers`), `fsm/params_identity.go`, `fsm/consensus_reads.go` (`cloneParams`); `fsm/stage3_fixture_test.go`, `fsm/stage3_params_test.go`, `fsm/stage3_activation_test.go` (new); the server and fsm mirror tests that the pin fallout touches (`server/table_registry_test.go`, `server/safestate_l3block_test.go`, `server/safestate_stage3_test.go`: `stage3PendingProtoFields`, `requireEveryFieldExcept`); `fsm/BUILD.bazel`.
- Task 65: `server/stage3_gate.go`, `server/stage3_gate_test.go` (new); `server/tracks_gate.go` (`probeFeatureVoters`), `server/consensus_admin.go` (barrier condition, gate call, `consensusMutableParams`), `server/membership.go`, `server/signed_claims_test.go`, `server/membership_test.go`; `cmd/arbiter-admin/stage3.go`, `cmd/arbiter-admin/stage3_test.go` (new), `cmd/arbiter-admin/main.go` (flags, usage text, `activationGate.exempt`, `keyValuesOptional`, `--verifier`), `cmd/arbiter-admin/BUILD.bazel`.
- Task 66: `fsm/snapshot.go` (versions, writer, reader, `stage3FieldsPresent`), `fsm/snapshot_count.go` (`snapshotEnvelopeBytes`), `fsm/consensus_history.go` (the bootstrap `stage3` refusal), `fsm/state_size.go` (Plan S3-A's: the v21 envelope); `fsm/stage3_restore.go` (new); `fsm/snapshot_v21_test.go` (new), `fsm/state_size_test.go` (the edited component list); `fsm/testdata/stage3_v21_plain.snapshot` (new, frozen); `README.md` (component list).
- Task 67: `fsm/snapshot_interned.go`, `fsm/snapshot_interned_test.go` (new); `fsm/snapshot.go` (`fsmSnapshot`, `Persist`, `SnapshotBodyMode`, `SetSnapshotBodyMode`, `SnapshotEncodingStats`, the kind-1 reader), `fsm/state_size.go`; `fsm/testdata/stage3_v21_interned.snapshot` (new, frozen); `config/config.go`, `config/raw.go`, `config/config_test.go` (`raft.snapshot_body`); `cmd/arbiter/services.go`; `metrics/snapshot.go`, `metrics/snapshot_test.go`; `README.md` (series and the example alert).
- Task 68: `fsm/eviction_voiding.go`, `fsm/eviction_voiding_test.go` (new); `fsm/apply.go` (`applyRegisterRC`, `applyRecordAttestation`, `applyRecordByteSideScan`, `applyRecordPromotionAck`, `applyRecordCleanupAck`, `applyEvictNode`), `fsm/rejected_cleanup.go`, `fsm/signed_claims.go` (`adoptParkedClaimsLocked`), `fsm/threeway.go`, `fsm/reads_work.go` (`evidencePresence`), `fsm/stage3.go` (the switch hook).
- Task 69: `fsm/table_registry.go` (fields, view copy), `fsm/apply_table_registry.go` (`applyAddTable`, `purgeRequiredLocked`), `fsm/stage3.go` (the switch hook), `fsm/stage3_restore.go`, `fsm/snapshot.go` (`stage3FieldsPresent`); `server/table_registry.go`; `fsm/purge_stage3_test.go` (new), `server/table_registry_test.go`.
- Task 70: `fsm/state.go` (`BlockVerification` fields), `fsm/select.go` (`poolRingLocked`, `eligibleRingLocked`, `ringStart`, `initialVerifierSet`), `fsm/apply.go` (`applyMarkReplaying`), `fsm/admission.go`, `fsm/reads_work.go`, `fsm/reads_work_types.go` (`Unmarkable`), `fsm/reads_verification.go` (Plan S3-A's view), `fsm/stage3_restore.go`, `fsm/snapshot.go` (`stage3FieldsPresent`); `server/safestate_stage3.go`; `fsm/selection_stage3_test.go` (new), `orchestrator/dispatch_test.go`, `server/safestate_stage3_test.go`.

**Local only, never committed** (Task 67): the work directory `$HOME/si-devnet2-multisource-s3e2-replay` (removed at the end of Task 67) and the evidence files `~/src/remotesrc/claude/arbiter/multi-source-s3e.replay-e2.txt` and `~/src/remotesrc/claude/arbiter/multi-source-s3e.v21size-e2.txt` (contract §48). The P5 rehearsal directory `$HOME/si-devnet2-multisource-s3e-rehearsal` is Plan S3-E4 Task 78's (decision D37).

**arbiter-proto, arbiter-core, housegate, sentio-node, production:** no change in this plan (housegate is read for the ledger only).

## Spec coverage map

| Spec item | Tasks |
|---|---|
| §6.1 `verifiers` entry fields: host, replica signer, consent, weight, failure domain, flags; `shared_rmt` rule; consent verified on add and key rotation; member node id unused; host enrolled | 64 (rules), 65 (`--verifier`); Plan S3-E1 (proto, canonical types, normalizer) |
| §6.1 `stage3` parameter: carried once set, one-way switches and `strict_copies`, non-decreasing `min_safe_copies` and `group_size`, failure domains for every enrolled indexer | 64 (K69), 65 (flags) |
| §6.1 per-group verifier floor | seam in 64 (`stage3MembershipRefusalLocked`); Plan S3-E3 Task 71 |
| §7.3 owner-aware selection for databases without a group, owner-hosted exclusion, stored ring, position and cursor, `VERIFIERS_UNAVAILABLE`, unmarkable blocks leaving the WorkSet | 70; the group half: Plan S3-E3 Task 73 |
| §8.8 FSM half: scoped marking from the `purge` switch, purge-required set (owner, pool, members), reruns | 69; `purge_members` accumulation: Plan S3-E3 Task 71; the data-plane paths: Plan S3-E1 Task 61 |
| §10.3 activation: one update setting `stage3` with every switch off, the stage-2 drain gate, no empty open block or global prefix, voter probe, `stage3_v1` from every non-quarantined data-plane node | 64 (Apply), 65 (gate, registration refusal, arbiter-admin gate); rehearsal and live: Plan S3-E4 Tasks 81–82 |
| §10.3 v21 container with both bodies, written once `stage3` commits, refused by older binaries; write-then-verify outside the lock; fallback metric; operational writer setting | 66, 67; alert and value: Plan S3-E4 Task 80 |
| §10.4 switch order, each switch an authority-signed update; steps 1–3 | 64 (rules, cutovers), 69, 68, 70; steps 4–10: Plans S3-E3, S3-E4, S3-F |
| §13.2 lossless interned encoding, byte-exact rehydration, size estimate on the measured doc | 67 (K70, K71; `$W.v21size-e2.txt`) |
| §14 eviction voiding: every listed message refused, evidence voided unless latched or challenged, restore unchanged, registration unchanged | 68 (K72); `MemberReady` and checkpoints: Plan S3-E3 |
| §17 FSM: VERIFIERS_UNAVAILABLE; selection excluding owner-hosted verifiers and with automatic fill (pool half); activation with a quarantined track; v21 interned and plain bodies, the forced fallback and an offline digest replay of the devnet2 log; eviction voiding including evidence; purge sets on scoped and unscoped paths | 70, 64, 66, 67, 68, 69; the latch-then-evict re-selection case and the group half: Plan S3-E3 |
| §18 v21 is irreversible and live at the activation | 66, 67 (rollback possible until the activation, K70); Plan S3-E4 |
| §20 umbrella D7 and §6.1 (`verifiers` gain host binding and flags), §6.5 (the selection floor becomes per group, owner-hosted verifiers excluded) | 64, 70 (pool half); Plan S3-E3 |

## Follow-ups (not in this plan)

- Placement, groups and backfill, `RebalanceGroup`, the seal key and header `group_key`, `MemberReady` and the group half of the eligible ring, `ReselectVerifiers`, `MemberCheckpoint`, classes and `MarkMemberLagging`, the per-group verifier floor and `purge_members` accumulation: Plan S3-E3 (Tasks 71–75), on this branch.
- The copy floor, strict mode and code-13 backpressure, the Leaving release, the k-raise precondition, the narrowed drain gate, `pre_stage3_golden.json`, the integration harness with real members, the release of the branch, the sentio-node release, the production monitoring and `raft.snapshot_body` value, the v21 runbook, rehearsal and live activation: Plan S3-E4 (Tasks 76–82); `TestNoStage3ProtoFieldIsPending` closes the pin-fallout map.
- Node-local in-memory interning, which would lower the memory term of L (K71): after history pruning (v22) is designed; until then the voter budget re-check of Plan S3-E4 Task 80 and Plan S3-A's state-size alerts bound it.
- The read-cap gauge `arbiter_snapshot_doc_read_cap_bytes` and the `ArbiterSnapshotDocNearReadCap` rule still compare the plain document with the cap, which after v21 applies to the stored body (K71); Plan S3-E4 Task 80 decides whether the rule moves to `arbiter_snapshot_body_bytes`.
- Spec §12's refusal to list a third party's member has no `operator_id` to check (K68): an operating rule in Plan S3-F's runbook.
- An operator RPC for `ReselectVerifiers` and a `GetStage3State` read are not added (Plan S3-E1 K64; Plan S3-E3 decides the former).
- The purge cutover's `IncarnationSeq` stays out of `Stage3StateView` (decision D33, contract §45): no runbook or live task reads it; a later task that needs it adds it to the view in its own task and records it.
- A metric of unmarkable blocks: the WorkSet carries them and `arbiter_admission_rejects_total{code="ADMISSION_CODE_VERIFIERS_UNAVAILABLE"}` counts the admission refusals; a gauge, if the P6 soak needs one, belongs to Plan S3-E4 Task 80.
- Plan S3-E1's contract spells every consumed name as contract §48 now lists it (reconciled by the cross-plan consistency pass); a later change there is renamed here.

---

## Task 64: arbiter — P5 worktree and pins; the stage3 parameter, its rules, the host-bound verifiers rules and the v21 activation (S3-E2)

Spec §6.1 (the `verifiers` entry fields and rules, the `stage3` parameter rules), §10.3 (the activation: "one authority-signed update that sets `stage3` with every switch off", judged by the stage-2 drain gate, with no empty open block and no global prefix), §10.4 (the switch order, each switch its own authority-signed update) and the §17 FSM test "activation with a quarantined track", as fixed by contract §44–§45 and clarifications K68 (no host-bound entry before `owner_aware_selection` is committed, a fixed host for an entry's life) and K69 (what restore replays, what reads state, and the order). This is the first task of the P5 arbiter branch: it creates the worktree that Tasks 65–70 and Plans S3-E3 and S3-E4 reuse, pins the three Plan S3-E1 releases, and lands what every later stage-3 task builds on: `Params.Stage3`, `State.Stage3` with one cutover record per switch, the Params-only rules that restore replays, the host-bound entry rules, the every-field `cloneVerifiers` (the scoping proposal's defect D3; its other two rebuild sites are Plan S3-E1 Tasks 59 and 60), the Apply-only seam `stage3MembershipRefusalLocked` and the activation Apply. No switch has behaviour yet: Apply accepts each one in order and records its cutover, and Tasks 68–70 and Plans S3-E3/S3-E4 attach behaviour through `onStage3SwitchOnLocked` and `stage3SwitchOnLocked`.

Line anchors are arbiter `526a721` (`v0.13.0`, `origin/main` when this was drafted: Plan S3-A merged as arbiter#133). For every fsm source file this task edits by hand they are identical to the header's `fb42260` anchors (`fsm/BUILD.bazel`, which gazelle writes, gained Plan S3-A's files); `server/safestate_stage3_test.go` is Plan S3-A Task 7's and exists only from `526a721`. The branch starts from `origin/main` at or after `ARBITER_TAG_P4`, which also carries Plan S3-C2's `ARBITER_PIN_P2` and Plan S3-D Task 51; every hunk below on a file those plans touched is applied by the hunk rule (the later intent on top of what is there), and Step 3 lists those files.

**Files:**
- Modify (pin commit): `go.mod` (`:18` `github.com/housegate/housegate v0.20.0`, `:20` `github.com/sentioxyz/arbiter-core v0.15.0`, `:21` `github.com/sentioxyz/arbiter-proto v0.12.0` at `526a721`; at the branch base the `ARBITER_TAG_P4` values), `go.sum`, `MODULE.bazel` (`:9-16` the two `bazel_dep`s, `:19-31` the two `git_override`s, written by the update scripts), `MODULE.bazel.lock`
- Modify (pin commit): `server/safestate_stage3_test.go` (Plan S3-A Task 7's: the import block `:13` gains `protoreflect`; after `requireEveryField` `:25-34` the map `stage3PendingProtoFields`, `stage3ProtoFieldPending` and `requireEveryFieldExcept`; in `TestBlockVerificationToPBCarriesEveryField` `:159` the first of its three `requireEveryField` calls becomes `requireEveryFieldExcept`), `server/table_registry_test.go` (`TestTableRegistryMirrorsProto`, the field loop `:38-40`), `server/safestate_l3block_test.go` (`TestL3BlockHeaderMirrorsProto`, the field loop `:72-74`)
- Modify: `fsm/signed_claims.go` (`cloneVerifiers` `:92-102`; new `cloneVerifierEntry` right after it)
- Create: `fsm/stage3.go`
- Modify: `fsm/state.go` (`Params`: `Stage3` after `TracksEnabled` `:127`; `State`: `Stage3` after `Tracks` `:400`), `fsm/consensus_updates.go` (`applyUpdateConsensusParams`: after `signedClaimsMembershipLocked` `:51-54`, before `wasEnabled :=` `:88`, after the tracks block `:144-148`; `verifyConsensusTransition`: after `next.TracksEnabled = …` `:246`), `fsm/params_identity.go` (`normalizeConsensusParams` `:64-66`), `fsm/consensus_reads.go` (`cloneParams` `:88-90`)
- Create: `fsm/stage3_fixture_test.go`, `fsm/stage3_params_test.go`, `fsm/stage3_activation_test.go`
- Modify: `fsm/BUILD.bazel` (gazelle: `stage3.go` in the `go_library` srcs after `"snapshot_query_block.go"`; the three test files in the `go_test` srcs after `"source_ack_test.go"`; no deps change)
- Test: the files above; unchanged and green: every golden and vector test (`TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, `TestPreActivationHistoryIsByteIdenticalAcrossTracks`, `TestL3CommitmentPreimagesAreFrozen`, `TestL3TrackHeaderPreimagesAreFrozen`, `TestL3TransitionHeaderPreimageIsFrozen`, `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`, `TestPreTracksParamsDigestIsTheStageOneDigest`, `TestPreActivationParamsDigestIsTheLegacyDigest`, the shared vector tests) and every `TestDeterminism_*`

**Interfaces:**
- Consumes:
  - Ledger (contract §48, decision D34): `PROTO_TAG_P5`, `HG_TAG_P5`, `CORE_TAG_P5` (Plan S3-E1 Appendix C), `ARBITER_TAG_P4`, `ARBITER_DIGEST_P4` (Plan S3-D Appendix C), read with `s3-ledger.py get` (the tool of Plan S3-C1 contract §19, written verbatim by Step 2).
  - `PROTO_TAG_P5` (Plan S3-E1 Task 57, contract §39), reached by arbiter only through arbiter-core `wire` and the server: the fields the pin fallout meets, `TableIncarnation.scoped_keeper_path = 19`, `purge_members = 20` and `BlockVerification` 14–22 (`eligible_ring`, `ring_position`, `marked_cursor_block`, `group_key`, `group_members`, `group_fill`, `group_databases`, `reselections`, `reselections_at_latch`); the new `ConsensusAdmin` (`RebalanceGroup`, `MarkMemberLagging`) and `VerifierGateway` (`SubmitMemberReady`, `SubmitMemberCheckpoint`) RPCs, which answer `Unimplemented` through the embedded stubs (`server/consensus_admin.go:26`, `server/gateway.go:15`).
  - `HG_TAG_P5` (Plan S3-E1 Task 58): nothing arbiter calls changes; the pin keeps arbiter's housegate equal to the one `CORE_TAG_P5` requires.
  - `CORE_TAG_P5` (Plan S3-E1 Tasks 59, 60, 63, contract §41): `arbiter.Stage3Params{GroupSize uint32; NetworkFillDefault bool; MinSafeCopies uint32; StrictCopies bool; IndexerFailureDomains []IndexerFailureDomain; MaxLagPublications, LagLow uint32; EvidenceTimeoutL2Blocks uint64; MaxUncleanedPartsPerPartition, MaxUncleanedPartsPerTable uint32; ExcludeQuarantinedHosts, Purge, EvictionVoiding, OwnerAwareSelection, Placement, Reselection, Checkpoints, NarrowedDrain bool}`, `arbiter.IndexerFailureDomain{IndexerID uint64; FailureDomain string}`, `var arbiter.Stage3Switches []string`, `func (p Stage3Params) SwitchOn(name string) bool`, `func (p Stage3Params) FailureDomain(indexerID uint64) (string, bool)`, `arbiter.ConsensusParamsUpdate.Stage3 *Stage3Params`, the `arbiter.VerifierEntry` fields `HostIndexerID *uint64`, `ReplicaSigner`, `ConsentJWS string`, `Weight uint32`, `FailureDomain string`, `Verify`, `Replicate`, `Counts`, `SharedRMT bool` (each `omitempty`), `arbiter.Stage3Feature = "stage3_v1"` in `arbiter.LocalNodeFeatures()`; `authority.GroupMemberConsentStatement{NetworkID, GenesisSnapshotID string; HostIndexerID uint64; NodeID string; Ed25519Pubkey []byte; ReplicaSigner string}`, `func authority.VerifyGroupMemberConsent(stmt GroupMemberConsentStatement, jws, signer string) error`, `func (s *authority.Signer) SignGroupMemberConsentAt(stmt GroupMemberConsentStatement, iat int64) (string, error)`; `authority.NormalizeConsensusParamsUpdate` copying every new field, with the §41 refusals the tests quote (`stage3: switch %s requires %s`, `stage3 requires tracks_enabled in the same update`, `stage3: indexer_failure_domains names indexer %d, which has no si_indexers entry`, `stage3: the placement switch needs a failure domain for every si_indexers entry; indexer %d has none`, `verifiers: %s: an entry without host_indexer_id carries no replica_signer, consent_jws, weight, failure_domain or member flags`, `verifiers: %s: an entry with host_indexer_id needs the stage3 parameter in the same update`); `wire.ConsensusParamsUpdateToPB`/`FromPB`, `wire.VerifierEntriesToPB`/`FromPB`, `wire.Stage3ParamsToPB`/`FromPB` carrying every field (every fsm test command round-trips through them in `mkLog`).
  - Plan S3-A (contract §4, Task 7): `requireEveryField` and `TestBlockVerificationToPBCarriesEveryField` (`server/safestate_stage3_test.go`).
  - Existing arbiter: `verifyConsensusTransition`, `validateConsensusHistory`, `signedClaimsMembershipLocked`, `drainGateRefusalLocked`, `tracksActiveLocked`, `tracksActivationRefusalLocked`, `indexerTrackLocked`, `IndexerTrack.Quarantined`, `purgeNodeIDsLocked`, `PurgeNodeSet`, `findSIIndexer`, `siIndexerLocked`, `hasRole`, `consensusParamsEqual`, `cloneParams`, `normalizeConsensusParams`, `appendIncarnation`'s sequence rule (`fsm/table_registry.go:196-198`), `snapshotDocument`, `safePrefixLocked`; fsm test helpers `tracksFSM`, `tracksReadyFSM`, `tracksUpdate`, `trackStateJSON`, `forgeConsensusChange`, `admitTrack`, `trackEnvelope`, `bindTrackClaim`, `verifyBlock`, `promoteTrack`, `ackTrackPromotion`, `passTrackBlock` (`fsm/tracks_fixture_test.go`), `signedConsensusUpdate`, `rejectConsensusUnchanged` (`fsm/consensus_updates_test.go`), `authorityFixture` (`fsm/promotion_test.go:16`), `siEntry` (`fsm/signed_claims_params_test.go:45`), `verifierRegistration`, `verifierMarkActive` (`fsm/signed_messages_fixture_test.go:73,87`), `ed25519KeyFor` (`fsm/evidence_test.go:16`), `snodeSigner`, `ownedClaim`, `pinnedEvidence`, `partPromotion`, `partPromotionAck`, `partCleanup`, `partCleanupAck` (`fsm/pre_tracks_golden_test.go`), `quarantineQ`, `releaseQ` (`fsm/quarantine_fixture_test.go`), `anchorFinal` (`fsm/table_set_transition_pipeline_test.go:57`), `anchorForBlock`, `mustApply`, `testPubkey` (`fsm/fsm_test.go`), `manifestFromInputs` (`fsm/promotion_frontier_test.go:22`), `mustSeal`, `mustGenesisID`, `testAccount`, `snapshotBytes`, `restoreInto`.
- Produces (contract §44–§45, verbatim):
  - fsm state: `Params.Stage3 *arbiter.Stage3Params` (`json:"stage3,omitempty"`, after `TracksEnabled`), `State.Stage3 *Stage3State` (after `Tracks`); `type Stage3State struct{ ActivationEpoch, ActivationBlockSeq, ActivationPromotionSeq uint64; Switches []Stage3SwitchCutover }` (tags `activation_epoch`, `activation_block_seq`, `activation_promotion_seq`, `switches,omitempty`); `type Stage3SwitchCutover struct{ Switch string; Epoch, BlockSeq, PromotionSeq, IncarnationSeq uint64 }` (tags `switch`, `epoch`, `block_seq`, `promotion_seq`, `incarnation_seq,omitempty`).
  - fsm names: `Stage3SwitchPurge`, `Stage3SwitchEvictionVoiding`, `Stage3SwitchOwnerAwareSelection`, `Stage3SwitchPlacement`, `Stage3SwitchReselection`, `Stage3SwitchCheckpoints`, `Stage3SwitchNarrowedDrain`; `func Stage3Switches() []string`; `func stage3SwitchValue(p *arbiter.Stage3Params, name string) bool`; `func (f *FSM) stage3ActiveLocked() bool`, `func (f *FSM) Stage3Active() bool`, `func (f *FSM) stage3SwitchOnLocked(name string) bool`, `func (f *FSM) Stage3SwitchOn(name string) bool`; `func cloneStage3Params(p *arbiter.Stage3Params) *arbiter.Stage3Params`; `func (f *FSM) verifierEntryLocked(nodeID string) (arbiter.VerifierEntry, bool)`; `func (f *FSM) poolVerifierLocked(nodeID string) bool`; `func verifyStage3Transition(previous, next Params, update *arbiter.Stage3Params) (*arbiter.Stage3Params, error)`; `func verifyHostBoundVerifiers(previous, next Params, genesisID string) error`; `func (f *FSM) stage3MembershipRefusalLocked(next Params) string` (seam); `func (f *FSM) stage3ActivationRefusalLocked() string`, `func (f *FSM) Stage3ActivationRefusal() string`; `func (f *FSM) activateStage3Locked()`; `func (f *FSM) recordStage3SwitchesLocked(previous *arbiter.Stage3Params)`; `func (f *FSM) onStage3SwitchOnLocked(name string)` (seam, a `switch name {` with no case yet); `func (f *FSM) Stage3ActivationNodes() []string`; `cloneVerifiers` copying every field.
  - Refusal texts: every text of contract §45's Params-only list, its Apply-only one and the activation's (`stage3 activation requires the tracks activation`), verbatim.
  - Beyond the contract (recorded in Appendix B): fsm `cloneVerifierEntry`, `findVerifierEntry`, `stage3FailureDomain`, `hostBoundFieldsSet`, `validReplicaSigner`; server test helper `stage3ProtoFieldPending(fd protoreflect.FieldDescriptor) bool`.
  - Server test map (contract §44): `var stage3PendingProtoFields map[string]string` with exactly the eleven entries the pin fallout needs (nine `BlockVerification.*`, two `TableIncarnation.*`), and `func requireEveryFieldExcept(t *testing.T, m proto.Message)`.
  - Test fixture (`fsm/stage3_fixture_test.go`, contract §45): `stage3ActivationParams() *arbiter.Stage3Params`, `stage3Update(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate`, `activateStage3(t *testing.T, f *FSM, a *authority.Signer)`, `stage3FSM(t *testing.T) (*FSM, *authority.Signer)`, `turnOnStage3Switch(t *testing.T, f *FSM, a *authority.Signer, name string)`, `stage3TestMember(host uint64) string`, `memberEntry(t *testing.T, f *FSM, host uint64, node, domain string) arbiter.VerifierEntry`, `rejectStage3Unchanged(t *testing.T, f *FSM, cmd wire.Command, reason string)`; beyond the contract (Appendix B): `stage3WithSwitch`, `turnOnStage3SwitchesThrough` (used by Task 70), `stage3MemberKey` (the node's deterministic ed25519 key), `stage3ReplicaSigner`, `memberConsent`, `hostSigner`, `withVerifier`, `stage3StateJSON`; in `fsm/stage3_params_test.go` `ownerAwareFSM` (switches 1–3 committed; Task 70's fixture), `setNonZero`, `stage3Previous`, `requireNormalizerRefuses`, `requireErrorContains`, `consentStatement`; in `fsm/stage3_activation_test.go` `stage3Activation`, `stage3Twin`, `stage3NeutralState`.
  - Tests (26, contract §45): `TestStage3ActivationSetsEveryParameterWithEverySwitchOff`, `TestStage3ActivationRequiresTheTracksActivation`, `TestStage3ActivationRefusesASwitchStrictCopiesOrAFloorAboveOne`, `TestStage3ActivationNeedsNoEmptyOpenBlockOrGlobalPrefix`, `TestStage3ActivationRunsTheStage2DrainGate`, `TestStage3ActivationWithAQuarantinedTrack`, `TestStage3ActivationWithEverySwitchOffChangesNoBehaviour`, `TestStage3MustBeCarriedOnceSet`, `TestStage3SwitchesAndStrictCopiesAreOneWay`, `TestStage3MinSafeCopiesAndGroupSizeNeverDecrease`, `TestStage3OneSwitchPerUpdate`, `TestStage3SwitchNeedsEveryEarlierSwitchCommitted`, `TestStage3FloorAboveOneNeedsCheckpointsAndNarrowedDrain`, `TestStage3PlacementNeedsADomainForEveryIndexer`, `TestStage3FailureDomainsNameOnlyEnrolledIndexers`, `TestStage3RecordsEachSwitchCutover`, `TestStage3RulesReplayOnRestore`, `TestHostBoundVerifierRefusedBeforeOwnerAwareSelection`, `TestHostBoundVerifierRules`, `TestHostBoundVerifierConsentIsVerifiedOnlyOnAddOrKeyRotation`, `TestVerifierHostIndexerIDNeverChanges`, `TestPoolVerifierCarriesNoHostBoundFields`, `TestMemberNodeIDMustBeUnusedByEveryRegisteredNode`, `TestCloneVerifiersCopiesEveryField`, `TestCloneParamsCopiesStage3`, `TestStage3ActivationNodesExemptTheSNodeOfAQuarantinedTrack`.

Notes:
- **Nothing runs before the activation (contract §44).** `Params.Stage3` and every new `VerifierEntry` field are `omitempty`, so with `stage3` absent the params digest, every signed `previous_params_digest` and every snapshot byte are those of `ARBITER_TAG_P4` (`TestPoolVerifierCarriesNoHostBoundFields` recomputes the stage-2 digest from a stage-2-shaped struct; the goldens run unchanged). Every new Apply branch tests `stage3Activation` or `f.stage3ActiveLocked()` first; `verifyStage3Transition` returns `nil, nil` for an update without `stage3` while none is committed, and `verifyHostBoundVerifiers` then only meets pool entries, which carry no host-bound field. `TestStage3ActivationWithEverySwitchOffChangesNoBehaviour` pins the switch-off identity for every later task: no switch is on in it, so Plans S3-E2 to S3-E4 keep it green as they add switch-gated behaviour.
- **Where each rule runs (K69).** Every rule that compares parameters alone is in `verifyConsensusTransition`, after the `tracks_enabled` rule and before the `max_writers` defence, so `validateConsensusHistory` replays it (`TestStage3RulesReplayOnRestore` forges each refused history with `forgeConsensusChange` and gets Apply's text back). The one rule that reads node records, a member node id unused by every registered node, is `stage3MembershipRefusalLocked`, right after `signedClaimsMembershipLocked`, like the writer and drain gates. The activation's only state precondition is the drain gate Apply already runs for every update (`fsm/consensus_updates.go:76-78`); Apply adds no state check of its own for it. `stage3ActivationRefusalLocked` (the tracks activation committed, then the drain gate's own string) is what Task 65's leader gate predicts from the same two rules: Apply refuses an activation before the tracks activation in `verifyStage3Transition` (`consensus update cannot set stage3 before the tracks activation`) and a pending drain with the drain gate's text, which `TestStage3ActivationRunsTheStage2DrainGate` compares with the prediction. Unlike `tracksActivationRefusalLocked` it asks for no empty open block and no global prefix.
- **Refusal order.** `verifyStage3Transition` refuses in contract §45's order: before tracks, an activation with a switch, strict copies or a floor, a dropped `stage3`, the defence-in-depth value check, a switch turned off, `strict_copies` off, a lowered floor or group size, two switches at once, a switch before an earlier one, the floor or strict before both `checkpoints` and `narrowed_drain`, then the failure-domain map. `verifyHostBoundVerifiers` walks the entries in their committed (node-id) order and, per entry, applies K68 first, then the fixed host, the enrolled host, the replica signer, the weight, the domain, `shared_rmt`, the consent and the consent swap.
- **Normalizer first.** Plan S3-E1's normalizer refuses several of these updates before the FSM sees them, and `SignConsensusParamsUpdateAt` normalizes before it hashes, so no authority can sign them: a switch before an earlier one in the same parameters (`stage3: switch %s requires %s`), the value checks, the failure-domain map rules, a pool entry with host-bound fields, and a host-bound entry's host, replica signer, weight, domain and `shared_rmt`. The FSM keeps its own text for each (defence in depth, and the text restore gives a forged container); the tests reach those rules by calling `verifyStage3Transition` and `verifyHostBoundVerifiers` directly, after `requireNormalizerRefuses` has shown the normalizer's text. The consent, K68, the fixed host, the one-way rules, the one-switch rule, the activation rule and the committed-switch floor rule are the FSM's alone and are tested through Apply.
- **The floor rule reads committed switches.** `min_safe_copies > 1` and `strict_copies` need `checkpoints` and `narrowed_drain` on in the previous parameters, not merely in the update that turns `narrowed_drain` on (`TestStage3FloorAboveOneNeedsCheckpointsAndNarrowedDrain`): each switch has its own epoch and cutover (K69), and Plan S3-E4's k-raise precondition then runs against a committed narrowed drain gate. arbiter-core's static form (§41) checks the update's parameters alone; the FSM is stricter (Appendix B).
- **Host binding (K68).** A host-bound entry is refused until `owner_aware_selection` is committed in the previous parameters, also when the same update turns that switch on. An entry's `host_indexer_id` never changes while it is listed, nil and set included, so a pool verifier never becomes a member in place and a member never moves host. The host's consent (`authority.VerifyGroupMemberConsent` under the host's current `signer`, over this network, genesis, host, node id, ed25519 key and replica signer) is verified when the entry is added and when its ed25519 key or replica signer changes; an unchanged entry keeps the consent it was listed with even after its host's signer rotates, and may not swap its `consent_jws` without a key change. Registration already requires the listing (`verifierSignatureLocked`), so a member can register only after it is listed; `poolVerifierLocked` (the network pool that Tasks 69 and 70 count) is false for every host-bound entry, registered or not, and for an evicted node (`TestHostBoundVerifierRefusedBeforeOwnerAwareSelection` registers `member-1` after switch 3 to show it).
- **Cutovers.** `activateStage3Locked` and `recordStage3SwitchesLocked` run after the new params and the tracks block are in place and before `ConsensusEpoch++`, so `ActivationEpoch` and each cutover's `Epoch` are the epoch the update produces (`ConsensusEpoch + 1`, the 1-based position of its `ConsensusUpdates` entry; `stage3FSM`'s activation is epoch 4). A cutover's `BlockSeq` and `PromotionSeq` are read after `onStage3SwitchOnLocked`, so Plan S3-E3's placement seal lands before them; `purge` records `IncarnationSeq = len(Incarnations) + 1`, the seq `appendIncarnation` gives the next incarnation. `onStage3SwitchOnLocked` is a `switch name {` with only `default:`; Tasks 68 and 69 add their cases.
- **Until Task 66.** `State.Stage3` is not yet a snapshot member: a snapshot taken on this commit after the activation restores `Params.Stage3` (part of `params`) but not the record, and the container is still v20. Nothing is released between Task 64 and Task 78 (K65), the tests here that restore read only the parameters and the replayed history, and Task 66 adds the `stage3` member, the v21 writer and `validateStage3State` (which refuses a committed `stage3` without its record). `rejectStage3Unchanged` compares the record as JSON for that reason.
- **The twin test.** `TestStage3ActivationWithEverySwitchOffChangesNoBehaviour` restores a copy of a `tracksFSM` network (so both share the random authority of `authorityFixture`), commits the same update to both (a fourth pool verifier `v4` and `max_writers` 3, plus `stage3` on one), then applies each command of a two-owner history to both: `v4`'s signed registration and activation (so the block-seeded selection draws three of four and a changed start would show), admission and a refused duplicate, two seals, claims, evidence, anchors, promotions, acknowledgements, cleanups, a publication, a quarantine and its release. Answers must be equal, and the snapshot document equal after removing `params.stage3`, the `stage3` member, the consensus history and the registry version. The version is compared as a fixed delta that no later command may change, because Task 65 makes the activation bump it once (contract §45).
- **Pin fallout (contract §44).** At the branch base the pin breaks exactly two tests, both by fields a later task maps: `TestTableRegistryMirrorsProto` (`scoped_keeper_path`, `purge_members`, Task 69) and Plan S3-A's `TestBlockVerificationToPBCarriesEveryField` (`eligible_ring`, the first of the nine unset fields 14–22: Task 70 and Plan S3-E3 Tasks 72 and 74). Both learn the map; `TestL3BlockHeaderMirrorsProto` consults it too, though no header field is pending (`L3BlockTrack.group_key` sits in a sub-message it does not walk). The map holds exactly the eleven entries a failing test needs, each value the owning task's name as Tasks 69 and 70 delete it (`"Task 69"`, `"Task 70"`, `"Plan S3-E3 Task 72"`, `"Plan S3-E3 Task 74"`); Step 7 says what to do if a test of Plans S3-C2 or S3-D fails on another listed field. Plan S3-E4 Task 78's `TestNoStage3ProtoFieldIsPending` requires the map to be empty.
- **Raft tags 38–42 and `stage3_v1`.** `CORE_TAG_P5`'s decoder reads tags 38–42; no binary proposes them before their switch (Plan S3-E1 K67), and until Plan S3-E3 adds their Apply cases a command carrying one reaches `Apply`'s `default` and is refused `empty command`, deterministically on every voter. The pin puts `stage3_v1` into `arbiter.LocalNodeFeatures()` and so into `GetProtocolInfo` (K65): no arbiter release, tag, PR or merge happens from this pin through Task 78.
- **Verification while drafting** (Appendix B): a script read this markdown and applied it step by step (every `replace … with …` pair matched exactly once, every new file written as given, Step 16's append after one blank line) to a `git archive` export of arbiter `526a721`, whose `go.mod` replaced arbiter-proto and arbiter-core with local stand-ins, since no P5 tag exists yet: arbiter-proto was Plan S3-E1 Task 57's drafted tree (`v0.12.0` plus a `PROTO_TAG_P4` stand-in plus §39, regenerated; its `proto/` and `gen/` byte-identical to Task 57's final draft), arbiter-core was `cfd4f42` (`v0.15.0`) plus a stub of contract §41 (the types, `Validate`, `FailureDomain`, `SwitchOn`, the normalizer refusals quoted above, the consent purpose with its sign and verify, the wire converters, `stage3_v1`; not the `wire.Command` fields of Raft tags 38–42, which no arbiter test enumerates and Apply's `default` refuses, and Step 6's stop rule covers any difference). Steps 4 and 5 ran against the stand-ins (Step 5's output is the one shown), and `GOWORK=off GOFLAGS=-mod=mod GOPROXY=off go test` ran at every step. Observed: Step 3's count `3`; Step 6's pin fallout exactly as shown; Steps 8 and 13 passing; Step 11's three failing subtests; Step 18's build failure with the errors quoted; Step 23's `26` and the eight goldens; Step 24 `gofmt-clean`, `vet-clean`, `no-failures` under `-race` for every package, no `time.Now` in `fsm/`, and its status listing (without `fsm/BUILD.bazel`, which gazelle writes). The result is byte-identical to the drafting tree. Then 49 single-point mutations, each failing its named test and none surviving: 41 of `fsm/stage3.go` (every refusal of `verifyStage3Transition` and `verifyHostBoundVerifiers` dropped or weakened, the activation and cutover epochs, the purge incarnation seq, the activation promotion seq, K68 off or read from the next parameters, the fixed host in both directions, the shallow `cloneStage3Params`, two switches allowed, the quarantine exemption dropped, carry-once-set dropped, the consent check dropped or not rerun on an ed25519 or replica-signer rotation, the consent swap allowed, the floor rule read from the update, an evicted node id admitted as a member, an evicted node or a registered member counted in the pool, the placement and enrolled-indexer domain rules dropped, the pool rule dropped or missing `shared_rmt`, the two halves of `stage3ActivationRefusalLocked`), 6 of the wiring (the Apply-only call, the cutover call, the host-bound call, the host copy in `cloneVerifierEntry`, the `cloneStage3Params` calls in `cloneParams` and `normalizeConsensusParams`), and 2 behaviour changes gated on `stage3ActiveLocked` (a shifted selection start, an extra statement seq) that `TestStage3ActivationWithEverySwitchOffChangesNoBehaviour` catches. One `-race` run of the whole suite on the heavily loaded drafting machine timed out in `integration`'s `TestPipeline_EndToEndOverTheWire` (a wall-clock end-to-end test this task does not touch); it passed in three runs alone and in the final whole-suite `-race` run. Bazel and gazelle were not run (offline, and no P5 tag exists yet): the `fsm/BUILD.bazel` change is what gazelle writes for the new files, and the fsm `go_library` and `go_test` deps already list every package they import.

- [ ] **Step 1: Create the worktree**

Remote read (`git fetch`): needs explicit user confirmation in chat before running.

```bash
git -C /Users/uranuswch/Dev/projects/sentio-network/arbiter fetch origin --tags
bash ~/.agents/skills/urwt/scripts/urwt-create.sh /Users/uranuswch/Dev/projects/sentio-network/arbiter claude multi-source-s3e
```

Expected: one JSON object with `"action": "created"` whose `path` is `~/src/remotesrc/claude/arbiter/multi-source-s3e` (expanded) on the new branch `urwt/claude/multi-source-s3e` from `origin/main`. `W` below is that path; the Bash tool keeps no variables between calls, so prefix every command with `W=<that path>;` and the ledger values Step 2 prints. `"action": "existing"` means an earlier run created it: stop and ask, because Tasks 65–70 and Plans S3-E3 and S3-E4 reuse this worktree and it is never re-created. If stderr says `needs approval to execute`, ask the user to run, in their own terminal, `wt -C /Users/uranuswch/Dev/projects/sentio-network/arbiter config approvals add`, then rerun; never pass `--yes` or `--clobber`.

- [ ] **Step 2: Write the ledger tool and read the five consumed values**

Remote read (housegate `git fetch`): needs explicit user confirmation in chat before running.

```bash
h=/Users/uranuswch/Dev/projects/sentio-network/housegate; L="$HOME/.cache/s3-ledger"
git -C "$h" fetch -q origin
mkdir -p "$L" && chmod 700 "$L"
p=docs/superpowers/plans/2026-10-15-multi-source-si-s3c1-p1-backups.md
for ref in origin/main origin/urwt/claude/ms-si-stage3-plans origin/urwt/claude/ms-si-stage3-plans-cf; do
  if git -C "$h" cat-file -e "$ref:$p" 2>/dev/null; then echo "$ref"; break; fi
done
```

Expected: one ref, the ledger copy that holds Plan S3-C1 (`origin/urwt/claude/ms-si-stage3-plans-cf` until the stage-3 plans PR merges, then `origin/main`, decision D34); no output means the plans branch is not on origin: stop and ask the user to push it. Print the tool's text from that ref (`REF` is the printed ref):

```bash
h=/Users/uranuswch/Dev/projects/sentio-network/housegate; p=docs/superpowers/plans/2026-10-15-multi-source-si-s3c1-p1-backups.md
git -C "$h" show "REF:$p" | awk '/^The ledger tool `\$HOME\/\.cache\/s3-ledger\/s3-ledger\.py` \(contract §19/{p=1; next} p && /^```python$/{f=1; next} f && /^```$/{exit} f'
```

Expected: the python program of Plan S3-C1 contract §19, from `#!/usr/bin/env python3` to `    main(sys.argv[1:])`. Write it, verbatim and with nothing else, to `$HOME/.cache/s3-ledger/s3-ledger.py` with the Write tool (an earlier copy is the same text and is overwritten, contract §48). Then check it and read the values (local reads):

```bash
h=/Users/uranuswch/Dev/projects/sentio-network/housegate; L="$HOME/.cache/s3-ledger"
python3 -I "$L/s3-ledger.py" ref "$h" s3c1
python3 -I "$L/s3-ledger.py" show "$h" s3c1 \
  | awk '/^The ledger tool `\$HOME\/\.cache\/s3-ledger\/s3-ledger\.py` \(contract §19/{p=1; next} p && /^```python$/{f=1; next} f && /^```$/{exit} f' \
  | cmp - "$L/s3-ledger.py" && echo ledger-tool-verbatim
python3 -I "$L/s3-ledger.py" get "$h" PROTO_TAG_P5 HG_TAG_P5 CORE_TAG_P5 ARBITER_TAG_P4 ARBITER_DIGEST_P4
```

Expected: the same ref as above; `ledger-tool-verbatim`; then `PROTO_TAG_P5=v…`, `HG_TAG_P5=v…`, `CORE_TAG_P5=v…`, `ARBITER_TAG_P4=v…` and `ARBITER_DIGEST_P4=sha256:…`, each read from its owner's Appendix C on its own ledger copy. If `cmp` reports a difference, rewrite the file from the printed block and rerun. A line ending in `; stop` (a value not recorded yet, or malformed) means Plan S3-E1 or Plan S3-D is not done: stop and ask; never predict a value. Every `PROTO_TAG_P5=<ledger>; HG_TAG_P5=<ledger>; CORE_TAG_P5=<ledger>; ARBITER_TAG_P4=<ledger>` prefix below takes these values.

- [ ] **Step 3: Check the base and the three releases**

Remote read (tag fetches of arbiter-proto, arbiter-core and housegate): needs explicit user confirmation in chat before running.

```bash
W=<worktree path>; PROTO_TAG_P5=<ledger>; HG_TAG_P5=<ledger>; CORE_TAG_P5=<ledger>; ARBITER_TAG_P4=<ledger>
R=/Users/uranuswch/Dev/projects/sentio-network
git -C "$W" log --oneline -1
git -C "$W" merge-base --is-ancestor "$(git -C "$W" rev-parse "$ARBITER_TAG_P4^{commit}")" HEAD && echo base-carries-p4
git -C "$W" diff --stat 526a721 HEAD -- fsm/state.go fsm/consensus_updates.go fsm/signed_claims.go fsm/params_identity.go fsm/consensus_reads.go fsm/BUILD.bazel server/table_registry_test.go server/safestate_l3block_test.go server/safestate_stage3_test.go
git -C "$W" grep -c '^	requireEveryField(t, want)$' -- server/safestate_stage3_test.go
git -C "$R/arbiter-proto" fetch -q --tags && git -C "$R/arbiter-core" fetch -q --tags && git -C "$R/housegate" fetch -q --tags
git -C "$R/arbiter-proto" show "$PROTO_TAG_P5":proto/consensus.proto | grep -cE '^  Stage3Params stage3 = (9|14);$|^  optional uint64 host_indexer_id = 3;$|^  bool shared_rmt = 11;$|^  Stage3StateView stage3_state = 9;$'
git -C "$R/arbiter-proto" show "$PROTO_TAG_P5":proto/table_registry.proto | grep -cE '^  bool scoped_keeper_path = 19;$|^  repeated string purge_members = 20;$|^  Stage3Params stage3 = 12;$'
git -C "$R/arbiter-proto" show "$PROTO_TAG_P5":proto/arbiter.proto | grep -cE '^  repeated string eligible_ring = 14;$|^  uint32 reselections_at_latch = 22;$'
git -C "$R/arbiter-core" show "$CORE_TAG_P5":go.mod | grep -cE "^[[:space:]]+github.com/sentioxyz/arbiter-proto $PROTO_TAG_P5\$|^[[:space:]]+github.com/housegate/housegate $HG_TAG_P5\$"
git -C "$R/housegate" show "$HG_TAG_P5":go.mod | grep -cE "^[[:space:]]+github.com/sentioxyz/arbiter-proto $PROTO_TAG_P5\$"
```

Expected: HEAD on `origin/main`; `base-carries-p4` (without it, stop: Plan S3-D Task 51 releases `ARBITER_TAG_P4` from `main` before this branch starts, cross-plan conventions); a diff stat naming only files Plans S3-C2 and S3-D changed after `526a721` (re-read each such file before applying a hunk of this task to it, the hunk rule); `server/safestate_stage3_test.go:3` (Plan S3-A Task 7's three `requireEveryField(t, want)` calls; Step 7 edits the last, in `TestBlockVerificationToPBCarriesEveryField`); then `5`, `3`, `2` (the Plan S3-E1 §39 fields this task and its pin fallout read), `2` (`CORE_TAG_P5` requires exactly `PROTO_TAG_P5` and `HG_TAG_P5`, so MVS selects no other version of either) and `1`. If a count differs, stop and reconcile with Plan S3-E1 Tasks 57–63 instead of improvising.

- [ ] **Step 4: Pin arbiter-proto, housegate and arbiter-core** (contract §48, in this order)

`go get`, the update scripts, `bazel mod tidy` and gazelle download the three modules and the two new `git_override` sources: remote read, needs explicit user confirmation in chat before running.

```bash
W=<worktree path>; PROTO_TAG_P5=<ledger>; HG_TAG_P5=<ledger>; CORE_TAG_P5=<ledger>
cd "$W" && GOWORK=off go get "github.com/sentioxyz/arbiter-proto@$PROTO_TAG_P5" && GOWORK=off go mod tidy && bazel mod tidy && bazel run //:gazelle
cd "$W" && bash scripts/update-housegate.sh "$HG_TAG_P5"
cd "$W" && bash scripts/update-arbiter-core.sh "$CORE_TAG_P5"
cd "$W" && GOWORK=off go list -m github.com/sentioxyz/arbiter-proto github.com/housegate/housegate github.com/sentioxyz/arbiter-core
git -C "$W" status --short
```

Expected: `Updated Housegate dependency:` with `Go version:     <HG_TAG_P5>`; `Updated arbiter-core dependency:` with `Go version:     <CORE_TAG_P5>` (it delegates once more to `update-housegate.sh` with the housegate it selects, `scripts/update-arbiter-core.sh:148`, a no-op at `HG_TAG_P5`); `go list` prints `github.com/sentioxyz/arbiter-proto <PROTO_TAG_P5>`, `github.com/housegate/housegate <HG_TAG_P5>`, `github.com/sentioxyz/arbiter-core <CORE_TAG_P5>`; `git status` lists ` M MODULE.bazel`, ` M MODULE.bazel.lock`, ` M go.mod`, ` M go.sum` and nothing else (gazelle finds no new import).

- [ ] **Step 5: Check the consumed arbiter-core API**

```bash
W=<worktree path>
cd "$W" && for s in arbiter-core.Stage3Feature arbiter-core.Stage3Switches arbiter-core.Stage3Params.SwitchOn arbiter-core.Stage3Params.FailureDomain arbiter-core/authority.NormalizeConsensusParamsUpdate arbiter-core/authority.VerifyGroupMemberConsent arbiter-core/authority.Signer.SignGroupMemberConsentAt arbiter-core/wire.Stage3ParamsToPB arbiter-core/wire.Stage3ParamsFromPB; do GOWORK=off go doc -short "github.com/sentioxyz/$s"; done
cd "$W" && GOWORK=off go doc github.com/sentioxyz/arbiter-core.Stage3Params | grep -cE '^	(GroupSize|NetworkFillDefault|MinSafeCopies|StrictCopies|IndexerFailureDomains|MaxLagPublications|LagLow|EvidenceTimeoutL2Blocks|MaxUncleanedPartsPerPartition|MaxUncleanedPartsPerTable|ExcludeQuarantinedHosts|Purge|EvictionVoiding|OwnerAwareSelection|Placement|Reselection|Checkpoints|NarrowedDrain) '
cd "$W" && GOWORK=off go doc github.com/sentioxyz/arbiter-core.VerifierEntry | grep -cE '^	(HostIndexerID +\*uint64|ReplicaSigner|ConsentJWS|Weight|FailureDomain|Verify|Replicate|Counts|SharedRMT) '
cd "$W" && GOWORK=off go doc github.com/sentioxyz/arbiter-core.ConsensusParamsUpdate | grep -cE '^	Stage3 +\*Stage3Params '
cd "$W" && GOWORK=off go doc github.com/sentioxyz/arbiter-core/authority.GroupMemberConsentStatement | grep -cE '^	(NetworkID|GenesisSnapshotID|HostIndexerID|NodeID|Ed25519Pubkey|ReplicaSigner) '
```

Expected, exactly:

```text
const Stage3Feature = "stage3_v1"
var Stage3Switches = []string{ ... }
func (p Stage3Params) SwitchOn(name string) bool
func (p Stage3Params) FailureDomain(indexerID uint64) (string, bool)
func NormalizeConsensusParamsUpdate(cmd arbiter.ConsensusParamsUpdate) (arbiter.ConsensusParamsUpdate, error)
func VerifyGroupMemberConsent(stmt GroupMemberConsentStatement, jws, signer string) error
func (s *Signer) SignGroupMemberConsentAt(stmt GroupMemberConsentStatement, iat int64) (string, error)
func Stage3ParamsToPB(v *arbiter.Stage3Params) *pb.Stage3Params
func Stage3ParamsFromPB(m *pb.Stage3Params) *arbiter.Stage3Params
18
9
1
6
```

A missing symbol, another signature or another count stops this task: the code below is written against Plan S3-E1 contract §41 verbatim.

- [ ] **Step 6: Run the suite and see the pin fallout**

```bash
W=<worktree path>
cd "$W" && GOWORK=off go test ./... -count=1 2>&1 | grep -E '^(--- FAIL|FAIL|panic)|_test\.go:[0-9]+:'
```

Expected (line numbers as at the branch base):

```text
--- FAIL: TestBlockVerificationToPBCarriesEveryField (0.00s)
    safestate_stage3_test.go:159: BlockVerification.eligible_ring is not set by the test literal
--- FAIL: TestTableRegistryMirrorsProto (0.00s)
    table_registry_test.go:41: TableIncarnation: proto field "scoped_keeper_path" has no json tag
    table_registry_test.go:41: TableIncarnation: proto field "purge_members" has no json tag
FAIL
FAIL	github.com/sentioxyz/arbiter/server	…
FAIL
```

Every other package passes, the frozen goldens included (`TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, `TestPreActivationHistoryIsByteIdenticalAcrossTracks`, `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`, the L3 preimage tests): the pin is byte-neutral, and `stage3_v1` in `LocalNodeFeatures()` only lengthens the feature lists the gate tests build from it. If any other test fails, or one of these fails for another reason, stop (contract §44): the pin is not byte-neutral or a test meets a field the map below does not name.

- [ ] **Step 7: Name the pending fields and let the three tests consult them**

In `server/safestate_stage3_test.go`, the import block gains `protoreflect` (skip this one edit if a later plan already imports it there); replace:

```go
	"google.golang.org/protobuf/proto"
```

with:

```go
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
```

and replace the end of `requireEveryField`:

```go
			t.Fatalf("%s.%s is not set by the test literal", msg.Descriptor().Name(), fd.Name())
		}
	}
}

func stage3PartitionStatesView() fsm.PartitionStatesView {
```

with:

```go
			t.Fatalf("%s.%s is not set by the test literal", msg.Descriptor().Name(), fd.Name())
		}
	}
}

// stage3PendingProtoFields names, as <Message>.<field>, every PROTO_TAG_P5
// field (plan S3-E1 contract §39) that a mirror or every-field test of this
// package meets before the stage-3 task that maps it, with that task (plan
// S3-E2 contract §44). The owning task deletes its entries; plan S3-E4 Task
// 78's TestNoStage3ProtoFieldIsPending requires the map to be empty.
var stage3PendingProtoFields = map[string]string{
	"BlockVerification.eligible_ring":         "Task 70",
	"BlockVerification.ring_position":         "Task 70",
	"BlockVerification.marked_cursor_block":   "Task 70",
	"BlockVerification.group_key":             "Plan S3-E3 Task 72",
	"BlockVerification.group_members":         "Plan S3-E3 Task 72",
	"BlockVerification.group_fill":            "Plan S3-E3 Task 72",
	"BlockVerification.group_databases":       "Plan S3-E3 Task 72",
	"BlockVerification.reselections":          "Plan S3-E3 Task 74",
	"BlockVerification.reselections_at_latch": "Plan S3-E3 Task 74",
	"TableIncarnation.scoped_keeper_path":     "Task 69",
	"TableIncarnation.purge_members":          "Task 69",
}

// stage3ProtoFieldPending reports whether fd is a stage3PendingProtoFields
// entry.
func stage3ProtoFieldPending(fd protoreflect.FieldDescriptor) bool {
	_, pending := stage3PendingProtoFields[string(fd.ContainingMessage().Name())+"."+string(fd.Name())]
	return pending
}

// requireEveryFieldExcept is requireEveryField minus the fields of m's
// message that stage3PendingProtoFields names.
func requireEveryFieldExcept(t *testing.T, m proto.Message) {
	t.Helper()
	msg := m.ProtoReflect()
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		if fd := fields.Get(i); !stage3ProtoFieldPending(fd) && !msg.Has(fd) {
			t.Fatalf("%s.%s is not set by the test literal", msg.Descriptor().Name(), fd.Name())
		}
	}
}

func stage3PartitionStatesView() fsm.PartitionStatesView {
```

In the same file, in `TestBlockVerificationToPBCarriesEveryField` (fields 14–22 stay unset in its literal until Task 70 and Plan S3-E3 serve them), replace:

```go
	requireEveryField(t, want)
	requireEveryField(t, want.GetStatements()[0])
	requireEveryField(t, want.GetEvidence()[0])
```

with:

```go
	requireEveryFieldExcept(t, want)
	requireEveryField(t, want.GetStatements()[0])
	requireEveryField(t, want.GetEvidence()[0])
```

In `server/table_registry_test.go`, `TestTableRegistryMirrorsProto` (`:38-40`), replace:

```go
		for i := 0; i < p.fields.Len(); i++ {
			name := string(p.fields.Get(i).Name())
			if !goTags[name] {
```

with:

```go
		for i := 0; i < p.fields.Len(); i++ {
			fd := p.fields.Get(i)
			name := string(fd.Name())
			// A PROTO_TAG_P5 field its owning task has not mapped yet
			// (stage3PendingProtoFields).
			if stage3ProtoFieldPending(fd) {
				continue
			}
			if !goTags[name] {
```

In `server/safestate_l3block_test.go`, `TestL3BlockHeaderMirrorsProto` (`:72-74`), replace:

```go
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())
		if !goTags[name] {
```

with:

```go
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := string(fd.Name())
		// A PROTO_TAG_P5 field its owning task has not mapped yet
		// (stage3PendingProtoFields).
		if stage3ProtoFieldPending(fd) {
			continue
		}
		if !goTags[name] {
```

If a test that Plans S3-C2 or S3-D added failed in Step 6 only because of `L3BlockTrack.group_key` or a `TableRegistrySnapshot` field, add that one entry to `stage3PendingProtoFields` with the owner contract §44 names (`"L3BlockTrack.group_key": "Plan S3-E3 Task 72"`, `"TableRegistrySnapshot.stage3": "Task 65"`, `"TableRegistrySnapshot.groups": "Plan S3-E3 Task 71"`, `"TableRegistrySnapshot.member_checkpoint_seqs": "Plan S3-E3 Task 75"`), make that test skip `stage3ProtoFieldPending` fields the way `TestTableRegistryMirrorsProto` does above (or call `requireEveryFieldExcept` instead of `requireEveryField`), run `gofmt -w` on the file, and name the test in the commit message of Step 9. Any other failure stops this task.

- [ ] **Step 8: Run the suite again**

The first Bazel build after the pin fetches the new `git_override` sources and Go modules: remote read, needs explicit user confirmation in chat before running (Step 4's confirmation covers it when it was asked for both).

```bash
W=<worktree path>
cd "$W" && GOWORK=off go test ./server/ -run '^(TestTableRegistryMirrorsProto|TestL3BlockHeaderMirrorsProto|TestBlockVerificationToPBCarriesEveryField)$' -count=1 -v 2>&1 | grep -E '^(--- |ok|FAIL)'
cd "$W" && test -z "$(gofmt -l $(git ls-files '*.go'))" && echo gofmt-clean
cd "$W" && GOWORK=off go test ./... -count=1 2>&1 | grep -E '^(--- FAIL|FAIL)' || echo no-failures
cd "$W" && bazel build //... && bazel test //... --test_output=errors
```

Expected: `--- PASS: TestL3BlockHeaderMirrorsProto`, `--- PASS: TestBlockVerificationToPBCarriesEveryField`, `--- PASS: TestTableRegistryMirrorsProto`, `ok  	github.com/sentioxyz/arbiter/server`; `gofmt-clean`; `no-failures`; Bazel builds and every test target passes.

- [ ] **Step 9: Commit the pins**

```bash
W=<worktree path>; PROTO_TAG_P5=<ledger>; HG_TAG_P5=<ledger>; CORE_TAG_P5=<ledger>
git -C "$W" add go.mod go.sum MODULE.bazel MODULE.bazel.lock server/safestate_stage3_test.go server/table_registry_test.go server/safestate_l3block_test.go
git -C "$W" status --short
git -C "$W" commit -m "chore(deps): pin arbiter-proto, housegate and arbiter-core for stage 3 P5 (Task 64)

Pins arbiter-proto $PROTO_TAG_P5, housegate $HG_TAG_P5 and arbiter-core
$CORE_TAG_P5 (plan S3-E1): the stage3 parameter, the host-bound
verifiers entry fields, the group-member consent purpose, the wire
converters and stage3_v1 in LocalNodeFeatures. No consensus format
moves. The proto fields a later stage-3 task maps are named in the
server test map stage3PendingProtoFields, which
TestTableRegistryMirrorsProto, TestL3BlockHeaderMirrorsProto and
requireEveryFieldExcept consult (plan S3-E2 contract §44). No arbiter
release until Task 78 (K65).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: the status lists the seven staged files only (plus the file of any test Step 7's rule touched, staged with them and named in the message); one commit.

- [ ] **Step 10: Write the failing every-field clone test**

Create `fsm/stage3_params_test.go` (Step 16 completes it):

```go
package fsm

import (
	"bytes"
	"crypto/ed25519"
	"reflect"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestCloneVerifiersCopiesEveryField: cloneVerifiers, and through it
// cloneParams and normalizeConsensusParams, copy every field of
// arbiter.VerifierEntry and share no memory with their input (the scoping
// proposal's defect D3).
func TestCloneVerifiersCopiesEveryField(t *testing.T) {
	var entry arbiter.VerifierEntry
	rv := reflect.ValueOf(&entry).Elem()
	for i := 0; i < rv.NumField(); i++ {
		setNonZero(t, rv.Field(i), rv.Type().Field(i).Name)
	}
	for _, tc := range []struct {
		name  string
		clone func([]arbiter.VerifierEntry) []arbiter.VerifierEntry
	}{
		{"cloneVerifiers", cloneVerifiers},
		{"cloneParams", func(in []arbiter.VerifierEntry) []arbiter.VerifierEntry {
			return cloneParams(Params{Verifiers: in}).Verifiers
		}},
		{"normalizeConsensusParams", func(in []arbiter.VerifierEntry) []arbiter.VerifierEntry {
			return normalizeConsensusParams(Params{Verifiers: in}).Verifiers
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := []arbiter.VerifierEntry{entry}
			in[0].Ed25519Pubkey = bytes.Clone(entry.Ed25519Pubkey)
			host := *entry.HostIndexerID
			in[0].HostIndexerID = &host
			out := tc.clone(in)
			if len(out) != 1 || !reflect.DeepEqual(out[0], entry) {
				t.Fatalf("%s = %+v, want every field of %+v", tc.name, out, entry)
			}
			ov := reflect.ValueOf(out[0])
			for i := 0; i < ov.NumField(); i++ {
				if ov.Field(i).IsZero() {
					t.Fatalf("%s dropped %s", tc.name, ov.Type().Field(i).Name)
				}
			}
			in[0].Ed25519Pubkey[0] ^= 0xff
			*in[0].HostIndexerID = 99
			if out[0].Ed25519Pubkey[0] == in[0].Ed25519Pubkey[0] || *out[0].HostIndexerID == 99 {
				t.Fatalf("%s shares the key bytes or the host_indexer_id with its input", tc.name)
			}
		})
	}
	if cloneVerifiers(nil) != nil || cloneVerifiers([]arbiter.VerifierEntry{}) != nil {
		t.Fatal("an empty list must clone to nil")
	}
}

// setNonZero sets v, a field of arbiter.VerifierEntry, to a value of its kind
// that is not the zero value.
func setNonZero(t *testing.T, v reflect.Value, name string) {
	t.Helper()
	switch v.Kind() {
	case reflect.String:
		v.SetString("x-" + strings.ToLower(name))
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Uint32, reflect.Uint64:
		v.SetUint(7)
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			t.Fatalf("field %s has an unsupported slice type %s; extend setNonZero", name, v.Type())
		}
		v.SetBytes(bytes.Repeat([]byte{0x5a}, ed25519.PublicKeySize))
	case reflect.Pointer:
		if v.Type().Elem().Kind() != reflect.Uint64 {
			t.Fatalf("field %s has an unsupported pointer type %s; extend setNonZero", name, v.Type())
		}
		host := uint64(7)
		v.Set(reflect.ValueOf(&host))
	default:
		t.Fatalf("field %s has an unsupported kind %s; extend setNonZero", name, v.Kind())
	}
}
```

- [ ] **Step 11: Run it and see it fail**

Run: `W=<worktree path>; cd "$W" && GOWORK=off go test ./fsm/ -run '^TestCloneVerifiersCopiesEveryField$' -race -count=1`
Expected: FAIL in all three subtests (`cloneVerifiers`, `cloneParams`, `normalizeConsensusParams`), each with `… = [{NodeID:x-nodeid Ed25519Pubkey:[90 90 … 90] HostIndexerID:<nil> ReplicaSigner: ConsentJWS: Weight:0 FailureDomain: Verify:false Replicate:false Counts:false SharedRMT:false}], want every field of {NodeID:x-nodeid Ed25519Pubkey:[90 90 … 90] HostIndexerID:0xc… ReplicaSigner:x-replicasigner ConsentJWS:x-consentjws Weight:7 FailureDomain:x-failuredomain Verify:true Replicate:true Counts:true SharedRMT:true}`: `cloneVerifiers` rebuilds `{NodeID, Ed25519Pubkey}` (`fsm/signed_claims.go:99`), so `cloneParams`, `normalizeConsensusParams` and `verifyVerifiersTransition` drop every host-bound field and the leader would sign what Apply then loses (defect D3). Under a Go toolchain newer than 1.26 a dependency prints one `WARNING: sonic/ast only supports …` line first; it is harmless here and below.

- [ ] **Step 12: Copy every field in `cloneVerifiers`**

In `fsm/signed_claims.go`, replace (`:92-102`):

```go
// cloneVerifiers detaches a list including its key bytes; empty becomes nil.
func cloneVerifiers(in []arbiter.VerifierEntry) []arbiter.VerifierEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]arbiter.VerifierEntry, len(in))
	for i, v := range in {
		out[i] = arbiter.VerifierEntry{NodeID: v.NodeID, Ed25519Pubkey: bytes.Clone(v.Ed25519Pubkey)}
	}
	return out
}
```

with:

```go
// cloneVerifiers detaches a list, every field of every entry included (its
// key bytes and host_indexer_id too); empty becomes nil.
func cloneVerifiers(in []arbiter.VerifierEntry) []arbiter.VerifierEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]arbiter.VerifierEntry, len(in))
	for i, v := range in {
		out[i] = cloneVerifierEntry(v)
	}
	return out
}

// cloneVerifierEntry copies every field of v, the key bytes and the
// host_indexer_id pointer detached.
func cloneVerifierEntry(v arbiter.VerifierEntry) arbiter.VerifierEntry {
	v.Ed25519Pubkey = bytes.Clone(v.Ed25519Pubkey)
	if v.HostIndexerID != nil {
		host := *v.HostIndexerID
		v.HostIndexerID = &host
	}
	return v
}
```

- [ ] **Step 13: Run it and see it pass**

Run: `W=<worktree path>; cd "$W" && GOWORK=off go test ./fsm/ -run '^TestCloneVerifiersCopiesEveryField$' -race -count=1 -v 2>&1 | grep -E '^(--- |    --- |ok|FAIL)' && GOWORK=off go test ./fsm/ -count=1`
Expected: `--- PASS: TestCloneVerifiersCopiesEveryField` with its three subtests passing, `ok  	github.com/sentioxyz/arbiter/fsm`, then `ok  	github.com/sentioxyz/arbiter/fsm` for the whole package.

- [ ] **Step 14: Commit the clone fix**

```bash
W=<worktree path>
git -C "$W" add fsm/signed_claims.go fsm/stage3_params_test.go
git -C "$W" commit -m "feat(stage3): cloneVerifiers copies every verifiers entry field (Task 64)

cloneVerifiers rebuilt each entry as {NodeID, Ed25519Pubkey}, so
cloneParams, normalizeConsensusParams and verifyVerifiersTransition
would drop host_indexer_id, the replica signer, the consent and the
member flags of a stage-3 entry (scoping defect D3). It now copies
every field, the key bytes and the host_indexer_id detached;
TestCloneVerifiersCopiesEveryField seeds every field by reflection.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C "$W" status --short
```

Expected: one commit; `git status --short` prints nothing.

- [ ] **Step 15: Write the stage-3 fixture**

Create `fsm/stage3_fixture_test.go`:

```go
package fsm

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The stage-3 fixture (housegate spec 2026-10-14 §10.3, plan S3-E2 contract
// §45) is tracksFSM, the two-owner network after the tracks activation
// (indexer 1 on s1 signed by authoritytest key 0 owns db.t, indexer 0 on s0
// signed by key 1 owns db0.x, verifiers v1..v3 Active and listed, cutover
// {block 1, promotion 0}), plus the v21 activation of stage3ActivationParams.
// Plans S3-E2 to S3-E4 build on it.

// stage3ActivationParams is the stage3 parameter the fixture activates: every
// switch off, strict_copies false, min_safe_copies 1, and a failure domain
// for each of the two enrolled indexers.
func stage3ActivationParams() *arbiter.Stage3Params {
	return &arbiter.Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1,
		IndexerFailureDomains: []arbiter.IndexerFailureDomain{{IndexerID: 0, FailureDomain: "node:n0"}, {IndexerID: 1, FailureDomain: "node:n1"}},
		MaxLagPublications:    8, LagLow: 2, EvidenceTimeoutL2Blocks: 30, MaxUncleanedPartsPerPartition: 2000, MaxUncleanedPartsPerTable: 20000}
}

// stage3Update is f's next consensus update after the v21 activation:
// tracksUpdate (which carries the committed si_indexers and verifiers, every
// field of every entry) carrying the committed stage3 parameter.
func stage3Update(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
	t.Helper()
	u := tracksUpdate(t, f)
	u.Stage3 = cloneStage3Params(f.st.Params.Stage3)
	return u
}

// activateStage3 commits the v21 activation of stage3ActivationParams on f,
// which must have the tracks activation committed.
func activateStage3(t *testing.T, f *FSM, a *authority.Signer) {
	t.Helper()
	u := tracksUpdate(t, f)
	u.Stage3 = stage3ActivationParams()
	mustApply(t, f, signedConsensusUpdate(t, a, u))
	if !f.Stage3Active() || f.st.Stage3 == nil {
		t.Fatal("the v21 activation did not commit")
	}
}

// stage3FSM is tracksFSM after the v21 activation: stage3 committed with
// every switch off and the activation record {epoch 4, block 1, promotion 0};
// the registry holds incarnations 1-3 (db.t, db.old, db0.x).
func stage3FSM(t *testing.T) (*FSM, *authority.Signer) {
	t.Helper()
	f, a := tracksFSM(t)
	activateStage3(t, f, a)
	return f, a
}

// stage3WithSwitch is a copy of p with switch name on.
func stage3WithSwitch(p *arbiter.Stage3Params, name string) *arbiter.Stage3Params {
	c := cloneStage3Params(p)
	switch name {
	case Stage3SwitchPurge:
		c.Purge = true
	case Stage3SwitchEvictionVoiding:
		c.EvictionVoiding = true
	case Stage3SwitchOwnerAwareSelection:
		c.OwnerAwareSelection = true
	case Stage3SwitchPlacement:
		c.Placement = true
	case Stage3SwitchReselection:
		c.Reselection = true
	case Stage3SwitchCheckpoints:
		c.Checkpoints = true
	case Stage3SwitchNarrowedDrain:
		c.NarrowedDrain = true
	default:
		panic(fmt.Sprintf("unknown stage3 switch %q", name))
	}
	return c
}

// turnOnStage3Switch commits the update that turns on switch name and
// nothing else.
func turnOnStage3Switch(t *testing.T, f *FSM, a *authority.Signer, name string) {
	t.Helper()
	u := stage3Update(t, f)
	u.Stage3 = stage3WithSwitch(u.Stage3, name)
	mustApply(t, f, signedConsensusUpdate(t, a, u))
	if !f.Stage3SwitchOn(name) {
		t.Fatalf("stage3 switch %s did not turn on", name)
	}
}

// turnOnStage3SwitchesThrough turns on every switch up to and including last,
// one update each, in §10.4 order.
func turnOnStage3SwitchesThrough(t *testing.T, f *FSM, a *authority.Signer, last string) {
	t.Helper()
	for _, name := range Stage3Switches() {
		turnOnStage3Switch(t, f, a, name)
		if name == last {
			return
		}
	}
	t.Fatalf("unknown stage3 switch %q", last)
}

// stage3TestMember is the node id of the fixture member hosted by indexer
// host.
func stage3TestMember(host uint64) string { return fmt.Sprintf("member-%d", host) }

// stage3MemberKey is node's ed25519 key: its own for every node id, and the
// same on every call, so a test can register the member and sign as it.
func stage3MemberKey(node string) ed25519.PrivateKey {
	seed := sha256.Sum256([]byte("stage3 fixture member ed25519 key " + node))
	return ed25519.NewKeyFromSeed(seed[:])
}

// stage3ReplicaSigner is node's replica signer key, its own for every node id.
func stage3ReplicaSigner(t testing.TB, node string) *authority.Signer {
	t.Helper()
	key := sha256.Sum256([]byte("stage3 fixture member replica key " + node))
	return authoritytest.MustSigner(t, hex.EncodeToString(key[:]))
}

// memberConsent is the consent of indexer host's enrolment key (the signer of
// its SNode: s1 key 0, s0 key 1) to entry, signed at authoritytest.Iat over
// f's network and genesis.
func memberConsent(t *testing.T, f *FSM, signer *authority.Signer, entry arbiter.VerifierEntry) string {
	t.Helper()
	jws, err := signer.SignGroupMemberConsentAt(authority.GroupMemberConsentStatement{NetworkID: f.st.Params.NetworkID,
		GenesisSnapshotID: mustGenesisID(t, f), HostIndexerID: *entry.HostIndexerID, NodeID: entry.NodeID,
		Ed25519Pubkey: entry.Ed25519Pubkey, ReplicaSigner: entry.ReplicaSigner}, authoritytest.Iat)
	if err != nil {
		t.Fatal(err)
	}
	return jws
}

// hostSigner is the enrolment key of indexer host's SNode.
func hostSigner(t *testing.T, f *FSM, host uint64) *authority.Signer {
	t.Helper()
	e, ok := f.siIndexerLocked(host)
	if !ok {
		t.Fatalf("indexer %d is not enrolled", host)
	}
	return snodeSigner(t, e.SNodeNodeID)
}

// memberEntry is node's host-bound verifiers entry on indexer host: every
// flag true, weight 1, failure domain domain, node's own ed25519 key and
// replica signer, and the host's consent.
func memberEntry(t *testing.T, f *FSM, host uint64, node, domain string) arbiter.VerifierEntry {
	t.Helper()
	h := host
	entry := arbiter.VerifierEntry{NodeID: node, Ed25519Pubkey: stage3MemberKey(node).Public().(ed25519.PublicKey),
		HostIndexerID: &h, ReplicaSigner: strings.ToLower(stage3ReplicaSigner(t, node).Address()), Weight: 1, FailureDomain: domain,
		Verify: true, Replicate: true, Counts: true, SharedRMT: true}
	entry.ConsentJWS = memberConsent(t, f, hostSigner(t, f, host), entry)
	return entry
}

// withVerifier is u listing entry too, replacing an entry of the same node.
func withVerifier(u arbiter.ConsensusParamsUpdate, entry arbiter.VerifierEntry) arbiter.ConsensusParamsUpdate {
	var out []arbiter.VerifierEntry
	for _, v := range u.Verifiers {
		if v.NodeID != entry.NodeID {
			out = append(out, v)
		}
	}
	u.Verifiers = append(out, entry)
	return u
}

// stage3StateJSON is f's stage-3 record as JSON, for comparisons the snapshot
// document cannot make before snapshot v21 (Task 66).
func stage3StateJSON(t *testing.T, f *FSM) []byte {
	t.Helper()
	b, err := json.Marshal(f.st.Stage3)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// rejectStage3Unchanged is rejectConsensusUnchanged that also compares the
// stage-3 record and the track state, so a refused update cannot hide a
// change to either.
func rejectStage3Unchanged(t *testing.T, f *FSM, cmd wire.Command, reason string) {
	t.Helper()
	stage3, tracks := stage3StateJSON(t, f), trackStateJSON(t, f)
	rejectConsensusUnchanged(t, f, cmd, reason)
	if !bytes.Equal(stage3, stage3StateJSON(t, f)) || tracks != trackStateJSON(t, f) {
		t.Fatal("a refused update changed the stage-3 record or the track state")
	}
}
```

- [ ] **Step 16: Write the failing parameter and verifiers-entry tests**

In `fsm/stage3_params_test.go`, replace the import block written in Step 10:

```go
import (
	"bytes"
	"crypto/ed25519"
	"reflect"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)
```

with:

```go
import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
)
```

and append to the end of the file:

```go
// The Params-only stage3 rules of verifyConsensusTransition (plan S3-E2
// contract §45, K69) and the host-bound verifiers entry rules (K68). Where
// arbiter-core's normalizer (plan S3-E1 contract §41) refuses an update
// first, the FSM's own rule is exercised directly, as restore would meet it.

// stage3Previous returns f's committed params and a detached copy to edit:
// the previous and next inputs of verifyStage3Transition and
// verifyHostBoundVerifiers.
func stage3Previous(f *FSM) (Params, Params) {
	return cloneParams(f.st.Params), cloneParams(f.st.Params)
}

// requireNormalizerRefuses: arbiter-core's normalizer (plan S3-E1 contract
// §41) refuses u with want, so no authority can sign it and Apply never sees
// it; the FSM's own rule is then met directly, as restore would meet it.
func requireNormalizerRefuses(t *testing.T, u arbiter.ConsensusParamsUpdate, want string) {
	t.Helper()
	_, err := authority.NormalizeConsensusParamsUpdate(u)
	requireErrorContains(t, err, want)
}

// requireErrorContains fails unless err's text contains want.
func requireErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

// TestStage3MustBeCarriedOnceSet: every update after the activation carries
// stage3.
func TestStage3MustBeCarriedOnceSet(t *testing.T) {
	f, a := stage3FSM(t)
	b, _ := authorityFixture(t)
	omitted := stage3Update(t, f)
	omitted.Stage3 = nil
	// An authority change too, so only the stage3 rule can refuse it.
	omitted.AuthorityAddresses = []string{b.Address()}
	rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, omitted), "consensus update must carry stage3 once set")
	carried := stage3Update(t, f)
	carried.AuthorityAddresses = []string{b.Address()}
	mustApply(t, f, signedConsensusUpdate(t, a, carried))
	if !f.Stage3Active() || !f.AuthorityAllowed(b.Address()) || !reflect.DeepEqual(f.st.Params.Stage3, stage3ActivationParams()) {
		t.Fatalf("an update carrying stage3 must rotate the authority and keep stage3: %+v", f.st.Params.Stage3)
	}
}

// TestStage3SwitchesAndStrictCopiesAreOneWay: a switch and strict_copies are
// never turned off.
func TestStage3SwitchesAndStrictCopiesAreOneWay(t *testing.T) {
	f, a := stage3FSM(t)
	turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
	off := stage3Update(t, f)
	off.Stage3.Purge = false
	rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, off), "consensus update cannot turn stage3 switch purge off")
	for _, name := range Stage3Switches()[1:] {
		turnOnStage3Switch(t, f, a, name)
	}
	strict := stage3Update(t, f)
	strict.Stage3.StrictCopies = true
	mustApply(t, f, signedConsensusUpdate(t, a, strict))
	lowered := stage3Update(t, f)
	lowered.Stage3.StrictCopies = false
	rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, lowered), "consensus update cannot turn stage3 strict_copies off")
	// Turning narrowed_drain and strict_copies off together: the switch is
	// named first.
	last := stage3Update(t, f)
	last.Stage3.NarrowedDrain = false
	last.Stage3.StrictCopies = false
	rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, last), "consensus update cannot turn stage3 switch narrowed_drain off")
}

// TestStage3MinSafeCopiesAndGroupSizeNeverDecrease: both only rise.
func TestStage3MinSafeCopiesAndGroupSizeNeverDecrease(t *testing.T) {
	f, a := stage3FSM(t)
	raised := stage3Update(t, f)
	raised.Stage3.GroupSize = 4
	mustApply(t, f, signedConsensusUpdate(t, a, raised))
	lowered := stage3Update(t, f)
	lowered.Stage3.GroupSize = 3
	rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, lowered), "consensus update cannot lower stage3 group_size (4 -> 3)")
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

// TestStage3OneSwitchPerUpdate: each switch gets its own update, epoch and
// cutover.
func TestStage3OneSwitchPerUpdate(t *testing.T) {
	f, a := stage3FSM(t)
	two := stage3Update(t, f)
	two.Stage3 = stage3WithSwitch(stage3WithSwitch(two.Stage3, Stage3SwitchPurge), Stage3SwitchEvictionVoiding)
	rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, two), "consensus update may turn on at most one stage3 switch, got purge, eviction_voiding")
	turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
	if len(f.st.Stage3.Switches) != 1 || f.st.Stage3.Switches[0].Switch != Stage3SwitchPurge {
		t.Fatalf("cutovers = %+v, want purge alone", f.st.Stage3.Switches)
	}
}

// TestStage3SwitchNeedsEveryEarlierSwitchCommitted: a switch turns on only
// once every earlier switch of §10.4 is committed in the previous
// parameters; the names are arbiter-core's, in its order.
func TestStage3SwitchNeedsEveryEarlierSwitchCommitted(t *testing.T) {
	if !slices.Equal(Stage3Switches(), arbiter.Stage3Switches) {
		t.Fatalf("Stage3Switches() = %v, arbiter-core lists %v", Stage3Switches(), arbiter.Stage3Switches)
	}
	for _, name := range Stage3Switches() {
		p := stage3WithSwitch(stage3ActivationParams(), name)
		if !stage3SwitchValue(p, name) || !p.SwitchOn(name) || stage3SwitchValue(nil, name) {
			t.Fatalf("switch %s: stage3SwitchValue disagrees with arbiter-core", name)
		}
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("an unknown switch name must panic")
			}
		}()
		stage3SwitchValue(stage3ActivationParams(), "pruge")
	}()
	f, a := stage3FSM(t)
	skipped := stage3Update(t, f)
	skipped.Stage3.EvictionVoiding = true
	// arbiter-core's normalizer refuses the update first (plan S3-E1 §41).
	requireNormalizerRefuses(t, skipped, "stage3: switch eviction_voiding requires purge")
	previous, next := stage3Previous(f)
	_, err := verifyStage3Transition(previous, next, skipped.Stage3)
	requireErrorContains(t, err, "consensus update cannot turn on stage3 switch eviction_voiding before purge")
	epoch := f.st.ConsensusEpoch
	for i, name := range Stage3Switches() {
		turnOnStage3Switch(t, f, a, name)
		if f.st.ConsensusEpoch != epoch+uint64(i)+1 {
			t.Fatalf("switch %s: epoch %d, want %d", name, f.st.ConsensusEpoch, epoch+uint64(i)+1)
		}
	}
}

// TestStage3FloorAboveOneNeedsCheckpointsAndNarrowedDrain: min_safe_copies
// above 1 and strict_copies need both switches committed, not merely turned
// on by the same update (K69).
func TestStage3FloorAboveOneNeedsCheckpointsAndNarrowedDrain(t *testing.T) {
	const want = "consensus update cannot raise stage3 min_safe_copies above 1 or set strict_copies before the checkpoints and narrowed_drain switches"
	f, a := stage3FSM(t)
	turnOnStage3SwitchesThrough(t, f, a, Stage3SwitchCheckpoints)
	for _, tc := range []struct {
		name string
		edit func(p *arbiter.Stage3Params)
	}{
		{"min_safe_copies 2", func(p *arbiter.Stage3Params) { p.MinSafeCopies = 2 }},
		{"strict_copies", func(p *arbiter.Stage3Params) { p.StrictCopies = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := stage3Update(t, f)
			u.Stage3 = stage3WithSwitch(u.Stage3, Stage3SwitchNarrowedDrain)
			tc.edit(u.Stage3)
			rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, u), want)
		})
	}
	previous, next := stage3Previous(f)
	early := cloneStage3Params(previous.Stage3)
	early.MinSafeCopies = 2
	_, err := verifyStage3Transition(previous, next, early)
	requireErrorContains(t, err, want)
	turnOnStage3Switch(t, f, a, Stage3SwitchNarrowedDrain)
	raised := stage3Update(t, f)
	raised.Stage3.MinSafeCopies = 2
	raised.Stage3.StrictCopies = true
	mustApply(t, f, signedConsensusUpdate(t, a, raised))
}

// TestStage3PlacementNeedsADomainForEveryIndexer: from the placement switch
// on, every si_indexers entry has a failure domain.
func TestStage3PlacementNeedsADomainForEveryIndexer(t *testing.T) {
	const want = "consensus update stage3 indexer_failure_domains has no domain for indexer 1"
	f, a := tracksFSM(t)
	u := tracksUpdate(t, f)
	u.Stage3 = stage3ActivationParams()
	u.Stage3.IndexerFailureDomains = u.Stage3.IndexerFailureDomains[:1]
	// Before placement a missing domain is accepted.
	mustApply(t, f, signedConsensusUpdate(t, a, u))
	turnOnStage3SwitchesThrough(t, f, a, Stage3SwitchOwnerAwareSelection)
	placement := stage3Update(t, f)
	placement.Stage3 = stage3WithSwitch(placement.Stage3, Stage3SwitchPlacement)
	// arbiter-core's normalizer refuses the update first.
	requireNormalizerRefuses(t, placement,
		"stage3: the placement switch needs a failure domain for every si_indexers entry; indexer 1 has none")
	previous, next := stage3Previous(f)
	_, err := verifyStage3Transition(previous, next, placement.Stage3)
	requireErrorContains(t, err, want)
	// An indexer enrolled after placement needs one too.
	placed := cloneStage3Params(placement.Stage3)
	placed.IndexerFailureDomains = stage3ActivationParams().IndexerFailureDomains
	previous.Stage3 = placed
	next.Stage3 = placed
	next.SIIndexers = append(next.SIIndexers, arbiter.SIIndexerEntry{IndexerID: 7, ActivationBlock: 900, SNodeNodeID: "s7"})
	_, err = verifyStage3Transition(previous, next, placed)
	requireErrorContains(t, err, "consensus update stage3 indexer_failure_domains has no domain for indexer 7")
	placement.Stage3.IndexerFailureDomains = stage3ActivationParams().IndexerFailureDomains
	mustApply(t, f, signedConsensusUpdate(t, a, placement))
}

// TestStage3FailureDomainsNameOnlyEnrolledIndexers: the map never names an
// indexer that is not enrolled.
func TestStage3FailureDomainsNameOnlyEnrolledIndexers(t *testing.T) {
	f, a := stage3FSM(t)
	u := stage3Update(t, f)
	u.Stage3.IndexerFailureDomains = append(u.Stage3.IndexerFailureDomains, arbiter.IndexerFailureDomain{IndexerID: 5, FailureDomain: "node:n5"})
	requireNormalizerRefuses(t, u, "stage3: indexer_failure_domains names indexer 5, which has no si_indexers entry")
	previous, next := stage3Previous(f)
	_, err := verifyStage3Transition(previous, next, u.Stage3)
	requireErrorContains(t, err, "consensus update stage3 indexer_failure_domains names indexer 5, which is not an si_indexers entry")
	// A map that names enrolled indexers only may change.
	moved := stage3Update(t, f)
	moved.Stage3.IndexerFailureDomains[0].FailureDomain = "node:n0b"
	mustApply(t, f, signedConsensusUpdate(t, a, moved))
	if got := stage3FailureDomain(f.st.Params.Stage3, 0); got != "node:n0b" {
		t.Fatalf("indexer 0's domain = %q, want node:n0b", got)
	}
}

// TestStage3RulesReplayOnRestore: restore replays the Params-only rules
// (validateConsensusHistory runs verifyConsensusTransition per change) and
// refuses every history Apply refuses, with the same text.
func TestStage3RulesReplayOnRestore(t *testing.T) {
	t.Run("a history with every switch replays", func(t *testing.T) {
		f, a := stage3FSM(t)
		turnOnStage3SwitchesThrough(t, f, a, Stage3SwitchNarrowedDrain)
		raised := stage3Update(t, f)
		raised.Stage3.GroupSize, raised.Stage3.MinSafeCopies = 5, 2
		mustApply(t, f, signedConsensusUpdate(t, a, raised))
		if err := validateConsensusHistory(f.st, nil, f.st.BootstrapParams); err != nil {
			t.Fatalf("replaying the stage-3 history = %v", err)
		}
		g := restoreInto(t, snapshotBytes(t, f))
		if !reflect.DeepEqual(g.st.Params.Stage3, f.st.Params.Stage3) {
			t.Fatalf("restored stage3 = %+v, want %+v", g.st.Params.Stage3, f.st.Params.Stage3)
		}
	})
	for _, tc := range []struct {
		name   string
		setup  func(t *testing.T, f *FSM, a *authority.Signer)
		update func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate
		want   string
	}{
		{"an activation with a switch on", nil, func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			u := stage3Update(t, f)
			u.Stage3 = stage3WithSwitch(stage3ActivationParams(), Stage3SwitchPurge)
			return u
		}, "consensus update setting stage3 must leave every switch off, strict_copies false and min_safe_copies 1"},
		{"an update that drops stage3", activateStage3, func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			u := stage3Update(t, f)
			u.Stage3 = nil
			u.MaxWriters = 3
			return u
		}, "consensus update must carry stage3 once set"},
		{"a switch turned off", func(t *testing.T, f *FSM, a *authority.Signer) {
			activateStage3(t, f, a)
			turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
		}, func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			u := stage3Update(t, f)
			u.Stage3.Purge = false
			return u
		}, "consensus update cannot turn stage3 switch purge off"},
		{"two switches in one update", activateStage3, func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			u := stage3Update(t, f)
			u.Stage3 = stage3WithSwitch(stage3WithSwitch(u.Stage3, Stage3SwitchPurge), Stage3SwitchEvictionVoiding)
			return u
		}, "consensus update may turn on at most one stage3 switch, got purge, eviction_voiding"},
		{"group_size lowered", activateStage3, func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			u := stage3Update(t, f)
			u.Stage3.GroupSize = 2
			return u
		}, "consensus update cannot lower stage3 group_size (3 -> 2)"},
		{"a host-bound entry before owner_aware_selection", activateStage3, func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			return withVerifier(stage3Update(t, f), memberEntry(t, f, 1, stage3TestMember(1), "node:n1"))
		}, "consensus update cannot list host-bound verifier member-1 before the stage3 owner_aware_selection switch is committed"},
		{"the floor raised by the update that turns narrowed_drain on", func(t *testing.T, f *FSM, a *authority.Signer) {
			activateStage3(t, f, a)
			turnOnStage3SwitchesThrough(t, f, a, Stage3SwitchCheckpoints)
		}, func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			u := stage3Update(t, f)
			u.Stage3 = stage3WithSwitch(u.Stage3, Stage3SwitchNarrowedDrain)
			u.Stage3.MinSafeCopies = 2
			return u
		}, "consensus update cannot raise stage3 min_safe_copies above 1 or set strict_copies before the checkpoints and narrowed_drain switches"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, a := tracksFSM(t)
			if tc.setup != nil {
				tc.setup(t, f, a)
			}
			cmd := signedConsensusUpdate(t, a, tc.update(t, f))
			rejectStage3Unchanged(t, f, cmd, tc.want)
			forgeConsensusChange(t, f, cmd)
			err := validateConsensusHistory(f.st, nil, f.st.BootstrapParams)
			requireErrorContains(t, err, tc.want)
		})
	}
}

// TestHostBoundVerifierRefusedBeforeOwnerAwareSelection: no host-bound entry
// exists before switch 3 is committed (K68).
func TestHostBoundVerifierRefusedBeforeOwnerAwareSelection(t *testing.T) {
	t.Run("before the activation", func(t *testing.T) {
		f, _ := tracksFSM(t)
		u := withVerifier(tracksUpdate(t, f), memberEntry(t, f, 1, stage3TestMember(1), "node:n1"))
		requireNormalizerRefuses(t, u, "verifiers: member-1: an entry with host_indexer_id needs the stage3 parameter in the same update")
	})
	f, a := stage3FSM(t)
	for _, name := range []string{"", Stage3SwitchPurge, Stage3SwitchEvictionVoiding} {
		if name != "" {
			turnOnStage3Switch(t, f, a, name)
		}
		u := withVerifier(stage3Update(t, f), memberEntry(t, f, 1, stage3TestMember(1), "node:n1"))
		if name == Stage3SwitchEvictionVoiding {
			// Together with the switch's own turn-on: still not committed.
			u.Stage3 = stage3WithSwitch(u.Stage3, Stage3SwitchOwnerAwareSelection)
		}
		rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, u),
			"consensus update cannot list host-bound verifier member-1 before the stage3 owner_aware_selection switch is committed")
	}
	turnOnStage3Switch(t, f, a, Stage3SwitchOwnerAwareSelection)
	// Listed under the key ed25519KeyFor('m') so that it can register: a
	// listed, registered member is still outside the network pool.
	member := memberEntry(t, f, 1, stage3TestMember(1), "node:n1")
	member.Ed25519Pubkey = ed25519KeyFor('m').Public().(ed25519.PublicKey)
	member.ConsentJWS = memberConsent(t, f, hostSigner(t, f, 1), member)
	mustApply(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), member)))
	mustApply(t, f, verifierRegistration(t, f, stage3TestMember(1), 'm', 1))
	if v, ok := f.verifierEntryLocked(stage3TestMember(1)); !ok || v.HostIndexerID == nil || *v.HostIndexerID != 1 ||
		f.st.Nodes[stage3TestMember(1)] == nil || f.poolVerifierLocked(stage3TestMember(1)) {
		t.Fatalf("member-1 = %+v, %v; want a listed, registered host-bound entry outside the pool", v, ok)
	}
	if !f.poolVerifierLocked("v1") || f.poolVerifierLocked("s1") {
		t.Fatal("v1 is a pool verifier and s1 is not")
	}
	// An evicted node leaves the pool, even while its entry stays listed.
	f.st.Nodes["v1"].Status = NodeEvicted
	if f.poolVerifierLocked("v1") {
		t.Fatal("an evicted verifier is not a pool verifier")
	}
}

// ownerAwareFSM is stage3FSM with switches 1-3 committed: host-bound entries
// may be listed.
func ownerAwareFSM(t *testing.T) (*FSM, *authority.Signer) {
	t.Helper()
	f, a := stage3FSM(t)
	turnOnStage3SwitchesThrough(t, f, a, Stage3SwitchOwnerAwareSelection)
	return f, a
}

// TestHostBoundVerifierRules: every Params-only rule of a host-bound entry,
// met directly where arbiter-core's normalizer refuses the update first.
func TestHostBoundVerifierRules(t *testing.T) {
	f, a := ownerAwareFSM(t)
	genesisID := mustGenesisID(t, f)
	node := stage3TestMember(1)
	check := func(t *testing.T, edit func(v *arbiter.VerifierEntry), want string) {
		t.Helper()
		previous, next := stage3Previous(f)
		v := memberEntry(t, f, 1, node, "node:n1")
		edit(&v)
		next.Verifiers = append(next.Verifiers, v)
		err := verifyHostBoundVerifiers(previous, next, genesisID)
		if want == "" {
			if err != nil {
				t.Fatalf("a valid entry: %v", err)
			}
			return
		}
		requireErrorContains(t, err, want)
	}
	check(t, func(*arbiter.VerifierEntry) {}, "")
	for _, tc := range []struct {
		name string
		edit func(v *arbiter.VerifierEntry)
		want string
	}{
		{"an unenrolled host", func(v *arbiter.VerifierEntry) { host := uint64(7); v.HostIndexerID = &host },
			"consensus update verifier member-1 host_indexer_id 7 is not an si_indexers entry"},
		{"an uppercase replica signer", func(v *arbiter.VerifierEntry) { v.ReplicaSigner = strings.ToUpper(v.ReplicaSigner) },
			"consensus update verifier member-1 replica_signer must be a non-zero lowercase 0x-prefixed 20-byte address"},
		{"the zero replica signer", func(v *arbiter.VerifierEntry) { v.ReplicaSigner = "0x0000000000000000000000000000000000000000" },
			"consensus update verifier member-1 replica_signer must be a non-zero lowercase 0x-prefixed 20-byte address"},
		{"weight 0", func(v *arbiter.VerifierEntry) { v.Weight = 0 }, "consensus update verifier member-1 weight must be 1 to 64, got 0"},
		{"weight 65", func(v *arbiter.VerifierEntry) { v.Weight = 65 }, "consensus update verifier member-1 weight must be 1 to 64, got 65"},
		{"another domain", func(v *arbiter.VerifierEntry) { v.FailureDomain = "node:n0" },
			`consensus update verifier member-1 failure_domain "node:n0" differs from stage3 indexer_failure_domains[1] "node:n1"`},
		{"verify without shared_rmt", func(v *arbiter.VerifierEntry) { v.SharedRMT = false },
			"consensus update verifier member-1 sets verify with shared_rmt false; such a member cannot verify before the per-operator transport (spec §6.1)"},
		{"a consent by another signer", func(v *arbiter.VerifierEntry) { v.ConsentJWS = memberConsent(t, f, hostSigner(t, f, 0), *v) },
			"consensus update verifier member-1 consent: "},
		{"a consent over another key", func(v *arbiter.VerifierEntry) {
			v.Ed25519Pubkey = stage3MemberKey("member-x").Public().(ed25519.PublicKey)
		},
			"consensus update verifier member-1 consent: "},
	} {
		t.Run(tc.name, func(t *testing.T) { check(t, tc.edit, tc.want) })
	}
	t.Run("a host without a domain", func(t *testing.T) {
		previous, next := stage3Previous(f)
		next.Stage3.IndexerFailureDomains = next.Stage3.IndexerFailureDomains[:1]
		v := memberEntry(t, f, 1, node, "node:n1")
		next.Verifiers = append(next.Verifiers, v)
		requireErrorContains(t, verifyHostBoundVerifiers(previous, next, genesisID),
			`consensus update verifier member-1 failure_domain "node:n1" differs from stage3 indexer_failure_domains[1] ""`)
	})
	// Through Apply the consent is the FSM's alone (plan S3-E1 §41).
	foreign := memberEntry(t, f, 1, node, "node:n1")
	foreign.ConsentJWS = memberConsent(t, f, hostSigner(t, f, 0), foreign)
	rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), foreign)), "consensus update verifier member-1 consent: ")
	mustApply(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), memberEntry(t, f, 1, node, "node:n1"))))
	mustApply(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), memberEntry(t, f, 0, stage3TestMember(0), "node:n0"))))
	if n := len(f.st.Params.Verifiers); n != 5 {
		t.Fatalf("verifiers = %d entries, want v1..v3, member-0 and member-1", n)
	}
}

// TestHostBoundVerifierConsentIsVerifiedOnlyOnAddOrKeyRotation: the host's
// consent is checked when the entry is added and when its ed25519 key or
// replica signer rotates; an unchanged entry keeps its consent, even once
// the host's signer has rotated, and cannot swap it.
func TestHostBoundVerifierConsentIsVerifiedOnlyOnAddOrKeyRotation(t *testing.T) {
	f, a := ownerAwareFSM(t)
	node := stage3TestMember(1)
	mustApply(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), memberEntry(t, f, 1, node, "node:n1"))))
	listed, _ := f.verifierEntryLocked(node)

	resigned := listed
	jws, err := hostSigner(t, f, 1).SignGroupMemberConsentAt(consentStatement(t, f, listed), authoritytest.Iat+1)
	if err != nil {
		t.Fatal(err)
	}
	resigned.ConsentJWS = jws
	rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), resigned)),
		"consensus update changes verifier member-1 consent_jws without rotating its keys")

	// Indexer 1 rotates its signer; member-1's consent by the old one stays.
	rotatedSigner := authoritytest.MustSigner(t, "0000000000000000000000000000000000000000000000000000000000000c33")
	rotation := stage3Update(t, f)
	for i, e := range rotation.SIIndexers {
		if e.IndexerID == 1 {
			rotation.SIIndexers[i] = siEntry(t, f, rotatedSigner, 1, e.ActivationBlock, e.SNodeNodeID)
		}
	}
	mustApply(t, f, signedConsensusUpdate(t, a, rotation))
	if v, _ := f.verifierEntryLocked(node); v.ConsentJWS != listed.ConsentJWS {
		t.Fatal("the host's rotation changed member-1's consent")
	}

	for _, tc := range []struct {
		name   string
		rotate func(v *arbiter.VerifierEntry)
	}{
		{"the ed25519 key", func(v *arbiter.VerifierEntry) {
			v.Ed25519Pubkey = stage3MemberKey(node + "-rotated").Public().(ed25519.PublicKey)
		}},
		{"the replica signer", func(v *arbiter.VerifierEntry) {
			v.ReplicaSigner = strings.ToLower(stage3ReplicaSigner(t, node+"-rotated").Address())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := f.verifierEntryLocked(node)
			tc.rotate(&v)
			rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), v)), "consensus update verifier member-1 consent: ")
			v.ConsentJWS = memberConsent(t, f, rotatedSigner, v)
			mustApply(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), v)))
		})
	}
}

// consentStatement is the consent statement of entry v on f.
func consentStatement(t *testing.T, f *FSM, v arbiter.VerifierEntry) authority.GroupMemberConsentStatement {
	t.Helper()
	return authority.GroupMemberConsentStatement{NetworkID: f.st.Params.NetworkID, GenesisSnapshotID: mustGenesisID(t, f), HostIndexerID: *v.HostIndexerID,
		NodeID: v.NodeID, Ed25519Pubkey: v.Ed25519Pubkey, ReplicaSigner: v.ReplicaSigner}
}

// TestVerifierHostIndexerIDNeverChanges: an entry's host_indexer_id is fixed
// for its life, nil and set included, so a pool verifier never becomes a
// member in place and a member never moves host.
func TestVerifierHostIndexerIDNeverChanges(t *testing.T) {
	f, a := ownerAwareFSM(t)
	node := stage3TestMember(1)
	mustApply(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), memberEntry(t, f, 1, node, "node:n1"))))
	pool := memberEntry(t, f, 1, "v1", "node:n1")
	moved := memberEntry(t, f, 0, node, "node:n0")
	delisted := arbiter.VerifierEntry{NodeID: node, Ed25519Pubkey: stage3MemberKey(node).Public().(ed25519.PublicKey)}
	for _, tc := range []struct {
		name  string
		entry arbiter.VerifierEntry
		want  string
	}{
		{"a pool verifier made a member", pool, "consensus update cannot change verifier v1 host_indexer_id"},
		{"a member moved to another host", moved, "consensus update cannot change verifier member-1 host_indexer_id"},
		{"a member made a pool verifier", delisted, "consensus update cannot change verifier member-1 host_indexer_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), tc.entry)), tc.want)
		})
	}
}

// TestPoolVerifierCarriesNoHostBoundFields: a pool entry sets no host-bound
// field, so it keeps its v20 bytes and the params digest of every epoch
// before the activation is the stage-2 digest (plan S3-E1 K58).
func TestPoolVerifierCarriesNoHostBoundFields(t *testing.T) {
	// stageTwoParams is fsm.Params before stage 3, field for field in
	// declaration order, with the v20 verifiers entry.
	type v20Verifier struct {
		NodeID        string `json:"node_id"`
		Ed25519Pubkey []byte `json:"ed25519_pubkey"`
	}
	type stageTwoParams struct {
		SchemaSnapshotID              string                       `json:"schema_snapshot_id"`
		ExecutorProfileID             string                       `json:"executor_profile_id"`
		AuthorityAddresses            []string                     `json:"authority_addresses,omitempty"`
		NetworkID                     string                       `json:"network_id"`
		MaxWriters                    uint64                       `json:"max_writers,omitempty"`
		ArtifactDispositionCapability uint32                       `json:"artifact_disposition_capability,omitempty"`
		TableRegistry                 *arbiter.TableRegistryParams `json:"table_registry,omitempty"`
		ClientLanes                   *arbiter.ClientLaneParams    `json:"client_lanes,omitempty"`
		SIIndexers                    []arbiter.SIIndexerEntry     `json:"si_indexers,omitempty"`
		Verifiers                     []v20Verifier                `json:"verifiers,omitempty"`
		TracksEnabled                 bool                         `json:"tracks_enabled,omitempty"`
	}
	f, a := tracksFSM(t)
	view, err := f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	p := view.Current
	shape := stageTwoParams{SchemaSnapshotID: p.SchemaSnapshotID, ExecutorProfileID: p.ExecutorProfileID, AuthorityAddresses: p.AuthorityAddresses,
		NetworkID: p.NetworkID, MaxWriters: p.MaxWriters, ArtifactDispositionCapability: p.ArtifactDispositionCapability,
		TableRegistry: p.TableRegistry, ClientLanes: p.ClientLanes, SIIndexers: p.SIIndexers, TracksEnabled: p.TracksEnabled}
	for _, v := range p.Verifiers {
		shape.Verifiers = append(shape.Verifiers, v20Verifier{NodeID: v.NodeID, Ed25519Pubkey: v.Ed25519Pubkey})
	}
	want, err := replay.CanonicalDigest("arbiter/consensus-params/v1", shape)
	if err != nil {
		t.Fatal(err)
	}
	if view.ParamsDigest != want {
		t.Fatalf("pre-activation params digest %s, want the stage-2 digest %s", view.ParamsDigest, want)
	}
	activateStage3(t, f, a)
	if got := f.st.ConsensusUpdates[len(f.st.ConsensusUpdates)-1].Update.PreviousParamsDigest; got != want {
		t.Fatalf("the activation bound previous digest %s, want the stage-2 digest %s", got, want)
	}
	for _, v := range f.st.Params.Verifiers {
		got, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		v20, _ := json.Marshal(v20Verifier{NodeID: v.NodeID, Ed25519Pubkey: v.Ed25519Pubkey})
		if !bytes.Equal(got, v20) {
			t.Fatalf("pool entry %s = %s, want its v20 bytes %s", v.NodeID, got, v20)
		}
	}
	// Every host-bound field, one at a time, on pool entry v1. The
	// normalizer refuses such an update first (plan S3-E1 §41); restore meets
	// the FSM's rule.
	rt := reflect.TypeOf(arbiter.VerifierEntry{})
	genesisID := mustGenesisID(t, f)
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if field.Name == "NodeID" || field.Name == "Ed25519Pubkey" || field.Name == "HostIndexerID" {
			continue
		}
		t.Run(field.Name, func(t *testing.T) {
			previous, next := stage3Previous(f)
			setNonZero(t, reflect.ValueOf(&next.Verifiers[0]).Elem().Field(i), field.Name)
			requireErrorContains(t, verifyHostBoundVerifiers(previous, next, genesisID),
				"consensus update verifier v1 sets host-bound fields without host_indexer_id")
		})
	}
	weighted := stage3Update(t, f)
	weighted.Verifiers[0].Weight = 1
	requireNormalizerRefuses(t, weighted,
		"verifiers: v1: an entry without host_indexer_id carries no replica_signer, consent_jws, weight, failure_domain or member flags")
}

// TestMemberNodeIDMustBeUnusedByEveryRegisteredNode: an added host-bound
// entry names no node any Nodes record holds, whatever its status (the
// Apply-only half; restore never checks it).
func TestMemberNodeIDMustBeUnusedByEveryRegisteredNode(t *testing.T) {
	f, a := ownerAwareFSM(t)
	// Delist v3 (an unregistered v4 keeps three listed): v3 is evicted and
	// its record stays.
	removal := stage3Update(t, f)
	var kept []arbiter.VerifierEntry
	for _, v := range removal.Verifiers {
		if v.NodeID != "v3" {
			kept = append(kept, v)
		}
	}
	removal.Verifiers = append(kept, arbiter.VerifierEntry{NodeID: "v4", Ed25519Pubkey: testPubkey('4')})
	mustApply(t, f, signedConsensusUpdate(t, a, removal))
	if n := f.st.Nodes["v3"]; n == nil || n.Status != NodeEvicted {
		t.Fatalf("v3 = %+v, want evicted", n)
	}
	rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), memberEntry(t, f, 1, "v3", "node:n1"))),
		"consensus update verifier v3: a member node_id must be unused by every registered SNode and verifier")
	// An SNode's id is refused by arbiter-core's normalizer first; the FSM's
	// own rule names it too.
	next := cloneParams(f.st.Params)
	next.Verifiers = append(next.Verifiers, memberEntry(t, f, 0, "s0", "node:n0"))
	if got := f.stage3MembershipRefusalLocked(next); got != "consensus update verifier s0: a member node_id must be unused by every registered SNode and verifier" {
		t.Fatalf("refusal = %q", got)
	}
	mustApply(t, f, signedConsensusUpdate(t, a, withVerifier(stage3Update(t, f), memberEntry(t, f, 1, stage3TestMember(1), "node:n1"))))
}

// TestCloneParamsCopiesStage3: cloneParams and normalizeConsensusParams copy
// stage3 deeply; nil stays nil and an empty domain list becomes nil.
func TestCloneParamsCopiesStage3(t *testing.T) {
	src := stage3WithSwitch(stage3ActivationParams(), Stage3SwitchPurge)
	src.StrictCopies, src.ExcludeQuarantinedHosts = true, true
	for _, clone := range []func(Params) Params{cloneParams, normalizeConsensusParams} {
		in := Params{Stage3: cloneStage3Params(src)}
		out := clone(in)
		if !reflect.DeepEqual(out.Stage3, src) {
			t.Fatalf("clone = %+v, want %+v", out.Stage3, src)
		}
		in.Stage3.IndexerFailureDomains[0].FailureDomain = "node:other"
		in.Stage3.GroupSize = 9
		if out.Stage3.IndexerFailureDomains[0].FailureDomain != "node:n0" || out.Stage3.GroupSize != 3 {
			t.Fatal("the clone shares stage3 with its input")
		}
		if clone(Params{}).Stage3 != nil {
			t.Fatal("a nil stage3 must stay nil")
		}
		empty := clone(Params{Stage3: &arbiter.Stage3Params{IndexerFailureDomains: []arbiter.IndexerFailureDomain{}}})
		if empty.Stage3.IndexerFailureDomains != nil {
			t.Fatal("an empty domain list must become nil")
		}
	}
	if !consensusParamsEqual(Params{Stage3: &arbiter.Stage3Params{}}, Params{Stage3: &arbiter.Stage3Params{IndexerFailureDomains: []arbiter.IndexerFailureDomain{}}}) {
		t.Fatal("an empty and an absent domain list must compare equal")
	}
}
```

- [ ] **Step 17: Write the failing activation tests**

Create `fsm/stage3_activation_test.go`:

```go
package fsm

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The v21 activation and the switch cutovers (housegate spec 2026-10-14
// §10.3-§10.4, plan S3-E2 contract §44-§45).

// stage3Activation is f's activation update: tracksUpdate with stage3, edited
// by edit when it is not nil.
func stage3Activation(t *testing.T, f *FSM, edit func(p *arbiter.Stage3Params)) arbiter.ConsensusParamsUpdate {
	t.Helper()
	u := tracksUpdate(t, f)
	u.Stage3 = stage3ActivationParams()
	if edit != nil {
		edit(u.Stage3)
	}
	return u
}

// TestStage3ActivationSetsEveryParameterWithEverySwitchOff: the activation
// commits stage3 exactly as signed, every switch off, and records its epoch,
// sealed tip and promotion sequence.
func TestStage3ActivationSetsEveryParameterWithEverySwitchOff(t *testing.T) {
	f, a := tracksFSM(t)
	if f.Stage3Active() || f.st.Stage3 != nil || f.Stage3ActivationRefusal() != "" {
		t.Fatalf("before the activation: active %v, state %+v, refusal %q", f.Stage3Active(), f.st.Stage3, f.Stage3ActivationRefusal())
	}
	blocks, promotions, epoch := uint64(len(f.st.Blocks)), f.st.PromotionSeq, f.st.ConsensusEpoch
	activateStage3(t, f, a)
	if !reflect.DeepEqual(f.st.Params.Stage3, stage3ActivationParams()) {
		t.Fatalf("stage3 = %+v, want %+v", f.st.Params.Stage3, stage3ActivationParams())
	}
	want := &Stage3State{ActivationEpoch: epoch + 1, ActivationBlockSeq: blocks, ActivationPromotionSeq: promotions}
	if !reflect.DeepEqual(f.st.Stage3, want) {
		t.Fatalf("stage3 state = %+v, want %+v", f.st.Stage3, want)
	}
	if f.st.ConsensusEpoch != want.ActivationEpoch || f.st.ConsensusUpdates[want.ActivationEpoch-1].Update.Stage3 == nil {
		t.Fatalf("epoch %d: the activation must be the update that produced its epoch", f.st.ConsensusEpoch)
	}
	for _, name := range Stage3Switches() {
		if f.Stage3SwitchOn(name) {
			t.Fatalf("switch %s is on after the activation", name)
		}
	}
	if !f.st.Params.TracksEnabled || len(f.st.Params.Verifiers) != 3 {
		t.Fatalf("the activation must carry tracks and the verifiers: %+v", f.st.Params)
	}
}

// TestStage3ActivationRequiresTheTracksActivation: stage3 is first set only
// after tracks_enabled is committed, never together with it.
func TestStage3ActivationRequiresTheTracksActivation(t *testing.T) {
	f, a := tracksReadyFSM(t)
	if got := f.Stage3ActivationRefusal(); got != "stage3 activation requires the tracks activation" {
		t.Fatalf("refusal = %q", got)
	}
	together := stage3Activation(t, f, nil)
	rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, together), "consensus update cannot set stage3 before the tracks activation")
	alone := stage3Activation(t, f, nil)
	alone.TracksEnabled = false
	requireNormalizerRefuses(t, alone, "stage3 requires tracks_enabled in the same update")
	if f.TracksActive() || f.Stage3Active() {
		t.Fatal("a refused update activated tracks or stage3")
	}
}

// TestStage3ActivationRefusesASwitchStrictCopiesOrAFloorAboveOne: the
// activation leaves every switch off, strict_copies false and
// min_safe_copies 1, even where arbiter-core's static rules would allow more.
func TestStage3ActivationRefusesASwitchStrictCopiesOrAFloorAboveOne(t *testing.T) {
	const want = "consensus update setting stage3 must leave every switch off, strict_copies false and min_safe_copies 1"
	every := func(p *arbiter.Stage3Params) {
		for _, name := range Stage3Switches() {
			*p = *stage3WithSwitch(p, name)
		}
	}
	for _, tc := range []struct {
		name string
		edit func(p *arbiter.Stage3Params)
	}{
		{"the purge switch", func(p *arbiter.Stage3Params) { p.Purge = true }},
		{"every switch", every},
		{"strict_copies", func(p *arbiter.Stage3Params) { every(p); p.StrictCopies = true }},
		{"min_safe_copies 2", func(p *arbiter.Stage3Params) { every(p); p.MinSafeCopies = 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, a := tracksFSM(t)
			rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, stage3Activation(t, f, tc.edit)), want)
			if f.Stage3Active() {
				t.Fatal("a refused activation committed stage3")
			}
		})
	}
	// strict_copies or a floor above 1 with every switch off: arbiter-core's
	// static rule refuses the update first (plan S3-E1 §41), and restore
	// meets the activation rule, not the later floor rule.
	for _, tc := range []struct {
		name string
		edit func(p *arbiter.Stage3Params)
	}{
		{"strict_copies alone", func(p *arbiter.Stage3Params) { p.StrictCopies = true }},
		{"min_safe_copies 2 alone", func(p *arbiter.Stage3Params) { p.MinSafeCopies = 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := tracksFSM(t)
			u := stage3Activation(t, f, tc.edit)
			requireNormalizerRefuses(t, u, "stage3: min_safe_copies above 1 and strict_copies require the checkpoints and narrowed_drain switches")
			previous, next := stage3Previous(f)
			_, err := verifyStage3Transition(previous, next, u.Stage3)
			requireErrorContains(t, err, want)
		})
	}
}

// TestStage3ActivationNeedsNoEmptyOpenBlockOrGlobalPrefix: unlike the tracks
// activation, an open block with statements and an unsafe sealed block do not
// refuse the activation.
func TestStage3ActivationNeedsNoEmptyOpenBlockOrGlobalPrefix(t *testing.T) {
	f, a := tracksFSM(t)
	key, account := testAccount(t)
	admitTrack(t, f, key, account, 1, 1)
	sealed := mustSeal(t, f).BlockSeq
	admitTrack(t, f, key, account, 2, 1)
	if len(f.st.OpenBlock.StatementSeqs) == 0 || safePrefixLocked(f) >= sealed {
		t.Fatalf("want a non-empty open block and an unsafe block %d: prefix %d", sealed, safePrefixLocked(f))
	}
	if f.tracksActivationRefusalLocked() == "" {
		t.Fatal("the tracks activation's rules would refuse this state")
	}
	if got := f.Stage3ActivationRefusal(); got != "" {
		t.Fatalf("stage3 activation refusal = %q, want none", got)
	}
	activateStage3(t, f, a)
	if f.st.Stage3.ActivationBlockSeq != sealed || len(f.st.OpenBlock.StatementSeqs) != 1 {
		t.Fatalf("stage3 state = %+v, open block %v", f.st.Stage3, f.st.OpenBlock.StatementSeqs)
	}
}

// TestStage3ActivationRunsTheStage2DrainGate: the drain gate is the
// activation's one state precondition, and the leader's predicted refusal is
// Apply's.
func TestStage3ActivationRunsTheStage2DrainGate(t *testing.T) {
	f, a := tracksFSM(t)
	key, account := testAccount(t)
	stmt := admitTrack(t, f, key, account, 1, 1)
	block := mustSeal(t, f).BlockSeq
	bindTrackClaim(t, f, stmt)
	verifyBlock(t, f, block)
	anchorFinal(t, f, block)
	p := promoteTrack(t, f, a, stmt)
	for _, step := range []struct {
		want string
		next func()
	}{
		{"consensus update requires all promotions to be acknowledged", func() { ackTrackPromotion(t, f, p) }},
		{"consensus update requires all promoted unsafe parts to be cleaned", func() { mustApply(t, f, partCleanup(t, f, a, p)) }},
		{"consensus update requires all cleanup commands to be acknowledged", func() { mustApply(t, f, partCleanupAck(t, f, p)) }},
	} {
		if got := f.Stage3ActivationRefusal(); got != step.want {
			t.Fatalf("refusal = %q, want %q", got, step.want)
		}
		rejectStage3Unchanged(t, f, signedConsensusUpdate(t, a, stage3Activation(t, f, nil)), step.want)
		step.next()
	}
	if got := f.Stage3ActivationRefusal(); got != "" {
		t.Fatalf("refusal after the drain = %q", got)
	}
	activateStage3(t, f, a)
	if f.st.Stage3.ActivationPromotionSeq != p {
		t.Fatalf("activation promotion seq = %d, want %d", f.st.Stage3.ActivationPromotionSeq, p)
	}
}

// TestStage3ActivationWithAQuarantinedTrack: a quarantined track's
// unacknowledged work does not hold the activation back (the drain gate skips
// it), and its SNode is exempt from the stage3_v1 requirement.
func TestStage3ActivationWithAQuarantinedTrack(t *testing.T) {
	f, a := tracksFSM(t)
	key, account := testAccount(t)
	stmt := admitTrack(t, f, key, account, 1, 0)
	block := mustSeal(t, f).BlockSeq
	bindTrackClaim(t, f, stmt)
	verifyBlock(t, f, block)
	anchorFinal(t, f, block)
	promoteTrack(t, f, a, stmt)
	if got := f.Stage3ActivationRefusal(); got != "consensus update requires all promotions to be acknowledged" {
		t.Fatalf("refusal before the quarantine = %q", got)
	}
	mustApply(t, f, quarantineQ(t, f, a, 0, 0, "stage3 activation test"))
	if got := f.Stage3ActivationRefusal(); got != "" {
		t.Fatalf("refusal with track 0 quarantined = %q", got)
	}
	if got := f.Stage3ActivationNodes(); !slices.Equal(got, []string{"s1", "v1", "v2", "v3"}) {
		t.Fatalf("activation nodes = %v, want s0 exempt", got)
	}
	activateStage3(t, f, a)
	if pp := f.st.PendingPromotions[f.st.PromotionSeq]; pp == nil || pp.Acked {
		t.Fatal("the quarantined track's promotion must stay recorded and unacknowledged")
	}
}

// TestStage3ActivationNodesExemptTheSNodeOfAQuarantinedTrack: the gate's node
// set is PurgeNodeSet minus the SNode of every quarantined track, and follows
// a release.
func TestStage3ActivationNodesExemptTheSNodeOfAQuarantinedTrack(t *testing.T) {
	f, a := tracksFSM(t)
	all := []string{"s0", "s1", "v1", "v2", "v3"}
	if got := f.Stage3ActivationNodes(); !slices.Equal(got, all) || !slices.Equal(f.PurgeNodeSet(), all) {
		t.Fatalf("activation nodes = %v, purge set %v, want %v", got, f.PurgeNodeSet(), all)
	}
	mustApply(t, f, quarantineQ(t, f, a, 0, 0, "stage3 nodes test"))
	if got := f.Stage3ActivationNodes(); !slices.Equal(got, []string{"s1", "v1", "v2", "v3"}) || !slices.Equal(f.PurgeNodeSet(), all) {
		t.Fatalf("with track 0 quarantined: activation nodes = %v, purge set %v", got, f.PurgeNodeSet())
	}
	mustApply(t, f, releaseQ(t, f, a, 0, 1))
	if got := f.Stage3ActivationNodes(); !slices.Equal(got, all) {
		t.Fatalf("after the release: activation nodes = %v, want %v", got, all)
	}
}

// stage3Twin applies every command to two FSMs whose only difference is the
// v21 activation, and requires the same answer from both. versionDelta is
// how far act's registry version is ahead of ref's after the two updates
// (Task 65 makes a committed stage3 change bump it); no later command may
// change it.
type stage3Twin struct {
	ref, act     *FSM
	versionDelta uint64
}

func (tw stage3Twin) apply(t *testing.T, c wire.Command) any {
	t.Helper()
	b, err := wire.Encode(c)
	if err != nil {
		t.Fatal(err)
	}
	want := tw.ref.Apply(&raft.Log{Data: b})
	got := tw.act.Apply(&raft.Log{Data: b})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("answers differ: with stage3 %+v, without %+v", got, want)
	}
	if !bytes.Equal(stage3NeutralState(t, tw.act), stage3NeutralState(t, tw.ref)) ||
		tw.act.st.TableRegistry.Version-tw.ref.st.TableRegistry.Version != tw.versionDelta {
		t.Fatalf("after %+v the replicated state differs beyond stage3 and the consensus history", c)
	}
	return want
}

// stage3NeutralState is f's snapshot document without params.stage3, the
// stage3 member, the consensus history and the registry version (stage3Twin
// compares the versions itself), followed by the spent-ids dump.
func stage3NeutralState(t *testing.T, f *FSM) []byte {
	t.Helper()
	jb, err := json.Marshal(snapshotDocument(f.st))
	if err != nil {
		t.Fatal(err)
	}
	var doc, params map[string]json.RawMessage
	if err := json.Unmarshal(jb, &doc); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(doc["params"], &params); err != nil {
		t.Fatal(err)
	}
	delete(params, "stage3")
	delete(doc, "stage3")
	delete(doc, "consensus_updates")
	if doc["params"], err = json.Marshal(params); err != nil {
		t.Fatal(err)
	}
	var registry map[string]json.RawMessage
	if err := json.Unmarshal(doc["table_registry"], &registry); err != nil {
		t.Fatal(err)
	}
	delete(registry, "version")
	if doc["table_registry"], err = json.Marshal(registry); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var spent bytes.Buffer
	if err := f.st.SpentIDs.Snapshot(&spent); err != nil {
		t.Fatal(err)
	}
	return append(out, spent.Bytes()...)
}

// TestStage3ActivationWithEverySwitchOffChangesNoBehaviour: contract §44.
// A tracksFSM network and its restored copy take the same update (a fourth
// pool verifier v4 and max_writers 3), one with stage3 and one without; then
// every command of a history of both owners (v4's registration and
// activation, so the block-seeded selection draws three of four, admission,
// a refused duplicate, seals, claims, evidence, anchors, promotions,
// acknowledgements, cleanups, a publication, a quarantine and its release)
// gets the same answer from both and leaves the same state, apart from
// params.stage3, the stage-3 record and the consensus history.
func TestStage3ActivationWithEverySwitchOffChangesNoBehaviour(t *testing.T) {
	ref, a := tracksFSM(t)
	act := restoreInto(t, snapshotBytes(t, ref))
	v4 := arbiter.VerifierEntry{NodeID: "v4", Ed25519Pubkey: testPubkey('4')}
	without := tracksUpdate(t, ref)
	without.MaxWriters, without.Verifiers = 3, append(without.Verifiers, v4)
	mustApply(t, ref, signedConsensusUpdate(t, a, without))
	with := tracksUpdate(t, act)
	with.MaxWriters, with.Verifiers, with.Stage3 = 3, append(with.Verifiers, v4), stage3ActivationParams()
	mustApply(t, act, signedConsensusUpdate(t, a, with))
	if !act.Stage3Active() || ref.Stage3Active() {
		t.Fatal("exactly one network must have stage3")
	}
	tw := stage3Twin{ref: ref, act: act, versionDelta: act.st.TableRegistry.Version - ref.st.TableRegistry.Version}
	if !bytes.Equal(stage3NeutralState(t, act), stage3NeutralState(t, ref)) || tw.versionDelta > 1 {
		t.Fatal("the two updates must leave the same state apart from stage3")
	}
	for _, c := range []wire.Command{verifierRegistration(t, ref, "v4", '4', 1), verifierMarkActive(t, ref, "v4", '4', 1)} {
		if r, refused := tw.apply(t, c).(Rejected); refused {
			t.Fatalf("v4: %s", r.Reason)
		}
	}
	key, account := testAccount(t)
	// One block per owner: each statement is sealed into its own block.
	var stmts, blocks []uint64
	for i, owner := range []uint64{1, 0} {
		env := trackEnvelope(t, ref, key, account, "", uint64(i)+1, owner)
		r := tw.apply(t, wire.Command{SubmitStatement: &wire.SubmitStatement{Envelope: env}}).(SubmitResult)
		if r.Code != arbiter.AdmissionCodeAccepted {
			t.Fatalf("statement %d = %+v", i+1, r)
		}
		stmts = append(stmts, r.StatementSeq)
		if i == 0 {
			duplicate := trackEnvelope(t, ref, key, account, "", 1, 1)
			if d := tw.apply(t, wire.Command{SubmitStatement: &wire.SubmitStatement{Envelope: duplicate}}).(SubmitResult); d.Code == arbiter.AdmissionCodeAccepted {
				t.Fatal("a duplicate client seq must be refused")
			}
		}
		blocks = append(blocks, tw.apply(t, wire.Command{SealL3Block: &wire.SealL3Block{}}).(SealResult).BlockSeq)
	}
	for i, block := range blocks {
		cmd, _ := ownedClaim(t, ref, stmts[i], "stage3-twin")
		tw.apply(t, cmd)
		tw.apply(t, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: block}})
		for _, c := range pinnedEvidence(t, ref, block) {
			tw.apply(t, c)
		}
		tw.apply(t, wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{L3BlockSeq: block, Anchor: anchorForBlock(t, ref, block),
			FinalityReached: true, LastMergeableReached: true}})
	}
	for _, s := range stmts {
		promotion := partPromotion(t, ref, a, s)
		tw.apply(t, promotion)
		p := promotion.RecordPromotionIssued.Promote.PromotionSeq
		tw.apply(t, partPromotionAck(t, ref, p))
		tw.apply(t, partCleanup(t, ref, a, p))
		tw.apply(t, partCleanupAck(t, ref, p))
	}
	tw.apply(t, wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: manifestFromInputs(t, ref)}})
	tw.apply(t, quarantineQ(t, ref, a, 0, 0, "stage3 twin"))
	tw.apply(t, releaseQ(t, ref, a, 0, 1))
	if last := blocks[len(blocks)-1]; act.st.SafeWatermark.SafeBlockSeq != last || len(act.st.Stage3.Switches) != 0 {
		t.Fatalf("the history must publish block %d and turn on no switch: watermark %+v, stage3 %+v", last, act.st.SafeWatermark, act.st.Stage3)
	}
}

// TestStage3RecordsEachSwitchCutover: each switch's update records its epoch,
// sealed tip and promotion sequence after the update, in §10.4 order; purge
// also records the next incarnation's sequence; an update that turns on no
// switch records nothing.
func TestStage3RecordsEachSwitchCutover(t *testing.T) {
	f, a := stage3FSM(t)
	turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
	want := []Stage3SwitchCutover{{Switch: Stage3SwitchPurge, Epoch: f.st.ConsensusEpoch, BlockSeq: uint64(len(f.st.Blocks)),
		PromotionSeq: f.st.PromotionSeq, IncarnationSeq: uint64(len(f.st.TableRegistry.Incarnations)) + 1}}
	key, account := testAccount(t)
	_, promotions := passTrackBlock(t, f, a, key, account, 1, 1)
	for _, p := range promotions {
		mustApply(t, f, partCleanup(t, f, a, p))
		mustApply(t, f, partCleanupAck(t, f, p))
	}
	raised := stage3Update(t, f)
	raised.Stage3.GroupSize = 4
	mustApply(t, f, signedConsensusUpdate(t, a, raised))
	for _, name := range Stage3Switches()[1:] {
		turnOnStage3Switch(t, f, a, name)
		want = append(want, Stage3SwitchCutover{Switch: name, Epoch: f.st.ConsensusEpoch, BlockSeq: uint64(len(f.st.Blocks)), PromotionSeq: f.st.PromotionSeq})
	}
	if !reflect.DeepEqual(f.st.Stage3.Switches, want) {
		t.Fatalf("cutovers = %+v, want %+v", f.st.Stage3.Switches, want)
	}
	if want[0].BlockSeq == want[1].BlockSeq || want[0].PromotionSeq == want[1].PromotionSeq || want[1].Epoch != want[0].Epoch+2 {
		t.Fatalf("the block between purge and eviction_voiding must move the cutover: %+v", want[:2])
	}
}
```

- [ ] **Step 18: Run them and see them fail**

Run: `W=<worktree path>; cd "$W" && GOWORK=off go test ./fsm/ -run '^(TestStage3ActivationSetsEveryParameterWithEverySwitchOff|TestStage3ActivationRequiresTheTracksActivation|TestStage3ActivationRefusesASwitchStrictCopiesOrAFloorAboveOne|TestStage3ActivationNeedsNoEmptyOpenBlockOrGlobalPrefix|TestStage3ActivationRunsTheStage2DrainGate|TestStage3ActivationWithAQuarantinedTrack|TestStage3ActivationWithEverySwitchOffChangesNoBehaviour|TestStage3MustBeCarriedOnceSet|TestStage3SwitchesAndStrictCopiesAreOneWay|TestStage3MinSafeCopiesAndGroupSizeNeverDecrease|TestStage3OneSwitchPerUpdate|TestStage3SwitchNeedsEveryEarlierSwitchCommitted|TestStage3FloorAboveOneNeedsCheckpointsAndNarrowedDrain|TestStage3PlacementNeedsADomainForEveryIndexer|TestStage3FailureDomainsNameOnlyEnrolledIndexers|TestStage3RecordsEachSwitchCutover|TestStage3RulesReplayOnRestore|TestHostBoundVerifierRefusedBeforeOwnerAwareSelection|TestHostBoundVerifierRules|TestHostBoundVerifierConsentIsVerifiedOnlyOnAddOrKeyRotation|TestVerifierHostIndexerIDNeverChanges|TestPoolVerifierCarriesNoHostBoundFields|TestMemberNodeIDMustBeUnusedByEveryRegisteredNode|TestCloneVerifiersCopiesEveryField|TestCloneParamsCopiesStage3|TestStage3ActivationNodesExemptTheSNodeOfAQuarantinedTrack)$' -race -count=1`
Expected: FAIL, `[build failed]`: `f.Stage3Active undefined (type *FSM has no field or method Stage3Active)`, `f.st.Stage3 undefined (type *State has no field or method Stage3)`, `f.Stage3ActivationRefusal undefined (type *FSM has no field or method Stage3ActivationRefusal)`, `f.st.Params.Stage3 undefined (type Params has no field or method Stage3)`, `undefined: Stage3State`, … `too many errors`.

- [ ] **Step 19: Add `Params.Stage3` and `State.Stage3`**

In `fsm/state.go`, replace (`Params`, `:127-128`):

```go
	TracksEnabled bool `json:"tracks_enabled,omitempty"`
}
```

with:

```go
	TracksEnabled bool `json:"tracks_enabled,omitempty"`
	// Stage3 is the stage-3 parameter (housegate spec 2026-10-14 §6.1,
	// §10.3-§10.4): nil until the v21 activation sets it with every switch
	// off, then carried by every update; its switches and strict_copies are
	// one-way and min_safe_copies and group_size never decrease (plan S3-E2
	// K69). omitempty keeps the params digest of every network that never
	// sets it unchanged.
	Stage3 *arbiter.Stage3Params `json:"stage3,omitempty"`
}
```

and (`State`, `:399-401`):

```go
	// persists it as tracks.
	Tracks *TrackState
}
```

with:

```go
	// persists it as tracks.
	Tracks *TrackState
	// Stage3 is the stage-3 activation record (housegate spec 2026-10-14
	// §10.3-§10.4): nil until the update that first sets Params.Stage3
	// applies, then present for good, with one cutover per switch turned on.
	Stage3 *Stage3State
}
```

- [ ] **Step 20: Create `fsm/stage3.go`**

Create `fsm/stage3.go`:

```go
package fsm

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
)

// Stage 3 of the multi-source storage-integrity network (housegate spec
// 2026-10-14 §10.3, §10.4) starts with one authority-signed update that sets
// the stage3 parameter with every switch off: the v21 activation. Before it
// every behaviour is byte for byte that of ARBITER_TAG_P4; after it, with
// every switch off, only the snapshot container changes (plan S3-E2
// contract §44). Each switch is then turned on by its own update, in §10.4
// order, and is never turned off.

// Stage3State is the replicated stage-3 activation record. It exists exactly
// when Params.Stage3 is committed. Plans S3-E3 and S3-E4 append members, each
// omitempty; the members below keep their names, tags and meaning.
type Stage3State struct {
	// ActivationEpoch is the consensus epoch the activation produced: the
	// 1-based position of the first ConsensusUpdates entry that set stage3.
	ActivationEpoch uint64 `json:"activation_epoch"`
	// ActivationBlockSeq and ActivationPromotionSeq are the sealed tip and
	// PromotionSeq when the activation applied.
	ActivationBlockSeq     uint64 `json:"activation_block_seq"`
	ActivationPromotionSeq uint64 `json:"activation_promotion_seq"`
	// Switches has one cutover per switch turned on, in §10.4 order.
	Switches []Stage3SwitchCutover `json:"switches,omitempty"`
}

// Stage3SwitchCutover records one switch's turn-on: the epoch its update
// produced, and the sealed tip and PromotionSeq after every mutation of that
// Apply. IncarnationSeq is set only for the purge switch: the sequence the
// next registry incarnation gets, so every incarnation from it on is
// incarnation-scoped (spec §8.8).
type Stage3SwitchCutover struct {
	Switch         string `json:"switch"`
	Epoch          uint64 `json:"epoch"`
	BlockSeq       uint64 `json:"block_seq"`
	PromotionSeq   uint64 `json:"promotion_seq"`
	IncarnationSeq uint64 `json:"incarnation_seq,omitempty"`
}

// The stage-3 switches (spec §10.4), in the order they are turned on. Each
// is the name of its Stage3Params field.
const (
	Stage3SwitchPurge               = "purge"
	Stage3SwitchEvictionVoiding     = "eviction_voiding"
	Stage3SwitchOwnerAwareSelection = "owner_aware_selection"
	Stage3SwitchPlacement           = "placement"
	Stage3SwitchReselection         = "reselection"
	Stage3SwitchCheckpoints         = "checkpoints"
	Stage3SwitchNarrowedDrain       = "narrowed_drain"
)

// Stage3Switches returns the switch names in §10.4 order in a fresh slice.
func Stage3Switches() []string {
	return []string{Stage3SwitchPurge, Stage3SwitchEvictionVoiding, Stage3SwitchOwnerAwareSelection,
		Stage3SwitchPlacement, Stage3SwitchReselection, Stage3SwitchCheckpoints, Stage3SwitchNarrowedDrain}
}

// stage3SwitchValue is switch name's value in p, false for a nil p. An
// unknown name is a programming error and panics.
func stage3SwitchValue(p *arbiter.Stage3Params, name string) bool {
	var v arbiter.Stage3Params
	if p != nil {
		v = *p
	}
	switch name {
	case Stage3SwitchPurge:
		return v.Purge
	case Stage3SwitchEvictionVoiding:
		return v.EvictionVoiding
	case Stage3SwitchOwnerAwareSelection:
		return v.OwnerAwareSelection
	case Stage3SwitchPlacement:
		return v.Placement
	case Stage3SwitchReselection:
		return v.Reselection
	case Stage3SwitchCheckpoints:
		return v.Checkpoints
	case Stage3SwitchNarrowedDrain:
		return v.NarrowedDrain
	}
	panic(fmt.Sprintf("fsm: unknown stage3 switch %q", name))
}

// stage3ActiveLocked reports whether the v21 activation has committed. The
// caller holds f.mu.
func (f *FSM) stage3ActiveLocked() bool { return f.st.Params.Stage3 != nil }

// Stage3Active is stage3ActiveLocked for readers outside Apply.
func (f *FSM) Stage3Active() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.stage3ActiveLocked()
}

// stage3SwitchOnLocked reports whether switch name is on in the committed
// parameters; false before the activation. The caller holds f.mu.
func (f *FSM) stage3SwitchOnLocked(name string) bool {
	return stage3SwitchValue(f.st.Params.Stage3, name)
}

// Stage3SwitchOn is stage3SwitchOnLocked for readers outside Apply.
func (f *FSM) Stage3SwitchOn(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.stage3SwitchOnLocked(name)
}

// cloneStage3Params is a deep copy of p; nil stays nil and an empty failure
// domain list becomes nil, so equality and digests never tell them apart.
func cloneStage3Params(p *arbiter.Stage3Params) *arbiter.Stage3Params {
	if p == nil {
		return nil
	}
	c := *p
	c.IndexerFailureDomains = nil
	if len(p.IndexerFailureDomains) > 0 {
		c.IndexerFailureDomains = slices.Clone(p.IndexerFailureDomains)
	}
	return &c
}

// stage3FailureDomain is indexer id's failure domain in p, "" when p is nil
// or names none.
func stage3FailureDomain(p *arbiter.Stage3Params, id uint64) string {
	if p == nil {
		return ""
	}
	domain, _ := p.FailureDomain(id)
	return domain
}

// findVerifierEntry returns the entry of entries listing nodeID.
func findVerifierEntry(entries []arbiter.VerifierEntry, nodeID string) (arbiter.VerifierEntry, bool) {
	for _, v := range entries {
		if v.NodeID == nodeID {
			return v, true
		}
	}
	return arbiter.VerifierEntry{}, false
}

// verifierEntryLocked returns a detached copy of nodeID's committed verifiers
// entry. The caller holds f.mu.
func (f *FSM) verifierEntryLocked(nodeID string) (arbiter.VerifierEntry, bool) {
	v, ok := findVerifierEntry(f.st.Params.Verifiers, nodeID)
	if !ok {
		return arbiter.VerifierEntry{}, false
	}
	return cloneVerifierEntry(v), true
}

// poolVerifierLocked reports whether nodeID is a network-pool verifier: a
// registered, non-evicted node with the VERIFIER role whose committed
// verifiers entry has no host_indexer_id. A host-bound member is never in
// the pool (plan S3-E2 K68). The caller holds f.mu.
func (f *FSM) poolVerifierLocked(nodeID string) bool {
	n := f.st.Nodes[nodeID]
	if n == nil || n.Status == NodeEvicted || !hasRole(n.Registration.Roles, arbiter.NodeRoleVerifier) {
		return false
	}
	v, ok := findVerifierEntry(f.st.Params.Verifiers, nodeID)
	return ok && v.HostIndexerID == nil
}

// hostBoundFieldsSet reports whether v sets any field only a host-bound entry
// may carry (spec §6.1, plan S3-E1 K58).
func hostBoundFieldsSet(v arbiter.VerifierEntry) bool {
	return v.ReplicaSigner != "" || v.ConsentJWS != "" || v.Weight != 0 || v.FailureDomain != "" ||
		v.Verify || v.Replicate || v.Counts || v.SharedRMT
}

// validReplicaSigner reports whether s is a non-zero lowercase 0x-prefixed
// 20-byte address.
func validReplicaSigner(s string) bool {
	if len(s) != 42 || !strings.HasPrefix(s, "0x") || s == "0x0000000000000000000000000000000000000000" {
		return false
	}
	return !strings.ContainsFunc(s[2:], func(r rune) bool { return !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') })
}

// verifyStage3Transition is the Params-only half of the stage3 rules (spec
// §6.1, §10.3, §10.4, plan S3-E2 K69); restore replays it through
// validateConsensusHistory. next carries the update's si_indexers and
// tracks_enabled already. It returns the stage3 parameter next commits.
func verifyStage3Transition(previous, next Params, update *arbiter.Stage3Params) (*arbiter.Stage3Params, error) {
	old := previous.Stage3
	switch {
	case old == nil && update == nil:
		return nil, nil
	case old == nil && !previous.TracksEnabled:
		return nil, fmt.Errorf("consensus update cannot set stage3 before the tracks activation")
	case old == nil && (update.StrictCopies || update.MinSafeCopies != 1 ||
		slices.ContainsFunc(Stage3Switches(), func(name string) bool { return stage3SwitchValue(update, name) })):
		return nil, fmt.Errorf("consensus update setting stage3 must leave every switch off, strict_copies false and min_safe_copies 1")
	case update == nil:
		return nil, fmt.Errorf("consensus update must carry stage3 once set")
	}
	p := cloneStage3Params(update)
	// Defence in depth: arbiter-core's normalizer refuses these already.
	if p.GroupSize < 1 || p.MinSafeCopies < 1 || p.LagLow < 1 || p.LagLow >= p.MaxLagPublications {
		return nil, fmt.Errorf("consensus update stage3 needs group_size and min_safe_copies of at least 1 and 1 <= lag_low < max_lag_publications")
	}
	if old != nil {
		for _, name := range Stage3Switches() {
			if stage3SwitchValue(old, name) && !stage3SwitchValue(p, name) {
				return nil, fmt.Errorf("consensus update cannot turn stage3 switch %s off", name)
			}
		}
		switch {
		case old.StrictCopies && !p.StrictCopies:
			return nil, fmt.Errorf("consensus update cannot turn stage3 strict_copies off")
		case p.MinSafeCopies < old.MinSafeCopies:
			return nil, fmt.Errorf("consensus update cannot lower stage3 min_safe_copies (%d -> %d)", old.MinSafeCopies, p.MinSafeCopies)
		case p.GroupSize < old.GroupSize:
			return nil, fmt.Errorf("consensus update cannot lower stage3 group_size (%d -> %d)", old.GroupSize, p.GroupSize)
		}
		var turned []string
		for _, name := range Stage3Switches() {
			if !stage3SwitchValue(old, name) && stage3SwitchValue(p, name) {
				turned = append(turned, name)
			}
		}
		if len(turned) > 1 {
			return nil, fmt.Errorf("consensus update may turn on at most one stage3 switch, got %s", strings.Join(turned, ", "))
		}
		if len(turned) == 1 {
			for _, earlier := range Stage3Switches()[:slices.Index(Stage3Switches(), turned[0])] {
				if !stage3SwitchValue(old, earlier) {
					return nil, fmt.Errorf("consensus update cannot turn on stage3 switch %s before %s", turned[0], earlier)
				}
			}
		}
	}
	// Before the narrowed drain gate the stage-2 gate refuses every update
	// while a promoted part is uncleaned, and a floor-held part would never be
	// cleaned (K69), so both switches must be committed first.
	if (p.MinSafeCopies > 1 || p.StrictCopies) &&
		!(stage3SwitchValue(old, Stage3SwitchCheckpoints) && stage3SwitchValue(old, Stage3SwitchNarrowedDrain)) {
		return nil, fmt.Errorf("consensus update cannot raise stage3 min_safe_copies above 1 or set strict_copies before the checkpoints and narrowed_drain switches")
	}
	for _, d := range p.IndexerFailureDomains {
		if _, ok := findSIIndexer(next.SIIndexers, d.IndexerID); !ok {
			return nil, fmt.Errorf("consensus update stage3 indexer_failure_domains names indexer %d, which is not an si_indexers entry", d.IndexerID)
		}
	}
	if stage3SwitchValue(p, Stage3SwitchPlacement) {
		for _, e := range next.SIIndexers {
			if _, ok := p.FailureDomain(e.IndexerID); !ok {
				return nil, fmt.Errorf("consensus update stage3 indexer_failure_domains has no domain for indexer %d", e.IndexerID)
			}
		}
	}
	return p, nil
}

// verifyHostBoundVerifiers is the Params-only half of the verifiers entry
// rules of spec §6.1 (plan S3-E2 K68); restore replays it. next carries the
// update's si_indexers, verifiers and stage3 already. Entries are judged in
// node-id order and each refusal names the entry.
func verifyHostBoundVerifiers(previous, next Params, genesisID string) error {
	for _, v := range next.Verifiers {
		old, existed := findVerifierEntry(previous.Verifiers, v.NodeID)
		if v.HostIndexerID == nil {
			// Pool entries keep their v20 bytes, so the params digest of every
			// earlier epoch is unchanged (plan S3-E1 K58).
			if hostBoundFieldsSet(v) {
				return fmt.Errorf("consensus update verifier %s sets host-bound fields without host_indexer_id", v.NodeID)
			}
			if existed && old.HostIndexerID != nil {
				return fmt.Errorf("consensus update cannot change verifier %s host_indexer_id", v.NodeID)
			}
			continue
		}
		if !stage3SwitchValue(previous.Stage3, Stage3SwitchOwnerAwareSelection) {
			return fmt.Errorf("consensus update cannot list host-bound verifier %s before the stage3 owner_aware_selection switch is committed", v.NodeID)
		}
		host := *v.HostIndexerID
		if existed && (old.HostIndexerID == nil || *old.HostIndexerID != host) {
			return fmt.Errorf("consensus update cannot change verifier %s host_indexer_id", v.NodeID)
		}
		entry, ok := findSIIndexer(next.SIIndexers, host)
		if !ok {
			return fmt.Errorf("consensus update verifier %s host_indexer_id %d is not an si_indexers entry", v.NodeID, host)
		}
		if !validReplicaSigner(v.ReplicaSigner) {
			return fmt.Errorf("consensus update verifier %s replica_signer must be a non-zero lowercase 0x-prefixed 20-byte address", v.NodeID)
		}
		if v.Weight < 1 || v.Weight > 64 {
			return fmt.Errorf("consensus update verifier %s weight must be 1 to 64, got %d", v.NodeID, v.Weight)
		}
		if want := stage3FailureDomain(next.Stage3, host); v.FailureDomain != want {
			return fmt.Errorf("consensus update verifier %s failure_domain %q differs from stage3 indexer_failure_domains[%d] %q", v.NodeID, v.FailureDomain, host, want)
		}
		if v.Verify && !v.SharedRMT {
			return fmt.Errorf("consensus update verifier %s sets verify with shared_rmt false; such a member cannot verify before the per-operator transport (spec §6.1)", v.NodeID)
		}
		// The host's consent is verified when the entry is added and when it
		// rotates a key; an unchanged entry keeps the consent it was listed
		// with, even after its host's signer rotates.
		if !existed || !bytes.Equal(old.Ed25519Pubkey, v.Ed25519Pubkey) || old.ReplicaSigner != v.ReplicaSigner {
			stmt := authority.GroupMemberConsentStatement{NetworkID: previous.NetworkID, GenesisSnapshotID: genesisID, HostIndexerID: host,
				NodeID: v.NodeID, Ed25519Pubkey: v.Ed25519Pubkey, ReplicaSigner: v.ReplicaSigner}
			if err := authority.VerifyGroupMemberConsent(stmt, v.ConsentJWS, entry.Signer); err != nil {
				return fmt.Errorf("consensus update verifier %s consent: %v", v.NodeID, err)
			}
		} else if old.ConsentJWS != v.ConsentJWS {
			return fmt.Errorf("consensus update changes verifier %s consent_jws without rotating its keys", v.NodeID)
		}
	}
	return nil
}

// stage3MembershipRefusalLocked is the half of the stage-3 verifiers rules
// that reads committed state; like signedClaimsMembershipLocked it runs only
// in Apply, never on restore. An added host-bound entry must name a node id
// no Nodes record holds, whatever its status, so a member never inherits an
// SNode's or a pool verifier's history (spec §6.1). Plan S3-E3 Task 71
// appends the per-group verifier floor here.
func (f *FSM) stage3MembershipRefusalLocked(next Params) string {
	for _, v := range next.Verifiers {
		if v.HostIndexerID == nil {
			continue
		}
		if _, existed := findVerifierEntry(f.st.Params.Verifiers, v.NodeID); existed {
			continue
		}
		if f.st.Nodes[v.NodeID] != nil {
			return fmt.Sprintf("consensus update verifier %s: a member node_id must be unused by every registered SNode and verifier", v.NodeID)
		}
	}
	return ""
}

// stage3ActivationRefusalLocked is why the update that first sets stage3
// cannot apply now, or "" (spec §10.3). Unlike the tracks activation it needs
// no empty open block and no global prefix: the stage-2 drain gate, which
// Apply runs for every update, is the whole state precondition. The leader's
// stage3 gate reads it through Stage3ActivationRefusal.
func (f *FSM) stage3ActivationRefusalLocked() string {
	if !f.tracksActiveLocked() {
		return "stage3 activation requires the tracks activation"
	}
	return f.drainGateRefusalLocked()
}

// Stage3ActivationRefusal is stage3ActivationRefusalLocked for the leader's
// stage3 gate.
func (f *FSM) Stage3ActivationRefusal() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.stage3ActivationRefusalLocked()
}

// Stage3ActivationNodes is the set of data-plane nodes the leader's stage3
// gate requires stage3_v1 from (spec §10.3): PurgeNodeSet minus the SNode of
// every si_indexers entry whose track is quarantined, sorted.
func (f *FSM) Stage3ActivationNodes() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	exempt := map[string]bool{}
	for _, e := range f.st.Params.SIIndexers {
		if track := f.indexerTrackLocked(e.IndexerID); track != nil && track.Quarantined {
			exempt[e.SNodeNodeID] = true
		}
	}
	var nodes []string
	for _, id := range f.purgeNodeIDsLocked() {
		if !exempt[id] {
			nodes = append(nodes, id)
		}
	}
	return nodes
}

// activateStage3Locked applies the v21 activation after the new params are in
// place and before ConsensusEpoch++: the epoch the update produces, the
// sealed tip and PromotionSeq.
func (f *FSM) activateStage3Locked() {
	f.st.Stage3 = &Stage3State{ActivationEpoch: f.st.ConsensusEpoch + 1,
		ActivationBlockSeq: uint64(len(f.st.Blocks)), ActivationPromotionSeq: f.st.PromotionSeq}
}

// recordStage3SwitchesLocked runs, for an update after the activation, the
// turn-on of the one switch the update turns on (verifyStage3Transition
// allows at most one): onStage3SwitchOnLocked, then its cutover record. It
// runs after the new params are in place and before ConsensusEpoch++.
func (f *FSM) recordStage3SwitchesLocked(previous *arbiter.Stage3Params) {
	for _, name := range Stage3Switches() {
		if stage3SwitchValue(previous, name) || !f.stage3SwitchOnLocked(name) {
			continue
		}
		f.onStage3SwitchOnLocked(name)
		cutover := Stage3SwitchCutover{Switch: name, Epoch: f.st.ConsensusEpoch + 1,
			BlockSeq: uint64(len(f.st.Blocks)), PromotionSeq: f.st.PromotionSeq}
		if name == Stage3SwitchPurge {
			// The seq appendIncarnation gives the next incarnation. The table
			// registry exists: stage3 needs the tracks activation, which needs
			// si_indexers, which need the registry.
			cutover.IncarnationSeq = uint64(len(f.st.TableRegistry.Incarnations)) + 1
		}
		f.st.Stage3.Switches = append(f.st.Stage3.Switches, cutover)
	}
}

// onStage3SwitchOnLocked is the turn-on hook of switch name, called once,
// after the new params are in place and before the cutover is recorded.
// Plan S3-E2 Task 69 handles purge and Task 68 eviction_voiding; Plan S3-E3
// adds placement and checkpoints, Plan S3-E4 narrowed_drain. A switch with
// no case needs nothing at its turn-on.
func (f *FSM) onStage3SwitchOnLocked(name string) {
	switch name {
	default:
	}
}
```

- [ ] **Step 21: Wire the rules into `verifyConsensusTransition` and the activation into Apply**

In `fsm/consensus_updates.go`, `verifyConsensusTransition`, replace (`:246-247`):

```go
	next.TracksEnabled = normalized.TracksEnabled
	// Defence in depth: arbiter-core's normalizer already refuses a writer
```

with:

```go
	next.TracksEnabled = normalized.TracksEnabled
	// stage3 and the host-bound verifiers entries (housegate spec 2026-10-14
	// §6.1, §10.3-§10.4, plan S3-E2 K68, K69) compare Params alone, so restore
	// replays them with the rules above; stage3MembershipRefusalLocked adds
	// the half that reads committed state.
	stage3, err := verifyStage3Transition(previous, next, normalized.Stage3)
	if err != nil {
		return Params{}, err
	}
	next.Stage3 = stage3
	if err := verifyHostBoundVerifiers(previous, next, genesisID); err != nil {
		return Params{}, err
	}
	// Defence in depth: arbiter-core's normalizer already refuses a writer
```

In `applyUpdateConsensusParams`, replace (`:51-54`):

```go
	evict, evictReason, err := f.signedClaimsMembershipLocked(next)
	if err != nil {
		return Rejected{Reason: err.Error()}
	}
```

with:

```go
	evict, evictReason, err := f.signedClaimsMembershipLocked(next)
	if err != nil {
		return Rejected{Reason: err.Error()}
	}
	// The stage-3 verifiers rule that reads committed state (plan S3-E2
	// contract §45); verifyConsensusTransition checked the Params-only half.
	if reason := f.stage3MembershipRefusalLocked(next); reason != "" {
		return Rejected{Reason: reason}
	}
```

replace (`:88-89`):

```go
	wasEnabled := f.st.Params.TableRegistry != nil
	previousLanes := f.st.Params.ClientLanes
```

with:

```go
	// The v21 activation (housegate spec 2026-10-14 §10.3) needs nothing
	// from state beyond the drain gate above: no empty open block and no
	// global prefix. stage3ActivationRefusalLocked is what the leader's gate
	// predicts from the same two rules.
	stage3Activation := f.st.Params.Stage3 == nil && next.Stage3 != nil
	previousStage3 := f.st.Params.Stage3
	wasEnabled := f.st.Params.TableRegistry != nil
	previousLanes := f.st.Params.ClientLanes
```

and replace (`:144-149`):

```go
	if tracksActivation {
		f.activateTracksLocked()
	} else if f.tracksActiveLocked() {
		f.enrolTracksLocked()
	}
	f.st.ConsensusEpoch++
```

with:

```go
	if tracksActivation {
		f.activateTracksLocked()
	} else if f.tracksActiveLocked() {
		f.enrolTracksLocked()
	}
	// The v21 activation records its point; every later update runs the
	// turn-on of the switch it turns on and records its cutover (plan S3-E2
	// contract §45), both in the epoch the update produces.
	if stage3Activation {
		f.activateStage3Locked()
	} else if f.stage3ActiveLocked() {
		f.recordStage3SwitchesLocked(previousStage3)
	}
	f.st.ConsensusEpoch++
```

- [ ] **Step 22: Copy `stage3` in `normalizeConsensusParams` and `cloneParams`**

In `fsm/params_identity.go`, replace (`:64-66`):

```go
	p.Verifiers = cloneVerifiers(p.Verifiers)
	return p
}
```

with:

```go
	p.Verifiers = cloneVerifiers(p.Verifiers)
	p.Stage3 = cloneStage3Params(p.Stage3)
	return p
}
```

In `fsm/consensus_reads.go`, replace (`:88-90`):

```go
	params.Verifiers = cloneVerifiers(params.Verifiers)
	return params
}
```

with:

```go
	params.Verifiers = cloneVerifiers(params.Verifiers)
	params.Stage3 = cloneStage3Params(params.Stage3)
	return params
}
```

- [ ] **Step 23: Run the named tests and the goldens and see them pass**

Run: `W=<worktree path>; cd "$W" && GOWORK=off go test ./fsm/ -run '^(TestStage3ActivationSetsEveryParameterWithEverySwitchOff|TestStage3ActivationRequiresTheTracksActivation|TestStage3ActivationRefusesASwitchStrictCopiesOrAFloorAboveOne|TestStage3ActivationNeedsNoEmptyOpenBlockOrGlobalPrefix|TestStage3ActivationRunsTheStage2DrainGate|TestStage3ActivationWithAQuarantinedTrack|TestStage3ActivationWithEverySwitchOffChangesNoBehaviour|TestStage3MustBeCarriedOnceSet|TestStage3SwitchesAndStrictCopiesAreOneWay|TestStage3MinSafeCopiesAndGroupSizeNeverDecrease|TestStage3OneSwitchPerUpdate|TestStage3SwitchNeedsEveryEarlierSwitchCommitted|TestStage3FloorAboveOneNeedsCheckpointsAndNarrowedDrain|TestStage3PlacementNeedsADomainForEveryIndexer|TestStage3FailureDomainsNameOnlyEnrolledIndexers|TestStage3RecordsEachSwitchCutover|TestStage3RulesReplayOnRestore|TestHostBoundVerifierRefusedBeforeOwnerAwareSelection|TestHostBoundVerifierRules|TestHostBoundVerifierConsentIsVerifiedOnlyOnAddOrKeyRotation|TestVerifierHostIndexerIDNeverChanges|TestPoolVerifierCarriesNoHostBoundFields|TestMemberNodeIDMustBeUnusedByEveryRegisteredNode|TestCloneVerifiersCopiesEveryField|TestCloneParamsCopiesStage3|TestStage3ActivationNodesExemptTheSNodeOfAQuarantinedTrack)$' -race -count=1 -v 2>&1 | grep -c '^--- PASS'`
Expected: `26` (each named test of contract §45 `--- PASS`, with its subtests).

Run: `W=<worktree path>; cd "$W" && GOWORK=off go test ./fsm/ -run '^(TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims|TestPreActivationHistoryIsByteIdenticalAcrossTracks|TestL3CommitmentPreimagesAreFrozen|TestL3TrackHeaderPreimagesAreFrozen|TestL3TransitionHeaderPreimageIsFrozen|TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease|TestPreTracksParamsDigestIsTheStageOneDigest|TestPreActivationParamsDigestIsTheLegacyDigest)$' -count=1 -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: eight `--- PASS` lines and `ok  	github.com/sentioxyz/arbiter/fsm`: the golden files and constants are unedited and still hold.

- [ ] **Step 24: Full verification** (contract §48)

Bazel reuses Step 8's downloads; if it reports fetching anything, stop and ask for explicit confirmation in chat first (remote read).

```bash
W=<worktree path>
cd "$W" && test -z "$(gofmt -l $(git ls-files '*.go') fsm/stage3.go fsm/stage3_fixture_test.go fsm/stage3_activation_test.go)" && echo gofmt-clean
cd "$W" && GOWORK=off go vet ./... && echo vet-clean
cd "$W" && GOWORK=off go test ./... -race -count=1 2>&1 | grep -E '^(--- FAIL|FAIL|panic)' || echo no-failures
cd "$W" && bazel run //:gazelle && git diff -- fsm/BUILD.bazel && git status --short
cd "$W" && bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors
cd "$W" && bazel query 'deps(//fsm:fsm, 1)' | grep com_github_sentioxyz_arbiter_proto; grep -rn 'time\.Now' fsm/; echo red-lines-checked
```

Expected: `gofmt-clean`; `vet-clean`; `no-failures`; the `fsm/BUILD.bazel` diff adds exactly `"stage3.go",` to the `go_library` srcs (after `"snapshot_query_block.go",`) and `"stage3_activation_test.go",`, `"stage3_fixture_test.go",`, `"stage3_params_test.go",` to the `go_test` srcs (after `"source_ack_test.go",`), with no deps change, and `git status --short` lists ` M fsm/BUILD.bazel`, ` M fsm/consensus_reads.go`, ` M fsm/consensus_updates.go`, ` M fsm/params_identity.go`, ` M fsm/state.go`, ` M fsm/stage3_params_test.go`, `?? fsm/stage3.go`, `?? fsm/stage3_activation_test.go` and `?? fsm/stage3_fixture_test.go`; Bazel builds and every test target passes under race (CI's command, `.github/workflows/ci.yml`); the red-line commands print only `red-lines-checked` (the fsm library imports no arbiter-proto and reads no wall clock).

- [ ] **Step 25: Commit**

```bash
W=<worktree path>
git -C "$W" add fsm/BUILD.bazel fsm/consensus_reads.go fsm/consensus_updates.go fsm/params_identity.go fsm/state.go fsm/stage3.go fsm/stage3_activation_test.go fsm/stage3_fixture_test.go fsm/stage3_params_test.go
git -C "$W" status --short
git -C "$W" commit -m "feat(stage3): stage3 parameter, host-bound verifiers rules and the v21 activation (Task 64)

Params.Stage3 and State.Stage3 (the activation point and one cutover
per switch). The Params-only rules run in verifyConsensusTransition, so
restore replays them: stage3 is first set after the tracks activation
with every switch off, strict_copies false and min_safe_copies 1, then
carried; switches and strict_copies are one-way, min_safe_copies and
group_size never fall, one switch per update and only after every
earlier one is committed, the floor and strict copies only after
checkpoints and narrowed_drain are committed, and failure domains for
enrolled indexers only (all of them from placement on). Host-bound
verifiers entries are refused until owner_aware_selection is committed,
keep their host for life, and carry the host's consent, verified on add
and key rotation (K68, K69). An added member's node id must be unused
by every registered node (stage3MembershipRefusalLocked, Apply only).
The activation needs only the stage-2 drain gate; each switch turn-on
runs onStage3SwitchOnLocked and records its cutover. No switch has
behaviour yet.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: the status lists the nine staged files only; one commit. `git -C "$W" log --oneline -3` shows this commit, the clone fix and the pin commit. Nothing is pushed: Task 70's last step pushes the branch (contract §48), and no release, tag, PR or merge happens in this plan (K65).

---

## Task 65: arbiter — the stage-3 gate, the post-activation registration refusal, the stage-3 reads and `arbiter-admin consensus update --stage3` (S3-E2)

Spec §10.3 (the v21 activation is one authority-signed update that sets `stage3` with every switch off; it requires the stage-2 drain gate, the leader's voter probe over the exact Raft voter set, and `stage3_v1` from every registered, non-evicted data-plane node not on a quarantined track), §10.4 (each switch an authority-signed update; one-way; each after every earlier one) and §6.1 (the host-bound `verifiers` entry fields, `stage3` carried once set). Contract §45 fixes the gate (`arbiter-stage3-voters`, `stage3Gate`, `probeFeatureVoters`, the refusal texts), the registration refusal, `consensusMutableParams`, the served `ConsensusParamsState.stage3_state` and `TableRegistrySnapshot.stage3` with the registry-version bump, and every `arbiter-admin` flag, Go name and refusal; clarifications K68 (host-bound entries refused until `owner_aware_selection`, enforced by Task 64's FSM) and K69 (the activation only after the tracks activation; one switch per update). Everything here is leader-local or read-only: no Raft command, no FSM rule and no snapshot byte changes, except that every Apply which commits a changed `Params.Stage3` bumps the table registry version once (contract §45). Before the activation nothing new runs: the gate passes every update without `stage3`, the registration refusal and the bump need `Params.Stage3`, and both reads serve nothing.

Worktree (contract §48): `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e` on branch `urwt/claude/multi-source-s3e`, the worktree Task 64 created and committed to; prefix every Bash call with `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e;`. Line anchors are arbiter `origin/main` `526a721` (`v0.13.0`), which already carries Plan S3-A (the stage-3 reads, `--hold`, and `UpdateConsensusParams` split into the held-update wrapper and `updateConsensusParams`). Task 64's commits move lines of `fsm/consensus_updates.go`, `fsm/consensus_reads.go` and the server test files that hold `stage3PendingProtoFields`, and the branch base at or after `ARBITER_TAG_P4` adds Plan S3-C2's pin and Plan S3-D Task 51 (`indexer settle`, the settle RPCs), so re-anchor each hunk on the branch under the hunk rule (header, Global Constraints): the quoted text binds, the line numbers guide. This task makes no remote call and runs nothing on devnet2.

**Files:**
- Create: `server/voter_probe_test.go` (`voterProbeCase`, `voterProbeCases`, `requireVoterProbeRefusal`, `TestTracksGateRefusalsAreUnchanged`)
- Modify: `server/tracks_gate.go` (`tracksVoterAddresses` and `probeTracksVoters` `:110-196` become `voterAddresses`, `probeTracksVoters` and `probeFeatureVoters`)
- Create: `server/stage3_gate.go` (`Stage3VotersMetadataKey`, `(*Server).stage3Gate`)
- Modify: `server/consensus_admin.go` (`GetConsensusParams` `:50`; `stage3StateToPB` after `trackStateToPB` `:83`; `consensusMutableParams` `:107`; in `updateConsensusParams` `:158` the barrier condition `:175` and the gate calls `:185`)
- Modify: `server/membership.go` (after the tracks refusal `:53`), `server/table_registry.go` (`tableRegistryToPB` `:100`)
- Create: `fsm/stage3_reads.go` (`(*FSM).Stage3StateView`, `stage3StateViewLocked`, `stage3ParamsChanged`)
- Modify: `fsm/consensus_reads.go` (`ConsensusParamsView` `:11`, `ConsensusParamsView()` `:20`), `fsm/table_registry.go` (`TableRegistryView` `:270`, `TableRegistryView()` `:317`), `fsm/consensus_updates.go` (`applyUpdateConsensusParams` `:90`, `:122`)
- Create: `fsm/stage3_reads_test.go`, `server/stage3_fixture_test.go`, `server/stage3_gate_test.go`, `server/stage3_params_test.go`
- Modify: `server/tracks_fixture_test.go` (`serverUpdateRequest` `:100` carries the committed `stage3`)
- Modify (only if Task 64's pin commit added it): the `stage3PendingProtoFields` entry `"TableRegistrySnapshot.stage3"` in `server/safestate_stage3_test.go`
- Create: `cmd/arbiter-admin/stage3.go` (`stage3VotersMetadataKey`, `stage3Activation`, `stage3GateFor`, `stage3ExemptSNodes`, `checkStage3NotActive`, `failureDomainFlags`, `stage3SwitchFlags`, `uint32Flag`, `stage3Flags`, `stage3ActivationRequired`, `setStage3Switch`, `verifierHostKeys`, `parseVerifierFlag`, `parseHostBoundVerifier`, `keyValuesOptional`)
- Modify: `cmd/arbiter-admin/main.go` (usage `:89`, `:157`; `verifierFlags.Set` `:219`; `keyValues` `:265`; the update flags `:387`, `:402`, `:404`; the flag checks `:482`; after the tracks prerequisite `:634`; after the tracks gate `:696`; the `planned` literal `:709`; the pre-submit re-check `:774`; the post-commit re-check `:812`; `activationGate` `:1408`; `observeGate` `:1604` and its three callers `runActivationGate` `:1517`, `recheckActivationGate` `:1533`, `warnGateAfterCommit` `:1551`; `gateObservation` `:1595`; `problems` `:1628`; `lines` `:1708`)
- Create: `cmd/arbiter-admin/stage3_test.go`, `cmd/arbiter-admin/stage3_integration_test.go`
- Modify: `server/BUILD.bazel`, `fsm/BUILD.bazel`, `cmd/arbiter-admin/BUILD.bazel` (gazelle), `README.md` (a new section before `## Artifact disposition admission` `:396`)

**Interfaces:**
- Consumes:
  - Task 64 (contract §45): `fsm.Params.Stage3 *arbiter.Stage3Params`, `fsm.State.Stage3 *Stage3State`, `fsm.Stage3State{ActivationEpoch, ActivationBlockSeq, ActivationPromotionSeq uint64; Switches []Stage3SwitchCutover}`, `fsm.Stage3SwitchCutover{Switch string; Epoch, BlockSeq, PromotionSeq, IncarnationSeq uint64}`, `fsm.Stage3SwitchPurge`, `(*fsm.FSM).Stage3Active() bool`, `(*fsm.FSM).Stage3ActivationRefusal() string` (`stage3 activation requires the tracks activation`, else the drain gate's text, else `""`), `(*fsm.FSM).Stage3ActivationNodes() []string`, `cloneStage3Params`, the activation and cutover Apply (`activateStage3Locked`, `recordStage3SwitchesLocked`), `normalizeConsensusParams` and `cloneParams` copying `Stage3`, the Params-only rules of `verifyConsensusTransition` (`consensus update must carry stage3 once set`, the order and one-way refusals), `stage3PendingProtoFields` (Task 64's pin commit, `server/safestate_stage3_test.go`), and the fixture `fsm/stage3_fixture_test.go` (`stage3ActivationParams`, `stage3Update`, `activateStage3`, `turnOnStage3Switch`).
  - Plan S3-E1 (contract §48; §39–§41 of its contract): `pb.Stage3Params` (fields 1–18), `pb.IndexerFailureDomain`, `pb.ConsensusMutableParams.Stage3` (9), `pb.ConsensusParamsUpdate.Stage3` (14), `pb.ConsensusParamsState.Stage3State` (9), `pb.Stage3StateView{ActivationEpoch, ActivationBlockSeq, ActivationPromotionSeq, Switches}`, `pb.Stage3SwitchCutover{SwitchName, Epoch, CutoverBlockSeq, CutoverPromotionSeq}`, `pb.TableRegistrySnapshot.Stage3` (12), `pb.VerifierEntry` fields 3–11 (`HostIndexerId *uint64`, `ReplicaSigner`, `ConsentJws`, `Weight`, `FailureDomain`, `Verify`, `Replicate`, `Counts`, `SharedRmt`); `arbiter.Stage3Params`, `arbiter.IndexerFailureDomain{IndexerID, FailureDomain}`, `arbiter.ConsensusParamsUpdate.Stage3`, the new `arbiter.VerifierEntry` fields, `arbiter.Stage3Feature` (`stage3_v1`, in `arbiter.LocalNodeFeatures()`), `arbiter.Stage3Switches` and `(arbiter.Stage3Params).SwitchOn` (its §41); `wire.Stage3ParamsToPB`, `wire.Stage3ParamsFromPB`, and `wire.VerifierEntriesToPB`/`FromPB` and `wire.ConsensusParamsUpdateToPB`/`FromPB` carrying every field; `authority.NormalizeConsensusParamsUpdate` with its `stage3` and host-bound `verifiers` rules (failure domains sorted, replica signers lowercased, `stage3 requires tracks_enabled in the same update`).
  - Plan S3-A (contract §4, merged in `526a721`): `arbiter-admin consensus update --hold none|issuance|drain` (default `drain`) with `takeConsensusHold`, `consensusHoldSession`, `hold.await`; `(*consensusAdminService).updateConsensusParams` returning `proposal`, `heldUpdateOutcome`; `PrepareConsensusUpdate` authorizing a draft without any gate.
  - Existing arbiter: `raftMembership`, `termReader`, `featureBook.answer`, `probeVoters`, `TracksVotersMetadataKey`, `tracksGate`, `signedClaimsGate`, `consensusReadBarrier`, `membershipMu`, `isDataPlaneRegistration`, `registryChangedLocked`; arbiter-admin `activationGate`, `runActivationGate`, `recheckActivationGate`, `warnGateAfterCommit`, `gateObservation`, `observeGate`, `diffObservations`, `tracksVotersMetadata`, `decodeVerifierPubkey`, `keyValues`; server test helpers `voterNode`, `newVoterNode`, `gateServer`, `prepareTracksGate`, `enableServerTracks`, `activateServerTracks`, `serverUpdateRequest`, `applyServerUpdate`, `mustDirectApply`, `quarantineCommand`, `signedSNodeRegistration`, `verifierRequest`, `tracksVoter`, `serveTracksVoters`, `serveVoterAdmins`, `serveLeaderAdmin`, `tracksVotersContext`, `hookedVoterNode`, `newRecordingNode`, `startServerWithConfig`, `publishServerGenesis`, `newServerTestFSM`; fsm test helpers `tracksFSM`, `mustApply`, `signedConsensusUpdate`; arbiter-admin test helpers `tracksAdmin`, `fakeAdmin`, `tracksGateFixture`, `tracksUpdateArgs`, `voterArgs`, `serveAdminImpl`, `refused`, `captureStderr`, `testConsensusState`, `adminKey`, `lanesIntegrationNode`, `integrationConsensusNode`, `writeEnrollment`.
- Produces (contract §45 unless marked added):
  - server: `const Stage3VotersMetadataKey = "arbiter-stage3-voters"`; `func (s *Server) stage3Gate(ctx context.Context, update arbiter.ConsensusParamsUpdate) error`; `func (s *Server) probeFeatureVoters(ctx context.Context, voters []string, metadataKey, feature, gate, release string) error`; `func stage3StateToPB(st *fsm.Stage3State) *pb.Stage3StateView`; the refusals `stage3 gate refused: raft configuration has non-voting servers %s; promote or remove them first`, `stage3 gate refused: %s` (the FSM's), `stage3 gate refused: data-plane nodes %s have not registered with this leader advertising stage3_v1; restart them on a stage-3 release`, every tracks probe text with `tracks gate` → `stage3 gate`, `arbiter-tracks-voters` → `arbiter-stage3-voters`, `track_manifests_v1` → `stage3_v1`, `stage-2 release` → `stage-3 release`, and the registration refusal `stage3 is active; this binary does not support stage-3 consensus`; `consensusMutableParams` sets `Stage3`, `GetConsensusParams` sets `Stage3State`, `tableRegistryToPB` sets `Stage3`. Added: `func voterAddresses(ctx context.Context, key string) []string` (replaces `tracksVoterAddresses`), the operational refusals `stage3 gate refused: raft configuration is unavailable on this node`, `stage3 gate refused: raft term is unavailable on this node` and Unavailable `stage3 gate: read raft configuration: %v` (the tracks gate's counterparts).
  - fsm: `func (f *FSM) Stage3StateView() *Stage3State`; `TableRegistryView.Stage3 *arbiter.Stage3Params`; the registry-version bump on a changed `Params.Stage3`. Added: `func (f *FSM) stage3StateViewLocked() *Stage3State`, `ConsensusParamsView.Stage3State *Stage3State` (the same copy taken under the parameters' lock, so one `GetConsensusParams` answer is never torn), `func stage3ParamsChanged(previous, next *arbiter.Stage3Params) bool`.
  - arbiter-admin: the flags `--stage3`, `--stage3-group-size`, `--stage3-network-fill-default`, `--stage3-min-safe-copies`, `--stage3-strict-copies`, `--stage3-max-lag-publications`, `--stage3-lag-low`, `--stage3-evidence-timeout-l2-blocks`, `--stage3-max-uncleaned-parts-per-partition`, `--stage3-max-uncleaned-parts-per-table`, `--stage3-exclude-quarantined-hosts`, `--stage3-switch NAME`, `--indexer-failure-domain 'id=N,domain=D'` and the extended `--verifier`; `const stage3VotersMetadataKey`, `type failureDomainFlags []arbiter.IndexerFailureDomain`, `func keyValuesOptional(value string, required, optional []string) (map[string]string, error)`, `func checkStage3NotActive(ctx context.Context, leader pb.ConsensusAdminClient) error`, `func stage3ExemptSNodes(params *pb.ConsensusParamsState, tracks *pb.TrackStateView) []string`, `var stage3Activation`, `activationGate.exempt func(nodeID string) bool`; every refusal of contract §45 verbatim. Added: `func stage3GateFor(exempt []string) activationGate`, `type stage3SwitchFlags []string`, `type uint32Flag uint32`, `type stage3Flags` with `register`, `check`, `given` and `target`, `var stage3ActivationRequired`, `func setStage3Switch(p *arbiter.Stage3Params, name string)`, `var verifierHostKeys`, `func parseVerifierFlag(value string) (arbiter.VerifierEntry, error)`, `func parseHostBoundVerifier(entry *arbiter.VerifierEntry, fields map[string]string) error`, `gateObservation.exempt []string`, `observeGate` taking the gate (`observeGate(ctx, g, leader, voters, dial)`), and the refusals `--stage3 needs the tracks activation first: tracks_enabled is not committed, and the stage-3 activation must be a later update`, `stage3 gate refused: read the leader's track state: %w`, `stage3 gate refused: read the leader's consensus parameters: %w`, `stage3 gate refused: the leader already reports stage3 committed; re-run consensus show before retrying`, `--verifier %s: host %q is not an unsigned integer`, `--verifier %s consent: %w`, `--indexer-failure-domain id %q is not an unsigned integer`, `key %q has no value` (an optional key given empty); `--voter is only used with … or with --tracks-enabled or --stage3` extends the existing text.
  - Tests (contract §45): server `TestStage3GatePassesEveryUpdateButTheActivation`, `TestStage3GateRefusesNonVoters`, `TestStage3GateRefusesTheFSMActivationRefusal`, `TestStage3GateRequiresStage3V1FromEveryDataPlaneNode`, `TestStage3GateExemptsTheSNodeOfAQuarantinedTrack`, `TestStage3GateProbesEveryVoter`, `TestTracksGateRefusalsAreUnchanged`, `TestRegisterNodeRefusesABinaryWithoutStage3V1AfterTheActivation`, `TestConsensusMutableParamsCarryStage3`, `TestUpdateConsensusParamsTakesTheMembershipBarrierForTheStage3Activation`, `TestGetConsensusParamsServesTheStage3StateView`, `TestTableRegistryServesTheCommittedStage3AndBumpsTheVersion`; arbiter-admin `TestConsensusUpdateStage3SetsEveryParameterWithEverySwitchOff`, `TestConsensusUpdateStage3KeepsTheCommittedValuesWhenFlagsAreOmitted`, `TestConsensusUpdateCarriesTheCommittedStage3AndHostBoundVerifiers`, `TestConsensusUpdateStage3SwitchTurnsOnOneSwitch`, `TestConsensusUpdateStage3FlagRefusals`, `TestVerifierFlagParsesHostBoundEntries`, `TestVerifierFlagRefusesHostBoundKeysWithoutHost`, `TestIndexerFailureDomainFlag`, `TestStage3ActivationGateExemptsSNodesOfQuarantinedTracks`, `TestStage3VotersMetadataKeyMatchesTheServer`. Added: fsm `TestStage3StateViewIsADetachedCopy`, `TestRegistryVersionBumpsOnceForEveryCommittedStage3Change`, `TestStage3ParamsChangedTreatsAnEmptyFailureDomainListAsAbsent`; arbiter-admin `TestConsensusShowJSONHasTheStage3FieldsTheRunbooksRead` (the jq paths Plans S3-E4 and S3-F read), `TestCLIStage3ActivationThroughServerAndFSM` (the CLI, the real server and the production FSM, the activation under the default drain hold); server test helpers `stageTwoFeatures`, `stageThreeVoter`, `stageTwoOnlyVoter`, `stage3VotersContext`, `stageThreeVotersContext`, `stage3GateText`, `serverStage3Params`, `serverStage3ActivationRequest`, `serverPurgeSwitchRequest`, `applyServerRequest`, `activateServerStage3`, `prepareStage3Gate`, `refusedStage3BeforeRaft`; arbiter-admin test helpers `stageTwoFeatures`, `stage3Admin`, `stage3GateFixture`, `stage3ActivationArgs`, `activatedStage3`, `stage3WithSwitches`, `writeConsent`, `memberFlag`, `lastUpdate`.

Notes:
- **One probe, two gates.** The tracks gate's voter probe becomes `probeFeatureVoters(ctx, voters, metadataKey, feature, gate, release)`; `probeTracksVoters` calls it with `TracksVotersMetadataKey`, `track_manifests_v1`, `"tracks gate"` and `"stage-2 release"`, so every tracks refusal is byte-identical. `TestTracksGateRefusalsAreUnchanged` is written and run first, on the unchanged code, then pins the texts across the refactor; `TestStage3GateProbesEveryVoter` runs the same cases through the stage-3 wording.
- **The leader's gate (contract §45).** `stage3Gate` runs after `tracksGate` in `updateConsensusParams`, under `membershipMu` and after the read barrier, which `updateConsensusParams` now also takes when `update.Stage3 != nil && !FSM.Stage3Active()`. It passes every update except the activation, a switch-on included. For the activation, in order: no non-voter; `FSM.Stage3ActivationRefusal()` empty (Task 64's: the tracks activation committed and the stage-2 drain gate passing; no empty open block and no global prefix); every node of `FSM.Stage3ActivationNodes()` advertised `stage3_v1` to this leader in the current term (the SNode of a quarantined track is not among them); every voter probed at the addresses named in `arbiter-stage3-voters`. The FSM's refusal is tested with one update that sets `tracks_enabled` and `stage3` together (Plan S3-E1's normalizer requires `tracks_enabled` beside `stage3`): the tracks gate passes it, and the stage-3 gate refuses with `stage3 gate refused: stage3 activation requires the tracks activation` before it reads the feature book (a drain-gate refusal needs a promoted, unacknowledged block, which the server fixtures do not build; Task 64 tests the drain branch in the FSM). A gate refusal of a held update is a definite outcome (`heldUpdateOutcome`: nothing was submitted), so it ends the hold.
- **After the activation.** `RegisterNode` refuses a data-plane registration without `stage3_v1` (`stage3 is active; this binary does not support stage-3 consensus`), read from applied state under `membershipMu`, like the tracks refusal; a registration issued while the activation holds the lock is judged after the commit (`TestUpdateConsensusParamsTakesTheMembershipBarrierForTheStage3Activation`).
- **Reads (contract §45, decision D33).** `consensusMutableParams` renders `Params.Stage3`; `GetConsensusParams` serves `stage3_state` from `ConsensusParamsView.Stage3State`, the copy `Stage3StateView()` makes, taken under the same lock as the parameters so a switch in `current.stage3` is never served beside another epoch's cutovers; the purge cutover's `IncarnationSeq` is not served. `TableRegistryView.Stage3` copies the committed parameter and `tableRegistryToPB` serves it as `TableRegistrySnapshot.stage3`. `Stage3StateView` copies only the four members of contract §45, so a member Plan S3-E3 or S3-E4 appends to `Stage3State` is never shared with a reader.
- **The registry bump.** `applyUpdateConsensusParams` sets `registryChanged` when `stage3ParamsChanged(previous, next)` holds: the activation, every switch-on and every other `stage3` change, for example a new `max_lag_publications`, after which Plan S3-E3 publishes trimmed `lag_advances`. `stage3ParamsChanged` compares every field and treats an empty failure-domain list as an absent one: a snapshot stores neither (`omitempty`), so a replica restored from a snapshot can never decide a later bump differently from one that applied the whole log (the version is replicated state; Plan S3-E1's normalizer already makes an empty list nil, so this is defence in depth). The change folds into the one `registryChangedLocked` call per Apply, is a deterministic function of the applied update (replay and restore are unaffected) and does nothing before the activation, when both are nil. The version the activation raises is part of the activation's own effect (contract §45); Task 64's `TestStage3ActivationWithEverySwitchOffChangesNoBehaviour` compares Apply results and the effect of later commands. If that test also compares `TableRegistry.Version` across the activation, Step 13 fails there: compare the registry with `Version` set equal in that test (one line of `fsm/stage3_activation_test.go`), commit it with Step 14, and record the change in Appendix B.
- **The CLI.** `--stage3` builds the activation: group_size 3, network_fill_default true and min_safe_copies 1 unless given, every switch off, the five values contract §45 makes required, and `--indexer-failure-domain` as the complete map (the normalizer sorts it). It needs the tracks activation committed (a later update, K69) and `--voter`. Before signing it reads the leader's track state once to exempt the SNodes of quarantined tracks (`stage3ExemptSNodes`), runs the shared activation gate with `stage3GateFor(exempt)`, then re-reads the parameters (`checkStage3NotActive`); after the hold it re-checks with the same exemptions and names the sorted `--voter` addresses in `arbiter-stage3-voters`; after the commit it re-checks again and only warns. An exempt node is listed in the observation (`exempt (quarantined track): <id>`). The leader's own gate decides again from its applied track state at submit, so a track released after the CLI's read is refused there. After the activation every update carries the committed `stage3` and each given `--stage3-*` value or map replaces only itself; `--stage3-switch NAME` turns on one switch and runs no gate. `--verifier` takes the host-bound keys only with `host=N`; `host` needs `replica-signer`, `consent` (a file, trimmed, like `enrollment=`) and `domain`; `weight` defaults to 1 and each flag to `true`; the CLI does not verify the consent (the FSM does, Task 64).
- **Holds.** The activation keeps Plan S3-A's default `--hold drain`, as the P5 runbook runs it (Plan S3-E4 contract): the hold is taken with the draft after the gate and `checkStage3NotActive` and before the re-check, exactly as for the tracks activation, and the leader's `PrepareConsensusUpdate` authorizes the draft without running any gate. `TestCLIStage3ActivationThroughServerAndFSM` commits the activation that way against the real server and FSM; the other new arbiter-admin tests build their arguments on `tracksUpdateArgs`, which ends with `--hold none`, because the fake leaders serve no hold RPC.
- **Fixtures satisfy Plan S3-E1's normalizer.** The stage-3 parameters name failure domains only for enrolled indexers, keep `lag_low` below `max_lag_publications`, turn switches on as prefixes of the §10.4 order, and the host-bound entry `member-b` (host 1, failure domain `node:n1`, every flag true, a non-zero lowercase replica signer, a consent) matches its host's domain, so `authority.NormalizeConsensusParamsUpdate` passes every update the tests sign.

- [ ] **Step 1: Base check**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; git -C "$W" status --porcelain; git -C "$W" log -1 --format=%s; grep -h -E -o 'func \(f \*FSM\) (Stage3ActivationNodes|Stage3ActivationRefusal|Stage3Active)\(' "$W"/fsm/*.go | sort -u; grep -c 'stage3PendingProtoFields = map' "$W"/server/safestate_stage3_test.go`
Expected: no status output; the subject ends with `(Task 64)`; the grep prints the three Task 64 readers, sorted: `func (f *FSM) Stage3ActivationNodes(`, `func (f *FSM) Stage3ActivationRefusal(` and `func (f *FSM) Stage3Active(`; the count is `1` (Task 64's pin-fallout map). Anything else stops this task.

- [ ] **Step 2: Write the characterization test of the tracks gate's voter probe**

Create `server/voter_probe_test.go`:

```go
package server

import (
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core"
)

// voterProbeCase is one way the voter addresses a request names fail the
// shared voter probe (probeFeatureVoters): want is the tracks gate's text,
// which the stage-3 gate gives with its own words (stage3GateText).
type voterProbeCase struct {
	name   string
	admins map[string]pb.ConsensusAdminServer // the addresses that answer
	named  []string                           // the addresses the request names
	want   string
	prefix bool // want is a prefix: the dial error follows it
}

// voterProbeCases runs against the raft voters arb-0, arb-1 and arb-2. A nil
// entry in an override stops that address from answering.
func voterProbeCases() []voterProbeCase {
	answer := func(id string, features ...string) pb.ConsensusAdminServer {
		return &tracksVoter{id: id, features: features}
	}
	all := arbiter.LocalNodeFeatures()
	served := func(override map[string]pb.ConsensusAdminServer) map[string]pb.ConsensusAdminServer {
		out := map[string]pb.ConsensusAdminServer{"arb-0:7100": answer("arb-0", all...), "arb-1:7100": answer("arb-1", all...), "arb-2:7100": answer("arb-2", all...)}
		for address, admin := range override {
			if admin == nil {
				delete(out, address)
				continue
			}
			out[address] = admin
		}
		return out
	}
	every := []string{"arb-0:7100", "arb-1:7100", "arb-2:7100"}
	return []voterProbeCase{
		{name: "no address named", admins: served(nil),
			want: "tracks gate refused: name every voter's admin address in arbiter-tracks-voters"},
		{name: "an address named twice", admins: served(nil), named: []string{"arb-0:7100", "arb-0:7100", "arb-1:7100"},
			want: "tracks gate refused: voter address arb-0:7100 is named more than once in arbiter-tracks-voters"},
		{name: "fewer addresses than voters", admins: served(nil), named: every[:2],
			want: "tracks gate refused: 2 voter addresses named for 3 raft voters [arb-0 arb-1 arb-2]; name every voter's admin address in arbiter-tracks-voters"},
		{name: "an address that does not answer", admins: served(map[string]pb.ConsensusAdminServer{"arb-2:7100": nil}), named: every,
			want: "tracks gate refused: voter at arb-2:7100 could not be probed: ", prefix: true},
		{name: "an answer without a node id", admins: served(map[string]pb.ConsensusAdminServer{"arb-1:7100": answer("", all...)}), named: every,
			want: "tracks gate refused: voter at arb-1:7100 reported no node id"},
		{name: "two addresses answering as one voter", admins: served(map[string]pb.ConsensusAdminServer{"arb-1:7100": answer("arb-0", all...)}), named: every,
			want: "tracks gate refused: arb-0:7100 and arb-1:7100 both answer as voter arb-0; name each voter's own admin address"},
		{name: "a node outside the voter configuration", admins: served(map[string]pb.ConsensusAdminServer{"arb-2:7100": answer("arb-9", all...)}), named: every,
			want: "tracks gate refused: node arb-9 (arb-2:7100) is not in the raft voter configuration [arb-0 arb-1 arb-2]"},
		{name: "a voter on a stage-1 release", admins: served(map[string]pb.ConsensusAdminServer{"arb-1:7100": answer("arb-1", arbiter.ClientLanesFeature, arbiter.SignedClaimsFeature)}), named: every,
			want: "tracks gate refused: voter arb-1 (arb-1:7100) lacks track_manifests_v1; restart it on a stage-2 release"},
	}
}

// requireVoterProbeRefusal requires err to be FailedPrecondition with
// message want (or starting with it).
func requireVoterProbeRefusal(t *testing.T, err error, want string, prefix bool) {
	t.Helper()
	msg := status.Convert(err).Message()
	if status.Code(err) != codes.FailedPrecondition || (prefix && !strings.HasPrefix(msg, want)) || (!prefix && msg != want) {
		t.Fatalf("probe = %v, want FailedPrecondition %q (prefix %v)", err, want, prefix)
	}
}

// TestTracksGateRefusalsAreUnchanged: plan S3-E2 contract §45. The tracks
// gate's voter probe runs through the probe the stage-3 gate shares; every
// tracks refusal keeps its exact text.
func TestTracksGateRefusalsAreUnchanged(t *testing.T) {
	for _, tc := range voterProbeCases() {
		t.Run(tc.name, func(t *testing.T) {
			vn := newVoterNode(t, "arb-0", "arb-1", "arb-2")
			s := gateServer(vn)
			serveVoterAdmins(t, s, tc.admins)
			err := s.probeTracksVoters(tracksVotersContext(tc.named...), vn.voters)
			requireVoterProbeRefusal(t, err, tc.want, tc.prefix)
		})
	}
}
```

- [ ] **Step 3: Run it on the unchanged probe and see it pass**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./server/ -run 'TestTracksGateRefusalsAreUnchanged' -race -count=1 -v`
Expected: PASS with its eight subtests (`no_address_named`, `an_address_named_twice`, `fewer_addresses_than_voters`, `an_address_that_does_not_answer`, `an_answer_without_a_node_id`, `two_addresses_answering_as_one_voter`, `a_node_outside_the_voter_configuration`, `a_voter_on_a_stage-1_release`) and `ok  	github.com/sentioxyz/arbiter/server`: the texts are today's.

- [ ] **Step 4: Commit the characterization test**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && bazel run //:gazelle && git status --short`
Expected: gazelle adds `"voter_probe_test.go",` to the `srcs` of `go_test(name = "server_test")` in `server/BUILD.bazel` (after `"tracks_read_api_test.go",`); `git status` lists ` M server/BUILD.bazel` and `?? server/voter_probe_test.go` only.

```bash
git -C "$W" add server/voter_probe_test.go server/BUILD.bazel
git -C "$W" commit -m "test(stage3): pin every voter-probe refusal of the tracks gate (Task 65)

Characterizes the tracks gate's voter probe before the stage-3 gate
shares it: no address, an address twice, a count mismatch, an
unreachable address, an answer without a node id, two addresses as one
voter, a foreign node and a voter without track_manifests_v1, each with
its exact text (plan S3-E2 contract §45).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Share the voter probe**

In `server/tracks_gate.go:110-196`, replace `tracksVoterAddresses` and `probeTracksVoters` (from the comment `// tracksVoterAddresses lists the admin addresses the request names in` through the closing brace of `probeTracksVoters`, the blank line after it kept) with:

```go
// voterAddresses lists the admin addresses the request names in the gRPC
// metadata key (arbiter-tracks-voters or arbiter-stage3-voters), in order,
// blanks dropped and duplicates kept so the gate can refuse them.
func voterAddresses(ctx context.Context, key string) []string {
	md, _ := metadata.FromIncomingContext(ctx)
	var out []string
	for _, v := range md.Get(key) {
		for _, address := range strings.Split(v, ",") {
			if address = strings.TrimSpace(address); address != "" {
				out = append(out, address)
			}
		}
	}
	return out
}

// probeTracksVoters is the voter half of the tracks gate (amendment A31):
// probeFeatureVoters with the tracks gate's key, feature and wording, so
// every refusal text is the one the tracks gate has always given.
func (s *Server) probeTracksVoters(ctx context.Context, voters []string) error {
	return s.probeFeatureVoters(ctx, voters, TracksVotersMetadataKey, arbiter.TrackManifestsFeature, "tracks gate", "stage-2 release")
}

// probeFeatureVoters is the voter half of an activation gate: the tracks gate
// (amendment A31) and the stage-3 gate (plan S3-E2 contract §45). The voters
// must decode the activation update and every command after it, and only a
// voter's own GetProtocolInfo can say what it runs, so the leader dials every
// admin address the request names in metadataKey and requires the answers to
// prove each Raft voter reports feature. Anything it cannot prove refuses the
// update with FailedPrecondition, every text prefixed "<gate> refused: " and
// naming the address or voter: no address named, an address named twice or a
// number of addresses other than the number of voters (both before any
// dial), an address that cannot be probed or reports no node id, two
// addresses answering as one node, a node outside the voter configuration, a
// voter without feature (to be restarted on a <release>), and a voter no
// address answered for (unreachable once the counts match and every answer
// names a distinct voter, but kept so the rule stands on its own). The
// answers are judged in address order, so the same cluster always gets the
// same refusal.
func (s *Server) probeFeatureVoters(ctx context.Context, voters []string, metadataKey, feature, gate, release string) error {
	addresses := voterAddresses(ctx, metadataKey)
	if len(addresses) == 0 {
		return status.Errorf(codes.FailedPrecondition, "%s refused: name every voter's admin address in %s", gate, metadataKey)
	}
	named := make(map[string]bool, len(addresses))
	for _, address := range addresses {
		if named[address] {
			return status.Errorf(codes.FailedPrecondition, "%s refused: voter address %s is named more than once in %s", gate, address, metadataKey)
		}
		named[address] = true
	}
	// The probed node ids must be exactly the voters, so a request that
	// names more or fewer addresses than there are voters can never pass:
	// refuse it before dialing anything (final review M4), since the gate
	// holds membershipMu exclusively while it probes.
	if len(addresses) != len(voters) {
		return status.Errorf(codes.FailedPrecondition, "%s refused: %d voter addresses named for %d raft voters %v; name every voter's admin address in %s",
			gate, len(addresses), len(voters), voters, metadataKey)
	}
	probes := s.probeVoters(ctx, addresses)
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	probedAt := make(map[string]string, len(addresses)) // node id -> address
	for i, address := range addresses {
		info, err := probes[i].info, probes[i].err
		if err != nil {
			return status.Errorf(codes.FailedPrecondition, "%s refused: voter at %s could not be probed: %v", gate, address, err)
		}
		id := info.GetNodeId()
		if id == "" {
			return status.Errorf(codes.FailedPrecondition, "%s refused: voter at %s reported no node id", gate, address)
		}
		if other, ok := probedAt[id]; ok {
			return status.Errorf(codes.FailedPrecondition, "%s refused: %s and %s both answer as voter %s; name each voter's own admin address", gate, other, address, id)
		}
		probedAt[id] = address
		if !slices.Contains(voters, id) {
			return status.Errorf(codes.FailedPrecondition, "%s refused: node %s (%s) is not in the raft voter configuration %v", gate, id, address, voters)
		}
		if !slices.Contains(info.GetFeatures(), feature) {
			return status.Errorf(codes.FailedPrecondition, "%s refused: voter %s (%s) lacks %s; restart it on a %s", gate, id, address, feature, release)
		}
	}
	var unprobed []string
	for _, id := range voters {
		if _, ok := probedAt[id]; !ok {
			unprobed = append(unprobed, id)
		}
	}
	if len(unprobed) > 0 {
		return status.Errorf(codes.FailedPrecondition, "%s refused: raft voters %s were not probed; name every voter's admin address in %s",
			gate, strings.Join(unprobed, ", "), metadataKey)
	}
	return nil
}
```

- [ ] **Step 6: Run the tracks tests and see them still pass**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test -z "$(gofmt -l server)" && go test ./server/ -run 'TestTracksGate|TestTracksActivation|TestRegistrationCannotSlip' -race -count=1`
Expected: gofmt lists nothing and `ok  	github.com/sentioxyz/arbiter/server`: `TestTracksGateRefusalsAreUnchanged` and every existing tracks-gate test (`TestTracksGateProbesEveryVoterItself`, `TestTracksGateRefusesUnprovableVoterAnswers` and `TestTracksGateFailsClosedOnAHungVoter` among them) pass unchanged.

- [ ] **Step 7: Write the failing server and FSM tests**

Create `server/stage3_fixture_test.go`:

```go
package server

import (
	"context"
	"slices"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/metadata"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// stageTwoFeatures is what a stage-2 data-plane binary advertises: every
// feature up to track_manifests_v1, no stage3_v1.
var stageTwoFeatures = []string{arbiter.ClientLanesFeature, arbiter.SignedClaimsFeature, arbiter.TrackManifestsFeature}

// stageThreeVoter answers as id with every local feature, stage3_v1
// included; stageTwoOnlyVoter answers as id on a stage-2 release.
func stageThreeVoter(id string) *tracksVoter {
	return &tracksVoter{id: id, features: arbiter.LocalNodeFeatures()}
}

func stageTwoOnlyVoter(id string) *tracksVoter {
	return &tracksVoter{id: id, features: slices.Clone(stageTwoFeatures)}
}

// stage3VotersContext is an UpdateConsensusParams request context naming
// addresses in arbiter-stage3-voters, comma-separated, as arbiter-admin sends
// its --voter flags for the stage-3 activation.
func stage3VotersContext(addresses ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(Stage3VotersMetadataKey, strings.Join(addresses, ",")))
}

// stageThreeVotersContext serves every id as a stage-3 voter at "<id>:7100"
// for s's gates and returns the request context naming them all.
func stageThreeVotersContext(t *testing.T, s *Server, ids ...string) context.Context {
	t.Helper()
	voters := make(map[string]*tracksVoter, len(ids))
	addresses := make([]string, 0, len(ids))
	for _, id := range ids {
		address := id + ":7100"
		voters[address] = stageThreeVoter(id)
		addresses = append(addresses, address)
	}
	serveTracksVoters(t, s, voters)
	return stage3VotersContext(addresses...)
}

// stage3GateText is a tracks gate refusal as the stage-3 gate words it (plan
// S3-E2 contract §45).
func stage3GateText(tracks string) string {
	return strings.NewReplacer("tracks gate", "stage3 gate", "arbiter-tracks-voters", "arbiter-stage3-voters",
		arbiter.TrackManifestsFeature, arbiter.Stage3Feature, "stage-2 release", "stage-3 release").Replace(tracks)
}

// serverStage3Params is the stage-3 parameter these tests activate: group
// size 3 with network fill, one safe copy, indexer 1's failure domain, every
// switch off.
func serverStage3Params() *arbiter.Stage3Params {
	return &arbiter.Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1,
		IndexerFailureDomains: []arbiter.IndexerFailureDomain{{IndexerID: 1, FailureDomain: "node:n1"}},
		MaxLagPublications:    8, LagLow: 2, EvidenceTimeoutL2Blocks: 30,
		MaxUncleanedPartsPerPartition: 2000, MaxUncleanedPartsPerTable: 20000}
}

// serverStage3ActivationRequest is the v21 activation over node's current
// view: every committed value carried, stage3 set with every switch off.
func serverStage3ActivationRequest(t *testing.T, node *fakeNode) *pb.UpdateConsensusParamsCmd {
	t.Helper()
	return serverUpdateRequest(t, node, func(u *arbiter.ConsensusParamsUpdate) { u.Stage3 = serverStage3Params() })
}

// serverPurgeSwitchRequest turns on the first switch, purge, over node's
// current view, carrying the rest of the committed stage3.
func serverPurgeSwitchRequest(t *testing.T, node *fakeNode) *pb.UpdateConsensusParamsCmd {
	t.Helper()
	return serverUpdateRequest(t, node, func(u *arbiter.ConsensusParamsUpdate) {
		next := *u.Stage3
		next.Purge = true
		u.Stage3 = &next
	})
}

// applyServerRequest commits req directly, bypassing the admin handler.
func applyServerRequest(t *testing.T, node *fakeNode, req *pb.UpdateConsensusParamsCmd) {
	t.Helper()
	mustDirectApply(t, node, wire.Command{UpdateConsensusParams: &wire.UpdateConsensusParams{
		Update: wire.ConsensusParamsUpdateFromPB(req.GetUpdate()), AuthorityJWS: req.GetAuthorityJws()}})
}

// activateServerStage3 commits the v21 activation directly on a network
// whose tracks activation is committed.
func activateServerStage3(t *testing.T, node *fakeNode) {
	t.Helper()
	applyServerRequest(t, node, serverStage3ActivationRequest(t, node))
	if !node.f.Stage3Active() {
		t.Fatal("stage3 not active")
	}
}

// prepareStage3Gate is prepareTracksGate (s1 advertising s1Features, v1..v3
// every local feature, all through s's gateway, so s's feature book holds
// them) followed by the tracks activation applied directly. Nothing is
// sealed or promoted, so the stage-2 drain gate passes.
func prepareStage3Gate(t *testing.T, s *Server, vn voterNode, s1Features ...string) {
	t.Helper()
	prepareTracksGate(t, s, vn, s1Features...)
	enableServerTracks(t, vn.fakeNode)
}
```

In `server/tracks_fixture_test.go:110-111`, in `serverUpdateRequest` (every committed value is carried, so a post-activation update carries `stage3`, which Task 64's FSM requires), replace

```go
		SIIndexers: view.Current.SIIndexers, Verifiers: view.Current.Verifiers, TracksEnabled: view.Current.TracksEnabled,
	}
```

with

```go
		SIIndexers: view.Current.SIIndexers, Verifiers: view.Current.Verifiers, TracksEnabled: view.Current.TracksEnabled,
		Stage3: view.Current.Stage3,
	}
```

Create `server/stage3_gate_test.go`:

```go
package server

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
)

// refusedStage3BeforeRaft requires err to be the stage-3 gate's
// FailedPrecondition with message want, and nothing to have reached Raft
// since before Apply calls.
func refusedStage3BeforeRaft(t *testing.T, vn voterNode, before int, err error, want string) {
	t.Helper()
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != want {
		t.Fatalf("stage3 activation = %v, want FailedPrecondition %q", err, want)
	}
	if vn.appliedCount() != before || vn.f.Stage3Active() {
		t.Fatal("the refused stage3 activation reached Raft")
	}
}

// TestStage3GatePassesEveryUpdateButTheActivation: plan S3-E2 contract §45.
// An update without stage3 before the activation, and every update after it
// (a switch-on included), skip the gate: here a non-voter, an SNode on a
// stage-2 binary and a request that names no voter refuse nothing.
func TestStage3GatePassesEveryUpdateButTheActivation(t *testing.T) {
	vn := newVoterNode(t, "arb-0")
	vn.nonvoters = []string{"arb-7"}
	s := gateServer(vn)
	prepareStage3Gate(t, s, vn, stageTwoFeatures...)
	admin := &consensusAdminService{s: s}
	ctx := context.Background()
	if _, err := admin.UpdateConsensusParams(ctx, serverUpdateRequest(t, vn.fakeNode, func(u *arbiter.ConsensusParamsUpdate) { u.MaxWriters = 2 })); err != nil {
		t.Fatalf("an update without stage3 = %v", err)
	}
	activateServerStage3(t, vn.fakeNode)
	if _, err := admin.UpdateConsensusParams(ctx, serverUpdateRequest(t, vn.fakeNode, func(u *arbiter.ConsensusParamsUpdate) { u.MaxWriters = 3 })); err != nil {
		t.Fatalf("an update after the activation = %v", err)
	}
	if _, err := admin.UpdateConsensusParams(ctx, serverPurgeSwitchRequest(t, vn.fakeNode)); err != nil {
		t.Fatalf("a switch-on after the activation = %v", err)
	}
	view, err := vn.f.ConsensusParamsView()
	if err != nil || view.Current.MaxWriters != 3 || view.Current.Stage3 == nil || !view.Current.Stage3.Purge {
		t.Fatalf("view after the updates: %+v, %v", view.Current, err)
	}
}

// TestStage3GateRefusesNonVoters: a non-voter applies every command, but no
// gate can prove it runs a stage-3 build.
func TestStage3GateRefusesNonVoters(t *testing.T) {
	vn := newVoterNode(t, "arb-0")
	vn.nonvoters = []string{"arb-7"}
	s := gateServer(vn)
	prepareStage3Gate(t, s, vn, arbiter.LocalNodeFeatures()...)
	before := vn.appliedCount()
	_, err := (&consensusAdminService{s: s}).UpdateConsensusParams(stageThreeVotersContext(t, s, "arb-0"), serverStage3ActivationRequest(t, vn.fakeNode))
	refusedStage3BeforeRaft(t, vn, before, err, "stage3 gate refused: raft configuration has non-voting servers arb-7; promote or remove them first")
}

// TestStage3GateRefusesTheFSMActivationRefusal: the gate answers with the
// FSM's own activation refusal (FSM.Stage3ActivationRefusal, which Apply
// decides again) and checks it before the feature book. Here one update sets
// tracks_enabled and stage3 together: the tracks gate passes it, and the
// stage-3 gate refuses because the tracks activation is not committed, though
// s1 has not advertised stage3_v1 either.
func TestStage3GateRefusesTheFSMActivationRefusal(t *testing.T) {
	vn := newVoterNode(t, "arb-0")
	s := gateServer(vn)
	prepareTracksGate(t, s, vn, stageTwoFeatures...)
	voter := stageThreeVoter("arb-0")
	serveTracksVoters(t, s, map[string]*tracksVoter{"arb-0:7100": voter})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(TracksVotersMetadataKey, "arb-0:7100", Stage3VotersMetadataKey, "arb-0:7100"))
	req := serverUpdateRequest(t, vn.fakeNode, func(u *arbiter.ConsensusParamsUpdate) {
		u.TracksEnabled, u.Stage3 = true, serverStage3Params()
	})
	before := vn.appliedCount()
	_, err := (&consensusAdminService{s: s}).UpdateConsensusParams(ctx, req)
	refusedStage3BeforeRaft(t, vn, before, err, "stage3 gate refused: stage3 activation requires the tracks activation")
	if vn.f.TracksActive() {
		t.Fatal("the refused update activated tracks")
	}
	if n := voter.probes.Load(); n != 1 {
		t.Fatalf("the voter was probed %d times, want once (by the tracks gate; the stage-3 gate refused before its probe)", n)
	}
}

// TestStage3GateRequiresStage3V1FromEveryDataPlaneNode: the leader refuses
// the activation before proposing while a registered data-plane node has not
// advertised stage3_v1 to it in this term, reads after a barrier, and
// accepts once the node re-registers on a stage-3 binary.
func TestStage3GateRequiresStage3V1FromEveryDataPlaneNode(t *testing.T) {
	vn := newVoterNode(t, "arb-0")
	var barriers atomic.Int32
	vn.barrier = func(time.Duration) error { barriers.Add(1); return nil }
	s := gateServer(vn)
	prepareStage3Gate(t, s, vn, stageTwoFeatures...) // s1 still runs a stage-2 binary
	admin := &consensusAdminService{s: s}
	ctx := stageThreeVotersContext(t, s, "arb-0")
	before, barriersBefore := vn.appliedCount(), barriers.Load()
	_, err := admin.UpdateConsensusParams(ctx, serverStage3ActivationRequest(t, vn.fakeNode))
	refusedStage3BeforeRaft(t, vn, before, err,
		"stage3 gate refused: data-plane nodes s1 have not registered with this leader advertising stage3_v1; restart them on a stage-3 release")
	if barriers.Load() == barriersBefore {
		t.Fatal("the stage3 gate read applied state without a barrier")
	}
	founder := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
	if _, err := (&membershipService{s: s}).RegisterNode(ctx, signedSNodeRegistration(t, vn.f, founder, "s1", 1760000000000, arbiter.LocalNodeFeatures()...)); err != nil {
		t.Fatalf("s1 re-registers on a stage-3 binary: %v", err)
	}
	if _, err := admin.UpdateConsensusParams(ctx, serverStage3ActivationRequest(t, vn.fakeNode)); err != nil {
		t.Fatalf("activation once every node advertises stage3_v1 = %v", err)
	}
	if !vn.f.Stage3Active() {
		t.Fatal("stage3 not active")
	}
}

// TestStage3GateExemptsTheSNodeOfAQuarantinedTrack: housegate spec
// 2026-10-14 §10.3. stage3_v1 is required from every non-quarantined
// data-plane node: the SNode of a quarantined track
// (FSM.Stage3ActivationNodes leaves it out) may still run a stage-2 binary.
func TestStage3GateExemptsTheSNodeOfAQuarantinedTrack(t *testing.T) {
	vn := newVoterNode(t, "arb-0")
	s := gateServer(vn)
	prepareStage3Gate(t, s, vn, stageTwoFeatures...)
	mustDirectApply(t, vn.fakeNode, quarantineCommand(t, vn.f, 1, 0, "s1 stays on a stage-2 release"))
	if got := vn.f.Stage3ActivationNodes(); !slices.Equal(got, []string{"v1", "v2", "v3"}) {
		t.Fatalf("Stage3ActivationNodes = %v, want the verifiers only", got)
	}
	if _, err := (&consensusAdminService{s: s}).UpdateConsensusParams(stageThreeVotersContext(t, s, "arb-0"), serverStage3ActivationRequest(t, vn.fakeNode)); err != nil {
		t.Fatalf("activation with the quarantined track's SNode on a stage-2 binary = %v", err)
	}
	if !vn.f.Stage3Active() {
		t.Fatal("stage3 not active")
	}
}

// TestStage3GateProbesEveryVoter: the leader probes every voter the request
// names in arbiter-stage3-voters itself, over a direct RPC too. A voter on a
// stage-2 release (track_manifests_v1, no stage3_v1) is refused; the tracks
// key alone names no stage-3 voter; the activation commits once every voter
// reports stage3_v1, each probed once; and every unprovable answer is
// refused with the tracks gate's text in the stage-3 gate's words.
func TestStage3GateProbesEveryVoter(t *testing.T) {
	vn := newVoterNode(t, "arb-0", "arb-1", "arb-2")
	s := gateServer(vn)
	prepareStage3Gate(t, s, vn, arbiter.LocalNodeFeatures()...)
	serveTracksVoters(t, s, map[string]*tracksVoter{
		"arb-0:7100": stageThreeVoter("arb-0"), "arb-1:7100": stageTwoOnlyVoter("arb-1"), "arb-2:7100": stageThreeVoter("arb-2")})
	client := pb.NewConsensusAdminClient(serveLeaderAdmin(t, s))
	every := "arb-0:7100,arb-1:7100,arb-2:7100"
	before := vn.appliedCount()
	_, err := client.UpdateConsensusParams(metadata.AppendToOutgoingContext(t.Context(), Stage3VotersMetadataKey, every), serverStage3ActivationRequest(t, vn.fakeNode))
	refusedStage3BeforeRaft(t, vn, before, err, "stage3 gate refused: voter arb-1 (arb-1:7100) lacks stage3_v1; restart it on a stage-3 release")
	_, err = client.UpdateConsensusParams(metadata.AppendToOutgoingContext(t.Context(), TracksVotersMetadataKey, every), serverStage3ActivationRequest(t, vn.fakeNode))
	refusedStage3BeforeRaft(t, vn, before, err, "stage3 gate refused: name every voter's admin address in arbiter-stage3-voters")

	voters := []*tracksVoter{stageThreeVoter("arb-0"), stageThreeVoter("arb-1"), stageThreeVoter("arb-2")}
	serveTracksVoters(t, s, map[string]*tracksVoter{"arb-0:7100": voters[0], "arb-1:7100": voters[1], "arb-2:7100": voters[2]})
	if _, err := client.UpdateConsensusParams(metadata.AppendToOutgoingContext(t.Context(), Stage3VotersMetadataKey, every), serverStage3ActivationRequest(t, vn.fakeNode)); err != nil {
		t.Fatalf("activation with every voter on a stage-3 release = %v", err)
	}
	if !vn.f.Stage3Active() {
		t.Fatal("stage3 not active")
	}
	for _, v := range voters {
		if n := v.probes.Load(); n != 1 {
			t.Fatalf("voter %s was probed %d times, want once", v.id, n)
		}
	}

	for _, tc := range voterProbeCases() {
		t.Run(tc.name, func(t *testing.T) {
			vn := newVoterNode(t, "arb-0", "arb-1", "arb-2")
			s := gateServer(vn)
			serveVoterAdmins(t, s, tc.admins)
			err := s.probeFeatureVoters(stage3VotersContext(tc.named...), vn.voters, Stage3VotersMetadataKey, arbiter.Stage3Feature, "stage3 gate", "stage-3 release")
			requireVoterProbeRefusal(t, err, stage3GateText(tc.want), tc.prefix)
		})
	}
}

// TestRegisterNodeRefusesABinaryWithoutStage3V1AfterTheActivation: plan
// S3-E2 contract §45. Before the activation a stage-2 data-plane binary
// registers as before; after it the gateway refuses the binary before
// proposing and accepts a stage-3 one.
func TestRegisterNodeRefusesABinaryWithoutStage3V1AfterTheActivation(t *testing.T) {
	rec := newRecordingNode(t)
	founder := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
	activateServerTracks(t, rec.fakeNode)
	m := &membershipService{s: New(Deps{Node: rec, FSM: rec.f, Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second}})}
	ctx := context.Background()
	if _, err := m.RegisterNode(ctx, signedSNodeRegistration(t, rec.f, founder, "s1", 1760000000000, stageTwoFeatures...)); err != nil {
		t.Fatalf("a stage-2 SNode before the stage-3 activation = %v", err)
	}
	activateServerStage3(t, rec.fakeNode)
	const want = "stage3 is active; this binary does not support stage-3 consensus"
	before := rec.proposals()
	for _, req := range []*pb.NodeRegistration{
		signedSNodeRegistration(t, rec.f, founder, "s1", 1760000000001, stageTwoFeatures...),
		verifierRequest("v4", 4, stageTwoFeatures...),
	} {
		_, err := m.RegisterNode(ctx, req)
		if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != want {
			t.Fatalf("%s on a stage-2 binary after the activation = %v", req.GetNodeId(), err)
		}
	}
	if rec.proposals() != before {
		t.Fatal("a refused registration was proposed")
	}
	if _, err := m.RegisterNode(ctx, signedSNodeRegistration(t, rec.f, founder, "s1", 1760000000002, arbiter.LocalNodeFeatures()...)); err != nil {
		t.Fatalf("a stage-3 SNode after the activation = %v", err)
	}
}

// TestUpdateConsensusParamsTakesTheMembershipBarrierForTheStage3Activation:
// the activation holds membershipMu exclusively from its read barrier through
// its proposal, so a stage-2 registration issued meanwhile runs only after
// the activation committed, where the gateway refuses it before proposing;
// an update after the activation takes neither the lock nor a barrier.
func TestUpdateConsensusParamsTakesTheMembershipBarrierForTheStage3Activation(t *testing.T) {
	vn := newVoterNode(t, "arb-0")
	var barriers atomic.Int32
	vn.barrier = func(time.Duration) error { barriers.Add(1); return nil }
	node := &hookedVoterNode{voterNode: vn}
	s := New(Deps{Node: node, FSM: vn.f, Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second,
		ConsensusUpdatesEnabled: true, ConsensusAdminMaxTokenAge: 5 * time.Minute}})
	prepareStage3Gate(t, s, vn, arbiter.LocalNodeFeatures()...)
	m := &membershipService{s: s}
	registered := make(chan error, 1)
	var once sync.Once
	node.beforeApply = func(cmd wire.Command) {
		if cmd.UpdateConsensusParams == nil || cmd.UpdateConsensusParams.Update.Stage3 == nil || vn.f.Stage3Active() {
			return
		}
		once.Do(func() {
			go func() {
				_, err := m.RegisterNode(context.Background(), verifierRequest("v4", 4, stageTwoFeatures...))
				registered <- err
			}()
			select {
			case err := <-registered:
				registered <- err
			case <-time.After(500 * time.Millisecond):
			}
		})
	}
	admin := &consensusAdminService{s: s}
	barriersBefore := barriers.Load()
	if _, err := admin.UpdateConsensusParams(stageThreeVotersContext(t, s, "arb-0"), serverStage3ActivationRequest(t, vn.fakeNode)); err != nil {
		t.Fatalf("activation: %v", err)
	}
	if barriers.Load() == barriersBefore {
		t.Fatal("the stage3 activation crossed no read barrier")
	}
	err := <-registered
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "stage3 is active; this binary does not support stage-3 consensus" {
		t.Fatalf("the concurrent stage-2 registration = %v, want the post-activation refusal", err)
	}
	node.mu.Lock()
	order := slices.Clone(node.order)
	node.mu.Unlock()
	if slices.Contains(order, "register v4") {
		t.Fatalf("apply order %v: the stage-2 registration reached the log", order)
	}
	barriersBefore = barriers.Load()
	if _, err := admin.UpdateConsensusParams(context.Background(), serverUpdateRequest(t, vn.fakeNode, func(u *arbiter.ConsensusParamsUpdate) { u.MaxWriters = 2 })); err != nil {
		t.Fatalf("an update after the activation = %v", err)
	}
	if barriers.Load() != barriersBefore {
		t.Fatal("an update after the activation crossed a read barrier")
	}
}
```

Create `server/stage3_params_test.go`:

```go
package server

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
)

// TestConsensusMutableParamsCarryStage3: plan S3-E2 contract §45. The
// committed stage3 is rendered through arbiter-core's converter, every field
// included; before the activation it is absent.
func TestConsensusMutableParamsCarryStage3(t *testing.T) {
	if consensusMutableParams(fsm.Params{}).GetStage3() != nil {
		t.Fatal("pre-activation params rendered stage3")
	}
	p := serverStage3Params()
	p.Purge, p.EvictionVoiding, p.ExcludeQuarantinedHosts = true, true, true
	if got := consensusMutableParams(fsm.Params{Stage3: p}).GetStage3(); !proto.Equal(got, wire.Stage3ParamsToPB(p)) {
		t.Fatalf("rendered stage3 %v, want %v", got, wire.Stage3ParamsToPB(p))
	}
}

// TestGetConsensusParamsServesTheStage3StateView: GetConsensusParams carries
// the committed stage3 in current (never in bootstrap) and the v21
// activation and switch cutovers in stage3_state, absent before the
// activation; the cutover's incarnation seq is not served (decision D33).
func TestGetConsensusParamsServesTheStage3StateView(t *testing.T) {
	state := newServerTestFSM(t)
	conn, _, node := startServerWithConfig(t, startServerConfig{f: state, leader: true})
	publishServerGenesis(t, node)
	activateServerTracks(t, node)
	client := pb.NewConsensusAdminClient(conn)
	got, err := client.GetConsensusParams(t.Context(), &emptypb.Empty{})
	if err != nil || got.GetStage3State() != nil || got.GetCurrent().GetStage3() != nil {
		t.Fatalf("before the activation: stage3_state %v, current.stage3 %v, %v", got.GetStage3State(), got.GetCurrent().GetStage3(), err)
	}
	activateServerStage3(t, node)
	applyServerRequest(t, node, serverPurgeSwitchRequest(t, node))
	got, err = client.GetConsensusParams(t.Context(), &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	want := state.Stage3StateView()
	if want == nil || len(want.Switches) != 1 {
		t.Fatalf("FSM stage3 state = %+v", want)
	}
	if !proto.Equal(got.GetStage3State(), stage3StateToPB(want)) {
		t.Fatalf("stage3_state = %v, want %v", got.GetStage3State(), stage3StateToPB(want))
	}
	view := got.GetStage3State()
	cut := view.GetSwitches()[0]
	if view.GetActivationEpoch() != got.GetEpoch()-1 || view.GetActivationBlockSeq() != want.ActivationBlockSeq ||
		view.GetActivationPromotionSeq() != want.ActivationPromotionSeq || cut.GetSwitchName() != fsm.Stage3SwitchPurge ||
		cut.GetEpoch() != got.GetEpoch() || cut.GetCutoverBlockSeq() != want.Switches[0].BlockSeq || cut.GetCutoverPromotionSeq() != want.Switches[0].PromotionSeq {
		t.Fatalf("stage3_state %v at epoch %d", view, got.GetEpoch())
	}
	if !got.GetCurrent().GetStage3().GetPurge() || got.GetBootstrap().GetStage3() != nil {
		t.Fatalf("current.stage3 %v, bootstrap.stage3 %v", got.GetCurrent().GetStage3(), got.GetBootstrap().GetStage3())
	}
	if stage3StateToPB(nil) != nil {
		t.Fatal("a nil stage3 state rendered a view")
	}
}

// TestTableRegistryServesTheCommittedStage3AndBumpsTheVersion: the registry
// snapshot carries the committed stage3 (absent before the activation), and
// every Apply that commits a changed stage3, the activation and each
// switch-on, bumps the registry version once, so WatchTableRegistry
// followers learn each switch; an update that keeps stage3 does not.
func TestTableRegistryServesTheCommittedStage3AndBumpsTheVersion(t *testing.T) {
	state := newServerTestFSM(t)
	conn, _, node := startServerWithConfig(t, startServerConfig{f: state, leader: true})
	publishServerGenesis(t, node)
	activateServerTracks(t, node)
	client := pb.NewTableRegistryClient(conn)
	read := func() *pb.TableRegistrySnapshot {
		t.Helper()
		got, err := client.GetTableRegistry(t.Context(), &emptypb.Empty{})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	before := read()
	if before.GetStage3() != nil {
		t.Fatalf("before the activation the registry served stage3 %v", before.GetStage3())
	}
	activateServerStage3(t, node)
	activated := read()
	committed := func() *arbiter.Stage3Params {
		t.Helper()
		view, err := state.ConsensusParamsView()
		if err != nil {
			t.Fatal(err)
		}
		return view.Current.Stage3
	}
	if activated.GetVersion() != before.GetVersion()+1 || !proto.Equal(activated.GetStage3(), wire.Stage3ParamsToPB(committed())) {
		t.Fatalf("after the activation: version %d (was %d), stage3 %v", activated.GetVersion(), before.GetVersion(), activated.GetStage3())
	}
	applyServerRequest(t, node, serverPurgeSwitchRequest(t, node))
	switched := read()
	if switched.GetVersion() != activated.GetVersion()+1 || !switched.GetStage3().GetPurge() || !proto.Equal(switched.GetStage3(), wire.Stage3ParamsToPB(committed())) {
		t.Fatalf("after the purge switch: version %d (was %d), stage3 %v", switched.GetVersion(), activated.GetVersion(), switched.GetStage3())
	}
	applyServerUpdate(t, node, func(u *arbiter.ConsensusParamsUpdate) {
		view, err := state.ConsensusParamsView()
		if err != nil {
			t.Fatal(err)
		}
		u.MaxWriters, u.SIIndexers, u.Verifiers, u.TracksEnabled, u.Stage3 = 3, view.Current.SIIndexers, view.Current.Verifiers, true, view.Current.Stage3
	})
	if kept := read(); kept.GetVersion() != switched.GetVersion() || !proto.Equal(kept.GetStage3(), switched.GetStage3()) {
		t.Fatalf("an update that keeps stage3: version %d (was %d), stage3 %v", kept.GetVersion(), switched.GetVersion(), kept.GetStage3())
	}
}
```

Create `fsm/stage3_reads_test.go`:

```go
package fsm

import (
	"reflect"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestStage3StateViewIsADetachedCopy: plan S3-E2 contract §45. Before the
// v21 activation both reads are nil; afterwards Stage3StateView and
// ConsensusParamsView.Stage3State carry the activation and every cutover, and
// editing either changes no committed state.
func TestStage3StateViewIsADetachedCopy(t *testing.T) {
	f, a := tracksFSM(t)
	view, err := f.ConsensusParamsView()
	if err != nil || f.Stage3StateView() != nil || view.Stage3State != nil {
		t.Fatalf("before the activation: Stage3StateView %+v, ConsensusParamsView.Stage3State %+v, %v", f.Stage3StateView(), view.Stage3State, err)
	}
	activateStage3(t, f, a)
	turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
	got := f.Stage3StateView()
	want := Stage3State{ActivationEpoch: f.st.Stage3.ActivationEpoch, ActivationBlockSeq: f.st.Stage3.ActivationBlockSeq,
		ActivationPromotionSeq: f.st.Stage3.ActivationPromotionSeq, Switches: f.st.Stage3.Switches}
	if got == nil || !reflect.DeepEqual(*got, want) || len(got.Switches) != 1 {
		t.Fatalf("Stage3StateView = %+v, want %+v", got, want)
	}
	if view, err = f.ConsensusParamsView(); err != nil || !reflect.DeepEqual(view.Stage3State, got) {
		t.Fatalf("ConsensusParamsView.Stage3State = %+v, want %+v (%v)", view.Stage3State, got, err)
	}
	got.ActivationEpoch, got.Switches[0].Epoch = 99, 99
	view.Stage3State.Switches[0].BlockSeq = 99
	if f.st.Stage3.ActivationEpoch == 99 || f.st.Stage3.Switches[0].Epoch == 99 || f.st.Stage3.Switches[0].BlockSeq == 99 {
		t.Fatalf("a read view shares committed state: %+v", f.st.Stage3)
	}
}

// TestRegistryVersionBumpsOnceForEveryCommittedStage3Change: plan S3-E2
// contract §45. The v21 activation, a switch-on and any other change of the
// committed stage3 bump the table registry version once each, so
// WatchTableRegistry followers learn them; an update that carries stage3
// unchanged does not.
func TestRegistryVersionBumpsOnceForEveryCommittedStage3Change(t *testing.T) {
	f, a := tracksFSM(t)
	version := func() uint64 { return f.st.TableRegistry.Version }
	// mutate edits a copy of the committed stage3 (nil before the
	// activation), never the committed parameter itself.
	step := func(name string, mutate func(u *arbiter.ConsensusParamsUpdate, next *arbiter.Stage3Params), bump uint64) {
		t.Helper()
		before := version()
		u := stage3Update(t, f)
		next := cloneStage3Params(u.Stage3)
		mutate(&u, next)
		mustApply(t, f, signedConsensusUpdate(t, a, u))
		if got := version() - before; got != bump {
			t.Fatalf("%s bumped the registry version by %d, want %d", name, got, bump)
		}
	}
	step("the activation", func(u *arbiter.ConsensusParamsUpdate, _ *arbiter.Stage3Params) { u.Stage3 = stage3ActivationParams() }, 1)
	step("an update carrying stage3 unchanged", func(u *arbiter.ConsensusParamsUpdate, _ *arbiter.Stage3Params) { u.MaxWriters = 3 }, 0)
	step("the purge switch-on", func(u *arbiter.ConsensusParamsUpdate, next *arbiter.Stage3Params) {
		next.Purge = true
		u.Stage3 = next
	}, 1)
	step("a new max_lag_publications", func(u *arbiter.ConsensusParamsUpdate, next *arbiter.Stage3Params) {
		next.MaxLagPublications++
		u.Stage3 = next
	}, 1)
}

// TestStage3ParamsChangedTreatsAnEmptyFailureDomainListAsAbsent: the bump
// rule compares every field, but an empty failure-domain list equals an
// absent one, as a restored snapshot stores neither.
func TestStage3ParamsChangedTreatsAnEmptyFailureDomainListAsAbsent(t *testing.T) {
	base := stage3ActivationParams()
	noDomains := cloneStage3Params(base)
	noDomains.IndexerFailureDomains = nil
	emptyDomains := cloneStage3Params(base)
	emptyDomains.IndexerFailureDomains = []arbiter.IndexerFailureDomain{}
	otherDomain := cloneStage3Params(base)
	otherDomain.IndexerFailureDomains[1].FailureDomain = "node:n9"
	lagLow := cloneStage3Params(base)
	lagLow.LagLow++
	purge := cloneStage3Params(base)
	purge.Purge = true
	for _, tc := range []struct {
		name           string
		previous, next *arbiter.Stage3Params
		want           bool
	}{
		{"both absent", nil, nil, false},
		{"the activation", nil, base, true},
		{"a copy", base, cloneStage3Params(base), false},
		{"an empty list for an absent one", noDomains, emptyDomains, false},
		{"another domain", base, otherDomain, true},
		{"a switch", base, purge, true},
		{"lag_low", base, lagLow, true},
	} {
		if got := stage3ParamsChanged(tc.previous, tc.next); got != tc.want {
			t.Errorf("%s: stage3ParamsChanged = %v, want %v", tc.name, got, tc.want)
		}
	}
}
```

- [ ] **Step 8: Run them and see them fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./server/ ./fsm/ -run 'TestStage3Gate|TestRegisterNodeRefusesABinaryWithoutStage3V1AfterTheActivation|TestConsensusMutableParamsCarryStage3|TestUpdateConsensusParamsTakesTheMembershipBarrierForTheStage3Activation|TestGetConsensusParamsServesTheStage3StateView|TestTableRegistryServesTheCommittedStage3AndBumpsTheVersion|TestStage3StateViewIsADetachedCopy|TestRegistryVersionBumpsOnceForEveryCommittedStage3Change|TestStage3ParamsChangedTreatsAnEmptyFailureDomainListAsAbsent' -race -count=1`
Expected: `FAIL	github.com/sentioxyz/arbiter/server [build failed]` and `FAIL	github.com/sentioxyz/arbiter/fsm [build failed]`; the server build reports `undefined: Stage3VotersMetadataKey` (in `stage3_fixture_test.go` and `stage3_gate_test.go`), `state.Stage3StateView undefined (type *fsm.FSM has no field or method Stage3StateView)` and `undefined: stage3StateToPB` (in `stage3_params_test.go`); the fsm build reports `f.Stage3StateView undefined (type *FSM has no field or method Stage3StateView)`, `view.Stage3State undefined (type ConsensusParamsView has no field or method Stage3State)` and `undefined: stage3ParamsChanged` (in `stage3_reads_test.go`).

- [ ] **Step 9: Implement the FSM reads and the registry bump**

Create `fsm/stage3_reads.go`:

```go
package fsm

import (
	"reflect"
	"slices"

	"github.com/sentioxyz/arbiter-core"
)

// Stage3StateView is a copy of the v21 activation and its switch cutovers
// (plan S3-E2 contract §45), nil before the activation. It copies the four
// members of contract §45 and nothing a later plan appends to Stage3State,
// so no reader reaches committed state through it. GetConsensusParams serves
// it as stage3_state, without the purge cutover's IncarnationSeq (decision
// D33).
func (f *FSM) Stage3StateView() *Stage3State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.stage3StateViewLocked()
}

func (f *FSM) stage3StateViewLocked() *Stage3State {
	if f.st.Stage3 == nil {
		return nil
	}
	return &Stage3State{
		ActivationEpoch:        f.st.Stage3.ActivationEpoch,
		ActivationBlockSeq:     f.st.Stage3.ActivationBlockSeq,
		ActivationPromotionSeq: f.st.Stage3.ActivationPromotionSeq,
		Switches:               slices.Clone(f.st.Stage3.Switches),
	}
}

// stage3ParamsChanged reports whether next differs from previous as a
// committed stage3 parameter. An empty failure-domain list counts as an
// absent one: a snapshot stores neither (omitempty), so a replica restored
// from it decides every later registry bump exactly as a replica that
// applied the whole log.
func stage3ParamsChanged(previous, next *arbiter.Stage3Params) bool {
	if previous == nil || next == nil {
		return previous != next
	}
	a, b := *previous, *next
	if !slices.Equal(a.IndexerFailureDomains, b.IndexerFailureDomains) {
		return true
	}
	a.IndexerFailureDomains, b.IndexerFailureDomains = nil, nil
	return !reflect.DeepEqual(a, b)
}
```

In `fsm/consensus_reads.go:16-18`, at the end of `ConsensusParamsView`, replace

```go
	PromotionSeq      uint64
	GenesisSnapshotID string
}
```

with

```go
	PromotionSeq      uint64
	GenesisSnapshotID string
	// Stage3State is Stage3StateView taken under the same lock as the
	// parameters, so one GetConsensusParams answer never pairs a switch with
	// another epoch's cutovers; nil before the v21 activation.
	Stage3State *Stage3State
}
```

In `fsm/consensus_reads.go:33-34`, in the literal `ConsensusParamsView()` returns, replace

```go
		Epoch: f.st.ConsensusEpoch, ParamsDigest: digest, PromotionSeq: f.st.PromotionSeq, GenesisSnapshotID: genesisID,
	}, nil
```

with

```go
		Epoch: f.st.ConsensusEpoch, ParamsDigest: digest, PromotionSeq: f.st.PromotionSeq, GenesisSnapshotID: genesisID,
		Stage3State: f.stage3StateViewLocked(),
	}, nil
```

In `fsm/table_registry.go:292-293`, at the end of `TableRegistryView`, replace

```go
	QuarantinedIndexers []uint64
}
```

with

```go
	QuarantinedIndexers []uint64
	// Stage3 is the committed stage-3 parameter (nil before the v21
	// activation); registry followers, the members' consensus loops among
	// them, learn every switch from it. Every Apply that commits a changed
	// stage3 bumps Version (plan S3-E2 contract §45).
	Stage3 *arbiter.Stage3Params
}
```

In `fsm/table_registry.go:347-348`, at the end of `TableRegistryView()`, replace

```go
	out.QuarantinedIndexers = f.quarantinedIndexersLocked()
	return out, true
```

with

```go
	out.QuarantinedIndexers = f.quarantinedIndexersLocked()
	out.Stage3 = cloneStage3Params(f.st.Params.Stage3)
	return out, true
```

In `fsm/consensus_updates.go:90-91`, in `applyUpdateConsensusParams` (right before the new parameters are installed; Task 64's `previousStage3` local, declared a few lines above, stays as it is), replace

```go
	previousSI := f.st.Params.SIIndexers
	f.st.Params = next
```

with

```go
	previousSI := f.st.Params.SIIndexers
	stage3Changed := stage3ParamsChanged(f.st.Params.Stage3, next.Stage3)
	f.st.Params = next
```

In `fsm/consensus_updates.go:122`, right after the `si_indexers` bump (`} else if !slices.Equal(previousSI, next.SIIndexers) { … }`) and before the evictions, so the change folds into the one `registryChangedLocked` call of the Apply, replace

```go
	// The nodes the update leaves out are evicted now (rule 6, CONTRACT §3c),
```

with

```go
	// Every committed change of stage3, the v21 activation and each
	// switch-on included, reaches registry followers the same way (plan
	// S3-E2 contract §45): the members' consensus loops learn the switches
	// from the registry snapshot. stage3 needs the tracks activation, so the
	// registry is enabled whenever it changes.
	if stage3Changed {
		registryChanged = true
	}
	// The nodes the update leaves out are evicted now (rule 6, CONTRACT §3c),
```

- [ ] **Step 10: Implement the gate, the refusal and the served fields**

Create `server/stage3_gate.go`:

```go
package server

import (
	"context"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core"
)

// Stage3VotersMetadataKey is the gRPC metadata key under which arbiter-admin
// consensus update --stage3 names, comma-separated, the direct
// ConsensusAdmin address of every Raft voter (its --voter flags; plan S3-E2
// contract §45, the TracksVotersMetadataKey pattern). The leader probes each
// named address itself before it proposes the v21 activation. It carries no
// authority: the authority token signs the update.
const Stage3VotersMetadataKey = "arbiter-stage3-voters"

// stage3Gate is the leader's v21 activation gate (housegate spec 2026-10-14
// §10.3, plan S3-E2 contract §45), the tracks gate's pattern reused. An
// update that first sets stage3 is refused, before anything is proposed,
// unless in this order:
//
//  1. the Raft configuration has no non-voter, which no gate could vouch for;
//  2. the FSM's own activation refusal is empty on this leader's applied
//     state (the tracks activation committed and the stage-2 drain gate
//     passing: FSM.Stage3ActivationRefusal, which Apply decides again; unlike
//     the tracks activation it needs no empty open block and no global
//     prefix);
//  3. every node of FSM.Stage3ActivationNodes (every registered, non-evicted
//     SNode and verifier except the SNode of a quarantined track) advertised
//     stage3_v1 in a registration applied through this leader in the current
//     term;
//  4. every voter, probed by this leader itself at the admin address the
//     request names in arbiter-stage3-voters, reports stage3_v1, and the
//     probed node ids are exactly the Raft voters (probeFeatureVoters).
//
// Every other update passes, a switch-on included: the activation already
// proved every voter and data-plane node stage-3, and RegisterNode refuses a
// stage-2 data-plane binary from then on. updateConsensusParams calls it
// under membershipMu and after a read barrier, after tracksGate. Node
// features are never replicated, so this is an admission decision only;
// arbiter-admin's own probe is a preflight.
func (s *Server) stage3Gate(ctx context.Context, update arbiter.ConsensusParamsUpdate) error {
	if update.Stage3 == nil || s.d.FSM.Stage3Active() {
		return nil
	}
	membership, ok := s.d.Node.(raftMembership)
	if !ok {
		return status.Error(codes.FailedPrecondition, "stage3 gate refused: raft configuration is unavailable on this node")
	}
	terms, ok := s.d.Node.(termReader)
	if !ok {
		return status.Error(codes.FailedPrecondition, "stage3 gate refused: raft term is unavailable on this node")
	}
	voters, nonvoters, err := membership.RaftServerIDs()
	if err != nil {
		return status.Errorf(codes.Unavailable, "stage3 gate: read raft configuration: %v", err)
	}
	if len(nonvoters) > 0 {
		return status.Errorf(codes.FailedPrecondition, "stage3 gate refused: raft configuration has non-voting servers %s; promote or remove them first", strings.Join(nonvoters, ", "))
	}
	if reason := s.d.FSM.Stage3ActivationRefusal(); reason != "" {
		return status.Errorf(codes.FailedPrecondition, "stage3 gate refused: %s", reason)
	}
	term := terms.CurrentTerm()
	var missing []string
	for _, id := range s.d.FSM.Stage3ActivationNodes() {
		if e, ok := s.features.answer(id, term); !ok || !slices.Contains(e.features, arbiter.Stage3Feature) {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return status.Errorf(codes.FailedPrecondition, "stage3 gate refused: data-plane nodes %s have not registered with this leader advertising %s; restart them on a stage-3 release",
			strings.Join(missing, ", "), arbiter.Stage3Feature)
	}
	return s.probeFeatureVoters(ctx, voters, Stage3VotersMetadataKey, arbiter.Stage3Feature, "stage3 gate", "stage-3 release")
}
```

In `server/consensus_admin.go:66-68`, at the end of `GetConsensusParams`, replace

```go
		PromotionSeq:      view.PromotionSeq,
	}, nil
}
```

with

```go
		PromotionSeq:      view.PromotionSeq,
		Stage3State:       stage3StateToPB(view.Stage3State),
	}, nil
}
```

In `server/consensus_admin.go:107`, between `trackStateToPB` and `consensusMutableParams`, replace

```go
	return out
}

func consensusMutableParams(p fsm.Params) *pb.ConsensusMutableParams {
```

with

```go
	return out
}

// stage3StateToPB maps the FSM's v21 activation and switch cutovers to
// ConsensusParamsState.stage3_state (plan S3-E1 §39); nil before the
// activation. The purge cutover's IncarnationSeq is not served (decision
// D33): no runbook reads it, and restore checks it.
func stage3StateToPB(st *fsm.Stage3State) *pb.Stage3StateView {
	if st == nil {
		return nil
	}
	out := &pb.Stage3StateView{ActivationEpoch: st.ActivationEpoch, ActivationBlockSeq: st.ActivationBlockSeq,
		ActivationPromotionSeq: st.ActivationPromotionSeq}
	for _, c := range st.Switches {
		out.Switches = append(out.Switches, &pb.Stage3SwitchCutover{SwitchName: c.Switch, Epoch: c.Epoch,
			CutoverBlockSeq: c.BlockSeq, CutoverPromotionSeq: c.PromotionSeq})
	}
	return out
}

func consensusMutableParams(p fsm.Params) *pb.ConsensusMutableParams {
```

In `server/consensus_admin.go:120-122`, at the end of `consensusMutableParams`, replace

```go
		TracksEnabled:                 p.TracksEnabled,
	}
}
```

with

```go
		TracksEnabled:                 p.TracksEnabled,
		Stage3:                        wire.Stage3ParamsToPB(p.Stage3),
	}
}
```

In `server/consensus_admin.go:174-175`, the barrier condition of `updateConsensusParams` (the body of the `if` is unchanged), replace

```go
	// after the activation, where RegisterNode refuses a stage-1 binary.
	if (len(update.SIIndexers) > 0 && !svc.s.d.FSM.SignedClaimsActive()) || (update.TracksEnabled && !svc.s.d.FSM.TracksActive()) {
```

with

```go
	// after the activation, where RegisterNode refuses a stage-1 binary. The
	// v21 activation decides its feature check the same way (plan S3-E2
	// contract §45), so RegisterNode refuses a stage-2 binary after it.
	if (len(update.SIIndexers) > 0 && !svc.s.d.FSM.SignedClaimsActive()) || (update.TracksEnabled && !svc.s.d.FSM.TracksActive()) ||
		(update.Stage3 != nil && !svc.s.d.FSM.Stage3Active()) {
```

In `server/consensus_admin.go:185-187`, the gate calls of `updateConsensusParams`, right after `tracksGate`, replace

```go
	if err := svc.s.tracksGate(ctx, update); err != nil {
		return proposal{}, err
	}
```

with

```go
	if err := svc.s.tracksGate(ctx, update); err != nil {
		return proposal{}, err
	}
	if err := svc.s.stage3Gate(ctx, update); err != nil {
		return proposal{}, err
	}
```

In `server/membership.go:54-55`, the tracks refusal of `RegisterNode`, replace

```go
		return nil, status.Error(codes.FailedPrecondition, "tracks are active; this binary does not support track-aware manifests")
	}
```

with

```go
		return nil, status.Error(codes.FailedPrecondition, "tracks are active; this binary does not support track-aware manifests")
	}
	// After the v21 activation (housegate spec 2026-10-14 §10.3) the network
	// may propose stage-3 commands and switch on stage-3 rules that a stage-2
	// binary cannot follow; refusing it here costs no log entry per retry.
	// Read from applied state like the refusals above, under membershipMu, so
	// a registration issued while the stage-3 gate runs is checked after the
	// activation commits.
	if isDataPlaneRegistration(req) && svc.s.d.FSM.Stage3Active() && !slices.Contains(req.GetFeatures(), arbiter.Stage3Feature) {
		return nil, status.Error(codes.FailedPrecondition, "stage3 is active; this binary does not support stage-3 consensus")
	}
```

In `server/table_registry.go:114-115`, the `TableRegistrySnapshot` literal of `tableRegistryToPB`, replace

```go
		QuarantinedIndexers: slices.Clone(v.QuarantinedIndexers),
	}
```

with

```go
		QuarantinedIndexers: slices.Clone(v.QuarantinedIndexers),
		// The committed stage-3 parameter, absent before the v21
		// activation (plan S3-E2 contract §45).
		Stage3: wire.Stage3ParamsToPB(v.Stage3),
	}
```

- [ ] **Step 11: Drop the pin-fallout entry this task serves**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; grep -n '"TableRegistrySnapshot.stage3"' "$W"/server/*_test.go`
Expected: no output: at `526a721` no mirror or every-field test reads `TableRegistrySnapshot` (`TestTableRegistryMirrorsProto` compares `TableIncarnation` and the cursor only), so Task 64's pin added no entry for it and there is nothing to do. If the grep instead prints the one line `"TableRegistrySnapshot.stage3": "Task 65",` of `stage3PendingProtoFields` (a test Plan S3-C2 or S3-D added made Task 64 add it), delete exactly that line (contract §44: the owning task deletes its entries) and run the grep again, which then prints nothing.

- [ ] **Step 12: Run them and see them pass**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test -z "$(gofmt -l server fsm)" && go vet ./server/ ./fsm/ && go test ./server/ ./fsm/ -run 'TestStage3Gate|TestTracksGateRefusalsAreUnchanged|TestRegisterNodeRefusesABinaryWithoutStage3V1AfterTheActivation|TestConsensusMutableParamsCarryStage3|TestUpdateConsensusParamsTakesTheMembershipBarrierForTheStage3Activation|TestGetConsensusParamsServesTheStage3StateView|TestTableRegistryServesTheCommittedStage3AndBumpsTheVersion|TestTableRegistryMirrorsProto|TestStage3StateViewIsADetachedCopy|TestRegistryVersionBumpsOnceForEveryCommittedStage3Change|TestStage3ParamsChangedTreatsAnEmptyFailureDomainListAsAbsent' -race -count=1 -v`
Expected: gofmt and vet are silent; the twelve server tests and the three fsm tests PASS (`TestStage3GateProbesEveryVoter` and `TestTracksGateRefusalsAreUnchanged` with their eight subtests each), `TestTableRegistryMirrorsProto` still passes, and both packages print `ok  	github.com/sentioxyz/arbiter/server` and `ok  	github.com/sentioxyz/arbiter/fsm`.

- [ ] **Step 13: Run the server, fsm and arbiter-admin suites**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && bazel run //:gazelle && git status --short && go test ./server/ ./fsm/ ./cmd/arbiter-admin/ -race -count=1`
Expected: gazelle adds `"stage3_reads.go",` to `go_library(name = "fsm")` and `"stage3_reads_test.go",` to `go_test(name = "fsm_test")` in `fsm/BUILD.bazel` (each after Task 64's `"stage3.go",` and `"stage3_params_test.go",`), `"stage3_gate.go",` to `go_library(name = "server")` and `"stage3_fixture_test.go",`, `"stage3_gate_test.go",`, `"stage3_params_test.go",` to `go_test(name = "server_test")` in `server/BUILD.bazel`, and no dependency; `git status` lists ` M fsm/BUILD.bazel`, ` M fsm/consensus_reads.go`, ` M fsm/consensus_updates.go`, ` M fsm/table_registry.go`, ` M server/BUILD.bazel`, ` M server/consensus_admin.go`, ` M server/membership.go`, ` M server/table_registry.go`, ` M server/tracks_fixture_test.go`, ` M server/tracks_gate.go`, `?? fsm/stage3_reads.go`, `?? fsm/stage3_reads_test.go`, `?? server/stage3_fixture_test.go`, `?? server/stage3_gate.go`, `?? server/stage3_gate_test.go` and `?? server/stage3_params_test.go` only (and ` M server/safestate_stage3_test.go` if Step 11 deleted its line); the three packages print `ok`, Task 64's stage-3 FSM tests and the frozen golden tests included.

- [ ] **Step 14: Commit the leader side**

```bash
git -C "$W" add server fsm
git -C "$W" commit -m "feat(stage3): the leader's stage-3 gate, the registration refusal and the stage-3 reads (Task 65)

The tracks gate's voter probe becomes probeFeatureVoters, which the new
stage3 gate shares with its own key arbiter-stage3-voters, feature
stage3_v1 and wording; every tracks refusal text is unchanged. The
update that first sets stage3 is refused before proposing unless the
raft configuration has no non-voter, FSM.Stage3ActivationRefusal is
empty, every node of FSM.Stage3ActivationNodes (the SNode of a
quarantined track exempt) advertised stage3_v1 to this leader in this
term and every voter the request names reports it; the activation takes
membershipMu and the read barrier. After the activation RegisterNode
refuses a data-plane binary without stage3_v1. consensusMutableParams
renders stage3, GetConsensusParams serves stage3_state (without the
purge cutover's incarnation seq, decision D33), the registry snapshot
serves stage3, and every Apply that commits a changed stage3 bumps the
registry version once (housegate spec 2026-10-14 §10.3, §10.4; plan
S3-E2 contract §45).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 15: Write the failing arbiter-admin tests**

Create `cmd/arbiter-admin/stage3_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core"

	"github.com/sentioxyz/arbiter/server"
)

// stageTwoFeatures is what a stage-2 data-plane binary advertises: every
// feature up to track_manifests_v1, no stage3_v1.
var stageTwoFeatures = []string{arbiter.ClientLanesFeature, arbiter.SignedClaimsFeature, arbiter.TrackManifestsFeature}

// stage3Admin is tracksAdmin that also records the arbiter-stage3-voters
// metadata each UpdateConsensusParams request carried and counts
// GetConsensusParams reads; paramsHook, when set, answers a read (counted
// from 1) instead of state.
type stage3Admin struct {
	*tracksAdmin
	stage3VoterMetadata [][]string
	paramsReads         atomic.Int32
	paramsHook          func(read int32) *pb.ConsensusParamsState
}

func (f *stage3Admin) GetConsensusParams(context.Context, *emptypb.Empty) (*pb.ConsensusParamsState, error) {
	n := f.paramsReads.Add(1)
	if f.paramsHook != nil {
		return f.paramsHook(n), nil
	}
	return f.state, nil
}

func (f *stage3Admin) UpdateConsensusParams(ctx context.Context, req *pb.UpdateConsensusParamsCmd) (*pb.Ack, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.mu.Lock()
	f.stage3VoterMetadata = append(f.stage3VoterMetadata, md.Get(server.Stage3VotersMetadataKey))
	f.mu.Unlock()
	return f.tracksAdmin.UpdateConsensusParams(ctx, req)
}

// stage3GateFixture is tracksGateFixture's devnet2 after the tracks
// activation (epoch 4, so tracksUpdateArgs restates it): indexers 0
// (snode-1) and 1 (snode-2), verifiers verifier-1..3, max_writers 2,
// tracks_enabled and no stage3; three voters and a feature book reporting
// every local feature, stage3_v1 included; a track state with both indexers
// unquarantined. The leader is served at its own address; voterAddrs[0]
// answers as arb-0 from the same fake.
func stage3GateFixture(t *testing.T) (leader *stage3Admin, leaderAddr string, voterAddrs []string) {
	t.Helper()
	tracks, _, voters := tracksGateFixture(t)
	tracks.state.Current.TracksEnabled = true
	tracks.view = &pb.TrackStateView{TracksEnabled: true, Indexers: []*pb.IndexerTrackView{{IndexerId: 0}, {IndexerId: 1}}}
	leader = &stage3Admin{tracksAdmin: tracks}
	return leader, serveAdminImpl(t, leader), voters
}

// stage3ActivationArgs is the v21 activation with devnet2's values (plan
// S3-E4 §57), a failure domain for both indexers and voters' --voter flags.
func stage3ActivationArgs(addr string, voters []string, extra ...string) []string {
	args := []string{"--stage3", "--indexer-failure-domain", "id=0,domain=node:n0", "--indexer-failure-domain", "id=1,domain=node:n1",
		"--stage3-max-lag-publications", "64", "--stage3-lag-low", "16", "--stage3-evidence-timeout-l2-blocks", "300",
		"--stage3-max-uncleaned-parts-per-partition", "2000", "--stage3-max-uncleaned-parts-per-table", "20000"}
	args = append(args, voterArgs(voters)...)
	return tracksUpdateArgs(addr, append(args, extra...)...)
}

// activatedStage3 is what stage3ActivationArgs commits: the defaults the
// flags leave in place and every switch off.
func activatedStage3() *pb.Stage3Params {
	return &pb.Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1,
		IndexerFailureDomains: []*pb.IndexerFailureDomain{{IndexerId: 0, FailureDomain: "node:n0"}, {IndexerId: 1, FailureDomain: "node:n1"}},
		MaxLagPublications:    64, LagLow: 16, EvidenceTimeoutL2Blocks: 300,
		MaxUncleanedPartsPerPartition: 2000, MaxUncleanedPartsPerTable: 20000}
}

// stage3WithSwitches is activatedStage3 with the first n switches on.
func stage3WithSwitches(n int) *pb.Stage3Params {
	p := activatedStage3()
	on := []*bool{&p.Purge, &p.EvictionVoiding, &p.OwnerAwareSelection, &p.Placement, &p.Reselection, &p.Checkpoints, &p.NarrowedDrain}
	for _, b := range on[:n] {
		*b = true
	}
	return p
}

// writeConsent writes a consent JWS file for --verifier consent=.
func writeConsent(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "consent.jws")
	if err := os.WriteFile(path, []byte("h.p.s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// memberFlag is a host-bound --verifier value for member-b on indexer 1.
func memberFlag(consent string, extra ...string) string {
	return strings.Join(append([]string{"node=member-b", "pubkey=" + strings.Repeat("0b", 32), "host=1",
		"replica-signer=0x00000000000000000000000000000000000000B2", "consent=" + consent, "domain=node:n1"}, extra...), ",")
}

// lastUpdate is the newest update the leader recorded.
func lastUpdate(t *testing.T, leader *stage3Admin) *pb.ConsensusParamsUpdate {
	t.Helper()
	leader.mu.Lock()
	defer leader.mu.Unlock()
	if len(leader.updates) == 0 {
		t.Fatal("nothing was submitted")
	}
	return leader.updates[len(leader.updates)-1].GetUpdate()
}

// TestConsensusUpdateStage3SetsEveryParameterWithEverySwitchOff: plan S3-E2
// contract §45. --stage3 with the five required values and the failure
// domains submits stage3 with group_size 3, network_fill_default true,
// min_safe_copies 1, strict_copies false and every switch off, carries every
// committed value, runs the stage3 gate (gate, pre-submit re-check and
// post-commit re-check read the book; the track state is read once for the
// exemptions; consensus parameters are read for show and once more after the
// gate) and names the sorted --voter addresses in arbiter-stage3-voters only.
func TestConsensusUpdateStage3SetsEveryParameterWithEverySwitchOff(t *testing.T) {
	leader, addr, voters := stage3GateFixture(t)
	warnings := captureStderr(t)
	given := slices.SortedFunc(slices.Values(voters), func(a, b string) int { return strings.Compare(b, a) })
	if err := run(context.Background(), stage3ActivationArgs(addr, given), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	update := lastUpdate(t, leader)
	if !proto.Equal(update.GetStage3(), activatedStage3()) {
		t.Fatalf("submitted stage3 = %v\nwant %v", update.GetStage3(), activatedStage3())
	}
	if !update.GetTracksEnabled() || len(update.GetSiIndexers()) != 2 || len(update.GetVerifiers()) != 3 ||
		update.GetClientLanes().GetMaxLanesPerAccount() != 256 || update.GetMaxWriters() != 2 {
		t.Fatalf("submitted update = %v", update)
	}
	want := strings.Join(slices.Sorted(slices.Values(voters)), ",")
	if len(leader.stage3VoterMetadata) != 1 || !slices.Equal(leader.stage3VoterMetadata[0], []string{want}) || len(leader.voterMetadata[0]) != 0 {
		t.Fatalf("arbiter-stage3-voters = %q, arbiter-tracks-voters = %q; want exactly %q and nothing", leader.stage3VoterMetadata, leader.voterMetadata, want)
	}
	if leader.bookReads.Load() != 3 || leader.trackReads.Load() != 1 || leader.paramsReads.Load() != 2 {
		t.Fatalf("book reads %d (want 3), track-state reads %d (want 1), parameter reads %d (want 2)",
			leader.bookReads.Load(), leader.trackReads.Load(), leader.paramsReads.Load())
	}
	if warnings.Len() != 0 {
		t.Fatalf("an unchanged cluster produced a warning: %s", warnings)
	}
}

// TestConsensusUpdateStage3KeepsTheCommittedValuesWhenFlagsAreOmitted: after
// the activation an update carries the committed stage3; a given --stage3-*
// value or --indexer-failure-domain list replaces only itself, and no gate
// runs.
func TestConsensusUpdateStage3KeepsTheCommittedValuesWhenFlagsAreOmitted(t *testing.T) {
	leader, addr, _ := stage3GateFixture(t)
	committed := stage3WithSwitches(1)
	committed.ExcludeQuarantinedHosts = true
	leader.state.Current.Stage3 = committed
	lagLow := proto.Clone(committed).(*pb.Stage3Params)
	lagLow.LagLow, lagLow.NetworkFillDefault = 20, false
	domains := proto.Clone(committed).(*pb.Stage3Params)
	domains.IndexerFailureDomains = []*pb.IndexerFailureDomain{{IndexerId: 0, FailureDomain: "node:n8"}, {IndexerId: 1, FailureDomain: "node:n9"}}
	for _, tc := range []struct {
		extra []string
		want  *pb.Stage3Params
	}{
		{nil, committed},
		{[]string{"--stage3-lag-low", "20", "--stage3-network-fill-default=false"}, lagLow},
		{[]string{"--indexer-failure-domain", "id=1,domain=node:n9", "--indexer-failure-domain", "id=0,domain=node:n8"}, domains},
	} {
		if err := run(context.Background(), tracksUpdateArgs(addr, tc.extra...), &bytes.Buffer{}); err != nil {
			t.Fatalf("%v: %v", tc.extra, err)
		}
		if got := lastUpdate(t, leader).GetStage3(); !proto.Equal(got, tc.want) {
			t.Fatalf("%v: submitted stage3 = %v\nwant %v", tc.extra, got, tc.want)
		}
	}
	if leader.bookReads.Load() != 0 || leader.trackReads.Load() != 0 {
		t.Fatal("an update after the activation ran the stage3 gate")
	}
	for i, values := range leader.stage3VoterMetadata {
		if len(values) != 0 {
			t.Fatalf("update %d carried arbiter-stage3-voters %q", i, values)
		}
	}
}

// TestConsensusUpdateCarriesTheCommittedStage3AndHostBoundVerifiers: an
// update without --verifier and --stage3-* flags carries the committed stage3
// and every field of every committed verifier entry, host-bound ones
// included, and a pool entry keeps only its node id and key.
func TestConsensusUpdateCarriesTheCommittedStage3AndHostBoundVerifiers(t *testing.T) {
	leader, addr, _ := stage3GateFixture(t)
	committed := stage3WithSwitches(3)
	host := uint64(1)
	member := &pb.VerifierEntry{NodeId: "member-b", Ed25519Pubkey: bytes.Repeat([]byte{0x0b}, 32), HostIndexerId: &host,
		ReplicaSigner: "0x00000000000000000000000000000000000000b2", ConsentJws: "h.p.s", Weight: 1, FailureDomain: "node:n1",
		Verify: true, Replicate: true, Counts: true, SharedRmt: true}
	leader.state.Current.Stage3 = committed
	leader.state.Current.Verifiers = append(leader.state.Current.Verifiers, member)
	if err := run(context.Background(), tracksUpdateArgs(addr), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	update := lastUpdate(t, leader)
	if !proto.Equal(update.GetStage3(), committed) {
		t.Fatalf("submitted stage3 = %v, want %v", update.GetStage3(), committed)
	}
	if len(update.GetVerifiers()) != 4 {
		t.Fatalf("submitted verifiers = %v", update.GetVerifiers())
	}
	for _, v := range update.GetVerifiers() {
		if v.GetNodeId() == "member-b" {
			if !proto.Equal(v, member) {
				t.Fatalf("carried member entry = %v, want %v", v, member)
			}
			continue
		}
		if !proto.Equal(v, &pb.VerifierEntry{NodeId: v.GetNodeId(), Ed25519Pubkey: v.GetEd25519Pubkey()}) {
			t.Fatalf("pool entry %s gained host-bound fields: %v", v.GetNodeId(), v)
		}
	}
}

// TestConsensusUpdateStage3SwitchTurnsOnOneSwitch: --stage3-switch NAME turns
// on that switch over the committed stage3 and changes nothing else; it runs
// no gate and names no voter.
func TestConsensusUpdateStage3SwitchTurnsOnOneSwitch(t *testing.T) {
	for i, name := range arbiter.Stage3Switches {
		t.Run(name, func(t *testing.T) {
			leader, addr, _ := stage3GateFixture(t)
			leader.state.Current.Stage3 = stage3WithSwitches(i)
			if err := run(context.Background(), tracksUpdateArgs(addr, "--stage3-switch", name), &bytes.Buffer{}); err != nil {
				t.Fatal(err)
			}
			if got := lastUpdate(t, leader).GetStage3(); !proto.Equal(got, stage3WithSwitches(i+1)) {
				t.Fatalf("submitted stage3 = %v, want %v", got, stage3WithSwitches(i+1))
			}
			if leader.bookReads.Load() != 0 || leader.trackReads.Load() != 0 || len(leader.stage3VoterMetadata[0]) != 0 {
				t.Fatal("a switch-on ran the stage3 gate or named voters")
			}
		})
	}
}

// TestConsensusUpdateStage3FlagRefusals: every refusal of plan S3-E2
// contract §45 (and the tracks prerequisite) is local: nothing is read from
// the feature book and nothing is submitted. The last two cases are the
// gate's own reads: the leader's track state and the not-active re-read.
func TestConsensusUpdateStage3FlagRefusals(t *testing.T) {
	poolWithWeight := "node=member-b,pubkey=" + strings.Repeat("0b", 32) + ",weight=1"
	for _, tc := range []struct {
		name      string
		committed *pb.Stage3Params
		noTracks  bool
		setup     func(leader *stage3Admin)
		args      func(addr string, voters []string, consent string) []string
		want      string
		bookReads int32
	}{
		{name: "the activation once stage3 is committed", committed: activatedStage3(),
			args: func(a string, v []string, _ string) []string { return stage3ActivationArgs(a, v) },
			want: "--stage3 sets stage3 for the first time, but the leader has it committed; omit --stage3"},
		{name: "the activation without its required values",
			args: func(a string, v []string, _ string) []string {
				return tracksUpdateArgs(a, append([]string{"--stage3", "--stage3-lag-low", "16"}, voterArgs(v)...)...)
			},
			want: "--stage3 needs --stage3-max-lag-publications, --stage3-lag-low, --stage3-evidence-timeout-l2-blocks, --stage3-max-uncleaned-parts-per-partition and --stage3-max-uncleaned-parts-per-table"},
		{name: "the activation without voters",
			args: func(a string, _ []string, _ string) []string { return stage3ActivationArgs(a, nil) },
			want: "--stage3 needs --voter: the leader probes every voter for stage3_v1"},
		{name: "the activation before the tracks activation", noTracks: true,
			args: func(a string, v []string, _ string) []string { return stage3ActivationArgs(a, v) },
			want: "--stage3 needs the tracks activation first: tracks_enabled is not committed, and the stage-3 activation must be a later update"},
		{name: "a value before the activation",
			args: func(a string, _ []string, _ string) []string { return tracksUpdateArgs(a, "--stage3-lag-low", "3") },
			want: "--stage3-* and --indexer-failure-domain need stage3: pass --stage3 for the activation"},
		{name: "a failure domain before the activation",
			args: func(a string, _ []string, _ string) []string {
				return tracksUpdateArgs(a, "--indexer-failure-domain", "id=0,domain=node:n0")
			},
			want: "--stage3-* and --indexer-failure-domain need stage3: pass --stage3 for the activation"},
		{name: "a switch before the activation",
			args: func(a string, _ []string, _ string) []string { return tracksUpdateArgs(a, "--stage3-switch", "purge") },
			want: "--stage3-* and --indexer-failure-domain need stage3: pass --stage3 for the activation"},
		{name: "a switch in the activation",
			args: func(a string, v []string, _ string) []string {
				return stage3ActivationArgs(a, v, "--stage3-switch", "purge")
			},
			want: "--stage3-switch cannot be combined with --stage3: the activation leaves every switch off"},
		{name: "an unknown switch", committed: activatedStage3(),
			args: func(a string, _ []string, _ string) []string {
				return tracksUpdateArgs(a, "--stage3-switch", "fast_path")
			},
			want: `--stage3-switch "fast_path" is not a switch (want purge, eviction_voiding, owner_aware_selection, placement, reselection, checkpoints, narrowed_drain)`},
		{name: "two switches", committed: activatedStage3(),
			args: func(a string, _ []string, _ string) []string {
				return tracksUpdateArgs(a, "--stage3-switch", "purge", "--stage3-switch", "eviction_voiding")
			},
			want: "--stage3-switch is given more than once; an update turns on one switch"},
		{name: "a switch that is on already", committed: stage3WithSwitches(1),
			args: func(a string, _ []string, _ string) []string { return tracksUpdateArgs(a, "--stage3-switch", "purge") },
			want: "--stage3-switch purge is on already"},
		{name: "a failure domain given twice", committed: activatedStage3(),
			args: func(a string, _ []string, _ string) []string {
				return tracksUpdateArgs(a, "--indexer-failure-domain", "id=0,domain=node:a", "--indexer-failure-domain", "id=0,domain=node:b")
			},
			want: "--indexer-failure-domain id 0 is given twice"},
		{name: "a host-bound key without host", committed: stage3WithSwitches(3),
			args: func(a string, _ []string, _ string) []string {
				return tracksUpdateArgs(a, "--verifier", poolWithWeight)
			},
			want: "--verifier member-b: host-bound keys (replica-signer, consent, domain, weight, verify, replicate, counts, shared-rmt) need host=N"},
		{name: "a weight above 64", committed: stage3WithSwitches(3),
			args: func(a string, _ []string, consent string) []string {
				return tracksUpdateArgs(a, "--verifier", memberFlag(consent, "weight=65"))
			},
			want: `--verifier member-b: weight "65" must be 1 to 64`},
		{name: "the leader's track state is unreadable",
			setup: func(leader *stage3Admin) {
				leader.viewErr = status.Error(codes.Unimplemented, "unknown method GetTrackState")
			},
			args: func(a string, v []string, _ string) []string { return stage3ActivationArgs(a, v) },
			want: "stage3 gate refused: read the leader's track state"},
		{name: "the leader reports stage3 after the gate ran",
			setup: func(leader *stage3Admin) {
				shown := proto.Clone(leader.state).(*pb.ConsensusParamsState)
				later := proto.Clone(leader.state).(*pb.ConsensusParamsState)
				later.Current.Stage3, later.Stage3State = activatedStage3(), &pb.Stage3StateView{ActivationEpoch: 5}
				leader.paramsHook = func(read int32) *pb.ConsensusParamsState {
					if read == 1 {
						return shown
					}
					return later
				}
			},
			args:      func(a string, v []string, _ string) []string { return stage3ActivationArgs(a, v) },
			want:      "stage3 gate refused: the leader already reports stage3 committed; re-run consensus show before retrying",
			bookReads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader, addr, voters := stage3GateFixture(t)
			leader.state.Current.Stage3 = tc.committed
			leader.state.Current.TracksEnabled = !tc.noTracks
			if tc.setup != nil {
				tc.setup(leader)
			}
			refused(t, leader.fakeAdmin, tc.args(addr, voters, writeConsent(t)), tc.want)
			if got := leader.bookReads.Load(); got != tc.bookReads {
				t.Fatalf("the refusal read the feature book %d times, want %d", got, tc.bookReads)
			}
		})
	}
}

// TestVerifierFlagParsesHostBoundEntries: a pool entry stays node and key; a
// host-bound entry carries its host (indexer 0 kept), the lowercased replica
// signer, the consent file's JWS, its failure domain, weight 1 and every
// flag true unless given; an update submits every field.
func TestVerifierFlagParsesHostBoundEntries(t *testing.T) {
	consent := writeConsent(t)
	var v verifierFlags
	for _, value := range []string{
		"node=verifier-1,pubkey=" + strings.Repeat("01", 32),
		memberFlag(consent),
		"node=member-c,pubkey=" + strings.Repeat("0c", 32) + ",host=0,replica-signer=0x00000000000000000000000000000000000000c3,consent=" + consent +
			",domain=node:n0,weight=7,verify=false,replicate=true,counts=false,shared-rmt=false",
	} {
		if err := v.Set(value); err != nil {
			t.Fatalf("%s: %v", value, err)
		}
	}
	host0, host1 := uint64(0), uint64(1)
	want := []arbiter.VerifierEntry{
		{NodeID: "verifier-1", Ed25519Pubkey: bytes.Repeat([]byte{1}, 32)},
		{NodeID: "member-b", Ed25519Pubkey: bytes.Repeat([]byte{0x0b}, 32), HostIndexerID: &host1,
			ReplicaSigner: "0x00000000000000000000000000000000000000b2", ConsentJWS: "h.p.s", Weight: 1, FailureDomain: "node:n1",
			Verify: true, Replicate: true, Counts: true, SharedRMT: true},
		{NodeID: "member-c", Ed25519Pubkey: bytes.Repeat([]byte{0x0c}, 32), HostIndexerID: &host0,
			ReplicaSigner: "0x00000000000000000000000000000000000000c3", ConsentJWS: "h.p.s", Weight: 7, FailureDomain: "node:n0",
			Replicate: true},
	}
	if !reflect.DeepEqual([]arbiter.VerifierEntry(v), want) {
		t.Fatalf("parsed %+v\nwant   %+v", []arbiter.VerifierEntry(v), want)
	}
	leader, addr, _ := stage3GateFixture(t)
	leader.state.Current.Stage3 = stage3WithSwitches(3)
	args := tracksUpdateArgs(addr, "--verifier", "node=verifier-1,pubkey="+strings.Repeat("01", 32),
		"--verifier", "node=verifier-2,pubkey="+strings.Repeat("02", 32), "--verifier", "node=verifier-3,pubkey="+strings.Repeat("03", 32),
		"--verifier", memberFlag(consent))
	if err := run(context.Background(), args, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	host := uint64(1)
	member := &pb.VerifierEntry{NodeId: "member-b", Ed25519Pubkey: bytes.Repeat([]byte{0x0b}, 32), HostIndexerId: &host,
		ReplicaSigner: "0x00000000000000000000000000000000000000b2", ConsentJws: "h.p.s", Weight: 1, FailureDomain: "node:n1",
		Verify: true, Replicate: true, Counts: true, SharedRmt: true}
	if got := lastUpdate(t, leader).GetVerifiers(); len(got) != 4 || !proto.Equal(got[0], member) {
		t.Fatalf("submitted verifiers = %v, want member-b first with every field", got)
	}
}

// TestVerifierFlagRefusesHostBoundKeysWithoutHost: every host-bound key needs
// host=N, host=N needs its three keys, and each value is checked locally.
func TestVerifierFlagRefusesHostBoundKeysWithoutHost(t *testing.T) {
	consent := writeConsent(t)
	pool := "node=v,pubkey=" + strings.Repeat("0a", 32)
	host := pool + ",host=1,replica-signer=0x00000000000000000000000000000000000000b2,consent=" + consent + ",domain=node:n1"
	needHost := "--verifier v: host-bound keys (replica-signer, consent, domain, weight, verify, replicate, counts, shared-rmt) need host=N"
	needKeys := "--verifier v: host=N needs replica-signer=0x..., consent=PATH and domain=D"
	for _, tc := range []struct{ value, want string }{
		{pool + ",weight=1", needHost},
		{pool + ",verify=true", needHost},
		{pool + ",consent=" + consent, needHost},
		{pool + ",shared-rmt=false", needHost},
		{pool + ",host=1,consent=" + consent + ",domain=node:n1", needKeys},
		{pool + ",host=1,replica-signer=0x00000000000000000000000000000000000000b2,domain=node:n1", needKeys},
		{pool + ",host=1,replica-signer=0x00000000000000000000000000000000000000b2,consent=" + consent, needKeys},
		{strings.Replace(host, "host=1", "host=one", 1), `--verifier v: host "one" is not an unsigned integer`},
		{host + ",weight=0", `--verifier v: weight "0" must be 1 to 64`},
		{host + ",weight=65", `--verifier v: weight "65" must be 1 to 64`},
		{host + ",weight=two", `--verifier v: weight "two" must be 1 to 64`},
		{host + ",verify=yes", `--verifier v: verify="yes" must be true or false`},
		{host + ",shared-rmt=1", `--verifier v: shared-rmt="1" must be true or false`},
		{strings.Replace(host, consent, filepath.Join(t.TempDir(), "missing.jws"), 1), "--verifier v consent: "},
		{pool + ",colour=red", `--verifier: unknown key "colour" (want node, pubkey, host, replica-signer, consent, domain, weight, verify, replicate, counts, shared-rmt)`},
		{pool + ",host=", `--verifier: key "host" has no value`},
		{"pubkey=" + strings.Repeat("0a", 32), "--verifier: missing node="},
	} {
		err := (&verifierFlags{}).Set(tc.value)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: err = %v, want %q", tc.value, err, tc.want)
		}
	}
}

// TestIndexerFailureDomainFlag: each --indexer-failure-domain adds one
// entry, in command-line order (the normalizer sorts the list); an id given
// twice, a malformed id and a missing or unknown key are refused.
func TestIndexerFailureDomainFlag(t *testing.T) {
	var f failureDomainFlags
	for _, value := range []string{"id=1,domain=node:n1", "id=0,domain=node:n0"} {
		if err := f.Set(value); err != nil {
			t.Fatalf("%s: %v", value, err)
		}
	}
	if want := (failureDomainFlags{{IndexerID: 1, FailureDomain: "node:n1"}, {IndexerID: 0, FailureDomain: "node:n0"}}); !reflect.DeepEqual(f, want) {
		t.Fatalf("parsed %+v, want %+v", f, want)
	}
	for _, tc := range []struct{ value, want string }{
		{"id=1,domain=node:n9", "--indexer-failure-domain id 1 is given twice"},
		{"id=x,domain=node:n2", `--indexer-failure-domain id "x" is not an unsigned integer`},
		{"id=2", "--indexer-failure-domain: missing domain="},
		{"id=2,domain=d,zone=west", `--indexer-failure-domain: unknown key "zone" (want id, domain)`},
	} {
		if err := f.Set(tc.value); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: err = %v, want %q", tc.value, err, tc.want)
		}
	}
	if len(f) != 2 {
		t.Fatalf("a refused value was kept: %+v", f)
	}
}

// TestStage3ActivationGateExemptsSNodesOfQuarantinedTracks: housegate spec
// 2026-10-14 §10.3. The CLI's gate requires stage3_v1 from every data-plane
// node of the feature book except the SNode of a quarantined track, which
// the observation lists as exempt; a voter is never exempt.
func TestStage3ActivationGateExemptsSNodesOfQuarantinedTracks(t *testing.T) {
	leader, addr, voters := stage3GateFixture(t)
	leader.book.Entries[1].Features = stageTwoFeatures // snode-2 still on a stage-2 release
	refused(t, leader.fakeAdmin, stage3ActivationArgs(addr, voters), "stage3 gate refused", "restart: "+authoritytest.SNodeNodeID1, "stage3-aware")
	leader.view.Indexers[1].Quarantined = true
	if got := stage3ExemptSNodes(leader.state, leader.view); !slices.Equal(got, []string{authoritytest.SNodeNodeID1}) {
		t.Fatalf("stage3ExemptSNodes = %v, want [%s]", got, authoritytest.SNodeNodeID1)
	}
	if err := run(context.Background(), stage3ActivationArgs(addr, voters), &bytes.Buffer{}); err != nil {
		t.Fatalf("activation with the quarantined track's SNode on a stage-2 release = %v", err)
	}
	if !proto.Equal(lastUpdate(t, leader).GetStage3(), activatedStage3()) {
		t.Fatalf("submitted stage3 = %v", lastUpdate(t, leader).GetStage3())
	}

	g := stage3GateFor([]string{authoritytest.SNodeNodeID1})
	observed := &gateObservation{configured: []string{"arb-0"},
		probes:  []voterProbe{{address: "arb-0:7100", nodeID: "arb-0", protocol: arbiter.ConsensusAdminProtocolVersion, features: stageTwoFeatures}},
		entries: []*pb.NodeFeatureEntry{{NodeId: authoritytest.SNodeNodeID1, Features: stageTwoFeatures, RegisteredUnix: 1}},
		exempt:  []string{authoritytest.SNodeNodeID1}}
	problems, restart := observed.problems(g)
	if len(problems) != 1 || !strings.Contains(problems[0], "voter arb-0 (arb-0:7100) lacks stage3_v1") || !slices.Equal(restart, []string{"arb-0"}) {
		t.Fatalf("problems %q, restart %v; want only the voter", problems, restart)
	}
	if !slices.Contains(observed.lines(), "exempt (quarantined track): "+authoritytest.SNodeNodeID1) {
		t.Fatalf("lines %q do not list the exemption", observed.lines())
	}
}

// TestStage3VotersMetadataKeyMatchesTheServer: the CLI's key is a literal,
// like the tracks key; this pins the two together.
func TestStage3VotersMetadataKeyMatchesTheServer(t *testing.T) {
	if stage3VotersMetadataKey != server.Stage3VotersMetadataKey {
		t.Fatalf("CLI sends %q, the leader reads %q", stage3VotersMetadataKey, server.Stage3VotersMetadataKey)
	}
}

// TestConsensusShowJSONHasTheStage3FieldsTheRunbooksRead: the stage-3
// runbooks read consensus show with jq (plan S3-F contract): current.stage3
// with proto names and false switches printed, and stage3_state with each
// cutover's switch_name and cutover_block_seq as a decimal string; both are
// null before the activation.
func TestConsensusShowJSONHasTheStage3FieldsTheRunbooksRead(t *testing.T) {
	t.Setenv(authorityPrivateKeyEnv, "")
	state := testConsensusState()
	addr := serveAdminImpl(t, &fakeAdmin{nodeID: "arb-0", state: state})
	show := func() map[string]json.RawMessage {
		t.Helper()
		var out bytes.Buffer
		if err := run(context.Background(), []string{"consensus", "show", "--address", addr}, &out); err != nil {
			t.Fatal(err)
		}
		var doc map[string]json.RawMessage
		if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
			t.Fatalf("%v\n%s", err, out.String())
		}
		return doc
	}
	before := show()
	var current map[string]json.RawMessage
	if err := json.Unmarshal(before["current"], &current); err != nil {
		t.Fatal(err)
	}
	if string(before["stage3_state"]) != "null" || string(current["stage3"]) != "null" {
		t.Fatalf("before the activation: stage3_state %s, current.stage3 %s", before["stage3_state"], current["stage3"])
	}
	state.Current.Stage3 = stage3WithSwitches(4)
	state.Stage3State = &pb.Stage3StateView{ActivationEpoch: 5, ActivationBlockSeq: 70, ActivationPromotionSeq: 40,
		Switches: []*pb.Stage3SwitchCutover{{SwitchName: "purge", Epoch: 6, CutoverBlockSeq: 71, CutoverPromotionSeq: 41},
			{SwitchName: "eviction_voiding", Epoch: 7, CutoverBlockSeq: 72, CutoverPromotionSeq: 42},
			{SwitchName: "owner_aware_selection", Epoch: 8, CutoverBlockSeq: 73, CutoverPromotionSeq: 43},
			{SwitchName: "placement", Epoch: 9, CutoverBlockSeq: 75, CutoverPromotionSeq: 44}}}
	var doc struct {
		Current struct {
			Stage3 struct {
				GroupSize     int64 `json:"group_size"`
				MinSafeCopies int64 `json:"min_safe_copies"`
				Placement     bool  `json:"placement"`
				Reselection   *bool `json:"reselection"`
				Domains       []struct {
					IndexerID     string `json:"indexer_id"`
					FailureDomain string `json:"failure_domain"`
				} `json:"indexer_failure_domains"`
			} `json:"stage3"`
		} `json:"current"`
		Stage3State struct {
			ActivationEpoch string `json:"activation_epoch"`
			Switches        []struct {
				SwitchName      string `json:"switch_name"`
				CutoverBlockSeq string `json:"cutover_block_seq"`
			} `json:"switches"`
		} `json:"stage3_state"`
	}
	raw, err := json.Marshal(show())
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	s3 := doc.Current.Stage3
	if s3.GroupSize != 3 || s3.MinSafeCopies != 1 || !s3.Placement || s3.Reselection == nil || *s3.Reselection || len(s3.Domains) != 2 || s3.Domains[1].IndexerID != "1" {
		t.Fatalf("current.stage3 = %+v from %s", s3, raw)
	}
	if doc.Stage3State.ActivationEpoch != "5" || len(doc.Stage3State.Switches) != 4 || doc.Stage3State.Switches[3].SwitchName != "placement" || doc.Stage3State.Switches[3].CutoverBlockSeq != "75" {
		t.Fatalf("stage3_state = %+v from %s", doc.Stage3State, raw)
	}
}
```

Create `cmd/arbiter-admin/stage3_integration_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"

	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/server"
)

// TestCLIStage3ActivationThroughServerAndFSM drives the v21 activation and a
// switch-on against the real server (feature book, GetProtocolInfo,
// RegisterNode, GetTrackState, the consensus hold, the leader's stage-3 gate
// and its voter probe) and the production FSM, mirroring
// TestCLITracksActivationQuarantineAndReleaseThroughServerAndFSM: a verifier
// still on a stage-2 release refuses the gate with state untouched; once it
// re-registers the activation commits under the default drain hold, as the
// P5 runbook runs it, with every switch off, and consensus show reports
// stage3 and stage3_state; the leader then refuses a stage-2 registration;
// --stage3-switch purge turns on the first switch and records its cutover; a
// later update carries stage3 without any --stage3 flag.
func TestCLIStage3ActivationThroughServerAndFSM(t *testing.T) {
	a := adminKey(t)
	params := fsm.Params{NetworkID: "cli-stage3-network", SchemaSnapshotID: "cli-schema", ExecutorProfileID: "cli-profile",
		AuthorityAddresses: []string{a.Address()}, MaxWriters: 1}
	state, err := fsm.New(params)
	if err != nil {
		t.Fatal(err)
	}
	executor := payloadexec.New(params.NetworkID, payloadexec.TableSchema{TableID: "db.admin", Columns: []lthash.Column{{Name: "value", Type: "UInt64"}}})
	genesis, err := executor.GenesisSnapshot(0, params.SchemaSnapshotID, params.ExecutorProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.ConfigureGenesis(genesis); err != nil {
		t.Fatal(err)
	}
	encodedGenesis, err := wire.Encode(wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: genesis}})
	if err != nil {
		t.Fatal(err)
	}
	if result := state.Apply(&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand, Data: encodedGenesis}); result != (fsm.Applied{}) {
		t.Fatalf("publish genesis: %v", result)
	}
	node := &lanesIntegrationNode{integrationConsensusNode: &integrationConsensusNode{state: state, index: 1}}
	node.leader.Store(true)
	node.setVoters("arb-0")
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	transport := grpc.NewServer()
	svc := server.New(server.Deps{Node: node, FSM: state, LeaderAddr: func() string { return "" }, Cfg: server.Config{
		NodeID: "arb-0", ApplyTimeout: time.Second, ConsensusUpdatesEnabled: true, ConsensusAdminMaxTokenAge: 5 * time.Minute,
	}})
	svc.RegisterAll(transport)
	go func() { _ = transport.Serve(lis) }()
	t.Cleanup(func() {
		svc.CloseStreams()
		transport.Stop()
	})
	address := lis.Addr().String()
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	update := func(epoch, maxWriters string, extra ...string) error {
		t.Helper()
		return run(context.Background(), append([]string{"consensus", "update", "--address", address, "--expected-epoch", epoch,
			"--authority", a.Address(), "--max-writers", maxWriters, "--hold", "none"}, extra...), &bytes.Buffer{})
	}
	show := func() *pb.ConsensusParamsState {
		t.Helper()
		var out bytes.Buffer
		if err := run(context.Background(), []string{"consensus", "show", "--address", address}, &out); err != nil {
			t.Fatalf("consensus show: %v", err)
		}
		var shown pb.ConsensusParamsState
		if err := protojson.Unmarshal(out.Bytes(), &shown); err != nil {
			t.Fatalf("consensus show output: %v\n%s", err, out.String())
		}
		return &shown
	}
	register := func(req *pb.NodeRegistration) error {
		t.Helper()
		_, err := pb.NewMembershipClient(conn).RegisterNode(context.Background(), req)
		return err
	}

	if err := update("0", "1", "--table-registry-chain-id", "7892301", "--table-registry-contract", "0x00000000000000000000000000000000000000d1",
		"--table-registry-si-indexer-id", "1", "--table-registry-activation-block", "100", "--table-registry-confirmation", "safe"); err != nil {
		t.Fatalf("enable the registry: %v", err)
	}
	all := arbiter.LocalNodeFeatures()
	if err := register(&pb.NodeRegistration{NodeId: "s1", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}, Features: all}); err != nil {
		t.Fatalf("register s1: %v", err)
	}
	keys := map[string]ed25519.PrivateKey{}
	for i, id := range []string{"v1", "v2", "v3"} {
		keys[id] = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(i + 1)}, ed25519.SeedSize))
		features := all
		if id == "v3" {
			features = stageTwoFeatures // still on a stage-2 release
		}
		if err := register(&pb.NodeRegistration{NodeId: id, Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER},
			Ed25519Pubkey: keys[id].Public().(ed25519.PublicKey), Features: features}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
	pubkey := func(id string) string { return hex.EncodeToString(keys[id].Public().(ed25519.PublicKey)) }
	view, err := state.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	founder := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
	enrollment := writeEnrollment(t, founder, view.Current.NetworkID, view.GenesisSnapshotID, 1, "s1")
	if err := update("1", "1", "--si-indexer", fmt.Sprintf("id=1,activation=100,signer=%s,node=s1,enrollment=%s", founder.Address(), enrollment),
		"--verifier", "node=v1,pubkey="+pubkey("v1"), "--verifier", "node=v2,pubkey="+pubkey("v2"), "--verifier", "node=v3,pubkey="+pubkey("v3"),
		"--voter", address); err != nil {
		t.Fatalf("signed-claims activation: %v", err)
	}
	if err := update("2", "1", "--tracks-enabled", "--voter", address); err != nil {
		t.Fatalf("tracks activation: %v", err)
	}

	// The activation keeps arbiter-admin's default --hold drain; an idle
	// network drains at once.
	activate := func() (string, error) {
		t.Helper()
		var out bytes.Buffer
		err := run(context.Background(), []string{"consensus", "update", "--address", address, "--expected-epoch", "3",
			"--authority", a.Address(), "--max-writers", "1", "--stage3", "--indexer-failure-domain", "id=1,domain=node:n1",
			"--stage3-max-lag-publications", "64", "--stage3-lag-low", "16", "--stage3-evidence-timeout-l2-blocks", "300",
			"--stage3-max-uncleaned-parts-per-partition", "2000", "--stage3-max-uncleaned-parts-per-table", "20000", "--voter", address}, &out)
		return out.String(), err
	}
	warnings := captureStderr(t)
	applies := node.applies.Load()
	if _, err := activate(); err == nil || !strings.Contains(err.Error(), "restart: v3") || !strings.Contains(err.Error(), "stage3-aware") {
		t.Fatalf("the stage3 gate must name v3: %v", err)
	}
	if node.applies.Load() != applies || state.Stage3Active() {
		t.Fatal("a refused stage3 gate changed committed state")
	}
	// v3 restarts on a stage-3 release and re-registers, signed with its
	// listed key.
	reg := arbiter.NodeRegistration{NodeID: "v3", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier},
		Ed25519Pubkey: keys["v3"].Public().(ed25519.PublicKey), RegistrationSeq: authoritytest.RegistrationSeq}
	sig, err := authority.SignVerifierMessage(keys["v3"], authority.VerifierMessageRegistration,
		authority.MessageContext{NetworkID: view.Current.NetworkID, GenesisSnapshotID: view.GenesisSnapshotID}, reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := register(wire.RegisterNodeToRequest(wire.RegisterNode{Registration: reg, Ed25519Signature: sig}, all)); err != nil {
		t.Fatalf("v3 re-registers on a stage-3 release: %v", err)
	}
	applies = node.applies.Load()
	out, err := activate()
	if err != nil {
		t.Fatalf("stage3 activation: %v", err)
	}
	if !strings.Contains(out, `"hold": "drain"`) || !strings.Contains(warnings.String(), "took a consensus hold of mode drain") || node.applies.Load() != applies+1 {
		t.Fatalf("the activation must commit under a drain hold, alone: %d entries applied\n%s\n%s", node.applies.Load()-applies, out, warnings)
	}
	want := &pb.Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1,
		IndexerFailureDomains: []*pb.IndexerFailureDomain{{IndexerId: 1, FailureDomain: "node:n1"}},
		MaxLagPublications:    64, LagLow: 16, EvidenceTimeoutL2Blocks: 300,
		MaxUncleanedPartsPerPartition: 2000, MaxUncleanedPartsPerTable: 20000}
	shown := show()
	if !state.Stage3Active() || !proto.Equal(shown.GetCurrent().GetStage3(), want) ||
		shown.GetStage3State().GetActivationEpoch() != 4 || len(shown.GetStage3State().GetSwitches()) != 0 {
		t.Fatalf("after the activation: current.stage3 %v, stage3_state %v", shown.GetCurrent().GetStage3(), shown.GetStage3State())
	}

	// A stage-2 data-plane binary is refused before anything is proposed.
	applies = node.applies.Load()
	err = register(&pb.NodeRegistration{NodeId: "v4", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER},
		Ed25519Pubkey: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{4}, ed25519.SeedSize)).Public().(ed25519.PublicKey), Features: stageTwoFeatures})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "stage3 is active; this binary does not support stage-3 consensus") {
		t.Fatalf("a stage-2 registration after the activation = %v", err)
	}
	if node.applies.Load() != applies {
		t.Fatal("the refused registration was proposed")
	}

	if err := update("4", "1", "--stage3-switch", "purge"); err != nil {
		t.Fatalf("--stage3-switch purge: %v", err)
	}
	shown = show()
	switches := shown.GetStage3State().GetSwitches()
	if !shown.GetCurrent().GetStage3().GetPurge() || shown.GetCurrent().GetStage3().GetEvictionVoiding() || len(switches) != 1 ||
		switches[0].GetSwitchName() != "purge" || switches[0].GetEpoch() != 5 {
		t.Fatalf("after the purge switch: current.stage3 %v, stage3_state %v", shown.GetCurrent().GetStage3(), shown.GetStage3State())
	}
	if err := update("5", "2"); err != nil {
		t.Fatalf("an update after the activation must carry stage3: %v", err)
	}
	if shown = show(); shown.GetCurrent().GetMaxWriters() != 2 || !shown.GetCurrent().GetStage3().GetPurge() {
		t.Fatalf("after the carrying update: current %v", shown.GetCurrent())
	}
}
```

- [ ] **Step 16: Run them and see them fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./cmd/arbiter-admin/ -run 'TestConsensusUpdateStage3|TestConsensusUpdateCarriesTheCommittedStage3AndHostBoundVerifiers|TestVerifierFlag|TestIndexerFailureDomainFlag|TestStage3ActivationGateExemptsSNodesOfQuarantinedTracks|TestStage3VotersMetadataKeyMatchesTheServer|TestConsensusShowJSONHasTheStage3FieldsTheRunbooksRead|TestCLIStage3ActivationThroughServerAndFSM' -race -count=1`
Expected: `FAIL	github.com/sentioxyz/arbiter/cmd/arbiter-admin [build failed]` with `undefined: failureDomainFlags`, `undefined: stage3ExemptSNodes`, `undefined: stage3GateFor`, `unknown field exempt in struct literal of type gateObservation` and `undefined: stage3VotersMetadataKey` (all in `stage3_test.go`).

- [ ] **Step 17: Implement the stage-3 flags and the CLI gate**

Create `cmd/arbiter-admin/stage3.go`:

```go
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core"
)

// stage3VotersMetadataKey carries the v21 activation's --voter admin
// addresses to the leader, which probes every voter itself before it
// proposes (plan S3-E2 contract §45); it equals
// server.Stage3VotersMetadataKey (TestStage3VotersMetadataKeyMatchesTheServer).
const stage3VotersMetadataKey = "arbiter-stage3-voters"

// stage3Activation is the v21 activation gate (housegate spec 2026-10-14
// §10.3, plan S3-E2 contract §45): every voter and every registered verifier
// and SNode reports stage3_v1. Like the tracks activation it evicts nobody;
// unlike it, the SNode of a quarantined track need not report the feature,
// which stage3GateFor sets as exempt. The leader's own stage3 gate decides
// again before proposing.
var stage3Activation = activationGate{feature: arbiter.Stage3Feature, name: "stage3", aware: "stage3-aware"}

// stage3GateFor is stage3Activation for one update, exempting the SNodes of
// quarantined tracks (stage3ExemptSNodes).
func stage3GateFor(exempt []string) activationGate {
	g := stage3Activation
	g.exempt = func(nodeID string) bool { return slices.Contains(exempt, nodeID) }
	return g
}

// stage3ExemptSNodes lists, sorted, the SNodes of the si_indexers entries
// whose track the leader reports quarantined: the data-plane nodes the v21
// activation does not require stage3_v1 from (FSM.Stage3ActivationNodes
// leaves the same nodes out).
func stage3ExemptSNodes(params *pb.ConsensusParamsState, tracks *pb.TrackStateView) []string {
	quarantined := map[uint64]bool{}
	for _, ix := range tracks.GetIndexers() {
		if ix.GetQuarantined() {
			quarantined[ix.GetIndexerId()] = true
		}
	}
	var out []string
	for _, e := range params.GetCurrent().GetSiIndexers() {
		if quarantined[e.GetIndexerId()] {
			out = append(out, e.GetSnodeNodeId())
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// checkStage3NotActive is the stage3 gate's last check: after the gate ran,
// the leader's barriered parameters must still carry no stage3. The gate
// runs only when consensus show reported none, so stage3 now means the
// cluster changed under the operator.
func checkStage3NotActive(ctx context.Context, leader pb.ConsensusAdminClient) error {
	state, err := leader.GetConsensusParams(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("stage3 gate refused: read the leader's consensus parameters: %w", err)
	}
	if state.GetCurrent().GetStage3() != nil || state.GetStage3State() != nil {
		return errors.New("stage3 gate refused: the leader already reports stage3 committed; re-run consensus show before retrying")
	}
	return nil
}

// failureDomainFlags collects repeated --indexer-failure-domain id=N,domain=D
// values in command-line order; the normalizer sorts them by indexer id.
type failureDomainFlags []arbiter.IndexerFailureDomain

func (f *failureDomainFlags) String() string { return fmt.Sprint(len(*f)) }
func (f *failureDomainFlags) Set(value string) error {
	fields, err := keyValues(value, "id", "domain")
	if err != nil {
		return fmt.Errorf("--indexer-failure-domain: %w", err)
	}
	id, err := strconv.ParseUint(fields["id"], 10, 64)
	if err != nil {
		return fmt.Errorf("--indexer-failure-domain id %q is not an unsigned integer", fields["id"])
	}
	if slices.ContainsFunc(*f, func(d arbiter.IndexerFailureDomain) bool { return d.IndexerID == id }) {
		return fmt.Errorf("--indexer-failure-domain id %d is given twice", id)
	}
	*f = append(*f, arbiter.IndexerFailureDomain{IndexerID: id, FailureDomain: fields["domain"]})
	return nil
}

// stage3SwitchFlags collects repeated --stage3-switch values; an update turns
// on one switch, which stage3Flags.check enforces after parsing.
type stage3SwitchFlags []string

func (s *stage3SwitchFlags) String() string { return strings.Join(*s, ",") }
func (s *stage3SwitchFlags) Set(value string) error {
	name := strings.TrimSpace(value)
	if !slices.Contains(arbiter.Stage3Switches, name) {
		return fmt.Errorf("--stage3-switch %q is not a switch (want purge, eviction_voiding, owner_aware_selection, placement, reselection, checkpoints, narrowed_drain)", value)
	}
	*s = append(*s, name)
	return nil
}

// uint32Flag is a flag.Value holding an unsigned 32-bit integer.
type uint32Flag uint32

func (u *uint32Flag) String() string { return strconv.FormatUint(uint64(*u), 10) }
func (u *uint32Flag) Set(value string) error {
	v, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
	if err != nil {
		return fmt.Errorf("%q is not an unsigned 32-bit integer", value)
	}
	*u = uint32Flag(v)
	return nil
}

// stage3Flags holds the consensus update's --stage3* and
// --indexer-failure-domain flags (plan S3-E2 contract §45).
type stage3Flags struct {
	activate                                       bool
	groupSize, minSafeCopies                       uint32Flag
	maxLagPublications, lagLow                     uint32Flag
	evidenceTimeout                                uint64
	maxUncleanedPerPartition, maxUncleanedPerTable uint32Flag
	networkFillDefault, strictCopies, excludeHosts bool
	switches                                       stage3SwitchFlags
	domains                                        failureDomainFlags
	present                                        map[string]bool
}

// stage3ActivationRequired are the flags the activation cannot default.
var stage3ActivationRequired = []string{"stage3-max-lag-publications", "stage3-lag-low", "stage3-evidence-timeout-l2-blocks",
	"stage3-max-uncleaned-parts-per-partition", "stage3-max-uncleaned-parts-per-table"}

func (s *stage3Flags) register(fs *flag.FlagSet) {
	fs.BoolVar(&s.activate, "stage3", false, "set stage3 for the first time: the irreversible v21 activation, every switch off (needs --voter and the five required --stage3-* values)")
	fs.Var(&s.groupSize, "stage3-group-size", "stage3 group_size (activation default 3); omitted keeps the committed value")
	fs.BoolVar(&s.networkFillDefault, "stage3-network-fill-default", true, "stage3 network_fill_default (activation default true); omitted keeps the committed value")
	fs.Var(&s.minSafeCopies, "stage3-min-safe-copies", "stage3 min_safe_copies (activation default 1; raise-only, above 1 only after checkpoints and narrowed_drain); omitted keeps the committed value")
	fs.BoolVar(&s.strictCopies, "stage3-strict-copies", false, "stage3 strict_copies (one-way, only after checkpoints and narrowed_drain); omitted keeps the committed value")
	fs.Var(&s.maxLagPublications, "stage3-max-lag-publications", "stage3 max_lag_publications (required with --stage3); omitted keeps the committed value")
	fs.Var(&s.lagLow, "stage3-lag-low", "stage3 lag_low, 1 <= lag_low < max_lag_publications (required with --stage3); omitted keeps the committed value")
	fs.Uint64Var(&s.evidenceTimeout, "stage3-evidence-timeout-l2-blocks", 0, "stage3 evidence_timeout_l2_blocks (required with --stage3); omitted keeps the committed value")
	fs.Var(&s.maxUncleanedPerPartition, "stage3-max-uncleaned-parts-per-partition", "stage3 max_uncleaned_parts_per_partition (required with --stage3); omitted keeps the committed value")
	fs.Var(&s.maxUncleanedPerTable, "stage3-max-uncleaned-parts-per-table", "stage3 max_uncleaned_parts_per_table (required with --stage3); omitted keeps the committed value")
	fs.BoolVar(&s.excludeHosts, "stage3-exclude-quarantined-hosts", false, "stage3 exclude_quarantined_hosts; omitted keeps the committed value")
	fs.Var(&s.switches, "stage3-switch", "turn on one stage3 switch (purge, eviction_voiding, owner_aware_selection, placement, reselection, checkpoints, narrowed_drain), each after every earlier one is committed")
	fs.Var(&s.domains, "indexer-failure-domain", "stage3 indexer_failure_domains entry id=N,domain=D; repeat for the complete map (omitted keeps the committed map)")
}

// check runs the rules that need only the flags, before anything is read.
func (s *stage3Flags) check(present map[string]bool, voters []string) error {
	s.present = present
	if len(s.switches) > 1 {
		return errors.New("--stage3-switch is given more than once; an update turns on one switch")
	}
	if !s.activate {
		return nil
	}
	if len(s.switches) > 0 {
		return errors.New("--stage3-switch cannot be combined with --stage3: the activation leaves every switch off")
	}
	for _, name := range stage3ActivationRequired {
		if !present[name] {
			return errors.New("--stage3 needs --stage3-max-lag-publications, --stage3-lag-low, --stage3-evidence-timeout-l2-blocks, --stage3-max-uncleaned-parts-per-partition and --stage3-max-uncleaned-parts-per-table")
		}
	}
	if len(voters) == 0 {
		return errors.New("--stage3 needs --voter: the leader probes every voter for stage3_v1")
	}
	return nil
}

// given reports whether any --stage3-* flag or --indexer-failure-domain was
// given (--stage3 itself excluded).
func (s *stage3Flags) given() bool {
	for name := range s.present {
		if strings.HasPrefix(name, "stage3-") || name == "indexer-failure-domain" {
			return true
		}
	}
	return false
}

// target is the stage3 parameter the update signs: for the activation the
// defaults (group_size 3, network_fill_default true, min_safe_copies 1) with
// every given flag applied and every switch off; afterwards the committed
// parameter with every given flag applied and at most one switch turned on;
// nil while stage3 is not committed and nothing asks for it.
func (s *stage3Flags) target(committed *arbiter.Stage3Params) (*arbiter.Stage3Params, error) {
	switch {
	case s.activate && committed != nil:
		return nil, errors.New("--stage3 sets stage3 for the first time, but the leader has it committed; omit --stage3")
	case !s.activate && committed == nil:
		if s.given() {
			return nil, errors.New("--stage3-* and --indexer-failure-domain need stage3: pass --stage3 for the activation")
		}
		return nil, nil
	}
	out := arbiter.Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1}
	if committed != nil {
		out = *committed
		out.IndexerFailureDomains = slices.Clone(committed.IndexerFailureDomains)
	}
	for _, f := range []struct {
		name  string
		apply func()
	}{
		{"stage3-group-size", func() { out.GroupSize = uint32(s.groupSize) }},
		{"stage3-network-fill-default", func() { out.NetworkFillDefault = s.networkFillDefault }},
		{"stage3-min-safe-copies", func() { out.MinSafeCopies = uint32(s.minSafeCopies) }},
		{"stage3-strict-copies", func() { out.StrictCopies = s.strictCopies }},
		{"stage3-max-lag-publications", func() { out.MaxLagPublications = uint32(s.maxLagPublications) }},
		{"stage3-lag-low", func() { out.LagLow = uint32(s.lagLow) }},
		{"stage3-evidence-timeout-l2-blocks", func() { out.EvidenceTimeoutL2Blocks = s.evidenceTimeout }},
		{"stage3-max-uncleaned-parts-per-partition", func() { out.MaxUncleanedPartsPerPartition = uint32(s.maxUncleanedPerPartition) }},
		{"stage3-max-uncleaned-parts-per-table", func() { out.MaxUncleanedPartsPerTable = uint32(s.maxUncleanedPerTable) }},
		{"stage3-exclude-quarantined-hosts", func() { out.ExcludeQuarantinedHosts = s.excludeHosts }},
		{"indexer-failure-domain", func() { out.IndexerFailureDomains = slices.Clone([]arbiter.IndexerFailureDomain(s.domains)) }},
	} {
		if s.present[f.name] {
			f.apply()
		}
	}
	if len(s.switches) == 1 {
		name := s.switches[0]
		if out.SwitchOn(name) {
			return nil, fmt.Errorf("--stage3-switch %s is on already", name)
		}
		setStage3Switch(&out, name)
	}
	return &out, nil
}

// setStage3Switch turns on the switch name (one of arbiter.Stage3Switches,
// which stage3SwitchFlags.Set checked).
func setStage3Switch(p *arbiter.Stage3Params, name string) {
	switch name {
	case "purge":
		p.Purge = true
	case "eviction_voiding":
		p.EvictionVoiding = true
	case "owner_aware_selection":
		p.OwnerAwareSelection = true
	case "placement":
		p.Placement = true
	case "reselection":
		p.Reselection = true
	case "checkpoints":
		p.Checkpoints = true
	case "narrowed_drain":
		p.NarrowedDrain = true
	}
}

// verifierHostKeys are the --verifier keys of a host-bound entry (plan S3-E1
// contract §39), in usage order.
var verifierHostKeys = []string{"host", "replica-signer", "consent", "domain", "weight", "verify", "replicate", "counts", "shared-rmt"}

// parseVerifierFlag reads node=NODE_ID,pubkey=HEX and, for a host-bound
// entry, host=N,replica-signer=0x...,consent=PATH,domain=D with the optional
// weight=W (default 1) and verify=, replicate=, counts=, shared-rmt=
// (each true or false, default true). consent names a file holding the host
// indexer's arbiter-group-member-v1 JWS, surrounding whitespace trimmed; the
// FSM verifies it.
func parseVerifierFlag(value string) (arbiter.VerifierEntry, error) {
	fields, err := keyValuesOptional(value, []string{"node", "pubkey"}, verifierHostKeys)
	if err != nil {
		return arbiter.VerifierEntry{}, fmt.Errorf("--verifier: %w", err)
	}
	node := fields["node"]
	pub, err := decodeVerifierPubkey(fields["pubkey"])
	if err != nil {
		return arbiter.VerifierEntry{}, fmt.Errorf("--verifier %s: %w", node, err)
	}
	entry := arbiter.VerifierEntry{NodeID: node, Ed25519Pubkey: pub}
	if _, ok := fields["host"]; ok {
		if err := parseHostBoundVerifier(&entry, fields); err != nil {
			return arbiter.VerifierEntry{}, err
		}
	} else if slices.ContainsFunc(verifierHostKeys[1:], func(k string) bool { _, given := fields[k]; return given }) {
		return arbiter.VerifierEntry{}, fmt.Errorf("--verifier %s: host-bound keys (replica-signer, consent, domain, weight, verify, replicate, counts, shared-rmt) need host=N", node)
	}
	if err := entry.Validate(); err != nil {
		return arbiter.VerifierEntry{}, fmt.Errorf("--verifier %s: %w", node, err)
	}
	return entry, nil
}

// parseHostBoundVerifier fills entry's host-bound fields from fields, which
// holds host.
func parseHostBoundVerifier(entry *arbiter.VerifierEntry, fields map[string]string) error {
	node := entry.NodeID
	host, err := strconv.ParseUint(fields["host"], 10, 64)
	if err != nil {
		return fmt.Errorf("--verifier %s: host %q is not an unsigned integer", node, fields["host"])
	}
	for _, k := range []string{"replica-signer", "consent", "domain"} {
		if _, ok := fields[k]; !ok {
			return fmt.Errorf("--verifier %s: host=N needs replica-signer=0x..., consent=PATH and domain=D", node)
		}
	}
	weight := uint64(1)
	if w, ok := fields["weight"]; ok {
		if weight, err = strconv.ParseUint(w, 10, 32); err != nil || weight < 1 || weight > 64 {
			return fmt.Errorf("--verifier %s: weight %q must be 1 to 64", node, w)
		}
	}
	flags := map[string]*bool{"verify": &entry.Verify, "replicate": &entry.Replicate, "counts": &entry.Counts, "shared-rmt": &entry.SharedRMT}
	for _, k := range []string{"verify", "replicate", "counts", "shared-rmt"} {
		switch v, ok := fields[k]; {
		case !ok || v == "true":
			*flags[k] = true
		case v == "false":
			*flags[k] = false
		default:
			return fmt.Errorf("--verifier %s: %s=%q must be true or false", node, k, v)
		}
	}
	jws, err := os.ReadFile(fields["consent"])
	if err != nil {
		return fmt.Errorf("--verifier %s consent: %w", node, err)
	}
	entry.HostIndexerID = &host
	entry.ReplicaSigner = strings.ToLower(fields["replica-signer"])
	entry.ConsentJWS = strings.TrimSpace(string(jws))
	entry.FailureDomain = fields["domain"]
	entry.Weight = uint32(weight)
	return nil
}

// keyValuesOptional splits "k=v,k=v": every required key exactly once, each
// optional key at most once, both with a non-empty value; the result holds
// only the keys given.
func keyValuesOptional(value string, required, optional []string) (map[string]string, error) {
	known := append(slices.Clone(required), optional...)
	out := map[string]string{}
	for _, part := range strings.Split(value, ",") {
		k, v, ok := strings.Cut(part, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" {
			return nil, fmt.Errorf("%q is not key=value", part)
		}
		if !slices.Contains(known, k) {
			return nil, fmt.Errorf("unknown key %q (want %s)", k, strings.Join(known, ", "))
		}
		if _, dup := out[k]; dup {
			return nil, fmt.Errorf("key %q is given twice", k)
		}
		if v == "" && slices.Contains(optional, k) {
			return nil, fmt.Errorf("key %q has no value", k)
		}
		out[k] = v
	}
	for _, k := range required {
		if out[k] == "" {
			return nil, fmt.Errorf("missing %s=", k)
		}
	}
	return out, nil
}
```

In `cmd/arbiter-admin/main.go` make these edits.

The `consensus update` usage line: in `cmd/arbiter-admin/main.go:89`, in the line `  arbiter-admin consensus update --address LEADER_GRPC …` (the rest of the line, from ` [--voter VOTER_GRPC ...]` on, is unchanged), replace

```text
[--verifier 'node=NODE_ID,pubkey=HEX' ...] [--evict-unnamed] [--tracks-enabled]
```

with

```text
[--verifier 'node=NODE_ID,pubkey=HEX[,host=N,replica-signer=0x...,consent=PATH,domain=D,weight=W,verify=B,replicate=B,counts=B,shared-rmt=B]' ...] [--evict-unnamed] [--tracks-enabled] [--stage3 --stage3-max-lag-publications N --stage3-lag-low N --stage3-evidence-timeout-l2-blocks N --stage3-max-uncleaned-parts-per-partition N --stage3-max-uncleaned-parts-per-table N] [--stage3-group-size N] [--stage3-network-fill-default=B] [--stage3-min-safe-copies N] [--stage3-strict-copies] [--stage3-exclude-quarantined-hosts=B] [--indexer-failure-domain 'id=N,domain=D' ...] [--stage3-switch NAME]
```

The usage paragraphs: in `cmd/arbiter-admin/main.go:156-157`, at the end of the `--tracks-enabled` paragraph, replace

```text
the committed value is carried; once set every update carries it. Run the
activation as its own update, restating every other value.
```

with

```text
the committed value is carried; once set every update carries it. Run the
activation as its own update, restating every other value.
--stage3 sets the stage3 parameter for the first time, the irreversible v21
activation: every switch off, group_size 3, network_fill_default true and
min_safe_copies 1 unless given, and the five values it cannot default
(--stage3-max-lag-publications, --stage3-lag-low,
--stage3-evidence-timeout-l2-blocks, --stage3-max-uncleaned-parts-per-partition,
--stage3-max-uncleaned-parts-per-table). It needs the tracks activation first
and --voter for every Raft voter: every voter must report stage3_v1, and every
registered verifier and SNode must have advertised it to the current leader,
except the SNode of a quarantined track. The CLI names the --voter addresses
to the leader, which probes every voter itself before it proposes; the
Arbiter also refuses the activation while the stage-2 drain gate does.
--indexer-failure-domain gives the complete indexer_failure_domains map, one
flag per enrolled indexer (from the placement switch on, every indexer needs
one). After the activation every update carries stage3: an omitted --stage3-*
value or map keeps the committed one, and --stage3-switch NAME turns on one
switch (purge, eviction_voiding, owner_aware_selection, placement, reselection,
checkpoints, narrowed_drain), each only after every earlier one is committed.
Switches and strict_copies are one-way; min_safe_copies and group_size never
decrease. A host-bound --verifier (host=N) names its host indexer, replica
signer, a file holding the host's arbiter-group-member-v1 consent JWS and the
host's failure domain; weight defaults to 1 and each flag to true. Host-bound
entries are refused until the owner_aware_selection switch is committed.
```

`verifierFlags.Set`: in `cmd/arbiter-admin/main.go:219-234`, replace

```go
func (v *verifierFlags) Set(value string) error {
	fields, err := keyValues(value, "node", "pubkey")
	if err != nil {
		return fmt.Errorf("--verifier: %w", err)
	}
	pub, err := decodeVerifierPubkey(fields["pubkey"])
	if err != nil {
		return fmt.Errorf("--verifier %s: %w", fields["node"], err)
	}
	entry := arbiter.VerifierEntry{NodeID: fields["node"], Ed25519Pubkey: pub}
	if err := entry.Validate(); err != nil {
		return fmt.Errorf("--verifier %s: %w", fields["node"], err)
	}
	*v = append(*v, entry)
	return nil
}
```

with

```go
func (v *verifierFlags) Set(value string) error {
	entry, err := parseVerifierFlag(value)
	if err != nil {
		return err
	}
	*v = append(*v, entry)
	return nil
}
```

`keyValues`, which now shares `keyValuesOptional` (same texts): in `cmd/arbiter-admin/main.go:264-287`, replace

```go
// keyValues splits "k=v,k=v" and requires exactly the keys named, once each.
func keyValues(value string, keys ...string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(value, ",") {
		k, v, ok := strings.Cut(part, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" {
			return nil, fmt.Errorf("%q is not key=value", part)
		}
		if !slices.Contains(keys, k) {
			return nil, fmt.Errorf("unknown key %q (want %s)", k, strings.Join(keys, ", "))
		}
		if _, dup := out[k]; dup {
			return nil, fmt.Errorf("key %q is given twice", k)
		}
		out[k] = v
	}
	for _, k := range keys {
		if out[k] == "" {
			return nil, fmt.Errorf("missing %s=", k)
		}
	}
	return out, nil
}
```

with

```go
// keyValues splits "k=v,k=v" and requires exactly the keys named, once each
// (keyValuesOptional with no optional key).
func keyValues(value string, keys ...string) (map[string]string, error) {
	return keyValuesOptional(value, keys, nil)
}
```

The update flags: in `cmd/arbiter-admin/main.go:387-388`, in the flag variables of `run`, replace

```go
	var tracksEnabled bool
	var holdName string
```

with

```go
	var tracksEnabled bool
	var stage3 stage3Flags
	var holdName string
```

In `cmd/arbiter-admin/main.go:402`, the help text of `--verifier`, replace

```go
		fs.Var(&verifiers, "verifier", "verifiers entry node=NODE_ID,pubkey=HEX; repeat for the complete list (omitted keeps the committed list)")
```

with

```go
		fs.Var(&verifiers, "verifier", "verifiers entry node=NODE_ID,pubkey=HEX, for a host-bound entry with host=N,replica-signer=0x...,consent=PATH,domain=D[,weight=W,verify=B,replicate=B,counts=B,shared-rmt=B]; repeat for the complete list (omitted keeps the committed list)")
```

In `cmd/arbiter-admin/main.go:404-405`, registering the stage-3 flags after `--tracks-enabled` and extending the help text of `--voter`, replace

```go
		fs.BoolVar(&tracksEnabled, "tracks-enabled", false, "set tracks_enabled, the irreversible tracks activation (needs --voter); omitted keeps the current value, and once set every update carries it")
		fs.Var(&voters, "voter", "direct gRPC address of a Raft voter; repeat for every voter (required to set or raise client lanes, for the signed-claims activation and for the tracks activation)")
```

with

```go
		fs.BoolVar(&tracksEnabled, "tracks-enabled", false, "set tracks_enabled, the irreversible tracks activation (needs --voter); omitted keeps the current value, and once set every update carries it")
		stage3.register(fs)
		fs.Var(&voters, "voter", "direct gRPC address of a Raft voter; repeat for every voter (required to set or raise client lanes, for the signed-claims activation, the tracks activation and the stage-3 activation)")
```

The flag checks: in `cmd/arbiter-admin/main.go:482-484`, replace

```go
		if len(voters) != 0 && !clientLanesSet && !siIndexersSet && !tracksEnabled {
			return errors.New("--voter is only used with --client-lanes-max-per-account or --si-indexer, or with --tracks-enabled")
		}
```

with

```go
		if len(voters) != 0 && !clientLanesSet && !siIndexersSet && !tracksEnabled && !stage3.activate {
			return errors.New("--voter is only used with --client-lanes-max-per-account or --si-indexer, or with --tracks-enabled or --stage3")
		}
		if err := stage3.check(present, voters); err != nil {
			return err
		}
```

The target, after the tracks prerequisite: in `cmd/arbiter-admin/main.go:634-635`, replace

```go
		return errors.New("--tracks-enabled needs the signed-claims activation first: si_indexers is not committed, and the tracks activation must be a later update (clarification C11)")
	}
```

with

```go
		return errors.New("--tracks-enabled needs the signed-claims activation first: si_indexers is not committed, and the tracks activation must be a later update (clarification C11)")
	}
	// The stage-3 activation must be a later update than the tracks
	// activation (plan S3-E2 K69). Absent, the committed stage3 is carried
	// with every given --stage3-* value applied: once set the FSM refuses an
	// update that drops it.
	if stage3.activate && !currentTracks {
		return errors.New("--stage3 needs the tracks activation first: tracks_enabled is not committed, and the stage-3 activation must be a later update")
	}
	stage3Target, err := stage3.target(wire.Stage3ParamsFromPB(state.GetCurrent().GetStage3()))
	if err != nil {
		return err
	}
```

The gate, after the tracks gate: in `cmd/arbiter-admin/main.go:696-700`, right before the `planned` update is built, replace

```go
		if err := checkTracksNotEnabled(ctx, client); err != nil {
			return err
		}
	}
	planned := arbiter.ConsensusParamsUpdate{
```

with

```go
		if err := checkTracksNotEnabled(ctx, client); err != nil {
			return err
		}
	}
	// stage3Seen is set exactly when this update is the v21 activation; the
	// observation is re-checked before submit and after commit. The leader's
	// track state names the quarantined tracks, whose SNodes are exempt.
	var stage3Gate activationGate
	var stage3Seen *gateObservation
	if stage3.activate {
		tracks, err := client.GetTrackState(ctx, &emptypb.Empty{})
		if err != nil {
			return fmt.Errorf("stage3 gate refused: read the leader's track state: %w", err)
		}
		stage3Gate = stage3GateFor(stage3ExemptSNodes(state, tracks))
		if stage3Seen, err = runActivationGate(ctx, stage3Gate, client, []string(voters), dialAdmin); err != nil {
			return err
		}
		if err := checkStage3NotActive(ctx, client); err != nil {
			return err
		}
	}
	planned := arbiter.ConsensusParamsUpdate{
```

The `planned` literal: in `cmd/arbiter-admin/main.go:709-710`, replace

```go
		TracksEnabled:                 tracksTarget,
	}
```

with

```go
		TracksEnabled:                 tracksTarget,
		Stage3:                        stage3Target,
	}
```

The pre-submit re-check: in `cmd/arbiter-admin/main.go:774-775`, at the end of the tracks re-check, right before `submitCtx := ctx`, replace

```go
		ctx = metadata.AppendToOutgoingContext(ctx, tracksVotersMetadataKey, tracksVotersMetadata([]string(voters)))
	}
```

with

```go
		ctx = metadata.AppendToOutgoingContext(ctx, tracksVotersMetadataKey, tracksVotersMetadata([]string(voters)))
	}
	if stage3Seen != nil {
		if stage3Seen, err = recheckActivationGate(ctx, stage3Gate, client, []string(voters), dialAdmin, stage3Seen); err != nil {
			return err
		}
		// As for the tracks activation, the leader probes these addresses
		// itself (plan S3-E2 contract §45), so each --voter must be the
		// voter's direct admin address as the leader can reach it.
		ctx = metadata.AppendToOutgoingContext(ctx, stage3VotersMetadataKey, tracksVotersMetadata([]string(voters)))
	}
```

The post-commit re-check: in `cmd/arbiter-admin/main.go:811-813`, replace

```go
	if tracksSeen != nil {
		warnGateAfterCommit(ctx, *timeout, tracksActivation, client, []string(voters), dialAdmin, tracksSeen)
	}
```

with

```go
	if tracksSeen != nil {
		warnGateAfterCommit(ctx, *timeout, tracksActivation, client, []string(voters), dialAdmin, tracksSeen)
	}
	if stage3Seen != nil {
		warnGateAfterCommit(ctx, *timeout, stage3Gate, client, []string(voters), dialAdmin, stage3Seen)
	}
```

The gate's exemption hook: in `cmd/arbiter-admin/main.go:1422-1423`, at the end of `activationGate`, replace

```go
	roleConflicts func(snodes, verifiers []string) []string
}
```

with

```go
	roleConflicts func(snodes, verifiers []string) []string
	// exempt, when set, reports a data-plane node of the feature book that
	// need not report feature: for the stage-3 activation the SNode of a
	// quarantined track (housegate spec 2026-10-14 §10.3). observeGate
	// records the exempt nodes, problems skips them and lines lists them.
	exempt func(nodeID string) bool
}
```

`runActivationGate` passes its gate to `observeGate`: in `cmd/arbiter-admin/main.go:1521`, replace

```go
	observed, err := observeGate(ctx, leader, voters, dial)
```

with

```go
	observed, err := observeGate(ctx, g, leader, voters, dial)
```

`recheckActivationGate` does too: in `cmd/arbiter-admin/main.go:1534-1536`, replace

```go
	now, err := observeGate(ctx, leader, voters, dial)
	if err != nil {
		return nil, fmt.Errorf("%s re-check before submit refused
```

with

```go
	now, err := observeGate(ctx, g, leader, voters, dial)
	if err != nil {
		return nil, fmt.Errorf("%s re-check before submit refused
```

and so does `warnGateAfterCommit`: in `cmd/arbiter-admin/main.go:1554-1556`, replace

```go
	now, err := observeGate(ctx, leader, voters, dial)
	if err != nil {
		fmt.Fprintf(stderr, "WARNING: the %s update committed
```

with

```go
	now, err := observeGate(ctx, g, leader, voters, dial)
	if err != nil {
		fmt.Fprintf(stderr, "WARNING: the %s update committed
```

The observation: in `cmd/arbiter-admin/main.go:1601-1602`, at the end of `gateObservation`, replace

```go
	snodes, verifiers []string
}
```

with

```go
	snodes, verifiers []string
	// exempt lists, sorted, the book's nodes the gate exempts
	// (activationGate.exempt).
	exempt []string
}
```

In `cmd/arbiter-admin/main.go:1604`, the signature of `observeGate`, replace

```go
func observeGate(ctx context.Context, leader pb.ConsensusAdminClient, voters []string, dial dialFunc) (*gateObservation, error) {
```

with

```go
func observeGate(ctx context.Context, g activationGate, leader pb.ConsensusAdminClient, voters []string, dial dialFunc) (*gateObservation, error) {
```

and, at the end of `observeGate`, in `cmd/arbiter-admin/main.go:1621-1624`, replace

```go
		observed.probes = append(observed.probes, probe)
	}
	return observed, nil
}
```

with

```go
		observed.probes = append(observed.probes, probe)
	}
	for _, e := range observed.entries {
		if g.exempt != nil && g.exempt(e.GetNodeId()) {
			observed.exempt = append(observed.exempt, e.GetNodeId())
		}
	}
	slices.Sort(observed.exempt)
	return observed, nil
}
```

In `problems`: in `cmd/arbiter-admin/main.go:1674-1675`, replace

```go
		id := e.GetNodeId()
		if g.bound != nil && !g.bound(id) {
```

with

```go
		id := e.GetNodeId()
		if slices.Contains(o.exempt, id) {
			continue // the SNode of a quarantined track: the activation does not need it
		}
		if g.bound != nil && !g.bound(id) {
```

In `lines`: in `cmd/arbiter-admin/main.go:1722-1724`, replace

```go
		out = append(out, fmt.Sprintf("registered roles: snode [%s], verifier [%s]", strings.Join(o.snodes, " "), strings.Join(o.verifiers, " ")))
	}
	return out
```

with

```go
		out = append(out, fmt.Sprintf("registered roles: snode [%s], verifier [%s]", strings.Join(o.snodes, " "), strings.Join(o.verifiers, " ")))
	}
	for _, id := range o.exempt {
		out = append(out, "exempt (quarantined track): "+id)
	}
	return out
```

- [ ] **Step 18: Run them and see them pass**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test -z "$(gofmt -l cmd/arbiter-admin)" && go vet ./cmd/arbiter-admin/ && go test ./cmd/arbiter-admin/ -run 'TestConsensusUpdateStage3|TestConsensusUpdateCarriesTheCommittedStage3AndHostBoundVerifiers|TestVerifierFlag|TestIndexerFailureDomainFlag|TestStage3ActivationGateExemptsSNodesOfQuarantinedTracks|TestStage3VotersMetadataKeyMatchesTheServer|TestConsensusShowJSONHasTheStage3FieldsTheRunbooksRead|TestCLIStage3ActivationThroughServerAndFSM|TestSIIndexerAndVerifierFlagsAreCheckedLocally' -race -count=1 -v && go test ./cmd/arbiter-admin/ -race -count=1`
Expected: gofmt and vet are silent; the eleven tests of `stage3_test.go` and `TestCLIStage3ActivationThroughServerAndFSM` PASS (`TestConsensusUpdateStage3SwitchTurnsOnOneSwitch` with one subtest per switch, seven in all, `TestConsensusUpdateStage3FlagRefusals` with sixteen), `TestSIIndexerAndVerifierFlagsAreCheckedLocally` still passes (the pool `--verifier` texts are unchanged), and the whole arbiter-admin suite prints `ok  	github.com/sentioxyz/arbiter/cmd/arbiter-admin`, the tracks, signed-claims, client-lanes and Plan S3-A hold tests included.

- [ ] **Step 19: Document the stage-3 activation**

In `README.md:396`, insert this section before the line `## Artifact disposition admission` (after Plan S3-A's `## Stage-3 reads and consensus holds` section):

```markdown
## Multi-source storage integrity (stage-3 activation)

**Activation.** `stage3` is the stage-3 consensus parameter (`ConsensusParamsUpdate.stage3`, shown as `current.stage3` by `consensus show`; housegate spec 2026-10-14 §6.1, §10.3). It is absent until one authority-signed update after the tracks activation sets it with every switch off, `strict_copies` false and `min_safe_copies` 1; afterwards every update must carry it. That update is the irreversible v21 activation. Apply judges it by the unchanged stage-2 drain gate and needs neither an empty open block nor a global prefix. The leader refuses it before proposing while the Raft configuration has a non-voter, while the FSM's own activation refusal holds (`stage3 gate refused: stage3 activation requires the tracks activation`, or the drain gate's reason), while a registered verifier or SNode has not advertised `stage3_v1` to it in the current term (`stage3 gate refused: data-plane nodes ... have not registered with this leader advertising stage3_v1; restart them on a stage-3 release`; the SNode of a quarantined track is exempt), and, last, unless every Raft voter passes the probe the leader makes itself at the direct admin addresses the request names in the `arbiter-stage3-voters` gRPC metadata (`stage3 gate refused: voter ... lacks stage3_v1; restart it on a stage-3 release`): the tracks gate's probe, with the tracks gate's refusals in the stage-3 gate's words. After the activation the leader refuses a data-plane registration without `stage3_v1` (`stage3 is active; this binary does not support stage-3 consensus`).

**Switches.** `purge`, `eviction_voiding`, `owner_aware_selection`, `placement`, `reselection`, `checkpoints` and `narrowed_drain` are one-way fields of `stage3`, turned on one per update and each only after every earlier one is committed (spec §10.4); `strict_copies` is one-way too, and `min_safe_copies` and `group_size` never decrease. A switch-on runs no gate. Every committed change of `stage3` bumps the table registry version, so registry followers learn each switch from `TableRegistrySnapshot.stage3`.

**Reads.** `consensus show` prints `current.stage3` (proto field names, false switches printed) and `stage3_state`: `activation_epoch`, `activation_block_seq`, `activation_promotion_seq` and, per switch turned on, `switch_name`, `epoch`, `cutover_block_seq` and `cutover_promotion_seq`; both are `null` before the activation.

**CLI.** `arbiter-admin consensus update ... --stage3 --indexer-failure-domain 'id=N,domain=D' [...] --stage3-max-lag-publications N --stage3-lag-low N --stage3-evidence-timeout-l2-blocks N --stage3-max-uncleaned-parts-per-partition N --stage3-max-uncleaned-parts-per-table N --voter VOTER_GRPC [--voter ...]` is the activation; `--stage3-group-size` (default 3), `--stage3-network-fill-default` (default true), `--stage3-min-safe-copies` (default 1), `--stage3-strict-copies` and `--stage3-exclude-quarantined-hosts` are optional, and every other value is carried. The CLI refuses unless the tracks activation is committed, the probed voters equal the leader's Raft voter set and each reports `stage3_v1`, and every registered verifier and SNode the leader's feature book lists advertised it, except the SNodes of the tracks the leader's track state reports quarantined (read once, before the gate; the leader's own gate decides again from its applied state); it re-reads the leader's parameters after its gate, takes its consensus hold (`--hold drain` by default), re-checks the gate right before submitting and again after the commit, and sends its `--voter` addresses to the leader in `arbiter-stage3-voters`, so name each voter by the direct admin address the leader itself can reach. After the activation an omitted `--stage3-*` value or `--indexer-failure-domain` map keeps the committed one, and `--stage3-switch NAME` turns on one switch. `--verifier 'node=NODE_ID,pubkey=HEX,host=N,replica-signer=0x...,consent=PATH,domain=D[,weight=W,verify=B,replicate=B,counts=B,shared-rmt=B]'` lists a host-bound member entry: `consent` names a file holding the host indexer's `arbiter-group-member-v1` JWS, `weight` defaults to 1 and each flag to `true`; the Arbiter refuses host-bound entries until `owner_aware_selection` is committed. The runbook is production `docs/storage-integrity-devnet2-stage3-p5-v21.md`.
```

- [ ] **Step 20: Full verification**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && bazel run //:gazelle && git status --short && test -z "$(git ls-files -z '*.go' | xargs -0 gofmt -l)" && go test ./... -race -count=1 && bazel build //... && bazel test //... --test_output=errors && bazel query 'deps(//fsm:fsm, 1)' | grep -c com_github_sentioxyz_arbiter_proto; grep -rn 'time\.Now' fsm/; git diff --exit-code --diff-filter=MDR HEAD~2 -- fsm/testdata accumulator/testdata`
Expected: gazelle adds `"stage3.go",` to `go_library(name = "arbiter-admin_lib")` (whose `srcs` becomes a list with `"main.go"`) and `"stage3_integration_test.go",`, `"stage3_test.go",` to `go_test(name = "arbiter-admin_test")` in `cmd/arbiter-admin/BUILD.bazel`, no dependency; `git status` lists ` M README.md`, ` M cmd/arbiter-admin/BUILD.bazel`, ` M cmd/arbiter-admin/main.go`, `?? cmd/arbiter-admin/stage3.go`, `?? cmd/arbiter-admin/stage3_integration_test.go` and `?? cmd/arbiter-admin/stage3_test.go` only; gofmt lists nothing; `go test ./...` prints `ok` for every package with tests (`fsm`, `server`, `cmd/arbiter-admin`, `orchestrator` and `integration` among them); the Bazel build and suite PASS, every frozen golden test included (`TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, `TestPreActivationHistoryIsByteIdenticalAcrossTracks`, `TestL3CommitmentPreimagesAreFrozen`, `TestL3TrackHeaderPreimagesAreFrozen`, `TestL3TransitionHeaderPreimageIsFrozen`, `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`), and Task 64's stage-3 FSM tests; the `bazel query` count is `0` and the `time.Now` grep prints nothing; no golden or fixture changed since Task 64's last commit (`HEAD~2`; the diff is empty).

- [ ] **Step 21: Commit the CLI**

```bash
git -C "$W" add README.md cmd/arbiter-admin
git -C "$W" commit -m "feat(stage3): arbiter-admin consensus update --stage3, --stage3-switch and host-bound --verifier (Task 65)

--stage3 builds the v21 activation (group_size 3, network fill on,
min_safe_copies 1 unless given, every switch off, the five required
values and --indexer-failure-domain), needs the tracks activation and
--voter, exempts the SNodes of the tracks the leader reports
quarantined, runs the shared activation gate for stage3_v1, re-reads
the parameters, re-checks before submitting (after the default drain
hold), names the voters in arbiter-stage3-voters and re-checks after
the commit. After the activation every update carries the committed
stage3, each given --stage3-* value replaces only itself, and
--stage3-switch NAME turns on one switch without a gate. --verifier
takes host-bound entries (host=N with replica-signer, consent=PATH and
domain; weight 1 and every flag true by default). The README documents
the activation, the switches, the reads and the CLI (housegate spec
2026-10-14 §6.1, §10.3, §10.4; plan S3-E2 contract §45, K68, K69).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; git -C "$W" status --porcelain; git -C "$W" log --format=%s -3`
Expected: no status output; the three commits of this task on top of Task 64's, newest first `feat(stage3): arbiter-admin consensus update --stage3, --stage3-switch and host-bound --verifier (Task 65)`, `feat(stage3): the leader's stage-3 gate, the registration refusal and the stage-3 reads (Task 65)`, `test(stage3): pin every voter-probe refusal of the tracks gate (Task 65)`. Nothing is pushed: Task 70's last step pushes the branch.

---

## Task 66: arbiter — snapshot v21, the stage-3 restore rules and the frozen plain container (S3-E2)

Spec §10.3 makes the stage-3 activation write a new snapshot container that an older binary refuses, and clarification K70 fixes its framing: `AFSM` (4 bytes), version byte 21, a body-kind byte (0 plain, 1 interned), the body length (8 bytes, big-endian), the body and the spent-ids dump; v3–v20 keep their 13-byte envelope. After Tasks 64 and 65 the branch commits `Params.Stage3` and `State.Stage3`, but the container cannot hold them: `writeSnapshot` (`fsm/snapshot.go:168-189`) writes v20 whenever `tracks_enabled` is committed (`snapshotWriteVersion`, `:198-220`) and `State.Stage3` is no `snapshotDoc` member (`:68-108`), so a stage-3 state goes into a v20 container: this binary loses its cutover records on restore, and nothing in the container stops `ARBITER_TAG_P4`, which knows none of the stage-3 fields, from decoding it with them silently dropped. This task moves the writer to v21 exactly when `Params.Stage3` is committed and keeps v20 byte for byte before it, writes the plain body (the v20 document plus the member `stage3`) behind the kind byte, teaches the reader v21 with the plain body (Task 67 admits the interned body and adds its write-then-verify in `Persist`), refuses every stage-3 field under an older version byte on the pattern of `tracksFieldsPresent` (`:501-503`, `:695-721`), adds `fsm/stage3_restore.go` with the skeleton every later stage-3 task extends (`validateStage3State`, running this task's `validateStage3Activation`), refuses `stage3` in the bootstrap parameters, moves the capacity counter and Plan S3-A's state-size sample to the 14-byte envelope, and freezes the plain v21 container of a reproducible stage-3 network.

Worktree: Task 64's (`W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e`, the `path` Task 64's Step 1 printed, branch `urwt/claude/multi-source-s3e`, contract §48); the Bash tool keeps no variables between calls, so every command below starts with `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e;` in its own Bash call. Line anchors are arbiter `526a721` (`v0.13.0`, `origin/main` when this was drafted: Plan S3-A merged as arbiter#133, so its Task 9 code in `fsm/state_size.go` and `fsm/snapshot.go` is quoted as it landed). Tasks 64 and 65 change none of the fsm files this task edits; Task 65 adds the README section `## Multi-source storage integrity (stage-3 activation)` in front of `## Artifact disposition admission`, which Step 12's paragraph closes. The branch base (`ARBITER_TAG_P4`, with Plan S3-C2's `ARBITER_PIN_P2` and Plan S3-D Task 51) is met by the hunk rule of the Global Constraints: every hunk below quotes the exact text it replaces and is applied on top of what is there. This task runs no remote or live action: nothing is fetched, pushed, released or deployed (the branch push is Task 70's last step, under its own confirmation).

**Files:**
- Create: `fsm/stage3_restore.go`, `fsm/snapshot_v21_test.go`
- Create (frozen, captured once in Step 11, never rewritten): `fsm/testdata/stage3_v21_plain.snapshot`
- Modify: `fsm/snapshot.go` (imports `:8-9`; the version comment and constants `:21-25`, `:37-42`, `:59-61`; `snapshotDoc` `:106-108`; `writeSnapshot` `:177-180`; `snapshotWriteVersion` `:191-192`, `:198-199`; `snapshotDocument` `:244-246`; the reader's version whitelist `:285-287` and its predicates `:303`, `:318`, `:426`; after `validateTrackState` `:504-507`; Plan S3-A Task 9's restore sample `:656`; before `tableRegistryHasV17Fields` `:723-724`)
- Modify: `fsm/snapshot_count.go` (`:17-19`, `:39-42`, `:66-68`, `:98`)
- Modify: `fsm/state_size.go` (Plan S3-A Task 9: the `StateSizeSample.ContainerBytes` comment `:56-57`, `containerStateSize` and `stateSizeSample` `:114-142`)
- Modify: `fsm/consensus_history.go` (`:31-33`)
- Test: `fsm/apply_artifact_disposition_test.go` (`rewriteSnapshotDocument`, `:721-747`), `fsm/artifact_capacity_test.go` (`:207-213`), `fsm/snapshot_test.go` (`:192`, `:255`), `fsm/snapshot_tracks_test.go` (`:310-318`), `fsm/state_size_test.go` (Plan S3-A Task 9: the list of `TestStateComponentsListEverySnapshotDocMember` `:27-28` and the read-cap case of `TestFailedRestoreKeepsThePreviousSample` `:236`)
- Modify: `README.md` (the **Snapshot versions:** paragraph `:300`; right before `## Artifact disposition admission`, `:396` at `526a721`, after Task 65's section on the branch; Plan S3-A Task 9's **State size and snapshots** paragraph, `:429` at `526a721`)
- Modify (gazelle): `fsm/BUILD.bazel`

**Interfaces:**
- Consumes:
  - Task 64 (contract §45): `Params.Stage3 *arbiter.Stage3Params` (`json:"stage3,omitempty"`), `State.Stage3 *Stage3State`, `Stage3State{ActivationEpoch, ActivationBlockSeq, ActivationPromotionSeq uint64; Switches []Stage3SwitchCutover}` and `Stage3SwitchCutover{Switch string; Epoch, BlockSeq, PromotionSeq, IncarnationSeq uint64}` with their JSON tags, `Stage3Switches()`, `stage3SwitchValue`, `Stage3SwitchPurge`, `Stage3SwitchEvictionVoiding`, `Stage3SwitchOwnerAwareSelection`, `(*FSM).Stage3Active`; the records `activateStage3Locked` and `recordStage3SwitchesLocked` write (`ActivationEpoch` the epoch the activation produces, the 1-based position of its `ConsensusUpdates` entry; a cutover's `Epoch` the epoch its update produces, `BlockSeq` and `PromotionSeq` `len(Blocks)` and `PromotionSeq`; the purge cutover's `IncarnationSeq` `uint64(len(TableRegistry.Incarnations)) + 1`); the refusal `consensus update setting stage3 must leave every switch off, strict_copies false and min_safe_copies 1`; the fixture `stage3ActivationParams`, `activateStage3`, `stage3FSM`, `turnOnStage3Switch` (`fsm/stage3_fixture_test.go`).
  - Task 65 (contract §45): the registry-version bump of every Apply that commits a changed `Params.Stage3` (part of the golden network's state) and its README section.
  - Plan S3-E1 (contract §48, through `CORE_TAG_P5`): `arbiter.Stage3Params`; `arbiter.VerifierEntry`'s `HostIndexerID *uint64`, `ReplicaSigner`, `ConsentJWS`, `Weight`, `FailureDomain`, `Verify`, `Replicate`, `Counts`, `SharedRMT`; `arbiter.ConsensusParamsUpdate.Stage3`.
  - Plan S3-A Task 9 (contract §4, merged in `526a721`): `containerStateSize` (`fsm/state_size.go:115`), `stateSizeSample` (`:132`), `readSnapshotMeasured` (`fsm/snapshot.go:273`), `snapshotDocComponents`, `StateComponents`, `(*FSM).StateSizeSample`, `StateSizeSourceSnapshot`, `StateSizeSourceRestore`, `(*fsmSnapshot).recordSize` (`fsm/snapshot.go:762`), `TestStateComponentsListEverySnapshotDocMember` (`fsm/state_size_test.go:21`), `TestFailedRestoreKeepsThePreviousSample` (`:222`), and its README paragraph **State size and snapshots**.
  - Existing fsm code: `writeSnapshot`, `readSnapshot`, `snapshotDocument`, `snapshotWriteVersion`, `tracksFieldsPresent`, `validateTrackState`, `validateConsensusHistory` (`fsm/consensus_history.go:12`), `CountSnapshotEncoding`; test helpers `snapshotBytes`, `restoreInto` (`fsm/snapshot_test.go:30`, `:44`), `restoreErr` (`fsm/apply_snapshot_query_abort_test.go:566`), `restoreConfigured` (`fsm/table_set_transition_restore_test.go:31`), `publishedTransitionFSM` (`:13`), `rewriteSnapshotDocument` (`fsm/apply_artifact_disposition_test.go:721`), `keepSnapshotDocument` (`fsm/snapshot_lanes_test.go:18`), `lifecycleScript` (`fsm/determinism_test.go:20`), `v20Params` (`fsm/snapshot_tracks_test.go:20`), `tmV20Fixture` (`:74`), `newTrackManifestFixture` (`fsm/track_manifest_fixture_test.go:56`), `tracksFSM`, `tracksUpdate`, `activateTracks`, `admitTrack`, `bindTrackClaim`, `verifyBlock`, `promoteTrack` (`fsm/tracks_fixture_test.go:81`, `:63`, `:71`, `:106`, `:117`, `:126`, `:136`), `ownerFSMWithAuthority`, `fixedClient` (`fsm/admission_source_test.go:38`, `:18`), `lanesFSM` (`fsm/admission_lanes_test.go:36`), `seededRegistryFSM`, `seed`, `ev` (`fsm/apply_table_registry_test.go:109`, `:18`, `:14`), `loadLegacyConsensusFixture` (`fsm/consensus_history_test.go:29`), `snodeSigner`, `fixedEnrollmentEntry`, `partPromotionAck` (`fsm/pre_tracks_golden_test.go:30`, `:44`, `:148`), `legacyGoldenAuthorityHex` (`fsm/legacy_lane_golden_test.go:23`), `messageContextFor` (`fsm/signed_messages_fixture_test.go:28`), `registryUpdate`, `testRegistryParams`, `signedConsensusUpdate`, `rejectConsensusUnchanged` (`fsm/consensus_updates_test.go`), `claimsUpdate`, `testVerifiers` (`fsm/signed_claims_params_test.go:70`, `:59`), `baseTransitionOn`, `mustSeal` (`fsm/table_set_transition_seal_test.go:43`, `:54`), `registerActive`, `mustApply`, `mustNewFSM`, `newTestFSM`, `testParams` (`fsm/fsm_test.go`), `advanceTo`, `seedIndexerCmd`, `ownedAddCmd` (`fsm/table_registry_owners_test.go`), `verifyTransition`, `anchorFinal` (`fsm/table_set_transition_pipeline_test.go:47`, `:57`), `publishManifestInputs` (`fsm/table_set_transition_manifest_test.go:98`); `authoritytest.Iat`, `(*authority.Signer).SignSNodeMessageAt`, `authority.SNodeMessagePromotionAck`, `authority.NewSignerFromHex`.
- Produces (contract §46, verbatim):
  - `fsm/snapshot.go`: `snapshotVersionV21 = 21`, `snapshotVersion = 21`; `snapshotBodyPlain byte = 0`, `snapshotBodyInterned byte = 1` (the latter read by Task 67); `snapshotDoc.Stage3 *Stage3State` (`json:"stage3,omitempty"`, after `Tracks`), set by `snapshotDocument`; `snapshotWriteVersion` returning 21 exactly when `st.Params.Stage3 != nil`; `writeSnapshot` writing `snapshotBodyPlain` after the v21 version byte; `func stage3FieldsPresent(doc *snapshotDoc) bool`, `func verifierEntryHasStage3Fields(v arbiter.VerifierEntry) bool`; reader refusals `unsupported snapshot version %d (require 3, 4, …, 20 or 21); pre-v3 state requires a backup and coordinated re-genesis`, `unsupported snapshot v21 body kind %d`, `stage3 state is present in a pre-v21 container`, and `snapshot: %w` around `validateStage3State`.
  - `fsm/snapshot_count.go`: `snapshotEnvelopePrefixBytes = 13` (unchanged, v3–v20), `snapshotV21EnvelopePrefixBytes = 14`, `func snapshotEnvelopeBytes(version byte) uint64`; `CountSnapshotEncoding`'s `SnapshotEnvelopeBytes` is the document plus `snapshotEnvelopeBytes(snapshotWriteVersion(st))`.
  - `fsm/stage3_restore.go` (new): `func validateStage3State(st *State) error` (this task: `validateStage3Activation`; Task 69 appends `validateStage3Incarnations`, Task 70 `validateStage3Selection`, Plans S3-E3 and S3-E4 theirs), `func validateStage3Activation(st *State) error` with the refusals `stage3 state must exist exactly when stage3 is committed`; `stage3 activation epoch %d is not the epoch of the first update that set stage3 (%d)`; `stage3 activation block %d or promotion %d is beyond the state's (%d, %d)`; `stage3 records a cutover for switch %s, which is off`; `stage3 switch %s is on without a cutover record`; `stage3 switch cutovers are not in the order of spec §10.4`; `stage3 switch %s cutover epoch %d is not the epoch of the update that turned it on (%d)`; `stage3 switch %s cutover (block %d, promotion %d) precedes the previous cutover or the activation`; `stage3 switch %s cutover carries an incarnation seq`; `stage3 purge cutover incarnation seq %d is outside 1..%d`.
  - `fsm/consensus_history.go`: `snapshot bootstrap consensus params: stage3 is set only by a consensus update, not at genesis`.
  - `fsm/testdata/stage3_v21_plain.snapshot` (frozen), written once by `TestStage3V21PlainContainerGolden` under `ARBITER_CAPTURE_STAGE3_V21_PLAIN_GOLDEN=1` from `stage3V21GoldenFSM(t *testing.T) *FSM`; named tests `TestSnapshotWritesV20UntilStage3IsCommitted`, `TestSnapshotWritesAV21PlainContainerOnceStage3IsCommitted`, `TestStage3V21PlainContainerGolden`, `TestRestoreReadsEveryEarlierVersionUnchanged`, `TestRestoreRefusesStage3FieldsInAPreV21Container`, `TestRestoreRefusesAnUnknownV21BodyKind`, `TestValidateStage3StateRequiresStateExactlyWithTheParameter`, `TestValidateStage3StateRefusesAnActivationEpochThatIsNotTheHistorys`, `TestValidateStage3StateRefusesSwitchCutoversThatAreNotTheHistorys`, `TestContainerStateSizeMeasuresAV21PlainContainer`, `TestCountSnapshotEncodingMatchesTheV21Writer`, the edited `TestStateComponentsListEverySnapshotDocMember`.
  - Added by this task beyond §46 (recorded in Appendix B): the refusals `stage3 switch %s cutover (block %d, promotion %d) is beyond the state's (%d, %d)` (`validateStage3Activation`) and `snapshot v21 body kind: %w` (a container cut after its version byte); the tests `TestRestoreRejectsAnOversizedV21LengthPrefix`, `TestBootstrapParamsNeverCarryStage3`; the test helpers `plainSnapshotBytes(t *testing.T, f *FSM) []byte`, `v21Document(t *testing.T, data []byte) []byte`, `v21Stage3`, `v21Cutover`, `v21Update`, `stage3SwitchedFSM(t *testing.T) *FSM`, `fixedPromotionAck(t *testing.T, f *FSM, p uint64) wire.Command`, `const stage3V21PlainGoldenPath`; Plan S3-A's unexported `stateSizeSample` gaining an `envelopeBytes uint64` parameter (`stateSizeSample(source string, envelopeBytes, docBytes, spentIDsBytes uint64, components []StateComponentSize) StateSizeSample`); `rewriteSnapshotDocument` reading and writing the envelope of each side's version (a v21 output with the plain kind byte).

Notes:
- **Before the activation nothing changes.** `snapshotWriteVersion` tests `Params.Stage3` first and is otherwise the base function, `snapshotDoc.Stage3` is `omitempty` and nil, and `stage3FieldsPresent` holds for no container a pre-stage3 history writes, so every v14–v20 container stays byte-identical and restores as before (`TestSnapshotWritesV20UntilStage3IsCommitted`, `TestRestoreReadsEveryEarlierVersionUnchanged`, the frozen goldens). The one visible difference is the reader's: it accepts the version byte 21, which no pre-stage3 container carries. `ARBITER_TAG_P4` refuses a v21 container at its version byte with `unsupported snapshot version 21 (require 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 or 20); pre-v3 state requires a backup and coordinated re-genesis` (checked while drafting with an export of `526a721`); Task 67's offline replay checks both directions over the devnet2 voter copies.
- **Plain body only.** The writer always frames the plain body. `Persist` streams `Snapshot`'s bytes in this task; Task 67 makes it write the verified interned body in `auto` mode. Every v21 test of this task therefore reads the plain container through `writeSnapshot` (`plainSnapshotBytes`), never through `Persist`, and tests kinds 2 and 255 as unknown (kind 1 is Task 67's), so Task 67 edits none of them; `restoreInto` keeps working on either body.
- **Restore order.** `validateStage3State` runs inside `readSnapshotMeasured` right after `validateTrackState`, before `validateRestoredState`, so its refusals name the stage-3 rule; `validateConsensusHistory` later replays the signed updates and Task 64's Params-only rules. `validateStage3Activation` checks, in this order: state exactly with the parameter; the activation epoch against the first update that set `stage3` (none sets it: no epoch, 0 included, is the history's); the activation block and promotion within the state; every cutover names a switch that is on (a name that is no switch of spec §10.4 is not a switch that is on, so `stage3SwitchValue` never sees it and never panics); every switch that is on has a cutover; strict §10.4 order (which also refuses a repeated cutover); then per cutover its epoch (the first update whose `stage3` has the switch on; none: no epoch is the history's), its block and promotion not below the previous cutover's (the activation's for the first) and not beyond the state, and the incarnation seq rule (`1..len(Incarnations)+1`, the sequence the registry would assign next, for `purge` only). The upper bound is not in contract §46; it is added so that a forged cutover beyond the chain cannot disable the later restore rules that compare blocks with a cutover (Task 70's `validateStage3Selection`, Plan S3-E3's).
- **Why a dedicated golden network.** `stage3FSM` inherits `authorityFixture`'s random authority key (`fsm/promotion_test.go:16`), `siEntry`'s enrolment signed at the current time (`fsm/signed_claims_params_test.go:34-49`), and after the tracks activation the FSM keeps each source acknowledgement's token (`PendingPromotion.AckJWS`, `fsm/apply.go:932`), which `sourceAck` signs at the current time. `stage3V21GoldenFSM` therefore builds the same network as `stage3FSM` (claimsFSM's registry and seed, indexer 1's SNode `s1` and indexer 0's `s0` at activation block 150, verifiers `v1..v3`, `db0.x` added through the published transition block 1, the tracks activation, the stage-3 activation with `stage3ActivationParams`) under `legacyGoldenAuthorityHex`, `fixedEnrollmentEntry` and `fixedClient`, and signs each acknowledgement at `authoritytest.Iat`; it then passes one block of indexer 1 and one of indexer 0 (admitted, sealed, claimed, verified, final, promoted, acknowledged, published). The file is captured on the branch after Tasks 64 and 65, so whatever they commit for the activation (Task 65's registry-version bump included) is part of it; with every switch off Tasks 67–70 and Plans S3-E3 and S3-E4 must leave it byte-identical, which is why it is frozen. Drafted against stand-ins of Tasks 64 and 65, the container was 175302 bytes, and a capture and a later run were byte-identical.
- **Existing tests this task edits** (none is a golden or vector file): `rewriteSnapshotDocument` becomes envelope-aware, so Tasks 67–70 and Plan S3-E3 can forge v21 documents with it; `TestArtifactDispositionCapacityDuplicateAllowanceKeyRejectsBeforeStateMutation` hand-builds a 13-byte envelope for `snapshotVersion` and now builds each version's envelope (v20 added to its list); `TestRestoreRejectsOversizedLengthPrefix` moves from `snapshotVersion` to `snapshotVersionV20`, because under 21 its sixth byte is a kind byte (`TestRestoreRejectsAnOversizedV21LengthPrefix` covers v21); `TestSnapshotRefusesFrontierlessDrainingRecordAndAcceptsV9` pins the newest version, now 21; `TestSnapshotRefusesTheNextVersion` moves one version up, to 22. Each of these fails without its edit once `snapshotVersion` is 21 (checked while drafting). The read-cap case of Plan S3-A's `TestFailedRestoreKeepsThePreviousSample` moves to `snapshotVersionV20` too: under 21 it would still be refused, by the unknown body kind 255, so it would pass but no longer test the read cap.
- **Budget estimators.** The cross-plan budget rule and Plan S3-B §13.4's estimator read `AFSM`, the version byte and an 8-byte length at offset 5; from the activation a voter's `state.bin` has the kind byte at offset 5 and the length at offset 6. This plan runs no runbook; Plan S3-E4 Task 81's v21 runbook reads `snapshotEnvelopeBytes`'s framing.
- **Checked while drafting** (2026-10-16, Appendix B): every hunk and file below was applied, in step order, to a `git archive` export of arbiter `526a721` carrying stand-ins of Tasks 64 and 65 (their drafted fsm code, Task 64's pin-fallout test map and Task 65's README section), built against stand-ins of `PROTO_TAG_P5` (contract §39, regenerated) and `CORE_TAG_P5` (contract §41's types, wire converters and verifier normalization) through `replace` directives, with `GOWORK=off GOFLAGS=-mod=mod GOPROXY=off`. A script applied every Replace/with pair of Steps 3, 5, 6, 7, 9 and 12 to the base files and reproduced the tested tree byte for byte. Observed: Step 4's build failure, Step 10's single golden failure (21 other tests passing), Step 11's capture (`65 70 83 77 21 0`) and its later pass, `gofmt` and `go vet ./...` clean, `go test -race ./...` passing, no `time.Now` in `fsm/` and no arbiter-proto import in the fsm library; 20 single-point mutations each failing a named test (the verifiers-field predicate, the bootstrap and signed-update halves of `stage3FieldsPresent`, the pre-v21 refusal, the kind check, the writer's version choice, `snapshotDocument`'s `Stage3`, `snapshotEnvelopeBytes`, the state-exactly-with-parameter rule, both epoch-0 cases, the switch-is-on rule, the order, the precedes, upper-bound and activation-bound checks, the incarnation-seq lower bound, the bootstrap refusal, and both state-size envelopes); the `526a721` export refuses the captured container with the text above. Gazelle ran offline (Go modules served from the local module cache) and wrote exactly Step 13's `fsm/BUILD.bazel` lines; `bazel build` and `bazel test` were not run, because the P5 modules exist only as local stand-ins.

- [ ] **Step 1: Base check**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; git -C "$W" rev-parse --abbrev-ref HEAD && git -C "$W" status --porcelain && git -C "$W" log -1 --format=%s && test ! -e "$W/fsm/testdata/stage3_v21_plain.snapshot" && test ! -e "$W/fsm/stage3_restore.go" && test ! -e "$W/fsm/snapshot_v21_test.go" && cd "$W" && grep -c '^func containerStateSize' fsm/state_size.go && grep -c '^func activateStage3(' fsm/stage3_fixture_test.go && grep -c '^func turnOnStage3Switch(' fsm/stage3_fixture_test.go && grep -c 'snapshotVersion       = 20$' fsm/snapshot.go && GOWORK=off go test ./fsm/ -count=1`

Expected: `urwt/claude/multi-source-s3e`, an empty status, the subject of Task 65's last commit (it ends with `(Task 65)`), `1`, `1`, `1`, `1`, and `ok  	github.com/sentioxyz/arbiter/fsm`. Stop if HEAD is not Task 65's last commit, the status is not empty, any of the three files already exists, or `snapshotVersion` is not 20.

- [ ] **Step 2: Write the failing v21 tests**

Create `fsm/snapshot_v21_test.go`:

```go
package fsm

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
)

const stage3V21PlainGoldenPath = "testdata/stage3_v21_plain.snapshot"

// plainSnapshotBytes is the container Snapshot captures under the read lock,
// written by writeSnapshot: for v21 always the plain body, whatever body
// Persist streams to the sink.
func plainSnapshotBytes(t *testing.T, f *FSM) []byte {
	t.Helper()
	f.mu.RLock()
	defer f.mu.RUnlock()
	var buf bytes.Buffer
	if err := writeSnapshot(&buf, f.st); err != nil {
		t.Fatalf("writeSnapshot: %v", err)
	}
	return buf.Bytes()
}

// v21Document is the plain body of a v21 container: the 14-byte envelope is
// AFSM, the version byte 21, the body kind 0 and the 8-byte big-endian body
// length (contract §46).
func v21Document(t *testing.T, data []byte) []byte {
	t.Helper()
	if len(data) < snapshotV21EnvelopePrefixBytes || string(data[:4]) != "AFSM" || data[4] != snapshotVersionV21 || data[5] != snapshotBodyPlain {
		t.Fatalf("not a plain v21 container: % x", data[:min(len(data), snapshotV21EnvelopePrefixBytes)])
	}
	n := binary.BigEndian.Uint64(data[6:snapshotV21EnvelopePrefixBytes])
	if n > uint64(len(data)-snapshotV21EnvelopePrefixBytes) {
		t.Fatalf("v21 body length %d exceeds the container", n)
	}
	return data[snapshotV21EnvelopePrefixBytes : snapshotV21EnvelopePrefixBytes+n]
}

func v21Stage3(doc map[string]any) map[string]any { return doc["stage3"].(map[string]any) }

func v21Cutover(doc map[string]any, i int) map[string]any {
	return v21Stage3(doc)["switches"].([]any)[i].(map[string]any)
}

func v21Update(doc map[string]any, i int) map[string]any {
	return doc["consensus_updates"].([]any)[i].(map[string]any)["update"].(map[string]any)
}

// stage3SwitchedFSM is stage3FSM with the first two switches of spec §10.4
// turned on, purge and then eviction_voiding, each in its own update.
func stage3SwitchedFSM(t *testing.T) *FSM {
	t.Helper()
	f, a := stage3FSM(t)
	turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
	turnOnStage3Switch(t, f, a, Stage3SwitchEvictionVoiding)
	if s3 := f.st.Stage3; len(s3.Switches) != 2 || s3.Switches[0].Switch != Stage3SwitchPurge || s3.Switches[1].Switch != Stage3SwitchEvictionVoiding {
		t.Fatalf("stage3 state %+v, want the purge and eviction_voiding cutovers", s3)
	}
	return f
}

// fixedPromotionAck is partPromotionAck signed at authoritytest.Iat:
// after the tracks activation the FSM keeps the source's acknowledgement
// token with the promotion, so a reproducible container needs a fixed iat.
func fixedPromotionAck(t *testing.T, f *FSM, p uint64) wire.Command {
	t.Helper()
	cmd := partPromotionAck(t, f, p)
	ack := cmd.RecordPromotionAck.Ack
	jws, err := snodeSigner(t, ack.NodeID).SignSNodeMessageAt(authority.SNodeMessagePromotionAck, messageContextFor(t, f), ack, authoritytest.Iat)
	if err != nil {
		t.Fatal(err)
	}
	cmd.RecordPromotionAck.SourceJWS = jws
	return cmd
}

// stage3V21GoldenFSM is stage3FSM's network built reproducibly, plus one
// promoted, acknowledged and published block of each owner after the stage-3
// activation. The authority key is legacyGoldenAuthorityHex, each enrolment is
// signed at authoritytest.Iat (fixedEnrollmentEntry), the client is
// fixedClient, and each source acknowledgement is signed at
// authoritytest.Iat; every other signature the fixture stores (consensus
// updates at iat 1, promotions at authoritytest.Iat, the client's statement
// tokens, ed25519 evidence) is already fixed. Two runs therefore write the
// same container byte for byte.
func stage3V21GoldenFSM(t *testing.T) *FSM {
	t.Helper()
	a, err := authority.NewSignerFromHex(legacyGoldenAuthorityHex)
	if err != nil {
		t.Fatal(err)
	}
	params := testParams()
	params.AuthorityAddresses = []string{a.Address()}
	f := mustNewFSM(t, params)
	// claimsFSM with the fixed authority.
	registry := registryUpdate(t, f, params, testRegistryParams())
	registry.MaxWriters = 2
	mustApply(t, f, signedConsensusUpdate(t, a, registry))
	seed(t, f, arbiter.LegacyTable{DatabaseID: "db", TableID: "old", Created: ev(5, "0x5", 0)})
	baseTransitionOn(t, f)
	// tracksReadyFSM with enrolments signed at a fixed iat.
	registerActive(t, f, "s0", arbiter.NodeRoleSNode)
	mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f, []arbiter.SIIndexerEntry{
		fixedEnrollmentEntry(t, f, snodeSigner(t, "s1"), 1, 100, "s1"),
		fixedEnrollmentEntry(t, f, snodeSigner(t, "s0"), 0, 150, "s0"),
	}, testVerifiers("v1", "v2", "v3"))))
	mustApply(t, f, advanceTo(149))
	mustApply(t, f, seedIndexerCmd(0, 149))
	mustApply(t, f, ownedAddCmd(t, f, "db0", "x", ev(150, "0x150", 0), ev(151, "0x151", 0), 0))
	transition := mustSeal(t, f).BlockSeq
	verifyTransition(t, f, transition)
	anchorFinal(t, f, transition)
	publishManifestInputs(t, f)
	activateTracks(t, f, a)
	activateStage3(t, f, a)
	// One block of each owner, promoted, acknowledged and published.
	key, account := fixedClient(t)
	for i, owner := range []uint64{1, 0} {
		stmt := admitTrack(t, f, key, account, uint64(i+1), owner)
		block := mustSeal(t, f).BlockSeq
		bindTrackClaim(t, f, stmt)
		verifyBlock(t, f, block)
		anchorFinal(t, f, block)
		mustApply(t, f, fixedPromotionAck(t, f, promoteTrack(t, f, a, stmt)))
		publishManifestInputs(t, f)
	}
	if !f.Stage3Active() || len(f.st.Blocks) != 3 || f.st.PromotionSeq != 2 || f.st.SafeWatermark.SafeBlockSeq != 3 {
		t.Fatalf("golden fixture: stage3 %v, %d blocks, promotion %d, safe watermark %+v", f.Stage3Active(), len(f.st.Blocks), f.st.PromotionSeq, f.st.SafeWatermark)
	}
	return f
}

// TestSnapshotWritesV20UntilStage3IsCommitted: until stage3 is committed the
// writer keeps v20 byte for byte, with no stage-3 member, and a refused
// activation leaves it so; the activation alone selects v21 (contract §44).
func TestSnapshotWritesV20UntilStage3IsCommitted(t *testing.T) {
	f, a := tracksFSM(t)
	before := snapshotBytes(t, f)
	if before[4] != snapshotVersionV20 || snapshotWriteVersion(f.st) != snapshotVersionV20 {
		t.Fatalf("tracks without stage3: version byte %d, writer version %d, want 20", before[4], snapshotWriteVersion(f.st))
	}
	for _, key := range []string{`"stage3":`, `"host_indexer_id":`, `"replica_signer":`} {
		if bytes.Contains(before, []byte(key)) {
			t.Fatalf("a pre-stage3 container carries %s", key)
		}
	}
	if got := snapshotBytes(t, restoreInto(t, before)); !bytes.Equal(got, before) {
		t.Fatal("restore ∘ snapshot of the v20 container is not the identity")
	}
	refused := tracksUpdate(t, f)
	refused.Stage3 = stage3ActivationParams()
	refused.Stage3.Purge = true
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, refused),
		"consensus update setting stage3 must leave every switch off, strict_copies false and min_safe_copies 1")
	if got := snapshotBytes(t, f); !bytes.Equal(got, before) || got[4] != snapshotVersionV20 {
		t.Fatal("a refused activation changed the v20 container")
	}
	activateStage3(t, f, a)
	if got := plainSnapshotBytes(t, f); got[4] != snapshotVersionV21 || snapshotWriteVersion(f.st) != snapshotVersionV21 {
		t.Fatalf("after the activation: version byte %d, want 21", got[4])
	}
}

// TestSnapshotWritesAV21PlainContainerOnceStage3IsCommitted: from the
// activation the writer frames v21 (AFSM, 21, body kind 0, length, body,
// spent-ids) with the plain body, which is the v20 document plus its last
// member stage3; the container restores, bare and configured, to the same
// stage-3 state and re-encodes identically.
func TestSnapshotWritesAV21PlainContainerOnceStage3IsCommitted(t *testing.T) {
	f, _ := stage3FSM(t)
	b := plainSnapshotBytes(t, f)
	doc := v21Document(t, b)
	stage3, err := json.Marshal(f.st.Stage3)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(doc, []byte(`,"stage3":`+string(stage3)+`}`)) {
		t.Fatalf("the plain body does not end with the stage3 member %s", stage3)
	}
	var fields, params map[string]json.RawMessage
	if err := json.Unmarshal(doc, &fields); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fields["params"], &params); err != nil {
		t.Fatal(err)
	}
	if _, ok := params["stage3"]; !ok {
		t.Fatal("the v21 body carries no committed stage3 parameter")
	}
	restored := restoreInto(t, b)
	if !restored.Stage3Active() || !reflect.DeepEqual(restored.st.Stage3, f.st.Stage3) || !reflect.DeepEqual(restored.st.Params.Stage3, f.st.Params.Stage3) {
		t.Fatalf("restored stage3 state %+v, parameter %+v", restored.st.Stage3, restored.st.Params.Stage3)
	}
	if again := plainSnapshotBytes(t, restored); !bytes.Equal(again, b) {
		t.Fatal("restore ∘ snapshot of the v21 container is not the identity")
	}
	if _, err := restoreConfigured(t, f, b); err != nil {
		t.Fatalf("configured restore: %v", err)
	}
}

// TestStage3V21PlainContainerGolden freezes the plain v21 container of
// stage3V21GoldenFSM. It is captured once, under
// ARBITER_CAPTURE_STAGE3_V21_PLAIN_GOLDEN=1, and never rewritten: a
// difference means stage-3 state or its encoding changed with every switch
// off.
func TestStage3V21PlainContainerGolden(t *testing.T) {
	got := plainSnapshotBytes(t, stage3V21GoldenFSM(t))
	if os.Getenv("ARBITER_CAPTURE_STAGE3_V21_PLAIN_GOLDEN") == "1" {
		if _, err := os.Stat(stage3V21PlainGoldenPath); err == nil {
			t.Fatalf("%s exists; it is captured once and never rewritten", stage3V21PlainGoldenPath)
		}
		if err := os.WriteFile(stage3V21PlainGoldenPath, got, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Skip("captured; commit the file")
	}
	want, err := os.ReadFile(stage3V21PlainGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the stage-3 golden network writes a different v21 container (%d bytes, golden %d)", len(got), len(want))
	}
	v21Document(t, want)
	restored := restoreInto(t, want)
	if !restored.Stage3Active() {
		t.Fatal("the golden container restored without stage3")
	}
	if again := plainSnapshotBytes(t, restored); !bytes.Equal(again, want) {
		t.Fatal("the restored golden container re-encodes differently")
	}
}

// TestRestoreReadsEveryEarlierVersionUnchanged: this binary still restores
// every container version before v21. Each container the writer produces
// before the activation (v14 to v20) keeps its version byte, carries no
// stage-3 member and re-encodes byte for byte; the frozen v3 container
// restores; and every version byte 4 to 13 still reaches the document.
func TestRestoreReadsEveryEarlierVersionUnchanged(t *testing.T) {
	script, scriptParams := lifecycleScript(t)
	v17 := mustNewFSM(t, scriptParams)
	for _, b := range script {
		v17.Apply(&raft.Log{Data: b})
	}
	lanes, _, _ := lanesFSM(t, 256)
	claims, _ := ownerFSMWithAuthority(t)
	full, _, _ := tmV20Fixture(t)
	for _, tc := range []struct {
		name string
		f    *FSM
		want byte
	}{
		{"registry disabled", newTestFSM(t), snapshotVersionV14},
		{"registry seeded", seededRegistryFSM(t), snapshotVersionV15},
		{"transition published", publishedTransitionFSM(t), snapshotVersionV16},
		{"v17 registry", v17, snapshotVersionV17},
		{"lanes active", lanes, snapshotVersionV18},
		{"signed claims", claims, snapshotVersionV19},
		{"every v20 member", full.f, snapshotVersionV20},
	} {
		b := snapshotBytes(t, tc.f)
		if b[4] != tc.want {
			t.Fatalf("%s: version byte = %d, want %d", tc.name, b[4], tc.want)
		}
		if bytes.Contains(b, []byte(`"stage3":`)) {
			t.Fatalf("%s: a pre-stage3 container carries a stage3 member", tc.name)
		}
		if got := snapshotBytes(t, restoreInto(t, b)); !bytes.Equal(got, b) {
			t.Fatalf("%s: restore ∘ snapshot is not the identity", tc.name)
		}
	}
	// The frozen v3 container (testdata/consensus-v3-authority.snapshot):
	// loadLegacyConsensusFixture fails the test unless it restores.
	loadLegacyConsensusFixture(t)
	v14 := snapshotBytes(t, newTestFSM(t))
	for version := byte(snapshotVersionV4); version < snapshotVersionV14; version++ {
		old := bytes.Clone(v14)
		old[4] = version
		if err := restoreErr(t, old); err != nil {
			t.Fatalf("a v%d container: %v", version, err)
		}
	}
}

// TestRestoreRefusesStage3FieldsInAPreV21Container: any stage-3 member under
// an older version byte is a forged field, not a migration: the parameter in
// the current or bootstrap parameters or in a signed update, the state even
// when empty, and every verifiers field beyond node_id and ed25519_pubkey.
func TestRestoreRefusesStage3FieldsInAPreV21Container(t *testing.T) {
	const want = "stage3 state is present in a pre-v21 container"
	f, _ := stage3FSM(t)
	relabelled := rewriteSnapshotDocument(t, plainSnapshotBytes(t, f), snapshotVersionV20, keepSnapshotDocument)
	if err := restoreErr(t, relabelled); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("v21 relabelled v20: %v", err)
	}
	tracks := snapshotBytes(t, newTrackManifestFixture(t).f)
	claimsFSM, _ := ownerFSMWithAuthority(t)
	claims := snapshotBytes(t, claimsFSM)
	if tracks[4] != snapshotVersionV20 || claims[4] != snapshotVersionV19 {
		t.Fatalf("fixture version bytes %d and %d, want 20 and 19", tracks[4], claims[4])
	}
	last := func(doc map[string]any) map[string]any {
		return v21Update(doc, len(doc["consensus_updates"].([]any))-1)
	}
	for name, mutate := range map[string]func(map[string]any){
		"params stage3": func(doc map[string]any) {
			v20Params(doc)["stage3"] = map[string]any{"group_size": 3, "min_safe_copies": 1}
		},
		"bootstrap stage3":   func(doc map[string]any) { doc["bootstrap_params"].(map[string]any)["stage3"] = map[string]any{} },
		"empty stage3 state": func(doc map[string]any) { doc["stage3"] = map[string]any{} },
		"update stage3":      func(doc map[string]any) { last(doc)["stage3"] = map[string]any{} },
	} {
		forged := rewriteSnapshotDocument(t, tracks, snapshotVersionV20, mutate)
		if _, err := readSnapshot(bytes.NewReader(forged)); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s under v20: readSnapshot = %v, want %q", name, err, want)
		}
	}
	if verifierEntryHasStage3Fields(testVerifiers("v1")[0]) {
		t.Fatal("a pool entry with only node_id and ed25519_pubkey counts as stage-3")
	}
	entry := reflect.TypeFor[arbiter.VerifierEntry]()
	for i := range entry.NumField() {
		name := strings.Split(entry.Field(i).Tag.Get("json"), ",")[0]
		if name == "node_id" || name == "ed25519_pubkey" {
			continue
		}
		var value any
		switch entry.Field(i).Type.Kind() {
		case reflect.Pointer:
			value = 0 // host_indexer_id 0 is a host binding too
		case reflect.String:
			value = "x"
		case reflect.Uint32:
			value = 1
		case reflect.Bool:
			value = true
		default:
			t.Fatalf("verifiers field %s has kind %s: extend this test and verifierEntryHasStage3Fields", name, entry.Field(i).Type.Kind())
		}
		raw, err := json.Marshal(map[string]any{"node_id": "v1", name: value})
		if err != nil {
			t.Fatal(err)
		}
		var v arbiter.VerifierEntry
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
		if !verifierEntryHasStage3Fields(v) {
			t.Fatalf("verifierEntryHasStage3Fields misses %s", name)
		}
		for _, data := range [][]byte{tracks, claims} {
			for where, entryOf := range map[string]func(map[string]any) map[string]any{
				"params": func(doc map[string]any) map[string]any {
					return v20Params(doc)["verifiers"].([]any)[0].(map[string]any)
				},
				"signed update": func(doc map[string]any) map[string]any { return last(doc)["verifiers"].([]any)[0].(map[string]any) },
			} {
				forged := rewriteSnapshotDocument(t, data, data[4], func(doc map[string]any) { entryOf(doc)[name] = value })
				if _, err := readSnapshot(bytes.NewReader(forged)); err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("%s %s under v%d: readSnapshot = %v, want %q", where, name, data[4], err, want)
				}
			}
		}
	}
}

// TestRestoreRefusesAnUnknownV21BodyKind: the byte after a v21 version byte
// names the body; this binary reads the plain body (kind 0) and refuses
// every kind it does not know, so a corrupt kind byte never decodes as plain.
func TestRestoreRefusesAnUnknownV21BodyKind(t *testing.T) {
	f, _ := stage3FSM(t)
	plain := plainSnapshotBytes(t, f)
	for _, kind := range []byte{2, 0xFF} {
		forged := bytes.Clone(plain)
		forged[5] = kind
		if err := restoreErr(t, forged); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("unsupported snapshot v21 body kind %d", kind)) {
			t.Fatalf("body kind %d: %v", kind, err)
		}
	}
	if err := restoreErr(t, plain[:5]); err == nil || !strings.Contains(err.Error(), "snapshot v21 body kind") {
		t.Fatalf("a container cut after its version byte: %v", err)
	}
}

// TestRestoreRejectsAnOversizedV21LengthPrefix: the read cap applies to the
// v21 body length, read after the kind byte, before anything is allocated.
func TestRestoreRejectsAnOversizedV21LengthPrefix(t *testing.T) {
	buf := []byte{'A', 'F', 'S', 'M', snapshotVersionV21, snapshotBodyPlain, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	g := newTestFSM(t)
	if err := g.Restore(io.NopCloser(bytes.NewReader(buf))); err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("an oversized v21 length prefix must error before allocating, got %v", err)
	}
}

// TestBootstrapParamsNeverCarryStage3: stage3 is set only by a signed update,
// so a v21 container whose bootstrap parameters carry it is refused.
func TestBootstrapParamsNeverCarryStage3(t *testing.T) {
	f, _ := stage3FSM(t)
	forged := rewriteSnapshotDocument(t, plainSnapshotBytes(t, f), snapshotVersionV21, func(doc map[string]any) {
		doc["bootstrap_params"].(map[string]any)["stage3"] = v20Params(doc)["stage3"]
	})
	if err := restoreErr(t, forged); err == nil || !strings.Contains(err.Error(), "snapshot bootstrap consensus params: stage3 is set only by a consensus update, not at genesis") {
		t.Fatalf("restore = %v", err)
	}
}

// TestValidateStage3StateRequiresStateExactlyWithTheParameter: the stage-3
// state exists exactly when the parameter is committed; a pre-stage3
// container under the v21 byte reads, and gains nothing.
func TestValidateStage3StateRequiresStateExactlyWithTheParameter(t *testing.T) {
	const want = "stage3 state must exist exactly when stage3 is committed"
	f, _ := stage3FSM(t)
	plain := plainSnapshotBytes(t, f)
	tracks := rewriteSnapshotDocument(t, snapshotBytes(t, newTrackManifestFixture(t).f), snapshotVersionV21, keepSnapshotDocument)
	for name, data := range map[string][]byte{"stage3": plain, "pre-stage3 under v21": tracks} {
		st, err := readSnapshot(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("%s: unedited container refused: %v", name, err)
		}
		if (st.Stage3 != nil) != (name == "stage3") {
			t.Fatalf("%s: restored stage3 state %+v", name, st.Stage3)
		}
	}
	for _, tc := range []struct {
		name   string
		data   []byte
		mutate func(map[string]any)
	}{
		{"parameter without state", plain, func(doc map[string]any) { delete(doc, "stage3") }},
		{"state without parameter", plain, func(doc map[string]any) { delete(v20Params(doc), "stage3") }},
		{"state before the activation", tracks, func(doc map[string]any) { doc["stage3"] = map[string]any{} }},
	} {
		forged := rewriteSnapshotDocument(t, tc.data, snapshotVersionV21, tc.mutate)
		if _, err := readSnapshot(bytes.NewReader(forged)); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: readSnapshot = %v, want %q", tc.name, err, want)
		}
	}
}

// TestValidateStage3StateRefusesAnActivationEpochThatIsNotTheHistorys: the
// activation epoch is the epoch of the first signed update that set stage3
// (a history in which no update sets it has none), and the activation block
// and promotion lie within the state.
func TestValidateStage3StateRefusesAnActivationEpochThatIsNotTheHistorys(t *testing.T) {
	f := stage3SwitchedFSM(t)
	plain := plainSnapshotBytes(t, f)
	s3 := f.st.Stage3
	activation := int(s3.ActivationEpoch)
	if _, err := readSnapshot(bytes.NewReader(plain)); err != nil {
		t.Fatalf("unedited container refused: %v", err)
	}
	// stage3FSM's activation is its last update: without it no update sets
	// stage3, and no activation epoch, 0 included, is the history's.
	only, _ := stage3FSM(t)
	for _, epoch := range []uint64{only.st.Stage3.ActivationEpoch, 0} {
		unset := rewriteSnapshotDocument(t, plainSnapshotBytes(t, only), snapshotVersionV21, func(doc map[string]any) {
			delete(v21Update(doc, int(only.st.Stage3.ActivationEpoch)-1), "stage3")
			v21Stage3(doc)["activation_epoch"] = epoch
		})
		if _, err := readSnapshot(bytes.NewReader(unset)); err == nil || !strings.Contains(err.Error(),
			fmt.Sprintf("stage3 activation epoch %d is not the epoch of the first update that set stage3 (0)", epoch)) {
			t.Fatalf("no update sets stage3, activation epoch %d: readSnapshot = %v", epoch, err)
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{"a later epoch", func(doc map[string]any) { v21Stage3(doc)["activation_epoch"] = activation + 1 },
			fmt.Sprintf("stage3 activation epoch %d is not the epoch of the first update that set stage3 (%d)", activation+1, activation)},
		{"an earlier epoch", func(doc map[string]any) { v21Stage3(doc)["activation_epoch"] = activation - 1 },
			fmt.Sprintf("stage3 activation epoch %d is not the epoch of the first update that set stage3 (%d)", activation-1, activation)},
		{"the activation update without stage3", func(doc map[string]any) { delete(v21Update(doc, activation-1), "stage3") },
			fmt.Sprintf("stage3 activation epoch %d is not the epoch of the first update that set stage3 (%d)", activation, activation+1)},
		{"a block beyond the chain", func(doc map[string]any) { v21Stage3(doc)["activation_block_seq"] = 99 },
			fmt.Sprintf("stage3 activation block 99 or promotion %d is beyond the state's (%d, %d)", s3.ActivationPromotionSeq, len(f.st.Blocks), f.st.PromotionSeq)},
		{"a promotion beyond the sequence", func(doc map[string]any) { v21Stage3(doc)["activation_promotion_seq"] = 99 },
			fmt.Sprintf("stage3 activation block %d or promotion 99 is beyond the state's (%d, %d)", s3.ActivationBlockSeq, len(f.st.Blocks), f.st.PromotionSeq)},
	} {
		forged := rewriteSnapshotDocument(t, plain, snapshotVersionV21, tc.mutate)
		if _, err := readSnapshot(bytes.NewReader(forged)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: readSnapshot = %v, want %q", tc.name, err, tc.want)
		}
	}
}

// TestValidateStage3StateRefusesSwitchCutoversThatAreNotTheHistorys: there is
// one cutover per switch that is on and none for a switch that is off, in the
// order of spec §10.4, each at the epoch of the update that turned its switch
// on, none before the previous cutover or the activation or beyond the
// state, and only the purge cutover carries an incarnation seq, one the
// registry could have assigned next.
func TestValidateStage3StateRefusesSwitchCutoversThatAreNotTheHistorys(t *testing.T) {
	f := stage3SwitchedFSM(t)
	plain := plainSnapshotBytes(t, f)
	s3 := f.st.Stage3
	purge, voiding := s3.Switches[0], s3.Switches[1]
	blocks, incarnations := len(f.st.Blocks), len(f.st.TableRegistry.Incarnations)
	if purge.IncarnationSeq != uint64(incarnations)+1 || voiding.IncarnationSeq != 0 {
		t.Fatalf("cutovers %+v", s3.Switches)
	}
	if _, err := readSnapshot(bytes.NewReader(plain)); err != nil {
		t.Fatalf("unedited container refused: %v", err)
	}
	appendCutover := func(doc map[string]any, c map[string]any) {
		v21Stage3(doc)["switches"] = append(v21Stage3(doc)["switches"].([]any), c)
	}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{"a cutover of a switch that is off", func(doc map[string]any) {
			appendCutover(doc, map[string]any{"switch": Stage3SwitchOwnerAwareSelection, "epoch": voiding.Epoch, "block_seq": voiding.BlockSeq, "promotion_seq": voiding.PromotionSeq})
		}, "stage3 records a cutover for switch owner_aware_selection, which is off"},
		{"a cutover of no switch", func(doc map[string]any) {
			appendCutover(doc, map[string]any{"switch": "nope", "epoch": voiding.Epoch, "block_seq": voiding.BlockSeq, "promotion_seq": voiding.PromotionSeq})
		}, "stage3 records a cutover for switch nope, which is off"},
		{"a switch without its cutover", func(doc map[string]any) {
			v21Stage3(doc)["switches"] = v21Stage3(doc)["switches"].([]any)[:1]
		}, "stage3 switch eviction_voiding is on without a cutover record"},
		{"cutovers out of order", func(doc map[string]any) {
			list := v21Stage3(doc)["switches"].([]any)
			list[0], list[1] = list[1], list[0]
		}, "stage3 switch cutovers are not in the order of spec §10.4"},
		{"a cutover recorded twice", func(doc map[string]any) {
			list := v21Stage3(doc)["switches"].([]any)
			v21Stage3(doc)["switches"] = []any{list[0], list[0], list[1]}
		}, "stage3 switch cutovers are not in the order of spec §10.4"},
		{"a cutover at another epoch", func(doc map[string]any) { v21Cutover(doc, 1)["epoch"] = voiding.Epoch + 1 },
			fmt.Sprintf("stage3 switch eviction_voiding cutover epoch %d is not the epoch of the update that turned it on (%d)", voiding.Epoch+1, voiding.Epoch)},
		{"a switch no update turned on", func(doc map[string]any) {
			v21Update(doc, int(voiding.Epoch)-1)["stage3"].(map[string]any)["eviction_voiding"] = false
		}, fmt.Sprintf("stage3 switch eviction_voiding cutover epoch %d is not the epoch of the update that turned it on (0)", voiding.Epoch)},
		{"a switch no update turned on, at epoch 0", func(doc map[string]any) {
			v21Update(doc, int(voiding.Epoch)-1)["stage3"].(map[string]any)["eviction_voiding"] = false
			v21Cutover(doc, 1)["epoch"] = 0
		}, "stage3 switch eviction_voiding cutover epoch 0 is not the epoch of the update that turned it on (0)"},
		{"a cutover before the activation", func(doc map[string]any) { v21Cutover(doc, 0)["block_seq"] = s3.ActivationBlockSeq - 1 },
			fmt.Sprintf("stage3 switch purge cutover (block %d, promotion %d) precedes the previous cutover or the activation", s3.ActivationBlockSeq-1, purge.PromotionSeq)},
		{"a cutover before the previous one", func(doc map[string]any) {
			v21Stage3(doc)["activation_block_seq"] = 0
			v21Cutover(doc, 1)["block_seq"] = 0
		}, fmt.Sprintf("stage3 switch eviction_voiding cutover (block 0, promotion %d) precedes the previous cutover or the activation", voiding.PromotionSeq)},
		{"a cutover beyond the chain", func(doc map[string]any) { v21Cutover(doc, 1)["block_seq"] = 99 },
			fmt.Sprintf("stage3 switch eviction_voiding cutover (block 99, promotion %d) is beyond the state's (%d, %d)", voiding.PromotionSeq, blocks, f.st.PromotionSeq)},
		{"an incarnation seq on another switch", func(doc map[string]any) { v21Cutover(doc, 1)["incarnation_seq"] = 1 },
			"stage3 switch eviction_voiding cutover carries an incarnation seq"},
		{"a purge cutover without an incarnation seq", func(doc map[string]any) { delete(v21Cutover(doc, 0), "incarnation_seq") },
			fmt.Sprintf("stage3 purge cutover incarnation seq 0 is outside 1..%d", incarnations+1)},
		{"a purge cutover beyond the next incarnation", func(doc map[string]any) { v21Cutover(doc, 0)["incarnation_seq"] = incarnations + 2 },
			fmt.Sprintf("stage3 purge cutover incarnation seq %d is outside 1..%d", incarnations+2, incarnations+1)},
	} {
		forged := rewriteSnapshotDocument(t, plain, snapshotVersionV21, tc.mutate)
		if _, err := readSnapshot(bytes.NewReader(forged)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: readSnapshot = %v, want %q", tc.name, err, tc.want)
		}
	}
}

// TestContainerStateSizeMeasuresAV21PlainContainer: the state-size sample of
// a v21 container reads its 14-byte envelope: the document is the plain
// body, the stage3 member is a component of its own, and the writer's and
// the restore's samples agree.
func TestContainerStateSizeMeasuresAV21PlainContainer(t *testing.T) {
	f, _ := stage3FSM(t)
	plain := plainSnapshotBytes(t, f)
	doc := v21Document(t, plain)
	n, tail := uint64(len(doc)), uint64(len(plain)-snapshotV21EnvelopePrefixBytes-len(doc))
	got, err := containerStateSize(plain)
	if err != nil {
		t.Fatalf("containerStateSize: %v", err)
	}
	if got.Source != StateSizeSourceSnapshot || got.DocBytes != n || got.SpentIDsBytes != tail || got.ContainerBytes != uint64(len(plain)) || got.StateBytes != n+tail {
		t.Fatalf("sample = %+v for a %d-byte v21 container with a %d-byte body", got, len(plain), n)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(doc, &fields); err != nil {
		t.Fatal(err)
	}
	stage3 := got.Components[len(got.Components)-2]
	if stage3.Component != "stage3" || stage3.Bytes != uint64(len(`"stage3":`)+len(fields["stage3"])) {
		t.Fatalf("stage3 component = %+v", stage3)
	}
	snapshotBytes(t, f)
	persisted, ok := f.StateSizeSample()
	if !ok || !reflect.DeepEqual(persisted, got) {
		t.Fatalf("Persist sampled %+v, want the plain container's %+v", persisted, got)
	}
	restored, ok := restoreInto(t, plain).StateSizeSample()
	got.Source = StateSizeSourceRestore
	if !ok || !reflect.DeepEqual(restored, got) {
		t.Fatalf("restore sample = %+v, want %+v", restored, got)
	}
	if _, err := containerStateSize(plain[:snapshotEnvelopePrefixBytes]); err == nil || !strings.Contains(err.Error(), "shorter than its 14-byte envelope") {
		t.Fatalf("a v21 container cut at 13 bytes: %v", err)
	}
}

// TestCountSnapshotEncodingMatchesTheV21Writer: the counter mirrors the v21
// writer, the stage3 member included, and charges the 14-byte envelope; a
// v20 state still charges 13 bytes.
func TestCountSnapshotEncodingMatchesTheV21Writer(t *testing.T) {
	f, _ := stage3FSM(t)
	dims, err := CountSnapshotEncoding(f.st)
	if err != nil {
		t.Fatal(err)
	}
	n := uint64(len(v21Document(t, plainSnapshotBytes(t, f))))
	if dims.JSONBytes != n || dims.SnapshotEnvelopeBytes != n+snapshotV21EnvelopePrefixBytes {
		t.Fatalf("v21 dimensions = %+v, body %d", dims, n)
	}
	g, _ := tracksFSM(t)
	dims, err = CountSnapshotEncoding(g.st)
	if err != nil {
		t.Fatal(err)
	}
	v20 := snapshotBytes(t, g)
	if n := binary.BigEndian.Uint64(v20[5:13]); dims.JSONBytes != n || dims.SnapshotEnvelopeBytes != n+snapshotEnvelopePrefixBytes {
		t.Fatalf("v20 dimensions = %+v, document %d", dims, n)
	}
}
```

- [ ] **Step 3: Adapt the existing tests that frame containers by hand**

In `fsm/apply_artifact_disposition_test.go` (`:721-747`), make `rewriteSnapshotDocument` read and write each side's envelope.

Replace

```go
func rewriteSnapshotDocument(t *testing.T, data []byte, version byte, mutate func(map[string]any)) []byte {
	t.Helper()
	const header = 13
	if len(data) < header {
		t.Fatal("short snapshot")
	}
	n := binary.BigEndian.Uint64(data[5:header])
	if n > uint64(len(data)-header) {
		t.Fatal("invalid snapshot document length")
	}
	var doc map[string]any
	if err := json.Unmarshal(data[header:header+int(n)], &doc); err != nil {
		t.Fatal(err)
	}
	mutate(doc)
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, header+len(encoded)+len(data[header+int(n):]))
	copy(out[:5], data[:5])
	out[4] = version
	binary.BigEndian.PutUint64(out[5:header], uint64(len(encoded)))
	copy(out[header:], encoded)
	copy(out[header+len(encoded):], data[header+int(n):])
	return out
}
```

with

```go
// rewriteSnapshotDocument decodes data's document, applies mutate, and
// writes it back under version. The input and the output envelopes follow
// their own version bytes: 13 bytes for v3-v20, 14 for v21, whose body-kind
// byte the output sets to plain (contract §46).
func rewriteSnapshotDocument(t *testing.T, data []byte, version byte, mutate func(map[string]any)) []byte {
	t.Helper()
	if len(data) < 5 {
		t.Fatal("short snapshot")
	}
	in, header := int(snapshotEnvelopeBytes(data[4])), int(snapshotEnvelopeBytes(version))
	if len(data) < in {
		t.Fatal("short snapshot")
	}
	if in == snapshotV21EnvelopePrefixBytes && data[5] != snapshotBodyPlain {
		t.Fatalf("rewriteSnapshotDocument reads a plain body only, got body kind %d", data[5])
	}
	n := binary.BigEndian.Uint64(data[in-8 : in])
	if n > uint64(len(data)-in) {
		t.Fatal("invalid snapshot document length")
	}
	var doc map[string]any
	if err := json.Unmarshal(data[in:in+int(n)], &doc); err != nil {
		t.Fatal(err)
	}
	mutate(doc)
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	tail := data[in+int(n):]
	out := make([]byte, header+len(encoded)+len(tail))
	copy(out[:4], data[:4])
	out[4] = version
	if header == snapshotV21EnvelopePrefixBytes {
		out[5] = snapshotBodyPlain
	}
	binary.BigEndian.PutUint64(out[header-8:header], uint64(len(encoded)))
	copy(out[header:], encoded)
	copy(out[header+len(encoded):], tail)
	return out
}
```

In `fsm/artifact_capacity_test.go` (`:207-213`), build each version's envelope and list v20 explicitly.

Replace

```go
	for _, version := range []byte{snapshotVersionV7, snapshotVersionV9, snapshotVersionV10, snapshotVersionV11, snapshotVersionV17, snapshotVersionV18, snapshotVersionV19, snapshotVersion} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			data := make([]byte, 13+len(doc))
			copy(data[:4], snapshotMagic[:])
			data[4] = version
			binary.BigEndian.PutUint64(data[5:13], uint64(len(doc)))
			copy(data[13:], doc)
```

with

```go
	for _, version := range []byte{snapshotVersionV7, snapshotVersionV9, snapshotVersionV10, snapshotVersionV11, snapshotVersionV17, snapshotVersionV18, snapshotVersionV19, snapshotVersionV20, snapshotVersion} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			// v21's envelope is one byte longer: its body kind, 0 (plain),
			// follows the version byte.
			header := int(snapshotEnvelopeBytes(version))
			data := make([]byte, header+len(doc))
			copy(data[:4], snapshotMagic[:])
			data[4] = version
			binary.BigEndian.PutUint64(data[header-8:header], uint64(len(doc)))
			copy(data[header:], doc)
```

In `fsm/snapshot_test.go`, `TestRestoreRejectsOversizedLengthPrefix` (`:192`) keeps testing the 13-byte envelope:

Replace

```go
	buf := []byte{'A', 'F', 'S', 'M', snapshotVersion, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
```

with

```go
	buf := []byte{'A', 'F', 'S', 'M', snapshotVersionV20, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
```

and `TestSnapshotRefusesFrontierlessDrainingRecordAndAcceptsV9` (`:255`) pins the newest version:

Replace

```go
	if data[4] != snapshotVersionV14 || snapshotVersion != 20 {
```

with

```go
	if data[4] != snapshotVersionV14 || snapshotVersion != 21 {
```

In `fsm/snapshot_tracks_test.go` (`:310-318`), `TestSnapshotRefusesTheNextVersion` moves one version up:

Replace

```go
// TestSnapshotRefusesTheNextVersion: the mechanism every older binary
// applies to a v20 container (arbiter v0.11.0's acceptance chain ends at 19),
// one version up.
func TestSnapshotRefusesTheNextVersion(t *testing.T) {
	next := rewriteSnapshotDocument(t, snapshotBytes(t, newTrackManifestFixture(t).f), snapshotVersionV20+1, keepSnapshotDocument)
	if err := restoreErr(t, next); err == nil || !strings.Contains(err.Error(), "unsupported snapshot version 21") {
		t.Fatalf("restore = %v", err)
	}
}
```

with

```go
// TestSnapshotRefusesTheNextVersion: the mechanism every older binary
// applies to a newer container (arbiter v0.11.0's acceptance chain ends at 19,
// the stage-2 releases' at 20), one version up from v21.
func TestSnapshotRefusesTheNextVersion(t *testing.T) {
	next := rewriteSnapshotDocument(t, snapshotBytes(t, newTrackManifestFixture(t).f), snapshotVersionV21+1, keepSnapshotDocument)
	if err := restoreErr(t, next); err == nil || !strings.Contains(err.Error(), "unsupported snapshot version 22") {
		t.Fatalf("restore = %v", err)
	}
}
```

In `fsm/state_size_test.go`, `TestStateComponentsListEverySnapshotDocMember` (`:27-28`) lists the new member:

Replace

```go
		"legacy_cleanup_proofs", "artifact_disposition", "table_registry", "lane_counts", "registration_seqs", "tracks",
		"spent_ids",
```

with

```go
		"legacy_cleanup_proofs", "artifact_disposition", "table_registry", "lane_counts", "registration_seqs", "tracks",
		"stage3", "spent_ids",
```

and the read-cap case of `TestFailedRestoreKeepsThePreviousSample` (`:236`) keeps the 13-byte envelope:

Replace

```go
		"above the read cap":    {'A', 'F', 'S', 'M', snapshotVersion, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
```

with

```go
		"above the read cap":    {'A', 'F', 'S', 'M', snapshotVersionV20, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
```

- [ ] **Step 4: Run them and see them fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && GOWORK=off go test ./fsm/ -run 'TestSnapshotWritesV20UntilStage3IsCommitted|TestValidateStage3State' -count=1`

Expected: the package fails to build:

```
# github.com/sentioxyz/arbiter/fsm [github.com/sentioxyz/arbiter/fsm.test]
fsm/apply_artifact_disposition_test.go:730:20: undefined: snapshotEnvelopeBytes
fsm/apply_artifact_disposition_test.go:734:11: undefined: snapshotV21EnvelopePrefixBytes
fsm/apply_artifact_disposition_test.go:734:56: undefined: snapshotBodyPlain
fsm/apply_artifact_disposition_test.go:754:15: undefined: snapshotV21EnvelopePrefixBytes
fsm/apply_artifact_disposition_test.go:755:12: undefined: snapshotBodyPlain
fsm/artifact_capacity_test.go:211:18: undefined: snapshotEnvelopeBytes
fsm/snapshot_tracks_test.go:314:85: undefined: snapshotVersionV21
fsm/snapshot_v21_test.go:43:17: undefined: snapshotV21EnvelopePrefixBytes
fsm/snapshot_v21_test.go:43:92: undefined: snapshotVersionV21
fsm/snapshot_v21_test.go:43:125: undefined: snapshotBodyPlain
fsm/snapshot_v21_test.go:43:125: too many errors
FAIL	github.com/sentioxyz/arbiter/fsm [build failed]
FAIL
```

(the line and column numbers move with the branch base under the hunk rule; the undefined names are the point).

- [ ] **Step 5: Implement the v21 envelope length**

In `fsm/snapshot_count.go`, the `SnapshotEncodingDimensions` comment (`:17-19`):

Replace

```go
// C1 lifecycle record cardinality. SnapshotEnvelopeBytes includes the AFSM
// magic, version, and big-endian JSON-length prefix, but deliberately excludes
// the separately encoded SpentIDs tail.
```

with

```go
// C1 lifecycle record cardinality. SnapshotEnvelopeBytes includes the AFSM
// magic, version, v21's body-kind byte, and big-endian JSON-length prefix,
// but deliberately excludes the separately encoded SpentIDs tail.
```

The constants (`:39-42`):

Replace

```go
const (
	snapshotEnvelopePrefixBytes = 13 // AFSM (4), version (1), JSON length (8)
	maxSnapshotCountingDepth    = 32
)
```

with

```go
const (
	snapshotEnvelopePrefixBytes    = 13 // v3-v20: AFSM (4), version (1), JSON length (8)
	snapshotV21EnvelopePrefixBytes = 14 // v21: AFSM (4), version (1), body kind (1), body length (8)
	maxSnapshotCountingDepth       = 32
)

// snapshotEnvelopeBytes is the envelope a container of the given version
// carries before its document: 13 bytes for v3-v20, 14 for v21, whose
// body-kind byte follows the version byte (clarification K70).
func snapshotEnvelopeBytes(version byte) uint64 {
	if version == snapshotVersionV21 {
		return snapshotV21EnvelopePrefixBytes
	}
	return snapshotEnvelopePrefixBytes
}
```

In `CountSnapshotEncoding`, the overflow check (`:66-68`):

Replace

```go
	if counter.n > math.MaxUint64-snapshotEnvelopePrefixBytes {
		return SnapshotEncodingDimensions{}, fmt.Errorf("snapshot count: envelope overflow")
	}
```

with

```go
	envelope := snapshotEnvelopeBytes(snapshotWriteVersion(st))
	if counter.n > math.MaxUint64-envelope {
		return SnapshotEncodingDimensions{}, fmt.Errorf("snapshot count: envelope overflow")
	}
```

and the result (`:98`):

Replace

```go
		SnapshotEnvelopeBytes: counter.n + snapshotEnvelopePrefixBytes,
```

with

```go
		SnapshotEnvelopeBytes: counter.n + envelope,
```

- [ ] **Step 6: Implement the v21 writer and reader**

In `fsm/snapshot.go`, the imports (`:8-9`; Plan S3-A Task 9 added `sync/atomic` after `io`):

Replace

```go
	"io"
	"sync/atomic"
```

with

```go
	"io"
	"slices"
	"sync/atomic"
```

The version comment (`:21-25`):

Replace

```go
// snapshotVersion is the newest container this binary writes. writeSnapshot
// writes it only once Params.TracksEnabled is committed (the tracks
// activation, housegate spec 2026-10-10 §7.6); before that it writes
// snapshotVersionV19 once Params.SIIndexers is committed (the signed-claims
// activation, §6.7), snapshotVersionV18 once Params.ClientLanes is committed
```

with

```go
// snapshotVersion is the newest container this binary writes. writeSnapshot
// writes it only once Params.Stage3 is committed (the stage-3 activation,
// housegate spec 2026-10-14 §10.3); before that it writes snapshotVersionV20
// once Params.TracksEnabled is committed (the tracks activation, housegate
// spec 2026-10-10 §7.6), snapshotVersionV19 once Params.SIIndexers is
// committed (the signed-claims activation, §6.7), snapshotVersionV18 once
// Params.ClientLanes is committed
```

The end of that comment and the first constants (`:37-42`):

Replace

```go
// tracks_enabled, tracks state, header tracks and open-block owner, whose loss
// would change chain hashes or make replicas' states differ), so each version
// byte is what makes the older binary refuse.
const (
	snapshotVersion       = 20
	snapshotVersionV20    = 20
```

with

```go
// tracks_enabled, tracks state, header tracks and open-block owner, whose loss
// would change chain hashes or make replicas' states differ, or v21's stage3
// parameter and state and its host-bound verifiers fields), so each version
// byte is what makes the older binary refuse. v21 also frames its body with a
// body-kind byte after the version byte (clarification K70), so v3-v20 keep
// their 13-byte envelope and v21 has a 14-byte one (snapshotEnvelopeBytes).
const (
	snapshotVersion       = 21
	snapshotVersionV21    = 21
	snapshotVersionV20    = 20
```

After the version constants (`:59-61`), the body kinds:

Replace

```go
	legacySnapshotVersion = 3
	maxSnapshotDocBytes   = 1 << 30
)
```

with

```go
	legacySnapshotVersion = 3
	maxSnapshotDocBytes   = 1 << 30
)

// The body kinds of a v21 container (clarification K70): the byte after the
// version byte names the body its length prefix frames. The plain body is
// exactly the JSON document v20 lays out plus the stage3 member; the interned
// body is the lossless encoding of the same bytes. writeSnapshot always writes
// the plain body.
const (
	snapshotBodyPlain    byte = 0
	snapshotBodyInterned byte = 1
)
```

The last member of `snapshotDoc` (`:106-108`):

Replace

```go
	// tracks_enabled is committed.
	Tracks *TrackState `json:"tracks,omitempty"`
}
```

with

```go
	// tracks_enabled is committed.
	Tracks *TrackState `json:"tracks,omitempty"`
	// Stage3 is State.Stage3 (v21): the stage-3 activation point and one
	// cutover record per switch turned on (housegate spec 2026-10-14
	// §10.3-§10.4, contract §45). It is written only once stage3 is
	// committed.
	Stage3 *Stage3State `json:"stage3,omitempty"`
}
```

In `writeSnapshot` (`:177-180`), the kind byte after the version byte:

Replace

```go
	if _, err := w.Write([]byte{snapshotWriteVersion(st)}); err != nil {
		return err
	}
	var lenBuf [8]byte
```

with

```go
	version := snapshotWriteVersion(st)
	if _, err := w.Write([]byte{version}); err != nil {
		return err
	}
	// v21 names the body after its version byte (clarification K70); this
	// writer always writes the plain body.
	if version == snapshotVersionV21 {
		if _, err := w.Write([]byte{snapshotBodyPlain}); err != nil {
			return err
		}
	}
	var lenBuf [8]byte
```

The `snapshotWriteVersion` comment (`:191-192`):

Replace

```go
// snapshotWriteVersion is snapshotVersion (v20) once tracks_enabled is
// committed, snapshotVersionV19 once si_indexers is committed,
```

with

```go
// snapshotWriteVersion is snapshotVersion (v21) once stage3 is committed,
// snapshotVersionV20 once tracks_enabled is committed, snapshotVersionV19
// once si_indexers is committed,
```

and its first rule (`:198-199`):

Replace

```go
func snapshotWriteVersion(st *State) byte {
	if st.Params.TracksEnabled {
```

with

```go
func snapshotWriteVersion(st *State) byte {
	if st.Params.Stage3 != nil {
		return snapshotVersionV21
	}
	if st.Params.TracksEnabled {
```

In `snapshotDocument` (`:244-246`):

Replace

```go
		Tracks:              st.Tracks,
	}
}
```

with

```go
		Tracks:              st.Tracks,
		Stage3:              st.Stage3,
	}
}
```

In `readSnapshotMeasured`, the version whitelist and its refusal (`:285-287`), followed by the kind byte:

Replace

```go
	if ver[0] != snapshotVersion && ver[0] != snapshotVersionV19 && ver[0] != snapshotVersionV18 && ver[0] != snapshotVersionV17 && ver[0] != snapshotVersionV16 && ver[0] != snapshotVersionV15 && ver[0] != snapshotVersionV14 && ver[0] != snapshotVersionV13 && ver[0] != snapshotVersionV12 && ver[0] != snapshotVersionV11 && ver[0] != snapshotVersionV10 && ver[0] != snapshotVersionV9 && ver[0] != snapshotVersionV8 && ver[0] != snapshotVersionV7 && ver[0] != snapshotVersionV6 && ver[0] != snapshotVersionV5 && ver[0] != snapshotVersionV4 && ver[0] != legacySnapshotVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d (require %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d or %d); pre-v3 state requires a backup and coordinated re-genesis", ver[0], legacySnapshotVersion, snapshotVersionV4, snapshotVersionV5, snapshotVersionV6, snapshotVersionV7, snapshotVersionV8, snapshotVersionV9, snapshotVersionV10, snapshotVersionV11, snapshotVersionV12, snapshotVersionV13, snapshotVersionV14, snapshotVersionV15, snapshotVersionV16, snapshotVersionV17, snapshotVersionV18, snapshotVersionV19, snapshotVersion)
	}
```

with

```go
	if ver[0] != snapshotVersion && ver[0] != snapshotVersionV20 && ver[0] != snapshotVersionV19 && ver[0] != snapshotVersionV18 && ver[0] != snapshotVersionV17 && ver[0] != snapshotVersionV16 && ver[0] != snapshotVersionV15 && ver[0] != snapshotVersionV14 && ver[0] != snapshotVersionV13 && ver[0] != snapshotVersionV12 && ver[0] != snapshotVersionV11 && ver[0] != snapshotVersionV10 && ver[0] != snapshotVersionV9 && ver[0] != snapshotVersionV8 && ver[0] != snapshotVersionV7 && ver[0] != snapshotVersionV6 && ver[0] != snapshotVersionV5 && ver[0] != snapshotVersionV4 && ver[0] != legacySnapshotVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d (require %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d or %d); pre-v3 state requires a backup and coordinated re-genesis", ver[0], legacySnapshotVersion, snapshotVersionV4, snapshotVersionV5, snapshotVersionV6, snapshotVersionV7, snapshotVersionV8, snapshotVersionV9, snapshotVersionV10, snapshotVersionV11, snapshotVersionV12, snapshotVersionV13, snapshotVersionV14, snapshotVersionV15, snapshotVersionV16, snapshotVersionV17, snapshotVersionV18, snapshotVersionV19, snapshotVersionV20, snapshotVersion)
	}
	// v21 names its body after the version byte (clarification K70). This
	// binary reads the plain body; any other kind is refused before the
	// length is read, so a corrupt kind byte never decodes as plain.
	if ver[0] == snapshotVersionV21 {
		var kind [1]byte
		if _, err := io.ReadFull(r, kind[:]); err != nil {
			return nil, fmt.Errorf("snapshot v21 body kind: %w", err)
		}
		if kind[0] != snapshotBodyPlain {
			return nil, fmt.Errorf("unsupported snapshot v21 body kind %d", kind[0])
		}
	}
```

The duplicate-allowance predicate (`:303`):

Replace

```go
	if ver[0] == snapshotVersion || ver[0] == snapshotVersionV19 || ver[0] == snapshotVersionV18 || ver[0] == snapshotVersionV17 || ver[0] == snapshotVersionV16 || ver[0] == snapshotVersionV15 || ver[0] == snapshotVersionV14 || ver[0] == snapshotVersionV13 || ver[0] == snapshotVersionV12 || ver[0] == snapshotVersionV11 || ver[0] == snapshotVersionV10 || ver[0] == snapshotVersionV9 || ver[0] == snapshotVersionV8 || ver[0] == snapshotVersionV7 {
```

with

```go
	if ver[0] == snapshotVersion || ver[0] == snapshotVersionV20 || ver[0] == snapshotVersionV19 || ver[0] == snapshotVersionV18 || ver[0] == snapshotVersionV17 || ver[0] == snapshotVersionV16 || ver[0] == snapshotVersionV15 || ver[0] == snapshotVersionV14 || ver[0] == snapshotVersionV13 || ver[0] == snapshotVersionV12 || ver[0] == snapshotVersionV11 || ver[0] == snapshotVersionV10 || ver[0] == snapshotVersionV9 || ver[0] == snapshotVersionV8 || ver[0] == snapshotVersionV7 {
```

The bootstrap-params predicate (`:318`):

Replace

```go
	if (ver[0] == snapshotVersion || ver[0] == snapshotVersionV19 || ver[0] == snapshotVersionV18 || ver[0] == snapshotVersionV17 || ver[0] == snapshotVersionV16 || ver[0] == snapshotVersionV15 || ver[0] == snapshotVersionV14 || ver[0] == snapshotVersionV13 || ver[0] == snapshotVersionV12 || ver[0] == snapshotVersionV11 || ver[0] == snapshotVersionV10 || ver[0] == snapshotVersionV9 || ver[0] == snapshotVersionV8 || ver[0] == snapshotVersionV7 || ver[0] == snapshotVersionV6 || ver[0] == snapshotVersionV5 || ver[0] == snapshotVersionV4) && doc.BootstrapParams == nil {
```

with

```go
	if (ver[0] == snapshotVersion || ver[0] == snapshotVersionV20 || ver[0] == snapshotVersionV19 || ver[0] == snapshotVersionV18 || ver[0] == snapshotVersionV17 || ver[0] == snapshotVersionV16 || ver[0] == snapshotVersionV15 || ver[0] == snapshotVersionV14 || ver[0] == snapshotVersionV13 || ver[0] == snapshotVersionV12 || ver[0] == snapshotVersionV11 || ver[0] == snapshotVersionV10 || ver[0] == snapshotVersionV9 || ver[0] == snapshotVersionV8 || ver[0] == snapshotVersionV7 || ver[0] == snapshotVersionV6 || ver[0] == snapshotVersionV5 || ver[0] == snapshotVersionV4) && doc.BootstrapParams == nil {
```

The artifact-disposition predicate (`:426`):

Replace

```go
	if ver[0] == snapshotVersion || ver[0] == snapshotVersionV19 || ver[0] == snapshotVersionV18 || ver[0] == snapshotVersionV17 || ver[0] == snapshotVersionV16 || ver[0] == snapshotVersionV15 || ver[0] == snapshotVersionV14 || ver[0] == snapshotVersionV13 || ver[0] == snapshotVersionV12 || ver[0] == snapshotVersionV11 || ver[0] == snapshotVersionV10 || ver[0] == snapshotVersionV9 || ver[0] == snapshotVersionV8 || ver[0] == snapshotVersionV7 || ver[0] == snapshotVersionV6 || ver[0] == snapshotVersionV5 {
```

with

```go
	if ver[0] == snapshotVersion || ver[0] == snapshotVersionV20 || ver[0] == snapshotVersionV19 || ver[0] == snapshotVersionV18 || ver[0] == snapshotVersionV17 || ver[0] == snapshotVersionV16 || ver[0] == snapshotVersionV15 || ver[0] == snapshotVersionV14 || ver[0] == snapshotVersionV13 || ver[0] == snapshotVersionV12 || ver[0] == snapshotVersionV11 || ver[0] == snapshotVersionV10 || ver[0] == snapshotVersionV9 || ver[0] == snapshotVersionV8 || ver[0] == snapshotVersionV7 || ver[0] == snapshotVersionV6 || ver[0] == snapshotVersionV5 {
```

After `validateTrackState` (`:504-507`), the stage-3 forged-field refusal and the restore rules:

Replace

```go
	st.Tracks = doc.Tracks
	if err := validateTrackState(st); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
```

with

```go
	st.Tracks = doc.Tracks
	if err := validateTrackState(st); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
	// Stage 3 (housegate spec 2026-10-14 §10.3, clarification K70) was added
	// in v21, written only once stage3 is committed. An older container cannot
	// have committed any of it, and the binary that wrote it would have
	// dropped it on its next snapshot, so any of it under an older version
	// byte is a forged field, not a migration. A present stage3 member, even
	// an empty one, is refused: no pre-v21 writer emits the key. This runs
	// before validateRestoredState, so the refusal names the version.
	if ver[0] < snapshotVersionV21 && stage3FieldsPresent(&doc) {
		return nil, fmt.Errorf("stage3 state is present in a pre-v21 container")
	}
	st.Stage3 = doc.Stage3
	if err := validateStage3State(st); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
```

Before `tableRegistryHasV17Fields` (`:723-724`), the stage-3 field predicates:

Replace

```go
// tableRegistryHasV17Fields reports whether any incarnation carries a field
// only a v17 container may hold.
```

with

```go
// stage3FieldsPresent reports any member only a v21 container may hold
// (contract §46): the stage3 parameter in the current or the bootstrap
// parameters or in any signed update, the stage3 state, and a verifiers
// entry of any of the three that carries a field beyond node_id and
// ed25519_pubkey. Each later stage-3 task adds the fields it introduces
// before the final return.
func stage3FieldsPresent(doc *snapshotDoc) bool {
	if doc.Params.Stage3 != nil || doc.Stage3 != nil || slices.ContainsFunc(doc.Params.Verifiers, verifierEntryHasStage3Fields) {
		return true
	}
	if b := doc.BootstrapParams; b != nil && (b.Stage3 != nil || slices.ContainsFunc(b.Verifiers, verifierEntryHasStage3Fields)) {
		return true
	}
	for _, change := range doc.ConsensusUpdates {
		if change.Update.Stage3 != nil || slices.ContainsFunc(change.Update.Verifiers, verifierEntryHasStage3Fields) {
			return true
		}
	}
	return false
}

// verifierEntryHasStage3Fields reports a verifiers entry that carries any
// field beyond node_id and ed25519_pubkey: a host binding or a member field
// (spec §6.1), which no pre-v21 writer emits. host_indexer_id 0 is a host
// binding.
func verifierEntryHasStage3Fields(v arbiter.VerifierEntry) bool {
	return v.HostIndexerID != nil || v.ReplicaSigner != "" || v.ConsentJWS != "" || v.Weight != 0 ||
		v.FailureDomain != "" || v.Verify || v.Replicate || v.Counts || v.SharedRMT
}

// tableRegistryHasV17Fields reports whether any incarnation carries a field
// only a v17 container may hold.
```

- [ ] **Step 7: Read the v21 envelope in the state-size sample**

In `fsm/state_size.go` (Plan S3-A Task 9), the `ContainerBytes` comment (`:56-57`):

Replace

```go
	// ContainerBytes is the whole container (a Raft snapshot's state.bin):
	// the 13-byte envelope, the document and the dump.
```

with

```go
	// ContainerBytes is the whole container (a Raft snapshot's state.bin):
	// the envelope (13 bytes, 14 for v21), the document and the dump.
```

`containerStateSize` and `stateSizeSample` (`:114-142`):

Replace

```go
// containerStateSize measures a container writeSnapshot wrote.
func containerStateSize(data []byte) (StateSizeSample, error) {
	if len(data) < snapshotEnvelopePrefixBytes {
		return StateSizeSample{}, fmt.Errorf("state size: container shorter than its %d-byte envelope", snapshotEnvelopePrefixBytes)
	}
	n := binary.BigEndian.Uint64(data[5:snapshotEnvelopePrefixBytes])
	if n > uint64(len(data)-snapshotEnvelopePrefixBytes) {
		return StateSizeSample{}, fmt.Errorf("state size: document length %d exceeds the container", n)
	}
	components, err := snapshotDocComponents(data[snapshotEnvelopePrefixBytes : snapshotEnvelopePrefixBytes+n])
	if err != nil {
		return StateSizeSample{}, err
	}
	return stateSizeSample(StateSizeSourceSnapshot, n, uint64(len(data))-snapshotEnvelopePrefixBytes-n, components), nil
}

// stateSizeSample assembles a sample from a document's member sizes and the
// dump's length.
func stateSizeSample(source string, docBytes, spentIDsBytes uint64, components []StateComponentSize) StateSizeSample {
	components[len(components)-1].Bytes = spentIDsBytes
	return StateSizeSample{
		Source:         source,
		StateBytes:     docBytes + spentIDsBytes,
		DocBytes:       docBytes,
		SpentIDsBytes:  spentIDsBytes,
		ContainerBytes: snapshotEnvelopePrefixBytes + docBytes + spentIDsBytes,
		Components:     components,
	}
}
```

with

```go
// containerStateSize measures a container writeSnapshot wrote: the 13-byte
// envelope of v3-v20 or the 14-byte one of v21, whose document is the plain
// body.
func containerStateSize(data []byte) (StateSizeSample, error) {
	if len(data) < snapshotEnvelopePrefixBytes {
		return StateSizeSample{}, fmt.Errorf("state size: container shorter than its %d-byte envelope", snapshotEnvelopePrefixBytes)
	}
	envelope := snapshotEnvelopeBytes(data[4])
	if uint64(len(data)) < envelope {
		return StateSizeSample{}, fmt.Errorf("state size: container shorter than its %d-byte envelope", envelope)
	}
	n := binary.BigEndian.Uint64(data[envelope-8 : envelope])
	if n > uint64(len(data))-envelope {
		return StateSizeSample{}, fmt.Errorf("state size: document length %d exceeds the container", n)
	}
	components, err := snapshotDocComponents(data[envelope : envelope+n])
	if err != nil {
		return StateSizeSample{}, err
	}
	return stateSizeSample(StateSizeSourceSnapshot, envelope, n, uint64(len(data))-envelope-n, components), nil
}

// stateSizeSample assembles a sample from a container's envelope length, its
// document's member sizes and the dump's length.
func stateSizeSample(source string, envelopeBytes, docBytes, spentIDsBytes uint64, components []StateComponentSize) StateSizeSample {
	components[len(components)-1].Bytes = spentIDsBytes
	return StateSizeSample{
		Source:         source,
		StateBytes:     docBytes + spentIDsBytes,
		DocBytes:       docBytes,
		SpentIDsBytes:  spentIDsBytes,
		ContainerBytes: envelopeBytes + docBytes + spentIDsBytes,
		Components:     components,
	}
}
```

In `fsm/snapshot.go`, Plan S3-A Task 9's restore sample at the end of `readSnapshotMeasured` (`:656`):

Replace

```go
		*size = stateSizeSample(StateSizeSourceRestore, n, tail.n, components)
```

with

```go
		*size = stateSizeSample(StateSizeSourceRestore, snapshotEnvelopeBytes(ver[0]), n, tail.n, components)
```

`stateSizeSample` has no other caller (`grep -rn 'stateSizeSample(' fsm/` lists the definition and these two calls).

- [ ] **Step 8: Add the stage-3 restore rules**

Create `fsm/stage3_restore.go`:

```go
package fsm

import (
	"fmt"
	"slices"

	"github.com/sentioxyz/arbiter-core"
)

// validateStage3State checks the stage-3 state a restored container carries
// (housegate spec 2026-10-14 §10.3-§10.4, contract §46). readSnapshot runs it
// right after validateTrackState and before validateRestoredState, so each
// refusal names its stage-3 rule rather than a consensus-history mismatch;
// validateConsensusHistory replays the signed updates themselves. Every
// later stage-3 task appends its own rule here, after the activation check.
func validateStage3State(st *State) error {
	if err := validateStage3Activation(st); err != nil {
		return err
	}
	return nil
}

// validateStage3Activation checks the activation point and the switch
// cutovers against the committed parameter and the signed history the
// container carries. Before the activation no stage-3 state exists. After it
// the state records the epoch of the first update that set stage3 and a block
// and promotion within the state, and exactly one cutover per switch that is
// on and none for a switch that is off, in the order of spec §10.4, each at
// the epoch of the update that turned its switch on, none before the previous
// cutover or the activation or beyond the state, and only the purge cutover
// with an incarnation seq, one the registry could have assigned next.
// Cutovers are visited in stored order, so every replica names the same
// corruption.
func validateStage3Activation(st *State) error {
	params, s3 := st.Params.Stage3, st.Stage3
	if (params != nil) != (s3 != nil) {
		return fmt.Errorf("stage3 state must exist exactly when stage3 is committed")
	}
	if s3 == nil {
		return nil
	}
	// firstEpoch is the epoch produced by the first signed update whose
	// stage3 parameter satisfies on (the update at index i produces epoch
	// i+1), or 0 when none does; no record may name epoch 0.
	firstEpoch := func(on func(*arbiter.Stage3Params) bool) uint64 {
		for i, change := range st.ConsensusUpdates {
			if p := change.Update.Stage3; p != nil && on(p) {
				return uint64(i) + 1
			}
		}
		return 0
	}
	if want := firstEpoch(func(*arbiter.Stage3Params) bool { return true }); want == 0 || s3.ActivationEpoch != want {
		return fmt.Errorf("stage3 activation epoch %d is not the epoch of the first update that set stage3 (%d)", s3.ActivationEpoch, want)
	}
	blocks := uint64(len(st.Blocks))
	if s3.ActivationBlockSeq > blocks || s3.ActivationPromotionSeq > st.PromotionSeq {
		return fmt.Errorf("stage3 activation block %d or promotion %d is beyond the state's (%d, %d)", s3.ActivationBlockSeq, s3.ActivationPromotionSeq, blocks, st.PromotionSeq)
	}
	order := Stage3Switches()
	for _, c := range s3.Switches {
		// A name that is no switch of spec §10.4 is not a switch that is on.
		if !slices.Contains(order, c.Switch) || !stage3SwitchValue(params, c.Switch) {
			return fmt.Errorf("stage3 records a cutover for switch %s, which is off", c.Switch)
		}
	}
	for _, name := range order {
		if stage3SwitchValue(params, name) && !slices.ContainsFunc(s3.Switches, func(c Stage3SwitchCutover) bool { return c.Switch == name }) {
			return fmt.Errorf("stage3 switch %s is on without a cutover record", name)
		}
	}
	for i := 1; i < len(s3.Switches); i++ {
		if slices.Index(order, s3.Switches[i].Switch) <= slices.Index(order, s3.Switches[i-1].Switch) {
			return fmt.Errorf("stage3 switch cutovers are not in the order of spec §10.4")
		}
	}
	nextIncarnation := uint64(1)
	if st.TableRegistry != nil {
		nextIncarnation += uint64(len(st.TableRegistry.Incarnations))
	}
	block, promotion := s3.ActivationBlockSeq, s3.ActivationPromotionSeq
	for _, c := range s3.Switches {
		name := c.Switch
		if want := firstEpoch(func(p *arbiter.Stage3Params) bool { return stage3SwitchValue(p, name) }); want == 0 || c.Epoch != want {
			return fmt.Errorf("stage3 switch %s cutover epoch %d is not the epoch of the update that turned it on (%d)", name, c.Epoch, want)
		}
		if c.BlockSeq < block || c.PromotionSeq < promotion {
			return fmt.Errorf("stage3 switch %s cutover (block %d, promotion %d) precedes the previous cutover or the activation", name, c.BlockSeq, c.PromotionSeq)
		}
		if c.BlockSeq > blocks || c.PromotionSeq > st.PromotionSeq {
			return fmt.Errorf("stage3 switch %s cutover (block %d, promotion %d) is beyond the state's (%d, %d)", name, c.BlockSeq, c.PromotionSeq, blocks, st.PromotionSeq)
		}
		switch {
		case name != Stage3SwitchPurge && c.IncarnationSeq != 0:
			return fmt.Errorf("stage3 switch %s cutover carries an incarnation seq", name)
		case name == Stage3SwitchPurge && (c.IncarnationSeq < 1 || c.IncarnationSeq > nextIncarnation):
			return fmt.Errorf("stage3 purge cutover incarnation seq %d is outside 1..%d", c.IncarnationSeq, nextIncarnation)
		}
		block, promotion = c.BlockSeq, c.PromotionSeq
	}
	return nil
}
```

- [ ] **Step 9: Refuse stage3 in the bootstrap parameters**

In `fsm/consensus_history.go`, `validateConsensusHistory` (`:31-33`):

Replace

```go
	if st.BootstrapParams.TracksEnabled {
		return fmt.Errorf("snapshot bootstrap consensus params: tracks_enabled is set only by a consensus update, not at genesis")
	}
```

with

```go
	if st.BootstrapParams.TracksEnabled {
		return fmt.Errorf("snapshot bootstrap consensus params: tracks_enabled is set only by a consensus update, not at genesis")
	}
	if st.BootstrapParams.Stage3 != nil {
		return fmt.Errorf("snapshot bootstrap consensus params: stage3 is set only by a consensus update, not at genesis")
	}
```

- [ ] **Step 10: Run the tests; only the golden file is missing**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test -z "$(gofmt -l fsm)" && GOWORK=off go vet ./fsm/ && GOWORK=off go test ./fsm/ -run 'TestSnapshotWritesV20UntilStage3IsCommitted|TestSnapshotWritesAV21PlainContainerOnceStage3IsCommitted|TestStage3V21PlainContainerGolden|TestRestoreReadsEveryEarlierVersionUnchanged|TestRestoreRefusesStage3FieldsInAPreV21Container|TestRestoreRefusesAnUnknownV21BodyKind|TestRestoreRejectsAnOversizedV21LengthPrefix|TestBootstrapParamsNeverCarryStage3|TestValidateStage3State|TestContainerStateSizeMeasuresAV21PlainContainer|TestCountSnapshotEncoding|TestStateComponentsListEverySnapshotDocMember|TestFailedRestoreKeepsThePreviousSample|TestSnapshotRefusesTheNextVersion|TestRestoreRejectsOversizedLengthPrefix|TestSnapshotRefusesFrontierlessDrainingRecordAndAcceptsV9|TestArtifactDispositionCapacityDuplicateAllowanceKey' -race -count=1`

Expected: gofmt lists nothing and vet is clean; the only failure is

```
--- FAIL: TestStage3V21PlainContainerGolden (…)
    snapshot_v21_test.go:234: open testdata/stage3_v21_plain.snapshot: no such file or directory
FAIL
FAIL	github.com/sentioxyz/arbiter/fsm	…
FAIL
```

Rerun with `-v` (`… -count=1 -v 2>&1 | grep -E '^--- '`) to see the other 21 top-level tests `--- PASS`. Any other failure stops the task: fix the code, never the expectation.

- [ ] **Step 11: Capture the plain v21 golden, once**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test ! -e fsm/testdata/stage3_v21_plain.snapshot && env ARBITER_CAPTURE_STAGE3_V21_PLAIN_GOLDEN=1 GOWORK=off go test ./fsm/ -run '^TestStage3V21PlainContainerGolden$' -count=1 -v && od -An -tu1 -N6 fsm/testdata/stage3_v21_plain.snapshot && shasum -a 256 fsm/testdata/stage3_v21_plain.snapshot`

Expected: `snapshot_v21_test.go:230: captured; commit the file` and `--- SKIP: TestStage3V21PlainContainerGolden`, then `65 70 83 77 21 0` (`AFSM`, version 21, plain body), then the file's SHA-256 (Step 14 prints it again after the commit; the two must match). Then, in a new Bash call at least a few seconds later (a token signed with the clock would differ by then), run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && GOWORK=off go test ./fsm/ -run '^TestStage3V21PlainContainerGolden$' -count=1 -v`

Expected: `--- PASS: TestStage3V21PlainContainerGolden`. If it fails, the fixture is not reproducible: delete the file (`rm -- "$W/fsm/testdata/stage3_v21_plain.snapshot"`, a file this task created and nothing committed), find the unfixed signature, and repeat this step; never commit a golden that does not pass a later run.

- [ ] **Step 12: Document snapshot v21**

In `README.md`, the end of the **Snapshot versions:** paragraph (`:300`):

Replace

```markdown
and **v20** once `tracks_enabled` is committed (see Multi-source storage integrity (tracks and quarantine) below).
```

with

```markdown
and **v20** once `tracks_enabled` is committed (see Multi-source storage integrity (tracks and quarantine) below), and **v21** once `stage3` is committed (see Snapshot v21 below).
```

Immediately before `## Artifact disposition admission` (`:396` at `526a721`; on the branch the heading follows Task 65's `## Multi-source storage integrity (stage-3 activation)` section, so this paragraph closes that section), insert this paragraph (a single line) followed by a blank line:

```markdown
**Snapshot v21** (housegate spec 2026-10-14 §10.3, Plan S3-E2). The container moves to v21 only once `stage3` is committed, by the stage-3 activation; until then the writer keeps v14 to v20 byte for byte, so every voter can return to the stage-2 release until the activation. v21 frames its body with a body-kind byte after the version byte: `AFSM`, `21`, the kind, the 8-byte big-endian body length, the body and the spent-ids dump (a 14-byte envelope; v3 to v20 have 13 bytes). Kind 0 is the plain body, exactly the JSON document v20 lays out plus its last member `stage3` (the activation epoch, block and promotion, and one cutover per switch turned on, in spec §10.4 order); v21 also carries the `stage3` parameter and the host-bound `verifiers` fields. A pre-v21 binary refuses a v21 container at its version byte, stage-3 state under an older version byte is refused as forged (`stage3 state is present in a pre-v21 container`), an unknown body kind is refused (`unsupported snapshot v21 body kind N`), `stage3` in the bootstrap parameters is refused, and restore checks the stage-3 state against the signed history (`stage3 state must exist exactly when stage3 is committed` and the cutover rules of `fsm/stage3_restore.go`). The activation is irreversible.
```

In Plan S3-A Task 9's **State size and snapshots** paragraph (`:429` at `526a721`; further down on the branch, below Task 65's section):

Replace

```markdown
(`challenge_counters` through `tracks`, each counted as its key, colon and value, 0 when omitted)
```

with

```markdown
(`challenge_counters` through `stage3`, each counted as its key, colon and value, 0 when omitted)
```

Run:

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && grep -c '^\*\*Snapshot v21\*\*' README.md && grep -c 'and \*\*v21\*\* once `stage3` is committed (see Snapshot v21 below)' README.md && grep -c '`challenge_counters` through `stage3`' README.md && grep -c '`challenge_counters` through `tracks`' README.md
```

Expected: `1`, `1`, `1`, then `0` (and that last `grep -c` exits 1).

- [ ] **Step 13: Full verification** (contract §48)

Bazel and gazelle reuse the downloads of Task 64's pins; if either reports fetching anything, stop and ask for explicit confirmation in chat first (remote read).

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && test -z "$(gofmt -l $(git ls-files '*.go') fsm/stage3_restore.go fsm/snapshot_v21_test.go)" && echo gofmt-clean
cd "$W" && GOWORK=off go vet ./... && echo vet-clean
cd "$W" && GOWORK=off go test ./... -race -count=1 2>&1 | grep -E '^(--- FAIL|FAIL|panic)' || echo no-failures
cd "$W" && bazel run //:gazelle && git diff -- fsm/BUILD.bazel && git status --short
cd "$W" && bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors
cd "$W" && git add -A fsm README.md && git diff --cached --exit-code --diff-filter=MDR -- fsm/testdata accumulator/testdata && git diff --exit-code --diff-filter=MDR "$(git merge-base HEAD origin/main)" HEAD -- fsm/testdata accumulator/testdata && echo goldens-untouched
cd "$W" && bazel query 'deps(//fsm:fsm, 1)' | grep com_github_sentioxyz_arbiter_proto; grep -rn 'time\.Now' fsm/; echo red-lines-checked
```

Expected: `gofmt-clean`; `vet-clean`; `no-failures` (the frozen goldens `TestPreActivationHistoryIsByteIdenticalAcrossTracks`, `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`, the L3 preimage tests and Tasks 64–65's stage-3 tests included); the `fsm/BUILD.bazel` diff adds exactly `"stage3_restore.go",` to the `go_library` srcs (among the `stage3*.go` files Tasks 64 and 65 added, before `"state.go",`) and `"snapshot_v21_test.go",` to the `go_test` srcs (after `"snapshot_tracks_test.go",`), with no deps change (`data = glob(["testdata/**"])` already ships the golden), and `git status --short` lists ` M README.md`, ` M fsm/BUILD.bazel`, ` M fsm/apply_artifact_disposition_test.go`, ` M fsm/artifact_capacity_test.go`, ` M fsm/consensus_history.go`, ` M fsm/snapshot.go`, ` M fsm/snapshot_count.go`, ` M fsm/snapshot_test.go`, ` M fsm/snapshot_tracks_test.go`, ` M fsm/state_size.go`, ` M fsm/state_size_test.go`, `?? fsm/snapshot_v21_test.go`, `?? fsm/stage3_restore.go` and `?? fsm/testdata/stage3_v21_plain.snapshot`; Bazel builds and every test target passes under race (CI's command, `.github/workflows/ci.yml`); `goldens-untouched` (the only testdata change is the new file, staged as an addition); the red-line commands print only `red-lines-checked` (the fsm library imports no arbiter-proto and reads no wall clock).

- [ ] **Step 14: Commit**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" add fsm/snapshot.go fsm/snapshot_count.go fsm/state_size.go fsm/consensus_history.go fsm/stage3_restore.go fsm/snapshot_v21_test.go fsm/testdata/stage3_v21_plain.snapshot fsm/state_size_test.go fsm/apply_artifact_disposition_test.go fsm/artifact_capacity_test.go fsm/snapshot_test.go fsm/snapshot_tracks_test.go fsm/BUILD.bazel README.md
git -C "$W" commit -m "feat(stage3): snapshot v21, the stage-3 restore rules and the frozen plain container (Task 66)

Housegate spec 2026-10-14 §10.3 and clarification K70: once stage3 is
committed the writer frames v21 (AFSM, 21, a body-kind byte, the body
length, the body, the spent-ids dump) with the plain body, the v20
document plus the stage3 member; until then it writes v20 byte for byte,
so every voter can still return to the stage-2 release. The reader
accepts v3-v21, refuses an unknown v21 body kind and any stage-3 field
under an older version byte, and checks the activation and the switch
cutovers against the signed history in fsm/stage3_restore.go, which the
later stage-3 tasks extend; stage3 in the bootstrap parameters is
refused. The capacity counter and the state-size sample read the 14-byte
v21 envelope, rewriteSnapshotDocument frames either envelope, and
fsm/testdata/stage3_v21_plain.snapshot freezes the plain container of a
reproducible stage-3 network.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C "$W" status --porcelain && git -C "$W" log --oneline -1 && git -C "$W" show --stat --format= HEAD | tail -1 && shasum -a 256 "$W/fsm/testdata/stage3_v21_plain.snapshot"
```

Expected: an empty status; the new commit `feat(stage3): snapshot v21, the stage-3 restore rules and the frozen plain container (Task 66)`; `14 files changed`; and the SHA-256 Step 11 printed. Nothing is pushed (Task 70's last step pushes the branch, contract §48). Task 67 starts from this commit.

---

## Task 67: arbiter — the v21 interned body, write-then-verify in Persist, `raft.snapshot_body`, the encoding metrics, the offline replay and size measurement (S3-E2)

Spec §10.3 makes the v21 container hold "either an interned body (§13.2) or a plain body", requires `Persist`, outside the FSM lock, to encode the interned body, rehydrate it and compare a digest with one taken under the read lock, to persist the plain body and raise `arbiter_snapshot_encoding_fallback_total` on a mismatch without ever cancelling a snapshot, and asks for an operational writer setting outside the one-way switches; §13.2 asks for a lossless encoding that "rehydrates the state byte-exactly, so every existing validator ... runs unchanged". Task 66 left the writer producing only the plain v21 body and the reader refusing every body kind but 0. This task adds the interned body as a generic, byte-exact JSON interning codec (clarification K70: `fsm/snapshot_interned.go`), the write-then-verify in `Persist` on Plan S3-A Task 9's `fsmSnapshot` (K71), the node-local `raft.snapshot_body: auto|plain`, the reader for body kind 1, the series `arbiter_snapshot_encoding_fallback_total` and `arbiter_snapshot_body_bytes{kind}`, the forced-mismatch test, the frozen `fsm/testdata/stage3_v21_interned.snapshot`, and, local only, the offline replay of devnet2 voter copies under `ARBITER_TAG_P4` and the branch head together with the size measurement of the interned body (contract §46, §48).

Worktree: Task 64's (`W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e`, branch `urwt/claude/multi-source-s3e`, never re-created); every command below runs in its own Bash call that starts with `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e;` (the Bash tool keeps no variables between calls). Line anchors are arbiter `526a721` (`v0.13.0`, `origin/main` when this was drafted: Plan S3-A merged as arbiter#133, so its Task 9 code is anchored where it stands: `fsm/state_size.go`, `fsmSnapshot.size`, `recordSize`, `readSnapshotMeasured`, `metrics/snapshot.go`, the README state-size paragraph and alert block). For the files below that the header's `fb42260` also has, the hunks are the same; only the line numbers moved. Task 64 (`Params.Stage3`, the fixture), Task 66 (the v21 framing, `stage3V21GoldenFSM`, the reader's kind-byte block, the 14-byte envelope in `containerStateSize`) and, on the branch base, Plan S3-D Task 51 (`consensus_admin.max_track_settle` in `config`, the hand-built test configurations and `metrics.New`, on lines next to the ones this task edits) changed several of the anchored places first: apply each hunk on top of their change (the hunk rule of the Global Constraints); the code and the named tests are binding, the anchors are guidance.

**Files:**
- Create: `fsm/snapshot_interned.go`, `fsm/snapshot_interned_test.go`
- Create (frozen, Step 11, never rewritten): `fsm/testdata/stage3_v21_interned.snapshot`
- Modify: `fsm/snapshot.go` (imports `:3-17`; in `readSnapshotMeasured`, Task 66's v21 body-kind block right after the version check `:285-287` and the document read `:296-299`; Plan S3-A Task 9's block from `// fsmSnapshot is a point-in-time byte capture.` through the end of `func (f *FSM) Snapshot()`, `:737-782`)
- Modify: `fsm/fsm.go` (`FSM` struct `:39-53`, after Plan S3-A's `stateSize` field `:50-52`)
- Modify: `fsm/state_size.go` (Plan S3-A's `StateSizeSample.ContainerBytes` comment `:56-57`, as Task 66 left it)
- Modify: `fsm/snapshot_test.go` (`snapshotBytes` `:30-42`)
- Modify: `config/config.go` (`RaftConfig` `:31-44`; `Validate`, after the `raft.trailing_logs` check `:569-571`), `config/raw.go` (`rawRaftConfig` `:50-63`; `toConfig` `:134`; after `uint64OrDefault` `:223-228`), `config/config_test.go` (imports `:3-9`; two tests appended)
- Modify: `cmd/arbiter/services.go` (`newFSM` `:62-67`), `cmd/arbiter/services_test.go` (one test before `TestNewServerAndLoopKeepsAdminAvailableWithoutCurrentSigner` `:58`), `cmd/arbiter/main_test.go` (`applyTestDefaults` `:322`), `integration/cluster_test.go` (`testConfig` `:137`), `integration/chpipeline/cluster_test.go` (`testConfig` `:155`)
- Modify: `metrics/snapshot.go` (Plan S3-A Task 9's file: imports `:3-10`; collector appended), `metrics/metrics.go` (`New`, right before its final `return m` `:151`), `metrics/snapshot_test.go` (Plan S3-A Task 9's file: one test appended)
- Modify: `README.md` (after Plan S3-A Task 9's **State size and snapshots** paragraph `:429`; after the closing fence `:593` of its `arbiter-state-size` YAML block), `docs/specs/issue-31-metrics.md` (after Plan S3-A Task 9's `arbiter_raft_snapshot_store_bytes` row `:48`; the sentence on absent series `:50`)
- Modify (gazelle): `fsm/BUILD.bazel` (the two new files; no new dependency: `crypto/sha256` is the standard library, and `metrics` already depends on `//fsm`)
- Local only, never committed (contract §48, decision D37): the work directory `$HOME/si-devnet2-multisource-s3e2-replay` (`$T`: `helpers.sh`, `statedigest/`, `v20check/`, `snapdump/`, `v21check/`, `v21size/`, `tools.sha256`, `ledger.env`, `old_tag.txt`, `src/{old,new}`, `bin/{old,new}`, `inputs/`, `out/`), removed in Step 41; the evidence `~/src/remotesrc/claude/arbiter/multi-source-s3e.replay-e2.txt` and `~/src/remotesrc/claude/arbiter/multi-source-s3e.v21size-e2.txt`, which stay

**Interfaces:**
- Consumes:
  - Task 66 (contract §46): `snapshotVersionV21` (21), `snapshotBodyPlain byte = 0`, `snapshotBodyInterned byte = 1`, the untyped `snapshotEnvelopePrefixBytes` (13) and `snapshotV21EnvelopePrefixBytes` (14), `snapshotWriteVersion` (21 exactly when `Params.Stage3` is committed), the plain v21 container `writeSnapshot` writes (`AFSM`, 21, kind 0, 8-byte big-endian body length, body, spent-ids dump), the reader's v21 body-kind block with its refusal `unsupported snapshot v21 body kind %d`, `containerStateSize` and `readSnapshotMeasured` reading the 14-byte envelope, the test fixture `stage3V21GoldenFSM(t *testing.T) *FSM` and the frozen `fsm/testdata/stage3_v21_plain.snapshot` (`TestStage3V21PlainContainerGolden`), and its named tests (`TestSnapshotWritesAV21PlainContainerOnceStage3IsCommitted`, `TestRestoreRefusesAnUnknownV21BodyKind`, `TestContainerStateSizeMeasuresAV21PlainContainer`, `TestCountSnapshotEncodingMatchesTheV21Writer`), which read the plain container through `writeSnapshot` and forge kinds 2 and 255 as unknown (Task 66's notes), so this task changes none of them.
  - Plan S3-A Task 9 (contract §4): `fsmSnapshot{data, size}`, `(*fsmSnapshot).recordSize`, `FSM.stateSize`, `StateSizeSample`, `StateSizeSourceRestore`, `(*FSM).StateSizeSample`, `containerStateSize`, `readSnapshotMeasured` (the variables `jb` and `n` it decodes and samples), `metrics/snapshot.go`, the `stateSizeReader` pattern (`metrics/metrics.go:19-25`, read by the collector at `:333`), the metrics fake `laneSnapshot` (`metrics/client_lanes_test.go:12`), the series `arbiter_state_bytes`, `arbiter_snapshot_doc_bytes`, `arbiter_snapshot_bytes`, `arbiter_snapshot_persist_seconds`; the rehearsal tools `$HOME/si-devnet2-multisource-s3a-rehearsal/{statedigest,v20check,snapdump}/main.go` (Plan S3-A Task 11; `statedigest` sha256 `c5168a19360228da9be001633cb5cf1c2f5a0f49c45c8f90df3dc176c37ac7e4`, `v20check` `fccc6a876b79ba43f9211afb10e0448d432ee37f1c570b36560ded04c5d004ab`).
  - Existing helpers: `memSink`, `restoreInto` (`fsm/snapshot_test.go:20-54`), `restoreErr` (`fsm/apply_snapshot_query_abort_test.go:566`), `snapshotJSONDocument` (`fsm/snapshot_count_test.go:16`), `newTestFSM` (`fsm/fsm_test.go:27`), `newTrackManifestFixture` (`fsm/track_manifest_fixture_test.go:56`); `config`'s `writeConfig`, `minimal` (`config/config_test.go:11-37`); `cmd/arbiter`'s `applyTestDefaults` (`cmd/arbiter/main_test.go:312`).
  - Ledger (contract §48): `ARBITER_TAG_P4` (Plan S3-D Task 51), read with `s3-ledger.py get` (Plan S3-C1 §19). Local inputs (contract §48): Plan S2-B's `$HOME/si-devnet2-multisource-tracks/{arbiter-0-raft-after.tgz,arbiter-0-after.yaml,v20-state.bin}` and the S2 samples `$HOME/si-devnet2-multisource-s2-rehearsal/sample14` (`raft`, `local.yaml`) and `sample19` (`data`, `local.yaml`); Plan S3-B's `$HOME/si-devnet2-stage3-p0/arbiter-{0,1,2}-raft.tgz` and `arbiter-{0,1,2}.yaml` (runbook §3.3).
- Produces (contract §46 names verbatim):
  - `fsm` (`fsm/snapshot_interned.go`): `const internedSnapshotFormat = "arbiter-snapshot-interned-v1"`, `const internStringMinBytes = 64`, `const internValueMinBytes = 256`, `const maxSnapshotRehydratedDocBytes = 1 << 32`; `func InternSnapshotBody(plain []byte) ([]byte, error)`, `func RehydrateSnapshotBody(interned []byte) ([]byte, error)`; the refusals `interned body: the plain document holds the reserved key %q`, `interned body: format %q is not arbiter-snapshot-interned-v1`, `interned body: value %d references value %d, which is not an earlier one` (the root counts as value `len(values)`), `interned body: string reference %d is out of range`, `interned body: rehydrated document exceeds %d bytes`, `interned body: malformed: %v`.
  - `fsm` (`fsm/snapshot.go`): `type SnapshotBodyMode string`, `SnapshotBodyAuto SnapshotBodyMode = "auto"`, `SnapshotBodyPlain SnapshotBodyMode = "plain"`, `func (f *FSM) SetSnapshotBodyMode(mode SnapshotBodyMode) error` (`snapshot body mode must be auto or plain, got %q`), `type SnapshotEncodingStats struct { Fallbacks uint64; LastBodyKind string; LastPlainBodyBytes, LastInternedBodyBytes uint64 }`, `func (f *FSM) SnapshotEncodingStats() SnapshotEncodingStats`; `fsmSnapshot.mode SnapshotBodyMode`, `fsmSnapshot.encoding *snapshotEncodingState`; `var internSnapshotBodyFunc = InternSnapshotBody`; the reader admits body kind 1 (refusal prefix `snapshot v21 body: `).
  - `config`: `RaftConfig.SnapshotBody string` (YAML `raft.snapshot_body`, raw default `auto`), `raft.snapshot_body must be auto or plain, got %q`; `cmd/arbiter`'s `newFSM` calls `SetSnapshotBodyMode` (`raft.snapshot_body: %w`).
  - `metrics`: `arbiter_snapshot_encoding_fallback_total` (counter), `arbiter_snapshot_body_bytes{kind="plain|interned"}` (gauge), both exported from the start for a source that implements `snapshotEncodingReader interface{ SnapshotEncodingStats() fsm.SnapshotEncodingStats }` (`*fsm.FSM` does); README example alert `ArbiterSnapshotEncodingFallback`.
  - Frozen fixture `fsm/testdata/stage3_v21_interned.snapshot` (`TestStage3V21InternedContainerGolden`, `ARBITER_CAPTURE_STAGE3_V21_INTERNED_GOLDEN=1`).
  - Named tests of contract §46: `TestInternedBodyRehydratesByteExactly`, `TestInternedBodyInternsEachRepeatedValueOnce`, `TestStage3V21InternedContainerGolden`, `TestPersistWritesTheInternedBodyWhenItVerifies`, `TestPersistFallsBackToThePlainBodyOnAMismatch`, `TestPersistFallsBackWhenThePlainDocumentHoldsAReservedKey`, `TestPersistWritesThePlainBodyWhenForcedPlain`, `TestPersistStreamsAV20ContainerUnchanged`, `TestRestoreReadsBothV21Bodies`, `TestRehydrateRefusesAForwardReference`, `TestRehydrateRefusesAnExpansionAboveTheCap`, `TestStateBytesStayThePlainLengthUnderTheInternedBody`, `TestSetSnapshotBodyModeRefusesAnUnknownMode`; `TestLoadConfigReadsRaftSnapshotBody`, `TestLoadConfigRefusesAnUnknownRaftSnapshotBody`; `TestMetricsExportSnapshotEncodingFallbackAndBodyBytes`.
  - Names this task adds (Appendix B): `maxInternedDepth = 10000`, `internedBodyPrefix`, `snapshotInterner` (`value`, `container`), `appendInternedRef`, `snapshotRehydrator` (`measure`, `measureValue`, `measureRef`, `emit`, `emitValue`), `parseInternedBody`, `internedArray`, `parseInternedRef`, `addRehydrated`, `skipJSONValue`, `scanJSONString`, `scanJSONLiteral`, `isHexDigit`, `internedMalformed`; `snapshotEncodingState{mu, mode, stats}` with `bodyMode()`, the field `FSM.encoding`, `snapshotPersisted`, `(*fsmSnapshot).encode`, `verifiedInternedBody`, `(*fsmSnapshot).recordEncoding`, `snapshotBodyKindV20`, `snapshotBodyKindPlain`, `snapshotBodyKindInterned`; `config.stringOrDefault`; `metrics.snapshotEncodingCollector`, `newSnapshotEncodingCollector`; test helpers `persistedSnapshotBytes`, `v21Parts`, `internedCorpus`, `stage3V21InternedGoldenPath`, `encodingSnapshot`; extra tests `TestRehydrateRefusesMalformedBodies`, `TestInternSnapshotBodyRefusesWhatItCannotRepresent`, `TestSnapshotEncodingStatsAreRaceFreeAgainstPersist`, `TestNewFSMAppliesTheRaftSnapshotBodySetting`; local only: `$T/helpers.sh` (`rehearsal_build`, `digest_lines`, `rehearsal_replay`, `rehearsal_v21size`), `$T/v21check/main.go`, `$T/v21size/main.go`, the evidence line formats of Steps 37–39.

Notes:
- **Generic interning, not a structural codec (K70).** The interner walks the compact `encoding/json` document once: every string token of at least 64 bytes (quotes included) becomes `{"$s":i}`, every object or array whose compact encoding is at least 256 bytes becomes `{"$v":i}`, children first, the outermost value never a reference; keys, numbers, `true`, `false` and `null` are copied verbatim. A value is deduplicated by its interned form, which is equivalent to deduplicating its plain bytes (equal plain bytes intern to equal forms because their children were interned at first occurrence, and rehydration is a function), so a repeated LtHash accumulator (a 4096-character hex string), a part triple or an unchanged partition entry is one table entry. Plans S3-E3 and S3-E4 add state without touching the codec. A plain document that holds an object key beginning with `$` is refused (a reference could not be told apart from it) and so is anything that is not compact JSON; `Persist` then writes the plain body and counts the fallback.
- **Bounded rehydration.** `RehydrateSnapshotBody` measures every value's expanded length and nesting depth before it allocates, references may only name earlier values (no cycles; the root counts as the value after the last), and a body that would expand beyond `maxSnapshotRehydratedDocBytes` (4 GiB) or nest deeper than `maxInternedDepth` (10000, the depth the interner also refuses) is refused as corrupt, so a forged doubling chain costs a linear scan and a deep chain cannot exhaust the stack. The 1 GiB read cap keeps applying to the stored body (K71).
- **Write-then-verify (K71).** `Snapshot()` is unchanged in cost: it still writes the plain container under the read lock. `Persist`, on raft's snapshot goroutine and outside the lock, interns the plain body, rehydrates the result and compares SHA-256 digests of the rehydrated bytes and of the plain body `Snapshot()` captured under the lock; only on equality does it write the 14-byte v21 envelope with kind 1, the interned body and the unchanged spent-ids dump (three sink writes, the interned body never copied). Any interning error, rehydration error or digest mismatch writes the plain container exactly as captured and counts one fallback; `plain` mode writes it and counts nothing; a pre-v21 container is streamed unchanged. Only a sink error fails or cancels a snapshot, as before. `recordSize` keeps measuring the plain container, so `arbiter_state_bytes`, `arbiter_snapshot_doc_bytes`, `arbiter_snapshot_bytes` and the component gauges keep their meaning, and the new `arbiter_snapshot_body_bytes{kind}` reports what was persisted. Transient memory in `Persist` is the plain container, the interned body, the rehydrated copy and the interner's tables, about two to three plain documents (K71); Plan S3-E4's rehearsal measures it against the voter budget.
- **The writer setting.** `raft.snapshot_body` is node-local and never replicated; voters may differ and change it at any restart, since every v21 binary reads both bodies. The FSM's zero value is `auto`, so harnesses that build an FSM directly (the integration clusters, `chpipeline`) persist the interned body as production does; `config.Validate` refuses an explicit empty value like the other explicit zeros (`TestLoad_RejectsExplicitZeroDefaults`), so the three test helpers that build a `config.Config` without `Load` (`cmd/arbiter`'s `applyTestDefaults`, the two integration `testConfig`s) set `SnapshotBody: "auto"` in Step 18.
- **`snapshotBytes` writes the plain container.** The shared test helper `snapshotBytes` (516 call sites) now persists in plain mode, so every test that reads, rewrites or compares a document, Task 66's plain golden rewrite and `TestCountSnapshotEncodingMatchesTheV21Writer` among them, and Tasks 68–70's and Plans S3-E3/S3-E4's `rejectStage3Unchanged` comparisons, keeps working on v21 states; `persistedSnapshotBytes` is the auto-mode path this task's tests use. For pre-v21 states nothing changes (`Persist` streams them unchanged in either mode).
- **Measured while drafting.** On the local copy of a devnet2 v20 voter container (`$HOME/si-devnet2-multisource-tracks/v20-state.bin`, the spec's measured document at promotion 53), this codec turns the 11,385,138-byte plain document into a 1,070,604-byte interned body (9.4 %, 10.6 times smaller; the spec estimated about 1.4 MB) that rehydrates byte-identically, in under half a second for intern, rehydrate and compare together; the 11,868,252-byte document of Plan S2-B's `arbiter-0` copy after the tracks activation interns to 1,089,643 bytes. Step 39 repeats this on the current copies.
- **No consensus byte changes before the activation.** Interning applies only to a v21 container, which exists only once `Params.Stage3` is committed; every pre-v21 container is persisted byte-identically, and the existing golden tests (`TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, `TestPreActivationHistoryIsByteIdenticalAcrossTracks`, `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`, the L3 golden tests) run unchanged. Steps 37–38 prove it on devnet2's own logs against `ARBITER_TAG_P4`. `fsm` still neither reads the clock nor imports arbiter-proto (`crypto/sha256` only).
- **On top of Task 66.** The code uses only contract §46's Task 66 names: `snapshotVersionV21`, `snapshotBodyPlain`, `snapshotBodyInterned`, the untyped envelope constants (13 and 14, compared with `len` and used in slice bounds) and the reader's `jb` and `n`; it replaces Task 66's kind-byte block whole, comment included, so its shape there does not matter. The tests call `stage3V21GoldenFSM(t)` as `func(*testing.T) *FSM`, Task 66's signature and the form contract §46 uses ("the container `Persist` writes … for `stage3V21GoldenFSM(t)`"). Task 66's own v21 tests read the plain container through `writeSnapshot` (its helper `plainSnapshotBytes`), never through `Persist`, and forge body kinds 2 and 255 as unknown, so none of them changes; Step 10 says what to do if one does after all.
- **Checked while drafting** (2026-10-16, no worktree: `git archive` exports in the drafting session's scratchpad): arbiter `526a721` with Task 64's and Task 66's drafted code applied as stand-ins, on arbiter-proto at Plan S3-E1's drafted contract §39 and an arbiter-core stub of its contract §41 (both `replace` modules), then every code block of this task applied in step order with `GOWORK=off GOFLAGS=-mod=mod GOPROXY=off`: each failing run printed the stated errors; Task 66's v21 tests passed unchanged; the capture, the refused second capture and the header bytes of Step 11 were as stated; `go test ./... -count=1` passed, and so did `-race` for `fsm`, `metrics`, `config`, `cmd/arbiter`, `integration` and `integration/chpipeline`; `gofmt` and `go vet ./...` were clean and `fsm/` names no `time.Now`. Nine single-point mutations each failed their tests: the digest comparison dropped, plain mode counting a fallback, the reader keeping the stored body length, a self-reference admitted, the root interned, the expansion cap dropped, the statistics not recorded, the reserved-key check dropped, the mode not taken from the FSM. Offline Bazel did not run: gazelle did (this task adds exactly the two files to `fsm/BUILD.bazel`), but the local repository cache lacks `526a721`'s module versions, so `bazel test` needs a download. Every bash block of Steps 30–41 passed `bash -n` and `shellcheck` (with `helpers.sh` prepended; only SC1091, the sourced file) and ran end to end under a scratch `HOME` with stand-in inputs (Plan S2-B's copies standing in for the P0 copies and the samples) and `v0.13.0` (`526a721`) as the old build: identical `statedigest` lines, the 11,868,827-byte v20 container from both builds, every cross restore `restore_ok`, the old build refusing both v21 goldens at the version byte, eight `rehydrated_identical true` lines, 24 evidence lines, and the removal.

- [ ] **Step 1: Base check**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; git -C "$W" rev-parse --abbrev-ref HEAD && git -C "$W" status --porcelain && git -C "$W" log --format=%s -1 && cd "$W" && grep -c 'unsupported snapshot v21 body kind' fsm/snapshot.go && ls fsm/testdata/stage3_v21_plain.snapshot && ! ls fsm/testdata/stage3_v21_interned.snapshot 2>/dev/null && go test ./fsm/ -run 'TestSnapshotWritesAV21PlainContainerOnceStage3IsCommitted|TestStage3V21PlainContainerGolden|TestRestoreRefusesAnUnknownV21BodyKind|TestContainerStateSizeMeasuresAV21PlainContainer|TestCountSnapshotEncodingMatchesTheV21Writer|TestStateSizeSampleIsRaceFreeAgainstReads' -race -count=1`
Expected: `urwt/claude/multi-source-s3e`, an empty status, a subject ending in `(Task 66)`, `1`, `fsm/testdata/stage3_v21_plain.snapshot`, and `ok  github.com/sentioxyz/arbiter/fsm`. Anything else: Task 66 is not complete; stop.

- [ ] **Step 2: Write the failing codec tests**

Create `fsm/snapshot_interned_test.go`:

```go
package fsm

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
)

// v21Parts splits a v21 container into its body kind, body and spent-ids
// tail.
func v21Parts(t *testing.T, data []byte) (byte, []byte, []byte) {
	t.Helper()
	if len(data) < snapshotV21EnvelopePrefixBytes || string(data[:4]) != "AFSM" || data[4] != snapshotVersionV21 {
		t.Fatalf("not a v21 container: % x", data[:min(len(data), snapshotV21EnvelopePrefixBytes)])
	}
	n := binary.BigEndian.Uint64(data[6:snapshotV21EnvelopePrefixBytes])
	if n > uint64(len(data)-snapshotV21EnvelopePrefixBytes) {
		t.Fatalf("v21 body length %d exceeds the container", n)
	}
	return data[5], data[snapshotV21EnvelopePrefixBytes : snapshotV21EnvelopePrefixBytes+n], data[snapshotV21EnvelopePrefixBytes+n:]
}

// internedCorpus is every document the codec tests round-trip: the plain
// bodies of a pre-stage-3, a tracks and a stage-3 state, documents with
// escapes, deep and empty containers and every literal form, and random
// documents with repeated long strings and values.
func internedCorpus(t *testing.T) map[string][]byte {
	t.Helper()
	long := strings.Repeat("ab", 2048)
	corpus := map[string][]byte{
		"pre-stage3": snapshotJSONDocument(t, newTestFSM(t)),
		"tracks":     snapshotJSONDocument(t, newTrackManifestFixture(t).f),
		"empty":      []byte(`{}`),
		"literals":   []byte(`{"a":[0,-0,1.5,-2e10,3E+2,4.0e-3,true,false,null,""],"b":{},"c":[]}`),
		"escapes": []byte(`{"k\"ey":"` + strings.Repeat(`\"\\\/\b\f\n\r\t <`, 8) + `","x":["` +
			strings.Repeat(`é`, 20) + `","` + strings.Repeat(`é`, 20) + `"]}`),
		"repeated": []byte(fmt.Sprintf(`{"a":"%s","b":["%s","%s"],"c":{"d":"%s","e":[{"f":"%s"},{"f":"%s"}]},"g":{"d":"%s","e":[{"f":"%s"},{"f":"%s"}]}}`,
			long, long, long, long, long, long, long, long, long)),
		"deep": []byte(strings.Repeat(`{"a":[`, 200) + `"` + long + `"` + strings.Repeat(`]}`, 200)),
	}
	_, body, _ := v21Parts(t, snapshotBytes(t, stage3V21GoldenFSM(t)))
	corpus["stage3"] = body
	rng := rand.New(rand.NewPCG(1, 2))
	pool := []any{long, strings.Repeat("c", 63), strings.Repeat("d", 62), map[string]any{"x": long, "y": []any{long, 1.0}}}
	var random func(depth int) any
	random = func(depth int) any {
		switch k := rng.IntN(7); {
		case depth > 5 || k == 0:
			return pool[rng.IntN(len(pool))]
		case k == 1:
			return rng.Float64() * 1e6
		case k == 2:
			return fmt.Sprintf("s%d", rng.IntN(100))
		case k <= 4:
			m := map[string]any{}
			for i := range rng.IntN(6) {
				m[fmt.Sprintf("k%d", i)] = random(depth + 1)
			}
			return m
		default:
			a := make([]any, rng.IntN(6))
			for i := range a {
				a[i] = random(depth + 1)
			}
			return a
		}
	}
	for i := range 20 {
		doc := map[string]any{}
		for j := range 8 {
			doc[fmt.Sprintf("m%d", j)] = random(0)
		}
		b, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		corpus[fmt.Sprintf("random-%d", i)] = b
	}
	return corpus
}

// TestInternedBodyRehydratesByteExactly: housegate spec 2026-10-14 §13.2,
// K70. Every document rehydrates to exactly its plain bytes, the interned body
// is compact JSON in the fixed member order, and interning is deterministic.
func TestInternedBodyRehydratesByteExactly(t *testing.T) {
	for name, plain := range internedCorpus(t) {
		t.Run(name, func(t *testing.T) {
			interned, err := InternSnapshotBody(plain)
			if err != nil {
				t.Fatalf("InternSnapshotBody: %v", err)
			}
			if !json.Valid(interned) || !bytes.HasPrefix(interned, []byte(`{"format":"arbiter-snapshot-interned-v1","strings":[`)) {
				t.Fatalf("interned body is not the interned format: %.200s", interned)
			}
			again, err := InternSnapshotBody(plain)
			if err != nil || !bytes.Equal(again, interned) {
				t.Fatalf("interning the same document twice differs: %v", err)
			}
			got, err := RehydrateSnapshotBody(interned)
			if err != nil {
				t.Fatalf("RehydrateSnapshotBody: %v", err)
			}
			if !bytes.Equal(got, plain) {
				t.Fatalf("rehydrated %d bytes differ from the %d plain bytes", len(got), len(plain))
			}
		})
	}
}

// TestInternedBodyInternsEachRepeatedValueOnce: a repeated long string and a
// repeated large object are each one table entry, children before parents
// (the entry before the array that holds it twice), and short strings, keys
// and the root stay verbatim.
func TestInternedBodyInternsEachRepeatedValueOnce(t *testing.T) {
	long := strings.Repeat("ab", 2048)
	short := strings.Repeat("c", 61) // a 63-byte token, below internStringMinBytes
	entry := fmt.Sprintf(`{"lthash":"%s","name":"p_1_1_0","physical":"%s"}`, long, short)
	plain := []byte(fmt.Sprintf(`{"a":%s,"b":[%s,%s],"c":"%s","short":"%s"}`, entry, entry, entry, long, short))
	interned, err := InternSnapshotBody(plain)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(`{"format":"arbiter-snapshot-interned-v1","strings":["%s"],"values":[{"lthash":{"$s":0},"name":"p_1_1_0","physical":"%s"},[{"$v":0},{"$v":0}]],"root":{"a":{"$v":0},"b":{"$v":1},"c":{"$s":0},"short":"%s"}}`, long, short, short)
	if string(interned) != want {
		t.Fatalf("interned body = %.400s\nwant %.400s", interned, want)
	}
	if got := bytes.Count(interned, []byte(long)); got != 1 {
		t.Fatalf("the long string is stored %d times, want once", got)
	}
	if len(interned) >= len(plain)/3 {
		t.Fatalf("interned %d bytes of a %d-byte document", len(interned), len(plain))
	}
}

// TestRehydrateRefusesAForwardReference: a value may reference earlier values
// only, so references can never cycle; the root counts as the value after the
// last.
func TestRehydrateRefusesAForwardReference(t *testing.T) {
	for body, want := range map[string]string{
		`{"format":"arbiter-snapshot-interned-v1","strings":[],"values":[[{"$v":0}]],"root":{"a":{"$v":0}}}`:       "interned body: value 0 references value 0, which is not an earlier one",
		`{"format":"arbiter-snapshot-interned-v1","strings":[],"values":[[1],[{"$v":2}],[3]],"root":{"a":1}}`:      "interned body: value 1 references value 2, which is not an earlier one",
		`{"format":"arbiter-snapshot-interned-v1","strings":[],"values":[[1]],"root":{"a":{"$v":1}}}`:              "interned body: value 1 references value 1, which is not an earlier one",
		`{"format":"arbiter-snapshot-interned-v1","strings":["x"],"values":[],"root":{"a":{"$s":1}}}`:              "interned body: string reference 1 is out of range",
		`{"format":"arbiter-snapshot-interned-v1","strings":[],"values":[],"root":{"a":[{"b":{"$s":0}}]}}`:         "interned body: string reference 0 is out of range",
		`{"format":"arbiter-snapshot-interned-v2","strings":[],"values":[],"root":{}}`:                             "interned body: format \"arbiter-snapshot-interned-v2\" is not arbiter-snapshot-interned-v1",
		`{"format":"arbiter-snapshot-interned-v1","strings":[],"values":[[1]],"root":{"a":{"$v":0},"b":{"$v":0}}}`: "",
	} {
		_, err := RehydrateSnapshotBody([]byte(body))
		if want == "" {
			if err != nil {
				t.Fatalf("%s: %v", body, err)
			}
			continue
		}
		if err == nil || err.Error() != want {
			t.Fatalf("%s: err = %v, want %q", body, err, want)
		}
	}
}

// TestRehydrateRefusesAnExpansionAboveTheCap: a doubling chain of references
// that would expand beyond maxSnapshotRehydratedDocBytes is refused before
// anything is allocated, and so is a chain that nests deeper than
// maxInternedDepth.
func TestRehydrateRefusesAnExpansionAboveTheCap(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"format":"arbiter-snapshot-interned-v1","strings":["` + strings.Repeat("x", 62) + `"],"values":[[{"$s":0},{"$s":0}]`)
	for i := 1; i < 40; i++ {
		fmt.Fprintf(&b, `,[{"$v":%d},{"$v":%d}]`, i-1, i-1)
	}
	b.WriteString(`],"root":{"a":{"$v":39}}}`)
	_, err := RehydrateSnapshotBody([]byte(b.String()))
	if want := fmt.Sprintf("interned body: rehydrated document exceeds %d bytes", uint64(1)<<32); err == nil || err.Error() != want {
		t.Fatalf("doubling chain: err = %v, want %q", err, want)
	}
	b.Reset()
	b.WriteString(`{"format":"arbiter-snapshot-interned-v1","strings":[],"values":[[1]`)
	for i := 1; i <= maxInternedDepth; i++ {
		fmt.Fprintf(&b, `,[{"$v":%d}]`, i-1)
	}
	fmt.Fprintf(&b, `],"root":{"a":{"$v":%d}}}`, maxInternedDepth)
	if _, err := RehydrateSnapshotBody([]byte(b.String())); err == nil || !strings.Contains(err.Error(), "interned body: malformed: the document nests deeper than 10000") {
		t.Fatalf("deep chain: err = %v", err)
	}
}

// TestRehydrateRefusesMalformedBodies: anything but the exact compact format
// is refused, never guessed at.
func TestRehydrateRefusesMalformedBodies(t *testing.T) {
	const head = `{"format":"arbiter-snapshot-interned-v1","strings":[`
	for name, body := range map[string]string{
		"empty":                  ``,
		"not the format":         `{"root":{}}`,
		"whitespace":             `{"format": "arbiter-snapshot-interned-v1","strings":[],"values":[],"root":{}}`,
		"members reordered":      head + `],"root":{},"values":[]}`,
		"trailing data":          head + `],"values":[],"root":{}}x`,
		"unterminated":           head + `],"values":[],"root":{}`,
		"a number in strings":    head + `1],"values":[],"root":{}}`,
		"a string in values":     head + `],"values":["x"],"root":{}}`,
		"a reserved key":         head + `],"values":[],"root":{"$x":1}}`,
		"a reference with more":  head + `"x"],"values":[],"root":{"$s":0,"b":1}}`,
		"a leading zero":         head + `"x"],"values":[],"root":{"a":{"$s":00}}}`,
		"a negative index":       head + `"x"],"values":[],"root":{"a":{"$s":-1}}}`,
		"a bad escape":           head + `"\q"],"values":[],"root":{}}`,
		"a raw control byte":     head + "\"\x01\"],\"values\":[],\"root\":{}}",
		"a bad literal":          head + `],"values":[],"root":{"a":tru}}`,
		"a missing colon":        head + `],"values":[],"root":{"a"1}}`,
		"an unterminated string": head + `"x],"values":[],"root":{}}`,
	} {
		_, err := RehydrateSnapshotBody([]byte(body))
		if err == nil || !strings.HasPrefix(err.Error(), "interned body: malformed: ") {
			t.Errorf("%s: err = %v, want a malformed refusal", name, err)
		}
	}
}

// TestInternSnapshotBodyRefusesWhatItCannotRepresent: a key beginning with
// "$" could not be told apart from a reference, and a document that is not
// compact JSON is not the writer's; both are refused, so Persist falls back.
func TestInternSnapshotBodyRefusesWhatItCannotRepresent(t *testing.T) {
	for doc, want := range map[string]string{
		`{"a":{"$v":1}}`:             `interned body: the plain document holds the reserved key "$v"`,
		`{"a":[{"b":1,"$x":2}]}`:     `interned body: the plain document holds the reserved key "$x"`,
		`{"a": 1}`:                   "interned body: malformed: ",
		`{"a":1}{}`:                  "interned body: malformed: trailing data at byte 7",
		`{"a":[1,]}`:                 "interned body: malformed: ",
		`{"a":01}`:                   "interned body: malformed: ",
		`{"a":"` + "\x7f\x01" + `"}`: "interned body: malformed: control byte in a string at byte 7",
	} {
		_, err := InternSnapshotBody([]byte(doc))
		if err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("%q: err = %v, want %q", doc, err, want)
		}
	}
}
```

- [ ] **Step 3: Run the codec tests and see them fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./fsm/ -run 'TestInternedBody|TestRehydrate|TestInternSnapshotBody' -count=1`
Expected: FAIL to compile — `undefined: InternSnapshotBody`, `undefined: RehydrateSnapshotBody`, `undefined: maxInternedDepth` (then `too many errors`), `FAIL github.com/sentioxyz/arbiter/fsm [build failed]`.

- [ ] **Step 4: Implement the interned codec**

Create `fsm/snapshot_interned.go`:

```go
package fsm

import (
	"bytes"
	"fmt"
	"strconv"
)

// The interned snapshot body (housegate spec 2026-10-14 §13.2, contract §46,
// clarification K70) is a lossless, generic compression of the plain v21 body:
// the compact JSON document
//
//	{"format":"arbiter-snapshot-interned-v1","strings":[...],"values":[...],"root":...}
//
// in which every JSON string token of at least internStringMinBytes bytes
// (quotes included) is replaced by {"$s":i}, an index into strings, and every
// object or array whose compact encoding is at least internValueMinBytes bytes
// by {"$v":i}, an index into values. Each table entry is stored once, in
// first-occurrence order, a value's children before the value itself, so a
// repeated LtHash accumulator, part triple or unchanged partition entry costs
// one entry and one short reference per occurrence. Every token is copied
// verbatim (object keys, numbers, true, false and null are never interned) and
// the outermost value is never a reference, so rehydration reproduces the
// plain bytes exactly. The codec knows nothing of snapshotDoc: state added by
// later plans is interned without a change here.
const (
	internedSnapshotFormat = "arbiter-snapshot-interned-v1"
	// internStringMinBytes is the shortest JSON string token, quotes
	// included, that is replaced by a string reference.
	internStringMinBytes = 64
	// internValueMinBytes is the shortest compact encoding of an object or
	// array that is replaced by a value reference.
	internValueMinBytes = 256
	// maxSnapshotRehydratedDocBytes bounds the document a stored interned
	// body may expand to (K71); the 1 GiB read cap bounds the stored body.
	maxSnapshotRehydratedDocBytes = 1 << 32
	// maxInternedDepth bounds the nesting of the plain document and of a
	// rehydration, so neither recursion can exhaust the stack. encoding/json
	// refuses deeper documents too.
	maxInternedDepth = 10000
)

// internedBodyPrefix is the fixed start of every interned body.
const internedBodyPrefix = `{"format":"` + internedSnapshotFormat + `","strings":[`

// InternSnapshotBody returns the interned body of a plain snapshot body, the
// compact JSON document the v21 writer marshals. It refuses a document that is
// not compact JSON (the writer never produces one) and one that holds an
// object key beginning with "$", which a reference could not be told apart
// from; the snapshot writer then persists the plain body. The result
// rehydrates to plain byte for byte (RehydrateSnapshotBody); the writer
// verifies that before it persists the interned body.
func InternSnapshotBody(plain []byte) ([]byte, error) {
	in := snapshotInterner{plain: plain, stringIndex: map[string]int{}, valueIndex: map[string]int{}}
	if err := in.value(0); err != nil {
		return nil, err
	}
	if in.pos != len(plain) {
		return nil, internedMalformed("trailing data at byte %d", in.pos)
	}
	size := len(internedBodyPrefix) + len(`],"values":[],"root":}`) + len(in.out)
	for _, s := range in.strings {
		size += len(s) + 1
	}
	for _, v := range in.values {
		size += len(v) + 1
	}
	out := make([]byte, 0, size)
	out = append(out, internedBodyPrefix...)
	for i, s := range in.strings {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, s...)
	}
	out = append(out, `],"values":[`...)
	for i, v := range in.values {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, v...)
	}
	out = append(out, `],"root":`...)
	out = append(out, in.out...)
	return append(out, '}'), nil
}

// snapshotInterner builds the interned form of a plain document in one pass.
// out holds the interned form of the values being scanned; a finished value
// that is interned is cut from out into values and replaced by its reference,
// so out never holds more than the open values' own bytes.
type snapshotInterner struct {
	plain       []byte
	pos         int
	out         []byte
	strings     [][]byte
	stringIndex map[string]int
	values      []string
	valueIndex  map[string]int
}

func (in *snapshotInterner) value(depth int) error {
	if depth > maxInternedDepth {
		return internedMalformed("the plain document nests deeper than %d", maxInternedDepth)
	}
	if in.pos >= len(in.plain) {
		return internedMalformed("unexpected end of the plain document at byte %d", in.pos)
	}
	start := in.pos
	switch in.plain[start] {
	case '"':
		end, err := scanJSONString(in.plain, start)
		if err != nil {
			return err
		}
		in.pos = end
		token := in.plain[start:end]
		if depth == 0 || len(token) < internStringMinBytes {
			in.out = append(in.out, token...)
			return nil
		}
		i, ok := in.stringIndex[string(token)]
		if !ok {
			i = len(in.strings)
			in.strings = append(in.strings, token)
			in.stringIndex[string(token)] = i
		}
		in.out = appendInternedRef(in.out, "$s", i)
		return nil
	case '{', '[':
		mark := len(in.out)
		if err := in.container(depth); err != nil {
			return err
		}
		if depth == 0 || in.pos-start < internValueMinBytes {
			return nil
		}
		// Equal plain bytes always intern to an equal form, because their
		// children were interned at their first occurrence, so the form is
		// the deduplication key.
		form := string(in.out[mark:])
		i, ok := in.valueIndex[form]
		if !ok {
			i = len(in.values)
			in.values = append(in.values, form)
			in.valueIndex[form] = i
		}
		in.out = appendInternedRef(in.out[:mark], "$v", i)
		return nil
	default:
		end, err := scanJSONLiteral(in.plain, start)
		if err != nil {
			return err
		}
		in.pos = end
		in.out = append(in.out, in.plain[start:end]...)
		return nil
	}
}

// container interns the object or array at in.pos, copying its structure and
// keys verbatim.
func (in *snapshotInterner) container(depth int) error {
	open := in.plain[in.pos]
	closing := byte(']')
	if open == '{' {
		closing = '}'
	}
	in.out = append(in.out, open)
	in.pos++
	if in.pos < len(in.plain) && in.plain[in.pos] == closing {
		in.out = append(in.out, closing)
		in.pos++
		return nil
	}
	for {
		if open == '{' {
			if in.pos >= len(in.plain) || in.plain[in.pos] != '"' {
				return internedMalformed("object key expected at byte %d", in.pos)
			}
			end, err := scanJSONString(in.plain, in.pos)
			if err != nil {
				return err
			}
			key := in.plain[in.pos:end]
			if key[1] == '$' {
				return fmt.Errorf("interned body: the plain document holds the reserved key %q", key[1:len(key)-1])
			}
			if end >= len(in.plain) || in.plain[end] != ':' {
				return internedMalformed("colon expected at byte %d", end)
			}
			in.out = append(in.out, in.plain[in.pos:end+1]...)
			in.pos = end + 1
		}
		if err := in.value(depth + 1); err != nil {
			return err
		}
		if in.pos >= len(in.plain) {
			return internedMalformed("unterminated container at byte %d", in.pos)
		}
		switch in.plain[in.pos] {
		case ',':
			in.out = append(in.out, ',')
			in.pos++
		case closing:
			in.out = append(in.out, closing)
			in.pos++
			return nil
		default:
			return internedMalformed("comma or %q expected at byte %d", closing, in.pos)
		}
	}
}

// appendInternedRef appends the reference {"<kind>":<index>}.
func appendInternedRef(out []byte, kind string, index int) []byte {
	out = append(out, `{"`...)
	out = append(out, kind...)
	out = append(out, `":`...)
	out = strconv.AppendInt(out, int64(index), 10)
	return append(out, '}')
}

// RehydrateSnapshotBody returns the plain body an interned body stands for.
// It refuses a body that is not exactly the interned format, a reference to a
// value that is not an earlier one (so references cannot cycle), a string
// reference out of range, and a body that would expand beyond
// maxSnapshotRehydratedDocBytes or maxInternedDepth; it measures every value
// before it allocates the result, so a doubling chain of references costs
// nothing.
func RehydrateSnapshotBody(interned []byte) ([]byte, error) {
	r, root, err := parseInternedBody(interned)
	if err != nil {
		return nil, err
	}
	r.lengths = make([]uint64, len(r.values))
	r.depths = make([]int, len(r.values))
	for i, v := range r.values {
		n, depth, err := r.measure(v, i)
		if err != nil {
			return nil, err
		}
		r.lengths[i], r.depths[i] = n, depth
	}
	n, _, err := r.measure(root, len(r.values))
	if err != nil {
		return nil, err
	}
	r.out = make([]byte, 0, n)
	r.emit(root)
	return r.out, nil
}

// snapshotRehydrator holds an interned body's tables (slices of the body) and
// each value's rehydrated length and nesting depth.
type snapshotRehydrator struct {
	strings [][]byte
	values  [][]byte
	lengths []uint64
	depths  []int
	out     []byte
}

// parseInternedBody splits an interned body into its tables and root form,
// checking the JSON syntax of every element.
func parseInternedBody(b []byte) (*snapshotRehydrator, []byte, error) {
	const formatKey = `{"format":`
	if !bytes.HasPrefix(b, []byte(formatKey)) {
		return nil, nil, internedMalformed("the body does not start with %s", formatKey)
	}
	pos := len(formatKey)
	end, err := scanJSONString(b, pos)
	if err != nil {
		return nil, nil, err
	}
	if format := b[pos:end]; string(format) != `"`+internedSnapshotFormat+`"` {
		return nil, nil, fmt.Errorf("interned body: format %q is not %s", format[1:len(format)-1], internedSnapshotFormat)
	}
	pos = end
	r := &snapshotRehydrator{}
	pos, err = internedArray(b, pos, `,"strings":[`, func(start, end int) error {
		if b[start] != '"' {
			return internedMalformed("strings entry at byte %d is not a string", start)
		}
		r.strings = append(r.strings, b[start:end])
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	pos, err = internedArray(b, pos, `,"values":[`, func(start, end int) error {
		if b[start] != '{' && b[start] != '[' {
			return internedMalformed("values entry at byte %d is not an object or an array", start)
		}
		r.values = append(r.values, b[start:end])
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	const rootKey = `,"root":`
	if !bytes.HasPrefix(b[pos:], []byte(rootKey)) {
		return nil, nil, internedMalformed("root expected at byte %d", pos)
	}
	pos += len(rootKey)
	rootEnd, err := skipJSONValue(b, pos, 0)
	if err != nil {
		return nil, nil, err
	}
	if rootEnd != len(b)-1 || b[rootEnd] != '}' {
		return nil, nil, internedMalformed("the body does not end after root at byte %d", rootEnd)
	}
	return r, b[pos:rootEnd], nil
}

// internedArray reads `<key>[v,v,...]` at b[pos:], handing each element's
// span to add, and returns the position after the closing bracket.
func internedArray(b []byte, pos int, key string, add func(start, end int) error) (int, error) {
	if !bytes.HasPrefix(b[pos:], []byte(key)) {
		return 0, internedMalformed("%s expected at byte %d", key, pos)
	}
	pos += len(key)
	if pos < len(b) && b[pos] == ']' {
		return pos + 1, nil
	}
	for {
		end, err := skipJSONValue(b, pos, 0)
		if err != nil {
			return 0, err
		}
		if err := add(pos, end); err != nil {
			return 0, err
		}
		if end >= len(b) {
			return 0, internedMalformed("unterminated table at byte %d", end)
		}
		switch b[end] {
		case ',':
			pos = end + 1
		case ']':
			return end + 1, nil
		default:
			return 0, internedMalformed("comma or ']' expected at byte %d", end)
		}
	}
}

// measure returns the rehydrated length and nesting depth of form, the
// interned form of value self (the root counts as the value after the last),
// whose references must name earlier values only.
func (r *snapshotRehydrator) measure(form []byte, self int) (uint64, int, error) {
	end, n, depth, err := r.measureValue(form, 0, 0, self)
	if err != nil {
		return 0, 0, err
	}
	if end != len(form) {
		return 0, 0, internedMalformed("trailing data in value %d", self)
	}
	return n, depth, nil
}

func (r *snapshotRehydrator) measureValue(b []byte, pos, depth, self int) (end int, n uint64, maxDepth int, err error) {
	if depth > maxInternedDepth {
		return 0, 0, 0, internedMalformed("the document nests deeper than %d", maxInternedDepth)
	}
	if pos >= len(b) {
		return 0, 0, 0, internedMalformed("unexpected end of value %d", self)
	}
	switch b[pos] {
	case '"':
		end, err = scanJSONString(b, pos)
		return end, uint64(end - pos), depth, err
	case '{', '[':
		if kind, index, refEnd, ok, err := parseInternedRef(b, pos); err != nil {
			return 0, 0, 0, err
		} else if ok {
			return r.measureRef(kind, index, refEnd, depth, self)
		}
		open, closing := b[pos], byte(']')
		if open == '{' {
			closing = '}'
		}
		n, maxDepth, pos = 1, depth, pos+1
		if pos < len(b) && b[pos] == closing {
			return pos + 1, 2, maxDepth, nil
		}
		for {
			if open == '{' {
				keyEnd, err := scanJSONString(b, pos)
				if err != nil {
					return 0, 0, 0, err
				}
				if keyEnd >= len(b) || b[keyEnd] != ':' {
					return 0, 0, 0, internedMalformed("colon expected in value %d", self)
				}
				n += uint64(keyEnd + 1 - pos)
				pos = keyEnd + 1
			}
			childEnd, childN, childDepth, err := r.measureValue(b, pos, depth+1, self)
			if err != nil {
				return 0, 0, 0, err
			}
			if n, err = addRehydrated(n, childN+1); err != nil {
				return 0, 0, 0, err
			}
			maxDepth = max(maxDepth, childDepth)
			if childEnd >= len(b) {
				return 0, 0, 0, internedMalformed("unterminated container in value %d", self)
			}
			switch b[childEnd] {
			case ',':
				pos = childEnd + 1
			case closing:
				return childEnd + 1, n, maxDepth, nil
			default:
				return 0, 0, 0, internedMalformed("comma or %q expected in value %d", closing, self)
			}
		}
	default:
		end, err = scanJSONLiteral(b, pos)
		return end, uint64(end - pos), depth, err
	}
}

// measureRef checks one reference of value self and returns what it expands
// to.
func (r *snapshotRehydrator) measureRef(kind byte, index uint64, end, depth, self int) (int, uint64, int, error) {
	if kind == 's' {
		if index >= uint64(len(r.strings)) {
			return 0, 0, 0, fmt.Errorf("interned body: string reference %d is out of range", index)
		}
		return end, uint64(len(r.strings[index])), depth, nil
	}
	if index >= uint64(self) {
		return 0, 0, 0, fmt.Errorf("interned body: value %d references value %d, which is not an earlier one", self, index)
	}
	refDepth := depth + r.depths[index]
	if refDepth > maxInternedDepth {
		return 0, 0, 0, internedMalformed("the document nests deeper than %d", maxInternedDepth)
	}
	return end, r.lengths[index], refDepth, nil
}

// addRehydrated adds b to a rehydrated length, refusing a sum above the cap.
func addRehydrated(a, b uint64) (uint64, error) {
	if b > maxSnapshotRehydratedDocBytes || a > maxSnapshotRehydratedDocBytes-b {
		return 0, fmt.Errorf("interned body: rehydrated document exceeds %d bytes", uint64(maxSnapshotRehydratedDocBytes))
	}
	return a + b, nil
}

// emit appends the rehydration of a form measure accepted.
func (r *snapshotRehydrator) emit(form []byte) {
	r.emitValue(form, 0)
}

func (r *snapshotRehydrator) emitValue(b []byte, pos int) int {
	switch b[pos] {
	case '"':
		end, _ := scanJSONString(b, pos)
		r.out = append(r.out, b[pos:end]...)
		return end
	case '{', '[':
		if kind, index, end, ok, _ := parseInternedRef(b, pos); ok {
			if kind == 's' {
				r.out = append(r.out, r.strings[index]...)
			} else {
				r.emit(r.values[index])
			}
			return end
		}
		open, closing := b[pos], byte(']')
		if open == '{' {
			closing = '}'
		}
		r.out = append(r.out, open)
		pos++
		if b[pos] == closing {
			r.out = append(r.out, closing)
			return pos + 1
		}
		for {
			if open == '{' {
				keyEnd, _ := scanJSONString(b, pos)
				r.out = append(r.out, b[pos:keyEnd+1]...)
				pos = keyEnd + 1
			}
			pos = r.emitValue(b, pos)
			r.out = append(r.out, b[pos])
			if b[pos] == closing {
				return pos + 1
			}
			pos++
		}
	default:
		end, _ := scanJSONLiteral(b, pos)
		r.out = append(r.out, b[pos:end]...)
		return end
	}
}

// parseInternedRef reads a reference {"$s":N} or {"$v":N} at b[pos]. ok is
// false for any other object or array; an object whose first key begins with
// "$" and is not a reference is malformed, because the interner refuses such
// keys.
func parseInternedRef(b []byte, pos int) (kind byte, index uint64, end int, ok bool, err error) {
	if !bytes.HasPrefix(b[pos:], []byte(`{"$`)) {
		return 0, 0, 0, false, nil
	}
	if len(b) < pos+7 || (b[pos+3] != 's' && b[pos+3] != 'v') || b[pos+4] != '"' || b[pos+5] != ':' {
		return 0, 0, 0, false, internedMalformed("reserved key that is not a reference at byte %d", pos)
	}
	digits := pos + 6
	end = digits
	for end < len(b) && b[end] >= '0' && b[end] <= '9' {
		end++
	}
	if end == digits || end >= len(b) || b[end] != '}' || (b[digits] == '0' && end > digits+1) {
		return 0, 0, 0, false, internedMalformed("malformed reference at byte %d", pos)
	}
	index, perr := strconv.ParseUint(string(b[digits:end]), 10, 63)
	if perr != nil {
		return 0, 0, 0, false, internedMalformed("reference index at byte %d: %v", digits, perr)
	}
	return b[pos+3], index, end + 1, true, nil
}

// skipJSONValue returns the end of the compact JSON value at b[pos], checking
// its syntax.
func skipJSONValue(b []byte, pos, depth int) (int, error) {
	if depth > maxInternedDepth {
		return 0, internedMalformed("the body nests deeper than %d", maxInternedDepth)
	}
	if pos >= len(b) {
		return 0, internedMalformed("unexpected end of the body at byte %d", pos)
	}
	switch b[pos] {
	case '"':
		return scanJSONString(b, pos)
	case '{', '[':
		open, closing := b[pos], byte(']')
		if open == '{' {
			closing = '}'
		}
		pos++
		if pos < len(b) && b[pos] == closing {
			return pos + 1, nil
		}
		for {
			if open == '{' {
				keyEnd, err := scanJSONString(b, pos)
				if err != nil {
					return 0, err
				}
				if keyEnd >= len(b) || b[keyEnd] != ':' {
					return 0, internedMalformed("colon expected at byte %d", keyEnd)
				}
				pos = keyEnd + 1
			}
			end, err := skipJSONValue(b, pos, depth+1)
			if err != nil {
				return 0, err
			}
			if end >= len(b) {
				return 0, internedMalformed("unterminated container at byte %d", end)
			}
			switch b[end] {
			case ',':
				pos = end + 1
			case closing:
				return end + 1, nil
			default:
				return 0, internedMalformed("comma or %q expected at byte %d", closing, end)
			}
		}
	default:
		return scanJSONLiteral(b, pos)
	}
}

// scanJSONString returns the end of the JSON string token at b[pos], which
// must open with a quote. It checks escapes and refuses raw control bytes.
func scanJSONString(b []byte, pos int) (int, error) {
	if pos >= len(b) || b[pos] != '"' {
		return 0, internedMalformed("string expected at byte %d", pos)
	}
	for i := pos + 1; i < len(b); i++ {
		switch c := b[i]; {
		case c == '"':
			return i + 1, nil
		case c == '\\':
			i++
			if i >= len(b) {
				return 0, internedMalformed("unterminated escape at byte %d", i)
			}
			switch b[i] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				if i+4 >= len(b) {
					return 0, internedMalformed("short unicode escape at byte %d", i)
				}
				for _, h := range b[i+1 : i+5] {
					if !isHexDigit(h) {
						return 0, internedMalformed("bad unicode escape at byte %d", i)
					}
				}
				i += 4
			default:
				return 0, internedMalformed("bad escape at byte %d", i)
			}
		case c < 0x20:
			return 0, internedMalformed("control byte in a string at byte %d", i)
		}
	}
	return 0, internedMalformed("unterminated string at byte %d", pos)
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// scanJSONLiteral returns the end of the number, true, false or null at
// b[pos].
func scanJSONLiteral(b []byte, pos int) (int, error) {
	for _, word := range []string{"true", "false", "null"} {
		if bytes.HasPrefix(b[pos:], []byte(word)) {
			return pos + len(word), nil
		}
	}
	i := pos
	if i < len(b) && b[i] == '-' {
		i++
	}
	switch {
	case i < len(b) && b[i] == '0':
		i++
	case i < len(b) && b[i] >= '1' && b[i] <= '9':
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	default:
		return 0, internedMalformed("value expected at byte %d", pos)
	}
	if i < len(b) && b[i] == '.' {
		i++
		start := i
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
		if i == start {
			return 0, internedMalformed("fraction digits expected at byte %d", i)
		}
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		start := i
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
		if i == start {
			return 0, internedMalformed("exponent digits expected at byte %d", i)
		}
	}
	return i, nil
}

// internedMalformed is the refusal of a body or plain document the codec
// cannot parse.
func internedMalformed(format string, args ...any) error {
	return fmt.Errorf("interned body: malformed: %s", fmt.Sprintf(format, args...))
}
```

- [ ] **Step 5: Run the codec tests and register the files**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && gofmt -l fsm && go vet ./fsm/ && go test ./fsm/ -run 'TestInternedBody|TestRehydrate|TestInternSnapshotBody' -race -count=1 -v 2>&1 | grep -E '^(--- |ok|FAIL)' && bazel run //:gazelle && git -C "$W" status --short`
Expected: no `gofmt` output; `--- PASS` for `TestInternedBodyRehydratesByteExactly` (its 28 documents as subtests), `TestInternedBodyInternsEachRepeatedValueOnce`, `TestRehydrateRefusesAForwardReference`, `TestRehydrateRefusesAnExpansionAboveTheCap`, `TestRehydrateRefusesMalformedBodies` and `TestInternSnapshotBodyRefusesWhatItCannotRepresent`, then `ok`; the status lists exactly `M fsm/BUILD.bazel`, `?? fsm/snapshot_interned.go` and `?? fsm/snapshot_interned_test.go` (gazelle adds the two files to `fsm/BUILD.bazel`, nothing else).

- [ ] **Step 6: Commit the codec**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" add fsm/snapshot_interned.go fsm/snapshot_interned_test.go fsm/BUILD.bazel
git -C "$W" commit -m "feat(stage3): lossless interned snapshot body codec (Task 67)

The v21 interned body (housegate spec 2026-10-14 §13.2, clarification
K70) is a generic interning of the compact JSON document: every string
token of at least 64 bytes and every object or array of at least 256
bytes is stored once, children first, and referenced by index; keys,
numbers and literals are copied verbatim, so RehydrateSnapshotBody
reproduces the plain bytes exactly. Rehydration measures every value
before it allocates and refuses forward references, out-of-range
strings, expansion beyond 4 GiB and nesting beyond 10000, so a forged
body costs a linear scan. A plain document with a key beginning with
\"\$\" is refused; the writer then keeps the plain body.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 7: Write the failing Persist tests**

In `fsm/snapshot_test.go`, replace `snapshotBytes` (`:30-42`) with:

```go
// snapshotBytes is the plain container f's next snapshot captures, persisted
// through Persist in plain mode, so tests can read, rewrite and compare its
// document whatever the state. persistedSnapshotBytes
// (snapshot_interned_test.go) is what Persist writes in f's own
// raft.snapshot_body mode: for a v21 state in auto mode the interned body.
func snapshotBytes(t *testing.T, f *FSM) []byte {
	t.Helper()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	snap.(*fsmSnapshot).mode = SnapshotBodyPlain
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	snap.Release()
	return sink.Bytes()
}
```

In `fsm/snapshot_interned_test.go`, add `"os"` and `"reflect"` to the import block (after `"math/rand/v2"`), then append:

```go
const stage3V21InternedGoldenPath = "testdata/stage3_v21_interned.snapshot"

// persistedSnapshotBytes is the container Persist writes for f's next
// snapshot in f's raft.snapshot_body mode (auto unless set): for a v21 state
// in auto mode the interned body. snapshotBytes is always the plain container.
func persistedSnapshotBytes(t *testing.T, f *FSM) []byte {
	t.Helper()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if sink.canceled {
		t.Fatal("Persist canceled the snapshot")
	}
	snap.Release()
	return sink.Bytes()
}

// TestStage3V21InternedContainerGolden: the frozen interned container
// (contract §46) is what Persist writes in auto mode for stage3V21GoldenFSM;
// its body rehydrates to exactly the body of the frozen plain container, and
// both restore to the same state. Captured once with
// ARBITER_CAPTURE_STAGE3_V21_INTERNED_GOLDEN=1 and never rewritten.
func TestStage3V21InternedContainerGolden(t *testing.T) {
	got := persistedSnapshotBytes(t, stage3V21GoldenFSM(t))
	if os.Getenv("ARBITER_CAPTURE_STAGE3_V21_INTERNED_GOLDEN") == "1" {
		if _, err := os.Stat(stage3V21InternedGoldenPath); err == nil {
			t.Fatalf("%s exists; it is captured once and never rewritten", stage3V21InternedGoldenPath)
		}
		if err := os.WriteFile(stage3V21InternedGoldenPath, got, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Skip("captured; commit the file")
	}
	want, err := os.ReadFile(stage3V21InternedGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Persist wrote %d bytes, the frozen interned container is %d bytes", len(got), len(want))
	}
	plainContainer, err := os.ReadFile("testdata/stage3_v21_plain.snapshot")
	if err != nil {
		t.Fatal(err)
	}
	kind, body, tail := v21Parts(t, want)
	plainKind, plainBody, plainTail := v21Parts(t, plainContainer)
	if kind != snapshotBodyInterned || plainKind != snapshotBodyPlain {
		t.Fatalf("body kinds %d and %d, want 1 and 0", kind, plainKind)
	}
	rehydrated, err := RehydrateSnapshotBody(body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rehydrated, plainBody) || !bytes.Equal(tail, plainTail) {
		t.Fatal("the frozen interned container does not rehydrate to the frozen plain container")
	}
	if a, b := snapshotBytes(t, restoreInto(t, want)), snapshotBytes(t, restoreInto(t, plainContainer)); !bytes.Equal(a, b) || !bytes.Equal(a, plainContainer) {
		t.Fatal("the two frozen containers restore to different states")
	}
}

// TestPersistWritesTheInternedBodyWhenItVerifies: in auto mode a v21 state is
// persisted with the interned body, which rehydrates to the plain body
// Snapshot captured; the tail is unchanged and the statistics name the body.
func TestPersistWritesTheInternedBodyWhenItVerifies(t *testing.T) {
	f := stage3V21GoldenFSM(t)
	plainKind, plainBody, plainTail := v21Parts(t, snapshotBytes(t, f))
	got := persistedSnapshotBytes(t, f)
	kind, body, tail := v21Parts(t, got)
	if plainKind != snapshotBodyPlain || kind != snapshotBodyInterned {
		t.Fatalf("body kinds %d (snapshotBytes) and %d (Persist), want 0 and 1", plainKind, kind)
	}
	if rehydrated, err := RehydrateSnapshotBody(body); err != nil || !bytes.Equal(rehydrated, plainBody) {
		t.Fatalf("the persisted interned body does not rehydrate to the plain body: %v", err)
	}
	if !bytes.Equal(tail, plainTail) || len(body) >= len(plainBody) {
		t.Fatalf("interned body %d bytes for a %d-byte plain body; tails equal %v", len(body), len(plainBody), bytes.Equal(tail, plainTail))
	}
	want := SnapshotEncodingStats{LastBodyKind: "interned", LastPlainBodyBytes: uint64(len(plainBody)), LastInternedBodyBytes: uint64(len(body))}
	if stats := f.SnapshotEncodingStats(); stats != want {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}
}

// TestPersistFallsBackToThePlainBodyOnAMismatch: the forced-mismatch run of
// spec §10.3. An interner whose output rehydrates to bytes one byte off makes
// Persist write the plain container exactly as Snapshot captured it, count
// one fallback per snapshot and never cancel; with the real interner the next
// snapshot is interned again.
func TestPersistFallsBackToThePlainBodyOnAMismatch(t *testing.T) {
	f := stage3V21GoldenFSM(t)
	plain := snapshotBytes(t, f)
	original := internSnapshotBodyFunc
	t.Cleanup(func() { internSnapshotBodyFunc = original })
	internSnapshotBodyFunc = func(body []byte) ([]byte, error) {
		changed := bytes.Clone(body)
		i := bytes.Index(changed, []byte(`"network_id":"`))
		if i < 0 {
			return nil, fmt.Errorf("no network_id in the plain body")
		}
		changed[i+len(`"network_id":"`)]++
		return InternSnapshotBody(changed)
	}
	for want := uint64(1); want <= 2; want++ {
		if got := persistedSnapshotBytes(t, f); !bytes.Equal(got, plain) {
			t.Fatalf("persist %d: a mismatching interned body was not replaced by the plain container", want)
		}
		_, body, _ := v21Parts(t, plain)
		if stats := f.SnapshotEncodingStats(); stats != (SnapshotEncodingStats{Fallbacks: want, LastBodyKind: "plain", LastPlainBodyBytes: uint64(len(body))}) {
			t.Fatalf("persist %d: stats = %+v", want, stats)
		}
	}
	internSnapshotBodyFunc = original
	if kind, _, _ := v21Parts(t, persistedSnapshotBytes(t, f)); kind != snapshotBodyInterned {
		t.Fatalf("with the real interner the next persist wrote body kind %d", kind)
	}
	if stats := f.SnapshotEncodingStats(); stats.Fallbacks != 2 || stats.LastBodyKind != "interned" {
		t.Fatalf("stats after recovery = %+v, want two fallbacks and an interned body", stats)
	}
}

// TestPersistFallsBackWhenThePlainDocumentHoldsAReservedKey: a key beginning
// with "$" (here a node id in registration_seqs) cannot be interned, so the
// plain container is persisted and the fallback counted.
func TestPersistFallsBackWhenThePlainDocumentHoldsAReservedKey(t *testing.T) {
	f := stage3V21GoldenFSM(t)
	if f.st.RegistrationSeqs == nil {
		f.st.RegistrationSeqs = map[string]uint64{}
	}
	f.st.RegistrationSeqs["$v"] = 1
	plain := snapshotBytes(t, f)
	if got := persistedSnapshotBytes(t, f); !bytes.Equal(got, plain) {
		t.Fatal("a document with a reserved key was not persisted plain")
	}
	if stats := f.SnapshotEncodingStats(); stats.Fallbacks != 1 || stats.LastBodyKind != "plain" || stats.LastInternedBodyBytes != 0 {
		t.Fatalf("stats = %+v, want one fallback and a plain body", stats)
	}
}

// TestPersistWritesThePlainBodyWhenForcedPlain: raft.snapshot_body: plain
// writes the plain container and counts nothing; auto turns interning back on.
func TestPersistWritesThePlainBodyWhenForcedPlain(t *testing.T) {
	f := stage3V21GoldenFSM(t)
	plain := snapshotBytes(t, f)
	if err := f.SetSnapshotBodyMode(SnapshotBodyPlain); err != nil {
		t.Fatal(err)
	}
	if got := persistedSnapshotBytes(t, f); !bytes.Equal(got, plain) {
		t.Fatal("plain mode did not write the plain container")
	}
	_, body, _ := v21Parts(t, plain)
	if stats := f.SnapshotEncodingStats(); stats != (SnapshotEncodingStats{LastBodyKind: "plain", LastPlainBodyBytes: uint64(len(body))}) {
		t.Fatalf("stats = %+v", stats)
	}
	if err := f.SetSnapshotBodyMode(SnapshotBodyAuto); err != nil {
		t.Fatal(err)
	}
	if kind, _, _ := v21Parts(t, persistedSnapshotBytes(t, f)); kind != snapshotBodyInterned {
		t.Fatalf("auto mode wrote body kind %d", kind)
	}
}

// TestPersistStreamsAV20ContainerUnchanged: before the stage-3 activation the
// container is whatever writeSnapshot wrote, in either mode.
func TestPersistStreamsAV20ContainerUnchanged(t *testing.T) {
	f := newTrackManifestFixture(t).f
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	written := snap.(*fsmSnapshot).data
	for _, mode := range []SnapshotBodyMode{SnapshotBodyAuto, SnapshotBodyPlain} {
		if err := f.SetSnapshotBodyMode(mode); err != nil {
			t.Fatal(err)
		}
		got := persistedSnapshotBytes(t, f)
		if !bytes.Equal(got, written) || got[4] != snapshotVersionV20 {
			t.Fatalf("%s: Persist changed the v%d container", mode, written[4])
		}
		doc := binary.BigEndian.Uint64(written[5:snapshotEnvelopePrefixBytes])
		if stats := f.SnapshotEncodingStats(); stats != (SnapshotEncodingStats{LastBodyKind: "v20", LastPlainBodyBytes: doc}) {
			t.Fatalf("%s: stats = %+v", mode, stats)
		}
	}
}

// TestRestoreReadsBothV21Bodies: a v21 binary restores the plain and the
// interned container of the same state to the same state, and refuses an
// interned body that does not rehydrate.
func TestRestoreReadsBothV21Bodies(t *testing.T) {
	f := stage3V21GoldenFSM(t)
	plain := snapshotBytes(t, f)
	interned := persistedSnapshotBytes(t, f)
	for name, data := range map[string][]byte{"plain": plain, "interned": interned} {
		if got := snapshotBytes(t, restoreInto(t, data)); !bytes.Equal(got, plain) {
			t.Fatalf("%s container restored to a different state", name)
		}
	}
	kind, body, _ := v21Parts(t, interned)
	if kind != snapshotBodyInterned {
		t.Fatalf("body kind %d", kind)
	}
	if !bytes.HasPrefix(body, []byte(`{"format":"arbiter-snapshot-interned-v1"`)) {
		t.Fatalf("interned body starts %.60s", body)
	}
	forged := bytes.Clone(interned)
	forged[snapshotV21EnvelopePrefixBytes+len(`{"format":"arbiter-snapshot-interned-v`)] = '2'
	if err := restoreErr(t, forged); err == nil || err.Error() != `snapshot v21 body: interned body: format "arbiter-snapshot-interned-v2" is not arbiter-snapshot-interned-v1` {
		t.Fatalf("restore of a forged interned body = %v", err)
	}
	forged = bytes.Clone(interned)
	forged[5] = 2
	if err := restoreErr(t, forged); err == nil || err.Error() != "unsupported snapshot v21 body kind 2" {
		t.Fatalf("restore of body kind 2 = %v", err)
	}
}

// TestStateBytesStayThePlainLengthUnderTheInternedBody: K71. The state-size
// sample of a persisted interned snapshot, and of its restore, describe the
// plain container, so arbiter_state_bytes and the other size gauges keep
// their meaning; the persisted body is SnapshotEncodingStats'.
func TestStateBytesStayThePlainLengthUnderTheInternedBody(t *testing.T) {
	f := stage3V21GoldenFSM(t)
	plain := snapshotBytes(t, f)
	_, plainBody, plainTail := v21Parts(t, plain)
	interned := persistedSnapshotBytes(t, f)
	written, ok := f.StateSizeSample()
	if !ok {
		t.Fatal("no state-size sample after an interned persist")
	}
	if written.DocBytes != uint64(len(plainBody)) || written.SpentIDsBytes != uint64(len(plainTail)) ||
		written.StateBytes != uint64(len(plainBody)+len(plainTail)) || written.ContainerBytes != uint64(len(plain)) {
		t.Fatalf("sample %+v does not describe the %d-byte plain container", written, len(plain))
	}
	if stats := f.SnapshotEncodingStats(); stats.LastInternedBodyBytes == 0 || stats.LastInternedBodyBytes >= written.DocBytes {
		t.Fatalf("stats = %+v", stats)
	}
	g := restoreInto(t, interned)
	restored, ok := g.StateSizeSample()
	if !ok {
		t.Fatal("no state-size sample after restoring an interned container")
	}
	written.Source = StateSizeSourceRestore
	if !reflect.DeepEqual(restored, written) {
		t.Fatalf("restore sample %+v\nwant %+v", restored, written)
	}
}

// TestSetSnapshotBodyModeRefusesAnUnknownMode: only auto and plain exist; a
// refused value leaves the mode unchanged.
func TestSetSnapshotBodyModeRefusesAnUnknownMode(t *testing.T) {
	f := stage3V21GoldenFSM(t)
	if err := f.SetSnapshotBodyMode(SnapshotBodyPlain); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []SnapshotBodyMode{"", "interned", "Auto", "plain "} {
		if err := f.SetSnapshotBodyMode(mode); err == nil || err.Error() != fmt.Sprintf("snapshot body mode must be auto or plain, got %q", mode) {
			t.Fatalf("SetSnapshotBodyMode(%q) = %v", mode, err)
		}
	}
	if kind, _, _ := v21Parts(t, persistedSnapshotBytes(t, f)); kind != snapshotBodyPlain {
		t.Fatalf("a refused mode changed the writer: body kind %d", kind)
	}
}

// TestSnapshotEncodingStatsAreRaceFreeAgainstPersist: Persist records on
// raft's snapshot goroutine while scrapes read; run under -race.
func TestSnapshotEncodingStatsAreRaceFreeAgainstPersist(t *testing.T) {
	f := stage3V21GoldenFSM(t)
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 20 {
			_ = snap.Persist(&memSink{})
		}
	}()
	for range 20 {
		_ = f.SnapshotEncodingStats()
		_ = f.SetSnapshotBodyMode(SnapshotBodyAuto)
	}
	<-done
}
```

- [ ] **Step 8: Run the Persist tests and see them fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./fsm/ -run 'TestPersist|TestRestoreReadsBothV21Bodies|TestStateBytesStay|TestSetSnapshotBodyMode|TestSnapshotEncodingStats|TestStage3V21InternedContainerGolden' -count=1`
Expected: FAIL to compile — `undefined: SnapshotEncodingStats`, `f.SnapshotEncodingStats undefined (type *FSM has no field or method SnapshotEncodingStats)`, `undefined: internSnapshotBodyFunc` (then `too many errors`).

- [ ] **Step 9: Implement write-then-verify and the kind-1 reader**

In `fsm/fsm.go`, append to the `FSM` struct, right after Plan S3-A Task 9's `stateSize atomic.Pointer[StateSizeSample]` field (`:52`):

```go
	// encoding is the node-local raft.snapshot_body setting and the snapshot
	// encoding statistics (snapshot.go): never snapshotted and never read by
	// Apply.
	encoding snapshotEncodingState
```

In `fsm/snapshot.go`, add `"crypto/sha256"` to the standard-library imports (after `"bytes"`) and `"sync"` (after Task 66's `"slices"`, before Plan S3-A's `"sync/atomic"`). Replace Task 66's v21 body-kind block together with the comment right above it, right after the version check (from `// v21 names its body after the version byte` through the closing brace of the `if ver[0] == snapshotVersionV21 {` block that reads the kind byte and returns `unsupported snapshot v21 body kind %d`), with:

```go
	// v21 names its body after the version byte (clarification K70): kind 0
	// is the plain body and kind 1 the interned body, rehydrated right after
	// it is read; any other kind is refused before the length is read, so a
	// corrupt kind byte never decodes as either.
	bodyKind := snapshotBodyPlain
	if ver[0] == snapshotVersionV21 {
		var kind [1]byte
		if _, err := io.ReadFull(r, kind[:]); err != nil {
			return nil, fmt.Errorf("snapshot v21 body kind: %w", err)
		}
		if kind[0] != snapshotBodyPlain && kind[0] != snapshotBodyInterned {
			return nil, fmt.Errorf("unsupported snapshot v21 body kind %d", kind[0])
		}
		bodyKind = kind[0]
	}
```

Right after the document read (`jb := make([]byte, n)` and its `io.ReadFull`, `:296-299`) insert:

```go
	// The read cap above bounds the stored body; an interned body is
	// rehydrated to the plain document, bounded by
	// maxSnapshotRehydratedDocBytes, and everything below reads the plain
	// document exactly as for the plain body, the state-size sample included.
	if bodyKind == snapshotBodyInterned {
		plain, err := RehydrateSnapshotBody(jb)
		if err != nil {
			return nil, fmt.Errorf("snapshot v21 body: %w", err)
		}
		jb, n = plain, uint64(len(plain))
	}
```

Replace Plan S3-A Task 9's block from `// fsmSnapshot is a point-in-time byte capture.` through the closing brace of `func (f *FSM) Snapshot()` (`:737-782`; `Restore` and `replaceRestored` after it stay as they are) with:

```go
// SnapshotBodyMode is the node-local writer setting raft.snapshot_body
// (housegate spec 2026-10-14 §10.3, contract §46): which body a v21 container
// is persisted with. It is never replicated and never part of State; a voter
// may change it at any restart, and every v21 binary reads both bodies.
type SnapshotBodyMode string

const (
	// SnapshotBodyAuto persists the interned body whenever it rehydrates to
	// exactly the plain body, and the plain body otherwise. It is the default.
	SnapshotBodyAuto SnapshotBodyMode = "auto"
	// SnapshotBodyPlain always persists the plain body.
	SnapshotBodyPlain SnapshotBodyMode = "plain"
)

// SnapshotEncodingStats is this process's view of the snapshots it persisted:
// node-local, never snapshotted and never read by Apply.
type SnapshotEncodingStats struct {
	// Fallbacks counts the v21 snapshots persisted with the plain body in
	// auto mode because the interned body could not be built or did not
	// rehydrate to exactly the plain body.
	Fallbacks uint64
	// LastBodyKind is the newest persisted snapshot's body: "v20" (a pre-v21
	// container, streamed as written), "plain" or "interned"; "" before the
	// first.
	LastBodyKind string
	// LastPlainBodyBytes is that snapshot's plain JSON document length.
	LastPlainBodyBytes uint64
	// LastInternedBodyBytes is its interned body's length, 0 when it was
	// persisted with the plain body.
	LastInternedBodyBytes uint64
}

// Values of SnapshotEncodingStats.LastBodyKind.
const (
	snapshotBodyKindV20      = "v20"
	snapshotBodyKindPlain    = "plain"
	snapshotBodyKindInterned = "interned"
)

// snapshotEncodingState is the FSM's node-local writer setting and encoding
// statistics.
type snapshotEncodingState struct {
	mu    sync.Mutex
	mode  SnapshotBodyMode // "" is SnapshotBodyAuto
	stats SnapshotEncodingStats
}

// internSnapshotBodyFunc is the interner Persist uses: InternSnapshotBody in
// production; the forced-mismatch test swaps it.
var internSnapshotBodyFunc = InternSnapshotBody

// SetSnapshotBodyMode sets this node's raft.snapshot_body; it applies to every
// snapshot taken afterwards.
func (f *FSM) SetSnapshotBodyMode(mode SnapshotBodyMode) error {
	if mode != SnapshotBodyAuto && mode != SnapshotBodyPlain {
		return fmt.Errorf("snapshot body mode must be auto or plain, got %q", mode)
	}
	f.encoding.mu.Lock()
	defer f.encoding.mu.Unlock()
	f.encoding.mode = mode
	return nil
}

// SnapshotEncodingStats returns a copy of this process's snapshot encoding
// statistics.
func (f *FSM) SnapshotEncodingStats() SnapshotEncodingStats {
	f.encoding.mu.Lock()
	defer f.encoding.mu.Unlock()
	return f.encoding.stats
}

func (e *snapshotEncodingState) bodyMode() SnapshotBodyMode {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mode == "" {
		return SnapshotBodyAuto
	}
	return e.mode
}

// fsmSnapshot is a point-in-time byte capture. Snapshot serializes the whole
// state under the read lock on raft's FSM goroutine, so its time is an Apply
// stall and it measures nothing; Persist encodes and streams the bytes on
// raft's snapshot goroutine and only then, outside the FSM lock, samples their
// size.
type fsmSnapshot struct {
	// data is the plain container writeSnapshot wrote.
	data []byte
	// size receives the sample once the sink holds the bytes; nil records
	// nothing.
	size *atomic.Pointer[StateSizeSample]
	// mode is the writer setting when the snapshot was taken.
	mode SnapshotBodyMode
	// encoding receives the encoding statistics once the sink holds the
	// bytes; nil records nothing.
	encoding *snapshotEncodingState
}

// snapshotPersisted describes the container Persist wrote.
type snapshotPersisted struct {
	kind                    string
	plainBody, internedBody uint64
	fallback                bool
}

// Persist implements raft.FSMSnapshot (write-then-verify, housegate spec
// 2026-10-14 §10.3, clarification K71). A v21 container in auto mode is
// persisted with the interned body once that body rehydrates to bytes whose
// SHA-256 equals that of the plain body Snapshot captured under the read lock;
// otherwise, and in plain mode, the plain container is persisted unchanged. A
// fallback is counted, never an error: only a sink failure fails or cancels a
// snapshot. Pre-v21 containers are streamed as written.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	chunks, persisted := s.encode()
	for _, chunk := range chunks {
		if _, err := sink.Write(chunk); err != nil {
			sink.Cancel()
			return err
		}
	}
	if err := sink.Close(); err != nil {
		return err
	}
	s.recordSize()
	s.recordEncoding(persisted)
	return nil
}

// encode returns the container to persist, in chunks so the interned body is
// never copied, and what it holds.
func (s *fsmSnapshot) encode() ([][]byte, snapshotPersisted) {
	data := s.data
	if len(data) < snapshotV21EnvelopePrefixBytes || data[4] != snapshotVersionV21 {
		var doc uint64
		if len(data) >= snapshotEnvelopePrefixBytes {
			doc = binary.BigEndian.Uint64(data[5:snapshotEnvelopePrefixBytes])
		}
		return [][]byte{data}, snapshotPersisted{kind: snapshotBodyKindV20, plainBody: doc}
	}
	n := binary.BigEndian.Uint64(data[6:snapshotV21EnvelopePrefixBytes])
	persisted := snapshotPersisted{kind: snapshotBodyKindPlain, plainBody: n}
	if s.mode == SnapshotBodyPlain {
		return [][]byte{data}, persisted
	}
	if data[5] != snapshotBodyPlain || n > uint64(len(data)-snapshotV21EnvelopePrefixBytes) {
		persisted.fallback = true
		return [][]byte{data}, persisted
	}
	plain := data[snapshotV21EnvelopePrefixBytes : snapshotV21EnvelopePrefixBytes+n]
	interned, ok := verifiedInternedBody(plain)
	if !ok {
		persisted.fallback = true
		return [][]byte{data}, persisted
	}
	header := make([]byte, snapshotV21EnvelopePrefixBytes)
	copy(header, data[:5])
	header[5] = snapshotBodyInterned
	binary.BigEndian.PutUint64(header[6:], uint64(len(interned)))
	persisted.kind, persisted.internedBody = snapshotBodyKindInterned, uint64(len(interned))
	return [][]byte{header, interned, data[snapshotV21EnvelopePrefixBytes+n:]}, persisted
}

// verifiedInternedBody interns plain and returns the interned body only if it
// rehydrates to bytes with plain's SHA-256.
func verifiedInternedBody(plain []byte) ([]byte, bool) {
	interned, err := internSnapshotBodyFunc(plain)
	if err != nil {
		return nil, false
	}
	rehydrated, err := RehydrateSnapshotBody(interned)
	if err != nil || sha256.Sum256(rehydrated) != sha256.Sum256(plain) {
		return nil, false
	}
	return interned, true
}

// recordSize stores this snapshot's state-size sample. It always measures
// the plain container, so arbiter_state_bytes and the other size gauges keep
// describing the plain document whichever body was persisted (K71). A
// container the scan cannot split, which writeSnapshot never produces, leaves
// the previous one.
func (s *fsmSnapshot) recordSize() {
	if s.size == nil {
		return
	}
	if sample, err := containerStateSize(s.data); err == nil {
		s.size.Store(&sample)
	}
}

// recordEncoding stores what this persist wrote.
func (s *fsmSnapshot) recordEncoding(p snapshotPersisted) {
	if s.encoding == nil {
		return
	}
	s.encoding.mu.Lock()
	defer s.encoding.mu.Unlock()
	if p.fallback {
		s.encoding.stats.Fallbacks++
	}
	s.encoding.stats.LastBodyKind = p.kind
	s.encoding.stats.LastPlainBodyBytes = p.plainBody
	s.encoding.stats.LastInternedBodyBytes = p.internedBody
}

func (s *fsmSnapshot) Release() {}

// Snapshot implements raft.FSM. It writes the plain container under the read
// lock, as before v21; encoding the body is Persist's, outside the lock.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var buf bytes.Buffer
	if err := writeSnapshot(&buf, f.st); err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: buf.Bytes(), size: &f.stateSize, mode: f.encoding.bodyMode(), encoding: &f.encoding}, nil
}
```

In `fsm/state_size.go`, replace the comment of `StateSizeSample.ContainerBytes` (Plan S3-A's, as Task 66 left it) with:

```go
	// ContainerBytes is the plain container: the envelope (13 bytes, 14 for
	// v21), the document and the dump. It is a Raft snapshot's state.bin
	// unless the snapshot was persisted with the v21 interned body, whose
	// length is SnapshotEncodingStats' (housegate spec 2026-10-14 §13.2,
	// clarification K71).
```

- [ ] **Step 10: Run the Persist tests and Task 66's v21 tests**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && gofmt -l fsm && go vet ./fsm/ && go test ./fsm/ -run 'TestPersist|TestRestoreReadsBothV21Bodies|TestStateBytesStay|TestSetSnapshotBodyMode|TestSnapshotEncodingStats|TestStage3V21InternedContainerGolden|TestSnapshotWritesAV21PlainContainerOnceStage3IsCommitted|TestStage3V21PlainContainerGolden|TestRestoreRefusesAnUnknownV21BodyKind|TestContainerStateSizeMeasuresAV21PlainContainer|TestCountSnapshotEncodingMatchesTheV21Writer' -race -count=1 -v 2>&1 | grep -E '^(--- |    snapshot_interned_test|ok|FAIL)'`
Expected: `--- PASS` for every named test except `--- FAIL: TestStage3V21InternedContainerGolden` with `snapshot_interned_test.go:<line>: open testdata/stage3_v21_interned.snapshot: no such file or directory` (the golden is captured in Step 11). Task 66's v21 tests pass unchanged (they read the plain container through `writeSnapshot` and forge kinds 2 and 255). Should one of them fail only because it persisted a v21 state through `Snapshot()` and `Persist` itself and now receives body kind 1 (auto mode), switch it to `snapshotBytes` (the plain container); should one forge kind 1 as unknown, use 2 (Review Focus 3 names kind 2), since kind 1 is now the interned body; Step 13's `git add -u fsm` takes either change. Any other failure: stop.

- [ ] **Step 11: Capture the frozen interned golden**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && ARBITER_CAPTURE_STAGE3_V21_INTERNED_GOLDEN=1 go test ./fsm/ -run '^TestStage3V21InternedContainerGolden$' -count=1 -v 2>&1 | grep -E 'captured|SKIP' && go test ./fsm/ -run '^TestStage3V21InternedContainerGolden$' -count=3 -v 2>&1 | grep -E '^(--- |ok)' && ARBITER_CAPTURE_STAGE3_V21_INTERNED_GOLDEN=1 go test ./fsm/ -run '^TestStage3V21InternedContainerGolden$' -count=1 2>&1 | grep -c 'it is captured once and never rewritten'; head -c 6 fsm/testdata/stage3_v21_interned.snapshot | xxd -p`
Expected: `captured; commit the file` and `--- SKIP`, then three `--- PASS: TestStage3V21InternedContainerGolden` and `ok` (the interned body is a pure function of the frozen plain body, so every run writes the same bytes), then `1` (a second capture is refused), then `4146534d1501` (`AFSM`, version 21, body kind 1). The file is never rewritten after this step.

- [ ] **Step 12: Run the fsm package, its goldens and the red lines**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./fsm/ -race -count=1 && go test ./fsm/ -run 'TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims|TestPreActivationHistoryIsByteIdenticalAcrossTracks|TestL3CommitmentPreimagesAreFrozen|TestL3TrackHeaderPreimagesAreFrozen|TestL3TransitionHeaderPreimageIsFrozen|TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease|TestStateComponentsListEverySnapshotDocMember|TestSnapshotRecordsAStateSizeSample|TestRestoreRecordsAStateSizeSample' -count=1 -v 2>&1 | grep -c '^--- PASS' && git -C "$W" status --short && grep -rn 'time\.Now' fsm/; bazel query 'deps(//fsm:fsm, 1)' | grep -c com_github_sentioxyz_arbiter_proto`
Expected: `ok  github.com/sentioxyz/arbiter/fsm`; `9`; the status lists `M fsm/fsm.go`, `M fsm/snapshot.go`, `M fsm/snapshot_interned_test.go`, `M fsm/snapshot_test.go`, `M fsm/state_size.go`, `?? fsm/testdata/stage3_v21_interned.snapshot` (and any Task 66 test adjusted in Step 10); the `grep` prints nothing; `0`.

- [ ] **Step 13: Commit write-then-verify and the golden**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" add fsm/fsm.go fsm/snapshot.go fsm/state_size.go fsm/snapshot_test.go fsm/snapshot_interned_test.go fsm/testdata/stage3_v21_interned.snapshot
git -C "$W" add -u fsm
git -C "$W" commit -m "feat(stage3): write-then-verify the v21 interned body in Persist (Task 67)

Snapshot still writes the plain v21 container under the read lock.
Persist, outside the lock, interns the body, rehydrates it and compares
the SHA-256 of the result with that of the plain body Snapshot captured
(housegate spec 2026-10-14 §10.3, clarification K71); only on equality
does it write body kind 1. Any mismatch or interning error writes the
plain container as captured and counts a fallback, plain mode writes it
and counts nothing, and pre-v21 containers stream unchanged; only a sink
error fails a snapshot. The reader rehydrates kind 1 and decodes the
plain document exactly as kind 0, so the state-size sample, and with it
arbiter_state_bytes, keeps describing the plain container. snapshotBytes
now persists the plain container for every test that reads a document.
Frozen: fsm/testdata/stage3_v21_interned.snapshot.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 14: Write the failing config tests**

In `config/config_test.go`, add `"fmt"` to the import block (before `"os"`, `:4`) and append:

```go
// TestLoadConfigReadsRaftSnapshotBody: raft.snapshot_body is the node-local
// v21 writer setting (housegate spec 2026-10-14 §10.3, contract §46); omitted
// means auto.
func TestLoadConfigReadsRaftSnapshotBody(t *testing.T) {
	for body, want := range map[string]string{
		minimal: "auto",
		strings.Replace(minimal, "  data_dir: /tmp/arb", "  data_dir: /tmp/arb\n  snapshot_body: auto", 1):  "auto",
		strings.Replace(minimal, "  data_dir: /tmp/arb", "  data_dir: /tmp/arb\n  snapshot_body: plain", 1): "plain",
	} {
		c, err := Load(writeConfig(t, body))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if c.Raft.SnapshotBody != want {
			t.Fatalf("raft.snapshot_body = %q, want %q", c.Raft.SnapshotBody, want)
		}
	}
}

// TestLoadConfigRefusesAnUnknownRaftSnapshotBody: only auto and plain exist,
// and an explicit empty value is refused like the other explicit zeros.
func TestLoadConfigRefusesAnUnknownRaftSnapshotBody(t *testing.T) {
	for _, value := range []string{`interned`, `Plain`, `""`} {
		body := strings.Replace(minimal, "  data_dir: /tmp/arb", "  data_dir: /tmp/arb\n  snapshot_body: "+value, 1)
		_, err := Load(writeConfig(t, body))
		want := fmt.Sprintf("raft.snapshot_body must be auto or plain, got %q", strings.Trim(value, `"`))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("snapshot_body %s: Load error = %v, want one containing %q", value, err, want)
		}
	}
}
```

- [ ] **Step 15: Run the config tests and see them fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./config/ -run 'TestLoadConfigReadsRaftSnapshotBody|TestLoadConfigRefusesAnUnknownRaftSnapshotBody' -count=1`
Expected: FAIL to compile — `c.Raft.SnapshotBody undefined (type RaftConfig has no field or method SnapshotBody)` (twice), `FAIL github.com/sentioxyz/arbiter/config [build failed]`.

- [ ] **Step 16: Implement `raft.snapshot_body` in config**

In `config/config.go`, append to `RaftConfig` (after `TrailingLogs`, `:43`):

```go
	// SnapshotBody is the node-local snapshot writer setting (housegate spec
	// 2026-10-14 §10.3): "auto" (the default) persists a v21 snapshot with
	// the interned body whenever it rehydrates to exactly the plain body,
	// "plain" always with the plain body. It is not a consensus parameter:
	// voters may differ and may change it at any restart, and every v21
	// binary reads both bodies. The values are fsm.SnapshotBodyAuto and
	// fsm.SnapshotBodyPlain.
	SnapshotBody string `yaml:"snapshot_body"`
```

and in `Validate`, right after the `raft.trailing_logs must be positive` check (`:569-571`):

```go
	if c.Raft.SnapshotBody != "auto" && c.Raft.SnapshotBody != "plain" {
		errs = append(errs, fmt.Errorf("raft.snapshot_body must be auto or plain, got %q", c.Raft.SnapshotBody))
	}
```

In `config/raw.go`, append to `rawRaftConfig` (after `TrailingLogs`, `:62`):

```go
	SnapshotBody       *string    `yaml:"snapshot_body"`
```

add to the `RaftConfig` literal of `toConfig` (after `TrailingLogs:`, `:134`):

```go
			SnapshotBody:       stringOrDefault(r.Raft.SnapshotBody, "auto"),
```

and append after `uint64OrDefault` (`:223-228`):

```go

func stringOrDefault(v *string, fallback string) string {
	if v != nil {
		return *v
	}
	return fallback
}
```

- [ ] **Step 17: Run the config tests**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && gofmt -l config && go test ./config/ -count=1 -v -run 'TestLoadConfigReadsRaftSnapshotBody|TestLoadConfigRefusesAnUnknownRaftSnapshotBody|TestLoad_RejectsExplicitZeroDefaults|TestLoad_DefaultsApplied' 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: four `--- PASS` lines and `ok  github.com/sentioxyz/arbiter/config`. (`cmd/arbiter` and `integration` now fail `config.Validate` with `raft.snapshot_body must be auto or plain, got ""` for the configs their helpers build without `Load`; Step 18 gives them the value.)

- [ ] **Step 18: Write the failing wiring test and set the value in the code-built test configs**

In `cmd/arbiter/services_test.go`, insert before `func TestNewServerAndLoopKeepsAdminAvailableWithoutCurrentSigner` (`:58`):

```go
// TestNewFSMAppliesTheRaftSnapshotBodySetting: newFSM hands raft.snapshot_body
// to the FSM (housegate spec 2026-10-14 §10.3, contract §46); a value
// config.Validate refuses is refused here too, so a caller that skips Validate
// never starts with an unknown writer mode.
func TestNewFSMAppliesTheRaftSnapshotBodySetting(t *testing.T) {
	for _, mode := range []string{"auto", "plain", "interned"} {
		cfg := config.Config{Genesis: config.GenesisConfig{
			NetworkID: "testnet-main", SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "housegate-replay-mvp-v0",
		}}
		applyTestDefaults(&cfg)
		cfg.Raft.SnapshotBody = mode
		_, _, err := newFSM(cfg)
		if mode != "interned" {
			if err != nil {
				t.Fatalf("raft.snapshot_body %s: %v", mode, err)
			}
			continue
		}
		if err == nil || err.Error() != `raft.snapshot_body: snapshot body mode must be auto or plain, got "interned"` {
			t.Fatalf("raft.snapshot_body interned: newFSM = %v", err)
		}
	}
}

```

In `cmd/arbiter/main_test.go`'s `applyTestDefaults`, after `c.Raft.TrailingLogs = 10240` (`:322`), add:

```go
	c.Raft.SnapshotBody = "auto"
```

In `integration/cluster_test.go` (`:137`) and `integration/chpipeline/cluster_test.go` (`:155`), the `testConfig` line `SnapshotThreshold:  8192, TrailingLogs: 10240,` becomes:

```go
			SnapshotThreshold:  8192, TrailingLogs: 10240, SnapshotBody: "auto",
```

- [ ] **Step 19: Run the wiring test and see it fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./cmd/arbiter/ -run 'TestNewFSMAppliesTheRaftSnapshotBodySetting' -count=1`
Expected: `--- FAIL: TestNewFSMAppliesTheRaftSnapshotBodySetting` with `services_test.go:<line>: raft.snapshot_body interned: newFSM = <nil>`: `newFSM` does not hand the setting to the FSM yet.

- [ ] **Step 20: Wire `newFSM`**

In `cmd/arbiter/services.go`'s `newFSM`, right after `state, err := fsm.NewWithNotify(configuredFSMParams(cfg), events)` and its `if err != nil { return nil, nil, err }` (`:64-67`), insert:

```go
	// raft.snapshot_body is node-local: it picks the body this voter's own
	// v21 snapshots are persisted with and is never replicated.
	if err := state.SetSnapshotBodyMode(fsm.SnapshotBodyMode(cfg.Raft.SnapshotBody)); err != nil {
		return nil, nil, fmt.Errorf("raft.snapshot_body: %w", err)
	}
```

- [ ] **Step 21: Run config, cmd/arbiter and the integration harnesses**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && gofmt -l cmd integration && go vet ./cmd/... ./integration/... && go test ./config/ ./cmd/arbiter/ ./integration/ ./integration/chpipeline/ -count=1 2>&1 | grep -E '^(ok|FAIL|---)'`
Expected: four `ok` lines (`config`, `cmd/arbiter`, `integration`, `integration/chpipeline`) and no `FAIL`.

- [ ] **Step 22: Commit the writer setting**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" add config/config.go config/raw.go config/config_test.go cmd/arbiter/services.go cmd/arbiter/services_test.go cmd/arbiter/main_test.go integration/cluster_test.go integration/chpipeline/cluster_test.go
git -C "$W" commit -m "feat(stage3): raft.snapshot_body writer setting (Task 67)

raft.snapshot_body (auto, the default, or plain) is the node-local v21
writer setting of housegate spec 2026-10-14 §10.3: plain forces the
plain body without a fallback count, auto writes the interned body once
it verifies. It is never replicated, voters may differ, and every v21
binary reads both bodies. newFSM hands it to the FSM; config.Validate
refuses any other value, an explicit empty one included, so the test
configs built without Load name auto.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 23: Write the failing metrics test**

Append to `metrics/snapshot_test.go` (Plan S3-A Task 9's file; it already imports `fmt`, `strings`, `testutil` and `fsm`):

```go
// encodingSnapshot is a collector source that also has snapshot encoding
// statistics, as *fsm.FSM does.
type encodingSnapshot struct {
	laneSnapshot
	stats fsm.SnapshotEncodingStats
}

func (s encodingSnapshot) SnapshotEncodingStats() fsm.SnapshotEncodingStats { return s.stats }

// *fsm.FSM is the collector source cmd/arbiter passes; it must offer the
// encoding statistics.
var _ snapshotEncodingReader = (*fsm.FSM)(nil)

// TestMetricsExportSnapshotEncodingFallbackAndBodyBytes: housegate spec
// 2026-10-14 §10.3, contract §46. The fallback counter and both body-length
// series exist from the start for a source with encoding statistics, carry
// the newest persisted snapshot, and are absent for a source without them.
func TestMetricsExportSnapshotEncodingFallbackAndBodyBytes(t *testing.T) {
	names := []string{"arbiter_snapshot_encoding_fallback_total", "arbiter_snapshot_body_bytes"}
	for _, name := range names {
		if n, err := testutil.GatherAndCount(New(laneSnapshot{}).registry, name); err != nil || n != 0 {
			t.Fatalf("%s for a source without encoding statistics: %d series, %v", name, n, err)
		}
	}
	const want = `
# HELP arbiter_snapshot_body_bytes Body lengths of the newest snapshot this process persisted, by kind: plain is its plain JSON document, interned the interned body it persisted (0 when it persisted the plain body or a pre-v21 container); both 0 before the first persist.
# TYPE arbiter_snapshot_body_bytes gauge
arbiter_snapshot_body_bytes{kind="interned"} %d
arbiter_snapshot_body_bytes{kind="plain"} %d
# HELP arbiter_snapshot_encoding_fallback_total v21 snapshots this process persisted with the plain body because the interned body could not be built or did not rehydrate to exactly the plain body; raft.snapshot_body: plain counts nothing. Any increase is a codec defect to investigate.
# TYPE arbiter_snapshot_encoding_fallback_total counter
arbiter_snapshot_encoding_fallback_total %d
`
	for name, stats := range map[string]fsm.SnapshotEncodingStats{
		"before a persist": {},
		"interned":         {LastBodyKind: "interned", LastPlainBodyBytes: 11385138, LastInternedBodyBytes: 1400000},
		"after fallbacks":  {Fallbacks: 3, LastBodyKind: "plain", LastPlainBodyBytes: 11385138},
		"a v20 container":  {LastBodyKind: "v20", LastPlainBodyBytes: 11385125},
	} {
		m := New(encodingSnapshot{stats: stats})
		expected := fmt.Sprintf(want, stats.LastInternedBodyBytes, stats.LastPlainBodyBytes, stats.Fallbacks)
		if err := testutil.GatherAndCompare(m.registry, strings.NewReader(expected), names...); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
```

- [ ] **Step 24: Run the metrics test and see it fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./metrics/ -run TestMetricsExportSnapshotEncodingFallbackAndBodyBytes -count=1`
Expected: FAIL to compile — `undefined: snapshotEncodingReader`, `FAIL github.com/sentioxyz/arbiter/metrics [build failed]`.

- [ ] **Step 25: Implement the encoding collector**

In `metrics/snapshot.go`, add the import group `"github.com/sentioxyz/arbiter/fsm"` after `"github.com/prometheus/client_golang/prometheus"` (`:9`, separated by a blank line), and append:

```go
// snapshotEncodingReader is the optional node-local snapshot-encoding half of
// a collector source: *fsm.FSM's SnapshotEncodingStats (housegate spec
// 2026-10-14 §10.3, contract §46). Like stateSizeReader it is not part of
// MetricsSnapshot, the replicated inventory replicas report identically.
type snapshotEncodingReader interface {
	SnapshotEncodingStats() fsm.SnapshotEncodingStats
}

// snapshotEncodingCollector exports the v21 writer's fallback counter and
// the body lengths of the newest snapshot this process persisted. Both series
// exist from the start, so the first fallback is an increase.
type snapshotEncodingCollector struct {
	source    snapshotEncodingReader
	fallbacks *prometheus.Desc
	bodyBytes *prometheus.Desc
}

func newSnapshotEncodingCollector(source snapshotEncodingReader) *snapshotEncodingCollector {
	return &snapshotEncodingCollector{
		source:    source,
		fallbacks: prometheus.NewDesc("arbiter_snapshot_encoding_fallback_total", "v21 snapshots this process persisted with the plain body because the interned body could not be built or did not rehydrate to exactly the plain body; raft.snapshot_body: plain counts nothing. Any increase is a codec defect to investigate.", nil, nil),
		bodyBytes: prometheus.NewDesc("arbiter_snapshot_body_bytes", "Body lengths of the newest snapshot this process persisted, by kind: plain is its plain JSON document, interned the interned body it persisted (0 when it persisted the plain body or a pre-v21 container); both 0 before the first persist.", []string{"kind"}, nil),
	}
}

func (c *snapshotEncodingCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.fallbacks
	ch <- c.bodyBytes
}

func (c *snapshotEncodingCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.source.SnapshotEncodingStats()
	ch <- prometheus.MustNewConstMetric(c.fallbacks, prometheus.CounterValue, float64(s.Fallbacks))
	ch <- prometheus.MustNewConstMetric(c.bodyBytes, prometheus.GaugeValue, float64(s.LastPlainBodyBytes), "plain")
	ch <- prometheus.MustNewConstMetric(c.bodyBytes, prometheus.GaugeValue, float64(s.LastInternedBodyBytes), "interned")
}
```

In `metrics/metrics.go`'s `New`, right before its final `return m` (`:151`, after Plan S3-A Task 8's hold block and whatever Plan S3-D Task 51 registered there), insert:

```go
	// The v21 encoding series, for a source that has them (*fsm.FSM).
	if encoding, ok := source.(snapshotEncodingReader); ok {
		m.registry.MustRegister(newSnapshotEncodingCollector(encoding))
	}
```

- [ ] **Step 26: Run the metrics tests**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && gofmt -l metrics && go vet ./metrics/ && go test ./metrics/ -race -count=1 -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: `--- PASS: TestMetricsExportSnapshotEncodingFallbackAndBodyBytes` among the unchanged `TestStateSizeMetricsAppearOnlyAfterASample`, `TestReadCapGaugeIsAlwaysEmitted`, `TestSnapshotFailureCountersStartAtZero`, `TestSnapshotTimingMetrics`, `TestExpositionUsesCurrentStateAndBoundedLabels` and the other existing tests, then `ok` (the existing fakes implement no `SnapshotEncodingStats`, so their expositions are unchanged).

- [ ] **Step 27: Documentation**

`README.md`, right after Plan S3-A Task 9's paragraph that starts `**State size and snapshots** (housegate spec 2026-10-14 §13.1).` (`:429`), insert a blank line and this paragraph (one line):

```markdown
**Snapshot encoding (v21)** (housegate spec 2026-10-14 §10.3, §13.2). Once `stage3` is committed every snapshot is a v21 container, which holds either the plain body (the JSON document as v20 lays it out, plus `stage3`) or the interned body: the same document with every JSON string of at least 64 bytes and every object or array of at least 256 bytes stored once in a table and referenced by index, so repeated LtHash accumulators, part triples and unchanged partition entries cost one entry each. `FSM.Snapshot` still writes the plain container under the read lock; `Persist`, outside the lock, interns its body, rehydrates the result and compares the SHA-256 of the rehydrated bytes with that of the plain body, and persists the interned body only on a match. Otherwise it persists the plain body and counts `arbiter_snapshot_encoding_fallback_total`; a fallback never cancels a snapshot, so compaction continues. `raft.snapshot_body` is the node-local writer setting: `auto` (the default) as described, `plain` always the plain body (counting nothing); it is not a consensus parameter, voters may differ, and every v21 binary reads both bodies, so it can be changed at any restart. `arbiter_snapshot_body_bytes{kind="plain|interned"}` are the newest persisted snapshot's plain document length and interned body length (`interned` 0 when it persisted plain). `arbiter_state_bytes`, `arbiter_state_component_bytes`, `arbiter_snapshot_doc_bytes` and `arbiter_snapshot_bytes` keep describing the plain container whichever body was persisted; the 1 GiB read cap applies to the stored body, and a body that would rehydrate beyond 4 GiB is refused as corrupt. `arbiter_snapshot_persist_seconds` includes the encoding and its check. Older binaries refuse every v21 container at its version byte.
```

After the closing fence of Plan S3-A Task 9's `arbiter-state-size` YAML block (the block ending with `ArbiterSnapshotRestoreFailed`, `:593`) and the blank line after it, insert this text followed by one blank line, so the `arbiter_seal_to_safe_seconds` paragraph after it still starts after a blank line:

````markdown
`ArbiterSnapshotEncodingFallback` pages on every v21 snapshot that was persisted with the plain body because its interned body did not verify. Nothing is lost (the plain body is always correct), but the encoder has a defect: keep that voter's newest snapshot for the investigation, and set `raft.snapshot_body: plain` on the voters if the fallbacks repeat until it is fixed.

```yaml
groups:
  - name: arbiter-snapshot-encoding
    rules:
      - alert: ArbiterSnapshotEncodingFallback
        expr: increase(arbiter_snapshot_encoding_fallback_total[1h]) > 0
        labels:
          severity: critical
        annotations:
          summary: A voter persisted a v21 snapshot with the plain body because the interned body did not rehydrate to exactly the plain body
```
````

`docs/specs/issue-31-metrics.md`, right after the `arbiter_raft_snapshot_store_bytes` row of Plan S3-A Task 9's table (`:48`), insert:

```markdown
| `arbiter_snapshot_encoding_fallback_total` | v21 snapshots persisted with the plain body in `raft.snapshot_body: auto` because the interned body could not be built or did not rehydrate exactly; exported from the start |
| `arbiter_snapshot_body_bytes{kind}` | Newest persisted snapshot's plain document length (`plain`) and interned body length (`interned`, 0 when it persisted plain); both 0 before the first persist |
```

and in the paragraph below the table (`:50`) replace `All series except the read cap and the failure counters are absent until their first observation.` with `All series except the read cap, the failure counters and the two v21 encoding series are absent until their first observation.` (both encoding series are exported from the start).

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && grep -c 'arbiter_snapshot_encoding_fallback_total' README.md docs/specs/issue-31-metrics.md && grep -c 'ArbiterSnapshotEncodingFallback' README.md && grep -c 'the two v21 encoding series are absent' docs/specs/issue-31-metrics.md && git -C "$W" diff --check`
Expected: `README.md:2`, `docs/specs/issue-31-metrics.md:1`, then `2`, then `1`, and no `git diff --check` output.

- [ ] **Step 28: Commit the metrics and the documentation**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" add metrics/snapshot.go metrics/metrics.go metrics/snapshot_test.go README.md docs/specs/issue-31-metrics.md
git -C "$W" commit -m "feat(stage3): snapshot encoding fallback and body-size metrics (Task 67)

arbiter_snapshot_encoding_fallback_total counts the v21 snapshots
persisted plain because the interned body did not verify, and
arbiter_snapshot_body_bytes{kind=\"plain|interned\"} reports the newest
persisted snapshot's plain document and interned body lengths; both are
exported from the start for the FSM, through the node-local
SnapshotEncodingStats (housegate spec 2026-10-14 §10.3). The README
documents the v21 encoding, raft.snapshot_body and the example alert
ArbiterSnapshotEncodingFallback; arbiter_state_bytes and the other size
gauges keep describing the plain container.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 29: Full verification**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test -z "$(gofmt -l $(git ls-files '*.go'))" && bazel run //:gazelle && git -C "$W" status --short && bazel build //... && bazel test //... --test_output=errors && bazel test --build_tests_only --@rules_go//go/config:race //fsm:fsm_test //metrics:metrics_test //config:config_test //cmd/arbiter:arbiter_test //integration:integration_test --test_output=errors && git -C "$W" log --format='%h %s' --diff-filter=MDR --grep='(Task 67)' -- fsm/testdata accumulator/testdata && git -C "$W" log --format='%s' --diff-filter=A --grep='(Task 67)' -- fsm/testdata; bazel query 'deps(//fsm:fsm, 1)' | grep -c com_github_sentioxyz_arbiter_proto; grep -rn 'time\.Now' fsm/`
Expected: gazelle makes no change and the status is empty; the whole suite and the five race targets PASS; the first `git log` prints nothing (no Task 67 commit modifies, deletes or renames a golden or vector file) and the second prints only `feat(stage3): write-then-verify the v21 interned body in Persist (Task 67)`; `0`; the `grep` prints nothing.

**Offline replay (Steps 30–41).** Steps 30–41 are local only: they replay local copies of devnet2 voters under `ARBITER_TAG_P4` (the voters' rollback release before the activation) and the branch head, prove that both write the same pre-v21 containers and restore each other's, that `ARBITER_TAG_P4` refuses both frozen v21 containers at the version byte while the branch restores both bodies, and measure the interned body on every copy. They change nothing in the worktree, write only `$T` (`$HOME/si-devnet2-multisource-s3e2-replay`) and the two evidence files beside the worktree, and remove `$T` at the end (contract §48, decision D37; no task reads `$T` afterwards, and the P5 rehearsal directory `$R5` is Plan S3-E4 Task 78's). The agent runs each bash block from a new block file in this session's scratchpad (`<scratchpad>/s3e2-t67/<nn>-<what>.sh`, written with the Write tool, never edited after it ran) with `bash -eo pipefail <file> < /dev/null`, because the Bash tool's own shell is zsh. Blocks marked `Confirm before running.` need explicit user confirmation in chat first.

- [ ] **Step 30: Create the work directory**

```bash
T="$HOME/si-devnet2-multisource-s3e2-replay"; W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"
[ ! -e "$T" ] || { echo "$T exists: move it aside; a replay directory is never reused"; exit 1; }
for f in "$W.replay-e2.txt" "$W.v21size-e2.txt"; do [ ! -e "$f" ] || { echo "$f exists: move it aside; evidence is never overwritten"; exit 1; }; done
mkdir -m 700 "$T" && mkdir -p "$T/statedigest" "$T/v20check" "$T/snapdump" "$T/v21check" "$T/v21size" "$T/inputs" "$T/out" && echo "created $T"
```

Expected: `created /Users/uranuswch/si-devnet2-multisource-s3e2-replay`. If `$T` or an evidence file exists, stop and ask the user: a directory or evidence file is never reused.

- [ ] **Step 31: Write `$T/helpers.sh`**

Write `/Users/uranuswch/si-devnet2-multisource-s3e2-replay/helpers.sh` with the Write tool, with exactly this content:

```bash
# Plan S3-E2 Task 67: offline replay and v21 size measurement over local
# copies of devnet2 voters (never committed). Source it at the top of every
# block; no shell state survives between Bash calls.
# shellcheck disable=SC2034 # the blocks that source this file read these
T="$HOME/si-devnet2-multisource-s3e2-replay"
W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"
R="$HOME/si-devnet2-multisource-s3a-rehearsal"
S2R="$HOME/si-devnet2-multisource-s2-rehearsal"
LIVE="$HOME/si-devnet2-multisource-tracks"
P0="$HOME/si-devnet2-stage3-p0"
REPLAY="$W.replay-e2.txt"
SIZES="$W.v21size-e2.txt"
# Both trees' go.mod toolchain, never the host's newer go; modules come from
# the local cache only (a missing module stops the build instead of reaching
# the network).
export GOTOOLCHAIN=go1.26.3 GOWORK=off GOPROXY=off

# rehearsal_build old|new exports the tag in $T/old_tag.txt (old) or the
# branch head (new) with git archive into $T/src/<build>, copies the five
# tools into its cmd/ and builds them into $T/bin/<build>; v21size needs the
# interned codec, so only new builds it.
rehearsal_build() {
  local ref tools
  case "$1" in
    old) ref="$(cat "$T/old_tag.txt")" || return 1 ;;
    new) ref="$(git -C "$W" rev-parse HEAD)" || return 1 ;;
    *) echo "usage: rehearsal_build old|new"; return 2 ;;
  esac
  rm -rf "${T:?}/src/$1" "${T:?}/bin/$1" && mkdir -p "$T/src/$1" "$T/bin/$1" || return 1
  git -C "$W" archive "$ref" | tar -x -C "$T/src/$1" || return 1
  for tool in statedigest v20check snapdump v21check v21size; do
    mkdir -p "$T/src/$1/cmd/$tool" && cp "$T/$tool/main.go" "$T/src/$1/cmd/$tool/main.go" || return 1
  done
  tools="./cmd/statedigest ./cmd/v20check ./cmd/snapdump ./cmd/v21check"
  [ "$1" = new ] && tools="$tools ./cmd/v21size"
  # shellcheck disable=SC2086 # tools is a list of package paths
  (cd "$T/src/$1" && GOFLAGS=-buildvcs=false go build -o "$T/bin/$1/" $tools)
}

# digest_lines prints the comparable lines of one statedigest output (Plan
# S3-A Task 11's filter).
digest_lines() {
  grep -E '^(snapshot_index|last_index|applied_commands|rejected_commands|written_version|genesis_configured|live_check|apply_error|apply_rejected|key|spent_ids|snapshot_sha256)' "$1"
}

# rehearsal_replay NAME RAFT_DIR CONFIG replays one copied raft data dir under
# both builds: the statedigest lines and the written containers must be
# identical, the container a pre-v21 one, and each build must restore the
# other's. It appends one result line to $REPLAY; an input recorded there
# already is skipped, so a block can be rerun after a failure.
rehearsal_replay() {
  local name="$1" dir="$T/out/$1" b version old_check new_check
  if grep -q "^$name: " "$REPLAY"; then echo "$name: recorded already"; return 0; fi
  mkdir -p "$dir" || return 1
  for b in old new; do
    "$T/bin/$b/statedigest" -data-dir "$2" -config "$3" > "$dir/digest.$b.txt" 2> "$dir/digest.$b.err" || { echo "$name: statedigest $b failed:"; cat "$dir/digest.$b.err"; return 1; }
    digest_lines "$dir/digest.$b.txt" > "$dir/digest.$b.cmp"
    if grep -q '^apply_error ' "$dir/digest.$b.cmp"; then echo "$name: apply_error under $b"; return 1; fi
    "$T/bin/$b/snapdump" -data-dir "$2" -config "$3" -out "$dir/written.$b.container" > "$dir/snapdump.$b.txt" || { echo "$name: snapdump $b failed"; return 1; }
  done
  if ! cmp -s "$dir/digest.old.cmp" "$dir/digest.new.cmp"; then
    echo "$name: old and new replay DIFFERENTLY; stop"
    diff "$dir/digest.old.cmp" "$dir/digest.new.cmp" | head -20
    return 1
  fi
  cmp -s "$dir/written.old.container" "$dir/written.new.container" || { echo "$name: the written containers differ; stop"; return 1; }
  version="$(od -An -j4 -N1 -tu1 "$dir/written.new.container" | tr -d ' ')"
  [ "$version" -ge 3 ] && [ "$version" -le 20 ] || { echo "$name: written version $version is not a pre-v21 container; stop"; return 1; }
  old_check="$("$T/bin/old/v20check" "$dir/written.new.container")"
  new_check="$("$T/bin/new/v21check" "$dir/written.old.container")"
  [ "$old_check" = "restore_ok version $version" ] || { echo "$name: old build: $old_check"; return 1; }
  [ "$new_check" = "restore_ok version $version body none" ] || { echo "$name: new build: $new_check"; return 1; }
  echo "$name: $(grep -c . "$dir/digest.new.cmp") statedigest lines identical ($(grep '^written_version' "$dir/digest.new.cmp"), $(grep '^snapshot_sha256' "$dir/digest.new.cmp")); the same v$version container ($(wc -c < "$dir/written.new.container" | tr -d ' ') bytes) from both builds; old restores new's: $old_check; new restores old's: $new_check" | tee -a "$REPLAY"
}

# rehearsal_v21size NAME CONTAINER measures one plain-body container with
# the new build's codec and appends one line to $SIZES (skipped when NAME is
# recorded already); the rehydrated body must equal the plain body.
rehearsal_v21size() {
  local line
  if grep -q "^$1 " "$SIZES"; then echo "$1: recorded already"; return 0; fi
  line="$("$T/bin/new/v21size" "$2")" || { echo "$1: v21size failed"; return 1; }
  case "$line" in
    *" rehydrated_identical true") ;;
    *) echo "$1: $line; stop"; return 1 ;;
  esac
  echo "$1 $line" | tee -a "$SIZES"
}
```

- [ ] **Step 32: Write `$T/v21check/main.go`**

Write `/Users/uranuswch/si-devnet2-multisource-s3e2-replay/v21check/main.go` with the Write tool:

```go
// Throwaway container check for the multi-source stage-3 P5 encoding
// rehearsal (never committed). It restores one raw snapshot container (v3 to
// v21, either v21 body) into this build's FSM with no configured genesis (the
// embedded-host restore path, as the stage-2 v20check does) and prints
// restore_ok or restore_refused with the version byte and the body kind.
package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/sentioxyz/arbiter/fsm"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: v21check CONTAINER")
		os.Exit(2)
	}
	data, err := os.ReadFile(os.Args[1])
	if err != nil || len(data) < 5 {
		fmt.Fprintln(os.Stderr, "v21check: not a snapshot container:", err)
		os.Exit(1)
	}
	body := "none"
	if data[4] == 21 && len(data) > 5 {
		switch data[5] {
		case 0:
			body = "plain"
		case 1:
			body = "interned"
		default:
			body = fmt.Sprintf("kind-%d", data[5])
		}
	}
	state, err := fsm.New(fsm.Params{NetworkID: "rehearsal", SchemaSnapshotID: "rehearsal", ExecutorProfileID: "rehearsal"})
	if err != nil {
		fmt.Fprintln(os.Stderr, "v21check:", err)
		os.Exit(1)
	}
	if err := state.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		fmt.Printf("restore_refused version %d body %s: %v\n", data[4], body, err)
		os.Exit(1)
	}
	fmt.Printf("restore_ok version %d body %s\n", data[4], body)
}
```

- [ ] **Step 33: Write `$T/v21size/main.go`**

Write `/Users/uranuswch/si-devnet2-multisource-s3e2-replay/v21size/main.go` with the Write tool:

```go
// Throwaway v21 body-size measurement for the multi-source stage-3 P5
// encoding rehearsal (never committed). It reads one snapshot container whose
// body is plain (v3 to v20, or v21 with body kind 0), interns the body with
// this build's fsm.InternSnapshotBody, rehydrates the result and prints both
// lengths and whether the rehydrated bytes equal the plain body exactly.
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/sentioxyz/arbiter/fsm"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: v21size CONTAINER")
		os.Exit(2)
	}
	plain, err := plainBody(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "v21size:", err)
		os.Exit(1)
	}
	interned, err := fsm.InternSnapshotBody(plain)
	if err != nil {
		fmt.Fprintln(os.Stderr, "v21size: intern:", err)
		os.Exit(1)
	}
	rehydrated, err := fsm.RehydrateSnapshotBody(interned)
	if err != nil {
		fmt.Fprintln(os.Stderr, "v21size: rehydrate:", err)
		os.Exit(1)
	}
	fmt.Printf("plain_bytes %d interned_bytes %d rehydrated_identical %t\n", len(plain), len(interned), bytes.Equal(rehydrated, plain))
}

// plainBody returns the JSON document of a container with a plain body.
func plainBody(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < 13 || string(data[:4]) != "AFSM" {
		return nil, fmt.Errorf("%s is not a snapshot container", path)
	}
	envelope := 13
	if data[4] >= 21 {
		if data[4] != 21 || len(data) < 14 || data[5] != 0 {
			return nil, fmt.Errorf("%s is not a v21 container with a plain body (version %d)", path, data[4])
		}
		envelope = 14
	}
	n := binary.BigEndian.Uint64(data[envelope-8 : envelope])
	if n > uint64(len(data)-envelope) {
		return nil, fmt.Errorf("%s: document length %d exceeds the container", path, n)
	}
	return data[envelope : envelope+int(n)], nil
}
```

- [ ] **Step 34: Copy Plan S3-A's tools and check the inputs**

```bash
source "$HOME/si-devnet2-multisource-s3e2-replay/helpers.sh"
for tool in statedigest v20check snapdump; do cp "$R/$tool/main.go" "$T/$tool/main.go" || { echo "missing $R/$tool/main.go: Plan S3-A Task 11's tools are gone; stop"; exit 1; }; done
(cd "$T" && shasum -a 256 statedigest/main.go v20check/main.go snapdump/main.go v21check/main.go v21size/main.go) | tee "$T/tools.sha256"
for f in "$S2R/sample14/raft" "$S2R/sample14/local.yaml" "$S2R/sample19/data" "$S2R/sample19/local.yaml" "$LIVE/arbiter-0-raft-after.tgz" "$LIVE/arbiter-0-after.yaml" "$LIVE/v20-state.bin" "$P0/arbiter-0-raft.tgz" "$P0/arbiter-1-raft.tgz" "$P0/arbiter-2-raft.tgz" "$P0/arbiter-0.yaml" "$P0/arbiter-1.yaml" "$P0/arbiter-2.yaml"; do
  [ -e "$f" ] || { echo "missing input $f; stop"; exit 1; }
done
echo "inputs present"
```

Expected: five checksum lines, the first two `c5168a19360228da9be001633cb5cf1c2f5a0f49c45c8f90df3dc176c37ac7e4  statedigest/main.go` and `fccc6a876b79ba43f9211afb10e0448d432ee37f1c570b36560ded04c5d004ab  v20check/main.go` (Plan S3-A's recorded checksums), then `inputs present`. A different checksum for those two, or a missing input: stop and find the original (Plans S3-A Task 11, S2-A Task 15, S2-B, S3-B Task 15); never rewrite those tools. `snapdump`, `v21check` and `v21size` have no recorded checksum; their lines are recorded as printed.

- [ ] **Step 35: Read `ARBITER_TAG_P4` and open the evidence files**

Remote read (the housegate fetch): needs explicit user confirmation in chat before running.

Confirm before running.

```bash
source "$HOME/si-devnet2-multisource-s3e2-replay/helpers.sh"
h=/Users/uranuswch/Dev/projects/sentio-network/housegate; L="$HOME/.cache/s3-ledger"
for f in "$REPLAY" "$SIZES"; do [ ! -e "$f" ] || { echo "$f exists: the evidence is opened once; stop"; exit 1; }; done
git -C "$h" fetch -q origin || exit 1
python3 -I "$L/s3-ledger.py" get "$h" ARBITER_TAG_P4 > "$T/ledger.env" || exit 1
old_tag="$(sed -n 's/^ARBITER_TAG_P4=//p' "$T/ledger.env")"
git -C "$W" rev-parse -q --verify "refs/tags/$old_tag^{commit}" > /dev/null || { echo "tag $old_tag is not in the local arbiter clone (Task 64's fetch brought the tags); stop"; exit 1; }
git -C "$W" merge-base --is-ancestor "$old_tag" HEAD || { echo "$old_tag is not an ancestor of the branch head; stop"; exit 1; }
echo "$old_tag" > "$T/old_tag.txt"
{ echo "old $old_tag $(git -C "$W" rev-parse "$old_tag^{commit}")"; echo "new $(git -C "$W" rev-parse HEAD) $(git -C "$W" rev-parse --abbrev-ref HEAD)"; cat "$T/tools.sha256"; } | tee "$REPLAY"
echo "new $(git -C "$W" rev-parse HEAD) $(git -C "$W" rev-parse --abbrev-ref HEAD)" > "$SIZES"
```

Expected: `old <ARBITER_TAG_P4> <its commit>`, `new <branch head> urwt/claude/multi-source-s3e` and the five checksum lines (the opening of `$W.replay-e2.txt`; `$W.v21size-e2.txt` opens with the `new` line). `s3-ledger: … ; stop` or a missing tag: stop; Task 64 read the same value before its first edit, and its `git fetch origin --tags` brought the tag.

- [ ] **Step 36: Build both trees**

```bash
source "$HOME/si-devnet2-multisource-s3e2-replay/helpers.sh"
rehearsal_build old && rehearsal_build new && ls "$T/bin/old" "$T/bin/new"
```

Expected: `$T/bin/old` lists `snapdump`, `statedigest`, `v20check`, `v21check`; `$T/bin/new` lists those and `v21size`. A tool that fails to compile against `ARBITER_TAG_P4` is fixed in `$T/src/old/cmd/<tool>` only. If `go` reports a module missing from the cache (`module lookup disabled by GOPROXY=off`), the build needs a module download, a remote read: needs explicit user confirmation in chat before running.

Confirm before running.

```bash
source "$HOME/si-devnet2-multisource-s3e2-replay/helpers.sh"
export GOPROXY=https://proxy.golang.org,direct
rehearsal_build old && rehearsal_build new && ls "$T/bin/old" "$T/bin/new"
```

Expected: the same two listings.

- [ ] **Step 37: Replay every copy under both builds**

```bash
source "$HOME/si-devnet2-multisource-s3e2-replay/helpers.sh"
ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX="$(openssl rand -hex 32)"; export ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX  # config.Load needs one; the replay signs nothing
mkdir -p "$T/inputs/sample14" "$T/inputs/sample19" "$T/inputs/s2b-arbiter-0" || exit 1
cp -R "$S2R/sample14/raft" "$S2R/sample14/local.yaml" "$T/inputs/sample14/" || exit 1
cp -R "$S2R/sample19/data" "$S2R/sample19/local.yaml" "$T/inputs/sample19/" || exit 1
tar -C "$T/inputs/s2b-arbiter-0" -xzf "$LIVE/arbiter-0-raft-after.tgz" && cp "$LIVE/arbiter-0-after.yaml" "$T/inputs/s2b-arbiter-0/arbiter.yaml" || exit 1
for i in 0 1 2; do
  mkdir -p "$T/inputs/p0-arbiter-$i" && tar -C "$T/inputs/p0-arbiter-$i" -xzf "$P0/arbiter-$i-raft.tgz" && cp "$P0/arbiter-$i.yaml" "$T/inputs/p0-arbiter-$i/arbiter.yaml" || exit 1
done
rehearsal_replay sample14 "$T/inputs/sample14/raft" "$T/inputs/sample14/local.yaml" || exit 1
rehearsal_replay sample19 "$T/inputs/sample19/data" "$T/inputs/sample19/local.yaml" || exit 1
for name in s2b-arbiter-0 p0-arbiter-0 p0-arbiter-1 p0-arbiter-2; do
  rehearsal_replay "$name" "$T/inputs/$name/raft" "$T/inputs/$name/arbiter.yaml" || exit 1
done
grep -h '^live_check' "$T/out/p0-arbiter-0/digest.new.cmp"
```

Expected, appended to `$W.replay-e2.txt`, six lines of the form `<name>: <n> statedigest lines identical (written_version <v>, snapshot_sha256 <hash>); the same v<v> container (<bytes> bytes) from both builds; old restores new's: restore_ok version <v>; new restores old's: restore_ok version <v> body none`, with `written_version 14` for `sample14`, `19` for `sample19`, `20` for `s2b-arbiter-0` (its container 11868827 bytes, Plan S3-A Task 11's record) and `20` for `p0-arbiter-0`, `-1` and `-2`; then the two `live_check` lines of `p0-arbiter-0` (`network_id=devnet2 genesis_snapshot_id=0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06 epoch=<n> …`, the values of Plan S3-B's copy). Any `DIFFERENTLY`, differing containers, a written version above 20, `apply_error` or a failed restore: stop; the branch changed a consensus byte before the activation (contract §44). After a stop, fix the cause and run the block again from a new block file: an input already recorded in `$W.replay-e2.txt` prints `<name>: recorded already` and is not replayed again (the same holds for Steps 38 and 39).

- [ ] **Step 38: Check both builds against the frozen v21 containers**

```bash
source "$HOME/si-devnet2-multisource-s3e2-replay/helpers.sh"
for body in plain interned; do
  if grep -q "^golden v21 $body container: " "$REPLAY"; then echo "golden v21 $body: recorded already"; continue; fi
  c="$W/fsm/testdata/stage3_v21_$body.snapshot"
  old_check="$("$T/bin/old/v20check" "$c")"
  new_check="$("$T/bin/new/v21check" "$c")" || { echo "$new_check"; exit 1; }
  case "$old_check" in
    "restore_refused version 21: unsupported snapshot version 21 "*) ;;
    *) echo "the old build did not refuse the v21 $body container at its version byte: $old_check; stop"; exit 1 ;;
  esac
  [ "$new_check" = "restore_ok version 21 body $body" ] || { echo "new build: $new_check; stop"; exit 1; }
  echo "golden v21 $body container: old refuses it at the version byte; new: $new_check" | tee -a "$REPLAY"
done
```

Expected, appended to `$W.replay-e2.txt`: `golden v21 plain container: old refuses it at the version byte; new: restore_ok version 21 body plain` and `golden v21 interned container: old refuses it at the version byte; new: restore_ok version 21 body interned` (`ARBITER_TAG_P4`'s refusal is `unsupported snapshot version 21 (require 3, 4, …, 19 or 20); …`, spec §10.3: v21 is refused by older binaries).

- [ ] **Step 39: Measure the interned body**

```bash
source "$HOME/si-devnet2-multisource-s3e2-replay/helpers.sh"
for name in sample14 sample19 s2b-arbiter-0 p0-arbiter-0 p0-arbiter-1 p0-arbiter-2; do
  rehearsal_v21size "$name" "$T/out/$name/written.new.container" || exit 1
done
rehearsal_v21size live-v20-state "$LIVE/v20-state.bin" || exit 1
rehearsal_v21size golden-v21-plain "$W/fsm/testdata/stage3_v21_plain.snapshot" || exit 1
```

Expected, appended to `$W.v21size-e2.txt`: eight lines `<name> plain_bytes <n> interned_bytes <m> rehydrated_identical true`, among them `s2b-arbiter-0 plain_bytes 11868252 interned_bytes 1089643 rehydrated_identical true` and `live-v20-state plain_bytes 11385138 interned_bytes 1070604 rehydrated_identical true` (measured while drafting; the codec is deterministic, so these two repeat exactly), and for the three P0 copies an interned body of about a tenth of the plain document. `rehydrated_identical false` or a failure: stop; the codec is wrong, and on a voter it would only ever have cost a fallback.

- [ ] **Step 40: Check the evidence**

```bash
source "$HOME/si-devnet2-multisource-s3e2-replay/helpers.sh"
[ "$(grep -c ' statedigest lines identical ' "$REPLAY")" -eq 6 ] && [ "$(grep -c '^golden v21 ' "$REPLAY")" -eq 2 ] || { echo "the replay evidence is incomplete; stop"; exit 1; }
[ "$(grep -c ' rehydrated_identical true$' "$SIZES")" -eq 8 ] || { echo "the size evidence is incomplete; stop"; exit 1; }
cat "$REPLAY" "$SIZES"
```

Expected: the whole of both evidence files, 24 lines: the `old` and `new` lines, five checksums, six replay lines and two golden lines; then the `new` line and eight size lines. An incomplete file: rerun the step that is missing from a new block file (recorded inputs are skipped).

- [ ] **Step 41: Remove the work directory**

Removes `$T`: needs explicit user confirmation in chat before running.

Confirm before running.

```bash
T="$HOME/si-devnet2-multisource-s3e2-replay"; W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"
[ -s "$W.replay-e2.txt" ] && [ -s "$W.v21size-e2.txt" ] || { echo "the evidence files are missing; keep $T"; exit 1; }
rm -rf -- "$T" && [ ! -e "$T" ] && echo "removed $T; the evidence stays in $W.replay-e2.txt and $W.v21size-e2.txt"
```

Expected: `removed /Users/uranuswch/si-devnet2-multisource-s3e2-replay; the evidence stays in /Users/uranuswch/src/remotesrc/claude/arbiter/multi-source-s3e.replay-e2.txt and /Users/uranuswch/src/remotesrc/claude/arbiter/multi-source-s3e.v21size-e2.txt`. Task 68 starts from the commit of Step 28 with an empty `git -C "$W" status --porcelain`; the evidence files stay beside the worktree for Plan S3-E4's PR body and rehearsal and are never committed.

---

## Task 68: arbiter — eviction voiding, stage-3 switch 2 (S3-E2)

Spec §14 (eviction voiding), §10.4 step 2 (the `eviction_voiding` switch, on before any member can be selected), §7.4 (a verifier evicted after the latch stays in the set and the latched verdict survives) and §17 (eviction voiding including evidence), as clarified by K72 and fixed by contract §47. Today an Evicted node keeps a usable key: result claims and promotion, cleanup and rejected-part cleanup acknowledgements are admitted on the `si_indexers` signature alone (`fsm/signed_claims.go:442-455`, `:552-560`, `:788-800`; `fsm/rejected_cleanup.go:337-345`), and verifier evidence on set membership and the ed25519 signature (`fsm/apply.go:589-645`). Once the switch is committed, each of those call sites refuses a node whose `Nodes` record is Evicted, right after its own signature check and before any mutation; `reevaluateBlock` stops judging an evicted verifier's evidence unless a statement of the block is challenged; `EvictNode` and the switch-on update re-evaluate the unlatched verdicts that judged it; and the WorkSet counts that evidence as missing, so its block is `EvidenceIncomplete` (a re-selection candidate of Plan S3-E3) and never `QuorumFailed`, which the orchestrator would open and resolve as Rejected (`orchestrator/dispatch.go:32-80`). The arbiter README's stage-3 section gains the operator's view of the switch.

This task continues the P5 branch `urwt/claude/multi-source-s3e`: its one commit lands on Task 67's last commit. Line anchors are arbiter `fb42260` (`v0.12.0`); on the branch every hunk is found by the quoted lines under the hunk rule of the Global Constraints. Of the files edited here, `fsm/stage3.go` (Task 64) and the README section `## Multi-source storage integrity (stage-3 activation)` (Task 65) come from earlier tasks of this plan; Task 69 adds the `purge` case beside this task's case in `onStage3SwitchOnLocked`, Task 70 edits other lines of `fsm/apply.go` and `fsm/reads_work.go`, and Plan S3-E3 Task 74 edits `reevaluateBlock` again (the latch count). No step of this task reaches a remote, so none needs a confirmation: Task 64 did the only fetch and Task 70 does the push.

**Files:**
- Create: `fsm/eviction_voiding.go` (`evictionVoidingLocked`, `evictedNodeLocked`, `evictedSignerRefusalLocked`, `voidedReplicaLocked`, `reevaluateBlocksNamingLocked`, `reevaluateVoidedBlocksLocked`, `reevaluateUnlatchedBlocksLocked`)
- Create: `fsm/eviction_voiding_test.go` (the twelve named tests of contract §47 and their `…V` fixtures)
- Modify: `fsm/apply.go` — `applyEvictNode` (after the status change `:138-139`), `applyRegisterRC` (after the authentication block `:483-487`), `applyRecordAttestation` (after the signature check `:608-610`), `applyRecordByteSideScan` (after the signature check `:637-639`), `applyRecordPromotionAck` (inside the post-activation block, after `:814-816`), `applyRecordCleanupAck` (inside the post-activation block, after `:1038-1040`)
- Modify: `fsm/rejected_cleanup.go` — `recordRejectedCleanupAckLocked` (after the source signature check `:343-345`)
- Modify: `fsm/signed_claims.go` — `adoptParkedClaimsLocked` (after the tracks discard `:681-683`)
- Modify: `fsm/threeway.go` — `reevaluateBlock` (the pinned state and the head of the evidence loop, `:73-79`)
- Modify: `fsm/reads_work.go` — `evidencePresence` (`:20-26`) and its one call in `WorkSet` (`:139`)
- Modify: `fsm/stage3.go` (Task 64) — the `eviction_voiding` case of `onStage3SwitchOnLocked`
- Modify: `fsm/BUILD.bazel` (gazelle adds `eviction_voiding.go` to `//fsm:fsm` and `eviction_voiding_test.go` to `//fsm:fsm_test`)
- Modify: `README.md` — one paragraph `**Eviction voiding (switch 2).**` right after Task 65's `**Switches.**` paragraph in `## Multi-source storage integrity (stage-3 activation)`
- Test: `fsm/eviction_voiding_test.go`; unchanged and green: `fsm/threeway_test.go`, `fsm/reads_work_test.go`, `fsm/rejected_cleanup_test.go`, `fsm/source_ack_test.go`, `fsm/rc_owner_parking_test.go`, `fsm/signed_messages_test.go`, `orchestrator/dispatch_test.go`, every golden test and every `TestDeterminism_*`

**Interfaces:**
- Consumes:
  - Task 64 (contract §44–§45): the switch names `Stage3SwitchPurge` and `Stage3SwitchEvictionVoiding`; `func (f *FSM) stage3SwitchOnLocked(name string) bool`; `func (f *FSM) Stage3SwitchOn(name string) bool`; the seam `func (f *FSM) onStage3SwitchOnLocked(name string)`, called once per switch turned on, after `f.st.Params = next` and before the cutover is recorded and `ConsensusEpoch++`; the fixture (`fsm/stage3_fixture_test.go`) `func stage3FSM(t *testing.T) (*FSM, *authority.Signer)` (tracksFSM plus the activation), `func turnOnStage3Switch(t *testing.T, f *FSM, a *authority.Signer, name string)`, `func stage3Update(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate` (tracksUpdate carrying the committed `stage3`).
  - Task 65: the README section `## Multi-source storage integrity (stage-3 activation)` and its `**Switches.**` paragraph (the anchor of Step 11). Task 66 (contract §46): the v21 container, which carries `State.Stage3` and whose reader runs `validateStage3State`, so `snapshotBytes` and `restoreInto` round-trip a stage-3 state; Task 67: `Persist` (whichever body it writes, the restore tests compare the container bytes the two replicas write). Nothing of Tasks 65–67 is edited here.
  - arbiter-core (`CORE_TAG_P5`, nothing new from it here): `authority.SNodeMessageResultClaim` (`"result_claim"`), `authority.SNodeMessagePromotionAck` (`"promotion_ack"`), `authority.SNodeMessageCleanupAck` (`"cleanup_ack"`), `arbiter.EvictNodeCommand{NodeID, ExpectedRegistrationSeq, Reason}`, `(*authority.Signer).SignEvictNodeWithContext(cmd arbiter.EvictNodeCommand, ctx authority.ConsensusContext) (string, error)`.
  - Existing arbiter: `NodeEvicted`; `func blockChallenged(stmts []*StatementState) bool` (`fsm/reads_work.go:34`); `reevaluateBlock`, `resultClaimRefusalLocked`, `sourceAckRefusalLocked`, `snodeSignatureLocked`, `verifyNodeSig`, `adoptParkedClaimsLocked`, `pendingRCKey`, `completePurgesLocked`; fsm test helpers `tracksFSM`, `tracksUpdate`, `admitTrack`, `trackEnvelope`, `bindTrackClaim`, `verifyBlock`, `promoteTrack`, `ackTrackPromotion` (`fsm/tracks_fixture_test.go`), `ownedClaim`, `pinnedEvidence`, `partPromotionAck`, `partCleanup`, `partCleanupAck`, `snodeSigner` (`fsm/pre_tracks_golden_test.go`), `signedAttestation`, `signedScan` (`fsm/evidence_test.go`), `signedRC` (`fsm/rc_owner_parking_test.go`), `testVerifiers` (`fsm/signed_claims_params_test.go`), `authorityContextFor`, `snodeRegistration`, `snodeMarkActive`, `verifierRegistration`, `verifierMarkActive`, `rejectSignedUnchanged` (`fsm/signed_messages_fixture_test.go`), `rejectBlockQ`, `releaseQ`, `mustApplyUnchangedQ` (`fsm/quarantine_fixture_test.go`), `rejectedCleanupQ`, `rejectedAckQ` (`fsm/rejected_cleanup_test.go`), `signedConsensusUpdate` (`fsm/consensus_updates_test.go`), `testAccount`, `submit` (`fsm/admission_test.go`), `mustApply` (`fsm/fsm_test.go`), `mustSeal` (`fsm/table_set_transition_seal_test.go`), `anchorFinal` (`fsm/table_set_transition_pipeline_test.go`), `mustWorkSet` (`fsm/reads_work_test.go`), `lthashHex` (`fsm/threeway_test.go`), `snapshotBytes`, `restoreInto` (`fsm/snapshot_test.go`).
- Produces (contract §47; the two names marked "added" are this task's own and are recorded in Appendix B):
  - `func (f *FSM) evictionVoidingLocked() bool` — `stage3SwitchOnLocked(Stage3SwitchEvictionVoiding)`.
  - `func (f *FSM) evictedNodeLocked(nodeID string) bool` — a `Nodes` record with `NodeEvicted`.
  - `func (f *FSM) evictedSignerRefusalLocked(kind, nodeID string) string` — `%s from %s: the node is evicted (stage3 eviction_voiding)` while the switch is on and the node is evicted, else `""`; kinds here `result_claim`, `attestation`, `byte_side_scan`, `promotion_ack`, `cleanup_ack`; Plan S3-E3 calls it with `member_ready` and `member_checkpoint`.
  - added: `func (f *FSM) voidedReplicaLocked() func(replica string) bool` — nil while the switch is off, else `evictedNodeLocked`.
  - `func (f *FSM) reevaluateBlocksNamingLocked(nodeID string)` (called by `applyEvictNode` while the switch is on), `func (f *FSM) reevaluateVoidedBlocksLocked()` (called by `onStage3SwitchOnLocked(Stage3SwitchEvictionVoiding)`), added: `func (f *FSM) reevaluateUnlatchedBlocksLocked(names func(set []string) bool)`.
  - `func evidencePresence(bv *BlockVerification, voided func(replica string) bool) (map[string]bool, map[string]bool, bool)` (was `evidencePresence(bv)`; the WorkSet passes `f.voidedReplicaLocked()`).
  - Tests (fsm): `TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode`, `TestEvictionVoidingOffKeepsTodaysAdmission`, `TestEvictionVoidingKeepsALatchedVerdictAcrossSnapshotRestore`, `TestEvictionVoidingDropsAnEvictedVerifiersEvidenceFromAnUnlatchedVerdict`, `TestEvictionVoidingKeepsTheEvidenceOfAChallengedBlock`, `TestEvictNodeReevaluatesTheBlocksNamingIt`, `TestEvictionVoidingSwitchOnReevaluatesTheBlocksOfEarlierEvictions`, `TestWorkSetTreatsAnEvictedVerifiersEvidenceAsMissing`, `TestAdoptParkedClaimsDiscardsAClaimOfAnEvictedSNode`, `TestReRegisteredNodeIsNoLongerVoided`, `TestRegistrationAndMarkActiveKeepTheirChecks`, `TestRestoreDoesNotReapplyEvictionVoiding`. Test helpers: `voidedRefusalV`, `purgeOnlyFSMV`, `voidingFSMV`, `evictV`, `markedBlockV`, `evidenceV`, `recordEvidenceV`, `verdictV`, `verdictJSONV`, `promotedBlockV`, `rejectedCleanupV`, `parkedClaimV`, `evictedMessageV`, `evictedMessagesV`.

Notes:
- **Switch-off identity (contract §44).** Every new branch asks `evictionVoidingLocked` first. Before the stage-3 activation (`Params.Stage3 == nil`) and while `eviction_voiding` is off, `evictedSignerRefusalLocked` answers `""`, `voidedReplicaLocked` answers nil, so `reevaluateBlock`'s loop and `evidencePresence` run the base code, and `applyEvictNode` re-evaluates nothing: every Apply result, state digest and container is the base's. `TestEvictionVoidingOffKeepsTodaysAdmission` (before stage 3 and with only `purge` on), the `eviction_voiding off` subtests and the unchanged goldens pin it.
- **Each check sits after its site's signature check and before the first mutation.** An RC is checked after `resultClaimRefusalLocked`, before it can park or bind; an attestation or a scan after `verifyNodeSig`; a promotion or cleanup acknowledgement after `sourceAckRefusalLocked`, inside the post-activation block every stage-3 network is in; a rejected-part cleanup acknowledgement after its `snodeSignatureLocked`. A badly signed message of an evicted node therefore keeps its signature refusal (the `signature checks run first` subtest). No check sits inside `snodeSignatureLocked` or `verifierSignatureLocked`: registration and MarkActive call them too and spec §14 keeps both. MarkActive and purge reports already refuse an evicted node (`fsm/apply.go:108`, `fsm/apply_table_registry.go:382`).
- **Idempotent no-ops stay Applied.** The paths ahead of those signature checks answer `Applied` without changing state whoever sends them, exactly as at the base: a duplicate attestation or scan (first wins), an acknowledgement of an already acknowledged promotion, of a cleanup that is no longer pending, or of a rejected partition that is no longer recorded. Voiding refuses what would change state. A resent RC is checked after voiding, so an evicted SNode's resend is refused.
- **Voided, not deleted.** An evicted verifier's attestation and scan stay in `BlockVerification`; `reevaluateBlock` skips them while the switch is on and no statement of the block is ChallengeReplay or Rejected (`blockChallenged`). A latched verdict is never re-evaluated (`fsm/threeway.go:43-45`), so it keeps judging the evidence that latched it, which is what restore's `validateLatchedConsensus` (`fsm/genesis_validation.go:540-593`) re-derives the latch from, without reading node status. Plan S3-E3's `ReselectVerifiers` is what removes an evicted verifier from a set, and it refuses latched and challenged blocks (spec §7.4).
- **Re-evaluation is narrow.** `reevaluateBlocksNamingLocked` (from `applyEvictNode`, after the status change) and `reevaluateVoidedBlocksLocked` (from the switch-on update) visit blocks in block order and re-evaluate only the blocks whose verifier set names an affected node and that already hold an unlatched verdict. A block without a verdict has judged nothing: re-evaluating it would only turn a nil verdict into an empty one, so it is left alone and its first evaluation skips the node (`TestEvictNodeReevaluatesTheBlocksNamingIt`). Contract §47 says every block whose `VerifierSet` names the node; this refinement changes no verdict that judged evidence and is recorded in Appendix B. Dropping evidence only lowers the pass count, so no re-evaluation latches quorum, moves a statement status or emits an event.
- **WorkSet and orchestrator.** With the switch on, `evidencePresence` reports a voided replica's attestation and scan as absent, so a block whose other evidence is complete but failing is listed in `EvidenceIncomplete` and never in `QuorumFailed`: the orchestrator's open-and-reject path never fires on an evicted verifier's evidence. `dispatchEvidence` reads `BlockDispatchInfo`'s stored presence, which still shows the evicted replica's evidence, so the leader never re-dispatches a job to it. Until Plan S3-E3's `reselection` switch (§10.4 step 7) such a block passes only if its two remaining verifiers latch; otherwise it waits in `EvidenceIncomplete`, and its track with it, instead of being rejected. `GetBlockVerification` serves voided evidence with `evaluated` false (Plan S3-A's rule for evidence a verdict never judged).
- **Parked claims (K72).** `adoptParkedClaimsLocked` takes a parked claim out of its slot and, with the switch on, discards it without binding when its `SourceNode` is evicted. Admission binds only an Active owner SNode (`ownerSourceLocked`) and otherwise answers `SOURCE_UNAVAILABLE` with state unchanged, so the path is unreachable through admission today; the white-box test drives the adoption directly with a statement bound to the evicted SNode.
- **Re-registration and MarkActive.** Voiding reads node status, not keys: a registration with a higher `registration_seq` resets the node to Syncing, so it is no longer evicted and its messages are admitted again; an identical resend stays an idempotent no-op that leaves the node Evicted; MarkActive keeps refusing an evicted node with `node is evicted; re-register first`.
- **Restore** re-applies nothing (spec §14): an evicted node's stored claim, acknowledgement record and evidence restore byte-identically, and only Apply refuses new messages, on a restored replica too (`TestRestoreDoesNotReapplyEvictionVoiding`).
- **An evicted SNode's in-flight work.** With the switch on, an evicted SNode can no longer acknowledge its promotions or cleanups, and the stage-2 drain gate counts them until its track is quarantined (`fsm/quarantine.go:215-254` skips a quarantined track's work). Spec §10.2's rule that an SNode whose track has in-flight work is quarantined before it is evicted is therefore mandatory from switch 2 on; Plan S3-F's runbook carries it.
- **The snapshot-query lane needs nothing.** Its verifier attestation (`applyServerOwnedSnapshotQueryAttestation`, `fsm/artifact_snapshot_query_attestation.go:36-90`) already requires an Active verifier at Apply (`validateSnapshotQueryAttestationRequest(…, true)`, `:63`, through `activeVerifier`) and has no Raft ingress (the Raft command `RecordSnapshotQueryAttestation` is refused, `fsm/apply_unscoped_paths.go:13-15`, and nothing outside tests calls the server-owned seam); purge reports already refuse an evicted node. Spec §14's `MemberReady` and checkpoints are Plan S3-E3's, through `evictedSignerRefusalLocked`.
- **README.** The paragraph of Step 11 tells operators what the switch changes: the refusal text, the evidence-incomplete wait of a block whose evicted verifier's evidence was voided, and the quarantine-before-evict rule. Its anchor is Task 65's `**Switches.**` paragraph, the only line of `README.md` that begins with it (Step 1 checks).
- **fsm red lines.** `fsm/eviction_voiding.go` imports `fmt` and `slices` only: no arbiter-proto, no clock.

- [ ] **Step 1: Check the worktree, its head and Task 64's seams**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; git -C "$W" status --porcelain; git -C "$W" branch --show-current; git -C "$W" log -1 --format=%s`
Expected: no status line; `urwt/claude/multi-source-s3e`; a subject ending in `(Task 67)`. Otherwise stop: Tasks 64–67 land first, in order, on this one branch (contract §48).

Run:

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" || exit 1
grep -c -e 'func (f \*FSM) stage3SwitchOnLocked(name string) bool' -e 'func (f \*FSM) onStage3SwitchOnLocked(name string)' -e 'Stage3SwitchEvictionVoiding *= "eviction_voiding"' fsm/stage3.go
grep -c -e 'func stage3FSM(t \*testing.T) (\*FSM, \*authority.Signer)' -e 'func turnOnStage3Switch(t \*testing.T, f \*FSM, a \*authority.Signer, name string)' -e 'func stage3Update(t \*testing.T, f \*FSM) arbiter.ConsensusParamsUpdate' fsm/stage3_fixture_test.go
grep -c '^\*\*Switches\.\*\* ' README.md
grep -rn -e 'evictedSignerRefusalLocked' -e 'evictionVoidingLocked' -e 'reevaluateVoidedBlocksLocked' -e 'func evidencePresence(bv \*BlockVerification, voided' fsm/
```

Expected: `3`, then `3`, then `1`, then nothing. If a count differs, stop: Task 64's switch predicate, its switch-on seam and its fixture are this task's inputs (contract §44–§45), Task 65's `**Switches.**` paragraph is Step 11's anchor, and nothing of this task exists yet.

- [ ] **Step 2: Write the failing tests**

Create `fsm/eviction_voiding_test.go`:

```go
package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// Fixtures of the eviction voiding tests (housegate spec 2026-10-14 §14, plan
// S3-E2 Task 68). They build on Task 64's stage3FSM, which is tracksFSM
// (tracks_fixture_test.go) after the stage-3 activation: indexer 1 (SNode s1)
// owns db.t, indexer 0 (SNode s0, authoritytest key 1) owns db0.x, verifiers
// v1..v3 are registered, Active and listed, and every block's verifier set is
// {v1, v2, v3}. Names end in V so they stay clear of the other tasks'
// fixtures.

// voidedRefusalV is the refusal of a message of kind from evicted node.
func voidedRefusalV(kind, node string) string {
	return fmt.Sprintf("%s from %s: the node is evicted (stage3 eviction_voiding)", kind, node)
}

// purgeOnlyFSMV is stage3FSM with the purge switch on: stage 3 is active and
// eviction_voiding is still off.
func purgeOnlyFSMV(t *testing.T) (*FSM, *authority.Signer) {
	t.Helper()
	f, a := stage3FSM(t)
	turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
	return f, a
}

// voidingFSMV is purgeOnlyFSMV with the eviction_voiding switch on (spec §10.4
// step 2).
func voidingFSMV(t *testing.T) (*FSM, *authority.Signer) {
	t.Helper()
	f, a := purgeOnlyFSMV(t)
	turnOnStage3Switch(t, f, a, Stage3SwitchEvictionVoiding)
	return f, a
}

// evictV evicts node with an authority-signed EvictNode bound to its current
// registration_seq.
func evictV(t *testing.T, f *FSM, a *authority.Signer, node string) {
	t.Helper()
	cmd := arbiter.EvictNodeCommand{NodeID: node, ExpectedRegistrationSeq: f.st.RegistrationSeqs[node], Reason: "eviction voiding fixture"}
	token, err := a.SignEvictNodeWithContext(cmd, authorityContextFor(t, f))
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, f, wire.Command{EvictNode: &wire.EvictNode{NodeID: cmd.NodeID, Reason: cmd.Reason,
		ExpectedRegistrationSeq: cmd.ExpectedRegistrationSeq, AuthorityJWS: token}})
	if n := f.st.Nodes[node]; n == nil || n.Status != NodeEvicted {
		t.Fatalf("%s = %+v after its eviction", node, n)
	}
}

// markedBlockV admits one statement of owner's table from a fresh account,
// seals it into its own block, binds its claim (bindTrackClaim) and marks the
// block replaying. It returns the block and the statement.
func markedBlockV(t *testing.T, f *FSM, owner uint64) (uint64, uint64) {
	t.Helper()
	key, account := testAccount(t)
	stmt := admitTrack(t, f, key, account, 1, owner)
	block := mustSeal(t, f).BlockSeq
	bindTrackClaim(t, f, stmt)
	mustApply(t, f, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: block}})
	if set := f.st.Verifications[block].VerifierSet; !slices.Equal(set, []string{"v1", "v2", "v3"}) {
		t.Fatalf("block %d verifier set = %v, want [v1 v2 v3]", block, set)
	}
	return block, stmt
}

// evidenceV is replica's attestation and byte-side scan for block seq, which
// is marked replaying: pinnedEvidence's correct replay signed by replica or,
// when pass is false, the same evidence with a wrong computed state root, so
// check 1 fails and the replica's complete evidence does not pass.
func evidenceV(t *testing.T, f *FSM, seq uint64, replica string, pass bool) (attestation, scan wire.Command) {
	t.Helper()
	pinned := pinnedEvidence(t, f, seq)
	receipt := pinned[0].RecordAttestation.Attestation.Receipt
	if !pass {
		receipt.ComputedStateRoot = "0x" + strings.Repeat("0", 64)
	}
	seed := replica[len(replica)-1]
	return wire.Command{RecordAttestation: &wire.RecordAttestation{Attestation: signedAttestation(t, replica, seed, receipt)}},
		wire.Command{RecordByteSideScan: &wire.RecordByteSideScan{Scan: signedScan(t, replica, seed, seq, pinned[1].RecordByteSideScan.Scan.Parts)}}
}

// recordEvidenceV applies replica's complete evidence for block seq.
func recordEvidenceV(t *testing.T, f *FSM, seq uint64, replica string, pass bool) {
	t.Helper()
	attestation, scan := evidenceV(t, f, seq, replica, pass)
	mustApply(t, f, attestation)
	mustApply(t, f, scan)
}

// verdictV is block seq's stored verdict: the replicas it judged, sorted, and
// whether it latched quorum. judged is nil when the block has no verdict.
func verdictV(f *FSM, seq uint64) (judged []string, quorum bool) {
	v := f.st.Verifications[seq].Verdict
	if v == nil {
		return nil, false
	}
	judged = []string{}
	for replica := range v.Replicas {
		judged = append(judged, replica)
	}
	sort.Strings(judged)
	return judged, v.Quorum
}

// verdictJSONV is block seq's stored verdict as JSON, for byte comparisons.
func verdictJSONV(t *testing.T, f *FSM, seq uint64) string {
	t.Helper()
	b, err := json.Marshal(f.st.Verifications[seq].Verdict)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// promotedBlockV passes one statement of owner 0's table through claim,
// verification by v1 and v2 (quorum latches) and finality, and issues the
// promotion of its one part. It returns the block and the promotion.
func promotedBlockV(t *testing.T, f *FSM, a *authority.Signer) (uint64, uint64) {
	t.Helper()
	key, account := testAccount(t)
	stmt := admitTrack(t, f, key, account, 1, 0)
	block := mustSeal(t, f).BlockSeq
	bindTrackClaim(t, f, stmt)
	verifyBlock(t, f, block)
	anchorFinal(t, f, block)
	return block, promoteTrack(t, f, a, stmt)
}

// rejectedCleanupV rejects a block of owner 0 holding one claimed statement,
// releases the track acknowledging it and schedules the cleanup of the
// rejected partition (TestRejectedCleanupOfIndexerZero's path). It returns the
// scheduled cleanup.
func rejectedCleanupV(t *testing.T, f *FSM, a *authority.Signer) arbiter.UnsafeCleanup {
	t.Helper()
	key, account := testAccount(t)
	stmt := admitTrack(t, f, key, account, 1, 0)
	block := mustSeal(t, f).BlockSeq
	hash := bindTrackClaim(t, f, stmt)
	rejectBlockQ(t, f, block)
	mustApply(t, f, releaseQ(t, f, a, 0, 1, block))
	partition := fmt.Sprintf("p%d", stmt)
	cl := arbiter.UnsafeCleanup{TableID: "db0.x", PartitionID: partition, RejectedBlockSeq: block,
		Parts: []arbiter.PartRef{{TableID: "db0.x", PartitionID: partition, PartRowLtHash: hash, PartName: fmt.Sprintf("all_%d_0", stmt)}}}
	mustApply(t, f, rejectedCleanupQ(t, f, a, cl))
	return cl
}

// parkedClaimV is s0's signed claim for env, a statement of db0.x that is not
// admitted, so the claim parks in s0's source-scoped slot.
func parkedClaimV(t *testing.T, f *FSM, env arbiter.StatementEnvelope) wire.Command {
	t.Helper()
	hash := lthashHex("voiding-parked-claim")
	_, cmd := signedRC(t, f, snodeSigner(t, "s0"), arbiter.RCRecord{StatementID: env.StatementID, SourceNode: "s0", SourceClaimRoot: "0xclaim",
		CandidateParts:       []arbiter.CandidatePart{{TableID: "db0.x", PartitionID: "p1", PartName: "all_1_0", PartRowLtHash: hash, RowCount: 1, Bytes: 32}},
		PartitionNewPartSums: []arbiter.PartitionLtHashSum{{TableID: "db0.x", PartitionID: "p1", NewPartsLtHashSum: hash}}})
	return cmd
}

// evictedMessageV is one message of spec §14's list sent by a node evicted
// just before it: build prepares f up to the message, evicts node and returns
// the message and a check that it took effect when admitted.
type evictedMessageV struct {
	name, kind, node string
	build            func(t *testing.T, f *FSM, a *authority.Signer) (wire.Command, func(t *testing.T))
}

func evictedMessagesV() []evictedMessageV {
	return []evictedMessageV{
		{"result claim that binds", "result_claim", "s0", func(t *testing.T, f *FSM, a *authority.Signer) (wire.Command, func(*testing.T)) {
			key, account := testAccount(t)
			stmt := admitTrack(t, f, key, account, 1, 0)
			mustSeal(t, f)
			cmd, _ := ownedClaim(t, f, stmt, "voiding-claim")
			evictV(t, f, a, "s0")
			return cmd, func(t *testing.T) {
				if f.st.Statements[stmt].RC == nil {
					t.Fatal("the evicted SNode's claim did not bind")
				}
			}
		}},
		{"result claim that parks", "result_claim", "s0", func(t *testing.T, f *FSM, a *authority.Signer) (wire.Command, func(*testing.T)) {
			key, account := testAccount(t)
			env := trackEnvelope(t, f, key, account, "", 1, 0)
			cmd := parkedClaimV(t, f, env)
			evictV(t, f, a, "s0")
			return cmd, func(t *testing.T) {
				if f.st.PendingRC[pendingRCKey(env.StatementID.Flat(), "s0")] == nil {
					t.Fatal("the evicted SNode's claim did not park")
				}
			}
		}},
		{"attestation", "attestation", "v3", func(t *testing.T, f *FSM, a *authority.Signer) (wire.Command, func(*testing.T)) {
			block, _ := markedBlockV(t, f, 1)
			attestation, _ := evidenceV(t, f, block, "v3", true)
			evictV(t, f, a, "v3")
			return attestation, func(t *testing.T) {
				if f.st.Verifications[block].Attestations["v3"] == nil {
					t.Fatal("the evicted verifier's attestation was not recorded")
				}
			}
		}},
		{"byte-side scan", "byte_side_scan", "v3", func(t *testing.T, f *FSM, a *authority.Signer) (wire.Command, func(*testing.T)) {
			block, _ := markedBlockV(t, f, 1)
			_, scan := evidenceV(t, f, block, "v3", true)
			evictV(t, f, a, "v3")
			return scan, func(t *testing.T) {
				if f.st.Verifications[block].ByteScans["v3"] == nil {
					t.Fatal("the evicted verifier's byte-side scan was not recorded")
				}
			}
		}},
		{"promotion acknowledgement", "promotion_ack", "s0", func(t *testing.T, f *FSM, a *authority.Signer) (wire.Command, func(*testing.T)) {
			_, p := promotedBlockV(t, f, a)
			ack := partPromotionAck(t, f, p)
			evictV(t, f, a, "s0")
			return ack, func(t *testing.T) {
				if !f.st.PendingPromotions[p].Acked {
					t.Fatal("the evicted SNode's acknowledgement was not recorded")
				}
			}
		}},
		{"cleanup acknowledgement", "cleanup_ack", "s0", func(t *testing.T, f *FSM, a *authority.Signer) (wire.Command, func(*testing.T)) {
			_, p := promotedBlockV(t, f, a)
			ackTrackPromotion(t, f, p)
			mustApply(t, f, partCleanup(t, f, a, p))
			ack := partCleanupAck(t, f, p)
			evictV(t, f, a, "s0")
			return ack, func(t *testing.T) {
				if f.st.PendingCleanups[p] != nil {
					t.Fatal("the evicted SNode's cleanup acknowledgement did not clear the cleanup")
				}
			}
		}},
		{"rejected-part cleanup acknowledgement", "cleanup_ack", "s0", func(t *testing.T, f *FSM, a *authority.Signer) (wire.Command, func(*testing.T)) {
			cl := rejectedCleanupV(t, f, a)
			ack := rejectedAckQ(t, f, snodeSigner(t, "s0"), "s0", cl)
			evictV(t, f, a, "s0")
			return ack, func(t *testing.T) {
				if f.st.Tracks.RejectedParts != nil {
					t.Fatalf("rejected parts after the evicted SNode's acknowledgement = %+v", f.st.Tracks.RejectedParts)
				}
			}
		}},
	}
}

// TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode: spec §14. With
// eviction_voiding on, a result claim (one that would bind and one that would
// park), an attestation, a byte-side scan, and a promotion, cleanup and
// rejected-part cleanup acknowledgement from a node evicted before it are
// refused, naming the kind and the node, with state unchanged. The refusal
// follows the site's signature check: a badly signed message of an evicted
// node keeps its signature refusal.
func TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode(t *testing.T) {
	for _, m := range evictedMessagesV() {
		t.Run(m.name, func(t *testing.T) {
			f, a := voidingFSMV(t)
			cmd, _ := m.build(t, f, a)
			rejectSignedUnchanged(t, f, cmd, voidedRefusalV(m.kind, m.node))
		})
	}
	t.Run("signature checks run first", func(t *testing.T) {
		f, a := voidingFSMV(t)
		block, _ := markedBlockV(t, f, 1)
		receipt := pinnedEvidence(t, f, block)[0].RecordAttestation.Attestation.Receipt
		forged := wire.Command{RecordAttestation: &wire.RecordAttestation{Attestation: signedAttestation(t, "v3", '9', receipt)}}
		key, account := testAccount(t)
		stmt := admitTrack(t, f, key, account, 1, 0)
		mustSeal(t, f)
		_, claim := signedRC(t, f, snodeSigner(t, "s1"), arbiter.RCRecord{StatementID: f.st.Statements[stmt].Env.StatementID,
			SourceNode: "s0", SourceClaimRoot: "0xclaim"})
		evictV(t, f, a, "v3")
		evictV(t, f, a, "s0")
		rejectSignedUnchanged(t, f, forged, `node "v3": ed25519 verification failed`)
		rejectSignedUnchanged(t, f, claim, "rc source snode signature")
	})
}

// TestEvictionVoidingOffKeepsTodaysAdmission: before the stage-3 activation,
// and after it while eviction_voiding is off, every message of the list above
// from an evicted node is admitted exactly as the base admits it.
func TestEvictionVoidingOffKeepsTodaysAdmission(t *testing.T) {
	for _, fixture := range []struct {
		name  string
		build func(*testing.T) (*FSM, *authority.Signer)
	}{{"before stage3", tracksFSM}, {"stage3 with only purge on", purgeOnlyFSMV}} {
		for _, m := range evictedMessagesV() {
			t.Run(fixture.name+"/"+m.name, func(t *testing.T) {
				f, a := fixture.build(t)
				cmd, applied := m.build(t, f, a)
				mustApply(t, f, cmd)
				applied(t)
			})
		}
	}
}

// TestEvictionVoidingKeepsALatchedVerdictAcrossSnapshotRestore: spec §7.4 and
// §14. v1 and v2 latch quorum; evicting v1 afterwards leaves the verdict, the
// statement status and the stored evidence as they were, a snapshot restores
// them byte-identically (restore re-derives the latch from the stored evidence
// and never reads node status), and the restored replica takes the block to
// Promotable.
func TestEvictionVoidingKeepsALatchedVerdictAcrossSnapshotRestore(t *testing.T) {
	f, a := voidingFSMV(t)
	block, stmt := markedBlockV(t, f, 1)
	recordEvidenceV(t, f, block, "v1", true)
	recordEvidenceV(t, f, block, "v2", true)
	if judged, quorum := verdictV(f, block); !quorum || !slices.Equal(judged, []string{"v1", "v2"}) {
		t.Fatalf("fixture: verdict judged %v, quorum %v; want [v1 v2] latched", judged, quorum)
	}
	latched := verdictJSONV(t, f, block)
	evictV(t, f, a, "v1")
	if got := verdictJSONV(t, f, block); got != latched {
		t.Fatalf("evicting a verifier of a latched block changed its verdict:\n%s\n%s", latched, got)
	}
	if status := f.st.Statements[stmt].Status; status != StatusQuorumVerified {
		t.Fatalf("statement %d is %v after the eviction, want QuorumVerified", stmt, status)
	}
	data := snapshotBytes(t, f)
	g := restoreInto(t, data)
	if got := verdictJSONV(t, g, block); got != latched {
		t.Fatalf("restored verdict differs:\n%s\n%s", latched, got)
	}
	if !bytes.Equal(data, snapshotBytes(t, g)) {
		t.Fatal("the restored replica writes a different snapshot")
	}
	anchorFinal(t, g, block)
	if status := g.st.Statements[stmt].Status; status != StatusPromotable {
		t.Fatalf("statement %d on the restored replica is %v after finality, want Promotable", stmt, status)
	}
}

// TestEvictionVoidingDropsAnEvictedVerifiersEvidenceFromAnUnlatchedVerdict:
// spec §14. v3's passing evidence is judged; its eviction re-evaluates the
// block and the verdict judges nothing; v1's passing evidence alone does not
// latch (it would with v3's), and v2's then does, over v1 and v2. v3's
// evidence stays stored: voiding skips it, it never deletes it.
func TestEvictionVoidingDropsAnEvictedVerifiersEvidenceFromAnUnlatchedVerdict(t *testing.T) {
	f, a := voidingFSMV(t)
	block, stmt := markedBlockV(t, f, 1)
	recordEvidenceV(t, f, block, "v3", true)
	if judged, quorum := verdictV(f, block); quorum || !slices.Equal(judged, []string{"v3"}) {
		t.Fatalf("fixture: verdict judged %v, quorum %v; want [v3] unlatched", judged, quorum)
	}
	evictV(t, f, a, "v3")
	if judged, quorum := verdictV(f, block); quorum || len(judged) != 0 || judged == nil {
		t.Fatalf("after v3's eviction the verdict judged %v, quorum %v; want an empty verdict", judged, quorum)
	}
	recordEvidenceV(t, f, block, "v1", true)
	if judged, quorum := verdictV(f, block); quorum || !slices.Equal(judged, []string{"v1"}) {
		t.Fatalf("after v1's evidence the verdict judged %v, quorum %v; want [v1] unlatched", judged, quorum)
	}
	if status := f.st.Statements[stmt].Status; status != StatusReplaying {
		t.Fatalf("statement %d is %v, want Replaying", stmt, status)
	}
	recordEvidenceV(t, f, block, "v2", true)
	if judged, quorum := verdictV(f, block); !quorum || !slices.Equal(judged, []string{"v1", "v2"}) {
		t.Fatalf("after v2's evidence the verdict judged %v, quorum %v; want [v1 v2] latched", judged, quorum)
	}
	if status := f.st.Statements[stmt].Status; status != StatusQuorumVerified {
		t.Fatalf("statement %d is %v, want QuorumVerified", stmt, status)
	}
	bv := f.st.Verifications[block]
	if bv.Attestations["v3"] == nil || bv.ByteScans["v3"] == nil {
		t.Fatal("voiding deleted the evicted verifier's stored evidence")
	}
}

// TestEvictionVoidingKeepsTheEvidenceOfAChallengedBlock: spec §14. Once a
// statement of the block is challenged, evicting v3 leaves its failing
// evidence in the verdict, and the next evaluation still judges it.
func TestEvictionVoidingKeepsTheEvidenceOfAChallengedBlock(t *testing.T) {
	f, a := voidingFSMV(t)
	block, stmt := markedBlockV(t, f, 1)
	recordEvidenceV(t, f, block, "v1", true)
	recordEvidenceV(t, f, block, "v3", false)
	mustApply(t, f, wire.Command{OpenChallenge: &wire.OpenChallenge{BlockSeq: block, Reason: "verifier mismatch", OpenedBy: "v1"}})
	if status := f.st.Statements[stmt].Status; status != StatusChallengeReplay {
		t.Fatalf("fixture: statement %d is %v, want ChallengeReplay", stmt, status)
	}
	before := verdictJSONV(t, f, block)
	evictV(t, f, a, "v3")
	if got := verdictJSONV(t, f, block); got != before {
		t.Fatalf("evicting v3 changed the verdict of a challenged block:\n%s\n%s", before, got)
	}
	recordEvidenceV(t, f, block, "v2", true)
	if judged, _ := verdictV(f, block); !slices.Equal(judged, []string{"v1", "v2", "v3"}) {
		t.Fatalf("the challenged block's verdict judged %v, want [v1 v2 v3]", judged)
	}
}

// TestEvictNodeReevaluatesTheBlocksNamingIt: with eviction_voiding on,
// EvictNode re-evaluates the unlatched verdicts of the blocks whose verifier
// set names the node: one that judged v3 loses it; one that judged only v1 is
// byte-identical; one never evaluated keeps no verdict; a latched one is
// untouched. With the switch off EvictNode re-evaluates nothing.
func TestEvictNodeReevaluatesTheBlocksNamingIt(t *testing.T) {
	t.Run("eviction_voiding on", func(t *testing.T) {
		f, a := voidingFSMV(t)
		counted, _ := markedBlockV(t, f, 1)
		recordEvidenceV(t, f, counted, "v3", true)
		other, _ := markedBlockV(t, f, 1)
		recordEvidenceV(t, f, other, "v1", true)
		unjudged, _ := markedBlockV(t, f, 1)
		latched, _ := markedBlockV(t, f, 1)
		recordEvidenceV(t, f, latched, "v1", true)
		recordEvidenceV(t, f, latched, "v3", true)
		otherBefore, latchedBefore := verdictJSONV(t, f, other), verdictJSONV(t, f, latched)
		if f.st.Verifications[unjudged].Verdict != nil {
			t.Fatal("fixture: a block without evidence has a verdict")
		}
		evictV(t, f, a, "v3")
		if judged, quorum := verdictV(f, counted); quorum || judged == nil || len(judged) != 0 {
			t.Fatalf("block %d verdict judged %v, quorum %v; want an empty verdict", counted, judged, quorum)
		}
		if got := verdictJSONV(t, f, other); got != otherBefore {
			t.Fatalf("block %d verdict changed:\n%s\n%s", other, otherBefore, got)
		}
		if v := f.st.Verifications[unjudged].Verdict; v != nil {
			t.Fatalf("block %d gained a verdict %+v", unjudged, v)
		}
		if got := verdictJSONV(t, f, latched); got != latchedBefore {
			t.Fatalf("latched block %d verdict changed:\n%s\n%s", latched, latchedBefore, got)
		}
	})
	t.Run("eviction_voiding off", func(t *testing.T) {
		f, a := purgeOnlyFSMV(t)
		counted, _ := markedBlockV(t, f, 1)
		recordEvidenceV(t, f, counted, "v3", true)
		before := verdictJSONV(t, f, counted)
		evictV(t, f, a, "v3")
		if got := verdictJSONV(t, f, counted); got != before {
			t.Fatalf("EvictNode changed a verdict with eviction_voiding off:\n%s\n%s", before, got)
		}
	})
}

// TestEvictionVoidingSwitchOnReevaluatesTheBlocksOfEarlierEvictions: v3 is
// evicted while the switch is off and its evidence stays judged; the update
// that turns eviction_voiding on re-evaluates the block and drops it, and
// leaves a verdict v3 helped latch untouched. v4 is listed, unregistered, so
// that update keeps three non-evicted listed verifiers.
func TestEvictionVoidingSwitchOnReevaluatesTheBlocksOfEarlierEvictions(t *testing.T) {
	f, a := purgeOnlyFSMV(t)
	u := stage3Update(t, f)
	u.Verifiers = append(u.Verifiers, testVerifiers("v4")...)
	mustApply(t, f, signedConsensusUpdate(t, a, u))
	counted, _ := markedBlockV(t, f, 1)
	recordEvidenceV(t, f, counted, "v3", true)
	latched, _ := markedBlockV(t, f, 1)
	recordEvidenceV(t, f, latched, "v1", true)
	recordEvidenceV(t, f, latched, "v3", true)
	latchedBefore := verdictJSONV(t, f, latched)
	evictV(t, f, a, "v3")
	if judged, _ := verdictV(f, counted); !slices.Equal(judged, []string{"v3"}) {
		t.Fatalf("with the switch off block %d verdict judged %v, want [v3]", counted, judged)
	}
	turnOnStage3Switch(t, f, a, Stage3SwitchEvictionVoiding)
	if judged, quorum := verdictV(f, counted); quorum || judged == nil || len(judged) != 0 {
		t.Fatalf("after the switch-on block %d verdict judged %v, quorum %v; want an empty verdict", counted, judged, quorum)
	}
	if got := verdictJSONV(t, f, latched); got != latchedBefore {
		t.Fatalf("the switch-on changed latched block %d:\n%s\n%s", latched, latchedBefore, got)
	}
}

// TestWorkSetTreatsAnEvictedVerifiersEvidenceAsMissing: v1 passes, v2 and v3
// fail, so the block is QuorumFailed over complete evidence. With
// eviction_voiding on, evicting v3 makes its evidence missing: the block is
// EvidenceIncomplete (a re-selection candidate) and never QuorumFailed, so
// the orchestrator does not reject it. With the switch off it stays
// QuorumFailed, as today.
func TestWorkSetTreatsAnEvictedVerifiersEvidenceAsMissing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		build   func(*testing.T) (*FSM, *authority.Signer)
		voiding bool
	}{{"eviction_voiding on", voidingFSMV, true}, {"eviction_voiding off", purgeOnlyFSMV, false}} {
		t.Run(tc.name, func(t *testing.T) {
			f, a := tc.build(t)
			block, _ := markedBlockV(t, f, 1)
			recordEvidenceV(t, f, block, "v1", true)
			recordEvidenceV(t, f, block, "v2", false)
			recordEvidenceV(t, f, block, "v3", false)
			if ws := mustWorkSet(t, f); !slices.Equal(ws.QuorumFailed, []uint64{block}) || len(ws.EvidenceIncomplete) != 0 {
				t.Fatalf("fixture: QuorumFailed %v, EvidenceIncomplete %+v; want [%d] and none", ws.QuorumFailed, ws.EvidenceIncomplete, block)
			}
			evictV(t, f, a, "v3")
			ws := mustWorkSet(t, f)
			if !tc.voiding {
				if !slices.Equal(ws.QuorumFailed, []uint64{block}) || len(ws.EvidenceIncomplete) != 0 {
					t.Fatalf("with the switch off: QuorumFailed %v, EvidenceIncomplete %+v; want [%d] and none", ws.QuorumFailed, ws.EvidenceIncomplete, block)
				}
				return
			}
			want := []BlockEvidence{{BlockSeq: block, VerifierSet: []string{"v1", "v2", "v3"},
				HasAttestation: map[string]bool{"v1": true, "v2": true, "v3": false},
				HasScan:        map[string]bool{"v1": true, "v2": true, "v3": false}}}
			if len(ws.QuorumFailed) != 0 || !reflect.DeepEqual(ws.EvidenceIncomplete, want) {
				t.Fatalf("with the switch on: QuorumFailed %v, EvidenceIncomplete %+v; want none and %+v", ws.QuorumFailed, ws.EvidenceIncomplete, want)
			}
		})
	}
}

// TestAdoptParkedClaimsDiscardsAClaimOfAnEvictedSNode: clarification K72. s0
// parks a claim and is then evicted. Admission cannot bind s0 any more (it
// answers SOURCE_UNAVAILABLE and adopts nothing), so the adoption is driven
// directly with a statement bound to s0: with eviction_voiding on the claim
// leaves its slot and is not bound; with the switch off it binds, as today.
func TestAdoptParkedClaimsDiscardsAClaimOfAnEvictedSNode(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(*testing.T) (*FSM, *authority.Signer)
		binds bool
	}{{"eviction_voiding on", voidingFSMV, false}, {"eviction_voiding off", purgeOnlyFSMV, true}} {
		t.Run(tc.name, func(t *testing.T) {
			f, a := tc.build(t)
			key, account := testAccount(t)
			env := trackEnvelope(t, f, key, account, "", 1, 0)
			mustApply(t, f, parkedClaimV(t, f, env))
			flat := env.StatementID.Flat()
			slot := pendingRCKey(flat, "s0")
			if f.st.PendingRC[slot] == nil {
				t.Fatal("fixture: the claim did not park")
			}
			evictV(t, f, a, "s0")
			if r := submit(t, f, env); r.Code != arbiter.AdmissionCodeSourceUnavailable {
				t.Fatalf("admission for the evicted owner's table = %+v, want SOURCE_UNAVAILABLE", r)
			}
			if f.st.PendingRC[slot] == nil {
				t.Fatal("a refused admission took the parked claim")
			}
			ss := &StatementState{Env: env, SourceNode: "s0", Status: StatusSequenced}
			f.adoptParkedClaimsLocked(ss, flat)
			if f.st.PendingRC[slot] != nil {
				t.Fatal("adoption left the parked claim in its slot")
			}
			if bound := ss.RC != nil; bound != tc.binds {
				t.Fatalf("adoption bound the evicted SNode's claim: %v, want %v", bound, tc.binds)
			}
		})
	}
}

// TestReRegisteredNodeIsNoLongerVoided: spec §14, voiding parks a node and is
// not a key fence. Evicted v3 and s0 are refused; after a registration with a
// higher registration_seq each is Syncing, not Evicted, and the same signed
// messages are admitted: v3's evidence is judged and s0's claim binds.
func TestReRegisteredNodeIsNoLongerVoided(t *testing.T) {
	f, a := voidingFSMV(t)
	block, _ := markedBlockV(t, f, 1)
	key, account := testAccount(t)
	stmt := admitTrack(t, f, key, account, 1, 0)
	mustSeal(t, f)
	claim, _ := ownedClaim(t, f, stmt, "re-registered-claim")
	attestation, scan := evidenceV(t, f, block, "v3", true)
	evictV(t, f, a, "v3")
	evictV(t, f, a, "s0")
	rejectSignedUnchanged(t, f, attestation, voidedRefusalV("attestation", "v3"))
	rejectSignedUnchanged(t, f, claim, voidedRefusalV("result_claim", "s0"))
	mustApply(t, f, verifierRegistration(t, f, "v3", '3', 1))
	mustApply(t, f, snodeRegistration(t, f, snodeSigner(t, "s0"), "s0", 1))
	for _, node := range []string{"v3", "s0"} {
		if status := f.st.Nodes[node].Status; status != NodeSyncing {
			t.Fatalf("%s is %v after its re-registration, want Syncing", node, status)
		}
	}
	mustApply(t, f, attestation)
	mustApply(t, f, scan)
	mustApply(t, f, claim)
	if judged, _ := verdictV(f, block); !slices.Equal(judged, []string{"v3"}) {
		t.Fatalf("block %d verdict judged %v, want [v3]", block, judged)
	}
	if f.st.Statements[stmt].RC == nil {
		t.Fatal("the re-registered SNode's claim did not bind")
	}
}

// TestRegistrationAndMarkActiveKeepTheirChecks: spec §14 keeps registration
// and activation as they are. With eviction_voiding on, an evicted node's
// identical resend of its last registration is still an idempotent no-op that
// leaves it Evicted, its MarkActive is still refused with today's text, and a
// registration with a higher registration_seq followed by its MarkActive
// makes it Active again.
func TestRegistrationAndMarkActiveKeepTheirChecks(t *testing.T) {
	f, a := voidingFSMV(t)
	evictV(t, f, a, "v3")
	evictV(t, f, a, "s0")
	s0 := snodeSigner(t, "s0")
	for _, node := range []struct {
		id                         string
		resend, markZero, register wire.Command
		markOne                    wire.Command
	}{
		{"v3", verifierRegistration(t, f, "v3", '3', 0), verifierMarkActive(t, f, "v3", '3', 0),
			verifierRegistration(t, f, "v3", '3', 1), verifierMarkActive(t, f, "v3", '3', 1)},
		{"s0", snodeRegistration(t, f, s0, "s0", 0), snodeMarkActive(t, f, s0, "s0", 0),
			snodeRegistration(t, f, s0, "s0", 1), snodeMarkActive(t, f, s0, "s0", 1)},
	} {
		mustApplyUnchangedQ(t, f, node.resend)
		if status := f.st.Nodes[node.id].Status; status != NodeEvicted {
			t.Fatalf("%s is %v after an identical resend, want Evicted", node.id, status)
		}
		rejectSignedUnchanged(t, f, node.markZero, "node is evicted; re-register first")
		mustApply(t, f, node.register)
		mustApply(t, f, node.markOne)
		if status := f.st.Nodes[node.id].Status; status != NodeActive {
			t.Fatalf("%s is %v after its re-registration and MarkActive, want Active", node.id, status)
		}
	}
}

// TestRestoreDoesNotReapplyEvictionVoiding: spec §14, restore does not
// re-apply the check to stored records. s0's bound claim and acknowledgement,
// a latched verdict over evicted v1's evidence and an unlatched verdict that
// voiding emptied restore byte-identically, and the restored replica still
// refuses a new message of an evicted node.
func TestRestoreDoesNotReapplyEvictionVoiding(t *testing.T) {
	f, a := voidingFSMV(t)
	latched, p := promotedBlockV(t, f, a)
	ackTrackPromotion(t, f, p)
	voided, _ := markedBlockV(t, f, 1)
	recordEvidenceV(t, f, voided, "v3", true)
	attestation, _ := evidenceV(t, f, voided, "v1", true)
	evictV(t, f, a, "v3")
	evictV(t, f, a, "v1")
	evictV(t, f, a, "s0")
	data := snapshotBytes(t, f)
	g := restoreInto(t, data)
	if !bytes.Equal(data, snapshotBytes(t, g)) {
		t.Fatal("the restored replica writes a different snapshot")
	}
	if pp := g.st.PendingPromotions[p]; pp == nil || !pp.Acked || pp.Ack == nil || pp.Ack.NodeID != "s0" {
		t.Fatalf("restored promotion %d = %+v, want s0's stored acknowledgement", p, pp)
	}
	if judged, quorum := verdictV(g, latched); !quorum || !slices.Equal(judged, []string{"v1", "v2"}) {
		t.Fatalf("restored latched block %d verdict judged %v, quorum %v; want [v1 v2] latched", latched, judged, quorum)
	}
	if judged, quorum := verdictV(g, voided); quorum || judged == nil || len(judged) != 0 || g.st.Verifications[voided].Attestations["v3"] == nil {
		t.Fatalf("restored block %d verdict judged %v, quorum %v; want the stored empty verdict and v3's stored evidence", voided, judged, quorum)
	}
	rejectSignedUnchanged(t, g, attestation, voidedRefusalV("attestation", "v1"))
}
```

- [ ] **Step 3: Run them and see them fail**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./fsm/ -run 'TestEvictionVoiding|TestEvictNodeReevaluatesTheBlocksNamingIt|TestWorkSetTreatsAnEvictedVerifiersEvidenceAsMissing|TestAdoptParkedClaimsDiscardsAClaimOfAnEvictedSNode|TestReRegisteredNodeIsNoLongerVoided|TestRegistrationAndMarkActiveKeepTheirChecks|TestRestoreDoesNotReapplyEvictionVoiding' -race -count=1`
Expected: the package compiles (the tests use only Apply, the WorkSet, Task 64's fixture and the existing `adoptParkedClaimsLocked`) and FAILs with exactly these failures (timings omitted):

```text
--- FAIL: TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode
    --- FAIL: TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode/result_claim_that_binds
        eviction_voiding_test.go:274: expected Rejected, got fsm.Applied {}
    --- FAIL: TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode/result_claim_that_parks
        eviction_voiding_test.go:274: expected Rejected, got fsm.Applied {}
    --- FAIL: TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode/attestation
        eviction_voiding_test.go:274: expected Rejected, got fsm.Applied {}
    --- FAIL: TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode/byte-side_scan
        eviction_voiding_test.go:274: expected Rejected, got fsm.Applied {}
    --- FAIL: TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode/promotion_acknowledgement
        eviction_voiding_test.go:274: expected Rejected, got fsm.Applied {}
    --- FAIL: TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode/cleanup_acknowledgement
        eviction_voiding_test.go:274: expected Rejected, got fsm.Applied {}
    --- FAIL: TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode/rejected-part_cleanup_acknowledgement
        eviction_voiding_test.go:274: expected Rejected, got fsm.Applied {}
--- FAIL: TestEvictionVoidingDropsAnEvictedVerifiersEvidenceFromAnUnlatchedVerdict
    eviction_voiding_test.go:363: after v3's eviction the verdict judged [v3], quorum false; want an empty verdict
--- FAIL: TestEvictNodeReevaluatesTheBlocksNamingIt
    --- FAIL: TestEvictNodeReevaluatesTheBlocksNamingIt/eviction_voiding_on
        eviction_voiding_test.go:430: block 2 verdict judged [v3], quorum false; want an empty verdict
--- FAIL: TestEvictionVoidingSwitchOnReevaluatesTheBlocksOfEarlierEvictions
    eviction_voiding_test.go:476: after the switch-on block 2 verdict judged [v3], quorum false; want an empty verdict
--- FAIL: TestWorkSetTreatsAnEvictedVerifiersEvidenceAsMissing
    --- FAIL: TestWorkSetTreatsAnEvictedVerifiersEvidenceAsMissing/eviction_voiding_on
        eviction_voiding_test.go:516: with the switch on: QuorumFailed [2], EvidenceIncomplete []; want none and [{BlockSeq:2 VerifierSet:[v1 v2 v3] HasAttestation:map[v1:true v2:true v3:false] HasScan:map[v1:true v2:true v3:false]}]
--- FAIL: TestAdoptParkedClaimsDiscardsAClaimOfAnEvictedSNode
    --- FAIL: TestAdoptParkedClaimsDiscardsAClaimOfAnEvictedSNode/eviction_voiding_on
        eviction_voiding_test.go:556: adoption bound the evicted SNode's claim: true, want false
--- FAIL: TestReRegisteredNodeIsNoLongerVoided
    eviction_voiding_test.go:576: expected Rejected, got fsm.Applied {}
--- FAIL: TestRestoreDoesNotReapplyEvictionVoiding
    eviction_voiding_test.go:657: restored block 3 verdict judged [v3], quorum false; want the stored empty verdict and v3's stored evidence
FAIL
FAIL	github.com/sentioxyz/arbiter/fsm
FAIL
```

Every other test of the run already PASSes, because it pins behaviour this task must keep: `TestEvictionVoidingOffKeepsTodaysAdmission`, `TestEvictionVoidingKeepsALatchedVerdictAcrossSnapshotRestore`, `TestEvictionVoidingKeepsTheEvidenceOfAChallengedBlock`, `TestRegistrationAndMarkActiveKeepTheirChecks`, the `eviction_voiding off` subtests and the `signature checks run first` subtest. If a test fails with another message, stop and compare the fixture with Task 64's: the block numbers assume `stage3FSM` seals nothing beyond tracksFSM's block 1.

- [ ] **Step 4: Create `fsm/eviction_voiding.go`**

Create `fsm/eviction_voiding.go`:

```go
package fsm

import (
	"fmt"
	"slices"
)

// Eviction voiding, stage-3 switch 2 (housegate spec 2026-10-14 §10.4 step 2,
// §14). Once eviction_voiding is committed, every result claim, attestation,
// byte-side scan, and promotion, cleanup and rejected-part cleanup
// acknowledgement from a node whose Nodes record is Evicted is refused, each
// at its own call site right after that site's signature check
// (evictedSignerRefusalLocked). The check never sits inside
// snodeSignatureLocked or verifierSignatureLocked: registration and MarkActive
// share them and keep their current checks, so a re-registration with a higher
// registration_seq makes the node speak again (voiding parks a node; it is not
// a key fence). Evidence an evicted verifier recorded before its eviction
// leaves the verdict of an unlatched, unchallenged block (reevaluateBlock) and
// counts as missing in the WorkSet (evidencePresence); a latched verdict is
// never re-evaluated. Restore re-applies none of this: it re-derives a latched
// quorum from the stored evidence and never reads node status. Before the
// switch every predicate here is false and every caller behaves as at the
// base.

// evictionVoidingLocked reports whether the eviction_voiding switch is
// committed.
func (f *FSM) evictionVoidingLocked() bool {
	return f.stage3SwitchOnLocked(Stage3SwitchEvictionVoiding)
}

// evictedNodeLocked reports whether nodeID names a Nodes record whose status
// is Evicted.
func (f *FSM) evictedNodeLocked(nodeID string) bool {
	n := f.st.Nodes[nodeID]
	return n != nil && n.Status == NodeEvicted
}

// evictedSignerRefusalLocked is the refusal of a message of kind signed by
// nodeID while eviction_voiding is on and the node is evicted, else "". Every
// caller runs it after its own signature check and before any mutation; Plan
// S3-E3 calls it for MemberReady (kind member_ready) and MemberCheckpoint
// (member_checkpoint).
func (f *FSM) evictedSignerRefusalLocked(kind, nodeID string) string {
	if !f.evictionVoidingLocked() || !f.evictedNodeLocked(nodeID) {
		return ""
	}
	return fmt.Sprintf("%s from %s: the node is evicted (stage3 eviction_voiding)", kind, nodeID)
}

// voidedReplicaLocked is the predicate of the replicas whose evidence voiding
// skips: nil while eviction_voiding is off, so reevaluateBlock and the WorkSet
// run exactly as at the base; afterwards every evicted node.
func (f *FSM) voidedReplicaLocked() func(replica string) bool {
	if !f.evictionVoidingLocked() {
		return nil
	}
	return f.evictedNodeLocked
}

// reevaluateBlocksNamingLocked re-evaluates, in block order, every block whose
// verifier set names nodeID and that has an unlatched verdict, so a verdict
// stops judging the evidence of a node evicted in this Apply. A block without
// a verdict has judged nothing; its first evaluation already skips the node.
// applyEvictNode calls it while eviction_voiding is on.
func (f *FSM) reevaluateBlocksNamingLocked(nodeID string) {
	f.reevaluateUnlatchedBlocksLocked(func(set []string) bool { return slices.Contains(set, nodeID) })
}

// reevaluateVoidedBlocksLocked re-evaluates, in block order, every block whose
// verifier set names an evicted node and that has an unlatched verdict: the
// switch-on update voids the evidence of every node evicted before it.
func (f *FSM) reevaluateVoidedBlocksLocked() {
	f.reevaluateUnlatchedBlocksLocked(func(set []string) bool { return slices.ContainsFunc(set, f.evictedNodeLocked) })
}

func (f *FSM) reevaluateUnlatchedBlocksLocked(names func(set []string) bool) {
	for seq := uint64(1); seq <= uint64(len(f.st.Blocks)); seq++ {
		bv := f.st.Verifications[seq]
		if bv == nil || bv.Verdict == nil || bv.Verdict.Quorum || !names(bv.VerifierSet) {
			continue
		}
		f.reevaluateBlock(seq)
	}
}
```

- [ ] **Step 5: Refuse an evicted node's messages and re-evaluate on eviction in `fsm/apply.go`**

In `applyEvictNode`, replace the status change (`:138-139`):

```go
	n.Status = NodeEvicted
	n.Reason = c.Reason
```

with:

```go
	n.Status = NodeEvicted
	n.Reason = c.Reason
	// Eviction voiding (spec 2026-10-14 §14): the unlatched verdicts that
	// judged the node's evidence stop judging it in this Apply.
	if f.evictionVoidingLocked() {
		f.reevaluateBlocksNamingLocked(c.NodeID)
	}
```

In `applyRegisterRC`, replace the authentication block (`:483-487`):

```go
	if active {
		if reason := f.resultClaimRefusalLocked(c); reason != "" {
			return Rejected{Reason: reason}
		}
	}
```

with:

```go
	if active {
		if reason := f.resultClaimRefusalLocked(c); reason != "" {
			return Rejected{Reason: reason}
		}
	}
	// Eviction voiding (spec 2026-10-14 §14): an evicted SNode's claim neither
	// parks nor binds once the switch is on.
	if reason := f.evictedSignerRefusalLocked(string(authority.SNodeMessageResultClaim), rc.SourceNode); reason != "" {
		return Rejected{Reason: reason}
	}
```

In `applyRecordAttestation`, replace the signature check and the record line (`:608-611`):

```go
	if err := f.verifyNodeSig(att.ReplicaID, att.ReceiptHash, att.Signature); err != nil {
		return Rejected{Reason: err.Error()}
	}
	rec := att
```

with:

```go
	if err := f.verifyNodeSig(att.ReplicaID, att.ReceiptHash, att.Signature); err != nil {
		return Rejected{Reason: err.Error()}
	}
	// Eviction voiding (spec 2026-10-14 §14): an evicted verifier records no
	// new evidence once the switch is on.
	if reason := f.evictedSignerRefusalLocked("attestation", att.ReplicaID); reason != "" {
		return Rejected{Reason: reason}
	}
	rec := att
```

In `applyRecordByteSideScan`, replace the signature check and the record line (`:637-640`):

```go
	if err := f.verifyNodeSig(scan.ReplicaID, scan.ScanHash, scan.Signature); err != nil {
		return Rejected{Reason: err.Error()}
	}
	rec := scan
```

with:

```go
	if err := f.verifyNodeSig(scan.ReplicaID, scan.ScanHash, scan.Signature); err != nil {
		return Rejected{Reason: err.Error()}
	}
	if reason := f.evictedSignerRefusalLocked("byte_side_scan", scan.ReplicaID); reason != "" {
		return Rejected{Reason: reason}
	}
	rec := scan
```

In `applyRecordPromotionAck`, replace the source acknowledgement check and the end of its block (`:814-817`):

```go
		if reason := f.sourceAckRefusalLocked(authority.SNodeMessagePromotionAck, ack.NodeID, ack.PromotionSeq, ack, c.SourceJWS); reason != "" {
			return Rejected{Reason: reason}
		}
	}
```

with:

```go
		if reason := f.sourceAckRefusalLocked(authority.SNodeMessagePromotionAck, ack.NodeID, ack.PromotionSeq, ack, c.SourceJWS); reason != "" {
			return Rejected{Reason: reason}
		}
		// Eviction voiding (spec 2026-10-14 §14), Applied:false included.
		if reason := f.evictedSignerRefusalLocked(string(authority.SNodeMessagePromotionAck), ack.NodeID); reason != "" {
			return Rejected{Reason: reason}
		}
	}
```

In `applyRecordCleanupAck`, replace the source acknowledgement check and the end of its block (`:1038-1041`):

```go
		if reason := f.sourceAckRefusalLocked(authority.SNodeMessageCleanupAck, c.Ack.NodeID, c.Ack.PromotionSeq, c.Ack, c.SourceJWS); reason != "" {
			return Rejected{Reason: reason}
		}
	}
```

with:

```go
		if reason := f.sourceAckRefusalLocked(authority.SNodeMessageCleanupAck, c.Ack.NodeID, c.Ack.PromotionSeq, c.Ack, c.SourceJWS); reason != "" {
			return Rejected{Reason: reason}
		}
		// Eviction voiding (spec 2026-10-14 §14).
		if reason := f.evictedSignerRefusalLocked(string(authority.SNodeMessageCleanupAck), c.Ack.NodeID); reason != "" {
			return Rejected{Reason: reason}
		}
	}
```

- [ ] **Step 6: Refuse an evicted SNode's rejected-part cleanup acknowledgement in `fsm/rejected_cleanup.go`**

In `recordRejectedCleanupAckLocked`, replace the source signature check (`:343-345`):

```go
	if err := f.snodeSignatureLocked(kind, expected, ack, c.SourceJWS); err != nil {
		return Rejected{Reason: fmt.Sprintf("%s from %s: %v", kind, ack.NodeID, err)}
	}
```

with:

```go
	if err := f.snodeSignatureLocked(kind, expected, ack, c.SourceJWS); err != nil {
		return Rejected{Reason: fmt.Sprintf("%s from %s: %v", kind, ack.NodeID, err)}
	}
	// Eviction voiding (spec 2026-10-14 §14).
	if reason := f.evictedSignerRefusalLocked(string(kind), ack.NodeID); reason != "" {
		return Rejected{Reason: reason}
	}
```

- [ ] **Step 7: Discard a claim parked by an evicted SNode in `fsm/signed_claims.go`**

In `adoptParkedClaimsLocked`'s `take`, replace the tracks discard (`:681-683`):

```go
		if f.tracksActiveLocked() && (namelessCandidatePartRefusal(*rc) != "" || foreignTableClaimRefusal(*rc, ss.Env.TargetTableID) != "") {
			return
		}
```

with:

```go
		if f.tracksActiveLocked() && (namelessCandidatePartRefusal(*rc) != "" || foreignTableClaimRefusal(*rc, ss.Env.TargetTableID) != "") {
			return
		}
		// Eviction voiding (spec 2026-10-14 §14, plan S3-E2 K72): a claim
		// parked by an SNode evicted since is discarded, never bound.
		if f.evictedSignerRefusalLocked(string(authority.SNodeMessageResultClaim), rc.SourceNode) != "" {
			return
		}
```

- [ ] **Step 8: Skip voided evidence in `reevaluateBlock` (`fsm/threeway.go`)**

In `reevaluateBlock`, replace the pinned state and the head of the evidence loop (`:73-79`):

```go
	expectedRoot, baseRoots, claimed, stateErr := f.pinnedBlockState(blockSeq)

	header := f.st.Blocks[blockSeq-1]
	verdict := &ThreeWayVerdict{Replicas: map[string]ReplicaVerdict{}}
	pass := 0
	for _, rid := range bv.VerifierSet {
		att := bv.Attestations[rid]
```

with:

```go
	expectedRoot, baseRoots, claimed, stateErr := f.pinnedBlockState(blockSeq)
	// Eviction voiding (spec 2026-10-14 §14): an evicted verifier's evidence
	// leaves the verdict unless a statement of the block is challenged; a
	// latched block never reaches this line.
	voided := f.voidedReplicaLocked()
	if blockChallenged(stmts) {
		voided = nil
	}

	header := f.st.Blocks[blockSeq-1]
	verdict := &ThreeWayVerdict{Replicas: map[string]ReplicaVerdict{}}
	pass := 0
	for _, rid := range bv.VerifierSet {
		if voided != nil && voided(rid) {
			continue // voided evidence is not judged (spec §14)
		}
		att := bv.Attestations[rid]
```

- [ ] **Step 9: Count voided evidence as missing in the WorkSet (`fsm/reads_work.go`)**

Replace the head of `evidencePresence` (`:20-26`):

```go
func evidencePresence(bv *BlockVerification) (map[string]bool, map[string]bool, bool) {
	hasAtt := make(map[string]bool, len(bv.VerifierSet))
	hasScan := make(map[string]bool, len(bv.VerifierSet))
	missing := false
	for _, rid := range bv.VerifierSet {
		hasAtt[rid] = bv.Attestations[rid] != nil
		hasScan[rid] = bv.ByteScans[rid] != nil
```

with:

```go
// evidencePresence reports, per verifier of bv's set, whether its attestation
// and byte-side scan are recorded, and whether any is missing. voided is nil
// before eviction voiding (spec 2026-10-14 §14); afterwards a voided replica's
// evidence counts as missing, so its block is EvidenceIncomplete and never
// QuorumFailed.
func evidencePresence(bv *BlockVerification, voided func(replica string) bool) (map[string]bool, map[string]bool, bool) {
	hasAtt := make(map[string]bool, len(bv.VerifierSet))
	hasScan := make(map[string]bool, len(bv.VerifierSet))
	missing := false
	for _, rid := range bv.VerifierSet {
		live := voided == nil || !voided(rid)
		hasAtt[rid] = live && bv.Attestations[rid] != nil
		hasScan[rid] = live && bv.ByteScans[rid] != nil
```

In `WorkSet`, replace the one call (`:139`):

```go
		hasAtt, hasScan, missing := evidencePresence(bv)
```

with:

```go
		hasAtt, hasScan, missing := evidencePresence(bv, f.voidedReplicaLocked())
```

- [ ] **Step 10: Re-evaluate at the switch-on in `fsm/stage3.go`**

Task 64 created `onStage3SwitchOnLocked` with an empty `switch name` (Step 1 found no call of `reevaluateVoidedBlocksLocked`). Replace its body:

```go
func (f *FSM) onStage3SwitchOnLocked(name string) {
	switch name {
	default:
	}
}
```

with:

```go
func (f *FSM) onStage3SwitchOnLocked(name string) {
	switch name {
	case Stage3SwitchEvictionVoiding:
		// Spec 2026-10-14 §14: the evidence of every node evicted before the
		// switch leaves the unlatched verdicts that judged it.
		f.reevaluateVoidedBlocksLocked()
	default:
	}
}
```

If Task 64's switch already holds other cases, add only the `case Stage3SwitchEvictionVoiding:` arm, before `default:`; Task 69 adds the `purge` case in front of it under the hunk rule (§10.4 order).

- [ ] **Step 11: Document the switch in `README.md`**

In `README.md`, inside Task 65's section `## Multi-source storage integrity (stage-3 activation)`, insert this paragraph as one line right after the paragraph that begins `**Switches.**`, with one empty line before it and one after it:

```markdown
**Eviction voiding (switch 2).** Once `eviction_voiding` is committed (housegate spec 2026-10-14 §14), the arbiter refuses every result claim, attestation, byte-side scan, and promotion, cleanup or rejected-part cleanup acknowledgement from a node whose registration is Evicted, right after the message's own signature check and with state unchanged: `<kind> from <node>: the node is evicted (stage3 eviction_voiding)`, where `<kind>` is `result_claim`, `attestation`, `byte_side_scan`, `promotion_ack` or `cleanup_ack`. A claim the node parked before its eviction is dropped, never bound. Evidence an evicted verifier recorded before its eviction stops counting in the verdict of a block whose quorum has not latched and none of whose statements is challenged: `EvictNode` re-evaluates the blocks whose verifier set names the node, and the update that turns the switch on does the same for every node evicted before it. The evidence stays stored, and `GetBlockVerification` serves it with `evaluated` false. The work set lists such a block as evidence-incomplete, never as quorum-failed, so the orchestrator never rejects a block over an evicted verifier's evidence; until the `reselection` switch (spec §10.4 step 7) replaces that verifier, the block passes only if its two remaining verifiers latch quorum, and otherwise it and its track wait. A latched verdict and the evidence of a challenged block are kept, across snapshots too. Registration and `MarkActive` keep their checks: a node that re-registers with a higher `registration_seq` is Syncing, no longer evicted, and is heard again. An evicted SNode can no longer acknowledge its own promotions and cleanups, which keep counting in the drain gate, so every consensus update is refused until its track is quarantined: quarantine a track with in-flight work before evicting its SNode (spec §10.2).
```

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && grep -c '^\*\*Eviction voiding (switch 2)\.\*\*' README.md && git diff --numstat -- README.md`
Expected: `1`, then `2	0	README.md` (the paragraph and its empty line).

- [ ] **Step 12: Run them and see them pass**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test -z "$(gofmt -l fsm)" && bazel run //:gazelle && git -C "$W" status --porcelain`
Expected: gofmt lists nothing (if it lists a file, run `gofmt -w` on it and repeat); gazelle adds `"eviction_voiding.go"` to the `srcs` of `//fsm:fsm` and `"eviction_voiding_test.go"` to the `srcs` of `//fsm:fsm_test`, in sorted position, and no dependency; the status lists exactly ` M README.md`, ` M fsm/BUILD.bazel`, ` M fsm/apply.go`, ` M fsm/reads_work.go`, ` M fsm/rejected_cleanup.go`, ` M fsm/signed_claims.go`, ` M fsm/stage3.go`, ` M fsm/threeway.go`, `?? fsm/eviction_voiding.go` and `?? fsm/eviction_voiding_test.go`.

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && go test ./fsm/ -run 'TestEvictionVoiding|TestEvictNodeReevaluatesTheBlocksNamingIt|TestWorkSetTreatsAnEvictedVerifiersEvidenceAsMissing|TestAdoptParkedClaimsDiscardsAClaimOfAnEvictedSNode|TestReRegisteredNodeIsNoLongerVoided|TestRegistrationAndMarkActiveKeepTheirChecks|TestRestoreDoesNotReapplyEvictionVoiding' -race -count=1 -v | grep -E '^--- |^ok|^FAIL'`
Expected (timings omitted):

```text
--- PASS: TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode
--- PASS: TestEvictionVoidingOffKeepsTodaysAdmission
--- PASS: TestEvictionVoidingKeepsALatchedVerdictAcrossSnapshotRestore
--- PASS: TestEvictionVoidingDropsAnEvictedVerifiersEvidenceFromAnUnlatchedVerdict
--- PASS: TestEvictionVoidingKeepsTheEvidenceOfAChallengedBlock
--- PASS: TestEvictNodeReevaluatesTheBlocksNamingIt
--- PASS: TestEvictionVoidingSwitchOnReevaluatesTheBlocksOfEarlierEvictions
--- PASS: TestWorkSetTreatsAnEvictedVerifiersEvidenceAsMissing
--- PASS: TestAdoptParkedClaimsDiscardsAClaimOfAnEvictedSNode
--- PASS: TestReRegisteredNodeIsNoLongerVoided
--- PASS: TestRegistrationAndMarkActiveKeepTheirChecks
--- PASS: TestRestoreDoesNotReapplyEvictionVoiding
ok  	github.com/sentioxyz/arbiter/fsm
```

and no data race.

- [ ] **Step 13: Run the full checks**

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && test -z "$(gofmt -l fsm $(git ls-files '*.go'))" && go vet ./fsm/ && go test -race ./fsm/ ./orchestrator/ ./server/ -count=1 && go test ./fsm/ -run 'TestPreActivationHistoryIsByteIdentical|TestL3CommitmentPreimagesAreFrozen|TestL3TrackHeaderPreimagesAreFrozen|TestL3TransitionHeaderPreimageIsFrozen|TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease|TestDeterminism' -count=1 -v | grep -E '^--- (PASS|FAIL)'`
Expected: gofmt and vet print nothing; `ok` for `fsm`, `orchestrator` and `server` with no data race (the orchestrator's dispatch tests read the WorkSet this task changes, and pass unchanged); then only `--- PASS` lines, among them `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, `TestPreActivationHistoryIsByteIdenticalAcrossTracks`, `TestL3CommitmentPreimagesAreFrozen`, `TestL3TrackHeaderPreimagesAreFrozen`, `TestL3TransitionHeaderPreimageIsFrozen`, `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease` and every `TestDeterminism_*`, and no `--- FAIL`.

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors`
Expected: the build succeeds and every test target PASSes (CI's two Bazel steps).

Run: `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e; cd "$W" && bazel query 'deps(//fsm:fsm, 1)' | grep -c com_github_sentioxyz_arbiter_proto; grep -rn 'time\.Now' fsm/; git -C "$W" status --porcelain -- fsm/testdata`
Expected: `0`, then nothing, then nothing (the CI red lines hold, and no golden file changed).

- [ ] **Step 14: Commit**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" add fsm/eviction_voiding.go fsm/eviction_voiding_test.go fsm/apply.go fsm/rejected_cleanup.go fsm/signed_claims.go fsm/threeway.go fsm/reads_work.go fsm/stage3.go fsm/BUILD.bazel README.md
git -C "$W" commit -m "feat(stage3): eviction voiding, switch 2 (Task 68)

Once the stage3 eviction_voiding switch is committed, a result claim,
attestation, byte-side scan, or promotion, cleanup or rejected-part
cleanup acknowledgement from an Evicted node is refused right after
its signature check, and a claim it parked is discarded at adoption
(K72). reevaluateBlock skips an evicted verifier's evidence unless
quorum latched or a statement is challenged, EvictNode and the
switch-on re-evaluate the unlatched verdicts that judged it, and the
WorkSet counts that evidence as missing (EvidenceIncomplete, never
QuorumFailed). Registration and MarkActive keep their checks and
restore re-applies nothing (housegate spec 2026-10-14 §14; plan S3-E2
contract §47). The README documents the switch for operators. With
the switch off nothing changes.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C "$W" status --porcelain
```

Expected: one new commit on `urwt/claude/multi-source-s3e` whose subject is `feat(stage3): eviction voiding, switch 2 (Task 68)`, and an empty status. Task 69 starts from this commit.

---

## Task 69: arbiter — purge switch: scoped incarnations, the member-aware purge-required set and their restore rules (S3-E2)

Spec §8.8 and §10.4 step 1 make `purge` the first stage-3 switch: from it every chain incarnation is recorded on the incarnation-scoped Keeper path `/sentio/<shard>/unsafe_inc/<db__table>/<incarnation_seq>`, and the purge-required set becomes the owner's SNode, every non-evicted network-pool verifier and every non-evicted node that has been a member of the incarnation's group since it was recorded, on scoped and unscoped paths alike, so a dead member delays a purge only until it is evicted. Today `applyAddTable` (`fsm/apply_table_registry.go:220-275`) records no path kind, and `purgeRequiredLocked` (`:469-489`) counts every non-evicted VERIFIER, which from step 4 on would make every member hold every purge network-wide (K68). This task adds the FSM half of the switch (the data-plane half, `ddl.KeeperScope`, the scoped root, the replica listings, the sweeps and `KnownKeeperReplica`, is Plan S3-E1 Task 61's, its contract §42): `TableIncarnation.ScopedKeeperPath`, set by `applyAddTable` while the switch is on; `TableIncarnation.PurgeMembers`, which Plan S3-E3 Task 71 fills; the member-aware `purgeRequiredLocked`; the switch-on rerun of `completePurgesLocked` in `onStage3SwitchOnLocked`; both fields in the registry view and in `tableRegistryToPB`; the pre-v21 forged-field arm of `stage3FieldsPresent`; the restore rule `validateStage3Incarnations` (contract §47); and one README paragraph. With the switch off nothing changes, and `GetPurgeNodeSet` never does.

Worktree: Task 64's URWT worktree `$HOME/src/remotesrc/claude/arbiter/multi-source-s3e` on branch `urwt/claude/multi-source-s3e` (contract §48), with `HEAD` at Task 68's last commit; every command below starts with `W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e` in its own Bash call. Line anchors are arbiter `526a721` (`v0.13.0`, `origin/main` when this was drafted: Plan S3-A merged as arbiter#133); for every source file this task edits they equal the header's `fb42260` anchors (`fsm/BUILD.bazel` and `README.md` moved with Plan S3-A). `fsm/stage3.go`, `fsm/stage3_restore.go`, `fsm/stage3_fixture_test.go`, `stage3FieldsPresent`, `stage3PendingProtoFields` and the README section `## Multi-source storage integrity (stage-3 activation)` are Tasks 64–66's and are named by function or heading; every hunk is applied on top of Tasks 64–68, and of anything Plans S3-C2 and S3-D changed after `526a721`, under the hunk rule (Global Constraints). This task makes no remote or live action and so asks for no confirmation in chat, with one guard: Tasks 64–68 filled the Go module cache and the Bazel caches, so no `go` or `bazel` command below should download or fetch anything; if one reports that it does, stop, because that is a remote read and needs explicit user confirmation in chat before it runs. The task ends with two local commits; Task 70 pushes the branch.

**Files:**
- Create: `fsm/purge_stage3_test.go`
- Modify: `fsm/table_registry.go` (`TableIncarnation` `:35-62`: two fields after `OwnerIndexerID` `:61`; `(*FSM).TableRegistryView` `:317-349`: one line after `c.PurgedBy = slices.Clone(inc.PurgedBy)` `:328`)
- Modify: `fsm/apply_table_registry.go` (`applyAddTable` `:220-275`: between the refusal block `:266-270` and `reg.appendIncarnation(inc)` `:271`; `purgeRequiredLocked` and its comment `:469-489`)
- Modify: `fsm/stage3.go` (Task 64's `onStage3SwitchOnLocked`: a first `case` in its `switch name {`)
- Modify: `fsm/snapshot.go` (Task 66's `stage3FieldsPresent`: before its final `return false`)
- Modify: `fsm/stage3_restore.go` (Task 66's `validateStage3State`: a call right after the `validateStage3Activation` check; `validateStage3Incarnations` appended at the end of the file)
- Modify: `server/safestate_stage3_test.go` (Task 64's `stage3PendingProtoFields`: its two `TableIncarnation.*` entries deleted)
- Modify: `server/table_registry.go` (`tableRegistryToPB` `:100-130`: the `pb.TableIncarnation` literal `:118-127`, after `OwnerIndexerId: inc.OwnerIndexerID,` `:126`)
- Test: `server/table_registry_test.go` (a new test right before `TestGetTableRegistry`, `:51` at `526a721`)
- Modify: `README.md` (Task 65's section `## Multi-source storage integrity (stage-3 activation)`: a last paragraph, right before the heading `## Artifact disposition admission`, `:396` at `526a721`)
- Modify (gazelle): `fsm/BUILD.bazel` (`fsm_test` srcs: `"purge_stage3_test.go",` after `"purge_owner_test.go",`, `:174` at `526a721`; no deps change)

**Interfaces:**
- Consumes:
  - Plan S3-E1 (contract §48, through Task 64's pin): `pb.TableIncarnation.ScopedKeeperPath` (field 19, `bool`) and `.PurgeMembers` (field 20, `[]string`) with their getters; `authority.GroupMemberConsentStatement{NetworkID, GenesisSnapshotID string; HostIndexerID uint64; NodeID string; Ed25519Pubkey []byte; ReplicaSigner string}`, `func (s *authority.Signer) SignGroupMemberConsentAt(stmt GroupMemberConsentStatement, iat int64) (string, error)`, `authoritytest.Iat`; the host-bound fields of `arbiter.VerifierEntry`.
  - Task 64 (contract §45): `Stage3SwitchPurge`, `Stage3SwitchEvictionVoiding`, `Stage3SwitchOwnerAwareSelection`, `Stage3SwitchPlacement`; `func stage3SwitchValue(p *arbiter.Stage3Params, name string) bool`; `func (f *FSM) stage3SwitchOnLocked(name string) bool`, `func (f *FSM) Stage3SwitchOn(name string) bool`; `func (f *FSM) poolVerifierLocked(nodeID string) bool` (a non-evicted VERIFIER-role node whose listed entry has no `host_indexer_id`); the seam `func (f *FSM) onStage3SwitchOnLocked(name string)` (called by `recordStage3SwitchesLocked` once the new parameters are in place and before it appends the cutover); `State.Stage3.Switches` with `Stage3SwitchCutover.IncarnationSeq` (`purge` only: `uint64(len(TableRegistry.Incarnations)) + 1` at the switch-on, the seq the next incarnation gets); the K68 refusal of host-bound entries until `owner_aware_selection` is committed; `var stage3PendingProtoFields map[string]string` in `server/safestate_stage3_test.go` with the entries `"TableIncarnation.scoped_keeper_path": "Task 69"` and `"TableIncarnation.purge_members": "Task 69"`, which `TestTableRegistryMirrorsProto` consults; the fixture `func stage3FSM(t *testing.T) (*FSM, *authority.Signer)`, `func stage3Update(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate`, `func turnOnStage3Switch(t *testing.T, f *FSM, a *authority.Signer, name string)`, `func stage3TestMember(host uint64) string`, `func memberEntry(t *testing.T, f *FSM, host uint64, node, domain string) arbiter.VerifierEntry`.
  - Task 65 (contract §45): every Apply that commits a changed `Params.Stage3`, each switch-on included, bumps the registry version once; `TableRegistryView.Stage3 *arbiter.Stage3Params`.
  - Task 66 (contract §46): `snapshotVersionV21`; `func stage3FieldsPresent(doc *snapshotDoc) bool`, refused under a pre-v21 version byte with `stage3 state is present in a pre-v21 container`; `func validateStage3State(st *State) error`, which `readSnapshot` runs right after `validateTrackState`, wrapped `snapshot: %w`; `validateStage3Activation`, which runs first and refuses a switch that is on without a cutover record and a purge cutover incarnation seq outside `1..len(Incarnations)+1`.
  - Task 68 (contract §47): its `case Stage3SwitchEvictionVoiding:` in `onStage3SwitchOnLocked`, which stays after this task's case; its re-evaluation in `applyEvictNode`, which runs before the purge completion there and is not touched.
  - Base (`526a721`): `completePurgesLocked` (`fsm/apply_table_registry.go:496`), `purgedCount` (`:525`), `registryChangedLocked` (`:28`), `incarnationOwnerSNodeLocked` (`:453`), `PurgeNodeSet` and `purgeNodeIDsLocked` (`:406-424`, unchanged), `hasRole`, `TableOriginChain`, `TableOriginGenesis`, the completion pattern of `applyEvictNode` (`fsm/apply.go:121-149`), `purgeReportRefusalLocked` (`fsm/signed_claims.go:701`: a purge report comes from the owner's SNode or a verifier); fsm test helpers `mustApply` (`fsm/fsm_test.go:83`), `ev` (`fsm/apply_table_registry_test.go:14`), `retireCmd` (`:230`), `ownedAddCmd` (`fsm/table_registry_owners_test.go:24`), `authorityContextFor`, `verifierRegistration`, `verifierMarkActive`, `snodePurged`, `verifierPurged` (`fsm/signed_messages_fixture_test.go:39`, `:73`, `:87`, `:98`, `:110`), `snodeSigner` (`fsm/pre_tracks_golden_test.go:30`), `ed25519KeyFor` (`fsm/evidence_test.go:16`), `signedConsensusUpdate` (`fsm/consensus_updates_test.go:26`), `mustGenesisID` (`fsm/table_registry_test.go:32`), `snapshotBytes`, `restoreInto` (`fsm/snapshot_test.go:30`, `:44`), `restoreConfigured` (`fsm/table_set_transition_restore_test.go:31`), `restoreRefusedBothWays` (`fsm/snapshot_tracks_forgery_test.go:22`), `tracksFSM` (`fsm/tracks_fixture_test.go:81`); server `tableRegistryToPB` (`server/table_registry.go:100`).
- Produces (contract §47 names verbatim; the extra tests and the test helpers are this task's, recorded in Appendix B):
  - `fsm`: `TableIncarnation.ScopedKeeperPath bool` (JSON `scoped_keeper_path,omitempty`) and `TableIncarnation.PurgeMembers []string` (JSON `purge_members,omitempty`, sorted, unique), declared after `OwnerIndexerID`; `TableRegistryView` deep-copies `PurgeMembers`; `applyAddTable` sets `ScopedKeeperPath` on every incarnation it appends while `purge` is on, Refused ones included, and nothing else sets or clears it; `func (f *FSM) purgeRequiredLocked(inc *TableIncarnation) []string` (signature unchanged) is, with `purge` on, the owner's SNode (registered, not evicted), every node for which `poolVerifierLocked` holds and every registered, non-evicted VERIFIER-role node that `inc.PurgeMembers` names, sorted, and with it off exactly today's set; `onStage3SwitchOnLocked`'s `case Stage3SwitchPurge:` reruns `completePurgesLocked` and bumps the registry version when a purge completed; `stage3FieldsPresent` is also true for any incarnation with `ScopedKeeperPath` or a non-empty `PurgeMembers`; `func validateStage3Incarnations(st *State) error`, run by `validateStage3State` right after `validateStage3Activation`, with the refusals (verbatim, checked in this order for each incarnation in seq order) `incarnation %d carries scoped_keeper_path without the purge switch`, `incarnation %d is recorded after the purge cutover (incarnation %d) without scoped_keeper_path`, `incarnation %d carries scoped_keeper_path but precedes the purge cutover (incarnation %d)`, `incarnation %d is not a chain incarnation but carries scoped_keeper_path`, `incarnation %d purge_members must be sorted, unique, non-empty node ids`, `incarnation %d carries purge_members before the placement switch` (Plan S3-E3 Task 71 replaces the last one with its subset rule).
  - `server`: `tableRegistryToPB` sets `pb.TableIncarnation.ScopedKeeperPath` and `.PurgeMembers`; `stage3PendingProtoFields` loses `TableIncarnation.scoped_keeper_path` and `TableIncarnation.purge_members`, so `TestTableRegistryMirrorsProto` pins both JSON tags.
  - Tests: fsm `TestPurgeSwitchOffKeepsEveryIncarnationUnscoped`, `TestPurgeSwitchMarksEveryLaterChainIncarnationScoped`, `TestPurgeRequiredSetBeforeThePurgeSwitchIsEveryNonEvictedVerifier`, `TestPurgeSwitchOnWithNoMembersChangesNoPurge`, `TestPurgeSwitchOnHookCompletesAPurgeThatWaitedOnlyForAMember` (added here), `TestPurgeRequiredSetIsTheOwnerThePoolAndPurgeMembersOnceThePurgeSwitchIsOn`, `TestEvictNodeOfAPurgeMemberCompletesThePurge`, `TestPurgeNodeSetIsUnchangedByThePurgeSwitch`, `TestRegistryViewServesScopedKeeperPathAndPurgeMembers`, `TestRestoreRefusesAScopedIncarnationBeforeThePurgeCutover`, `TestRestoreRefusesAnUnscopedIncarnationAfterThePurgeCutover`, `TestRestoreRefusesMalformedPurgeMembers`, `TestRestoreRefusesScopedKeeperPathAndPurgeMembersInAPreV21Container` (added here); server `TestTableRegistryToPBCarriesScopedKeeperPathAndPurgeMembers` and the edited `TestTableRegistryMirrorsProto` (through the pending map); fsm test helpers `purgeTestAdd`, `purgeTestRetire`, `purgeTestPurging`, `purgeTestSeed`, `purgeTestMember`, `purgeTestMembers`, `purgeTestReport`, `purgeTestEvict`, `purgeTestRoundTrip`.
  - `README.md`: the paragraph `**Purge switch**` (Step 19).

Notes:
- **Switch-off identity (contract §44).** Both fields are `omitempty` and only `applyAddTable` under the switch (and, later, Plan S3-E3 Task 71 under `placement`) sets them, so every pre-stage-3 container and every container written while `purge` is off is byte-identical to Task 68's, and the golden tests (`fsm/testdata/stage3_v21_plain.snapshot` and Task 67's interned one included, both written with every switch off) run unchanged. With the switch off `purgeRequiredLocked` is the base expression split into its two arms (the owner's SNode, then every VERIFIER), so it returns exactly the base set; `TestPurgeRequiredSetBeforeThePurgeSwitchIsEveryNonEvictedVerifier` runs the same history before the activation and after it.
- **Who is awaited from the switch on.** Contract §47 says "every non-evicted node of `inc.PurgeMembers`". `purgeRequiredLocked` walks the `Nodes` records, as today, so a named node is awaited only once it is registered: a member creates its `hg_member_unsafe` replicas only while its node is registered and not evicted (spec §8.3), so a named node that never registered holds nothing. It must also hold the VERIFIER role: a member is a VERIFIER-role node (spec S7), and `applyRecordTablePurged` accepts a report only from the owner's SNode or a verifier (`purgeReportRefusalLocked`), so a named node of another role could never report and would hold the purge for good; for a real member the check changes nothing. The pool is `poolVerifierLocked`, every non-evicted VERIFIER whose listed entry has no host: rule 6 of the signed-claims membership (`fsm/signed_claims.go:277-292`) evicts every running verifier the list does not hold, so every non-evicted verifier is listed and the pool leaves out exactly the host-bound entries. A listed member leaves the set only through `EvictNode`, and delisting one evicts it in the same Apply (rule 6), which reruns completion (`evictNodesLocked`).
- **The switch-on rerun is a no-op in committed histories.** K68 refuses every host-bound entry until `owner_aware_selection` (switch 3) is committed, and every running verifier is listed (rule 6), so when `purge` turns on the pool is every non-evicted verifier and `purge_members` is empty: the set does not change and nothing completes (`TestPurgeSwitchOnWithNoMembersChangesNoPurge`). The rerun is still made, as contract §47 asks, so completion stays a function of committed state whatever a later change does; `TestPurgeSwitchOnHookCompletesAPurgeThatWaitedOnlyForAMember` drives the hook on a hand-built state. Inside the switch-on Apply, Task 65's bump for the changed `stage3` comes first and `recordStage3SwitchesLocked` calls the hook afterwards (Task 64's order), so a purge completed by the hook bumps the version a second time in that Apply; versions only need to increase, so followers are unaffected (Appendix B).
- **The scoped rule.** Chain incarnations are appended only by `applyAddTable` (`fsm/apply_table_registry.go:260-271`); genesis incarnations exist from the registry's enabling, before any `stage3`, and Legacy ones come from seeds (`:63`, `:113`), so neither is ever scoped, even a Legacy seed of an indexer enrolled after the switch. The purge cutover's `IncarnationSeq` is the seq of the first incarnation recorded under the switch, so restore requires exactly: scoped if and only if the switch is on, the incarnation is a chain one and its seq is at least the cutover's. A Refused incarnation is marked too (contract §47): it holds no tables, and marking it keeps the rule a function of the seq alone. `IncarnationSeq` stays unserved (decision D33); the served `scoped_keeper_path` tells every reconciler which path an incarnation uses.
- **`purge_members` in this task.** Nothing in Tasks 64–70 writes it; Plan S3-E3 Task 71 fills it at record time, at the placement backfill and at every member a `RebalanceGroup` adds, while the incarnation is physical (K60). The tests here set it directly on the live state where a purge set needs it, and restore such a state only to show the refusals. A present but empty list counts as absent (`omitempty` never writes one), in `stage3FieldsPresent` and in the restore rule alike.
- **Fixture facts.** In `stage3FSM` incarnation 1 is the genesis `db.t` (owner 1), 2 the Legacy `db.old`, 3 the chain `db0.x` (owner 0), and the registry cursor is at block 151, so this file creates its tables from block 160 on, each test with its own blocks. `memberEntry` gives the member its own ed25519 key and the host's consent; `purgeTestMember` replaces the key with `ed25519KeyFor(seed)` and re-signs the consent over it with the host's enrolment key (`snodeSigner`: `s0` for indexer 0, `s1` for indexer 1), which lets the member register, report purges and be evicted through the base helpers' signed paths.
- **Documentation.** The plan's File Structure lists no README edit for this task; Tasks 65 and 66 opened the README's stage-3 section, so Step 19 adds the purge switch there, as one paragraph (Appendix B). The base purge sentences (`README.md` `:296`, `:328`) describe the rule before the switch and stay true for it.
- **Checked while drafting** (2026-10-16): every code block of this task was applied, step by step, to a `git archive` export of arbiter `526a721` carrying stand-ins of Task 64 (its 2026-10-16 draft), Task 65's fsm half and README, and Task 66 (its working draft, with the plain golden captured), over arbiter-proto and arbiter-core stubs of Plan S3-E1's contract wired in with `replace` directives; `GOWORK=off GOFLAGS=-mod=mod GOPROXY=off go test`. Observed: Step 4's build failure and mirror failure, Step 6's four passes and nine failures, Step 12's thirteen passes under `-race`, Step 16's failure, Step 18's passes; `go test ./... -count=1` and `go test ./... -race -count=1` pass, `go vet ./...` and `gofmt` are clean; no `time.Now` in `fsm/`. Each of 25 single-point mutations (no marking; marking that skips Refused incarnations; marking with the switch off; the old purge set; members not counted; the pool taken for every verifier; the owner arm dropped; the member-aware set with the switch off; no rerun; no bump; the view sharing `purge_members`; no `stage3FieldsPresent` arm, or one missing `purge_members`; no restore call; each of the six restore rules dropped; the cutover compare off by one; an empty id or a repeat admitted; no transport mapping, or one missing `purge_members`) fails at least one named test. On top of Task 68's in-progress draft (its hook case merged after this task's by the hunk rule) the whole fsm, server and orchestrator suites pass; on Task 67's draft tree, which lacks Task 65, every test passes except the one-bump assertion of `TestPurgeSwitchOnWithNoMembersChangesNoPurge`, which needs Task 65's bump, as expected. A patch tool given Step 10's hunk without the function name put it into `tracksFieldsPresent`, whose tail is the same; `TestRestoreRefusesScopedKeeperPathAndPurgeMembersInAPreV21Container` caught it. Bazel and gazelle were not run (offline, and no P5 tag exists yet); the `fsm/BUILD.bazel` change is what gazelle writes for the new file.

- [ ] **Step 1: Base check**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" rev-parse --abbrev-ref HEAD
git -C "$W" status --porcelain
git -C "$W" log -1 --format=%s | grep -q '(Task 68)$' && echo head-is-task-68
git -C "$W" grep -n -e 'func (f \*FSM) onStage3SwitchOnLocked' -e 'func (f \*FSM) poolVerifierLocked' -e 'func stage3FieldsPresent' -e 'func validateStage3State' -e 'func stage3TestMember' -e 'func memberEntry' -e 'Stage3 \*arbiter.Stage3Params$' -- fsm
git -C "$W" grep -n -e '"TableIncarnation\.scoped_keeper_path"' -e '"TableIncarnation\.purge_members"' -- server
cd "$W" && GOWORK=off go test ./fsm/ ./server/ -count=1
```

Expected: `urwt/claude/multi-source-s3e`; an empty status; `head-is-task-68`; seven fsm lines, `fsm/snapshot.go` (`func stage3FieldsPresent`), `fsm/stage3.go` twice (`poolVerifierLocked`, `onStage3SwitchOnLocked`), `fsm/stage3_fixture_test.go` twice (`stage3TestMember`, `memberEntry`), `fsm/stage3_restore.go` (`validateStage3State`) and `fsm/table_registry.go` (Task 65's `TableRegistryView.Stage3`); two lines of `server/safestate_stage3_test.go`, both valued `"Task 69"`; then `ok  	github.com/sentioxyz/arbiter/fsm` and `ok  	github.com/sentioxyz/arbiter/server`. Stop if any line is missing or a package fails: Tasks 64–68 are the base of every hunk below.

- [ ] **Step 2: Write the failing FSM tests**

Create `fsm/purge_stage3_test.go`:

```go
package fsm

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The purge switch (housegate spec 2026-10-14 §8.8, §10.4 step 1; plan S3-E2
// contract §47, Task 69) on the stage-3 fixture (stage3FSM): owner 1 (SNode
// s1, genesis table db.t), owner 0 (SNode s0, chain table db0.x), the pool
// verifiers v1..v3, the registry cursor at block 151. The tables of this file
// are created from block 160 on, each test with its own blocks. A member is
// listed only once the owner_aware_selection switch is committed (K68), and
// purge_members is filled by Plan S3-E3 Task 71, so these tests set it
// directly on the live state where they need it.

// purgeTestAdd records db.table, owned by owner, created at block and declared
// at block+1, and returns the incarnation it appended (a Refused one
// included).
func purgeTestAdd(t *testing.T, f *FSM, db, table string, block, owner uint64) *TableIncarnation {
	t.Helper()
	mustApply(t, f, ownedAddCmd(t, f, db, table, ev(block, fmt.Sprintf("0x%d", block), 0), ev(block+1, fmt.Sprintf("0x%d", block+1), 0), owner))
	reg := f.st.TableRegistry
	return reg.Incarnations[len(reg.Incarnations)-1]
}

// purgeTestRetire retires db.table at block.
func purgeTestRetire(t *testing.T, f *FSM, db, table string, block uint64) {
	t.Helper()
	mustApply(t, f, retireCmd(db, wire.TableRetireReasonTableDeleted, ev(block, fmt.Sprintf("0x%d", block), 0), table))
}

// purgeTestPurging records dbp.table owned by owner over blocks block and
// block+1 and retires it at block+2: a Pending incarnation whose add
// transition was never sealed retires straight to Purging.
func purgeTestPurging(t *testing.T, f *FSM, table string, block, owner uint64) *TableIncarnation {
	t.Helper()
	inc := purgeTestAdd(t, f, "dbp", table, block, owner)
	purgeTestRetire(t, f, "dbp", table, block+2)
	if inc.Status != TableStatusPurging {
		t.Fatalf("dbp.%s = %s, want purging", table, inc.Status)
	}
	return inc
}

// purgeTestSeed is the ed25519 key seed node signs with: the pool verifier
// v<n> registered with seed '<n>' (registerActive), and purgeTestMember lists
// member-0 with seed 'a' and member-1 with seed 'b'.
func purgeTestSeed(t *testing.T, node string) byte {
	t.Helper()
	switch node {
	case "v1", "v2", "v3":
		return node[1]
	case stage3TestMember(0):
		return 'a'
	case stage3TestMember(1):
		return 'b'
	}
	t.Fatalf("no ed25519 seed for %s", node)
	return 0
}

// purgeTestMember lists stage3TestMember(host) as a host-bound verifier
// (memberEntry: every flag on, weight 1, its host's failure domain) whose
// ed25519 key is ed25519KeyFor(purgeTestSeed(node)), with its consent
// re-signed over that key by the host's enrolment key, then registers and
// activates it through its signed messages, so its purge reports and its
// eviction take the production paths. The owner_aware_selection switch must
// be committed (K68).
func purgeTestMember(t *testing.T, f *FSM, a *authority.Signer, host uint64) string {
	t.Helper()
	node := stage3TestMember(host)
	seed := purgeTestSeed(t, node)
	entry := memberEntry(t, f, host, node, fmt.Sprintf("node:n%d", host))
	entry.Ed25519Pubkey = ed25519KeyFor(seed).Public().(ed25519.PublicKey)
	consent, err := snodeSigner(t, fmt.Sprintf("s%d", host)).SignGroupMemberConsentAt(authority.GroupMemberConsentStatement{
		NetworkID: f.st.Params.NetworkID, GenesisSnapshotID: mustGenesisID(t, f), HostIndexerID: host, NodeID: node,
		Ed25519Pubkey: entry.Ed25519Pubkey, ReplicaSigner: entry.ReplicaSigner}, authoritytest.Iat)
	if err != nil {
		t.Fatal(err)
	}
	entry.ConsentJWS = consent
	u := stage3Update(t, f)
	u.Verifiers = append(u.Verifiers, entry)
	mustApply(t, f, signedConsensusUpdate(t, a, u))
	mustApply(t, f, verifierRegistration(t, f, node, seed, 1))
	mustApply(t, f, verifierMarkActive(t, f, node, seed, 1))
	if f.poolVerifierLocked(node) {
		t.Fatalf("host-bound %s counts as a network-pool verifier", node)
	}
	return node
}

// purgeTestMembers turns on whichever of the switches purge,
// eviction_voiding and owner_aware_selection is still off, in that order,
// then lists, registers and activates member-0 (hosted by indexer 0) and
// member-1 (indexer 1).
func purgeTestMembers(t *testing.T, f *FSM, a *authority.Signer) (string, string) {
	t.Helper()
	for _, name := range []string{Stage3SwitchPurge, Stage3SwitchEvictionVoiding, Stage3SwitchOwnerAwareSelection} {
		if !f.Stage3SwitchOn(name) {
			turnOnStage3Switch(t, f, a, name)
		}
	}
	return purgeTestMember(t, f, a, 0), purgeTestMember(t, f, a, 1)
}

// purgeTestReport applies each node's signed purge report for inc: s0 and s1
// sign with their indexers' enrolment keys, every verifier with its
// purgeTestSeed key.
func purgeTestReport(t *testing.T, f *FSM, inc *TableIncarnation, nodes ...string) {
	t.Helper()
	for _, node := range nodes {
		if node == "s0" || node == "s1" {
			mustApply(t, f, snodePurged(t, f, snodeSigner(t, node), node, inc.Seq))
			continue
		}
		mustApply(t, f, verifierPurged(t, f, node, purgeTestSeed(t, node), inc.Seq))
	}
}

// purgeTestEvict applies the authority-signed eviction of node at its current
// registration_seq.
func purgeTestEvict(t *testing.T, f *FSM, a *authority.Signer, node, reason string) {
	t.Helper()
	seq := f.st.RegistrationSeqs[node]
	token, err := a.SignEvictNodeWithContext(arbiter.EvictNodeCommand{NodeID: node, ExpectedRegistrationSeq: seq, Reason: reason}, authorityContextFor(t, f))
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, f, wire.Command{EvictNode: &wire.EvictNode{NodeID: node, ExpectedRegistrationSeq: seq, Reason: reason, AuthorityJWS: token}})
}

// purgeTestRoundTrip requires f's container to be v21, accepted by the bare
// and the configured restore, each re-writing it byte for byte, and returns
// the bare restore.
func purgeTestRoundTrip(t *testing.T, f *FSM, label string) *FSM {
	t.Helper()
	data := snapshotBytes(t, f)
	if data[4] != snapshotVersionV21 {
		t.Fatalf("%s: version byte = %d, want 21", label, data[4])
	}
	bare := restoreInto(t, data)
	if !bytes.Equal(snapshotBytes(t, bare), data) {
		t.Fatalf("%s: bare restore ∘ snapshot is not the identity", label)
	}
	configured, err := restoreConfigured(t, f, data)
	if err != nil {
		t.Fatalf("%s: configured restore: %v", label, err)
	}
	if !bytes.Equal(snapshotBytes(t, configured), data) {
		t.Fatalf("%s: configured restore ∘ snapshot is not the identity", label)
	}
	return bare
}

// TestPurgeSwitchOffKeepsEveryIncarnationUnscoped: before the stage-3
// activation, and after it while the purge switch is off, AddTable records no
// incarnation on the scoped path, an accepted or a Refused one, so no
// container carries either new member and the pre-activation container stays
// v20 (contract §44).
func TestPurgeSwitchOffKeepsEveryIncarnationUnscoped(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture func(*testing.T) (*FSM, *authority.Signer)
		version byte
	}{
		{"before the activation", tracksFSM, snapshotVersionV20},
		{"after the activation with every switch off", stage3FSM, snapshotVersionV21},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := tc.fixture(t)
			added := purgeTestAdd(t, f, "dbp", "a", 160, 0)
			refused := purgeTestAdd(t, f, "db0", "x", 162, 0)
			if added.Status != TableStatusPending || refused.Status != TableStatusRefused {
				t.Fatalf("fixture: dbp.a is %s and the second db0.x %s, want pending and refused", added.Status, refused.Status)
			}
			for _, inc := range f.st.TableRegistry.Incarnations {
				if inc.ScopedKeeperPath || inc.PurgeMembers != nil {
					t.Fatalf("incarnation %d (%s) carries a purge-switch field: %+v", inc.Seq, inc.Key(), inc)
				}
			}
			data := snapshotBytes(t, f)
			if data[4] != tc.version {
				t.Fatalf("version byte = %d, want %d", data[4], tc.version)
			}
			if bytes.Contains(data, []byte(`"scoped_keeper_path"`)) || bytes.Contains(data, []byte(`"purge_members"`)) {
				t.Fatal("the container carries a purge-switch incarnation member")
			}
		})
	}
}

// TestPurgeSwitchMarksEveryLaterChainIncarnationScoped: from the purge switch
// on, every incarnation AddTable records is on the scoped path, the Refused
// one included; the cutover records the first such seq; an incarnation
// recorded earlier keeps the unscoped path whatever its status becomes; and
// restore keeps every flag.
func TestPurgeSwitchMarksEveryLaterChainIncarnationScoped(t *testing.T) {
	f, a := stage3FSM(t)
	purgeTestAdd(t, f, "dbp", "before", 160, 1)
	next := uint64(len(f.st.TableRegistry.Incarnations)) + 1
	turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
	if c := f.st.Stage3.Switches; len(c) != 1 || c[0].Switch != Stage3SwitchPurge || c[0].IncarnationSeq != next {
		t.Fatalf("cutovers = %+v, want the purge switch at incarnation %d", c, next)
	}
	after := purgeTestAdd(t, f, "dbp", "after", 162, 0)
	refused := purgeTestAdd(t, f, "db0", "x", 164, 0)
	if after.Seq != next || after.Status != TableStatusPending || refused.Status != TableStatusRefused {
		t.Fatalf("fixture: dbp.after is incarnation %d (%s), the second db0.x %s", after.Seq, after.Status, refused.Status)
	}
	purgeTestRetire(t, f, "dbp", "before", 166)
	purgeTestRetire(t, f, "dbp", "after", 167)
	for _, inc := range f.st.TableRegistry.Incarnations {
		if want := inc.Seq >= next; inc.ScopedKeeperPath != want {
			t.Fatalf("incarnation %d (%s, %s, %s) scoped_keeper_path = %v, want %v", inc.Seq, inc.Key(), inc.Origin, inc.Status, inc.ScopedKeeperPath, want)
		}
	}
	g := purgeTestRoundTrip(t, f, "scoped incarnations")
	for i, inc := range g.st.TableRegistry.Incarnations {
		if inc.ScopedKeeperPath != f.st.TableRegistry.Incarnations[i].ScopedKeeperPath {
			t.Fatalf("restored incarnation %d scoped_keeper_path = %v", inc.Seq, inc.ScopedKeeperPath)
		}
	}
}

// TestPurgeRequiredSetBeforeThePurgeSwitchIsEveryNonEvictedVerifier: after
// the activation and before the purge switch the purge-required set is the
// pre-stage-3 one (Plan S2-A contract §3b.5), computed alike on the same
// history without the activation: the owner's SNode and every non-evicted
// verifier; an evicted verifier leaves it.
func TestPurgeRequiredSetBeforeThePurgeSwitchIsEveryNonEvictedVerifier(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture func(*testing.T) (*FSM, *authority.Signer)
	}{
		{"before the activation", tracksFSM},
		{"after the activation with every switch off", stage3FSM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, a := tc.fixture(t)
			first := purgeTestPurging(t, f, "p", 160, 1)
			second := purgeTestPurging(t, f, "q", 163, 0)
			if got := f.purgeRequiredLocked(first); !slices.Equal(got, []string{"s1", "v1", "v2", "v3"}) {
				t.Fatalf("owner 1's purge waits for %v", got)
			}
			if got := f.purgeRequiredLocked(second); !slices.Equal(got, []string{"s0", "v1", "v2", "v3"}) {
				t.Fatalf("owner 0's purge waits for %v", got)
			}
			purgeTestEvict(t, f, a, "v3", "verifier lost")
			if got := f.purgeRequiredLocked(first); !slices.Equal(got, []string{"s1", "v1", "v2"}) {
				t.Fatalf("after evicting v3 owner 1's purge waits for %v", got)
			}
			purgeTestReport(t, f, first, "s1", "v1", "v2")
			if first.Status != TableStatusPurged {
				t.Fatalf("owner 1's purge = %s after every awaited report", first.Status)
			}
		})
	}
}

// TestPurgeSwitchOnWithNoMembersChangesNoPurge: with no host-bound entry
// (none can exist before the owner_aware_selection switch, K68) the network
// pool is every non-evicted verifier, so the switch-on rerun of
// completePurgesLocked completes nothing: a purge that waits for v3 still
// waits, its set is unchanged, and the registry version moves only by the one
// bump of the changed stage3 parameter (Task 65).
func TestPurgeSwitchOnWithNoMembersChangesNoPurge(t *testing.T) {
	f, a := stage3FSM(t)
	inc := purgeTestPurging(t, f, "p", 160, 1)
	purgeTestReport(t, f, inc, "s1", "v1", "v2")
	want := []string{"s1", "v1", "v2", "v3"}
	if got := f.purgeRequiredLocked(inc); !slices.Equal(got, want) {
		t.Fatalf("before the switch the purge waits for %v, want %v", got, want)
	}
	version := f.st.TableRegistry.Version
	turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
	if got := f.purgeRequiredLocked(inc); inc.Status != TableStatusPurging || !slices.Equal(got, want) {
		t.Fatalf("after the switch the purge is %s and waits for %v, want purging and %v", inc.Status, got, want)
	}
	if got := f.st.TableRegistry.Version; got != version+1 {
		t.Fatalf("registry version %d -> %d, want one bump", version, got)
	}
	purgeTestReport(t, f, inc, "v3")
	if inc.Status != TableStatusPurged {
		t.Fatalf("the purge = %s after v3's report", inc.Status)
	}
}

// TestPurgeSwitchOnHookCompletesAPurgeThatWaitedOnlyForAMember pins the
// switch-on hook directly (white box): K68 keeps every host-bound entry out
// of a committed history before the owner_aware_selection switch, so this
// state is built by hand. A purge that waits only for a registered
// host-bound verifier that purge_members does not name completes when the
// purge switch turns on, and the hook bumps the registry version once (the
// applyEvictNode pattern).
func TestPurgeSwitchOnHookCompletesAPurgeThatWaitedOnlyForAMember(t *testing.T) {
	f, _ := stage3FSM(t)
	inc := purgeTestPurging(t, f, "p", 160, 1)
	m0 := stage3TestMember(0)
	entry := memberEntry(t, f, 0, m0, "node:n0")
	f.st.Params.Verifiers = append(f.st.Params.Verifiers, entry)
	f.st.Nodes[m0] = &NodeInfo{Registration: arbiter.NodeRegistration{NodeID: m0, Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier},
		Ed25519Pubkey: entry.Ed25519Pubkey, RegistrationSeq: 1}, Status: NodeActive}
	purgeTestReport(t, f, inc, "s1", "v1", "v2", "v3")
	if inc.Status != TableStatusPurging {
		t.Fatalf("before the switch the purge = %s, want it waiting for %s", inc.Status, m0)
	}
	version := f.st.TableRegistry.Version
	f.st.Params.Stage3.Purge = true
	f.onStage3SwitchOnLocked(Stage3SwitchPurge)
	if inc.Status != TableStatusPurged || f.st.TableRegistry.Version != version+1 {
		t.Fatalf("after the switch-on hook the purge = %s, registry version %d -> %d", inc.Status, version, f.st.TableRegistry.Version)
	}
}

// TestPurgeRequiredSetIsTheOwnerThePoolAndPurgeMembersOnceThePurgeSwitchIsOn:
// spec §8.8. With the purge switch on, a purge waits for the owner's SNode,
// every network-pool verifier and every non-evicted verifier purge_members
// names, on the unscoped path (an incarnation recorded before the switch)
// and on the scoped path alike. A named node with no registration holds no
// replica (a member creates its replicas only while registered, spec §8.3)
// and is not awaited. A listed, registered member that purge_members does
// not name holds no replica of the table and is not awaited either
// (GetPurgeNodeSet still lists it); its report is accepted all the same,
// since a member reports at once the purge of an incarnation it never held.
func TestPurgeRequiredSetIsTheOwnerThePoolAndPurgeMembersOnceThePurgeSwitchIsOn(t *testing.T) {
	f, a := stage3FSM(t)
	unscoped := purgeTestAdd(t, f, "dbp", "u", 160, 1)
	m0, m1 := purgeTestMembers(t, f, a)
	purgeTestRetire(t, f, "dbp", "u", 162)
	scoped := purgeTestPurging(t, f, "s", 163, 1)
	owned0 := purgeTestPurging(t, f, "o", 166, 0)
	if unscoped.Status != TableStatusPurging || unscoped.ScopedKeeperPath || !scoped.ScopedKeeperPath || !owned0.ScopedKeeperPath {
		t.Fatalf("fixture: dbp.u %s scoped %v, dbp.s scoped %v, dbp.o scoped %v", unscoped.Status, unscoped.ScopedKeeperPath, scoped.ScopedKeeperPath, owned0.ScopedKeeperPath)
	}
	unscoped.PurgeMembers = []string{m0, "member-9"}
	scoped.PurgeMembers = []string{m0}
	owned0.PurgeMembers = []string{m1}
	for _, tc := range []struct {
		name string
		inc  *TableIncarnation
		want []string
	}{
		{"unscoped, owner 1", unscoped, []string{m0, "s1", "v1", "v2", "v3"}},
		{"scoped, owner 1", scoped, []string{m0, "s1", "v1", "v2", "v3"}},
		{"scoped, owner 0", owned0, []string{m1, "s0", "v1", "v2", "v3"}},
	} {
		if got := f.purgeRequiredLocked(tc.inc); !slices.Equal(got, tc.want) {
			t.Fatalf("%s: the purge waits for %v, want %v", tc.name, got, tc.want)
		}
	}
	if got := f.PurgeNodeSet(); !slices.Equal(got, []string{m0, m1, "s0", "s1", "v1", "v2", "v3"}) {
		t.Fatalf("GetPurgeNodeSet = %v", got)
	}
	purgeTestReport(t, f, scoped, "s1", "v1", "v2", "v3", m1)
	if scoped.Status != TableStatusPurging {
		t.Fatalf("dbp.s = %s before %s reported", scoped.Status, m0)
	}
	purgeTestReport(t, f, scoped, m0)
	if scoped.Status != TableStatusPurged {
		t.Fatalf("dbp.s = %s after every awaited report", scoped.Status)
	}
}

// TestEvictNodeOfAPurgeMemberCompletesThePurge: a member leaves a purge's
// required set only through EvictNode (spec §8.8), whose Apply reruns
// completePurgesLocked and bumps the registry version once.
func TestEvictNodeOfAPurgeMemberCompletesThePurge(t *testing.T) {
	f, a := stage3FSM(t)
	m0, _ := purgeTestMembers(t, f, a)
	inc := purgeTestPurging(t, f, "p", 160, 1)
	inc.PurgeMembers = []string{m0}
	purgeTestReport(t, f, inc, "s1", "v1", "v2", "v3")
	if inc.Status != TableStatusPurging {
		t.Fatalf("the purge = %s before %s reported", inc.Status, m0)
	}
	version := f.st.TableRegistry.Version
	purgeTestEvict(t, f, a, m0, "member host lost")
	if inc.Status != TableStatusPurged || f.st.TableRegistry.Version != version+1 {
		t.Fatalf("after evicting %s the purge = %s, registry version %d -> %d", m0, inc.Status, version, f.st.TableRegistry.Version)
	}
	if slices.Contains(f.PurgeNodeSet(), m0) {
		t.Fatalf("evicted %s is still in GetPurgeNodeSet", m0)
	}
}

// TestPurgeNodeSetIsUnchangedByThePurgeSwitch: GetPurgeNodeSet stays every
// registered, non-evicted SNode and verifier through the purge switch and the
// members' registration, because the data plane reads it as the live replica
// membership of every table (spec §2): only purge completion is member-aware.
func TestPurgeNodeSetIsUnchangedByThePurgeSwitch(t *testing.T) {
	f, a := stage3FSM(t)
	want := []string{"s0", "s1", "v1", "v2", "v3"}
	if got := f.PurgeNodeSet(); !slices.Equal(got, want) {
		t.Fatalf("after the activation GetPurgeNodeSet = %v, want %v", got, want)
	}
	turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
	if got := f.PurgeNodeSet(); !slices.Equal(got, want) {
		t.Fatalf("after the purge switch GetPurgeNodeSet = %v, want %v", got, want)
	}
	m0, m1 := purgeTestMembers(t, f, a)
	if got := f.PurgeNodeSet(); !slices.Equal(got, []string{m0, m1, "s0", "s1", "v1", "v2", "v3"}) {
		t.Fatalf("with two registered members GetPurgeNodeSet = %v", got)
	}
	purgeTestEvict(t, f, a, m1, "member host lost")
	if got := f.PurgeNodeSet(); !slices.Equal(got, []string{m0, "s0", "s1", "v1", "v2", "v3"}) {
		t.Fatalf("after evicting %s GetPurgeNodeSet = %v", m1, got)
	}
}

// TestRegistryViewServesScopedKeeperPathAndPurgeMembers: the registry view,
// which GetTableRegistry and WatchTableRegistry serve and every reconciler
// derives the Keeper path from, carries both fields as a deep copy.
func TestRegistryViewServesScopedKeeperPathAndPurgeMembers(t *testing.T) {
	f, a := stage3FSM(t)
	turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
	inc := purgeTestAdd(t, f, "dbp", "v", 160, 1)
	inc.PurgeMembers = []string{"member-0", "member-1"}
	view, ok := f.TableRegistryView()
	if !ok {
		t.Fatal("the registry view is disabled")
	}
	got := view.Live("dbp.v")
	if got == nil || !got.ScopedKeeperPath || !slices.Equal(got.PurgeMembers, []string{"member-0", "member-1"}) {
		t.Fatalf("view of dbp.v = %+v", got)
	}
	if old := view.Live("db0.x"); old == nil || old.ScopedKeeperPath || old.PurgeMembers != nil {
		t.Fatalf("view of db0.x = %+v, want unscoped without purge_members", old)
	}
	got.PurgeMembers[0] = "mutated"
	if inc.PurgeMembers[0] != "member-0" {
		t.Fatal("the view shares purge_members with the replicated state")
	}
}

// TestRestoreRefusesAScopedIncarnationBeforeThePurgeCutover: a scoped
// incarnation is exactly a chain incarnation recorded at or after the purge
// cutover. One without the switch, one recorded before the cutover, and a
// genesis one under a cutover forged down to it are each refused by both
// restores; the honest container round-trips.
func TestRestoreRefusesAScopedIncarnationBeforeThePurgeCutover(t *testing.T) {
	t.Run("without the purge switch", func(t *testing.T) {
		f, _ := stage3FSM(t)
		purgeTestRoundTrip(t, f, "stage 3 with every switch off")
		inc := f.st.TableRegistry.Live("db0.x")
		inc.ScopedKeeperPath = true
		restoreRefusedBothWays(t, f, snapshotBytes(t, f), fmt.Sprintf("incarnation %d carries scoped_keeper_path without the purge switch", inc.Seq))
	})
	t.Run("recorded before the cutover", func(t *testing.T) {
		f, a := stage3FSM(t)
		turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
		purgeTestAdd(t, f, "dbp", "after", 160, 1)
		purgeTestRoundTrip(t, f, "one scoped incarnation")
		cutover := f.st.Stage3.Switches[0].IncarnationSeq
		inc := f.st.TableRegistry.Live("db0.x")
		inc.ScopedKeeperPath = true
		restoreRefusedBothWays(t, f, snapshotBytes(t, f), fmt.Sprintf("incarnation %d carries scoped_keeper_path but precedes the purge cutover (incarnation %d)", inc.Seq, cutover))
	})
	t.Run("a genesis incarnation at a forged cutover", func(t *testing.T) {
		f, a := stage3FSM(t)
		turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
		genesis := f.st.TableRegistry.Incarnations[0]
		if genesis.Origin != TableOriginGenesis {
			t.Fatalf("fixture: incarnation 1 is %s", genesis.Origin)
		}
		f.st.Stage3.Switches[0].IncarnationSeq = genesis.Seq
		for _, inc := range f.st.TableRegistry.Incarnations {
			inc.ScopedKeeperPath = inc.Origin == TableOriginChain
		}
		genesis.ScopedKeeperPath = true
		restoreRefusedBothWays(t, f, snapshotBytes(t, f), fmt.Sprintf("incarnation %d is not a chain incarnation but carries scoped_keeper_path", genesis.Seq))
	})
}

// TestRestoreRefusesAnUnscopedIncarnationAfterThePurgeCutover: every chain
// incarnation at or after the purge cutover must be scoped, whether the flag
// was dropped or the cutover forged below an unscoped incarnation.
func TestRestoreRefusesAnUnscopedIncarnationAfterThePurgeCutover(t *testing.T) {
	t.Run("the flag dropped", func(t *testing.T) {
		f, a := stage3FSM(t)
		turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
		inc := purgeTestAdd(t, f, "dbp", "after", 160, 1)
		purgeTestRoundTrip(t, f, "one scoped incarnation")
		cutover := f.st.Stage3.Switches[0].IncarnationSeq
		inc.ScopedKeeperPath = false
		restoreRefusedBothWays(t, f, snapshotBytes(t, f), fmt.Sprintf("incarnation %d is recorded after the purge cutover (incarnation %d) without scoped_keeper_path", inc.Seq, cutover))
	})
	t.Run("the cutover forged below an unscoped incarnation", func(t *testing.T) {
		f, a := stage3FSM(t)
		turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
		inc := f.st.TableRegistry.Live("db0.x")
		f.st.Stage3.Switches[0].IncarnationSeq = inc.Seq
		restoreRefusedBothWays(t, f, snapshotBytes(t, f), fmt.Sprintf("incarnation %d is recorded after the purge cutover (incarnation %d) without scoped_keeper_path", inc.Seq, inc.Seq))
	})
}

// TestRestoreRefusesMalformedPurgeMembers: purge_members is sorted, unique
// and names no empty id, and before the placement switch it is absent (Plan
// S3-E3 Task 71 fills it from placement on and replaces the second rule).
func TestRestoreRefusesMalformedPurgeMembers(t *testing.T) {
	f, a := stage3FSM(t)
	turnOnStage3Switch(t, f, a, Stage3SwitchPurge)
	inc := purgeTestAdd(t, f, "dbp", "m", 160, 1)
	purgeTestRoundTrip(t, f, "no purge_members")
	for _, tc := range []struct {
		name    string
		members []string
		want    string
	}{
		{"unsorted", []string{"v2", "v1"}, "purge_members must be sorted, unique, non-empty node ids"},
		{"repeated", []string{"v1", "v1"}, "purge_members must be sorted, unique, non-empty node ids"},
		{"an empty id", []string{"", "v1"}, "purge_members must be sorted, unique, non-empty node ids"},
		{"well formed before the placement switch", []string{"v1"}, "carries purge_members before the placement switch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inc.PurgeMembers = tc.members
			restoreRefusedBothWays(t, f, snapshotBytes(t, f), fmt.Sprintf("incarnation %d %s", inc.Seq, tc.want))
		})
	}
}

// TestRestoreRefusesScopedKeeperPathAndPurgeMembersInAPreV21Container: no
// pre-v21 writer records either field and a pre-v21 binary would drop both,
// so either one under an older version byte is a forged field.
func TestRestoreRefusesScopedKeeperPathAndPurgeMembersInAPreV21Container(t *testing.T) {
	for _, tc := range []struct {
		name  string
		forge func(*TableIncarnation)
	}{
		{"scoped_keeper_path", func(inc *TableIncarnation) { inc.ScopedKeeperPath = true }},
		{"purge_members", func(inc *TableIncarnation) { inc.PurgeMembers = []string{"v1"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := tracksFSM(t)
			tc.forge(f.st.TableRegistry.Live("db0.x"))
			data := snapshotBytes(t, f)
			if data[4] != snapshotVersionV20 {
				t.Fatalf("version byte = %d, want 20", data[4])
			}
			restoreRefusedBothWays(t, f, data, "stage3 state is present in a pre-v21 container")
		})
	}
}
```

- [ ] **Step 3: Release the two pending proto fields**

`TestTableRegistryMirrorsProto` skips the proto fields `stage3PendingProtoFields` names; this task maps both `TableIncarnation` fields, so it deletes their entries first (contract §44), and the test then pins the new JSON tags.

In `server/safestate_stage3_test.go`, replace the end of `stage3PendingProtoFields`:

```go
	"BlockVerification.reselections_at_latch": "Plan S3-E3 Task 74",
	"TableIncarnation.scoped_keeper_path":     "Task 69",
	"TableIncarnation.purge_members":          "Task 69",
}
```

with:

```go
	"BlockVerification.reselections_at_latch": "Plan S3-E3 Task 74",
}
```

Every other entry of the map stays as it is (the remaining keys keep their alignment, so `gofmt` changes nothing).

- [ ] **Step 4: Run the tests and see them fail**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && GOWORK=off go test ./fsm/ -run '^(TestPurgeSwitchOffKeepsEveryIncarnationUnscoped|TestPurgeSwitchMarksEveryLaterChainIncarnationScoped|TestPurgeRequiredSetBeforeThePurgeSwitchIsEveryNonEvictedVerifier|TestPurgeSwitchOnWithNoMembersChangesNoPurge|TestPurgeSwitchOnHookCompletesAPurgeThatWaitedOnlyForAMember|TestPurgeRequiredSetIsTheOwnerThePoolAndPurgeMembersOnceThePurgeSwitchIsOn|TestEvictNodeOfAPurgeMemberCompletesThePurge|TestPurgeNodeSetIsUnchangedByThePurgeSwitch|TestRegistryViewServesScopedKeeperPathAndPurgeMembers|TestRestoreRefusesAScopedIncarnationBeforeThePurgeCutover|TestRestoreRefusesAnUnscopedIncarnationAfterThePurgeCutover|TestRestoreRefusesMalformedPurgeMembers|TestRestoreRefusesScopedKeeperPathAndPurgeMembersInAPreV21Container)$' -race -count=1
cd "$W" && GOWORK=off go test ./server/ -run '^TestTableRegistryMirrorsProto$' -race -count=1
```

Expected: the fsm test binary does not build, `FAIL	github.com/sentioxyz/arbiter/fsm [build failed]`, with errors such as `fsm/purge_stage3_test.go:187:12: inc.ScopedKeeperPath undefined (type *TableIncarnation has no field or method ScopedKeeperPath)` and `fsm/purge_stage3_test.go:187:36: inc.PurgeMembers undefined (type *TableIncarnation has no field or method PurgeMembers)`; the server test FAILS with `TableIncarnation: proto field "scoped_keeper_path" has no json tag` and `TableIncarnation: proto field "purge_members" has no json tag`.

- [ ] **Step 5: Add the two incarnation fields and their view copy**

In `fsm/table_registry.go`, replace the end of `TableIncarnation` (`:61-62`):

```go
	OwnerIndexerID *uint64 `json:"owner_indexer_id,omitempty"`
}
```

with:

```go
	OwnerIndexerID *uint64 `json:"owner_indexer_id,omitempty"`
	// ScopedKeeperPath marks a chain incarnation recorded while the stage3
	// purge switch is on (housegate spec 2026-10-14 §8.8): its hg_unsafe and
	// hg_member_unsafe replicas use the incarnation-scoped Keeper path
	// /sentio/<shard>/unsafe_inc/<db__table>/<seq>, which every reconciler
	// derives from this flag in the registry view. applyAddTable sets it and
	// nothing ever clears it; false, and omitted, on every other
	// incarnation, which keeps the unscoped path.
	ScopedKeeperPath bool `json:"scoped_keeper_path,omitempty"`
	// PurgeMembers lists, sorted and unique, every node that has been a
	// member of the incarnation's database group at any generation since the
	// incarnation was recorded (spec §8.8). With the purge switch on, the
	// purge waits for each of them that is not evicted. Plan S3-E3 Task 71
	// fills it from the placement switch on; nil before.
	PurgeMembers []string `json:"purge_members,omitempty"`
}
```

In `fsm/table_registry.go`, inside `(*FSM).TableRegistryView` (`:317-349`), replace (`:328`):

```go
		c.PurgedBy = slices.Clone(inc.PurgedBy)
```

with:

```go
		c.PurgedBy = slices.Clone(inc.PurgedBy)
		c.PurgeMembers = slices.Clone(inc.PurgeMembers)
```

- [ ] **Step 6: Run the tests again**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && GOWORK=off go test ./fsm/ -run '^(TestPurgeSwitchOffKeepsEveryIncarnationUnscoped|TestPurgeSwitchMarksEveryLaterChainIncarnationScoped|TestPurgeRequiredSetBeforeThePurgeSwitchIsEveryNonEvictedVerifier|TestPurgeSwitchOnWithNoMembersChangesNoPurge|TestPurgeSwitchOnHookCompletesAPurgeThatWaitedOnlyForAMember|TestPurgeRequiredSetIsTheOwnerThePoolAndPurgeMembersOnceThePurgeSwitchIsOn|TestEvictNodeOfAPurgeMemberCompletesThePurge|TestPurgeNodeSetIsUnchangedByThePurgeSwitch|TestRegistryViewServesScopedKeeperPathAndPurgeMembers|TestRestoreRefusesAScopedIncarnationBeforeThePurgeCutover|TestRestoreRefusesAnUnscopedIncarnationAfterThePurgeCutover|TestRestoreRefusesMalformedPurgeMembers|TestRestoreRefusesScopedKeeperPathAndPurgeMembersInAPreV21Container)$' -race -count=1 -v 2>&1 | grep -E '^(--- |    purge_stage3_test|        purge_stage3_test|ok|FAIL)'
cd "$W" && GOWORK=off go test ./server/ -run '^TestTableRegistryMirrorsProto$' -race -count=1
```

Expected: the server test passes (`ok  	github.com/sentioxyz/arbiter/server`). The fsm package builds; `TestPurgeSwitchOffKeepsEveryIncarnationUnscoped`, `TestPurgeRequiredSetBeforeThePurgeSwitchIsEveryNonEvictedVerifier`, `TestPurgeSwitchOnWithNoMembersChangesNoPurge` and `TestPurgeNodeSetIsUnchangedByThePurgeSwitch` PASS (they pin today's behaviour, and the members register through the signed paths), and the other nine FAIL: `incarnation 5 (dbp.after, chain, purging) scoped_keeper_path = false, want true`; `after the switch-on hook the purge = purging, registry version 15 -> 15`; `fixture: dbp.u purging scoped false, dbp.s scoped false, dbp.o scoped false`; `after evicting member-0 the purge = purging, registry version 18 -> 18`; `view of dbp.v = &{Seq:4 … ScopedKeeperPath:false PurgeMembers:[member-0 member-1]}`; `bare restore = <nil>, want "incarnation 3 carries scoped_keeper_path without the purge switch"` (and its two sibling subtests); `bare restore = <nil>, want "incarnation 4 is recorded after the purge cutover (incarnation 4) without scoped_keeper_path"` (and `incarnation 3 … (incarnation 3)`); `bare restore = <nil>, want "incarnation 4 purge_members must be sorted, unique, non-empty node ids"` (three subtests) and `… "incarnation 4 carries purge_members before the placement switch"`; `bare restore = <nil>, want "stage3 state is present in a pre-v21 container"` (two subtests); then `FAIL	github.com/sentioxyz/arbiter/fsm`. The registry version numbers are the fixture's and differ if Tasks 64–68 changed its history; within each pair the two numbers are equal.

- [ ] **Step 7: Record scoped incarnations in AddTable**

In `fsm/apply_table_registry.go`, inside `applyAddTable`, replace the end of the refusal block and the append (`:266-271`):

```go
		inc.RefusedReason = refusal.Detail
	}
	reg.appendIncarnation(inc)
```

with:

```go
		inc.RefusedReason = refusal.Detail
	}
	// From the stage3 purge switch on, every incarnation AddTable records,
	// a Refused one included, is on the incarnation-scoped Keeper path
	// (housegate spec 2026-10-14 §8.8); the switch's cutover records the
	// first such seq, and restore requires exactly this
	// (validateStage3Incarnations).
	if f.stage3SwitchOnLocked(Stage3SwitchPurge) {
		inc.ScopedKeeperPath = true
	}
	reg.appendIncarnation(inc)
```

- [ ] **Step 8: Make the purge-required set member-aware**

In `fsm/apply_table_registry.go`, replace `purgeRequiredLocked` and its comment (`:469-489`):

```go
// purgeRequiredLocked is the set of nodes whose reports complete the purge of
// inc after the signed-claims activation (spec 2026-10-10 §6.6, contract
// §3b.5), sorted: every non-evicted verifier, plus the owner's SNode when
// that node is registered and not evicted. Only those nodes hold the table.
// GetPurgeNodeSet is unaffected: it stays every registered, non-evicted
// SNode and verifier.
func (f *FSM) purgeRequiredLocked(inc *TableIncarnation) []string {
	owner, ownerOK := f.incarnationOwnerSNodeLocked(inc)
	var required []string
	for id, node := range f.st.Nodes {
		if node == nil || node.Status == NodeEvicted {
			continue
		}
		roles := node.Registration.Roles
		if hasRole(roles, arbiter.NodeRoleVerifier) || (ownerOK && id == owner && hasRole(roles, arbiter.NodeRoleSNode)) {
			required = append(required, id)
		}
	}
	slices.Sort(required)
	return required
}
```

with:

```go
// purgeRequiredLocked is the set of nodes whose reports complete the purge of
// inc after the signed-claims activation, sorted; only those nodes hold the
// table. It holds the owner's SNode when that node is registered and not
// evicted, and, until the stage3 purge switch (spec 2026-10-10 §6.6, contract
// §3b.5), every non-evicted verifier. From the switch on (housegate spec
// 2026-10-14 §8.8, plan S3-E2 contract §47), on the scoped and the unscoped
// Keeper path alike, the verifiers are the network pool (poolVerifierLocked)
// and the non-evicted verifiers inc.PurgeMembers names: a member is a
// VERIFIER-role node, the only role besides the owner's SNode whose purge
// report Apply accepts, and it leaves the set only through EvictNode.
// GetPurgeNodeSet is unaffected: it stays every registered, non-evicted
// SNode and verifier.
func (f *FSM) purgeRequiredLocked(inc *TableIncarnation) []string {
	owner, ownerOK := f.incarnationOwnerSNodeLocked(inc)
	memberAware := f.stage3SwitchOnLocked(Stage3SwitchPurge)
	var required []string
	for id, node := range f.st.Nodes {
		if node == nil || node.Status == NodeEvicted {
			continue
		}
		roles := node.Registration.Roles
		if ownerOK && id == owner && hasRole(roles, arbiter.NodeRoleSNode) {
			required = append(required, id)
			continue
		}
		if hasRole(roles, arbiter.NodeRoleVerifier) && (!memberAware || f.poolVerifierLocked(id) || slices.Contains(inc.PurgeMembers, id)) {
			required = append(required, id)
		}
	}
	slices.Sort(required)
	return required
}
```

- [ ] **Step 9: Rerun purge completion at the switch-on**

In `fsm/stage3.go`, inside Task 64's `onStage3SwitchOnLocked`, replace the opening of its switch:

```go
func (f *FSM) onStage3SwitchOnLocked(name string) {
	switch name {
```

with (the new case comes first, in spec §10.4 order; Task 68's `case Stage3SwitchEvictionVoiding:` and the `default:` follow it unchanged):

```go
func (f *FSM) onStage3SwitchOnLocked(name string) {
	switch name {
	case Stage3SwitchPurge:
		// The purge-required set now counts only the owner's SNode, the
		// network pool and purge_members (housegate spec 2026-10-14 §8.8),
		// so a purge that waited only for another verifier completes here,
		// as in applyEvictNode. No host-bound entry can exist before the
		// owner_aware_selection switch (K68), so in a committed history this
		// completes nothing; the rerun keeps completion a function of
		// committed state alone.
		if reg := f.st.TableRegistry; reg != nil {
			before := purgedCount(reg)
			f.completePurgesLocked()
			if purgedCount(reg) != before {
				f.registryChangedLocked(reg)
			}
		}
```

- [ ] **Step 10: Refuse both fields in a pre-v21 container**

In `fsm/snapshot.go`, inside Task 66's `stage3FieldsPresent`, replace its tail (the consensus-update loop and the final `return false`):

```go
	for _, change := range doc.ConsensusUpdates {
		if change.Update.Stage3 != nil || slices.ContainsFunc(change.Update.Verifiers, verifierEntryHasStage3Fields) {
			return true
		}
	}
	return false
}
```

with:

```go
	for _, change := range doc.ConsensusUpdates {
		if change.Update.Stage3 != nil || slices.ContainsFunc(change.Update.Verifiers, verifierEntryHasStage3Fields) {
			return true
		}
	}
	// The purge switch's incarnation fields (Task 69).
	if reg := doc.TableRegistry; reg != nil {
		for _, inc := range reg.Incarnations {
			if inc != nil && (inc.ScopedKeeperPath || len(inc.PurgeMembers) != 0) {
				return true
			}
		}
	}
	return false
}
```

The new lines belong in `stage3FieldsPresent`, never in `tracksFieldsPresent` just above it, which also ends with `return false`; `TestRestoreRefusesScopedKeeperPathAndPurgeMembersInAPreV21Container` fails if they land there.

- [ ] **Step 11: Add the restore rule**

In `fsm/stage3_restore.go`, inside Task 66's `validateStage3State`, replace the activation check:

```go
	if err := validateStage3Activation(st); err != nil {
		return err
	}
```

with (Task 70 adds its `validateStage3Selection` call after this one):

```go
	if err := validateStage3Activation(st); err != nil {
		return err
	}
	if err := validateStage3Incarnations(st); err != nil {
		return err
	}
```

Append to the end of `fsm/stage3_restore.go` (it imports `fmt` already):

```go
// validateStage3Incarnations is the restore rule of the purge switch's
// incarnation fields (housegate spec 2026-10-14 §8.8, plan S3-E2 contract
// §47). applyAddTable sets scoped_keeper_path on exactly the chain
// incarnations it records from the purge switch on, whose first seq the
// switch's cutover records (validateStage3Activation, which runs first,
// requires that cutover and bounds its seq), and nothing else sets or clears
// it. purge_members is sorted, unique and free of empty ids; before the
// placement switch nothing records it (Plan S3-E3 Task 71 replaces this rule
// with its subset rule). Incarnations are visited in seq order, so every
// replica names the same corruption.
func validateStage3Incarnations(st *State) error {
	reg := st.TableRegistry
	if reg == nil {
		return nil
	}
	purge := stage3SwitchValue(st.Params.Stage3, Stage3SwitchPurge)
	var cutover uint64
	if purge && st.Stage3 != nil {
		for _, c := range st.Stage3.Switches {
			if c.Switch == Stage3SwitchPurge {
				cutover = c.IncarnationSeq
			}
		}
	}
	placement := stage3SwitchValue(st.Params.Stage3, Stage3SwitchPlacement)
	for _, inc := range reg.Incarnations {
		if inc == nil {
			continue
		}
		chain := inc.Origin == TableOriginChain
		switch {
		case inc.ScopedKeeperPath && !purge:
			return fmt.Errorf("incarnation %d carries scoped_keeper_path without the purge switch", inc.Seq)
		case purge && chain && inc.Seq >= cutover && !inc.ScopedKeeperPath:
			return fmt.Errorf("incarnation %d is recorded after the purge cutover (incarnation %d) without scoped_keeper_path", inc.Seq, cutover)
		case inc.ScopedKeeperPath && inc.Seq < cutover:
			return fmt.Errorf("incarnation %d carries scoped_keeper_path but precedes the purge cutover (incarnation %d)", inc.Seq, cutover)
		case inc.ScopedKeeperPath && !chain:
			return fmt.Errorf("incarnation %d is not a chain incarnation but carries scoped_keeper_path", inc.Seq)
		}
		if len(inc.PurgeMembers) == 0 {
			continue
		}
		for i, id := range inc.PurgeMembers {
			if id == "" || (i > 0 && inc.PurgeMembers[i-1] >= id) {
				return fmt.Errorf("incarnation %d purge_members must be sorted, unique, non-empty node ids", inc.Seq)
			}
		}
		if !placement {
			return fmt.Errorf("incarnation %d carries purge_members before the placement switch", inc.Seq)
		}
	}
	return nil
}
```

- [ ] **Step 12: Run the FSM tests and see them pass**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && GOWORK=off go test ./fsm/ -run '^(TestPurgeSwitchOffKeepsEveryIncarnationUnscoped|TestPurgeSwitchMarksEveryLaterChainIncarnationScoped|TestPurgeRequiredSetBeforeThePurgeSwitchIsEveryNonEvictedVerifier|TestPurgeSwitchOnWithNoMembersChangesNoPurge|TestPurgeSwitchOnHookCompletesAPurgeThatWaitedOnlyForAMember|TestPurgeRequiredSetIsTheOwnerThePoolAndPurgeMembersOnceThePurgeSwitchIsOn|TestEvictNodeOfAPurgeMemberCompletesThePurge|TestPurgeNodeSetIsUnchangedByThePurgeSwitch|TestRegistryViewServesScopedKeeperPathAndPurgeMembers|TestRestoreRefusesAScopedIncarnationBeforeThePurgeCutover|TestRestoreRefusesAnUnscopedIncarnationAfterThePurgeCutover|TestRestoreRefusesMalformedPurgeMembers|TestRestoreRefusesScopedKeeperPathAndPurgeMembersInAPreV21Container)$' -race -count=1 -v 2>&1 | grep -E '^(--- |ok|FAIL)'
cd "$W" && GOWORK=off go test ./fsm/ ./server/ -count=1 && GOWORK=off go vet ./fsm/ ./server/ && echo vet-clean
```

Expected: thirteen `--- PASS` lines, one per named test, and `ok  	github.com/sentioxyz/arbiter/fsm`; then `ok` for both whole packages (the frozen goldens, Tasks 64–68's stage-3 tests and `TestTableRegistryMirrorsProto` included) and `vet-clean`.

- [ ] **Step 13: Run gazelle**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && test -z "$(gofmt -l fsm server)" && echo gofmt-clean
cd "$W" && bazel run //:gazelle && git status --short && git diff -- fsm/BUILD.bazel
```

Expected: `gofmt-clean` (`fsm` and `server` hold every Go file this task touches, the untracked test file included); the status lists exactly ` M fsm/BUILD.bazel`, ` M fsm/apply_table_registry.go`, ` M fsm/snapshot.go`, ` M fsm/stage3.go`, ` M fsm/stage3_restore.go`, ` M fsm/table_registry.go`, ` M server/safestate_stage3_test.go` and `?? fsm/purge_stage3_test.go`; the `fsm/BUILD.bazel` diff adds only `"purge_stage3_test.go",` to the `fsm_test` srcs, right after `"purge_owner_test.go",` (every import of the file is an `fsm_test` dependency already).

- [ ] **Step 14: Commit the FSM half**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" add fsm/table_registry.go fsm/apply_table_registry.go fsm/stage3.go fsm/snapshot.go fsm/stage3_restore.go fsm/purge_stage3_test.go fsm/BUILD.bazel server/safestate_stage3_test.go
git -C "$W" status --short
git -C "$W" commit -m "feat(stage3): purge switch: scoped incarnations and member-aware purge sets (Task 69)

From the stage3 purge switch (housegate spec 2026-10-14 §8.8, §10.4
step 1) applyAddTable records every incarnation, Refused ones included,
with scoped_keeper_path, and a purge waits for the owner's SNode, the
network pool and the non-evicted verifiers purge_members names, on the
scoped and the unscoped path alike; the switch-on reruns purge
completion. Restore refuses either field under a pre-v21 version byte
and checks scoped_keeper_path against the purge cutover and the form of
purge_members (validateStage3Incarnations). With the switch off nothing
changes, and GetPurgeNodeSet never does. TestTableRegistryMirrorsProto
now pins both JSON tags.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: the status lists the eight staged files only (`M ` for seven, `A  fsm/purge_stage3_test.go`); one commit.

- [ ] **Step 15: Write the failing server test**

In `server/table_registry_test.go`, replace the opening line of `TestGetTableRegistry` (`:51` at `526a721`):

```go
func TestGetTableRegistry(t *testing.T) {
```

with (the file imports `reflect` and `fsm` already):

```go
// TestTableRegistryToPBCarriesScopedKeeperPathAndPurgeMembers: the transport
// form carries both purge-switch fields (TableIncarnation fields 19 and 20),
// so every reconciler derives the same Keeper path and purge set (housegate
// spec 2026-10-14 §8.8); an incarnation without them sends neither.
func TestTableRegistryToPBCarriesScopedKeeperPathAndPurgeMembers(t *testing.T) {
	owner := uint64(1)
	view := fsm.TableRegistryView{Version: 7, Seeded: true, Incarnations: []fsm.TableIncarnation{
		{Seq: 1, DatabaseID: "db", TableID: "old", Origin: fsm.TableOriginChain, Status: fsm.TableStatusActive, OwnerIndexerID: &owner},
		{Seq: 2, DatabaseID: "db", TableID: "new", Origin: fsm.TableOriginChain, Status: fsm.TableStatusPurging, OwnerIndexerID: &owner,
			ScopedKeeperPath: true, PurgeMembers: []string{"member-0", "member-2"}},
	}}
	got := tableRegistryToPB(view).GetIncarnations()
	if len(got) != 2 {
		t.Fatalf("incarnations = %v", got)
	}
	if got[0].GetScopedKeeperPath() || got[0].GetPurgeMembers() != nil {
		t.Fatalf("an unscoped incarnation sent scoped_keeper_path %v and purge_members %v", got[0].GetScopedKeeperPath(), got[0].GetPurgeMembers())
	}
	if !got[1].GetScopedKeeperPath() || !reflect.DeepEqual(got[1].GetPurgeMembers(), []string{"member-0", "member-2"}) {
		t.Fatalf("a scoped incarnation sent scoped_keeper_path %v and purge_members %v", got[1].GetScopedKeeperPath(), got[1].GetPurgeMembers())
	}
}

func TestGetTableRegistry(t *testing.T) {
```

- [ ] **Step 16: Run it and see it fail**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && GOWORK=off go test ./server/ -run '^TestTableRegistryToPBCarriesScopedKeeperPathAndPurgeMembers$' -race -count=1
```

Expected: FAIL with `a scoped incarnation sent scoped_keeper_path false and purge_members []`.

- [ ] **Step 17: Serve both fields**

In `server/table_registry.go`, inside `tableRegistryToPB`'s `pb.TableIncarnation` literal, replace its last field and the literal's end (`:126-127`):

```go
			OwnerIndexerId: inc.OwnerIndexerID,
		})
```

with:

```go
			OwnerIndexerId: inc.OwnerIndexerID,
			// The purge switch's fields (housegate spec 2026-10-14 §8.8): every
			// reconciler derives the incarnation's Keeper path from
			// scoped_keeper_path, and purge_members names the members its
			// purge waits for.
			ScopedKeeperPath: inc.ScopedKeeperPath, PurgeMembers: inc.PurgeMembers,
		})
```

The view is already a detached copy (`TableRegistryView`), so the slice is passed as `PurgedBy` is.

- [ ] **Step 18: Run the server tests and see them pass**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && GOWORK=off go test ./server/ -run '^(TestTableRegistryToPBCarriesScopedKeeperPathAndPurgeMembers|TestTableRegistryMirrorsProto|TestGetTableRegistry|TestWatchTableRegistryCoalescesAndClosesWithNotLeader|TestTableRegistryEnumMappingsAreComplete)$' -race -count=1
cd "$W" && GOWORK=off go test ./server/ -count=1
```

Expected: `ok  	github.com/sentioxyz/arbiter/server` twice.

- [ ] **Step 19: Document the purge switch**

In `README.md`, inside the section `## Multi-source storage integrity (stage-3 activation)` (Tasks 65 and 66's), add a last paragraph: replace the heading that follows the section:

```markdown
## Artifact disposition admission
```

with:

```markdown
**Purge switch** (housegate spec 2026-10-14 §8.8, §10.4 step 1). From the `purge` switch on, every incarnation `AddTable` records carries `scoped_keeper_path`, a Refused one too: the `hg_unsafe` and `hg_member_unsafe` replicas of a scoped incarnation use the incarnation-scoped Keeper path `/sentio/<shard>/unsafe_inc/<db__table>/<incarnation_seq>`, and `GetTableRegistry` and `WatchTableRegistry` serve the flag, so every reconciler derives the same path; an incarnation recorded earlier keeps the unscoped path for its life. A purge, on either path, then waits for the owner's SNode, every non-evicted network-pool verifier (a listed verifier without `host_indexer_id`) and every registered, non-evicted verifier that the incarnation's `purge_members` names (served too, and filled from the `placement` switch on), so a dead member holds a purge until it is evicted; the switch-on reruns purge completion, and `EvictNode` reruns it as before. `GetPurgeNodeSet` does not change: it stays every registered, non-evicted SNode and verifier. Restore refuses `scoped_keeper_path` on an incarnation that is not a chain incarnation recorded at or after the purge cutover, a chain incarnation recorded at or after the cutover without it, and a `purge_members` list that is unsorted, repeats a node, names an empty id or appears before the `placement` switch.

## Artifact disposition admission
```

so the new paragraph follows the section's last paragraph after one blank line, as the section's other paragraphs do.

- [ ] **Step 20: Full verification** (contract §48)

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
cd "$W" && test -z "$(git ls-files -z '*.go' | xargs -0 gofmt -l)" && echo gofmt-clean
cd "$W" && GOWORK=off go vet ./... && echo vet-clean
cd "$W" && GOWORK=off go test ./... -race -count=1 2>&1 | grep -E '^(--- FAIL|FAIL|panic)' || echo no-failures
cd "$W" && bazel run //:gazelle && git status --short
cd "$W" && bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors
cd "$W" && git diff --exit-code HEAD~1 -- fsm/testdata accumulator/testdata fsm/l3_track_golden_test.go && echo goldens-unchanged
cd "$W" && bazel query 'deps(//fsm:fsm, 1)' | grep com_github_sentioxyz_arbiter_proto; grep -rn 'time\.Now' fsm/; echo red-lines-checked
```

Expected: `gofmt-clean` (contract §48's check over every tracked Go file, NUL-separated); `vet-clean`; `no-failures`; gazelle changes nothing and the status lists exactly ` M README.md`, ` M server/table_registry.go` and ` M server/table_registry_test.go`; Bazel builds and every test target passes under race (CI's command, `.github/workflows/ci.yml`); `goldens-unchanged` (no golden or vector file differs from Task 68's last commit, `HEAD~1`); the red-line commands print only `red-lines-checked` (the fsm library imports no arbiter-proto and reads no wall clock).

- [ ] **Step 21: Commit the transport half and the README**

```bash
W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e
git -C "$W" add README.md server/table_registry.go server/table_registry_test.go
git -C "$W" status --short
git -C "$W" commit -m "feat(stage3): serve scoped_keeper_path and purge_members in the table registry (Task 69)

tableRegistryToPB sets TableIncarnation fields 19 and 20 (plan S3-E1
contract §39), so GetTableRegistry and WatchTableRegistry give every
reconciler the Keeper path kind of each incarnation and the members its
purge waits for (housegate spec 2026-10-14 §8.8). The README's stage-3
section describes the purge switch.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C "$W" status --porcelain
git -C "$W" log --oneline -3
```

Expected: the status before the commit lists the three staged files only; after it the status is empty and the log shows this task's two commits on top of Task 68's last commit. Nothing is pushed: Task 70's last step pushes the branch (contract §48), and no release, tag, PR or merge happens in this plan (K65). Task 70 starts from here.

---

## Task 70: arbiter — owner-aware selection (switch 3): the pool's eligible ring, `VERIFIERS_UNAVAILABLE`, unmarkable blocks, the branch push (S3-E2)

Spec §7.3 (the eligible ring, the stored ring, position and registry cursor block, `VERIFIERS_UNAVAILABLE` computed with `MarkReplaying`'s filter, a block that cannot find three verifiers leaving the WorkSet), §10.4 step 3 (`owner_aware_selection`: a database without a group gets a pool-only ring, and entries hosted by the block's owner are excluded) and §17 (FSM: `VERIFIERS_UNAVAILABLE`, selection excluding owner-hosted verifiers), as fixed by contract §47 and clarification K68. With the switch on, `MarkReplaying` draws the verifier set from `eligibleRingLocked(owner, sources)`, which in this plan is the Active network pool minus the block's sources (no database has a group before the placement switch, and K68 keeps every host-bound entry, the owner's own included, out of every ring), at today's block-seeded start, and stores the ring, the last ring position used and the committed registry cursor block in `BlockVerification`; admission answers `VERIFIERS_UNAVAILABLE` right after `SOURCE_UNAVAILABLE` when that ring holds fewer than three verifiers; the WorkSet moves such a sealed, claim-complete block from `SealedUnmarked` to the new `Unmarkable`, so the unchanged orchestrator proposes nothing for it; restore checks every stored record (`validateStage3Selection`) and refuses one under a pre-v21 version byte; `GetBlockVerification` serves the three fields. With the switch off every path is byte for byte today's (`TestOwnerAwareSelectionOffSelectsByteIdentically`). The task ends with the push of the P5 branch, the only remote action of this plan.

Worktree: Task 64's (`W=$HOME/src/remotesrc/claude/arbiter/multi-source-s3e`, branch `urwt/claude/multi-source-s3e`, contract §48), with `HEAD` at Task 69's last commit; every command below starts with `W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e";` in its own Bash call. Line anchors are arbiter `526a721` (`v0.13.0`, Plan S3-A's release, which the branch base carries; for `fsm/state.go`, `fsm/select.go`, `fsm/apply.go`, `fsm/admission.go` and `orchestrator/dispatch_test.go` they are also the header's `fb42260` lines). `fsm/stage3_restore.go` and `stage3FieldsPresent` (in `fsm/snapshot.go`) are Task 66's, extended by Task 69, so this task inserts into them by position (before their final `return`), not by line; Task 68's hunks in `fsm/apply.go` (the signers' call sites) and `fsm/reads_work.go` (`evidencePresence` and its one call) touch none of the lines quoted here. Every hunk is applied on top of Tasks 64–69 under the hunk rule (Global Constraints).

**Files:**
- Modify: `fsm/state.go` (`BlockVerification` `:269-279`: three fields after `LastMergeable` `:278`)
- Modify: `fsm/select.go` (`poolRingLocked`, `eligibleRingLocked`, `blockRingLocked`, `ringStart`, `blockSeededPosition`, `initialVerifierSet`, `verifiersUnavailableLocked` inserted between `selectVerifiers` `:55-76` and `ownerSourceLocked` `:78-94`; `selectVerifiers` itself and the imports are unchanged)
- Modify: `fsm/apply.go` (`applyMarkReplaying` `:425-450`: the selection `:439-443`)
- Modify: `fsm/admission.go` (after the owner-source check `:133-147`, before the lane budget comment `:148`)
- Modify: `fsm/reads_work_types.go` (`WorkSet` `:72-94`: `Unmarkable` after `Tracks` `:93`)
- Modify: `fsm/reads_work.go` (`WorkSet()`: the block loop head `:109-111`; the unmarked branch `:132-136`)
- Modify: `fsm/reads_verification.go` (`BlockVerificationView` `:48-58`; `(*FSM).BlockVerificationView` `:99`)
- Modify: `fsm/stage3_restore.go` (Task 66's file, extended by Task 69: `validateStage3State` gains the call; `verificationHasStage3Fields`, `validateStage3Selection`, `ownerAwareSelectionCutover`, `ringIsSortedAndUnique` appended) and `fsm/snapshot.go` (Task 66's `stage3FieldsPresent`, extended by Task 69: one more check before its final `return false`)
- Create: `fsm/selection_stage3_test.go`
- Modify (gazelle): `fsm/BUILD.bazel` (the test file in `fsm_test`)
- Modify: `orchestrator/dispatch_test.go` (imports `:3-12`; one test appended after `:137`)
- Modify: `server/safestate_stage3.go` (`blockVerificationToPB` `:133-157`: the end of its literal `:144-146`)
- Modify: `server/safestate_stage3_test.go` (`stage3BlockVerificationView` `:94-97`; `TestBlockVerificationToPBCarriesEveryField` `:157-158`; Task 64's `stage3PendingProtoFields` loses its three `BlockVerification` entries owned by this task)
- Remote: `git push -u origin HEAD` of branch `urwt/claude/multi-source-s3e` (Step 23, explicit confirmation)

**Interfaces:**
- Consumes:
  - Task 64 (contract §45): `Stage3SwitchEvictionVoiding`, `Stage3SwitchOwnerAwareSelection`; `func stage3SwitchValue(p *arbiter.Stage3Params, name string) bool`; `func (f *FSM) stage3SwitchOnLocked(name string) bool`; `func (f *FSM) poolVerifierLocked(nodeID string) bool` (a registered, non-evicted VERIFIER whose listed entry has no `host_indexer_id`); `func (f *FSM) verifierEntryLocked(nodeID string) (arbiter.VerifierEntry, bool)`; `State.Stage3 *Stage3State` with `Switches []Stage3SwitchCutover` (`Switch`, `BlockSeq`: `len(Blocks)` at the switch-on Apply); the fixture `stage3FSM(t) (*FSM, *authority.Signer)`, `stage3Update(t, f) arbiter.ConsensusParamsUpdate`, `turnOnStage3Switch(t, f, a, name string)`, `stage3TestMember(host uint64) string`, `memberEntry(t, f, host uint64, node, domain string) arbiter.VerifierEntry`, `rejectStage3Unchanged(t, f, cmd, reason)` and `stage3ActivationParams()` (failure domains `node:n0`, `node:n1`); Task 64's own beyond-contract test helpers (its Appendix B) `turnOnStage3SwitchesThrough(t, f, a, last string)` (`fsm/stage3_fixture_test.go`) and `ownerAwareFSM(t) (*FSM, *authority.Signer)` (`fsm/stage3_params_test.go`: `stage3FSM` with switches 1–3 committed), which this task's tests share rather than redefine; the server test map `stage3PendingProtoFields map[string]string` and `requireEveryFieldExcept(t *testing.T, m proto.Message)` (`server/safestate_stage3_test.go`), with the entries `BlockVerification.eligible_ring`, `BlockVerification.ring_position` and `BlockVerification.marked_cursor_block` (value `Task 70`).
  - Task 66 (contract §46): `func stage3FieldsPresent(doc *snapshotDoc) bool` (a stage-3 field under a pre-v21 version byte is refused with `stage3 state is present in a pre-v21 container`), `func validateStage3State(st *State) error` (run by `readSnapshot` right after `validateTrackState`, wrapped `snapshot: %w`; its `validateStage3Activation` runs first and refuses a switch that is on without its cutover record), the v21 writer and reader; Task 69: the `validateStage3Incarnations(st)` call in `validateStage3State` and the incarnation arm of `stage3FieldsPresent`; Task 67: `Persist` in `auto` mode (the tests here are body-kind agnostic); Task 68: eviction voiding on in the fixture (no Task 68 function is called here).
  - Plan S3-A Task 7 (contract §4): `fsm.BlockVerificationView`, `(*FSM).BlockVerificationView`, `blockVerificationToPB`, `stage3BlockVerificationView`, `requireEveryField`, `TestBlockVerificationToPBCarriesEveryField`, `TestGetBlockVerificationServesTheSealedBlock`.
  - Plan S3-E1 (contract §48): `arbiter.AdmissionCodeVerifiersUnavailable` (12) of `CORE_TAG_P5`; `pb.BlockVerification.EligibleRing []string`, `.RingPosition uint32`, `.MarkedCursorBlock uint64` (fields 14–16) of `PROTO_TAG_P5`.
  - Existing arbiter: `selectVerifiers`, `activeNodesWithRole`, `u64FromDigest`, `blockTrackLocked` (`fsm/tracks.go:178`), `cloneStrings`, `VerifierSelectN`, `arbiter.VerifierSelectSeedPrefix`, `replay.DigestString`, `TableRegistry.Cursor.BlockNumber`; fsm test helpers `tracksFSM`, `admitTrack`, `trackEnvelope`, `trackSNode`, `bindTrackClaim` (`fsm/tracks_fixture_test.go`), `rejectTracksUnchanged`, `mustSeal`, `submit`, `fixedClient`, `testVerifiers`, `verifierRegistration`, `verifierMarkActive`, `authorityContextFor`, `signedConsensusUpdate`, `mustApply`, `advanceTo`, `snapshotBytes`, `restoreInto`, `restoreErr`, `restoreConfigured`, `restoreRefusedBothWays`, `rewriteSnapshotDocument`, `v20RoundTrip`, `refusedOwnerSwitch`, `quarantineQ`, `testClientLaneA`, `testClientLaneB`, `newTestFSM`, `registerActive`, `mustWorkSet`; orchestrator test helpers `newHarness`, `testParamsO`, `seedGenesisWriterAndVerifiers`, `submitViaNode`, `statementFixture`, `testAccountO`, `sealViaNode`, `registerRCViaNode`, `verifierSetOf`, `fakeNode.rejectApply`, and `(*Loop).handleDispatchRows`.
- Produces (contract §47, verbatim):
  - `BlockVerification.EligibleRing []string` (`json:"eligible_ring,omitempty"`), `RingPosition uint32` (`json:"ring_position,omitempty"`), `MarkedCursorBlock uint64` (`json:"marked_cursor_block,omitempty"`), after `LastMergeable`.
  - `func (f *FSM) poolRingLocked(sources []string) []string`; `func (f *FSM) eligibleRingLocked(owner uint64, sources []string) []string` (the seam Plan S3-E3 Task 73 extends); `func ringStart(blockSeq uint64, n int) int`; `func initialVerifierSet(ring []string, position uint32) []string`; `func (f *FSM) verifiersUnavailableLocked(owner uint64, source string) string`; `func validateStage3Selection(st *State) error`; `WorkSet.Unmarkable []uint64`; `BlockVerificationView.EligibleRing []string`, `.RingPosition uint32`, `.MarkedCursorBlock uint64`; `blockVerificationToPB` setting `EligibleRing`, `RingPosition`, `MarkedCursorBlock`.
  - Refusals: `eligible verifier ring below 3 (spec §7.3 floor)` (`MarkReplaying`); `SubmitResult{Code: arbiter.AdmissionCodeVerifiersUnavailable, Message: "storage-integrity verifiers unavailable for indexer %d: %d eligible, at least %d required"}` (admission; the last value is `VerifierSelectN`, 3); restore: `block %d carries an eligible ring without the owner_aware_selection switch`, `block %d carries an eligible ring but no verifier set`, `block %d eligible ring must be sorted, unique and hold at least 3 verifiers`, `block %d ring position %d is not the block-seeded position %d`, `block %d verifier set is not the ring's three positions ending at %d`, `block %d marked cursor block %d is beyond the registry cursor %d`, `block %d was sealed after the owner_aware_selection cutover (block %d) and marked without an eligible ring`, each wrapped `snapshot: %w` by the reader.
  - Beyond the contract (recorded for Appendix B): `func (f *FSM) blockRingLocked(bv *BlockVerification) []string` (the ring `MarkReplaying` and the WorkSet's unmarkable rule compute for a sealed block: `eligibleRingLocked` of the header's track owner over `bv.SourceNodes`, the pool for a block without a track; Plan S3-E3 Task 73 passes the block's recorded group seal through it, which changes both callers at once), `func blockSeededPosition(blockSeq uint64, n int) uint32` (the one formula `(ringStart(seq, n) + 2) % n` that `MarkReplaying` and restore share), `func verificationHasStage3Fields(bv *BlockVerification) bool`, `func ownerAwareSelectionCutover(st *State) (Stage3SwitchCutover, bool)`, `func ringIsSortedAndUnique(ring []string) bool`; fsm test helpers `ownerAwareOffFSM` and, with the suffix `OA`, `addPoolVerifiersOA`, `resyncVerifierOA`, `reactivateVerifierOA`, `evictVerifierOA`, `claimedBlockOA`, `markReplayingCmdOA`, `markReplayingOA`, `verificationJSONOA`, `verifiersUnavailableOA`, `restoresBothWaysOA`, `forgedVerificationOA`.
  - Named tests: fsm `TestOwnerAwareSelectionOffSelectsByteIdentically`, `TestOwnerAwareSelectionWithOnlyThePoolPicksTodaysSet`, `TestOwnerAwareSelectionDrawsFromTheActivePoolOnly`, `TestOwnerAwareSelectionStoresTheRingThePositionAndTheCursor`, `TestMarkReplayingRefusesAnEligibleRingBelowThree`, `TestAdmissionAnswersVerifiersUnavailableBelowThreeEligible`, `TestVerifiersUnavailableIsCheckedRightAfterSourceUnavailable`, `TestVerifiersUnavailableLeavesTheCoordinateUnspent`, `TestWorkSetDropsAnUnmarkableBlockUntilTheEligibleCountChanges`, `TestRestoreRefusesAnEligibleRingThatDoesNotYieldTheVerifierSet`, `TestRestoreRefusesAnEligibleRingBeforeTheOwnerAwareSelectionSwitch`, `TestRestoreRefusesABlockMarkedAfterTheCutoverWithoutARing`, `TestBlockVerificationServesTheEligibleRing`; orchestrator `TestOrchestratorProposesNoMarkReplayingForAnUnmarkableBlock`; server the edited `TestBlockVerificationToPBCarriesEveryField`.

Notes:
- **Switch off is today's code.** `applyMarkReplaying` tests the switch first and otherwise runs today's `selectVerifiers` branch with today's refusal text; admission's new check returns `""` before computing anything while the switch is off; the WorkSet reads the switch once per call and, off, keeps today's two branches; none of the three new `BlockVerification` fields is written (all `omitempty`), so every container written before the switch is byte-identical. `selectVerifiers` is not refactored: `TestOwnerAwareSelectionWithOnlyThePoolPicksTodaysSet` pins that `initialVerifierSet(ring, blockSeededPosition(seq, n))` equals it over rings of 3 to 7 and blocks 1 to 200, so the two formulas cannot drift.
- **Why the pool ring is today's ring when no host-bound entry exists.** `poolRingLocked` is `selectVerifiers`' ring (`activeNodesWithRole(VERIFIER, sources)`) restricted to `poolVerifierLocked`, a VERIFIER whose listed entry has no `host_indexer_id`. After the signed-claims activation every registered, non-evicted VERIFIER is listed under its registered key (rule 6 of `signedClaimsMembershipLocked`, `fsm/signed_claims.go:277-288`, and a post-activation registration requires listing), and owner-aware selection needs the tracks activation, which needs the signed-claims activation, so the two rings coincide until a host-bound entry is listed, which K68 refuses before this switch is committed. From then on they differ exactly by the host-bound entries: the owner-hosted exclusion of §10.4 step 3 and, until Plan S3-E3's group half, the exclusion of every member.
- **One filter for admission, `MarkReplaying` and the WorkSet.** After the signed-claims activation a statement's source is its owner's SNode (`ownerSourceLocked`), and after the tracks activation a block holds one owner's statements, so a block's `SourceNodes` is `[source]` of its statements: `verifiersUnavailableLocked(owner, source)` computes `eligibleRingLocked(owner, [source])`, the ring `blockRingLocked` computes at the marking and in the WorkSet. Every block sealed after the tracks activation carries a track (statement blocks from the open block's owner, transition blocks from their plan's owner, `fsm/table_set_transition.go:449-453`); every earlier block was safe at that activation (`tracksActivationRefusalLocked`, `fsm/tracks.go:118`) and so marked, so no unmarked block lacks a track once the switch is on, and `blockRingLocked` gives such a block the pool rather than inventing an owner.
- **Where the admission check sits.** Right after the `SOURCE_UNAVAILABLE` block (`fsm/admission.go:133-147`) and before the lane budget, the owner-switch seal preparation and the spent-id insert: a duplicate `client_seq`, a refused target, both fences, a quarantined owner and a down owner SNode answer first; the coordinate stays unspent and no seal is prepared (`TestVerifiersUnavailableIsCheckedRightAfterSourceUnavailable`, `TestVerifiersUnavailableLeavesTheCoordinateUnspent`). Plan S3-E4's `COPIES_BACKPRESSURE` goes after it (Plan S3-E4 contract). The gateway needs no change: `ingressService.SubmitStatement` already reports every non-`ACCEPTED` code under its proto name (`reportAdmission(pb.AdmissionCode(submit.Code).String())`, `server/ingress.go:108`), so `arbiter_admission_rejects_total{code="ADMISSION_CODE_VERIFIERS_UNAVAILABLE"}` counts the refusals once `PROTO_TAG_P5` names code 12, and HouseGate's `HG_TAG_P5` classifies it as retryable (Plan S3-E1 Task 58).
- **The stored record.** `EligibleRing` is the ring at the marking, `RingPosition` the last of the three consecutive positions used (`(ringStart + 2) % n`, so Plan S3-E3's `ReselectVerifiers` continues from `RingPosition + 1`), and `MarkedCursorBlock` the committed `TableRegistry.Cursor.BlockNumber` (the base of Plan S3-E3's re-selection timeout). Restore checks the record's internal consistency, the switch, the cutover boundary and the cursor bound; it does not re-derive which nodes were Active, because node status legitimately changes after the marking (spec §7.4: restore "does not re-derive node status"), and, like today's restore of `VerifierSet`, it does not re-check the sources against the ring. Plan S3-E3 Task 74 extends the position and set rules with stored re-selections.
- **The cutover boundary.** The `owner_aware_selection` cutover records `BlockSeq = len(Blocks)` at the switch-on Apply (Task 64). A block with a higher seq was sealed after it and can be marked only with the switch on, so its marked record must carry a ring; a block at or below it may be marked on either side of the switch, so either form restores (`TestRestoreRefusesABlockMarkedAfterTheCutoverWithoutARing`). A snapshot-query block is never marked and carries no record.
- **The WorkSet is recomputed on every read.** An unmarkable block is listed under `Unmarkable` on every pass while its ring is below three and returns to `SealedUnmarked` on the first pass after the count changes; nothing is stored, so restore has nothing to check. `orchestrator.handleDispatchRows` is unchanged: it ranges over `SealedUnmarked` only (`orchestrator/dispatch.go:15-23`).
- **Frozen fixtures.** Task 66's `stage3_v21_plain.snapshot` and Task 67's `stage3_v21_interned.snapshot` are captured from `stage3V21GoldenFSM`, whose switches are all off, so no record there carries a selection field and both goldens stay byte-identical; Step 15 re-runs them. No golden or vector file is edited.
- **White-box member nodes.** `TestOwnerAwareSelectionDrawsFromTheActivePoolOnly` lists two host-bound entries with Task 64's `memberEntry` (consent signed by the host's enrolment key) after the switch is committed, as K68 requires, and then writes their two Active node records directly: a member's registration and activation are Plan S3-E3's subject, and the test needs only that Active host-bound VERIFIER nodes stay out of the ring and out of admission's count although `activeNodesWithRole` lists them. `TestRefusedOwnerSwitchSealsNothing` sets a node's status the same way (`fsm/owner_switch_test.go:110`).
- **The forged containers.** `forgedVerificationOA` restores the honest container into a bare replica, edits one `BlockVerification` and snapshots it again, so the forgery is independent of the v21 body kind that `Persist` writes (Task 67's `auto` mode writes the interned body); the pre-v21 case uses the existing 13-byte `rewriteSnapshotDocument` on a tracks-fixture v20 container.
- **Shared fixture.** Task 64's `fsm/stage3_params_test.go` already defines `ownerAwareFSM`, so this task's tests use it and define only `ownerAwareOffFSM` (switches 1–2) and helpers with the suffix `OA`; a second definition would not compile in package `fsm`.
- **No README change.** The P5 behaviour is documented by Plan S3-E4 (release notes and the v21 runbook, Task 81); this plan's README edits are Tasks 66–67's component list and series.
- **Checked while drafting.** Every code block of this task, applied from this text (each replace, insert and append as its step words it) to a `git archive` export of arbiter `526a721` on top of scratch stand-ins of Tasks 64–66 and 69 (Task 64's pin, fallout map, fixture and stage-3 rules, Task 65's FSM half, Task 66's v21 framing, restore skeleton and plain golden, Task 69's incarnation fields, pre-v21 arm and restore call) and contract-shaped scratch replace modules for Plan S3-E1's arbiter-proto fields and arbiter-core types, compiles with `go vet` clean and `gofmt` silent; the fourteen fsm and orchestrator tests fail to build as Step 4 states and pass after Steps 5–13, also under `-race`; the server test fails as Step 18 states and passes after Step 19; the shell blocks and `Run:` lines pass `bash -n` and `shellcheck`; `go test ./...` of the export passes (2026-10-16). Each of 33 single-point mutations (selection without the ring, host-bound entries or sources in the ring, the start instead of the last position, an unsorted set, no ring or no cursor stored, the ring stored before the floor check, a field written with the switch off, no admission check, the check ignoring the switch, counting the source, the check moved before the quarantine or source check, after the lane budget or after the insert, no unmarkable rule, the rule with the switch off, the view dropping or sharing the ring, no pre-v21 arm, no restore call, each of the eight restore conditions dropped, the cutover bound off by one, the server dropping the ring or the cursor) is caught by at least one named test. Bazel was not run (no offline module cache for the stubbed modules); Step 22 runs it on the branch.

- [ ] **Step 1: Check the worktree, the branch head and the seams this task builds on**

```bash
W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"; cd "$W" || exit 1
git rev-parse --abbrev-ref HEAD
git status --porcelain
git log -1 --format=%s
CORE="$(GOWORK=off go list -m -f '{{.Dir}}' github.com/sentioxyz/arbiter-core)"
PROTO="$(GOWORK=off go list -m -f '{{.Dir}}' github.com/sentioxyz/arbiter-proto)"
for p in 'func (f *FSM) poolVerifierLocked(' 'func (f *FSM) verifierEntryLocked(' 'func (f *FSM) stage3SwitchOnLocked(' 'func stage3SwitchValue(' 'func (f *FSM) blockTrackLocked(' 'func stage3FieldsPresent(' 'func validateStage3State(' 'func validateStage3Incarnations(' 'func stage3FSM(' 'func stage3Update(' 'func turnOnStage3Switch(' 'func turnOnStage3SwitchesThrough(' 'func ownerAwareFSM(' 'func memberEntry(' 'func stage3TestMember(' 'func rejectStage3Unchanged(' 'func mustWorkSet(' 'func (f *FSM) BlockVerificationView('; do
  grep -rqF -- "$p" fsm || echo "missing in fsm: $p"
done
grep -rqE 'Stage3SwitchOwnerAwareSelection +=' fsm || echo "missing in fsm: Stage3SwitchOwnerAwareSelection"
for p in 'stage3PendingProtoFields = map' 'func requireEveryFieldExcept(' 'func blockVerificationToPB(' 'func stage3BlockVerificationView('; do
  grep -rqF -- "$p" server || echo "missing in server: $p"
done
grep -rqE 'AdmissionCodeVerifiersUnavailable +AdmissionCode += 12' "$CORE" || echo "missing in arbiter-core: AdmissionCodeVerifiersUnavailable"
grep -qF 'MarkedCursorBlock' "$PROTO/gen/pb/arbiter.pb.go" || echo "missing in arbiter-proto: BlockVerification.marked_cursor_block"
grep -rhE '"BlockVerification\.(eligible_ring|ring_position|marked_cursor_block)":' server | wc -l
grep -rn 'func (f \*FSM) poolRingLocked\|func (f \*FSM) eligibleRingLocked\|func ringStart\|func initialVerifierSet\|func validateStage3Selection\|Unmarkable *\[\]uint64\|EligibleRing *\[\]string' fsm server orchestrator
```

Expected: `urwt/claude/multi-source-s3e`; `git status --porcelain` prints nothing; the subject of Task 69's last commit, ending in `(Task 69)`; no `missing` line; `3` (the three `BlockVerification` entries Task 64's pin fallout left for this task); and the last `grep` prints nothing (none of this task's names exists yet). If the branch, the tree or `HEAD` differs, a seam is missing or one of this task's names already exists, stop: Tasks 64–69 land in order on this branch before this task (contract §48), and a missing seam is an earlier task's step to finish, not this task's to invent.

- [ ] **Step 2: Write the failing FSM tests**

Create `fsm/selection_stage3_test.go`:

```go
package fsm

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/accumulator"
)

// The owner-aware selection fixture (housegate spec 2026-10-14 §7.3, §10.4
// step 3; plan S3-E2 contract §47) is Task 64's ownerAwareFSM: stage3FSM,
// the tracks fixture (indexer 1 with SNode s1 and table db.t, indexer 0 with
// SNode s0 and table db0.x, the network pool v1..v3 registered at seq 0,
// Active and listed) after the stage-3 activation, with switches 1-3
// committed in order. Block 1 is indexer 0's published add transition, so
// the first statement block is block 2. The helpers here carry the suffix
// OA (owner-aware) so they never collide with another task's.

// ownerAwareOffFSM is stage3FSM with purge and eviction_voiding on and
// owner_aware_selection still off.
func ownerAwareOffFSM(t *testing.T) (*FSM, *authority.Signer) {
	t.Helper()
	f, a := stage3FSM(t)
	turnOnStage3SwitchesThrough(t, f, a, Stage3SwitchEvictionVoiding)
	return f, a
}

// addPoolVerifiersOA lists ids as pool entries (no host_indexer_id) in one
// authority-signed update, then registers and activates each at seq 1 with
// the key testVerifiers lists for it.
func addPoolVerifiersOA(t *testing.T, f *FSM, a *authority.Signer, ids ...string) {
	t.Helper()
	u := stage3Update(t, f)
	u.Verifiers = append(u.Verifiers, testVerifiers(ids...)...)
	mustApply(t, f, signedConsensusUpdate(t, a, u))
	for _, id := range ids {
		mustApply(t, f, verifierRegistration(t, f, id, id[len(id)-1], 1))
		mustApply(t, f, verifierMarkActive(t, f, id, id[len(id)-1], 1))
	}
}

// resyncVerifierOA re-registers verifier id at registration seq seq: its node
// is Syncing, so it leaves every Active ring until reactivateVerifierOA.
func resyncVerifierOA(t *testing.T, f *FSM, id string, seq uint64) {
	t.Helper()
	mustApply(t, f, verifierRegistration(t, f, id, id[len(id)-1], seq))
	if n := f.st.Nodes[id]; n == nil || n.Status != NodeSyncing {
		t.Fatalf("%s after its re-registration = %+v, want Syncing", id, n)
	}
}

// reactivateVerifierOA marks verifier id Active again at registration seq seq.
func reactivateVerifierOA(t *testing.T, f *FSM, id string, seq uint64) {
	t.Helper()
	mustApply(t, f, verifierMarkActive(t, f, id, id[len(id)-1], seq))
}

// evictVerifierOA applies the authority-signed eviction of verifier id at its
// current registration seq seq.
func evictVerifierOA(t *testing.T, f *FSM, a *authority.Signer, id string, seq uint64) {
	t.Helper()
	cmd := arbiter.EvictNodeCommand{NodeID: id, ExpectedRegistrationSeq: seq, Reason: "verifier lost"}
	token, err := a.SignEvictNodeWithContext(cmd, authorityContextFor(t, f))
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, f, wire.Command{EvictNode: &wire.EvictNode{NodeID: id, Reason: cmd.Reason, ExpectedRegistrationSeq: seq, AuthorityJWS: token}})
	if n := f.st.Nodes[id]; n == nil || n.Status != NodeEvicted {
		t.Fatalf("%s after its eviction = %+v", id, n)
	}
}

// claimedBlockOA admits account's statement seq for owner's table, seals it
// alone and binds its result claim: the returned block is sealed,
// claim-complete and unmarked.
func claimedBlockOA(t *testing.T, f *FSM, key *ecdsa.PrivateKey, account string, seq, owner uint64) uint64 {
	t.Helper()
	if len(f.st.OpenBlock.StatementSeqs) != 0 {
		t.Fatal("claimedBlockOA needs an empty open block")
	}
	stmt := admitTrack(t, f, key, account, seq, owner)
	block := mustSeal(t, f).BlockSeq
	bindTrackClaim(t, f, stmt)
	return block
}

// markReplayingCmdOA is MarkReplaying for block.
func markReplayingCmdOA(block uint64) wire.Command {
	return wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: block}}
}

// markReplayingOA applies MarkReplaying for block.
func markReplayingOA(t *testing.T, f *FSM, block uint64) {
	t.Helper()
	mustApply(t, f, markReplayingCmdOA(block))
}

// verificationJSONOA is block's verification record as the snapshot
// document encodes it.
func verificationJSONOA(t *testing.T, f *FSM, block uint64) string {
	t.Helper()
	b, err := json.Marshal(f.st.Verifications[block])
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// verifiersUnavailableOA is admission's VERIFIERS_UNAVAILABLE answer for a
// statement of owner whose eligible ring holds eligible verifiers.
func verifiersUnavailableOA(owner uint64, eligible int) SubmitResult {
	return SubmitResult{Code: arbiter.AdmissionCodeVerifiersUnavailable,
		Message: fmt.Sprintf("storage-integrity verifiers unavailable for indexer %d: %d eligible, at least 3 required", owner, eligible)}
}

// restoresBothWaysOA requires the bare and the configured restore of data to
// accept it.
func restoresBothWaysOA(t *testing.T, f *FSM, data []byte) {
	t.Helper()
	if err := restoreErr(t, data); err != nil {
		t.Fatalf("bare restore: %v", err)
	}
	if _, err := restoreConfigured(t, f, data); err != nil {
		t.Fatalf("configured restore: %v", err)
	}
}

// forgedVerificationOA is f's container with block's verification record
// edited by forge: a bare replica restored from f's own container is edited
// and snapshotted again, so every other byte is f's whatever body kind the
// writer chose.
func forgedVerificationOA(t *testing.T, f *FSM, block uint64, forge func(*BlockVerification)) []byte {
	t.Helper()
	g := restoreInto(t, snapshotBytes(t, f))
	bv := g.st.Verifications[block]
	if bv == nil {
		t.Fatalf("block %d has no verification record", block)
	}
	forge(bv)
	return snapshotBytes(t, g)
}

// TestOwnerAwareSelectionOffSelectsByteIdentically: contract §44, §47. With
// the switch off (stage3 active, purge and eviction_voiding on) selection is
// today's: the same statements on tracksFSM, which never set stage3, leave
// byte-identical verification records with no selection field; below the
// pool floor admission accepts, MarkReplaying gives today's refusal and the
// WorkSet lists the block SealedUnmarked with nothing Unmarkable.
func TestOwnerAwareSelectionOffSelectsByteIdentically(t *testing.T) {
	base, _ := tracksFSM(t)
	off, _ := ownerAwareOffFSM(t)
	key, account := fixedClient(t)
	marked := func(f *FSM) []string {
		var records []string
		for i, owner := range []uint64{1, 0, 1} {
			block := claimedBlockOA(t, f, key, account, uint64(i+1), owner)
			want := f.selectVerifiers(block, f.st.Verifications[block].SourceNodes)
			markReplayingOA(t, f, block)
			if got := f.st.Verifications[block].VerifierSet; !slices.Equal(got, want) {
				t.Fatalf("block %d verifier set = %v, want selectVerifiers' %v", block, got, want)
			}
			records = append(records, verificationJSONOA(t, f, block))
		}
		return records
	}
	baseRecords, offRecords := marked(base), marked(off)
	if !slices.Equal(baseRecords, offRecords) {
		t.Fatalf("verification records differ with the switch off:\n%v\n%v", baseRecords, offRecords)
	}
	for _, record := range offRecords {
		for _, field := range []string{"eligible_ring", "ring_position", "marked_cursor_block"} {
			if strings.Contains(record, field) {
				t.Fatalf("a record marked with the switch off carries %s: %s", field, record)
			}
		}
	}
	for _, f := range []*FSM{base, off} {
		resyncVerifierOA(t, f, "v3", 1)
		block := claimedBlockOA(t, f, key, account, 4, 0)
		reason := "active non-source verifier pool below 3 (§7.4 floor)"
		if f == base {
			rejectTracksUnchanged(t, f, markReplayingCmdOA(block), reason)
		} else {
			rejectStage3Unchanged(t, f, markReplayingCmdOA(block), reason)
		}
		if ws := mustWorkSet(t, f); !slices.Equal(ws.SealedUnmarked, []uint64{block}) || ws.Unmarkable != nil {
			t.Fatalf("WorkSet below the pool floor: sealed unmarked %v, unmarkable %v", ws.SealedUnmarked, ws.Unmarkable)
		}
	}
}

// TestOwnerAwareSelectionWithOnlyThePoolPicksTodaysSet: contract §47. With
// no host-bound entry the eligible ring is today's ring and the block-seeded
// position yields today's set: for rings of 3 to 7 Active verifiers over
// blocks 1 to 200, and through MarkReplaying with the switch on over five
// blocks of both owners and a pool of five.
func TestOwnerAwareSelectionWithOnlyThePoolPicksTodaysSet(t *testing.T) {
	for n := VerifierSelectN; n <= 7; n++ {
		f := newTestFSM(t)
		var ids []string
		for i := 1; i <= n; i++ {
			id := fmt.Sprintf("v%d", i)
			registerActive(t, f, id, arbiter.NodeRoleVerifier)
			ids = append(ids, id)
		}
		ring := f.activeNodesWithRole(arbiter.NodeRoleVerifier, nil)
		if !slices.Equal(ring, ids) {
			t.Fatalf("ring = %v, want %v", ring, ids)
		}
		for block := uint64(1); block <= 200; block++ {
			got := initialVerifierSet(ring, blockSeededPosition(block, len(ring)))
			if want := f.selectVerifiers(block, nil); !slices.Equal(got, want) {
				t.Fatalf("ring of %d, block %d: initialVerifierSet = %v, selectVerifiers = %v", n, block, got, want)
			}
		}
	}

	f, a := ownerAwareFSM(t)
	addPoolVerifiersOA(t, f, a, "v4", "v5")
	key, account := fixedClient(t)
	for i, owner := range []uint64{1, 0, 1, 0, 1} {
		block := claimedBlockOA(t, f, key, account, uint64(i+1), owner)
		bv := f.st.Verifications[block]
		excluded := map[string]bool{}
		for _, s := range bv.SourceNodes {
			excluded[s] = true
		}
		ring := f.eligibleRingLocked(owner, bv.SourceNodes)
		if today := f.activeNodesWithRole(arbiter.NodeRoleVerifier, excluded); !slices.Equal(ring, today) || len(ring) != 5 {
			t.Fatalf("block %d: eligible ring %v, today's ring %v", block, ring, today)
		}
		want := f.selectVerifiers(block, bv.SourceNodes)
		markReplayingOA(t, f, block)
		if !slices.Equal(bv.VerifierSet, want) || !slices.Equal(bv.EligibleRing, ring) {
			t.Fatalf("block %d: set %v ring %v, want today's set %v", block, bv.VerifierSet, bv.EligibleRing, want)
		}
	}
}

// TestOwnerAwareSelectionDrawsFromTheActivePoolOnly: contract §47, K68. With
// two host-bound members listed and Active (one hosted by each owner) and a
// pool verifier still Syncing, the eligible ring of a block of either owner
// is the Active pool minus its source: no member enters it, although today's
// ring would hold both, and admission counts the same ring.
func TestOwnerAwareSelectionDrawsFromTheActivePoolOnly(t *testing.T) {
	f, a := ownerAwareFSM(t)
	u := stage3Update(t, f)
	u.Verifiers = append(u.Verifiers, memberEntry(t, f, 0, stage3TestMember(0), "node:n0"), memberEntry(t, f, 1, stage3TestMember(1), "node:n1"))
	u.Verifiers = append(u.Verifiers, testVerifiers("v4")...)
	mustApply(t, f, signedConsensusUpdate(t, a, u))
	mustApply(t, f, verifierRegistration(t, f, "v4", '4', 1))
	// A member's registration and activation are Plan S3-E3's subject; the
	// ring needs only two Active host-bound VERIFIER nodes, so their records
	// are written directly.
	for _, host := range []uint64{0, 1} {
		id := stage3TestMember(host)
		entry, ok := f.verifierEntryLocked(id)
		if !ok || entry.HostIndexerID == nil || *entry.HostIndexerID != host {
			t.Fatalf("%s is not listed as hosted by indexer %d: %+v", id, host, entry)
		}
		f.st.Nodes[id] = &NodeInfo{Registration: arbiter.NodeRegistration{NodeID: id, Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier},
			Ed25519Pubkey: bytes.Clone(entry.Ed25519Pubkey)}, Status: NodeActive}
		if f.poolVerifierLocked(id) {
			t.Fatalf("host-bound %s counts as a pool verifier", id)
		}
	}
	if today := f.activeNodesWithRole(arbiter.NodeRoleVerifier, nil); !slices.Equal(today, []string{"member-0", "member-1", "v1", "v2", "v3"}) {
		t.Fatalf("today's ring = %v, want both members and v1..v3", today)
	}
	key, account := fixedClient(t)
	pool := []string{"v1", "v2", "v3"}
	for i, owner := range []uint64{1, 0} {
		block := claimedBlockOA(t, f, key, account, uint64(i+1), owner)
		markReplayingOA(t, f, block)
		if bv := f.st.Verifications[block]; !slices.Equal(bv.EligibleRing, pool) || !slices.Equal(bv.VerifierSet, pool) {
			t.Fatalf("block %d of indexer %d: ring %v set %v, want the Active pool %v", block, owner, bv.EligibleRing, bv.VerifierSet, pool)
		}
	}
	if ring := f.poolRingLocked([]string{"v2"}); !slices.Equal(ring, []string{"v1", "v3"}) {
		t.Fatalf("pool ring without source v2 = %v", ring)
	}
	resyncVerifierOA(t, f, "v3", 1)
	if got := submit(t, f, trackEnvelope(t, f, key, account, "", 3, 1)); got != verifiersUnavailableOA(1, 2) {
		t.Fatalf("with two Active members and two Active pool verifiers = %+v, want %+v", got, verifiersUnavailableOA(1, 2))
	}
}

// TestOwnerAwareSelectionStoresTheRingThePositionAndTheCursor: contract §47.
// MarkReplaying stores the ring it drew from, the last ring position used
// (the block-seeded start plus two) and the committed registry cursor block,
// and the set is the ring's three positions ending there; a later cursor
// move leaves the record unchanged, and both restores keep it.
func TestOwnerAwareSelectionStoresTheRingThePositionAndTheCursor(t *testing.T) {
	f, a := ownerAwareFSM(t)
	addPoolVerifiersOA(t, f, a, "v4", "v5")
	key, account := fixedClient(t)
	block := claimedBlockOA(t, f, key, account, 1, 1)
	cursor := f.st.TableRegistry.Cursor.BlockNumber + 10
	mustApply(t, f, advanceTo(cursor))
	if got := f.st.TableRegistry.Cursor.BlockNumber; got != cursor {
		t.Fatalf("registry cursor = %d, want %d", got, cursor)
	}
	markReplayingOA(t, f, block)
	bv := f.st.Verifications[block]
	ring := []string{"v1", "v2", "v3", "v4", "v5"}
	position := uint32((ringStart(block, len(ring)) + 2) % len(ring))
	want := []string{ring[(position+3)%5], ring[(position+4)%5], ring[position]}
	slices.Sort(want)
	if !slices.Equal(bv.EligibleRing, ring) || bv.RingPosition != position || bv.MarkedCursorBlock != cursor || !slices.Equal(bv.VerifierSet, want) {
		t.Fatalf("record = ring %v position %d cursor %d set %v, want ring %v position %d cursor %d set %v",
			bv.EligibleRing, bv.RingPosition, bv.MarkedCursorBlock, bv.VerifierSet, ring, position, cursor, want)
	}
	for _, ss := range f.blockStatements(block) {
		if ss.Status != StatusReplaying {
			t.Fatalf("statement %d is %s after the marking", ss.Seq, ss.Status)
		}
	}
	record := verificationJSONOA(t, f, block)
	mustApply(t, f, advanceTo(cursor+10))
	if got := verificationJSONOA(t, f, block); got != record {
		t.Fatalf("a cursor move changed the record:\n%s\n%s", record, got)
	}
	data := snapshotBytes(t, f)
	restoresBothWaysOA(t, f, data)
	if got := verificationJSONOA(t, restoreInto(t, data), block); got != record {
		t.Fatalf("restored record:\n%s\nwant\n%s", got, record)
	}
	rejectStage3Unchanged(t, f, markReplayingCmdOA(block), "block already marked replaying")
}

// TestMarkReplayingRefusesAnEligibleRingBelowThree: contract §47, spec §7.3.
// A block whose eligible ring fell below three after its seal (a pool
// verifier back in Syncing, or evicted) is refused with state unchanged and
// marked by the first MarkReplaying after the ring holds three again.
func TestMarkReplayingRefusesAnEligibleRingBelowThree(t *testing.T) {
	const reason = "eligible verifier ring below 3 (spec §7.3 floor)"
	t.Run("a pool verifier back in Syncing", func(t *testing.T) {
		f, _ := ownerAwareFSM(t)
		key, account := fixedClient(t)
		block := claimedBlockOA(t, f, key, account, 1, 1)
		resyncVerifierOA(t, f, "v3", 1)
		rejectStage3Unchanged(t, f, markReplayingCmdOA(block), reason)
		reactivateVerifierOA(t, f, "v3", 1)
		markReplayingOA(t, f, block)
		if got := f.st.Verifications[block].VerifierSet; !slices.Equal(got, []string{"v1", "v2", "v3"}) {
			t.Fatalf("verifier set = %v", got)
		}
	})
	t.Run("an evicted pool verifier", func(t *testing.T) {
		f, a := ownerAwareFSM(t)
		key, account := fixedClient(t)
		block := claimedBlockOA(t, f, key, account, 1, 0)
		evictVerifierOA(t, f, a, "v2", 0)
		rejectStage3Unchanged(t, f, markReplayingCmdOA(block), reason)
		addPoolVerifiersOA(t, f, a, "v4")
		markReplayingOA(t, f, block)
		if got := f.st.Verifications[block].VerifierSet; !slices.Equal(got, []string{"v1", "v3", "v4"}) {
			t.Fatalf("verifier set = %v", got)
		}
	})
}

// TestAdmissionAnswersVerifiersUnavailableBelowThreeEligible: contract §47,
// spec §7.3. With the switch on and fewer than three Active pool verifiers
// every owner's statement is answered VERIFIERS_UNAVAILABLE with its eligible
// count and nothing changes; once the pool holds three the same statements
// are admitted. With the switch off the same state admits.
func TestAdmissionAnswersVerifiersUnavailableBelowThreeEligible(t *testing.T) {
	f, _ := ownerAwareFSM(t)
	key, account := fixedClient(t)
	resyncVerifierOA(t, f, "v3", 1)
	for i, owner := range []uint64{1, 0} {
		before := snapshotBytes(t, f)
		if got := submit(t, f, trackEnvelope(t, f, key, account, "", uint64(i+1), owner)); got != verifiersUnavailableOA(owner, 2) {
			t.Fatalf("indexer %d's statement = %+v, want %+v", owner, got, verifiersUnavailableOA(owner, 2))
		}
		if !bytes.Equal(before, snapshotBytes(t, f)) {
			t.Fatal("a VERIFIERS_UNAVAILABLE answer changed state")
		}
	}
	resyncVerifierOA(t, f, "v2", 1)
	if got := submit(t, f, trackEnvelope(t, f, key, account, "", 1, 1)); got != verifiersUnavailableOA(1, 1) {
		t.Fatalf("with one eligible verifier = %+v", got)
	}
	reactivateVerifierOA(t, f, "v2", 1)
	reactivateVerifierOA(t, f, "v3", 1)
	for i, owner := range []uint64{1, 0} {
		admitTrack(t, f, key, account, uint64(i+1), owner)
	}

	off, _ := ownerAwareOffFSM(t)
	resyncVerifierOA(t, off, "v3", 1)
	if r := submit(t, off, trackEnvelope(t, off, key, account, "", 1, 1)); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("with the switch off = %+v, want ACCEPTED", r)
	}
}

// TestVerifiersUnavailableIsCheckedRightAfterSourceUnavailable: contract
// §47. Below three eligible verifiers a duplicate client_seq, a quarantined
// owner and a down owner SNode still answer first, and VERIFIERS_UNAVAILABLE
// answers before the lane budget, each with state unchanged.
func TestVerifiersUnavailableIsCheckedRightAfterSourceUnavailable(t *testing.T) {
	t.Run("a duplicate client_seq answers first", func(t *testing.T) {
		f, _ := ownerAwareFSM(t)
		key, account := fixedClient(t)
		admitTrack(t, f, key, account, 1, 1)
		resyncVerifierOA(t, f, "v3", 1)
		want := SubmitResult{Code: arbiter.AdmissionCodeDuplicateClientSeq, Message: "client_seq already spent (client bug: do not retry with the same seq)"}
		if got := refusedOwnerSwitch(t, f, trackEnvelope(t, f, key, account, "", 1, 1)); !reflect.DeepEqual(got, want) {
			t.Fatalf("answer = %+v, want %+v", got, want)
		}
	})
	t.Run("a quarantined owner answers INDEXER_QUARANTINED first", func(t *testing.T) {
		f, a := ownerAwareFSM(t)
		key, account := fixedClient(t)
		mustApply(t, f, quarantineQ(t, f, a, 0, 0, "operator hold"))
		resyncVerifierOA(t, f, "v3", 1)
		want := SubmitResult{Code: arbiter.AdmissionCodeIndexerQuarantined, Message: "storage-integrity indexer 0 is quarantined"}
		if got := refusedOwnerSwitch(t, f, trackEnvelope(t, f, key, account, "", 1, 0)); !reflect.DeepEqual(got, want) {
			t.Fatalf("answer = %+v, want %+v", got, want)
		}
		if got := refusedOwnerSwitch(t, f, trackEnvelope(t, f, key, account, "", 1, 1)); !reflect.DeepEqual(got, verifiersUnavailableOA(1, 2)) {
			t.Fatalf("indexer 1's answer = %+v", got)
		}
	})
	t.Run("the owner's SNode down answers SOURCE_UNAVAILABLE first", func(t *testing.T) {
		f, _ := ownerAwareFSM(t)
		key, account := fixedClient(t)
		resyncVerifierOA(t, f, "v3", 1)
		f.st.Nodes[trackSNode(0)].Status = NodeSyncing
		env := trackEnvelope(t, f, key, account, "", 1, 0)
		want := SubmitResult{Code: arbiter.AdmissionCodeSourceUnavailable, Message: "storage-integrity source s0 of indexer 0 is not active"}
		if got := refusedOwnerSwitch(t, f, env); !reflect.DeepEqual(got, want) {
			t.Fatalf("answer = %+v, want %+v", got, want)
		}
		f.st.Nodes[trackSNode(0)].Status = NodeActive
		if got := refusedOwnerSwitch(t, f, env); !reflect.DeepEqual(got, verifiersUnavailableOA(0, 2)) {
			t.Fatalf("answer once s0 is Active = %+v", got)
		}
	})
	t.Run("VERIFIERS_UNAVAILABLE answers before the lane budget", func(t *testing.T) {
		f, a := ownerAwareFSM(t)
		lanes := stage3Update(t, f)
		lanes.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 1}
		mustApply(t, f, signedConsensusUpdate(t, a, lanes))
		key, account := fixedClient(t)
		if r := submit(t, f, trackEnvelope(t, f, key, account, testClientLaneA, 1, 1)); r.Code != arbiter.AdmissionCodeAccepted {
			t.Fatalf("first lane = %+v", r)
		}
		resyncVerifierOA(t, f, "v3", 1)
		env := trackEnvelope(t, f, key, account, testClientLaneB, 1, 1)
		if got := refusedOwnerSwitch(t, f, env); !reflect.DeepEqual(got, verifiersUnavailableOA(1, 2)) {
			t.Fatalf("answer = %+v", got)
		}
		reactivateVerifierOA(t, f, "v3", 1)
		want := SubmitResult{Code: arbiter.AdmissionCodeLaneBudgetExceeded,
			Message: fmt.Sprintf("account %s already holds 1 client lanes (max_lanes_per_account 1)", account)}
		if got := refusedOwnerSwitch(t, f, env); !reflect.DeepEqual(got, want) {
			t.Fatalf("answer with three eligible = %+v, want %+v", got, want)
		}
	})
}

// TestVerifiersUnavailableLeavesTheCoordinateUnspent: contract §47. A
// statement refused with VERIFIERS_UNAVAILABLE spends no client_seq, takes no
// statement seq and prepares no owner-switch seal: the other owner's open
// block stays open, and once the ring holds three the same envelope is
// admitted with the next statement seq and seals that block first.
func TestVerifiersUnavailableLeavesTheCoordinateUnspent(t *testing.T) {
	f, _ := ownerAwareFSM(t)
	key, account := fixedClient(t)
	first := admitTrack(t, f, key, account, 1, 1)
	blocks := len(f.st.Blocks)
	resyncVerifierOA(t, f, "v3", 1)
	env := trackEnvelope(t, f, key, account, "", 2, 0)
	if got := refusedOwnerSwitch(t, f, env); !reflect.DeepEqual(got, verifiersUnavailableOA(0, 2)) {
		t.Fatalf("answer = %+v", got)
	}
	if st := f.st.SpentIDs.Status(env.StatementID.Coord()); st == accumulator.StatusSpentDuplicate {
		t.Fatal("the refused statement's client_seq is spent")
	}
	if ob := f.st.OpenBlock; ob.Owner == nil || *ob.Owner != 1 || !slices.Equal(ob.StatementSeqs, []uint64{first}) || len(f.st.Blocks) != blocks {
		t.Fatalf("open block %+v, %d blocks: the refusal sealed or moved the open block", ob, len(f.st.Blocks))
	}
	reactivateVerifierOA(t, f, "v3", 1)
	if r := submit(t, f, env); r != (SubmitResult{Code: arbiter.AdmissionCodeAccepted, StatementSeq: first + 1}) {
		t.Fatalf("the same envelope with three eligible = %+v", r)
	}
	if h := f.st.Blocks[len(f.st.Blocks)-1]; len(f.st.Blocks) != blocks+1 || h.Track == nil || h.Track.IndexerID != 1 || h.StatementCount != 1 {
		t.Fatalf("owner-switch seal = %+v (%d blocks)", h, len(f.st.Blocks))
	}
}

// TestWorkSetDropsAnUnmarkableBlockUntilTheEligibleCountChanges: contract
// §47, spec §7.3. With the switch on a sealed, claim-complete, unmarked block
// whose ring is below three is Unmarkable and not SealedUnmarked on every
// pass, a block awaiting its claim stays AwaitingRC, and once the ring holds
// three the block is SealedUnmarked again. With the switch off the same
// state lists it SealedUnmarked.
func TestWorkSetDropsAnUnmarkableBlockUntilTheEligibleCountChanges(t *testing.T) {
	f, _ := ownerAwareFSM(t)
	key, account := fixedClient(t)
	claimed := claimedBlockOA(t, f, key, account, 1, 1)
	admitTrack(t, f, key, account, 2, 0)
	awaiting := mustSeal(t, f).BlockSeq
	if ws := mustWorkSet(t, f); !slices.Equal(ws.SealedUnmarked, []uint64{claimed}) || ws.Unmarkable != nil || !slices.Equal(ws.AwaitingRC, []uint64{awaiting}) {
		t.Fatalf("with three eligible: sealed unmarked %v, unmarkable %v, awaiting %v", ws.SealedUnmarked, ws.Unmarkable, ws.AwaitingRC)
	}
	resyncVerifierOA(t, f, "v3", 1)
	for pass := 1; pass <= 2; pass++ {
		if ws := mustWorkSet(t, f); ws.SealedUnmarked != nil || !slices.Equal(ws.Unmarkable, []uint64{claimed}) || !slices.Equal(ws.AwaitingRC, []uint64{awaiting}) {
			t.Fatalf("pass %d with two eligible: sealed unmarked %v, unmarkable %v, awaiting %v", pass, ws.SealedUnmarked, ws.Unmarkable, ws.AwaitingRC)
		}
	}
	reactivateVerifierOA(t, f, "v3", 1)
	if ws := mustWorkSet(t, f); !slices.Equal(ws.SealedUnmarked, []uint64{claimed}) || ws.Unmarkable != nil {
		t.Fatalf("with three eligible again: sealed unmarked %v, unmarkable %v", ws.SealedUnmarked, ws.Unmarkable)
	}
	markReplayingOA(t, f, claimed)
	if ws := mustWorkSet(t, f); ws.SealedUnmarked != nil || ws.Unmarkable != nil {
		t.Fatalf("after the marking: sealed unmarked %v, unmarkable %v", ws.SealedUnmarked, ws.Unmarkable)
	}

	off, _ := ownerAwareOffFSM(t)
	block := claimedBlockOA(t, off, key, account, 1, 1)
	resyncVerifierOA(t, off, "v3", 1)
	if ws := mustWorkSet(t, off); !slices.Equal(ws.SealedUnmarked, []uint64{block}) || ws.Unmarkable != nil {
		t.Fatalf("with the switch off: sealed unmarked %v, unmarkable %v", ws.SealedUnmarked, ws.Unmarkable)
	}
}

// TestRestoreRefusesAnEligibleRingThatDoesNotYieldTheVerifierSet: contract
// §47. The honest container restores; each forged selection record of a
// block marked over a ring of five, or of an unmarked block, is refused by
// both restores, naming the block.
func TestRestoreRefusesAnEligibleRingThatDoesNotYieldTheVerifierSet(t *testing.T) {
	f, a := ownerAwareFSM(t)
	addPoolVerifiersOA(t, f, a, "v4", "v5")
	key, account := fixedClient(t)
	marked := claimedBlockOA(t, f, key, account, 1, 1)
	markReplayingOA(t, f, marked)
	unmarked := claimedBlockOA(t, f, key, account, 2, 0)
	restoresBothWaysOA(t, f, snapshotBytes(t, f))
	bv := f.st.Verifications[marked]
	ring, position, cursor := slices.Clone(bv.EligibleRing), bv.RingPosition, f.st.TableRegistry.Cursor.BlockNumber
	shape := fmt.Sprintf("block %d eligible ring must be sorted, unique and hold at least 3 verifiers", marked)
	yields := fmt.Sprintf("block %d verifier set is not the ring's three positions ending at %d", marked, position)
	for _, tc := range []struct {
		name  string
		block uint64
		forge func(*BlockVerification)
		want  string
	}{
		{"a ring without a verifier set", unmarked, func(bv *BlockVerification) {
			bv.EligibleRing, bv.RingPosition, bv.MarkedCursorBlock = slices.Clone(ring), position, cursor
		}, fmt.Sprintf("block %d carries an eligible ring but no verifier set", unmarked)},
		{"an unsorted ring", marked, func(bv *BlockVerification) {
			bv.EligibleRing[0], bv.EligibleRing[1] = bv.EligibleRing[1], bv.EligibleRing[0]
		}, shape},
		{"a repeated ring entry", marked, func(bv *BlockVerification) { bv.EligibleRing[1] = bv.EligibleRing[0] }, shape},
		{"a ring of two", marked, func(bv *BlockVerification) { bv.EligibleRing = bv.EligibleRing[:2] }, shape},
		{"a position that is not the block-seeded one", marked, func(bv *BlockVerification) { bv.RingPosition = (position + 1) % 5 },
			fmt.Sprintf("block %d ring position %d is not the block-seeded position %d", marked, (position+1)%5, position)},
		{"a ring whose positions yield another set", marked, func(bv *BlockVerification) { bv.EligibleRing[position] += "x" }, yields},
		{"a set that is not the ring's", marked, func(bv *BlockVerification) { bv.VerifierSet = initialVerifierSet(ring, (position+1)%5) }, yields},
		{"a marked cursor block beyond the registry cursor", marked, func(bv *BlockVerification) { bv.MarkedCursorBlock = cursor + 1 },
			fmt.Sprintf("block %d marked cursor block %d is beyond the registry cursor %d", marked, cursor+1, cursor)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restoreRefusedBothWays(t, f, forgedVerificationOA(t, f, tc.block, tc.forge), tc.want)
		})
	}
}

// TestRestoreRefusesAnEligibleRingBeforeTheOwnerAwareSelectionSwitch:
// contract §46, §47. Any field of the selection record in a v21 container
// whose owner_aware_selection switch is off is refused, and an eligible ring
// in a v20 container is refused at its version byte.
func TestRestoreRefusesAnEligibleRingBeforeTheOwnerAwareSelectionSwitch(t *testing.T) {
	key, account := fixedClient(t)
	off, _ := ownerAwareOffFSM(t)
	block := claimedBlockOA(t, off, key, account, 1, 1)
	markReplayingOA(t, off, block)
	restoresBothWaysOA(t, off, snapshotBytes(t, off))
	want := fmt.Sprintf("block %d carries an eligible ring without the owner_aware_selection switch", block)
	for _, tc := range []struct {
		name  string
		forge func(*BlockVerification)
	}{
		{"an eligible ring", func(bv *BlockVerification) { bv.EligibleRing = []string{"v1", "v2", "v3"} }},
		{"a ring position", func(bv *BlockVerification) { bv.RingPosition = 2 }},
		{"a marked cursor block", func(bv *BlockVerification) { bv.MarkedCursorBlock = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restoreRefusedBothWays(t, off, forgedVerificationOA(t, off, block, tc.forge), want)
		})
	}
	t.Run("an eligible ring in a v20 container", func(t *testing.T) {
		f, _ := tracksFSM(t)
		block := claimedBlockOA(t, f, key, account, 1, 1)
		markReplayingOA(t, f, block)
		v20RoundTrip(t, f, "a block marked without stage3")
		forged := rewriteSnapshotDocument(t, snapshotBytes(t, f), snapshotVersionV20, func(doc map[string]any) {
			record := doc["verifications"].(map[string]any)[strconv.FormatUint(block, 10)].(map[string]any)
			record["eligible_ring"] = []any{"v1", "v2", "v3"}
		})
		restoreRefusedBothWays(t, f, forged, "stage3 state is present in a pre-v21 container")
	})
}

// TestRestoreRefusesABlockMarkedAfterTheCutoverWithoutARing: contract §47. A
// block sealed after the owner_aware_selection cutover can be marked only
// with the switch on, so its record must carry the ring; a block sealed at
// or before the cutover may carry one (marked after it) or not (marked
// before it).
func TestRestoreRefusesABlockMarkedAfterTheCutoverWithoutARing(t *testing.T) {
	f, a := ownerAwareOffFSM(t)
	key, account := fixedClient(t)
	before := claimedBlockOA(t, f, key, account, 1, 1)
	markReplayingOA(t, f, before)
	straddling := claimedBlockOA(t, f, key, account, 2, 0)
	turnOnStage3Switch(t, f, a, Stage3SwitchOwnerAwareSelection)
	cutover, ok := ownerAwareSelectionCutover(f.st)
	if !ok || cutover.BlockSeq != straddling {
		t.Fatalf("owner_aware_selection cutover = %+v, %v; want block %d", cutover, ok, straddling)
	}
	markReplayingOA(t, f, straddling)
	after := claimedBlockOA(t, f, key, account, 3, 1)
	markReplayingOA(t, f, after)
	for block, ring := range map[uint64]bool{before: false, straddling: true, after: true} {
		if got := len(f.st.Verifications[block].EligibleRing) > 0; got != ring {
			t.Fatalf("block %d carries a ring: %v, want %v", block, got, ring)
		}
	}
	restoresBothWaysOA(t, f, snapshotBytes(t, f))
	clear := func(bv *BlockVerification) { bv.EligibleRing, bv.RingPosition, bv.MarkedCursorBlock = nil, 0, 0 }
	restoresBothWaysOA(t, f, forgedVerificationOA(t, f, straddling, clear))
	restoreRefusedBothWays(t, f, forgedVerificationOA(t, f, after, clear),
		fmt.Sprintf("block %d was sealed after the owner_aware_selection cutover (block %d) and marked without an eligible ring", after, straddling))
}

// TestBlockVerificationServesTheEligibleRing: contract §47. The block
// verification view carries the stored ring, position and cursor block,
// detached from state, and none of them for a block marked with the switch
// off.
func TestBlockVerificationServesTheEligibleRing(t *testing.T) {
	f, a := ownerAwareFSM(t)
	addPoolVerifiersOA(t, f, a, "v4", "v5")
	key, account := fixedClient(t)
	block := claimedBlockOA(t, f, key, account, 1, 1)
	markReplayingOA(t, f, block)
	bv := f.st.Verifications[block]
	view, err := f.BlockVerificationView(block)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(view.EligibleRing, bv.EligibleRing) || len(view.EligibleRing) != 5 || view.RingPosition != bv.RingPosition ||
		view.MarkedCursorBlock != bv.MarkedCursorBlock || view.MarkedCursorBlock == 0 || !slices.Equal(view.Dispatch.VerifierSet, bv.VerifierSet) {
		t.Fatalf("view ring %v position %d cursor %d set %v; record ring %v position %d cursor %d set %v", view.EligibleRing, view.RingPosition,
			view.MarkedCursorBlock, view.Dispatch.VerifierSet, bv.EligibleRing, bv.RingPosition, bv.MarkedCursorBlock, bv.VerifierSet)
	}
	view.EligibleRing[0] = "forged"
	if bv.EligibleRing[0] != "v1" {
		t.Fatal("the view shares the stored ring")
	}

	off, _ := ownerAwareOffFSM(t)
	block = claimedBlockOA(t, off, key, account, 1, 1)
	markReplayingOA(t, off, block)
	view, err = off.BlockVerificationView(block)
	if err != nil || view.EligibleRing != nil || view.RingPosition != 0 || view.MarkedCursorBlock != 0 {
		t.Fatalf("a block marked with the switch off: %v, ring %v position %d cursor %d", err, view.EligibleRing, view.RingPosition, view.MarkedCursorBlock)
	}
}
```

- [ ] **Step 3: Write the failing orchestrator test**

In `orchestrator/dispatch_test.go`, replace the import block (`:3-12`):

```go
import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sentioxyz/arbiter/fsm"
)
```

with:

```go
import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
)
```

Append to the end of `orchestrator/dispatch_test.go`:

```go
// TestOrchestratorProposesNoMarkReplayingForAnUnmarkableBlock: plan S3-E2
// contract §47 (housegate spec 2026-10-14 §7.3). handleDispatchRows proposes
// MarkReplaying for SealedUnmarked rows only, so a block the FSM lists under
// WorkSet.Unmarkable (owner_aware_selection on, eligible ring below three)
// costs no proposal on any pass; the same block listed SealedUnmarked is
// marked by exactly one.
func TestOrchestratorProposesNoMarkReplayingForAnUnmarkableBlock(t *testing.T) {
	f, node, _, _, _, o, _ := newHarness(t, testParamsO())
	seedGenesisWriterAndVerifiers(t, node)
	submitViaNode(t, node, statementFixture{account: testAccountO(t), seq: 1})
	sealViaNode(t, node)
	registerRCViaNode(t, node, f, 1)
	ws, err := f.WorkSet()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ws.SealedUnmarked, []uint64{1}) || ws.Unmarkable != nil {
		t.Fatalf("fixture WorkSet: sealed unmarked %v, unmarkable %v", ws.SealedUnmarked, ws.Unmarkable)
	}
	var marks []uint64
	node.rejectApply = func(c wire.Command) (fsm.Rejected, bool) {
		if c.MarkReplaying != nil {
			marks = append(marks, c.MarkReplaying.BlockSeq)
		}
		return fsm.Rejected{}, false
	}
	unmarkable := fsm.WorkSet{Unmarkable: ws.SealedUnmarked}
	for pass := 1; pass <= 3; pass++ {
		if err := o.handleDispatchRows(t.Context(), unmarkable); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}
	if info, ok := f.BlockDispatchInfo(1); len(marks) != 0 || !ok || len(info.VerifierSet) != 0 {
		t.Fatalf("an unmarkable block was proposed %v or marked %v", marks, info.VerifierSet)
	}
	if err := o.handleDispatchRows(t.Context(), ws); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(marks, []uint64{1}) || len(verifierSetOf(t, f, 1)) != fsm.VerifierSelectN {
		t.Fatalf("the SealedUnmarked row: proposals %v", marks)
	}
}
```

- [ ] **Step 4: Run the new tests and see them fail**

Run: `W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"; cd "$W" && go test ./fsm/ ./orchestrator/ -run 'TestOwnerAwareSelection|TestMarkReplayingRefusesAnEligibleRingBelowThree|TestAdmissionAnswersVerifiersUnavailableBelowThreeEligible|TestVerifiersUnavailable|TestWorkSetDropsAnUnmarkableBlock|TestRestoreRefusesAnEligibleRing|TestRestoreRefusesABlockMarkedAfterTheCutoverWithoutARing|TestBlockVerificationServesTheEligibleRing|TestOrchestratorProposesNoMarkReplayingForAnUnmarkableBlock' -count=1`

Expected: both test packages fail to build. The fsm errors begin `fsm/selection_stage3_test.go:…: ws.Unmarkable undefined (type WorkSet has no field or method Unmarkable)` and continue with `undefined: initialVerifierSet`, `undefined: blockSeededPosition`, `f.eligibleRingLocked undefined (type *FSM has no field or method eligibleRingLocked)`, `bv.EligibleRing undefined (type *BlockVerification has no field or method EligibleRing)` and `f.poolRingLocked undefined (type *FSM has no field or method poolRingLocked)`, then `too many errors` and `FAIL	github.com/sentioxyz/arbiter/fsm [build failed]`; the orchestrator errors are `orchestrator/dispatch_test.go:…: ws.Unmarkable undefined (type fsm.WorkSet has no field or method Unmarkable)` (twice) and `unknown field Unmarkable in struct literal of type fsm.WorkSet`, then `FAIL	github.com/sentioxyz/arbiter/orchestrator [build failed]`. Any other error means a seam of Step 1 differs from its contract: stop and reconcile it first.

- [ ] **Step 5: Add the selection record to `BlockVerification`**

In `fsm/state.go`, replace the last two fields of `BlockVerification` and its closing brace (`:277-279`):

```go
	Finality      bool                                 `json:"finality,omitempty"`
	LastMergeable bool                                 `json:"last_mergeable,omitempty"`
}
```

with:

```go
	Finality      bool                                 `json:"finality,omitempty"`
	LastMergeable bool                                 `json:"last_mergeable,omitempty"`
	// EligibleRing, RingPosition and MarkedCursorBlock are the stage-3
	// selection record (housegate spec 2026-10-14 §7.3), written by
	// MarkReplaying once the owner_aware_selection switch is on: the sorted
	// ring the verifier set was drawn from, the last ring position used, and
	// the committed table-registry cursor block at the marking (the base of
	// Plan S3-E3's re-selection timeout). A block marked before the switch
	// keeps all three empty, and omitempty leaves its record byte-identical.
	EligibleRing      []string `json:"eligible_ring,omitempty"`
	RingPosition      uint32   `json:"ring_position,omitempty"`
	MarkedCursorBlock uint64   `json:"marked_cursor_block,omitempty"`
}
```

- [ ] **Step 6: Add the eligible ring, the block-seeded position and the admission count**

In `fsm/select.go`, insert the following before the line `// ownerSourceLocked is a statement's source once si_indexers is committed` (`:78`), that is after the closing brace of `selectVerifiers` (`:76`) and the blank line after it; the block ends with its own blank line, so one blank line separates it from that comment (the file already imports `fmt`, `sort`, `strconv`, `replay` and `arbiter`):

```go
// poolRingLocked is the network pool's ring (housegate spec 2026-10-14 §7.3):
// selectVerifiers' ring, the Active VERIFIER nodes minus sources sorted by
// NodeID, restricted to the nodes for which poolVerifierLocked holds, that is
// those whose listed verifiers entry has no host_indexer_id. With no
// host-bound entry listed it is exactly selectVerifiers' ring: after the
// signed-claims activation every registered, non-evicted VERIFIER is listed
// under its registered key (signedClaimsMembershipLocked, rule 6).
func (f *FSM) poolRingLocked(sources []string) []string {
	excluded := make(map[string]bool, len(sources))
	for _, s := range sources {
		excluded[s] = true
	}
	var ring []string
	for _, id := range f.activeNodesWithRole(arbiter.NodeRoleVerifier, excluded) {
		if f.poolVerifierLocked(id) {
			ring = append(ring, id)
		}
	}
	return ring
}

// eligibleRingLocked is the ring a block of owner whose sources are sources
// draws its verifier set from once owner_aware_selection is on (spec §7.3,
// §10.4 step 3). Admission's VERIFIERS_UNAVAILABLE count, MarkReplaying and
// the WorkSet's unmarkable rule all call it, so their filters never differ.
// No database has a group before the placement switch, so the ring is the
// pool's: a host-bound entry, one hosted by owner included, enters no ring
// (clarification K68). Plan S3-E3 Task 73 adds the group half.
func (f *FSM) eligibleRingLocked(owner uint64, sources []string) []string {
	return f.poolRingLocked(sources)
}

// blockRingLocked is the eligible ring of the sealed block bv records, as
// MarkReplaying and the WorkSet's unmarkable rule both compute it:
// eligibleRingLocked of the header's track owner over the block's sources.
// Every block sealed since the tracks activation has a track; an earlier one
// was safe, so marked, before that activation, and would draw from the pool
// alone. Plan S3-E3 Task 73 passes the block's recorded group seal here.
func (f *FSM) blockRingLocked(bv *BlockVerification) []string {
	owner, ok := f.blockTrackLocked(bv.BlockSeq)
	if !ok {
		return f.poolRingLocked(bv.SourceNodes)
	}
	return f.eligibleRingLocked(owner, bv.SourceNodes)
}

// ringStart is the block-seeded start in a ring of n entries for block
// blockSeq: selectVerifiers' frozen VerifierSelectSeedPrefix derivation
// (arbiter design §7.1). n is at least 1.
func ringStart(blockSeq uint64, n int) int {
	return int(u64FromDigest(replay.DigestString(arbiter.VerifierSelectSeedPrefix+strconv.FormatUint(blockSeq, 10))) % uint64(n))
}

// blockSeededPosition is the last ring position MarkReplaying uses for block
// blockSeq in a ring of n entries: the block-seeded start plus
// VerifierSelectN-1, so the set covers the three consecutive positions from
// the start, as selectVerifiers' does. Restore recomputes it.
func blockSeededPosition(blockSeq uint64, n int) uint32 {
	return uint32((ringStart(blockSeq, n) + VerifierSelectN - 1) % n)
}

// initialVerifierSet is the verifier set ring yields at the last ring
// position position: the entries at position-2, position-1 and position,
// cyclically, sorted. It is nil for a ring below VerifierSelectN or a
// position outside the ring, which no caller passes. Plan S3-E3 Task 74
// advances the position with stored re-selections.
func initialVerifierSet(ring []string, position uint32) []string {
	n := len(ring)
	if n < VerifierSelectN || int(position) >= n {
		return nil
	}
	out := make([]string, 0, VerifierSelectN)
	for back := VerifierSelectN - 1; back >= 0; back-- {
		out = append(out, ring[(int(position)+n-back)%n])
	}
	sort.Strings(out)
	return out
}

// verifiersUnavailableLocked is the VERIFIERS_UNAVAILABLE message for a
// statement of owner bound to source, or "" (spec §7.3): with
// owner_aware_selection on, the eligible ring its block would be marked from
// holds fewer than VerifierSelectN verifiers. source is the block's only
// source: after the signed-claims activation every statement of owner is
// bound to owner's SNode (ownerSourceLocked), and after the tracks
// activation a block holds one owner's statements.
func (f *FSM) verifiersUnavailableLocked(owner uint64, source string) string {
	if !f.stage3SwitchOnLocked(Stage3SwitchOwnerAwareSelection) {
		return ""
	}
	var sources []string
	if source != "" {
		sources = []string{source}
	}
	if eligible := len(f.eligibleRingLocked(owner, sources)); eligible < VerifierSelectN {
		return fmt.Sprintf("storage-integrity verifiers unavailable for indexer %d: %d eligible, at least %d required", owner, eligible, VerifierSelectN)
	}
	return ""
}

```

- [ ] **Step 7: Draw `MarkReplaying`'s set from the eligible ring once the switch is on**

In `fsm/apply.go`, in `applyMarkReplaying`, replace the selection (`:439-443`):

```go
	set := f.selectVerifiers(c.BlockSeq, bv.SourceNodes)
	if set == nil {
		return Rejected{Reason: "active non-source verifier pool below 3 (§7.4 floor)"}
	}
	bv.VerifierSet = set
```

with:

```go
	var set []string
	if f.stage3SwitchOnLocked(Stage3SwitchOwnerAwareSelection) {
		// Owner-aware selection (housegate spec 2026-10-14 §7.3, §10.4 step
		// 3): three consecutive positions of the block's eligible ring from
		// today's block-seeded start; the ring, the last position used and
		// the committed registry cursor block are stored with the set.
		// Nothing is written before the floor check.
		ring := f.blockRingLocked(bv)
		if len(ring) < VerifierSelectN {
			return Rejected{Reason: "eligible verifier ring below 3 (spec §7.3 floor)"}
		}
		position := blockSeededPosition(c.BlockSeq, len(ring))
		set = initialVerifierSet(ring, position)
		bv.EligibleRing, bv.RingPosition = ring, position
		if reg := f.st.TableRegistry; reg != nil {
			bv.MarkedCursorBlock = reg.Cursor.BlockNumber
		}
	} else {
		set = f.selectVerifiers(c.BlockSeq, bv.SourceNodes)
		if set == nil {
			return Rejected{Reason: "active non-source verifier pool below 3 (§7.4 floor)"}
		}
	}
	bv.VerifierSet = set
```

- [ ] **Step 8: Answer `VERIFIERS_UNAVAILABLE` right after `SOURCE_UNAVAILABLE`**

In `fsm/admission.go`, replace the end of the owner-source block (`:144-147`):

```go
		if source, unavailable = f.ownerSourceLocked(live); unavailable != "" {
			return SubmitResult{Code: arbiter.AdmissionCodeSourceUnavailable, Message: unavailable}
		}
	}
```

with:

```go
		if source, unavailable = f.ownerSourceLocked(live); unavailable != "" {
			return SubmitResult{Code: arbiter.AdmissionCodeSourceUnavailable, Message: unavailable}
		}
	}
	// 5a'. Verifier availability (housegate spec 2026-10-14 §7.3): once
	// owner_aware_selection is on, a statement whose block would find fewer
	// than three eligible verifiers is refused with the retryable
	// VERIFIERS_UNAVAILABLE, right after the source check and before the lane
	// budget, the owner-switch seal and the only state mutation, so its
	// client_seq stays unspent. The count is MarkReplaying's own filter
	// (eligibleRingLocked); the switch needs the tracks activation, so owner
	// is set.
	if unavailable := f.verifiersUnavailableLocked(owner, source); unavailable != "" {
		return SubmitResult{Code: arbiter.AdmissionCodeVerifiersUnavailable, Message: unavailable}
	}
```

- [ ] **Step 9: Add `WorkSet.Unmarkable`**

In `fsm/reads_work_types.go`, replace the end of `WorkSet` (`:91-94`):

```go
	// Tracks has one row per enrolled indexer, sorted by indexer id, after
	// the tracks activation; nil before it.
	Tracks []TrackWork
}
```

with:

```go
	// Tracks has one row per enrolled indexer, sorted by indexer id, after
	// the tracks activation; nil before it.
	Tracks []TrackWork
	// Unmarkable lists, once owner_aware_selection is on, every sealed,
	// claim-complete, unmarked block whose eligible ring holds fewer than
	// VerifierSelectN verifiers (housegate spec 2026-10-14 §7.3):
	// MarkReplaying would refuse it, so it is left out of SealedUnmarked and
	// the orchestrator proposes nothing for it. It is recomputed on every
	// read, so the block returns to SealedUnmarked once its ring reaches
	// three. Nil while the switch is off.
	Unmarkable []uint64
}
```

- [ ] **Step 10: Leave an unmarkable block out of `SealedUnmarked`**

In `fsm/reads_work.go`, in `WorkSet()`, replace the head of the block loop (`:109-111`):

```go
	for idx := range f.st.Blocks {
		blockSeq := uint64(idx + 1)
		stmts := f.blockStatements(blockSeq)
```

with:

```go
	selection := f.stage3SwitchOnLocked(Stage3SwitchOwnerAwareSelection)
	for idx := range f.st.Blocks {
		blockSeq := uint64(idx + 1)
		stmts := f.blockStatements(blockSeq)
```

In `fsm/reads_work.go`, replace the unmarked branch's classification (`:132-136`):

```go
			if waiting {
				ws.AwaitingRC = append(ws.AwaitingRC, blockSeq)
			} else {
				ws.SealedUnmarked = append(ws.SealedUnmarked, blockSeq)
			}
```

with:

```go
			switch {
			case waiting:
				ws.AwaitingRC = append(ws.AwaitingRC, blockSeq)
			case selection && len(f.blockRingLocked(bv)) < VerifierSelectN:
				// MarkReplaying would refuse it (spec §7.3): it waits out of
				// SealedUnmarked until its eligible count changes.
				ws.Unmarkable = append(ws.Unmarkable, blockSeq)
			default:
				ws.SealedUnmarked = append(ws.SealedUnmarked, blockSeq)
			}
```

- [ ] **Step 11: Carry the record in `BlockVerificationView`**

In `fsm/reads_verification.go`, replace the end of the `BlockVerificationView` type (`:55-58`):

```go
	Anchor           *arbiter.AnchorRef
	Finality         bool
	LastMergeable    bool
}
```

with:

```go
	Anchor           *arbiter.AnchorRef
	Finality         bool
	LastMergeable    bool
	// EligibleRing, RingPosition and MarkedCursorBlock are the block's stored
	// stage-3 selection record (BlockVerification), empty for a block marked
	// before the owner_aware_selection switch.
	EligibleRing      []string
	RingPosition      uint32
	MarkedCursorBlock uint64
}
```

In `fsm/reads_verification.go`, in `(*FSM).BlockVerificationView`, replace (`:99`):

```go
	view.Finality, view.LastMergeable = bv.Finality, bv.LastMergeable
```

with:

```go
	view.Finality, view.LastMergeable = bv.Finality, bv.LastMergeable
	view.EligibleRing = cloneStrings(bv.EligibleRing)
	view.RingPosition, view.MarkedCursorBlock = bv.RingPosition, bv.MarkedCursorBlock
```

- [ ] **Step 12: Add the restore rules**

Append to the end of `fsm/stage3_restore.go` (Task 66's file, which already imports `fmt` and `slices`):

```go
// verificationHasStage3Fields reports a verification record carrying any
// field of the stage-3 selection record; stage3FieldsPresent refuses one
// under a pre-v21 version byte.
func verificationHasStage3Fields(bv *BlockVerification) bool {
	return bv != nil && (len(bv.EligibleRing) > 0 || bv.RingPosition != 0 || bv.MarkedCursorBlock != 0)
}

// validateStage3Selection checks every stored selection record against the
// rules MarkReplaying follows (housegate spec 2026-10-14 §7.3, plan S3-E2
// contract §47): a record exists only with the owner_aware_selection switch
// and a verifier set; its ring is sorted, unique and at least
// VerifierSelectN long; its position is the block-seeded one; the ring yields
// the stored set at that position; its cursor block is one the committed
// registry cursor has reached; and every block sealed after the switch's
// cutover and marked carries one. It does not re-derive which nodes were
// eligible, because node status may change after the marking. Blocks are
// visited in seq order, so every replica names the same corruption. Plan
// S3-E3 Task 74 extends the position and set rules with stored
// re-selections.
func validateStage3Selection(st *State) error {
	on := stage3SwitchValue(st.Params.Stage3, Stage3SwitchOwnerAwareSelection)
	var cutoverBlock uint64
	if on {
		cutover, ok := ownerAwareSelectionCutover(st)
		if !ok {
			// validateStage3Activation, which runs first, refuses this.
			return fmt.Errorf("stage3 switch %s is on without a cutover record", Stage3SwitchOwnerAwareSelection)
		}
		cutoverBlock = cutover.BlockSeq
	}
	var cursor uint64
	if st.TableRegistry != nil {
		cursor = st.TableRegistry.Cursor.BlockNumber
	}
	for seq := uint64(1); seq <= uint64(len(st.Blocks)); seq++ {
		bv := st.Verifications[seq]
		if bv == nil {
			continue // validateLatchedConsensus refuses a block without its record
		}
		if !verificationHasStage3Fields(bv) {
			if on && seq > cutoverBlock && len(bv.VerifierSet) > 0 {
				return fmt.Errorf("block %d was sealed after the owner_aware_selection cutover (block %d) and marked without an eligible ring", seq, cutoverBlock)
			}
			continue
		}
		switch {
		case !on:
			return fmt.Errorf("block %d carries an eligible ring without the owner_aware_selection switch", seq)
		case len(bv.VerifierSet) == 0:
			return fmt.Errorf("block %d carries an eligible ring but no verifier set", seq)
		case len(bv.EligibleRing) < VerifierSelectN || !ringIsSortedAndUnique(bv.EligibleRing):
			return fmt.Errorf("block %d eligible ring must be sorted, unique and hold at least 3 verifiers", seq)
		}
		if want := blockSeededPosition(seq, len(bv.EligibleRing)); bv.RingPosition != want {
			return fmt.Errorf("block %d ring position %d is not the block-seeded position %d", seq, bv.RingPosition, want)
		}
		if !slices.Equal(bv.VerifierSet, initialVerifierSet(bv.EligibleRing, bv.RingPosition)) {
			return fmt.Errorf("block %d verifier set is not the ring's three positions ending at %d", seq, bv.RingPosition)
		}
		if bv.MarkedCursorBlock > cursor {
			return fmt.Errorf("block %d marked cursor block %d is beyond the registry cursor %d", seq, bv.MarkedCursorBlock, cursor)
		}
	}
	return nil
}

// ownerAwareSelectionCutover is the recorded cutover of the
// owner_aware_selection switch.
func ownerAwareSelectionCutover(st *State) (Stage3SwitchCutover, bool) {
	if st.Stage3 == nil {
		return Stage3SwitchCutover{}, false
	}
	for _, c := range st.Stage3.Switches {
		if c.Switch == Stage3SwitchOwnerAwareSelection {
			return c, true
		}
	}
	return Stage3SwitchCutover{}, false
}

// ringIsSortedAndUnique reports a strictly ascending list of non-empty ids.
func ringIsSortedAndUnique(ring []string) bool {
	for i, id := range ring {
		if id == "" || (i > 0 && ring[i-1] >= id) {
			return false
		}
	}
	return true
}
```

- [ ] **Step 13: Wire the rules into `stage3FieldsPresent` and `validateStage3State`**

Run `W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"; cd "$W" && grep -n 'func stage3FieldsPresent\|func validateStage3State' fsm/*.go` to find both functions. Expected: one line in `fsm/snapshot.go` (`func stage3FieldsPresent(doc *snapshotDoc) bool {`) and one in `fsm/stage3_restore.go` (`func validateStage3State(st *State) error {`), where Task 66 put them; if Task 66 put `stage3FieldsPresent` elsewhere, edit it there.

In `fsm/snapshot.go`, insert immediately before the final `return false` of `func stage3FieldsPresent(` (after the incarnation check Task 69 added there):

```go
	// The owner_aware_selection switch's selection record (Task 70).
	for _, bv := range doc.Verifications {
		if verificationHasStage3Fields(bv) {
			return true
		}
	}
```

In `fsm/stage3_restore.go`, insert immediately before the final `return nil` of `func validateStage3State(` (after the `validateStage3Incarnations(st)` check Task 69 added there):

```go
	if err := validateStage3Selection(st); err != nil {
		return err
	}
```

- [ ] **Step 14: Run the new tests and see them pass**

Run: `W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"; cd "$W" && test -z "$(gofmt -l fsm orchestrator)" && bazel run //:gazelle && git diff --stat -- fsm/BUILD.bazel orchestrator/BUILD.bazel && go test ./fsm/ ./orchestrator/ -run 'TestOwnerAwareSelection|TestMarkReplayingRefusesAnEligibleRingBelowThree|TestAdmissionAnswersVerifiersUnavailableBelowThreeEligible|TestVerifiersUnavailable|TestWorkSetDropsAnUnmarkableBlock|TestRestoreRefusesAnEligibleRing|TestRestoreRefusesABlockMarkedAfterTheCutoverWithoutARing|TestBlockVerificationServesTheEligibleRing|TestOrchestratorProposesNoMarkReplayingForAnUnmarkableBlock' -race -count=1 -v 2>&1 | grep -E '^(--- |ok|FAIL)'`

Expected: `gofmt` lists nothing (if it lists a file, run `gofmt -w` on it and repeat); gazelle adds `"selection_stage3_test.go",` to the `srcs` of `go_test(name = "fsm_test")` in `fsm/BUILD.bazel`, right after `"seal_test.go",`, and changes nothing else (the diff stat names `fsm/BUILD.bazel` with one insertion: the test imports only `//accumulator`, `@arbiter_core//:arbiter-core`, `@arbiter_core//authority` and `@arbiter_core//wire`, which `fsm_test` already depends on, and `orchestrator_test` already depends on `@arbiter_core//wire`); then `--- PASS:` for `TestOwnerAwareSelectionOffSelectsByteIdentically`, `TestOwnerAwareSelectionWithOnlyThePoolPicksTodaysSet`, `TestOwnerAwareSelectionDrawsFromTheActivePoolOnly`, `TestOwnerAwareSelectionStoresTheRingThePositionAndTheCursor`, `TestMarkReplayingRefusesAnEligibleRingBelowThree`, `TestAdmissionAnswersVerifiersUnavailableBelowThreeEligible`, `TestVerifiersUnavailableIsCheckedRightAfterSourceUnavailable`, `TestVerifiersUnavailableLeavesTheCoordinateUnspent`, `TestWorkSetDropsAnUnmarkableBlockUntilTheEligibleCountChanges`, `TestRestoreRefusesAnEligibleRingThatDoesNotYieldTheVerifierSet`, `TestRestoreRefusesAnEligibleRingBeforeTheOwnerAwareSelectionSwitch`, `TestRestoreRefusesABlockMarkedAfterTheCutoverWithoutARing`, `TestBlockVerificationServesTheEligibleRing` and `TestOrchestratorProposesNoMarkReplayingForAnUnmarkableBlock`, no `--- FAIL`, and `ok` for both packages.

- [ ] **Step 15: Run the whole fsm and orchestrator packages, the goldens and the fsm red lines**

```bash
W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"; cd "$W" || exit 1
go test ./fsm/ ./orchestrator/ -race -count=1
go test ./fsm/ -run 'TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims|TestPreActivationHistoryIsByteIdenticalAcrossTracks|TestL3CommitmentPreimagesAreFrozen|TestL3TrackHeaderPreimagesAreFrozen|TestL3TransitionHeaderPreimageIsFrozen|TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease|TestStage3V21PlainContainerGolden|TestStage3V21InternedContainerGolden|TestStage3ActivationWithEverySwitchOffChangesNoBehaviour|TestPurgeSwitchOffKeepsEveryIncarnationUnscoped|TestEvictionVoidingOffKeepsTodaysAdmission' -count=1 -v 2>&1 | grep -E '^(--- |ok|FAIL)'
go vet ./fsm/ ./orchestrator/
grep -rn 'time\.Now' fsm/
git status --porcelain fsm/testdata
```

Expected: `ok  	github.com/sentioxyz/arbiter/fsm` and `ok  	github.com/sentioxyz/arbiter/orchestrator` (every earlier task's tests included); `--- PASS:` for each of the eleven listed tests and `ok`; `go vet` prints nothing; the `time.Now` grep prints nothing; `git status --porcelain fsm/testdata` prints nothing (no golden was rewritten: `TestStage3V21PlainContainerGolden` and `TestStage3V21InternedContainerGolden` compare against their frozen bytes, which carry no selection field because `stage3V21GoldenFSM` turns no switch on). A failing golden or switch-off test means a new branch runs with its switch off: stop and find it.

- [ ] **Step 16: Commit the FSM half**

```bash
W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"
git -C "$W" add fsm orchestrator
git -C "$W" status --porcelain
git -C "$W" commit -m "feat(stage3): owner-aware selection from the pool's eligible ring (Task 70)

Switch 3 of housegate spec 2026-10-14 §10.4 (owner_aware_selection,
§7.3). With the switch on, MarkReplaying draws the verifier set from
the block's eligible ring, in this plan the Active network pool minus
the block's source, so no host-bound entry enters a ring (K68), at
today's block-seeded start, and stores the ring, the last ring position
used and the committed registry cursor block in BlockVerification.
Admission answers VERIFIERS_UNAVAILABLE right after SOURCE_UNAVAILABLE
while that ring holds fewer than three verifiers, with the client_seq
unspent and no owner-switch seal prepared; such a sealed block moves
from the WorkSet's SealedUnmarked to Unmarkable until its ring holds
three, so the orchestrator proposes nothing for it. Restore checks
every stored record (validateStage3Selection) and refuses one in a
pre-v21 container. With the switch off every path is today's.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: `git status --porcelain` lists only staged entries (`M ` for `fsm/BUILD.bazel`, `fsm/admission.go`, `fsm/apply.go`, `fsm/reads_verification.go`, `fsm/reads_work.go`, `fsm/reads_work_types.go`, `fsm/select.go`, `fsm/snapshot.go`, `fsm/stage3_restore.go`, `fsm/state.go`, `orchestrator/dispatch_test.go`; `A ` for `fsm/selection_stage3_test.go`), and the commit succeeds.

- [ ] **Step 17: Make the server converter test require the three fields**

Delete the three pin-fallout entries this task owns:

```bash
W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"; cd "$W" || exit 1
F="$(grep -l 'stage3PendingProtoFields = map' server/*_test.go)"; echo "$F"
sed -i '' -E '/"BlockVerification\.(eligible_ring|ring_position|marked_cursor_block)":/d' "$F" && gofmt -w "$F"
grep -rnE 'BlockVerification\.(eligible_ring|ring_position|marked_cursor_block)' server/
```

Expected: one file name (`server/safestate_stage3_test.go`, contract §44), then nothing from the last `grep`.

In `server/safestate_stage3_test.go`, in `stage3BlockVerificationView()`, replace (`:94-97`):

```go
		Anchor:   &arbiter.AnchorRef{L3BlockHash: "0xchain", StateRoot: "0xstate", L2TxRef: "0xtx", L2BlockNumber: 77, DARef: "da-1"},
		Finality: true, LastMergeable: true,
	}
}
```

with:

```go
		Anchor:   &arbiter.AnchorRef{L3BlockHash: "0xchain", StateRoot: "0xstate", L2TxRef: "0xtx", L2BlockNumber: 77, DARef: "da-1"},
		Finality: true, LastMergeable: true,
		// The stage-3 selection record (plan S3-E2 Task 70).
		EligibleRing: []string{"v1", "v2", "v3"}, RingPosition: 1, MarkedCursorBlock: 160,
	}
}
```

In `server/safestate_stage3_test.go`, in `TestBlockVerificationToPBCarriesEveryField`, replace the last line of `want` and its closing brace (`:157-158`):

```go
		VerdictEvaluated: true, Quorum: true, Anchor: wire.AnchorRefToPB(*view.Anchor), Finality: true, LastMergeable: true,
	}
```

with:

```go
		VerdictEvaluated: true, Quorum: true, Anchor: wire.AnchorRefToPB(*view.Anchor), Finality: true, LastMergeable: true,
		EligibleRing: []string{"v1", "v2", "v3"}, RingPosition: 1, MarkedCursorBlock: 160,
	}
```

The test's top-level check stays Task 64's `requireEveryFieldExcept(t, want)`, which now requires `eligible_ring`, `ring_position` and `marked_cursor_block` to be set, while `group_key`, `group_members`, `group_fill`, `group_databases`, `reselections` and `reselections_at_latch` stay pending for Plan S3-E3.

- [ ] **Step 18: Run it and see it fail**

Run: `W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"; cd "$W" && test -z "$(gofmt -l server)" && go test ./server/ -run 'TestBlockVerificationToPBCarriesEveryField' -count=1`

Expected: `--- FAIL: TestBlockVerificationToPBCarriesEveryField` with a line that begins `safestate_stage3_test.go:…: blockVerificationToPB = header:{l3_block_seq:5` and carries `want` (the converter drops the three fields, so `proto.Equal` fails; protobuf's text format may put one or two spaces between fields), then `FAIL	github.com/sentioxyz/arbiter/server`.

- [ ] **Step 19: Serve the three fields**

In `server/safestate_stage3.go`, in `blockVerificationToPB`, replace the end of the `out` literal (`:144-146`; the file already imports `slices`):

```go
		Finality:         v.Finality,
		LastMergeable:    v.LastMergeable,
	}
```

with:

```go
		Finality:         v.Finality,
		LastMergeable:    v.LastMergeable,
		// The stage-3 selection record (housegate spec 2026-10-14 §7.3),
		// absent on a block marked before owner_aware_selection.
		EligibleRing:      slices.Clone(v.EligibleRing),
		RingPosition:      v.RingPosition,
		MarkedCursorBlock: v.MarkedCursorBlock,
	}
```

- [ ] **Step 20: Run the server package and see it pass**

Run: `W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"; cd "$W" && test -z "$(gofmt -l server)" && go test ./server/ -run 'TestBlockVerificationToPBCarriesEveryField|TestL3BlockHeaderMirrorsProto|TestTableRegistryMirrorsProto|TestGetBlockVerification' -race -count=1 -v 2>&1 | grep -E '^(--- |ok|FAIL)' && go test ./server/ -race -count=1`

Expected: `--- PASS:` for `TestL3BlockHeaderMirrorsProto`, `TestBlockVerificationToPBCarriesEveryField`, `TestGetBlockVerificationServesTheSealedBlock`, `TestGetBlockVerificationNotFoundAndDataLoss` and `TestTableRegistryMirrorsProto`, then `ok  	github.com/sentioxyz/arbiter/server` twice (the named tests, then the whole package).

- [ ] **Step 21: Commit the server half**

```bash
W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"
git -C "$W" add server
git -C "$W" status --porcelain
git -C "$W" commit -m "feat(stage3): serve the eligible ring in GetBlockVerification (Task 70)

blockVerificationToPB sets BlockVerification.eligible_ring,
ring_position and marked_cursor_block (arbiter-proto fields 14-16)
from the FSM's stored selection record, and stage3PendingProtoFields
drops the three entries the pin left for this task.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: `M  server/safestate_stage3.go` and `M  server/safestate_stage3_test.go` (the map is in the latter), then the commit succeeds.

- [ ] **Step 22: Full verification of the branch before the push**

```bash
W="$HOME/src/remotesrc/claude/arbiter/multi-source-s3e"; cd "$W" || exit 1
test -z "$(git ls-files -z '*.go' | xargs -0 gofmt -l)" && echo gofmt-clean
bazel run //:gazelle && git status --porcelain
bazel build //... && bazel test //... --test_output=errors
bazel query 'deps(//fsm:fsm, 1)' | grep com_github_sentioxyz_arbiter_proto; grep -rn 'time\.Now' fsm/
BASE="$(git merge-base origin/main HEAD)"
git diff --name-status "$BASE" HEAD -- fsm/testdata fsm/l3_track_golden_test.go accumulator/testdata
git log --reverse --format=%s "$BASE"..HEAD
git log --format=%s "$BASE"..HEAD | grep -vcE '\(Task (6[4-9]|70)\)$'
```

Expected: `gofmt-clean`; gazelle changes nothing (`git status --porcelain` prints nothing); every target builds and every test passes; the red-line `grep`s print nothing; the `testdata` diff lists exactly `A	fsm/testdata/stage3_v21_interned.snapshot` and `A	fsm/testdata/stage3_v21_plain.snapshot` (the two frozen fixtures Tasks 66 and 67 added) and nothing for `fsm/l3_track_golden_test.go` or `accumulator/testdata`; the branch log lists, oldest first, `chore(deps): pin arbiter-proto, housegate and arbiter-core for stage 3 P5 (Task 64)` and every commit of Tasks 64–70, the two of this task last; the final count is `0` (every commit names its task). Any other line stops the push.

- [ ] **Step 23: Push the P5 branch (requires explicit user confirmation)**

Remote write (push): needs explicit user confirmation in chat before running. It publishes branch `urwt/claude/multi-source-s3e` (the commits of Tasks 64–70) to `github.com/sentioxyz/arbiter`. It opens no PR, merges nothing, creates no tag and starts no workflow (`ci.yml` runs on pushes to `main` and on pull requests, `cut-release.yml` only by `workflow_dispatch`); K65 keeps the branch unreleased until Plan S3-E4 Task 78, and Plan S3-E3 continues on this worktree.

Confirm before running. Then, from fish or any shell (the two lines are the same in both):

```bash
git -C ~/src/remotesrc/claude/arbiter/multi-source-s3e push -u origin HEAD
git -C ~/src/remotesrc/claude/arbiter/multi-source-s3e rev-parse HEAD '@{u}'
```

Expected: `* [new branch]      HEAD -> urwt/claude/multi-source-s3e` and `branch 'urwt/claude/multi-source-s3e' set up to track 'origin/urwt/claude/multi-source-s3e'.`, then the same commit hash twice (the local head and its upstream). If the remote refuses the push, or the branch already exists on the remote with other commits, stop and report to the user; never force-push.

---

## Appendix A: binding contract (continues Plans S3-A, S3-B and S3-E1)

Plan S3-A's Appendix A (§0–§6), Plan S3-B's (§7–§15) and Plan S3-E1's (§38–§43) stay binding; this appendix continues the numbering, so `contract §N` means the same thing in every Stage 3 plan. Every name below is binding verbatim for Tasks 64–70 and for Plans S3-E3 and S3-E4, which extend the seams of §44 and never rename what is listed here. A task that needs a name not listed here adds it in its own section and records it in Appendix B.

### 44. Activation model and seams (read first) (Tasks 64–70)

- **Before the activation.** With `Params.Stage3 == nil` nothing of this plan runs: every new Apply branch is guarded by `f.stage3ActiveLocked()` or `f.stage3SwitchOnLocked(<switch>)` and falls through to today's code; `snapshotWriteVersion` returns 20 or lower exactly as at the base; no new snapshot member, state field or registry field is written (all are `omitempty`, nil or zero). A replay of any pre-stage3 history under the branch and under `ARBITER_TAG_P4` gives identical state digests and the same v20 container, each build restoring the other's (Task 67's offline replay).
- **The activation (spec §10.3).** One authority-signed `UpdateConsensusParams` whose `update.stage3` is set for the first time, with every switch false, `strict_copies` false and `min_safe_copies` 1. Apply judges it by the stage-2 drain gate (unchanged) and needs no empty open block and no global prefix; it creates `State.Stage3`. From then on the writer writes v21; nothing else changes while every switch is off (`TestStage3ActivationWithEverySwitchOffChangesNoBehaviour`).
- **Switches 1–3 in this plan.** `purge` (Task 69), `eviction_voiding` (Task 68), `owner_aware_selection` (Task 70). `placement` (with the seal key), `reselection` and `checkpoints` are Plan S3-E3's, `narrowed_drain` Plan S3-E4's; their parameter bits, order rules and cutover records exist from Task 64, their behaviour does not. A switch whose behaviour is not built yet is still accepted by Apply on this branch; the branch is released only once Plan S3-E4 Task 78 has built every switch.
- **Seams the later plans extend (binding names, extended in place):**
  - `Stage3State` (`fsm/stage3.go`): Plans S3-E3 and S3-E4 append members, each `omitempty`; the members of §45 keep their names, tags and meaning.
  - `func (f *FSM) stage3MembershipRefusalLocked(next Params) string` (the Apply-only half): Plan S3-E3 Task 71 appends the per-group verifier floor of spec §6.1.
  - `func (f *FSM) onStage3SwitchOnLocked(name string)` (called once per switch turned on, after the new parameters are in place and before the cutover is recorded): Task 69 handles `purge`, Task 68 `eviction_voiding`; Plan S3-E3 adds `placement` (backfill, seal, cutover block) and `checkpoints`, Plan S3-E4 `narrowed_drain`.
  - `func stage3FieldsPresent(doc *snapshotDoc) bool` and `func validateStage3State(st *State) error` (`fsm/stage3_restore.go`): each later stage-3 task appends its fields to the first and its restore rule to the second.
  - `func (f *FSM) eligibleRingLocked(owner uint64, sources []string) []string`: Plan S3-E3 Task 73 adds the group half (and may change the signature in its own contract); admission, `MarkReplaying` and the WorkSet always call the same function, so their filters never differ.
  - `func initialVerifierSet(ring []string, position uint32) []string` and `validateStage3Selection`: Plan S3-E3 Task 74 extends the restore rule with stored re-selections.
  - `func (f *FSM) evictedSignerRefusalLocked(kind, nodeID string) string`: Plan S3-E3 calls it for `MemberReady` (`kind` `member_ready`) and `MemberCheckpoint` (`member_checkpoint`).
  - `TableIncarnation.PurgeMembers`: Plan S3-E3 Task 71 fills it (at record time and at every member add while the incarnation is physical, K60) and adds the restore rule that it is a subset of the group's members and past members.
- **Pin fallout (Task 64).** Pinning `PROTO_TAG_P5` adds protobuf fields that later tasks map; a mirror or every-field test that fails only because of such a field is handled, in Task 64's pin commit, by naming the field in the server test map `stage3PendingProtoFields map[string]string` (key `<Message>.<field>`, value the owning task, e.g. `"TableIncarnation.scoped_keeper_path": "Task 69"`), which `TestTableRegistryMirrorsProto`, `TestL3BlockHeaderMirrorsProto` and the test helper `requireEveryFieldExcept(t *testing.T, m proto.Message)` (Plan S3-A's `requireEveryField` minus the map's fields of that message) consult; the entries expected at the base (each added only when a test actually fails because of it): `TableIncarnation.scoped_keeper_path`, `TableIncarnation.purge_members` (Task 69), `BlockVerification.eligible_ring`, `BlockVerification.ring_position`, `BlockVerification.marked_cursor_block` (Task 70), `BlockVerification.group_key`, `group_members`, `group_fill`, `group_databases` (Plan S3-E3 Task 72), `BlockVerification.reselections`, `reselections_at_latch` (Plan S3-E3 Task 74), `L3BlockTrack.group_key` (Plan S3-E3 Task 72), `TableRegistrySnapshot.stage3` (Task 65), `TableRegistrySnapshot.groups` (Plan S3-E3 Task 71), `TableRegistrySnapshot.member_checkpoint_seqs` (Plan S3-E3 Task 75). The owning task deletes its entries; Plan S3-E4 Task 78 adds `TestNoStage3ProtoFieldIsPending` (the map is empty). Any other failure after the pin stops Task 64.

### 45. Parameters, activation and the stage3 gate (Tasks 64–65)

- **State and parameters** (`fsm/state.go`, new `fsm/stage3.go`):
  - `Params.Stage3 *arbiter.Stage3Params` with tag `json:"stage3,omitempty"`, declared after `TracksEnabled`; `State.Stage3 *Stage3State`, declared after `Tracks`; both nil until the activation.
  - `type Stage3State struct{ ActivationEpoch, ActivationBlockSeq, ActivationPromotionSeq uint64; Switches []Stage3SwitchCutover }` with JSON tags `activation_epoch`, `activation_block_seq`, `activation_promotion_seq`, `switches,omitempty`. `ActivationEpoch` is the consensus epoch the activation produces (the 1-based position of the first `ConsensusUpdates` entry whose `Update.Stage3` is set); `ActivationBlockSeq` and `ActivationPromotionSeq` are `len(Blocks)` and `PromotionSeq` when it applies.
  - `type Stage3SwitchCutover struct{ Switch string; Epoch, BlockSeq, PromotionSeq, IncarnationSeq uint64 }` with JSON tags `switch`, `epoch`, `block_seq`, `promotion_seq`, `incarnation_seq,omitempty`: one per switch turned on, appended in §10.4 order; `Epoch` is the epoch the switch-on update produces; `BlockSeq` and `PromotionSeq` are `len(Blocks)` and `PromotionSeq` after every mutation of that Apply (for `placement` after its seal, Plan S3-E3); `IncarnationSeq` is set only for `purge`: the sequence the next registry incarnation gets, `uint64(len(TableRegistry.Incarnations)) + 1`.
  - Switch names (exported constants, §10.4 order): `Stage3SwitchPurge = "purge"`, `Stage3SwitchEvictionVoiding = "eviction_voiding"`, `Stage3SwitchOwnerAwareSelection = "owner_aware_selection"`, `Stage3SwitchPlacement = "placement"`, `Stage3SwitchReselection = "reselection"`, `Stage3SwitchCheckpoints = "checkpoints"`, `Stage3SwitchNarrowedDrain = "narrowed_drain"`; `func Stage3Switches() []string` (a fresh slice in that order); `func stage3SwitchValue(p *arbiter.Stage3Params, name string) bool` (false for nil `p`; panics on an unknown name, a programming error).
  - Helpers: `func (f *FSM) stage3ActiveLocked() bool` (`Params.Stage3 != nil`), `func (f *FSM) Stage3Active() bool`, `func (f *FSM) stage3SwitchOnLocked(name string) bool`, `func (f *FSM) Stage3SwitchOn(name string) bool`, `func cloneStage3Params(p *arbiter.Stage3Params) *arbiter.Stage3Params` (deep copy, nil stays nil; used by `cloneParams` and `normalizeConsensusParams`), `func (f *FSM) verifierEntryLocked(nodeID string) (arbiter.VerifierEntry, bool)`, `func (f *FSM) poolVerifierLocked(nodeID string) bool` (a non-evicted node with the VERIFIER role whose listed entry has no `host_indexer_id`: the network pool).
  - `cloneVerifiers` (`fsm/signed_claims.go:93-102`) copies every field of `arbiter.VerifierEntry`, the `HostIndexerID` pointer and the key bytes detached. `listsVerifier` stays (node id, key).
- **Params-only rules** (`verifyConsensusTransition`, after the `tracks_enabled` rule at `fsm/consensus_updates.go:240-246` and before the `max_writers` defence; replayed by `validateConsensusHistory`): `func verifyStage3Transition(previous, next Params, update *arbiter.Stage3Params) (*arbiter.Stage3Params, error)` then `func verifyHostBoundVerifiers(previous, next Params, genesisID string) error`. Refusals, verbatim, in this order:
  - `consensus update cannot set stage3 before the tracks activation`
  - `consensus update setting stage3 must leave every switch off, strict_copies false and min_safe_copies 1`
  - `consensus update must carry stage3 once set`
  - `consensus update stage3 needs group_size and min_safe_copies of at least 1 and 1 <= lag_low < max_lag_publications` (defence in depth over Plan S3-E1's normalizer)
  - `consensus update cannot turn stage3 switch %s off`
  - `consensus update cannot turn stage3 strict_copies off`
  - `consensus update cannot lower stage3 min_safe_copies (%d -> %d)`; `consensus update cannot lower stage3 group_size (%d -> %d)`
  - `consensus update may turn on at most one stage3 switch, got %s` (the names joined by `, `)
  - `consensus update cannot turn on stage3 switch %s before %s` (the first earlier switch that is off in the previous parameters)
  - `consensus update cannot raise stage3 min_safe_copies above 1 or set strict_copies before the checkpoints and narrowed_drain switches`
  - `consensus update stage3 indexer_failure_domains names indexer %d, which is not an si_indexers entry`
  - `consensus update stage3 indexer_failure_domains has no domain for indexer %d` (once `placement` is on in the next parameters)
  - per entry with a `host_indexer_id` (every refusal names the entry's `node_id`): `consensus update cannot list host-bound verifier %s before the stage3 owner_aware_selection switch is committed` (K68); `consensus update cannot change verifier %s host_indexer_id`; `consensus update verifier %s host_indexer_id %d is not an si_indexers entry`; `consensus update verifier %s replica_signer must be a non-zero lowercase 0x-prefixed 20-byte address`; `consensus update verifier %s weight must be 1 to 64, got %d`; `consensus update verifier %s failure_domain %q differs from stage3 indexer_failure_domains[%d] %q` (an absent map entry is `""`); `consensus update verifier %s sets verify with shared_rmt false; such a member cannot verify before the per-operator transport (spec §6.1)`; `consensus update verifier %s consent: %v` (Plan S3-E1's `authority.VerifyGroupMemberConsent` under the host entry's `signer`, for an added entry and for one whose `ed25519_pubkey` or `replica_signer` changed); `consensus update changes verifier %s consent_jws without rotating its keys`
  - per entry without a `host_indexer_id`: `consensus update verifier %s sets host-bound fields without host_indexer_id` (any of `replica_signer`, `consent_jws`, `weight`, `failure_domain`, `verify`, `replicate`, `counts`, `shared_rmt` set), so pool entries keep their v20 bytes and the params digest of every earlier epoch is unchanged (Plan S3-E1 K58).
- **Apply-only half** (`applyUpdateConsensusParams`, right after `signedClaimsMembershipLocked` at `fsm/consensus_updates.go:51-54`): `stage3MembershipRefusalLocked(next)` refuses an added host-bound entry whose node id names any `Nodes` record, whatever its status: `consensus update verifier %s: a member node_id must be unused by every registered SNode and verifier`. Plan S3-E3 appends the per-group floor (§44).
- **Activation and cutovers** (`applyUpdateConsensusParams`): `func (f *FSM) stage3ActivationRefusalLocked() string` and `func (f *FSM) Stage3ActivationRefusal() string` return `stage3 activation requires the tracks activation` while `tracks_enabled` is not committed, else the stage-2 drain gate's own refusal string (`drainGateRefusalLocked`), else `""`; Apply runs the drain gate for every update as today, so the activation needs nothing else from state (no empty open block, no global prefix, unlike `tracksActivationRefusalLocked`). After `f.st.Params = next` and the tracks block, the activation calls `func (f *FSM) activateStage3Locked()` and every later update `func (f *FSM) recordStage3SwitchesLocked(previous *arbiter.Stage3Params)`, which, for the switch the update turns on, calls `onStage3SwitchOnLocked` and then appends its cutover (for `purge` with `IncarnationSeq`); both run before `ConsensusEpoch++`. `func (f *FSM) Stage3ActivationNodes() []string`: `PurgeNodeSet()` minus the SNodes whose si_indexers track is quarantined (`IndexerTrack.Quarantined`), sorted.
- **Named tests, Task 64** (`fsm/stage3_params_test.go`, `fsm/stage3_activation_test.go`): `TestStage3ActivationSetsEveryParameterWithEverySwitchOff`, `TestStage3ActivationRequiresTheTracksActivation`, `TestStage3ActivationRefusesASwitchStrictCopiesOrAFloorAboveOne`, `TestStage3ActivationNeedsNoEmptyOpenBlockOrGlobalPrefix`, `TestStage3ActivationRunsTheStage2DrainGate`, `TestStage3ActivationWithAQuarantinedTrack`, `TestStage3ActivationWithEverySwitchOffChangesNoBehaviour`, `TestStage3MustBeCarriedOnceSet`, `TestStage3SwitchesAndStrictCopiesAreOneWay`, `TestStage3MinSafeCopiesAndGroupSizeNeverDecrease`, `TestStage3OneSwitchPerUpdate`, `TestStage3SwitchNeedsEveryEarlierSwitchCommitted`, `TestStage3FloorAboveOneNeedsCheckpointsAndNarrowedDrain`, `TestStage3PlacementNeedsADomainForEveryIndexer`, `TestStage3FailureDomainsNameOnlyEnrolledIndexers`, `TestStage3RecordsEachSwitchCutover`, `TestStage3RulesReplayOnRestore`, `TestHostBoundVerifierRefusedBeforeOwnerAwareSelection`, `TestHostBoundVerifierRules`, `TestHostBoundVerifierConsentIsVerifiedOnlyOnAddOrKeyRotation`, `TestVerifierHostIndexerIDNeverChanges`, `TestPoolVerifierCarriesNoHostBoundFields`, `TestMemberNodeIDMustBeUnusedByEveryRegisteredNode`, `TestCloneVerifiersCopiesEveryField` (reflection over every field of `arbiter.VerifierEntry` through `cloneVerifiers`, `cloneParams` and `normalizeConsensusParams`), `TestCloneParamsCopiesStage3`, `TestStage3ActivationNodesExemptTheSNodeOfAQuarantinedTrack`; the existing golden tests pass unchanged.
- **Test fixture, Task 64** (`fsm/stage3_fixture_test.go`, used by Tasks 64–70 and Plans S3-E3/S3-E4): `stage3ActivationParams() *arbiter.Stage3Params` = `{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1, IndexerFailureDomains: [{0, "node:n0"}, {1, "node:n1"}], MaxLagPublications: 8, LagLow: 2, EvidenceTimeoutL2Blocks: 30, MaxUncleanedPartsPerPartition: 2000, MaxUncleanedPartsPerTable: 20000}`; `stage3Update(t, f) arbiter.ConsensusParamsUpdate` (`tracksUpdate` carrying the committed `stage3`); `activateStage3(t, f, a)`; `stage3FSM(t) (*FSM, *authority.Signer)` (`tracksFSM` plus the activation); `turnOnStage3Switch(t, f, a, name string)`; `stage3TestMember(host uint64) string` = `fmt.Sprintf("member-%d", host)`; `memberEntry(t, f, host uint64, node, domain string) arbiter.VerifierEntry` (all four flags true, weight 1, a fresh ed25519 key and replica signer, consent signed at `authoritytest.Iat` by the host's enrolment key); `rejectStage3Unchanged(t, f, cmd, reason)`.
- **Leader gate, Task 65** (new `server/stage3_gate.go`): `const Stage3VotersMetadataKey = "arbiter-stage3-voters"`; `func (s *Server) stage3Gate(ctx context.Context, update arbiter.ConsensusParamsUpdate) error`, called after `tracksGate` in `UpdateConsensusParams`, which also takes `membershipMu` and the read barrier when `update.Stage3 != nil && !s.d.FSM.Stage3Active()`. It passes every update except the activation; for the activation, in this order: no non-voter, `FSM.Stage3ActivationRefusal()` empty, every node of `FSM.Stage3ActivationNodes()` advertised `arbiter.Stage3Feature` in a registration applied through this leader in the current term, every voter probed at the addresses named in `arbiter-stage3-voters` reports it and the probed ids are exactly the voters. The voter probe is `func (s *Server) probeFeatureVoters(ctx context.Context, voters []string, metadataKey, feature, gate, release string) error`, which `probeTracksVoters` now calls with its own values, so every tracks refusal text stays byte-identical. Refusals (FailedPrecondition): the tracks gate's texts with `tracks gate` → `stage3 gate`, `arbiter-tracks-voters` → `arbiter-stage3-voters`, `track_manifests_v1` → `stage3_v1`, `stage-2 release` → `stage-3 release` (e.g. `stage3 gate refused: data-plane nodes %s have not registered with this leader advertising stage3_v1; restart them on a stage-3 release`), and `stage3 gate refused: %s` for the FSM refusal.
- **Registration refusal, Task 65** (`server/membership.go`, after the tracks refusal at `:53`): `isDataPlaneRegistration(req) && FSM.Stage3Active() && !slices.Contains(req.GetFeatures(), arbiter.Stage3Feature)` → FailedPrecondition `stage3 is active; this binary does not support stage-3 consensus`.
- **Mutable params and the stage-3 reads, Task 65** (`server/consensus_admin.go:107-122`, `server/table_registry.go`): `consensusMutableParams` sets `Stage3: wire.Stage3ParamsToPB(p.Stage3)`; `GetConsensusParams` sets `ConsensusParamsState.stage3_state` (Plan S3-E1 §39, field 9) from `func (f *FSM) Stage3StateView() *Stage3State` (a deep copy, nil before the activation) through `func stage3StateToPB(st *fsm.Stage3State) *pb.Stage3StateView` (`activation_epoch`, `activation_block_seq`, `activation_promotion_seq`, and per cutover `switch_name` = `Switch`, `epoch`, `cutover_block_seq` = `BlockSeq`, `cutover_promotion_seq` = `PromotionSeq`; `IncarnationSeq` is not served, by decision D33: a read view exposes a field only when a runbook or live task reads it, and no runbook or live task of Plans S3-E2 to S3-F reads the purge cutover's incarnation seq, which restore checks (`validateStage3Activation`, Task 69's `validateStage3Incarnations`) while the served `TableIncarnation.scoped_keeper_path` shows which incarnations are scoped; a later task that needs it adds it to the view in its own task and records it in its Appendix B); `TableRegistryView` gains `Stage3 *arbiter.Stage3Params` (a copy of the committed parameter) and `tableRegistryToPB` sets `TableRegistrySnapshot.stage3` (field 12) with `wire.Stage3ParamsToPB`, and every Apply that commits a changed `Params.Stage3` bumps the registry version once, so `WatchTableRegistry` followers (the members' consensus loops, Plan S3-E1 §42.3) learn each switch; `stage3PendingProtoFields` loses `TableRegistrySnapshot.stage3`. Named tests: `TestGetConsensusParamsServesTheStage3StateView`, `TestTableRegistryServesTheCommittedStage3AndBumpsTheVersion`.
- **arbiter-admin `consensus update`, Task 65** (`cmd/arbiter-admin/main.go`, new `cmd/arbiter-admin/stage3.go`): flags `--stage3` (bool: the activation; needs `--voter`), `--stage3-group-size` (activation default 3), `--stage3-network-fill-default` (activation default true), `--stage3-min-safe-copies` (activation default 1), `--stage3-strict-copies`, `--stage3-max-lag-publications`, `--stage3-lag-low`, `--stage3-evidence-timeout-l2-blocks`, `--stage3-max-uncleaned-parts-per-partition`, `--stage3-max-uncleaned-parts-per-table` (these five required with `--stage3`), `--stage3-exclude-quarantined-hosts`, `--stage3-switch NAME`, `--indexer-failure-domain 'id=N,domain=D'` (repeat for the complete map; omitted keeps the committed one), and the extended `--verifier 'node=NODE_ID,pubkey=HEX[,host=N,replica-signer=0x...,consent=PATH,domain=D,weight=W,verify=B,replicate=B,counts=B,shared-rmt=B]'` (`host` needs `replica-signer`, `consent` and `domain`; `weight` defaults to 1 and each flag to `true` when `host` is given). After the activation every omitted `--stage3-*` value keeps the committed one, and the committed `stage3` and host-bound entries are carried into every update. Go names: `const stage3VotersMetadataKey = "arbiter-stage3-voters"` (`TestStage3VotersMetadataKeyMatchesTheServer`), `type failureDomainFlags []arbiter.IndexerFailureDomain`, `func keyValuesOptional(value string, required, optional []string) (map[string]string, error)`, `func checkStage3NotActive(ctx context.Context, leader pb.ConsensusAdminClient) error`, `func stage3ExemptSNodes(params *pb.ConsensusParamsState, tracks *pb.TrackStateView) []string`, `var stage3Activation = activationGate{feature: arbiter.Stage3Feature, name: "stage3", aware: "stage3-aware"}` with the new field `activationGate.exempt func(nodeID string) bool` (a feature-book node it returns true for is skipped by `problems` and listed as `exempt (quarantined track): <id>` by `lines`). Refusals (verbatim): `--stage3 sets stage3 for the first time, but the leader has it committed; omit --stage3`; `--stage3 needs --stage3-max-lag-publications, --stage3-lag-low, --stage3-evidence-timeout-l2-blocks, --stage3-max-uncleaned-parts-per-partition and --stage3-max-uncleaned-parts-per-table`; `--stage3 needs --voter: the leader probes every voter for stage3_v1`; `--stage3-* and --indexer-failure-domain need stage3: pass --stage3 for the activation`; `--stage3-switch cannot be combined with --stage3: the activation leaves every switch off`; `--stage3-switch %q is not a switch (want purge, eviction_voiding, owner_aware_selection, placement, reselection, checkpoints, narrowed_drain)`; `--stage3-switch is given more than once; an update turns on one switch`; `--stage3-switch %s is on already`; `--verifier %s: host-bound keys (replica-signer, consent, domain, weight, verify, replicate, counts, shared-rmt) need host=N`; `--verifier %s: host=N needs replica-signer=0x..., consent=PATH and domain=D`; `--verifier %s: weight %q must be 1 to 64`; `--verifier %s: %s=%q must be true or false`; `--indexer-failure-domain id %d is given twice`. `--hold` keeps Plan S3-A's default `drain` (K6; Plan S3-E4 K79 replaces it at switch 9).
- **Named tests, Task 65:** server `TestStage3GatePassesEveryUpdateButTheActivation`, `TestStage3GateRefusesNonVoters`, `TestStage3GateRefusesTheFSMActivationRefusal`, `TestStage3GateRequiresStage3V1FromEveryDataPlaneNode`, `TestStage3GateExemptsTheSNodeOfAQuarantinedTrack`, `TestStage3GateProbesEveryVoter`, `TestTracksGateRefusalsAreUnchanged`, `TestRegisterNodeRefusesABinaryWithoutStage3V1AfterTheActivation`, `TestConsensusMutableParamsCarryStage3`, `TestUpdateConsensusParamsTakesTheMembershipBarrierForTheStage3Activation`; arbiter-admin `TestConsensusUpdateStage3SetsEveryParameterWithEverySwitchOff`, `TestConsensusUpdateStage3KeepsTheCommittedValuesWhenFlagsAreOmitted`, `TestConsensusUpdateCarriesTheCommittedStage3AndHostBoundVerifiers`, `TestConsensusUpdateStage3SwitchTurnsOnOneSwitch`, `TestConsensusUpdateStage3FlagRefusals`, `TestVerifierFlagParsesHostBoundEntries`, `TestVerifierFlagRefusesHostBoundKeysWithoutHost`, `TestIndexerFailureDomainFlag`, `TestStage3ActivationGateExemptsSNodesOfQuarantinedTracks`, `TestStage3VotersMetadataKeyMatchesTheServer`.

### 46. Snapshot v21 (Tasks 66–67)

- **Versions and framing, Task 66** (`fsm/snapshot.go`, `fsm/snapshot_count.go`): `snapshotVersionV21 = 21`; `snapshotVersion = 21` (the newest written); every version whitelist and every `ver[0] == snapshotVersion || …` predicate of `readSnapshot` (`:274`, `:292`, `:301`, `:408`) names `snapshotVersionV20` explicitly, so v20 stays readable; `snapshotBodyPlain byte = 0`, `snapshotBodyInterned byte = 1`; `snapshotEnvelopePrefixBytes = 13` stays for v3–v20 and `snapshotV21EnvelopePrefixBytes = 14` is added, with `func snapshotEnvelopeBytes(version byte) uint64`; `CountSnapshotEncoding`'s `SnapshotEnvelopeBytes` uses `snapshotEnvelopeBytes(snapshotWriteVersion(st))`. `snapshotWriteVersion(st)` returns 21 exactly when `st.Params.Stage3 != nil` and is otherwise unchanged. `writeSnapshot` writes, for v21, the kind byte `snapshotBodyPlain` after the version byte. `snapshotDoc` gains `Stage3 *Stage3State` (JSON `stage3,omitempty`) after `Tracks`, set by `snapshotDocument`; Plan S3-A's `TestStateComponentsListEverySnapshotDocMember` is edited to list `"stage3"` between `"tracks"` and `"spent_ids"` (its README component list too), and `containerStateSize` and `readSnapshotMeasured` read the 14-byte v21 envelope.
- **Reader, Task 66:** the version refusal lists 21 (`unsupported snapshot version %d (require 3, 4, …, 20 or 21); …`, the existing text extended); a v21 kind byte other than 0 is refused with `unsupported snapshot v21 body kind %d` (Task 67 admits 1); the stage-3 forged-field refusal, after the tracks one at `:483-489`: `if ver[0] < snapshotVersionV21 && stage3FieldsPresent(&doc)` → `stage3 state is present in a pre-v21 container`; `func stage3FieldsPresent(doc *snapshotDoc) bool` is true for `doc.Params.Stage3`, `doc.BootstrapParams.Stage3`, `doc.Stage3`, any `ConsensusUpdates[i].Update.Stage3`, and any verifier entry of `Params`, `BootstrapParams` or a consensus update for which `func verifierEntryHasStage3Fields(v arbiter.VerifierEntry) bool` holds (any field beyond `node_id` and `ed25519_pubkey`); Tasks 69 and 70 add their fields (§47). Then `st.Stage3 = doc.Stage3` and `validateStage3State(st)` right after `validateTrackState` (`:486-489`), wrapped `snapshot: %w`.
- **Restore skeleton, Task 66** (new `fsm/stage3_restore.go`): `func validateStage3State(st *State) error` runs `validateStage3Activation(st)` (this task), then `validateStage3Incarnations(st)` (Task 69) and `validateStage3Selection(st)` (Task 70); later plans append theirs. `validateConsensusHistory` gains, beside its other bootstrap checks (`fsm/consensus_history.go:22-33`), `snapshot bootstrap consensus params: stage3 is set only by a consensus update, not at genesis`. `validateStage3Activation` refusals: `stage3 state must exist exactly when stage3 is committed`; `stage3 activation epoch %d is not the epoch of the first update that set stage3 (%d)`; `stage3 activation block %d or promotion %d is beyond the state's (%d, %d)`; `stage3 records a cutover for switch %s, which is off`; `stage3 switch %s is on without a cutover record`; `stage3 switch cutovers are not in the order of spec §10.4`; `stage3 switch %s cutover epoch %d is not the epoch of the update that turned it on (%d)`; `stage3 switch %s cutover (block %d, promotion %d) precedes the previous cutover or the activation`; `stage3 switch %s cutover carries an incarnation seq`; `stage3 purge cutover incarnation seq %d is outside 1..%d`.
- **Golden, Task 66:** `fsm/testdata/stage3_v21_plain.snapshot` (frozen), written once by `TestStage3V21PlainContainerGolden` under `ARBITER_CAPTURE_STAGE3_V21_PLAIN_GOLDEN=1` from `stage3V21GoldenFSM(t)` (`stage3FSM` plus one promoted, acknowledged and published block of each owner, every signature at `authoritytest.Iat`); the test compares the bytes, restores them, and re-writes the identical container.
- **Named tests, Task 66:** `TestSnapshotWritesV20UntilStage3IsCommitted`, `TestSnapshotWritesAV21PlainContainerOnceStage3IsCommitted`, `TestStage3V21PlainContainerGolden`, `TestRestoreReadsEveryEarlierVersionUnchanged`, `TestRestoreRefusesStage3FieldsInAPreV21Container`, `TestRestoreRefusesAnUnknownV21BodyKind`, `TestValidateStage3StateRequiresStateExactlyWithTheParameter`, `TestValidateStage3StateRefusesAnActivationEpochThatIsNotTheHistorys`, `TestValidateStage3StateRefusesSwitchCutoversThatAreNotTheHistorys`, `TestContainerStateSizeMeasuresAV21PlainContainer`, `TestCountSnapshotEncodingMatchesTheV21Writer`, the edited `TestStateComponentsListEverySnapshotDocMember`.
- **Interned body, Task 67** (new `fsm/snapshot_interned.go`): `const internedSnapshotFormat = "arbiter-snapshot-interned-v1"`, `const internStringMinBytes = 64` (a JSON string token, quotes included), `const internValueMinBytes = 256` (an object or array, compact encoding), `const maxSnapshotRehydratedDocBytes = 1 << 32`. Exported `func InternSnapshotBody(plain []byte) ([]byte, error)` and `func RehydrateSnapshotBody(interned []byte) ([]byte, error)`. The interned body is the compact JSON of `{"format": internedSnapshotFormat, "strings": [<string tokens verbatim>], "values": [<interned forms>], "root": <interned form of the document>}`; a string token of at least `internStringMinBytes` becomes `{"$s":i}` and an object or array of at least `internValueMinBytes` becomes `{"$v":i}` (indexes in first-occurrence order, a value's children interned before it, the root never a reference); object keys, numbers, `true`, `false` and `null` stay verbatim. Refusals: `interned body: the plain document holds the reserved key %q` (any key beginning with `$`; the writer then persists plain), `interned body: format %q is not arbiter-snapshot-interned-v1`, `interned body: value %d references value %d, which is not an earlier one`, `interned body: string reference %d is out of range`, `interned body: rehydrated document exceeds %d bytes`, `interned body: malformed: %v`.
- **Write-then-verify, Task 67** (`fsm/snapshot.go`, on Plan S3-A Task 9's `fsmSnapshot`): `type SnapshotBodyMode string`, `SnapshotBodyAuto SnapshotBodyMode = "auto"`, `SnapshotBodyPlain SnapshotBodyMode = "plain"`; `func (f *FSM) SetSnapshotBodyMode(mode SnapshotBodyMode) error` (refuses another value with `snapshot body mode must be auto or plain, got %q`; node-local, never replicated). `Snapshot()` keeps writing the plain container under the read lock; `fsmSnapshot` gains `mode SnapshotBodyMode` and `encoding *snapshotEncodingState`; `Persist`, outside the lock, for a v21 container in `auto` mode, calls `internSnapshotBodyFunc` (an unexported package variable, `InternSnapshotBody` in production, swapped by the forced-mismatch test), `RehydrateSnapshotBody` and compares the SHA-256 of the result with the SHA-256 of the plain body; on equality it writes the v21 container with `snapshotBodyInterned` and the interned body, otherwise the plain container, counting a fallback; in `plain` mode it writes the plain container and counts nothing; a v20 container is streamed unchanged. It never cancels a snapshot except on a sink error (as today), and `recordSize` keeps sampling the plain container, so `arbiter_state_bytes` stays the plain length. `type SnapshotEncodingStats struct { Fallbacks uint64; LastBodyKind string; LastPlainBodyBytes, LastInternedBodyBytes uint64 }` (`LastBodyKind` ∈ `v20`, `plain`, `interned`, `""` before the first persist; `LastInternedBodyBytes` 0 when the newest snapshot persisted plain) and `func (f *FSM) SnapshotEncodingStats() SnapshotEncodingStats`. The reader admits kind 1: it rehydrates the body (the 1 GiB cap applies to the stored body, the rehydration guard to the result) and decodes the plain bytes exactly as kind 0; `readSnapshotMeasured` samples the rehydrated plain document.
- **Config, Task 67:** YAML `raft.snapshot_body` (`config.RaftConfig.SnapshotBody string`, YAML key `snapshot_body`, raw default `auto`), refusal `raft.snapshot_body must be auto or plain, got %q`; `cmd/arbiter/services.go` calls `SetSnapshotBodyMode` right after `fsm.NewWithNotify` (`:62`); `configs/*.local.yaml` unchanged (the default applies).
- **Metrics, Task 67** (`metrics/snapshot.go`, Plan S3-A's file): `arbiter_snapshot_encoding_fallback_total` (counter: v21 snapshots persisted plain because the interned body did not verify), `arbiter_snapshot_body_bytes{kind}` (gauge, `kind` ∈ `plain`, `interned`, both always exported: the newest persisted snapshot's plain body and interned body lengths, interned 0 when it persisted plain), read through the unexported `snapshotEncodingReader interface{ SnapshotEncodingStats() fsm.SnapshotEncodingStats }` that `*fsm.FSM` implements. README: both series and the example alert `ArbiterSnapshotEncodingFallback` (`increase(arbiter_snapshot_encoding_fallback_total[1h]) > 0`, critical); the deployed rule is Plan S3-E4 Task 80's.
- **Golden, Task 67:** `fsm/testdata/stage3_v21_interned.snapshot` (frozen), written once by `TestStage3V21InternedContainerGolden` under `ARBITER_CAPTURE_STAGE3_V21_INTERNED_GOLDEN=1`: the container `Persist` writes in `auto` mode for `stage3V21GoldenFSM(t)`; its body rehydrates to exactly the body of `stage3_v21_plain.snapshot`, and both restore to the same state.
- **Named tests, Task 67:** `TestInternedBodyRehydratesByteExactly`, `TestInternedBodyInternsEachRepeatedValueOnce`, `TestStage3V21InternedContainerGolden`, `TestPersistWritesTheInternedBodyWhenItVerifies`, `TestPersistFallsBackToThePlainBodyOnAMismatch`, `TestPersistFallsBackWhenThePlainDocumentHoldsAReservedKey`, `TestPersistWritesThePlainBodyWhenForcedPlain`, `TestPersistStreamsAV20ContainerUnchanged`, `TestRestoreReadsBothV21Bodies`, `TestRehydrateRefusesAForwardReference`, `TestRehydrateRefusesAnExpansionAboveTheCap`, `TestStateBytesStayThePlainLengthUnderTheInternedBody`, `TestSetSnapshotBodyModeRefusesAnUnknownMode`; config `TestLoadConfigReadsRaftSnapshotBody`, `TestLoadConfigRefusesAnUnknownRaftSnapshotBody`; metrics `TestMetricsExportSnapshotEncodingFallbackAndBodyBytes`.

### 47. Switches 1–3: eviction voiding, purge sets, owner-aware selection (Tasks 68–70)

- **Eviction voiding, Task 68** (new `fsm/eviction_voiding.go`): `func (f *FSM) evictionVoidingLocked() bool` (`stage3SwitchOnLocked(Stage3SwitchEvictionVoiding)`), `func (f *FSM) evictedNodeLocked(nodeID string) bool` (a `Nodes` record with `NodeEvicted`), `func (f *FSM) evictedSignerRefusalLocked(kind, nodeID string) string` → `%s from %s: the node is evicted (stage3 eviction_voiding)` when voiding is on and the node is evicted, else `""`. Call sites, each right after that site's signature check and before any mutation: `applyRegisterRC` (`fsm/apply.go:483-487`, kind `result_claim`, node `rc.SourceNode`), `applyRecordAttestation` (`:608`, kind `attestation`, `att.ReplicaID`), `applyRecordByteSideScan` (`:637`, kind `byte_side_scan`, `scan.ReplicaID`), `applyRecordPromotionAck` (`:813-817`, kind `promotion_ack`), `applyRecordCleanupAck` (`:1034-1041`, kind `cleanup_ack`), `recordRejectedCleanupAckLocked` (`fsm/rejected_cleanup.go:343`, kind `cleanup_ack`); never inside `snodeSignatureLocked` or `verifierSignatureLocked`, which registration and activation share (spec §14 keeps both). `adoptParkedClaimsLocked` (`fsm/signed_claims.go:674-695`) discards, without binding, a parked claim whose `SourceNode` is evicted while voiding is on (K72). `reevaluateBlock` (`fsm/threeway.go:78-100`) skips the evidence of an evicted replica while voiding is on and no statement of the block is ChallengeReplay or Rejected (`blockChallenged`); a latched block is never re-evaluated (`:43-45`, unchanged). `func (f *FSM) reevaluateBlocksNamingLocked(nodeID string)` re-evaluates every block whose `VerifierSet` names the node, called by `applyEvictNode` after the status change while voiding is on; `func (f *FSM) reevaluateVoidedBlocksLocked()` does it for every evicted node, called by `onStage3SwitchOnLocked(Stage3SwitchEvictionVoiding)`. `evidencePresence(bv *BlockVerification, voided func(replica string) bool)` (`fsm/reads_work.go:20-32`): with voiding on, an evicted replica's evidence counts as missing, so its block is `EvidenceIncomplete` (a re-selection candidate from Plan S3-E3) and never `QuorumFailed`; with voiding off `voided` is nil and the WorkSet is unchanged. Restore re-applies none of this (spec §14).
- **Named tests, Task 68** (`fsm/eviction_voiding_test.go`): `TestEvictionVoidingRefusesEveryMessageOfAnEvictedNode`, `TestEvictionVoidingOffKeepsTodaysAdmission`, `TestEvictionVoidingKeepsALatchedVerdictAcrossSnapshotRestore`, `TestEvictionVoidingDropsAnEvictedVerifiersEvidenceFromAnUnlatchedVerdict`, `TestEvictionVoidingKeepsTheEvidenceOfAChallengedBlock`, `TestEvictNodeReevaluatesTheBlocksNamingIt`, `TestEvictionVoidingSwitchOnReevaluatesTheBlocksOfEarlierEvictions`, `TestWorkSetTreatsAnEvictedVerifiersEvidenceAsMissing`, `TestAdoptParkedClaimsDiscardsAClaimOfAnEvictedSNode`, `TestReRegisteredNodeIsNoLongerVoided`, `TestRegistrationAndMarkActiveKeepTheirChecks`, `TestRestoreDoesNotReapplyEvictionVoiding`.
- **Purge sets, Task 69** (`fsm/table_registry.go`, `fsm/apply_table_registry.go`, `server/table_registry.go`): `TableIncarnation.ScopedKeeperPath bool` (JSON `scoped_keeper_path,omitempty`) and `TableIncarnation.PurgeMembers []string` (JSON `purge_members,omitempty`) (sorted, unique), after `OwnerIndexerID`. `applyAddTable` sets `ScopedKeeperPath` on every incarnation it appends while `purge` is on (Refused ones included); nothing else ever sets or clears it. With `purge` on, `purgeRequiredLocked` (`:475-489`) is the owner's SNode (registered, not evicted), every node for which `poolVerifierLocked` holds, and every non-evicted node of `inc.PurgeMembers`, sorted; with it off it is today's rule; `purgeNodeIDsLocked` (`GetPurgeNodeSet`) is unchanged. `onStage3SwitchOnLocked(Stage3SwitchPurge)` reruns `completePurgesLocked` (the cutover's `IncarnationSeq` is recorded by Task 64's `recordStage3SwitchesLocked` right after), bumping the registry version when a purge completed (the `applyEvictNode` pattern). `TableRegistryView` deep-copies `PurgeMembers`; `tableRegistryToPB` sets `ScopedKeeperPath` and `PurgeMembers` (Plan S3-E1's fields 19 and 20); `stage3PendingProtoFields` loses both entries. `stage3FieldsPresent` adds any incarnation with either field. `func validateStage3Incarnations(st *State) error` refusals: `incarnation %d carries scoped_keeper_path without the purge switch`; `incarnation %d is recorded after the purge cutover (incarnation %d) without scoped_keeper_path`; `incarnation %d carries scoped_keeper_path but precedes the purge cutover (incarnation %d)`; `incarnation %d is not a chain incarnation but carries scoped_keeper_path`; `incarnation %d purge_members must be sorted, unique, non-empty node ids`; `incarnation %d carries purge_members before the placement switch` (Plan S3-E3 Task 71 replaces this rule with its subset rule).
- **Named tests, Task 69** (`fsm/purge_stage3_test.go`, `server/table_registry_test.go`): `TestPurgeSwitchMarksEveryLaterChainIncarnationScoped`, `TestPurgeSwitchOffKeepsEveryIncarnationUnscoped`, `TestPurgeRequiredSetIsTheOwnerThePoolAndPurgeMembersOnceThePurgeSwitchIsOn`, `TestPurgeRequiredSetBeforeThePurgeSwitchIsEveryNonEvictedVerifier`, `TestPurgeSwitchOnWithNoMembersChangesNoPurge`, `TestEvictNodeOfAPurgeMemberCompletesThePurge`, `TestPurgeNodeSetIsUnchangedByThePurgeSwitch`, `TestRegistryViewServesScopedKeeperPathAndPurgeMembers`, `TestRestoreRefusesAScopedIncarnationBeforeThePurgeCutover`, `TestRestoreRefusesAnUnscopedIncarnationAfterThePurgeCutover`, `TestRestoreRefusesMalformedPurgeMembers`, `TestTableRegistryToPBCarriesScopedKeeperPathAndPurgeMembers`, the edited `TestTableRegistryMirrorsProto`.
- **Owner-aware selection, Task 70** (`fsm/select.go`, `fsm/apply.go`, `fsm/admission.go`, `fsm/reads_work.go`, `fsm/reads_work_types.go`, `fsm/reads_verification.go`, `server/safestate_stage3.go`): `BlockVerification` gains `EligibleRing []string` (JSON `eligible_ring,omitempty`), `RingPosition uint32` (JSON `ring_position,omitempty`), `MarkedCursorBlock uint64` (JSON `marked_cursor_block,omitempty`) after `LastMergeable`. `func (f *FSM) poolRingLocked(sources []string) []string` (Active pool verifiers, `poolVerifierLocked`, minus `sources`, sorted by node id); `func (f *FSM) eligibleRingLocked(owner uint64, sources []string) []string` (this plan: `poolRingLocked(sources)`; Plan S3-E3 adds members recorded with the block or the statement's group, never one hosted by `owner`, and the pool only while fill is on or members are fewer than three); `func ringStart(blockSeq uint64, n int) int` (`u64FromDigest(replay.DigestString(arbiter.VerifierSelectSeedPrefix + strconv.FormatUint(blockSeq, 10))) % n`, today's start); `func initialVerifierSet(ring []string, position uint32) []string` (the sorted entries at `position-2`, `position-1`, `position` modulo the ring size). `applyMarkReplaying` with the switch off is byte for byte today's; with it on it takes `ring := eligibleRingLocked(blockOwner, bv.SourceNodes)` (`blockOwner` from the header's track), refuses below `VerifierSelectN` with `eligible verifier ring below 3 (spec §7.3 floor)`, and stores `VerifierSet = initialVerifierSet(ring, p)` with `p = (ringStart(seq, len(ring)) + 2) % len(ring)`, `EligibleRing = ring`, `RingPosition = p` and `MarkedCursorBlock = TableRegistry.Cursor.BlockNumber`; with no host-bound entry the set equals `selectVerifiers`'s. Admission, right after the `SOURCE_UNAVAILABLE` check (`fsm/admission.go:141-147`) and before the lane budget, with the switch on: `func (f *FSM) verifiersUnavailableLocked(owner uint64, source string) string` → `SubmitResult{Code: arbiter.AdmissionCodeVerifiersUnavailable, Message: …}` with `storage-integrity verifiers unavailable for indexer %d: %d eligible, at least %d required` and state unchanged (the coordinate unspent, no seal prepared). WorkSet: `WorkSet.Unmarkable []uint64` lists, with the switch on, every sealed, claim-complete, unmarked block whose eligible ring is below three, which is then left out of `SealedUnmarked` (recomputed every pass, so the block returns once the count changes); the orchestrator's `handleDispatchRows` is unchanged and proposes no `MarkReplaying` for it. `BlockVerificationView` gains `EligibleRing []string`, `RingPosition uint32` and `MarkedCursorBlock uint64`; `blockVerificationToPB` sets `EligibleRing`, `RingPosition` and `MarkedCursorBlock` (Plan S3-E1 §39 fields 14–16); `stage3PendingProtoFields` loses the three entries. `stage3FieldsPresent` adds any verification with `EligibleRing`, `RingPosition` or `MarkedCursorBlock`. `func validateStage3Selection(st *State) error` refusals: `block %d carries an eligible ring without the owner_aware_selection switch`; `block %d carries an eligible ring but no verifier set`; `block %d eligible ring must be sorted, unique and hold at least 3 verifiers`; `block %d ring position %d is not the block-seeded position %d`; `block %d verifier set is not the ring's three positions ending at %d` (Plan S3-E3 Task 74 extends both rules with stored re-selections, which advance the position and replace members of the set); `block %d marked cursor block %d is beyond the registry cursor %d`; `block %d was sealed after the owner_aware_selection cutover (block %d) and marked without an eligible ring`.
- **Named tests, Task 70** (`fsm/selection_stage3_test.go`, `orchestrator/dispatch_test.go`, `server/safestate_stage3_test.go`): `TestOwnerAwareSelectionOffSelectsByteIdentically`, `TestOwnerAwareSelectionWithOnlyThePoolPicksTodaysSet`, `TestOwnerAwareSelectionDrawsFromTheActivePoolOnly`, `TestOwnerAwareSelectionStoresTheRingThePositionAndTheCursor`, `TestMarkReplayingRefusesAnEligibleRingBelowThree`, `TestAdmissionAnswersVerifiersUnavailableBelowThreeEligible`, `TestVerifiersUnavailableIsCheckedRightAfterSourceUnavailable`, `TestVerifiersUnavailableLeavesTheCoordinateUnspent`, `TestWorkSetDropsAnUnmarkableBlockUntilTheEligibleCountChanges`, `TestOrchestratorProposesNoMarkReplayingForAnUnmarkableBlock`, `TestRestoreRefusesAnEligibleRingThatDoesNotYieldTheVerifierSet`, `TestRestoreRefusesAnEligibleRingBeforeTheOwnerAwareSelectionSwitch`, `TestRestoreRefusesABlockMarkedAfterTheCutoverWithoutARing`, `TestBlockVerificationServesTheEligibleRing`, the edited `TestBlockVerificationToPBCarriesEveryField`.

### 48. Consumed names, ledger, repositories and commands (Tasks 64–70)

- **Consumed from Plan S3-A** (contract §1, §4, §6): `SafeState.GetBlockVerification` and `BlockVerification` fields 1–13; `fsm.BlockVerificationView`, `fsm.BlockDispatchInfo`, `(*FSM).BlockVerificationView`, `blockVerificationToPB`, `requireEveryField`, `TestBlockVerificationToPBCarriesEveryField` (Task 7); `fsm.DrainView`, `(*FSM).DrainView`, `ConsensusHolds`, `arbiter-admin consensus update --hold none|issuance|drain` (default `drain`, K6) (Task 8); `fsm.StateSizeSample`, `StateComponents()`, `containerStateSize`, `readSnapshotMeasured`, `(*fsmSnapshot).recordSize`, `FSM.stateSize`, `TestStateComponentsListEverySnapshotDocMember`, `raftnode.SnapshotObserver`, the `metrics` reader pattern `stateSizeReader`, `metrics/snapshot.go`, and the series `arbiter_state_bytes`, `arbiter_snapshot_doc_bytes`, `arbiter_snapshot_doc_read_cap_bytes`, `arbiter_snapshot_persist_seconds` (Task 9); the rehearsal tools `$HOME/si-devnet2-multisource-s3a-rehearsal/statedigest/main.go`, `v20check/main.go`, `snapdump/main.go` (Task 11).
- **Consumed from Plan S3-B** (contract §15): the local voter copies of runbook `docs/storage-integrity-devnet2-stage3-p0.md` §3.3, `$HOME/si-devnet2-stage3-p0/arbiter-{0,1,2}-raft.tgz` and `arbiter-{0,1,2}.yaml` (Task 15), the throwaway-key rule for `ARBITER_ANCHOR_EVM_PRIVATE_KEY_HEX`, and through Plan S3-A Task 11 Plan S2-B's `$HOME/si-devnet2-multisource-tracks/arbiter-0-raft-after.tgz` with `arbiter-0-after.yaml` and the S2 samples `$HOME/si-devnet2-multisource-s2-rehearsal/sample14` and `sample19`. No record procedure runs here.
- **Consumed from Plan S3-C1** (contract §19): the ledger tool `$HOME/.cache/s3-ledger/s3-ledger.py` (verb `get`), written verbatim from §19 with the Write tool by Task 64's first ledger step.
- **Consumed from Plan S3-D** (Task 51): `ARBITER_TAG_P4`, `ARBITER_DIGEST_P4`. The branch base already carries Plan S3-D's `PROTO_TAG_P4` settle surface (Task 49, produced unconditionally, decision D26: `ConsensusAdmin.SettleTrack` and `GetTrackSettle` at method indexes 14 and 15, `SNodeHello.settle`, `PromotionCommand.settle`), which this plan leaves unchanged.
- **Consumed from Plan S3-E1** (spelled as Plan S3-E1 §39–§42 gives them; reconciled by the cross-plan consistency pass):
  - Task 57 (contract §39), arbiter-proto `PROTO_TAG_P5`: `ConsensusParamsUpdate.stage3 = 14`, `ConsensusMutableParams.stage3 = 9`; `Stage3Params { uint32 group_size = 1; bool network_fill_default = 2; uint32 min_safe_copies = 3; bool strict_copies = 4; repeated IndexerFailureDomain indexer_failure_domains = 5; uint32 max_lag_publications = 6; uint32 lag_low = 7; uint64 evidence_timeout_l2_blocks = 8; uint32 max_uncleaned_parts_per_partition = 9; uint32 max_uncleaned_parts_per_table = 10; bool exclude_quarantined_hosts = 11; bool purge = 12; bool eviction_voiding = 13; bool owner_aware_selection = 14; bool placement = 15; bool reselection = 16; bool checkpoints = 17; bool narrowed_drain = 18; }`; `IndexerFailureDomain { uint64 indexer_id = 1; string failure_domain = 2; }`; `VerifierEntry { … optional uint64 host_indexer_id = 3; string replica_signer = 4; string consent_jws = 5; uint32 weight = 6; string failure_domain = 7; bool verify = 8; bool replicate = 9; bool counts = 10; bool shared_rmt = 11; }`; `AdmissionCode.ADMISSION_CODE_VERIFIERS_UNAVAILABLE = 12` (code 13 unused here); `BlockVerification` fields `eligible_ring = 14` (repeated string), `ring_position = 15` (uint32), `marked_cursor_block = 16` (uint64), `group_key = 17`, `group_members = 18`, `group_fill = 19`, `group_databases = 20` (`BlockGroupRef`), `reselections = 21` (`BlockReselection`), `reselections_at_latch = 22`; `L3BlockTrack.group_key = 2`; `TableIncarnation.scoped_keeper_path = 19` (bool), `purge_members = 20` (repeated string); `TableRegistrySnapshot.groups = 10`, `member_checkpoint_seqs = 11`, `stage3 = 12`; `ConsensusParamsState.stage3_state = 9` (`Stage3StateView{activation_epoch, activation_block_seq, activation_promotion_seq, switches}`, `Stage3SwitchCutover{switch_name, epoch, cutover_block_seq, cutover_promotion_seq}`); Raft tags 38–42 (unused here); the RPCs `ConsensusAdmin.RebalanceGroup` (method index 16) and `MarkMemberLagging` (17), 18 `ConsensusAdmin` methods in all (decision D26), and `VerifierGateway.SubmitMemberReady` (4) and `SubmitMemberCheckpoint` (5) (unused here: the embedded `pb.UnimplementedConsensusAdminServer` at `server/consensus_admin.go:26` and `pb.UnimplementedVerifierGatewayServer` at `server/gateway.go:15` answer `Unimplemented` until Plan S3-E3 serves them, so the pin needs no server change for them). Go: `pb.Stage3Params`, `pb.IndexerFailureDomain`, `pb.ConsensusMutableParams.Stage3`, `pb.ConsensusParamsState.Stage3State`, `pb.Stage3StateView`, `pb.Stage3SwitchCutover`, `pb.TableIncarnation.ScopedKeeperPath`, `.PurgeMembers`, `pb.TableRegistrySnapshot.Stage3`, `pb.BlockVerification.EligibleRing`, `.RingPosition`, `.MarkedCursorBlock`, `pb.AdmissionCode_ADMISSION_CODE_VERIFIERS_UNAVAILABLE`.
  - Task 58: `HG_TAG_P5` (housegate pinning `PROTO_TAG_P5`, codes 12 and 13 at the ingress).
  - Task 59 (canonical types and authority): `arbiter.Stage3Params` with fields `GroupSize`, `NetworkFillDefault`, `MinSafeCopies`, `StrictCopies`, `IndexerFailureDomains`, `MaxLagPublications`, `LagLow`, `EvidenceTimeoutL2Blocks`, `MaxUncleanedPartsPerPartition`, `MaxUncleanedPartsPerTable`, `ExcludeQuarantinedHosts`, `Purge`, `EvictionVoiding`, `OwnerAwareSelection`, `Placement`, `Reselection`, `Checkpoints`, `NarrowedDrain` (JSON tags the proto names); `arbiter.IndexerFailureDomain{IndexerID, FailureDomain}`; `arbiter.ConsensusParamsUpdate.Stage3 *Stage3Params` (`json:"stage3,omitempty"`); `arbiter.VerifierEntry` fields `HostIndexerID *uint64`, `ReplicaSigner`, `ConsentJWS`, `Weight uint32`, `FailureDomain`, `Verify`, `Replicate`, `Counts`, `SharedRMT` (each `omitempty`); `arbiter.AdmissionCodeVerifiersUnavailable` (12); `authority.GroupMemberConsentPurpose = "arbiter-group-member-v1"`, `authority.GroupMemberConsentStatement{NetworkID, GenesisSnapshotID string; HostIndexerID uint64; NodeID string; Ed25519Pubkey []byte; ReplicaSigner string}`, `authority.VerifyGroupMemberConsent(stmt GroupMemberConsentStatement, jws, signer string) error`, `(*authority.Signer).SignGroupMemberConsentAt(stmt GroupMemberConsentStatement, iat int64) (string, error)`; `authority.NormalizeConsensusParamsUpdate` copying every new field (indexer failure domains sorted by indexer id, replica signers lowercased).
  - Task 60 (wire): `wire.Stage3ParamsToPB`, `wire.Stage3ParamsFromPB`; `wire.VerifierEntriesToPB` / `FromPB` and `wire.ConsensusParamsUpdateToPB` / `FromPB` carrying every new field.
  - Task 63: `arbiter.Stage3Feature = "stage3_v1"` in `arbiter.LocalNodeFeatures()`; `CORE_TAG_P5`.
- **Release ledger:** consumed `PROTO_TAG_P5`, `HG_TAG_P5`, `CORE_TAG_P5` (Plan S3-E1's Appendix C), `ARBITER_TAG_P4`, `ARBITER_DIGEST_P4` (Plan S3-D's Appendix C), read only from the ledger copy (decision D34: for Plans S3-D and S3-E1 the plans-cf branch `origin/urwt/claude/ms-si-stage3-plans-cf` until the stage-3 plans PR merges, then `origin/main`; `s3-ledger.py get` picks it, Plan S3-C1 §19): `h=/Users/uranuswch/Dev/projects/sentio-network/housegate; L="$HOME/.cache/s3-ledger"; git -C "$h" fetch -q origin; python3 -I "$L/s3-ledger.py" get "$h" PROTO_TAG_P5 HG_TAG_P5 CORE_TAG_P5 ARBITER_TAG_P4 ARBITER_DIGEST_P4` (the fetch is a remote read: explicit confirmation). Produced: none; Appendix C has no row.
- **Worktree:** one URWT worktree for Plans S3-E2 to S3-E4: `git -C /Users/uranuswch/Dev/projects/sentio-network/arbiter fetch origin --tags` (confirmed), then `bash ~/.agents/skills/urwt/scripts/urwt-create.sh /Users/uranuswch/Dev/projects/sentio-network/arbiter claude multi-source-s3e` → `~/src/remotesrc/claude/arbiter/multi-source-s3e` (its JSON `path` is `$W`; every Bash call is prefixed `W=<path>;`), branch `urwt/claude/multi-source-s3e` from `origin/main`; Task 64 checks `git -C "$W" merge-base --is-ancestor "$(git -C "$W" rev-parse "$ARBITER_TAG_P4^{commit}")" HEAD` and stops if `ARBITER_TAG_P4` is not in the base. Tasks 65–70 reuse it (never re-created); each task starts with `git -C "$W" status --porcelain` empty and `HEAD` at the previous task's last commit.
- **Pins (Task 64), in this order:** `GOWORK=off go get github.com/sentioxyz/arbiter-proto@$PROTO_TAG_P5 && GOWORK=off go mod tidy && bazel mod tidy && bazel run //:gazelle`; `bash scripts/update-housegate.sh $HG_TAG_P5`; `bash scripts/update-arbiter-core.sh $CORE_TAG_P5`; then the pin-fallout rule of §44 and one commit `chore(deps): pin arbiter-proto, housegate and arbiter-core for stage 3 P5 (Task 64)`.
- **Checks (every task):** `test -z "$(gofmt -l $(git ls-files '*.go'))"`; focused runs `go test ./<pkg>/ -run '<named tests>' -race -count=1` (TDD: fail, implement, pass); `bazel run //:gazelle` after a new file; `bazel build //... && bazel test //...`; the fsm red lines `bazel query 'deps(//fsm:fsm, 1)' | grep com_github_sentioxyz_arbiter_proto` printing nothing and `grep -rn 'time\.Now' fsm/` printing nothing.
- **Commits:** subjects `feat(stage3): <what> (Task N)` (`test(stage3): …` for a test-only commit); every message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Code comments in English; markdown never hard-wrapped.
- **Runbooks:** none. This plan has no live task; Plan S3-E4 Task 81 writes `docs/storage-integrity-devnet2-stage3-p5-v21.md` and cites §44–§47 for what the build does.
- **Push (Task 70, last step; explicit confirmation):** `git -C "$W" push -u origin HEAD`. No PR, no merge, no release, no tag in this plan (K65); Plan S3-E4 Task 78 opens the single PR of Tasks 64–78 (its body ending with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`) and cuts `ARBITER_TAG_P5`.
- **Work directory and evidence (Task 67; local only, never committed; decision D37):** `T="$HOME/si-devnet2-multisource-s3e2-replay"` (new, mode 700; Task 67 stops if it exists and never reuses an earlier directory): `$T/helpers.sh` with the bash functions `rehearsal_build old|new` (exports `ARBITER_TAG_P4` or the branch head with `git archive` into `$T/src/<build>`, copies the five tools into its `cmd/`, builds them into `$T/bin/<build>`), `rehearsal_replay <name> <raft-dir> <arbiter.yaml>` (statedigest and snapdump under both builds, the comparable lines of Plan S3-A's `digest_lines` compared, the containers compared and cross-restored, one result line per input appended to `"$W.replay-e2.txt"`) and `rehearsal_v21size <name> <container>` (appends to `"$W.v21size-e2.txt"`); `$T/statedigest/main.go`, `$T/v20check/main.go`, `$T/snapdump/main.go` (copied verbatim from Plan S3-A's `$HOME/si-devnet2-multisource-s3a-rehearsal`, their SHA-256 recorded in `$T/tools.sha256`); `$T/v21check/main.go` (new: restores a v3–v21 container with this build and prints `restore_ok version <n> body <none|plain|interned>`); `$T/v21size/main.go` (new: reads a v20 or v21 plain container, runs `fsm.InternSnapshotBody` and `fsm.RehydrateSnapshotBody`, prints `plain_bytes <n> interned_bytes <m> rehydrated_identical <true|false>`); `$T/src/{old,new}` (`git archive` exports of `ARBITER_TAG_P4` and the branch head), `$T/bin/{old,new}`, `$T/old_tag.txt`, and the local voter copies of §48's Plan S3-A and S3-B inputs extracted under `$T/inputs/<name>`. Evidence, beside the worktree like Plan S3-E4's PR body file: `"$W.replay-e2.txt"` (`~/src/remotesrc/claude/arbiter/multi-source-s3e.replay-e2.txt`; it opens with the line of `$T/old_tag.txt` and the lines of `$T/tools.sha256`, then per input: identical `statedigest` lines under old and new, `snapshot_sha256` included, both builds writing the same v20 container, each restoring the other's with `restore_ok version 20`) and `"$W.v21size-e2.txt"` (`~/src/remotesrc/claude/arbiter/multi-source-s3e.v21size-e2.txt`; per input: `rehydrated_identical true` and the sizes). Task 67's last local step, once both files are complete, removes the work directory (`Confirm before running.`, then `rm -rf -- "$T"`), so no second P5 rehearsal directory exists: the one P5 rehearsal directory `R5="$HOME/si-devnet2-multisource-s3e-rehearsal"` is created by Plan S3-E4 Task 78 and only read by the P5 runbook, which keeps its live rehearsal evidence under its own `$si_backup` (D37). No task of this plan creates, writes or reads `$R5`, and nothing after Task 67 reads `$T`.

## Appendix B: drafting decisions and cross-task facts

Facts the drafters recorded while drafting and verifying each task in scratch exports. The plan consistency pass replaces this appendix with its final form; until then it is the drafters' record only. `<scratch>` stands for local verification directories that are not part of the plan.

### Task 64

- Line anchors are arbiter 526a721 (v0.13.0, origin/main now: Plan S3-A merged as arbiter#133). For every fsm source file Task 64 edits by hand they are identical to the header's fb42260 anchors; only fsm/BUILD.bazel (gazelle-written) differs. At 526a721 go.mod pins housegate v0.20.0 (:18), arbiter-core v0.15.0 (:20), arbiter-proto v0.12.0 (:21).
- Pin fallout at the base is exactly two tests: TestBlockVerificationToPBCarriesEveryField (BlockVerification.eligible_ring first of the nine unset fields 14-22) and TestTableRegistryMirrorsProto (scoped_keeper_path, purge_members). stage3PendingProtoFields holds exactly 11 entries (9 BlockVerification.*, 2 TableIncarnation.*). TestL3BlockHeaderMirrorsProto consults the map but walks only top-level header fields, so L3BlockTrack.group_key is not pending; no test walks TableRegistrySnapshot, so its three contract §44 entries are not added. Step 7 says what to do if an S3-C2/S3-D test fails on one of them.
- Plan S3-E1's normalizer (contract §41) refuses many updates before verifyConsensusTransition: switch-before-earlier in the same parameters, the value checks, the failure-domain map rules, pool entries with host-bound fields, a host-bound entry's host, replica signer, weight, domain and shared_rmt, and strict_copies/min_safe_copies>1 without both switches in the update. The FSM keeps its own §45 texts as defence in depth and for restore; the tests reach them by calling verifyStage3Transition / verifyHostBoundVerifiers directly after requireNormalizerRefuses shows the normalizer's text. Consent, K68, the fixed host, one-way rules, one-switch rule, activation rule and the committed-switch floor rule are the FSM's alone and are tested through Apply.
- The floor rule reads committed switches: min_safe_copies>1 and strict_copies need checkpoints and narrowed_drain on in the previous parameters, so the update that turns narrowed_drain on cannot also raise the floor (stricter than E1's static form; matches spec §10.4 steps 9 then 10).
- K68 is enforced against previous.Stage3 (committed), also when the same update turns owner_aware_selection on. host_indexer_id is fixed for an entry's life in both directions (pool->member, member->pool, member->other host). Consent is verified (under the host's current si_indexers signer) on add and on an ed25519 or replica_signer change; an unchanged entry keeps its consent across a host signer rotation and cannot swap consent_jws.
- stage3MembershipRefusalLocked (Apply-only, after signedClaimsMembershipLocked) refuses an added host-bound entry whose node id names any Nodes record, evicted included.
- activateStage3Locked / recordStage3SwitchesLocked run after the tracks block and before ConsensusEpoch++, i.e. after the per-Apply registry bump block; cutover BlockSeq/PromotionSeq are read after onStage3SwitchOnLocked; purge records IncarnationSeq = len(Incarnations)+1. stage3FSM's activation is epoch 4, block 1, promotion 0.
- State.Stage3 is not a snapshot member until Task 66 (container stays v20 on this commit); rejectStage3Unchanged compares the record as JSON for that reason.
- TestStage3ActivationWithEverySwitchOffChangesNoBehaviour tolerates a fixed registry-version delta of at most 1 between the twins (Task 65's stage3 bump), which no later command may change; it passes with or without Task 65's bump.
- poolVerifierLocked is false for host-bound entries (registered or not) and evicted nodes; the test registers member-1 after switch 3 with key ed25519KeyFor('m') and a re-signed consent.
- Raft tags 38-42 reach Apply's default ('empty command') until Plan S3-E3 adds their cases; stage3_v1 enters LocalNodeFeatures at the CORE_TAG_P5 pin, so no arbiter release until Task 78 (K65).
- fsm go_library and go_test deps already list every package the new files import; gazelle only adds stage3.go after snapshot_query_block.go and the three test files after source_ack_test.go.
- r2 changes vs the first draft: re-anchored on 526a721; two test additions in this round (activation strict_copies/min_safe_copies 2 alone met directly; a registered member and an evicted node outside the pool).
- Names added while drafting: fsm cloneVerifierEntry(v arbiter.VerifierEntry) arbiter.VerifierEntry (fsm/signed_claims.go, right after cloneVerifiers); fsm findVerifierEntry(entries []arbiter.VerifierEntry, nodeID string) (arbiter.VerifierEntry, bool); fsm stage3FailureDomain(p *arbiter.Stage3Params, id uint64) string; fsm hostBoundFieldsSet(v arbiter.VerifierEntry) bool; fsm validReplicaSigner(s string) bool; server test helper stage3ProtoFieldPending(fd protoreflect.FieldDescriptor) bool; fixture stage3WithSwitch(p *arbiter.Stage3Params, name string) *arbiter.Stage3Params; fixture turnOnStage3SwitchesThrough(t, f, a, last string) (used by Task 70); fixture stage3MemberKey(node string) ed25519.PrivateKey (deterministic per node id); fixture stage3ReplicaSigner(t testing.TB, node string) *authority.Signer; fixture memberConsent(t, f, signer *authority.Signer, entry arbiter.VerifierEntry) string; fixture hostSigner(t, f, host uint64) *authority.Signer; fixture withVerifier(u arbiter.ConsensusParamsUpdate, entry arbiter.VerifierEntry) arbiter.ConsensusParamsUpdate; fixture stage3StateJSON(t, f) []byte; fsm/stage3_params_test.go: ownerAwareFSM(t) (*FSM, *authority.Signer) (switches 1-3 committed; Task 70's fixture), setNonZero, stage3Previous, requireNormalizerRefuses, requireErrorContains, consentStatement; fsm/stage3_activation_test.go: stage3Activation, type stage3Twin (apply method, versionDelta), stage3NeutralState
- Verified while drafting: Assembled the task from the template plus the scratch tree; a script (tools/apply_step.py) applied the markdown step by step (each replace pair matched exactly once, files created as written, Step 16 append) to a fresh git archive of arbiter origin/main 526a721 with go.mod replace to stand-ins: arbiter-proto = Plan S3-E1 Task 57 drafted tree (proto/ and gen/ byte-identical to Task 57's final draft), arbiter-core = cfd4f42 + contract §41 stub. Result byte-identical to the drafting tree. / GOWORK=off GOFLAGS=-mod=mod GOPROXY=off go test at every step: Step 3 count 3; Step 5 go doc output exactly as stated; Step 6 fallout exactly as stated; Step 8 three PASS + ok; Step 11 three failing subtests with the quoted message; Step 13 pass; Step 18 build failure with the quoted errors; Step 23 26 PASS and the eight goldens PASS; Step 24 gofmt-clean, go vet clean, go test ./... -race no failures, no time.Now in fsm/, status listing as stated (minus fsm/BUILD.bazel). / 49 single-point mutations (41 in fsm/stage3.go, 6 in the wiring, 2 stage3-gated behaviour changes in select.go/admission.go) each fail their named test; 0 survive. / One earlier whole-suite -race run timed out in integration TestPipeline_EndToEndOverTheWire under machine load average ~125; it passed 3 times alone and in the final whole-suite -race run. / bash -n of all 13 bash blocks and the Run lines (placeholders substituted): clean; shellcheck clean except SC2046 on contract §48's own gofmt command (kept verbatim). / Checked against the inputs: update scripts' output lines, CI's bazel test command, urwt-create.sh JSON contract, Plan S3-C1 §19 ledger tool extraction (awk) and verbs ref/show/get, E1 contract §41 names/signatures (wire converters, consent API, Stage3Params methods), E1 Task 59 in-progress normalizer texts match the quoted refusals. / Not run: Bazel and gazelle (offline; no P5 tags; git_override sources unavailable), any remote command.

### Task 65

- This run resumed an interrupted r2 revision (work/e2/task-65-r2, never written out). It re-anchors the r1 draft from Oct 15 on arbiter origin/main 526a721 (v0.13.0), which now has Plan S3-A merged (#133: --hold, updateConsensusParams/proposal/heldUpdateOutcome, the stage-3 reads). arbiter-core origin/main is cfd4f42 (v0.15.0), arbiter-proto 7a73f25 (v0.12.0). The plan header's base fb42260 is out of date.
- Changes since r1: stage3StateToPB moved into server/consensus_admin.go next to trackStateToPB. The registry bump uses stage3ParamsChanged instead of reflect.DeepEqual, so a snapshot-restored replica (omitempty drops an empty failure-domain list) can never decide a bump differently from one that replayed the log. ConsensusParamsView.Stage3 is renamed Stage3State. The --stage3-switch refusal is now the contract's literal text. New FSM tests cover the reads and the bump. A new end-to-end CLI/server/FSM test commits the activation under the default drain hold, as the P5 runbook (Plan S3-E4 contract) runs it. The r1 note claiming the CLI re-check catches quarantine changes was wrong and is corrected: exemptions are read once, and the leader's own gate decides again at submit.
- The Task 64 interfaces consumed (Stage3Active, Stage3ActivationRefusal, Stage3ActivationNodes, cloneStage3Params, stage3PendingProtoFields in server/safestate_stage3_test.go, and the fixture's stage3ActivationParams/stage3Update/activateStage3/turnOnStage3Switch) match the Task 64 draft at e2/tasks/task-64.md.
- At 526a721 no mirror or every-field test reads TableRegistrySnapshot, so Task 64's pin adds no 'TableRegistrySnapshot.stage3' entry. Step 11 is a check that deletes the line only if a Plan S3-C2 or S3-D test made Task 64 add it.
- Gazelle adds source files only, no dependency: fsm stage3_reads.go and stage3_reads_test.go; server stage3_gate.go, stage3_fixture_test.go, stage3_gate_test.go, stage3_params_test.go and voter_probe_test.go; arbiter-admin stage3.go, stage3_test.go and stage3_integration_test.go.
- The leader's PrepareConsensusUpdate (S3-A) runs no gate. A stage3 gate refusal of a held update is definite (heldUpdateOutcome: not submitted), so it ends the hold. The CLI takes the hold after its gate and checkStage3NotActive and before the pre-submit re-check.
- Plan S3-E4's activation command (contract §57: --stage3, --indexer-failure-domain list, the five required values, three --voter, --hold drain) and Plan S3-F's consumed flags and jq paths (--stage3-switch, --stage3-min-safe-copies 2, host-bound --verifier, .current.stage3.<field>, .stage3_state.switches[].cutover_block_seq) are all served; TestConsensusShowJSONHasTheStage3FieldsTheRunbooksRead pins the jq paths.
- Names added while drafting: server: func voterAddresses(ctx context.Context, key string) []string (replaces tracksVoterAddresses); server refusals: stage3 gate refused: raft configuration is unavailable on this node; stage3 gate refused: raft term is unavailable on this node; Unavailable stage3 gate: read raft configuration: %v; fsm: func (f *FSM) stage3StateViewLocked() *Stage3State; fsm: ConsensusParamsView.Stage3State *Stage3State (the Stage3StateView copy taken under the parameters' lock); fsm: func stage3ParamsChanged(previous, next *arbiter.Stage3Params) bool (the registry-bump rule; an empty failure-domain list equals an absent one); arbiter-admin: func stage3GateFor(exempt []string) activationGate; arbiter-admin: type stage3SwitchFlags []string; type uint32Flag uint32; type stage3Flags (register, check, given, target); var stage3ActivationRequired; func setStage3Switch(p *arbiter.Stage3Params, name string); var verifierHostKeys; func parseVerifierFlag(value string) (arbiter.VerifierEntry, error); func parseHostBoundVerifier(entry *arbiter.VerifierEntry, fields map[string]string) error; arbiter-admin: gateObservation.exempt []string; observeGate(ctx, g, leader, voters, dial) now takes the gate; arbiter-admin refusals: --stage3 needs the tracks activation first: tracks_enabled is not committed, and the stage-3 activation must be a later update; stage3 gate refused: read the leader's track state: %w; stage3 gate refused: read the leader's consensus parameters: %w; stage3 gate refused: the leader already reports stage3 committed; re-run consensus show before retrying; --verifier %s: host %q is not an unsigned integer; --verifier %s consent: %w; --indexer-failure-domain id %q is not an unsigned integer; key %q has no value; --voter is only used with --client-lanes-max-per-account or --si-indexer, or with --tracks-enabled or --stage3 (extended text); tests: fsm TestStage3StateViewIsADetachedCopy, TestRegistryVersionBumpsOnceForEveryCommittedStage3Change, TestStage3ParamsChangedTreatsAnEmptyFailureDomainListAsAbsent; arbiter-admin TestConsensusShowJSONHasTheStage3FieldsTheRunbooksRead, TestCLIStage3ActivationThroughServerAndFSM (CLI + real server + production FSM, activation under the default --hold drain); test helpers: server stageTwoFeatures, stageThreeVoter, stageTwoOnlyVoter, stage3VotersContext, stageThreeVotersContext, stage3GateText, serverStage3Params, serverStage3ActivationRequest, serverPurgeSwitchRequest, applyServerRequest, activateServerStage3, prepareStage3Gate, refusedStage3BeforeRaft; arbiter-admin stageTwoFeatures, stage3Admin, stage3GateFixture, stage3ActivationArgs, activatedStage3, stage3WithSwitches, writeConsent, memberFlag, lastUpdate
- Verified while drafting: Fresh scratch work/e2/task-65-r3: git archive of arbiter origin/main 526a721, plus a Task 64 stand-in built from the contract §44–§45 names (with its fsm BUILD lines), plus replace modules core5 (arbiter-core cfd4f42 with stubs of E1 §41: Stage3Params, Validate, Stage3Switches, SwitchOn, the normalizer rules, the wire converters, Stage3Feature in LocalNodeFeatures) and proto5 (arbiter-proto 7a73f25 regenerated with the E1 §39 fields and the P4 settle surface). / Generator gen_task65.py renders the markdown from the verified tree. It asserts every anchor occurs exactly once in the stand-in base, and that applying every hunk reproduces each verified file exactly. / Applier apply_task.py parsed the rendered markdown into a fresh clone and ran every step's command with GOWORK=off GOFLAGS=-mod=mod GOPROXY=off: / Results: Step 3 PASS (8 subtests). Step 6 ok. Step 8 FAIL [build failed] for server and fsm with the stated undefined-symbol messages. Step 12 PASS. Step 13: server, fsm and arbiter-admin ok with -race. Step 16 FAIL [build failed] with the stated messages. Step 18 PASS: 11 stage3_test.go tests, the integration test, 7 switch subtests, 16 refusal subtests, and the full arbiter-admin suite. Step 20: go test ./... -race ok for every package. The fsm suite was killed once by an external SIGTERM; re-run alone it passed (ok, 44.6s). / After all steps the applied tree was byte-identical to the verified tree, BUILD.bazel files from gazelle included. The three commits were created with the stated subjects and the final status was clean. The final markdown (prose-only changes after that run) was re-applied structurally and is still identical. / Mutation checks: 12 mutations, all killed by the named tests. They removed or disabled: the stage3Gate call, the registration refusal, the Stage3 clause of the barrier condition, the registry bump (semantic and compile), the empty-list rule, the CLI exemption skip, the non-voter check, the FSM-refusal check, the required-flags check, the arbiter-stage3-voters metadata, and the TableRegistrySnapshot.stage3 field. / bash -n and shellcheck -S warning are clean on all 12 'Run:' commands and all 3 bash blocks. The markdown has no TBD, TODO or placeholder text and no hard-wrapped prose; every commit message ends with the Co-Authored-By line. / fsm red lines: direct fsm imports of arbiter-proto (go list -f '{{join .Imports}}') = 0; grep time\.Now fsm/ is empty; no testdata changed. / Bazel: only gazelle ran. bazel build, test and query were not run, because the stub modules are not wired for Bazel. Disclosure: the first gazelle run, in a new scratch workspace (task-65-r2/arb-gz2), completed. A second fresh workspace failed trying to reach proxy.golang.org, which shows a fresh output base fetches gazelle's Go deps, so the arb-gz2 run most likely fetched public Go modules over the network. Every later gazelle run reused that warm workspace offline.

### Task 66

- Line anchors are arbiter 526a721 (v0.13.0, origin/main, Plan S3-A merged as arbiter#133). Tasks 64 and 65 (r2) touch none of the fsm files Task 66 edits, so the fsm anchors are the base lines: snapshot.go writeSnapshot :168-189, snapshotWriteVersion :198-220, snapshotDoc :68-108, the version whitelist :285-287, the predicates :303/:318/:426, after validateTrackState :504-507, the restore sample :656, tableRegistryHasV17Fields :723; snapshot_count.go :17-19/:39-42/:66-68/:98; state_size.go :56-57/:114-142; consensus_history.go :31-33.
- In README.md the Snapshot versions paragraph is :300, `## Artifact disposition admission` is :396 at 526a721 (on the branch it follows Task 65's new section `## Multi-source storage integrity (stage-3 activation)`, which the Snapshot v21 paragraph closes), and the State size and snapshots paragraph is :429 at 526a721. The Snapshot versions sentence points to `(see Snapshot v21 below)`, so it does not depend on what Task 65 calls its heading.
- Built from stand-ins, the golden container is 175302 bytes and starts with `65 70 83 77 21 0`. Two separate captures in two directories gave the same SHA-256 (89707ed2…bb96, stand-in specific; the real capture differs).
- 526a721 refuses the captured container with `unsupported snapshot version 21 (require 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 or 20); pre-v3 state requires a backup and coordinated re-genesis`.
- The read-cap case of TestFailedRestoreKeepsThePreviousSample would still pass without its edit: under version 21 the unknown body kind 255 refuses it first. The edit keeps it a read-cap test. The other five existing-test edits each fail without the edit (checked).
- validateStage3Activation takes the epoch of each record from the first signed update whose stage3 satisfies the rule (the update at index i produces epoch i+1), or 0 when no update does. A record with epoch 0 is refused too, and the epoch-0 test cases pin this.
- Step 4's build failure starts at fsm/apply_artifact_disposition_test.go:730:20 `undefined: snapshotEnvelopeBytes`. Step 10's single failure is snapshot_v21_test.go:234 (the golden file is missing) while 21 other top-level tests pass. Step 11 prints `snapshot_v21_test.go:230: captured; commit the file`.
- Gazelle adds exactly `stage3_restore.go` to the go_library srcs and `snapshot_v21_test.go` to the go_test srcs (after snapshot_tracks_test.go), with no deps change. The commit is 14 files changed.
- Names added while drafting: refusal `stage3 switch %s cutover (block %d, promotion %d) is beyond the state's (%d, %d)` (validateStage3Activation, upper bound beyond contract §46); refusal `snapshot v21 body kind: %w` (container cut after its version byte); TestRestoreRejectsAnOversizedV21LengthPrefix; TestBootstrapParamsNeverCarryStage3; test helper plainSnapshotBytes(t *testing.T, f *FSM) []byte; test helper v21Document(t *testing.T, data []byte) []byte; test helpers v21Stage3, v21Cutover, v21Update; test helper stage3SwitchedFSM(t *testing.T) *FSM; test helper fixedPromotionAck(t *testing.T, f *FSM, p uint64) wire.Command; const stage3V21PlainGoldenPath; stateSizeSample(source string, envelopeBytes, docBytes, spentIDsBytes uint64, components []StateComponentSize) StateSizeSample (Plan S3-A's unexported helper gains envelopeBytes); rewriteSnapshotDocument made envelope-aware (13 bytes for v3-v20, 14 for v21 with the plain kind byte)
- Verified while drafting: Scratch tree: work/e2/task-66-r2/arb2, a git archive of arbiter 526a721 plus the latest Task 64 r2 draft tree (task-64-r2/arb at f0abe6f), Task 65 r2's fsm and README diffs, and gazelle output for those stand-ins. It builds against the PROTO_TAG_P5 and CORE_TAG_P5 stand-ins (proto5, core5) through go.mod replace, with GOWORK=off GOFLAGS=-mod=mod GOPROXY=off. / A script (pairs.py) applied every Replace/with pair of Steps 3, 5, 6, 7, 9 and 12 to the base files and reproduced the tested tree byte for byte. apply_doc.py then parsed the generated task-66.md itself, applied its two Create blocks, 32 Replace pairs and the README insertion to a fresh base, and reproduced all 12 files byte for byte. / Step 1 in a simulated worktree (wt-sim on branch urwt/claude/multi-source-s3e) printed the expected lines and `ok`. / Step 4 was reproduced in arb-red and gave the exact undefined-name list in the doc. / Step 10 was reproduced: only TestStage3V21PlainContainerGolden fails. / Step 11: the capture skipped as expected, od printed `65 70 83 77 21 0`, and a later rerun passed. / Step 12's four greps printed 1, 1, 1, 0. / Step 13 without the Bazel lines printed gofmt-clean, vet-clean, no-failures and goldens-untouched. / Step 14's commit printed `14 files changed` and the same SHA-256. / Full go vet ./... was clean. go test ./... passed, and go test -race ./... passed for all 22 packages. / No time.Now appears in fsm/, and the fsm library's direct imports contain no arbiter-proto. / 20 single-point mutations were each caught by a named test: the verifiers predicate, the bootstrap and update halves of stage3FieldsPresent, the pre-v21 refusal, the kind check, the writer version, snapshotDocument's Stage3, snapshotEnvelopeBytes, the state-with-parameter rule, both epoch-0 rules, the switch-on rule, the order, the precedes, upper and activation bounds, the incarnation lower bound, the bootstrap refusal, and both state-size envelopes. Two of them first survived, so tests for the epoch-0 cases were added to TestValidateStage3StateRefusesAnActivationEpochThatIsNotTheHistorys and TestValidateStage3StateRefusesSwitchCutoversThatAreNotTheHistorys. / Gazelle ran with the network blocked (proxy set to a dead port, Go modules served from the local module cache through a file:// GOPROXY) and wrote the expected fsm/BUILD.bazel lines. bazel build and bazel test were not run because the P5 modules exist only as local stand-ins. / Every bash block and Run command passed bash -n. shellcheck is clean apart from the intended SC2046 (contract §48's gofmt idiom) and SC2016 (literal backticks in grep patterns). Inline code spans and fences are balanced; the README check was moved into a bash block so no inline span contains a backtick. / The r1 draft was backed up to work/e2/task-66-r2/task-66.r1.md before task-66.md was overwritten.

### Task 67

- Re-anchored on arbiter origin/main 526a721 (v0.13.0, Plan S3-A merged as arbiter#133). Plan S3-A Task 9's real code matches what the round-1 draft assumed from the plan text. New anchors: fsm/snapshot.go imports :3-17, version check :285-287, document read :296-299, fsmSnapshot block through Snapshot() :737-782; FSM struct :39-53 (stateSize :52); state_size.go ContainerBytes comment :56-57; config.go RaftConfig :31-44, trailing_logs check :569-571; raw.go :50-63, :134, :223-228; services.go newFSM :62-67; metrics.go New 'return m' :151 (stateSizeReader is in metrics.go:19-25, not metrics/snapshot.go); README paragraph :429 and the arbiter-state-size YAML fence :593; issue-31-metrics.md row :48 and sentence :50.
- Task 66's drafted tests read the plain container through writeSnapshot (its helper plainSnapshotBytes) and forge body kinds 2 and 255, so Task 67 changes none of them. Step 10 keeps a short fallback in case Task 66 changes.
- Step 9 now replaces Task 66's kind-byte block together with its comment. The comment said 'This binary reads the plain body', which becomes stale once kind 1 exists.
- Step 27 adds a docs/specs/issue-31-metrics.md edit: the sentence 'All series except the read cap and the failure counters are absent...' now also names the two v21 encoding series, which are exported from the start. Step 21 now also runs ./integration/chpipeline/, whose cluster_test.go this task edits.
- Plan S3-D Task 51 edits lines next to this task's in config.go, raw.go, cmd/arbiter/main_test.go, both integration testConfigs and metrics.New, but not the Raft lines Task 67 changes. This is covered by the hunk rule in the anchors paragraph.
- Interned body measured again on the local devnet2 copies: v20-state.bin 11,385,138 to 1,070,604 bytes; S2-B arbiter-0 after tracks 11,868,252 to 1,089,643. Both rehydrate identically, and the numbers equal the round-1 draft's.
- The frozen interned golden produced on top of Task 66's drafted plain golden (175,302-byte container) is 45,482 bytes, sha256 e21085dc...; header bytes 4146534d1501. Two independent application passes produced identical bytes.
- Local inputs currently present: only $HOME/si-devnet2-multisource-tracks (Plan S2-B). The S3-A rehearsal dir, the S2 rehearsal samples and $HOME/si-devnet2-stage3-p0 do not exist yet, so Step 34 stops until Plans S3-A/S3-B produce them, as the contract §48 design intends.
- Names added while drafting: fsm: maxInternedDepth = 10000, internedBodyPrefix, snapshotInterner (value, container), appendInternedRef, snapshotRehydrator (measure, measureValue, measureRef, emit, emitValue), parseInternedBody, internedArray, parseInternedRef, addRehydrated, skipJSONValue, scanJSONString, scanJSONLiteral, isHexDigit, internedMalformed; fsm: snapshotEncodingState{mu, mode, stats} with bodyMode(), the field FSM.encoding, snapshotPersisted, (*fsmSnapshot).encode, verifiedInternedBody, (*fsmSnapshot).recordEncoding, snapshotBodyKindV20/Plain/Interned (v20, plain, interned); fsm reader refusal prefix 'snapshot v21 body: ' around RehydrateSnapshotBody errors; config.stringOrDefault; metrics: snapshotEncodingCollector, newSnapshotEncodingCollector (registered in New only for a source that implements snapshotEncodingReader); test helpers: persistedSnapshotBytes, v21Parts, internedCorpus, stage3V21InternedGoldenPath, encodingSnapshot; the shared snapshotBytes now persists in plain mode; extra tests: TestRehydrateRefusesMalformedBodies, TestInternSnapshotBodyRefusesWhatItCannotRepresent, TestSnapshotEncodingStatsAreRaceFreeAgainstPersist, TestNewFSMAppliesTheRaftSnapshotBodySetting; local only: $T/helpers.sh (rehearsal_build, digest_lines, rehearsal_replay, rehearsal_v21size), $T/v21check/main.go (prints 'restore_ok|restore_refused version N body none|plain|interned'), $T/v21size/main.go (prints 'plain_bytes N interned_bytes M rehydrated_identical B'), $T/ledger.env, $T/tools.sha256, and the evidence line formats of Steps 35-39
- Verified while drafting: Scratch: work/e2/task-67-r2/arb, a git archive of arbiter 526a721 with these stand-ins: Task 64's drafted commits (task-64-r2 HEAD f0abe6f), Task 66's drafted text, arbiter-proto at Plan S3-E1's contract §39 (proto5) and an arbiter-core §41 stub (core5) as replace modules. All Go runs used GOWORK=off GOFLAGS=-mod=mod GOPROXY=off. / TDD order checked step by step. Step 3 fails to compile (undefined InternSnapshotBody/RehydrateSnapshotBody/maxInternedDepth). Step 5 passes with 28 subtests. Step 8 fails to compile (undefined SnapshotEncodingStats, internSnapshotBodyFunc). Step 10 passes except the uncaptured golden. Step 11 prints 'captured', then 3 PASS, a refused second capture (1) and 4146534d1501. Step 12: fsm under -race ok, 9 golden/state-size PASS lines, status as stated. Steps 15/17, 19/21 and 24/26 behaved as stated. / Full suite: go test ./... -count=1 all ok. -race ok for fsm, metrics, config, cmd/arbiter, integration and integration/chpipeline. gofmt -l empty and go vet ./... clean; no time.Now in fsm/. / Nine single-point mutations each failed their tests: digest compare dropped, plain mode counting a fallback, reader keeping the stored length, self-reference admitted, root interned, expansion cap dropped, stats not recorded, reserved-key check dropped, mode not taken from the FSM. / Second clean pass: every code block was re-extracted from the final markdown and applied by a script that follows the step instructions on a fresh clone. The resulting tree is byte-identical to the first pass, golden included. / Bazel: offline gazelle ran and Task 67 adds exactly snapshot_interned.go and snapshot_interned_test.go to fsm/BUILD.bazel. The scratch replace also rewrote labels, which is a setup artefact and was reverted. bazel test could not run offline because the repository cache lacks 526a721's module versions. It tried one git ls-remote, which failed, and was not retried. / Rehearsal Steps 30-41 ran end to end under a scratch HOME. Stand-ins: Plan S2-B's tgz copies for the P0 copies, the round-1 sample copies, S3-A tool sources with the recorded checksums c5168a19/fccc6a87, a git clone as W, and v0.13.0 (526a721 tree) as ARBITER_TAG_P4, with the ledger/fetch block replaced by a stand-in. Results: 6 identical replay lines, the 11,868,827-byte v20 container from both builds, every cross restore restore_ok, the old build refusing both v21 goldens at the version byte, 8 rehydrated_identical true lines, 24 evidence lines, and $T removed. / bash -n passes on every fenced bash block and every inline Run command. shellcheck (helpers.sh prepended) reports only SC1091 info. 4 commits end with the Co-Authored-By line; 3 confirmation lines and 3 'Confirm before running.' markers (fetch, module download, rm -rf). No placeholders, and no hard-wrapped prose.

### Task 68

- The task has 14 steps. Step 1 checks the worktree and its seams. Steps 2-3 add the failing tests and show them failing. Step 4 creates fsm/eviction_voiding.go. Steps 5-9 are the call-site hunks in apply.go, rejected_cleanup.go, signed_claims.go, threeway.go and reads_work.go. Step 10 is the exact onStage3SwitchOnLocked hunk. Step 11 is the README paragraph. Steps 12-13 run the tests and the checks, and Step 14 is the one commit 'feat(stage3): eviction voiding, switch 2 (Task 68)' ending with the Co-Authored-By line.
- Every refusal is '%s from %s: the node is evicted (stage3 eviction_voiding)'. It sits right after each site's own signature check: applyRegisterRC (result_claim, before park/bind), applyRecordAttestation (attestation), applyRecordByteSideScan (byte_side_scan), applyRecordPromotionAck (promotion_ack), applyRecordCleanupAck (cleanup_ack) and recordRejectedCleanupAckLocked (cleanup_ack). None is inside snodeSignatureLocked or verifierSignatureLocked.
- Line anchors at arbiter fb42260 are byte-identical at origin/main 526a721 for apply.go, rejected_cleanup.go, signed_claims.go and threeway.go. reads_work.go differs elsewhere, but :20-26 and :139 are the same at both. Intro anchors corrected from round 1: orchestrator/dispatch.go:32-80 and fsm/genesis_validation.go:540-593.
- Task 64's current stand-in has `func (f *FSM) onStage3SwitchOnLocked(name string) { switch name { default: } }`. Step 10 replaces exactly that and adds `case Stage3SwitchEvictionVoiding: f.reevaluateVoidedBlocksLocked()` before default. Task 69's round-1 draft already expects to add its purge case in front of it.
- The snapshot-query lane needs no change: applyServerOwnedSnapshotQueryAttestation (fsm/artifact_snapshot_query_attestation.go:36-90) already requires an Active verifier at :63, the Raft command RecordSnapshotQueryAttestation is refused (fsm/apply_unscoped_paths.go:13-15), and nothing outside tests calls the seam. MarkActive (apply.go:108) and purge reports (apply_table_registry.go:382) already refuse evicted nodes.
- The guard `if f.evictionVoidingLocked()` in applyEvictNode is an identity guard only. With the switch off, re-evaluation would reproduce the same verdicts, so a mutation that removes it is not observable. Every other check is pinned by at least one named test (mutation run below).
- With the switch on, an evicted SNode can no longer acknowledge its own promotions or cleanups. The drain gate (fsm/quarantine.go:215-254) then refuses every consensus update until its track is quarantined, so quarantine-before-evict (spec §10.2) becomes mandatory from switch 2 on. Both the README paragraph and the task notes say this.
- Names added while drafting: func (f *FSM) voidedReplicaLocked() func(replica string) bool (returns nil while eviction_voiding is off, else evictedNodeLocked; the WorkSet and reevaluateBlock use it); func (f *FSM) reevaluateUnlatchedBlocksLocked(names func(set []string) bool) (the shared loop under reevaluateBlocksNamingLocked and reevaluateVoidedBlocksLocked); Test fixtures in fsm/eviction_voiding_test.go: voidedRefusalV, purgeOnlyFSMV, voidingFSMV, evictV, markedBlockV, evidenceV, recordEvidenceV, verdictV, verdictJSONV, promotedBlockV, rejectedCleanupV, parkedClaimV, type evictedMessageV, evictedMessagesV; README.md paragraph '**Eviction voiding (switch 2).**', inserted right after Task 65's '**Switches.**' paragraph in '## Multi-source storage integrity (stage-3 activation)'
- Verified while drafting: Scratch tree work/e2/task-68-r3/arb: arbiter origin/main 526a721 plus the current stand-ins. Task 64 is task-64-r2 HEAD f0abe6f, Task 65 is task-65-r2's fsm and README parts, Task 66 is the task-66-r2 working copy at 05:05 with its plain golden captured once in scratch, and CORE_TAG_P5/PROTO_TAG_P5 are the core5/proto5 replace modules. Tests ran with GOWORK=off GOFLAGS=-mod=mod GOPROXY=off. / Step 1 commands on the pre-68 tree print 3, 3, 1, then nothing. / Step 2 then Step 3 (-race): the package compiles and the FAIL output, timings stripped, is byte-identical to the Expected block in Step 3. The same holds on a second stack, task-67-r2/arb HEAD e63f3e5 (Tasks 64, 66 r1 and 67). / Steps 4-10 applied from the markdown by a parser (tools/apply_md.py). Every quoted old block matched exactly once. / Step 11: the README paragraph went in after the **Switches.** paragraph. The grep prints 1 and `git diff --numstat` prints `2\t0\tREADME.md`. / Step 12 (-race -v): the 12 named tests PASS, and the output, timings stripped, is byte-identical to the Expected block. No data race. / Step 13: gofmt and vet are clean. `go test -race ./fsm/ ./orchestrator/ ./server/` is ok on both stacks; on the Task 67 stack ./metrics/ and ./config/ are ok too. All the listed golden and TestDeterminism_* tests PASS, `grep time.Now fsm/` prints nothing and fsm/testdata is unchanged. / Bazel 9.1.0 offline, tree = origin/main archive plus the fsm/server overlay. `bazel run //:gazelle` adds exactly eviction_voiding.go to //fsm:fsm and eviction_voiding_test.go to //fsm:fsm_test, with no deps. `bazel query 'deps(//fsm:fsm, 1)' | grep -c com_github_sentioxyz_arbiter_proto` prints 0. / Bazel with --override_module=arbiter_core=<core5 stand-in> and --override_repository for arbiter-proto: `bazel build //...` succeeds, and `bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors` passes all 22 test targets. A -test.v run of //fsm:fsm_test lists the 12 named tests as PASS. / Mutation run: 17 mutations. Every refusal site, the K72 discard, the reevaluateBlock skip, the blockChallenged exemption, the evidencePresence voiding, the EvictNode and switch-on re-evaluation, the verdict-nil skip, the Evicted predicate, and the switch guards in evictedSignerRefusalLocked and voidedReplicaLocked were each killed by a named test. The only survivor is the redundant switch guard around the EvictNode re-evaluation, which has no observable effect. / Step 14's bash block, with W pointed at the scratch tree: bash -n and shellcheck are clean, it commits 9 files with the stated subject and the Co-Authored-By line, and the status is empty afterwards. Every bash block and inline Run command passes bash -n. shellcheck warns only SC2046 on the contract's own gofmt command `$(git ls-files '*.go')`, where word splitting is intended.

### Task 69

- Base: arbiter origin/main is now 526a721 (v0.13.0, Plan S3-A merged as arbiter#133). Every source file Task 69 edits (fsm/table_registry.go, fsm/apply_table_registry.go, server/table_registry.go, server/table_registry_test.go) has the same anchors as fb42260. Only fsm/BUILD.bazel (purge_owner_test.go at :174) and README.md (the heading '## Artifact disposition admission' at :396) moved.
- Task 64's 2026-10-16 draft put stage3PendingProtoFields in server/safestate_stage3_test.go, not in server/table_registry_test.go. Its two TableIncarnation entries are valued "Task 69" and are the last two in the map. Task 69 deletes them in its FSM-half commit, and from then on TestTableRegistryMirrorsProto pins both JSON tags.
- purgeRequiredLocked with purge on: the owner's SNode (registered, not evicted), every node for which poolVerifierLocked holds, and every registered, non-evicted VERIFIER-role node that inc.PurgeMembers names. A named node that never registered is not awaited (spec §8.3: a member creates replicas only while registered). A named node of another role could never report (purgeReportRefusalLocked), so it is not awaited either. With purge off the function returns exactly the base set.
- Rule 6 (fsm/signed_claims.go:277-292) evicts every running verifier the list does not hold. K68 refuses host-bound entries until owner_aware_selection is committed. So at the purge switch-on the pool equals every non-evicted verifier, purge_members is empty, and the hook's completePurgesLocked rerun completes nothing in any committed history. TestPurgeSwitchOnWithNoMembersChangesNoPurge pins this; the white-box test drives the hook directly.
- Order inside the switch-on Apply: Task 65's registry bump for the changed stage3 runs first, then recordStage3SwitchesLocked calls onStage3SwitchOnLocked (Task 64's order). A purge completed by the hook would therefore bump the version a second time in the same Apply. This is harmless and unreachable in committed histories (recorded for Appendix B).
- Scoped rule: chain incarnations are appended only by applyAddTable (fsm/apply_table_registry.go:260-271). Genesis and Legacy incarnations (:63, :113, table_registry.go:261) are never scoped. Refused incarnations are marked too (contract §47). Restore requires scoped iff purge is on, origin is chain, and seq >= the purge cutover's IncarnationSeq. validateStage3Activation runs first and guarantees the cutover exists and is in 1..len+1.
- Fixture facts (stage3FSM of Task 64): incarnation 1 is genesis db.t (owner 1), 2 is Legacy db.old, 3 is chain db0.x (owner 0); the registry cursor is at block 151; registry version 9; consensus epoch 4. Task 69 tables are created from block 160 on. Members are listed through memberEntry plus a re-signed consent (snodeSigner s0/s1), with keys ed25519KeyFor('a'/'b'), and they register, report and are evicted through the signed base helpers.
- Task 67's draft overrides the test helper snapshotBytes to persist the plain body, so Task 69's byte checks (data[4], bytes.Contains, round-trip identity) are unaffected by the interned body.
- Patch-tool hazard: tracksFieldsPresent sits directly above stage3FieldsPresent in fsm/snapshot.go and ends with the same 'return false'. A fuzzy 3-way apply put Step 10's hunk into tracksFieldsPresent. Step 10 names the function and warns about this, and TestRestoreRefusesScopedKeeperPathAndPurgeMembersInAPreV21Container catches a misplaced hunk.
- Task 69 makes no remote or live action. The only guard concerns go/bazel downloads: the caches are expected warm from Tasks 64-68, and any download is treated as a remote read that needs explicit user confirmation in chat. No commands are handed to the user, so no fish is needed.
- Two commits: 'feat(stage3): purge switch: scoped incarnations and member-aware purge sets (Task 69)' (fsm plus server/safestate_stage3_test.go plus fsm/BUILD.bazel) and 'feat(stage3): serve scoped_keeper_path and purge_members in the table registry (Task 69)' (server/table_registry.go, server/table_registry_test.go, README.md). Both end with Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>.
- Scratch work: <scratch> It contains arb (branch t69), mdcheck, replaytree, arb68, arb67, mut/mutate.py, apply_md.py, replay.py, build.py and task-69.template.md.
- Names added while drafting: fsm test helpers in fsm/purge_stage3_test.go: purgeTestAdd, purgeTestRetire, purgeTestPurging, purgeTestSeed, purgeTestMember, purgeTestMembers, purgeTestReport, purgeTestEvict, purgeTestRoundTrip; Extra fsm test beyond contract §47: TestPurgeSwitchOnHookCompletesAPurgeThatWaitedOnlyForAMember (white-box test of the switch-on hook on a hand-built state); Extra fsm test beyond contract §47: TestRestoreRefusesScopedKeeperPathAndPurgeMembersInAPreV21Container (the stage3FieldsPresent arm under a v20 version byte); README.md paragraph '**Purge switch**', added as the last paragraph of Task 65's section '## Multi-source storage integrity (stage-3 activation)', right before '## Artifact disposition admission'
- Verified while drafting: Built the scratch tree as a clone of the sibling composite <scratch>/work/e2/task-68-r3/arb at 1c3fc58. That tree is arbiter 526a721 export plus a scratch pin, Task 64's draft (fsm/stage3.go and the fixture are identical to Task 64 r3's working copy), the Task 65 fsm and README stand-in, the Task 66 stand-in, and the captured plain golden. It uses the arbiter-proto and arbiter-core stubs of Plan S3-E1's contract via replace (proto5, core5). Tests ran with GOWORK=off GOFLAGS=-mod=mod GOPROXY=off go test. / TDD observed exactly as the steps state. Step 4: fsm build failure (purge_stage3_test.go:187:12 ScopedKeeperPath undefined) and TestTableRegistryMirrorsProto failing with 'proto field "scoped_keeper_path"/"purge_members" has no json tag'. Step 6: 4 pass and 9 fail with the listed messages. Step 12: 13 named tests pass under -race, the whole fsm and server packages pass, and vet is clean. Step 16: 'a scoped incarnation sent scoped_keeper_path false and purge_members []'. Step 18: the server tests pass. / go test ./... -count=1 and go test ./... -race -count=1 pass. go vet ./... is clean, gofmt is clean, there is no time.Now in fsm/, and fsm/testdata is unchanged against the pre-task head (the goldens-unchanged command was checked in the Step 20 state). / Ran 25 single-point mutations (marking, purge set, pool, owner arm, hook rerun and bump, view clone, stage3FieldsPresent arm, the restore call, each of the six restore rules, off-by-one, empty id, repeats, transport mapping). Every one is caught by at least one named test. / Built the final markdown from the verified files with build.py and applied it with apply_md.py (Create/replace/append blocks) to a fresh clone at 1c3fc58. The result is byte-identical to the verified tree (diff -r: trees-identical). / Replayed the markdown step by step (replay.py) on a fresh tree, running every bash block except bazel lines and commits with W pointed at the scratch tree. The outputs match each step's Expected text. / Compatibility check against Task 68: applied its in-progress working diff on top of Task 69. The single onStage3SwitchOnLocked conflict resolved by the hunk rule (purge case first). fsm, server and orchestrator tests all pass. / Compatibility check against Task 67: Task 69 on Task 67's draft tree, which lacks Task 65, passes every test except the one-bump assertion of TestPurgeSwitchOnWithNoMembersChangesNoPurge. That is expected, because the assertion depends on Task 65's bump. / All 10 bash blocks pass bash -n, and shellcheck -s bash is clean (the gofmt checks were rewritten as 'gofmt -l fsm server' and 'git ls-files -z | xargs -0 gofmt -l', which are equivalent to contract §48's check). No hard-wrapped prose, no placeholders. / Not run: Bazel and gazelle (offline, and the P5 tags do not exist yet); the fsm/BUILD.bazel change is stated as what gazelle writes (purge_stage3_test.go after purge_owner_test.go, no deps change).

### Task 70

- arbiter origin/main is now 526a721 (v0.13.0, S3-A merged: fsm/reads_verification.go, server/safestate_stage3.go and its test exist). The task's line anchors use 526a721. For fsm/state.go, select.go, apply.go, admission.go and orchestrator/dispatch_test.go those lines are the same as fb42260.
- Base anchors at 526a721: BlockVerification fsm/state.go:269-279 (hunk :277-279); selectVerifiers fsm/select.go:55-76, insertion before :78; applyMarkReplaying selection fsm/apply.go:439-443; SOURCE_UNAVAILABLE block fsm/admission.go:133-147 (hunk :144-147); WorkSet type fsm/reads_work_types.go:72-94 (hunk :91-94); WorkSet loop head fsm/reads_work.go:109-111 and unmarked branch :132-136; BlockVerificationView fsm/reads_verification.go:48-58 and copy line :99; blockVerificationToPB server/safestate_stage3.go:144-146; server test view :94-97 and want :157-158.
- poolRingLocked is activeNodesWithRole(VERIFIER, sources) filtered by Task 64's poolVerifierLocked. It equals today's selectVerifiers ring while no host-bound entry is listed, because of rule 6 of signedClaimsMembershipLocked (fsm/signed_claims.go:277-288).
- Every block sealed after the tracks activation carries a track: statement blocks via prepareStatementSealLocked, transition blocks via plan.owner (fsm/table_set_transition.go:449-453). tracksActivationRefusalLocked requires every earlier block to be safe, hence marked (fsm/tracks.go:118).
- The ingress already counts every non-ACCEPTED code by proto name (server/ingress.go:108), so VERIFIERS_UNAVAILABLE needs no server change beyond the converter.
- arbiter CI: ci.yml runs on pushes to main and on pull_request only; cut-release.yml is workflow_dispatch only. A branch push therefore starts no workflow.
- No hunk of Task 70 overlaps the drafts of Tasks 64-69. Task 68 touches evidencePresence and its call; Task 69 inserts before the same final returns, which is handled by position.
- Names added while drafting: func (f *FSM) blockRingLocked(bv *BlockVerification) []string (fsm/select.go; the single ring MarkReplaying and the WorkSet unmarkable rule compute: eligibleRingLocked of the header's track owner over bv.SourceNodes, pool for a trackless block; Plan S3-E3 Task 73 passes the group seal here); func blockSeededPosition(blockSeq uint64, n int) uint32 (fsm/select.go; (ringStart+2)%n, shared by MarkReplaying and restore); func verificationHasStage3Fields(bv *BlockVerification) bool (fsm/stage3_restore.go); func ownerAwareSelectionCutover(st *State) (Stage3SwitchCutover, bool) (fsm/stage3_restore.go); func ringIsSortedAndUnique(ring []string) bool (fsm/stage3_restore.go); fsm test helpers: ownerAwareOffFSM, addPoolVerifiersOA, resyncVerifierOA, reactivateVerifierOA, evictVerifierOA, claimedBlockOA, markReplayingCmdOA, markReplayingOA, verificationJSONOA, verifiersUnavailableOA, restoresBothWaysOA, forgedVerificationOA
- Verified while drafting: Built the scratch base <scratch> It is an export of arbiter 526a721 with stub replace modules core5/proto5 for Plan S3-E1, plus stand-ins from other drafts: Task 64 (r2), Task 65's fsm part, Task 66, its plain golden, and my own minimal Task 69 (TableIncarnation fields, the stage3FieldsPresent arm, the validateStage3Incarnations call, its pending entries removed). Baseline go test of fsm, server and orchestrator was green. / TDD on that base: the Step 4 command failed to build with exactly the stated errors (ws.Unmarkable undefined, undefined initialVerifierSet and blockSeededPosition, ...; the orchestrator's three errors). After Steps 5-13 all 13 fsm tests and the orchestrator test passed under -race. Step 18 failed as stated and Step 20 passed (5 named server tests and the whole server package under -race). / The markdown was applied from its own prose by an independent parser (tools/apply_md.py) to fresh clones at the pre-Task-70 commit. The result was byte-identical (diff -r) to the hand-applied tree, three times, after each wording change. / GOWORK=off GOFLAGS=-mod=mod GOPROXY=off go test ./... -count=1 passes on every package. go vet ./... is clean, gofmt is silent and grep time.Now fsm/ is empty. The goldens pass and fsm/testdata is unchanged. / 33 single-point mutations, 29 scripted (tools/mutate.py) plus 4 that move the admission check (before quarantine, before the source check, after the lane budget, after the insert), were each caught by a named test. / The Step 1 script was run against the pre-Task-70 tree: no 'missing' lines, the count was 3, and the final grep printed nothing. All 7 bash blocks and 5 Run: lines pass bash -n and shellcheck -s bash with no findings. / Bazel was not run: there is no offline Bazel output base or module cache. Step 22 runs gazelle, build and test on the real branch.

## Appendix C: execution record

Written by the plan consistency pass from the release ledger table above (one row per ledger name this plan produces, empty until recorded, in the S3-B D23 shape), together with the run-record list. Not written yet: see `2026-10-15-multi-source-si-s3cf-status.md`.
