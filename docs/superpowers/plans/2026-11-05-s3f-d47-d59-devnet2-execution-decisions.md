# Plan S3-F decisions D47–D59: the Keeper gate, step 6 and the exit on devnet2 (user decisions, 2026-11-04 and 2026-11-05)

Plan S3-F's Tasks 86–89 ran on devnet2 from 2026-11-04 to 2026-11-05 and are recorded in Appendix C of `2026-10-15-multi-source-si-s3f-p6-switch-on.md` (housegate#309, #310, #312, #313). On the way the plan's text met devnet2 as it is: a shared production Keeper whose own load already exceeds some of the plan's thresholds, an operator that restarts ClickHouse on a users-only change, and SI monitoring that has been off since the user's decision of 2026-11-03. Each mismatch stopped execution and was put to the user. This document records those decisions (D47–D59), with the user's words, and the execution rulings taken alongside them.

The ledger records already cite D47–D59 by number. This document is their binding text. It changes no code, runbook fence or execution step of the plan; those runs are finished. It adds one header line to the plan.

## Context

- **Standing authorization.** On 2026-11-04 at 05:48:01Z, during Task 86, the user said 「你可以自己驱动整个流程吗，不需要每个step都来问我，另外production repo尽量多几个change再commit，不要频繁commit」. From then on the main session drove the plan's steps without per-step confirmation, irreversible ones included. It still stopped for keyed user-terminal writes (D40), for failures and unknown outcomes, and for anything outside the plan. Every decision below came out of such a stop.
- **Monitoring off.** On 2026-11-03 (Task 83 review I-1) the user decided 「不需要7天连续历史和monitoring，这里记录就可以了」: devnet2 SI monitoring stays off for S3-F, and no exit alert is committed. D50–D58 are what that decision means for Task 89's text, which assumed VictoriaMetrics history throughout.
- **The shared Keeper.** `clickhouse-keeper-extra` is shared with production `clickhouse-extra`. The user is that cluster's owner: asked for the K86 owner agreement, the user answered 「我就是负责任」 (2026-11-04).
- **D41, D45 and D46 still hold.** Every devnet2 group has one member, so at k = 2 `safe_copies` is exactly 2, on `node:sea-dg-7` and `node:sea-dg-3`.

## The user's decisions

### Task 87: the Keeper gate and step 6

**D47 (2026-11-04): one fresh sample, latency gate 20 ms.** A read-only snapshot of the shared Keeper on 2026-11-04 showed SI at about 0.3% of its znodes and a capacity projection far inside every limit, but keeper-1's `zk_avg_latency` at 13 ms, over the plan's 10 ms, from production load alone. The user answered 「免采样，延迟阈值放宽到 20ms」.
- The K86 baseline of ≥ 24 h and ≥ 6 samples is waived. The gate runs on one fresh three-replica sample taken immediately before step 6.
- The average-latency gate is 20 ms. The latency abort is > max(20 ms, 2 × the baseline peak).
- Every other K86 threshold and every other step-6 abort criterion is unchanged: working set, data and du, free space, leader change, Keeper restart.
- Ruling: D47 also covers §8.5's final Keeper check, which is step 6's own. The 1 h sample-freshness rule of the gate block was kept.

**D48 (2026-11-04T06:45:26Z): accept the operator's software restarts on the DDL-user CHI applies.** The first `si_member_ddl` apply (D-b, indexer-b) made the Altinity operator decide "Config change(s) require host restart" and software-restart the source ClickHouse (about 20 s, same pod UID, container restart count +1). The plan permits no ClickHouse restart there, and the operator's restart policy names no rule for users or that profile, so the exact trigger was not found. The user answered 「接受，继续 c」.
- The restarts are accepted for the three DDL-user applies (D-b, D-c, D-a). D-b and D-c each software-restarted their single source host once; D-a, on the network CHI, restarted its four hosts (source and verifier-1/2/3) once each. Every pod kept its UID.
- §8.2's before/after CHI comparison is replaced by: the same pod UID, a restart explained by the operator's `hostSoftwareRestart` in the apply window, ClickHouse answering, and no quarantined track.
- All other read checks are unchanged: exact grants, merge setting 0, CHI Completed, no `si_cnp_drops`. Each host's probe then passed.

**D49 (2026-11-04T07:54:31Z): outstanding-requests limit 50.** Host b's reconciler hour aborted on "Keeper 1 outstanding over 10 in consecutive samples" (keeper-1 at 0, 1, 17, 17, 20 while latency stayed at 13 ms). member-b is in no group, so reconciler-b had created nothing; SI's Keeper footprint was unchanged. The user answered 「放宽到 50，重跑窗口」.
- The outstanding gate and abort are > 50 in two consecutive samples, in every step-6 Keeper check, the hour-end verification included.
- Host b's hour window was rerun in full. The three hosts' windows then passed with no SNode restart and no Keeper UID, leader or restart change.

### Task 89: k = 2 and the exit with monitoring off

The prep for Task 89 found that every VictoriaMetrics consumer of the plan reads nothing on devnet2: there are no `network="devnet2-si-v2"` series, the voters' metrics Service is not rendered, and the chart refuses an exit alert without monitoring. Several of those blocks would write FAILED.json on the empty result. Ten proposals were put to the user. The user answered on the soak length only (D53). The other nine were taken as recommended, and the main session says so here rather than claiming an explicit answer for each.

- **D50: the §12.4 preflight becomes a direct-read capture.** The monitoring preflight before the k2 update reads pods, Endpoints, member metrics, Keeper and independence directly, and records monitoring off. The update itself is unchanged.
- **D51: the exit alert (op X) is skipped.** No production commit, `DEVNET2_P6_EXIT_ALERT` stays false, custody op X is never advanced, and soak-start drops its vmalert check.
- **D52: a direct-read audit replaces the VM-history daily audit.** Each observation checks each voter's held-cleanup age (≤ 1800 s) and cleanups, `under_replicated_groups`, the four `under_replicated_total` reasons against the previous read under the same process identity, member checkpoint progress, Keeper and budget. It makes no continuous-coverage claim.
- **D53: the soak is fifteen minutes.** The user's words, in order:
  - 「目前devnet没有任何流量和user/client，所以我觉得不需要soak 7天。这里调整下吧，最好改成小时级别」 (2026-11-04): two hours, an observation every 30 min;
  - 「改成1h吧」 (2026-11-04): one hour, observations at t0, +20, +40 and +60 min;
  - 「我想再压缩一下soak的时间，改成15分钟」 (2026-11-05T03:07:47Z): fifteen minutes, which binds.
  The run is 900 s with observations at t0, +5, +10 and +15 min. Each is two complete reads ≥ 60 s apart inside a 280 s window, the first starting by s + 45 s. The user reviews the run once, after the last observation, in place of seven daily reviews.
- **D54: one detached read-only observer** takes the observations; nothing in it mutates.
- **D55: the D47/D49 Keeper limits apply to every Task 89 Keeper check**: §12.3, every soak sample, every closing (each DROP's and 14-end's) and §15.4's leaving read. Average latency ≤ 20 ms, outstanding ≤ 50, the same aborts. The leader and restart rule stays as the plan has it: any leader change or restart of the shared Keeper during the run is a breach. D49 and D55 are transcribed into the Keeper agreement record beside D47.
- **D56: the §14 DROP guard and 14-end use a direct-read closing.** A fresh two-read copy proof, k2, Keeper, budget with lifecycle reserve, all voters at `under_replicated_groups` 0, and the counter tail against the last observation, with a tty review if anything rose.
- **D57: no restarts and no RebalanceGroup during the soak.** Any change of UID, IP, node, restart count or start time of the nine bound pods fails the run. Under D46 a member-c restart at k2 would also under-replicate owners a and b.
- **D58: the record states the gap.** Task 89's record drops the three alert receipts and says monitoring off, direct reads and no continuous history. Its under-replication sentence is generated from the evidence. Every other validation is kept.
- **D59: the promotion bound stays 30** (reserve 39; 15 spent of the original half of 384).

## What is weaker than the plan, and why it was accepted

- **No continuous history.** The exit soak proves the copy floor, held-cleanup age and counters at its reads (two per observation, two per closing), not between them. K87 (a)'s held age is unproven between reads.
- **Fifteen minutes instead of seven days.** Spec §10.4's seven-day exit criterion was not run.
- **No exit alert.** Nothing would page on an under-replicated devnet2 group today.
- **Keeper limits relaxed by its owner.** Latency 20 ms and outstanding 50 instead of 10 and 10, from one sample instead of a day's baseline.

The user accepted these because devnet2 has no traffic and no users or clients: a long soak would watch an idle network, and the shared Keeper's latency and outstanding values come from production load, not SI. The user owns `clickhouse-extra` and set its limits. The record states each gap in its own words, so that nothing in Appendix C reads as a seven-day or continuously monitored exit.

## Execution rulings (not user decisions)

Each was taken by the main session within the plan, recorded in the task's ledger text, and, where it changed tooling, independently reviewed before use.

- **Custody: adopt-upstream (Task 86).** Production `main` moved with unrelated commits between phase commits, which the custody chain could not follow. The user chose 「扩展 custody 支持 fast-forward (Recommended)」. The custody tool gained fast-forward-only adoption of upstream commits, refused if any adopted commit touches a protected SI path (storage-integrity docs, charts and scripts, `charts/sentio-node`, the devnet2 overlays), with receipts in the same hash chain. Review: REJECT, then APPROVE_WITH_FIXES; all findings fixed or ruled acceptable before first use. Six adoptions were made in Tasks 86–88.
- **Custody: rebase-phase and a stable run id (Task 87).** After R-b's local commit, production `main` moved within about 75 s (an unrelated bot commit); the push guard stopped before publishing and nothing changed live. The user chose 「方案1吧」: the run id is pinned to a recorded custody hash, so a tooling repin no longer invalidates earlier receipts, and an unpublished phase commit can be rebased with identical bytes under its own receipt. Reviewed APPROVE_WITH_FIXES, then approved. Used once: R-b `ad174d749` became `0d8ed9330`.
- **Task 88: empty checkpoint book.** Before `checkpoints` the sequence book is served empty, which the brief's convergence verifier cannot pass. Ruling: a pair absent from that book counts as sequence 0 only when its baseline rows show `checkpoint_seq` 0 and no entry; every other refusal unchanged (twelve negative fixtures refused). Cited in housegate#312, which superseded #311 after the review bot asked for that citation.
- **Task 89: 7001 recovered, not repeated.** The INSERT 7001 committed (track 0, block 183) but its wrapper's safe readback hit an agent-side `error: EOF`. Its safe state was proven read-only: exactly one row 7001 in the owner's `hg_safe` table, and block 183 final, quorum and keyed. The write was never repeated and no done receipt was forged; the record accepts the recovered receipt.
- **Task 89: the first soak attempt.** It hit the same EOF while capturing its baseline, before `run.json` or the current-run pointer existed, so no run existed. Its partial evidence was kept, and a fresh run with fresh labels, run-bm0pUek6, passed: observations 0–3 passed, no events. The user reviewed it on 2026-11-05T04:14:59Z: 「确认通过」.
- **Smaller ones**, all in the ledger text: a BSD `seq` guard in Task 86's host check; owner a's acceptance key devuser3 and stdin from `/dev/null` (as in Task 85); the helper chain's rebinding of `R` fixed in the user blocks; host b's evidence split so the passing D49 window stands alone, with the aborted samples kept beside it.

## What changes in Plan S3-F

- **Header:** one amendment line after D46's, pointing here.

Nothing else. The ledger texts of Tasks 87–89 (Appendix C) already cite these decisions; their pins and run records stand as merged.

## What does not change

- D41, D45 and D46, and k = 2 with every group at one member.
- No arbiter, arbiter-core, sentio-node, housegate or chart code change. No spec change. Strict mode stays off.
- The plan's thresholds for any other network. D47–D59 are limited to devnet2 under this plan's run.
- The runbook fences, markers and the pins of Appendices A and B.

## Evidence

- Task 87 record, housegate#310: KEEPER_HEADROOM and STEP6_END, the D47 sample maxima and thresholds, the D48 restarts, the aborted and rerun host-b window, and the six signed production commits.
- Task 88 record, housegate#312: the empty-book deviation with both verifier hashes.
- Task 89 record, housegate#313: K2_EPOCH 16, P6_END 2026-11-05, the 7001 recovery, the run run-bm0pUek6, the fifteen-minute wording, and the public record sentioxyz/production#826 (merged 2026-11-05).

## Follow-ups

- **Re-enable devnet2 SI monitoring**, with its delivery proof (S3-B K13), and then commit the exit alert (op X) in its own window, as the P5 MON-rule precedent did. Any longer soak would want this first.
- **The DDL-user restart.** Find what in the users/profile change makes the operator restart, before a CHI apply of that kind on a network with traffic.
- **Flaky GitHub SSH auth.** Roughly 30–50% of git network operations were refused with `Permission denied (publickey)`; a retry wrapper for that error only carried the run.
- **The local proxy cuts long `kubectl exec` streams**, the `error: EOF` behind the 7001 readback and the first soak attempt. Long reads need a path that survives it, or a retry that proves the step's outcome first, as 7001's recovery did.
