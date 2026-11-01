# Plan S3-D decision D44: devnet2's exception to D42's post-enforce backup check (user decision, 2026-11-01)

Decision D42 (`2026-10-29-s3d-d42-devnet2-census-1h.md`) shortened the devnet2 census to one hour. Because a one-hour window cannot see daily clients, D42 added a read-only check before Task 56 starts. The first scheduled backup run of each host after its installation's enforce must meet both of these conditions:

1. its Job is `Complete`;
2. `si_cnp_drops` shows no `POLICY_DENIED` flow towards that installation's ClickHouse since the enforce.

Task 55 recorded the check as `task55-backup-after-enforce.txt` (evidence directory `$HOME/si-devnet2-stage3-p4-hardening`).

## What happened

- The first post-enforce backup window is 2026-10-30, 03:14–04:50Z. It covers the three CronJob runs at 03:17, 03:47 and 04:17 UTC.
- A launchd job was to take the Hubble `POLICY_DENIED` read for that window. Its `cd` into the live worktree `multi-source-s3d-live` failed on every run, so the read was never taken.
- The job wrote its file late, at 2026-11-01T06:35:11Z. By then the Hubble flows of the window had passed retention, so the read cannot be recovered.
- Condition 1 holds. Condition 2 was never observed.
- Plan S3-D Task 56's ledger record (housegate#295) was blocked in review on this gap.

On 2026-11-01 the user decided in chat: 「写 D44 例外再合并」.

## Decision D44

**D44: D42's post-enforce backup check is accepted on devnet2 on functional evidence.** Its no-drop condition (2) is not counted as observed for the first post-enforce window. The functional evidence is:

- **Daily backups.** Every daily `si-backup` Job of indexer-a, indexer-b and indexer-c on 2026-10-30, 10-31 and 11-01 is `Complete` with `succeeded=1`. Each ran under enforce of its installation's SI policy and of the Keeper allowlist. The 10-30 Jobs are:
  - `sentio-node-devnet2-indexer-a-si-backup-29888837`, 03:17:00 to 03:39:42Z;
  - `…-indexer-b-si-backup-29888867`, 03:47:00 to 03:47:05Z;
  - `…-indexer-c-si-backup-29888897`, 04:17:00 to 04:17:05Z.
- **Backup logs.** The 11-01 logs show `backed up hg_safe`.
- **The drill's own backup.** Task 56's backup `20261101T084647Z-incr` ran under enforce: Job `sentio-node-devnet2-indexer-c-si-backup-p4-rebuild`, incremental, chain 2, `safe_block_seq` 156, 1 table, 11 parts. It was listed in the bucket's `owner-2/` before the loss, and its dry run covered every part. `rebuild-safe` then restored all 11 parts of indexer-c's `hg_safe` from it through the gate. The owner audit was clean, and `hg_safe` equalled the arbiter's `SafeParts` by hash.
- **Drop reads taken during Task 56** (`si_cnp_drops`, through the Hubble relay):
  - 2026-11-01 at 08:26Z, a 30-minute window. The only `POLICY_DENIED` flows were from `sentio-network-devnet2/devnet2-operator-altinity-clickhouse-operator` to `clickhouse/clickhouse-keeper-extra-1-0` on 2181. This is finding F1 below; production#807 admitted that flow.
  - 2026-11-01 at 09:28Z, a 3-hour window, after #807. No `POLICY_DENIED` flow. The node rings had wrapped, so this read covers only the newest flows.
  - Neither read covers the 10-30 window, and neither is offered as if it did.

**The lesson of F1 binds later plans.** The one-hour census of D42 missed a real client: devnet2's clickhouse-operator checks each SI CHI's Keeper root path on every reconcile, and no reconcile happened inside the census. Under the enforced Keeper allowlist the operator's reconcile stalled (Task 56's restore-grant apply), and production#807 fixed it. So a one-hour census is not proof that a flow is absent.

Plans S3-E1–S3-E4 and S3-F must not reuse D42's one-hour window, or this D44 acceptance, for any new enforce. A policy they introduce or tighten needs either a census that covers every client's period, including daily and event-driven clients such as the CronJobs and the operator's reconciles, or a fresh user decision. Any functional check of that kind they adopt must take its drop read as part of the same run, not from a separate scheduled job.

D44 is limited to the 2026-10-30 window of Plan S3-D Task 55 on devnet2. It is not widened by an edit.

## The text this amends

- **Plan S3-D.** The header gains a pointer to this document. Wherever D42's added check is said to be met before Task 56, it now reads "met as decision D44 records: condition 1 observed, condition 2 accepted on functional evidence".
- **Task 56's run record** (`settle rollout and owner rebuild run`, Appendix C) cites D44 for this gap instead of "the main session accepted the gap".
- **No runbook text changes.** The hardening runbook's §2 and D42's check stay as written for any future run. The owner-rebuild runbook is unaffected, and its §5.3 bucket-listing check (D43) is unchanged.
- **No Appendix C row changes.** `s3-ledger.py` is unaffected.
