# Plan S3-D decision D43: devnet2's rebuild drill without a PersistentVolume patch (user decision, 2026-11-01)

Plan S3-D (`2026-10-15-multi-source-si-s3d-p4-settle-rebuild-hardening.md`) Task 56 drills the same-identity rebuild of indexer-c's source ClickHouse with the owner-rebuild runbook `docs/storage-integrity-devnet2-stage3-p4-owner-rebuild.md` (production, merged in `P4_PR`). Its §5.4 patches the three PersistentVolumes of the old ClickHouse to reclaim policy `Retain`, and its §5.5 labels them `storage-integrity.sentio.xyz/fenced=true` before it deletes their claims, so the volumes stay `Released` as evidence until the user deletes their directories after `P4_END`. Both use `si_kadmin`, the admin context `sentio-sea-admin`.

The runbook's §1.2 precondition read on 2026-11-01 found `patch persistentvolumes: no`. The main session then checked both contexts:

- `sentio-sea` and `sentio-sea-admin` both answer `no` for patch and for delete of PersistentVolumes.
- Both answer `yes` for delete of PersistentVolumeClaims and for exec in `sentio-network-devnet2`.
- indexer-c's three volumes (the data, log and member claims `*-storage-integrity-si-v2-indexer-c-source-0-0-0`) are `local-path` volumes of the provisioner `rancher.io/local-path`, reclaim policy `Delete`, under `/mnt/ssd/pvc/` on `sea-dg-3`. When their claims are deleted, the provisioner deletes the volumes and their directories.

`P4_REBUILD_RTO` is consumed by no later plan, and the old volume is no rollback path: the fence is one-way (contract §37.4; the runbook's §7 and the rollback table's row 7 name only fix-forward from §5.4 on). On 2026-11-01 the user chose, in chat, to drop the PersistentVolume patch and label and let the old volumes be deleted with their claims: 「改走 D43」.

## Decision D43

**D43: on devnet2, no PersistentVolume is patched or labelled.** No admin context on devnet2 may patch PersistentVolumes, and no other way of obtaining that right is attempted.

- **§5.4 (loss)** keeps every check and step except the `Retain` patch and its record `pv-retain.txt`, which are dropped. `pv-before.txt` is still recorded.
- **§5.5 (isolate)** drops the fenced label. It deletes the claims as before. The `local-path` provisioner then deletes the three volumes and their directories, which is a true loss of the old server's data. The block waits up to 5 minutes for each volume of `pv-before.txt` to be gone (`kubectl get pv <name>` answers NotFound) and records `pv-deleted.txt` (claim, volume, `deleted`), which replaces `pv-fenced.txt`.
- **The Keeper fence of §5.5 is unchanged.** `snode-3` must be inactive on every table of `snode-3-tables.txt`, with `verifier-1..3` active, and the scoped root `unsafe_inc` must be empty before `rebuild-safe` runs. `rebuild-safe`'s own fence (K54) is unchanged.
- **Before the loss (§5.3)** the backup taken after the quarantine and its passing `rebuild-safe --dry-run` stay mandatory, as the plan already requires. One read-only check is added: the backup name in `backup-name.txt` is present in the bucket listing of `owner-<INDEXER_C_ID>/` (`si_backup_listed`, run as `si_snode` with the read-only restore key `storage-integrity-si-v2-restore-c`; key and password on stdin only). It is recorded as `backup-listed.txt`, and §5.4 refuses without it.
- **§5.6 (re-create)** gates on `pv-deleted.txt` instead of `pv-fenced.txt`. Its read-only check confirms that the new claims bind fresh `local-path` volumes on `INDEXER_C_CHI_NODE` (`sea-dg-3`), as the node pin requires, and that none of them is a volume of `pv-deleted.txt`.
- **§6 (end)** records `pv-deleted.txt` in `p4-end.txt` instead of the fenced volumes. The step "the user deletes the fenced volumes' directories after `P4_END`" goes away: nothing is left on `sea-dg-3`.
- **Confirmation.** The user's choice covers the changed content of §5.4 and §5.5; each block is still run only after its gate, as Task 56 says.

D43 applies to devnet2 only. Where a PersistentVolume patch right exists, the runbook's original isolation (`Retain`, `Released`, fenced, deleted only by the user after the record) remains the rule; the runbook's §8 says so.

## The Plan S3-D text this amends

Wherever Plan S3-D says the old indexer-c volume is kept (`Retain`, `Released`, labelled fenced, never rebound or deleted by an agent, deleted only by the user after the record), it now reads "except as decision D43 records for devnet2, where the volumes are deleted with their claims in §5.5". This applies to the Goal, the irreversible-steps constraint, Review Focus 3, Task 56's Files, Interfaces ("Produces (devnet2)" and the evidence names), its stop rules and its Steps 16, 17, 18, 24 and 25. In particular:

**Production runbook (Task 56's single production PR, worktree `multi-source-s3d-t56`).** `docs/storage-integrity-devnet2-stage3-p4-owner-rebuild.md` gets the changes above: the §0 helper `si_backup_listed`, §5.3's listing block (the last block of §5.3), §5.4 without the patch and with the listing gate, §5.5 with `pv-deleted.txt`, §5.6's gate and checks, §6's record and the matching sentences of the introduction, §7 and §8. The same PR carries one further runbook fix found in the same run: the gateway's DDL keeps an empty physical companion table in `devnet2` for every user table (on indexer-c, ``devnet2.`s3eaccept2.s3e_e2e` ``, 0 rows). §1.2 accepts empty ones, §5.3 records them in `tables-c.tsv` and stops on any row, and §5.6 creates them again, so the rebuilt ClickHouse matches the hosts that were never rebuilt.

**Task 56 (devnet2, live).**
- Step 16's gate text drops "patch the three PersistentVolumes … to reclaim policy Retain"; its expected output is three volumes `Bound` on `INDEXER_C_CHI_NODE`, without `Retain`.
- Step 17's gate text is "delete the PVCs of storage-integrity-si-v2-indexer-c: the local-path provisioner deletes the three old volumes and their directories (D43)"; its expected output is `pv-deleted.txt` with the three volumes `deleted`. The Keeper part is unchanged.
- Step 18 expects new volumes on `INDEXER_C_CHI_NODE` and `new volumes only`.
- Step 24 expects the deleted volumes in `p4-end.txt`, and drops "The fenced volumes' directories … stay until the user deletes them".
- Step 25's run record names the deleted volumes (`- deleted <claim> <volume>` sub-items instead of `- fenced …`) and states D43: "D43: devnet2 had no PersistentVolume patch right; the old volumes of indexer-c's ClickHouse were deleted with their claims after the backup was proven in the bucket listing and the dry run passed, by the user's decision of 2026-11-01". The closing instruction to tell the user to delete the fenced volumes goes away. Task 56's local `p4r-check.py record` reads `pv-deleted.txt` accordingly.
- The runbook blocks of Task 56 are read from the `multi-source-s3d-t56` worktree. Its single-service commits (V4, H-b, H-c, H-a, G-*, R-on, R-off) are still made in the detached `origin/main` live checkout and pushed as `HEAD:main`, as the plan says.

No Appendix C row changes. `s3-ledger.py` is unaffected.
