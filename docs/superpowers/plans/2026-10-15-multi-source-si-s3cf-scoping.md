# Stage 3 beyond P0, L and E: scoping proposal (context for Plans S3-C1 … S3-F)

The read-only scoping proposal the ten plans were drafted from (2026-10-15). The plans and their contracts are binding where they differ from it; decisions K38, K51 and D25–D40 (in `2026-10-15-multi-source-si-s3cf-status.md`) override it.


## 0. Bases and size calibration

**Revisions read.** I read every revision at origin/main, which matches the spec and S3-A bases:
- arbiter fb42260 (v0.12.0)
- arbiter-core 8f0bc43 (v0.14.1)
- arbiter-proto 1c6ae67 (v0.11.0)
- housegate 66d4df7 (v0.19.0)
- sentio-node 5d2f169
- production add848c3f. Its SI paths have not changed since S3-B's base 9d3d0326b: `git diff --stat 9d3d0326b origin/main -- charts/storage-integrity charts/sentio-node k8s-sea/sentio-network-devnet2 k8s-sea/clickhouse terraform docs scripts/storage-integrity` prints nothing.

S3-A and S3-B have not been executed yet. Every new plan therefore anchors at these revisions and uses the existing "later intent on top" hunk rule, both over S3-A/S3-B and over any earlier new plan.

**Size calibration.**
- S3-A is 20,953 lines for 11 tasks; S3-B is 19,230 lines for 10 tasks.
- Header plus appendices take 0.5–0.8k lines (S3-A Appendix A–C is lines 20583–20953; S3-B Appendix A is 18558–19230 and includes the ledger tool).
- Per task: S ≈ 1.0k lines, M ≈ 1.5k, L ≈ 2.2–2.6k, XL ≈ 3.2–3.5k. S3-A Tasks 5 and 8 are about 3.3k each; live tasks run 1.0–1.7k.

**Why S3-C and S3-E are split.**
- **S3-C as one plan:** 27 tasks, about 55k lines. P2 alone is about 28k. I split it four ways:
  - S3-C1 = P1
  - S3-C2 = P2 code
  - S3-C3 = P2 hosts and rollout
  - S3-C4 = P3
- **S3-E as one plan:** 26 tasks, about 62k lines. I split it four ways:
  - S3-E1 = wire and data plane, no arbiter code
  - S3-E2 = arbiter activation, v21 encoding and switches 1–3
  - S3-E3 = groups, seal key, readiness, re-selection, checkpoints
  - S3-E4 = floor, narrowed drain, release and the live activation
  - E2–E4 share one arbiter branch and one release (the stage3_v1 rule, K65).
- **S3-D and S3-F** stay single plans.

**Overview: 10 plans, Tasks 22–89.**

| Plan | File (docs/superpowers/plans/) | Phases | Tasks | Est. lines |
|---|---|---|---|---|
| S3-C1 | 2026-10-15-multi-source-si-s3c1-p1-backups.md | P1 | 22–28 | ≈12.7k |
| S3-C2 | 2026-10-15-multi-source-si-s3c2-p2-members-code.md | P2 (code) | 29–36 | ≈18k |
| S3-C3 | 2026-10-15-multi-source-si-s3c3-p2-hosts-rollout.md | P2 (hosts, live) | 37–41 | ≈11k |
| S3-C4 | 2026-10-15-multi-source-si-s3c4-p3-member-reads.md | P3 | 42–48 | ≈13.7k |
| S3-D | 2026-10-15-multi-source-si-s3d-p4-settle-rebuild-hardening.md | P4 | 49–56 | ≈16.5k |
| S3-E1 | 2026-10-15-multi-source-si-s3e1-p5-wire-dataplane.md | P5 (proto, housegate, core) | 57–63 | ≈15.6k |
| S3-E2 | 2026-10-15-multi-source-si-s3e2-p5-activation-encoding.md | P5 (arbiter: activation, v21, switches 1–3) | 64–70 | ≈16k |
| S3-E3 | 2026-10-15-multi-source-si-s3e3-p5-groups-checkpoints.md | P5 (arbiter: switches 5–8a) | 71–75 | ≈15.2k |
| S3-E4 | 2026-10-15-multi-source-si-s3e4-p5-floor-release-activation.md | P5 (arbiter: 8b–9; release; live) | 76–82 | ≈15k |
| S3-F | 2026-10-15-multi-source-si-s3f-p6-switch-on.md | P6 | 83–89 | ≈13.5k |

## 1. Per-plan proposals

### S3-C1: P1, verified object-store backups and the restore-and-gate library

**Goal.** Each owner gets incremental backups to a versioned GCS bucket, with keys limited to that owner's prefix and no delete right, plus read-only restore keys. The restore-and-gate library goes in now, including the pure §8.5 gate that P2 reuses unchanged. Then one confirmed restore drill per owner, verified against a manifest. No track pauses (spec §16 P1, line 345).

**Contract and clarifications.** Contract §16–§19; clarifications K23–K28.

| Task | Repo | Deliverable | Size |
|---|---|---|---|
| 22 | housegate | `replay.OwnerEntry(m, indexerID)`; export `payloadexec.VerifyLedger` (verifyLedger, pkg/replay/payloadexec/executor.go:565-600) and `SumPartRowLtHashes` (wraps :680-691); `chexec.ScanAllParts` (scan.go:26-65 needs part names); `chexec.NewMaterializerIn(networkID, conn, database, prefix)` (materializer.go:46-48,105-124; NewMaterializer stays byte-identical); `sitable.CopyTable(id, seq)` = `PhysicalTable(id)+"__i"+seq` with an injectivity test; release `HG_TAG_P1`; writes the generalized ledger tool (see §2) | S |
| 23 | arbiter-core | pin `HG_TAG_P1`; extract `dataplane/chparts` from snode (parts.go:11-43, intake.go:136-141, promote_ch.go:21-46, promote_replace.go:189-259), with snode delegating and applied on top of S3-A Task 5; new pure `member/gate` covering Plan(M,R,L) → Diverged or Need, the scratch classifier (kept, rename race, split, doubled, Foreign) and arithmetic closure | M |
| 24 | arbiter-core | `dataplane/objstore`: BACKUP/RESTORE builders (`S3(...)` destination, `base_backup` chain capped at N links), backup index object (snapshot_id, track_frontiers[O], parts by name/hash_of_all_files/rows), `RestoreAndGate`, drill verdict. Mechanics M12 (incremental BACKUP/RESTORE to an S3 endpoint keeps hash_of_all_files), M13 (BACKUP with create/get/list-only credentials), M14 (secret masking in query_log, system.backups, system.processes and the server log); MinIO in CI, GCS run by hand on the deployed image (K9 pattern); release `CORE_TAG_P1` | L |
| 25 | sentio-node | pins; `sentio-node storage-integrity backup` and `restore-drill --owner N` (commands/storage_integrity.go:13-28 pattern); config `storage_integrity.backup` (destination only, keys from env, S3-B K16); `si_backup` login; release `SN_COMMIT_P1`/`SN_DIGEST_P1` | M |
| 26 | production | `terraform/storage_integrity_devnet2_si_v2_replica.tf`: bucket with the storage_integrity_devnet2_si_v2.tf:5-58 flags; per-owner backup SAs on a no-delete custom role (storage_integrity_devnet2.tf:38-48 pattern) with the writer role granted on one managed folder per prefix; per-host restore SAs (objectViewer). `si_backup` user in every SI installation as a users.d reload (literal hash, grant-probe rows, logins script). `charts/sentio-node` `siBackup` CronJob outside the host checksum (node.yaml:486). Alerts `StorageIntegrityBackupStale`/`StorageIntegrityBackupFailed`, added to the reviewed MONITORING_ALERTS set. Render tests | L |
| 27 | production | runbook `docs/storage-integrity-devnet2-stage3-p1-backups.md` (terraform `-target`, HMAC keys as a user action, mechanics against a throwaway prefix, users, CronJobs one commit per owner, a drill per owner on a re-created staging installation); P1 PR | L |
| 28 | devnet2 (live) | bucket, keys, users, jobs, first full backups, one drill per owner | M |

**Ledger.**
- Produces: `HG_TAG_P1`, `CORE_TAG_P1`, `SN_COMMIT_P1`/`SN_DIGEST_P1`, `P1_MECHANICS` (the GCS/deployed-image M12–M14 record), `P1_PR`, `P1_END`.
- Consumes:
  - from S3-A: `HG_TAG`, `PROTO_TAG`, `CORE_TAG`, `ARBITER_TAG`/`ARBITER_DIGEST`
  - from S3-B: `SN_COMMIT`/`SN_DIGEST`, `L_END`, `E_END`, `INDEXER_C_ID`

**Clarifications.**
- **K23, backup destination.** Use `S3()` with HMAC keys from the job's Secret env on every statement. Never use a config.d backup disk, because it needs a pod restart and the P1 row says no track pauses. Masking is measured by M14. The `si_backup` grants are measured on 26.3: BACKUP ON hg_safe.\*, the S3 source right, SELECT on system.parts/system.backups.
- **K24, "not delete".** M13 tests whether BACKUP completes without delete (lock file, failed-upload cleanup, multipart abort). If it does not, stop and amend spec §16 P1 to "delete on own prefix only, versioned bucket with noncurrent retention". Backup names are never reused, because an overwrite needs delete.
- **K25, list confinement.** IAM Conditions of bucket bindings cannot test a list's prefix (Cloud Storage API attributes are Credential Access Boundary only); use one managed folder per writer prefix holding the writer's only binding, prove it in Task 28 §5, and stop (no residual) if a row differs.
- **K26, granularity.** Prefix `owner-<indexer_id>/`, which survives a signer rotation. One backup SA per owner, one restore SA per member host.
- **K27, drill reference.** A drill gates against the manifest named in the backup's index object. A backup ahead of publication keeps only the needed parts. `member/gate` is the gate P2 reuses unchanged.
- **K28, promotion budget.** Use a quadratic fit (§4, defect D1) in every stage-3 runbook from P1 on.

**Depends on.**
- S3-A: `CORE_TAG`, because chmechanics is the base for M12–M14.
- S3-B: `L_END` and `E_END`. Users are locked and the third owner prefix exists.

### S3-C2: P2 code layer, the si-member off consensus

**Goal.** The arbiter-core member role (manifest feed, copies, byte sources, install gate, startup audit, shadow verification) as a separate sentio-node process. Plus the integration proofs. No Raft-reachable call.

**Contract and clarifications.** Contract §20–§24; clarifications K29–K37.

| Task | Repo | Deliverable | Size |
|---|---|---|---|
| 29 | arbiter-core | `Client.GetSafeWatermark`/`GetManifest` (today only a probe, dataplane/client.go:216-228); `manifestfeed.Follower`: ParentSnapshotID walk, never GetManifestByBlock; SnapshotID must equal the requested id and the recomputed root; parent link; VerifyLedger; bounded LRU; bounded walk then full fetch. `ManifestStore` (dataplane/manifests.go:15-57) is not reused. `wire.TableRegistrySnapshot.ManifestOwner`, a port of arbiter fsm/reads_manifest.go:448-463 with a conformance test. `wire.BlockVerification` + `Client.GetBlockVerification` (S3-A noted it "has no canonical type yet") | M |
| 30 | arbiter-core | `ddl.MemberPinned`; `CopyIntent` (hg_replica.<db__table>__i<seq>, explicit ENGINE = MergeTree, never AS); `MemberUnsafeIntent` (defined, used from S3-E1); `ListMemberTables`. `member/copyset.Reconciler`: static group scope, Retiring drops the copy, Purged/Refused/unknown drop it. Startup refusals: databases that intersect the SNode's, or a node id that collides | M |
| 31 | arbiter-core | `member/sources`: OwnerPull (resolve by `hash_of_all_files`, fall back to row hash; no-split settings; `_part` filter), PeerUnsafePull, Restore (objstore), "missed" as an outcome; startup self-check of grants through system.grants (never SHOW GRANTS) plus a probe that INSERT INTO FUNCTION remote is refused | L |
| 32 | arbiter-core | `member/install`: scratch `<node_id>_<boot_nonce>_<n>` in hg_rstage, ATTACH PARTITION FROM, Diverged = DROP PARTITION + ATTACH (never REPLACE), closure, journal per owner, startup audit and self-audit (twins → local DROP PART), rerun on `dataplane/chrestart`, drop of prefixed rstage tables at startup, stop on free-space threshold, local quarantine of Foreign tables | XL |
| 33 | arbiter-core | `member/shadow`: poll GetBlockVerification with backoff, replay through `NewMaterializerIn(hg_rstage, prefix)` (verifier/backends.go:21-41 template), compare receipts with evaluated evidence and the verdict, pull candidates from the owner's hg_unsafe, count missed scans apart, durable cursor; counters `sentio_node_si_member_shadow_{blocks,mismatches,missed_scans}_total` | L |
| 34 | arbiter-core | `member.Role` composition and metrics. ClickHouse-gated confinement suite with `table_engines_require_grant`: as si_member, the eight Replicated\*MergeTree engines, Buffer/Distributed/Merge/Alias, CREATE DATABASE ENGINE=Replicated, ATTACH/MOVE/MV into hg_member_unsafe and INSERT INTO FUNCTION remote are all refused; si_member_ddl's const merge constraint refuses a merges-allowed replica. Release `CORE_TAG_P2` | L |
| 35 | sentio-node | pins; `sentio-node si-member --config-path` (separate process); `storage_integrity.member` config (node id, state dir, DSN, `clickhouse_username`, env-only password, static peers, thresholds) with the S3-B §8 refusals; `TestSIMemberInstallsACopyFromTheOwnerPull`; release `SN_COMMIT_P2`/`SN_DIGEST_P2` | L |
| 36 | arbiter | pin `CORE_TAG_P2` on main with no release (it adds no consensus change). chpipeline `startMember` against a second ClickHouse (harness_roles_test.go:18-104). Tests: `TestPipeline_MemberCopiesNeverHoldRowsOfARejectedStatement` (fraud_test.go:57-95), `...FollowsTheOwnerAcrossPromotions`, kill-mid-stream, and a ≥500-blocks-per-owner shadow soak (K38) | M |

**Ledger.**
- Produces: `CORE_TAG_P2`, `SN_COMMIT_P2`/`SN_DIGEST_P2`, `ARBITER_PIN_P2` (a commit, not a release).
- Consumes: `HG_TAG_P1`, `CORE_TAG_P1`, `PROTO_TAG`, `ARBITER_TAG`.

**Clarifications.**
- **K29, member ids.** Pin `member-a`, `member-b`, `member-c` now: never reused, and unused by every SNode, verifier and voter. The P2 id becomes the P6 VERIFIER id and Keeper replica name, so nothing is re-keyed. Spec §8.5 (line 192) already names scratch by node id.
- **K30, process layout.** The si-member gets its own StatefulSet. A container in the host pod would restart the SNode on every member change, through the checksum at production charts/sentio-node/templates/node.yaml:486. The pod has a `si-member` container (si_member) and a `si-member-reconciler` slot (si_member_ddl) that stays empty until §10.4 step 6. Its status endpoint is a Service with a bearer-token Secret. This settles §8.2's "password mounted only into the reconciler" now, so S3-F does not re-architect it.
- **K31, copy reconciler.** `member/copyset` is a new package, not a mode of `dataplane/tableset`. Tableset's sweep and decommission logic (reconciler.go:704-787) is SNode-specific.
- **K32, spec gap: database creation.** Spec §8.2 gives si_member no `CREATE DATABASE ON hg_replica.*`, and si_member_ddl has none on hg_member_unsafe. Amend the spec grant lists. The alternative is an admin runbook step.
- **K33, spec gap: restore right.** The object-store restore source needs the S3 source READ right for si_member, which §8.2 omits. Amend the spec; measure it in Task 34.
- **K34, shadow semantics.** Compare against evaluated evidence only; evidence recorded after the latch is compared as recorded. A QuorumFailed or Rejected block must reproduce the honest verifiers' receipts. A candidate part that was already cleaned counts as a missed scan.
- **K35, pull bounds.** The si_member profile's `max_memory_usage` bounds the largest pullable part. A refused pull falls back to restore and never loops.
- **K36, journal recovery.** A crash between ATTACH and the journal write is reconciled from system.parts plus scans of unjournaled parts only. Foreign quarantines the whole table locally.
- **K37, feed shape.** One follower per watermark, demultiplexed per owner.

**Depends on.** S3-C1 code (`HG_TAG_P1`, `CORE_TAG_P1`). Drafting needs only S3-C1's §16–§18 names.

### S3-C3: P2 hosts and devnet2 rollout

**Goal.**
- One restart per SI ClickHouse, one at a time, carrying every server-level change stage 3 needs.
- Engine grants by name, from a census.
- `/sentio/<shard>/unsafe_inc` created, then the si_member users.
- si-members on every host.
- A shadow soak and the P2 exit.

**Contract and clarifications.** Contract §25–§28; clarifications K38–K43.

| Task | Repo | Deliverable | Size |
|---|---|---|---|
| 37 | production | Restart-bundle files: `table_engines_require_grant`, `keep_free_space_bytes` on every disk (closes S3-B K21), member disk and policy, `<replicated_merge_tree>` merge default 0, `background_fetches_pool_size`, config-defined NOT OVERRIDABLE named collections with from_env si_reader passwords. By-name `TABLE ENGINE` grants for si_snode, si_verifier and si_gateway (census constants). si_member user, profile and collection grants. Grant-probe rows; render tests; everything off by default (render-inertness.sh) | L |
| 38 | production | `charts/sentio-node` `siMember` StatefulSet (own ConfigMap/PVC/checksum/image pin/metrics/status Service and token, reconciler slot off); `MEMBER_IDS` row in the multi-source runbook §1.2 ledger; VMServiceScrape `si_component=member` and alerts (unscraped, ShadowMismatch, ForeignPart, Diverged, CopyBehind, MemberDiskHigh) with per-group runbook URLs (S3-B has a single `monitoring.runbookURL`); render tests | L |
| 39 | production | runbook `docs/storage-integrity-devnet2-stage3-p2-members.md` (census → staging proof → engine grants → restarts b → c → network → unsafe_inc → si_member → members one host at a time → bootstrap → soak → measurements → exit); P2 PR | L |
| 40 | devnet2 (live) | census; engine grants (reload); the three restart windows; unsafe_inc; si_member users | M |
| 41 | devnet2 (live) | si-members per host; bootstrap by pull and restore; soak within budget; exit record | M |

**Ledger.**
- Produces: `P2_PR`, `ENGINE_CENSUS` (record), `P2_RESTART_END`, `UNSAFE_INC_ROOT` (record), `MEMBER_IDS`, `P2_END`.
- Consumes: `SN_COMMIT_P2`/`SN_DIGEST_P2`, `P1_END`, `L_END`, `E_END`, `INDEXER_C_ID`.

**Clarifications.**
- **K38 (blocking; needs a spec amendment): the P2 exit volume (spec line 346) is infeasible on devnet2.**
  - The exit asks for ≥500 blocks per owner, about 1,500 promotions network-wide.
  - Spec §2 (line 41) measures 11.4 MB at promotion 53, so S(n) ≈ 4.06 KB·n².
  - That crosses L/4 = 128 MiB near n ≈ 182 and the 1 GiB read cap near n ≈ 510.
  - v21 does not help: it shrinks the snapshot document, not `arbiter_state_bytes` (S3-A defines it as the plain encoding length) or the heap.
  - **Proposal:** the 500-block criterion runs in arbiter chpipeline (Task 36). The devnet2 soak shadow-verifies every block it sees, within half the promotion budget, with zero mismatches.
- **K39, one restart per SI ClickHouse carries all of Task 37's server changes.** This includes the merge default that the spec attaches to step 6; folding it in now saves a second restart. The census must show that no tenant ReplicatedMergeTree table relies on merges. P4's interserver step 1 and node pins do not ride along.
- **K40, member disk shape.** On the network CHI and indexer-b, use a directory on the existing data PV, recorded as a deviation. A new VCT is used only if staging proves that operator 0.27.2 adds it without recreating the StatefulSet.
- **K41, engine census.** Seven days of `query_log` CREATEs plus `system.tables` engines, materialized-view inner engines included, and `Memory` where temporary tables appear. `default` and `clickhouse_operator` are users.xml users without `<grants>`, so they keep every engine.
- **K42, unsafe_inc.** §8.2 says the root exists before *any* member user, and si_member exists from P2. So the P2 runbook creates `/sentio/<shard>/unsafe_inc` (refusing a non-empty one), not step 6. Reconcilers never create it.
- **K43, installation-wide users.** si_member and the collections also exist on the network CHI's verifier hosts. Record this residual in the K18 style.

**Depends on.** S3-C2 (`SN_COMMIT_P2`) and S3-C1 live (`P1_END`).

### S3-C4: P3, reads from members

**Goal.** A member host serves a quarantined or unreachable owner's databases in safe mode from its copy, through:
- a Replica status;
- forward routing that keeps replica-served sessions local;
- serving leases;
- the agent's read failover, rolled to every host in one window.

**Contract and clarifications.** Contract §29–§32; clarifications K44–K50.

| Task | Repo | Deliverable | Size |
|---|---|---|---|
| 42 | housegate | `sitable.Status` Replica (sitable.go:17-78; Table gains CopyTable and ReplicaOf, :81-88; Snapshot gains `Served()`, :117-122; Static/Fake); explicit `sitablestate` rows (decide falls through to allow at plugin.go:137); registry/network types; RpcNetworkState accepts `replica` (pkg/network/rpc.go:371-377) | M |
| 43 | housegate | `forward.Plugin` table-state port (forward.go:44-80, :91-115, :204-246; build.go:838-847); replica-served hello/USE stays local; retryable reconnect refusal; `buildStorageIntegrityArgs` Replica tables with SafeTable = UnsafeTable = copy (pkg/rewriter/storage_integrity.go:160-196); `unsafe_latest` refused (:221-233); plugin order with tablerefguard/sireserved; revisit S3-A's carrier-computed reserved-name follow-up | L |
| 44 | housegate | agent replica tier (build.go:1797-1835, upstream_select.go:29-109) and a matcher for the read-side `indexer_quarantined` 392 text (client_error.go:96-107 covers only writes); `agent_replica_failovers_total{reason}`; release `HG_TAG_P3` | L |
| 45 | arbiter-core | `member/serve`: Serving view, owner probe of O's HouseGate (`t_unreach`), entry stable across two polls, lease (revoke and wait before any install), authenticated endpoint (K30); release `CORE_TAG_P3` | M |
| 46 | sentio-node | lockstep pins `HG_TAG_P3` and `CORE_TAG_P3` (Served()); member status client; Replica entries in tablestate build() (snapshot.go:53-147, :241-267, :276), failing closed; status and info RPC fields (rpc/storage_integrity_info.go:14-38); release `SN_COMMIT_P3`/`SN_DIGEST_P3` | M |
| 47 | production | `GRANT SELECT ON hg_replica.*` for si_gateway on member-hosting installations; host flag (off); agent sidecar pin constant; monitoring; render tests; runbook `...-stage3-p3-replica-reads.md`; P3 PR | L |
| 48 | devnet2 (live) | one agent window across all hosts (indexer-b first), flags, drill, exit | M |

**Ledger.**
- Produces: `HG_TAG_P3`, `CORE_TAG_P3`, `SN_COMMIT_P3`/`SN_DIGEST_P3`, `P3_PR`, `P3_END`.
- Consumes: `P2_END`, `MEMBER_IDS`, `CORE_TAG_P2`, `HG_TAG_P1`.

**Clarifications.**
- **K44, lease versus in-flight queries.** Lease TTL ≥ si_gateway `max_execution_time` plus a margin. Installs wait for release or expiry.
- **K45, Replica entries carry Schema and SchemaHash** from the registry incarnation.
- **K46, agent failover scope.** Failover happens only at hello or at a read's first error before any data, and never for writes or INSERT … SELECT. It does not silently switch read modes: `unsafe_latest` is refused with the safe-mode message, and the drill sets `SQL_x_read_mode='safe'` explicitly.
- **K47, rollout order.** `HG_TAG_P3` and `SN_COMMIT_P3` land together. Every agent is upgraded before any host flag is on, because older clients refuse status `replica`.
- **K48, spec gap.** The si_gateway hg_replica grant is not in S3-B contract §11. Record that it reopens the carrier-name follow-up and test it.
- **K49, drill trigger.** Quarantine indexer-c through tracks runbook §8 on devnet2. Exercise the `t_unreach` path in integration.
- **K50, replica_hosts.** Until placement, the static configuration (every other SI indexer), advertised per host.

**Depends on.** S3-C3 (`P2_END`).

### S3-D: P4, settle, same-identity owner rebuild, network hardening

**Goal.**
- An operator-triggered settle for lost acknowledgements on a quarantined track.
- A same-identity owner rebuild of indexer-c: Keeper fence, hg_safe restored through the gate, RTO recorded.
- Network hardening: interserver credentials in two rolling steps, ingress-only Cilium policies probed from every pod, a Keeper ingress allowlist, node pins.
- Recording the failure-domain values that S3-F lists.

**Contract and clarifications.** Contract §33–§37; clarifications K51–K57.

| Task | Repo | Deliverable | Size |
|---|---|---|---|
| 49 | arbiter-proto | (if K53 chooses an RPC) `ConsensusAdmin.SettleTrack`, not Raft-reachable, with a conformance ledger; release `PROTO_TAG_P4` | S |
| 50 | arbiter-core | SNode settle answer: handlePromote (snode/promote.go:14) re-acknowledges from the journal (RecordAck, snode/state.go:201), idempotent, never applies new state. Rebuild library: restore hg_safe to the FSM's SafeParts through the gate; Keeper fence checks (is_active absent, SYSTEM DROP REPLICA … FROM ZKPATH). Mechanic M15 (RESTORE keeps part names). Release `CORE_TAG_P4` | L |
| 51 | arbiter | leader-local settle mode, an exception to C8 (fsm/reads_work.go:300-305; orchestrator/promotion.go:34-38 streams IssuedUnacked); admin RPC and `arbiter-admin track settle`; release `ARBITER_TAG_P4`/`ARBITER_DIGEST_P4` | M |
| 52 | sentio-node | pins; `storage-integrity rebuild-safe`; under K51(b), a dedicated SI signer key; release `SN_COMMIT_P4`/`SN_DIGEST_P4` | M |
| 53 | production | `clickhouse.interserver{enabled, allowEmpty, secret}` on every SI CHI, sharing one Secret; ingress-only CNPs `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2-network-policy.yaml` with probe matrix `si_network_probe.tsv`; keeper-extra ingress CNP (k8s-sea/clickhouse/network-policy.yaml:108-137; keeper listens on 0.0.0.0 per clickhouse-extra-keeper.yaml:21-26); per-cluster node pins (clickhouse.yaml:65-80); render tests | XL |
| 54 | production | hardening runbook plus owner-rebuild/settle runbook; P4 PR. The Keeper CNP is cross-namespace and shared production infrastructure, so it must go through a PR | L |
| 55 | devnet2 (live) | census → interserver step 1 → step 2 (one installation per window) → CNPs observe then enforce → Keeper allowlist observe then enforce → pins → probes | M |
| 56 | devnet2 (live) | settle rollout; indexer-c rebuild drill (quarantine, scale to 0, fence, re-create, restore, audit, rotation per K51, release) | M |

**Ledger.**
- Produces: `PROTO_TAG_P4` (conditional), `CORE_TAG_P4`, `ARBITER_TAG_P4`/`ARBITER_DIGEST_P4`, `SN_COMMIT_P4`/`SN_DIGEST_P4`, `P4_PR`, `INTERSERVER_STEP1_END`, `INTERSERVER_STEP2_END`, `DEVNET2_FAILURE_DOMAINS`, `HARDENING_END`, `P4_REBUILD_RTO`, `P4_END`.
- Consumes: `P1_END` (restore source), `P3_END` (all member flows exist for the policies), `CORE_TAG_P3`, `ARBITER_TAG`.

**Clarifications.**
- **K51 (spec conflict): "signer rotation" (§16 P4, line 348).**
  - The SI claim signer is the on-chain PRIVATE_KEY (sentio-node standalone/storage_integrity_enrollment.go:36-47).
  - A new key makes EnsureRegistered register a *new* indexer id (common/env.go:402-430).
  - `IIndexerRegistry.Update` has no signer argument (bindings/bindings.go:6355).
  - The FSM itself allows a rotation (arbiter fsm/signed_claims.go:132-176), but the host cannot keep its id with a new key.
  - **Options:** (a) the drill keeps the key, which lives in a Secret and survives the lost PV, and rotation is not drilled; (b) sentio-node gets an optional dedicated SI signer key (changes umbrella D6). **Recommendation:** (a). The user decides.
- **K52, rebuilt hg_safe versus the owner audit.** The audit keys twins on acknowledged names (S3-A contract §3). A rebuild must reproduce names as well as hashes (M15). Otherwise the audit needs a rebuilt-reference rule matching on (row hash, hash_of_all_files).
- **K53, settle shape.** Operator-triggered, streams only already-issued promotions, never issues new ones. Either a new RPC (Task 49) or a flag on the release path.
- **K54, Keeper fence.** Unscoped paths only, because the purge switch does not exist before P6.
- **K55, failure domain naming.** The node of the indexer's source ClickHouse, `node:<hostname>`, recorded as `DEVNET2_FAILURE_DOMAINS`. It feeds `stage3.indexer_failure_domains` and makes `safe_copies` "per distinct Kubernetes node".
- **K56, interserver step 1.** Its mixed state is measured on two staging CHIs. If an uncredentialed server refuses a credentialed fetcher, step 1 runs under a drain hold.
- **K57, policy scope.** Egress enumeration is deferred (GCS needs world/FQDN). CNPs are kubectl-applied and never select gateway pods.

**Depends on.** Code: S3-C1/C2 libraries. Live: `P3_END`.

### S3-E1: P5 wire, ingress and data plane (arbiter-proto, housegate, arbiter-core)

**Goal.**
- Every v21 number fixed and released.
- The ingress classification of the new admission codes.
- Canonical types, authority purposes, wire tags 38–42.
- Incarnation-scoped Keeper paths.
- The member consensus mode (verify role, hg_member_unsafe reconciler, readiness and checkpoint signing, hardlink source, purge reports), all inert by configuration.

**Contract and clarifications.** Contract §38–§43, with §39 the verbatim proto; clarifications K58–K67.

| Task | Repo | Deliverable | Size |
|---|---|---|---|
| 57 | arbiter-proto | see the proto proposal below; release `PROTO_TAG_P5` (buf breaking against `PROTO_TAG` or `PROTO_TAG_P4`) | M |
| 58 | housegate | pin `PROTO_TAG_P5` (housegate is still on v0.11.0); add codes 12 and 13 to the terminal-reject list (pkg/storageintegrity/arbiter_proto.go:415-424; an unknown code becomes OutcomeUnknown at :426-427); chproto client errors (client_error.go:85-98); relay mapping (relay.go:2304-2308); release `HG_TAG_P5` | S |
| 59 | arbiter-core | canonical types (consensus.go:79-85, :127-165, :172-211; types.go:128-148); authority purposes, consent, member-ready and checkpoint (authority/quarantine.go:13-125 and snode_messages.go:16-76, :165-205 patterns); normalizer that copies every field (authority/consensus.go:125 rebuilds `{NodeID, Ed25519Pubkey}` today); every-field reflection test; authority goldens | L |
| 60 | arbiter-core | wire: converters (wire/consensus.go:139-151 also rebuild the entry), tags 38–42 in command.go:204-535, registry snapshot fields (wire/table_registry_snapshot.go:236-327), BlockVerification canonical type; assertMirror (conformance/arbiter_wire_test.go:20-75); encoded-tag goldens | L |
| 61 | arbiter-core | `ddl.ZooKeeperPath(inc)` → `/sentio/<shard>/unsafe_inc/<db__table>/<seq>` when scoped, byte-identical otherwise (naming.go:42-46); both roots in KeeperReplicas, KeeperUnsafeTables, sweep, clearDecommissioned, sweepPurgedKeeperPaths (table.go:158-179, reconciler.go:672-842); owner-sweep alerts for unknown replicas and non-incarnation roots; ClickHouse test: a stale unscoped last replica dropped after a scoped re-create leaves the scoped table intact | M |
| 62 | arbiter-core | extract evidence handling from verifier (verifier.go:327-347, :413-439) into a shared component, byte-identical for arbiter-verifier; member verify role scanning hg_member_unsafe (backends.go:94-150); group-scoped hg_member_unsafe reconciler as si_member_ddl (REPLICA_ALREADY_EXISTS stops and alerts, never SYSTEM DROP REPLICA as in table.go:116-128); VERIFIER registration with registration_seq | L |
| 63 | arbiter-core | MemberReady and MemberCheckpoint clients and signers (checkpoint_seq above both the journal and the published value); checkpoint loop with closed-enum excluded reasons; hardlink byte source; purge reports and quiescence hook; readiness in the status endpoint; `stage3_v1` in LocalNodeFeatures (consensus.go:35-38); release `CORE_TAG_P5` | L |

**Proto proposal (Task 57, extending the consensus report).**
- consensus.proto:
  - `Stage3Params stage3` as field 14 on ConsensusParamsUpdate and field 9 on ConsensusMutableParams.
  - `Stage3Params` fields 1–18, the switches included; `IndexerFailureDomain`.
  - VerifierEntry 3–11, with `optional uint64 host_indexer_id = 3`.
  - Commands RebalanceGroup, ReselectVerifiers, MarkMemberLagging, MemberReady, MemberCheckpoint (with CheckpointGroup and ExcludedTable).
  - ConsensusAdmin gains `RebalanceGroup` and `MarkMemberLagging`.
- raftlog.proto: tags 38–42. The highest today is 37 (verified); 29 stays unused.
- arbiter.proto:
  - `ADMISSION_CODE_VERIFIERS_UNAVAILABLE = 12` and `..._COPIES_BACKPRESSURE = 13`. The highest today is 11 (verified).
  - `L3BlockTrack.group_key = 2`.
  - VerifierGateway gains `SubmitMemberReady` and `SubmitMemberCheckpoint`, request equals command, returning Ack.
  - BlockVerification read fields from 14: eligible_ring, ring_position, group_key, group_members, group_fill, group_databases, reselections.
- table_registry.proto: TableIncarnation `scoped_keeper_path = 19` and `purge_members = 20`; TableRegistrySnapshot `groups = 10` and `member_checkpoint_seqs = 11` (next free 19 and 10, verified).
- `conformance/stage3_test.go`:
  - pins every number;
  - checks that 38–42 are the only new oneof members;
  - checks that the read messages are not Raft-reachable (the S3-A TestStage3MessagesAreNotRaftReachable pattern).

**Ledger.**
- Produces: `PROTO_TAG_P5`, `HG_TAG_P5`, `CORE_TAG_P5`.
- Consumes: `PROTO_TAG` (or `PROTO_TAG_P4`), `HG_TAG_P3` (housegate base), `CORE_TAG_P4` (core base).

**Clarifications.**
- **K58, spec ambiguity: "true by default" bools (§6.1, line 104).** Proto3 cannot express absent-means-true. The host-bound fields are allowed only when `host_indexer_id` is set and are always explicit there; arbiter-admin supplies true/1; weight 0 is refused. Pool entries keep their exact v20 bytes and params digest.
- **K59, spec gap.** Strict-mode backpressure (§9.3, line 245) has no admission code; add code 13 now.
- **K60, purge set representation.** Per-incarnation `purge_members`, accumulated at record time and at every member add while the incarnation is physical.
- **K61, spec ambiguity: salt (§6.3, line 117).** "d's first physical incarnation" is ambiguous at backfill and after a re-placement. Use `Created.BlockHash` of the lowest-seq incarnation of (O, d) that is physical when the group is placed; a genesis-origin database uses the genesis snapshot id.
- **K62, spec gap: restore of transition blocks (§7.1, line 135).** BlockVerification stores the (database, group_generation) list of its groups at seal.
- **K63, purposes.** `arbiter-rebalance-group-v1`, `arbiter-reselect-verifiers-v1`, `arbiter-mark-member-lagging-v1` (ES256K authority, `-command-v1` domains), `arbiter-group-member-v1` (consent), `arbiter-member-ready-v1` (ed25519, its own domain), `arbiter-member-checkpoint-v1` (ES256K replica_signer; not named in spec §9.1, line 228).
- **K64, no GetStage3State RPC.** GetConsensusParams, GetTableRegistry and GetBlockVerification carry everything the runbooks need.
- **K65, stage3_v1 release rule.** stage3_v1 reaches GetProtocolInfo when arbiter pins `CORE_TAG_P5`, so the S2-A rule holds: no arbiter release between Task 64's pin and Task 78.
- **K66, member consensus mode ships in `CORE_TAG_P5`.** Task 78's harness then runs real members, and S3-F needs no code release.
- **K67, member RPC gating.** Member gateway RPCs answer FailedPrecondition before their switch, and Apply refuses too.

**Depends on.** Tasks 57–61 need only `PROTO_TAG`. Tasks 62–63 need the names from S3-C2 §22–§23 and S3-C4 §30.

### S3-E2: P5 arbiter, activation, v21 encoding and switches 1–3

**Goal.** The `stage3` parameters and activation, the stage3 gate, both v21 bodies with write-then-verify, eviction voiding, purge sets and scoped marking, and owner-aware selection for pool-only rings. Everything is inert behind switches. One arbiter branch, `urwt/claude/multi-source-s3e`, with no release in this plan.

**Contract and clarifications.** Contract §44–§48; clarifications K68–K72.

| Task | Repo | Deliverable | Size |
|---|---|---|---|
| 64 | arbiter | pins; `Params.Stage3` (fsm/state.go:85-128, params_identity.go:44-66); Params-only rules in verifyConsensusTransition (consensus_updates.go:158-269, after :233-246); VerifierEntry rules (signed_claims.go:186-207); cloneVerifiers fix (:93-102); per-group floor in the Apply-only half (:219-300); `State.Stage3` (state.go:343-401); activation Apply on the tracks.go:105-142 pattern, without the empty-block/global-prefix rule; cutover records | XL |
| 65 | arbiter | `server/stage3_gate.go` (tracks_gate.go:74-108, :140-206 pattern; consensus_admin.go:124-165); post-activation registration refusal (membership.go:53); consensusMutableParams (:107-122); arbiter-admin `--stage3`, switch flags, extended `--verifier`, `--indexer-failure-domain`; activationGate exemption hook for quarantined tracks (cmd/arbiter-admin/main.go:1057-1083) | L |
| 66 | arbiter | `snapshotVersionV21`, body-kind byte, write v20 until stage3 commits (snapshot.go:39-60, :197-219, :262-303); forged-field refusal `stage3FieldsPresent` (:475-489, :673-700 pattern); `fsm/stage3_restore.go` skeleton (genesis_validation.go:54-106, tracks_restore.go:37-224); container goldens | M |
| 67 | arbiter | interned body, write-then-verify in Persist outside the lock (snapshot.go:716-756) on S3-A Task 9's observer; `raft.snapshot_body: auto\|plain` (config/config.go:31-42); `arbiter_snapshot_encoding_fallback_total` and a body-size gauge per kind; forced-mismatch test; offline replay of a devnet2 log copy | L |
| 68 | arbiter | eviction voiding as explicit checks at each call site (signed_claims.go:552-560, :788-800; rejected_cleanup.go:337-345; apply.go:589-645; not inside snodeSignatureLocked, which registration shares, :442-455); reevaluateBlock skip (threeway.go:35-112); applyEvictNode re-evaluation (apply.go:121-149); WorkSet evidencePresence (reads_work.go:20-32) | M |
| 69 | arbiter | ScopedKeeperPath marking (applyAddTable, apply_table_registry.go:220-275); pool versus member purge-required set (:469-494); completePurges reruns; restore rules | M |
| 70 | arbiter | eligible ring for pool-only databases and the owner-hosted exclusion (select.go:55-77; apply.go:425-450); stored EligibleRing, RingPosition and cursor block (state.go:267-279); VERIFIERS_UNAVAILABLE right after SOURCE_UNAVAILABLE (admission.go:133-147); unmarkable blocks leave the WorkSet (reads_work.go:124-137) and the orchestrator stops proposing them (dispatch.go:18-25); byte-identical with the switch off | L |

**Ledger.** Produces nothing. Consumes `PROTO_TAG_P5`, `HG_TAG_P5`, `CORE_TAG_P5`, `ARBITER_TAG_P4` (base).

**Clarifications.**
- **K68, spec ambiguity (§10.4, line 272).** "From the activation, entries with a host_indexer_id enter no eligible ring until step 6" is not enforced by the switches. selectVerifiers takes every Active VERIFIER (select.go:61-66), and purgeRequiredLocked counts every non-evicted VERIFIER (apply_table_registry.go:475-489). **Resolution:** Apply refuses host-bound entries until owner_aware_selection is on. Registration already requires listing (signed_claims.go:461-473), so this machine-enforces step 4's place in the order without changing switch-off behaviour. §12's third-party refusal has no `operator_id` to check, so it stays an operating rule in S3-F.
- **K69, consensus rules.** Params-only rules replay on restore. `min_safe_copies > 1` and `strict_copies` require checkpoints and narrowed_drain on; otherwise the stage-2 gate (quarantine.go:233-242) deadlocks governance.
- **K70, framing.** v21 plus a body-kind byte (0 plain, 1 interned). Rollback stays possible until the activation.
- **K71, spec gap: memory budget (§13.2, line 317).** The interned codec lives in arbiter fsm, and Persist compares SHA-256 of the plain bytes. `arbiter_state_bytes` keeps describing the plain document. v21 lowers the PVC and read-cap terms of L but not the memory term; decide whether node-local in-memory string interning is in scope.
- **K72, parked claims.** `adoptParkedClaimsLocked` (signed_claims.go:657-695) refuses a claim parked by a since-evicted SNode once the switch is on.

**Depends on.** The S3-E1 contract (§39, §41, §42). It can be drafted in parallel with E1.

### S3-E3: P5 arbiter, groups, seal key, readiness, re-selection, checkpoints

**Goal.** Placement and backfill, RebalanceGroup with Leaving records, the seal key and the header `group_key`, MemberReady and group-aware rings, ReselectVerifiers, MemberCheckpoint with classes and MarkMemberLagging. Inert until switches 5, 7 and 8. Same branch as E2.

**Contract and clarifications.** Contract §49–§52; clarifications K73–K77.

| Task | Repo | Deliverable | Size |
|---|---|---|---|
| 71 | arbiter | `Stage3State.Groups`, durable GroupGenerations; an exported pure placement function; candidates, weighted score, fill; switch-on backfill (including genesis-origin incarnations, table_registry.go:255); RebalanceGroup (quarantine.go:29-54, :108-138 command pattern; fsm.go:72-147 dispatch) with the C10 seal hook; Leaving records; purge_members accumulation; registry view Groups (table_registry.go:317-349; server/table_registry.go:100-115); `ConsensusAdmin.RebalanceGroup`, `arbiter-admin group rebalance`, `consensus groups`, placement preview; goldens; restore rules | XL |
| 72 | arbiter | GroupKey; OpenL3Block and L3BlockTrack fields (state.go:194-202, :238-242); (owner, key) seal at admission (admission.go:159-187, :214-218); key-changing commands seal first (consensus_updates.go:88-152); transition blocks per key (table_set_transition.go:223-484); grouper keyed on (owner, key) (server/grouper.go:294-361); `arbiter_block_seal_reason`; new `fsm/l3_group_key_golden_test.go` (l3_track_golden_test.go is never edited); header serving (server/safestate.go:170-199) | XL |
| 73 | arbiter | MemberReady (tag 40), `SubmitMemberReady` handler (server/table_purge.go:23-40 pattern); group half of the eligible ring and the admission count; key-change seal; restore re-verification | L |
| 74 | arbiter | ReselectVerifiers (tag 39) with the latched-refusal amendment and a latch count (threeway.go:101-111; genesis_validation.go:538-593); proof per epoch (consensus_history.go:181 pattern); orchestrator proposal (dispatch.go:17-31; signer.go:9-39); quiet-track test driven by the cursor (apply_table_registry.go:196-218) | L |
| 75 | arbiter | MemberCheckpoint (tag 41) with checkpoint_seq kept durably; chain checks; classes and hysteresis; MarkMemberLagging (tag 42) with orchestrator timeout, ConsensusAdmin RPC and `arbiter-admin member lagging`; per-epoch replica_signer re-verification (consensus_history.go:181-215); `SubmitMemberCheckpoint` handler | XL |

**Ledger.** None. Consumes the S3-E1 names.

**Clarifications.**
- **K73, spec ambiguity (§6.4 versus §7.1/§9.3).** Delisting or evicting a member removes it from rings, keys (delisting only) and E in the same Apply. Its `members` entry changes only through RebalanceGroup. Raising `group_size` does not re-place existing groups.
- **K74, Leaving record.** It stores the entry at apply, plus the frozen E (node ids) and |E| for the non-strict release (amended §6.4).
- **K75, re-selection.** Replaced verifiers' evidence is deleted in the same Apply, so restore can check that evidence keys ⊆ VerifierSet. Each re-selection records the cursor block.
- **K76, bounded lag state.** A `LagAdvances` counter per (member, group), set at acceptance by a parent walk of at most `max_lag_publications + 1` steps and incremented per advancing publication of a non-quarantined O. No per-owner history.
- **K77, MarkMemberLagging RPC.** The operator RPC exists, because the playbook says "wait for or issue".

**Depends on.** The S3-E2 contract, which pins the `Stage3State` skeleton and switch flags. Drafting can run in parallel with E2 once §44 is fixed.

### S3-E4: P5 floor, narrowed drain, release and the v21 activation

**Goal.**
- The floor in Apply and the WorkSet, with strict mode, the Leaving release and the k-raise precondition.
- The narrowed drain gate.
- The integration harness and the release of the whole branch.
- The sentio-node release that advertises stage3_v1.
- The production runbook and rehearsal.
- The live activation with every switch off.

**Contract and clarifications.** Contract §53–§57; clarifications K78–K82.

| Task | Repo | Deliverable | Size |
|---|---|---|---|
| 76 | arbiter | `floorLocked(pp)` shared by applyScheduleUnsafeCleanup (apply.go:960-1020) and the WorkSet (reads_work.go:317-364); escapes; strict mode and code-13 backpressure (admission.go:148-158); `under_replicated` events and metrics; Leaving release evaluation; k-raise precondition (consensus_updates.go:51-87); `arbiter_floor_held_cleanup_age_seconds` | L |
| 77 | arbiter | touched tracks; floor-only holds are not in-flight (quarantine.go:203-254; call site consensus_updates.go:74-78); `DrainViewFor(draft)` keeping `DrainView()` (S3-A contract §4); hold readiness on the draft; arbiter-admin default `--hold issuance` once the leader reports narrowed_drain | M |
| 78 | arbiter | `fsm/testdata/pre_stage3_golden.json` with `TestPreActivationHistoryIsByteIdenticalAcrossStage3` (pre_tracks_golden_test.go pattern); chpipeline with three SNodes, members and the pool, each switch in order; offline replay of the devnet2 logs; full verification; release `ARBITER_TAG_P5`/`ARBITER_DIGEST_P5` | XL |
| 79 | sentio-node | pins `CORE_TAG_P5` and `HG_TAG_P5` (stage3_v1 SNodes, code 12 at the ingress); member consensus-mode config keys (ed25519 seed file, replica_signer key file, consent file), inert; `storage-integrity member-consent-statement` (the enrollment-statement pattern); replica_hosts from every current DatabaseGroup member's served host_indexer_id (never filtered by MemberReady); release `SN_COMMIT_P5`/`SN_DIGEST_P5`. It may start once `CORE_TAG_P5` exists | M |
| 80 | production | monitoring rules (fallback, seal reason, under_replicated, VERIFIERS_UNAVAILABLE, member classes, held-cleanup age); `raft.snapshot_body` value; voter budget re-check (values.yaml:162-170); render tests | M |
| 81 | production | runbook `...-stage3-p5-v21.md`: rehearsal on a disposable three-voter cluster restored from a copy of live state, voter restarts from v21, forced-mismatch run, v21check; activation under `--hold drain`; P5 PR | L |
| 82 | devnet2 (live) | verifiers, then voters on `ARBITER_DIGEST_P5` (still writing v20), hosts on `SN_DIGEST_P5`, rehearsal, activation, every voter writing v21 | L |

**Ledger.**
- Produces: `ARBITER_TAG_P5`/`ARBITER_DIGEST_P5`, `SN_COMMIT_P5`/`SN_DIGEST_P5`, `P5_PR`, `P5_REHEARSAL` (record), `ACTIVATION_EPOCH`, `P5_END`.
- Consumes: the S3-E1 names, `P4_END`, `MEMBER_IDS`.

**Clarifications.**
- **K78, floor gauge.** The held-cleanup age is a leader-local gauge (the P6 exit's "bounded held-cleanup age" is otherwise unspecified).
- **K79, K6 sunset.** S3-A's `--hold drain` default is replaced at switch 9.
- **K80, harness scope.** The §17 integration list, with real members from K66.
- **K81, rehearsal.** An older binary refuses v21 at the version byte (snapshot.go:274). The rehearsal tools are throwaway.
- **K82, activation preconditions.** Every non-quarantined data-plane node advertises stage3_v1. Unregistered P2/P3 members do not count. An unupgraded quarantined SNode can never register again, by design.

**Depends on.** S3-E3 and `P4_END` (spec phase order).

### S3-F: P6, switch-on in the order of §10.4 and raising k

**Goal.** Steps 1–10 on devnet2. Each switch is one confirmed authority-signed update. The Keeper headroom gate runs before step 6, and the exit is k = 2 for 7 days. No code: everything shipped in S3-E (K66).

**Contract and clarifications.** Contract §58–§61; clarifications K83–K87.

| Task | Repo | Deliverable | Size |
|---|---|---|---|
| 83 | production | member consensus-mode values; si_member_ddl user and const merge constraint profile (gated until step 6); reconciler container with its Secret only; member-entry and consent files; failure domains from `DEVNET2_FAILURE_DOMAINS`; monitoring; render tests | L |
| 84 | production | runbook `...-stage3-p6-switches.md`: per-step prerequisites, the update under a hold, verification reads, a forward-fix playbook per one-way switch, the §16 operator playbook, restarts strictly one at a time; P6 PR | XL |
| 85 | devnet2 (live) | steps 1–3 (purge, eviction_voiding, owner_aware_selection) | M |
| 86 | devnet2 (live) | step 4 (consents, entries, register, mark active) and step 5 (placement preview, switch-on, groups verified) | L |
| 87 | devnet2 (live) | Keeper headroom gate, then step 6 (si_member_ddl, reconcilers, replicas, MemberReady) | L |
| 88 | devnet2 (live) | steps 7–9 (reselection, checkpoints, narrowed_drain) | M |
| 89 | devnet2 (live) | step 10 (k = 2 after the precondition preview, the P4 anti-affinity proof and the Keeper check) and the 7-day exit | M |

**Ledger.**
- Produces: `P6_PR`, `PURGE_EPOCH`, `VOIDING_EPOCH`, `SELECTION_EPOCH`, `MEMBERS_EPOCH`, `PLACEMENT_EPOCH`, `PLACEMENT_CUTOVER_BLOCK`, `KEEPER_HEADROOM` (record), `STEP6_END`, `RESELECTION_EPOCH`, `CHECKPOINTS_EPOCH`, `NARROWED_DRAIN_EPOCH`, `K2_EPOCH`, `P6_END`.
- Consumes: `P5_END`, `ACTIVATION_EPOCH`, `ARBITER_DIGEST_P5`, `SN_DIGEST_P5`, `DEVNET2_FAILURE_DOMAINS`, `MEMBER_IDS`, `P4_END`, `UNSAFE_INC_ROOT`.

**Clarifications.**
- **K83, holds.** `--hold drain` through step 8, issuance from step 9.
- **K84, step 3 host check.** Before step 3, every SI host runs `SN_COMMIT_P5`. The stage3 gate cannot check the hosts' HouseGate, and an older host maps code 12 to OutcomeUnknown.
- **K85, step 4 consents.** Consents are signed inside each host pod with the si_indexers key.
- **K86, Keeper thresholds.** The runbook fixes the Keeper thresholds (znodes, memory, latency at owner + pool + members per table) and the abort criteria.
- **K87, exit metrics.** `safe_copies` counted over `node:` domains; held-cleanup age from K78.

**Depends on.** `P5_END`, `P4_END` (step 10), `P2_END` (members running, si_member, unsafe_inc).

## 2. Cross-plan conventions to fix before drafting

- **Numbering.** Tasks 22–89 and contract sections §16–§61 are pre-allocated as above. Clarification ranges, so plans can be drafted in parallel:
  - S3-C1 K23–K28
  - S3-C2 K29–K37
  - S3-C3 K38–K43
  - S3-C4 K44–K50
  - S3-D K51–K57
  - S3-E1 K58–K67
  - S3-E2 K68–K72
  - S3-E3 K73–K77
  - S3-E4 K78–K82
  - S3-F K83–K87
- **Ledger names.** Every name carries a phase suffix (`_P1`…`_P5`), so nothing collides with S3-A's `PROTO_TAG`/`CORE_TAG`/`HG_TAG`/`ARBITER_TAG`.
- **Ledger tool.** S3-B's `s3b-ledger.py` hardcodes the two plan files (its `S3A`/`S3B` constants). Proposal: `$HOME/.cache/s3-ledger/s3-ledger.py`, defined verbatim in S3-C1 §19 and written first by Task 22. It discovers plan files by globbing `docs/superpowers/plans/*-multi-source-si-s3*-*.md`, refuses a name defined in two files, and keeps the `ref`/`show`/`get`/`empty`/`set` verbs and S3-B's record procedure (D23). S3-B's own tasks keep `s3b-ledger.py`.
- **Activation model.** From S3-C1 through S3-D, no consensus change: no tag, field, snapshot version, header byte or feature string, and `LocalNodeFeatures` unchanged through `CORE_TAG_P4`. Every component stays rollback-able. The only exception is S3-D's optional additive, non-Raft settle RPC.
- **Branching rule for P5.** The stage3_v1 rule binds Tasks 64–78 to one arbiter branch. S3-D's `ARBITER_TAG_P4` must be cut from main before E2's pin merges.
- **Never edit.** Everything listed by S2-A and S3-A, plus `pre_tracks_golden.json`, the `l3_track_golden_test.go` constants and `conformance/stage3_reads_test.go`.
- **New frozen fixtures.** `pre_stage3_golden.json`, the l3 group_key vectors, v21 plain and interned containers, placement and group-key digests, authority/consent/member-ready/checkpoint vectors, tag 38–42 encodings, `conformance/stage3_test.go`.
- **Worktrees.** URWT `multi-source-s3c1` … `multi-source-s3f`, and `-live` variants for the live tasks.

## 3. Spec ambiguities and conflicts found (spec line numbers)

1. **§16 P2 exit (line 346) versus §2/§13.1 (lines 41, 307-313).** The 500-blocks-per-owner soak is infeasible at any v21 encoding. Blocking; K38 needs a spec amendment.
2. **§16 P1 (line 345).** "Tracks paused: none" forces per-statement `S3()` keys (K23). "Not delete" and "list on own prefix" are unmeasured (K24, K25).
3. **§8.2 si_member grants (line 172).** No `CREATE DATABASE ON hg_replica.*` and no S3 READ for the restore source. si_member_ddl has no `CREATE DATABASE ON hg_member_unsafe.*` (K32, K33).
4. **§8.2 (line 174).** The unsafe_inc root "before any member user" means P2, not step 6 (K42). The `<replicated_merge_tree>` merge default is a restart-only server setting, so it is folded into P2's restart (K39).
5. **§8.5 (line 192).** Scratch names use a node id that P2 members do not have yet (K29).
6. **§11 (line 293).** The lease does not cover in-flight queries (K44). si_gateway needs hg_replica SELECT (K48). Older clients refuse status `replica` (K47).
7. **§16 P4 (line 348).** "Signer rotation" is impossible with the same on-chain id (K51). The rebuild conflicts with the audit's name rule (K52).
8. **§6.1 (line 104).** "True by default" bools versus proto3 (K58).
9. **§9.3 (line 245).** Strict backpressure has no admission code (K59).
10. **§6.3 (line 117).** Salt ambiguity at backfill and re-placement (K61).
11. **§7.1 (line 135).** The restore rule needs a stored database/generation list (K62).
12. **§9.1 (line 228).** No checkpoint purpose and no excluded-reason set (K63).
13. **§10.4 (line 272).** The "no eligible ring until step 6" guarantee is not enforced by the switches (K68).
14. **§13.2 (line 317).** v21 lowers the document, not the memory term of L (K71).
15. **§6.4 versus §7.1/§9.3.** Delisting versus membership changes only through RebalanceGroup (K73).
16. **§16 P6 (line 350).** "Bounded held-cleanup age" has no metric (K78).
17. **§12 (line 303).** "Listing a third party's member is refused" is not machine-checkable without `operator_id`; it stays an operating rule (K68 note).

The amended items from housegate#236 are placed as follows:
- the two member users: S3-C2 Task 34, S3-C3 Task 37, S3-F Task 83;
- `table_engines_require_grant` at P2's restart: S3-C3;
- latched re-selection refusal: Task 74;
- strict Leaving release: Tasks 71 (record) and 76 (evaluation);
- domain counting: Task 76;
- checkpoint_seq: Tasks 63 and 75.

## 4. Defects in the written plans found while scoping

- **D1, S3-B runbook §13.4 budget formula (S3-B plan line 6216).**
  - The formula is linear and says a single sample "overstates the growth per promotion and so understates the budget". Under quadratic growth the opposite holds: the marginal cost is about 2·a·n, about twice the average.
  - At devnet2's current point the formula gives about 570 promotions to L/4, where a quadratic fit gives about 129.
  - Amend it before S3-B Task 15 runs. K28 carries the fix forward.
- **D2, ledger tool.** `s3b-ledger.py` cannot read new plan files (it hardcodes its two plan files); see §2.
- **D3, the three field-by-field VerifierEntry rebuilds.** arbiter-core authority/consensus.go:125, arbiter-core wire/consensus.go:139-149 and arbiter fsm/signed_claims.go:93-102 would silently drop the new fields. Owned by Tasks 59, 60 and 64, each with an every-field test.

## 5. Dependencies, release order and drafting waves

**Execution DAG.**
- S3-A → S3-B.
- S3-B → S3-C1 code → S3-C1 live → S3-C2 → S3-C3 → S3-C4.
- S3-C4 → S3-D live. S3-D's code can start after S3-C2.
- S3-E1 → S3-E2 → S3-E3 → S3-E4 code. The E4 live activation also waits for `P4_END`.
- S3-E4 → S3-F, which also needs `DEVNET2_FAILURE_DOMAINS` from S3-D.

**Release order per repository.** Names are tied to phases; release order follows the phases.
- housegate: `HG_TAG` → `HG_TAG_P1` → `HG_TAG_P3` → `HG_TAG_P5` (pins `PROTO_TAG_P5`).
- arbiter-proto: `PROTO_TAG` → `PROTO_TAG_P4` (optional) → `PROTO_TAG_P5`.
- arbiter-core: `CORE_TAG` → `_P1` → `_P2` → `_P3` → `_P4` → `_P5`.
- arbiter: `ARBITER_TAG` → pin commit `ARBITER_PIN_P2` → `ARBITER_TAG_P4` → `ARBITER_TAG_P5`.
- sentio-node: `SN_COMMIT` → `_P1` → `_P2` → `_P3` → `_P4` → `_P5`.

**Live-order constraints.**
- unsafe_inc exists before the first si_member (S3-C3).
- Every host runs `HG_TAG_P5` before switch 3.
- The Keeper gate passes before step 6.
- S3-D's hardening is done before k is raised.

**Drafting waves.** Fix the names in §1 and §2 first; this proposal is that pass.
- **Wave 1, in parallel:** S3-C1, S3-C2, S3-D, S3-E1 Tasks 57–61 with E1 contract §39–§42, and S3-E2.
- **Wave 2, in parallel, once the S3-C2 and S3-E2 contracts are fixed:** S3-C3, S3-C4, S3-E1 Tasks 62–63, S3-E3.
- **Wave 3, in parallel:** S3-E4 and S3-F.

## 6. Outside the plans

- **Security, before P1 creates GCS keys.** production `k8s-sea/clickhouse/clickhouse-extra.yaml` and `k8s-sea/clickhouse/clickhouse.yaml` each contain, in commented YAML, a GCP service-account private key (one key marker each; contents not reproduced here). The member-plane report adds that the same blocks hold a plaintext ClickHouse password. Rotate that service-account key and purge the history. P1 must not reuse that service account. This goes beyond S3-B's plaintext-credentials follow-up.
- **Out of scope by design.** §8.9 mutation pre-state, P7/v22 and P8 have no task.