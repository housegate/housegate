# Multi-source storage-integrity network — Stage 3：复制的 database group — 设计

**状态：** 设计稿，起草于 2026-10-14。本文是 umbrella spec [2026-10-10-multi-source-si-network-design.md](2026-10-10-multi-source-si-network-design.md) 的 Stage 3，该 umbrella 的 stage 1 和 stage 2 已建成并在 devnet2 上线。四项定型选择于 2026-10-14 与项目负责人一起确定：group 按 (owner, database) 划分；track 仍按 owner 划分；block 在 group 的 verify-member 集合上 seal；group 确定性放置，并由 governance 做 rebalance。每个子项目（§15）都在其所属仓库中有自己的 implementation plan。英文版 [2026-10-14-multi-source-si-stage3-database-groups-design.md](2026-10-14-multi-source-si-stage3-database-groups-design.md) 是事实源；协议语义变更时从英文版重新生成本中文版。

**仓库：** arbiter-proto、arbiter-core、arbiter（private）、housegate、sentio-node、production；rewriter-proto、rewriter-go 和 rewriter 仅用于 §11 的可选读取项。

**相关文档：** [2026-06-22-storage-integrity-design.md](2026-06-22-storage-integrity-design.md)（§5.4、§9、§10、§12.2、§12.5、§13：每个 attesting replica 都持有 `hg_safe`），[2026-06-10-multi-replica-trust-design.md](2026-06-10-multi-replica-trust-design.md)，[2026-06-30-sentio-arbiter-design.md](2026-06-30-sentio-arbiter-design.md)（§7.1 确定性 verifier 选择，§7.4 quorum，§10.6 `Sharder`），[Spec D](2026-08-18-storage-integrity-lag-replay-and-bootstrap-design.md)（本文部分取代，见 §20），[2026-07-01 insert/update/delete 设计](2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md)（mutation pre-state）。

**读取的 revision：** arbiter `fb42260`、arbiter-core `8f0bc43`、arbiter-proto `1c6ae67`、housegate `66d4df7`、sentio-node `5d2f169`、production `cd5b2174c`。跨仓库引用写作 `repo:path:line`，指向上述 ref；不带仓库前缀的路径属于 housegate。线上事实来自 2026-10-11（stage 1）和 2026-10-14（stage 2）的 devnet2 rollout 记录，以及一份 v20 voter snapshot 的本地副本。标注为 *实测*（*measured*）的 ClickHouse 机制于 2026-10-12 和 2026-10-14 在 Docker 中的 upstream `clickhouse-server` 26.3.37.3 上测得；P0 会在已部署的 `26.3-lts-decimal512` 镜像上把它们逐一重新 pin 一遍。

**术语。**
- *Owner*：托管某个 database 的 SI indexer（umbrella D4）。它的 SNode 是唯一的 source。
- *Group* G(O, d)：owner O 加上持有并校验 database d 的 SI 表的那些 *member*。
- *Member*：运行在 O 以外某个 SI indexer 主机上的 `si-member` 进程，拥有自己的 VERIFIER-role node identity。Member 具有 *verify* 角色（可进入 verify set）和 *replicate* 角色（持有一份 copy），并且可以 *count*（计入 copy floor）。
- *Network pool*：不由任何 indexer 托管的 `verifiers` 条目，即今天由网络运营的三个 verifier。
- *Copy*：某个 member 针对一个 group 的 `hg_replica` 表，其内容恰好等于某个已发布 manifest 所列出的内容。
- *Install*：把已校验的 parts 加入 copy。*Checkpoint*：member 对其 copy 所等于的 manifest 所做的签名声明。
- *Verify set*：为一个 block 选出的三个 verifier。*Eligible ring*：一个 block 的 verify set 可以从中抽取的 verifier。
- *Ready*：一旦某 member 针对某 group 的 `MemberReady` 在其加入该 group 时的 generation 或之后被记录，该 member 即在该 group 中 ready。
- *Physical incarnation*：处于 Pending、Active、Retiring 或 Purging 状态的 incarnation（`hasPhysicalTables`）；Legacy 和 Refused incarnation 不持有任何 `hg_*` 状态。
- S1–S17 是本阶段的决策；它们不延续 umbrella 的 D 系列编号。
- "parts are synced via Keeper" 中的 "Keeper" 指 arbiter，与 06-22 §5.2 一致：arbiter 发布的 manifest 授权每一次 install。ClickHouse Keeper 仍然只搬运 `hg_unsafe` 的字节，与今天相同。

## 1. 目标

每张 SI 表都在多个 indexer 上拥有已校验的 copy，而持有这些 copy 的 indexer 正是校验其写入的那些 indexer，这正是 06-22 设计的本意：一个 source 执行，其结果只校验一次，校验后的结果送达其他 member。一张表的 safe 数据不再依赖单块磁盘。被 quarantine 或不可达的 owner 的已校验数据仍然可读。校验容量随 indexer 数量增长，而不是压在三个全局 verifier 身上。Owner 仍是唯一的 writer，liveness 仍按 track 计算，本阶段的代价是一个不可逆的共识格式（snapshot v21），外加 devnet2 第三个 SI indexer 的不可逆 enrolment。

## 2. 本设计所依据的实测事实

- **只有一份可查询的 copy。** 一张表唯一的 `hg_safe` copy 位于其 owner 的 source ClickHouse 上。Verifier 的 `hg_unsafe` copy 会被 owner 的 cleanup 擦除，该 cleanup 是一个复制的 `DROP PART`（`arbiter-core:snode/cleanup.go`；实测：在一个 RMT replica 上执行 `DROP PART` 会在每个 replica 上移除该 part）。Verifier 会创建 `hg_safe`，但收不到任何 promotion；promotion 只发往 owner 的 SNode，且只有它的 acknowledgement 算数（umbrella §6.6）。
- **专用 verifier 是部署形态的产物。** 06-22 §9 在 source 和每个 attesting replica 上做 promotion，§10 从"每个 attesting replica 的 hg_safe"获取 mutation pre-state，06-30 在每个 indexer 节点旁配一个 verifier，而 Spec D D1 说 verifier 会成为 safe replica。没有任何 spec 决定 verifier 必须是独立节点。当前形态源于 v1 是 single-writer，源于加入 verifier 时 devnet2 的 indexer ClickHouse 没有 Keeper，也源于 Spec D 从未被实现。
- **Verifier 选择是全局且冻结的。** `VerifierQuorum = 2` 和 `VerifierSelectN = 3` 是冻结的常量（`arbiter:fsm/state.go:21-27`）。`selectVerifiers` 取所有 Active VERIFIER 节点按 NodeID 排序的 ring，去掉该 block 的 source，从一个 block-seeded 起点挑选三个连续位置，不足三个时返回 nil（`arbiter:fsm/select.go:55-77`）。它在 `applyMarkReplaying` 中每个 block 运行一次，后者拒绝 nil 集合，并且从不重新选择（`arbiter:fsm/apply.go:425-445`）。一个 block 若其完整 evidence 中 pass 少于两个，就是 QuorumFailed；orchestrator 会打开一个 challenge 并立即将其裁定为 Rejected（`arbiter:orchestrator/dispatch.go:32-79`），从而 quarantine 该 track。
- **Verifier identity 没有主机。** `VerifierEntry` 是 `{node_id, ed25519_pubkey}`（`arbiter-core:consensus.go:79-85`）；activation 之后的注册恰好携带一个 role；每次更新都必须保留至少三个已列出且未被 evict 的 verifier（`arbiter:fsm/signed_claims.go:284-299`）。因此 FSM 无法把某个 owner 所运行的 verifier 排除在该 owner 的 block 之外。
- **每个 block 一个 owner，多张表。** tracks activation 之后，另一个 owner 的 statement 会先 seal 当前的 open block（`arbiter:fsm/admission.go:159-187`），且 leader 在 50 ms 窗口内按 owner 对提交进行分组。一个 block 可以包含其 owner 多张表的 statement；每个被选中的 verifier 都会 replay 整个 block，并对其中每个 candidate part 做 byte scan。每个 track 最多保留一个未发布的 transition block。
- **校验输入。** INSERT replay 只需要 block 的 pinned manifest 和 DA payload；byte scan 读取 verifier 自己的、block 中每张表的 `hg_unsafe` RMT replica。Keeper 路径是 `/sentio/<shard>/unsafe/<db__table>`，以 node id 作为 replica name（`arbiter-core:dataplane/ddl/naming.go:42-46`）；它既不按 owner 也不按 incarnation 划分 scope。Reconciler 把不在 `GetPurgeNodeSet`（每个已注册、未被 evict 的 SNode 和 verifier，`arbiter:fsm/apply_table_registry.go:404-425`）中的 replica name 视为已退役节点：只要存在这样的 replica，verifier 就拒绝创建 incarnation，而 owner 会尝试对其执行 `SYSTEM DROP REPLICA`（`arbiter-core:dataplane/tableset/reconciler.go:704-787`）。
- **Purge 是名字复用的 fence。** 一个名字在其 incarnation 被 Purged 之后即可重新声明（`arbiter:fsm/apply_table_registry.go:291-296`）；purge 需要 owner 的 SNode 和每个未被 evict 的 verifier（`apply_table_registry.go:469-494`）。实测：当一个已 purge 的 incarnation 的陈旧 replica 留在未 scope 的路径上时，重新创建的表的 `hg_unsafe` 会克隆旧 incarnation 的每一行。
- **Mutation。** UPDATE 和 DELETE 尚未实现（admission 只接收 INSERT）。06-22 §10 和 07-01 设计需要每个 attesting worker 本地的 `hg_safe` pre-state，而今天的 verifier 没有。
- **Stage 2 的实现现状。** Safe prefix、promotion frontier 和 manifest debt 都按 track 计；manifest 携带 `track_frontiers`，restore 会依据保留下来的、source 签名的 acknowledgement 证明其中的 `applied_promotion_seq` cutoff 是精确的（C17）；没有前进的 track 被原样 Carried（`arbiter:fsm/reads_manifest.go:251-292`），且 manifest 组装不检查 quarantine，因此被 quarantine 的 track 仍可能因一个迟到的 acknowledgement 而 Advance。Cleanup coverage 是 leader 的一条 WorkSet 规则（`arbiter:fsm/reads_work.go:317-364`）；`applyScheduleUnsafeCleanup` 检查 quarantine 但不检查 coverage（`arbiter:fsm/apply.go:960-1020`）。Drain gate 只跳过被 quarantine 的 track（`arbiter:fsm/quarantine.go:205-256`）。每个 consensus update 都绑定到一个精确的 `expected_promotion_seq`（`arbiter:fsm/consensus_updates.go:32-34`）。
- **永不清除的 release refusal 是潜在隐患。** Stage-2 runbook 列出两种"在协议内没有回头路"的情况：(a) 带有已 promote parts 的 Rejected statement，以及 (b) 在裁决之前发出、其 acknowledgement 丢失的 promotion。Statement challenge 只由 orchestrator 打开，且只针对 QuorumFailed block（`arbiter:orchestrator/dispatch.go:67-79`），而 promotion 需要 quorum，因此情况 (a) 不会由已上线的 orchestrator 引发。但 FSM 仍然允许对可 promote 或已部分 promote 的 statement 发起 challenge；这样的行会留在 owner 的 `hg_safe` 中，进不了任何已发布的 manifest，而该 track 又无法越过这个 block。
- **Quarantine 会把数据藏起来。** 被 quarantine 的 owner 的主机拒绝其表的每一次读写（C13），而 peer 把这些表当作 Ordinary 处理并路由回 owner，因此没有任何节点为它们提供服务；在 devnet2 上这种情况持续了 10.9 h。
- **共识状态呈二次方增长。** 一份 devnet2 v20 voter snapshot（promotion 53 时）的本地副本为 11,385,138 字节，其中 manifest 占 6.86 MB（62 个 manifest；最后一个用 190 KB 列出 39 个 part，每个 part 条目约 4.9 KB，主要是 4096 字符的 LtHash hex）。每次发布都会存储每张表的完整 manifest（`arbiter:fsm/apply.go:945`），每个 cutover 之后的 acknowledgement 都会存储完整的 partition inventory（`arbiter:fsm/apply.go:925-933`）；没有任何裁剪。Reader 拒绝超过 1 GiB 的 doc（`arbiter:fsm/snapshot.go:282`），writer 则没有上限。推算：仅 manifest 这一项就约为 4.9 KB × Σ(各次发布中的 parts)，在全网约 650 次 single-part promotion 时达到 1 GiB。Voter 以 2Gi 内存限制和 2Gi PVC 运行（`production:charts/storage-integrity/values.yaml:162-170`），因此它们会更早失败。
- **重启复活。** MergeTree 上的 `REPLACE PARTITION` 会把被替换的 parts 作为 outdated 留在磁盘上，直到 `old_parts_lifetime`（480 s）到期；在该窗口内重启 ClickHouse 会重新激活它们，因为替换 parts 的 block range 并不覆盖它们（实测：3 行变成 5 行）。随后 owner 对该 partition 的下一次 promotion 会在 closure check 上失败。没有任何代码修复它（`arbiter-core:snode/promote_replace.go:349-356`；housegate `CLAUDE.md` 的 merge-guard note）。
- **Peer-trusted 旁路。** `tablerefguard` 对 peer-trusted、非 forwarded 的 session 提前返回（`pkg/plugins/tablerefguard/plugin.go:80`），`sireserved` 只作用于 maintenance 和 platform-operator session（`pkg/plugins/sireserved/plugin.go:24-33`），而 reserved database 只有 `hg_safe`、`hg_unsafe` 和 `hg_promote`（`pkg/sitable/sitable.go:145-147`）。
- **被 evict 的节点仍能签署某些消息。** Result claim，以及 promotion、cleanup 和 rejected-cleanup acknowledgement，仅凭 `si_indexers` 签名即被接纳（`arbiter:fsm/signed_claims.go:442-455`、`:788-800`），verifier evidence 则凭集合成员身份和 ed25519 签名被接纳（`arbiter:fsm/apply.go:589-645`）；MarkActive 和 purge report 已经会拒绝被 evict 的节点。
- **基础设施。** 每个 SI ClickHouse 都共用 production 的 `clickhouse-keeper-extra` ensemble，该 ensemble 同时服务 production 的 `clickhouse-extra` CHI，且没有 ACL；该 namespace 没有 NetworkPolicy；interserver HTTP 没有凭据；`default` 用户没有密码、对所有网络开放，并拥有 access management 权限；SNode 和 verifier 只用地址连接（`sentio-node:standalone/standalone.go:212-214`、`arbiter:cmd/internal/startup/clickhouse.go:22`）；indexer-a 的 source 与 verifier 一起打包在网络 CHI 中；`local-path` 不强制大小限制，并把卷放在共享的节点 SSD 上；没有部署任何 SI 告警规则。DA payload 无限期保留（AUDIT pin 从不释放）。
- **ClickHouse 机制（实测）。** RMT replica 持有字节完全相同的 parts（相同的 name 和 `hash_of_all_files`）。`ATTACH PARTITION FROM` 保留 `hash_of_all_files`，且不受 `parts_to_*` 限流；在同一磁盘上跨 database 时它使用 hardlink。带 `base_backup` 的 `BACKUP ... PARTITIONS` 是增量的，`RESTORE ... AS <staging> PARTITIONS` 会恢复每个 part 及其 `hash_of_all_files`。用 `REPLACE PARTITION` 修复一个 partition 并在 outdated 窗口内重新加载，会带回一个外来 part（3 行变成 10 行）；先 `DROP PARTITION` 再 `ATTACH PARTITION FROM` 则不会。`INSERT ... SELECT FROM remote(...)` 在 `min_insert_block_size_rows` 不小于该 part 的行数、`min_insert_block_size_bytes = 0`、`max_threads = 1` 且 `max_insert_threads = 1` 时，为一个 3M 行的 part 产出单个 part。被授予 `REMOTE` 的用户可以通过 `remote()` 写入；`READ ON REMOTE` 拒绝写入；named collection 的凭据可以被覆盖，除非设为 `NOT OVERRIDABLE`。在一个 RMT replica 上发出的 `OPTIMIZE` 合并了另一个 replica 的 parts，尽管该表固定关闭了 merge。

## 3. 范围

范围内：database group；校验并持有 copy 的 member；感知 owner 的 verifier 选择与重新选择；manifest-gated install；copy floor；收窄的 drain gate；在 owner 被 quarantine 或不可达时从 member 读取；共识状态增长的防护以及一种无损编码；§2 中既有的隐患；把 devnet2 扩展到三个 SI indexer。

范围外：multi-writer（S1）；owner rebase 与 takeover、history pruning、O(1) acknowledgement 记录，以及针对 release refusal 情况 (a) 的 subtractive revert（v22，§16）；按 (owner, database) 划分的 track（S2）；超出 pre-state 契约的 UPDATE/DELETE（§8.9）；由第三方运行的 member 参与校验或计数（§12）；DA rematerialisation（P8）；把 SI 迁到专用的 Keeper ensemble，这需要单独的设计（§14）。

## 4. 决策

**S1 — Owner 仍是唯一的 writer。** Owner 的 SNode 仍是唯一的 source、唯一的 promotion 目标和唯一的 source acknowledgement。Member 从不写任何 owner 侧对象，也从不 apply `PromoteSafePartition`。否决：multi-writer，因为 candidate attribution 是对共享 RMT 表做的 before/after part diff（在另一个 replica 上插入的 part 会出现在 diff 中，实测），abort 是复制的 `DROP PART`，`replicated_deduplication_window` 为 0，FSM 对每个 statement 和每个 promotion 只绑定一个 source，而且 `REPLACE PARTITION` 本来就会串行化一个 partition 的 promotion。

**S2 — 单位是 database group；track 仍按 owner 划分。** 每个拥有 physical SI incarnation 的 database d 都有一个 group G(O, d)；G 的每个 member 都持有 d 的每张 SI 表。Track、quarantine 和 manifest 的 `track_frontiers` 仍按 owner 划分。后果：一个 owner 的 track 的 liveness 取决于它最弱的 group，因为它的 prefix 是顺序的。否决：按表划分的 group，因为一个 block 包含同一 owner 的多张表，而每个被选中的 verifier 都必须持有所有这些表（若 `dex.swaps` 的 group 是 {B, C, D}、`dex.pools` 的 group 是 {C, D, E}，那么同时包含这两张表的 block 只有两个 eligible verifier，会被拒绝），而按表拆分 block 会成倍增加 anchor 和 replay，却并不能把各表隔离开。否决：按 (owner, database) 划分的 track，因为每个 manifest 都要为每个 database 携带一个 frontier。

**S3 — Member 既校验又复制。** Member 条目携带 `verify`、`replicate` 和 `counts` 标志，默认全为 true。Verify member 可以进入 eligible ring；replicate member 持有 copy；counting member 计入 copy floor。校验只需要临时的 `hg_member_unsafe`；复制则额外需要永久的 `hg_replica`。

**S4 — Group 确定性放置，并由 governance 做 rebalance。** 当一个 database 获得其第一个 physical incarnation 时，Apply 用整数加权 rendezvous hashing（§6.3）放置其 group，而开启 placement 的那次 update 会放置所有现存的 database。已放置的 group 只能通过带 generation 检查、由 authority 签名的 `RebalanceGroup` 改变（§6.4）。用户和 owner 都不能选择 member（06-30 §7.1，06-10 R6）；placement salt 是用户无法控制的链上数据。

**S5 — Block 在 group 的 verify-member 集合上 seal。** Open block 记录其 owner 和 group key：其 statement 所属 group 的已排序 ready verify member 以及 network-fill 标志。(owner, key) 不同的 statement 会先 seal open block；任何改变某个在 open block 中有 statement 的 group 的 key 的命令，也会先 seal open block。Group key 相同的 database 共享 block，因此在 devnet2 上（一个 owner 的所有 group 都重合），seal 行为与今天相同。Leader 的分组窗口按同一对键分组。

**S6 — Verifier 选择按 group 进行，并支持确定性的重新选择。** 一个 block 的 verify set 是由现有的 block-seeded ring 从其 group 的 eligible member 中选出的三个 verifier；在该 group 的 fill 开启时，或仅靠其 member 不足三个时，还会加上 network pool。2-of-3 规则不变。由 authority 签名的 `ReselectVerifiers` 按存储的 ring 顺序替换失效、不再 eligible 或超时的 verifier，超时以已提交的 L2 registry block 计量，因此在安静的 track 上也会触发。

**S7 — Member 是绑定主机的 verifier identity。** Member 是列在受治理的 `verifiers` 参数中的 VERIFIER-role 节点，带有新的 `host_indexer_id`，并经该 indexer 的 `si_indexers` signer 同意；没有主机的条目构成 network pool。Member 用单独的 `replica_signer` 签 checkpoint，因此泄露的 evidence 密钥无法移动 copy floor，而泄露的 replica 密钥只能降低其 owner 的 floor。

**S8 — Copy 由 manifest gate。** Member 只 install 某个已校验的已发布 manifest 在 `track_frontiers[O]` 处为该 group 的表列出的 parts，且一份 copy 总是等于某个这样的 manifest 的 part row hash multiset。Promotion 仍只在 owner 上进行。否决：promotion fan-out（字面理解 06-22 §9，以及 Spec D D2–D4），因为 member 会继承先 promote 后 reject 的行、与 owner 一起冻结，并且各自都需要 acknowledgement、base CAS 和 revert。

**S9 — 每个字节来源都经过同一个 gate。** 该 gate（§8.5）接受以下来源的 parts：member 自己的 `hg_member_unsafe` replica（hardlink，即它扫描过的字节）、从 owner 的 `hg_safe` 或某个 peer 的 unsafe replica 做的 no-split pull、object-store restore，以及后续的 DA rematerialisation。不信任任何传输通道。

**S10 — Copy floor gate cleanup。** 一个 promotion 的 unsafe parts 只有在足够多 counting member 的 checkpoint 反映了它之后才会被 cleanup（§9.3），因此在 §9.3 的 escape 之外，Active incarnation 的一行已校验数据在任何时刻都至少位于 min(k, 1 + |E|) 个 ClickHouse 主机上，其中 k = `stage3.min_safe_copies`，E 的定义见 §9.3。Escape 是已提交状态的纯函数；strict mode 要求 reflecting member 覆盖 k − 1 个不同的 failure domain，用 owner 的写 liveness 换取 floor。Floor 以休眠状态（k = 1）上线。

**S11 — Drain gate 收窄。** 一个 consensus update 只等待其所改变的 `si_indexers` 条目对应 track 的 in-flight 工作，而被 floor 扣住的 unsafe parts 不算 in-flight 工作；一个短暂的 issuance hold 提供每次 update 都需要的精确 `expected_promotion_seq`（§10）。

**S12 — Owner 无法服务时由 member 服务读取。** Member 主机仅在 owner 被 quarantine 或从该 member 主机不可达时，才以 safe mode、按最新的已发布状态从其 copy 提供某个 database 的 SI 表（§11）。

**S13 — 在 group 足够大之前由 network pool 补位。** 当 group 的 fill 开启或其 eligible member 少于三个时，pool 加入该 group 的 eligible ring。一旦某个 group 在不同主机上拥有三个 ready 的 peer verify member，就按 group 退役 pool，这至少需要四个自运营的 SI indexer。

**S14 — 只有当每个 SI indexer 都是自运营时，peer 才计数。** 此时 peer member 与网络 verifier 一样可信。在第三方的 member 参与校验或计数之前，§12 必须成立；在 `operator_id` 存在之前，列出第三方的 member 会被拒绝。

**S15 — 一个不可逆格式。** 本阶段的每项共识变更都在 snapshot v21 中发布，各自位于独立的单向开关之后，按 §10.4 的顺序开启；v22 保留给 owner rebase 与 takeover、history pruning、O(1) acknowledgement 记录以及情况 (a) 的 revert。

**S16 — Purge 仍是 fence。** 新 incarnation 使用 incarnation-scoped 的 Keeper 路径；对于每个位于未 scope 路径上的 incarnation，purge 要等待每个可能在那里持有 replica 的节点；失效的 member 只有通过 eviction 才会离开该集合（§8.8）。

**S17 — 先修复已知隐患，并把 devnet2 扩展到三个 indexer。** P0 封堵 peer-trusted 旁路和重启复活，ClickHouse lockdown 先于任何 member 连接，eviction voiding 在 v21 中发布，devnet2 enrol 第三个 SI indexer，使每个 owner 都有两个 peer（§14，§16）。

## 5. 用例子说明概念

Indexer A–F 都是 SI indexer。A 托管 database `dex` 和 `nft`。Placement 给出 G(A, dex) = A + {B, C, D} 和 G(A, nft) = A + {C, E, F}；network pool 是 {v1, v2, v3}；两个 group 的 fill 都开启。

在一个分组窗口内，A 的 ingress 依次提交 `dex.swaps`、`dex.pools`、`nft.mints`、`dex.swaps`。Leader 按 key 对它们分组：block 100 包含三条 `dex` statement，其 verify set 是 {B, C, D, v1, v2, v3} 中的三个；block 101 包含 `nft.mints`，其 verify set 是 {C, E, F, v1, v2, v3} 中的三个。每个被选中的 member 从 DA 和 pinned manifest replay 其 block，并对其自己的 `hg_member_unsafe` 所 fetch 的 candidate parts 做 byte scan；quorum、finality 和 owner 的 promotion 照常进行。当覆盖 block 100 的 manifest 发布时，B、C 和 D 通过 hardlink 把新的 `dex` parts install 到各自的 `hg_replica` copy 中，按 manifest 校验它们并签 checkpoint；这些 parts 的 `hg_unsafe` cleanup 要等到其中 k − 1 个签完。

如果 A 被 quarantine，B 可以从其 `dex` copy 回答 `SELECT ... FROM dex.swaps JOIN dex.pools`。如果 B 宕机，A 的 `dex` block 仍可由 {C, D, v1, v2, v3} 校验，而 `nft` 不受影响。如果 A 的两个 database 有相同的 key，它们会共享 block。

在有 indexer a、b、c 的 devnet2 上，owner a 的每个 group 都是 {b, c}，owner b 的是 {a, c}，owner c 的是 {a, b}；每个 block 从五个 eligible verifier（两个 peer 加上 pool）中抽取三个，每张表在稳态下有三份 copy，在 k = 2 下至少有两份。

## 6. Group 与 placement

### 6.1 参数

- `verifiers` 条目新增 `host_indexer_id`（指针；network pool 为 nil）、`replica_signer`（secp256k1 地址）、`consent_jws`、`weight`（1–64，默认 1）、`failure_domain`（字符串），以及标志 `verify`、`replicate`、`counts` 和 `shared_rmt`（默认全为 true）。每个带 `host_indexer_id` 的条目都必须有非空的 `failure_domain`，且等于 `indexer_failure_domains[host_indexer_id]`；相同的字符串即同一个 domain。在 per-operator transport（P8）能够为 check 3 提供输入之前，Apply 拒绝 `shared_rmt = false` 且 `verify = true` 的条目。Consent 声明是一个 purpose 为 `arbiter-group-member-v1` 的 ES256K JWS，签名内容为 `{network_id, genesis_snapshot_id, host_indexer_id, node_id, ed25519_pubkey, replica_signer}`，由主机的 `si_indexers` signer 签名；与 enrolment 声明一样，它在条目被添加或其密钥轮换时被校验。Member 的 `node_id` 不得已被任何 SNode 或 verifier 使用，且其主机必须已 enrol。
- 新的 `stage3` 参数携带 `group_size`（默认 3）、`network_fill_default`（默认 true）、`min_safe_copies`（默认 1）、`strict_copies`（默认 false）、`indexer_failure_domains`（indexer id → domain）、`max_lag_publications` 和 `lag_low`（满足 1 ≤ `lag_low` < `max_lag_publications`）、`evidence_timeout_l2_blocks`、`max_uncleaned_parts_per_partition`、`max_uncleaned_parts_per_table`、`exclude_quarantined_hosts`（默认 false），以及 §10.4 的开关。一旦设置，之后的每次 update 都会携带它；其开关和 `strict_copies` 是单向的，`min_safe_copies` 和 `group_size` 永不减小，其他字段可以在任意 update 中改变。`indexer_failure_domains` 必须列出每个已 enrol 的 indexer；若留下某个未设置的 domain，Apply 会拒绝开启 placement 的 update 以及之后的任何 update。
- 现有的 floor"至少三个已列出、未被 evict 的 verifier"保持不变。新增一个 *per-group verifier floor*，与全局 floor 一样统计已列出、未被 evict 的条目：每个 group 必须至少有三个，在其 fill 开启时把 network pool 计入。它从不拒绝 `EvictNode`。

### 6.2 Group 记录

Table registry 新增以 (owner, database) 为键的 `DatabaseGroup{owner, database, members, network_fill, generation, past_members}`，并在 `GetTableRegistry` / `WatchTableRegistry` 中提供。`members` 就是被选中的列表本身，因此审计者直接与它比对，而不必重建节点历史。`past_members` 列出这样的每个节点：它在任一更早的 generation 曾是 member，且尚未针对"它作为 member 期间任一时刻曾为 physical 的 d 的每个 incarnation"报告 purge（§8.8）；它只能通过报告或通过 `EvictNode` 离开。每个 (owner, database) 保留一个持久的 `group_generation`，即使 group 被移除也会保留；重新放置的 group 从它继续，因此旧的 `RebalanceGroup` 或 `MemberReady` 无法被 replay。一旦 placement 开启，每个拥有 physical incarnation 的 (owner, database) 恰好有一个 group；restore 会强制这一点。Group 在 purge 其 database 最后一个 physical incarnation 的那次 Apply 中被移除。

### 6.3 Placement

每当 registry 获得 d 的一个 physical incarnation 而 G(O, d) 不存在时，Apply 就放置 G(O, d)；Legacy 和 Refused incarnation 从不创建或保留 group。开启 `placement` 开关的 update（它同时开启 seal key）按 (owner, database) 顺序、基于当时已提交的条目，为每个已经拥有 physical incarnation 的 database 放置一个 group，包括从不经过 `AddTable` 的 genesis-origin incarnation。

- 候选者是已列出、未被 evict、带 `verify` 或 `replicate` 的 member 条目，其主机已 enrol、不是 O，并且在 `exclude_quarantined_hosts` 开启时未被 quarantine。
- 权重为 w 的候选者 c 的得分 s_c，是 i 取遍 [0, w) 时 `CanonicalDigest("arbiter-group-placement-v1", {network_id, owner, database, salt, node_id, i})` 的 256-bit big-endian 值的最大值。`salt` 是 d 的第一个 physical incarnation 的 `TableCreated` 事件所在 L2 block 的 hash，对于 genesis-origin database 则是 genesis snapshot id，因此用户无法通过刷 database 名称把 placement 引向自己选定的 member。w 次均匀抽样的最大值对候选者的排序与 −ln(u)/w 完全一致，且只用整数运算。
- 按得分降序选取候选者，得分相同时按 `node_id` 升序，跳过其主机 indexer 或 failure domain 已被某个已选 member 或 O 占用的候选者，直到选出 `group_size` 个。
- `network_fill` 取 `network_fill_default`，当选出的 verify member 少于三个时强制开启。

Group 可以小于 `group_size`；此时由 network pool 补足 per-group verifier floor（S13）。新放置的 group 的 member，无论是在 `AddTable` 时还是在 backfill 时放置的，都只有在 ready 之后才进入 eligible ring；在此之前由自动 fill 提供 pool。

### 6.4 Rebalancing

`RebalanceGroup{owner, database, expected_generation, add, remove, network_fill}` 由 authority 签名，并像 `QuarantineIndexer` 一样置于 `ConsensusParamsUpdate` 之外。Apply 拒绝：过期的 generation、由 owner 托管的 member、位于同一主机或同一 failure domain 上的两个 member、与 owner 处于同一 failure domain 的 member、低于 per-group verifier floor 的结果，以及在 group 仍需要自动 fill 时关闭 fill。如果 open block 包含该 group 所属 database 的 statement，Apply 先按 C10 的纪律 seal 它；若无法准备 seal，则以状态不变的方式拒绝。新增的 member 只有在 ready 之后才进入 eligible ring，即在它用 `MemberReady{node_id, owner, database, generation}` 报告其 replica 之后；该消息用其 ed25519 node key 在 purpose `arbiter-member-ready-v1` 下签名（只有 `MemberCheckpoint` 使用 `replica_signer`）；它只从其第一个 checkpoint 起计入 copy floor。被移除的 member 处于 Leaving：它继续校验，直到没有任何未完成 block 的 verify set 点名它；并保留其 copy，直到有足够多的其他 copy 持有 `RebalanceGroup` apply 时最新的 entry：非 strict mode 下，E 中除该 Leaving member 外、覆盖至少 min(k − 1, |E|) 个不同 failure domain 的 member（E 与 |E| 按 §9.3 定义，两者都按 `RebalanceGroup` apply 时的取值计算）拥有位于该 entry 或之后、且不排除该 group 任何表的 checkpoint；strict mode 下，该 group 中覆盖至少 k − 1 个不同 failure domain 的 member（每次检查时按 §9.3 计算 E 的方式、并以该次检查时的 k 和 `strict_copies` 计数，且从不计入该 Leaving member 自身）拥有位于该 entry 或之后、且不排除该 group 任何表的 checkpoint。Strict mode 下，若某个 group 无法达到 k − 1（例如唯一已追上的 member 被一个 Bootstrapping member 替换），它会一直保留该 Leaving member 的 copy，直到能够达到，或该 member 被 evict。Apply 在其条件首次成立时把释放记录到 group 记录中且永不撤回，registry view 发布这一已记录的释放。之后它删除其 copy 和 `hg_member_unsafe` replica，留在 `past_members` 中，并在每个 incarnation 处于 Purging 时报告该 incarnation 的 purge（§8.8）。`RebalanceGroup`、`MarkMemberLagging` 和 `EvictNode` 会重新运行 `completePurgesLocked`。

## 7. Block 与校验

### 7.1 Seal key 与 header

- `GroupKey = CanonicalDigest("arbiter-group-key-v1", {sorted node ids of the group's ready verify members, network_fill})`。Open block 记录 `Owner`、`GroupKey`，以及该 key 所 hash 的 member 列表和 fill 标志；`commitSealLocked` 把它们复制到该 block 的 `BlockVerification` 中。
- 当新 statement 的 (owner, `GroupKey`) 不同时，admission 会 seal open block：在 spent-id 插入之前准备 seal，只有在插入成功之后才提交它（C10）。任何改变某个在 open block 中有 statement 的 group 的 ready verify member 或 fill 的命令——`RebalanceGroup`、`MemberReady`，或者翻转某个条目的 `verify` 标志或将其除名的 `ConsensusParamsUpdate`——都会先 seal open block，若 seal 无法准备，则以状态不变的方式被拒绝。
- 在 `placement` 开关所记录的 cutover block 之后 seal 的每个 block（包括 transition block），其 sealed header 都携带 `group_key`（位于 track pointer 中，omitempty），更早的 block 则省略它。Restore 恰好要求这一点，并要求 open block 的 `GroupKey` 恰好在开关开启且该 block 含有 statement 时存在，且等于其每条 statement 所属 group 的当前 key。Golden chain-hash 向量固定了携带 `group_key` 的一个 statement block 和一个 transition block。
- 没有 group 的 database（placement 开启之前）保持 stage-2 行为：按 owner seal，并从 network pool 中抽取 verifier。
- 一个 transition block 只承载一个 key 的变更。每个 track 最多一个未发布 transition block 的限制保持不变，因此一个 owner 针对不同 key 的 transition 会依次 seal，每个都在前一个发布之后。Restore 要求 transition block 的每项变更都属于这样一个 database：其 group 在 seal 时以 header 的 `group_key` 被记录在该 block 的 `BlockVerification` 中。

### 7.2 分组窗口

Leader 的提交分组窗口按 (owner, `GroupKey`) 分组。每多一个 block 就多花一笔 anchor 交易和三次 replay；新增指标 `arbiter_block_seal_reason{reason="group_switch"}` 并为其配置告警。

### 7.3 选择与 admission

- 在 `MarkReplaying` 时，eligible ring 由以下部分组成：与该 block 一起记录、仍以 `verify` 列出、未被 evict、Active、在该 block 所记录的 group 中 ready、不由该 block 的 owner 托管，并且在 `exclude_quarantined_hosts` 开启时不由被 quarantine 的 indexer 托管的 member；当记录的 fill 开启或仅靠 member 不足三个时，再加上 Active 的 network pool。Block-seeded 起点和三个连续位置不变。`MarkReplaying` 把 eligible ring、一个 ring position（最后使用的位置）以及已提交的 L2 registry cursor block 存入 `BlockVerification`。
- 对于一个 statement，若用同一过滤条件计算出的其 group 的 eligible ring 少于三个 verifier，admission 就以新的可重试错误 `VERIFIERS_UNAVAILABLE` 拒绝它，状态不变（与 `SOURCE_UNAVAILABLE` 类似）。
- 在 `MarkReplaying` 时仍找不到三个 verifier 的 block 保持未标记状态，且在 eligible 数量变化之前 WorkSet 不再列出它，这样 orchestrator 就不会在每一轮都提议一个会被拒绝的命令。
- 在 seal 之后被移除的 member 永不会被选中；已经选出的集合只能通过 `ReselectVerifiers` 改变。

### 7.4 重新选择

`ReselectVerifiers{block_seq, expected_set}` 是 `ConsensusParamsUpdate` 之外的 authority 命令，由 orchestrator 的 authority signer 签名，由 leader 为 evidence 不完整的 block 提议。
- 对于记录集合中的某个 verifier（member 或 pool），只有当它对该 block 没有完整 evidence，并且它已被 Evicted、不再 eligible，或者已提交的 table-registry L2 cursor 自 marking 或上一次重新选择以来已前进至少 `evidence_timeout_l2_blocks` 时，Apply 才会替换它。无论 track 的流量如何，cursor 每隔 `cursorAdvanceInterval` 都会前进。拥有完整 evidence 的 verifier 只有在被 Evicted 时才会被替换。
- 替换者是存储的 ring 中位于存储的 ring position 之后的下一些位置，跳过已在集合中、曾被从集合中移除或不再 eligible 的 verifier；每次重新选择都记录 L2 registry cursor block；一个 block 最多被重新选择 ring size − 3 次，没有替换者时该命令被拒绝。
- 对于 quorum 已经 latch、或其某条 statement 已被 challenge 的 block，Apply 拒绝 `ReselectVerifiers`：已 latch 的结论不再需要更多 evidence，而促成 latch 的 evidence 必须留在 restore 用来检查它的集合中。因此在 latch 之后被 evict 的 verifier 仍留在集合中；eviction voiding（§14）保留已 latch 的结论。
- 剩余 verifier 已被接受的 evidence 保留；被替换 verifier 的 evidence 被忽略。
- 命令的 authority JWS 和 epoch 会被存储。Restore 对每次存储的重新选择做结构性检查（被替换的 id 曾在集合中，替换者在存储的 ring 中紧随 cursor，集合是三个已排序且唯一的 id，且 latch 之后没有任何重新选择：Apply 在结论 latch 时记录该 block 当时的重新选择次数，restore 要求它等于存储的次数），对新集合运行 latched-quorum 检查，并在其 epoch 下校验 proof；它不重新推导节点状态。

### 7.5 保持不变的部分

只要每个 SI indexer 都是自运营的，evidence 格式、三项检查、2-of-3 规则以及 QuorumFailed 处理都保持原样（§12 针对第三方改变最后一项）。

## 8. Member 数据面

### 8.1 si-member 进程

每个 SI indexer 主机为每个 member identity 运行一个 `si-member` 进程，与 SNode 分离，因此它的崩溃或内存占用不会拖垮 SNode。它包含 verifier 角色（replay、byte scan、evidence）和 installer，并以自己的两个用户（§8.2）和 settings profile（`max_memory_usage`、`max_concurrent_queries_for_user`、线程数、优先级）连接主机的 source ClickHouse。它的表位于自己的 database 中——`hg_member_unsafe`（RMT replica，replica name = member node id）、`hg_replica`（copy）和 `hg_rstage`（scratch）——并通过 member storage policy 放在专用磁盘上，因此每个 member hardlink 都留在该磁盘上。Member 磁盘使用一种在独立于 source ClickHouse 磁盘的设备上强制容量限制的 storage class（而不是 `local-path`）；在这样的 storage class 出现之前，每个 SI ClickHouse 的每块磁盘都设置 `keep_free_space_bytes`，使 ClickHouse 在节点 SSD 写满之前就拒绝 fetch 和 insert，并且 P2 在节点 SSD 而非 PVC 上测量余量。主机的后台 fetch pool 按额外的 replica 调整大小，当 member 磁盘越过空闲空间阈值时 install 停止。一个主机只运行一个 member identity。

### 8.2 Member 用户

Member 在其主机上使用两个 ClickHouse 用户，使得位于 owner Keeper 路径上的 replicated 表只能存在于任何 member 用户都无法写入的地方。ClickHouse 允许任何可以建表的用户给表指定任意 Keeper 路径上的八个 `Replicated*MergeTree` engine 中的任意一个，而通过这样一张表进行的每次写入、`DROP PARTITION`、`TRUNCATE` 或 `OPTIMIZE` 都会复制到该路径上的每个 replica，无论该用户对 owner 的 database 拥有什么授权；因此要隔离的是 engine 本身。每个托管 member 的 SI ClickHouse 都把 `access_control_improvements.table_engines_require_grant` 设为 true（一个 config.d 文件；ClickHouse 26.3 只在服务器启动时读取它，因此每个 SI ClickHouse 在 P2 开始时为此重启一次、一次一个，且在某个 ClickHouse 重启之前其上不存在任何 member 用户），此后每个用户只能创建被授予的 engine（`TABLE ENGINE ON <engine>`）。在该次重启之前，每个在该 ClickHouse 上建表或建库的用户（协议用户以及执行客户 DDL 的用户）都按名称恰好获得其 `CREATE` 语句 census 所显示的 engine——若它创建临时表，则包括 `Memory`；ClickHouse 26.3 还把 `CREATE DATABASE … ENGINE = X` 检查为 `TABLE ENGINE ON X`，而 `GRANT` 无法指名它，因此 database DDL 不写 `ENGINE`；没有 `<grants>` 的 users.xml 用户拥有全部 engine，因此每个这样的用户（包括两个 member 用户）都用 `<grants>` 查询定义。

- **`si_member_ddl`** 创建和删除 member 的 replica：`hg_member_unsafe.*` 上的 `CREATE DATABASE`（不写 `ENGINE`）、`CREATE TABLE` 和 `DROP TABLE`、`TABLE ENGINE ON ReplicatedMergeTree`，此外没有任何权限——在任何地方都没有 `INSERT`、`ALTER`、`OPTIMIZE`、`TRUNCATE` 或 `SYSTEM`，在任何其他 database 中也没有 `CREATE TABLE`。只有 member 的 reconciler 使用它。
- **`si_member`** 负责其他一切：`hg_member_unsafe.*` 上的 `SELECT`（作为 `ATTACH PARTITION FROM` 的来源）和那里的 `SYSTEM SYNC REPLICA`，此外对它没有其他权限；`hg_replica.*` 上的 `SELECT`、`INSERT`、`ALTER DELETE`、`CREATE TABLE` 和 `DROP TABLE`（ClickHouse 26.3 把 `ATTACH PARTITION … FROM` 检查为来源上的 `SELECT` 和目标上的 `ALTER DELETE, INSERT`，把 `DROP PART` / `DROP PARTITION` 检查为 `ALTER DELETE`）；`hg_rstage.*` 上的 `SELECT`、`INSERT`、`ALTER`、`CREATE TABLE`、`DROP TABLE`、`TRUNCATE` 和 `OPTIMIZE`（绝不授予 `ALL`：那里没有 dictionary、view、`SYSTEM` 或 definer 权限）；`hg_replica.*` 和 `hg_rstage.*` 上的 `CREATE DATABASE`（不写 `ENGINE`）；`READ ON S3`（§8.6 的 object-store restore 来源）；`TABLE ENGINE ON MergeTree`（copy 和 scratch，均以显式的 `ENGINE = MergeTree` 创建，绝不 `AS` 某张 replicated 表）和 `ON Memory`（其临时表），按名称授予，绝不以 `ON *` 加 revoke 的方式授予，因为八个 `Replicated*MergeTree` engine 各自是独立的名称，而 `Buffer`、`Distributed`、`Merge` 和 `Alias` 会触及其他表；`system.parts`、`system.tables`、`system.detached_tables`、`system.replicas` 和 `system.zookeeper` 上的 `SELECT`；其 collection 上的 `NAMED COLLECTION`；`READ ON REMOTE`；`CREATE TEMPORARY TABLE`。

两个用户都对主机的 `hg_safe`、`hg_unsafe` 或 `hg_promote` 没有任何权限，也没有 `SYSTEM DROP REPLICA`：运行期间 member 用 `DROP TABLE` 移除自己的 replica，而在其状态丢失后残留的陈旧 replica 由 owner 的 sweep 在该 member 被 evict 之后移除（§8.8）。一个 Docker 测试证明拆分所隔离的范围（§17）：`si_member` 在任何地方都不创建 replicated 表，且没有任何 member 用户能对 replicated 表写入、删除 partition、truncate 或 optimize。`si_member_ddl` 仍为其创建或 attach 的每张表选择 Keeper 路径、replica 名和 MergeTree setting，而 ClickHouse 无法按用户限制这些：它可以把一张表 attach 为某个非 active 的 replica（停机的 verifier、正在重启的 owner）再 drop 它——ClickHouse drop 表自身的 replica 时不检查 `is_active`——从而删除该 replica 的 Keeper 状态；可以创建一个允许 merge 的 replica，它分配的 merge 会被每个 replica 执行；也可以在之后某个 incarnation 路径的上层建表，再作为最后一个 replica drop 它，从而删除整棵子树。因此泄露的 `si_member_ddl` 凭据与 pool verifier 的凭据一样，在有按路径的 Keeper ACL（§18）之前是一项残留风险，并通过以下方式收窄：`si_member_ddl` 只从 §10.4 第 6 步起存在，其密码只挂载给 reconciler；每个托管 member 的 SI ClickHouse 把 `<replicated_merge_tree>` 的 `max_bytes_to_merge_at_max_space_in_pool` 默认值设为 0，且 `si_member_ddl` 的 profile 用 `merge_tree_` constraint 把它设为不可改；`/sentio/<shard>/unsafe_inc` 在任何 member 用户存在之前就已创建；owner 的 sweep 对其路径上任何不是 owner、pool verifier 或当前/过去 member 的 replica，以及 SI 根下任何不是 incarnation 路径的 replicated 表根发出告警。Network pool 的 verifier 保留对其自身 `hg_unsafe` replica 的 `ALTER DELETE`（它们是受信任的自运营节点）；因此在 P8 退役该 pool 之前，泄露的 verifier 凭据仍是一项残留风险。

### 8.3 DDL 与 reconciliation

- arbiter-core `ddl` 新增一个 member intent set：`hg_member_unsafe.<db__table>`（位于 incarnation 的 Keeper 路径上的 ReplicatedMergeTree，replica = member node id，原样使用 `ddl.UnsafeSettings`，按 `hg_unsafe` 的方式检查）和 `hg_replica.<db__table>__i<incarnation_seq>`（MergeTree，`ddl.SafeSettings`），没有 promote 表。Member 的 pinned type 恰好只列出和删除这两类表，且从不通过 reconciler 触碰 `hg_rstage` 中的表。
- Member 的 reconciler 接收它所属的 group 集合（来自 registry follower），而不是 stage-1 的 `Owner` 过滤条件；它以 `si_member_ddl` 身份创建和删除 `hg_member_unsafe` replica，其余一切以 `si_member` 身份进行（§8.2）。si-member 从不创建或打开 `hg_safe`、`hg_unsafe` 或 `hg_promote`，且启动时会拒绝其 database 与主机 SNode 的 database 有交集的 member 配置。
- Member 只在其节点已注册且未被 evict 时、并且只在 §8.8 的 purge 规则开启之后，才创建 `hg_member_unsafe` replica；否则每个 reconciler 都会把它的 replica 视为已退役。`shared_rmt = false` 的 member 不创建任何 replica，`verify = false`，并从 owner pull、object store 或 DA 进行 install。
- Copy 是 incarnation-scoped 的，因此被删除后重建的名字永远不会接管旧的 copy。

### 8.4 Manifest feed

Installer 轮询 `GetSafeWatermark`，沿 `ParentSnapshotID` 链前进（从不使用 `GetManifestByBlock`，因为连续的 manifest 可能共享同一个 `safe_block_seq`，C6），并且只在 `track_frontiers[O]` 与其 checkpoint 不同时才获取完整的 manifest。它只接受满足以下条件的 manifest：`SnapshotID` 既等于请求的 id，也等于重新计算出的 `ManifestRoot`；parent 链接成立；每个 partition root 都等于其所列 part row hash 之和。表通过 registry 归属到 group，规则与 `manifestTableOwnerLocked` 所用的相同。Manifest 缓存在一个有界 LRU 中。

### 8.5 Install gate

对于 owner O、目标 manifest S、表 T、incarnation i 和 partition p：
1. M 是 S 为 (T, p) 列出的 `PartRowLtHash` multiset；R 是其 partition root；L 是本地 copy 的 row hash multiset。
2. 如果 L 不是 M 的子 multiset，p 就是 Diverged：该表停止提供服务，p 在一个新的 scratch 中重建，用 `DROP PARTITION` 将其从 copy 中移除，再用 `ATTACH PARTITION FROM` 把重建好的 partition 带回。Copy 上从不使用 `REPLACE PARTITION`。
3. Need = M − L。每个需要的 part 从第一个可用来源（§8.6）fetch 到 `hg_rstage` 中一个名为 `<node_id>_<boot_nonce>_<n>` 的新 scratch 表中，每个来源 part 对应一个 scratch part。
4. 对每个 scratch part 执行 `ScanParts`。Row hash 在 Need 中的 part 保留一次。零行的 scratch 是 rename race，从一个干净的 scratch 重试一次。若干 scratch part 之和等于某个所需 hash，说明发生了 split，回退到下一个来源。行数翻倍的重试 INSERT 被丢弃，并从干净状态重试一次。其他任何情况都是 Foreign：保留 scratch 以供取证，在本地 quarantine 该表并触发告警。
5. 保留的 parts 通过从 scratch 执行 `ATTACH PARTITION p FROM` install，它只追加、不淘汰任何 part，并保留 `hash_of_all_files`。
6. 以算术方式检查 closure，Σ(L) + Σ(kept) = R，且该表的本地 partition 集合必须等于 S 的；完整的 partition scan 只在限速的 self-audit 中运行。
7. Member 按 group 记录 (S, `track_frontiers[O]`) 日志，并可以签一个 checkpoint（§9.1）。

启动时，member 删除所有带其 node-id 前缀的 `hg_rstage` 表。Replay materializer 接收一个 database 和一个前缀，因此 replay 的 scratch 表也位于 `hg_rstage` 中，并使用该 member 的前缀。

### 8.6 字节来源

- **自己的 `hg_member_unsafe`（一旦 member 持有 replica 且 floor 扣住 cleanup，它就是主要来源）。** Verify member 已经持有 owner 的 candidate parts，与它扫描过的字节完全相同。Installer 把所需的 parts 从那里 attach 到 scratch，在 scratch 内删除不需要的 parts，然后从第 4 步继续。
- **Owner pull。** `INSERT INTO hg_rstage.<scratch> SELECT * FROM remote(<owner_collection>, database = 'hg_safe', table = '<table>') WHERE _part = '<name>'`，使用 §2 的 no-split 设置，其中当前的名字通过 `system.parts.hash_of_all_files` = manifest 的 `PartPhysHash` 解析（REPLACE 会保留该 hash）。过滤只作用于 `_part`，从不作用于 manifest 的逻辑 partition id。当 hash 查找结果为空（owner 的 parts 被重建过）时，installer 在同一 partition 中行数相同的 candidate parts 上按 row hash 解析。Owner 的 reader 用户拥有 `hg_safe.*` 和 `hg_unsafe.*` 上的 `SELECT`（后者用于 P2 的 shadow pull，以及在 cleanup 之前 owner 可达时的 unsafe pull），以及 `system.parts` 上的 `SHOW COLUMNS` 和 `SELECT(database, table, name, active, rows, partition_id, hash_of_all_files)`；collection 的 host、port、user 和 password 均为 `NOT OVERRIDABLE`；`si_member` 的 `READ ON REMOTE` 通过 `system.grants`（有 READ、无 WRITE）以及一个探测（`INSERT INTO FUNCTION remote(...)` 被拒绝）来验证。
- **Peer unsafe pull。** 当 owner 的 ClickHouse 在 cleanup 之前不可达时，可以改由网络 verifier 的 `hg_unsafe` 或另一个 member 的 `hg_member_unsafe` 做同样的 pull，通过该 ClickHouse 上一个对其 unsafe database 拥有 `SELECT` 并具备相同 `system.parts` 授权的 reader 用户进行。
- **Object-store restore。** Bootstrap、大缺口和重建会用 `RESTORE TABLE ... AS hg_rstage.<scratch> PARTITIONS ...` 恢复 owner 的已校验备份（P1），然后从第 4 步继续，只保留所需的 parts。
- **DA rematerialisation**（P8）：pinned executor 用真实 DDL 把 statement materialise 到 scratch 中，每个 (statement, partition) 一条 INSERT；当没有任何 ClickHouse copy 存活时，它是唯一的来源。

### 8.7 启动审计

在 member 签 checkpoint 或为某个 database 提供服务之前，它把每份 copy 作为 multiset 审计：row hash 和 `hash_of_all_files` 都相同的孪生 part 只保留一个，其余用本地 `DROP PART` 删除（它会写一个覆盖 part），任何剩余的不匹配都是 Diverged。当 ClickHouse uptime 倒退时，审计会重新运行。

### 8.8 Purge

- 从 `purge` 开关（§10.4）起，开关之后记录的每个链上 incarnation 都使用 incarnation-scoped 的 Keeper 路径 `/sentio/<shard>/unsafe_inc/<db__table>/<incarnation_seq>`。任何 scoped 路径都不会是 legacy `/sentio/<shard>/unsafe/<db__table>` 路径的后代，因为当一张表的最后一个 replica 被删除时，ClickHouse 会移除该表路径下的所有子节点。Registry view 为每个 incarnation 标记 `scoped_keeper_path`，使每个 reconciler 推导出相同的路径；`ddl.ZooKeeperPath`、replica 列表和 sweep 覆盖两个根。因此，重建的名字永远不会加入更早 incarnation 的 replica，也不会因其丢失元数据；下面的 purge 集合在 scoped 根上只起垃圾回收的作用。
- 对于位于未 scope 路径上的 incarnation，purge-required 集合是 owner 的 SNode、每个未被 evict 的 network-pool verifier（pool 持有每张 SI 表的一个 replica），以及自该 incarnation 被记录以来在任一 generation 曾是其 group member 的每个未被 evict 的节点，无论其标志或 class 如何。这样的节点只有通过 `EvictNode` 才会离开该集合，之后 owner 的 sweep 和 `clearDecommissioned` 会在名字可被复用之前删除其 Keeper replica。Member 对它从未持有过的任何 Purging incarnation 立即报告 purge。
- 对于位于 scoped 路径上的 incarnation，purge-required 集合相同。
- Member 从 Retiring 起停止服务并删除 copy。它只在 Purging 时、在其 quiescence hook 确认其 verify set 中没有未解决的 block 引用该表之后，才删除其 `hg_member_unsafe` replica 并报告。
- Copy 从不延迟 purge：它们是 incarnation-scoped 的，且不在任何 Keeper 路径上。

### 8.9 Mutation pre-state

当 bounded UPDATE/DELETE 落地时，mutation block 的 verify set 只从其 copy 等于该 block 的 `PrevSafeSnapshotID` manifest 的 replicate member 中抽取；07-01 的 canonical publication cut 随该设计一起决定。

## 9. Copy floor 与 checkpoint

### 9.1 Checkpoint

Replicate member 用其 `replica_signer` 签名 `MemberCheckpoint{node_id, owner, checkpoint_seq, groups: [{database, group_generation, excluded}], snapshot_id, entry}`，其中 `checkpoint_seq` 按 (member, owner) 严格递增，`entry` 是 `snapshot_id` 的 `track_frontiers[O]`，`excluded` 列出它尚未 install 的表及原因。Apply 接受它的条件是：签名者是每个所列 group 的未被 evict 的当前 member，其 `checkpoint_seq` 高于该 member 针对 O 存储的值（因此任何已签名的 checkpoint 都无法被 replay，无论在同一 entry 还是其他 entry），且所针对的 manifest 位于 watermark 链上、其 `track_frontiers[O]` 等于 `entry`，并且在链上不早于该 member 的上一个 checkpoint。随后 Apply 只对每个所列 group，用这个 checkpoint 及其签名和 epoch 替换该 group 存储的 checkpoint；未列出的 group 保持原样。因此同一 entry 上更晚的 checkpoint 既可以缩小也可以扩大某个 group 的 excluded 集合：扩大只会降低 floor 计入的 copy 数，而缩小是该 member 最新的签名声明，任何更旧的声明都无法覆盖它。Apply 按 (member, owner) 持久保存已接受的最大 `checkpoint_seq`——跨越 group 被移除、member 离开 O 的所有 group 以及重新注册——并在该 member 的 `replica_signer` 轮换时重新起算，因为旧密钥签名的 checkpoint 不再能通过校验；registry view 发布该值，member 以高于其 journal 和所发布值两者的序号签下一个 checkpoint，因此丢失本地状态的 member 能够继续，而不是被永久拒绝。Restore 要求存储的值不小于该 member 和 owner 在当前密钥下每个已存储 checkpoint 的 `checkpoint_seq`。Restore 在该 epoch 的 `replica_signer` 下重新校验每个签名，且不重新检查节点状态。`MemberCheckpoint`、`MemberReady`、`MarkMemberLagging`、`ReselectVerifiers` 和 `RebalanceGroup` 是 `ConsensusParamsUpdate` 之外的命令。

### 9.2 Member class

Class 按 (member, group) 存储：Bootstrapping（自 member 加入该 group 以来没有被接受的 checkpoint；在更早的、该节点当时已经是 member 的 generation 上做的 checkpoint 会保留其 class，而同 entry 的重新 checkpoint（同一 entry 上更高的 `checkpoint_seq`，§9.1）刷新存储的 generation 和 excluded 集合，对 class 的唯一改变是结束 Bootstrapping）、CaughtUp（checkpoint entry 等于 O 最新发布的 entry）、Behind、Lagging 和 Leaving。当 `entry.safe_block_seq ≥ maxBlock(P)` 且 `entry.applied_promotion_seq ≥ P.seq` 时，称一个 entry *反映*（*reflects*）promotion P。Lag 以 O 的已发布 advance 计数，而不是以 promotion 计数：当 member 落后超过 `max_lag_publications` 个 advance 时，它变为 Lagging，并把它当时所落后于的那个 entry 记录为其 observed entry；或者通过由 authority 签名的 `MarkMemberLagging{node_id, owner, database, observed_entry}` 变为 Lagging，该命令由 orchestrator 在超时后提议，并由 Apply 依据已提交状态重新检查。只有当其 checkpoint 前进越过 observed entry 且其 lag 低于 `lag_low` 时，它才会回到 Behind。被 quarantine 的 owner 不累积 lag。Restore 要求每个存储的 class 都与存储的 checkpoint 和已发布的 entry 一致。

### 9.3 Floor

Owner O 的 promotion P 的 cleanup，在 Apply（`applyScheduleUnsafeCleanup`）和 WorkSet 中同样要求：
- **base(P)：** P 已被 acknowledge 为 Applied；最新发布的 manifest S 满足 `maxBlock(P) ≤ S.track_frontiers[O].safe_block_seq` 且 `P.seq ≤ S.track_frontiers[O].applied_promotion_seq`；O 未被 quarantine。Quarantine 条款在 Apply 中已经存在；发布相关的条款是 Apply 中新增的，位于 `checkpoints` 开关之后；cutoff 条款由 block 条款蕴含，作为一项检查保留。
- **并且满足以下之一：**
  - P 的 incarnation 不是 Active（purge 豁免，使 arbiter-core#49 的 purge hold 永不等待 member）；
  - partition 或表超过 `max_uncleaned_parts_per_partition` / `max_uncleaned_parts_per_table`（pressure bypass；strict mode 下由下文的 admission backpressure 取代）；
  - E 中 checkpoint 反映 P、且其 groups 不排除 P 所在表的 member 覆盖至少 min(k − 1, |E|) 个不同的 failure domain；其中 E 是 P 所属 group 中已列出、未被 evict、counting、class 为 CaughtUp 或 Behind、且 failure domain 不同于 O 的 replicate member 集合，|E| 是其中不同 failure domain 的个数。Member 的 failure domain 是其 entry 的 `failure_domain`；O 的是 `indexer_failure_domains[O]`。按 domain 计数而不是挑选 member，使该条款与顺序无关：同一 domain 中的两个 member 只计一次，无论其中哪一个满足条件，因此每个 voter 算出的结果相同。把一个 group 的两个 member、或某个 member 与其 owner 放进同一 domain 的 `ConsensusParamsUpdate` 会被接受，只会降低计数（引发 `under_replicated`）；`RebalanceGroup` 负责修复。Eviction 或除名会在同一次 Apply 中把 member 从 E 中移除。

每个 escape，以及 |E| 降到 k − 1 以下，都会引发一个 `under_replicated` 事件。Rejected-part cleanup（promotion seq 0，C4）从不被 gate。在 `checkpoints` 开关之前调度的 cleanup 不会被重新检查。

**Strict mode**（`strict_copies`）：计数条款要求 k − 1 个不同的 reflecting domain，而不是 min(k − 1, |E|)；Lagging member 留在 E 中；pressure bypass 变为 backpressure——对于 cleanup 被 floor 扣住且超出 uncleaned-parts 上限的表，owner 针对该表的 statement 会收到可重试的拒绝，直到 floor 满足——因此以该表的写 liveness 为代价保住 durability。Purge 豁免仍然有效。

**提高 k** 到某个值，要求对每个 Active incarnation，都有覆盖 k − 1 个不同于 O 的 failure domain 的 counting member，其最新 checkpoint 等于 O 最新发布的 entry 且不排除任何表。

**故障独立性。** FSM 统计的是声明的 failure domain（`failure_domain`、`indexer_failure_domains`）；声明的 domain 只有通过 anti-affinity 才与部署相符，anti-affinity 由 P4 建立、由 §10.4 第 10 步要求，并且 `safe_copies` 按不同的 Kubernetes 节点报告。

## 10. Drain gate 与 activation

### 10.1 Drain gate

- 当一个 update 改变 track T 的 `si_indexers` 条目时，它就触及 T，因为此后 T 的 acknowledgement 将在另一个 signer 下校验。对 `verifiers` 和 `stage3` 的变更不触及任何 track。Authority 集合的变更不触及任何 track，但要求每个 SNode 的 allowlist 先列出新的 authority（C12）。
- 在 `narrowed_drain` 开关之后，drain gate 只因其触及的 track 的 in-flight 工作而拒绝 update。仅因 copy floor 而被 Apply 扣住 cleanup 的已 promote unsafe part 不算 in-flight 工作；未被 acknowledge 的 promotion、已调度的 cleanup（无论是否已派发）、rejected-part cleanup（C4），以及等待 copy floor 以外任何事物的已 promote unsafe part 仍然算。
- Restore 不需要新规则：在 tracks activation 之后，它已经接受更早 epoch 的未 acknowledge promotion 和待处理 cleanup，并在各自的 epoch 下校验其 authority proof（C14），且没有任何 restore validator 读取 `PromotedUnsafe`。
- 每个 update 仍绑定到一个精确的 `expected_promotion_seq`。签名之前，`arbiter-admin` 向 leader 请求一个 issuance hold：leader 停止在每个 track 上提议 `RecordPromotionIssued`，同时 acknowledgement 和 cleanup 继续进行，返回该 update 用于签名的冻结 `PromotionSeq`，并在 update 提交或被拒绝时、在一个有界超时之后，或在 leadership 变更时解除 hold。

### 10.2 Leader drain mode

对于仍需要已 drain 的 track 的 update（P0 起，以及 `narrowed_drain` 开关之前的每个 update），leader 记录每个受影响 track 的 promotion frontier block，继续为不高于它的 block 发出 promotion、重新发出和 cleanup，只拒绝更晚 block 的第一个 promotion，直到 update 提交、经过一个有界超时，或 leadership 变更。它报告仍处于 in-flight 的 block。该超时至少为 copy floor 未扣住的工作的 promotion → acknowledgement → publication → cleanup acknowledgement 的实测 p99。有 in-flight 工作的卡住 track 无法 drain；runbook 会先 quarantine 它，而其 track 有 in-flight 工作的 SNode 会在被 evict 之前先被 quarantine。

### 10.3 Activation

v21 activation 是一次由 authority 签名、设置 `stage3` 且所有开关关闭的 update。它需要满足 stage-2 drain gate，借助 §10.2 达成（有 in-flight 工作的卡住 track 先被 quarantine）；它不需要空的 open block，也不需要在 sealed tip 处具备全局 prefix。它还需要 leader 在精确的 raft voter 集合上完成 voter probe（C15），以及每个已注册、未被 evict、不在被 quarantine 的 track 上的数据面节点都通告 `stage3_v1`。Snapshot v21 在 `stage3` 提交之后写出，旧版本二进制会拒绝它。

v21 容器要么持有 interned body（§13.2），要么持有 plain body（按 v20 布局的完整状态）；每个 v21 二进制都能读取两者。在 `Persist` 中、FSM 锁之外，writer 编码 interned body，再解码并 rehydrate，然后把 canonical digest 与在 `Snapshot()` 中于读锁下捕获的 digest 比较；不匹配时它持久化 plain body，递增 `arbiter_snapshot_encoding_fallback_total` 并发出 page。它从不取消 snapshot。一个位于单向开关之外的运维 writer 设置可以强制使用 plain body，之后也可以再关闭。Activation 会在一个由线上状态副本恢复出来的一次性三 voter 集群上演练，包括 voter 从 v21 snapshot 重启以及一次强制不匹配的运行。

### 10.4 开关与顺序

开关是 `stage3` 的单向字段；每个开关都要求此列表中所有更早的开关，若前置开关处于关闭状态，Apply 拒绝该开关的开启。从 activation 起，带 `host_indexer_id` 的条目在第 6 步之前不进入任何 eligible ring。顺序如下，每个开关步骤都是一次需要明确确认的 authority 签名 update：
1. `purge`（§8.8）：新 incarnation 的 incarnation-scoped Keeper 路径，以及感知 member 的 purge-required 集合。在任何 member 注册之前开启。
2. `eviction_voiding`（§14）：在任何 member 可被选中之前开启。
3. `owner_aware_selection`（§7.3）：没有 group 的 database 获得只含 pool 的 ring，且 `host_indexer_id` 为该 block owner 的条目被排除。
4. *非开关步骤：* 带 consent 和 failure domain 的 member 条目被列入 `verifiers`；si-member 节点注册并 mark active；它们暂不进入任何 ring。
5. `placement`（§6.3，§7.1），它同时开启 seal key：开启它的 update 放置每个现存的 database，seal open block，并记录 cutover block。
6. *非开关步骤：* member 创建其 `hg_member_unsafe` replica 并发送 `MemberReady`；此后它们进入 eligible ring。
7. `reselection`（§7.4）。
8. `checkpoints`（§9.1–§9.3）：checkpoint、member class 以及 floor 的 Apply 条款。
9. `narrowed_drain`（§10.1）。
10. *参数：* 在满足其前置条件、P4 的加固（anti-affinity 已验证）以及 §16 的 Keeper 容量检查之后，提高 k（§9.3）。

## 11. 从 member 读取

Member 主机 H 仅当以下条件全部成立时，才从其 copy 提供 database d 的 SI 表：
- O 被 quarantine，或者 O 的 HouseGate 已持续 `t_unreach` 不可达（si-member 主机配置，由 si-member 自己对 O 的 HouseGate 的探测来测量）；
- H 上 d 的每张 SI 表都等于在同一轮轮询中从 arbiter 读取的最新已发布 manifest 的 `track_frontiers[O]`，且该 entry 在两轮轮询中保持不变（这适用于两种触发条件，因为被 quarantine 的 owner 的 entry 仍可能因迟到的 acknowledgement 而前进）；
- 启动审计和 self-audit 都是干净的，且 d 中没有任何表处于 Foreign、Diverged 或被排除状态；
- peer-trusted guard（§14）和 ClickHouse lockdown 已生效；
- 主机标志处于开启状态。

在 d 被服务期间，d 的 install 暂停。当 `track_frontiers[O]` 变化时，H 停止服务 d，执行 install，只有在新 entry 满足这些条件之后才重新服务。服务是 member 按 database 授予主机的一个 lease；在任何 install 之前，member 撤销该 lease，并等待主机释放它或它过期。

- **主机状态。** si-member 按 database 暴露一个经认证的本地状态端点（entry、审计状态、serving lease）。sentio-node 的 table state 轮询它，并以新的只读状态 `Replica` 而非 Ordinary 回答这些表；端点不可达时 fail closed（不提供服务）。`sitable.Table` 新增 copy 的物理表名；`Snapshot` 新增 `Served()`；`sitablestate` 为 `Replica` 增加显式的行（其 `decide` 对未知状态会 fall through 到 allow）；写和 DDL 以 392 拒绝，并在错误中指明 owner。Status RPC 只新增 additive 字段（`replica_of`、`served_from`、`owner_entry`、`frozen_since`，以及按 database 的 `replica_hosts`：在 placement 开启之前，它是每个主机的 status RPC 所通告的 P2 静态 group 配置（其他每个 SI indexer）；从 placement 开启起，它是 registry 的 `DatabaseGroup` 加上每个主机通告的 readiness）。
- **路由。** `forward.Plugin` 接收一个 table-state port，把 hello database 或 `USE` 目标为 replica-served 的 session 保持在本地，而不是把它们 pivot 到 owner；当某个 database 不再由 replica 服务时，这类 session 上的查询会收到一个要求客户端重连的可重试拒绝。该 database 保留其 remote mapping，因此其普通表仍发往 O。`buildStorageIntegrityArgs` 为 Replica 表添加 `SafeTable` = `UnsafeTable` = copy（两个 engine 都拒绝空的 unsafe 表），并且 housegate 在 engine 运行之前就拒绝它们的 `unsafe_latest`；两个 engine 都在 remote mapping 之前解析 SI 表。O 宕机期间，d 的普通表不提供服务。
- **读模式。** Safe 读从 copy 回答。`unsafe_latest`（devnet2 的默认值）会被拒绝，并在消息中指明使用 safe mode。若要在 O 被 quarantine 期间从已发布状态回答它，需要在 rewriter-proto 和两个 engine 中引入按表的读模式、修订 Spec G D2，以及一个在 devnet2 以外关闭的主机标志。
- **Agent。** 当 owner 以 392 `indexer_quarantined` 应答或不可达时，agent 把读（从不把写）failover 到 `replica_hosts` 中的主机。在 devnet2 上，这需要在一个计划窗口内升级每个 SI 主机的 agent sidecar，这一升级也会带来 deferred-lane 行为，而 stage-1 runbook 的 sidecar 规则要求该行为单独计划。
- **由 rejection 引起的 quarantine。** Copy 从不持有被 reject 的行（S8），因此 member 可以在由被 reject 的 block 引起的 quarantine 期间提供服务；固定这一点的 P2 集成测试是 P3 的准入条件。只要没有已 promote 的 statement 可以被 challenge（§2，情况 (a)），这一点就成立；v22 revert 必须在任何 challenger 能够打开 challenge 之前移除这样的行（§14）。

## 12. 独立性与第三方 member

只要每个 SI indexer 都是自运营的，peer member 就与网络 verifier 一样可信，既校验也计数。引入第三方之后，两个串通的 peer 可以 reject 竞争者的诚实 block——QuorumFailed 会被立即裁定为 Rejected 并 quarantine 该 track——而两个 peer 加上 source 可以放过坏数据，且 v1 没有 challenge window。第三方的 member 只有在以下各项全部具备之后才可以校验或计数：带 Sybil 抗性的 `operator_id` 绑定；QuorumFailed 交由一个新的 verify set 裁决，而不是立即 reject；一个带独立 challenger 和可达 ChallengeReplay→Safe 路径的 challenge window；每个 group 至少有一个网络运营的 member 或 challenger；为 `shared_rmt = false` 的 member 提供的 per-operator 字节传输，其拉取的字节也为 check 3 提供输入，使这类 member 可以校验；以及 umbrella §17 的 gate。在 `operator_id` 存在之前，列出第三方的 member 会被拒绝。

## 13. 共识状态增长

### 13.1 防护（P0，无共识变更）

- Gauge：snapshot doc 大小和内存中状态大小（以未 compact 的 canonical 编码长度计，按组件统计），以及 restore 和 snapshot-install 时间。
- 令 L = min(read cap, voter memory / 4, voter PVC / 4)——今天为 512 MiB——在 L/4（128 MiB）时 warn，在 L/2（256 MiB）时 page，并在 3L/4（384 MiB）之前采取行动。内存告警使用内存中状态的 gauge；PVC 告警使用 doc 加 trailing log。
- 临时措施是一个 production PR，把 voter 内存和 PVC 调整到至少为实测状态加 trailing log 的四倍；只有当 voter 超过 4 GiB 时，提高 1 GiB 的 read cap 才有意义。
- Writer 从不因大小而拒绝 snapshot；它只告警。
- devnet2 演练分配 promotion 预算；churn 演练在集成环境中或在一张分区的一次性表上运行。

### 13.2 无损编码（v21）

Interned body 维护一张由不同的 LtHash accumulator 和不同的 part 三元组（row hash、name、physical hash）构成的表，并在 manifest、acknowledgement 记录、`SafeParts`、statement 和 verification 中通过索引引用它们；未变化的 partition 条目就是一个引用。Restore 逐字节精确地 rehydrate 状态，因此每个现有的 validator，包括对完整 acknowledgement body 的 C17 重新校验，都无需改动地运行。在实测的 doc 上估算：从 11.4 MB 降到约 1.4 MB。剩余的超线性项来自每次 `REPLACE PARTITION` 都会重命名的 parts；history pruning（v22）和已校验的 `hg_safe` merge（06-22 §12.4）为其设定上界。

## 14. 先行修复的隐患

- **Peer-trusted guard（P0）。** `ReservedDatabases` 新增 `hg_member_unsafe`、`hg_replica` 和 `hg_rstage`（并预留 `hg_xfer`）。在 peer-trusted、非 forwarded 的 session 上，HouseGate 拒绝对 reserved database 或 reserved row-id 列的任何提及，包括字符串字面量（`sireserved` 的 namespace 规则），并针对每种 rewriter peer-leg 形式做回归测试。
- **Owner `hg_safe` 审计（P0）。** 在构建每个 promotion 的 shadow 之前，以及每当 ClickHouse uptime 倒退时，owner SNode 都把其 partition 与"其自己上次 acknowledge 的 inventory"和"FSM 存储的 acknowledgement"二者中较新的那个进行比较。在 hash 相同的孪生 part 中，它保留名字在该 inventory 中的 part，其余用本地 `DROP PART` 删除；如果没有任何孪生 part 带有已 acknowledge 的名字，或者 FSM 领先，审计就是不干净的，SNode 不会继续。审计在 promotion-intent 收敛之后运行，并先以 report-only 方式上线；审计不干净期间，主机以 Pending（733，可重试）回答 owner 的 SI 表。
- **ClickHouse lockdown（P1 之前）。** 先为每个协议客户端（SNode DSN、verifier 和 arbiter 的启动路径、si-member）上线 ClickHouse 凭据；然后一次一个 CHI、每次都附带不会触发任何重启的 staging 证明，在每个 SI ClickHouse 上为 `default` 用户设置密码和网络限制；reader 用户恰好获得 §8.6 的授权（§8.2 的 member 用户只在其 ClickHouse 完成 P2 重启之后才创建）；named collection 设为 `NOT OVERRIDABLE` 并用 `GRANT NAMED COLLECTION` 加以限制；修订 devnet2 posture 注释（production#735），允许来自 peer source ClickHouse 的 9000 端口 member pull 和 9009 端口 member fetch。
- **网络加固（P4，在提高 k 之前）。** 分两步滚动引入 interserver 凭据（先在所有地方使用 `allow_empty: true` 的凭据，再改为 `allow_empty: false`）；只限 ingress 或枚举每个 egress（DNS、Keeper、inter-ClickHouse、object store）的 NetworkPolicy，事先从每个 pod 探测；共享 Keeper ensemble 上的 ingress allowlist；anti-affinity 或节点固定，使每个 SI source CHI 和 verifier 落在不同的节点上。把 SI 迁到专用的 Keeper ensemble 是一项单独的设计（在暂停每个 track 的情况下对每个 SI ClickHouse 做一次单窗口 cutover），不是本阶段的前提。
- **Eviction voiding（v21）。** 来自 Evicted 节点的 result claim、verifier attestation 和 byte-side scan，promotion、cleanup 和 rejected-cleanup acknowledgement，`MemberReady` 以及 checkpoint 都会被拒绝。Evicted verifier 在被 evict 之前记录的 evidence 会退出该 block 的裁决，除非 quorum 已经 latch 或该 block 的某条 statement 已被 challenge。Restore 不对存储的记录重新应用该检查。注册和 activation 保持现有检查。Voiding 只是把节点 park 住，并不是密钥 fence：带更高 `registration_seq` 的重新注册会恢复它。
- **Release refusal 的出口（不在 v21 中）。** 针对丢失 acknowledgement 的 settle 请求是一个非共识的 orchestrator 和 SNode 功能（P4），因为 FSM 已经在 quarantine 下接受迟到的 acknowledgement。对已 promote 的 rejected parts 的 subtractive revert 需要单独的设计和 v22，并且必须在任何独立 challenger 能够打开 challenge 之前生效。

## 15. 按仓库划分的变更

- **arbiter-proto：** `VerifierEntry` 的主机字段、标志和 consent；`stage3` 参数；registry view 中的 `DatabaseGroup`；header `group_key`；命令 `RebalanceGroup`、`ReselectVerifiers`、`MemberReady`、`MemberCheckpoint`、`MarkMemberLagging`；admission 码 `VERIFIERS_UNAVAILABLE`；读 RPC `GetPartitionStates`、`GetPromotionOutcomes` 和 `GetBlockVerification(block_seq)`，以及 leader issuance-hold RPC；feature `stage3_v1`；状态字段。
- **arbiter-core：** `si-member` 角色（verifier 加 installer），包括 member DDL intent set 和 group-scoped reconciler、incarnation-scoped Keeper 路径、install gate 和字节来源、checkpoint 和 readiness 签名、本地状态端点、带前缀的 replay scratch；owner 审计；wire converter；v21 容器 codec（如果它位于共享代码中）；每个 DSN 中的 ClickHouse 凭据。
- **arbiter：** placement 和 backfill、group 记录、seal key 和分组窗口、感知 owner 的选择和 admission 拒绝、重新选择、member readiness、checkpoint 和 class、Apply 与 WorkSet 中的 floor、收窄的 drain gate 和 issuance hold、eviction voiding、purge 集合、带两种 body 和 write-then-verify 的 snapshot v21、leader drain mode、读 RPC（`GetPartitionStates`、`GetPromotionOutcomes`、`GetBlockVerification(block_seq)`，后者返回 replay job、每个 statement 的 candidate parts、verify set 和每个 evidence 结果，用于 shadow verification）、指标、`arbiter-admin`；启动路径中的 ClickHouse 凭据。
- **housegate：** peer-trusted guard 和 reserved database；`sitable` 的 `Replica` 状态、copy 的表名和 `Served()`；`sitablestate` 的行；带 table-state port 的 `forward.Plugin`；面向 Replica 表的 `buildStorageIntegrityArgs`；`pkg/replay` 辅助函数（`OwnerEntry`、partition scan、一个导出的 ledger check）；接收 database 和前缀的 materializer；agent 读 failover 和状态字段。
- **rewriter-proto、rewriter-go、rewriter（可选）：** 按表的读模式，仅当要从 copy 回答 `unsafe_latest` 时才需要。
- **sentio-node：** 在 SNode 旁运行 `si-member`，使用其自己的 identity、密钥、两个 ClickHouse 用户（§8.2）和磁盘；来自 member 端点的 `Replica` 状态；consent、readiness 和 checkpoint 密钥处理；SI ClickHouse DSN 的凭据；pin。
- **production：** voter 规格；面向 track、member、floor、copy、磁盘、snapshot 大小和编码 fallback 的告警规则（`charts/storage-integrity` 中的 `VMRule` / `VMServiceScrape`）；object-store bucket 和备份 job；每个 indexer overlay 中的 member 进程、两个 member 用户（`si_member_ddl`、`si_member`）以及带显式 engine 授权的 `table_engines_require_grant`、collection 和磁盘；lockdown、NetworkPolicy 和 interserver 凭据；anti-affinity；indexer-c；包含 §16 operator playbook 的 runbook。

## 16. 阶段与 devnet2 rollout

| 阶段 | 内容 | 共识 | 暂停的 track | 退出条件 |
|---|---|---|---|---|
| P0 | §13.1 的防护和 voter 规格调整；peer-trusted guard；owner 审计（先 report-only，再修复）；在 CI 中于已部署镜像上固定 ClickHouse 机制；读 RPC；issuance hold 和 leader drain mode；告警规则 | 无 | 每次 SNode 重启暂停一个 track | 防护和告警已上线；两个 owner 上审计均干净；机制在已部署镜像上为绿色 |
| L | ClickHouse lockdown（§14），凭据先行 | 无 | 网络 CHI 上的 per-CHI 变更会让每个 track 的校验暂停数秒 | 每个 SI ClickHouse 上的 `default` 均已锁定；授权已探测 |
| E | Enrol indexer-c（见下文） | 参数 update；不可逆 | track a 和 b 通过 P0 drain mode 完成 drain | 一条写入分配给 indexer-c 的 database 的签名 INSERT 到达 safe |
| P1 | 每个 owner 的已校验 object-store 备份（versioned GCS bucket；备份密钥可在其自己的前缀上 list、read 和 create，但不能 delete；restore 密钥只读）以及 restore-and-gate 库 | 无 | 无 | 每个 owner 完成一次 restore 演练，并对照 manifest 校验通过 |
| P2 | 在每个主机上为其他 owner 运行 si-member，不接入共识、不建 Keeper replica：copy 只来自 owner pull 和 restore；shadow verification 从 DA replay，并在把每个 candidate part 从 owner 的 `hg_unsafe` pull 到 `hg_rstage` 之后扫描它；静态 group 配置 = 其他每个 indexer；实测磁盘余量 | 无 | 每个 SI ClickHouse 重启一次、一次一个（§8.2）：每个 owner 的 track 在其 source 重启时暂停，网络 CHI 的重启会暂停每个 track 的校验 | 在集成环境（`chpipeline`）中，每个 owner 至少 500 个 block 上零 shadow mismatch（错过的 scan 单独计数）；在 devnet2 上，在 §13.1 promotion 预算的一半以内，soak 看到的每个 block 零 shadow mismatch（共识状态大约随 promotion 数二次增长，因此 devnet2 无法承载每个 owner 500 个 block）；copy 连续 24 h 等于最新 entry；member 磁盘低于 50 %；"copy 从不持有 Rejected statement 的行"这一集成测试通过；在每个 SI ClickHouse 上以 `si_member` 身份执行 `CREATE TABLE … ENGINE = ReplicatedMergeTree` 被拒绝 |
| P3 | 从 member 读取（§11），在一个窗口内升级 agent sidecar | 无 | 无 | 被 quarantine 的 owner 的 database 的客户端通过升级后的 agent 从 member 读到 safe 数据 |
| P4 | 针对丢失 acknowledgement 的 settle；同 identity 的 owner 重建（主机保留其 signer key——它存放在 Secret 中，volume 丢失也不受影响——加 Keeper fence：旧 pod 和 PV 被销毁或隔离、`SYSTEM DROP REPLICA` 成功、检查 `is_active`）；网络加固（§14）。不演练 signer 轮换：SI claim signer 就是该 indexer 的链上注册 key，新 key 会注册出一个新的 indexer id | 无 | 被重建的 track | 记录重建 RTO；从每个 pod 探测加固效果 |
| P5 | 所有开关关闭的 v21 activation（§10.3） | v21，不可逆 | activation 时 drain 每个 track | 每个 voter 都写 v21；演练通过 |
| P6 | §10.4 的顺序；在第 6 步之前，`clickhouse-keeper-extra` 在预计的 replica 数（每张表的 owner、pool 和 member）下实测的 znode 数、内存和请求延迟相对其上限留有余量；然后把 k 提高到 2 | 参数 update | 除每次 update 自身的 drain 外无 | k = 2 持续 7 天且 held-cleanup age 有界；anti-affinity 重新检查；不同节点上 `safe_copies` ≥ 2 |
| P7 | v22：使用冷 governance 密钥的 owner rebase 与 takeover、history pruning、O(1) acknowledgement 记录、情况 (a) 的 revert | v22 | 按设计 | 按设计 |
| P8 | DA rematerialisation；per-operator transport；一旦每个 group 在不同主机上有三个 peer verify member，就按 group 退役 network pool | 按需 | 无 | 按设计 |

**Enrol indexer-c（E）。** Enrolment 是不可逆的：`si_indexers` 只追加（`arbiter:fsm/signed_claims.go:131`）。步骤：一个在 L2 上注资的新 signer 密钥；其 source CHI，采用 umbrella §8 的 source profile（按 §20 修订）以及 §8.1 的 member 磁盘；其 stage-2 sentio-node，使用 SNode `snode-3`（stage-1 runbook 台账中的下一个 id）、`tableIDs: []` 和空集合 schema root，并且仅在 enrolment 前一刻才启动，因为它一启动就会在链上注册自己（`EnsureRegistered`），并把自己加入 `createUserDatabase` 对活跃 indexer 的 round-robin；记录合约分配的 indexer id；enrolment update，`max_writers` 为 3，activation block 约在 safe head 之后 1800 个 L2 block，通过 drain mode 覆盖 track a 和 b；activation 时的 seed。Indexer-c 不需要拥有自己的 database 也能作为 member 主机。验收：不断创建 database，直到有一个被分配给 indexer-c，在 activation 之后在其中创建一张表，并把一条签名 INSERT 推进到 safe。

**P6 之后 devnet2 的稳态。** Owner a 的 member 为 {b, c}，owner b 的为 {a, c}，owner c 的为 {a, b}；每个 block 从五个 eligible verifier 中抽取三个；每张表在稳态下有三份 copy，在 k = 2 下，除 §9.3 中会触发告警的 escape 之外，在不同节点上至少有两份（strict mode 去掉 pressure escape，并要求 reflecting member 覆盖 k − 1 个不同的 failure domain）。一个 indexer 宕机只会停住它自己的 track，并使另外两个 owner 各少一个 verifier 的余量。

**Operator playbook。** Member 宕机：等待或发出 `MarkMemberLagging`，然后执行 `EvictNode` 或 `RebalanceGroup`。Member lagging：从 object store 恢复。Diverged：自动重建，重复发生时 page。Foreign：停止服务，保留 scratch，从 owner pull 或 restore 重建，调查传输通道。从 P6 起，严格一次只重启一个 indexer，因为每次重启都会从另外两个 owner 的 ring 中移除一个 verifier。每个线上步骤都需要在 chat 中明确确认。

## 17. 测试

- **FSM：** 带权重、salt、failure domain 和主机唯一性的 placement 确定性，以及审计者的重新推导；开关开启时的 placement backfill；seal key，包括 key 相同的 database 共享 block，以及改变 key 的参数 update；分组窗口；排除 owner 托管 verifier 的选择和自动 fill；`VERIFIERS_UNAVAILABLE`；安静 track 上的重新选择及其 restore 检查，以及两次 pass 的 latch 之后 evict 一个通过的 verifier、被拒绝的重新选择和一次 snapshot/restore；`MemberReady`；checkpoint 接受（拒绝被 replay 或更低的 `checkpoint_seq`，同 entry 的 checkpoint 只替换其所列 group）和 restore 重新校验；member class 及其滞回；floor、每个 escape 和 strict mode，包括同一 failure domain 中的两个 member 只计一次，以及制造这种冲突的参数 update；strict mode 下当唯一已追上的 member 被 Bootstrapping member 替换时 Leaving member 的释放；提高 k 的前置条件；收窄的 drain gate 和 issuance hold；带多 partition frontier block 以及带 `Applied:false` 重新发出的 drain mode；带被 quarantine track 的 activation；v21 的 interned 和 plain body、强制 fallback，以及对 devnet2 log 的离线 digest replay；包括 evidence 在内的 eviction voiding；scoped 和未 scope 路径上的 purge 集合。
- **数据面：** install gate（子 multiset、closure、在 outdated 窗口内重新加载的 Diverged 修复、Foreign、split、rename race、翻倍重试）；从 `hg_member_unsafe` 的 hardlink install；拒绝写入的 pull 授权；restore 来源；启动审计；member purge，包括失效 member 之后接 purge 并在两种路径上同名重建，以及在 scoped 重建之后删除未 scope 路径上陈旧的最后一个 replica、同时保持 scoped 表的 Keeper 树和写入完好；member 用户的隔离：以 `si_member` 身份，在 `hg_rstage` 和 `hg_replica` 中创建八个 `Replicated*MergeTree` engine 中的任意一个以及 `Buffer`/`Distributed`/`Merge`/`Alias`、`CREATE DATABASE … ENGINE = Replicated`，以及向 `hg_member_unsafe` 执行 `ATTACH PARTITION FROM`/`MOVE PARTITION TO TABLE`/建立 materialized view 均被拒绝；以 `si_member_ddl` 身份，允许 merge 的 replica 被其 profile 拒绝，attach 为其他 replica 后 `DROP`、以及位于 incarnation 路径上层的表都会触发 sweep 告警；丢失状态的 member 从所发布的 `checkpoint_seq` 继续；跨越仅 ClickHouse 重启的 owner 审计。
- **housegate：** 针对每种 peer-leg 形式的 peer-trusted guard；`Replica` 状态行；`forward.Plugin` 把 replica-served session 保持在本地；SI 表在 remote mapping 之前解析。
- **集成（`chpipeline`）：** 在同一个 Keeper 上运行三个带 member 和 network pool 的 SNode；对每个 owner 的并发写；在流中途杀掉一个 member；在从 member 读取某 owner 的表时 quarantine 该 owner；一个 indexer 宕机只停住它自己的 track。

## 18. 风险

- v21 是第三个不可逆格式，即使所有开关都关闭，其编码在 activation 时也已生效；plain-body fallback 能让 compaction 继续运行，但格式本身无法回滚。
- Indexer-c 的 enrolment 不可逆；它会成为一个永久 track，之后每次带 gate 的 activation 都必须升级或 quarantine 它。
- Member 与其主机的 SNode 争夺 CPU、服务器内存和 fetch 线程；placement 和 profile 必须保持容量感知。
- 声明的 failure domain 只有在部署强制执行时才算数。
- 在具备 per-path ACL 的专用 Keeper 出现之前，从 P6 第 6 步起，共享的 production Keeper 承载 member 的 replica；P4 的 ingress allowlist 限制了哪些 pod 能访问它，但任何 SI ClickHouse 仍可写其路径，且在任一 replica 上执行的 `OPTIMIZE` 都会合并 owner 的 candidate。
- 在混合期内，network pool 的故障仍会影响每个 track。
- 一个 owner 的 track 的 liveness 取决于其最弱的 group，且重叠的 group 会在局部耦合各个 track：两个失效的 member 会停住其 group 同时包含这两者的那些 owner。
- 互不相同的 group 会拆分 block，并增加 anchor 和 replay。
- Checkpoint 证明的是与某个 manifest 一致，而不是持有数据；self-audit 以及之后的 06-22 §13 serving audit 弥补这一缺口。

## 19. 开放问题

- Placement 权重：容量单位是什么，以及由谁设置。
- 是每个 verify member 都复制（1 + r 份 copy，默认），还是只有其中 k − 1 个复制。
- Network pool 的最终形态：只做补位、作为每个 group 的强制 member，还是只做 challenger。
- `exclude_quarantined_hosts` 的默认值。
- 是否会从 copy 回答 `unsafe_latest`。
- Mutation 时代的 publication cut（07-01）。
- 专用 Keeper ensemble 的迁移设计。

## 20. 对早先决策的变更

- **Umbrella D7 和 §6.1：** `verifiers` 条目新增主机绑定、密钥、权重、failure domain 和标志（§6.1）。
- **Umbrella §6.5：** "三个 active 非 source verifier 的选择 floor 不变"变为按 group 计算（§7.3），并排除 owner 托管的 verifier。
- **Umbrella D8 和 §6.6：** "其他 SNode 不持有该表"对 SNode 仍然成立；member 不是 SNode，它们在 promotion 和 cleanup 路径之外持有 manifest-gated copy，且其 checkpoint 从不是 source acknowledgement。§6.6 的 purge 规则由 §8.8 取代。
- **Umbrella D9 和 §7.1：** `placement` 开关之后，block 在 (owner, `group_key`) 上 seal，分组窗口以同一对为键，transition block 按 key 划分，每个 track 最多一个未发布。
- **Umbrella §7.4：** S11 收窄 drain gate。
- **Umbrella §7.7**（Spec D 兼容性）：member 跟随已发布的 manifest，因此不存在 promotion cursor，Spec D 的 scalar-cursor 义务就此解除。
- **Umbrella §7.8：** verifier liveness 变为按 group 计算，混合期内共享 network pool。
- **Umbrella §8：** "verifier 为每张 SI 表保留一个 `hg_unsafe` replica"只对 network pool 成立；member 持有其 group 的表；source ClickHouse profile 新增来自 owner 各 group 的每个 member ClickHouse 的 interserver 可达性，以及 `access_control_improvements.table_engines_require_grant = true` 和为每个用户按名称授予的 engine（§8.2）。专用 verifier 的 `hg_safe` 保持为空。
- **Umbrella §9、§10 和 C13：** member 主机以 `Replica`（§11）而非 Ordinary 回答被 quarantine 或不可达的 owner 的表。
- **Umbrella §3、§16 和 §17：** verifier sharding 通过 group 纳入范围；泄露的 `replica_signer` 只能降低其 owner 的 copy floor；object storage 是一种 replica 传输方式，而不是 §17 的 transport 项，后者仍是 source Keeper 隔离和经认证的 interserver fetch。
- **Umbrella §18：** every-node 规则只适用于 network pool。
- **Spec D：** D2–D7 被取代（fan-out 和 per-node acknowledgement 由 S8 取代，D5 由 §8.6 取代，D6 由 §9.2 取代，D7 由 §8.4 取代）；D1 重述为"database group 的 member 既校验又持有 manifest-gated copy"；D8 由 §11 取代，后者只在 owner 被 quarantine 或不可达时提供服务。
- **Umbrella D11；dynamic SI table set D9 及其 create 规则；06-22 §6：** 从 `purge` 开关起，新 incarnation 的 `hg_unsafe` 和 `hg_member_unsafe` replica 使用位于单独根下的 incarnation-scoped Keeper 路径（§8.8），copy 为 `hg_replica.<db__table>__i<seq>`。`hg_safe` / `hg_unsafe` / `hg_promote` 表名、`_hg_row_id` 推导和签名的 target id 不变，因此 D9 否决 per-incarnation 名称的理由不适用。
- **Umbrella D2 和 §11：** agent 获得到 `replica_hosts` 的只读 failover（§11）；写入、discovery、签名和 lane 不变。
- **06-22：** 除 §6 的 Keeper 路径外不变。本阶段用 manifest-gated install 实现 §9、§10 和 §12.5；§12.1 的 engine 拆分保持不变，被服务的 copy 从不在 Keeper 路径上。
- **Arbiter 设计 §7.1 / §7.4：** 选择仍是确定性的，且仍为 2-of-3，现在基于 group 的 eligible ring。
