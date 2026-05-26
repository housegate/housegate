# 单实例 Indexer 的数据可靠性 — 设计文档

**日期：** 2026-05-12
**状态：** Proposed
**作者：** poetry, Claude

> 这是 `2026-05-12-data-reliability-design.md` 的中文版本，两版内容等价。

## 1. 目标

在去中心化 indexer 网络中，每个 indexer 节点运行**恰好一个 housegate +
一个 ClickHouse instance**。没有进程内复制，没有 `ReplicatedMergeTree`
quorum，没有 failover 副本。当宿主机损坏、磁盘丢失、或者 indexer 因任
何原因离线时，用户的索引数据必须保持**持久**（可逐 byte 恢复），且网络
必须保持**可用**（引用这些数据的查询在有界恢复窗口内仍能被服务）。

本文档定义一套分层架构来交付上述两个属性，同时：
- 不引入中心化 SPOF（例如 Kafka 风格的 WAL）；
- 不要求 operator 跨组织边界协调 quorum 成员关系。

核心思路：把**持久性下沉到 S3 兼容的对象存储**（per-operator 的 bucket，
由运营该 indexer 的 operator 自有自付），把 **ownership / handoff 协调
上推到 housegate 已经信任的链上注册中心**（commitgate 和 permission 已经
在用这个机制）。**没有新增 MQ，没有新增共识系统。**

## 2. 非目标

- **不构建 WAL 或消息队列。** 对象存储本身就是 durable log，再叠一层 MQ
  是冗余的，而且会把本方案试图消除的 SPOF 重新引入。
- **不改变 proxy 的无状态契约。** housegate 继续不在本地持久化任何东西。
  所有持久化责任转移到 CH instance 的 storage policy 和链上注册中心。
- **不要求跨 operator 的 ClickHouse Keeper quorum。** 去中心化网络中
  operator 之间没有共享信任边界；跨 operator 跑一个 Keeper 集群与网络
  模型矛盾。本设计改用对象存储 + 链上协调。
- **本版不解决拜占庭恶意 operator 问题。** L4 的 part 哈希清单是基础设
  施；fraud-proof 验证和惩罚机制是显式的未来工作。
- **不替代现有 per-bucket CH 复制特性。** 想要更强本地 HA 的 operator
  可以在自己边界内跑 `ReplicatedMergeTree` + 自有 Keeper；那个跟本设计
  正交，互不影响。

## 3. 背景：今天的数据都在哪里

设计可靠性之前必须先讲清楚什么数据是有风险的。代码盘点结果：

| 数据 | 位置 | 宿主机丢失能否幸存 |
|---|---|---|
| 用户 MergeTree 表（被索引的数据） | CH instance 的本地磁盘 | **否** —— 这是缺口 |
| Schema (CREATE TABLE/DATABASE) | CH 本地 metadata + commitgate 上链 DDL | 部分 —— 链上有 DDL 事件，本地 metadata 丢了 |
| Permissions / ownership | 链上，经 `statemirror` 同步到 Redis | 是 |
| Indexer 连接信息（地址、端口、ID） | 链上，同步到 Redis | 是 |
| housegate session 状态 | 进程内存 | N/A —— proxy 设计上无状态 |
| Usage / billing | 委托给 sentio-node RPC | 是 —— 外部系统 |
| Concurrency limiter 状态 | Redis sorted sets | 设计上短暂存在 |

**唯一的缺口是本地盘上的用户 MergeTree 数据。** 其他所有数据已经有持久
性保证。所以本文档范围窄：**让用户数据安全且可达**。

## 4. 四层模型

可靠性不是单一属性，是若干属性的集合，混在一起会得到糊涂的设计。把关注
点拆成 4 层，每层用独立机制解决：

```
┌─────────────────────────────────────────────────────────────┐
│ L4  完整性  │  周期性的 part 哈希清单上链                     │
├─────────────────────────────────────────────────────────────┤
│ L3  元数据  │  链上注册中心: db → indexer →                  │
│            │  bucket URI → 签名公钥                          │
├─────────────────────────────────────────────────────────────┤
│ L2  可用性  │  Bucket 接管协议 + 跨 indexer 只读 fallback     │
├─────────────────────────────────────────────────────────────┤
│ L1  持久性  │  S3-backed MergeTree；本地盘只做缓存            │
└─────────────────────────────────────────────────────────────┘
```

四层相互独立：单 L1 就给出宿主机丢失的耐久性；L1+L3 给出耐久性 + 数据
可定位；L1+L2+L3 给出有界 RTO 的可用性；L4 在上面叠加 tamper-evidence。
operator 可以单独采用 L1 立刻受益；后续层不需要架构性重构就能组合上去。

## 5. L1 层 —— S3-backed MergeTree 提供持久性

### 5.1 机制

ClickHouse 自 21.x 起原生支持 S3-backed `MergeTree`。在 `storage_policy`
里配置一块 `s3` 类型的 disk，建表时指向该 policy，parts 就直接落到 bucket。
本地盘退化为读缓存。`fsync-on-insert` 和 `cache_on_write` 都可配置。

每个 indexer operator 自备一个 S3 兼容 bucket：可以是 AWS S3、自家 MinIO、
Backblaze B2、Wasabi、Cloudflare R2，或任何 S3 API 兼容的存储。bucket 凭证
永远不离开 operator 自己的环境；housegate 不感知 bucket credentials。

### 5.2 Indexer 端配置

ClickHouse 的 storage policy 写在 `config.xml`（**CH 自己的配置，不是
housegate 的**）。典型配置：

```xml
<storage_configuration>
  <disks>
    <s3_main>
      <type>s3</type>
      <endpoint>https://s3.us-east-1.amazonaws.com/housegate-indexer-7/</endpoint>
      <access_key_id from_env="HG_S3_ACCESS_KEY"/>
      <secret_access_key from_env="HG_S3_SECRET_KEY"/>
      <cache_enabled>true</cache_enabled>
      <cache_path>/var/lib/clickhouse/s3_cache/</cache_path>
      <cache_size>200Gi</cache_size>
    </s3_main>
  </disks>
  <policies>
    <s3_main_policy>
      <volumes><main><disk>s3_main</disk></main></volumes>
    </s3_main_policy>
  </policies>
</storage_configuration>
```

用户建表时附带 `SETTINGS storage_policy = 's3_main_policy'`。rewriter
**不需要**感知 storage policy；这是纯 CH 端的事。housegate 唯一可能要
做的事，是 commitgate observer 在校验 `CREATE TABLE` 时**可选地**要求
带上 storage policy 子句（通过 rewriter 层对 DDL 的检查实现，详见 §8）。

### 5.3 性能权衡

S3-backed MergeTree 在冷缓存读上有 latency 开销。缓解措施：
- 配 `cache_size` 容得下工作集；读全部本地命中。
- `cache_on_write = true`，新插入的 parts 立刻缓存，INSERT 之后的第一
  次读是热的。
- 跨冷数据的大扫描会付 S3 GET 代价；对于"被索引的链上数据"这类场景，访
  问模式严重偏向近期数据，这个代价可接受。

### 5.4 成本模型

对象存储 operator 自付：存储 GB-月 + PUT/GET 请求。对于典型 indexer 负
载（写多、append 为主、批量 parts 的 MergeTree），GET 成本主要来自冷读
的 cache miss，并被 `cache_size` 限上界。PUT 成本随 merge 粒度变化；
operator 在自己 CH 上调 `min_bytes_for_wide_part` 和 merge 参数即可。
**这些都不归 housegate 管。**

### 5.5 L1 单独的收益

仅 L1 之后：
- 宿主机挂了 → 所有用户数据 parts 还在 operator 的 bucket 里。operator
  起一台新 CH 指向同一个 bucket，attach 已有 parts，恢复服务。**零数据丢失。**
- bucket 自身是持久性边界；bucket 级的持久性（AWS S3 的 11 个 9，MinIO
  通过纠删码自定）由存储 provider 兜底。

L1 **不提供可用性** —— 仍存在 indexer 离线的恢复窗口，这是 L2 解决的。

## 6. L2 层 —— Bucket 接管协议提供可用性

### 6.1 目标

当 indexer 离线（宿主机宕机、网络分区、operator 维护）时，针对该 indexer
所托管 DB 的查询必须要么 (a) 由另一个临时 attach 同一 bucket 的 indexer
服务，要么 (b) 以明确的"恢复中"信号失败而非静默错误路由。恢复窗口要有
界，**目标 RTO 在分钟级，不是小时级**。

### 6.2 两种接管口味

区分两类场景。**协议一样，触发条件不同。**

**Operator 主动迁移（计划内，常见）。** owning operator 想换硬件、轮换
密钥、换 hosting provider。签 `transfer(database_id, new_indexer_id,
new_bucket_uri)` 交易。链上注册中心原子更新映射；statemirror 传播新指
针；housegate 的 NetworkState 查询下一跳就解析到新 indexer。Redis 收敛
后老 indexer 即可下线。

**网络发起接管（计划外，少见）。** owning operator 在配置的存活阈值内
无响应。`claim` 交易允许网络把 DB 重分配给一台 standby indexer。claim
必须由以下之一授权：(a) 原 operator 的预签名授权（warm standby），或
(b) 网络治理（多签、slashing 条件等 —— 本文档不涉及；合约接口要支持，
但策略由部署该网络的人定）。

两种情况下，**bucket 都必须对新 indexer 可访问**。两种子模式：

- **共享 bucket 模式。** operator 预先在自己 bucket 上给指定 standby
  operator 的 IAM 身份开读权限。claim 触发时 standby 以只读副本身份
  attach。适合单一 operator 组织内部的 warm-standby 对。
- **Bucket 快照模式。** 原 bucket 仍是 record system。standby indexer
  异步镜像维护（S3 replication、`aws s3 sync`、MinIO bucket replication）。
  claim 时 standby 把镜像提升为主，并把自己的 bucket URI 重新注册到链
  上。适合跨 operator 边界。

部署时二选一；**链上协议一样**。

### 6.3 链上注册中心扩展

现有的链上 DB 注册中心（commitgate / statemirror 已经在用）扩展为：

```
Database {
  id:              DatabaseID
  owner:           Address           // 既有
  indexer_id:      IndexerID         // 既有
  bucket_uri:      string            // 新增: s3://… 或 S3 兼容 URI
  signing_pubkey:  bytes             // 新增: 用于 DB 数据清单签名的公钥
  standby_indexers: [IndexerID]      // 新增: 预授权接管集合
  takeover_policy:  enum             // 新增: { OwnerOnly,
                                     //        StandbySigned,
                                     //        Governance }
  generation:      uint64            // 新增: 每次接管自增，用于围栏
                                     //      过期写入者
}

操作:
  transfer(db_id, new_indexer_id, new_bucket_uri) -> owner 签
  claim   (db_id, new_indexer_id, new_bucket_uri) -> 按策略签
  heartbeat(indexer_id) -> indexer 签（周期性存活证明）
```

**`generation` 是 fencing 机制。** 新 indexer 接管时把 generation 自增；
housegate 的 commitgate observer 拒绝任何来自 generation 小于链上当前值
的 indexer 的写。这防止短暂恢复联网的"僵尸"indexer 把已经被接管转移走
的数据再次污染。

### 6.4 housegate 端改动

rewriter 和 forward plugin 已经通过 NetworkState 解析 `(database) →
(indexer_id, address)`。两处改动：

1. **在解析出的 indexer 信息里加 `bucket_uri` 和 `generation`。** rewriter
   只把 `bucket_uri` 当不透明 metadata 用（仅供 L4 清单发布参考）；proxy
   本身不直接访问 bucket。`generation` 被 commitgate 用作写入围栏。
2. **暴露 "in-recovery" 状态。** 当 NetworkState 报告 indexer 心跳过期、
   且还没有 claim 触发时，对其 DB 的查询以 `Code: 999. DB::Exception:
   housegate: database '<name>' in recovery, please retry` 失败。比 TCP
   层静默超时更友好。

接管交易本身由 operator 或治理工具发起，**不由 housegate 发起**。housegate
是注册中心的消费者，不是写入者。

### 6.5 可选：跨 indexer 只读 fallback

对希望比 RTO-bounded 接管更强读可用性的部署，standby 模式可以保持一个
热读副本：

- operator A 的 housegate 按计划把近期 parts 导出到 operator B 可读的
  共享 bucket prefix。
- operator B 的 CH 把这些 parts 以只读方式 attach（`ATTACH PART`）。
- 链上注册中心把 operator B 的 indexer 标记为该 DB 的 read-fallback
  （新增字段 `read_replicas: [IndexerID]`）。
- rewriter 现有的跨 indexer 路由已支持远程读；增加一个"主下线时尝试的
  fallback 列表"。

这是**可选**项，会带来稳态成本（存储重复 + 网络出口流量），仅适合高价
值 DB。核心规范不要求；列在这里作为支持的扩展。

## 7. L3 层 —— 元数据走链上注册中心

绝大部分已经实现（commitgate, statemirror, NetworkState）。本文档把
schema 扩展为 §6.3 所列字段，并新增：

- **接管时的 DDL 幂等性。** 新 indexer attach 存在 parts 的 bucket 时，
  schema 要么已经存在要么要重新应用。CH metadata 文件（`metadata/` 下的
  `.sql`）如果被 `storage_policy` 包括也会在 bucket 里；否则用链上 DDL
  历史重放。commitgate observer 已经把 DDL 事件上链；接管处理器按序读
  事件，在对新 CH 实例放流量前重放完成。
- **权限重新派生。** 权限本来就在链上，零改动。新 indexer 读同一个
  statemirror Redis，执行同一套 bitmap。

## 8. L4 层 —— Part 哈希清单提供完整性

### 8.1 目标

去中心化网络里 operator 原则上可以篡改自家 bucket 的 parts、伪造查询
结果。L4 通过**周期性把 parts 的内容哈希提交上链**让此类篡改可被任何方
检测出来。

这是基础设施层；完整 fraud-proof 验证和经济惩罚是本文档显式排除的未来
工作。

### 8.2 机制

新增一个 housegate 插件 `manifest`，仅 server 模式运行。周期性（例如每
N 分钟或每 M 次 commit merge，可配）：

1. 查询本地 CH `system.parts` 表，拿到自上次清单以来的 `(database,
   table, part_name, hash, bytes_on_disk, modification_time)`。
2. 在新 parts 上构 Merkle 根（`merkle(按 part_name 排序，叶子 =
   blake3(part_name || part_hash))`）。
3. 用 indexer 签名密钥（§6.3 注册的那个）对 Merkle 根签名。
4. 提交 `commit_manifest(database_id, generation, since_seq, to_seq,
   merkle_root, sig)` 交易。

Merkle 根和每个叶子的清单内容也写到 bucket 的 `_manifests/` prefix 下，
让验证者不需要扫所有 parts 就能重算根。

### 8.3 验证者角色

验证者（网络看门狗、用户侧检查、治理工具）可以：
- 读 bucket 的 `_manifests/` 和链上 `commit_manifest` 事件；
- 重算 Merkle 根并确认与链上 commitment 一致；
- 抽查单个 parts，下载并重算叶子 hash。

清单验证失败 → 验证者报警。惩罚策略（如有）由治理定义，与本文档正交。

### 8.4 成本

清单提交频率低（分钟级或更粗），链上 payload 很小（hash + sig）。链费用
由 operator 自付，有界。

## 9. 故障模式走查

为验证设计，逐个故障场景端到端走一遍。

### 9.1 indexer 宿主机硬盘损坏

- L1：parts 在 S3，完好。
- operator 起新硬件上的新 CH，`storage_configuration` 指向同一 bucket，
  ATTACH 现有 parts。
- operator 提交 `transfer(db_id, new_indexer_id, same_bucket_uri)`
  （只换 `indexer_id`，bucket 不变）。
- statemirror 传播新地址；rewriter 下次查询重新解析。RTO ≈ 起新机时间 +
  状态传播时间。

### 9.2 indexer 网络分区数小时

- L2 心跳阈值超过；链上显示心跳过期。
- 按策略发起网络 `claim`；standby 通过共享 bucket 或快照模式 attach。
- 新 indexer 自增 generation。旧 indexer 恢复后如尝试写入，commitgate
  observer 从链上读最新 generation，发现本地 generation 过期，以 "fenced"
  错误拒绝写入。
- 最终对账：旧 indexer 的 bucket 要么被抛弃（快照模式）要么重 attach 为
  只读镜像（共享模式）。**手动操作**；本文档不自动化对账。

### 9.3 operator 主动优雅迁移

- operator 提交 `transfer`，带停机窗口。
- statemirror 收敛；rewriter 路由到新 indexer。
- 老 indexer 在 drain 期（可配，默认 60s 滑动窗口让在飞查询完成）之后下线。

### 9.4 短暂 indexer 重启（最常见）

- housegate 停，CH 停，几秒内都重启。
- 心跳阈值未越过，不触发接管。
- 已有 client 连接看到 TCP RST，重连。
- L1 保证重启不丢 part；CH 启动时按既有逻辑重放自己的 WAL。

### 9.5 静默 bucket 篡改（拜占庭 operator）

- L4 清单提交使篡改可被检测。
- 验证者识别分歧并报警。
- 网络治理按策略处理 slashing / 重分配（非本文档范围）。

## 10. 实施计划

本文档拆成 4 个大致独立的里程碑。每个里程碑独立可用。

### 里程碑 M1：L1 启用

- 写 operator 操作手册，告诉运营方在自家 CH 上配 S3/MinIO 的
  `storage_policy`。
- 可选：commitgate observer 要求 `CREATE TABLE` 必须带
  `SETTINGS storage_policy = 's3_main_policy'`（或部署方约定的名字）。
  在 rewrite 后通过 `qctx.RawSQL` 检查实现。
- M1 严格说**不需要 proxy 代码改动**；CH 全部搞定。可选的 commitgate
  observer 是小增量。
- **交付物：** operator 可以把 MergeTree 自托管在 S3 上；数据扛宿主机丢失。

### 里程碑 M2：L3 注册中心 schema 扩展

- 链上 Database 结构扩展 `bucket_uri`, `signing_pubkey`,
  `standby_indexers`, `takeover_policy`, `generation`。
- statemirror 暴露新字段。
- NetworkState 消费者（rewriter、forward plugin）消费 `generation`
  用作 fencing。
- 增加 commitgate observer，本地 generation 过期时拒绝写入。
- **交付物：** 接管协议表面就位，即使接管本身还要手动。

### 里程碑 M3：L2 接管协议

- 实现链上 `transfer` / `claim` / `heartbeat` 入口（合约工作，housegate
  仓库之外）。
- 实现 housegate 端的 "in-recovery" 错误暴露。
- 实现 standby indexer 的 DDL 重放工具（CLI 或 startup hook，从链上读
  DDL 历史并应用到新 CH）。
- **交付物：** indexer failover 带有界 RTO。

### 里程碑 M4：L4 清单

- 新增 `manifest` 插件（仅 server 模式）。
- 定义链上 `commit_manifest` 入口。
- 写验证者工具文档。
- **交付物：** tamper-evidence；未来 fraud-proof 工作的基础。

## 11. Open questions

锁设计前我想再过一遍的事项：

1. **Bucket 凭证轮换。** operator 周期性轮换 S3 key。CH 支持 `from_env`，
   但当前版本 reload 需要重启 CH。可接受？还是要做热轮换？
2. **DDL 重放确定性。** 从链上重放 DDL 到新 CH 假设有序且幂等。当前在
   用的 CH DDL 操作里有哪些在 `IF NOT EXISTS` / `IF EXISTS` 重写之下
   **不**幂等？（大部分都幂等；ALTER TABLE 对不存在的列要小心。）
3. **清单频率。** 分钟级是起点；要 benchmark 确认 part 数增长不会让清单
   尺寸爆炸。可能要流式 Merkle 而非快照 Merkle。
4. **Read-fallback 的归属。** operator B 替 operator A 的数据服务读查询
   时，S3 GET 出口流量谁付？大概率 operator B 通过现有 usage/billing 路
   径反向计费，但策略要明文规定。
5. **Bucket 区域故障。** S3 region 级 outage 罕见但真实。设计是否鼓励
   存储层多 region bucket replication？还是 v1 单 region 就行？

## 12. 范围外（显式未来工作）

- Fraud-proof 验证协议和惩罚策略（L4 把基础设施搭好；博弈论那层另起一
  份文档）。
- 跨 operator 的 ClickHouse Keeper quorum（与去中心化架构不匹配，见 §2）。
- 被 fence 的 indexer 恢复后 bucket 分叉的自动对账（按 §9.2 手动处理）。
- housegate-keeper as MQ：本设计取代它，不是 follow-up。
- 加密静态存储 parts，operator 自有密钥（CH 独立支持，与可靠性正交）。

## 13. 为什么不选另两个方案

为了完整性，把原始 prompt 考虑的三个选项及各自被拒绝作为主机制的原因列
一下：

- **housegate-keeper 作为 MQ。** 中心 MQ 即 SPOF；让它 HA 又把本去中心
  化模型要避开的共识问题重新引入。对象存储已经是 durable log，再叠一
  层 MQ 是冗余。
- **链上数据同步。** 链上吞吐量比 CH 写吞吐量低 3-4 个数量级。适合做
  元数据和清单（本设计两处都在用），不适合做数据面。
- **只 S3/MinIO，不做 L2/L3/L4。** 只有持久性没有可用性是不够的；少了
  协调层，死 indexer 的数据安全但不可达。L2 + L3 补这个缺口。

最终方案是 **S3 做持久性 + 链做协调**，让每个底座干自己擅长的事。
