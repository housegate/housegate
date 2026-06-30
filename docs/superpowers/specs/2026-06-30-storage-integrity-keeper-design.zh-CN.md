# Sentio Sequencer — 设计文档（storage integrity 层的 Go 协调器）

**日期：** 2026-06-30 **状态：** Proposed **事实源：** 英文版；协议语义变更时从英文版重新生成中文版。 **基座：** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §5.2 + §12 + §15 Q15，这几节定义了该角色的职责但没有定义其架构。本 spec 填补这个空缺。 **依赖：** [`pkg/replay`](../../../pkg/replay)（verifier 核心 + manifest/receipt/attestation 类型）、[`pkg/lthash`](../../../pkg/lthash)、[`pkg/auth`](../../../pkg/auth)（secp256k1 relay 签名）。

**命名说明。** storage design 的 §5.2 原本把这个角色叫 "Keeper"。本 spec 把它改名为 **Sentio Sequencer**，理由是：(a) 它的核心信任根功能是 `statement_seq` 分配 / L3 sequencing；(b) rollup-sequencer 的框架匹配它的实际形态（sequencing + attestation 聚合 + publication）；(c) 它消除了与不相关的 **ClickHouse Keeper**（[§1.1](#11-clickhouse-keeper不是我们不是-go)）之间持续的混淆。storage design 的文本中有些地方仍写作 "Keeper"；两者指的是本 spec 定义的那个角色。

本文是**设计 + Go API spec**，不是实现。它冻结组件边界、状态模型、签名方案和包映射，让 [§11](#11-交付分期) 中的 P0/P1 实现能对着一个固定目标推进。本文档不写任何生产 Go 代码。

---

## 1. 定位与术语

### 1.1 ClickHouse Keeper（不是我们，不是 Go）

**ClickHouse Keeper** 是 C++ 写的、ZooKeeper 兼容的协调服务，它就在 *ClickHouse 仓库内部* 发布（[`ClickHouse/ClickHouse`](https://github.com/ClickHouse/ClickHouse)，在 `programs/keeper/` 和 `src/Coordination/` 下）。它是 ClickHouse 用来协调 `ReplicatedMergeTree` 复制的、基于 Raft 的 ZooKeeper 替代品。在本设计的 [§12.1](2026-06-22-storage-integrity-design.md) engine 拆分中，ClickHouse Keeper 支撑 `hg_unsafe` ReplicatedMergeTree 表，且只支撑它。

Sentio Sequencer 与 ClickHouse Keeper **既无代码关系，也无协议关系**：

- 我们**不** fork、patch、embed 或重新实现它。ClickHouse Keeper 仍是外部 C++ 依赖，原样部署。
- 我们**不**从 Go 端说 ZooKeeper wire protocol。ClickHouse Keeper 被当作 ClickHouse 与之通信的不透明协调服务；integrity layer 绝不把它的 znodes 当作 integrity state 的事实源。
- 本仓库的 [`pkg/replicationproxy`](../../../pkg/replicationproxy) `KeeperServer` 是一个 **L4 TCP 透传**，可选地转发 ClickHouse↔Keeper 连接用于网络隔离（让一个 ClickHouse 实例只对其 co-located HouseGate 开 TCP）。它只搬字节，不解释字节，而且明确**不是 integrity gate**（[storage design §16](2026-06-22-storage-integrity-design.md)）。那个组件是关于 ClickHouse Keeper 的，不是关于 Sequencer 的。

因此这里**不存在 "用 Go 重写 clickhouse-keeper" 的项目**。C++ 代码留在它该留的地方。选择 "Sentio Sequencer" 这个名字，部分原因就是为了让这个边界显而易见：sequencer 负责 sequencing L3 blocks，ClickHouse Keeper 负责 ReplicatedMergeTree 的协调，两者绝不混为一谈。

### 1.2 Sentio Sequencer（我们，Go）

**Sentio Sequencer** 是 integrity layer 自己的组件——一个 **rollup sequencer + attestation 聚合器 + safe-state publisher**。它的职责，原样继承自 [storage design §5.2](2026-06-22-storage-integrity-design.md)，是担任协议的 sequencer、validator、registry、replay orchestrator、attestation collector、safe-state publisher 和 promotion issuer。"Sequencer" 命名的是该角色的信任根功能（`statement_seq` 分配、L3 block 构造、ordering），但要理解——如同 rollup sequencer 一般——它同时拥有把 ordering 变成已最终化 safe state 的相邻 attestation/publication 机制。

我们用 **Go** 实现它，理由有三条具体的、不是口味问题的事实：

1. **它站在 [`pkg/replay`](../../../pkg/replay) 之上。** verifier 核心（`replay.Verifier`）、manifest/receipt/attestation 类型（`SafeSnapshotManifest`、`ExecutionReceipt`、`ReplayAttestation`、`ReplayJob`）以及进程内 executor（`pkg/replay/payloadexec`）都已经是 Go，并且已经定义了 integrity layer 的数据模型。一个用其他语言消费它们的 Sequencer 会白白付出跨语言边界税。复用映射见 [§4](#4-状态模型)。
2. **它和 HouseGate 共享部署与运维面。** 同一套 Go 工具链、同一套 [`pkg/log`](../../../pkg/log)、同一套 config/secrets loader、同一套可观测性栈。一类二进制，一套 runbook。
3. **Fork ClickHouse Keeper 会把安全协议耦合到不稳定内部实现上。** storage design [§16](2026-06-22-storage-integrity-design.md) 已经否决了 "HouseGate/keeper 作为 *gate* merge 的 ClickHouse-to-Keeper reverse proxy"，因为 gating 需要解析 `ReplicationLogEntry` 序列化（跨版本不是稳定 API），而且 part 字节走的是 interserver HTTP，不是 Keeper。把 Sequencer 的 sequencing/attestation 逻辑实现 *在* ClickHouse Keeper 内部会继承完全相同的耦合。让它们分离正是让 ClickHouse "原样运行" 的前提——这是 storage design 既定的 v1 目标。

两个角色的对照表只在 [storage design §12.1](2026-06-22-storage-integrity-design.md) 再列一次：ClickHouse Keeper 协调 ClickHouse 的 ReplicatedMergeTree；Sentio Sequencer 为 integrity log 做 sequencing。

---

## 2. 目标与非目标

目标（全部服务于 [storage design §2](2026-06-22-storage-integrity-design.md) 的目标；本 spec 是分解，不是重新定范围）：

1. 给 §5.2 的职责清单一个具体架构：组件、接口、durable state、共识边界和签名方案。
2. 冻结 Go 包映射和关键 interface 签名，让 P0/P1 工作对着固定目标推进。
3. **保持 storage design 的安全谓词不变**——尤其是 [§9](2026-06-22-storage-integrity-design.md) 的三方 promotion 谓词（replay quorum **AND** partition-delta **AND** byte-side `part_row_lthash`），它绝不能退化为 root-only promotion。
4. 把 Sequencer HA 当作 v1 关键关注点对待，因为 storage design 已证明（[§12.3 后果 4](2026-06-22-storage-integrity-design.md)）v1 写可用性是 `Sequencer_liveness × promotion_throughput ≥ ingest_rate`。
5. 按名复用 [`pkg/replay`](../../../pkg/replay) 对象，而不是另起炉灶。

非目标（本子设计特有；storage design 自己的非目标仍然适用）：

1. 本文档不含生产 Go 实现。停在 interface 签名和字段级 type sketch。
2. 不 fork、patch 或重新实现 ClickHouse Keeper，也不要 Go 代码说 ZooKeeper wire protocol。
3. 不声称 Sentio Sequencer 解决了恶意 safe-query serving。那仍是独立的 [§13](2026-06-22-storage-integrity-design.md) serving-integrity 问题（概率性审计、query attestation）。
4. v1 不要 multi-Raft。理由见 [§9](#9-共识与-ha)，说明为何 "before production v1 上 multi-Raft" 是过度定范围，以及本 spec 如何分阶段。
5. 不新增链上 / 经济 slashing game。Sentio Sequencer 产出 *证据*（签名 attestation、mismatch receipt）；消费它的经济层不在本文范围。

---

## 3. 组件图

Sentio Sequencer 是一个进程，包含七个逻辑子系统。前六个是对 replicated state 的纯函数；第七个拥有 replication 本身。

```text
                         ┌─────────────────────────────────────────────────────────┐
                         │                Sentio Sequencer process                 │
   HouseGate ─submit────▶│  1. Sequencer      ──▶  2. Dedup (accumulator + hi-seq)  │
   (signed envelope)      │        │                       │ (non-membership proof) │
                         │        ▼                       ▼                         │
                         │  3. Claim registry ◀── RCRecord from Source SNode       │
                         │        │                                                 │
                         │        ▼                                                 │
                         │  4. Replay orchestrator ──▶ ReplayJob ──▶ Verifier pool │
                         │        │                       ◀── ReplayAttestation ── │
                         │        ▼                       (Ed25519-signed)         │
                         │  5. Promotion publisher (§9 three-way predicate)        │
                         │        │                                                 │
                         │        ▼                                                 │
                         │  ─▶ PromotionCommand (secp256k1) ──▶ SNode REPLACEMENT  │
                         │  ─▶ SafeSnapshotManifest         ──▶ publish + L2 anchor│
                         │                                                         │
                         │  6. Schema-transition lane  ◀── DDL admission          │
                         │  6. Membership authority    ◀── replica lifecycle       │
                         │                                                         │
                         │  7. Consensus core (single Raft group, etcd/raft + bbolt)│
                         │     replicates the durable state machine that 1–6 read   │
                         └─────────────────────────────────────────────────────────┘
```

子系统与各自拥有什么：

| # | 子系统 | 拥有 | 通信对象 |
|---|---|---|---|
| 1 | **Sequencer** | `statement_seq` 分配、L3 block 构造、`statement_id → statement_seq` 锚定 | HouseGate（submit）、子系统 2 |
| 2 | **Dedup** | mountain-range Merkle accumulator（`spent_ids_root`）、per-account high-water mark、non-membership proof 校验 | Sequencer |
| 3 | **Claim registry** | `RCRecord` 存储 + 校验 front（linkage、schema、payload、part 算术检查） | Source SNode（claim 提交） |
| 4 | **Replay orchestrator** | 从 L3 + prev safe snapshot + source claim 构造 `ReplayJob`、分发给 verifier replicas、收集 attestation、开 challenge | Verifier pool / SNode replicas |
| 5 | **Promotion publisher** | [§9](2026-06-22-storage-integrity-design.md) 三方谓词、`PromotionCommand` 签名 + 签发、`SafeSnapshotManifest` 发布、safe watermark 推进、L2 anchoring | SNode（promote）、L2 anchor |
| 6a | **Schema-transition lane** | singleton/block-boundary DDL、schema barrier、anchored `schema_snapshot_id` / `schema_root` | HouseGate（DDL submit）、SNode（观测到的 `schema_hash`） |
| 6b | **Membership authority** | `ReplicaStatus`、source-node 选择、Active read-set 准入、lagging-replica promotion replay、cold-bootstrap gating | SNode replicas |
| 7 | **Consensus core** | 支撑 1–6 的 replicated state machine；唯一被允许 *commit* state 的子系统 | Raft peers（其他 Sequencer 节点） |

**该图隐含的边界不变量：**

- **HouseGate 只 submit，从不决策。** ingress HouseGate 校验已签名 envelope 的签名、计算 `payload_hash`、spool payload，然后向 Sequencer 提交 `StatementEnvelopeV2`。它不分配 `statement_seq`、不 promote，并且明确不是 "correctness 的最终裁判"（[storage design §5.1](2026-06-22-storage-integrity-design.md)）。
- **Verifier replicas 只 attest，从不 promote。** 一个 verifier replica 运行 [`replay.Verifier.Verify`](../../../pkg/replay/verifier.go) 并返回 `ReplayAttestation`。Promotion 是 publisher 的决策，gated 在一个 attestation quorum 加上 [§9](2026-06-22-storage-integrity-design.md) 三方谓词上。Verifier 类型原样复用（见 [§4](#4-状态模型)）。
- **ClickHouse Keeper 完全不在此图中。** 它协调 ClickHouse 自己的 `hg_unsafe` ReplicatedMergeTree，对子系统 1–6 不可见。HouseGate 可选地 L4 转发 ClickHouse↔Keeper 连接，这部分逻辑在 [`pkg/replicationproxy`](../../../pkg/replicationproxy) 中，且仅用于网络隔离。

---

## 4. 状态模型

状态模型分两类：**从 [`pkg/replay`](../../../pkg/replay) 原样复用的类型**，和**本设计引入的类型**。复用没有商量余地：integrity layer 的正确性论证建立在这些确切类型上，发明别名会让契约漂移。

### 4.1 从 `pkg/replay` 复用（不要重定义）

这些是承重类型。下面的字段列表是摘要；权威定义在所链接的源码。

- **`replay.SafeSnapshotManifest`** — [`pkg/replay/types.go:11`](../../../pkg/replay/types.go)。发布的 safe state。字段：`SnapshotID, ParentSnapshotID, SafeBlockSeq uint64, SchemaSnapshotID, SchemaRoot, ExecutorProfileID, DataRoot, StateRoot, ManifestRoot, Tables []TableManifest`。有值接收者 `Seal()`、`Validate()`、`Compute*Root()` 方法供 publisher 调用；`SnapshotID` 未设置时默认取 `ManifestRoot`。
- **`replay.ReplayJob`** — [`pkg/replay/types.go:212`](../../../pkg/replay/types.go)。一个 sequenced block 的 verifier 输入：`BlockSeq, PrevSafeSnapshotID, PrevStateRoot, SchemaSnapshotID, ExecutorProfileID, SourceClaimRoot, Statements []Statement`。orchestrator 从 `L3Block + RCRecord + prev manifest` 构造它。
- **`replay.Statement`** — [`pkg/replay/types.go:223`](../../../pkg/replay/types.go)。已签名 envelope 的 replay 相关投影：`StatementID, StatementSeq uint64, SQL, SQLHash, SettingsHash, PayloadRef, PayloadHash, PayloadLength uint64, TargetTableID, UserJWS`。注意 `StatementSeq` 在到达 verifier 时已经存在且非空——是 Sequencer 分配的。
- **`replay.ExecutionReceipt`** — [`pkg/replay/types.go:265`](../../../pkg/replay/types.go)。verifier 签名的内容：block 元数据 + `StatementRoot, PayloadRoot, SourceClaimRoot, ComputedStateRoot, MatchSourceRoot bool, PartitionCommitmentsAfter, AffectedParts, ReplayLogHash`。通过 `canonicalDigest("replay-execution-receipt", r)` 实现 `Hash() (string, error)`。mismatch（`MatchSourceRoot=false`）也会被签名——那就是 challenge-evidence 路径。
- **`replay.ReplayAttestation`** — [`pkg/replay/types.go:286`](../../../pkg/replay/types.go)。publisher 收集的 verifier 输出：`ReplicaID, Receipt ExecutionReceipt, ReceiptHash, Signature, MatchSourceRoot bool`。
- **`replay.Verifier`** — [`pkg/replay/verifier.go:31`](../../../pkg/replay/verifier.go)（是 struct，不是 interface）：`Snapshots SnapshotStore, Payloads PayloadStore, Executor Executor, Signer Signer`。入口 `Verify(ctx, ReplayJob) (ReplayAttestation, error)`。它已经强制：snapshot 加载 + `Validate`、`block_seq > snap.SafeBlockSeq`、执行 *前* payload `hash`/`length` 匹配、executor 输出形状、receipt 签名。Sequencer **不**重新实现这些。
- **接口** — `replay.SnapshotStore`（[`:9`](../../../pkg/replay/verifier.go)）、`replay.PayloadStore`（[`:14`](../../../pkg/replay/verifier.go)）、`replay.Executor`（[`:19`](../../../pkg/replay/verifier.go)）、`replay.Signer`（[`:25`](../../../pkg/replay/verifier.go)）。Sequencer 提供生产级 `SnapshotStore`/`PayloadStore` 实现，backed by 共识核心的 replicated state（或外部 DA 承载 payload——open question [§12 Q2](#12-open-questions)）。
- **哈希原语** — `replay.DigestBytes` / `DigestString`（[`pkg/replay/hash.go:15`](../../../pkg/replay/hash.go)）和未导出的 `canonicalDigest(domain, v)`（`"housegate-replay-mvp-v0:<domain>\x00<body"` → sha256，`0x` 前缀 hex）。**Sequencer 侧所有新增 commitment 复用 `canonicalDigest` 加新 domain tag**（见 [§4.3](#43-哈希与规范化)），而不是引入并行的 hash profile。

### 4.2 本设计引入的新类型

所有新类型都是 JSON 序列化的 Go struct，`json` tag 约定与 `pkg/replay` 一致，因此它们能同样地流过 `canonicalDigest`。下面的字段级 sketch 冻结形状；确切的 wire schema 是 P0 工作（open question [§12 Q1](#12-open-questions)）。

```go
// StatementEnvelopeV2 是 HouseGate 校验完用户 JWS 后提交的内容。
// 它承载 Phase-1（agent materialize 后）的 SQL；Phase-2 physical rewrite 不被签名，
// 由 executor 在 replay 时重算（storage design §7）。
StatementEnvelopeV2 {
  envelope_version    int      `json:"envelope_version"`
  network_id          uint64   `json:"network_id"`
  keeper_shard_id     uint32   `json:"keeper_shard_id"`
  client_account      string   `json:"client_account"`
  statement_id        string   `json:"statement_id"`     // 被签名；喂 _hg_row_id
  statement_kind      string   `json:"statement_kind"`   // insert|mutation|ddl
  virtual_table_id    string   `json:"virtual_table_id"`
  rewritten_sql       string   `json:"rewritten_sql"`    // Phase-1 输出
  sql_hash            string   `json:"sql_hash"`         // H(rewritten_sql)
  settings_hash       string   `json:"settings_hash"`
  schema_snapshot_id  string   `json:"schema_snapshot_id"`
  payload_ref         string   `json:"payload_ref,omitempty"`
  payload_hash        string   `json:"payload_hash,omitempty"`
  payload_length      uint64   `json:"payload_length,omitempty"`
  payload_format      string   `json:"payload_format,omitempty"`
  row_id_profile_id   string   `json:"row_id_profile_id"`
  user_jws_v2         string   `json:"user_jws_v2"`
}

// L3Block 是 sequencer 已 commit 的批次。statement_seq 在这里分配，
// 不在 envelope 里。spent_ids_root_after 是本 block 的 statement_id 折叠进
// L3-derived mountain-range accumulator 之后的 root（storage design §7）。
L3Block {
  l3_block_seq        uint64                  `json:"l3_block_seq"`
  prev_l3_hash        string                  `json:"prev_l3_hash"`
  l2_anchor_ref       string                  `json:"l2_anchor_ref,omitempty"`
  statement_seq_start uint64                  `json:"statement_seq_start"`
  statements          []StatementEnvelopeV2   `json:"statements"`
  schema_snapshot_id  string                  `json:"schema_snapshot_id"`
  executor_profile_id string                  `json:"executor_profile_id"`
  prev_safe_snapshot_id string                `json:"prev_safe_snapshot_id"`
  prev_state_root     string                  `json:"prev_state_root"`
  spent_ids_root_after string                 `json:"spent_ids_root_after"`
}

// RCRecord 是 source SNode 的 result claim。claim registry 只在校验 front
//（§5）通过后才接受它。
RCRecord {
  l3_block_seq           uint64           `json:"l3_block_seq"`
  statement_seq          uint64           `json:"statement_seq"`
  source_node            string           `json:"source_node"`
  unsafe_table           string           `json:"unsafe_table"`
  candidate_parts        []CandidatePart  `json:"candidate_parts"`
  partition_deltas       []PartitionDelta `json:"partition_deltas"`
  source_claim_state_root string          `json:"source_claim_state_root"`
}

CandidatePart {
  part_name        string `json:"part_name"`
  partition_id     string `json:"partition_id"`
  part_phys_hash   string `json:"part_phys_hash"`
  part_row_lthash  string `json:"part_row_lthash"`   // 0x 前缀 hex，即 lthash.Hash.Bytes()
  row_count        uint64 `json:"row_count"`
  bytes            uint64 `json:"bytes"`
}

PartitionDelta {
  table_id         string `json:"table_id"`
  partition_id     string `json:"partition_id"`
  delta            string `json:"delta"`             // 0x 前缀 hex；sum(new) - sum(old) LtHash
}

// PromotionCommand 是 publisher 在 §9 三方谓词通过后签发的。它由 Sequencer 身份
//（§6.3）secp256k1 签名，驱动每个 attesting SNode 上的本地 REPLACE PARTITION
//（storage §12.2）。
PromotionCommand {
  command_kind          string   `json:"command_kind"`            // insert|mutation|merge|drop_unsafe
  table_id              string   `json:"table_id"`
  partition_id          string   `json:"partition_id"`
  base_safe_snapshot_id string   `json:"base_safe_snapshot_id"`  // publish lock 的 CAS base
  base_partition_root   string   `json:"base_partition_root"`    // partition 的 CAS base
  promotion_seq         uint64   `json:"promotion_seq"`          // per-(table,partition_id) 单调
  target_snapshot_id    string   `json:"target_snapshot_id"`     // 结果 SafeSnapshotManifest
  target_state_root     string   `json:"target_state_root"`
  promoted_part_hashes  []string `json:"promoted_part_hashes"`   // candidate part_phys_hash 集合
  issued_at_unix        int64    `json:"issued_at_unix"`
  expires_at_unix       int64    `json:"expires_at_unix"`        // SNode 必须在此前 publish
  sequencer_address     string   `json:"sequencer_address"`      // secp256k1 signer 地址
  sequencer_signature   string   `json:"sequencer_signature"`    // 由 Sign() 填充
}

// SchemaTransition 是 schema-transition lane（§7）上的一条 singleton/block-boundary
// DDL 记录。它铸造一个新的 schema_snapshot_id。
SchemaTransition {
  transition_seq        uint64   `json:"transition_seq"`
  prev_schema_snapshot_id string `json:"prev_schema_snapshot_id"`
  new_schema_snapshot_id  string `json:"new_schema_snapshot_id"`
  new_schema_root        string  `json:"new_schema_root"`
  ddl_envelope           StatementEnvelopeV2 `json:"ddl_envelope"`
  observed_schema_hashes map[string]string   `json:"observed_schema_hashes,omitempty"` // table_id -> SNode 上报
}

// ReplicaStatus 跟踪一个 SNode 的生命周期（§8）。
ReplicaStatus struct {
  replica_id            string    `json:"replica_id"`
  indexer_address       string    `json:"indexer_address"`
  state                 string    `json:"state"`           // joining|catching_up|active|suspended|leaving
  last_safe_snapshot_id string    `json:"last_safe_snapshot_id"`
  last_seen_unix        int64     `json:"last_seen_unix"`
  active_read_set       bool      `json:"active_read_set"`
}
```

`SafeSnapshotManifest` 发布、`ReplayJob` 构造、`ReplayAttestation` 收集**不是**新类型——它们是 publisher 和 orchestrator 对复用的 `pkg/replay` 类型执行的操作。storage design 已经命名了它们；本设计复用 Go struct。

### 4.3 哈希与规范化

Sequencer 铸造的每一个新 commitment（L3 block hash、accumulator root、`PromotionCommand` 签名 payload、`SchemaTransition` root）都走 `pkg/replay.canonicalDigest` 加新 domain tag：

```text
canonicalDigest("sequencer-l3-block",            l3BlockJSON)
canonicalDigest("sequencer-spent-ids",           accumulatorStateJSON)
canonicalDigest("sequencer-promotion-command",   promotionCommandCanonicalJSON)   // 去掉 sequencer_signature
canonicalDigest("sequencer-schema-transition",   schemaTransitionJSON)
```

`canonicalDigest` 当前未导出。P0 要么导出一个薄包装 `replay.CanonicalDigest(domain, v)`，要么在 `keeper` 包里加一个调用它的 helper——目标是整个 integrity layer 一个 canonicalization profile，不是两个。这列为 [§11](#11-交付分期) 的 P0 任务。

---

## 5. Sequencing 与 dedup

本节把 [storage design §7](2026-06-22-storage-integrity-design.md) 的 sequencing 规则映射到 [§3](#3-组件图) 的组件上。没有任何规则被改变；这里的价值是明确每条规则在 *哪里* 被强制。

### 5.1 `statement_seq` vs `statement_id` —— 由不同组件强制

| | `statement_id` | `statement_seq` |
|---|---|---|
| 分配方 | client/agent（签名前） | **Sequencer**（子系统 1，submit 之后） |
| 是否签名 | **是**（在 `user_jws_v2` 里） | **否** |
| 校验方 | HouseGate（签名）、Sequencer 子系统（形状） | n/a —— Sequencer 铸造它 |
| dedup 强制方 | **Dedup**（子系统 2）：accumulator + high-water | n/a |
| 角色 | 身份 / dedup / 喂 `_hg_row_id` | 排序 / part 归属 |

这个拆分是承重的：`statement_seq` 不能被签名，因为 [签名者在签名时无法知道自己的位置](2026-06-22-storage-integrity-design.md)（storage design §7）。因此 Sequencer 只从 envelope 接受 `statement_id`，并在 L3 block 中锚定 `statement_id → statement_seq` 绑定，使映射可审计。

### 5.2 Dedup：accumulator + high-water mark（子系统 2）

原样采用 [storage design §7](2026-06-22-storage-integrity-design.md)：

- 一个 **mountain-range Merkle accumulator** 在每个 L3 block 中提交 `spent_ids_root`。它是 sequenced `statement_id` 的纯函数——任何 replay L3 流的诚实节点都能相同地重建它，所以去中心化 Sequencer 不改变 dedup 事实。
- 接受要求一个 **non-membership proof**，证明 `statement_id` 不在之前 `spent_ids_root` 之下。
- 一个 **per-account high-water mark** `hi_seq[account]` 给正常流量 O(1) 接受：新的 `client_seq > hi_seq` 不需要 non-membership proof；只有乱序的 `client_seq ≤ hi_seq` 才回退到 accumulator proof。这把 dedup 状态 bound 到每个活跃 account 一个整数加一个 gap 集合，并且按 `client_account` 干净分片。
- accumulator 是 append-only 且永久的；范围是 **per-account-global**。

mountain-range 构造及其 non-membership proof 测试向量是 storage-design P0 交付物；Sequencer 在 P1 的工作是把它接到子系统 2，藏在 [§10.2](#102-dedup) 的 `Accumulator` 接口背后。

### 5.3 L3 block 构造（子系统 1）

dedup 通过后，Sequencer：

1. 给每个被接受的 `statement_id` 分配单调的 `statement_seq`（block 的 `statement_seq_start` 加偏移）。
2. 把已接受的 envelope 批量化进一个 `L3Block`，整个 block 带一个 `schema_snapshot_id` 和一个 `executor_profile_id`（v1 block 级 schema scoping 规则，[storage design §7](2026-06-22-storage-integrity-design.md)）。
3. 把本 block 的 `statement_id` 折叠进 accumulator，记录 `spent_ids_root_after`。
4. 通过共识核心（子系统 7）commit 这个 block。Raft commit 之前 block 不算 durable。

### 5.4 Source-claim 校验 front（子系统 3）

claim registry 只在以下检查按此顺序（便宜→昂贵、fail-closed）通过后，才接受 source SNode 的 `RCRecord`：

1. **Linkage** —— `RCRecord.l3_block_seq` 和 `RCRecord.statement_seq` 引用一个已 commit 的 L3 block；`source_node` 匹配 Sequencer 对该 statement 的 source 分配。
2. **Schema/settings** —— candidate parts 的表引用在 block 的 `schema_snapshot_id` 下可解析。
3. **Payload 可用性** —— `payload_ref` 可从 payload store 取回；`payload_hash`/`payload_length` 匹配（[storage design §5.4](2026-06-22-storage-integrity-design.md) 在此检查 *之后* 才加载字节）。
4. **Part 算术** —— 每个 partition 的 `Σ candidate_parts.part_row_lthash` 与 `partition_deltas` 内部自洽，且 `partition_deltas` 在 executor 使用的同一 LtHash 算术下折叠进 `source_claim_state_root`（[§8](2026-06-22-storage-integrity-design.md)）。

未通过 front 的 `RCRecord` 被拒绝；orchestrator 看不到它。这是 *注册校验*，不是 promotion——promotion 仍要求 [§6](#6-replay-orchestration-与-promotion) 的三方谓词。两者不同：一个对 `bytes_evil` 自洽的 `RCRecord` 能通过 front，但会在 promotion 的检查 2/3 失败。

---

## 6. Replay orchestration 与 promotion

这是安全关键核心。[storage design §9](2026-06-22-storage-integrity-design.md) 的三方 promotion 谓词在此原样复现并显式命名，使任何实现都无法悄悄弱化它。

### 6.1 Replay orchestration（子系统 4）

对每个带已接受 `RCRecord` 的已 commit L3 block，orchestrator：

1. 从 `L3Block + RCRecord.source_claim_state_root + 上一个 SafeSnapshotManifest 身份` 构造一个 [`replay.ReplayJob`](../../../pkg/replay/types.go)。映射是机械的——每个 `StatementEnvelopeV2` 变成一个 `replay.Statement`（已经是 replay 相关投影），`L3Block.l3_block_seq → ReplayJob.BlockSeq`，`prev_safe_snapshot_id/prev_state_root → ReplayJob.Prev*`，等等。
2. 把 `ReplayJob` 分发给 ≥3 个独立 verifier replicas（storage design P0 freeze：**promote 需要 ≥2 of 3**；source 的 self-attestation 不算）。
3. 每个 replica 运行 [`replay.Verifier.Verify`](../../../pkg/replay/verifier.go)，它加载 prev safe snapshot、在执行 *前* 校验 payload hash/length、运行 pinned executor、返回带 `ComputedStateRoot` 和 `MatchSourceRoot` 的签名 `ReplayAttestation`。
4. 超时或不一致时，orchestrator 开 **challenge replay**（[§6.4](#64-challenge-replay)）。

verifier replicas *独立于* Sequencer 进程（它们持有 pinned ClickHouse build + scratch executor）；Sequencer 只构造 job 并收集 attestation。

### 6.2 三方 promotion 谓词（子系统 5）

Promotion **不是** root-only equality。它是三个检查的合取，每一个都承重，完全如同 [storage design §9](2026-06-22-storage-integrity-design.md)：

1. **Replay 检查** —— 一个 quorum（≥2 of 3）的 replicas 独立 replay 已签名 L3 payload 并产出与 `RCRecord.source_claim_state_root` 相同的 `computed_state_root`。*证明 payload 的正确执行产出此 root。*
2. **Partition-delta 检查** —— 对每个 touched partition，source 上报的 `Σ(candidate_parts.part_row_lthash)` 等于 replicas 在 replay 时算出的 partition delta。*把 source 的 per-part claim 绑定到 replay root；击败共谋 source——否则它会为 evil rows 上报求和等于正确 delta 的 per-part hash，没有碰撞则不可行，而 per-row `_hg_row_id` 排除了碰撞。*
3. **Byte-side part-lthash 检查** —— 每个 attesting replica 读取它实际 fetch 到的 part 字节（`SELECT ... WHERE _part IN (...)`），重算 `part_row_lthash`，确认等于 `RCRecord.candidate_parts`。*把 source 上报的 per-part hash 绑定到磁盘实际字节；三者中唯一触碰 source 实际 part 字节的检查。*

**三项中任一失败，promotion 拒绝。** 没有 checks 2 和 3 的 root match 显式 *不是* promotion，且 2 和 3 互补，不冗余：check 2 关闭 "为 `bytes_evil` 上报看起来正确的 `part_row_lthash`" 这一半；check 3 关闭 "为 `bytes_evil` 上报 hash 但存了不同字节" 这一半。Promotion 链是 `root —check 2→ Σ source per-part claim —check 3→ 实际磁盘字节`；每个环节都需要。

> **Spec 守卫。** 任何让 part 仅凭 root-only check 进入 `hg_safe` 的代码改动都是正确性回归，不是优化。该谓词的 P0 freeze 正是 [acceptance grep](#验证) 要检查的。

### 6.3 INSERT vs mutation 路径

第三项检查因 statement class 而异（[storage design §10](2026-06-22-storage-integrity-design.md)）：

- **INSERT 路径** —— 检查 3 是对共享的、已复制的 `hg_unsafe` candidate parts 的 fetched-byte 扫描（每个 replica fetch 同样的字节）。
- **Mutation 路径** —— 没有共享 fetched-byte 对象（每个 replica 在自己 scratch 里重新生成 mutated parts）。检查 3 变成 **recomputed-commitment match**：每个 replica 从自己本地 materialize 的 post-state 重算 post-mutation per-partition `partition_commitment`，确认它等于 safe pre-state commitment 加上声称的 `partition_deltas`。比较是绝对对绝对（`partition_commitment` 是绝对 LtHash accumulator；`partition_deltas` 是 `Σ new − Σ old`；可加性使 `post = pre + delta` 精确）。

两条路径共享不变的检查 1 和 2。Mutation-class statements（`ALTER … UPDATE/DELETE`）在 v1 只在 bounded profile 下接受（[storage design §10](2026-06-22-storage-integrity-design.md)）；`INSERT … SELECT` 在 admission 处拒绝。

### 6.4 Challenge replay

mismatch 或超时会开 challenge replay。**Challenge 裁决用与 promotion 相同的三方谓词**（[storage design §11](2026-06-22-storage-integrity-design.md)）——它 *不* 仅凭 reproduced-root equality 本身解决，因为那正是该谓词要拒绝的 `bytes_evil`-with-truthful-root 情况。一个签名 mismatch attestation（`MatchSourceRoot=false`，仍由 `replay.Verifier` 签）是不可抵赖的 challenge 证据。v1 中心化 Sequencer 立即裁决（无 challenge window）；challenge-window 安全模型是去中心化阶段（[§9](#9-共识与-ha)）的事。

### 6.5 Promotion 命令签发与 CAS

当谓词通过，publisher（子系统 5）：

1. Seal 结果 `SafeSnapshotManifest`（`replay.SafeSnapshotManifest.Seal()`）。
2. 铸造一个 `PromotionCommand`，携带 **CAS base** —— `base_safe_snapshot_id` + `base_partition_root` + `(table_id, partition_id)` 单调的 `promotion_seq`。CAS base 实现 [storage design §12.2](2026-06-22-storage-integrity-design.md) 的 publish-time-base 规则：SNode 取本地 publish lock、检查当前活跃 `hg_safe` partition 是否仍匹配 base，只有匹配才执行 Sequencer 签名的 `REPLACE PARTITION`（来自 `hg_promote`）。若另一个 promotion 已经推进了该 partition，本命令被拒绝并 rebase。
3. 用 **Sequencer 身份（secp256k1）** 签名命令 —— 见 [§10.5](#105-promotion-与签名方案)。这是一个 *不同于* attestation 签名的签名。
4. 发布 manifest、推进 safe watermark、把 L3 block hash + state root anchor 到 L2。

并发 INSERT promotion 进同一 partition 由 `promotion_seq` 顺序在 `(table_id, partition_id)` 序列化，这是 [storage design §10](2026-06-22-storage-integrity-design.md) mutation barrier 的 INSERT-path 对应。

---

## 7. Schema-transition lane（子系统 6a）

采用 [storage design §7](2026-06-22-storage-integrity-design.md) 的 schema-transition 规则。v1 中每个被接受的 schema 变更都在独立 lane 上作为 **singleton block 或 block-boundary transition** sequencing，不走 unsafe-part promotion。该 lane：

1. 安装 **table/database 级 schema barrier**，停止接受旧 schema 下的新写入，drain 或拒绝未完成的旧 schema unsafe 写入。
2. 铸造新的 `schema_snapshot_id` 和 `schema_root`（lane 发出一条 `SchemaTransition` 记录，[§4.2](#42-本设计引入的新类型)）。
3. 向所有 protocol-owned 物理 surface（`hg_safe`、`hg_unsafe`、`hg_promote` 模板、mutation scratch 模板、replay scratch 模板）下发 Sequencer 签名的 DDL。
4. 接受 SNode 上报的 `schema_hash` 观测并与 anchored root 比较。**Source-side `system.columns` 是观测，不是权威** —— verifier 只从 anchored DDL/settings log 派生 schema。本地 schema 匹配 anchored root 后才恢复正常写入。

v1 的 DDL admission class（复现自 [storage design §7](2026-06-22-storage-integrity-design.md)）：

| Statement class | v1 路由 |
|---|---|
| `CREATE TABLE` | 仅当 engine、partition key、order key、primary key、storage policy、defaults/materialized 表达式和类型都在 verified whitelist 上才接受。Sequencer 分配稳定 `table_id`/`column_id` 并注入保留列（`_hg_row_id`）。 |
| `ADD COLUMN` | 仅对非 key、非保留、有确定性 immutable `DEFAULT`/`NULL` 语义和稳定 `column_id` 的列是 metadata-only。仅当 profile 定义了旧 sealed parts 如何 canonicalize 缺失列时才 commitment-neutral。 |
| `RENAME COLUMN` | Metadata-only：commitment 绑 `column_id`，不是显示名。保留列永不改名。 |
| `MODIFY DEFAULT` | 除非 profile 证明它只影响未来 insert、不影响旧 sealed parts 的读时值，否则拒绝。 |
| `DROP COLUMN` / `MODIFY COLUMN` 类型 | v1 默认拒绝；后续可接受形式必须是 mutation-class rehash。 |
| `TRUNCATE` / `DROP PARTITION` | Mutation-class 但便宜：delta 是 `-partition_commitment`。 |
| Partition/order/primary key、engine、storage policy、TTL、projection/index 变更 | v1 拒绝。 |
| `_hg_row_id` 及其他 protocol 列 | 永不让用户改。 |

---

## 8. Replica 生命周期与 membership（子系统 6b）

membership authority 拥有 [storage design §11](2026-06-22-storage-integrity-design.md) 状态机和 §12.5 的 bootstrap 路径。状态转换（复现）：

```text
[*] → Accepted → Sequenced → UnsafeExecuting → UnsafeRegistered
                                   → Replaying → QuorumVerified → FinalityWait → Safe
                                                → ChallengeReplay → Safe | Rejected → Dropped
```

职责：

- **Source-node 选择** —— 为一条 statement 选 optimistic-execution source（storage design 的 route A 默认）。
- **Replay-quorum membership** —— 为一个 `ReplayJob` 选 ≥3 个 verifier replicas 并计 attestation；source 的 self-attestation 不计入 2-of-3。
- **Lagging-replica promotion replay** —— 一个尚未通过 ReplicatedMergeTree fetch 到 candidate parts 的 replica 是 liveness 问题，不是 safety 问题（它没有 attest）。当它追上后，它 **按顺序 replay 已记录的 per-`(table_id, partition_id)` promotion 序列**，把每个 promotion 的 CAS base 对该 promotion 记录的 `base_safe_snapshot_id`/`base_partition_root` 解析，而不是对它当前 watermark（[storage design §12.5](2026-06-22-storage-integrity-design.md)）。这正是让迟到的 replica 仍能复现每一步的原因。
- **Cold bootstrap** —— 一个全新/长期离线的节点从空开始。两条恢复路径，都不在热路径：(1) **从 L3 流 replay**（从 genesis 或最旧保留 safe snapshot replay 已签名 payload 重建 `hg_safe`）；(2) **从 peer 的 `hg_safe` 拷贝**（fetch safe parts、按发布的 manifest 校验每个 part 的 `part_phys_hash`/`part_row_lthash`、attach）。新节点在产出匹配网络当前 safe watermark 的 `SafeSnapshotManifest` 之前不得进入 **Active read set**（[storage design §12.5 open question 14](2026-06-22-storage-integrity-design.md)）。
- **审计钩子** —— 喂给 [storage design §13](2026-06-22-storage-integrity-design.md) 的 safe-serving 审计（定期 safe-part 扫描、query 采样、cross-node 比较）；审计失败把 replica 从 read set 剔除。

---

## 9. 共识与 HA

本节是本设计 **与 `.omo/plans` 草案分叉** 的地方，草案要求 "before production v1 上 multi-Raft"。该范围对 v1 是错的，理由是技术的，不是进度驱动的。

### 9.1 v1 = 单 Raft group（选定路径）

v1 Sentio Sequencer 跨一个小规模（3 节点）ensemble 跑 **一个 Raft group**，复制整个 durable state machine（子系统 1–6 的已 commit state）。库是 [`go.etcd.io/raft/v3`](https://github.com/etcd-io/raft)——etcd、TiKV、CockroachDB 构建于其上的同一个库。Sequencer 提供：

- 一个 `StateMachine` 实现（`Apply`/`Snapshot`/`Restore` 三元组，[§10.3](#103-state)），包裹子系统 1–6。
- 一个由 **bbolt**（`go.etcd.io/bbolt`）支撑的 `raft.Storage` 实现，作为 WAL/log store，加上周期性 `StateMachine.Snapshot()` 用于 log compaction。
- 一个 Sequencer peer 之间的 Raft message transport（gRPC 或原生 etcd-raft `Transport` 接口——open question [§12 Q4](#12-open-questions)）。

sequencer/publisher 决定的一切，在权威化之前都通过这一个 group commit。需要 linearizable 语义的读走 `raft.Node.Status()`/`ReadIndex`（Sequencer 大多数读是对已 commit state 的读，可由 follower 带 lease 本地服务）。

### 9.2 为什么 multi-Raft 不是 v1（修正草案）

**Multi-Raft 不是 `go.etcd.io/raft/v3` 的原生能力。** 该库围绕每个 group 一个 `raft.Node` 构建。[etcd-dev MultiRaft 讨论](https://groups.google.com/g/etcd-dev/c/cq88rpcxvm8) 确认 MultiRaft 从未合并回 etcd 的 raft 库。需要在此库上做 multi-Raft 的系统——**TiKV**（[设计文章](https://www.pingcap.com/blog/design-and-implementation-of-multi-raft/)）和 **CockroachDB**（[scaling-raft 文章](https://www.cockroachlabs.com/blog/scaling-raft/)）——各自在它之上自建了 region/group 路由、per-group `raft.Node` 扇出、heartbeat 合并和 snapshot 管理。那本身就是一个多季度的子系统，有自己的正确性陷阱（跨 group 事务排序、region split/merge、批量 heartbeat）。

把它塞进 "v1、production 之前" 会让 multi-Raft——而不是 sequencing、不是三方谓词、不是 promote 数据面——成为主导的实现风险和最长 pole。它直接违背 storage design 既定的 v1 目标 "implementable without forking ClickHouse" 的自然类比：*implementable without inventing a distributed database*。

因此分阶段是：

- **v1（P1）：** 单 Raft group、3 节点、完整 sequencer + INSERT promote。`StateMachine` 和 `Sharder` 接口（下文）已定义，但 `Sharder` 对每个 key 返回同一个 group。
- **P5+：** multi-Raft + 按 table/database/account 分片，生产级横向扩展路径。重开 [storage design §15 Q15](2026-06-22-storage-integrity-design.md) 的 "multi-Raft group layout、按 table/database 的 shard routing、cross-Sequencer L2 height clock"。`Sharder` 接口正是让这成为一个函数 *替换* 而非重写的东西。

面向 Sequencer、为未来留出空间的接口：

```go
// Sharder 把一个 state key 映射到负责它的 Raft group。v1 对每个 key 只有一个
// group（group 0）；P5+ 引入 per-table/account 的 group。
type Sharder interface {
    GroupForStatement(env StatementEnvelopeV2) (groupID uint64, err error)
    GroupForPartition(tableID, partitionID string) (groupID uint64, err error)
    GroupForSchema() (groupID uint64, err error) // schema lane
    Groups() []uint64
}
```

`Sharder` 是 v1 承诺的 *唯一* multi-Raft seam；共识核心调用它并对一切得到 `0`。

### 9.3 Sequencer liveness 是写可用性单点（承认、缓解、不假装不存在）

[storage design §12.3 后果 4](2026-06-22-storage-integrity-design.md) 证明 v1 写可用性是 `Sequencer_liveness × promotion_throughput ≥ ingest_rate`：在 `hg_unsafe` 处于 `STOP MERGES` 且 `parts_to_throw_insert` 被 pin 的情况下，唯一 drain `hg_unsafe` 的是 promotion，而 promotion 需要 Sequencer。Sequencer 宕机会停止 promotion、parts 堆积、某 partition 撞顶、写入网络范围被拒，且 **无本地逃生阀**（不能 merge `hg_unsafe`——那是安全不变量；不能本地 promote——Sequencer 拥有命令）。

**v1 缓解是 HA，不是去中心化。** 3 节点 Raft group 容忍一个节点故障而不中断 promotion。但 "trusted"（[storage design §4](2026-06-22-storage-integrity-design.md)）不等于 "highly available"：为 *safety* 信任 Sequencer ensemble 不保证其为写入的 *liveness*。本设计接受并点名的两个具体后果：

1. Raft ensemble 必须在运维上被当作写关键 infra（不是协调 infra）。它的 quorum 在写路径上。
2. 既定的逃生路径——从 L3 流 replay / 从 peer 的 `hg_safe` 拷贝（[storage design §12.5](2026-06-22-storage-integrity-design.md)）——在 Sequencer 宕机 *之后* 恢复 `hg_safe`，但它们 **不** 在 Sequencer 仍宕机时 drain `hg_unsafe`。v1 对此无修复；这是 engine 拆分的代价。

P5+ 去中心化改变的是 *谁检查、谁承担经济后果*，不是这条 liveness 算术。

---

## 10. Go 包与 API 映射

本节冻结包布局和关键 interface 签名。**只有签名**——没有实现。命名遵循仓库的 leaf-package 约定（每个子包有独立 Go 标识符以避免冲突，如同 `pkg/plugins/*`）。

### 10.1 布局

```text
pkg/sequencer/
  types/         — 新 JSON 类型（§4.2）+ 重新导出 pkg/replay 类型的别名
  state/         — replicated StateMachine：对 sequencer entry 的 Apply/Snapshot/Restore
  dedup/         — mountain-range accumulator + per-account high-water mark
  replay/        — ReplayJob 构造、分发、attestation 收集、三方检查
  promotion/     — PromotionCommand 构造 + secp256k1 签名 + CAS-base 铸造
  schema/        — schema-transition lane、barrier、DDL admission
  membership/    — ReplicaStatus 生命周期、Active read-set 准入、bootstrap gating
  consensus/     — etcd/raft Node 封装 + bbolt 支撑的 raft.Storage + Sharder
  signing/       — PromotionSigner (secp256k1) + AttestationSigner 适配器 (Ed25519)
  internal/      — 无 protobuf 的 JSON canonicalization helper（包裹 replay.canonicalDigest）
cmd/sentio-sequencer/ — 二进制：config、Raft bootstrap、gRPC/HTTP admin 面、Run(ctx)
```

### 10.2 `dedup`

```go
package dedup

// Accumulator 是 statement_id 上的 L3-derived mountain-range Merkle accumulator
//（storage design §7）。Append-only 且永久；范围 per-account-global。
type Accumulator interface {
    // Root 返回当前 spent_ids_root。
    Root() string
    // ProveNonMembership 返回 id 尚未被消费的 non-membership proof。
    ProveNonMembership(id string) (NonMembershipProof, error)
    // VerifyNonMembership 对给定 root 校验一个 proof。
    VerifyNonMembership(root string, id string, p NonMembershipProof) error
    // Add 把 id 折叠进 accumulator。仅由 state machine 在 commit 时调用。
    Add(id string) error
}

// HighWaterMark 是 per-account 的 O(1) 快路径：新的 client_seq > hi_seq[account]
// 不需要 non-membership proof；只有乱序的 client_seq ≤ hi_seq 回退到 accumulator proof。
type HighWaterMark interface {
    Get(account string) (clientSeq uint64, ok bool)
    Advance(account string, clientSeq uint64) error
    Gap(account string) []uint64 // 仍需 accumulator proof 的乱序集合
}
```

### 10.3 `state`

```go
package state

// Entry 是一个已 commit 的 Sequencer state 单元。共识核心通过 raft commit 一个
// []byte 编码的 Entry；StateMachine.Apply 解码并应用它。
// 变体：SequencerEntry（新 L3 block）、ClaimEntry（已接受 RCRecord）、
// PromotionEntry（已签发 PromotionCommand + 新 SafeSnapshotManifest）、
// SchemaEntry（SchemaTransition）、MembershipEntry（ReplicaStatus 变更）。
type Entry struct {
    Seq   uint64      `json:"seq"`
    Kind  string      `json:"kind"`
    // payload 是上面某个 Entry 变体之一，JSON 编码。
    Payload json.RawMessage `json:"payload"`
}

// StateMachine 是把已 commit Entry 应用到内存 state。
// 它是唯一允许改变子系统 1–6 已 commit 视图的东西。
// Snapshot/Restore 是共识核心压缩 Raft log 的方式。
type StateMachine interface {
    Apply(ctx context.Context, e Entry) error
    Snapshot() (Snapshot, error)
    Restore(s Snapshot) error

    // API 面使用的只读视图（从已 commit state 服务）：
    L3Block(seq uint64) (types.L3Block, bool)
    RCRecord(blockSeq, statementSeq uint64) (types.RCRecord, bool)
    CurrentSafeSnapshot() (replay.SafeSnapshotManifest, error)
    Replica(replicaID string) (types.ReplicaStatus, bool)
}
```

### 10.4 `replay`（orchestrator）

```go
package replay // pkg/sequencer/replay，与 pkg/replay 是不同 import path

// Orchestrator 构造 ReplayJob、分发它们、收集 attestation，并评估
// storage-design §9 三方谓词。它不签 promotion 命令——那是 pkg/sequencer/promotion 的活。
type Orchestrator interface {
    // BuildJob 把一个已 commit L3Block + 已接受 RCRecord + prev manifest 身份
    // 映射成 replay.ReplayJob。机械映射（§6.1）。
    BuildJob(block types.L3Block, claim types.RCRecord, prev replay.SafeSnapshotManifest) (replay.ReplayJob, error)

    // Dispatch 把 job 发给 ≥3 个 verifier replicas 并收集 attestation。
    // source 的 self-attestation 不计入 quorum。
    Dispatch(ctx context.Context, job replay.ReplayJob, replicas []string) ([]replay.ReplayAttestation, error)

    // ThreeWayPromote 对收集的 attestation 和 source RCRecord 评估 §9 谓词。
    // 返回 (ok, evidence)。绝不能退化为 root-equality：三项检查都强制（spec 守卫 §6.2）。
    ThreeWayPromote(atts []replay.ReplayAttestation, claim types.RCRecord) (PromotionDecision, error)
}

type PromotionDecision struct {
    Ok              bool
    Root            string             // computed_state_root（quorum 一致）
    PartitionDeltas []types.PartitionDelta
    ByteSideOK      map[string]bool    // part_phys_hash -> 重算匹配
    FailureReason   string             // !Ok 时填充，作为 challenge 证据
}
```

### 10.5 `promotion` 与签名方案

**两套不同签名方案，两个不同身份。** 这是本设计强化 `.omo` 草案的第二个地方，草案把签名留得含糊。

| 被签名对象 | 方案 | 身份 | 接口 | 复用 |
|---|---|---|---|---|
| `ExecutionReceipt` → `ReplayAttestation.Signature` | **Ed25519** | verifier replica（`replica_id`） | `replay.Signer`（[`pkg/replay/verifier.go:25`](../../../pkg/replay/verifier.go)） | `payloadexec.Ed25519Signer`（[`pkg/replay/payloadexec/signer.go:14`](../../../pkg/replay/payloadexec/signer.go)）已满足它 |
| `PromotionCommand.sequencer_signature` | **secp256k1** | Sequencer ensemble（一个地址） | 新 `signing.PromotionSigner` | 由 `auth.RelaySigner`（[`pkg/auth/relay_signer.go:23`](../../../pkg/auth/relay_signer.go)）支撑 |

双身份拆分是刻意的，并映射到信任模型：verifier replicas 是独立见证者，绝不能共享 key（共谋 quorum 就是威胁），所以它们得到 per-replica Ed25519 key；Sequencer 是发 *SNode 会执行的命令* 的单一信任根，它复用部署既有的 secp256k1 relay 身份（HouseGate 用于 peer 信任的同一把 key），这样 SNode 用它们已维护的 allowlist 验证 promotion 命令。

```go
package signing

// PromotionSigner 用 Sequencer 的 secp256k1 身份签 PromotionCommand。
// 由 *auth.RelaySigner 支撑。签名 payload 是
// canonicalDigest("sequencer-promotion-command", cmd 去掉 signature 的 canonical json)。
type PromotionSigner interface {
    Sign(cmd types.PromotionCommand) (types.PromotionCommand, error) // 填 sequencer_address + sequencer_signature
    Address() string
}

// PromotionValidator 在 SNode 上运行。对 allowlist 校验 sequencer_signature。
type PromotionValidator interface {
    Validate(cmd types.PromotionCommand, allowedSequencerAddresses []string) error
}

// AttestationSigner 把 verifier replica 的 Ed25519 key 适配到 replay.Signer。
// 这个 seam 让 Sequencer 侧 verifier 复用 payloadexec.Ed25519Signer 或 KMS 支撑的
// Ed25519 key 而不碰 pkg/replay。
type AttestationSigner interface {
    replay.Signer
}
```

`pkg/auth` 的 `RelaySigner` 已经暴露 `Address()` 和一把私有 secp256k1 key；`PromotionSigner` 实现是一个薄适配器，构造 canonical 签名 payload、用 relay key 签、盖 `sequencer_address`。它 **不** 复用 `SignToken`/`SignPeerLogin`（那俩分别绑 SQL/audience）——promotion 有自己的 payload schema。

### 10.6 `consensus`

```go
package consensus

// Node 包裹 go.etcd.io/raft/v3 的 raft.Node + bbolt 支撑的 raft.Storage +
// transport。v1 跑一个 group（group 0）；Sharder 是 P5+ 的 seam。
type Node interface {
    // Propose 把一个 Entry 提交给 Sharder 返回的 Raft group。
    // 阻塞到 Raft commit（或 ctx 取消）。Linearizable。
    Propose(ctx context.Context, e state.Entry) error
    // ReadIndex 服务的、对已 commit state 的 linearizable 读。
    Read(ctx context.Context, fn func(state.StateMachine) error) error
    // Status 为 admin 面暴露 Raft leader/term/health。
    Status() Status
    Run(ctx context.Context) error // 驱动 Ready 循环直到 ctx 取消
}

type Sharder interface { /* 同 §9.2 */ }
```

### 10.7 二进制

`cmd/sentio-sequencer/` 镜像 `cmd/housegate/` 的结构：flag 解析、通过 [`pkg/secretsload`](../../../pkg/secretsload) 的 age 加密 config、signal-context 接线、通过 [`pkg/metricshttp`](../../../pkg/metricshttp) 的 `/metrics` server、通过 [`pkg/log`](../../../pkg/log) 的结构化日志，然后 `sequencer.New(opts).Run(ctx)`。二进制里没有 domain 逻辑。

---

## 11. 交付分期

这些范围对照 storage design 的 P0–P4（[§14](2026-06-22-storage-integrity-design.md)）；本设计加入 Sequencer 特有的分解。

**P0 —— 冻结协议面。**
- 冻结 [§4.2](#42-本设计引入的新类型) 的 JSON wire schema 和 `json` tag。
- 导出 `replay.CanonicalDigest(domain, v)`（或加 `pkg/sequencer/internal` helper），让新 commitment 共享一个 canonicalization profile（[§4.3](#43-哈希与规范化)）。
- 冻结双签名方案（[§10.5](#105-promotion-与签名方案)）：`PromotionCommand` 签名 payload canonicalization + `PromotionValidator` 测试向量。
- mountain-range accumulator 构造 + non-membership proof 测试向量（storage-design P0 交付物）。

**P1 —— sequencer + 单 Raft + INSERT promote。**
- `pkg/sequencer/state` `StateMachine`，带 [§10.3](#103-state) 的 entry 变体。
- `pkg/sequencer/consensus` 基于 etcd-raft + bbolt 的单 group `Node`，`Sharder` 返回 group 0。
- `pkg/sequencer/dedup` accumulator + high-water mark 接进 Sequencer 子系统。
- `pkg/sequencer/replay` orchestrator：在复用的 `replay.Verifier`/`Executor` 之上 `BuildJob` + `Dispatch` + `ThreeWayPromote`。
- `pkg/sequencer/promotion` secp256k1 `PromotionSigner` + CAS-base 铸造。
- INSERT 路径端到端：submit → sequence → claim → replay → 三方 → promote → `SafeSnapshotManifest` 发布。
- 测试：单元（dedup、三方谓词正/负、CAS-base replay）、集成（3 节点 Raft，一节点故障仍保持 promotion 吞吐）。

**P2 —— bounded UPDATE/DELETE（mutation 路径）。**
- Mutation barrier、affected safe-part 发现、scratch clone。
- Mutation 第三检查（recomputed-commitment match，[§6.3](#63-insert-vs-mutation-路径)）。
- touched data 的 admission cap。

**P3 —— 加固 safe serving。**
- `pkg/sequencer/membership` 审计钩子喂给 storage-design §13 审计。
- Replica read-set health 评分。

**P4+ —— multi-Raft 分片与去中心化。**
- 把 v1 `Sharder` 替换成 per-table/account group 路由（[§9.2](#92-为什么-multi-raft-不是-v1修正草案)）。
- 去中心化阶段的 challenge window（[storage design §11](2026-06-22-storage-integrity-design.md)）。

---

## 12. Open questions

1. **L3/RC JSON wire 字段**（[storage design §15 Q4](2026-06-22-storage-integrity-design.md)，收窄）：[§4.2](#42-本设计引入的新类型) 的 sketch 冻结了形状；确切的 JSON 字段集合、optional-vs-required、`omitempty` 策略是 P0。
2. **Payload DA**（[storage design §15 Q6](2026-06-22-storage-integrity-design.md)）：支撑 `replay.Verifier` 的 `PayloadStore` 实现是共识核心的 replicated state、外部 DA 层、还是两者加 fallback？影响 `pkg/sequencer/replay` 分发。
3. **Chain commitment**（[storage design §15 Q5](2026-06-22-storage-integrity-design.md)）：L2 anchor 存完整 L3 block payload、DA 引用、还是只存 block/root commitment？影响 `promotion.Publish`。
4. **Raft transport**（[§9.1](#91-v1--单-raft-group选定路径)）：Sequencer 间 Raft 消息用 gRPC transport，还是原生 etcd-raft `Transport`？P1 决策。
5. **Snapshot 频率**（[§9.1](#91-v1--单-raft-group选定路径)）：`StateMachine.Snapshot()` 对 bbolt log compaction 的节奏。在 Raft log 长度与 snapshot 成本之间权衡。
6. **zh-CN 镜像**（[约定](#)）：按仓库双语 spec 约定，P0 字段冻结后从英文源生成 `2026-06-30-storage-integrity-keeper-design.zh-CN.md`，标识符保留英文。可选；不影响英文事实源。

---

## 验证

本文档对已批准计划中的要求自检。接受 grep：

- **必须包含** —— 以下三组（以上均已呈现）：
  - component-boundary terms：`Sentio Sequencer`、`ClickHouse Keeper`、`Go`、`C++`、`ZooKeeper-compatible`
  - sequencing terms：`pkg/sequencer`、`statement_seq`、`statement_id`、`multi-Raft`
  - promotion/replay terms：`Ed25519`、`secp256k1`、`REPLACE PARTITION`、`three-way`、`SafeSnapshotManifest`、`ReplayAttestation`、`PromotionCommand`
- **必须不包含** —— ClickHouse fork/rewrite plan、root-only promotion predicate，或任何把 C++ coordination service 说成 promotion authority 的措辞（均无；两个 Keeper 角色在 [§1](#1-定位与术语) 分离，且从未再被混用）。

## 13. 参考文献

- [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) —— 父 spec；本文档分解其 §5.2 的 Sequencer 角色（该文档早期措辞中写作 "Keeper"；见顶部[命名说明](#)）。
- [2026-06-10 multi-replica trust design](2026-06-10-multi-replica-trust-design.md) —— row-id profile、ledger equation、accumulator 背景。
- [`pkg/replay`](../../../pkg/replay) —— verifier 核心 + 原样复用的 manifest/receipt/attestation 类型（[§4.1](#41-从-pkgreplay-复用不要重定义)）。
- [`pkg/lthash`](../../../pkg/lthash) —— `part_row_lthash` 和 `partition_commitment` 使用的 LtHash 算术。
- [`pkg/auth`](../../../pkg/auth) —— `RelaySigner` 支撑 `PromotionSigner`（[§10.5](#105-promotion-与签名方案)）。
- [`pkg/replicationproxy`](../../../pkg/replicationproxy) —— 可选的 L4 ClickHouse-Keeper 透传；网络隔离，非 integrity。
- [`etcd-io/raft`](https://github.com/etcd-io/raft) —— v1 构建于其上的 Raft 库（[§9](#9-共识与-ha)）。
- [etcd-dev MultiRaft 讨论](https://groups.google.com/g/etcd-dev/c/cq88rpcxvm8) —— 为何 multi-Raft 非该库原生（[§9.2](#92-为什么-multi-raft-不是-v1修正草案)）。
- [TiKV multi-Raft 设计](https://www.pingcap.com/blog/design-and-implementation-of-multi-raft/) 与 [CockroachDB scaling-raft](https://www.cockroachlabs.com/blog/scaling-raft/) —— 延后到 P5+ 的 multi-Raft 工作的先前技术。
- [ClickHouse Keeper 文档](https://clickhouse.com/docs/guides/sre/keeper/clickhouse-keeper) —— 那个不相关的 C++ 协调服务（[§1.1](#11-clickhouse-keeper不是我们不是-go)）。
