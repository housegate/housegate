# DA 层数据同步 MVP — 设计文档

**日期：** 2026-05-25
**状态：** Draft
**作者：** poetry, Claude

> 这是 `2026-05-25-da-mvp-design.md` 的中文版本，两版内容等价。
> 本设计取代 [`2026-05-12-data-reliability-design.md`](./2026-05-12-data-reliability-design.md)
> 中 L1（S3-backed MergeTree）作为持久性候选方案的位置，最终是否替代取决
> 于本文 §7 定义的测量结果。

## 1. 目标

用单人两周时间，做一个**端到端的 prototype**：在 source CH 上写入的数据
被发布到 DA 层（Celestia Mocha-4 测试网），一个全新 CH 实例只靠 DA blob
就能被重建出来。这个 MVP 存在的唯一意义是**采集三组测量数据** —— 稳态
吞吐、端到端延迟、$/GB 成本 —— 用以决策 DA 能否承担 housegate 的数据
面持久层（替代上一份可靠性文档里 L1 的 S3 方案）、做 commitment 辅助、
还是两条都不行。

**这不是生产代码，是测量台。** 之所以要建是因为公开资料里关于 DA 层在
持续 ClickHouse 负载下的吞吐、延迟、$/GB 的数据太稀薄，无法靠分析独立
判断。我们需要从一个真实负载里跑出的数。

## 2. 非目标

为了诚实地装进两周的预算，下面这些**显式**不在 MVP 范围。每一项都是真
实需要解决的问题；它们只有在 MVP 拿到 go/no-go 之后才会进入 in-scope：

- **不做实时同步。** publisher 以 60s 周期 poll `system.parts`，写入即发
  的 hook 留作后续工作。
- **不集成 housegate plugin。** publisher 和 rebuilder 是独立 CLI（sidecar）。
  接进 proxy plugin chain 留作后续。
- **不处理 DDL / schema 演化。** publisher 启动时锁定 schema，期间任何
  漂移都直接 abort。`ALTER TABLE`、`DROP COLUMN` 都留作后续。
- **不处理多表 / 多 DB 协同。** 单表，单 publisher 进程。fan-out 留作
  后续。
- **publisher 不做高可用。** 单进程、单 checkpoint 文件。lease / 选主
  留作后续。
- **不做拜占庭防御。** 完整性只用每 blob 一个 blake3 hash 写到 anchor
  合约里；Merkle proof 留作后续。
- **不做经济模型 / staking。** 谁付 DA gas、谁罚 publisher，全部后续。
- **不做数据保密性。** Celestia blob 是公开的；本设计假定被索引的链上
  数据本来就是公开的，见 §10 Q1。

## 3. 关键设计决策

每一行是选择 + 一句理由。被淘汰的备选见 §11。

| 决策点 | 选择 | 理由 |
|---|---|---|
| DA 层 | **Celestia Mocha-4 测试网** | Go SDK 成熟、PayForBlobs spec 稳定、测试 TIA 免费；后续可在 EigenDA / Avail 上重跑同一套 harness 做对比 |
| 集成方式 | **Sidecar CLI 进程（不是 housegate plugin）** | MVP 测的是 DA，不是集成。避免动 proxy 热路径，团队可以并行迭代 |
| 数据形态 | **Parquet**（`SELECT … FORMAT Parquet`） | CH 原生双向支持，跨 CH 版本稳定，压缩好，语言无关 |
| 代码位置 | housegate repo 下 `tools/da-mvp/`，**两个独立 binary** 分别构建在 `tools/da-mvp/cmd/da-publisher/` 和 `tools/da-mvp/cmd/da-rebuilder/`，**不**作为 `housegate` 主 binary 的子命令 | 把实验代码挡在生产 binary 之外；同时仍共用 Bazel 构建 / CI / 工程约定。MVP 升舱时 binary 可挪到 `cmd/`，库代码挪到 `pkg/da/` |
| Anchor 链 | **本地 anvil + 一个 Solidity 合约** | MVP 不需要真链；合约接口设计为未来直接部署到生产 housegate 注册中心链 |
| 同步语义 | **异步 batch，60s poll** | MVP 测吞吐 / 成本，不测延迟下限；batch 摊薄 PFB gas |
| 完整性 | **每 blob 一个 blake3 hash，写到 anchor** | rebuilder 能验从 DA 读到的内容；Merkle commitment 留作后续 |

## 4. 架构

五个组件：source CH、publisher、Celestia 测试网、本地 anvil 上的 anchor
合约、rebuilder、target CH。数据流刻意做成单向 —— rebuilder 不回流任何
东西到 source。

```
┌────────────────┐                              ┌────────────────────────┐
│   Source CH    │                              │  Celestia Mocha-4 DA   │
│   (写入)        │                              └────────────┬───────────┘
└────────┬───────┘                                           ▲
         │ ① SELECT … FORMAT Parquet                         │ ③ PFB(blob)
         ▼                                                   │
   ┌─────────────┐                                           │
   │ da-publisher│───────────────────────────────────────────┤
   │  (sidecar)  │                                           │
   └─────┬───────┘                                           │ ⑤ GetBlob
         │ ④ Anchor.publish(commit, hash, seq, …)            │
         ▼                                                   │
   ┌──────────────────────┐                                  │
   │  anvil + DAAnchor.sol │◀── ⑥ queryFilter(Published) ────┤
   └──────────────────────┘                                  │
                                                  ┌──────────┴──────┐
                                                  │  da-rebuilder   │
                                                  │     (CLI)       │
                                                  └────────┬────────┘
                                                           │ ⑦ INSERT … FORMAT Parquet
                                                           ▼
                                                  ┌─────────────────┐
                                                  │    Target CH    │
                                                  │   (rebuild)     │
                                                  └─────────────────┘
```

顺序不变量：publisher **先**写 Celestia PFB，**再**调用 anchor
`publish()`。所以任何在链上可见的 anchor 都对应一个已经在 DA 上 commit
的 blob。rebuilder 按 `partSeq` 顺序消费 anchor，行为是确定的。

## 5. 组件

### 5.1 `da-publisher`

独立 binary，由 `tools/da-mvp/cmd/da-publisher/main.go` 编译，用 cobra
解析 flag（风格跟现有 `housegate secret-*` 子命令一致，但是独立 binary
而非子命令）。调用形式：`da-publisher --source-ch … …`。flag / 配置：

```
--source-ch <DSN>              # tcp://host:9000?username=…&password=…
--database <name>              # source database
--table <name>                 # source table
--celestia-rpc <URL>           # 本地 light-node RPC，例如 http://localhost:26658
--celestia-token <token>       # light node 启动后从 config 取
--celestia-namespace <hex>     # 10 字节 namespace，见 §6
--anchor-rpc <URL>             # anvil JSON-RPC
--anchor-contract <addr>       # 已部署的 DAAnchor 地址
--anchor-private-key <hex>     # 用于 anchor tx 的签名 key
--interval 60s                 # poll 周期
--checkpoint-file ./pub.state  # JSON 文件：{ last_modification_time, last_part_seq }
--metrics-listen :9100         # Prometheus /metrics
```

主循环（目标 < 300 行 Go）：

```
checkpoint := load(checkpointFile)              // { lastModTime, lastPartSeq }
schemaHash := blake3(canonical(SHOW CREATE TABLE db.tbl))
for {
    parts := query(`
        SELECT name, modification_time, bytes_on_disk
        FROM system.parts
        WHERE database=? AND table=? AND active=1
              AND modification_time > ?
        ORDER BY modification_time, name`,
        database, table, checkpoint.LastModTime)

    for _, p := range parts {
        parquet := exportPart(sourceCH, database, table, p.Name)   // SELECT WHERE _part=… FORMAT Parquet
        hash := blake3(parquet)
        chunks := split(parquet, maxBlobSize)                       // 见 §6
        var partSeq uint64

        for i, c := range chunks {
            height, commitment := celestia.SubmitPayForBlob(namespace, c)
            if i == 0 {
                // 第一个 chunk 由合约分配 partSeq
                partSeq, _ = anchor.publish(dbId, tableId,
                    height, commitment, len(c), hash, schemaHash,
                    uint8(0), uint8(len(chunks)))
            } else {
                // 后续 chunk 复用同一个 partSeq
                anchor.publishChunk(dbId, tableId, partSeq,
                    height, commitment, len(c), hash, schemaHash,
                    uint8(i), uint8(len(chunks)))
            }
        }

        checkpoint.LastModTime = p.ModificationTime
        save(checkpointFile, checkpoint)
    }
    sleep(interval)
}
```

**Prometheus 指标**（注册到 `housegate_da_publisher_…`）：

| 指标 | 类型 | 备注 |
|---|---|---|
| `parts_published_total` | counter | 每成功 anchor 一个 part 加一 |
| `blobs_published_total` | counter | 每次 Celestia PFB 加一（chunks ≥ parts） |
| `bytes_published_total` | counter | 未压缩 Parquet 字节 |
| `publish_lag_seconds` | gauge | `now() − checkpoint.LastModTime` |
| `celestia_submit_seconds` | histogram | PFB latency |
| `anchor_submit_seconds` | histogram | anchor tx 确认 latency |
| `celestia_errors_total{kind}` | counter | 提交错误，按类别 |

**失败 / 重启：** Celestia 或 anvil 错误用 linear backoff（1 s → 30 s 封
顶）。进程崩溃后从 `checkpoint.LastModTime` 续 —— 即使重发已经在 DA 上
的 part 也是安全的，因为 `partSeq` 由 anchor 合约分配，rebuilder 看到的
总是单调递增。

**每表一个 publisher 的契约。** MVP 假设每个 `(database, table)` 只有
一个 publisher 进程。两个 publisher 同时跑会对同一份源数据发出 partSeq
不同的重复 anchor。lease / 选主留作后续。

### 5.2 `da-rebuilder`

独立 binary，由 `tools/da-mvp/cmd/da-rebuilder/main.go` 编译，flag 跟
publisher 对称：

```
--target-ch <DSN>
--database <name>
--table <name>
--celestia-rpc <URL>
--celestia-token <token>
--celestia-namespace <hex>
--anchor-rpc <URL>
--anchor-contract <addr>
--since-seq 0
--verify-after { count | sample | full }
```

主循环：

```
expectedSchemaHash := blake3(canonical(SHOW CREATE TABLE db.tbl))
since := flag.SinceSeq
for {
    anchors := anchor.QueryFilterPublished(dbId, tableId, since)
    if len(anchors) == 0 { break }       // 或 sleep + poll（如 --follow）

    groups := groupByPartSeq(anchors)    // 同一 part 的 chunks 共享 partSeq，
                                         // 用 chunkCount 确认完整

    for _, g := range groups in seq order {
        if g.SchemaHash != expectedSchemaHash {
            abort("schema drift detected at partSeq=%d", g.PartSeq)
        }
        blob := assemble(fetchChunks(g))                 // 每个 chunk 一次 Celestia GetBlob
        if blake3(blob) != g.BlobHash { abort("blob hash mismatch") }
        exec(targetCH, "INSERT INTO db.tbl FORMAT Parquet", blob)
        since = g.PartSeq + 1
    }
}
verify(targetCH, sourceCH, mode=flag.VerifyAfter)
```

**Verification 模式**（重建后正确性校验）：

- `count`: 两边 `SELECT count() FROM db.tbl` 一致。
- `sample`: 两边 `SELECT * FROM db.tbl ORDER BY <pk> LIMIT 1000 OFFSET 0/中间/末尾` 一致。
- `full`: 两边 `SELECT cityHash64(toString(t)) FROM db.tbl ORDER BY <pk>` 聚合一致 —— 全表行级 hash。

`full` 仅适用小表；`sample` 是默认。

### 5.3 `DAAnchor.sol`

单一合约，连同 import 大约 50 行：

```solidity
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract DAAnchor {
    event Published(
        bytes32 indexed dbId,
        bytes32 indexed tableId,
        uint64  partSeq,
        uint64  daHeight,
        bytes32 daCommitment,
        uint32  blobSize,
        bytes32 blobHash,
        bytes32 schemaHash,
        uint8   chunkIdx,
        uint8   chunkCount
    );

    // 每个 (dbId, tableId) 的单调序列分配器。
    mapping(bytes32 => uint64) private _nextSeq;

    function publish(
        bytes32 dbId,
        bytes32 tableId,
        uint64  daHeight,
        bytes32 daCommitment,
        uint32  blobSize,
        bytes32 blobHash,
        bytes32 schemaHash,
        uint8   chunkIdx,
        uint8   chunkCount
    ) external returns (uint64 seq) {
        bytes32 key = keccak256(abi.encode(dbId, tableId));
        // 同一 part 的 chunks 共享 partSeq；仅 chunkIdx=0 时递增
        if (chunkIdx == 0) {
            seq = _nextSeq[key]++;
        } else {
            // 后续 chunks 必须传入 chunkIdx=0 时拿到的 seq。
            // MVP 通过单独的 helper 暴露：
            revert("MVP: use publishChunk(seq, …) for chunkIdx > 0");
        }
        emit Published(
            dbId, tableId, seq, daHeight, daCommitment,
            blobSize, blobHash, schemaHash, chunkIdx, chunkCount
        );
    }

    function publishChunk(
        bytes32 dbId, bytes32 tableId, uint64 seq,
        uint64 daHeight, bytes32 daCommitment,
        uint32 blobSize, bytes32 blobHash, bytes32 schemaHash,
        uint8 chunkIdx, uint8 chunkCount
    ) external {
        emit Published(
            dbId, tableId, seq, daHeight, daCommitment,
            blobSize, blobHash, schemaHash, chunkIdx, chunkCount
        );
    }
}
```

部署产物：`tools/da-mvp/contracts/` 下的 Foundry / Hardhat 工程。编译后
的 ABI 以 JSON 形式 check-in，Go publisher / rebuilder 不需要 solc 构建
依赖。

MVP 不做访问控制 —— anvil 本地跑，生产版用 housegate 注册中心已有的鉴权。

### 5.4 Schema 处理（最小化）

publisher 和 rebuilder 都在启动时计算
`schemaHash = blake3(canonicalize(SHOW CREATE TABLE db.tbl))`。
`canonicalize` 去掉空白、注释，以及不影响列布局的引擎参数
（`storage_policy`、`ttl` 等）—— 只 hash 列定义和类型。

- Publisher：运行中 `schemaHash` 变化（罕见，需要 online ALTER）则
  abort，要求重新拿 checkpoint。
- Rebuilder：anchor 里的 `schemaHash` 跟本地 `expectedSchemaHash` 不一致
  则在该 anchor 处 abort，并报告 partSeq。

跨 schema 变化的 DDL 重放属于 **MVP 之外**；MVP 只能在 schema 稳定的窗
口内重建。

## 6. DA 层细节 —— Celestia Mocha-4

- **单 PFB blob 上限：** ~1.97 MiB。MVP 目标 **1.5 MiB** 每 chunk，留余量。
- **Block time：** ~12 秒。
- **Block 容量：** mainnet ~1.4 MiB（Mocha-4 更宽松，但外推用 mainnet）。
- **网络共享吞吐：** mainnet ~117 KB/s 总吞吐 —— 跨**所有** Celestia
  用户共享。这就是 Experiment A 要打的关键数。
- **Namespace：** 10 字节。MVP 的分配方案：
  `0x68676d76 || <blake3(dbId||tableId) 的 6 字节>` —— `0x68676d76` 是
  ASCII `"hgmv"`（housegate MVP）。
- **Light node：** `celestia light start --p2p.network mocha --core.ip
  <consensus RPC> --rpc.skip-auth=false`。auth token 启动后从
  `~/.celestia-light-mocha-4/keys/auth.token` 取。
- **Go SDK：** `github.com/celestiaorg/celestia-node/api/rpc/client` ——
  `Blob.Submit(ctx, []*blob.Blob{…}, …)` 返回 `(height uint64, error)`，
  `Blob.Get(ctx, height, namespace, commitment)` 拉回 blob。

## 7. 测量计划 —— MVP 的真正交付物

建这个 prototype 唯一意义是填出下面三张表。每个数据点跑 ≥ 2 小时来
平滑块时间噪声和 gas 价格抖动。

### 7.1 Experiment A —— 吞吐

目标：找到稳态 INSERT 速率，在此速率下 `publish_lag_seconds` 不再增长。

- 负载：`loadgen` 工具按 `{100, 1k, 10k, 100k}` rows/s 阶梯发送
  `INSERT INTO db.tbl VALUES (…)`。行 schema 模拟典型 indexer 表：约 12
  列，混合 `String`、`UInt64`、`Decimal128`、`DateTime64`。
- 测：
  - 每个速率下 `publish_lag_seconds` 随时间的曲线
  - `bytes_published_total` 的导数 → MB/s
  - Celestia block 占用率：(我们的 blob 字节) / (block 容量)
- 输出：rate-vs-lag 曲线、MB/s-vs-rate 曲线、每个速率下占 Mocha block
  容量的比例。

### 7.2 Experiment B —— 延迟

目标：从 source `INSERT` 可见到 target `SELECT` 可见的端到端 latency。

- 注入带 `inject_ts DateTime64(6)` 列的行，值是插入时的 wall clock。
  rebuilder 以 `--follow` 模式持续跟。
- 在 target 上测：`now64() - inject_ts` 的 median / P50 / P95 / P99。
- 按段拆解：
  - source CH part flush 延迟
  - publisher poll cycle
  - Celestia commit（≈ 12 s + finality）
  - rebuilder fetch + INSERT
- 输出：堆叠 latency-breakdown 图。

### 7.3 Experiment C —— 成本

目标：每 GB 已发布数据的 $ 成本，从 Mocha 测试网 TIA 消耗外推到
mainnet TIA 价格。

- 在 24+ 小时内发布 100 GB 代表性数据。
- 统计：消耗的测试网 TIA、anvil gas（对成本无关，但展示 tx overhead）。
- 外推：mainnet TIA 现货价 × 统计 → $/GB。
- 对比：
  - S3：$0.023/GB-month + PUT 成本
  - 假想 Foundation 自运维 Keeper 集群：$X/月估算 / 全网总 GB

### 7.4 输出物

一份后续文档
`docs/superpowers/specs/2026-XX-XX-da-mvp-report.md`，包含三组实验的表
格和对照 §8 的明确 go/no-go 建议。

## 8. Go / no-go 决策树

后续报告的结论会按下面这棵树推：

```
Q1: 在典型 indexer 负载下（例如稳定 100k rows/s），
    publish_lag_seconds 是否收敛？
  ├── 是 → Q2
  └── 否 → DA 不能做数据面。
           ├── 退路 A：DA 仅承担 L4 commitment（发 Merkle root）。
           └── 退路 B：放弃 DA，转 Keeper + RMT
                      （见上一份可靠性 spec，作为 L1 的替代）。

Q2: 外推 mainnet $/GB 是否在 S3 的 10× 之内？
  ├── 是 → Q3
  └── 否 → 同样走 Q1 的退路 A / B。

Q3: P99 端到端延迟是否小于 5 分钟？
  ├── 是 → DA 可作为主力数据面候选。
  └── 否 → DA 仅适合做 commitment + 选定 hot DB 全量发布；
          cold DB 走 Keeper+RMT 或现有 S3 路径。
```

任一 Q 答否都会汇到一条具体退路。MVP 不能回答生产部署的所有问题 —— 这
棵树是刻意收窄的。

## 9. 实施时间表（2 周，1 工程师）

| Day | 任务 | 交付 |
|---|---|---|
| 1 | Celestia light node 起；Go 里跑通 PFB submit/get demo | demo binary 提交一个 blob 并读回 |
| 2-3 | `da-publisher` 主循环 + checkpoint | 单 part 端到端发布 |
| 4 | blob 切片 + anchor `publishChunk` 联通 | > 1.5 MiB 的 part 正确拆分 |
| 5-6 | `da-rebuilder` | 一张表在 target 上完整重建 |
| 7 | `DAAnchor.sol`、Foundry 工程、CI 里集成 anvil | ABI JSON check-in；集成测试绿 |
| 8-9 | `loadgen` + Experiment A | 吞吐数据表 |
| 10 | Experiment B + Experiment C | 延迟 + 成本表 |
| 11-12 | 报告 + go/no-go 提案 | 后续 spec commit |

时间紧；§2 列出的"显式不做"是这套设计能装进两周的代价。

## 10. MVP 之外（不在范围，但留路标）

如果 go/no-go 全部为是（Q1+Q2+Q3 都过），生产路径沿下面几条轴展开。
任何一项都不阻塞 MVP。

- **Publisher 嵌入 housegate。** 把 publisher 移成 `daPublisher` plugin，
  在 rewrite 之后 hook Data-block 流。去掉 poll 周期；延迟下限降到
  "part 一封箱就发"。
- **多 indexer 协同。** 决定每个 indexer 各自独立发布（浪费但 trustless），
  还是按 `(database, table)` 选出一个 publisher 由其他 indexer 验证。
- **DDL 重放。** anchor `Published` 旁边记 schema-change 事件；rebuilder
  在恢复数据回放前先在 target 上应用 DDL。
- **Fraud-proof / Merkle commitments。** 每 blob 加一个行级 Merkle root；
  允许低成本 challenge rebuilder 的错误输出。
- **Publisher 质押 / 罚没。** publisher 抵押 bond，被证明的错报会罚没。
- **Anchor 合约迁移。** `DAAnchor.sol` 重部署到生产 housegate 注册中心
  链；接进 `commitgate`，让 proxy 拒收 anchor 落后过远的写入。
- **DA 选型。** 如果 Mocha 数据让 Celestia 不合格，把同一套 harness 拿
  到 EigenDA 和 Avail 上重跑。MVP 的接口层面是有意 DA 无关的。

## 11. 备选方案（为什么 MVP 没选）

- **Day 1 就把 publisher 做成 housegate plugin。** 延迟更紧但把 DA
  验证耦合到 plugin chain 的迭代。MVP 测的是 DA 不是集成，sidecar 胜。
- **直接发原始 INSERT SQL 字符串，不发 Parquet。** INSERT-heavy 负载下
  blob 更小，但 `now()` / `generateUUIDv4()` 等非确定函数会让回放结果
  错。Parquet 拿到的是**已物化**的行状态，重放安全。
- **发原始 CH part 文件（.bin / .mrk / 等）。** 存储最省，但绑死 CH
  版本和引擎内部；对一个测量台来说太脆。
- **MVP 里就用 Avail 或 EigenDA 而不是 Celestia。** 两者都可行；Celestia
  在 OSS 路径上最成熟、起步成本最低。DA 层对比留作后续。
- **anchor 合约直接部署到公开 testnet。** 可以，但 anvil 更快、更可重
  现，把实验跟 testnet 的不稳定隔离开。
- **用 operator 持有的 key 加密 blob。** 引入 key 管理复杂度。本设计
  假定 indexer 数据是公开的（§2、§10 Q1）；锁定该假定前需要团队确认。

## 12. Open questions

1. **数据保密性。** Celestia blob 是公开的 —— 任何人都能读。housegate
   索引的是链上数据，本身就是公开的，所以应该没问题 —— 但 MVP 开工前
   需要**显式**确认。如果范围内有任何一个数据集不能公开，MVP 就需要
   加一层 AEAD 包装。
2. **DA 层对比的节奏。** Mocha-4 数字出来之后是否要对 EigenDA 和 Avail
   重跑？建议：仅当 Mocha-4 的结果是"接近可行但不完全"才跑三个；这是
   额外一周工作。
3. **单 publisher 假设。** MVP 每张表只有一个 publisher。生产很可能需
   要 N 个 indexer 都能发布（独立性强）—— 但重复的 blob 会让 Experiment
   C 的成本数失真。一旦模式确定下来，成本数需要按 per-publisher 和
   per-network-total 两个维度重新陈述。
4. **持续负载下的 anchor 合约 gas。** anvil 是免费的，但生产链上每次
   anchor `publish()` 都要 gas。估算是机械的，留作后续；但 MVP 要记录
   anchor tx 数量，方便后续外推干净。

---

**驱动决策的问题（请团队提前回答）：** §7 拿出数后，如果 Q1/Q2/Q3 任
一为 "否"，哪一条退路（退路 A：commitment-only DA；退路 B：Keeper +
RMT）成为默认？**在 MVP 完成之前**回答这个，可以省一次往返，让报告
的建议落得干净。
