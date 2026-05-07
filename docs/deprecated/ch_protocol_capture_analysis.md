# ClickHouse Protocol Capture Analysis: CH1 (19000) → CH2 (29000) remote() Query

> **Date**: 2026-03-02  
> **Test query**: `SELECT 1 FROM remote('127.0.0.1:29000', 'system', 'one', 'default', '')`  
> **Executed on**: CH1 (port 19000), targeting CH2 (port 29000)

## Summary of Key Findings

| Finding | Detail |
|---------|--------|
| **Query format** | **普通 SELECT**，不是 `remote()` |
| **Hello 中的 database** | **空字符串** `""` |
| **数据库信息在哪里** | 在 SQL body 中以 `system`.`one` 全限定名出现 |
| **查询类型标记** | `SECONDARY_QUERY` (type=2)，表明这是一个子查询 |
| **Client name** | `"ClickHouse server"` (不是 "ClickHouse client") |
| **Settings 传播** | 是的，settings 通过 Query 包传播 |

## Complete Packet Exchange Timeline

```
CH1 (19000)                                     CH2 (29000)
    |                                               |
    |  ===== Connection 1: TablesStatus Check =====  |
    |                                               |
    |--- TCP SYN ---------------------------------->|
    |<-- TCP SYN-ACK -------------------------------|
    |--- TCP ACK ---------------------------------->|
    |                                               |
    |--- [Packet 1] Client Hello ------------------>|  34 bytes
    |<-- [Packet 2] Server Hello -------------------|  95 bytes
    |--- [Packet 3] Client Addendum --------------->|  24 bytes
    |<-- [Packet 4] Server Addendum? --------------->|  14 bytes
    |                                               |
    |--- [Packet 5] DESC TABLE system.one --------->|  314 bytes (Query type=1)
    |<-- [Response] Table structure (columns) -------|  ~123 bytes
    |<-- [Data blocks]  ←  table desc results -------|
    |<-- End of data ------- ------------------------|
    |                                               |
    |--- [Packet 6] TablesStatusRequest ----------->|  13 bytes
    |<-- [Packet 7] TablesStatusResponse ------------|  14 bytes
    |                                               |
    |--- [Packet 8] SELECT 1 AS `1` FROM ... ------>|  341 bytes (Query type=1)
    |--- [Data Block] _shard_count (UInt32) -------->|  74 bytes
    |--- [Data Block] empty block ----------------->|  40 bytes
    |<-- [Response] result data ---------------------|
    |<-- End of data --------------------------------|
    |                                               |
    |--- FIN ---------------------------------------->|
    |<-- FIN ----------------------------------------|
    |                                               |
    |  ===== Connection 2: Health check (empty) ==== |
    |  ===== Connection 3: Health check (empty) ==== |
```

## Detailed Packet Analysis

### Packet 1: Client Hello (CH1 → CH2)

```
Offset  Bytes               Decoded
------  ----                -------
0x00    00                  Packet type = 0 (Hello)
0x01    11 "ClickHouse server"  Client name = "ClickHouse server" ← 注意！不是 "ClickHouse client"
0x13    1a                  Version major = 26
0x14    02                  Version minor = 2
0x15    d3a903              Revision = 54483 (VarInt)
0x18    00                  Database = "" ← ⚠️ 空字符串！
0x19    07 "default"        User = "default"
0x21    00                  Password = "" (empty)
```

**🔑 关键发现 1**: Hello 包中 `database` 字段是**空字符串**，不是 `"system"`。

数据库信息不在 Hello 中传递，而是完全通过 SQL body 中的全限定表名来指定。

### Packet 2: Server Hello (CH2 → CH1)

```
Offset  Bytes               Decoded
------  ----                -------
0x00    00                  Packet type = 0 (ServerHello)
0x01    0a "ClickHouse"     Server name
0x0c    1a                  Version major = 26
0x0d    02                  Version minor = 2
0x0e    d3a903              Revision = 54483
0x11    05 "Etc/UTC"        Timezone
0x18    0d "graph-indexer"  Display name
        ...                 Nonce, password complexity rules, etc.
```

### Packet 3: Client Addendum

```
Protocol addendum after ServerHello exchange.
Contains: quota_key="notchunked", token="notchunked" (or similar fields)
```

### Packet 5: First Query — DESC TABLE (表结构探测)

```
Query packet (type=1):
  Query ID:     "8dd534ce-e5f3-47e3-8aba-e76a1b49effc"
  Query Type:   SECONDARY_QUERY (type=2) ← CH1 标记为子查询
  User:         "default"
  Address:      "127.0.0.1:57468" (CH1 的 client 地址)
  OS User:      "sentio"
  Hostname:     "graph-indexer"
  Client:       "ClickHouse client" 26.2 rev 54483
  
  Settings:
    queue_max_wait_ms          = "0"
    max_result_rows            = "0"
    max_result_bytes           = "0"
    describe_compact_output    = "0"
    allow_experimental_analyzer = "1"
  
  Stage:        Complete (2)
  Compression:  enabled
  
  *** SQL: DESC TABLE system.one ***
```

**🔑 关键发现 2**: CH1 首先发送 `DESC TABLE system.one` 来获取表结构，然后才发送实际查询。

### Packet 6: TablesStatusRequest

```
Packet type: 5 (TablesStatusRequest)
Number of tables: 1
  Table 1: database='system', table='one'
```

**数据库+表信息通过 TablesStatusRequest 显式传递**，格式为 (database, table) 对。

### Packet 8: The Actual SELECT Query (核心查询)

```
Query packet (type=1):
  Query ID:     "8dd534ce-e5f3-47e3-8aba-e76a1b49effc" (同一个 Query ID)
  Query Type:   SECONDARY_QUERY (type=2)
  User:         "default"
  Address:      "127.0.0.1:57468"
  OS User:      "sentio"
  Hostname:     "graph-indexer"
  Client:       "ClickHouse client" 26.2 rev 54483
  
  Settings:
    queue_max_wait_ms            = "0"
    skip_unavailable_shards      = "0"
    allow_experimental_analyzer  = "1"
  
  Stage:        Complete? (需确认)
  Compression:  enabled

  *** SQL: SELECT 1 AS `1` FROM `system`.`one` AS `__table1` ***
```

后续还发送了两个 Data Block：
- `_shard_count` (UInt32) — shard 总数
- `_shard_num` (UInt32) — 当前 shard 编号

这些是 ClickHouse 分布式查询的内部变量。

## 核心结论

### 1. 查询格式：普通 SELECT，不是 remote()

**CH2 收到的是一个普通的 `SELECT` 语句**，而不是 `remote()` 函数调用。

```
原始查询 (在 CH1 上执行):
  SELECT 1 FROM remote('127.0.0.1:29000', 'system', 'one', 'default', '')

CH2 实际收到的查询:
  SELECT 1 AS `1` FROM `system`.`one` AS `__table1`
```

CH1 在本地解析了 `remote()` 函数，提取出:
- 目标地址 → 用于建立 TCP 连接
- 数据库和表 → 放入 SQL body 的全限定表名
- 用户/密码 → 放入 Hello 包

### 2. Hello 包中的 database 字段为空

```
Hello.database = ""  (空字符串)
```

数据库信息**完全不通过 Hello 传递**，而是在 SQL body 中以全限定名 `` `system`.`one` `` 出现。

### 3. 查询被标记为 SECONDARY_QUERY

```
initial_query_type = 2 (SECONDARY_QUERY)
```

这告诉 CH2 这不是用户直接发起的查询，而是来自另一个 ClickHouse 节点的二级/子查询。

### 4. Settings 通过 Query 包传播

多个 settings (`queue_max_wait_ms`, `skip_unavailable_shards`, `allow_experimental_analyzer` 等) 通过 Query 包的 Settings 段传播到 CH2。

### 5. 查询流程是两阶段的

1. **阶段一**: `DESC TABLE system.one` — 获取表结构
2. **阶段二**: `SELECT 1 AS \`1\` FROM \`system\`.\`one\` AS \`__table1\`` — 执行实际查询

两个阶段共用同一个 TCP 连接和同一个 Query ID。

### 6. Client Name 标识

| 位置 | Client Name |
|------|------------|
| Hello 包 | `"ClickHouse server"` ← CH1 以服务器身份连接 |
| Query ClientInfo | `"ClickHouse client"` ← 原始客户端信息 |

## 对 Proxy 设计的影响

### 路由决策

1. **Proxy 不需要解析 SQL 来做路由**。Hello 包中的 database 字段是空的。
2. **路由信息必须通过其他机制传递**（比如 custom settings、请求头中的标记等）。
3. **TablesStatusRequest 包含了 database 和 table 信息**，如果 proxy 需要做表级路由，可以解析这个包。

### 查询重写

如果 proxy 需要修改数据库名，需要修改：
1. ❌ Hello 中的 database（为空，不需要修改）
2. ✅ **SQL body 中的全限定表名** — `system`.`one` 需要重写 
3. ✅ **TablesStatusRequest 中的 database 字段**
4. ✅ **DESC TABLE 查询中的表名**

### SECONDARY_QUERY 标识

`initial_query_type = 2` 可以让 proxy 识别这是来自另一个 CH 节点的子查询，而不是直接客户端查询。这对于实现 proxy 路由逻辑很重要。
