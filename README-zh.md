# House Gate

一个轻量级的 ClickHouse native TCP 协议代理。它透明地坐落在客户端和 ClickHouse 之间，提供 **查询审计**、**JWS 鉴权**、**SQL 重写**、**动态 upstream 路由** 以及 **Prometheus 监控** 能力。

---

## 目录

- [前置依赖](#前置依赖)
- [快速开始](#快速开始)
- [构建](#构建)
- **Part 1 — 配置**
  - [配置文件](#配置文件)
  - [完整参数参考](#完整参数参考)
- **Part 2 — 运行 House Gate**
  - [1. Standalone 模式](#1-standalone-模式)
    - [1.1 Relay 模式](#11-relay-模式)
    - [1.2 Sidecar 模式](#12-sidecar-模式)
    - [1.3 Router-Only Server（路由型 Server）](#13-router-only-server路由型-server)
  - [2. Library 模式](#2-library-模式)
- **Part 3 — 多 Proxy 路由原理**
  - [拓扑总览](#拓扑总览)
  - [Server 的双监听端口（external / internal）](#server-的双监听端口external--internal)
  - [forward-decision 插件](#forward-decision-插件)
  - [Sidecar 自动发现 upstream](#sidecar-自动发现-upstream)
  - [跨分片 `remote()` 信封](#跨分片-remote-信封)

---

## 前置依赖

| 依赖 | 版本 | 备注 |
|-----|------|------|
| Bazel | 8.5.1 | 必须 — 基于 Bzlmod 的构建系统 |
| Docker | 20.10+ | 可选，用于容器化部署 |

> **只走 Bazel。** 不要用 `go build` / `go test` / `go install`。vendored 的 protobuf 依赖一个由 Bazel 固定的运行时版本；普通 Go 工具链解析到的 `google.golang.org/protobuf` 不一致，进程一启动就会在 `init()` panic。

## 快速开始

```bash
# 构建
bazel build //cmd:housegate

# 运行（最简场景用环境变量即可，不需要配置文件）
HOUSEGATE_LISTEN=":9001" HOUSEGATE_UPSTREAM="localhost:9000" \
  bazel-bin/cmd/housegate_/housegate
```

如果需要单 upstream 之外的功能（鉴权、SQL 重写、分片路由、sidecar 模式等），用配置文件 — 见 [Part 1 — 配置](#配置文件)。

## 构建

```bash
bazel build //cmd:housegate                                    # 二进制
bazel test //...                                               # 全量测试
bazel test //pkg/proxy:proxy_test --test_filter='Validate'     # 过滤
make build / make test                                         # 委派给 Bazel
bazel mod tidy && bazel run //:gazelle                         # 修改 .proto / go.mod 后
```

构建产物：`bazel-bin/cmd/housegate_/housegate`，或 `bazel run //cmd:housegate -- -config config.json`。

> **为什么 `go build` 不行：** vendored 的 gRPC/protobuf 代码假设了某个特定的 `google.golang.org/protobuf` 运行时。Bazel 把版本固定住；`go build` 会从 `go.mod` 解析到不同版本，`protos/rewriter.pb.go` 的 `init()` 会以版本不一致 panic。

---

# Part 1 — 配置

## 配置文件

JSON 或 YAML，按扩展名选择（`.yaml` / `.yml` → YAML；其它 → JSON）。加载顺序：

1. CLI flag `-config /path/to/config.{json,yaml}`
2. 环境变量 `HOUSEGATE_CONFIG`
3. 当前工作目录下的 `config.json`（自动检测）
4. 内置默认值 + `HOUSEGATE_*` 环境变量覆写

文件可以是 **age 加密** 的 — proxy 在内存中解密（Linux 上落在 `memfd`，不写盘）。二进制内置 `secret-keygen` / `secret-encrypt` / `secret-decrypt` / `secret-edit` 子命令。详见 [docs/secrets.md](docs/secrets.md)。

```bash
# 生成 identity，加密配置，用密文启动
bazel-bin/cmd/housegate_/housegate secret-keygen > ~/.housegate.age
HOUSEGATE_AGE_IDENTITY_FILE=~/.housegate.age \
  bazel-bin/cmd/housegate_/housegate secret-encrypt config.json config.json.age
HOUSEGATE_AGE_IDENTITY_FILE=~/.housegate.age \
  bazel-bin/cmd/housegate_/housegate -config config.json.age
```

> **配置项分层。** 每个 plugin 拥有自己的 section：`auth`、`rewriter`、`sidecar`、`usage`、`concurrency_limit`、`state`、`logging`、`network_state`。旧的扁平 key（`auth_enabled`、`log_queries`、`sidecar_mode`、`network_state_redis`，以及 `dbrewriter` block 等）已不再被识别。

> **Duration 格式：** 所有 `duration` 字段接受人类可读字符串（`"5s"`、`"1m"`、`"24h"`）或裸纳秒整数。小于 1s 的值会触发 warning — 运维经常把秒和纳秒搞混。

## 完整参数参考

### 顶层 — 网络与生命周期

| Key | 类型 | 必填 | 默认值 | 说明 |
|-----|------|------|--------|------|
| `listen` | string | 是 | `:9001` | native ClickHouse 协议的 TCP 监听地址（**external 端口** — sidecar 和本地 CH 的 `remote()` 回环都打在这里） |
| `internal_listen` | string | 否 | `` (空) | 可选的第二个 TCP 监听端口，仅供其他 housegate 节点拨入。打在这里的 session 启动前就被预设为 `IsPeerTrusted=true` + `IsInternalPort=true`；auth 和 rewrite 自动跳过，`__route__` 信封直接拒绝。请通过防火墙 / SG 把它绑定到 peer-only 子网。详见 [Part 3](#part-3--多-proxy-路由原理)。|
| `upstream` | string | 否 | `` (空) | 单 upstream 地址。设置 `shard` 时被忽略。空 + 没有 `shard` + 不是 sidecar ⇒ router-only server（每个 session 都通过 NetworkState 转发到 peer — 见 [§1.3](#13-router-only-server路由型-server)）。 |
| `metrics_listen` | string | 否 | `:9091` | Prometheus metrics HTTP 地址 |
| `dial_timeout` | duration | 否 | `5s` | 连 upstream 的 dial 超时 |
| `idle_timeout` | duration | 否 | `5m` | 客户端连接空闲超时 |
| `max_connection_lifetime` | duration | 否 | `24h` | 单个客户端连接的硬性生命周期上限 |
| `shutdown_timeout` | duration | 否 | `30s` | SIGINT/SIGTERM 时的 graceful drain 时间预算 |
| `stats_interval` | duration | 否 | `10s` | 周期性包统计日志间隔 |
| `streaming_buf_size` | int | 否 | `131072` | 流式协议解析器 bufio 大小（字节） |
| `validate_checksum` | bool | 否 | `false` | 校验压缩 Data 块的 CityHash128 校验和 |
| `log_level` | string | 否 | `info` | housegate 自身日志的默认 level。接受 `debug`/`info`/`warn`/`error`/`fatal`（大小写不敏感）以及 slog 的偏移语法（`DEBUG+1`）。CLI 覆盖：`-log-level`；环境变量：`HOUSEGATE_LOG_LEVEL`。 |
| `log_file` | string | 否 | `` (stderr) | 将 housegate 自身日志重定向到该文件（`O_APPEND \| O_CREATE`，关闭 ANSI 颜色）。轮转交给外部工具（`logrotate`）。CLI 覆盖：`-log-file`；环境变量：`HOUSEGATE_LOG_FILE`。 |

### 顶层 — 跨切面凭证

| Key | 类型 | 必填 | 默认值 | 说明 |
|-----|------|------|--------|------|
| `relay_private_key_hex` | string | 否 | `` | 用于签 proxy-to-proxy（`__route__`）JWS token 的以太坊私钥。集群内所有 relay 应共用，且其地址必须出现在 `auth.allowed_addresses` 中。 |
| `ckh_manager_config_path` | string | 是（relay） | `` | 旧版 ClickHouseManager 配置路径；SQL rewriter 解析 table mapping 时需要 |
| `credential_replace_enabled` | bool | 否 | `true` | 转发到 upstream 前用运维管理的凭证替换客户端 ClickHouse 凭证 |
| `redis_default_addr` | string | 否 | `` | 当某个特性 section 自己的 `redis_addr` 留空时使用的 Redis 兜底地址 |

### `auth` — JWS / 以太坊签名

| Key | 类型 | 必填 | 默认值 | 说明 |
|-----|------|------|--------|------|
| `auth.enabled` | bool | 否 | `false` | 对每条 query 启用 JWS 校验 |
| `auth.allowed_addresses` | []string | 否 | `[]` | 允许的小写 `0x…` 地址。空 = 任意通过签名校验的签名者都允许 |
| `auth.max_token_age` | duration | 否 | `1m` | JWS `iat` claim 的最大年龄 |
| `auth.allow_no_auth` | bool | 否 | `false` | 放行未签名的 query（用于灰度切换） |

### `rewriter` — 外部 SQL Rewriter gRPC 服务

rewriter 是物理/逻辑数据库映射的唯一权威。连接上的每条 SQL 都会过它一遍。要点：

- **两阶段 Rewrite。** 阶段 1 用空 options 调一次 gRPC，拿到 AST 解析得到的 accessed table names。阶段 2 构造 `RewriteTableForSelectStmtArgs`（通过 `SentioNetworkTableMapper` 做 sentio-network 的 table 名解析）和 `RewriteTableForDynamicArgs`（鉴权过滤后的 `database_map`，再加上指向其它 indexer 的 logical 的 `remote_upstreams`），再调一次。
- **权限敏感的 `database_map`。** 只包含连接 account 拥有读/写/admin 权限的数据库；不可访问的数据库下的表通过 rewriter 不可寻址。
- **Fail-open。** gRPC 错误或 `UnsupportedStatement` 会回退到原始 SQL 并打 debug 级日志；rewriter 抖动不会阻塞 query。
- **错误反向映射。** 当 upstream 返回的 `Exception` 引用了被重写的库表名时，同一个每连接 Rewriter 通过 `RewriteErrorMessage` 把消息映射回客户端实际使用的名字。
- **wire-level `hello.Database` 重写。** `OnHello` 把 `hello.Database` 替换成 `rewriter.physical_database`；用户输入值保留在 `SessionState.LogicalDatabase`。

| Key | 类型 | 必填 | 默认值 | 说明 |
|-----|------|------|--------|------|
| `rewriter.service_addr` | string | 否 | `localhost:50051` | `sql-rewriter` gRPC 地址 |
| `rewriter.timeout` | duration | 否 | `5s` | 单次 gRPC 超时 |
| `rewriter.physical_database` | string | 否 | `` | 本部署中承载所有 logical database 的那个唯一物理 ClickHouse 数据库。空 = 同时关闭 `database_map` 和 `hello.Database` 替换 |
| `rewriter.delimiter` | string | 否 | `_` | `<logical>` 与 `<original_table>` 之间的分隔符 |

### `sidecar` — Sidecar 模式设置

| Key | 类型 | 必填 | 默认值 | 说明 |
|-----|------|------|--------|------|
| `sidecar.mode` | bool | 否 | `false` | 启用 sidecar 模式 |
| `sidecar.upstream` | string | 条件必填 | `` | 固定的 server-side proxy 地址（如 `10.0.0.8:9001`）。**该字段** 与顶层 `network_state.source` **二者必填其一**。留空时 sidecar 会基于 NetworkState 在每个 session 自动选一个 server-mode peer（详见 [§Sidecar 自动发现 upstream](#sidecar-自动发现-upstream)）。 |
| `sidecar.private_key_hex` | string | 是（sidecar） | `` | sidecar 的以太坊私钥，用于 JWS 签名。优先用 `HOUSEGATE_SIDECAR_KEY` 环境变量或 age 加密配置文件，不要明文写在配置里。 |

### `usage` — 计费 / 用量上报

| Key | 类型 | 必填 | 默认值 | 说明 |
|-----|------|------|--------|------|
| `usage.enabled` | bool | 否 | `false` | 启用对 `sentio-node` 的余额检查 + 用量上报 |
| `usage.sentio_node_addr` | string | 启用时 | `` | sentio-node 服务的 gRPC 地址 |
| `usage.redis_addr` | string | 启用时 | `` → `redis_default_addr` | query-check 缓存使用的 Redis |

### `concurrency_limit` — 用户级并发

| Key | 类型 | 必填 | 默认值 | 说明 |
|-----|------|------|--------|------|
| `concurrency_limit.enabled` | bool | 否 | `false` | 启用每用户的并发执行上限 |
| `concurrency_limit.per_user` | int | 否 | `0` | 每个识别身份的最大并发 query 数。`0` = 跟踪但不强制 |
| `concurrency_limit.timeout` | duration | 否 | `60s` | 残留 permit 的回收窗口 |
| `concurrency_limit.fail_open` | bool | 否 | `true` | Redis 故障时：`true` 放行并打 warn 日志，`false` 拒绝 |
| `concurrency_limit.redis_addr` | string | 启用时 | `` → `redis_default_addr` | 限流器有序集合所用的 Redis |

### `state` — Session 跟踪器（仅 OnHello）

rewriter 化迁移之后，state plugin 仅剩 OnHello 一件事：把 `ClientHello.Database` 拷贝到 `SessionState.LogicalDatabase`，让随后在 OnQuery 阶段跑的 rewriter 能读到。这个 section 当前没有运维可调字段。

### `logging`

| Key | 类型 | 必填 | 默认值 | 说明 |
|-----|------|------|--------|------|
| `logging.queries` | bool | 否 | `true` | 记录 SQL query 内容 |
| `logging.data` | bool | 否 | `false` | 记录 Data 包内容（仅调试） |
| `logging.max_query_bytes` | int | 否 | `300` | 日志中 query 的截断长度（字节） |
| `logging.max_data_bytes` | int | 否 | `200` | 日志中 Data 包的截断长度（字节） |

### `network_state`

`network_state.source` 是多态字段，按后缀自动识别：

- 以 `.yaml` / `.yml` 结尾 → 启动时作为静态 fixture 加载到 `InMemoryNetworkState`。schema 见 [pkg/network/yaml.go](pkg/network/yaml.go)；示例在 [configs/local.network_state.yaml](configs/local.network_state.yaml)。
- 其它 → 视为给 statemirror 消费者用的 Redis 地址。空时回落到 `redis_default_addr`。

```yaml
network_state: { source: "configs/local.network_state.yaml" }   # YAML 模式
network_state: { source: "redis.internal:6379" }                # Redis 模式
```

环境变量回落：`HOUSEGATE_NETWORK_STATE_SOURCE`（现代）和 `HOUSEGATE_NETWORK_STATE_REDIS`（旧字段，仅 Redis）。

### `shard`、`health_check`、`pool`、`routing` — 分片与副本

可选；存在时优先于扁平的 `upstream` 字段。每个 proxy 实例管理一个 shard。

| Key | 类型 | 必填 | 默认值 | 说明 |
|-----|------|------|--------|------|
| `shard.name` | string | 否 | `` | 可读的 shard 名 |
| `shard.replicas` | []Replica | 是（出现 `shard` 时） | `[]` | 每副本的 `{host, port, weight, is_backup}` |
| `shard.settings` | map[string]string | 否 | `{}` | 该 shard 默认合入 query 的 ClickHouse settings |
| `health_check.interval` | duration | 否 | `5s` | 主动健康检查间隔 |
| `health_check.timeout` | duration | 否 | `3s` | 单次探测超时 |
| `health_check.failure_threshold` | int | 否 | `3` | 标记不健康前的连续失败次数 |
| `health_check.recovery_threshold` | int | 否 | `2` | 重新标记健康前的连续成功次数 |
| `pool.max_idle` | int | 否 | `5` | 每副本的空闲连接数 |
| `pool.max_open` | int | 否 | `50` | 每副本的最大连接数 |
| `pool.max_lifetime` | duration | 否 | `1h` | 池中连接的最大寿命 |
| `pool.max_idle_time` | duration | 否 | `10m` | 关闭前的最大空闲时间 |
| `pool.dial_timeout` | duration | 否 | `5s` | 每副本的 dial 超时 |
| `routing.strategy` | string | 否 | `round-robin` | 取值：`round-robin`、`random`、`least-conn`、`weighted` 之一 |

---

# Part 2 — 运行 House Gate

有两种方式运行 House Gate：

- **Standalone** — 用 `housegate` 二进制配合配置文件（或环境变量 / CLI flag）。三种运行时模式从配置中推导。
- **Library** — 在自己的 Go 进程中 import `housegate`，调用 `housegate.New(opts).Run(ctx)`。同一套 plugin chain；依赖注入和生命周期由你掌握。

## 1. Standalone 模式

```bash
bazel-bin/cmd/housegate_/housegate -config config.json
# 或通过 Bazel（必要时会重新构建）
bazel run //cmd:housegate -- -config config.json
```

模式 **从配置推导**，不是 flag：

| 触发条件 | 模式 |
|----------|------|
| `sidecar.mode: true` | Sidecar |
| 配置了 `upstream` 或 `shard`（且不是 sidecar） | Server（带本地 CH） |
| 都没配 | Server（router-only — 通过 NetworkState 转发到 peer） |

按 `Ctrl+C` 进入 graceful shutdown；退出前会打印最终统计。启动时 proxy 会输出解析出来的模式与关键设置：

```
housegate listening  addr=:9001  mode=server  indexer_id=42
metrics listening    addr=:9091
```

### 1.1 Relay 模式

Server-mode proxy。终结客户端 TCP，校验 JWS 鉴权，通过 `sql-rewriter` gRPC 重写 SQL，并转发到本地的 ClickHouse 副本。**必须** 配置 `network_state.source` 和 `ckh_manager_config_path`。

最简配置（不开鉴权，单 upstream）：

```json
{
  "listen": ":9001",
  "upstream": "127.0.0.1:9000",
  "metrics_listen": ":9091",

  "ckh_manager_config_path": "/etc/housegate/ckh-manager.yaml",
  "redis_default_addr": "localhost:6379",

  "rewriter":      { "service_addr": "localhost:50051", "timeout": "5s" },
  "network_state": { "source":       "localhost:6379" },
  "logging":       { "queries":      true, "data": false }
}
```

带 JWS 鉴权：

```json
{
  "listen": ":9001",
  "upstream": "127.0.0.1:9000",

  "ckh_manager_config_path": "/etc/housegate/ckh-manager.yaml",
  "redis_default_addr": "localhost:6379",
  "relay_private_key_hex": "0xYOUR_RELAY_PRIVATE_KEY_HERE",

  "auth": {
    "enabled": true,
    "allowed_addresses": ["0x1234567890123456789012345678901234567890"],
    "max_token_age": "1m",
    "allow_no_auth": false
  },

  "rewriter":      { "service_addr": "localhost:50051" },
  "network_state": { "source":       "localhost:6379" }
}
```

Shard 感知（每副本连接池 + 路由）：

```json
{
  "listen": ":9001",
  "ckh_manager_config_path": "/etc/housegate/ckh-manager.yaml",
  "redis_default_addr": "redis.internal:6379",

  "shard": {
    "name": "shard-01",
    "replicas": [
      { "host": "ch-shard01-r1.internal", "port": 9000, "weight": 1 },
      { "host": "ch-shard01-r2.internal", "port": 9000, "weight": 1 }
    ]
  },
  "health_check": { "interval": "5s", "timeout": "3s", "failure_threshold": 3, "recovery_threshold": 2 },
  "pool":         { "max_idle": 5, "max_open": 50, "max_lifetime": "1h", "max_idle_time": "10m", "dial_timeout": "5s" },
  "routing":      { "strategy": "round-robin" },

  "network_state": { "source": "redis.internal:6379" }
}
```

### 1.2 Sidecar 模式

无本地 ClickHouse — 每条 query 用 `sidecar.private_key_hex` 签名后转发给 `sidecar.upstream` 上的 relay-mode proxy。Server-side 的特性（重写、分片路由）都关闭。

配置文件方式：

```json
{
  "listen": ":9001",
  "metrics_listen": ":9091",

  "sidecar": {
    "mode": true,
    "upstream": "10.0.0.8:9001",
    "private_key_hex": "0xYOUR_SIDECAR_PRIVATE_KEY_HERE"
  }
}
```

Sidecar 也可以不带配置文件启动，用 CLI flag 或环境变量：

```bash
# CLI flag
bazel-bin/cmd/housegate_/housegate \
  -sidecar -sidecar-upstream 10.0.0.8:9001 \
  -sidecar-key 0xYOUR_PRIVATE_KEY_HERE -listen :9001

# 环境变量（推荐用于 secret）
HOUSEGATE_SIDECAR=true \
HOUSEGATE_SIDECAR_UPSTREAM=10.0.0.8:9001 \
HOUSEGATE_SIDECAR_KEY=0xYOUR_PRIVATE_KEY_HERE \
HOUSEGATE_LISTEN=:9001 \
bazel-bin/cmd/housegate_/housegate

# 混合（secret 走环境变量，路由走 flag）
HOUSEGATE_SIDECAR_KEY=0xYOUR_PRIVATE_KEY_HERE \
bazel-bin/cmd/housegate_/housegate -sidecar -sidecar-upstream 10.0.0.8:9001
```

> **安全提示：** CLI flag 在进程列表（`ps`、`/proc`）中是可见的。请优先用 `HOUSEGATE_SIDECAR_KEY` 或配置文件传私钥。

覆盖优先级（高 → 低）：CLI flag → 环境变量 → 配置文件 → 内置默认值。

所有 CLI flag：

| Flag | 默认值 | 说明 |
|------|--------|------|
| `-sidecar` | `false` | 启用 sidecar 模式（覆盖 `sidecar.mode`） |
| `-sidecar-upstream` | (空) | server-side proxy 地址 |
| `-sidecar-key` | (空) | JWS 签名用的以太坊私钥 |
| `-listen` | `:9001` | proxy 监听地址 |
| `-metrics-listen` | `:9091` | Prometheus metrics 地址 |
| `-dial-timeout` | `5s` | upstream dial 超时 |
| `-idle-timeout` | `5m` | 连接空闲超时 |
| `-log-queries` | `true` | 记录 SQL query 内容 |
| `-config` | (空) | JSON/YAML 配置文件路径（亦可用 `HOUSEGATE_CONFIG`）；接受 age 加密文件 |

### 1.3 Router-Only Server（路由型 Server）

**没有本地 ClickHouse** 的 server-mode housegate（既没配 `shard`，也没配 `upstream`）。所有 session 都通过 `NetworkState` 转发到同伴 relay。它就是过去 "forwarding-only" 模式扮演的角色，现在被收敛进了 server 模式之下的一个子形态。**必须** 配置 `network_state.source`；不需要 rewriter / ckh-manager — 在这个配置下 SQL rewriter 会被自动禁用。

```json
{
  "listen": ":9001",
  "metrics_listen": ":9091",
  "network_state": { "source": "redis.internal:6379" }
}
```

dialer 在没有 plugin 设置 route target 时，会落到一个兜底分支：从 `RetrieveAllIndexerInfos()` 里随机挑一个 `ClickhouseProxyPort` 非零的 peer，并通过 `selfListenPort` + `isLocalAddress` 排除自身。dial 失败时最多重试三个 peer 才向客户端报错。如果 sidecar 接下来又通过 `USE` 切到了别的 host，接收方的 router-only server 会和任何普通 server-mode proxy 一样，靠 forward-decision 插件把整个 session 转移过去 — 详见 [Part 3](#part-3--多-proxy-路由原理)。

## 2. Library 模式

把 House Gate 嵌入到自己的 Go 进程里。Plugin chain 与 standalone 完全相同 — 二进制只是这个 API 之上的薄壳。

```go
import (
    "context"

    "housegate/housegate"
    "housegate/housegate/pkg/config"
)

cfg := /* 你的 *config.Config — 从文件加载或代码构造 */

p, err := housegate.New(housegate.Options{
    Config: cfg,

    // 可选：上报本 proxy 代表哪个 indexer。每次 Proxy.IndexerId() 调用
    // 都会重新从 getter 取值，所以启动后才学到 id（例如链上注册）的
    // host 不需要重建 proxy 即可更新。
    GetIndexerId: func() uint64 { return myIndexerRegistry.Id() },

    // 可选：依赖覆盖。任何非 nil 的字段都会被原样使用（生命周期由
    // 调用方掌握）。nil → New 从 Config 自行构造，并在 Run 返回时
    // 销毁。
    //
    //   NetworkState        network.State
    //   CkhManager          ckhmanager.Manager
    //   Validator           auth.Validator
    //   Rewriter            rewriter.Factory
    //   CredProvider        credentials.CredentialProvider
    //   Signer              auth.Signer
    //   UsageClient         billing.UsageClient
    //   Cluster             cluster.Cluster
    //   CommitGateObservers []commitgate.Observer
    //   RedisClients        map[string]*redis.Client
    //   Logger              *slog.Logger  // 任意 slog.Logger；nil → pkg/log 默认
})
if err != nil { return err }

// 阻塞直到 ctx 被取消或 listener 出错。
if err := p.Run(ctx); err != nil { return err }

// 或者自己提供 listener（TLS-wrap、":0" 端口绑定、unix socket 等）：
// err := p.RunWith(ctx, ln)
```

`Proxy` 接口：

| 方法 | 说明 |
|------|------|
| `Run(ctx) error` | 绑定 `cfg.Listen`，serve 直到 `ctx` 被取消或 listener 出错。资源销毁在返回前完成。 |
| `RunWith(ctx, ln) error` | 同 `Run`，但 listener 由调用方拥有 |
| `Addr() net.Addr` | 已绑定的 listener 地址；Run/RunWith 绑定前为 nil，绑定后稳定 |
| `IndexerId() uint64` | 每次调用都从 `GetIndexerId` getter 取值（没有 getter 时返回 0） |

**所有权规则。** 你传进 `Options` 的非 nil 字段，由你负责在 `Run` 返回后 close。`New` 自行从配置构造的依赖，由 proxy 在反向构造序中销毁。没有单独的 `Close` 方法 — `Run` / `RunWith` 就是生命周期范围。

适合用 library 模式的场景：
- 把 House Gate 嵌入到一个更大的服务里，复用其中已经存在的 Redis pool、network-state mirror 或 ckh-manager。
- 测试场景需要 `:0` 端口的 proxy，搭配自定义的 `Validator` / `NetworkState` / `Rewriter` fake。
- indexer id 不是静态的（运行时链上注册）— 把 `GetIndexerId` 接到 registry 上。

生命周期与所有权细节见 [docs/superpowers/specs/2026-04-26-cmd-library-mode-design.md](docs/superpowers/specs/2026-04-26-cmd-library-mode-design.md)。

---

# Part 3 — 多 Proxy 路由原理

线上部署里通常会有多个 server-mode housegate 并排跑，每个 indexer 一个。Sidecar 上的 `clickhouse-client` 可能会发出 `USE tenantB`，但 `tenantB` 实际并不在当前 sidecar 正在通信的那个 server-mode proxy 上，而在另一个 proxy 上。整个 proxy 网络要透明地解决这个问题。完整设计见 [docs/superpowers/specs/2026-04-28-two-port-server-mode.md](docs/superpowers/specs/2026-04-28-two-port-server-mode.md)。

## 拓扑总览

```
                     network_state（yaml 文件 或 Redis statemirror）
                              │
                              │  读取：indexer_infos、database_infos、
                              │       database_permissions
                              ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │                                                                      │
   │  sidecar（客户端侧）                                                  │
   │   • 用 sidecar.private_key_hex 给每条 query 签 JWS                   │
   │   • 通过 Selector 在每个 session 自选一个 server-mode peer          │
   │     （或使用固定的 sidecar.upstream）                                │
   │                                                                      │
   └────────────┬─────────────────────────────────────────────────────────┘
                │  ClientHello + JWS（在 password 字段里）
                ▼
   ┌────────────────────────────────────┐         ┌────────────────────────────┐
   │ server-mode proxy A                │         │ server-mode proxy B        │
   │                                    │         │                            │
   │  external 端口  :9001 ─┐           │         │  external 端口  :9001      │
   │   • Stripper           │           │         │  internal 端口  :9101      │
   │   • credential         │           │         │   （仅 peer）              │
   │   • auth (JWS)         │           │         │                            │
   │   • forward-decision   │           │         │                            │
   │   • rewrite, …         │           │         │                            │
   │                        │           │         │                            │
   │  internal 端口  :9101  │           │         │                            │
   │   （仅 peer，          │           │         │                            │
   │    session 进入时       │           │         │                            │
   │    预设为 peer-trusted）│           │         │                            │
   │                        │           │         │                            │
   │   ClickHouse A ◄───────┘           │         │   ClickHouse B             │
   └─────────────────┬──────────────────┘         └────────────────────────────┘
                     │  USE tenantB → forward.Plugin 检测到 tenantB 在 B 上
                     │
                     ▼
                Session.RebindToPeer(B:internal)
                ── 用 __peer__ 信封 + relay JWS 重新做一次 hello ──►
                                                              │
                                                              ▼
                                             B 的 internal 端口预设
                                             IsPeerTrusted；auth + rewrite
                                             跳过；其余 plugin 正常运行。
```

架构层不变量：**ClickHouse 实例只跟自己同位的 housegate 建 TCP，永远不直连其它 shard 的 CH 或别的 housegate。** 跨 shard 的 `remote()` 一律先回环到本地 housegate；网络层 ACL（防火墙 / SG）必须硬性阻止 CH 触达任何其他 housegate 或任何 peer 的 internal 端口。

## Server 的双监听端口（external / internal）

配置了 `internal_listen` 时，[`buildServer`](build.go) 会在该地址绑定第二个 `*proxy.Server`。第二个 listener 注入了一个 `PreflagSession` 回调，会在 OnHello 之前就把每个进来的 session 标记成 `IsPeerTrusted=true` + `IsInternalPort=true`，于是 peer-trust-aware 的插件（auth、rewrite、commitgate）会自动跳过自己。两个 listener 共享同一套插件链，**只有这一处预设标记不同**。

| 维度 | external 端口（`listen`） | internal 端口（`internal_listen`） |
|---|---|---|
| 谁能拨进来 | sidecar、本地 CH 的 `remote()` 回环 | 仅其它 housegate（靠防火墙强制） |
| Session 预设状态 | 无 | `IsPeerTrusted = true`、`IsInternalPort = true` |
| `__route__` 信封 | 接受（loopback 路径） | **拒绝**（`routeplugin.Stripper` 直接关闭，避免转发环） |
| `__peer__` 信封 | 出现时接受 | 接受（验证发起 peer 的 JWS） |
| `auth` 插件 | 运行（验证 sidecar JWS） | 跳过（`PeerTrustAware.RunOnPeerTrust=false`） |
| `forward-decision` | 运行 | 不适用（internal 端口不再向外转发） |
| `rewrite`、`commitgate` | 运行 | 跳过（peer-trust 主动 opt-out） |
| `metrics`、`usage`、`concurrency`、`sessionstate` | 运行 | 运行 |

即便网络层已经做了隔离，internal 端口仍然会校验 `__peer__` JWS — JWS 提供了"是谁在调我"的密码学证明，用于审计与 metrics 打标。

## forward-decision 插件

[`pkg/plugins/forward/`](pkg/plugins/forward/) 只在 external 端口的链上运行，会在两个时机触发：

**OnHello。** 通过 `NetworkState.RetrieveDatabaseInfo` 解析 `hello.Database`。如果该数据库属于另一个 indexer，就调用 `Session.RebindToPeer(peer:internal)` — 用全新签出的 `__peer__` handshake 在新 upstream 上重放 hello，并把 peer 返回的 ServerHello 缓存下来由 relay 直接回写给客户端。同时把 session 标记为 `IsForwarding=true`。如果 `hello.Database == ""`，先延迟决策；首条 `USE` 再来决定。如果数据库根本不存在，直接合成一个 `Code: 81 Database doesn't exist` 异常返回。

**OnQuery（USE 检测）。** 一个收紧的正则（`^\s*USE\s+(\S+)\s*;?\s*$`，大小写不敏感）专门捕获独立的 `USE <name>`。如果新的数据库解析到与当前 upstream 不同的 peer，就在转发 USE 包之前再触发一次 `RebindToPeer`。Upstream codec 的原子替换（Session 内的 `atomic.Pointer[chproto.Codec]`）保证 relay 的读写两个 goroutine 不会出现竞态；`clientToUpstream` 与 `upstreamToClient` 都会在每轮迭代里重新取一次 upstream 指针，并能容忍中途切换。

单条语句里出现的跨数据库 SQL（`tenant1.x JOIN tenant2.y`）**不会** 在 session 层重新路由 — 走的是下面要讲的 SQL rewriter `remote()` 子句路径。

`ForwardAware` 标记接口和 `PeerTrustAware` 一样，是默认开启 / 主动 opt-out 的设计。无论是否在转发都应该在源 proxy 上跑的插件（auth、metrics、concurrency、usage、sessionstate、credential）保持运行；属于 *host* proxy 那一侧的插件（rewrite、commitgate）则实现 `RunOnForward()=false`。

## Sidecar 自动发现 upstream

当 `sidecar.upstream` 留空时，[`pkg/plugins/sidecar.Selector`](pkg/plugins/sidecar/upstream_select.go) 会按 `network_state.source` 在每个 session 用一个两层算法挑 server-mode peer：

```
account = derive_address(sidecar.private_key_hex)
perms   = NetworkState.RetrieveDatabasePermissions(account)

permissioned = 至少持有 perms 中某个 DB、且 ClickhouseProxyPort 非零的 indexer
bound        = 任何 ClickhouseProxyPort 非零的 indexer

switch {
case len(permissioned) > 0:  pick = random(permissioned)            // 正常路径
case len(bound) > 0:         pick = random(bound); IsBootstrap=true // 见下
default:                     return error("no bound indexers")
}
```

**Bootstrap 兜底。** 全新账户在所有数据库上都没有权限 — 因为它还没创建过任何数据库。如果在 dial 阶段就拒绝它，会导致新用户连 `CREATE DATABASE` 都跑不起来（先有鸡还是先有蛋）。所以当 `permissioned` 为空但 `bound` 非空时，Selector 仍会随机挑一个 bound indexer；用户的第一条 `CREATE DATABASE` 会落到那台机器上，`commitgate` 把这个新数据库注册回 NetworkState，后续 session 就能正常解析了。

Bootstrap 路径会打 warn 日志，并把 Prometheus counter `clickhouse_proxy_sidecar_bootstrap_fallback_total` 自增 1，方便运维识别那些不该走 bootstrap 路径的账户。

在选定 tier 内做随机选择既能均衡负载，也免费送一个 fail-over 机制（下一次 session 重新 roll）。固定 `sidecar.upstream` 仍然有效，作为显式的 override。

## 跨分片 `remote()` 信封

当 SQL rewriter 给某个 logical DB 生成 `remote()` 子句时，本地 CH 通过 housegate 拨回外部时携带的两段信息嵌套地编码在 user / password 字段里：

- **`user`**：`__route__|<peer-addr>|__peer__|<self-address>` — 本地 proxy 的 `routeplugin.Stripper` 会在回环这一跳剥掉 route 信封，把连接转发到 `<peer-addr>`（通常是该 peer 的 internal 端口）。peer 信封继续往下游走，由对端校验。
- **`password`**：用 relay 私钥签的 peer-relay JWS，audience = peer 的 indexer-id。接收方的 `credential.Plugin` 校验 JWS 之后，session 被打上 `IsPeerTrusted=true`，链路上的 auth + rewrite 通过 `PeerTrustAware` 自动跳过。

两个信封共用 `|` 这个分隔符约定（[pkg/route](pkg/route/) 与 [pkg/peer](pkg/peer/)）。两个标记会复合：一个既是 routed 又是 peer-trusted 的 session，先用 route 过滤器过一遍，再把 peer-trust 过滤器作用在剩下的 plugin 上。实现位于 [pkg/rewriter/sentio.go](pkg/rewriter/sentio.go)（`buildSentioTableMappings`、`buildRemoteUpstreams`）。

这是 **per-clause** 的跨分片机制；上文的 forward-decision 插件是 **per-session** 的对应版本，专治那些"整个 session 的 scope 就是某个远端 logical DB"的情况。
