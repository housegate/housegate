# Signed inline `INSERT ... VALUES` (agent mode)

This agent-only feature is default-off. It signs a complete inline `INSERT ... VALUES` whose rows arrive inside the query text, as sent by `clickhouse-client` 26.3 and later and the pinned clickhouse-go `Exec`. A 25.x client puts the rows on the wire as Native blocks but truncates the Query after `VALUES`; HouseGate leaves that truncated shape unsupported. The existing `FORMAT Values` form with rows on stdin remains supported by the ordinary signed streaming lane.

## Flow

1. **Materialize.** The `materialize` plugin rewrites `now()`, `rand()`, `generateUUIDv4()` and the other configured nondeterministic functions to constant expressions. Enabling `inline_values` requires both `materialize.enabled` and `storage_integrity.agent.enabled` at startup. For this lane, only the recorded `applied` and `noop` outcomes are accepted; a missing, failed or unknown materialization outcome refuses the statement.
2. **Close.** A lexical gate accepts parenthesized row tuples composed of supported literals, arithmetic and comparison operators, parentheses, and function calls. It refuses `SELECT`, `FROM`, `WITH`, `IN`, `NULL`, `DEFAULT`, `CAST`, `AS`, `INTERVAL`, `AND`, `OR`, `NOT`, bare identifiers, `{name:Type}` parameters, heredocs, comments, quoted identifiers, and the shared nondeterministic and server-state deny lists (`now`, `rand`, `dictGet*`, `currentUser`, `currentDatabase`, `hostName`, `version`, `getSetting`, `sleep`, `throwIf`, and others). Nothing reaches the evaluation connection until this check passes.
3. **Evaluate.** The agent opens or reuses a dedicated pooled connection to the same endpoint already selected for the current session; it never invokes the random selector again. It runs one signed `SELECT <individually quoted columns> FROM VALUES('<structure>', <rows>)` against the tenant's ClickHouse with `max_execution_time`, `max_result_rows`, `max_result_bytes` and `max_block_size` bounded by the configuration. The helper carries the configured `agent.owner` as `SQL_x_payer` and requests the configured `agent.driver` privilege with `SQL_sentio_driver`; the operator/indexer key still signs the helper, and the server still authorizes the owner relationship and driver privilege. The entire attempt is deadline-bound and is never retried. An exception, timeout, transport error, empty result, name/order/type mismatch or row/byte overflow refuses the statement.
4. **Sign and forward.** The agent rewrites the body to `INSERT INTO <db>.<table> (<individually quoted columns>) FORMAT Native`, encodes the evaluated blocks at the current upstream codec revision, and signs exactly the packet bytes later written upstream. The server receives the existing signed streaming Native shape, so the statement-token and payload contracts, server ingress, intake journal, replay executors and Arbiter interfaces do not change.

## Configuration

The example is additive to the normal agent configuration. Keep `agent.private_key_hex` and either a pinned `agent.upstream` or `network_state.source`; the storage-integrity agent also needs its schema-providing NetworkState unless the embedding host injects one. Configure the receiving server's normal [`auth`](../README.md#auth--jws--ethereum-signature), storage-integrity ingress and upstream ClickHouse credentials separately.

```yaml
network_state:
  source: /etc/housegate/network-state.yaml
agent:
  mode: true
  upstream: "housegate-server.internal:9001" # omit to auto-discover through network_state.source
  private_key_hex: "0x..."                   # prefer HOUSEGATE_AGENT_KEY or an encrypted config
materialize:
  enabled: true
  engine: native
  native_library_path: "/opt/housegate/libpolyglot_sql_ffi.so"
storage_integrity:
  agent:
    enabled: true
    network_id: mainnet-1
    state_dir: /var/lib/housegate/si
    inline_values:
      enabled: true
      evaluation_timeout: 10s
      max_rows: 65536
```

| Key | Type | Required | Default | Description |
|---|---|---|---|---|
| `storage_integrity.agent.inline_values.enabled` | bool | No | `false` | Enables the lane. `true` requires `storage_integrity.agent.enabled` and `materialize.enabled`. |
| `storage_integrity.agent.inline_values.evaluation_timeout` | duration | No | `10s` | Bounds the one evaluation attempt and its helper-query `max_execution_time`; must be at least `1s`. |
| `storage_integrity.agent.inline_values.max_rows` | uint | No | `65536` | Bounds the aggregate evaluated rows and supplies `max_result_rows` and `max_block_size`; must be positive. |
| `storage_integrity.agent.max_payload_bytes` | uint | No | `67108864` | Bounds the encoded signed payload and supplies the helper query's `max_result_bytes`; must be positive when the SI agent is enabled. |

## Account and usage context

The helper query preserves the ordinary agent's configured account context exactly: an empty `agent.owner` leaves the signer as payer, a non-empty owner is carried verbatim for the server's operator-for-owner authorization, and `agent.driver: true` requests the existing driver privilege that the server grants only to its configured indexer signer. Helper connections are pooled separately by owner and driver state so one context cannot inherit another context's authenticated session state.

Ordinary query usage and `indexing_usage` are separate paths. The helper goes through the ordinary server authorization and query-usage policy with the payer/driver context above; it is not described as universally unbilled. The helper SQL is a SELECT that touches no table, so it emits no driver-INSERT `indexing_usage` report. The synthesized INSERT later follows the existing signed INSERT path.

## Supported columns and statement limits

The declared NetworkState schema remains authoritative. Every helper result and encoded block must resolve through `payloadexec.ResolveColumnProfile`; the current admitted set is `String`, `FixedString(32)`, `Bool`, `Float32`, `Float64`, `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Date`, `DateTime`, `DateTime(<tz>)`, `DateTime64(P)` and `DateTime64(P, <tz>)`. `NULL`, `Nullable`, arrays, tuples, `UUID`, `Decimal` and `Date32` are outside that authority.

The inline lane also refuses `INSERT ... SELECT` / `WITH`, `INSERT ... VALUES ... SETTINGS`, query parameters, `async_insert`, multi-statement input, `FORMAT Values` with inline data after the format name, and query compression. It does not deduplicate client retries. A 25.x truncated `INSERT ... VALUES ` remains unsupported, while `INSERT ... FORMAT Values` with rows on stdin remains supported by the existing streaming path.

The original expression text is not part of the signed record. HouseGate logs it only at debug level with the statement id; info-level signing logs carry the statement id, table id and payload size instead.

## Errors, sequence allocation and metrics

Every refusal raised during `OnQuery` after the lane claims a complete inline VALUES statement reaches the client with the prefix `storage_integrity inline VALUES: `. These admission checks include materialization, lexical closure, schema and column validation, the single helper evaluation, and its aggregate row and byte limits. They run before a statement id is minted, so they consume no `client_seq`.

Once `OnQuery` admits the statement, HouseGate durably reserves its sequence and never rolls it back. A later client-marker, packet-encoding, strict-hook, upstream-sample, cancellation, transport or upstream-execution failure can therefore leave a sequence gap; later exceptions follow their owning stage and are not guaranteed to use the inline admission prefix.

The agent exports `clickhouse_proxy_agent_inline_values_total{result="synthesized"|"evaluation_failed"|"closure_refused"}`. `synthesized` counts statements admitted into the relay lane, `closure_refused` counts lexical-policy refusals, and `evaluation_failed` covers the evaluation call and post-evaluation shape, type, row and byte validation failures.

## Rollout

The code is agent-only and default-off, with no new wire or signed-contract field. Upgrade the agent, verify the normal signing key, optional owner and driver settings, selected-upstream or NetworkState configuration, schema source, materializer backend and receiving server auth/ingress configuration, then enable `inline_values.enabled`. Roll back by disabling the flag. Repository acceptance does not by itself establish production deployment or live production acceptance.

Design: [signed inline VALUES design](superpowers/specs/2026-09-23-signed-inline-values-design.md).
