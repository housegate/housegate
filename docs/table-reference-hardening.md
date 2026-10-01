# Table-reference hardening: operator note

Spec: [superpowers/specs/2026-09-26-table-reference-hardening-design.md](superpowers/specs/2026-09-26-table-reference-hardening-design.md) (§9 is the HouseGate half, §14 the execution record). Fail-closed rewriting and `rewriter.fail_open_on_unavailable` are described in [rewriter-fail-closed.md](rewriter-fail-closed.md).

The SQL rewriter engines enforce the table-reference policy: a tenant statement may only reach its own logical databases, never the physical database, the protocol-owned `hg_safe` / `hg_unsafe` / `hg_promote`, a `system` table outside the allowlist, a refused table function or engine, or an `Identifier` query parameter in a table position. HouseGate proves the engine at startup, sends it the protected namespace on every request, and adds two checks of its own in front of it: a lexical guard and a Query-packet settings check.

## Minimum engines and the startup probes

HouseGate requires rewriter-go v0.16.0 (native engine, with the FFI library of the same release) or rewriter-grpc v0.16.0. rewriter-go v0.15.0 is not enough: it answers `Success` for the old-analyzer switch (`SETTINGS allow_experimental_analyzer = 0`) and for a read inside a table-engine argument, both of which v0.16.0 refuses (spec §13). The v0.16.0 FFI assets are byte-identical to the v0.15.0 ones; the difference is in the engine, which is why HouseGate checks behaviour, not the library hash.

Every request carries `protected_databases = [rewriter.physical_database, hg_safe, hg_unsafe, hg_promote]`, whatever `storage_integrity.enabled` says, so the engine refuses those databases in every position.

Every server that has a rewriter, built from config or injected by an embedding host, runs the table-reference probe before it listens, with or without storage integrity: twelve fixed statements (an identifier parameter in a table position, the physical database as an IN operand, `USE` and a `merge()` argument, `hg_promote`, IN-operand and `INSERT … SELECT` source rewriting, a refused table function, an unmodelled statement class, a refused system table, the analyzer switch, an engine-argument subquery) whose exact code, statement type, SQL and message must match the answers both v0.16.0 engines were measured to give. An engine that fails any case refuses startup:

```
rewriter table-reference probe (engine=native probe=analyzer-off-refused): code=Success, want UnsupportedStatement; deploy rewriter-go >= v0.16.0 or rewriter-grpc >= v0.16.0 (table-reference policy, spec 2026-09-26)
```

The error names the case and never quotes the probe SQL or the engine's text. `rewriter.fail_open_on_unavailable` does not cover a probe failure: it only lets a server start when the rewriter cannot be built at all. With storage integrity enabled, the eight storage-integrity probes run after it (their requests carry `protected_databases` too).

Roll the engines out before this HouseGate: an older engine stops HouseGate from starting. Embedding hosts that inject their own rewriter factory must implement `rewriter.TableReferenceProbeFactory` (`ProbeTableReferencePolicy`); test doubles can answer the probe with `rewriter.TableReferenceProbeAnswer`.

## The table-reference guard (`tableref_guard`)

A lexical guard runs on the original SQL before `forward` and `rewrite`, on every server that forwards to ClickHouse (a `shard`, an `upstream` or a host-injected cluster), whether or not a rewriter was built: on a server started under `rewriter.fail_open_on_unavailable` without its rewriter, the guard is the only policy layer left. Router-only servers forward whole sessions to peers and do not run it. It checks ordinary sessions, including the indexer driver and sessions this server forwards to a peer; it skips maintenance and platform-operator sessions (the storage-integrity `sireserved` guard covers those) and peer sessions (`remote()` loopbacks and sessions forwarded from a peer), whose SQL the origin already checked.

It refuses, naming the rule:

- `reserved_name`: `hg_safe`, `hg_unsafe` or `hg_promote` anywhere outside a comment, string literals included (case-insensitive).
- `physical_database`: the configured `rewriter.physical_database` (case-sensitive) as a qualifier (`phys.t`, `` `phys`.t ``, `"phys".t`); in a `USE` statement; as the `FROM` / `IN` object of a `SHOW` statement; as the object of `DATABASE` (`CREATE`, `DROP`, `ATTACH`, `DETACH`, `ALTER`, `TRUNCATE`, `EXISTS`, `DESCRIBE`, `SHOW CREATE … DATABASE`, `SYSTEM … DATABASE [REPLICA]`); in a `RENAME`, `EXCHANGE`, `BACKUP` or `RESTORE` statement that names a database; after `TABLES FROM` / `TABLES IN` (`TRUNCATE ALL TABLES FROM phys`); and inside the arguments of a refused table function or a lookup function (`joinGet`, `joinGetOrNull`, `dictGet*`, `dictHas`, `dictIsIn`, `hasColumnInTable`), as a name or as a string naming it or a table in it. A column, alias or string literal elsewhere that equals the name is allowed. With an empty `rewriter.physical_database` this rule is inactive and HouseGate logs a startup warning.
- `carrier_callable`: a call to a table function the engines refuse (`merge`, `remote`, `remoteSecure`, `cluster`, `clusterAllReplicas`, `loop`, `dictionary`, every `mergeTree*` name but the ordinary function `mergeTreePartInfo`, `timeSeriesData`, `timeSeriesTags`, `timeSeriesMetrics`, `timeSeriesSelector`, `prometheusQuery`, `prometheusQueryRange`, `clickhouse`, `mysql`, `postgresql`, `mongodb`, `jdbc`, `odbc`, `executable`, `fuzzQuery`, `fuzzJSON`, and the ClickHouse table functions the engines do not recognise, such as `primes`, `hive`, `icebergS3`, `timeSeriesSamples`); and a `CREATE` / `ATTACH` / `REPLACE` `TABLE` or `VIEW` whose `ENGINE` is outside the engines' allowlist (the MergeTree family, `Replicated*` only without arguments, `Memory`, `Log`, `TinyLog`, `StripeLog`, `Null`, `Set`, `Join`, `View`, `MaterializedView`, `LiveView`). A name followed by `(` in a table-name position (`INSERT INTO db1.cluster (a) …`) is not a call.
- `identifier_placeholder`: any `{name:Identifier}` query parameter, in any position.
- `escaped_identifier`: a backslash inside a backtick or double-quoted identifier. Backslashes in string literals are allowed; the guard compares a single-quoted literal as the value ClickHouse decodes (`'\x70hys.t'` is `phys.t`).
- `scan`: a statement the guard cannot scan with certainty: a `$` that opens no heredoc or directly follows an identifier character, a `#` that opens no comment (only `# ` and `#!` do), an unterminated quote, comment or heredoc, a `\x` escape without two hex digits, or a non-ASCII byte outside a quoted identifier, string literal, comment or heredoc.

A refusal reaches the client as an Exception (relay default code 403) reading `table-reference guard: <rule>: <detail>; the rewriter applies the same policy`, for example `table-reference guard: physical_database: protected database phys is not addressable; the rewriter applies the same policy`. The rewriter enforces the same policy whatever the guard does; the guard is defence in depth and refuses only what it can decide lexically.

Known safe false refusals: a statement starting with a UTF-8 byte-order mark (`scan`; ClickHouse accepts it), a column called `database` aliased with the physical database's name (`SELECT database phys`), a tuple element written like a qualifier (`phys.1`), and a column-position `Identifier` parameter. Rewrite such statements, or use a direct ClickHouse connection for operator work.

```yaml
tableref_guard:
  mode: enforce   # default (also when empty); observe logs and counts instead of refusing. Case-sensitive.
```

Every hit, refused or observed, increments `clickhouse_proxy_tableref_guard_rejections_total{rule="…"}` (`rule` is one of the six names above); `observe` also logs a warning naming the rule. Rollout (spec §11): run one release cycle with `mode: observe` on devnet2 and require `rule="identifier_placeholder"` and `rule="escaped_identifier"` to stay at zero for driver and processor traffic before switching production to `enforce`.

## Sessions that connect to the physical database

The Sentio indexer driver connects through its agent sidecar with the ClientHello database set to `rewriter.physical_database` (production `charts/sentio-node` sets `housegate_dsn` to `clickhouse://default@<sidecar>/<physicalDatabase>`), and other internal services do the same. That name is not a logical database, so HouseGate gives such a session no logical context: every rewriter request carries an empty `upstream_logical_database_in_context`. The engines then rewrite qualified logical names (`` `p1_0`.`events` `` reads ``phys.`p1_0.events` ``), pass the allowlisted `system` reads that bind the physical name as a string literal (`system.tables WHERE database = 'phys'`), and refuse an unqualified table name (`unqualified table "<t>" does not resolve through the session's logical database`). Before this was fixed the session's logical context was the physical database itself, which is in `protected_databases`, and the v0.16.0 engines refused every statement on it. The guard and the Query-packet check run on these sessions as on any driver session; the guard still refuses the physical database as a qualifier.

Measured on the native engine (rewriter-go v0.16.0) with the driver's own statement shapes (sentio-core `common/chx` and `common/clickhousemanager`), through a real sidecar and relay (`TestTableReference_SentioDriverPhysicalHelloDatabase`): `CREATE DATABASE IF NOT EXISTS`, `CREATE TABLE … ENGINE = MergeTree()`, the `ALTER TABLE` column, comment and setting changes, batch `INSERT`, `INSERT … SELECT`, lightweight `DELETE`, `ALTER TABLE … DELETE`, `DROP VIEW` and the `system.tables`, `columns`, `data_skipping_indices`, `projections`, `parts` and `mutations` reads all pass. Four driver shapes are refused by the native engine with `UnsupportedStatement` whatever the session context; these are engine parse gaps, not the table-reference policy:

- the cluster probe (`SELECT cluster FROM (… WHERE cluster not like 'all-%' …)`); the driver logs the failure and runs without a cluster;
- `CREATE OR REPLACE VIEW … AS (…) COMMENT '…'`;
- `CREATE MATERIALIZED VIEW … TO … AS (…) COMMENT '…'`;
- the patch-part probe before a lightweight delete (`… AND startsWith(name, 'patch-')`).

These shapes have not been measured on rewriter-grpc 0.16.0, the engine the HouseGate deployments run; measure them against the released image together with the startup probe smoke before the rollout.

A cluster-aware driver is not supported. When the ClickHouse behind HouseGate reports a cluster with more than one replica and a local one (`system.clusters`, names starting with `all-` excluded), the driver adds `ON CLUSTER` to its DDL, issues `SYSTEM SYNC REPLICA ON CLUSTER`, and creates `ReplicatedMergeTree('/clickhouse/tables/…', '{replica}')` tables; the engines and the guard refuse a `Replicated*` engine with arguments, and the engines refuse `SYSTEM`. As of `sentioxyz/production` `218c3b43a` no HouseGate-fronted ClickHouse has such a cluster: devnet2 and testnet-v2 run Altinity installations whose `node-a` / `node-b` clusters are one shard of one replica each, and so are the storage-integrity `source` / `verifier-*` clusters. Keep it that way, or extend the policy first.

## Settings in the native Query packet

ClickHouse clients can send per-query settings in the native Query packet, outside the SQL text, where the rewriter never sees them; `clickhouse-client` copies a `SETTINGS` clause and command-line settings there, and expands `--compatibility=21.1` into `compatibility`, `enable_global_with_statement = 0`, `legacy_column_name_of_tuple_literal = 1` and `allow_experimental_analyzer = 0`. HouseGate refuses, before forwarding, the settings the rewriter refuses in SQL:

- whatever the value: `additional_table_filters`, `additional_result_filter`, `parallel_replicas_custom_key`, `dialect`, `polyglot_dialect`, `allow_experimental_polyglot_dialect`, `allow_experimental_prql_dialect`, `allow_experimental_kusto_dialect`, any name ending in `_dialect`, `enable_global_with_statement`, `compatibility`, `implicit_table_at_top_level`, `promql_table`, `promql_database`, `legacy_column_name_of_tuple_literal` and `profile`;
- `enable_analyzer` / `allow_experimental_analyzer` unless the value is exactly `1`, `true`, `'1'` or `'true'` (any case, no other spelling);
- any setting in the old (pre-54429) Query-packet format, whatever its name or value; only very old clients send it.

Names are compared case-insensitively. The client receives an Exception `table setting <name> is not accepted (native-protocol query setting)` (code 403); the connection stays usable for the next query. The check runs on the same servers as the guard, on ordinary and driver sessions, on sessions this server forwards to a peer and on sessions forwarded to it from a peer; it skips maintenance, platform-operator and `remote()` peer sessions. It has no observe mode. Each refusal is logged at warn level (`query refused: native-protocol query setting is not accepted`, with the setting, query id, connection, account and user) and counted in `clickhouse_proxy_query_settings_rejections_total{setting="…"}`, labelled with the lowercased name, `other_dialect` for another `_dialect` name, or `old_format`.

## Keep the analyzer on in every ClickHouse settings profile

The rewriter's name-binding rules are proven for the new analyzer only; the old analyzer (and settings such as `compatibility` that restore old defaults: `compatibility = 21.1` turns `enable_analyzer` off and `enable_global_with_statement` off) can make a name the rewriter trusted read another tenant's table. Neither the rewriter nor HouseGate can see a ClickHouse user's settings profile, so every profile a HouseGate session can use (the profile of the shared user in `users.xml` / `users.d`, its parent profiles, and any profile a `SETTINGS PROFILE` grant attaches) must keep the analyzer on and must not set any setting in the list above. ClickHouse 24.3 and later default `enable_analyzer` to `1`.

Check every ClickHouse server behind HouseGate. Run the first query connected as the user HouseGate uses, since it shows that user's effective defaults; the second lists every profile on the server:

```sql
SELECT name, value, changed
FROM system.settings
WHERE name IN ('enable_analyzer', 'allow_experimental_analyzer', 'compatibility', 'enable_global_with_statement',
               'legacy_column_name_of_tuple_literal', 'implicit_table_at_top_level', 'dialect',
               'additional_table_filters', 'additional_result_filter', 'parallel_replicas_custom_key',
               'promql_table', 'promql_database')
   OR endsWith(name, '_dialect');

SELECT profile_name, user_name, role_name, setting_name, value
FROM system.settings_profile_elements
WHERE setting_name IN ('enable_analyzer', 'allow_experimental_analyzer', 'compatibility', 'enable_global_with_statement',
                       'legacy_column_name_of_tuple_literal', 'implicit_table_at_top_level', 'dialect',
                       'additional_table_filters', 'additional_result_filter', 'parallel_replicas_custom_key',
                       'promql_table', 'promql_database')
   OR endsWith(setting_name, '_dialect');
```

Expected: `enable_analyzer` and `allow_experimental_analyzer` are `1`, every other row of the first query has `changed = 0`, and the second query returns no row, or only `enable_analyzer` / `allow_experimental_analyzer` set to `1`. Measured on ClickHouse 25.8: a default server passes; a `users.d` file setting `<compatibility>21.1</compatibility>` in the `default` profile shows `compatibility`, `enable_analyzer`, `allow_experimental_analyzer`, `enable_global_with_statement`, `legacy_column_name_of_tuple_literal` and the PRQL / Kusto dialect switches as changed in the first query and `compatibility` in the second. Add this check to every ClickHouse deployment change; as of `sentioxyz/production` `d1a757627` no ClickHouse configuration sets any of these settings.

## Not covered yet

- The class (c) `system` tables the Sentio driver reads by physical name (`tables`, `columns`, `parts*`, `mutations`, …) still pass through verbatim, and `PermissionCommitGateObserver` still exempts every `system` read: step 2, housegate/housegate#218.
- `url` / `s3` / `file` and the other external-storage table functions are a non-goal: ClickHouse user grants own egress.
