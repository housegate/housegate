# Table-reference hardening: operator note

Spec: `2026-09-26-table-reference-hardening-design` (internal design doc) (§9 is the HouseGate half, §14 the execution record). Fail-closed rewriting and `rewriter.fail_open_on_unavailable` are described in [rewriter-fail-closed.md](rewriter-fail-closed.md).

The SQL rewriter engines enforce the table-reference policy: a tenant statement may only reach its own logical databases, never the physical database, the protocol-owned `hg_safe` / `hg_unsafe` / `hg_promote`, a `system` table outside the allowlist, a refused table function or engine, or an `Identifier` query parameter in a table position. HouseGate proves the engine at startup, sends it the protected namespace on every request, and adds two checks of its own in front of it: a lexical guard and a Query-packet settings check.

## Minimum engines and the startup probes

> **Deploy order (rewriter-go v0.23.4 / rewriter-grpc v0.21.3).** Upgrade the rewriter engine first: the `housegate-rewriter` sidecar image to `0.21.3`, or the native FFI library to v0.23.4. Only then roll out this HouseGate. It refuses to start against an older engine, because its startup probe now requires the unmapped-database refusal (see "Databases outside `database_map`"). An older HouseGate runs unchanged against the new engine; its clients see the engine's refusal where they used to see the permission observer's or ClickHouse's error.

HouseGate requires rewriter-go v0.23.4 (native engine, with the FFI library of the same release) or rewriter-grpc v0.21.3, the first releases that refuse a qualified read of a database outside the session's `database_map` (housegate/housegate#336; an older engine answers `Success` for the `unmapped-database-refused` probe case and forwards the SQL unchanged). The floor before that was rewriter-go v0.23.2 / rewriter-grpc v0.21.1, the first releases that refuse `system.completions` and `information_schema` (housegate/housegate#319; an older engine answers `Success` for the `system-completions-refused` and `information-schema-refused` probe cases). Before that it was rewriter-go v0.22.0 / rewriter-grpc v0.19.0, the first releases that refuse what a ClickHouse 26.8 backend honours (see "Settings in the native Query packet"); an older engine answers `Success` for the three `ch268-*` probe cases below. The earlier floor was v0.17.0, for these reasons. rewriter-go v0.15.0 is not enough: it answers `Success` for the old-analyzer switch (`SETTINGS allow_experimental_analyzer = 0`) and for a read inside a table-engine argument, both of which v0.16.0 refuses (spec §13). The v0.16.0 engines are not enough either: they refuse statements the Sentio indexer driver sends on every deployment. rewriter-go v0.16.0 refuses `startsWith(...)` (the patch-part probe before every lightweight delete) and every parenthesised `CREATE [MATERIALIZED] VIEW … AS (…) COMMENT '…'` with column comments (every entity schema); rewriter-grpc v0.16.0 refuses a single grouped literal tuple (`x IN ((1, 0, '0'))`) and, with storage integrity enabled, a `CREATE TABLE` with a `` `from` `` or `` `join` `` column before a parameterised type such as `DateTime64(6, 'UTC')` (rewriter-grpc v0.15.0 has the second gap too). The v0.17.0 releases (rewriter-go 24b54fb, rewriter-grpc b56ae37) answer all of these `Success`. The v0.17.0 FFI assets are built from the same Polyglot v0.13.0 commit as the v0.16.0 and v0.15.0 ones; the differences are in the engines, which is why HouseGate checks behaviour, not the library hash.

Every request carries `protected_databases = [rewriter.physical_database, hg_safe, hg_unsafe, hg_promote]`, whatever `storage_integrity.enabled` says, so the engine refuses those databases in every position.

Every server that has a rewriter, built from config or injected by an embedding host, runs the table-reference probe before it listens, with or without storage integrity: twenty-one fixed statements (an identifier parameter in a table position, the physical database as an IN operand, `USE` and a `merge()` argument, `hg_promote`, IN-operand and `INSERT … SELECT` source rewriting, a refused table function, an unmodelled statement class, a refused system table, `system.completions` and `information_schema.tables` (both refused since rewriter-go v0.23.2 / rewriter-grpc v0.21.1), `SELECT * FROM default.t` (refused since rewriter-go v0.23.4 / rewriter-grpc v0.21.3), the analyzer switch, an engine-argument subquery, three Sentio-driver shapes: `startsWith(...)`, a parenthesised view with a column `COMMENT` and a trailing `COMMENT`, and `a IN ((1, 2))`, and three ClickHouse 26.8 cases: `SETTINGS filter = …`, `pg_get_userbyid(1)` and the `_database` column) whose exact code, statement type, SQL and message must match the answers both engines were measured to give (the 26.8 cases on 2026-11-10 against rewriter-grpc v0.19.0's tree and rewriter-go v0.22.0). The first two driver cases refuse rewriter-go v0.16.0 and the third refuses rewriter-grpc v0.16.0. An engine that fails any case refuses startup:

```
rewriter table-reference probe (engine=grpc probe=driver-grouped-literal-tuple): code=UnsupportedStatement, want Success; deploy rewriter-go >= v0.23.4 or rewriter-grpc >= v0.21.3 (table-reference policy, spec 2026-09-26, its ClickHouse 26.8 refusals, the metadata-table and unmapped-database refusals, and Sentio-driver compatibility)
```

The error names the case and never quotes the probe SQL or the engine's text. `rewriter.fail_open_on_unavailable` does not cover a probe failure: it only lets a server start when the rewriter cannot be built at all. With storage integrity enabled, the eight storage-integrity probes run after it (their requests carry `protected_databases` too).

For the move from a pre-v0.16.0 engine (the physical-hello fix below), ship the rewriter engine and HouseGate together, in one rollout; never upgrade either alone. From an engine at or above the previous floor, upgrade the engine first, as the deploy-order note above says. In a sentio-node deployment that means one deploy that changes both the sentio-node image (which embeds this HouseGate) and the in-pod rewriter sidecar image (`housegate-rewriter`) to rewriter-grpc 0.19.0; a native deployment moves to rewriter-go v0.22.0 in the same release. Neither order works:

- **Engine first fails.** Every engine since v0.16.0 protects every `database_map` value, even when the request carries no `protected_databases`. An older HouseGate (before the physical-hello fix described below) gives a session that connects to `rewriter.physical_database` that physical database as its logical context, so against a v0.16.0 or v0.17.0 engine every Sentio driver statement is refused with `protected database <phys> is not addressable`.
- **HouseGate first fails.** This HouseGate refuses to start against an engine older than the floor above: the startup probe fails.

As of `sentioxyz/production` `218c3b43a` devnet2 indexer-a runs `housegate-rewriter:0.15.0`, other devnet2 overlays 0.14.0 and testnet-v2 0.11.0; each of them would stop this HouseGate from starting. The probe answers were measured through rewriter-grpc's gRPC service on the build box, not against a released image: on 2026-10-02 against b56ae37 (the v0.17.0 source), and the three ClickHouse 26.8 cases on 2026-11-10 against the v0.19.0 tree with housegate/rewriter#4. Run `TestReleasedGRPCStorageIntegrityProbeSmoke` (which also runs the table-reference probe) against the released image before the rollout; it passes against `housegate-rewriter:0.21.3` (2026-11-11), and `0.21.2` fails it on `unmapped-database-refused`. Embedding hosts that inject their own rewriter factory must implement `rewriter.TableReferenceProbeFactory` (`ProbeTableReferencePolicy`); test doubles can answer the probe with `rewriter.TableReferenceProbeAnswer`.

## The table-reference guard (`tableref_guard`)

A lexical guard runs on the original SQL before `forward` and `rewrite`, on every server that forwards to ClickHouse (a `shard`, an `upstream` or a host-injected cluster), whether or not a rewriter was built: on a server started under `rewriter.fail_open_on_unavailable` without its rewriter, the guard is the only policy layer left. Router-only servers forward whole sessions to peers and do not run it. It checks ordinary sessions, including the indexer driver, sessions this server forwards to a peer, and sessions forwarded to it from a peer: the receiving server owns their original client SQL and checks it against its own `rewriter.physical_database`, and the origin may be a router-only server, which runs no guard. It skips maintenance and platform-operator sessions (the storage-integrity `sireserved` guard covers those) and `remote()` peer loopbacks, whose SQL the origin already rewrote.

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

The Sentio indexer driver connects through its agent sidecar with the ClientHello database set to `rewriter.physical_database` (production `charts/sentio-node` sets `housegate_dsn` to `clickhouse://default@<sidecar>/<physicalDatabase>`), and other internal services do the same. That name is not a logical database, so HouseGate gives such a session no logical context: every rewriter request carries an empty `upstream_logical_database_in_context`. The engines then rewrite qualified logical names (`` `p1_0`.`events` `` reads ``phys.`p1_0.events` ``), pass the allowlisted `system` reads that bind the physical name as a string literal (`system.tables WHERE database = 'phys'`), and refuse an unqualified table name (`unqualified table "<t>" does not resolve through the session's logical database`). Before this was fixed the session's logical context was the physical database itself, which is in `protected_databases`, and the v0.16.0 engines refused every statement on it (this is why the engine and HouseGate must ship together, see above). The guard and the Query-packet check run on these sessions as on any driver session; the guard still refuses the physical database as a qualifier.

Measured on the native engine (rewriter-go v0.17.0) with the driver's own statement shapes (sentio-core `common/chx` and `common/clickhousemanager`), through a real sidecar and relay (`TestTableReference_SentioDriverPhysicalHelloDatabase`): `CREATE DATABASE IF NOT EXISTS`, `CREATE TABLE … ENGINE = MergeTree()`, `CREATE OR REPLACE VIEW` and `CREATE MATERIALIZED VIEW … TO` with parenthesised bodies, column comments and a trailing `COMMENT` (both comments reach ClickHouse), the `ALTER TABLE` column, comment and setting changes, batch `INSERT`, `INSERT … SELECT`, lightweight `DELETE`, `ALTER TABLE … DELETE`, `DROP VIEW`, the cluster probe (`… WHERE cluster not like 'all-%' …`), the patch-part probe (`… AND startsWith(name, 'patch-')`) and the `system.tables`, `columns`, `data_skipping_indices`, `projections`, `parts` and `mutations` reads (including the escaped-`_` `LIKE` prefix) all pass; the test requires every one to succeed. rewriter-go v0.16.0 refused four of these shapes (the cluster probe, both view `COMMENT` forms and `startsWith`) as `UnsupportedStatement`.

Both engines also carry a regression test over the driver statement corpus captured from sentio-core (rewriter-go `TestDriverStatementCompat`, rewriter-grpc `DriverCompat.EveryDriverStatementIsAccepted`, driver-session request, storage integrity off and on). On v0.17.0 every statement answers `Success` except the 44 external-table filter statements (`` IN (SELECT s FROM `filter_<hex>`) ``, refused with `InvalidRewriteRequest`, a known deferred limitation) and four statements the driver cannot reach today (three cluster-aware forms, see below, and one unescaped raw literal); these were measured in the engine repositories, not through HouseGate.

A cluster-aware driver is not supported. When the ClickHouse behind HouseGate reports a cluster with more than one replica and a local one (`system.clusters`, names starting with `all-` excluded), the driver adds `ON CLUSTER` to its DDL, issues `SYSTEM SYNC REPLICA ON CLUSTER`, and creates `ReplicatedMergeTree('/clickhouse/tables/…', '{replica}')` tables; the engines and the guard refuse a `Replicated*` engine with arguments, and the engines refuse `SYSTEM`. As of `sentioxyz/production` `218c3b43a` no HouseGate-fronted ClickHouse has such a cluster: devnet2 and testnet-v2 run Altinity installations whose `node-a` / `node-b` clusters are one shard of one replica each, and so are the storage-integrity `source` / `verifier-*` clusters. Keep it that way, or extend the policy first.

## Settings in the native Query packet

ClickHouse clients can send per-query settings in the native Query packet, outside the SQL text, where the rewriter never sees them; `clickhouse-client` copies a `SETTINGS` clause and command-line settings there, and expands `--compatibility=21.1` into `compatibility`, `enable_global_with_statement = 0`, `legacy_column_name_of_tuple_literal = 1` and `allow_experimental_analyzer = 0`. HouseGate refuses, before forwarding, the settings the rewriter refuses in SQL:

- whatever the value: `additional_table_filters`, `additional_result_filter`, `parallel_replicas_custom_key`, `dialect`, `polyglot_dialect`, `allow_experimental_polyglot_dialect`, `allow_experimental_prql_dialect`, `allow_experimental_kusto_dialect`, any name ending in `_dialect`, `enable_global_with_statement`, `compatibility`, `implicit_table_at_top_level`, `promql_table`, `promql_database`, `legacy_column_name_of_tuple_literal` and `profile`;
- whatever the value, the settings a ClickHouse 26.8 server honours: `select`, `filter`, `order`, `sort` (SQL text the server wraps around the query), `database` (the current database, like `USE`), `format`, `input_format`, `output_format`, `default_format`, `asterisk_include_virtual_columns` (adds the physical `_database` to `SELECT *`), `s3_allow_server_credentials_in_user_queries`, any name starting with `dynamic_disk_allow_`, `url_base`, `s3_base`, `analyzer_compatibility_allow_cte_redefinition`, `analyzer_compatibility_prefer_alias_over_subcolumn`, `analyzer_compatibility_multiple_joins_qualify_column_names`, `analyzer_inline_views`, `enable_streaming_queries`, `ai_function_text_default_credentials` and `ai_function_embedding_default_credentials`. One exception: `database` naming the session's current logical database is removed from the packet and the query goes on. A 26.8 `clickhouse-client` copies `--database` (or a configured database) into that setting on every query and follows each `USE`, so it names the database HouseGate already holds the session in, whose mapping is the upstream session's current database; forwarding the logical name would point a 26.8 backend at it unmapped. Any other value is refused, and so is the setting on a session with no logical database (one that connected to `rewriter.physical_database` or named none) and, on a server without a rewriter, after an interactive `USE` of a database this server hosts (the rewriter tracks such a `USE`; the forward plugin tracks one that routes the session to a peer, once the peer has answered it with success). The same client sends `--format` as a format setting, which is refused: pick the output format in the SQL text (`FORMAT …`) instead;
- `enable_analyzer` / `allow_experimental_analyzer` unless the value is exactly `1`, `true`, `'1'` or `'true'` (any case, no other spelling);
- any setting in the old (pre-54429) Query-packet format, whatever its name or value; only very old clients send it.

Names are compared case-insensitively. The client receives an Exception `table setting <name> is not accepted (native-protocol query setting)` (code 403); the connection stays usable for the next query. The check runs on the same servers as the guard, on ordinary and driver sessions, on sessions this server forwards to a peer and on sessions forwarded to it from a peer; it skips maintenance, platform-operator and `remote()` peer sessions. It has no observe mode. Each refusal is logged at warn level (`query refused: native-protocol query setting is not accepted`, with the setting, query id, connection, account and user) and counted in `clickhouse_proxy_query_settings_rejections_total{setting="…"}`, labelled with the lowercased name, `other_dialect` for another `_dialect` name, `dynamic_disk_allow` for a `dynamic_disk_allow_` name, or `old_format`.

## Keep the analyzer on in every ClickHouse settings profile

The rewriter's name-binding rules are proven for the new analyzer only; the old analyzer (and settings such as `compatibility` that restore old defaults: `compatibility = 21.1` turns `enable_analyzer` off and `enable_global_with_statement` off) can make a name the rewriter trusted read another tenant's table. Neither the rewriter nor HouseGate can see a ClickHouse user's settings profile, so every profile a HouseGate session can use (the profile of the shared user in `users.xml` / `users.d`, its parent profiles, and any profile a `SETTINGS PROFILE` grant attaches) must keep the analyzer on and must not set any setting in the list above. ClickHouse 24.3 and later default `enable_analyzer` to `1`.

Check every ClickHouse server behind HouseGate. Run the first query connected as the user HouseGate uses, since it shows that user's effective defaults; the second lists every profile on the server:

```sql
SELECT name, value, changed
FROM system.settings
WHERE name IN ('enable_analyzer', 'allow_experimental_analyzer', 'compatibility', 'enable_global_with_statement',
               'legacy_column_name_of_tuple_literal', 'implicit_table_at_top_level', 'dialect',
               'additional_table_filters', 'additional_result_filter', 'parallel_replicas_custom_key',
               'promql_table', 'promql_database', 'select', 'filter', 'order', 'sort', 'database', 'format',
               'input_format', 'output_format', 'default_format', 'asterisk_include_virtual_columns',
               's3_allow_server_credentials_in_user_queries', 'url_base', 's3_base',
               'analyzer_compatibility_allow_cte_redefinition', 'analyzer_compatibility_prefer_alias_over_subcolumn',
               'analyzer_compatibility_multiple_joins_qualify_column_names', 'analyzer_inline_views',
               'enable_streaming_queries', 'ai_function_text_default_credentials',
               'ai_function_embedding_default_credentials')
   OR endsWith(name, '_dialect') OR startsWith(name, 'dynamic_disk_allow_');

SELECT profile_name, user_name, role_name, setting_name, value
FROM system.settings_profile_elements
WHERE setting_name IN ('enable_analyzer', 'allow_experimental_analyzer', 'compatibility', 'enable_global_with_statement',
                       'legacy_column_name_of_tuple_literal', 'implicit_table_at_top_level', 'dialect',
                       'additional_table_filters', 'additional_result_filter', 'parallel_replicas_custom_key',
                       'promql_table', 'promql_database', 'select', 'filter', 'order', 'sort', 'database', 'format',
                       'input_format', 'output_format', 'default_format', 'asterisk_include_virtual_columns',
                       's3_allow_server_credentials_in_user_queries', 'url_base', 's3_base',
                       'analyzer_compatibility_allow_cte_redefinition', 'analyzer_compatibility_prefer_alias_over_subcolumn',
                       'analyzer_compatibility_multiple_joins_qualify_column_names', 'analyzer_inline_views',
                       'enable_streaming_queries', 'ai_function_text_default_credentials',
                       'ai_function_embedding_default_credentials')
   OR endsWith(setting_name, '_dialect') OR startsWith(setting_name, 'dynamic_disk_allow_');
```

Expected: `enable_analyzer` and `allow_experimental_analyzer` are `1`, every other row of the first query has `changed = 0`, and the second query returns no row, or only `enable_analyzer` / `allow_experimental_analyzer` set to `1`. Measured on ClickHouse 25.8: a default server passes; a `users.d` file setting `<compatibility>21.1</compatibility>` in the `default` profile shows `compatibility`, `enable_analyzer`, `allow_experimental_analyzer`, `enable_global_with_statement`, `legacy_column_name_of_tuple_literal` and the PRQL / Kusto dialect switches as changed in the first query and `compatibility` in the second. Add this check to every ClickHouse deployment change; as of `sentioxyz/production` `d1a757627` no ClickHouse configuration sets any of these settings.

## SHOW TABLES

`SHOW TABLES [{FROM | IN} <db>]` is answered from the tenant's own tables: the engine reads `system.tables` of the physical database, keeps the names that start with `<logical>.` and strips that prefix. From rewriter-go v0.23.0 / rewriter-grpc v0.20.1 (housegate/housegate#319) the same mapping also answers `[NOT] LIKE` / `ILIKE '<pattern>'`, `LIMIT <n>` and `SHOW FULL TABLES` (which adds the `engine` column): the pattern and the limit apply to the logical name, rows are ordered by name as ClickHouse's own `SHOW TABLES` is, and another tenant's table in the same physical database never appears. Older engines refuse these forms with `statement is not supported`, which is safe, so the probe floor above is unchanged. Still refused: `SHOW TEMPORARY TABLES` (session tables, no database filter covers them), `SHOW EXTENDED TABLES`, `WHERE`, `FORMAT`, `SETTINGS`, `INTO OUTFILE`, an empty pattern, and (native engine only) a `LIMIT` that is not a plain unsigned decimal. `TestTableReference_ShowTablesLikeLimitFull` runs these through a proxy on the native engine.

## Metadata tables

Every tenant reaches ClickHouse as one shared user, so ClickHouse's own per-user filtering of its metadata tables filters nothing between tenants. From rewriter-go v0.23.2 / rewriter-grpc v0.21.1 (housegate/housegate#319) both engines refuse, in every position and with or without storage integrity:

- `system.completions`, `system.parts_columns`, `system.projection_parts` and `system.projection_parts_columns`: `system table system.<t> is not accessible; tenant-filtered metadata reads are tracked in housegate/housegate#218`. They show every tenant's database, table and column names (the `parts*` tables also their partition values), and no client is known to read them through HouseGate: the Sentio driver reads only the class (c) tables listed under "Not covered yet".
- every table of `information_schema` / `INFORMATION_SCHEMA`: `database <db> is not accessible; tenant-filtered metadata reads are tracked in housegate/housegate#218`. Its views read `system.tables`, `system.columns` and `system.databases`. With `auth.enabled` the permission observer already refused these reads, because `information_schema` is not a registered database; the engine refusal also covers auth-disabled servers.

An interactive `clickhouse-client` 25.8 or newer loads its suggestions from `system.completions` over a second connection. Through HouseGate that load is refused and the client prints `Cannot load data for command line suggestions: …` once at startup; the session works and keyword completion stays. Start the client with `-A` (`--disable_suggestion`) to skip the load. Serving `system.completions` with only its global contexts (functions, settings, keywords), so that completion works again, is tracked in housegate/housegate#319. `TestTableReference_MetadataTablesRefused` runs these refusals through a proxy on the native engine.

## Databases outside `database_map`

A qualified table name is read by ClickHouse as `<database>.<table>` under the one ClickHouse user every tenant shares. Tenant tables live in the physical database (`phys."db1.t"`), so a logical name the session cannot resolve names some other database on the server: `default`, or one another workload keeps there. From rewriter-go v0.23.4 / rewriter-grpc v0.21.3 (housegate/housegate#336) both engines refuse a read-position table whose database is not exactly `system` and is not in the session's `database_map`, in every read position (FROM, JOIN, `IN (SELECT …)` and `IN <table>`, scalar and `EXISTS` subqueries, CTE bodies, UNION arms, `view()`, and the bodies of `INSERT … SELECT`, `CREATE TABLE … AS SELECT`, views and materialized views), with or without storage integrity:

```
database "<db>" does not resolve through the caller's databases (it is not mapped for this account, or the account has no access)
```

The code is `InvalidRewriteRequest` (`UnsupportedStatement` inside an `INSERT … SELECT` / CTAS body). Before these releases the engines forwarded such a read verbatim, and only the permission observer (with `auth.enabled`) and the shared user's ClickHouse grants stopped it; on an auth-disabled server, nothing in HouseGate did. Write targets, `DESCRIBE`, `EXISTS`, `SHOW`, `USE` and the string lookups (`joinGet`, `dictGet*`, `hasColumnInTable`) already refused these names and keep their own messages; `remote()`, `cluster()` and `merge()` were already refused as table functions.

What changes for clients:

- With `auth.enabled`, `database_map` holds only the databases the account may read or write, so a read of a registered database the account has no access to now gets this message instead of the permission observer's `permission: account … lacks read on database …`.
- ClickHouse names are case-sensitive: `SYSTEM.processes` and `Information_Schema.tables` name databases that do not exist, and are refused with this message.
- With an empty `rewriter.physical_database` the map is empty, so every qualified read except `system` is refused.
- The Sentio driver reads only its own logical databases and `system`, and is unaffected.
- A database granted only to the public address (`0x000…000`) is not mapped yet, so its reads are refused: housegate/housegate#340.

`TestTableReference_UnmappedDatabaseRefused` runs these refusals through a proxy on the native engine with auth off, against a real table in ClickHouse's `default` database.

## The upstream ClickHouse user's grants (`upstream_grants_check`)

Every session HouseGate forwards to ClickHouse runs as one ClickHouse user: the credential provider's default credential (`ckh_manager_config_path`, `credential.subgraph`). That includes tenant sessions, the maintenance and platform-operator sessions the rewriter never sees, and peer `remote()` loopbacks. The engines refuse what they model. That user's grants are what stops everything else, for example the external table functions the engines leave alone (`url`, `s3`, `file`, …). From this release (housegate/housegate#341), a server that forwards to ClickHouse (a `shard` or an `upstream`) runs `SHOW GRANTS FINAL` (and reads `system.enabled_roles` for role admin rights) as that user. It does so at startup and every 10 minutes, and reports every grant outside this set:

| Target | Expected privileges |
|---|---|
| `rewriter.physical_database` (`<phys>.*` or a table in it) | any |
| `hg_safe.*`, `hg_unsafe.*` | `SELECT` / `SHOW …`, with storage integrity enabled |
| `hg_replica.*` | `SELECT` / `SHOW …`, with storage integrity enabled and `upstream_grants_check.replica_copies: true` (the server serves stage-3 replica copies) |
| `system.<table>` (column lists included) | `SELECT` / `SHOW …` |
| `*.*` | `REMOTE` (`GRANT READ ON REMOTE`: the `remote()` reads of other indexers' databases) and `CREATE TEMPORARY TABLE` (`GLOBAL IN`, external tables) |
| `TABLE ENGINE ON <engine>` | an engine the rewriter accepts (the MergeTree family and its Replicated forms, `Memory`, `Log`, …) |

Everything else is reported, for example:
- any other database (`default.*`, `hg_promote.*`, …);
- any other `*.*` privilege (`SELECT`, `FILE`, `URL`, `S3`, `SOURCES`, `SYSTEM`, `KILL QUERY`, …);
- user, role and named-collection privileges (`ON *`) and `TABLE ENGINE ON *`;
- database or table patterns (`ph*.*`);
- `WITH GRANT OPTION`, and a role held `WITH ADMIN OPTION`.

`FINAL` lists the privileges a user holds through its roles, so a role cannot hide a grant. The check first runs `SET ROLE ALL` on its connection, so non-default roles count too (FINAL expands them either way). `FINAL` drops the role grants themselves, so the check also reads `system.enabled_roles` for every role held `WITH ADMIN OPTION`, directly or inherited through a parent role. Column lists are parsed with ClickHouse's identifier quoting, so a quoted column such as `` SELECT(`(`) `` cannot hide the privilege after it. `REVOKE` lines are ignored: a partial revoke does not narrow a grant the set does not expect. The `system` database stays readable without a grant (`select_from_system_db_requires_grant` is off by default); the engines' system-table allowlist decides those reads. The only maintenance statement sentio-node sends is the database GC's `DROP TABLE IF EXISTS <phys>.<db>.<table> SYNC`, which the physical-database row covers.

`upstream_grants_check.mode`:
- `warn` (the default): the startup check runs in the background, and every violation is logged (`upstream grants check: …`, with the list) and counted in the gauge `clickhouse_proxy_upstream_grant_violations{upstream}`. A failed read is counted in `clickhouse_proxy_upstream_grant_check_errors_total{upstream}`.
- `enforce`: as `warn`, except that a violation found at startup refuses startup. A ClickHouse that cannot be read is never fatal, and a 10-minute re-check never stops a running server.
- `off`: no check.

The check is skipped when:
- the ClickHouse cluster is host-injected;
- the server is router-only;
- `credential_replace_enabled` is off, so each client brings its own credentials.

```yaml
upstream_grants_check:
  mode: warn            # warn (default) | enforce | off
  replica_copies: false # true when this server serves hg_replica copies
```

A deployment that still forwards as a superuser (`default`, or a user with `GRANT ALL ON *.*`) is reported, not refused, in `warn` mode. Narrow the user before setting `enforce`. `TestUpstreamGrantsCheck_RealClickHouse` checks a narrowed gateway user, the same user with a `default.*` grant, a `url()` source and a role grant, on ClickHouse 25.8 and 26.8.

## Not covered yet

- The class (c) `system` tables the Sentio driver reads by physical name (`tables`, `columns`, `data_skipping_indices`, `projections`, `parts`, `databases`, `mutations`) still pass through verbatim, and `PermissionCommitGateObserver` still exempts every `system` read: step 2, housegate/housegate#218. The metadata tables no client needs are refused instead (see "Metadata tables" above).
- `url` / `s3` / `file` and the other external-storage table functions are a non-goal for the engines: ClickHouse user grants own egress, and `upstream_grants_check` reports a `URL` / `S3` / `FILE` / `SOURCES` grant on the upstream user.
