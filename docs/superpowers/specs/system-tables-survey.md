# system tables survey: what housegate tenants can see, who reads it, what to refuse

Date: 2026-09-30. Read-only survey supporting the decision to refuse `SHOW PROCESSLIST`, `SHOW MERGES`, `SHOW DICTIONARIES FROM <unmapped>`, the `system` tables behind them, and every `system` table that exposes another tenant's data or activity.

## TL;DR

- **The premise "ClickHouse enforces per-row visibility" is false for housegate.** Every tenant reaches ClickHouse as the same user, so ClickHouse's own filters (`processes` shows only your user's queries, `tables` shows only databases you can see) filter nothing. Measured on 26.2 and 25.8: a tenant-1 session sees tenant 2's query text and query_id, table and column names, partition values, mutation commands, merges, dictionary sources and errors. The table-reference hardening design ([2026-09-26-table-reference-hardening-design.md](2026-09-26-table-reference-hardening-design.md), non-goal "Reads of `system.*`") and the comment on `systemDatabase` in housegate `pkg/network/permission_commitgate_observer.go:246-255` both rest on that premise.
- **Class (a), cross-tenant sensitive, 57 tables**, including 7 that are config-dependent and absent from the default image. See [Class (a) list](#class-a-list-refuse).
- **Class (c), per-database metadata, 15 tables** plus the `information_schema` views. They show every tenant's names, and `parts*` also show partition *values*, which are data.
- **Today the rewriter admits every `system` table in every position, in both SI modes.** Some forms bypass the rewriter entirely when SI is disabled: `viewIfPermitted(...)` fails to parse, and `merge('system', …)` / `remote(…, system.x)` / `SHOW TABLES LIKE` / `SHOW FULL TABLES` / `KILL QUERY` come back as `UnsupportedStatement`. With SI off, both outcomes forward the original SQL unchanged.
- **The only production tenant-path consumer is the Sentio driver (network_v1).** It reads `tables`, `columns`, `data_skipping_indices`, `projections`, `parts`, `mutations` and `clusters` through housegate. It filters on the *physical* database name and the `<logical>.` table-name prefix. All of housegate's own reads (metrics collector, SI guards, schema registry, relay completion probe) use direct connections, not the rewriter.

## Method

- Containers `clickhouse/clickhouse-server:26.2` (26.2.15.4) and `:25.8` (25.8.28.1), default config, the `default` user as the single housegate user.
- One physical database `phys`. Tenant 2 got `phys."db2.orders_T2NAME"`, which has a skip index, a projection, a partition key over a data column, column and table comments, and three inserts. Tenant 2 also got a view, an MV with a `TO` table, a dictionary source table, two dictionaries (one with a password, one loaded), a dropped table, a detached partition and a 30M-row `db2.big_T2NAME` for live merges. Tenant 1 got a mirror table.
- Tenant-2 activity: a SELECT with a comment and literal, a query-cache query, a synchronous and an asynchronous mutation, `OPTIMIZE … FINAL`, an async insert, a failing query, a query with an explicit query_id, a long-running query that stayed live during the probe, and a `query_condition_cache` query on 25.8.
- Markers: `T2NAME` = names (tables, columns, indexes, comments), `QRYT2` = query text, `qidT2` = query_id, `PARTT2V` = partition value (built with `concat` so it never appears literally in a query), `DATAT2V` = a row value (also built with `concat`).
- Probe: `SELECT * FROM system.<t> LIMIT 200000` for every row of `system.tables WHERE database='system'`, from a separate session, grepping for each marker. `merges` and `processes` were also sampled live while an `OPTIMIZE FINAL` and a mutation of `db2.big_T2NAME` were running, together with `SHOW MERGES` and `SHOW PROCESSLIST`. `asynchronous_loader` was re-read after a server restart.
- SHOW mapping: each SHOW form was run once, and the internal `SELECT` ClickHouse executes for it was read back from `system.text_log` (`(internal) SELECT … FROM system.x`).
- Rewriter: rewriter-go worktree `rewriter-go.fix-cte-in-view` at `4b20d20`, `cmd/rewrite` built into scratch, FFI v0.14.0. The request is the one housegate sends: `database_map {db1: phys}`, `known_physical_databases [phys]`, delim `_`, logical context `db1`, physical context `phys`. The SI-on variant adds contract V2, one Active table `db1.t`, `READ_MODE_SAFE`, and reserved databases `hg_safe`/`hg_unsafe`/`hg_promote`. 220 statements, each run in both modes.
- Consumers: `rg` over sentio-core `5c3e324`, sentio `cc3666b2d`, sentio-node `36b5c0a`, housegate (this branch), and clickhouse-go `v2.43.0-sentioxyz` from the module cache. clickhouse-client's connect-time queries were captured from `query_log` of a real interactive session: the 26.2 and 25.8 image clients, run under `script`.

## Task 1: classification

26.2 has 137 system tables and 25.8 has 123; together that is 139 distinct names. 18 exist only in 26.2: `aggregated_zookeeper_log`, `azure_queue_metadata_cache`, `background_schedule_pool[_log]`, `database_replicas`, `delta_lake_metadata_log`, `fail_points`, `instrumentation`, `jemalloc_profile_text`, `jemalloc_stats`, `primes`, `s3queue_metadata_cache`, `tokenizers`, `unicode`, `user_defined_functions`, `zookeeper_connection_log`. 2 exist only in 25.8: `azure_queue` and `s3queue`. **The set grows every release, so a denylist goes stale. Enforce an allowlist.**

Classes: **a** = cross-tenant sensitive, refuse. **c** = per-database metadata, which shows every tenant's names unless filtered. **b\*** = global, but operator-sensitive or an aggregate side channel. It does not expose another tenant's rows; the recommendation is to refuse it anyway, except `clusters`. **b** = global server info.

Hit legend, per version: N = tenant-2 names, Q = tenant-2 query text, I = tenant-2 query_id, P = tenant-2 partition value, D = tenant-2 row value, `·` = rows but no tenant-2 marker, `0 rows` = empty in this setup (evidence comes from the schema), `err` = not configured, `—` = the table does not exist in that version. `merges` shows `0 rows` in the bulk probe; it was captured live separately, showing `db2.big_T2NAME` with its partition and progress, and so did `SHOW MERGES`. The row value `DATAT2V` appeared in **no** system table. Data leaks through partition values (`parts*`, `part_log`, `projection_parts`) and through query text that contains literals (`query_log`, `processes`, `mutations`, `text_log`, `errors`, `query_cache`, and the 25.8 `query_condition_cache` predicate).

| table | class | 26.2 | 25.8 | what crosses tenants |
|---|---|---|---|---|
| `aggregated_zookeeper_log` | a | 0 rows | — | 26.2: keeper parent_path (contains /clickhouse/tables/<db>/<table>) |
| `asynchronous_insert_log` | a | N | N | async insert query, database, table, query_id, exception |
| `asynchronous_inserts` | a | 0 rows | 0 rows | pending async inserts: query, database, table, entries.query_id |
| `asynchronous_loader` | a | · | · | job text "load table phys.db2.x" for every table after restart (measured); free text, not filterable |
| `azure_queue` | a | — | 0 rows | 25.8: AzureQueue per-table file status |
| `azure_queue_metadata_cache` | a | 0 rows | — | 26.2: AzureQueue file status |
| `background_schedule_pool` | a | N | — | 26.2: per-table background tasks: database, table, query_id |
| `background_schedule_pool_log` | a | 0 rows | — | 26.2: per-table background task history, exceptions |
| `backup_log` | a | 0 rows | 0 rows | backup history, query_id, error text |
| `backups` | a | 0 rows | 0 rows | backup names, query_id, error text |
| `blob_storage_log` | a | 0 rows | 0 rows | object-storage ops with query_id and local part paths |
| `crash_log` | a | 0 rows | 0 rows | query_id (26.2 also query text) of crashing queries |
| `delta_lake_metadata_log` | a | 0 rows | — | 26.2: query_id, table path, metadata content |
| `detached_tables` | a | 0 rows | 0 rows | names of detached tables; no consumer |
| `distributed_ddl_queue` | a | err | err | ON CLUSTER DDL query text of every tenant (error here: no keeper configured) |
| `distribution_queue` | a | 0 rows | 0 rows | Distributed-table pending data per table, last_exception |
| `dns_cache` | a | · | · | hosts resolved by the server, incl. hosts named in tenants' remote()/url() calls |
| `dropped_tables` | a | N | N | names of dropped tables still in the Atomic trash (measured); no consumer, refuse rather than filter |
| `dropped_tables_parts` | a | N | N | parts of dropped tables (measured) |
| `error_log` | a | NQI | · | 26.2: last_error_message + last_error_query_id; 25.8 schema has only codes/counters |
| `errors` | a | NQI | NQI | last_error_message (other tenant query text / names), query_id |
| `iceberg_metadata_log` | a | 0 rows | 0 rows | query_id, table path, metadata file content |
| `kafka_consumers` | a | 0 rows | 0 rows | Kafka tables: topics, offsets, exception text |
| `merges` | a | 0 rows | 0 rows | database, table, partition, result part of running merges and mutations (SHOW MERGES) |
| `moves` | a | 0 rows | 0 rows | running part moves: database, table, part |
| `mutations` | a | NQ | NQ | mutation command text (WHERE literals), table names, fail reasons |
| `named_collections` | a | 0 rows | 0 rows | connection definitions (hosts, credentials when display_secrets is on) shared by all tenants |
| `opentelemetry_span_log` | a | 0 rows | 0 rows | span attributes carry db.statement query text (empty: tracing off) |
| `part_log` | a | NP | NP | per-part insert/merge/mutation events: table, partition value, query_id, timings |
| `part_moves_between_shards` | a | 0 rows | 0 rows | part move tasks per table |
| `processes` | a | QI | QI | query text, query_id of every session of the shared user (SHOW PROCESSLIST) |
| `processors_profile_log` | a | NI | I | query_id; 26.2 plan_step_description carries table names |
| `query_cache` | a | NQ | NQ | cached query text (and result sizes) |
| `query_condition_cache` | a | · | 0 rows | 25.8: table_uuid, part_name, and plaintext WHERE predicate when query_condition_cache_store_conditions_as_plaintext=1 (measured); 26.2: hashes only |
| `query_log` | a | NQIP | NQIP | full query text, query_id, tables/columns accessed, exceptions |
| `query_metric_log` | a | I | I | query_id + per-query profile counters |
| `query_thread_log` | a | 0 rows | 0 rows | query text, query_id (empty: query-thread logging off by default) |
| `query_views_log` | a | N | N | view names, view_query text, initial_query_id |
| `replicas` | a | 0 rows | 0 rows | per-replicated-table state: names, keeper paths, queue sizes, merges_in_queue |
| `replicated_fetches` | a | 0 rows | 0 rows | running part fetches per table |
| `replication_queue` | a | 0 rows | 0 rows | replication/merge/mutation tasks per table, last_exception |
| `s3queue` | a | — | 0 rows | 25.8: S3Queue per-table file status |
| `s3queue_log` | a | 0 rows | 0 rows | S3Queue per-table file names processed, exceptions |
| `s3queue_metadata_cache` | a | 0 rows | — | 26.2: S3Queue file status, keeper path, exception |
| `schema_inference_cache` | a | 0 rows | 0 rows | source URLs/paths and inferred schemas of other tenants' file()/url()/s3() reads |
| `stack_trace` | a | I | I | query_id of running queries |
| `text_log` | a | NQIP | NQIP | server log lines: query text, table names, query_id |
| `trace_log` | a | I | I | query_id + stacks |
| `user_defined_functions` | a | 0 rows | — | 26.2: executable UDF config (commands); SQL UDF bodies are also in functions.create_query |
| `view_refreshes` | a | 0 rows | 0 rows | refreshable MV names, status, exception text, row counts |
| `columns` | c | N | N | column names, types, defaults, comments |
| `completions` | c | N | N | every database/table/column/dictionary name plus global words; read by clickhouse-client >= 25.x for autocomplete |
| `data_skipping_indices` | c | N | N | index names and expressions per table |
| `database_replicas` | c | 0 rows | — | 26.2: Replicated-database state (physical database only) |
| `databases` | c | · | · | database names (single physical db here; logical names only visible via tables) |
| `detached_parts` | c | N | N | detached part names per table |
| `dictionaries` | c | N | N | dictionary names, attribute names, source (db.table), status, element_count, last_exception (SHOW DICTIONARIES) |
| `iceberg_history` | c | 0 rows | 0 rows | Iceberg per-table snapshot history |
| `parts` | c | NP | NP | part names, partition VALUES (data), rows, bytes, min/max date/time, modification_time |
| `parts_columns` | c | NP | NP | as parts, per column |
| `projection_parts` | c | NP | NP | as parts, per projection (incl. partition values) |
| `projection_parts_columns` | c | N | N | as parts, per projection column |
| `projections` | c | N | N | projection names and query text |
| `rocksdb` | c | 0 rows | 0 rows | EmbeddedRocksDB per-table statistics |
| `tables` | c | N | N | names, engine_full, create_table_query (view/MV bodies, dictionary sources), partition/sorting keys, comments, total_rows/bytes |
| `asynchronous_metric_log` | b* | · | · | history of asynchronous_metrics |
| `asynchronous_metrics` | b* | · | · | server-wide gauges (NumberOfTables etc.) |
| `certificates` | b* | 0 rows | err | server TLS certificates |
| `clusters` | b* | · | · | cluster topology, hosts, ports (SHOW CLUSTERS); read by sentio chx on connect |
| `current_roles` | b* | 0 rows | 0 rows | SHOW CURRENT ROLES |
| `dimensional_metrics` | b* | · | 0 rows | server-wide labelled metrics |
| `disks` | b* | · | · | disk names and paths |
| `enabled_roles` | b* | 0 rows | 0 rows | SHOW ENABLED ROLES |
| `events` | b* | · | · | server-wide ProfileEvents counters (aggregate of all tenants) |
| `fail_points` | b* | · | — | 26.2: debug fail points |
| `filesystem_cache` | b* | 0 rows | 0 rows | cache keys/paths/sizes; no names, but reflects all tenants' reads |
| `filesystem_cache_settings` | b* | 0 rows | 0 rows | cache config (SHOW FILESYSTEM CACHES reads the cache registry) |
| `grants` | b* | · | · | the shared account's grants (e.g. GRANT ALL ON *.*); SHOW GRANTS reads access storage directly |
| `graphite_retentions` | b* | 0 rows | 0 rows | GraphiteMergeTree config |
| `histogram_metrics` | b* | · | · | server-wide histograms |
| `instrumentation` | b* | 0 rows | — | 26.2: XRay instrumentation points |
| `jemalloc_profile_text` | b* | 0 rows | — | 26.2: allocator profile |
| `macros` | b* | 0 rows | 0 rows | server macros (shard/replica names) |
| `metric_log` | b* | · | · | history of events/metrics |
| `metrics` | b* | · | · | server-wide gauges |
| `models` | b* | err | err | CatBoost models (errors: not configured) |
| `quota_limits` | b* | · | · | quota limits |
| `quota_usage` | b* | · | · | SHOW QUOTA: usage of the shared account = aggregate of all tenants |
| `quotas` | b* | · | · | SHOW QUOTAS |
| `quotas_usage` | b* | · | · | as quota_usage |
| `remote_data_paths` | b* | 0 rows | 0 rows | local/remote blob paths (store/<uuid>/...), no names |
| `resources` | b* | 0 rows | 0 rows | scheduling resources |
| `role_grants` | b* | 0 rows | 0 rows | role grants |
| `roles` | b* | 0 rows | 0 rows | SHOW ROLES |
| `row_policies` | b* | 0 rows | 0 rows | SHOW ROW POLICIES; policy expressions could name tables |
| `scheduler` | b* | 0 rows | 0 rows | scheduler nodes |
| `server_settings` | b* | · | · | full server config values (paths, hosts, limits) |
| `settings_profile_elements` | b* | · | · | profile contents |
| `settings_profiles` | b* | · | · | SHOW PROFILES |
| `storage_policies` | b* | · | · | storage policies |
| `symbols` | b* | · | · | binary symbols (introspection) |
| `user_directories` | b* | · | · | access storage config paths |
| `user_processes` | b* | · | · | memory/ProfileEvents per user = aggregate of all tenants (side channel) |
| `users` | b* | · | · | accounts, auth type, host restrictions (SHOW USERS) |
| `workloads` | b* | 0 rows | 0 rows | workload scheduling config |
| `zookeeper_connection_log` | b* | 0 rows | — | 26.2: keeper connection history |
| `aggregate_function_combinators` | b | · | · | static / server-global |
| `azure_queue_settings` | b | 0 rows | 0 rows | static / server-global |
| `build_options` | b | · | · | static / server-global |
| `codecs` | b | · | · | static / server-global |
| `collations` | b | · | · | static / server-global |
| `contributors` | b | · | · | static / server-global |
| `dashboards` | b | · | · | built-in dashboard queries over the *_log tables |
| `data_type_families` | b | · | · | static / server-global |
| `database_engines` | b | · | · | static / server-global |
| `formats` | b | · | · | static / server-global |
| `functions` | b | · | · | static function list; caveat: SQL UDFs created by any tenant appear with create_query |
| `jemalloc_bins` | b | · | · | static / server-global |
| `jemalloc_stats` | b | · | — | static / server-global |
| `keywords` | b | · | · | static / server-global |
| `licenses` | b | · | · | static / server-global |
| `merge_tree_settings` | b | · | · | static / server-global |
| `numbers` | b | · | · | static / server-global |
| `numbers_mt` | b | · | · | static / server-global |
| `one` | b | · | · | SELECT without FROM targets it implicitly |
| `primes` | b | · | — | static / server-global |
| `privileges` | b | · | · | static privilege list (SHOW PRIVILEGES) |
| `replicated_merge_tree_settings` | b | · | · | static / server-global |
| `s3_queue_settings` | b | 0 rows | 0 rows | static / server-global |
| `settings` | b | · | · | session settings (SHOW SETTINGS, SHOW CHANGED SETTINGS, SHOW SETTING) |
| `settings_changes` | b | · | · | static / server-global |
| `table_engines` | b | · | · | SHOW ENGINES |
| `table_functions` | b | · | · | static / server-global |
| `time_zones` | b | · | · | static / server-global |
| `tokenizers` | b | · | — | static / server-global |
| `unicode` | b | · | — | static / server-global |
| `warnings` | b | · | · | server warnings; read by clickhouse-client on connect |
| `zeros` | b | · | · | static / server-global |
| `zeros_mt` | b | · | · | static / server-global |
| `filesystem_cache_log` | a | absent | absent | cache reads with query_id and paths (absent) |
| `filesystem_read_prefetches_log` | a | absent | absent | prefetch events with query_id (absent) |
| `session_log` | a | absent | absent | login/logout of every session of the shared user, client addresses (absent: not enabled in default image) |
| `transactions` | a | absent | absent | experimental transactions with query_id (absent) |
| `transactions_info_log` | a | absent | absent | transaction events (absent) |
| `zookeeper` | a | absent | absent | keeper tree incl. /clickhouse/tables/<db>/<table> replication logs (absent: no keeper) |
| `zookeeper_log` | a | absent | absent | keeper requests with paths (absent: not enabled) |
| `latency_log` | b* | absent | absent | server-wide latency histograms (absent) |

`information_schema.*` and `INFORMATION_SCHEMA.*` (`tables`, `columns`, `schemata`, `views`, `key_column_usage`, `referential_constraints`, `statistics`, `character_sets`, `collations`, `engines`) are views over `system.tables` / `columns` / `databases`. They are class (c): `information_schema.tables` returned 9 tenant-2 names, `columns` 21, `views` 2 and `key_column_usage` 5. The rewriter admits them the same way.

### Class (a) list (refuse)

Present in 26.2 and/or 25.8 (50): `aggregated_zookeeper_log`, `asynchronous_insert_log`, `asynchronous_inserts`, `asynchronous_loader`, `azure_queue`, `azure_queue_metadata_cache`, `background_schedule_pool`, `background_schedule_pool_log`, `backup_log`, `backups`, `blob_storage_log`, `crash_log`, `delta_lake_metadata_log`, `detached_tables`, `distributed_ddl_queue`, `distribution_queue`, `dns_cache`, `dropped_tables`, `dropped_tables_parts`, `error_log`, `errors`, `iceberg_metadata_log`, `kafka_consumers`, `merges`, `moves`, `mutations`, `named_collections`, `opentelemetry_span_log`, `part_log`, `part_moves_between_shards`, `processes`, `processors_profile_log`, `query_cache`, `query_condition_cache`, `query_log`, `query_metric_log`, `query_thread_log`, `query_views_log`, `replicas`, `replicated_fetches`, `replication_queue`, `s3queue`, `s3queue_log`, `s3queue_metadata_cache`, `schema_inference_cache`, `stack_trace`, `text_log`, `trace_log`, `user_defined_functions`, `view_refreshes`.

Config-dependent, absent from the default image (7): `session_log`, `zookeeper`, `zookeeper_log`, `transactions`, `transactions_info_log`, `filesystem_cache_log`, `filesystem_read_prefetches_log`.

Borderline entries, and why they are (a):

- `mutations` is (a) because its command text carries another tenant's WHERE literals. The Sentio driver needs its **own** tables' rows (Task 2), so it has to be served as a filtered rewrite, not as a passthrough.
- `dropped_tables*` / `detached_tables` are strictly names (c), but nobody consumes them. Refuse them rather than build a filter.
- `asynchronous_loader` puts names in free-text `job` strings, so it cannot be filtered structurally.
- `named_collections` / `user_defined_functions` are global objects, but tenants (or the operator) define them, and they carry hosts, credentials or commands.
- `dns_cache` / `schema_inference_cache` reveal the external endpoints other tenants contacted.

### SHOW → system table (measured, both versions)

| SHOW form | reads | notes |
|---|---|---|
| `SHOW DATABASES [LIKE]` | `databases` | |
| `SHOW [FULL] TABLES [FROM d] [LIKE]`, `SHOW TEMPORARY TABLES` | `tables` | |
| `SHOW DICTIONARIES [FROM d] [LIKE]` | `dictionaries` | without `FROM` it uses the session database, which housegate sets to `phys` → every tenant's dictionaries (measured: 2 tenant-2 dictionaries) |
| `SHOW [FULL] COLUMNS FROM t [FROM d]` | `columns` | |
| `SHOW INDEX/INDEXES/KEYS FROM t` | `tables` + `data_skipping_indices` | |
| `SHOW CREATE TABLE/VIEW/DICTIONARY/DATABASE` | catalog directly | same content as `tables.create_table_query`; passwords print as `[HIDDEN]` |
| `SHOW PROCESSLIST` | `processes` | the live tenant-2 query was visible |
| `SHOW MERGES` | `merges` | the live tenant-2 merge was visible |
| `SHOW USERS` / `ROLES` / `CURRENT ROLES` / `ENABLED ROLES` / `PROFILES` / `ROW POLICIES` / `QUOTAS` | `users` / `roles` / `current_roles` / `enabled_roles` / `settings_profiles` / `row_policies` / `quotas` | |
| `SHOW QUOTA` | `quota_usage` | |
| `SHOW GRANTS`, `SHOW ACCESS`, `SHOW CREATE USER/ROLE/QUOTA/…` | access storage directly | equivalent to `grants` / `users` / … |
| `SHOW CLUSTERS`, `SHOW CLUSTER 'x'` | `clusters` | |
| `SHOW SETTINGS LIKE`, `SHOW CHANGED SETTINGS`, `SHOW SETTING x` | `settings` | |
| `SHOW FUNCTIONS [LIKE]` | `functions` | |
| `SHOW ENGINES` | `table_engines` | |
| `SHOW FILESYSTEM CACHES` | filesystem-cache registry directly | ≈ `filesystem_cache_settings` |
| `SHOW PRIVILEGES` | `privileges` | |

## Task 2: who reads system tables

| consumer | tables read | through housegate's rewriter as tenant traffic? | if refused |
|---|---|---|---|
| **Sentio driver, network_v1** (sentio `driver/cmd/clickhouse.go:39-68` builds `chx.New(conn, WithDatabase(SENTIO_NETWORK_HOUSEGATE_DB), WithTableNamePrefix("<proc>_<replica>."), WithLogicDatabase("<proc>_<replica>"))`; the queries are in sentio-core `common/chx`) | `tables` (`operator.go:38,88`: `WHERE database = ? AND name LIKE ?`, including `create_table_query`, `as_select`, `engine_full`); `columns` (`:185`); `data_skipping_indices` (`:229`); `projections` (`:259`, `query.go:276`); `parts` (`query.go:264` patch-part backlog before a lightweight delete; `query.go:375` `ListPartitions` → `driver/entity/clickhouse/entity_list.go:634` cache sizing and `chain/clickhouse/range_store.go:93`); `mutations` (`query.go:318` `waitMutations` after an async heavyweight DELETE or `APPLY PATCHES`); `clusters` (`clickhousemanager/conn.go:86` via `helper.AutoGetCluster`, on every `chx.New`) | **Yes.** The `database = ?` argument is the **physical** database name (`SENTIO_NETWORK_HOUSEGATE_DB`), and names carry the `<logical>.` prefix. The driver depends on system passthrough exposing the physical layout. | Schema load (`load`/`loadSimple`, from `InitEntitySchema` / timeseries `Init`) errors, so the processor does not start. The `parts` probe error fails `Delete`. **A `mutations` refusal hangs `Delete`:** `waitMutations` treats a query error as transient and keeps polling every 5s until ctx expires. `clusters` failure is non-fatal: it logs an error and uses no `ON CLUSTER`. If (c) is rewritten to logical names, `database = 'phys' AND name LIKE 'proc_0.%'` matches nothing, so the driver sees no tables and tries to re-create them. The driver must switch in lockstep. |
| Sentio platform services (sentio: `common/clickhouse/utils.go:404` `query_log`, `writer.go:337,866` `mutations`/`parts`, `service/exports/service/queue.go:285` `processes`, `service/mvcontroller` `view_refreshes`/`clusters`, `service/graphql/database/table_schema.go` `tables`/`columns`, `txindex` `clusters`) | as listed | **No.** In sentio, housegate appears only in `driver/cmd/clickhouse.go`; everything else uses direct ClickHouse connections. | Unaffected. |
| sentio-node | none in its own non-test code (tests read `system.tables` and `system.zookeeper` over direct connections). It embeds housegate, so housegate's internal reads below run inside it. | — | Unaffected. |
| housegate metrics collector (`pkg/metrics/chpoller.go:85-99`) | `metrics`, `events`, `asynchronous_metrics`, `mutations` (count) | No. It polls replicas directly with `ckh_manager_config_path` credentials. | Unaffected by a rewriter refusal. **A ClickHouse-side REVOKE on the shared user would break it.** |
| housegate SI merge guard (`pkg/storageintegrity/merge_guard.go:94,102`, `merge_guard_tables.go:76,113`) | `tables`, `merges` | No (direct SI runtime connection). | Unaffected by a rewriter refusal. |
| housegate SI parts-pressure guard (`pkg/storageintegrity/parts_pressure.go:263,302`) | `parts` ⋈ `tables` | No (direct). | Unaffected by a rewriter refusal. |
| housegate schema registry (`pkg/schemaregistry/loader.go:95,105`) | `tables`, `columns` | No (direct). | Unaffected by a rewriter refusal. |
| housegate relay legacy completion probe (`pkg/proxy/relay.go:602`) | `processes WHERE query_id = unhex(…)` | No. It writes straight to an upstream codec with the session's **own ClickHouse login** and never enters the plugin chain. | Unaffected by a rewriter refusal. A ClickHouse-side REVOKE on `processes` would make the non-chunked fallback close the session fail-closed. |
| housegate rewriter **output** | `SHOW TABLES [FROM]` → `SELECT … FROM system.tables WHERE database='phys' AND startsWith(name,'db1.')`; SI `DESCRIBE` → `SELECT … FROM system.columns WHERE database='hg_safe' …` (also pinned as `rewriter.StorageIntegrityProbeExpectedSQL`) | Emitted by the engine after classification. | The refusal must key on **caller input** (`original_accessed_tables` / positions), never on emitted SQL. Otherwise the existing SHOW TABLES synthesis and SI DESCRIBE break. |
| housegate `PermissionCommitGateObserver.checkAccess` (`pkg/network/permission_commitgate_observer.go:277`) | exempts every `system` read | — | Defense-in-depth hook for the same list. Its comment ("Per-row visibility is enforced server-side via viewIfPermitted") is wrong for a single ClickHouse user. |
| **clickhouse-client**, interactive (measured) | On connect: `SELECT * FROM viewIfPermitted(SELECT message FROM system.warnings ELSE null(...))`. Autocomplete: 26.x client → `system.completions` (contexts function, table engine, format, …, database, table, column, dictionary); 25.x client → `viewIfPermitted` UNION over `functions`, `table_engines`, `formats`, `table_functions`, `data_type_families`, `merge_tree_settings`, `settings`, `keywords`, `clusters`, `macros`, `storage_policies`, `aggregate_function_combinators`, `databases`, `tables`, `dictionaries`, `columns`. Non-interactive `-q` sends neither. | Yes. **Today both leak with SI off.** The 26.x query passes the rewriter unchanged. The 25.x and warnings queries hit a polyglot `SyntaxError` (it cannot parse `viewIfPermitted … ELSE`), and the plugin fails open and forwards them verbatim (`pkg/plugins/rewrite/rewriter.go:196-197`). With SI on, the `viewIfPermitted` queries are refused. | Non-fatal. The client ignores a failed warnings load and logs "Cannot load data for command line suggestions", which only loses autocomplete. A housegate refusal is not an `ACCESS_DENIED` that `viewIfPermitted` could absorb, so one refused arm (for example `dictionaries`) kills the whole 25.x suggestion UNION. Autocomplete survives only if `completions` is served as a filtered rewrite and the 25.x UNION's arms are all allowed or rewritten. |
| **clickhouse-go** (`v2.43.0-sentioxyz`, library code) | none (only examples/tests) | — | Nothing breaks. |
| **Grafana ClickHouse datasource** (native via clickhouse-go; from knowledge of the plugin source, not measured) | query-builder schema browsing: `system.databases`, `system.tables WHERE database=…`, `system.columns WHERE database=… AND table=…`. Health check is `SELECT 1`/`version()`. | Yes | Health check still works. The builder's database/table/column pickers go empty (refuse) or show physical names (passthrough); a logical-name rewrite fixes them. Hand-written SQL panels are unaffected. |
| **DBeaver, Metabase, clickhouse-jdbc, clickhouse-connect** (from knowledge) | JDBC `DatabaseMetaData` → `databases`/`tables`/`columns`/`functions`/`data_type_families`. DBeaver also reads `parts` for table statistics and `processes` for its session manager. clickhouse-connect reads `settings` on connect. | **N/A.** These are HTTP-protocol clients and housegate only speaks native TCP. | Nothing breaks via housegate. With a native-protocol DBeaver driver, the session manager (`processes`) would stop working, which is intended. |

## Task 3: current rewriter behaviour (rewriter-go `4b20d20`, FFI v0.14.0)

**`SELECT * FROM system.<t>`**: for **all 139** table names, in both SI modes, the result is `Success` with the SQL unchanged. `original_accessed_tables` reports `system.<t>` with `logical_database: system`. `information_schema.*` / `INFORMATION_SCHEMA.*`, `` `system`.`processes` ``, `SYSTEM.PROCESSES`, `system.tables WHERE database='phys'` and `system.columns WHERE database=currentDatabase()` behave the same way.

**`system.<t>` in other positions** (both modes, `Success`, system reference unchanged, reported): JOIN, comma join, `IN (SELECT … FROM system.query_log)`, scalar subquery, derived table, CTE, `UNION ALL`, `ARRAY JOIN (subquery)`, `view(SELECT … FROM system.query_log)`, and `INSERT INTO db1.t SELECT query FROM system.query_log`. `CREATE VIEW db1.v AS SELECT query FROM system.query_log` is also `Success`, so a tenant can persist another tenant's activity into its own table or a view that keeps it live. The SI-on runs rewrite `db1.t` into the safe derived read and leave `system.*` untouched.

**Table functions over `system`**:

| input | SI off | SI on |
|---|---|---|
| `merge('system','^processes$')`, `merge(system,…)`, `remote('127.0.0.1', system.processes)`, `remote(…,'system','processes')`, `cluster(…)`, `clusterAllReplicas(…)`, `loop(system, one)`, `remoteSecure`, `mysql` | `UnsupportedStatement` "table function X is not accepted" → **housegate forwards the original SQL** (`pkg/rewriter/sentio.go:381-398`). Measured on 26.2: `merge('system','^processes$')` and `remote('127.0.0.1', system.processes)` both return rows. | refused |
| `merge(REGEXP('sys.*'),'proc')` | `UnsupportedStatement` → forwarded | `RewriteError` (SI namespace not statically resolvable) |
| `viewIfPermitted(SELECT … FROM system.x ELSE null(…))` | `SyntaxError` (polyglot cannot parse `ELSE`) → **plugin fail-open forwards the original** (measured on 26.2: returns tenant-2 table names) | refused |
| `dictGet('system.x', …)` | `InvalidRewriteRequest` | same |
| `CREATE TABLE db1.c AS system.query_log`, `USE system`, `SHOW TABLES FROM system` | `InvalidRewriteRequest` | same |

**SHOW forms** (logical context `db1`; with SI on, `db1` holds an Active table):

| input | SI off | SI on |
|---|---|---|
| `SHOW PROCESSLIST`, `SHOW MERGES` | `Success`, verbatim | `Success`, verbatim |
| `SHOW DICTIONARIES` / `… LIKE '%'` | `Success`, verbatim → runs in `phys` → **every tenant's dictionaries** (measured) | refused ("storage-integrity logical database db1 is not directly addressable") |
| `SHOW DICTIONARIES FROM db2` / `FROM nosuchdb` / `FROM system` (unmapped) | `Success`, verbatim | `Success`, verbatim |
| `SHOW DICTIONARIES FROM db1` (mapped) | `Success`, verbatim, **not** mapped to `phys` (ClickHouse errors: no database `db1`) | refused (as above) |
| `SHOW DICTIONARIES FROM phys` | `InvalidRewriteRequest` (protected database) | same |
| `SHOW DATABASES [LIKE]` | synthesized `SELECT name FROM (SELECT 'db1' AS name)` | same |
| `SHOW TABLES`, `SHOW TABLES FROM db1` | synthesized from `system.tables WHERE database='phys' AND startsWith(name,'db1.')` | same |
| `SHOW TABLES FROM db2` / `system` / `nosuchdb` / `phys` | `InvalidRewriteRequest` | same |
| **`SHOW TABLES LIKE '%'`, `SHOW FULL TABLES`** (no `FROM`), `SHOW TEMPORARY TABLES` | `UnsupportedStatement` → **forwarded** → runs in `phys` → **every tenant's tables** (measured: 9 tenant-2 names) | refused |
| `SHOW COLUMNS FROM t` / `` FROM `db2.orders` `` | rewritten to ``phys.`db1.t` `` / ``phys.`db1.db2.orders` `` | refused (db1 is SI) |
| `SHOW COLUMNS FROM t FROM db1` | `Success`, verbatim (not mapped) | refused |
| `SHOW COLUMNS FROM system.processes` | `Success`, verbatim | `Success`, verbatim |
| `SHOW INDEX FROM t` | as `SHOW COLUMNS` | refused |
| `SHOW CREATE TABLE db1.t` | rewritten to ``phys.`db1.t` `` | refused (SI table) |
| `SHOW CREATE TABLE system.x`, `DESCRIBE system.x`, `EXISTS system.x`, `SHOW CREATE USER`, `SHOW CREATE DICTIONARY` | `UnsupportedStatement` → forwarded | refused |
| `SHOW USERS/ROLES/GRANTS/PROFILES/ROW POLICIES/QUOTAS/QUOTA/ACCESS/CLUSTERS/CLUSTER/SETTINGS LIKE/FUNCTIONS/ENGINES/FILESYSTEM CACHES` | `Success`, verbatim | `Success`, verbatim |
| `SHOW CURRENT ROLES`, `SHOW ENABLED ROLES`, `SHOW CHANGED SETTINGS`, `SHOW SETTING x`, `SHOW PRIVILEGES` | `Success`, verbatim | refused (SI catch-all: "statement class is not modelled") |
| `KILL QUERY WHERE …`, `KILL MUTATION WHERE …` | `UnsupportedStatement` → **forwarded** (one shared ClickHouse user, so `KILL QUERY WHERE 1` would kill every tenant's queries) | refused |

Out of scope but found on the way: with SI off, `KILL QUERY` / `KILL MUTATION` pass through. Any refuse list therefore depends on the fail-closed rewrite of the table-reference hardening plan landing for SI-off deployments too. Until then, every `UnsupportedStatement` and `SyntaxError` is a bypass.

rewriter-grpc was not run; the premise says it admits `system.*` the same way.

## Task 4: recommendation

1. **Refuse class (a) and class (b\*) tables through an allowlist, not a denylist.** The refusal keys on caller-input table references in every position (FROM/JOIN/IN/subquery/CTE/`view()`/`viewIfPermitted`/INSERT…SELECT/CREATE VIEW bodies/table-function arguments) and on the SHOW forms that read them:
   - SHOW forms: `PROCESSLIST`, `MERGES`, `DICTIONARIES` in every form, `USERS`, `ROLES`, `CURRENT ROLES`, `ENABLED ROLES`, `GRANTS`, `ACCESS`, `PROFILES`, `ROW POLICIES`, `QUOTA(S)`, `CREATE USER/ROLE/QUOTA/…`, `FILESYSTEM CACHES`.
   - The allowlist is class (b) as listed above plus `clusters`. The Sentio driver reads `clusters` through housegate; failure there is non-fatal but logs an error on every controller.
   - Everything else under `system`, including tables a future ClickHouse release adds, is refused.
   - For the user-decided three: `processes`, `merges` and `dictionaries` are refused. `dictionaries` loses nothing that matters: the driver does not read it, and clickhouse-client autocomplete survives if `completions` is rewritten. Note that the 25.x client's UNION includes `dictionaries`, so its autocomplete dies either way.
2. **Class (c): rewrite, do not refuse or pass through.** Serve `tables`, `columns`, `data_skipping_indices`, `projections`, `parts`, `parts_columns`, `projection_parts[_columns]`, `databases`, `completions` and `information_schema.{tables,columns,schemata,views,key_column_usage}` as derived reads, the same way `SHOW TABLES` / `SHOW DATABASES` are already synthesized:
   - Filter to `database = <physical> AND startsWith(name|table, '<logical>.')` for each logical database in the caller's `database_map`.
   - Project `database` back to the logical name and strip the prefix from `name`/`table`. Rewrite the embedded text columns (`create_table_query`, `engine_full`, `as_select`) the same way.
   - For `completions`, keep the global contexts and rebuild the database/table/column/dictionary contexts from the filtered rows.
   - Treat **`mutations` the same way**: it is (a) as a whole table, but filtered to the caller's own tables it is safe, and the driver needs it.
   - Refuse the remaining (c) entries: `detached_parts`, `rocksdb`, `iceberg_history`, `database_replicas`, plus `dropped_tables*` and `detached_tables`.
   - Route `SHOW COLUMNS/INDEX … FROM t FROM db1` and `SHOW DICTIONARIES` / `SHOW TABLES LIKE` / `SHOW FULL TABLES` without `FROM` through the same mapping. Today they are forwarded verbatim and either error or list every tenant's objects.
3. **Consumers to change:**
   - **(i) Sentio driver / sentio-core `common/chx`.** Switch the metadata reads from `database = <SENTIO_NETWORK_HOUSEGATE_DB> AND name LIKE '<proc>_<replica>.%'` to logical names (`database = '<proc>_<replica>'`, bare table names). This is the view the (c) rewrite returns, so both changes must ship together, or the rewrite must accept both shapes for one release. Also make `waitMutations` fail on a refusal (a non-transient error code) instead of polling until ctx expires.
   - **(ii) housegate.** Enforce on input references only, so its own emitted `system.tables`/`system.columns` SQL keeps working. Tighten `PermissionCommitGateObserver`'s blanket `system` read exemption to the same allowlist, and correct its comment. Do **not** implement this as a ClickHouse REVOKE on the shared user: the relay completion probe uses the session's login, and the metrics collector and SI guards probably use the same credentials.
   - **(iii) Docs.** The table-reference hardening design's non-goal "Reads of `system.*` … ClickHouse enforces per-row visibility" needs to become a goal, or a linked follow-up with this list.
   - clickhouse-client, clickhouse-go and Grafana need no change: autocomplete and schema pickers keep working on the rewritten (c) set.
4. **Precondition:** the fail-closed rewrite (table-reference hardening) must also cover SI-off deployments. Otherwise `viewIfPermitted` (a `SyntaxError`), `merge('system',…)` / `remote(…)` / `KILL QUERY` / `SHOW TABLES LIKE` (`UnsupportedStatement`) keep bypassing any list.
