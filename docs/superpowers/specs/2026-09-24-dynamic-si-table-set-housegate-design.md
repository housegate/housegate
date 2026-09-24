# Dynamic storage-integrity table set — sub-project 3: housegate and the rewriter V2 contract — Design

**Status:** design, approved section by section on 2026-09-24.

**Parent spec:** [2026-09-23-dynamic-si-table-set-design.md](2026-09-23-dynamic-si-table-set-design.md) (the umbrella). This document refines its §5, §8, §9 and §11 for housegate; §14 of the umbrella records the resulting amendments. Where the two disagree, this document wins for housegate and the rewriter engines.

**Repositories:** rewriter-proto, rewriter-go, rewriter-grpc, housegate. sentio-node is not changed here (see §13).

## 1. Goal

Housegate decides every query from the arbiter's committed table registry instead of the static `storage_integrity.tables` list, without a restart or a config edit when the set changes:

- **Pending:** a data read or write returns a retryable error.
- **Refused:** a data read or write returns a non-retryable error that names the refusal code and reason.
- **Active:** reads are rewritten to `hg_safe` / `hg_unsafe`, and writes take the signed statement lane.
- **Legacy and tables outside the SI indexer:** behave exactly as today.

A `DROP TABLE` of an SI table succeeds, so that it can reach the chain and start retirement. The agent sidecar signs an INSERT only for an Active table. Every other INSERT passes through unsigned instead of failing with "not declared in network state" (the devnet2 report that started this design).

## 2. Measured facts this design rests on

Measured on `origin/main` on 2026-09-24: housegate `db46c31`, rewriter-go `1b6f0d3`, rewriter-grpc `7567dc5`, rewriter-proto `d3844a5`, sentio-node `36b5c0a`.

- **Umbrella sub-project 1 has not been done.**
  - Both engines reject `DROP TABLE` of an SI table: rewriter-go `internal/handlers/writes.go` `decideWriteTarget`, rewriter-grpc `src/handlers/storage_integrity.cc`, and the shared cases `si_drop_rejected` / `si_multi_drop_rejected`.
  - rewriter-proto defines only `STORAGE_INTEGRITY_CONTRACT_V1`.
  - `DROP DATABASE` of a user database that contains SI tables is not rejected. The engine records the logical→physical rewrite and returns a diagnostic `SELECT`, and the shared physical database is left to an external garbage collector (rewriter-go `internal/handlers/dblevel.go`).
- **Housegate freezes the SI table set at build in about ten places.** Every one keys off `len(cfg.StorageIntegrity.Tables) > 0` or the slice itself:
  - the rewriter options (`build.go` `storageIntegrityRewriterOptions`);
  - `rewrite.Plugin.FailClosedOnError`, the contract-echo gate and `RejectUndecodableQuery`;
  - the startup capability check and behavioural probe;
  - the `sireserved` wiring;
  - the exception scrubber (`pkg/rewriter/storage_integrity.go` `NewStorageIntegrityScrubber`, built once);
  - the `internal_listen` and read-state warnings;
  - the runtime's schema map and bijection check (`storage_integrity_runtime.go`);
  - the MergeGuard table list.

  Only schema lookups through `registry.TableSchemas` read network state per query.
- **The rewriter arguments are table-count gated.** `buildStorageIntegrityArgs` returns nil when there are no tables. Session `SET` refusal is not SI-specific code: it is the engine catch-all firing only because the SI contract was sent. In `unsafe_latest` mode it fetches promoted parts for every configured table, not only the ones the query accesses.
- **MergeGuard health is one global latch** (`storage_integrity_merge_supervisor.go`). One missing table fails every admission (`ingress.go` `CheckMergeHealth`).
- **The server ingress resolves a table's schema through `NetworkStateLoader`, that is, the latest chain declaration.** The registry's schema is the first declaration after the `TableCreated` (umbrella D3), so the two can differ.
- **The agent's `sistatement` claims every payload-local INSERT** and fails with "table %s is not declared in network state" when the loader misses (`pkg/plugins/sistatement/plugin.go` `loadSchema`). `RpcNetworkState` does not implement `registry.TableSchemas`.
- **An `OnQuery` plugin error already ends only the current query and keeps the session** (`pkg/proxy/relay.go`). `ClientError.KeepSession` matters only at the strict input-complete boundary.
- **Nothing in housegate models per-table status.** sentio-node pins housegate v0.13.0 and arbiter-core v0.6.0, does not call `TableRegistry`, and exposes no table-schema or table-status JSON-RPC.

## 3. Scope

**In scope:**
- the rewriter V2 contract (the umbrella's sub-project 1);
- the housegate `TableState` port and its static implementation;
- the explicit `storage_integrity.enabled` switch;
- the `sitablestate` plugin;
- the dynamic rewriter arguments and scrubber;
- the dynamic runtime and ingress schema resolution;
- per-table MergeGuard health;
- the agent's status-driven signing and its JSON-RPC client contract.

**Out of scope:**
- the sentio-node `TableState` implementation, JSON-RPC server, MergeGuard adapter and dependency bumps, all of which move to sub-project 4 with the data plane;
- `ALTER` of SI tables;
- multiple SI indexers.

## 4. Decisions

- **H1 — A per-query immutable snapshot.** `TableState` hands out versioned, immutable snapshots. Each query takes exactly one and every stage reads it, so the rewriter, `sitablestate`, the ingress and the scrubber never disagree within a query. Rejected alternatives:
  - separate `Status` / `ActiveSet` / `Schema` calls per stage, which let a version change between stages split one query's view;
  - rebuilding the plugin chain on every change, which interrupts sessions.
- **H2 — The host owns every membership judgement.** Housegate acts only on the returned status. Which indexer a database belongs to, default deny (umbrella D10), the activation window, and incarnation selection all belong to the host.
- **H3 — The activation window follows chain facts.** Between registry enablement and the committed seed, a table whose latest `TableCreated` precedes `activation_block` is reported Ordinary, which is what the seed will record as Legacy. A table created later is Pending. Existing tables see no interruption. The alternatives were rejected:
  - Pending for everything means a 13–25 minute outage at activation;
  - Ordinary for everything lets a new table take ordinary writes that `hg_*` later shadows.
- **H4 — Same-name recreation (umbrella D9) is refused in housegate.** It is enforced in `sitablestate`, which runs before commitgate and therefore before `createTable` reaches the chain. The sentio-node Observer needs no D9 check.
- **H5 — Metadata statements are allowed on Pending and Refused tables.** `DESCRIBE`, `SHOW CREATE`, `EXISTS` and `SHOW TABLES` read the ordinary table's definition. Only data reads and writes are refused.
- **H6 — The rewriter contract is activated by version, not by table count.** A V2 request with an empty table map still activates the catch-all, so session `SET`, `SYSTEM` and unmodelled statements stay refused while the Active set is empty. A session `SET` persists, and a table that later becomes Active would otherwise execute signed statements under it.
- **H7 — The agent signs Active tables only and passes everything else through.** The server is the authority. An unsigned INSERT into an Active table is always rejected by the ingress, so a pass-through is safe even when the agent's view is stale or its status RPC fails.

## 5. The `TableState` port and snapshot model

A new package `pkg/sitable` holds only types and the static implementation, with no dependency on the proxy packages. Hosts inject an implementation through `Options.StorageIntegrityTableState`.

```go
type Status uint8

const (
    Ordinary Status = iota // not governed: another indexer, Legacy, or registry not enabled
    Pending                // includes default deny: on the SI indexer and not recorded
    Refused
    Active
    Gone                   // the newest incarnation is Retiring or Purging
)

type Table struct {
    ID            string // "<database>.<table>", logical
    Status        Status
    RefusedCode   string // Refused only; the arbiter's refused_code vocabulary
    RefusedReason string
    Schema        payloadexec.TableSchema // set for Active and Gone
    SchemaHash    string
}

// Snapshot is immutable; every method is in-memory and performs no I/O.
type Snapshot interface {
    Version() uint64
    Lookup(database, table string) Table // answers for any table
    Active() []Table                     // sorted by ID
    Schema(id string) (Table, bool)      // Active or Gone
}

type TableState interface {
    Current() Snapshot
    Changed() <-chan struct{} // broadcast on every version change
}
```

- **Legacy and ungoverned are one status.** Housegate treats a Legacy table and a table outside the SI indexer identically, so both are `Ordinary`. A host that needs to tell them apart does so in its own metrics.
- **A Purged name has no SI incarnation.** The host reports it as unrecorded, which means Pending on the SI indexer. A `CREATE` after purge therefore needs no special case in housegate.
- **One snapshot per query.** The rewrite plugin calls `Current()` once at the start of `OnQuery` and stores the result in `QueryContext`. Every later stage reads that value: the rewriter arguments, `sitablestate`, the ingress, parts-pressure resolution and the exception scrubber.
- **`sitable.Static`** is built from the configured `tables` and the schemas loaded at startup. Its rules:
  - every listed table is Active;
  - every other table is Ordinary;
  - the version is a constant 1;
  - `Changed()` never fires.

  It serves the standalone binary, tests, and any host that does not inject a `TableState`, with today's behaviour.

## 6. The explicit switch

### 6.1 Configuration

- `storage_integrity.enabled` (`*bool`) defaults to `len(tables) > 0`, so every existing config keeps its meaning.
- When it is enabled, exactly one table-set source is required: the static `tables`, or an injected `Options.StorageIntegrityTableState`. Both, or neither, is a validation error.
- A host that injects a `TableState` sets `enabled: true` explicitly, because the default derived from an empty `tables` list is false.
- Rules that required `tables` now require `enabled`: `runtime`, and `read.default_mode`.

### 6.2 Sites that change

| Site | Today | New |
|---|---|---|
| rewrite fail-closed handling: `FailClosedOnError`, contract echo, every non-Success rejected, transport error rejected, `RejectUndecodableQuery` | tables non-empty | `enabled` |
| `StorageIntegrityArgs` in each rewriter request | omitted when there are no tables | always sent when `enabled`, possibly with an empty table map (H6) |
| session `SET` refusal | engine catch-all under an active contract | same catch-all, now independent of table count through the V2 contract (§8) |
| startup capability check and behavioural probe | tables non-empty | `enabled`; probe moves to V2 (§8.4) |
| `sireserved` plugin | tables non-empty | `enabled` |
| exception scrubber | built once at startup | built per snapshot version and cached by version; bare `hg_safe`, `hg_unsafe` and `_hg_row_id` are always scrubbed |
| `internal_listen` bypass and read-state warnings | tables non-empty; the warning lists them | `enabled`; tables are no longer listed |
| rewriter factory `StorageIntegrityOptions.Tables` | static slice | the `TableState`, read through the query's snapshot |
| `unsafe_latest` promoted parts | fetched for every configured table | fetched only for the Active tables the query accesses |

While `enabled` is set, a rewriter outage fails every query, including queries on Ordinary tables. That is today's behaviour whenever SI tables are configured, and it stays deliberate.

## 7. The `sitablestate` plugin

### 7.1 Placement and bypass rules

- It is a server-mode `QueryPlugin`, registered only when `enabled`. It sits after `rewrite` and before the SI ingress and commitgate, because it needs the rewriter's `StatementType`, its `AccessedTables` (logical database and table) and the query's snapshot.
- Peer-trusted sessions and origin-side forwarding sessions bypass it (`PeerTrustAware` and `ForwardAware` opt-outs, as for rewrite). `IsForwardedFromPeer` makes it run on the receiving host.
- Maintenance and platform-operator sessions keep their existing bypass.

### 7.2 Decision matrix

Every accessed table is looked up with `Snapshot.Lookup`. If any table refuses, the statement is refused. When several refusals apply, the first one in this order wins: unknown table, then non-retryable, then retryable.

| Status | Data read | Data write | Metadata (`DESCRIBE`, `SHOW`, `EXISTS`) | `CREATE TABLE` of the same name | `DROP TABLE` | Other DDL (`ALTER`, `RENAME`, MV targeting it) |
|---|---|---|---|---|---|---|
| Ordinary | allow | allow | allow | allow | allow | allow |
| Pending | retryable | retryable | allow | allow (ClickHouse reports that it exists) | allow (drops the ordinary table; the chain `deleteTable` retires or cancels the incarnation) | non-retryable |
| Refused | non-retryable with code and reason | same | allow | allow | allow (so the user can recreate) | non-retryable |
| Active | rewritten by the engine | signed lane (ingress) | rewritten by the engine | allow | allowed by the V2 contract (§8) | already rejected by the engine |
| Gone | unknown table (ClickHouse code 60, `UNKNOWN_TABLE`) | unknown table | unknown table | **retryable (H4)** | unknown table | unknown table |

### 7.3 Rules beyond the matrix

1. **Gone is answered by the plugin, not by ClickHouse.** `DROP DATABASE` leaves the shared physical database to garbage collection, so a Retiring table may still exist physically and would otherwise be readable and writable.
2. **Data-carrying creation into a governed table is refused (non-retryable).** This covers `CREATE TABLE ... AS SELECT`, `CREATE MATERIALIZED VIEW ... POPULATE`, and `CREATE MATERIALIZED VIEW ... TO` whose target is governed. "Governed" means `Lookup` of the target returns Pending (including default deny) or Active. Such a statement would put rows into a Pending table's ordinary storage, which violates umbrella D10. The message tells the user to create the table first and then INSERT.
3. **`ALTER` and `RENAME` are refused on Pending and Refused tables.** The registry schema is the first declaration after creation, and a rename is invisible to the chain.

### 7.4 Error contract

- Errors are raised at `OnQuery`, so the relay ends the query and keeps the session. `KeepSession` is not needed.
- Clients match on these message prefixes, which are a stable contract:
  - `storage_integrity: table <id> is pending activation (retryable)`
  - `storage_integrity: table <id> was refused: <code>: <reason>`
  - `storage_integrity: table <id> is still being purged; retry CREATE later (retryable)`
  - `storage_integrity: table <id> no longer accepts writes` (§9.6)
  - `storage_integrity: table <id> requires a signed INSERT; the client's table state is stale (retryable)` (§10.3)
- The implementation plan fixes the ClickHouse codes for the retryable and non-retryable classes. Two constraints apply:
  - neither may make clickhouse-go or clickhouse-client drop the connection;
  - neither may reuse a code with existing Housegate meaning (252 back-pressure, 403 plugin rejection).

  Gone reuses code 60 so that clients see an ordinary unknown table.

## 8. Rewriter contract V2 (rewriter-proto, rewriter-go, rewriter-grpc)

### 8.1 rewriter-proto

- Add `STORAGE_INTEGRITY_CONTRACT_V2 = 2`.
- Update the enum comment, which today says every non-INSERT DDL on an SI table is rejected.

### 8.2 V2 differs from V1 in exactly two ways

1. **DROP of SI tables.**
   - `DROP TABLE [IF EXISTS] <logical SI table>` returns Success. `SYNC` and several targets in one statement are allowed, including a mix of SI and ordinary tables.
   - Each SI target is rewritten to drop only its ordinary physical table, the name any ordinary table gets through `database_map`, for example ``phys.`db1.t` ``. `hg_*` tables are untouched, because the data plane purges them.
   - The target stays in `AccessedTables` with `IsStorageIntegrity`, so commitgate and the host Observer still see a `DROP TABLE` and call the chain's `deleteTable`.
   - These stay rejected: a physical target such as `DROP TABLE hg_safe.x`; `TRUNCATE`, `DROP VIEW` and `DROP DICTIONARY` of an SI table; and `ON CLUSTER`.
2. **Version-based activation (H6).** A request carrying V2 activates the catch-all even when the table map is empty.

### 8.3 Unchanged

- Pending and Refused tables are not in the arguments, so their `DROP` already takes the ordinary path.
- `DROP DATABASE` keeps its current behaviour. §7.3 rule 1 covers the physical leftovers.

### 8.4 Compatibility, cases and probe

- **Engines accept V1 and V2, and housegate sends only V2.** V1 behaviour is byte-for-byte unchanged, so the independently deployed gRPC rewriter can be upgraded before housegate. The new housegate requires a V2 acknowledgement and never falls back to V1.
- **Shared cases file.** `storage_integrity_cases.json` gains a per-case `contract_version`, and every existing case is marked V1. New V2 cases cover:
  - DROP of one SI table;
  - DROP of several tables mixing SI and ordinary;
  - `IF EXISTS` and `SYNC`;
  - `TRUNCATE`, a physical target and `ON CLUSTER`, all still rejected;
  - `SET` and `SYSTEM` rejected under an empty table map.

  The rewriter-go and rewriter-grpc copies stay byte-identical.
- **The housegate startup probe moves to V2.**
  - It keeps its five cases.
  - It adds the exact rewritten SQL for `DROP TABLE db1.t`.
  - It adds `SYSTEM RELOAD CONFIG` rejected under an empty table map. That case distinguishes V1-only builds.
- **Version floors.** The probe's minimum builds, the `ffifetch` native library pin and the CLAUDE.md pin paragraph move to the new releases (rewriter-go v0.13.0 and rewriter-grpc v0.15.0 as planned; the tags actually cut are authoritative).
- **Release order.**
  1. rewriter-proto tag.
  2. rewriter-go and rewriter-grpc releases.
  3. Upgrade the production gRPC rewriter service.
  4. housegate.

## 9. Server runtime

1. **Ingress schema.** The ingress uses the query snapshot's `Schema(id)` and requires Active. It checks the v2 token's `schema_hash` against the snapshot's `SchemaHash` instead of the latest chain declaration. Under `sitable.Static` the snapshot carries the schemas loaded at startup, so the static path is unchanged.
2. **Runtime schema resolution follows `TableState`.**
   - New admissions use the query's snapshot.
   - Journal recovery and cleanup use `TableState.Current().Schema(id)`, which includes Gone tables, so statements admitted before a retirement can finish.
   - Recovery must not require a table to still be Active. Touched partitions are already journaled.
   - A record that needs a schema whose table is already Purged fails closed with a named error for the operator. The plan audits every recovery path that reads a schema.
3. **Bijection check.**
   - `sitable.Static` keeps `ValidatePhysicalTableNames` and the config-to-schema bijection check.
   - Dynamic hosts drop both, because physical-name injectivity is enforced by the arbiter's admission rules v1 (`physical_name_collision`).
4. **MergeGuard per table.**
   - The guarded set follows the snapshot:
     - Active tables' `hg_*` must exist and pin merges off.
     - Pending and Gone tables' `hg_*` are checked when present and are not an error when absent. They are created when `AddTable` commits and dropped during purge.
   - Health becomes per table: `CheckMergeHealth(tableID)`. Admission checks only the target table, so one unready table blocks only itself.
   - The supervisor also wakes on `Changed()`, so a newly Active table is asserted immediately instead of after `reassert_interval`.
   - The injected `Options.StorageIntegrityRuntime.MergeGuard` interface changes to the per-table signature.
5. **Parts pressure and read state.**
   - Pressure scans are already database-scoped.
   - `partsPressureTarget` resolves its schema through the snapshot.
   - `unsafe_latest` touches only accessed Active tables (§6.2).
6. **Retirement while a write is in flight.** A write admitted under a snapshot in which the table is Active may reach the arbiter after `RetireTables` committed. The arbiter answers `SCHEMA_NOT_ALLOWED`, and the intake terminates the statement through its existing rejection path. The client sees the non-retryable `no longer accepts writes` message (§7.4), not the arbiter's internal code.

## 10. Agent

### 10.1 sistatement

For each payload-local INSERT, and each inline `INSERT ... VALUES` when that lane is enabled, `sistatement` first obtains the target's status:

- **Active:** it signs with the registry schema and `schema_hash` through the v2 lane.
- **Anything else:** it passes the statement through unchanged and unsigned, and the server decides. This covers Ordinary, Pending, Refused and Gone. The inline lane evaluates and signs only for Active.

The original devnet2 failure becomes a sequence of well-defined steps:
1. A new table is Pending.
2. The agent passes the INSERT through.
3. The server answers `pending activation (retryable)`.
4. Once the table is Active, the agent signs and the INSERT succeeds. This takes about 13 minutes on devnet2 with `confirmation = safe`.

### 10.2 Status source

- **JSON-RPC contract.** Housegate specifies the method and sentio-node implements it in sub-project 4:

  ```
  sentio_getStorageIntegrityTableStatus(database, table) →
    {"status": "ordinary|pending|refused|active|gone",
     "refused_code": "", "refused_reason": "",
     "schema_json": "", "schema_hash": "", "registry_version": 0}
  ```

  The answer has the same semantics as `Snapshot.Lookup` on the serving node.
- **One call per INSERT, no cache.** The sidecar runs next to its indexer.
- **Hash check.** The agent recomputes `TableSchemaHash` with `agent.network_id` and refuses the INSERT, unsigned, if it differs from `schema_hash`.
- **Status providers.** `RpcNetworkState` implements the call. The YAML `table_schemas` source stays for local use and tests: a declared table is Active, and every other table is Ordinary.
- **Dropped methods.** The umbrella's `sentio_getTableSchema` and `sentio_getLatestTableSchema` are not needed and are dropped.

### 10.3 Failures and skew

- **Status RPC failure.** The agent passes the statement through and increments a metric. This is safe by H7. Failing closed would stop INSERTs into Ordinary tables whenever the RPC is down.
- **View skew between the agent's node and the serving node:**
  - Agent sees Active while the server sees Pending: the server's retryable error applies.
  - Agent sees Pending while the server sees Active: the ingress rejects the unsigned write. That rejection now uses the retryable `requires a signed INSERT; the client's table state is stale` message.

### 10.4 Unchanged

USE tracking, materialize, `client_seq` persistence and inline evaluation are unchanged. Only the first step of the claim decision changes.

## 11. Testing

### 11.1 rewriter-go and rewriter-grpc

- The V2 cases are added to the shared file.
- Every existing V1 case passes unmodified.
- The two copies stay byte-identical.

### 11.2 housegate unit tests

- **`sitable`:** the static snapshot, and a versioned fake whose versions can be switched during a test.
- **`sitablestate`:**
  - the full status × statement-class matrix;
  - precedence between refusals when several tables are accessed;
  - the data-carrying creation refusals;
  - the bypass rules (peer, forward, `IsForwardedFromPeer`, maintenance).
- **Switch:** the default derivation, and the validation errors for both sources and for neither.
- **Rewriter:**
  - arguments sent with an empty table map;
  - promoted parts fetched for accessed tables only;
  - the scrubber cached per version.
- **One snapshot per query:** the fake changes version between two plugin hooks, and every stage of the query must still observe one version.
- **MergeGuard:** per-table health, and immediate assertion after `Changed()`.
- **Runtime:**
  - the registry schema wins over a different latest chain declaration;
  - a Gone table still recovers;
  - a record needing a Purged table's schema fails with the named error.
- **Agent:**
  - Active signs and every other status passes through;
  - pass-through on RPC failure;
  - hash-mismatch refusal;
  - the stale-view message.

### 11.3 Integration tests

These are docker-bound, use real ClickHouse and the CLI, and are added to the explicit list in `ci.yml`. The fake `TableState` drives the whole lifecycle:

1. CREATE, then Pending: reads and writes get the retryable error.
2. The test creates `hg_*` in place of the data plane, and the table becomes Active: a signed INSERT succeeds.
3. DROP, then Gone: reads get `UNKNOWN_TABLE`, and a same-name CREATE gets the retryable error.
4. Purged: the same-name CREATE succeeds.

The existing static-config integration suite passes unchanged, which proves that `sitable.Static` preserves today's behaviour.

## 12. Rollout

1. rewriter-proto V2, then the rewriter-go and rewriter-grpc releases, then the production gRPC rewriter service. Older housegates keep using V1.
2. The housegate release. It requires V2 and defaults to `sitable.Static`, so existing deployments keep their behaviour.
3. Production behaviour is unchanged until a host injects a dynamic `TableState` and the registry is enabled. sentio-node's bump to this housegate belongs to sub-project 4 and carries the per-table MergeGuard adapter and the V2 native library, with the V2 gRPC rewriter service as a prerequisite.

## 13. Host contract for sub-project 4 (sentio-node)

- **Implement `sitable.TableState` following `WatchTableRegistry`.**
  - Govern only databases whose `indexerId` is the registry's `si_indexer_id`.
  - Before the seed commits, report a table whose latest `TableCreated` precedes `activation_block` as Ordinary (H3).
  - Report a table the registry does not record as Pending (default deny).
  - Select among incarnations with the arbiter's `TableRegistryView.Live` rule.
  - Report **Active only when the registry says Active and this node's `hg_*` tables for it are ensured.**
  - Report Gone when the newest incarnation is Retiring or Purging.
  - Until the registry is enabled, keep presenting the current static table set as Active, for example through `sitable.Static`.
- **Serve `sentio_getStorageIntegrityTableStatus`** with the same semantics.
- **Adapt the injected MergeGuard** to the per-table interface.
- **The Observer needs no D9 check (H4).** Declaration compensation remains a sub-project 4 item.

## 14. Risks

- **Stale host view.** A host that reports Active before its `hg_*` exist would let admissions fail at the ingress or the arbiter. §13 makes local readiness part of Active, and MergeGuard's per-table check is a second guard.
- **Client handling of retryable errors.** clickhouse-client does not retry. Users see a retryable error for up to the confirmation latency after `CREATE TABLE`. The message prefix is the contract that SDKs and tools can match on.
- **Rewriter outage blast radius.** With `enabled`, a rewriter outage fails every query on the node, including queries on Ordinary tables (§6.2). This is unchanged from today's configured-SI behaviour.
- **Mixed rewriter versions.** A housegate that requires V2, deployed against a V1-only gRPC service, refuses startup through the probe. This is loud and safe, and the release order in §8.4 avoids it.
