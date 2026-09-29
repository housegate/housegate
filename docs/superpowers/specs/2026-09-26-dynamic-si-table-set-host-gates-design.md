# Dynamic SI Table Set — Sub-project 5a: Host Gates Before Registry Enablement

**Status:** approved design (2026-09-26). **Scope:** sentio-node only. **Umbrella:** `2026-09-23-dynamic-si-table-set-design.md`. **Predecessor:** sub-project 4 (`2026-09-25-dynamic-si-table-set-data-plane-design.md`, plans `2026-09-25-si-data-plane-reconciler.md` and `2026-09-25-si-sentio-node-table-state.md`), whose final review left two gates that must clear before governance enables the table registry on devnet2: G1 (processor and driver tables) and G2 (dropping a genesis table). Sub-project 5 is split in two: this document (5a, code) and a later rollout spec (5b: shared chart, images, the arbiter upgrade from v0.7.1, consensus parameters, `activation_block`, runbook).

## 1. Goal

Clear G1 and G2 in sentio-node so that enabling the registry changes nothing for processor indexing and a genesis table can be dropped, purged and recreated without breaking the next node restart.

Success means:

- Reads and writes of tables in processor databases behave the same before and after the registry is enabled.
- After a genesis table goes Retiring → Purging → Purged and its name is recreated (with the same or a different schema), a node restart succeeds, `schema_root` is unchanged, and the purged incarnation's `hg_*` tables are not recreated.
- With the registry disabled, both changes are behaviourally invisible: the build can ship in the first rollout sync.

## 2. Measured facts (sentio-node `origin/main` 9788949, arbiter `10ab917`, arbiter-core `v0.10.0`, sentio-core `063059e`)

- `database_registry.Server.EnsureTable` registers processor tables in the processor database `<processorId>_<replica>` (`database_registry/server.go:52-109`) and never declares a schema. The processor database lives on the indexer the processor was allocated to (`Databases._createProcessorDatabase`, `DatabaseType.PROCESSOR`), so an SI indexer hosts processor databases whenever processors are allocated to it.
- The arbiter's 2c watcher pairs `AddTable` only with the first `TableSchemaSet` after a `TableCreated` (arbiter `README.md:318`), so an undeclared processor table never gets an incarnation. The seed records every table that exists at activation as Legacy.
- sentio-node's table state governs a database when its chain `IndexerID` equals the registry's `SIIndexerID` (`storageintegrityadapter/tablestate/snapshot.go:64-68`) and answers an unrecorded table created at or after activation Pending by default deny (`snapshot.go:145-150`). `ChainDatabase` carries only `IndexerID` and `Tables` (`model.go:95-98`); the chain source drops the type (`chain.go:33`). HouseGate's `sitablestate` refuses Pending reads and writes with the retryable 733; driver sessions are not exempt. Result today: a processor table created after activation stalls its processor.
- sentio-core's mirrored `DatabaseInfo.DbType` is always serialized (`json:"dbType"`, no `omitempty`; `network/state/types.go:93`), `DatabaseTypeUser = 0`, `DatabaseTypeProcessor = 1`.
- Startup constructs the registry follower (`standalone/standalone.go:254`) but runs it only later, inside the startup transaction (`standalone/storage_integrity_table_state.go` `begin`). Before that, `loadStorageIntegritySchemaSets` loads every `snode.table_ids` schema (`standalone.go:262-285`, `storage_integrity_schemas.go:21-50`): from the contract's latest schema version at the finalized header under `schema_source: network_state` (`storage_integrity_contract_snapshot.go:73-140`), or from `hg_unsafe.<db>__<t>` in ClickHouse under `clickhouse`. `snode.New` requires every genesis table and `payloadexec.SchemaRoot(NetworkID, Tables) == SchemaRoot` (arbiter-core `snode/config.go:77-79,132`). Create mode runs `ddl.EnsureProtocolTables` over all genesis tables (`standalone.go:336-350,652-665,940-944`), and `valid()` requires at least one table.
- After a genesis table is purged: under `clickhouse`, its schema can no longer be derived and startup fails; under `network_state`, the preflight recreates its `hg_*` tables on every start (the registry-driven reconciler then drops them again as leftovers), and if the name was recreated the latest contract version is the new schema, so `SchemaRoot` no longer matches and `snode.New` fails. Removing the id from `table_ids` changes `schema_root`, which is immutable genesis.
- arbiter seeds genesis incarnations with `schema_hash` and no `schema_json`; arbiter-core's shared rule (`dataplane/registry_schema.go`) serves a live genesis-origin incarnation the configured schema.
- devnet2's SI indexer uses `schema_source: network_state`; its genesis set is `devnet101.swap_new2`. devnet2 verifiers configure their tables inline (full schema) with `-ensure-tables=create`.

## 3. Decisions

- **H1 — Processor databases are ungoverned (G1).** A table in a PROCESSOR database is never default-denied: if the registry has not recorded it, it is Ordinary. Chosen over declaring processor tables into SI (driver signing, a sub-project of its own) and over keeping processors off the SI indexer (operational constraint plus data migration).
- **H2 — Registry records still win.** A processor table that the registry does record (someone declared a schema for it, so the arbiter admitted it) is answered from the registry like any other recorded key. Declaring a schema brings a table into SI; the driver's unsigned writes to it are then refused. This is known behaviour, documented, not intercepted.
- **H3 — Retired genesis ids keep their schema, lose their tables (G2).** The genesis schema set feeding `schema_root` never changes. A genesis id whose genesis incarnation is no longer live takes its immutable schema from the contract by hash and is excluded from the create-mode preflight.
- **H4 — One-shot registry read at startup.** Before loading genesis schemas, startup reads the registry once through the existing arbiter client, instead of moving the follower's start out of the startup transaction. The startup transaction and the `standalone.go` source-shape audits stay as they are.
- **H5 — `clickhouse` schema source refuses retired genesis ids.** Under `clickhouse` there is no durable source for a dropped table's schema; startup refuses with an error naming the id and telling the operator to switch to `network_state`.

## 4. G1: processor databases are ungoverned

1. `tablestate.ChainDatabase` gains the database type (`Processor bool`, or a `Type` mirroring sentio-core's `DatabaseType`); `ChainSource` conversion (`chain.go`) fills it from `DatabaseInfo.DbType`.
2. The governed set (`snapshot.go`) includes a database only when it is on the SI indexer **and** is not a PROCESSOR database. Because every registry-recorded key (live Pending, Refused, Active, Retiring or Purging) is already answered from the registry regardless of the chain state, this changes only unrecorded tables in processor databases: they fall back to Ordinary instead of the governed-database fallback Pending. User databases keep the activation window and default deny unchanged.
3. Declaration compensation already skips non-user databases; no change.
4. Tests: `TestLookupMatrix` rows — an unrecorded table created after activation in a processor database on the SI indexer answers Ordinary; a registry-recorded key in a processor database answers from the registry (Pending, and Active when Ready); a user database's unrecorded post-activation table still answers Pending; the chain conversion carries the type. Acceptance (docker, the existing dynamic lifecycle harness): with the registry enabled, a processor-database table created after activation accepts an ordinary INSERT and SELECT.

## 5. G2: restarting after a genesis table was dropped

### 5.1 One-shot registry read

Before `loadStorageIntegritySchemaSets`, startup calls `GetTableRegistry` once through the arbiter client (the same `dataplane.Client` the follower uses, through `WithLeaderRetry`).

- `FAILED_PRECONDITION` with the message `dataplane.TableRegistryDisabledMessage`, or `Unimplemented`, means the registry is disabled: every genesis id is live, today's behaviour.
- Otherwise the answer is decoded with `wire.TableRegistrySnapshotFromPB`. A transport failure after the client's retry budget, or a decode failure, refuses startup (fail loud, matching the follower's `WaitReady` semantics).
- The call is bounded by `dataplane.DefaultRegistryStartupTimeout`.

### 5.2 Splitting the genesis ids

For each id in `snode.table_ids`, with the one-shot snapshot `snap`. The id's **genesis incarnation** is the key's incarnation with genesis origin; there is exactly one per genesis id, and its absence while the registry is enabled refuses startup.

| Registry state for the id | Schema source | Create-mode preflight |
|---|---|---|
| Registry disabled | as today | yes |
| `snap.Live(id)` is the genesis incarnation, status Legacy or Active | as today | yes |
| `snap.Live(id)` is the genesis incarnation, status Retiring or Purging | as today | **no** — the reconciler is about to drop these tables; the preflight must not recreate them |
| The genesis incarnation is Purged, or `snap.Live(id)` is a chain-origin incarnation (the name was recreated) | **by hash (5.3)** | **no** |

"Retired genesis" below means the last row.

### 5.3 Schema of a retired genesis id

Under `schema_source: network_state`, the schema is found by hash: read the table's schema versions from the `Databases` contract at the finalized header, from 1 to `LatestTableSchemaVersion`, and select the version whose `payloadexec.TableSchemaHash(network_id, schema)` equals the genesis incarnation's `SchemaHash`. No match refuses startup naming the id and the hash. The search reads at most the table's version count (small; the loader already reads the latest version).

Under `schema_source: clickhouse`, any retired genesis id refuses startup (H5).

Implementation first verifies that arbiter keeps a Purged genesis incarnation's `schema_hash` (no compaction clears it). If it does not, the by-hash search is replaced by a `schema_root` search: choose, for each retired id, the contract version such that `payloadexec.SchemaRoot(network_id, all genesis schemas) == snode.schema_root`; this is exact because `schema_root` commits to every genesis schema, and it refuses startup when no combination matches.

### 5.4 What each set feeds

- `snode.Config.Tables`: every genesis id, with its genesis schema (live ones as loaded today, retired ones from 5.3), so `SchemaRoot` still matches.
- The create-mode `EnsureProtocolTables` preflight and the network-state cross-check: only the ids 5.2 marks for the preflight. When none remain, the preflight is skipped (`valid()`'s non-empty requirement moves to the full set).
- `storageIntegrityTableStateDeps.genesis` stays the full set; the table state serves it only while the registry is disabled.

### 5.5 Tests

- Unit: every row of the 5.2 table, plus a key with no genesis incarnation (refusal).
- Unit: the by-hash version search — match on version 1 of 3, no match, a later version with a different schema, and the `clickhouse` refusal.
- Source shape: the one-shot read precedes `loadStorageIntegritySchemaSets` in `standalone.Run`, through a method in another file so the existing audits stay unchanged.
- Acceptance (docker): a genesis table goes Retiring → Purging → Purged and is recreated with a different schema; a restart of the embedded node succeeds, `schema_root` is unchanged, and the purged incarnation's `hg_*` tables do not reappear.

## 6. Delivery, compatibility and risks

- One sentio-node branch and PR, squash-merged on green CI; the merge's CI image is what 5b pins. No change to arbiter, arbiter-core, housegate or sentio-core.
- With the registry disabled both changes are inert, so the build can go to devnet2 in 5b's first sync of indexer-a without behaviour change.
- The one-shot view may be older than the follower's first answer. It only decides the preflight set and where a retired id's schema comes from; table creation and drops are the reconciler's, driven by the follower's latest view, so a stale view at worst verifies one table that exists anyway.
- Declaring a processor table's schema brings it into SI (H2).
- A `clickhouse`-source deployment must move to `network_state` before dropping any genesis table (H5); README documents it.
- arbiter-verifier: devnet2's verifiers carry inline schemas and are unaffected; implementation confirms the reference `arbiter-verifier` does not run its own create-mode preflight over purged genesis ids, and records the result.

## 7. Out of scope (sub-project 5b)

Shared chart changes (`charts/sentio-node` table guards and `enabled`, `charts/storage-integrity` `table_registry:` block, sidecar status-RPC source), image pins (sentio-node, housegate `v0.15.0`, the in-pod rewriter `housegate-rewriter:0.15.0` — Gate R4), the arbiter v0.7.1 → main upgrade (measured as a rolling upgrade inside a maintenance window with per-voter backups; rollback ends at the first main-leader election), enabling consensus administration and the registry, choosing `activation_block` and `deploy_block`, auditor/watcher enablement, the security follow-up gate, and the runbook.
