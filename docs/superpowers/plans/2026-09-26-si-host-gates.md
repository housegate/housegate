# Dynamic SI Table Set Host Gates in sentio-node Implementation Plan (sub-project 5a)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Clear gates G1 and G2 in sentio-node so governance can enable the arbiter's table registry on devnet2: tables in a processor's own database stay Ordinary (processor indexing behaves exactly as before the registry is enabled), and a genesis table can be retired, purged and recreated under the same name without breaking the next node restart (`schema_root` unchanged, the purged incarnation's `hg_*` tables not recreated). With the registry disabled both changes are behaviourally invisible.

**Architecture:** G1 is a one-field change in the table state: `tablestate.ChainDatabase` gains `Processor`, filled from sentio-core's `DatabaseInfo.DbType`, and the snapshot builder governs a database only when it is on the SI indexer and is not a PROCESSOR database; registry-recorded keys are still answered from the registry. G2 adds one new file, `standalone/storage_integrity_genesis.go`: before any genesis schema loads, `standalone.Run` reads the registry once through the arbiter client it already built (`readStorageIntegrityGenesisPlan`, the same `GetTableRegistry` + `WithLeaderRetry` call and disabled/`Unimplemented` classification as the follower's first read), classifies every `storage_integrity.snode.table_ids` entry against design 5a §5.2, and hands the plan to `loadStorageIntegritySchemaSets`. Live and retiring ids load as before; a retired id's immutable genesis schema comes from the `Databases` contract by the genesis incarnation's `schema_hash` (`network_state`) or refuses startup (`clickhouse`). `snode.Config.Tables` and the table state keep every genesis schema, so `schema_root` still matches; the create-mode preflight and the network-state cross-check get only the live Legacy/Active ids, and are skipped when none remain. The registry follower still starts inside the startup transaction; the source-shape audits in `standalone/storage_integrity_bootstrap_test.go` are not edited.

**Tech Stack:** Go 1.27 (module `compute-network-node`), Bazel 9.1.0 + Bzlmod + gazelle, housegate `v0.15.0` (`pkg/schemaregistry`, `pkg/replay/payloadexec`, `pkg/sitable`), arbiter-core `v0.10.1` (`dataplane.Client.WithLeaderRetry`, `dataplane.TableRegistryDisabledMessage`, `dataplane.DefaultRegistryStartupTimeout`, `wire.TableRegistrySnapshotFromPB`, `wire.TableRegistrySnapshot.Live`, `snode.New`), arbiter-proto `v0.8.0` (`pb.TableRegistryClient`, `pb.NotLeader`), sentio-core `063059e` (`statecore.DatabaseTypeProcessor`), ClickHouse 25.8.32.4 with Keeper (acceptance).

**Spec:** `docs/superpowers/specs/2026-09-26-dynamic-si-table-set-host-gates-design.md` (binding): §3 H1–H5, §4 (G1), §5 (G2), §6. §7 is sub-project 5b; see "Handoff to sub-project 5b". Background: sub-project 4 plan B (`docs/superpowers/plans/2026-09-25-si-sentio-node-table-state.md`, "Execution record and additional sub-project 5 gates").

## Global Constraints

- **Repository and base.** sentio-node `origin/main` at `46ea7de` or later (`46ea7de` is `9788949` plus the arbiter-core `v0.10.1` bump, #187). One branch, `feat/si-host-gates`; never push to `main`; merge through a PR.
- **No other repository changes.** arbiter, arbiter-core, housegate, sentio-core and every `go.mod` / `MODULE.bazel` pin stay as they are.
- **Registry disabled means invisible.** With the registry disabled (or an arbiter without the `TableRegistry` service), every genesis id is planned `{preflight: true}`, loads exactly as before, and no retired-schema lookup runs.
- **One-shot read.** `pb.NewTableRegistryClient(conn).GetTableRegistry` inside `(*dataplane.Client).WithLeaderRetry`, bounded by `dataplane.DefaultRegistryStartupTimeout` (2 minutes). Disabled means `codes.Unimplemented`, or `codes.FailedPrecondition` with message `dataplane.TableRegistryDisabledMessage` (`"table registry is disabled"`) and no `pb.NotLeader` detail. A decode failure (`wire.TableRegistrySnapshotFromPB`) refuses at once.
- **Genesis sets.** `snode.Config.Tables` and `storageIntegrityTableStateDeps.genesis` always receive every `storage_integrity.snode.table_ids` entry with its genesis schema, in configured order. The create-mode `ddl.EnsureProtocolTables` preflight and the network-state cross-check receive only ids whose live incarnation is the genesis incarnation with status Legacy or Active.
- **Processor database.** A database is a PROCESSOR database exactly when `statecore.DatabaseInfo.DbType == statecore.DatabaseTypeProcessor` (`1`); every other value, known or not, stays governed.
- **Source-shape audits.** `standalone/storage_integrity_bootstrap_test.go` and `standalone/storage_integrity_table_state_test.go` are not edited. `newStorageIntegrityBootDeps` keeps its nine arguments `(chConn, pinned, tables, protocolTablesMode, schemaSnapshot, schemaSets, si.SNode.NetworkID, siRole, roleLogger)`; `loadStorageIntegrityContractSnapshot` keeps `(ctx, env.EthClient, env.DatabasesContract, si.SNode.TableIDs)`; `si` gains no bare reference and `cfg.StorageIntegrity` no second read.
- **Tests.** Bazel is the ground truth (`bazel test //...`); `go test` is the fast loop. Docker-bound acceptance tests need `SENTIO_SI_CH_E2E=1` and `CH_ADDR` pointing at a Keeper-enabled ClickHouse, started as `.github/workflows/ci.yml`'s `integration-clickhouse` job does (`scripts/ci/clickhouse-keeper.xml`, image `clickhouse/clickhouse-server:25.8.32.4`). New acceptance tests keep the `TestStorageIntegrity` prefix and join the job's filter.
- **Conventions.** Run `bazel run //:gazelle` after adding Go files or imports and commit the `BUILD.bazel` it writes; conventional commit scopes; English code and comments; Markdown not hard-wrapped; every commit message ends with a blank line and `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

Local acceptance environment, used by Tasks 3, 9 and 11 (bash, from the repository root):

```bash
docker run -d --rm --name sp5a-ch --hostname sp5a-ch -p 127.0.0.1::9000 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/scripts/ci/clickhouse-keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
  clickhouse/clickhouse-server:25.8.32.4
export CH_ADDR="127.0.0.1:$(docker inspect --format '{{(index (index .NetworkSettings.Ports "9000/tcp") 0).HostPort}}' sp5a-ch)"
until docker exec sp5a-ch clickhouse-client --query "SELECT count() FROM system.zookeeper WHERE path = '/'" >/dev/null 2>&1; do sleep 1; done
export SENTIO_SI_CH_E2E=1
```

Remove it afterwards with `docker rm -f sp5a-ch`.

## Review Focus

1. **A follower's redirect is not a disabled registry.** `WithLeaderRetry` treats every FAILED_PRECONDITION as a leader miss; the follower intercepts only the leader's own "disabled" answer, recognised by the absence of a `NotLeader` detail. Reading a redirect that happens to carry the same text as "disabled" would make every genesis id live, so the preflight would recreate a purged genesis table and a recreated name would load the wrong schema. Test: Task 4 `TestReadStorageIntegrityTableRegistryDoesNotReadARedirectAsDisabled` (the redirect is retried until the timeout and startup refuses).
2. **A retired id takes the genesis incarnation's hash, never the live one's.** After a recreation, `Live(id)` is the new chain (or Legacy, or a newer Refused) incarnation; taking its hash would put the recreated schema into `schema_root`. A registry without exactly one genesis incarnation for a genesis id, or with a genesis incarnation in a status the arbiter never gives it, refuses startup. Tests: Task 5 `TestPlanStorageIntegrityGenesisRows` (rows `recreated`, `recreated_early`, `seeded_legacy`, `refused_after`) and `TestPlanStorageIntegrityGenesisRefusesAnInconsistentRegistry`.
3. **The by-hash search trusts only the recomputed hash.** Versions are write-once and survive recreation, so the table's history holds both schemas; the on-chain `schemaHash` field is whatever the declarer wrote, undecodable or foreign content can sit in the history, and the hash depends on the network id. Test: Task 6 `TestContractStorageIntegrityGenesisSchemasSearchesByHash` (match on version 1 of 3 with a different later version, skipped undecodable and foreign versions, another network id, a failed read, every read pinned to one finalized header).
4. **Only PROCESSOR is ungoverned, and registry records still win there.** An unknown future database type must stay governed (fail closed), and a processor table whose schema someone declared must be answered from the registry, Active only when this node's reconciler is ready (H2). Tests: Task 1 `TestDatabaseInfoChainProjectsIndexerTypeAndCreatedBlock` (database `future`, type 2); Task 2 `TestLookupMatrix` processor rows `declared_pending`, `declared_active`, `declared_unready` and the Active list.
5. **Nothing live to preflight, and `schema_root` keeps every genesis table.** When every genesis id is retiring or retired, the old non-empty checks in the preflight adapters would refuse startup, and a Run edit that fed the preflight subset to `snode.New` would change `schema_root`. Tests: Task 7 `TestLoadStorageIntegritySchemaSetsSplitsTheGenesisSet` (only a retired id: no loader call, empty preflight) and `TestRunReadsTheRegistryBeforeLoadingGenesisSchemas` (`snode.Config.Tables` and the table state's `genesis` bound to `schemaSets.snode`); Task 8 `TestPrepareStorageIntegrityListenerPermitSkipsAnEmptyPreflight`; Task 9 `TestStorageIntegrityGenesisRestartAfterPurge` steps 3–5.

## Plan decisions

- **S1 — Purged genesis incarnations keep `schema_hash` (measured), so the by-hash search of spec §5.3 is used, not the `schema_root` fallback.** Evidence (arbiter `10ab917`): `fsm/table_registry.go:245-257` `enableTableRegistryLocked` seeds each genesis incarnation with `SchemaHash: tm.SchemaHash` from the genesis manifest; `fsm/apply_table_registry.go:174-227` (`applyRetireTables`), `:229-250` (`applyRecordTablePurged`) and `:281-302` (`completePurgesLocked`) change only `Status`, `Deleted`, `RetireReason` and `PurgedBy`; no FSM path compacts or clears an incarnation (the registry keeps every Purged and Refused incarnation, which is why arbiter-core `v0.10.1` raised the data-plane receive limit, `dataplane/client.go:21-28`); `fsm/table_registry_validation.go:55-60` refuses to restore a snapshot unless each genesis-prefix incarnation, in any status, still carries the genesis manifest's `SchemaHash`; `server/table_registry.go:99-122` (`tableRegistryToPB`) serves `schema_hash` for every incarnation. The genesis hash is `payloadexec.TableSchemaHash(network_id, schema)` (the manifest's per-table hash, the same value `payloadexec.SchemaRoot` folds), so it is directly comparable with a recomputed contract version.
- **S2 — The one-shot read mirrors the follower's first read; the helper is not exported.** arbiter-core `v0.10.1` `dataplane/registry_follower.go:168-203` (`initial`) calls `GetTableRegistry` inside `WithLeaderRetry` and returns nil from the callback for `registryDisabled(err)` (`:252-255`: FAILED_PRECONDITION without a `NotLeader` detail, via `leaderPrecondition`, `:261-272`, and the message `TableRegistryDisabledMessage`) and for `codes.Unimplemented`. Both helpers are unexported, so `storageIntegrityRegistryDisabled` mirrors them. `WithLeaderRetry` (`dataplane/client.go:162-208`) has no attempt budget: it retries `Unavailable`, `DeadlineExceeded`, `Canceled`, `Unknown` and every FAILED_PRECONDITION (`notLeaderHint`, `:223-234`) until the context ends and returns any other code at once, so the call is bounded by a `dataplane.DefaultRegistryStartupTimeout` context, the follower's own `WaitReady` budget. The follower decodes inside the callback, which turns a decode failure into an endless retry until its timeout; the one-shot read decodes after the call and refuses at once with `decode table registry: ...` (spec §5.1 "a decode failure refuses startup"). The read goes through the `*dataplane.Client` that `standalone.Run` already builds for the follower and the SNode, through a one-method interface (`storageIntegrityRegistryClient`) so tests use a real client against an in-process gRPC server.
- **S3 — Where the split lives (audit-compatible).** All new logic is in `standalone/storage_integrity_genesis.go`. `Run` gains exactly: the `readStorageIntegrityGenesisPlan(ctx, arbClient, si.SNode.TableIDs, ...)` call right after `siRegistry` is constructed (before the contract snapshot, the schema load and the startup transaction); a `retiredSchemas` variable defaulting to the `clickhouse` refusal and set to the contract search inside the existing `network_state` branch; two new arguments to `loadStorageIntegritySchemaSets`; and `snode.Config.Tables` / the table state's `genesis` switched to `schemaSets.snode` while `tables := schemaSets.preflight`. The audits that constrain this are `TestRunStartsAllExternallyServingListenersOnlyThroughPreparedPermit` (`newStorageIntegrityBootDeps` bound to the nine exact identifiers, `tables` among them; four bare `si` references; one `cfg.StorageIntegrity` read; the snapshot's four arguments; snapshot before schema load; the snapshot and `NewNetworkStateLoader` inside the `network_state` branch; the post-Ensure cross-check bound to `schemaSnapshot`), `TestRunPinnedSnapshotAndSyncerPlacementAuditsRejectSourceMutations` (the byte markers `schemaLoader = schemaregistry.NewNetworkStateLoader(\n\t\t\t\t\tschemaSnapshot,` and `\t\t\t\tprotocolTablesMode,\n\t\t\t\tschemaSnapshot,\n\t\t\t\tschemaSets,`) and `TestStorageIntegrityProductionAdaptersCallConcreteDependencies` (the ensure adapter returns exactly `ddl.EnsureProtocolTables(ctx, d.conn, d.pinned, d.tables, d.mode, d.logger)`, the cross-check adapter is exactly two statements over `c.schemaSets.refs`, and the constructor binds `tables`/`schemaSets` straight into the adapters). Hence the preflight subset reaches the adapters as `tables` and `schemaSets.refs`, and the empty-preflight skip is a `skipPreflight` field set by the constructor from `len(tables) == 0` and read in `prepareStorageIntegrityListenerPermit` (neither is audited beyond construction discipline); the adapters' non-empty checks move to the full set (`len(c.schemaSets.snode) > 0`) per spec §5.4. No audit changes. `TestRunReadsTheRegistryBeforeLoadingGenesisSchemas` (Task 7, a new file) adds the §5.5 source-shape guard.
- **S4 — Contract schema versions.** `bindings.IDatabases` exposes `LatestTableSchemaVersion(opts, databaseID, tableID) (uint32, error)` and `GetTableSchema(opts, databaseID, tableID, version) (bindings.TypesTableSchema{SchemaHash [32]byte; SchemaJson string}, error)`, already abstracted as `storageIntegrityDatabasesSchemaCaller` in `standalone/storage_integrity_contract_snapshot.go:25-28`. compute-network-contracts `a834736` `src/Databases.sol:47-51,258-278` documents and implements versions as write-once and kept across `deleteTable` and recreation, and `getTableSchema` has no liveness check (`:341-347`), so any finalized header returns the same content for an existing version. The search reads at its own finalized header (one `HeaderByNumber(rpc.FinalizedBlockNumber)` per retired id, every call pinned to its hash) instead of threading the contract snapshot's header, which `Run` holds only as the `registry.TableSchemas` interface. The hash is recomputed with `payloadexec.TableSchemaHash(network_id, schema)`, exactly as `schemaregistry.NetworkStateLoader` and arbiter-core's `dataplane.RegistrySchema` do; a version whose JSON does not decode or names another table cannot match and is skipped; the first match wins (matches are identical schemas).
- **S5 — arbiter-verifier does not recreate a purged genesis table (spec §6 item, measured; no arbiter change).** `cmd/arbiter-verifier/main.go:181-221` loads inline `tables` (or, with `table_ids`, derives schemas from ClickHouse via `startup.TableSchemas.Wait`) and runs `verifier.New` under `startup.RunRoleWithRegistry`; `-ensure-tables` is only a cross-check of the mode (`resolveEnsureMode`, `:89-115`). There is no host-side preflight: protocol tables are ensured by arbiter-core's `verifier.Role.ensureProtocolTablesMode` (`verifier/verifier.go:184-200`), which waits for the registry follower and then runs `tableset.Reconciler.Reconcile`; with the registry enabled its registry pass (`dataplane/tableset/reconciler.go:376-419`) never ensures a key whose live incarnation is Purged or not the genesis one. The SNode embedded in sentio-node uses the same reconciler; the only recreation came from sentio-node's own preflight, which this plan fixes. Residual (flagged for 5b, not a devnet2 issue): a verifier configured with `table_ids` instead of inline `tables` derives schemas from its local `hg_unsafe` and would fail to restart after a genesis purge, like sentio-node's `clickhouse` source; devnet2's verifiers carry inline schemas.
- **S6 — Acceptance harness.** G1 fits the existing dynamic lifecycle harness unchanged in shape: a fake `TableRegistry` gRPC server, the real follower and table state, and the embedded HouseGate against ClickHouse; the only helper change is an `ordinaryInserts` switch on the fake rewriter so a processor table's INSERT is classified ordinary, as a real engine classifies any table the snapshot does not report Active. G2 cannot use an embedded `standalone.Run` restart: `Run` needs a live chain (`env.EthClient`, `env.DatabasesContract`, the syncer) and a real arbiter for membership (`TestStorageIntegritySmoke` is gated on `SENTIO_SI_E2E` and a live deployment). The smallest faithful test restarts the storage-integrity prefix of `Run` up to the listener permit with the production functions in `Run`'s order — `readStorageIntegrityGenesisPlan` over a real `dataplane.Client`, `loadStorageIntegrityContractSnapshot`, `schemaregistry.NewNetworkStateLoader`, `contractStorageIntegrityGenesisSchemas`, `loadStorageIntegritySchemaSets`, the real `snode.New` (which checks `schema_root`), `newStorageIntegrityBootDeps` and `prepareStorageIntegrityListenerPermit` (which runs the create-mode preflight) — against ClickHouse, with a scripted registry and a scripted `Databases` history. Everything after the permit (listeners, syncer, HouseGate, the follower) does not read the genesis split. The source-shape guard (S3) pins `Run` to the same order.
- **S7 — Processor flag shape.** `ChainDatabase.Processor bool` (spec §4.1 allows a bool or a type mirror). The builder needs only "is PROCESSOR"; a bool keeps unknown types governed by construction. Declaration compensation already skips non-user databases (`database_registry/compensation.go:204-213`) and is unchanged.
- **S8 — Retiring and Purging genesis ids keep today's schema source.** Spec §5.2 row 3. Under `network_state` their latest declaration is still the genesis one unless the name was recreated, which `Live` then reports as a later incarnation (row 4). Under `clickhouse` the loader reads `hg_unsafe`, which the reconciler drops during the purge; such a restart fails with the loader's error, which the README's H5 note (move to `network_state` before dropping any genesis table) covers.
- **S9 — The contract snapshot still reads every genesis id's latest declaration.** Its argument is pinned to `si.SNode.TableIDs` by the audits and it fails closed on an empty or undecodable latest declaration; only the network-state loader's hash check is limited to non-retired ids. A recreated name's latest declaration is written by this node's schema declarer (`database_registry.SchemaDeclarer.DeclarePhysical`, always a well-formed `<database>.<table>` schema), so this adds no new failure; it is recorded as a risk in the handoff.
- **S10 — Order of tasks.** G1 first (Tasks 1–3: model, builder, acceptance). G2 builds the new file bottom-up (Tasks 4–6: read, classification, retired schemas), wires it (Task 7), lets the preflight run empty (Task 8), then proves the restart (Task 9). After Task 7 alone, a node whose every genesis table is retired refuses startup at the (still non-empty) preflight check, which is fail-closed; Task 8 removes that.

## File Structure

- **Table state (G1):** modify `storageintegrityadapter/tablestate/model.go`, `storageintegrityadapter/tablestate/chain.go`, `storageintegrityadapter/tablestate/chain_test.go` — Task 1; `storageintegrityadapter/tablestate/snapshot.go`, `storageintegrityadapter/tablestate/state_test.go` — Task 2.
- **G1 acceptance:** modify `standalone/storage_integrity_acceptance_ch_test.go`, `standalone/storage_integrity_dynamic_ch_test.go`, `.github/workflows/ci.yml` — Task 3.
- **Genesis plan (G2):** create `standalone/storage_integrity_genesis.go` and `standalone/storage_integrity_genesis_test.go` — Task 4, extended in Tasks 5–8; `standalone/BUILD.bazel` — Task 4 (gazelle).
- **Wiring:** replace `standalone/storage_integrity_schemas.go`; modify `standalone/storage_integrity_schemas_test.go`, `standalone/standalone.go` — Task 7; `standalone/standalone.go` — Task 8.
- **G2 acceptance:** create `standalone/storage_integrity_genesis_ch_test.go`; modify `standalone/BUILD.bazel` (gazelle), `.github/workflows/ci.yml` — Task 9.
- **Docs:** `README.md` — Task 10.

---

## Task 1: The chain state carries the database type

**Files:**

- Modify: `storageintegrityadapter/tablestate/model.go:94-98` (`ChainDatabase`), `storageintegrityadapter/tablestate/chain.go:33` (`databaseInfoChain.Databases`)
- Test: `storageintegrityadapter/tablestate/chain_test.go:18-30` (replace `TestDatabaseInfoChainProjectsIndexerAndCreatedBlock`)

**Interfaces:**

- Consumes: `statecore.DatabaseInfo.DbType statecore.DatabaseType`, `statecore.DatabaseTypeProcessor` (sentio-core `063059e`, `network/state/types.go:32-36,93`).
- Produces: `tablestate.ChainDatabase{IndexerID uint64; Processor bool; Tables map[string]ChainTable}`; `DatabaseInfoChain(reader).Databases(ctx)` sets `Processor` exactly for `DbType == DatabaseTypeProcessor`. Task 2 reads it.

- [ ] **Step 1: Write the failing test.**

`storageintegrityadapter/tablestate/chain_test.go`, replace:

```go
func TestDatabaseInfoChainProjectsIndexerAndCreatedBlock(t *testing.T) {
	chain := DatabaseInfoChain(staticDatabaseInfos{
		"tenant": {DatabaseId: "tenant", IndexerId: 7, PendingDelete: true, Tables: []statecore.TableInfo{
			{TableId: "t", CreatedBlock: 1500, SchemaVersion: 3},
			{TableId: "old"},
		}},
	})
	got, err := chain.Databases(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]ChainDatabase{
		"tenant": {IndexerID: 7, Tables: map[string]ChainTable{"t": {CreatedBlock: 1500}, "old": {}}},
	}, got)
}
```

with:

```go
func TestDatabaseInfoChainProjectsIndexerTypeAndCreatedBlock(t *testing.T) {
	chain := DatabaseInfoChain(staticDatabaseInfos{
		"tenant": {DatabaseId: "tenant", DbType: statecore.DatabaseTypeUser, IndexerId: 7, PendingDelete: true, Tables: []statecore.TableInfo{
			{TableId: "t", CreatedBlock: 1500, SchemaVersion: 3},
			{TableId: "old"},
		}},
		"proc_0": {DatabaseId: "proc_0", DbType: statecore.DatabaseTypeProcessor, IndexerId: 7, ProcessorId: "proc", Tables: []statecore.TableInfo{
			{TableId: "events", CreatedBlock: 1600},
		}},
		// A type this build does not know stays governed: only PROCESSOR is
		// exempt from default deny (design 5a H1), so an unknown type fails
		// closed.
		"future": {DatabaseId: "future", DbType: statecore.DatabaseType(2), IndexerId: 7},
	})
	got, err := chain.Databases(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]ChainDatabase{
		"tenant": {IndexerID: 7, Tables: map[string]ChainTable{"t": {CreatedBlock: 1500}, "old": {}}},
		"proc_0": {IndexerID: 7, Processor: true, Tables: map[string]ChainTable{"events": {CreatedBlock: 1600}}},
		"future": {IndexerID: 7, Tables: map[string]ChainTable{}},
	}, got)
}
```

- [ ] **Step 2: Run it and verify it fails.** `go test ./storageintegrityadapter/tablestate/ -run TestDatabaseInfoChain`. Expected: FAIL to compile with `chain_test.go:36:28: unknown field Processor in struct literal of type ChainDatabase`.
- [ ] **Step 3: Implement.**

`storageintegrityadapter/tablestate/model.go`, replace:

```go
// ChainDatabase is one database of the syncer's chain state.
type ChainDatabase struct {
	IndexerID uint64
	Tables    map[string]ChainTable
}
```

with:

```go
// ChainDatabase is one database of the syncer's chain state.
type ChainDatabase struct {
	IndexerID uint64
	// Processor is set for a PROCESSOR database (a processor's
	// "<processorId>_<replica>" database). The table state never governs one:
	// its unrecorded tables are Ordinary, not default-denied (design 5a H1).
	Processor bool
	Tables    map[string]ChainTable
}
```

`storageintegrityadapter/tablestate/chain.go`, replace:

```go
		out[id] = ChainDatabase{IndexerID: info.IndexerId, Tables: tables}
```

with:

```go
		out[id] = ChainDatabase{
			IndexerID: info.IndexerId,
			Processor: info.DbType == statecore.DatabaseTypeProcessor,
			Tables:    tables,
		}
```

- [ ] **Step 4: Run the package.** `go test ./storageintegrityadapter/tablestate/`. Expected: `ok`.
- [ ] **Step 5: Commit.**

```bash
git add storageintegrityadapter/tablestate/model.go storageintegrityadapter/tablestate/chain.go storageintegrityadapter/tablestate/chain_test.go
git commit -F - <<'EOF'
feat(storage-integrity): carry the database type into the table state's chain view

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 2: Processor databases are ungoverned

**Files:**

- Modify: `storageintegrityadapter/tablestate/snapshot.go:64-68` (the governed set in `build`)
- Test: `storageintegrityadapter/tablestate/state_test.go:26-28, :135, :215-216, :228-233, :268, :287` (constants, a `procInc` helper before `chainState`, `TestLookupMatrix`)

**Interfaces:**

- Consumes: `ChainDatabase.Processor` (Task 1).
- Produces: with the registry enabled, `Snapshot.Lookup(db, t)` answers Ordinary for a table of a PROCESSOR database on the SI indexer unless the registry records a live non-Legacy, non-Purged incarnation for it, which is answered as for any recorded key (design 5a H1, H2). User databases are unchanged.

- [ ] **Step 1: Write the failing test.**

`storageintegrityadapter/tablestate/state_test.go` edit 1 of 6, replace:

```go
	siDatabase    = "tenant"
	otherDatabase = "elsewhere"
)
```

with:

```go
	siDatabase    = "tenant"
	otherDatabase = "elsewhere"
	// procDatabase is a PROCESSOR database on the SI indexer (design 5a H1).
	procDatabase = "proc_0"
)
```

`storageintegrityadapter/tablestate/state_test.go` edit 2 of 6, replace:

```go
func chainState(tables map[string]uint64) map[string]ChainDatabase {
```

with:

```go
// procInc is a chain incarnation in the processor database: a processor
// table whose schema someone declared, so the arbiter admitted it (H2).
func procInc(t *testing.T, table string, status IncarnationStatus) Incarnation {
	schema := schemaFor(procDatabase + "." + table)
	return Incarnation{
		DatabaseID: procDatabase, TableID: table, Origin: OriginChain, Status: status,
		SchemaJSON: schemaJSON(t, schema), SchemaHash: payloadexec.TableSchemaHash(testNetwork, schema),
	}
}

func chainState(tables map[string]uint64) map[string]ChainDatabase {
```

`storageintegrityadapter/tablestate/state_test.go` edit 3 of 6, replace:

```go
		chainInc(t, 15, "shadowed", IncarnationRefused),
	}
```

with:

```go
		chainInc(t, 15, "shadowed", IncarnationRefused),
		procInc(t, "declared_pending", IncarnationPending),
		procInc(t, "declared_active", IncarnationActive),
		procInc(t, "declared_unready", IncarnationActive),
	}
```

`storageintegrityadapter/tablestate/state_test.go` edit 4 of 6, replace:

```go
		ready.set(siDatabase+"."+id, true)
	}

	for _, seeded := range []bool{false, true} {
		reg := newFakeRegistry(true, Registry{Version: 42, Seeded: seeded, SIIndexerID: siIndexer, ActivationBlock: activation, Incarnations: incs})
		state := newTestState(t, reg, ready, &fakeChain{dbs: chainState(chainTables)}, genesis)
```

with:

```go
		ready.set(siDatabase+"."+id, true)
	}
	ready.set(procDatabase+".declared_active", true)
	chain := chainState(chainTables)
	chain[procDatabase] = ChainDatabase{IndexerID: siIndexer, Processor: true, Tables: map[string]ChainTable{
		"unrecorded_new": {CreatedBlock: 1500}, "declared_pending": {CreatedBlock: 1500},
		"declared_active": {CreatedBlock: 1500}, "declared_unready": {CreatedBlock: 1500},
	}}

	for _, seeded := range []bool{false, true} {
		reg := newFakeRegistry(true, Registry{Version: 42, Seeded: seeded, SIIndexerID: siIndexer, ActivationBlock: activation, Incarnations: incs})
		state := newTestState(t, reg, ready, &fakeChain{dbs: chain}, genesis)
```

`storageintegrityadapter/tablestate/state_test.go` edit 5 of 6, replace:

```go
		require.Equal(t, sitable.Ordinary, snap.Lookup(otherDatabase, "x").Status, "a database on another indexer is Ordinary")
```

with:

```go
		// Design 5a H1/H2: a PROCESSOR database on the SI indexer is
		// ungoverned, so an unrecorded table there is Ordinary even when it was
		// created after activation, while a key the registry records is
		// answered from the registry like any other.
		for table, want := range map[string]sitable.Status{
			"unrecorded_new":   sitable.Ordinary,
			"never_synced":     sitable.Ordinary,
			"declared_pending": sitable.Pending,
			"declared_active":  sitable.Active,
			"declared_unready": sitable.Pending,
		} {
			require.Equalf(t, want, snap.Lookup(procDatabase, table).Status, "seeded=%v processor table %s", seeded, table)
		}
		require.Equal(t, sitable.Ordinary, snap.Lookup(otherDatabase, "x").Status, "a database on another indexer is Ordinary")
```

`storageintegrityadapter/tablestate/state_test.go` edit 6 of 6, replace:

```go
		require.Equal(t, []string{"tenant.active_ready", "tenant.genesis_t", "tenant.reused", "tenant.shadowed"}, ids)
```

with:

```go
		require.Equal(t, []string{"proc_0.declared_active", "tenant.active_ready", "tenant.genesis_t", "tenant.reused", "tenant.shadowed"}, ids)
```

- [ ] **Step 2: Run it and verify it fails.** `go test ./storageintegrityadapter/tablestate/ -run TestLookupMatrix`. Expected: FAIL with `seeded=false processor table unrecorded_new` (or `never_synced`; map order varies), `expected: 0x0` (Ordinary), `actual: 0x1` (Pending).
- [ ] **Step 3: Implement.**

`storageintegrityadapter/tablestate/snapshot.go`, replace:

```go
	for database, info := range in.Chain {
		if info.IndexerID == in.Registry.SIIndexerID {
			c.governed[database] = struct{}{}
		}
	}
```

with:

```go
	for database, info := range in.Chain {
		// A PROCESSOR database is never governed (design 5a H1): the driver
		// creates its tables without declaring a schema, so default deny would
		// stall the processor. A key the registry records there is still
		// answered from the registry below (H2).
		if info.IndexerID == in.Registry.SIIndexerID && !info.Processor {
			c.governed[database] = struct{}{}
		}
	}
```

- [ ] **Step 4: Run the package.** `go test ./storageintegrityadapter/tablestate/` and `bazel test //storageintegrityadapter/tablestate:tablestate_test`. Expected: `ok` / `PASSED`.
- [ ] **Step 5: Commit.**

```bash
git add storageintegrityadapter/tablestate/snapshot.go storageintegrityadapter/tablestate/state_test.go
git commit -F - <<'EOF'
feat(storage-integrity): never default-deny tables in processor databases

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 3: Acceptance — a processor table stays ordinary with the registry enabled

**Files:**

- Test: `standalone/storage_integrity_acceptance_ch_test.go:294-297, :329-330, :525-528, :590` (the fake rewriter and the proxy config gain `ordinaryInserts`); append to `standalone/storage_integrity_dynamic_ch_test.go` (after line 313)
- Modify: `.github/workflows/ci.yml:140, :156-157` (acceptance filter and PASS count)

**Interfaces:**

- Consumes: `newStorageIntegrityTableStateRuntime`, `(*storageIntegrityTableStateRuntime).begin`, `fakeTableRegistry`, `startFakeTableRegistry`, `fixedDatabaseInfos`, `startStorageIntegrityAcceptanceProxy`, `dialStorageIntegritySession`, `(*storageIntegritySession).query` (all existing).
- Produces: `storageIntegrityAcceptanceProxyConfig.ordinaryInserts bool`; `TestStorageIntegrityDynamicProcessorTableIsOrdinary`.

- [ ] **Step 1: Write the acceptance test.**

`standalone/storage_integrity_acceptance_ch_test.go` edit 1 of 4, replace:

```go
type storageIntegrityAcceptanceRewriter struct {
	database string
	table    string
}
```

with:

```go
type storageIntegrityAcceptanceRewriter struct {
	database string
	table    string
	// ordinaryInserts classifies the table's INSERT as an ordinary write, the
	// way a real engine does for a table the snapshot does not report Active.
	ordinaryInserts bool
}
```

`standalone/storage_integrity_acceptance_ch_test.go` edit 2 of 4, replace:

```go
			PhysicalDatabase:   f.database,
			IsStorageIntegrity: true,
```

with:

```go
			PhysicalDatabase:   f.database,
			IsStorageIntegrity: !f.ordinaryInserts,
```

`standalone/storage_integrity_acceptance_ch_test.go` edit 3 of 4, replace:

```go
	// tableState, when set, is injected the way standalone.Run injects the
	// dynamic table state: the switch is on and the static list is empty.
	tableState sitable.TableState
}
```

with:

```go
	// tableState, when set, is injected the way standalone.Run injects the
	// dynamic table state: the switch is on and the static list is empty.
	tableState sitable.TableState
	// ordinaryInserts makes the fake rewriter classify the table's INSERT as
	// an ordinary write.
	ordinaryInserts bool
}
```

`standalone/storage_integrity_acceptance_ch_test.go` edit 4 of 4, replace:

```go
		Rewriter:                   &storageIntegrityAcceptanceRewriter{database: cfg.database, table: cfg.table},
```

with:

```go
		Rewriter:                   &storageIntegrityAcceptanceRewriter{database: cfg.database, table: cfg.table, ordinaryInserts: cfg.ordinaryInserts},
```

`standalone/storage_integrity_dynamic_ch_test.go`, append at the end of the file:

```go
// TestStorageIntegrityDynamicProcessorTableIsOrdinary is design 5a's G1
// acceptance: with the registry enabled, a table the driver created after
// activation in a PROCESSOR database on the SI indexer is ungoverned, so the
// embedded HouseGate serves its ordinary INSERT and SELECT exactly as it did
// before the registry was enabled (H1). A user database's table in the same
// position stays Pending (TestStorageIntegrityDynamicTableLifecycle step 1).
//
// Requires SENTIO_SI_CH_E2E=1 and CH_ADDR.
func TestStorageIntegrityDynamicProcessorTableIsOrdinary(t *testing.T) {
	addr := requireStorageIntegrityCHAcceptance(t)
	conn := openStorageIntegrityAcceptanceConn(t, addr)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	require.NoError(t, conn.Ping(ctx))

	const (
		networkID = "sentio-node-si-processor"
		tableName = "events"
		indexer   = uint64(7)
	)
	database := fmt.Sprintf("si_proc_%d_0", time.Now().UnixNano())
	logicalID := database + "." + tableName
	require.NoError(t, conn.Exec(ctx, "CREATE DATABASE "+quoteSmokeIdentifier(database)))
	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancelCleanup()
		if err := conn.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteSmokeIdentifier(database)+" SYNC"); err != nil {
			t.Errorf("drop %s: %v", database, err)
		}
	})
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.%s (tag String, value UInt64) ENGINE = MergeTree ORDER BY tag",
		quoteSmokeIdentifier(database), quoteSmokeIdentifier(tableName))))

	// The arbiter registry: enabled and seeded, nothing recorded. The driver
	// created the table after activation_block and declared no schema.
	fakeRegistry := newFakeTableRegistry(&pb.TableRegistrySnapshot{
		Params:  &pb.TableRegistryParams{SiIndexerId: indexer, ActivationBlock: 100, Confirmation: "safe"},
		Version: 1,
		Seeded:  true,
	})
	arbClient, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "arb-1", GRPCAddr: startFakeTableRegistry(t, fakeRegistry)}}})
	require.NoError(t, err)
	t.Cleanup(arbClient.Close)
	tableState, err := newStorageIntegrityTableStateRuntime(storageIntegrityTableStateDeps{
		follower:  dataplane.NewRegistryFollower(arbClient, slog.Default()),
		readiness: clickHouseProtocolReadiness(conn),
		chain: fixedDatabaseInfos{database: {
			DatabaseId: database, DbType: statecore.DatabaseTypeProcessor, IndexerId: indexer, ProcessorId: "processor",
			Tables: []statecore.TableInfo{{TableId: tableName, TableType: "ANALYTIC", CreatedBlock: 150}},
		}},
		networkID: networkID,
	})
	require.NoError(t, err)
	tx := newStartupTransaction(ctx)
	t.Cleanup(tx.stopAndWait)
	require.NoError(t, tableState.begin(tx.Context(), tx))
	require.NoError(t, tx.commit())
	require.Equal(t, "ordinary", tableState.tableState().Current().Lookup(database, tableName).Status.String())

	signerKeyHex := "3333333333333333333333333333333333333333333333333333333333333333"
	signer, err := auth.NewRelaySigner(signerKeyHex)
	require.NoError(t, err)
	netState := network.NewInMemoryNetworkState()
	netState.DatabaseInfos[network.Database("system")] = network.DatabaseInfo{IndexerId: 0}
	netState.DatabaseInfos[network.Database(database)] = network.DatabaseInfo{IndexerId: 0}
	netState.DatabasePermissions[network.AccountAddress(strings.ToLower(signer.Address()))] =
		network.DatabasePermissions{network.Database(database): registry.DbAuthWrite | registry.DbAuthRead}
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	require.NoError(t, err)
	spool, err := sicore.NewFilePayloadSpool(t.TempDir())
	require.NoError(t, err)
	proxyAddr := startStorageIntegrityAcceptanceProxy(t, addr, storageIntegrityAcceptanceProxyConfig{
		networkID:       networkID,
		database:        database,
		table:           tableName,
		logicalID:       logicalID,
		signerAddress:   signer.Address(),
		netState:        netState,
		tableState:      tableState.tableState(),
		ordinaryInserts: true,
		runtime: housegate.StorageIntegrityRuntimeOptions{
			SourcePreparer:     storageintegrityadapter.NewSourcePreparer(&throttlingSNodeRole{table: sicore.PhysicalTableName(logicalID)}),
			StatementSubmitter: &acceptanceStatementSubmitter{},
			StatusQuerier:      acceptanceStatusQuerier{},
			PayloadWriter:      acceptancePayloadWriter{},
			Journal:            journal,
			PayloadSpool:       spool,
			MergeGuard:         acceptanceMergeGuard{},
		},
	})
	session := dialStorageIntegritySession(t, ctx, proxyAddr, database, signer)
	defer session.close()

	_, err = session.query(fmt.Sprintf("INSERT INTO %s SELECT 'processor-row', 7", logicalID))
	require.NoError(t, err, "an ordinary INSERT into a processor table must not be refused")
	rows, err := session.query(fmt.Sprintf("SELECT tag FROM %s", logicalID))
	require.NoError(t, err, "an ordinary SELECT of a processor table must not be refused")
	require.Contains(t, string(rows), "processor-row")
	var count uint64
	require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s.%s",
		quoteSmokeIdentifier(database), quoteSmokeIdentifier(tableName))).Scan(&count))
	require.Equal(t, uint64(1), count)
}
```

- [ ] **Step 2: Run it against ClickHouse.** With the acceptance environment from Global Constraints: `go test ./standalone/ -run 'TestStorageIntegrityDynamicProcessorTableIsOrdinary|TestStorageIntegrityDynamicTableLifecycle' -count=1 -v`. Expected: `--- PASS` for both (the lifecycle test proves the fake rewriter's default is unchanged). Before Task 2 this test fails with `expected: "ordinary"`, `actual: "pending"`; without that early assertion the proxy answers the INSERT with `DB::Exception (733): storage_integrity: table si_proc_…_0.events is pending activation (retryable)`.
- [ ] **Step 3: Add it to CI.**

`.github/workflows/ci.yml` edit 1 of 2, replace:

```yaml
            --test_filter='TestStorageIntegrityMalformedColumnTypeCreatesNoTable|TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession|TestStorageIntegrityDynamicTableLifecycle' \
```

with:

```yaml
            --test_filter='TestStorageIntegrityMalformedColumnTypeCreatesNoTable|TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession|TestStorageIntegrityDynamicTableLifecycle|TestStorageIntegrityDynamicProcessorTableIsOrdinary' \
```

`.github/workflows/ci.yml` edit 2 of 2, replace:

```yaml
          if [ "${passes}" -ne 3 ]; then
            echo "expected 3 storage-integrity acceptance PASS markers, saw ${passes}"
```

with:

```yaml
          if [ "${passes}" -ne 4 ]; then
            echo "expected 4 storage-integrity acceptance PASS markers, saw ${passes}"
```

- [ ] **Step 4: Verify.** `go vet ./standalone/`. Expected: no output.
- [ ] **Step 5: Commit.**

```bash
git add standalone/storage_integrity_acceptance_ch_test.go standalone/storage_integrity_dynamic_ch_test.go .github/workflows/ci.yml
git commit -F - <<'EOF'
test(storage-integrity): acceptance for ordinary processor tables with the registry enabled

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 4: Read the arbiter table registry once

**Files:**

- Create: `standalone/storage_integrity_genesis.go`, `standalone/storage_integrity_genesis_test.go`
- Modify: `standalone/BUILD.bazel` (gazelle)

**Interfaces:**

- Consumes: `(*dataplane.Client).WithLeaderRetry(ctx context.Context, fn func(ctx context.Context, conn *grpc.ClientConn) error) error`, `pb.NewTableRegistryClient(conn).GetTableRegistry(ctx, *emptypb.Empty) (*pb.TableRegistrySnapshot, error)`, `dataplane.TableRegistryDisabledMessage`, `wire.TableRegistrySnapshotFromPB`.
- Produces: `type storageIntegrityRegistryClient interface{ WithLeaderRetry(...) error }`; `func readStorageIntegrityTableRegistry(ctx context.Context, client storageIntegrityRegistryClient, timeout time.Duration) (wire.TableRegistrySnapshot, bool, error)` (`bool` is "enabled"); `func storageIntegrityRegistryDisabled(err error) bool`. Test helpers used by Tasks 5–9: `scriptedTableRegistry` (with `set`), `scriptedArbiterClient(t, registry) *dataplane.Client`, `genesisRegistrySnapshot(...*pb.TableIncarnation) *pb.TableRegistrySnapshot`.

- [ ] **Step 1: Write the failing test.**

Create `standalone/storage_integrity_genesis_test.go`:

```go
package standalone

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// scriptedTableRegistry answers every GetTableRegistry with its current
// result, which set replaces.
type scriptedTableRegistry struct {
	pb.UnimplementedTableRegistryServer
	mu     sync.Mutex
	answer *pb.TableRegistrySnapshot
	err    error
	calls  atomic.Int32
}

func (s *scriptedTableRegistry) GetTableRegistry(context.Context, *emptypb.Empty) (*pb.TableRegistrySnapshot, error) {
	s.calls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.answer, s.err
}

func (s *scriptedTableRegistry) set(answer *pb.TableRegistrySnapshot, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.answer, s.err = answer, err
}

// scriptedArbiterClient serves registry over gRPC and returns the real
// dataplane client standalone.Run reads it through. A nil registry serves no
// TableRegistry service at all, as an arbiter older than the registry does.
func scriptedArbiterClient(t *testing.T, registry *scriptedTableRegistry) *dataplane.Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	if registry != nil {
		pb.RegisterTableRegistryServer(server, registry)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "arb-1", GRPCAddr: listener.Addr().String()}}})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func genesisRegistrySnapshot(incarnations ...*pb.TableIncarnation) *pb.TableRegistrySnapshot {
	for i, inc := range incarnations {
		inc.Seq = uint64(i + 1)
	}
	return &pb.TableRegistrySnapshot{
		Params:       &pb.TableRegistryParams{SiIndexerId: 7, ActivationBlock: 100, Confirmation: "safe"},
		Version:      3,
		Seeded:       true,
		Incarnations: incarnations,
	}
}

func TestReadStorageIntegrityTableRegistryClassifiesTheAnswer(t *testing.T) {
	enabled := genesisRegistrySnapshot(&pb.TableIncarnation{
		DatabaseId: "orders", TableId: "t", SchemaHash: "0x01",
		Origin: pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_GENESIS,
		Status: pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_ACTIVE,
	})
	for _, tc := range []struct {
		name        string
		registry    *scriptedTableRegistry
		wantEnabled bool
	}{
		{name: "enabled", registry: &scriptedTableRegistry{answer: enabled}, wantEnabled: true},
		{name: "disabled by the leader", registry: &scriptedTableRegistry{err: status.Error(codes.FailedPrecondition, dataplane.TableRegistryDisabledMessage)}},
		{name: "arbiter without the TableRegistry service"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, gotEnabled, err := readStorageIntegrityTableRegistry(t.Context(), scriptedArbiterClient(t, tc.registry), 5*time.Second)
			require.NoError(t, err)
			require.Equal(t, tc.wantEnabled, gotEnabled)
			if tc.wantEnabled {
				require.Equal(t, uint64(3), snap.Version)
				require.Len(t, snap.Incarnations, 1)
				require.Equal(t, wire.TableOriginGenesis, snap.Incarnations[0].Origin)
			} else {
				require.Equal(t, wire.TableRegistrySnapshot{}, snap)
			}
		})
	}
}

// A follower answers FAILED_PRECONDITION with a NotLeader detail; reading that
// as "disabled" would make every genesis id live and let the preflight
// recreate a purged genesis table. It must be followed, and a registry that
// never answers refuses startup.
func TestReadStorageIntegrityTableRegistryDoesNotReadARedirectAsDisabled(t *testing.T) {
	redirect, err := status.New(codes.FailedPrecondition, dataplane.TableRegistryDisabledMessage).
		WithDetails(&pb.NotLeader{LeaderAddr: ""})
	require.NoError(t, err)
	registry := &scriptedTableRegistry{err: redirect.Err()}
	_, _, err = readStorageIntegrityTableRegistry(t.Context(), scriptedArbiterClient(t, registry), 500*time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "get table registry")
	require.Greater(t, registry.calls.Load(), int32(1), "the redirect is retried until the startup timeout")
}

func TestReadStorageIntegrityTableRegistryRefusesFailures(t *testing.T) {
	undecodable := genesisRegistrySnapshot(&pb.TableIncarnation{DatabaseId: "orders", TableId: "t",
		Origin: pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_GENESIS,
		Status: pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_ACTIVE})
	undecodable.Incarnations[0].Seq = 2

	registry := &scriptedTableRegistry{answer: undecodable}
	_, _, err := readStorageIntegrityTableRegistry(t.Context(), scriptedArbiterClient(t, registry), 5*time.Second)
	require.ErrorContains(t, err, "decode table registry")
	require.Equal(t, int32(1), registry.calls.Load(), "an undecodable answer refuses at once")

	registry = &scriptedTableRegistry{err: status.Error(codes.Internal, "boom")}
	_, _, err = readStorageIntegrityTableRegistry(t.Context(), scriptedArbiterClient(t, registry), 5*time.Second)
	require.ErrorContains(t, err, "boom")
	require.Equal(t, int32(1), registry.calls.Load(), "a non-retryable error refuses at once")
}
```

- [ ] **Step 2: Run it and verify it fails.** `go test ./standalone/ -run TestReadStorageIntegrityTableRegistry`. Expected: FAIL to compile with `storage_integrity_genesis_test.go:91:29: undefined: readStorageIntegrityTableRegistry`.
- [ ] **Step 3: Implement.**

Create `standalone/storage_integrity_genesis.go`:

```go
package standalone

import (
	"context"
	"fmt"
	"time"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// storageIntegrityRegistryClient is the arbiter client's leader-retrying call
// seam; *dataplane.Client implements it.
type storageIntegrityRegistryClient interface {
	WithLeaderRetry(ctx context.Context, fn func(ctx context.Context, conn *grpc.ClientConn) error) error
}

var _ storageIntegrityRegistryClient = (*dataplane.Client)(nil)

// readStorageIntegrityTableRegistry reads the arbiter's table registry once,
// before the registry follower runs (design 5a §5.1). It classifies the
// answer exactly as dataplane.RegistryFollower's first Get does: the leader's
// "table registry is disabled" and an arbiter without the TableRegistry
// service (Unimplemented) both mean the registry is disabled. Anything else
// that WithLeaderRetry gives up on, a timeout, or an answer that does not
// decode refuses startup.
func readStorageIntegrityTableRegistry(
	ctx context.Context,
	client storageIntegrityRegistryClient,
	timeout time.Duration,
) (wire.TableRegistrySnapshot, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var answer *pb.TableRegistrySnapshot
	err := client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		m, err := pb.NewTableRegistryClient(conn).GetTableRegistry(ctx, &emptypb.Empty{})
		switch {
		case err == nil:
			answer = m
			return nil
		case storageIntegrityRegistryDisabled(err):
			return nil
		}
		return err
	})
	if err != nil {
		return wire.TableRegistrySnapshot{}, false, fmt.Errorf("get table registry: %w", err)
	}
	if answer == nil {
		return wire.TableRegistrySnapshot{}, false, nil
	}
	snap, err := wire.TableRegistrySnapshotFromPB(answer)
	if err != nil {
		return wire.TableRegistrySnapshot{}, false, fmt.Errorf("decode table registry: %w", err)
	}
	return snap, true, nil
}

// storageIntegrityRegistryDisabled mirrors the unexported classification of
// dataplane.RegistryFollower's first Get: Unimplemented, or FAILED_PRECONDITION
// with dataplane.TableRegistryDisabledMessage and no NotLeader detail. A
// FAILED_PRECONDITION that carries NotLeader is a follower's redirect, which
// WithLeaderRetry must keep following.
func storageIntegrityRegistryDisabled(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unimplemented:
		return true
	case codes.FailedPrecondition:
		for _, detail := range st.Details() {
			if _, notLeader := detail.(*pb.NotLeader); notLeader {
				return false
			}
		}
		return st.Message() == dataplane.TableRegistryDisabledMessage
	}
	return false
}
```

- [ ] **Step 4: Run.** `go test ./standalone/ -run TestReadStorageIntegrityTableRegistry`. Expected: `ok` (the redirect test takes about 0.5s). Then `bazel run //:gazelle` (it adds both files and the `@arbiter_core//wire`, `@org_golang_google_grpc//codes`, `@org_golang_google_grpc//status`, `@org_golang_google_protobuf//types/known/emptypb` library deps and the `codes`/`status` test deps to `standalone/BUILD.bazel`) and `bazel test //standalone:standalone_test`. Expected: `PASSED`.
- [ ] **Step 5: Commit.**

```bash
git add standalone/storage_integrity_genesis.go standalone/storage_integrity_genesis_test.go standalone/BUILD.bazel
git commit -F - <<'EOF'
feat(storage-integrity): read the arbiter table registry once at startup

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 5: Classify the genesis tables against the registry

**Files:**

- Modify: `standalone/storage_integrity_genesis.go:3-15` (imports) and append
- Test: append to `standalone/storage_integrity_genesis_test.go`

**Interfaces:**

- Consumes: `readStorageIntegrityTableRegistry` (Task 4), `wire.TableRegistrySnapshot.Live(key string) *wire.TableIncarnation`, `wire.TableOriginGenesis`, `wire.TableStatus*`, `dataplane.DefaultRegistryStartupTimeout`.
- Produces: `type storageIntegrityGenesisTable struct{ id string; preflight bool; retiredSchemaHash string }` with `retired() bool`; `func planStorageIntegrityGenesis(ids []string, snap wire.TableRegistrySnapshot, enabled bool) ([]storageIntegrityGenesisTable, error)`; `func readStorageIntegrityGenesisPlan(ctx context.Context, client storageIntegrityRegistryClient, genesisTableIDs []string, logger *slog.Logger) ([]storageIntegrityGenesisTable, error)` (Run's one-shot read, Task 7). Rows (spec §5.2): registry disabled → `{preflight: true}`; live genesis Legacy/Active → `{preflight: true}`; live genesis Retiring/Purging → `{}`; genesis Purged or a later incarnation live → `{retiredSchemaHash: <genesis incarnation's schema_hash>}`.

- [ ] **Step 1: Write the failing test.**

`standalone/storage_integrity_genesis_test.go`, append at the end of the file:

```go
func genesisIncarnation(table string, status wire.TableIncarnationStatus) wire.TableIncarnation {
	return wire.TableIncarnation{DatabaseID: "orders", TableID: table, Origin: wire.TableOriginGenesis, Status: status, SchemaHash: "0xgenesis-" + table}
}

func laterIncarnation(table string, origin wire.TableOrigin, status wire.TableIncarnationStatus) wire.TableIncarnation {
	return wire.TableIncarnation{DatabaseID: "orders", TableID: table, Origin: origin, Status: status, SchemaHash: "0xlater-" + table}
}

func numberedRegistry(incarnations ...wire.TableIncarnation) wire.TableRegistrySnapshot {
	for i := range incarnations {
		incarnations[i].Seq = uint64(i + 1)
	}
	return wire.TableRegistrySnapshot{Version: 9, Seeded: true, Incarnations: incarnations}
}

// TestPlanStorageIntegrityGenesisRows is design 5a §5.2's table, one row per
// registry state of a genesis id.
func TestPlanStorageIntegrityGenesisRows(t *testing.T) {
	snap := numberedRegistry(
		genesisIncarnation("active", wire.TableStatusActive),
		genesisIncarnation("legacy", wire.TableStatusLegacy),
		genesisIncarnation("retiring", wire.TableStatusRetiring),
		genesisIncarnation("purging", wire.TableStatusPurging),
		genesisIncarnation("purged", wire.TableStatusPurged),
		genesisIncarnation("recreated", wire.TableStatusPurged),
		genesisIncarnation("recreated_early", wire.TableStatusPurging),
		genesisIncarnation("seeded_legacy", wire.TableStatusPurged),
		genesisIncarnation("refused_after", wire.TableStatusPurged),
		laterIncarnation("recreated", wire.TableOriginChain, wire.TableStatusActive),
		laterIncarnation("recreated_early", wire.TableOriginChain, wire.TableStatusPending),
		laterIncarnation("seeded_legacy", wire.TableOriginLegacy, wire.TableStatusLegacy),
		laterIncarnation("refused_after", wire.TableOriginChain, wire.TableStatusRefused),
	)
	ids := []string{"orders.active", "orders.legacy", "orders.retiring", "orders.purging", "orders.purged",
		"orders.recreated", "orders.recreated_early", "orders.seeded_legacy", "orders.refused_after"}

	plan, err := planStorageIntegrityGenesis(ids, snap, true)
	require.NoError(t, err)
	require.Equal(t, []storageIntegrityGenesisTable{
		{id: "orders.active", preflight: true},
		{id: "orders.legacy", preflight: true},
		{id: "orders.retiring"},
		{id: "orders.purging"},
		{id: "orders.purged", retiredSchemaHash: "0xgenesis-purged"},
		{id: "orders.recreated", retiredSchemaHash: "0xgenesis-recreated"},
		{id: "orders.recreated_early", retiredSchemaHash: "0xgenesis-recreated_early"},
		{id: "orders.seeded_legacy", retiredSchemaHash: "0xgenesis-seeded_legacy"},
		{id: "orders.refused_after", retiredSchemaHash: "0xgenesis-refused_after"},
	}, plan, "a retired id always takes the genesis incarnation's hash, never the live incarnation's")

	plan, err = planStorageIntegrityGenesis(ids, snap, false)
	require.NoError(t, err)
	for _, table := range plan {
		require.Equalf(t, storageIntegrityGenesisTable{id: table.id, preflight: true}, table,
			"registry disabled: %s is live, as before", table.id)
	}
}

func TestPlanStorageIntegrityGenesisRefusesAnInconsistentRegistry(t *testing.T) {
	for _, tc := range []struct {
		name string
		snap wire.TableRegistrySnapshot
		want string
	}{
		{name: "key absent", snap: numberedRegistry(genesisIncarnation("other", wire.TableStatusActive)),
			want: `genesis table "orders.t" has no genesis incarnation`},
		{name: "only a chain incarnation", snap: numberedRegistry(laterIncarnation("t", wire.TableOriginChain, wire.TableStatusActive)),
			want: `genesis table "orders.t" has no genesis incarnation`},
		{name: "two genesis incarnations", snap: numberedRegistry(genesisIncarnation("t", wire.TableStatusPurged), genesisIncarnation("t", wire.TableStatusActive)),
			want: `genesis table "orders.t" has more than one genesis incarnation`},
		{name: "genesis pending", snap: numberedRegistry(genesisIncarnation("t", wire.TableStatusPending)),
			want: `genesis incarnation 1 is pending`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := planStorageIntegrityGenesis([]string{"orders.t"}, tc.snap, true)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
```

- [ ] **Step 2: Run it and verify it fails.** `go test ./standalone/ -run TestPlanStorageIntegrityGenesis`. Expected: FAIL to compile with `storage_integrity_genesis_test.go:173:15: undefined: planStorageIntegrityGenesis`.
- [ ] **Step 3: Implement.**

`standalone/storage_integrity_genesis.go` edit 1 of 2, replace:

```go
import (
	"context"
	"fmt"
	"time"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)
```

with:

```go
import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)
```

`standalone/storage_integrity_genesis.go` edit 2 of 2, append at the end of the file:

```go
// storageIntegrityGenesisTable is one genesis id (storage_integrity.snode.
// table_ids) classified against the one-shot registry view (design 5a §5.2).
type storageIntegrityGenesisTable struct {
	id string
	// preflight is set when the create-mode preflight ensures and
	// cross-checks the id's hg_* tables. It is cleared once the genesis
	// incarnation retires: the reconciler drops those tables, and the
	// preflight must not recreate them.
	preflight bool
	// retiredSchemaHash is the genesis incarnation's schema_hash when the id
	// is retired: its genesis incarnation is Purged, or a later incarnation of
	// the name is live. The genesis schema is then found by this hash (§5.3).
	// It is empty otherwise, and the schema is loaded as before.
	retiredSchemaHash string
}

// retired reports whether the genesis schema must be found by hash.
func (g storageIntegrityGenesisTable) retired() bool { return g.retiredSchemaHash != "" }

// planStorageIntegrityGenesis classifies every genesis id (design 5a §5.2).
// While the registry is disabled every id is live, as before. With it
// enabled, each id must have exactly one genesis-origin incarnation.
func planStorageIntegrityGenesis(
	ids []string,
	snap wire.TableRegistrySnapshot,
	enabled bool,
) ([]storageIntegrityGenesisTable, error) {
	plan := make([]storageIntegrityGenesisTable, 0, len(ids))
	for _, id := range ids {
		if !enabled {
			plan = append(plan, storageIntegrityGenesisTable{id: id, preflight: true})
			continue
		}
		var genesis *wire.TableIncarnation
		for i := range snap.Incarnations {
			inc := &snap.Incarnations[i]
			if inc.Key() != id || inc.Origin != wire.TableOriginGenesis {
				continue
			}
			if genesis != nil {
				return nil, fmt.Errorf("storage integrity genesis table %q has more than one genesis incarnation in the table registry", id)
			}
			genesis = inc
		}
		if genesis == nil {
			return nil, fmt.Errorf("storage integrity genesis table %q has no genesis incarnation in the table registry", id)
		}
		if live := snap.Live(id); live.Seq != genesis.Seq {
			plan = append(plan, retiredStorageIntegrityGenesis(id, genesis))
			continue
		}
		switch genesis.Status {
		case wire.TableStatusLegacy, wire.TableStatusActive:
			plan = append(plan, storageIntegrityGenesisTable{id: id, preflight: true})
		case wire.TableStatusRetiring, wire.TableStatusPurging:
			plan = append(plan, storageIntegrityGenesisTable{id: id})
		case wire.TableStatusPurged:
			plan = append(plan, retiredStorageIntegrityGenesis(id, genesis))
		default:
			return nil, fmt.Errorf("storage integrity genesis table %q: genesis incarnation %d is %s", id, genesis.Seq, genesis.Status)
		}
	}
	return plan, nil
}

func retiredStorageIntegrityGenesis(id string, genesis *wire.TableIncarnation) storageIntegrityGenesisTable {
	return storageIntegrityGenesisTable{id: id, retiredSchemaHash: genesis.SchemaHash}
}

// readStorageIntegrityGenesisPlan is standalone.Run's one-shot registry read
// (design 5a H4): it reads the registry through the arbiter client before any
// genesis schema is loaded and classifies the genesis ids. The registry
// follower still starts inside the startup transaction; this view decides
// only where each genesis schema comes from and which ids the create-mode
// preflight ensures.
func readStorageIntegrityGenesisPlan(
	ctx context.Context,
	client storageIntegrityRegistryClient,
	genesisTableIDs []string,
	logger *slog.Logger,
) ([]storageIntegrityGenesisTable, error) {
	snap, enabled, err := readStorageIntegrityTableRegistry(ctx, client, dataplane.DefaultRegistryStartupTimeout)
	if err != nil {
		return nil, err
	}
	plan, err := planStorageIntegrityGenesis(genesisTableIDs, snap, enabled)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	for _, table := range plan {
		switch {
		case table.retired():
			logger.Info("storage integrity genesis table is retired: its genesis schema is found by hash and its protocol tables are not ensured",
				"table", table.id, "schema_hash", table.retiredSchemaHash, "registry_version", snap.Version)
		case !table.preflight:
			logger.Info("storage integrity genesis table is retiring: its protocol tables are not ensured",
				"table", table.id, "registry_version", snap.Version)
		}
	}
	return plan, nil
}
```

- [ ] **Step 4: Run.** `go test ./standalone/ -run 'TestPlanStorageIntegrityGenesis|TestReadStorageIntegrityTableRegistry'`. Expected: `ok`. `bazel run //:gazelle` leaves `BUILD.bazel` unchanged.
- [ ] **Step 5: Commit.**

```bash
git add standalone/storage_integrity_genesis.go standalone/storage_integrity_genesis_test.go
git commit -F - <<'EOF'
feat(storage-integrity): classify genesis tables against the registry

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 6: Find a retired genesis table's schema by its genesis hash

**Files:**

- Modify: `standalone/storage_integrity_genesis.go:3-16` (imports) and append
- Test: `standalone/storage_integrity_genesis_test.go:3-19` (imports) and append

**Interfaces:**

- Consumes: `storageIntegrityHeaderReader` and `storageIntegrityDatabasesSchemaCaller` (`standalone/storage_integrity_contract_snapshot.go:20-28`), `payloadexec.TableSchemaHash`, `storageIntegritySchemaSourceDefault` (`"clickhouse"`); test fake `fakeStorageIntegrityHeaderReader` (`standalone/storage_integrity_contract_snapshot_test.go`).
- Produces: `type storageIntegrityRetiredGenesisSchemas func(ctx context.Context, tableID, schemaHash string) (payloadexec.TableSchema, error)`; `func refuseRetiredStorageIntegrityGenesis(ctx context.Context, tableID, schemaHash string) (payloadexec.TableSchema, error)` (H5); `func contractStorageIntegrityGenesisSchemas(headers storageIntegrityHeaderReader, caller storageIntegrityDatabasesSchemaCaller, networkID string) storageIntegrityRetiredGenesisSchemas` (spec §5.3). Test helpers used by Task 9: `versionedDatabasesCaller`, `contractSchemaVersion(t, networkID, schema) bindings.TypesTableSchema`.

- [ ] **Step 1: Write the failing test.**

`standalone/storage_integrity_genesis_test.go` edit 1 of 2, replace:

```go
import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)
```

with:

```go
import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"compute-network-node/bindings"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)
```

`standalone/storage_integrity_genesis_test.go` edit 2 of 2, append at the end of the file:

```go
// versionedDatabasesCaller is the Databases contract's schema history for
// one table: versions[i] is version i+1.
type versionedDatabasesCaller struct {
	versions []bindings.TypesTableSchema
	failAt   uint32
	reads    []uint32
	blocks   []ethcommon.Hash
}

func (c *versionedDatabasesCaller) LatestTableSchemaVersion(opts *bind.CallOpts, _, _ string) (uint32, error) {
	c.blocks = append(c.blocks, opts.BlockHash)
	return uint32(len(c.versions)), nil
}

func (c *versionedDatabasesCaller) GetTableSchema(opts *bind.CallOpts, _, _ string, version uint32) (bindings.TypesTableSchema, error) {
	c.blocks = append(c.blocks, opts.BlockHash)
	c.reads = append(c.reads, version)
	if version == c.failAt {
		return bindings.TypesTableSchema{}, errors.New("rpc unavailable")
	}
	return c.versions[version-1], nil
}

// contractSchemaVersion is one Databases-contract declaration of schema, with
// the hash the schema declarer records for it.
func contractSchemaVersion(t *testing.T, networkID string, schema payloadexec.TableSchema) bindings.TypesTableSchema {
	t.Helper()
	raw, err := json.Marshal(schema)
	require.NoError(t, err)
	return bindings.TypesTableSchema{
		SchemaHash: ethcommon.HexToHash(payloadexec.TableSchemaHash(networkID, schema)),
		SchemaJson: string(raw),
	}
}

func TestContractStorageIntegrityGenesisSchemasSearchesByHash(t *testing.T) {
	const networkID = "devnet2"
	genesis := payloadexec.TableSchema{TableID: "orders.t", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}}
	recreated := payloadexec.TableSchema{TableID: "orders.t", Columns: []lthash.Column{{Name: "value", Type: "String"}}}
	third := payloadexec.TableSchema{TableID: "orders.t", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}}}
	genesisHash := payloadexec.TableSchemaHash(networkID, genesis)
	headers := &fakeStorageIntegrityHeaderReader{header: &types.Header{Number: ethcommon.Big1}}

	t.Run("match on version 1 of 3", func(t *testing.T) {
		caller := &versionedDatabasesCaller{versions: []bindings.TypesTableSchema{
			contractSchemaVersion(t, networkID, genesis), contractSchemaVersion(t, networkID, recreated), contractSchemaVersion(t, networkID, third),
		}}
		got, err := contractStorageIntegrityGenesisSchemas(headers, caller, networkID)(t.Context(), "orders.t", genesisHash)
		require.NoError(t, err)
		require.Equal(t, genesis, got, "a later version with a different schema does not shadow the genesis one")
		require.Equal(t, []uint32{1}, caller.reads, "the search stops at the first match")
		for _, block := range caller.blocks {
			require.Equal(t, headers.header.Hash(), block, "every read is pinned to one finalized header")
		}
	})
	t.Run("undecodable and foreign versions are skipped", func(t *testing.T) {
		foreign := genesis
		foreign.TableID = "orders.other"
		caller := &versionedDatabasesCaller{versions: []bindings.TypesTableSchema{
			{SchemaHash: [32]byte{1}, SchemaJson: "{not json"}, contractSchemaVersion(t, networkID, foreign), contractSchemaVersion(t, networkID, genesis),
		}}
		got, err := contractStorageIntegrityGenesisSchemas(headers, caller, networkID)(t.Context(), "orders.t", genesisHash)
		require.NoError(t, err)
		require.Equal(t, genesis, got)
		require.Equal(t, []uint32{1, 2, 3}, caller.reads)
	})
	t.Run("no match", func(t *testing.T) {
		caller := &versionedDatabasesCaller{versions: []bindings.TypesTableSchema{contractSchemaVersion(t, networkID, recreated), contractSchemaVersion(t, networkID, third)}}
		_, err := contractStorageIntegrityGenesisSchemas(headers, caller, networkID)(t.Context(), "orders.t", genesisHash)
		require.ErrorContains(t, err, `genesis table "orders.t": none of its 2 declared schema versions`)
		require.ErrorContains(t, err, genesisHash)
	})
	t.Run("the network id is part of the hash", func(t *testing.T) {
		caller := &versionedDatabasesCaller{versions: []bindings.TypesTableSchema{contractSchemaVersion(t, networkID, genesis)}}
		_, err := contractStorageIntegrityGenesisSchemas(headers, caller, "other-network")(t.Context(), "orders.t", genesisHash)
		require.ErrorContains(t, err, "none of its 1 declared schema versions")
	})
	t.Run("a failed read refuses", func(t *testing.T) {
		caller := &versionedDatabasesCaller{failAt: 1, versions: []bindings.TypesTableSchema{contractSchemaVersion(t, networkID, genesis)}}
		_, err := contractStorageIntegrityGenesisSchemas(headers, caller, networkID)(t.Context(), "orders.t", genesisHash)
		require.ErrorContains(t, err, "rpc unavailable")
	})
}

func TestRefuseRetiredStorageIntegrityGenesisNamesTheFix(t *testing.T) {
	_, err := refuseRetiredStorageIntegrityGenesis(t.Context(), "orders.t", "0x01")
	require.ErrorContains(t, err, `genesis table "orders.t" was dropped`)
	require.ErrorContains(t, err, `schema_source "clickhouse"`)
	require.ErrorContains(t, err, "set storage_integrity.snode.schema_source to network_state")
}
```

- [ ] **Step 2: Run it and verify it fails.** `go test ./standalone/ -run 'TestContractStorageIntegrityGenesisSchemas|TestRefuseRetiredStorageIntegrityGenesis'`. Expected: FAIL to compile with `storage_integrity_genesis_test.go:273:15: undefined: contractStorageIntegrityGenesisSchemas`.
- [ ] **Step 3: Implement.**

`standalone/storage_integrity_genesis.go` edit 1 of 2, replace:

```go
import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)
```

with:

```go
import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)
```

`standalone/storage_integrity_genesis.go` edit 2 of 2, append at the end of the file:

```go
// storageIntegrityRetiredGenesisSchemas returns the immutable genesis schema
// of a retired genesis id, identified by its genesis incarnation's
// schema_hash (design 5a §5.3). schema_root commits to that schema, so it is
// what snode.Config.Tables must keep.
type storageIntegrityRetiredGenesisSchemas func(ctx context.Context, tableID, schemaHash string) (payloadexec.TableSchema, error)

// refuseRetiredStorageIntegrityGenesis is the clickhouse schema source's
// answer (design 5a H5): it derives schemas from the local hg_unsafe tables,
// which the reconciler drops when a genesis table is purged, so there is no
// durable source left for the schema.
func refuseRetiredStorageIntegrityGenesis(_ context.Context, tableID, _ string) (payloadexec.TableSchema, error) {
	return payloadexec.TableSchema{}, fmt.Errorf(
		"storage integrity genesis table %q was dropped (its genesis incarnation is no longer live in the table registry) "+
			"and schema_source %q cannot recover a dropped table's schema: set storage_integrity.snode.schema_source to network_state",
		tableID, storageIntegritySchemaSourceDefault,
	)
}

// contractStorageIntegrityGenesisSchemas finds a retired genesis id's schema
// among the table's Databases-contract declarations, read at one finalized
// header: versions 1 through LatestTableSchemaVersion, the first whose
// TableSchemaHash under networkID equals the genesis incarnation's
// schema_hash. Declarations are write-once per version and survive table
// deletion and recreation, so any finalized header answers the same. A
// version that does not decode or names another table cannot match and is
// skipped; a failed read, or no match, refuses startup.
func contractStorageIntegrityGenesisSchemas(
	headers storageIntegrityHeaderReader,
	caller storageIntegrityDatabasesSchemaCaller,
	networkID string,
) storageIntegrityRetiredGenesisSchemas {
	return func(ctx context.Context, tableID, schemaHash string) (payloadexec.TableSchema, error) {
		databaseID, table, ok := strings.Cut(tableID, ".")
		if !ok || databaseID == "" || table == "" {
			return payloadexec.TableSchema{}, fmt.Errorf("storage-integrity genesis table %q must have <database>.<table> form", tableID)
		}
		header, err := headers.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
		if err != nil {
			return payloadexec.TableSchema{}, fmt.Errorf("read finalized ethereum header for genesis table %q: %w", tableID, err)
		}
		if header == nil {
			return payloadexec.TableSchema{}, fmt.Errorf("finalized ethereum header is nil for genesis table %q", tableID)
		}
		blockHash := header.Hash()
		callOpts := &bind.CallOpts{Context: ctx, BlockHash: blockHash}
		latest, err := caller.LatestTableSchemaVersion(callOpts, databaseID, table)
		if err != nil {
			return payloadexec.TableSchema{}, fmt.Errorf("read latest schema version for genesis table %q at block hash %s: %w", tableID, blockHash.Hex(), err)
		}
		for version := uint32(1); version <= latest; version++ {
			content, err := caller.GetTableSchema(callOpts, databaseID, table, version)
			if err != nil {
				return payloadexec.TableSchema{}, fmt.Errorf("read schema for genesis table %q version %d at block hash %s: %w", tableID, version, blockHash.Hex(), err)
			}
			var schema payloadexec.TableSchema
			if err := json.Unmarshal([]byte(content.SchemaJson), &schema); err != nil || schema.TableID != tableID {
				continue
			}
			if payloadexec.TableSchemaHash(networkID, schema) == schemaHash {
				return schema, nil
			}
		}
		return payloadexec.TableSchema{}, fmt.Errorf(
			"storage integrity genesis table %q: none of its %d declared schema versions at block hash %s hashes to its genesis schema_hash %s under network %q",
			tableID, latest, blockHash.Hex(), schemaHash, networkID,
		)
	}
}
```

- [ ] **Step 4: Run.** `go test ./standalone/ -run 'TestContractStorageIntegrityGenesisSchemas|TestRefuseRetiredStorageIntegrityGenesis'`. Expected: `ok`. `bazel run //:gazelle` leaves `BUILD.bazel` unchanged.
- [ ] **Step 5: Commit.**

```bash
git add standalone/storage_integrity_genesis.go standalone/storage_integrity_genesis_test.go
git commit -F - <<'EOF'
feat(storage-integrity): find a retired genesis table's schema by its genesis hash

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 7: Keep every genesis schema, ensure only the live genesis tables

**Files:**

- Replace: `standalone/storage_integrity_schemas.go` (whole file)
- Modify: `standalone/standalone.go:254, :260-261, :272-276, :279-285, :309, :377` (`Run`: the one-shot read after `siRegistry`, `retiredSchemas`, the schema load, `tables`, `snode.Config.Tables`, the table state's `genesis`)
- Test: `standalone/storage_integrity_schemas_test.go:41, :48-49, :52`; `standalone/storage_integrity_genesis_test.go:3-28` (imports) and append

**Interfaces:**

- Consumes: `readStorageIntegrityGenesisPlan` (Task 5), `refuseRetiredStorageIntegrityGenesis` and `contractStorageIntegrityGenesisSchemas` (Task 6); AST helpers of `standalone/storage_integrity_bootstrap_test.go` (`namedFunction`, `canonicalStorageIntegrityBindings`, `boundIdentifier`, `identifierCalls`, `identifierCall`, `assignedIdentifierForCall`, `identifierBoundTo`, `identifierExpression`, `selectorPathFromObject`, `selectorEqualsStringFromObject`, `selectorCall`, `compositeFieldValues`, `nodeContains`, `readStandaloneSource`) and `recordingStorageIntegritySchemaLoader` (`standalone/storage_integrity_schemas_test.go`).
- Produces: `func loadStorageIntegritySchemaSets(ctx context.Context, loader schemaregistry.Loader, genesis []storageIntegrityGenesisTable, retired storageIntegrityRetiredGenesisSchemas) (storageIntegritySchemaSets, error)`; `storageIntegritySchemaSets{snode, preflight []payloadexec.TableSchema; refs []schemaregistry.TableRef}` where `snode` is every genesis table in configured order and `preflight`/`refs` the ids planned `preflight`; `func storageIntegrityGenesisRef(tableID string) schemaregistry.TableRef`. In `Run`, `tables` is `schemaSets.preflight`; `snode.Config.Tables` and `storageIntegrityTableStateDeps.genesis` are `schemaSets.snode`.

- [ ] **Step 1: Write the failing tests.**

`standalone/storage_integrity_schemas_test.go` edit 1 of 3, replace:

```go
	got, err := loadStorageIntegritySchemaSets(t.Context(), loader, []string{"orders.t", "catalog.products"})
```

with:

```go
	live := []storageIntegrityGenesisTable{{id: "orders.t", preflight: true}, {id: "catalog.products", preflight: true}}
	got, err := loadStorageIntegritySchemaSets(t.Context(), loader, live, nil)
```

`standalone/storage_integrity_schemas_test.go` edit 2 of 3, replace:

```go
		"the genesis schemas keep the configured order")
	require.Equal(t, loader.refs, got.refs)
```

with:

```go
		"the genesis schemas keep the configured order")
	require.Equal(t, got.snode, got.preflight, "with the registry disabled every genesis table is preflighted")
	require.Equal(t, loader.refs, got.refs)
```

`standalone/storage_integrity_schemas_test.go` edit 3 of 3, replace:

```go
	_, err = loadStorageIntegritySchemaSets(t.Context(), loader, []string{"orders.t", "catalog.products"})
```

with:

```go
	_, err = loadStorageIntegritySchemaSets(t.Context(), loader, live, nil)
```

`standalone/storage_integrity_genesis_test.go` edit 1 of 2, replace:

```go
import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"compute-network-node/bindings"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)
```

with:

```go
import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"compute-network-node/bindings"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)
```

`standalone/storage_integrity_genesis_test.go` edit 2 of 2, append at the end of the file:

```go
func TestLoadStorageIntegritySchemaSetsSplitsTheGenesisSet(t *testing.T) {
	loader := &recordingStorageIntegritySchemaLoader{schemas: []payloadexec.TableSchema{
		{TableID: "orders.retiring"},
		{TableID: "orders.live"},
	}}
	plan := []storageIntegrityGenesisTable{
		{id: "orders.live", preflight: true},
		{id: "orders.retired", retiredSchemaHash: "0xgenesis"},
		{id: "orders.retiring"},
	}
	var asked []string
	retired := func(_ context.Context, tableID, schemaHash string) (payloadexec.TableSchema, error) {
		asked = append(asked, tableID+"@"+schemaHash)
		return payloadexec.TableSchema{TableID: tableID, PartitionBy: "from-contract"}, nil
	}

	got, err := loadStorageIntegritySchemaSets(t.Context(), loader, plan, retired)
	require.NoError(t, err)
	require.Equal(t, []schemaregistry.TableRef{
		{TableID: "orders.live", Database: "hg_unsafe", Table: "orders__live"},
		{TableID: "orders.retiring", Database: "hg_unsafe", Table: "orders__retiring"},
	}, loader.refs, "a retired id is never loaded from its latest declaration or hg_unsafe")
	require.Equal(t, []string{"orders.retired@0xgenesis"}, asked)
	require.Equal(t, []payloadexec.TableSchema{
		{TableID: "orders.live"},
		{TableID: "orders.retired", PartitionBy: "from-contract"},
		{TableID: "orders.retiring"},
	}, got.snode, "the SNode keeps every genesis table in configured order, so schema_root still matches")
	require.Equal(t, []payloadexec.TableSchema{{TableID: "orders.live"}}, got.preflight)
	require.Equal(t, []schemaregistry.TableRef{{TableID: "orders.live", Database: "hg_unsafe", Table: "orders__live"}}, got.refs)

	loader.refs = nil
	onlyRetired := []storageIntegrityGenesisTable{{id: "orders.retired", retiredSchemaHash: "0xgenesis"}}
	got, err = loadStorageIntegritySchemaSets(t.Context(), loader, onlyRetired, retired)
	require.NoError(t, err)
	require.Nil(t, loader.refs, "nothing is left to load")
	require.Len(t, got.snode, 1)
	require.Empty(t, got.preflight)
	require.Empty(t, got.refs)

	_, err = loadStorageIntegritySchemaSets(t.Context(), loader, onlyRetired, refuseRetiredStorageIntegrityGenesis)
	require.ErrorContains(t, err, "set storage_integrity.snode.schema_source to network_state")
	_, err = loadStorageIntegritySchemaSets(t.Context(), loader, onlyRetired, nil)
	require.ErrorContains(t, err, "no retired-schema source")
}

// TestRunReadsTheRegistryBeforeLoadingGenesisSchemas is design 5a §5.5's
// source-shape guard: Run reads the registry once, through the arbiter client,
// before the genesis schemas load and outside the startup transaction; the
// plan and the source-appropriate retired-schema lookup feed the one schema
// load; and the SNode and the table state keep every genesis table.
func TestRunReadsTheRegistryBeforeLoadingGenesisSchemas(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "standalone.go", readStandaloneSource(t), 0)
	require.NoError(t, err)
	runDecl := namedFunction(file, "Run")
	require.NotNil(t, runDecl)
	siBinding := boundIdentifier(canonicalStorageIntegrityBindings(runDecl)[0])

	reads := identifierCalls(runDecl, "readStorageIntegrityGenesisPlan")
	require.Len(t, reads, 1)
	require.Len(t, reads[0].Args, 4)
	require.True(t, identifierExpression(reads[0].Args[0], "ctx"))
	require.True(t, identifierExpression(reads[0].Args[1], "arbClient"), "the one-shot read goes through the follower's arbiter client")
	require.True(t, selectorPathFromObject(reads[0].Args[2], siBinding.Obj, "SNode", "TableIDs"))
	planBinding := assignedIdentifierForCall(t, runDecl, reads[0])

	loads := identifierCalls(runDecl, "loadStorageIntegritySchemaSets")
	require.Len(t, loads, 1)
	require.Len(t, loads[0].Args, 4)
	require.Less(t, int(reads[0].Pos()), int(loads[0].Pos()), "the registry is read before any genesis schema loads")
	require.True(t, identifierBoundTo(loads[0].Args[2], planBinding))
	require.True(t, identifierExpression(loads[0].Args[3], "retiredSchemas"))
	schemaSetsBinding := assignedIdentifierForCall(t, runDecl, loads[0])

	var starts []*ast.CallExpr
	ast.Inspect(runDecl.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok && selectorCall(call, "siListenerPermit", "start") {
			starts = append(starts, call)
		}
		return true
	})
	require.Len(t, starts, 1)
	require.Less(t, int(reads[0].Pos()), int(starts[0].Pos()), "the one-shot read happens before the startup transaction")

	var contractSources []*ast.CallExpr
	var networkStateBranches []*ast.IfStmt
	ast.Inspect(runDecl.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && identifierCall(call, "contractStorageIntegrityGenesisSchemas") {
			contractSources = append(contractSources, call)
		}
		if branch, ok := node.(*ast.IfStmt); ok && selectorEqualsStringFromObject(branch.Cond, siBinding.Obj, "network_state", "SNode", "SchemaSource") {
			networkStateBranches = append(networkStateBranches, branch)
		}
		return true
	})
	require.Len(t, contractSources, 1)
	require.Len(t, networkStateBranches, 1)
	require.True(t, nodeContains(networkStateBranches[0].Body, contractSources[0]),
		"only schema_source network_state finds a retired genesis schema in the contract (H5)")

	var snodeTables, tableStateGenesis []ast.Expr
	ast.Inspect(runDecl.Body, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if selector, ok := literal.Type.(*ast.SelectorExpr); ok && selector.Sel.Name == "Config" && identifierExpression(selector.X, "snode") {
			snodeTables = append(snodeTables, compositeFieldValues(literal, "Tables")...)
		}
		if identifierExpression(literal.Type, "storageIntegrityTableStateDeps") {
			tableStateGenesis = append(tableStateGenesis, compositeFieldValues(literal, "genesis")...)
		}
		return true
	})
	require.Len(t, snodeTables, 1)
	require.True(t, selectorPathFromObject(snodeTables[0], schemaSetsBinding.Obj, "snode"),
		"schema_root commits to every genesis schema, retired ones included")
	require.Len(t, tableStateGenesis, 1)
	require.True(t, selectorPathFromObject(tableStateGenesis[0], schemaSetsBinding.Obj, "snode"))
}
```

- [ ] **Step 2: Run them and verify they fail.** `go test ./standalone/ -run 'TestLoadStorageIntegritySchemaSets|TestRunReadsTheRegistryBeforeLoadingGenesisSchemas'`. Expected: FAIL to compile with `storage_integrity_genesis_test.go:337:72: too many arguments in call to loadStorageIntegritySchemaSets` (and the same for `storage_integrity_schemas_test.go`).
- [ ] **Step 3: Implement.**

Replace the whole file `standalone/storage_integrity_schemas.go`:

```go
package standalone

import (
	"context"
	"fmt"

	housegateConfig "github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
)

type storageIntegritySchemaSets struct {
	// snode is every genesis table with its genesis schema, in configured
	// order: snode.Config.Tables, whose schema_root commits to it, and the
	// table state's genesis set.
	snode []payloadexec.TableSchema
	// preflight is the subset the create-mode preflight ensures (design 5a
	// §5.4): the genesis tables whose genesis incarnation is still live and
	// neither retiring nor purged.
	preflight []payloadexec.TableSchema
	// refs are preflight's references, which the network-state cross-check
	// loads again after the preflight.
	refs []schemaregistry.TableRef
}

// loadStorageIntegritySchemaSets loads the genesis table set
// (storage_integrity.snode.table_ids) once, in its configured order. A
// retired genesis id takes its schema from retired, by the genesis
// incarnation's hash; every other id is loaded through loader as before. The
// embedded HouseGate reads every other table's schema from the arbiter
// registry through the injected table state.
func loadStorageIntegritySchemaSets(
	ctx context.Context,
	loader schemaregistry.Loader,
	genesis []storageIntegrityGenesisTable,
	retired storageIntegrityRetiredGenesisSchemas,
) (storageIntegritySchemaSets, error) {
	var loadRefs []schemaregistry.TableRef
	for _, table := range genesis {
		if !table.retired() {
			loadRefs = append(loadRefs, storageIntegrityGenesisRef(table.id))
		}
	}
	byID := make(map[string]payloadexec.TableSchema, len(genesis))
	if len(loadRefs) > 0 {
		loaded, err := loader.Load(ctx, loadRefs)
		if err != nil {
			return storageIntegritySchemaSets{}, err
		}
		for _, schema := range loaded {
			byID[schema.TableID] = schema
		}
	}
	sets := storageIntegritySchemaSets{snode: make([]payloadexec.TableSchema, 0, len(genesis))}
	for _, table := range genesis {
		if table.retired() {
			if retired == nil {
				return storageIntegritySchemaSets{}, fmt.Errorf("storage integrity genesis table %q is retired and no retired-schema source is configured", table.id)
			}
			schema, err := retired(ctx, table.id, table.retiredSchemaHash)
			if err != nil {
				return storageIntegritySchemaSets{}, err
			}
			byID[table.id] = schema
		}
		schema, ok := byID[table.id]
		if !ok {
			return storageIntegritySchemaSets{}, fmt.Errorf("storage integrity genesis table %q has no loaded schema", table.id)
		}
		sets.snode = append(sets.snode, schema)
		if table.preflight {
			sets.preflight = append(sets.preflight, schema)
			sets.refs = append(sets.refs, storageIntegrityGenesisRef(table.id))
		}
	}
	return sets, nil
}

func storageIntegrityGenesisRef(tableID string) schemaregistry.TableRef {
	return schemaregistry.TableRef{
		TableID:  tableID,
		Database: housegateConfig.StorageIntegrityUnsafeDatabase,
		Table:    housegateConfig.StorageIntegrityPhysicalTable(tableID),
	}
}
```

`standalone/standalone.go` edit 1 of 6, replace:

```go
			siRegistry := dataplane.NewRegistryFollower(arbClient, newSlogLogger(logger))
```

with:

```go
			siRegistry := dataplane.NewRegistryFollower(arbClient, newSlogLogger(logger))
			genesisPlan, err := readStorageIntegrityGenesisPlan(ctx, arbClient, si.SNode.TableIDs, newSlogLogger(logger))
			if err != nil {
				return fmt.Errorf("read the storage-integrity table registry for the genesis tables: %w", err)
			}
```

`standalone/standalone.go` edit 2 of 6, replace:

```go
				schemaSnapshot registry.TableSchemas
			)
```

with:

```go
				schemaSnapshot registry.TableSchemas
				retiredSchemas storageIntegrityRetiredGenesisSchemas = refuseRetiredStorageIntegrityGenesis
			)
```

`standalone/standalone.go` edit 3 of 6, replace:

```go
				schemaLoader = schemaregistry.NewNetworkStateLoader(
					schemaSnapshot,
					si.SNode.NetworkID,
				)
			}
```

with:

```go
				schemaLoader = schemaregistry.NewNetworkStateLoader(
					schemaSnapshot,
					si.SNode.NetworkID,
				)
				retiredSchemas = contractStorageIntegrityGenesisSchemas(
					env.EthClient,
					env.DatabasesContract,
					si.SNode.NetworkID,
				)
			}
```

`standalone/standalone.go` edit 4 of 6, replace:

```go
				schemaLoader,
				si.SNode.TableIDs,
			)
			if err != nil {
				return fmt.Errorf("derive storage-integrity schemas: %w", err)
			}
			tables := schemaSets.snode
```

with:

```go
				schemaLoader,
				genesisPlan,
				retiredSchemas,
			)
			if err != nil {
				return fmt.Errorf("derive storage-integrity schemas: %w", err)
			}
			// tables are the genesis tables the create-mode preflight ensures;
			// the SNode and the table state take every genesis table
			// (schemaSets.snode), retired ones included (design 5a §5.4).
			tables := schemaSets.preflight
```

`standalone/standalone.go` edit 5 of 6, replace:

```go
				Tables:                tables,
```

with:

```go
				Tables:                schemaSets.snode,
```

`standalone/standalone.go` edit 6 of 6, replace:

```go
				genesis:          tables,
```

with:

```go
				genesis:          schemaSets.snode,
```

- [ ] **Step 4: Run the package, audits included.** `go test ./standalone/`. Expected: `ok` — every existing source-shape audit in `storage_integrity_bootstrap_test.go` and `storage_integrity_table_state_test.go` passes unchanged. `bazel run //:gazelle` leaves `BUILD.bazel` unchanged.
- [ ] **Step 5: Commit.**

```bash
git add standalone/storage_integrity_schemas.go standalone/storage_integrity_schemas_test.go standalone/storage_integrity_genesis_test.go standalone/standalone.go
git commit -F - <<'EOF'
feat(storage-integrity): keep retired genesis schemas and ensure only live genesis tables

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 8: Skip the create-mode preflight when no genesis table is live

**Files:**

- Modify: `standalone/standalone.go:649-651, :669-671, :692, :736-738, :955-956` (`storageIntegrityBootDeps`, the two adapters' `valid`, `newStorageIntegrityBootDeps`, `prepareStorageIntegrityListenerPermit`)
- Test: `standalone/storage_integrity_genesis_test.go:3-32` (imports) and append

**Interfaces:**

- Consumes: `testStorageIntegrityBootDeps`, `recordingStorageIntegrityBootDependencies` (`standalone/storage_integrity_bootstrap_test.go`).
- Produces: `storageIntegrityBootDeps.skipPreflight bool`, set by `newStorageIntegrityBootDeps` to `len(tables) == 0`; `prepareStorageIntegrityListenerPermit` skips `runStorageIntegrityProtocolPreflight` in create mode when it is set; `ddlStorageIntegrityProtocolTables.valid()` no longer requires tables and `networkStateStorageIntegritySchemaCrossCheck.valid()` requires `len(schemaSets.snode) > 0` instead of `len(schemaSets.refs) > 0` (spec §5.4).

- [ ] **Step 1: Write the failing test.**

`standalone/storage_integrity_genesis_test.go` edit 1 of 2, replace:

```go
import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"compute-network-node/bindings"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)
```

with:

```go
import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"compute-network-node/bindings"
	"compute-network-node/config"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)
```

`standalone/storage_integrity_genesis_test.go` edit 2 of 2, append at the end of the file:

```go
// With every genesis table retiring or retired the create-mode preflight has
// nothing to ensure and is skipped; the role lifecycle still runs.
func TestPrepareStorageIntegrityListenerPermitSkipsAnEmptyPreflight(t *testing.T) {
	recorder := &recordingStorageIntegrityBootDependencies{}
	deps := testStorageIntegrityBootDeps(ddl.ModeCreateAndVerify, recorder)
	deps.skipPreflight = true
	permit, err := prepareStorageIntegrityListenerPermit(t.Context(), config.StorageIntegrityConfig{
		Enabled: true,
		SNode:   config.StorageIntegritySNode{SchemaSource: "network_state"},
	}, deps)
	require.NoError(t, err)
	require.Empty(t, recorder.calls, "no ensure and no cross-check")
	runtime, err := permit.start(t.Context(), func(*startupTransaction) error { return nil })
	require.NoError(t, err)
	runtime.stopAndWait()
	require.Equal(t, []string{"prepare", "run-ready", "register"}, recorder.calls)

	require.True(t, newStorageIntegrityBootDeps(nil, ddl.Pinned{}, nil, ddl.ModeCreateAndVerify, nil, storageIntegritySchemaSets{}, "", nil, nil).skipPreflight,
		"no preflight tables: skip")
	require.False(t, newStorageIntegrityBootDeps(nil, ddl.Pinned{}, []payloadexec.TableSchema{{TableID: "orders.t"}}, ddl.ModeCreateAndVerify, nil, storageIntegritySchemaSets{}, "", nil, nil).skipPreflight)
}
```

- [ ] **Step 2: Run it and verify it fails.** `go test ./standalone/ -run TestPrepareStorageIntegrityListenerPermitSkipsAnEmptyPreflight`. Expected: FAIL to compile with `storage_integrity_genesis_test.go:451:7: deps.skipPreflight undefined (type *storageIntegrityBootDeps has no field or method skipPreflight)`.
- [ ] **Step 3: Implement.**

`standalone/standalone.go` edit 1 of 5, replace:

```go
type storageIntegrityBootDeps struct {
	mode             ddl.Mode
	protocolTables   storageIntegrityProtocolTables
```

with:

```go
type storageIntegrityBootDeps struct {
	mode ddl.Mode
	// skipPreflight is set when no genesis table takes part in the
	// create-mode preflight: every one is retiring or retired, and the
	// reconciler owns their tables (design 5a §5.4).
	skipPreflight    bool
	protocolTables   storageIntegrityProtocolTables
```

`standalone/standalone.go` edit 2 of 5, replace:

```go
		d.mode == ddl.ModeCreateAndVerify &&
		len(d.tables) > 0 &&
		d.pinned.UnsafeDB != "" &&
```

with:

```go
		d.mode == ddl.ModeCreateAndVerify &&
		d.pinned.UnsafeDB != "" &&
```

`standalone/standalone.go` edit 3 of 5, replace:

```go
		len(c.schemaSets.refs) > 0 &&
```

with:

```go
		len(c.schemaSets.snode) > 0 &&
```

`standalone/standalone.go` edit 4 of 5, replace:

```go
	return &storageIntegrityBootDeps{
		mode: mode,
		protocolTables: &ddlStorageIntegrityProtocolTables{
```

with:

```go
	return &storageIntegrityBootDeps{
		mode:          mode,
		skipPreflight: len(tables) == 0,
		protocolTables: &ddlStorageIntegrityProtocolTables{
```

`standalone/standalone.go` edit 5 of 5, replace:

```go
		case ddl.ModeCreateAndVerify:
			if err := runStorageIntegrityProtocolPreflight(ctx, deps); err != nil {
```

with:

```go
		case ddl.ModeCreateAndVerify:
			if deps.skipPreflight {
				break
			}
			if err := runStorageIntegrityProtocolPreflight(ctx, deps); err != nil {
```

- [ ] **Step 4: Run the package.** `go test ./standalone/`. Expected: `ok`. `bazel run //:gazelle` leaves `BUILD.bazel` unchanged.
- [ ] **Step 5: Commit.**

```bash
git add standalone/standalone.go standalone/storage_integrity_genesis_test.go
git commit -F - <<'EOF'
feat(storage-integrity): skip the create-mode preflight when no genesis table is live

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 9: Acceptance — restart after a genesis table was purged and recreated

**Files:**

- Create: `standalone/storage_integrity_genesis_ch_test.go`
- Modify: `standalone/BUILD.bazel` (gazelle), `.github/workflows/ci.yml:132, :140, :156-157`

**Interfaces:**

- Consumes: everything above; `snode.New(snode.Config, snode.Deps) (*snode.Role, error)`; test helpers `scriptedTableRegistry`, `scriptedArbiterClient`, `genesisRegistrySnapshot` (Task 4), `versionedDatabasesCaller`, `contractSchemaVersion` (Task 6), `fakeStorageIntegrityHeaderReader`, `requireStorageIntegrityCHAcceptance`, `openStorageIntegrityAcceptanceConn`, `quoteSmokeIdentifier`.
- Produces: `TestStorageIntegrityGenesisRestartAfterPurge` (S6: the storage-integrity prefix of `Run` up to the listener permit, restarted five times).

- [ ] **Step 1: Write the acceptance test.**

Create `standalone/storage_integrity_genesis_ch_test.go`:

```go
package standalone

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"compute-network-node/bindings"
	"compute-network-node/config"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/snode"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestStorageIntegrityGenesisRestartAfterPurge is design 5a's G2 acceptance.
// It restarts the storage-integrity half of standalone.Run up to the listener
// permit — the one-shot registry read, the contract snapshot at the finalized
// header, the genesis schema load, snode.New and the create-mode preflight —
// against a Keeper-enabled ClickHouse, with a scripted arbiter registry and a
// scripted Databases contract, while a genesis table is retired, purged and
// recreated with a different schema. Every restart must succeed with the
// configured schema_root, and once the genesis incarnation retires the
// preflight must not recreate its hg_* tables. The rest of Run (listeners,
// syncer, HouseGate) needs a live chain and arbiter and does not read the
// genesis split.
//
// Requires SENTIO_SI_CH_E2E=1 and CH_ADDR.
func TestStorageIntegrityGenesisRestartAfterPurge(t *testing.T) {
	addr := requireStorageIntegrityCHAcceptance(t)
	conn := openStorageIntegrityAcceptanceConn(t, addr)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	require.NoError(t, conn.Ping(ctx))

	const networkID = "sentio-node-si-genesis"
	table := fmt.Sprintf("si_genesis_%d", time.Now().UnixNano())
	logicalID := "default." + table
	genesis := payloadexec.TableSchema{TableID: logicalID, PartitionBy: "partition", Columns: []lthash.Column{
		{Name: "partition", Type: "String"},
		{Name: "value", Type: "UInt64"},
	}}
	recreated := payloadexec.TableSchema{TableID: logicalID, PartitionBy: "partition", Columns: []lthash.Column{
		{Name: "partition", Type: "String"},
		{Name: "value", Type: "String"},
	}}
	genesisHash := payloadexec.TableSchemaHash(networkID, genesis)
	schemaRoot := payloadexec.SchemaRoot(networkID, []payloadexec.TableSchema{genesis})
	pinned := ddl.Pinned{
		UnsafeDB: config.StorageIntegrityUnsafeDatabase, SafeDB: config.StorageIntegritySafeDatabase,
		PromoteDB: config.StorageIntegrityPromoteDatabase, NodeID: "si-genesis-node", KeeperShardID: 0,
	}
	physical := ddl.CHTableName(logicalID)
	protocolTables := func() uint64 {
		var n uint64
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE name = ? AND database IN (?, ?, ?)",
			physical, pinned.UnsafeDB, pinned.SafeDB, pinned.PromoteDB,
		).Scan(&n))
		return n
	}
	dropProtocolTables := func() {
		for _, db := range []string{pinned.PromoteDB, pinned.SafeDB, pinned.UnsafeDB} {
			require.NoError(t, conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s.%s SYNC",
				quoteSmokeIdentifier(db), quoteSmokeIdentifier(physical))))
		}
	}
	t.Cleanup(dropProtocolTables)

	registry := &scriptedTableRegistry{err: status.Error(codes.FailedPrecondition, dataplane.TableRegistryDisabledMessage)}
	arbClient := scriptedArbiterClient(t, registry)
	headers := &fakeStorageIntegrityHeaderReader{header: &types.Header{Number: big.NewInt(42)}}
	contract := &versionedDatabasesCaller{versions: []bindings.TypesTableSchema{contractSchemaVersion(t, networkID, genesis)}}
	genesisInc := func(status pb.TableIncarnationStatus) *pb.TableIncarnation {
		return &pb.TableIncarnation{DatabaseId: "default", TableId: table, SchemaHash: genesisHash,
			Origin: pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_GENESIS, Status: status}
	}

	// restart runs Run's storage-integrity startup up to the listener permit.
	restart := func(step string) storageIntegritySchemaSets {
		t.Helper()
		plan, err := readStorageIntegrityGenesisPlan(ctx, arbClient, []string{logicalID}, slog.Default())
		require.NoError(t, err, step)
		snapshot, err := loadStorageIntegrityContractSnapshot(ctx, headers, contract, []string{logicalID})
		require.NoError(t, err, step)
		sets, err := loadStorageIntegritySchemaSets(ctx, schemaregistry.NewNetworkStateLoader(snapshot, networkID), plan,
			contractStorageIntegrityGenesisSchemas(headers, contract, networkID))
		require.NoError(t, err, step)
		role, err := snode.New(snode.Config{
			NodeID:             pinned.NodeID,
			NetworkID:          networkID,
			SchemaSnapshotID:   "si-genesis",
			ExecutorProfileID:  "housegate-replay-mvp-v0",
			SchemaRoot:         schemaRoot,
			Tables:             sets.snode,
			StateDir:           t.TempDir(),
			UnsafeDatabase:     pinned.UnsafeDB,
			SafeDatabase:       pinned.SafeDB,
			PromoteDatabase:    pinned.PromoteDB,
			AuthorityAddresses: []string{"0x1234"},
			SchemaSource:       ddl.SchemaSourceNetworkState,
		}, snode.Deps{Client: arbClient, Registry: dataplane.NewRegistryFollower(arbClient, slog.Default()), Conn: conn, Logger: slog.Default()})
		require.NoError(t, err, "%s: the SNode accepts the configured schema_root", step)
		bootDeps := newStorageIntegrityBootDeps(conn, pinned, sets.preflight, ddl.ModeCreateAndVerify, snapshot, sets, networkID, role, slog.Default())
		_, err = prepareStorageIntegrityListenerPermit(ctx, config.StorageIntegrityConfig{
			Enabled: true,
			SNode:   config.StorageIntegritySNode{SchemaSource: "network_state"},
		}, bootDeps)
		require.NoError(t, err, step)
		require.Equal(t, []payloadexec.TableSchema{genesis}, sets.snode, "%s: the SNode keeps the genesis schema", step)
		require.Equal(t, schemaRoot, payloadexec.SchemaRoot(networkID, sets.snode), step)
		return sets
	}

	// 1. The registry is disabled: the preflight creates the genesis tables.
	restart("registry disabled")
	require.Equal(t, uint64(3), protocolTables())

	// 2. Enabled, the genesis incarnation Active: the preflight verifies them.
	registry.set(genesisRegistrySnapshot(genesisInc(pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_ACTIVE)), nil)
	sets := restart("genesis active")
	require.Len(t, sets.preflight, 1)
	require.Equal(t, uint64(3), protocolTables())

	// 3. Retiring, then Purging: the reconciler drops the tables; restarts must
	// not recreate them.
	registry.set(genesisRegistrySnapshot(genesisInc(pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_RETIRING)), nil)
	sets = restart("genesis retiring")
	require.Empty(t, sets.preflight)
	dropProtocolTables()
	registry.set(genesisRegistrySnapshot(genesisInc(pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PURGING)), nil)
	restart("genesis purging")
	require.Zero(t, protocolTables(), "a purging genesis table's hg_* tables are not recreated")

	// 4. Purged: the schema comes from the contract by hash.
	registry.set(genesisRegistrySnapshot(genesisInc(pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PURGED)), nil)
	restart("genesis purged")
	require.Zero(t, protocolTables(), "a purged genesis table's hg_* tables are not recreated")

	// 5. The name is recreated with a different schema: the latest declaration
	// is the new one, yet the SNode keeps the genesis schema and schema_root.
	recreatedJSON, err := json.Marshal(recreated)
	require.NoError(t, err)
	contract.versions = append(contract.versions, contractSchemaVersion(t, networkID, recreated))
	registry.set(genesisRegistrySnapshot(
		genesisInc(pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PURGED),
		&pb.TableIncarnation{DatabaseId: "default", TableId: table,
			SchemaHash: payloadexec.TableSchemaHash(networkID, recreated), SchemaJson: string(recreatedJSON),
			Origin: pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_CHAIN,
			Status: pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PENDING},
	), nil)
	restart("genesis recreated")
	require.Zero(t, protocolTables(), "the preflight never creates the recreated table's hg_* tables either; the reconciler does")
}
```

- [ ] **Step 2: Run it against ClickHouse.** `bazel run //:gazelle` (it adds the file to `standalone_test`), then, with the acceptance environment: `go test ./standalone/ -run TestStorageIntegrityGenesisRestartAfterPurge -count=1 -v`. Expected: `--- PASS: TestStorageIntegrityGenesisRestartAfterPurge`. With `planStorageIntegrityGenesis` forced to "every id live" (today's behaviour) the test fails at step 3 (`Should be empty` for the preflight set). Steps 4 and 5 guard the two failures spec §2 measured on `main`: the preflight recreating a purged genesis table's `hg_*` tables, and a recreated name's latest declaration changing `schema_root` so `snode.New` refuses.
- [ ] **Step 3: Add it to CI.**

`.github/workflows/ci.yml` edit 1 of 3, replace:

```yaml
      - name: storage-integrity acceptance (Spec L D1/D6, dynamic table set)
```

with:

```yaml
      - name: storage-integrity acceptance (Spec L D1/D6, dynamic table set, host gates)
```

`.github/workflows/ci.yml` edit 2 of 3, replace:

```yaml
            --test_filter='TestStorageIntegrityMalformedColumnTypeCreatesNoTable|TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession|TestStorageIntegrityDynamicTableLifecycle|TestStorageIntegrityDynamicProcessorTableIsOrdinary' \
```

with:

```yaml
            --test_filter='TestStorageIntegrityMalformedColumnTypeCreatesNoTable|TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession|TestStorageIntegrityDynamicTableLifecycle|TestStorageIntegrityDynamicProcessorTableIsOrdinary|TestStorageIntegrityGenesisRestartAfterPurge' \
```

`.github/workflows/ci.yml` edit 3 of 3, replace:

```yaml
          if [ "${passes}" -ne 4 ]; then
            echo "expected 4 storage-integrity acceptance PASS markers, saw ${passes}"
```

with:

```yaml
          if [ "${passes}" -ne 5 ]; then
            echo "expected 5 storage-integrity acceptance PASS markers, saw ${passes}"
```

- [ ] **Step 4: Verify.** `go vet ./standalone/`. Expected: no output.
- [ ] **Step 5: Commit.**

```bash
git add standalone/storage_integrity_genesis_ch_test.go standalone/BUILD.bazel .github/workflows/ci.yml
git commit -F - <<'EOF'
test(storage-integrity): acceptance for a restart after a genesis table was purged and recreated

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 10: Document processor databases and retired genesis tables

**Files:**

- Modify: `README.md:88, :102-103, :106`

**Interfaces:** none (operator documentation of H1, H2, H5 and spec §5).

- [ ] **Step 1: Edit the README.**

`README.md` edit 1 of 3, replace:

```markdown
`storage_integrity.snode.table_ids` is the genesis table set: the tables served while the arbiter's table registry is disabled, and the only ones whose schemas startup loads.
```

with:

```markdown
`storage_integrity.snode.table_ids` is the genesis table set: the tables served while the arbiter's table registry is disabled, and the only ones whose schemas startup loads. Before loading them, startup reads the arbiter's table registry once (a disabled registry, or an arbiter without it, leaves every genesis table as before). A genesis table whose genesis incarnation is Retiring or Purging is loaded as before but not ensured by the create-mode preflight, because the reconciler is dropping its `hg_*` tables. A retired genesis table — its genesis incarnation is Purged, or its name was recreated — keeps its genesis schema, so `schema_root` never changes: `network_state` finds it among the table's `Databases` declarations by the genesis incarnation's `schema_hash`, and the preflight skips it. `schema_source: clickhouse` has no durable copy of a dropped table's schema and refuses startup naming the table, so move a `clickhouse` deployment to `network_state` before dropping any genesis table.
```

`README.md` edit 2 of 3, replace:

```markdown
- **Ordinary** while the registry is disabled (only the genesis set is Active), for databases on other indexers or unknown to the chain state, for Legacy incarnations, and, before the seed commits, for tables whose `TableCreated` block precedes `activation_block` (zero, the value of tables created before the upgrade, included);
- **Pending** for a table the registry does not record (default deny, including a Purged name), for a Pending incarnation, and for an Active incarnation until this node's reconciler reports its three `hg_*` tables ready or when its registry schema fails the `schema_hash` self-check;
```

with:

```markdown
- **Ordinary** while the registry is disabled (only the genesis set is Active), for databases on other indexers or unknown to the chain state, for tables of PROCESSOR databases that the registry does not record, for Legacy incarnations, and, before the seed commits, for tables whose `TableCreated` block precedes `activation_block` (zero, the value of tables created before the upgrade, included);
- **Pending** for a table of any other database on the SI indexer that the registry does not record (default deny, including a Purged name), for a Pending incarnation, and for an Active incarnation until this node's reconciler reports its three `hg_*` tables ready or when its registry schema fails the `schema_hash` self-check;
```

`README.md` edit 3 of 3, replace:

```markdown
The storage-node JSON-RPC serves the same answer as
```

with:

```markdown
A processor's own database (`<processorId>_<replica>`) is never governed: the driver creates its tables without declaring a schema, so they stay Ordinary and processor indexing behaves as before the registry was enabled. Declaring a schema for such a table brings it into the registry like any other table; from then on it is answered from the registry, and the driver's unsigned writes to it are refused.

The storage-node JSON-RPC serves the same answer as
```

- [ ] **Step 2: Check.** Each replaced block occurs exactly once before the edit (`grep -c` on a distinctive phrase of each returns `1`), and the file has no hard-wrapped paragraph.
- [ ] **Step 3: Commit.**

```bash
git add README.md
git commit -F - <<'EOF'
docs: processor databases and retired genesis tables under the table registry

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 11: Verify, open the PR and merge (controller-only, gated)

**Files:** none.

- [ ] **Step 1: Full verification on the branch.** `gofmt -l $(git ls-files '*.go')` (empty), `go vet ./... && go test -race ./...`, `bazel run //:gazelle` (no diff), `bazel build //... && bazel test //...`, then with the acceptance environment the CI job's two storage-integrity steps: `bazel test //standalone:standalone_test --test_filter='TestStorageIntegrityProtocolTableDriftFailsBootstrap' --test_env=SENTIO_SI_CH_E2E --test_env=CH_ADDR --nocache_test_results --test_arg=-test.v --test_output=all` (one PASS) and `bazel test //standalone:standalone_test --test_filter='TestStorageIntegrityMalformedColumnTypeCreatesNoTable|TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession|TestStorageIntegrityDynamicTableLifecycle|TestStorageIntegrityDynamicProcessorTableIsOrdinary|TestStorageIntegrityGenesisRestartAfterPurge' --test_env=SENTIO_SI_CH_E2E --test_env=CH_ADDR --nocache_test_results --test_arg=-test.v --test_timeout=900 --test_output=all` (five `--- PASS: TestStorageIntegrity` markers). Record the results in the PR.
- [ ] **Step 2: Baseline rule.** Any failing target is compared with a clean `origin/main` build by checking out only the changed paths (`git checkout origin/main -- <paths>`), never `git stash`; a matching failure set is not a regression.
- [ ] **Step 3: PR.** Push `feat/si-host-gates` and open a PR against `main` with the rulings S1–S10, the verification results and the handoff below. End the description with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
- [ ] **Step 4: Merge.** Squash-merge after review and a green CI, including the `integration-clickhouse` job's five acceptance PASS markers and its drift marker. Record the merge commit's full SHA; 5b pins its image.

---

## Spec coverage

| Spec item | Where |
|---|---|
| §3 H1, §4.1–4.2 processor databases ungoverned, type carried from `DbType` | Task 1 (`ChainDatabase.Processor`, conversion), Task 2 (governed set) |
| §3 H2 registry records still win | Task 2 (`declared_*` rows), Task 10 (README) |
| §4.3 compensation unchanged | S7 (no change; `compensation.go` already skips non-user databases) |
| §4.4 tests: matrix rows, conversion, G1 acceptance | Tasks 1, 2, 3 |
| §3 H4, §5.1 one-shot read: `GetTableRegistry` via `WithLeaderRetry`, disabled/`Unimplemented`, decode and transport refusal, `DefaultRegistryStartupTimeout` | Task 4, S2 |
| §5.2 split table, missing genesis incarnation refuses | Task 5 |
| §3 H3, §5.3 by-hash search; the Purged `schema_hash` check | Task 6, S1, S4 |
| §3 H5 `clickhouse` refuses retired genesis ids | Task 6 (`refuseRetiredStorageIntegrityGenesis`), Task 7 (default in `Run`), Task 10 |
| §5.4 what each set feeds; empty preflight skipped, non-empty requirement on the full set | Task 7 (`snode`, `preflight`, `refs`, `Run`), Task 8 |
| §5.5 unit tests, source shape, G2 acceptance | Tasks 4–8, Task 7 `TestRunReadsTheRegistryBeforeLoadingGenesisSchemas`, Task 9 |
| §6 one PR, CI image for 5b, inert when disabled, arbiter-verifier check, README note | Task 11, "Handoff to sub-project 5b", S5, Task 10 |

## Verification record

How the code in this plan was verified before it was written down (2026-09-26, macOS arm64, Go 1.27.1, Bazel 9.1.0, Docker via OrbStack, ClickHouse 25.8.32.4 with Keeper from `scripts/ci/clickhouse-keeper.xml`, container `sp5a-ch`):

- **Prototype.** A throwaway worktree of sentio-node `origin/main` `46ea7de` held the complete change; `go vet`, `go test -race ./standalone/ ./storageintegrityadapter/...` (with the acceptance environment), `bazel test //...` and the CI acceptance filter all passed there. Mutation checks: with Task 2's `snapshot.go` edit reverted, `TestLookupMatrix` failed on the processor rows and `TestStorageIntegrityDynamicProcessorTableIsOrdinary` failed with `expected: "ordinary"`, `actual: "pending"` (and, with that assertion removed, on the proxy's `DB::Exception (733): … is pending activation (retryable)`); with `planStorageIntegrityGenesis` forced to "every id live", `TestStorageIntegrityGenesisRestartAfterPurge` failed at step 3.
- **Task by task.** A script applied exactly the edit blocks above, in order, to a fresh worktree of `origin/main` `46ea7de` on branch `feat/si-host-gates`: each "verify it fails" command failed with the text named in its Expected line, each run step passed, `gofmt -l` stayed empty, gazelle changed `standalone/BUILD.bazel` only in Tasks 4 and 9, and each task was committed with its message. The docker-bound runs in Tasks 3 and 9 were part of the script. Every file of the replayed branch is byte-identical to the prototype's.
- **Final branch.** `go vet ./...` clean; `go test -race ./...` passed; `bazel run //:gazelle` produced no diff; `bazel build //...` and `bazel test //...` passed (15 of 15 targets); the CI acceptance filter produced five `--- PASS: TestStorageIntegrity` markers (`MalformedColumnTypeCreatesNoTable`, `BackpressureKeepsTheEmbeddedProxySession`, `DynamicTableLifecycle`, `DynamicProcessorTableIsOrdinary`, `GenesisRestartAfterPurge`) and `TestStorageIntegrityProtocolTableDriftFailsBootstrap` passed.
- **Not run locally.** `standalone.Run` end to end (`TestStorageIntegritySmoke` needs a live chain and arbiter); the real arbiter answering `GetTableRegistry` (the tests use in-process gRPC servers speaking arbiter-proto `v0.8.0`, and S1/S2 cite the arbiter and arbiter-core sources); the GitHub Actions CI run.
- **Measured facts behind the rulings.** S1–S5 cite arbiter `10ab917`, arbiter-core `v0.10.1` and compute-network-contracts `a834736` by file and line. `TableIncarnation` in a Purged status still carries `SchemaHash` in the restore validator and the read API. The one-shot read's `Unimplemented` case was exercised against a gRPC server with no `TableRegistry` service registered, and the redirect case against a FAILED_PRECONDITION carrying a `pb.NotLeader` detail and the disabled message.

## Handoff to sub-project 5b

- **Image.** Pin the `main` CI image of this plan's squash-merge commit: `ghcr.io/sentioxyz/sentio-node:sha-<full merge SHA>` (`.github/workflows/ci.yml` `docker-push`, `type=sha,format=long`), by digest (`docker buildx imagetools inspect ghcr.io/sentioxyz/sentio-node:sha-<full merge SHA>`). It contains `46ea7de`, so it also carries arbiter-core `v0.10.1` (256 MiB data-plane receive limit, the plan A carry-over) and supersedes the `9788949` image named in the sub-project 5 context notes. Gate R4 is unchanged: deploy it together with the in-pod `housegate-rewriter:0.15.0`.
- **Behaviour with the registry disabled (the first sync).** None visible, apart from one extra `GetTableRegistry` call before the genesis schemas load. devnet2's current arbiter (`v0.7.1`) has no `TableRegistry` service and answers `Unimplemented`, which reads as disabled, so this image can also precede the arbiter upgrade. An unreachable arbiter now fails startup before the listeners, after the same 2-minute budget as before, with `read the storage-integrity table registry for the genesis tables: get table registry: context deadline exceeded`.
- **Processor tables (G1 cleared).** Tables in a processor's own database are Ordinary with the registry enabled; `sentio_node_storage_integrity_tables{status}` no longer counts them. Declaring a schema for a processor table brings it into SI and the driver's unsigned writes to it are then refused (H2): do not declare processor tables.
- **Dropping a genesis table (G2 cleared, with conditions).**
  - `schema_source: clickhouse` refuses startup once any genesis table is retired, with `storage integrity genesis table "<id>" was dropped (…) and schema_source "clickhouse" cannot recover a dropped table's schema: set storage_integrity.snode.schema_source to network_state`. devnet2's indexer-a already uses `network_state`.
  - Under `network_state` a retired genesis table's schema comes from the `Databases` contract's declaration history, which is never deleted; the node needs a finalized-header RPC that serves `getTableSchema` for old versions (the same RPC the contract snapshot already uses).
  - Do not drop a genesis table until every SI indexer node runs this image, and do not roll a node back past it afterwards: an older image fails that restart (`clickhouse`) or recreates the purged `hg_*` tables and, after a recreation, fails `schema_root` (`network_state`).
  - The contract snapshot still requires every genesis id's latest declaration to be well formed (S9); a recreated genesis name gets its declaration from the node's own schema declarer.
  - Log lines to expect after a drop: `storage integrity genesis table is retiring: its protocol tables are not ensured` and `… is retired: its genesis schema is found by hash and its protocol tables are not ensured`.
- **arbiter-verifier.** No change needed (S5). A verifier configured with `table_ids` instead of inline `tables` would not restart after a genesis purge; keep devnet2's verifiers on inline schemas.
- **CI.** The `integration-clickhouse` job now expects five storage-integrity acceptance PASS markers.
