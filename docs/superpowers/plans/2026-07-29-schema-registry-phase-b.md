# Schema Registry Phase B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement [the Phase B spec](../specs/2026-07-29-schema-registry-phase-b-design.md): schemas declared once on-chain (`setTableSchema` + `TableSchemaSet`), mirrored through the network-state pipeline into a new `TableSchemas` collection, consumed via a hash-verifying `NetworkStateLoader` behind the Phase-A seam, with automatic declaration on the CREATE TABLE path and the ClickHouse introspection demoted to a cross-check.

**Architecture:** Seven tasks across four repos following the spec's §10 order: contract first (Task 1), then three parallelizable middles (sentio-core collection Task 2, sentio-node syncer leg Task 3, housegate interface/loader/hook Task 4), then the sentio-node declaration leg (Task 5) and read leg + consumer switch (Task 6), then E2E + rollout docs (Task 7). Every consumer keeps anchoring to the genesis `schema_root`; network state is delivery only.

**Tech Stack:** Solidity 0.8.24 + Foundry + UUPS (compute-network-contracts); Go 1.26 across sentio-core / sentio-node (Bazel) / housegate (Bazel); statemirror Redis hashes; abigen bindings (manually copied downstream).

**Repos & branches:**
- compute-network-contracts `/Users/uranuswch/Dev/sentio_xyz/compute-network-contracts` — `feat/table-schema-registry` off main
- sentio-core `/Users/uranuswch/Dev/sentio_xyz/sentio-core` — `feat/table-schemas-collection` off main
- sentio-node `/Users/uranuswch/Dev/sentio_xyz/sentio-node` — `feat/schema-registry-phase-b` off main (Tasks 3, 5, 6 stack on one branch)
- housegate `/Users/uranuswch/Dev/housegate/housegate` — `feat/schema-registry-network-loader` off main

## Global Constraints

- Parent invariant verbatim: network state is the schema's **availability** layer, never its **integrity** layer — every consumer re-hashes content against a consensus-anchored commitment; any failure refuses startup.
- Hash domains reused, never invented: per-table = `payloadexec.TableSchemaHash(networkID, schema)`, set = `payloadexec.SchemaRoot` (housegate `pkg/replay/payloadexec/exports.go:14-21`).
- Column truth comes from ClickHouse materialization only — declaration reads `system.columns` via the Phase-A `ClickHouseLoader` after DDL success; SQL-text parsing is forbidden anywhere in this plan.
- `schemaJson` canonical form: `{"table_id":…,"partition_by":…,"columns":[{"name":…,"type":…}…]}` in ClickHouse DDL column order (exactly `payloadexec.TableSchema`'s JSON encoding).
- Version allocation is contract-owned (per-table cursor, spec decision 6 — supersedes the parent's caller-supplied version); cursors never reset across delete/recreate; history is never cleared.
- Composite state key is the flat string `<databaseId>/<tableId>@<version>`; ids containing `/` or `@` are rejected at declaration time; `PlainState` stores flat `map[string]TableSchemaInfo` with identity codecs (spec decision 2).
- New statemirror collections must wire BOTH mirror paths: `ReplaceInner`'s batch diff (the syncer main loop) AND `SyncMirror`'s full push (spec decision 7) — plus `Clone`, file store, postgres store.
- housegate's content lookup is a standalone `registry.TableSchemas` interface — never a method on the `Registry` union (spec decision 3; convention `pkg/registry/metadata.go:6-19`).
- The commitgate hook is an optional marker interface (`RouteAware` mechanics); existing observers compile untouched; dispatch is best-effort and must never block the relay byte path.
- Consumer switch is config-gated: `schema_source: clickhouse | network_state`, default `clickhouse` (spec decision 9).
- Contract storage: new mappings appended AFTER `_permissionAccountsByDb` (`Databases.sol:46`), never inserted; the repo's UUPS upgrade-safety reviewer applies.
- Bindings sync is manual: regenerate in contracts, copy the whole file into sentio-node (currently drifted 31 lines — the copy realigns).
- Bazel repos (housegate, sentio-node): `go mod tidy && bazel mod tidy && bazel run //:gazelle` after dep/package changes; housegate docker tests join `//pkg/integration:integration_test`.
- English comments/logs; `fmt.Errorf("context: %w", err)`.

---

## File map

```
contracts    src/interfaces/IDatabases.sol      event/fn/view/errors
             src/Databases.sol                  storage (appended) + setTableSchema/getters
             src/libraries/Types.sol            struct TableSchema
             test/DatabasesSchema.t.sol         new suite
             bindings/bindings.go               regenerated (gen_bindings.sh)
sentio-core  common/statemirror/on_chain_mapping_constants.go   MappingTableSchemas
             network/state/types.go             TableSchemaInfo + key fns
             network/state/state.go             PlainState field + iface + methods + Clone
             network/state/state_mirrored.go    codec + ReplaceInner + SyncMirror + upsert
             network/state/store_file.go        Save/Load
             network/state/store_postgres.go    TableSchemaRow + migrate + Save/Load
sentio-node  bindings/bindings.go               verbatim copy from contracts
             handlers/database_event.go         ParseTableSchemaSet + onTableSchemaSet
             database_registry/observer.go      observerContract.setTableSchema + hook impl
             commands/declare_table_schemas.go  backfill/repair command
             standalone/networkstate/{redis,networkstate,convert}.go   read leg
             config/config.go + standalone/standalone.go               schema_source switch
housegate    pkg/registry/schemas.go            TableSchema + TableSchemas interface
             pkg/schemaregistry/networkstate_loader.go   NetworkStateLoader + cross-check
             pkg/plugins/commitgate/{observer,plugin}.go AfterStatementSuccess hook
             pkg/network/{types,inmemory,yaml}.go        fixture mirror
             configs/local.network_state.yaml            sample block
```

---

### Task 1: Contract — `setTableSchema` + `TableSchemaSet` (compute-network-contracts)

**Files:**
- Modify: `src/interfaces/IDatabases.sol`, `src/Databases.sol`, `src/libraries/Types.sol`
- Create: `test/DatabasesSchema.t.sol`
- Regenerate: `bindings/bindings.go` (`./gen_bindings.sh`)

**Interfaces:**
- Produces (Tasks 3/5 consume via bindings): event `TableSchemaSet(string databaseId, string tableId, uint32 version, bytes32 schemaHash, string schemaJson)`; `setTableSchema(string calldata databaseId, address caller, string calldata tableId, bytes32 schemaHash, string calldata schemaJson) external returns (uint32)`; views `getTableSchema(string calldata databaseId, string calldata tableId, uint32 version) external view returns (Types.TableSchema memory)` and `latestTableSchemaVersion(string calldata databaseId, string calldata tableId) external view returns (uint32)` (0 = never declared); errors `SchemaHashEmpty()`, `SchemaJsonEmpty()`.

- [ ] **Step 1: Write the failing tests**

`test/DatabasesSchema.t.sol` on the `Databases.t.sol` skeleton — copy its `setUp` (`test/Databases.t.sol:39-71`), constants block, and `_registerIndexer`/prank conventions; then:

```solidity
contract DatabasesSchemaTest is Test, NetworkEnv {
    // constants + setUp copied per Databases.t.sol:15-71, plus one created
    // database DATABASE_ID and one created table TABLE_ID owned by
    // INDEXER_SIGNER with CALLER holding Write permission.

    bytes32 constant HASH_V1 = keccak256("schema-v1");
    string constant JSON_V1 = "{\"table_id\":\"table_1\",\"partition_by\":\"p\",\"columns\":[{\"name\":\"p\",\"type\":\"String\"}]}";

    function test_setTableSchema_allocatesVersionsAndEmits() public {
        vm.prank(INDEXER_SIGNER);
        vm.expectEmit(false, false, false, true);
        emit IDatabases.TableSchemaSet(DATABASE_ID, TABLE_ID, 1, HASH_V1, JSON_V1);
        uint32 v1 = databases.setTableSchema(DATABASE_ID, CALLER, TABLE_ID, HASH_V1, JSON_V1);
        assertEq(v1, 1);
        vm.prank(INDEXER_SIGNER);
        uint32 v2 = databases.setTableSchema(DATABASE_ID, CALLER, TABLE_ID, keccak256("schema-v2"), JSON_V1);
        assertEq(v2, 2);
        assertEq(databases.latestTableSchemaVersion(DATABASE_ID, TABLE_ID), 2);
        Types.TableSchema memory s = databases.getTableSchema(DATABASE_ID, TABLE_ID, 1);
        assertEq(s.schemaHash, HASH_V1);
        assertEq(s.schemaJson, JSON_V1);
    }

    function test_setTableSchema_cursorSurvivesDeleteRecreate() public {
        vm.prank(INDEXER_SIGNER);
        databases.setTableSchema(DATABASE_ID, CALLER, TABLE_ID, HASH_V1, JSON_V1); // v1
        vm.prank(INDEXER_SIGNER);
        databases.deleteTable(DATABASE_ID, CALLER, TABLE_ID);
        vm.prank(INDEXER_SIGNER);
        databases.createTable(DATABASE_ID, CALLER, TABLE_ID, TABLE_TYPE);
        vm.prank(INDEXER_SIGNER);
        uint32 v = databases.setTableSchema(DATABASE_ID, CALLER, TABLE_ID, keccak256("post-recreate"), JSON_V1);
        assertEq(v, 2); // cursor did NOT reset
        assertEq(databases.getTableSchema(DATABASE_ID, TABLE_ID, 1).schemaHash, HASH_V1); // history intact
    }

    function test_setTableSchema_revertsForNonSigner() public {
        vm.prank(address(0xDEAD));
        vm.expectRevert(abi.encodeWithSelector(IDatabases.NotIndexerSigner.selector, INDEXER_ID, address(0xDEAD)));
        databases.setTableSchema(DATABASE_ID, CALLER, TABLE_ID, HASH_V1, JSON_V1);
    }

    function test_setTableSchema_revertsForNonWriter() public {
        vm.prank(INDEXER_SIGNER);
        vm.expectRevert(abi.encodeWithSelector(IDatabases.NotDatabaseWriter.selector, DATABASE_ID, address(0xBAD)));
        databases.setTableSchema(DATABASE_ID, address(0xBAD), TABLE_ID, HASH_V1, JSON_V1);
    }

    function test_setTableSchema_revertsForInactiveTable() public {
        vm.prank(INDEXER_SIGNER);
        databases.deleteTable(DATABASE_ID, CALLER, TABLE_ID);
        vm.prank(INDEXER_SIGNER);
        vm.expectRevert(abi.encodeWithSelector(IDatabases.TableNotActive.selector, DATABASE_ID, TABLE_ID));
        databases.setTableSchema(DATABASE_ID, CALLER, TABLE_ID, HASH_V1, JSON_V1);
    }

    function test_setTableSchema_revertsForEmptyHashOrJson() public {
        vm.prank(INDEXER_SIGNER);
        vm.expectRevert(IDatabases.SchemaHashEmpty.selector);
        databases.setTableSchema(DATABASE_ID, CALLER, TABLE_ID, bytes32(0), JSON_V1);
        vm.prank(INDEXER_SIGNER);
        vm.expectRevert(IDatabases.SchemaJsonEmpty.selector);
        databases.setTableSchema(DATABASE_ID, CALLER, TABLE_ID, HASH_V1, "");
    }
}
```

(Adapt constant names — `INDEXER_ID`, `CALLER`, `TABLE_TYPE` — to what `Databases.t.sol:15-34` actually names them; the assertions are the contract.)

- [ ] **Step 2: Run to verify failure**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/compute-network-contracts && git checkout -b feat/table-schema-registry && forge test --match-contract DatabasesSchemaTest -vv`
Expected: compilation failure — `setTableSchema` undefined.

- [ ] **Step 3: Implement**

`Types.sol`, beside `Table` (`src/libraries/Types.sol:81-87`):

```solidity
    struct TableSchema {
        bytes32 schemaHash;
        string schemaJson;
    }
```

`IDatabases.sol`: the event, the three functions, the two errors (next to the existing table errors). `Databases.sol` — storage appended after `_permissionAccountsByDb` (`src/Databases.sol:46`):

```solidity
    // Column-level schema declarations per (databaseId, tableId, version).
    // Write-once per version; the cursor below allocates versions and never
    // resets (deleteTable keeps history — replay of old blocks needs it).
    mapping(string => mapping(string => mapping(uint32 => Types.TableSchema))) private _tableSchemas;
    mapping(string => mapping(string => uint32)) private _tableSchemaVersions;
```

and beside `createTable` (`src/Databases.sol:230-249`), reusing its guard block verbatim:

```solidity
    function setTableSchema(
        string calldata databaseId,
        address caller,
        string calldata tableId,
        bytes32 schemaHash,
        string calldata schemaJson
    ) external returns (uint32) {
        Types.Database storage db = _requireUsable(databaseId);
        address signer = _indexerRegistry().getSigner(db.indexerId);
        if (signer == address(0) || msg.sender != signer) revert NotIndexerSigner(db.indexerId, msg.sender);
        if (!isDatabaseWriter(databaseId, caller)) revert NotDatabaseWriter(databaseId, caller);
        if (!_tables[databaseId][tableId].active) revert TableNotActive(databaseId, tableId);
        if (schemaHash == bytes32(0)) revert SchemaHashEmpty();
        if (bytes(schemaJson).length == 0) revert SchemaJsonEmpty();
        uint32 version = _tableSchemaVersions[databaseId][tableId] + 1;
        _tableSchemaVersions[databaseId][tableId] = version;
        _tableSchemas[databaseId][tableId][version] = Types.TableSchema({schemaHash: schemaHash, schemaJson: schemaJson});
        emit TableSchemaSet(databaseId, tableId, version, schemaHash, schemaJson);
        return version;
    }

    function getTableSchema(string calldata databaseId, string calldata tableId, uint32 version)
        external view returns (Types.TableSchema memory)
    {
        return _tableSchemas[databaseId][tableId][version];
    }

    function latestTableSchemaVersion(string calldata databaseId, string calldata tableId)
        external view returns (uint32)
    {
        return _tableSchemaVersions[databaseId][tableId];
    }
```

- [ ] **Step 4: Run tests + regenerate bindings + commit + PR**

```bash
forge clean && forge build && forge test --match-contract DatabasesSchemaTest -vv && forge test
./gen_bindings.sh
git add src/ test/ bindings/
git commit -m "feat(databases): on-chain table schema declarations (schema-registry Phase B)"
git push origin feat/table-schema-registry
gh pr create --repo sentioxyz/compute-network-contracts --title "feat(databases): setTableSchema + TableSchemaSet (schema-registry Phase B)" --body "Commitment-only storage (hash + json in event log), contract-owned version cursor surviving delete/recreate, createTable's exact guard block. Spec: housegate docs/superpowers/specs/2026-07-29-schema-registry-phase-b-design.md §3. Devnet upgrade after merge via scripts/Upgrade.sol (Databases is already in _resolveProxy)."
```

Expected: full suite green (the UUPS reviewer hook checks the appended-storage discipline).

---

### Task 2: sentio-core — the `TableSchemas` collection end to end

**Files:**
- Modify: `common/statemirror/on_chain_mapping_constants.go`, `network/state/types.go`, `network/state/state.go`, `network/state/state_mirrored.go`, `network/state/store_file.go`, `network/state/store_postgres.go`
- Test: `network/state/state_test.go` (extend), `network/state/state_mirrored_test.go` (extend — find the existing fake-mirror fixture there and reuse it)

**Interfaces:**
- Produces (Tasks 3/6 consume):

```go
// statemirror
MappingTableSchemas OnChainKey = "TableSchemas"

// state package
type TableSchemaInfo struct {
    DatabaseId string `json:"databaseId" yaml:"database_id"`
    TableId    string `json:"tableId" yaml:"table_id"`
    Version    uint32 `json:"version" yaml:"version"`
    SchemaHash string `json:"schemaHash" yaml:"schema_hash"`
    SchemaJson string `json:"schemaJson" yaml:"schema_json"`
}
func TableSchemaKey(databaseId, tableId string, version uint32) string      // "<db>/<table>@<version>"
func ParseTableSchemaKey(key string) (databaseId, tableId string, version uint32, err error)
// PlainState gains: TableSchemas map[string]TableSchemaInfo `yaml:"table_schemas"`
// State interface gains:
GetTableSchema(key string) (TableSchemaInfo, bool)
GetTableSchemas() map[string]TableSchemaInfo
UpsertTableSchema(ctx context.Context, info TableSchemaInfo) error
// TableInfo gains pointer fields: SchemaVersion uint32, SchemaHash string (json/yaml tagged)
```

- [ ] **Step 1: Write the failing tests**

Extend `state_test.go` (follow its table style): key round-trip (`TableSchemaKey`/`ParseTableSchemaKey` for plain and version-10 keys; parse rejects missing `@`, missing `/`, non-numeric version); `PlainState.UpsertTableSchema` + `GetTableSchema` + `Clone` isolation (mutate clone, source unchanged). Extend `state_mirrored_test.go` with the decisive coverage the exploration flagged: build a `StateMirrored` over a fake mirror, `ReplaceInner` with a working copy that adds one `TableSchemaInfo` → assert the fake mirror received an Added diff under `MappingTableSchemas` with field `db_1/table_1@1`; then `SyncMirror` → assert full-push includes it; then single-key `UpsertTableSchema` → assert the syncDatabase-style targeted sync fired.

- [ ] **Step 2: Run to verify failure**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/sentio-core && git checkout -b feat/table-schemas-collection && go test ./network/state/ -run 'TableSchema' -v`
Expected: FAIL — types undefined.

- [ ] **Step 3: Implement**

Follow the exploration's per-collection recipe with `MappingDatabases` as the line-by-line template: constant (`on_chain_mapping_constants.go:11`); types + key functions (`types.go` — `ParseTableSchemaKey` splits on the LAST `@` then the FIRST `/`, rejecting leftovers); `PlainState` field + `Clone` (`maps.Clone`, flat map — one line) + the three methods (replace-or-set into the map, keyed by `TableSchemaKey(info.DatabaseId, info.TableId, info.Version)`); `StateMirrored`: codec field + `newCodec[TableSchemaInfo]()` (`state_mirrored.go:29`), a `diffApply` segment in `ReplaceInner` (template `state_mirrored.go:66-69`), a full-push segment in `SyncMirror` (template `:453-461`), and `UpsertTableSchema` delegating to inner + targeted sync (template `syncDatabase` `:403-416`); `store_file.go`: nil-map init on Load, field flows through the existing whole-state YAML marshal; `store_postgres.go`: `TableSchemaRow{Key string \`gorm:"primaryKey"\`; ...}` + `TableName() "sentio_node_table_schemas"` following `IndexerInfoRow` (`store_postgres.go:25-36`), added to AutoMigrate and the Save/Load loops. `TableInfo` gains the two pointer fields.

- [ ] **Step 4: Run + commit + PR**

```bash
go build ./... && go test ./network/... ./common/statemirror/... -count=1
git add common/statemirror/ network/state/
git commit -m "feat(state): TableSchemas collection through state, mirror, and stores"
git push origin feat/table-schemas-collection
gh pr create --repo sentioxyz/sentio-core --title "feat(state): TableSchemas network-state collection (schema-registry Phase B)" --body "Flat composite-key collection (<db>/<table>@<version>) wired through PlainState, BOTH StateMirrored paths (ReplaceInner batch diff + SyncMirror full push), file and postgres stores. Spec: housegate docs/superpowers/specs/2026-07-29-schema-registry-phase-b-design.md §4."
```

---

### Task 3: sentio-node syncer leg — bindings copy + `onTableSchemaSet`

**Files:**
- Modify: `bindings/bindings.go` (verbatim copy from Task 1's regenerated file), `handlers/database_event.go`, `go.mod` (sentio-core → Task 2 version)
- Test: `handlers/database_event_test.go` (find the existing fabricated-log fixture style and follow it)

**Interfaces:**
- Consumes: Task 1's `IDatabasesFilterer.ParseTableSchemaSet`, Task 2's `State.UpsertTableSchema`/`TableSchemaInfo`.
- Produces: schemas flow chain→state for any table on a synced node; full-replay rebuild included by construction.

- [ ] **Step 1: Copy bindings + bump sentio-core, write the failing handler test**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node && git checkout -b feat/schema-registry-phase-b
cp /Users/uranuswch/Dev/sentio_xyz/compute-network-contracts/bindings/bindings.go bindings/bindings.go
go get sentioxyz/sentio-core@<task-2-version> && go mod tidy
```

Test: fabricate a `TableSchemaSet` log (pack via the binding's ABI — read how existing handler tests fabricate `TableCreated` logs and mirror it), run `DatabaseEventHandler.Handle` against a state pre-seeded with the database, assert: `GetTableSchema("db_1/table_1@1")` returns the content AND the database's `TableInfo` now carries `SchemaVersion: 1` + the hash; a log for an unknown database is skipped with a warning (mirroring `onTableCreated`'s guard); a `TableSchemaSet` whose ids contain `/` is still stored faithfully (the guard against separator ids lives at declaration, not here — the syncer mirrors the chain verbatim).

- [ ] **Step 2: Run to verify failure, then implement**

`handlers/database_event.go`: add `ParseTableSchemaSet` to the sequential-try chain (`database_event.go:33-55`) and:

```go
func (h *DatabaseEventHandler) onTableSchemaSet(ctx context.Context, decoded *bindings.IDatabasesTableSchemaSet) error {
    if _, ok := h.hctx.State.GetDatabase(decoded.DatabaseId); !ok {
        h.hctx.Logger.Warn("table schema for unknown database, skipping", "database", decoded.DatabaseId, "table", decoded.TableId)
        return nil
    }
    info := statecore.TableSchemaInfo{
        DatabaseId: decoded.DatabaseId, TableId: decoded.TableId,
        Version: uint32(decoded.Version), SchemaHash: "0x" + hex.EncodeToString(decoded.SchemaHash[:]), SchemaJson: decoded.SchemaJson,
    }
    if err := h.hctx.State.UpsertTableSchema(ctx, info); err != nil {
        return err
    }
    return h.upsertTableSchemaPointer(ctx, decoded) // updates TableInfo.SchemaVersion/SchemaHash via UpsertDatabaseTable
}
```

(`SchemaHash` arrives as `[32]byte` from abigen — encode as `"0x" + hex`, matching what declarers store; confirm against the generated struct. `upsertTableSchemaPointer` reads the existing `TableInfo`, sets the two fields, and calls `UpsertDatabaseTable` — mirroring `onTableCreated` `:142-157`.) No `FilterQuery` change (address-filtered).

- [ ] **Step 3: Run + commit**

```bash
go build ./... && go test ./handlers/ -count=1 && bazel mod tidy && bazel run //:gazelle && bazel build //...
git add bindings/ handlers/ go.mod go.sum MODULE.bazel $(git ls-files -mo --exclude-standard '*BUILD.bazel')
git commit -m "feat(handlers): mirror TableSchemaSet into the TableSchemas collection"
```

(PR opens at the end of Task 6 — Tasks 3/5/6 stack on this branch.)

---

### Task 4: housegate — `registry.TableSchemas`, `NetworkStateLoader`, commitgate hook, fixtures

**Files:**
- Create: `pkg/registry/schemas.go`, `pkg/schemaregistry/networkstate_loader.go`, `pkg/schemaregistry/networkstate_loader_test.go`
- Modify: `pkg/plugins/commitgate/observer.go` (optional interface), `pkg/plugins/commitgate/plugin.go` (dispatch), `pkg/plugins/commitgate/plugin_test.go`
- Modify: `pkg/network/types.go`, `pkg/network/inmemory.go`, `pkg/network/yaml.go`, `configs/local.network_state.yaml`

**Interfaces:**
- Produces (Tasks 5/6 consume):

```go
// pkg/registry/schemas.go — standalone, NOT in the Registry union
type TableSchema struct {
    DatabaseId string
    TableId    string
    Version    uint32
    SchemaHash string // "0x" + hex
    SchemaJson string
}
type TableSchemas interface {
    TableSchema(databaseId, tableId string, version uint32) (TableSchema, bool)
    LatestTableSchema(databaseId, tableId string) (TableSchema, bool)
}

// pkg/schemaregistry
func NewNetworkStateLoader(schemas registry.TableSchemas, networkID string) *NetworkStateLoader // implements Loader
func (l *NetworkStateLoader) WithClickHouseCrossCheck(conn clickhouse.Conn) *NetworkStateLoader
var ErrSchemaContentMissing, ErrSchemaHashMismatch, ErrClickHouseDrift error

// pkg/plugins/commitgate — optional observer interface (marker pattern)
type SuccessObserver interface {
    AfterStatementSuccess(ctx context.Context, ev Event)
}
```

- [ ] **Step 1: Write the failing loader tests**

`networkstate_loader_test.go`, pure Go, table-driven like the Phase-A `loader_test.go`. Fake `registry.TableSchemas` backed by a map. Cases: happy path (declared json for a 2-column table; loader returns the decoded `payloadexec.TableSchema` and — asserted — `payloadexec.TableSchemaHash(networkID, schema)` equals the declared hash); content missing → `errors.Is(err, ErrSchemaContentMissing)`; declared hash ≠ recomputed → `ErrSchemaHashMismatch` (build the fixture by declaring a tampered hash); malformed `SchemaJson` → error; cross-check mode with a fake CH-side result differing in one column type → `ErrClickHouseDrift` (inject via a tiny `chLoader Loader` seam on the struct rather than a real conn — the decorator takes the interface internally; `WithClickHouseCrossCheck(conn)` wraps the real one, `withCrossCheckLoader(l Loader)` is the test seam); loader uses the version from `LatestTableSchema` per ref.

- [ ] **Step 2: Write the failing commitgate hook test**

In `plugin_test.go`, following the file's existing fixture style (find how it drives OnQuery/OnQueryComplete/OnException today): an observer implementing both `Observer` and `SuccessObserver` records calls. Cases: gated CREATE TABLE → OnQuery → OnQueryComplete ⇒ exactly one `AfterStatementSuccess` with the stashed Event (dispatch may be async — sync via a channel with timeout); OnQuery → OnException → OnQueryComplete ⇒ zero success calls; a plain observer without the marker ⇒ no calls, no panic; non-gated statement ⇒ no calls.

- [ ] **Step 3: Run to verify failure, then implement**

Loader: decode `SchemaJson` into `payloadexec.TableSchema` (canonical form IS its JSON encoding), verify `TableSchemaHash`, assemble; cross-check decorator loads the same refs through the inner CH loader and `reflect.DeepEqual`s per-table (any diff → `ErrClickHouseDrift` naming the table and the first differing field). Commitgate: `SuccessObserver` interface beside `Observer` (`observer.go`); in the plugin, `OnException` marks the stashed event failed (add a `failed bool` beside the stash — the stash mechanics already exist, `plugin.go:65-90,165-176`); `OnQueryComplete` — before clearing the stash — when a stash exists and is not failed, snapshot the Event value and dispatch `go func` to every subscribed observer that type-asserts to `SuccessObserver` (goroutine because declaration submits a chain tx; the relay byte path must never wait — document on the interface: "dispatched asynchronously after EndOfStream; best-effort, may be lost on crash; implementations own their retries"). Fixture mirror: `TableSchemaInfo` in `pkg/network/types.go` (tags byte-identical to sentio-core's), `InMemoryNetworkState.TableSchemas map[string]TableSchemaInfo` + the two interface methods (RLock; latest = linear scan for max version), `yaml.go` `table_schemas:` section + copy loop + count log, sample block appended to `configs/local.network_state.yaml`.

- [ ] **Step 4: Bazel + run + commit + PR**

```bash
cd /Users/uranuswch/Dev/housegate/housegate && git checkout main && git pull && git checkout -b feat/schema-registry-network-loader
# (implement, then:)
bazel mod tidy && bazel run //:gazelle
bazel test //pkg/schemaregistry:schemaregistry_test //pkg/plugins/commitgate:commitgate_test //pkg/network:network_test
bazel build //...
git add pkg/ configs/ $(git ls-files -mo --exclude-standard '*BUILD.bazel')
git commit -m "feat(schemaregistry): network-state loader, TableSchemas interface, commitgate success hook

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin feat/schema-registry-network-loader
gh pr create --repo housegate/housegate --base main --title "feat(schemaregistry): network-state schema loader + commitgate AfterStatementSuccess (Phase B)" --body "Standalone registry.TableSchemas interface (metadata.go convention), NetworkStateLoader with the hash-verification ladder + CH cross-check decorator behind the Phase-A Loader seam, optional commitgate SuccessObserver (marker pattern, async best-effort dispatch), yaml/inmemory fixture mirror. Spec: docs/superpowers/specs/2026-07-29-schema-registry-phase-b-design.md §6."
```

---

### Task 5: sentio-node declaration leg — observer hook + backfill command

**Files (same branch as Task 3):**
- Modify: `database_registry/observer.go`, `go.mod` (housegate → Task 4 version)
- Create: `commands/declare_table_schemas.go`
- Test: `database_registry/observer_test.go` (extend — the fake `observerContract` fixture at `newObserverWithContract`), docker-gated declare test

**Interfaces:**
- Consumes: Task 1's contract method (via `env.SubmitAndWaitTx` + bindings), Task 4's `commitgate.SuccessObserver` + `schemaregistry.ClickHouseLoader` + `payloadexec.TableSchemaHash`, `snode.CHTableName`, `config.StorageIntegrityUnsafeDatabase`.
- Produces: automatic declaration on CREATE TABLE; `sentio-node declare-table-schemas` for backfill/repair (rollout step 3).

- [ ] **Step 1: Write the failing tests**

Observer tests (fake contract records `setTableSchema` calls; settable `latestTableSchemaVersion`/`getTableSchema` answers; CH via `requireCH`-style gate where the real loader runs): declare-on-create (AfterStatementSuccess for a CreateTable event whose physical table exists in CH ⇒ one `setTableSchema` call whose hash equals `payloadexec.TableSchemaHash(networkID, derived)`); skip-if-identical (latest declared hash already matches ⇒ zero calls); separator rejection (table id containing `/` or `@` ⇒ zero calls, one loud error log); non-SI table (not under the unsafe database / not resolvable) ⇒ zero calls; CH read failure ⇒ zero calls, error logged, DDL unaffected (the hook returns nothing). Command test: fake contract + two CH tables, one already declared with matching hash ⇒ exactly one `setTableSchema` for the other.

- [ ] **Step 2: Run to verify failure, then implement**

`observer.go`: `observerContract` gains `setTableSchema(ctx, env, databaseId, caller string, tableId string, schemaHash [32]byte, schemaJson string) (uint32, error)` + `latestTableSchema(ctx, databaseId, tableId string) (version uint32, hash [32]byte, err error)`; `envContract` implements both (`SubmitAndWaitTx` / contract view call). The observer struct gains the CH conn + SI network id + unsafe database (injected at construction from the standalone assembly — same values the SI block already has); implement `AfterStatementSuccess(ctx, ev)`: filter `ev.Type == CreateTable`; resolve `(db, table)` from `ev.AccessedTables` exactly as `onCreateTable` does (`observer.go:240-250`); reject ids containing `/` or `@` (loud error — spec decision 2's declaration-time guard); derive via `schemaregistry.NewClickHouseLoader(conn).Load` on the single ref `{TableID: id, Database: unsafeDB, Table: snode.CHTableName(id)}`; marshal to canonical JSON (`json.Marshal(payloadexec.TableSchema)`); compute `TableSchemaHash(networkID, schema)`; skip if the latest on-chain hash matches; else submit. All failure paths: `log.Error` + return (best-effort; the command repairs). `commands/declare_table_schemas.go`: cobra command following the siblings (`commands/claim.go` shape) — loads config, dials CH + contract env, iterates `storage_integrity.snode.table_ids`, runs the same declare-one function per table, prints a summary (declared/skipped/failed).

- [ ] **Step 3: Run + commit**

```bash
go build ./... && go test ./database_registry/ ./commands/ -count=1
git add database_registry/ commands/ go.mod go.sum
git commit -m "feat(database_registry): declare table schemas after CREATE TABLE success

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: sentio-node read leg + `schema_source` switch

**Files (same branch):**
- Modify: `standalone/networkstate/redis.go`, `standalone/networkstate/networkstate.go`, `standalone/networkstate/convert.go`, `config/config.go`, `standalone/standalone.go`
- Test: `standalone/networkstate/networkstate_test.go` (extend), `config/config_test.go` (extend)

**Interfaces:**
- Consumes: Task 2's collection + key fns, Task 4's `registry.TableSchemas` + `NewNetworkStateLoader` + `WithClickHouseCrossCheck`.
- Produces: both `registry.Registry` implementations also satisfy `registry.TableSchemas`; config `storage_integrity.snode.schema_source: clickhouse | network_state` (default `clickhouse`).

- [ ] **Step 1: Write the failing tests**

Read leg (fake mirror / statecore fixtures, following `networkstate_test.go`'s style): `TableSchema("db_1","t",1)` hit and miss; `LatestTableSchema` picks the max version among `@1..@3`; `convert.go`'s `tableSchemaFromCore` field fidelity. Config: `schema_source` allowlist (`""`≡`clickhouse`, `network_state`, junk rejected); compile-time `var _ registry.TableSchemas = (*RedisNetworkState)(nil)` + same for `FromStatecore`.

- [ ] **Step 2: Run to verify failure, then implement**

`redis.go`: the two methods via `mirror.Get(ctx, statemirror.MappingTableSchemas, statecore.TableSchemaKey(...))` + JSON unmarshal + `tableSchemaFromCore`; latest via `mirror.Scan` glob `<db>/<table>@*` picking max `ParseTableSchemaKey` version. `networkstate.go`: same over the in-memory state. `convert.go`: the projection. `config.go`: `SchemaSource string \`yaml:"schema_source"\`` on `StorageIntegritySNode` + allowlist validation. `standalone.go`: in the SI assembly block, choose the loader —

```go
var loader schemaregistry.Loader = schemaregistry.NewClickHouseLoader(chConn)
if si.SNode.SchemaSource == "network_state" {
    loader = schemaregistry.NewNetworkStateLoader(netState, si.SNode.NetworkID).WithClickHouseCrossCheck(chConn)
}
tables, err := loader.Load(ctx, refs)
```

(`netState` is the existing `RedisNetworkState` from the housegate wiring — it now satisfies `registry.TableSchemas`.) Everything downstream (snode.Config.Tables, SchemaResolver, genesis anchor) is untouched.

- [ ] **Step 3: Run + commit + PR (closes the Task 3/5/6 branch)**

```bash
go build ./... && go test ./standalone/... ./config/ ./handlers/ ./database_registry/ -count=1
bazel mod tidy && bazel run //:gazelle && bazel build //...
git add standalone/ config/ $(git ls-files -mo --exclude-standard '*BUILD.bazel')
git commit -m "feat(standalone): network-state schema source with ClickHouse cross-check

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin feat/schema-registry-phase-b
gh pr create --repo sentioxyz/sentio-node --title "feat: schema-registry Phase B — syncer, declaration, and network-state schema source" --body "TableSchemaSet mirroring (bindings realigned), automatic declaration via the commitgate success hook (CH readback, skip-if-identical), declare-table-schemas backfill command, registry.TableSchemas read leg, config-gated schema_source (default clickhouse — flip per rollout step 6). Spec: housegate docs/superpowers/specs/2026-07-29-schema-registry-phase-b-design.md §5. Depends on sentio-core and housegate Phase B PRs."
```

---

### Task 7: E2E + rollout runbook + spec status

**Files:**
- Modify: the sentio-node SI smoke + its runbook doc; housegate `docs/superpowers/specs/2026-07-29-schema-registry-phase-b-design.md` (status) on the `docs/schema-registry` branch

- [ ] **Step 1: Extend the gated E2E**

New smoke phase (behind the existing `SENTIO_SI_E2E=1` gate, services per its runbook plus a local chain with the upgraded contract — anvil + the repo's deploy flow, or the devnet): CREATE TABLE through housegate → poll `latestTableSchemaVersion` until the declaration lands → wipe the consumer's derived state → boot with `schema_source: network_state` → assert startup passes the hash ladder + genesis anchor + CH cross-check → one INSERT to ACK2. Plus the backfill rehearsal: run `declare-table-schemas` against pre-existing tables, assert declared hashes equal Phase-A-derived hashes and a second run declares nothing.

- [ ] **Step 2: Rollout runbook**

Document the spec §10 sequence operationally: devnet contract upgrade (`forge clean && forge build` → `diff_deployed.sh` → `Upgrade.sol` broadcast → `upgrade-devnet.yml` for devnet), deploy sentio-node (syncer picks up events; old nodes unaffected), backfill command run, verify collection population (`redis-cli HLEN statemirror:v1:TableSchemas`), flip `schema_source` per node with rolling restarts, confirm every role re-anchors. Include the rollback: flip back to `clickhouse` — Phase A behavior is fully preserved.

- [ ] **Step 3: Final verification + spec status**

All four repos green (contracts `forge test`; the three Go repos build+test per their tasks). Mark the Phase B spec Implemented with the four PR links; commit on `docs/schema-registry`.

---

## Self-review notes (already applied)

- Spec §10 order → task order matches (1 → {2,3,4} → 5 → 6 → 7); Task 3 is deployable before any declarer exists (spec step 3).
- Decision coverage: 1 → Tasks 4 (hook) + 5 (observer impl); 2 → Task 2 (flat map + key fns) + Task 5 (declaration-time separator guard) + Task 3's test pinning that the syncer mirrors verbatim; 3 → Task 4; 4 → Tasks 4+6 (ladder + cross-check wiring); 5 → Task 5 (declarer computes hash, canonical JSON = `payloadexec.TableSchema` encoding); 6 → Task 1 (cursor + delete/recreate test) + Task 5 (skip-if-identical); 7 → Task 2 (both mirror paths tested) ; 8 → Task 3 Step 1 (verbatim copy); 9 → Task 6 (config default) + Task 7 (runbook + rollback).
- The `SchemaHash` cross-language representation is pinned in two places: `[32]byte` on-chain / in bindings, `"0x"+hex` everywhere in Go state — Task 3 encodes at the mirror boundary, Task 5 decodes when comparing (`latestTableSchema` returns `[32]byte`; compare against `TableSchemaHash`'s string form by encoding the same way). Implementers: `TableSchemaHash` returns a `"0x"`-prefixed string (housegate digest convention) — confirm at Task 5 and encode the on-chain bytes identically before comparing.
- Fixture-reuse markers ("find the existing … style") appear where sentio-core/sentio-node test scaffolding is the source of truth; each carries the full assertion contract.
- Anvil-vs-devnet choice for the Task 7 smoke is left to the executor based on what the SI smoke already assumes about chain availability — both paths are documented in the runbook step.
