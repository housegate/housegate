# Dynamic Storage-Integrity Table Set in sentio-node Implementation Plan (sub-project 4, plan B: host)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** sentio-node supplies HouseGate's storage-integrity table state from the arbiter's committed table registry, the embedded SNode reconciler's readiness and the syncer's chain state, so a table created through HouseGate becomes Active, retires and is recreated without a restart or a config edit; the storage-node JSON-RPC answers the agent's `sentio_getStorageIntegrityTableStatus`; lost schema declarations are compensated; and the node exports the table-set metrics. sentio-core records each table's creation block, which the activation window needs.

**Architecture:** A new package `storageintegrityadapter/tablestate` implements `sitable.TableState` over three narrow ports (`RegistrySource`, `Readiness`, `ChainSource`). A pure builder evaluates design §9.2's `Lookup` order for every table the registry or a governed database's chain state names, and publishes an immutable `Snapshot` whose version moves only when its answers change; a `Run` loop rebuilds it on every registry change and every second. One adapter file maps plan A's `wire.TableRegistrySnapshot` onto the package's model. `standalone.Run` owns the process's one `dataplane.RegistryFollower`: it hands the follower to `snode.Deps.Registry`, starts it inside the startup transaction, waits for its first answer, publishes the first snapshot, and only then constructs HouseGate with `Options.StorageIntegrityTableState`. The chain state comes from the Redis state mirror (the same source HouseGate routes on), which now carries `TableInfo.CreatedBlock`. Config validation forces `housegate.storage_integrity.enabled` on, drops a legacy table list equal to `snode.table_ids` (the genesis set) and rejects any other. The schema declarer always declares after a CREATE (a recreation's latest on-chain hash may belong to an earlier incarnation) and spells column types canonically; a 5-minute loop re-declares tables whose declaration was lost.

**Tech Stack:** Go (module `compute-network-node`), Bazel 9.1.0 + Bzlmod + gazelle (sentio-node, sentio-core), housegate `$HOUSEGATE_TAG` (`pkg/sitable`, `pkg/registry`, `pkg/network`), arbiter-core `$ARBITER_CORE_TAG` (`dataplane.RegistryFollower`, `wire.TableRegistrySnapshot`, `snode.Role.TableReady`), arbiter-proto `$ARBITER_PROTO_PSEUDO` (`TableRegistry` service), sentio-core `$SENTIO_CORE_VERSION`, Prometheus client_golang, ClickHouse 25.8 with Keeper (acceptance).

**Spec:** `docs/superpowers/specs/2026-09-25-dynamic-si-table-set-data-plane-design.md` (binding): §9, the sentio-core and sentio-node rows of §10, §11 step 2, and §12 for the handoff. The host contract is §13 of `docs/superpowers/specs/2026-09-24-dynamic-si-table-set-housegate-design.md`, with its `sitable` model (§5) and JSON-RPC contract (§10.2). Plan A (`docs/superpowers/plans/2026-09-25-si-data-plane-reconciler.md`, section "Handoff to plan B") supplies the arbiter-core names this plan uses. Out of scope: the helm chart, Gate R4 and the devnet2 rollout (sub-project 5, see the handoff at the end), the snapshot-query lane, and `ALTER` of storage-integrity tables.

## Global Constraints

- **Repositories and bases.** housegate `origin/main` at `04fb8c0` or later (Task 1 only cuts a tag). sentio-core `origin/main` (Task 2, branch `feat/table-created-block`). sentio-node `origin/main` at `1494fb1` or later (Tasks 4–15, one branch `feat/si-dynamic-table-state`). Never push to `main`; each repo merges through a PR (sentio-node and sentio-core require it).
- **Pins the controller fills in** (never predict them; the workflows derive versions from the release day):
  - `$HOUSEGATE_TAG`: the housegate tag Task 1 cuts; `$HOUSEGATE_SHA` is `git -C housegate rev-parse "$HOUSEGATE_TAG^{commit}"`.
  - `$ARBITER_CORE_TAG`: plan A's arbiter-core release (handoff item 1, planned `v0.10.0`); `$ARBITER_CORE_SHA` is its `CORE_SHA`, equal to `git -C arbiter-core rev-parse "$ARBITER_CORE_TAG^{commit}"`.
  - `$ARBITER_PROTO_PSEUDO`: the arbiter-proto version plan A's handoff item 1 names (planned tag `v0.8.0`; go.mod only, no Bazel pin).
  - `$SENTIO_CORE_VERSION`: the Go pseudo-version of the sentio-core `main` commit that merges Task 2 (`v0.0.0-<UTC yyyymmddhhmmss>-<sha12>`); `$SENTIO_CORE_SHA` is that commit's full hash. sentio-core is pinned by commit, not by tag, as today.
- **Plan A interface (handoff items 2–4), used verbatim:** `dataplane.NewRegistryFollower(c *Client, logger *slog.Logger) *RegistryFollower` with `Run(ctx) error`, `View() (wire.TableRegistrySnapshot, bool)`, `Changed() <-chan struct{}`, `Ready() <-chan struct{}`, `Connected() bool`; `dataplane.RegistryView`; `dataplane.WaitReady(ctx, v, timeout) error`; `dataplane.DefaultRegistryStartupTimeout`; `wire.TableRegistrySnapshot{Params arbiter.TableRegistryParams; Version; Seeded; Cursor; Incarnations []wire.TableIncarnation}` with the `wire.TableStatus*` and `wire.TableOrigin*` constants; `snode.Deps.Registry dataplane.RegistryView`; `(*snode.Role).TableReady(tableID string) bool`; `snode.ErrTableNotReady`. Only `storageintegrityadapter/tablestate/arbitercore.go` and `standalone/storage_integrity_table_state.go` name plan A's registry types.
- **One follower per process.** sentio-node constructs and runs it; the SNode role and the table state read the same value.
- **HouseGate contract.** `housegate.storage_integrity.enabled: true`, `tables` empty, `Options.StorageIntegrityTableState` set, `Options.StorageIntegrityReadState` the `*snode.Role`. A snapshot is immutable and does no I/O; `Changed()` closes at the next version change after the call. The new housegate requires rewriter contract V2 (a V1 rewriter refuses HouseGate startup), so every test fake acknowledges `STORAGE_INTEGRITY_CONTRACT_V2`.
- **JSON-RPC.** Method `sentio_getStorageIntegrityTableStatus`, positional params `[database, table]`, result exactly housegate's `registry.TableStatus`: `{"status": "ordinary|pending|refused|active|gone", "refused_code": "", "refused_reason": "", "schema_json": "", "schema_hash": "", "registry_version": 0}`.
- **Metrics** (registered on `Proxy.MetricsRegistry()`, or the default registry when it is nil): `sentio_node_storage_integrity_tables{status}`, `sentio_node_storage_integrity_table_pending_seconds{table}`, `sentio_node_storage_integrity_schema_compensations_total{outcome="declared|failed"}`, `sentio_node_storage_integrity_registry_follower_connected`, `sentio_node_storage_integrity_registry_enabled`, `sentio_node_storage_integrity_registry_version`.
- **Defaults.** Table-state refresh 1s (`tablestate.DefaultRefreshInterval`); registry startup wait `dataplane.DefaultRegistryStartupTimeout` (2 minutes); compensation every 5 minutes, minimum age 10 minutes.
- **Tests.** Bazel is the ground truth (`bazel test //...`); `go test ./...` is the fast loop. Docker-bound acceptance tests need `SENTIO_SI_CH_E2E=1` and `CH_ADDR` pointing at a Keeper-enabled ClickHouse, started as `.github/workflows/ci.yml`'s `integration-clickhouse` job does (`scripts/ci/clickhouse-keeper.xml`, image `clickhouse/clickhouse-server:25.8.32.4`).
- **source-shape audits.** `standalone/storage_integrity_bootstrap_test.go` parses `standalone.go`. New wiring in `Run` goes through methods of `storageIntegrityTableStateRuntime` defined in another file, so `Run` gains no new `Register`, `Prepare`, `RunWithReady`, `.Run(` call or `cfg.StorageIntegrity` reference; the one assertion that changes is the contract snapshot's table list (Task 13).
- **Conventions.** Run `bazel run //:gazelle` after adding or removing Go files and `bazel mod tidy` after a go.mod change; conventional commit scopes; English code and comments; Markdown not hard-wrapped; every commit message ends with a blank line and `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Active before this node is ready (spec E6).** The registry can record a table Active before this node's reconciler has created and verified its `hg_*` tables, and a genesis incarnation carries no `schema_json` at all. Reporting Active too early sends signed writes to tables that do not exist; failing to serve a genesis schema makes every genesis table Pending the moment the registry is enabled. Tests: Task 7 `TestLookupMatrix` (rows `active_unready`, `bad_hash` and `genesis_t`, and `Schema` of the genesis table returning the configured schema); Task 14 `TestStorageIntegrityDynamicTableLifecycle` step 2 (Active in the registry, `hg_*` absent: the agent's status and the proxy still say pending).
2. **HouseGate must never run on a snapshot that was not built from the registry, and a change must never be lost.** HouseGate's runtime asserts merges and recovers journals from `Current()` as soon as it runs; the refresh loop must subscribe to the follower before it builds, or a version accepted mid-build waits for the next tick. Tests: Task 13 `TestStorageIntegrityTableStateBeginPublishesBeforeReturning`, `TestStorageIntegrityTableStateBeginFailsWhenTheRegistryNeverAnswers` and the source-shape guard `TestRunBuildsTheTableStateBeforeHousegate`; Task 7 `TestRunWakesOnRegistryChangeDuringRefresh` (one-hour tick, the change must still land).
3. **The version moves only when an answer changes.** A registry version bump, an Active-but-unready table, or a newly synced default-deny table all leave every `Lookup` answer unchanged; bumping the version for them would wake HouseGate's merge supervisor and scrubber cache for nothing, while missing a real change would leave HouseGate on stale statuses. Test: Task 7 `TestVersionMovesOnlyOnContentChange` (and `registry_version` still advancing: Task 14 step 2 waits for `RegistryVersion == 2` with the content unchanged).
4. **Same-name recreation must get a new declaration.** Contract schema versions survive `DROP` and re-`CREATE`, the old post-CREATE path skipped a declaration whose hash equalled the latest on-chain one, and `onTableCreated` copied the previous incarnation's schema pointer, so a recreated table with the same schema never got the first-declaration-after-`TableCreated` the registry needs and stayed Pending forever, invisible to any compensation. Tests: Task 5 `TestDatabaseEventHandlerTableCreatedRecordsBlockAndStartsWithoutSchema`; Task 11 `TestSchemaDeclarer_DeclarePhysicalDeclaresEvenWhenLatestMatches`; Task 12 `TestSchemaCompensatorSelectsOnlyLostDeclarationsAfterMinAge` (legacy, declared, pending-delete and peer tables excluded; a recreation restarts the clock).
5. **Chain-state failures must fail closed.** Every governed table that is not listed answers the snapshot's fallback, which is Ordinary for a database the snapshot does not govern. If an undecodable mirror entry silently dropped a governed database, all its tables would read Ordinary and take unsigned writes. Tests: Task 6 `TestRedisNetworkStateDatabaseInfosRejectsUndecodableEntries`; Task 7 `TestRefreshKeepsPreviousSnapshotOnChainError` and `TestLookupMatrix` row `never_synced` (a governed database's table the chain state has not synced yet is Pending).

## Plan decisions

- **B1 — Plan A's interface and one adapter.** The handoff section of plan A is final for names, and this plan uses them verbatim (Global Constraints). The table state reads three narrow ports defined in `tablestate/state.go`: `RegistrySource` (`View() (Registry, bool)`, `Changed()`, `Connected()`), `Readiness` (`Ready(tableID) bool`) and `ChainSource`. `tablestate/arbitercore.go` is the only file that converts `wire.TableRegistrySnapshot` into the package's `Registry` model, and `standalone/storage_integrity_table_state.go` is the only file that calls `dataplane.WaitReady` and runs the follower. The model re-implements the arbiter's `Live` rule over the converted incarnations (tested in Task 7) instead of calling `wire.TableRegistrySnapshot.Live`, so the builder is testable without arbiter-core. The prototype compiled every task against local stubs of these exact names on arbiter-core `d059aab` (see "Verification record").
- **B2 — Genesis schemas.** arbiter `enableTableRegistryLocked` seeds genesis incarnations with `schema_hash` but no `schema_json`. The builder therefore serves a genesis incarnation's schema from the configured genesis set (`snode.table_ids`, loaded at startup) and applies the same self-check: `payloadexec.TableSchemaHash(network_id, schema)` must equal the registry's `schema_hash`. Chain incarnations decode `schema_json` with `payloadexec.DecodeTableSchemaJSON`, which also requires the schema to name the table and to use admitted column types. Any failure reports the table Pending (Retiring or Purging: Gone without a schema) and logs the reason once per change.
- **B3 — Chain state source.** The Redis state mirror, read by a new `RedisNetworkState.DatabaseInfos`, which fails on an undecodable entry (HouseGate's `All` skips it). It is available before the syncer runs, is what HouseGate already routes and authorizes on, and is where spec §9.1 puts `CreatedBlock`. The refresh loop polls it every second; a refresh that fails keeps the previous snapshot.
- **B4 — Tables the chain state does not name.** Spec step 2 makes an unknown database Ordinary. For a known database on the SI indexer, a table missing from the chain state (created, not yet synced) is Pending even before the seed commits: it can only be newer than what the syncer has seen, and a retryable error for a few seconds is safer than an ordinary write into a table the registry is about to govern. The fallback of a governed database is therefore Pending.
- **B5 — Versioning.** A snapshot's content is the set of answers that differ from the fallback, plus the governed databases and the enabled flag. A default-deny Pending table is not stored even when the chain state names it, so syncing it changes nothing. The registry version is kept beside the snapshot, not in it: `registry_version` in the JSON-RPC answer is the version of the latest build, which the current snapshot answers for.
- **B6 — Readiness freshness.** `Role.TableReady` is in-memory and is read during each build, never inside `Lookup` (a snapshot must not change between two stages of one query). Readiness therefore reaches HouseGate within one refresh interval (1s). At startup the first snapshot is built before the SNode role runs, so a registry-Active table reads Pending until the first refresh after commit; the listeners open at that commit, so the window is at most one interval and its errors are retryable.
- **B7 — Follower ownership and startup.** `standalone.Run` constructs the follower next to the arbiter client and passes it to `snode.Deps.Registry`. Inside the startup transaction, before `housegate.New`, `storageIntegrityTableStateRuntime.begin` starts the follower with `tx.startImmediateTask` (it must run before commit; the helper's comment is updated to name this second production use), waits with `dataplane.WaitReady(ctx, follower, dataplane.DefaultRegistryStartupTimeout)`, refreshes, and registers the refresh and compensation loops as transaction tasks. The SNode's own `Register`/`RunWithReady` wait on the same follower's `Ready`, which is already closed.
- **B8 — Recreation and declarations.** `onTableCreated` records `CreatedBlock` and starts the schema pointer empty (it used to copy the latest pointer of an earlier incarnation, the "S5" backfill). `SchemaDeclarer.DeclarePhysical`, used by the post-CREATE hook and the compensation loop, always declares; `Declare`, used only by the genesis backfill command, keeps its skip-if-identical idempotence. Both spell supported column types with `payloadexec.CanonicalColumnType` and keep an unsupported type verbatim, so the arbiter records a Refused incarnation with a readable reason.
- **B9 — Compensation selection.** A table qualifies when its database is on this node's indexer and not pending delete, `CreatedBlock > 0` (created after the syncer recorded blocks; a pre-upgrade table is seeded Legacy and needs no declaration), `SchemaVersion == 0` (no declaration since this incarnation's `TableCreated`, by B8), and it was first seen at least 10 minutes ago in this process. The caller is the indexer signer, which the `Databases` contract accepts as a writer of every database the indexer hosts. The physical table is `<node.PhysicalDatabase>.` followed by `` `<database>.<table>` `` (the naming `DatabaseGC` drops), tried on each configured local replica in turn. Outcomes are `declared` and `failed`.
- **B10 — Config migration.** With `storage_integrity.enabled`, validation runs `migrateHousegateStorageIntegrity`: an explicit `housegate.storage_integrity.enabled: false` is an error; a legacy `tables` list equal to `snode.table_ids` as a set is dropped with a deprecation warning; any other non-empty list is an error; the switch is then forced on. It is idempotent because `Run` validates a loaded config a second time. The `table_ids ⊆ housegate tables` rule is removed; the D2 physical-name check stays.
- **B11 — Read state and runtime schemas.** The read state is always the `*snode.Role`; `newOwnedStorageIntegrityReadState` and its test are deleted (a partial journal can no longer exist, since HouseGate's table set is the registry's). `StorageIntegrityRuntimeOptions.TableSchemas` is no longer set: HouseGate reads it only for its static table set. `loadStorageIntegritySchemaSets` loads the genesis set only.
- **B12 — `snode.ErrTableNotReady`.** Plan A handoff item 4 marks it pre-write; the source preparer adds it to the terminal list next to `ErrSchemaHashMismatch`.
- **B13 — JSON-RPC shape.** The handler returns housegate's own `registry.TableStatus`, so field names cannot drift from the client that decodes them, and the wire test drives housegate's `network.RpcNetworkState` against the handler. Empty `database` or `table` is an error. `schema_json` is re-marshalled from the decoded schema; the agent hashes decoded fields, so byte equality with the registry's string is not required.
- **B14 — Metrics.** The four spec metrics, with the follower's connection state from `RegistryFollower.Connected()`, plus the registry-enabled flag and version. They register on `hg.MetricsRegistry()` right after `housegate.New`, or on the default registry when the collector is disabled (both are served by the same `/metrics` handler). Plan A's `Role.TableSetStats` is not exported here.
- **B15 — Acceptance emulation.** The lifecycle test drives the real follower from a fake `TableRegistry` gRPC server, the real table state, the real JSON-RPC handler and HouseGate's real agent client, and an embedded HouseGate against Keeper-enabled ClickHouse. It stands in for the reconciler with a readiness check that the three `hg_*` tables exist, and creates them with `ddl.EnsureProtocolTables` as the reconciler would; the Active step proves the signed lane engaged because the throttling SNode fake refuses the prepare with code 252.
- **B16 — Dependency set.** All four pins move in one commit (Task 4) because each alone breaks the build: sentio-node's `TableInfo.CreatedBlock` and `MergeGuard` fakes need the new sentio-core and housegate, and plan A's arbiter-core needs the new arbiter-proto. arbiter-core `$ARBITER_CORE_TAG` itself still pins housegate `db46c31` (plan A handoff item 1); Go's minimal version selection and the root `git_override` both resolve housegate to `$HOUSEGATE_TAG`, and the prototype compiled arbiter-core `d059aab` against `04fb8c0` without change. sentio-core's go.mod pin (today `fa19e1f`) and Bazel pin (today `d9474c3`) have drifted apart; both move to `$SENTIO_CORE_SHA`.

## File Structure

- **sentio-core:** modify `network/state/types.go`, `network/state/BUILD.bazel`; create `network/state/table_created_block_test.go` — Task 2.
- **Pins:** `go.mod`, `go.sum`, `MODULE.bazel` (and `MODULE.bazel.lock` if `bazel mod tidy` changes it) — Task 4 (and `go.mod`/`MODULE.bazel` again in Task 7 when `prometheus/client_golang` becomes direct).
- **Bump fallout:** modify `standalone/storage_integrity_schemas_test.go`, `standalone/storage_integrity_acceptance_ch_test.go`, `storageintegrityadapter/adapter.go`, `storageintegrityadapter/adapter_test.go` — Task 4.
- **Chain facts:** modify `handlers/database_event.go`, `handlers/database_event_test.go` — Task 5; `standalone/networkstate/redis.go`, create `standalone/networkstate/database_infos_test.go` — Task 6.
- **Table state:** create `storageintegrityadapter/tablestate/{model,snapshot,state,metrics,status,chain}.go` with `state_test.go`, `chain_test.go` — Task 7; `arbitercore.go`, `arbitercore_test.go` — Task 8.
- **JSON-RPC:** create `rpc/storage_integrity_status.go` and its test, modify `rpc/storage_service.go`, `rpc/server.go` — Task 9.
- **Config:** modify `config/config.go`, `config/config_test.go` — Task 10.
- **Declarations:** modify `database_registry/schema_declarer.go`, create `database_registry/schema_declarer_test.go` — Task 11; create `database_registry/compensation.go` and its test, modify `database_registry/schema_declarer.go`, `standalone/schema_registry.go` — Task 12.
- **Wiring:** create `standalone/storage_integrity_table_state.go` and its test; replace `standalone/storage_integrity_schemas.go` and `standalone/storage_integrity_schemas_test.go`; modify `standalone/standalone.go`, `standalone/startup_transaction.go`, `standalone/storage_integrity_bootstrap_test.go`; delete `standalone/storage_integrity_read_state.go` and its test — Task 13.
- **Acceptance:** create `standalone/storage_integrity_dynamic_ch_test.go`, modify `standalone/storage_integrity_acceptance_ch_test.go`, `.github/workflows/ci.yml` — Task 14.
- **Docs:** `README.md` — Task 15.
- Every new or changed package's `BUILD.bazel` comes from `bazel run //:gazelle` (sentio-core's single test-source addition is written by hand in Task 2).

---

## Task 1: Cut the housegate release (controller-only, gated)

The implementer does not run this task. sentio-node's `git_override` needs a housegate tag that contains `04fb8c0` (the `sitable` port, V2 contract and agent status client).

**Files:** none. **Interfaces:** Produces `$HOUSEGATE_TAG` and `$HOUSEGATE_SHA`.

- [ ] **Step 1: Confirm `main` is releasable.** In `/Users/uranuswch/Dev/housegate/housegate`: `git fetch origin && git merge-base --is-ancestor 04fb8c0 origin/main` exits 0, and the latest `main` CI run is green (`gh run list --repo housegate/housegate --branch main --workflow ci.yml --limit 1`).
- [ ] **Step 2: Cut the tag.** `gh workflow run release.yml --repo housegate/housegate --ref main -f bump=auto`, then `gh run watch --repo housegate/housegate "$(gh run list --repo housegate/housegate --workflow release.yml --limit 1 --json databaseId -q '.[0].databaseId')"`. The workflow computes the version (first cut on a UTC day bumps the minor, a later cut the same day the patch), pushes the tag, publishes binaries and the image, and chains the Homebrew sync; if only the Homebrew job fails, re-run that job alone (CLAUDE.md "CI").
- [ ] **Step 3: Verify the tag by content and record the pins.**

```bash
git fetch --tags origin
HOUSEGATE_TAG=$(git tag --list 'v*' --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -n 1)
git merge-base --is-ancestor 04fb8c0 "$HOUSEGATE_TAG"
git show "$HOUSEGATE_TAG":pkg/sitable/sitable.go | grep -q 'type TableState interface'
git show "$HOUSEGATE_TAG":pkg/network/rpc.go | grep -q 'sentio_getStorageIntegrityTableStatus'
HOUSEGATE_SHA=$(git rev-parse "$HOUSEGATE_TAG^{commit}")
echo "$HOUSEGATE_TAG $HOUSEGATE_SHA"
```

Expected: every command exits 0; record both values for Task 4. No commit.

## Task 2: sentio-core `TableInfo.CreatedBlock`

**Files:**

- Modify: `network/state/types.go:39-44` (`TableInfo`), `network/state/BUILD.bazel:26-28` (`state_test` srcs)
- Test: create `network/state/table_created_block_test.go`

**Interfaces:**

- Consumes: `PlainState.UpsertDatabaseTable` / `DeleteDatabaseTable`, `NewStateMirrored`, `statemirror.NewFileMirror`, `FileStore`.
- Produces: `TableInfo.CreatedBlock uint64` with tags `json:"createdBlock,omitempty" yaml:"created_block,omitempty"`. No migration: the Postgres store keeps `tables` as `jsonb`, the file store is YAML and the Redis mirror is JSON, so an old row decodes with zero, which design E5 defines as "created before the upgrade", and a zero field keeps the old bytes.

Work in `/Users/uranuswch/Dev/sentio_xyz/sentio-core` on branch `feat/table-created-block` from `origin/main`.

- [ ] **Step 1: Write the failing test.** Create `network/state/table_created_block_test.go`:

```go
package state

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"sentioxyz/sentio-core/common/statemirror"
)

func TestTableInfoCreatedBlockOverwrittenOnRecreate(t *testing.T) {
	ctx := context.Background()
	state := &PlainState{Databases: map[string]DatabaseInfo{"db_1": {DatabaseId: "db_1"}}}
	if err := state.UpsertDatabaseTable(ctx, "db_1", TableInfo{TableId: "t", TableType: "user", CreatedBlock: 100}); err != nil {
		t.Fatalf("UpsertDatabaseTable create: %v", err)
	}
	if err := state.DeleteDatabaseTable(ctx, "db_1", "t"); err != nil {
		t.Fatalf("DeleteDatabaseTable: %v", err)
	}
	if err := state.UpsertDatabaseTable(ctx, "db_1", TableInfo{TableId: "t", TableType: "user", CreatedBlock: 250}); err != nil {
		t.Fatalf("UpsertDatabaseTable recreate: %v", err)
	}
	tables := state.Databases["db_1"].Tables
	if len(tables) != 1 || tables[0].CreatedBlock != 250 {
		t.Fatalf("tables after recreate = %+v, want one table created at block 250", tables)
	}
}

func TestTableInfoCreatedBlockIsMirrored(t *testing.T) {
	ctx := context.Background()
	mirror, err := statemirror.NewFileMirror(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileMirror: %v", err)
	}
	state, err := NewStateMirrored(ctx, &PlainState{Databases: map[string]DatabaseInfo{"db_1": {DatabaseId: "db_1", IndexerId: 7}}}, mirror)
	if err != nil {
		t.Fatalf("NewStateMirrored: %v", err)
	}
	if err := state.UpsertDatabaseTable(ctx, "db_1", TableInfo{TableId: "t", TableType: "user", CreatedBlock: 4242}); err != nil {
		t.Fatalf("UpsertDatabaseTable: %v", err)
	}
	raw, ok, err := mirror.Get(ctx, statemirror.MappingDatabases, "db_1")
	if err != nil || !ok {
		t.Fatalf("mirror.Get(db_1) = ok %v, err %v", ok, err)
	}
	var got DatabaseInfo
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("decode mirrored database: %v", err)
	}
	if len(got.Tables) != 1 || got.Tables[0].CreatedBlock != 4242 {
		t.Fatalf("mirrored tables = %+v, want createdBlock 4242", got.Tables)
	}
}

func TestTableInfoWithoutCreatedBlockDecodesAsZero(t *testing.T) {
	// A mirror or store row written before the upgrade has no createdBlock
	// field; zero means "created before the upgrade" (design E5).
	var got DatabaseInfo
	if err := json.Unmarshal([]byte(`{"databaseId":"db_1","indexerId":7,"tables":[{"tableId":"t","tableType":"user"}]}`), &got); err != nil {
		t.Fatalf("decode legacy database: %v", err)
	}
	if got.Tables[0].CreatedBlock != 0 {
		t.Fatalf("legacy createdBlock = %d, want 0", got.Tables[0].CreatedBlock)
	}
	encoded, err := json.Marshal(TableInfo{TableId: "t", TableType: "user"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if string(encoded) != `{"tableId":"t","tableType":"user"}` {
		t.Fatalf("zero createdBlock must be omitted to keep legacy bytes, got %s", encoded)
	}
}

func TestFileStoreTableCreatedBlockRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := NewFileStore(filepath.Join(t.TempDir(), "state.yaml"))
	want := &PlainState{Databases: map[string]DatabaseInfo{
		"db_1": {DatabaseId: "db_1", Tables: []TableInfo{{TableId: "t", TableType: "user", CreatedBlock: 99}}},
	}}
	if err := store.Save(ctx, want); err != nil {
		t.Fatalf("FileStore.Save: %v", err)
	}
	got, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("FileStore.Load: %v", err)
	}
	if block := got.Databases["db_1"].Tables[0].CreatedBlock; block != 99 {
		t.Fatalf("loaded createdBlock = %d, want 99", block)
	}
}
```

- [ ] **Step 2: Run it and verify it fails.** `go test ./network/state/ -run CreatedBlock`. Expected: build failure, `unknown field CreatedBlock in struct literal of type TableInfo` and `has no field or method CreatedBlock`.
- [ ] **Step 3: Add the field and the test source.**

`network/state/types.go`, replace:

```go
type TableInfo struct {
	TableId       string `json:"tableId" yaml:"table_id"`
	TableType     string `json:"tableType" yaml:"table_type"`
	SchemaVersion uint32 `json:"schemaVersion,omitempty" yaml:"schema_version,omitempty"`
	SchemaHash    string `json:"schemaHash,omitempty" yaml:"schema_hash,omitempty"`
}
```

with:

```go
type TableInfo struct {
	TableId       string `json:"tableId" yaml:"table_id"`
	TableType     string `json:"tableType" yaml:"table_type"`
	SchemaVersion uint32 `json:"schemaVersion,omitempty" yaml:"schema_version,omitempty"`
	SchemaHash    string `json:"schemaHash,omitempty" yaml:"schema_hash,omitempty"`
	// CreatedBlock is the L2 block of the TableCreated event that created this
	// incarnation of the table; a recreation overwrites it. Zero means the
	// table was created before the syncer recorded creation blocks, which the
	// storage-integrity activation window treats as created before
	// activation_block.
	CreatedBlock uint64 `json:"createdBlock,omitempty" yaml:"created_block,omitempty"`
}
```

`network/state/BUILD.bazel`, replace:

```starlark
        "state_test.go",
        "store_postgres_test.go",
    ],
```

with:

```starlark
        "state_test.go",
        "store_postgres_test.go",
        "table_created_block_test.go",
    ],
```

- [ ] **Step 4: Run the package.** `go test ./network/state/` then `bazel test //network/state:state_test`. Expected: `ok` and `PASSED`.
- [ ] **Step 5: Commit and open the PR.**

```bash
git add network/state/types.go network/state/BUILD.bazel network/state/table_created_block_test.go
git commit -F - <<'EOF'
feat(state): record the TableCreated block on TableInfo

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

Push the branch and open a PR against `main`; the controller merges it in Task 3.

## Task 3: Merge sentio-core and collect the pins (controller-only, gated)

**Files:** none. **Interfaces:** Produces `$SENTIO_CORE_SHA`, `$SENTIO_CORE_VERSION`; confirms plan A's `$ARBITER_CORE_TAG`, `$ARBITER_CORE_SHA`, `$ARBITER_PROTO_PSEUDO`.

- [ ] **Step 1: Merge the Task 2 PR** after review and a green sentio-core CI.
- [ ] **Step 2: Record the sentio-core pins.**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-core && git fetch origin
SENTIO_CORE_SHA=$(git log -1 --format=%H --grep='record the TableCreated block on TableInfo' origin/main)
git show "$SENTIO_CORE_SHA":network/state/types.go | grep -q 'CreatedBlock uint64'
SENTIO_CORE_VERSION="v0.0.0-$(TZ=UTC git log -1 --date=format-local:%Y%m%d%H%M%S --format=%cd "$SENTIO_CORE_SHA")-$(git rev-parse --short=12 "$SENTIO_CORE_SHA")"
echo "$SENTIO_CORE_SHA $SENTIO_CORE_VERSION"
```

A later `main` commit may be pinned instead as long as the `grep` passes on it.

- [ ] **Step 3: Confirm plan A's releases** (plan A's Task 12 cuts them and its gate records the tags): `git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core rev-parse "$ARBITER_CORE_TAG^{commit}"` equals `$ARBITER_CORE_SHA`; `go list -m github.com/sentioxyz/arbiter-proto@$ARBITER_PROTO_PSEUDO` resolves; and `git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core grep -nE 'func (NewRegistryFollower|WaitReady)|func \([a-z]+ \*RegistryFollower\) (Run|View|Changed|Ready|Connected)|func \([a-z]+ \*Role\) TableReady|ErrTableNotReady|Registry +dataplane.RegistryView' "$ARBITER_CORE_TAG" -- dataplane snode` lists every name in Global Constraints' plan A interface. If a name differs from plan A's handoff, correct it in this plan's Tasks 4, 8, 13 and 14 before dispatching them; they are the only places that name it.

## Task 4: Bump housegate, arbiter-core, arbiter-proto and sentio-core together

**Files:**

- Modify: `go.mod`, `go.sum`, `MODULE.bazel:12-19` (`bazel_dep` versions), `MODULE.bazel:23-41` (`git_override` commits), `MODULE.bazel.lock` if `bazel mod tidy` rewrites it
- Modify: `standalone/storage_integrity_schemas_test.go:135-144, 195`, `standalone/storage_integrity_acceptance_ch_test.go:272, 277, 298-301, 310`, `storageintegrityadapter/adapter.go:65-66`
- Test: `storageintegrityadapter/adapter_test.go:162`

**Interfaces:**

- Consumes: housegate `StorageIntegrityMergeGuard.AssertTables(ctx, activeTableIDs []string) (sicore.MergeGuardReport, error)` (replaces `AssertStopMerges`; an Active table missing from `report.Tables` counts as not asserted), `rewriter.StorageIntegrityContractV2`; arbiter-core `snode.ErrTableNotReady`.
- Produces: the pinned dependency set every later task builds on; `SourcePreparer.PrepareLocalStatement` maps `snode.ErrTableNotReady` to `sicore.ErrPrepareTerminalReject`.

Work in `/Users/uranuswch/Dev/sentio_xyz/sentio-node` on a new branch `feat/si-dynamic-table-state` from `origin/main`.

- [ ] **Step 1: Move the Go pins.**

```bash
go mod edit -require=sentioxyz/sentio-core@$SENTIO_CORE_VERSION -replace=sentioxyz/sentio-core=github.com/sentioxyz/sentio-core@$SENTIO_CORE_VERSION
go get github.com/housegate/housegate@$HOUSEGATE_TAG github.com/sentioxyz/arbiter-core@$ARBITER_CORE_TAG github.com/sentioxyz/arbiter-proto@$ARBITER_PROTO_PSEUDO
go mod tidy
grep -nE 'housegate/housegate |arbiter-core |arbiter-proto |sentio-core ' go.mod
```

Expected: the four modules at the new versions (the `replace` line for sentio-core too). Expect a wide transitive diff (`rewriter-go`, `rewriter-proto`, polyglot); do not pin transitives back.

- [ ] **Step 2: Move the Bazel pins.** In `MODULE.bazel` set `bazel_dep(name = "arbiter_core", version = ...)` to `$ARBITER_CORE_TAG` without its `v`, `bazel_dep(name = "housegate", version = ...)` to `$HOUSEGATE_TAG` without its `v`, the `housegate` `git_override` comment to `# Resolved Housegate $HOUSEGATE_TAG; source is pinned by the commit below.` and its `commit` to `$HOUSEGATE_SHA`, and the `arbiter_core` comment and `commit` likewise to `$ARBITER_CORE_TAG` / `$ARBITER_CORE_SHA`. Then:

```bash
./scripts/update-sentio-core.sh "$SENTIO_CORE_SHA"
bazel mod tidy
git -C /Users/uranuswch/Dev/housegate/housegate rev-parse "$HOUSEGATE_TAG^{commit}"
git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core rev-parse "$ARBITER_CORE_TAG^{commit}"
grep -n -A3 'git_override' MODULE.bazel
```

Expected: each `rev-parse` equals the matching `git_override` commit, and the `sentio-core` override is `$SENTIO_CORE_SHA` (upgrade-dependency skill: a `go.mod`-only bump leaves Bazel compiling the old sources).

- [ ] **Step 3: Verify the fallout fails to compile.** `go vet ./standalone/`. Expected: `acceptanceMergeGuard does not implement housegate.StorageIntegrityMergeGuard (missing method AssertTables)`.
- [ ] **Step 4: Move the two MergeGuard fakes and the two rewriter fakes.**

`standalone/storage_integrity_schemas_test.go` edit 1 of 2, replace:

```go
	return rewriter.StorageIntegrityContractV1
}

// ProbeStorageIntegrityBuild satisfies rewriter.StorageIntegrityProbeFactory.
// Spec I made contract v1 alone insufficient: HouseGate now refuses startup for
// a configured storage_integrity.tables set unless the factory also exposes the
// behavioural conformance probe, because an old engine can acknowledge the
// contract while missing the fail-closed behaviour. This fixture is not an
// engine, so the probe is a no-op; real engine conformance is covered by
// HouseGate's and rewriter-go's own suites.
```

with:

```go
	return rewriter.StorageIntegrityContractV2
}

// ProbeStorageIntegrityBuild satisfies rewriter.StorageIntegrityProbeFactory.
// HouseGate refuses startup for an enabled storage-integrity surface unless the
// factory acknowledges contract V2 and exposes the behavioural conformance
// probe, because an old engine can acknowledge the contract while missing the
// fail-closed behaviour. This fixture is not an engine, so the probe is a
// no-op; real engine conformance is covered by HouseGate's and rewriter-go's
// own suites.
```

`standalone/storage_integrity_schemas_test.go` edit 2 of 2, replace:

```go
func (storageIntegrityRuntimePorts) AssertStopMerges(context.Context) error { return nil }
```

with:

```go
// AssertTables reports every Active table healthy; HouseGate treats an Active
// table missing from the report as not yet asserted and refuses its admissions.
func (storageIntegrityRuntimePorts) AssertTables(_ context.Context, activeTableIDs []string) (sicore.MergeGuardReport, error) {
	report := sicore.MergeGuardReport{Tables: make(map[string]error, len(activeTableIDs))}
	for _, id := range activeTableIDs {
		report.Tables[id] = nil
	}
	return report, nil
}
```

`standalone/storage_integrity_acceptance_ch_test.go` edit 1 of 4, replace:

```go
func (acceptanceMergeGuard) AssertStopMerges(context.Context) error { return nil }
```

with:

```go
// AssertTables reports every Active table healthy; HouseGate treats an Active
// table missing from the report as not yet asserted and refuses its admissions.
func (acceptanceMergeGuard) AssertTables(_ context.Context, activeTableIDs []string) (sicore.MergeGuardReport, error) {
	report := sicore.MergeGuardReport{Tables: make(map[string]error, len(activeTableIDs))}
	for _, id := range activeTableIDs {
		report.Tables[id] = nil
	}
	return report, nil
}
```

`standalone/storage_integrity_acceptance_ch_test.go` edit 2 of 4, replace:

```go
// statement untouched, and acknowledges contract v1 on every call.
```

with:

```go
// statement untouched, and acknowledges contract V2 on every call.
```

`standalone/storage_integrity_acceptance_ch_test.go` edit 3 of 4, replace:

```go
// table set proves it speaks contract v1 and passes the behavioral probe.
func (f *storageIntegrityAcceptanceRewriter) StorageIntegrityContractVersion() rewriterpb.StorageIntegrityContractVersion {
	return rewriterpb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1
}
```

with:

```go
// table set proves it speaks contract V2 and passes the behavioral probe.
func (f *storageIntegrityAcceptanceRewriter) StorageIntegrityContractVersion() rewriterpb.StorageIntegrityContractVersion {
	return rewriterpb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2
}
```

`standalone/storage_integrity_acceptance_ch_test.go` edit 4 of 4, replace:

```go
		StorageIntegrityContractVersion: rewriterpb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1,
```

with:

```go
		StorageIntegrityContractVersion: rewriterpb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2,
```

- [ ] **Step 5: Run the fast suite.** `go vet ./... && go test ./standalone/ ./storageintegrityadapter/`. Expected: `ok` for both (`TestHousegateNewHandlesSNodeSchemaSubsetReadState` still passes under the new housegate; Task 13 replaces it).
- [ ] **Step 6: Write the failing classification row.**

`storageintegrityadapter/adapter_test.go`, replace:

```go
		{snode.ErrSchemaHashMismatch, true, "pre-write: refused before the journal record exists"},
```

with:

```go
		{snode.ErrSchemaHashMismatch, true, "pre-write: refused before the journal record exists"},
		{snode.ErrTableNotReady, true, "pre-write: the source has not made the table Ready, so nothing was recorded or written"},
```

- [ ] **Step 7: Run it and verify it fails.** `go test ./storageintegrityadapter/ -run TestPrepareErrorClassificationPreservesSentinels`. Expected: FAIL, `terminal classification for snode: ... (pre-write: the source has not made the table Ready, so nothing was recorded or written)`, expected `true`, actual `false`.
- [ ] **Step 8: Classify `ErrTableNotReady` as pre-write terminal.**

`storageintegrityadapter/adapter.go`, replace:

```go
		if errors.Is(err, snode.ErrSchemaHashMismatch) ||
			errors.Is(err, snode.ErrEncodingNotSupported) ||
```

with:

```go
		if errors.Is(err, snode.ErrSchemaHashMismatch) ||
			errors.Is(err, snode.ErrTableNotReady) ||
			errors.Is(err, snode.ErrEncodingNotSupported) ||
```

- [ ] **Step 9: Run everything.** `go test ./...`, then `bazel run //:gazelle && bazel build //... && bazel test //...`, then the docker acceptance: `SENTIO_SI_CH_E2E=1 CH_ADDR=$CH_ADDR go test ./standalone/ -run 'TestStorageIntegrity(MalformedColumnTypeCreatesNoTable|BackpressureKeepsTheEmbeddedProxySession|ProtocolTableDriftFailsBootstrap)' -v`. Expected: all `ok`/`PASSED`, and three `--- PASS` lines.
- [ ] **Step 10: Commit.**

```bash
git add go.mod go.sum MODULE.bazel MODULE.bazel.lock standalone/storage_integrity_schemas_test.go standalone/storage_integrity_acceptance_ch_test.go storageintegrityadapter/adapter.go storageintegrityadapter/adapter_test.go
git commit -F - <<'EOF'
chore(deps): bump housegate, arbiter-core, arbiter-proto and sentio-core for the dynamic SI table set

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

Add the version table (old → new for the four modules, both pin mechanisms) to the PR description at the end.

## Task 5: Record `CreatedBlock` on `TableCreated` and start each incarnation without a schema pointer

**Files:**

- Modify: `handlers/database_event.go:50` (dispatch), `handlers/database_event.go:146-164` (`onTableCreated`)
- Test: `handlers/database_event_test.go:12-14` (imports), `:101-143` (replace `TestDatabaseEventHandlerTableCreatedBackfillsLatestSchemaPointer`), append two log helpers

**Interfaces:**

- Consumes: `types.Log.BlockNumber`, `statecore.TableInfo.CreatedBlock` (Task 4's sentio-core).
- Produces: `func (h *DatabaseEventHandler) onTableCreated(ctx context.Context, decoded *bindings.IDatabasesTableCreated, blockNumber uint64) error`. A (re)created table's `TableInfo` is `{TableId, TableType, CreatedBlock}` with `SchemaVersion == 0` until a `TableSchemaSet` of this incarnation arrives (B8); Task 12 reads that zero.

- [ ] **Step 1: Write the failing test.** In `handlers/database_event_test.go`, delete the whole function `TestDatabaseEventHandlerTableCreatedBackfillsLatestSchemaPointer` (lines 101–143, up to the blank line before `func newDatabaseEventHandlerForTest`) and put this in its place:

```go
// TestDatabaseEventHandlerTableCreatedRecordsBlockAndStartsWithoutSchema pins
// design 2026-09-25 §9.1 and §9.5: TableCreated records its block, and a
// recreation starts with no schema pointer even when earlier incarnations
// declared schemas, because the registry reads only the first declaration
// after the TableCreated (umbrella D3). The compensation loop reads a zero
// SchemaVersion as "no declaration since this incarnation was created".
func TestDatabaseEventHandlerTableCreatedRecordsBlockAndStartsWithoutSchema(t *testing.T) {
	state := &statecore.PlainState{
		Databases: map[string]statecore.DatabaseInfo{
			"db_1": {DatabaseId: "db_1"},
		},
		TableSchemas: map[string]statecore.TableSchemaInfo{
			statecore.TableSchemaKey("db_1", "table_1", 3): {
				DatabaseId: "db_1",
				TableId:    "table_1",
				Version:    3,
				SchemaHash: "0x03",
			},
		},
	}
	handler := newDatabaseEventHandlerForTest(t, state)
	ctx := context.Background()

	require.NoError(t, handler.Handle(ctx, tableCreatedLog(t, "db_1", "table_1", "ANALYTIC", 120)))
	table := state.Databases["db_1"].Tables[0]
	require.Equal(t, statecore.TableInfo{TableId: "table_1", TableType: "ANALYTIC", CreatedBlock: 120}, table,
		"a creation must not inherit an earlier incarnation's schema pointer")

	require.NoError(t, handler.Handle(ctx, tableSchemaSetLog(t, "db_1", "table_1", 4, [32]byte{4}, `{"table_id":"db_1.table_1"}`)))
	require.Equal(t, uint32(4), state.Databases["db_1"].Tables[0].SchemaVersion)
	require.Equal(t, uint64(120), state.Databases["db_1"].Tables[0].CreatedBlock, "a declaration keeps the creation block")

	require.NoError(t, handler.Handle(ctx, tableDeletedLog(t, "db_1", "table_1")))
	require.NoError(t, handler.Handle(ctx, tableCreatedLog(t, "db_1", "table_1", "ANALYTIC", 450)))
	require.Equal(t, []statecore.TableInfo{{TableId: "table_1", TableType: "ANALYTIC", CreatedBlock: 450}}, state.Databases["db_1"].Tables,
		"a recreation overwrites the block and starts without a schema pointer")
}
```

Append these helpers at the end of the file:

```go
func tableCreatedLog(t *testing.T, databaseID, tableID, tableType string, block uint64) types.Log {
	t.Helper()
	contractABI, err := bindings.IDatabasesMetaData.GetAbi()
	require.NoError(t, err)
	event := contractABI.Events["TableCreated"]
	data, err := event.Inputs.NonIndexed().Pack(tableID, databaseID, tableType)
	require.NoError(t, err)
	return types.Log{Topics: []ethcommon.Hash{event.ID}, Data: data, BlockNumber: block}
}

func tableDeletedLog(t *testing.T, databaseID, tableID string) types.Log {
	t.Helper()
	contractABI, err := bindings.IDatabasesMetaData.GetAbi()
	require.NoError(t, err)
	event := contractABI.Events["TableDeleted"]
	data, err := event.Inputs.NonIndexed().Pack(databaseID, tableID)
	require.NoError(t, err)
	return types.Log{Topics: []ethcommon.Hash{event.ID}, Data: data}
}
```

`handlers/database_event_test.go`, replace:

```go
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)
```

with:

```go
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
)
```

- [ ] **Step 2: Run it and verify it fails.** `go test ./handlers/ -run TableCreated`. Expected: FAIL with `a creation must not inherit an earlier incarnation's schema pointer` (actual `SchemaVersion: 3, SchemaHash: "0x03", CreatedBlock: 0`).
- [ ] **Step 3: Implement.**

`handlers/database_event.go` edit 1 of 2, replace:

```go
		return h.onTableCreated(ctx, decoded)
```

with:

```go
		return h.onTableCreated(ctx, decoded, l.BlockNumber)
```

`handlers/database_event.go` edit 2 of 2, replace:

```go
func (h *DatabaseEventHandler) onTableCreated(ctx context.Context, decoded *bindings.IDatabasesTableCreated) error {
	logger := log.WithContext(ctx)
	if _, ok := h.hctx.State.GetDatabase(decoded.DatabaseId); !ok {
		logger.Warnf("skipping TableCreated: database %s not in state", decoded.DatabaseId)
		return nil
	}
	table := statecore.TableInfo{
		TableId:   decoded.TableId,
		TableType: decoded.TableType,
	}
	for _, schema := range h.hctx.State.GetTableSchemas() {
		if schema.DatabaseId != decoded.DatabaseId ||
			schema.TableId != decoded.TableId ||
			schema.Version <= table.SchemaVersion {
			continue
		}
		table.SchemaVersion = schema.Version
		table.SchemaHash = schema.SchemaHash
	}
```

with:

```go
// onTableCreated records a table incarnation and its creation block. The
// schema pointer starts empty even when an earlier incarnation of the same name
// declared schemas: the storage-integrity registry takes only the first
// declaration after this TableCreated (umbrella D3), so an older declaration is
// not this incarnation's, and a zero SchemaVersion is what the declaration
// compensation loop reads as "not declared yet".
func (h *DatabaseEventHandler) onTableCreated(ctx context.Context, decoded *bindings.IDatabasesTableCreated, blockNumber uint64) error {
	logger := log.WithContext(ctx)
	if _, ok := h.hctx.State.GetDatabase(decoded.DatabaseId); !ok {
		logger.Warnf("skipping TableCreated: database %s not in state", decoded.DatabaseId)
		return nil
	}
	table := statecore.TableInfo{
		TableId:      decoded.TableId,
		TableType:    decoded.TableType,
		CreatedBlock: blockNumber,
	}
```

- [ ] **Step 4: Run the package.** `go test ./handlers/` and `bazel test //handlers:handlers_test`. Expected: `ok` / `PASSED`.
- [ ] **Step 5: Commit.**

```bash
git add handlers/database_event.go handlers/database_event_test.go
git commit -F - <<'EOF'
feat(sync): record TableCreated blocks and start each incarnation without a schema pointer

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 6: Read the mirrored chain state with creation blocks

**Files:**

- Modify: `standalone/networkstate/redis.go:125` (insert before `// --- registry.Access`)
- Test: create `standalone/networkstate/database_infos_test.go`

**Interfaces:**

- Consumes: `statemirror.Mirror.GetAll(ctx, statemirror.MappingDatabases)`.
- Produces: `func (r *RedisNetworkState) DatabaseInfos(ctx context.Context) (map[string]statecore.DatabaseInfo, error)` — every database with its tables and their `CreatedBlock`; an undecodable entry is an error (Review Focus 5). It satisfies Task 7's `tablestate.DatabaseInfoReader` and Task 12's `database_registry.CompensationChain`.

- [ ] **Step 1: Write the failing test.** Create `standalone/networkstate/database_infos_test.go`:

```go
package networkstate

import (
	"context"
	"testing"

	"sentioxyz/sentio-core/common/statemirror"
	statecore "sentioxyz/sentio-core/network/state"

	"github.com/stretchr/testify/require"
)

func TestRedisNetworkStateDatabaseInfosCarriesCreatedBlock(t *testing.T) {
	ctx := context.Background()
	mirror, err := statemirror.NewFileMirror(t.TempDir())
	require.NoError(t, err)
	_, err = statecore.NewStateMirrored(ctx, &statecore.PlainState{Databases: map[string]statecore.DatabaseInfo{
		"tenant": {DatabaseId: "tenant", IndexerId: 7, Tables: []statecore.TableInfo{
			{TableId: "t", TableType: "user", CreatedBlock: 1500, SchemaVersion: 2, SchemaHash: "0x02"},
			{TableId: "old", TableType: "user"},
		}},
	}}, mirror)
	require.NoError(t, err)

	got, err := (&RedisNetworkState{mirror: mirror}).DatabaseInfos(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]statecore.DatabaseInfo{
		"tenant": {DatabaseId: "tenant", IndexerId: 7, Tables: []statecore.TableInfo{
			{TableId: "t", TableType: "user", CreatedBlock: 1500, SchemaVersion: 2, SchemaHash: "0x02"},
			{TableId: "old", TableType: "user"},
		}},
	}, got)
}

func TestRedisNetworkStateDatabaseInfosRejectsUndecodableEntries(t *testing.T) {
	ctx := context.Background()
	mirror, err := statemirror.NewFileMirror(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, mirror.Upsert(ctx, statemirror.MappingDatabases, func(context.Context, statemirror.OnChainKey) (map[string]string, error) {
		return map[string]string{"tenant": "{not json"}, nil
	}))
	_, err = (&RedisNetworkState{mirror: mirror}).DatabaseInfos(ctx)
	require.ErrorContains(t, err, `database "tenant"`, "a table-state build must fail rather than silently drop a governed database")
}
```

- [ ] **Step 2: Run it and verify it fails.** `go test ./standalone/networkstate/`. Expected: build failure, `DatabaseInfos undefined (type *RedisNetworkState has no field or method DatabaseInfos)`.
- [ ] **Step 3: Implement.**

`standalone/networkstate/redis.go`, replace:

```go
// --- registry.Access
```

with:

```go
// DatabaseInfos reads every mirrored database with its tables, including
// each table's CreatedBlock. Unlike All it fails on an undecodable entry: the
// storage-integrity table state must not build a snapshot in which a governed
// database silently disappears (its tables would read as Ordinary).
func (r *RedisNetworkState) DatabaseInfos(ctx context.Context) (map[string]statecore.DatabaseInfo, error) {
	all, err := r.mirror.GetAll(ctx, statemirror.MappingDatabases)
	if err != nil {
		return nil, fmt.Errorf("read mirrored databases: %w", err)
	}
	result := make(map[string]statecore.DatabaseInfo, len(all))
	for field, value := range all {
		var info statecore.DatabaseInfo
		if err := json.Unmarshal([]byte(value), &info); err != nil {
			return nil, fmt.Errorf("decode mirrored database %q: %w", field, err)
		}
		result[field] = info
	}
	return result, nil
}

// --- registry.Access
```

- [ ] **Step 4: Run the package.** `go test ./standalone/networkstate/`, `bazel run //:gazelle`, `bazel test //standalone/networkstate:networkstate_test`. Expected: `ok` / `PASSED`.
- [ ] **Step 5: Commit.**

```bash
git add standalone/networkstate/redis.go standalone/networkstate/database_infos_test.go standalone/networkstate/BUILD.bazel
git commit -F - <<'EOF'
feat(standalone): read mirrored databases with their creation blocks

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 7: The table state: model, snapshot builder, refresh loop, status and metrics

**Files:**

- Create: `storageintegrityadapter/tablestate/model.go`, `snapshot.go`, `state.go`, `metrics.go`, `status.go`, `chain.go`, `BUILD.bazel` (gazelle)
- Test: create `storageintegrityadapter/tablestate/state_test.go`, `chain_test.go`

**Interfaces:**

- Consumes: housegate `sitable.Status`/`Table`/`Snapshot`/`TableState`/`TableID`, `payloadexec.TableSchemaHash`, `payloadexec.DecodeTableSchemaJSON`, `registry.TableStatus`; sentio-core `statecore.DatabaseInfo`; Prometheus.
- Produces (package `tablestate`, import path `compute-network-node/storageintegrityadapter/tablestate`):

```go
type IncarnationStatus uint8 // IncarnationLegacy, IncarnationPending, IncarnationRefused, IncarnationActive, IncarnationRetiring, IncarnationPurging, IncarnationPurged
type IncarnationOrigin uint8 // OriginGenesis, OriginLegacy, OriginChain
type Incarnation struct { Seq uint64; DatabaseID, TableID string; Origin IncarnationOrigin; Status IncarnationStatus; SchemaJSON, SchemaHash, RefusedCode, RefusedReason string }
func (i Incarnation) Key() string
type Registry struct { Version uint64; Seeded bool; SIIndexerID, ActivationBlock uint64; Incarnations []Incarnation }
func (r Registry) Live(key string) *Incarnation
type ChainTable struct { CreatedBlock uint64 }
type ChainDatabase struct { IndexerID uint64; Tables map[string]ChainTable }
type RegistrySource interface { View() (Registry, bool); Changed() <-chan struct{}; Connected() bool }
type Readiness interface { Ready(tableID string) bool }
type ChainSource interface { Databases(ctx context.Context) (map[string]ChainDatabase, error) }
type DatabaseInfoReader interface { DatabaseInfos(ctx context.Context) (map[string]statecore.DatabaseInfo, error) }
func DatabaseInfoChain(reader DatabaseInfoReader) ChainSource
const DefaultRefreshInterval = time.Second
type Config struct { Registry RegistrySource; Readiness Readiness; Chain ChainSource; Genesis []payloadexec.TableSchema; NetworkID string; RefreshInterval time.Duration; Metrics *Metrics; Logger *slog.Logger; Now func() time.Time }
func New(cfg Config) (*State, error)
func (s *State) Current() sitable.Snapshot
func (s *State) Changed() <-chan struct{}
func (s *State) Refresh(ctx context.Context) error
func (s *State) Run(ctx context.Context) error // returns ctx.Err()
func (s *State) StorageIntegrityTableStatus(database, table string) registry.TableStatus
type Snapshot struct{ /* unexported */ } // implements sitable.Snapshot
func NewMetrics() *Metrics
func (m *Metrics) Register(reg prometheus.Registerer) error
func (m *Metrics) ObserveCompensation(outcome string)
```

`Lookup` answers in design §9.2's order: registry disabled → the genesis set is Active, everything else Ordinary; database not on `si_indexer_id` or unknown → Ordinary; live incarnation (`Live`) Legacy → Ordinary, Pending → Pending, Refused → Refused with code and reason, Active → Active only when `Ready` and the schema passes the hash self-check, else Pending; Retiring/Purging → Gone with the schema; Purged or no incarnation → Ordinary when the seed has not committed and the chain state shows `CreatedBlock < activation_block`, else Pending (B2, B4, B5).

- [ ] **Step 1: Write the failing tests.** Create `storageintegrityadapter/tablestate/state_test.go`:

```go
package tablestate

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	testNetwork   = "devnet2"
	siIndexer     = uint64(7)
	otherIndexer  = uint64(9)
	activation    = uint64(1000)
	siDatabase    = "tenant"
	otherDatabase = "elsewhere"
)

type fakeRegistry struct {
	mu      sync.Mutex
	view    Registry
	enabled bool
	changed chan struct{}
}

func newFakeRegistry(enabled bool, view Registry) *fakeRegistry {
	return &fakeRegistry{view: view, enabled: enabled, changed: make(chan struct{})}
}

func (f *fakeRegistry) View() (Registry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.view, f.enabled
}

func (f *fakeRegistry) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed
}

func (f *fakeRegistry) Connected() bool { return true }

func (f *fakeRegistry) set(enabled bool, view Registry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.view, f.enabled = view, enabled
	close(f.changed)
	f.changed = make(chan struct{})
}

type fakeReadiness struct {
	mu    sync.Mutex
	ready map[string]bool
}

func (f *fakeReadiness) Ready(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ready[id]
}

func (f *fakeReadiness) set(id string, ready bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ready == nil {
		f.ready = map[string]bool{}
	}
	f.ready[id] = ready
}

type fakeChain struct {
	mu  sync.Mutex
	dbs map[string]ChainDatabase
	err error
}

func (f *fakeChain) Databases(context.Context) (map[string]ChainDatabase, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dbs, f.err
}

func (f *fakeChain) set(dbs map[string]ChainDatabase) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dbs = dbs
}

func schemaFor(id string) payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: id, PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}}
}

func schemaJSON(t *testing.T, schema payloadexec.TableSchema) string {
	t.Helper()
	raw, err := json.Marshal(schema)
	require.NoError(t, err)
	return string(raw)
}

func chainInc(t *testing.T, seq uint64, table string, status IncarnationStatus) Incarnation {
	schema := schemaFor(siDatabase + "." + table)
	return Incarnation{
		Seq: seq, DatabaseID: siDatabase, TableID: table, Origin: OriginChain, Status: status,
		SchemaJSON: schemaJSON(t, schema), SchemaHash: payloadexec.TableSchemaHash(testNetwork, schema),
	}
}

func chainState(tables map[string]uint64) map[string]ChainDatabase {
	si := ChainDatabase{IndexerID: siIndexer, Tables: map[string]ChainTable{}}
	for table, created := range tables {
		si.Tables[table] = ChainTable{CreatedBlock: created}
	}
	return map[string]ChainDatabase{
		siDatabase:    si,
		otherDatabase: {IndexerID: otherIndexer, Tables: map[string]ChainTable{"x": {CreatedBlock: 5000}}},
	}
}

func newTestState(t *testing.T, reg *fakeRegistry, ready *fakeReadiness, chain *fakeChain, genesis ...payloadexec.TableSchema) *State {
	t.Helper()
	state, err := New(Config{Registry: reg, Readiness: ready, Chain: chain, Genesis: genesis, NetworkID: testNetwork})
	require.NoError(t, err)
	return state
}

func TestLookupMatrix(t *testing.T) {
	genesis := schemaFor(siDatabase + ".genesis_t")
	genesisInc := Incarnation{Seq: 1, DatabaseID: siDatabase, TableID: "genesis_t", Origin: OriginGenesis, Status: IncarnationActive,
		SchemaHash: payloadexec.TableSchemaHash(testNetwork, genesis)}
	refused := chainInc(t, 0, "refused_t", IncarnationRefused)
	refused.RefusedCode, refused.RefusedReason = "column_type", `column "v" has unsupported type "Decimal(18, 4)"`
	badHash := chainInc(t, 0, "bad_hash", IncarnationActive)
	badHash.SchemaHash = "0xdeadbeef"
	incs := []Incarnation{
		genesisInc,
		{Seq: 2, DatabaseID: siDatabase, TableID: "legacy_t", Origin: OriginLegacy, Status: IncarnationLegacy},
		chainInc(t, 3, "pending_t", IncarnationPending),
		refused,
		chainInc(t, 5, "active_ready", IncarnationActive),
		chainInc(t, 6, "active_unready", IncarnationActive),
		chainInc(t, 7, "retiring_t", IncarnationRetiring),
		chainInc(t, 8, "purging_t", IncarnationPurging),
		chainInc(t, 9, "purged_old", IncarnationPurged),
		chainInc(t, 10, "purged_new", IncarnationPurged),
		badHash,
		chainInc(t, 12, "reused", IncarnationPurged),
		chainInc(t, 13, "reused", IncarnationActive),
		chainInc(t, 14, "shadowed", IncarnationActive),
		chainInc(t, 15, "shadowed", IncarnationRefused),
	}
	for i := range incs {
		incs[i].Seq = uint64(i + 1)
	}
	chainTables := map[string]uint64{
		"genesis_t": 0, "legacy_t": 0, "pending_t": 1500, "refused_t": 1500, "active_ready": 1500,
		"active_unready": 1500, "retiring_t": 1500, "purging_t": 1500, "purged_old": 400, "purged_new": 1500,
		"bad_hash": 1500, "reused": 1500, "shadowed": 1500, "unrecorded_old": 400, "unrecorded_zero": 0,
		"unrecorded_new": 1500,
	}
	ready := &fakeReadiness{}
	for _, id := range []string{"genesis_t", "active_ready", "bad_hash", "reused", "shadowed"} {
		ready.set(siDatabase+"."+id, true)
	}

	for _, seeded := range []bool{false, true} {
		reg := newFakeRegistry(true, Registry{Version: 42, Seeded: seeded, SIIndexerID: siIndexer, ActivationBlock: activation, Incarnations: incs})
		state := newTestState(t, reg, ready, &fakeChain{dbs: chainState(chainTables)}, genesis)
		require.NoError(t, state.Refresh(context.Background()))
		snap := state.Current()

		windowed := sitable.Pending
		if !seeded {
			windowed = sitable.Ordinary // H3: created before activation_block, seed not committed
		}
		for table, want := range map[string]sitable.Status{
			"genesis_t":       sitable.Active,
			"legacy_t":        sitable.Ordinary,
			"pending_t":       sitable.Pending,
			"refused_t":       sitable.Refused,
			"active_ready":    sitable.Active,
			"active_unready":  sitable.Pending, // E6: Active only when the reconciler reports Ready
			"retiring_t":      sitable.Gone,
			"purging_t":       sitable.Gone,
			"purged_old":      windowed, // Purged is unrecorded: step 4 on CreatedBlock 400
			"purged_new":      sitable.Pending,
			"bad_hash":        sitable.Pending, // schema-hash self-check
			"reused":          sitable.Active,  // Live skips the Purged predecessor
			"shadowed":        sitable.Active,  // Live skips a newer Refused declaration
			"unrecorded_old":  windowed,
			"unrecorded_zero": windowed, // zero CreatedBlock: created before the upgrade
			"unrecorded_new":  sitable.Pending,
			"never_synced":    sitable.Pending, // governed database, table not in chain state yet
		} {
			got := snap.Lookup(siDatabase, table)
			require.Equalf(t, want, got.Status, "seeded=%v table %s", seeded, table)
			require.Equal(t, siDatabase+"."+table, got.ID)
		}
		refusedGot := snap.Lookup(siDatabase, "refused_t")
		require.Equal(t, "column_type", refusedGot.RefusedCode)
		require.Contains(t, refusedGot.RefusedReason, "Decimal(18, 4)")

		require.Equal(t, sitable.Ordinary, snap.Lookup(otherDatabase, "x").Status, "a database on another indexer is Ordinary")
		require.Equal(t, sitable.Ordinary, snap.Lookup("unknown_db", "t").Status, "an unknown database is Ordinary")

		active, ok := snap.Schema(siDatabase + ".active_ready")
		require.True(t, ok)
		require.Equal(t, payloadexec.TableSchemaHash(testNetwork, active.Schema), active.SchemaHash)
		gone, ok := snap.Schema(siDatabase + ".retiring_t")
		require.True(t, ok, "a Gone table keeps its schema for recovery")
		require.Equal(t, sitable.Gone, gone.Status)
		genesisTable, ok := snap.Schema(siDatabase + ".genesis_t")
		require.True(t, ok)
		require.Equal(t, genesis, genesisTable.Schema, "a genesis incarnation carries no schema_json; the configured schema serves it")
		_, ok = snap.Schema(siDatabase + ".active_unready")
		require.False(t, ok, "an unready table is not Active and has no schema")

		var ids []string
		for _, table := range snap.Active() {
			ids = append(ids, table.ID)
		}
		require.Equal(t, []string{"tenant.active_ready", "tenant.genesis_t", "tenant.reused", "tenant.shadowed"}, ids)
	}
}

func TestLookupWhileRegistryDisabledServesGenesisSet(t *testing.T) {
	genesis := schemaFor(siDatabase + ".genesis_t")
	reg := newFakeRegistry(false, Registry{})
	chain := &fakeChain{err: errors.New("chain state must not be read while the registry is disabled")}
	state := newTestState(t, reg, &fakeReadiness{}, chain, genesis)
	require.NoError(t, state.Refresh(context.Background()))
	snap := state.Current()

	got := snap.Lookup(siDatabase, "genesis_t")
	require.Equal(t, sitable.Active, got.Status)
	require.Equal(t, payloadexec.TableSchemaHash(testNetwork, genesis), got.SchemaHash)
	require.Equal(t, sitable.Ordinary, snap.Lookup(siDatabase, "anything_else").Status)
	require.Equal(t, sitable.Ordinary, snap.Lookup("unknown_db", "t").Status)
	require.Equal(t, uint64(1), snap.Version(), "the disabled refresh reproduces the initial content")
}

func TestVersionMovesOnlyOnContentChange(t *testing.T) {
	reg := newFakeRegistry(true, Registry{Version: 1, SIIndexerID: siIndexer, ActivationBlock: activation})
	ready := &fakeReadiness{}
	chain := &fakeChain{dbs: chainState(map[string]uint64{"t": 1500})}
	state := newTestState(t, reg, ready, chain)
	ctx := context.Background()

	require.NoError(t, state.Refresh(ctx))
	first := state.Current()
	require.Equal(t, uint64(2), first.Version(), "enabling the registry changes the content")
	wake := state.Changed()

	require.NoError(t, state.Refresh(ctx))
	reg.set(true, Registry{Version: 2, SIIndexerID: siIndexer, ActivationBlock: activation})
	require.NoError(t, state.Refresh(ctx))
	require.Same(t, first, state.Current(), "a registry version bump without a content change keeps the snapshot")
	select {
	case <-wake:
		t.Fatal("Changed must not fire without a content change")
	default:
	}

	reg.set(true, Registry{Version: 3, SIIndexerID: siIndexer, ActivationBlock: activation,
		Incarnations: []Incarnation{chainInc(t, 1, "t", IncarnationActive)}})
	require.NoError(t, state.Refresh(ctx))
	require.Same(t, first, state.Current(), "Active but not Ready is still Pending: no content change")

	ready.set(siDatabase+".t", true)
	require.NoError(t, state.Refresh(ctx))
	second := state.Current()
	require.Equal(t, uint64(3), second.Version())
	require.Equal(t, sitable.Active, second.Lookup(siDatabase, "t").Status)
	select {
	case <-wake:
	default:
		t.Fatal("Changed must fire on a content change")
	}
	require.Equal(t, sitable.Pending, first.Lookup(siDatabase, "t").Status, "an earlier snapshot is immutable")

	chain.set(chainState(map[string]uint64{"t": 1500, "u": 1600}))
	require.NoError(t, state.Refresh(ctx))
	require.Equal(t, uint64(3), state.Current().Version(), "a newly synced default-deny table answers what the fallback already answered")
}

func TestRefreshKeepsPreviousSnapshotOnChainError(t *testing.T) {
	reg := newFakeRegistry(true, Registry{Version: 1, SIIndexerID: siIndexer, ActivationBlock: activation})
	chain := &fakeChain{dbs: chainState(nil)}
	state := newTestState(t, reg, &fakeReadiness{}, chain)
	require.NoError(t, state.Refresh(context.Background()))
	before := state.Current()
	chain.err = errors.New("redis down")
	require.Error(t, state.Refresh(context.Background()))
	require.Same(t, before, state.Current())
}

func TestRunWakesOnRegistryChangeDuringRefresh(t *testing.T) {
	reg := newFakeRegistry(true, Registry{Version: 1, SIIndexerID: siIndexer, ActivationBlock: activation})
	ready := &fakeReadiness{}
	ready.set(siDatabase+".t", true)
	state, err := New(Config{Registry: reg, Readiness: ready, Chain: &fakeChain{dbs: chainState(map[string]uint64{"t": 1500})},
		NetworkID: testNetwork, RefreshInterval: time.Hour})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- state.Run(ctx) }()
	require.Eventually(t, func() bool { return state.Current().Lookup(siDatabase, "t").Status == sitable.Pending }, 5*time.Second, time.Millisecond)

	reg.set(true, Registry{Version: 2, SIIndexerID: siIndexer, ActivationBlock: activation,
		Incarnations: []Incarnation{chainInc(t, 1, "t", IncarnationActive)}})
	require.Eventually(t, func() bool { return state.Current().Lookup(siDatabase, "t").Status == sitable.Active }, 5*time.Second, time.Millisecond,
		"a registry change must wake Run long before the one-hour tick")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestStorageIntegrityTableStatusShape(t *testing.T) {
	refused := chainInc(t, 2, "r", IncarnationRefused)
	refused.RefusedCode, refused.RefusedReason = "no_columns", "schema has no columns"
	reg := newFakeRegistry(true, Registry{Version: 17, Seeded: true, SIIndexerID: siIndexer, ActivationBlock: activation,
		Incarnations: []Incarnation{chainInc(t, 1, "a", IncarnationActive), refused, chainInc(t, 3, "g", IncarnationRetiring)}})
	ready := &fakeReadiness{}
	ready.set(siDatabase+".a", true)
	state := newTestState(t, reg, ready, &fakeChain{dbs: chainState(map[string]uint64{"a": 1500, "r": 1500, "g": 1500})})
	require.NoError(t, state.Refresh(context.Background()))

	active := state.StorageIntegrityTableStatus(siDatabase, "a")
	require.Equal(t, "active", active.Status)
	require.Equal(t, uint64(17), active.RegistryVersion)
	var decoded payloadexec.TableSchema
	require.NoError(t, json.Unmarshal([]byte(active.SchemaJSON), &decoded))
	require.Equal(t, schemaFor(siDatabase+".a"), decoded)
	require.Equal(t, payloadexec.TableSchemaHash(testNetwork, decoded), active.SchemaHash)

	r := state.StorageIntegrityTableStatus(siDatabase, "r")
	require.Equal(t, "refused", r.Status)
	require.Equal(t, "no_columns", r.RefusedCode)
	require.Equal(t, "schema has no columns", r.RefusedReason)
	require.Empty(t, r.SchemaJSON)

	g := state.StorageIntegrityTableStatus(siDatabase, "g")
	require.Equal(t, "gone", g.Status)
	require.NotEmpty(t, g.SchemaJSON)

	require.Equal(t, "pending", state.StorageIntegrityTableStatus(siDatabase, "nope").Status)
	require.Equal(t, "ordinary", state.StorageIntegrityTableStatus(otherDatabase, "x").Status)
}

func TestMetricsCountStatusesAndPendingDwell(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	reg := newFakeRegistry(true, Registry{Version: 1, Seeded: true, SIIndexerID: siIndexer, ActivationBlock: activation,
		Incarnations: []Incarnation{chainInc(t, 1, "p", IncarnationPending)}})
	metrics := NewMetrics()
	registry := prometheus.NewRegistry()
	require.NoError(t, metrics.Register(registry))
	require.Error(t, metrics.Register(registry), "a second registration must fail, not panic")
	state, err := New(Config{Registry: reg, Readiness: &fakeReadiness{}, Chain: &fakeChain{dbs: chainState(map[string]uint64{"p": 1500})},
		NetworkID: testNetwork, Metrics: metrics, Now: func() time.Time { return now }})
	require.NoError(t, err)
	require.NoError(t, state.Refresh(context.Background()))
	now = now.Add(90 * time.Second)
	require.NoError(t, state.Refresh(context.Background()))

	require.Equal(t, 1.0, testutil.ToFloat64(metrics.tables.WithLabelValues("pending")))
	require.Equal(t, 90.0, testutil.ToFloat64(metrics.pendingSeconds.WithLabelValues("tenant.p")))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.connected))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.enabled))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.registryVer))
	metrics.ObserveCompensation("declared")
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.compensations.WithLabelValues("declared")))
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.compensations.WithLabelValues("failed")))
}
```

Create `storageintegrityadapter/tablestate/chain_test.go`:

```go
package tablestate

import (
	"context"
	"testing"

	statecore "sentioxyz/sentio-core/network/state"

	"github.com/stretchr/testify/require"
)

type staticDatabaseInfos map[string]statecore.DatabaseInfo

func (s staticDatabaseInfos) DatabaseInfos(context.Context) (map[string]statecore.DatabaseInfo, error) {
	return s, nil
}

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

- [ ] **Step 2: Run them and verify they fail.** `go mod tidy` (the test imports `prometheus/client_golang/prometheus/testutil`, which makes the module direct), then `go test ./storageintegrityadapter/tablestate/`. Expected: build failure, `undefined: Registry`, `undefined: ChainDatabase`, `undefined: Incarnation` and the like.
- [ ] **Step 3: Implement.**

Create `storageintegrityadapter/tablestate/model.go`:

```go
// Package tablestate is sentio-node's implementation of HouseGate's
// storage-integrity table-state port (housegate pkg/sitable, design
// 2026-09-25 §9.2). It joins the arbiter's committed table registry, the
// embedded SNode reconciler's local readiness and the syncer's chain state
// into versioned, immutable snapshots.
package tablestate

import "github.com/housegate/housegate/pkg/replay/payloadexec"

// IncarnationStatus mirrors the arbiter's TableIncarnationStatus vocabulary.
type IncarnationStatus uint8

const (
	IncarnationLegacy IncarnationStatus = iota + 1
	IncarnationPending
	IncarnationRefused
	IncarnationActive
	IncarnationRetiring
	IncarnationPurging
	IncarnationPurged
)

// IncarnationOrigin mirrors the arbiter's TableOrigin vocabulary.
type IncarnationOrigin uint8

const (
	OriginGenesis IncarnationOrigin = iota + 1
	OriginLegacy
	OriginChain
)

// Incarnation is the part of one registry incarnation the host reads.
type Incarnation struct {
	Seq           uint64
	DatabaseID    string
	TableID       string
	Origin        IncarnationOrigin
	Status        IncarnationStatus
	SchemaJSON    string
	SchemaHash    string
	RefusedCode   string
	RefusedReason string
}

// Key is the logical table id "<database>.<table>".
func (i Incarnation) Key() string { return i.DatabaseID + "." + i.TableID }

// Registry is one committed registry view. Incarnations are in seq order.
type Registry struct {
	Version         uint64
	Seeded          bool
	SIIndexerID     uint64
	ActivationBlock uint64
	Incarnations    []Incarnation
}

// Live applies the arbiter's TableRegistryView.Live rule: the newest
// incarnation of key that is neither Purged nor Refused, else the newest one,
// else nil.
func (r Registry) Live(key string) *Incarnation {
	var newest *Incarnation
	for i := len(r.Incarnations) - 1; i >= 0; i-- {
		inc := &r.Incarnations[i]
		if inc.Key() != key {
			continue
		}
		if newest == nil {
			newest = inc
		}
		if inc.Status != IncarnationPurged && inc.Status != IncarnationRefused {
			return inc
		}
	}
	return newest
}

// ChainTable is one table of the syncer's chain state.
type ChainTable struct {
	// CreatedBlock is TableInfo.CreatedBlock; zero means created before the
	// syncer recorded creation blocks.
	CreatedBlock uint64
}

// ChainDatabase is one database of the syncer's chain state.
type ChainDatabase struct {
	IndexerID uint64
	Tables    map[string]ChainTable
}

// Inputs is everything one snapshot is built from.
type Inputs struct {
	// RegistryEnabled is false until governance enables the registry; the
	// genesis set is then served exactly like sitable.Static.
	RegistryEnabled bool
	Registry        Registry
	// Ready reports whether this node's reconciler has ensured and verified
	// every hg_* table of the key's current incarnation.
	Ready func(tableID string) bool
	// Chain is the syncer's chain state keyed by database id.
	Chain map[string]ChainDatabase
	// Genesis is the configured genesis set's startup schemas keyed by id.
	Genesis   map[string]payloadexec.TableSchema
	NetworkID string
}
```

Create `storageintegrityadapter/tablestate/snapshot.go`:

```go
package tablestate

import (
	"fmt"
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
)

type tableKey struct {
	database string
	table    string
}

// content is everything a snapshot answers. Two builds with equal content
// share one version (design §9.2: the version moves only on a content change),
// so tables lists only answers that differ from the fallback: a governed table
// that is Pending by default deny is answered by the fallback, whether or not
// the chain state already names it.
type content struct {
	enabled  bool
	governed map[string]struct{}
	tables   map[tableKey]sitable.Table
}

// build's second result names every table it evaluated with its status, for
// the metrics; it never takes part in versioning.
type evaluated map[string]sitable.Status

// Issue is one registry entry the builder could not serve as recorded. The
// table is reported Pending (or Gone without a schema) instead.
type Issue struct {
	TableID string
	Reason  string
}

// build evaluates design §9.2's Lookup order for every table the inputs name.
// Tables it does not list answer the snapshot's fallback: Pending in a
// governed database, Ordinary everywhere else.
func build(in Inputs) (content, evaluated, []Issue) {
	c := content{governed: map[string]struct{}{}, tables: map[tableKey]sitable.Table{}}
	seen := evaluated{}
	if !in.RegistryEnabled {
		// Step 1: the genesis set is Active and everything else is Ordinary.
		for id, schema := range in.Genesis {
			database, table, _ := strings.Cut(id, ".")
			c.tables[tableKey{database, table}] = sitable.Table{
				ID:         id,
				Status:     sitable.Active,
				Schema:     schema,
				SchemaHash: payloadexec.TableSchemaHash(in.NetworkID, schema),
			}
			seen[id] = sitable.Active
		}
		return c, seen, nil
	}
	c.enabled = true
	for database, info := range in.Chain {
		if info.IndexerID == in.Registry.SIIndexerID {
			c.governed[database] = struct{}{}
		}
	}
	keys := map[tableKey]struct{}{}
	for _, inc := range in.Registry.Incarnations {
		if _, ok := c.governed[inc.DatabaseID]; ok {
			keys[tableKey{inc.DatabaseID, inc.TableID}] = struct{}{}
		}
	}
	for database := range c.governed {
		for table := range in.Chain[database].Tables {
			keys[tableKey{database, table}] = struct{}{}
		}
	}
	var issues []Issue
	for key := range keys {
		table, issue := evaluate(in, key)
		seen[table.ID] = table.Status
		if table.Status != sitable.Pending {
			c.tables[key] = table
		}
		if issue != nil {
			issues = append(issues, *issue)
		}
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].TableID < issues[j].TableID })
	return c, seen, issues
}

// evaluate applies steps 3 and 4 of the Lookup order to one governed table.
func evaluate(in Inputs, key tableKey) (sitable.Table, *Issue) {
	id := sitable.TableID(key.database, key.table)
	if inc := in.Registry.Live(id); inc != nil && inc.Status != IncarnationPurged {
		switch inc.Status {
		case IncarnationLegacy:
			return sitable.Table{ID: id, Status: sitable.Ordinary}, nil
		case IncarnationPending:
			return sitable.Table{ID: id, Status: sitable.Pending}, nil
		case IncarnationRefused:
			return sitable.Table{ID: id, Status: sitable.Refused, RefusedCode: inc.RefusedCode, RefusedReason: inc.RefusedReason}, nil
		case IncarnationActive:
			schema, err := resolveSchema(in, *inc)
			if err != nil {
				return sitable.Table{ID: id, Status: sitable.Pending}, &Issue{TableID: id, Reason: err.Error()}
			}
			if in.Ready == nil || !in.Ready(id) {
				return sitable.Table{ID: id, Status: sitable.Pending}, nil
			}
			return sitable.Table{ID: id, Status: sitable.Active, Schema: schema, SchemaHash: inc.SchemaHash}, nil
		case IncarnationRetiring, IncarnationPurging:
			schema, err := resolveSchema(in, *inc)
			if err != nil {
				return sitable.Table{ID: id, Status: sitable.Gone}, &Issue{TableID: id, Reason: err.Error()}
			}
			return sitable.Table{ID: id, Status: sitable.Gone, Schema: schema, SchemaHash: inc.SchemaHash}, nil
		default:
			return sitable.Table{ID: id, Status: sitable.Pending}, &Issue{TableID: id, Reason: fmt.Sprintf("unknown incarnation status %d", inc.Status)}
		}
	}
	// Step 4: no live incarnation (none at all, or the newest is Purged).
	chainTable, onChain := in.Chain[key.database].Tables[key.table]
	if !in.Registry.Seeded && onChain && chainTable.CreatedBlock < in.Registry.ActivationBlock {
		return sitable.Table{ID: id, Status: sitable.Ordinary}, nil
	}
	return sitable.Table{ID: id, Status: sitable.Pending}, nil
}

// resolveSchema decodes an incarnation's schema and requires the schema-hash
// self-check (design §9.2): payloadexec.TableSchemaHash(network_id, schema)
// must equal the registry's schema_hash. A genesis incarnation carries no
// schema_json, so its schema is the configured genesis schema.
func resolveSchema(in Inputs, inc Incarnation) (payloadexec.TableSchema, error) {
	id := inc.Key()
	var schema payloadexec.TableSchema
	switch {
	case inc.SchemaJSON != "":
		decoded, err := payloadexec.DecodeTableSchemaJSON(id, inc.SchemaJSON)
		if err != nil {
			return payloadexec.TableSchema{}, err
		}
		schema = decoded
	case inc.Origin == OriginGenesis:
		genesis, ok := in.Genesis[id]
		if !ok {
			return payloadexec.TableSchema{}, fmt.Errorf("genesis table has no configured schema (storage_integrity.snode.table_ids)")
		}
		schema = genesis
	default:
		return payloadexec.TableSchema{}, fmt.Errorf("registry incarnation %d has no schema_json", inc.Seq)
	}
	if got := payloadexec.TableSchemaHash(in.NetworkID, schema); got != inc.SchemaHash {
		return payloadexec.TableSchema{}, fmt.Errorf("schema hash %s recomputed for network %q differs from the registry's %s", got, in.NetworkID, inc.SchemaHash)
	}
	return schema, nil
}

// equal compares two contents. A table's schema is compared through its
// SchemaHash, which digests every schema field.
func (c content) equal(other content) bool {
	if c.enabled != other.enabled || len(c.governed) != len(other.governed) || len(c.tables) != len(other.tables) {
		return false
	}
	for database := range c.governed {
		if _, ok := other.governed[database]; !ok {
			return false
		}
	}
	for key, a := range c.tables {
		b, ok := other.tables[key]
		if !ok || a.ID != b.ID || a.Status != b.Status || a.RefusedCode != b.RefusedCode ||
			a.RefusedReason != b.RefusedReason || a.SchemaHash != b.SchemaHash || a.Schema.TableID != b.Schema.TableID {
			return false
		}
	}
	return true
}

// Snapshot is an immutable sitable.Snapshot. Every method is in-memory.
type Snapshot struct {
	version uint64
	content content
	active  []sitable.Table
}

var _ sitable.Snapshot = (*Snapshot)(nil)

func newSnapshot(version uint64, c content) *Snapshot {
	s := &Snapshot{version: version, content: c}
	for _, table := range c.tables {
		if table.Status == sitable.Active {
			s.active = append(s.active, table)
		}
	}
	sort.Slice(s.active, func(i, j int) bool { return s.active[i].ID < s.active[j].ID })
	return s
}

func (s *Snapshot) Version() uint64 { return s.version }

func (s *Snapshot) Lookup(database, table string) sitable.Table {
	if t, ok := s.content.tables[tableKey{database, table}]; ok {
		return cloneTable(t)
	}
	status := sitable.Ordinary
	if _, governed := s.content.governed[database]; governed {
		status = sitable.Pending // default deny: governed and not recorded
	}
	return sitable.Table{ID: sitable.TableID(database, table), Status: status}
}

func (s *Snapshot) Active() []sitable.Table {
	out := make([]sitable.Table, len(s.active))
	for i, t := range s.active {
		out[i] = cloneTable(t)
	}
	return out
}

func (s *Snapshot) Schema(id string) (sitable.Table, bool) {
	database, table, _ := strings.Cut(id, ".")
	t, ok := s.content.tables[tableKey{database, table}]
	if !ok || (t.Status != sitable.Active && t.Status != sitable.Gone) {
		return sitable.Table{}, false
	}
	return cloneTable(t), true
}

// cloneTable hands out a Table whose Columns slice the snapshot does not share.
func cloneTable(t sitable.Table) sitable.Table {
	if t.Schema.Columns != nil {
		t.Schema.Columns = append([]lthash.Column(nil), t.Schema.Columns...)
	}
	return t
}
```

Create `storageintegrityadapter/tablestate/state.go`:

```go
package tablestate

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
)

// RegistrySource is the registry follower surface the state reads.
type RegistrySource interface {
	// View returns the latest accepted registry and whether the registry is
	// enabled.
	View() (Registry, bool)
	// Changed returns a channel closed at the next accepted registry version
	// after the call.
	Changed() <-chan struct{}
	// Connected reports whether the follower's watch stream is open.
	Connected() bool
}

// Readiness is the embedded SNode reconciler's per-table readiness.
type Readiness interface {
	Ready(tableID string) bool
}

// ChainSource reads the syncer's chain state.
type ChainSource interface {
	Databases(ctx context.Context) (map[string]ChainDatabase, error)
}

// DefaultRefreshInterval bounds how long a chain-state or readiness change
// takes to reach a snapshot. Registry changes wake the loop immediately.
const DefaultRefreshInterval = time.Second

// Config wires a State.
type Config struct {
	Registry  RegistrySource
	Readiness Readiness
	Chain     ChainSource
	// Genesis is the configured genesis table set (storage_integrity.snode.table_ids)
	// with its startup schemas.
	Genesis         []payloadexec.TableSchema
	NetworkID       string
	RefreshInterval time.Duration
	Metrics         *Metrics
	Logger          *slog.Logger
	Now             func() time.Time
}

// State is sentio-node's sitable.TableState.
type State struct {
	cfg     Config
	genesis map[string]payloadexec.TableSchema

	refreshMu sync.Mutex // serializes builds so an older view never publishes last

	mu           sync.Mutex
	current      *Snapshot
	registryVer  uint64 // registry version of the latest build; 0 while disabled
	changed      chan struct{}
	issues       map[string]string
	pendingSince map[string]time.Time
}

var _ sitable.TableState = (*State)(nil)

// New builds the state. Until the first Refresh it serves the genesis set as
// the registry-disabled rule does, which is exactly what the node served
// before the registry existed; standalone refreshes before HouseGate starts.
func New(cfg Config) (*State, error) {
	var errs []error
	if cfg.Registry == nil {
		errs = append(errs, errors.New("tablestate: registry source is required"))
	}
	if cfg.Readiness == nil {
		errs = append(errs, errors.New("tablestate: readiness is required"))
	}
	if cfg.Chain == nil {
		errs = append(errs, errors.New("tablestate: chain source is required"))
	}
	if strings.TrimSpace(cfg.NetworkID) == "" {
		errs = append(errs, errors.New("tablestate: network id is required"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = DefaultRefreshInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	genesis := make(map[string]payloadexec.TableSchema, len(cfg.Genesis))
	for _, schema := range cfg.Genesis {
		genesis[schema.TableID] = schema
	}
	initial, _, _ := build(Inputs{Genesis: genesis, NetworkID: cfg.NetworkID})
	return &State{
		cfg:          cfg,
		genesis:      genesis,
		current:      newSnapshot(1, initial),
		changed:      make(chan struct{}),
		issues:       map[string]string{},
		pendingSince: map[string]time.Time{},
	}, nil
}

// Current returns the latest snapshot.
func (s *State) Current() sitable.Snapshot { return s.snapshot() }

func (s *State) snapshot() *Snapshot {
	snap, _ := s.snapshotAndRegistryVersion()
	return snap
}

func (s *State) snapshotAndRegistryVersion() (*Snapshot, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current, s.registryVer
}

// Changed returns a channel closed at the next version change after the call.
func (s *State) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

// Refresh rebuilds the snapshot from the current inputs and publishes it when
// its content changed. On error the previous snapshot stays in place.
func (s *State) Refresh(ctx context.Context) error {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	view, enabled := s.cfg.Registry.View()
	in := Inputs{
		RegistryEnabled: enabled,
		Registry:        view,
		Ready:           s.cfg.Readiness.Ready,
		Genesis:         s.genesis,
		NetworkID:       s.cfg.NetworkID,
	}
	if enabled {
		chain, err := s.cfg.Chain.Databases(ctx)
		if err != nil {
			return err
		}
		in.Chain = chain
	}
	c, seen, issues := build(in)
	var registryVersion uint64
	if enabled {
		registryVersion = view.Version
	}
	s.publish(c, registryVersion, seen, issues)
	s.cfg.Metrics.observeFollower(enabled, s.cfg.Registry.Connected(), view.Version)
	return nil
}

func (s *State) publish(c content, registryVersion uint64, seen evaluated, issues []Issue) {
	now := s.cfg.Now()
	s.mu.Lock()
	if !c.equal(s.current.content) {
		s.current = newSnapshot(s.current.version+1, c)
		close(s.changed)
		s.changed = make(chan struct{})
	}
	// The registry version moves on every build, without a new snapshot when
	// the content is unchanged: the current snapshot answers for it too.
	s.registryVer = registryVersion
	next := make(map[string]string, len(issues))
	for _, issue := range issues {
		next[issue.TableID] = issue.Reason
		if s.issues[issue.TableID] != issue.Reason {
			s.cfg.Logger.Error("storage-integrity table cannot be served as the registry records it",
				"table", issue.TableID, "reason", issue.Reason)
		}
	}
	s.issues = next
	pending := make(map[string]time.Time)
	for id, status := range seen {
		if status != sitable.Pending {
			continue
		}
		since, ok := s.pendingSince[id]
		if !ok {
			since = now
		}
		pending[id] = since
	}
	s.pendingSince = pending
	s.mu.Unlock()
	s.cfg.Metrics.observeSnapshot(seen, pending, now)
}

// Run refreshes on every registry change and every RefreshInterval until ctx
// ends. It subscribes to the registry before each refresh, so a version
// accepted during a refresh wakes the next one.
func (s *State) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.RefreshInterval)
	defer ticker.Stop()
	for {
		wake := s.cfg.Registry.Changed()
		if err := s.Refresh(ctx); err != nil && ctx.Err() == nil {
			s.cfg.Logger.Warn("storage-integrity table state refresh failed; serving the previous snapshot", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-ticker.C:
		}
	}
}
```

Create `storageintegrityadapter/tablestate/metrics.go`:

```go
package tablestate

import (
	"errors"
	"time"

	"github.com/housegate/housegate/pkg/sitable"
	"github.com/prometheus/client_golang/prometheus"
)

// compensationOutcomes are database_registry.CompensationDeclared and
// CompensationFailed, pre-registered so both series exist from startup.
var compensationOutcomes = []string{"declared", "failed"}

var tableStatuses = []sitable.Status{sitable.Ordinary, sitable.Pending, sitable.Refused, sitable.Active, sitable.Gone}

// Metrics are the host's storage-integrity table metrics (design §9.6). The
// zero value is unusable; a nil *Metrics records nothing.
type Metrics struct {
	tables         *prometheus.GaugeVec
	pendingSeconds *prometheus.GaugeVec
	compensations  *prometheus.CounterVec
	connected      prometheus.Gauge
	enabled        prometheus.Gauge
	registryVer    prometheus.Gauge
}

// NewMetrics builds unregistered collectors; Register attaches them once the
// embedded HouseGate's registry exists.
func NewMetrics() *Metrics {
	m := &Metrics{
		tables: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sentio_node_storage_integrity_tables",
			Help: "Tables named by the registry or by the chain state of a governed database, by the status the current snapshot reports.",
		}, []string{"status"}),
		pendingSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sentio_node_storage_integrity_table_pending_seconds",
			Help: "Seconds a table named by the registry or the chain state has been reported Pending without interruption.",
		}, []string{"table"}),
		compensations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sentio_node_storage_integrity_schema_compensations_total",
			Help: "Schema declarations re-run by the compensation loop, by outcome.",
		}, []string{"outcome"}),
		connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sentio_node_storage_integrity_registry_follower_connected",
			Help: "1 while the arbiter table-registry watch stream is open, 0 while the follower reconnects.",
		}),
		enabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sentio_node_storage_integrity_registry_enabled",
			Help: "1 once governance has enabled the arbiter table registry, 0 while the genesis set is served.",
		}),
		registryVer: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sentio_node_storage_integrity_registry_version",
			Help: "The arbiter table-registry version the follower last delivered; 0 while disabled.",
		}),
	}
	for _, status := range tableStatuses {
		m.tables.WithLabelValues(status.String())
	}
	for _, outcome := range compensationOutcomes {
		m.compensations.WithLabelValues(outcome)
	}
	return m
}

// Register attaches every collector to reg. A collector already registered
// with reg is an error, so a double registration fails loudly instead of
// panicking.
func (m *Metrics) Register(reg prometheus.Registerer) error {
	if m == nil || reg == nil {
		return errors.New("tablestate metrics: metrics and registerer are required")
	}
	var errs []error
	for _, c := range []prometheus.Collector{m.tables, m.pendingSeconds, m.compensations, m.connected, m.enabled, m.registryVer} {
		if err := reg.Register(c); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ObserveCompensation counts one compensation outcome; it is the
// database_registry.CompensationConfig.Observe callback.
func (m *Metrics) ObserveCompensation(outcome string) {
	if m == nil {
		return
	}
	m.compensations.WithLabelValues(outcome).Inc()
}

func (m *Metrics) observeSnapshot(seen evaluated, pendingSince map[string]time.Time, now time.Time) {
	if m == nil {
		return
	}
	counts := map[sitable.Status]int{}
	for _, status := range seen {
		counts[status]++
	}
	for _, status := range tableStatuses {
		m.tables.WithLabelValues(status.String()).Set(float64(counts[status]))
	}
	m.pendingSeconds.Reset()
	for id, since := range pendingSince {
		m.pendingSeconds.WithLabelValues(id).Set(now.Sub(since).Seconds())
	}
}

func (m *Metrics) observeFollower(enabled, connected bool, version uint64) {
	if m == nil {
		return
	}
	m.connected.Set(boolGauge(connected))
	m.enabled.Set(boolGauge(enabled))
	if !enabled {
		version = 0
	}
	m.registryVer.Set(float64(version))
}

func boolGauge(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
```

Create `storageintegrityadapter/tablestate/status.go`:

```go
package tablestate

import (
	"encoding/json"

	hgregistry "github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sitable"
)

// StorageIntegrityTableStatus answers sentio_getStorageIntegrityTableStatus
// (housegate spec 2026-09-24 §10.2) with the current snapshot's Lookup.
// schema_json and schema_hash are set for Active and Gone tables that carry a
// schema; registry_version is the registry version of the latest build, which
// the current snapshot answers for.
func (s *State) StorageIntegrityTableStatus(database, table string) hgregistry.TableStatus {
	snap, registryVersion := s.snapshotAndRegistryVersion()
	t := snap.Lookup(database, table)
	out := hgregistry.TableStatus{
		Status:          t.Status.String(),
		RefusedCode:     t.RefusedCode,
		RefusedReason:   t.RefusedReason,
		RegistryVersion: registryVersion,
	}
	if (t.Status == sitable.Active || t.Status == sitable.Gone) && t.Schema.TableID != "" {
		if raw, err := json.Marshal(t.Schema); err == nil {
			out.SchemaJSON = string(raw)
			out.SchemaHash = t.SchemaHash
		}
	}
	return out
}
```

Create `storageintegrityadapter/tablestate/chain.go`:

```go
package tablestate

import (
	"context"

	statecore "sentioxyz/sentio-core/network/state"
)

// DatabaseInfoReader reads the syncer's mirrored chain state
// (networkstate.RedisNetworkState implements it).
type DatabaseInfoReader interface {
	DatabaseInfos(ctx context.Context) (map[string]statecore.DatabaseInfo, error)
}

// DatabaseInfoChain adapts the mirrored chain state to ChainSource.
func DatabaseInfoChain(reader DatabaseInfoReader) ChainSource {
	return databaseInfoChain{reader: reader}
}

type databaseInfoChain struct{ reader DatabaseInfoReader }

func (c databaseInfoChain) Databases(ctx context.Context) (map[string]ChainDatabase, error) {
	infos, err := c.reader.DatabaseInfos(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]ChainDatabase, len(infos))
	for id, info := range infos {
		tables := make(map[string]ChainTable, len(info.Tables))
		for _, table := range info.Tables {
			tables[table.TableId] = ChainTable{CreatedBlock: table.CreatedBlock}
		}
		out[id] = ChainDatabase{IndexerID: info.IndexerId, Tables: tables}
	}
	return out, nil
}
```

- [ ] **Step 4: Run the package.** `go vet ./storageintegrityadapter/tablestate/ && go test -race ./storageintegrityadapter/tablestate/`, then `bazel mod tidy && bazel run //:gazelle && bazel test //storageintegrityadapter/tablestate:tablestate_test`. Expected: `ok` / `PASSED`; `bazel mod tidy` adds `com_github_prometheus_client_golang` to `use_repo`.
- [ ] **Step 5: Commit.**

```bash
git add go.mod go.sum MODULE.bazel MODULE.bazel.lock storageintegrityadapter/tablestate
git commit -F - <<'EOF'
feat(storage-integrity): table state over the arbiter registry, reconciler readiness and chain state

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 8: Adapt arbiter-core's registry follower to the table state

**Files:**

- Create: `storageintegrityadapter/tablestate/arbitercore.go`
- Test: create `storageintegrityadapter/tablestate/arbitercore_test.go`

**Interfaces:**

- Consumes: plan A `wire.TableRegistrySnapshot`, `wire.TableIncarnation`, `wire.TableStatus*`, `wire.TableOrigin*`, `arbiter.TableRegistryParams{SIIndexerID, ActivationBlock}`.
- Produces: `type RegistryFollower interface { View() (wire.TableRegistrySnapshot, bool); Changed() <-chan struct{}; Connected() bool }` (`*dataplane.RegistryFollower` implements it), `func FromFollower(follower RegistryFollower) RegistrySource`, `type ReadinessFunc func(tableID string) bool` implementing `Readiness`. This file is the only one in the package that imports arbiter-core (B1).

- [ ] **Step 1: Write the failing test.** Create `storageintegrityadapter/tablestate/arbitercore_test.go`:

```go
package tablestate

import (
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/stretchr/testify/require"
)

type staticFollower struct {
	snapshot wire.TableRegistrySnapshot
	enabled  bool
}

func (f staticFollower) View() (wire.TableRegistrySnapshot, bool) { return f.snapshot, f.enabled }
func (f staticFollower) Changed() <-chan struct{}                 { return nil }
func (f staticFollower) Connected() bool                          { return f.enabled }

func TestFromFollowerMapsEveryField(t *testing.T) {
	source := FromFollower(staticFollower{enabled: true, snapshot: wire.TableRegistrySnapshot{
		Params:  arbiter.TableRegistryParams{ChainID: 1, SIIndexerID: 7, ActivationBlock: 1000, Confirmation: "safe"},
		Version: 12,
		Seeded:  true,
		Incarnations: []wire.TableIncarnation{{
			Seq: 3, DatabaseID: "tenant", TableID: "t", Origin: wire.TableOriginChain, Status: wire.TableStatusRefused,
			SchemaJSON: "{}", SchemaHash: "0x01", RefusedCode: "column_type", RefusedReason: "bad type",
		}},
	}})
	got, enabled := source.View()
	require.True(t, enabled)
	require.Equal(t, Registry{
		Version: 12, Seeded: true, SIIndexerID: 7, ActivationBlock: 1000,
		Incarnations: []Incarnation{{
			Seq: 3, DatabaseID: "tenant", TableID: "t", Origin: OriginChain, Status: IncarnationRefused,
			SchemaJSON: "{}", SchemaHash: "0x01", RefusedCode: "column_type", RefusedReason: "bad type",
		}},
	}, got)

	for status, want := range map[wire.TableIncarnationStatus]IncarnationStatus{
		wire.TableStatusLegacy: IncarnationLegacy, wire.TableStatusPending: IncarnationPending,
		wire.TableStatusRefused: IncarnationRefused, wire.TableStatusActive: IncarnationActive,
		wire.TableStatusRetiring: IncarnationRetiring, wire.TableStatusPurging: IncarnationPurging,
		wire.TableStatusPurged: IncarnationPurged,
	} {
		require.Equal(t, want, statusFromWire[status], status)
	}
	for origin, want := range map[wire.TableOrigin]IncarnationOrigin{
		wire.TableOriginGenesis: OriginGenesis, wire.TableOriginLegacy: OriginLegacy, wire.TableOriginChain: OriginChain,
	} {
		require.Equal(t, want, originFromWire[origin], origin)
	}

	require.True(t, source.Connected())
	_, enabled = FromFollower(staticFollower{}).View()
	require.False(t, enabled)
}
```

- [ ] **Step 2: Run it and verify it fails.** `go test ./storageintegrityadapter/tablestate/`. Expected: build failure, `undefined: FromFollower`, `undefined: statusFromWire`, `undefined: originFromWire`.
- [ ] **Step 3: Implement.** Create `storageintegrityadapter/tablestate/arbitercore.go`:

```go
package tablestate

// arbitercore.go is the only file that names arbiter-core's registry types
// (plan A: wire.TableRegistrySnapshot and dataplane.RegistryFollower). The
// rest of the package reads the narrow RegistrySource and Readiness ports.

import (
	"github.com/sentioxyz/arbiter-core/wire"
)

// RegistryFollower is the dataplane.RegistryFollower surface the state reads.
type RegistryFollower interface {
	View() (wire.TableRegistrySnapshot, bool)
	Changed() <-chan struct{}
	Connected() bool
}

// FromFollower adapts arbiter-core's registry follower to RegistrySource.
func FromFollower(follower RegistryFollower) RegistrySource {
	return followerSource{follower: follower}
}

type followerSource struct{ follower RegistryFollower }

func (s followerSource) Changed() <-chan struct{} { return s.follower.Changed() }

func (s followerSource) Connected() bool { return s.follower.Connected() }

func (s followerSource) View() (Registry, bool) {
	snapshot, enabled := s.follower.View()
	if !enabled {
		return Registry{}, false
	}
	out := Registry{
		Version:         snapshot.Version,
		Seeded:          snapshot.Seeded,
		SIIndexerID:     snapshot.Params.SIIndexerID,
		ActivationBlock: snapshot.Params.ActivationBlock,
		Incarnations:    make([]Incarnation, 0, len(snapshot.Incarnations)),
	}
	for _, inc := range snapshot.Incarnations {
		out.Incarnations = append(out.Incarnations, Incarnation{
			Seq:           inc.Seq,
			DatabaseID:    inc.DatabaseID,
			TableID:       inc.TableID,
			Origin:        originFromWire[inc.Origin],
			Status:        statusFromWire[inc.Status],
			SchemaJSON:    inc.SchemaJSON,
			SchemaHash:    inc.SchemaHash,
			RefusedCode:   inc.RefusedCode,
			RefusedReason: inc.RefusedReason,
		})
	}
	return out, true
}

// The decoder refuses unknown enum values, so both maps are total; a zero
// value would still evaluate fail-closed (Pending) in evaluate.
var statusFromWire = map[wire.TableIncarnationStatus]IncarnationStatus{
	wire.TableStatusLegacy:   IncarnationLegacy,
	wire.TableStatusPending:  IncarnationPending,
	wire.TableStatusRefused:  IncarnationRefused,
	wire.TableStatusActive:   IncarnationActive,
	wire.TableStatusRetiring: IncarnationRetiring,
	wire.TableStatusPurging:  IncarnationPurging,
	wire.TableStatusPurged:   IncarnationPurged,
}

var originFromWire = map[wire.TableOrigin]IncarnationOrigin{
	wire.TableOriginGenesis: OriginGenesis,
	wire.TableOriginLegacy:  OriginLegacy,
	wire.TableOriginChain:   OriginChain,
}

// ReadinessFunc adapts the SNode reconciler's per-table readiness method.
type ReadinessFunc func(tableID string) bool

func (f ReadinessFunc) Ready(tableID string) bool { return f(tableID) }
```

- [ ] **Step 4: Run the package.** `go test -race ./storageintegrityadapter/tablestate/`, `bazel run //:gazelle`, `bazel test //storageintegrityadapter/tablestate:tablestate_test`. Expected: `ok` / `PASSED`.
- [ ] **Step 5: Commit.**

```bash
git add storageintegrityadapter/tablestate
git commit -F - <<'EOF'
feat(storage-integrity): adapt the arbiter-core registry follower to the table state

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 9: Serve `sentio_getStorageIntegrityTableStatus`

**Files:**

- Create: `rpc/storage_integrity_status.go`
- Modify: `rpc/storage_service.go:23-29` (service struct and constructor), `:36-41` (constructor body), `rpc/server.go:49-51` (`NewStorageNodeRPCServer`)
- Test: create `rpc/storage_integrity_status_test.go`

**Interfaces:**

- Consumes: housegate `registry.TableStatus`, `registry.TableStatusOrdinary`; housegate `network.NewRpcNetworkState` (test only, the agent's real client).
- Produces:

```go
type StorageIntegrityTableStatusSource interface { StorageIntegrityTableStatus(database, table string) hgregistry.TableStatus }
type StorageNodeOption func(*StorageNodeService)
func WithStorageIntegrityTableStatus(source StorageIntegrityTableStatusSource) StorageNodeOption
func NewStorageNodeService(syncer *syncer.Syncer, env *common.NodeEnv, opts ...StorageNodeOption) *StorageNodeService
func NewStorageNodeRPCServer(syncer *syncer.Syncer, env *common.NodeEnv, opts ...StorageNodeOption) (*http.Server, error)
func (s *StorageNodeService) GetStorageIntegrityTableStatus(ctx context.Context, database string, table string) (hgregistry.TableStatus, error) // JSON-RPC sentio_getStorageIntegrityTableStatus
```

Existing callers (`StartStorageNodeRPCServer`, `standalone.Run` until Task 13) compile unchanged and answer `ordinary`.

- [ ] **Step 1: Write the failing test.** Create `rpc/storage_integrity_status_test.go`:

```go
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/network"
	hgregistry "github.com/housegate/housegate/pkg/registry"
	"github.com/stretchr/testify/require"
)

type fixedTableStatuses map[string]hgregistry.TableStatus

func (f fixedTableStatuses) StorageIntegrityTableStatus(database, table string) hgregistry.TableStatus {
	if status, ok := f[database+"."+table]; ok {
		return status
	}
	return hgregistry.TableStatus{Status: hgregistry.TableStatusPending}
}

func startStorageRPC(t *testing.T, opts ...StorageNodeOption) string {
	t.Helper()
	server, err := NewStorageNodeRPCServer(nil, nil, opts...)
	require.NoError(t, err)
	httpServer := httptest.NewServer(server.Handler)
	t.Cleanup(httpServer.Close)
	return httpServer.URL
}

// TestStorageIntegrityTableStatusMatchesHousegateClient drives HouseGate's own
// agent-side client against the sentio-node handler, so the method name,
// positional parameters and result field names are checked end to end.
func TestStorageIntegrityTableStatusMatchesHousegateClient(t *testing.T) {
	want := map[string]hgregistry.TableStatus{
		"tenant.a": {Status: "active", SchemaJSON: `{"table_id":"tenant.a","partition_by":"p","columns":[{"name":"p","type":"String"}]}`, SchemaHash: "0xabc", RegistryVersion: 17},
		"tenant.r": {Status: "refused", RefusedCode: "column_type", RefusedReason: `column "v" has unsupported type "Decimal(18, 4)"`, RegistryVersion: 17},
		"tenant.g": {Status: "gone", SchemaJSON: `{"table_id":"tenant.g"}`, SchemaHash: "0xdef", RegistryVersion: 18},
		"other.o":  {Status: "ordinary"},
	}
	url := startStorageRPC(t, WithStorageIntegrityTableStatus(fixedTableStatuses(want)))
	client, err := network.NewRpcNetworkState(url, network.RpcOptions{})
	require.NoError(t, err)

	for id, status := range want {
		database, table, _ := strings.Cut(id, ".")
		got, err := client.StorageIntegrityTableStatus(context.Background(), database, table)
		require.NoError(t, err, id)
		require.Equal(t, status, got, id)
	}
	got, err := client.StorageIntegrityTableStatus(context.Background(), "tenant", "missing")
	require.NoError(t, err)
	require.Equal(t, hgregistry.TableStatus{Status: "pending"}, got)
}

func TestStorageIntegrityTableStatusWithoutStorageIntegrityIsOrdinary(t *testing.T) {
	client, err := network.NewRpcNetworkState(startStorageRPC(t), network.RpcOptions{})
	require.NoError(t, err)
	got, err := client.StorageIntegrityTableStatus(context.Background(), "tenant", "t")
	require.NoError(t, err)
	require.Equal(t, hgregistry.TableStatus{Status: "ordinary"}, got)
}

// TestStorageIntegrityTableStatusWireShape pins the exact JSON object: every
// contract field is present even when empty, and nothing else is.
func TestStorageIntegrityTableStatusWireShape(t *testing.T) {
	url := startStorageRPC(t, WithStorageIntegrityTableStatus(fixedTableStatuses{
		"tenant.r": {Status: "refused", RefusedCode: "no_columns", RefusedReason: "schema has no columns", RegistryVersion: 3},
	}))
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityTableStatus","params":["tenant","r"]}`)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var envelope struct {
		Result map[string]any `json:"result"`
	}
	require.NoError(t, json.Unmarshal(raw, &envelope), string(raw))
	require.Equal(t, map[string]any{
		"status": "refused", "refused_code": "no_columns", "refused_reason": "schema has no columns",
		"schema_json": "", "schema_hash": "", "registry_version": float64(3),
	}, envelope.Result)

	empty := []byte(`{"jsonrpc":"2.0","id":2,"method":"sentio_getStorageIntegrityTableStatus","params":["","r"]}`)
	resp, err = http.Post(url, "application/json", bytes.NewReader(empty))
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(raw), "database and table are required")
}
```

- [ ] **Step 2: Run it and verify it fails.** `go test ./rpc/`. Expected: build failure, `undefined: StorageNodeOption`, `undefined: WithStorageIntegrityTableStatus`.
- [ ] **Step 3: Implement.** Create `rpc/storage_integrity_status.go`:

```go
package rpc

import (
	"context"
	"errors"

	hgregistry "github.com/housegate/housegate/pkg/registry"
)

// StorageIntegrityTableStatusSource answers one table's storage-integrity
// status with the serving node's table-state Lookup semantics.
type StorageIntegrityTableStatusSource interface {
	StorageIntegrityTableStatus(database, table string) hgregistry.TableStatus
}

// StorageNodeOption configures optional StorageNodeService capabilities.
type StorageNodeOption func(*StorageNodeService)

// WithStorageIntegrityTableStatus serves sentio_getStorageIntegrityTableStatus
// from source. Without it the method answers "ordinary" for every table.
func WithStorageIntegrityTableStatus(source StorageIntegrityTableStatusSource) StorageNodeOption {
	return func(s *StorageNodeService) { s.tableStatus = source }
}

// GetStorageIntegrityTableStatus is housegate's agent status contract
// (housegate spec 2026-09-24 §10.2).
//
// JSON-RPC method: sentio_getStorageIntegrityTableStatus
//
//	params: [database, table]
//	result: {"status": "ordinary|pending|refused|active|gone",
//	         "refused_code": "", "refused_reason": "",
//	         "schema_json": "", "schema_hash": "", "registry_version": 0}
func (s *StorageNodeService) GetStorageIntegrityTableStatus(
	_ context.Context, database string, table string,
) (hgregistry.TableStatus, error) {
	if database == "" || table == "" {
		return hgregistry.TableStatus{}, errors.New("database and table are required")
	}
	if s.tableStatus == nil {
		return hgregistry.TableStatus{Status: hgregistry.TableStatusOrdinary}, nil
	}
	return s.tableStatus.StorageIntegrityTableStatus(database, table), nil
}
```

`rpc/storage_service.go` edit 1 of 2, replace:

```go
type StorageNodeService struct {
	syncer  *syncer.Syncer
	env     *common.NodeEnv
	decoder *RelayErrorDecoder // eagerly constructed in NewStorageNodeService
}

func NewStorageNodeService(syncer *syncer.Syncer, env *common.NodeEnv) *StorageNodeService {
```

with:

```go
type StorageNodeService struct {
	syncer      *syncer.Syncer
	env         *common.NodeEnv
	decoder     *RelayErrorDecoder // eagerly constructed in NewStorageNodeService
	tableStatus StorageIntegrityTableStatusSource
}

func NewStorageNodeService(syncer *syncer.Syncer, env *common.NodeEnv, opts ...StorageNodeOption) *StorageNodeService {
```

`rpc/storage_service.go` edit 2 of 2, replace:

```go
	return &StorageNodeService{
		syncer:  syncer,
		env:     env,
		decoder: dec,
	}
}
```

with:

```go
	service := &StorageNodeService{
		syncer:  syncer,
		env:     env,
		decoder: dec,
	}
	for _, opt := range opts {
		opt(service)
	}
	return service
}
```

`rpc/server.go`, replace:

```go
func NewStorageNodeRPCServer(syncer *syncer.Syncer, env *common.NodeEnv) (*http.Server, error) {
	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("sentio", NewStorageNodeService(syncer, env)); err != nil {
```

with:

```go
func NewStorageNodeRPCServer(syncer *syncer.Syncer, env *common.NodeEnv, opts ...StorageNodeOption) (*http.Server, error) {
	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("sentio", NewStorageNodeService(syncer, env, opts...)); err != nil {
```

- [ ] **Step 4: Run the package.** `go test ./rpc/`, `bazel run //:gazelle`, `bazel test //rpc:rpc_test`. Expected: `ok` / `PASSED`.
- [ ] **Step 5: Commit.**

```bash
git add rpc/storage_integrity_status.go rpc/storage_integrity_status_test.go rpc/storage_service.go rpc/server.go rpc/BUILD.bazel
git commit -F - <<'EOF'
feat(rpc): serve sentio_getStorageIntegrityTableStatus

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 10: Config migration to the registry-driven table set

**Files:**

- Modify: `config/config.go:14` (imports), `config/config.go:454-478` (end of `validateStorageIntegrity`)
- Test: `config/config_test.go:276-289` (replace two subtests of `TestConfigValidate_StorageIntegrityAssembly`), `:320` (insert `TestMigrateHousegateStorageIntegrity`)

**Interfaces:**

- Consumes: housegate `StorageIntegrityConfig.Enabled *bool`, `IsEnabled()`, `Tables`.
- Produces: `func (c *Config) migrateHousegateStorageIntegrity() (warning string, err error)`, called by `validateStorageIntegrity` when `storage_integrity.enabled` (B10). After a successful `LoadConfig`/`Validate`, an enabled node has `Housegate.StorageIntegrity.Enabled == &true` and `Tables == nil`.

- [ ] **Step 1: Write the failing tests.**

`config/config_test.go` edit 1 of 2, replace:

```go
	t.Run("table id not in housegate storage_integrity.tables", func(t *testing.T) {
		cfg := base
		cfg.StorageIntegrity.SNode.TableIDs = append(cfg.StorageIntegrity.SNode.TableIDs, "ghost.t")
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), `storage_integrity.snode.table_ids[1] "ghost.t" is not listed in housegate.storage_integrity.tables`)
	})

	t.Run("housegate table list may be a superset", func(t *testing.T) {
		cfg := base
		cfg.Housegate.StorageIntegrity.Tables = append(cfg.Housegate.StorageIntegrity.Tables, "catalog.products")
		require.NoError(t, cfg.Validate())
	})
```

with:

```go
	t.Run("genesis table ids need no housegate table list", func(t *testing.T) {
		cfg := base
		cfg.Housegate.StorageIntegrity.Tables = nil
		cfg.StorageIntegrity.SNode.TableIDs = []string{"orders.t", "ghost.t"}
		require.NoError(t, cfg.Validate())
	})

	t.Run("a legacy housegate list that differs from the genesis set is an error", func(t *testing.T) {
		for name, tables := range map[string][]string{
			"superset": {"orders.t", "ghost.t", "catalog.products"},
			"subset":   {"orders.t"},
			"other":    {"orders.t", "catalog.products"},
		} {
			cfg := base
			cfg.StorageIntegrity.SNode.TableIDs = []string{"orders.t", "ghost.t"}
			cfg.Housegate.StorageIntegrity.Tables = tables
			err := cfg.Validate()
			require.Error(t, err, name)
			require.Contains(t, err.Error(), "differs from storage_integrity.snode.table_ids", name)
		}
	})
```

`config/config_test.go` edit 2 of 2, replace:

```go
func TestStorageIntegrityIngressNetworkIDDefaultsToSNodeNetworkID(t *testing.T) {
```

with:

```go
func TestMigrateHousegateStorageIntegrity(t *testing.T) {
	newConfig := func(legacy []string) Config {
		cfg := indexerConfig(t)
		cfg.StorageIntegrity = validStorageIntegrityConfig()
		cfg.StorageIntegrity.SNode.TableIDs = []string{"orders.t", "catalog.products"}
		cfg.Housegate.StorageIntegrity.Tables = legacy
		return cfg
	}

	t.Run("a legacy list equal to the genesis set is dropped with a warning", func(t *testing.T) {
		cfg := newConfig([]string{"catalog.products", "orders.t"})
		warning, err := cfg.migrateHousegateStorageIntegrity()
		require.NoError(t, err)
		require.Contains(t, warning, "housegate.storage_integrity.tables is deprecated and ignored")
		require.Empty(t, cfg.Housegate.StorageIntegrity.Tables)
		require.NotNil(t, cfg.Housegate.StorageIntegrity.Enabled)
		require.True(t, *cfg.Housegate.StorageIntegrity.Enabled)
		require.True(t, cfg.Housegate.StorageIntegrity.IsEnabled())

		warning, err = cfg.migrateHousegateStorageIntegrity()
		require.NoError(t, err, "the migration must be idempotent: Run validates a loaded config again")
		require.Empty(t, warning)
	})

	t.Run("no legacy list forces the switch on silently", func(t *testing.T) {
		cfg := newConfig(nil)
		warning, err := cfg.migrateHousegateStorageIntegrity()
		require.NoError(t, err)
		require.Empty(t, warning)
		require.True(t, cfg.Housegate.StorageIntegrity.IsEnabled())
	})

	t.Run("an explicit housegate switch off is an error", func(t *testing.T) {
		cfg := newConfig(nil)
		off := false
		cfg.Housegate.StorageIntegrity.Enabled = &off
		_, err := cfg.migrateHousegateStorageIntegrity()
		require.ErrorContains(t, err, "housegate.storage_integrity.enabled must not be false")
	})

	t.Run("a duplicate-bearing legacy list is not the genesis set", func(t *testing.T) {
		cfg := newConfig([]string{"orders.t", "orders.t"})
		_, err := cfg.migrateHousegateStorageIntegrity()
		require.ErrorContains(t, err, "differs from storage_integrity.snode.table_ids")
	})

	t.Run("storage integrity disabled leaves housegate untouched", func(t *testing.T) {
		cfg := newConfig([]string{"x.y"})
		cfg.StorageIntegrity.Enabled = false
		require.NoError(t, cfg.Validate())
		require.Equal(t, []string{"x.y"}, cfg.Housegate.StorageIntegrity.Tables)
		require.Nil(t, cfg.Housegate.StorageIntegrity.Enabled)
	})
}

func TestStorageIntegrityIngressNetworkIDDefaultsToSNodeNetworkID(t *testing.T) {
```

- [ ] **Step 2: Run them and verify they fail.** `go test ./config/`. Expected: build failure, `cfg.migrateHousegateStorageIntegrity undefined (type Config has no field or method migrateHousegateStorageIntegrity)`.
- [ ] **Step 3: Implement.**

`config/config.go` edit 1 of 2, replace:

```go
	housegateConfig "github.com/housegate/housegate/pkg/config"
```

with:

```go
	"compute-network-node/common/log"

	housegateConfig "github.com/housegate/housegate/pkg/config"
```

`config/config.go` edit 2 of 2, replace:

```go
	declared := make(map[string]bool, len(c.Housegate.StorageIntegrity.Tables))
	for _, tableID := range c.Housegate.StorageIntegrity.Tables {
		declared[tableID] = true
	}
	for i, tableID := range c.StorageIntegrity.SNode.TableIDs {
		if got, want := snode.CHTableName(tableID), sicore.PhysicalTableName(tableID); got != want {
			return fmt.Errorf(
				"storage_integrity.snode.table_ids[%d] %q: arbiter-core and housegate disagree on the physical table name (D2 freeze broken): %q != %q",
				i,
				tableID,
				got,
				want,
			)
		}
		if !declared[tableID] {
			return fmt.Errorf(
				"storage_integrity.snode.table_ids[%d] %q is not listed in housegate.storage_integrity.tables (the merge guard, ingress and read rewrite derive hg_unsafe/hg_safe.%s from it)",
				i,
				tableID,
				snode.CHTableName(tableID),
			)
		}
	}
	return nil
}
```

with:

```go
	for i, tableID := range c.StorageIntegrity.SNode.TableIDs {
		if got, want := snode.CHTableName(tableID), sicore.PhysicalTableName(tableID); got != want {
			return fmt.Errorf(
				"storage_integrity.snode.table_ids[%d] %q: arbiter-core and housegate disagree on the physical table name (D2 freeze broken): %q != %q",
				i,
				tableID,
				got,
				want,
			)
		}
	}
	warning, err := c.migrateHousegateStorageIntegrity()
	if err != nil {
		return err
	}
	if warning != "" {
		log.Warnf("%s", warning)
	}
	return nil
}

// migrateHousegateStorageIntegrity applies the dynamic table-set migration
// (design 2026-09-25 §9.4) to an enabled storage-integrity config. The
// embedded HouseGate reads its table set from the injected table state, so
// its switch is forced on and its static table list must be empty. A legacy
// list equal to storage_integrity.snode.table_ids (the genesis set) is dropped
// with the returned deprecation warning; any other list is an error. It is
// idempotent: a second call finds the list already dropped.
func (c *Config) migrateHousegateStorageIntegrity() (string, error) {
	hg := &c.Housegate.StorageIntegrity
	if hg.Enabled != nil && !*hg.Enabled {
		return "", errors.New("housegate.storage_integrity.enabled must not be false when storage_integrity.enabled is true")
	}
	var warning string
	if len(hg.Tables) > 0 {
		if !sameTableSet(hg.Tables, c.StorageIntegrity.SNode.TableIDs) {
			return "", fmt.Errorf(
				"housegate.storage_integrity.tables %v differs from storage_integrity.snode.table_ids %v; the list is deprecated (the table set now comes from the arbiter registry, with snode.table_ids as the genesis set): remove it",
				hg.Tables,
				c.StorageIntegrity.SNode.TableIDs,
			)
		}
		warning = "housegate.storage_integrity.tables is deprecated and ignored: it equals storage_integrity.snode.table_ids, which is the genesis table set; remove it"
		hg.Tables = nil
	}
	enabled := true
	hg.Enabled = &enabled
	return warning, nil
}

func sameTableSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]int, len(a))
	for _, id := range a {
		set[id]++
	}
	for _, id := range b {
		if set[id] == 0 {
			return false
		}
		set[id]--
	}
	return true
}
```

- [ ] **Step 4: Run the package.** `go test ./config/`, `bazel run //:gazelle` (adds `//common/log` to `config`'s deps), `bazel test //config:config_test`. Expected: `ok` / `PASSED`, with the deprecation warning logged by the subtests that validate a legacy list.
- [ ] **Step 5: Commit.**

```bash
git add config/config.go config/config_test.go config/BUILD.bazel
git commit -F - <<'EOF'
feat(config): take the storage-integrity table set from the registry, with snode.table_ids as genesis

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 11: Declare every post-CREATE schema, canonically spelled

**Files:**

- Modify: `database_registry/schema_declarer.go:18-19, 122-123, 139-143, 145-148, 180-185, 192-194, 232-233, 242-248, 263`
- Test: create `database_registry/schema_declarer_test.go`

**Interfaces:**

- Consumes: `payloadexec.CanonicalColumnType(typeName string) (string, error)`.
- Produces: `DeclarePhysical` never reads the latest declaration and always submits; `Declare` (genesis backfill) keeps its skip-if-identical rule; both declare `canonicalDeclaredSchema(schema)` (B8).

- [ ] **Step 1: Write the failing tests.** Create `database_registry/schema_declarer_test.go`:

```go
package database_registry

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/stretchr/testify/require"
)

// TestSchemaDeclarer_DeclarePhysicalDeclaresEvenWhenLatestMatches pins the
// recreation rule: the registry reads only the first declaration after a
// TableCreated, and contract versions survive DROP and re-CREATE, so the
// post-CREATE and compensation paths must declare even when the latest
// on-chain hash (an earlier incarnation's) is identical.
func TestSchemaDeclarer_DeclarePhysicalDeclaresEvenWhenLatestMatches(t *testing.T) {
	const networkID = "testnet"
	schema := payloadexec.TableSchema{TableID: "alpha.events", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}}}
	contract := &fakeContract{latestVersion: 3, latestHash: schemaHashBytes(t, networkID, schema)}
	declarer := newSchemaDeclarerWithLoader(newTestEnv(t), contract, &fakeSchemaLoader{schemas: []payloadexec.TableSchema{schema}}, networkID, "")

	status, err := declarer.DeclarePhysical(context.Background(), "alpha", "events", ownerAddr, "devnet2", "alpha.events", "")
	require.NoError(t, err)
	require.Equal(t, SchemaDeclarationDeclared, status)
	require.Equal(t, 1, contract.setSchemaCalls)
	require.Zero(t, contract.latestCalls, "the physical path must not consult the latest declaration")
}

// TestSchemaDeclarer_CanonicalizesColumnTypes pins design §9.5: every
// supported column type is declared in its canonical spelling, and an
// unsupported type is still declared verbatim so the arbiter records the
// incarnation Refused with a reason the client can read.
func TestSchemaDeclarer_CanonicalizesColumnTypes(t *testing.T) {
	const networkID = "testnet"
	loaded := payloadexec.TableSchema{TableID: "alpha.events", PartitionBy: "p", Columns: []lthash.Column{
		{Name: "p", Type: "String"},
		{Name: "at", Type: "DateTime64(3,'UTC')"},
		{Name: "digest", Type: "FixedString(032)"},
		{Name: "amount", Type: "Decimal(18, 4)"},
	}}
	contract := &fakeContract{}
	declarer := newSchemaDeclarerWithLoader(newTestEnv(t), contract, &fakeSchemaLoader{schemas: []payloadexec.TableSchema{loaded}}, networkID, "")

	status, err := declarer.DeclarePhysical(context.Background(), "alpha", "events", ownerAddr, "devnet2", "alpha.events", "")
	require.NoError(t, err)
	require.Equal(t, SchemaDeclarationDeclared, status)

	want := payloadexec.TableSchema{TableID: "alpha.events", PartitionBy: "p", Columns: []lthash.Column{
		{Name: "p", Type: "String"},
		{Name: "at", Type: "DateTime64(3, 'UTC')"},
		{Name: "digest", Type: "FixedString(32)"},
		{Name: "amount", Type: "Decimal(18, 4)"},
	}}
	var declared payloadexec.TableSchema
	require.NoError(t, json.Unmarshal([]byte(contract.setSchemaJSON), &declared))
	require.Equal(t, want, declared)
	require.Equal(t, schemaHashBytes(t, networkID, want), contract.setSchemaHash, "the hash commits to the canonical spelling")
}
```

- [ ] **Step 2: Run them and verify they fail.** `go test ./database_registry/ -run SchemaDeclarer`. Expected: `--- FAIL: TestSchemaDeclarer_DeclarePhysicalDeclaresEvenWhenLatestMatches` (status `skipped`, 0 calls) and `--- FAIL: TestSchemaDeclarer_CanonicalizesColumnTypes` (`DateTime64(3,'UTC')` and `FixedString(032)` declared verbatim).
- [ ] **Step 3: Implement.**

`database_registry/schema_declarer.go` edit 1 of 9, replace:

```go
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
```

with:

```go
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
```

`database_registry/schema_declarer.go` edit 2 of 9, replace:

```go
// Declare derives and declares one logical database/table pair. Re-running it
// is idempotent: an identical latest commitment is skipped.
```

with:

```go
// Declare derives and declares one logical database/table pair from its
// hg_unsafe protocol table. Re-running it is idempotent: an identical latest
// commitment is skipped. It serves the genesis backfill command only.
```

`database_registry/schema_declarer.go` edit 3 of 9, replace:

```go
			Table:    snode.CHTableName(logicalID),
		},
		d.loader,
	)
}
```

with:

```go
			Table:    snode.CHTableName(logicalID),
		},
		d.loader,
		true,
	)
}
```

`database_registry/schema_declarer.go` edit 4 of 9, replace:

```go
// DeclarePhysical derives and declares one logical table from its exact
// post-rewrite ClickHouse coordinates. The physical table name is supplied by
// the commitgate observer instead of assuming storage-integrity's hg_unsafe
// naming convention.
```

with:

```go
// DeclarePhysical derives and declares one logical table from its exact
// post-rewrite ClickHouse coordinates. The physical table name is supplied by
// the commitgate observer (or the compensation loop) instead of assuming
// storage-integrity's hg_unsafe naming convention. It always declares: the
// storage-integrity registry reads only the first declaration after the
// table's TableCreated, and contract schema versions survive DROP and
// re-CREATE, so an identical latest hash may belong to an earlier incarnation.
```

`database_registry/schema_declarer.go` edit 5 of 9, replace:

```go
			Database: physicalDatabase,
			Table:    physicalTable,
		},
		loader,
	)
}
```

with:

```go
			Database: physicalDatabase,
			Table:    physicalTable,
		},
		loader,
		false,
	)
}
```

`database_registry/schema_declarer.go` edit 6 of 9, replace:

```go
	ref schemaregistry.TableRef,
	loader schemaregistry.Loader,
) (SchemaDeclarationStatus, error) {
```

with:

```go
	ref schemaregistry.TableRef,
	loader schemaregistry.Loader,
	skipIdenticalLatest bool,
) (SchemaDeclarationStatus, error) {
```

`database_registry/schema_declarer.go` edit 7 of 9, replace:

```go
	schema := schemas[0]
	schemaJSON, err := json.Marshal(schema)
```

with:

```go
	schema := canonicalDeclaredSchema(schemas[0])
	schemaJSON, err := json.Marshal(schema)
```

`database_registry/schema_declarer.go` edit 8 of 9, replace:

```go
	version, latestHash, err := d.contract.latestTableSchema(ctx, databaseID, tableID)
	if err != nil {
		return SchemaDeclarationFailed, fmt.Errorf("read latest schema for %s: %w", logicalID, err)
	}
	if version != 0 && bytes.Equal(latestHash[:], hash[:]) {
		return SchemaDeclarationSkipped, nil
	}
```

with:

```go
	if skipIdenticalLatest {
		version, latestHash, err := d.contract.latestTableSchema(ctx, databaseID, tableID)
		if err != nil {
			return SchemaDeclarationFailed, fmt.Errorf("read latest schema for %s: %w", logicalID, err)
		}
		if version != 0 && bytes.Equal(latestHash[:], hash[:]) {
			return SchemaDeclarationSkipped, nil
		}
	}
```

`database_registry/schema_declarer.go` edit 9 of 9, replace:

```go
func decodeSchemaHash(hash string) ([32]byte, error) {
```

with:

```go
// canonicalDeclaredSchema spells every supported column type canonically
// (design 2026-09-25 §9.5), so a ClickHouse rendering such as
// DateTime64(3,'UTC') cannot make the arbiter refuse the declaration as
// non-canonical. An unsupported type is kept verbatim: the arbiter records the
// incarnation Refused and the client receives its code and reason.
func canonicalDeclaredSchema(schema payloadexec.TableSchema) payloadexec.TableSchema {
	out := schema
	out.Columns = make([]lthash.Column, len(schema.Columns))
	for i, column := range schema.Columns {
		if canonical, err := payloadexec.CanonicalColumnType(column.Type); err == nil {
			column.Type = canonical
		}
		out.Columns[i] = column
	}
	return out
}

func decodeSchemaHash(hash string) ([32]byte, error) {
```

- [ ] **Step 4: Run the package.** `go test ./database_registry/`, `bazel run //:gazelle`, `bazel test //database_registry:database_registry_test`. Expected: `ok` / `PASSED` (`TestSchemaDeclarer_DeclareAndSkipIdentical` still passes: the backfill path skips).
- [ ] **Step 5: Commit.**

```bash
git add database_registry/schema_declarer.go database_registry/schema_declarer_test.go database_registry/BUILD.bazel
git commit -F - <<'EOF'
fix(database-registry): declare every post-CREATE schema and spell column types canonically

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 12: Compensate lost schema declarations

**Files:**

- Create: `database_registry/compensation.go`
- Modify: `database_registry/schema_declarer.go:35-38` (`SchemaDeclarationConfig.Upstreams`), `standalone/schema_registry.go:136-139`
- Test: create `database_registry/compensation_test.go`

**Interfaces:**

- Consumes: Task 6's `DatabaseInfos`, Task 11's `DeclarePhysical`, `env.IndexerRuntime.{IndexerId,SignerAddress}`.
- Produces:

```go
const DefaultCompensationInterval = 5 * time.Minute
const DefaultCompensationMinAge = 10 * time.Minute
const CompensationDeclared, CompensationFailed = "declared", "failed"
type CompensationChain interface { DatabaseInfos(ctx context.Context) (map[string]statecore.DatabaseInfo, error) }
type CompensationConfig struct { PhysicalDatabase string; Interval, MinAge time.Duration; Observe func(outcome string); Now func() time.Time }
func NewSchemaCompensator(env *common.NodeEnv, chain CompensationChain, declaration SchemaDeclarationConfig, cfg CompensationConfig) (*SchemaCompensator, error)
func (c *SchemaCompensator) Pass(ctx context.Context) error
func (c *SchemaCompensator) Run(ctx context.Context) error // returns ctx.Err()
// SchemaDeclarationConfig gains: Upstreams []string
```

- [ ] **Step 1: Write the failing tests.** Create `database_registry/compensation_test.go`:

```go
package database_registry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	statecore "sentioxyz/sentio-core/network/state"

	"github.com/stretchr/testify/require"
)

type compensationCall struct {
	database, table, caller, physicalDatabase, physicalTable, upstream string
}

type recordingDeclarer struct {
	mu    sync.Mutex
	calls []compensationCall
	fail  map[string]error // keyed by upstream
}

func (r *recordingDeclarer) DeclarePhysical(
	_ context.Context,
	databaseID, tableID, caller, physicalDatabase, physicalTable, upstreamAddress string,
) (SchemaDeclarationStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, compensationCall{databaseID, tableID, caller, physicalDatabase, physicalTable, upstreamAddress})
	if err := r.fail[upstreamAddress]; err != nil {
		return SchemaDeclarationFailed, err
	}
	return SchemaDeclarationDeclared, nil
}

type staticChain struct {
	databases map[string]statecore.DatabaseInfo
	err       error
}

func (s *staticChain) DatabaseInfos(context.Context) (map[string]statecore.DatabaseInfo, error) {
	return s.databases, s.err
}

func compensationChainFixture() *staticChain {
	return &staticChain{databases: map[string]statecore.DatabaseInfo{
		"mine": {DatabaseId: "mine", IndexerId: 7, Tables: []statecore.TableInfo{
			{TableId: "lost", CreatedBlock: 500},
			{TableId: "declared", CreatedBlock: 500, SchemaVersion: 1, SchemaHash: "0x01"},
			{TableId: "legacy", CreatedBlock: 0},
		}},
		"dropping": {DatabaseId: "dropping", IndexerId: 7, PendingDelete: true, Tables: []statecore.TableInfo{
			{TableId: "lost", CreatedBlock: 500},
		}},
		"peer": {DatabaseId: "peer", IndexerId: 9, Tables: []statecore.TableInfo{
			{TableId: "lost", CreatedBlock: 500},
		}},
	}}
}

func TestSchemaCompensatorSelectsOnlyLostDeclarationsAfterMinAge(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	declarer := &recordingDeclarer{}
	var outcomes []string
	chain := compensationChainFixture()
	c, err := newSchemaCompensator(declarer, chain, []string{"ch-0:9000"}, func() uint64 { return 7 }, func() string { return "0xsigner" }, CompensationConfig{
		PhysicalDatabase: "devnet2",
		Observe:          func(outcome string) { outcomes = append(outcomes, outcome) },
		Now:              func() time.Time { return now },
	})
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, c.Pass(ctx))
	require.Empty(t, declarer.calls, "a table first seen now is not due")

	now = now.Add(DefaultCompensationMinAge - time.Second)
	require.NoError(t, c.Pass(ctx))
	require.Empty(t, declarer.calls, "a table younger than the minimum age is not due")

	now = now.Add(time.Second)
	require.NoError(t, c.Pass(ctx))
	require.Equal(t, []compensationCall{{"mine", "lost", "0xsigner", "devnet2", "mine.lost", "ch-0:9000"}}, declarer.calls,
		"only this indexer's live database, a post-upgrade creation and no declaration since it qualify")
	require.Equal(t, []string{CompensationDeclared}, outcomes)

	// A recreation (new CreatedBlock) restarts the clock.
	chain.databases["mine"].Tables[0] = statecore.TableInfo{TableId: "lost", CreatedBlock: 900}
	require.NoError(t, c.Pass(ctx))
	require.Len(t, declarer.calls, 1)
}

func TestSchemaCompensatorTriesEveryReplicaAndRetriesFailures(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	declarer := &recordingDeclarer{fail: map[string]error{"ch-0:9000": errors.New("table not found"), "ch-1:9000": errors.New("table not found")}}
	var outcomes []string
	c, err := newSchemaCompensator(declarer, compensationChainFixture(), []string{"ch-0:9000", "ch-1:9000"}, func() uint64 { return 7 }, func() string { return "0xsigner" }, CompensationConfig{
		PhysicalDatabase: "devnet2",
		MinAge:           time.Minute,
		Observe:          func(outcome string) { outcomes = append(outcomes, outcome) },
		Now:              func() time.Time { return now },
	})
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, c.Pass(ctx))
	now = now.Add(time.Minute)

	require.NoError(t, c.Pass(ctx))
	require.Len(t, declarer.calls, 2, "each replica is tried once")
	require.Equal(t, []string{CompensationFailed}, outcomes)

	delete(declarer.fail, "ch-1:9000")
	require.NoError(t, c.Pass(ctx))
	require.Equal(t, "ch-1:9000", declarer.calls[len(declarer.calls)-1].upstream, "a failed table is retried on the next pass")
	require.Equal(t, []string{CompensationFailed, CompensationDeclared}, outcomes)
}

func TestSchemaCompensatorWaitsForIndexerID(t *testing.T) {
	declarer := &recordingDeclarer{}
	c, err := newSchemaCompensator(declarer, compensationChainFixture(), []string{"ch-0:9000"}, func() uint64 { return 0 }, func() string { return "0xsigner" }, CompensationConfig{
		PhysicalDatabase: "devnet2",
	})
	require.NoError(t, err)
	require.ErrorContains(t, c.Pass(context.Background()), "indexer id is not known yet")
	require.Empty(t, declarer.calls)
}
```

- [ ] **Step 2: Run them and verify they fail.** `go test ./database_registry/`. Expected: build failure, `undefined: newSchemaCompensator`, `undefined: CompensationConfig`.
- [ ] **Step 3: Implement.** Create `database_registry/compensation.go`:

```go
package database_registry

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"compute-network-node/common"
	"compute-network-node/common/log"

	statecore "sentioxyz/sentio-core/network/state"
)

// Declaration compensation defaults (design 2026-09-25 §9.5).
const (
	DefaultCompensationInterval = 5 * time.Minute
	DefaultCompensationMinAge   = 10 * time.Minute
)

// Compensation outcomes passed to CompensationConfig.Observe.
const (
	CompensationDeclared = "declared"
	CompensationFailed   = "failed"
)

// CompensationChain reads the syncer's chain state.
type CompensationChain interface {
	DatabaseInfos(ctx context.Context) (map[string]statecore.DatabaseInfo, error)
}

// CompensationConfig tunes the declaration compensation loop.
type CompensationConfig struct {
	// PhysicalDatabase is the ClickHouse database that holds every logical
	// table as `<database>.<table>` (node.PhysicalDatabase).
	PhysicalDatabase string
	Interval         time.Duration
	MinAge           time.Duration
	// Observe receives every attempt's outcome; it may be nil.
	Observe func(outcome string)
	Now     func() time.Time
}

type compensationKey struct {
	database     string
	table        string
	createdBlock uint64
}

// SchemaCompensator re-declares the schema of tables whose post-CREATE
// declaration was lost (design 2026-09-25 §9.5). A table qualifies when it
// belongs to this indexer's databases, was created after the syncer began
// recording creation blocks (CreatedBlock > 0, which excludes tables that the
// registry seeds as Legacy), has no declaration since that creation
// (SchemaVersion == 0), and was first seen at least MinAge ago.
type SchemaCompensator struct {
	declarer  schemaDeclarationRunner
	chain     CompensationChain
	indexerID func() uint64
	caller    func() string
	upstreams []string
	cfg       CompensationConfig
	firstSeen map[compensationKey]time.Time
}

// NewSchemaCompensator builds the loop over the same replica-pinned declarer
// the Observer uses. The caller is this indexer's signer, which the Databases
// contract accepts as a writer of every database the indexer hosts.
func NewSchemaCompensator(
	env *common.NodeEnv,
	chain CompensationChain,
	declaration SchemaDeclarationConfig,
	cfg CompensationConfig,
) (*SchemaCompensator, error) {
	if env == nil {
		return nil, errors.New("database_registry: compensation requires a node env")
	}
	if declaration.OpenConnection == nil {
		return nil, errors.New("database_registry: compensation requires a replica-pinned schema connection")
	}
	declarer := newSchemaDeclarerWithConnectionOpener(env, envContract{env: env}, declaration.OpenConnection, declaration.NetworkID)
	return newSchemaCompensator(declarer, chain, declaration.Upstreams,
		func() uint64 {
			if env.IndexerRuntime == nil || env.IndexerRuntime.IndexerId == nil {
				return 0
			}
			return env.IndexerRuntime.IndexerId.Uint64()
		},
		func() string {
			if env.IndexerRuntime == nil {
				return ""
			}
			return env.IndexerRuntime.SignerAddress.Hex()
		},
		cfg,
	)
}

func newSchemaCompensator(
	declarer schemaDeclarationRunner,
	chain CompensationChain,
	upstreams []string,
	indexerID func() uint64,
	caller func() string,
	cfg CompensationConfig,
) (*SchemaCompensator, error) {
	if declarer == nil || chain == nil || indexerID == nil || caller == nil {
		return nil, errors.New("database_registry: compensation requires a declarer, chain state, indexer id and caller")
	}
	if strings.TrimSpace(cfg.PhysicalDatabase) == "" {
		return nil, errors.New("database_registry: compensation requires the physical database")
	}
	if len(upstreams) == 0 {
		return nil, errors.New("database_registry: compensation requires at least one local ClickHouse upstream")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultCompensationInterval
	}
	if cfg.MinAge <= 0 {
		cfg.MinAge = DefaultCompensationMinAge
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &SchemaCompensator{
		declarer:  declarer,
		chain:     chain,
		indexerID: indexerID,
		caller:    caller,
		upstreams: append([]string(nil), upstreams...),
		cfg:       cfg,
		firstSeen: map[compensationKey]time.Time{},
	}, nil
}

// Run passes once per Interval until ctx ends.
func (c *SchemaCompensator) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.Pass(ctx); err != nil && ctx.Err() == nil {
				log.WithContext(ctx).Warnf("table schema compensation pass failed: %v", err)
			}
		}
	}
}

// Pass selects the qualifying tables and declares each one. A per-table
// failure is logged and counted, and the table is retried on the next pass.
func (c *SchemaCompensator) Pass(ctx context.Context) error {
	databases, err := c.chain.DatabaseInfos(ctx)
	if err != nil {
		return fmt.Errorf("read chain state: %w", err)
	}
	self := c.indexerID()
	if self == 0 {
		return errors.New("indexer id is not known yet")
	}
	now := c.cfg.Now()
	candidates := map[compensationKey]struct{}{}
	for databaseID, database := range databases {
		if database.IndexerId != self || database.PendingDelete {
			continue
		}
		for _, table := range database.Tables {
			if table.CreatedBlock == 0 || table.SchemaVersion != 0 {
				continue
			}
			candidates[compensationKey{database: databaseID, table: table.TableId, createdBlock: table.CreatedBlock}] = struct{}{}
		}
	}
	for key := range c.firstSeen {
		if _, ok := candidates[key]; !ok {
			delete(c.firstSeen, key)
		}
	}
	due := make([]compensationKey, 0, len(candidates))
	for key := range candidates {
		first, seen := c.firstSeen[key]
		if !seen {
			c.firstSeen[key] = now
			continue
		}
		if now.Sub(first) >= c.cfg.MinAge {
			due = append(due, key)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].database != due[j].database {
			return due[i].database < due[j].database
		}
		return due[i].table < due[j].table
	})
	logger := log.WithContext(ctx)
	for _, key := range due {
		if err := c.declare(ctx, key); err != nil {
			c.observe(CompensationFailed)
			logger.Errorf("compensate table schema declaration %s.%s: %v", key.database, key.table, err)
			continue
		}
		c.observe(CompensationDeclared)
		delete(c.firstSeen, key)
		logger.Infof("compensated lost table schema declaration for %s.%s (created at block %d)", key.database, key.table, key.createdBlock)
	}
	return nil
}

func (c *SchemaCompensator) declare(ctx context.Context, key compensationKey) error {
	var errs []error
	for _, upstream := range c.upstreams {
		declareCtx, cancel := context.WithTimeout(ctx, SchemaDeclarationTimeout)
		_, err := c.declarer.DeclarePhysical(declareCtx, key.database, key.table, c.caller(),
			c.cfg.PhysicalDatabase, key.database+"."+key.table, upstream)
		cancel()
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("upstream %s: %w", upstream, err))
	}
	return errors.Join(errs...)
}

func (c *SchemaCompensator) observe(outcome string) {
	if c.cfg.Observe != nil {
		c.cfg.Observe(outcome)
	}
}
```

`database_registry/schema_declarer.go`, replace:

```go
	Conn clickhouse.Conn

	NetworkID string
}
```

with:

```go
	Conn clickhouse.Conn

	NetworkID string

	// Upstreams are the local ClickHouse replicas OpenConnection accepts. The
	// declaration compensation loop, which has no query event to name the
	// replica, tries them in order.
	Upstreams []string
}
```

`standalone/schema_registry.go`, replace:

```go
	return []database_registry.SchemaDeclarationConfig{{
		OpenConnection: resolved.openUpstream,
		NetworkID:      resolved.networkID,
	}}, nil
```

with:

```go
	return []database_registry.SchemaDeclarationConfig{{
		OpenConnection: resolved.openUpstream,
		NetworkID:      resolved.networkID,
		Upstreams:      append([]string(nil), resolved.addrs...),
	}}, nil
```

- [ ] **Step 4: Run the packages.** `go vet ./database_registry/ ./standalone/ && go test -race ./database_registry/ ./standalone/`, `bazel run //:gazelle`, `bazel test //database_registry:database_registry_test //standalone:standalone_test`. Expected: `ok` / `PASSED`.
- [ ] **Step 5: Commit.**

```bash
git add database_registry/compensation.go database_registry/compensation_test.go database_registry/schema_declarer.go database_registry/BUILD.bazel standalone/schema_registry.go
git commit -F - <<'EOF'
feat(database-registry): re-declare schemas whose post-CREATE declaration was lost

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 13: Inject the dynamic table state into the embedded HouseGate

**Files:**

- Create: `standalone/storage_integrity_table_state.go`
- Replace: `standalone/storage_integrity_schemas.go` (whole file, 59 lines)
- Modify: `standalone/standalone.go:139-141, 252-254, 264-266, 275-280, 318-320, 369-380, 383-393, 417, 485, 498-499`, `standalone/startup_transaction.go:468-471`
- Delete: `standalone/storage_integrity_read_state.go`, `standalone/storage_integrity_read_state_test.go`
- Test: create `standalone/storage_integrity_table_state_test.go`; replace `standalone/storage_integrity_schemas_test.go` (whole file, 203 lines); modify `standalone/storage_integrity_bootstrap_test.go:1509-1510`

**Interfaces:**

- Consumes: Tasks 6–12; plan A `dataplane.NewRegistryFollower`, `dataplane.RegistryView`, `dataplane.WaitReady`, `dataplane.DefaultRegistryStartupTimeout`, `snode.Deps.Registry`, `(*snode.Role).TableReady`; housegate `Options.StorageIntegrityTableState`, `Proxy.MetricsRegistry()`.
- Produces (package `standalone`, unexported):

```go
type storageIntegrityRegistryFollower interface { tablestate.RegistryFollower; dataplane.RegistryView; Run(ctx context.Context) error }
type storageIntegrityTableStateDeps struct { follower storageIntegrityRegistryFollower; readiness func(tableID string) bool; chain tablestate.DatabaseInfoReader; genesis []payloadexec.TableSchema; networkID string; logger *slog.Logger; env *common.NodeEnv; declarations []database_registry.SchemaDeclarationConfig; physicalDatabase string }
func newStorageIntegrityTableStateRuntime(deps storageIntegrityTableStateDeps) (*storageIntegrityTableStateRuntime, error)
func (r *storageIntegrityTableStateRuntime) tableState() sitable.TableState          // nil-safe
func (r *storageIntegrityTableStateRuntime) storageRPCOptions() []rpc.StorageNodeOption // nil-safe
func (r *storageIntegrityTableStateRuntime) begin(ctx context.Context, tx *startupTransaction) error
func (r *storageIntegrityTableStateRuntime) registerMetrics(registry *prometheus.Registry) error
func loadStorageIntegritySchemaSets(ctx context.Context, loader schemaregistry.Loader, genesisTableIDs []string) (storageIntegritySchemaSets, error) // {snode, refs}
```

The only `bootstrap_test` assertion that changes is the contract snapshot's table list, now `si.SNode.TableIDs`; every other source-shape audit passes unchanged because `Run` calls only methods of the runtime (Global Constraints). `begin` runs inside the permit callback before `housegate.New` (B7).

- [ ] **Step 1: Write the failing tests.** Create `standalone/storage_integrity_table_state_test.go`:

```go
package standalone

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"sync"
	"testing"
	"time"

	statecore "sentioxyz/sentio-core/network/state"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/stretchr/testify/require"
)

type scriptedRegistryFollower struct {
	mu       sync.Mutex
	snapshot wire.TableRegistrySnapshot
	enabled  bool
	answer   bool // Run closes ready when true
	ready    chan struct{}
	changed  chan struct{}
}

func newScriptedRegistryFollower(snapshot wire.TableRegistrySnapshot, enabled, answer bool) *scriptedRegistryFollower {
	return &scriptedRegistryFollower{snapshot: snapshot, enabled: enabled, answer: answer, ready: make(chan struct{}), changed: make(chan struct{})}
}

func (f *scriptedRegistryFollower) View() (wire.TableRegistrySnapshot, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshot, f.enabled
}

func (f *scriptedRegistryFollower) Changed() <-chan struct{} { return f.changed }
func (f *scriptedRegistryFollower) Connected() bool          { return f.answer }
func (f *scriptedRegistryFollower) Ready() <-chan struct{}   { return f.ready }

func (f *scriptedRegistryFollower) Run(ctx context.Context) error {
	if f.answer {
		close(f.ready)
	}
	<-ctx.Done()
	return ctx.Err()
}

type fixedDatabaseInfos map[string]statecore.DatabaseInfo

func (f fixedDatabaseInfos) DatabaseInfos(context.Context) (map[string]statecore.DatabaseInfo, error) {
	return f, nil
}

func activeTenantRegistry(t *testing.T) wire.TableRegistrySnapshot {
	t.Helper()
	schema := payloadexec.TableSchema{TableID: "tenant.t", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}}}
	return wire.TableRegistrySnapshot{
		Params:  arbiter.TableRegistryParams{ChainID: 1, SIIndexerID: 7, ActivationBlock: 100, Confirmation: "safe"},
		Version: 5,
		Seeded:  true,
		Incarnations: []wire.TableIncarnation{{
			Seq: 1, DatabaseID: "tenant", TableID: "t", Origin: wire.TableOriginChain, Status: wire.TableStatusActive,
			SchemaJSON: `{"table_id":"tenant.t","partition_by":"p","columns":[{"name":"p","type":"String"}]}`,
			SchemaHash: payloadexec.TableSchemaHash("net", schema),
		}},
	}
}

func newTestTableStateRuntime(t *testing.T, follower *scriptedRegistryFollower) *storageIntegrityTableStateRuntime {
	t.Helper()
	runtime, err := newStorageIntegrityTableStateRuntime(storageIntegrityTableStateDeps{
		follower:  follower,
		readiness: func(string) bool { return true },
		chain: fixedDatabaseInfos{"tenant": {DatabaseId: "tenant", IndexerId: 7, Tables: []statecore.TableInfo{
			{TableId: "t", TableType: "user", CreatedBlock: 150},
		}}},
		networkID: "net",
	})
	require.NoError(t, err)
	return runtime
}

// TestStorageIntegrityTableStateBeginPublishesBeforeReturning pins design
// §9.2: the first snapshot is built from the registry before HouseGate runs,
// so begin returns only after the follower answered and the snapshot moved.
func TestStorageIntegrityTableStateBeginPublishesBeforeReturning(t *testing.T) {
	runtime := newTestTableStateRuntime(t, newScriptedRegistryFollower(activeTenantRegistry(t), true, true))
	require.Equal(t, sitable.Ordinary, runtime.tableState().Current().Lookup("tenant", "t").Status,
		"before begin the state serves only the genesis set")

	tx := newStartupTransaction(t.Context())
	t.Cleanup(tx.stopAndWait)
	require.NoError(t, runtime.begin(tx.Context(), tx))
	require.Equal(t, sitable.Active, runtime.tableState().Current().Lookup("tenant", "t").Status)
	require.NoError(t, tx.commit(), "the refresh loop is a registered task that starts at commit")
}

func TestStorageIntegrityTableStateBeginFailsWhenTheRegistryNeverAnswers(t *testing.T) {
	runtime := newTestTableStateRuntime(t, newScriptedRegistryFollower(wire.TableRegistrySnapshot{}, false, false))
	runtime.readyTimeout = 50 * time.Millisecond
	tx := newStartupTransaction(t.Context())
	t.Cleanup(tx.stopAndWait)
	err := runtime.begin(tx.Context(), tx)
	require.ErrorContains(t, err, "wait for the arbiter table registry")
}

func TestStorageIntegrityTableStateRuntimeIsNilSafe(t *testing.T) {
	var runtime *storageIntegrityTableStateRuntime
	require.Nil(t, runtime.tableState(), "a disabled node must hand HouseGate a nil interface")
	require.Empty(t, runtime.storageRPCOptions())
}

func TestStorageIntegrityTableStateMetricsFallBackToTheDefaultRegistry(t *testing.T) {
	runtime := newTestTableStateRuntime(t, newScriptedRegistryFollower(wire.TableRegistrySnapshot{}, false, true))
	registry := prometheus.NewRegistry()
	require.NoError(t, runtime.registerMetrics(registry))
	require.Error(t, runtime.registerMetrics(registry), "a second registration is reported, not a panic")
}

// TestRunBuildsTheTableStateBeforeHousegate is the source-shape guard for the
// new wiring: the first snapshot is published inside the permit runner before
// housegate.New, HouseGate receives the runtime's table state, and the storage
// RPC serves its status method.
func TestRunBuildsTheTableStateBeforeHousegate(t *testing.T) {
	source := readStandaloneSource(t)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "standalone.go", source, 0)
	require.NoError(t, err)
	runDecl := namedFunction(file, "Run")
	require.NotNil(t, runDecl)

	var starts []*ast.CallExpr
	ast.Inspect(runDecl.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok && selectorCall(call, "siListenerPermit", "start") {
			starts = append(starts, call)
		}
		return true
	})
	require.Len(t, starts, 1)
	runner := starts[0].Args[1].(*ast.FuncLit)

	begins := selectorCallsInNode(runDecl.Body, "siTableState", "begin")
	require.Len(t, begins, 1)
	require.True(t, nodeContains(runner.Body, begins[0]), "the follower and first snapshot are owned by the startup transaction")
	require.True(t, identifierExpression(begins[0].Args[0], "runCtx"))
	require.True(t, identifierExpression(begins[0].Args[1], "tx"))
	housegateNew := assignedConstructorCall(t, file, runDecl, "github.com/housegate/housegate", "New")
	require.Less(t, int(begins[0].Pos()), int(housegateNew.Pos()), "HouseGate must start from a snapshot built from the registry")

	var tableStateBindings []ast.Expr
	ast.Inspect(runDecl.Body, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}
		tableStateBindings = append(tableStateBindings, compositeFieldValues(literal, "StorageIntegrityTableState")...)
		return true
	})
	require.Len(t, tableStateBindings, 1)
	call, ok := tableStateBindings[0].(*ast.CallExpr)
	require.True(t, ok)
	require.True(t, selectorCall(call, "siTableState", "tableState"))

	require.Len(t, selectorCallsInNode(runDecl.Body, "siTableState", "storageRPCOptions"), 1)
	require.Len(t, selectorCallsInNode(runDecl.Body, "siTableState", "registerMetrics"), 1)
}
```

Replace the whole of `standalone/storage_integrity_schemas_test.go` with:

```go
package standalone

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"compute-network-node/storageintegrityadapter/tablestate"
	statecore "sentioxyz/sentio-core/network/state"

	"github.com/housegate/housegate"
	housegateConfig "github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/schemaregistry"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
	rewriterpb "github.com/housegate/rewriter-proto/gen/pb"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/stretchr/testify/require"
)

type recordingStorageIntegritySchemaLoader struct {
	refs    []schemaregistry.TableRef
	schemas []payloadexec.TableSchema
}

func (l *recordingStorageIntegritySchemaLoader) Load(_ context.Context, refs []schemaregistry.TableRef) ([]payloadexec.TableSchema, error) {
	l.refs = append([]schemaregistry.TableRef(nil), refs...)
	return append([]payloadexec.TableSchema(nil), l.schemas...), nil
}

func TestLoadStorageIntegritySchemaSetsLoadsTheGenesisSet(t *testing.T) {
	loader := &recordingStorageIntegritySchemaLoader{schemas: []payloadexec.TableSchema{
		{TableID: "catalog.products"},
		{TableID: "orders.t"},
	}}

	got, err := loadStorageIntegritySchemaSets(t.Context(), loader, []string{"orders.t", "catalog.products"})
	require.NoError(t, err)
	require.Equal(t, []schemaregistry.TableRef{
		{TableID: "orders.t", Database: "hg_unsafe", Table: "orders__t"},
		{TableID: "catalog.products", Database: "hg_unsafe", Table: "catalog__products"},
	}, loader.refs)
	require.Equal(t, []payloadexec.TableSchema{{TableID: "orders.t"}, {TableID: "catalog.products"}}, got.snode,
		"the genesis schemas keep the configured order")
	require.Equal(t, loader.refs, got.refs)

	loader.schemas = loader.schemas[:1]
	_, err = loadStorageIntegritySchemaSets(t.Context(), loader, []string{"orders.t", "catalog.products"})
	require.ErrorContains(t, err, `genesis table "orders.t" has no loaded schema`)
}

// TestHousegateNewAcceptsTheInjectedTableState boots the embedded HouseGate the
// way standalone.Run now configures it (design 2026-09-25 §9.4): the switch is
// on, the static table list is empty, the table set comes from the injected
// table state and the read state is the SNode role, so unsafe_latest is
// available as a default.
func TestHousegateNewAcceptsTheInjectedTableState(t *testing.T) {
	for _, defaultMode := range []string{"safe", "unsafe_latest"} {
		t.Run(defaultMode, func(t *testing.T) {
			state, err := tablestate.New(tablestate.Config{
				Registry:  tablestate.FromFollower(disabledRegistryFollower{}),
				Readiness: tablestate.ReadinessFunc(func(string) bool { return true }),
				Chain:     tablestate.DatabaseInfoChain(emptyDatabaseInfos{}),
				Genesis:   []payloadexec.TableSchema{{TableID: "orders.t"}},
				NetworkID: "testnet",
			})
			require.NoError(t, err)
			require.NoError(t, state.Refresh(t.Context()))

			enabled := true
			cfg := housegateConfig.Default()
			cfg.Listen = "127.0.0.1:0"
			cfg.StorageIntegrity.Enabled = &enabled
			cfg.StorageIntegrity.Read.DefaultMode = defaultMode
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.NetworkID = "testnet"
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x0000000000000000000000000000000000000001"}
			cfg.StorageIntegrity.Ingress.MaxTokenAge = housegateConfig.Duration{Duration: time.Minute}
			cfg.StorageIntegrity.Ingress.RequestTimeout = housegateConfig.Duration{Duration: time.Second}
			cfg.StorageIntegrity.Ingress.MaxPayloadBytes = 1 << 20
			cfg.StorageIntegrity.Runtime.Enabled = true
			cfg.StorageIntegrity.Runtime.ExpectedSource = "snode-1"
			cfg.StorageIntegrity.Runtime.JournalDir = filepath.Join(t.TempDir(), "journal")
			cfg.StorageIntegrity.Runtime.PayloadSpoolDir = filepath.Join(t.TempDir(), "payload-spool")
			cfg.StorageIntegrity.Runtime.Backpressure.Enabled = false

			proxy, err := housegate.New(housegate.Options{
				Config:                     &cfg,
				NetworkState:               network.NewInMemoryNetworkState(),
				Rewriter:                   storageIntegrityCapableRewriterFactory{},
				StorageIntegrityReadState:  noPromotedParts{},
				StorageIntegrityTableState: state,
				StorageIntegrityRuntime: housegate.StorageIntegrityRuntimeOptions{
					StatementSubmitter: storageIntegrityRuntimePorts{},
					SourcePreparer:     storageIntegrityRuntimePorts{},
					StatusQuerier:      storageIntegrityRuntimePorts{},
					PayloadWriter:      storageIntegrityRuntimePorts{},
					MergeGuard:         storageIntegrityRuntimePorts{},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, proxy)

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = listener.Close() })
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_ = proxy.RunWith(ctx, listener)
		})
	}
}

type noPromotedParts struct{}

func (noPromotedParts) PromotedUnsafeParts(string) ([]string, error) { return nil, nil }

type disabledRegistryFollower struct{}

func (disabledRegistryFollower) View() (wire.TableRegistrySnapshot, bool) {
	return wire.TableRegistrySnapshot{}, false
}

func (disabledRegistryFollower) Changed() <-chan struct{} { return nil }

func (disabledRegistryFollower) Connected() bool { return false }

type emptyDatabaseInfos struct{}

func (emptyDatabaseInfos) DatabaseInfos(context.Context) (map[string]statecore.DatabaseInfo, error) {
	return map[string]statecore.DatabaseInfo{}, nil
}

type storageIntegrityCapableRewriterFactory struct{}

func (storageIntegrityCapableRewriterFactory) NewRewriter(rewriter.Session) rewriter.Rewriter {
	return storageIntegrityNoopRewriter{}
}

func (storageIntegrityCapableRewriterFactory) Close() error { return nil }

func (storageIntegrityCapableRewriterFactory) StorageIntegrityContractVersion() rewriterpb.StorageIntegrityContractVersion {
	return rewriter.StorageIntegrityContractV2
}

// ProbeStorageIntegrityBuild satisfies rewriter.StorageIntegrityProbeFactory.
// HouseGate refuses startup for an enabled storage-integrity surface unless the
// factory acknowledges contract V2 and exposes the behavioural conformance
// probe, because an old engine can acknowledge the contract while missing the
// fail-closed behaviour. This fixture is not an engine, so the probe is a
// no-op; real engine conformance is covered by HouseGate's and rewriter-go's
// own suites.
func (storageIntegrityCapableRewriterFactory) ProbeStorageIntegrityBuild(context.Context) error {
	return nil
}

type storageIntegrityNoopRewriter struct{}

func (storageIntegrityNoopRewriter) Rewrite(_ context.Context, sql, _ string) (rewriter.RewriteResult, error) {
	return rewriter.RewriteResult{SQL: sql}, nil
}

func (storageIntegrityNoopRewriter) RewriteErrorMessage(_ context.Context, message string) (string, error) {
	return message, nil
}

func (storageIntegrityNoopRewriter) Close() error { return nil }

type storageIntegrityRuntimePorts struct{}

func (storageIntegrityRuntimePorts) SubmitStatement(context.Context, sicore.StatementEnvelope) (sicore.SubmitOutcome, error) {
	return sicore.SubmitOutcome{}, nil
}

func (storageIntegrityRuntimePorts) PrepareLocalStatement(context.Context, sicore.StatementEnvelope, []byte) (sicore.PreparedLocalResult, error) {
	return sicore.PreparedLocalResult{}, nil
}

func (storageIntegrityRuntimePorts) RegisterPreparedClaim(context.Context, string) (sicore.ClaimOutcome, error) {
	return sicore.ClaimOutcome{}, nil
}

func (storageIntegrityRuntimePorts) AbortPreparedStatement(context.Context, string, []sicore.CandidatePart, string) error {
	return nil
}

func (storageIntegrityRuntimePorts) LookupPreparedStatement(context.Context, string) (sicore.PreparedLocalResult, bool, error) {
	return sicore.PreparedLocalResult{}, false, nil
}

func (storageIntegrityRuntimePorts) QuerySubmitStatus(context.Context, string) (sicore.SubmitOutcome, error) {
	return sicore.SubmitOutcome{}, nil
}

func (storageIntegrityRuntimePorts) QueryClaimStatus(context.Context, string) (sicore.ClaimOutcome, error) {
	return sicore.ClaimOutcome{}, nil
}

func (storageIntegrityRuntimePorts) PutPayload(context.Context, []byte, string, uint64) (sicore.PayloadPutResult, error) {
	return sicore.PayloadPutResult{}, nil
}

// AssertTables reports every Active table healthy; HouseGate treats an Active
// table missing from the report as not yet asserted and refuses its admissions.
func (storageIntegrityRuntimePorts) AssertTables(_ context.Context, activeTableIDs []string) (sicore.MergeGuardReport, error) {
	report := sicore.MergeGuardReport{Tables: make(map[string]error, len(activeTableIDs))}
	for _, id := range activeTableIDs {
		report.Tables[id] = nil
	}
	return report, nil
}
```

- [ ] **Step 2: Run them and verify they fail.** `go vet ./standalone/`. Expected: build failure, `undefined: storageIntegrityTableStateRuntime`.
- [ ] **Step 3: Add the runtime.** Create `standalone/storage_integrity_table_state.go`:

```go
package standalone

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"compute-network-node/common"
	"compute-network-node/database_registry"
	"compute-network-node/rpc"
	"compute-network-node/storageintegrityadapter/tablestate"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sentioxyz/arbiter-core/dataplane"
)

// storageIntegrityRegistryFollower is the arbiter-core registry follower
// lifecycle this runtime owns: sentio-node runs the one follower of the
// process, and the SNode role reads it through snode.Deps.Registry.
type storageIntegrityRegistryFollower interface {
	tablestate.RegistryFollower
	dataplane.RegistryView
	Run(ctx context.Context) error
}

var _ storageIntegrityRegistryFollower = (*dataplane.RegistryFollower)(nil)

type storageIntegrityTableStateDeps struct {
	follower  storageIntegrityRegistryFollower
	readiness func(tableID string) bool
	chain     tablestate.DatabaseInfoReader
	genesis   []payloadexec.TableSchema
	networkID string
	logger    *slog.Logger

	// Declaration compensation; declarations is empty on a router-only node.
	env              *common.NodeEnv
	declarations     []database_registry.SchemaDeclarationConfig
	physicalDatabase string
}

// storageIntegrityTableStateRuntime owns the dynamic table set's host side:
// the registry follower, the table state injected into HouseGate, its
// metrics and the declaration compensation loop.
type storageIntegrityTableStateRuntime struct {
	readyTimeout time.Duration
	follower     storageIntegrityRegistryFollower
	state        *tablestate.State
	metrics      *tablestate.Metrics
	compensator  *database_registry.SchemaCompensator
}

func newStorageIntegrityTableStateRuntime(deps storageIntegrityTableStateDeps) (*storageIntegrityTableStateRuntime, error) {
	if deps.follower == nil || deps.readiness == nil || deps.chain == nil {
		return nil, errors.New("storage-integrity table state requires the registry follower, reconciler readiness and chain state")
	}
	metrics := tablestate.NewMetrics()
	state, err := tablestate.New(tablestate.Config{
		Registry:  tablestate.FromFollower(deps.follower),
		Readiness: tablestate.ReadinessFunc(deps.readiness),
		Chain:     tablestate.DatabaseInfoChain(deps.chain),
		Genesis:   deps.genesis,
		NetworkID: deps.networkID,
		Metrics:   metrics,
		Logger:    deps.logger,
	})
	if err != nil {
		return nil, err
	}
	runtime := &storageIntegrityTableStateRuntime{
		readyTimeout: dataplane.DefaultRegistryStartupTimeout,
		follower:     deps.follower,
		state:        state,
		metrics:      metrics,
	}
	if len(deps.declarations) > 0 {
		runtime.compensator, err = database_registry.NewSchemaCompensator(deps.env, deps.chain, deps.declarations[0], database_registry.CompensationConfig{
			PhysicalDatabase: deps.physicalDatabase,
			Observe:          metrics.ObserveCompensation,
		})
		if err != nil {
			return nil, fmt.Errorf("storage-integrity declaration compensation: %w", err)
		}
	}
	return runtime, nil
}

// tableState is the value for housegate.Options.StorageIntegrityTableState;
// a nil runtime yields a nil interface.
func (r *storageIntegrityTableStateRuntime) tableState() sitable.TableState {
	if r == nil {
		return nil
	}
	return r.state
}

// storageRPCOptions serve sentio_getStorageIntegrityTableStatus from the table
// state; without a runtime the method answers "ordinary".
func (r *storageIntegrityTableStateRuntime) storageRPCOptions() []rpc.StorageNodeOption {
	if r == nil {
		return nil
	}
	return []rpc.StorageNodeOption{rpc.WithStorageIntegrityTableStatus(r.state)}
}

// begin runs inside the startup transaction before housegate.New: it starts
// the registry follower, waits for its first answer, publishes the first
// snapshot (design §9.2: in sync before HouseGate runs) and schedules the
// refresh and compensation loops to start at commit.
func (r *storageIntegrityTableStateRuntime) begin(ctx context.Context, tx *startupTransaction) error {
	if err := tx.startImmediateTask("storage integrity registry follower", r.follower.Run); err != nil {
		return err
	}
	if err := dataplane.WaitReady(ctx, r.follower, r.readyTimeout); err != nil {
		return fmt.Errorf("wait for the arbiter table registry: %w", err)
	}
	if err := r.state.Refresh(ctx); err != nil {
		return fmt.Errorf("build the first storage-integrity table-state snapshot: %w", err)
	}
	if err := tx.addTask("storage integrity table state", r.state.Run); err != nil {
		return err
	}
	if r.compensator != nil {
		if err := tx.addTask("table schema declaration compensation", r.compensator.Run); err != nil {
			return err
		}
	}
	return nil
}

// registerMetrics attaches the metrics to the embedded HouseGate's registry
// (design §9.6), or to the default registry when HouseGate runs without its
// collector; the /metrics handler serves both.
func (r *storageIntegrityTableStateRuntime) registerMetrics(registry *prometheus.Registry) error {
	var registerer prometheus.Registerer = prometheus.DefaultRegisterer
	if registry != nil {
		registerer = registry
	}
	if err := r.metrics.Register(registerer); err != nil {
		return fmt.Errorf("register storage-integrity table-state metrics: %w", err)
	}
	return nil
}
```

Replace the whole of `standalone/storage_integrity_schemas.go` with:

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
	snode []payloadexec.TableSchema
	refs  []schemaregistry.TableRef
}

// loadStorageIntegritySchemaSets loads the genesis table set
// (storage_integrity.snode.table_ids) once, in its configured order. The
// embedded HouseGate reads every other table's schema from the arbiter
// registry through the injected table state.
func loadStorageIntegritySchemaSets(
	ctx context.Context,
	loader schemaregistry.Loader,
	genesisTableIDs []string,
) (storageIntegritySchemaSets, error) {
	refs := make([]schemaregistry.TableRef, 0, len(genesisTableIDs))
	for _, id := range genesisTableIDs {
		refs = append(refs, schemaregistry.TableRef{
			TableID:  id,
			Database: housegateConfig.StorageIntegrityUnsafeDatabase,
			Table:    housegateConfig.StorageIntegrityPhysicalTable(id),
		})
	}
	loaded, err := loader.Load(ctx, refs)
	if err != nil {
		return storageIntegritySchemaSets{}, err
	}
	byID := make(map[string]payloadexec.TableSchema, len(loaded))
	for _, schema := range loaded {
		byID[schema.TableID] = schema
	}
	schemas := make([]payloadexec.TableSchema, 0, len(genesisTableIDs))
	for _, id := range genesisTableIDs {
		schema, ok := byID[id]
		if !ok {
			return storageIntegritySchemaSets{}, fmt.Errorf("storage integrity genesis table %q has no loaded schema", id)
		}
		schemas = append(schemas, schema)
	}
	return storageIntegritySchemaSets{snode: schemas, refs: refs}, nil
}
```

- [ ] **Step 4: Wire it into `Run`.**

`standalone/standalone.go` edit 1 of 10, replace:

```go
		siListenerPermit      *storageIntegrityListenerPermit
		schemaRegistryConfigs []database_registry.SchemaDeclarationConfig
	)
```

with:

```go
		siListenerPermit      *storageIntegrityListenerPermit
		schemaRegistryConfigs []database_registry.SchemaDeclarationConfig
		siTableState          *storageIntegrityTableStateRuntime
	)
```

`standalone/standalone.go` edit 2 of 10, replace:

```go
			defer arbClient.Close()

			chConn := siCHConn
```

with:

```go
			defer arbClient.Close()
			siRegistry := dataplane.NewRegistryFollower(arbClient, newSlogLogger(logger))

			chConn := siCHConn
```

`standalone/standalone.go` edit 3 of 10, replace:

```go
					env.DatabasesContract,
					cfg.Housegate.StorageIntegrity.Tables,
				)
```

with:

```go
					env.DatabasesContract,
					si.SNode.TableIDs,
				)
```

`standalone/standalone.go` edit 4 of 10, replace:

```go
			schemaSets, err := loadStorageIntegritySchemaSets(
				ctx,
				schemaLoader,
				si.SNode.TableIDs,
				cfg.Housegate.StorageIntegrity.Tables,
			)
```

with:

```go
			schemaSets, err := loadStorageIntegritySchemaSets(
				ctx,
				schemaLoader,
				si.SNode.TableIDs,
			)
```

`standalone/standalone.go` edit 5 of 10, replace:

```go
			}, snode.Deps{
				Client:   arbClient,
				Conn:     chConn,
```

with:

```go
			}, snode.Deps{
				Client:   arbClient,
				Registry: siRegistry,
				Conn:     chConn,
```

`standalone/standalone.go` edit 6 of 10, replace:

```go
				MergeConn:            storageintegrityadapter.NewMergeConn(chConn),
				TableSchemas:         schemaSets.runtime,
			}
		}
		var siReadState rewriter.StorageIntegrityReadState // nil-safe: a nil *snode.Role must not become a non-nil interface
		if siRole != nil {
			siReadState = newOwnedStorageIntegrityReadState(
				siRole,
				si.SNode.TableIDs,
				cfg.Housegate.StorageIntegrity.Tables,
			)
		}
```

with:

```go
				MergeConn:            storageintegrityadapter.NewMergeConn(chConn),
			}
			siTableState, err = newStorageIntegrityTableStateRuntime(storageIntegrityTableStateDeps{
				follower:         siRegistry,
				readiness:        siRole.TableReady,
				chain:            netState,
				genesis:          tables,
				networkID:        si.SNode.NetworkID,
				logger:           roleLogger,
				env:              env,
				declarations:     schemaRegistryConfigs,
				physicalDatabase: cfg.Node.PhysicalDatabase,
			})
			if err != nil {
				return fmt.Errorf("construct storage-integrity table state: %w", err)
			}
		}
		var siReadState rewriter.StorageIntegrityReadState // nil-safe: a nil *snode.Role must not become a non-nil interface
		if siRole != nil {
			siReadState = siRole
		}
```

`standalone/standalone.go` edit 7 of 10, replace:

```go
			Config:                    &cfg.Housegate,
			NetworkState:              netState,
			Signer:                    relaySigner,
			UsageClient:               usageClient,
			IndexingUsageReporter:     indexingReporter,
			CommitGateObservers:       observers,
			RedisClients:              redisClients,
			GetIndexerId:              indexerIdGetter(env),
			StorageIntegrityRuntime:   siRuntime,
			StorageIntegrityReadState: siReadState,
		}
```

with:

```go
			Config:                     &cfg.Housegate,
			NetworkState:               netState,
			Signer:                     relaySigner,
			UsageClient:                usageClient,
			IndexingUsageReporter:      indexingReporter,
			CommitGateObservers:        observers,
			RedisClients:               redisClients,
			GetIndexerId:               indexerIdGetter(env),
			StorageIntegrityRuntime:    siRuntime,
			StorageIntegrityReadState:  siReadState,
			StorageIntegrityTableState: siTableState.tableState(),
		}
```

`standalone/standalone.go` edit 8 of 10, replace:

```go
		storageRPCServer, err := rpc.NewStorageNodeRPCServer(s, env)
```

with:

```go
		storageRPCServer, err := rpc.NewStorageNodeRPCServer(s, env, siTableState.storageRPCOptions()...)
```

`standalone/standalone.go` edit 9 of 10, replace:

```go
			hg, err = housegate.New(*housegateOptions)
```

with:

```go
			if siTableState != nil {
				if err := siTableState.begin(runCtx, tx); err != nil {
					return err
				}
			}
			hg, err = housegate.New(*housegateOptions)
```

`standalone/standalone.go` edit 10 of 10, replace:

```go
			metricsHandler = metricshttp.Handler(hg.MetricsRegistry(), cfg.Housegate.Observability.Pprof)
		}
```

with:

```go
			metricsHandler = metricshttp.Handler(hg.MetricsRegistry(), cfg.Housegate.Observability.Pprof)
			if siTableState != nil {
				if err := siTableState.registerMetrics(hg.MetricsRegistry()); err != nil {
					return err
				}
			}
		}
```

`standalone/startup_transaction.go`, replace:

```go
// startImmediateTask is reserved for a dependency whose cleanup exists only
// inside its blocking Run lifecycle. HouseGate's native multi-listener Run is
// the one production use: it must begin immediately after construction so a
// later startup failure still executes its teardown stack.
```

with:

```go
// startImmediateTask is reserved for a dependency that must run before commit.
// HouseGate's native multi-listener Run must begin immediately after
// construction so a later startup failure still executes its teardown stack;
// the storage-integrity registry follower must deliver its first answer before
// HouseGate is constructed. Those are the two production uses.
```

`standalone/storage_integrity_bootstrap_test.go`, replace:

```go
	require.True(t, selectorPathFromObject(snapshotCalls[0].Args[3], cfgBinding.Obj, "Housegate", "StorageIntegrity", "Tables"),
		"the snapshot must read exactly the configured HouseGate SI table surface")
```

with:

```go
	require.True(t, selectorPathFromObject(snapshotCalls[0].Args[3], siBinding.Obj, "SNode", "TableIDs"),
		"the snapshot must read exactly the genesis table set (storage_integrity.snode.table_ids)")
```

Delete the owned read-state wrapper: `git rm standalone/storage_integrity_read_state.go standalone/storage_integrity_read_state_test.go`.

- [ ] **Step 5: Run everything.** `go vet ./... && go test -race ./...`, then `bazel run //:gazelle && bazel test //...`. Expected: all `ok` / `PASSED`, including every `storage_integrity_bootstrap_test.go` audit and `TestRunBuildsTheTableStateBeforeHousegate`.
- [ ] **Step 6: Commit.**

```bash
git add standalone
git commit -F - <<'EOF'
feat(standalone): inject the dynamic table state into HouseGate

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 14: Acceptance: a dynamic table lifecycle driven by a fake registry

**Files:**

- Create: `standalone/storage_integrity_dynamic_ch_test.go`
- Modify: `standalone/storage_integrity_acceptance_ch_test.go:34-35, 331-339, 506-508, 549-550, 563-567`, `.github/workflows/ci.yml:132, 140, 156-157`

**Interfaces:**

- Consumes: arbiter-proto `pb.TableRegistryServer`, `pb.RegisterTableRegistryServer`, `pb.TableRegistrySnapshot`/`TableIncarnation`; plan A `dataplane.NewRegistryFollower`; arbiter-core `ddl.EnsureProtocolTables`, `ddl.CHTableName`; housegate `network.NewRpcNetworkState`; the acceptance helpers (`startStorageIntegrityAcceptanceProxy`, `dialStorageIntegritySession`, `throttlingSNodeRole`).
- Produces: `storageIntegrityAcceptanceProxyConfig.tableState sitable.TableState` (set: the switch is on and the static list empty); the fake rewriter classifies `SELECT ... FROM <db>.<table>` and `CREATE TABLE <db>.<table>` with the accessed table; `TestStorageIntegrityDynamicTableLifecycle` (B15), gated by `SENTIO_SI_CH_E2E=1` and `CH_ADDR` like its siblings.

- [ ] **Step 1: Write the failing test.** Create `standalone/storage_integrity_dynamic_ch_test.go`:

```go
package standalone

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"compute-network-node/config"
	"compute-network-node/rpc"
	"compute-network-node/storageintegrityadapter"
	statecore "sentioxyz/sentio-core/network/state"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// fakeTableRegistry is an arbiter TableRegistry gRPC server whose committed
// registry the test replaces step by step. Every publish bumps the version,
// as the arbiter does on each registry-changing command.
type fakeTableRegistry struct {
	pb.UnimplementedTableRegistryServer
	mu       sync.Mutex
	snapshot *pb.TableRegistrySnapshot
	changed  chan struct{}
}

func newFakeTableRegistry(snapshot *pb.TableRegistrySnapshot) *fakeTableRegistry {
	return &fakeTableRegistry{snapshot: snapshot, changed: make(chan struct{})}
}

func (f *fakeTableRegistry) current() (*pb.TableRegistrySnapshot, <-chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return proto.Clone(f.snapshot).(*pb.TableRegistrySnapshot), f.changed
}

func (f *fakeTableRegistry) publish(incarnations ...*pb.TableIncarnation) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshot.Version++
	f.snapshot.Incarnations = incarnations
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *fakeTableRegistry) GetTableRegistry(context.Context, *emptypb.Empty) (*pb.TableRegistrySnapshot, error) {
	snapshot, _ := f.current()
	return snapshot, nil
}

func (f *fakeTableRegistry) WatchTableRegistry(req *pb.WatchTableRegistryRequest, stream grpc.ServerStreamingServer[pb.TableRegistrySnapshot]) error {
	sent := req.GetSinceVersion()
	for {
		snapshot, changed := f.current()
		if snapshot.GetVersion() > sent {
			if err := stream.Send(snapshot); err != nil {
				return err
			}
			sent = snapshot.GetVersion()
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-changed:
		}
	}
}

func startFakeTableRegistry(t *testing.T, registry *fakeTableRegistry) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pb.RegisterTableRegistryServer(server, registry)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// clickHouseProtocolReadiness stands in for the embedded reconciler's Ready:
// a table is ready once its three hg_* tables exist.
func clickHouseProtocolReadiness(conn clickhouse.Conn) func(string) bool {
	return func(tableID string) bool {
		physical := ddl.CHTableName(tableID)
		var n uint64
		err := conn.QueryRow(context.Background(),
			"SELECT count() FROM system.tables WHERE name = ? AND database IN (?, ?, ?)",
			physical, config.StorageIntegrityUnsafeDatabase, config.StorageIntegritySafeDatabase, config.StorageIntegrityPromoteDatabase,
		).Scan(&n)
		return err == nil && n == 3
	}
}

// TestStorageIntegrityDynamicTableLifecycle drives sentio-node's table state
// from a fake arbiter registry over gRPC, through the embedded HouseGate and
// the agent's JSON-RPC status client, against a Keeper-enabled ClickHouse:
// unrecorded -> Pending; Active before the hg_* tables exist -> still Pending;
// hg_* created -> Active and the signed lane engages; Retiring -> Gone;
// Purged -> Pending again (design 2026-09-25 §9.2, §9.3, §10).
//
// Requires SENTIO_SI_CH_E2E=1 and CH_ADDR.
func TestStorageIntegrityDynamicTableLifecycle(t *testing.T) {
	addr := requireStorageIntegrityCHAcceptance(t)
	conn := openStorageIntegrityAcceptanceConn(t, addr)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	require.NoError(t, conn.Ping(ctx))

	const (
		networkID = "sentio-node-si-dynamic"
		database  = "default"
		indexer   = uint64(7)
	)
	tableName := fmt.Sprintf("si_dyn_%d", time.Now().UnixNano())
	logicalID := database + "." + tableName
	schema := payloadexec.TableSchema{TableID: logicalID, PartitionBy: "partition", Columns: []lthash.Column{
		{Name: "partition", Type: "String"},
		{Name: "value", Type: "UInt64"},
	}}
	schemaJSON, err := json.Marshal(schema)
	require.NoError(t, err)
	schemaHash := payloadexec.TableSchemaHash(networkID, schema)

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.%s (partition String, value UInt64) ENGINE = MergeTree ORDER BY partition",
		quoteSmokeIdentifier(database), quoteSmokeIdentifier(tableName))))
	physical := ddl.CHTableName(logicalID)
	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancelCleanup()
		for _, db := range []string{database, config.StorageIntegrityPromoteDatabase, config.StorageIntegritySafeDatabase, config.StorageIntegrityUnsafeDatabase} {
			table := physical
			if db == database {
				table = tableName
			}
			if err := conn.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s SYNC",
				quoteSmokeIdentifier(db), quoteSmokeIdentifier(table))); err != nil {
				t.Errorf("drop %s.%s: %v", db, table, err)
			}
		}
	})

	// The arbiter registry: enabled and seeded, nothing recorded yet.
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
		chain: fixedDatabaseInfos{database: {DatabaseId: database, IndexerId: indexer, Tables: []statecore.TableInfo{
			{TableId: tableName, TableType: "user", CreatedBlock: 150},
		}}},
		networkID: networkID,
	})
	require.NoError(t, err)
	tx := newStartupTransaction(ctx)
	t.Cleanup(tx.stopAndWait)
	require.NoError(t, tableState.begin(tx.Context(), tx))
	require.NoError(t, tx.commit())

	rpcServer, err := rpc.NewStorageNodeRPCServer(nil, nil, tableState.storageRPCOptions()...)
	require.NoError(t, err)
	rpcHTTP := httptest.NewServer(rpcServer.Handler)
	t.Cleanup(rpcHTTP.Close)
	agentStatuses, err := network.NewRpcNetworkState(rpcHTTP.URL, network.RpcOptions{})
	require.NoError(t, err)
	statusOf := func() registry.TableStatus {
		status, err := agentStatuses.StorageIntegrityTableStatus(ctx, database, tableName)
		require.NoError(t, err)
		return status
	}
	waitForStatus := func(want string) registry.TableStatus {
		var got registry.TableStatus
		require.Eventually(t, func() bool { got = statusOf(); return got.Status == want }, 30*time.Second, 50*time.Millisecond,
			"table status never became %q", want)
		return got
	}

	signerKeyHex := "2222222222222222222222222222222222222222222222222222222222222222"
	signer, err := auth.NewRelaySigner(signerKeyHex)
	require.NoError(t, err)
	netState := network.NewInMemoryNetworkState()
	netState.DatabaseInfos[network.Database("system")] = network.DatabaseInfo{IndexerId: 0}
	netState.DatabaseInfos[network.Database(database)] = network.DatabaseInfo{IndexerId: 0}
	netState.DatabasePermissions[network.AccountAddress(strings.ToLower(signer.Address()))] =
		network.DatabasePermissions{network.Database(database): registry.DbAuthWrite | registry.DbAuthRead}
	role := &throttlingSNodeRole{table: sicore.PhysicalTableName(logicalID)}
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	require.NoError(t, err)
	spool, err := sicore.NewFilePayloadSpool(t.TempDir())
	require.NoError(t, err)
	proxyAddr := startStorageIntegrityAcceptanceProxy(t, addr, storageIntegrityAcceptanceProxyConfig{
		networkID:     networkID,
		database:      database,
		table:         tableName,
		logicalID:     logicalID,
		signerAddress: signer.Address(),
		netState:      netState,
		tableState:    tableState.tableState(),
		runtime: housegate.StorageIntegrityRuntimeOptions{
			SourcePreparer:     storageintegrityadapter.NewSourcePreparer(role),
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
	selectSQL := fmt.Sprintf("SELECT count() FROM %s", logicalID)
	insert := func() error {
		return session.stagedInsert(stagedInsertWireRequest{
			Database:      database,
			User:          "default",
			SQL:           fmt.Sprintf("INSERT INTO %s (partition, value) FORMAT Native", logicalID),
			StatementID:   fmt.Sprintf("%s:1:si-dynamic-%d", strings.ToLower(signer.Address()), time.Now().UnixNano()),
			SignerKeyHex:  signerKeyHex,
			NetworkID:     networkID,
			SchemaHash:    schemaHash,
			TargetTableID: logicalID,
			Partition:     "p_eu",
			Value:         7,
		})
	}

	// 1. Unrecorded on the SI indexer: default deny.
	require.Equal(t, "pending", statusOf().Status)
	_, err = session.query(selectSQL)
	require.ErrorContains(t, err, "is pending activation (retryable)")
	require.ErrorContains(t, insert(), "is pending activation (retryable)")

	// 2. Active in the registry, but the hg_* tables do not exist yet (E6).
	activeInc := &pb.TableIncarnation{
		Seq: 1, DatabaseId: database, TableId: tableName,
		Origin:     pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_CHAIN,
		Status:     pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_ACTIVE,
		SchemaHash: schemaHash, SchemaJson: string(schemaJSON),
	}
	fakeRegistry.publish(activeInc)
	require.Eventually(t, func() bool { return statusOf().RegistryVersion == 2 }, 30*time.Second, 50*time.Millisecond)
	require.Equal(t, "pending", statusOf().Status, "Active is reported only once the local hg_* tables are ready")

	// 3. The reconciler's work: create and verify the three hg_* tables.
	require.NoError(t, ddl.EnsureProtocolTables(ctx, conn, ddl.Pinned{
		UnsafeDB: config.StorageIntegrityUnsafeDatabase, SafeDB: config.StorageIntegritySafeDatabase,
		PromoteDB: config.StorageIntegrityPromoteDatabase, NodeID: "si-dynamic-node", KeeperShardID: 0,
	}, []payloadexec.TableSchema{schema}, ddl.ModeCreateAndVerify, slog.Default()))
	active := waitForStatus("active")
	require.Equal(t, schemaHash, active.SchemaHash)
	var served payloadexec.TableSchema
	require.NoError(t, json.Unmarshal([]byte(active.SchemaJSON), &served))
	require.Equal(t, schemaHash, payloadexec.TableSchemaHash(networkID, served), "the agent's hash check must pass")
	_, err = session.query(selectSQL)
	require.NoError(t, err, "an Active table is readable")
	insertErr := insert()
	require.ErrorContains(t, insertErr, "252", "the signed INSERT reached the source prepare (the throttled SNode refuses it)")
	require.NotContains(t, insertErr.Error(), "pending activation")

	// 4. Retired: Gone reads as an unknown table, and a same-name CREATE waits for the purge.
	retiring := proto.Clone(activeInc).(*pb.TableIncarnation)
	retiring.Status = pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_RETIRING
	fakeRegistry.publish(retiring)
	gone := waitForStatus("gone")
	require.Equal(t, schemaHash, gone.SchemaHash, "a Gone table keeps its schema")
	_, err = session.query(selectSQL)
	require.ErrorContains(t, err, fmt.Sprintf("Table %s does not exist", logicalID))
	_, err = session.query(fmt.Sprintf("CREATE TABLE %s (partition String, value UInt64) ENGINE = MergeTree ORDER BY partition", logicalID))
	require.ErrorContains(t, err, "is still being purged; retry CREATE later (retryable)")

	// 5. Purged: the name is unrecorded again, so it is Pending by default deny.
	purged := proto.Clone(activeInc).(*pb.TableIncarnation)
	purged.Status = pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PURGED
	fakeRegistry.publish(purged)
	waitForStatus("pending")
	_, err = session.query(selectSQL)
	require.ErrorContains(t, err, "is pending activation (retryable)")
}
```

- [ ] **Step 2: Run it and verify it fails.** `go vet ./standalone/`. Expected: build failure, `unknown field tableState in struct literal of type storageIntegrityAcceptanceProxyConfig`.
- [ ] **Step 3: Extend the acceptance harness and CI.**

`standalone/storage_integrity_acceptance_ch_test.go` edit 1 of 5, replace:

```go
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sqlmeta"
```

with:

```go
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
```

`standalone/storage_integrity_acceptance_ch_test.go` edit 2 of 5, replace:

```go
	case strings.HasPrefix(upper, "SELECT"):
		// Classification is not optional even for a table-less read: with auth
		// on, HouseGate's PermissionCommitGateObserver refuses to forward a
		// statement the rewriter left Unspecified.
		out.StatementType = sqlmeta.StatementTypeSelect
	}
	return out, nil
}
```

with:

```go
	case strings.HasPrefix(upper, "CREATE TABLE "+strings.ToUpper(f.database+"."+f.table)):
		out.StatementType = sqlmeta.StatementTypeCreateTable
		out.AccessedTables = []sqlmeta.AccessedTable{f.target()}
	case strings.HasPrefix(upper, "SELECT"):
		// Classification is not optional even for a table-less read: with auth
		// on, HouseGate's PermissionCommitGateObserver refuses to forward a
		// statement the rewriter left Unspecified.
		out.StatementType = sqlmeta.StatementTypeSelect
		if strings.Contains(upper, " FROM "+strings.ToUpper(f.database+"."+f.table)) {
			out.AccessedTables = []sqlmeta.AccessedTable{f.target()}
		}
	}
	return out, nil
}

// target is the accessed-table record the fake reports for its one table.
func (f *storageIntegrityAcceptanceRewriter) target() sqlmeta.AccessedTable {
	return sqlmeta.AccessedTable{
		OriginalDatabase: f.database,
		OriginalTable:    f.table,
		LogicalDatabase:  f.database,
		PhysicalDatabase: f.database,
	}
}
```

`standalone/storage_integrity_acceptance_ch_test.go` edit 3 of 5, replace:

```go
	netState      *network.InMemoryNetworkState
	runtime       housegate.StorageIntegrityRuntimeOptions
}
```

with:

```go
	netState      *network.InMemoryNetworkState
	runtime       housegate.StorageIntegrityRuntimeOptions
	// tableState, when set, is injected the way standalone.Run injects the
	// dynamic table state: the switch is on and the static list is empty.
	tableState sitable.TableState
}
```

`standalone/storage_integrity_acceptance_ch_test.go` edit 4 of 5, replace:

```go
	hgCfg.StorageIntegrity.Tables = []string{cfg.logicalID}
	hgCfg.StorageIntegrity.Ingress.Enabled = true
```

with:

```go
	if cfg.tableState != nil {
		enabled := true
		hgCfg.StorageIntegrity.Enabled = &enabled
	} else {
		hgCfg.StorageIntegrity.Tables = []string{cfg.logicalID}
	}
	hgCfg.StorageIntegrity.Ingress.Enabled = true
```

`standalone/storage_integrity_acceptance_ch_test.go` edit 5 of 5, replace:

```go
		Config:                  &hgCfg,
		NetworkState:            cfg.netState,
		Rewriter:                &storageIntegrityAcceptanceRewriter{database: cfg.database, table: cfg.table},
		StorageIntegrityRuntime: cfg.runtime,
	})
```

with:

```go
		Config:                     &hgCfg,
		NetworkState:               cfg.netState,
		Rewriter:                   &storageIntegrityAcceptanceRewriter{database: cfg.database, table: cfg.table},
		StorageIntegrityRuntime:    cfg.runtime,
		StorageIntegrityTableState: cfg.tableState,
	})
```

`.github/workflows/ci.yml` edit 1 of 3, replace:

```yaml
            --test_filter='TestStorageIntegrityMalformedColumnTypeCreatesNoTable|TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession' \
```

with:

```yaml
            --test_filter='TestStorageIntegrityMalformedColumnTypeCreatesNoTable|TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession|TestStorageIntegrityDynamicTableLifecycle' \
```

`.github/workflows/ci.yml` edit 2 of 3, replace:

```yaml
          if [ "${passes}" -ne 2 ]; then
            echo "expected 2 storage-integrity acceptance PASS markers, saw ${passes}"
```

with:

```yaml
          if [ "${passes}" -ne 3 ]; then
            echo "expected 3 storage-integrity acceptance PASS markers, saw ${passes}"
```

`.github/workflows/ci.yml` edit 3 of 3, replace:

```yaml
      - name: storage-integrity acceptance (Spec L D1/D6)
```

with:

```yaml
      - name: storage-integrity acceptance (Spec L D1/D6, dynamic table set)
```

- [ ] **Step 4: Run it against ClickHouse.** Start a Keeper-enabled ClickHouse as CI does:

```bash
docker run -d --rm --name sentio-node-si-ch -p 127.0.0.1::9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 -v "$PWD/scripts/ci/clickhouse-keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" clickhouse/clickhouse-server:25.8.32.4
export CH_ADDR=127.0.0.1:$(docker inspect --format '{{(index (index .NetworkSettings.Ports "9000/tcp") 0).HostPort}}' sentio-node-si-ch)
SENTIO_SI_CH_E2E=1 go test ./standalone/ -count=1 -v -run 'TestStorageIntegrity(MalformedColumnTypeCreatesNoTable|BackpressureKeepsTheEmbeddedProxySession|DynamicTableLifecycle|ProtocolTableDriftFailsBootstrap)'
bazel test //standalone:standalone_test --test_filter='TestStorageIntegrityMalformedColumnTypeCreatesNoTable|TestStorageIntegrityBackpressureKeepsTheEmbeddedProxySession|TestStorageIntegrityDynamicTableLifecycle' --test_env=SENTIO_SI_CH_E2E=1 --test_env=CH_ADDR --nocache_test_results --test_output=errors
docker rm -f sentio-node-si-ch
```

Expected: four `--- PASS` lines, `TestStorageIntegrityDynamicTableLifecycle` among them (about 1s), and the Bazel run `PASSED`. `go vet ./standalone/ && go test ./standalone/` without the env passes too (the new test skips).
- [ ] **Step 5: Commit.**

```bash
git add standalone/storage_integrity_dynamic_ch_test.go standalone/storage_integrity_acceptance_ch_test.go standalone/BUILD.bazel .github/workflows/ci.yml
git commit -F - <<'EOF'
test(storage-integrity): drive a dynamic table lifecycle from a fake registry

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 15: Document the dynamic table set

**Files:** Modify `README.md:41-43` (sample HouseGate block), `:87` (schema-source paragraph), `:95-112` (declaration and onboarding). **Interfaces:** none.

- [ ] **Step 1: Edit the README.**

`README.md` edit 1 of 3, replace:

```markdown
  storage_integrity:
    tables: ["orders.t"]           # logical ids; hg_unsafe/hg_safe.orders__t derived
    read:
```

with:

```markdown
  storage_integrity:
    # No `tables` list: sentio-node sets `enabled: true` and injects the table
    # set from the arbiter registry (genesis set = storage_integrity.snode.table_ids).
    read:
```

`README.md` edit 2 of 3, replace:

```markdown
`storage_integrity.snode.table_ids` selects the frozen schema set. `schema_source` defaults to `clickhouse` (the Phase-A path). Despite its legacy name, `network_state` does not read the Redis statemirror for SI bootstrap: it captures one finalized Ethereum header, reads only the configured HouseGate SI table declarations from the `Databases` contract
```

with:

```markdown
`storage_integrity.snode.table_ids` is the genesis table set: the tables served while the arbiter's table registry is disabled, and the only ones whose schemas startup loads. A legacy `housegate.storage_integrity.tables` equal to it is ignored with a deprecation warning; any other list is a config error. `schema_source` defaults to `clickhouse` (the Phase-A path). Despite its legacy name, `network_state` does not read the Redis statemirror for SI bootstrap: it captures one finalized Ethereum header, reads only the genesis table declarations from the `Databases` contract
```

`README.md` edit 3 of 3, replace:

```markdown
Every successful end-user `CREATE TABLE` through HouseGate triggers an
idempotent schema-declaration attempt against the exact ClickHouse replica and
physical table that executed the DDL. This path runs whether or not storage
integrity is enabled and whether or not the table appears in
`storage_integrity.snode.table_ids`; that list only selects the frozen
storage-integrity consensus set. The post-success hook is asynchronous and
best-effort: transport, process, or chain failures are logged and do not change
the already-successful ClickHouse result.

To onboard a table into the frozen storage-integrity set:

1. Create the table through HouseGate. After ClickHouse materializes it, the commitgate success hook reads the schema back and declares it on-chain.
2. Add its logical `<database>.<table>` id to BOTH `storage_integrity.snode.table_ids` and `housegate.storage_integrity.tables` (one list drives the merge guard, the ingress and the read rewrite). With `schema_source: network_state`, the role creates its pinned `hg_unsafe.<table>` and `hg_safe.<table>` protocol tables; with `clickhouse`, both must already exist with the pinned DDL because deriving a schema from the table it would create is circular.
3. Run the schema-root print flow with the declared schema set.
4. Governance-update the genesis `schema_root`.
5. Rolling-restart every role. Each role recomputes the root and refuses to
   start if its derived schema does not match the anchor.
```

with:

```markdown
Every successful end-user `CREATE TABLE` through HouseGate triggers a schema declaration against the exact ClickHouse replica and physical table that executed the DDL, with every supported column type in its canonical spelling. This path runs whether or not storage integrity is enabled. It always declares, even when the latest on-chain hash is identical: contract schema versions survive DROP and re-CREATE, and the arbiter's registry reads only the first declaration after the table's `TableCreated`. The post-success hook is asynchronous and best-effort; on a storage-integrity indexer node a compensation loop (every 5 minutes) re-declares any table of this indexer's databases that was created after the upgrade and still has no declaration 10 minutes after it was first seen, trying each local replica in turn.

### Dynamic storage-integrity table set

On a storage-integrity node, sentio-node runs one arbiter table-registry follower, hands it to the embedded SNode (whose reconciler creates and drops the `hg_*` tables) and builds HouseGate's table state from it (design `housegate/docs/superpowers/specs/2026-09-25-dynamic-si-table-set-data-plane-design.md` §9). Startup waits for the registry's first answer and publishes the first snapshot before HouseGate is constructed. A table is reported:

- **Ordinary** while the registry is disabled (only the genesis set is Active), for databases on other indexers or unknown to the chain state, for Legacy incarnations, and, before the seed commits, for tables whose `TableCreated` block precedes `activation_block` (zero, the value of tables created before the upgrade, included);
- **Pending** for a table the registry does not record (default deny, including a Purged name), for a Pending incarnation, and for an Active incarnation until this node's reconciler reports its three `hg_*` tables ready or when its registry schema fails the `schema_hash` self-check;
- **Refused** with the arbiter's `refused_code` and `refused_reason`; **Active** otherwise; **Gone** while the incarnation is Retiring or Purging.

The storage-node JSON-RPC serves the same answer as `sentio_getStorageIntegrityTableStatus(database, table)`, the method the HouseGate agent sidecar calls before signing an INSERT; a node without storage integrity answers `ordinary`. The embedded HouseGate's `/metrics` gains `sentio_node_storage_integrity_tables{status}`, `sentio_node_storage_integrity_table_pending_seconds{table}`, `sentio_node_storage_integrity_schema_compensations_total{outcome}`, `sentio_node_storage_integrity_registry_follower_connected`, `sentio_node_storage_integrity_registry_enabled` and `sentio_node_storage_integrity_registry_version`.

To onboard a table after the registry is enabled, create it through HouseGate: the declaration reaches the chain, the arbiter records the incarnation, every data-plane node creates its `hg_*` tables, and the table turns Active with no config edit or restart. Until then reads and writes get the retryable `pending activation` error. To add a table to the genesis set of a network whose registry is not yet enabled, add its id to `storage_integrity.snode.table_ids` only, run the schema-root print flow, governance-update the genesis `schema_root`, and rolling-restart every role.
```

- [ ] **Step 2: Check.** `grep -n "housegate.storage_integrity.tables" README.md` shows only the deprecation sentence; every new paragraph is one line.
- [ ] **Step 3: Commit.**

```bash
git add README.md
git commit -F - <<'EOF'
docs(readme): describe the registry-driven storage-integrity table set

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

## Task 16: Verify, open the PR and merge (controller-only, gated)

**Files:** none.

- [ ] **Step 1: Full verification on the branch.** `go vet ./... && go test -race ./...`, `bazel run //:gazelle` (no diff), `bazel build //... && bazel test //...`, and Task 14 Step 4's docker run. Record the results in the PR.
- [ ] **Step 2: Baseline rule.** Any failing target is compared with a clean `origin/main` build by checking out only the changed paths (`git checkout origin/main -- <paths>`), never `git stash`; a matching failure set is not a regression.
- [ ] **Step 3: PR.** Push `feat/si-dynamic-table-state`, open the PR against `main` with the version table from Task 4, the rulings B1–B16, the verification results and the handoff below. End the description with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
- [ ] **Step 4: Merge** after review and a green CI, including the `integration-clickhouse` job's three acceptance PASS markers.

---

## Spec coverage

| Spec item | Where |
|---|---|
| §9.1 `TableInfo.CreatedBlock`, set on create and recreate, mirrored, zero = pre-upgrade | Task 2 (field, mirror, stores), Task 5 (`onTableCreated` sets and overwrites), Task 6 (read from the mirror) |
| §9.2 inputs: follower, reconciler `Ready`, chain state, indexer id, genesis set, network id | Task 7 (`Config`), Task 8 (follower adapter), Task 13 (`siRole.TableReady`, Redis mirror, `schemaSets.snode`, `si.SNode.NetworkID`; the indexer id gates compensation, Task 12) |
| §9.2 rebuild on any change, version only on content change, first snapshot before HouseGate | Task 7 (`Run`, `publish`, `TestVersionMovesOnlyOnContentChange`), Task 13 (`begin` before `housegate.New`) |
| §9.2 `Lookup` order, activation window, Active + Ready, schema-hash self-check | Task 7 (`evaluate`, `resolveSchema`, `TestLookupMatrix`), B2, B4 |
| §9.3 JSON-RPC shape; `ordinary` without SI | Task 9 (wire test against housegate's client), Task 7 (`StorageIntegrityTableStatus`), Task 13 (wired), Task 14 (end to end) |
| §9.4 config migration, legacy-list rule, subset rule removed, read state from the role, MergeGuard fakes, bootstrap source-shape test | Task 10, Task 13 (read state, `TableSchemas`, bootstrap assertion), Task 4 (fakes) |
| §9.5 compensation loop, 5 min / 10 min, canonical types | Task 12, Task 11, Task 5 (the zero pointer it reads) |
| §9.6 metrics on `MetricsRegistry()` | Task 7 (`Metrics`), Task 13 (`registerMetrics`), B14 |
| §10 sentio-core row | Task 2 |
| §10 sentio-node row: `Lookup` matrix, version bumps, JSON-RPC, config migration, compensation, acceptance lifecycle | Tasks 7, 9, 10, 12, 14 |
| §11 step 2: housegate release, sentio-core tag, sentio-node bumps | Tasks 1, 3 (sentio-core pinned by commit, B16), 4 |
| E5, E6 | Task 7 (window, readiness), Handoff (activation_block rule) |
| §12 handoff | "Handoff to sub-project 5" below |
| Plan A handoff item 4 (`ErrTableNotReady` pre-write) | Task 4 |

## Verification record

How the code in this plan was verified before it was written down (2026-09-26, macOS arm64, Go 1.27.1, Docker via OrbStack, ClickHouse 25.8.32.4 with Keeper from `scripts/ci/clickhouse-keeper.xml`):

- **Task by task.** Fresh worktrees of sentio-core `origin/main` (`ba452f6`) and sentio-node `origin/main` (`1494fb1`). A script applied exactly the edit blocks and files above, ran each "verify it fails" step (every one failed with the text named in its Expected line), then each run step (every one passed), checked `gofmt` and committed (Tasks 2 and 4–14); every resulting Go file is byte-identical to the prototype's. The docker-bound runs in Tasks 4 and 14 were part of the script: all four acceptance tests passed, `TestStorageIntegrityDynamicTableLifecycle` in about 1.1s. Task 15's README blocks were applied to the replayed tree to confirm each replaced block exists exactly once.
- **Pins in the prototype.** `$HOUSEGATE_TAG` was emulated by a `go.mod` `replace` to housegate `04fb8c0`; `$SENTIO_CORE_VERSION` by a replace to the Task 2 commit; `$ARBITER_PROTO_PSEUDO` by a replace to arbiter-proto `a3dbb1f` (it carries the `TableRegistry` service); `$ARBITER_CORE_TAG` by a replace to arbiter-core `d059aab` plus local stubs of plan A's handoff names (`dataplane.RegistryFollower` with a real Get-then-Watch loop over `TableRegistry`, `WaitReady`, `RegistryView`, `DefaultRegistryStartupTimeout`, `wire.TableRegistrySnapshot` with its decoder, `snode.Deps.Registry`, `Role.TableReady` returning true, `snode.ErrTableNotReady`). The lifecycle test therefore ran the stub follower, not plan A's; its readiness came from the test's ClickHouse check, not plan A's reconciler.
- **Not run locally.** Bazel: `go_deps` cannot resolve directory `replace`s, so gazelle, `bazel mod tidy` and `bazel test` run for the first time at execution against the real pins (Tasks 4–16). `standalone.Run` end to end (`TestStorageIntegritySmoke`, which needs a live chain and arbiter). Plan A's real follower and reconciler behaviour.
- **Measured facts behind the rulings.** B2: arbiter `fsm/table_registry.go` `enableTableRegistryLocked` sets only `SchemaHash` on genesis incarnations. B8: `Databases.sol` keeps `_tableSchemaVersions` across `deleteTable` ("remain intact across table deletion and recreation"), and the old declarer skipped an identical latest hash. B9: `isDatabaseWriter` accepts the indexer signer; `DatabaseGC` drops `` <physical>.`<db>.<table>` ``. B3: `RedisNetworkState.All` skips undecodable entries. Task 4: with the new housegate only the two `AssertStopMerges` fakes fail to compile, and `MergeGuardReport` must name every Active table or HouseGate treats it as unasserted (with the static table set, an empty report made the embedded proxy exit at startup and the back-pressure acceptance time out). `CanonicalColumnType` maps `DateTime64(3,'UTC')` to `DateTime64(3, 'UTC')` and `FixedString(032)` to `FixedString(32)`, and rejects `Decimal(18, 4)`.

## Handoff to sub-project 5 (devnet2 rollout)

Base: design §12. What this plan leaves for sub-project 5:

- **Chart (shared, so through a PR).** Drop the chart's `tables == tableIDs` guard and stop rendering `housegate.storage_integrity.tables` (a list equal to `storage_integrity.snode.table_ids` is tolerated with a deprecation warning; any other list now fails config validation). Render `housegate.storage_integrity.enabled: true` for storage-integrity nodes (sentio-node forces it anyway; rendering it keeps the chart honest). `storage_integrity.snode.table_ids` stays and now means the genesis set.
- **Gate R4.** The new housegate refuses to start against a V1 rewriter. Deploy the in-pod rewriter `housegate-rewriter:0.15.0@sha256:57812c8c…` (rewriter-grpc with contract V2) together with, or before, the sentio-node upgrade; a node whose rewriter is V1 crash-loops at HouseGate construction, which is loud and safe.
- **Order.**
  1. Arbiter voters on sub-project 2c plus plan A's arbiter (`SubmitTablePurged`, P12).
  2. Verifiers on plan A's arbiter-core.
  3. SNodes and sentio-node on this plan (together with Gate R4). With the registry still disabled, every node serves exactly the genesis set, as before.
  4. Confirm every storage-integrity indexer node runs the sentio-core with `CreatedBlock`: its mirrored `TableInfo` for a table created after the upgrade has a non-zero `createdBlock` (for example `sentio_getDatabaseInfoById` on a fresh test table).
  5. Choose `activation_block` after the block by which step 4 holds on every such node (design E5; the rule in spec §12 and the risk in §13): a table created earlier but recorded with `CreatedBlock = 0` is treated as pre-upgrade and reported Ordinary before the seed, which is correct only if the seed will record it Legacy.
  6. Enable the registry through governance. The followers answer, the table state switches from the genesis set to the registry (genesis tables stay Active once each node's reconciler reports them ready; B6), and tables created after `activation_block` are Pending until recorded.
  7. Enable the auditors everywhere and confirm there are no mismatches.
  8. Enable the watcher.
- **Agent.** The sidecar's `network_state.source` points at its co-located indexer's storage-node RPC, which now serves `sentio_getStorageIntegrityTableStatus`.
- **Observe during rollout.** `sentio_node_storage_integrity_registry_follower_connected` (1 on every node), `sentio_node_storage_integrity_registry_enabled` and `_registry_version` (equal across nodes), `sentio_node_storage_integrity_tables{status}`, `sentio_node_storage_integrity_table_pending_seconds` (a table Pending well beyond the confirmation latency, about 13 minutes on devnet2 with `confirmation = safe`, points at a lost declaration or an unready reconciler), and `sentio_node_storage_integrity_schema_compensations_total{outcome="failed"}`.
- **End-to-end check.** Run the umbrella §10 check with no restart and no config edit: CREATE → retryable `pending activation` → Active → signed INSERT → promotion → DROP → retire (reads `Table … does not exist`, same-name CREATE retryable) → purge → same-name CREATE → Active again (this plan's B8 is what makes the recreated table get its first declaration).
- **Recommendation.** Finish the query-parameter and physical-name security follow-up before enabling the watcher (spec §12).
