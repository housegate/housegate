# Dynamic storage-integrity table set — Design

**Status:** design, approved section by section on 2026-09-23; umbrella spec. Each sub-project (§11) gets its own implementation plan in the repository that owns it.

**Repositories:** housegate, rewriter-proto / rewriter-go / rewriter-grpc, arbiter-core, arbiter (private), sentio-node, production.

## 1. Goal

Every user table created on a storage-integrity (SI) indexer after an activation point becomes an SI table automatically, without restarting any role, editing any config, or resetting the network. Dropping such a table retires it from consensus and releases its physical storage; its name can then be reused.

Today the SI table set is pinned statically in six places: housegate `storage_integrity.tables`, the agent sidecar's YAML `table_schemas`, the source SNode's `table_ids`, each verifier's `tables`, the arbiter genesis (`SchemaRoot` plus one `TableManifest` per table, hashed into the genesis snapshot id), and the arbiter FSM, which rejects any manifest whose table set differs from genesis (`fsm/manifest_validation.go`, `fsm/genesis_validation.go`, `orchestrator/promotion.go`). Adding a table therefore requires a new genesis.

## 2. Measured facts this design rests on

- sentio-node already declares every successful user `CREATE TABLE` on chain (`database_registry/observer.go` `AfterStatementSuccess` → `schema_declarer.go` → `Databases.setTableSchema`) and mirrors `TableSchemaSet` into Redis (`statemirror:v1:TableSchemas`). On devnet2 on 2026-09-23 `devnet101/swap_new3@1` and `swap_new4@1` were present there minutes after creation.
- `Databases.deleteTable` only flips `active=false` and emits `TableDeleted`; `createTable` explicitly allows re-creating a dropped id. Schema versions are allocated monotonically per `(databaseId, tableId)` and survive deletion and recreation, so a recreated table's first schema is version ≥ 2 (`compute-network-contracts/src/Databases.sol`).
- `createTable` always passes `tableType = "user"` from the observer; `tableType` drives billing SKU mapping (`standalone/indexingusageadapter/sku.go`).
- The arbiter FSM `Apply` is deterministic and reads no chain; chain observations are made leader-side and proposed as commands (precedent: `RecordAnchorFinality`, `orchestrator/promotion.go`). The arbiter already has an L2 client for anchoring (`anchor/evm`), and on devnet2 the anchor chain (7892301) is the same L2 that hosts the Databases contract.
- L3 block headers carry no table set; each block pins its base via `PrevSafeSnapshotID`, and `FSM.ManifestInputs` copies the table set and `SchemaRoot` from the parent manifest. Statement admission does not check `TargetTableID` (`fsm/admission.go` step 4 is a placeholder; `SCHEMA_NOT_ALLOWED` is reserved).
- There is no general FSM watch stream; SNode and verifier learn state through unary `SafeState` reads and two push streams (verifier dispatch, promotions).
- On devnet2 on 2026-09-23 the L2 `safe` head lagged `latest` by about 12 minutes and `finalized` by about 23 minutes.

## 3. Scope

In scope: automatic SI membership for new tables on one SI indexer per network, table retirement on `DROP TABLE` / `DROP DATABASE`, physical purge, and same-name recreation after purge.

Out of scope: schema changes (`ALTER`) of SI tables — they stay rejected, but the registry data model keeps a schema version so a later spec can add them; bringing existing (pre-activation) tables into SI; more than one SI indexer per network; reorg handling below the configured confirmation level.

## 4. Decisions

**D1 — Scope is one SI indexer per network.** Membership applies to tables whose database belongs to the configured `si_indexer_id`. Other indexers are unaffected.

**D2 — Existing tables stay non-SI ("Legacy").** Tables active on the SI indexer at `activation_block` are recorded as Legacy and keep the ordinary read/write path forever. A Legacy table that is dropped and recreated after activation becomes an SI table (new incarnation).

**D3 — Membership is derived from chain events plus consensus parameters; no contract change.** A table incarnation is one `TableCreated` event. It is an SI incarnation iff its `TableCreated` is at L2 block ≥ `activation_block` and its database's `indexerId == si_indexer_id` (read at the same block). Its schema is the first `TableSchemaSet` for that `(databaseId, tableId)` after that `TableCreated`. Rejected: a new `tableType` value (overloads the billing dimension) and a contract-level SI flag (requires a shared-contract upgrade; its extra value — per-table opt-in/out and chain-only auditability — is not needed by D1/D2).

**D4 — The arbiter's replicated table registry is the single authority.** Every role and housegate follows the committed registry; nobody derives membership independently from the chain.

**D5 — Chain observation is leader-attested, audited asynchronously.** The leader's L2 watcher proposes commands carrying chain evidence; `Apply` performs only deterministic checks. Every node runs an asynchronous auditor that re-checks committed evidence against its own L2 RPC and alarms on mismatch without affecting consensus. This is the same trust level as `RecordAnchorFinality`. Rejected: receipt Merkle proofs in the command — `Apply` still could not prove the block is canonical.

**D6 — Confirmation level is a consensus parameter.** `confirmation ∈ {finalized, safe}`, default `finalized`; devnet2 uses `safe`. `latest + N` is out of scope because a committed registry change cannot be undone after a reorg.

**D7 — Table-set changes take effect through a dedicated empty transition block.** After `AddTable` / `RetireTable` commits, the orchestrator seals an L3 block with no statements whose header carries the transition. It rides the ordinary replay → quorum → anchor → promotion pipeline, so the change is verifier-attested and anchored, and "the state root no longer contains table T from block N" has an exact N.

**D8 — `DROP` retires and then purges.** Retirement removes the table from the state root at its transition block. Physical tables and Keeper paths are dropped once that block is Safe (which implies anchored-final) and the safe watermark has reached it. History stays verifiable through earlier anchored blocks and DA.

**D9 — Same-name recreation waits for purge.** `CREATE TABLE` for a name whose previous SI incarnation is not yet Purged is refused with a retryable error before any on-chain call. Physical naming (`hg_*.<db>__<table>`, Keeper `/sentio/0/unsafe/<db>__<table>`), `row_id` derivation and the signed target id are unchanged. Rejected: per-incarnation physical names (touches `row_id`, statement target and the frozen D2 naming).

**D10 — Default deny on the SI indexer.** In housegate, a table on the SI indexer that the registry does not record as Legacy is treated as Pending, including a table whose declaration has not reached the chain yet. No write can land in an ordinary table that will later be shadowed by `hg_*`.

**D11 — Registry parameters are enabled by governance, not genesis.** `databases_contract`, `si_indexer_id`, `activation_block` and `confirmation` extend `ConsensusParamsUpdate` (authority-signed). A running network (devnet2) upgrades without a genesis reset; the FSM snapshot migration seeds the registry from the genesis tables as Active.

## 5. Table incarnation lifecycle

```
            TableCreated (chain)
                   │
                Pending ──(schema declared + confirmed → AddTable committed → add transition block Safe, local physical tables ready)──▶ Active
                   │                                                                                                                    │
                   │                                                                                                    TableDeleted (chain)
                   ▼                                                                                                                    ▼
                Refused (terminal: unsupported column type or physical-name collision)                                              Retiring
                                                                                                                                        │ RetireTable committed; retire transition block N sealed
                                                                                                                                        │ block N Safe and safe watermark ≥ N
                                                                                                                                        ▼
                                                                                                                                     Purging ── all roles drop hg_* + Keeper path, RecordTablePurged ──▶ Purged (name reusable)
```

A `TableDeleted` on a Pending incarnation retires it too: if its add transition was not yet sealed the `AddTable` is cancelled and it moves directly to Purging; otherwise it follows the Retiring path. `DROP DATABASE` retires every SI incarnation in the database. A `TableCreated` followed by `TableDeleted` before any schema declaration reaches the confirmation head produces no registry entry.

Housegate-visible behaviour:

| State | Read | Write | Same-name `CREATE` |
|---|---|---|---|
| Legacy | ordinary | ordinary | n/a |
| Pending | retryable error | retryable error | table exists |
| Refused | non-retryable error naming the reason | same | table exists |
| Active | rewritten to `hg_safe` / `hg_unsafe` | signed SI lane | table exists |
| Retiring / Purging | `UNKNOWN_TABLE` (ordinary table already dropped) | same | retryable error |
| Purged | `UNKNOWN_TABLE` | same | allowed; new Pending incarnation |

## 6. Consensus layer (arbiter, arbiter-core wire, arbiter-proto)

**Registry state (FSM, snapshot v15).**

```
tableKey(db.table) → {
  incarnation: {createdBlockNumber, createdBlockHash, createdLogIndex} | genesis | legacy-seed,
  status: Legacy | Pending | Refused | Active | Retiring | Purging | Purged,
  schemaVersion, schemaHash, schemaJSON, refusedReason,
  addBlockSeq, retireBlockSeq, purgedBy: set<roleID>,
  evidence: [{kind, blockNumber, blockHash, logIndex, txHash, fields}]
}
L2Cursor { chainID, contract, lastBlockNumber, lastBlockHash }
RegistryVersion uint64   // bumped on every mutation
```

Purged incarnations are retained (compacted to key, incarnation and status) so the audit trail survives; the live-name index points at the newest incarnation.

**Commands** (new `RaftCommand` oneof tags; gated like `ConsensusUpdatesEnabled`: every voter must run the new binary before governance enables the registry):

- `SeedLegacyTables{atBlock, tables[]}` — once, at `activation_block`, the tables active on the SI indexer at that block.
- `AddTable{key, created evidence, schema evidence}`.
- `RetireTable{key, incarnation, deleted evidence}` — table or database deletion.
- `AdvanceL2Cursor{toBlockNumber, toBlockHash}` — progress when no relevant event occurred.
- `RecordTablePurged{roleID, key, incarnation}`.

**Deterministic Apply checks.** Cursor continuity (every command's evidence is at or after the cursor and advances it monotonically); `schemaHash == TableSchemaHash(networkID, schemaJSON)`; every column type accepted by `payloadexec.ResolveColumnProfile`; the physical name is injective against every non-Purged SI incarnation in the registry (Legacy tables have no `hg_*` names) (`ValidatePhysicalTableNames`). A failed type or name check records the incarnation as Refused rather than rejecting the command, so the cursor still advances.

**L2 watcher (orchestrator, leader only).** Reuses the anchor L2 RPC with a new `databases_contract` address. It scans `TableCreated`, `TableSchemaSet`, `TableDeleted` and database-deletion events from the cursor up to the configured confirmation head, resolves each database's `indexerId` at the event block, applies D3, and proposes commands in log order. It never proposes beyond the confirmation head.

**Transition blocks.** `L3BlockHeader` gains `TableSetTransition{adds[], retires[], newSchemaRoot}`. The orchestrator seals a transition block, containing no statements, at the next seal opportunity after an `AddTable` / `RetireTable` commits. A retire transition is sealed only after every statement already admitted for the table is in a sealed block. When a transition block becomes Safe, the next published manifest carries the new table set and `SchemaRoot`; `AddTable` incarnations move to Active and `RetireTable` incarnations to Purging.

**Admission.** `fsm/admission.go` step 4 requires `TargetTableID` to be Active and `SchemaHash` to equal its registry hash; otherwise `SCHEMA_NOT_ALLOWED`. A table leaves admission the moment its `RetireTable` commits.

**Relaxed validation.** `manifest_validation.go`, `genesis_validation.go` and `orchestrator/promotion.go` compare a manifest's table set and `SchemaRoot` against the registry state as of the manifest's block instead of the configured genesis. `validateConfiguredGenesis` stays for genesis only. `validateRestoredState` re-derives the registry's table-set history and checks every restored manifest against it.

**Purge completion.** An incarnation becomes Purged when `RecordTablePurged` has been committed by the source role and every verifier in the current consensus parameters.

**Read API.** New `TableRegistry` service: `GetTableRegistry()` (full snapshot plus `RegistryVersion`) and `WatchTableRegistry(sinceVersion)` (server stream of changes), both leader-barriered like `SafeState`.

## 7. Data plane (arbiter-core: SNode, verifier, ddl)

- **Dynamic table set.** SNode and verifier load the registry at startup and follow `WatchTableRegistry`, resuming from the last seen version after reconnect. `schemaFor`, intake schema resolution, promotion lookups and the diagnostic source-claim root read the registry-backed set; schemas come from the registry's `schemaJSON`. Static `tables` / `schema_root` config is used only to validate genesis.
- **Replay executor.** housegate `payloadexec` takes a schema resolver instead of a fixed table list; `apply_rows.go` derives the expected `prev.SchemaRoot` from the registry schemas of the tables in `prev`.
- **Create on Pending.** When `AddTable` commits, each role runs `EnsureProtocolTables` in create-and-verify mode for that table: `hg_unsafe` (ReplicatedMergeTree at `/sentio/0/unsafe/<db>__<table>`, one replica per role), `hg_safe` and `hg_promote` with the pinned merge setting. A verifier refuses to attest an add transition block until its own tables for that table exist, so an add cannot reach quorum before the verifiers are ready.
- **Purge.** When an incarnation enters Purging, each role runs `DROP TABLE ... SYNC` on its three tables (the last replica removes the Keeper path; `SYSTEM DROP REPLICA` cleans replicas of decommissioned verifiers) and submits `RecordTablePurged`.
- **Reconcile loop.** The 60-second protocol-table loop follows the current registry: create missing tables for Pending/Active, re-run idempotent purge for Purging, and only alarm on unknown local `hg_*` tables.
- **Unchanged:** DA, D2 physical naming, `row_id` derivation, `lthash` row encoding.

## 8. housegate

- **Host port.** New `Options.StorageIntegrityTableState`:

  ```go
  type TableState interface {
      Status(ctx context.Context, logicalTableID string) (TableStatus, error) // Legacy | Pending | Active | Gone | Refused(reason)
      ActiveSet() ActiveTables                                               // current Active set + registry version
      Schema(logicalTableID string) (payloadexec.TableSchema, bool)          // includes Retiring/Purging until Purged
  }
  ```

  Tables not in the registry on the SI indexer report Pending (D10).
- **Explicit switch.** `storage_integrity.enabled` replaces every `len(Tables) > 0` switch (fail-closed rewriter handling, SET refusal, startup probe, contract acknowledgement, `sireserved`). Static `tables` remain for the standalone binary and tests through a static `TableState`; configuring both is a validation error.
- **Rewriter.** `buildStorageIntegrityArgs` reads `ActiveSet()` per query; the exception scrubber is cached per registry version.
- **`sitablestate` plugin.** Runs after rewrite. For each `AccessedTables` entry: Pending → retryable, query-terminal `ClientError` (`KeepSession`, same class as code-252 back-pressure; exact code fixed in the housegate plan); Refused → non-retryable error with the reason. Peer-trusted, forwarded and maintenance sessions keep their existing bypass rules. Gone tables need no handling: the ordinary table is already dropped and ClickHouse answers `UNKNOWN_TABLE`.
- **DDL.** `DROP TABLE` on an Active or Pending SI table is allowed and drops only the ordinary physical table; `hg_*` tables are purged by the data plane. This is a rewriter contract change in rewriter-go, rewriter-grpc and the shared `storage_integrity_cases.json`, with a contract version bump. Other DDL on SI tables stays rejected.
- **MergeGuard.** Table list comes from `TableState` (Pending and Active); health becomes per table so one unready table blocks only itself.
- **Runtime.** The schema resolver reads `TableState.Schema`; the startup bijection check is dropped (consensus owns collision checks). Parts pressure is already database-scoped.
- **Agent.** `RpcNetworkState` implements `registry.TableSchemas` and a table-status call backed by the new sentio-node RPC (§9); the sidecar points `network_state.source` at its co-located indexer and drops the static YAML. `sistatement` queries status per INSERT without caching: Active → sign; anything else → pass through and let the server decide.

## 9. sentio-node

- **`TableState` implementation.** Follows `WatchTableRegistry` over the existing arbiter gRPC client. Reports Active only when the registry says Active and the embedded SNode has ensured the table's physical tables.
- **Wiring.** `SNode.TableIDs`, `housegate.storage_integrity.tables` and the startup contract schema snapshot are replaced by the registry; startup preflight only requires the registry to be reachable.
- **commitgate Observer.** `CREATE TABLE` for a name whose previous SI incarnation is not Purged → retryable error before `createTable`. `DROP TABLE` and post-CREATE schema declaration keep their current behaviour.
- **Declaration compensation.** A background reconciler re-declares any table on this indexer with a `TableCreated` but no schema declaration after a configurable delay (reusing `schema_backfill`), so a failed declaration transaction cannot leave a table Pending forever.
- **JSON-RPC.** `sentio_getTableSchema(db, table, version)`, `sentio_getLatestTableSchema(db, table)`, `sentio_getStorageIntegrityTableStatus(db, table)`.
- **Metrics.** Tables per status, Pending dwell time per table, declaration compensations.

## 10. Testing

| Repository | Focus |
|---|---|
| arbiter | FSM Apply determinism for every new command, cursor continuity, Refused outcomes, retire drain ordering, purge acks, snapshot v15 round trip and v14→v15 migration, restore validation of table-set history; watcher against a fake L2 (confirmation levels, drop/recreate, database deletion); integration of a transition block through replay, quorum and promotion |
| arbiter-core | dynamic table set in SNode and verifier, create/purge including Keeper paths on the Keeper-backed ClickHouse containers, reconcile convergence after a crash mid-create or mid-purge |
| housegate | static `TableState`, `sitablestate` decisions per state, per-table MergeGuard health, agent sign/pass-through via RPC, DROP contract cases in both rewriter engines |
| sentio-node | `TableState` implementation, RPC methods, same-name recreate refusal, declaration compensation |
| devnet2 end to end | CREATE → retryable error → Active (≈13 min with `safe`) → INSERT → promotion → DROP → retire → purge → same-name CREATE |

## 11. Sub-projects and rollout order

1. **rewriter-proto, rewriter-go, rewriter-grpc** — DROP of SI tables, contract version bump.
2. **arbiter-core + arbiter** — registry, commands, watcher, transition blocks, `TableRegistry` API, dynamic SNode/verifier, snapshot v15. Registry disabled by default.
3. **housegate** — `TableState` port, explicit switch, `sitablestate`, per-table MergeGuard, dynamic runtime, agent RPC source.
4. **sentio-node** — `TableState` implementation, RPC methods, Observer changes, declaration compensation, wiring.
5. **production (devnet2)** — upgrade every role with the registry disabled and verify no behaviour change; enable the registry with an authority-signed `ConsensusParamsUpdate` (`activation_block` = a future L2 height, `si_indexer_id` = indexer-a, `confirmation = safe`); run the end-to-end check in §10.

## 12. Risks

- **Leader-attested chain observation.** A faulty leader could commit a wrong `AddTable`. Mitigated by the asynchronous auditor and by the table being inert until real signed statements target it; the evidence is permanently recorded for challenge.
- **Activation latency.** With `finalized`, a new table is writable only after about 25 minutes; clients must treat the Pending error as retryable.
- **Stuck Pending.** A table whose declaration never lands stays Pending; declaration compensation and the Pending dwell-time metric bound and expose this.
- **Purge liveness.** A permanently offline verifier blocks Purged, and with it same-name recreation, until it is removed from the consensus verifier set.

## 13. Amendments from sub-project 2a (arbiter registry core, merged 2026-09-23)

Sub-project 2a shipped as sentioxyz/arbiter-proto#13, sentioxyz/arbiter-core#39 and sentioxyz/arbiter#118. Implementation review changed or sharpened the design as follows; later sub-projects build on these, not on the original §6 wording.

- **Command shape.** Raft tags 31–35: `SeedLegacyTables`, `AddTable`, `RetireTables` (one command for a table deletion or every live table of a deleted database, table ids sorted), `AdvanceL2Cursor`, `RecordTablePurged{node_id, incarnation_seq}`. The registry cursor orders events by `(block_number, log_index)` and rejects a same-block event whose block hash differs from the cursor's; `AddTable` also rejects created/schema evidence in one block with different hashes.
- **Enablement.** `TableRegistryParams` is `ConsensusParamsUpdate` field 9 / `ConsensusMutableParams` field 4, set once and resent unchanged by every later update; genesis or bootstrap params carrying it are refused. Enabling seeds every genesis manifest table as an Active incarnation in stable table-id order, and is refused if any genesis table id is not `<database>.<table>`.
- **Refusal set (D5, §6 "Deterministic Apply checks").** Besides the hash, column-type and physical-name checks, `AddTable` records Refused for: a `.` inside the database or table id (it would make `TableKey` ambiguous); a declared `_hg_row_id` column; empty or duplicate column names; a partition-freeze violation. The checks reuse the validators arbiter's static table config uses, so a schema the FSM accepts is one every role can create.
- **Snapshots.** Format v15 carries the registry; while the registry is disabled the writer keeps producing v14, so the disabled rollout window (§11 step 5) stays rollback-safe and not-yet-upgraded followers can install snapshots.
- **Mixed-version prerequisite (§11 step 5).** An older voter drops field 9 and diverges, and `arbiter-admin consensus capability` still reports protocol version 1 for both binaries. Every voter's version must be verified out of band before enabling; the devnet2 rollout plan adds a distinguishing capability signal.
- **Prerequisites for 2c before anything proposes `AddTable`.** (1) The Refused/Pending decision currently depends on the housegate and ddl validator versions compiled into the arbiter, so a dependency bump could replay an old `AddTable` differently; freeze the rules (golden test or a governed profile version) and store stable refusal codes instead of library error text; refuse non-canonical type spellings. (2) Persist the retire reason and a per-incarnation last-changed registry version (for `WatchTableRegistry(sinceVersion)`). (3) The watcher must omit already-registered keys from `SeedLegacyTables` (one registered key rejects the whole seed). (4) Roles treat a late `RecordTablePurged` rejection ("not purging") as success, and a re-registered node reconciles stale `hg_*` tables against the registry at startup.

## 14. Amendments from the sub-project 3 design (housegate and the rewriter V2 contract, 2026-09-24)

[2026-09-24-dynamic-si-table-set-housegate-design.md](2026-09-24-dynamic-si-table-set-housegate-design.md) refines §5, §8, §9 and §11. It is authoritative for housegate and the rewriter engines.

- **Sub-project split (§11).** Sub-project 1 (the rewriter DROP contract, now `STORAGE_INTEGRITY_CONTRACT_V2`) is folded into sub-project 3. Sub-project 3 now covers the rewriter engines and housegate only. The sentio-node `TableState` implementation, its JSON-RPC server and its MergeGuard adapter move to sub-project 4, together with the data plane, because a table may be reported Active only after the local `hg_*` tables exist.
- **Host port (§8).** The per-call `TableState{Status, ActiveSet, Schema}` interface is replaced by a versioned, immutable `Snapshot` taken once per query. The statuses are `Ordinary` (Legacy or ungoverned), `Pending`, `Refused`, `Active` and `Gone` (Retiring/Purging). Every membership judgement belongs to the host, including default deny and incarnation selection.
- **Activation window.** Before the seed commits, a host reports a table whose latest `TableCreated` precedes `activation_block` as Ordinary, so existing tables see no interruption.
- **Behaviour table (§5).**
  - Metadata statements (`DESCRIBE`, `SHOW`, `EXISTS`) are allowed on Pending and Refused tables; only data reads and writes are refused.
  - Retiring and Purging tables are answered as unknown by housegate itself, because `DROP DATABASE` leaves the shared physical database to garbage collection.
  - Same-name `CREATE` refusal (D9) moves from the sentio-node Observer to housegate's `sitablestate`.
  - Data-carrying creation into a governed table (`CREATE ... AS SELECT`, materialized views with `POPULATE` or `TO` a governed table) is refused.
- **Rewriter contract.** V2 allows `DROP TABLE` of SI tables by dropping only the ordinary physical table. The contract is activated by version, not by table count, so session `SET` stays refused while the Active set is empty. The reserved physical databases travel explicitly in `reserved_databases` and stay protected with an empty Active set. The engines keep V1 unchanged, and housegate requires V2.
- **Agent and JSON-RPC (§8, §9).**
  - The agent signs only Active tables and passes every other INSERT through unsigned; the server stays the authority.
  - sentio-node exposes one method, `sentio_getStorageIntegrityTableStatus`. `sentio_getTableSchema` and `sentio_getLatestTableSchema` are dropped.

## 15. Amendments from the sub-project 4 design (data plane and sentio-node host, 2026-09-25)

[2026-09-25-dynamic-si-table-set-data-plane-design.md](2026-09-25-dynamic-si-table-set-data-plane-design.md) refines §7 and §9. It is authoritative for arbiter-core, arbiter, sentio-core and sentio-node.

- **Verifiers follow the registry.** SNode and verifier share one arbiter-core registry follower (`GetTableRegistry`, then a resumable `WatchTableRegistry` stream) and one level-triggered table-set reconciler. The reconciler replaces the verify-only reconcile loop. It creates the tables of Pending, Active and Retiring incarnations; drops the tables of Purging incarnations, together with their Keeper paths and any decommissioned replicas; and only reports unknown `hg_*` tables. The verifier refuses to attest an add transition until its own tables are Ready.
- **Purge reports.** `RecordTablePurged` reaches the arbiter through a new idempotent data-plane RPC, `SubmitTablePurged`:
  - an already-recorded node, or an already-Purged incarnation, succeeds;
  - `FailedPrecondition` means "not purging yet" and is retried, never read as success.

  This replaces the earlier "treat a late not-purging rejection as success" rule.
- **Activation window.** The window uses the table creation block recorded by sentio-core (`TableInfo.CreatedBlock`). A zero value is pre-upgrade, and therefore Legacy by construction, so `activation_block` must follow the upgrade of every storage-integrity indexer node. These chain facts are syncer state already trusted for routing. D4 governs membership, which still comes only from the registry.
- **Active.** A table is Active to housegate only when the registry says Active and the local reconciler reports it Ready.
- **Snapshot queries.** The snapshot-query lane keeps a static table set until it is enabled.
- **Observer.** The sentio-node Observer needs no D9 check. Declaration compensation re-declares tables without a schema, using canonical type spellings.
