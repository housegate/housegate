# Dynamic storage-integrity table set — sub-project 4: data plane and sentio-node host — Design

**Status:** design, approved section by section on 2026-09-25.

**Parent specs:**
- [2026-09-23-dynamic-si-table-set-design.md](2026-09-23-dynamic-si-table-set-design.md) (the umbrella). This document refines its §7 and §9. §15 of the umbrella records the amendments.
- [2026-09-24-dynamic-si-table-set-housegate-design.md](2026-09-24-dynamic-si-table-set-housegate-design.md) (sub-project 3). Its §13 is the host contract that this sub-project implements.

**Repositories:** arbiter-proto, arbiter, arbiter-core (plan A: data plane), sentio-core and sentio-node (plan B: host). Housegate only cuts a release here. The production repo is sub-project 5.

## 1. Goal

When the arbiter's registry records a table as Pending, every data-plane node (the source SNode and every verifier) creates that table's `hg_unsafe`, `hg_safe` and `hg_promote` tables without a restart or a config edit. Housegate is told that a table is Active only once the local tables exist. When a table is retired and reaches Purging, every node drops its tables and Keeper paths and reports `RecordTablePurged`, after which the name can be reused. sentio-node supplies housegate's `sitable.TableState`, serves the agent's status JSON-RPC, and re-declares schemas whose declaration transaction was lost.

**Success criterion.** Sub-project 5 can run the umbrella §10 end-to-end check on devnet2 without restarting any role or editing any config: CREATE → retryable error → Active → INSERT → promotion → DROP → retire → purge → same-name CREATE.

## 2. Measured facts this design rests on

Measured on 2026-09-25 against:
- arbiter-core `d059aab`, which pins arbiter-proto `1b3de4c` and housegate `db46c31`;
- arbiter `ee26f54`;
- arbiter-proto `a3dbb1f`;
- sentio-node `1494fb1`, which pins housegate v0.14.1, arbiter-core v0.9.0 and arbiter-proto `4656854`;
- housegate `04fb8c0`.

**arbiter-core does not know about dynamic tables.**
- Its arbiter-proto pin predates the `TableRegistry` service, and `wire` has no `TableRegistrySnapshot` decoder.
- No gRPC method lets a data-plane node submit `RecordTablePurged`, which exists only as the Raft command `RecordTablePurgedCmd` (tag 35).

**Both roles read only a static table set.**
- SNode reads `snode.Config.Tables` in `schemaFor` (`snode.go:502`, used by intake `staged.go:252` and promotion `promote.go:33`, `promote_replace.go:28,103`), and in the diagnostic source-claim root (`view.go:13-35`, which also uses the static `SchemaRoot`).
- The verifier's `CHScanner.schemaFor` (`backends.go:136`) likewise reads only `cfg.Tables`.
- The verifier's replay executor is already `payloadexec.NewDynamic` (2b). It attests a transition block from the job alone and does not check that its own tables exist.

**Protocol-table DDL creates and verifies, but never drops.**
- `dataplane/ddl.EnsureProtocolTables` works on the whole configured list at once: every table succeeds or none does.
- The 60-second reconcile loop in both roles is verify-only, and a missing table is fatal.
- No code runs `DROP TABLE … SYNC`, removes a Keeper path, or sweeps unknown `hg_*` tables.
- The only drops today are part-level.

**The existing leader call helper cannot carry a stream.** `dataplane.Client.WithLeaderRetry` gives each call a 5-second timeout. The unexported `runSubscription` loop (`dataplane/subscribe.go:18`) already follows leader hints, reconnects and backs off for streams.

**sentio-node's current storage-integrity wiring:**
- It injects `MergeConn` and runtime `TableSchemas`, but no `MergeGuard`. The guard interface changes break only two test fakes.
- It validates `snode.table_ids ⊆ housegate.storage_integrity.tables` and builds its read state from the same lists.
- Nothing in sentio-node streams from the arbiter.
- It registers no metrics of its own. Housegate exposes `Proxy.MetricsRegistry()`.

**The chain state has no table creation block.** sentio-core's `TableInfo` is `{TableId, TableType, SchemaVersion, SchemaHash}`. The `onTableCreated` handler receives `BlockNumber` but discards it. The syncer follows the chain head.

**Schema declaration can be lost.** sentio-node's Observer declares a schema asynchronously after a successful `CREATE TABLE` and only logs a failure. `schema_backfill` covers only `snode.table_ids`.

## 3. Scope

**In scope:**
- the `SubmitTablePurged` RPC and its arbiter handler;
- the registry snapshot decoder, the registry follower and the table-set reconciler in arbiter-core;
- the SNode and verifier integration;
- the arbiter `cmd` config semantics;
- restore tightening P12;
- `TableInfo.CreatedBlock` in sentio-core;
- the sentio-node `TableState`, its JSON-RPC, config migration, declaration compensation and metrics;
- the dependency bumps and a housegate release.

**Out of scope:**
- Deferred to sub-project 5: the helm chart changes, the production gRPC rewriter bump (Gate R4) and the devnet2 rollout.
- The snapshot-query lane's dynamic appender. The lane is default-off and not wired.
- The query-parameter and physical-name security follow-up, which is tracked separately.
- ALTER of storage-integrity tables.

## 4. Decisions

- **E1. One spec, two plans.**
  - Plan A is the data plane: arbiter-proto, then arbiter-core, then arbiter.
  - Plan B is the host: a housegate release, sentio-core, then sentio-node. It depends on plan A's releases.
- **E2. Level-triggered reconciliation.** Each role derives a desired table state from the registry, compares it with ClickHouse, and closes the difference. The alternatives were rejected:
  - Event-by-event diffs need their own crash compensation.
  - Arbiter-pushed commands duplicate information the registry already carries.
- **E3. The verifier follows the registry, like the SNode.** One follower and one reconciler serve both roles. Creation, purge, readiness and scanner schemas all come from the same source. This resolves the umbrella's open question of whether verifiers need registry access.
- **E4. `RecordTablePurged` goes through a new idempotent RPC.** Resubmission is safe: an already-recorded node or an already-Purged incarnation returns success. `FailedPrecondition` means "not purging yet, retry later" and is never read as success.
- **E5. The activation window uses the creation block recorded by sentio-core.**
  - `TableInfo.CreatedBlock` is new. A zero value means the table was created before the upgrade, which makes it Legacy by construction.
  - Rollout rule: `activation_block` must follow the block by which every storage-integrity indexer node runs the new sentio-core.
  - These chain facts are the syncer state sentio-node already trusts for routing and permissions. Umbrella D4 governs membership, which still comes only from the registry.
- **E6. A table is Active to housegate only when the registry says Active and the local reconciler reports it Ready.**

## 5. arbiter-proto and arbiter: the purge report

**RPC.** Add `SubmitTablePurged(node_id, incarnation_seq)` to the data-plane gRPC service that already carries `AckCleanup`, authenticated the same way.

**arbiter handler.** The leader proposes `RecordTablePurgedCmd{node_id, incarnation_seq}` and answers as follows:

| Situation | Answer |
|---|---|
| The node is already recorded for the incarnation, or the incarnation is already Purged | success |
| The incarnation is not Purging (Active or Retiring) | `FailedPrecondition` |
| This arbiter is not the leader | `NotLeader`, like the other data-plane RPCs |

**arbiter `cmd` config.**
- In `arbiter-snode` and `arbiter-verifier`, `tables` / `table_ids` now name only the genesis table set. That set is used while the registry is disabled.
- Following the registry needs no new key. Every role with arbiter peers follows it.
- Startup preflight waits only for the genesis schemas.

**Restore tightening (P12, carried from 2c).** Restore refuses:
- a chain-origin Retiring incarnation without an add block;
- a chain-origin incarnation without `schema_json`.

The orchestrator fixtures that forged those states are rebuilt.

## 6. arbiter-core: decoder and registry follower

**Proto pin.** arbiter-core bumps arbiter-proto to the release that carries the `TableRegistry` service and `SubmitTablePurged`.

**`wire` decoder.** Go types mirror `TableRegistrySnapshot`: params, version, seeded, cursor, and incarnations with status, origin, retire reason, refused code and reason, schema JSON, hash and version, add and retire block sequence numbers, and `purged_by`.
- Enum vocabularies map one-to-one to arbiter `fsm`. An unknown value is an error.
- Helpers:
  - `Live(key)`, with the same rule as arbiter's `TableRegistryView.Live`: the newest incarnation that is neither Purged nor Refused, else the newest;
  - `ActiveTables()`;
  - schema decoding to `payloadexec.TableSchema`.

**`dataplane` `RegistryFollower`:**
- **Startup.**
  1. Call `GetTableRegistry` once through `WithLeaderRetry`.
  2. `FailedPrecondition` "table registry is disabled" marks the follower Disabled.
  3. Open `WatchTableRegistry(since_version)` on a stream built from the existing subscription loop. That loop becomes reusable: it follows leader hints, reconnects and backs off, and has no per-call timeout.
  4. While the registry is disabled, the watch waits and delivers the first snapshot once governance enables it.
- **State.**
  - Each message is the whole truth.
  - Only a strictly greater version is accepted.
  - After a disconnect, the follower resumes from the last version seen.
- **Interface.** `View() (snapshot, enabled bool)`, `Changed() <-chan struct{}` (closed at the next accepted version after the call), and `Ready()` (after the first successful `Get`).
- **Startup gate.** A role waits for `Ready`. If it cannot reach the arbiter within its startup timeout, startup fails. While Disabled, roles use the configured genesis set exactly as today. After enablement the genesis tables are seeded as Active, and the registry takes over.

## 7. arbiter-core: the table-set reconciler

One reconciler instance runs in each role. It runs on every `Changed()` and on the existing reconcile interval, and it replaces the verify-only reconcile loop.

### 7.1 Desired state (registry enabled)

| Registry state of the key's incarnations | This node |
|---|---|
| Pending, Active or Retiring (including genesis tables) | all three `hg_*` tables exist and verify |
| Purging | all three dropped, then `SubmitTablePurged` |
| Purged, Refused, Legacy | no `hg_*` tables. Leftovers of an earlier incarnation of the key are dropped (for example a node that was offline during purge). |
| no entry at all | unknown `hg_*` tables are only reported (log and metric), never dropped |

Umbrella D9 guarantees at most one non-Purged storage-integrity incarnation per key, so physical names never collide. While the registry is disabled, the reconciler covers only the configured genesis set in its configured mode, which is today's behaviour.

### 7.2 Creation

- **Per-table entry point.** `ddl` gains a per-table create-and-verify entry point, generated from the registry's `schema_json`.
- **Which tables are created.**
  - Chain-origin tables are always created.
  - Genesis tables keep their configured `schema_source` mode.
- **Drift.** An existing table that has drifted is fatal, as today. It is never auto-repaired.

### 7.3 Readiness

`Ready(tableID)` is true when all three tables of the current incarnation exist and verify. It gates two things:
- the verifier's attestation (§8);
- sentio-node's Active report (§9).

### 7.4 Purge ordering

1. Purge starts only when the incarnation is Purging. Reaching Purging already means the retire block is Safe and the safe watermark covers it.
2. The SNode also waits until a quiescence hook reports that no promotion or unsafe cleanup still references the table. The verifier has no such work.
3. Drop `hg_promote`, then `hg_safe`, then `hg_unsafe`, each with `DROP TABLE … SYNC`. For `hg_unsafe` this removes the node's replica from Keeper. The last replica's drop removes the Keeper table path.
4. **Decommissioned replicas.** After its own drop, the source SNode runs `SYSTEM DROP REPLICA '<r>' FROM ZKPATH '<path>'` for every replica under the table's Keeper path that is not in the current node set. Without this step the path survives, and a same-name recreation fails because the Keeper table structure differs.
5. Call `SubmitTablePurged`. Retry `FailedPrecondition` and transport errors with backoff.

### 7.5 Failures and metrics

- **Failures.** Errors are isolated per table, with per-table backoff.
- **Metrics:**
  - tables per reconciler state;
  - per-table reconcile failures;
  - unknown `hg_*` tables.

## 8. arbiter-core: role integration

### SNode

- **Intake** (`staged.go`, envelope schema resolution). The table must be Active in the follower's view, and the envelope's schema hash must equal the registry hash. The reconciler must also report the table Ready; otherwise the intake refusal is retryable. This is defence in depth, because housegate refuses first.
- **Promotion and replace** (`promote*.go`). These accept the key's current non-Purged storage-integrity incarnation (Active, Retiring or Purging), so statements admitted before a retirement can still be promoted.
- **Source-claim root** (`view.go`, diagnostic). It is computed over the tables still in the state root (Active ∪ Retiring), with `SchemaRootFromHashes` over the registry hashes instead of the static `SchemaRoot`. The plan verifies that the root is diagnostic only. If any consumer compares it, it is computed per block instead.
- **Reconciler identity.** The reconciler uses the SNode node id as its replica name. Promotion and cleanup provide the quiescence hook.

### Verifier

- **Attestation gate.** For a replay job whose table-set transition adds tables:
  1. Check that every added table is Ready.
  2. If one is not, trigger a reconcile and wait a bounded time.
  3. If it is still not Ready, return an error and do not attest. The job is re-dispatched, or another verifier forms the quorum.

  An add transition therefore cannot reach quorum before verifiers have their tables. The wait bound and the re-dispatch behaviour are fixed in the plan after measuring the dispatch path.
- **Executor.** Unchanged. It is `payloadexec.NewDynamic` over the job's carried schemas.
- **Scanner.** While the registry is enabled, `CHScanner.schemaFor` (and the SNode's schema lookup) let the key's live registry incarnation decide: a genesis-origin incarnation uses the configured genesis schema, a chain-origin one its registry schema. Only while the registry is disabled do they fall back to the configured genesis tables. Checking the genesis tables first would scan a same-name chain recreation of a retired genesis table with the stale genesis schema (plan A ruling P12).
- **Reconciler identity.** The reconciler uses the verifier replica id. It needs no quiescence hook.

### Snapshot-query lane

The static table set stays. A read of a table outside the genesis set is refused with an explicit error. The dynamic appender is a handoff item for whenever the lane is enabled.

## 9. sentio-core and sentio-node: the host

### 9.1 sentio-core

- `TableInfo` gains `CreatedBlock uint64`. `onTableCreated` sets it, and recreation overwrites it.
- The Redis state mirror carries the field.
- A zero value means "created before the upgrade" (E5).

### 9.2 `TableState` implementation

**Inputs:**
- the arbiter-core `RegistryFollower`;
- the embedded SNode reconciler's `Ready`;
- the syncer's chain state: each database's `IndexerId`, and each table's existence and `CreatedBlock`;
- this node's indexer id;
- the genesis static set;
- the network id.

**Snapshots.**
- A snapshot is rebuilt whenever any input changes.
- The version increases only when the content changes.
- The first snapshot is built after the follower is Ready and before housegate runs, so `Current()` is in sync before `Run`.

**`Lookup(database, table)`,** evaluated in this order:
1. **Registry disabled:** the genesis tables are Active and everything else is Ordinary, as with `sitable.Static`.
2. **Database outside the registry's `si_indexer_id`, or unknown:** Ordinary.
3. **The key has a live incarnation** (selected by `Live`):

   | Incarnation status | Reported as |
   |---|---|
   | Legacy | Ordinary |
   | Pending | Pending |
   | Refused | Refused, with `refused_code` and `refused_reason` |
   | Active | Active **only if the reconciler reports Ready**, otherwise Pending |
   | Retiring or Purging | Gone, with the schema kept |
   | Purged | treated as unrecorded (continue to step 4) |

4. **No live incarnation:**
   - If the seed has not committed and `CreatedBlock < activation_block` (zero included): Ordinary. This is sub-project 3's H3.
   - Otherwise: Pending (default deny).

**Active schemas.** An Active table's schema is decoded from the registry's `schema_json`. The snapshot builder recomputes `payloadexec.TableSchemaHash(network_id, schema)` and requires it to equal the registry's `schema_hash`. On a mismatch it logs an error and reports the table Pending, so a format drift cannot make every signed INSERT fail.

### 9.3 JSON-RPC

- `StorageNodeService.GetStorageIntegrityTableStatus(database, table)` is served as `sentio_getStorageIntegrityTableStatus`.
- The result shape is exactly sub-project 3's §10.2 contract: status name, refused code and reason, schema JSON and hash, and registry version.
- A node without storage integrity answers `ordinary`.

### 9.4 Config migration

When sentio-node's storage integrity is enabled:
- It injects the `TableState` and sets `housegate.storage_integrity.enabled = true`.
- `snode.table_ids` now means the genesis table set.
- **Compatibility.** A legacy `housegate.storage_integrity.tables` equal to `snode.table_ids` is ignored with a deprecation warning. A different list is a config error.
- The `table_ids ⊆ housegate tables` rule is removed.
- The read state is always the embedded SNode role.
- The two test fakes move to the new MergeGuard interface. Production injects no guard.

### 9.5 Declaration compensation

- A background loop runs on the storage-integrity indexer node, every 5 minutes by default.
- It finds tables of this indexer's databases that exist on chain but have no schema declaration, and that were first seen more than 10 minutes ago.
- For each one, it runs the existing declarer against the local ordinary physical table.
- The declarer canonicalizes column types with `payloadexec.CanonicalColumnType` before declaring.
  - An unsupported type is still declared. The arbiter records the incarnation as Refused, and the client receives the refusal code and reason, which is the designed behaviour.

### 9.6 Metrics

Registered on housegate's `MetricsRegistry()`:
- tables per status;
- Pending dwell time per table;
- declaration compensations by outcome;
- registry follower connection state.

## 10. Testing

| Repository | Focus |
|---|---|
| arbiter | `SubmitTablePurged`: idempotent success, `FailedPrecondition` before Purging, NotLeader. P12 restore refusals with rebuilt orchestrator fixtures. |
| arbiter-core | **wire:** decoder round trip; unknown enum refused. **Follower,** against a fake gRPC server: disabled → wait → first snapshot; resume after disconnect; NotLeader following; stale versions ignored. **Reconciler,** on Keeper-backed ClickHouse (the existing `integration-clickhouse` CI job): Pending creation; Active verification; Purging drop with the Keeper path gone and the report sent; decommissioned-replica drop; convergence after a crash mid-create and mid-purge; leftover sweep; per-table isolation. **SNode:** intake and promotion schema resolution. **Verifier:** no attestation before Ready, attestation after; scanner for chain tables. |
| sentio-core | `CreatedBlock` set on create and recreate; mirrored to Redis. |
| sentio-node | **`TableState`:** the full `Lookup` matrix including the activation window, disabled registry and readiness; version bumps only on content changes. JSON-RPC wire shape. Config-migration compatibility. Declaration compensation. **Acceptance:** the existing ClickHouse acceptance test gains a dynamic lifecycle driven by a fake registry server. |

## 11. Release order

1. **Plan A:**
   1. arbiter-proto, with the new RPC. Tag it.
   2. arbiter-core: decoder, follower, reconciler and roles. Tag it.
   3. arbiter: handler, `cmd` config semantics and P12. Merge it.
2. **Plan B:**
   1. A housegate release tag on `04fb8c0` or later. sentio-node's Bazel `git_override` needs a version.
   2. sentio-core with `CreatedBlock`. Tag it.
   3. sentio-node: all pin bumps, the `TableState`, JSON-RPC, config migration, declaration compensation and metrics.
3. **Compatibility.** A new role talking to an old arbiter gets `Unimplemented` from `SubmitTablePurged`. It treats this as retry-later. No incarnation can be Purging before the registry is enabled.

## 12. Handoff to sub-project 5 (devnet2 rollout)

- **Chart.** The chart drops the `tables == tableIDs` guard and renders `housegate.storage_integrity.enabled`. The chart is shared, so this change goes through a PR.
- **Gate R4.** Deploy the in-pod rewriter `housegate-rewriter:0.15.0@sha256:57812c8c…` together with the sentio-node upgrade.
- **Order:**
  1. Arbiter voters, on 2c plus this sub-project.
  2. Verifiers.
  3. SNodes and sentio-node.
  4. Confirm that every storage-integrity indexer runs the sentio-core with `CreatedBlock`.
  5. Choose an `activation_block` after that point.
  6. Enable the registry.
  7. Enable the auditors everywhere, and confirm there are no mismatches.
  8. Enable the watcher.
- **Agent.** The agent sidecar's `network_state.source` points at its co-located indexer RPC.
- **End-to-end check.** Run the umbrella §10 check.
- **Recommendation.** Finish the query-parameter and physical-name security follow-up before enabling the watcher. Some of its findings, such as the gRPC engine accepting `MATERIALIZED VIEW … TO` a storage-integrity table, matter only once chain-origin Active tables exist.

## 13. Risks

- **Reconciler drops the wrong tables.** Drops are limited to Purging incarnations and to leftovers of keys the registry records as Purged, Refused or Legacy. Unknown tables are only reported. Every drop is logged and counted.
- **Stuck Purging.** A node that cannot drop, or cannot reach the arbiter, blocks Purged and with it same-name recreation (umbrella §12 "Purge liveness"). Per-table metrics expose this.
- **Follower stream loss.** The registry view goes stale while the stream is down.
  - Housegate keeps serving the last snapshot, which is fail-closed for new tables because they stay Pending.
  - Retirements take effect on the next snapshot.
- **Activation-window rule.** Choosing `activation_block` before every node runs the new sentio-core would report some tables Ordinary that the seed later records differently. The rollout rule in §12 prevents this.
