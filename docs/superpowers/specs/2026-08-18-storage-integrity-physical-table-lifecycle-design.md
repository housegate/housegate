# Protocol-Owned Physical Tables, Pinned Settings, and Admission Back-Pressure

**Date:** 2026-08-18 **Status:** Proposed **Roadmap:** [v1 closure roadmap](2026-08-18-storage-integrity-v1-closure-roadmap.md) Spec C. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §6, §12.1–§12.4; [2026-07-06 P1c data plane](2026-07-06-arbiter-p1c-dataplane-design.md); [2026-07-28 schema registry](2026-07-28-schema-registry-design.md) / [Phase B](2026-07-29-schema-registry-phase-b-design.md); [2026-07-30 chain schema source](2026-07-30-arbiter-chain-schema-source-design.md). **Code base:** arbiter-core `829c44f` (`snode/config.go`, `snode/promote_replace.go`), housegate `c6f7a6d` (`pkg/storageintegrity/merge_guard.go`, `pkg/config/storage_integrity_config.go`), sentio-node `9f12620` (`standalone/standalone.go`, `database_registry/observer.go`). **Source of truth:** English version.

## 1. Problem

Three parts of §12 are specified as v1 invariants and have no owner in code:

1. **Nobody creates `hg_unsafe.*` / `hg_safe.*`.** The only `CREATE TABLE` in the data plane is `hg_promote.<t> AS hg_safe.<t>` (`snode/promote_replace.go`). `hg_unsafe` as `ReplicatedMergeTree` with the anchored zk path, `hg_safe` as `MergeTree`, the `_hg_row_id` column, and the pinned settings exist only in housegate's `pkg/integration/storage_promotion_mvp_test.go`. Onboarding a table is a manual DDL step per node (arbiter README), so replicas can drift (different engines, different settings, missing `_hg_row_id`) and a verifier that lacks the RMT table silently never receives candidate parts.
2. **The no-merge property is commanded, not pinned.** `MergeGuard` re-asserts `SYSTEM STOP MERGES` at startup and on an interval, but `max_bytes_to_merge_at_max_space_in_pool = 0` and `parts_to_throw_insert` / `parts_to_delay_insert` (§12.2, §12.3) are not in any DDL. Between a ClickHouse restart and the next re-assert, merges are live.
3. **§12.3 admission throttling is absent.** The design calls it "mandatory, not optional" and names the ingress HouseGate as the actor: when a partition's `hg_unsafe` part count approaches the pinned ceiling, back-pressure INSERTs with a retryable rejection instead of letting ClickHouse hard-fail with `Too many parts`. Today the first symptom of promotion falling behind is a hard error from ClickHouse inside the staged prepare.

Also recorded here so they are decided rather than implicit: the physical naming (`hg_unsafe.<db>__<table>` via `snode.CHTableName`, not §6's `Transfer_<table_id>`), the P1c partition-key freeze (bare `String` column), and the interim `hg_safe` no-merge policy.

## 2. Goals / non-goals

Goals: (1) every data-plane role derives identical protocol-owned DDL from the schema source and creates/verifies it idempotently; (2) the pinned MergeTree settings are in that DDL and verified against `system.tables`/`system.merge_tree_settings` on startup, fail-closed on drift; (3) the ingress HouseGate refuses SI INSERTs to a partition whose `hg_unsafe` part count crosses a soft limit, with a retryable error the client can distinguish; (4) an `hg_safe` growth signal exists.

Non-goals: the DDL / schema-transition lane (Keeper-signed DDL, `ADD COLUMN`, …), including hot onboarding of a table after a role has started; §12.4 ledger-gated `hg_safe` merges (stays stopped, see §7); per-account fair scheduling of the throttle; partition-key expressions beyond the P1c freeze; migrating existing hand-created tables (devnet2 has none).

## 3. Decisions

**D1 — Table set and schema come from one coherent startup schema snapshot, not from local ClickHouse discovery during reconcile.** `EnsureProtocolTables` takes the frozen `[]payloadexec.TableSchema` the role resolved at startup (`snode.Config.Tables` / verifier `cfg.Tables`, filled from `schema_source: network_state|chain|clickhouse` and bound to the configured `schema_root`). Under `clickhouse` schema source the routine still runs but can only *verify* (it would be circular to create from what it reads); creation requires `network_state` or `chain`. The 60-second reconcile re-verifies exactly that anchored startup set; it does not silently adopt a later table/schema declaration. Hot onboarding requires the separately authenticated schema-transition lane and is outside this spec, so v1 operators restart the role after the new coherent snapshot is configured. Rejected: creating tables from an independently re-read user's logical table on the source CH — replicas could observe different schema sets and would depend on the source's local state, exactly the trust the layer removes.

**D2 — Naming freeze:** `hg_unsafe.<CHTableName(table_id)>`, `hg_safe.<…>`, `hg_promote.<…>` where `CHTableName(id) = ReplaceAll(id, ".", "__")` and `table_id` is the logical `<database>.<table>`. Base design §6's `Transfer_<table_id>` form is retired (Spec B records it). Zk path `/sentio/{keeper_shard_id}/unsafe/{CHTableName}` (`keeper_shard_id = 0` in v1), replica name = the node id the role registers with the Arbiter.

**D3 — Pinned settings (frozen, part of the DDL, verified on startup):**

| Table | Engine | Settings |
|---|---|---|
| `hg_unsafe.*` | `ReplicatedMergeTree('/sentio/0/unsafe/<t>', '<node_id>')` | `max_bytes_to_merge_at_max_space_in_pool = 0`, `parts_to_delay_insert = 1000`, `parts_to_throw_insert = 3000`, `max_parts_in_total = 100000`, `replicated_deduplication_window = 0` |
| `hg_safe.*` | `MergeTree` | `max_bytes_to_merge_at_max_space_in_pool = 0` (interim, §7) |
| `hg_promote.*` | `MergeTree` (as today, `AS hg_safe.<t>`) | inherits `hg_safe` |

`replicated_deduplication_window = 0` because statement dedup is the accumulator's job and part production must stay exactly one part per `(statement, partition)`: rows re-inserted under a new `statement_id` carry different `_hg_row_id`s so RMT block dedup would never legitimately fire, and a same-statement retry is already prevented by the intake journal — leaving the window on would only add a hidden path that changes the candidate-part set. Columns: `_hg_row_id FixedString(32)` first, then the declared user columns in declared order; `PARTITION BY <partition_by>`; `ORDER BY (<partition_by>, _hg_row_id)` (v1: the declared schema has no order key; the design's `ORDER BY (…, _hg_row_id)` suffix rule is honoured with the partition column as prefix). If the declared schema ever carries an explicit order key, it goes before `_hg_row_id`.

**D4 — Drift is fail-closed.** If a table exists with a different engine, missing/extra columns, or any pinned setting differing, the role refuses to start (verifier and SNode) with a message naming table + field. There is no auto-`ALTER` in v1 (an `ALTER` on `hg_unsafe` would be an unverified schema transition).

**D5 — Throttle location and signal.** The soft limit is enforced by the ingress HouseGate (§12.3) *and* mirrored by SNode's `PrepareLocalStatement` as a defence in depth; the source of truth is the co-located ClickHouse's `system.parts` for `hg_unsafe.<t>` grouped by `partition_id`, polled on an interval (default 2s) with a cheap invalidation on every ACK2/cleanup event the process observes. Soft limit default `= 0.8 × parts_to_throw_insert` (2400) per partition; hard-stop `= parts_to_throw_insert - 50` (a prepare that would land above it is refused before touching ClickHouse). Rejection surfaces as a ClickHouse `Exception` with code `252` (`TOO_MANY_PARTS`) and message prefix `storage_integrity: back-pressure` so existing client retry logic (which already treats 252 as retryable) works unchanged; it is also a distinct `OutcomeCategory` in the intake (`Retryable`, never journaled as a source write). Rejected: a new custom error code (clients would not retry).

**D6 — Cleanup is the drain.** With merges stopped, the only thing that lowers the part count is post-promotion `hg_unsafe` cleanup (already implemented). The throttle therefore also exposes `storage_integrity_unsafe_parts{table,partition}` and `storage_integrity_backpressure_total{table}` metrics; an operator alert on the former is the operational half of §12.3 consequence 4.

## 4. `EnsureProtocolTables` (arbiter-core, new package `snode/schema` or `dataplane/ddl`)

```go
type Pinned struct { UnsafeDB, SafeDB, PromoteDB, NodeID string; KeeperShardID uint32 }
func BuildDDL(p Pinned, t payloadexec.TableSchema) (unsafeDDL, safeDDL string)          // pure, golden-tested
func EnsureProtocolTables(ctx, conn, p Pinned, tables []payloadexec.TableSchema, mode Mode) error
// mode: CreateAndVerify (network_state/chain schema source) | VerifyOnly (clickhouse schema source)
```

`EnsureProtocolTables` runs `CREATE DATABASE IF NOT EXISTS` for the three DBs, then per table: `CREATE TABLE IF NOT EXISTS` (unsafe, safe); then `VerifyProtocolTable` reads `system.tables` (engine, `engine_full`, `sorting_key`, `partition_key`), `system.columns` (name/type/position), and `system.merge_tree_settings`-resolved per-table settings (`SELECT name, value FROM system.merge_tree_settings` is global; per-table overrides are parsed from `engine_full` — pin the parser with tests) and compares against `BuildDDL`'s intent. Any diff → `ErrProtocolTableDrift` naming the field. After verification the existing `MergeGuard`/`SYSTEM STOP MERGES` still runs (belt and braces; the DDL pin makes the window between restart and re-assert safe).

Callers: `snode.Role.Register` and `verifier.Role.Register` (arbiter-core) call it before registering with the Arbiter; a periodic reconcile (default 60s) re-runs it against the same `Config.Tables` snapshot to detect post-startup deletion or drift. sentio-node's `standalone.go` passes `NodeID` = its indexer node id and the schema-source-derived mode. Reference binaries `cmd/arbiter-snode` / `cmd/arbiter-verifier` gain `--ensure-tables=verify|create` mirroring the schema source. A later declaration becomes part of protocol DDL only after a controlled role restart loads the new coherent schema snapshot; no reconcile cycle mixes snapshots.

`snode/config.go`'s `validatePartitionBy` (bare `String` column) stays and is enforced by `BuildDDL` too, with an error that says exactly what the freeze is; a declared table that violates it is skipped with a warning at ensure time (so one bad declaration does not stop the node) and is refused at SI admission (ingress) with a clear message.

## 5. Ingress back-pressure (housegate `pkg/storageintegrity`)

New `PartsPressureGuard` over the existing `MergeConn`/`MergeRows` port (unit-testable with the fake): `Snapshot(ctx) (map[TablePartition]int, error)` runs `SELECT table, partition_id, count() FROM system.parts WHERE database = 'hg_unsafe' AND active GROUP BY table, partition_id`; `Allow(table, partitionID) (bool, reason)` compares against `soft_limit`/`hard_limit`. The partition id for an incoming INSERT is derived **before** prepare from the decoded payload's partition column values (the Native decode already yields rows; group by the `partition_by` column — the P1c freeze makes this a direct string value); a payload spanning several partitions is checked per partition and refused if any is over the limit. `StorageIntegrityIngress.ConsumeStorageIntegrityAdmission` consults it after the merge-health latch and before the payload put. Config: `storage_integrity.runtime.backpressure.{enabled(true), poll_interval(2s), soft_parts_per_partition(2400), hard_parts_per_partition(2950)}`.

SNode mirror (arbiter-core `snode/staged.go` `PrepareLocalStatement`): same query, hard limit only, error class `ErrBackpressure` mapped by housegate to `Retryable`.

## 6. Interim `hg_safe` policy and metrics

`hg_safe` keeps `SYSTEM STOP MERGES` + the pinned `max_bytes_to_merge_at_max_space_in_pool = 0`. Because every promotion `REPLACE PARTITION` publishes a whole partition built from base + candidates, the part count in `hg_safe` grows by the number of candidate parts per promotion and never shrinks in v1. Add `storage_integrity_safe_parts{table,partition}` (same poller) so P4 controlled compaction has a trigger signal; document in the operator runbook that a partition approaching `parts_to_throw_insert` on `hg_safe` is a P4 prerequisite, not an incident to fix by enabling merges (`allow_native_background_merges` stays rejected).

## 7. Testing / acceptance

- arbiter-core: `BuildDDL` golden tests (unsafe/safe DDL text for a two-column String-partitioned schema); docker `EnsureProtocolTables` create → verify → tamper (change a setting via `ALTER TABLE MODIFY SETTING`) → `ErrProtocolTableDrift`; verify-only mode never creates; reconcile detects drift against the frozen startup table set; RMT table created on two CH nodes with the same zk path replicates a part.
- housegate: `PartsPressureGuard` fake-conn tests (below/at/above soft, hard; multi-partition payload); ingress refuses with `Exception 252` and does not touch payload store / journal; metrics registered once (init() global pattern).
- sentio-node: SI smoke extended — declared table on chain → node starts → `hg_unsafe`/`hg_safe` exist with pinned settings; drift → startup failure.
- Spec B: base design §6 naming/DDL example replaced with `BuildDDL` output.

## 8. Delivery

1. arbiter-core: `BuildDDL` + `EnsureProtocolTables` + verify + tests → tag.
2. arbiter-core: wire into `snode.NewRole` / `verifier.NewRole` + reference binaries; SNode prepare hard-limit mirror.
3. housegate: `PartsPressureGuard` + config + ingress wiring + metrics.
4. sentio-node: pass NodeID/schema source; smoke.
5. Docs (Spec B).
