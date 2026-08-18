# Storage-Integrity Read Surface Rewrite (safe / unsafe_latest, reserved-column hiding)

**Date:** 2026-08-18 **Status:** Proposed **Roadmap:** [v1 closure roadmap](2026-08-18-storage-integrity-v1-closure-roadmap.md) Spec G. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §5.1, §6, §7 (Phase-2), §11, §12.2 (unsafe cleanup / `_part` filter); [designs PROGRESS.md 2026-07-08](https://github.com/sentioxyz/designs/blob/main/sento-network/PROGRESS.md) (read-semantics note); [2026-08-18 Spec C](2026-08-18-storage-integrity-physical-table-lifecycle-design.md) (naming). **Code base:** rewriter-proto `v0.1.0`, rewriter-go `v0.6.0`, rewriter-grpc `1656832`, housegate `c6f7a6d` (`pkg/rewriter/sentio.go`, `pkg/plugins/rewrite`), sentio-node `9f12620`. **Source of truth:** English version.

## 1. Problem

With the SI ingress enabled, an INSERT into an SI table is written only to `hg_unsafe.<t>` (the ordinary upstream write is suppressed) and later promoted into `hg_safe.<t>`. Nothing rewrites the user's reads: a `SELECT ... FROM db.t` still resolves to the multi-tenant physical table `physical.<db.t>`, which receives no rows. The rewriter contract has no notion of a safe/unsafe surface, of `_hg_row_id`, or of read modes; base design §11's three read modes and §6's "hide reserved columns / reject writes to them" are unimplemented anywhere. Until this lands, an SI deployment is write-only from the user's point of view.

## 2. Goals / non-goals

Goals: (1) reads of SI tables resolve to `hg_safe.<t>` (mode `safe`) or to `hg_safe ∪ hg_unsafe` minus already-promoted unsafe parts (mode `unsafe_latest`); (2) `_hg_row_id` never appears on the logical surface and any user reference to it is rejected; (3) non-SI-lane writes and DDL against SI tables are rejected at HouseGate; (4) both rewriter engines implement the same behaviour under one contract; (5) mode selection is per query (setting) with a configured default.

Non-goals: `as_of_safe(block)` time travel (P4; the manifest-indexed read exists on the Arbiter API, no SQL surface); cross-table joins mixing SI and non-SI tables in `unsafe_latest` (allowed but not optimized); Phase-2 rewrite of *writes* (the SI lane's INSERT never reaches ClickHouse through the rewriter — the SNode writes `hg_unsafe` directly — so no write rewrite is needed in v1).

## 3. Decisions

**D1 — Default read mode is a config policy, shipped default `safe`.** Base design §11 says default `safe`; the 07-08 PROGRESS note records the later product preference for `safe ∪ unsafe` by default because safe-only freshness is unacceptable for the indexing product. Both are supported; `storage_integrity.read.default_mode: safe|unsafe_latest` (default `safe`) decides, and a query may override with the setting `SQL_x_read_mode = 'safe' | 'unsafe_latest'`. Spec F's devnet2 config sets `unsafe_latest` for dogfooding. **Reviewer decision point:** flip the shipped default if the product call stands.

**D2 — `unsafe_latest` dedup uses a part-name exclusion list, not row-level distinct.** §12.2: promotion copies into `hg_safe` and schedules `hg_unsafe` cleanup; until cleanup, promoted rows exist in both. The rewrite excludes them with `WHERE _part NOT IN (<promoted-not-yet-cleaned unsafe part names>)`, the list supplied by the co-located SNode's local promotion journal. Rejected: `UNION DISTINCT` / `_hg_row_id NOT IN (SELECT …)` — correct but O(safe partition) per read. If the exclusion port is unavailable (no co-located SNode), `unsafe_latest` is refused with an error, not silently degraded to duplicates.

**D3 — Reserved-column hiding by projection subquery.** Every rewritten SI table reference becomes a derived table `(SELECT * EXCEPT (_hg_row_id) FROM hg_safe.<t>)` (or the union form). This hides the column from `SELECT *`, `DESCRIBE` of the derived surface is handled separately (§4.3), and any explicit `_hg_row_id` in the user's SQL is a `RewriteError`. Rejected: relying on clients to ignore the column.

**D4 — Membership is explicit.** The SI table set is `storage_integrity.tables[]` (renamed from `runtime.merge_guard.tables`, used by the merge guard, back-pressure, ingress, and the read rewrite alike). Being declared on chain is not sufficient (Phase B declares every user table).

## 4. Rewriter contract (rewriter-proto, minor bump)

```proto
message StorageIntegrityArgs {
  enum ReadMode { READ_MODE_UNSPECIFIED = 0; READ_MODE_SAFE = 1; READ_MODE_UNSAFE_LATEST = 2; }
  message Table {
    string safe_table = 1;                       // "hg_safe.db__t"
    string unsafe_table = 2;                     // "hg_unsafe.db__t"
    repeated string excluded_unsafe_parts = 3;   // promoted, not yet cleaned
  }
  map<string, Table> tables = 1;   // key = logical "db.table" as the user may write it (after USE resolution by the caller)
  ReadMode read_mode = 2;
  string reserved_row_id_column = 3;             // "_hg_row_id"
}
// RewriteTableDynamicArgs gains: StorageIntegrityArgs storage_integrity = 12;
```

Semantics (both engines, golden-tested from one shared case file):

### 4.1 SELECT family (SELECT, WITH, UNION, subqueries, JOINs, `INSERT … SELECT` source side is out of scope per Spec E)

For every table reference resolving (with the caller-supplied default database) to a key in `tables`:

- mode SAFE → `(SELECT * EXCEPT (<rid>) FROM <safe_table>) AS <original alias or table name>`
- mode UNSAFE_LATEST → `(SELECT * EXCEPT (<rid>) FROM <safe_table> UNION ALL SELECT * EXCEPT (<rid>) FROM <unsafe_table> WHERE _part NOT IN (<parts>)) AS <alias>` (`WHERE` omitted when the list is empty)
- the reference is **not** passed through `database_map` / static maps (SI mapping wins; add before dynamic resolution in both `nameresolve.ApplyDynamic` and C++ `name_rewrite.cc`).
- any identifier equal to `reserved_row_id_column` anywhere in the statement (select list, WHERE, ORDER BY, function args) → `RewriteCode = RewriteError`, message `reserved column _hg_row_id is not addressable`.
- `original_accessed_tables` still reports the logical table (`is_storage_integrity = true` — new bool on `AccessedTable`) so HouseGate's auth/usage plugins keep working on logical names.

### 4.2 EXISTS TABLE / SHOW TABLES / SHOW DATABASES

`EXISTS TABLE db.t` → `EXISTS TABLE <safe_table>`. `SHOW TABLES` already synthesizes from `system.tables`; SI tables are listed under their logical names because they also exist as logical entries in the multi-tenant map today (unchanged); nothing to do beyond a test.

### 4.3 DESCRIBE / SHOW CREATE TABLE

`DESCRIBE db.t` → `SELECT name, type, default_type, default_expression, comment, codec_expression, ttl_expression FROM system.columns WHERE database = '<safe_db>' AND table = '<safe_t>' AND name != '<rid>' ORDER BY position` (metadata-shaped SELECT, same pattern as SHOW TABLES). `SHOW CREATE TABLE db.t` → `UnsupportedStatement` in v1 (the physical DDL exposes engine, zk path, and `_hg_row_id`; an operator/debug surface can be added later).

### 4.4 Everything else touching an SI table

`INSERT` (any form), `ALTER`, `DROP`, `TRUNCATE`, `RENAME`, `EXCHANGE`, `OPTIMIZE`, `ATTACH`, `DETACH`, `CREATE … AS SELECT`, `GRANT/REVOKE` on the table → the rewriter returns `RewriteCode = UnsupportedStatement` with message `storage-integrity table <t> accepts writes only through the signed statement lane`. HouseGate's SI ingress intercepts signed INSERTs **before** the rewrite plugin runs (it already sets `SuppressUpstreamExecution`), so a signed INSERT never reaches this branch; an unsigned INSERT does and is refused.

## 5. HouseGate wiring

- `pkg/rewriter/sentio.go` `buildDynamicArgs`: fill `storage_integrity` from a new `Options.StorageIntegrityReadState` port `{ Tables() []TableRef; ExcludedUnsafeParts(tableID) ([]string, error) }` and the per-query mode (setting `SQL_x_read_mode`, else config default). The port is provided by sentio-node from the embedded SNode role (`snode.Role.PromotedUnsafeParts(tableID)` — new method reading its local promotion journal: parts recorded in a promotion ack whose cleanup ack has not been journaled). Reference binaries and hosts without an SNode leave it nil → `unsafe_latest` refused (D2).
- `pkg/plugins/rewrite`: no new logic beyond passing the setting through; strip `SQL_x_read_mode` from the forwarded settings like the other `SQL_x_*` keys.
- Config: `storage_integrity.tables[]` (shared), `storage_integrity.read.{default_mode}`; `Validate()`: `read` requires `tables` non-empty; server-mode only.

## 6. Testing / acceptance

- rewriter shared golden cases (`storage_integrity_cases.json`, consumed by rewriter-go harness and rewriter-grpc tests): SAFE plain select; SAFE with alias + JOIN to a non-SI table; UNSAFE_LATEST with 0/2 excluded parts; `SELECT _hg_row_id` → reject; `SELECT * ` hides rid; DESCRIBE metadata SELECT; EXISTS; INSERT/ALTER/DROP → UnsupportedStatement; SI table under `USE db` default-database resolution; a non-SI table in the same statement still goes through `database_map`.
- housegate: `buildDynamicArgs` fills args; setting parsing (invalid mode → Exception); nil port + unsafe_latest → Exception; docker `pkg/integration`: SI INSERT via staged path then `SELECT count()` returns rows in `unsafe_latest` immediately and in `safe` only after a fake promotion moves the partition (reuse `storage_promotion_mvp_test.go` fixtures).
- sentio-node: `PromotedUnsafeParts` unit test over the promotion journal (promoted → listed; cleanup acked → dropped).

## 7. Delivery

1. rewriter-proto: `StorageIntegrityArgs` + `AccessedTable.is_storage_integrity` → tag.
2. rewriter-go + rewriter-grpc: engine implementation + shared goldens (one PR each; the C++ one can trail, housegate's default engine is still gRPC — see Spec E for the engine parity oracle).
3. housegate: port, config, dynamic args, tests.
4. sentio-node: `PromotedUnsafeParts` + wiring.
