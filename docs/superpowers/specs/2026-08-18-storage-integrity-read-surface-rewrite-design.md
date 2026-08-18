# Storage-Integrity Read Surface Rewrite (safe / unsafe_latest, reserved-column hiding)

**Date:** 2026-08-18 **Status:** Proposed **Roadmap:** [v1 closure roadmap](2026-08-18-storage-integrity-v1-closure-roadmap.md) Spec G. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §5.1, §6, §7 (Phase-2), §11, §12.2 (unsafe cleanup / `_part` filter); [designs PROGRESS.md 2026-07-08](https://github.com/sentioxyz/designs/blob/main/sento-network/PROGRESS.md) (read-semantics note); [2026-08-18 Spec C](2026-08-18-storage-integrity-physical-table-lifecycle-design.md) (naming). **Code base:** rewriter-proto `v0.1.0`, rewriter-go `v0.6.0`, rewriter-grpc `1656832`, housegate `c6f7a6d` (`pkg/rewriter/sentio.go`, `pkg/plugins/rewrite`), sentio-node `9f12620`. **Source of truth:** English version.

## 1. Problem

With the SI ingress enabled, an INSERT into an SI table is written only to `hg_unsafe.<t>` (the ordinary upstream write is suppressed) and later promoted into `hg_safe.<t>`. Nothing rewrites the user's reads: a `SELECT ... FROM db.t` still resolves to the multi-tenant physical table `physical.<db.t>`, which receives no rows. The rewriter contract has no notion of a safe/unsafe surface, of `_hg_row_id`, or of read modes; base design §11's three read modes and §6's "hide reserved columns / reject writes to them" are unimplemented anywhere. Until this lands, an SI deployment is write-only from the user's point of view.

## 2. Goals / non-goals

Goals: (1) reads of SI tables resolve to `hg_safe.<t>` (mode `safe`) or to `hg_safe ∪ hg_unsafe` minus already-promoted unsafe parts (mode `unsafe_latest`); (2) `_hg_row_id` never appears on the logical surface and any user reference to it is rejected; (3) non-SI-lane writes and DDL against SI tables are rejected at HouseGate; (4) both rewriter engines implement the same behaviour under one contract; (5) mode selection is per query (setting) with a configured default.

Non-goals: `as_of_safe(block)` time travel (P4; the manifest-indexed read exists on the Arbiter API, no SQL surface); cross-table joins mixing SI and non-SI tables in `unsafe_latest` (allowed but not optimized); changing the existing physical rewrite of SI INSERT Query packets. The rewriter must keep returning Success with a resolvable rewritten target and `is_storage_integrity = true`; HouseGate's signed lane owns admission and suppresses the row execution after the upstream sample-block exchange, while the SNode writes `hg_unsafe` directly.

## 3. Decisions

**D1 — Default read mode is a config policy, shipped default `safe`.** Base design §11 says default `safe`; the 07-08 PROGRESS note records the later product preference for `safe ∪ unsafe` by default because safe-only freshness is unacceptable for the indexing product. Both are supported; `storage_integrity.read.default_mode: safe|unsafe_latest` (default `safe`) decides, and a query may override with the setting `SQL_x_read_mode = 'safe' | 'unsafe_latest'`. Spec F's devnet2 config sets `unsafe_latest` for dogfooding. **Reviewer decision point:** flip the shipped default if the product call stands.

**D2 — `unsafe_latest` dedup uses a part-name exclusion list, not row-level distinct.** §12.2: promotion copies into `hg_safe` and schedules `hg_unsafe` cleanup; until cleanup, promoted rows exist in both. The rewrite excludes them with `WHERE _part NOT IN (<promoted-not-yet-cleaned unsafe part names>)`, the list supplied by the co-located SNode's local promotion journal. Rejected: `UNION DISTINCT` / `_hg_row_id NOT IN (SELECT …)` — correct but O(safe partition) per read. If the exclusion port is unavailable (no co-located SNode), `unsafe_latest` is refused with an error, not silently degraded to duplicates.

**D3 — Reserved-column hiding by projection subquery.** Every rewritten SI table reference becomes a derived table `(SELECT * EXCEPT (_hg_row_id) FROM hg_safe.<t>)` (or the union form). This hides the column from `SELECT *`, `DESCRIBE` of the derived surface is handled separately (§4.3), and any explicit `_hg_row_id` in the user's SQL is a `RewriteError`. Rejected: relying on clients to ignore the column.

**D4 — Membership is explicit.** The SI table set is `storage_integrity.tables[]` (renamed from `runtime.merge_guard.tables`, used by the merge guard, back-pressure, ingress, and the read rewrite alike). Being declared on chain is not sufficient (Phase B declares every user table).

**D5 — Configured SI membership makes rewrite classification safety-critical.** A successful rewriter response can identify an SI access with `is_storage_integrity`; a transport/protocol error has no accessed-table result, so HouseGate cannot safely infer that the original SQL is non-SI. Whenever `storage_integrity.tables[]` is non-empty, every runtime failure before a trustworthy rewriter response is therefore a fail-closed `RejectedError` for the whole query, including with `storage_integrity.ingress.enabled: false`; inability to construct/wire the rewriter refuses server startup. Ordinary fail-open startup/runtime behavior remains only when the configured SI set is empty. Rejected: a HouseGate SQL pre-parser or regex membership gate — the proxy's architecture deliberately delegates SQL parsing to the rewriter, and a partial parser is not a trustworthy security boundary. This trades availability for the invariant that an SI write/read can never escape onto the logical table during a rewriter outage.

**D6 — A positive contract acknowledgement, not `RewriteCode=Success`, makes a response trustworthy.** Protobuf additions are wire-compatible, so an older backend can ignore `StorageIntegrityArgs`, return `Success`, and leave every SI-specific response field at its zero value. HouseGate therefore sends `STORAGE_INTEGRITY_CONTRACT_V1` whenever the SI table set is non-empty and accepts no response — successful or rejected — unless it echoes that exact version in `RewriteSQLResponse.storage_integrity_contract_version`. Both engines first resolve the effective table-rewrite option using the existing contract (last `TableNameRewrite` carrying static or dynamic args wins; within one option, static wins). Only an effective dynamic option with non-empty SI tables is an SI request: its version is validated before parsing/dispatch and the acknowledgement is stamped on every response path after acceptance. An earlier shadowed SI option must neither trigger nor acknowledge SI; opposite option order makes it active. This activation rule is intentionally about the table-policy submessage: DB-level handlers (`USE`, SHOW DB/TABLES, DB DDL) may independently read the last dynamic option's `database_map` even behind a later static option, but they never consume `storage_integrity`, so that unrelated mapping does not acknowledge a shadowed SI block. A missing/unknown version on the effective SI option returns `InvalidRewriteRequest` without an acknowledgement, while an effective non-SI/static option keeps both fields unspecified. The same proof crosses HouseGate's in-process/custom seams: `RewriteResult` carries the echoed version, and an injected `rewriter.Factory` used with SI configuration must implement `StorageIntegrityCapableFactory` and advertise v1 at startup. The built-in Sentio factory advertises v1 because its backend wrapper enforces the wire echo. A custom factory that advertises v1 but omits the per-query result echo is still rejected by the rewrite plugin. This fails closed against old gRPC servers, old native libraries, no-op test/custom factories, and accidental field loss at an adapter boundary while leaving non-SI deployments backward compatible.

## 4. Rewriter contract (rewriter-proto, minor bump)

```proto
enum StorageIntegrityContractVersion {
  STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED = 0;
  STORAGE_INTEGRITY_CONTRACT_V1 = 1;
}

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
  StorageIntegrityContractVersion contract_version = 4;
}
// RewriteTableDynamicArgs gains: StorageIntegrityArgs storage_integrity = 12;
// RewriteSQLResponse gains:
//   StorageIntegrityContractVersion storage_integrity_contract_version = 16;
// Tag 15 remains owned by Spec E's insert_class.
```

When the effective last-wins table-rewrite selection is dynamic and its `storage_integrity.tables` is non-empty, `contract_version` must be v1. An engine that recognizes that exact request stamps response v1 before any parse/dispatch path, so syntax errors and semantic rejections are acknowledged too. A missing or unsupported request version is `InvalidRewriteRequest` with response version unspecified. When the effective selection has no SI tables (including a later static option shadowing an earlier SI option), engines neither require nor emit the acknowledgement; DB-level use of that earlier option's `database_map` does not change this because no DB-level handler consumes the SI submessage.

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

`ALTER`, `DROP`, `TRUNCATE`, `RENAME`, `EXCHANGE`, `OPTIMIZE`, `ATTACH`, `DETACH`, `CREATE … AS SELECT`, `GRANT/REVOKE` on the table → the rewriter returns `RewriteCode = UnsupportedStatement` with message `storage-integrity table <t> accepts writes only through the signed statement lane`. `INSERT` is the deliberate exception: the rewrite plugin runs before SI ingress, so both engines return Success, preserve the ordinary physical INSERT target needed for upstream sample-block negotiation, and mark the logical access `is_storage_integrity = true`. HouseGate then admits only the signed SI lane and rejects a non-lane INSERT fail-closed with the same message (including when the ingress lane is disabled). This ownership remains after Spec A: agent-side sample synthesis does not make the server-side ingress/rewrite contract optional.

## 5. HouseGate wiring

- `pkg/rewriter/sentio.go` `buildDynamicArgs`: fill membership from `storage_integrity.tables[]`, including `contract_version = STORAGE_INTEGRITY_CONTRACT_V1`; use a new `Options.StorageIntegrityReadState` port `{ PromotedUnsafeParts(tableID) ([]string, error) }` only for the per-query exclusion list, plus the per-query mode (setting `SQL_x_read_mode`, else config default). The port is provided by sentio-node from the embedded SNode role (`snode.Role.PromotedUnsafeParts(tableID)` — new method reading its local promotion journal: parts recorded in a promotion ack whose cleanup ack has not been journaled). Reference binaries and hosts without an SNode leave it nil → `unsafe_latest` refused (D2). The wrapper requires a v1 response acknowledgement before trusting any code or classification and copies it into `RewriteResult` (D6).
- `pkg/plugins/rewrite`: no new setting-mutation logic. Read `SQL_x_read_mode` but leave it in the forwarded settings, matching existing `SQL_x_*` custom settings; the SI response wrapper fails closed on any non-Success response carrying an SI accessed-table flag and rejects SI INSERT when the signed ingress lane is not enabled. When SI membership is configured, missing/untrustworthy runtime responses (backend transport/protocol error, nil response, closed backend, or a missing/wrong v1 acknowledgement) also become `RejectedError`, and a nil or non-`StorageIntegrityCapableFactory` startup factory refuses `buildServer`; only an empty SI set keeps the legacy fail-open path (D5/D6).
- Config: `storage_integrity.tables[]` (shared), `storage_integrity.read.{default_mode}`; `Validate()`: `read` requires `tables` non-empty; server-mode only.

## 6. Testing / acceptance

- rewriter shared golden cases (`storage_integrity_cases.json`, consumed by rewriter-go harness and rewriter-grpc tests) request v1 and assert a v1 echo for every SI case: SAFE plain select; SAFE with alias + JOIN to a non-SI table; UNSAFE_LATEST with 0/2 excluded parts; `SELECT _hg_row_id` → reject; `SELECT * ` hides rid; DESCRIBE metadata SELECT; EXISTS; INSERT → Success with ordinary physical rewrite + `is_storage_integrity`; ALTER/DROP → UnsupportedStatement; SI table under `USE db` default-database resolution; a non-SI table in the same statement still goes through `database_map`. Both engines also unit-test missing/unknown versions → unacknowledged `InvalidRewriteRequest`, v1 syntax errors → acknowledged, no/empty SI blocks → unchanged unacknowledged behavior, and both `[SI dynamic, static]` / `[static, SI dynamic]` orders so acknowledgement follows the effective last-wins option only.
- housegate: `buildDynamicArgs` fills args (including request contract v1) and disables the old no-mappings fast path whenever SI args exist; setting parsing (invalid mode → Exception); nil port + unsafe_latest → Exception; a rewriter-classified SI INSERT is rejected when the signed ingress lane is disabled and reaches ingress when it is enabled; with a non-empty SI table set, a nil/non-capable startup factory refuses startup and backend/nil-response/closed-rewriter/missing-or-wrong-ack errors fail closed (including separate zero/missing and unknown-nonzero acknowledgement regressions for both the built-in wrapper and a custom rewriter, plus a successful old backend and a no-op injected factory), while the same failures retain legacy behavior with no SI tables; docker `pkg/integration`: SI INSERT via staged path then `SELECT count()` returns rows in `unsafe_latest` immediately and in `safe` only after a fake promotion moves the partition (reuse `storage_promotion_mvp_test.go` fixtures).
- sentio-node: `PromotedUnsafeParts` unit test over the promotion journal (promoted → listed; cleanup acked → dropped).

## 7. Delivery

1. rewriter-proto: `StorageIntegrityArgs` + `AccessedTable.is_storage_integrity` + request/response contract-version acknowledgement → tag.
2. rewriter-go + rewriter-grpc: engine implementation + shared goldens (one PR each; the C++ one can trail, housegate's default engine is still gRPC — see Spec E for the engine parity oracle).
3. housegate: port, config, dynamic args, tests.
4. sentio-node: `PromotedUnsafeParts` + wiring.
