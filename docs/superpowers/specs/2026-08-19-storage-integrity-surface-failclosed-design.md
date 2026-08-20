# Storage-Integrity Surface: Fail-Closed Hardening

**Date:** 2026-08-19 **Status:** Proposed **Roadmap:** [remediation roadmap](2026-08-19-storage-integrity-remediation-roadmap.md) Spec I. **Remediates:** [Spec G read surface](2026-08-18-storage-integrity-read-surface-rewrite-design.md) (Implemented) — findings from the 2026-08-19 review, reproduced against the live native engine and ClickHouse 25.8. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §5.1, §6, §11, §12.2; [peer-trust design](2026-04-28-peer-trust-design.md). **Code base:** rewriter-go `dbac7bc` (v0.7.1), rewriter-grpc `ddc24b9` (v0.12.1), rewriter-proto `1879d30` (v0.2.0), housegate `621eaab` (v0.9.3). **Source of truth:** English version.

## 1. Problem

Spec G reserved the SI physical namespaces thoroughly — 178 corpus cases, ~147 of them adversarial rejects covering direct refs, table functions, engines, dictionary sources, DB-scope GRANT, CTAS bodies. Two statement classes escape, and both escape through the same structural gap: **the fail-closed decision is keyed on the response carrying an SI-flagged accessed table, but several paths produce no accessed table at all.**

**1a — unmodelled statement classes pass through as `Success`.** `SYSTEM`, `CHECK TABLE` and friends are not handled by any handler in either engine. `doRewrite` falls to its catch-all (`rewriter-go/native.go:188-195`), regenerates the AST, and returns `Code = Success` with the SQL unchanged and `original_accessed_tables` empty. HouseGate's `case Success` (`housegate/pkg/rewriter/sentio.go:336-348`) forwards it. Reproduced: `SYSTEM START MERGES hg_unsafe.db1__t`, `SYSTEM RESTART REPLICA hg_safe.db1__t`, `SYSTEM SYNC REPLICA hg_unsafe.db1__t`, `CHECK TABLE hg_safe.db1__t` — all `Success`, all `accessed=NONE`.

Consequence: any authenticated user can restart merges on `hg_unsafe`. Merges being stopped is what makes the candidate-part boundary equal the statement boundary (base design §12.2). A merged part carries a new name that is not in `excluded_unsafe_parts`, so `unsafe_latest` silently returns already-promoted rows a second time; the frozen active-part inventory that promotion's closure check assumes is gone. The `merge_guard` re-assert (default 30s) narrows the window and exists only when `storage_integrity.runtime.enabled`, which requires the ingress — a read-only SI deployment has no guard at all.

**1b — engine rejections that precede target recording are forwarded verbatim.** `sentio.go:349-366` returns `RewriteResult{SQL: sql}` with a **nil error** for `UnsupportedStatement`; the Spec G D-2 fail-closed gate at `sentio.go:327-335` only fires when some `original_accessed_tables` entry has `is_storage_integrity`. Engine paths that reject before `preflightStorageIntegrityWrite` records a write target produce neither. Reproduced: `TRUNCATE DATABASE hg_safe`, `TRUNCATE ALL TABLES FROM hg_safe`, `ALTER DATABASE hg_safe MODIFY COMMENT 'x'`, `DROP DICTIONARY hg_safe.db1__t`, `CREATE LIVE VIEW other.v AS SELECT * FROM db1.t` — all `UnsupportedStatement`, `accessed=NONE`, SQL echoed and forwarded.

Consequence: `TRUNCATE DATABASE hg_safe` reaches ClickHouse and empties the authoritative committed state.

Four further defects and one undocumented trust boundary are folded in here because they live in the same files:

- `escapeSQLLiteral` doubles `'` but does not escape `\` (`rewriter-go/internal/handlers/dblevel.go:353`; C++ `src/handlers/common.h:42-50`, whose own comment admits it). ClickHouse honours `\'` inside single-quoted literals, so a value ending in a backslash escapes the closing quote. Used to build the `_part NOT IN (…)` exclusion list. Not reachable today — part names come from `system.parts.name` — but it is the only place caller data becomes executable SQL in the SI surface, and the corpus has zero coverage (every case uses `all_1_1_0`-style names).
- The reserved-column check is scoped per *statement*, not per *table*: `SELECT a FROM db1.t WHERE a IN (SELECT _hg_row_id FROM other.u)` is rejected even though `other.u` is an ordinary table that may legitimately own that column.
- The SI namespace gate is not CTE-aware: `WITH t AS (SELECT 1 AS id) SELECT a FROM other.u WHERE id IN t` under `USE db1` with SI key `db1.t` is rejected because the gate does not carry CTE scope, while `CollectSelectTables`/`RewriteSelectTables` do. The corpus contains **zero** `WITH <name> AS (…)` cases despite Spec G §4.1 listing `WITH` as in scope.
- `PREWHERE` is not in the pre-reject set alongside `FINAL`/`SAMPLE`/`WITH OFFSET`, so it degrades into a raw ClickHouse error whose text quotes `hg_safe.db1__t` and `_hg_row_id` back to the client — handing out the names Spec G D-9 declares protocol-owned and unaddressable.
- C++ only: `restoreStorageIntegrityProjectionSyntax` (`src/handlers/storage_integrity.cc:768-811`) textually replaces `EXCEPT <rid>` → `EXCEPT (<rid>)` across the whole formatted statement with no literal awareness, so `SELECT 'EXCEPT _hg_row_id' AS s FROM db1.t` has its string literal mutated. Go is clean on the same input — a real divergence the shared corpus cannot see.
- Rewrite is skipped entirely for peer-trusted, forwarded, maintenance and platform-operator sessions (`pkg/plugins/rewrite/rewriter.go:298`, `:303`, `:130-133`); with no accessed tables the SI ingress also short-circuits (`pkg/plugins/storageintegrity/plugin.go:162`). Those sessions can address `hg_safe`/`hg_unsafe` directly. This is the pre-existing peer-trust posture, but Spec G raised what it protects and neither the spec nor the plan recorded it.

## 2. Goals / non-goals

Goals: (1) no statement reaches ClickHouse unexamined when the request carries a non-empty SI table set; (2) the two Critical paths are closed by an enumerated reject with a useful message *and* by a catch-all so the next unmodelled class is safe by default; (3) the exclusion-list literal is escaped correctly in both engines; (4) the four correctness defects are fixed with corpus coverage; (5) the peer-trust bypass becomes a recorded, tested decision with an operator-visible warning.

Non-goals: making peer/forwarded sessions run SI policy (D6); a general `ErrorReverseMap` (only the SI physical-name scrub in D7e); RFC-8785 canonicalization; contract renegotiation beyond the D5 startup assertion; bounding the exclusion list (Spec L owns the read cost).

## 3. Decisions

**D1 — The engines fail closed on the catch-all when SI is active.** `doRewrite` already computes `siVersion == STORAGE_INTEGRITY_CONTRACT_V1` whenever the request carries a non-empty `storage_integrity.tables` (`rewriter-go/native.go:97-109`). When execution reaches the pass-through branch with `siVersion == V1`, return `UnsupportedStatement` with `storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded` instead of `Success`. Same in `rewriter-grpc`'s dispatch tail. Rejected: enumerating every statement class — the enumeration is necessary for good messages (D2) but cannot be the safety property, because the next ClickHouse release adds a class nobody enumerated.

**D2 — Enumerate the known-dangerous classes for message quality and test anchoring.** `SYSTEM` (all forms), `CHECK TABLE`/`CHECK ALL TABLES`, `TRUNCATE DATABASE`/`TRUNCATE ALL TABLES FROM`, `ALTER DATABASE`, `DROP DICTIONARY`, `CREATE LIVE VIEW` get explicit rejects naming the SI table or reserved namespace they touch, using Spec G §4.4's message shape (`storage-integrity table <t> accepts writes only through the signed statement lane` / `... reserved namespace <db> is not addressable`). Where the statement names no SI object and SI is configured, D1's generic message applies.

**D3 — HouseGate treats any non-Success as a rejection when SI tables are configured.** In `sentio.go`, when `len(dynArgs.StorageIntegrity.Tables) > 0`, every non-`Success` response becomes `*RejectedError` regardless of the accessed-table flag. The SI-flag path stays for the message it produces. Ordinary (empty-SI) fail-open is unchanged and its existing test must keep passing. This is defence in depth: D1 makes the engines safe, D3 makes HouseGate safe even against an engine that regresses or an older engine build.

**D4 — `escapeSQLLiteral` escapes backslashes in both engines.** Escape `\` before `'`. This is a one-line change per engine that also hardens six other call sites. Add corpus cases with part names containing `'`, `\`, and `\'`.

**D5 — HouseGate asserts the engine build, not just the contract enum.** `STORAGE_INTEGRITY_CONTRACT_V1` cannot distinguish rewriter-go v0.7.0 (broken DESCRIBE) from v0.7.1, nor rewriter-grpc v0.12.0 from v0.12.1. At startup, when SI tables are configured, HouseGate issues one probe rewrite of a known SI DESCRIBE and requires the exact expected SQL; a mismatch refuses startup naming the engine and the expected build. Rejected: a new contract enum value — it would need a proto bump per patch release; rejected: version strings in the response — the native engine has no version RPC and adding one is a larger change than the probe.

**D6 — The four bypasses are two different problems and get two different treatments.** They were grouped in the review; the double-rewrite rationale only covers half of them.

*Peer-trusted and forwarded sessions* carry SQL the originating proxy already rewrote. Running SI rewrite again would double-rewrite it, which the peer-trust design forbids, so the bypass stays in v1. Controls: (a) a decision record here and in the base design (Spec B's edit list); (b) a HouseGate startup warning when SI tables are configured *and* `internal_listen` is set, naming the requirement that the internal port be reachable only from peer subnets; (c) a test asserting the bypass is deliberate (a peer-trusted session's SI-table SELECT is not rewritten), so changing it later is a visible test change.

*Maintenance and platform-operator sessions* have no such excuse. Their SQL is raw and un-rewritten, and the bypass exists only because those roles are trusted operators — which means an operator credential, or anything that can set those flags, can address `hg_safe`/`hg_unsafe` directly and read `_hg_row_id`. That is a distinct threat model and needs its own answer in this spec rather than inheriting the peer one:
- Reserved-namespace addressing is refused for these sessions too. The rewrite bypass stays (they legitimately need un-rewritten SQL), but a narrow pre-check rejects any statement that names a reserved SI physical database or the reserved column. **It must not live inside `rewrite.Plugin`**: that plugin declares `RunOnForward() == false`, and `forward.Plugin` runs earlier and can set `IsForwarding`, so a guard placed there is skipped on exactly the forwarded sessions it needs to cover. It belongs in its own plugin that opts into both the forward and peer-trust filters and gates on the operator flags itself, leaving plain peer-trusted sessions untouched. Operators who need raw physical access use a direct ClickHouse connection, not the proxy — which is the existing posture for every other protocol-owned surface.
- A startup warning names the SI tables that are reachable this way, and the count of configured platform-operator addresses.
- Dedicated tests: a maintenance session and a platform-operator session are each refused on `SELECT … FROM hg_safe.<t>` and on `_hg_row_id`, are still allowed on an ordinary table, **and are still refused when the session is also forwarding** — the last case is what proves the guard survives the chain filter that would have skipped it.

Rejected: running full SI rewrite for maintenance sessions (it would defeat the purpose of the bypass); rejected: leaving them undocumented under the peer rationale (the review's finding, and correct).

**D7 — The four correctness defects.**
- (a) Scope the reserved-column check to references that resolve to an SI table; an identically named column on an ordinary table is allowed.
- (b) Make the SI namespace/`IN`-target gates CTE-aware, reusing the scope the collect/rewrite walkers already carry.
- (c) Add `PREWHERE` to the pre-reject set with the same clean message as `FINAL`/`SAMPLE`.
- (d) C++: make `restoreStorageIntegrityProjectionSyntax` literal-aware (or emit the parenthesised form directly and delete the textual fixup).
- (e) Scrub SI physical names and the reserved column from Exception text before it reaches the client, for SI-classified statements only. This is a narrow, SI-scoped mapping in `pkg/plugins/rewrite`'s `OnException`, not a revival of the general `ErrorReverseMap` stub.

## 4. Corpus

`storage_integrity_cases.json` grows and stays byte-identical in both repos. New cases, all with `want_sql` (not only `want_sql_contains`):

| Group | Cases |
|---|---|
| D1 catch-all | `SYSTEM START MERGES` / `STOP MERGES` / `RESTART REPLICA` / `SYNC REPLICA` on both namespaces and on a configured logical table; `CHECK TABLE`; one deliberately exotic unmodelled statement asserting the generic message |
| D2 enumerated | `TRUNCATE DATABASE hg_safe`, `TRUNCATE ALL TABLES FROM hg_unsafe`, `ALTER DATABASE hg_safe MODIFY COMMENT`, `DROP DICTIONARY hg_safe.x`, `CREATE LIVE VIEW … AS SELECT FROM <si>` |
| D4 escaping | exclusion part names containing `'`, `\`, `\'`, and a name that would close the literal |
| D7a | `SELECT a FROM <si> WHERE a IN (SELECT _hg_row_id FROM other.u)` → Success, `other.u` untouched |
| D7b | CTE alias shadowing an SI logical name in `FROM` and in `IN`; an SI table inside a CTE body (the working case, to lock it) |
| D7c | `PREWHERE` on an SI table → clean reject |
| D7d | `SELECT 'EXCEPT _hg_row_id' AS s FROM <si>` → literal preserved verbatim |

Spec J makes the C++ runner actually compare `want_sql`; without J these cases are documentation. **I's corpus additions and J's runner change must land together or J first** — otherwise the C++ half of I is unverified.

## 5. Testing / acceptance

- Both engines: the new corpus cases pass; the Go oracle diff (`REWRITER_ORACLE_ADDR`) is green across the whole corpus.
- HouseGate unit: `TestRewriter_FailOpen` (empty SI) still passes; new `TestRewriter_FailsClosedOnAnyNonSuccessWhenSIConfigured` covers `UnsupportedStatement`/`SyntaxError`/`RewriteError` with and without an SI-flagged accessed table; D5 probe refuses startup against a stubbed old-build response; D6 asserts the peer-trust bypass.
- HouseGate docker (`pkg/integration/storage_integrity_read_test.go`, native engine, already in the CI list): `SYSTEM START MERGES hg_unsafe.<phys>` and `TRUNCATE DATABASE hg_safe` both return an Exception and **the tables are provably untouched afterwards** — assert `system.parts` count and a `SELECT count()` on `hg_safe` before and after. This is the regression test for the two Critical findings and must fail on the pre-fix build.
- A test that a peer-trusted session reaching the internal port is *not* SI-rewritten (D6, peer branch); and that maintenance / platform-operator sessions are refused on reserved-namespace and reserved-column addressing while ordinary tables still work (D6, operator branch).

## 6. Delivery

1. rewriter-go: D1 catch-all, D2 enumeration, D4 escaping, D7a–c, corpus additions → tag.
2. rewriter-grpc: the same, plus D7d → tag. (Depends on Spec J's runner change to be meaningful.)
3. housegate: D3, D5 probe, D6 warning + tests, D7e scrub, engine pins, docker regression test → tag.
4. Spec B edit list gains the D6 decision record.
