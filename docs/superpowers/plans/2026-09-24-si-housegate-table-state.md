# Dynamic Storage-Integrity Table Set in HouseGate Implementation Plan (sub-project 3, housegate half)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** HouseGate decides every storage-integrity (SI) query from a host-supplied, versioned table-state snapshot instead of the static `storage_integrity.tables` list: Pending data access fails retryably, Refused fails non-retryably with the arbiter's reason, Active is rewritten and signed as today, Gone reads as an unknown table, a `DROP TABLE` of an SI table succeeds through rewriter contract V2, and the agent signs only Active tables and passes every other INSERT through unsigned — with no restart or config edit when the set changes, and with today's behaviour for every existing static config.

**Architecture:** A leaf package `pkg/sitable` defines the port (`Status`, `Table`, immutable `Snapshot`, `TableState`), the static implementation over `storage_integrity.tables`, and a switchable `Fake`. `storage_integrity.enabled` becomes an explicit switch (default `len(tables) > 0`), and `buildServer` requires exactly one table-set source (the static list or `Options.StorageIntegrityTableState`). The rewrite plugin takes exactly one snapshot per query into `QueryContext.TableSnapshot` and the rewriter context; the rewriter always sends contract-V2 arguments when enabled (even with no Active table), always names the reserved databases `hg_safe` / `hg_unsafe` / `hg_promote` in them (plan A's `reserved_databases`, so the engines protect them while no table is Active), fetches promoted parts only for the accessed Active tables (two passes), and a per-version scrubber cleans exceptions. A new `sitablestate` QueryPlugin, after rewrite and before the SI ingress and commitgate, applies the status matrix. The ingress binds the snapshot's registry schema; the runtime resolves recovery schemas through the table state, keeps the bijection check for the static set only, maps `SCHEMA_NOT_ALLOWED`, and supervises merge-guard health per table, woken by `Changed()`. The agent's `sistatement` asks a `registry.TableStatuses` source first (`RpcNetworkState` implements `sentio_getStorageIntegrityTableStatus`). The engines' V2 contract comes from plan A.

**Tech Stack:** Go (module `github.com/housegate/housegate`), Bazel 9.1.0 + Bzlmod + gazelle, rewriter-proto `$PROTO_PSEUDO` (`STORAGE_INTEGRITY_CONTRACT_V2`, `StorageIntegrityArgs.reserved_databases`), rewriter-go `$REWRITER_GO_TAG` (native FFI, polyglot `$POLYGLOT_VERSION`), rewriter-grpc `$REWRITER_GRPC_TAG`, sentioxyz clickhouse-go v2.47.0-sentioxyz-20260629 / ch-go v0.73.0-sentioxyz-20260629, arbiter-proto v0.6.0, ClickHouse server 25.8 and client 26.x (integration), testcontainers.

**Spec:** `docs/superpowers/specs/2026-09-24-dynamic-si-table-set-housegate-design.md` (binding, §1–§14) and §14 of `docs/superpowers/specs/2026-09-23-dynamic-si-table-set-design.md`; plan A `docs/superpowers/plans/2026-09-24-si-rewriter-contract-v2.md`, section "Handoff to plan B" (items 1–7), supplies the contract values. Out of scope (spec §3): the sentio-node `TableState`, its JSON-RPC server and MergeGuard adapter (sub-project 4, see the handoff at the end), `ALTER` of SI tables, multiple SI indexers.

## Global Constraints

- **Base:** `origin/main` at `872c91b` or later; one feature branch `feat/si-table-state`; never push to `main`.
- **Plan A outputs (the controller fills them from plan A's Gate R4 record before Task 1):** `PROTO_PSEUDO` (rewriter-proto pseudo-version of `PROTO_SHA`), `REWRITER_GO_TAG` (planned `v0.13.0`), `REWRITER_GRPC_TAG` (planned `v0.15.0`), `POLYGLOT_VERSION` (planned `v0.12.1`), `FFI_SHA256_LINUX` / `FFI_SHA256_DARWIN` (from the release's `SHA256SUMS`). The tags actually cut are authoritative.
- **Contract:** HouseGate sends only `STORAGE_INTEGRITY_CONTRACT_V2 = 2` and requires a V2 acknowledgement on every SI response and every probe response; it never falls back to V1. A V1-only rewriter refuses startup.
- **Probe:** after Task 1, seven cases in this order: DESCRIBE fingerprint, `SYSTEM RELOAD CONFIG`, `SYSTEM START MERGES hg_unsafe.db1__t`, `TRUNCATE DATABASE hg_safe`, the tagged heredoc, `DROP TABLE db1.t` → Success / `STATEMENT_TYPE_DROP_TABLE` / `"success"` with SQL native `DROP TABLE phys."db1.t"` and gRPC ``DROP TABLE phys.`db1.t` ``, and `SYSTEM RELOAD CONFIG` under an empty table map → `UnsupportedStatement` / `"storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"`. Task 4 adds `reserved_databases = [hg_safe, hg_unsafe, hg_promote]` to every probe request and an eighth case, `SELECT * FROM hg_safe.db1__t` under an empty table map → `RewriteError` / `storage-integrity physical table hg_safe.db1__t is not directly addressable`. Required-build text: `rewriter-go >= v0.13.0 or rewriter-grpc >= v0.15.0 (storage-integrity contract V2)`.
- **Reserved databases:** every enabled request's `StorageIntegrityArgs.ReservedDatabases` is exactly `[hg_safe, hg_unsafe, hg_promote]`, from `sitable.ReservedDatabases()`, which a `pkg/config` test pins to `config.StorageIntegritySafeDatabase`, `StorageIntegrityUnsafeDatabase` and `StorageIntegrityPromoteDatabase`; no other code spells those names for this purpose.
- **Switch:** `storage_integrity.enabled` (`*bool`, yaml/json `enabled`, omitempty) defaults to `len(tables) > 0`; enabled needs exactly one of `tables` or `Options.StorageIntegrityTableState`; an injected state with the switch off is an error; `runtime` and `read.default_mode` require enabled.
- **Snapshot:** exactly one `TableState.Current()` per query, at the start of `rewrite.Plugin.OnQuery`, stored in `QueryContext.TableSnapshot` and attached with `rewriter.WithTableSnapshot`; later stages never call `Current()`. `sitable.StaticVersion = 1`; `sitable.Fake` starts at version 1 and `Set` increments.
- **Query-plugin order (server):** auth → usage → sireserved (enabled) → concurrency → lthash → forward → rewrite → **sitablestate (enabled)** → SI ingress → indexing_usage → commitgate → route signer → metrics.
- **Refusal codes:** retryable `733` (`TABLE_IS_BEING_RESTARTED`), non-retryable `392` (`QUERY_IS_PROHIBITED`), Gone `60` (`UNKNOWN_TABLE`), all as `chproto.ClientError` without `KeepSession`; `252` back-pressure and the generic `403` keep their meaning.
- **Message prefixes (stable client contract):** `storage_integrity: table <id> is pending activation (retryable)`; `storage_integrity: table <id> was refused: <code>: <reason>`; `storage_integrity: table <id> is still being purged; retry CREATE later (retryable)`; `storage_integrity: table <id> no longer accepts writes`; `storage_integrity: table <id> requires a signed INSERT; the client's table state is stale (retryable)`; plus `storage_integrity: table <id> is governed by storage integrity and cannot be created with data; create the table first, then INSERT`, `storage_integrity: table <id> is governed by storage integrity; ALTER and RENAME are not supported`, and Gone's `Table <id> does not exist`.
- **JSON-RPC (agent → sentio-node):** method `sentio_getStorageIntegrityTableStatus`, positional params `[database, table]`, result `{"status": "ordinary|pending|refused|active|gone", "refused_code": "", "refused_reason": "", "schema_json": "", "schema_hash": "", "registry_version": 0}`; one call per INSERT, no cache.
- **Metric:** `clickhouse_proxy_agent_si_table_status_failures_total` (counter, no labels).
- **Physical names:** `hg_safe.<db>__<table>` / `hg_unsafe.<db>__<table>` (`sitable.PhysicalTable`, pinned against `config.StorageIntegrityPhysicalTable`).
- **Tests:** Bazel is the ground truth (`bazel test //...`). `go test` on the root package and `pkg/storageintegrity` needs `-vet=off`: `go vet` reports a pre-existing `suspect or` at `pkg/storageintegrity/intake_test.go:1802` (present on `872c91b`). Docker-bound targets stay `manual`; the new lifecycle test lives in the already-listed `//pkg/integration:integration_test`.
- **Conventions:** run `bazel run //:gazelle` after adding files; `pkg/log` structured logging; English code and comments; Markdown not hard-wrapped; every commit message ends with a blank line and `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A table-state change lost between two supervisor passes.** `Changed()` is edge-triggered: a close that lands after a pass and before `Run` calls `Changed()` again is invisible, and the next tick is `reassert_interval` (30s) away, during which a newly Active table refuses every admission. `Run` therefore subscribes first and then compares the current version with the last asserted one. Test: Task 7 Step 5 `TestMergeSupervisorAssertsImmediatelyOnChange` (the version moves with no sleep before `Run` subscribes; run under `-race`).
2. **The two-pass `unsafe_latest` rewrite.** Parts can be chosen only after the rewriter reports the accessed tables, so a second pass is needed; its arguments must be a fresh value (the prototype's first draft mutated the first request's arguments in place, so the recorded first request showed the second pass's exclusions) and the accessed SI set must be the same in both passes. Tests: Task 4 Step 1 `TestSentioRewriter_UnsafeLatestFetchesPartsForAccessedTablesOnly` (asserts the classification pass excluded nothing and an unaccessed Active table carries no parts), `TestSentioRewriter_UnsafeLatestRejectsAMovingAccessedSet`, `TestSentioRewriter_UnsafeLatestWithoutPromotedPartsIsOnePass`.
3. **Lexical traps in data-carrying CREATE detection.** The rewriter reports `CREATE_TABLE` for plain and `AS SELECT` creates and `CREATE_MATERIALIZED_VIEW` with or without `POPULATE` / `TO`, so a header lexer decides; `AS SELECT` inside a comment, string, quoted identifier or `COMMENT` clause, `EMPTY AS SELECT`, `TTL ... TO DISK`, refreshable `APPEND TO`, and `POPULATE`/`TO` inside the view body must not flip the verdict. Tests: Task 5 Step 1 `TestCreateTableCarriesData` (11 spellings), `TestMaterializedViewHeader` (7), `TestDataCarryingCreationIntoGovernedTables` (15 rows).
4. **`hg_*` reachable while no table is Active.** Before `reserved_databases`, both engines learned the protected databases only from Active entries (measured: `SELECT * FROM hg_safe.db1__t` was a V2 `Success` pass-through under an empty map), yet Pending (ensured) and Gone (not yet purged) tables have `hg_*` tables. A request that forgets the list, or an engine that ignores it, silently reopens the hole exactly when no table is Active — the state the static-config suite never exercises. Tests: Task 4 Step 1 `TestBuildStorageIntegrityArgs_EmptyTableMapStillSendsV2` and `TestSentioRewriter_SendsV2WithEmptyTableMap` (the list rides on a request with no Active table); Task 4 Step 1 probe case `v2-empty-map-reserved-database` (startup refuses an engine that forwards `SELECT * FROM hg_safe.db1__t` under an empty map); Task 10 Step 1 lifecycle step 1 (a direct `INSERT INTO hg_unsafe.tsdb__t` while Pending is refused and lands no row).
5. **A refusal that makes the client reconnect.** The codes must end only the query. clickhouse-go closes its pooled connection on every error regardless of code (Plan decision R3), so the measurable client is clickhouse-client. Test: Task 10 Step 1 `requireSameConnection` — `SELECT connectionId()` before and after a 733 and a 392 (step 1) and a 60 (step 3) in one `--multiquery --ignore-error` run must return the same id.

## Plan decisions

- **R1 — Plan A values.** The V2 enum, pins and probe expectations are plan A handoff items 1–5. Item 3 shows the engines quote the DROP output differently, so the probe pins one exact string per engine (`sqlAfterByEngine`, keyed by `rewriter.EngineNative` / `EngineGRPC`; `""` means gRPC) instead of normalising quotes. The prototype compiled Task 1 against a local copy of rewriter-proto with the V2 enum and a `ReservedDatabases []string` field (Go field and getter only, no descriptor entry, so it is not marshalled over gRPC) added (a `go mod edit -replace` that is not part of the plan); every other task was built against that copy. The native engine available locally (v0.11.0) is V1-only, so the V2 behaviours themselves (the SI DROP and the empty-map catch-all) were not executed; the integration run emulated V2 acknowledgement with V1 plus one extra Active table (see "Verification record").
- **R2 — Snapshot plumbing.** The snapshot travels in `QueryContext.TableSnapshot` and in the rewriter's `context.Context` (`WithTableSnapshot`), not through the `rewriter.Rewriter` interface, so injected factories keep compiling. `sentioRewriter` falls back to `TableState.Current()` only when no snapshot is attached, and `RewriteErrorMessage` reuses the snapshot of the connection's last `Rewrite`. The scrubber cache keeps only the newest version; `OnException` uses the session's last query snapshot (evicted on close) and otherwise the current one.
- **R3 — Error codes.** Retryable `733 TABLE_IS_BEING_RESTARTED` (the table exists but is temporarily unavailable), non-retryable `392 QUERY_IS_PROHIBITED` (a policy refusal), Gone `60 UNKNOWN_TABLE` (spec). Names are from ch-go `proto/error_codes.go` (`ErrTableIsBeingRestarted = 733`, `ErrQueryIsProhibited = 392`, `ErrUnknownTable = 60`); none is 252 or 403, and HouseGate uses no other `ClientError` code. clickhouse-go (sentioxyz v2.47.0-sentioxyz-20260629): `conn.exception()` (`conn.go:260-270`) decodes every server Exception into `*proto.Exception` with no code branch, and `(*clickhouse).release` closes the pooled connection on any non-nil error (`clickhouse.go:368-395`); a search of both forks' non-test code finds no code comparison except ch-go's caller-side helpers `Exception.IsCode` / `IsErr` (`client.go:119-160`), which the libraries themselves never call with a code, so no code keeps or specially treats a clickhouse-go connection — the driver reconnects transparently, and HouseGate itself keeps the session because an `OnQuery` error ends only the query. clickhouse-client 26.8.1.368 against server 25.8.28 was measured through the prototype: the `connectionId()` before and after 733, 392 and 60 refusals in one `--multiquery --ignore-error` run was identical. The codes live in `pkg/chproto` next to `CodeTooManyParts`, and `sitablestate` aliases them, so the ingress and the runtime need not import a sibling plugin.
- **R4 — Which tables `unsafe_latest` touches.** HouseGate does not parse SQL to rewrite it (CLAUDE.md §4) and the rewriter reports `original_accessed_tables` only in its response, so the accessed tables cannot be known before the call. Ruling: pass 1 sends every Active table with no exclusions; if any accessed SI table has promoted parts, pass 2 sends those parts (and only those) and must report the same sorted accessed SI set, else `RejectedError`. With no promoted parts, pass 1 is final (its arguments are exactly the correct ones). `unsafe_latest` without a read-state port is still refused before any call, as today.
- **R5 — Data-carrying creation.** Measured with rewriter-go v0.11.0 native and no SI arguments: `CREATE TABLE db1.t2 AS SELECT * FROM db1.t` → `CREATE_TABLE`, accessed `[db1.t2]` only; `CREATE TABLE db1.t2 AS db1.t` → `[db1.t2, db1.t]`; `CREATE MATERIALIZED VIEW db1.mv TO db1.t AS SELECT a FROM db1.src` → `[mv, t, src]`; `... POPULATE AS SELECT a FROM db1.t` → `[mv, t]`. Classes: the first accessed table of `CREATE_TABLE` is the created one (data-carrying when the header has a top-level `AS SELECT` / `AS WITH` / `AS (SELECT` without `EMPTY`), the rest are data reads for CTAS and metadata for a schema clone; the first of `CREATE_MATERIALIZED_VIEW` is the view (data-carrying with `POPULATE`), the header's `TO` target (matched on original database and table) is a view target, the rest are data reads; `CREATE VIEW` sources are data reads, because a view bound to a Pending table's ordinary storage would keep reading it after activation. "Governed" is Pending or Active (spec §7.3), so CTAS into a Refused name passes and into a Gone name is the retryable purge message. Unknown statement types with accessed tables are treated as data reads; `CREATE/DROP DATABASE`, `GRANT` and `REVOKE` are not decided.
- **R6 — Recovery audit (spec §9.2).** Every schema read on the runtime paths: (1) `ConsumeStorageIntegrityAdmission` → `partsPressureTarget(rec, adm.TableSchema)` uses the query snapshot's schema; (2) `restorePressureReservations`, terminal record → the journaled `TouchedPartitionIDs` (or the legacy candidate set), no schema, unchanged; (3) non-terminal record with a journaled set (every record written since journal v1 computes it at admission even with back-pressure off) → now the journaled set, no schema; (4) non-terminal record without one (pre-v1 journal) → `TableState.Current().Schema(id)`, which answers Active and Gone; when it misses (Purged: the host reports the name unrecorded) → `ErrStorageIntegrityRecoverySchemaPurged` naming the statement and table, and startup fails closed. The orchestrator (`pkg/storageintegrity` intake, recovery, cleanup, candidate binding hooks) reads no schema: `PayloadPartitionIDs` and `DecodeNativePayload` are called only from `partsPressureTarget`.
- **R7 — What `sitable.Static` serves.** Listed tables are Active with the runtime's authoritative `TableSchemas` when the host injects them, else (ingress on) the declared network-state schema loaded once at startup, else no schema; a listed table without a schema refuses signed INSERTs at the ingress. Today the ingress loaded the latest declaration per query; a static deployment now needs a restart to see a later declaration, which matches the runtime's startup-fixed set. `ValidatePhysicalTableNames` and the config-to-schema bijection run only for `Static` (spec §9.3).
- **R8 — The ingress without a snapshot.** A disabled deployment (no `TableState`) keeps the declared-schema loader path, so the existing rewriter-mock integration tests that mark tables SI without configuring any keep passing; `buildServer` resolves `registry.TableSchemas` for the ingress only when storage integrity is disabled.
- **R9 — The stale-view refusal.** With a snapshot, an INSERT into an Active table with no `SQL_x_statement_token` gets the retryable code-733 message, checked after the payload-shape check and before the statement-id check (the target is now resolved first). Without a snapshot the ordering and messages are unchanged.
- **R10 — Agent claim order.** The status lookup runs as soon as the INSERT target resolves, before every lane rule (compression, user settings, inline VALUES parsing and evaluation); only Active continues into the lane. A target the shared parser cannot resolve keeps today's classification and errors. A failed lookup passes through and is counted; a hash mismatch or a `schema_json` for another table refuses (unsigned). A YAML- or host-declared table is Active and every other table Ordinary, so an undeclared table now passes through instead of failing with "not declared in network state" (the devnet2 report).
- **R11 — Merge guard membership.** A snapshot cannot enumerate Pending or Gone tables, so `AssertTables` enumerates `hg_safe` / `hg_unsafe` and checks every present table (starting only tables that pin the setting), requiring both tables of each Active id. Startup: the static set keeps its fail-fast error; a dynamic host logs and starts with the failing tables' latches closed.
- **R12 — Same-name CREATE of an Active table.** `sitablestate` allows it (spec matrix); both engines already reject it under an SI table map (measured message `storage-integrity table db1.t accepts writes only through the signed statement lane`), so the client sees that rejection.
- **R13 — `SubmitOutcome.AdmissionCode`** is `json:",omitempty"`, so journal records without it keep their exact bytes (`TestSubmitOutcomeJournalShapeIsUnchangedWithoutACode`).
- **R14 — The native engine does not rewrite `DESCRIBE` of an ordinary table** (measured: `DESCRIBE TABLE db1.t` passes through unchanged and ClickHouse answers `UNKNOWN_DATABASE`), a pre-existing engine gap; the lifecycle test checks metadata access on a Pending table with `EXISTS TABLE`.
- **R15 — Reserved databases come from the contract, not a HouseGate scan (controller ruling).** An earlier draft closed plan A's handoff gap (the engines knew `hg_*` only from Active entries) with a lexical `hg_*` mention guard in `sitablestate` while the Active set was empty; it was rejected because it refuses string literals that merely contain the names and misses names assembled by concatenation. Plan A instead adds `repeated string reserved_databases = 5` to `StorageIntegrityArgs` (Go `ReservedDatabases []string`, handoff item 1), which V2 engines protect with the same protected-namespace logic V1 applies to map-derived physical databases, also under an empty table map. HouseGate sends `[hg_safe, hg_unsafe, hg_promote]` on every enabled request (`hg_promote` too: the promotion lane's staging database is protocol-owned, and `config.StorageIntegrityPromoteDatabase` already pins it). The list lives in `sitable.ReservedDatabases()` because `pkg/config` imports `pkg/rewriter` (so the rewriter cannot read the config constants), and `pkg/config/sitable_names_test.go` pins each entry to its config constant. The probe proves the engine honours the list (handoff item 6: `SELECT * FROM hg_safe.db1__t` under an empty map → `RewriteError` / `storage-integrity physical table hg_safe.db1__t is not directly addressable`; a build without the field answers `Success`), and the five older probe cases keep their outputs with the list set (item 5); `sitablestate` does no reserved-name scanning. The operator-session `sireserved` guard is unchanged.

## File Structure

- **Port and config:** create `pkg/sitable/{sitable,static,fake}.go` (+ test) — Task 2; modify `pkg/config/storage_integrity_config.go` — Task 3.
- **Rewriter:** modify `pkg/rewriter/{types,sentio,probe,storage_integrity}.go` — Tasks 1, 4; create `pkg/rewriter/table_state_test.go` — Task 4.
- **Plugins:** modify `pkg/plugin/context.go`, `pkg/plugins/rewrite/rewriter.go` — Task 4; create `pkg/plugins/sitablestate/{plugin,lexer}.go` (+ tests) and modify `pkg/chproto/client_error.go` — Task 5; modify `pkg/plugins/storageintegrity/plugin.go` — Task 6; modify `pkg/plugins/sistatement/{plugin,observer}.go` — Task 9.
- **Root package:** create `storage_integrity_table_state.go`, modify `proxy.go`, `build.go` — Task 4 (and `build.go` again in Tasks 1, 5, 6, 7, 9); replace `storage_integrity_merge_supervisor.go`, `storage_integrity_runtime.go` — Task 7; modify `storage_integrity_ingress.go` — Tasks 7, 8.
- **Storage-integrity core:** create `pkg/storageintegrity/merge_guard_tables.go` — Task 7; modify `pkg/storageintegrity/{intake,arbiter_proto}.go` — Task 8.
- **Registry and network:** create `pkg/registry/table_status.go`, modify `pkg/network/rpc.go`, `pkg/proxy/observer.go` — Task 9.
- **Integration:** create `pkg/integration/storage_integrity_table_state_test.go` — Task 10.
- **Pins and docs:** `go.mod`, `go.sum`, `.github/workflows/ci.yml`, `configs/local.server*.yaml` — Task 1; `CLAUDE.md` — Tasks 1, 4–10; `README.md` — Tasks 1, 3, 4, 9; `pkg/rewriter/AGENTS.md` — Tasks 1, 4.
- Every new or changed package's `BUILD.bazel` comes from `bazel run //:gazelle`.

---

## Task 1: Rewriter contract V2: pins, send and require V2, the seven-case startup probe

**Files:**

- Modify: `go.mod`, `go.sum` (rewriter-proto `$PROTO_PSEUDO`, rewriter-go `$REWRITER_GO_TAG`); `MODULE.bazel.lock` only if `bazel mod tidy` changes it
- Modify: `pkg/rewriter/types.go:44` (add `StorageIntegrityContractV2`), `pkg/rewriter/storage_integrity.go:116`, `pkg/rewriter/sentio.go:219-224,315-324`, `pkg/rewriter/probe.go` (whole file), `build.go:476-483,722`
- Modify: `.github/workflows/ci.yml:111`, `configs/local.server.yaml:48`, `configs/local.server-mock-remote.yaml:48`, `pkg/integration/storage_integrity_read_test.go:47,201,343,455` (FFI tag), `CLAUDE.md` (§4 probe paragraph, `pkg/ffifetch` floor, FFI pin bullet), `README.md:170,193`, `pkg/rewriter/AGENTS.md:14,25,32`
- Test: `pkg/rewriter/probe_test.go` (whole file), `pkg/rewriter/backend_test.go:282,301,432`, `pkg/rewriter/storage_integrity_test.go:58`, `pkg/plugins/rewrite/rewriter_test.go:34,297-321`, `build_test.go:54,245,280`

**Interfaces:**

- Consumes (plan A handoff): `pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2` (value 2) from `github.com/housegate/rewriter-proto/gen/pb`; V2 engines rewriter-go `$REWRITER_GO_TAG` / rewriter-grpc `$REWRITER_GRPC_TAG`; the exact V2 outputs of plan A handoff items 3–5.
- Produces (package `rewriter`):

```go
const StorageIntegrityContractV2 = pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2
const (
	StorageIntegrityProbeDropExpectedSQLNative = `DROP TABLE phys."db1.t"`
	StorageIntegrityProbeDropExpectedSQLGRPC   = "DROP TABLE phys.`db1.t`"
)
func (*SentioNetworkFactory) StorageIntegrityContractVersion() pb.StorageIntegrityContractVersion // now returns V2
```

Every SI request carries `ContractVersion: StorageIntegrityContractV2`; every SI response and every probe response must acknowledge V2; `buildServer` requires a factory reporting V2. After this task a V1-only rewriter refuses startup (spec §14 "Mixed rewriter versions").

- [ ] **Step 1: Record the plan A outputs**

The controller exports `PROTO_SHA` (and, when the cut tags differ from the planned ones, `REWRITER_GO_TAG`, `REWRITER_GRPC_TAG`, `POLYGLOT_VERSION`) from plan A's tracking issue (Gate R4). Every later command in this task reads these variables. `go list` prints the pseudo-version, or `v0.3.0` once the tag on `PROTO_SHA` is visible to the module proxy; either pins the same commit.

```bash
: "${PROTO_SHA:?export PROTO_SHA, the rewriter-proto commit plan A Gate R1 recorded}"
export PROTO_PSEUDO=$(GOFLAGS=-mod=mod go list -m -f '{{.Version}}' github.com/housegate/rewriter-proto@"$PROTO_SHA")
export REWRITER_GO_TAG=${REWRITER_GO_TAG:-v0.13.0}       # plan A Gate R2 records the tag actually cut
export REWRITER_GRPC_TAG=${REWRITER_GRPC_TAG:-v0.15.0}   # plan A Gate R3
export POLYGLOT_VERSION=${POLYGLOT_VERSION:-v0.12.1}     # the polyglot release rewriter-go $REWRITER_GO_TAG builds its FFI from
echo "$PROTO_PSEUDO" | grep -q -- "-${PROTO_SHA:0:12}$" || echo "$PROTO_PSEUDO" | grep -q '^v0\.3\.0$'
gh release download "$REWRITER_GO_TAG" -R housegate/rewriter-go -p SHA256SUMS -O - | tee /tmp/rewriter-go-SHA256SUMS
export FFI_SHA256_LINUX=$(awk '/libpolyglot_sql_ffi-linux-x86_64.so/ {print $1}' /tmp/rewriter-go-SHA256SUMS)
export FFI_SHA256_DARWIN=$(awk '/libpolyglot_sql_ffi-macos-arm64.dylib/ {print $1}' /tmp/rewriter-go-SHA256SUMS)
test ${#FFI_SHA256_LINUX} -eq 64 && test ${#FFI_SHA256_DARWIN} -eq 64
```

- [ ] **Step 2: Pin rewriter-proto and rewriter-go**

Follow `.claude/skills/upgrade-dependency/SKILL.md` Steps 2–3. Neither module is a `replace` target, and housegate has no `bazel_dep` pin on them, so `go.mod` is the only pin mechanism for the Go modules.

```bash
go get github.com/housegate/rewriter-proto@"$PROTO_PSEUDO" github.com/housegate/rewriter-go@"$REWRITER_GO_TAG"
go mod tidy
bazel mod tidy && bazel run //:gazelle
grep -n 'STORAGE_INTEGRITY_CONTRACT_V2 ' "$(go env GOMODCACHE)/github.com/housegate/rewriter-proto@$PROTO_PSEUDO/gen/pb/rewriter.pb.go"
```

- [ ] **Step 3: Write the failing tests**

Replace the whole of `pkg/rewriter/probe_test.go` with (seven probes, V2 acknowledgement, the empty-map request keyed separately, the per-engine DROP fingerprint):

```go
package rewriter

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/network"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

type scriptedProbeBackend struct {
	fakeBackend
	responses map[string]*pb.RewriteSQLResponse
	requests  []*pb.RewriteSQLRequest
	deadlines []bool
}

func (b *scriptedProbeBackend) Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	b.lastReq = req
	b.requests = append(b.requests, req)
	_, hasDeadline := ctx.Deadline()
	b.deadlines = append(b.deadlines, hasDeadline)
	resp, ok := b.responses[probeKey(req)]
	if !ok {
		return nil, fmt.Errorf("unexpected storage-integrity probe SQL %q", req.GetSql())
	}
	return resp, nil
}

func TestProbeStorageIntegrityBuild(t *testing.T) {
	t.Run("correct build passes", func(t *testing.T) {
		be := &scriptedProbeBackend{responses: conformingProbeResponses()}
		f := newSIFactory(be, nil, true)
		if err := f.ProbeStorageIntegrityBuild(context.Background()); err != nil {
			t.Fatalf("probe: %v", err)
		}
		if got := strings.Join(probeSQLs(be.requests), " | "); got != strings.Join([]string{
			storageIntegrityProbeSQL,
			"SYSTEM RELOAD CONFIG",
			"SYSTEM START MERGES hg_unsafe.db1__t",
			"TRUNCATE DATABASE hg_safe",
			storageIntegrityProbeHeredocSQL,
			"DROP TABLE db1.t",
			"SYSTEM RELOAD CONFIG (empty table map)",
		}, " | ") {
			t.Fatalf("probe SQLs = %s", got)
		}
		si := be.requests[3].GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
		if si.GetContractVersion() != StorageIntegrityContractV2 || si.GetTables()["db1.t"].GetSafeTable() != "hg_safe.db1__t" {
			t.Fatalf("probe request did not carry the fixed SI args: %v", si)
		}
		empty := be.requests[6].GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
		if empty.GetContractVersion() != StorageIntegrityContractV2 || empty.GetTables() == nil || len(empty.GetTables()) != 0 {
			t.Fatalf("empty-map probe args = %v, want V2 with an empty table map", empty)
		}
		for i, hasDeadline := range be.deadlines {
			if !hasDeadline {
				t.Fatalf("probe request %d had no deadline", i)
			}
		}
	})

	// A pre-Spec-N engine answers every Spec I probe correctly and then forwards
	// the tagged heredoc as Success — which is exactly the shape rewriter-go
	// v0.9.0 has. Without this case the probe would pass a build in which any
	// authenticated user can read hg_safe through merge($tag$hg_safe$tag$, ...).
	t.Run("pre-Spec-N build is refused on the heredoc probe", func(t *testing.T) {
		responses := conformingProbeResponses()
		responses[storageIntegrityProbeHeredocSQL] = acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			StatementType:   pb.StatementType_STATEMENT_TYPE_SELECT,
			SqlAfterRewrite: "SELECT * FROM merge('hg_safe', 'db1__t')",
		})
		be := &scriptedProbeBackend{responses: responses}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil {
			t.Fatal("a build that forwards a tagged heredoc into hg_safe must fail the probe")
		}
		if !strings.Contains(err.Error(), "tagged-heredoc-namespace") {
			t.Fatalf("err = %v, want the heredoc probe named", err)
		}
		if len(be.requests) != 5 {
			t.Fatalf("the heredoc probe must run fifth, after all four Spec I probes; got %v", probeSQLs(be.requests))
		}
	})

	// A V1-only build (rewriter-go < v0.13.0) acknowledges V1, never V2; and a
	// build that acknowledges V2 but still rejects the SI DROP or activates the
	// catch-all by table count is refused on the matching V2 probe.
	t.Run("V1 acknowledgement is refused", func(t *testing.T) {
		responses := conformingProbeResponses()
		for _, resp := range responses {
			resp.StorageIntegrityContractVersion = StorageIntegrityContractV1
		}
		err := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "probe=describe-fingerprint") || !strings.Contains(err.Error(), "acknowledgement") {
			t.Fatalf("err = %v, want the first probe refused on its V1 acknowledgement", err)
		}
	})

	t.Run("V1 DROP rejection is refused", func(t *testing.T) {
		responses := conformingProbeResponses()
		responses["DROP TABLE db1.t"] = acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "DROP TABLE db1.t",
			Message:         "storage-integrity table db1.t accepts writes only through the signed statement lane",
		})
		err := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "probe=v2-si-drop-ordinary-physical") {
			t.Fatalf("err = %v, want the V2 DROP probe named", err)
		}
	})

	// The DROP fingerprint is exact per engine: the native engine quotes the
	// physical table with double quotes, the gRPC engine with backticks.
	t.Run("DROP fingerprint follows the engine", func(t *testing.T) {
		responses := conformingProbeResponses()
		responses["DROP TABLE db1.t"].SqlAfterRewrite = StorageIntegrityProbeDropExpectedSQLNative
		grpc := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true)
		if err := grpc.ProbeStorageIntegrityBuild(context.Background()); err == nil || !strings.Contains(err.Error(), "probe=v2-si-drop-ordinary-physical") {
			t.Fatalf("grpc engine given the native spelling: err = %v, want the DROP probe refused", err)
		}
		native := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true)
		native.options.Engine = EngineNative
		if err := native.ProbeStorageIntegrityBuild(context.Background()); err != nil {
			t.Fatalf("native engine given the native spelling: %v", err)
		}
	})

	t.Run("table-count catch-all is refused", func(t *testing.T) {
		responses := conformingProbeResponses()
		responses["SYSTEM RELOAD CONFIG (empty table map)"] = acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			SqlAfterRewrite: "SYSTEM RELOAD CONFIG",
		})
		err := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "probe=v2-empty-map-catch-all") {
			t.Fatalf("err = %v, want the empty-map catch-all probe named", err)
		}
	})

	t.Run("old build is refused", func(t *testing.T) {
		be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:          pb.RewriteCode_Success,
			StatementType: pb.StatementType_STATEMENT_TYPE_DESCRIBE,
			SqlAfterRewrite: "SELECT name, type, default_type, default_expression, comment, " +
				"codec_expression, ttl_expression FROM system.columns WHERE database = 'hg_safe' " +
				"AND table = 'db1__t' AND name != '_hg_row_id' ORDER BY position",
		})}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "storage-integrity engine probe") {
			t.Fatalf("err = %v, want a build-probe refusal", err)
		}
	})

	t.Run("missing acknowledgement is refused", func(t *testing.T) {
		be := &fakeBackend{resp: &pb.RewriteSQLResponse{
			Code: pb.RewriteCode_Success, SqlAfterRewrite: StorageIntegrityProbeExpectedSQL}}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "acknowledgement") {
			t.Fatalf("err = %v, want an acknowledgement refusal", err)
		}
	})

	t.Run("rejected probe is refused", func(t *testing.T) {
		be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code: pb.RewriteCode_UnsupportedStatement, Message: "nope"})}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "UnsupportedStatement") {
			t.Fatalf("err = %v, want a rejected-probe refusal", err)
		}
	})
}

func TestProbeStorageIntegrityBuildRefusesIncompleteSpecIBehavior(t *testing.T) {
	for _, tc := range []struct {
		name      string
		probeSQL  string
		probeName string
		mutate    func(*pb.RewriteSQLResponse)
	}{
		{
			name:      "wrong DESCRIBE success message",
			probeSQL:  storageIntegrityProbeSQL,
			probeName: "describe-fingerprint",
			mutate: func(resp *pb.RewriteSQLResponse) {
				resp.Message = ""
			},
		},
		{
			name:      "old catch-all success despite matching DESCRIBE",
			probeSQL:  "SYSTEM RELOAD CONFIG",
			probeName: "unmodelled-catch-all",
			mutate: func(resp *pb.RewriteSQLResponse) {
				resp.Code = pb.RewriteCode_Success
				resp.Message = ""
			},
		},
		{
			name:      "old physical SYSTEM target success",
			probeSQL:  "SYSTEM START MERGES hg_unsafe.db1__t",
			probeName: "protected-physical-system-target",
			mutate: func(resp *pb.RewriteSQLResponse) {
				resp.Code = pb.RewriteCode_Success
				resp.Message = ""
			},
		},
		{
			name:      "stub physical database rejection",
			probeSQL:  "TRUNCATE DATABASE hg_safe",
			probeName: "protected-physical-database",
			mutate: func(resp *pb.RewriteSQLResponse) {
				resp.Message = "unsupported target hg_safe from hg_unsafe"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			responses := conformingProbeResponses()
			tc.mutate(responses[tc.probeSQL])
			be := &scriptedProbeBackend{responses: responses}
			err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
			if err == nil {
				t.Fatal("probe passed an incomplete Spec I backend")
			}
			for _, want := range []string{"storage-integrity engine probe", "engine=grpc", "probe=" + tc.probeName, storageIntegrityProbeRequiredBuild} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %q, want %q", err, want)
				}
			}
			for _, protectedName := range []string{"hg_safe", "hg_unsafe", "db1__t"} {
				if strings.Contains(err.Error(), protectedName) {
					t.Fatalf("err leaked protocol-owned name %q: %v", protectedName, err)
				}
			}
		})
	}
}

// TestReleasedGRPCStorageIntegrityProbeSmoke drives the real gRPC transport
// through the same startup conformance suite. It is opt-in because CI does not
// run a rewriter-grpc service; release validation supplies the address with:
//
//	bazel test //pkg/rewriter:rewriter_test \
//	  --test_filter=TestReleasedGRPCStorageIntegrityProbeSmoke \
//	  --test_env=HOUSEGATE_TEST_REWRITER_GRPC_ADDR=127.0.0.1:50051
func TestReleasedGRPCStorageIntegrityProbeSmoke(t *testing.T) {
	addr := os.Getenv("HOUSEGATE_TEST_REWRITER_GRPC_ADDR")
	if addr == "" {
		t.Skip("HOUSEGATE_TEST_REWRITER_GRPC_ADDR not set; released gRPC engine unavailable")
	}
	f, err := NewSentioNetworkFactory(Options{
		Engine:      EngineGRPC,
		ServiceAddr: addr,
		Timeout:     10 * time.Second,
	}, network.NewInMemoryNetworkState())
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory(grpc): %v", err)
	}
	defer f.Close()
	if err := f.ProbeStorageIntegrityBuild(context.Background()); err != nil {
		t.Fatalf("released gRPC storage-integrity probe: %v", err)
	}
}

func conformingProbeResponses() map[string]*pb.RewriteSQLResponse {
	return map[string]*pb.RewriteSQLResponse{
		storageIntegrityProbeSQL: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			StatementType:   pb.StatementType_STATEMENT_TYPE_DESCRIBE,
			SqlAfterRewrite: StorageIntegrityProbeExpectedSQL,
			Message:         "success",
		}),
		"SYSTEM RELOAD CONFIG": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "SYSTEM RELOAD CONFIG",
			Message:         "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded",
		}),
		"SYSTEM START MERGES hg_unsafe.db1__t": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "SYSTEM START MERGES hg_unsafe.db1__t",
			Message:         "storage-integrity physical table hg_unsafe.db1__t is not directly addressable",
		}),
		"TRUNCATE DATABASE hg_safe": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "TRUNCATE DATABASE hg_safe",
			Message:         "storage-integrity physical database hg_safe is not directly addressable",
		}),
		storageIntegrityProbeHeredocSQL: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_RewriteError,
			SqlAfterRewrite: storageIntegrityProbeHeredocSQL,
			Message:         storageIntegrityProbeHeredocMessage,
		}),
		"DROP TABLE db1.t": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			StatementType:   pb.StatementType_STATEMENT_TYPE_DROP_TABLE,
			SqlAfterRewrite: StorageIntegrityProbeDropExpectedSQLGRPC,
			Message:         "success",
		}),
		"SYSTEM RELOAD CONFIG (empty table map)": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "SYSTEM RELOAD CONFIG",
			Message:         storageIntegrityProbeEmptyMapMessage,
		}),
	}
}

// probeKey distinguishes the two SYSTEM RELOAD CONFIG probes by whether the
// request carried an empty table map.
func probeKey(req *pb.RewriteSQLRequest) string {
	si := req.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
	if len(si.GetTables()) == 0 {
		return req.GetSql() + " (empty table map)"
	}
	return req.GetSql()
}

func probeSQLs(reqs []*pb.RewriteSQLRequest) []string {
	sqls := make([]string, 0, len(reqs))
	for _, req := range reqs {
		sqls = append(sqls, probeKey(req))
	}
	return sqls
}
```

In `pkg/rewriter/backend_test.go`, replace:

```go
	resp.StorageIntegrityContractVersion = StorageIntegrityContractV1
	return resp
```

with:

```go
	resp.StorageIntegrityContractVersion = StorageIntegrityContractV2
	return resp
```

In `pkg/rewriter/backend_test.go`, replace:

```go
	if si.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE || si.GetContractVersion() != StorageIntegrityContractV1 || si.GetTables()["db1.t"].GetSafeTable() != "hg_safe.db1__t" {
```

with:

```go
	if si.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE || si.GetContractVersion() != StorageIntegrityContractV2 || si.GetTables()["db1.t"].GetSafeTable() != "hg_safe.db1__t" {
```

In `pkg/rewriter/backend_test.go`, replace:

```go
	if err != nil || res.StorageIntegrityContractVersion != StorageIntegrityContractV1 {
```

with:

```go
	if err != nil || res.StorageIntegrityContractVersion != StorageIntegrityContractV2 {
```

In `pkg/rewriter/storage_integrity_test.go`, replace:

```go
		got.GetContractVersion() != StorageIntegrityContractV1 {
```

with:

```go
		got.GetContractVersion() != StorageIntegrityContractV2 {
```

In `pkg/plugins/rewrite/rewriter_test.go`, replace:

```go
		RequiredStorageIntegrityContractVersion: pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1,
```

with:

```go
		RequiredStorageIntegrityContractVersion: rewriter.StorageIntegrityContractV2,
```

In `pkg/plugins/rewrite/rewriter_test.go`, replace: (all 4 occurrences)

```go
rewriter.StorageIntegrityContractV1
```

with:

```go
rewriter.StorageIntegrityContractV2
```

In `build_test.go`, replace:

```go
func (siCapableStubRewriterFactory) StorageIntegrityContractVersion() rewriterpb.StorageIntegrityContractVersion {
	return rewriter.StorageIntegrityContractV1
}
```

with:

```go
func (siCapableStubRewriterFactory) StorageIntegrityContractVersion() rewriterpb.StorageIntegrityContractVersion {
	return rewriter.StorageIntegrityContractV2
}
```

In `build_test.go`, replace:

```go
	if !rewritePlugin.FailClosedOnError || rewritePlugin.RequiredStorageIntegrityContractVersion != rewriter.StorageIntegrityContractV1 {
```

with:

```go
	if !rewritePlugin.FailClosedOnError || rewritePlugin.RequiredStorageIntegrityContractVersion != rewriter.StorageIntegrityContractV2 {
```

In `build_test.go`, replace:

```go
	if err == nil || !strings.Contains(err.Error(), "storage-integrity contract v1") {
```

with:

```go
	if err == nil || !strings.Contains(err.Error(), "storage-integrity contract V2") {
```

- [ ] **Step 4: Run them to verify they fail**

Run: `go test -vet=off -count=1 ./pkg/rewriter/`

Expected: FAIL, build failed: `undefined: StorageIntegrityContractV2` (and the probe-test constants `StorageIntegrityProbeDropExpectedSQLGRPC`, `storageIntegrityProbeEmptyMapMessage`).

- [ ] **Step 5: Send and require V2**

In `pkg/rewriter/types.go`, replace:

```go
const StorageIntegrityContractV1 = pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1

```

with:

```go
const StorageIntegrityContractV1 = pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V1

// StorageIntegrityContractV2 is the only contract HouseGate sends and accepts
// (spec 2026-09-24 §8): V1 plus the SI DROP TABLE rewrite, activated by the
// contract version rather than by the table count. HouseGate never falls back
// to V1.
const StorageIntegrityContractV2 = pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_V2

```

In `pkg/rewriter/storage_integrity.go`, replace:

```go
		ContractVersion:     StorageIntegrityContractV1,
```

with:

```go
		ContractVersion:     StorageIntegrityContractV2,
```

In `pkg/rewriter/sentio.go`, replace:

```go
// StorageIntegrityContractVersion implements StorageIntegrityCapableFactory.
// The marker is truthful because sentioRewriter requires the exact backend
// acknowledgement whenever SI membership is configured.
func (*SentioNetworkFactory) StorageIntegrityContractVersion() pb.StorageIntegrityContractVersion {
	return StorageIntegrityContractV1
}
```

with:

```go
// StorageIntegrityContractVersion implements StorageIntegrityCapableFactory.
// The marker is truthful because sentioRewriter requires the exact backend
// acknowledgement whenever storage integrity is enabled.
func (*SentioNetworkFactory) StorageIntegrityContractVersion() pb.StorageIntegrityContractVersion {
	return StorageIntegrityContractV2
}
```

In `pkg/rewriter/sentio.go`, replace:

```go
		resp.GetStorageIntegrityContractVersion() != StorageIntegrityContractV1 {
		return RewriteResult{}, &RejectedError{Code: pb.RewriteCode_RewriteError,
			Message: fmt.Sprintf("storage-integrity rewriter contract acknowledgement unavailable: got %s, want %s",
				resp.GetStorageIntegrityContractVersion(), StorageIntegrityContractV1)}
```

with:

```go
		resp.GetStorageIntegrityContractVersion() != StorageIntegrityContractV2 {
		return RewriteResult{}, &RejectedError{Code: pb.RewriteCode_RewriteError,
			Message: fmt.Sprintf("storage-integrity rewriter contract acknowledgement unavailable: got %s, want %s",
				resp.GetStorageIntegrityContractVersion(), StorageIntegrityContractV2)}
```

Replace the whole of `pkg/rewriter/probe.go` with (V2 arguments, the `emptyTables` switch, two V2 probes, per-engine DROP fingerprint; the DROP SQL, the empty-map message and the floors are plan A handoff items 3–5):

```go
package rewriter

import (
	"context"
	"fmt"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"
)

const (
	// storageIntegrityProbeSQL is the fixed DESCRIBE rewritten by the startup
	// build probe. Both engines construct its output byte-identically.
	storageIntegrityProbeSQL = "DESCRIBE TABLE db1.t"

	storageIntegrityProbeUnmodelledSQL           = "SYSTEM RELOAD CONFIG"
	storageIntegrityProbeUnmodelledMessage       = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"
	storageIntegrityProbePhysicalSystemSQL       = "SYSTEM START MERGES hg_unsafe.db1__t"
	storageIntegrityProbePhysicalSystemMessage   = "storage-integrity physical table hg_unsafe.db1__t is not directly addressable"
	storageIntegrityProbePhysicalDatabaseSQL     = "TRUNCATE DATABASE hg_safe"
	storageIntegrityProbePhysicalDatabaseMessage = "storage-integrity physical database hg_safe is not directly addressable"

	// A tagged heredoc is the Spec N D6 version discriminator. Polyglot encodes
	// it as literal_type "dollar_string" with the tag packed into the value as
	// "<tag>\x00<body>"; an engine that reads the raw value without consulting
	// literal_type is handed "tag\x00hg_safe", matches nothing, and forwards a
	// statement its own generator re-emits as merge('hg_safe', ...). Every
	// rewriter-go build before v0.10.0 answers Success here.
	storageIntegrityProbeHeredocSQL     = "SELECT * FROM merge($tag$hg_safe$tag$, 'db1__t')"
	storageIntegrityProbeHeredocMessage = "storage-integrity physical table hg_safe.db1__t is not directly addressable"

	// Contract V2 (spec 2026-09-24 §8.2). DROP of an SI table succeeds and
	// drops only the ordinary physical table; the catch-all is activated by
	// the contract version even when the table map is empty. Both values are
	// copied from plan A's "Handoff to plan B" section.
	storageIntegrityProbeDropSQL         = "DROP TABLE db1.t"
	storageIntegrityProbeEmptyMapMessage = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"
)

// The exact V2 rewrite of storageIntegrityProbeDropSQL under the fixed probe
// arguments: the ordinary physical table only, hg_safe / hg_unsafe untouched.
// The engines differ in identifier quoting only (plan A handoff item 3), so
// the probe pins one exact string per engine.
const (
	StorageIntegrityProbeDropExpectedSQLNative = `DROP TABLE phys."db1.t"`
	StorageIntegrityProbeDropExpectedSQLGRPC   = "DROP TABLE phys.`db1.t`"
)

// StorageIntegrityProbeExpectedSQL is the exact output a compatible Spec I
// engine emits for storageIntegrityProbeSQL under the fixed probe arguments.
// It must stay identical to the shared si_describe_metadata_select corpus case.
const StorageIntegrityProbeExpectedSQL = "SELECT name, type, default_kind AS default_type, default_expression, comment, '' AS codec_expression, '' AS ttl_expression FROM system.columns WHERE database = 'hg_safe' AND table = 'db1__t' AND name != '_hg_row_id' ORDER BY position"

// The final release tags are pinned separately when the fixed Go and C++
// engines are published. The probe itself identifies the required behavior
// without guessing an unreleased version.
const storageIntegrityProbeRequiredBuild = "rewriter-go >= v0.13.0 or rewriter-grpc >= v0.15.0 (storage-integrity contract V2)"

// StorageIntegrityProbeFactory is a Factory whose concrete engine behavior can
// be verified at startup. Contract v1 alone cannot distinguish patch builds.
type StorageIntegrityProbeFactory interface {
	Factory
	ProbeStorageIntegrityBuild(ctx context.Context) error
}

// storageIntegrityProbeArgs returns the fixed probe arguments; emptyTables
// sends the V2 contract with an empty table map.
func storageIntegrityProbeArgs(emptyTables bool) *pb.RewriteTableDynamicArgs {
	tables := map[string]*pb.StorageIntegrityArgs_Table{
		"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
	}
	if emptyTables {
		tables = map[string]*pb.StorageIntegrityArgs_Table{}
	}
	return &pb.RewriteTableDynamicArgs{
		DatabaseMap:            map[string]string{"db1": "phys"},
		KnownPhysicalDatabases: []string{"phys"},
		Delim:                  "_",
		StorageIntegrity: &pb.StorageIntegrityArgs{
			Tables:              tables,
			ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
			ReservedRowIdColumn: DefaultReservedRowIDColumn,
			ContractVersion:     StorageIntegrityContractV2,
		},
	}
}

type storageIntegrityBuildProbe struct {
	name          string
	emptyTables   bool
	sql           string
	code          pb.RewriteCode
	statementType pb.StatementType
	sqlAfter      string
	// sqlAfterByEngine, when set, replaces sqlAfter with the engine's own
	// exact output (keyed by EngineGRPC / EngineNative).
	sqlAfterByEngine map[string]string
	message          string
}

var storageIntegrityBuildProbes = []storageIntegrityBuildProbe{
	{
		name:          "describe-fingerprint",
		sql:           storageIntegrityProbeSQL,
		code:          pb.RewriteCode_Success,
		statementType: pb.StatementType_STATEMENT_TYPE_DESCRIBE,
		sqlAfter:      StorageIntegrityProbeExpectedSQL,
		message:       "success",
	},
	{
		name:          "unmodelled-catch-all",
		sql:           storageIntegrityProbeUnmodelledSQL,
		code:          pb.RewriteCode_UnsupportedStatement,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbeUnmodelledSQL,
		message:       storageIntegrityProbeUnmodelledMessage,
	},
	{
		name:          "protected-physical-system-target",
		sql:           storageIntegrityProbePhysicalSystemSQL,
		code:          pb.RewriteCode_UnsupportedStatement,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbePhysicalSystemSQL,
		message:       storageIntegrityProbePhysicalSystemMessage,
	},
	{
		// This D2 invariant is intentionally not a version discriminator:
		// older engines also rejected TRUNCATE, but HouseGate must require the
		// deterministic protected-database classification and message.
		name:          "protected-physical-database",
		sql:           storageIntegrityProbePhysicalDatabaseSQL,
		code:          pb.RewriteCode_UnsupportedStatement,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbePhysicalDatabaseSQL,
		message:       storageIntegrityProbePhysicalDatabaseMessage,
	},
	{
		// Spec N D6. Unlike the D2 invariant above, this one IS a version
		// discriminator, and it is the reason the required-build floor moved.
		// It discriminates the ENGINE build, not the FFI library: v0.9.0 and
		// v0.10.0 ship byte-identical polyglot artifacts (same SHA256SUMS), so
		// the fix lives entirely in rewriter-go's Go code and no library pin
		// could have caught a stale one. A behavioural probe can, which is why
		// the floor is enforced here rather than only declared in go.mod.
		name:          "tagged-heredoc-namespace",
		sql:           storageIntegrityProbeHeredocSQL,
		code:          pb.RewriteCode_RewriteError,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbeHeredocSQL,
		message:       storageIntegrityProbeHeredocMessage,
	},
	{
		// V2 D1: the DROP succeeds, rewritten to the ordinary physical table.
		// Every V1-only build rejects it.
		name:          "v2-si-drop-ordinary-physical",
		sql:           storageIntegrityProbeDropSQL,
		code:          pb.RewriteCode_Success,
		statementType: pb.StatementType_STATEMENT_TYPE_DROP_TABLE,
		sqlAfterByEngine: map[string]string{
			EngineNative: StorageIntegrityProbeDropExpectedSQLNative,
			EngineGRPC:   StorageIntegrityProbeDropExpectedSQLGRPC,
		},
		message: "success",
	},
	{
		// V2 D2 (H6): the catch-all fires under an empty table map. A V1-only
		// build activates it by table count and answers Success here.
		name:          "v2-empty-map-catch-all",
		emptyTables:   true,
		sql:           storageIntegrityProbeUnmodelledSQL,
		code:          pb.RewriteCode_UnsupportedStatement,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbeUnmodelledSQL,
		message:       storageIntegrityProbeEmptyMapMessage,
	},
}

// ProbeStorageIntegrityBuild issues a bounded suite of fixed SI rewrites. The
// exact DESCRIBE fingerprint proves the read shape; the rejection probes prove
// the Spec I fail-closed surface that older engines could acknowledge as v1
// without implementing.
func (f *SentioNetworkFactory) ProbeStorageIntegrityBuild(ctx context.Context) error {
	engine := f.options.Engine
	if engine == "" {
		engine = EngineGRPC
	}
	if f.backend == nil {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): no rewrite backend", engine)
	}
	timeout := f.options.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for _, probe := range storageIntegrityBuildProbes {
		resp, err := f.backend.Rewrite(probeCtx, &pb.RewriteSQLRequest{
			Sql:     probe.sql,
			Options: []*pb.RewriteOption{rewriteOption(storageIntegrityProbeArgs(probe.emptyTables))},
		})
		if err != nil {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): %w; deploy %s",
				engine, probe.name, err, storageIntegrityProbeRequiredBuild)
		}
		if resp == nil {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): nil response; deploy %s",
				engine, probe.name, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetStorageIntegrityContractVersion() != StorageIntegrityContractV2 {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): contract acknowledgement %s, want %s; deploy %s",
				engine, probe.name, resp.GetStorageIntegrityContractVersion(), StorageIntegrityContractV2, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetCode() != probe.code {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): code=%s, want %s; deploy %s",
				engine, probe.name, resp.GetCode(), probe.code, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetStatementType() != probe.statementType {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): statement type=%s, want %s; deploy %s",
				engine, probe.name, resp.GetStatementType(), probe.statementType, storageIntegrityProbeRequiredBuild)
		}
		sqlAfter := probe.sqlAfter
		if probe.sqlAfterByEngine != nil {
			sqlAfter = probe.sqlAfterByEngine[engine]
		}
		if resp.GetSqlAfterRewrite() != sqlAfter {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): SQL fingerprint mismatch; deploy %s",
				engine, probe.name, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetMessage() != probe.message {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): message fingerprint mismatch; deploy %s",
				engine, probe.name, storageIntegrityProbeRequiredBuild)
		}
	}
	return nil
}

var _ StorageIntegrityProbeFactory = (*SentioNetworkFactory)(nil)
```

In `build.go`, replace:

```go
		if !ok || capable.StorageIntegrityContractVersion() != rewriter.StorageIntegrityContractV1 {
			return nil, fmt.Errorf("storage_integrity.tables requires a storage-integrity contract v1 capable SQL rewriter; refusing fail-open startup")
		}
		// Contract v1 proves only that the backend understood the request; old
```

with:

```go
		if !ok || capable.StorageIntegrityContractVersion() != rewriter.StorageIntegrityContractV2 {
			return nil, fmt.Errorf("storage_integrity.tables requires a storage-integrity contract V2 capable SQL rewriter; refusing fail-open startup")
		}
		// Contract V2 proves only that the backend understood the request; old
```

In `build.go`, replace:

```go
			rewritePlug.RequiredStorageIntegrityContractVersion = rewriter.StorageIntegrityContractV1
```

with:

```go
			rewritePlug.RequiredStorageIntegrityContractVersion = rewriter.StorageIntegrityContractV2
```

- [ ] **Step 6: Run the unit tests**

Run: `go test -vet=off -count=1 ./pkg/rewriter/ ./pkg/plugins/rewrite/ . && bazel test //pkg/rewriter:rewriter_test //pkg/plugins/rewrite:rewrite_test //:housegate_test`

Expected: PASS for all three packages.

- [ ] **Step 7: Run the probe against the new native engine**

Run:

```bash
export POLYGLOT_SQL_FFI_PATH="$(go run ./cmd fetch-rewriter-lib --tag "$REWRITER_GO_TAG" | tail -n 1)"
go test -vet=off -count=1 -run 'TestNativeEngineSmoke|TestNativeEngineProbeSmoke' -v ./pkg/rewriter/
```

Expected: both tests `PASS` and neither `SKIP`s. (Measured with the old v0.11.0 library instead: `TestNativeEngineProbeSmoke` fails with `storage-integrity engine probe (engine=native probe=describe-fingerprint): contract acknowledgement STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED, want STORAGE_INTEGRITY_CONTRACT_V2; deploy rewriter-go >= v0.13.0 or rewriter-grpc >= v0.15.0 (storage-integrity contract V2)`.)

- [ ] **Step 8: Move the FFI tag and the documented floors**

Every file that names the FFI release tag moves to `$REWRITER_GO_TAG`; historical plans, specs and the tier-2 measurement record keep theirs (upgrade-dependency Step 4). The CLAUDE.md edits are scripted because the tag, polyglot version and hashes come from the release:

```bash
python3 - <<'EOF'
import os, re
tag, grpc, poly = os.environ["REWRITER_GO_TAG"], os.environ["REWRITER_GRPC_TAG"], os.environ["POLYGLOT_VERSION"]
linux, darwin = os.environ["FFI_SHA256_LINUX"], os.environ["FFI_SHA256_DARWIN"]
p = "CLAUDE.md"
s = open(p).read()
old_suite = "Startup requires every concrete or injected factory to implement `rewriter.StorageIntegrityProbeFactory` and pass one bounded behavioral suite: the exact DESCRIBE fingerprint (`rewriter.StorageIntegrityProbeExpectedSQL`), the generic `SYSTEM RELOAD CONFIG` catch-all, the protected physical target in `SYSTEM START MERGES hg_unsafe.db1__t`, the `TRUNCATE DATABASE hg_safe` D2 invariant, and the Spec N tagged heredoc `SELECT * FROM merge($tag$hg_safe$tag$, 'db1__t')`. Every response must carry the contract-v1 acknowledgement and match its exact code/type/SQL/message; v0.10.0 native and v0.13.1 gRPC are the minimum released builds, and the heredoc case is the only one of the five that discriminates them — the other four are also satisfied by the pre-Spec-N builds."
new_suite = f"Startup requires every concrete or injected factory to implement `rewriter.StorageIntegrityProbeFactory` and pass one bounded behavioral suite of seven cases: the exact DESCRIBE fingerprint (`rewriter.StorageIntegrityProbeExpectedSQL`), the generic `SYSTEM RELOAD CONFIG` catch-all, the protected physical target in `SYSTEM START MERGES hg_unsafe.db1__t`, the `TRUNCATE DATABASE hg_safe` D2 invariant, the Spec N tagged heredoc `SELECT * FROM merge($tag$hg_safe$tag$, 'db1__t')`, and the two contract-V2 cases: `DROP TABLE db1.t` rewritten to drop only the ordinary physical table (pinned per engine, `rewriter.StorageIntegrityProbeDropExpectedSQLNative` / `rewriter.StorageIntegrityProbeDropExpectedSQLGRPC`, because the engines quote identifiers differently) and `SYSTEM RELOAD CONFIG` refused under an empty table map. Every response must carry the contract-V2 acknowledgement and match its exact code/type/SQL/message; rewriter-go {tag} native and rewriter-grpc {grpc} are the minimum released builds, and the two V2 cases are what refuse every V1-only build."
assert s.count(old_suite) == 1
s = s.replace(old_suite, new_suite)
old_floor = "The native engine requires an FFI library built from rewriter-go >= v0.11.0 (polyglot >= v0.10.0 — the go.mod floor)"
assert s.count(old_floor) == 1
s = s.replace(old_floor, f"The native engine requires an FFI library built from rewriter-go >= {tag} (polyglot >= {poly} — the go.mod floor)")
pin = re.search(r"- \*\*Current ordinary native FFI release pin\.\*\* `rewriter-go` v0\.11\.0 publishes .*?both built from Polyglot v0\.10\.0 and carrying the ABI required by its Go binding\.", s)
assert pin
s = s.replace(pin.group(0), f"- **Current ordinary native FFI release pin.** `rewriter-go` {tag} publishes [linux/amd64](https://github.com/housegate/rewriter-go/releases/download/{tag}/libpolyglot_sql_ffi-linux-x86_64.so) (`sha256:{linux}`) and [darwin/arm64](https://github.com/housegate/rewriter-go/releases/download/{tag}/libpolyglot_sql_ffi-macos-arm64.dylib) (`sha256:{darwin}`), both built from Polyglot {poly} and carrying the ABI required by its Go binding and the storage-integrity contract V2.")
open(p, "w").write(s)
EOF
sed -i.bak "s/--tag v0\.11\.0/--tag $REWRITER_GO_TAG/" .github/workflows/ci.yml pkg/integration/storage_integrity_read_test.go
sed -i.bak "s/native_library_release: v0\.11\.0/native_library_release: $REWRITER_GO_TAG/" configs/local.server.yaml configs/local.server-mock-remote.yaml
rm -f .github/workflows/ci.yml.bak pkg/integration/storage_integrity_read_test.go.bak configs/*.bak
git grep -n 'v0\.11\.0' -- ':!docs/superpowers' ':!docs/issue-153-tier2-measurement.md' ':!go.sum'   # expect no output
```

In `README.md`, replace:

```markdown
With no `storage_integrity.tables`, a backend error or `UnsupportedStatement` falls back to the original SQL. Configured SI membership requires a contract-v1-capable backend at startup and fails closed on every untrustworthy response.
```

with:

```markdown
With no `storage_integrity.tables`, a backend error or `UnsupportedStatement` falls back to the original SQL. Configured SI membership requires a contract-V2-capable backend (rewriter-go v0.13.0+ native, rewriter-grpc v0.15.0+) at startup and fails closed on every untrustworthy response.
```

In `README.md`, replace:

```markdown
- SI requests require the rewriter's exact contract-v1 acknowledgement.
```

with:

```markdown
- SI requests require the rewriter's exact contract-V2 acknowledgement.
```

In `pkg/rewriter/AGENTS.md`, replace:

```markdown
Builds contract-v1 args, validates acknowledgements
```

with:

```markdown
Builds contract-V2 args, validates acknowledgements
```

In `pkg/rewriter/AGENTS.md`, replace:

```markdown
reports exact contract-v1 capability
```

with:

```markdown
reports exact contract-V2 capability
```

In `pkg/rewriter/AGENTS.md`, replace:

```markdown
do not relax the existing SI contract-v1, SI behavioral-probe
```

with:

```markdown
do not relax the existing SI contract-V2, SI behavioral-probe
```

- [ ] **Step 9: Run the whole unit suite**

Run: `bazel test //...`

Expected: every target PASSES.

- [ ] **Step 10: Commit**

```bash
git add go.mod go.sum MODULE.bazel.lock pkg/rewriter/ pkg/plugins/rewrite/rewriter_test.go build.go build_test.go .github/workflows/ci.yml configs/ pkg/integration/storage_integrity_read_test.go CLAUDE.md README.md
git commit -m "$(cat <<'EOF'
feat(rewriter): send and require storage-integrity contract V2

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `pkg/sitable`: the table-state port, `Static`, and a switchable `Fake`

**Files:**

- Create: `pkg/sitable/sitable.go`, `pkg/sitable/static.go`, `pkg/sitable/fake.go`, `pkg/sitable/BUILD.bazel` (gazelle)
- Test: create `pkg/sitable/sitable_test.go`, `pkg/config/sitable_names_test.go`; `pkg/config/BUILD.bazel` (gazelle)

**Interfaces:**

- Consumes: `payloadexec.TableSchema`, `payloadexec.TableSchemaHash(networkID, schema)`.
- Produces (package `sitable`, spec §5 verbatim plus helpers):

```go
type Status uint8 // Ordinary, Pending, Refused, Active, Gone
func (s Status) String() string                 // "ordinary" | "pending" | "refused" | "active" | "gone"
func ParseStatus(name string) (Status, bool)
type Table struct { ID string; Status Status; RefusedCode, RefusedReason string; Schema payloadexec.TableSchema; SchemaHash string }
type Snapshot interface { Version() uint64; Lookup(database, table string) Table; Active() []Table; Schema(id string) (Table, bool) }
type TableState interface { Current() Snapshot; Changed() <-chan struct{} }
const SafeDatabase, UnsafeDatabase, PromoteDatabase = "hg_safe", "hg_unsafe", "hg_promote"
func ReservedDatabases() []string // [SafeDatabase, UnsafeDatabase, PromoteDatabase], a fresh slice
func PhysicalTable(id string) string
func TableID(database, table string) string
func NewSnapshot(version uint64, fallback Status, tables []Table) Snapshot
const StaticVersion uint64 = 1
func NewStatic(tableIDs []string, schemas map[string]payloadexec.TableSchema, networkID string) *Static
func (s *Static) TableIDs() []string
func (s *Static) Schemas() []payloadexec.TableSchema
func NewFake(fallback Status, tables ...Table) *Fake
func (f *Fake) Set(tables ...Table) uint64
```

`Changed()` returns a channel closed at the next version change after the call; `Static.Changed()` never fires. The database names repeat `config.StorageIntegritySafeDatabase` / `UnsafeDatabase` / `PromoteDatabase` because `pkg/config` imports `pkg/rewriter`, which imports this package; `pkg/config/sitable_names_test.go` pins the equality, so the rewriter takes the reserved-database list from here (Task 4) without scattering literals.

- [ ] **Step 1: Write the failing tests**

Create `pkg/sitable/sitable_test.go`:

```go
package sitable

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func testSchema(id string) payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: id, Columns: []lthash.Column{{Name: "a", Type: "UInt32"}}}
}

func TestStaticSnapshot(t *testing.T) {
	schemas := map[string]payloadexec.TableSchema{"db1.t": testSchema("db1.t")}
	st := NewStatic([]string{"db1.u", "db1.t"}, schemas, "net")
	snap := st.Current()
	if snap.Version() != StaticVersion {
		t.Fatalf("version = %d, want %d", snap.Version(), StaticVersion)
	}
	got := snap.Lookup("db1", "t")
	if got.Status != Active || got.ID != "db1.t" || got.SchemaHash != payloadexec.TableSchemaHash("net", testSchema("db1.t")) {
		t.Fatalf("Lookup(db1, t) = %+v", got)
	}
	if other := snap.Lookup("db1", "other"); other.Status != Ordinary || other.ID != "db1.other" {
		t.Fatalf("unlisted table = %+v, want Ordinary", other)
	}
	var ids []string
	for _, table := range snap.Active() {
		ids = append(ids, table.ID)
	}
	if !reflect.DeepEqual(ids, []string{"db1.t", "db1.u"}) {
		t.Fatalf("Active ids = %v, want sorted [db1.t db1.u]", ids)
	}
	if _, ok := snap.Schema("db1.other"); ok {
		t.Fatal("Schema of an Ordinary table must miss")
	}
	if table, ok := snap.Schema("db1.u"); !ok || table.SchemaHash != "" {
		t.Fatalf("a listed table without a startup schema is Active with an empty hash, got %+v ok=%v", table, ok)
	}
	if got := st.Schemas(); len(got) != 1 || got[0].TableID != "db1.t" {
		t.Fatalf("Schemas() = %+v", got)
	}
	select {
	case <-st.Changed():
		t.Fatal("Static.Changed must never fire")
	default:
	}
}

func TestFakeSwitchesVersionsAndWakes(t *testing.T) {
	f := NewFake(Pending, Table{ID: "db1.t", Status: Pending})
	first := f.Current()
	wake := f.Changed()
	if v := f.Set(Table{ID: "db1.t", Status: Active, Schema: testSchema("db1.t")}); v != 2 {
		t.Fatalf("Set returned version %d, want 2", v)
	}
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("Set must close the Changed channel handed out before it")
	}
	if first.Version() != 1 || first.Lookup("db1", "t").Status != Pending {
		t.Fatalf("an earlier snapshot must stay immutable: v=%d %+v", first.Version(), first.Lookup("db1", "t"))
	}
	second := f.Current()
	if second.Version() != 2 || second.Lookup("db1", "t").Status != Active {
		t.Fatalf("new snapshot = v%d %+v", second.Version(), second.Lookup("db1", "t"))
	}
	if unknown := second.Lookup("db2", "x"); unknown.Status != Pending {
		t.Fatalf("fallback = %v, want Pending", unknown.Status)
	}
	f.Set(Table{ID: "db1.t", Status: Gone, Schema: testSchema("db1.t")})
	if table, ok := f.Current().Schema("db1.t"); !ok || table.Status != Gone {
		t.Fatalf("Schema must answer a Gone table, got %+v ok=%v", table, ok)
	}
	if len(f.Current().Active()) != 0 {
		t.Fatal("a Gone table is not Active")
	}
}

func TestStatusNamesRoundTrip(t *testing.T) {
	for _, s := range []Status{Ordinary, Pending, Refused, Active, Gone} {
		got, ok := ParseStatus(s.String())
		if !ok || got != s {
			t.Fatalf("ParseStatus(%q) = %v, %v", s.String(), got, ok)
		}
	}
	if _, ok := ParseStatus("retiring"); ok {
		t.Fatal("unknown names must not parse")
	}
}

func TestReservedDatabasesIsACopy(t *testing.T) {
	got := ReservedDatabases()
	if strings.Join(got, ",") != "hg_safe,hg_unsafe,hg_promote" {
		t.Fatalf("ReservedDatabases() = %v", got)
	}
	got[0] = "mutated"
	if ReservedDatabases()[0] != SafeDatabase {
		t.Fatal("ReservedDatabases must return a fresh slice")
	}
}
```

Create `pkg/config/sitable_names_test.go`:

```go
package config

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/sitable"
)

// TestSitablePhysicalNamesMatchConfig pins the D2 names pkg/sitable repeats
// because a leaf package cannot import pkg/config.
func TestSitablePhysicalNamesMatchConfig(t *testing.T) {
	want := []string{StorageIntegritySafeDatabase, StorageIntegrityUnsafeDatabase, StorageIntegrityPromoteDatabase}
	if got := sitable.ReservedDatabases(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sitable.ReservedDatabases() = %v drifted from config %v", got, want)
	}
	if sitable.SafeDatabase != StorageIntegritySafeDatabase || sitable.UnsafeDatabase != StorageIntegrityUnsafeDatabase || sitable.PromoteDatabase != StorageIntegrityPromoteDatabase {
		t.Fatalf("sitable databases %q/%q/%q drifted from config", sitable.SafeDatabase, sitable.UnsafeDatabase, sitable.PromoteDatabase)
	}
	for _, id := range []string{"db1.t", "a_b.c_d"} {
		if sitable.PhysicalTable(id) != StorageIntegrityPhysicalTable(id) {
			t.Fatalf("PhysicalTable(%q) = %q, config = %q", id, sitable.PhysicalTable(id), StorageIntegrityPhysicalTable(id))
		}
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -count=1 ./pkg/sitable/ ./pkg/config/`

Expected: FAIL, build failed: `undefined: NewStatic`, `undefined: StaticVersion`, `undefined: NewFake` in `pkg/sitable`, and `pkg/config` cannot build because `pkg/sitable` has no non-test Go files (once it has, `undefined: sitable.ReservedDatabases`).

- [ ] **Step 3: Write the package**

Create `pkg/sitable/sitable.go`:

```go
// Package sitable is the storage-integrity table-state port (spec
// 2026-09-24 §5). A host hands HouseGate versioned, immutable snapshots of
// every table's status; HouseGate takes exactly one snapshot per query and
// every stage of that query reads it. The package holds only types and the
// static implementation and imports no proxy package.
package sitable

import (
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Status is a table's storage-integrity status as the host judges it.
type Status uint8

const (
	// Ordinary is not governed: another indexer, Legacy, or registry not enabled.
	Ordinary Status = iota
	// Pending includes default deny: on the SI indexer and not recorded.
	Pending
	// Refused carries the arbiter's refused_code and refused_reason.
	Refused
	// Active is served from hg_safe / hg_unsafe and written through the signed lane.
	Active
	// Gone means the newest incarnation is Retiring or Purging.
	Gone
)

// String returns the lowercase wire name used by the JSON-RPC contract.
func (s Status) String() string {
	switch s {
	case Ordinary:
		return "ordinary"
	case Pending:
		return "pending"
	case Refused:
		return "refused"
	case Active:
		return "active"
	case Gone:
		return "gone"
	default:
		return "unknown"
	}
}

// ParseStatus maps a JSON-RPC status name back to a Status.
func ParseStatus(name string) (Status, bool) {
	switch name {
	case "ordinary":
		return Ordinary, true
	case "pending":
		return Pending, true
	case "refused":
		return Refused, true
	case "active":
		return Active, true
	case "gone":
		return Gone, true
	default:
		return Ordinary, false
	}
}

// Table is one table's status in a snapshot.
type Table struct {
	ID            string // "<database>.<table>", logical
	Status        Status
	RefusedCode   string // Refused only; the arbiter's refused_code vocabulary
	RefusedReason string
	Schema        payloadexec.TableSchema // set for Active and Gone
	SchemaHash    string
}

// Snapshot is immutable; every method is in-memory and performs no I/O.
type Snapshot interface {
	Version() uint64
	Lookup(database, table string) Table // answers for any table
	Active() []Table                     // sorted by ID
	Schema(id string) (Table, bool)      // Active or Gone
}

// TableState hands out snapshots. Changed returns a channel that is closed at
// the next version change after the call; callers call Changed again after
// every wake.
type TableState interface {
	Current() Snapshot
	Changed() <-chan struct{}
}

// Protocol-owned storage-integrity databases (Spec C D2 naming freeze). They
// equal config.StorageIntegritySafeDatabase / UnsafeDatabase /
// PromoteDatabase; this leaf package cannot import pkg/config (which imports
// pkg/rewriter), and a pkg/config test pins the equality.
const (
	SafeDatabase    = "hg_safe"
	UnsafeDatabase  = "hg_unsafe"
	PromoteDatabase = "hg_promote"
)

// ReservedDatabases returns the protocol-owned databases HouseGate sends as
// StorageIntegrityArgs.reserved_databases on every enabled request, so the
// engines protect them even while no table is Active.
func ReservedDatabases() []string {
	return []string{SafeDatabase, UnsafeDatabase, PromoteDatabase}
}

// PhysicalTable maps a logical table id to its hg_safe / hg_unsafe table name
// (the same rule as storageintegrity.PhysicalTableName).
func PhysicalTable(id string) string {
	return strings.ReplaceAll(id, ".", "__")
}

// TableID joins a logical database and table into the snapshot id.
func TableID(database, table string) string {
	return database + "." + table
}

type tableKey struct {
	database string
	table    string
}

type snapshot struct {
	version  uint64
	fallback Status
	byKey    map[tableKey]Table
	byID     map[string]Table
	active   []Table
}

// NewSnapshot builds an immutable snapshot. Every table not listed answers
// fallback. The database/table split of each entry is taken from its ID at the
// first dot, so IDs must be "<database>.<table>" with a dot-free database.
func NewSnapshot(version uint64, fallback Status, tables []Table) Snapshot {
	s := &snapshot{
		version:  version,
		fallback: fallback,
		byKey:    make(map[tableKey]Table, len(tables)),
		byID:     make(map[string]Table, len(tables)),
	}
	for _, t := range tables {
		database, table, _ := strings.Cut(t.ID, ".")
		s.byKey[tableKey{database: database, table: table}] = t
		s.byID[t.ID] = t
		if t.Status == Active {
			s.active = append(s.active, t)
		}
	}
	sort.Slice(s.active, func(i, j int) bool { return s.active[i].ID < s.active[j].ID })
	return s
}

func (s *snapshot) Version() uint64 { return s.version }

func (s *snapshot) Lookup(database, table string) Table {
	if t, ok := s.byKey[tableKey{database: database, table: table}]; ok {
		return t
	}
	return Table{ID: TableID(database, table), Status: s.fallback}
}

func (s *snapshot) Active() []Table {
	return append([]Table(nil), s.active...)
}

func (s *snapshot) Schema(id string) (Table, bool) {
	t, ok := s.byID[id]
	if !ok || (t.Status != Active && t.Status != Gone) {
		return Table{}, false
	}
	return t, true
}

// neverChanged is shared by every state whose version never moves.
var neverChanged = make(chan struct{})
```

Create `pkg/sitable/static.go`:

```go
package sitable

import "github.com/housegate/housegate/pkg/replay/payloadexec"

// StaticVersion is the constant version of every Static snapshot.
const StaticVersion uint64 = 1

// Static is the TableState of a fixed configured table set: every listed
// table is Active, every other table is Ordinary, the version is always
// StaticVersion and Changed never fires. It serves the standalone binary,
// tests, and every host that does not inject a TableState.
type Static struct {
	snap Snapshot
	ids  []string
}

// NewStatic builds the static state. schemas carries the schemas loaded at
// startup, keyed by table id; a listed table without one stays Active with a
// zero schema, and the signed lane refuses writes to it.
func NewStatic(tableIDs []string, schemas map[string]payloadexec.TableSchema, networkID string) *Static {
	tables := make([]Table, 0, len(tableIDs))
	for _, id := range tableIDs {
		t := Table{ID: id, Status: Active}
		if schema, ok := schemas[id]; ok {
			t.Schema = schema
			t.SchemaHash = payloadexec.TableSchemaHash(networkID, schema)
		}
		tables = append(tables, t)
	}
	return &Static{snap: NewSnapshot(StaticVersion, Ordinary, tables), ids: append([]string(nil), tableIDs...)}
}

func (s *Static) Current() Snapshot { return s.snap }

func (s *Static) Changed() <-chan struct{} { return neverChanged }

// TableIDs returns the configured ids in configuration order.
func (s *Static) TableIDs() []string { return append([]string(nil), s.ids...) }

// Schemas returns the startup schemas of the listed tables that have one.
func (s *Static) Schemas() []payloadexec.TableSchema {
	var out []payloadexec.TableSchema
	for _, id := range s.ids {
		if t, ok := s.snap.Schema(id); ok && t.SchemaHash != "" {
			out = append(out, t.Schema)
		}
	}
	return out
}

var _ TableState = (*Static)(nil)
```

Create `pkg/sitable/fake.go`:

```go
package sitable

import "sync"

// Fake is a TableState whose version tests switch explicitly. Every Set
// publishes a new snapshot with the next version and wakes every Changed
// waiter. It is safe for concurrent use.
type Fake struct {
	mu       sync.Mutex
	fallback Status
	version  uint64
	snap     Snapshot
	changed  chan struct{}
}

// NewFake starts at version 1 with the given tables; every unlisted table
// answers fallback.
func NewFake(fallback Status, tables ...Table) *Fake {
	return &Fake{
		fallback: fallback,
		version:  1,
		snap:     NewSnapshot(1, fallback, tables),
		changed:  make(chan struct{}),
	}
}

// Set replaces the table set, bumps the version and returns it.
func (f *Fake) Set(tables ...Table) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.version++
	f.snap = NewSnapshot(f.version, f.fallback, tables)
	close(f.changed)
	f.changed = make(chan struct{})
	return f.version
}

func (f *Fake) Current() Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap
}

func (f *Fake) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed
}

var _ TableState = (*Fake)(nil)
```

- [ ] **Step 4: Generate BUILD files and run the tests**

Run: `bazel run //:gazelle && go test -count=1 ./pkg/sitable/ ./pkg/config/ && bazel test //pkg/sitable:sitable_test //pkg/config:config_test`

Expected: PASS; gazelle creates `pkg/sitable/BUILD.bazel` with `go_library` `//pkg/sitable` (deps `//pkg/replay/payloadexec`) and `go_test` `sitable_test`, and adds `//pkg/sitable` to `config_test`.

- [ ] **Step 5: Commit**

```bash
git add pkg/sitable/ pkg/config/sitable_names_test.go pkg/config/BUILD.bazel
git commit -m "$(cat <<'EOF'
feat(sitable): add the storage-integrity table-state port and its static implementation

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: The explicit `storage_integrity.enabled` switch

**Files:**

- Modify: `pkg/config/storage_integrity_config.go:57-65` (field), `:223` (add `IsEnabled`), `:223-236` (`validate` head), `:261-263`, `:310-312`
- Modify: `README.md:185-208` (the `storage_integrity` section and its example), `configs/local.server.yaml:56-62`
- Test: create `pkg/config/storage_integrity_enabled_test.go`; modify `pkg/config/storage_integrity_config_test.go:279-297,363-368`; `pkg/config/BUILD.bazel` (gazelle)

**Interfaces:**

- Produces (package `config`):

```go
type StorageIntegrityConfig struct {
	Enabled *bool `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	// ... existing fields unchanged
}
func (c StorageIntegrityConfig) IsEnabled() bool // explicit value, else len(Tables) > 0
```

Validation (spec §6.1): explicit `false` with `tables` is an error; `enabled` without `tables` outside server mode is an error; `read.default_mode` and `runtime.enabled` now require `IsEnabled()` instead of `tables`. The "exactly one table-set source" rule needs `Options`, so it is enforced at build time in Task 4 (`resolveStorageIntegrityTableState`).

- [ ] **Step 1: Write the failing tests**

Create `pkg/config/storage_integrity_enabled_test.go`:

```go
package config

import (
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func boolPtr(v bool) *bool { return &v }

func TestStorageIntegrityEnabledDefaultsToTables(t *testing.T) {
	var c StorageIntegrityConfig
	if c.IsEnabled() {
		t.Fatal("no tables and no switch must be disabled")
	}
	c.Tables = []string{"db1.t"}
	if !c.IsEnabled() {
		t.Fatal("configured tables with no switch must be enabled")
	}
	c.Enabled = boolPtr(false)
	if c.IsEnabled() {
		t.Fatal("an explicit false wins")
	}
	c.Tables = nil
	c.Enabled = boolPtr(true)
	if !c.IsEnabled() {
		t.Fatal("an explicit true with no tables is enabled (injected TableState)")
	}
}

func TestStorageIntegrityEnabledYAML(t *testing.T) {
	var c StorageIntegrityConfig
	if err := yaml.Unmarshal([]byte("enabled: true\n"), &c); err != nil {
		t.Fatal(err)
	}
	if c.Enabled == nil || !*c.Enabled {
		t.Fatalf("enabled: true decoded as %v", c.Enabled)
	}
	var absent StorageIntegrityConfig
	if err := yaml.Unmarshal([]byte("tables: [db1.t]\n"), &absent); err != nil {
		t.Fatal(err)
	}
	if absent.Enabled != nil || !absent.IsEnabled() {
		t.Fatalf("an absent switch must stay nil and default from tables, got %v", absent.Enabled)
	}
}

func TestStorageIntegrityEnabledValidation(t *testing.T) {
	t.Run("explicit false with tables is rejected", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Enabled = boolPtr(false)
		cfg.StorageIntegrity.Tables = []string{"db1.t"}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "storage_integrity.tables requires storage_integrity.enabled (it is explicitly false)") {
			t.Fatalf("Validate err = %v", err)
		}
	})
	t.Run("explicit true without tables is valid in server mode", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Enabled = boolPtr(true)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate err = %v", err)
		}
	})
	t.Run("explicit true is server mode only", func(t *testing.T) {
		cfg := Default()
		cfg.Agent = agentConfigForStorageIntegrityTest()
		cfg.StorageIntegrity.Enabled = boolPtr(true)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "storage_integrity.enabled is server mode only") {
			t.Fatalf("Validate err = %v", err)
		}
	})
	t.Run("read.default_mode requires enabled", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Read.DefaultMode = "safe"
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "storage_integrity.read.default_mode requires storage_integrity.enabled") {
			t.Fatalf("Validate err = %v", err)
		}
		cfg.StorageIntegrity.Enabled = boolPtr(true)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("an explicit switch satisfies read.default_mode: %v", err)
		}
	})
	t.Run("runtime accepts an explicit switch without tables", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Tables = nil
		cfg.StorageIntegrity.Enabled = boolPtr(true)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate err = %v", err)
		}
	})
}
```

In `pkg/config/storage_integrity_config_test.go`, replace:

```go
			"storage_integrity.runtime.payload_spool_dir",
			"storage_integrity.tables",
		} {
```

with:

```go
			"storage_integrity.runtime.payload_spool_dir",
			"storage_integrity.enabled",
		} {
```

In `pkg/config/storage_integrity_config_test.go`, replace:

```go
	t.Run("runtime enabled requires storage_integrity.tables", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Tables = nil
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.tables is required when storage_integrity.runtime.enabled") {
```

with:

```go
	t.Run("runtime enabled requires storage_integrity.enabled", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Tables = nil
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.enabled is required when storage_integrity.runtime.enabled") {
```

In `pkg/config/storage_integrity_config_test.go`, replace:

```go
"storage_integrity.read.default_mode requires storage_integrity.tables"
```

with:

```go
"storage_integrity.read.default_mode requires storage_integrity.enabled"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -count=1 ./pkg/config/`

Expected: FAIL, build failed: `cfg.StorageIntegrity.Enabled undefined` and `c.IsEnabled undefined`.

- [ ] **Step 3: Add the switch**

In `pkg/config/storage_integrity_config.go`, replace:

```go
type StorageIntegrityConfig struct {
	// Tables is the explicit SI membership (Spec G D4): logical
```

with:

```go
type StorageIntegrityConfig struct {
	// Enabled is the explicit storage-integrity switch (spec 2026-09-24 §6).
	// Nil defaults to len(Tables) > 0, so every existing config keeps its
	// meaning. A host that injects Options.StorageIntegrityTableState sets it
	// to true explicitly. Read it through IsEnabled.
	Enabled *bool `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	// Tables is the explicit SI membership (Spec G D4): logical
```

In `pkg/config/storage_integrity_config.go`, replace:

```go
func (c StorageIntegrityConfig) validate(mode Mode) error {
	var errs []error

```

with:

```go
// IsEnabled reports the effective storage-integrity switch: the explicit
// value when set, otherwise len(Tables) > 0.
func (c StorageIntegrityConfig) IsEnabled() bool {
	if c.Enabled != nil {
		return *c.Enabled
	}
	return len(c.Tables) > 0
}

func (c StorageIntegrityConfig) validate(mode Mode) error {
	var errs []error
	if c.Enabled != nil && !*c.Enabled && len(c.Tables) > 0 {
		errs = append(errs, errors.New("storage_integrity.tables requires storage_integrity.enabled (it is explicitly false)"))
	}
	if c.IsEnabled() && len(c.Tables) == 0 && mode != ModeServer {
		errs = append(errs, errors.New("storage_integrity.enabled is server mode only"))
	}

```

In `pkg/config/storage_integrity_config.go`, replace:

```go
	if c.Read.DefaultMode != "" && len(c.Tables) == 0 {
		errs = append(errs, errors.New("storage_integrity.read.default_mode requires storage_integrity.tables"))
	}
```

with:

```go
	if c.Read.DefaultMode != "" && !c.IsEnabled() {
		errs = append(errs, errors.New("storage_integrity.read.default_mode requires storage_integrity.enabled"))
	}
```

In `pkg/config/storage_integrity_config.go`, replace:

```go
		if len(c.Tables) == 0 {
			errs = append(errs, errors.New("storage_integrity.tables is required when storage_integrity.runtime.enabled"))
		}
```

with:

```go
		if !c.IsEnabled() {
			errs = append(errs, errors.New("storage_integrity.enabled is required when storage_integrity.runtime.enabled"))
		}
```

- [ ] **Step 4: Run the config tests**

Run: `bazel run //:gazelle && go test -count=1 ./pkg/config/ && bazel test //pkg/config:config_test`

Expected: PASS.

- [ ] **Step 5: Document the switch**

In `README.md`, replace:

```markdown
`storage_integrity.tables` is the shared logical membership list. HouseGate derives both guarded physical homes from each `<database>.<table>` id; operators must not configure `runtime.merge_guard.tables` separately. An empty `read.default_mode` has the same safe behavior as `safe`.
```

with:

```markdown
`storage_integrity.enabled` switches storage integrity on. It defaults to true exactly when `storage_integrity.tables` is non-empty, so existing configs keep their meaning. An enabled server needs exactly one table-set source: the static `storage_integrity.tables` list, or a table-state port injected by the embedding host through `Options.StorageIntegrityTableState` (then set `enabled: true` explicitly and leave `tables` empty). `read.default_mode` and `runtime` require `enabled`.

`storage_integrity.tables` is the static logical membership list: every listed table is Active and every other table is ordinary. HouseGate derives both guarded physical homes from each `<database>.<table>` id; operators must not configure `runtime.merge_guard.tables` separately. An empty `read.default_mode` has the same safe behavior as `safe`.
```

In `README.md`, replace:

````markdown
```yaml
storage_integrity:
  tables: ["tenant.events"]        # logical <db>.<table> ids; hg_unsafe/hg_safe.tenant__events are derived
````

with:

````markdown
```yaml
storage_integrity:
  # enabled: true                  # default: true when tables is non-empty; set it with an injected TableState and no tables
  tables: ["tenant.events"]        # logical <db>.<table> ids; hg_unsafe/hg_safe.tenant__events are derived
````

In `configs/local.server.yaml`, replace:

```yaml
# storage_integrity:
#   tables: ["tenant.events"]        # logical <db>.<table> ids; hg_unsafe/hg_safe.tenant__events are derived
```

with:

```yaml
# storage_integrity:
#   enabled: true                    # default: true when tables is non-empty; an embedding host that injects a TableState sets it and omits tables
#   tables: ["tenant.events"]        # logical <db>.<table> ids; hg_unsafe/hg_safe.tenant__events are derived
```

- [ ] **Step 6: Commit**

```bash
git add pkg/config/ README.md configs/local.server.yaml
git commit -m "$(cat <<'EOF'
feat(config): add the explicit storage_integrity.enabled switch

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: One snapshot per query through the rewriter: dynamic arguments with the reserved databases, accessed-table promoted parts, a per-version scrubber, the reserved-database probe, and the build wiring

**Files:**

- Modify: `pkg/rewriter/storage_integrity.go` (whole file), `pkg/rewriter/sentio.go:21,247-251,262-395,437-466`, `pkg/rewriter/probe.go` (Task 1 version: imports, the reserved-database constants, `storageIntegrityProbeArgs`, an eighth probe)
- Modify: `pkg/plugin/context.go:17-21,101` (`QueryContext.TableSnapshot`), `pkg/plugins/rewrite/rewriter.go:17-19,64-82,117-121,243-248,265-291`
- Create: `storage_integrity_table_state.go`
- Modify: `proxy.go:117` (`Options.StorageIntegrityTableState`), `build.go:42-49,111-152,440-497,601-613,716-727`
- Modify: `CLAUDE.md` §4 (three paragraphs), `README.md:170,193`, `pkg/rewriter/AGENTS.md:25,33`
- Test: create `pkg/rewriter/table_state_test.go`, `storage_integrity_table_state_test.go`; replace `pkg/rewriter/storage_integrity_test.go` (whole file); modify `pkg/rewriter/probe_test.go` (Task 1 version), `pkg/rewriter/backend_test.go:293-296`, `pkg/plugins/rewrite/rewriter_test.go:13-16,329-349` and append, `build_test.go:32-34,105-127,138-156,342,357,245`

**Interfaces:**

- Consumes: Task 2 `sitable.*` (including `sitable.ReservedDatabases()`); Task 3 `config.StorageIntegrityConfig.IsEnabled()`; Task 1 `rewriter.StorageIntegrityContractV2` and the pinned rewriter-proto's `StorageIntegrityArgs.ReservedDatabases []string` (proto `repeated string reserved_databases = 5`, plan A handoff item 1).
- Produces (package `rewriter`):

```go
type StorageIntegrityOptions struct {
	Enabled           bool
	TableState        sitable.TableState
	DefaultReadMode   ReadMode
	ReadState         StorageIntegrityReadState
	InsertLaneEnabled bool
} // StorageIntegrityTable and the Tables field are removed
func WithTableSnapshot(ctx context.Context, snap sitable.Snapshot) context.Context
func TableSnapshotFromContext(ctx context.Context) (sitable.Snapshot, bool)
func NewStorageIntegrityScrubber(snap sitable.Snapshot) *StorageIntegrityScrubber // never nil
type StorageIntegrityScrubberCache struct{ /* unexported */ }
func (c *StorageIntegrityScrubberCache) For(snap sitable.Snapshot) *StorageIntegrityScrubber
func (c *StorageIntegrityScrubberCache) Builds() int
```

Every enabled request's `StorageIntegrityArgs` carries `ReservedDatabases: sitable.ReservedDatabases()`, even with an empty table map, and so does every startup probe request; the probe gains an eighth case, `v2-empty-map-reserved-database`.

- Produces (package `plugin`): `QueryContext.TableSnapshot sitable.Snapshot`.
- Produces (package `rewrite`): `Plugin.TableState sitable.TableState` (replaces `StorageIntegrityScrubber`).
- Produces (package `housegate`):

```go
type Options struct { /* ... */ StorageIntegrityTableState sitable.TableState }
func resolveStorageIntegrityTableState(opts Options, reg registry.Registry) (sitable.TableState, *sitable.Static, error)
func staticStorageIntegritySchemas(opts Options, reg registry.Registry) map[string]payloadexec.TableSchema
func storageIntegrityRewriterOptions(cfg *config.Config, rs rewriter.StorageIntegrityReadState, state sitable.TableState) rewriter.StorageIntegrityOptions
func storageIntegrityTableStateLabel(static *sitable.Static) string
```

- [ ] **Step 1: Write the failing rewriter and plugin tests**

Replace the whole of `pkg/rewriter/storage_integrity_test.go` with (options over a table state, the empty-map V2 block, Active-only listing, `promotedPartsFor`, the scrubber from a snapshot and its per-version cache):

```go
package rewriter

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/sitable"
)

type fakeReadState struct {
	parts map[string][]string
	err   error
	calls []string
}

func (f *fakeReadState) PromotedUnsafeParts(tableID string) ([]string, error) {
	f.calls = append(f.calls, tableID)
	if f.err != nil {
		return nil, f.err
	}
	return f.parts[tableID], nil
}

func siOpts(rs StorageIntegrityReadState) StorageIntegrityOptions {
	return StorageIntegrityOptions{
		Enabled:         true,
		TableState:      sitable.NewStatic([]string{"db1.t"}, nil, "net"),
		DefaultReadMode: ReadModeSafe,
		ReadState:       rs,
	}
}

func siSnap() sitable.Snapshot {
	return sitable.NewStatic([]string{"db1.t"}, nil, "net").Current()
}

func TestParseReadMode(t *testing.T) {
	for raw, want := range map[string]ReadMode{"safe": ReadModeSafe, "'unsafe_latest'": ReadModeUnsafeLatest, `" safe "`: ReadModeSafe} {
		got, err := ParseReadMode(raw)
		if err != nil || got != want {
			t.Fatalf("ParseReadMode(%q) = %q, %v", raw, got, err)
		}
	}
	for _, bad := range []string{"", "latest", "SAFE "} {
		if _, err := ParseReadMode(bad); err == nil {
			t.Fatalf("ParseReadMode(%q) must fail", bad)
		}
	}
}

func TestBuildStorageIntegrityArgs(t *testing.T) {
	if got, err := buildStorageIntegrityArgs(StorageIntegrityOptions{}, siSnap(), ReadModeSafe, nil); got != nil || err != nil {
		t.Fatalf("disabled → nil args, got %v %v", got, err)
	}
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0", "all_2_2_0"}}}
	got, err := buildStorageIntegrityArgs(siOpts(rs), siSnap(), ReadModeSafe, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE || got.GetReservedRowIdColumn() != "_hg_row_id" ||
		got.GetContractVersion() != StorageIntegrityContractV2 || len(got.GetReservedDatabases()) != 3 {
		t.Fatalf("args = %v", got)
	}
	tbl := got.GetTables()["db1.t"]
	if tbl.GetSafeTable() != "hg_safe.db1__t" || tbl.GetUnsafeTable() != "hg_unsafe.db1__t" || len(tbl.GetExcludedUnsafeParts()) != 0 {
		t.Fatalf("safe mode table = %v (must not consult the port)", tbl)
	}
	got, err = buildStorageIntegrityArgs(siOpts(rs), siSnap(), ReadModeUnsafeLatest, map[string][]string{"db1.t": {"all_1_1_0", "all_2_2_0"}})
	if err != nil {
		t.Fatal(err)
	}
	if parts := got.GetTables()["db1.t"].GetExcludedUnsafeParts(); len(parts) != 2 || parts[0] != "all_1_1_0" {
		t.Fatalf("excluded = %v", parts)
	}
	if got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST {
		t.Fatalf("mode = %v", got.GetReadMode())
	}
	if got, err := buildStorageIntegrityArgs(siOpts(rs), siSnap(), "", nil); err != nil || got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE {
		t.Fatalf("empty mode must mean safe: %v %v", got, err)
	}
	if len(rs.calls) != 0 {
		t.Fatalf("building args must never call the port: %v", rs.calls)
	}
}

// TestBuildStorageIntegrityArgs_EmptyTableMapStillSendsV2 is spec 2026-09-24
// H6: an enabled deployment with no Active table still sends the contract,
// so the engine's catch-all refuses session SET and SYSTEM.
func TestBuildStorageIntegrityArgs_EmptyTableMapStillSendsV2(t *testing.T) {
	opts := StorageIntegrityOptions{Enabled: true, TableState: sitable.NewFake(sitable.Pending)}
	got, err := buildStorageIntegrityArgs(opts, opts.TableState.Current(), ReadModeSafe, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.GetContractVersion() != StorageIntegrityContractV2 || len(got.GetTables()) != 0 || got.GetTables() == nil {
		t.Fatalf("args = %v, want V2 with an empty (non-nil) table map", got)
	}
	// With no Active table the engines learn the protected databases only
	// from reserved_databases, so they must be sent anyway.
	if strings.Join(got.GetReservedDatabases(), ",") != "hg_safe,hg_unsafe,hg_promote" {
		t.Fatalf("reserved databases = %v, want hg_safe, hg_unsafe, hg_promote", got.GetReservedDatabases())
	}
}

func TestBuildStorageIntegrityArgs_ListsOnlyActiveTables(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary,
		sitable.Table{ID: "db1.a", Status: sitable.Active},
		sitable.Table{ID: "db1.p", Status: sitable.Pending},
		sitable.Table{ID: "db1.r", Status: sitable.Refused},
		sitable.Table{ID: "db1.g", Status: sitable.Gone},
	)
	got, err := buildStorageIntegrityArgs(StorageIntegrityOptions{Enabled: true, TableState: fake}, fake.Current(), ReadModeSafe, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.GetTables()) != 1 || got.GetTables()["db1.a"].GetSafeTable() != "hg_safe.db1__a" {
		t.Fatalf("tables = %v, want only the Active db1.a", got.GetTables())
	}
}

func TestPromotedPartsFor(t *testing.T) {
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}}}
	got, err := promotedPartsFor(rs, []string{"db1.t", "db1.u"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got["db1.t"]) != 1 || strings.Join(rs.calls, ",") != "db1.t,db1.u" {
		t.Fatalf("parts = %v calls = %v, want exactly the accessed tables asked and only non-empty kept", got, rs.calls)
	}
	journalErr := errors.New("journal locked")
	_, err = promotedPartsFor(&fakeReadState{err: journalErr}, []string{"db1.t"})
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "journal locked") || !errors.Is(err, journalErr) {
		t.Fatalf("port error must surface as RejectedError preserving the cause: %v", err)
	}
}

func TestBuildStorageIntegrityArgs_unsafeLatestWithoutPortIsRejected(t *testing.T) {
	_, err := buildStorageIntegrityArgs(siOpts(nil), siSnap(), ReadModeUnsafeLatest, nil)
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "unsafe_latest") {
		t.Fatalf("err = %v, want RejectedError about unsafe_latest", err)
	}
}

func TestReadModeContext(t *testing.T) {
	if _, ok := ReadModeFromContext(context.Background()); ok {
		t.Fatal("empty ctx must report no mode")
	}
	ctx := WithReadMode(context.Background(), ReadModeUnsafeLatest)
	if m, ok := ReadModeFromContext(ctx); !ok || m != ReadModeUnsafeLatest {
		t.Fatalf("got %q %v", m, ok)
	}
}

func TestStorageIntegrityScrubber(t *testing.T) {
	s := NewStorageIntegrityScrubber(siSnap())
	for _, tc := range []struct{ in, want string }{
		{"Table hg_safe.db1__t does not exist", "Table db1.t does not exist"},
		{"Missing columns: '_hg_row_id' while processing hg_unsafe.db1__t",
			"Missing columns: '<storage-integrity>' while processing db1.t"},
		{"Database hg_safe does not exist", "Database <storage-integrity> does not exist"},
		{"Table other.u does not exist", "Table other.u does not exist"},
		{"", ""},
	} {
		if got := s.Scrub(tc.in); got != tc.want {
			t.Errorf("Scrub(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	var none *StorageIntegrityScrubber
	if got := none.Scrub("Table hg_safe.db1__t does not exist"); got != "Table hg_safe.db1__t does not exist" {
		t.Errorf("a nil scrubber must be a no-op, got %q", got)
	}
}

// TestStorageIntegrityScrubber_EmptySnapshotStillRedactsReservedNames is spec
// 2026-09-24 §6.2: bare hg_safe, hg_unsafe and _hg_row_id are always scrubbed.
func TestStorageIntegrityScrubber_EmptySnapshotStillRedactsReservedNames(t *testing.T) {
	s := NewStorageIntegrityScrubber(sitable.NewFake(sitable.Pending).Current())
	got := s.Scrub("Table hg_unsafe.db9__x does not exist in hg_safe (column _hg_row_id)")
	want := "Table <storage-integrity>.db9__x does not exist in <storage-integrity> (column <storage-integrity>)"
	if got != want {
		t.Fatalf("Scrub = %q, want %q", got, want)
	}
}

func TestStorageIntegrityScrubberCacheBuildsOncePerVersion(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active})
	var cache StorageIntegrityScrubberCache
	first := cache.For(fake.Current())
	if cache.For(fake.Current()) != first || cache.Builds() != 1 {
		t.Fatalf("same version must reuse the scrubber; builds = %d", cache.Builds())
	}
	fake.Set(sitable.Table{ID: "db1.t", Status: sitable.Active}, sitable.Table{ID: "db1.u", Status: sitable.Active})
	second := cache.For(fake.Current())
	if second == first || cache.Builds() != 2 {
		t.Fatalf("a new version must rebuild; builds = %d", cache.Builds())
	}
	if got := second.Scrub("hg_safe.db1__u"); got != "db1.u" {
		t.Fatalf("rebuilt scrubber = %q, want db1.u", got)
	}
}
```

In `pkg/rewriter/backend_test.go`, the unsafe_latest half of this test now needs the response to name the accessed SI table, because parts are fetched only for accessed tables. Replace:

```go
func TestSentioRewriter_ShipsStorageIntegrityArgs(t *testing.T) {
	be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "x", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT})}
```

with:

```go
func TestSentioRewriter_ShipsStorageIntegrityArgs(t *testing.T) {
	be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "x", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT,
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "db1", OriginalTable: "t", LogicalDatabase: "db1", IsStorageIntegrity: true}}})}
```

Create `pkg/rewriter/table_state_test.go`:

```go
package rewriter

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/sitable"
)

// sequenceBackend answers successive Rewrite calls from a script and records
// every request, so a test can assert on each pass of one query.
type sequenceBackend struct {
	fakeBackend
	script   []*pb.RewriteSQLResponse
	requests []*pb.RewriteSQLRequest
}

func (b *sequenceBackend) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	b.requests = append(b.requests, req)
	if len(b.requests) > len(b.script) {
		return nil, errors.New("unexpected extra rewrite pass")
	}
	return b.script[len(b.requests)-1], nil
}

func siArgsOf(req *pb.RewriteSQLRequest) *pb.StorageIntegrityArgs {
	return req.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
}

func siAccessed(ids ...string) []*pb.AccessedTable {
	var out []*pb.AccessedTable
	for _, id := range ids {
		db, table, _ := strings.Cut(id, ".")
		out = append(out, &pb.AccessedTable{OriginalDatabase: db, OriginalTable: table, LogicalDatabase: db, IsStorageIntegrity: true})
	}
	return out
}

func dynamicSIFactory(be backend, state sitable.TableState, rs StorageIntegrityReadState) *SentioNetworkFactory {
	f := newFakeFactory(be)
	f.options.StorageIntegrity = StorageIntegrityOptions{Enabled: true, TableState: state, ReadState: rs, InsertLaneEnabled: true}
	return f
}

func TestSentioRewriter_SendsV2WithEmptyTableMap(t *testing.T) {
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{acknowledgedSIResponse(&pb.RewriteSQLResponse{
		Code: pb.RewriteCode_UnsupportedStatement, Message: "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"})}}
	rw := dynamicSIFactory(be, sitable.NewFake(sitable.Pending), nil).NewRewriter(&fakeSession{})
	_, err := rw.Rewrite(context.Background(), "SET max_threads = 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("err = %v, want the catch-all refusal", err)
	}
	si := siArgsOf(be.requests[0])
	if si == nil || si.GetContractVersion() != StorageIntegrityContractV2 || len(si.GetTables()) != 0 {
		t.Fatalf("args = %v, want V2 with an empty table map", si)
	}
	if strings.Join(si.GetReservedDatabases(), ",") != "hg_safe,hg_unsafe,hg_promote" {
		t.Fatalf("reserved databases = %v, want them on a request with no Active table", si.GetReservedDatabases())
	}
}

func TestSentioRewriter_UsesTheContextSnapshot(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active})
	pinned := fake.Current()
	fake.Set() // the table leaves the Active set after the query took its snapshot
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "x"})}}
	rw := dynamicSIFactory(be, fake, nil).NewRewriter(&fakeSession{})
	if _, err := rw.Rewrite(WithTableSnapshot(context.Background(), pinned), "SELECT a FROM db1.t", ""); err != nil {
		t.Fatal(err)
	}
	if _, ok := siArgsOf(be.requests[0]).GetTables()["db1.t"]; !ok {
		t.Fatalf("args = %v, want the query snapshot's Active set, not Current()", siArgsOf(be.requests[0]))
	}
}

func TestSentioRewriter_UnsafeLatestFetchesPartsForAccessedTablesOnly(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary,
		sitable.Table{ID: "db1.t", Status: sitable.Active},
		sitable.Table{ID: "db1.u", Status: sitable.Active},
	)
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}, "db1.u": {"all_9_9_0"}}}
	first := acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass1", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT, OriginalAccessedTables: siAccessed("db1.t")})
	second := acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass2", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT, OriginalAccessedTables: siAccessed("db1.t")})
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{first, second}}
	rw := dynamicSIFactory(be, fake, rs).NewRewriter(&fakeSession{})
	res, err := rw.Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT a FROM db1.t", "")
	if err != nil {
		t.Fatal(err)
	}
	if res.SQL != "pass2" {
		t.Fatalf("SQL = %q, want the second pass", res.SQL)
	}
	if strings.Join(rs.calls, ",") != "db1.t" {
		t.Fatalf("port calls = %v, want only the accessed db1.t", rs.calls)
	}
	if got := siArgsOf(be.requests[0]).GetTables()["db1.t"].GetExcludedUnsafeParts(); len(got) != 0 {
		t.Fatalf("classification pass excluded %v, want none", got)
	}
	tables := siArgsOf(be.requests[1]).GetTables()
	if got := tables["db1.t"].GetExcludedUnsafeParts(); len(got) != 1 || got[0] != "all_1_1_0" {
		t.Fatalf("rewrite pass excluded %v for db1.t", got)
	}
	if got := tables["db1.u"].GetExcludedUnsafeParts(); len(got) != 0 {
		t.Fatalf("an unaccessed Active table must carry no parts, got %v", got)
	}
}

func TestSentioRewriter_UnsafeLatestWithoutPromotedPartsIsOnePass(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active})
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass1", OriginalAccessedTables: siAccessed("db1.t")})}}
	rw := dynamicSIFactory(be, fake, &fakeReadState{}).NewRewriter(&fakeSession{})
	if _, err := rw.Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT a FROM db1.t", ""); err != nil {
		t.Fatal(err)
	}
	if len(be.requests) != 1 {
		t.Fatalf("passes = %d, want 1 when no accessed table has promoted parts", len(be.requests))
	}
}

func TestSentioRewriter_UnsafeLatestRejectsAMovingAccessedSet(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active}, sitable.Table{ID: "db1.u", Status: sitable.Active})
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}}}
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{
		acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass1", OriginalAccessedTables: siAccessed("db1.t")}),
		acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass2", OriginalAccessedTables: siAccessed("db1.t", "db1.u")}),
	}}
	_, err := dynamicSIFactory(be, fake, rs).NewRewriter(&fakeSession{}).Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT a FROM db1.t", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "unsafe_latest rewrite accessed") {
		t.Fatalf("err = %v, want a RejectedError for the moved accessed set", err)
	}
}

func TestSentioRewriter_RequiresV2Acknowledgement(t *testing.T) {
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{{Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1", StorageIntegrityContractVersion: StorageIntegrityContractV1}}}
	_, err := dynamicSIFactory(be, sitable.NewFake(sitable.Pending), nil).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "contract acknowledgement") {
		t.Fatalf("a V1 acknowledgement must fail closed, got %v", err)
	}
}
```

In `pkg/rewriter/probe_test.go`, the probe gains an eighth case and every probe request must carry the production reserved-database list. Replace:

```go
			"DROP TABLE db1.t",
			"SYSTEM RELOAD CONFIG (empty table map)",
		}, " | ") {
```

with:

```go
			"DROP TABLE db1.t",
			"SYSTEM RELOAD CONFIG (empty table map)",
			"SELECT * FROM hg_safe.db1__t (empty table map)",
		}, " | ") {
```

In `pkg/rewriter/probe_test.go`, replace:

```go
		empty := be.requests[6].GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
		if empty.GetContractVersion() != StorageIntegrityContractV2 || empty.GetTables() == nil || len(empty.GetTables()) != 0 {
			t.Fatalf("empty-map probe args = %v, want V2 with an empty table map", empty)
		}
```

with:

```go
		empty := be.requests[6].GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
		if empty.GetContractVersion() != StorageIntegrityContractV2 || empty.GetTables() == nil || len(empty.GetTables()) != 0 {
			t.Fatalf("empty-map probe args = %v, want V2 with an empty table map", empty)
		}
		for i, req := range be.requests {
			si := req.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
			if strings.Join(si.GetReservedDatabases(), ",") != "hg_safe,hg_unsafe,hg_promote" {
				t.Fatalf("probe request %d reserved databases = %v, want the production list", i, si.GetReservedDatabases())
			}
		}
```

In `pkg/rewriter/probe_test.go`, replace:

```go
	// The DROP fingerprint is exact per engine:
```

with:

```go
	// A build that ignores reserved_databases forwards a direct hg_safe read
	// while no table map entry names hg_safe.
	t.Run("ignored reserved databases are refused", func(t *testing.T) {
		responses := conformingProbeResponses()
		responses["SELECT * FROM hg_safe.db1__t (empty table map)"] = acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			StatementType:   pb.StatementType_STATEMENT_TYPE_SELECT,
			SqlAfterRewrite: "SELECT * FROM hg_safe.db1__t",
			Message:         "success",
		})
		err := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "probe=v2-empty-map-reserved-database") {
			t.Fatalf("err = %v, want the reserved-database probe named", err)
		}
	})

	// The DROP fingerprint is exact per engine:
```

In `pkg/rewriter/probe_test.go`, replace:

```go
		"SYSTEM RELOAD CONFIG (empty table map)": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "SYSTEM RELOAD CONFIG",
			Message:         storageIntegrityProbeEmptyMapMessage,
		}),
	}
}
```

with:

```go
		"SYSTEM RELOAD CONFIG (empty table map)": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "SYSTEM RELOAD CONFIG",
			Message:         storageIntegrityProbeEmptyMapMessage,
		}),
		"SELECT * FROM hg_safe.db1__t (empty table map)": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_RewriteError,
			StatementType:   pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
			SqlAfterRewrite: "SELECT * FROM hg_safe.db1__t",
			Message:         storageIntegrityProbeReservedMessage,
		}),
	}
}
```

In `pkg/plugins/rewrite/rewriter_test.go`, replace:

```go
	"github.com/housegate/housegate/pkg/rewriter"
)
```

with:

```go
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sitable"
)
```

In `pkg/plugins/rewrite/rewriter_test.go`, replace:

```go
	p := &Plugin{
		Factory: &fakeFactory{rw: rw},
		StorageIntegrityScrubber: rewriter.NewStorageIntegrityScrubber(rewriter.StorageIntegrityOptions{
			Tables: []rewriter.StorageIntegrityTable{
				{TableID: "db1.t", SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
			}}),
	}
```

with:

```go
	p := &Plugin{
		Factory:    &fakeFactory{rw: rw},
		TableState: sitable.NewStatic([]string{"db1.t"}, nil, "net"),
	}
```

In `pkg/plugins/rewrite/rewriter_test.go`, append three tests after `TestOnException_ScrubsStorageIntegrityNames` — replace its tail:

```go
	if !strings.Contains(exc.Message, "db1.t") {
		t.Fatalf("the logical name must survive scrubbing: %q", exc.Message)
	}
}
```

with:

```go
	if !strings.Contains(exc.Message, "db1.t") {
		t.Fatalf("the logical name must survive scrubbing: %q", exc.Message)
	}
}

func TestOnQuery_TakesOneSnapshotIntoContextAndQueryContext(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active})
	rw := &fakeRewriter{out: "SELECT 1", storageIntegrityContractVersion: rewriter.StorageIntegrityContractV2}
	p := &Plugin{Factory: &fakeFactory{rw: rw}, TableState: fake}
	sess := newSessionForTest(t, 50)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT a FROM db1.t", Query: &chproto.Query{Body: "SELECT a FROM db1.t"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatal(err)
	}
	if qctx.TableSnapshot == nil || qctx.TableSnapshot.Version() != 1 {
		t.Fatalf("QueryContext.TableSnapshot = %v, want version 1", qctx.TableSnapshot)
	}
	ctxSnap, ok := rewriter.TableSnapshotFromContext(rw.lastCtx)
	if !ok || ctxSnap != qctx.TableSnapshot {
		t.Fatal("the rewriter must receive the same snapshot the query context carries")
	}
}

func TestOnQuery_DisabledTakesNoSnapshot(t *testing.T) {
	rw := &fakeRewriter{out: "SELECT 1"}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}
	qctx := &plugin.QueryContext{Session: newSessionForTest(t, 51), OriginalSQL: "SELECT 1", Query: &chproto.Query{Body: "SELECT 1"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatal(err)
	}
	if qctx.TableSnapshot != nil {
		t.Fatal("a disabled deployment must not take a snapshot")
	}
	if _, ok := rewriter.TableSnapshotFromContext(rw.lastCtx); ok {
		t.Fatal("a disabled deployment must not attach a snapshot to the rewriter context")
	}
}

// TestOnException_ScrubsWithTheSessionsQuerySnapshot pins that the scrubber
// follows the snapshot the failing query ran under, not a later version.
func TestOnException_ScrubsWithTheSessionsQuerySnapshot(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active})
	rw := &fakeRewriter{out: "SELECT 1", storageIntegrityContractVersion: rewriter.StorageIntegrityContractV2}
	p := &Plugin{Factory: &fakeFactory{rw: rw}, TableState: fake}
	sess := newSessionForTest(t, 52)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT a FROM db1.t", Query: &chproto.Query{Body: "SELECT a FROM db1.t"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatal(err)
	}
	fake.Set() // db1.t leaves the Active set while the query runs
	exc := &chproto.Exception{Message: "Table hg_safe.db1__t does not exist"}
	if err := p.OnException(context.Background(), sess, exc); err != nil {
		t.Fatal(err)
	}
	if exc.Message != "Table db1.t does not exist" {
		t.Fatalf("scrubbed = %q, want the query snapshot's logical name", exc.Message)
	}
	p.OnClose(sess)
	exc = &chproto.Exception{Message: "Table hg_safe.db1__t does not exist"}
	if err := p.OnException(context.Background(), sess, exc); err != nil {
		t.Fatal(err)
	}
	if exc.Message != "Table <storage-integrity>.db1__t does not exist" {
		t.Fatalf("after close the current snapshot applies and still redacts the database, got %q", exc.Message)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -vet=off -count=1 ./pkg/rewriter/ ./pkg/plugins/rewrite/`

Expected: FAIL, build failed: `unknown field Enabled in struct literal of type StorageIntegrityOptions` `too many arguments in call to buildStorageIntegrityArgs` and `undefined: storageIntegrityProbeReservedMessage` (pkg/rewriter); `qctx.TableSnapshot undefined`, `undefined: rewriter.TableSnapshotFromContext` and `unknown field TableState in struct literal of type Plugin` (pkg/plugins/rewrite).

- [ ] **Step 3: Rewrite the storage-integrity adapter and extend the probe**

Replace the whole of `pkg/rewriter/storage_integrity.go` with:

```go
package rewriter

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/sitable"
)

// ReadModeSettingKey is the per-query ClickHouse custom setting that selects
// the storage-integrity read mode (Spec G D1). Like the other SQL_x_* keys
// it is read by housegate and forwarded unchanged (ClickHouse accepts it
// under custom_settings_prefixes = SQL_).
const ReadModeSettingKey = "SQL_x_read_mode"

// ReadMode selects which physical surface an SI table read resolves to.
type ReadMode string

const (
	ReadModeSafe         ReadMode = "safe"          // hg_safe only
	ReadModeUnsafeLatest ReadMode = "unsafe_latest" // hg_safe ∪ hg_unsafe minus promoted-not-yet-cleaned parts
)

// DefaultReservedRowIDColumn is the protocol row-identity column hidden
// from the logical surface (Spec G D3).
const DefaultReservedRowIDColumn = "_hg_row_id"

// ParseReadMode parses a setting/config value. Surrounding whitespace and
// the '…' / "…" quoting clickhouse-go's CustomSetting adds are stripped.
// Empty is an error -- callers decide their own default.
func ParseReadMode(raw string) (ReadMode, error) {
	v := strings.Trim(strings.TrimSpace(raw), "\"'")
	v = strings.TrimSpace(v)
	switch ReadMode(v) {
	case ReadModeSafe, ReadModeUnsafeLatest:
		return ReadMode(v), nil
	}
	return "", fmt.Errorf("%s: invalid value %q (want 'safe' or 'unsafe_latest')", ReadModeSettingKey, raw)
}

// StorageIntegrityReadState is the host port supplying the unsafe parts a
// promotion already copied into hg_safe but whose cleanup is not yet
// acknowledged (Spec G D2). sentio-node satisfies it with *snode.Role. Nil
// means "no co-located SNode": unsafe_latest is refused, never degraded.
type StorageIntegrityReadState interface {
	PromotedUnsafeParts(tableID string) ([]string, error)
}

// StorageIntegrityOptions is the read-surface slice of rewriter.Options.
type StorageIntegrityOptions struct {
	// Enabled is storage_integrity.enabled. It activates the V2 contract on
	// every request, fail-closed handling and the acknowledgement gate,
	// independent of how many tables are Active (spec 2026-09-24 H6).
	Enabled bool
	// TableState supplies the per-query snapshot when the caller did not
	// attach one to the context (WithTableSnapshot). Required when Enabled.
	TableState      sitable.TableState
	DefaultReadMode ReadMode                  // "" → safe
	ReadState       StorageIntegrityReadState // nil → unsafe_latest refused
	// InsertLaneEnabled is true when the SI ingress plugin is wired (it then
	// owns INSERT admission). When false, an INSERT whose accessed tables
	// include an SI table is rejected here (plan deviation D-1).
	InsertLaneEnabled bool
}

type tableSnapshotCtxKey struct{}

// WithTableSnapshot attaches the query's table-state snapshot. The rewrite
// plugin takes exactly one snapshot per query and every stage reads it.
func WithTableSnapshot(ctx context.Context, snap sitable.Snapshot) context.Context {
	return context.WithValue(ctx, tableSnapshotCtxKey{}, snap)
}

// TableSnapshotFromContext returns the snapshot attached by WithTableSnapshot.
func TableSnapshotFromContext(ctx context.Context) (sitable.Snapshot, bool) {
	snap, ok := ctx.Value(tableSnapshotCtxKey{}).(sitable.Snapshot)
	return snap, ok && snap != nil
}

// snapshotFor returns the query's snapshot, falling back to the current one.
func (o StorageIntegrityOptions) snapshotFor(ctx context.Context) sitable.Snapshot {
	if snap, ok := TableSnapshotFromContext(ctx); ok {
		return snap
	}
	if o.TableState != nil {
		return o.TableState.Current()
	}
	return sitable.NewSnapshot(0, sitable.Ordinary, nil)
}

type readModeCtxKey struct{}

// WithReadMode attaches the per-query read mode (from SQL_x_read_mode).
func WithReadMode(ctx context.Context, m ReadMode) context.Context {
	return context.WithValue(ctx, readModeCtxKey{}, m)
}

// ReadModeFromContext returns the per-query read mode, if any.
func ReadModeFromContext(ctx context.Context) (ReadMode, bool) {
	m, ok := ctx.Value(readModeCtxKey{}).(ReadMode)
	return m, ok && m != ""
}

// RejectedError is a rewrite outcome that MUST reach the client as an
// Exception (the plugin fails closed on it) instead of falling open to
// the original SQL. Used for every storage-integrity rejection (reserved
// column, non-lane write, unavailable read mode) and for any failure before
// a trustworthy classification when SI membership is configured.
type RejectedError struct {
	Code    pb.RewriteCode
	Message string
	Cause   error
}

func (e *RejectedError) Error() string {
	return "rewriter rejected SQL (code=" + e.Code.String() + "): " + e.Message
}

func (e *RejectedError) Unwrap() error {
	return e.Cause
}

// buildStorageIntegrityArgs renders the proto block for one call. It returns
// nil only when storage integrity is disabled; when enabled the block is sent
// even with an empty table map, because the V2 contract is activated by
// version (spec 2026-09-24 H6), and it always names the reserved databases. Every Active table of the snapshot is listed;
// parts carries the promoted-but-not-yet-cleaned unsafe parts of the Active
// tables the query accesses, and is consulted only in unsafe_latest mode.
func buildStorageIntegrityArgs(opts StorageIntegrityOptions, snap sitable.Snapshot, mode ReadMode, parts map[string][]string) (*pb.StorageIntegrityArgs, error) {
	if !opts.Enabled {
		return nil, nil
	}
	if mode == "" {
		mode = ReadModeSafe
	}
	active := snap.Active()
	out := &pb.StorageIntegrityArgs{
		Tables:              make(map[string]*pb.StorageIntegrityArgs_Table, len(active)),
		ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
		ReservedRowIdColumn: DefaultReservedRowIDColumn,
		ContractVersion:     StorageIntegrityContractV2,
		// Sent on every enabled request: under V2 the engines protect these
		// databases even when no table is Active (plan A handoff).
		ReservedDatabases: sitable.ReservedDatabases(),
	}
	if mode == ReadModeUnsafeLatest {
		if opts.ReadState == nil {
			return nil, &RejectedError{Code: pb.RewriteCode_RewriteError,
				Message: "storage_integrity read mode unsafe_latest is unavailable on this housegate: no promotion-state port (co-located SNode) is wired"}
		}
		out.ReadMode = pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST
	}
	for _, t := range active {
		phys := sitable.PhysicalTable(t.ID)
		entry := &pb.StorageIntegrityArgs_Table{
			SafeTable:   sitable.SafeDatabase + "." + phys,
			UnsafeTable: sitable.UnsafeDatabase + "." + phys,
		}
		if mode == ReadModeUnsafeLatest {
			entry.ExcludedUnsafeParts = parts[t.ID]
		}
		out.Tables[t.ID] = entry
	}
	return out, nil
}

// promotedPartsFor fetches the promoted-but-not-yet-cleaned unsafe parts of
// exactly the given Active tables (spec 2026-09-24 §6.2). Every port error is a
// RejectedError: unsafe_latest fails closed, never degrades (Spec G D2).
func promotedPartsFor(rs StorageIntegrityReadState, tableIDs []string) (map[string][]string, error) {
	out := make(map[string][]string, len(tableIDs))
	for _, id := range tableIDs {
		parts, err := rs.PromotedUnsafeParts(id)
		if err != nil {
			return nil, &RejectedError{Code: pb.RewriteCode_RewriteError,
				Message: fmt.Sprintf("storage_integrity read mode unsafe_latest: cannot resolve promoted unsafe parts for %s: %v", id, err),
				Cause:   err}
		}
		if len(parts) > 0 {
			out[id] = parts
		}
	}
	return out, nil
}

// storageIntegrityAccessedIDs returns the sorted, de-duplicated logical ids of
// the SI-flagged accessed tables.
func storageIntegrityAccessedIDs(tables []*pb.AccessedTable) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range tables {
		if !t.GetIsStorageIntegrity() {
			continue
		}
		db := t.GetLogicalDatabase()
		if db == "" {
			db = t.GetOriginalDatabase()
		}
		id := t.GetOriginalTable()
		if db != "" {
			id = db + "." + id
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// storageIntegrityAccess returns the first SI-flagged accessed table's
// logical "db.table" key and whether one exists.
func storageIntegrityAccess(tables []*pb.AccessedTable) (string, bool) {
	for _, t := range tables {
		if t.GetIsStorageIntegrity() {
			db := t.GetLogicalDatabase()
			if db == "" {
				db = t.GetOriginalDatabase()
			}
			if db == "" {
				return t.GetOriginalTable(), true
			}
			return db + "." + t.GetOriginalTable(), true
		}
	}
	return "", false
}

// storageIntegrityRedaction replaces a protocol-owned name that has no logical
// equivalent, such as a reserved physical database or the row-id column.
const storageIntegrityRedaction = "<storage-integrity>"

// StorageIntegrityScrubber removes protocol-owned SI names from text that is
// about to reach a client. Qualified physical names map back to their logical
// table id; reserved databases and the row-id column are redacted.
//
// This is deliberately a narrow SI mapping, not a general exception reverse
// mapper. The selected rewriter backend still owns general reverse mapping.
type StorageIntegrityScrubber struct {
	replacer *strings.Replacer
}

// NewStorageIntegrityScrubber builds the scrubber for one snapshot: every
// Active table's qualified physical names map back to its logical id, and the
// two reserved databases and the row-id column are always redacted, even when
// no table is Active.
func NewStorageIntegrityScrubber(snap sitable.Snapshot) *StorageIntegrityScrubber {
	active := snap.Active()
	// Qualified names must precede their bare database prefixes.
	pairs := make([]string, 0, len(active)*4+6)
	for _, table := range active {
		phys := sitable.PhysicalTable(table.ID)
		pairs = append(pairs,
			sitable.SafeDatabase+"."+phys, table.ID,
			sitable.UnsafeDatabase+"."+phys, table.ID,
		)
	}
	pairs = append(pairs,
		sitable.SafeDatabase, storageIntegrityRedaction,
		sitable.UnsafeDatabase, storageIntegrityRedaction,
		DefaultReservedRowIDColumn, storageIntegrityRedaction,
	)
	return &StorageIntegrityScrubber{replacer: strings.NewReplacer(pairs...)}
}

// StorageIntegrityScrubberCache keeps the scrubber of the newest snapshot
// version it was asked for, so a scrubber is built once per version rather
// than once per exception.
type StorageIntegrityScrubberCache struct {
	mu      sync.Mutex
	version uint64
	built   bool
	current *StorageIntegrityScrubber
	builds  int
}

// For returns the scrubber for snap's version, building it on a version change.
func (c *StorageIntegrityScrubberCache) For(snap sitable.Snapshot) *StorageIntegrityScrubber {
	if c == nil || snap == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.built && c.version == snap.Version() {
		return c.current
	}
	c.current = NewStorageIntegrityScrubber(snap)
	c.version = snap.Version()
	c.built = true
	c.builds++
	return c.current
}

// Builds reports how many scrubbers the cache has built (test observability).
func (c *StorageIntegrityScrubberCache) Builds() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.builds
}

// Scrub is safe on a nil receiver and an empty message.
func (s *StorageIntegrityScrubber) Scrub(message string) string {
	if s == nil || s.replacer == nil || message == "" {
		return message
	}
	return s.replacer.Replace(message)
}
```

In `pkg/rewriter/probe.go`, send the reserved databases on every probe request and add the eighth case (plan A handoff item 1 names the field; item 5 keeps the five older outcomes with the list set; item 6 gives this case's outcome). Replace:

```go
	storageIntegrityProbeDropSQL         = "DROP TABLE db1.t"
	storageIntegrityProbeEmptyMapMessage = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"
)
```

with:

```go
	storageIntegrityProbeDropSQL         = "DROP TABLE db1.t"
	storageIntegrityProbeEmptyMapMessage = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"

	// reserved_databases under an empty table map: the engine must protect
	// hg_safe although no table map entry names it (plan A handoff item 6).
	storageIntegrityProbeReservedSQL     = "SELECT * FROM hg_safe.db1__t"
	storageIntegrityProbeReservedMessage = "storage-integrity physical table hg_safe.db1__t is not directly addressable"
)
```

In `pkg/rewriter/probe.go`, replace:

```go
	pb "github.com/housegate/rewriter-proto/gen/pb"
)
```

with:

```go
	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/sitable"
)
```

In `pkg/rewriter/probe.go`, replace:

```go
// storageIntegrityProbeArgs returns the fixed probe arguments; emptyTables
// sends the V2 contract with an empty table map.
```

with:

```go
// storageIntegrityProbeArgs returns the fixed probe arguments; emptyTables
// sends the V2 contract with an empty table map. Every request names the
// reserved databases, as every production request does.
```

In `pkg/rewriter/probe.go`, replace:

```go
			ReservedRowIdColumn: DefaultReservedRowIDColumn,
			ContractVersion:     StorageIntegrityContractV2,
		},
```

with:

```go
			ReservedRowIdColumn: DefaultReservedRowIDColumn,
			ContractVersion:     StorageIntegrityContractV2,
			ReservedDatabases:   sitable.ReservedDatabases(),
		},
```

In `pkg/rewriter/probe.go`, replace:

```go
		sqlAfter:      storageIntegrityProbeUnmodelledSQL,
		message:       storageIntegrityProbeEmptyMapMessage,
	},
}
```

with:

```go
		sqlAfter:      storageIntegrityProbeUnmodelledSQL,
		message:       storageIntegrityProbeEmptyMapMessage,
	},
	{
		// reserved_databases: with no table map entry the engine still
		// protects hg_safe. A build that ignores the field forwards this read.
		name:          "v2-empty-map-reserved-database",
		emptyTables:   true,
		sql:           storageIntegrityProbeReservedSQL,
		code:          pb.RewriteCode_RewriteError,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbeReservedSQL,
		message:       storageIntegrityProbeReservedMessage,
	},
}
```

In `pkg/rewriter/sentio.go`, replace:

```go
	"github.com/housegate/housegate/pkg/route"
	"github.com/housegate/housegate/pkg/sqlmeta"
```

with:

```go
	"github.com/housegate/housegate/pkg/route"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
```

In `pkg/rewriter/sentio.go`, replace:

```go
	mu                   sync.Mutex
	lastSQL              string
	lastEffectiveAccount string

```

with:

```go
	mu                   sync.Mutex
	lastSQL              string
	lastEffectiveAccount string
	lastSnapshot         sitable.Snapshot

```

In `pkg/rewriter/sentio.go`, replace:

```go
// Error handling: ordinary failures retain the legacy fail-open contract when
// no SI membership is configured. With SI membership, pre-classification
// failures and SI-specific rejections return *RejectedError and MUST reach the
// client as an Exception. UnsupportedStatement remains a passthrough only when
// the configured SI table set is empty.
```

with:

```go
// Error handling: ordinary failures retain the legacy fail-open contract when
// storage integrity is disabled. When it is enabled, pre-classification
// failures and SI-specific rejections return *RejectedError and MUST reach the
// client as an Exception. UnsupportedStatement remains a passthrough only when
// storage integrity is disabled.
```

In `pkg/rewriter/sentio.go`, replace:

```go
	r.mu.Lock()
	r.lastSQL = sql
	r.lastEffectiveAccount = effectiveAccount
	r.mu.Unlock()

	dbMap, knownPhys, err := r.factory.buildDatabaseMap(effectiveAccount)
	if err != nil {
		return RewriteResult{}, r.rewriteFailure(fmt.Errorf("build database map: %w", err))
	}
	logicalToRemote, remoteUpstreams := r.factory.buildRemoteUpstreams(dbMap)
	mode := r.factory.options.StorageIntegrity.DefaultReadMode
	if m, ok := ReadModeFromContext(ctx); ok {
		mode = m
	}
	siArgs, err := buildStorageIntegrityArgs(r.factory.options.StorageIntegrity, mode)
	if err != nil {
```

with:

```go
	si := r.factory.options.StorageIntegrity
	snap := si.snapshotFor(ctx)
	r.mu.Lock()
	r.lastSQL = sql
	r.lastEffectiveAccount = effectiveAccount
	r.lastSnapshot = snap
	r.mu.Unlock()

	dbMap, knownPhys, err := r.factory.buildDatabaseMap(effectiveAccount)
	if err != nil {
		return RewriteResult{}, r.rewriteFailure(fmt.Errorf("build database map: %w", err))
	}
	logicalToRemote, remoteUpstreams := r.factory.buildRemoteUpstreams(dbMap)
	mode := si.DefaultReadMode
	if m, ok := ReadModeFromContext(ctx); ok {
		mode = m
	}
	siArgs, err := buildStorageIntegrityArgs(si, snap, mode, nil)
	if err != nil {
```

In `pkg/rewriter/sentio.go`, replace:

```go
	req := &pb.RewriteSQLRequest{
		Sql:     sql,
		Options: []*pb.RewriteOption{rewriteOption(dynArgs)},
	}
	resp, err := r.callWithTimeout(ctx, req)
	if err != nil {
		return RewriteResult{}, r.rewriteFailure(fmt.Errorf("rewrite: %w", err))
	}
	if resp == nil {
		return RewriteResult{}, r.rewriteFailure(fmt.Errorf("rewrite: nil response"))
	}
	// Spec G D-8: additive protobuf fields are not proof that the backend
	// understood SI. An old server can ignore the request and still return
	// Success, so require an exact positive acknowledgement first.
	if len(r.factory.options.StorageIntegrity.Tables) > 0 &&
		resp.GetStorageIntegrityContractVersion() != StorageIntegrityContractV2 {
		return RewriteResult{}, &RejectedError{Code: pb.RewriteCode_RewriteError,
			Message: fmt.Sprintf("storage-integrity rewriter contract acknowledgement unavailable: got %s, want %s",
				resp.GetStorageIntegrityContractVersion(), StorageIntegrityContractV2)}
	}
	// Spec G fail-closed rule (plan D-2): a non-Success answer that involves
	// a storage-integrity table must reach the client as an Exception.
	if key, si := storageIntegrityAccess(resp.GetOriginalAccessedTables()); si {
```

with:

```go
	resp, err := r.rewriteOnce(ctx, sql, dynArgs)
	if err != nil {
		return RewriteResult{}, err
	}
	// unsafe_latest needs the promoted-not-yet-cleaned parts of the Active
	// tables this query reads, and only the rewriter knows which those are.
	// The first pass classifies with no exclusions; when an accessed table
	// has promoted parts, the second pass rewrites with exactly those
	// (spec 2026-09-24 §6.2). The accessed SI set must not move between the
	// two passes, since the parts were chosen from the first.
	if si.Enabled && mode == ReadModeUnsafeLatest && resp.GetCode() == pb.RewriteCode_Success {
		accessed := storageIntegrityAccessedIDs(resp.GetOriginalAccessedTables())
		parts, err := promotedPartsFor(si.ReadState, accessed)
		if err != nil {
			return RewriteResult{}, err
		}
		if len(parts) > 0 {
			partsArgs, err := buildStorageIntegrityArgs(si, snap, mode, parts)
			if err != nil {
				return RewriteResult{}, err
			}
			second, err := r.rewriteOnce(ctx, sql, buildDynamicArgs(dbMap, knownPhys, r.sess.LogicalDatabaseName(), r.sess.PhysicalDatabaseName(), r.factory.options.Delim, logicalToRemote, remoteUpstreams, partsArgs))
			if err != nil {
				return RewriteResult{}, err
			}
			if got := storageIntegrityAccessedIDs(second.GetOriginalAccessedTables()); strings.Join(got, ",") != strings.Join(accessed, ",") {
				return RewriteResult{}, &RejectedError{Code: pb.RewriteCode_RewriteError,
					Message: fmt.Sprintf("storage-integrity unsafe_latest rewrite accessed %v after classifying %v", got, accessed)}
			}
			resp = second
		}
	}
	// Spec G fail-closed rule (plan D-2): a non-Success answer that involves
	// a storage-integrity table must reach the client as an Exception.
	if key, isSI := storageIntegrityAccess(resp.GetOriginalAccessedTables()); isSI {
```

In `pkg/rewriter/sentio.go`, replace:

```go
	if len(r.factory.options.StorageIntegrity.Tables) > 0 && resp.GetCode() != pb.RewriteCode_Success {
```

with:

```go
	if si.Enabled && resp.GetCode() != pb.RewriteCode_Success {
```

In `pkg/rewriter/sentio.go`, replace:

```go
func (r *sentioRewriter) rewriteFailure(err error) error {
	if len(r.factory.options.StorageIntegrity.Tables) == 0 {
```

with:

```go
// rewriteOnce performs one backend call and applies the transport and
// acknowledgement checks every pass must satisfy.
func (r *sentioRewriter) rewriteOnce(ctx context.Context, sql string, dynArgs *pb.RewriteTableDynamicArgs) (*pb.RewriteSQLResponse, error) {
	req := &pb.RewriteSQLRequest{
		Sql:     sql,
		Options: []*pb.RewriteOption{rewriteOption(dynArgs)},
	}
	resp, err := r.callWithTimeout(ctx, req)
	if err != nil {
		return nil, r.rewriteFailure(fmt.Errorf("rewrite: %w", err))
	}
	if resp == nil {
		return nil, r.rewriteFailure(fmt.Errorf("rewrite: nil response"))
	}
	// Spec G D-8: additive protobuf fields are not proof that the backend
	// understood SI. An old server can ignore the request and still return
	// Success, so require an exact positive acknowledgement first.
	if r.factory.options.StorageIntegrity.Enabled &&
		resp.GetStorageIntegrityContractVersion() != StorageIntegrityContractV2 {
		return nil, &RejectedError{Code: pb.RewriteCode_RewriteError,
			Message: fmt.Sprintf("storage-integrity rewriter contract acknowledgement unavailable: got %s, want %s",
				resp.GetStorageIntegrityContractVersion(), StorageIntegrityContractV2)}
	}
	return resp, nil
}

func (r *sentioRewriter) rewriteFailure(err error) error {
	if !r.factory.options.StorageIntegrity.Enabled {
```

In `pkg/rewriter/sentio.go`, replace:

```go
	sql := r.lastSQL
	effectiveAccount := r.lastEffectiveAccount
	r.mu.Unlock()
```

with:

```go
	sql := r.lastSQL
	effectiveAccount := r.lastEffectiveAccount
	snap := r.lastSnapshot
	r.mu.Unlock()
```

In `pkg/rewriter/sentio.go`, replace:

```go
	siArgs, _ := buildStorageIntegrityArgs(r.factory.options.StorageIntegrity, r.factory.options.StorageIntegrity.DefaultReadMode)
```

with:

```go
	if snap == nil {
		snap = r.factory.options.StorageIntegrity.snapshotFor(ctx)
	}
	siArgs, _ := buildStorageIntegrityArgs(r.factory.options.StorageIntegrity, snap, r.factory.options.StorageIntegrity.DefaultReadMode, nil)
```

- [ ] **Step 4: Run the rewriter tests**

Run: `go test -vet=off -count=1 ./pkg/rewriter/`

Expected: PASS.

- [ ] **Step 5: Carry the snapshot through the query context and the rewrite plugin**

In `pkg/plugin/context.go`, replace:

```go
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/sqlmeta"
)
```

with:

```go
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
)
```

In `pkg/plugin/context.go`, replace:

```go
	ExistenceClause sqlmeta.ExistenceClause

	// AbortWithSuccess
```

with:

```go
	ExistenceClause sqlmeta.ExistenceClause

	// TableSnapshot is the storage-integrity table-state snapshot this query
	// runs under (spec 2026-09-24 H1). The rewrite plugin takes exactly one at
	// the start of OnQuery when storage integrity is enabled; the rewriter
	// arguments, sitablestate, the ingress, parts-pressure resolution and the
	// exception scrubber all read this value, never TableState.Current(), so
	// one query never observes two versions. Nil when storage integrity is
	// disabled or the rewrite plugin did not run.
	TableSnapshot sitable.Snapshot

	// AbortWithSuccess
```

In `pkg/plugins/rewrite/rewriter.go`, replace:

```go
// Ordinary rewrite errors retain the legacy fail-open posture only when no
// storage-integrity surface is configured. RejectedError and configured-SI
// classification/acknowledgement failures are returned to the client.
```

with:

```go
// Ordinary rewrite errors retain the legacy fail-open posture only when
// storage integrity is disabled. RejectedError and enabled-SI
// classification/acknowledgement failures are returned to the client.
```

In `pkg/plugins/rewrite/rewriter.go`, replace:

```go
	"github.com/housegate/housegate/pkg/rewriter"
)
```

with:

```go
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sitable"
)
```

In `pkg/plugins/rewrite/rewriter.go`, replace:

```go
	// StorageIntegrityScrubber removes protocol-owned SI names from Exception
	// text before it reaches the client. Nil disables scrubbing.
	StorageIntegrityScrubber *rewriter.StorageIntegrityScrubber

	// rewriters caches one Rewriter per session id (int64). Lifetime
	// is OnQuery-first-touch through OnClose.
	rewriters sync.Map // map[int64]rewriter.Rewriter
}
```

with:

```go
	// TableState is the storage-integrity table-state port. Non-nil exactly
	// when storage_integrity.enabled: OnQuery then takes one snapshot per
	// query into QueryContext.TableSnapshot and the rewriter context, and
	// OnException scrubs protocol-owned names with the scrubber of the
	// session's last snapshot.
	TableState sitable.TableState

	// rewriters caches one Rewriter per session id (int64). Lifetime
	// is OnQuery-first-touch through OnClose.
	rewriters sync.Map // map[int64]rewriter.Rewriter

	// snapshots keeps each session's last query snapshot for OnException.
	snapshots sync.Map // map[int64]sitable.Snapshot

	scrubbers rewriter.StorageIntegrityScrubberCache
}
```

In `pkg/plugins/rewrite/rewriter.go`, replace:

```go
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if qctx.Session != nil {
```

with:

```go
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p.TableState != nil {
		// Spec 2026-09-24 H1: exactly one snapshot per query, taken here and
		// read by every later stage.
		tableSnap := p.TableState.Current()
		qctx.TableSnapshot = tableSnap
		ctx = rewriter.WithTableSnapshot(ctx, tableSnap)
		if qctx.Session != nil {
			p.snapshots.Store(qctx.Session.ID(), tableSnap)
		}
	}
	if qctx.Session != nil {
```

In `pkg/plugins/rewrite/rewriter.go`, replace:

```go
	if scrubbed := p.StorageIntegrityScrubber.Scrub(exc.Message); scrubbed != exc.Message {
```

with:

```go
	if scrubbed := p.scrubberFor(sess).Scrub(exc.Message); scrubbed != exc.Message {
```

In `pkg/plugins/rewrite/rewriter.go`, replace:

```go
// OnConnect satisfies ConnLifecyclePlugin. We can't build the
```

with:

```go
// scrubberFor returns the scrubber of the session's last query snapshot, or
// of the current snapshot when the session has not run a query yet. It is nil
// (a no-op) when storage integrity is disabled.
func (p *Plugin) scrubberFor(sess chsession.Session) *rewriter.StorageIntegrityScrubber {
	if p.TableState == nil {
		return nil
	}
	if v, ok := p.snapshots.Load(sess.ID()); ok {
		return p.scrubbers.For(v.(sitable.Snapshot))
	}
	return p.scrubbers.For(p.TableState.Current())
}

// OnConnect satisfies ConnLifecyclePlugin. We can't build the
```

In `pkg/plugins/rewrite/rewriter.go`, replace:

```go
func (p *Plugin) evict(id int64) {
	if v, ok := p.rewriters.LoadAndDelete(id); ok {
```

with:

```go
func (p *Plugin) evict(id int64) {
	p.snapshots.Delete(id)
	if v, ok := p.rewriters.LoadAndDelete(id); ok {
```

- [ ] **Step 6: Run the plugin tests**

Run: `go test -vet=off -count=1 ./pkg/plugin/ ./pkg/plugins/rewrite/`

Expected: PASS.

- [ ] **Step 7: Write the failing build tests**

Create `storage_integrity_table_state_test.go`:

```go
package housegate

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugins/rewrite"
	"github.com/housegate/housegate/pkg/plugins/sireserved"
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sitable"
)

func boolPtr(v bool) *bool { return &v }

func TestResolveStorageIntegrityTableState(t *testing.T) {
	fake := sitable.NewFake(sitable.Pending)
	for _, tc := range []struct {
		name     string
		enabled  *bool
		tables   []string
		injected sitable.TableState
		wantErr  string
		static   bool
		dynamic  bool
	}{
		{name: "disabled"},
		{name: "disabled with an injected state", injected: fake, wantErr: "requires storage_integrity.enabled: true"},
		{name: "tables default to enabled and static", tables: []string{"db1.t"}, static: true},
		{name: "explicit switch with an injected state", enabled: boolPtr(true), injected: fake, dynamic: true},
		{name: "both sources", enabled: boolPtr(true), tables: []string{"db1.t"}, injected: fake, wantErr: "not both"},
		{name: "neither source", enabled: boolPtr(true), wantErr: "requires a table-set source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := minimalServerCfg(t)
			cfg.StorageIntegrity.Enabled = tc.enabled
			cfg.StorageIntegrity.Tables = tc.tables
			state, static, err := resolveStorageIntegrityTableState(Options{Config: cfg, StorageIntegrityTableState: tc.injected}, network.NewInMemoryNetworkState())
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case tc.static:
				if static == nil || state != sitable.TableState(static) || state.Current().Lookup("db1", "t").Status != sitable.Active {
					t.Fatalf("state = %v static = %v, want Static with db1.t Active", state, static)
				}
			case tc.dynamic:
				if static != nil || state != sitable.TableState(fake) {
					t.Fatalf("state = %v static = %v, want the injected state", state, static)
				}
			default:
				if state != nil || static != nil {
					t.Fatalf("disabled must return no state, got %v %v", state, static)
				}
			}
		})
	}
}

// TestStaticSchemasPreferTheRuntimeSet pins that sitable.Static serves the
// runtime's authoritative startup schemas when the host supplies them.
func TestStaticSchemasPreferTheRuntimeSet(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Tables = []string{"net1.events"}
	opts := Options{Config: cfg, StorageIntegrityRuntime: StorageIntegrityRuntimeOptions{TableSchemas: bpSchemas()}}
	_, static, err := resolveStorageIntegrityTableState(opts, network.NewInMemoryNetworkState())
	if err != nil {
		t.Fatal(err)
	}
	table, ok := static.Current().Schema("net1.events")
	if !ok || table.Schema.PartitionBy != "region" || table.SchemaHash == "" {
		t.Fatalf("static schema = %+v ok=%v, want the runtime set's schema", table, ok)
	}
}

// TestBuildServer_InjectedTableStateEnablesTheSurface pins spec 2026-09-24
// §6.2: an explicit switch with an injected state and no tables wires the
// fail-closed rewrite plugin over that state and the reserved-name guard.
func TestBuildServer_InjectedTableStateEnablesTheSurface(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Enabled = boolPtr(true)
	fake := sitable.NewFake(sitable.Pending)
	bs, err := buildServer(Options{
		Config:                     cfg,
		NetworkState:               network.NewInMemoryNetworkState(),
		Rewriter:                   siProbeStubRewriterFactory{},
		StorageIntegrityTableState: fake,
	}, nil)
	if err != nil {
		t.Fatalf("build with an injected table state: %v", err)
	}
	defer bs.teardown()
	var rw *rewrite.Plugin
	guarded := false
	for _, candidate := range requireExternalChain(t, bs).QueryPlugins {
		switch typed := candidate.(type) {
		case *rewrite.Plugin:
			rw = typed
		case *sireserved.Plugin:
			guarded = true
		}
	}
	if rw == nil || rw.TableState != sitable.TableState(fake) || !rw.FailClosedOnError || rw.RequiredStorageIntegrityContractVersion != rewriter.StorageIntegrityContractV2 {
		t.Fatalf("rewrite plugin = %+v, want fail-closed V2 over the injected state", rw)
	}
	if !guarded {
		t.Fatal("an enabled deployment must wire the reserved-name guard")
	}
}
```

In `build_test.go`, replace:

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

In `build_test.go`, replace:

```go
func TestStorageIntegrityRewriterOptions_DerivesPhysicalNames(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Tables = []string{"tenant.events", "db1.t"}
	cfg.StorageIntegrity.Read.DefaultMode = "unsafe_latest"
	cfg.StorageIntegrity.Ingress.Enabled = false
	rs := &buildFakeReadState{}

	got := storageIntegrityRewriterOptions(cfg, rs)
	if len(got.Tables) != 2 || got.Tables[0] != (rewriter.StorageIntegrityTable{
		TableID:     "tenant.events",
		SafeTable:   "hg_safe.tenant__events",
		UnsafeTable: "hg_unsafe.tenant__events",
	}) {
		t.Fatalf("tables = %+v", got.Tables)
	}
	if got.DefaultReadMode != rewriter.ReadModeUnsafeLatest || got.ReadState != rs || got.InsertLaneEnabled {
		t.Fatalf("opts = %+v", got)
	}

	cfg.StorageIntegrity.Ingress.Enabled = true
	if !storageIntegrityRewriterOptions(cfg, nil).InsertLaneEnabled {
		t.Fatal("ingress enabled must enable the insert lane")
	}
}
```

with:

```go
func TestStorageIntegrityRewriterOptions_CarriesTheTableState(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Tables = []string{"tenant.events", "db1.t"}
	cfg.StorageIntegrity.Read.DefaultMode = "unsafe_latest"
	cfg.StorageIntegrity.Ingress.Enabled = false
	rs := &buildFakeReadState{}
	state := sitable.NewStatic(cfg.StorageIntegrity.Tables, nil, "")

	got := storageIntegrityRewriterOptions(cfg, rs, state)
	if !got.Enabled || got.TableState != state {
		t.Fatalf("opts = %+v, want enabled over the given state", got)
	}
	if got.DefaultReadMode != rewriter.ReadModeUnsafeLatest || got.ReadState != rs || got.InsertLaneEnabled {
		t.Fatalf("opts = %+v", got)
	}
	if storageIntegrityRewriterOptions(cfg, rs, nil).Enabled {
		t.Fatal("no table state means storage integrity is disabled")
	}

	cfg.StorageIntegrity.Ingress.Enabled = true
	if !storageIntegrityRewriterOptions(cfg, nil, state).InsertLaneEnabled {
		t.Fatal("ingress enabled must enable the insert lane")
	}
}
```

In `build_test.go`, replace:

```go
	for _, want := range []string{"2 platform-operator", "tenant.events", "ordinary columns",
```

with:

```go
	for _, want := range []string{"2 platform-operator", "ordinary columns",
```

In `build_test.go`, replace:

```go
	if !rewritePlugin.FailClosedOnError || rewritePlugin.RequiredStorageIntegrityContractVersion != rewriter.StorageIntegrityContractV2 {
```

with:

```go
	if !rewritePlugin.FailClosedOnError || rewritePlugin.RequiredStorageIntegrityContractVersion != rewriter.StorageIntegrityContractV2 || rewritePlugin.TableState == nil {
```

In `build_test.go`, replace: (all 2 occurrences)

```go
"storage_integrity.tables requires an available SQL rewriter"
```

with:

```go
"storage_integrity.enabled requires an available SQL rewriter"
```

- [ ] **Step 8: Run them to verify they fail**

Run: `go test -vet=off -count=1 -run "TestResolveStorageIntegrityTableState|TestStaticSchemasPreferTheRuntimeSet|TestBuildServer_InjectedTableStateEnablesTheSurface|TestStorageIntegrityRewriterOptions" .`

Expected: FAIL, build failed: `build.go` itself no longer compiles (`out.Tables undefined`, `undefined: rewriter.StorageIntegrityTable`, `siOptions.Tables undefined`) because the rewriter lost its table slice in Step 3; the next step rewires it.

- [ ] **Step 9: Resolve the table-set source and wire it**

Create `storage_integrity_table_state.go`:

```go
package housegate

import (
	"context"
	"errors"
	"fmt"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
	"github.com/housegate/housegate/pkg/sitable"
)

// resolveStorageIntegrityTableState selects the table-set source (spec
// 2026-09-24 §6.1). It returns (nil, nil, nil) when storage integrity is
// disabled. When enabled exactly one source is required: the configured
// storage_integrity.tables (served by sitable.Static, returned as the second
// value) or the host-injected Options.StorageIntegrityTableState.
func resolveStorageIntegrityTableState(opts Options, reg registry.Registry) (sitable.TableState, *sitable.Static, error) {
	cfg := opts.Config
	injected := opts.StorageIntegrityTableState
	if isNilInterface(injected) {
		injected = nil
	}
	hasTables := len(cfg.StorageIntegrity.Tables) > 0
	if !cfg.StorageIntegrity.IsEnabled() {
		if injected != nil {
			return nil, nil, errors.New("Options.StorageIntegrityTableState requires storage_integrity.enabled: true")
		}
		return nil, nil, nil
	}
	switch {
	case hasTables && injected != nil:
		return nil, nil, errors.New("storage_integrity: configure exactly one table-set source, storage_integrity.tables or Options.StorageIntegrityTableState, not both")
	case injected != nil:
		return injected, nil, nil
	case !hasTables:
		return nil, nil, errors.New("storage_integrity.enabled requires a table-set source: storage_integrity.tables or Options.StorageIntegrityTableState")
	}
	static := sitable.NewStatic(cfg.StorageIntegrity.Tables, staticStorageIntegritySchemas(opts, reg), cfg.StorageIntegrity.Ingress.NetworkID)
	return static, static, nil
}

// staticStorageIntegritySchemas loads the startup schemas of the configured
// tables for sitable.Static. The runtime's authoritative set wins; otherwise,
// with the ingress enabled, each table is loaded from the declared
// network-state schema, as the ingress used to do per query. A table that
// cannot be loaded stays schema-less and the ingress refuses writes to it.
func staticStorageIntegritySchemas(opts Options, reg registry.Registry) map[string]payloadexec.TableSchema {
	cfg := opts.Config
	out := map[string]payloadexec.TableSchema{}
	if len(opts.StorageIntegrityRuntime.TableSchemas) > 0 {
		for _, schema := range opts.StorageIntegrityRuntime.TableSchemas {
			out[schema.TableID] = schema
		}
		return out
	}
	if !cfg.StorageIntegrity.Ingress.Enabled {
		return out
	}
	source, err := resolveTableSchemas(opts, reg, "storage_integrity.ingress")
	if err != nil {
		log.Warnw("storage_integrity: no declared schema source; signed INSERTs will be refused", "error", err)
		return out
	}
	loader := schemaregistry.NewNetworkStateLoader(source, cfg.StorageIntegrity.Ingress.NetworkID)
	for _, id := range cfg.StorageIntegrity.Tables {
		db, table, _ := config.SplitStorageIntegrityTableID(id)
		schemas, err := loader.Load(context.Background(), []schemaregistry.TableRef{{
			TableID: id, Database: db, Table: table, LogicalDatabase: db, LogicalTable: table,
		}})
		if err != nil || len(schemas) != 1 {
			log.Warnw("storage_integrity: table has no declared schema at startup; signed INSERTs into it will be refused", "table", id, "error", err)
			continue
		}
		out[id] = schemas[0]
	}
	return out
}

// storageIntegrityTableStateLabel names the source for startup logs.
func storageIntegrityTableStateLabel(static *sitable.Static) string {
	if static != nil {
		return fmt.Sprintf("static (%d tables)", len(static.TableIDs()))
	}
	return "injected"
}
```

In `proxy.go`, replace:

```go
	StorageIntegrityTableSchemas registry.TableSchemas
```

with:

```go
	StorageIntegrityTableSchemas registry.TableSchemas
	// StorageIntegrityTableState is the host's storage-integrity table-state
	// port (spec 2026-09-24 §5): versioned snapshots of every table's status.
	// It requires storage_integrity.enabled: true and excludes
	// storage_integrity.tables; when nil, an enabled server serves the
	// configured tables through sitable.Static.
	StorageIntegrityTableState sitable.TableState
```

In `proxy.go`, replace:

```go
	"github.com/housegate/housegate/pkg/rewriter"
```

with:

```go
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sitable"
```

In `build.go`, replace:

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

In `build.go`, replace:

```go
// storageIntegrityRewriterOptions derives the rewriter's SI read-surface
// options from config: physical names per Spec C D2, the default read mode,
// the host port, and whether the signed INSERT lane (ingress) is on.
func storageIntegrityRewriterOptions(cfg *config.Config, rs rewriter.StorageIntegrityReadState) rewriter.StorageIntegrityOptions {
	out := rewriter.StorageIntegrityOptions{
		DefaultReadMode:   rewriter.ReadMode(cfg.StorageIntegrity.Read.DefaultMode),
		ReadState:         rs,
		InsertLaneEnabled: cfg.StorageIntegrity.Ingress.Enabled,
	}
	for _, id := range cfg.StorageIntegrity.Tables {
		phys := config.StorageIntegrityPhysicalTable(id)
		out.Tables = append(out.Tables, rewriter.StorageIntegrityTable{
			TableID:     id,
			SafeTable:   config.StorageIntegritySafeDatabase + "." + phys,
			UnsafeTable: config.StorageIntegrityUnsafeDatabase + "." + phys,
		})
	}
	return out
}
```

with:

```go
// storageIntegrityRewriterOptions derives the rewriter's SI options: the
// table-state port (nil when storage integrity is disabled), the default read
// mode, the host read-state port, and whether the signed INSERT lane
// (ingress) is on.
func storageIntegrityRewriterOptions(cfg *config.Config, rs rewriter.StorageIntegrityReadState, state sitable.TableState) rewriter.StorageIntegrityOptions {
	return rewriter.StorageIntegrityOptions{
		Enabled:           state != nil,
		TableState:        state,
		DefaultReadMode:   rewriter.ReadMode(cfg.StorageIntegrity.Read.DefaultMode),
		ReadState:         rs,
		InsertLaneEnabled: cfg.StorageIntegrity.Ingress.Enabled,
	}
}
```

In `build.go`, replace:

```go
	if len(cfg.StorageIntegrity.Tables) == 0 {
		return ""
	}
```

with:

```go
	if !cfg.StorageIntegrity.IsEnabled() {
		return ""
	}
```

In `build.go`, replace:

```go
		warnings = append(warnings, fmt.Sprintf("storage_integrity: %d platform-operator addresses use the raw-SQL bypass for SI tables [%s]; the operator guard conservatively rejects every hg_safe / hg_unsafe / _hg_row_id mention (including ordinary columns and string literals), Identifier placeholders, any backslash-bearing literal or quoted identifier, and local-catalog object-carrier callables regardless of arguments; use a direct ClickHouse connection for physical access",
			count, strings.Join(cfg.StorageIntegrity.Tables, ", ")))
```

with:

```go
		warnings = append(warnings, fmt.Sprintf("storage_integrity: %d platform-operator addresses use the raw-SQL bypass for SI tables; the operator guard conservatively rejects every hg_safe / hg_unsafe / _hg_row_id mention (including ordinary columns and string literals), Identifier placeholders, any backslash-bearing literal or quoted identifier, and local-catalog object-carrier callables regardless of arguments; use a direct ClickHouse connection for physical access",
			count))
```

In `build.go`, replace:

```go
	siOptions := storageIntegrityRewriterOptions(cfg, siReadState)
	if siOptions.DefaultReadMode == rewriter.ReadModeUnsafeLatest && siReadState == nil {
		return nil, fmt.Errorf("storage_integrity.read.default_mode unsafe_latest requires Options.StorageIntegrityReadState (co-located SNode promotion journal); reference binaries can only serve safe reads")
	}
	if len(cfg.StorageIntegrity.Tables) > 0 && siReadState == nil {
		log.Warnw("storage_integrity: no read-state port wired; unsafe_latest reads will be refused", "tables", len(cfg.StorageIntegrity.Tables))
	}
	if warning := storageIntegrityInternalListenWarning(cfg); warning != "" {
		log.Warnw(warning, "internal_listen", cfg.InternalListen, "tables", len(cfg.StorageIntegrity.Tables))
	}
```

with:

```go
	siState, siStatic, err := resolveStorageIntegrityTableState(opts, reg)
	if err != nil {
		return nil, err
	}
	siOptions := storageIntegrityRewriterOptions(cfg, siReadState, siState)
	if siOptions.DefaultReadMode == rewriter.ReadModeUnsafeLatest && siReadState == nil {
		return nil, fmt.Errorf("storage_integrity.read.default_mode unsafe_latest requires Options.StorageIntegrityReadState (co-located SNode promotion journal); reference binaries can only serve safe reads")
	}
	if siOptions.Enabled && siReadState == nil {
		log.Warn("storage_integrity: no read-state port wired; unsafe_latest reads will be refused")
	}
	if warning := storageIntegrityInternalListenWarning(cfg); warning != "" {
		log.Warnw(warning, "internal_listen", cfg.InternalListen)
	}
```

In `build.go`, replace:

```go
	if len(siOptions.Tables) > 0 && rwFactory == nil {
		return nil, fmt.Errorf("storage_integrity.tables requires an available SQL rewriter; refusing fail-open startup")
	}
	if len(siOptions.Tables) > 0 {
		capable, ok := rwFactory.(rewriter.StorageIntegrityCapableFactory)
		if !ok || capable.StorageIntegrityContractVersion() != rewriter.StorageIntegrityContractV2 {
			return nil, fmt.Errorf("storage_integrity.tables requires a storage-integrity contract V2 capable SQL rewriter; refusing fail-open startup")
		}
```

with:

```go
	if siOptions.Enabled && rwFactory == nil {
		return nil, fmt.Errorf("storage_integrity.enabled requires an available SQL rewriter; refusing fail-open startup")
	}
	if siOptions.Enabled {
		capable, ok := rwFactory.(rewriter.StorageIntegrityCapableFactory)
		if !ok || capable.StorageIntegrityContractVersion() != rewriter.StorageIntegrityContractV2 {
			return nil, fmt.Errorf("storage_integrity.enabled requires a storage-integrity contract V2 capable SQL rewriter; refusing fail-open startup")
		}
```

In `build.go`, replace:

```go
			return nil, fmt.Errorf("storage_integrity.tables requires a SQL rewriter implementing rewriter.StorageIntegrityProbeFactory; refusing unverified startup")
```

with:

```go
			return nil, fmt.Errorf("storage_integrity.enabled requires a SQL rewriter implementing rewriter.StorageIntegrityProbeFactory; refusing unverified startup")
```

In `build.go`, replace:

```go
		log.Infow("storage-integrity rewriter build verified", "tables", len(siOptions.Tables))
```

with:

```go
		log.Infow("storage-integrity rewriter build verified", "table_state", storageIntegrityTableStateLabel(siStatic))
```

In `build.go`, replace:

```go
	if len(siOptions.Tables) > 0 {
		queryPlugins = append(queryPlugins, &sireserved.Plugin{
```

with:

```go
	if siOptions.Enabled {
		queryPlugins = append(queryPlugins, &sireserved.Plugin{
```

In `build.go`, replace:

```go
		log.Infow("storage-integrity reserved-name guard enabled", "tables", len(siOptions.Tables))
```

with:

```go
		log.Info("storage-integrity reserved-name guard enabled")
```

In `build.go`, replace:

```go
			FailClosedOnError: len(siOptions.Tables) > 0,
		}
		if len(siOptions.Tables) > 0 {
			rewritePlug.RequiredStorageIntegrityContractVersion = rewriter.StorageIntegrityContractV2
			rewritePlug.StorageIntegrityScrubber = rewriter.NewStorageIntegrityScrubber(siOptions)
		}
```

with:

```go
			FailClosedOnError: siOptions.Enabled,
		}
		if siOptions.Enabled {
			rewritePlug.RequiredStorageIntegrityContractVersion = rewriter.StorageIntegrityContractV2
			rewritePlug.TableState = siState
		}
```

- [ ] **Step 10: Run the build tests**

Run: `go build ./... && go test -vet=off -count=1 . ./pkg/rewriter/ ./pkg/plugins/rewrite/ ./pkg/plugin/`

Expected: PASS.

- [ ] **Step 11: Document the per-query snapshot**

In `CLAUDE.md`, replace:

```markdown
On the empty-SI-table surface, the rewriter is **fail-open by default** (rewriter down → original SQL forwarded, only a warn log); configured SI is fail-closed as described below.
```

with:

```markdown
With storage integrity disabled, the rewriter is **fail-open by default** (rewriter down → original SQL forwarded, only a warn log); with `storage_integrity.enabled` it is fail-closed as described below, for queries on ordinary tables too.
```

In `CLAUDE.md`, replace:

```markdown
When `storage_integrity.tables` is configured, the same request payload also carries rewriter-proto v0.2.0+ `StorageIntegrityArgs`: each logical table's `hg_safe` / `hg_unsafe` names, the configured or per-query read mode (`storage_integrity.read.default_mode` / `SQL_x_read_mode`), the reserved row-id column, and — for `unsafe_latest` — the promoted-but-not-yet-cleaned unsafe parts returned by `Options.StorageIntegrityReadState.PromotedUnsafeParts`.
```

with:

```markdown
When storage integrity is enabled (`storage_integrity.enabled`, which defaults to true exactly when `storage_integrity.tables` is non-empty), every request carries rewriter-proto `StorageIntegrityArgs`, even with an empty table map, because contract V2 is activated by version (spec 2026-09-24 H6). The table source is a `sitable.TableState`: `sitable.Static` over the configured `storage_integrity.tables` (every listed table Active, every other table Ordinary, version 1) or a host-injected `Options.StorageIntegrityTableState` — exactly one of the two, checked by `resolveStorageIntegrityTableState`. The rewrite plugin takes one immutable `sitable.Snapshot` per query, stores it in `QueryContext.TableSnapshot` and hands it to the rewriter through `rewriter.WithTableSnapshot`; every later stage reads that snapshot, never `Current()`. The args name the reserved databases `hg_safe` / `hg_unsafe` / `hg_promote` (`reserved_databases`, from `sitable.ReservedDatabases()`, pinned to the `config` constants) so the engines protect them even while no table is Active, and list each Active table's `hg_safe` / `hg_unsafe` names, the configured or per-query read mode (`storage_integrity.read.default_mode` / `SQL_x_read_mode`), the reserved row-id column, and — for `unsafe_latest` — the promoted-but-not-yet-cleaned unsafe parts returned by `Options.StorageIntegrityReadState.PromotedUnsafeParts` for exactly the Active tables the query accesses: a first pass classifies with no exclusions, and when an accessed table has promoted parts a second pass rewrites with them and must report the same accessed SI set.
```

In `CLAUDE.md`, replace:

```markdown
Legacy fail-open remains only when the configured SI table set is empty. With a configured SI table set a session-level `SET` is also refused (it is modelled by no handler in either engine, so it reaches the catch-all)
```

with:

```markdown
Legacy fail-open remains only when storage integrity is disabled. With storage integrity enabled a session-level `SET` is also refused, even while no table is Active (it is modelled by no handler in either engine, so it reaches the catch-all, which contract V2 activates by version)
```

In `CLAUDE.md`, replace:

```markdown
pass one bounded behavioral suite of seven cases:
```

with:

```markdown
pass one bounded behavioral suite of eight cases, every request naming the reserved databases:
```

In `CLAUDE.md`, replace:

```markdown
and `SYSTEM RELOAD CONFIG` refused under an empty table map. Every response must carry
```

with:

```markdown
`SYSTEM RELOAD CONFIG` refused under an empty table map, and `SELECT * FROM hg_safe.db1__t` refused under an empty table map (the engine protects `hg_safe` from `reserved_databases` alone). Every response must carry
```

In `CLAUDE.md`, replace:

```markdown
With a configured SI table set, both released Spec I engines fail closed on their catch-all:
```

with:

```markdown
With storage integrity enabled, both engines fail closed on their catch-all (contract V2 activates it by version, so this holds while no table is Active too):
```

In `CLAUDE.md`, replace:

```markdown
Upstream Exception text is scrubbed of `hg_safe` / `hg_unsafe` names and `_hg_row_id` before it reaches the client.
```

with:

```markdown
Upstream Exception text is scrubbed of `hg_safe` / `hg_unsafe` names and `_hg_row_id` before it reaches the client, by a scrubber built once per snapshot version (`rewriter.StorageIntegrityScrubberCache`) from the session's last query snapshot; the bare reserved names are scrubbed even when no table is Active.
```

In `CLAUDE.md`, replace:

```markdown
and `buildServer` warns when it is combined with SI tables.
```

with:

```markdown
and `buildServer` warns when it is combined with storage integrity.
```

In `CLAUDE.md`, replace:

```markdown
— server-side SI read policy. Config supplies explicit logical membership and safe/unsafe physical names; the rewriter adapter resolves `safe` / `unsafe_latest`, consults the host promotion journal, sends and verifies contract v1, and turns every untrustworthy outcome into `RejectedError`.
```

with:

```markdown
— server-side SI read policy. The query's `sitable.Snapshot` supplies the Active set; the adapter derives the D2 safe/unsafe physical names, resolves `safe` / `unsafe_latest`, consults the host promotion journal for the accessed Active tables only, sends and verifies contract V2, and turns every untrustworthy outcome into `RejectedError`.
```

In `README.md`, replace:

```markdown
With no `storage_integrity.tables`, a backend error or `UnsupportedStatement` falls back to the original SQL. Configured SI membership requires
```

with:

```markdown
With storage integrity disabled (`storage_integrity.enabled` false, the default when `storage_integrity.tables` is empty), a backend error or `UnsupportedStatement` falls back to the original SQL. Enabled storage integrity requires
```

In `README.md`, replace:

```markdown
ordinary tables retain the legacy fail-open behavior when the SI list is empty.
```

with:

```markdown
ordinary tables retain the legacy fail-open behavior only when storage integrity is disabled. With it enabled, the arguments are sent even when no table is Active and always name `hg_safe`, `hg_unsafe` and `hg_promote` as reserved databases, so session `SET`, `SYSTEM` and direct access to those databases stay refused.
```

In `pkg/rewriter/AGENTS.md`, replace:

```markdown
- Without configured SI tables, backend startup failure preserves the warn-and-disable fail-open posture. With `storage_integrity.tables`, server construction instead requires
```

with:

```markdown
- With storage integrity disabled, backend startup failure preserves the warn-and-disable fail-open posture. With `storage_integrity.enabled`, server construction instead requires
```

- [ ] **Step 12: Run the whole unit suite**

Run: `bazel run //:gazelle && bazel test //...`

Expected: every target PASSES.

- [ ] **Step 13: Commit**

```bash
git add pkg/rewriter/ pkg/plugin/ pkg/plugins/rewrite/ storage_integrity_table_state.go storage_integrity_table_state_test.go proxy.go build.go build_test.go BUILD.bazel CLAUDE.md README.md
git commit -m "$(cat <<'EOF'
feat(rewriter): take one table-state snapshot per query and build the SI arguments from it

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: The `sitablestate` plugin: the status matrix, data-carrying creation, Gone as an unknown table

**Files:**

- Create: `pkg/plugins/sitablestate/plugin.go`, `pkg/plugins/sitablestate/lexer.go`, `pkg/plugins/sitablestate/BUILD.bazel` (gazelle)
- Modify: `pkg/chproto/client_error.go:7` (the three refusal codes)
- Modify: `build.go:42` (import), `build.go:727-729` (register after rewrite), `CLAUDE.md` (plugin list, chain filters), `pkg/plugins/AGENTS.md:24,47-48`
- Test: create `pkg/plugins/sitablestate/plugin_test.go`, `pkg/plugins/sitablestate/lexer_test.go`, `pkg/plugins/sitablestate/snapshot_test.go`; append to `storage_integrity_table_state_test.go`

**Interfaces:**

- Consumes: `plugin.QueryContext.{TableSnapshot, StatementType, AccessedTables, OriginalSQL}` (Task 4), `sitable.Snapshot.Lookup`.
- Produces (packages `chproto` and `sitablestate`):

```go
// package chproto
const (
	CodeUnknownTable          int32 = 60  // UNKNOWN_TABLE
	CodeQueryIsProhibited     int32 = 392 // QUERY_IS_PROHIBITED
	CodeTableIsBeingRestarted int32 = 733 // TABLE_IS_BEING_RESTARTED
)
// package sitablestate
const (
	CodeRetryable    = chproto.CodeTableIsBeingRestarted
	CodeNonRetryable = chproto.CodeQueryIsProhibited
	CodeUnknownTable = chproto.CodeUnknownTable
)
type Plugin struct{}
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error // returns *chproto.ClientError
func (*Plugin) RunOnPeerTrust() bool // false
func (*Plugin) RunOnForward() bool   // false
```

Error message prefixes (spec §7.4, stable contract): `storage_integrity: table <id> is pending activation (retryable)`, `storage_integrity: table <id> was refused: <code>: <reason>`, `storage_integrity: table <id> is still being purged; retry CREATE later (retryable)`; plus `storage_integrity: table <id> is governed by storage integrity and cannot be created with data; create the table first, then INSERT`, `storage_integrity: table <id> is governed by storage integrity; ALTER and RENAME are not supported`, and Gone's `Table <id> does not exist` (code 60).

- [ ] **Step 1: Write the failing tests**

Create `pkg/plugins/sitablestate/lexer_test.go`:

```go
package sitablestate

import "testing"

func TestCreateTableCarriesData(t *testing.T) {
	for sql, want := range map[string]bool{
		"CREATE TABLE db1.t2 AS SELECT * FROM db1.t":                                          true,
		"CREATE TABLE db1.t2 ENGINE = MergeTree ORDER BY a AS SELECT a FROM db1.src":          true,
		"CREATE TABLE db1.t (a UInt32) ENGINE = MergeTree ORDER BY a AS (SELECT a FROM x)":    true,
		"create table db1.t engine = Memory as with 1 as a select a":                          true,
		"CREATE TABLE db1.t (a UInt32) ENGINE = MergeTree ORDER BY a":                         false,
		"CREATE TABLE db1.t2 AS db1.t":                                                        false,
		"CREATE TABLE db1.t2 EMPTY AS SELECT * FROM db1.t":                                    false,
		"CREATE TABLE db1.t (a UInt32 DEFAULT 1, b String ALIAS toString(a)) ENGINE = Memory": false,
		"CREATE TABLE db1.t COMMENT 'AS SELECT' ENGINE = Memory":                              false,
		"CREATE TABLE db1.t /* AS SELECT */ (a UInt8) ENGINE = Memory":                        false,
		"CREATE TABLE db1.`as select` (a UInt8) ENGINE = Memory":                              false,
	} {
		if got := createTableCarriesData(sql); got != want {
			t.Errorf("createTableCarriesData(%q) = %v, want %v", sql, got, want)
		}
	}
}

func TestMaterializedViewHeader(t *testing.T) {
	for _, tc := range []struct {
		sql                 string
		populate, hasTo     bool
		toDatabase, toTable string
	}{
		{"CREATE MATERIALIZED VIEW db1.mv TO db1.t AS SELECT a FROM db1.src", false, true, "db1", "t"},
		{"CREATE MATERIALIZED VIEW mv TO `t x` AS SELECT a FROM src", false, true, "", "t x"},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = MergeTree ORDER BY a POPULATE AS SELECT a FROM db1.t", true, false, "", ""},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = MergeTree ORDER BY a AS SELECT a FROM db1.t", false, false, "", ""},
		{"CREATE MATERIALIZED VIEW db1.mv REFRESH EVERY 1 HOUR APPEND TO db1.t AS SELECT a FROM db1.src", false, true, "db1", "t"},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY TO DISK 'cold' AS SELECT a FROM db1.src", false, false, "", ""},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = Memory AS SELECT a FROM db1.src WHERE x = 'POPULATE' AND b TO c", false, false, "", ""},
	} {
		populate, db, table, hasTo := materializedViewHeader(tc.sql)
		if populate != tc.populate || hasTo != tc.hasTo || db != tc.toDatabase || table != tc.toTable {
			t.Errorf("materializedViewHeader(%q) = %v %q %q %v, want %v %q %q %v", tc.sql, populate, db, table, hasTo, tc.populate, tc.toDatabase, tc.toTable, tc.hasTo)
		}
	}
}
```

Create `pkg/plugins/sitablestate/plugin_test.go`:

```go
package sitablestate

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

func newSession(t *testing.T, id int64) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return chsession.New(id, client)
}

func accessed(ids ...string) []sqlmeta.AccessedTable {
	var out []sqlmeta.AccessedTable
	for _, id := range ids {
		db, table, _ := strings.Cut(id, ".")
		out = append(out, sqlmeta.AccessedTable{OriginalDatabase: db, OriginalTable: table, LogicalDatabase: db})
	}
	return out
}

// statusSnapshot has db1.o Ordinary, db1.p Pending, db1.r Refused, db1.a
// Active and db1.g Gone; everything else is Ordinary.
func statusSnapshot() sitable.Snapshot {
	return sitable.NewSnapshot(7, sitable.Ordinary, []sitable.Table{
		{ID: "db1.p", Status: sitable.Pending},
		{ID: "db1.r", Status: sitable.Refused, RefusedCode: "column_type", RefusedReason: "column b: type Decimal(10, 2) is not admitted"},
		{ID: "db1.a", Status: sitable.Active},
		{ID: "db1.g", Status: sitable.Gone},
	})
}

func run(t *testing.T, typ sqlmeta.StatementType, sql string, tables []sqlmeta.AccessedTable) error {
	t.Helper()
	qctx := &plugin.QueryContext{
		Session:        newSession(t, 1),
		OriginalSQL:    sql,
		Query:          &chproto.Query{Body: sql},
		StatementType:  typ,
		AccessedTables: tables,
		TableSnapshot:  statusSnapshot(),
	}
	return (&Plugin{}).OnQuery(context.Background(), qctx)
}

type outcome struct {
	code   int32  // 0 = allowed
	prefix string // message prefix when refused
}

func check(t *testing.T, name string, err error, want outcome) {
	t.Helper()
	if want.code == 0 {
		if err != nil {
			t.Errorf("%s: err = %v, want allowed", name, err)
		}
		return
	}
	var ce *chproto.ClientError
	if !errors.As(err, &ce) {
		t.Errorf("%s: err = %v, want ClientError code %d", name, err, want.code)
		return
	}
	if ce.Code != want.code || !strings.HasPrefix(ce.Message, want.prefix) || ce.KeepSession {
		t.Errorf("%s: got code %d message %q keep=%v, want code %d prefix %q", name, ce.Code, ce.Message, ce.KeepSession, want.code, want.prefix)
	}
}

var (
	ok         = outcome{}
	pendingErr = func(id string) outcome {
		return outcome{733, "storage_integrity: table " + id + " is pending activation (retryable)"}
	}
	refusedErr = func(id string) outcome {
		return outcome{392, "storage_integrity: table " + id + " was refused: column_type: column b"}
	}
	unknownErr = func(id string) outcome { return outcome{60, "Table " + id + " does not exist"} }
	purgingErr = func(id string) outcome {
		return outcome{733, "storage_integrity: table " + id + " is still being purged; retry CREATE later (retryable)"}
	}
	withDataErr = func(id string) outcome {
		return outcome{392, "storage_integrity: table " + id + " is governed by storage integrity and cannot be created with data; create the table first, then INSERT"}
	}
	alterErr = func(id string) outcome {
		return outcome{392, "storage_integrity: table " + id + " is governed by storage integrity; ALTER and RENAME are not supported"}
	}
)

// TestDecisionMatrix is spec 2026-09-24 §7.2 row by row.
func TestDecisionMatrix(t *testing.T) {
	type column struct {
		name string
		typ  sqlmeta.StatementType
		sql  func(id string) string
	}
	columns := []column{
		{"data read", sqlmeta.StatementTypeSelect, func(id string) string { return "SELECT * FROM " + id }},
		{"data write", sqlmeta.StatementTypeInsert, func(id string) string { return "INSERT INTO " + id + " FORMAT Native" }},
		{"describe", sqlmeta.StatementTypeDescribe, func(id string) string { return "DESCRIBE TABLE " + id }},
		{"show create", sqlmeta.StatementTypeShowCreateTable, func(id string) string { return "SHOW CREATE TABLE " + id }},
		{"exists", sqlmeta.StatementTypeExistsTable, func(id string) string { return "EXISTS TABLE " + id }},
		{"create same name", sqlmeta.StatementTypeCreateTable, func(id string) string { return "CREATE TABLE " + id + " (a UInt8) ENGINE = Memory" }},
		{"drop", sqlmeta.StatementTypeDropTable, func(id string) string { return "DROP TABLE " + id }},
		{"alter", sqlmeta.StatementTypeAlterTable, func(id string) string { return "ALTER TABLE " + id + " ADD COLUMN b UInt8" }},
		{"rename", sqlmeta.StatementTypeRenameTable, func(id string) string { return "RENAME TABLE " + id + " TO db1.z" }},
	}
	rows := map[string][]outcome{
		"db1.o": {ok, ok, ok, ok, ok, ok, ok, ok, ok},
		"db1.p": {pendingErr("db1.p"), pendingErr("db1.p"), ok, ok, ok, ok, ok, alterErr("db1.p"), alterErr("db1.p")},
		"db1.r": {refusedErr("db1.r"), refusedErr("db1.r"), ok, ok, ok, ok, ok, refusedErr("db1.r"), refusedErr("db1.r")},
		"db1.a": {ok, ok, ok, ok, ok, ok, ok, ok, ok},
		"db1.g": {unknownErr("db1.g"), unknownErr("db1.g"), unknownErr("db1.g"), unknownErr("db1.g"), unknownErr("db1.g"), purgingErr("db1.g"), unknownErr("db1.g"), unknownErr("db1.g"), unknownErr("db1.g")},
	}
	for id, want := range rows {
		for i, col := range columns {
			tables := accessed(id)
			if col.typ == sqlmeta.StatementTypeRenameTable {
				tables = accessed(id, "db1.z")
			}
			check(t, id+" / "+col.name, run(t, col.typ, col.sql(id), tables), want[i])
		}
	}
}

// TestDataCarryingCreationIntoGovernedTables is §7.3 rule 2.
func TestDataCarryingCreationIntoGovernedTables(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    sqlmeta.StatementType
		sql    string
		tables []sqlmeta.AccessedTable
		want   outcome
	}{
		{"CTAS into a pending name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.p ENGINE = MergeTree ORDER BY a AS SELECT a FROM db1.o", accessed("db1.p"), withDataErr("db1.p")},
		{"CTAS into an active name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.a ENGINE = MergeTree ORDER BY a AS SELECT a FROM db1.o", accessed("db1.a"), withDataErr("db1.a")},
		{"CTAS into an ordinary name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.o ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a", accessed("db1.o"), ok},
		{"CTAS into a refused name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.r ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a", accessed("db1.r"), ok},
		{"CTAS EMPTY into a pending name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.p ENGINE = MergeTree ORDER BY a EMPTY AS SELECT 1 AS a", accessed("db1.p"), ok},
		{"CTAS reading a pending table", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.o ENGINE = Memory AS SELECT * FROM db1.p", accessed("db1.o", "db1.p"), pendingErr("db1.p")},
		{"schema clone of a pending table", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.o AS db1.p", accessed("db1.o", "db1.p"), ok},
		{"MV POPULATE into a pending name", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.p ENGINE = Memory POPULATE AS SELECT a FROM db1.o", accessed("db1.p", "db1.o"), withDataErr("db1.p")},
		{"MV TO a pending table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.p AS SELECT a FROM db1.o", accessed("db1.mv", "db1.p", "db1.o"), withDataErr("db1.p")},
		{"MV TO an active table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.a AS SELECT a FROM db1.o", accessed("db1.mv", "db1.a", "db1.o"), withDataErr("db1.a")},
		{"MV TO a refused table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.r AS SELECT a FROM db1.o", accessed("db1.mv", "db1.r", "db1.o"), refusedErr("db1.r")},
		{"MV TO an ordinary table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.o AS SELECT a FROM db1.o", accessed("db1.mv", "db1.o"), ok},
		{"MV reading a pending source", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv ENGINE = Memory AS SELECT a FROM db1.p", accessed("db1.mv", "db1.p"), pendingErr("db1.p")},
		{"view over a gone table", sqlmeta.StatementTypeCreateView, "CREATE VIEW db1.v AS SELECT a FROM db1.g", accessed("db1.v", "db1.g"), unknownErr("db1.g")},
		{"CTAS into a gone name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.g ENGINE = Memory AS SELECT 1 AS a", accessed("db1.g"), purgingErr("db1.g")},
	} {
		check(t, tc.name, run(t, tc.typ, tc.sql, tc.tables), tc.want)
	}
}

// TestRefusalPrecedence: unknown table, then non-retryable, then retryable;
// the first accessed table breaks a tie.
func TestRefusalPrecedence(t *testing.T) {
	check(t, "retryable then unknown", run(t, sqlmeta.StatementTypeSelect, "SELECT", accessed("db1.p", "db1.g")), unknownErr("db1.g"))
	check(t, "retryable then non-retryable", run(t, sqlmeta.StatementTypeSelect, "SELECT", accessed("db1.p", "db1.r")), refusedErr("db1.r"))
	check(t, "non-retryable then unknown", run(t, sqlmeta.StatementTypeSelect, "SELECT", accessed("db1.r", "db1.g", "db1.p")), unknownErr("db1.g"))
	check(t, "two retryable keep the first", run(t, sqlmeta.StatementTypeSelect, "SELECT", accessed("db1.p", "db1.o", "db2.p")), pendingErr("db1.p"))
	check(t, "ordinary and active only", run(t, sqlmeta.StatementTypeSelect, "SELECT", accessed("db1.o", "db1.a")), ok)
}

func TestUnqualifiedTableUsesTheSessionDatabase(t *testing.T) {
	sess := newSession(t, 2)
	sess.State().SetLogicalDatabase("db1")
	qctx := &plugin.QueryContext{
		Session: sess, OriginalSQL: "SELECT * FROM p", StatementType: sqlmeta.StatementTypeSelect,
		AccessedTables: []sqlmeta.AccessedTable{{OriginalTable: "p"}}, TableSnapshot: statusSnapshot(),
	}
	check(t, "unqualified pending", (&Plugin{}).OnQuery(context.Background(), qctx), pendingErr("db1.p"))
}

func TestDatabaseLevelStatementsAreNotDecided(t *testing.T) {
	for _, typ := range []sqlmeta.StatementType{sqlmeta.StatementTypeDropDatabase, sqlmeta.StatementTypeCreateDatabase, sqlmeta.StatementTypeGrant, sqlmeta.StatementTypeRevoke} {
		check(t, typ.String(), run(t, typ, "X", accessed("db1.g")), ok)
	}
}

// TestBypassRules is §7.1: peer-trusted and origin-side forwarding sessions
// skip the plugin through the chain filters, IsForwardedFromPeer runs it on
// the receiving host, and maintenance / platform-operator sessions keep their
// existing bypass.
func TestBypassRules(t *testing.T) {
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{&Plugin{}}}
	query := func(sess chsession.Session) error {
		return chain.OnQuery(context.Background(), &plugin.QueryContext{
			Session: sess, OriginalSQL: "SELECT * FROM db1.g", StatementType: sqlmeta.StatementTypeSelect,
			AccessedTables: accessed("db1.g"), TableSnapshot: statusSnapshot(),
		})
	}
	peer := newSession(t, 10)
	peer.State().SetPeerTrust("10.0.0.7:9001")
	check(t, "peer-trusted", query(peer), ok)

	forwarding := newSession(t, 11)
	forwarding.State().SetForwarding(true)
	check(t, "origin-side forwarding", query(forwarding), ok)

	forwarded := newSession(t, 12)
	forwarded.State().SetPeerTrustForwarded("10.0.0.7:9001", true)
	check(t, "forwarded from peer", query(forwarded), unknownErr("db1.g"))

	maintenance := newSession(t, 13)
	maintenance.State().SetMaintenance(true)
	check(t, "maintenance", query(maintenance), ok)

	operator := newSession(t, 14)
	operator.State().SetPlatformOperator(true)
	check(t, "platform operator", query(operator), ok)

	check(t, "ordinary session", query(newSession(t, 15)), unknownErr("db1.g"))
}

func TestNoSnapshotIsANoOp(t *testing.T) {
	qctx := &plugin.QueryContext{Session: newSession(t, 3), StatementType: sqlmeta.StatementTypeSelect, AccessedTables: accessed("db1.g")}
	check(t, "disabled", (&Plugin{}).OnQuery(context.Background(), qctx), ok)
}
```

Create `pkg/plugins/sitablestate/snapshot_test.go`:

```go
package sitablestate

import (
	"context"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/rewrite"
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

type versionRecordingRewriter struct{ seen []uint64 }

func (r *versionRecordingRewriter) Rewrite(ctx context.Context, sql, _ string) (rewriter.RewriteResult, error) {
	if snap, ok := rewriter.TableSnapshotFromContext(ctx); ok {
		r.seen = append(r.seen, snap.Version())
	}
	return rewriter.RewriteResult{
		SQL:                             sql,
		StatementType:                   sqlmeta.StatementTypeSelect,
		AccessedTables:                  accessed("db1.t"),
		StorageIntegrityContractVersion: rewriter.StorageIntegrityContractV2,
	}, nil
}
func (r *versionRecordingRewriter) RewriteErrorMessage(_ context.Context, m string) (string, error) {
	return m, nil
}
func (r *versionRecordingRewriter) Close() error { return nil }

type oneRewriterFactory struct{ rw rewriter.Rewriter }

func (f oneRewriterFactory) NewRewriter(rewriter.Session) rewriter.Rewriter { return f.rw }
func (f oneRewriterFactory) Close() error                                   { return nil }

// hookFunc adapts a function into a QueryPlugin.
type hookFunc func(*plugin.QueryContext)

func (h hookFunc) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	h(qctx)
	return nil
}

// TestOneSnapshotPerQuery is spec 2026-09-24 §11.2: the fake changes version
// between two hooks of one query, and every stage still observes the version
// the rewrite plugin took. The table is Active in version 1 and Gone in
// version 2, so a stage that re-read Current() would refuse the query.
func TestOneSnapshotPerQuery(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active})
	rw := &versionRecordingRewriter{}
	var later []uint64
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{
		&rewrite.Plugin{Factory: oneRewriterFactory{rw: rw}, TableState: fake, RequiredStorageIntegrityContractVersion: rewriter.StorageIntegrityContractV2},
		hookFunc(func(*plugin.QueryContext) { fake.Set(sitable.Table{ID: "db1.t", Status: sitable.Gone}) }),
		&Plugin{},
		hookFunc(func(qctx *plugin.QueryContext) { later = append(later, qctx.TableSnapshot.Version()) }),
	}}
	qctx := &plugin.QueryContext{Session: newSession(t, 20), OriginalSQL: "SELECT a FROM db1.t", Query: &chproto.Query{Body: "SELECT a FROM db1.t"}}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("a query that started under version 1 must finish under it: %v", err)
	}
	if len(rw.seen) != 1 || rw.seen[0] != 1 || len(later) != 1 || later[0] != 1 {
		t.Fatalf("rewriter saw %v, later stage saw %v, want [1] and [1]", rw.seen, later)
	}
	if fake.Current().Version() != 2 {
		t.Fatalf("the fake must have moved to version 2, got %d", fake.Current().Version())
	}
	next := &plugin.QueryContext{Session: newSession(t, 21), OriginalSQL: "SELECT a FROM db1.t", Query: &chproto.Query{Body: "SELECT a FROM db1.t"}}
	chain.QueryPlugins = []plugin.QueryPlugin{chain.QueryPlugins[0], &Plugin{}}
	check(t, "the next query sees version 2", chain.OnQuery(context.Background(), next), unknownErr("db1.t"))
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -count=1 ./pkg/plugins/sitablestate/`

Expected: FAIL, build failed: `undefined: createTableCarriesData`, `undefined: materializedViewHeader`, `undefined: Plugin`.

- [ ] **Step 3: Write the lexer and the plugin**

The rewriter's classification does not say whether a CREATE carries data; measured against rewriter-go v0.11.0 (Plan decision R5), `CREATE TABLE ... AS SELECT` reports `CREATE_TABLE` with only the target in `AccessedTables`, and a materialized view reports `CREATE_MATERIALIZED_VIEW` with or without `POPULATE` / `TO`. `lexer.go` reads only the top-level clause keywords of the CREATE header.

In `pkg/chproto/client_error.go`, replace:

```go
const CodeTooManyParts int32 = 252
```

with:

```go
const CodeTooManyParts int32 = 252

// ClickHouse error codes of the storage-integrity table lifecycle refusals
// (spec 2026-09-24 §7.4). None of them is 252 back-pressure or the 403 generic
// plugin rejection, and neither ClickHouse client treats them specially.
const (
	// CodeUnknownTable is UNKNOWN_TABLE: a Gone table reads as any missing table.
	CodeUnknownTable int32 = 60
	// CodeQueryIsProhibited is QUERY_IS_PROHIBITED: a non-retryable refusal.
	CodeQueryIsProhibited int32 = 392
	// CodeTableIsBeingRestarted is TABLE_IS_BEING_RESTARTED: the table exists
	// but is temporarily unavailable, so the refusal is retryable.
	CodeTableIsBeingRestarted int32 = 733
)
```

Create `pkg/plugins/sitablestate/lexer.go`:

```go
package sitablestate

import "strings"

// The rewriter's classification does not say whether a CREATE carries data:
// both engines report CREATE_TABLE for a plain CREATE and for
// CREATE ... AS SELECT, and CREATE_MATERIALIZED_VIEW with or without POPULATE
// or TO (measured against rewriter-go v0.11.0, 2026-09-24). This file reads
// only the top-level clause keywords of a CREATE header; it never rewrites.

type tokenKind uint8

const (
	tokWord   tokenKind = iota // bare word, compared case-insensitively
	tokIdent                   // `quoted` or "quoted" identifier, value unquoted
	tokString                  // 'string literal'
	tokPunct                   // one punctuation byte
)

type token struct {
	kind  tokenKind
	text  string
	depth int // parenthesis depth before the token
}

func (t token) isWord(upper string) bool {
	return t.kind == tokWord && strings.EqualFold(t.text, upper)
}

// tokenize splits sql into tokens, skipping whitespace and comments. An
// unterminated quote or comment ends the scan; the caller then sees fewer
// tokens and classifies conservatively.
func tokenize(sql string) []token {
	var out []token
	depth := 0
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-', c == '#':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return out
			}
			i += end + 4
		case c == '\'' || c == '`' || c == '"':
			value, next, ok := readQuoted(sql, i)
			if !ok {
				return out
			}
			kind := tokIdent
			if c == '\'' {
				kind = tokString
			}
			out = append(out, token{kind: kind, text: value, depth: depth})
			i = next
		case isWordByte(c):
			start := i
			for i < len(sql) && isWordByte(sql[i]) {
				i++
			}
			out = append(out, token{kind: tokWord, text: sql[start:i], depth: depth})
		default:
			out = append(out, token{kind: tokPunct, text: string(c), depth: depth})
			if c == '(' {
				depth++
			} else if c == ')' && depth > 0 {
				depth--
			}
			i++
		}
	}
	return out
}

func readQuoted(sql string, start int) (string, int, bool) {
	quote := sql[start]
	var b strings.Builder
	for i := start + 1; i < len(sql); i++ {
		switch sql[i] {
		case '\\':
			if i+1 < len(sql) {
				b.WriteByte(sql[i+1])
				i++
			}
		case quote:
			if i+1 < len(sql) && sql[i+1] == quote {
				b.WriteByte(quote)
				i++
				continue
			}
			return b.String(), i + 1, true
		default:
			b.WriteByte(sql[i])
		}
	}
	return "", len(sql), false
}

func isWordByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 0x80
}

// bodyStart returns the index of the top-level AS that introduces a SELECT
// body (AS SELECT, AS WITH, AS (SELECT ...)), or -1.
func bodyStart(toks []token) int {
	for i, t := range toks {
		if t.depth != 0 || !t.isWord("AS") || i+1 >= len(toks) {
			continue
		}
		next := toks[i+1]
		if next.isWord("SELECT") || next.isWord("WITH") {
			return i
		}
		if next.kind == tokPunct && next.text == "(" && i+2 < len(toks) && (toks[i+2].isWord("SELECT") || toks[i+2].isWord("WITH")) {
			return i
		}
	}
	return -1
}

// createTableCarriesData reports CREATE TABLE ... AS SELECT without EMPTY.
func createTableCarriesData(sql string) bool {
	toks := tokenize(sql)
	body := bodyStart(toks)
	if body < 0 {
		return false
	}
	for _, t := range toks[:body] {
		if t.depth == 0 && t.isWord("EMPTY") {
			return false
		}
	}
	return true
}

// materializedViewHeader reads a CREATE MATERIALIZED VIEW header: whether it
// populates, and its TO target (database may be empty) when present.
func materializedViewHeader(sql string) (populate bool, toDatabase, toTable string, hasTo bool) {
	toks := tokenize(sql)
	end := bodyStart(toks)
	if end < 0 {
		end = len(toks)
	}
	for i := 0; i < end; i++ {
		t := toks[i]
		if t.depth != 0 {
			continue
		}
		if t.isWord("POPULATE") {
			populate = true
			continue
		}
		if !t.isWord("TO") || i+1 >= end {
			continue
		}
		next := toks[i+1]
		if next.isWord("DISK") || next.isWord("VOLUME") || (next.kind != tokWord && next.kind != tokIdent) {
			continue
		}
		toTable = next.text
		if i+3 < end && toks[i+2].kind == tokPunct && toks[i+2].text == "." && (toks[i+3].kind == tokWord || toks[i+3].kind == tokIdent) {
			toDatabase, toTable = next.text, toks[i+3].text
		}
		hasTo = true
	}
	return populate, toDatabase, toTable, hasTo
}
```

Create `pkg/plugins/sitablestate/plugin.go`:

```go
// Package sitablestate enforces the storage-integrity table lifecycle on
// every query (spec 2026-09-24 §7). It runs after the rewrite plugin, reads
// the query's single table-state snapshot, the rewriter's StatementType and
// AccessedTables, and refuses what a table's status does not allow:
// Pending data access is retryable, Refused is not, a Gone table is answered
// as an unknown table, and data-carrying creation into a governed table is
// refused. Everything the matrix allows passes through untouched.
package sitablestate

import (
	"context"
	"fmt"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// The three refusal classes (plan ruling R3). They alias chproto so the
// signed ingress and the runtime answer with the same codes.
const (
	CodeRetryable    = chproto.CodeTableIsBeingRestarted // 733 TABLE_IS_BEING_RESTARTED
	CodeNonRetryable = chproto.CodeQueryIsProhibited     // 392 QUERY_IS_PROHIBITED
	CodeUnknownTable = chproto.CodeUnknownTable          // 60 UNKNOWN_TABLE
)

// Plugin is the server-mode table-state QueryPlugin. It is registered only
// when storage_integrity.enabled.
type Plugin struct{}

type class uint8

const (
	classNone       class = iota
	classRead             // data read
	classWrite            // data write
	classMetadata         // DESCRIBE, SHOW, EXISTS
	classCreate           // CREATE of this name, no data
	classCreateData       // CREATE ... AS SELECT, or MATERIALIZED VIEW ... POPULATE
	classDrop             // DROP TABLE / DROP VIEW
	classOtherDDL         // ALTER, RENAME
	classViewTarget       // the TO target of a MATERIALIZED VIEW
)

type severity uint8

const (
	allow severity = iota
	retryable
	nonRetryable
	unknownTable
)

type access struct {
	database string
	table    string
	class    class
}

// OnQuery refuses the statement when any accessed table refuses it. When
// several do, an unknown table wins over a non-retryable refusal, which wins
// over a retryable one; ties keep the first accessed table.
func (p *Plugin) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.TableSnapshot == nil || qctx.Session == nil {
		return nil
	}
	state := qctx.Session.State().Snapshot()
	if state.Maintenance || state.PlatformOperator {
		return nil
	}
	sessionDB := qctx.Session.State().LogicalDatabaseName()
	var worst severity
	var worstErr error
	for _, a := range accessesOf(qctx, sessionDB) {
		sev, err := decide(qctx.TableSnapshot.Lookup(a.database, a.table), a.class)
		if sev > worst {
			worst, worstErr = sev, err
		}
	}
	return worstErr
}

// decide is the §7.2 matrix plus the §7.3 rules for one accessed table.
func decide(t sitable.Table, c class) (severity, error) {
	switch t.Status {
	case sitable.Pending:
		switch c {
		case classRead, classWrite:
			return retryable, refuse(CodeRetryable, "storage_integrity: table %s is pending activation (retryable)", t.ID)
		case classCreateData, classViewTarget:
			return nonRetryable, dataCarrying(t.ID)
		case classOtherDDL:
			return nonRetryable, refuse(CodeNonRetryable, "storage_integrity: table %s is governed by storage integrity; ALTER and RENAME are not supported", t.ID)
		}
	case sitable.Refused:
		switch c {
		case classRead, classWrite, classOtherDDL, classViewTarget:
			return nonRetryable, refuse(CodeNonRetryable, "storage_integrity: table %s was refused: %s: %s", t.ID, t.RefusedCode, t.RefusedReason)
		}
	case sitable.Active:
		switch c {
		case classCreateData, classViewTarget:
			return nonRetryable, dataCarrying(t.ID)
		}
	case sitable.Gone:
		switch c {
		case classNone:
		case classCreate, classCreateData:
			return retryable, refuse(CodeRetryable, "storage_integrity: table %s is still being purged; retry CREATE later (retryable)", t.ID)
		default:
			return unknownTable, refuse(CodeUnknownTable, "Table %s does not exist", t.ID)
		}
	}
	return allow, nil
}

func dataCarrying(id string) error {
	return refuse(CodeNonRetryable, "storage_integrity: table %s is governed by storage integrity and cannot be created with data; create the table first, then INSERT", id)
}

func refuse(code int32, format string, args ...any) error {
	return &chproto.ClientError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// accessesOf assigns a class to every accessed table from the rewriter's
// statement type. Unrecognised types with accessed tables are treated as data
// reads, the conservative class.
func accessesOf(qctx *plugin.QueryContext, sessionDB string) []access {
	tables := qctx.AccessedTables
	out := make([]access, 0, len(tables))
	add := func(t sqlmeta.AccessedTable, c class) {
		db := t.LogicalDatabase
		if db == "" {
			db = t.OriginalDatabase
		}
		if db == "" {
			db = sessionDB
		}
		out = append(out, access{database: db, table: t.OriginalTable, class: c})
	}
	sql := qctx.OriginalSQL
	switch qctx.StatementType {
	case sqlmeta.StatementTypeCreateDatabase, sqlmeta.StatementTypeDropDatabase,
		sqlmeta.StatementTypeGrant, sqlmeta.StatementTypeRevoke:
		return nil
	case sqlmeta.StatementTypeInsert:
		for i, t := range tables {
			add(t, pick(i == 0, classWrite, classRead))
		}
	case sqlmeta.StatementTypeUpdate, sqlmeta.StatementTypeDelete, sqlmeta.StatementTypeTruncateTable:
		for _, t := range tables {
			add(t, classWrite)
		}
	case sqlmeta.StatementTypeDescribe, sqlmeta.StatementTypeShowCreateTable, sqlmeta.StatementTypeExistsTable,
		sqlmeta.StatementTypeShowTables, sqlmeta.StatementTypeShowDatabases, sqlmeta.StatementTypeUse:
		for _, t := range tables {
			add(t, classMetadata)
		}
	case sqlmeta.StatementTypeCreateTable:
		withData := createTableCarriesData(sql)
		for i, t := range tables {
			switch {
			case i == 0:
				add(t, pick(withData, classCreateData, classCreate))
			default:
				add(t, pick(withData, classRead, classMetadata))
			}
		}
	case sqlmeta.StatementTypeCreateView:
		for i, t := range tables {
			add(t, pick(i == 0, classCreate, classRead))
		}
	case sqlmeta.StatementTypeCreateMaterializedView:
		populate, toDB, toTable, hasTo := materializedViewHeader(sql)
		for i, t := range tables {
			switch {
			case i == 0:
				add(t, pick(populate, classCreateData, classCreate))
			case hasTo && t.OriginalTable == toTable && t.OriginalDatabase == toDB:
				add(t, classViewTarget)
			default:
				add(t, classRead)
			}
		}
	case sqlmeta.StatementTypeDropTable, sqlmeta.StatementTypeDropView:
		for _, t := range tables {
			add(t, classDrop)
		}
	case sqlmeta.StatementTypeAlterTable, sqlmeta.StatementTypeRenameTable:
		for _, t := range tables {
			add(t, classOtherDDL)
		}
	default:
		for _, t := range tables {
			add(t, classRead)
		}
	}
	return out
}

func pick(cond bool, yes, no class) class {
	if cond {
		return yes
	}
	return no
}

// RunOnPeerTrust opts out like rewrite: a peer-trusted remote() loopback runs
// SQL its origin already decided. IsForwardedFromPeer overrides this in the
// chain, so the receiving host of a forward pivot runs the plugin.
func (*Plugin) RunOnPeerTrust() bool { return false }

// RunOnForward opts out on the origin side of a forward pivot; the host that
// owns the database decides.
func (*Plugin) RunOnForward() bool { return false }

var (
	_ plugin.QueryPlugin    = (*Plugin)(nil)
	_ plugin.PeerTrustAware = (*Plugin)(nil)
	_ plugin.ForwardAware   = (*Plugin)(nil)
)
```

- [ ] **Step 4: Run the plugin tests**

Run: `bazel run //:gazelle && go test -count=1 -race ./pkg/plugins/sitablestate/ && bazel test //pkg/plugins/sitablestate:sitablestate_test`

Expected: PASS, including `TestOneSnapshotPerQuery` (the fake moves to a version in which the table is Gone between two hooks, and the query still completes under version 1).

- [ ] **Step 5: Write the failing wiring test**

In `storage_integrity_table_state_test.go`, replace:

```go
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugins/rewrite"
	"github.com/housegate/housegate/pkg/plugins/sireserved"
```

with:

```go
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugins/commitgate"
	"github.com/housegate/housegate/pkg/plugins/rewrite"
	"github.com/housegate/housegate/pkg/plugins/sireserved"
	"github.com/housegate/housegate/pkg/plugins/sitablestate"
	"github.com/housegate/housegate/pkg/plugins/storageintegrity"
```

In `storage_integrity_table_state_test.go`, append two tests after `TestBuildServer_InjectedTableStateEnablesTheSurface` — replace its tail:

```go
	if !guarded {
		t.Fatal("an enabled deployment must wire the reserved-name guard")
	}
}
```

with:

```go
	if !guarded {
		t.Fatal("an enabled deployment must wire the reserved-name guard")
	}
}

// TestBuildServer_TableStateGateWiring is spec 2026-09-24 §7.1: sitablestate
// runs after rewrite and before the SI ingress and commitgate.
func TestBuildServer_TableStateGateWiring(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Enabled = boolPtr(true)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
	fake := sitable.NewFake(sitable.Pending)
	bs, err := buildServer(Options{
		Config:                            cfg,
		NetworkState:                      network.NewInMemoryNetworkState(),
		Rewriter:                          siProbeStubRewriterFactory{},
		StorageIntegrityTableState:        fake,
		StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{},
	}, nil)
	if err != nil {
		t.Fatalf("build with an injected table state: %v", err)
	}
	defer bs.teardown()
	rewriteAt, gateAt, ingressAt, commitAt := -1, -1, -1, -1
	for i, candidate := range requireExternalChain(t, bs).QueryPlugins {
		switch candidate.(type) {
		case *rewrite.Plugin:
			rewriteAt = i
		case *sitablestate.Plugin:
			gateAt = i
		case *storageintegrity.Plugin:
			ingressAt = i
		case *commitgate.Plugin:
			commitAt = i
		}
	}
	if !(rewriteAt >= 0 && rewriteAt < gateAt && gateAt < ingressAt && ingressAt < commitAt) {
		t.Fatalf("plugin order rewrite=%d sitablestate=%d ingress=%d commitgate=%d, want strictly increasing", rewriteAt, gateAt, ingressAt, commitAt)
	}
}

func TestBuildServer_DisabledWiresNoTableStateGate(t *testing.T) {
	bs, err := buildServer(Options{
		Config:       minimalServerCfg(t),
		NetworkState: network.NewInMemoryNetworkState(),
		Rewriter:     stubRewriterFactory{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.teardown()
	for _, candidate := range requireExternalChain(t, bs).QueryPlugins {
		if _, ok := candidate.(*sitablestate.Plugin); ok {
			t.Fatal("a disabled deployment must not wire sitablestate")
		}
		if p, ok := candidate.(*rewrite.Plugin); ok && p.TableState != nil {
			t.Fatal("a disabled deployment must not give the rewrite plugin a table state")
		}
	}
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `go test -vet=off -count=1 -run "TestBuildServer_TableStateGateWiring|TestBuildServer_DisabledWiresNoTableStateGate" .`

Expected: FAIL: `plugin order rewrite=N sitablestate=-1 ...`.

- [ ] **Step 7: Register the plugin after rewrite**

In `build.go`, replace:

```go
	"github.com/housegate/housegate/pkg/plugins/sistatement"
```

with:

```go
	"github.com/housegate/housegate/pkg/plugins/sistatement"
	"github.com/housegate/housegate/pkg/plugins/sitablestate"
```

In `build.go`, replace:

```go
	if rewritePlug != nil {
		queryPlugins = append(queryPlugins, rewritePlug)
	}

```

with:

```go
	if rewritePlug != nil {
		queryPlugins = append(queryPlugins, rewritePlug)
	}
	// sitablestate reads the rewriter's classification and the query's
	// snapshot, so it runs right after rewrite and before the SI ingress and
	// commitgate (spec 2026-09-24 §7.1).
	if siOptions.Enabled {
		queryPlugins = append(queryPlugins, &sitablestate.Plugin{})
		log.Infow("storage-integrity table-state gate enabled", "table_state", storageIntegrityTableStateLabel(siStatic))
	}

```

- [ ] **Step 8: Run the build tests**

Run: `go test -vet=off -count=1 . && bazel test //:housegate_test`

Expected: PASS.

- [ ] **Step 9: Document the plugin**

In `CLAUDE.md`, replace:

```markdown
`route` (split into `Stripper` / `Signer`, both `RouteAware`)
```

with:

```markdown
`route` (split into `Stripper` / `Signer`, both `RouteAware`), `sitablestate` (server-mode, registered only when `storage_integrity.enabled`, right after `rewrite` and before the SI ingress and commitgate; reads the query's `TableSnapshot`, `StatementType` and `AccessedTables` and applies the spec 2026-09-24 §7 matrix: Pending data reads/writes fail retryable with code 733 `TABLE_IS_BEING_RESTARTED`, Refused ones non-retryable with code 392 `QUERY_IS_PROHIBITED` naming the arbiter's refused code and reason, Pending/Refused `ALTER`/`RENAME` and data-carrying creation into a Pending or Active table (`CREATE TABLE ... AS SELECT` without `EMPTY`, `CREATE MATERIALIZED VIEW ... POPULATE` or `TO` a governed table, detected by a top-level CREATE-header lexer because the rewriter's classification does not distinguish them) are non-retryable, and a Gone table is answered with code 60 `UNKNOWN_TABLE` except for a same-name CREATE, which is retryable (spec H4); precedence is unknown table, then non-retryable, then retryable; metadata statements, `DROP TABLE` and same-name `CREATE` pass for Pending and Refused tables. Direct access to `hg_*` is not this plugin's concern: the engines refuse it from `StorageIntegrityArgs.reserved_databases`, which HouseGate always sends. Opts out of peer-trusted and origin-side forwarding sessions like rewrite; maintenance and platform-operator sessions bypass it)
```

In `pkg/plugins/AGENTS.md`, replace:

```markdown
|-- sisnapshotquery/ # agent-side signed INSERT ... SELECT preparation (default-off, injection-only ports)
```

with:

```markdown
|-- sisnapshotquery/ # agent-side signed INSERT ... SELECT preparation (default-off, injection-only ports)
|-- sitablestate/   # server-side SI table-status gate: Pending/Refused/Gone refusals (spec 2026-09-24 §7)
```

In `pkg/plugins/AGENTS.md`, replace:

```markdown
auth, forward, rewrite, indexing usage, commitgate, and storage-integrity ingress opt out where
```

with:

```markdown
auth, forward, rewrite, sitablestate, indexing usage, commitgate, and storage-integrity ingress opt out where
```

In `pkg/plugins/AGENTS.md`, replace:

```markdown
rewrite, indexing usage, commitgate, and storage-integrity ingress opt out so the receiving host
```

with:

```markdown
rewrite, sitablestate, indexing usage, commitgate, and storage-integrity ingress opt out so the receiving host
```

In `CLAUDE.md`, replace:

```markdown
Production opt-outs: auth, forward, rewrite, indexing-usage, commitgate, and storage-integrity ingress.
```

with:

```markdown
Production opt-outs: auth, forward, rewrite, sitablestate, indexing-usage, commitgate, and storage-integrity ingress.
```

In `CLAUDE.md`, replace:

```markdown
Rewrite, indexing-usage, commitgate, and storage-integrity ingress opt out; the receiving host runs the work on its own session.
```

with:

```markdown
Rewrite, sitablestate, indexing-usage, commitgate, and storage-integrity ingress opt out; the receiving host runs the work on its own session.
```

- [ ] **Step 10: Commit**

```bash
git add pkg/plugins/sitablestate/ pkg/chproto/client_error.go pkg/plugins/AGENTS.md build.go BUILD.bazel storage_integrity_table_state_test.go CLAUDE.md
git commit -m "$(cat <<'EOF'
feat(sitablestate): refuse what a table's storage-integrity status does not allow

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: The signed ingress reads the query snapshot: registry schema and hash, Active only, the stale-view refusal

**Files:**

- Modify: `pkg/plugins/storageintegrity/plugin.go:22-31` (imports), `:98-100` (`Admission.TableSchema`), `:117-119`, `:221-241` (target before statement id, stale-view refusal), `:254-267`, `:278-282`, `:520-523`, `:873` (helpers)
- Modify: `build.go:759-763` (declared-schema loader only when disabled), `CLAUDE.md` (`storageintegrity` plugin entry)
- Test: create `pkg/plugins/storageintegrity/table_state_test.go`; append to `storage_integrity_table_state_test.go`

**Interfaces:**

- Consumes: `QueryContext.TableSnapshot` (Task 4), `chproto.CodeTableIsBeingRestarted` (Task 5).
- Produces (package `storageintegrity`, the ingress plugin):

```go
type Admission struct {
	// ... existing fields
	TableSchema *payloadexec.TableSchema // the snapshot's registry schema; nil when storage integrity is disabled
}
```

With a snapshot the ingress admits only an Active target, binds `Table.SchemaHash` (the registry's, not the latest chain declaration) and refuses an INSERT without `SQL_x_statement_token` with `chproto.ClientError{Code: 733, Message: "storage_integrity: table <id> requires a signed INSERT; the client's table state is stale (retryable)"}`. Without a snapshot (storage integrity disabled, a rewriter marking tables SI on its own) the declared network-state loader path is unchanged.

- [ ] **Step 1: Write the failing tests**

Create `pkg/plugins/storageintegrity/table_state_test.go`:

```go
package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// registrySchema differs from the network state's latest declaration
// (ingressSchema) by one column, as a registry schema that is the first
// declaration after creation can (umbrella D3).
func registrySchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: "tenant.events", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}}}
}

func activeSnapshot(status sitable.Status) sitable.Snapshot {
	schema := registrySchema()
	return sitable.NewSnapshot(3, sitable.Ordinary, []sitable.Table{{
		ID: "tenant.events", Status: status, Schema: schema, SchemaHash: payloadexec.TableSchemaHash("testnet-v2", schema),
	}})
}

func dropStatementToken(qctx *plugin.QueryContext) {
	kept := qctx.Query.Settings[:0]
	for _, s := range qctx.Query.Settings {
		if s.Key != auth.StatementTokenSettingKey {
			kept = append(kept, s)
		}
	}
	qctx.Query.Settings = kept
}

func snapshotQueryContext(t *testing.T, id int64, signer *auth.RelaySigner, snap sitable.Snapshot) *plugin.QueryContext {
	t.Helper()
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, id, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{IsStorageIntegrity: true, OriginalDatabase: "tenant", OriginalTable: "events", LogicalDatabase: "tenant"}}
	qctx.TableSnapshot = snap
	dropStatementToken(qctx)
	return qctx
}

// TestIngressSnapshotSchemaWinsOverTheLatestDeclaration is spec 2026-09-24
// §9.1: with a snapshot the ingress binds the registry schema and hash, not
// the network state's latest chain declaration.
func TestIngressSnapshotSchemaWinsOverTheLatestDeclaration(t *testing.T) {
	p, signer, latestHash := newV2Ingress(t)
	registryHash := payloadexec.TableSchemaHash("testnet-v2", registrySchema())
	if registryHash == latestHash {
		t.Fatal("fixture: the two schemas must hash differently")
	}
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}

	qctx := snapshotQueryContext(t, 40, signer, activeSnapshot(sitable.Active))
	withStatementToken(t, qctx, signer, v2Statement(signer, qctx.Query.ID, qctx.OriginalSQL, registryHash, payload, 54453))
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatal(err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	adm, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("a token over the registry schema must be admitted: %v", err)
	}
	if adm.SchemaHash != registryHash || adm.TableSchema == nil || len(adm.TableSchema.Columns) != 1 {
		t.Fatalf("admission schema = %s %+v, want the registry schema", adm.SchemaHash, adm.TableSchema)
	}

	stale := snapshotQueryContext(t, 41, signer, activeSnapshot(sitable.Active))
	withStatementToken(t, stale, signer, v2Statement(signer, stale.Query.ID, stale.OriginalSQL, latestHash, payload, 54453))
	if err := p.OnQuery(context.Background(), stale); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), stale, payload); err != nil {
		t.Fatal(err)
	}
	p.OnQueryInputComplete(context.Background(), stale)
	if _, err := p.ConsumeAdmission(stale.Session.ID()); err == nil || !strings.Contains(err.Error(), "statement token rejected") {
		t.Fatalf("a token over the latest declaration must be rejected, got %v", err)
	}
}

func TestIngressSnapshotAdmitsOnlyActiveTables(t *testing.T) {
	p, signer, _ := newV2Ingress(t)
	for _, status := range []sitable.Status{sitable.Pending, sitable.Refused, sitable.Gone, sitable.Ordinary} {
		qctx := snapshotQueryContext(t, 42, signer, activeSnapshot(status))
		withDefaultCaptureToken(t, qctx, signer, []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd})
		err := p.OnQuery(context.Background(), qctx)
		if err == nil || !strings.Contains(err.Error(), "is not active") {
			t.Fatalf("status %s: err = %v, want an active-only refusal", status, err)
		}
	}
}

// TestIngressStaleViewUnsignedInsertIsRetryable is spec 2026-09-24 §10.3.
func TestIngressStaleViewUnsignedInsertIsRetryable(t *testing.T) {
	p, signer, _ := newV2Ingress(t)
	qctx := snapshotQueryContext(t, 43, signer, activeSnapshot(sitable.Active))
	qctx.Query.ID = "8d7f5b0e-client-generated"
	err := p.OnQuery(context.Background(), qctx)
	var ce *chproto.ClientError
	if !errors.As(err, &ce) || ce.Code != chproto.CodeTableIsBeingRestarted ||
		ce.Message != "storage_integrity: table tenant.events requires a signed INSERT; the client's table state is stale (retryable)" {
		t.Fatalf("err = %v, want the retryable stale-view refusal", err)
	}
	if ce.KeepSession {
		t.Fatal("an OnQuery refusal already keeps the session; KeepSession must stay unset")
	}
}

// TestIngressWithoutSnapshotKeepsTheLoaderPath pins that a disabled
// deployment (no snapshot) still resolves the declared schema and names a
// missing statement id, exactly as before.
func TestIngressWithoutSnapshotKeepsTheLoaderPath(t *testing.T) {
	p, signer, _ := newV2Ingress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 44, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.Query.ID = ""
	if err := p.OnQuery(context.Background(), qctx); err == nil || !strings.Contains(err.Error(), "query id is required") {
		t.Fatalf("err = %v, want the unchanged statement-id error", err)
	}
}
```

In `storage_integrity_table_state_test.go`, replace:

```go
	"github.com/housegate/housegate/pkg/plugins/storageintegrity"
```

with:

```go
	"github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/registry"
```

In `storage_integrity_table_state_test.go`, append after `TestBuildServer_DisabledWiresNoTableStateGate` — replace its tail:

```go
		if p, ok := candidate.(*rewrite.Plugin); ok && p.TableState != nil {
			t.Fatal("a disabled deployment must not give the rewrite plugin a table state")
		}
	}
}
```

with:

```go
		if p, ok := candidate.(*rewrite.Plugin); ok && p.TableState != nil {
			t.Fatal("a disabled deployment must not give the rewrite plugin a table state")
		}
	}
}

// registryOnly exposes only registry.Registry, hiding the declared-schema
// view the in-memory state also implements.
type registryOnly struct{ registry.Registry }

// TestBuildServer_EnabledIngressNeedsNoDeclaredSchemaSource pins spec
// 2026-09-24 §9.1: with storage integrity enabled the ingress binds the
// query snapshot's schema, so it needs no registry.TableSchemas source; the
// disabled ingress still does.
func TestBuildServer_EnabledIngressNeedsNoDeclaredSchemaSource(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Enabled = boolPtr(true)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
	bs, err := buildServer(Options{
		Config:                            cfg,
		NetworkState:                      registryOnly{network.NewInMemoryNetworkState()},
		Rewriter:                          siProbeStubRewriterFactory{},
		StorageIntegrityTableState:        sitable.NewFake(sitable.Pending),
		StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{},
	}, nil)
	if err != nil {
		t.Fatalf("an enabled ingress must build without a declared-schema source: %v", err)
	}
	bs.teardown()

	cfg.StorageIntegrity.Enabled = nil
	_, err = buildServer(Options{
		Config:                            cfg,
		NetworkState:                      registryOnly{network.NewInMemoryNetworkState()},
		Rewriter:                          stubRewriterFactory{},
		StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "implements registry.TableSchemas") {
		t.Fatalf("err = %v, want the disabled ingress to require a declared-schema source", err)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -vet=off -count=1 ./pkg/plugins/storageintegrity/ && go test -vet=off -count=1 -run TestBuildServer_EnabledIngressNeedsNoDeclaredSchemaSource .`

Expected: FAIL, build failed: `adm.TableSchema undefined (type Admission has no field or method TableSchema)`; the root test (once the package builds) fails with `an enabled ingress must build without a declared-schema source: storage_integrity.ingress requires a NetworkState that implements registry.TableSchemas`.

- [ ] **Step 3: Bind the snapshot in the ingress**

In `pkg/plugins/storageintegrity/plugin.go`, replace:

```go
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chsession"
```

with:

```go
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
```

In `pkg/plugins/storageintegrity/plugin.go`, replace:

```go
	"github.com/housegate/housegate/pkg/schemaregistry"
```

with:

```go
	"github.com/housegate/housegate/pkg/schemaregistry"
	"github.com/housegate/housegate/pkg/sitable"
```

In `pkg/plugins/storageintegrity/plugin.go`, replace:

```go
	SchemaHash      string
	RowIDProfileID  string
	Payload         CapturedPayload
}
```

with:

```go
	SchemaHash      string
	RowIDProfileID  string
	Payload         CapturedPayload
	// TableSchema is the target's schema from the query's table-state
	// snapshot (spec 2026-09-24 §9.1). Nil when storage integrity is disabled
	// and the schema came from the declared network-state loader.
	TableSchema *payloadexec.TableSchema
}
```

In `pkg/plugins/storageintegrity/plugin.go`, replace:

```go
	statementToken  string
	schemaHash      string
	complete        bool
}
```

with:

```go
	statementToken  string
	schemaHash      string
	tableSchema     *payloadexec.TableSchema
	complete        bool
}
```

In `pkg/plugins/storageintegrity/plugin.go`, replace:

```go
	payloadEncoding, err := requirePayloadLocalInsert(signedSQL)
	if err != nil {
		return err
	}
	stmtID, err := statementID(qctx)
	if err != nil {
		return err
	}
```

with:

```go
	payloadEncoding, err := requirePayloadLocalInsert(signedSQL)
	if err != nil {
		return err
	}
	target, err := resolveTargetTable(qctx, signedSQL)
	if err != nil {
		return err
	}
	// Spec 2026-09-24 §10.3: an agent whose view is stale passes an INSERT
	// into an Active table through unsigned. Name that cause, retryable,
	// before the statement-id check reports a malformed id the client never
	// had. Only the snapshot path knows the table is Active.
	if qctx.TableSnapshot != nil && !hasStatementToken(qctx) {
		return &chproto.ClientError{Code: chproto.CodeTableIsBeingRestarted,
			Message: fmt.Sprintf("storage_integrity: table %s requires a signed INSERT; the client's table state is stale (retryable)", target.id)}
	}
	stmtID, err := statementID(qctx)
	if err != nil {
		return err
	}
```

In `pkg/plugins/storageintegrity/plugin.go`, replace:

```go
	target, err := resolveTargetTable(qctx, signedSQL)
	if err != nil {
		return err
	}
	tableID := target.id
```

with:

```go
	tableID := target.id
```

In `pkg/plugins/storageintegrity/plugin.go`, replace:

```go
	if p.schemaLoader == nil || strings.TrimSpace(p.networkID) == "" {
```

with:

```go
	if (qctx.TableSnapshot == nil && p.schemaLoader == nil) || strings.TrimSpace(p.networkID) == "" {
```

In `pkg/plugins/storageintegrity/plugin.go`, replace:

```go
	schemaHash, err := p.resolveSchemaHash(ctx, target)
	if err != nil {
		return err
	}
```

with:

```go
	var (
		schemaHash  string
		tableSchema *payloadexec.TableSchema
	)
	if qctx.TableSnapshot != nil {
		tableSchema, schemaHash, err = snapshotSchema(qctx.TableSnapshot, target)
	} else {
		schemaHash, err = p.resolveSchemaHash(ctx, target)
	}
	if err != nil {
		return err
	}
```

In `pkg/plugins/storageintegrity/plugin.go`, replace:

```go
		payloadEncoding: payloadEncoding,
		statementToken:  statementToken,
		schemaHash:      schemaHash,
	}
```

with:

```go
		payloadEncoding: payloadEncoding,
		statementToken:  statementToken,
		schemaHash:      schemaHash,
		tableSchema:     tableSchema,
	}
```

In `pkg/plugins/storageintegrity/plugin.go`, replace:

```go
	admission.SchemaHash = state.schemaHash
	admission.RowIDProfileID = payloadexec.RowIDProfileID
```

with:

```go
	admission.SchemaHash = state.schemaHash
	admission.TableSchema = state.tableSchema
	admission.RowIDProfileID = payloadexec.RowIDProfileID
```

In `pkg/plugins/storageintegrity/plugin.go`, replace:

```go
func normalizeStructuredTablePath(db, table string) (string, error) {
```

with:

```go
func hasStatementToken(qctx *plugin.QueryContext) bool {
	for _, setting := range qctx.Query.Settings {
		if setting.Key == auth.StatementTokenSettingKey {
			return true
		}
	}
	return false
}

// snapshotSchema is spec 2026-09-24 §9.1: the ingress admits only an Active
// table and binds the schema and hash the query's snapshot carries, the
// registry's schema rather than the latest chain declaration.
func snapshotSchema(snap sitable.Snapshot, target resolvedTableTarget) (*payloadexec.TableSchema, string, error) {
	table := snap.Lookup(target.database, target.table)
	if table.Status != sitable.Active {
		return nil, "", fmt.Errorf("storage_integrity table %s is not active (status %s); the signed lane admits only active tables", target.id, table.Status)
	}
	if table.SchemaHash == "" || table.Schema.TableID == "" {
		return nil, "", fmt.Errorf("storage_integrity cannot resolve the schema of active table %s", target.id)
	}
	schema := table.Schema
	return &schema, table.SchemaHash, nil
}

func normalizeStructuredTablePath(db, table string) (string, error) {
```

In `build.go`, replace:

```go
		ingressCfg := cfg.StorageIntegrity.Ingress
		ingressSchemas, err := resolveTableSchemas(opts, reg, "storage_integrity.ingress")
		if err != nil {
			return nil, err
		}
```

with:

```go
		ingressCfg := cfg.StorageIntegrity.Ingress
		// With storage integrity enabled the ingress reads each query's
		// snapshot; the declared-schema loader serves only the disabled case,
		// where a rewriter marks the table SI on its own.
		var ingressSchemas registry.TableSchemas
		if !siOptions.Enabled {
			ingressSchemas, err = resolveTableSchemas(opts, reg, "storage_integrity.ingress")
			if err != nil {
				return nil, err
			}
		}
```

- [ ] **Step 4: Run the tests**

Run: `bazel run //:gazelle && go test -vet=off -count=1 ./pkg/plugins/storageintegrity/ . && bazel test //pkg/plugins/storageintegrity:storageintegrity_test //:housegate_test`

Expected: PASS; the existing ingress tests (no snapshot) are unchanged.

- [ ] **Step 5: Document the ingress binding**

In `CLAUDE.md`, replace:

```markdown
`storageintegrity` (server-mode ingress validates the v2 statement token against its own exact Native-byte capture, the network-state `schema_hash`, `settings_hash == EmptySettingsHash`, and the `statement_kind` it classified itself;
```

with:

```markdown
`storageintegrity` (server-mode ingress validates the v2 statement token against its own exact Native-byte capture, the `schema_hash` of the target's registry schema in the query's `TableSnapshot` (admitting only an Active target; the declared network-state schema only when storage integrity is disabled), `settings_hash == EmptySettingsHash`, and the `statement_kind` it classified itself; an INSERT into an Active table that carries no `SQL_x_statement_token` is refused with the retryable code-733 `requires a signed INSERT; the client's table state is stale` message before the statement-id check;
```

- [ ] **Step 6: Commit**

```bash
git add pkg/plugins/storageintegrity/ build.go BUILD.bazel storage_integrity_table_state_test.go CLAUDE.md
git commit -m "$(cat <<'EOF'
feat(storage-integrity): bind the ingress to the query snapshot's registry schema

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Runtime over the table state: per-table merge-guard health, a Changed-driven supervisor, Static-only schema checks

**Files:**

- Create: `pkg/storageintegrity/merge_guard_tables.go`
- Replace: `storage_integrity_merge_supervisor.go` (whole file), `storage_integrity_runtime.go` (whole file)
- Modify: `storage_integrity_ingress.go:50,82,91,507-511`, `build.go:732,743,1001`, `CLAUDE.md` (merge-guard rough edge)
- Test: create `pkg/storageintegrity/merge_guard_tables_test.go`, `storage_integrity_runtime_dynamic_test.go`; replace `storage_integrity_merge_supervisor_test.go`; modify `storage_integrity_ingress_test.go:279-289`, `build_test.go` (22 runtime calls, 7 startup calls, `orderedBuildMergeGuard`, `recordingBuildMergeGuard`, `TestBuildStorageIntegrityRuntimeWrapsMergeSupervisor`)

**Interfaces:**

- Consumes: `sitable.TableState` (Task 2), `sicore.PhysicalTableName`.
- Produces (package `storageintegrity`):

```go
var MergeGuardDatabases = []string{"hg_safe", "hg_unsafe"}
var ErrMergeGuardTableMissing error
type MergeGuardReport struct {
	Tables map[string]error     // one entry per requested Active table id; nil = healthy
	Other  map[MergeTable]error // present hg_* tables of no requested id (Pending / Gone)
}
func (g *MergeGuard) AssertTables(ctx context.Context, activeTableIDs []string) (MergeGuardReport, error)
```

- Produces (package `housegate`) — the injected-guard interface change of spec §9.4:

```go
type StorageIntegrityMergeGuard interface {
	AssertTables(ctx context.Context, activeTableIDs []string) (sicore.MergeGuardReport, error)
}
type StorageIntegrityMergeHealth interface { CheckMergeHealth(tableID string) error }
func NewStorageIntegrityMergeSupervisor(guard StorageIntegrityMergeGuard, state sitable.TableState, interval time.Duration) *StorageIntegrityMergeSupervisor
func (s *StorageIntegrityMergeSupervisor) Assert(ctx context.Context) error
func (s *StorageIntegrityMergeSupervisor) CheckMergeHealth(tableID string) error
func (s *StorageIntegrityMergeSupervisor) Unhealthy() []string
func (s *StorageIntegrityMergeSupervisor) Run(ctx context.Context)
func buildStorageIntegrityRuntimeConsumer(runtimeCfg config.StorageIntegrityRuntimeConfig, state sitable.TableState, static *sitable.Static, opts StorageIntegrityRuntimeOptions) (*StorageIntegrityIngress, *StorageIntegrityMergeSupervisor, error)
func startStorageIntegrityRuntime(ctx context.Context, runtime *StorageIntegrityIngress, guard *StorageIntegrityMergeSupervisor, failOnTableError bool) error
func tableStateSchemaResolver(state sitable.TableState) StorageIntegrityTableSchemaResolver
func NewStorageIntegrityIngress(orch *sicore.Orchestrator, guard StorageIntegrityMergeHealth, matKind sicore.MaterializerKind) (*StorageIntegrityIngress, error)
```

`AssertStopMerges` stays on `*sicore.MergeGuard` for its existing fixed-table callers (`pkg/integration/storage_integrity_merge_guard_test.go`); the runtime no longer calls it. `runtimeTableSchemaResolver` and `storageIntegrityMergeTables` are removed.

- [ ] **Step 1: Write the failing merge-guard tests**

Create `pkg/storageintegrity/merge_guard_tables_test.go`:

```go
package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func tablesConn() *fakeMergeConn {
	return &fakeMergeConn{engines: map[MergeTable]string{
		{Database: "hg_safe", Table: "db1__ok"}:         pinnedEngine,
		{Database: "hg_unsafe", Table: "db1__ok"}:       pinnedEngine,
		{Database: "hg_safe", Table: "db1__unpinned"}:   "MergeTree ORDER BY _hg_row_id",
		{Database: "hg_unsafe", Table: "db1__unpinned"}: pinnedEngine,
		{Database: "hg_safe", Table: "db1__pending"}:    pinnedEngine,
	}}
}

func TestMergeGuardAssertTablesReportsPerTable(t *testing.T) {
	conn := tablesConn()
	report, err := NewMergeGuard(conn, nil).AssertTables(context.Background(), []string{"db1.ok", "db1.unpinned", "db1.missing"})
	if err != nil {
		t.Fatal(err)
	}
	if report.Tables["db1.ok"] != nil {
		t.Fatalf("db1.ok = %v, want healthy", report.Tables["db1.ok"])
	}
	if !errors.Is(report.Tables["db1.unpinned"], ErrMergeSettingNotPinned) {
		t.Fatalf("db1.unpinned = %v, want ErrMergeSettingNotPinned", report.Tables["db1.unpinned"])
	}
	if !errors.Is(report.Tables["db1.missing"], ErrMergeGuardTableMissing) {
		t.Fatalf("db1.missing = %v, want ErrMergeGuardTableMissing", report.Tables["db1.missing"])
	}
	if len(report.Tables) != 3 {
		t.Fatalf("report has %d table entries, want exactly the 3 requested", len(report.Tables))
	}
	// A Pending table's present hg_* table is checked, but absence of its
	// sibling is not an error.
	if err, ok := report.Other[MergeTable{Database: "hg_safe", Table: "db1__pending"}]; !ok || err != nil {
		t.Fatalf("Other[hg_safe.db1__pending] = %v (present %v), want checked and healthy", err, ok)
	}
	for _, stmt := range conn.execs {
		if strings.Contains(stmt, "db1__unpinned`") && strings.Contains(stmt, "hg_safe") {
			t.Fatalf("an unpinned table must never be started: %q", stmt)
		}
	}
	if !strings.Contains(strings.Join(conn.execs, "\n"), "SYSTEM START MERGES `hg_safe`.`db1__pending`") {
		t.Fatalf("a present Pending table must be started too: %v", conn.execs)
	}
}

func TestMergeGuardAssertTablesAttributesAMergeToItsTable(t *testing.T) {
	conn := tablesConn()
	conn.merging = []MergeTable{{Database: "hg_unsafe", Table: "db1__ok"}}
	report, err := NewMergeGuard(conn, nil).AssertTables(context.Background(), []string{"db1.ok"})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(report.Tables["db1.ok"], ErrNativeMergesEnabled) {
		t.Fatalf("db1.ok = %v, want ErrNativeMergesEnabled", report.Tables["db1.ok"])
	}
}

func TestMergeGuardAssertTablesEnumerationFailureIsGlobal(t *testing.T) {
	conn := tablesConn()
	conn.settingsErr = errors.New("clickhouse down")
	_, err := NewMergeGuard(conn, nil).AssertTables(context.Background(), []string{"db1.ok"})
	if err == nil || !strings.Contains(err.Error(), "clickhouse down") {
		t.Fatalf("err = %v, want the enumeration failure", err)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -vet=off -count=1 -run MergeGuardAssertTables ./pkg/storageintegrity/`

Expected: FAIL, build failed: `NewMergeGuard(conn, nil).AssertTables undefined`, `undefined: ErrMergeGuardTableMissing`. (`-vet=off` because `go vet` reports a pre-existing `suspect or` at `intake_test.go:1802`; Bazel does not run that analyzer.)

- [ ] **Step 3: Add the per-table assertion**

Create `pkg/storageintegrity/merge_guard_tables.go`:

```go
package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// MergeGuardDatabases are the two databases whose tables the per-table guard
// enumerates. They equal the Spec C D2 physical homes.
var MergeGuardDatabases = []string{"hg_safe", "hg_unsafe"}

// ErrMergeGuardTableMissing reports an Active table whose hg_safe or
// hg_unsafe table does not exist.
var ErrMergeGuardTableMissing = errors.New("storageintegrity: guarded table missing")

// MergeGuardReport is one per-table assertion pass (spec 2026-09-24 §9.4).
// Tables holds exactly one entry per requested Active table id: nil when both
// of its hg_* tables exist, pin the merge setting and show no merge. Other
// maps each present hg_* table that belongs to no requested id (a Pending or
// Gone table's) to its outcome; absence there is never an error.
type MergeGuardReport struct {
	Tables map[string]error
	Other  map[MergeTable]error
}

// AssertTables keeps native merges off every table present in hg_safe and
// hg_unsafe and reports per logical table. activeTableIDs are required: both
// of their physical tables must exist. Every other present hg_* table is
// checked the same way but may be absent. The returned error is set only when
// the enumeration itself fails; it then applies to every table.
//
// Order per pass, as in AssertStopMerges: read engine settings, START MERGES
// on every present table that pins the setting (a table that does not is never
// started), then verify that none of them shows an in-flight merge.
func (g *MergeGuard) AssertTables(ctx context.Context, activeTableIDs []string) (MergeGuardReport, error) {
	report := MergeGuardReport{Tables: map[string]error{}, Other: map[MergeTable]error{}}
	engines, err := g.presentEngines(ctx)
	if err != nil {
		return report, err
	}
	owner := map[MergeTable]string{}
	for _, id := range activeTableIDs {
		phys := PhysicalTableName(id)
		for _, db := range MergeGuardDatabases {
			owner[MergeTable{Database: db, Table: phys}] = id
		}
	}
	tableErr := map[MergeTable]error{}
	var healthy []MergeTable
	for table, engine := range engines {
		if engineSettings(engine)[PinnedMergeSetting] != "0" {
			tableErr[table] = fmt.Errorf("%w: %s.%s", ErrMergeSettingNotPinned, table.Database, table.Table)
			continue
		}
		healthy = append(healthy, table)
	}
	sort.Slice(healthy, func(i, j int) bool {
		if healthy[i].Database != healthy[j].Database {
			return healthy[i].Database < healthy[j].Database
		}
		return healthy[i].Table < healthy[j].Table
	})
	var started []MergeTable
	for _, table := range healthy {
		stmt := fmt.Sprintf("SYSTEM START MERGES %s.%s", quoteMergeIdent(table.Database), quoteMergeIdent(table.Table))
		if err := g.conn.Exec(ctx, stmt); err != nil {
			tableErr[table] = fmt.Errorf("storageintegrity: START MERGES failed: %w", err)
			continue
		}
		started = append(started, table)
	}
	if len(started) > 0 {
		offenders, err := g.queryPairs(ctx, "SELECT database, table FROM system.merges WHERE "+tableFilter(started, "table"))
		if err != nil {
			for _, table := range started {
				tableErr[table] = fmt.Errorf("storageintegrity: verify merges probe failed: %w", err)
			}
		}
		for _, o := range offenders {
			table := MergeTable{Database: o[0], Table: o[1]}
			tableErr[table] = fmt.Errorf("%w: %s.%s", ErrNativeMergesEnabled, table.Database, table.Table)
		}
	}
	for _, id := range activeTableIDs {
		var errs []error
		phys := PhysicalTableName(id)
		for _, db := range MergeGuardDatabases {
			table := MergeTable{Database: db, Table: phys}
			if _, ok := engines[table]; !ok {
				errs = append(errs, fmt.Errorf("%w: %s.%s", ErrMergeGuardTableMissing, db, phys))
				continue
			}
			errs = append(errs, tableErr[table])
		}
		report.Tables[id] = errors.Join(errs...)
	}
	for table := range engines {
		if _, ok := owner[table]; !ok {
			report.Other[table] = tableErr[table]
		}
	}
	return report, nil
}

func (g *MergeGuard) presentEngines(ctx context.Context) (map[MergeTable]string, error) {
	quoted := make([]string, len(MergeGuardDatabases))
	for i, db := range MergeGuardDatabases {
		quoted[i] = quoteMergeString(db)
	}
	rows, err := g.conn.Query(ctx, "SELECT database, name, engine_full FROM system.tables WHERE database IN ("+strings.Join(quoted, ", ")+")")
	if err != nil {
		return nil, fmt.Errorf("storageintegrity: engine settings probe failed: %w", err)
	}
	defer rows.Close()
	engines := map[MergeTable]string{}
	for rows.Next() {
		var db, tbl, engine string
		if err := rows.Scan(&db, &tbl, &engine); err != nil {
			return nil, fmt.Errorf("storageintegrity: scan engine settings probe: %w", err)
		}
		engines[MergeTable{Database: db, Table: tbl}] = engine
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storageintegrity: read engine settings probe: %w", err)
	}
	return engines, nil
}

func tableFilter(tables []MergeTable, tableColumn string) string {
	preds := make([]string, 0, len(tables))
	for _, t := range tables {
		preds = append(preds, fmt.Sprintf("(database = %s AND %s = %s)", quoteMergeString(t.Database), tableColumn, quoteMergeString(t.Table)))
	}
	if len(preds) == 0 {
		return "0"
	}
	return strings.Join(preds, " OR ")
}
```

- [ ] **Step 4: Run the merge-guard tests**

Run: `go test -vet=off -count=1 ./pkg/storageintegrity/`

Expected: PASS, including the existing fixed-table `AssertStopMerges` tests.

- [ ] **Step 5: Write the failing supervisor and runtime tests**

Replace the whole of `storage_integrity_merge_supervisor_test.go` with:

```go
package housegate

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/sitable"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// controllableMergeGuard answers every requested id with its configured
// per-table error (nil = healthy) and records each pass's requested ids.
type controllableMergeGuard struct {
	mu        sync.Mutex
	tableErrs map[string]error
	globalErr error
	passes    [][]string
	called    chan struct{}
}

func (g *controllableMergeGuard) AssertTables(_ context.Context, ids []string) (sicore.MergeGuardReport, error) {
	g.mu.Lock()
	g.passes = append(g.passes, append([]string(nil), ids...))
	report := sicore.MergeGuardReport{Tables: map[string]error{}}
	for _, id := range ids {
		report.Tables[id] = g.tableErrs[id]
	}
	err := g.globalErr
	g.mu.Unlock()
	if g.called != nil {
		select {
		case g.called <- struct{}{}:
		default:
		}
	}
	return report, err
}

func (g *controllableMergeGuard) set(id string, err error) {
	g.mu.Lock()
	if g.tableErrs == nil {
		g.tableErrs = map[string]error{}
	}
	g.tableErrs[id] = err
	g.mu.Unlock()
}

func active(ids ...string) []sitable.Table {
	out := make([]sitable.Table, 0, len(ids))
	for _, id := range ids {
		out = append(out, sitable.Table{ID: id, Status: sitable.Active})
	}
	return out
}

func TestMergeSupervisorStartsClosedUntilAssertSucceeds(t *testing.T) {
	guard := &controllableMergeGuard{}
	supervisor := NewStorageIntegrityMergeSupervisor(guard, sitable.NewFake(sitable.Ordinary, active("db1.t")...), time.Second)
	if err := supervisor.CheckMergeHealth("db1.t"); err == nil || !strings.Contains(err.Error(), "not asserted") {
		t.Fatalf("initial health err = %v, want not asserted", err)
	}
	if err := supervisor.Assert(context.Background()); err != nil {
		t.Fatalf("Assert: %v", err)
	}
	if err := supervisor.CheckMergeHealth("db1.t"); err != nil {
		t.Fatalf("health after successful assert: %v", err)
	}
}

// TestMergeSupervisorHealthIsPerTable is spec 2026-09-24 §9.4: one unready
// table blocks only itself.
func TestMergeSupervisorHealthIsPerTable(t *testing.T) {
	guard := &controllableMergeGuard{}
	guard.set("db1.bad", sicore.ErrMergeGuardTableMissing)
	supervisor := NewStorageIntegrityMergeSupervisor(guard, sitable.NewFake(sitable.Ordinary, active("db1.good", "db1.bad")...), time.Second)
	err := supervisor.Assert(context.Background())
	if err == nil || !strings.Contains(err.Error(), "db1.bad") || strings.Contains(err.Error(), "db1.good") {
		t.Fatalf("Assert err = %v, want only db1.bad reported", err)
	}
	if err := supervisor.CheckMergeHealth("db1.good"); err != nil {
		t.Fatalf("a healthy table must admit while another is unready: %v", err)
	}
	if err := supervisor.CheckMergeHealth("db1.bad"); !errors.Is(err, sicore.ErrMergeGuardTableMissing) {
		t.Fatalf("db1.bad health = %v, want ErrMergeGuardTableMissing", err)
	}
	if got := strings.Join(supervisor.Unhealthy(), ","); got != "db1.bad" {
		t.Fatalf("Unhealthy = %q", got)
	}
}

func TestMergeSupervisorGlobalFailureClosesEveryTable(t *testing.T) {
	guard := &controllableMergeGuard{globalErr: errors.New("clickhouse reconnect failed")}
	supervisor := NewStorageIntegrityMergeSupervisor(guard, sitable.NewFake(sitable.Ordinary, active("db1.a", "db1.b")...), time.Second)
	if err := supervisor.Assert(context.Background()); err == nil {
		t.Fatal("a global failure must fail the pass")
	}
	for _, id := range []string{"db1.a", "db1.b"} {
		if err := supervisor.CheckMergeHealth(id); err == nil || !strings.Contains(err.Error(), "clickhouse reconnect failed") {
			t.Fatalf("%s health = %v, want the global failure", id, err)
		}
	}
	guard.mu.Lock()
	guard.globalErr = nil
	guard.mu.Unlock()
	if err := supervisor.Assert(context.Background()); err != nil {
		t.Fatalf("recovery Assert: %v", err)
	}
	if err := supervisor.CheckMergeHealth("db1.a"); err != nil {
		t.Fatalf("health after recovery: %v", err)
	}
}

func TestMergeSupervisorRunPeriodicallyReasserts(t *testing.T) {
	called := make(chan struct{}, 1)
	guard := &controllableMergeGuard{called: called}
	supervisor := NewStorageIntegrityMergeSupervisor(guard, sitable.NewFake(sitable.Ordinary, active("db1.t")...), time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go supervisor.Run(ctx)
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("periodic reassert did not run")
	}
}

// TestMergeSupervisorAssertsImmediatelyOnChange is spec 2026-09-24 §9.4: a
// newly Active table is asserted on Changed(), not after reassert_interval.
func TestMergeSupervisorAssertsImmediatelyOnChange(t *testing.T) {
	called := make(chan struct{}, 4)
	guard := &controllableMergeGuard{called: called}
	fake := sitable.NewFake(sitable.Ordinary, active("db1.t")...)
	supervisor := NewStorageIntegrityMergeSupervisor(guard, fake, time.Hour)
	if err := supervisor.Assert(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-called
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go supervisor.Run(ctx)
	if err := supervisor.CheckMergeHealth("db1.new"); err == nil {
		t.Fatal("a table no pass has asserted must fail closed")
	}
	fake.Set(active("db1.t", "db1.new")...)
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("a table-state change did not trigger a reassert")
	}
	deadline := time.Now().Add(time.Second)
	for supervisor.CheckMergeHealth("db1.new") != nil {
		if time.Now().After(deadline) {
			t.Fatalf("db1.new health = %v after the change-triggered pass", supervisor.CheckMergeHealth("db1.new"))
		}
		time.Sleep(5 * time.Millisecond)
	}
	guard.mu.Lock()
	last := guard.passes[len(guard.passes)-1]
	guard.mu.Unlock()
	if strings.Join(last, ",") != "db1.new,db1.t" {
		t.Fatalf("change-triggered pass asserted %v, want the new snapshot's sorted Active set", last)
	}
}
```

Create `storage_integrity_runtime_dynamic_test.go`:

```go
package housegate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/sitable"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

func dynamicRuntimePorts() StorageIntegrityRuntimeOptions {
	return StorageIntegrityRuntimeOptions{
		StatementSubmitter: &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}},
		SourcePreparer:     &rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}},
		StatusQuerier:      rootRecordingStatusQuerier{},
		PayloadWriter:      &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}},
		MergeGuard:         &recordingBuildMergeGuard{},
	}
}

// TestBuildStorageIntegrityRuntimeDynamicSkipsTheStaticSchemaChecks is spec
// 2026-09-24 §9.3: a dynamic host has no startup schema set and no
// config-to-schema bijection; the static set keeps both.
func TestBuildStorageIntegrityRuntimeDynamicSkipsTheStaticSchemaChecks(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)
	ingress, guard, err := buildStorageIntegrityRuntimeConsumer(cfg.StorageIntegrity.Runtime, sitable.NewFake(sitable.Pending), nil, dynamicRuntimePorts())
	if err != nil {
		t.Fatalf("a dynamic runtime needs no startup schema set: %v", err)
	}
	defer ingress.Close()
	if guard == nil {
		t.Fatal("the runtime must return its merge supervisor")
	}
	if _, _, err := buildStaticRuntimeConsumer(cfg.StorageIntegrity.Runtime, []string{"net1.events"}, dynamicRuntimePorts()); err == nil || !strings.Contains(err.Error(), "authoritative table schema set") {
		t.Fatalf("err = %v, want the static set to keep requiring its schemas", err)
	}
}

// TestStartStorageIntegrityRuntimeFailsFastOnlyForTheStaticSet is spec
// 2026-09-24 §9.4: a dynamic host starts with the unready table's latch
// closed; the static set keeps its fail-fast startup.
func TestStartStorageIntegrityRuntimeFailsFastOnlyForTheStaticSet(t *testing.T) {
	guard := &controllableMergeGuard{}
	guard.set("db1.bad", sicore.ErrMergeGuardTableMissing)
	supervisor := NewStorageIntegrityMergeSupervisor(guard, sitable.NewFake(sitable.Ordinary, active("db1.good", "db1.bad")...), time.Second)
	if err := startStorageIntegrityRuntime(context.Background(), nil, supervisor, false); err != nil {
		t.Fatalf("a dynamic host must start with one unready table: %v", err)
	}
	if err := supervisor.CheckMergeHealth("db1.good"); err != nil {
		t.Fatalf("the ready table must admit: %v", err)
	}
	if err := supervisor.CheckMergeHealth("db1.bad"); err == nil {
		t.Fatal("the unready table must stay closed")
	}
	if err := startStorageIntegrityRuntime(context.Background(), nil, supervisor, true); err == nil || !strings.Contains(err.Error(), "storage_integrity.merge_guard") {
		t.Fatalf("err = %v, want the static set to fail startup", err)
	}
}
```

In `storage_integrity_ingress_test.go`, replace:

```go
func (g *unhealthyMergeGuard) AssertStopMerges(context.Context) error {
	return g.err
}

func (g *unhealthyMergeGuard) CheckMergeHealth() error {
	return g.err
}
```

with:

```go
func (g *unhealthyMergeGuard) CheckMergeHealth(string) error {
	return g.err
}
```

In `build_test.go`, route every existing runtime construction through a static-set helper and keep the static startup fail-fast. Run the rename first, before the helper exists, so the helper's own call is not renamed:

```bash
perl -0pi -e 's/buildStorageIntegrityRuntimeConsumer\(/buildStaticRuntimeConsumer(/g; s/startStorageIntegrityRuntime\((ctx|context\.Background\(\)), (\w+), (\w+)\)/startStorageIntegrityRuntime($1, $2, $3, true)/g' build_test.go
grep -c 'buildStaticRuntimeConsumer(' build_test.go        # 22
grep -c 'startStorageIntegrityRuntime(.*, true)' build_test.go   # 7
```

In `build_test.go`, replace:

```go
func (g *recordingBuildMergeGuard) AssertStopMerges(context.Context) error {
	g.calls++
	return g.err
}
```

with:

```go
func (g *recordingBuildMergeGuard) AssertTables(_ context.Context, ids []string) (sicore.MergeGuardReport, error) {
	g.calls++
	report := sicore.MergeGuardReport{Tables: map[string]error{}}
	for _, id := range ids {
		report.Tables[id] = g.err
	}
	return report, nil
}

// buildStaticRuntimeConsumer builds the runtime over the configured table set,
// the way buildServer does for storage_integrity.tables.
func buildStaticRuntimeConsumer(runtimeCfg config.StorageIntegrityRuntimeConfig, tables []string, opts StorageIntegrityRuntimeOptions) (*StorageIntegrityIngress, *StorageIntegrityMergeSupervisor, error) {
	schemas := map[string]payloadexec.TableSchema{}
	for _, schema := range opts.TableSchemas {
		schemas[schema.TableID] = schema
	}
	static := sitable.NewStatic(tables, schemas, "")
	return buildStorageIntegrityRuntimeConsumer(runtimeCfg, static, static, opts)
}
```

In `build_test.go`, replace:

```go
func (g *orderedBuildMergeGuard) AssertStopMerges(context.Context) error {
	g.order.add("merge")
	return nil
}
```

with:

```go
func (g *orderedBuildMergeGuard) AssertTables(_ context.Context, ids []string) (sicore.MergeGuardReport, error) {
	g.order.add("merge")
	report := sicore.MergeGuardReport{Tables: map[string]error{}}
	for _, id := range ids {
		report.Tables[id] = nil
	}
	return report, nil
}
```

In `build_test.go`, replace:

```go
	supervisor, ok := guard.(*StorageIntegrityMergeSupervisor)
	if !ok {
		t.Fatalf("runtime merge guard type = %T, want *StorageIntegrityMergeSupervisor", guard)
	}
	if ingress.guard != supervisor {
```

with:

```go
	supervisor := guard
	if supervisor == nil {
		t.Fatal("runtime must return its merge supervisor")
	}
	if ingress.guard != supervisor {
```

- [ ] **Step 6: Run them to verify they fail**

Run: `go test -vet=off -count=1 .`

Expected: FAIL, build failed: `*recordingBuildMergeGuard does not implement StorageIntegrityMergeGuard (missing method AssertStopMerges)` and `too many arguments in call to startStorageIntegrityRuntime`.

- [ ] **Step 7: Supervise per table over the table state**

Replace the whole of `storage_integrity_merge_supervisor.go` with:

```go
package housegate

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/sitable"
)

var errStorageIntegrityMergeGuardNotAsserted = errors.New("merge guard not asserted")

// StorageIntegrityMergeHealth answers the merge-guard health of one logical
// table (spec 2026-09-24 §9.4); ingress admission checks only its target.
type StorageIntegrityMergeHealth interface {
	CheckMergeHealth(tableID string) error
}

// StorageIntegrityMergeSupervisor reasserts the idempotent merge guard over
// the Active tables of the current table-state snapshot, on an interval and
// whenever the table state changes, and keeps one fail-closed health latch
// per table.
type StorageIntegrityMergeSupervisor struct {
	guard    StorageIntegrityMergeGuard
	state    sitable.TableState
	interval time.Duration

	assertMu sync.Mutex
	healthMu sync.RWMutex
	health   map[string]error
	version  uint64 // snapshot version of the last pass
	asserted bool
}

func NewStorageIntegrityMergeSupervisor(guard StorageIntegrityMergeGuard, state sitable.TableState, interval time.Duration) *StorageIntegrityMergeSupervisor {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &StorageIntegrityMergeSupervisor{
		guard:    guard,
		state:    state,
		interval: interval,
		health:   map[string]error{},
	}
}

// Assert runs one pass over the current snapshot's Active tables, replaces
// every latch, and returns the joined failures of the Active tables (nil when
// all are healthy).
func (s *StorageIntegrityMergeSupervisor) Assert(ctx context.Context) error {
	if s == nil || s.guard == nil || s.state == nil {
		return errors.New("storage_integrity: merge guard and table state are required")
	}
	s.assertMu.Lock()
	defer s.assertMu.Unlock()
	snap := s.state.Current()
	active := snap.Active()
	ids := make([]string, 0, len(active))
	for _, table := range active {
		ids = append(ids, table.ID)
	}
	report, err := s.guard.AssertTables(ctx, ids)
	health := make(map[string]error, len(ids))
	var failures []error
	for _, id := range ids {
		tableErr := err
		if tableErr == nil {
			var ok bool
			if tableErr, ok = report.Tables[id]; !ok {
				tableErr = errStorageIntegrityMergeGuardNotAsserted
			}
		}
		health[id] = tableErr
		if tableErr != nil {
			failures = append(failures, fmt.Errorf("%s: %w", id, tableErr))
		}
	}
	for table, tableErr := range report.Other {
		if tableErr != nil {
			log.Warnw("storage_integrity: merge guard found an unhealthy non-Active table", "database", table.Database, "table", table.Table, "error", tableErr)
		}
	}
	s.healthMu.Lock()
	s.health = health
	s.version = snap.Version()
	s.asserted = true
	s.healthMu.Unlock()
	return errors.Join(failures...)
}

// CheckMergeHealth fails closed for a table no pass has asserted yet, such as
// a table that became Active after the last pass.
func (s *StorageIntegrityMergeSupervisor) CheckMergeHealth(tableID string) error {
	if s == nil {
		return errors.New("storage_integrity: merge supervisor is required")
	}
	s.healthMu.RLock()
	err, ok := s.health[tableID]
	s.healthMu.RUnlock()
	if !ok {
		err = errStorageIntegrityMergeGuardNotAsserted
	}
	if err != nil {
		return fmt.Errorf("storage_integrity: merge guard unhealthy for %s: %w", tableID, err)
	}
	return nil
}

// Unhealthy returns the sorted ids whose latch is closed (diagnostics).
func (s *StorageIntegrityMergeSupervisor) Unhealthy() []string {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	var out []string
	for id, err := range s.health {
		if err != nil {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// Run reasserts every interval and immediately after every table-state
// change, so a newly Active table is asserted without waiting for the tick.
// It subscribes before comparing versions, so a change that lands between a
// pass and the next subscription is never missed.
func (s *StorageIntegrityMergeSupervisor) Run(ctx context.Context) {
	if s == nil {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		changed := s.state.Changed()
		if !s.assertedVersion(s.state.Current().Version()) {
			s.reassert(ctx)
			continue
		}
		select {
		case <-ticker.C:
		case <-changed:
		case <-ctx.Done():
			return
		}
		s.reassert(ctx)
	}
}

func (s *StorageIntegrityMergeSupervisor) assertedVersion(version uint64) bool {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	return s.asserted && s.version == version
}

func (s *StorageIntegrityMergeSupervisor) reassert(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if err := s.Assert(ctx); err != nil && ctx.Err() == nil {
		log.Warnw("storage_integrity: merge guard reassert found unhealthy tables", "error", err)
	}
}

var _ StorageIntegrityMergeHealth = (*StorageIntegrityMergeSupervisor)(nil)
```

Replace the whole of `storage_integrity_runtime.go` with (the new guard interface, the `(state, static)` signature, Static-only schema checks, the table-state resolver, the per-table startup pass):

```go
package housegate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// StorageIntegrityMergeGuard keeps native merges off the hg_* tables and
// reports per logical table (spec 2026-09-24 §9.4): activeTableIDs are
// required to exist, every other present hg_* table is checked when present.
// *storageintegrity.MergeGuard satisfies it; hosts may inject their own
// implementation at the library boundary. The error return is reserved for a
// failure that applies to every table.
type StorageIntegrityMergeGuard interface {
	AssertTables(ctx context.Context, activeTableIDs []string) (sicore.MergeGuardReport, error)
}

// StorageIntegrityRuntimeOptions supplies the host-owned C1/P1e runtime ports.
// HouseGate can adapt arbiter-proto clients into its core ports and can build
// its durable local journal/spool/merge-guard helpers from config, but the
// selected-SNode SourcePreparer remains host-owned because HouseGate does not
// import arbiter-core. Production construction requires that adapter to also
// implement PreparedStatementLookup.
type StorageIntegrityRuntimeOptions struct {
	ArbiterIngressClient pb.ArbiterIngressClient
	PayloadStoreClient   pb.PayloadStoreClient

	StatementSubmitter sicore.StatementSubmitter
	SourcePreparer     sicore.SourcePreparer
	StatusQuerier      sicore.IntakeStatusQuerier
	PayloadWriter      sicore.PayloadWriter
	Journal            sicore.IntakeJournal
	PayloadSpool       *sicore.FilePayloadSpool
	MergeConn          sicore.MergeConn
	MergeGuard         StorageIntegrityMergeGuard
	// TableSchemas is the complete authoritative startup schema set. Runtime
	// construction validates its frozen physical outputs globally before any
	// listener, DDL, or Keeper-backed role can mix distinct logical tables.
	TableSchemas  []payloadexec.TableSchema
	PartsPressure StorageIntegrityPartsPressure
}

// buildStorageIntegrityRuntimeConsumer builds the runtime over the table-state
// port. state is always non-nil (the runtime requires storage_integrity.enabled);
// static is non-nil exactly when the table set is the configured
// storage_integrity.tables, and only then are the startup schema set, the
// physical-name injectivity and the config-to-schema bijection checked
// (dynamic hosts rely on the arbiter's physical_name_collision admission rule).
func buildStorageIntegrityRuntimeConsumer(runtimeCfg config.StorageIntegrityRuntimeConfig, state sitable.TableState, static *sitable.Static, opts StorageIntegrityRuntimeOptions) (*StorageIntegrityIngress, *StorageIntegrityMergeSupervisor, error) {
	expectedSource := strings.TrimSpace(runtimeCfg.ExpectedSource)

	submitter := opts.StatementSubmitter
	if submitter == nil && opts.ArbiterIngressClient != nil {
		submitter = sicore.NewArbiterStatementSubmitter(opts.ArbiterIngressClient)
	}
	statusQuerier := opts.StatusQuerier
	if statusQuerier == nil && opts.ArbiterIngressClient != nil {
		statusQuerier = sicore.NewArbiterIntakeStatusQuerier(opts.ArbiterIngressClient)
	}
	payloadWriter := opts.PayloadWriter
	if payloadWriter == nil && opts.PayloadStoreClient != nil {
		payloadWriter = sicore.NewArbiterPayloadStoreWriter(opts.PayloadStoreClient)
	}

	journal := opts.Journal
	if journal == nil && strings.TrimSpace(runtimeCfg.JournalDir) != "" {
		var err error
		journal, err = sicore.NewFileIntakeJournal(strings.TrimSpace(runtimeCfg.JournalDir))
		if err != nil {
			return nil, nil, fmt.Errorf("storage_integrity.runtime.journal: %w", err)
		}
	}

	spool := opts.PayloadSpool
	if spool == nil && strings.TrimSpace(runtimeCfg.PayloadSpoolDir) != "" {
		var err error
		spool, err = sicore.NewFilePayloadSpool(strings.TrimSpace(runtimeCfg.PayloadSpoolDir))
		if err != nil {
			return nil, nil, fmt.Errorf("storage_integrity.runtime.payload_spool: %w", err)
		}
	}
	var leaseManager sicore.PayloadLeaseManager
	if payloadWriter != nil && spool != nil {
		spoolingWriter := sicore.NewSpoolingPayloadWriterWithLeasePolicy(
			spool,
			payloadWriter,
			runtimeCfg.PayloadLease.RefreshBefore.Duration,
		)
		payloadWriter = spoolingWriter
		leaseManager = sicore.NewPayloadLeaseSupervisor(
			spoolingWriter,
			runtimeCfg.PayloadLease.RefreshInterval.Duration,
		)
	}

	rawMergeGuard := buildStorageIntegrityMergeGuard(opts)

	var errs []error
	if submitter == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.statement_submitter is required"))
	}
	if opts.SourcePreparer == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.source_preparer is required"))
	} else if _, ok := opts.SourcePreparer.(sicore.PreparedStatementLookup); !ok {
		errs = append(errs, errors.New("storage_integrity.runtime.source_preparer must implement prepared statement lookup"))
	}
	if statusQuerier == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.status_querier is required"))
	}
	if payloadWriter == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.payload_writer is required"))
	}
	if journal == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.journal or journal_dir is required"))
	}
	if spool == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.payload_spool or payload_spool_dir is required"))
	}
	if rawMergeGuard == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.merge_guard or merge_conn is required"))
	}
	if state == nil {
		errs = append(errs, errors.New("storage_integrity.runtime requires storage_integrity.enabled and a table-state source"))
	}
	if static != nil {
		if len(opts.TableSchemas) == 0 {
			errs = append(errs, errors.New("storage_integrity.runtime requires the authoritative table schema set (StorageIntegrityRuntimeOptions.TableSchemas)"))
		} else {
			if err := sicore.ValidatePhysicalTableNames(opts.TableSchemas); err != nil {
				errs = append(errs, fmt.Errorf("storage_integrity.runtime schema set: %w", err))
			}
			if err := validateStorageIntegrityRuntimeTableSchemas(static.TableIDs(), opts.TableSchemas); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if joined := errors.Join(errs...); joined != nil {
		return nil, nil, joined
	}
	mergeGuard := NewStorageIntegrityMergeSupervisor(
		rawMergeGuard,
		state,
		runtimeCfg.MergeGuard.ReassertInterval.Duration,
	)

	var orch *sicore.Orchestrator
	orchCfg := sicore.OrchestratorConfig{
		ExpectedSource:      expectedSource,
		Journal:             journal,
		PayloadLeaseManager: leaseManager,
	}
	orch = sicore.NewOrchestratorWithQuerier(submitter, opts.SourcePreparer, statusQuerier, orchCfg)
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, mergeGuard, sicore.MaterializerNative, payloadWriter)
	if err != nil {
		return nil, nil, fmt.Errorf("storage_integrity.runtime: %w", err)
	}
	ingress.leaseManager = leaseManager
	ingress.mergeRunner = mergeGuard
	schemaResolver := tableStateSchemaResolver(state)
	ingress.WithTableSchemas(schemaResolver)
	if backpressure := runtimeCfg.Backpressure; backpressure.Enabled {
		unsafeDatabase := strings.TrimSpace(backpressure.UnsafeDatabase)
		safeDatabase := strings.TrimSpace(backpressure.SafeDatabase)
		if safeDatabase == "" || safeDatabase == unsafeDatabase {
			return nil, nil, errors.New("storage_integrity.runtime.backpressure requires a non-empty safe_database distinct from unsafe_database")
		}
		pressure := opts.PartsPressure
		var pressureRunner StorageIntegrityPartsPressureLifecycle
		if pressure == nil {
			if opts.MergeConn == nil {
				return nil, nil, errors.New("storage_integrity.runtime.backpressure requires merge_conn (or set storage_integrity.runtime.backpressure.enabled: false)")
			}
			guard := sicore.NewPartsPressureGuard(opts.MergeConn, sicore.PartsPressureConfig{
				UnsafeDatabase:        unsafeDatabase,
				SafeDatabase:          safeDatabase,
				SoftPartsPerPartition: backpressure.SoftPartsPerPartition,
				HardPartsPerPartition: backpressure.HardPartsPerPartition,
				RefreshTimeout:        backpressure.RefreshTimeout.Duration,
				SnapshotTTL:           backpressure.SnapshotTTL.Duration,
			})
			supervisor := NewStorageIntegrityPartsPressureSupervisor(
				guard,
				backpressure.PollInterval.Duration,
				unsafeDatabase,
				safeDatabase,
			)
			pressureRunner = supervisor
			pressure = supervisor
		} else {
			var ok bool
			pressureRunner, ok = pressure.(StorageIntegrityPartsPressureLifecycle)
			if !ok {
				return nil, nil, errors.New("storage_integrity.runtime injected parts pressure must implement the parts pressure lifecycle (Refresh and Run)")
			}
		}
		ingress.pressureRunner = pressureRunner
		ingress.WithPartsPressure(pressure, schemaResolver)
	}
	return ingress, mergeGuard, nil
}

func validateStorageIntegrityRuntimeTableSchemas(tables []string, schemas []payloadexec.TableSchema) error {
	configured := make(map[string]struct{}, len(tables))
	for _, tableID := range tables {
		configured[tableID] = struct{}{}
	}
	resolved := make(map[string]struct{}, len(schemas))
	var errs []error
	for _, schema := range schemas {
		resolved[schema.TableID] = struct{}{}
		if _, ok := configured[schema.TableID]; !ok {
			errs = append(errs, fmt.Errorf("storage_integrity.runtime table schema %q is not listed in storage_integrity.tables", schema.TableID))
		}
	}
	for _, tableID := range tables {
		if _, ok := resolved[tableID]; !ok {
			errs = append(errs, fmt.Errorf("storage_integrity.tables entry %q has no authoritative runtime table schema", tableID))
		}
	}
	return errors.Join(errs...)
}

// tableStateSchemaResolver resolves journal-recovery and cleanup schemas from
// the current snapshot, which answers Active and Gone tables (spec 2026-09-24
// §9.2), so statements admitted before a retirement still finish. Under
// sitable.Static it answers the startup schema set, exactly as before.
func tableStateSchemaResolver(state sitable.TableState) StorageIntegrityTableSchemaResolver {
	return StorageIntegrityTableSchemaResolverFunc(func(tableID string) (payloadexec.TableSchema, bool) {
		table, ok := state.Current().Schema(tableID)
		if !ok || table.Schema.TableID == "" {
			return payloadexec.TableSchema{}, false
		}
		return table.Schema, true
	})
}

// startStorageIntegrityRuntime asserts the merge guard once before recovery.
// failOnTableError keeps the static table set's startup fail-fast; a dynamic
// host starts with the failing tables' latches closed, so one unready table
// blocks only its own admissions (spec 2026-09-24 §9.4).
func startStorageIntegrityRuntime(ctx context.Context, runtime *StorageIntegrityIngress, guard *StorageIntegrityMergeSupervisor, failOnTableError bool) error {
	if guard != nil {
		if err := guard.Assert(ctx); err != nil {
			if failOnTableError {
				return fmt.Errorf("storage_integrity.merge_guard: %w", err)
			}
			log.Warnw("storage_integrity: merge guard unhealthy at startup; affected tables refuse admission until a reassert succeeds", "error", err)
		}
	}
	if runtime == nil {
		return nil
	}
	if runtime.pressureRunner != nil {
		if err := runtime.pressureRunner.Refresh(ctx); err != nil {
			return fmt.Errorf("storage_integrity.backpressure: initial parts snapshot: %w", err)
		}
	}
	if err := runtime.RecoverPending(ctx); err != nil {
		return fmt.Errorf("storage_integrity.recovery: %w", err)
	}
	if runtime.pressureRunner != nil {
		runtime.pressureRunner.Invalidate()
		if err := runtime.pressureRunner.Refresh(ctx); err != nil {
			return fmt.Errorf("storage_integrity.backpressure: post-recovery parts snapshot: %w", err)
		}
	}
	runtime.StartBackground(ctx)
	return nil
}

func buildStorageIntegrityMergeGuard(opts StorageIntegrityRuntimeOptions) StorageIntegrityMergeGuard {
	if opts.MergeGuard != nil {
		return opts.MergeGuard
	}
	if opts.MergeConn == nil {
		return nil
	}
	return sicore.NewMergeGuard(opts.MergeConn, nil)
}
```

In `storage_integrity_ingress.go`, replace:

```go
	orch           *sicore.Orchestrator
	guard          StorageIntegrityMergeGuard
```

with:

```go
	orch           *sicore.Orchestrator
	guard          StorageIntegrityMergeHealth
```

In `storage_integrity_ingress.go`, replace:

```go
func NewStorageIntegrityIngress(orch *sicore.Orchestrator, guard StorageIntegrityMergeGuard, matKind sicore.MaterializerKind) (*StorageIntegrityIngress, error) {
```

with:

```go
func NewStorageIntegrityIngress(orch *sicore.Orchestrator, guard StorageIntegrityMergeHealth, matKind sicore.MaterializerKind) (*StorageIntegrityIngress, error) {
```

In `storage_integrity_ingress.go`, replace:

```go
func NewStorageIntegrityIngressWithPayloadWriter(orch *sicore.Orchestrator, guard StorageIntegrityMergeGuard, matKind sicore.MaterializerKind, writer sicore.PayloadWriter) (*StorageIntegrityIngress, error) {
```

with:

```go
func NewStorageIntegrityIngressWithPayloadWriter(orch *sicore.Orchestrator, guard StorageIntegrityMergeHealth, matKind sicore.MaterializerKind, writer sicore.PayloadWriter) (*StorageIntegrityIngress, error) {
```

In `storage_integrity_ingress.go`, replace:

```go
	if health, ok := i.guard.(StorageIntegrityMergeHealth); ok {
		if err := health.CheckMergeHealth(); err != nil {
			return fmt.Errorf("storage_integrity ingress: merge health: %w", err)
		}
	}
```

with:

```go
	if i.guard != nil {
		// Only the target's latch: one unready table blocks only itself.
		if err := i.guard.CheckMergeHealth(adm.TableID); err != nil {
			return fmt.Errorf("storage_integrity ingress: merge health: %w", err)
		}
	}
```

In `build.go`, replace:

```go
	var storageIntegrityMergeGuard StorageIntegrityMergeGuard
```

with:

```go
	var storageIntegrityMergeGuard *StorageIntegrityMergeSupervisor
```

In `build.go`, replace:

```go
			consumer, guard, err := buildStorageIntegrityRuntimeConsumer(cfg.StorageIntegrity.Runtime, cfg.StorageIntegrity.Tables, opts.StorageIntegrityRuntime)
```

with:

```go
			consumer, guard, err := buildStorageIntegrityRuntimeConsumer(cfg.StorageIntegrity.Runtime, siState, siStatic, opts.StorageIntegrityRuntime)
```

In `build.go`, replace:

```go
			if err := startStorageIntegrityRuntime(ctx, storageIntegrityRuntime, storageIntegrityMergeGuard); err != nil {
```

with:

```go
			if err := startStorageIntegrityRuntime(ctx, storageIntegrityRuntime, storageIntegrityMergeGuard, siStatic != nil); err != nil {
```

- [ ] **Step 8: Run the runtime tests**

Run: `bazel run //:gazelle && go test -vet=off -count=1 -race -run "MergeSupervisor|StorageIntegrityRuntime|StartStorageIntegrity|BuildStorageIntegrity" . && go test -vet=off -count=1 . && bazel test //:housegate_test //pkg/storageintegrity:storageintegrity_test`

Expected: PASS, including `TestMergeSupervisorAssertsImmediatelyOnChange` under `-race`.

- [ ] **Step 9: Document per-table health**

In `CLAUDE.md`, replace:

```markdown
`pkg/storageintegrity.MergeGuard` (its method keeps the historical name `AssertStopMerges`) verifies every guarded table pins `max_bytes_to_merge_at_max_space_in_pool = 0`, issues `SYSTEM START MERGES`, and fails closed if `system.merges` shows a merge.
```

with:

```markdown
`pkg/storageintegrity.MergeGuard` verifies that a guarded table pins `max_bytes_to_merge_at_max_space_in_pool = 0`, issues `SYSTEM START MERGES`, and fails closed if `system.merges` shows a merge. Health is per logical table (spec 2026-09-24 §9.4): `MergeGuard.AssertTables(ctx, activeTableIDs)` checks every table present in `hg_safe` / `hg_unsafe`, requires both tables of each Active id, and reports per id (a Pending or Gone table's `hg_*` is checked when present and never required); `StorageIntegrityMergeSupervisor` reasserts over the current snapshot's Active set on `reassert_interval` and on every `TableState.Changed()`, and ingress admission checks only its target's latch, `CheckMergeHealth(tableID)`. A static table set keeps its fail-fast startup; a dynamic host starts with an unready table's latch closed. An injected `StorageIntegrityRuntimeOptions.MergeGuard` implements `AssertTables`; the fixed-table `AssertStopMerges` keeps its historical name for existing callers.
```

- [ ] **Step 10: Commit**

```bash
git add pkg/storageintegrity/merge_guard_tables.go pkg/storageintegrity/merge_guard_tables_test.go pkg/storageintegrity/BUILD.bazel storage_integrity_merge_supervisor.go storage_integrity_merge_supervisor_test.go storage_integrity_runtime.go storage_integrity_runtime_dynamic_test.go storage_integrity_ingress.go storage_integrity_ingress_test.go build.go build_test.go BUILD.bazel CLAUDE.md
git commit -m "$(cat <<'EOF'
feat(storage-integrity): per-table merge-guard health over the table state

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Admission and recovery schemas: the snapshot at admission, the table state in recovery, the Purged error, `SCHEMA_NOT_ALLOWED`

**Files:**

- Modify: `storage_integrity_ingress.go:312-323` (recovery), `:434-446` (`partsPressureTarget`), `:528` (admission), `:645-649` (`SCHEMA_NOT_ALLOWED`), new error variables
- Modify: `pkg/storageintegrity/intake.go:384-388` (`SubmitOutcome.AdmissionCode`), `pkg/storageintegrity/arbiter_proto.go:365-392`, `CLAUDE.md` (`pkg/storageintegrity` bullet)
- Test: create `storage_integrity_table_state_runtime_test.go`, `pkg/storageintegrity/admission_code_test.go`; modify `storage_integrity_backpressure_ingress_test.go:809`

**Interfaces:**

- Consumes: `siplugin.Admission.TableSchema` (Task 6), `tableStateSchemaResolver` (Task 7), `chproto.CodeQueryIsProhibited` (Task 5).
- Produces (package `storageintegrity`, core):

```go
type SubmitOutcome struct {
	Category      OutcomeCategory
	Reason        string
	AdmissionCode string `json:",omitempty"` // arbiter code name on a terminal reject
}
var AdmissionCodeSchemaNotAllowed = pb.AdmissionCode_ADMISSION_CODE_SCHEMA_NOT_ALLOWED.String()
```

- Produces (package `housegate`):

```go
var ErrStorageIntegrityRecoverySchemaPurged error // "storage_integrity recovery: table schema purged"
func (i *StorageIntegrityIngress) partsPressureTarget(rec sicore.AdmissionRecord, snapshotSchema *payloadexec.TableSchema) (string, []string, error)
```

Recovery audit (Plan decision R6): the only schema reads on the recovery path are in `restorePressureReservations`; a terminal record uses its journaled touched set (unchanged), a non-terminal record now uses its journaled set when present and resolves a schema only when the set is nil (a pre-journal-v1 record), failing with `ErrStorageIntegrityRecoverySchemaPurged` when the table state no longer holds it.

- [ ] **Step 1: Write the failing tests**

Create `storage_integrity_table_state_runtime_test.go`:

```go
package housegate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// leavePendingRecord journals one non-terminal, prepared statement for
// net1.events: the source wrote, and the arbiter answered retryable.
func leavePendingRecord(t *testing.T, touched []string) sicore.IntakeJournal {
	t.Helper()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	owner := AdmissionRecordFromPlugin(bpEUAdmission())
	owner.TouchedPartitionIDs = touched
	first := sicore.NewOrchestrator(
		&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeRetryable, Reason: "arbiter busy"}},
		&rootRecordingPreparer{
			source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
			candidates: []sicore.CandidatePart{{TableID: owner.TableID, PartitionID: "p_eu", PartName: "eu_part_1"}},
		},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	if res, _ := first.Orchestrate(context.Background(), owner); res.IsTerminal() {
		t.Fatalf("fixture: the record must stay non-terminal, got %+v", res)
	}
	return journal
}

func recoverWith(t *testing.T, journal sicore.IntakeJournal, state sitable.TableState) (*fakePartsPressure, error) {
	t.Helper()
	pressure := &fakePartsPressure{}
	restarted := sicore.NewOrchestrator(
		&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeRetryable, Reason: "arbiter busy"}},
		&rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatal(err)
	}
	ingress.WithPartsPressure(pressure, tableStateSchemaResolver(state))
	// Recovery keeps retrying the retryable submit after the restore hook ran;
	// the bound ends that loop once the hook's outcome is observable.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return pressure, ingress.RecoverPending(ctx)
}

func goneState() sitable.TableState {
	return sitable.NewFake(sitable.Pending, sitable.Table{ID: "net1.events", Status: sitable.Gone, Schema: bpSchemas()[0]})
}

// purgedState reports the name as unrecorded (default deny: Pending), which
// is how a host reports a Purged table (spec 2026-09-24 §5).
func purgedState() sitable.TableState { return sitable.NewFake(sitable.Pending) }

func TestRecoveryUsesJournaledPartitionsWithoutASchema(t *testing.T) {
	journal := leavePendingRecord(t, []string{"p_eu"})
	pressure, err := recoverWith(t, journal, purgedState())
	if errors.Is(err, ErrStorageIntegrityRecoverySchemaPurged) {
		t.Fatalf("a record with journaled partitions must not need a schema: %v", err)
	}
	pressure.mu.Lock()
	defer pressure.mu.Unlock()
	if pressure.restored != 1 {
		t.Fatalf("restored = %d, want the record restored from its journaled partitions", pressure.restored)
	}
}

func TestRecoveryOfAGoneTableResolvesItsSchema(t *testing.T) {
	journal := leavePendingRecord(t, nil)
	pressure, err := recoverWith(t, journal, goneState())
	if errors.Is(err, ErrStorageIntegrityRecoverySchemaPurged) {
		t.Fatalf("a Gone table's schema is still in the table state: %v", err)
	}
	pressure.mu.Lock()
	defer pressure.mu.Unlock()
	if pressure.restored != 1 {
		t.Fatalf("restored = %d, want 1", pressure.restored)
	}
}

func TestRecoveryNeedingAPurgedSchemaFailsWithTheNamedError(t *testing.T) {
	journal := leavePendingRecord(t, nil)
	_, err := recoverWith(t, journal, purgedState())
	if !errors.Is(err, ErrStorageIntegrityRecoverySchemaPurged) {
		t.Fatalf("err = %v, want ErrStorageIntegrityRecoverySchemaPurged", err)
	}
}

// TestSchemaNotAllowedReachesTheClientAsNoLongerAcceptsWrites is spec
// 2026-09-24 §9.6.
func TestSchemaNotAllowedReachesTheClientAsNoLongerAcceptsWrites(t *testing.T) {
	ingress, _, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "table retired", AdmissionCode: sicore.AdmissionCodeSchemaNotAllowed}
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var ce *chproto.ClientError
	if !errors.As(err, &ce) || ce.Code != chproto.CodeQueryIsProhibited || ce.Message != "storage_integrity: table net1.events no longer accepts writes" {
		t.Fatalf("err = %v, want the non-retryable no-longer-accepts-writes refusal", err)
	}
}

func TestAdmissionUsesTheSnapshotSchemaNotTheResolver(t *testing.T) {
	ingress, _, _, _ := newBackpressureIngress(t, &fakePartsPressure{})
	ingress.schemas = StorageIntegrityTableSchemaResolverFunc(func(string) (payloadexec.TableSchema, bool) {
		t.Fatal("a new admission must not consult the recovery resolver")
		return payloadexec.TableSchema{}, false
	})
	adm := bpAdmission()
	schema := bpSchemas()[0]
	adm.TableSchema = &schema
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm); err != nil {
		t.Fatalf("Consume: %v", err)
	}
}
```

Create `pkg/storageintegrity/admission_code_test.go`:

```go
package storageintegrity

import (
	"encoding/json"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

func TestSubmitOutcomeCarriesTheArbiterRejectCode(t *testing.T) {
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_SCHEMA_NOT_ALLOWED, Message: "table retired"})
	if got.Category != OutcomeTerminalReject || got.AdmissionCode != AdmissionCodeSchemaNotAllowed || got.Reason != "table retired" {
		t.Fatalf("outcome = %+v", got)
	}
	if accepted := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_ACCEPTED, StatementSeq: 1}); accepted.AdmissionCode != "" {
		t.Fatalf("an accepted outcome carries no reject code, got %q", accepted.AdmissionCode)
	}
}

// TestSubmitOutcomeJournalShapeIsUnchangedWithoutACode pins that records
// written before AdmissionCode existed, and records without one, keep their
// exact JSON bytes in the intake journal.
func TestSubmitOutcomeJournalShapeIsUnchangedWithoutACode(t *testing.T) {
	b, err := json.Marshal(SubmitOutcome{Category: OutcomeAccepted, Reason: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"Category":1,"Reason":"ok"}` {
		t.Fatalf("journal shape = %s", b)
	}
}
```

In `storage_integrity_backpressure_ingress_test.go`, replace:

```go
	table, partitions, err := ingress.partsPressureTarget(ownerRecord)
```

with:

```go
	table, partitions, err := ingress.partsPressureTarget(ownerRecord, nil)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -vet=off -count=1 ./pkg/storageintegrity/ .`

Expected: FAIL, build failed: `unknown field AdmissionCode in struct literal of type SubmitOutcome`, `undefined: AdmissionCodeSchemaNotAllowed`, `undefined: ErrStorageIntegrityRecoverySchemaPurged`, `too many arguments in call to ingress.partsPressureTarget`.

- [ ] **Step 3: Carry the arbiter's reject code**

In `pkg/storageintegrity/intake.go`, replace:

```go
// SubmitOutcome is the Arbiter SubmitStatement result.
type SubmitOutcome struct {
	Category OutcomeCategory
	Reason   string
}
```

with:

```go
// SubmitOutcome is the Arbiter SubmitStatement result.
type SubmitOutcome struct {
	Category OutcomeCategory
	Reason   string
	// AdmissionCode is the arbiter's application-level code name on a
	// terminal reject (for example AdmissionCodeSchemaNotAllowed), empty
	// otherwise. omitempty keeps journal records without it byte-identical.
	AdmissionCode string `json:",omitempty"`
}
```

In `pkg/storageintegrity/arbiter_proto.go`, replace:

```go
// SubmitOutcomeFromSequencedAck maps Arbiter's application-level admission
```

with:

```go
// AdmissionCodeSchemaNotAllowed is the arbiter's refusal of a statement whose
// target table is not in its admitted schema set, for example a table retired
// after the statement was admitted (spec 2026-09-24 §9.6).
var AdmissionCodeSchemaNotAllowed = pb.AdmissionCode_ADMISSION_CODE_SCHEMA_NOT_ALLOWED.String()

// SubmitOutcomeFromSequencedAck maps Arbiter's application-level admission
```

In `pkg/storageintegrity/arbiter_proto.go`, replace:

```go
		return SubmitOutcome{Category: OutcomeTerminalReject, Reason: firstNonEmpty(reason, ack.GetCode().String())}
	default:
```

with:

```go
		return SubmitOutcome{Category: OutcomeTerminalReject, Reason: firstNonEmpty(reason, ack.GetCode().String()), AdmissionCode: ack.GetCode().String()}
	default:
```

- [ ] **Step 4: Resolve admission and recovery schemas through the table state**

In `storage_integrity_ingress.go`, replace:

```go
		} else {
			table, partitions, err = i.partsPressureTarget(record.Admission)
			if err != nil {
				return fmt.Errorf("storage_integrity ingress: restore pressure target for %s: %w", record.StatementID, err)
			}
		}
```

with:

```go
		} else if record.Admission.TouchedPartitionIDs != nil {
			// The payload-derived set was journaled at admission, so recovery
			// needs no schema and no longer requires the table to be Active
			// (spec 2026-09-24 §9.2).
			table = sicore.PhysicalTableName(record.Admission.TableID)
			partitions = clonePartitionIDs(record.Admission.TouchedPartitionIDs)
		} else {
			table, partitions, err = i.partsPressureTarget(record.Admission, nil)
			if errors.Is(err, errStorageIntegritySchemaUnavailable) {
				return fmt.Errorf("%w: statement %s needs the schema of %s to rebuild its touched partitions, and the table state no longer holds it (the table was purged); resolve the statement with the operator runbook before restarting", ErrStorageIntegrityRecoverySchemaPurged, record.StatementID, record.Admission.TableID)
			}
			if err != nil {
				return fmt.Errorf("storage_integrity ingress: restore pressure target for %s: %w", record.StatementID, err)
			}
		}
```

In `storage_integrity_ingress.go`, replace:

```go
func (i *StorageIntegrityIngress) partsPressureTarget(rec sicore.AdmissionRecord) (string, []string, error) {
	if i.schemas == nil && i.pressure == nil {
		return "", nil, nil
	}
	if i.schemas == nil {
		return "", nil, fmt.Errorf("storage_integrity ingress: back-pressure requires a table schema resolver")
	}
	schema, ok := i.schemas.StorageIntegrityTableSchema(rec.TableID)
	if !ok {
		return "", nil, fmt.Errorf("storage_integrity ingress: no pinned schema for table %q", rec.TableID)
	}
```

with:

```go
// partsPressureTarget derives the physical table and the payload-touched
// partitions. A new admission passes the schema of the query's snapshot
// (spec 2026-09-24 §9.1); journal recovery passes nil and resolves through the
// table-state resolver, which answers Active and Gone tables.
func (i *StorageIntegrityIngress) partsPressureTarget(rec sicore.AdmissionRecord, snapshotSchema *payloadexec.TableSchema) (string, []string, error) {
	if i.schemas == nil && i.pressure == nil {
		return "", nil, nil
	}
	var schema payloadexec.TableSchema
	if snapshotSchema != nil {
		schema = *snapshotSchema
	} else {
		if i.schemas == nil {
			return "", nil, fmt.Errorf("storage_integrity ingress: back-pressure requires a table schema resolver")
		}
		var ok bool
		schema, ok = i.schemas.StorageIntegrityTableSchema(rec.TableID)
		if !ok {
			return "", nil, fmt.Errorf("%w: no pinned schema for table %q", errStorageIntegritySchemaUnavailable, rec.TableID)
		}
	}
```

In `storage_integrity_ingress.go`, replace:

```go
	table, partitions, err := i.partsPressureTarget(rec)
	if err != nil {
		return err
	}
```

with:

```go
	table, partitions, err := i.partsPressureTarget(rec, adm.TableSchema)
	if err != nil {
		return err
	}
```

In `storage_integrity_ingress.go`, replace:

```go
	if !res.Ack2 {
		return fmt.Errorf("storage_integrity ingress: statement %s did not reach ACK2 (lifecycle %s, reason %q)", rec.StatementID, res.Lifecycle, res.Reason)
	}
	return nil
}
```

with:

```go
	if !res.Ack2 {
		if res.Submit.AdmissionCode == sicore.AdmissionCodeSchemaNotAllowed {
			// The table retired between this statement's snapshot and the
			// arbiter's sequencing (spec 2026-09-24 §9.6). The arbiter code is
			// internal; the client sees the stable non-retryable prefix.
			return &chproto.ClientError{Code: chproto.CodeQueryIsProhibited,
				Message: fmt.Sprintf("storage_integrity: table %s no longer accepts writes", rec.TableID)}
		}
		return fmt.Errorf("storage_integrity ingress: statement %s did not reach ACK2 (lifecycle %s, reason %q)", rec.StatementID, res.Lifecycle, res.Reason)
	}
	return nil
}

// errStorageIntegritySchemaUnavailable marks a schema the resolver cannot
// answer; recovery turns it into ErrStorageIntegrityRecoverySchemaPurged.
var errStorageIntegritySchemaUnavailable = errors.New("storage_integrity ingress: table schema unavailable")

// ErrStorageIntegrityRecoverySchemaPurged is returned by startup recovery when
// a non-terminal journal record has no journaled touched-partition set and
// its table's schema is no longer in the table state because the table was
// purged (spec 2026-09-24 §9.2). Recovery fails closed; the operator resolves
// the statement before restarting.
var ErrStorageIntegrityRecoverySchemaPurged = errors.New("storage_integrity recovery: table schema purged")
```

- [ ] **Step 5: Run the tests**

Run: `bazel run //:gazelle && go test -vet=off -count=1 ./pkg/storageintegrity/ . && bazel test //:housegate_test //pkg/storageintegrity:storageintegrity_test`

Expected: PASS. The two recovery tests that leave a retryable record behind bound `RecoverPending` with a one-second context and assert on the restore hook, which runs before the retry loop.

- [ ] **Step 6: Document the recovery audit**

In `CLAUDE.md`, replace:

```markdown
Runtime pressure resolution is derived only from the same globally collision-validated authoritative `TableSchemas` set used by ingress, and v2 admissions must match its recomputed schema hash before any pressure or intake side effect.
```

with:

```markdown
Runtime pressure resolution uses the admission's snapshot schema (spec 2026-09-24 §9.1) — under `sitable.Static` the globally collision-validated startup set — and v2 admissions must match its recomputed schema hash before any pressure or intake side effect. Journal recovery resolves schemas through `TableState.Current().Schema(id)`, which answers Active and Gone tables, reuses a non-terminal record's journaled touched partitions without reading any schema, and fails closed with `ErrStorageIntegrityRecoverySchemaPurged` only for a record without journaled partitions whose table was purged. An arbiter `ADMISSION_CODE_SCHEMA_NOT_ALLOWED` (the table retired after admission; carried as `SubmitOutcome.AdmissionCode`) reaches the client as the non-retryable code-392 `storage_integrity: table <id> no longer accepts writes`.
```

- [ ] **Step 7: Commit**

```bash
git add storage_integrity_ingress.go storage_integrity_table_state_runtime_test.go storage_integrity_backpressure_ingress_test.go pkg/storageintegrity/intake.go pkg/storageintegrity/arbiter_proto.go pkg/storageintegrity/admission_code_test.go pkg/storageintegrity/BUILD.bazel BUILD.bazel CLAUDE.md
git commit -m "$(cat <<'EOF'
feat(storage-integrity): resolve admission and recovery schemas through the table state

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Agent: sign Active tables only, pass everything else through, the `sentio_getStorageIntegrityTableStatus` client

**Files:**

- Create: `pkg/registry/table_status.go`
- Modify: `pkg/network/rpc.go` (append), `pkg/plugins/sistatement/plugin.go:8,25,30-35,56,90-92,124,155-157,191-199,288-308`, `pkg/plugins/sistatement/observer.go` (append), `pkg/proxy/observer.go:68-71,88,164`, `build.go:1174-1177,1206`, `build.go:1300` (add `resolveAgentTableStatuses`)
- Modify: `CLAUDE.md` (`sistatement` entry), `README.md:210`
- Test: create `pkg/registry/table_status_test.go`, `pkg/network/rpc_table_status_test.go`, `pkg/plugins/sistatement/table_status_test.go`, `pkg/proxy/observer_table_status_test.go`, `agent_table_status_test.go`; modify `pkg/plugins/sistatement/plugin_test.go:374,418-421`, `pkg/plugins/sistatement/inline_values_test.go:334-336`

**Interfaces:**

- Produces (package `registry`) — the JSON-RPC contract of spec §10.2:

```go
const TableStatusOrdinary, TableStatusPending, TableStatusRefused, TableStatusActive, TableStatusGone = "ordinary", "pending", "refused", "active", "gone"
type TableStatus struct {
	Status          string `json:"status"`
	RefusedCode     string `json:"refused_code"`
	RefusedReason   string `json:"refused_reason"`
	SchemaJSON      string `json:"schema_json"`
	SchemaHash      string `json:"schema_hash"`
	RegistryVersion uint64 `json:"registry_version"`
}
type TableStatuses interface {
	StorageIntegrityTableStatus(ctx context.Context, database, table string) (TableStatus, error)
}
func TableStatusesFromSchemas(schemas TableSchemas) TableStatuses // declared → active, else ordinary
```

- Produces: `func (r *RpcNetworkState) StorageIntegrityTableStatus(ctx context.Context, database, table string) (registry.TableStatus, error)` — method `sentio_getStorageIntegrityTableStatus`, params `[database, table]`; a transport error, a JSON-null result or an unknown status name is an error.
- Produces (package `sistatement`): `Options.Statuses registry.TableStatuses` (`Options.Schemas` stays as the declared-schema fallback); `type StatusObserver interface{ TableStatusLookupFailed() }`.
- Produces (package `proxy`): `(*MetricsObserver).TableStatusLookupFailed()` → counter `clickhouse_proxy_agent_si_table_status_failures_total`.
- Produces (package `housegate`): `func resolveAgentTableStatuses(opts Options, reg registry.Registry) (registry.TableStatuses, error)`.

sentio-node implements the server side of the method in sub-project 4 (spec §13).

- [ ] **Step 1: Write the failing status-source tests**

Create `pkg/registry/table_status_test.go`:

```go
package registry

import (
	"context"
	"testing"
)

type mapSchemas map[string]TableSchema

func (m mapSchemas) TableSchema(db, table string, _ uint32) (TableSchema, bool) {
	s, ok := m[db+"/"+table]
	return s, ok
}

func (m mapSchemas) LatestTableSchema(db, table string) (TableSchema, bool) {
	s, ok := m[db+"/"+table]
	return s, ok
}

func TestTableStatusesFromSchemas(t *testing.T) {
	src := TableStatusesFromSchemas(mapSchemas{"shop/orders": {DatabaseId: "shop", TableId: "orders", Version: 2, SchemaHash: "0xabc", SchemaJson: `{"table_id":"shop.orders"}`}})
	got, err := src.StorageIntegrityTableStatus(context.Background(), "shop", "orders")
	if err != nil || got.Status != TableStatusActive || got.SchemaHash != "0xabc" || got.SchemaJSON != `{"table_id":"shop.orders"}` {
		t.Fatalf("declared = %+v, %v; want active with the latest declaration", got, err)
	}
	got, err = src.StorageIntegrityTableStatus(context.Background(), "shop", "other")
	if err != nil || got.Status != TableStatusOrdinary || got.SchemaJSON != "" {
		t.Fatalf("undeclared = %+v, %v; want ordinary", got, err)
	}
}
```

Create `pkg/network/rpc_table_status_test.go`:

```go
package network_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/registry"
)

// TestRpcNetworkState_TableStatusWireShape pins the spec 2026-09-24 §10.2
// JSON-RPC contract sentio-node implements: method name, positional
// (database, table) params, and the six result fields.
func TestRpcNetworkState_TableStatusWireShape(t *testing.T) {
	var gotParams []interface{}
	result := json.RawMessage(`{"status":"refused","refused_code":"column_type","refused_reason":"column b","schema_json":"","schema_hash":"","registry_version":42}`)
	rpc, fake := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getStorageIntegrityTableStatus": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			gotParams = params
			return result, nil
		},
	})
	got, err := rpc.StorageIntegrityTableStatus(context.Background(), "shop", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(fake.calls, ",") != "sentio_getStorageIntegrityTableStatus" || len(gotParams) != 2 || gotParams[0] != "shop" || gotParams[1] != "orders" {
		t.Fatalf("calls = %v params = %v", fake.calls, gotParams)
	}
	want := registry.TableStatus{Status: "refused", RefusedCode: "column_type", RefusedReason: "column b", RegistryVersion: 42}
	if got != want {
		t.Fatalf("status = %+v, want %+v", got, want)
	}
}

func TestRpcNetworkState_TableStatusFailures(t *testing.T) {
	for name, method := range map[string]rpcMethod{
		"rpc error": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return nil, &rpcErrEnvelope{Code: -32000, Message: "down"}
		},
		"null result":    func([]interface{}) (interface{}, *rpcErrEnvelope) { return nil, nil },
		"unknown status": func([]interface{}) (interface{}, *rpcErrEnvelope) { return map[string]any{"status": "retiring"}, nil },
	} {
		rpc, _ := newFakeRpc(t, map[string]rpcMethod{"sentio_getStorageIntegrityTableStatus": method})
		if _, err := rpc.StorageIntegrityTableStatus(context.Background(), "shop", "orders"); err == nil {
			t.Fatalf("%s: want an error", name)
		}
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -count=1 ./pkg/registry/ ./pkg/network/`

Expected: FAIL, build failed: `undefined: TableStatusesFromSchemas`, `rpc.StorageIntegrityTableStatus undefined`.

- [ ] **Step 3: Add the status contract and the RPC client**

Create `pkg/registry/table_status.go`:

```go
package registry

import "context"

// Storage-integrity table status names of the JSON-RPC contract (spec
// 2026-09-24 §10.2). They equal sitable.Status.String().
const (
	TableStatusOrdinary = "ordinary"
	TableStatusPending  = "pending"
	TableStatusRefused  = "refused"
	TableStatusActive   = "active"
	TableStatusGone     = "gone"
)

// TableStatus is one answer of sentio_getStorageIntegrityTableStatus, with the
// semantics of sitable.Snapshot.Lookup on the serving node. SchemaJSON and
// SchemaHash are set for active (and gone) tables.
type TableStatus struct {
	Status          string `json:"status"`
	RefusedCode     string `json:"refused_code"`
	RefusedReason   string `json:"refused_reason"`
	SchemaJSON      string `json:"schema_json"`
	SchemaHash      string `json:"schema_hash"`
	RegistryVersion uint64 `json:"registry_version"`
}

// TableStatuses is the agent's per-INSERT status source. Like TableSchemas it
// stays out of Registry so routing-only implementations need not stub it.
type TableStatuses interface {
	StorageIntegrityTableStatus(ctx context.Context, database, table string) (TableStatus, error)
}

// TableStatusesFromSchemas adapts a declared-schema source (the YAML
// table_schemas fixture, or a host-injected TableSchemas) into a status
// source: a declared table is active with its latest declaration, and every
// other table is ordinary.
func TableStatusesFromSchemas(schemas TableSchemas) TableStatuses {
	return declaredSchemaStatuses{schemas: schemas}
}

type declaredSchemaStatuses struct{ schemas TableSchemas }

func (d declaredSchemaStatuses) StorageIntegrityTableStatus(_ context.Context, database, table string) (TableStatus, error) {
	latest, ok := d.schemas.LatestTableSchema(database, table)
	if !ok {
		return TableStatus{Status: TableStatusOrdinary}, nil
	}
	return TableStatus{Status: TableStatusActive, SchemaJSON: latest.SchemaJson, SchemaHash: latest.SchemaHash}, nil
}
```

In `pkg/network/rpc.go`, append the method after `IsOperator` — replace:

```go
func (r *RpcNetworkState) IsOperator(owner, signer string) bool {
	return owner != "" && owner == signer
}
```

with:

```go
func (r *RpcNetworkState) IsOperator(owner, signer string) bool {
	return owner != "" && owner == signer
}

// --- registry.TableStatuses

// StorageIntegrityTableStatus calls sentio_getStorageIntegrityTableStatus
// (spec 2026-09-24 §10.2). A transport error, a JSON-null result, or an
// unknown status name is an error: the agent then passes the INSERT through
// unsigned and counts the failure.
func (r *RpcNetworkState) StorageIntegrityTableStatus(ctx context.Context, database, table string) (registry.TableStatus, error) {
	var status registry.TableStatus
	ok, err := r.call(ctx, "sentio_getStorageIntegrityTableStatus", []interface{}{database, table}, &status)
	if err != nil {
		return registry.TableStatus{}, fmt.Errorf("rpc: getStorageIntegrityTableStatus %s.%s: %w", database, table, err)
	}
	if !ok {
		return registry.TableStatus{}, fmt.Errorf("rpc: getStorageIntegrityTableStatus %s.%s returned null", database, table)
	}
	switch status.Status {
	case registry.TableStatusOrdinary, registry.TableStatusPending, registry.TableStatusRefused,
		registry.TableStatusActive, registry.TableStatusGone:
		return status, nil
	default:
		return registry.TableStatus{}, fmt.Errorf("rpc: getStorageIntegrityTableStatus %s.%s: unknown status %q", database, table, status.Status)
	}
}

var _ registry.TableStatuses = (*RpcNetworkState)(nil)

// Compile-time check the rpc backend satisfies registry.Registry.
var _ registry.Registry = (*RpcNetworkState)(nil)

// Compile-time check the rpc backend satisfies registry.Registry.
var _ registry.Registry = (*RpcNetworkState)(nil)
```

- [ ] **Step 4: Run the status-source tests**

Run: `go test -count=1 ./pkg/registry/ ./pkg/network/`

Expected: PASS.

- [ ] **Step 5: Write the failing agent tests**

Create `pkg/plugins/sistatement/table_status_test.go`:

```go
package sistatement

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// scriptedStatuses answers every lookup with one status (or error) and
// records the (database, table) pairs asked.
type scriptedStatuses struct {
	mu     sync.Mutex
	status registry.TableStatus
	err    error
	asked  []string
}

func (s *scriptedStatuses) StorageIntegrityTableStatus(_ context.Context, database, table string) (registry.TableStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked = append(s.asked, database+"."+table)
	return s.status, s.err
}

func activeStatus(t *testing.T, schema payloadexec.TableSchema, hash string) registry.TableStatus {
	t.Helper()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	return registry.TableStatus{Status: registry.TableStatusActive, SchemaJSON: string(js), SchemaHash: hash, RegistryVersion: 9}
}

type statusMetrics struct {
	inlineMetrics
	failed int
}

func (m *statusMetrics) TableStatusLookupFailed() { m.failed++ }

func newStatusPlugin(t *testing.T, statuses registry.TableStatuses, inline bool, ev ValuesEvaluator) (*Plugin, *SeqCounter, *statusMetrics) {
	t.Helper()
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := OpenSeqCounter(t.TempDir(), signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	metrics := &statusMetrics{}
	opts := Options{Signer: signer, Statuses: statuses, NetworkID: testNetworkID, Seq: seq, MaxPayloadBytes: 1 << 20, Observer: metrics}
	if inline {
		opts.Evaluator = ev
		opts.InlineValues = InlineValuesOptions{Enabled: true, EvaluationTimeout: 5 * time.Second, MaxRows: 1000}
	}
	p, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p, seq, metrics
}

// TestPlugin_SignsActiveTablesOnly is spec 2026-09-24 §10.1: Active signs,
// every other status passes through unchanged and unsigned.
func TestPlugin_SignsActiveTablesOnly(t *testing.T) {
	for _, status := range []string{registry.TableStatusOrdinary, registry.TableStatusPending, registry.TableStatusRefused, registry.TableStatusGone} {
		statuses := &scriptedStatuses{status: registry.TableStatus{Status: status, RefusedCode: "column_type"}}
		p, seq, _ := newStatusPlugin(t, statuses, false, nil)
		q := insertQctx(newSession(1, ""), "INSERT INTO shop.orders FORMAT Native")
		q.Query.Compression = proto.CompressionEnabled // a pass-through is not subject to the lane's rules
		if err := p.OnQuery(context.Background(), q); err != nil {
			t.Fatalf("%s: err = %v, want pass-through", status, err)
		}
		if q.DeferredInsert != nil || q.Query.ID != "client-uuid-1" || seq.Last() != 0 {
			t.Fatalf("%s: the statement was claimed (deferred=%v id=%q seq=%d)", status, q.DeferredInsert, q.Query.ID, seq.Last())
		}
		if strings.Join(statuses.asked, ",") != "shop.orders" {
			t.Fatalf("%s: asked %v", status, statuses.asked)
		}
	}
	statuses := &scriptedStatuses{status: activeStatus(t, testSchema(), payloadexec.TableSchemaHash(testNetworkID, testSchema()))}
	p, seq, _ := newStatusPlugin(t, statuses, false, nil)
	q := insertQctx(newSession(2, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if q.DeferredInsert == nil || seq.Last() != 1 {
		t.Fatalf("an Active table must be claimed for signing (deferred=%v seq=%d)", q.DeferredInsert, seq.Last())
	}
}

func TestPlugin_StatusFailurePassesThroughAndCounts(t *testing.T) {
	statuses := &scriptedStatuses{err: errors.New("rpc: connection refused")}
	p, seq, metrics := newStatusPlugin(t, statuses, false, nil)
	q := insertQctx(newSession(3, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("a status failure must pass through, got %v", err)
	}
	if q.DeferredInsert != nil || seq.Last() != 0 || metrics.failed != 1 {
		t.Fatalf("deferred=%v seq=%d failures=%d, want unsigned pass-through counted once", q.DeferredInsert, seq.Last(), metrics.failed)
	}
}

func TestPlugin_HashMismatchRefusesUnsigned(t *testing.T) {
	statuses := &scriptedStatuses{status: activeStatus(t, testSchema(), "0xdeadbeef")}
	p, seq, _ := newStatusPlugin(t, statuses, false, nil)
	q := insertQctx(newSession(4, ""), "INSERT INTO shop.orders FORMAT Native")
	err := p.OnQuery(context.Background(), q)
	if err == nil || !strings.Contains(err.Error(), "does not match the recomputed") {
		t.Fatalf("err = %v, want a hash-mismatch refusal", err)
	}
	if q.DeferredInsert != nil || seq.Last() != 0 {
		t.Fatal("a refused statement must not be claimed")
	}
}

func TestPlugin_ActiveSchemaForAnotherTableIsRefused(t *testing.T) {
	other := testSchema()
	other.TableID = "shop.other"
	statuses := &scriptedStatuses{status: activeStatus(t, other, payloadexec.TableSchemaHash(testNetworkID, other))}
	p, _, _ := newStatusPlugin(t, statuses, false, nil)
	err := p.OnQuery(context.Background(), insertQctx(newSession(5, ""), "INSERT INTO shop.orders FORMAT Native"))
	if err == nil || !strings.Contains(err.Error(), `carries a schema for "shop.other"`) {
		t.Fatalf("err = %v, want an identity refusal", err)
	}
}

// TestPlugin_InlineValuesEvaluatesOnlyActiveTables pins that the inline lane
// neither evaluates nor refuses a statement into a non-Active table.
func TestPlugin_InlineValuesEvaluatesOnlyActiveTables(t *testing.T) {
	ev := &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}}
	statuses := &scriptedStatuses{status: registry.TableStatus{Status: registry.TableStatusPending}}
	p, seq, _ := newStatusPlugin(t, statuses, true, ev)
	q := inlineQctx(newSession(6, ""), "INSERT INTO shop.orders VALUES (rand(), 'eu', 1.5)")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("a Pending inline INSERT must pass through, even one the closure gate would refuse: %v", err)
	}
	if ev.calls != 0 || q.SynthesizedInsert != nil || seq.Last() != 0 {
		t.Fatalf("evaluations=%d synthesized=%v seq=%d, want none", ev.calls, q.SynthesizedInsert, seq.Last())
	}
}
```

In `pkg/plugins/sistatement/plugin_test.go`, an undeclared (Ordinary) table now passes through instead of failing; delete the row:

```go
		{"schema missing", func(q *plugin.QueryContext) { q.Query.Body = "INSERT INTO shop.unknown FORMAT Native" }, "not declared"},

```

with:

```go

```

In `pkg/plugins/sistatement/plugin_test.go`, replace:

```go
	rejectedInsert := insertQctx(sess, "INSERT INTO orders FORMAT Native")
	if err := p.OnQuery(context.Background(), rejectedInsert); err == nil || !strings.Contains(err.Error(), "other.orders") {
		t.Fatalf("rejected USE must preserve the prior database: %v", err)
	}
```

with:

```go
	// The rejected USE leaves the signing database at "other", so the
	// unqualified INSERT resolves to the undeclared (Ordinary) other.orders
	// and passes through unsigned.
	rejectedInsert := insertQctx(sess, "INSERT INTO orders FORMAT Native")
	if err := p.OnQuery(context.Background(), rejectedInsert); err != nil || rejectedInsert.DeferredInsert != nil {
		t.Fatalf("rejected USE must preserve the prior database: err=%v deferred=%v", err, rejectedInsert.DeferredInsert)
	}
```

In `pkg/plugins/sistatement/inline_values_test.go`, delete the refusal row for an undeclared table (covered as a pass-through by `TestPlugin_InlineValuesEvaluatesOnlyActiveTables`):

```go
		{"schema missing", func(_ *Plugin, q *plugin.QueryContext, _ *fakeEvaluator) {
			q.Query.Body = "INSERT INTO shop.missing VALUES (1)"
		}, false, false},

```

with:

```go

```

Create `pkg/proxy/observer_table_status_test.go`:

```go
package proxy

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/plugins/sistatement"
)

func TestMetricsObserver_TableStatusLookupFailures(t *testing.T) {
	var obs sistatement.StatusObserver = NewMetricsObserver()
	before := testutil.ToFloat64(agentSITableStatusFailuresTotal)
	obs.TableStatusLookupFailed()
	want := fmt.Sprintf(`
# HELP clickhouse_proxy_agent_si_table_status_failures_total Agent-mode storage-integrity table status lookups that failed; the INSERT passed through unsigned
# TYPE clickhouse_proxy_agent_si_table_status_failures_total counter
clickhouse_proxy_agent_si_table_status_failures_total %g
`, before+1)
	if err := testutil.CollectAndCompare(agentSITableStatusFailuresTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}
```

Create `agent_table_status_test.go`:

```go
package housegate

import (
	"context"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

func TestResolveAgentTableStatuses(t *testing.T) {
	rpc, err := network.NewRpcNetworkState("http://127.0.0.1:1", network.RpcOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := resolveAgentTableStatuses(Options{}, rpc); err != nil || got != registry.TableStatuses(rpc) {
		t.Fatalf("an RPC network state must answer statuses itself, got %T %v", got, err)
	}

	yaml := network.NewInMemoryNetworkState()
	yaml.TableSchemas["shop/orders@1"] = network.TableSchemaInfo{DatabaseId: "shop", TableId: "orders", Version: 1, SchemaHash: "0x1", SchemaJson: "{}"}
	got, err := resolveAgentTableStatuses(Options{}, yaml)
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := got.StorageIntegrityTableStatus(context.Background(), "shop", "orders"); st.Status != registry.TableStatusActive {
		t.Fatalf("a declared YAML table must be active, got %+v", st)
	}
	if st, _ := got.StorageIntegrityTableStatus(context.Background(), "shop", "other"); st.Status != registry.TableStatusOrdinary {
		t.Fatalf("an undeclared YAML table must be ordinary, got %+v", st)
	}

	if _, err := resolveAgentTableStatuses(Options{StorageIntegrityTableSchemas: yaml}, registryOnly{network.NewInMemoryNetworkState()}); err != nil {
		t.Fatalf("host-injected schemas must satisfy the agent: %v", err)
	}
	if _, err := resolveAgentTableStatuses(Options{}, registryOnly{network.NewInMemoryNetworkState()}); err == nil || !strings.Contains(err.Error(), "requires a table status source") {
		t.Fatalf("err = %v, want the missing-source refusal", err)
	}
}
```

- [ ] **Step 6: Run them to verify they fail**

Run: `go test -vet=off -count=1 ./pkg/plugins/sistatement/ ./pkg/proxy/ .`

Expected: FAIL, build failed: `unknown field Statuses in struct literal of type Options`, `undefined: sistatement.StatusObserver`, `undefined: resolveAgentTableStatuses`.

- [ ] **Step 7: Ask for the status first**

In `pkg/plugins/sistatement/plugin.go`, replace:

```go
	"encoding/hex"
```

with:

```go
	"encoding/hex"
	"encoding/json"
```

In `pkg/plugins/sistatement/plugin.go`, replace:

```go
	"github.com/housegate/housegate/pkg/schemaregistry"

```

with:

```go

```

In `pkg/plugins/sistatement/plugin.go`, replace:

```go
type Options struct {
	Signer          auth.StatementSignerV2
	Schemas         registry.TableSchemas
```

with:

```go
type Options struct {
	Signer auth.StatementSignerV2
	// Statuses answers each INSERT target's storage-integrity status (spec
	// 2026-09-24 §10.2): RpcNetworkState in production. When nil, Schemas is
	// adapted: a declared table is Active, every other table Ordinary.
	Statuses        registry.TableStatuses
	Schemas         registry.TableSchemas
```

In `pkg/plugins/sistatement/plugin.go`, replace:

```go
	loader        *schemaregistry.NetworkStateLoader
```

with:

```go
	statuses      registry.TableStatuses
```

In `pkg/plugins/sistatement/plugin.go`, replace:

```go
	if opts.Schemas == nil {
		errs = append(errs, errors.New("network-state TableSchemas source is required"))
	}
```

with:

```go
	statuses := opts.Statuses
	if statuses == nil && opts.Schemas != nil {
		statuses = registry.TableStatusesFromSchemas(opts.Schemas)
	}
	if statuses == nil {
		errs = append(errs, errors.New("a table status source (Statuses or Schemas) is required"))
	}
```

In `pkg/plugins/sistatement/plugin.go`, replace:

```go
		loader:        schemaregistry.NewNetworkStateLoader(opts.Schemas, opts.NetworkID),
```

with:

```go
		statuses:      statuses,
```

In `pkg/plugins/sistatement/plugin.go`, replace:

```go
	// Spec D1/D6: InsertPayloadEncoding refuses the 26.x inline VALUES shape
```

with:

```go
	// Spec 2026-09-24 §10.1: the target's status comes first. Only an Active
	// table is signed; every other INSERT passes through unchanged and the
	// server decides. A target this parser cannot resolve keeps today's
	// classification below.
	target, targetErr := sicore.ResolveInsertTarget(sql, p.sessionDatabase(qctx.Session))
	var (
		schema     payloadexec.TableSchema
		schemaHash string
	)
	if targetErr == nil {
		var active bool
		schema, schemaHash, active, err = p.activeTarget(ctx, target)
		if err != nil {
			return err
		}
		if !active {
			return nil
		}
	}
	// Spec D1/D6: InsertPayloadEncoding refuses the 26.x inline VALUES shape
```

In `pkg/plugins/sistatement/plugin.go`, replace:

```go
	target, err := sicore.ResolveInsertTarget(sql, p.sessionDatabase(qctx.Session))
	if err != nil {
		return fmt.Errorf("storage_integrity agent: %w", err)
	}
	tableID := target.CanonicalID()
	schema, schemaHash, err := p.loadSchema(ctx, target)
	if err != nil {
		return err
	}
```

with:

```go
	if targetErr != nil {
		return fmt.Errorf("storage_integrity agent: %w", targetErr)
	}
	tableID := target.CanonicalID()
```

In `pkg/plugins/sistatement/plugin.go`, replace:

```go
func (p *Plugin) loadSchema(ctx context.Context, target sicore.InsertTarget) (payloadexec.TableSchema, string, error) {
	tableID := target.CanonicalID()
	if tableID == "" || target.Database == "" || target.Table == "" {
		return payloadexec.TableSchema{}, "", fmt.Errorf("storage_integrity agent: invalid structured table target %#v", target)
	}
	schemas, err := p.loader.Load(ctx, []schemaregistry.TableRef{
		{
			TableID:         tableID,
			Database:        target.Database,
			Table:           target.Table,
			LogicalDatabase: target.Database,
			LogicalTable:    target.Table,
		},
	})
	if err != nil {
		return payloadexec.TableSchema{}, "", fmt.Errorf("storage_integrity agent: table %s is not declared in network state (SI INSERT requires a declared, hash-verified schema): %w", tableID, err)
	}
	schema := schemas[0]
	return schema, payloadexec.TableSchemaHash(p.networkID, schema), nil
}
```

with:

```go
// activeTarget asks the status source about the INSERT target. It returns
// active=false, and no error, for every status but Active and when the status
// lookup itself fails: the INSERT then passes through unsigned, which is safe
// because the server rejects an unsigned INSERT into an Active table (spec
// 2026-09-24 H7). For an Active table it decodes the registry schema and
// refuses the INSERT when the recomputed hash differs from the declared one.
func (p *Plugin) activeTarget(ctx context.Context, target sicore.InsertTarget) (payloadexec.TableSchema, string, bool, error) {
	tableID := target.CanonicalID()
	if tableID == "" || target.Database == "" || target.Table == "" {
		return payloadexec.TableSchema{}, "", false, nil
	}
	_, logger := log.FromContext(ctx)
	status, err := p.statuses.StorageIntegrityTableStatus(ctx, target.Database, target.Table)
	if err != nil {
		p.observeStatus(func(o StatusObserver) { o.TableStatusLookupFailed() })
		logger.Warnw("sistatement: table status unavailable; passing the INSERT through unsigned", "table_id", tableID, "error", err)
		return payloadexec.TableSchema{}, "", false, nil
	}
	if status.Status != registry.TableStatusActive {
		logger.Debugw("sistatement: target is not active; passing the INSERT through unsigned", "table_id", tableID, "status", status.Status)
		return payloadexec.TableSchema{}, "", false, nil
	}
	var schema payloadexec.TableSchema
	if err := json.Unmarshal([]byte(status.SchemaJSON), &schema); err != nil {
		return payloadexec.TableSchema{}, "", false, fmt.Errorf("storage_integrity agent: active table %s has an undecodable schema_json: %w", tableID, err)
	}
	if schema.TableID != tableID {
		return payloadexec.TableSchema{}, "", false, fmt.Errorf("storage_integrity agent: active table %s carries a schema for %q", tableID, schema.TableID)
	}
	hash := payloadexec.TableSchemaHash(p.networkID, schema)
	if hash != status.SchemaHash {
		return payloadexec.TableSchema{}, "", false, fmt.Errorf("storage_integrity agent: active table %s schema_hash %s does not match the recomputed %s for network %s; refusing to sign", tableID, status.SchemaHash, hash, p.networkID)
	}
	return schema, hash, true, nil
}

func (p *Plugin) observeStatus(fn func(StatusObserver)) {
	if o, ok := p.observer.(StatusObserver); ok && o != nil {
		fn(o)
	}
}
```

In `pkg/plugins/sistatement/observer.go`, replace:

```go
	InlineValuesClosureRefused()
}
```

with:

```go
	InlineValuesClosureRefused()
}

// StatusObserver counts status lookups that failed, after which the INSERT
// passed through unsigned (spec 2026-09-24 §10.3). *proxy.MetricsObserver
// satisfies it; an Observer that does not is simply not counted.
type StatusObserver interface {
	TableStatusLookupFailed()
}
```

In `pkg/proxy/observer.go`, replace:

```go
	agentInlineValuesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_agent_inline_values_total",
		Help: "Agent-mode signed inline INSERT ... VALUES outcomes",
	}, []string{"result"})
)
```

with:

```go
	agentInlineValuesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_agent_inline_values_total",
		Help: "Agent-mode signed inline INSERT ... VALUES outcomes",
	}, []string{"result"})
	agentSITableStatusFailuresTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_proxy_agent_si_table_status_failures_total",
		Help: "Agent-mode storage-integrity table status lookups that failed; the INSERT passed through unsigned",
	})
)
```

In `pkg/proxy/observer.go`, replace:

```go
	prometheus.MustRegister(agentInlineValuesTotal)
```

with:

```go
	prometheus.MustRegister(agentInlineValuesTotal)
	prometheus.MustRegister(agentSITableStatusFailuresTotal)
```

In `pkg/proxy/observer.go`, replace:

```go
func (m *MetricsObserver) InlineValuesSynthesized() {
```

with:

```go
func (m *MetricsObserver) TableStatusLookupFailed() { agentSITableStatusFailuresTotal.Inc() }

func (m *MetricsObserver) InlineValuesSynthesized() {
```

In `build.go`, replace:

```go
		schemas, err := resolveTableSchemas(opts, reg, "storage_integrity.agent")
		if err != nil {
			return nil, err
		}
```

with:

```go
		statuses, err := resolveAgentTableStatuses(opts, reg)
		if err != nil {
			return nil, err
		}
```

In `build.go`, replace:

```go
			Signer:          stmtSigner,
			Schemas:         schemas,
```

with:

```go
			Signer:          stmtSigner,
			Statuses:        statuses,
```

In `build.go`, replace:

```go
// buildAgentDialer returns the per-session upstream dialer for agent
```

with:

```go
// resolveAgentTableStatuses selects the agent's per-INSERT table status source
// (spec 2026-09-24 §10.2): a host-injected declared-schema source, then a
// registry that answers sentio_getStorageIntegrityTableStatus (RpcNetworkState),
// then a registry with declared schemas (the YAML table_schemas fixture). A
// declared-schema source reports its declared tables Active and every other
// table Ordinary.
func resolveAgentTableStatuses(opts Options, reg registry.Registry) (registry.TableStatuses, error) {
	if opts.StorageIntegrityTableSchemas != nil {
		return registry.TableStatusesFromSchemas(opts.StorageIntegrityTableSchemas), nil
	}
	if statuses, ok := reg.(registry.TableStatuses); ok && statuses != nil {
		return statuses, nil
	}
	if schemas, ok := reg.(registry.TableSchemas); ok && schemas != nil {
		return registry.TableStatusesFromSchemas(schemas), nil
	}
	return nil, fmt.Errorf("storage_integrity.agent requires a table status source: an RPC network state (sentio_getStorageIntegrityTableStatus), a YAML table_schemas fixture, or Options.StorageIntegrityTableSchemas")
}

// buildAgentDialer returns the per-session upstream dialer for agent
```

- [ ] **Step 8: Run the agent tests**

Run: `bazel run //:gazelle && go test -vet=off -count=1 ./pkg/registry/ ./pkg/network/ ./pkg/plugins/sistatement/ ./pkg/proxy/ . && bazel test //pkg/plugins/sistatement:sistatement_test //pkg/network:network_test //pkg/proxy:proxy_test //:housegate_test`

Expected: PASS.

- [ ] **Step 9: Document the status-first agent**

In `CLAUDE.md`, replace:

```markdown
resolves the declared network-state schema, sets `QueryContext.DeferredInsert`
```

with:

```markdown
first asks a `registry.TableStatuses` source for the target's status — `RpcNetworkState` calls `sentio_getStorageIntegrityTableStatus(database, table)` → `{status, refused_code, refused_reason, schema_json, schema_hash, registry_version}` with the semantics of `sitable.Snapshot.Lookup` on the serving node, and a YAML or host `TableSchemas` source reports its declared tables Active and every other table Ordinary (`resolveAgentTableStatuses`) — and claims only an Active table, recomputing `TableSchemaHash` with `storage_integrity.agent.network_id` and refusing a mismatch; every other status, and a failed lookup (counted in `clickhouse_proxy_agent_si_table_status_failures_total`), passes the INSERT through unchanged and unsigned so the server decides (spec 2026-09-24 H7), and the inline VALUES lane evaluates only Active targets; for an Active table it sets `QueryContext.DeferredInsert`
```

In `README.md`, replace:

```markdown
provide the NetworkState schema source required by the SI agent (unless the embedding host injects it)
```

with:

```markdown
provide the table status source the SI agent needs — an RPC `network_state.source` that serves `sentio_getStorageIntegrityTableStatus`, or a YAML `table_schemas` fixture whose declared tables count as Active (unless the embedding host injects one); the agent signs only Active tables and passes every other INSERT through unsigned
```

- [ ] **Step 10: Commit**

```bash
git add pkg/registry/ pkg/network/ pkg/plugins/sistatement/ pkg/proxy/ build.go agent_table_status_test.go BUILD.bazel CLAUDE.md README.md
git commit -m "$(cat <<'EOF'
feat(sistatement): sign only Active tables and read their status over JSON-RPC

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Docker-bound lifecycle test and the remaining documentation

**Files:**

- Create: `pkg/integration/storage_integrity_table_state_test.go` (in the existing `//pkg/integration:integration_test` target, which `.github/workflows/ci.yml:124` already lists)
- Modify: `CLAUDE.md` (Key Modules: `pkg/sitable`; CI section), `pkg/integration/BUILD.bazel` (gazelle)

**Interfaces:**

- Consumes: every earlier task; `testenv.StartServerProxy`, `testenv.StartAgentProxy`, `testenv.RunCLI`, `testenv.RunCLIStdin`, `testenv.RunCLIMultiqueryIgnoreError`, the package's `capturingConsumer`, `authProxyConfig`, `openConnNoDB`, `authTestKey1`.
- Produces: `TestStorageIntegrityTableStateLifecycle` — a `sitable.Fake` drives `tsdb.t` through Pending → Active → DROP → Gone → Purged → CREATE against ClickHouse 25.8, the native rewriter `$REWRITER_GO_TAG`, the agent's signed lane and `clickhouse client`; `statusRegistry` answers the agent's status calls from the same fake, as sentio-node will. The test creates `hg_unsafe.tsdb__t` / `hg_safe.tsdb__t` while the table is Pending (the data plane ensures them when `AddTable` commits) and asserts that a direct `INSERT INTO hg_unsafe.tsdb__t` — the Active set being empty — is refused with code 403 and `storage-integrity physical table hg_unsafe.tsdb__t is not directly addressable` and lands no row: the engine protects `hg_unsafe` only through `reserved_databases`.

- [ ] **Step 1: Write the lifecycle test**

It skips without `POLYGLOT_SQL_FFI_PATH`, like the other native-engine integration tests. It also measures Plan decision R3: `SELECT connectionId()` before and after a 733 and a 392 (and later a 60) in one `--multiquery --ignore-error` run must return the same id, so clickhouse-client kept its connection.

Create `pkg/integration/storage_integrity_table_state_test.go`:

```go
package integration

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
)

// statusRegistry is the agent's view of the same fake table state the server
// reads: it answers sentio_getStorageIntegrityTableStatus from the snapshot,
// the way sentio-node will (spec 2026-09-24 §10.2, §13).
type statusRegistry struct {
	*network.InMemoryNetworkState
	state sitable.TableState
}

func (r statusRegistry) StorageIntegrityTableStatus(_ context.Context, database, table string) (registry.TableStatus, error) {
	snap := r.state.Current()
	t := snap.Lookup(database, table)
	out := registry.TableStatus{Status: t.Status.String(), RefusedCode: t.RefusedCode, RefusedReason: t.RefusedReason, RegistryVersion: snap.Version()}
	if t.Status == sitable.Active || t.Status == sitable.Gone {
		js, err := json.Marshal(t.Schema)
		if err != nil {
			return registry.TableStatus{}, err
		}
		out.SchemaJSON, out.SchemaHash = string(js), t.SchemaHash
	}
	return out, nil
}

var connectionIDLine = regexp.MustCompile(`(?m)^[0-9]+$`)

// requireSameConnection runs `SELECT connectionId()` around the refused
// statements and requires both answers to match: clickhouse-client kept its
// connection across every refusal (plan ruling R3).
func requireSameConnection(t *testing.T, out string) {
	t.Helper()
	ids := connectionIDLine.FindAllString(out, -1)
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("connectionId() before/after = %v, want one unchanged connection\nout: %s", ids, out)
	}
}

// TestStorageIntegrityTableStateLifecycle is spec 2026-09-24 §11.3: a fake
// TableState drives one table through Pending, Active, Gone and Purged with
// real ClickHouse, the native rewriter, the signed agent lane and the CLI.
func TestStorageIntegrityTableStateLifecycle(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; fetch the contract-V2 library with `go run ./cmd fetch-rewriter-lib --tag` at the tag .github/workflows/ci.yml fetches, and pass --test_env")
	}
	bin := testenv.ClickHouseCLI(t)
	ctx := context.Background()
	const (
		phys      = "phys_ts"
		networkID = "itest-net-table-state"
	)
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{
		"DROP DATABASE IF EXISTS " + phys,
		"CREATE DATABASE " + phys,
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"CREATE DATABASE IF NOT EXISTS hg_unsafe",
		"DROP TABLE IF EXISTS hg_safe.tsdb__t",
		"DROP TABLE IF EXISTS hg_unsafe.tsdb__t",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_safe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_unsafe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys)
	})

	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	schema := payloadexec.TableSchema{TableID: "tsdb.t", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}}}
	activeT := sitable.Table{ID: "tsdb.t", Status: sitable.Active, Schema: schema, SchemaHash: payloadexec.TableSchemaHash(networkID, schema)}
	state := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "tsdb.t", Status: sitable.Pending})
	consumer := &capturingConsumer{}

	server := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("tsdb"),
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), "tsdb", registry.DbAuthOwner),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			enabled := true
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.StorageIntegrity.Enabled = &enabled
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.NetworkID = networkID
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
		}),
		func(_ *config.Config, opts *housegate.Options) {
			opts.StorageIntegrityTableState = state
			opts.StorageIntegrityAdmissionConsumer = consumer
		},
	)
	agentProxy := testenv.StartAgentProxy(t, authTestKey1, server.Addr,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
		}),
		func(_ *config.Config, opts *housegate.Options) {
			opts.NetworkState = statusRegistry{InMemoryNetworkState: opts.NetworkState.(*network.InMemoryNetworkState), state: state}
		},
	)
	run := func(query string) (string, error) {
		t.Helper()
		return testenv.RunCLI(t, bin, agentProxy.Addr, "", query)
	}
	mustRun := func(query string) string {
		t.Helper()
		out, err := run(query)
		if err != nil {
			t.Fatalf("%q: %v\nout: %s", query, err, out)
		}
		return out
	}
	mustRefuse := func(query, code, prefix string) {
		t.Helper()
		out, err := run(query)
		if err == nil || !strings.Contains(out, "Code: "+code+".") || !strings.Contains(out, prefix) {
			t.Fatalf("%q: err=%v, want code %s and %q\nout: %s", query, err, code, prefix, out)
		}
	}
	const create = "CREATE TABLE tsdb.t (id UInt64, region String) ENGINE = MergeTree ORDER BY id"

	// 1. CREATE, then Pending: data reads and writes are retryable, metadata is
	// allowed. The test creates hg_* in place of the data plane, which ensures
	// them when AddTable commits, so they exist while the table is Pending.
	mustRun(create)
	for _, q := range []string{
		"CREATE TABLE hg_unsafe.tsdb__t (_hg_row_id FixedString(32), id UInt64, region String) ENGINE = MergeTree ORDER BY id SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0",
		"CREATE TABLE hg_safe.tsdb__t AS hg_unsafe.tsdb__t",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	mustRefuse("SELECT count() FROM tsdb.t", "733", "storage_integrity: table tsdb.t is pending activation (retryable)")
	out, err := testenv.RunCLIStdin(t, bin, agentProxy.Addr, "", "INSERT INTO tsdb.t FORMAT CSV", "1,eu\n")
	if err == nil || !strings.Contains(out, "storage_integrity: table tsdb.t is pending activation (retryable)") {
		t.Fatalf("a Pending INSERT must pass the agent unsigned and be refused retryably: err=%v\nout: %s", err, out)
	}
	// No table is Active, so the engine knows the protected databases only
	// from reserved_databases: a direct write into hg_unsafe is refused by the
	// rewriter (a RejectedError, code 403) and lands no row.
	out, err = testenv.RunCLIStdin(t, bin, agentProxy.Addr, "", "INSERT INTO hg_unsafe.tsdb__t FORMAT CSV", "0123456789abcdef0123456789abcdef,1,eu\n")
	if err == nil || !strings.Contains(out, "Code: 403.") || !strings.Contains(out, "storage-integrity physical table hg_unsafe.tsdb__t is not directly addressable") {
		t.Fatalf("a direct hg_unsafe INSERT must be refused while the Active set is empty: err=%v\nout: %s", err, out)
	}
	var unsafeRows uint64
	if err := seed.QueryRow(ctx, "SELECT count() FROM hg_unsafe.tsdb__t").Scan(&unsafeRows); err != nil || unsafeRows != 0 {
		t.Fatalf("hg_unsafe.tsdb__t rows = %d err=%v, want the refused INSERT to land nothing", unsafeRows, err)
	}
	if got := mustRun("EXISTS TABLE tsdb.t"); got != "1" {
		t.Fatalf("EXISTS of a Pending table must read the ordinary table, got %q", got)
	}
	// Refusals end only the query: clickhouse-client keeps its connection
	// across a 733 and a 392 (plan ruling R3).
	out, _ = testenv.RunCLIMultiqueryIgnoreError(t, bin, agentProxy.Addr, "",
		"SELECT connectionId(); SELECT count() FROM tsdb.t; ALTER TABLE tsdb.t ADD COLUMN x UInt8; SELECT connectionId()")
	if !strings.Contains(out, "Code: 733.") || !strings.Contains(out, "Code: 392.") {
		t.Fatalf("multiquery must show both refusals\nout: %s", out)
	}
	requireSameConnection(t, out)

	// 2. The table becomes Active and a signed INSERT is admitted over the
	// registry schema.
	state.Set(activeT)
	if out, err := testenv.RunCLIStdin(t, bin, agentProxy.Addr, "", "INSERT INTO tsdb.t FORMAT CSV", "1,eu\n"); err != nil {
		t.Fatalf("signed INSERT into the Active table: %v\nout: %s", err, out)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		consumer.mu.Lock()
		n := len(consumer.seen)
		consumer.mu.Unlock()
		if n == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	consumer.mu.Lock()
	if len(consumer.seen) != 1 || consumer.seen[0].SchemaHash != activeT.SchemaHash || consumer.seen[0].TableID != "tsdb.t" {
		consumer.mu.Unlock()
		t.Fatalf("admissions = %+v, want one over the registry schema", consumer.seen)
	}
	consumer.mu.Unlock()
	if got := mustRun("SELECT count() FROM tsdb.t"); got != "0" {
		t.Fatalf("an Active read is served from hg_safe, which holds no rows yet; got %q", got)
	}

	// 3. DROP succeeds under contract V2 and drops only the ordinary table;
	// once the host retires the table it is Gone: unknown to reads, and a
	// same-name CREATE is retryable.
	mustRun("DROP TABLE tsdb.t")
	var ordinary uint64
	if err := seed.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = '"+phys+"' AND name = 'tsdb.t'").Scan(&ordinary); err != nil || ordinary != 0 {
		t.Fatalf("the ordinary physical table must be gone: n=%d err=%v", ordinary, err)
	}
	var protocol uint64
	if err := seed.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database IN ('hg_safe', 'hg_unsafe') AND name = 'tsdb__t'").Scan(&protocol); err != nil || protocol != 2 {
		t.Fatalf("hg_* must stay for the data plane to purge: n=%d err=%v", protocol, err)
	}
	gone := activeT
	gone.Status = sitable.Gone
	state.Set(gone)
	mustRefuse("SELECT count() FROM tsdb.t", "60", "Table tsdb.t does not exist")
	mustRefuse(create, "733", "storage_integrity: table tsdb.t is still being purged; retry CREATE later (retryable)")
	out, _ = testenv.RunCLIMultiqueryIgnoreError(t, bin, agentProxy.Addr, "",
		"SELECT connectionId(); SELECT count() FROM tsdb.t; SELECT connectionId()")
	requireSameConnection(t, out)

	// 4. Purged: the name is unrecorded again (default deny: Pending), and
	// the same-name CREATE succeeds.
	state.Set(sitable.Table{ID: "tsdb.t", Status: sitable.Pending})
	mustRun(create)
}
```

- [ ] **Step 2: Check that it builds**

Run: `bazel run //:gazelle && go vet ./pkg/integration/ 2>&1 | grep -v "unkeyed fields"`

Expected: no output.

- [ ] **Step 3: Run it against Docker, the CLI and the V2 native engine**

Run:

```bash
export POLYGLOT_SQL_FFI_PATH="$(go run ./cmd fetch-rewriter-lib --tag "$REWRITER_GO_TAG" | tail -n 1)"
mkdir -p tests/bin && { [ -x tests/bin/clickhouse ] || { curl -sSL https://clickhouse.com/install.sh | sh && mv clickhouse tests/bin/; }; }
go test -vet=off -count=1 -timeout 600s -run TestStorageIntegrityTableStateLifecycle -v ./pkg/integration/
```

Expected: `--- PASS: TestStorageIntegrityTableStateLifecycle` (not SKIP).

- [ ] **Step 4: Run the whole docker-bound suite unchanged**

Run:

```bash
bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test //pkg/integration/sipressurescale:sipressurescale_test \
  --test_env=POLYGLOT_SQL_FFI_PATH --test_output=errors
```

Expected: all three targets PASS. The existing static-config tests (`storage_integrity_read_test.go`, the agent, formats and inline-VALUES tests) are unchanged, which is spec §11.3's proof that `sitable.Static` preserves today's behaviour. CI needs no new list entry: the test lives in `//pkg/integration:integration_test`, and Task 1 moved the CI FFI fetch to `$REWRITER_GO_TAG`.

- [ ] **Step 5: Document the port**

In `CLAUDE.md`, replace:

```markdown
- **[pkg/chsession/](pkg/chsession/)** — `Session` interface + `SessionState`
```

with:

```markdown
- **[pkg/sitable/](pkg/sitable/)** — the storage-integrity table-state port (spec 2026-09-24 §5): `Status` (`Ordinary` = Legacy or ungoverned, `Pending` incl. default deny, `Refused`, `Active`, `Gone` = Retiring/Purging), immutable versioned `Snapshot` (`Lookup` answers any table, `Active()` sorted, `Schema(id)` for Active and Gone), `TableState{Current, Changed}`, `sitable.Static` (the configured `storage_integrity.tables`, all Active, version 1, `Changed` never fires) and the switchable test `sitable.Fake`. Every membership judgement (indexer, default deny, activation window, incarnation choice, local `hg_*` readiness before Active) belongs to the host (sentio-node, sub-project 4); HouseGate only acts on the returned status. A Purged name is reported unrecorded, i.e. Pending on the SI indexer.
- **[pkg/chsession/](pkg/chsession/)** — `Session` interface + `SessionState`
```

In `CLAUDE.md`, replace:

```markdown
Integration targets are tagged `manual`, so a plain `bazel test //...` skips them (docker-less environments would otherwise fail); CI lists them explicitly.
```

with:

```markdown
Integration targets are tagged `manual`, so a plain `bazel test //...` skips them (docker-less environments would otherwise fail); CI lists them explicitly. The table-state lifecycle test (`pkg/integration/storage_integrity_table_state_test.go`) lives in `//pkg/integration:integration_test` and needs the contract-V2 native library through `POLYGLOT_SQL_FFI_PATH`, which the CI job fetches.
```

- [ ] **Step 6: Commit**

```bash
git add pkg/integration/storage_integrity_table_state_test.go pkg/integration/BUILD.bazel CLAUDE.md
git commit -m "$(cat <<'EOF'
test(integration): drive one table through the storage-integrity lifecycle

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Spec coverage

| Spec section | Where |
|---|---|
| §1 Goal, §4 H1 (one snapshot per query) | Task 4 (snapshot taken, carried, used by the rewriter and scrubber), Task 5 `TestOneSnapshotPerQuery`, Tasks 6 and 8 (ingress and runtime read it) |
| §4 H2 (host owns membership), H3 (activation window) | Task 2 (`sitable` only reports statuses), handoff notes |
| §4 H4 (same-name CREATE refused while Gone), H5 (metadata allowed) | Task 5 matrix |
| §4 H6 (contract by version) | Task 1 (V2 sent and required), Task 4 (arguments always sent when enabled) |
| §4 H7 (agent signs Active only) | Task 9 |
| §5 port and snapshot model, `Options.StorageIntegrityTableState` | Task 2, Task 4 |
| §6.1 the switch | Task 3 (config rules), Task 4 (`resolveStorageIntegrityTableState`: both / neither / injected-while-disabled) |
| §6.2 every site | fail-closed, echo, `RejectUndecodableQuery`: Task 4; arguments with an empty map: Task 4; session `SET`: Tasks 1 and 4; capability check and probe: Tasks 1 and 4; `sireserved`: Task 4; scrubber per version: Task 4; `internal_listen` and read-state warnings: Task 4; factory table state: Task 4; `unsafe_latest` parts: Task 4 |
| §7.1 placement and bypass | Task 5 (`TestBypassRules`, `TestBuildServer_TableStateGateWiring`) |
| §7.2 matrix, precedence | Task 5 (`TestDecisionMatrix`, `TestRefusalPrecedence`) |
| §7.3 rules 1–3 | Task 5 (Gone as code 60, data-carrying creation, ALTER/RENAME) |
| §7.4 error contract and codes | Task 5 (codes, R3), Task 6 (stale view), Task 8 (no longer accepts writes), Task 10 (connection kept) |
| §8 rewriter contract V2 (housegate side) | Task 1 |
| §9.1 ingress schema | Task 6 |
| §9.2 runtime schema resolution, recovery audit, Purged error | Task 7 (table-state resolver), Task 8 (audit R6, `ErrStorageIntegrityRecoverySchemaPurged`) |
| §9.3 bijection only for Static | Task 7 |
| §9.4 per-table MergeGuard, `Changed()` wake, interface change | Task 7 |
| §9.5 parts pressure and read state | Task 4 (`unsafe_latest`), Task 8 (`partsPressureTarget` from the snapshot) |
| §9.6 `SCHEMA_NOT_ALLOWED` | Task 8 |
| §10.1–10.3 agent, JSON-RPC, failures and skew | Task 9, Task 6 (stale-view message) |
| §11.2 unit tests | every task's Step 1 |
| §11.3 integration lifecycle, static suite unchanged | Task 10 |
| §12 rollout, §13 host contract, §14 risks | Task 1 (V1-only rewriter refuses startup), handoff notes |

## Verification record

How the code in this plan was verified before it was written down (2026-09-24, macOS arm64, Go 1.27.1, Bazel 9.1.0, Docker via OrbStack, ClickHouse server 25.8.28 in testcontainers, ClickHouse client 26.8.1.368, rewriter-go FFI v0.11.0):

- **Task by task.** The ten tasks were applied in order to a throwaway worktree of `872c91b` by a script that performs exactly the edits shown above (every `replace` asserted its exact occurrence count), ran each "verify they fail" and "run" step, and committed. Every failure step failed with the build errors named in its "Expected" line and every run step passed; `git status` was clean after each commit, so the `git add` lists are complete. Task 1's pin step was replaced by a local `replace` of rewriter-proto with `STORAGE_INTEGRITY_CONTRACT_V2 = 2` and a `ReservedDatabases []string` field (Go field and getter only) added (R1). The reserved-databases revision (R15) was re-verified the same way in a fresh worktree: all ten tasks again passed step by step.
- **Final state.** `go build ./...`, `go vet ./...` (only the pre-existing `intake_test.go:1802` finding), `gofmt -l` (clean), `go test -vet=off` over every non-integration package, and `bazel run //:gazelle && bazel test //...` (63/63 targets) all pass.
- **Docker-bound suite.** `go test -vet=off ./pkg/integration/` passes on the final state (every test PASS, one pre-existing environment skip `TestStoragePromotionMVP_hardlinkUnsafePartIntoPromoteTable`), including `TestStorageIntegrityTableStateLifecycle`, with two emulations for the V1-only local engine: V2 acknowledgement was emulated by sending the V1 value with an extra always-Active anchor table (a V1 engine acknowledges only a non-empty map), the probe ran its five V1 cases, `DROP TABLE tsdb.t` was replaced by a direct drop of the ordinary table after asserting the V1 rejection, Under that emulation the anchor's map entry is what makes the V1 engine protect `hg_unsafe`, so the step-1 assertion that a direct `INSERT INTO hg_unsafe.tsdb__t` is refused (code 403, `storage-integrity physical table hg_unsafe.tsdb__t is not directly addressable`, measured) and lands no row passed, but it exercised the map-derived protection rather than `reserved_databases`. Not executed locally: the three V2 probe cases against a real engine, `DROP TABLE` of an Active table through V2, and `reserved_databases` protecting `hg_*` under a genuinely empty map (the argument is covered by unit tests). Task 1 Step 7 and Task 10 Steps 3–4 run them against the real `$REWRITER_GO_TAG` engine.
- **Measurements quoted in the rulings.** The rewriter classifications in R5, R12 and R14 were measured by calling rewriter-go v0.11.0 native directly with and without an SI table map; the connection retention in R3 by the lifecycle test's `requireSameConnection`.

## Handoff notes for sub-project 4 (sentio-node)

- **Inject the port and switch it on.** Set `storage_integrity.enabled: true`, leave `storage_integrity.tables` empty, and pass `Options.StorageIntegrityTableState`. Until the registry is enabled, present today's static set, for example with `sitable.NewStatic(ids, schemas, networkID)` (spec §13). A snapshot must be immutable and its methods must do no I/O; `Changed()` must return a channel closed at the next version change after the call.
- **Status judgement stays in the host** (spec §13): govern only databases whose indexer is the registry's `si_indexer_id`; Ordinary for Legacy, other indexers and, before the seed, tables created before `activation_block`; Pending for unrecorded names (default deny, including a Purged name); Active only when the registry says Active **and** this node's `hg_*` tables exist; Gone while the newest incarnation is Retiring or Purging, with its schema (recovery needs it); `Table.ID` is `<database>.<table>`; `SchemaHash` is the registry's `schema_hash`, which must equal `payloadexec.TableSchemaHash(network_id, schema)`.
- **Serve `sentio_getStorageIntegrityTableStatus(database, table)`** with the Global Constraints shape and `Snapshot.Lookup` semantics; the agent calls it once per INSERT and passes through on any error.
- **Reserved databases:** HouseGate sends `hg_safe`, `hg_unsafe` and `hg_promote` as `reserved_databases`, so no host code needs to hide them; the host must not create ordinary tables in those databases.
- **Adapt the injected MergeGuard** to `AssertTables(ctx, activeTableIDs) (sicore.MergeGuardReport, error)`; `sicore.NewMergeGuard(conn, nil)` already implements it.
- **Dependencies:** bump housegate to the release carrying this plan, the V2 native library (`$REWRITER_GO_TAG`) and — first — the production gRPC rewriter to `$REWRITER_GRPC_TAG` (spec §8.4 release order); a V1 rewriter refuses HouseGate startup.
- **Operators:** `ErrStorageIntegrityRecoverySchemaPurged` at startup means a pre-journal-v1 non-terminal record targets a purged table; resolve it with the arbiter's statement status before restarting.
