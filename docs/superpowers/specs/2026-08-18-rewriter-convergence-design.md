# Rewriter Convergence: Materializer Profile, Admission Guards, INSERT … SELECT, Doc Truth

**Date:** 2026-08-18 **Status:** Proposed **Roadmap:** [v1 closure roadmap](2026-08-18-storage-integrity-v1-closure-roadmap.md) Spec E. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §7 (Phase-1), §14 P0 admission bans, §15 Q8/Q11; [2026-07-01 agent materialize](2026-07-01-agent-materialize-nondeterminism-design.md); [2026-06-11 rewriter-go integration](2026-06-11-rewriter-go-integration-design.md). **Code base:** rewriter-proto `v0.1.0`, rewriter-go `v0.6.0` (`internal/engine/materialize*.go`, `internal/handlers/writes.go`, `internal/nameresolve/resolve.go`), rewriter-grpc `1656832` (`src/handlers/materialize.cc`, `writes.cc`, `name_rewrite.cc`), housegate `c6f7a6d`. **Source of truth:** English version.

## 1. Problems (each is a finding of the 2026-08-18 review)

1. **Two materializers, one `profile_id`, different behaviour.** `sentio-p1-nondet-v1` accepts 16 functions in Go (`materialize_nodes.go:131-154`) and 20 in C++ (`materialize.cc:158-224` adds `nowInBlock`, `nowInBlock64`, `randConstant`, `generateUUIDv7`); Go also rewrites the *bare* keywords `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `LOCALTIME` / `LOCALTIMESTAMP` (polyglot parses them as pseudo-functions) while C++ leaves them (ClickHouse parses them as identifiers). Same input, two `rewritten_sql`s, two `sql_hash`es, one profile id → the profile does not pin behaviour, contradicting §14 P0's "executor profile governance".
2. **`DEFAULT now()` is materialized instead of banned.** The AST walk is statement-agnostic; `CREATE TABLE t (ts DateTime DEFAULT now())` through `MaterializeSQL` becomes a frozen constant default. §14 P0 wants schema-level non-determinism rejected at admission.
3. **`INSERT … SELECT` is accepted, its source table neither rewritten nor reported** (`writes_cases.json` `insert_select_source_untouched`; C++ identical). Multi-tenant correctness bug (the logical source name reaches the physical DB) and it makes §15 Q11's "reject at admission" impossible for HouseGate, which reads only `statement_type` / `original_accessed_tables`.
4. **Doc drift on physical naming.** Both engines hard-code `phys.\`logical.table\`` (dotted), but `rewriter-proto/proto/rewriter.proto:148-178`, `rewriter-grpc/README.md`, and `housegate/CLAUDE.md:168` still describe the underscore join; housegate's CLAUDE.md also names a non-existent field `RewriteTableDynamicArgs.physical_database` (real: `upstream_physical_database_in_context`) and claims `DESCRIBE` is rewritten (neither engine handles it).
5. **`Optimize` RPC** is contract-declared and C++-implemented, absent from rewriter-go.

## 2. Decisions

**D1 — One frozen materializer set per profile id; C++ extensions get their own profile.** `sentio-p1-nondet-v1` := the 16-function Go set, exactly. C++ keeps its four extras under a new `sentio-p1-nondet-v1-cpp-ext` profile that is **not** allowlisted by housegate's `materialize` plugin (agents must not sign under it); C++ under `sentio-p1-nondet-v1` rejects those four with `MaterializeUnsupportedStatement` like Go. Rejected: adding the four to the shared set — `nowInBlock`/`randConstant` have per-block semantics ClickHouse evaluates once per block, so a per-statement constant changes semantics for multi-block INSERTs; keep them out of the signed profile.

**D2 — Bare keyword forms are rejected under the shared profile.** `CURRENT_TIMESTAMP` etc. without parentheses: Go currently rewrites (via polyglot pseudo-function), C++ leaves as identifier (and ClickHouse would fail with `UNKNOWN_IDENTIFIER`). Under `sentio-p1-nondet-v1` both engines return `MaterializeUnsupportedStatement` for the bare form with a message telling the user to write `now()`. Rejected: making C++ rewrite them — it would materialize something ClickHouse itself does not accept, i.e. change the statement's validity.

**D3 — Statement-kind guard in `MaterializeSQL`.** The materializer only walks `SELECT`-family, `INSERT` (including VALUES), `ALTER … UPDATE/DELETE` predicates and value expressions. For `CREATE TABLE` / `ALTER … ADD|MODIFY COLUMN` / `CREATE MATERIALIZED VIEW` etc., a non-deterministic function anywhere in a column default/materialized/TTL expression returns `MaterializeUnsupportedStatement` (`schema-level non-determinism is not materializable`); anywhere else in DDL it is left untouched. Both engines; goldens shared.

**D4 — `INSERT … SELECT`: rewrite the source and report it; classify it.** Both engines rewrite the SELECT side with the ordinary SELECT handler (fixing the multi-tenant bug), report every source table in `original_accessed_tables`, and set a new `RewriteSQLResponse.insert_class` enum: `INSERT_CLASS_UNSPECIFIED | PAYLOAD_LOCAL | SELECT | INPUT_FUNCTION`. HouseGate's SI ingress rejects anything but `PAYLOAD_LOCAL` from the response (today it pattern-matches SQL text — keep that as belt-and-braces). Rejected: keeping the text match only — housegate's rule is that it does not parse SQL.

**D5 — `Optimize` in rewriter-go: implement to parity** (JOIN left/right swap on already-rewritten SQL) so `rewriter.engine: native` is a full replacement; until then housegate logs a warning at startup if `engine: native` and any caller wires `Optimize` (none today). Rejected: dropping the RPC — the C++ service and its callers exist.

**D6 — `DESCRIBE`**: both engines rewrite `DESCRIBE [TABLE] db.t` through the same name resolution as `EXISTS TABLE` (dynamic + static maps), classify it as `STATEMENT_TYPE_DESCRIBE` (new enum value), and Spec G's SI branch turns it into the metadata SELECT. Today it passes through unrewritten and reaches ClickHouse with the logical name.

## 3. Contract changes (rewriter-proto, minor bump)

- `RewriteSQLResponse.insert_class` (enum above).
- `StatementType` gains `STATEMENT_TYPE_DESCRIBE`.
- `MaterializationPolicy.profile_id` doc: enumerate `sentio-p1-nondet-v1` (shared, frozen list inline in the proto comment) and `sentio-p1-nondet-v1-cpp-ext`.
- Fix `proto/rewriter.proto:148-178` naming text and examples to the dotted form; add the note that the trailing separator is a literal `.`.
- Spec G's `StorageIntegrityArgs` rides the same bump if sequenced together.

## 4. Engine work

rewriter-go: `materialize_nodes.go` (drop bare-keyword pseudo-function path under the shared profile; profile switch), `materialize.go` (statement-kind guard D3), `handlers/writes.go` (`dispatchInsert` → rewrite SELECT source via `RewriteSelect`, populate accessed tables + `insert_class`), `handlers/dblevel.go` or new `describe.go` (D6), new `handlers/optimize.go` (D5, parity with `join_swap.cc`), goldens: `materialize_cases.json` (shared file, includes profile column), `writes_cases.json` (`insert_select_source_rewritten` replaces `_untouched`), `describe_cases.json`. rewriter-grpc: `materialize.cc` (profile switch; D3 guard on `ASTCreateQuery`/`ASTAlterCommand` column defaults; reject bare keywords — no-op today, add the test), `writes.cc` (`INSERT … SELECT` source rewrite via the SELECT handler; `insert_class`), `describe` handler, README fix, `CLAUDE.md` profile note. The differential oracle harness (`rewriter-go/internal/harness/oracle.go`, `REWRITER_ORACLE_ADDR`) must pass on the shared goldens after both land — make it a required CI job in rewriter-go against a rewriter-grpc container.

housegate: `pkg/plugins/materialize` allowlists profile ids (`sentio-p1-nondet-v1` only, config `materialize.profile_id`); SI ingress reads `insert_class`; `pkg/rewriter/sentio.go` passes `DESCRIBE` through the rewrite path (it already does for unknown types — verify); CLAUDE.md fixes (Spec B lists them).

## 5. Testing / acceptance

Shared golden files consumed by both engines with a profile column; oracle parity job green; housegate `pkg/integration` cross-engine test asserts identical `sql_hash` for the same materialized INSERT under both engines (this is the property D1/D2 restore); `INSERT INTO db.t SELECT * FROM db.s` under a static map rewrites **both** tables and reports both; `CREATE TABLE … DEFAULT now()` → unsupported in both; `DESCRIBE db.t` → physical name in both.

## 6. Delivery

1. rewriter-proto bump (this spec + Spec G together) → tag.
2. rewriter-go: D1/D2/D3 materializer + goldens; D4 INSERT…SELECT; D6 DESCRIBE; D5 Optimize (last, independent).
3. rewriter-grpc: mirror + oracle job.
4. housegate: profile allowlist, `insert_class` consumption, doc fixes.
