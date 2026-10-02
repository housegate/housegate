# Rewriter rejections and `rewriter.fail_open_on_unavailable`

HouseGate sends every ordinary tenant query through the SQL rewriter (the external `sql-rewriter` gRPC service or the in-process native engine) before it reaches ClickHouse. This note describes what happens when the rewriter does not return a rewritten statement, and the one switch operators can turn.

## Rejections always fail closed

HouseGate refuses the query when the rewriter:

- answers with anything other than `Success` (`UnsupportedStatement`, `SyntaxError`, `InvalidRewriteRequest`, `RewriteError`, or a code a newer engine adds);
- answers `Success` without a statement;
- fails after it received the statement: a gRPC status error such as `UNKNOWN` or `INTERNAL`, a crash that drops the connection mid-call, a deadline that expires while the engine works on the statement, or an internal error of the native engine;
- cannot be sent the statement for a reason that depends on the statement, such as a request that fails to serialise.

A statement that is not valid UTF-8 (raw bytes such as `0xFF` in a literal or comment; escape sequences like `'\xFF'` are plain ASCII and unaffected) is refused before the rewriter is called, with `statement is not valid UTF-8`, for both engines. The gRPC contract carries the statement in a proto3 string, which cannot hold such bytes.

The client receives a ClickHouse Exception and nothing is sent to ClickHouse. For an engine answer the message carries the engine's reason; for a failure it is generic (`rewriter failed to process the statement`, `rewriter unavailable`), and the details, including the rewriter address, go to the server log only. This holds with and without `storage_integrity.enabled`, and no configuration restores a pass-through.

Before this change, deployments with storage integrity disabled forwarded the original SQL verbatim on such an answer. That neutralised every engine refusal: a statement the engine could not parse or did not model (for example one containing a vertical tab, or a `#` glued to a keyword) reached ClickHouse unrewritten, where it could read or rename another tenant's physical tables.

Every statement reaches the engine, including on a session with no current database when `rewriter.physical_database` is empty; HouseGate no longer skips the rewriter because there is nothing to map. With an empty `physical_database` the engine has no database map, so it refuses every write, DDL, `USE` and `EXISTS` that names a logical database (for example `CREATE TABLE db1.t …`, `INSERT INTO db1.t …`, `USE db1`), while reads and `SET` still pass. HouseGate logs a startup warning (`rewriter.physical_database is empty: …`) in that configuration; set `physical_database` on any server that runs a rewriter.

An undecodable client Query packet is refused for the same reason: it cannot be rewritten, and forwarding its raw bytes would bypass the rewriter. This refusal happens before HouseGate knows who sent the query, so it also applies to maintenance and platform-operator sessions, whose decoded queries otherwise bypass the rewriter.

### What can now be refused

Any statement the selected engine does not model is refused instead of forwarded. On a deployment with storage integrity disabled, check client traffic for these shapes before upgrading, and have the client use a form the engine rewrites (or move the workload to a maintenance or platform-operator session, which bypasses the rewriter by design).

Measured on 2026-09-30 through HouseGate's own rewriter client with storage integrity disabled, `physical_database: phys`, the logical database `db1` mapped and auth off, both with no current database and with `db1` as the session database. Every row was forwarded before this change and is refused now wherever it says refused. Statements accepted everywhere are omitted: `SELECT` (qualified, unqualified with a session database, `system` tables, `FORMAT`, `SETTINGS`, `WITH`, `EXPLAIN`), `INSERT … VALUES (…)`, `INSERT … FORMAT Native|CSV|JSONEachRow`, `INSERT … SELECT`, `CREATE DATABASE` (with or without `ENGINE`), `CREATE TABLE` (engine, `AS db.t`, `AS SELECT`), views and materialized views, `DROP` / `TRUNCATE TABLE` / `RENAME` / `EXCHANGE`, `ALTER … ADD|DELETE|UPDATE`, lightweight `DELETE`, `SYSTEM FLUSH LOGS`, `SYSTEM STOP MERGES`, `CHECK TABLE`, `SET`, `USE`, `SHOW TABLES FROM`, `SHOW DATABASES`, `SHOW CREATE TABLE`, `SHOW COLUMNS`, `SHOW PROCESSLIST`, `DESCRIBE`, `EXISTS`, `GRANT` / `REVOKE`, `WATCH`.

| Statement | native v0.13.0, no database | native v0.13.0, session database `db1` | C++ 0.11.0 (stand-in), no database | C++ 0.11.0 (stand-in), session database `db1` |
|---|---|---|---|---|
| `INSERT INTO db1.t FORMAT Values` | refused (SyntaxError) | refused (SyntaxError) | accepted | accepted |
| `INSERT INTO FUNCTION remote('127.0.0.1:9000', db1.t) VALUES (1)` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) |
| `INSERT INTO FUNCTION file('x.csv', 'CSV', 'a UInt8') VALUES (1)` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) |
| `CREATE TABLE db1.n AS remote('127.0.0.1:9000', db1.t)` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) |
| `CREATE TEMPORARY TABLE tmp (a UInt8)` | refused (InvalidRewriteRequest) | accepted | refused (InvalidRewriteRequest) | accepted |
| `DROP TEMPORARY TABLE tmp` | refused (InvalidRewriteRequest) | accepted | refused (InvalidRewriteRequest) | accepted |
| `TRUNCATE db1.t` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | accepted | accepted |
| `UNDROP TABLE db1.t` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) |
| `OPTIMIZE TABLE db1.t` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) |
| `OPTIMIZE TABLE db1.t FINAL` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) |
| `DETACH TABLE db1.t` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) |
| `ATTACH TABLE db1.t` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | accepted | accepted |
| `KILL QUERY WHERE query_id = 'x'` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) |
| `SHOW TABLES` | refused (InvalidRewriteRequest) | accepted | refused (InvalidRewriteRequest) | accepted |
| `SHOW CREATE DATABASE db1` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) |
| `CREATE DICTIONARY db1.d (a UInt8) PRIMARY KEY a SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)` | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) | refused (UnsupportedStatement) |
| `SELECT * FROM db1.t<VT>WHERE 1` | refused (SyntaxError) | refused (SyntaxError) | accepted | accepted |
| `SELECT#x\n 1` | accepted | accepted | refused (SyntaxError) | refused (SyntaxError) |
| `SELECT 1; SELECT 2` | refused (SyntaxError) | refused (SyntaxError) | refused (SyntaxError) | refused (SyntaxError) |

`<VT>` is a vertical tab. The C++ columns come from rewriter-grpc 0.11.0, a stand-in: devnet2 indexer-b runs rewriter-grpc 0.14.0, whose image could not be pulled for this measurement. Re-run the list against 0.14.0 before rolling out there.

Notes on shapes that are accepted but still do not work:

- The bare streamed `INSERT INTO db1.t VALUES` (the shape older ClickHouse clients send before streaming rows) is accepted by native v0.13.0 but rewritten to `INSERT INTO VALUES`, dropping the table, so ClickHouse fails it; this predates the fail-closed change. The C++ engine rewrites it to ``INSERT INTO phys.`db1.t` FORMAT Values``, which works. The table-reference hardening builds of rewriter-go refuse it outright.
- Native v0.13.0 rewrites `SELECT#x\n 1` to `SELECT#x`, and `WATCH db1.lv` to `WATCH AS db1`; both fail in ClickHouse.

The client sees an Exception such as `rewriter rejected SQL (code=UnsupportedStatement): statement is not supported`. Re-check this list whenever the engine or its pin moves; the exact set differs by engine and build.

This list is a dated record of the fail-closed change against native v0.13.0 and C++ 0.11.0. HouseGate now requires rewriter-go v0.17.0 / rewriter-grpc v0.17.0, whose table-reference policy refuses more of the shapes listed as accepted above (for example `system` tables outside the allowlist such as `system.processes` and `SHOW PROCESSLIST`, table functions on a tenant's own tables such as `merge('db1', …)`, and engines such as `Buffer` or `Distributed`), and the table-reference guard and Query-packet settings check refuse some statements before the rewriter sees them. See [table-reference-hardening.md](table-reference-hardening.md) and spec 2026-09-26 §12.

## `rewriter.fail_open_on_unavailable`

```yaml
rewriter:
  fail_open_on_unavailable: false   # default
```

This switch covers only a rewriter that cannot be reached: at startup, when the rewriter cannot be built, and per query, when the request cannot be delivered because of the connection or the query's deadline.

**At startup.** A server that forwards to ClickHouse (a `shard`, an `upstream`, or a cluster injected by an embedding host) builds its rewriter before it listens: it dials the gRPC service, or fetches and loads the native library.

- `false` (default): if that fails (service unreachable, empty `service_addr` with the gRPC engine, library fetch or load failure), startup is refused with an error such as `SQL rewriter unavailable at startup: …; refusing to forward queries without it (set rewriter.fail_open_on_unavailable: true to run without the rewriter)`.
- `true`: HouseGate logs `SQL rewriter unavailable at startup; running without the rewrite plugin, every query is forwarded unrewritten (rewriter.fail_open_on_unavailable)` and runs for the life of the process with no rewriting at all: no logical-to-physical mapping and no engine policy. Only the lexical table-reference guard and the Query-packet settings check ([table-reference-hardening.md](table-reference-hardening.md)) still run. Restart once the rewriter is back.

The switch covers only a rewriter that cannot be built. A rewriter that is built, from config or injected by an embedding host, must also pass the table-reference probe (and, with storage integrity, the storage-integrity probe) before HouseGate listens; a probe failure refuses startup whatever the switch says, naming the failed case and the required engine builds.

**Per query.** Only a transport failure before the request left HouseGate counts: the connection to the rewriter is down, the rewriter was closed during shutdown, or the query's deadline or cancellation fired before the request was sent. HouseGate classifies a failure this way only when both hold: the request message was never handed to the transport, and the gRPC status is `Unavailable`, `DeadlineExceeded` or `Canceled`. This is a bounded heuristic, not a proof that the engine never saw the statement: a request queued into a transport that then dies counts as sent (and is refused), and a deadline that expires while a very large request is still being serialised or flow-controlled counts as unavailable.

- `false` (default): the client receives the Exception `rewriter unavailable` and nothing is sent to ClickHouse.
- `true`: HouseGate logs a warning (`rewriter unavailable; forwarding original SQL (rewriter.fail_open_on_unavailable)`) and forwards the original SQL unrewritten.

Every other failure is a rejection even with the switch on, because it can depend on the statement: a pre-send failure with any other status (for example a request that fails to serialise), an engine status error, a crash mid-call, a deadline that expires after the request was sent, or a native engine error.

Residual risk with `true`: a statement that crashes the rewriter is itself refused, but while the service restarts every query finds the connection down and is forwarded unrewritten. Anyone who can crash or stall the engine therefore gets a pass-through window. Use `true` only where availability outweighs isolation, for example a single-tenant deployment or local development without a rewriter.

`fail_open_on_unavailable: true` together with `storage_integrity.enabled` is a configuration error (`rewriter.fail_open_on_unavailable cannot be combined with storage_integrity.enabled`); storage integrity always fails closed.

**Servers that never rewrite.** A router-only server (no `shard`, no `upstream`, and no cluster injected by the host) forwards whole connections to peers and never builds a rewriter, so it starts regardless of the rewriter settings and this switch. An embedding host that injects its own rewriter factory is not built at startup, so the build check does not apply, but the factory must still pass the table-reference probe; a nil factory, including a typed-nil one, counts as not injected.

**Upgrading.** A server with an `upstream` that has been running without a working rewriter (the old warn-and-continue startup) now refuses to start. Either point it at a rewriter (`engine: native` with `native_library_release`, or `engine: grpc` with a reachable `service_addr`), or set `fail_open_on_unavailable: true` deliberately.

**Rollout.** Startup now depends on the rewriter being reachable within `rewriter.timeout` (default 5s). Where the rewriter runs as a separate process or container, start it first (for example a Kubernetes native sidecar with a startup probe on its gRPC port) or rely on restart-on-failure; a slow rewriter makes HouseGate, and any process that embeds it, exit at startup instead of running without rewriting.

## What the switch does not cover

- **Sessions that bypass the rewriter.** Maintenance sessions (indexer-signed), platform-operator sessions and peer-trusted sessions arriving from another HouseGate never call the rewriter, so neither rejections nor the switch apply to their decoded queries (an undecodable Query packet is still refused, see above). On an origin HouseGate that forwards a whole session to a peer, the receiving HouseGate runs the rewriter and enforces its answer. Keep `internal_listen` reachable only from trusted peer subnets.
- **The agent's `materialize` step.** The agent-side `MaterializeSQL` call is a determinism aid, not an isolation boundary: on failure it leaves the statement unchanged, and the server-side HouseGate still rewrites and enforces the result. The signed inline-VALUES lane already refuses a statement whose materialization failed.
