# Rewriter rejections and `rewriter.fail_open_on_unavailable`

HouseGate sends every ordinary tenant query through the SQL rewriter (the external `sql-rewriter` gRPC service or the in-process native engine) before it reaches ClickHouse. This note describes what happens when the rewriter does not return a rewritten statement, and the one switch operators can turn.

## Rejections always fail closed

When the rewriter answers with anything other than `Success` (`UnsupportedStatement`, `SyntaxError`, `InvalidRewriteRequest`, `RewriteError`, or a code a newer engine adds), HouseGate refuses the query. The client receives a ClickHouse Exception whose message carries the engine's reason, and nothing is sent to ClickHouse. This holds with and without `storage_integrity.enabled`.

Before this change, deployments with storage integrity disabled forwarded the original SQL verbatim on such an answer. That neutralised every engine refusal: a statement the engine could not parse or did not model (for example one containing a vertical tab, or a `#` glued to a keyword) reached ClickHouse unrewritten, where it could read or rename another tenant's physical tables. There is no configuration that restores that pass-through.

An undecodable client Query packet is refused for the same reason: it cannot be rewritten, and forwarding its raw bytes would bypass the rewriter.

### What can now be refused

Any statement the selected engine does not model is refused instead of forwarded. On a deployment with storage integrity disabled, check client traffic for these shapes before upgrading, and have the client use a form the engine rewrites (or move the workload to a maintenance or platform-operator session, which bypasses the rewriter by design).

Measured against the pinned native engine (rewriter-go v0.13.0) with storage integrity disabled, each of these was forwarded before and is refused now:

| Statement | Engine answer |
|---|---|
| `OPTIMIZE TABLE db1.t FINAL` | `UnsupportedStatement`: statement is not supported |
| `DETACH TABLE db1.t`, `ATTACH TABLE db1.t` | `UnsupportedStatement`: statement is not supported |
| `KILL QUERY WHERE …` | `UnsupportedStatement`: statement is not supported |
| `CREATE DICTIONARY …` | `UnsupportedStatement`: CREATE DICTIONARY is not supported |
| `INSERT INTO db1.t FORMAT Values` (rows streamed after the statement) | `SyntaxError`: the parser expects the rows inline |
| `SHOW TABLES` with no current database (no hello database, no `USE`) | `InvalidRewriteRequest`: send `USE <db>` or `SHOW TABLES FROM <db>` |
| Any statement containing a vertical tab, or other text the parser rejects but ClickHouse accepts | `SyntaxError` |

The same run accepted `SYSTEM …`, `CHECK TABLE`, `SET`, `CREATE DATABASE … ENGINE = …`, and the bare streamed `INSERT INTO db1.t VALUES` (the shape the ClickHouse native client sends before streaming the rows), so those still pass on v0.13.0. The table-reference hardening engine builds refuse more: rewriter-go refuses the bare streamed `INSERT … VALUES` and `CREATE DATABASE … ENGINE`, which the C++ engine (rewriter-grpc) accepts. Re-check this list whenever the engine pin moves, and measure the C++ engine separately if you run it; the exact set differs by engine and build.

The client sees an Exception such as `rewriter rejected SQL (code=UnsupportedStatement): statement is not supported`.

## `rewriter.fail_open_on_unavailable`

```yaml
rewriter:
  fail_open_on_unavailable: false   # default
```

This switch covers only a rewriter that cannot answer, at startup or per query.

**At startup.** A server with a `shard` or `upstream` builds its rewriter before it listens: it dials the gRPC service, or fetches and loads the native library.

- `false` (default): if that fails (service unreachable, empty `service_addr` with the gRPC engine, library fetch or load failure), startup is refused with an error such as `SQL rewriter unavailable at startup: …; refusing to forward queries without it (set rewriter.fail_open_on_unavailable: true to run without the rewriter)`.
- `true`: HouseGate logs `SQL rewriter unavailable at startup; running without the rewrite plugin, every query is forwarded unrewritten (rewriter.fail_open_on_unavailable)` and runs for the life of the process with no rewriting at all: no logical-to-physical mapping and no engine policy. Restart once the rewriter is back.

**Per query.** Once running, a dial failure, a call timeout, a nil response, a rewriter closed during shutdown, or a failed network-state lookup while building the request:

- `false` (default): the client receives an Exception `rewrite unavailable: …` and nothing is sent to ClickHouse.
- `true`: HouseGate logs a warning (`rewriter unavailable; forwarding original SQL (rewriter.fail_open_on_unavailable)`) and forwards the original SQL unrewritten.

Use `true` only where availability outweighs isolation, for example a single-tenant deployment or local development without a rewriter.

**Servers that never rewrite.** A router-only server (no `shard`, no `upstream`) forwards whole connections to peers and never builds a rewriter, so it starts regardless of the rewriter settings and this switch. An embedding host that injects its own rewriter factory is not subject to the startup check either.

**Upgrading.** A server with an `upstream` that has been running without a working rewriter (the old warn-and-continue startup) now refuses to start. Either point it at a rewriter (`engine: native` with `native_library_release`, or `engine: grpc` with a reachable `service_addr`), or set `fail_open_on_unavailable: true` deliberately.

It never applies to a rejection: an engine answer is always enforced.

`fail_open_on_unavailable: true` together with `storage_integrity.enabled` is a configuration error (`rewriter.fail_open_on_unavailable cannot be combined with storage_integrity.enabled`); storage integrity always fails closed.

## What the switch does not cover

- **Sessions that bypass the rewriter.** Maintenance sessions (indexer-signed), platform-operator sessions and peer-trusted sessions arriving from another HouseGate never call the rewriter, so neither rejections nor the switch apply to them. On an origin HouseGate that forwards a whole session to a peer, the receiving HouseGate runs the rewriter and enforces its answer. Keep `internal_listen` reachable only from trusted peer subnets.
- **The agent's `materialize` step.** The agent-side `MaterializeSQL` call is a determinism aid, not an isolation boundary: on failure it leaves the statement unchanged, and the server-side HouseGate still rewrites and enforces the result. The signed inline-VALUES lane already refuses a statement whose materialization failed.
