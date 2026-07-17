# HouseGate Storage Integrity Signed Ingress Admission

Date: 2026-07-16

## Purpose

This change defines the HouseGate-local contract that turns a signed,
materialized INSERT or UPDATE/DELETE mutation into a complete storage-integrity
admission record. It verifies the client-side signature before accepting the
server-side rewritten statement, resolves the logical target table from
rewriter metadata, and captures exact Native `ClientData` wire bytes for later
staged SNode intake.

The implementation is intentionally fail-closed at every boundary that would
otherwise make the signed SQL and the captured payload refer to different
query lifecycles.

## Design Anchors

The contract implements the HouseGate ingress responsibilities in the unified
storage-integrity design:

- Section 4.1: client-side HouseGate materializes allowed nondeterministic
  functions, injects protocol row identity where required, and signs the final
  client-side `Query.Body` with `purpose = housegate-query` and
  `qhash = Keccak256(Query.Body)`.
- Section 4.2: server-side HouseGate verifies JWS, signer allowlist, purpose and
  qhash before using sql-rewriter output for statement classification and
  logical/physical target mapping.
- Section 6.1: INSERT Native bytes are retained as the payload used by later
  unsafe write, payload-store and replay stages.
- Section 7.1: UPDATE/DELETE enter through the same signed admission boundary,
  while bounded-predicate validation and mutation execution remain later
  stages.

This contract does not change the Sentio Arbiter design or require any Arbiter
API/FSM change.

## SQL Identity Boundary

The ingress gate handles two SQL representations and must not conflate them:

1. `QueryContext.OriginalSQL` is the materialized SQL received from the
   client-side HouseGate. The JWS qhash is verified against this exact string.
2. `QueryContext.Query.Body` is the current server-side SQL after earlier
   HouseGate plugins may have rewritten logical objects to physical objects.
   Statement-kind validation is performed against this forwarded form.

If `OriginalSQL` is empty, the current `Query.Body` is used as the signed SQL
for compatibility with direct plugin tests and callers that have not populated
the original field. Production Relay always initializes `OriginalSQL` before
running the query chain.

Both SQL forms are checked for unmaterialized nondeterministic functions when
they differ. This prevents a valid signed input from becoming nondeterministic
through a later rewrite.

## JWS Contract

The authenticated token is read from `SQL_x_auth_token`. Admission requires:

- a syntactically and cryptographically valid JWS;
- an authenticated, non-empty signer address accepted by the configured
  validator/allowlist;
- `purpose = housegate-query`;
- `qhash = Keccak256(signed SQL)`;
- a non-empty ClickHouse query ID used as `statement_id`.

`RelaySigner.SignToken` emits `housegate-query` by default. Legacy
`ValidateQuery` remains purpose-optional for existing query-auth users, while
`ValidateQueryPurpose` is used by storage integrity to require domain
separation. A validator that cannot validate purpose claims is rejected.

The enabled ingress plugin also opts into strict Query decoding. A client Query
that cannot be decoded is rejected before raw-splice fallback, because an
opaque Query cannot pass JWS, kind or target validation. When ingress is
disabled or no strict query plugin is active, Relay retains its legacy D8 raw
pass-through behavior for forward compatibility.

## Statement And Target Admission

When `storage_integrity.ingress.enabled` is false, the plugin is a no-op. When
enabled, it admits these storage-integrity write forms:

- `INSERT`;
- normalized `UPDATE` and `DELETE`;
- `ALTER TABLE ... UPDATE` and `ALTER TABLE ... DELETE`.

Read-like statements pass through. DDL, DCL and destructive statement kinds
such as `CREATE`, `DROP`, `TRUNCATE`, ordinary `ALTER`, `RENAME`, `GRANT`,
`REVOKE`, `ATTACH`, `DETACH` and `OPTIMIZE` fail closed in this lane.

Target validation uses the signed SQL target plus every entry in
`QueryContext.AccessedTables`. It does not assume the first metadata entry is
the write target. Backtick-quoted identifiers and quoted dotted identifiers are
normalized structurally. The resulting admission stores the logical table ID;
the server-side physical rewrite is not treated as a different signed target.
An unqualified SQL target is resolved against the session logical database. If
that database is unavailable and metadata contains more than one matching
logical target, admission rejects the ambiguity instead of choosing by order.

The plugin rejects a disagreement between SQL statement kind,
`QueryContext.StatementType` and rewriter target metadata.

## Native Payload Capture

Relay invokes correctness-critical `StrictDataPlugin` hooks before forwarding
each client `Data` packet. The storage-integrity hook copies the exact complete
on-wire packet, including packet code, block name, compressed or uncompressed
body, and the terminating empty Data block. It does not mutate the packet and
does not retain Relay-owned memory.

The existing `DataPlugin` hook remains a fail-open observation path. Strict
capture is separate: a strict-hook error writes a synthetic ClickHouse
exception, aborts the exact query context and prevents the rejected packet from
being forwarded.

`StrictDataLimitPlugin` exposes the remaining cumulative payload budget before
Relay reads the next packet. The codec applies that budget while reading a
`ClientData` packet, including the bytes already read for its packet code, so an
oversized packet is rejected without first allocating or copying the full
packet. When several strict plugins provide a limit, Relay enforces the
smallest enabled limit.

The default cumulative payload limit is 64 MiB. A configured zero limit is
invalid while ingress is enabled.

## Query Lifecycle

Storage-integrity completion is tied to client input, not to an upstream TCP
read heuristic:

1. `OnQuery` creates one active admission for the statement and session.
2. Strict Data capture appends only packets owned by that same query context.
3. Relay recognizes protocol completion only when a decoded `ClientData` block
   has zero columns and zero rows.
4. Relay forwards that terminating packet successfully and then calls
   `OnQueryInputComplete` with the exact `QueryContext`.
5. The plugin moves the matching active admission to the consumable pending
   set. Upstream `OnQueryComplete` does not publish admission readiness.

Only one active or unconsumed pending storage-integrity admission is allowed
per session. A query-scoped abort removes state only when both session ID and
statement ID match, so a late event cannot delete a newer statement.

If a query is rejected before forwarding, Relay drains and discards its Data
packets until the terminating empty block. It does not forward those bytes and
does not accept a new Query while rejected input is still being drained. A new
Query received before the active forwarded query's input terminator is also a
protocol error. These rules prevent old or rejected Data from being captured
under a later statement. Relay applies the rejected Query's declared
compression mode before draining, so compressed and uncompressed terminators
have the same lifecycle semantics.

`AbortWithSuccess`, query-forward failure, strict capture failure, payload-limit
failure and client-input classification failure all invoke query-scoped abort.
Generic `OnQueryComplete` remains the release point for existing per-query
resources, but is not a storage-integrity correctness signal.

## Admission Record

`ConsumeAdmission(sessionID)` succeeds only for a matching, input-complete
record. It removes the pending record and returns:

- statement ID and admitted statement kind;
- normalized logical table ID;
- signed/materialized SQL;
- normalized signer address;
- exact captured Native bytes;
- payload byte length;
- deterministic `sha256:<hex>` payload hash;
- client protocol revision pinned when the query was admitted;
- completion flag.

An INSERT without any captured Data packet is rejected. UPDATE/DELETE do not
require a Native payload in this stage.

## Configuration

`storage_integrity.ingress.enabled` defaults to `false`. Enabling it is valid
only in server mode and requires:

- a non-empty signer allowlist;
- a positive max token age;
- a positive request timeout;
- a positive max payload size.

Defaults are:

- `max_token_age: 1m`;
- `request_timeout: 5s`;
- `max_payload_bytes: 67108864`.

## Verification

Focused gate:

```bash
go test ./pkg/chproto ./pkg/config ./pkg/sqlident ./pkg/auth ./pkg/plugin ./pkg/proxy ./pkg/plugins/storageintegrity -count=1
```

Race gate:

```bash
go test -race ./pkg/chproto ./pkg/config ./pkg/sqlident ./pkg/auth ./pkg/plugin ./pkg/proxy ./pkg/plugins/storageintegrity -count=1
```

Key coverage includes:

- purpose-bound signer and validator behavior;
- strict rejection of undecodable Query packets while ingress is enabled;
- qhash verification against signed SQL before server rewrite;
- rejection of nondeterminism in signed and rewritten SQL;
- logical target resolution across multiple and quoted metadata entries;
- exact Native capture, ownership, hash, length and client revision;
- compressed and uncompressed empty-Data completion detection;
- query-scoped input completion and abort;
- rejection of active and rejected query pipelining;
- read-time cumulative payload limiting;
- strict-hook short-circuiting before fail-open observers.

## Non-Scope

This change does not implement physical rewrite to `hg_unsafe`, bounded
predicate analysis, staged prepare, payload-store writes, unsafe ClickHouse
writes, Arbiter/SNode calls, ACK2 convergence, replay roots, manifests,
SafeAudit, repair, compaction, promotion or runtime plugin wiring.
