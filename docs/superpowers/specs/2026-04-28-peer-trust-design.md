# Peer-Trust Cross-Proxy Authentication — Design Spec

**Date:** 2026-04-28
**Status:** Implemented
**Authors:** poetry, Claude

## 1. Goal

When a server-mode housegate's rewriter emits a `remote(...)` clause that
targets **another housegate** (cross-shard / cross-indexer query routing),
the receiving proxy must accept and execute the inner SQL **without**
re-running its full client-auth pipeline. Existing per-query JWS auth
binds `qhash = Keccak256(SQL)` and is fundamentally incompatible with this
flow: ClickHouse rewrites the statement before issuing it through
`remote()`, so any token bound to the outer SQL fails verification on the
peer side.

The peer-trust scheme replaces "the originating client signed this exact
SQL" with "an authenticated upstream housegate vouches for this leg" —
authenticating at the **handshake**, not per-query, and short-circuiting
auth+rewrite on the receiving side so the inner statement passes through.

## 2. Non-goals

- **Not a saga / cross-system rollback.** Peer trust is a handshake-level
  bearer credential. The receiving proxy still runs concurrency, usage,
  and commitgate plugins normally; only auth (qhash) and rewrite are
  bypassed because the upstream's rewriter produced final SQL.
- **Not a per-query identity claim.** The originating client's identity
  is established and authorized at the **upstream** proxy. The peer JWS
  authenticates the upstream proxy itself; downstream auditing relies on
  the upstream's logs, not the peer's. (A future revision could add a
  forwarded-identity claim.)
- **No protocol additions to the ClickHouse native protocol.** Everything
  rides inside the existing `user` / `password` fields of the
  `remote()`-initiated handshake.
- **No new key material.** The same secp256k1 relay key already used for
  the SQL-binding JWS is reused for the peer JWS — different payload,
  same trust anchor. Operators still configure one allowlist.

## 3. Why query-binding JWS doesn't work for `remote()`

A `remote('host:port', 'u', 'p', 'db', 'tbl')` clause causes ClickHouse to
open a fresh native-TCP connection to `host:port`, transmit
`'u'` / `'p'` in the ClientHello, then send a Query packet whose body is
**not** the original SQL. ClickHouse rewrites and prunes the inner
statement: column projection pushdown, predicate pushdown, alias
resolution, etc. By the time the peer proxy reads the Query packet, the
SQL is something like:

```
SELECT value FROM "testnet"."tenant2.transfer"
```

— totally distinct from the outer
`SELECT … UNION ALL SELECT value FROM remote(...)` the client signed.

Even if a JWS were forwarded in `Settings[SQL_x_auth_token]`, two things
fail:

1. **Settings propagation is gated** on both ends configuring
   `custom_settings_prefixes = ['SQL_']` — operators routinely forget to
   do this on the peer.
2. **`qhash = Keccak256(outer SQL)` ≠ `Keccak256(inner SQL)`** — the JWS
   payload's binding is wrong by construction. No setting-passthrough
   tweak can fix this; the upstream proxy cannot predict ClickHouse's
   rewrite of the inner statement.

The peer-trust scheme solves this by signing **identity claims** instead
of SQL, and by carrying the JWS in the protocol's existing
auth-at-handshake field (`password`) rather than per-query settings.

## 4. Wire format

Two prefixed envelopes share the same `|` delimiter convention so they
can nest cleanly:

| Envelope | Prefix | Format | Set by | Read by |
|---|---|---|---|---|
| Route | `__route__` | `__route__\|<target-host:port>\|<realUser>` | rewriter @ static-mapping path | `routeplugin.Stripper` @ OnHello |
| Peer | `__peer__` | `__peer__\|<lowercased-eth-address>` | rewriter @ both paths (when `peerSigner` wired) | `credential.Plugin` @ OnHello |

### 4.1 Outbound: rewriter emits

For a `remote()` call that goes through the local proxy's callback
address (the static-mapping path), the rewriter wraps a peer envelope
**inside** a route envelope so the route Stripper peels the outer layer
locally and the peer envelope reaches the receiving proxy verbatim:

```text
user     = "__route__|10.0.0.8:9001|__peer__|0xabcdef..."
password = <peer-relay JWS>
```

For the dynamic-mapping path (a logical database routed via
`logical_database_to_remote_upstream_index`), `remote()` dials the peer
directly — no local callback, no route envelope:

```text
user     = "__peer__|0xabcdef..."
password = <peer-relay JWS>
```

### 4.2 Peer-relay JWS payload

```json
{
  "iat":     1714305600,
  "exp":     1714305900,
  "aud":     "42",
  "purpose": "peer-relay"
}
```

| Field | Meaning |
|---|---|
| `iat` | Issued-at, unix seconds. |
| `exp` | Expiration, unix seconds. Caller (rewriter) sets a 5-minute TTL. |
| `aud` | Receiving proxy's indexer-id, decimal-stringified. Prevents replay across peers. |
| `purpose` | `"peer-relay"` constant. Domain-separates from query-binding JWS so a query token cannot replay as a peer login (and vice versa). |

Signed via the same ES256K (secp256k1) primitive as the existing relay
JWS — header `{"alg":"ES256K","typ":"JWT"}`. The receiving validator
recovers the signer address and matches against
`cfg.Auth.AllowedAddresses`.

### 4.3 Why both `__route__` and `__peer__` exist

The route envelope answers **"which proxy should I dial?"** — it lets
the local CH bounce back through this proxy so we keep one TCP
connection-tracking surface and so the local rewriter can emit a single
constant `callbackAddr` regardless of which peer is targeted. Without
it, every peer would need its own outbound CA in the firewall, and
multiplexing would be harder.

The peer envelope answers **"who is the upstream proxy and is it
trusted?"** — completely independent question, deliberately a separate
layer. They nest because the route layer is consumed locally and the
peer layer must survive forwarding to the peer.

## 5. Receiving-side flow

```
ProxyA's rewriter: emit remote(callbackAddr,
                              "__route__|peer:9001|__peer__|0xA",
                              <peer-JWS, aud=peer.IndexerID>,
                              db, table)
                ↓ ClickHouse-A executes; dials callbackAddr
ProxyA listener: accept → OnConnect → OnHello
  routeplugin.Stripper: parses __route__|peer:9001| prefix
                         → SetRouteTarget("peer:9001")
                         → hello.User = "__peer__|0xA"  (realUser, peer
                           envelope still intact)
  credential.Plugin (RouteAware? NO):
                         → skipped (PluginChain bypasses non-RouteAware
                           on routed sessions)
  ... all other Hello plugins skipped, session is "routed"
ProxyA dialer: routeplugin.RouteTarget → dial peer:9001 directly
ProxyA → forward Hello (user="__peer__|0xA", pwd=<JWS>) verbatim to peer
                ↓
ProxyB listener: accept → OnConnect → OnHello on a fresh inbound conn
  routeplugin.Stripper: no __route__ prefix → no-op
  credential.Plugin: peer.ParseUser("__peer__|0xA") → match
                     → PeerValidator.ValidatePeerLogin(JWS, aud=ProxyB.indexerID)
                     → recovered addr ∈ AllowedAddresses
                     → SessionState.SetPeerTrust(recoveredAddr)
                     → hello.User = ""
                     → fall through to provider replace (CH credentials)
  sessionstate.Plugin: track database / settings
  rewrite.Plugin OnHello: not peer-trust gated (Hello stage runs always)
ProxyB OnHandshakeComplete / OnQuery / ...
  Filter: routed=false, peerTrust=true
  → auth.Plugin (PeerTrustAware, RunOnPeerTrust=false): SKIP
  → rewrite.Plugin (PeerTrustAware, RunOnPeerTrust=false): SKIP
  → concurrency / usage / commitgate / sessionstate / metrics: RUN normally
ProxyB → forward inner SQL to local ClickHouse
```

## 6. Architecture: two marker interfaces

The `__route__` and `__peer__` envelopes both surface as plugin-chain
filters via opt-in / opt-out marker interfaces in `pkg/plugin`:

| Interface | Default for non-implementers | Production opters |
|---|---|---|
| `RouteAware.RunOnRouted() bool` | **skip** (must opt-in to keep firing) | `routeplugin.Stripper`, `routeplugin.Signer`, `metricsplugin.Plugin` |
| `PeerTrustAware.RunOnPeerTrust() bool` | **run** (must opt-out to skip) | `authplugin.Plugin` (returns `false`), `rewrite.Plugin` (returns `false`) |

Default polarity is intentionally inverted because the two filters
describe different things. A *routed* session is a transit-only path —
the receiving proxy is the destination, the local proxy just forwards,
so almost everything (auth, rewrite, state, usage) is meaningless and
should be off by default. A *peer-trusted* session is a normal client
session as far as ClickHouse is concerned — it just happens to have
been authenticated by an upstream proxy instead of a client, so almost
everything (state, usage, concurrency, commitgate) should keep running
to track resources and gate DDL; only the two layers whose semantics
target the *original* client SQL (auth qhash, rewrite) skip.

The two filters compose: a session can be both routed *and*
peer-trusted (e.g. a multi-hop relay). `PluginChain` applies the
RouteAware filter first, then the PeerTrustAware filter on the
survivors. Plugin authors who need either behaviour declare the
relevant marker; plugin authors who don't declare anything get the
defaults and never have to know either filter exists.

`OnHello` is intentionally exempt from the peer-trust filter (the
credential plugin lives there and is what flips the flag), as are the
lifecycle hooks `OnConnect` / `OnDisconnect` / `OnClose` — they fire
before the session has a known route/peer status and must run
unconditionally to keep tear-up/tear-down symmetric.

## 7. Configuration

### 7.1 New top-level fields

```yaml
indexer_id: 42                           # this proxy's identity in NetworkState
relay_private_key_hex: "0xabc..."        # signs both query-binding JWS and peer-relay JWS
relay_public_key_hex: "0x04..."          # informational; for registry / advertisement
```

| Field | Env override | Required for peer-trust? |
|---|---|---|
| `indexer_id` | `HOUSEGATE_INDEXER_ID` | Yes (audience claim) |
| `relay_private_key_hex` | `HOUSEGATE_PRIVATE_KEY` | Yes (signs outbound peer JWS) |
| `relay_public_key_hex` | `HOUSEGATE_PUBLIC_KEY` | Optional |

`Options.GetIndexerId` (dynamic, library-host-supplied) takes precedence
over `Config.IndexerID` (static). `Options.Signer` (library-host-supplied
auth.Signer) takes precedence over a lib-built `RelaySigner`.
`relay_public_key_hex` is currently informational — sign-side only needs
the private key — but is loaded so library hosts that publish peer
identity into a registry have a single canonical source.

### 7.2 Allowlist

The auth plugin's `cfg.Auth.AllowedAddresses` is the single allowlist
used by both validators (query-binding and peer-relay). Adding a new
peer means adding its eth address there; no separate peer-only list.

## 8. Trust model & threat surface

**Trusted:**
- All peers that share the same allowlist trust each other to vouch for
  client traffic.
- The upstream proxy is responsible for client-side authorization
  (rate-limit, concurrency, audit). The peer trusts that this happened.

**Mitigations against bearer-token misuse:**
- `aud` claim binds the token to a specific peer's indexer-id. A token
  signed for proxy B cannot be replayed against proxy C.
- `exp` (5 min default) bounds replay window if the JWS leaks (e.g.
  appears in a query log).
- `purpose: peer-relay` domain-separates from query JWS, so an attacker
  cannot mint a query token offline and present it as peer auth.
- Stripper does not enforce that the client connection is local; the
  caller (cmd / library host) is expected to firewall the listener.
  This is unchanged from the pre-existing route envelope and applies
  identically to the nested peer envelope.

**Not mitigated (acceptable):**
- A compromised peer can issue arbitrary peer JWSs and so submit
  arbitrary SQL through any other peer. The recovery model is removing
  the address from the allowlist + key rotation. Same property as the
  pre-existing relay JWS.

## 9. Open questions / future work

1. **Forwarded client identity.** The peer JWS currently authenticates
   only the upstream proxy. A future `client_addr` claim in the payload
   would let downstream auditing attribute the query to the original
   client. Cheap to add when needed.
2. **Per-peer key rotation.** Today all peers share one relay key.
   Per-peer keys would allow finer-grained revocation but require
   either NetworkState to publish per-peer pubkeys or a static config
   map. Not motivated yet.
3. **Sidecar peer trust.** Sidecars don't currently have peer-trust
   handshake logic on inbound — they only sign outbound. If a sidecar
   ever needs to accept a peer-routed connection, the credential
   plugin's wiring would have to be extended into `buildSidecar`.
4. **Loop prevention.** A peer-trusted session that itself emits
   `remote()` would mint another peer JWS through this proxy's
   rewriter. There is no max-hop counter today; the natural backstop
   is `peerTokenTTL` and the fact that `IsPeerTrusted` causes rewrite
   to bypass (so a peer connection's inner SQL is forwarded as-is and
   doesn't traverse rewriter again on this proxy). Should be revisited
   if multi-hop topologies become common.

## 10. File map

| File | Responsibility |
|---|---|
| `pkg/peer/parser.go` | `__peer__|<addr>` `Format`/`Parse` (mirror of `pkg/route`) |
| `pkg/auth/types.go` | `JWSPeerPayload`, `PeerLoginPurpose` constant |
| `pkg/auth/relay_signer.go` | `RelaySigner.SignPeerLogin(audience, ttl)` |
| `pkg/auth/eth_validator.go` | `EthValidator.ValidatePeerLogin(token, expectedAud)` |
| `pkg/auth/signer.go` | `PeerSigner` / `PeerValidator` interfaces |
| `pkg/chsession/state.go` | `IsPeerTrusted` / `PeerAddress` + `SetPeerTrust` / `PeerTrusted` / `GetPeerAddress` |
| `pkg/plugin/plugin.go` | `PeerTrustAware` marker interface |
| `pkg/plugin/chain.go` | `runsOnPeerTrust(p)` filter applied alongside `runsOnRouted(p)` |
| `pkg/plugins/credential/injector.go` | OnHello: detect `__peer__\|`, validate JWS, set IsPeerTrusted, then run provider replace |
| `pkg/plugins/auth/jws.go` | `RunOnPeerTrust() bool { return false }` — opt-out |
| `pkg/plugins/rewrite/rewriter.go` | `RunOnPeerTrust() bool { return false }` — opt-out |
| `pkg/rewriter/types.go` | `PeerSigner` interface (rewriter-package-scoped, no pkg/auth dep) |
| `pkg/rewriter/sentio.go` | `SentioNetworkFactory.peerSigner` + `SetPeerSigner` + `resolveRemoteCredentials` helper used at both remote() emission paths |
| `pkg/config/config.go` | `IndexerID`, `RelayPrivateKeyHex`, `RelayPublicKeyHex` + env-var defaults |
| `proxy.go` | `New()` derives `Options.GetIndexerId` from `Config.IndexerID` when caller didn't supply one |
| `build.go` | Builds `PeerSigner` from `RelaySigner`, wires it into rewriter and credential plugin |
