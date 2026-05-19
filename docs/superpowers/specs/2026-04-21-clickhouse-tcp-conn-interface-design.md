# ClickHouse TCP Conn Interface — Design Spec

**Date:** 2026-04-21
**Status:** Draft (awaiting review)
**Scope:** Abstract the ClickHouse native TCP connection handling in `housegate` into (A) a pure-protocol codec layer and (C) a session layer with replayable state, replacing the current `proxy.go` god-file structure.

---

## 1. Problem & Goals

Today all ClickHouse native TCP handling lives in `pkg/proxy/proxy.go` (~1800 LOC): handshake, packet loop, handshake routing, agent signing, forwarding, compressed Data-block frame probing, Prometheus metrics, and every piece of per-connection state are co-located. Dependencies are injected after construction via seven `Set*` methods on `*Proxy`, and test isolation is poor (init-registered Prometheus globals panic on double-import).

The target architecture (CLAUDE.md §"Target Architecture") calls for a `pkg/protocol/` codec plus a `pkg/proxy/session.go` + `relay.go` split. This spec defines the two central abstractions — **Codec (layer A)** and **Session (layer C)** — that make that split concrete.

### Goals

- **G1**: A `chproto` package that is a thin, proxy-oriented wrapper over `github.com/ClickHouse/ch-go/proto`, reusing its primitives and adding only what proxy needs (tagged-union read, zero-copy splice, chunked addendum, feature helpers).
- **G2**: A `chsession` package that holds replayable per-connection state as plain data, decoupled from both the packet loop and business plugins.
- **G3**: Interfaces shaped so the MVP (one client conn ↔ one long-lived upstream conn) can evolve to per-query pooled upstream with session-state replay (target arch §5.4) **without breaking the Session interface**.
- **G4**: Packet modification is expressed as middleware chains over a small set of hook points (`OnHello`, `OnQuery`, `OnException`, `OnClose`), run by a single `Relay` driver.

### Non-goals

- Implementing the plugin system itself (this spec defines the hook surface; plugins are authored separately).
- Rewriter service changes (the gRPC rewriter stays as-is; we only change how it's called).
- Replacing `cluster` / pool code.
- HTTP interface.
- Temp-table replay (explicitly out of scope — operationally rare, adds large complexity).

---

## 2. Design Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | Abstract at two layers: **A (codec)** + **C (session)**. No separate "endpoint" layer between them. | User preference; the two codec instances the session holds are symmetric enough that a B layer would only add boilerplate. |
| D2 | Codec is **hybrid**: header-aware `ReadPacket(decodeTypes...)` returns typed struct for requested types, raw bytes for everything else. | Data/Progress/Pong etc. stay zero-copy; only Hello/Query/Exception need decode/re-encode. Matches current `handleDataBlock` performance path. |
| D3 | `chproto` is a **thin wrapper over `ch-go/proto`**, not a reimplementation. Re-export `proto.ClientHello`/`Query`/`Exception`; add `ReadPacket`/`Splice`/`Addendum`/feature helpers. | ~500-800 LOC versus rewriting a full codec. ch-go already owns the low-level primitives. |
| D4 | Upstream binding: **MVP = C1** (session-level 1:1 long-lived upstream), interface shaped for **C3** (mostly C1, swap upstream on failover with state replay). | User-stated target. `RebindUpstream` method + `SessionState.Replay()` are implemented day one; just not called. |
| D5 | `SessionState` contains: identity & credentials, protocol negotiation results (client-side), `Database`, session-wide `Settings`, `LastQueryID`/`ClientInfo`, JWS `Identity`, `LastRewriteArgs`. **No temp-table tracking**. | Per user decision: identities/protocol/db/settings/query-id/identity/rewrite-args in; temp tables out. |
| D6 | `USE`/`SET` detection uses **regex/prefix match**, not rewriter AST. | User preference: AST round-trip is too heavy for two trivially-stable syntaxes. Comment and user-facing doc must explain. |
| D7 | Only **session-level** settings (from `SET k=v` statements) enter `SessionState.Settings`. `Query.Settings` (per-query payload) stay on the query, never leak to session. | Semantic correctness. |
| D8 | On Decode failure: **fail-open** — forward original bytes via Splice. | Matches current behavior (Hello falls back to `fallbackRevision`); preserves forward-compatibility. |
| D9 | Plugin invocation model: **middleware chain** per hook point. Protocol phase is an **implicit** state machine (pre-handshake → idle → in-query) expressed in Relay code flow, not a `SessionPhase` enum. | Simple, predictable, matches current code. Explicit state machine adds a second source of truth for little gain at our scale. |

---

## 3. Package Layout

```
pkg/
├── chproto/                       # Layer A: pure ClickHouse native protocol codec
│   ├── packet.go                  # Packet types, tagged-union definition, known codes
│   ├── codec.go                   # Codec type, ReadPacket, Write*, error sentinels
│   ├── hello.go                   # Hello / ServerHello Decode/Encode (wraps ch-go)
│   ├── query.go                   # Query Decode/Encode (wraps ch-go)
│   ├── exception.go               # Exception Decode/Encode (wraps ch-go)
│   ├── addendum.go                # Chunked-protocol negotiation state machine
│   ├── compress.go                # LZ4/ZSTD frame boundary detection (splice only, no decompress)
│   └── features.go                # SupportsAddendum / SupportsChunkedPackets / revision consts
│
├── chsession/                     # Layer C: session + replayable state
│   ├── session.go                 # Session interface, sessionImpl, BindUpstream / RebindUpstream
│   ├── state.go                   # SessionState struct, Snapshot, Replay
│   ├── identity.go                # IdentityClaims
│   └── errors.go                  # Sentinel errors
│
└── proxy/                         # Assembly: server + relay + plugin wiring
    ├── server.go                  # Listener, accept loop, constructs Session + Relay per conn
    ├── relay.go                   # Relay struct, handshake, clientToUpstream / upstreamToClient loops
    ├── hooks.go                   # Hooks interface, PluginChain implementation
    └── plugins/                   # Built-in plugins
        ├── credentials.go         # CredentialInjector (Hello user/pwd injection)
        ├── route_strip.go         # __route__ prefix stripper (Hello)
        ├── jws_validator.go       # JWSValidator (Query)
        ├── state_tracker.go       # Regex-driven USE/SET tracker (Query)
        ├── rewriter_plugin.go     # SQL rewrite via gRPC (Query)
        ├── token_strip.go         # JWS token stripper (Query)
        └── error_reverse_map.go   # Exception reverse-map (Exception)
```

**Dependency direction:**
- `chproto` has **zero** dependency on proxy business logic. Potentially open-sourceable.
- `chsession` depends on `chproto` only.
- `proxy` depends on both above plus `cluster`, `rewriter`, etc.

### 3.1 Migration Map

| Current location | New location |
|---|---|
| `proxy.go` `handshakeClientHello`, `decodeServerHello`, `readNextPacket` | `chproto/codec.go` + `hello.go` |
| `pkg/proxy/chunked.go` | `chproto/addendum.go` |
| `proxy.go` `handleDataBlock` compressed frame probing | `chproto/compress.go` (hidden behind `Splice`) |
| `proxy.go` extra `ClientCode` constants (lines 80-89) | `chproto/packet.go` |
| `proxy.go` `copyClientToUpstreamStreaming` + `copyUpstreamToClientFromReader` | `proxy/relay.go` |
| `Proxy` struct fields `user`/`database`/`password`/`revision`/chunked flags | `chsession.SessionState` |
| `credential_provider.go`, `relay_signer.go` call points | Implemented as plugins in `proxy/plugins/` |
| Seven `Set*` setters on `*Proxy` | Removed; plugins injected into `Hooks` at construction |
| `init()`-registered Prometheus metrics in `observer.go` | Injected `prometheus.Registerer`; `MetricsPlugin` in hook chain |

---

## 4. Layer A — `chproto` Codec

### 4.1 Relationship to `ch-go/proto`

`ch-go/proto` already provides:

- `proto.Reader`, `proto.Buffer` — VarUInt, length-prefixed strings, UUID, etc.
- `proto.ClientCode`, `proto.ServerCode` — packet type enums
- `proto.ClientHello`, `proto.ServerHello`, `proto.Query`, `proto.Exception`, `proto.Progress` — struct + Decode/Encode
- Block `Input`/`Results`, Settings, compression, feature/revision constants

`chproto` **re-exports** those and **adds only**:

1. Tagged-union streaming read (`ReadPacket`) — ch-go assumes caller knows what packet to expect next; a proxy does not.
2. Zero-copy splice (`Splice`) — ch-go has no equivalent.
3. Addendum / chunked-protocol negotiation — ch-go 1.0 does not cover it completely.
4. Proxy-specific `ClientCode` constants not exposed by ch-go (KeepAlive=6, Scalar=7, IgnoredPartUUIDs=8, ReadTaskResponse=9, MergeTreeReadTaskResponse=10, QueryPlan=11, ClusterFunctionReadTaskResponse=13).
5. Feature helpers keyed on negotiated revision.

### 4.2 Core Types

```go
package chproto

import "github.com/ClickHouse/ch-go/proto"

// Re-exports
type (
    ClientCode  = proto.ClientCode
    ServerCode  = proto.ServerCode
    ClientHello = proto.ClientHello
    ServerHello = proto.ServerHello
    Query       = proto.Query
    Exception   = proto.Exception
    ClientInfo  = proto.ClientInfo
    Setting     = proto.Setting
)

// Proxy-only client codes ch-go does not expose
const (
    ClientKeepAlive                     ClientCode = 6
    ClientScalar                        ClientCode = 7
    ClientIgnoredPartUUIDs              ClientCode = 8
    ClientReadTaskResponse              ClientCode = 9
    ClientMergeTreeReadTaskResponse     ClientCode = 10
    ClientQueryPlan                     ClientCode = 11
    ClientClusterFunctionReadTaskResp   ClientCode = 13
)

// Direction tells Codec what kind of packets to expect on Read.
type Direction int
const (
    DirFromClient  Direction = iota // read ClientCode, write ServerCode
    DirToUpstream                   // read ServerCode, write ClientCode
)

// Packet is the tagged union returned by ReadPacket.
type Packet struct {
    Type    uint64 // ClientCode or ServerCode, depending on Direction
    RawLen  int    // total on-wire bytes including header
    Decoded any    // non-nil only if Type was requested in decodeTypes
    Raw     []byte // full bytes when Decoded is nil (caller hands to Splice)
}

// Codec wraps one side of a ClickHouse TCP connection.
type Codec struct {
    r        *proto.Reader
    w        *proto.Buffer
    conn     io.ReadWriter
    rev      atomic.Int64 // set via SetRevision after handshake
    dir      Direction
}

func NewCodec(conn io.ReadWriter, dir Direction) *Codec
func (c *Codec) Conn() io.ReadWriter
func (c *Codec) SetRevision(rev int)
func (c *Codec) Revision() int
```

### 4.3 Read / Write / Splice

```go
// ReadPacket reads the next packet header. If Type is listed in decodeTypes,
// the body is decoded into a typed struct (Packet.Decoded set, Packet.Raw nil).
// Otherwise, the body is accumulated into Packet.Raw (Decoded nil) for Splice.
//
// Errors:
//   io.EOF                — peer closed cleanly
//   ErrMalformed          — protocol error; caller must close
//   ErrUnknownPacket      — type not in known table; caller may log and Splice
//   ErrDecode             — decode of a requested type failed; per D8 caller
//                           should fall back to Splice(raw bytes).
func (c *Codec) ReadPacket(decodeTypes ...uint64) (*Packet, error)

// Splice forwards a Packet to dst without further decoding.
// For Data/Scalar packets the internal LZ4/ZSTD frame walker ensures
// complete frame boundaries are preserved; callers never touch the frame
// layout themselves.
func (c *Codec) Splice(dst io.Writer, p *Packet) error

// Write methods: delegate to ch-go Encode, taking revision into account.
func (c *Codec) WriteClientHello(h *ClientHello) error
func (c *Codec) WriteServerHello(h *ServerHello) error
func (c *Codec) WriteQuery(q *Query) error
func (c *Codec) WriteException(e *Exception) error
```

### 4.4 Addendum (chunked) Negotiation

Ported from `pkg/proxy/chunked.go`. Semantics preserved verbatim — only the interface is new.

```go
type AddendumOpts struct {
    ProposedRecv string // "notchunked" | "chunked" | "chunked_optional"
    ProposedSend string
}
type AddendumResult struct {
    NegotiatedRecv string
    NegotiatedSend string
}

func (c *Codec) NegotiateAddendum(opts AddendumOpts) (AddendumResult, error)
func (c *Codec) SendAddendum(res AddendumResult) error
```

### 4.5 Feature Helpers

```go
func SupportsAddendum(rev int) bool        // rev >= DBMS_MIN_REVISION_WITH_ADDENDUM
func SupportsChunkedPackets(rev int) bool  // rev >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS
// …and others as needed
```

### 4.6 Decode-failure Policy

Per D8: on `ErrDecode` the caller (Relay) shall forward the original bytes through `Splice`. Rationale: ch-go struct evolution lag, unknown feature flags. Logged as WARN; the warning counter is emitted as a Prometheus metric.

---

## 5. Layer C — `chsession` Session

### 5.1 `SessionState`

```go
package chsession

type VersionTriple struct{ Major, Minor, Patch int }

type SessionState struct {
    // Negotiated at handshake; stable per session. NOT replayed on rebind —
    // a new upstream performs its own handshake and renegotiates.
    ClientHostname    string
    ClientVersion     VersionTriple
    ClientRevision    int
    ServerDisplayName string
    Timezone          string
    ChunkedRecv       string
    ChunkedSend       string

    // Identity & credentials.
    AuthenticatedUser string
    MappedUser        string
    MappedPassword    string
    Identity          IdentityClaims // from JWS validation

    // Replayable on rebind.
    Database string
    Settings map[string]chproto.Setting // only SET-statement settings; NOT Query.Settings

    // Runtime tracking; NOT replayed. Used for observability and error reverse-mapping.
    LastQueryID      string
    LastClientInfo   *chproto.ClientInfo
    LastRewriteArgs  rewriter.RewriteArgs
    HasActiveRewrite bool

    mu sync.RWMutex
}

// Snapshot returns an immutable view for plugins/observers.
type SessionStateSnapshot struct { /* same fields, unexported mu */ }

func (s *SessionState) Snapshot() SessionStateSnapshot
func (s *SessionState) SetDatabase(db string)
func (s *SessionState) AddSetting(k string, v chproto.Setting)
func (s *SessionState) MarkActiveRewrite(args rewriter.RewriteArgs)
func (s *SessionState) ClearActiveRewrite()

// Replay re-applies the replayable subset (Database, Settings) to a newly
// bound upstream via plain SQL (USE <db>; SET k=v; …). Used only by
// RebindUpstream when replayState=true. MVP does not invoke this, but the
// method is implemented and unit-tested day one so C3 adoption is a flip.
func (s *SessionState) Replay(ctx context.Context, up *chproto.Codec) error
```

### 5.2 `Session` Interface

```go
type Session interface {
    ID() int64
    State() *SessionState           // caller may mutate; plugins should Snapshot
    Client() *chproto.Codec
    Upstream() *chproto.Codec       // atomic pointer under the hood
    RemoteAddr() net.Addr           // client-side
    Close() error

    BindUpstream(ctx context.Context, up *chproto.Codec) error
    RebindUpstream(ctx context.Context, newUp *chproto.Codec, replayState bool) error
}

type sessionImpl struct {
    id        int64
    state     *SessionState
    client    *chproto.Codec
    up        atomic.Pointer[chproto.Codec]
    clientConn net.Conn
    closeOnce sync.Once
}

func New(id int64, clientConn net.Conn) Session
```

### 5.3 Lifecycle

1. **Construct.** `chsession.New(id, clientConn)` wraps the client conn in a Codec. No network traffic.
2. **Handshake.** Relay reads `ClientHello` via `Client().ReadPacket(ClientHello)`, runs `OnHello` plugins (credential injection, route strip), dials or borrows an upstream, constructs its Codec, calls `BindUpstream`. Relay forwards the injected Hello upstream, reads `ServerHello`, replays it to the client, runs Addendum on both sides, then sets `SessionState` fields (revision, timezone, chunked modes, MappedUser, Database, Identity).
3. **Serve.** Relay runs two goroutines over `Client()` and `Upstream()` codecs. See §6.
4. **Close.** `Session.Close()` closes both sides once; idempotent via `sync.Once`. Calls `Hooks.OnClose`.

### 5.4 Rebind Semantics (future C3)

```go
func (s *sessionImpl) RebindUpstream(ctx, newUp, replay) error {
    old := s.up.Swap(newUp)
    go func() {
        // best effort; any in-flight Splice owning the old pointer finishes first
        _ = old.Conn().(io.Closer).Close()
    }()
    // Handshake the new upstream by reconstructing a ClientHello from
    // SessionState (ClientHostname, ClientVersion, ClientRevision, Database,
    // MappedUser, MappedPassword). Read its ServerHello, run Addendum. The
    // new upstream's negotiated revision must equal state.ClientRevision, else
    // reject — mismatched revisions would require renegotiating the client side
    // too, which we don't support mid-session.
    if err := handshakeUpstream(ctx, newUp, s.state); err != nil { return err }
    if replay {
        return s.state.Replay(ctx, newUp) // USE <db>; SET ...
    }
    return nil
}
```

MVP calls `BindUpstream` exactly once at session start. `RebindUpstream` is exercised by unit tests that drive it directly; no production caller yet.

### 5.5 State Mutation Responsibilities

Session itself does **not** parse SQL. A dedicated `StateTrackerPlugin` in the `OnQuery` chain updates `SessionState.Database` and `SessionState.Settings` using regex:

```go
// plugins/state_tracker.go
//
// Detect USE <database> and SET k=v using regex, NOT the rewriter AST.
// Rationale (D6): the rewriter gRPC round-trip is ~1-5ms per call, and these
// two statements have trivially-stable syntax. Regex keeps the hot path
// free of network calls. Any false positives (e.g. USE inside a comment)
// are harmless because the same statement flows to the real ClickHouse,
// whose AST parse is authoritative — we only mirror the state for our
// rewrite decisions.
var (
    reUse = regexp.MustCompile(`(?i)^\s*USE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*;?\s*$`)
    reSet = regexp.MustCompile(`(?i)^\s*SET\s+([a-zA-Z_]\w*)\s*=\s*(.+?)\s*;?\s*$`)
)
```

User-facing documentation must call out this regex tracking so operators understand the semantics (e.g., `SET ... , ..., ...` compound statements are not supported; only single `SET k=v`).

---

## 6. Relay & Hooks

### 6.1 Hooks Interface

```go
// proxy/hooks.go
type QueryContext struct {
    Session      chsession.Session
    OriginalSQL  string
    RewrittenSQL string
    Query        *chproto.Query
    RewriteArgs  rewriter.RewriteArgs
    Values       map[string]any // plugin-to-plugin scratch
}

type Hooks interface {
    OnHello(ctx context.Context, sess chsession.Session, hello *chproto.ClientHello) error
    OnQuery(ctx context.Context, qctx *QueryContext) error
    OnException(ctx context.Context, sess chsession.Session, exc *chproto.Exception) error
    OnClose(sess chsession.Session)
}
```

A reference implementation runs an ordered plugin list per hook (middleware chain). First non-nil error short-circuits the chain; Relay converts it to a ClickHouse Exception and writes to the client.

### 6.2 Plugin Order (MVP)

| Hook | Plugins (in order) |
|---|---|
| `OnHello` | CredentialInjector → RouteStripper |
| `OnQuery` | JWSValidator → StateTracker → Rewriter → TokenStripper |
| `OnException` | ErrorReverseMapper |
| `OnClose` | MetricsPlugin (records duration, error rate) |

### 6.3 Relay Main Loop

```go
// proxy/relay.go
type Relay struct {
    sess  chsession.Session
    hooks Hooks
    log   *zap.Logger
}

func (r *Relay) Run(ctx context.Context) error {
    if err := r.handshake(ctx); err != nil { return err }
    errCh := make(chan error, 2)
    go func() { errCh <- r.clientToUpstream(ctx) }()
    go func() { errCh <- r.upstreamToClient(ctx) }()
    err := <-errCh
    _ = r.sess.Close()
    <-errCh
    r.hooks.OnClose(r.sess)
    return err
}
```

**`clientToUpstream`:** reads packets from `sess.Client()`, decoding only `ClientQuery`. On Query: run `OnQuery`, then `sess.Upstream().WriteQuery(qctx.Query)`. Any other packet: `Splice` to upstream conn.

**`upstreamToClient`:** reads from `sess.Upstream()`, decoding `ServerException` only if `sess.State().Snapshot().HasActiveRewrite`. On Exception: `OnException` mutates `exc.Message`, then `WriteException`. On `ServerEndOfStream`: `state.ClearActiveRewrite()`. Everything else: Splice.

### 6.4 Implicit Protocol Phase State Machine

```
                    OnHello
                      │
    [pre-handshake]──────►[idle]──OnQuery (set HasActiveRewrite)──►[in-query]
                           ▲                                          │
                           │                                          │
                           └── EndOfStream | Exception ───────────────┘
                                 (ClearActiveRewrite)
```

This is enforced by code flow in Relay, not by an explicit `SessionPhase` enum. Pre-handshake packets other than Hello cause immediate connection close. `idle` accepts Query / Cancel / Ping / Data (Data for chunked inserts). `in-query` gates Exception decoding for reverse-mapping.

### 6.5 Server Assembly

```go
// proxy/server.go
type Server struct {
    hooks Hooks
    ctx   context.Context
    log   *zap.Logger
    // upstream factory varies by mode (server / agent / forwarding-only),
    // supplied by cmd/proxy/main.go
    dialUpstream func(ctx context.Context, s chsession.Session) (*chproto.Codec, error)
}

func (s *Server) Serve(ln net.Listener) error {
    for {
        c, err := ln.Accept()
        if err != nil { return err }
        go s.handle(c)
    }
}

func (s *Server) handle(c net.Conn) {
    sess := chsession.New(nextID(), c)
    defer sess.Close()
    up, err := s.dialUpstream(s.ctx, sess)
    if err != nil { /* log + close */; return }
    _ = sess.BindUpstream(s.ctx, up)
    (&Relay{sess: sess, hooks: s.hooks, log: s.log}).Run(s.ctx)
}
```

The three deployment modes (server / agent / forwarding-only) differ only in `dialUpstream` and which plugins are wired into `Hooks`. The Codec, Session, and Relay code are identical across modes.

---

## 7. Error Handling

- **Codec `ErrDecode`** → Relay logs WARN, forwards original bytes via Splice (D8).
- **Plugin error in `OnQuery`** → Relay builds a synthetic `chproto.Exception`, writes to client, skips sending to upstream. Session remains open.
- **Plugin error in `OnHello`** → connection closed before upstream is bound.
- **Upstream TCP error mid-stream** → Relay returns from `upstreamToClient`, `Run` closes Session; MVP does not reconnect. Future C3 may convert to `RebindUpstream(replay=true)` for idempotent queries.
- **Client TCP error** → Relay returns from `clientToUpstream`, Session closed.

---

## 8. Testing Plan

- **`chproto`**: table-driven tests per packet type with golden byte buffers; Splice round-trip on real ClickHouse Data block captures; Addendum state machine exhaustive truth table; `ReadPacket` fuzz with random byte corruption ensuring `ErrMalformed` (not panic).
- **`chsession`**: unit tests of `State.Snapshot`, `AddSetting`, `SetDatabase`, concurrent Snapshot+mutate race tests via `-race`; **`Replay` tests** drive `RebindUpstream(replay=true)` against a scripted mock upstream codec even though production does not yet call it — this is how we prevent C3 bitrot.
- **`proxy/relay`**: in-memory duplex pipe test fixtures (`net.Pipe`) with a mock upstream returning scripted ServerHello/Exception/Data, asserting plugin order and state transitions. No dependency on a real ClickHouse for the proxy tests; existing `rewriter_e2e_test.go` style end-to-end tests remain and move under a build tag.

---

## 9. Rollout Plan

1. Introduce `chproto` and `chsession` packages alongside the current `proxy.go`; no behavior change. Unit tests only.
2. Rewrite `proxy.go` into `proxy/server.go` + `relay.go` + `hooks.go` using the new packages. Existing `credential_provider`, `relay_signer`, rewriter client wrapped as plugins. Expect the Bazel `proxy_test` suite to pass at baseline.
3. Delete the legacy `copyClientToUpstream` (already dead) and seven `Set*` setters once nothing references them.
4. C3 enablement is a separate future PR that flips `BindUpstream` → per-query `RebindUpstream` driven by a pool borrow/return; no interface change required.

---

## 10. Open Questions (Deferred)

- Per-query pool borrow policy (QPS threshold? replica affinity?). Out of scope for this spec.
- Whether `SessionState.Settings` should be size-capped. Parking for a follow-up.
- Chunked INSERT data flow across a mid-query rebind — likely unsafe to rebind during an in-flight INSERT; needs explicit "rebind only in `idle`" gate. Revisit in the C3 PR.
